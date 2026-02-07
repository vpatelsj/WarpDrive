package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/warpdrive/warpdrive/pkg/auth"
	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/cache"
	"github.com/warpdrive/warpdrive/pkg/config"
	kfuse "github.com/warpdrive/warpdrive/pkg/fuse"
	"github.com/warpdrive/warpdrive/pkg/metrics"
	"github.com/warpdrive/warpdrive/pkg/namespace"
	"github.com/warpdrive/warpdrive/pkg/telemetry"
)

func main() {
	configPath := flag.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	mountPoint := flag.String("mount", "", "Mount point (overrides config)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	if *mountPoint != "" {
		cfg.MountPoint = *mountPoint
	}
	if cfg.MountPoint == "" {
		slog.Error("mount_point is required (set in config or via --mount)")
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.MountPoint, 0o755); err != nil {
		slog.Error("failed to create mount point", "path", cfg.MountPoint, "error", err)
		os.Exit(1)
	}

	// ── Auth Manager ──────────────────────────────────────────────
	audit := auth.NewAuditLogger(10000, nil)
	authMgr := auth.NewManager(audit)

	// Register all auth providers
	entraP := auth.NewEntraProvider()
	authMgr.RegisterProvider(entraP)
	authMgr.RegisterProvider(auth.NewAWSProvider(entraP))
	authMgr.RegisterProvider(auth.NewGCPProvider(entraP))
	authMgr.RegisterProvider(auth.NewKeyVaultProvider())
	authMgr.RegisterProvider(auth.NewStaticProvider())
	authMgr.RegisterProvider(auth.NewNoneProvider())

	slog.Info("auth manager initialized", "providers", authMgr.ProviderCount())

	// ── Backends (with auth wrapping) ─────────────────────────────
	reg := backend.NewRegistry()
	for _, bcfg := range cfg.Backends {
		remotePath := bcfg.Config["root"]
		be, err := backend.NewRcloneBackend(bcfg.Name, bcfg.Type, remotePath, bcfg.Config)
		if err != nil {
			slog.Error("failed to create backend", "name", bcfg.Name, "type", bcfg.Type, "error", err)
			os.Exit(1)
		}

		// Configure auth for this backend
		authCfg := auth.ProviderConfig{
			Method:          bcfg.Auth.Method,
			TenantID:        bcfg.Auth.TenantID,
			ClientIDEnv:     bcfg.Auth.ClientIDEnv,
			ClientSecretEnv: bcfg.Auth.ClientSecretEnv,
			RoleARN:         bcfg.Auth.RoleARN,
			GCPProjectNum:   bcfg.Auth.GCPProjectNum,
			GCPPoolID:       bcfg.Auth.GCPPoolID,
			GCPProviderID:   bcfg.Auth.GCPProviderID,
			GCPServiceAcct:  bcfg.Auth.GCPServiceAcct,
			VaultURL:        bcfg.Auth.VaultURL,
			SecretName:      bcfg.Auth.SecretName,
			AccessKeyID:     bcfg.Auth.AccessKeyID,
			SecretAccessKey: bcfg.Auth.SecretAccessKey,
			Extra:           bcfg.Auth.Extra,
		}

		// Default to "none" if no auth method specified
		if authCfg.Method == "" {
			authCfg.Method = "none"
		}

		authMgr.SetBackendAuth(bcfg.Name, authCfg)

		// Wrap with AuthenticatedBackend
		authBe := backend.NewAuthenticatedBackend(be, authMgr)

		if err := reg.Register(authBe); err != nil {
			slog.Error("failed to register backend", "name", bcfg.Name, "error", err)
			os.Exit(1)
		}
		slog.Info("registered backend", "name", bcfg.Name, "type", bcfg.Type, "auth", authCfg.Method)
	}

	cacheMgr, err := cache.New(cfg.Cache, reg)
	if err != nil {
		slog.Error("failed to create cache manager", "error", err)
		os.Exit(1)
	}
	defer cacheMgr.Close()

	ns := namespace.New(cfg.Backends)

	root := kfuse.NewRoot(ns, cacheMgr, reg)

	// ── Telemetry ─────────────────────────────────────────────────
	if cfg.Telemetry.Enabled {
		tc, err := telemetry.NewCollector(telemetry.CollectorConfig{
			Enabled:           true,
			Sink:              cfg.Telemetry.Sink,
			FilePath:          cfg.Telemetry.FilePath,
			ControlPlaneAddr:  cfg.Telemetry.ControlPlaneAddr,
			SampleMetadataOps: cfg.Telemetry.SampleMetadataOps,
			BatchSize:         cfg.Telemetry.BatchSize,
			FlushInterval:     cfg.Telemetry.FlushInterval,
		})
		if err != nil {
			slog.Warn("telemetry collector failed to initialize", "error", err)
		} else {
			root.SetTelemetry(tc)
			defer tc.Close()
			slog.Info("telemetry enabled", "sink", cfg.Telemetry.Sink)
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// ── Metrics + Health Server ──────────────────────────────────
	metrics.RegisterHealthCheck("cache_db", func() error {
		// Check badger DB is accessible via cache stats
		_ = cacheMgr.GetStats()
		return nil
	})

	metricsStop := make(chan struct{})
	if cfg.Metrics.MetricsEnabled() {
		go func() {
			if err := metrics.MetricsServer(cfg.Metrics.Addr, metricsStop); err != nil {
				slog.Error("metrics server error", "error", err)
			}
		}()
		slog.Info("metrics server started", "addr", cfg.Metrics.Addr)
	} else {
		slog.Info("metrics server disabled")
	}
	defer close(metricsStop)

	slog.Info("mounting warpdrive filesystem", "mount_point", cfg.MountPoint, "backends", len(cfg.Backends))

	if err := kfuse.Mount(ctx, kfuse.MountConfig{
		MountPoint: cfg.MountPoint,
		AllowOther: cfg.AllowOther,
		Debug:      cfg.FUSEDebug,
	}, root); err != nil {
		slog.Error("mount failed", "error", err)
		os.Exit(1)
	}

	slog.Info("warpdrive unmounted cleanly")
}
