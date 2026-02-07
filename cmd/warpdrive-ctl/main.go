// Package main provides the warpdrive-ctl CLI for cache and governance operations.
//
// Usage:
//
//	warpdrive-ctl warm [--config <file>] --backend <name> [--prefix <path>] [--recursive] [--max-size 500GB] [--workers 32]
//	warpdrive-ctl stats [--config <file>]
//	warpdrive-ctl usage [--config <file>] [--group-by team|user] [--backend all|<name>] [--format table|csv]
//	warpdrive-ctl stale [--config <file>] [--days 90] [--min-size 0] [--format table|csv]
//	warpdrive-ctl quota set|list [--config <file>] [--team <name>] [--backend <name>] [--soft <size>] [--hard <size>]
//	warpdrive-ctl status [--config <file>]
//	warpdrive-ctl move [--config <file>] --src <backend:path> --dst <backend:path> [--delete]
//	warpdrive-ctl serve [--config <file>] [--addr :8080]
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/warpdrive/warpdrive/pkg/auth"
	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/cache"
	"github.com/warpdrive/warpdrive/pkg/config"
	"github.com/warpdrive/warpdrive/pkg/control"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "warm":
		runWarm(os.Args[2:])
	case "stats":
		runStats(os.Args[2:])
	case "usage":
		runUsage(os.Args[2:])
	case "stale":
		runStale(os.Args[2:])
	case "quota":
		runQuota(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "move":
		runMove(os.Args[2:])
	case "serve":
		runServe(os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, "warpdrive-ctl — WarpDrive admin CLI\n\n")
	fmt.Fprint(os.Stderr, "Usage:\n")
	fmt.Fprint(os.Stderr, "  warpdrive-ctl <command> [flags]\n\n")
	fmt.Fprint(os.Stderr, "Commands:\n")
	fmt.Fprint(os.Stderr, "  warm     Pre-populate cache for a path\n")
	fmt.Fprint(os.Stderr, "  stats    Show cache statistics\n")
	fmt.Fprint(os.Stderr, "  usage    Show storage usage by team/user\n")
	fmt.Fprint(os.Stderr, "  stale    Find stale (unused) data\n")
	fmt.Fprint(os.Stderr, "  quota    Manage storage quotas\n")
	fmt.Fprint(os.Stderr, "  status   Show mount + control plane health\n")
	fmt.Fprint(os.Stderr, "  move     Move/copy data between backends\n")
	fmt.Fprint(os.Stderr, "  serve    Start the control plane server\n\n")
	fmt.Fprint(os.Stderr, "Use \"warpdrive-ctl <command> --help\" for more information about a command.\n")
}

// runWarm implements the "warpdrive-ctl warm" subcommand.
func runWarm(args []string) {
	fs := flag.NewFlagSet("warm", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	backendName := fs.String("backend", "", "Backend name (required)")
	prefix := fs.String("prefix", "", "Remote path prefix to warm")
	recursive := fs.Bool("recursive", true, "Warm recursively")
	maxSizeStr := fs.String("max-size", "0", "Max bytes to warm (e.g., 500GB, 0=unlimited)")
	workers := fs.Int("workers", 32, "Parallel download workers")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl warm [flags]\n\n")
		fmt.Fprint(os.Stderr, "Pre-populate the local cache for files under a backend prefix.\n")
		fmt.Fprint(os.Stderr, "Re-running the same command will skip already-cached files (resume support).\n\n")
		fmt.Fprint(os.Stderr, "Flags:\n")
		fs.PrintDefaults()
		fmt.Fprint(os.Stderr, "\nExamples:\n")
		fmt.Fprint(os.Stderr, "  warpdrive-ctl warm --config /etc/warpdrive/config.yaml --backend training\n")
		fmt.Fprint(os.Stderr, "  warpdrive-ctl warm --config myconfig.yaml --backend s3-data --prefix datasets/imagenet --max-size 500GB\n")
		fmt.Fprint(os.Stderr, "  warpdrive-ctl warm --config myconfig.yaml --backend training --workers 64\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *backendName == "" {
		fmt.Fprintln(os.Stderr, "Error: --backend is required")
		fs.Usage()
		os.Exit(1)
	}

	// Parse max size.
	var maxSize int64
	if *maxSizeStr != "0" && *maxSizeStr != "" {
		parsed, err := config.ParseSize(*maxSizeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid --max-size %q: %v\n", *maxSizeStr, err)
			os.Exit(1)
		}
		maxSize = parsed
	}

	// Load config.
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	// Set up backends with auth.
	reg, cacheMgr, cleanup := setupStack(cfg)
	defer cleanup()

	// Verify backend exists.
	if _, err := reg.Get(*backendName); err != nil {
		fmt.Fprintf(os.Stderr, "Error: unknown backend %q\n", *backendName)
		fmt.Fprintln(os.Stderr, "Available backends:")
		for _, b := range cfg.Backends {
			fmt.Fprintf(os.Stderr, "  - %s (%s)\n", b.Name, b.Type)
		}
		os.Exit(1)
	}

	// Set up signal handling.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	warmCfg := cache.WarmConfig{
		BackendName: *backendName,
		Prefix:      *prefix,
		Recursive:   *recursive,
		MaxSize:     maxSize,
		Workers:     *workers,
	}

	fmt.Println("WarpDrive Cache Warming")
	fmt.Println("────────────────────────────────────")
	fmt.Printf("Backend:    %s\n", *backendName)
	fmt.Printf("Prefix:     %s\n", displayOrDefault(*prefix, "/"))
	fmt.Printf("Recursive:  %v\n", *recursive)
	fmt.Printf("Max Size:   %s\n", displayOrDefault(*maxSizeStr, "unlimited"))
	fmt.Printf("Workers:    %d\n", *workers)
	fmt.Println("────────────────────────────────────")
	fmt.Println()

	start := time.Now()
	var lastPrint atomic.Int64

	err = cacheMgr.Warm(ctx, warmCfg, func(p cache.WarmProgress) {
		// Throttle progress output to at most once per 500ms.
		now := time.Now().UnixMilli()
		last := lastPrint.Load()
		if now-last < 500 {
			return
		}
		lastPrint.Store(now)

		pct := float64(0)
		if p.BytesTotal > 0 {
			pct = float64(p.BytesWarmed) / float64(p.BytesTotal) * 100
		}

		fmt.Printf("\r%d/%d files | %s/%s (%.1f%%) | %.1f MB/s | ETA %s   ",
			p.FilesWarmed, p.FilesTotal,
			humanBytes(p.BytesWarmed),
			humanBytes(p.BytesTotal),
			pct,
			p.Throughput,
			formatETA(p.ETA),
		)
	})

	elapsed := time.Since(start)
	fmt.Println()
	fmt.Println()

	if err != nil {
		if ctx.Err() != nil {
			fmt.Println("Warning: Warming interrupted (Ctrl+C). Re-run to resume.")
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Printf("Done: Cache warming complete in %s\n", elapsed.Truncate(time.Millisecond))
	}
}

// runStats implements the "warpdrive-ctl stats" subcommand.
func runStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl stats [flags]\n\nShow cache statistics.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	_, cacheMgr, cleanup := setupStack(cfg)
	defer cleanup()

	stats := cacheMgr.GetStats()

	fmt.Println("WarpDrive Cache Statistics")
	fmt.Println("────────────────────────────────────")
	fmt.Printf("Hits:         %d\n", stats.Hits)
	fmt.Printf("Misses:       %d\n", stats.Misses)
	fmt.Printf("Evictions:    %d\n", stats.Evictions)
	fmt.Printf("Bytes Cached: %s\n", humanBytes(stats.BytesCached))
	if stats.Hits+stats.Misses > 0 {
		hitRate := float64(stats.Hits) / float64(stats.Hits+stats.Misses) * 100
		fmt.Printf("Hit Rate:     %.1f%%\n", hitRate)
	}
	fmt.Println("────────────────────────────────────")
}

// setupStack initializes the backend registry and cache manager from config.
// Returns a cleanup function that should be deferred.
func setupStack(cfg *config.Config) (*backend.Registry, *cache.CacheManager, func()) {
	audit := auth.NewAuditLogger(1000, nil)
	authMgr := auth.NewManager(audit)

	entraP := auth.NewEntraProvider()
	authMgr.RegisterProvider(entraP)
	authMgr.RegisterProvider(auth.NewAWSProvider(entraP))
	authMgr.RegisterProvider(auth.NewGCPProvider(entraP))
	authMgr.RegisterProvider(auth.NewKeyVaultProvider())
	authMgr.RegisterProvider(auth.NewStaticProvider())
	authMgr.RegisterProvider(auth.NewNoneProvider())

	reg := backend.NewRegistry()
	for _, bcfg := range cfg.Backends {
		remotePath := bcfg.Config["root"]
		be, err := backend.NewRcloneBackend(bcfg.Name, bcfg.Type, remotePath, bcfg.Config)
		if err != nil {
			slog.Error("failed to create backend", "name", bcfg.Name, "error", err)
			os.Exit(1)
		}

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
		if authCfg.Method == "" {
			authCfg.Method = "none"
		}

		authMgr.SetBackendAuth(bcfg.Name, authCfg)
		authBe := backend.NewAuthenticatedBackend(be, authMgr)
		if err := reg.Register(authBe); err != nil {
			slog.Error("failed to register backend", "name", bcfg.Name, "error", err)
			os.Exit(1)
		}
	}

	cacheMgr, err := cache.New(cfg.Cache, reg)
	if err != nil {
		slog.Error("failed to create cache manager", "error", err)
		os.Exit(1)
	}

	return reg, cacheMgr, func() { cacheMgr.Close() }
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	suffix := []string{"KB", "MB", "GB", "TB", "PB"}
	if exp >= len(suffix) {
		exp = len(suffix) - 1
	}
	return fmt.Sprintf("%.2f %s", float64(b)/float64(div), suffix[exp])
}

func formatETA(d time.Duration) string {
	if d <= 0 {
		return "--"
	}
	d = d.Truncate(time.Second)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func displayOrDefault(s, def string) string {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return def
	}
	return s
}

// ─────────────────────────── Governance Commands ───────────────────────────

// setupControlServer creates an in-memory control plane server from config.
// Used by governance commands that operate locally.
func setupControlServer(cfg *config.Config) (*control.Server, *backend.Registry, func()) {
	reg, _, cleanup := setupStack(cfg)

	ctrlCfg := control.ControlPlaneConfig{
		RESTAddr:             cfg.ControlPlane.RESTAddr,
		StorageCrawlInterval: cfg.ControlPlane.StorageCrawlInterval,
	}
	srv := control.NewServer(ctrlCfg, reg)
	return srv, reg, cleanup
}

// runUsage implements "warpdrive-ctl usage".
func runUsage(args []string) {
	fs := flag.NewFlagSet("usage", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	groupBy := fs.String("group-by", "user", "Group by: team, user")
	backendFilter := fs.String("backend", "all", "Backend filter (all or specific name)")
	format := fs.String("format", "table", "Output format: table, csv")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl usage [flags]\n\nShow storage usage by team or user.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	srv, _, cleanup := setupControlServer(cfg)
	defer cleanup()

	// Crawl to index files
	ctx := context.Background()
	if err := srv.CrawlAll(ctx); err != nil {
		slog.Warn("crawl failed", "error", err)
	}

	rows := srv.QueryUsage(*groupBy, *backendFilter)

	switch *format {
	case "csv":
		w := csv.NewWriter(os.Stdout)
		w.Write([]string{"group", "backend", "files", "bytes", "last_access"})
		for _, r := range rows {
			w.Write([]string{r.GroupName, r.BackendName,
				fmt.Sprintf("%d", r.FileCount),
				fmt.Sprintf("%d", r.TotalBytes),
				r.LastAccess.Format("2006-01-02 15:04:05")})
		}
		w.Flush()
	default:
		fmt.Println("Storage Usage")
		fmt.Println("────────────────────────────────────────────────────────────")
		fmt.Printf("%-20s %-15s %8s %12s %s\n", "GROUP", "BACKEND", "FILES", "BYTES", "LAST ACCESS")
		fmt.Println("────────────────────────────────────────────────────────────")
		for _, r := range rows {
			la := "-"
			if !r.LastAccess.IsZero() {
				la = r.LastAccess.Format("2006-01-02")
			}
			fmt.Printf("%-20s %-15s %8d %12s %s\n", r.GroupName, r.BackendName, r.FileCount, humanBytes(r.TotalBytes), la)
		}
		if len(rows) == 0 {
			fmt.Println("  (no usage data)")
		}
		fmt.Println("────────────────────────────────────────────────────────────")
	}
}

// runStale implements "warpdrive-ctl stale".
func runStale(args []string) {
	fs := flag.NewFlagSet("stale", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	days := fs.Int("days", 90, "Days since last access to consider stale")
	minSizeStr := fs.String("min-size", "0", "Minimum file size (e.g. 10GB)")
	format := fs.String("format", "table", "Output format: table, csv")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl stale [flags]\n\nFind stale (unused) data.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	var minSize int64
	if *minSizeStr != "0" && *minSizeStr != "" {
		parsed, err := config.ParseSize(*minSizeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid --min-size %q: %v\n", *minSizeStr, err)
			os.Exit(1)
		}
		minSize = parsed
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	srv, _, cleanup := setupControlServer(cfg)
	defer cleanup()

	ctx := context.Background()
	if err := srv.CrawlAll(ctx); err != nil {
		slog.Warn("crawl failed", "error", err)
	}

	files := srv.QueryStale(*days, minSize)

	switch *format {
	case "csv":
		w := csv.NewWriter(os.Stdout)
		w.Write([]string{"backend", "path", "size_bytes", "last_modified", "last_access"})
		for _, f := range files {
			w.Write([]string{f.BackendName, f.Path,
				fmt.Sprintf("%d", f.SizeBytes),
				f.LastModified.Format("2006-01-02"),
				f.LastAccess.Format("2006-01-02")})
		}
		w.Flush()
	default:
		fmt.Printf("Stale Data (not accessed in %d days)\n", *days)
		fmt.Println("────────────────────────────────────────────────────────────")
		fmt.Printf("%-15s %-30s %12s %s\n", "BACKEND", "PATH", "SIZE", "LAST ACCESS")
		fmt.Println("────────────────────────────────────────────────────────────")
		for _, f := range files {
			la := f.LastAccess.Format("2006-01-02")
			fmt.Printf("%-15s %-30s %12s %s\n", f.BackendName, truncPath(f.Path, 30), humanBytes(f.SizeBytes), la)
		}
		if len(files) == 0 {
			fmt.Println("  (no stale files found)")
		}
		fmt.Println("────────────────────────────────────────────────────────────")
	}
}

// runQuota implements "warpdrive-ctl quota".
func runQuota(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: warpdrive-ctl quota <set|list> [flags]")
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		runQuotaList(args[1:])
	case "set":
		runQuotaSet(args[1:])
	case "--help", "-h", "help":
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl quota <set|list> [flags]\n\n")
		fmt.Fprint(os.Stderr, "Manage storage quotas per team per backend.\n\n")
		fmt.Fprint(os.Stderr, "Subcommands:\n")
		fmt.Fprint(os.Stderr, "  set    Set a quota for a team\n")
		fmt.Fprint(os.Stderr, "  list   List all configured quotas\n")
	default:
		fmt.Fprintf(os.Stderr, "Unknown quota subcommand: %s\nUsage: warpdrive-ctl quota <set|list> [flags]\n", args[0])
		os.Exit(1)
	}
}

func runQuotaList(args []string) {
	fs := flag.NewFlagSet("quota list", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	srv, _, cleanup := setupControlServer(cfg)
	defer cleanup()

	quotas := srv.GetQuotas()

	fmt.Println("Quotas")
	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Printf("%-15s %-15s %12s %12s %12s %s\n", "TEAM", "BACKEND", "SOFT LIMIT", "HARD LIMIT", "USAGE", "STATUS")
	fmt.Println("────────────────────────────────────────────────────────────")
	for _, q := range quotas {
		be := q.BackendName
		if be == "" {
			be = "all"
		}
		status := "OK"
		if q.Exceeded {
			status = "EXCEEDED"
		} else if q.SoftExceeded {
			status = "WARNING"
		}
		fmt.Printf("%-15s %-15s %12s %12s %12s %s\n",
			q.TeamName, be,
			humanBytes(q.SoftLimit), humanBytes(q.HardLimit),
			humanBytes(q.CurrentUsage), status)
	}
	if len(quotas) == 0 {
		fmt.Println("  (no quotas configured)")
	}
	fmt.Println("────────────────────────────────────────────────────────────")
}

func runQuotaSet(args []string) {
	fs := flag.NewFlagSet("quota set", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	team := fs.String("team", "", "Team name (required)")
	backendName := fs.String("backend", "", "Backend name (empty = all backends)")
	softStr := fs.String("soft", "0", "Soft limit (e.g. 500TB)")
	hardStr := fs.String("hard", "0", "Hard limit (e.g. 600TB)")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl quota set [flags]\n\nSet a storage quota for a team.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *team == "" {
		fmt.Fprintln(os.Stderr, "Error: --team is required")
		fs.Usage()
		os.Exit(1)
	}

	soft, _ := config.ParseSize(*softStr)
	hard, _ := config.ParseSize(*hardStr)

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	srv, _, cleanup := setupControlServer(cfg)
	defer cleanup()

	if err := srv.SetQuota(control.Quota{
		TeamName:    *team,
		BackendName: *backendName,
		SoftLimit:   soft,
		HardLimit:   hard,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Quota set for team %q (backend=%s, soft=%s, hard=%s)\n",
		*team, displayOrDefault(*backendName, "all"),
		humanBytes(soft), humanBytes(hard))
}

// runStatus implements "warpdrive-ctl status".
func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl status [flags]\n\nShow system health.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	_, cacheMgr, cleanup := setupStack(cfg)
	defer cleanup()

	stats := cacheMgr.GetStats()

	fmt.Println("WarpDrive Status")
	fmt.Println("────────────────────────────────────")
	fmt.Printf("Mount Point:    %s\n", cfg.MountPoint)
	fmt.Printf("Backends:       %d configured\n", len(cfg.Backends))
	for _, b := range cfg.Backends {
		fmt.Printf("  - %-15s (%s) at %s\n", b.Name, b.Type, b.MountPath)
	}
	fmt.Println()
	fmt.Println("Cache")
	fmt.Println("────────────────────────────────────")
	fmt.Printf("Hits:         %d\n", stats.Hits)
	fmt.Printf("Misses:       %d\n", stats.Misses)
	fmt.Printf("Evictions:    %d\n", stats.Evictions)
	fmt.Printf("Bytes Cached: %s\n", humanBytes(stats.BytesCached))
	if stats.Hits+stats.Misses > 0 {
		hitRate := float64(stats.Hits) / float64(stats.Hits+stats.Misses) * 100
		fmt.Printf("Hit Rate:     %.1f%%\n", hitRate)
	}

	// Telemetry status
	fmt.Println()
	fmt.Println("Telemetry")
	fmt.Println("────────────────────────────────────")
	if cfg.Telemetry.Enabled {
		fmt.Printf("Enabled:      true\n")
		fmt.Printf("Sink:         %s\n", cfg.Telemetry.Sink)
	} else {
		fmt.Printf("Enabled:      false\n")
	}
	fmt.Println("────────────────────────────────────")
}

// runMove implements "warpdrive-ctl move".
func runMove(args []string) {
	fs := flag.NewFlagSet("move", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	src := fs.String("src", "", "Source: backend:path (required)")
	dst := fs.String("dst", "", "Destination: backend:path (required)")
	deleteSrc := fs.Bool("delete", false, "Delete source after copy (true move)")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl move [flags]\n\nMove/copy data between backends.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprint(os.Stderr, "\nExamples:\n")
		fmt.Fprint(os.Stderr, "  warpdrive-ctl move --src s3:datasets/old.tar --dst azure:archive/old.tar\n")
		fmt.Fprint(os.Stderr, "  warpdrive-ctl move --src s3:data/ --dst gcs:data/ --delete\n")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *src == "" || *dst == "" {
		fmt.Fprintln(os.Stderr, "Error: --src and --dst are required")
		fs.Usage()
		os.Exit(1)
	}

	srcBackend, srcPath, ok := parseBackendPath(*src)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: invalid --src format %q (expected backend:path)\n", *src)
		os.Exit(1)
	}
	dstBackend, dstPath, ok := parseBackendPath(*dst)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: invalid --dst format %q (expected backend:path)\n", *dst)
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	reg, _, cleanup := setupStack(cfg)
	defer cleanup()

	srcBE, err := reg.Get(srcBackend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: unknown source backend %q\n", srcBackend)
		os.Exit(1)
	}
	dstBE, err := reg.Get(dstBackend)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: unknown destination backend %q\n", dstBackend)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	op := "Copy"
	if *deleteSrc {
		op = "Move"
	}

	fmt.Printf("%s: %s:%s → %s:%s\n", op, srcBackend, srcPath, dstBackend, dstPath)

	// Open source.
	reader, err := srcBE.Open(ctx, srcPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening source: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	// Get source size.
	info, err := srcBE.Stat(ctx, srcPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error stat source: %v\n", err)
		os.Exit(1)
	}

	// Write to destination.
	if err := dstBE.Write(ctx, dstPath, reader, info.Size); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing destination: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Copied %s to %s:%s\n", humanBytes(info.Size), dstBackend, dstPath)

	// Delete source if requested.
	if *deleteSrc {
		if err := srcBE.Delete(ctx, srcPath); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: copy succeeded but delete of source failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Deleted source %s:%s\n", srcBackend, srcPath)
	}
}

// parseBackendPath parses "backend:path" format.
func parseBackendPath(s string) (backendName, path string, ok bool) {
	idx := strings.Index(s, ":")
	if idx <= 0 || idx == len(s)-1 {
		return "", "", false
	}
	return s[:idx], s[idx+1:], true
}

// runServe implements "warpdrive-ctl serve" — starts the control plane server.
func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	configPath := fs.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
	addr := fs.String("addr", "", "Listen address (overrides config, default :8080)")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: warpdrive-ctl serve [flags]\n\nStart the control plane server.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}

	srv, _, cleanup := setupControlServer(cfg)
	defer cleanup()

	if *addr != "" {
		srv.SetRESTAddr(*addr)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	fmt.Println("WarpDrive Control Plane")
	fmt.Println("────────────────────────────────────")
	listenAddr := cfg.ControlPlane.RESTAddr
	if *addr != "" {
		listenAddr = *addr
	}
	if listenAddr == "" {
		listenAddr = ":8080"
	}
	fmt.Printf("Listening:    %s\n", listenAddr)
	fmt.Printf("Backends:     %d configured\n", len(cfg.Backends))
	fmt.Println("────────────────────────────────────")

	if err := srv.Run(ctx); err != nil {
		slog.Error("control plane error", "error", err)
		os.Exit(1)
	}
	fmt.Println("Control plane shut down cleanly.")
}

func truncPath(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return "..." + s[len(s)-maxLen+3:]
}
