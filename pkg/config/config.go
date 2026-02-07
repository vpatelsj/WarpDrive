package config

import (
	"fmt"
	"time"
)

// Config is the top-level WarpDrive configuration.
type Config struct {
	MountPoint   string             `yaml:"mount_point"`
	AllowOther   bool               `yaml:"allow_other"`
	FUSEDebug    bool               `yaml:"fuse_debug"`
	Cache        CacheConfig        `yaml:"cache"`
	Backends     []BackendConfig    `yaml:"backends"`
	Metrics      MetricsConfig      `yaml:"metrics"`
	Telemetry    TelemetryConfig    `yaml:"telemetry"`
	ControlPlane ControlPlaneConfig `yaml:"control_plane"`
}

// MetricsConfig configures the Prometheus metrics and health endpoint.
type MetricsConfig struct {
	Enabled *bool  `yaml:"enabled"` // pointer to distinguish unset from false; default true
	Addr    string `yaml:"addr"`    // listen address; default ":9090"
}

// MetricsEnabled returns whether the metrics server should run.
func (m MetricsConfig) MetricsEnabled() bool {
	if m.Enabled == nil {
		return true // default: enabled
	}
	return *m.Enabled
}

// TelemetryConfig configures telemetry collection on the node agent.
type TelemetryConfig struct {
	Enabled           bool          `yaml:"enabled"`
	Sink              string        `yaml:"sink"` // "stdout", "file", "nop"
	FilePath          string        `yaml:"file_path"`
	ControlPlaneAddr  string        `yaml:"control_plane_addr"`
	SampleMetadataOps float64       `yaml:"sample_metadata_ops"`
	BatchSize         int           `yaml:"batch_size"`
	FlushInterval     time.Duration `yaml:"flush_interval"`
}

// ControlPlaneConfig configures the control plane server.
type ControlPlaneConfig struct {
	RESTAddr             string        `yaml:"rest_addr"`
	StorageCrawlInterval time.Duration `yaml:"storage_crawl_interval"`
}

// CacheConfig configures the local NVMe cache engine.
type CacheConfig struct {
	Path             string        `yaml:"path"`
	Dir              string        `yaml:"dir"` // Alias for Path
	MaxSizeRaw       string        `yaml:"max_size"`
	BlockSizeRaw     string        `yaml:"block_size"`
	MaxSize          int64         `yaml:"-"`
	BlockSize        int64         `yaml:"-"`
	ReadaheadBlocks  int           `yaml:"readahead_blocks"`
	MaxParallelFetch int           `yaml:"max_parallel_fetch"`
	StaleTTL         time.Duration `yaml:"stale_ttl"`
	EvictHighWater   float64       `yaml:"evict_high_water"`
	EvictLowWater    float64       `yaml:"evict_low_water"`
}

// BackendAuthConfig is the auth section from a backend's config.
type BackendAuthConfig struct {
	Method          string            `yaml:"method"`                          // managed_identity, service_principal, oidc_federation, gcp_wif, static_keyvault, static, none
	TenantID        string            `yaml:"tenant_id,omitempty"`             // For service_principal
	ClientIDEnv     string            `yaml:"client_id_env,omitempty"`         // Env var name for client ID
	ClientSecretEnv string            `yaml:"client_secret_env,omitempty"`     // Env var name for client secret
	RoleARN         string            `yaml:"role_arn,omitempty"`              // For AWS OIDC federation
	GCPProjectNum   string            `yaml:"project_number,omitempty"`        // For GCP WIF
	GCPPoolID       string            `yaml:"pool_id,omitempty"`               // For GCP WIF
	GCPProviderID   string            `yaml:"provider_id,omitempty"`           // For GCP WIF
	GCPServiceAcct  string            `yaml:"service_account_email,omitempty"` // For GCP WIF
	VaultURL        string            `yaml:"vault_url,omitempty"`             // For Key Vault
	SecretName      string            `yaml:"secret_name,omitempty"`           // For Key Vault
	AccessKeyID     string            `yaml:"access_key_id,omitempty"`         // For static credentials
	SecretAccessKey string            `yaml:"secret_access_key,omitempty"`     // For static credentials
	Extra           map[string]string `yaml:"extra,omitempty"`                 // Catch-all
}

// BackendConfig describes a single storage backend.
type BackendConfig struct {
	Name      string            `yaml:"name"`
	Type      string            `yaml:"type"`
	MountPath string            `yaml:"mount_path"`
	Config    map[string]string `yaml:"config"`
	Auth      BackendAuthConfig `yaml:"auth"`
}

// Validate checks the configuration for logical errors.
func (c *Config) Validate() error {
	if c.Cache.BlockSize < 0 {
		return fmt.Errorf("config: block_size must be positive, got %d", c.Cache.BlockSize)
	}
	if c.Cache.MaxSize < 0 {
		return fmt.Errorf("config: max_size must be positive, got %d", c.Cache.MaxSize)
	}
	if c.Cache.EvictHighWater > 0 && c.Cache.EvictLowWater > 0 &&
		c.Cache.EvictHighWater <= c.Cache.EvictLowWater {
		return fmt.Errorf("config: evict_high_water (%.2f) must be greater than evict_low_water (%.2f)",
			c.Cache.EvictHighWater, c.Cache.EvictLowWater)
	}
	if c.Cache.EvictHighWater > 1.0 || c.Cache.EvictLowWater > 1.0 {
		return fmt.Errorf("config: evict water marks must be <= 1.0")
	}
	names := make(map[string]bool)
	mountPaths := make(map[string]bool)
	for _, be := range c.Backends {
		if be.Name == "" {
			return fmt.Errorf("config: backend name cannot be empty")
		}
		if be.Type == "" {
			return fmt.Errorf("config: backend %q has empty type", be.Name)
		}
		if names[be.Name] {
			return fmt.Errorf("config: duplicate backend name %q", be.Name)
		}
		names[be.Name] = true
		mp := be.MountPath
		if mp == "" {
			mp = "/" + be.Name
		}
		if mountPaths[mp] {
			return fmt.Errorf("config: duplicate mount_path %q", mp)
		}
		mountPaths[mp] = true

		// Validate auth config per method
		if err := validateAuthConfig(be.Name, be.Auth); err != nil {
			return err
		}
	}
	return nil
}

// validateAuthConfig checks that required fields are set for each auth method.
func validateAuthConfig(backendName string, auth BackendAuthConfig) error {
	switch auth.Method {
	case "", "none":
		// No validation needed
	case "managed_identity":
		// No extra config required — uses IMDS
	case "service_principal":
		if auth.TenantID == "" {
			return fmt.Errorf("config: backend %q: service_principal requires tenant_id", backendName)
		}
		if auth.ClientIDEnv == "" {
			return fmt.Errorf("config: backend %q: service_principal requires client_id_env", backendName)
		}
		if auth.ClientSecretEnv == "" {
			return fmt.Errorf("config: backend %q: service_principal requires client_secret_env", backendName)
		}
	case "oidc_federation":
		if auth.RoleARN == "" {
			return fmt.Errorf("config: backend %q: oidc_federation requires role_arn", backendName)
		}
	case "gcp_wif":
		if auth.GCPProjectNum == "" {
			return fmt.Errorf("config: backend %q: gcp_wif requires project_number", backendName)
		}
		if auth.GCPPoolID == "" {
			return fmt.Errorf("config: backend %q: gcp_wif requires pool_id", backendName)
		}
		if auth.GCPProviderID == "" {
			return fmt.Errorf("config: backend %q: gcp_wif requires provider_id", backendName)
		}
		if auth.GCPServiceAcct == "" {
			return fmt.Errorf("config: backend %q: gcp_wif requires service_account_email", backendName)
		}
	case "static_keyvault":
		if auth.VaultURL == "" {
			return fmt.Errorf("config: backend %q: static_keyvault requires vault_url", backendName)
		}
		if auth.SecretName == "" {
			return fmt.Errorf("config: backend %q: static_keyvault requires secret_name", backendName)
		}
	case "static":
		// static credentials can be empty strings (e.g., for local testing)
	default:
		return fmt.Errorf("config: backend %q: unknown auth method %q", backendName, auth.Method)
	}
	return nil
}
