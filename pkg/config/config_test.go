package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	content := `
mount_point: /data
allow_other: true
cache:
  dir: /nvme/warpdrive-cache
  max_size: 2TB
  block_size: 4MB
  readahead_blocks: 4
  max_parallel_fetch: 16
  stale_ttl: 60s
  evict_high_water: 0.90
  evict_low_water: 0.80
backends:
  - name: azure_training
    type: azureblob
    mount_path: /training_sets
    config:
      account: mai-eastus-storage
      container: training-sets
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.MountPoint != "/data" {
		t.Errorf("MountPoint = %q, want /data", cfg.MountPoint)
	}
	if cfg.Cache.Path != "/nvme/warpdrive-cache" {
		t.Errorf("Cache.Path = %q, want /nvme/warpdrive-cache", cfg.Cache.Path)
	}
	if cfg.Cache.MaxSize != 2*1024*1024*1024*1024 {
		t.Errorf("Cache.MaxSize = %d, want 2TB", cfg.Cache.MaxSize)
	}
	if cfg.Cache.BlockSize != 4*1024*1024 {
		t.Errorf("Cache.BlockSize = %d, want 4MB", cfg.Cache.BlockSize)
	}
	if len(cfg.Backends) != 1 {
		t.Fatalf("Backends len = %d, want 1", len(cfg.Backends))
	}
	if cfg.Backends[0].Name != "azure_training" {
		t.Errorf("Backend name = %q, want azure_training", cfg.Backends[0].Name)
	}
}

func TestLoad_Defaults(t *testing.T) {
	content := `
backends: []
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.MountPoint != "/data" {
		t.Errorf("Default MountPoint = %q, want /data", cfg.MountPoint)
	}
	if cfg.Cache.ReadaheadBlocks != 4 {
		t.Errorf("Default ReadaheadBlocks = %d, want 4", cfg.Cache.ReadaheadBlocks)
	}
	if cfg.Cache.EvictHighWater != 0.90 {
		t.Errorf("Default EvictHighWater = %f, want 0.90", cfg.Cache.EvictHighWater)
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"1024", 1024},
		{"4MB", 4 * 1024 * 1024},
		{"2TB", 2 * 1024 * 1024 * 1024 * 1024},
		{"500GB", 500 * 1024 * 1024 * 1024},
		{"1KB", 1024},
	}

	for _, tt := range tests {
		got, err := ParseSize(tt.input)
		if err != nil {
			t.Errorf("ParseSize(%q) error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseSize_Invalid(t *testing.T) {
	_, err := ParseSize("invalid")
	if err == nil {
		t.Error("ParseSize(\"invalid\") should return error")
	}
}

func TestLoad_InvalidMaxSize(t *testing.T) {
	content := `
cache:
  max_size: "notasize"
backends: []
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected error for invalid max_size, got nil")
	}
	if !strings.Contains(err.Error(), "invalid cache.max_size") {
		t.Errorf("error should mention invalid cache.max_size, got: %v", err)
	}
}

func TestLoad_InvalidBlockSize(t *testing.T) {
	content := `
cache:
  block_size: "xyz"
backends: []
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(cfgPath)
	if err == nil {
		t.Fatal("expected error for invalid block_size, got nil")
	}
	if !strings.Contains(err.Error(), "invalid cache.block_size") {
		t.Errorf("error should mention invalid cache.block_size, got: %v", err)
	}
}

func TestLoad_StaleTTLDefault(t *testing.T) {
	content := `
backends: []
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Cache.StaleTTL != 60*time.Second {
		t.Errorf("Default StaleTTL = %v, want 60s", cfg.Cache.StaleTTL)
	}
}

func TestLoad_MetricsDefaults(t *testing.T) {
	content := `
backends: []
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !cfg.Metrics.MetricsEnabled() {
		t.Error("Metrics should be enabled by default")
	}
	if cfg.Metrics.Addr != ":9090" {
		t.Errorf("Metrics.Addr = %q, want :9090", cfg.Metrics.Addr)
	}
}

func TestLoad_MetricsDisabled(t *testing.T) {
	content := `
metrics:
  enabled: false
  addr: ":8080"
backends: []
`
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Metrics.MetricsEnabled() {
		t.Error("Metrics should be disabled when set to false")
	}
	if cfg.Metrics.Addr != ":8080" {
		t.Errorf("Metrics.Addr = %q, want :8080", cfg.Metrics.Addr)
	}
}

func TestValidate_DuplicateBackendName(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "dup", Type: "local", MountPath: "/a"},
			{Name: "dup", Type: "local", MountPath: "/b"},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for duplicate backend name")
	}
}

func TestValidate_DuplicateMountPath(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "a", Type: "local", MountPath: "/data"},
			{Name: "b", Type: "local", MountPath: "/data"},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for duplicate mount_path")
	}
}

func TestValidate_EmptyBackendName(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "", Type: "local", MountPath: "/data"},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty backend name")
	}
}

func TestValidate_EmptyBackendType(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "x", Type: "", MountPath: "/data"},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty backend type")
	}
}

func TestValidate_InvalidWaterMarks(t *testing.T) {
	cfg := &Config{
		Cache: CacheConfig{
			EvictHighWater: 0.70,
			EvictLowWater:  0.90,
		},
		Backends: []BackendConfig{},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when high_water <= low_water")
	}
}

func TestValidate_WaterMarkAboveOne(t *testing.T) {
	cfg := &Config{
		Cache: CacheConfig{
			EvictHighWater: 1.5,
			EvictLowWater:  0.90,
		},
		Backends: []BackendConfig{},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when water mark > 1.0")
	}
}

func TestValidate_OK(t *testing.T) {
	cfg := &Config{
		Cache: CacheConfig{
			EvictHighWater: 0.90,
			EvictLowWater:  0.80,
		},
		Backends: []BackendConfig{
			{Name: "a", Type: "local", MountPath: "/data"},
			{Name: "b", Type: "s3", MountPath: "/archive"},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate should pass, got: %v", err)
	}
}

// ─────────────────────── Auth Validation Tests ───────────────────────

func TestValidate_AuthNone(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "a", Type: "local", MountPath: "/data", Auth: BackendAuthConfig{Method: "none"}},
			{Name: "b", Type: "local", MountPath: "/other", Auth: BackendAuthConfig{Method: ""}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("none/empty auth should be valid, got: %v", err)
	}
}

func TestValidate_AuthServicePrincipal_Valid(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "az", Type: "azureblob", MountPath: "/data", Auth: BackendAuthConfig{
				Method:          "service_principal",
				TenantID:        "tenant-123",
				ClientIDEnv:     "CLIENT_ID",
				ClientSecretEnv: "CLIENT_SECRET",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid service_principal should pass, got: %v", err)
	}
}

func TestValidate_AuthServicePrincipal_MissingTenant(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "az", Type: "azureblob", MountPath: "/data", Auth: BackendAuthConfig{
				Method:          "service_principal",
				ClientIDEnv:     "CLIENT_ID",
				ClientSecretEnv: "CLIENT_SECRET",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for service_principal without tenant_id")
	}
}

func TestValidate_AuthServicePrincipal_MissingClientIDEnv(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "az", Type: "azureblob", MountPath: "/data", Auth: BackendAuthConfig{
				Method:          "service_principal",
				TenantID:        "tenant-123",
				ClientSecretEnv: "CLIENT_SECRET",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for service_principal without client_id_env")
	}
}

func TestValidate_AuthServicePrincipal_MissingClientSecretEnv(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "az", Type: "azureblob", MountPath: "/data", Auth: BackendAuthConfig{
				Method:      "service_principal",
				TenantID:    "tenant-123",
				ClientIDEnv: "CLIENT_ID",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for service_principal without client_secret_env")
	}
}

func TestValidate_AuthOIDCFederation_MissingRoleARN(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "s3", Type: "s3", MountPath: "/data", Auth: BackendAuthConfig{
				Method: "oidc_federation",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for oidc_federation without role_arn")
	}
}

func TestValidate_AuthOIDCFederation_Valid(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "s3", Type: "s3", MountPath: "/data", Auth: BackendAuthConfig{
				Method:  "oidc_federation",
				RoleARN: "arn:aws:iam::123456789:role/test",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid oidc_federation should pass, got: %v", err)
	}
}

func TestValidate_AuthGCPWIF_MissingFields(t *testing.T) {
	tests := []struct {
		name string
		auth BackendAuthConfig
	}{
		{"missing project_number", BackendAuthConfig{Method: "gcp_wif", GCPPoolID: "p", GCPProviderID: "pr", GCPServiceAcct: "sa"}},
		{"missing pool_id", BackendAuthConfig{Method: "gcp_wif", GCPProjectNum: "1", GCPProviderID: "pr", GCPServiceAcct: "sa"}},
		{"missing provider_id", BackendAuthConfig{Method: "gcp_wif", GCPProjectNum: "1", GCPPoolID: "p", GCPServiceAcct: "sa"}},
		{"missing service_account", BackendAuthConfig{Method: "gcp_wif", GCPProjectNum: "1", GCPPoolID: "p", GCPProviderID: "pr"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Backends: []BackendConfig{
					{Name: "gcs", Type: "gcs", MountPath: "/data", Auth: tt.auth},
				},
			}
			cfg.applyDefaults()
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for gcp_wif with %s", tt.name)
			}
		})
	}
}

func TestValidate_AuthGCPWIF_Valid(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "gcs", Type: "gcs", MountPath: "/data", Auth: BackendAuthConfig{
				Method:         "gcp_wif",
				GCPProjectNum:  "123456",
				GCPPoolID:      "pool-1",
				GCPProviderID:  "provider-1",
				GCPServiceAcct: "sa@proj.iam.gserviceaccount.com",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid gcp_wif should pass, got: %v", err)
	}
}

func TestValidate_AuthStaticKeyvault_MissingFields(t *testing.T) {
	tests := []struct {
		name string
		auth BackendAuthConfig
	}{
		{"missing vault_url", BackendAuthConfig{Method: "static_keyvault", SecretName: "secret"}},
		{"missing secret_name", BackendAuthConfig{Method: "static_keyvault", VaultURL: "https://kv.vault.azure.net"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Backends: []BackendConfig{
					{Name: "vast", Type: "s3", MountPath: "/data", Auth: tt.auth},
				},
			}
			cfg.applyDefaults()
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for static_keyvault with %s", tt.name)
			}
		})
	}
}

func TestValidate_AuthUnknownMethod(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "x", Type: "local", MountPath: "/data", Auth: BackendAuthConfig{
				Method: "unsupported_method",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for unknown auth method")
	}
}

func TestValidate_AuthManagedIdentity(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "az", Type: "azureblob", MountPath: "/data", Auth: BackendAuthConfig{
				Method: "managed_identity",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("managed_identity should be valid with no extra fields, got: %v", err)
	}
}

func TestValidate_AuthStatic(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "dev", Type: "s3", MountPath: "/data", Auth: BackendAuthConfig{
				Method:          "static",
				AccessKeyID:     "AKIAEXAMPLE",
				SecretAccessKey: "secret123",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("static auth should be valid, got: %v", err)
	}
}

func TestValidate_AuthStaticKeyvault_Valid(t *testing.T) {
	cfg := &Config{
		Backends: []BackendConfig{
			{Name: "vast", Type: "s3", MountPath: "/data", Auth: BackendAuthConfig{
				Method:     "static_keyvault",
				VaultURL:   "https://my-vault.vault.azure.net",
				SecretName: "s3-creds",
			}},
		},
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Errorf("valid static_keyvault should pass, got: %v", err)
	}
}
