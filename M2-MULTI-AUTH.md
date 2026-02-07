# M2: Multi-Backend + Auth — Technical Spec

## Timeline: Weeks 5-12 (overlaps M1 by 2 weeks)

## Goal

Multiple storage backends mounted simultaneously in a unified namespace. Auth federation so GPU nodes can access Azure Blob (same tenant), AWS S3, GCS, and on-prem storage (Vast, Nebius) without distributing static credentials.

**Out of scope for M2:** Cross-tenant Azure Blob access and SFI compliance. These are deferred to a future milestone once the core auth patterns are proven.

---

## M2.1: Additional rclone Backends (Weeks 5-7)

### Design

M1 proved the architecture with Azure Blob. Now we validate and enable: AWS S3, Google Cloud Storage, Vast (S3-compatible), Nebius (S3-compatible), and on-prem NFS.

Each backend gets an integration test suite that runs against a real account. The rclone backend abstraction from M1 should handle these without code changes — we're validating config and edge cases.

### Implementation

No new code files. Extend `NewRcloneBackend` to accept all rclone config params. Add blank imports for new backend types:

```go
// pkg/backend/rclone.go — add imports
import (
    _ "github.com/rclone/rclone/backend/azureblob"
    _ "github.com/rclone/rclone/backend/s3"
    _ "github.com/rclone/rclone/backend/googlecloudstorage"
    _ "github.com/rclone/rclone/backend/local"
    _ "github.com/rclone/rclone/backend/sftp"       // For on-prem NFS via SFTP
)
```

### Validation Matrix

Each backend must pass:

| Test | S3 | GCS | Vast (S3) | Nebius (S3) | SFTP/NFS |
|------|----|-----|-----------|-------------|----------|
| List 10K objects | ✓ | ✓ | ✓ | ✓ | ✓ |
| ReadAt 4MB at offset 0 | ✓ | ✓ | ✓ | ✓ | ✓ |
| ReadAt 4MB at offset 1GB | ✓ | ✓ | ✓ | ✓ | ✓ |
| Read 10GB file sequentially | ✓ | ✓ | ✓ | ✓ | ✓ |
| Stat existing file | ✓ | ✓ | ✓ | ✓ | ✓ |
| Stat non-existent file → ErrNotFound | ✓ | ✓ | ✓ | ✓ | ✓ |
| List empty prefix → empty result | ✓ | ✓ | ✓ | ✓ | ✓ |
| Concurrent 32 reads | ✓ | ✓ | ✓ | ✓ | ✓ |
| Error handling (network timeout) | ✓ | ✓ | ✓ | ✓ | ✓ |

### Backend-Specific Config

```yaml
# AWS S3
- name: s3_training
  type: s3
  mount_path: /s3_data
  config:
    provider: AWS           # rclone s3 provider
    bucket: my-training-data
    region: us-east-1
    # auth handled separately in auth section

# GCS
- name: gcs_shared
  type: googlecloudstorage
  mount_path: /gcs_data
  config:
    bucket: my-gcs-bucket
    project_number: "123456789"

# Vast Data (S3-compatible)
- name: vast_onprem
  type: s3
  mount_path: /vast
  config:
    provider: Other
    endpoint: https://vast.internal.company.com
    bucket: ml-data
    force_path_style: true   # Vast requires path-style

# Nebius
- name: nebius_storage
  type: s3
  mount_path: /nebius
  config:
    provider: Other
    endpoint: https://storage.ai.nebius.cloud
    bucket: training-data
    region: eu-north1
```

---

## M2.2: Unified Namespace (Weeks 6-8)

### Design

The root FUSE directory presents one entry per configured backend `mount_path`. Each entry delegates to the corresponding backend. Backends are independent — failure of one doesn't affect others.

### Implementation: `pkg/namespace/namespace.go`

```go
package namespace

import (
    "fmt"
    "sort"
    "strings"
)

// BackendMount maps a mount path to a backend name.
type BackendMount struct {
    MountPath   string // e.g., "/training_sets"
    BackendName string // e.g., "azure_training"
}

// Namespace resolves filesystem paths to backend + remote path.
type Namespace struct {
    mounts []BackendMount // Sorted by MountPath length descending (longest match first)
}

// New creates a namespace from backend configs.
func New(mounts []BackendMount) *Namespace {
    // Sort mounts by path length descending for longest-prefix matching
    sorted := make([]BackendMount, len(mounts))
    copy(sorted, mounts)
    sort.Slice(sorted, func(i, j int) bool {
        return len(sorted[i].MountPath) > len(sorted[j].MountPath)
    })
    return &Namespace{mounts: sorted}
}

// Resolve maps an absolute path to a backend name and remote path.
// path is relative to mount root, e.g., "training_sets/imagenet/shard-0000.tar"
// Returns: backendName, remotePath within backend, error
func (ns *Namespace) Resolve(fsPath string) (string, string, error) {
    fsPath = "/" + strings.TrimPrefix(fsPath, "/")

    for _, m := range ns.mounts {
        if strings.HasPrefix(fsPath, m.MountPath+"/") {
            remotePath := strings.TrimPrefix(fsPath, m.MountPath+"/")
            return m.BackendName, remotePath, nil
        }
        if fsPath == m.MountPath {
            return m.BackendName, "", nil
        }
    }
    return "", "", fmt.Errorf("namespace.Resolve: no backend for path %s", fsPath)
}

// MountPoints returns top-level directory names (for root Readdir).
func (ns *Namespace) MountPoints() []string {
    var points []string
    for _, m := range ns.mounts {
        // Extract first path component
        name := strings.TrimPrefix(m.MountPath, "/")
        if idx := strings.Index(name, "/"); idx > 0 {
            name = name[:idx]
        }
        points = append(points, name)
    }
    return unique(points)
}
```

### Backend Isolation

```go
// In pkg/fuse/dir.go — Readdir for root
func (r *WarpDriveRoot) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
    entries := make([]fuse.DirEntry, 0)
    for _, mp := range r.ns.MountPoints() {
        entries = append(entries, fuse.DirEntry{
            Name: mp,
            Mode: syscall.S_IFDIR,
        })
    }
    return gofuse.NewListDirStream(entries), 0
}

// In pkg/fuse/dir.go — Readdir for backend directory
func (d *WarpDriveDir) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
    b, ok := d.root.backends.Get(d.backendName)
    if !ok {
        return nil, syscall.EIO
    }

    objects, err := b.List(ctx, d.remotePath)
    if err != nil {
        // Backend failure: return EIO but don't crash other mounts
        slog.Error("Backend List failed", "backend", d.backendName, "error", err)
        return nil, syscall.EIO
    }

    // Convert to fuse.DirEntry slice
    // ...
}
```

---

## M2.3: Auth Framework + Providers (Weeks 7-12)

### Design

WarpDrive needs to authenticate to multiple cloud providers without static credentials on GPU nodes. The auth strategy per backend type:

| Backend | Auth Method | Flow |
|---------|-------------|------|
| Azure Blob (same tenant) | Managed Identity | IMDS → access token for storage.azure.com |
| Azure Blob (service principal) | Client credentials | Client ID + secret/cert → Entra token |
| AWS S3 | OIDC Federation | Entra JWT → AWS STS AssumeRoleWithWebIdentity → temp creds |
| GCS | Workload Identity Federation | Entra JWT → GCP STS → impersonate SA → access token |
| Vast / Nebius (S3-compat) | Key Vault static creds | Entra token → Key Vault → retrieve S3 access/secret key |
| NFS / SFTP | Key Vault static creds | Entra token → Key Vault → retrieve SSH key / password |

All providers share a common interface. The `AuthManager` caches tokens and auto-refreshes before expiry.

### Config Schema (auth section only)

```yaml
backends:
  - name: azure_training
    type: azureblob
    mount_path: /training_sets
    config:
      account: mai-eastus-storage
      container: training-sets
    auth:
      method: managed_identity   # Uses Azure IMDS on the node

  - name: azure_checkpoints
    type: azureblob
    mount_path: /checkpoints
    config:
      account: mai-eastus-storage
      container: checkpoints
    auth:
      method: service_principal
      client_id_env: AZURE_CLIENT_ID        # Read from env var
      client_secret_env: AZURE_CLIENT_SECRET # Read from env var
      tenant_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

  - name: s3_datasets
    type: s3
    mount_path: /datasets
    config:
      bucket: company-ml-datasets
      region: us-east-1
    auth:
      method: oidc_federation    # Entra JWT → AWS STS
      role_arn: arn:aws:iam::123456789:role/warpdrive-access

  - name: gcs_shared
    type: googlecloudstorage
    mount_path: /gcs_data
    config:
      bucket: my-gcs-bucket
    auth:
      method: gcp_wif            # Entra JWT → GCP Workload Identity
      project_number: "123456789"
      pool_id: warpdrive-pool
      provider_id: entra-provider
      service_account_email: warpdrive@project.iam.gserviceaccount.com

  - name: vast_archive
    type: s3
    mount_path: /archive
    config:
      endpoint: https://vast.internal.company.com
      bucket: ml-archive
    auth:
      method: static_keyvault
      vault_url: https://warpdrive-vault.vault.azure.net
      secret_name: vast-s3-creds  # JSON: {"access_key":"...","secret_key":"..."}
```

### Interfaces: `pkg/auth/auth.go`

```go
package auth

import (
    "context"
    "fmt"
    "sync"
    "time"
)

// Credentials represents resolved credentials for a backend.
type Credentials struct {
    // For Azure Blob
    AccessToken string

    // For AWS S3
    AccessKeyID    string
    SecretAccessKey string
    SessionToken   string

    // For static (Vast, Nebius, NFS)
    StaticKey    string
    StaticSecret string

    // Common
    ExpiresAt time.Time
    Provider  string // "managed_identity", "service_principal", "aws_sts", "gcp_wif", "keyvault"
}

// IsExpired returns true if credentials have expired or will expire within the buffer.
func (c *Credentials) IsExpired(buffer time.Duration) bool {
    if c.ExpiresAt.IsZero() {
        return false // Static creds don't expire
    }
    return time.Now().Add(buffer).After(c.ExpiresAt)
}

// Provider resolves credentials for a specific backend type.
type Provider interface {
    // Name returns the provider name (e.g., "managed_identity").
    Name() string

    // Resolve returns current valid credentials for the given backend.
    // Handles token acquisition, refresh, and federation.
    Resolve(ctx context.Context, backendName string, cfg BackendAuthConfig) (*Credentials, error)

    // SupportsMethod returns true if this provider handles the given auth method.
    SupportsMethod(method string) bool
}

// BackendAuthConfig is the auth section from a backend's config.
type BackendAuthConfig struct {
    Method              string            // managed_identity, service_principal, oidc_federation, gcp_wif, static_keyvault
    TenantID            string            // For service_principal
    ClientIDEnv         string            // Env var name for client ID
    ClientSecretEnv     string            // Env var name for client secret
    RoleARN             string            // For AWS OIDC federation
    GCPProjectNumber    string            // For GCP WIF
    GCPPoolID           string            // For GCP WIF
    GCPProviderID       string            // For GCP WIF
    GCPServiceAccount   string            // For GCP WIF
    VaultURL            string            // For Key Vault
    SecretName          string            // For Key Vault
    Extra               map[string]string // Catch-all for future options
}

// Manager orchestrates auth across all providers.
type Manager struct {
    providers []Provider
    cache     sync.Map // backendName -> *Credentials
    audit     *AuditLogger

    // Backend auth configs (loaded from main config)
    backendCfgs map[string]BackendAuthConfig

    // Refresh buffer: request new creds this far before expiry
    refreshBuffer time.Duration // Default 5 minutes
}

func NewManager(audit *AuditLogger) *Manager {
    return &Manager{
        audit:         audit,
        backendCfgs:   make(map[string]BackendAuthConfig),
        refreshBuffer: 5 * time.Minute,
    }
}

func (m *Manager) RegisterProvider(p Provider) {
    m.providers = append(m.providers, p)
}

func (m *Manager) SetBackendAuth(backendName string, cfg BackendAuthConfig) {
    m.backendCfgs[backendName] = cfg
}

// GetCredentials returns valid credentials for the named backend.
// Caches and auto-refreshes tokens.
func (m *Manager) GetCredentials(ctx context.Context, backendName string) (*Credentials, error) {
    // 1. Check cache
    if cached, ok := m.cache.Load(backendName); ok {
        creds := cached.(*Credentials)
        if !creds.IsExpired(m.refreshBuffer) {
            return creds, nil
        }
    }

    // 2. Find backend auth config
    cfg, ok := m.backendCfgs[backendName]
    if !ok {
        return nil, fmt.Errorf("auth.GetCredentials: no auth config for backend %s", backendName)
    }

    // 3. Find matching provider
    for _, p := range m.providers {
        if p.SupportsMethod(cfg.Method) {
            creds, err := p.Resolve(ctx, backendName, cfg)
            if err != nil {
                m.audit.Log(AuditEntry{
                    Timestamp:   time.Now(),
                    BackendName: backendName,
                    Provider:    p.Name(),
                    Success:     false,
                    Error:       err.Error(),
                })
                return nil, fmt.Errorf("auth.GetCredentials: provider %s: %w", p.Name(), err)
            }

            // 4. Cache
            m.cache.Store(backendName, creds)

            // 5. Audit
            m.audit.Log(AuditEntry{
                Timestamp:   time.Now(),
                BackendName: backendName,
                Provider:    p.Name(),
                Success:     true,
            })

            return creds, nil
        }
    }

    return nil, fmt.Errorf("auth.GetCredentials: no provider supports method %q for backend %s", cfg.Method, backendName)
}
```

### Azure Managed Identity Provider: `pkg/auth/entra.go`

```go
package auth

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

// EntraProvider handles Azure Entra ID auth (managed identity + service principal).
type EntraProvider struct {
    client *http.Client
}

func NewEntraProvider() *EntraProvider {
    return &EntraProvider{
        client: &http.Client{Timeout: 30 * time.Second},
    }
}

func (p *EntraProvider) Name() string { return "entra" }

func (p *EntraProvider) SupportsMethod(method string) bool {
    return method == "managed_identity" || method == "service_principal"
}

func (p *EntraProvider) Resolve(ctx context.Context, backendName string, cfg BackendAuthConfig) (*Credentials, error) {
    switch cfg.Method {
    case "managed_identity":
        return p.resolveManagedIdentity(ctx)
    case "service_principal":
        return p.resolveServicePrincipal(ctx, cfg)
    default:
        return nil, fmt.Errorf("entra: unsupported method %s", cfg.Method)
    }
}

// resolveManagedIdentity gets a token from Azure Instance Metadata Service (IMDS).
// Works on Azure VMs, VMSS, and AKS pods with managed identity assigned.
func (p *EntraProvider) resolveManagedIdentity(ctx context.Context) (*Credentials, error) {
    imdsURL := "http://169.254.169.254/metadata/identity/oauth2/token"
    params := url.Values{
        "api-version": {"2018-02-01"},
        "resource":    {"https://storage.azure.com/"},
    }

    req, err := http.NewRequestWithContext(ctx, "GET", imdsURL+"?"+params.Encode(), nil)
    if err != nil {
        return nil, fmt.Errorf("entra.managedIdentity: build request: %w", err)
    }
    req.Header.Set("Metadata", "true")

    resp, err := p.client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("entra.managedIdentity: IMDS request: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("entra.managedIdentity: IMDS returned %d", resp.StatusCode)
    }

    var result struct {
        AccessToken string `json:"access_token"`
        ExpiresIn   string `json:"expires_in"`
        ExpiresOn   string `json:"expires_on"` // Unix timestamp
        Resource    string `json:"resource"`
        TokenType   string `json:"token_type"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("entra.managedIdentity: decode response: %w", err)
    }

    // Parse expires_on (Unix timestamp string)
    expiresOnSec, _ := strconv.ParseInt(result.ExpiresOn, 10, 64)
    expiresAt := time.Unix(expiresOnSec, 0)

    return &Credentials{
        AccessToken: result.AccessToken,
        ExpiresAt:   expiresAt,
        Provider:    "managed_identity",
    }, nil
}

// resolveServicePrincipal gets a token using client credentials flow.
// Client ID and secret are read from environment variables specified in config.
func (p *EntraProvider) resolveServicePrincipal(ctx context.Context, cfg BackendAuthConfig) (*Credentials, error) {
    clientID := os.Getenv(cfg.ClientIDEnv)
    clientSecret := os.Getenv(cfg.ClientSecretEnv)
    if clientID == "" || clientSecret == "" {
        return nil, fmt.Errorf("entra.servicePrincipal: env vars %s/%s not set", cfg.ClientIDEnv, cfg.ClientSecretEnv)
    }

    tokenURL := fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", cfg.TenantID)
    data := url.Values{
        "grant_type":    {"client_credentials"},
        "client_id":     {clientID},
        "client_secret": {clientSecret},
        "scope":         {"https://storage.azure.com/.default"},
    }

    req, err := http.NewRequestWithContext(ctx, "POST", tokenURL, strings.NewReader(data.Encode()))
    if err != nil {
        return nil, fmt.Errorf("entra.servicePrincipal: build request: %w", err)
    }
    req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

    resp, err := p.client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("entra.servicePrincipal: token request: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("entra.servicePrincipal: token endpoint returned %d", resp.StatusCode)
    }

    var result struct {
        AccessToken string `json:"access_token"`
        ExpiresIn   int    `json:"expires_in"` // Seconds
        TokenType   string `json:"token_type"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, fmt.Errorf("entra.servicePrincipal: decode response: %w", err)
    }

    return &Credentials{
        AccessToken: result.AccessToken,
        ExpiresAt:   time.Now().Add(time.Duration(result.ExpiresIn) * time.Second),
        Provider:    "service_principal",
    }, nil
}
```

### AWS STS Federation Provider: `pkg/auth/aws.go`

```go
package auth

import (
    "context"
    "encoding/xml"
    "fmt"
    "net/http"
    "net/url"
    "strings"
    "time"
)

// AWSProvider federates Entra ID tokens to AWS STS temporary credentials.
// Flow: Managed Identity JWT → AWS STS AssumeRoleWithWebIdentity → temp AWS creds
type AWSProvider struct {
    entra  *EntraProvider // To get the initial Entra JWT for federation
    client *http.Client
}

func NewAWSProvider(entra *EntraProvider) *AWSProvider {
    return &AWSProvider{
        entra:  entra,
        client: &http.Client{Timeout: 30 * time.Second},
    }
}

func (p *AWSProvider) Name() string                      { return "aws_sts" }
func (p *AWSProvider) SupportsMethod(method string) bool { return method == "oidc_federation" }

func (p *AWSProvider) Resolve(ctx context.Context, backendName string, cfg BackendAuthConfig) (*Credentials, error) {
    // 1. Get Entra ID token (JWT) — this becomes the web identity token for AWS
    entraToken, err := p.entra.resolveManagedIdentity(ctx)
    if err != nil {
        return nil, fmt.Errorf("aws.Resolve: get entra token: %w", err)
    }

    // 2. Call AWS STS AssumeRoleWithWebIdentity
    stsURL := "https://sts.amazonaws.com/"
    data := url.Values{
        "Action":           {"AssumeRoleWithWebIdentity"},
        "Version":          {"2011-06-15"},
        "RoleArn":          {cfg.RoleARN},
        "WebIdentityToken": {entraToken.AccessToken},
        "RoleSessionName":  {fmt.Sprintf("warpdrive-%s", backendName)},
        "DurationSeconds":  {"3600"},
    }

    req, err := http.NewRequestWithContext(ctx, "POST", stsURL, strings.NewReader(data.Encode()))
    if err != nil {
        return nil, fmt.Errorf("aws.Resolve: build STS request: %w", err)
    }
    req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

    resp, err := p.client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("aws.Resolve: STS request: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("aws.Resolve: STS returned %d", resp.StatusCode)
    }

    // 3. Parse XML response
    var stsResp struct {
        XMLName xml.Name `xml:"AssumeRoleWithWebIdentityResponse"`
        Result  struct {
            Credentials struct {
                AccessKeyId     string `xml:"AccessKeyId"`
                SecretAccessKey string `xml:"SecretAccessKey"`
                SessionToken    string `xml:"SessionToken"`
                Expiration      string `xml:"Expiration"` // ISO 8601
            } `xml:"Credentials"`
        } `xml:"AssumeRoleWithWebIdentityResult"`
    }

    if err := xml.NewDecoder(resp.Body).Decode(&stsResp); err != nil {
        return nil, fmt.Errorf("aws.Resolve: decode STS response: %w", err)
    }

    expiry, _ := time.Parse(time.RFC3339, stsResp.Result.Credentials.Expiration)

    return &Credentials{
        AccessKeyID:    stsResp.Result.Credentials.AccessKeyId,
        SecretAccessKey: stsResp.Result.Credentials.SecretAccessKey,
        SessionToken:   stsResp.Result.Credentials.SessionToken,
        ExpiresAt:      expiry,
        Provider:       "aws_sts",
    }, nil
}
```

### GCP Workload Identity Federation Provider: `pkg/auth/gcp.go`

```go
package auth

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "strings"
    "time"
)

// GCPProvider federates Entra ID tokens to GCP access tokens via Workload Identity Federation.
// Flow: Managed Identity JWT → GCP STS token exchange → SA impersonation → access token
type GCPProvider struct {
    entra  *EntraProvider
    client *http.Client
}

func NewGCPProvider(entra *EntraProvider) *GCPProvider {
    return &GCPProvider{
        entra:  entra,
        client: &http.Client{Timeout: 30 * time.Second},
    }
}

func (p *GCPProvider) Name() string                      { return "gcp_wif" }
func (p *GCPProvider) SupportsMethod(method string) bool { return method == "gcp_wif" }

func (p *GCPProvider) Resolve(ctx context.Context, backendName string, cfg BackendAuthConfig) (*Credentials, error) {
    // 1. Get Entra token
    entraToken, err := p.entra.resolveManagedIdentity(ctx)
    if err != nil {
        return nil, fmt.Errorf("gcp.Resolve: get entra token: %w", err)
    }

    // 2. Exchange Entra JWT for GCP STS federation token
    audience := fmt.Sprintf(
        "//iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s/providers/%s",
        cfg.GCPProjectNumber, cfg.GCPPoolID, cfg.GCPProviderID,
    )

    stsURL := "https://sts.googleapis.com/v1/token"
    stsData := url.Values{
        "grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
        "subject_token":        {entraToken.AccessToken},
        "subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
        "requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
        "audience":             {audience},
        "scope":                {"https://www.googleapis.com/auth/cloud-platform"},
    }

    req, err := http.NewRequestWithContext(ctx, "POST", stsURL, strings.NewReader(stsData.Encode()))
    if err != nil {
        return nil, fmt.Errorf("gcp.Resolve: build STS request: %w", err)
    }
    req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

    resp, err := p.client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("gcp.Resolve: GCP STS request: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("gcp.Resolve: GCP STS returned %d", resp.StatusCode)
    }

    var stsResp struct {
        AccessToken string `json:"access_token"`
        ExpiresIn   int    `json:"expires_in"`
        TokenType   string `json:"token_type"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&stsResp); err != nil {
        return nil, fmt.Errorf("gcp.Resolve: decode STS response: %w", err)
    }

    // 3. Impersonate service account to get a scoped access token
    saURL := fmt.Sprintf(
        "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
        cfg.GCPServiceAccount,
    )
    saBody := `{"scope":["https://www.googleapis.com/auth/devstorage.read_only"],"lifetime":"3600s"}`

    saReq, err := http.NewRequestWithContext(ctx, "POST", saURL, strings.NewReader(saBody))
    if err != nil {
        return nil, fmt.Errorf("gcp.Resolve: build SA request: %w", err)
    }
    saReq.Header.Set("Content-Type", "application/json")
    saReq.Header.Set("Authorization", "Bearer "+stsResp.AccessToken)

    saResp, err := p.client.Do(saReq)
    if err != nil {
        return nil, fmt.Errorf("gcp.Resolve: SA impersonation request: %w", err)
    }
    defer saResp.Body.Close()

    if saResp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("gcp.Resolve: SA impersonation returned %d", saResp.StatusCode)
    }

    var saResult struct {
        AccessToken string `json:"accessToken"`
        ExpireTime  string `json:"expireTime"` // RFC3339
    }
    if err := json.NewDecoder(saResp.Body).Decode(&saResult); err != nil {
        return nil, fmt.Errorf("gcp.Resolve: decode SA response: %w", err)
    }

    expiry, _ := time.Parse(time.RFC3339, saResult.ExpireTime)

    return &Credentials{
        AccessToken: saResult.AccessToken,
        ExpiresAt:   expiry,
        Provider:    "gcp_wif",
    }, nil
}
```

### Key Vault for Static Credentials: `pkg/auth/keyvault.go`

```go
package auth

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "time"
)

// KeyVaultProvider retrieves static credentials (S3 keys, SSH keys) from Azure Key Vault.
// Used for Vast, Nebius, NFS — systems that don't support OIDC federation.
// Authenticates to Key Vault using the node's managed identity.
type KeyVaultProvider struct {
    client *http.Client
}

func NewKeyVaultProvider() *KeyVaultProvider {
    return &KeyVaultProvider{
        client: &http.Client{Timeout: 30 * time.Second},
    }
}

func (p *KeyVaultProvider) Name() string                      { return "keyvault" }
func (p *KeyVaultProvider) SupportsMethod(method string) bool { return method == "static_keyvault" }

func (p *KeyVaultProvider) Resolve(ctx context.Context, backendName string, cfg BackendAuthConfig) (*Credentials, error) {
    // 1. Get managed identity token for Key Vault resource
    kvToken, err := p.getKeyVaultToken(ctx)
    if err != nil {
        return nil, fmt.Errorf("keyvault.Resolve: get KV token: %w", err)
    }

    // 2. Retrieve secret from Key Vault
    secretURL := fmt.Sprintf("%s/secrets/%s?api-version=7.4", cfg.VaultURL, cfg.SecretName)
    req, err := http.NewRequestWithContext(ctx, "GET", secretURL, nil)
    if err != nil {
        return nil, fmt.Errorf("keyvault.Resolve: build request: %w", err)
    }
    req.Header.Set("Authorization", "Bearer "+kvToken)

    resp, err := p.client.Do(req)
    if err != nil {
        return nil, fmt.Errorf("keyvault.Resolve: KV request: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("keyvault.Resolve: KV returned %d", resp.StatusCode)
    }

    var kvResp struct {
        Value string `json:"value"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&kvResp); err != nil {
        return nil, fmt.Errorf("keyvault.Resolve: decode KV response: %w", err)
    }

    // 3. Parse secret value as JSON containing access key and secret key
    var secretData struct {
        AccessKey string `json:"access_key"`
        SecretKey string `json:"secret_key"`
    }
    if err := json.Unmarshal([]byte(kvResp.Value), &secretData); err != nil {
        return nil, fmt.Errorf("keyvault.Resolve: parse secret JSON: %w", err)
    }

    return &Credentials{
        StaticKey:    secretData.AccessKey,
        StaticSecret: secretData.SecretKey,
        ExpiresAt:    time.Now().Add(30 * time.Minute), // Re-fetch every 30min to pick up rotations
        Provider:     "keyvault",
    }, nil
}

// getKeyVaultToken gets a managed identity token scoped to Azure Key Vault.
func (p *KeyVaultProvider) getKeyVaultToken(ctx context.Context) (string, error) {
    imdsURL := "http://169.254.169.254/metadata/identity/oauth2/token"
    params := url.Values{
        "api-version": {"2018-02-01"},
        "resource":    {"https://vault.azure.net"},
    }

    req, err := http.NewRequestWithContext(ctx, "GET", imdsURL+"?"+params.Encode(), nil)
    if err != nil {
        return "", err
    }
    req.Header.Set("Metadata", "true")

    resp, err := p.client.Do(req)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return "", fmt.Errorf("IMDS returned %d for Key Vault token", resp.StatusCode)
    }

    var result struct {
        AccessToken string `json:"access_token"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return "", err
    }

    return result.AccessToken, nil
}
```

### Audit Logger: `pkg/auth/audit.go`

```go
package auth

import (
    "log/slog"
    "sync"
    "time"
)

// AuditEntry records a credential resolution event.
type AuditEntry struct {
    Timestamp   time.Time `json:"ts"`
    BackendName string    `json:"backend"`
    Provider    string    `json:"provider"`
    UserID      string    `json:"user,omitempty"`
    Success     bool      `json:"success"`
    Error       string    `json:"error,omitempty"`
}

// AuditLogger records all credential operations in a ring buffer.
type AuditLogger struct {
    mu      sync.Mutex
    entries []AuditEntry
    maxSize int
    sink    func(AuditEntry) // Optional external sink (e.g., telemetry emitter)
}

func NewAuditLogger(maxSize int, sink func(AuditEntry)) *AuditLogger {
    return &AuditLogger{
        entries: make([]AuditEntry, 0, maxSize),
        maxSize: maxSize,
        sink:    sink,
    }
}

func (al *AuditLogger) Log(entry AuditEntry) {
    al.mu.Lock()
    defer al.mu.Unlock()

    al.entries = append(al.entries, entry)
    if len(al.entries) > al.maxSize {
        al.entries = al.entries[len(al.entries)-al.maxSize:]
    }

    // Structured log for external aggregation
    if entry.Success {
        slog.Info("Auth credential resolved",
            "backend", entry.BackendName,
            "provider", entry.Provider)
    } else {
        slog.Warn("Auth credential failed",
            "backend", entry.BackendName,
            "provider", entry.Provider,
            "error", entry.Error)
    }

    if al.sink != nil {
        al.sink(entry)
    }
}

// Recent returns the last N entries.
func (al *AuditLogger) Recent(limit int) []AuditEntry {
    al.mu.Lock()
    defer al.mu.Unlock()

    if limit > len(al.entries) {
        limit = len(al.entries)
    }
    result := make([]AuditEntry, limit)
    copy(result, al.entries[len(al.entries)-limit:])
    return result
}
```

### Integrating Auth with Backends

```go
// pkg/backend/authenticated.go

package backend

import (
    "context"
    "fmt"
    "io"
    "sync"
    "time"

    "github.com/warpdrive/warpdrive/pkg/auth"
)

// AuthenticatedBackend wraps a Backend with credential refresh before each operation.
type AuthenticatedBackend struct {
    inner     *RcloneBackend
    authMgr   *auth.Manager

    mu        sync.Mutex
    lastCreds *auth.Credentials
}

func NewAuthenticatedBackend(inner *RcloneBackend, authMgr *auth.Manager) *AuthenticatedBackend {
    return &AuthenticatedBackend{
        inner:   inner,
        authMgr: authMgr,
    }
}

// refreshIfNeeded checks credentials and updates the rclone backend's auth.
func (ab *AuthenticatedBackend) refreshIfNeeded(ctx context.Context) error {
    ab.mu.Lock()
    defer ab.mu.Unlock()

    // Skip refresh if current creds are still valid
    if ab.lastCreds != nil && !ab.lastCreds.IsExpired(2*time.Minute) {
        return nil
    }

    creds, err := ab.authMgr.GetCredentials(ctx, ab.inner.Name())
    if err != nil {
        return fmt.Errorf("auth refresh for %s: %w", ab.inner.Name(), err)
    }

    // Update the rclone backend's credentials
    // This depends on backend type:
    //   azureblob: set access_token in rclone config
    //   s3: set access_key_id, secret_access_key, session_token
    //   gcs: set access_token
    ab.inner.UpdateCredentials(creds)
    ab.lastCreds = creds
    return nil
}

func (ab *AuthenticatedBackend) Name() string { return ab.inner.Name() }
func (ab *AuthenticatedBackend) Type() string { return ab.inner.Type() }

func (ab *AuthenticatedBackend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
    if err := ab.refreshIfNeeded(ctx); err != nil {
        return nil, err
    }
    return ab.inner.List(ctx, prefix)
}

func (ab *AuthenticatedBackend) Stat(ctx context.Context, path string) (ObjectInfo, error) {
    if err := ab.refreshIfNeeded(ctx); err != nil {
        return ObjectInfo{}, err
    }
    return ab.inner.Stat(ctx, path)
}

func (ab *AuthenticatedBackend) ReadAt(ctx context.Context, path string, p []byte, off int64) (int, error) {
    if err := ab.refreshIfNeeded(ctx); err != nil {
        return 0, err
    }
    return ab.inner.ReadAt(ctx, path, p, off)
}

func (ab *AuthenticatedBackend) Open(ctx context.Context, path string) (io.ReadCloser, error) {
    if err := ab.refreshIfNeeded(ctx); err != nil {
        return nil, err
    }
    return ab.inner.Open(ctx, path)
}

func (ab *AuthenticatedBackend) Write(ctx context.Context, path string, r io.Reader, size int64) error {
    if err := ab.refreshIfNeeded(ctx); err != nil {
        return err
    }
    return ab.inner.Write(ctx, path, r, size)
}

func (ab *AuthenticatedBackend) Delete(ctx context.Context, path string) error {
    if err := ab.refreshIfNeeded(ctx); err != nil {
        return err
    }
    return ab.inner.Delete(ctx, path)
}

func (ab *AuthenticatedBackend) Close() error {
    return ab.inner.Close()
}
```

### Wiring in main: `cmd/warpdrive-mount/main.go` (updated)

```go
func main() {
    cfg, _ := config.Load(*configPath)

    // 1. Create auth manager
    auditLog := auth.NewAuditLogger(10000, nil)
    authMgr := auth.NewManager(auditLog)

    // Register auth providers
    entraProvider := auth.NewEntraProvider()
    authMgr.RegisterProvider(entraProvider)
    authMgr.RegisterProvider(auth.NewAWSProvider(entraProvider))
    authMgr.RegisterProvider(auth.NewGCPProvider(entraProvider))
    authMgr.RegisterProvider(auth.NewKeyVaultProvider())

    // 2. Create backend registry with auth wrapping
    reg := backend.NewRegistry()
    for _, bcfg := range cfg.Backends {
        // Create base rclone backend
        inner, err := backend.NewRcloneBackend(bcfg.Name, bcfg.Type, bcfg.Config)
        if err != nil {
            slog.Error("Failed to create backend", "name", bcfg.Name, "error", err)
            os.Exit(1)
        }

        // Set auth config
        authMgr.SetBackendAuth(bcfg.Name, bcfg.Auth)

        // Wrap with auth
        authed := backend.NewAuthenticatedBackend(inner, authMgr)
        reg.Register(bcfg.Name, authed)
    }

    // 3. Rest of startup (cache, namespace, FUSE) unchanged...
}
```

---

## COPILOT PROMPT — M2.2: Unified Namespace

```
You are extending the FUSE filesystem for WarpDrive to support multiple backends simultaneously.

Project: Go module at github.com/warpdrive/warpdrive
Package: pkg/namespace/ and updates to pkg/fuse/

## What to build

A namespace layer that presents multiple storage backends as subdirectories under a single mount point. Backend failures are isolated.

## Files to create/modify

1. `pkg/namespace/namespace.go`:
   - BackendMount struct: MountPath (string, e.g. "/training_sets"), BackendName (string)
   - Namespace struct: sorted list of mounts (longest prefix first)
   - New(mounts []BackendMount) *Namespace
   - Resolve(fsPath string) (backendName, remotePath string, err error): longest-prefix match
   - MountPoints() []string: unique top-level directory names
   - IsRootPath(fsPath string) bool: true if path is exactly a mount point

2. Modify `pkg/fuse/fs.go`:
   - WarpDriveRoot.Readdir: return mount points as directory entries
   - WarpDriveRoot.Lookup: if name matches a mount point, return WarpDriveDir for that backend
   - Add health tracking: if a backend fails 3 consecutive operations, mark as degraded
   - Degraded backends: Readdir shows entry but operations return EIO with slog.Warn

3. Modify `pkg/fuse/dir.go`:
   - WarpDriveDir.Readdir: delegate to the specific backend's List method
   - On backend error: return syscall.EIO, log error, do NOT crash or affect other backends

4. `pkg/namespace/namespace_test.go`:
   - Test resolve with multiple backends
   - Test longest-prefix matching
   - Test path not matching any backend → error
   - Test mount point listing

## Key constraints
- Backends MUST be independent. Panic in one backend's goroutine must be recovered, not crash the process.
- Cache namespace is per-backend: cache keys include backendName
- Root directory always responds even if all backends are down
- If a backend is unreachable, its directory appears in ls but accessing files returns EIO
```

---

## COPILOT PROMPT — M2.3: Auth Framework + Providers

```
You are building the credential management system for WarpDrive, a cross-cloud data fabric.

Project: Go module at github.com/warpdrive/warpdrive
Package: pkg/auth/

## What to build

Transparent credential management for multiple cloud storage backends. GPU nodes get credentials via Azure Managed Identity and federate to AWS and GCP. Static credentials for on-prem systems come from Azure Key Vault.

NO cross-tenant Azure access in this milestone. All Azure Blob access is same-tenant only.

## Files to create

1. `pkg/auth/auth.go` — Manager and interfaces:
   - Credentials struct: AccessToken, AccessKeyID, SecretAccessKey, SessionToken, StaticKey, StaticSecret, ExpiresAt, Provider
   - IsExpired(buffer time.Duration) bool helper on Credentials
   - Provider interface: Name(), Resolve(ctx, backendName, BackendAuthConfig) (*Credentials, error), SupportsMethod(string) bool
   - BackendAuthConfig struct: Method, TenantID, ClientIDEnv, ClientSecretEnv, RoleARN, GCPProjectNumber/PoolID/ProviderID/ServiceAccount, VaultURL, SecretName, Extra map
   - Manager struct: holds []Provider, sync.Map cache, AuditLogger, map of backend auth configs, refreshBuffer (5min)
   - GetCredentials(ctx, backendName): check cache, if expired find matching provider, resolve, cache, audit log. Return error if no provider supports the method.

2. `pkg/auth/entra.go` — Azure same-tenant only:
   - EntraProvider supports methods: "managed_identity" and "service_principal"
   - managed_identity: GET Azure IMDS at 169.254.169.254/metadata/identity/oauth2/token, resource=https://storage.azure.com/, set Metadata:true header. Parse access_token and expires_on from JSON response.
   - service_principal: POST to login.microsoftonline.com/{tenantId}/oauth2/v2.0/token with grant_type=client_credentials, scope=https://storage.azure.com/.default. Read client_id and client_secret from env vars specified in config.

3. `pkg/auth/aws.go` — AWS STS federation:
   - AWSProvider supports method: "oidc_federation"
   - Step 1: Get Entra managed identity JWT
   - Step 2: POST to https://sts.amazonaws.com/ with Action=AssumeRoleWithWebIdentity, WebIdentityToken=<entra_jwt>, RoleArn from config, DurationSeconds=3600
   - Parse XML response: extract AccessKeyId, SecretAccessKey, SessionToken, Expiration

4. `pkg/auth/gcp.go` — GCP Workload Identity Federation:
   - GCPProvider supports method: "gcp_wif"
   - Step 1: Get Entra managed identity JWT
   - Step 2: POST https://sts.googleapis.com/v1/token with grant_type=urn:ietf:params:oauth:grant-type:token-exchange, subject_token=<entra_jwt>, audience=//iam.googleapis.com/projects/.../providers/...
   - Step 3: POST iamcredentials.googleapis.com generateAccessToken with the STS token, scope=devstorage.read_only
   - Return the GCS access token

5. `pkg/auth/keyvault.go` — Azure Key Vault for static creds:
   - KeyVaultProvider supports method: "static_keyvault"
   - Step 1: Get managed identity token for resource https://vault.azure.net
   - Step 2: GET https://{vault}/secrets/{name}?api-version=7.4
   - Step 3: Parse secret value as JSON {"access_key":"...","secret_key":"..."}
   - Set ExpiresAt to Now()+30min to re-fetch periodically for key rotations

6. `pkg/auth/audit.go` — Audit logger:
   - AuditEntry: Timestamp, BackendName, Provider, UserID, Success, Error
   - AuditLogger: thread-safe ring buffer (max 10000), structured slog output
   - Optional external sink function for telemetry integration
   - Recent(limit) returns last N entries

7. `pkg/backend/authenticated.go` — Auth-wrapping backend:
   - AuthenticatedBackend wraps a RcloneBackend + auth.Manager
   - refreshIfNeeded(ctx): check if lastCreds are expired, if so call authMgr.GetCredentials, then call inner.UpdateCredentials(creds)
   - All Backend interface methods: call refreshIfNeeded first, then delegate to inner

8. `pkg/auth/auth_test.go` — Tests:
   - TestManager_CachesCredentials: resolve twice, verify second call uses cache
   - TestManager_RefreshesExpiredCreds: set short expiry, wait, verify re-resolve
   - TestManager_RoutesToCorrectProvider: register all 4 providers, verify each method routes correctly
   - TestManager_ErrorOnUnknownMethod: verify error for unsupported method
   - TestAuditLogger_RecordsEntries: log success and failure, verify Recent returns them
   - TestAuditLogger_RingBuffer: log 20000 entries with maxSize 10000, verify only last 10000 kept
   - Mock HTTP tests: httptest.NewServer for IMDS, token endpoints, STS, Key Vault

## Key constraints
- NO cross-tenant complexity. All Azure access is same-tenant via managed identity or service principal.
- Never log access tokens — log only provider name and backend name for debugging
- All HTTP calls have 30s timeout and respect ctx cancellation
- Retry strategy: 3 attempts, backoff 1s → 2s → 4s, only on HTTP 429/500/503
- Thread-safe: GetCredentials is called from 32+ concurrent FUSE goroutines
- The auth manager's cache key is the backend name (string)
- Credential refresh happens inside a mutex to prevent thundering herd (only one goroutine refreshes at a time per backend)
```

---

## M2 Exit Criteria

- [ ] Three+ backends mounted simultaneously from different providers
- [ ] `ls /data/` shows all configured mount points
- [ ] Azure Blob access via managed identity works (same tenant)
- [ ] Azure Blob access via service principal works (same tenant)
- [ ] AWS S3 access via Entra → STS federation works
- [ ] GCS access via Entra → WIF works
- [ ] Vast access via Key Vault static credentials works
- [ ] Backend failure isolation: killing one backend's connectivity doesn't crash mount
- [ ] All auth operations audit-logged
- [ ] Token refresh works: 6+ hour test without credential expiry errors
- [ ] Auth provider routing is correct: each backend uses its configured method