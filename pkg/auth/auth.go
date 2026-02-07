package auth

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/warpdrive/warpdrive/pkg/metrics"
)

// Credentials represents resolved credentials for a backend.
type Credentials struct {
	// For Azure Blob
	AccessToken string

	// For AWS S3
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// For static (Vast, Nebius, NFS)
	StaticKey    string
	StaticSecret string

	// Common
	ExpiresAt time.Time
	Provider  string // "managed_identity", "service_principal", "aws_sts", "gcp_wif", "keyvault", "static", "none"
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
	Resolve(ctx context.Context, backendName string, cfg ProviderConfig) (*Credentials, error)

	// SupportsMethod returns true if this provider handles the given auth method.
	SupportsMethod(method string) bool
}

// ProviderConfig is the auth section from a backend's config.
type ProviderConfig struct {
	Method          string
	TenantID        string
	ClientIDEnv     string
	ClientSecretEnv string
	RoleARN         string
	GCPProjectNum   string
	GCPPoolID       string
	GCPProviderID   string
	GCPServiceAcct  string
	VaultURL        string
	SecretName      string
	AccessKeyID     string
	SecretAccessKey string
	Extra           map[string]string
}

// Manager orchestrates auth across all providers.
type Manager struct {
	providers []Provider
	cache     sync.Map // backendName -> *Credentials
	audit     *AuditLogger

	// Backend auth configs (loaded from main config)
	mu          sync.Mutex
	backendCfgs map[string]ProviderConfig

	// Per-backend refresh mutexes to prevent thundering herd
	refreshMu sync.Map // backendName -> *sync.Mutex

	// Refresh buffer: request new creds this far before expiry
	refreshBuffer time.Duration // Default 5 minutes
}

// NewManager creates an auth manager.
func NewManager(audit *AuditLogger) *Manager {
	return &Manager{
		audit:         audit,
		backendCfgs:   make(map[string]ProviderConfig),
		refreshBuffer: 5 * time.Minute,
	}
}

// RegisterProvider adds a credential provider.
func (m *Manager) RegisterProvider(p Provider) {
	m.providers = append(m.providers, p)
}

// ProviderCount returns the number of registered providers.
func (m *Manager) ProviderCount() int {
	return len(m.providers)
}

// SetBackendAuth configures auth for a specific backend.
func (m *Manager) SetBackendAuth(backendName string, cfg ProviderConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backendCfgs[backendName] = cfg
}

// GetCredentials returns valid credentials for the named backend.
// Caches and auto-refreshes tokens. Thread-safe.
func (m *Manager) GetCredentials(ctx context.Context, backendName string) (*Credentials, error) {
	// 1. Check cache — fast path
	if cached, ok := m.cache.Load(backendName); ok {
		creds := cached.(*Credentials)
		if !creds.IsExpired(m.refreshBuffer) {
			return creds, nil
		}
	}

	// 2. Get per-backend mutex to serialize refreshes (prevent thundering herd)
	muIface, _ := m.refreshMu.LoadOrStore(backendName, &sync.Mutex{})
	mu := muIface.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// 3. Double-check cache after acquiring lock (another goroutine may have refreshed)
	if cached, ok := m.cache.Load(backendName); ok {
		creds := cached.(*Credentials)
		if !creds.IsExpired(m.refreshBuffer) {
			return creds, nil
		}
	}

	// 4. Find backend auth config
	m.mu.Lock()
	cfg, ok := m.backendCfgs[backendName]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("auth.GetCredentials: no auth config for backend %s", backendName)
	}

	// 5. Find matching provider
	for _, p := range m.providers {
		if p.SupportsMethod(cfg.Method) {
			creds, err := p.Resolve(ctx, backendName, cfg)
			if err != nil {
				metrics.AuthRefreshes.WithLabelValues(p.Name(), "failure").Inc()
				if m.audit != nil {
					m.audit.Log(AuditEntry{
						Timestamp:   time.Now(),
						BackendName: backendName,
						Provider:    p.Name(),
						Success:     false,
						Error:       err.Error(),
					})
				}
				return nil, fmt.Errorf("auth.GetCredentials: provider %s: %w", p.Name(), err)
			}

			// 6. Cache
			m.cache.Store(backendName, creds)
			metrics.AuthRefreshes.WithLabelValues(p.Name(), "success").Inc()

			// 7. Audit
			if m.audit != nil {
				m.audit.Log(AuditEntry{
					Timestamp:   time.Now(),
					BackendName: backendName,
					Provider:    p.Name(),
					Success:     true,
				})
			}

			return creds, nil
		}
	}

	return nil, fmt.Errorf("auth.GetCredentials: no provider supports method %q for backend %s", cfg.Method, backendName)
}

// InvalidateCache removes cached credentials for a backend.
func (m *Manager) InvalidateCache(backendName string) {
	m.cache.Delete(backendName)
}
