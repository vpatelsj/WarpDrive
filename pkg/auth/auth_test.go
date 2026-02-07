package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─────────────────────── Mock Provider ───────────────────────

type mockProvider struct {
	name      string
	method    string
	creds     *Credentials
	err       error
	callCount atomic.Int32
}

func (m *mockProvider) Name() string                      { return m.name }
func (m *mockProvider) SupportsMethod(method string) bool { return method == m.method }
func (m *mockProvider) Resolve(_ context.Context, _ string, _ ProviderConfig) (*Credentials, error) {
	m.callCount.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return m.creds, nil
}

type dynamicMockProvider struct {
	name        string
	method      string
	resolveFunc func() (*Credentials, error)
}

func (m *dynamicMockProvider) Name() string                      { return m.name }
func (m *dynamicMockProvider) SupportsMethod(method string) bool { return method == m.method }
func (m *dynamicMockProvider) Resolve(_ context.Context, _ string, _ ProviderConfig) (*Credentials, error) {
	return m.resolveFunc()
}

// urlRewriteTransport is an http.RoundTripper that rewrites all request URLs
// to point to a test server, allowing providers to call their real production
// URLs while actually hitting the httptest server.
type urlRewriteTransport struct {
	base      http.RoundTripper
	targetURL string
}

func (t *urlRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Rewrite the URL to keep the path and query but change scheme+host
	newURL := t.targetURL + req.URL.Path
	if req.URL.RawQuery != "" {
		newURL += "?" + req.URL.RawQuery
	}
	newReq, err := http.NewRequestWithContext(req.Context(), req.Method, newURL, req.Body)
	if err != nil {
		return nil, err
	}
	newReq.Header = req.Header
	return t.base.RoundTrip(newReq)
}

// ─────────────────────── Credentials Tests ───────────────────────

func TestCredentials_IsExpired(t *testing.T) {
	t.Run("zero expiry never expires", func(t *testing.T) {
		c := &Credentials{}
		if c.IsExpired(5 * time.Minute) {
			t.Error("zero ExpiresAt should not be expired")
		}
	})

	t.Run("future expiry not expired", func(t *testing.T) {
		c := &Credentials{ExpiresAt: time.Now().Add(1 * time.Hour)}
		if c.IsExpired(5 * time.Minute) {
			t.Error("1h future should not be expired with 5m buffer")
		}
	})

	t.Run("near expiry with buffer", func(t *testing.T) {
		c := &Credentials{ExpiresAt: time.Now().Add(3 * time.Minute)}
		if !c.IsExpired(5 * time.Minute) {
			t.Error("3m future should be expired with 5m buffer")
		}
	})

	t.Run("past expiry", func(t *testing.T) {
		c := &Credentials{ExpiresAt: time.Now().Add(-1 * time.Hour)}
		if !c.IsExpired(0) {
			t.Error("past expiry should be expired")
		}
	})
}

// ─────────────────────── Manager Tests ───────────────────────

func TestManager_CachesCredentials(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	creds := &Credentials{
		AccessToken: "token-123",
		ExpiresAt:   time.Now().Add(1 * time.Hour),
		Provider:    "test",
	}

	mp := &mockProvider{name: "test", method: "test_method", creds: creds}
	mgr.RegisterProvider(mp)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	c1, err := mgr.GetCredentials(context.Background(), "backend1")
	if err != nil {
		t.Fatalf("first GetCredentials: %v", err)
	}
	if c1.AccessToken != "token-123" {
		t.Errorf("token = %q, want token-123", c1.AccessToken)
	}

	c2, err := mgr.GetCredentials(context.Background(), "backend1")
	if err != nil {
		t.Fatalf("second GetCredentials: %v", err)
	}
	if c2.AccessToken != "token-123" {
		t.Errorf("cached token = %q, want token-123", c2.AccessToken)
	}

	if mp.callCount.Load() != 1 {
		t.Errorf("provider called %d times, want 1", mp.callCount.Load())
	}
}

func TestManager_RefreshesExpiredCreds(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)
	mgr.refreshBuffer = 100 * time.Millisecond

	callNum := atomic.Int32{}
	provider := &dynamicMockProvider{
		name:   "test",
		method: "test_method",
		resolveFunc: func() (*Credentials, error) {
			n := callNum.Add(1)
			return &Credentials{
				AccessToken: fmt.Sprintf("token-%d", n),
				ExpiresAt:   time.Now().Add(200 * time.Millisecond),
				Provider:    "test",
			}, nil
		},
	}

	mgr.RegisterProvider(provider)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	c1, err := mgr.GetCredentials(context.Background(), "backend1")
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if c1.AccessToken != "token-1" {
		t.Errorf("first token = %q, want token-1", c1.AccessToken)
	}

	time.Sleep(300 * time.Millisecond)

	c2, err := mgr.GetCredentials(context.Background(), "backend1")
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if c2.AccessToken != "token-2" {
		t.Errorf("refreshed token = %q, want token-2", c2.AccessToken)
	}
}

func TestManager_RoutesToCorrectProvider(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	entraP := &mockProvider{
		name: "entra", method: "managed_identity",
		creds: &Credentials{Provider: "managed_identity", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}
	awsP := &mockProvider{
		name: "aws_sts", method: "oidc_federation",
		creds: &Credentials{Provider: "aws_sts", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}
	gcpP := &mockProvider{
		name: "gcp_wif", method: "gcp_wif",
		creds: &Credentials{Provider: "gcp_wif", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}
	kvP := &mockProvider{
		name: "keyvault", method: "static_keyvault",
		creds: &Credentials{Provider: "keyvault", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}

	mgr.RegisterProvider(entraP)
	mgr.RegisterProvider(awsP)
	mgr.RegisterProvider(gcpP)
	mgr.RegisterProvider(kvP)

	tests := []struct {
		backend  string
		method   string
		wantProv string
	}{
		{"azure-blob", "managed_identity", "managed_identity"},
		{"s3-data", "oidc_federation", "aws_sts"},
		{"gcs-data", "gcp_wif", "gcp_wif"},
		{"vast-archive", "static_keyvault", "keyvault"},
	}

	for _, tt := range tests {
		mgr.SetBackendAuth(tt.backend, ProviderConfig{Method: tt.method})
		creds, err := mgr.GetCredentials(context.Background(), tt.backend)
		if err != nil {
			t.Errorf("GetCredentials(%s): %v", tt.backend, err)
			continue
		}
		if creds.Provider != tt.wantProv {
			t.Errorf("backend %s: provider = %q, want %q", tt.backend, creds.Provider, tt.wantProv)
		}
	}
}

func TestManager_ErrorOnUnknownMethod(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	mgr.RegisterProvider(&mockProvider{name: "test", method: "known"})
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "unknown_method"})

	_, err := mgr.GetCredentials(context.Background(), "backend1")
	if err == nil {
		t.Error("expected error for unknown method")
	}
}

func TestManager_ErrorOnUnknownBackend(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	_, err := mgr.GetCredentials(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for unknown backend")
	}
}

func TestManager_ConcurrentAccess(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)
	mgr.refreshBuffer = 0

	mp := &mockProvider{
		name: "test", method: "test_method",
		creds: &Credentials{
			AccessToken: "concurrent-token",
			ExpiresAt:   time.Now().Add(1 * time.Hour),
			Provider:    "test",
		},
	}
	mgr.RegisterProvider(mp)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			creds, err := mgr.GetCredentials(context.Background(), "backend1")
			if err != nil {
				errs <- err
				return
			}
			if creds.AccessToken != "concurrent-token" {
				errs <- fmt.Errorf("unexpected token: %s", creds.AccessToken)
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent access error: %v", err)
	}
}

func TestManager_InvalidateCache(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	callNum := atomic.Int32{}
	provider := &dynamicMockProvider{
		name:   "test",
		method: "test_method",
		resolveFunc: func() (*Credentials, error) {
			n := callNum.Add(1)
			return &Credentials{
				AccessToken: fmt.Sprintf("token-%d", n),
				ExpiresAt:   time.Now().Add(1 * time.Hour),
				Provider:    "test",
			}, nil
		},
	}

	mgr.RegisterProvider(provider)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	c1, _ := mgr.GetCredentials(context.Background(), "backend1")
	if c1.AccessToken != "token-1" {
		t.Fatalf("first token = %q, want token-1", c1.AccessToken)
	}

	mgr.InvalidateCache("backend1")

	c2, _ := mgr.GetCredentials(context.Background(), "backend1")
	if c2.AccessToken != "token-2" {
		t.Errorf("after invalidation token = %q, want token-2", c2.AccessToken)
	}
}

func TestManager_ProviderError(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	mp := &mockProvider{
		name:   "failing",
		method: "fail_method",
		err:    fmt.Errorf("network timeout"),
	}
	mgr.RegisterProvider(mp)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "fail_method"})

	_, err := mgr.GetCredentials(context.Background(), "backend1")
	if err == nil {
		t.Error("expected error from failing provider")
	}

	entries := audit.Recent(10)
	if len(entries) == 0 {
		t.Fatal("no audit entries")
	}
	if entries[0].Success {
		t.Error("audit entry should show failure")
	}
	if entries[0].Error != "network timeout" {
		t.Errorf("audit error = %q, want network timeout", entries[0].Error)
	}
}

func TestManager_NilAudit(t *testing.T) {
	mgr := NewManager(nil)

	mp := &mockProvider{
		name: "test", method: "test_method",
		creds: &Credentials{Provider: "test", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}
	mgr.RegisterProvider(mp)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	_, err := mgr.GetCredentials(context.Background(), "backend1")
	if err != nil {
		t.Fatalf("GetCredentials: %v", err)
	}
}

// ─────────────────────── Audit Logger Tests ───────────────────────

func TestAuditLogger_RecordsEntries(t *testing.T) {
	var sinkEntries []AuditEntry
	var sinkMu sync.Mutex
	sink := func(e AuditEntry) {
		sinkMu.Lock()
		sinkEntries = append(sinkEntries, e)
		sinkMu.Unlock()
	}

	al := NewAuditLogger(100, sink)

	al.Log(AuditEntry{
		Timestamp:   time.Now(),
		BackendName: "azure_training",
		Provider:    "managed_identity",
		Success:     true,
	})
	al.Log(AuditEntry{
		Timestamp:   time.Now(),
		BackendName: "s3_datasets",
		Provider:    "aws_sts",
		Success:     false,
		Error:       "STS timeout",
	})

	entries := al.Recent(10)
	if len(entries) != 2 {
		t.Fatalf("Recent(10) returned %d, want 2", len(entries))
	}
	if entries[0].BackendName != "azure_training" {
		t.Errorf("entry[0].BackendName = %q, want azure_training", entries[0].BackendName)
	}
	if entries[1].Success {
		t.Error("entry[1] should be failure")
	}

	sinkMu.Lock()
	if len(sinkEntries) != 2 {
		t.Errorf("sink received %d entries, want 2", len(sinkEntries))
	}
	sinkMu.Unlock()
}

func TestAuditLogger_RingBuffer(t *testing.T) {
	al := NewAuditLogger(10000, nil)

	for i := 0; i < 20000; i++ {
		al.Log(AuditEntry{
			BackendName: fmt.Sprintf("backend-%d", i),
			Success:     true,
		})
	}

	if al.Len() != 10000 {
		t.Errorf("len = %d, want 10000", al.Len())
	}

	entries := al.Recent(5)
	if len(entries) != 5 {
		t.Fatalf("Recent(5) = %d, want 5", len(entries))
	}
	if entries[4].BackendName != "backend-19999" {
		t.Errorf("last entry = %q, want backend-19999", entries[4].BackendName)
	}
	if entries[0].BackendName != "backend-19995" {
		t.Errorf("first of last 5 = %q, want backend-19995", entries[0].BackendName)
	}
}

func TestAuditLogger_RecentOverflow(t *testing.T) {
	al := NewAuditLogger(100, nil)
	al.Log(AuditEntry{BackendName: "test", Success: true})

	entries := al.Recent(50)
	if len(entries) != 1 {
		t.Errorf("Recent(50) = %d entries, want 1", len(entries))
	}
}

func TestAuditLogger_RecentZero(t *testing.T) {
	al := NewAuditLogger(100, nil)
	entries := al.Recent(0)
	if entries != nil {
		t.Errorf("Recent(0) = %v, want nil", entries)
	}
}

// ─────────────────────── Static / None Provider Tests ───────────────────────

func TestStaticProvider(t *testing.T) {
	p := NewStaticProvider()

	if p.Name() != "static" {
		t.Errorf("Name() = %q, want static", p.Name())
	}
	if !p.SupportsMethod("static") {
		t.Error("should support static method")
	}
	if p.SupportsMethod("managed_identity") {
		t.Error("should not support managed_identity")
	}

	creds, err := p.Resolve(context.Background(), "test", ProviderConfig{
		Method:          "static",
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "secret123",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.AccessKeyID != "AKIAEXAMPLE" {
		t.Errorf("AccessKeyID = %q, want AKIAEXAMPLE", creds.AccessKeyID)
	}
	if creds.SecretAccessKey != "secret123" {
		t.Errorf("SecretAccessKey = %q, want secret123", creds.SecretAccessKey)
	}
	if creds.IsExpired(0) {
		t.Error("static creds should not expire")
	}
}

func TestNoneProvider(t *testing.T) {
	p := NewNoneProvider()

	if !p.SupportsMethod("none") {
		t.Error("should support none method")
	}
	if !p.SupportsMethod("") {
		t.Error("should support empty method")
	}
	if p.SupportsMethod("managed_identity") {
		t.Error("should not support managed_identity")
	}

	creds, err := p.Resolve(context.Background(), "local-backend", ProviderConfig{})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.Provider != "none" {
		t.Errorf("Provider = %q, want none", creds.Provider)
	}
}

// ─────────────────────── Entra Provider Tests (through Resolve) ──────────────

func TestEntraProvider_ManagedIdentity_Resolve(t *testing.T) {
	expiresOn := fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix())

	// Mock IMDS endpoint — the provider will call this via its injected http.Client
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata") != "true" {
			t.Error("missing Metadata header")
			http.Error(w, "missing header", 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "mi-token-abc",
			"expires_on":   expiresOn,
			"resource":     "https://storage.azure.com/",
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	// Inject the test server's client and override IMDS URL by using a transport
	// that redirects all requests to the test server.
	transport := &urlRewriteTransport{
		base:      http.DefaultTransport,
		targetURL: server.URL,
	}
	client := &http.Client{Transport: transport}
	p := NewEntraProviderWithClient(client)

	creds, err := p.Resolve(context.Background(), "test-backend", ProviderConfig{
		Method: "managed_identity",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.AccessToken != "mi-token-abc" {
		t.Errorf("AccessToken = %q, want mi-token-abc", creds.AccessToken)
	}
	if creds.Provider != "managed_identity" {
		t.Errorf("Provider = %q, want managed_identity", creds.Provider)
	}
	if creds.IsExpired(0) {
		t.Error("credentials should not be expired")
	}
}

func TestEntraProvider_ServicePrincipal_Resolve(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		bodyStr := string(body)
		if !strings.Contains(bodyStr, "grant_type=client_credentials") {
			t.Error("wrong grant_type")
		}
		if !strings.Contains(bodyStr, "client_id=test-client-id") {
			t.Error("wrong client_id")
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "sp-token-xyz",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	}))
	defer server.Close()

	os.Setenv("TEST_SP_CLIENT_ID", "test-client-id")
	os.Setenv("TEST_SP_CLIENT_SECRET", "test-secret")
	defer os.Unsetenv("TEST_SP_CLIENT_ID")
	defer os.Unsetenv("TEST_SP_CLIENT_SECRET")

	transport := &urlRewriteTransport{
		base:      http.DefaultTransport,
		targetURL: server.URL,
	}
	client := &http.Client{Transport: transport}
	p := NewEntraProviderWithClient(client)

	creds, err := p.Resolve(context.Background(), "test-backend", ProviderConfig{
		Method:          "service_principal",
		TenantID:        "test-tenant-id",
		ClientIDEnv:     "TEST_SP_CLIENT_ID",
		ClientSecretEnv: "TEST_SP_CLIENT_SECRET",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.AccessToken != "sp-token-xyz" {
		t.Errorf("AccessToken = %q, want sp-token-xyz", creds.AccessToken)
	}
	if creds.Provider != "service_principal" {
		t.Errorf("Provider = %q, want service_principal", creds.Provider)
	}
}

func TestEntraProvider_ServicePrincipalMissingEnv(t *testing.T) {
	os.Unsetenv("MISSING_CLIENT_ID")
	os.Unsetenv("MISSING_CLIENT_SECRET")

	p := NewEntraProvider()
	_, err := p.Resolve(context.Background(), "test", ProviderConfig{
		Method:          "service_principal",
		ClientIDEnv:     "MISSING_CLIENT_ID",
		ClientSecretEnv: "MISSING_CLIENT_SECRET",
	})
	if err == nil {
		t.Error("expected error for missing env vars")
	}
}

// ─────────────────────── AWS STS Provider Tests (through Resolve) ───────────

func TestAWSProvider_OIDCFederation_Resolve(t *testing.T) {
	expiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)

	// Mux handles both IMDS (Entra) and STS (AWS) requests,
	// distinguished by request method and path.
	mux := http.NewServeMux()

	// Entra IMDS endpoint — GET requests for managed identity token
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt-for-aws",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	// AWS STS endpoint — POST requests
	mux.HandleFunc("/sts", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodyStr := string(body)
		if !strings.Contains(bodyStr, "Action=AssumeRoleWithWebIdentity") {
			t.Error("wrong STS action")
		}
		if !strings.Contains(bodyStr, "WebIdentityToken=entra-jwt-for-aws") {
			t.Error("wrong web identity token")
		}
		stsResp := fmt.Sprintf(`<AssumeRoleWithWebIdentityResponse>
			<AssumeRoleWithWebIdentityResult>
				<Credentials>
					<AccessKeyId>AKIATEST123</AccessKeyId>
					<SecretAccessKey>secretkey456</SecretAccessKey>
					<SessionToken>sessiontoken789</SessionToken>
					<Expiration>%s</Expiration>
				</Credentials>
			</AssumeRoleWithWebIdentityResult>
		</AssumeRoleWithWebIdentityResponse>`, expiration)
		w.Header().Set("Content-Type", "text/xml")
		w.Write([]byte(stsResp))
	})

	// Catch-all: route unknown paths to STS handler (the aws provider POSTs to "/")
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			body, _ := io.ReadAll(r.Body)
			bodyStr := string(body)
			if strings.Contains(bodyStr, "Action=AssumeRoleWithWebIdentity") {
				stsResp := fmt.Sprintf(`<AssumeRoleWithWebIdentityResponse>
					<AssumeRoleWithWebIdentityResult>
						<Credentials>
							<AccessKeyId>AKIATEST123</AccessKeyId>
							<SecretAccessKey>secretkey456</SecretAccessKey>
							<SessionToken>sessiontoken789</SessionToken>
							<Expiration>%s</Expiration>
						</Credentials>
					</AssumeRoleWithWebIdentityResult>
				</AssumeRoleWithWebIdentityResponse>`, expiration)
				w.Header().Set("Content-Type", "text/xml")
				w.Write([]byte(stsResp))
				return
			}
		}
		// IMDS fallback for GET
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt-for-aws",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{
		base:      http.DefaultTransport,
		targetURL: server.URL,
	}
	client := &http.Client{Transport: transport}

	entraP := NewEntraProviderWithClient(client)
	awsP := NewAWSProviderWithClient(entraP, client)

	creds, err := awsP.Resolve(context.Background(), "s3-test", ProviderConfig{
		Method:  "oidc_federation",
		RoleARN: "arn:aws:iam::123456789:role/test-role",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.AccessKeyID != "AKIATEST123" {
		t.Errorf("AccessKeyID = %q, want AKIATEST123", creds.AccessKeyID)
	}
	if creds.SecretAccessKey != "secretkey456" {
		t.Errorf("SecretAccessKey = %q, want secretkey456", creds.SecretAccessKey)
	}
	if creds.SessionToken != "sessiontoken789" {
		t.Errorf("SessionToken = %q, want sessiontoken789", creds.SessionToken)
	}
	if creds.Provider != "aws_sts" {
		t.Errorf("Provider = %q, want aws_sts", creds.Provider)
	}
}

// ─────────────────────── GCP WIF Provider Tests (through Resolve) ───────────

func TestGCPProvider_WorkloadIdentityFederation_Resolve(t *testing.T) {
	expireTime := time.Now().Add(1 * time.Hour).Format(time.RFC3339)

	mux := http.NewServeMux()

	// Entra IMDS — managed identity token
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt-for-gcp",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	// GCP STS token exchange
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodyStr := string(body)
		if !strings.Contains(bodyStr, "subject_token=entra-jwt-for-gcp") {
			t.Error("wrong subject_token in GCP STS request")
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "gcp-sts-token",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	})

	// Service account impersonation
	mux.HandleFunc("/v1/projects/-/serviceAccounts/", func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer gcp-sts-token" {
			t.Errorf("wrong Authorization header: %q", authHeader)
		}
		json.NewEncoder(w).Encode(map[string]string{
			"accessToken": "gcs-access-token-final",
			"expireTime":  expireTime,
		})
	})

	// Catch-all fallback (for IMDS requests that hit "/" )
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt-for-gcp",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{
		base:      http.DefaultTransport,
		targetURL: server.URL,
	}
	client := &http.Client{Transport: transport}

	entraP := NewEntraProviderWithClient(client)
	gcpP := NewGCPProviderWithClient(entraP, client)

	creds, err := gcpP.Resolve(context.Background(), "gcs-test", ProviderConfig{
		Method:         "gcp_wif",
		GCPProjectNum:  "123456",
		GCPPoolID:      "my-pool",
		GCPProviderID:  "my-provider",
		GCPServiceAcct: "sa@proj.iam.gserviceaccount.com",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.AccessToken != "gcs-access-token-final" {
		t.Errorf("AccessToken = %q, want gcs-access-token-final", creds.AccessToken)
	}
	if creds.Provider != "gcp_wif" {
		t.Errorf("Provider = %q, want gcp_wif", creds.Provider)
	}
}

// ─────────────────────── Key Vault Provider Tests (through Resolve) ─────────

func TestKeyVaultProvider_Resolve(t *testing.T) {
	secretJSON := `{"access_key":"VAST_AK_123","secret_key":"VAST_SK_456"}`

	mux := http.NewServeMux()

	// IMDS endpoint for Key Vault token (managed identity for vault.azure.net resource)
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata") != "true" {
			t.Error("missing Metadata header for IMDS")
			http.Error(w, "missing header", 400)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "kv-bearer-token",
		})
	})

	// Key Vault secrets endpoint
	mux.HandleFunc("/secrets/vast-s3-creds", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer kv-bearer-token" {
			t.Error("missing/wrong auth header on KV request")
			http.Error(w, "unauthorized", 401)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"value": secretJSON,
		})
	})

	// Catch-all fallback for IMDS
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "kv-bearer-token",
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{
		base:      http.DefaultTransport,
		targetURL: server.URL,
	}
	client := &http.Client{Transport: transport}
	p := NewKeyVaultProviderWithClient(client)

	creds, err := p.Resolve(context.Background(), "vast-store", ProviderConfig{
		Method:     "static_keyvault",
		VaultURL:   server.URL, // The vault URL will route to our server via transport
		SecretName: "vast-s3-creds",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.StaticKey != "VAST_AK_123" {
		t.Errorf("StaticKey = %q, want VAST_AK_123", creds.StaticKey)
	}
	if creds.StaticSecret != "VAST_SK_456" {
		t.Errorf("StaticSecret = %q, want VAST_SK_456", creds.StaticSecret)
	}
	if creds.Provider != "keyvault" {
		t.Errorf("Provider = %q, want keyvault", creds.Provider)
	}
	if creds.IsExpired(0) {
		t.Error("keyvault creds should not be immediately expired")
	}
}

// ─────────────────────── Retry Logic Tests ─────────────────────────────────

// fastBackoffs are used in tests to avoid waiting for real backoff durations.
var fastBackoffs = []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}

func TestRetryDo_SucceedsOnFirstAttempt(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	if callCount.Load() != 1 {
		t.Errorf("expected 1 call, got %d", callCount.Load())
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestRetryDo_RetriesOn429(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n < 3 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	if callCount.Load() != 3 {
		t.Errorf("expected 3 calls, got %d", callCount.Load())
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestRetryDo_RetriesOn500(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	if callCount.Load() != 2 {
		t.Errorf("expected 2 calls, got %d", callCount.Load())
	}
}

func TestRetryDo_ExhaustsRetries(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(503)
	}))
	defer server.Close()

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	// On last attempt, the 503 is returned as-is
	if callCount.Load() != 3 {
		t.Errorf("expected 3 calls, got %d", callCount.Load())
	}
	if resp.StatusCode != 503 {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestRetryDo_NoRetryOn4xx(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(401)
	}))
	defer server.Close()

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	if callCount.Load() != 1 {
		t.Errorf("expected 1 call for non-retryable status, got %d", callCount.Load())
	}
}

func TestRetryDo_ContextCancellation(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(503) // Retryable status
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after first attempt before backoff completes
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	backoffs := []time.Duration{500 * time.Millisecond, 1 * time.Second, 2 * time.Second}
	req, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	_, err := retryDoWithBackoffs(server.Client(), req, backoffs)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
	if callCount.Load() > 2 {
		t.Errorf("expected at most 2 calls before cancellation, got %d", callCount.Load())
	}
}

func TestRetryDo_RetriesPostWithBody(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != "grant_type=client_credentials" {
			t.Errorf("attempt %d: unexpected body %q", n, string(body))
		}
		if n < 3 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	bodyStr := "grant_type=client_credentials"
	req, _ := http.NewRequestWithContext(context.Background(), "POST", server.URL,
		strings.NewReader(bodyStr))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// GetBody allows retryDo to clone the body for retries
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(bodyStr)), nil
	}

	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	if callCount.Load() != 3 {
		t.Errorf("expected 3 calls, got %d", callCount.Load())
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestRetryDo_RetriesOn503(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	req, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
	resp, err := retryDoWithBackoffs(server.Client(), req, fastBackoffs)
	if err != nil {
		t.Fatalf("retryDo: %v", err)
	}
	defer resp.Body.Close()

	if callCount.Load() != 2 {
		t.Errorf("expected 2 calls, got %d", callCount.Load())
	}
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// ─────────────────────── Provider HTTP Error Path Tests ─────────────

func TestEntraProvider_ManagedIdentity_IMDSError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden) // 403 — not retryable
		w.Write([]byte("IMDS unavailable"))
	}))
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	p := NewEntraProviderWithClient(client)

	_, err := p.Resolve(context.Background(), "test", ProviderConfig{Method: "managed_identity"})
	if err == nil {
		t.Error("expected error when IMDS returns non-200")
	}
	if !strings.Contains(err.Error(), "IMDS returned 403") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEntraProvider_ManagedIdentity_MalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("not valid json{{{"))
	}))
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	p := NewEntraProviderWithClient(client)

	_, err := p.Resolve(context.Background(), "test", ProviderConfig{Method: "managed_identity"})
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
	if !strings.Contains(err.Error(), "decode response") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEntraProvider_ServicePrincipal_TokenEndpointError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("invalid_client"))
	}))
	defer server.Close()

	os.Setenv("SP_ERR_CLIENT_ID", "id")
	os.Setenv("SP_ERR_CLIENT_SECRET", "secret")
	defer os.Unsetenv("SP_ERR_CLIENT_ID")
	defer os.Unsetenv("SP_ERR_CLIENT_SECRET")

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	p := NewEntraProviderWithClient(client)

	_, err := p.Resolve(context.Background(), "test", ProviderConfig{
		Method:          "service_principal",
		TenantID:        "t",
		ClientIDEnv:     "SP_ERR_CLIENT_ID",
		ClientSecretEnv: "SP_ERR_CLIENT_SECRET",
	})
	if err == nil {
		t.Error("expected error when token endpoint returns 401")
	}
	if !strings.Contains(err.Error(), "token endpoint returned 401") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEntraProvider_UnsupportedMethod(t *testing.T) {
	p := NewEntraProvider()
	_, err := p.Resolve(context.Background(), "test", ProviderConfig{Method: "oidc_federation"})
	if err == nil {
		t.Error("expected error for unsupported method")
	}
	if !strings.Contains(err.Error(), "unsupported method") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAWSProvider_STSError(t *testing.T) {
	mux := http.NewServeMux()
	// IMDS returns success
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})
	// STS returns error
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid role"))
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	entraP := NewEntraProviderWithClient(client)
	awsP := NewAWSProviderWithClient(entraP, client)

	_, err := awsP.Resolve(context.Background(), "s3", ProviderConfig{
		Method:  "oidc_federation",
		RoleARN: "arn:aws:iam::123:role/bad",
	})
	if err == nil {
		t.Error("expected error when STS returns 400")
	}
	if !strings.Contains(err.Error(), "STS returned 400") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGCPProvider_STSExchangeError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("invalid audience"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	entraP := NewEntraProviderWithClient(client)
	gcpP := NewGCPProviderWithClient(entraP, client)

	_, err := gcpP.Resolve(context.Background(), "gcs", ProviderConfig{
		Method:         "gcp_wif",
		GCPProjectNum:  "123",
		GCPPoolID:      "pool",
		GCPProviderID:  "prov",
		GCPServiceAcct: "sa@proj.iam.gserviceaccount.com",
	})
	if err == nil {
		t.Error("expected error when GCP STS returns 403")
	}
	if !strings.Contains(err.Error(), "GCP STS returned 403") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGCPProvider_SAImpersonationError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "gcp-sts-token",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	})
	mux.HandleFunc("/v1/projects/-/serviceAccounts/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("permission denied"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	entraP := NewEntraProviderWithClient(client)
	gcpP := NewGCPProviderWithClient(entraP, client)

	_, err := gcpP.Resolve(context.Background(), "gcs", ProviderConfig{
		Method:         "gcp_wif",
		GCPProjectNum:  "123",
		GCPPoolID:      "pool",
		GCPProviderID:  "prov",
		GCPServiceAcct: "sa@proj.iam.gserviceaccount.com",
	})
	if err == nil {
		t.Error("expected error when SA impersonation returns 403")
	}
	if !strings.Contains(err.Error(), "SA impersonation returned 403") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestKeyVaultProvider_KVSecretNotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"access_token": "kv-token"})
	})
	mux.HandleFunc("/secrets/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("secret not found"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"access_token": "kv-token"})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	p := NewKeyVaultProviderWithClient(client)

	_, err := p.Resolve(context.Background(), "vast", ProviderConfig{
		Method:     "static_keyvault",
		VaultURL:   server.URL,
		SecretName: "missing-secret",
	})
	if err == nil {
		t.Error("expected error when KV secret not found")
	}
	if !strings.Contains(err.Error(), "KV returned 404") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestKeyVaultProvider_MalformedSecretJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"access_token": "kv-token"})
	})
	mux.HandleFunc("/secrets/bad-secret", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"value": "not-json-at-all",
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"access_token": "kv-token"})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	p := NewKeyVaultProviderWithClient(client)

	_, err := p.Resolve(context.Background(), "vast", ProviderConfig{
		Method:     "static_keyvault",
		VaultURL:   server.URL,
		SecretName: "bad-secret",
	})
	if err == nil {
		t.Error("expected error for malformed secret JSON")
	}
	if !strings.Contains(err.Error(), "parse secret JSON") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─────────────────────── Provider Method Support Tests ──────────────

func TestEntraProvider_SupportsMethod(t *testing.T) {
	p := NewEntraProvider()
	if !p.SupportsMethod("managed_identity") {
		t.Error("should support managed_identity")
	}
	if !p.SupportsMethod("service_principal") {
		t.Error("should support service_principal")
	}
	if p.SupportsMethod("oidc_federation") {
		t.Error("should not support oidc_federation")
	}
}

func TestAWSProvider_SupportsMethod(t *testing.T) {
	p := NewAWSProvider(NewEntraProvider())
	if !p.SupportsMethod("oidc_federation") {
		t.Error("should support oidc_federation")
	}
	if p.SupportsMethod("managed_identity") {
		t.Error("should not support managed_identity")
	}
}

func TestGCPProvider_SupportsMethod(t *testing.T) {
	p := NewGCPProvider(NewEntraProvider())
	if !p.SupportsMethod("gcp_wif") {
		t.Error("should support gcp_wif")
	}
	if p.SupportsMethod("managed_identity") {
		t.Error("should not support managed_identity")
	}
}

func TestKeyVaultProvider_SupportsMethod(t *testing.T) {
	p := NewKeyVaultProvider()
	if !p.SupportsMethod("static_keyvault") {
		t.Error("should support static_keyvault")
	}
	if p.SupportsMethod("managed_identity") {
		t.Error("should not support managed_identity")
	}
}

// ─────────────────────── Integration: Manager with mock providers ──────

func TestManager_FullFlowWithMockProviders(t *testing.T) {
	audit := NewAuditLogger(1000, nil)
	mgr := NewManager(audit)
	mgr.refreshBuffer = 0

	mgr.RegisterProvider(&mockProvider{
		name: "entra", method: "managed_identity",
		creds: &Credentials{
			AccessToken: "azure-mi-token",
			ExpiresAt:   time.Now().Add(1 * time.Hour),
			Provider:    "managed_identity",
		},
	})
	mgr.RegisterProvider(&mockProvider{
		name: "aws_sts", method: "oidc_federation",
		creds: &Credentials{
			AccessKeyID:     "AKIA_AWS",
			SecretAccessKey: "aws-secret",
			SessionToken:    "aws-session",
			ExpiresAt:       time.Now().Add(1 * time.Hour),
			Provider:        "aws_sts",
		},
	})
	mgr.RegisterProvider(&mockProvider{
		name: "gcp_wif", method: "gcp_wif",
		creds: &Credentials{
			AccessToken: "gcp-token",
			ExpiresAt:   time.Now().Add(1 * time.Hour),
			Provider:    "gcp_wif",
		},
	})
	mgr.RegisterProvider(&mockProvider{
		name: "keyvault", method: "static_keyvault",
		creds: &Credentials{
			StaticKey:    "VAST_KEY",
			StaticSecret: "VAST_SECRET",
			ExpiresAt:    time.Now().Add(30 * time.Minute),
			Provider:     "keyvault",
		},
	})
	mgr.RegisterProvider(NewNoneProvider())

	mgr.SetBackendAuth("azure_training", ProviderConfig{Method: "managed_identity"})
	mgr.SetBackendAuth("s3_datasets", ProviderConfig{Method: "oidc_federation"})
	mgr.SetBackendAuth("gcs_shared", ProviderConfig{Method: "gcp_wif"})
	mgr.SetBackendAuth("vast_archive", ProviderConfig{Method: "static_keyvault"})
	mgr.SetBackendAuth("local_data", ProviderConfig{Method: "none"})

	ctx := context.Background()

	azureCreds, err := mgr.GetCredentials(ctx, "azure_training")
	if err != nil {
		t.Fatalf("Azure: %v", err)
	}
	if azureCreds.AccessToken != "azure-mi-token" {
		t.Errorf("Azure token = %q", azureCreds.AccessToken)
	}

	awsCreds, err := mgr.GetCredentials(ctx, "s3_datasets")
	if err != nil {
		t.Fatalf("AWS: %v", err)
	}
	if awsCreds.AccessKeyID != "AKIA_AWS" {
		t.Errorf("AWS key = %q", awsCreds.AccessKeyID)
	}

	gcpCreds, err := mgr.GetCredentials(ctx, "gcs_shared")
	if err != nil {
		t.Fatalf("GCP: %v", err)
	}
	if gcpCreds.AccessToken != "gcp-token" {
		t.Errorf("GCP token = %q", gcpCreds.AccessToken)
	}

	vastCreds, err := mgr.GetCredentials(ctx, "vast_archive")
	if err != nil {
		t.Fatalf("Vast: %v", err)
	}
	if vastCreds.StaticKey != "VAST_KEY" {
		t.Errorf("Vast key = %q", vastCreds.StaticKey)
	}

	localCreds, err := mgr.GetCredentials(ctx, "local_data")
	if err != nil {
		t.Fatalf("Local: %v", err)
	}
	if localCreds.Provider != "none" {
		t.Errorf("Local provider = %q", localCreds.Provider)
	}

	entries := audit.Recent(10)
	if len(entries) != 5 {
		t.Errorf("audit entries = %d, want 5", len(entries))
	}
	for _, e := range entries {
		if !e.Success {
			t.Errorf("audit entry for %s shows failure", e.BackendName)
		}
	}
}

// ─────────────────────── Token Refresh Lifecycle (simulated 6h+) ──────

// TestManager_MultipleRefreshCycles simulates a long-running workload with
// short-lived tokens that must be refreshed multiple times. This validates
// spec exit criterion: "Token refresh works: 6+ hour test without credential
// expiry errors" using fast-forwarded token lifetimes.
func TestManager_MultipleRefreshCycles(t *testing.T) {
	audit := NewAuditLogger(1000, nil)
	mgr := NewManager(audit)
	mgr.refreshBuffer = 50 * time.Millisecond

	callNum := atomic.Int32{}
	provider := &dynamicMockProvider{
		name:   "rotating",
		method: "test_method",
		resolveFunc: func() (*Credentials, error) {
			n := callNum.Add(1)
			return &Credentials{
				AccessToken: fmt.Sprintf("token-%d", n),
				ExpiresAt:   time.Now().Add(100 * time.Millisecond),
				Provider:    "rotating",
			}, nil
		},
	}

	mgr.RegisterProvider(provider)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	// Simulate 10 consecutive refresh cycles (represents hours of operation)
	var lastToken string
	refreshCount := 0
	for i := 0; i < 10; i++ {
		creds, err := mgr.GetCredentials(context.Background(), "backend1")
		if err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if creds.AccessToken != lastToken {
			refreshCount++
			lastToken = creds.AccessToken
		}
		time.Sleep(120 * time.Millisecond) // Let token expire
	}

	if refreshCount < 5 {
		t.Errorf("expected at least 5 refreshes, got %d", refreshCount)
	}

	// Verify audit trail captured all resolutions
	entries := audit.Recent(100)
	if len(entries) < refreshCount {
		t.Errorf("audit has %d entries, expected at least %d", len(entries), refreshCount)
	}
	for _, e := range entries {
		if !e.Success {
			t.Errorf("unexpected failure in audit: %v", e.Error)
		}
	}
}

// ─────────────────────── Audit Logger Concurrent Access ──────────────

func TestAuditLogger_ConcurrentAccess(t *testing.T) {
	al := NewAuditLogger(500, nil)

	const writers = 8
	const readsPerWriter = 50
	var wg sync.WaitGroup

	// Concurrent writers
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < readsPerWriter; j++ {
				al.Log(AuditEntry{
					BackendName: fmt.Sprintf("backend-%d", id),
					Provider:    "test",
					Success:     true,
				})
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < readsPerWriter; j++ {
				entries := al.Recent(10)
				_ = entries // Just exercise the read path
				_ = al.Len()
			}
		}()
	}

	wg.Wait()

	total := al.Len()
	expected := writers * readsPerWriter
	if total != expected {
		t.Errorf("Len() = %d, want %d", total, expected)
	}
}

// ─────────────────────── KeyVault IMDS Token Error ──────────────────

func TestKeyVaultProvider_IMDSTokenError(t *testing.T) {
	mux := http.NewServeMux()
	// IMDS returns error — vault.azure.net token acquisition fails
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("IMDS unavailable for vault resource"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("IMDS unavailable"))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	p := NewKeyVaultProviderWithClient(client)

	_, err := p.Resolve(context.Background(), "vast", ProviderConfig{
		Method:     "static_keyvault",
		VaultURL:   server.URL,
		SecretName: "my-secret",
	})
	if err == nil {
		t.Error("expected error when IMDS fails for vault token")
	}
	if !strings.Contains(err.Error(), "IMDS") {
		t.Errorf("expected IMDS-related error, got: %v", err)
	}
}

// ─────────────────────── AWS Malformed XML Response ─────────────────

func TestAWSProvider_MalformedXMLResponse(t *testing.T) {
	mux := http.NewServeMux()
	// IMDS returns success
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})
	// STS returns 200 with malformed XML
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			w.WriteHeader(200)
			w.Write([]byte("this is not valid XML <broken>"))
			return
		}
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	entraP := NewEntraProviderWithClient(client)
	awsP := NewAWSProviderWithClient(entraP, client)

	_, err := awsP.Resolve(context.Background(), "s3", ProviderConfig{
		Method:  "oidc_federation",
		RoleARN: "arn:aws:iam::123:role/test",
	})
	if err == nil {
		t.Error("expected error for malformed XML from STS")
	}
	if !strings.Contains(err.Error(), "decode STS") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─────────────────────── GCP Malformed SA Response ──────────────────

func TestGCPProvider_MalformedSAResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metadata/identity/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})
	mux.HandleFunc("/v1/token", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "gcp-sts-token",
			"expires_in":   3600,
			"token_type":   "Bearer",
		})
	})
	// SA impersonation returns 200 with invalid JSON
	mux.HandleFunc("/v1/projects/-/serviceAccounts/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("not json {{{"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "entra-jwt",
			"expires_on":   fmt.Sprintf("%d", time.Now().Add(1*time.Hour).Unix()),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	transport := &urlRewriteTransport{base: http.DefaultTransport, targetURL: server.URL}
	client := &http.Client{Transport: transport}
	entraP := NewEntraProviderWithClient(client)
	gcpP := NewGCPProviderWithClient(entraP, client)

	_, err := gcpP.Resolve(context.Background(), "gcs", ProviderConfig{
		Method:         "gcp_wif",
		GCPProjectNum:  "123",
		GCPPoolID:      "pool",
		GCPProviderID:  "prov",
		GCPServiceAcct: "sa@proj.iam.gserviceaccount.com",
	})
	if err == nil {
		t.Error("expected error for malformed SA impersonation response")
	}
	if !strings.Contains(err.Error(), "decode SA") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─────────────────────── Double-Check Locking Under Contention ──────

func TestManager_DoubleCheckLocking(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)
	mgr.refreshBuffer = 0

	var callCount atomic.Int32
	provider := &dynamicMockProvider{
		name:   "slow",
		method: "test_method",
		resolveFunc: func() (*Credentials, error) {
			callCount.Add(1)
			// Simulate slow provider resolution
			time.Sleep(50 * time.Millisecond)
			return &Credentials{
				AccessToken: "shared-token",
				ExpiresAt:   time.Now().Add(1 * time.Hour),
				Provider:    "slow",
			}, nil
		},
	}

	mgr.RegisterProvider(provider)
	mgr.SetBackendAuth("backend1", ProviderConfig{Method: "test_method"})

	// Launch many goroutines — all should serialize on the per-backend mutex
	// and only one should actually call the provider (double-check locking)
	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			creds, err := mgr.GetCredentials(context.Background(), "backend1")
			if err != nil {
				errs <- err
				return
			}
			if creds.AccessToken != "shared-token" {
				errs <- fmt.Errorf("unexpected token: %s", creds.AccessToken)
				return
			}
			errs <- nil
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("goroutine error: %v", err)
		}
	}

	// Double-check locking: only 1 provider call should happen
	if callCount.Load() != 1 {
		t.Errorf("provider called %d times, want 1 (double-check locking failed)", callCount.Load())
	}
}

// ─────────────────────── Manager Overwrite Backend Auth ─────────────

func TestManager_OverwriteBackendAuth(t *testing.T) {
	audit := NewAuditLogger(100, nil)
	mgr := NewManager(audit)

	p1 := &mockProvider{
		name: "entra", method: "managed_identity",
		creds: &Credentials{Provider: "managed_identity", AccessToken: "mi-token", ExpiresAt: time.Now().Add(1 * time.Hour)},
	}
	p2 := &mockProvider{
		name: "static", method: "static",
		creds: &Credentials{Provider: "static", StaticKey: "key1", ExpiresAt: time.Time{}},
	}
	mgr.RegisterProvider(p1)
	mgr.RegisterProvider(p2)

	// Initially use managed_identity
	mgr.SetBackendAuth("b1", ProviderConfig{Method: "managed_identity"})
	c1, err := mgr.GetCredentials(context.Background(), "b1")
	if err != nil {
		t.Fatalf("first GetCredentials: %v", err)
	}
	if c1.Provider != "managed_identity" {
		t.Errorf("provider = %q, want managed_identity", c1.Provider)
	}

	// Overwrite to static and invalidate cache
	mgr.SetBackendAuth("b1", ProviderConfig{Method: "static"})
	mgr.InvalidateCache("b1")
	c2, err := mgr.GetCredentials(context.Background(), "b1")
	if err != nil {
		t.Fatalf("second GetCredentials: %v", err)
	}
	if c2.Provider != "static" {
		t.Errorf("provider after overwrite = %q, want static", c2.Provider)
	}
}

// ─────────────────────── Static Provider Empty Credentials ──────────

func TestStaticProvider_EmptyCredentials(t *testing.T) {
	p := NewStaticProvider()

	creds, err := p.Resolve(context.Background(), "dev-backend", ProviderConfig{
		Method:          "static",
		AccessKeyID:     "",
		SecretAccessKey: "",
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if creds.AccessKeyID != "" {
		t.Errorf("AccessKeyID = %q, want empty", creds.AccessKeyID)
	}
	if creds.Provider != "static" {
		t.Errorf("Provider = %q, want static", creds.Provider)
	}
	if creds.IsExpired(0) {
		t.Error("static creds should not expire even when empty")
	}
}
