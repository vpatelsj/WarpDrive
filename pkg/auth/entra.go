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

// NewEntraProvider creates a new Entra provider.
func NewEntraProvider() *EntraProvider {
	return &EntraProvider{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewEntraProviderWithClient creates a provider with a custom HTTP client (for testing).
func NewEntraProviderWithClient(client *http.Client) *EntraProvider {
	return &EntraProvider{client: client}
}

func (p *EntraProvider) Name() string { return "entra" }

func (p *EntraProvider) SupportsMethod(method string) bool {
	return method == "managed_identity" || method == "service_principal"
}

func (p *EntraProvider) Resolve(ctx context.Context, backendName string, cfg ProviderConfig) (*Credentials, error) {
	switch cfg.Method {
	case "managed_identity":
		return p.ResolveManagedIdentity(ctx)
	case "service_principal":
		return p.resolveServicePrincipal(ctx, cfg)
	default:
		return nil, fmt.Errorf("entra: unsupported method %s", cfg.Method)
	}
}

// ResolveManagedIdentity gets a token from Azure Instance Metadata Service (IMDS).
// Works on Azure VMs, VMSS, and AKS pods with managed identity assigned.
func (p *EntraProvider) ResolveManagedIdentity(ctx context.Context) (*Credentials, error) {
	return p.resolveManagedIdentityForResource(ctx, "https://storage.azure.com/")
}

// resolveManagedIdentityForResource gets a managed identity token for the specified resource.
func (p *EntraProvider) resolveManagedIdentityForResource(ctx context.Context, resource string) (*Credentials, error) {
	imdsURL := "http://169.254.169.254/metadata/identity/oauth2/token"
	params := url.Values{
		"api-version": {"2018-02-01"},
		"resource":    {resource},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", imdsURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("entra.managedIdentity: build request: %w", err)
	}
	req.Header.Set("Metadata", "true")

	resp, err := retryDo(p.client, req)
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

	expiresOnSec, _ := strconv.ParseInt(result.ExpiresOn, 10, 64)
	expiresAt := time.Unix(expiresOnSec, 0)

	return &Credentials{
		AccessToken: result.AccessToken,
		ExpiresAt:   expiresAt,
		Provider:    "managed_identity",
	}, nil
}

// resolveServicePrincipal gets a token using client credentials flow.
func (p *EntraProvider) resolveServicePrincipal(ctx context.Context, cfg ProviderConfig) (*Credentials, error) {
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

	resp, err := retryDo(p.client, req)
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
