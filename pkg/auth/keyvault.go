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

// NewKeyVaultProvider creates a new Key Vault credential provider.
func NewKeyVaultProvider() *KeyVaultProvider {
	return &KeyVaultProvider{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewKeyVaultProviderWithClient creates a Key Vault provider with a custom HTTP client (for testing).
func NewKeyVaultProviderWithClient(client *http.Client) *KeyVaultProvider {
	return &KeyVaultProvider{client: client}
}

func (p *KeyVaultProvider) Name() string                      { return "keyvault" }
func (p *KeyVaultProvider) SupportsMethod(method string) bool { return method == "static_keyvault" }

func (p *KeyVaultProvider) Resolve(ctx context.Context, backendName string, cfg ProviderConfig) (*Credentials, error) {
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

	resp, err := retryDo(p.client, req)
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

	resp, err := retryDo(p.client, req)
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
