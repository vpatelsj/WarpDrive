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

// NewGCPProvider creates a GCP Workload Identity Federation provider.
func NewGCPProvider(entra *EntraProvider) *GCPProvider {
	return &GCPProvider{
		entra:  entra,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewGCPProviderWithClient creates a GCP provider with a custom HTTP client (for testing).
func NewGCPProviderWithClient(entra *EntraProvider, client *http.Client) *GCPProvider {
	return &GCPProvider{
		entra:  entra,
		client: client,
	}
}

func (p *GCPProvider) Name() string                      { return "gcp_wif" }
func (p *GCPProvider) SupportsMethod(method string) bool { return method == "gcp_wif" }

func (p *GCPProvider) Resolve(ctx context.Context, backendName string, cfg ProviderConfig) (*Credentials, error) {
	// 1. Get Entra token
	entraToken, err := p.entra.ResolveManagedIdentity(ctx)
	if err != nil {
		return nil, fmt.Errorf("gcp.Resolve: get entra token: %w", err)
	}

	// 2. Exchange Entra JWT for GCP STS federation token
	audience := fmt.Sprintf(
		"//iam.googleapis.com/projects/%s/locations/global/workloadIdentityPools/%s/providers/%s",
		cfg.GCPProjectNum, cfg.GCPPoolID, cfg.GCPProviderID,
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

	resp, err := retryDo(p.client, req)
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
		cfg.GCPServiceAcct,
	)
	saBody := `{"scope":["https://www.googleapis.com/auth/devstorage.read_only"],"lifetime":"3600s"}`

	saReq, err := http.NewRequestWithContext(ctx, "POST", saURL, strings.NewReader(saBody))
	if err != nil {
		return nil, fmt.Errorf("gcp.Resolve: build SA request: %w", err)
	}
	saReq.Header.Set("Content-Type", "application/json")
	saReq.Header.Set("Authorization", "Bearer "+stsResp.AccessToken)

	saResp, err := retryDo(p.client, saReq)
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
