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
	entra  *EntraProvider
	client *http.Client
}

// NewAWSProvider creates an AWS STS federation provider.
func NewAWSProvider(entra *EntraProvider) *AWSProvider {
	return &AWSProvider{
		entra:  entra,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// NewAWSProviderWithClient creates an AWS provider with a custom HTTP client (for testing).
func NewAWSProviderWithClient(entra *EntraProvider, client *http.Client) *AWSProvider {
	return &AWSProvider{
		entra:  entra,
		client: client,
	}
}

func (p *AWSProvider) Name() string                      { return "aws_sts" }
func (p *AWSProvider) SupportsMethod(method string) bool { return method == "oidc_federation" }

func (p *AWSProvider) Resolve(ctx context.Context, backendName string, cfg ProviderConfig) (*Credentials, error) {
	// 1. Get Entra ID token (JWT) — this becomes the web identity token for AWS
	entraToken, err := p.entra.ResolveManagedIdentity(ctx)
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

	resp, err := retryDo(p.client, req)
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
		AccessKeyID:     stsResp.Result.Credentials.AccessKeyId,
		SecretAccessKey: stsResp.Result.Credentials.SecretAccessKey,
		SessionToken:    stsResp.Result.Credentials.SessionToken,
		ExpiresAt:       expiry,
		Provider:        "aws_sts",
	}, nil
}
