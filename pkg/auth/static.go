package auth

import (
	"context"
)

// StaticProvider provides static credentials directly from config.
// Used for testing and local dev (e.g., S3-compatible stores with fixed keys).
type StaticProvider struct{}

// NewStaticProvider creates a static credential provider.
func NewStaticProvider() *StaticProvider {
	return &StaticProvider{}
}

func (p *StaticProvider) Name() string                      { return "static" }
func (p *StaticProvider) SupportsMethod(method string) bool { return method == "static" }

func (p *StaticProvider) Resolve(_ context.Context, _ string, cfg ProviderConfig) (*Credentials, error) {
	return &Credentials{
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		StaticKey:       cfg.AccessKeyID,
		StaticSecret:    cfg.SecretAccessKey,
		Provider:        "static",
		// ExpiresAt zero => never expires
	}, nil
}

// NoneProvider handles backends that require no auth (e.g., local filesystem).
type NoneProvider struct{}

// NewNoneProvider creates a no-auth provider.
func NewNoneProvider() *NoneProvider {
	return &NoneProvider{}
}

func (p *NoneProvider) Name() string                      { return "none" }
func (p *NoneProvider) SupportsMethod(method string) bool { return method == "none" || method == "" }

func (p *NoneProvider) Resolve(_ context.Context, _ string, _ ProviderConfig) (*Credentials, error) {
	return &Credentials{
		Provider: "none",
	}, nil
}
