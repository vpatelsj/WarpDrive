package backend

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/warpdrive/warpdrive/pkg/auth"
)

// AuthenticatedBackend wraps a Backend with credential refresh before each operation.
type AuthenticatedBackend struct {
	inner   *RcloneBackend
	authMgr *auth.Manager

	mu        sync.Mutex
	lastCreds *auth.Credentials
}

// NewAuthenticatedBackend wraps a backend with auth credential refresh.
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

	// Only update credentials if they are non-trivial (not the "none" provider)
	if creds.Provider != "none" {
		ab.inner.UpdateCredentials(creds)
		slog.Debug("Auth credentials refreshed",
			"backend", ab.inner.Name(),
			"provider", creds.Provider,
		)
	}

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
