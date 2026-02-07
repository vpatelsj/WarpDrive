package backend

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/auth"
)

// mockAuthProvider is a controllable auth provider for testing.
type mockAuthProvider struct {
	name      string
	method    string
	creds     func() *auth.Credentials
	err       error
	callCount atomic.Int32
}

func (m *mockAuthProvider) Name() string                      { return m.name }
func (m *mockAuthProvider) SupportsMethod(method string) bool { return method == m.method }
func (m *mockAuthProvider) Resolve(_ context.Context, _ string, _ auth.ProviderConfig) (*auth.Credentials, error) {
	m.callCount.Add(1)
	if m.err != nil {
		return nil, m.err
	}
	return m.creds(), nil
}

func newTestAuthenticatedBackend(t *testing.T, providerMethod string, creds func() *auth.Credentials) (*AuthenticatedBackend, *mockAuthProvider) {
	t.Helper()

	dir := t.TempDir()
	populateTestDir(t, dir)

	inner, err := NewRcloneBackend("test-auth-be", "local", dir, map[string]string{})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	mp := &mockAuthProvider{
		name:   "test",
		method: providerMethod,
		creds:  creds,
	}

	audit := auth.NewAuditLogger(100, nil)
	mgr := auth.NewManager(audit)
	mgr.RegisterProvider(mp)
	mgr.SetBackendAuth("test-auth-be", auth.ProviderConfig{Method: providerMethod})

	ab := NewAuthenticatedBackend(inner, mgr)
	return ab, mp
}

func TestAuthenticatedBackend_Name(t *testing.T) {
	ab, _ := newTestAuthenticatedBackend(t, "test_method", func() *auth.Credentials {
		return &auth.Credentials{Provider: "test", ExpiresAt: time.Now().Add(1 * time.Hour)}
	})
	defer ab.Close()

	if ab.Name() != "test-auth-be" {
		t.Errorf("Name() = %q, want test-auth-be", ab.Name())
	}
	if ab.Type() != "local" {
		t.Errorf("Type() = %q, want local", ab.Type())
	}
}

func TestAuthenticatedBackend_RefreshesOnFirstCall(t *testing.T) {
	ab, mp := newTestAuthenticatedBackend(t, "test_method", func() *auth.Credentials {
		return &auth.Credentials{
			AccessToken: "fresh-token",
			Provider:    "test",
			ExpiresAt:   time.Now().Add(1 * time.Hour),
		}
	})
	defer ab.Close()

	entries, err := ab.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) < 3 {
		t.Errorf("List returned %d entries, want >= 3", len(entries))
	}

	// Provider should have been called once for the initial credential fetch
	if mp.callCount.Load() != 1 {
		t.Errorf("provider called %d times, want 1", mp.callCount.Load())
	}
}

func TestAuthenticatedBackend_CachesCredentials(t *testing.T) {
	ab, mp := newTestAuthenticatedBackend(t, "test_method", func() *auth.Credentials {
		return &auth.Credentials{
			Provider:  "test",
			ExpiresAt: time.Now().Add(1 * time.Hour),
		}
	})
	defer ab.Close()

	ctx := context.Background()

	// Multiple operations should reuse cached creds
	ab.List(ctx, "")
	ab.Stat(ctx, "file1.txt")
	ab.List(ctx, "")

	// Auth manager also caches, so provider should only be called once
	if mp.callCount.Load() != 1 {
		t.Errorf("provider called %d times, want 1", mp.callCount.Load())
	}
}

func TestAuthenticatedBackend_NoneProviderSkipsUpdate(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)

	inner, err := NewRcloneBackend("none-be", "local", dir, map[string]string{})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	audit := auth.NewAuditLogger(100, nil)
	mgr := auth.NewManager(audit)
	mgr.RegisterProvider(auth.NewNoneProvider())
	mgr.SetBackendAuth("none-be", auth.ProviderConfig{Method: "none"})

	ab := NewAuthenticatedBackend(inner, mgr)
	defer ab.Close()

	entries, err := ab.List(context.Background(), "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) < 3 {
		t.Errorf("List returned %d entries, want >= 3", len(entries))
	}
}

func TestAuthenticatedBackend_ErrorPropagation(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)

	inner, err := NewRcloneBackend("err-be", "local", dir, map[string]string{})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	failProvider := &mockAuthProvider{
		name:   "failing",
		method: "fail_method",
		creds:  func() *auth.Credentials { return nil },
		err:    fmt.Errorf("token endpoint unreachable"),
	}

	audit := auth.NewAuditLogger(100, nil)
	mgr := auth.NewManager(audit)
	mgr.RegisterProvider(failProvider)
	mgr.SetBackendAuth("err-be", auth.ProviderConfig{Method: "fail_method"})

	ab := NewAuthenticatedBackend(inner, mgr)
	defer ab.Close()

	_, err = ab.List(context.Background(), "")
	if err == nil {
		t.Error("expected error when auth provider fails")
	}

	_, err = ab.Stat(context.Background(), "file1.txt")
	if err == nil {
		t.Error("expected error for Stat when auth fails")
	}

	buf := make([]byte, 10)
	_, err = ab.ReadAt(context.Background(), "file1.txt", buf, 0)
	if err == nil {
		t.Error("expected error for ReadAt when auth fails")
	}

	_, err = ab.Open(context.Background(), "file1.txt")
	if err == nil {
		t.Error("expected error for Open when auth fails")
	}

	err = ab.Write(context.Background(), "file1.txt", nil, 0)
	if err == nil {
		t.Error("expected error for Write when auth fails")
	}

	err = ab.Delete(context.Background(), "file1.txt")
	if err == nil {
		t.Error("expected error for Delete when auth fails")
	}
}

func TestAuthenticatedBackend_ConcurrentAccess(t *testing.T) {
	ab, _ := newTestAuthenticatedBackend(t, "test_method", func() *auth.Credentials {
		return &auth.Credentials{
			Provider:  "test",
			ExpiresAt: time.Now().Add(1 * time.Hour),
		}
	})
	defer ab.Close()

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ab.List(context.Background(), "")
			if err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent access error: %v", err)
	}
}

func TestAuthenticatedBackend_DelegatesAllOps(t *testing.T) {
	ab, _ := newTestAuthenticatedBackend(t, "test_method", func() *auth.Credentials {
		return &auth.Credentials{
			Provider:  "test",
			ExpiresAt: time.Now().Add(1 * time.Hour),
		}
	})
	defer ab.Close()

	ctx := context.Background()

	// List
	entries, err := ab.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) == 0 {
		t.Error("List returned no entries")
	}

	// Stat
	info, err := ab.Stat(ctx, "file1.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != 11 {
		t.Errorf("file1.txt size = %d, want 11", info.Size)
	}

	// ReadAt
	buf := make([]byte, 5)
	n, err := ab.ReadAt(ctx, "file1.txt", buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("ReadAt = %q, want hello", string(buf[:n]))
	}

	// Open
	rc, err := ab.Open(ctx, "file1.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	rc.Close()
}
