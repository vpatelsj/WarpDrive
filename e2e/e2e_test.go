package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/auth"
	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/cache"
	"github.com/warpdrive/warpdrive/pkg/config"
	kfuse "github.com/warpdrive/warpdrive/pkg/fuse"
	"github.com/warpdrive/warpdrive/pkg/namespace"
)

// testEnv holds all the moving parts for one e2e scenario.
type testEnv struct {
	sourceDir  string
	cacheDir   string
	mountPoint string
	cancel     context.CancelFunc
	done       chan error
}

// newTestEnv sets up directories, seeds test files, boots the full stack,
// and blocks until the FUSE mount is ready.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping FUSE e2e test in -short mode")
	}

	sourceDir := t.TempDir()
	cacheDir := t.TempDir()
	mountPoint := t.TempDir()

	seedTestFiles(t, sourceDir)

	env := &testEnv{
		sourceDir:  sourceDir,
		cacheDir:   cacheDir,
		mountPoint: mountPoint,
		done:       make(chan error, 1),
	}

	cfg := &config.Config{
		MountPoint: mountPoint,
		Cache: config.CacheConfig{
			Path:             cacheDir,
			MaxSizeRaw:       "512MB",
			BlockSizeRaw:     "4MB",
			ReadaheadBlocks:  2,
			MaxParallelFetch: 4,
		},
		Backends: []config.BackendConfig{
			{
				Name:      "local-e2e",
				Type:      "local",
				MountPath: "/data",
				Config: map[string]string{
					"root": sourceDir,
				},
			},
		},
	}

	if v, err := config.ParseSize(cfg.Cache.MaxSizeRaw); err == nil {
		cfg.Cache.MaxSize = v
	}
	if v, err := config.ParseSize(cfg.Cache.BlockSizeRaw); err == nil {
		cfg.Cache.BlockSize = v
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("local-e2e", "local", sourceDir, map[string]string{})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	// Wrap with auth (none provider for local testing)
	audit := auth.NewAuditLogger(100, nil)
	authMgr := auth.NewManager(audit)
	authMgr.RegisterProvider(auth.NewNoneProvider())
	authMgr.SetBackendAuth("local-e2e", auth.ProviderConfig{Method: "none"})
	authBe := backend.NewAuthenticatedBackend(be, authMgr)

	if err := reg.Register(authBe); err != nil {
		t.Fatalf("register backend: %v", err)
	}

	cacheMgr, err := cache.New(cfg.Cache, reg)
	if err != nil {
		t.Fatalf("create cache manager: %v", err)
	}

	ns := namespace.New(cfg.Backends)
	root := kfuse.NewRoot(ns, cacheMgr, reg)

	ctx, cancel := context.WithCancel(context.Background())
	env.cancel = cancel

	go func() {
		err := kfuse.Mount(ctx, kfuse.MountConfig{
			MountPoint: mountPoint,
			AllowOther: false,
		}, root)
		cacheMgr.Close()
		env.done <- err
	}()

	dataDir := filepath.Join(mountPoint, "data")
	if err := waitForMount(dataDir, 10*time.Second); err != nil {
		cancel()
		t.Fatalf("mount did not become ready: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-env.done:
			if err != nil {
				t.Logf("mount exit: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Log("WARNING: mount goroutine did not exit within 15s")
		}
	})

	return env
}

func seedTestFiles(t *testing.T, dir string) {
	t.Helper()

	writeFile(t, dir, "hello.txt", []byte("Hello, WarpDrive!\n"))
	writeFile(t, dir, "empty.txt", []byte{})

	sub := filepath.Join(dir, "subdir")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dir, "subdir/nested.txt", []byte("nested content\n"))

	bigBuf := make([]byte, 8*1024*1024)
	if _, err := io.ReadFull(rand.Reader, bigBuf); err != nil {
		t.Fatalf("rand: %v", err)
	}
	writeFile(t, dir, "random.bin", bigBuf)
}

func writeFile(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func waitForMount(targetPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(targetPath); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", targetPath)
}

// ──────────────────────────────── Tests ────────────────────────────────

func TestE2E_ListRoot(t *testing.T) {
	env := newTestEnv(t)

	entries, err := os.ReadDir(env.mountPoint)
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}

	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}

	if len(names) != 1 || names[0] != "data" {
		t.Errorf("root listing = %v; want [data]", names)
	}
}

func TestE2E_ListBackendDir(t *testing.T) {
	env := newTestEnv(t)

	dataDir := filepath.Join(env.mountPoint, "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("ReadDir data/: %v", err)
	}

	want := map[string]bool{
		"empty.txt":  true,
		"hello.txt":  true,
		"random.bin": true,
		"subdir":     true,
	}
	got := map[string]bool{}
	for _, e := range entries {
		got[e.Name()] = true
	}

	for name := range want {
		if !got[name] {
			t.Errorf("missing expected entry %q in data/", name)
		}
	}
}

func TestE2E_ReadSmallFile(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "hello.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if string(data) != "Hello, WarpDrive!\n" {
		t.Errorf("content = %q; want %q", string(data), "Hello, WarpDrive!\n")
	}
}

func TestE2E_ReadEmptyFile(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "empty.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if len(data) != 0 {
		t.Errorf("expected empty file, got %d bytes", len(data))
	}
}

func TestE2E_ReadNestedFile(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "subdir", "nested.txt")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if string(data) != "nested content\n" {
		t.Errorf("content = %q; want %q", string(data), "nested content\n")
	}
}

func TestE2E_ReadLargeFile(t *testing.T) {
	env := newTestEnv(t)

	mountPath := filepath.Join(env.mountPoint, "data", "random.bin")
	mountData, err := os.ReadFile(mountPath)
	if err != nil {
		t.Fatalf("ReadFile (mount): %v", err)
	}

	srcData, err := os.ReadFile(filepath.Join(env.sourceDir, "random.bin"))
	if err != nil {
		t.Fatalf("ReadFile (source): %v", err)
	}

	if !bytes.Equal(mountData, srcData) {
		t.Errorf("large file mismatch: mount=%d bytes, source=%d bytes", len(mountData), len(srcData))
	}
}

func TestE2E_StatFile(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "hello.txt")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if info.IsDir() {
		t.Error("hello.txt should not be a directory")
	}
	if info.Size() != int64(len("Hello, WarpDrive!\n")) {
		t.Errorf("Size = %d; want %d", info.Size(), len("Hello, WarpDrive!\n"))
	}
}

func TestE2E_StatDir(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "subdir")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	if !info.IsDir() {
		t.Error("subdir should be a directory")
	}
}

func TestE2E_FileNotFound(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "no-such-file.txt")
	_, err := os.Stat(path)
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
	if !os.IsNotExist(err) {
		t.Errorf("expected ENOENT, got: %v", err)
	}
}

func TestE2E_ReadOnlyRejectsWrite(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "hello.txt")
	err := os.WriteFile(path, []byte("hack"), 0o644)
	if err == nil {
		t.Error("write should be rejected on read-only mount")
	}
}

func TestE2E_PartialRead(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "hello.txt")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 5)
	n, err := f.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(buf[:n]) != "Hello" {
		t.Errorf("partial read = %q; want %q", string(buf[:n]), "Hello")
	}

	buf2 := make([]byte, 10)
	n, err = f.ReadAt(buf2, 7)
	if err != nil {
		t.Fatalf("ReadAt offset 7: %v", err)
	}
	if string(buf2[:n]) != "WarpDrive!" {
		t.Errorf("partial read = %q; want %q", string(buf2[:n]), "WarpDrive!")
	}
}

func TestE2E_ConcurrentReads(t *testing.T) {
	env := newTestEnv(t)

	path := filepath.Join(env.mountPoint, "data", "random.bin")
	srcData, err := os.ReadFile(filepath.Join(env.sourceDir, "random.bin"))
	if err != nil {
		t.Fatalf("read source: %v", err)
	}

	const goroutines = 8
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			data, err := os.ReadFile(path)
			if err != nil {
				errs <- fmt.Errorf("ReadFile: %w", err)
				return
			}
			if !bytes.Equal(data, srcData) {
				errs <- fmt.Errorf("data mismatch: got %d bytes", len(data))
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

// ──────────────────────── Multi-Backend Auth E2E Tests ────────────────────

// newMultiBackendEnv sets up a multi-backend environment with auth wrapping.
// Creates two local backends mounted at different paths.
func newMultiBackendEnv(t *testing.T) *multiBackendEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping FUSE e2e test in -short mode")
	}

	sourceDir1 := t.TempDir()
	sourceDir2 := t.TempDir()
	cacheDir := t.TempDir()
	mountPoint := t.TempDir()

	// Seed data in both sources
	writeFile(t, sourceDir1, "models/bert.bin", []byte("bert-weights-data"))
	writeFile(t, sourceDir1, "models/gpt.bin", []byte("gpt-weights-data"))
	writeFile(t, sourceDir2, "images/cat.jpg", []byte("cat-image-bytes"))
	writeFile(t, sourceDir2, "images/dog.jpg", []byte("dog-image-bytes"))

	cfg := &config.Config{
		MountPoint: mountPoint,
		Cache: config.CacheConfig{
			Path:             cacheDir,
			MaxSizeRaw:       "512MB",
			BlockSizeRaw:     "4MB",
			ReadaheadBlocks:  2,
			MaxParallelFetch: 4,
		},
		Backends: []config.BackendConfig{
			{
				Name:      "models",
				Type:      "local",
				MountPath: "/models",
				Config:    map[string]string{"root": sourceDir1},
				Auth:      config.BackendAuthConfig{Method: "none"},
			},
			{
				Name:      "datasets",
				Type:      "local",
				MountPath: "/datasets",
				Config:    map[string]string{"root": sourceDir2},
				Auth:      config.BackendAuthConfig{Method: "static"},
			},
		},
	}

	if v, err := config.ParseSize(cfg.Cache.MaxSizeRaw); err == nil {
		cfg.Cache.MaxSize = v
	}
	if v, err := config.ParseSize(cfg.Cache.BlockSizeRaw); err == nil {
		cfg.Cache.BlockSize = v
	}

	// Set up auth manager with multiple providers
	audit := auth.NewAuditLogger(100, nil)
	authMgr := auth.NewManager(audit)
	authMgr.RegisterProvider(auth.NewNoneProvider())
	authMgr.RegisterProvider(auth.NewStaticProvider())

	reg := backend.NewRegistry()
	for _, bcfg := range cfg.Backends {
		be, err := backend.NewRcloneBackend(bcfg.Name, bcfg.Type, bcfg.Config["root"], bcfg.Config)
		if err != nil {
			t.Fatalf("create backend %s: %v", bcfg.Name, err)
		}

		authCfg := auth.ProviderConfig{
			Method:          bcfg.Auth.Method,
			AccessKeyID:     bcfg.Auth.AccessKeyID,
			SecretAccessKey: bcfg.Auth.SecretAccessKey,
		}
		if authCfg.Method == "" {
			authCfg.Method = "none"
		}
		authMgr.SetBackendAuth(bcfg.Name, authCfg)
		authBe := backend.NewAuthenticatedBackend(be, authMgr)

		if err := reg.Register(authBe); err != nil {
			t.Fatalf("register backend %s: %v", bcfg.Name, err)
		}
	}

	cacheMgr, err := cache.New(cfg.Cache, reg)
	if err != nil {
		t.Fatalf("create cache manager: %v", err)
	}

	ns := namespace.New(cfg.Backends)
	root := kfuse.NewRoot(ns, cacheMgr, reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		err := kfuse.Mount(ctx, kfuse.MountConfig{
			MountPoint: mountPoint,
			AllowOther: false,
		}, root)
		cacheMgr.Close()
		done <- err
	}()

	// Wait for both mount paths
	modelsDir := filepath.Join(mountPoint, "models")
	datasetsDir := filepath.Join(mountPoint, "datasets")
	if err := waitForMount(modelsDir, 10*time.Second); err != nil {
		cancel()
		t.Fatalf("models mount not ready: %v", err)
	}
	if err := waitForMount(datasetsDir, 10*time.Second); err != nil {
		cancel()
		t.Fatalf("datasets mount not ready: %v", err)
	}

	env := &multiBackendEnv{
		sourceDir1: sourceDir1,
		sourceDir2: sourceDir2,
		cacheDir:   cacheDir,
		mountPoint: mountPoint,
		authMgr:    authMgr,
		audit:      audit,
		cancel:     cancel,
		done:       done,
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Logf("mount exit: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Log("WARNING: mount goroutine did not exit within 15s")
		}
	})

	return env
}

type multiBackendEnv struct {
	sourceDir1 string
	sourceDir2 string
	cacheDir   string
	mountPoint string
	authMgr    *auth.Manager
	audit      *auth.AuditLogger
	cancel     context.CancelFunc
	done       chan error
}

func TestE2E_MultiBackend_ListRoot(t *testing.T) {
	env := newMultiBackendEnv(t)

	entries, err := os.ReadDir(env.mountPoint)
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}

	if !names["models"] {
		t.Error("missing 'models' in root listing")
	}
	if !names["datasets"] {
		t.Error("missing 'datasets' in root listing")
	}
}

func TestE2E_MultiBackend_ReadAcrossBackends(t *testing.T) {
	env := newMultiBackendEnv(t)

	// Read from first backend
	data1, err := os.ReadFile(filepath.Join(env.mountPoint, "models", "models", "bert.bin"))
	if err != nil {
		t.Fatalf("ReadFile bert.bin: %v", err)
	}
	if string(data1) != "bert-weights-data" {
		t.Errorf("bert.bin = %q, want bert-weights-data", string(data1))
	}

	// Read from second backend
	data2, err := os.ReadFile(filepath.Join(env.mountPoint, "datasets", "images", "cat.jpg"))
	if err != nil {
		t.Fatalf("ReadFile cat.jpg: %v", err)
	}
	if string(data2) != "cat-image-bytes" {
		t.Errorf("cat.jpg = %q, want cat-image-bytes", string(data2))
	}
}

func TestE2E_MultiBackend_AuthIsolation(t *testing.T) {
	env := newMultiBackendEnv(t)

	// Read from both backends — each uses its own auth provider
	_, err1 := os.ReadFile(filepath.Join(env.mountPoint, "models", "models", "gpt.bin"))
	if err1 != nil {
		t.Fatalf("ReadFile gpt.bin: %v", err1)
	}

	_, err2 := os.ReadFile(filepath.Join(env.mountPoint, "datasets", "images", "dog.jpg"))
	if err2 != nil {
		t.Fatalf("ReadFile dog.jpg: %v", err2)
	}

	// Verify auth audit captured both backends
	entries := env.audit.Recent(10)
	backendsSeen := make(map[string]bool)
	for _, e := range entries {
		backendsSeen[e.BackendName] = true
	}

	if !backendsSeen["models"] {
		t.Error("audit missing 'models' backend entry")
	}
	if !backendsSeen["datasets"] {
		t.Error("audit missing 'datasets' backend entry")
	}
}

func TestE2E_MultiBackend_CrossBackendConcurrentReads(t *testing.T) {
	env := newMultiBackendEnv(t)

	const goroutines = 4
	errs := make(chan error, goroutines*2)

	// Concurrent reads from backend 1
	for i := 0; i < goroutines; i++ {
		go func() {
			data, err := os.ReadFile(filepath.Join(env.mountPoint, "models", "models", "bert.bin"))
			if err != nil {
				errs <- fmt.Errorf("models read: %w", err)
				return
			}
			if string(data) != "bert-weights-data" {
				errs <- fmt.Errorf("models data mismatch: %q", string(data))
				return
			}
			errs <- nil
		}()
	}

	// Concurrent reads from backend 2
	for i := 0; i < goroutines; i++ {
		go func() {
			data, err := os.ReadFile(filepath.Join(env.mountPoint, "datasets", "images", "cat.jpg"))
			if err != nil {
				errs <- fmt.Errorf("datasets read: %w", err)
				return
			}
			if string(data) != "cat-image-bytes" {
				errs <- fmt.Errorf("datasets data mismatch: %q", string(data))
				return
			}
			errs <- nil
		}()
	}

	for i := 0; i < goroutines*2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent cross-backend: %v", err)
		}
	}
}

// TestE2E_MultiBackend_FailureIsolation verifies that one backend failing
// does not crash the mount or affect the other healthy backend.
// Spec exit criterion: "Backend failure isolation: killing one backend's
// connectivity doesn't crash mount"
func TestE2E_MultiBackend_FailureIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping FUSE e2e test in -short mode")
	}

	sourceDir1 := t.TempDir()
	sourceDir2 := t.TempDir()
	cacheDir := t.TempDir()
	mountPoint := t.TempDir()

	// Seed data in both sources
	writeFile(t, sourceDir1, "model.bin", []byte("model-data"))
	writeFile(t, sourceDir2, "image.jpg", []byte("image-data"))

	cfg := &config.Config{
		MountPoint: mountPoint,
		Cache: config.CacheConfig{
			Path:             cacheDir,
			MaxSizeRaw:       "512MB",
			BlockSizeRaw:     "4MB",
			ReadaheadBlocks:  2,
			MaxParallelFetch: 4,
		},
		Backends: []config.BackendConfig{
			{
				Name:      "healthy",
				Type:      "local",
				MountPath: "/healthy",
				Config:    map[string]string{"root": sourceDir1},
				Auth:      config.BackendAuthConfig{Method: "none"},
			},
			{
				Name:      "doomed",
				Type:      "local",
				MountPath: "/doomed",
				Config:    map[string]string{"root": sourceDir2},
				Auth:      config.BackendAuthConfig{Method: "none"},
			},
		},
	}

	if v, err := config.ParseSize(cfg.Cache.MaxSizeRaw); err == nil {
		cfg.Cache.MaxSize = v
	}
	if v, err := config.ParseSize(cfg.Cache.BlockSizeRaw); err == nil {
		cfg.Cache.BlockSize = v
	}

	audit := auth.NewAuditLogger(100, nil)
	authMgr := auth.NewManager(audit)
	authMgr.RegisterProvider(auth.NewNoneProvider())

	reg := backend.NewRegistry()
	for _, bcfg := range cfg.Backends {
		be, err := backend.NewRcloneBackend(bcfg.Name, bcfg.Type, bcfg.Config["root"], bcfg.Config)
		if err != nil {
			t.Fatalf("create backend %s: %v", bcfg.Name, err)
		}
		authCfg := auth.ProviderConfig{Method: "none"}
		authMgr.SetBackendAuth(bcfg.Name, authCfg)
		authBe := backend.NewAuthenticatedBackend(be, authMgr)
		if err := reg.Register(authBe); err != nil {
			t.Fatalf("register backend %s: %v", bcfg.Name, err)
		}
	}

	cacheMgr, err := cache.New(cfg.Cache, reg)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}

	ns := namespace.New(cfg.Backends)
	root := kfuse.NewRoot(ns, cacheMgr, reg)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		err := kfuse.Mount(ctx, kfuse.MountConfig{
			MountPoint: mountPoint,
			AllowOther: false,
		}, root)
		cacheMgr.Close()
		done <- err
	}()

	healthyDir := filepath.Join(mountPoint, "healthy")
	doomedDir := filepath.Join(mountPoint, "doomed")
	if err := waitForMount(healthyDir, 10*time.Second); err != nil {
		cancel()
		t.Fatalf("healthy mount not ready: %v", err)
	}
	if err := waitForMount(doomedDir, 10*time.Second); err != nil {
		cancel()
		t.Fatalf("doomed mount not ready: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Logf("mount exit: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Log("WARNING: mount goroutine did not exit within 15s")
		}
	})

	// 1. Verify both backends initially work
	data1, err := os.ReadFile(filepath.Join(healthyDir, "model.bin"))
	if err != nil {
		t.Fatalf("read healthy before break: %v", err)
	}
	if string(data1) != "model-data" {
		t.Errorf("healthy data = %q, want model-data", string(data1))
	}

	data2, err := os.ReadFile(filepath.Join(doomedDir, "image.jpg"))
	if err != nil {
		t.Fatalf("read doomed before break: %v", err)
	}
	if string(data2) != "image-data" {
		t.Errorf("doomed data = %q, want image-data", string(data2))
	}

	// 2. "Kill" the doomed backend by removing its source data
	os.RemoveAll(sourceDir2)

	// 3. Healthy backend should still be perfectly functional
	data1Again, err := os.ReadFile(filepath.Join(healthyDir, "model.bin"))
	if err != nil {
		t.Fatalf("read healthy after doomed break: %v", err)
	}
	if string(data1Again) != "model-data" {
		t.Errorf("healthy data after break = %q, want model-data", string(data1Again))
	}

	// 4. Root listing should still work (mount is alive)
	entries, err := os.ReadDir(mountPoint)
	if err != nil {
		t.Fatalf("ReadDir root after break: %v", err)
	}
	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name()] = true
	}
	if !names["healthy"] {
		t.Error("missing 'healthy' in root listing after backend failure")
	}
	if !names["doomed"] {
		t.Error("missing 'doomed' in root listing after backend failure — mount point should still appear")
	}
}
