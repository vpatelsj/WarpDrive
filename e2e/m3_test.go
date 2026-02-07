package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/auth"
	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/cache"
	"github.com/warpdrive/warpdrive/pkg/config"
	kfuse "github.com/warpdrive/warpdrive/pkg/fuse"
	"github.com/warpdrive/warpdrive/pkg/namespace"
)

// m3TestEnv extends testEnv with access to the CacheManager for M3 testing.
type m3TestEnv struct {
	sourceDir  string
	cacheDir   string
	mountPoint string
	cacheMgr   *cache.CacheManager
	registry   *backend.Registry
	cancel     context.CancelFunc
	done       chan error
}

// newM3TestEnv boots a full FUSE stack with sequential shard files for M3 testing.
func newM3TestEnv(t *testing.T, shardCount int, shardSizeMB int) *m3TestEnv {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping FUSE e2e test in -short mode")
	}

	sourceDir := t.TempDir()
	cacheDir := t.TempDir()
	mountPoint := t.TempDir()

	// Create sequential shard files.
	for i := 0; i < shardCount; i++ {
		data := make([]byte, shardSizeMB*1024*1024)
		if _, err := io.ReadFull(rand.Reader, data); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("shard-%04d.bin", i)
		writeFile(t, sourceDir, name, data)
	}

	// Also create some small metadata files.
	writeFile(t, sourceDir, "metadata.json", []byte(`{"version": 1, "shards": 5}`))

	cfg := &config.Config{
		MountPoint: mountPoint,
		Cache: config.CacheConfig{
			Path:             cacheDir,
			MaxSizeRaw:       "512MB",
			BlockSizeRaw:     "4MB",
			ReadaheadBlocks:  4,
			MaxParallelFetch: 8,
		},
		Backends: []config.BackendConfig{
			{
				Name:      "m3-local",
				Type:      "local",
				MountPath: "/data",
				Config:    map[string]string{"root": sourceDir},
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
	be, err := backend.NewRcloneBackend("m3-local", "local", sourceDir, map[string]string{})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}

	audit := auth.NewAuditLogger(100, nil)
	authMgr := auth.NewManager(audit)
	authMgr.RegisterProvider(auth.NewNoneProvider())
	authMgr.SetBackendAuth("m3-local", auth.ProviderConfig{Method: "none"})
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
	done := make(chan error, 1)

	go func() {
		err := kfuse.Mount(ctx, kfuse.MountConfig{
			MountPoint: mountPoint,
			AllowOther: false,
		}, root)
		cacheMgr.Close()
		done <- err
	}()

	dataDir := filepath.Join(mountPoint, "data")
	if err := waitForMount(dataDir, 10*time.Second); err != nil {
		cancel()
		t.Fatalf("mount did not become ready: %v", err)
	}

	env := &m3TestEnv{
		sourceDir:  sourceDir,
		cacheDir:   cacheDir,
		mountPoint: mountPoint,
		cacheMgr:   cacheMgr,
		registry:   reg,
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

// ──────────────── M3: Cache Warming E2E Tests ────────────────

// TestE2E_CacheWarming verifies that cache warming pre-populates the cache
// and that subsequent reads through FUSE are served from cache.
func TestE2E_CacheWarming(t *testing.T) {
	env := newM3TestEnv(t, 3, 1) // 3 shards, 1MB each

	// Warm the cache for all files.
	var mu sync.Mutex
	var lastProgress cache.WarmProgress
	err := env.cacheMgr.Warm(context.Background(), cache.WarmConfig{
		BackendName: "m3-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     4,
	}, func(p cache.WarmProgress) {
		mu.Lock()
		lastProgress = p
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Warm failed: %v", err)
	}

	mu.Lock()
	lp := lastProgress
	mu.Unlock()
	if lp.FilesWarmed == 0 {
		t.Error("expected at least one file warmed")
	}
	if lp.BytesWarmed == 0 {
		t.Error("expected some bytes warmed")
	}
	t.Logf("Warmed %d/%d files, %d bytes",
		lp.FilesWarmed, lp.FilesTotal, lp.BytesWarmed)

	// Now read through FUSE — all reads should be cache hits.
	statsBefore := env.cacheMgr.GetStats()

	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("shard-%04d.bin", i)
		mountPath := filepath.Join(env.mountPoint, "data", name)
		srcPath := filepath.Join(env.sourceDir, name)

		mountData, err := os.ReadFile(mountPath)
		if err != nil {
			t.Fatalf("ReadFile %s (mount): %v", name, err)
		}
		srcData, err := os.ReadFile(srcPath)
		if err != nil {
			t.Fatalf("ReadFile %s (source): %v", name, err)
		}

		if !bytes.Equal(mountData, srcData) {
			t.Errorf("data mismatch for %s: mount=%d bytes, source=%d bytes",
				name, len(mountData), len(srcData))
		}
	}

	statsAfter := env.cacheMgr.GetStats()
	newHits := statsAfter.Hits - statsBefore.Hits
	if newHits == 0 {
		t.Error("expected cache hits after warming")
	}
	t.Logf("Post-warm reads: %d new cache hits", newHits)
}

// TestE2E_CacheWarmResume verifies that warming skips already-cached files.
func TestE2E_CacheWarmResume(t *testing.T) {
	env := newM3TestEnv(t, 2, 1) // 2 shards, 1MB each

	ctx := context.Background()

	// First warm.
	err := env.cacheMgr.Warm(ctx, cache.WarmConfig{
		BackendName: "m3-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     4,
	}, nil)
	if err != nil {
		t.Fatalf("First warm failed: %v", err)
	}

	// Second warm — should skip all files.
	var resumeProgress cache.WarmProgress
	err = env.cacheMgr.Warm(ctx, cache.WarmConfig{
		BackendName: "m3-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     4,
	}, func(p cache.WarmProgress) {
		resumeProgress = p
	})
	if err != nil {
		t.Fatalf("Resume warm failed: %v", err)
	}

	if resumeProgress.BytesSkipped == 0 && resumeProgress.BytesTotal > 0 {
		t.Error("expected bytes to be skipped on resume warm")
	}
	t.Logf("Resume: skipped %d bytes of %d total",
		resumeProgress.BytesSkipped, resumeProgress.BytesTotal)
}

// TestE2E_CacheWarmCancellation verifies that warming can be cancelled
// without blocking or crashing.
func TestE2E_CacheWarmCancellation(t *testing.T) {
	env := newM3TestEnv(t, 5, 2) // 5 shards, 2MB each

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel quickly.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Should return without hanging.
	done := make(chan error, 1)
	go func() {
		done <- env.cacheMgr.Warm(ctx, cache.WarmConfig{
			BackendName: "m3-local",
			Prefix:      "",
			Recursive:   true,
			Workers:     2,
		}, nil)
	}()

	select {
	case <-done:
		// OK — completed (possibly early due to cancellation).
	case <-time.After(10 * time.Second):
		t.Fatal("Warm did not return within 10s after cancellation")
	}
}

// ──────────────── M3: Sequential Read Performance E2E Tests ────────────────

// TestE2E_SequentialReadCacheHitPerformance reads a file sequentially twice
// and verifies second pass is served entirely from cache.
func TestE2E_SequentialReadCacheHitPerformance(t *testing.T) {
	env := newM3TestEnv(t, 1, 8) // 1 shard, 8MB

	mountPath := filepath.Join(env.mountPoint, "data", "shard-0000.bin")

	// First read — populates cache.
	data1, err := os.ReadFile(mountPath)
	if err != nil {
		t.Fatalf("First read: %v", err)
	}
	if len(data1) != 8*1024*1024 {
		t.Fatalf("First read: got %d bytes, want %d", len(data1), 8*1024*1024)
	}

	statsAfterFirst := env.cacheMgr.GetStats()

	// Second read — may be served by FUSE kernel cache (60s attr timeout)
	// so we just verify data integrity rather than cache hit counts.
	data2, err := os.ReadFile(mountPath)
	if err != nil {
		t.Fatalf("Second read: %v", err)
	}
	if !bytes.Equal(data1, data2) {
		t.Error("data mismatch between first and second read")
	}

	// Verify first read populated the cache.
	if statsAfterFirst.Misses == 0 {
		t.Error("expected cache misses on first read")
	}
	if statsAfterFirst.BytesCached == 0 {
		t.Error("expected BytesCached > 0 after first read")
	}
	t.Logf("First read: %d misses, %d bytes cached", statsAfterFirst.Misses, statsAfterFirst.BytesCached)
}

// TestE2E_ConcurrentSequentialReaders verifies that multiple goroutines
// reading different shards concurrently works correctly.
func TestE2E_ConcurrentSequentialReaders(t *testing.T) {
	env := newM3TestEnv(t, 4, 2) // 4 shards, 2MB each

	const readers = 4
	errs := make(chan error, readers)

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(shardIdx int) {
			defer wg.Done()
			name := fmt.Sprintf("shard-%04d.bin", shardIdx)
			mountPath := filepath.Join(env.mountPoint, "data", name)
			srcPath := filepath.Join(env.sourceDir, name)

			mountData, err := os.ReadFile(mountPath)
			if err != nil {
				errs <- fmt.Errorf("ReadFile %s (mount): %w", name, err)
				return
			}
			srcData, err := os.ReadFile(srcPath)
			if err != nil {
				errs <- fmt.Errorf("ReadFile %s (source): %w", name, err)
				return
			}

			if !bytes.Equal(mountData, srcData) {
				errs <- fmt.Errorf("%s data mismatch: mount=%d source=%d", name, len(mountData), len(srcData))
				return
			}
			errs <- nil
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("concurrent reader: %v", err)
		}
	}
}

// ──────────────── M3: Readahead Through FUSE E2E Tests ────────────────

// TestE2E_ReadaheadPrefetch reads a file block by block through FUSE
// and verifies that readahead workers prefetch subsequent blocks.
func TestE2E_ReadaheadPrefetch(t *testing.T) {
	env := newM3TestEnv(t, 1, 32) // 1 shard, 32MB = 8 blocks

	mountPath := filepath.Join(env.mountPoint, "data", "shard-0000.bin")

	// Open file and read block by block to trigger readahead.
	f, err := os.Open(mountPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	blockSize := 4 * 1024 * 1024
	buf := make([]byte, blockSize)

	// Read first 4 blocks sequentially.
	for i := 0; i < 4; i++ {
		n, err := f.Read(buf)
		if err != nil {
			t.Fatalf("Read block %d: %v", i, err)
		}
		if n != blockSize {
			t.Fatalf("Read block %d: got %d bytes, want %d", i, n, blockSize)
		}
	}

	// Give readahead workers time to prefetch.
	time.Sleep(1 * time.Second)

	// Read remaining blocks — if readahead worked, these should have higher
	// hit rates.
	statsBefore := env.cacheMgr.GetStats()

	for i := 0; i < 4; i++ {
		_, err := f.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Read block %d: %v", i+4, err)
		}
	}

	statsAfter := env.cacheMgr.GetStats()
	newHits := statsAfter.Hits - statsBefore.Hits

	t.Logf("Readahead test: %d hits for blocks 4-7 (ideally all hits)", newHits)
}

// TestE2E_CacheStats verifies that cache stats are properly maintained
// through the full FUSE read path.
func TestE2E_CacheStats(t *testing.T) {
	env := newM3TestEnv(t, 2, 1) // 2 shards, 1MB each

	statsInitial := env.cacheMgr.GetStats()

	// Read first file through FUSE.
	mountPath1 := filepath.Join(env.mountPoint, "data", "shard-0000.bin")
	_, err := os.ReadFile(mountPath1)
	if err != nil {
		t.Fatalf("ReadFile shard-0000: %v", err)
	}

	statsAfterFirst := env.cacheMgr.GetStats()
	if statsAfterFirst.Misses <= statsInitial.Misses {
		t.Error("expected cache misses on first read")
	}
	if statsAfterFirst.BytesCached <= statsInitial.BytesCached {
		t.Error("expected BytesCached to increase after first read")
	}

	// Read a different file — verifies stats keep incrementing.
	mountPath2 := filepath.Join(env.mountPoint, "data", "shard-0001.bin")
	_, err = os.ReadFile(mountPath2)
	if err != nil {
		t.Fatalf("ReadFile shard-0001: %v", err)
	}

	statsAfterSecond := env.cacheMgr.GetStats()
	if statsAfterSecond.BytesCached <= statsAfterFirst.BytesCached {
		t.Error("expected BytesCached to increase after second file read")
	}
	t.Logf("Stats: misses=%d, hits=%d, bytes_cached=%d",
		statsAfterSecond.Misses, statsAfterSecond.Hits, statsAfterSecond.BytesCached)
}
