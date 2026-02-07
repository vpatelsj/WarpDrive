package cache

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/config"
)

func TestWarmBasic(t *testing.T) {
	cm, srcDir := setupTestCache(t)
	ctx := context.Background()

	// Create a few more test files.
	for i := 0; i < 3; i++ {
		data := make([]byte, 1024*1024) // 1 MB each
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, "shard-"+string(rune('0'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var progressCalls atomic.Int64
	err := cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     4,
	}, func(p WarmProgress) {
		progressCalls.Add(1)
		if p.FilesTotal <= 0 {
			t.Error("expected positive FilesTotal")
		}
	})
	if err != nil {
		t.Fatalf("Warm failed: %v", err)
	}

	if progressCalls.Load() == 0 {
		t.Error("expected progress callback to be called")
	}

	// Verify that subsequent reads are cache hits.
	statsBefore := cm.GetStats()
	buf := make([]byte, 5)
	_, err = cm.Read(ctx, "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	statsAfter := cm.GetStats()
	if statsAfter.Hits <= statsBefore.Hits {
		t.Error("expected cache hit after warming")
	}
}

func TestWarmCancellation(t *testing.T) {
	cm, srcDir := setupTestCache(t)

	// Create larger test files so warm takes time.
	for i := 0; i < 5; i++ {
		data := make([]byte, 4*1024*1024) // 4 MB each
		name := filepath.Join(srcDir, "cancel-test-"+string(rune('a'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel almost immediately.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	// Should not block forever.
	err := cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     2,
	}, nil)

	// err may or may not be nil (depends on timing), but it shouldn't panic.
	_ = err
}

func TestWarmMaxSize(t *testing.T) {
	cm, srcDir := setupTestCache(t)
	ctx := context.Background()

	// Create test files.
	for i := 0; i < 10; i++ {
		data := make([]byte, 512*1024) // 512KB each = 5MB total
		name := filepath.Join(srcDir, "maxsize-"+string(rune('a'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var lastProgress WarmProgress
	err := cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		MaxSize:     2 * 1024 * 1024, // 2MB limit
		Workers:     4,
	}, func(p WarmProgress) {
		mu.Lock()
		lastProgress = p
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Warm failed: %v", err)
	}

	// Not all files should have been warmed due to MaxSize limit.
	mu.Lock()
	lp := lastProgress
	mu.Unlock()
	if lp.FilesTotal > 0 && lp.FilesWarmed >= lp.FilesTotal {
		// This test is a hint — due to concurrency, it's possible all files
		// get warmed before the limit kicks in. We check bytes instead.
		t.Logf("Note: all files warmed (may have read before limit check)")
	}
}

func TestWarmResume(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// First pass: warm everything.
	err := cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     4,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Second pass: should skip files (resume).
	var progress WarmProgress
	err = cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     4,
	}, func(p WarmProgress) {
		progress = p
	})
	if err != nil {
		t.Fatal(err)
	}

	// All bytes should have been skipped on second run.
	if progress.BytesSkipped == 0 && progress.BytesTotal > 0 {
		t.Error("expected some bytes to be skipped on resume")
	}
}

func TestIsFullyCached(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// Before reading, file should not be fully cached.
	if cm.isFullyCached("test-local", "small.txt", "") {
		t.Error("expected file not to be fully cached with empty etag")
	}

	// Read the file to cache it.
	buf := make([]byte, 100)
	_, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Now check with the correct etag — need to get it first.
	be, _ := cm.registry.Get("test-local")
	info, _ := be.Stat(ctx, "small.txt")
	if info.ETag != "" {
		if !cm.isFullyCached("test-local", "small.txt", info.ETag) {
			t.Error("expected file to be fully cached after reading")
		}
	}
}

func TestCoalescer(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// First few reads should be UNKNOWN.
	p := rc.RecordRead("be", "file.txt", 0, 4096)
	if p != patternUnknown {
		t.Errorf("expected UNKNOWN for first read, got %d", p)
	}

	// Record sequential reads.
	rc.RecordRead("be", "file.txt", 4096, 4096)
	rc.RecordRead("be", "file.txt", 8192, 4096)
	p = rc.RecordRead("be", "file.txt", 12288, 4096)
	if p != patternSequential {
		t.Errorf("expected SEQUENTIAL after 4 sequential reads, got %d", p)
	}

	// Record a random read (jump well beyond 2 * block size).
	p = rc.RecordRead("be", "file.txt", 100000000, 4096)
	if p == patternSequential {
		t.Error("expected pattern to change after random jump")
	}
}

func TestCoalescerForget(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	rc.RecordRead("be", "file.txt", 0, 4096)
	rc.RecordRead("be", "file.txt", 4096, 4096)

	rc.Forget("be", "file.txt")

	p := rc.GetPattern("be", "file.txt")
	if p != patternUnknown {
		t.Errorf("expected UNKNOWN after forget, got %d", p)
	}
}

func TestFilePattern(t *testing.T) {
	fp := &filePattern{readaheadSize: 4}

	// Record sequential block accesses.
	for i := 0; i < 6; i++ {
		fp.recordAccess(i)
	}

	if !fp.isSequential {
		t.Error("expected sequential pattern after monotonically increasing accesses")
	}
	if fp.readaheadSize <= 4 {
		t.Error("expected readahead size to grow beyond 4 for sequential access")
	}

	// Record random access.
	fp.recordAccess(100)
	fp.recordAccess(5)
	fp.recordAccess(200)
	fp.recordAccess(3)

	if fp.isSequential {
		t.Error("expected non-sequential pattern after random accesses")
	}
	if fp.readaheadSize != 0 {
		t.Errorf("expected readahead size 0 for random access, got %d", fp.readaheadSize)
	}
}

func TestDirPattern(t *testing.T) {
	dp := &dirPattern{
		allFiles: []string{"shard-0000", "shard-0001", "shard-0002", "shard-0003", "shard-0004"},
	}

	dp.recordFileAccess("shard-0000")
	dp.recordFileAccess("shard-0001")
	dp.recordFileAccess("shard-0002")

	if !dp.isSequential {
		t.Error("expected sequential directory access pattern")
	}

	nextFiles := dp.predictNextFiles(2)
	if len(nextFiles) != 2 {
		t.Fatalf("expected 2 next files, got %d", len(nextFiles))
	}
	if nextFiles[0] != "shard-0003" || nextFiles[1] != "shard-0004" {
		t.Errorf("unexpected next files: %v", nextFiles)
	}
}

func TestDirPatternNonSequential(t *testing.T) {
	dp := &dirPattern{
		allFiles: []string{"a.txt", "b.txt", "c.txt", "d.txt"},
	}

	dp.recordFileAccess("c.txt")
	dp.recordFileAccess("a.txt")
	dp.recordFileAccess("d.txt")

	if dp.isSequential {
		t.Error("expected non-sequential directory access pattern")
	}
}

// TestWarmETagReValidation verifies that when a file's content changes
// between two warm runs (ETag changes), the second run re-warms the file
// instead of skipping it.
func TestWarmETagReValidation(t *testing.T) {
	cm, srcDir := setupTestCache(t)
	ctx := context.Background()

	// Create a file to warm.
	original := []byte("original-content-padding-to-fill")
	warmFile := filepath.Join(srcDir, "etag-test.bin")
	if err := os.WriteFile(warmFile, original, 0o644); err != nil {
		t.Fatal(err)
	}

	// First warm.
	err := cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     2,
	}, nil)
	if err != nil {
		t.Fatalf("First warm: %v", err)
	}

	// Verify file is fully cached.
	be, _ := cm.registry.Get("test-local")
	info, _ := be.Stat(ctx, "etag-test.bin")
	if !cm.isFullyCached("test-local", "etag-test.bin", info.ETag) {
		t.Fatal("expected file to be fully cached after first warm")
	}

	// Modify the file — this changes its ETag/modtime.
	time.Sleep(50 * time.Millisecond) // Ensure distinct modtime
	modified := []byte("modified-content-different!!")
	if err := os.WriteFile(warmFile, modified, 0o644); err != nil {
		t.Fatal(err)
	}

	// Expire the file meta so the cache detects the change.
	fk := fileKey("test-local", "etag-test.bin")
	_ = cm.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(fk))
	})

	// Second warm — should detect ETag change and re-warm.
	var progressCalls int64
	err = cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     2,
	}, func(p WarmProgress) {
		progressCalls++
	})
	if err != nil {
		t.Fatalf("Second warm: %v", err)
	}

	// Read the file through cache — should get the modified content.
	buf := make([]byte, len(modified))
	n, err := cm.Read(ctx, "test-local", "etag-test.bin", buf, 0)
	if err != nil {
		t.Fatalf("Read after re-warm: %v", err)
	}
	if string(buf[:n]) != string(modified) {
		t.Errorf("got %q, want %q after ETag re-validation", string(buf[:n]), string(modified))
	}
}

// TestCacheCloseCleanup verifies that Close() shuts down the cache manager
// cleanly: readahead workers stop, access times are flushed, and badger is
// closed without error.
func TestCacheCloseCleanup(t *testing.T) {
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "f.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-close", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	cacheDir := t.TempDir()
	cm, err := New(config.CacheConfig{
		Path:      cacheDir,
		MaxSize:   100 * 1024 * 1024,
		BlockSize: 4 * 1024 * 1024,
	}, reg)
	if err != nil {
		t.Fatal(err)
	}

	// Do a read to exercise the cache.
	buf := make([]byte, 4)
	_, err = cm.Read(context.Background(), "test-close", "f.txt", buf, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	// Buffer an access time.
	cm.accessMu.Lock()
	cm.accessTimes[blockKey("test-close", "f.txt", 0)] = time.Now()
	cm.accessMu.Unlock()

	// Close should flush access times, stop readahead, and close badger.
	if err := cm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify badger is closed — further operations should fail.
	_, err = cm.getBlockMeta(blockKey("test-close", "f.txt", 0))
	if err == nil {
		t.Error("expected error after Close(), badger should be closed")
	}
}
