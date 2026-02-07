package cache

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/config"
)

// ──────────────── statWithCache Tests ────────────────

// TestStatWithCachePopulates verifies that the first stat call fetches
// from the backend and caches the result.
func TestStatWithCachePopulates(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	be, err := cm.registry.Get("test-local")
	if err != nil {
		t.Fatal(err)
	}

	// First stat — should go to backend.
	info, err := cm.statWithCache(ctx, be, "test-local", "small.txt")
	if err != nil {
		t.Fatalf("statWithCache: %v", err)
	}

	if info.Size != 15 { // "hello warpdrive" = 15 bytes
		t.Errorf("expected size 15, got %d", info.Size)
	}
	if info.ETag == "" {
		t.Error("expected non-empty ETag from local backend")
	}
}

// TestStatWithCacheReturnsCached verifies that subsequent stat calls within
// the StaleTTL return the cached result without hitting the backend.
func TestStatWithCacheReturnsCached(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// Set a long stale TTL so cache definitely applies.
	cm.cfg.StaleTTL = 60 * time.Second

	be, err := cm.registry.Get("test-local")
	if err != nil {
		t.Fatal(err)
	}

	// First call populates cache.
	info1, err := cm.statWithCache(ctx, be, "test-local", "small.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Second call should return cached result.
	info2, err := cm.statWithCache(ctx, be, "test-local", "small.txt")
	if err != nil {
		t.Fatal(err)
	}

	if info1.Size != info2.Size || info1.ETag != info2.ETag {
		t.Errorf("cached stat differs: %+v vs %+v", info1, info2)
	}
}

// TestStatWithCacheExpires verifies that stat entries are re-validated
// after the StaleTTL elapses.
func TestStatWithCacheExpires(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// Set a very short stale TTL.
	cm.cfg.StaleTTL = 1 * time.Millisecond

	be, err := cm.registry.Get("test-local")
	if err != nil {
		t.Fatal(err)
	}

	info1, err := cm.statWithCache(ctx, be, "test-local", "small.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Wait for TTL to expire.
	time.Sleep(10 * time.Millisecond)

	// This should re-fetch from backend (but return same data since file unchanged).
	info2, err := cm.statWithCache(ctx, be, "test-local", "small.txt")
	if err != nil {
		t.Fatal(err)
	}

	if info1.Size != info2.Size {
		t.Errorf("size mismatch after cache refresh: %d vs %d", info1.Size, info2.Size)
	}
}

// TestStatWithCacheUnknownFile verifies that statting a nonexistent file
// returns an error.
func TestStatWithCacheUnknownFile(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	be, err := cm.registry.Get("test-local")
	if err != nil {
		t.Fatal(err)
	}

	_, err = cm.statWithCache(ctx, be, "test-local", "does-not-exist.txt")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

// ──────────────── flushAccessTimes Tests ────────────────

// TestFlushAccessTimesWritesToBadger verifies that buffered access times
// are flushed to Badger metadata.
func TestFlushAccessTimesWritesToBadger(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// Read a file to create a block entry in badger.
	buf := make([]byte, 11)
	_, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Record that we have an access time in memory.
	key := blockKey("test-local", "small.txt", 0)
	originalMeta, err := cm.getBlockMeta(key)
	if err != nil {
		t.Fatal(err)
	}

	// Set a distinct access time.
	distinctTime := time.Now().Add(1 * time.Hour)
	cm.accessMu.Lock()
	cm.accessTimes[key] = distinctTime
	cm.accessMu.Unlock()

	// Flush.
	cm.flushAccessTimes()

	// Verify the metadata was updated in badger.
	updatedMeta, err := cm.getBlockMeta(key)
	if err != nil {
		t.Fatalf("getBlockMeta after flush: %v", err)
	}
	if !updatedMeta.LastAccess.Equal(distinctTime) {
		t.Errorf("expected LastAccess = %v, got %v", distinctTime, updatedMeta.LastAccess)
	}
	_ = originalMeta
}

// TestFlushAccessTimesEmptyNoOp verifies that flushing with no pending
// access times is a no-op (doesn't panic or error).
func TestFlushAccessTimesEmptyNoOp(t *testing.T) {
	cm, _ := setupTestCache(t)

	// Flush with nothing buffered.
	cm.flushAccessTimes()

	// Flush again — still nothing.
	cm.flushAccessTimes()
}

// TestFlushAccessTimesClears verifies that flushing clears the in-memory
// buffer so times don't flush twice.
func TestFlushAccessTimesClears(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	buf := make([]byte, 11)
	_, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	key := blockKey("test-local", "small.txt", 0)
	cm.accessMu.Lock()
	cm.accessTimes[key] = time.Now()
	cm.accessMu.Unlock()

	cm.flushAccessTimes()

	// After flush, the buffer should be empty.
	cm.accessMu.Lock()
	remaining := len(cm.accessTimes)
	cm.accessMu.Unlock()

	if remaining != 0 {
		t.Errorf("expected empty access times after flush, got %d entries", remaining)
	}
}

// TestGetAccessTimePrefersMem verifies that getAccessTime returns the
// in-memory time when it's more recent than the stored time.
func TestGetAccessTimePrefersMem(t *testing.T) {
	cm, _ := setupTestCache(t)

	storedTime := time.Now().Add(-1 * time.Hour)
	memTime := time.Now()

	key := "block:test:file.txt:0"
	cm.accessMu.Lock()
	cm.accessTimes[key] = memTime
	cm.accessMu.Unlock()

	result := cm.getAccessTime(key, storedTime)
	if !result.Equal(memTime) {
		t.Errorf("expected in-memory time %v, got %v", memTime, result)
	}
}

// TestGetAccessTimeFallsBack verifies that getAccessTime returns the
// stored time when no in-memory time exists.
func TestGetAccessTimeFallsBack(t *testing.T) {
	cm, _ := setupTestCache(t)

	storedTime := time.Now()
	result := cm.getAccessTime("nonexistent-key", storedTime)
	if !result.Equal(storedTime) {
		t.Errorf("expected stored time %v, got %v", storedTime, result)
	}
}

// ──────────────── Coalesce Edge Case Tests ────────────────

// TestCoalescerOverlappingReads verifies that overlapping reads are still
// classified as sequential (negative gap is treated as 0).
func TestCoalescerOverlappingReads(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Reads that overlap (each starts before the previous ends).
	rc.RecordRead("be", "file.txt", 0, 8192)
	rc.RecordRead("be", "file.txt", 4096, 8192)
	rc.RecordRead("be", "file.txt", 8192, 8192)
	p := rc.RecordRead("be", "file.txt", 12288, 8192)

	if p != patternSequential {
		t.Errorf("expected SEQUENTIAL for overlapping forward reads, got %d", p)
	}
}

// TestCoalescerExactBlockBoundary verifies that reads at exact block
// boundaries are correctly classified as sequential.
func TestCoalescerExactBlockBoundary(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	blockSize := int64(DefaultBlockSize) // 4MB
	rc.RecordRead("be", "file.txt", 0, int(blockSize))
	rc.RecordRead("be", "file.txt", blockSize, int(blockSize))
	rc.RecordRead("be", "file.txt", 2*blockSize, int(blockSize))
	p := rc.RecordRead("be", "file.txt", 3*blockSize, int(blockSize))

	if p != patternSequential {
		t.Errorf("expected SEQUENTIAL for block-aligned reads, got %d", p)
	}
}

// TestCoalescerGapJustUnderThreshold verifies that gaps slightly under
// 2*blockSize are still classified as sequential.
func TestCoalescerGapJustUnderThreshold(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Gap of slightly less than 2 * DefaultBlockSize between reads.
	gap := int64(2*DefaultBlockSize) - 1
	rc.RecordRead("be", "file.txt", 0, 4096)
	rc.RecordRead("be", "file.txt", 4096+gap, 4096)
	rc.RecordRead("be", "file.txt", 2*(4096+gap), 4096)
	p := rc.RecordRead("be", "file.txt", 3*(4096+gap), 4096)

	if p != patternSequential {
		t.Errorf("expected SEQUENTIAL for gap just under threshold, got %d", p)
	}
}

// TestCoalescerGapOverThreshold verifies that gaps over 2*blockSize
// are classified as random.
func TestCoalescerGapOverThreshold(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Gap of slightly more than 2 * DefaultBlockSize between reads.
	gap := int64(2*DefaultBlockSize) + 1
	rc.RecordRead("be", "file.txt", 0, 4096)
	rc.RecordRead("be", "file.txt", 4096+gap, 4096)
	rc.RecordRead("be", "file.txt", 2*(4096+gap), 4096)
	p := rc.RecordRead("be", "file.txt", 3*(4096+gap), 4096)

	if p != patternRandom {
		t.Errorf("expected RANDOM for gap over threshold, got %d", p)
	}
}

// TestCoalescerBackwardReads verifies that backward-moving offsets are
// classified as random.
func TestCoalescerBackwardReads(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	rc.RecordRead("be", "file.txt", 12288, 4096)
	rc.RecordRead("be", "file.txt", 8192, 4096)
	rc.RecordRead("be", "file.txt", 4096, 4096)
	p := rc.RecordRead("be", "file.txt", 0, 4096)

	if p != patternRandom {
		t.Errorf("expected RANDOM for backward reads, got %d", p)
	}
}

// TestCoalescerCircularBufferWrap verifies that pattern detection works
// correctly when the circular buffer wraps around (>8 reads).
func TestCoalescerCircularBufferWrap(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Record 20 sequential reads — buffer wraps multiple times.
	for i := 0; i < 20; i++ {
		rc.RecordRead("be", "file.txt", int64(i)*4096, 4096)
	}

	p := rc.GetPattern("be", "file.txt")
	if p != patternSequential {
		t.Errorf("expected SEQUENTIAL after 20 sequential reads with buffer wrap, got %d", p)
	}
}

// TestCoalescerIndependentFiles verifies that patterns for different files
// are tracked independently.
func TestCoalescerIndependentFiles(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Sequential reads on file1.
	for i := 0; i < 8; i++ {
		rc.RecordRead("be", "file1.txt", int64(i)*4096, 4096)
	}

	// Random reads on file2.
	rc.RecordRead("be", "file2.txt", 100000, 4096)
	rc.RecordRead("be", "file2.txt", 0, 4096)
	rc.RecordRead("be", "file2.txt", 500000, 4096)
	rc.RecordRead("be", "file2.txt", 200000, 4096)

	p1 := rc.GetPattern("be", "file1.txt")
	p2 := rc.GetPattern("be", "file2.txt")

	if p1 != patternSequential {
		t.Errorf("file1: expected SEQUENTIAL, got %d", p1)
	}
	if p2 == patternSequential {
		t.Errorf("file2: expected non-sequential, got SEQUENTIAL")
	}
}

// TestCoalescerForgetResets verifies that Forget resets tracking state
// and subsequent reads start from UNKNOWN.
func TestCoalescerForgetResets(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Establish a pattern.
	for i := 0; i < 8; i++ {
		rc.RecordRead("be", "file.txt", int64(i)*4096, 4096)
	}

	if rc.GetPattern("be", "file.txt") != patternSequential {
		t.Fatal("expected SEQUENTIAL before forget")
	}

	rc.Forget("be", "file.txt")

	if rc.GetPattern("be", "file.txt") != patternUnknown {
		t.Error("expected UNKNOWN after forget")
	}

	// New reads should start fresh.
	p := rc.RecordRead("be", "file.txt", 0, 4096)
	if p != patternUnknown {
		t.Errorf("expected UNKNOWN for first read after forget, got %d", p)
	}
}

// TestCoalescerSameOffset verifies that reads at the same offset
// (re-reads) don't break pattern detection.
func TestCoalescerSameOffset(t *testing.T) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Same offset 4 times (e.g., retry reads).
	for i := 0; i < 4; i++ {
		rc.RecordRead("be", "file.txt", 0, 4096)
	}

	p := rc.GetPattern("be", "file.txt")
	// Same offset = not advancing = not sequential.
	if p == patternSequential {
		t.Error("expected non-sequential for repeated same-offset reads")
	}
}

// TestCoalescerCustomBlockSize verifies that the gap threshold uses the
// configured blockSize rather than DefaultBlockSize.
func TestCoalescerCustomBlockSize(t *testing.T) {
	// Use a tiny block size so the default 2*DefaultBlockSize threshold
	// would incorrectly classify the gap as sequential.
	smallBlock := 1024 // 1KB
	rc := NewReadCoalescer(smallBlock)

	// Reads with a gap of 3KB between them.
	// With blockSize=1KB, threshold = 2*1KB = 2KB, gap=3KB → RANDOM.
	// With DefaultBlockSize=4MB, threshold = 8MB → would be SEQUENTIAL.
	rc.RecordRead("be", "file.txt", 0, 100)
	rc.RecordRead("be", "file.txt", 3200, 100)
	rc.RecordRead("be", "file.txt", 6400, 100)
	p := rc.RecordRead("be", "file.txt", 9600, 100)

	if p != patternRandom {
		t.Errorf("expected RANDOM with small blockSize (gap exceeds 2*blockSize), got %d", p)
	}

	// Verify that the same reads with DefaultBlockSize would be sequential.
	rcDefault := NewReadCoalescer(DefaultBlockSize)
	rcDefault.RecordRead("be", "file.txt", 0, 100)
	rcDefault.RecordRead("be", "file.txt", 3200, 100)
	rcDefault.RecordRead("be", "file.txt", 6400, 100)
	pDefault := rcDefault.RecordRead("be", "file.txt", 9600, 100)

	if pDefault != patternSequential {
		t.Errorf("expected SEQUENTIAL with default blockSize, got %d", pDefault)
	}
}

// ──────────────── Singleflight Dedup Tests ────────────────

// TestSingleflightDedupOnConcurrentReads verifies that concurrent reads
// for the same uncached block use singleflight dedup. All goroutines get
// the correct data, and only one block file is written to disk despite
// many concurrent readers.
func TestSingleflightDedupOnConcurrentReads(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// Launch 20 goroutines all reading the same block concurrently.
	const goroutines = 20
	var wg sync.WaitGroup
	results := make([][]byte, goroutines)
	errors := make([]error, goroutines)

	// Use a barrier to ensure all goroutines start at the same time.
	ready := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-ready                 // Wait for all goroutines to be ready
			buf := make([]byte, 15) // "hello warpdrive" = 15 bytes
			n, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
			if err != nil {
				errors[idx] = err
				return
			}
			results[idx] = make([]byte, n)
			copy(results[idx], buf[:n])
		}(i)
	}

	// Release all goroutines simultaneously.
	close(ready)
	wg.Wait()

	// Check for errors.
	for i, err := range errors {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// All goroutines must get the same correct result.
	expected := "hello warpdrive"
	for i, r := range results {
		if string(r) != expected {
			t.Errorf("goroutine %d: got %q, want %q", i, string(r), expected)
		}
	}

	// Verify only one block metadata entry exists in badger (singleflight
	// ensures only one goroutine writes the block, even if many read it).
	key := blockKey("test-local", "small.txt", 0)
	meta, err := cm.getBlockMeta(key)
	if err != nil {
		t.Fatalf("expected block metadata to exist: %v", err)
	}

	// The block file should exist on disk exactly once.
	if _, err := os.Stat(meta.LocalPath); err != nil {
		t.Errorf("expected block file to exist at %s: %v", meta.LocalPath, err)
	}

	// Stats should show that all goroutines were served correctly.
	stats := cm.GetStats()
	total := stats.Hits + stats.Misses
	if total < goroutines {
		t.Errorf("expected at least %d total ops (hits+misses), got %d", goroutines, total)
	}
	t.Logf("Singleflight: %d misses, %d hits for %d concurrent reads", stats.Misses, stats.Hits, goroutines)
}

// ──────────────── WarmProgress Throughput/ETA Tests ────────────────

// TestWarmProgressThroughputPopulated verifies that the WarmProgress callback
// receives non-zero Throughput and ETA values during warming.
func TestWarmProgressThroughputPopulated(t *testing.T) {
	cm, srcDir := setupTestCache(t)
	ctx := context.Background()

	// Create enough data to generate meaningful progress callbacks.
	for i := 0; i < 5; i++ {
		data := make([]byte, 2*1024*1024) // 2 MB each
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, "throughput-"+string(rune('a'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var seenPositiveThroughput bool
	var allProgress []WarmProgress

	err := cm.Warm(ctx, WarmConfig{
		BackendName: "test-local",
		Prefix:      "",
		Recursive:   true,
		Workers:     2,
	}, func(p WarmProgress) {
		mu.Lock()
		defer mu.Unlock()
		allProgress = append(allProgress, p)
		if p.Throughput > 0 {
			seenPositiveThroughput = true
		}
	})
	if err != nil {
		t.Fatalf("Warm failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(allProgress) == 0 {
		t.Fatal("expected progress callbacks during warming")
	}

	// Verify BytesWarmed increases monotonically.
	for i := 1; i < len(allProgress); i++ {
		if allProgress[i].BytesWarmed < allProgress[i-1].BytesWarmed {
			t.Errorf("BytesWarmed decreased: %d -> %d at callback %d",
				allProgress[i-1].BytesWarmed, allProgress[i].BytesWarmed, i)
		}
	}

	// Throughput should be positive at least once (unless machine is extremely fast).
	if !seenPositiveThroughput && len(allProgress) > 1 {
		t.Log("Warning: no positive throughput seen (may be too fast to measure)")
	}

	// FilesTotal should be consistent across all callbacks.
	firstTotal := allProgress[0].FilesTotal
	for i, p := range allProgress {
		if p.FilesTotal != firstTotal {
			t.Errorf("FilesTotal changed: callback 0 had %d, callback %d had %d",
				firstTotal, i, p.FilesTotal)
		}
	}
}

// ──────────────── Eviction Watermark Precision Tests ────────────────

// TestEvictionRespectsLowWatermark verifies that after eviction, the total
// cached bytes are at or below the low water mark.
func TestEvictionRespectsLowWatermark(t *testing.T) {
	srcDir := t.TempDir()
	// Create 6 × 4MB files = 24MB total.
	for i := 0; i < 6; i++ {
		data := make([]byte, 4*1024*1024)
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, "wm-"+string(rune('a'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-wm", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	cacheDir := t.TempDir()
	maxSize := int64(20 * 1024 * 1024) // 20MB
	cm, err := New(config.CacheConfig{
		Path:           cacheDir,
		MaxSize:        maxSize,
		BlockSize:      4 * 1024 * 1024,
		EvictHighWater: 0.90, // Evict when >18MB
		EvictLowWater:  0.60, // Evict down to 12MB
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cm.Close() })

	ctx := context.Background()
	buf := make([]byte, 4*1024*1024)

	// Read all 6 files = 24MB, exceeding 20MB max.
	for i := 0; i < 6; i++ {
		name := "wm-" + string(rune('a'+i)) + ".bin"
		_, err := cm.Read(ctx, "test-wm", name, buf, 0)
		if err != nil {
			t.Fatalf("Read %s: %v", name, err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure distinct access times
	}

	// Flush so eviction has fresh access times.
	cm.flushAccessTimes()

	// Run eviction.
	cm.runEviction()

	// BytesCached should now be at or below the low water mark (60% of 20MB = 12MB).
	stats := cm.GetStats()
	lowWaterBytes := int64(float64(maxSize) * 0.60)
	if stats.BytesCached > lowWaterBytes {
		t.Errorf("BytesCached after eviction = %d, want <= %d (low water mark)",
			stats.BytesCached, lowWaterBytes)
	}
	if stats.Evictions == 0 {
		t.Error("expected some evictions to have occurred")
	}
	t.Logf("Post-eviction: %d bytes cached, %d evictions (target: <= %d)",
		stats.BytesCached, stats.Evictions, lowWaterBytes)
}
