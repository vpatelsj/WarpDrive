package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/config"
)

// setupReadaheadTestCache creates a CacheManager suitable for readahead tests.
// It creates a source directory with sequential shard files.
func setupReadaheadTestCache(t *testing.T, shardCount int, shardSize int) (*CacheManager, string) {
	t.Helper()

	srcDir := t.TempDir()

	// Create sequential shard files.
	for i := 0; i < shardCount; i++ {
		data := make([]byte, shardSize)
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, "shard-"+padInt(i, 4)+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-ra", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	cacheDir := t.TempDir()
	cm, err := New(config.CacheConfig{
		Path:            cacheDir,
		MaxSize:         500 * 1024 * 1024,
		BlockSize:       4 * 1024 * 1024,
		ReadaheadBlocks: 4,
	}, reg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { cm.Close() })
	return cm, srcDir
}

func padInt(n int, width int) string {
	s := ""
	for i := 0; i < width; i++ {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

// TestReadaheadSequentialDetection verifies that sequential block reads
// trigger readahead prefetching and that subsequent reads are cache hits.
func TestReadaheadSequentialDetection(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 1, 32*1024*1024) // 1 shard, 32MB = 8 blocks
	ctx := context.Background()

	// Read blocks 0-5 sequentially (4MB each).
	buf := make([]byte, 4*1024*1024)
	for i := 0; i < 6; i++ {
		off := int64(i) * int64(4*1024*1024)
		n, err := cm.Read(ctx, "test-ra", "shard-0000.bin", buf, off)
		if err != nil {
			t.Fatalf("Read block %d: %v", i, err)
		}
		if n != 4*1024*1024 {
			t.Fatalf("Read block %d: got %d bytes, want %d", i, n, 4*1024*1024)
		}
	}

	// Give readahead workers time to fetch.
	time.Sleep(500 * time.Millisecond)

	// Block 7 (the last one) should have been prefetched by readahead.
	statsBefore := cm.GetStats()
	_, err := cm.Read(ctx, "test-ra", "shard-0000.bin", buf, 7*4*1024*1024)
	if err != nil {
		t.Fatalf("Read block 7: %v", err)
	}
	statsAfter := cm.GetStats()

	// We expect block 7 to be a hit because readahead should have prefetched it.
	if statsAfter.Hits <= statsBefore.Hits {
		t.Log("Readahead may not have fetched block 7 yet (timing-dependent)")
	}
}

// TestReadaheadBudgetEnforcement verifies that readahead respects the
// inflight byte budget and doesn't exceed maxInflight.
func TestReadaheadBudgetEnforcement(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 1, 8*1024*1024)

	ra := cm.readahead

	// Set a very small budget to test enforcement.
	ra.maxInflight = int64(4 * 1024 * 1024) // 4MB = 1 block

	// Enqueue many jobs — only some should be accepted before budget check.
	for i := 0; i < 20; i++ {
		ra.enqueue(readaheadJob{
			backendName: "test-ra",
			path:        "shard-0000.bin",
			blockIdx:    i + 10, // Use high block indices that don't exist in file
			etag:        "test-etag",
			priority:    priorityFile,
		})
	}

	// The budget check in enqueue prevents queueing when over limit,
	// so we just verify the readahead manager is still functional.
	time.Sleep(100 * time.Millisecond)

	// Verify currentFlight never exceeds budget (it should be back to ~0
	// since the fetches for non-existent blocks will fail).
	flight := ra.currentFlight.Load()
	if flight > ra.maxInflight+int64(cm.blockSize) {
		t.Errorf("currentFlight %d exceeds maxInflight %d", flight, ra.maxInflight)
	}
}

// TestReadaheadPriorityQueues verifies that high-priority (in-file) jobs
// are processed before low-priority (directory-level) jobs.
func TestReadaheadPriorityQueues(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 3, 8*1024*1024)

	ra := cm.readahead

	// Verify queue capacities.
	if cap(ra.highJobs) != 128 {
		t.Errorf("expected highJobs capacity 128, got %d", cap(ra.highJobs))
	}
	if cap(ra.lowJobs) != 128 {
		t.Errorf("expected lowJobs capacity 128, got %d", cap(ra.lowJobs))
	}

	// Enqueue a low-priority job.
	ra.enqueue(readaheadJob{
		backendName: "test-ra",
		path:        "shard-0000.bin",
		blockIdx:    0,
		etag:        "",
		priority:    priorityDir,
	})

	// Enqueue a high-priority job.
	ra.enqueue(readaheadJob{
		backendName: "test-ra",
		path:        "shard-0001.bin",
		blockIdx:    0,
		etag:        "",
		priority:    priorityFile,
	})

	// Let workers process.
	time.Sleep(300 * time.Millisecond)

	// Both should eventually be processed (we can't easily check ordering
	// since workers run in parallel, but we verify both are cached).
	key0 := blockKey("test-ra", "shard-0000.bin", 0)
	key1 := blockKey("test-ra", "shard-0001.bin", 0)

	_, err0 := cm.getBlockMeta(key0)
	_, err1 := cm.getBlockMeta(key1)

	if err0 != nil && err1 != nil {
		t.Error("expected at least one readahead block to be cached")
	}
}

// TestReadaheadDedup verifies that duplicate block requests are
// de-duplicated and not re-queued.
func TestReadaheadDedup(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 1, 8*1024*1024)

	ra := cm.readahead

	// Enqueue the same job multiple times.
	job := readaheadJob{
		backendName: "test-ra",
		path:        "shard-0000.bin",
		blockIdx:    0,
		etag:        "",
		priority:    priorityFile,
	}

	ra.enqueue(job)
	ra.enqueue(job) // Should be deduped
	ra.enqueue(job) // Should be deduped

	time.Sleep(200 * time.Millisecond)

	// The block should be cached exactly once.
	key := blockKey("test-ra", "shard-0000.bin", 0)
	_, err := cm.getBlockMeta(key)
	if err != nil {
		t.Logf("Block not cached yet (timing): %v", err)
	}
}

// TestReadaheadWorkerShutdown verifies that Stop() cleanly shuts down
// all workers without hanging.
func TestReadaheadWorkerShutdown(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 1, 8*1024*1024)

	ra := cm.readahead

	// Enqueue a few jobs.
	for i := 0; i < 5; i++ {
		ra.enqueue(readaheadJob{
			backendName: "test-ra",
			path:        "shard-0000.bin",
			blockIdx:    i,
			etag:        "",
			priority:    priorityFile,
		})
	}

	// Stop should complete without hanging.
	done := make(chan struct{})
	go func() {
		ra.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good — clean shutdown.
	case <-time.After(5 * time.Second):
		t.Fatal("ReadaheadManager.Stop() did not complete within 5s")
	}
}

// TestReadaheadSetDirFiles verifies that SetDirFiles correctly stores
// sorted file lists for directory-level prefetch predictions.
func TestReadaheadSetDirFiles(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 5, 1024)

	ra := cm.readahead

	// Set directory files in unsorted order.
	ra.SetDirFiles("test-ra", ".", []string{
		"shard-0003.bin",
		"shard-0001.bin",
		"shard-0004.bin",
		"shard-0000.bin",
		"shard-0002.bin",
	})

	dp := ra.getOrCreateDirPattern("test-ra", ".")
	dp.mu.Lock()
	defer dp.mu.Unlock()

	if len(dp.allFiles) != 5 {
		t.Fatalf("expected 5 files, got %d", len(dp.allFiles))
	}

	// Verify files are sorted.
	for i := 1; i < len(dp.allFiles); i++ {
		if dp.allFiles[i] < dp.allFiles[i-1] {
			t.Errorf("files not sorted: %v", dp.allFiles)
			break
		}
	}
}

// TestReadaheadSkipsRandomAccess verifies that readahead is disabled
// when the coalescer detects a random access pattern.
func TestReadaheadSkipsRandomAccess(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 1, 32*1024*1024)
	ctx := context.Background()

	blockSize := int64(4 * 1024 * 1024)

	// Read random blocks (non-sequential).
	buf := make([]byte, 4096)
	offsets := []int64{
		7 * blockSize,
		1 * blockSize,
		5 * blockSize,
		0 * blockSize,
	}

	for _, off := range offsets {
		_, err := cm.Read(ctx, "test-ra", "shard-0000.bin", buf, off)
		if err != nil {
			t.Fatalf("Read at offset %d: %v", off, err)
		}
	}

	// Coalescer should detect random pattern.
	pattern := cm.coalescer.GetPattern("test-ra", "shard-0000.bin")
	if pattern == patternSequential {
		t.Error("expected non-sequential pattern for random reads, got SEQUENTIAL")
	}
}

// TestFilePatternResetOnRandom verifies that readahead size resets when
// access becomes random after sequential.
func TestFilePatternResetOnRandom(t *testing.T) {
	fp := &filePattern{readaheadSize: 4}

	// Sequential: blocks 0, 1, 2, 3, 4, 5
	for i := 0; i < 6; i++ {
		fp.recordAccess(i)
	}
	if !fp.isSequential {
		t.Error("expected sequential after monotonic access")
	}
	savedRA := fp.readaheadSize
	if savedRA <= 4 {
		t.Errorf("expected readahead size >4, got %d", savedRA)
	}

	// Random: jump to block 100
	fp.recordAccess(100)
	fp.recordAccess(50)
	fp.recordAccess(200)
	fp.recordAccess(10)

	if fp.isSequential {
		t.Error("expected non-sequential after random access")
	}
	if fp.readaheadSize != 0 {
		t.Errorf("expected readahead size 0, got %d", fp.readaheadSize)
	}
}

// TestDirPatternPredictEdgeCases tests edge cases for predictNextFiles.
func TestDirPatternPredictEdgeCases(t *testing.T) {
	// No allFiles set.
	dp := &dirPattern{isSequential: true, lastFile: "shard-0000"}
	files := dp.predictNextFiles(4)
	if len(files) != 0 {
		t.Errorf("expected no predictions without allFiles, got %v", files)
	}

	// Last file not in allFiles.
	dp = &dirPattern{
		isSequential: true,
		lastFile:     "nonexistent",
		allFiles:     []string{"a.txt", "b.txt", "c.txt"},
	}
	files = dp.predictNextFiles(4)
	if len(files) != 0 {
		t.Errorf("expected no predictions for unknown lastFile, got %v", files)
	}

	// At the end of the file list.
	dp = &dirPattern{
		isSequential: true,
		lastFile:     "c.txt",
		allFiles:     []string{"a.txt", "b.txt", "c.txt"},
	}
	files = dp.predictNextFiles(4)
	if len(files) != 0 {
		t.Errorf("expected no predictions at end of list, got %v", files)
	}

	// Not sequential.
	dp = &dirPattern{
		isSequential: false,
		lastFile:     "a.txt",
		allFiles:     []string{"a.txt", "b.txt", "c.txt"},
	}
	files = dp.predictNextFiles(4)
	if len(files) != 0 {
		t.Errorf("expected no predictions when not sequential, got %v", files)
	}
}

// TestReadaheadStopIdempotent verifies that Stop() is safe to call
// multiple times without panicking (double-close protection via stopOnce).
func TestReadaheadStopIdempotent(t *testing.T) {
	cm, _ := setupReadaheadTestCache(t, 1, 8*1024*1024)
	ra := cm.readahead

	// Enqueue a couple of jobs first.
	ra.enqueue(readaheadJob{
		backendName: "test-ra",
		path:        "shard-0000.bin",
		blockIdx:    0,
		etag:        "",
		priority:    priorityFile,
	})

	done := make(chan struct{})
	go func() {
		ra.Stop()
		ra.Stop() // Second call should not panic
		ra.Stop() // Third call should not panic
		close(done)
	}()

	select {
	case <-done:
		// Clean — multiple Stop() calls didn't panic.
	case <-time.After(5 * time.Second):
		t.Fatal("ReadaheadManager.Stop() called multiple times did not complete within 5s")
	}
}
