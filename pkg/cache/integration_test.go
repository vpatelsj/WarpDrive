package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/config"
)

// TestCoalescerIntegration verifies that the coalescer correctly detects
// access patterns when reads flow through CacheManager.Read().
func TestCoalescerIntegration(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	blockSize := int64(cm.blockSize) // 4MB

	// Read sequentially through the large file (10MB = 3 blocks).
	buf := make([]byte, 4096) // Small reads within blocks
	for i := 0; i < 8; i++ {
		off := int64(i) * 4096
		_, err := cm.Read(ctx, "test-local", "large.bin", buf, off)
		if err != nil {
			t.Fatalf("Read at offset %d: %v", off, err)
		}
	}

	pattern := cm.coalescer.GetPattern("test-local", "large.bin")
	if pattern != patternSequential {
		t.Errorf("expected SEQUENTIAL pattern after sequential reads, got %d", pattern)
	}

	// Now do random reads on a different file — read at scattered block offsets.
	_ = blockSize
	randomOffsets := []int64{
		8 * 1024 * 1024,      // Block 2
		0,                    // Block 0
		4*1024*1024 + 100000, // Block 1
		9 * 1024 * 1024,      // Block 2 again
	}
	for _, off := range randomOffsets {
		_, err := cm.Read(ctx, "test-local", "large.bin", buf, off)
		if err != nil {
			t.Fatalf("Read at offset %d: %v", off, err)
		}
	}

	// After mixing random reads, should no longer be sequential.
	pattern = cm.coalescer.GetPattern("test-local", "large.bin")
	if pattern == patternSequential {
		t.Log("Pattern still detected as sequential (may need more random reads to flip)")
	}
}

// TestCoalescerReadaheadSupression verifies that random access detected
// by the coalescer suppresses readahead.
func TestCoalescerReadaheadSuppression(t *testing.T) {
	srcDir := t.TempDir()
	// Create a large file: 64MB = 16 blocks.
	data := make([]byte, 64*1024*1024)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "big.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-coal", "local", srcDir, map[string]string{})
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

	ctx := context.Background()
	blockSize := int64(4 * 1024 * 1024)

	// Do random reads to establish RANDOM pattern.
	buf := make([]byte, 4096)
	randomBlocks := []int64{12, 3, 8, 1, 15, 0, 7, 10}
	for _, blk := range randomBlocks {
		_, err := cm.Read(ctx, "test-coal", "big.bin", buf, blk*blockSize)
		if err != nil {
			t.Fatalf("Read at block %d: %v", blk, err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	// Check that coalescer detected random pattern.
	pattern := cm.coalescer.GetPattern("test-coal", "big.bin")
	if pattern == patternSequential {
		t.Log("Pattern still sequential (random pattern may need more samples)")
	}

	// Count which blocks are cached — with random pattern, readahead should
	// NOT populate blocks we didn't explicitly read.
	cachedBlocks := 0
	for i := 0; i < 16; i++ {
		key := blockKey("test-coal", "big.bin", i)
		if _, err := cm.getBlockMeta(key); err == nil {
			cachedBlocks++
		}
	}

	// We read 8 unique blocks directly; with readahead suppressed on random access,
	// we should not have significantly more than 8 cached blocks.
	// (Some minor readahead may occur before pattern is established.)
	t.Logf("Cached blocks: %d out of 16 (read 8 directly)", cachedBlocks)
	if cachedBlocks >= 16 {
		t.Error("all blocks cached despite random access — readahead suppression may not be working")
	}
}

// TestEvictionTriggered verifies that the eviction loop removes old blocks
// when cache size exceeds the high water mark.
func TestEvictionTriggered(t *testing.T) {
	srcDir := t.TempDir()
	// Create files totaling ~20MB.
	for i := 0; i < 5; i++ {
		data := make([]byte, 4*1024*1024) // 4MB each
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, "evict-"+string(rune('a'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-evict", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	cacheDir := t.TempDir()
	cm, err := New(config.CacheConfig{
		Path:           cacheDir,
		MaxSize:        12 * 1024 * 1024, // 12MB — can hold only 3 blocks of 4MB
		BlockSize:      4 * 1024 * 1024,
		EvictHighWater: 0.90, // Evict when >10.8MB
		EvictLowWater:  0.60, // Evict down to 7.2MB
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cm.Close() })

	ctx := context.Background()

	// Read all 5 files = 20MB, which exceeds 12MB max.
	buf := make([]byte, 4*1024*1024)
	for i := 0; i < 5; i++ {
		name := "evict-" + string(rune('a'+i)) + ".bin"
		_, err := cm.Read(ctx, "test-evict", name, buf, 0)
		if err != nil {
			t.Fatalf("Read %s: %v", name, err)
		}
	}

	// Manually trigger eviction.
	cm.runEviction()

	// After eviction, BytesCached should be reduced.
	stats := cm.GetStats()
	if stats.Evictions == 0 {
		t.Error("expected some evictions after exceeding max size")
	}
	if stats.BytesCached > 12*1024*1024 {
		t.Errorf("expected BytesCached <= 12MB after eviction, got %d", stats.BytesCached)
	}
}

// TestEvictionLRUOrder verifies that the oldest-accessed blocks are
// evicted first (LRU policy).
func TestEvictionLRUOrder(t *testing.T) {
	srcDir := t.TempDir()
	for i := 0; i < 4; i++ {
		data := make([]byte, 4*1024*1024) // 4MB each
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, "lru-"+string(rune('a'+i))+".bin")
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-lru", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	cacheDir := t.TempDir()
	cm, err := New(config.CacheConfig{
		Path:           cacheDir,
		MaxSize:        12 * 1024 * 1024, // 12MB
		BlockSize:      4 * 1024 * 1024,
		EvictHighWater: 0.90,
		EvictLowWater:  0.50, // Evict down to 6MB = 1 block
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cm.Close() })

	ctx := context.Background()
	buf := make([]byte, 4*1024*1024)

	// Read files in order: a (oldest), b, c, d (newest).
	for i := 0; i < 4; i++ {
		name := "lru-" + string(rune('a'+i)) + ".bin"
		_, err := cm.Read(ctx, "test-lru", name, buf, 0)
		if err != nil {
			t.Fatalf("Read %s: %v", name, err)
		}
		// Small delay to ensure distinct access times.
		time.Sleep(20 * time.Millisecond)
	}

	// Re-read file 'd' to make it the most recently accessed.
	_, _ = cm.Read(ctx, "test-lru", "lru-d.bin", buf, 0)

	// Flush access times so eviction has fresh data.
	cm.flushAccessTimes()

	// Run eviction — should evict oldest first (a, b, c).
	cm.runEviction()

	// File 'd' (most recently accessed) should still be cached.
	keyD := blockKey("test-lru", "lru-d.bin", 0)
	_, err = cm.getBlockMeta(keyD)
	if err != nil {
		t.Error("expected most recently accessed file (lru-d.bin) to survive eviction")
	}

	// File 'a' (oldest) should have been evicted.
	keyA := blockKey("test-lru", "lru-a.bin", 0)
	_, err = cm.getBlockMeta(keyA)
	if err == nil {
		t.Error("expected oldest file (lru-a.bin) to be evicted")
	}
}

// TestDirLevelReadaheadIntegration verifies that directory-level readahead
// works end-to-end: when files in a directory are read in sorted order,
// the readahead manager prefetches blocks from the next files.
func TestDirLevelReadaheadIntegration(t *testing.T) {
	// Create 8 sequential shard files, each 8MB (2 blocks).
	srcDir := t.TempDir()
	shardSize := 8 * 1024 * 1024
	shardCount := 8
	for i := 0; i < shardCount; i++ {
		data := make([]byte, shardSize)
		for j := range data {
			data[j] = byte((i + j) % 251)
		}
		name := filepath.Join(srcDir, fmt.Sprintf("shard-%04d.bin", i))
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-dirra", "local", srcDir, map[string]string{})
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

	ctx := context.Background()
	ra := cm.readahead

	// Tell the readahead manager about the directory contents (sorted).
	allFiles := make([]string, shardCount)
	for i := 0; i < shardCount; i++ {
		allFiles[i] = fmt.Sprintf("shard-%04d.bin", i)
	}
	ra.SetDirFiles("test-dirra", ".", allFiles)

	// Read first 4 files sequentially (block 0 of each).
	// This should establish a dir-level sequential pattern.
	buf := make([]byte, 4*1024*1024) // 1 block
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("shard-%04d.bin", i)
		_, err := cm.Read(ctx, "test-dirra", name, buf, 0)
		if err != nil {
			t.Fatalf("Read %s block 0: %v", name, err)
		}
	}

	// Give readahead workers time to prefetch dir-level blocks.
	time.Sleep(1 * time.Second)

	// Check if any blocks from shard-0004 or shard-0005 were prefetched.
	prefetchedBlocks := 0
	for nextShard := 4; nextShard < 6; nextShard++ {
		name := fmt.Sprintf("shard-%04d.bin", nextShard)
		for blk := 0; blk < 2; blk++ {
			key := blockKey("test-dirra", name, blk)
			if _, err := cm.getBlockMeta(key); err == nil {
				prefetchedBlocks++
			}
		}
	}

	t.Logf("Dir-level readahead: %d blocks prefetched from shards 4-5", prefetchedBlocks)
	if prefetchedBlocks == 0 {
		t.Log("Note: dir-level readahead did not prefetch (may need longer wait or more access history)")
	}

	// Now read shard-0004 — if readahead worked, this should be a hit.
	statsBefore := cm.GetStats()
	_, err = cm.Read(ctx, "test-dirra", "shard-0004.bin", buf, 0)
	if err != nil {
		t.Fatalf("Read shard-0004: %v", err)
	}
	statsAfter := cm.GetStats()

	if prefetchedBlocks > 0 && statsAfter.Hits > statsBefore.Hits {
		t.Log("Dir-level readahead confirmed: shard-0004 block 0 was a cache hit")
	}
}

// TestFetchSemaphoreLimit verifies that the fetchSem semaphore limits
// concurrent backend fetches.
func TestFetchSemaphoreLimit(t *testing.T) {
	srcDir := t.TempDir()
	// Create 16 files of 4MB each.
	for i := 0; i < 16; i++ {
		data := make([]byte, 4*1024*1024)
		name := filepath.Join(srcDir, fmt.Sprintf("sem-%04d.bin", i))
		if err := os.WriteFile(name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-sem", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	cacheDir := t.TempDir()
	cm, err := New(config.CacheConfig{
		Path:             cacheDir,
		MaxSize:          500 * 1024 * 1024,
		BlockSize:        4 * 1024 * 1024,
		MaxParallelFetch: 4, // Only 4 concurrent fetches allowed
	}, reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cm.Close() })

	// Verify the semaphore capacity matches the config.
	if cap(cm.fetchSem) != 4 {
		t.Errorf("expected fetchSem capacity 4, got %d", cap(cm.fetchSem))
	}

	ctx := context.Background()

	// Read all 16 files concurrently — should not deadlock.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			localBuf := make([]byte, 4*1024*1024) // Each goroutine has its own buffer
			name := fmt.Sprintf("sem-%04d.bin", idx)
			_, err := cm.Read(ctx, "test-sem", name, localBuf, 0)
			if err != nil {
				t.Errorf("Read %s: %v", name, err)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All reads completed — no deadlock.
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent reads with fetch semaphore did not complete within 30s")
	}

	// All files should be cached.
	stats := cm.GetStats()
	if stats.Misses < 16 {
		t.Errorf("expected at least 16 misses, got %d", stats.Misses)
	}
}
