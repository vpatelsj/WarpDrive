package cache

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/config"
)

// setupBenchCache creates a CacheManager for benchmarks. The returned
// source directory contains a file of the given size filled with
// deterministic data.
func setupBenchCache(b *testing.B, fileSize int) (*CacheManager, string) {
	b.Helper()

	srcDir := b.TempDir()
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "bench.bin"), data, 0o644); err != nil {
		b.Fatal(err)
	}

	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("bench", "local", srcDir, map[string]string{})
	if err != nil {
		b.Fatal(err)
	}
	reg.Register(be)

	cacheDir := b.TempDir()
	cm, err := New(config.CacheConfig{
		Path:            cacheDir,
		MaxSize:         500 * 1024 * 1024,
		BlockSize:       4 * 1024 * 1024,
		ReadaheadBlocks: 4,
	}, reg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { cm.Close() })
	return cm, srcDir
}

// BenchmarkCacheHit measures throughput for reads served entirely from cache.
func BenchmarkCacheHit(b *testing.B) {
	fileSize := 8 * 1024 * 1024 // 8 MB = 2 blocks
	cm, _ := setupBenchCache(b, fileSize)
	ctx := context.Background()

	// Pre-populate cache.
	buf := make([]byte, fileSize)
	if _, err := cm.Read(ctx, "bench", "bench.bin", buf, 0); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(fileSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := cm.Read(ctx, "bench", "bench.bin", buf, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheHitSmallRead measures per-operation latency for small
// cached reads (simulates random 4KB reads from a DataLoader).
func BenchmarkCacheHitSmallRead(b *testing.B) {
	fileSize := 8 * 1024 * 1024
	cm, _ := setupBenchCache(b, fileSize)
	ctx := context.Background()

	// Pre-populate cache.
	fullBuf := make([]byte, fileSize)
	if _, err := cm.Read(ctx, "bench", "bench.bin", fullBuf, 0); err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, 4096)
	b.SetBytes(4096)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off := int64((i * 4096) % fileSize)
		_, err := cm.Read(ctx, "bench", "bench.bin", buf, off)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCacheMiss measures per-block fetch latency (local backend, so
// this is the overhead of the cache engine - fetch, store, metadata write).
func BenchmarkCacheMiss(b *testing.B) {
	blockSize := 4 * 1024 * 1024
	cm, _ := setupBenchCache(b, 64*1024*1024) // 64 MB
	ctx := context.Background()
	buf := make([]byte, blockSize)

	b.SetBytes(int64(blockSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Read different block each time so there's no cache hit.
		// Use modulo to wrap around the file.
		blockIdx := i % 16
		off := int64(blockIdx) * int64(blockSize)

		// Clear the cached block first so it's a miss.
		key := blockKey("bench", "bench.bin", blockIdx)
		if meta, err := cm.getBlockMeta(key); err == nil {
			cm.deleteBlock(key, meta)
		}

		_, err := cm.Read(ctx, "bench", "bench.bin", buf, off)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCoalescerRecordRead measures the overhead of pattern detection
// per read operation.
func BenchmarkCoalescerRecordRead(b *testing.B) {
	rc := NewReadCoalescer(DefaultBlockSize)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc.RecordRead("backend", "file.bin", int64(i)*4096, 4096)
	}
}

// BenchmarkCoalescerGetPattern measures the overhead of querying the
// access pattern.
func BenchmarkCoalescerGetPattern(b *testing.B) {
	rc := NewReadCoalescer(DefaultBlockSize)

	// Pre-populate with sequential reads.
	for i := 0; i < 100; i++ {
		rc.RecordRead("backend", "file.bin", int64(i)*4096, 4096)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rc.GetPattern("backend", "file.bin")
	}
}

// BenchmarkReadaheadEnqueue measures the overhead of enqueuing readahead
// jobs (budget check, dedup, channel send).
func BenchmarkReadaheadEnqueue(b *testing.B) {
	cm, _ := setupBenchCache(b, 8*1024*1024)
	ra := cm.readahead

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ra.enqueue(readaheadJob{
			backendName: "bench",
			path:        "bench.bin",
			blockIdx:    i % 1000, // Vary to avoid dedup
			etag:        "etag",
			priority:    priorityFile,
		})
	}
}

// BenchmarkStatWithCache measures the overhead of stat calls with caching.
func BenchmarkStatWithCache(b *testing.B) {
	cm, _ := setupBenchCache(b, 1024)
	ctx := context.Background()

	// First call populates the stat cache.
	be, _ := cm.registry.Get("bench")
	if _, err := cm.statWithCache(ctx, be, "bench", "bench.bin"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cm.statWithCache(ctx, be, "bench", "bench.bin")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBlockKey measures the overhead of generating block keys.
func BenchmarkBlockKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = blockKey("my-backend", "path/to/file.bin", i%100)
	}
}

// BenchmarkBlockLocalPath measures the overhead of computing on-disk paths
// (includes SHA-256 hashing).
func BenchmarkBlockLocalPath(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = blockLocalPath("/cache", "my-backend", "path/to/file.bin", i%100)
	}
}

// BenchmarkCacheHitParallel measures aggregate throughput with multiple
// concurrent readers. Validates the M3 spec target: "32 readers: >10 GB/s".
func BenchmarkCacheHitParallel(b *testing.B) {
	fileSize := 8 * 1024 * 1024 // 8 MB = 2 blocks
	cm, _ := setupBenchCache(b, fileSize)
	ctx := context.Background()

	// Pre-populate cache.
	preBuf := make([]byte, fileSize)
	if _, err := cm.Read(ctx, "bench", "bench.bin", preBuf, 0); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(fileSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, fileSize)
		for pb.Next() {
			_, err := cm.Read(ctx, "bench", "bench.bin", buf, 0)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
