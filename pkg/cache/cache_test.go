package cache

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/config"
)

// setupTestCache creates a CacheManager with a local rclone backend pointing
// at a temp directory with test data.
func setupTestCache(t *testing.T) (*CacheManager, string) {
	t.Helper()

	// Create source data directory with test files.
	srcDir := t.TempDir()
	data := make([]byte, 10*1024*1024) // 10 MB
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "large.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "small.txt"), []byte("hello warpdrive"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Create backend.
	reg := backend.NewRegistry()
	be, err := backend.NewRcloneBackend("test-local", "local", srcDir, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	reg.Register(be)

	// Create cache.
	cacheDir := t.TempDir()
	cm, err := New(config.CacheConfig{
		Path:      cacheDir,
		MaxSize:   100 * 1024 * 1024, // 100 MB
		BlockSize: 4 * 1024 * 1024,   // 4 MB
	}, reg)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cm.Close()
	})

	return cm, srcDir
}

func TestCacheMiss(t *testing.T) {
	cm, _ := setupTestCache(t)

	buf := make([]byte, 5)
	n, err := cm.Read(context.Background(), "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(buf[:n]) != "hello" {
		t.Errorf("got %q, want %q", string(buf[:n]), "hello")
	}

	stats := cm.GetStats()
	if stats.Misses == 0 {
		t.Error("expected cache miss")
	}
}

func TestCacheHit(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// First read (miss).
	buf := make([]byte, 5)
	_, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Second read (should be a hit).
	buf2 := make([]byte, 5)
	_, err = cm.Read(ctx, "test-local", "small.txt", buf2, 0)
	if err != nil {
		t.Fatal(err)
	}

	stats := cm.GetStats()
	if stats.Hits == 0 {
		t.Error("expected cache hit on second read")
	}
}

func TestCacheMultiBlock(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	// Read across multiple blocks (10 MB file, 4 MB blocks = 3 blocks).
	buf := make([]byte, 10*1024*1024)
	n, err := cm.Read(ctx, "test-local", "large.bin", buf, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 10*1024*1024 {
		t.Fatalf("got %d bytes, want %d", n, 10*1024*1024)
	}

	// Verify data integrity.
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%251) {
			t.Fatalf("data mismatch at offset %d: got %d, want %d", i, buf[i], byte(i%251))
		}
	}
}

func TestCacheConcurrentReads(t *testing.T) {
	cm, _ := setupTestCache(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	errors := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 15)
			n, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
			if err != nil {
				errors <- err
				return
			}
			if string(buf[:n]) != "hello warpdrive" {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent read error: %v", err)
	}
}

func TestCacheETagInvalidation(t *testing.T) {
	cm, srcDir := setupTestCache(t)
	ctx := context.Background()

	// Read small.txt (fills cache).
	buf := make([]byte, 15)
	_, err := cm.Read(ctx, "test-local", "small.txt", buf, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Modify the source file (changes ETag/modtime).
	time.Sleep(50 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(srcDir, "small.txt"), []byte("modified!!!"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Force file meta expiry by deleting it from badger.
	fk := fileKey("test-local", "small.txt")
	_ = cm.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(fk))
	})

	// Re-read should detect the change.
	buf2 := make([]byte, 11)
	n, err := cm.Read(ctx, "test-local", "small.txt", buf2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf2[:n]) != "modified!!!" {
		t.Errorf("got %q, want %q after invalidation", string(buf2[:n]), "modified!!!")
	}
}

func TestBlockKey(t *testing.T) {
	k := blockKey("mybackend", "path/to/file.dat", 3)
	want := "block:mybackend:path/to/file.dat:3"
	if k != want {
		t.Errorf("got %q, want %q", k, want)
	}
}

func TestBlockLocalPath(t *testing.T) {
	p := blockLocalPath("/cache", "be", "file.txt", 0)
	if filepath.Ext(p) != ".blk" {
		t.Errorf("expected .blk extension, got %s", p)
	}
	if !filepath.IsAbs(p) {
		t.Errorf("expected absolute path, got %s", p)
	}
}

func TestStats(t *testing.T) {
	var s Stats
	s.Hits.Add(5)
	s.Misses.Add(3)
	s.BytesCached.Add(1024)

	snap := s.Snapshot()
	if snap.Hits != 5 || snap.Misses != 3 || snap.BytesCached != 1024 {
		t.Errorf("unexpected stats: %+v", snap)
	}
}
