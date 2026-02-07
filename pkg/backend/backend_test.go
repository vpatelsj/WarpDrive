package backend

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newLocalTestBackend(t *testing.T, dir string) *RcloneBackend {
	t.Helper()
	b, err := NewRcloneBackend("test_local", "local", dir, map[string]string{})
	if err != nil {
		t.Fatalf("Failed to create test backend: %v", err)
	}
	return b
}

func populateTestDir(t *testing.T, dir string) {
	t.Helper()

	sub := filepath.Join(dir, "subdir")
	if err := os.MkdirAll(sub, 0755); err != nil {
		t.Fatal(err)
	}

	for _, f := range []struct {
		name    string
		content []byte
	}{
		{"file1.txt", []byte("hello world")},
		{"file2.txt", []byte("goodbye world")},
		{"subdir/nested.txt", []byte("nested content")},
	} {
		if err := os.WriteFile(filepath.Join(dir, f.name), f.content, 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create a 16 MB file with repeating byte pattern.
	large, err := os.Create(filepath.Join(dir, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	defer large.Close()
	blk := make([]byte, 4*1024*1024) // 4 MB
	for i := range blk {
		blk[i] = byte(i % 256)
	}
	for i := 0; i < 4; i++ {
		if _, err := large.Write(blk); err != nil {
			t.Fatal(err)
		}
	}
}

// ---- Registry tests ----

func TestRegistry(t *testing.T) {
	reg := NewRegistry()
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)

	if err := reg.Register(b); err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if err := reg.Register(b); err == nil {
		t.Error("duplicate Register should fail")
	}

	got, err := reg.Get("test_local")
	if err != nil {
		t.Fatal("Get returned error:", err)
	}
	if got.Name() != "test_local" {
		t.Errorf("Name = %q, want test_local", got.Name())
	}
	if _, err := reg.Get("missing"); err == nil {
		t.Error("Get(missing) should return error")
	}
	if all := reg.All(); len(all) != 1 {
		t.Errorf("All() len = %d, want 1", len(all))
	}
	if err := reg.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// ---- RcloneBackend tests ----

func TestRcloneBackend_List(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	entries, err := b.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 3 {
		t.Errorf("List returned %d entries, want >= 3", len(entries))
	}

	var hasDir, hasFile bool
	for _, e := range entries {
		if e.IsDir {
			hasDir = true
		} else {
			hasFile = true
		}
	}
	if !hasDir {
		t.Error("no directory entry found")
	}
	if !hasFile {
		t.Error("no file entry found")
	}
}

func TestRcloneBackend_Stat(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	ctx := context.Background()

	info, err := b.Stat(ctx, "file1.txt")
	if err != nil {
		t.Fatalf("Stat file: %v", err)
	}
	if info.Size != 11 {
		t.Errorf("file1.txt Size = %d, want 11", info.Size)
	}
	if info.IsDir {
		t.Error("file1.txt reported as dir")
	}

	info, err = b.Stat(ctx, "subdir")
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if !info.IsDir {
		t.Error("subdir not reported as dir")
	}

	if _, err := b.Stat(ctx, "no_such_file.txt"); err == nil {
		t.Error("Stat nonexistent should error")
	}
}

func TestRcloneBackend_ReadAt(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	ctx := context.Background()

	buf := make([]byte, 5)
	n, err := b.ReadAt(ctx, "file1.txt", buf, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("ReadAt(0) = %q, want hello", string(buf[:n]))
	}

	buf = make([]byte, 5)
	n, err = b.ReadAt(ctx, "file1.txt", buf, 6)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("ReadAt(6) = %q, want world", string(buf[:n]))
	}
}

func TestRcloneBackend_ReadAt_BeyondEOF(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	buf := make([]byte, 100)
	_, err := b.ReadAt(context.Background(), "file1.txt", buf, 1000)
	if err != io.EOF {
		t.Errorf("ReadAt beyond EOF: got %v, want io.EOF", err)
	}
}

func TestRcloneBackend_ReadAt_LargeFile(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	ctx := context.Background()
	blockSz := 4 * 1024 * 1024
	buf := make([]byte, blockSz)

	n, err := b.ReadAt(ctx, "large.bin", buf, 0)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != blockSz {
		t.Errorf("block 0: got %d bytes, want %d", n, blockSz)
	}
	for i := 0; i < n; i++ {
		if buf[i] != byte(i%256) {
			t.Fatalf("block 0: byte %d = %d, want %d", i, buf[i], byte(i%256))
		}
	}
}

func TestRcloneBackend_Open(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	rc, err := b.Open(context.Background(), "file1.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello world" {
		t.Errorf("Open = %q, want hello world", string(data))
	}
}

func TestRcloneBackend_ConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	populateTestDir(t, dir)
	b := newLocalTestBackend(t, dir)
	defer b.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 11)
			_, err := b.ReadAt(context.Background(), "file1.txt", buf, 0)
			if err != nil && err != io.EOF {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent read error: %v", err)
	}
}
