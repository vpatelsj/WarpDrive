# M1: Core Mount + Single Backend — Technical Spec

## Timeline: Weeks 1-6

## Goal

A researcher can FUSE-mount a single Azure Blob container on a GPU node and read training data through the mount with local NVMe caching. This milestone proves the architecture end-to-end.

---

## M1.1: rclone Backend Integration (Weeks 1-3)

### Design

We use rclone as a **Go library**, not a subprocess. Import `github.com/rclone/rclone` and use its `fs` and `backend` packages directly. This gives us native Go access to 70+ storage backends with battle-tested retry logic, connection pooling, and auth handling.

The key insight: rclone's `fs.Fs` interface already provides `List`, `NewObject`, `Open`, etc. Our `Backend` interface wraps this with a simpler API tuned for our cache engine's needs (block-level ReadAt, not streaming).

### Implementation: `pkg/backend/rclone.go`

```go
package backend

import (
    "context"
    "fmt"
    "io"
    "sync"

    // rclone imports
    _ "github.com/rclone/rclone/backend/azureblob"   // Register azureblob
    _ "github.com/rclone/rclone/backend/s3"           // Register s3
    _ "github.com/rclone/rclone/backend/googlecloudstorage" // Register gcs
    _ "github.com/rclone/rclone/backend/local"        // Register local
    "github.com/rclone/rclone/fs"
    "github.com/rclone/rclone/fs/config/configmap"
)

// RcloneBackend wraps an rclone fs.Fs as a Backend.
type RcloneBackend struct {
    name     string
    backType string
    rfs      fs.Fs
    mu       sync.RWMutex
}

// NewRcloneBackend creates a backend from config.
// backendType is the rclone backend name (e.g., "azureblob", "s3").
// params maps rclone config keys to values.
// remotePath is the bucket/container + optional prefix.
func NewRcloneBackend(name, backendType, remotePath string, params map[string]string) (*RcloneBackend, error) {
    // Build rclone config map
    m := configmap.Simple(params)
    
    // Create the rclone Fs
    // The remote format is "backendType:remotePath"
    // But since we're using configmap directly, we need to use fs.NewFs
    // with an in-memory config.
    
    // Register a temporary remote in rclone's config
    // ...implementation creates rclone Fs from params...
    
    return &RcloneBackend{
        name:     name,
        backType: backendType,
        // rfs: initialized rclone Fs
    }, nil
}

func (b *RcloneBackend) Name() string { return b.name }
func (b *RcloneBackend) Type() string { return b.backType }

func (b *RcloneBackend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
    // Use b.rfs.List(ctx, prefix) which returns fs.DirEntries
    // Convert each entry to ObjectInfo
    // Handle pagination internally — rclone does this for us
    return nil, nil
}

func (b *RcloneBackend) Stat(ctx context.Context, path string) (ObjectInfo, error) {
    // Use b.rfs.NewObject(ctx, path) for files
    // For directories, try List(ctx, path) and check if non-empty
    return ObjectInfo{}, nil
}

func (b *RcloneBackend) ReadAt(ctx context.Context, path string, p []byte, off int64) (int, error) {
    // Open object with range request
    // Use rclone's Object.Open with &fs.RangeOption{Start: off, End: off + int64(len(p)) - 1}
    // Read into p, return bytes read
    return 0, nil
}

func (b *RcloneBackend) Open(ctx context.Context, path string) (io.ReadCloser, error) {
    // Open object for full streaming read
    // Use rclone's Object.Open with no range options
    return nil, nil
}

func (b *RcloneBackend) Write(ctx context.Context, path string, r io.Reader, size int64) error {
    // Use rclone's fs.Put or Fs.Put to write
    return nil
}

func (b *RcloneBackend) Delete(ctx context.Context, path string) error {
    // Use rclone's Object.Remove
    return nil
}

func (b *RcloneBackend) Close() error {
    // Cleanup any connections
    return nil
}
```

### Key rclone integration notes for the LLM agent:

1. **Importing backends**: rclone uses blank imports to register backends. You MUST import the backends you need (azureblob, s3, etc.) with `_` imports.
2. **Creating an Fs without config file**: Use `rclone/fs/config/configmap.Simple` to pass config as a Go map. Then use the backend's `NewFs` function directly.
3. **Range reads**: rclone objects support `Open(ctx, options...)` where options include `*fs.RangeOption`. This maps to HTTP Range headers / S3 GetObject with Range.
4. **Thread safety**: rclone Fs instances are safe for concurrent use. Individual Object handles are NOT — open a new one per read.

### Tests for M1.1

```go
// pkg/backend/backend_test.go

func TestRcloneBackend_List(t *testing.T) {
    // Use rclone's "memory" backend for testing (or local filesystem)
    // Create temp dir, populate with test files
    // Create backend pointing to temp dir via "local" backend type
    // Verify List returns expected ObjectInfo entries
}

func TestRcloneBackend_ReadAt(t *testing.T) {
    // Create a test file with known content (e.g., 16MB of sequential bytes)
    // ReadAt at offset 0, verify first block
    // ReadAt at offset 4MB, verify second block
    // ReadAt at offset beyond file size, verify io.EOF
}

func TestRcloneBackend_Stat(t *testing.T) {
    // Stat a known file — verify size, modtime
    // Stat a non-existent file — verify ErrNotFound
    // Stat a directory — verify IsDir=true
}

func TestRcloneBackend_LargeFile(t *testing.T) {
    // Create a 1GB test file
    // Read in 4MB chunks via ReadAt, verify each chunk
    // Ensure no memory leak (check runtime.MemStats)
}
```

---

## M1.2: Local NVMe Cache Engine (Weeks 2-5)

### Design

The cache stores fixed-size blocks on local NVMe. Each block is identified by `(backendName, path, blockIndex)`. Blocks are stored as flat files on the NVMe filesystem. Metadata (mapping, LRU timestamps, ETags) is stored in a badger key-value store.

**Why block-level, not file-level?**
Training datasets are often large tar files (1-10GB WebDataset shards). File-level caching would require downloading the entire 10GB shard before the first byte is available. Block-level caching allows immediate access to the first 4MB while the rest streams in.

### Data Layout on NVMe

```
/nvme/warpdrive-cache/
├── meta/                    # Badger DB directory
│   ├── 000001.vlog
│   └── 000001.sst
├── blocks/                  # Cached blocks
│   ├── ab/                  # First 2 chars of hash
│   │   ├── abcdef1234.blk   # Block data file
│   │   └── abcdef5678.blk
│   └── cd/
│       └── cd901234ab.blk
└── tmp/                     # In-progress downloads
```

Block filename = SHA256(backendName + ":" + path + ":" + blockIndex) truncated to 20 hex chars.

### Metadata Schema (Badger keys)

```
Key format:    "block:{backendName}:{path}:{blockIndex}"
Value format:  JSON-encoded BlockMeta

Key format:    "file:{backendName}:{path}"
Value format:  JSON-encoded FileMeta (ETag, size, last validated time)
```

```go
// BlockMeta stored in badger for each cached block.
type BlockMeta struct {
    BackendName  string    `json:"b"`
    Path         string    `json:"p"`
    BlockIndex   int       `json:"i"`
    LocalPath    string    `json:"l"`    // Path on NVMe
    Size         int       `json:"s"`    // Actual bytes in block (last block may be smaller)
    ETag         string    `json:"e"`    // ETag from origin at time of fetch
    CachedAt     time.Time `json:"ca"`
    LastAccess   time.Time `json:"la"`   // Updated on every read
}

// FileMeta tracks remote file metadata for cache validation.
type FileMeta struct {
    BackendName    string    `json:"b"`
    Path           string    `json:"p"`
    Size           int64     `json:"s"`
    ETag           string    `json:"e"`
    LastValidated  time.Time `json:"lv"`  // When we last checked ETag against origin
}
```

### Implementation: `pkg/cache/cache.go`

```go
package cache

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "sync/atomic"
    "time"

    "github.com/dgraph-io/badger/v4"
    "github.com/warpdrive/warpdrive/pkg/backend"
)

type CacheManager struct {
    cfg       Config
    db        *badger.DB          // Metadata store
    backends  *backend.Registry   // To fetch from origin
    
    // In-flight tracking: prevent duplicate fetches for same block
    inflight  sync.Map            // key: blockKey -> *singleflight result
    
    // Metrics (atomic counters)
    hits      atomic.Uint64
    misses    atomic.Uint64
    evictions atomic.Uint64
    currentSz atomic.Int64
    
    // Readahead
    readahead *ReadaheadManager
    
    // Eviction
    evictMu   sync.Mutex
    evictCh   chan struct{}        // Signal to run eviction
    
    // Shutdown
    closeCh   chan struct{}
    wg        sync.WaitGroup
}

// NewCacheManager creates and initializes the cache.
func NewCacheManager(cfg Config, backends *backend.Registry) (*CacheManager, error) {
    // 1. Create cache directories if needed
    // 2. Open badger DB at cfg.Dir/meta/
    // 3. Calculate current cache size from existing blocks
    // 4. Start background eviction goroutine
    // 5. Start readahead manager
    return nil, nil
}

// Read is the main cache entry point. Called by the FUSE layer.
func (cm *CacheManager) Read(ctx context.Context, backendName, path string, p []byte, off int64) (int, error) {
    // 1. Determine which blocks are needed for this read
    //    startBlock = off / blockSize
    //    endBlock = (off + len(p) - 1) / blockSize
    //
    // 2. For each block:
    //    a. Check if cached (badger lookup)
    //    b. If cached: validate ETag if stale (beyond StaleTTL)
    //       - If ETag changed: invalidate, fetch fresh
    //       - If ETag same: update LastValidated, serve from cache
    //    c. If not cached: fetch from backend
    //       - Use singleflight to deduplicate concurrent requests for same block
    //       - Write to NVMe, record in badger
    //    d. Read requested byte range from local NVMe file
    //    e. Update LastAccess in badger
    //    f. Trigger readahead for subsequent blocks
    //
    // 3. Copy data into p, return bytes read
    //
    // 4. Update hit/miss metrics
    return 0, nil
}

// fetchBlock downloads a single block from the origin backend.
func (cm *CacheManager) fetchBlock(ctx context.Context, backendName, path string, blockIndex int) (*BlockMeta, error) {
    // 1. Calculate byte range: start = blockIndex * blockSize, end = min(start + blockSize, fileSize)
    // 2. Allocate buffer
    // 3. Call backend.ReadAt(ctx, path, buf, start)
    // 4. Write buf to temp file in tmp/ dir
    // 5. Rename temp file to final path in blocks/ dir (atomic)
    // 6. Record BlockMeta in badger
    // 7. Update currentSize
    // 8. If currentSize > high water mark, signal eviction
    return nil, nil
}

// blockKey returns the badger key for a block.
func blockKey(backendName, path string, blockIndex int) string {
    return fmt.Sprintf("block:%s:%s:%d", backendName, path, blockIndex)
}

// blockLocalPath returns the NVMe file path for a block.
func (cm *CacheManager) blockLocalPath(backendName, path string, blockIndex int) string {
    h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", backendName, path, blockIndex)))
    hex := hex.EncodeToString(h[:10]) // 20 hex chars
    return filepath.Join(cm.cfg.Dir, "blocks", hex[:2], hex+".blk")
}
```

### Implementation: `pkg/cache/eviction.go`

```go
package cache

import (
    "sort"
    "time"
)

// evictionLoop runs in a background goroutine.
// Wakes up when signaled (cache exceeds high water mark) or periodically.
func (cm *CacheManager) evictionLoop() {
    defer cm.wg.Done()
    
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-cm.closeCh:
            return
        case <-cm.evictCh:
            cm.runEviction()
        case <-ticker.C:
            if cm.shouldEvict() {
                cm.runEviction()
            }
        }
    }
}

// runEviction evicts LRU blocks until cache is below low water mark.
func (cm *CacheManager) runEviction() {
    cm.evictMu.Lock()
    defer cm.evictMu.Unlock()
    
    targetSize := int64(float64(cm.cfg.MaxSize) * cm.cfg.EvictLowWater)
    
    // 1. Scan all block entries in badger
    // 2. Sort by LastAccess ascending (oldest first)
    // 3. Delete blocks from NVMe and badger until currentSize <= targetSize
    // 4. Update eviction counter
}

// shouldEvict returns true if cache exceeds high water mark.
func (cm *CacheManager) shouldEvict() bool {
    return cm.currentSz.Load() > int64(float64(cm.cfg.MaxSize)*cm.cfg.EvictHighWater)
}
```

### Implementation: `pkg/cache/readahead.go`

```go
package cache

import (
    "context"
    "sync"
    "sync/atomic"
)

// ReadaheadManager handles background prefetching of upcoming blocks.
type ReadaheadManager struct {
    cm            *CacheManager
    maxInflight   int64          // Max bytes of inflight prefetch (default 256MB)
    currentFlight atomic.Int64
    
    // Worker pool
    workCh  chan readaheadJob
    wg      sync.WaitGroup
}

type readaheadJob struct {
    backendName string
    path        string
    blockIndex  int
}

// TriggerReadahead is called after a cache hit or fetch.
// It prefetches the next N blocks if they're not already cached.
func (ra *ReadaheadManager) TriggerReadahead(backendName, path string, currentBlock int) {
    // For i in 1..readaheadBlocks:
    //   nextBlock = currentBlock + i
    //   If not already cached and not already inflight:
    //     Submit readaheadJob to workCh (non-blocking, drop if full)
}

// worker processes readahead jobs.
func (ra *ReadaheadManager) worker() {
    defer ra.wg.Done()
    for job := range ra.workCh {
        // Check budget: if currentFlight > maxInflight, skip
        // Otherwise: fetch block via cm.fetchBlock
        // Update currentFlight
    }
}
```

### Tests for M1.2

```go
func TestCacheManager_ReadCacheMiss(t *testing.T) {
    // Setup: in-memory backend with known file content
    // Read at offset 0 — should fetch from backend, cache locally
    // Verify data matches original
    // Verify block file exists on disk
    // Verify badger has BlockMeta entry
}

func TestCacheManager_ReadCacheHit(t *testing.T) {
    // Setup: pre-populate cache with a block
    // Read the same block — should NOT call backend
    // Verify hit counter incremented
}

func TestCacheManager_Eviction(t *testing.T) {
    // Setup: small max cache (e.g., 3 blocks worth)
    // Read 5 different blocks — should trigger eviction
    // Verify cache size stays within bounds
    // Verify oldest-accessed blocks were evicted
}

func TestCacheManager_ConcurrentReads(t *testing.T) {
    // Launch 100 goroutines all reading same block simultaneously
    // Verify only 1 backend fetch occurred (singleflight)
    // Verify all goroutines got correct data
}

func TestCacheManager_ETagInvalidation(t *testing.T) {
    // Read a block, cache it
    // Change the file content on the backend (different ETag)
    // Wait for StaleTTL to expire
    // Read again — should detect ETag mismatch, re-fetch
}

func BenchmarkCacheManager_ReadCacheHit(b *testing.B) {
    // Pre-populate cache
    // Benchmark cache-hit read performance
    // Target: <0.5ms per 4MB block read from NVMe
}
```

---

## M1.3: FUSE Filesystem (Weeks 3-6)

### Design

We use `github.com/hanwen/go-fuse/v2` which supports low-level FUSE API with splice/zero-copy. The filesystem is **read-only in M1**. Write support comes in M2.

The FUSE implementation maps:
- `Lookup` → backend.Stat
- `Getattr` → cached metadata from backend.Stat
- `ReadDir` → backend.List (cached with TTL)
- `Open` → create file handle with cache manager reference
- `Read` → cache.Read (which handles cache hit/miss transparently)

### Implementation: `pkg/fuse/fs.go`

```go
package fuse

import (
    "context"
    "sync"
    "syscall"
    "time"

    gofuse "github.com/hanwen/go-fuse/v2/fs"
    "github.com/hanwen/go-fuse/v2/fuse"
    
    "github.com/warpdrive/warpdrive/pkg/backend"
    "github.com/warpdrive/warpdrive/pkg/cache"
    "github.com/warpdrive/warpdrive/pkg/namespace"
)

// WarpDriveRoot is the FUSE root node.
type WarpDriveRoot struct {
    gofuse.Inode
    
    ns       *namespace.Namespace  // Maps paths to backends
    cache    cache.Manager
    backends *backend.Registry
    
    // Directory listing cache
    dirCache sync.Map  // path -> *dirCacheEntry
    dirTTL   time.Duration
}

type dirCacheEntry struct {
    entries  []fuse.DirEntry
    fetched  time.Time
}

// Ensure we implement the right interfaces.
var _ = (gofuse.NodeLookuper)((*WarpDriveRoot)(nil))
var _ = (gofuse.NodeReaddirer)((*WarpDriveRoot)(nil))

// WarpDriveDir represents a directory in the filesystem.
type WarpDriveDir struct {
    gofuse.Inode
    root        *WarpDriveRoot
    backendName string   // Which backend this dir belongs to
    remotePath  string   // Path within the backend
}

// WarpDriveFile represents an open file.
type WarpDriveFile struct {
    gofuse.Inode
    root        *WarpDriveRoot
    backendName string
    remotePath  string
    size        int64
    etag        string
}

// WarpDriveFileHandle is created on Open().
type WarpDriveFileHandle struct {
    file *WarpDriveFile
}

// Read implements the FUSE Read call.
func (fh *WarpDriveFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, error) {
    // Delegate to cache manager
    n, err := fh.file.root.cache.Read(ctx, fh.file.backendName, fh.file.remotePath, dest, off)
    if err != nil {
        return nil, syscall.EIO
    }
    return fuse.ReadResultData(dest[:n]), nil
}
```

### Mount Setup: `pkg/fuse/mount.go`

```go
package fuse

import (
    "time"
    
    gofuse "github.com/hanwen/go-fuse/v2/fs"
    "github.com/hanwen/go-fuse/v2/fuse"
)

type MountConfig struct {
    MountPoint    string
    AllowOther    bool
    FUSEWorkers   int           // Default 32
    MaxReadBytes  int           // Default 1MB (1048576)
    DirCacheTTL   time.Duration // Default 300s
}

// Mount creates and serves the FUSE filesystem.
// Blocks until the filesystem is unmounted or ctx is cancelled.
func Mount(ctx context.Context, cfg MountConfig, root *WarpDriveRoot) error {
    opts := &gofuse.Options{
        MountOptions: fuse.MountOptions{
            AllowOther:    cfg.AllowOther,
            Name:          "warpdrive",
            FsName:        "warpdrive",
            MaxReadAhead:  1024 * 1024, // 1MB
            DirectMount:   true,
            Debug:         false,
        },
        AttrTimeout:  &ttl,       // 60s
        EntryTimeout: &ttl,       // 60s
    }
    
    server, err := gofuse.Mount(cfg.MountPoint, root, opts)
    if err != nil {
        return fmt.Errorf("fuse.Mount: mount at %s: %w", cfg.MountPoint, err)
    }
    
    // Wait for unmount or context cancellation
    go func() {
        <-ctx.Done()
        server.Unmount()
    }()
    
    server.Wait()
    return nil
}
```

### Main binary: `cmd/warpdrive-mount/main.go`

```go
package main

import (
    "context"
    "flag"
    "log/slog"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/warpdrive/warpdrive/pkg/backend"
    "github.com/warpdrive/warpdrive/pkg/cache"
    "github.com/warpdrive/warpdrive/pkg/config"
    "github.com/warpdrive/warpdrive/pkg/fuse"
    "github.com/warpdrive/warpdrive/pkg/namespace"
)

func main() {
    configPath := flag.String("config", "/etc/warpdrive/config.yaml", "Path to config file")
    flag.Parse()
    
    // 1. Load config
    cfg, err := config.Load(*configPath)
    if err != nil {
        slog.Error("Failed to load config", "error", err)
        os.Exit(1)
    }
    
    // 2. Create backend registry
    reg := backend.NewRegistry()
    for _, bcfg := range cfg.Backends {
        b, err := backend.NewRcloneBackend(bcfg.Name, bcfg.Type, /* ... */)
        if err != nil {
            slog.Error("Failed to create backend", "name", bcfg.Name, "error", err)
            os.Exit(1)
        }
        reg.Register(bcfg.Name, b)
    }
    
    // 3. Create cache manager
    cacheMgr, err := cache.NewCacheManager(cfg.Cache, reg)
    if err != nil {
        slog.Error("Failed to create cache", "error", err)
        os.Exit(1)
    }
    defer cacheMgr.Close()
    
    // 4. Create namespace
    ns := namespace.New(cfg.Backends)
    
    // 5. Create and mount FUSE filesystem
    root := fuse.NewRoot(ns, cacheMgr, reg)
    
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
    defer cancel()
    
    slog.Info("Mounting WarpDrive filesystem", "mount_point", cfg.MountPoint)
    if err := fuse.Mount(ctx, fuse.MountConfig{
        MountPoint: cfg.MountPoint,
        AllowOther: cfg.AllowOther,
    }, root); err != nil {
        slog.Error("Mount failed", "error", err)
        os.Exit(1)
    }
}
```

### Tests for M1.3

```go
func TestFUSE_Readdir(t *testing.T) {
    // Mount with in-memory backend containing test files
    // os.ReadDir on mount point — verify files listed
}

func TestFUSE_ReadFile(t *testing.T) {
    // Mount with backend containing a known file
    // os.ReadFile on mounted path — verify content matches
}

func TestFUSE_LargeSequentialRead(t *testing.T) {
    // Backend has a 100MB file
    // Read entire file through mount in 4MB chunks
    // Verify data integrity (checksum)
    // Verify cache was populated
}

func TestFUSE_ConcurrentReaders(t *testing.T) {
    // Launch 32 goroutines, each reading different files
    // Verify no deadlocks, no data corruption
    // Measure aggregate throughput
}

func TestFUSE_WriteReturnsROFS(t *testing.T) {
    // Attempt to create/write a file through mount
    // Verify EROFS returned
}
```

---

## M1 Exit Criteria Validation Script

```bash
#!/bin/bash
# hack/validate-m1.sh

set -e

echo "=== M1 Validation ==="

# 1. Mount is up
mountpoint -q /data || { echo "FAIL: /data not mounted"; exit 1; }
echo "PASS: Mount is active"

# 2. Can list files
ls /data/training_sets/ > /dev/null || { echo "FAIL: Cannot list"; exit 1; }
echo "PASS: Directory listing works"

# 3. Can read a file
head -c 1024 /data/training_sets/shard-0000.tar > /dev/null || { echo "FAIL: Cannot read"; exit 1; }
echo "PASS: File read works"

# 4. Cache hit performance
warpdrive-bench --dir /data/training_sets/ --readers 1 --duration 30s --cached
# Should report >4 GB/s

# 5. Concurrent throughput
warpdrive-bench --dir /data/training_sets/ --readers 32 --duration 30s --cached
# Should report >10 GB/s aggregate

# 6. Stability test (24h in CI, manual for production)
# warpdrive-bench --dir /data/training_sets/ --readers 16 --duration 24h --cached --check-memory

echo "=== M1 Validation Complete ==="
```

---

## COPILOT PROMPT — M1.1: rclone Backend Integration

```
You are building the storage backend package for WarpDrive, a cross-cloud data fabric for GPU training.

Project: Go module at github.com/warpdrive/warpdrive
Package: pkg/backend/

## What to build

Create a Backend interface and an implementation that wraps rclone as a Go library.

## Files to create

1. `pkg/backend/backend.go` — Interface definitions:
   - `ObjectInfo` struct with Path, Size, ModTime, ETag, IsDir, ContentType
   - `Backend` interface with methods: Name(), Type(), List(ctx, prefix), Stat(ctx, path), ReadAt(ctx, path, p, off), Open(ctx, path), Write(ctx, path, reader, size), Delete(ctx, path), Close()
   - `Registry` struct that stores named backends with Register, Get, All, Close methods

2. `pkg/backend/rclone.go` — rclone implementation:
   - `RcloneBackend` struct wrapping rclone's fs.Fs
   - `NewRcloneBackend(name, backendType, remotePath string, params map[string]string)` constructor
   - Import rclone backends with blank imports: azureblob, s3, googlecloudstorage, local
   - Use github.com/rclone/rclone/fs and github.com/rclone/rclone/fs/config/configmap
   - For List: use fs.DirEntries, convert to []ObjectInfo
   - For ReadAt: open object with fs.RangeOption{Start, End}, read into buffer
   - For Stat: use Fs.NewObject, convert to ObjectInfo
   - Handle errors: wrap with backend name, map rclone fs.ErrorObjectNotFound to sentinel ErrNotFound

3. `pkg/backend/backend_test.go` — Tests using the "local" rclone backend:
   - TestList: create temp dir with files, verify listing
   - TestReadAt: create 16MB file, read at various offsets
   - TestStat: verify file and dir stat
   - TestReadAtBeyondEOF: verify io.EOF behavior
   - TestConcurrentReads: 50 goroutines reading same file

## Constraints
- Go 1.22+
- Use log/slog for logging
- All public functions return error as last value
- Use fmt.Errorf("backend.FuncName: %w", err) for error wrapping
- Define ErrNotFound as sentinel error
- Thread-safe: all methods must be safe for concurrent use
```

---

## COPILOT PROMPT — M1.2: Cache Engine

```
You are building the NVMe cache engine for WarpDrive, a cross-cloud data fabric for GPU training.

Project: Go module at github.com/warpdrive/warpdrive
Package: pkg/cache/
Dependency: pkg/backend/ (the Backend interface, already implemented)

## What to build

A block-level cache that stores 4MB chunks of remote files on local NVMe. Sits between the FUSE layer and the storage backends.

## Files to create

1. `pkg/cache/cache.go` — CacheManager:
   - Config struct: Dir, MaxSize, BlockSize (default 4MB/4194304), ReadaheadBlocks (4), MaxParallelFetch (16), StaleTTL (60s), EvictHighWater (0.90), EvictLowWater (0.80)
   - Stats struct: HitCount, MissCount, EvictionCount, CurrentSize, MaxSize, PrefetchCount, BackendFetches
   - NewCacheManager(cfg Config, backends *backend.Registry) — opens badger DB, calculates current size, starts background goroutines
   - Read(ctx, backendName, path, p []byte, off int64) (int, error) — main entry point:
     * Calculate which blocks cover the byte range [off, off+len(p))
     * For each block: look up in badger. If found and not stale, read from NVMe. If stale, validate ETag against backend. If not found, fetch from backend.
     * Use sync.Map + singleflight pattern to deduplicate concurrent fetches of same block
     * After read, trigger readahead
     * Return bytes copied into p
   - Invalidate(backendName, path) — remove all blocks for a file
   - Stats() — return atomic counters
   - Close() — flush badger, stop goroutines

2. `pkg/cache/block.go` — Block storage:
   - BlockMeta struct stored as JSON in badger: BackendName, Path, BlockIndex, LocalPath, Size, ETag, CachedAt, LastAccess
   - FileMeta struct for ETag tracking: BackendName, Path, Size, ETag, LastValidated
   - blockKey(backendName, path, blockIndex) string — badger key format
   - blockLocalPath(cacheDir, backendName, path, blockIndex) string — SHA256 hash, 2-char subdirectory
   - fetchBlock(ctx, backend, path, blockIndex) — downloads one block via backend.ReadAt, writes to temp file, renames to final path (atomic), records in badger

3. `pkg/cache/eviction.go` — LRU eviction:
   - evictionLoop() goroutine: wakes on signal or every 30s
   - runEviction(): scan all BlockMeta from badger, sort by LastAccess asc, delete oldest until CurrentSize <= MaxSize * LowWater
   - Delete = remove NVMe file + remove badger entry + decrement CurrentSize

4. `pkg/cache/readahead.go` — Background prefetch:
   - ReadaheadManager with worker pool (4 workers)
   - TriggerReadahead(backendName, path, currentBlock): enqueue next ReadaheadBlocks blocks if not cached
   - Budget cap: maxInflightBytes (256MB default), skip if over budget
   - Non-blocking: if work channel full, drop the request

5. `pkg/cache/cache_test.go` — Tests:
   - Use a mock Backend that serves data from memory
   - TestRead_CacheMiss: read uncached block, verify fetch + cache write
   - TestRead_CacheHit: pre-populate, read again, verify no backend call
   - TestEviction: small cache, fill past high water, verify eviction
   - TestConcurrentSameBlock: 100 goroutines read same block, verify 1 fetch
   - TestETagInvalidation: change backend data, wait past StaleTTL, verify re-fetch
   - BenchmarkRead_CacheHit: target <0.5ms per 4MB block

## Key details
- Use github.com/dgraph-io/badger/v4 for metadata
- Blocks stored as files on NVMe: /cacheDir/blocks/XX/XXXXXXXXXXXX.blk
- Temp files during download: /cacheDir/tmp/XXXX.tmp then os.Rename (atomic on same filesystem)
- All goroutine-safe. CacheManager.Read called by 32+ concurrent FUSE workers.
- Use atomic operations for counters (sync/atomic)
- On startup, scan blocks/ directory to calculate CurrentSize (handle unclean shutdown)
```

---

## COPILOT PROMPT — M1.3: FUSE Filesystem

```
You are building the FUSE filesystem layer for WarpDrive, a cross-cloud data fabric.

Project: Go module at github.com/warpdrive/warpdrive
Package: pkg/fuse/
Dependencies: pkg/backend/, pkg/cache/, pkg/namespace/ (all already implemented)

## What to build

A read-only FUSE filesystem using github.com/hanwen/go-fuse/v2 that presents remote storage backends as a local directory tree. All reads go through the cache engine.

## Files to create

1. `pkg/namespace/namespace.go` — Namespace mapper:
   - Namespace struct holds backend configs with mount_path mappings
   - Resolve(path string) (backendName string, remotePath string, err error)
     * Input: "/training_sets/imagenet/shard-0000.tar"
     * Output: backendName="azure_training", remotePath="imagenet/shard-0000.tar"
   - ListMountPoints() []string — returns top-level directories

2. `pkg/fuse/fs.go` — Root node:
   - WarpDriveRoot struct embeds gofuse.Inode
   - Implements NodeLookuper: resolve child name to either a WarpDriveDir (if mount point or remote dir) or WarpDriveFile
   - Implements NodeReaddirer: at root, list mount points. In subdirs, list remote backend contents.
   - Directory listing is cached with configurable TTL (default 300s) using sync.Map

3. `pkg/fuse/dir.go` — Directory node:
   - WarpDriveDir struct with backendName, remotePath
   - Lookup: calls backend.Stat to check if child is file or dir
   - Readdir: calls backend.List, returns fuse.DirEntry slice

4. `pkg/fuse/file.go` — File node and handle:
   - WarpDriveFile struct with backendName, remotePath, size, etag
   - Getattr: returns size, mode 0444, mtime from backend
   - Open: returns WarpDriveFileHandle
   - WarpDriveFileHandle.Read: calls cache.Read(ctx, backendName, remotePath, dest, off)
   - Return fuse.ReadResultData(dest[:n])

5. `pkg/fuse/mount.go` — Mount helper:
   - Mount(ctx, MountConfig, root) error
   - MountConfig: MountPoint, AllowOther, FUSEWorkers(32)
   - Sets fuse.MountOptions: MaxReadAhead=1MB, DirectMount=true, Name="warpdrive"
   - Sets AttrTimeout and EntryTimeout to 60s
   - Blocks until unmount or ctx cancellation

6. `cmd/warpdrive-mount/main.go` — Main binary:
   - Loads YAML config from --config flag
   - Creates backend Registry with rclone backends
   - Creates CacheManager
   - Creates Namespace from config
   - Creates WarpDriveRoot, calls Mount
   - Handles SIGTERM/SIGINT for graceful unmount

## Key details
- Read-only in M1: Write operations return syscall.EROFS
- Use hanwen/go-fuse/v2 (NOT bazil.org/fuse — hanwen is faster, supports splice)
- File nodes: implement gofuse.NodeOpener, gofuse.NodeGetattrer
- Dir nodes: implement gofuse.NodeLookuper, gofuse.NodeReaddirer
- Handle ENOENT properly: if backend.Stat returns ErrNotFound, return syscall.ENOENT
- Root directory: Readdir returns one entry per configured backend mount_path
- Subdirectories: Readdir delegates to backend.List
- All metadata calls (Stat, List) should be cached in memory with TTL
- The FUSE max_read option should be set to 1MB (1048576) for large reads

## Testing
- Create a test that mounts a FUSE filesystem backed by a local directory via rclone "local" backend
- Verify ls, cat, head, reading large files, concurrent reads
- Test that write operations return EROFS
- Benchmark: 32 concurrent readers, report aggregate throughput
```

---

## M1 Definition of Done Checklist

- [ ] `go build ./cmd/warpdrive-mount/` succeeds
- [ ] Unit tests pass: `go test ./pkg/backend/ ./pkg/cache/ ./pkg/fuse/ ./pkg/namespace/`
- [ ] Can mount a local directory as FUSE filesystem and read files
- [ ] Can mount an Azure Blob container and read files (integration test)
- [ ] Cache stores blocks on NVMe, subsequent reads are cache hits
- [ ] Cache eviction runs when threshold exceeded
- [ ] Readahead prefetches next 4 blocks on sequential access
- [ ] 32 concurrent readers sustain >10GB/s aggregate from cache
- [ ] Single reader sustains >4GB/s from cache
- [ ] Mount stable under 24-hour continuous read load
- [ ] Prometheus metrics exposed at :9090/metrics
