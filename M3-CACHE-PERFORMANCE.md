# M3: Cache Performance — Technical Spec

## Timeline: Weeks 10-18 (overlaps M2 by 3 weeks)

## Goal

Cache layer is fast enough to sustain GPU training throughput. First epoch is tolerable. Second epoch is near wire-speed. Manual cache warming available.

---

## M3.1: High-Throughput Parallel FUSE (Weeks 10-13)

### Design

The default go-fuse configuration bottlenecks on metadata serialization and small FUSE reads. We optimize for ML training's access pattern: many workers, large sequential reads.

### Key Optimizations

```go
// pkg/fuse/mount.go — optimized mount options

func Mount(ctx context.Context, cfg MountConfig, root *WarpDriveRoot) error {
    opts := &gofuse.Options{
        MountOptions: fuse.MountOptions{
            AllowOther:      cfg.AllowOther,
            Name:            "warpdrive",
            FsName:          "warpdrive",
            MaxReadAhead:    1048576,   // 1MB — critical for throughput
            DirectMount:     true,
            DirectMountStrict: true,
            EnableLocks:     false,     // Not needed, reduces overhead
            ExplicitDataCacheControl: true, // We control caching
            MaxBackground:   64,        // Max background FUSE requests
            CongestionThreshold: 48,    // Threshold for congestion
            DisableReadDirPlus: false,  // Enable READDIRPLUS for fewer round-trips
        },
        AttrTimeout:  &sixtySeconds,
        EntryTimeout: &sixtySeconds,
        NegativeTimeout: &fiveSeconds,
    }
    // ...
}
```

### Read Coalescing

```go
// pkg/cache/coalesce.go

// ReadCoalescer merges adjacent small reads into larger backend requests.
// Tracks recent reads per file handle and detects sequential patterns.
type ReadCoalescer struct {
    // Track last N reads per (backend, path) pair
    history sync.Map // key -> *readHistory
}

type readHistory struct {
    mu      sync.Mutex
    reads   [8]readRecord // Circular buffer of last 8 reads
    idx     int
    pattern accessPattern // SEQUENTIAL, RANDOM, UNKNOWN
}

type readRecord struct {
    offset int64
    size   int
    time   time.Time
}

type accessPattern int
const (
    UNKNOWN    accessPattern = iota
    SEQUENTIAL               // Reads are advancing through file
    RANDOM                   // Reads are at non-sequential offsets
)

// Classify examines read history and returns the detected pattern.
func (h *readHistory) Classify() accessPattern {
    h.mu.Lock()
    defer h.mu.Unlock()
    
    // If last 4 reads are at monotonically increasing offsets
    // with gaps < 2 * blockSize: SEQUENTIAL
    // Otherwise: RANDOM
    return UNKNOWN
}
```

### Performance Target Validation

```go
// cmd/warpdrive-bench/main.go

// Benchmark tool that simulates DataLoader access patterns.
package main

import (
    "flag"
    "fmt"
    "os"
    "sync"
    "sync/atomic"
    "time"
)

func main() {
    dir := flag.String("dir", "/data/training_sets", "Directory to read from")
    readers := flag.Int("readers", 32, "Number of concurrent readers")
    duration := flag.Duration("duration", 30*time.Second, "Test duration")
    cached := flag.Bool("cached", false, "Expect all reads to be cache hits")
    chunkSize := flag.Int("chunk", 4*1024*1024, "Read chunk size")
    flag.Parse()
    
    // List all files in directory
    files, _ := os.ReadDir(*dir)
    
    var totalBytes atomic.Int64
    var totalOps atomic.Int64
    var latencies sync.Map // for percentile calculation
    
    start := time.Now()
    var wg sync.WaitGroup
    
    for i := 0; i < *readers; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            buf := make([]byte, *chunkSize)
            fileIdx := workerID % len(files)
            
            for time.Since(start) < *duration {
                // Open file, read sequentially in chunks
                f, err := os.Open(filepath.Join(*dir, files[fileIdx].Name()))
                if err != nil { continue }
                
                for {
                    opStart := time.Now()
                    n, err := f.Read(buf)
                    lat := time.Since(opStart)
                    
                    totalBytes.Add(int64(n))
                    totalOps.Add(1)
                    // Record latency...
                    
                    if err != nil { break }
                }
                f.Close()
                fileIdx = (fileIdx + 1) % len(files)
            }
        }(i)
    }
    
    wg.Wait()
    elapsed := time.Since(start)
    
    throughput := float64(totalBytes.Load()) / elapsed.Seconds() / 1e9
    fmt.Printf("Readers:    %d\n", *readers)
    fmt.Printf("Duration:   %s\n", elapsed)
    fmt.Printf("Throughput: %.2f GB/s\n", throughput)
    fmt.Printf("Operations: %d\n", totalOps.Load())
    fmt.Printf("Avg Latency: %.2f ms\n", /* calculate */)
    // Print p50, p95, p99 latencies
}
```

---

## M3.2: Intelligent Readahead (Weeks 12-15)

### Design: Per-File and Directory-Level Prefetch

```go
// pkg/cache/readahead.go — enhanced version

type ReadaheadManager struct {
    cm            *CacheManager
    maxInflight   int64 // Bytes (default 256MB)
    currentFlight atomic.Int64
    
    // Per-file pattern tracking
    patterns sync.Map // "backend:path" -> *filePattern
    
    // Directory-level tracking
    dirPatterns sync.Map // "backend:dirPath" -> *dirPattern
    
    // Worker pool
    workCh  chan readaheadJob
    workers int // default 8
    wg      sync.WaitGroup
}

// filePattern tracks access pattern for a single file.
type filePattern struct {
    mu            sync.Mutex
    lastBlocks    [8]int      // Last 8 block indices accessed
    count         int
    isSequential  bool
    readaheadSize int         // Dynamic: starts at 4 blocks, grows to 8 for sequential
}

// dirPattern tracks which files in a directory are being read.
type dirPattern struct {
    mu        sync.Mutex
    fileOrder []string  // Files read in order
    lastFile  string
    isSequential bool   // Files being read in sorted name order
}

// TriggerReadahead is called after each cache read.
func (ra *ReadaheadManager) TriggerReadahead(backendName, path string, currentBlock int) {
    // 1. Update per-file pattern
    fp := ra.getOrCreateFilePattern(backendName, path)
    fp.mu.Lock()
    fp.recordAccess(currentBlock)
    isSeq := fp.isSequential
    ahead := fp.readaheadSize
    fp.mu.Unlock()
    
    // 2. If sequential file access, prefetch next N blocks
    if isSeq {
        for i := 1; i <= ahead; i++ {
            ra.enqueue(readaheadJob{
                backendName: backendName,
                path:        path,
                blockIndex:  currentBlock + i,
                priority:    priorityFile, // Higher than dir-level
            })
        }
    }
    
    // 3. Update directory-level pattern
    dirPath := filepath.Dir(path)
    dp := ra.getOrCreateDirPattern(backendName, dirPath)
    dp.mu.Lock()
    dp.recordFileAccess(filepath.Base(path))
    isDirSeq := dp.isSequential
    nextFiles := dp.predictNextFiles(4)
    dp.mu.Unlock()
    
    // 4. If files in directory being read sequentially, prefetch next files' first blocks
    if isDirSeq {
        for _, nextFile := range nextFiles {
            fullPath := filepath.Join(dirPath, nextFile)
            // Prefetch first 4 blocks of next file
            for b := 0; b < 4; b++ {
                ra.enqueue(readaheadJob{
                    backendName: backendName,
                    path:        fullPath,
                    blockIndex:  b,
                    priority:    priorityDir, // Lower than in-file prefetch
                })
            }
        }
    }
}

// recordAccess updates the file's access pattern.
func (fp *filePattern) recordAccess(block int) {
    fp.lastBlocks[fp.count%8] = block
    fp.count++
    
    if fp.count >= 4 {
        // Check if last 4 blocks are monotonically increasing with step ~1
        sequential := true
        for i := 1; i < min(fp.count, 4); i++ {
            curr := fp.lastBlocks[(fp.count-i)%8]
            prev := fp.lastBlocks[(fp.count-i-1)%8]
            if curr <= prev || curr-prev > 2 {
                sequential = false
                break
            }
        }
        fp.isSequential = sequential
        
        // Dynamic readahead sizing
        if sequential {
            if fp.readaheadSize < 8 {
                fp.readaheadSize++
            }
        } else {
            fp.readaheadSize = 0 // Disable readahead for random access
        }
    }
}
```

---

## M3.3: Cache Warming CLI (Weeks 13-16)

### Implementation: `pkg/cache/warmup.go`

```go
package cache

import (
    "context"
    "fmt"
    "sync"
    "sync/atomic"
)

// WarmConfig configures a cache warming operation.
type WarmConfig struct {
    BackendName string
    Prefix      string // Remote path prefix to warm
    Recursive   bool
    MaxSize     int64  // Stop after warming this many bytes (0 = unlimited)
    Workers     int    // Parallel download workers (default 32)
}

// WarmProgress reports warming progress.
type WarmProgress struct {
    FilesTotal    int64
    FilesWarmed   int64
    BytesTotal    int64
    BytesWarmed   int64
    BytesSkipped  int64 // Already cached
    Throughput    float64 // Current MB/s
    ETA           time.Duration
}

// Warm pre-populates the cache for files under a prefix.
func (cm *CacheManager) Warm(ctx context.Context, cfg WarmConfig, progress func(WarmProgress)) error {
    b, ok := cm.backends.Get(cfg.BackendName)
    if !ok {
        return fmt.Errorf("cache.Warm: unknown backend %s", cfg.BackendName)
    }
    
    // 1. List all files under prefix (recursive if configured)
    files, err := cm.listAllFiles(ctx, b, cfg.Prefix, cfg.Recursive)
    if err != nil {
        return err
    }
    
    var totalBytes int64
    for _, f := range files {
        totalBytes += f.Size
    }
    
    // 2. Filter out already-cached files (ETag match)
    var toWarm []ObjectInfo
    var skippedBytes int64
    for _, f := range files {
        if cm.isFullyCached(cfg.BackendName, f.Path, f.ETag) {
            skippedBytes += f.Size
        } else {
            toWarm = append(toWarm, f)
        }
    }
    
    // 3. Download files through cache engine (which caches blocks)
    var warmedFiles, warmedBytes atomic.Int64
    warmedBytes.Store(skippedBytes) // Already cached counts as warmed
    
    sem := make(chan struct{}, cfg.Workers)
    var wg sync.WaitGroup
    
    for _, f := range toWarm {
        if ctx.Err() != nil {
            break
        }
        if cfg.MaxSize > 0 && warmedBytes.Load() >= cfg.MaxSize {
            break
        }
        
        wg.Add(1)
        sem <- struct{}{}
        go func(file ObjectInfo) {
            defer wg.Done()
            defer func() { <-sem }()
            
            // Read entire file through cache engine (block by block)
            buf := make([]byte, cm.cfg.BlockSize)
            for off := int64(0); off < file.Size; off += int64(cm.cfg.BlockSize) {
                n, err := cm.Read(ctx, cfg.BackendName, file.Path, buf, off)
                if err != nil {
                    slog.Warn("Warm: read failed", "path", file.Path, "offset", off, "error", err)
                    return
                }
                warmedBytes.Add(int64(n))
            }
            warmedFiles.Add(1)
            
            // Report progress
            if progress != nil {
                progress(WarmProgress{
                    FilesTotal:   int64(len(files)),
                    FilesWarmed:  warmedFiles.Load(),
                    BytesTotal:   totalBytes,
                    BytesWarmed:  warmedBytes.Load(),
                    BytesSkipped: skippedBytes,
                })
            }
        }(f)
    }
    
    wg.Wait()
    return nil
}
```

### CLI: `cmd/warpdrive-ctl/warm.go`

```go
// Subcommand: warpdrive-ctl warm <path> [flags]

func warmCmd() *cobra.Command {
    var recursive bool
    var maxSize string
    var workers int
    
    cmd := &cobra.Command{
        Use:   "warm <path>",
        Short: "Pre-populate cache for a path",
        Args:  cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            path := args[0]
            
            // Resolve path to backend + remote path via namespace
            backendName, remotePath, err := ns.Resolve(path)
            
            // Parse maxSize
            maxBytes := parseSize(maxSize) // "500GB" -> int64
            
            cfg := cache.WarmConfig{
                BackendName: backendName,
                Prefix:      remotePath,
                Recursive:   recursive,
                MaxSize:     maxBytes,
                Workers:     workers,
            }
            
            // Progress bar
            bar := progressbar.New(0)
            return cacheMgr.Warm(ctx, cfg, func(p cache.WarmProgress) {
                bar.SetTotal(p.BytesTotal)
                bar.Set(int(p.BytesWarmed))
                fmt.Printf("\r%d/%d files | %s/%s | %.1f MB/s",
                    p.FilesWarmed, p.FilesTotal,
                    humanize.Bytes(uint64(p.BytesWarmed)),
                    humanize.Bytes(uint64(p.BytesTotal)),
                    p.Throughput)
            })
        },
    }
    
    cmd.Flags().BoolVarP(&recursive, "recursive", "r", true, "Warm recursively")
    cmd.Flags().StringVar(&maxSize, "max-size", "0", "Max bytes to warm (e.g., 500GB)")
    cmd.Flags().IntVar(&workers, "workers", 32, "Parallel download workers")
    return cmd
}
```

### K8s Init Container Example

```yaml
# deploy/examples/warm-init-container.yaml
initContainers:
  - name: warpdrive-warm
    image: warpdrive/warpdrive-ctl:latest
    command:
      - warpdrive-ctl
      - warm
      - /data/training_sets/imagenet
      - --recursive
      - --max-size=500GB
      - --workers=32
    volumeMounts:
      - name: warpdrive-data
        mountPath: /data
      - name: warpdrive-config
        mountPath: /etc/warpdrive
```

### Slurm Prolog Script

```bash
#!/bin/bash
# deploy/slurm/warpdrive-prolog.sh
# Slurm prolog script — runs before each job

WARMUP_PATH="${WARPDRIVE_WARMUP_PATH:-}"
if [ -n "$WARMUP_PATH" ]; then
    echo "WarpDrive: warming cache for $WARMUP_PATH"
    warpdrive-ctl warm "$WARMUP_PATH" --recursive --workers 32
    echo "WarpDrive: cache warm complete"
fi
```

---

## COPILOT PROMPT — M3: Cache Performance

```
You are optimizing the cache and FUSE layers for WarpDrive to achieve high throughput for ML training.

Project: Go module at github.com/warpdrive/warpdrive
Packages: pkg/cache/, pkg/fuse/, cmd/warpdrive-bench/, cmd/warpdrive-ctl/

## What to build

### 1. Optimize FUSE mount options in pkg/fuse/mount.go:
- Set MaxReadAhead to 1048576 (1MB)
- Set MaxBackground to 64, CongestionThreshold to 48
- Enable DisableReadDirPlus=false for faster directory reads
- Set EnableLocks=false (not needed)
- AttrTimeout and EntryTimeout = 60s
- NegativeTimeout = 5s

### 2. Enhance readahead in pkg/cache/readahead.go:
- Per-file access pattern tracking: track last 8 block indices per file
- Classify as SEQUENTIAL (monotonically increasing, step ~1) or RANDOM
- Sequential: readahead 4 blocks, grow to 8 if pattern persists
- Random: disable readahead for that file handle
- Directory-level readahead: if files being read in sorted name order (shard-0000, shard-0001...), prefetch first 4 blocks of next 4 files
- Budget cap: 256MB total inflight prefetch. Skip if over budget.
- 8 worker goroutines, priority queue (in-file > dir-level)

### 3. Cache warming in pkg/cache/warmup.go:
- WarmConfig: BackendName, Prefix, Recursive, MaxSize, Workers
- WarmProgress: FilesTotal/Warmed, BytesTotal/Warmed/Skipped, Throughput, ETA
- Warm(ctx, cfg, progressCallback): list all files, skip already-cached (ETag match), download remainder through cache engine in parallel
- Support cancellation via context

### 4. CLI warming command in cmd/warpdrive-ctl/warm.go:
- `warpdrive-ctl warm <path> [--recursive] [--max-size 500GB] [--workers 32]`
- Progress bar with files warmed, bytes, throughput
- Resume support: re-running skips already-cached files

### 5. Benchmark tool in cmd/warpdrive-bench/main.go:
- `warpdrive-bench --dir <path> --readers <N> --duration <dur> --chunk <size>`
- Launch N concurrent goroutines reading files sequentially
- Report: aggregate throughput (GB/s), ops/s, latency p50/p95/p99
- Verify data integrity (optional --checksum flag)

## Performance targets
- Cache hit, 1 reader: >4 GB/s sequential
- Cache hit, 32 readers: >10 GB/s aggregate
- Cache hit, random 4MB: >5000 IOPS
- First epoch with readahead: >80% of cache-hit speed for sequential shards
- Warm 1TB same-region: <15 minutes
- No memory leaks under 48-hour stress test

## Key constraints
- All goroutine-safe
- Readahead must not starve active reads (separate goroutine pool, lower priority)
- Cache warming is interruptible — handle SIGINT/SIGTERM gracefully
- Use atomic operations for all counters
```

---

## M3 Exit Criteria

- [ ] warpdrive-bench shows >10GB/s aggregate with 32 readers from cache
- [ ] warpdrive-bench shows >4GB/s with 1 reader from cache
- [ ] First-epoch throughput within 80% of cache-hit for sequential shard access
- [ ] `warpdrive-ctl warm` completes 1TB in <15 minutes same-region
- [ ] `warpdrive-ctl warm --resume` skips already-cached files
- [ ] 48-hour stress test: no crashes, no memory leak, no OOM
- [ ] Readahead correctly detects sequential vs random access patterns
