package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/warpdrive/warpdrive/pkg/backend"
)

// WarmConfig configures a cache warming operation.
type WarmConfig struct {
	BackendName string
	Prefix      string // Remote path prefix to warm
	Recursive   bool
	MaxSize     int64 // Stop after warming this many bytes (0 = unlimited)
	Workers     int   // Parallel download workers (default 32)
}

// WarmProgress reports warming progress.
type WarmProgress struct {
	FilesTotal   int64
	FilesWarmed  int64
	BytesTotal   int64
	BytesWarmed  int64
	BytesSkipped int64   // Already cached
	Throughput   float64 // Current MB/s
	ETA          time.Duration
}

// Warm pre-populates the cache for files under a prefix.
func (cm *CacheManager) Warm(ctx context.Context, cfg WarmConfig, progress func(WarmProgress)) error {
	be, err := cm.registry.Get(cfg.BackendName)
	if err != nil {
		return fmt.Errorf("cache.Warm: unknown backend %s: %w", cfg.BackendName, err)
	}

	workers := cfg.Workers
	if workers <= 0 {
		workers = 32
	}

	// 1. List all files under prefix (recursive if configured).
	files, err := cm.listAllFiles(ctx, be, cfg.Prefix, cfg.Recursive)
	if err != nil {
		return fmt.Errorf("cache.Warm: listing files: %w", err)
	}

	var totalBytes int64
	for _, f := range files {
		totalBytes += f.Size
	}

	// 2. Filter out already-cached files (ETag match).
	var toWarm []backend.ObjectInfo
	var skippedBytes int64
	for _, f := range files {
		if cm.isFullyCached(cfg.BackendName, f.Path, f.ETag) {
			skippedBytes += f.Size
		} else {
			toWarm = append(toWarm, f)
		}
	}

	slog.Info("cache warm starting",
		"total_files", len(files),
		"to_warm", len(toWarm),
		"skipped_cached", len(files)-len(toWarm),
		"total_bytes", totalBytes,
	)

	// 3. Download files through cache engine (which caches blocks).
	var warmedFiles, warmedBytes atomic.Int64
	warmedBytes.Store(skippedBytes) // Already cached counts as warmed
	startTime := time.Now()

	sem := make(chan struct{}, workers)
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
		go func(file backend.ObjectInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			if ctx.Err() != nil {
				return
			}

			// Read entire file through cache engine (block by block).
			buf := make([]byte, cm.blockSize)
			for off := int64(0); off < file.Size; off += int64(cm.blockSize) {
				if ctx.Err() != nil {
					return
				}

				n, readErr := cm.Read(ctx, cfg.BackendName, file.Path, buf, off)
				if readErr != nil {
					slog.Warn("Warm: read failed", "path", file.Path, "offset", off, "error", readErr)
					return
				}
				warmedBytes.Add(int64(n))
			}
			warmedFiles.Add(1)

			// Report progress.
			if progress != nil {
				elapsed := time.Since(startTime).Seconds()
				bytesWarmed := warmedBytes.Load()
				var throughput float64
				if elapsed > 0 {
					throughput = float64(bytesWarmed-skippedBytes) / elapsed / (1024 * 1024)
				}

				var eta time.Duration
				if throughput > 0 {
					remainingBytes := totalBytes - bytesWarmed
					eta = time.Duration(float64(remainingBytes)/(throughput*1024*1024)) * time.Second
				}

				progress(WarmProgress{
					FilesTotal:   int64(len(files)),
					FilesWarmed:  warmedFiles.Load(),
					BytesTotal:   totalBytes,
					BytesWarmed:  bytesWarmed,
					BytesSkipped: skippedBytes,
					Throughput:   throughput,
					ETA:          eta,
				})
			}
		}(f)
	}

	wg.Wait()

	slog.Info("cache warm completed",
		"files_warmed", warmedFiles.Load(),
		"bytes_warmed", warmedBytes.Load(),
		"bytes_skipped", skippedBytes,
		"elapsed", time.Since(startTime),
	)
	return nil
}

// listAllFiles lists all files under a prefix. If recursive, it traverses
// subdirectories. Directories are excluded from the result.
func (cm *CacheManager) listAllFiles(ctx context.Context, be backend.Backend, prefix string, recursive bool) ([]backend.ObjectInfo, error) {
	objects, err := be.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	var files []backend.ObjectInfo
	for _, obj := range objects {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if obj.IsDir {
			if recursive {
				subFiles, err := cm.listAllFiles(ctx, be, obj.Path, true)
				if err != nil {
					slog.Warn("Warm: list subdirectory failed", "path", obj.Path, "error", err)
					continue
				}
				files = append(files, subFiles...)
			}
			continue
		}
		files = append(files, obj)
	}
	return files, nil
}

// isFullyCached checks if all blocks of a file are cached with matching ETag.
func (cm *CacheManager) isFullyCached(backendName, path, etag string) bool {
	// We need to know the file size to determine the number of blocks.
	// Check file metadata cache.
	fk := fileKey(backendName, path)
	var fm FileMeta
	err := cm.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fk))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &fm)
		})
	})
	if err != nil {
		return false
	}

	// If both ETags are known, they must match.
	if etag != "" && fm.ETag != "" && fm.ETag != etag {
		return false
	}

	// Check all blocks.
	numBlocks := int((fm.Size + int64(cm.blockSize) - 1) / int64(cm.blockSize))
	for i := 0; i < numBlocks; i++ {
		key := blockKey(backendName, path, i)
		meta, err := cm.getBlockMeta(key)
		if err != nil {
			return false
		}
		// If both ETags are known, they must match.
		if etag != "" && meta.ETag != "" && meta.ETag != etag {
			return false
		}
	}

	return true
}
