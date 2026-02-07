package cache

import (
	"context"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	// Default max inflight readahead bytes (256MB).
	defaultMaxInflight = 256 * 1024 * 1024

	// Priority levels for readahead jobs.
	priorityFile = 1 // Higher priority: in-file prefetch
	priorityDir  = 2 // Lower priority: directory-level prefetch
)

// ReadaheadManager prefetches blocks based on detected access patterns.
// It supports per-file sequential detection and directory-level prefetch
// when files are being read in sorted name order.
type ReadaheadManager struct {
	cm      *CacheManager
	workers int

	maxInflight   int64 // Bytes (default 256MB)
	currentFlight atomic.Int64

	// Per-file pattern tracking.
	patterns sync.Map // "backend:path" -> *filePattern

	// Directory-level tracking.
	dirPatterns sync.Map // "backend:dirPath" -> *dirPattern

	// Job queues: high priority (in-file) and low priority (dir-level).
	highJobs chan readaheadJob
	lowJobs  chan readaheadJob
	wg       sync.WaitGroup

	// Track which blocks are already queued to avoid duplicates.
	queued sync.Map // map[string]struct{}

	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
}

type readaheadJob struct {
	backendName string
	path        string
	blockIdx    int
	etag        string
	priority    int
}

// filePattern tracks access pattern for a single file.
type filePattern struct {
	mu            sync.Mutex
	lastBlocks    [8]int // Last 8 block indices accessed
	count         int
	isSequential  bool
	readaheadSize int // Dynamic: starts at 4, grows to 8 for sequential
}

// dirPattern tracks which files in a directory are being read.
type dirPattern struct {
	mu           sync.Mutex
	fileOrder    []string // Files read in order
	lastFile     string
	isSequential bool     // Files being read in sorted name order
	allFiles     []string // All known files in the directory (sorted)
}

// NewReadaheadManager creates a ReadaheadManager with the specified worker count.
func NewReadaheadManager(cm *CacheManager, workers int) *ReadaheadManager {
	if workers <= 0 {
		workers = 8
	}
	ctx, cancel := context.WithCancel(cm.ctx)
	ra := &ReadaheadManager{
		cm:          cm,
		workers:     workers,
		maxInflight: defaultMaxInflight,
		highJobs:    make(chan readaheadJob, 128),
		lowJobs:     make(chan readaheadJob, 128),
		ctx:         ctx,
		cancel:      cancel,
	}
	for i := 0; i < workers; i++ {
		ra.wg.Add(1)
		go ra.worker()
	}
	return ra
}

// TriggerReadahead is called after each cache read.
// It detects per-file and directory-level sequential patterns and enqueues
// appropriate prefetch jobs.
func (ra *ReadaheadManager) TriggerReadahead(backendName, path string, currentBlock int, etag string, count int) {
	// 1. Update per-file pattern.
	fp := ra.getOrCreateFilePattern(backendName, path)
	fp.mu.Lock()
	fp.recordAccess(currentBlock)
	isSeq := fp.isSequential
	ahead := fp.readaheadSize
	fp.mu.Unlock()

	// 2. If sequential file access, prefetch next N blocks.
	if isSeq && ahead > 0 {
		for i := 1; i <= ahead; i++ {
			ra.enqueue(readaheadJob{
				backendName: backendName,
				path:        path,
				blockIdx:    currentBlock + i,
				etag:        etag,
				priority:    priorityFile,
			})
		}
	} else if count > 0 {
		// Fallback to simple linear readahead if pattern not yet detected.
		for i := 0; i < count; i++ {
			ra.enqueue(readaheadJob{
				backendName: backendName,
				path:        path,
				blockIdx:    currentBlock + i,
				etag:        etag,
				priority:    priorityFile,
			})
		}
	}

	// 3. Update directory-level pattern.
	dirPath := filepath.Dir(path)
	dp := ra.getOrCreateDirPattern(backendName, dirPath)
	dp.mu.Lock()
	dp.recordFileAccess(filepath.Base(path))
	isDirSeq := dp.isSequential
	nextFiles := dp.predictNextFiles(4)
	dp.mu.Unlock()

	// 4. If files in directory being read sequentially, prefetch next files' first blocks.
	if isDirSeq {
		for _, nextFile := range nextFiles {
			fullPath := filepath.Join(dirPath, nextFile)
			// Prefetch first 4 blocks of next file.
			for b := 0; b < 4; b++ {
				ra.enqueue(readaheadJob{
					backendName: backendName,
					path:        fullPath,
					blockIdx:    b,
					etag:        "", // Unknown etag for prefetch
					priority:    priorityDir,
				})
			}
		}
	}
}

// enqueue adds a readahead job, respecting budget limits.
func (ra *ReadaheadManager) enqueue(job readaheadJob) {
	key := blockKey(job.backendName, job.path, job.blockIdx)

	// Check budget.
	if ra.currentFlight.Load() >= ra.maxInflight {
		return
	}

	// Skip if already queued or in cache.
	if _, loaded := ra.queued.LoadOrStore(key, struct{}{}); loaded {
		return
	}

	// Route to appropriate queue based on priority.
	ch := ra.highJobs
	if job.priority == priorityDir {
		ch = ra.lowJobs
	}

	select {
	case ch <- job:
	default:
		// Queue full — drop prefetch, not critical.
		ra.queued.Delete(key)
	}
}

// worker processes readahead jobs, preferring high-priority jobs.
func (ra *ReadaheadManager) worker() {
	defer ra.wg.Done()
	for {
		var job readaheadJob
		var ok bool

		// Prefer high-priority jobs, fall back to low-priority.
		select {
		case job, ok = <-ra.highJobs:
			if !ok {
				return
			}
		default:
			select {
			case job, ok = <-ra.highJobs:
				if !ok {
					return
				}
			case job, ok = <-ra.lowJobs:
				if !ok {
					return
				}
			case <-ra.ctx.Done():
				return
			}
		}

		key := blockKey(job.backendName, job.path, job.blockIdx)

		// Skip if already cached.
		if _, err := ra.cm.getBlockMeta(key); err == nil {
			ra.queued.Delete(key)
			continue
		}

		// Check budget before fetching.
		blockSize := int64(ra.cm.blockSize)
		if ra.currentFlight.Add(blockSize) > ra.maxInflight+blockSize {
			ra.currentFlight.Add(-blockSize)
			ra.queued.Delete(key)
			continue
		}

		be, err := ra.cm.registry.Get(job.backendName)
		if err != nil {
			ra.currentFlight.Add(-blockSize)
			ra.queued.Delete(key)
			continue
		}

		// Fetch and cache the block.
		_, fetchErr := ra.cm.fetchBlock(ra.ctx, be, job.backendName, job.path, job.blockIdx, job.etag)
		if fetchErr != nil {
			slog.Debug("readahead fetch failed", "path", job.path, "block", job.blockIdx, "err", fetchErr)
		}

		ra.currentFlight.Add(-blockSize)
		ra.queued.Delete(key)
	}
}

// getOrCreateFilePattern retrieves or creates a filePattern for a (backend, path) pair.
func (ra *ReadaheadManager) getOrCreateFilePattern(backendName, path string) *filePattern {
	key := backendName + ":" + path
	val, _ := ra.patterns.LoadOrStore(key, &filePattern{
		readaheadSize: 4,
	})
	return val.(*filePattern)
}

// getOrCreateDirPattern retrieves or creates a dirPattern for a (backend, dirPath) pair.
func (ra *ReadaheadManager) getOrCreateDirPattern(backendName, dirPath string) *dirPattern {
	key := backendName + ":" + dirPath
	val, _ := ra.dirPatterns.LoadOrStore(key, &dirPattern{})
	return val.(*dirPattern)
}

// recordAccess updates the file's access pattern.
func (fp *filePattern) recordAccess(block int) {
	fp.lastBlocks[fp.count%8] = block
	fp.count++

	if fp.count >= 4 {
		// Check if last 4 blocks are monotonically increasing with step ~1.
		sequential := true
		checkCount := min(fp.count, 4)
		for i := 1; i < checkCount; i++ {
			curr := fp.lastBlocks[(fp.count-i)%8]
			prev := fp.lastBlocks[(fp.count-i-1)%8]
			if curr <= prev || curr-prev > 2 {
				sequential = false
				break
			}
		}
		fp.isSequential = sequential

		// Dynamic readahead sizing.
		if sequential {
			if fp.readaheadSize < 8 {
				fp.readaheadSize++
			}
		} else {
			fp.readaheadSize = 0 // Disable readahead for random access
		}
	}
}

// recordFileAccess records that a file in this directory was accessed.
func (dp *dirPattern) recordFileAccess(filename string) {
	if dp.lastFile == filename {
		return // Same file, no update needed
	}
	dp.lastFile = filename

	// Check for duplicate in recent order.
	for _, f := range dp.fileOrder {
		if f == filename {
			return
		}
	}
	dp.fileOrder = append(dp.fileOrder, filename)

	// Check if file accesses are in sorted name order.
	if len(dp.fileOrder) >= 3 {
		dp.isSequential = sort.StringsAreSorted(dp.fileOrder)
	}
}

// predictNextFiles predicts the next N files that will be accessed.
// For sequential access patterns (shard-0000, shard-0001, ...) it returns
// the next files in sorted order after the last accessed file.
func (dp *dirPattern) predictNextFiles(count int) []string {
	if !dp.isSequential || len(dp.allFiles) == 0 || dp.lastFile == "" {
		return nil
	}

	// Find the position of the last file in the sorted list.
	idx := sort.SearchStrings(dp.allFiles, dp.lastFile)
	if idx >= len(dp.allFiles) || dp.allFiles[idx] != dp.lastFile {
		return nil
	}

	// Return the next 'count' files.
	start := idx + 1
	end := start + count
	if end > len(dp.allFiles) {
		end = len(dp.allFiles)
	}
	if start >= end {
		return nil
	}

	return dp.allFiles[start:end]
}

// SetDirFiles sets the known files for a directory (used after listing).
func (ra *ReadaheadManager) SetDirFiles(backendName, dirPath string, files []string) {
	dp := ra.getOrCreateDirPattern(backendName, dirPath)
	dp.mu.Lock()
	defer dp.mu.Unlock()
	sorted := make([]string, len(files))
	copy(sorted, files)
	sort.Strings(sorted)
	dp.allFiles = sorted
}

// Stop shuts down all readahead workers. Safe to call multiple times.
func (ra *ReadaheadManager) Stop() {
	ra.stopOnce.Do(func() {
		ra.cancel()
		close(ra.highJobs)
		close(ra.lowJobs)
		ra.wg.Wait()
	})
}
