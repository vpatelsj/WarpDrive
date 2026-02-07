package cache

import (
	"sync"
	"time"
)

// accessPattern classifies how a file is being read.
type accessPattern int

const (
	patternUnknown    accessPattern = iota
	patternSequential               // Reads are advancing through the file
	patternRandom                   // Reads are at non-sequential offsets
)

// readRecord stores a single read event for pattern detection.
type readRecord struct {
	offset int64
	size   int
	time   time.Time
}

// readHistory tracks recent reads for a single (backend, path) pair.
type readHistory struct {
	mu        sync.Mutex
	reads     [8]readRecord // Circular buffer of last 8 reads
	idx       int
	count     int
	pattern   accessPattern
	blockSize int // Configured block size for gap threshold
}

// ReadCoalescer merges adjacent small reads into larger backend requests.
// Tracks recent reads per file handle and detects sequential patterns.
type ReadCoalescer struct {
	history   sync.Map // key -> *readHistory
	blockSize int
}

// NewReadCoalescer creates a new ReadCoalescer.
func NewReadCoalescer(blockSize int) *ReadCoalescer {
	return &ReadCoalescer{
		blockSize: blockSize,
	}
}

// RecordRead records a read event and returns the detected access pattern.
func (rc *ReadCoalescer) RecordRead(backendName, path string, offset int64, size int) accessPattern {
	key := backendName + ":" + path
	val, _ := rc.history.LoadOrStore(key, &readHistory{blockSize: rc.blockSize})
	h := val.(*readHistory)

	h.mu.Lock()
	defer h.mu.Unlock()

	h.reads[h.idx%8] = readRecord{
		offset: offset,
		size:   size,
		time:   time.Now(),
	}
	h.idx++
	h.count++
	h.pattern = h.classify()
	return h.pattern
}

// GetPattern returns the current access pattern for a (backend, path) pair.
func (rc *ReadCoalescer) GetPattern(backendName, path string) accessPattern {
	key := backendName + ":" + path
	val, ok := rc.history.Load(key)
	if !ok {
		return patternUnknown
	}
	h := val.(*readHistory)
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pattern
}

// classify examines read history and returns the detected pattern.
// If last 4 reads are at monotonically increasing offsets with gaps
// less than 2 * blockSize, classify as SEQUENTIAL. Otherwise RANDOM.
func (h *readHistory) classify() accessPattern {
	n := h.count
	if n < 4 {
		return patternUnknown
	}

	// Collect the last 4 reads in chronological order.
	checkCount := 4
	recent := make([]readRecord, checkCount)
	for i := 0; i < checkCount; i++ {
		// h.idx points to the next write slot, so h.idx-1 is the most recent.
		pos := ((h.idx-checkCount+i)%8 + 8) % 8
		recent[i] = h.reads[pos]
	}

	for i := 1; i < checkCount; i++ {
		curr := recent[i]
		prev := recent[i-1]

		// Current offset should be ahead of previous.
		if curr.offset <= prev.offset {
			return patternRandom
		}

		// Gap between reads should be small (within 2 * blockSize).
		gap := curr.offset - (prev.offset + int64(prev.size))
		if gap < 0 {
			gap = 0 // Overlapping reads are OK
		}
		threshold := int64(2 * h.blockSize)
		if threshold <= 0 {
			threshold = int64(2 * DefaultBlockSize)
		}
		if gap > threshold {
			return patternRandom
		}
	}

	return patternSequential
}

// Forget removes tracking data for a (backend, path) pair.
func (rc *ReadCoalescer) Forget(backendName, path string) {
	key := backendName + ":" + path
	rc.history.Delete(key)
}
