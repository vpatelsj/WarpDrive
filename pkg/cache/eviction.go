package cache

import (
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/warpdrive/warpdrive/pkg/metrics"
)

// evictionLoop runs periodically to enforce the max cache size via LRU.
func (cm *CacheManager) evictionLoop(interval time.Duration) {
	defer cm.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
			cm.runEviction()
		}
	}
}

// blockWithAccess pairs block metadata with its effective access time.
type blockWithAccess struct {
	key        string
	meta       BlockMeta
	accessTime time.Time
}

// runEviction collects all cached blocks, sorts by LRU, and evicts the
// oldest until total size is below the low water mark.
func (cm *CacheManager) runEviction() {
	highWater := cm.cfg.EvictHighWater
	if highWater == 0 {
		highWater = 0.90
	}
	lowWater := cm.cfg.EvictLowWater
	if lowWater == 0 {
		lowWater = 0.80
	}

	threshold := int64(float64(cm.maxSize) * highWater)
	target := int64(float64(cm.maxSize) * lowWater)

	// Quick check: skip expensive scan if clearly under threshold.
	if cm.stats.BytesCached.Load() <= threshold {
		return
	}

	var blocks []blockWithAccess
	var totalSize int64

	// Scan all block metadata.
	err := cm.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("block:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			err := item.Value(func(val []byte) error {
				var m BlockMeta
				if err := json.Unmarshal(val, &m); err != nil {
					return nil // skip corrupt entries
				}
				// Use in-memory access time if more recent.
				accessTime := cm.getAccessTime(key, m.LastAccess)
				blocks = append(blocks, blockWithAccess{key: key, meta: m, accessTime: accessTime})
				totalSize += int64(m.Size)
				return nil
			})
			if err != nil {
				continue
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("eviction scan failed", "error", err)
		return
	}

	if totalSize <= threshold {
		// Correct the counter if it drifted.
		cm.stats.BytesCached.Store(totalSize)
		return
	}

	// Sort by last access time (oldest first) for LRU eviction.
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].accessTime.Before(blocks[j].accessTime)
	})

	evicted := 0
	for _, b := range blocks {
		if totalSize <= target {
			break
		}
		cm.deleteBlock(b.key, &b.meta)
		totalSize -= int64(b.meta.Size)
		evicted++
	}

	if evicted > 0 {
		metrics.CacheEvictions.Add(float64(evicted))
		metrics.CacheSize.Set(float64(totalSize))
		if cm.maxSize > 0 {
			metrics.CacheUtilization.Set(float64(totalSize) / float64(cm.maxSize))
		}
		slog.Info("eviction completed", "evicted", evicted, "remaining_bytes", totalSize)
	}
}
