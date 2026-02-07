package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"
)

// BlockMeta is stored as JSON in badger for each cached block.
type BlockMeta struct {
	BackendName string    `json:"b"`
	Path        string    `json:"p"`
	BlockIndex  int       `json:"i"`
	LocalPath   string    `json:"l"`
	Size        int       `json:"s"`
	ETag        string    `json:"e"`
	CachedAt    time.Time `json:"ca"`
	LastAccess  time.Time `json:"la"`
}

// FileMeta tracks remote file metadata for ETag-based cache validation.
type FileMeta struct {
	BackendName   string    `json:"b"`
	Path          string    `json:"p"`
	Size          int64     `json:"s"`
	ETag          string    `json:"e"`
	LastValidated time.Time `json:"lv"`
}

// blockKey returns the badger key for a cached block.
func blockKey(backendName, path string, blockIndex int) string {
	return fmt.Sprintf("block:%s:%s:%d", backendName, path, blockIndex)
}

// fileKey returns the badger key for file metadata.
func fileKey(backendName, path string) string {
	return fmt.Sprintf("file:%s:%s", backendName, path)
}

// blockLocalPath returns the NVMe file path for a cached block.
// Uses a 2-char prefix subdirectory for fan-out.
func blockLocalPath(cacheDir, backendName, path string, blockIndex int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", backendName, path, blockIndex)))
	hexStr := hex.EncodeToString(h[:10]) // 20 hex chars
	return filepath.Join(cacheDir, "blocks", hexStr[:2], hexStr+".blk")
}
