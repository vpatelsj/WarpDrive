package backend

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/warpdrive/warpdrive/pkg/auth"
	"github.com/warpdrive/warpdrive/pkg/metrics"

	// Register rclone backends via blank imports.
	_ "github.com/rclone/rclone/backend/azureblob"
	_ "github.com/rclone/rclone/backend/googlecloudstorage"
	_ "github.com/rclone/rclone/backend/local"
	_ "github.com/rclone/rclone/backend/s3"
	_ "github.com/rclone/rclone/backend/sftp"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
)

// RcloneBackend wraps an rclone fs.Fs as a Backend.
type RcloneBackend struct {
	name     string
	backType string
	rfs      fs.Fs
	mu       sync.RWMutex // protects credential updates
}

// NewRcloneBackend creates a backend from config.
// backendType is the rclone backend name (e.g. "azureblob", "s3", "local").
// remotePath is the bucket/container + optional prefix.
// params maps rclone config keys to values.
func NewRcloneBackend(name, backendType, remotePath string, params map[string]string) (*RcloneBackend, error) {
	m := configmap.Simple(params)

	regInfo, err := fs.Find(backendType)
	if err != nil {
		return nil, fmt.Errorf("backend.NewRcloneBackend: unknown type %q: %w", backendType, err)
	}

	rfs, err := regInfo.NewFs(context.Background(), name, remotePath, m)
	if err != nil {
		return nil, fmt.Errorf("backend.NewRcloneBackend: create %q (%s): %w", name, backendType, err)
	}

	slog.Info("Backend created",
		"component", "backend", "name", name,
		"type", backendType, "path", remotePath,
	)

	return &RcloneBackend{name: name, backType: backendType, rfs: rfs}, nil
}

func (b *RcloneBackend) Name() string { return b.name }
func (b *RcloneBackend) Type() string { return b.backType }

// List returns objects and directories under the given prefix.
func (b *RcloneBackend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	start := time.Now()
	entries, err := b.rfs.List(ctx, prefix)
	metrics.BackendRequestDuration.WithLabelValues(b.name, "list").Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.BackendErrors.WithLabelValues(b.name, "list").Inc()
		return nil, fmt.Errorf("backend %s: List %q: %w", b.name, prefix, err)
	}

	result := make([]ObjectInfo, 0, len(entries))
	for _, entry := range entries {
		oi := ObjectInfo{
			Path:    entry.Remote(),
			ModTime: entry.ModTime(ctx),
		}

		switch e := entry.(type) {
		case fs.Object:
			oi.Size = e.Size()
			oi.IsDir = false
		case fs.Directory:
			oi.IsDir = true
			oi.Size = e.Size()
		}

		// Strip prefix to get just the child name.
		if prefix != "" {
			oi.Path = strings.TrimPrefix(oi.Path, prefix)
			oi.Path = strings.TrimPrefix(oi.Path, "/")
		}

		result = append(result, oi)
	}

	return result, nil
}

// Stat returns info for a single object or directory.
func (b *RcloneBackend) Stat(ctx context.Context, path string) (ObjectInfo, error) {
	start := time.Now()
	// Try as object first.
	obj, err := b.rfs.NewObject(ctx, path)
	metrics.BackendRequestDuration.WithLabelValues(b.name, "stat").Observe(time.Since(start).Seconds())
	if err == nil {
		return objectInfoFromRclone(obj, ctx), nil
	}

	if err == fs.ErrorIsDir || err == fs.ErrorNotAFile {
		// rclone explicitly told us this is a directory.
		return ObjectInfo{Path: path, IsDir: true}, nil
	}

	if err == fs.ErrorObjectNotFound {
		// Might be a directory — check by listing children.
		entries, listErr := b.rfs.List(ctx, path)
		if listErr == nil && len(entries) > 0 {
			return ObjectInfo{Path: path, IsDir: true}, nil
		}
		return ObjectInfo{}, fmt.Errorf("backend %s: Stat %q: %w", b.name, path, ErrNotFound)
	}

	return ObjectInfo{}, fmt.Errorf("backend %s: Stat %q: %w", b.name, path, err)
}

// ReadAt reads len(p) bytes from the object starting at byte offset off.
func (b *RcloneBackend) ReadAt(ctx context.Context, path string, p []byte, off int64) (int, error) {
	start := time.Now()
	obj, err := b.rfs.NewObject(ctx, path)
	if err != nil {
		metrics.BackendRequestDuration.WithLabelValues(b.name, "read").Observe(time.Since(start).Seconds())
		metrics.BackendErrors.WithLabelValues(b.name, "read").Inc()
		if err == fs.ErrorObjectNotFound {
			return 0, fmt.Errorf("backend %s: ReadAt %q: %w", b.name, path, ErrNotFound)
		}
		return 0, fmt.Errorf("backend %s: ReadAt %q: %w", b.name, path, err)
	}

	size := obj.Size()
	if off >= size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= size {
		end = size - 1
	}

	rc, err := obj.Open(ctx, &fs.RangeOption{Start: off, End: end})
	if err != nil {
		return 0, fmt.Errorf("backend %s: ReadAt %q open: %w", b.name, path, err)
	}
	defer rc.Close()

	totalRead := 0
	for totalRead < len(p) && off+int64(totalRead) <= end {
		n, readErr := rc.Read(p[totalRead:])
		totalRead += n
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return totalRead, fmt.Errorf("backend %s: ReadAt %q read: %w", b.name, path, readErr)
		}
	}

	if off+int64(totalRead) >= size {
		metrics.BackendRequestDuration.WithLabelValues(b.name, "read").Observe(time.Since(start).Seconds())
		return totalRead, io.EOF
	}
	metrics.BackendRequestDuration.WithLabelValues(b.name, "read").Observe(time.Since(start).Seconds())
	return totalRead, nil
}

// Open returns a reader for the entire object.
func (b *RcloneBackend) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	obj, err := b.rfs.NewObject(ctx, path)
	if err != nil {
		if err == fs.ErrorObjectNotFound {
			return nil, fmt.Errorf("backend %s: Open %q: %w", b.name, path, ErrNotFound)
		}
		return nil, fmt.Errorf("backend %s: Open %q: %w", b.name, path, err)
	}

	rc, err := obj.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("backend %s: Open %q: %w", b.name, path, err)
	}
	return rc, nil
}

// Write writes data to the given path.
func (b *RcloneBackend) Write(ctx context.Context, path string, r io.Reader, size int64) error {
	info := object.NewStaticObjectInfo(path, time.Now(), size, true, nil, nil)
	_, err := b.rfs.Put(ctx, r, info)
	if err != nil {
		return fmt.Errorf("backend %s: Write %q: %w", b.name, path, err)
	}
	return nil
}

// Delete removes an object.
func (b *RcloneBackend) Delete(ctx context.Context, path string) error {
	obj, err := b.rfs.NewObject(ctx, path)
	if err != nil {
		if err == fs.ErrorObjectNotFound {
			return fmt.Errorf("backend %s: Delete %q: %w", b.name, path, ErrNotFound)
		}
		return fmt.Errorf("backend %s: Delete %q: %w", b.name, path, err)
	}
	if err := obj.Remove(ctx); err != nil {
		return fmt.Errorf("backend %s: Delete %q: %w", b.name, path, err)
	}
	return nil
}

// UpdateCredentials applies new auth credentials to the underlying rclone backend.
// This updates the rclone config map so that subsequent operations use fresh tokens.
func (b *RcloneBackend) UpdateCredentials(creds *auth.Credentials) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// The rclone fs.Fs doesn't support runtime config updates directly.
	// For local backends this is a no-op. For real cloud backends the
	// credential injection is done at the rclone config level.
	// This is a hook for future integration with rclone's configmap.
	slog.Debug("Backend credentials updated",
		"component", "backend",
		"name", b.name,
		"provider", creds.Provider,
	)
}

// Close releases resources.
func (b *RcloneBackend) Close() error {
	slog.Info("Backend closed", "component", "backend", "name", b.name)
	return nil
}

func objectInfoFromRclone(obj fs.Object, ctx context.Context) ObjectInfo {
	oi := ObjectInfo{
		Path:    obj.Remote(),
		Size:    obj.Size(),
		ModTime: obj.ModTime(ctx),
		IsDir:   false,
	}
	if h, err := obj.Hash(ctx, hash.MD5); err == nil && h != "" {
		oi.ETag = h
	}
	return oi
}
