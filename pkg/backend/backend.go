package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// ErrNotFound is returned when an object or directory does not exist.
var ErrNotFound = errors.New("not found")

// ObjectInfo describes a remote object or directory.
type ObjectInfo struct {
	Path        string
	Size        int64
	ModTime     time.Time
	ETag        string
	IsDir       bool
	ContentType string
}

// Backend abstracts a remote storage system.
// Implementations wrap rclone backends.
type Backend interface {
	// Name returns the configured name of this backend.
	Name() string

	// Type returns the backend type (e.g. "azureblob", "s3", "local").
	Type() string

	// List returns objects and directories under the given prefix.
	// Returns direct children only (delimiter-based listing).
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)

	// Stat returns info for a single object or directory.
	Stat(ctx context.Context, path string) (ObjectInfo, error)

	// ReadAt reads len(p) bytes from the object starting at byte offset off.
	ReadAt(ctx context.Context, path string, p []byte, off int64) (int, error)

	// Open returns a reader for the entire object.
	Open(ctx context.Context, path string) (io.ReadCloser, error)

	// Write writes data to the given path. Creates or overwrites.
	Write(ctx context.Context, path string, r io.Reader, size int64) error

	// Delete removes an object.
	Delete(ctx context.Context, path string) error

	// Close releases resources held by this backend.
	Close() error
}

// Registry manages named backends.
type Registry struct {
	mu       sync.RWMutex
	backends map[string]Backend
}

// NewRegistry creates an empty backend registry.
func NewRegistry() *Registry {
	return &Registry{
		backends: make(map[string]Backend),
	}
}

// Register adds a backend to the registry, keyed by its Name().
func (r *Registry) Register(b Backend) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := b.Name()
	if _, exists := r.backends[name]; exists {
		return fmt.Errorf("backend.Registry: backend %q already registered", name)
	}
	r.backends[name] = b
	return nil
}

// Get returns a backend by name.
func (r *Registry) Get(name string) (Backend, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.backends[name]
	if !ok {
		return nil, fmt.Errorf("backend.Registry: backend %q not found", name)
	}
	return b, nil
}

// All returns a copy of all registered backends.
func (r *Registry) All() map[string]Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := make(map[string]Backend, len(r.backends))
	for k, v := range r.backends {
		m[k] = v
	}
	return m
}

// Close closes all registered backends.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var errs []error
	for name, b := range r.backends {
		if err := b.Close(); err != nil {
			errs = append(errs, fmt.Errorf("backend %s: %w", name, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
