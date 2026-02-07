package fuse

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/warpdrive/warpdrive/pkg/backend"
	"github.com/warpdrive/warpdrive/pkg/cache"
	"github.com/warpdrive/warpdrive/pkg/metrics"
	"github.com/warpdrive/warpdrive/pkg/namespace"
	"github.com/warpdrive/warpdrive/pkg/telemetry"
)

// hostname is resolved once at startup.
var hostname string

func init() {
	h, err := os.Hostname()
	if err != nil {
		h = "unknown"
	}
	hostname = h
}

// WarpDriveRoot is the root FUSE inode. It lists the top-level mount-point
// directories that correspond to configured backends.
type WarpDriveRoot struct {
	gofuse.Inode

	ns        *namespace.Namespace
	cache     *cache.CacheManager
	backends  *backend.Registry
	telemetry *telemetry.Collector

	// Bounded directory listing cache.
	dirMu              sync.Mutex
	dirCache           map[string]*dirCacheEntry
	dirTTL             time.Duration
	maxDirCacheEntries int
}

type dirCacheEntry struct {
	entries []fuse.DirEntry
	fetched time.Time
}

// Compile-time interface checks.
var _ = (gofuse.NodeLookuper)((*WarpDriveRoot)(nil))
var _ = (gofuse.NodeReaddirer)((*WarpDriveRoot)(nil))
var _ = (gofuse.NodeGetattrer)((*WarpDriveRoot)(nil))

// NewRoot creates a WarpDriveRoot for mounting.
func NewRoot(ns *namespace.Namespace, c *cache.CacheManager, reg *backend.Registry) *WarpDriveRoot {
	return &WarpDriveRoot{
		ns:                 ns,
		cache:              c,
		backends:           reg,
		dirTTL:             5 * time.Minute,
		dirCache:           make(map[string]*dirCacheEntry),
		maxDirCacheEntries: 1024,
	}
}

// SetTelemetry attaches a telemetry collector. If nil, telemetry is disabled.
func (r *WarpDriveRoot) SetTelemetry(tc *telemetry.Collector) {
	r.telemetry = tc
}

// recordEvent is a helper to emit a telemetry event if collector is configured.
func (r *WarpDriveRoot) recordEvent(evt telemetry.AccessEvent) {
	if r.telemetry != nil {
		r.telemetry.Record(evt)
	}
}

// Getattr returns root directory attributes.
func (r *WarpDriveRoot) Getattr(ctx context.Context, fh gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.Nlink = 2
	return 0
}

// Lookup resolves a child name under the root.
func (r *WarpDriveRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	mounts := r.ns.MountPoints()
	for _, m := range mounts {
		mpName := m.MountPath
		if len(mpName) > 0 && mpName[0] == '/' {
			mpName = mpName[1:]
		}
		if mpName == name {
			dir := &WarpDriveDir{
				root:        r,
				backendName: m.Name,
				remotePath:  "",
			}
			out.Mode = 0o755 | syscall.S_IFDIR
			out.Nlink = 2
			setEntryTTL(out)
			child := r.NewInode(ctx, dir, gofuse.StableAttr{Mode: syscall.S_IFDIR})
			return child, 0
		}
	}
	return nil, syscall.ENOENT
}

// Readdir lists mount points at the root level.
func (r *WarpDriveRoot) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	mounts := r.ns.MountPoints()
	entries := make([]fuse.DirEntry, 0, len(mounts))
	for _, m := range mounts {
		name := m.MountPath
		if len(name) > 0 && name[0] == '/' {
			name = name[1:]
		}
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: syscall.S_IFDIR,
		})
	}
	return gofuse.NewListDirStream(entries), 0
}

// WarpDriveDir represents a directory within a backend.
type WarpDriveDir struct {
	gofuse.Inode

	root        *WarpDriveRoot
	backendName string
	remotePath  string
}

var _ = (gofuse.NodeLookuper)((*WarpDriveDir)(nil))
var _ = (gofuse.NodeReaddirer)((*WarpDriveDir)(nil))
var _ = (gofuse.NodeGetattrer)((*WarpDriveDir)(nil))

// Getattr for directories.
func (d *WarpDriveDir) Getattr(ctx context.Context, fh gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0o755 | syscall.S_IFDIR
	out.Nlink = 2
	return 0
}

// Lookup resolves a child within this directory.
func (d *WarpDriveDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	start := time.Now()
	metrics.FUSEOperations.WithLabelValues("lookup").Inc()
	childPath := joinPath(d.remotePath, name)

	be, err := d.root.backends.Get(d.backendName)
	if err != nil {
		slog.Error("backend not found", "name", d.backendName)
		return nil, syscall.EIO
	}

	info, err := be.Stat(ctx, childPath)
	if err != nil {
		d.root.recordEvent(telemetry.AccessEvent{
			Timestamp:   start,
			BackendName: d.backendName,
			Path:        childPath,
			Operation:   "stat",
			NodeHost:    hostname,
			LatencyMs:   float64(time.Since(start).Milliseconds()),
			Error:       "not found",
		})
		return nil, syscall.ENOENT
	}

	// Record stat telemetry
	d.root.recordEvent(telemetry.AccessEvent{
		Timestamp:   start,
		BackendName: d.backendName,
		Path:        childPath,
		Operation:   "stat",
		NodeHost:    hostname,
		LatencyMs:   float64(time.Since(start).Milliseconds()),
	})

	if info.IsDir {
		dir := &WarpDriveDir{
			root:        d.root,
			backendName: d.backendName,
			remotePath:  childPath,
		}
		out.Mode = 0o755 | syscall.S_IFDIR
		out.Nlink = 2
		setEntryTTL(out)
		child := d.NewInode(ctx, dir, gofuse.StableAttr{Mode: syscall.S_IFDIR})
		return child, 0
	}

	file := &WarpDriveFile{
		root:        d.root,
		backendName: d.backendName,
		remotePath:  childPath,
		size:        info.Size,
		etag:        info.ETag,
	}
	out.Mode = 0o444 | syscall.S_IFREG
	out.Size = uint64(info.Size)
	if !info.ModTime.IsZero() {
		out.SetTimes(nil, &info.ModTime, nil)
	}
	setEntryTTL(out)
	child := d.NewInode(ctx, file, gofuse.StableAttr{Mode: syscall.S_IFREG})
	return child, 0
}

// Readdir lists the contents of this directory.
func (d *WarpDriveDir) Readdir(ctx context.Context) (gofuse.DirStream, syscall.Errno) {
	start := time.Now()
	metrics.FUSEOperations.WithLabelValues("readdir").Inc()
	cacheKey := d.backendName + ":" + d.remotePath

	d.root.dirMu.Lock()
	if entry, ok := d.root.dirCache[cacheKey]; ok && time.Since(entry.fetched) < d.root.dirTTL {
		entries := entry.entries
		d.root.dirMu.Unlock()
		return gofuse.NewListDirStream(entries), 0
	}
	d.root.dirMu.Unlock()

	be, err := d.root.backends.Get(d.backendName)
	if err != nil {
		return nil, syscall.EIO
	}

	objects, err := be.List(ctx, d.remotePath)
	if err != nil {
		slog.Error("list failed", "backend", d.backendName, "path", d.remotePath, "err", err)
		d.root.recordEvent(telemetry.AccessEvent{
			Timestamp:   start,
			BackendName: d.backendName,
			Path:        d.remotePath,
			Operation:   "list",
			NodeHost:    hostname,
			LatencyMs:   float64(time.Since(start).Milliseconds()),
			Error:       err.Error(),
		})
		return nil, syscall.EIO
	}

	// Record list telemetry
	d.root.recordEvent(telemetry.AccessEvent{
		Timestamp:   start,
		BackendName: d.backendName,
		Path:        d.remotePath,
		Operation:   "list",
		NodeHost:    hostname,
		LatencyMs:   float64(time.Since(start).Milliseconds()),
	})

	entries := make([]fuse.DirEntry, 0, len(objects))
	for _, obj := range objects {
		mode := uint32(syscall.S_IFREG)
		if obj.IsDir {
			mode = syscall.S_IFDIR
		}
		// Extract just the filename from the path.
		name := obj.Path
		if idx := strings.LastIndex(name, "/"); idx >= 0 {
			name = name[idx+1:]
		}
		if name == "" {
			continue
		}
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: mode,
		})
	}

	// Inform readahead manager about directory contents for dir-level prefetch.
	var fileNames []string
	for _, e := range entries {
		if e.Mode&syscall.S_IFDIR == 0 {
			fileNames = append(fileNames, e.Name)
		}
	}
	if len(fileNames) > 0 {
		d.root.cache.SetDirFiles(d.backendName, d.remotePath, fileNames)
	}

	d.root.dirMu.Lock()
	d.root.dirCache[cacheKey] = &dirCacheEntry{
		entries: entries,
		fetched: time.Now(),
	}
	// Evict stale entries if cache is too large.
	if len(d.root.dirCache) > d.root.maxDirCacheEntries {
		now := time.Now()
		for k, v := range d.root.dirCache {
			if now.Sub(v.fetched) > d.root.dirTTL {
				delete(d.root.dirCache, k)
			}
		}
		// Hard cap: if still too large, remove arbitrary entries.
		for k := range d.root.dirCache {
			if len(d.root.dirCache) <= d.root.maxDirCacheEntries/2 {
				break
			}
			delete(d.root.dirCache, k)
		}
	}
	d.root.dirMu.Unlock()

	return gofuse.NewListDirStream(entries), 0
}

// WarpDriveFile represents a regular file.
type WarpDriveFile struct {
	gofuse.Inode

	root        *WarpDriveRoot
	backendName string
	remotePath  string
	size        int64
	etag        string
}

var _ = (gofuse.NodeGetattrer)((*WarpDriveFile)(nil))
var _ = (gofuse.NodeOpener)((*WarpDriveFile)(nil))
var _ = (gofuse.NodeSetattrer)((*WarpDriveFile)(nil))

// Getattr for files.
func (f *WarpDriveFile) Getattr(ctx context.Context, fh gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0o444 | syscall.S_IFREG
	out.Size = uint64(f.size)
	out.Nlink = 1
	return 0
}

// Setattr rejects all mutations — M1 is read-only.
func (f *WarpDriveFile) Setattr(ctx context.Context, fh gofuse.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return syscall.EROFS
}

// Open creates a file handle for reading.
func (f *WarpDriveFile) Open(ctx context.Context, flags uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_APPEND|syscall.O_CREAT|syscall.O_TRUNC) != 0 {
		return nil, 0, syscall.EROFS
	}
	handle := &WarpDriveFileHandle{file: f}
	return handle, 0, 0
}

// WarpDriveFileHandle implements stateless read operations.
type WarpDriveFileHandle struct {
	file *WarpDriveFile
}

var _ = (gofuse.FileReader)((*WarpDriveFileHandle)(nil))

// Read delegates to the cache manager and records telemetry.
func (fh *WarpDriveFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	start := time.Now()
	metrics.FUSEOperations.WithLabelValues("read").Inc()
	n, cacheHit, err := fh.file.root.cache.ReadWithCacheHit(ctx, fh.file.backendName, fh.file.remotePath, dest, off)
	latency := time.Since(start)
	metrics.FUSELatency.WithLabelValues("read").Observe(latency.Seconds())
	metrics.BackendBytesRead.WithLabelValues(fh.file.backendName).Add(float64(n))

	// Record telemetry
	evt := telemetry.AccessEvent{
		Timestamp:   start,
		UserID:      getUserFromContext(ctx),
		BackendName: fh.file.backendName,
		Path:        fh.file.remotePath,
		Operation:   "read",
		BytesRead:   int64(n),
		CacheHit:    cacheHit,
		NodeHost:    hostname,
		LatencyMs:   float64(latency.Milliseconds()),
	}
	if err != nil {
		evt.Error = err.Error()
	}
	fh.file.root.recordEvent(evt)

	if err != nil {
		slog.Error("read failed", "backend", fh.file.backendName, "path", fh.file.remotePath, "off", off, "err", err)
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func joinPath(base, child string) string {
	if base == "" {
		return child
	}
	return base + "/" + child
}

func setEntryTTL(out *fuse.EntryOut) {
	out.SetEntryTimeout(60 * time.Second)
	out.SetAttrTimeout(60 * time.Second)
}

// getUserFromContext extracts the user ID from FUSE request context.
// On Linux, this uses the UID from the FUSE caller.
// Returns "unknown" if not available.
func getUserFromContext(ctx context.Context) string {
	// Try to get FUSE caller from context.
	// The go-fuse library embeds caller info in the context.
	if caller, ok := fuse.FromContext(ctx); ok {
		if caller.Uid != 0 {
			return fmt.Sprintf("uid:%d", caller.Uid)
		}
	}
	return "unknown"
}
