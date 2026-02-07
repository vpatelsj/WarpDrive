package namespace

import (
	"fmt"
	"sort"
	"strings"

	"github.com/warpdrive/warpdrive/pkg/config"
)

// ResolveResult holds the result of resolving a filesystem path to a backend.
type ResolveResult struct {
	BackendName string
	RemotePath  string
}

// MountPoint describes a mounted backend.
type MountPoint struct {
	Name      string
	MountPath string
}

// Namespace maps FUSE paths to (backend, remote-path) pairs.
type Namespace struct {
	mounts []mountEntry
}

type mountEntry struct {
	prefix      string // e.g. "/training"
	backendName string
}

// New creates a Namespace from backend configurations. Mount paths are
// sorted longest-prefix-first so the most specific match wins.
func New(backends []config.BackendConfig) *Namespace {
	entries := make([]mountEntry, 0, len(backends))
	for _, b := range backends {
		mp := b.MountPath
		if mp == "" {
			mp = "/" + b.Name
		}
		mp = "/" + strings.Trim(mp, "/")
		entries = append(entries, mountEntry{
			prefix:      mp,
			backendName: b.Name,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return len(entries[i].prefix) > len(entries[j].prefix)
	})
	return &Namespace{mounts: entries}
}

// Resolve maps a FUSE path to a backend name and remote path.
func (ns *Namespace) Resolve(path string) (*ResolveResult, error) {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	for _, m := range ns.mounts {
		if path == m.prefix || strings.HasPrefix(path, m.prefix+"/") {
			remote := strings.TrimPrefix(path, m.prefix)
			remote = strings.TrimPrefix(remote, "/")
			return &ResolveResult{
				BackendName: m.backendName,
				RemotePath:  remote,
			}, nil
		}
	}
	return nil, fmt.Errorf("namespace: no backend mounted at %q", path)
}

// MountPoints returns all configured mount points.
func (ns *Namespace) MountPoints() []MountPoint {
	result := make([]MountPoint, len(ns.mounts))
	for i, m := range ns.mounts {
		result[i] = MountPoint{
			Name:      m.backendName,
			MountPath: m.prefix,
		}
	}
	return result
}

// IsRootPath returns true if the path is the filesystem root.
func (ns *Namespace) IsRootPath(path string) bool {
	return path == "/" || path == ""
}
