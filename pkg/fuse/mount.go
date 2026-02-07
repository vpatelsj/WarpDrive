package fuse

import (
	"context"
	"fmt"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// MountConfig holds FUSE mount configuration.
type MountConfig struct {
	MountPoint  string
	AllowOther  bool
	Debug       bool
	FUSEWorkers int           // Default 32
	DirCacheTTL time.Duration // Default 5m
}

// Mount creates and serves the FUSE filesystem. It blocks until the
// filesystem is unmounted or the context is cancelled.
func Mount(ctx context.Context, cfg MountConfig, root *WarpDriveRoot) error {
	if cfg.DirCacheTTL > 0 {
		root.dirTTL = cfg.DirCacheTTL
	}

	attrTTL := 60 * time.Second
	entryTTL := 60 * time.Second
	negTTL := 5 * time.Second

	opts := &gofuse.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:               cfg.AllowOther,
			Name:                     "warpdrive",
			FsName:                   "warpdrive",
			MaxReadAhead:             1048576, // 1MB — critical for throughput
			DirectMount:              true,
			DirectMountStrict:        true,
			EnableLocks:              false, // Not needed, reduces overhead
			ExplicitDataCacheControl: true,  // We control caching
			MaxBackground:            64,    // Max background FUSE requests
			DisableReadDirPlus:       false, // Enable READDIRPLUS for fewer round-trips
			Debug:                    cfg.Debug,
		},
		AttrTimeout:     &attrTTL,
		EntryTimeout:    &entryTTL,
		NegativeTimeout: &negTTL,
	}

	if cfg.FUSEWorkers > 0 {
		opts.MountOptions.MaxBackground = cfg.FUSEWorkers
	}

	server, err := gofuse.Mount(cfg.MountPoint, root, opts)
	if err != nil {
		return fmt.Errorf("fuse: mount at %s: %w", cfg.MountPoint, err)
	}

	// Unmount on context cancellation.
	go func() {
		<-ctx.Done()
		server.Unmount()
	}()

	server.Wait()
	return nil
}
