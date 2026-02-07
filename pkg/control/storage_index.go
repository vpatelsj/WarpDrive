package control

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/warpdrive/warpdrive/pkg/backend"
)

// StorageCrawler periodically indexes all backends.
type StorageCrawler struct {
	server   *Server
	interval time.Duration
}

// Run starts the periodic crawl loop.
func (sc *StorageCrawler) Run(ctx context.Context) {
	// Initial crawl
	if err := sc.crawlAll(ctx); err != nil {
		slog.Error("initial crawl failed", "error", err)
	}

	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := sc.crawlAll(ctx); err != nil {
				slog.Error("periodic crawl failed", "error", err)
			}
		}
	}
}

// crawlAll lists all files in all backends and upserts into the store.
func (sc *StorageCrawler) crawlAll(ctx context.Context) error {
	if sc.server.backends == nil {
		return nil
	}
	for name, b := range sc.server.backends.All() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Info("crawling backend", "name", name)
		start := time.Now()
		count, err := sc.crawlBackend(ctx, name, b, "")
		if err != nil {
			slog.Error("crawl failed", "backend", name, "error", err)
			continue
		}
		slog.Info("crawl complete", "backend", name, "files", count, "duration", time.Since(start))
	}
	return nil
}

// crawlBackend recursively lists and indexes a single backend.
func (sc *StorageCrawler) crawlBackend(ctx context.Context, name string, b backend.Backend, prefix string) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}

	objects, err := b.List(ctx, prefix)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, obj := range objects {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}

		path := obj.Path
		// Normalize: remove leading slashes
		path = strings.TrimPrefix(path, "/")

		if obj.IsDir {
			// Recurse into subdirectory
			subPath := path
			if prefix != "" && !strings.HasPrefix(subPath, prefix) {
				subPath = prefix + "/" + subPath
			}
			n, err := sc.crawlBackend(ctx, name, b, subPath)
			if err != nil {
				slog.Warn("crawl subdir failed", "backend", name, "path", subPath, "error", err)
				continue
			}
			count += n
		} else {
			sc.server.store.UpsertFile(StorageFile{
				BackendName:  name,
				Path:         path,
				SizeBytes:    obj.Size,
				ETag:         obj.ETag,
				LastModified: obj.ModTime,
			})
			count++
		}
	}
	return count, nil
}
