# WarpDrive — Current Architecture

> This document describes what is **implemented and present** in the repository
> today. For planned features and aspirational design, see
> [ARCHITECTURE_PLANNED.md](ARCHITECTURE_PLANNED.md).

## Overview

WarpDrive is a cross-cloud data fabric for GPU training workloads. It presents a
unified POSIX filesystem mount over heterogeneous storage backends with
intelligent NVMe caching and cross-cloud auth federation.

**Language:** Go  
**Storage abstraction:** rclone (used as a Go library, not CLI)  
**FUSE library:** `github.com/hanwen/go-fuse/v2`  
**Cache metadata:** `github.com/dgraph-io/badger/v4`  
**Config format:** YAML  
**Build:** Go modules + Makefile  
**Deployment:** Helm chart (K8s), systemd (bare metal), Slurm prolog/epilog

## Repository Structure

```
warpdrive/
├── cmd/
│   ├── warpdrive-mount/          # Node agent binary (FUSE mount + cache + metrics + telemetry)
│   │   └── main.go
│   ├── warpdrive-ctl/            # Admin CLI binary
│   │   └── main.go
│   └── warpdrive-bench/          # Benchmark tool
│       └── main.go
├── pkg/
│   ├── auth/                 # Multi-cloud credential federation
│   │   ├── auth.go           # Manager, provider interface, audit logger
│   │   ├── entra.go          # Azure Entra ID (cross-tenant)
│   │   ├── aws.go            # AWS STS OIDC federation
│   │   ├── gcp.go            # GCP Workload Identity Federation
│   │   ├── keyvault.go       # Azure Key Vault for static creds
│   │   ├── static.go         # Static credentials provider
│   │   ├── audit.go          # Audit logging
│   │   ├── retry.go          # Retry helpers
│   │   └── auth_test.go
│   ├── backend/              # Storage backend abstraction over rclone
│   │   ├── backend.go        # Interface definitions + registry
│   │   ├── rclone.go         # rclone integration
│   │   ├── authenticated.go  # Auth-wrapping layer
│   │   ├── backend_test.go
│   │   └── authenticated_test.go
│   ├── cache/                # NVMe block cache engine
│   │   ├── cache.go          # Cache manager
│   │   ├── block.go          # Block-level operations
│   │   ├── coalesce.go       # Request coalescing
│   │   ├── eviction.go       # LRU eviction
│   │   ├── readahead.go      # Readahead/prefetch
│   │   ├── warmup.go         # Cache warming
│   │   ├── cache_test.go
│   │   ├── bench_test.go
│   │   ├── integration_test.go
│   │   ├── internals_test.go
│   │   ├── readahead_test.go
│   │   └── warmup_test.go
│   ├── config/               # Configuration loading + validation
│   │   ├── config.go         # Config structs, validation
│   │   ├── loader.go         # YAML loader with env var expansion
│   │   └── config_test.go
│   ├── control/              # Control plane (REST, in-memory)
│   │   ├── api.go            # REST API handlers
│   │   ├── server.go         # HTTP server
│   │   ├── storage_index.go  # Storage crawler + index
│   │   └── control_test.go
│   ├── fuse/                 # FUSE filesystem
│   │   ├── fs.go             # Root, dir, file nodes (single file)
│   │   └── mount.go          # Mount management
│   ├── metrics/              # Prometheus metrics + health
│   │   ├── metrics.go        # Metric definitions, health checks, server
│   │   └── metrics_test.go
│   ├── namespace/            # Unified namespace tree
│   │   ├── namespace.go
│   │   └── namespace_test.go
│   └── telemetry/            # Access event collection
│       ├── collector.go
│       ├── emitter.go
│       ├── events.go
│       └── telemetry_test.go
├── deploy/
│   ├── helm/warpdrive/           # Kubernetes Helm chart
│   ├── systemd/              # systemd service unit
│   ├── slurm/                # Slurm prolog/epilog + Ansible playbook
│   ├── monitoring/           # Grafana dashboard + alert rules
│   └── examples/             # Example manifests (init containers)
├── e2e/                      # End-to-end tests
├── scripts/                  # Demo + verification scripts
├── Makefile
├── go.mod / go.sum
├── config.example.yaml
└── config.test.yaml
```

## Key Interfaces

### Backend (pkg/backend/backend.go)

```go
type Backend interface {
    Name() string
    Type() string
    List(ctx context.Context, prefix string) ([]ObjectInfo, error)
    Stat(ctx context.Context, path string) (ObjectInfo, error)
    ReadAt(ctx context.Context, path string, p []byte, off int64) (int, error)
    Open(ctx context.Context, path string) (io.ReadCloser, error)
    Close() error
}
```

Note: `Write` and `Delete` are defined in the interface but not exercised —
the filesystem is currently read-only.

### Cache Manager (pkg/cache/cache.go)

Caches blocks on local NVMe. Supports readahead prefetching, LRU eviction
with configurable high/low water marks, and cache warming via
`warpdrive-ctl warmup`.

### Auth Manager (pkg/auth/auth.go)

Resolves credentials per-backend. Providers: Entra ID (managed identity /
service principal), AWS STS (OIDC federation from Entra), GCP WIF,
Azure Key Vault, static, none.

## Configuration

See [config.example.yaml](config.example.yaml). Key sections:

- `mount_point` / `allow_other` / `fuse_debug`
- `cache.*` — path, max_size, block_size, eviction, readahead
- `backends[]` — name, type, mount_path, config, auth
- `metrics.*` — enabled, addr
- `telemetry.*` — sink, sampling, batching
- `control_plane.*` — REST addr

Environment variables are expanded via `${VAR}` syntax in YAML values.

## Metrics

Prometheus metrics served at configurable address (default `:9090/metrics`).
Health checks at `/healthz`.

## Error Handling

- All public functions return `error` as last value
- Errors wrapped with `fmt.Errorf("pkg.Func: %w", err)`
- Context cancellation respected in all loops

## Logging

Uses `log/slog` (structured logging). No secrets in logs.

## Testing

```bash
make test           # unit tests with -race
make test-e2e       # end-to-end (requires FUSE)
make bench-go       # cache benchmarks
```
