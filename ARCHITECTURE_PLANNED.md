# WarpDrive — Planned Architecture

> This document describes features and components that are **planned but not
> yet implemented**. For what exists today, see
> [ARCHITECTURE_CURRENT.md](ARCHITECTURE_CURRENT.md).

## Planned Components

### `cmd/warpdrive-control/` — Control Plane Binary

A standalone binary for the centralized control plane. Currently the control
plane logic lives in `pkg/control/` and is served as an in-memory REST API
from `warpdrive-ctl`. The plan:

- Separate binary with its own `main.go`
- gRPC + REST (via grpc-gateway) serving
- PostgreSQL 16 for persistence (currently in-memory maps)

### `proto/` — Protobuf Definitions

```
proto/
├── telemetry.proto
├── control.proto
└── governance.proto
```

Telemetry events, control plane RPCs, and governance queries will be defined
as protobuf messages for type-safe gRPC communication.

### `dashboard/` — Governance Dashboard

A React + TypeScript (Vite) web UI for:
- Storage usage attribution per team
- Stale data detection and recommendations
- Quota management
- Cost projections

### `Dockerfile`

Multi-stage Docker build for `warpdrive-mount` and `warpdrive-control`.

### `hack/` — Developer Scripts

```
hack/
├── setup-dev.sh
└── run-local.sh
```

## Planned Features

### Write Support

The `Backend` interface defines `Write` and `Delete` methods but they are
not currently exercised. The filesystem is read-only. Write-back caching
and conflict resolution are planned.

### gRPC Telemetry Sink

Currently telemetry supports `stdout`, `file`, and `nop` sinks. A `grpc`
sink will stream events to the control plane binary.

### Control Plane Persistence

Replace in-memory storage with PostgreSQL:
- Storage index persistence
- Team mapping storage
- Quota state
- Telemetry event archival

### Governance Features

Planned files in `pkg/control/`:
- `governance.go` — usage attribution, stale data detection
- `quota.go` — per-team quota enforcement

### SFI Compliance (`pkg/auth/sfi.go`)

Azure Secure Future Initiative compliance:
- Private link validation
- Service tag enforcement
- Cross-tenant access auditing

### Cross-Tenant Configuration

```yaml
cross_tenant:
  home_tenant_id: "..."
  app_registration_id: "..."
  target_tenants:
    - tenant_id: "..."
      private_endpoint: pe-warpdrive-tenant2-blob.privatelink.blob.core.windows.net
  sfi:
    enforce_private_link: true
    allowed_service_tags: ["Storage", "AzureActiveDirectory"]
```

Config struct and validation not yet implemented.

### Additional Source Files

Planned but not yet present:
- `pkg/fuse/dir.go` — directory operations (currently inlined in `fs.go`)
- `pkg/fuse/file.go` — file operations (currently inlined in `fs.go`)
- `pkg/namespace/config.go` — namespace config parsing
- `pkg/cache/metrics.go` — cache-specific metrics (currently in `pkg/metrics/`)

## Build Targets (Planned)

```bash
make docker           # Build Docker image
make helm-package     # Package Helm chart
make run-control      # Run control plane
make test-integration # Integration tests against real backends
```
