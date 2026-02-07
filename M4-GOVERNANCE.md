# M4: Governance — Technical Spec

## Timeline: Weeks 14-22 (parallel to M3)

## Goal

Administrators can see who owns what data, where, across all backends. Stale data is flagged. Quotas are enforceable.

---

## M4.1: Telemetry Collection (Weeks 14-16)

### Design

The node agent emits structured events for every file access. Events are batched and sent to the control plane via gRPC. For environments without the control plane, events can go to stdout (K8s log aggregation).

### Implementation: `pkg/telemetry/collector.go`

```go
package telemetry

import (
    "context"
    "sync"
    "time"
)

// CollectorConfig configures telemetry collection.
type CollectorConfig struct {
    Enabled          bool
    Sink             string        // "grpc", "stdout", "file"
    ControlPlaneAddr string        // gRPC address for "grpc" sink
    SampleMetadataOps float64     // Sampling rate for list/stat (0.1 = 10%)
    BatchSize        int           // Batch events before sending (default 100)
    FlushInterval    time.Duration // Max time before flushing (default 5s)
}

// Collector collects and batches access events.
type Collector struct {
    cfg     CollectorConfig
    emitter Emitter
    
    batch   []AccessEvent
    mu      sync.Mutex
    
    // Async flush
    flushCh chan struct{}
    closeCh chan struct{}
    wg      sync.WaitGroup
}

// NewCollector creates a telemetry collector.
func NewCollector(cfg CollectorConfig) (*Collector, error) {
    var emitter Emitter
    switch cfg.Sink {
    case "grpc":
        emitter = NewGRPCEmitter(cfg.ControlPlaneAddr)
    case "stdout":
        emitter = NewStdoutEmitter()
    case "file":
        emitter = NewFileEmitter("/var/log/warpdrive/telemetry.jsonl")
    }
    
    c := &Collector{
        cfg:     cfg,
        emitter: emitter,
        batch:   make([]AccessEvent, 0, cfg.BatchSize),
        flushCh: make(chan struct{}, 1),
        closeCh: make(chan struct{}),
    }
    
    c.wg.Add(1)
    go c.flushLoop()
    return c, nil
}

// Record adds an access event. Non-blocking.
func (c *Collector) Record(evt AccessEvent) {
    if !c.cfg.Enabled {
        return
    }
    
    // Sampling for metadata ops
    if (evt.Operation == "list" || evt.Operation == "stat") && !shouldSample(c.cfg.SampleMetadataOps) {
        return
    }
    
    c.mu.Lock()
    c.batch = append(c.batch, evt)
    shouldFlush := len(c.batch) >= c.cfg.BatchSize
    c.mu.Unlock()
    
    if shouldFlush {
        select {
        case c.flushCh <- struct{}{}:
        default:
        }
    }
}

func (c *Collector) flushLoop() {
    defer c.wg.Done()
    ticker := time.NewTicker(c.cfg.FlushInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-c.closeCh:
            c.flush() // Final flush
            return
        case <-c.flushCh:
            c.flush()
        case <-ticker.C:
            c.flush()
        }
    }
}

func (c *Collector) flush() {
    c.mu.Lock()
    if len(c.batch) == 0 {
        c.mu.Unlock()
        return
    }
    batch := c.batch
    c.batch = make([]AccessEvent, 0, c.cfg.BatchSize)
    c.mu.Unlock()
    
    // Send to emitter (non-blocking, drop on error)
    if err := c.emitter.Emit(batch); err != nil {
        slog.Warn("Telemetry flush failed", "count", len(batch), "error", err)
    }
}
```

### gRPC Emitter: `pkg/telemetry/emitter.go`

```go
package telemetry

// Emitter sends batches of events to a sink.
type Emitter interface {
    Emit(events []AccessEvent) error
    Close() error
}

// GRPCEmitter sends events to the control plane via gRPC.
type GRPCEmitter struct {
    addr   string
    conn   *grpc.ClientConn
    client proto.TelemetryServiceClient
}

// StdoutEmitter writes JSON lines to stdout (for K8s log aggregation).
type StdoutEmitter struct {
    encoder *json.Encoder
}

// FileEmitter writes JSON lines to a file.
type FileEmitter struct {
    file    *os.File
    encoder *json.Encoder
    mu      sync.Mutex
}
```

### Protobuf: `proto/telemetry.proto`

```protobuf
syntax = "proto3";
package warpdrive.telemetry;

service TelemetryService {
    rpc IngestEvents(IngestEventsRequest) returns (IngestEventsResponse);
}

message AccessEvent {
    string timestamp = 1;      // RFC3339
    string user_id = 2;
    string backend_name = 3;
    string path = 4;
    string operation = 5;      // read, list, stat, write
    int64 bytes_read = 6;
    bool cache_hit = 7;
    string node_host = 8;
    double latency_ms = 9;
    string error = 10;
}

message IngestEventsRequest {
    repeated AccessEvent events = 1;
}

message IngestEventsResponse {
    int32 accepted = 1;
}
```

### Integration with FUSE layer

```go
// In pkg/fuse/file.go — instrument reads
func (fh *WarpDriveFileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, error) {
    start := time.Now()
    n, err := fh.file.root.cache.Read(ctx, fh.file.backendName, fh.file.remotePath, dest, off)
    latency := time.Since(start)
    
    // Record telemetry
    fh.file.root.telemetry.Record(telemetry.AccessEvent{
        Timestamp:   start,
        UserID:      getUserFromContext(ctx), // From FUSE request context
        BackendName: fh.file.backendName,
        Path:        fh.file.remotePath,
        Operation:   "read",
        BytesRead:   int64(n),
        CacheHit:    /* from cache stats */,
        NodeHost:    hostname,
        LatencyMs:   float64(latency.Milliseconds()),
    })
    
    if err != nil {
        return nil, syscall.EIO
    }
    return fuse.ReadResultData(dest[:n]), nil
}
```

---

## M4.2: Control Plane + Storage Index (Weeks 15-19)

### Design

Central Go service that:
1. Receives telemetry from node agents via gRPC
2. Periodically crawls backends to build a storage index (file sizes, modification times)
3. Joins access data with storage index to compute: usage by team, stale data, growth

### Database Schema (PostgreSQL)

```sql
-- Storage index: one row per file across all backends
CREATE TABLE storage_files (
    id              BIGSERIAL PRIMARY KEY,
    backend_name    TEXT NOT NULL,
    path            TEXT NOT NULL,
    size_bytes      BIGINT NOT NULL,
    etag            TEXT,
    last_modified   TIMESTAMPTZ,
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_crawled    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (backend_name, path)
);

CREATE INDEX idx_files_backend ON storage_files(backend_name);
CREATE INDEX idx_files_size ON storage_files(size_bytes DESC);

-- Access log: aggregated per file per day per user
CREATE TABLE access_log (
    id              BIGSERIAL PRIMARY KEY,
    backend_name    TEXT NOT NULL,
    path            TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    date            DATE NOT NULL,
    read_count      INT NOT NULL DEFAULT 0,
    bytes_read      BIGINT NOT NULL DEFAULT 0,
    last_access     TIMESTAMPTZ NOT NULL,
    UNIQUE (backend_name, path, user_id, date)
);

CREATE INDEX idx_access_last ON access_log(last_access);
CREATE INDEX idx_access_user ON access_log(user_id);

-- Team mapping
CREATE TABLE teams (
    user_id     TEXT NOT NULL,
    team_name   TEXT NOT NULL,
    PRIMARY KEY (user_id, team_name)
);

-- Quotas
CREATE TABLE quotas (
    id          SERIAL PRIMARY KEY,
    team_name   TEXT NOT NULL,
    backend_name TEXT,              -- NULL = all backends
    soft_limit  BIGINT NOT NULL,    -- Bytes
    hard_limit  BIGINT NOT NULL,    -- Bytes
    UNIQUE (team_name, backend_name)
);
```

### Implementation: `pkg/control/server.go`

```go
package control

import (
    "context"
    "database/sql"
    "net"
    "net/http"
    
    "google.golang.org/grpc"
    _ "github.com/lib/pq"
)

type Server struct {
    db        *sql.DB
    grpcSrv   *grpc.Server
    httpSrv   *http.Server
    backends  *backend.Registry
    crawlInterval time.Duration
}

func NewServer(cfg ControlPlaneConfig, backends *backend.Registry) (*Server, error) {
    db, err := sql.Open("postgres", cfg.Database.DSN())
    if err != nil {
        return nil, err
    }
    
    s := &Server{
        db:            db,
        backends:      backends,
        crawlInterval: cfg.StorageCrawlInterval,
    }
    
    // Setup gRPC server
    s.grpcSrv = grpc.NewServer()
    proto.RegisterTelemetryServiceServer(s.grpcSrv, s)
    
    // Setup HTTP server (REST API + dashboard)
    mux := http.NewServeMux()
    s.registerAPIRoutes(mux)
    s.registerDashboardRoutes(mux) // Serve React build
    s.httpSrv = &http.Server{Handler: mux}
    
    return s, nil
}
```

### REST API: `pkg/control/api.go`

```go
package control

import (
    "encoding/json"
    "net/http"
)

func (s *Server) registerAPIRoutes(mux *http.ServeMux) {
    mux.HandleFunc("GET /api/v1/usage", s.handleUsage)
    mux.HandleFunc("GET /api/v1/stale", s.handleStale)
    mux.HandleFunc("GET /api/v1/growth", s.handleGrowth)
    mux.HandleFunc("GET /api/v1/quota", s.handleQuotaList)
    mux.HandleFunc("PUT /api/v1/quota", s.handleQuotaSet)
    mux.HandleFunc("GET /api/v1/user/{userId}", s.handleUserDetail)
    mux.HandleFunc("GET /api/v1/backends", s.handleBackendList)
}

// GET /api/v1/usage?group_by=team&backend=all
func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
    groupBy := r.URL.Query().Get("group_by") // team, user, backend
    backendFilter := r.URL.Query().Get("backend") // "all" or specific name
    
    var query string
    switch groupBy {
    case "team":
        query = `
            SELECT t.team_name, sf.backend_name,
                   COUNT(*) as file_count,
                   SUM(sf.size_bytes) as total_bytes,
                   MAX(al.last_access) as last_access
            FROM storage_files sf
            LEFT JOIN access_log al ON sf.backend_name = al.backend_name AND sf.path = al.path
            LEFT JOIN teams t ON al.user_id = t.user_id
            GROUP BY t.team_name, sf.backend_name
            ORDER BY total_bytes DESC`
    case "user":
        query = `
            SELECT al.user_id, sf.backend_name,
                   COUNT(DISTINCT sf.path) as file_count,
                   SUM(DISTINCT sf.size_bytes) as total_bytes,
                   MAX(al.last_access) as last_access
            FROM access_log al
            JOIN storage_files sf ON al.backend_name = sf.backend_name AND al.path = sf.path
            GROUP BY al.user_id, sf.backend_name
            ORDER BY total_bytes DESC`
    }
    
    // Execute query, marshal to JSON, write response
}

// GET /api/v1/stale?days=90&min_size=1GB
func (s *Server) handleStale(w http.ResponseWriter, r *http.Request) {
    days := parseIntParam(r, "days", 90)
    minSize := parseSizeParam(r, "min_size", 0)
    
    query := `
        SELECT sf.backend_name, sf.path, sf.size_bytes, sf.last_modified,
               COALESCE(MAX(al.last_access), sf.first_seen) as last_access
        FROM storage_files sf
        LEFT JOIN access_log al ON sf.backend_name = al.backend_name AND sf.path = al.path
        GROUP BY sf.id
        HAVING COALESCE(MAX(al.last_access), sf.first_seen) < NOW() - $1::interval
           AND sf.size_bytes >= $2
        ORDER BY sf.size_bytes DESC
        LIMIT 1000`
    
    // Execute with params: fmt.Sprintf("%d days", days), minSize
}

// GET /api/v1/growth?backend=s3_training&period=30d
func (s *Server) handleGrowth(w http.ResponseWriter, r *http.Request) {
    // Query storage_files.first_seen bucketed by day/week
    // Return time series of cumulative storage
}
```

### Storage Crawler: `pkg/control/storage_index.go`

```go
package control

import (
    "context"
    "time"
)

// StorageCrawler periodically indexes all backends.
type StorageCrawler struct {
    server   *Server
    interval time.Duration
}

// Run starts the periodic crawl loop.
func (sc *StorageCrawler) Run(ctx context.Context) {
    ticker := time.NewTicker(sc.interval)
    defer ticker.Stop()
    
    // Initial crawl
    sc.crawlAll(ctx)
    
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            sc.crawlAll(ctx)
        }
    }
}

// crawlAll lists all files in all backends and upserts into storage_files.
func (sc *StorageCrawler) crawlAll(ctx context.Context) {
    for name, b := range sc.server.backends.All() {
        slog.Info("Crawling backend", "name", name)
        start := time.Now()
        count, err := sc.crawlBackend(ctx, name, b)
        if err != nil {
            slog.Error("Crawl failed", "backend", name, "error", err)
            continue
        }
        slog.Info("Crawl complete", "backend", name, "files", count, "duration", time.Since(start))
    }
}

// crawlBackend recursively lists and indexes a single backend.
func (sc *StorageCrawler) crawlBackend(ctx context.Context, name string, b backend.Backend) (int, error) {
    // Recursive listing with batched upserts (1000 rows per batch)
    // UPSERT: INSERT ... ON CONFLICT (backend_name, path) DO UPDATE SET size_bytes=, etag=, last_crawled=
    return 0, nil
}
```

---

## M4.3: Governance Dashboard (Weeks 18-22)

### Design

Simple React SPA. No design system overkill. Functional, fast.

### Pages

1. **Overview** — Bar chart of storage by team. Total across all backends.
2. **Backend Detail** — Per-backend breakdown. Growth line chart.
3. **Stale Data** — Table of stale files, sortable by size/date. Bulk select + mark for deletion.
4. **User Drill-down** — All data owned by a user across backends.
5. **Quotas** — Set/edit quotas per team per backend.

### Tech Stack

```
dashboard/
├── src/
│   ├── App.tsx
│   ├── api.ts                # Fetch wrappers for /api/v1/*
│   ├── pages/
│   │   ├── Overview.tsx      # Storage by team bar chart
│   │   ├── BackendDetail.tsx # Per-backend view
│   │   ├── StaleData.tsx     # Stale file table
│   │   ├── UserDetail.tsx    # Per-user breakdown
│   │   └── Quotas.tsx        # Quota management
│   └── components/
│       ├── StorageChart.tsx   # Recharts bar chart
│       ├── GrowthChart.tsx    # Recharts line chart
│       ├── FileTable.tsx      # Sortable table
│       └── SizeFormat.tsx     # Human-readable file sizes
├── package.json
├── tsconfig.json
└── vite.config.ts
```

Dependencies: React 18, react-router-dom, recharts, @tanstack/react-table

---

## M4.4: Admin CLI (Weeks 19-22)

### Commands

```go
// cmd/warpdrive-ctl/main.go — using cobra

func main() {
    root := &cobra.Command{Use: "warpdrive-ctl"}
    
    root.AddCommand(usageCmd())    // warpdrive-ctl usage --group-by team
    root.AddCommand(staleCmd())    // warpdrive-ctl stale --days 90 --min-size 10GB
    root.AddCommand(quotaCmd())    // warpdrive-ctl quota set/list/delete
    root.AddCommand(moveCmd())     // warpdrive-ctl move <src> <dst>
    root.AddCommand(warmCmd())     // warpdrive-ctl warm <path> (from M3)
    root.AddCommand(statusCmd())   // warpdrive-ctl status (show mount health, cache stats)
    
    root.Execute()
}
```

Each command calls the control plane REST API and formats output as a table or CSV.

---

## COPILOT PROMPT — M4: Governance

```
You are building the governance and telemetry system for WarpDrive.

Project: Go module at github.com/warpdrive/warpdrive
Packages: pkg/telemetry/, pkg/control/, proto/, dashboard/, cmd/warpdrive-ctl/

## What to build

### 1. Telemetry collection (pkg/telemetry/):
- AccessEvent struct: Timestamp, UserID, BackendName, Path, Operation, BytesRead, CacheHit, NodeHost, LatencyMs, Error
- Collector: batches events (100 per batch, 5s flush interval), sends to emitter
- Record(evt) is non-blocking, sampling for metadata ops (10%)
- Emitters: GRPCEmitter (sends to control plane), StdoutEmitter (JSON lines), FileEmitter
- Integrate with FUSE: instrument every Read, Readdir, Stat call

### 2. Protobuf definitions (proto/):
- telemetry.proto: TelemetryService with IngestEvents RPC
- governance.proto: messages for usage, stale data, quotas

### 3. Control plane (pkg/control/):
- Server: gRPC (port 50051) + REST API (port 8080) + serves dashboard static files
- PostgreSQL database with tables: storage_files, access_log, teams, quotas
- StorageCrawler: periodic (daily) recursive listing of all backends, upsert into storage_files
- Telemetry ingestion: receive gRPC events, aggregate into access_log (per file per user per day)
- REST API endpoints:
  - GET /api/v1/usage?group_by=team|user&backend=all|<name>
  - GET /api/v1/stale?days=90&min_size=1GB (files not accessed in N days)
  - GET /api/v1/growth?backend=<name>&period=30d
  - GET /api/v1/user/{userId} (all data for a user)
  - GET /api/v1/backends (list all backends with stats)
  - GET /api/v1/quota (list quotas)
  - PUT /api/v1/quota (create/update quota)
- Quota enforcement: on each telemetry event, check if team exceeds hard limit

### 4. Dashboard (dashboard/):
- React 18 + TypeScript + Vite + Recharts + @tanstack/react-table
- Pages: Overview (storage by team bar chart), Backend Detail (growth line chart), Stale Data (sortable table with bulk actions), User Detail, Quotas
- API calls to /api/v1/* endpoints
- Keep it simple and functional. No design framework needed.
- Build output goes to dashboard/dist/, served by control plane at /

### 5. Admin CLI (cmd/warpdrive-ctl/):
- Uses cobra for commands
- `warpdrive-ctl usage --group-by team` — calls GET /api/v1/usage, prints table
- `warpdrive-ctl stale --days 90 --min-size 10GB --format table|csv`
- `warpdrive-ctl quota set --team vision --backend s3 --soft 500TB --hard 600TB`
- `warpdrive-ctl quota list`
- `warpdrive-ctl status` — shows mount health, cache stats, backend connectivity
- All commands talk to control plane REST API (address from config or --control-plane flag)

## Database details
- Use github.com/lib/pq for PostgreSQL
- Run migrations on startup (embed SQL in Go with embed.FS)
- Aggregation: access_log is INSERT ON CONFLICT UPDATE (add to read_count, update last_access)
- Stale query joins storage_files with access_log to find files where MAX(last_access) < threshold

## Key constraints
- Telemetry collection must NOT slow down FUSE reads. Record() is non-blocking with bounded channel.
- Control plane handles 1000+ events/second from multiple nodes
- Dashboard loads in <3 seconds with 100K+ indexed files
- All REST endpoints return JSON with proper Content-Type
- CLI output is human-readable by default, --format csv for scripting
```

---

## M4 Exit Criteria

- [ ] Node agents emit telemetry to control plane
- [ ] Control plane ingests and aggregates events in PostgreSQL
- [ ] Storage crawler indexes all backends (files, sizes, modification times)
- [ ] `warpdrive-ctl usage --group-by team` shows storage by team across all backends
- [ ] `warpdrive-ctl stale --days 90` identifies stale files with >95% accuracy
- [ ] Dashboard shows overview page with storage by team bar chart
- [ ] Dashboard stale data table is sortable and supports bulk selection
- [ ] Quota soft limit triggers log warning
- [ ] Dashboard loads in <3 seconds
