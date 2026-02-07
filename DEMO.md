# WarpDrive — Live Demo Guide

> **Audience:** Engineering team
> **Duration:** ~15 minutes
> **Prerequisites:** macOS with [macFUSE](https://osxfuse.github.io/) installed, Go 1.22+

---

## Quick Setup

```bash
# Clone and build
cd ~/dev/warpdrive
make build

# Verify the binary
./bin/warpdrive-mount --help
```

---

## Demo 1 — Mount a Local Directory as a FUSE Filesystem

This shows the core capability: presenting any storage backend as a unified
POSIX mount with transparent block-level NVMe caching.

### 1. Prepare source data

```bash
# Create a "remote" data source (simulates a cloud bucket)
mkdir -p /tmp/warpdrive-demo-source/models/llama
mkdir -p /tmp/warpdrive-demo-source/datasets/imagenet

# Seed some files
echo "This is a pretrained model checkpoint" > /tmp/warpdrive-demo-source/models/llama/checkpoint.bin
dd if=/dev/urandom of=/tmp/warpdrive-demo-source/models/llama/weights.bin bs=1M count=32 2>/dev/null
echo "label1,image1.jpg" > /tmp/warpdrive-demo-source/datasets/imagenet/train.csv
echo '{"name":"imagenet","version":"2024.1"}' > /tmp/warpdrive-demo-source/datasets/imagenet/metadata.json
```

### 2. Write a config file

```bash
cat > /tmp/warpdrive-demo.yaml << 'EOF'
mount_point: /tmp/warpdrive-demo-mount

cache:
  path: /tmp/warpdrive-demo-cache
  max_size: 512MB
  block_size: 4MB

backends:
  - name: training-data
    type: local
    mount_path: /training
    config:
      root: /tmp/warpdrive-demo-source
EOF
```

### 3. Mount the filesystem

```bash
# Create mount point
mkdir -p /tmp/warpdrive-demo-mount

# Mount (runs in foreground — open a second terminal for browsing)
./bin/warpdrive-mount --config /tmp/warpdrive-demo.yaml
```

### 4. Browse the mounted filesystem (in a second terminal)

```bash
# Root shows the namespace mountpoints
ls /tmp/warpdrive-demo-mount/
# → training

# Drill into the backend
ls /tmp/warpdrive-demo-mount/training/
# → datasets  models

# Subdirectories work
ls /tmp/warpdrive-demo-mount/training/models/llama/
# → checkpoint.bin  weights.bin

# Read a file — goes through the cache layer
cat /tmp/warpdrive-demo-mount/training/datasets/imagenet/metadata.json
# → {"name":"imagenet","version":"2024.1"}

# File metadata is accurate
stat /tmp/warpdrive-demo-mount/training/models/llama/checkpoint.bin
# → shows correct size (38 bytes)

# Large file reads work — data integrity preserved
md5 /tmp/warpdrive-demo-source/models/llama/weights.bin
md5 /tmp/warpdrive-demo-mount/training/models/llama/weights.bin
# → both checksums match
```

### 5. Show read-only enforcement

```bash
# Writes are rejected (M1 is read-only by design for data safety)
echo "hack" > /tmp/warpdrive-demo-mount/training/test.txt
# → Read-only file system

touch /tmp/warpdrive-demo-mount/training/newfile
# → Read-only file system

rm /tmp/warpdrive-demo-mount/training/datasets/imagenet/train.csv
# → Read-only file system
```

### 6. Show caching in action

```bash
# First read of a large file — fetches from backend
time cat /tmp/warpdrive-demo-mount/training/models/llama/weights.bin > /dev/null

# Second read — served from NVMe cache (significantly faster)
time cat /tmp/warpdrive-demo-mount/training/models/llama/weights.bin > /dev/null

# Inspect the cache directory
ls /tmp/warpdrive-demo-cache/blocks/ | head
# → two-char prefix directories (fan-out for performance)

du -sh /tmp/warpdrive-demo-cache/
# → shows cached block data on disk
```

### 7. Unmount

```bash
# In the terminal running warpdrive-mount, press Ctrl+C
# Or from another terminal:
umount /tmp/warpdrive-demo-mount
```

---

## Demo 2 — Automated E2E Tests

This proves the full stack works end-to-end with race detection.

```bash
# Run the full e2e suite (verbose)
make test-e2e

# Expected output:
# --- PASS: TestE2E_ListRoot
# --- PASS: TestE2E_ListBackendDir
# --- PASS: TestE2E_ReadSmallFile
# --- PASS: TestE2E_ReadEmptyFile
# --- PASS: TestE2E_ReadNestedFile
# --- PASS: TestE2E_ReadLargeFile
# --- PASS: TestE2E_StatFile
# --- PASS: TestE2E_StatDir
# --- PASS: TestE2E_FileNotFound
# --- PASS: TestE2E_ReadOnlyRejectsWrite
# --- PASS: TestE2E_PartialRead
# --- PASS: TestE2E_ConcurrentReads
# --- PASS: TestE2E_MultiBackend_ListRoot
# --- PASS: TestE2E_MultiBackend_ReadAcrossBackends
# --- PASS: TestE2E_MultiBackend_AuthIsolation
# --- PASS: TestE2E_MultiBackend_CrossBackendConcurrentReads
# --- PASS: TestE2E_MultiBackend_FailureIsolation
# PASS
# ok  github.com/warpdrive/warpdrive/e2e  ~8s
```

### What each test proves

| Test | Feature | What it does |
|------|---------|-------------|
| `ListRoot` | Namespace layer | Verifies `/` lists configured mountpoints |
| `ListBackendDir` | Backend + List | Lists files in a backend directory |
| `ReadSmallFile` | Full read path | Reads 14 bytes through FUSE → cache → rclone → local FS |
| `ReadEmptyFile` | Edge case | Zero-byte files don't crash |
| `ReadNestedFile` | Directory traversal | Nested subdirectories resolve correctly |
| `ReadLargeFile` | Block cache | 8 MB random file, byte-for-byte integrity check |
| `StatFile` | Metadata | File size and type are accurate |
| `StatDir` | Metadata | Directory type is correct |
| `FileNotFound` | Error handling | Returns proper ENOENT |
| `ReadOnlyRejectsWrite` | Security | Write operations return EROFS |
| `PartialRead` | Offset reads | `ReadAt` with arbitrary offsets works |
| `ConcurrentReads` | Thread safety | 8 goroutines reading the same file in parallel |
| `MultiBackend_ListRoot` | Multi-backend namespace | Root lists all configured mount points |
| `MultiBackend_ReadAcrossBackends` | Cross-backend reads | Reads from two separate backends |
| `MultiBackend_AuthIsolation` | Per-backend auth | Audit trail captures per-backend auth events |
| `MultiBackend_CrossBackendConcurrentReads` | Concurrent multi-backend | 8 goroutines reading across two backends |
| `MultiBackend_FailureIsolation` | Backend isolation | One backend failing doesn't crash mount or affect others |

### Run unit tests too

```bash
# All tests (unit + e2e), with race detector
make test

# With coverage report
make test-cover
open coverage.html

# Run Go benchmarks for cache internals
make bench-go
# → BenchmarkCacheHit-10          1708    677248 ns/op   12386 MB/s
# → BenchmarkCacheHitSmallRead    4987    240099 ns/op      17 MB/s
# → BenchmarkCacheHitParallel-10  2335    503581 ns/op   16658 MB/s
# → BenchmarkCacheMiss-10          908   1490123 ns/op    2814 MB/s
# → BenchmarkCoalescerRecordRead  7264033   202.6 ns/op
# → BenchmarkCoalescerGetPattern  34607439   29.44 ns/op
# → BenchmarkReadaheadEnqueue     4682017   362.7 ns/op
# → BenchmarkStatWithCache        562591    1903 ns/op
# → BenchmarkBlockKey             14918808  72.95 ns/op
# → BenchmarkBlockLocalPath       4386950   263.1 ns/op
```

---

## Demo 3 — Multi-Backend Auth Framework (M2.3)

This demonstrates the auth framework that enables per-backend credential
management with support for Azure Entra ID, AWS STS, GCP Workload Identity,
Key Vault, static credentials, and no-auth.

### 1. Multi-backend config with auth

```bash
cat > /tmp/warpdrive-demo-multi.yaml << 'EOF'
mount_point: /tmp/warpdrive-demo-mount

cache:
  path: /tmp/warpdrive-demo-cache
  max_size: 512MB
  block_size: 4MB

backends:
  # Azure Blob with managed identity (on Azure VMs)
  - name: azure-models
    type: azureblob
    mount_path: /models
    config:
      container: training-models
    auth:
      method: managed_identity

  # S3 via OIDC federation (Entra JWT → AWS STS)
  - name: s3-datasets
    type: s3
    mount_path: /datasets
    config:
      bucket: ml-datasets
      region: us-east-1
    auth:
      method: oidc_federation
      role_arn: arn:aws:iam::123456789:role/warpdrive-s3-reader

  # GCS via workload identity federation
  - name: gcs-shared
    type: googlecloudstorage
    mount_path: /shared
    config:
      bucket: shared-data
    auth:
      method: gcp_wif
      project_number: "123456789"
      pool_id: azure-pool
      provider_id: azure-provider
      service_account_email: warpdrive@project.iam.gserviceaccount.com

  # Vast S3 via Key Vault stored credentials
  - name: vast-archive
    type: s3
    mount_path: /archive
    config:
      endpoint: https://vast.internal:443
      bucket: archive
    auth:
      method: static_keyvault
      vault_url: https://my-vault.vault.azure.net
      secret_name: vast-s3-creds

  # Local NFS — no auth needed
  - name: local-scratch
    type: local
    mount_path: /scratch
    config:
      root: /mnt/nfs/scratch
    auth:
      method: none
EOF
```

### 2. Auth method reference

| Method | Provider | Use Case |
|--------|----------|----------|
| `managed_identity` | Azure Entra ID (IMDS) | Azure VM → Azure Blob/ADLS |
| `service_principal` | Azure Entra ID (client creds) | CI/CD → Azure resources |
| `oidc_federation` | AWS STS | Azure VM → S3 (Entra JWT federated) |
| `gcp_wif` | GCP Workload Identity | Azure VM → GCS (3-step federation) |
| `static_keyvault` | Azure Key Vault | Azure VM → Vast/Nebius/NFS (rotated secrets) |
| `static` | Config file | Dev/test with inline credentials |
| `none` | No-op | Local filesystem, NFS |

### 3. Local multi-backend demo (no cloud credentials needed)

```bash
# Prepare two source directories
mkdir -p /tmp/warpdrive-source-models/llama
mkdir -p /tmp/warpdrive-source-datasets/imagenet
echo "model-checkpoint" > /tmp/warpdrive-source-models/llama/weights.bin
echo "training-labels" > /tmp/warpdrive-source-datasets/imagenet/labels.csv

# Write multi-backend config
cat > /tmp/warpdrive-demo-multi-local.yaml << 'EOF'
mount_point: /tmp/warpdrive-demo-mount

cache:
  path: /tmp/warpdrive-demo-cache
  max_size: 512MB
  block_size: 4MB

backends:
  - name: models
    type: local
    mount_path: /models
    config:
      root: /tmp/warpdrive-source-models
    auth:
      method: none

  - name: datasets
    type: local
    mount_path: /datasets
    config:
      root: /tmp/warpdrive-source-datasets
    auth:
      method: none
EOF

# Mount
mkdir -p /tmp/warpdrive-demo-mount
./bin/warpdrive-mount --config /tmp/warpdrive-demo-multi-local.yaml
```

### 4. Browse multi-backend mount (second terminal)

```bash
# Root shows both namespaces
ls /tmp/warpdrive-demo-mount/
# → datasets  models

# Read from models backend
cat /tmp/warpdrive-demo-mount/models/llama/weights.bin
# → model-checkpoint

# Read from datasets backend
cat /tmp/warpdrive-demo-mount/datasets/imagenet/labels.csv
# → training-labels

# Each backend has independent auth — audit trail tracks per-backend
```

### 5. Architecture: Auth flow diagram

```
  ┌──────────────┐
  │   FUSE layer  │
  └──────┬───────┘
         │
  ┌──────▼───────────────┐
  │ AuthenticatedBackend │ ◄── refreshIfNeeded() before each op
  └──────┬───────────────┘
         │
  ┌──────▼───────┐     ┌────────────────┐
  │  Auth Manager │────►│ Provider router │
  └──────────────┘     └───────┬────────┘
                               │
         ┌─────────┬───────────┼───────────┬──────────┐
         ▼         ▼           ▼           ▼          ▼
     ┌───────┐ ┌───────┐ ┌─────────┐ ┌─────────┐ ┌─────┐
     │ Entra │ │AWS STS│ │ GCP WIF │ │KeyVault │ │None │
     └───────┘ └───────┘ └─────────┘ └─────────┘ └─────┘
```

- **Per-backend credential isolation**: Each backend has its own auth config
- **Automatic refresh**: Tokens refreshed 5 min before expiry
- **Thundering herd prevention**: Double-check locking per backend
- **Audit trail**: Every credential operation logged with ring buffer

### 6. Cleanup

```bash
umount /tmp/warpdrive-demo-mount 2>/dev/null
rm -rf /tmp/warpdrive-demo-mount /tmp/warpdrive-demo-cache
rm -rf /tmp/warpdrive-source-models /tmp/warpdrive-source-datasets
rm -f /tmp/warpdrive-demo-multi-local.yaml
```

---

## Demo 4 — Architecture Walkthrough (Code Tour)

Use this to walk the team through the codebase layer by layer.

### Layer 1: Config (`pkg/config/`)

```
config.go   — Struct definitions: Config, CacheConfig, BackendConfig
loader.go   — YAML loader with ${ENV_VAR} expansion and ParseSize ("4MB" → 4194304)
```

**Key talking point:** Config supports human-readable sizes (`512MB`, `2TB`) and
env-var expansion for secrets in CI/CD.

### Layer 2: Backend (`pkg/backend/`)

```
backend.go         — Backend interface (List, Stat, ReadAt, Open, Write, Delete)
                     Registry for managing named backends
rclone.go          — RcloneBackend wraps rclone/rclone as a Go library (not CLI)
                     Supports: local, s3, azureblob, gcs via blank imports
authenticated.go   — AuthenticatedBackend wraps backends with credential refresh
```

**Key talking point:** We use rclone as a **Go library**, not shelling out.
Adding a new cloud is one blank import + config entry. Each backend is wrapped
with `AuthenticatedBackend` for automatic credential management.

### Layer 2.5: Auth (`pkg/auth/`)

```
auth.go      — Manager (credential cache, double-check locking, provider routing)
               Credentials, Provider interface, ProviderConfig
audit.go     — AuditLogger (ring buffer, slog output, optional external sink)
entra.go     — Azure Entra ID (managed_identity via IMDS, service_principal)
aws.go       — AWS STS (Entra JWT → AssumeRoleWithWebIdentity)
gcp.go       — GCP Workload Identity Federation (3-step: Entra → STS → SA)
keyvault.go  — Azure Key Vault (static creds for Vast, Nebius, NFS)
static.go    — Static credentials and NoneProvider for local/test
```

**Key talking points:**
- Per-backend credential isolation via `sync.Map` cache
- Thundering herd prevention: one refresh at a time per backend
- Token pre-refresh: 5 min before expiry to avoid on-path latency
- Full audit trail with ring buffer and structured logging
- Provider routing: auth method in config → correct provider automatically

### Layer 3: Cache (`pkg/cache/`)

```
cache.go      — CacheManager: block-level NVMe caching with singleflight dedup
block.go      — BlockMeta/FileMeta structs, key functions, SHA-256 path hashing
eviction.go   — LRU eviction loop (scans badger, sorts by last-access, evicts oldest)
readahead.go  — Prefetch worker pool (8 workers, 128-job queues, priority routing)
coalesce.go   — ReadCoalescer: per-file pattern detection (SEQUENTIAL/RANDOM/UNKNOWN)
warmup.go     — CacheManager.Warm(): parallel cache pre-population with resume
```

**Key talking points:**
- Files are split into 4 MB blocks → only cache what you read
- Singleflight dedup → 100 goroutines reading the same block = 1 backend fetch
- ETag-based invalidation → stale cache blocks are automatically purged
- ReadCoalescer tracks last 8 reads per file, classifies access patterns
- Readahead → sequential reads trigger background prefetch of next N blocks
- Dir-level readahead → files read in sorted order trigger prefetch of next files
- 256 MB inflight budget cap prevents cache thrashing
- Badger metadata DB → survives restarts, enables cache warming

### Layer 4: Namespace (`pkg/namespace/`)

```
namespace.go — Maps FUSE paths → (backend, remote-path)
               Longest-prefix-first matching for nested mounts
```

**Key talking point:** You can mount multiple backends at different paths:
```yaml
backends:
  - name: azure-models  → /models
  - name: s3-datasets   → /datasets
  - name: nfs-scratch   → /scratch
```

### Layer 5: FUSE (`pkg/fuse/`)

```
fs.go    — WarpDriveRoot, WarpDriveDir, WarpDriveFile inodes + WarpDriveFileHandle
mount.go — Mount config, go-fuse server lifecycle, context-based unmount
```

**Key talking points:**
- Read-only by design (M1) — `Setattr` returns EROFS, `Open` rejects write flags
- Directory listing cache with configurable TTL (5 min default)
- Entry/attr timeouts set to 60s to reduce FUSE round-trips
- `WarpDriveFileHandle.Read` delegates to CacheManager, which handles block boundaries
- `Readdir` calls `SetDirFiles` on the readahead manager, enabling dir-level prefetch
  of next files when shards are read in sorted order

---

## Demo 5 — M3: Cache Performance & Warming

This demo exercises the M3 cache performance features: optimized FUSE mount,
intelligent readahead, read coalescing, cache warming, and the benchmark tool.

### Prerequisites

```bash
# Build both binaries
make build
# → bin/warpdrive-mount and bin/warpdrive-bench
```

### 1. Create source data with sequential shards

```bash
# Create training-like shard files
SOURCE_DIR=/tmp/warpdrive-m3-source
mkdir -p $SOURCE_DIR
for i in $(seq -w 0 9); do
    dd if=/dev/urandom of=$SOURCE_DIR/shard-${i}.bin bs=1048576 count=4 2>/dev/null
done
echo "epoch=1, shards=10" > $SOURCE_DIR/manifest.json
ls -lh $SOURCE_DIR/
# → 10 × 4MB shards + manifest = ~40MB
```

### 2. Mount with optimized FUSE settings

```bash
cat > /tmp/warpdrive-m3.yaml << 'EOF'
mount_point: /tmp/warpdrive-m3-mount

cache:
  path: /tmp/warpdrive-m3-cache
  max_size: 512MB
  block_size: 4MB
  readahead_blocks: 4
  max_parallel_fetch: 8

backends:
  - name: training
    type: local
    mount_path: /data
    config:
      root: /tmp/warpdrive-m3-source
    auth:
      method: none
EOF

mkdir -p /tmp/warpdrive-m3-mount
./bin/warpdrive-mount --config /tmp/warpdrive-m3.yaml
```

### 3. Demonstrate sequential readahead (second terminal)

```bash
# First read — populates cache, readahead prefetches ahead
time cat /tmp/warpdrive-m3-mount/data/shard-00.bin > /dev/null
# → First read fetches from backend

# Second read — served from NVMe cache
time cat /tmp/warpdrive-m3-mount/data/shard-00.bin > /dev/null
# → Significantly faster (cache hit)

# Sequential reads trigger readahead — subsequent shards
# will partially be in cache already
for i in $(seq -w 0 9); do
    time cat /tmp/warpdrive-m3-mount/data/shard-${i}.bin > /dev/null
done
# → Later shards benefit from readahead prefetching
```

### 4. Run the benchmark tool

```bash
# Benchmark cached reads with 4 concurrent readers
./bin/warpdrive-bench \
    --dir /tmp/warpdrive-m3-mount/data \
    --readers 4 \
    --duration 10s \
    --chunk 4194304

# Output:
# WarpDrive Benchmark
# -----------------------------------
# Directory:  /tmp/warpdrive-m3-mount/data
# Files:      10
# Readers:    4
# Duration:   10s
# Chunk Size: 4.00 MB
# Checksum:   false
# -----------------------------------
#
# Results
# -----------------------------------
# Throughput: X.XX GB/s
# Operations: XXXX
# IOPS:       XXXX
# Latency:
#   P50: X.XX ms
#   P95: X.XX ms
#   P99: X.XX ms
```

### 5. Verify data integrity with checksums

```bash
# Benchmark with integrity checking
./bin/warpdrive-bench \
    --dir /tmp/warpdrive-m3-mount/data \
    --readers 2 \
    --duration 5s \
    --checksum

# Compare source vs cached checksums
md5 /tmp/warpdrive-m3-source/shard-00.bin
md5 /tmp/warpdrive-m3-mount/data/shard-00.bin
# → Checksums match
```

### 6. Architecture: M3 Cache Performance Stack

```
  ┌──────────────────────────┐
  │   FUSE layer (optimized) │  MaxReadAhead=1MB, MaxBackground=64
  └──────────┬───────────────┘
             │
  ┌──────────▼───────────────┐
  │  Read Coalescer          │  Detects SEQUENTIAL / RANDOM patterns
  └──────────┬───────────────┘
             │
  ┌──────────▼───────────────┐
  │  CacheManager (4MB blk)  │  Singleflight dedup, ETag validation
  └──────────┬───────────────┘
             │
  ┌──────────▼───────────────┐
  │  Readahead Manager       │  Per-file + dir-level prefetch
  │  ├─ 8 workers            │  Priority queue: file > dir
  │  ├─ 256MB budget cap     │  Budget enforcement
  │  └─ Pattern detection    │  Sequential → grow, Random → disable
  └──────────┬───────────────┘
             │
  ┌──────────▼───────────────┐
  │  NVMe Block Cache        │  LRU eviction, badger metadata
  └──────────┬───────────────┘
             │
  ┌──────────▼───────────────┐
  │  Backend (rclone)        │  local / s3 / azureblob / gcs
  └──────────────────────────┘
```

### 7. Cleanup

```bash
umount /tmp/warpdrive-m3-mount 2>/dev/null
rm -rf /tmp/warpdrive-m3-mount /tmp/warpdrive-m3-cache /tmp/warpdrive-m3-source
rm -f /tmp/warpdrive-m3.yaml
```

---

## Demo 6 — M3: Cache Warming CLI (warpdrive-ctl)

The `warpdrive-ctl` CLI provides operational cache management — pre-populating
the NVMe cache before training starts, so the first epoch reads are fast.
Supports resume: re-running the same command skips already-cached files.

### 1. Prepare source data and config

```bash
# Create source data
mkdir -p /tmp/warpdrive-ctl-source
for i in $(seq -w 0 4); do
    dd if=/dev/urandom of=/tmp/warpdrive-ctl-source/shard-${i}.bin bs=1M count=4 2>/dev/null
done
echo '{"epoch": 1, "shards": 5}' > /tmp/warpdrive-ctl-source/manifest.json

# Write config
cat > /tmp/warpdrive-ctl.yaml << 'EOF'
mount_point: /tmp/warpdrive-ctl-mount

cache:
  path: /tmp/warpdrive-ctl-cache
  max_size: 512MB
  block_size: 4MB

backends:
  - name: training
    type: local
    mount_path: /data
    config:
      root: /tmp/warpdrive-ctl-source
    auth:
      method: none
EOF
```

### 2. Warm the cache

```bash
# Pre-populate cache for the entire training backend
./bin/warpdrive-ctl warm \
    --config /tmp/warpdrive-ctl.yaml \
    --backend training \
    --recursive \
    --workers 8

# Output:
# WarpDrive Cache Warming
# ────────────────────────────────────
# Backend:    training
# Prefix:     /
# Recursive:  true
# Max Size:   unlimited
# Workers:    8
# ────────────────────────────────────
#
# 5/6 files | 20.00 MB/20.00 MB (100.0%) | 450.0 MB/s | ETA --
#
# Done: Cache warming complete in 45ms
```

### 3. Re-run (resume support)

```bash
# Re-running skips already-cached files
./bin/warpdrive-ctl warm \
    --config /tmp/warpdrive-ctl.yaml \
    --backend training \
    --recursive

# Notice log: "skipped_cached=6" — all files already in cache
```

### 4. Warm with a size limit

```bash
# Only warm up to 10MB
./bin/warpdrive-ctl warm \
    --config /tmp/warpdrive-ctl.yaml \
    --backend training \
    --recursive \
    --max-size 10MB
```

### 5. Check cache stats

```bash
./bin/warpdrive-ctl stats --config /tmp/warpdrive-ctl.yaml

# WarpDrive Cache Statistics
# ────────────────────────────────────
# Hits:         0
# Misses:       12
# Evictions:    0
# Bytes Cached: 20.00 MB
# ────────────────────────────────────
```

### 6. K8s and Slurm integration

Cache warming integrates into production workflows:

**K8s Init Container** (`deploy/examples/warm-init-container.yaml`):
```yaml
initContainers:
  - name: warpdrive-warm
    image: ghcr.io/warpdrive/warpdrive-ctl:latest
    command:
      - warpdrive-ctl
      - warm
      - --config
      - /etc/warpdrive/config.yaml
      - --backend
      - training-data
      - --prefix
      - datasets/imagenet
      - --max-size
      - "500GB"
```

**Slurm Prolog** (`deploy/slurm/warpdrive-prolog.sh`):
```bash
# Set in job submission:
sbatch --export=WARPDRIVE_WARMUP_BACKEND=training-data,WARPDRIVE_WARMUP_PREFIX=datasets/imagenet job.sh
```

### 7. Cleanup

```bash
rm -rf /tmp/warpdrive-ctl-source /tmp/warpdrive-ctl-cache /tmp/warpdrive-ctl-mount
rm -f /tmp/warpdrive-ctl.yaml
```

---

## Demo Transcript

Below is a sample script you can follow when presenting to the team.

---

**[SLIDE: Title]**

> _"Today I'm going to demo WarpDrive — our cross-cloud data fabric for GPU
> training workloads. The goal is to present a single POSIX mount point that
> unifies Azure Blob, S3, GCS, Vast, NFS — whatever storage backends our
> training clusters need — with intelligent NVMe caching underneath."_

**[TERMINAL: Show the config]**

```bash
cat /tmp/warpdrive-demo.yaml
```

> _"Here's a minimal config. We define a mount point, configure the cache —
> 512 MB, 4 MB blocks — and list our backends. Right now I'm using a local
> directory to simulate a cloud bucket. In production, you'd change `type` to
> `s3` or `azureblob` and add credentials."_

**[TERMINAL: Mount the filesystem]**

```bash
./bin/warpdrive-mount --config /tmp/warpdrive-demo.yaml
```

> _"The mount agent starts up. It creates an rclone backend, opens a badger
> metadata DB for cache tracking, spins up the eviction loop and readahead
> workers, then mounts the FUSE filesystem. Let me switch to another terminal."_

**[TERMINAL 2: Browse the mount]**

```bash
ls /tmp/warpdrive-demo-mount/
```

> _"The root shows `training` — that's our namespace mount point. Everything
> under `/training` maps to the backend we configured."_

```bash
ls -la /tmp/warpdrive-demo-mount/training/models/llama/
```

> _"We see the model files. `weights.bin` is 32 MB. Let's read it."_

```bash
time cat /tmp/warpdrive-demo-mount/training/models/llama/weights.bin > /dev/null
```

> _"First read: the cache manager splits this into 4 MB blocks, fetches each
> from the backend, writes them to the local cache directory, and stores
> metadata in badger. Watch what happens on the second read."_

```bash
time cat /tmp/warpdrive-demo-mount/training/models/llama/weights.bin > /dev/null
```

> _"Significantly faster — those blocks are now served directly from local
> NVMe. No backend round-trip. In production with real NVMe drives and remote
> cloud storage, you'd see the difference go from seconds to microseconds."_

**[TERMINAL 2: Show integrity]**

```bash
md5 /tmp/warpdrive-demo-source/models/llama/weights.bin
md5 /tmp/warpdrive-demo-mount/training/models/llama/weights.bin
```

> _"Checksums match. Byte-for-byte integrity through the entire stack: FUSE →
> cache → rclone → source."_

**[TERMINAL 2: Show read-only safety]**

```bash
echo "oops" > /tmp/warpdrive-demo-mount/training/test.txt
```

> _"Write rejected. M1 is intentionally read-only. Training data is immutable.
> We don't want a rogue process corrupting a shared dataset."_

**[TERMINAL 2: Show ENOENT handling]**

```bash
cat /tmp/warpdrive-demo-mount/training/nonexistent.txt
```

> _"Proper POSIX error handling. No crash, no hang — just ENOENT."_

**[TERMINAL 2: Run the tests]**

```bash
make test-e2e
```

> _"And here's the automated proof. 17 e2e tests run with the race detector
> enabled. Each test boots the full stack — real FUSE mount, real cache, real
> backend — against temp directories. The concurrent reads test fires 8
> goroutines reading the same 8 MB file simultaneously. The multi-backend tests
> verify auth isolation, cross-backend concurrent reads, and backend failure
> isolation. All pass in about 8 seconds."_

**[SLIDE: Architecture diagram]**

> _"Let me walk through the layers. At the top we have the FUSE interface —
> go-fuse v2. Below that, the namespace layer maps paths to backends. The
> cache layer splits files into 4 MB blocks with singleflight dedup,
> readahead, and LRU eviction. At the bottom, rclone gives us a uniform
> interface to any cloud storage."_

> _"We use rclone as a Go library, not a CLI. Adding a new cloud is literally
> one import line and a config entry. No new bindings, no FFI."_

**[SLIDE: What's next]**

> _"M1 is the core mount. M2.3 is now complete — we have a full auth framework
> with per-backend credential management supporting Azure Entra ID, AWS STS
> federation, GCP Workload Identity, Key Vault-stored secrets, and static
> credentials. Each backend gets its own auth context with automatic refresh
> and audit logging. M3 is complete — we have optimized FUSE mount options
> (1MB readahead, 64 background requests), intelligent per-file and
> directory-level readahead with budget caps, read coalescing for pattern
> detection, cache warming with resume support, and a warpdrive-bench tool for
> performance validation. M4 is complete — governance, usage attribution,
> stale data detection, quotas, and telemetry. Coming up: M5 — Helm chart,
> systemd units, Slurm integration."_

> _"Questions?"_

---

## Demo 7: M4 — Governance, Telemetry & Control Plane

> _"Finally, M4 adds the governance layer. This is what turns a fast mount
> into a managed platform. We get telemetry collection on every file access,
> a control plane for usage analytics, stale data detection, and quota
> management."_

### 7.1 Telemetry

> _"Every file read, stat, and directory listing is recorded as an AccessEvent.
> Events flow through a Collector that batches them and flushes to a
> configurable sink — stdout (for K8s log aggregation), file (JSONL), or
> directly to the control plane."_

**Config (add to your config.yaml):**

```yaml
telemetry:
  enabled: true
  sink: "file"
  file_path: "/tmp/warpdrive-telemetry.jsonl"
  sample_metadata_ops: 0.1    # Sample 10% of stat/list ops
  batch_size: 100
  flush_interval: 5s
```

**Verify telemetry is collected:**

```bash
# After running some reads through the mount:
cat /tmp/warpdrive-telemetry.jsonl | head -5
```

> _"Each line is a JSON event with timestamp, user, backend, path, operation,
> bytes read, cache hit status, and latency. This is the raw data that feeds
> the governance layer."_

### 7.2 Usage Attribution

> _"The warpdrive-ctl usage command shows storage usage grouped by team or user.
> It crawls all backends to index files, then correlates access records."_

```bash
warpdrive-ctl usage --config config.yaml --group-by team --format table
warpdrive-ctl usage --config config.yaml --group-by user --backend s3 --format csv
```

### 7.3 Stale Data Detection

> _"The stale command finds files that haven't been accessed in N days.
> This is how you identify datasets that nobody is using — candidates for
> archival or deletion."_

```bash
warpdrive-ctl stale --config config.yaml --days 90 --min-size 10GB
```

### 7.4 Quota Management

> _"Quotas let you set soft and hard limits per team per backend.
> Soft limits log warnings; hard limits could block writes in a future
> read-write version."_

```bash
# Set a quota
warpdrive-ctl quota set --config config.yaml --team ml-training --backend s3 --soft 500TB --hard 600TB

# List all quotas with current usage
warpdrive-ctl quota list --config config.yaml
```

### 7.5 System Status

> _"The status command gives a single-pane view of the mount, cache health,
> and telemetry status."_

```bash
warpdrive-ctl status --config config.yaml
```

### 7.6 Cross-Backend Move

> _"The move command copies data between backends — useful for archiving stale
> data to cheaper storage."_

```bash
# Copy a file from S3 to Azure (keeps source)
warpdrive-ctl move --config config.yaml --src s3:datasets/old.tar --dst azure:archive/old.tar

# True move: copy + delete source
warpdrive-ctl move --config config.yaml --src s3:stale/model_v1.bin --dst gcs:archive/model_v1.bin --delete
```

### 7.7 Control Plane Server

> _"The serve command starts the control plane as a long-running HTTP service.
> Node agents send telemetry via HTTP, and the dashboard talks to the REST API."_

```bash
# Start the control plane (blocks until Ctrl+C)
warpdrive-ctl serve --config config.yaml --addr :8080
```

### 7.8 REST API

> _"All governance data is exposed via a REST API for dashboard integration."_

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/usage` | GET | Usage by team or user |
| `/api/v1/stale` | GET | Stale file detection |
| `/api/v1/growth` | GET | Storage growth over time |
| `/api/v1/quota` | GET/PUT | Quota management |
| `/api/v1/user/{id}` | GET | Per-user access details |
| `/api/v1/backends` | GET | Backend statistics |
| `/api/v1/ingest` | POST | Telemetry event ingestion |
| `/api/v1/crawl` | POST | Trigger storage crawl |
| `/api/v1/team` | POST | Set user-to-team mapping |

### 7.9 Run the M4 tests

```bash
go test ./pkg/telemetry/ -v
go test ./pkg/control/ -v
go test ./e2e/ -v -run "TestM4"
```

> _"17 telemetry tests, 40 control plane tests, 6 M4 e2e tests — all passing.
> The control plane tests include full API handler coverage with httptest,
> HTTP emitter end-to-end, Server.Run lifecycle, and a full API workflow test."_

---

## Cleanup

```bash
# Remove demo artifacts
umount /tmp/warpdrive-demo-mount 2>/dev/null
rm -rf /tmp/warpdrive-demo-mount /tmp/warpdrive-demo-cache /tmp/warpdrive-demo-source /tmp/warpdrive-demo.yaml
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `mount: No such file or directory` | Install macFUSE: `brew install --cask macfuse` |
| `mount failed: operation not permitted` | System Preferences → Security → Allow macFUSE kernel extension |
| `mount point busy` | `umount -f /tmp/warpdrive-demo-mount` or `diskutil unmount force /tmp/warpdrive-demo-mount` |
| `go test` hangs | A previous mount wasn't cleaned up: `pkill warpdrive-mount && umount -f /tmp/warpdrive-demo-mount` |
| Tests fail with `too many open files` | `ulimit -n 4096` in your shell |
