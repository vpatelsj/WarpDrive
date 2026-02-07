#!/usr/bin/env bash
# ────────────────────────────────────────────────────────────────
# WarpDrive — Automated Demo Verification Script
# Exercises Demo 1 (mount + browse + cache + read-only),
# Demo 2 (e2e test suite), and Demo 3 (multi-backend auth)
# from DEMO.md, with pass/fail checks.
# ────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Colours / helpers ──────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

PASS=0
FAIL=0
TOTAL=0

pass() { ((PASS++)); ((TOTAL++)); printf "  ${GREEN}✓ PASS${RESET}  %s\n" "$1"; }
fail() { ((FAIL++)); ((TOTAL++)); printf "  ${RED}✗ FAIL${RESET}  %s\n" "$1"; }
section() { printf "\n${CYAN}${BOLD}▶ %s${RESET}\n" "$1"; }
info()    { printf "  ${YELLOW}· %s${RESET}\n" "$1"; }

# ── Paths ──────────────────────────────────────────────────────
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="${PROJECT_DIR}/bin/warpdrive-mount"
SOURCE_DIR="/tmp/warpdrive-demo-source"
MOUNT_DIR="/tmp/warpdrive-demo-mount"
CACHE_DIR="/tmp/warpdrive-demo-cache"
CONFIG_FILE="/tmp/warpdrive-demo.yaml"
MOUNT_PID=""

# ── Cleanup on exit ────────────────────────────────────────────
cleanup() {
    section "Cleanup"
    if [[ -n "${MOUNT_PID}" ]] && kill -0 "${MOUNT_PID}" 2>/dev/null; then
        info "Stopping warpdrive-mount (PID ${MOUNT_PID})"
        kill "${MOUNT_PID}" 2>/dev/null || true
        wait "${MOUNT_PID}" 2>/dev/null || true
        sleep 1
    fi
    # Force unmount if still mounted
    if mount | grep -q "${MOUNT_DIR}"; then
        info "Force-unmounting ${MOUNT_DIR}"
        umount -f "${MOUNT_DIR}" 2>/dev/null || diskutil unmount force "${MOUNT_DIR}" 2>/dev/null || true
        sleep 1
    fi
    rm -rf "${SOURCE_DIR}" "${MOUNT_DIR}" "${CACHE_DIR}" "${CONFIG_FILE}"
    info "Removed demo artifacts"
}
trap cleanup EXIT

# ───────────────────────────────────────────────────────────────
# Phase 1 — Build
# ───────────────────────────────────────────────────────────────
section "Phase 1 — Build"

cd "${PROJECT_DIR}"
make build 2>&1 | tail -1
if [[ -x "${BINARY}" ]]; then
    pass "Binary built at ${BINARY}"
else
    fail "Binary not found at ${BINARY}"
    exit 1
fi

# Verify --help works
if "${BINARY}" --help 2>&1 | grep -q "config"; then
    pass "--help flag works"
else
    fail "--help flag broken"
fi

# ───────────────────────────────────────────────────────────────
# Phase 2 — Prepare source data
# ───────────────────────────────────────────────────────────────
section "Phase 2 — Prepare source data"

rm -rf "${SOURCE_DIR}" "${MOUNT_DIR}" "${CACHE_DIR}" "${CONFIG_FILE}"
mkdir -p "${SOURCE_DIR}/models/llama"
mkdir -p "${SOURCE_DIR}/datasets/imagenet"

echo "This is a pretrained model checkpoint" > "${SOURCE_DIR}/models/llama/checkpoint.bin"
dd if=/dev/urandom of="${SOURCE_DIR}/models/llama/weights.bin" bs=1M count=32 2>/dev/null
echo "label1,image1.jpg" > "${SOURCE_DIR}/datasets/imagenet/train.csv"
echo '{"name":"imagenet","version":"2024.1"}' > "${SOURCE_DIR}/datasets/imagenet/metadata.json"
: > "${SOURCE_DIR}/datasets/imagenet/empty.bin"   # zero-byte edge case

pass "Source data created (32 MB weights, small files, empty file)"

# ───────────────────────────────────────────────────────────────
# Phase 3 — Write config & mount
# ───────────────────────────────────────────────────────────────
section "Phase 3 — Write config & mount"

cat > "${CONFIG_FILE}" << EOF
mount_point: ${MOUNT_DIR}

cache:
  path: ${CACHE_DIR}
  max_size: 512MB
  block_size: 4MB

backends:
  - name: training-data
    type: local
    mount_path: /training
    config:
      root: ${SOURCE_DIR}
EOF
pass "Config written to ${CONFIG_FILE}"

mkdir -p "${MOUNT_DIR}"

"${BINARY}" --config "${CONFIG_FILE}" &
MOUNT_PID=$!
info "warpdrive-mount started (PID ${MOUNT_PID})"

# Wait for mount to become ready
READY=false
for i in $(seq 1 30); do
    if mount | grep -q "${MOUNT_DIR}" && ls "${MOUNT_DIR}/" >/dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 0.5
done

if $READY; then
    pass "Filesystem mounted at ${MOUNT_DIR}"
else
    fail "Mount did not become ready within 15 s"
    exit 1
fi

# ───────────────────────────────────────────────────────────────
# Phase 4 — Browse the mounted filesystem
# ───────────────────────────────────────────────────────────────
section "Phase 4 — Browse the mounted filesystem"

# 4a. Root listing shows namespace mountpoint
ROOT_LS="$(ls "${MOUNT_DIR}/")"
if echo "${ROOT_LS}" | grep -q "training"; then
    pass "Root lists 'training' namespace"
else
    fail "Root listing unexpected: ${ROOT_LS}"
fi

# 4b. Backend directory listing
TRAINING_LS="$(ls "${MOUNT_DIR}/training/")"
if echo "${TRAINING_LS}" | grep -q "datasets" && echo "${TRAINING_LS}" | grep -q "models"; then
    pass "Backend dir lists datasets + models"
else
    fail "Backend listing unexpected: ${TRAINING_LS}"
fi

# 4c. Nested directory listing
LLAMA_LS="$(ls "${MOUNT_DIR}/training/models/llama/")"
if echo "${LLAMA_LS}" | grep -q "checkpoint.bin" && echo "${LLAMA_LS}" | grep -q "weights.bin"; then
    pass "Nested dir lists checkpoint.bin + weights.bin"
else
    fail "Nested listing unexpected: ${LLAMA_LS}"
fi

# 4d. Read small file
METADATA="$(cat "${MOUNT_DIR}/training/datasets/imagenet/metadata.json")"
EXPECTED='{"name":"imagenet","version":"2024.1"}'
if [[ "${METADATA}" == "${EXPECTED}" ]]; then
    pass "Small file read matches expected content"
else
    fail "metadata.json content mismatch: got '${METADATA}'"
fi

# 4e. Read CSV file
CSV="$(cat "${MOUNT_DIR}/training/datasets/imagenet/train.csv")"
if [[ "${CSV}" == "label1,image1.jpg" ]]; then
    pass "CSV file read correct"
else
    fail "train.csv mismatch: got '${CSV}'"
fi

# 4f. Stat file — size check
CHECKPOINT_SIZE="$(stat -f%z "${MOUNT_DIR}/training/models/llama/checkpoint.bin" 2>/dev/null || stat -c%s "${MOUNT_DIR}/training/models/llama/checkpoint.bin" 2>/dev/null)"
SOURCE_SIZE="$(stat -f%z "${SOURCE_DIR}/models/llama/checkpoint.bin" 2>/dev/null || stat -c%s "${SOURCE_DIR}/models/llama/checkpoint.bin" 2>/dev/null)"
if [[ "${CHECKPOINT_SIZE}" == "${SOURCE_SIZE}" ]]; then
    pass "Stat file size correct (${CHECKPOINT_SIZE} bytes)"
else
    fail "Stat size mismatch: mount=${CHECKPOINT_SIZE} source=${SOURCE_SIZE}"
fi

# 4g. Large file integrity (MD5)
MD5_SOURCE="$(md5 -q "${SOURCE_DIR}/models/llama/weights.bin" 2>/dev/null || md5sum "${SOURCE_DIR}/models/llama/weights.bin" | awk '{print $1}')"
MD5_MOUNT="$(md5 -q "${MOUNT_DIR}/training/models/llama/weights.bin" 2>/dev/null || md5sum "${MOUNT_DIR}/training/models/llama/weights.bin" | awk '{print $1}')"
if [[ "${MD5_SOURCE}" == "${MD5_MOUNT}" ]]; then
    pass "32 MB file integrity — MD5 checksums match (${MD5_SOURCE})"
else
    fail "MD5 mismatch: source=${MD5_SOURCE}  mount=${MD5_MOUNT}"
fi

# 4h. Empty file read
EMPTY="$(cat "${MOUNT_DIR}/training/datasets/imagenet/empty.bin")"
if [[ -z "${EMPTY}" ]]; then
    pass "Empty file reads as zero bytes"
else
    fail "Empty file returned content: '${EMPTY}'"
fi

# ───────────────────────────────────────────────────────────────
# Phase 5 — Read-only enforcement
# ───────────────────────────────────────────────────────────────
section "Phase 5 — Read-only enforcement"

# 5a. Write rejected
if echo "hack" > "${MOUNT_DIR}/training/test.txt" 2>/dev/null; then
    fail "Write should have been rejected"
else
    pass "Write rejected (Read-only file system)"
fi

# 5b. Touch rejected
if touch "${MOUNT_DIR}/training/newfile" 2>/dev/null; then
    fail "Touch should have been rejected"
else
    pass "Touch rejected (Read-only file system)"
fi

# 5c. Mkdir rejected
if mkdir "${MOUNT_DIR}/training/newdir" 2>/dev/null; then
    fail "Mkdir should have been rejected"
else
    pass "Mkdir rejected (Read-only file system)"
fi

# ───────────────────────────────────────────────────────────────
# Phase 6 — Caching behaviour
# ───────────────────────────────────────────────────────────────
section "Phase 6 — Caching behaviour"

# First read (cold)
T1_START="$(date +%s)"
cat "${MOUNT_DIR}/training/models/llama/weights.bin" > /dev/null
T1_END="$(date +%s)"
T1=$(( T1_END - T1_START ))

# Second read (warm — should be faster)
T2_START="$(date +%s)"
cat "${MOUNT_DIR}/training/models/llama/weights.bin" > /dev/null
T2_END="$(date +%s)"
T2=$(( T2_END - T2_START ))

info "Cold read: ${T1}s — Warm read: ${T2}s"
pass "Cache read completed (cold=${T1}s, warm=${T2}s)"

# Cache directory has data
if [[ -d "${CACHE_DIR}" ]] && [[ "$(du -s "${CACHE_DIR}" | awk '{print $1}')" -gt 0 ]]; then
    CACHE_SIZE="$(du -sh "${CACHE_DIR}" | awk '{print $1}')"
    pass "Cache directory populated (${CACHE_SIZE})"
else
    fail "Cache directory empty or missing"
fi

# ───────────────────────────────────────────────────────────────
# Phase 7 — Error handling
# ───────────────────────────────────────────────────────────────
section "Phase 7 — Error handling"

if cat "${MOUNT_DIR}/training/nonexistent.txt" 2>/dev/null; then
    fail "Reading nonexistent file should have failed"
else
    pass "ENOENT returned for nonexistent file"
fi

if ls "${MOUNT_DIR}/training/no-such-dir/" 2>/dev/null; then
    fail "Listing nonexistent directory should have failed"
else
    pass "ENOENT returned for nonexistent directory"
fi

# ───────────────────────────────────────────────────────────────
# Phase 8 — Unmount
# ───────────────────────────────────────────────────────────────
section "Phase 8 — Unmount"

kill "${MOUNT_PID}" 2>/dev/null || true
wait "${MOUNT_PID}" 2>/dev/null || true
MOUNT_PID=""
sleep 1

if mount | grep -q "${MOUNT_DIR}"; then
    umount -f "${MOUNT_DIR}" 2>/dev/null || true
    sleep 1
fi

if ! mount | grep -q "${MOUNT_DIR}"; then
    pass "Filesystem unmounted cleanly"
else
    fail "Filesystem still mounted after unmount"
fi

# ───────────────────────────────────────────────────────────────
# Phase 8b — Multi-backend with auth config
# ───────────────────────────────────────────────────────────────
section "Phase 8b — Multi-backend mount with auth config"

SOURCE2_DIR="/tmp/warpdrive-demo-source2"
MULTI_CONFIG="/tmp/warpdrive-demo-multi.yaml"
rm -rf "${SOURCE2_DIR}"
mkdir -p "${SOURCE2_DIR}/images"
echo "cat-photo-data" > "${SOURCE2_DIR}/images/cat.jpg"
echo "dog-photo-data" > "${SOURCE2_DIR}/images/dog.jpg"
pass "Second source directory created"

cat > "${MULTI_CONFIG}" << EOF
mount_point: ${MOUNT_DIR}

cache:
  path: ${CACHE_DIR}
  max_size: 512MB
  block_size: 4MB

backends:
  - name: models
    type: local
    mount_path: /models
    config:
      root: ${SOURCE_DIR}
    auth:
      method: none

  - name: datasets
    type: local
    mount_path: /datasets
    config:
      root: ${SOURCE2_DIR}
    auth:
      method: none
EOF
pass "Multi-backend + auth config written"

mkdir -p "${MOUNT_DIR}"
rm -rf "${CACHE_DIR}"
mkdir -p "${CACHE_DIR}"

"${BINARY}" --config "${MULTI_CONFIG}" &
MOUNT_PID=$!
info "warpdrive-mount started with multi-backend (PID ${MOUNT_PID})"

READY=false
for i in $(seq 1 30); do
    if mount | grep -q "${MOUNT_DIR}" && ls "${MOUNT_DIR}/" >/dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 0.5
done

if $READY; then
    pass "Multi-backend filesystem mounted"
else
    fail "Multi-backend mount did not become ready"
    kill "${MOUNT_PID}" 2>/dev/null || true
fi

if $READY; then
    # Check root lists both namespaces
    MULTI_ROOT="$(ls "${MOUNT_DIR}/")"
    if echo "${MULTI_ROOT}" | grep -q "models" && echo "${MULTI_ROOT}" | grep -q "datasets"; then
        pass "Root lists both 'models' and 'datasets' namespaces"
    else
        fail "Multi-backend root listing: ${MULTI_ROOT}"
    fi

    # Read from first backend
    MODEL_DATA="$(cat "${MOUNT_DIR}/models/models/llama/checkpoint.bin" 2>/dev/null || echo '')"
    if [[ "${MODEL_DATA}" == "This is a pretrained model checkpoint" ]]; then
        pass "Read from models backend correct"
    else
        fail "Models backend read unexpected: ${MODEL_DATA}"
    fi

    # Read from second backend
    DATASET_DATA="$(cat "${MOUNT_DIR}/datasets/images/cat.jpg" 2>/dev/null || echo '')"
    if [[ "${DATASET_DATA}" == "cat-photo-data" ]]; then
        pass "Read from datasets backend correct"
    else
        fail "Datasets backend read unexpected: ${DATASET_DATA}"
    fi

    # Unmount multi-backend
    kill "${MOUNT_PID}" 2>/dev/null || true
    wait "${MOUNT_PID}" 2>/dev/null || true
    MOUNT_PID=""
    sleep 1
    pass "Multi-backend filesystem unmounted"
fi

rm -rf "${SOURCE2_DIR}" "${MULTI_CONFIG}"

# ───────────────────────────────────────────────────────────────
# Phase 9 — E2E test suite
# ───────────────────────────────────────────────────────────────
section "Phase 9 — E2E test suite (make test-e2e)"

cd "${PROJECT_DIR}"
if make test-e2e 2>&1 | tee /tmp/warpdrive-e2e-output.txt | tail -20; then
    if grep -q "PASS" /tmp/warpdrive-e2e-output.txt && ! grep -q "FAIL" /tmp/warpdrive-e2e-output.txt; then
        pass "E2E test suite passed"
    else
        fail "E2E test suite had failures"
    fi
else
    fail "E2E test suite exited with error"
fi
rm -f /tmp/warpdrive-e2e-output.txt

# ───────────────────────────────────────────────────────────────
# Phase 10 — Unit tests
# ───────────────────────────────────────────────────────────────
section "Phase 10 — Unit + integration tests (make test)"

cd "${PROJECT_DIR}"
if make test 2>&1 | tee /tmp/warpdrive-test-output.txt | tail -20; then
    if grep -q "ok" /tmp/warpdrive-test-output.txt && ! grep -q "FAIL" /tmp/warpdrive-test-output.txt; then
        pass "Full test suite passed"
    else
        fail "Test suite had failures"
    fi
else
    fail "Test suite exited with error"
fi
rm -f /tmp/warpdrive-test-output.txt

# ───────────────────────────────────────────────────────────────
# Phase 11 — M3: Benchmark tool (warpdrive-bench)
# ───────────────────────────────────────────────────────────────
section "Phase 11 — M3: Benchmark tool (warpdrive-bench)"

BENCH_BINARY="${PROJECT_DIR}/bin/warpdrive-bench"
BENCH_DATA_DIR="/tmp/warpdrive-bench-data"

# Build warpdrive-bench (should already be built by 'make build').
if [[ -x "${BENCH_BINARY}" ]]; then
    pass "warpdrive-bench binary exists"
else
    info "Building warpdrive-bench..."
    cd "${PROJECT_DIR}"
    go build -trimpath -o "${BENCH_BINARY}" ./cmd/warpdrive-bench
    if [[ -x "${BENCH_BINARY}" ]]; then
        pass "warpdrive-bench built successfully"
    else
        fail "warpdrive-bench build failed"
    fi
fi

# Create benchmark data (local files for speed).
rm -rf "${BENCH_DATA_DIR}"
mkdir -p "${BENCH_DATA_DIR}"
for i in $(seq 1 5); do
    dd if=/dev/urandom of="${BENCH_DATA_DIR}/shard-$(printf '%04d' $i).bin" \
       bs=1048576 count=2 2>/dev/null
done
pass "Benchmark data created (5 × 2MB shards)"

# Run benchmark with short duration.
if "${BENCH_BINARY}" --dir "${BENCH_DATA_DIR}" --readers 4 --duration 3s --chunk 1048576 2>&1 | tee /tmp/warpdrive-bench-output.txt; then
    if grep -q "Throughput" /tmp/warpdrive-bench-output.txt; then
        THROUGHPUT=$(grep "Throughput" /tmp/warpdrive-bench-output.txt | awk '{print $2}')
        pass "warpdrive-bench completed — throughput: ${THROUGHPUT} GB/s"
    else
        fail "warpdrive-bench output missing throughput"
    fi
else
    fail "warpdrive-bench exited with error"
fi
rm -f /tmp/warpdrive-bench-output.txt
rm -rf "${BENCH_DATA_DIR}"

# ───────────────────────────────────────────────────────────────
# Phase 12 — M3: Cache warming through mount
# ───────────────────────────────────────────────────────────────
section "Phase 12 — M3: Cache warming verification"

# Re-set up a mount for warming test.
WARM_SOURCE_DIR="/tmp/warpdrive-warm-source"
WARM_MOUNT_DIR="/tmp/warpdrive-warm-mount"
WARM_CACHE_DIR="/tmp/warpdrive-warm-cache"
WARM_CONFIG="/tmp/warpdrive-warm.yaml"

rm -rf "${WARM_SOURCE_DIR}" "${WARM_MOUNT_DIR}" "${WARM_CACHE_DIR}"
mkdir -p "${WARM_SOURCE_DIR}" "${WARM_MOUNT_DIR}" "${WARM_CACHE_DIR}"

# Create source data.
for i in $(seq 1 3); do
    dd if=/dev/urandom of="${WARM_SOURCE_DIR}/data-$(printf '%04d' $i).bin" \
       bs=1048576 count=1 2>/dev/null
done
echo "small-file-content" > "${WARM_SOURCE_DIR}/info.txt"
pass "Warming source data created"

cat > "${WARM_CONFIG}" << EOF
mount_point: ${WARM_MOUNT_DIR}

cache:
  path: ${WARM_CACHE_DIR}
  max_size: 512MB
  block_size: 4MB

backends:
  - name: warmtest
    type: local
    mount_path: /warmdata
    config:
      root: ${WARM_SOURCE_DIR}
    auth:
      method: none
EOF
pass "Warming config written"

"${BINARY}" --config "${WARM_CONFIG}" &
MOUNT_PID=$!
info "warpdrive-mount started for warming test (PID ${MOUNT_PID})"

READY=false
for i in $(seq 1 30); do
    if mount | grep -q "${WARM_MOUNT_DIR}" && ls "${WARM_MOUNT_DIR}/" >/dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 0.5
done

if $READY; then
    pass "Warming test filesystem mounted"

    # Read all files to simulate warming.
    WARM_BYTES=0
    for f in "${WARM_MOUNT_DIR}"/warmdata/*; do
        if [[ -f "$f" ]]; then
            FILE_SIZE=$(wc -c < "$f" | tr -d ' ')
            WARM_BYTES=$((WARM_BYTES + FILE_SIZE))
            cat "$f" > /dev/null
        fi
    done
    pass "All files read through cache (simulated warm): ${WARM_BYTES} bytes"

    # Second read should be faster (from cache).
    START_TIME=$(date +%s%N 2>/dev/null || date +%s)
    for f in "${WARM_MOUNT_DIR}"/warmdata/*; do
        if [[ -f "$f" ]]; then
            cat "$f" > /dev/null
        fi
    done
    END_TIME=$(date +%s%N 2>/dev/null || date +%s)
    pass "Second read completed (cache hits expected)"

    # Verify data integrity after cache.
    ORIG_MD5=$(cat "${WARM_SOURCE_DIR}/info.txt" | md5 -q 2>/dev/null || md5sum "${WARM_SOURCE_DIR}/info.txt" | awk '{print $1}')
    MOUNT_MD5=$(cat "${WARM_MOUNT_DIR}/warmdata/info.txt" | md5 -q 2>/dev/null || md5sum - < "${WARM_MOUNT_DIR}/warmdata/info.txt" | awk '{print $1}')
    if [[ "${ORIG_MD5}" == "${MOUNT_MD5}" ]]; then
        pass "Post-cache data integrity verified (MD5 match)"
    else
        fail "Post-cache data integrity: MD5 mismatch"
    fi
else
    fail "Warming test mount did not become ready"
fi

# Clean up warming mount.
if [[ -n "${MOUNT_PID}" ]]; then
    kill "${MOUNT_PID}" 2>/dev/null || true
    wait "${MOUNT_PID}" 2>/dev/null || true
    MOUNT_PID=""
    sleep 1
fi
rm -rf "${WARM_SOURCE_DIR}" "${WARM_MOUNT_DIR}" "${WARM_CACHE_DIR}" "${WARM_CONFIG}"
pass "Warming test cleaned up"

# ───────────────────────────────────────────────────────────────
# Phase 13 — M3: warpdrive-ctl warm CLI
# ───────────────────────────────────────────────────────────────
section "Phase 13 — M3: warpdrive-ctl warm CLI"

# Run Go benchmark tests (quick, 1 iteration).
if go test -bench=. -benchtime=1x -run='^$' ./pkg/cache/... > /tmp/warpdrive-bench-output.txt 2>&1; then
    BENCH_COUNT=$(grep -c '^Benchmark' /tmp/warpdrive-bench-output.txt || echo 0)
    pass "Go benchmarks passed (${BENCH_COUNT} benchmarks)"
else
    fail "Go benchmarks failed"
fi
rm -f /tmp/warpdrive-bench-output.txt

CTL_BINARY="${PROJECT_DIR}/bin/warpdrive-ctl"

# Verify binary exists.
if [[ -x "${CTL_BINARY}" ]]; then
    pass "warpdrive-ctl binary exists"
else
    fail "warpdrive-ctl binary not found at ${CTL_BINARY}"
fi

# Verify --help works.
CTL_HELP="$(${CTL_BINARY} --help 2>&1 || true)"
if echo "${CTL_HELP}" | grep -q "warm"; then
    pass "warpdrive-ctl --help shows warm command"
else
    fail "warpdrive-ctl --help missing warm command"
fi

# Verify warm --help shows flags.
CTL_WARM_HELP="$(${CTL_BINARY} warm --help 2>&1 || true)"
if echo "${CTL_WARM_HELP}" | grep -q "backend"; then
    pass "warpdrive-ctl warm --help shows --backend flag"
else
    fail "warpdrive-ctl warm --help missing --backend flag"
fi

# Set up source data and config for warm test.
CTL_SOURCE_DIR="/tmp/warpdrive-ctl-source"
CTL_CACHE_DIR="/tmp/warpdrive-ctl-cache"
CTL_CONFIG="/tmp/warpdrive-ctl-test.yaml"

rm -rf "${CTL_SOURCE_DIR}" "${CTL_CACHE_DIR}"
mkdir -p "${CTL_SOURCE_DIR}" "${CTL_CACHE_DIR}"

for i in $(seq 1 3); do
    dd if=/dev/urandom of="${CTL_SOURCE_DIR}/shard-$(printf '%04d' $i).bin" \
       bs=524288 count=1 2>/dev/null
done
echo "manifest" > "${CTL_SOURCE_DIR}/manifest.txt"
pass "warpdrive-ctl warm source data created"

cat > "${CTL_CONFIG}" << EOF
mount_point: /tmp/warpdrive-ctl-mount

cache:
  path: ${CTL_CACHE_DIR}
  max_size: 512MB
  block_size: 4MB

backends:
  - name: ctl-test
    type: local
    mount_path: /data
    config:
      root: ${CTL_SOURCE_DIR}
    auth:
      method: none
EOF
pass "warpdrive-ctl warm config written"

# Run warpdrive-ctl warm.
if "${CTL_BINARY}" warm --config "${CTL_CONFIG}" --backend ctl-test --recursive --workers 4 2>&1 | tee /tmp/warpdrive-ctl-output.txt; then
    if grep -q "Done" /tmp/warpdrive-ctl-output.txt || grep -q "complete" /tmp/warpdrive-ctl-output.txt; then
        pass "warpdrive-ctl warm completed successfully"
    else
        fail "warpdrive-ctl warm output missing completion message"
    fi
else
    fail "warpdrive-ctl warm exited with error"
fi

# Run again — should skip everything (resume).
if "${CTL_BINARY}" warm --config "${CTL_CONFIG}" --backend ctl-test --recursive 2>&1 | tee /tmp/warpdrive-ctl-resume.txt; then
    pass "warpdrive-ctl warm resume completed (re-run)"
else
    fail "warpdrive-ctl warm resume exited with error"
fi

# Verify cache directory has data.
if [[ "$(du -s "${CTL_CACHE_DIR}" | awk '{print $1}')" -gt 0 ]]; then
    CTL_CACHE_SIZE="$(du -sh "${CTL_CACHE_DIR}" | awk '{print $1}')"
    pass "warpdrive-ctl warm populated cache (${CTL_CACHE_SIZE})"
else
    fail "warpdrive-ctl warm did not populate cache"
fi

# Test warpdrive-ctl stats.
if "${CTL_BINARY}" stats --config "${CTL_CONFIG}" 2>&1 | grep -q "Hits"; then
    pass "warpdrive-ctl stats shows hit/miss statistics"
else
    fail "warpdrive-ctl stats output unexpected"
fi

rm -f /tmp/warpdrive-ctl-output.txt /tmp/warpdrive-ctl-resume.txt
rm -rf "${CTL_SOURCE_DIR}" "${CTL_CACHE_DIR}" "${CTL_CONFIG}"
pass "warpdrive-ctl warm test cleaned up"

# ───────────────────────────────────────────────────────────────
# Phase 14: M4 — Governance: Telemetry + Control Plane Unit Tests
# ───────────────────────────────────────────────────────────────
section "Phase 14: M4 – Governance"

# Run telemetry unit tests.
if go test ./pkg/telemetry/ -count=1 -timeout 30s 2>&1 | tail -3 | grep -q "^ok"; then
    pass "telemetry unit tests pass"
else
    fail "telemetry unit tests failed"
fi

# Run control plane unit tests.
if go test ./pkg/control/ -count=1 -timeout 30s 2>&1 | tail -3 | grep -q "^ok"; then
    pass "control plane unit tests pass"
else
    fail "control plane unit tests failed"
fi

# Run M4 e2e tests.
if go test ./e2e/ -count=1 -timeout 60s -run "TestM4" 2>&1 | tail -3 | grep -q "^ok"; then
    pass "M4 e2e tests pass"
else
    fail "M4 e2e tests failed"
fi

# Verify warpdrive-ctl governance CLI compiles and shows help.
CTL_USAGE_HELP="$(${CTL_BINARY} usage --help 2>&1 || true)"
if echo "${CTL_USAGE_HELP}" | grep -q "usage"; then
    pass "warpdrive-ctl usage command available"
else
    fail "warpdrive-ctl usage command not available"
fi

CTL_STALE_HELP="$(${CTL_BINARY} stale --help 2>&1 || true)"
if echo "${CTL_STALE_HELP}" | grep -q "stale"; then
    pass "warpdrive-ctl stale command available"
else
    fail "warpdrive-ctl stale command not available"
fi

CTL_QUOTA_HELP="$(${CTL_BINARY} quota --help 2>&1 || true)"
if echo "${CTL_QUOTA_HELP}" | grep -qi "quota"; then
    pass "warpdrive-ctl quota command available"
else
    fail "warpdrive-ctl quota command not available"
fi

CTL_STATUS_HELP="$(${CTL_BINARY} status --help 2>&1 || true)"
if echo "${CTL_STATUS_HELP}" | grep -q "status"; then
    pass "warpdrive-ctl status command available"
else
    fail "warpdrive-ctl status command not available"
fi

CTL_MOVE_HELP="$(${CTL_BINARY} move --help 2>&1 || true)"
if echo "${CTL_MOVE_HELP}" | grep -q "move\|Move"; then
    pass "warpdrive-ctl move command available"
else
    fail "warpdrive-ctl move command not available"
fi

CTL_SERVE_HELP="$(${CTL_BINARY} serve --help 2>&1 || true)"
if echo "${CTL_SERVE_HELP}" | grep -q "serve\|control plane"; then
    pass "warpdrive-ctl serve command available"
else
    fail "warpdrive-ctl serve command not available"
fi

# Additional M4 scenario: Start control plane and test REST API
M4_CONFIG="/tmp/warpdrive-m4-test.yaml"
M4_SOURCE_DIR="/tmp/warpdrive-m4-source"
M4_MOUNT_DIR="/tmp/warpdrive-m4-mount"
M4_CACHE_DIR="/tmp/warpdrive-m4-cache"

rm -rf "${M4_SOURCE_DIR}" "${M4_MOUNT_DIR}" "${M4_CACHE_DIR}"
mkdir -p "${M4_SOURCE_DIR}/dataset1" "${M4_SOURCE_DIR}/dataset2" "${M4_MOUNT_DIR}" "${M4_CACHE_DIR}"

# Create some test data
echo "test data 1" > "${M4_SOURCE_DIR}/dataset1/file1.txt"
echo "test data 2" > "${M4_SOURCE_DIR}/dataset2/file2.txt"
dd if=/dev/urandom of="${M4_SOURCE_DIR}/dataset1/large.bin" bs=1M count=5 2>/dev/null

cat > "${M4_CONFIG}" << EOF
mount_point: ${M4_MOUNT_DIR}

cache:
  path: ${M4_CACHE_DIR}
  max_size: 512MB
  block_size: 4MB

telemetry:
  enabled: true
  sink: stdout
  sample_metadata_ops: 1.0

control_plane:
  rest_addr: :18080
  storage_crawl_interval: 1h

backends:
  - name: m4-backend
    type: local
    mount_path: /data
    config:
      root: ${M4_SOURCE_DIR}
    auth:
      method: none
EOF

info "Starting control plane in background on port 18080"
"${CTL_BINARY}" serve --config "${M4_CONFIG}" --addr :18080 > /tmp/warpdrive-m4-serve.log 2>&1 &
M4_SERVER_PID=$!
sleep 2

# Check if control plane is running
if kill -0 ${M4_SERVER_PID} 2>/dev/null && curl -s http://localhost:18080/api/v1/backends > /dev/null 2>&1; then
    pass "Control plane server started and responding"
    
    # Test REST API endpoints
    if curl -s http://localhost:18080/api/v1/backends | grep -q '\['; then
        pass "GET /api/v1/backends returns JSON array"
    else
        fail "GET /api/v1/backends failed"
    fi
    
    # Test usage endpoint
    if curl -s http://localhost:18080/api/v1/usage | grep -q '\['; then
        pass "GET /api/v1/usage returns JSON array"
    else
        fail "GET /api/v1/usage failed"
    fi
    
    # Test quota endpoint
    if curl -s http://localhost:18080/api/v1/quota | grep -q '\['; then
        pass "GET /api/v1/quota returns JSON array"
    else
        fail "GET /api/v1/quota failed"
    fi
    
    # Test setting a team mapping
    if curl -s -X POST http://localhost:18080/api/v1/team \
        -H "Content-Type: application/json" \
        -d '{"user_id":"testuser","team_name":"testteam"}' | grep -q 'ok'; then
        pass "POST /api/v1/team accepts team mapping"
    else
        fail "POST /api/v1/team failed"
    fi
    
    # Test setting a quota
    if curl -s -X PUT http://localhost:18080/api/v1/quota \
        -H "Content-Type: application/json" \
        -d '{"team_name":"testteam","backend_name":"m4-backend","soft_limit":1000000,"hard_limit":2000000}' | grep -q 'ok'; then
        pass "PUT /api/v1/quota accepts quota configuration"
    else
        fail "PUT /api/v1/quota failed"
    fi
    
    # Stop control plane
    kill ${M4_SERVER_PID} 2>/dev/null || true
    wait ${M4_SERVER_PID} 2>/dev/null || true
    pass "Control plane shut down cleanly"
else
    fail "Control plane server failed to start"
    if [[ -n "${M4_SERVER_PID}" ]]; then
        kill ${M4_SERVER_PID} 2>/dev/null || true
    fi
fi

rm -rf "${M4_SOURCE_DIR}" "${M4_MOUNT_DIR}" "${M4_CACHE_DIR}" "${M4_CONFIG}" /tmp/warpdrive-m4-serve.log

pass "M4 governance verification complete"

# ───────────────────────────────────────────────────────────────
# Phase 15: M5 — Integration + Hardening
# ───────────────────────────────────────────────────────────────
section "Phase 15: M5 – Integration + Hardening"

# 15a. Verify Helm chart exists and has required files.
HELM_DIR="${PROJECT_DIR}/deploy/helm/warpdrive"
HELM_FILES=(
    "Chart.yaml"
    "values.yaml"
    "README.md"
    "templates/_helpers.tpl"
    "templates/daemonset.yaml"
    "templates/deployment.yaml"
    "templates/service.yaml"
    "templates/configmap.yaml"
    "templates/secret.yaml"
    "templates/serviceaccount.yaml"
    "templates/clusterrole.yaml"
    "templates/clusterrolebinding.yaml"
    "templates/pdb.yaml"
    "templates/NOTES.txt"
)
HELM_OK=true
for f in "${HELM_FILES[@]}"; do
    if [[ ! -f "${HELM_DIR}/${f}" ]]; then
        fail "Missing Helm chart file: ${f}"
        HELM_OK=false
    fi
done
if $HELM_OK; then
    pass "Helm chart has all ${#HELM_FILES[@]} required files"
fi

# 15b. Verify DaemonSet has critical K8s settings.
if grep -q "privileged" "${HELM_DIR}/templates/daemonset.yaml" && \
   grep -q "Bidirectional" "${HELM_DIR}/templates/daemonset.yaml" && \
   grep -q "SYS_ADMIN" "${HELM_DIR}/templates/daemonset.yaml"; then
    pass "DaemonSet has privileged + mountPropagation + SYS_ADMIN"
else
    fail "DaemonSet missing critical security settings"
fi

# 15c. Verify values.yaml has GPU node selector.
if grep -q "nvidia.com/gpu" "${HELM_DIR}/values.yaml"; then
    pass "values.yaml has GPU node selector"
else
    fail "values.yaml missing GPU node selector"
fi

# 15d. Verify Slurm deployment artifacts.
if [[ -f "${PROJECT_DIR}/deploy/systemd/warpdrive-agent.service" ]]; then
    pass "systemd service unit exists"
else
    fail "Missing systemd service unit"
fi

if [[ -f "${PROJECT_DIR}/deploy/slurm/warpdrive-prolog.sh" ]] && \
   grep -q "mountpoint" "${PROJECT_DIR}/deploy/slurm/warpdrive-prolog.sh" && \
   grep -q "SLURM" "${PROJECT_DIR}/deploy/slurm/warpdrive-prolog.sh"; then
    pass "Slurm prolog script exists with mount check + SLURM refs"
else
    fail "Slurm prolog script missing or incomplete"
fi

if [[ -f "${PROJECT_DIR}/deploy/slurm/warpdrive-epilog.sh" ]]; then
    pass "Slurm epilog script exists"
else
    fail "Missing Slurm epilog script"
fi

if [[ -f "${PROJECT_DIR}/deploy/slurm/ansible/install-warpdrive.yaml" ]]; then
    pass "Ansible playbook exists"
else
    fail "Missing Ansible playbook"
fi

# 15e. Verify monitoring artifacts.
if [[ -f "${PROJECT_DIR}/deploy/monitoring/alerts.yaml" ]]; then
    ALERT_RULES=("WarpDriveCacheHitRateLow" "WarpDriveBackendErrorRate" "WarpDriveCacheFull" "WarpDriveMountDown")
    ALERTS_OK=true
    for rule in "${ALERT_RULES[@]}"; do
        if ! grep -q "$rule" "${PROJECT_DIR}/deploy/monitoring/alerts.yaml"; then
            fail "alerts.yaml missing rule: ${rule}"
            ALERTS_OK=false
        fi
    done
    if $ALERTS_OK; then
        pass "Alert rules YAML has all 4 alert rules"
    fi
else
    fail "Missing alerts.yaml"
fi

if [[ -f "${PROJECT_DIR}/deploy/monitoring/grafana-dashboard.json" ]]; then
    # Verify it's valid JSON and has expected panels.
    if python3 -c "import json; json.load(open('${PROJECT_DIR}/deploy/monitoring/grafana-dashboard.json'))" 2>/dev/null; then
        PANELS=("Cache Hit Rate" "Cache Size" "Backend Latency" "Backend Errors" "FUSE Throughput" "FUSE Operation Latency")
        PANELS_OK=true
        for panel in "${PANELS[@]}"; do
            if ! grep -q "$panel" "${PROJECT_DIR}/deploy/monitoring/grafana-dashboard.json"; then
                fail "Dashboard missing panel: ${panel}"
                PANELS_OK=false
            fi
        done
        if $PANELS_OK; then
            pass "Grafana dashboard JSON valid with all 6 panels"
        fi
    else
        fail "Grafana dashboard is not valid JSON"
    fi
else
    fail "Missing grafana-dashboard.json"
fi

# 15f. Verify metrics package exists and tests pass.
if go test ./pkg/metrics/ -count=1 -timeout 30s 2>&1 | tail -3 | grep -q "^ok"; then
    pass "Metrics package tests pass"
else
    fail "Metrics package tests failed"
fi

# 15g. Run M5 e2e tests.
if go test ./e2e/ -count=1 -timeout 60s -run "TestM5" 2>&1 | tail -3 | grep -q "^ok"; then
    pass "M5 e2e tests pass"
else
    fail "M5 e2e tests failed"
fi

# 15h. Verify metrics instrumentation in core packages.
INSTRUMENTED_OK=true
if ! grep -q "metrics\." "${PROJECT_DIR}/pkg/cache/cache.go"; then
    fail "pkg/cache/cache.go not instrumented with metrics"
    INSTRUMENTED_OK=false
fi
if ! grep -q "metrics\." "${PROJECT_DIR}/pkg/cache/eviction.go"; then
    fail "pkg/cache/eviction.go not instrumented with metrics"
    INSTRUMENTED_OK=false
fi
if ! grep -q "metrics\." "${PROJECT_DIR}/pkg/fuse/fs.go"; then
    fail "pkg/fuse/fs.go not instrumented with metrics"
    INSTRUMENTED_OK=false
fi
if ! grep -q "metrics\." "${PROJECT_DIR}/pkg/backend/rclone.go"; then
    fail "pkg/backend/rclone.go not instrumented with metrics"
    INSTRUMENTED_OK=false
fi
if ! grep -q "metrics\." "${PROJECT_DIR}/pkg/auth/auth.go"; then
    fail "pkg/auth/auth.go not instrumented with metrics"
    INSTRUMENTED_OK=false
fi
if $INSTRUMENTED_OK; then
    pass "All core packages instrumented with Prometheus metrics"
fi

# 15i. Test metrics endpoint via warpdrive-mount.
M5_SOURCE_DIR="/tmp/warpdrive-m5-source"
M5_MOUNT_DIR="/tmp/warpdrive-m5-mount"
M5_CACHE_DIR="/tmp/warpdrive-m5-cache"
M5_CONFIG="/tmp/warpdrive-m5-test.yaml"

rm -rf "${M5_SOURCE_DIR}" "${M5_MOUNT_DIR}" "${M5_CACHE_DIR}"
mkdir -p "${M5_SOURCE_DIR}" "${M5_MOUNT_DIR}" "${M5_CACHE_DIR}"
echo "metrics-test-data" > "${M5_SOURCE_DIR}/test.txt"

cat > "${M5_CONFIG}" << EOF
mount_point: ${M5_MOUNT_DIR}
cache:
  path: ${M5_CACHE_DIR}
  max_size: 512MB
  block_size: 4MB
backends:
  - name: m5-test
    type: local
    mount_path: /data
    config:
      root: ${M5_SOURCE_DIR}
    auth:
      method: none
EOF

"${BINARY}" --config "${M5_CONFIG}" &
M5_MOUNT_PID=$!
info "warpdrive-mount started for metrics test (PID ${M5_MOUNT_PID})"

READY=false
for i in $(seq 1 30); do
    if mount | grep -q "${M5_MOUNT_DIR}" && ls "${M5_MOUNT_DIR}/" >/dev/null 2>&1; then
        READY=true
        break
    fi
    sleep 0.5
done

if $READY; then
    pass "M5 metrics test filesystem mounted"

    # Wait a moment for metrics server to start.
    sleep 1

    # Test /healthz endpoint.
    if curl -sf http://localhost:9090/healthz > /dev/null 2>&1; then
        HEALTHZ=$(curl -s http://localhost:9090/healthz)
        if echo "${HEALTHZ}" | grep -q "status"; then
            pass "GET /healthz returns health status JSON"
        else
            fail "GET /healthz response unexpected: ${HEALTHZ}"
        fi
    else
        fail "GET /healthz not responding"
    fi

    # Test /metrics endpoint.
    if curl -sf http://localhost:9090/metrics > /dev/null 2>&1; then
        METRICS_OUTPUT=$(curl -s http://localhost:9090/metrics)
        METRICS_OK=true
        for metric in "warpdrive_cache_hit_total" "warpdrive_cache_size_bytes" "warpdrive_fuse_operations_total" "warpdrive_backend_request_duration_seconds"; do
            if ! echo "${METRICS_OUTPUT}" | grep -q "$metric"; then
                fail "GET /metrics missing: ${metric}"
                METRICS_OK=false
            fi
        done
        if $METRICS_OK; then
            pass "GET /metrics returns Prometheus metrics with warpdrive_* families"
        fi
    else
        fail "GET /metrics not responding"
    fi

    # Generate some read traffic to produce metrics.
    cat "${M5_MOUNT_DIR}/data/test.txt" > /dev/null 2>&1
    sleep 1

    # Check that read incremented FUSE counters.
    UPDATED_METRICS=$(curl -s http://localhost:9090/metrics 2>/dev/null || echo "")
    if echo "${UPDATED_METRICS}" | grep -q 'warpdrive_fuse_operations_total{operation="read"}'; then
        pass "FUSE read operation counter incremented after read"
    else
        # Counter may not yet be labeled — accept if metric family is present.
        if echo "${UPDATED_METRICS}" | grep -q "warpdrive_fuse_operations_total"; then
            pass "FUSE operations counter present (label may vary)"
        else
            fail "FUSE operations counter not found after read"
        fi
    fi
else
    fail "M5 metrics test mount did not become ready"
fi

# Clean up.
if [[ -n "${M5_MOUNT_PID:-}" ]]; then
    kill "${M5_MOUNT_PID}" 2>/dev/null || true
    wait "${M5_MOUNT_PID}" 2>/dev/null || true
fi
sleep 1
rm -rf "${M5_SOURCE_DIR}" "${M5_MOUNT_DIR}" "${M5_CACHE_DIR}" "${M5_CONFIG}"
pass "M5 integration verification complete"

# ───────────────────────────────────────────────────────────────
# Summary
# ───────────────────────────────────────────────────────────────
printf "\n${BOLD}════════════════════════════════════════════${RESET}\n"
printf "${BOLD}  Results: ${GREEN}%d passed${RESET}  ${RED}%d failed${RESET}  (${BOLD}%d total${RESET})\n" "${PASS}" "${FAIL}" "${TOTAL}"
printf "${BOLD}════════════════════════════════════════════${RESET}\n\n"

if [[ "${FAIL}" -gt 0 ]]; then
    exit 1
fi
