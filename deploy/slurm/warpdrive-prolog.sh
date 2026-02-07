#!/bin/bash
# /etc/slurm/prolog.d/warpdrive-prolog.sh
# Slurm prolog script — runs before each SLURM job on the compute node
#
# To use, add to your Slurm configuration:
#   Prolog=/etc/slurm/prolog.d/warpdrive-prolog.sh
#
# Environment variables (set in job submission or slurm.conf):
#   WARPDRIVE_CONFIG          — Path to warpdrive config file (default: /etc/warpdrive/config.yaml)
#   WARPDRIVE_WARMUP_PATH     — Path to warm in cache (or use SLURM_JOB_COMMENT)
#   WARPDRIVE_WARMUP_BACKEND  — Backend name to warm (default: from config)
#   WARPDRIVE_WARMUP_PREFIX   — Remote path prefix to warm (default: "")
#   WARPDRIVE_WARMUP_WORKERS  — Parallel download workers (default: 32)
#   WARPDRIVE_WARMUP_MAX_SIZE — Max bytes to warm, e.g. "500GB" (default: unlimited)
#
# Example:
#   sbatch --comment="/data/imagenet" --export=WARPDRIVE_WARMUP_BACKEND=training-data job.sh

set -euo pipefail

WARPDRIVE_CONFIG="${WARPDRIVE_CONFIG:-/etc/warpdrive/config.yaml}"
WARMUP_PATH="${WARPDRIVE_WARMUP_PATH:-${SLURM_JOB_COMMENT:-}}"
WARMUP_BACKEND="${WARPDRIVE_WARMUP_BACKEND:-}"
WARMUP_PREFIX="${WARPDRIVE_WARMUP_PREFIX:-}"
WARMUP_WORKERS="${WARPDRIVE_WARMUP_WORKERS:-32}"
WARMUP_MAX_SIZE="${WARPDRIVE_WARMUP_MAX_SIZE:-0}"

# Check if mount is healthy
if ! mountpoint -q /data; then
    echo "KHADI ERROR: /data is not mounted. Restarting agent..."
    systemctl restart warpdrive-agent
    sleep 5
    if ! mountpoint -q /data; then
        echo "KHADI ERROR: /data still not mounted after restart"
        exit 1
    fi
fi

# Warm cache if path or backend specified
if [ -n "$WARMUP_PATH" ]; then
    echo "KHADI: Warming cache for SLURM job ${SLURM_JOB_ID:-unknown}: ${WARMUP_PATH}"
    timeout 1800 warpdrive-ctl warm "$WARMUP_PATH" \
        --recursive \
        --max-size "$WARMUP_MAX_SIZE" \
        --workers "$WARMUP_WORKERS" \
        2>&1 | logger -t warpdrive-prolog
    echo "KHADI: Cache warm complete for SLURM job ${SLURM_JOB_ID:-unknown}"
elif [ -n "$WARMUP_BACKEND" ]; then
    echo "KHADI: Warming cache for backend=$WARMUP_BACKEND prefix=$WARMUP_PREFIX"

    ARGS=(warm --config "$WARPDRIVE_CONFIG" --backend "$WARMUP_BACKEND" --recursive --workers "$WARMUP_WORKERS")
    if [ -n "$WARMUP_PREFIX" ]; then
        ARGS+=(--prefix "$WARMUP_PREFIX")
    fi
    if [ "$WARMUP_MAX_SIZE" != "0" ]; then
        ARGS+=(--max-size "$WARMUP_MAX_SIZE")
    fi

    warpdrive-ctl "${ARGS[@]}"
    echo "KHADI: Cache warm complete"
fi
