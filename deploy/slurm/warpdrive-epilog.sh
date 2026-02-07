#!/bin/bash
# /etc/slurm/epilog.d/warpdrive-epilog.sh
# Runs after each Slurm job completes

set -euo pipefail

# Report job stats to control plane
warpdrive-ctl job-report \
    --job-id "$SLURM_JOB_ID" \
    --user "$SLURM_JOB_USER" \
    --status "$SLURM_JOB_EXIT_CODE" \
    2>&1 | logger -t warpdrive-epilog
