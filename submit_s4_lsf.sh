#!/usr/bin/env bash
# =============================================================================
# submit_s4_lsf.sh - S4 basin-matching LSF job submitter
#
# Purpose:
#   Submit S4 basin-matching jobs on an LSF cluster in three steps:
#     1. Array job (s4_trace)      - split stations into N shards and trace basins in parallel
#     2. Finalize job (s4_finalize) - merge shard outputs into final CSV and optional GPKG
#     3. Summary job (s4_summary)   - generate a summary report after finalize completes
#
# Usage:
#   ./submit_s4_lsf.sh [ARRAY_SIZE]
#
# Arguments:
#   ARRAY_SIZE    shard count (default 16)
#
# Environment variables, export before submit to override:
#   S4_QUEUE                  LSF queue (default normal)
#   S4_NCORES                 CPU cores per shard (default 24)
#   S4_MEM                    memory request per shard (default 120G)
#   S4_PTILE                  CPU cores per node (default 24)
#   S4_GPKG_EXCLUDE_SATELLITE exclude satellite stations from GPKG (default 1=enabled)
#                             set to 0 to keep satellite stations
#   PYTHON_BIN                Python interpreter path (default python3)
#   S4_RESUME                 enable resume mode (default True; see Python script)
#   S4_N_WORKERS              parallel workers per shard (default 24)
#   S4_BATCH_SIZE             stations per worker task (default 50)
#   S4_SAVE_GPKG              write GPKG output (default True)
#   S4_MAXTASKSPERCHILD       restart worker after this many tasks (default 8)
#   OUTPUT_R_ROOT             override Output_r root for cross-machine migration
#   MERIT_DIR                 MERIT Hydro dataset path
#
# Examples:
#   ./submit_s4_lsf.sh                     # normal 16-shard submit
#   ./submit_s4_lsf.sh 8                   # 8 shard
#   export S4_QUEUE=priority               # use the priority queue
#   export S4_GPKG_EXCLUDE_SATELLITE=0     # keep satellite stations
#   ./submit_s4_lsf.sh
#
# Rerun by clearing old shard files; otherwise existing shards are skipped by default:
#   rm -rf output/s4_shards output/s4_upstream_basins.csv output/s4_upstream_basins.gpkg
#   export S4_GPKG_EXCLUDE_SATELLITE=1
#   ./submit_s4_lsf.sh 16
#
# More thorough cleanup:
#   rm -rf output/s4_* output/logs/s4_lsf/
#
# Resume after interruption; completed shards are skipped automatically:
#   ./submit_s4_lsf.sh 16
#
# Check job status:
#   bjobs | grep s4_
#
# Log path:
#   output/logs/s4_lsf/s4_trace.<index>.live.log
#   output/logs/s4_lsf/s4_finalize.live.log
#   output/logs/s4_lsf/s4_summary.<timestamp>.log
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/submit_s4_lsf.py" "$@"
