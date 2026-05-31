#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/share/home/dq134/.conda/envs/wzx/bin/python3}"

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/submit_s6_fast.py" "$@"
