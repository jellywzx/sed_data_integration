#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if ! command -v bsub >/dev/null 2>&1; then
  echo "Error: bsub not found in PATH."
  exit 1
fi

ARRAY_SIZE="${1:-16}"
QUEUE="${S4_QUEUE:-normal}"
CORES="${S4_NCORES:-24}"
MEM="${S4_MEM:-120G}"
PTILE="${S4_PTILE:-24}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! [[ "${ARRAY_SIZE}" =~ ^[0-9]+$ ]] || [ "${ARRAY_SIZE}" -le 0 ]; then
  echo "Error: ARRAY_SIZE must be a positive integer, got '${ARRAY_SIZE}'."
  exit 1
fi

mkdir -p output/logs/s4_lsf output/s4_shards

echo "Submitting s4 array job..."
ARRAY_SUBMIT_OUTPUT="$(
  S4_SHARD_COUNT="${ARRAY_SIZE}" PYTHON_BIN="${PYTHON_BIN}" \
    bsub \
      -q "${QUEUE}" \
      -J "s4_trace[1-${ARRAY_SIZE}]" \
      -n "${CORES}" \
      -R "rusage[mem=${MEM}]" \
      -R "span[ptile=${PTILE}] span[hosts=1]" \
      < "${SCRIPT_DIR}/s4_trace_array.lsf"
)"
echo "${ARRAY_SUBMIT_OUTPUT}"

ARRAY_JOB_ID="$(printf '%s\n' "${ARRAY_SUBMIT_OUTPUT}" | sed -n 's/.*<\([0-9][0-9]*\)>.*/\1/p')"
if [ -z "${ARRAY_JOB_ID}" ]; then
  echo "Error: failed to parse array job id from bsub output."
  exit 1
fi

TMP_FINALIZE_LSF="$(mktemp /tmp/s4_finalize_merge.XXXXXX.lsf)"
sed "s/<array_jobid>/${ARRAY_JOB_ID}/g" "${SCRIPT_DIR}/s4_finalize_merge.lsf" > "${TMP_FINALIZE_LSF}"

echo "Submitting finalize job (depends on array job ${ARRAY_JOB_ID})..."
FINALIZE_SUBMIT_OUTPUT="$(
  S4_SHARD_COUNT="${ARRAY_SIZE}" PYTHON_BIN="${PYTHON_BIN}" \
    bsub -q "${QUEUE}" < "${TMP_FINALIZE_LSF}"
)"
echo "${FINALIZE_SUBMIT_OUTPUT}"

rm -f "${TMP_FINALIZE_LSF}"

echo
echo "Submitted."
echo "Array job id: ${ARRAY_JOB_ID}"
echo "Logs:"
echo "  output/logs/s4_lsf/s4_trace.<index>.out"
echo "  output/logs/s4_lsf/s4_trace.<index>.err"
echo "  output/logs/s4_lsf/s4_trace.<index>.live.log   (runtime direct log)"
echo "  output/logs/s4_lsf/s4_finalize.out"
echo "  output/logs/s4_lsf/s4_finalize.err"
echo "  output/logs/s4_lsf/s4_finalize.live.log        (runtime direct log)"
echo
echo "Useful commands:"
echo "  bjobs | grep s4_"
echo "  tail -f output/logs/s4_lsf/s4_trace.1.live.log"
