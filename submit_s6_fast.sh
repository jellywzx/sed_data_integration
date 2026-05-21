#!/usr/bin/env bash
set -euo pipefail

# Fast multi-node submitter for s6:
# - s6_basin_merge_to_nc.py
# - s6_export_daily_matrix_nc.py
# - s6_export_monthly_matrix_nc.py
# - s6_export_annual_matrix_nc.py
# - s6_export_climatology_to_nc.py
# - s6_export_satellite_validation_to_nc.py
#
# Usage:
#   bash submit_s6_fast.sh
#
# Optional overrides via environment variables:
#   PYTHON_BIN=/path/to/python3
#   LSF_QUEUE=normal
#   LSF_PROJECT=myproj
#   LSF_EXTRA="-gpu num=0"
#   DRY_RUN=1
#   RUN_ONLY=annual
#
# Worker/core tuning:
#   MERGE_N=48 MERGE_WORKERS=40 MERGE_METADATA_WORKERS=32
#   DAILY_N=48 DAILY_WORKERS=40
#   MONTHLY_N=24 MONTHLY_WORKERS=20
#   ANNUAL_N=4 ANNUAL_WORKERS=4
#   CLIM_N=4
#   SATVAL_N=24
#
# Memory (MB):
#   MERGE_MEM_MB=240000 DAILY_MEM_MB=240000 MONTHLY_MEM_MB=120000 ANNUAL_MEM_MB=16000 CLIM_MEM_MB=16000 SATVAL_MEM_MB=64000

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${ROOT_DIR}/output"
LOG_DIR="${OUT_DIR}/logs/s6_lsf_parallel"
mkdir -p "${LOG_DIR}"
SUBMIT_LOG="${LOG_DIR}/submit_s6_fast.$(date +%Y%m%d_%H%M%S).log"

PYTHON_BIN="${PYTHON_BIN:-/share/home/dq134/.conda/envs/wzx/bin/python3}"
if [[ ! -x "${PYTHON_BIN}" ]]; then
  PYTHON_BIN="$(command -v python3 || true)"
fi
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "Error: python3 not found. Set PYTHON_BIN explicitly." >&2
  exit 1
fi

S5_CSV="${OUT_DIR}/s5_basin_clustered_stations.csv"
MATRIX_DIR="${OUT_DIR}/s6_matrix_by_resolution"
CLIM_INPUT_DIR="/share/home/dq134/wzx/sed_data/sediment_wzx_1111/output_resolution_organized/climatology"

if [[ ! -f "${S5_CSV}" ]]; then
  echo "Error: missing input ${S5_CSV}" >&2
  exit 1
fi
if [[ ! -d "${CLIM_INPUT_DIR}" ]]; then
  echo "Error: missing climatology dir ${CLIM_INPUT_DIR}" >&2
  exit 1
fi

LSF_QUEUE="${LSF_QUEUE:-}"
LSF_PROJECT="${LSF_PROJECT:-}"
LSF_EXTRA="${LSF_EXTRA:-}"
DRY_RUN="${DRY_RUN:-0}"
RUN_ONLY="${RUN_ONLY:-}"

MERGE_N="${MERGE_N:-48}"
MERGE_WORKERS="${MERGE_WORKERS:-40}"
MERGE_METADATA_WORKERS="${MERGE_METADATA_WORKERS:-32}"

DAILY_N="${DAILY_N:-48}"
DAILY_WORKERS="${DAILY_WORKERS:-40}"

MONTHLY_N="${MONTHLY_N:-24}"
MONTHLY_WORKERS="${MONTHLY_WORKERS:-20}"

ANNUAL_N="${ANNUAL_N:-4}"
ANNUAL_WORKERS="${ANNUAL_WORKERS:-4}"

CLIM_N="${CLIM_N:-4}"
SATVAL_N="${SATVAL_N:-24}"

MERGE_MEM_MB="${MERGE_MEM_MB:-240000}"
DAILY_MEM_MB="${DAILY_MEM_MB:-240000}"
MONTHLY_MEM_MB="${MONTHLY_MEM_MB:-120000}"
ANNUAL_MEM_MB="${ANNUAL_MEM_MB:-16000}"
CLIM_MEM_MB="${CLIM_MEM_MB:-16000}"
SATVAL_MEM_MB="${SATVAL_MEM_MB:-64000}"

JOB_TAG="${JOB_TAG:-s6fast}"

submit_job() {
  local name="$1"
  local n="$2"
  local mem_mb="$3"
  local cmd="$4"
  local dep="${5:-}"

  local -a bsub_cmd=(
    bsub
    -n "${n}"
    -R "span[hosts=1] rusage[mem=4096]"
    -M "${mem_mb}"
    -J "${name}"
    -oo "${LOG_DIR}/${name}.%J.out"
    -eo "${LOG_DIR}/${name}.%J.err"
  )
  if [[ -n "${LSF_QUEUE}" ]]; then
    bsub_cmd+=(-q "${LSF_QUEUE}")
  fi
  if [[ -n "${LSF_PROJECT}" ]]; then
    bsub_cmd+=(-P "${LSF_PROJECT}")
  fi
  if [[ -n "${LSF_EXTRA}" ]]; then
    local -a extra_tokens=()
    read -r -a extra_tokens <<< "${LSF_EXTRA}"
    bsub_cmd+=("${extra_tokens[@]}")
  fi
  if [[ -n "${dep}" ]]; then
    bsub_cmd+=(-w "${dep}")
  fi
  bsub_cmd+=(bash -lc "cd ${ROOT_DIR} && ${cmd}")

  local rendered_cmd=""
  printf -v rendered_cmd '%q ' "${bsub_cmd[@]}"
  rendered_cmd="${rendered_cmd% }"

  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[DRY_RUN] ${rendered_cmd}" | tee -a "${SUBMIT_LOG}" >&2
    printf 'DRYRUN_%s\n' "${name}"
    return 0
  fi

  local output
  output="$("${bsub_cmd[@]}" 2>&1)"
  local rc=$?
  {
    echo "[$(date +%F' '%T)] ${name}"
    echo "${rendered_cmd}"
    echo "${output}"
    echo
  } >> "${SUBMIT_LOG}"
  echo "${output}" >&2
  if [[ ${rc} -ne 0 ]]; then
    echo "Error: bsub failed for ${name}. See ${SUBMIT_LOG}" >&2
    exit ${rc}
  fi
  local jid
  jid="$(sed -n 's/.*<\([0-9]\+\)>.*/\1/p' <<< "${output}")"
  if [[ -z "${jid}" ]]; then
    echo "Error: failed to parse job id from bsub output for ${name}. See ${SUBMIT_LOG}" >&2
    exit 1
  fi
  printf '%s\n' "${jid}"
}

echo "ROOT_DIR=${ROOT_DIR}"
echo "PYTHON_BIN=${PYTHON_BIN}"
echo "LOG_DIR=${LOG_DIR}"
echo "SUBMIT_LOG=${SUBMIT_LOG}"
echo "DRY_RUN=${DRY_RUN}"
echo "RUN_ONLY=${RUN_ONLY:-all}"

merge_cmd="${PYTHON_BIN} s6_basin_merge_to_nc.py -i ${S5_CSV} -o ${OUT_DIR}/s6_basin_merged_all.nc --quality-order-csv ${OUT_DIR}/s6_cluster_quality_order.csv -w ${MERGE_WORKERS} --metadata-workers ${MERGE_METADATA_WORKERS}"
daily_cmd="${PYTHON_BIN} s6_export_daily_matrix_nc.py -i ${S5_CSV} --out-dir ${MATRIX_DIR} --workers ${DAILY_WORKERS} --resolution-workers 1"
monthly_cmd="${PYTHON_BIN} s6_export_monthly_matrix_nc.py -i ${S5_CSV} --out-dir ${MATRIX_DIR} --workers ${MONTHLY_WORKERS} --resolution-workers 1"
annual_cmd="${PYTHON_BIN} s6_export_annual_matrix_nc.py -i ${S5_CSV} --out-dir ${MATRIX_DIR} --workers ${ANNUAL_WORKERS} --resolution-workers 1"
clim_cmd="${PYTHON_BIN} s6_export_climatology_to_nc.py --input-dir ${CLIM_INPUT_DIR} --output ${OUT_DIR}/s6_climatology_only.nc --output-shp ${OUT_DIR}/s6_climatology_stations.shp"
satval_cmd="${PYTHON_BIN} s6_export_satellite_validation_to_nc.py"

should_run() {
  local step="$1"
  if [[ -z "${RUN_ONLY}" ]]; then
    return 0
  fi
  [[ ",${RUN_ONLY}," == *",${step},"* ]]
}

merge_jid=""
daily_jid=""
monthly_jid=""
annual_jid=""
clim_jid=""
satval_jid=""

if should_run "merge"; then
  merge_jid="$(submit_job "${JOB_TAG}_merge" "${MERGE_N}" "${MERGE_MEM_MB}" "${merge_cmd}")"
fi
if should_run "daily"; then
  daily_jid="$(submit_job "${JOB_TAG}_daily" "${DAILY_N}" "${DAILY_MEM_MB}" "${daily_cmd}")"
fi
if should_run "monthly"; then
  monthly_jid="$(submit_job "${JOB_TAG}_monthly" "${MONTHLY_N}" "${MONTHLY_MEM_MB}" "${monthly_cmd}")"
fi
if should_run "annual"; then
  annual_jid="$(submit_job "${JOB_TAG}_annual" "${ANNUAL_N}" "${ANNUAL_MEM_MB}" "${annual_cmd}")"
fi
if should_run "clim"; then
  clim_jid="$(submit_job "${JOB_TAG}_clim" "${CLIM_N}" "${CLIM_MEM_MB}" "${clim_cmd}")"
fi
if should_run "satellite"; then
  satval_jid="$(submit_job "${JOB_TAG}_satellite" "${SATVAL_N}" "${SATVAL_MEM_MB}" "${satval_cmd}")"
fi

echo "Submitted jobs:"
[[ -n "${merge_jid}" ]] && echo "  merge   : ${merge_jid}"
[[ -n "${daily_jid}" ]] && echo "  daily   : ${daily_jid}"
[[ -n "${monthly_jid}" ]] && echo "  monthly : ${monthly_jid}"
[[ -n "${annual_jid}" ]] && echo "  annual  : ${annual_jid}"
[[ -n "${clim_jid}" ]] && echo "  clim    : ${clim_jid}"
[[ -n "${satval_jid}" ]] && echo "  satellite: ${satval_jid}"

check_jid=""
if [[ -z "${RUN_ONLY}" ]]; then
  check_dep="done(${merge_jid}) && done(${daily_jid}) && done(${monthly_jid}) && done(${annual_jid}) && done(${clim_jid}) && done(${satval_jid})"
  check_cmd="ls -lh ${OUT_DIR}/s6_basin_merged_all.nc ${OUT_DIR}/s6_cluster_quality_order.csv ${MATRIX_DIR}/s6_basin_matrix_daily.nc ${MATRIX_DIR}/s6_basin_matrix_monthly.nc ${MATRIX_DIR}/s6_basin_matrix_annual.nc ${OUT_DIR}/s6_climatology_only.nc ${OUT_DIR}/s6_satellite_validation_only.nc ${OUT_DIR}/s6_satellite_validation_catalog.csv"
  check_jid="$(submit_job "${JOB_TAG}_check" "1" "4000" "${check_cmd}" "${check_dep}")"
  echo "  check   : ${check_jid}"
fi

track_ids=()
[[ -n "${merge_jid}" ]] && track_ids+=("${merge_jid}")
[[ -n "${daily_jid}" ]] && track_ids+=("${daily_jid}")
[[ -n "${monthly_jid}" ]] && track_ids+=("${monthly_jid}")
[[ -n "${annual_jid}" ]] && track_ids+=("${annual_jid}")
[[ -n "${clim_jid}" ]] && track_ids+=("${clim_jid}")
[[ -n "${satval_jid}" ]] && track_ids+=("${satval_jid}")
[[ -n "${check_jid}" ]] && track_ids+=("${check_jid}")
if [[ ${#track_ids[@]} -gt 0 ]]; then
  echo "Track with: bjobs -w ${track_ids[*]}"
fi
echo "Submit log: ${SUBMIT_LOG}"
