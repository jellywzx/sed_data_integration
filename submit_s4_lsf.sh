#!/usr/bin/env bash
# =============================================================================
# submit_s4_lsf.sh — S4 流域匹配 LSF 作业提交脚本
#
# 功能：
#   在 LSF 集群上提交 S4 流域匹配作业，流程分三步：
#     1. Array job (s4_trace)     — 将站点拆分到 N 个 shard 并行追溯上游流域
#     2. Finalize job (s4_finalize)— 合并所有 shard 产出最终 CSV 及可选 GPKG
#     3. Summary job (s4_summary)  — finalize 完成后生成摘要报告
#
# 用法：
#   ./submit_s4_lsf.sh [ARRAY_SIZE]
#
# 参数：
#   ARRAY_SIZE    shard 数量（默认 16）
#
# 环境变量（可在提交前 export 覆盖）：
#   S4_QUEUE                  LSF 队列（默认 normal）
#   S4_NCORES                 每个 shard 的 CPU 核数（默认 24）
#   S4_MEM                    每个 shard 的内存申请（默认 120G）
#   S4_PTILE                  每个节点的 CPU 数（默认 24）
#   S4_GPKG_EXCLUDE_SATELLITE 是否从 GPKG 中排除卫星站点（默认 1=启用）
#                             设 0 可保留卫星站点
#   PYTHON_BIN                Python 解释器路径（默认 python3）
#   S4_RESUME                 是否启用断点续跑（默认 True，参考 Python 脚本）
#   S4_N_WORKERS              每个 shard 内并行 worker 数（默认 24）
#   S4_BATCH_SIZE             每个 worker 任务处理的站点数（默认 50）
#   S4_SAVE_GPKG              是否输出 GPKG 文件（默认 True）
#   S4_MAXTASKSPERCHILD       worker 最多处理任务数后重启（默认 8）
#   OUTPUT_R_ROOT             覆盖 Output_r 根目录（跨机器迁移）
#   MERIT_DIR                 MERIT Hydro 数据集路径
#
# 示例：
#   ./submit_s4_lsf.sh                     # 16 shard 正常提交
#   ./submit_s4_lsf.sh 8                   # 8 shard
#   export S4_QUEUE=priority               # 使用 priority 队列
#   export S4_GPKG_EXCLUDE_SATELLITE=0     # 保留卫星站点
#   ./submit_s4_lsf.sh
#
# 重新跑（清除旧 shard 文件强制重新 trace）：
#   rm -rf output/s4_shards output/s4_upstream_basins.csv output/s4_upstream_basins.gpkg
#   export S4_GPKG_EXCLUDE_SATELLITE=1
#   ./submit_s4_lsf.sh 16
#
# 更彻底的清理：
#   rm -rf output/s4_* output/logs/s4_lsf/
#
# 中断后续跑（resume 机制自动跳过已完成的 shard）：
#   ./submit_s4_lsf.sh 16
#
# 查看作业状态：
#   bjobs | grep s4_
#
# 日志路径：
#   output/logs/s4_lsf/s4_trace.<index>.live.log
#   output/logs/s4_lsf/s4_finalize.live.log
#   output/logs/s4_lsf/s4_summary.<timestamp>.log
# =============================================================================

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
S4_GPKG_EXCLUDE_SATELLITE="${S4_GPKG_EXCLUDE_SATELLITE:-1}"
export S4_GPKG_EXCLUDE_SATELLITE

if ! [[ "${ARRAY_SIZE}" =~ ^[0-9]+$ ]] || [ "${ARRAY_SIZE}" -le 0 ]; then
  echo "Error: ARRAY_SIZE must be a positive integer, got '${ARRAY_SIZE}'."
  exit 1
fi

mkdir -p output/logs/s4_lsf output/s4_shards

SUMMARY_LOG="output/logs/s4_lsf/s4_summary.$(date +%Y%m%d_%H%M%S).log"
SUMMARY_SCRIPT="output/logs/s4_lsf/.s4_summary_check.sh"
cat > "${SUMMARY_SCRIPT}" << 'SUMEOF'
#!/usr/bin/env bash
set -u

summary_log="$1"
array_job_id="$2"
finalize_job_id="$3"
array_size="$4"
python_bin="$5"

"${python_bin}" - "$summary_log" "$array_job_id" "$finalize_job_id" "$array_size" << 'PYEOF'
import csv
import os
import sys
from collections import Counter
from pathlib import Path

summary_log, array_job_id, finalize_job_id, array_size_text = sys.argv[1:5]
array_size = int(array_size_text)

root = Path.cwd()
out_dir = root / "output"
log_dir = out_dir / "logs" / "s4_lsf"
shard_dir = out_dir / "s4_shards"

csv_path = out_dir / "s4_upstream_basins.csv"
partial_csv_path = out_dir / "s4_upstream_basins.partial.csv"
reported_area_csv = out_dir / "s4_reported_area_check.csv"
gpkg_path = out_dir / "s4_upstream_basins.gpkg"
local_gpkg_path = out_dir / "s4_local_catchments.gpkg"
finalize_out = log_dir / "s4_finalize.out"
finalize_err = log_dir / "s4_finalize.err"
finalize_live = log_dir / "s4_finalize.live.log"

def size_text(path):
    if not path.is_file():
        return "MISSING"
    size = path.stat().st_size
    units = ["B", "K", "M", "G", "T"]
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    return "{:.1f}{}".format(value, unit) if unit != "B" else "{}B".format(size)

def lsf_status(path):
    if not path.is_file():
        return "NO_LSF_LOG"
    text = path.read_text(errors="replace")
    if "Successfully completed." in text:
        return "DONE"
    if "Exited with exit code" in text:
        return "EXIT"
    return "ENDED_UNKNOWN"

def clean(value):
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null"} else text

def count_true(rows, name):
    if not rows or name not in rows[0]:
        return None
    return sum(1 for row in rows if clean(row.get(name)).lower() in {"1", "true", "t", "yes", "y"})

def counter_for(rows, name, limit=12):
    if not rows or name not in rows[0]:
        return []
    counter = Counter(clean(row.get(name)) or "(blank)" for row in rows)
    return counter.most_common(limit)

rows = []
if csv_path.is_file():
    with csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))

total_rows = len(rows)
matched_rows = 0
if rows:
    matched_rows = sum(1 for row in rows if clean(row.get("basin_id")))

skipped_rows = 0
if rows and "method" in rows[0]:
    skipped_rows = sum(
        1
        for row in rows
        if clean(row.get("method")) == "source_remote_sensing_no_basin_match"
    )

shard_files = [shard_dir / "s4_upstream_basins.shard_{:04d}.csv".format(i) for i in range(array_size)]
present_shards = [path for path in shard_files if path.is_file()]
missing_shards = [path.name for path in shard_files if not path.is_file()]

output_files = [
    csv_path,
    partial_csv_path,
    reported_area_csv,
    gpkg_path,
    local_gpkg_path,
    finalize_out,
    finalize_err,
    finalize_live,
]

lines = []
lines.append("S4 basin matching summary")
lines.append("Generated: {}".format(__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
lines.append("Array job id: {}".format(array_job_id))
lines.append("Finalize job id: {}".format(finalize_job_id))
lines.append("Finalize LSF status: {}".format(lsf_status(finalize_out)))
lines.append("S4_GPKG_EXCLUDE_SATELLITE: {}".format(os.environ.get("S4_GPKG_EXCLUDE_SATELLITE", "")))
lines.append("")
lines.append("Shard completion:")
lines.append("  expected shards: {}".format(array_size))
lines.append("  present shards : {}".format(len(present_shards)))
if missing_shards:
    lines.append("  missing shards : {}".format(", ".join(missing_shards[:40])))
    if len(missing_shards) > 40:
        lines.append("  missing shards : ... {} more".format(len(missing_shards) - 40))
else:
    lines.append("  missing shards : none")
lines.append("")
lines.append("Basin matching counts from s4_upstream_basins.csv:")
lines.append("  output rows          : {}".format(total_rows if csv_path.is_file() else "CSV_MISSING"))
lines.append("  rows with basin_id   : {}".format(matched_rows if csv_path.is_file() else "CSV_MISSING"))
lines.append("  no-basin skip rows   : {}".format(skipped_rows if csv_path.is_file() else "CSV_MISSING"))
for col in ("point_in_local", "point_in_basin"):
    value = count_true(rows, col)
    if value is not None:
        lines.append("  {} true : {}".format(col, value))
lines.append("")

for col in ("basin_status", "basin_flag", "match_quality", "method", "source"):
    counts = counter_for(rows, col)
    if counts:
        lines.append("{} counts:".format(col))
        for key, value in counts:
            lines.append("  {:<42} {}".format(key, value))
        lines.append("")

lines.append("Output files:")
for path in output_files:
    lines.append("  {:<36} {}".format(path.relative_to(root).as_posix(), size_text(path)))
lines.append("")
lines.append("GPKG status:")
lines.append("  upstream basins gpkg : {}".format("YES" if gpkg_path.is_file() else "NO"))
lines.append("  local catchments gpkg: {}".format("YES" if local_gpkg_path.is_file() else "NO"))
lines.append("")

all_shards_present = len(missing_shards) == 0
required_outputs_present = csv_path.is_file()
if all_shards_present and required_outputs_present and lsf_status(finalize_out) == "DONE":
    result = "S4_COMPLETED"
elif required_outputs_present:
    result = "S4_OUTPUT_CSV_PRESENT_BUT_REVIEW_LOGS"
else:
    result = "S4_INCOMPLETE_OR_FAILED"
lines.append("RESULT: {}".format(result))

text = "\n".join(lines) + "\n"
Path(summary_log).write_text(text)
print(text, end="")
PYEOF
SUMEOF
chmod +x "${SUMMARY_SCRIPT}"

echo "Submitting s4 array job..."
ARRAY_SUBMIT_OUTPUT="$(
  S4_SHARD_COUNT="${ARRAY_SIZE}" PYTHON_BIN="${PYTHON_BIN}" S4_GPKG_EXCLUDE_SATELLITE="${S4_GPKG_EXCLUDE_SATELLITE}" \
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
sed -i 's/-w "done(/-w "ended(/' "${TMP_FINALIZE_LSF}"

echo "Submitting finalize job (depends on array job ${ARRAY_JOB_ID})..."
FINALIZE_SUBMIT_OUTPUT="$(
  S4_SHARD_COUNT="${ARRAY_SIZE}" PYTHON_BIN="${PYTHON_BIN}" S4_GPKG_EXCLUDE_SATELLITE="${S4_GPKG_EXCLUDE_SATELLITE}" \
    bsub -q "${QUEUE}" < "${TMP_FINALIZE_LSF}"
)"
echo "${FINALIZE_SUBMIT_OUTPUT}"

FINALIZE_JOB_ID="$(printf '%s\n' "${FINALIZE_SUBMIT_OUTPUT}" | sed -n 's/.*<\([0-9][0-9]*\)>.*/\1/p')"
if [ -z "${FINALIZE_JOB_ID}" ]; then
  echo "Error: failed to parse finalize job id from bsub output."
  exit 1
fi

rm -f "${TMP_FINALIZE_LSF}"

echo "Submitting summary job (depends on finalize job ${FINALIZE_JOB_ID})..."
SUMMARY_SUBMIT_OUTPUT="$(
  bsub \
    -q "${QUEUE}" \
    -J "s4_summary" \
    -w "ended(${FINALIZE_JOB_ID})" \
    -o output/logs/s4_lsf/s4_summary.out \
    -e output/logs/s4_lsf/s4_summary.err \
    -n 1 \
    -R "rusage[mem=2G]" \
    -R "span[hosts=1]" \
    bash -lc "cd '${SCRIPT_DIR}' && S4_GPKG_EXCLUDE_SATELLITE='${S4_GPKG_EXCLUDE_SATELLITE}' '${SUMMARY_SCRIPT}' '${SUMMARY_LOG}' '${ARRAY_JOB_ID}' '${FINALIZE_JOB_ID}' '${ARRAY_SIZE}' '${PYTHON_BIN}'"
)"
echo "${SUMMARY_SUBMIT_OUTPUT}"

SUMMARY_JOB_ID="$(printf '%s\n' "${SUMMARY_SUBMIT_OUTPUT}" | sed -n 's/.*<\([0-9][0-9]*\)>.*/\1/p')"
if [ -z "${SUMMARY_JOB_ID}" ]; then
  echo "Error: failed to parse summary job id from bsub output."
  exit 1
fi

echo
echo "Submitted."
echo "Array job id: ${ARRAY_JOB_ID}"
echo "Finalize job id: ${FINALIZE_JOB_ID}"
echo "Summary job id: ${SUMMARY_JOB_ID}"
echo "S4_GPKG_EXCLUDE_SATELLITE: ${S4_GPKG_EXCLUDE_SATELLITE}"
echo "Logs:"
echo "  output/logs/s4_lsf/s4_trace.<index>.out"
echo "  output/logs/s4_lsf/s4_trace.<index>.err"
echo "  output/logs/s4_lsf/s4_trace.<index>.live.log   (runtime direct log)"
echo "  output/logs/s4_lsf/s4_finalize.out"
echo "  output/logs/s4_lsf/s4_finalize.err"
echo "  output/logs/s4_lsf/s4_finalize.live.log        (runtime direct log)"
echo "  output/logs/s4_lsf/s4_summary.out"
echo "  output/logs/s4_lsf/s4_summary.err"
echo "  ${SUMMARY_LOG}                                 (summary report)"
echo
echo "Useful commands:"
echo "  bjobs | grep s4_"
echo "  tail -f output/logs/s4_lsf/s4_trace.1.live.log"
echo "  cat ${SUMMARY_LOG}"
