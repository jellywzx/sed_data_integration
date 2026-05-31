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
# 重新跑（清除旧 shard 文件强制重新 trace），否则如果有shard文件的情况是默认跳过：
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
PYTHON_BIN="${PYTHON_BIN:-python3}"

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/submit_s4_lsf.py" "$@"
