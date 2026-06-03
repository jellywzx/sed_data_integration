# s10 validation results

`s10_validation_results.py` 是一个只基于 s8 发布产品的验证诊断脚本，用于补强论文
“结果与验证”第 5 节中的第一套验证：同 cluster / 同站多源 overlap 一致性与可验证性诊断。

## 1. 脚本目的

这个脚本回答两个问题：

1. s8 发布产品中的 `sed_reference_overlap_candidates.csv.gz` 是否保存了同一 `cluster_uid` 或 `cluster_id`、同一 `resolution`、同一 `time/date` 下多个来源的真实候选数值。
2. 如果保存了，计算真正的 source-pair overlap consistency metrics；如果没有保存，则输出可复现的产品诊断，说明发布产品能验证什么、不能验证什么。

## 2. 输入限制

脚本只读取 s8 release 目录内已有文件：

```bash
scripts_basin_test/output/sed_reference_release/
```

脚本不读取 s6/s7 中间文件、source station 原始 NetCDF、原始 source dataset、notebook 或历史实验脚本。这个限制是设计的一部分：本脚本只评估“发布产品自身”的可验证性。

## 3. 运行命令

默认路径已经内置，通常直接运行即可：

```bash
python s10_validation_results.py
```

脚本会按代码中内置的 `DEFAULT_WORKERS` 自动使用最多 24 个 worker process，并行检查彼此独立的 release schema 文件。标准运行不需要输入任何参数。若临时需要退回单核模式，可运行：

```bash
python s10_validation_results.py --workers 1
```

运行时会打印带时间戳的进度日志。需要注意：多核并行主要用于多个 release 文件之间的 schema inspection；后续读取 `sed_reference_master.nc` 的记录级 provenance 时，仍可能表现为单核 I/O，这是单个大型 NetCDF/HDF5 文件读取的常见瓶颈。

默认读取：

```bash
output/sed_reference_release
```

默认输出：

```bash
output/validation_results
```

如需临时覆盖路径，也可以显式指定：

```bash
python s10_validation_results.py \
  --release-dir scripts_basin_test/output/sed_reference_release \
  --out-dir scripts_basin_test/output/validation_results \
  --workers 4
```

所有 CSV、Markdown 和图件都会写入 `--out-dir`；图件写入 `--out-dir/figures`。

## 4. 输出文件含义

- `validation_product_schema_inventory.csv`：记录 s8 release 产品中的 NetCDF dimensions、variables、global attributes，catalog CSV 列，以及可读取的 GPKG 图层和字段。
  为了避免大文件全量扫描，NetCDF/CSV/GPKG 的缺失值和图层行数采用快速元数据检查；GPKG 使用 SQLite metadata 读取图层和字段，不打开几何图层。
- `validation_overlap_availability_diagnostic.csv`：诊断 release 产品是否包含 candidate-level values，以及是否支持真正的 source-pair metrics。
- `validation_selected_source_summary.csv`：按 `resolution/source/source_family` 汇总 selected / winning records。
- `validation_overlap_flag_summary.csv`：按 `resolution/source/source_family` 汇总 `is_overlap`；如果产品没有 `is_overlap` 字段，则 overlap 指标输出为缺失值并写明原因。
- `validation_overlap_candidate_summary.csv`：按 `resolution/source/source_family` 汇总 candidate sidecar 行数、selected 行数、overlap group 数和 cluster 覆盖。
- `validation_results_summary.md`：汇总输入文件、schema inspection、方法、数值结果或 skipped reason、限制和本轮跳过的验证。
- Path A 才会生成：`validation_overlap_pair_records.csv`、`validation_overlap_source_pairs.csv`、`validation_overlap_source_pairs_by_variable.csv`，以及 `Q/SSC/SSL` 的 scatter 和 bias box 图。其中 `validation_overlap_source_pairs_by_variable.csv` 保留 `resolution` 分组，`validation_overlap_source_pairs.csv` 是跨 `resolution` pooled summary。

## 5. Path A 和 Path B 的区别

Path A 仅在 s8 release 目录中存在 `sed_reference_overlap_candidates.csv.gz` 且该 sidecar 包含 candidate-level 记录时启用：同一个 `cluster_uid` 或 `cluster_id`、同一个 `resolution`、同一个 `time/date` 下，至少两个不同 `source` 或 `source_family` 同时有真实的 `Q`、`SSC` 或 `SSL` 数值。满足这个条件时，脚本会构建稳定排序的 source pair，并计算 bias、RMSE、MAE、MAPE、Pearson correlation、Spearman rank correlation、记录数、cluster 数和时间范围。

Path B 用于 sidecar 缺失、sidecar schema 不完整、sidecar 为空，或 sidecar 中没有可用多来源数值 pair 的情况。Path B 不报错、不读取上游数据、不伪造 pairwise metrics，而是输出 schema inventory、availability diagnostic、selected source summary、overlap flag summary、candidate summary 和 summary 文档。

## 6. 为什么 selected-only 产品不能计算真正 pairwise validation

`is_overlap=1` 只能说明存在 overlap 或多来源竞争，但不等价于保存了所有候选来源数值。如果 s8 发布产品只保存每个 cluster-time cell 的胜出来源，那么 non-selected sources 的数值已经不在发布产品中。真正的 source-pair overlap consistency 需要同一 cluster-time key 下 pair 两边的真实数值，因此不能从 selected-only s8 产品中计算，也不能用推断值或占位值伪造。

## 7. 后续严格验证建议

当前推荐的严格验证路径是发布 `sed_reference_overlap_candidates.csv.gz`。如果后续需要更完整审计，可将 candidate-level 表前移到 s6 生成，或另写一个明确允许读取上游候选数据的验证脚本。这样可以清楚地区分“发布产品自身可验证性诊断”和“上游候选数据审计验证”。
