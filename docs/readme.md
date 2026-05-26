# Sediment Reference Dataset Basin Pipeline

> 当前文档对应 `experiment/restore_satellite_basin` 分支下的 `scripts_basin_test` 主线流程。
>
> 主线目标：把多来源泥沙观测数据整理成一套以 `cluster_uid + resolution` 为核心连接键的 sediment reference dataset，并发布到 `scripts_basin_test/output/sed_reference_release/`。

---

## 1. 项目在做什么

这套流程用于构建 basin-based sediment reference dataset。它会把不同来源、不同时间分辨率的泥沙观测数据统一整理、做 basin tracing、合并为 cluster，并生成 NetCDF、catalog 和 GIS sidecar。

最终发布包包含：

| 类型 | 标准文件 |
|---|---|
| master NetCDF | `sed_reference_master.nc` |
| matrix NetCDF | `sed_reference_timeseries_daily.nc`、`sed_reference_timeseries_monthly.nc`、`sed_reference_timeseries_annual.nc` |
| climatology NetCDF | `sed_reference_climatology.nc` |
| catalog | `station_catalog.csv`、`source_station_catalog.csv`、`source_dataset_catalog.csv` |
| overlap provenance | `sed_reference_overlap_candidates.csv.gz` |
| satellite validation | `sed_reference_satellite_validation.nc`、`satellite_validation_catalog.csv` |
| GIS sidecar | `sed_reference_cluster_points.gpkg`、`sed_reference_source_stations.gpkg`、可选 `sed_reference_cluster_basins.gpkg` |
| 发布校验 | `release_validation_report.csv`、`release_inventory.csv`、发布版 `README.md` |

这里的 `cluster` 不是严格意义上的“同一个物理站点”，而是 basin 合并规则下的站点合并组。一个 cluster 可以包含多个原始站点；发布层标准连接键是：

```text
cluster_uid + resolution
```

---

## 2. 快速开始

### 2.1 推荐入口

优先使用一键主线脚本：

```bash
python run_s1_s8_basin_pipeline.py --help
python run_s1_s8_basin_pipeline.py
```

常用重跑方式：

```bash
# 只跑连续阶段
python run_s1_s8_basin_pipeline.py --start-at s3 --end-at s6

# 只跑指定阶段；--steps 优先级高于 --start-at/--end-at
python run_s1_s8_basin_pipeline.py --steps s4,s5,s8

# 只预览命令，不执行
python run_s1_s8_basin_pipeline.py --steps s6,s7 --dry-run
```

### 2.2 常用运行参数

| 参数 | 作用 |
|---|---|
| `--python` | 指定 Python 3 解释器 |
| `--log-file` | 指定整条流水线日志文件 |
| `--start-at` / `--end-at` | 运行一个连续阶段区间 |
| `--steps` | 运行逗号分隔的非连续阶段列表 |
| `--dry-run` | 打印命令但不执行 |
| `--s2-workers` | s2 并行数 |
| `--s3-workers` | s3 并行数 |
| `--s3-exclude-resolutions` | 默认排除 `climatology`，使其不进入 basin 主线 |
| `--s4-workers` / `--s4-batch-size` | s4 basin tracing 并行配置 |
| `--merit-dir` | MERIT Hydro 数据目录 |
| `--s6-workers` | s6 master merge 并行数 |
| `--matrix-workers` | matrix 导出总 worker 预算 |
| `--include-local-basins` | 额外生成 local basin sidecar |
| `--s8-link-mode` | 发布文件物化方式：`hardlink`、`symlink` 或 `copy` |
| `--s8-skip-gpkg` | 跳过发布层 GPKG |
| `--s8-no-basin-polygons` | 不发布 basin polygon sidecar |
| `--s8-skip-validation` | 跳过发布校验 |
| `--s8-no-force` | 不覆盖已有发布目录 |

### 2.3 路径约定

默认假设仓库目录位于：

```text
Output_r/scripts_basin_test/
```

统一输出目录为：

```text
scripts_basin_test/output/
```

如需跨平台迁移，可设置：

```bash
export OUTPUT_R_ROOT=/path/to/Output_r
```

`s4` 需要 MERIT Hydro 数据。默认路径由 `run_s1_s8_basin_pipeline.py` 推断，也可以显式传入：

```bash
python run_s1_s8_basin_pipeline.py --steps s4 --merit-dir /path/to/MERIT_Hydro_v07_Basins_v01_bugfix1
```

---

## 3. 主线阶段总览

当前主线是：

```text
s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7 -> s8
```

| 阶段 | 入口 | 主要作用 | 关键输出 |
|---|---|---|---|
| s1 | `s1_verify_time_resolution.py` | 校验输入 `nc` 的时间语义，生成 review queue 和 override 模板 | `s1_verify_time_resolution_results.csv`、`s1_resolution_review_queue.csv`、`s1_resolution_review_overrides.csv` |
| s2 | `s2_reorganize_qc_by_resolution.py` | 按 s1 判定重组文件到标准分辨率目录 | `../output_resolution_organized/`、`s2_resolution_classification_details.csv` |
| s3 | `s3_collect_qc_stations.py` | 扫描整理后的 `nc`，提取 basin 主线站点表 | `s3_collected_stations.csv` |
| s4 | `s4_basin_trace_watch.py` 或 `submit_s4_lsf.sh` | 对站点做 upstream basin tracing，并写出 basin 诊断字段 | `s4_upstream_basins.csv`、`s4_upstream_basins.gpkg`、`s4_local_catchments.gpkg`、`s4_reported_area_check.csv` |
| s5 | `s5_basin_merge.py` | 基于 basin 结果分配 cluster，生成 cluster 级站点表 | `s5_basin_clustered_stations.csv`、`s5_basin_cluster_report.csv` |
| s6 | `submit_s6_fast.sh` 或 s6 系列脚本 | 生成 master、matrix、climatology 和 satellite validation NetCDF | `s6_basin_merged_all.nc`、`s6_matrix_by_resolution/*.nc`、`s6_climatology_only.nc`、`s6_satellite_validation_only.nc` |
| s7 | `s7_export_cluster_shp.py`、`s7_export_source_station_shp.py`、`s7_export_cluster_basin_shp.py` | 导出 cluster/source/basin 空间 sidecar 和 catalog | `s7_cluster_points.gpkg`、`s7_source_stations.gpkg`、`s7_cluster_basins.gpkg`、相关 catalog |
| s8 | `s8_publish_reference_dataset.py` | 把 s6/s7 产物整理为标准发布包并执行发布校验 | `sed_reference_release/` |

---

## 4. 核心规则

### 4.1 时间分辨率规则

最终主分类只保留：

```text
daily / monthly / annual / climatology / other
```

| 原始判定 | 最终归类 |
|---|---|
| `hourly` | `daily` |
| `daily` | `daily` |
| `single_point` | `daily` 或 `climatology` |
| `monthly` | `monthly` |
| `quarterly` | `monthly` |
| `annual` | `annual` 或 `climatology` |
| 其他不明确情况 | `other` |

说明：

1. `single_point` 和 `annual` 如果从元数据判断为多年平均或气候态，则归为 `climatology`。
2. `climatology` 在 s2 之后单独保留，默认不进入 basin tracing 和 basin merge。
3. basin 主线只处理非 `climatology` 站点。
4. 第二阶段不直接信任第一阶段目录名，实际以 s1 输出的 `temporal_semantics` 为准。

### 4.2 Provenance 保留规则

最终数据必须能回答：

1. 一个 `cluster` 下有哪些原始站点。
2. 每个原始站点来自哪个数据源。
3. 每条最终记录来自哪个 `source_station_uid`。
4. 每条最终记录来自哪个原始文件路径。
5. 如果存在多来源竞争，候选来源是什么，最终胜出来源是什么。

发布层中的主要 provenance 文件是：

```text
source_station_catalog.csv
source_dataset_catalog.csv
sed_reference_overlap_candidates.csv.gz
```

### 4.3 多来源重叠规则

如果同一个 `cluster`、同一个 `resolution`、同一个时间点存在多个来源：

1. 多个来源都可以进入候选。
2. `master nc` 和 `matrix nc` 只保存最终胜出记录。
3. 胜出规则按质量分数排序。
4. `is_overlap = 1` 表示该记录来自多来源竞争。
5. 真正的 candidate-level overlap 一致性分析应使用 `sed_reference_overlap_candidates.csv.gz`。

### 4.4 Basin 发布策略

当前 basin 发布策略偏保守：

1. 发布层只区分 `resolved` 和 `unresolved`。
2. `unresolved` 记录可以保留在主数据中。
3. 只有 `resolved` 且具备有效 basin polygon 的记录才进入 basin polygon sidecar。
4. `unresolved` 记录不会被强行发布为 basin polygon。

关键 basin 诊断字段包括：

```text
distance_m
match_quality
point_in_local
point_in_basin
basin_status
basin_flag
```

几何与策略职责分工：

1. `basin_tracer.py` 负责几何计算，生成 `point_in_local` 和 `point_in_basin`。
2. `basin_policy.py` 读取诊断字段，并给出最终 `resolved / unresolved`。
3. 几何判断直接基于原始站点坐标，不做 snapping，不修改经纬度。
4. 点面判断使用 `covers()`，使落在 polygon 边界上的点也被视为在面内。
5. `s4 / s5 / s6 / s7` 负责传递和写出这些结果，不重复计算几何关系。

### 4.5 Cluster merge 规则

`s5_basin_merge.py` 的高影响规则：

1. 只有 `basin_status=resolved` 且 `basin_id` 有效的站点参与 basin cluster 合并。
2. 同一个 `basin_id` 内，只有所有跨组站点对都满足距离阈值和 `uparea_merit` 相对误差阈值时，两个候选 cluster 才能合并。
3. 合并方式为 `complete-linkage`。
4. 不满足合并条件的站点保留为 singleton，`cluster_id=station_id`。
5. `s5` 会把 basin 元数据并回站点表，并对 `unresolved` 行屏蔽部分 release-facing basin 字段。

---

## 5. s4 和 s6 的生产运行建议

### 5.1 s4：Basin tracing

生产环境推荐使用 LSF 提交脚本：

```bash
bash submit_s4_lsf.sh
bash submit_s4_lsf.sh 16
```

常用环境变量：

```text
S4_QUEUE
S4_NCORES
S4_MEM
S4_PTILE
PYTHON_BIN
```

日志目录：

```text
scripts_basin_test/output/logs/s4_lsf/
```

分片中间结果目录：

```text
scripts_basin_test/output/s4_shards/
```

如只是在本地调试，也可以通过 `run_s1_s8_basin_pipeline.py --steps s4` 或直接运行 `s4_basin_trace_watch.py`。

### 5.2 s6：NetCDF 导出

生产环境推荐使用：

```bash
bash submit_s6_fast.sh
```

`s6` 不是单一任务，而是一组可并行任务：

| 子任务 | 脚本 | 产物 |
|---|---|---|
| merge | `s6_basin_merge_to_nc.py` | `s6_basin_merged_all.nc`、`s6_cluster_quality_order.csv` |
| daily | `s6_export_daily_matrix_nc.py` | `s6_basin_matrix_daily.nc` |
| monthly | `s6_export_monthly_matrix_nc.py` | `s6_basin_matrix_monthly.nc` |
| annual | `s6_export_annual_matrix_nc.py` | `s6_basin_matrix_annual.nc` |
| climatology | `s6_export_climatology_to_nc.py` | `s6_climatology_only.nc` |
| satellite validation | `s6_export_satellite_validation_to_nc.py` | `s6_satellite_validation_only.nc`、`s6_satellite_validation_catalog.csv` |

常用环境变量：

```text
RUN_ONLY
DRY_RUN
LSF_QUEUE
LSF_PROJECT
LSF_EXTRA
MERGE_N / MERGE_WORKERS / MERGE_METADATA_WORKERS
DAILY_N / DAILY_WORKERS
MONTHLY_N / MONTHLY_WORKERS
ANNUAL_N / ANNUAL_WORKERS
CLIM_N
SATVAL_N
```

---

## 6. 发布包结构

完整发布目录：

```text
scripts_basin_test/output/sed_reference_release/
├── sed_reference_master.nc
├── sed_reference_timeseries_daily.nc
├── sed_reference_timeseries_monthly.nc
├── sed_reference_timeseries_annual.nc
├── sed_reference_climatology.nc
├── station_catalog.csv
├── source_station_catalog.csv
├── source_dataset_catalog.csv
├── sed_reference_overlap_candidates.csv.gz
├── sed_reference_satellite_validation.nc
├── satellite_validation_catalog.csv
├── sed_reference_cluster_points.gpkg
├── sed_reference_source_stations.gpkg
├── sed_reference_cluster_basins.gpkg
├── release_validation_report.csv
├── release_inventory.csv
└── README.md
```

其中：

1. `sed_reference_master.nc` 保留记录级 provenance，适合审计和回溯。
2. `sed_reference_timeseries_*.nc` 是 `station x time` 矩阵，适合最近站点匹配、抽取参考时间序列和模型对比。
3. `sed_reference_climatology.nc` 单独发布，不进入 basin 主线 merge。
4. `station_catalog.csv` 是发布层主索引，一行对应一个 `cluster_uid + resolution`。
5. `source_station_catalog.csv` 用于回查原始站点。
6. `source_dataset_catalog.csv` 用于回查数据源级信息。
7. `sed_reference_overlap_candidates.csv.gz` 用于 source-pair overlap candidate 分析。
8. `sed_reference_cluster_points.gpkg` 提供 cluster 点位图层。
9. `sed_reference_source_stations.gpkg` 提供 source station 点位图层。
10. `sed_reference_cluster_basins.gpkg` 提供 resolved basin polygon sidecar。

---

## 7. 发布数据的标准使用方式

面向下游用户时，推荐按下面顺序使用发布包：

1. 根据模型或分析目标选择 `daily / monthly / annual`。
2. 读取对应的 matrix NetCDF：
   - `sed_reference_timeseries_daily.nc`
   - `sed_reference_timeseries_monthly.nc`
   - `sed_reference_timeseries_annual.nc`
3. 读取 `station_catalog.csv`，过滤到目标 `resolution`。
4. 用目标经纬度在过滤后的站点中找最近 `cluster_uid`，或使用 `sed_reference_cluster_points.gpkg` 做空间匹配。
5. 从 matrix NetCDF 中抽取该 `cluster_uid` 的 `Q / SSC / SSL` 时间序列。
6. 如需记录级 provenance，查询 `sed_reference_master.nc`。
7. 如需原始站点和原始路径，查询 `source_station_catalog.csv`。
8. 如需 overlap 候选来源和 source-pair 一致性分析，使用 `sed_reference_overlap_candidates.csv.gz`。

仓库中也提供了一个示例脚本：

```bash
python tools/example_reference_workflow.py \
  --release-dir /path/to/sed_reference_release \
  --resolution monthly \
  --lat 30.5 \
  --lon 114.3 \
  --variable SSC
```

可选模型对比：

```bash
python tools/example_reference_workflow.py \
  --release-dir /path/to/sed_reference_release \
  --resolution monthly \
  --lat 30.5 \
  --lon 114.3 \
  --variable SSC \
  --model-nc /path/to/model.nc \
  --model-var sediment \
  --out-csv /tmp/aligned_timeseries.csv
```

---

## 8. 什么时候需要重跑

| 变化 | 建议重跑范围 | 原因 |
|---|---|---|
| 时间分辨率规则变化 | 从 s2 起 | s2 会重组目录，后续站点表和 station_id 都会变化 |
| `single_point / quarterly / annual / climatology` 判定逻辑变化 | 从 s2 起 | 会改变进入各分辨率目录的文件 |
| basin tracing 或 `basin_status` 规则变化 | 从 s4 起 | s4 重新生成 basin 诊断，s5 以后都依赖它 |
| cluster merge 规则变化 | 从 s5 起 | cluster_id / cluster_uid 可能变化 |
| s6 输出字段或发布 contract 变化 | 至少重跑 s6 -> s8 | master/matrix/release 需要保持一致 |
| 只改发布层命名、link mode、是否输出 GPKG | 通常只需重跑 s8 | s8 负责发布包物化和校验 |

注意：`s3` 生成的站点顺序会影响后续 `station_id`，而 `station_id` 又会影响 `s4 / s5 / s6 / s7`。如果不确定上游是否变化，优先从较早阶段重跑。

---

## 9. 依赖

常见 Python 依赖：

```text
pandas
numpy
xarray
netCDF4
h5netcdf
geopandas
pyshp
matplotlib
```

部分脚本的额外说明：

1. `s7_export_cluster_shp.py` 需要 `pyshp + geopandas`。
2. `s7_export_source_station_shp.py` 和 `s7_export_cluster_basin_shp.py` 需要 `geopandas`。
3. `s8_publish_reference_dataset.py` 在发布 GPKG sidecar 时需要 `geopandas`。
4. `tools/example_reference_workflow.py` 读取 NetCDF 需要 `netCDF4`，可选模型对比需要 `xarray`，画图需要 `matplotlib`。

---

## 10. 代码导航

| 文件 | 作用 |
|---|---|
| `run_s1_s8_basin_pipeline.py` | s1-s8 一键主线入口，支持区间运行、指定阶段运行和 dry-run |
| `pipeline_paths.py` | 统一维护主线输出路径、发布包路径和日志路径 |
| `time_resolution.py` | 时间分辨率判定逻辑 |
| `basin_tracer.py` | upstream basin tracing 和点面几何诊断 |
| `basin_policy.py` | basin 诊断结果到 `resolved / unresolved` 的发布策略 |
| `s1_verify_time_resolution.py` | s1 时间分辨率验证 |
| `s2_reorganize_qc_by_resolution.py` | s2 按分辨率重组输入文件 |
| `s3_collect_qc_stations.py` | s3 收集 basin 主线站点表 |
| `s4_basin_trace_watch.py` | s4 basin tracing 主脚本 |
| `s5_basin_merge.py` | s5 basin cluster 合并 |
| `s6_basin_merge_to_nc.py` | s6 master NetCDF 导出 |
| `s6_export_*_matrix_nc.py` | s6 daily/monthly/annual matrix NetCDF 导出 |
| `s6_export_climatology_to_nc.py` | climatology 独立 NetCDF 导出 |
| `s6_export_satellite_validation_to_nc.py` | satellite validation-only 数据导出 |
| `s7_export_cluster_shp.py` | cluster 点位 GPKG 和 catalog 导出 |
| `s7_export_source_station_shp.py` | source station GPKG 和 catalog 导出 |
| `s7_export_cluster_basin_shp.py` | cluster basin polygon GPKG 导出 |
| `s8_publish_reference_dataset.py` | 标准发布包生成、发布 README、validation 和 inventory |
| `tools/example_reference_workflow.py` | 发布包下游使用示例 |

---

## 11. 非主线脚本说明

仓库里仍保留一些历史脚本、兼容脚本或人工审计脚本，例如：

```text
s4_cluster_qc_stations.py
s6_merge_timeseries_by_cluster.py
s7_merge_overlap_by_cluster.py
s8_merge_qc_csv_to_one_nc.py
s6_summarize_matrix_ncs.py
```

这些脚本不是当前 `s1 -> s8` 主线发布 contract 的一部分。人工质检与审计脚本可按需要单独运行，但不应替代主线发布流程。

---

## 12. 一句话总结

当前主线按 `s1 -> s8` 构建 basin-based sediment reference dataset：`daily / monthly / annual` 进入 basin 主线，`climatology` 单独导出；发布层以 `cluster_uid + resolution` 为标准连接键，保留 master、matrix、catalog、空间 sidecar、satellite validation 和 overlap provenance，并只为 `resolved` 结果发布 basin polygon sidecar。
