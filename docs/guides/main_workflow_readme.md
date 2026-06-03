# Sediment Reference Dataset Basin Pipeline

> 当前文档对应 `master` 分支下的 `scripts_basin_test` 主线流程。
>
> 主线目标：把多来源泥沙观测数据整理成一套以 `cluster_uid + resolution` 为核心连接键的 basin-based sediment reference dataset，并发布到 `scripts_basin_test/output/sed_reference_release/`。

---

## 1. 项目简介

本仓库用于构建一套泥沙观测参考数据集。流程会把多来源、不同时间分辨率的 QC 后 NetCDF 数据统一整理，识别时间分辨率，执行 upstream basin tracing，按 basin 规则合并站点，最后生成可供模型验证、站点匹配和 provenance 回溯使用的发布包。

最终发布包位于：

```text
scripts_basin_test/output/sed_reference_release/
```

发布层的核心连接键是：

```text
cluster_uid + resolution
```

其中，`cluster` 不是严格意义上的“同一个物理站点”，而是 basin 合并规则下形成的站点合并组。一个 cluster 可以包含多个原始站点，并保留到 source station 与原始文件路径的追溯关系。

---

## 2. 最终发布内容

发布包主要包含以下文件：

| 类型 | 标准文件 |
|---|---|
| Master NetCDF | `sed_reference_master.nc` |
| Matrix NetCDF | `sed_reference_timeseries_daily.nc`、`sed_reference_timeseries_monthly.nc`、`sed_reference_timeseries_annual.nc` |
| Climatology NetCDF | `sed_reference_climatology.nc` |
| Satellite NetCDF | `sed_reference_satellite.nc` |
| Catalog | `station_catalog.csv`、`source_station_catalog.csv`、`source_dataset_catalog.csv`、`satellite_catalog.csv` |
| Overlap provenance | `sed_reference_overlap_candidates.csv.gz` |
| GIS sidecar | `sed_reference_cluster_points.gpkg`、`sed_reference_source_stations.gpkg`、可选 `sed_reference_cluster_basins.gpkg` |
| Release validation | `release_validation_report.csv`、`release_inventory.csv`、发布版 `README.md` |

推荐把发布结果理解为五层：

1. `master` 层：以 `s6_basin_merged_all.nc` 为基础，保留记录级 provenance。
2. `matrix` 层：以 `daily / monthly / annual` 三个矩阵 NetCDF 为基础，适合最近站点匹配、时间序列抽取和模型对比。
3. `climatology` 层：以 `s6_climatology_only.nc` 为基础，独立发布，不进入 basin 主线 merge。
4. `satellite` 层：以 satellite source family 为基础，独立发布为 `sed_reference_satellite.nc`，用于 satellite-vs-station validation、空间诊断和下游对比；默认不进入主 station-reference merge。
5. `release` 层：由 `s8_publish_reference_dataset.py` 整理为标准命名的对外交付数据包。

> 注意：旧命名 `sed_reference_satellite_validation.nc` 和 `satellite_validation_catalog.csv` 如仍存在，仅作为兼容别名；推荐使用新的发布级文件名 `sed_reference_satellite.nc` 和 `satellite_catalog.csv`。
> `s8_publish_reference_dataset.py` 默认要求发布级 satellite NetCDF 和 catalog 同时存在；缺任一文件时 release 会失败。
> Satellite-only cluster 允许不出现在主 station catalog 中；发布校验要求其 `cluster_uid / cluster_id` 自洽，并在报告中统计可联到主 catalog 的数量。

---

## 3. Quick start

### 3.1 一键运行主线

推荐使用统一入口运行 `s1 -> s8`：

```bash
python run_s1_s8_basin_pipeline.py --help
python run_s1_s8_basin_pipeline.py
```

常用运行方式：

```bash
# 只运行连续阶段
python run_s1_s8_basin_pipeline.py --start-at s3 --end-at s6

# 只运行指定阶段；--steps 优先级高于 --start-at/--end-at
python run_s1_s8_basin_pipeline.py --steps s4,s5,s8

# 只打印命令，不实际执行
python run_s1_s8_basin_pipeline.py --steps s6,s7 --dry-run
```

### 3.2 常用参数

| 参数 | 作用 |
|---|---|
| `--python` | 指定 Python 3 解释器 |
| `--log-file` | 指定整条流水线日志文件 |
| `--start-at` / `--end-at` | 运行一个连续阶段区间 |
| `--steps` | 运行逗号分隔的非连续阶段列表 |
| `--dry-run` | 打印命令但不执行 |
| `--strict-s1` | s1 返回非零时强制视为失败 |
| `--s2-workers` | s2 并行数 |
| `--s2-clear` | 给 s2 传入 `--clear-all` |
| `--s3-workers` | s3 并行数 |
| `--s3-exclude-resolutions` | 默认排除 `climatology`，使其不进入 basin 主线 |
| `--s4-workers` | s4 basin tracing worker 数 |
| `--s4-batch-size` | s4 分批大小 |
| `--s4-no-resume` | 禁用 s4 resume 模式 |
| `--s4-no-gpkg` | 禁用 s4 GPKG 输出 |
| `--merit-dir` | MERIT Hydro 数据目录 |
| `--s6-workers` | s6 master merge worker 数 |
| `--matrix-workers` | matrix 导出总 worker 预算 |
| `--matrix-resolution-workers` | matrix 分辨率级 worker 配置 |
| `--s6-include-climatology` | 将 climatology 纳入 s6 主 merge |
| `--skip-climatology-export` | 跳过 climatology 独立导出 |
| `--include-local-basins` | 额外生成 local basin sidecar |
| `--s8-link-mode` | 发布文件物化方式：`hardlink`、`symlink` 或 `copy` |
| `--s8-skip-gpkg` | 跳过发布层 GPKG |
| `--s8-no-basin-polygons` | 不发布 basin polygon sidecar |
| `--s8-skip-validation` | 跳过发布校验 |
| `--s8-no-force` | 不覆盖已有发布目录 |

---

## 4. 路径约定

默认假设仓库目录位于：

```text
Output_r/scripts_basin_test/
```

统一输出目录为：

```text
scripts_basin_test/output/
```

分辨率整理后的数据目录为：

```text
../output_resolution_organized/
```

日志目录为：

```text
scripts_basin_test/output/logs/
```

如需跨平台迁移或从其他目录运行，可设置：

```bash
export OUTPUT_R_ROOT=/path/to/Output_r
```

`s4` 需要 MERIT Hydro 数据。默认路径由脚本推断，也可以显式传入：

```bash
python run_s1_s8_basin_pipeline.py \
  --steps s4 \
  --merit-dir /path/to/MERIT_Hydro_v07_Basins_v01_bugfix1
```

---

## 5. 主线流程总览

当前主线为：

```text
s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7 -> s8
```

| 阶段 | 入口 | 主要作用 | 关键输出 |
|---|---|---|---|
| s1 | `s1_verify_time_resolution.py` | 校验输入 NetCDF 的时间语义，生成主分类结果、人工 review queue 和 override 模板 | `s1_verify_time_resolution_results.csv`、`s1_resolution_review_queue.csv`、`s1_resolution_review_overrides.csv` |
| s2 | `s2_reorganize_qc_by_resolution.py` | 按 s1 判定结果重组 QC 文件到标准分辨率目录 | `../output_resolution_organized/`、`s2_resolution_classification_details.csv` |
| s3 | `s3_collect_qc_stations.py` | 扫描整理后的 NetCDF，提取 basin 主线使用的站点元数据 | `s3_collected_stations.csv` |
| s4 | `s4_basin_trace_watch.py` 或 `submit_s4_lsf.sh` | 为站点执行 upstream basin tracing，生成 basin 匹配结果和诊断字段 | `s4_upstream_basins.csv`、`s4_upstream_basins.gpkg`、`s4_local_catchments.gpkg`、`s4_reported_area_check.csv` |
| s5 | `s5_basin_merge.py` | 基于 s4 basin 结果分配 `cluster_id`，生成 cluster 级站点表 | `s5_basin_clustered_stations.csv`、`s5_basin_cluster_report.csv` |
| s6 | `submit_s6_fast.sh` 或 s6 系列脚本 | 生成 master、matrix、climatology 和 satellite NetCDF | `s6_basin_merged_all.nc`、`s6_matrix_by_resolution/*.nc`、`s6_climatology_only.nc`、`s6_satellite_validation_only.nc` |
| s7 | `s7_export_cluster_shp.py`、`s7_export_source_station_shp.py`、`s7_export_cluster_basin_shp.py` | 导出 cluster/source/basin 空间 sidecar 和 catalog | `s7_cluster_points.gpkg`、`s7_source_stations.gpkg`、`s7_cluster_basins.gpkg`、相关 catalog |
| s8 | `s8_publish_reference_dataset.py` | 把 s6/s7 产物整理成标准发布包，并执行发布校验 | `sed_reference_release/` |

---

## 6. Satellite 发布规则

Satellite 是发布包的强制组成部分。完整 release 必须包含：

```text
sed_reference_satellite.nc
satellite_catalog.csv
```

Satellite 数据的设计原则：

1. 从 s5 candidates 中筛选 `source_family == satellite` 的记录。
2. 仅保留标准时间分辨率：`daily / monthly / annual`。
3. 通过 `cluster_uid / cluster_id` 与主线 basin cluster 关联。
4. 不进入 `sed_reference_master.nc`。
5. 不进入 `sed_reference_timeseries_daily.nc`、`sed_reference_timeseries_monthly.nc`、`sed_reference_timeseries_annual.nc`。
6. 使用场景是 satellite-vs-station validation、spatial diagnostics 和 downstream comparison。
7. 如果 satellite NetCDF 或 satellite catalog 缺失，发布流程应失败，而不是生成不完整 release。

`s6` 阶段当前 satellite 中间产物为：

```text
scripts_basin_test/output/s6_satellite_validation_only.nc
scripts_basin_test/output/s6_satellite_validation_catalog.csv
```

`s8` 发布阶段应将它们发布为：

```text
scripts_basin_test/output/sed_reference_release/sed_reference_satellite.nc
scripts_basin_test/output/sed_reference_release/satellite_catalog.csv
```

Satellite NetCDF 主要包含：

```text
n_satellite_stations
n_satellite_records
n_sources
```

站点级常见字段：

```text
satellite_station_uid
cluster_uid
cluster_id_station
source
source_family
source_station_native_id
station_name
river_name
station_resolution
lat
lon
candidate_path
resolved_candidate_path
merge_policy
validation_only
```

记录级常见字段：

```text
satellite_station_index
cluster_id
time
date
resolution
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
```

Satellite catalog 主要字段：

```text
satellite_station_uid
cluster_uid
cluster_id
source
source_family
resolution
lat
lon
station_name
river_name
source_station_native_id
candidate_path
resolved_candidate_path
n_records
time_start
time_end
validation_only
merge_policy
```

---

## 7. Climatology 发布规则

Climatology 是发布包中的独立参考层。它必须单独发布为：

```text
sed_reference_climatology.nc
```

Climatology 数据的设计原则：

1. 从 `output_resolution_organized/climatology/` 单独扫描和导出。
2. 不进入 basin tracing。
3. 不进入 basin cluster merge。
4. 不进入 `sed_reference_master.nc`。
5. 不进入 `sed_reference_timeseries_daily.nc`、`sed_reference_timeseries_monthly.nc`、`sed_reference_timeseries_annual.nc`。
6. 不使用 `cluster_uid + resolution` 作为主索引。
7. 使用 climatology 产品内部的 `station_uid` 作为稳定站点键。
8. 主要用于多年平均、气候态或无明确逐日/逐月/逐年时间序列语义的数据。
9. 下游分析时应与 `daily / monthly / annual` 时间序列产品分开使用，不应自动和 matrix 文件混合。

`s6` 阶段 climatology 中间产物为：

```text
scripts_basin_test/output/s6_climatology_only.nc
scripts_basin_test/output/s6_climatology_stations.shp
```

`s8` 发布阶段应将主 NetCDF 发布为：

```text
scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc
```

Climatology 发布约束：

1. `sed_reference_climatology.nc` 是 release 的核心 NetCDF 之一。
2. 文件中的记录应只对应 `climatology` resolution 语义。
3. release validation 应检查 climatology 记录数、时间覆盖和 resolution code 是否一致。
4. 如果 climatology 文件缺失，完整 release 应失败。
5. 如果 climatology 文件包含非 climatology resolution code，应视为 mixed-run 或上游分类错误，需要从较早阶段重跑。

Climatology NetCDF 常见字段包括：

```text
station_uid
source_station_path
time
temporal_span
resolution
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
lat
lon
source
```

推荐使用方式：

1. 直接读取 `sed_reference_climatology.nc`。
2. 使用 `station_uid` 定位 climatology station。
3. 使用 `source_station_path` 回溯原始文件。
4. 如需与模型或 station matrix 对比，应先明确 climatology 的统计语义和模型输出的统计窗口。
5. 不要把 climatology 自动当作 daily/monthly/annual 某个时间步的观测值。

---

## 8. 核心规则

### 8.1 时间分辨率规则

最终主分类包括：

```text
daily / monthly / annual / climatology / other
```

当前归类规则：

| 原始判定 | 最终归类 |
|---|---|
| `hourly` | `daily` |
| `daily` | `daily` |
| `single_point` | `daily` 或 `climatology` |
| `monthly` | `monthly` |
| `quarterly` | `monthly` |
| `annual` | `annual` 或 `climatology` |
| 其他不明确情况 | `other` |

补充说明：

1. `single_point` 如果从元数据判断是多年平均或气候态，则归为 `climatology`。
2. `annual` 如果从元数据判断是多年平均或气候态，则归为 `climatology`。
3. `climatology` 在 s2 后单独保留，但默认不进入 basin 主线。
4. basin 主线默认只处理非 `climatology` 站点。
5. s2 不直接信任输入目录名，实际以 s1 输出的 `temporal_semantics` 为准。

### 8.2 Cluster 规则

1. 一个 `cluster` 可以包含多个原始站点。
2. 主线最终必须同时保留 cluster 层、原始站点层和记录层。
3. 最终记录必须能追溯到 `source_station_uid` 和原始文件路径。
4. 发布层标准 join key 是 `cluster_uid + resolution`。

### 8.3 Provenance 规则

最终数据需要支持以下追溯：

1. 一个 `cluster` 下有哪些原始站点。
2. 每个原始站点来自哪个数据源。
3. 每条最终记录来自哪个 `source_station_uid`。
4. 每条最终记录来自哪个原始文件路径。
5. 如果存在多来源竞争，候选来源是什么，最终胜出来源是什么。

主要 provenance 文件包括：

```text
source_station_catalog.csv
source_dataset_catalog.csv
sed_reference_overlap_candidates.csv.gz
```

### 8.4 多来源重叠规则

如果同一个 `cluster`、同一个 `resolution`、同一个时间点存在多个来源：

1. 多个来源都可以进入候选。
2. 最终记录层只保留一条胜出记录。
3. 胜出规则按质量分数排序。
4. `is_overlap = 1` 表示该记录来自多来源竞争。
5. `master nc` 和 `matrix nc` 只保存胜出记录。
6. 真正的 source-pair overlap 一致性分析应使用 `sed_reference_overlap_candidates.csv.gz`。

### 8.5 Basin 发布策略

当前 basin 发布策略采用保守规则：

1. 发布层只区分 `resolved` 和 `unresolved`。
2. `unresolved` 记录可以保留在主数据中。
3. 是否进入 basin polygon sidecar 属于 s7/s8 发布层规则，不属于 s5 合并规则本身。
4. 只有 `resolved` 且具备有效 basin polygon 的记录才进入 basin polygon sidecar。

关键 basin 诊断字段：

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
2. `basin_policy.py` 负责读取诊断结果，并给出最终 `resolved / unresolved`。
3. 几何判断直接基于原始站点坐标。
4. 当前不做 snapping，不修改原始经纬度。
5. 点面判断使用 `covers()` 而不是 `contains()`，使边界点也视为在面内。
6. `s4 / s5 / s6 / s7` 只传递和写出这些结果，不重复计算几何关系。

### 8.6 Basin cluster merge 规则

`s5_basin_merge.py` 的高影响规则：

1. 只有 `basin_status=resolved` 且 `basin_id` 有效的站点才参与 basin cluster 合并。
2. 同一 `basin_id` 内，只有所有跨组站点对都满足距离阈值和 `uparea_merit` 相对误差阈值时，两个候选 cluster 才能合并。
3. 合并方式是 `complete-linkage`。
4. 不满足条件的站点保留为 singleton，`cluster_id=station_id`。
5. `s5` 会把 basin 元数据并回站点表，并对 `unresolved` 行屏蔽部分 release-facing basin 字段。

---

## 9. 数据结构

### 9.1 Cluster 层

常见关键字段：

```text
cluster_uid
cluster_id
lat
lon
basin_area
pfaf_code
basin_status
basin_flag
basin_distance_m
point_in_local
point_in_basin
n_source_stations_in_cluster
```

### 9.2 原始站点层

常见关键字段：

```text
source_station_uid
source_station_native_id
source_station_name
source_station_river_name
source_station_lat
source_station_lon
source_station_paths
source_station_resolutions
```

### 9.3 观测记录层

常见关键字段：

```text
station_index
source_station_index
time
resolution
Q
SSC
SSL
source
is_overlap
```

---

## 10. s4 和 s6 的生产运行建议

### 10.1 s4：Basin tracing

生产环境推荐使用 LSF 提交脚本：

```bash
bash submit_s4_lsf.sh
bash submit_s4_lsf.sh 16
```

`submit_s4_lsf.sh` 是兼容入口，内部调用 `submit_s4_lsf.py`，会提交三阶段 LSF 流程：

1. 先跑 `s4_trace[1-N]` 数组分片任务。
2. 再自动提交 finalize 合并任务。
3. 最后提交 summary 检查任务。

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

如只是在本地调试，也可以直接运行：

```bash
python s4_basin_trace_watch.py
```

或通过统一入口运行：

```bash
python run_s1_s8_basin_pipeline.py --steps s4
```

默认统一入口会调用 `submit_s4_lsf.py --wait` 将 S4 提交到 LSF，并等待 summary 完成后再进入下一阶段；如需回到本地执行，使用：

```bash
python run_s1_s8_basin_pipeline.py --steps s4 --local-s4
```

### 10.2 s6：NetCDF 导出

生产环境推荐使用：

```bash
python submit_s6_fast.py --wait
```

`bash submit_s6_fast.sh` 仍可作为兼容入口，内部会调用 Python submitter。`s6` 是一组并行任务，不是单一脚本。`submit_s6_fast.py` 当前会并行提交：

| 子任务 | 脚本 | 产物 |
|---|---|---|
| merge | `s6_basin_merge_to_nc.py` | `s6_basin_merged_all.nc`、`s6_cluster_quality_order.csv` |
| daily | `s6_export_daily_matrix_nc.py` | `s6_basin_matrix_daily.nc` |
| monthly | `s6_export_monthly_matrix_nc.py` | `s6_basin_matrix_monthly.nc` |
| annual | `s6_export_annual_matrix_nc.py` | `s6_basin_matrix_annual.nc` |
| clim | `s6_export_climatology_to_nc.py` | `s6_climatology_only.nc` |
| satellite | `s6_export_satellite_validation_to_nc.py` | `s6_satellite_validation_only.nc`、`s6_satellite_validation_catalog.csv` |

提交脚本还会额外提交一个依赖型 `summary` 任务，用于检查关键输出是否齐全；统一入口默认调用 `submit_s6_fast.py --wait`，等 S6 集群任务完成后再进入 S7。若需回到本地顺序执行，使用 `--local-s6`。

常用环境变量：

```text
RUN_ONLY
DRY_RUN
LSF_QUEUE
LSF_PROJECT
LSF_EXTRA
MERGE_N
MERGE_WORKERS
MERGE_METADATA_WORKERS
DAILY_N
DAILY_WORKERS
MONTHLY_N
MONTHLY_WORKERS
ANNUAL_N
ANNUAL_WORKERS
CLIM_N
SATVAL_N
```

---

## 11. 空间文件说明

当前主线空间产品统一使用 `GPKG`。发布层中的标准空间文件由 `s8_publish_reference_dataset.py` 生成。

### 11.1 Cluster 点位文件

文件：

```text
s7_cluster_points.gpkg
sed_reference_release/sed_reference_cluster_points.gpkg
```

作用：

1. 提供 `cluster_summary / cluster_daily / cluster_monthly / cluster_annual` 多图层。
2. 作为 NetCDF、catalog 与空间数据之间的多分辨率连接层。
3. 标准 join key 是 `cluster_uid + resolution`。

### 11.2 原始站点点位文件

文件：

```text
s7_source_stations.gpkg
sed_reference_release/sed_reference_source_stations.gpkg
```

作用：

1. 查看某个 `cluster_uid + resolution` 内有哪些原始站点参与。
2. 标准 join key 是 `source_station_uid + resolution`。

### 11.3 Cluster 级流域单元文件

文件：

```text
s7_cluster_basins.gpkg
sed_reference_release/sed_reference_cluster_basins.gpkg
```

作用：

1. 查看每个 `cluster_uid + resolution` 对应的最终流域面。
2. 与 cluster 点位文件和发布 catalog 按复合键做空间联动。
3. 只为 `basin_status=resolved` 的记录导出 basin polygon。
4. `unresolved` 记录即使保留在主数据中，也不会进入 basin polygon sidecar。

---

## 12. 发布包结构

完整发布目录示例：

```text
scripts_basin_test/output/sed_reference_release/
├── sed_reference_master.nc
├── sed_reference_timeseries_daily.nc
├── sed_reference_timeseries_monthly.nc
├── sed_reference_timeseries_annual.nc
├── sed_reference_climatology.nc
├── sed_reference_satellite.nc
├── station_catalog.csv
├── source_station_catalog.csv
├── source_dataset_catalog.csv
├── satellite_catalog.csv
├── sed_reference_overlap_candidates.csv.gz
├── sed_reference_cluster_points.gpkg
├── sed_reference_source_stations.gpkg
├── sed_reference_cluster_basins.gpkg
├── release_validation_report.csv
├── release_inventory.csv
└── README.md
```

其中：

1. `sed_reference_master.nc` 保留记录级 provenance，适合审计和回溯。
2. `sed_reference_timeseries_*.nc` 是 `station x time` 矩阵，适合最近站点匹配、时间序列抽取和模型对比。
3. `sed_reference_climatology.nc` 单独发布，不进入 basin 主线 merge。
4. `sed_reference_satellite.nc` 是发布级 satellite 数据集，强制出现在 release 中，但不进入主 station-reference merge。
5. `station_catalog.csv` 是发布层主索引，一行对应一个 `cluster_uid + resolution`。
6. `source_station_catalog.csv` 用于回查原始站点。
7. `source_dataset_catalog.csv` 用于回查数据源级信息。
8. `satellite_catalog.csv` 用于回查 satellite station、source、时间范围和原始路径。
9. `sed_reference_overlap_candidates.csv.gz` 用于 source-pair overlap candidate 分析。
10. `sed_reference_cluster_points.gpkg` 提供 cluster 点位图层。
11. `sed_reference_source_stations.gpkg` 提供 source station 点位图层。
12. `sed_reference_cluster_basins.gpkg` 提供 resolved basin polygon sidecar。
13. `release_validation_report.csv` 和 `release_inventory.csv` 用于发布前后检查。

---

## 13. 发布数据的标准使用方式

面向下游用户时，推荐按下面顺序使用发布包：

1. 根据模型输出或分析目标选择目标时间分辨率：`daily / monthly / annual`。
2. 读取对应 matrix NetCDF：
   - `sed_reference_timeseries_daily.nc`
   - `sed_reference_timeseries_monthly.nc`
   - `sed_reference_timeseries_annual.nc`
3. 读取 `station_catalog.csv`，过滤到目标 `resolution`。
4. 用过滤后的 `lat/lon` 或 `sed_reference_cluster_points.gpkg` 找最近的 `cluster_uid`。
5. 从 matrix NetCDF 中抽取该 `cluster_uid` 的 `Q / SSC / SSL` 时间序列。
6. 将参考时间序列与模型结果按时间对齐。
7. 如需完整记录级 provenance，查询 `sed_reference_master.nc`。
8. 如需原始站点和原始路径，查询 `source_station_catalog.csv`。
9. 如需 climatology 参考值，单独读取 `sed_reference_climatology.nc`，不要自动混入 matrix 时间序列。
10. 如需 satellite-vs-station validation 或空间诊断，读取 `sed_reference_satellite.nc` 和 `satellite_catalog.csv`。
11. 如需真正的 source-pair overlap 指标，使用 `sed_reference_overlap_candidates.csv.gz`。

如仓库中存在示例脚本，可参考：

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

## 14. 推荐运行顺序

如果是完整重跑，推荐顺序为：

```text
s1_verify_time_resolution.py
s2_reorganize_qc_by_resolution.py
s3_collect_qc_stations.py
submit_s4_lsf.sh
submit_s4_lsf.py
s5_basin_merge.py
submit_s6_fast.sh
submit_s6_fast.py
s7_export_cluster_shp.py
s7_export_source_station_shp.py
s7_export_cluster_basin_shp.py
s8_publish_reference_dataset.py
```

如果是调试或单步运行，可以按底层 Python 脚本顺序执行：

```text
s4_basin_trace_watch.py
s5_basin_merge.py
s6_basin_merge_to_nc.py
s6_export_daily_matrix_nc.py
s6_export_monthly_matrix_nc.py
s6_export_annual_matrix_nc.py
s6_export_climatology_to_nc.py
s6_export_satellite_validation_to_nc.py
s7_export_cluster_shp.py
s7_export_source_station_shp.py
s7_export_cluster_basin_shp.py
s8_publish_reference_dataset.py
```

更推荐使用统一入口控制阶段：

```bash
python run_s1_s8_basin_pipeline.py --start-at s1 --end-at s8
```

---

## 15. 什么时候需要重跑

| 变化 | 建议重跑范围 | 原因 |
|---|---|---|
| 时间分辨率规则变化 | 从 s2 起 | s2 会改变整理后的文件目录，后续站点表和 station_id 都会变化 |
| `single_point / quarterly / annual / climatology` 判定逻辑变化 | 从 s2 起 | 会改变进入各分辨率目录的文件 |
| basin tracing 或 `basin_status` 规则变化 | 从 s4 起 | s4 重新生成 basin 诊断，s5 以后都依赖它 |
| cluster merge 规则变化 | 从 s5 起 | cluster_id / cluster_uid 可能变化 |
| s6 输出字段或发布 contract 变化 | 至少重跑 s6 -> s8 | master、matrix、climatology、satellite 和 release 需要保持一致 |
| climatology 分类规则或 climatology schema 变化 | 至少重跑 s6 -> s8，必要时从 s2 起 | climatology 独立产品依赖 s2 分类目录和 s6 独立导出 |
| satellite source family 或 satellite schema 变化 | 至少重跑 s6 -> s8 | satellite NetCDF 和 catalog 是强制 release 产物 |
| 只改发布层命名、link mode、是否输出 GPKG | 通常只需重跑 s8 | s8 负责发布包物化和校验 |

注意：

1. `s2` 会改变整理后的文件目录。
2. `s3` 会重建站点列表。
3. `s3` 的站点顺序会影响后续 `station_id`。
4. `station_id` 又会影响 `s4 / s5 / s6 / s7`。

如果不确定上游是否变化，优先从较早阶段重跑。

---

## 16. 依赖说明

常见 Python 依赖包括：

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
4. 示例工作流读取 NetCDF 需要 `netCDF4`；模型对比通常需要 `xarray`；画图需要 `matplotlib`。

---

## 17. 代码导航

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
| `submit_s4_lsf.py` | Python LSF submitter，提交并等待 S4 array/finalize/summary |
| `s5_basin_merge.py` | s5 basin cluster 合并 |
| `s6_basin_merge_to_nc.py` | s6 master NetCDF 导出 |
| `s6_export_daily_matrix_nc.py` | s6 daily matrix NetCDF 导出 |
| `s6_export_monthly_matrix_nc.py` | s6 monthly matrix NetCDF 导出 |
| `s6_export_annual_matrix_nc.py` | s6 annual matrix NetCDF 导出 |
| `s6_export_climatology_to_nc.py` | climatology 独立 NetCDF 导出 |
| `s6_export_satellite_validation_to_nc.py` | satellite-only NetCDF 和 catalog 导出 |
| `submit_s6_fast.py` | Python LSF submitter，并行提交并等待 S6 master/matrix/climatology/satellite/summary |
| `s7_export_cluster_shp.py` | cluster 点位 GPKG 和 catalog 导出 |
| `s7_export_source_station_shp.py` | source station GPKG 和 catalog 导出 |
| `s7_export_cluster_basin_shp.py` | cluster basin polygon GPKG 导出 |
| `s8_publish_reference_dataset.py` | 标准发布包生成、发布 README、validation 和 inventory |

---

## 18. 非主线脚本说明

当前目录下仍保留一些历史脚本、兼容脚本或辅助脚本，例如：

```text
s4_cluster_qc_stations.py
s6_merge_timeseries_by_cluster.py
s7_merge_overlap_by_cluster.py
s8_merge_qc_csv_to_one_nc.py
s6_summarize_matrix_ncs.py
```

这些脚本不是当前 `s1 -> s8` 主线构建流程的一部分。人工质检与审计脚本也不属于主线发布 contract，如需使用，请参考对应脚本和相关验证文档。

---

## 19. 一句话总结

当前 `master` 分支主线按 `s1 -> s8` 构建 basin-based sediment reference dataset：`daily / monthly / annual` 进入 basin 主线，`climatology` 单独导出为独立发布产品，`satellite` 作为强制的发布级独立 NetCDF 数据集输出；主线与 climatology 发布记录必须至少包含 `SSC` 或 `SSL`，不发布 Q-only 时间步；`s4` 和 `s6` 生产环境优先通过 `submit_s4_lsf.sh` 与 `submit_s6_fast.sh` 运行；发布层以 `cluster_uid + resolution` 为标准连接键，保留 master NetCDF、matrix NetCDF、climatology、satellite、catalog、空间 sidecar 和 overlap provenance，并仅为 `resolved` 结果发布 basin polygon sidecar。
