# scripts_basin_test 最新说明

## 1. 这套流程现在在做什么

`scripts_basin_test` 是当前 `experiment/restore_satellite_basin` 分支上的 `basin mainline`，主线范围是 `s1 -> s8`。

这套流程的目标是构建一套以 `cluster` 为核心的泥沙参考数据集，并发布到 `scripts_basin_test/output/sed_reference_release/`。当前主合同包括：

1. `master nc`：`sed_reference_master.nc`
2. `timeseries matrix nc`：`daily / monthly / annual`
3. `climatology nc`：`sed_reference_climatology.nc`
4. `catalog`：`station_catalog.csv`、`source_station_catalog.csv`、`source_dataset_catalog.csv`
5. `GPKG sidecar`：`cluster points`、`source stations`，以及可选的 `cluster basins`
6. 发布级 provenance sidecar：`sed_reference_overlap_candidates.csv.gz`

这里的 `cluster` 不是严格意义上的“同一个物理站点”，而是：

**同一个 basin 合并规则下的站点合并组。**

当前推荐把内部产物理解为同一套参考数据的不同层级：

1. `master` 层：以 `s6_basin_merged_all.nc` 为基础，保留完整记录级 provenance
2. `matrix` 层：以 `daily / monthly / annual` 三个矩阵 `nc` 为基础，适合最近站点匹配和模型对比
3. `climatology` 层：以 `s6_climatology_only.nc` 为基础，独立发布，不进入 basin 主线 merge
4. `release` 层：由 `s8_publish_reference_dataset.py` 整理为标准命名的对外数据包

---

## 2. 核心规则

### 2.1 cluster 规则

1. 一个 `cluster` 可以包含多个原始站点
2. 主线最终必须同时保留 `cluster` 层、原始站点层和记录层
3. 最终记录必须能追溯到 `source_station_uid` 和原始文件路径
4. 发布层的标准空间和 catalog 连接键是 `cluster_uid + resolution`

### 2.2 时间分辨率规则

最终主分类只保留：

1. `daily`
2. `monthly`
3. `annual`
4. `climatology`

当前归类规则是：

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

1. `single_point` 如果从元数据判断是多年平均或气候态，则归为 `climatology`
2. `annual` 如果从元数据判断是多年平均或气候态，则归为 `climatology`
3. `climatology` 在 `s2` 之后会被单独保留，但默认不进入 basin 主线
4. basin 主线默认只处理非 `climatology` 的站点
5. 第二阶段不直接信任第一阶段目录名，实际以 `s1` 输出的 `temporal_semantics` 为准

### 2.3 多来源重叠规则

如果同一个 `cluster`、同一 `resolution`、同一时间点存在多个来源：

1. 多个来源都可以进入候选
2. 最终记录层只保留一条胜出记录
3. 胜出规则按质量分数排序
4. `is_overlap = 1` 表示该条记录来自多来源竞争
5. `master nc` 和 `matrix nc` 只保存胜出记录
6. 真正的 source-pair overlap 一致性分析应使用 `sed_reference_overlap_candidates.csv.gz`

### 2.4 basin 发布策略

当前 basin 发布策略采用保守规则：

1. 发布层只区分 `resolved` 和 `unresolved`
2. `unresolved` 记录可以保留在主数据中
3. 是否进入 basin polygon sidecar 属于 `s7 / s8` 发布层规则，不属于 `s5` 合并规则本身

当前主线使用的关键 basin 诊断字段包括：

1. `distance_m`
2. `match_quality`
3. `point_in_local`
4. `point_in_basin`
5. `basin_status`
6. `basin_flag`

### 2.5 点是否在流域多边形内的判定过程

这里要区分两层逻辑：

1. `basin_tracer.py` 负责几何计算，生成 `point_in_local` 和 `point_in_basin`
2. `basin_policy.py` 负责读取这些诊断结果，并给出最终 `resolved / unresolved`

当前实现要点：

1. 几何判断直接基于原始站点坐标
2. 当前不做 snapping，不修改原始经纬度
3. 使用 `covers()` 而不是 `contains()`，让边界点也视为面内
4. `point_in_local` 更偏向匹配河段附近的局地证据
5. `point_in_basin` 更偏向完整上游流域的辅助诊断
6. `s4 / s5 / s6 / s7` 只是传递和写出这些结果，不重复计算几何关系

---

## 3. 最终数据结构

### 3.1 cluster 层

常见关键字段：

1. `cluster_uid`
2. `cluster_id`
3. `lat`
4. `lon`
5. `basin_area`
6. `pfaf_code`
7. `basin_status`
8. `basin_flag`
9. `basin_distance_m`
10. `point_in_local`
11. `point_in_basin`
12. `n_source_stations_in_cluster`

### 3.2 原始站点层

常见关键字段：

1. `source_station_uid`
2. `source_station_native_id`
3. `source_station_name`
4. `source_station_river_name`
5. `source_station_lat`
6. `source_station_lon`
7. `source_station_paths`
8. `source_station_resolutions`

### 3.3 观测记录层

常见关键字段：

1. `station_index`
2. `source_station_index`
3. `time`
4. `resolution`
5. `Q`
6. `SSC`
7. `SSL`
8. `source`
9. `is_overlap`

---

## 4. 主流程脚本

当前主线流程按 `s1 -> s8` 运行。`s4` 和 `s6` 生产环境优先使用 submit 脚本，底层 Python 脚本主要用于调试或单步运行。

### s1_verify_time_resolution.py

作用：

1. 校验输入文件的时间分辨率
2. 生成主分类结果、人工 review queue 和 manual override 模板

关键输入：

1. `qc` 输入目录下的原始 `nc`

关键输出：

1. `scripts_basin_test/output/s1_verify_time_resolution_results.csv`
2. `scripts_basin_test/output/s1_resolution_review_queue.csv`
3. `scripts_basin_test/output/s1_resolution_review_overrides.csv`

高影响注意事项：

1. `s2` 实际以 `s1` 输出的 `temporal_semantics` 为准
2. 如果 override 发生变化，通常应从 `s2` 起重跑

### s2_reorganize_qc_by_resolution.py

作用：

1. 按 `s1` 的最终判定重组 `qc` 文件
2. 输出到 `output_resolution_organized/`

关键输入：

1. `scripts_basin_test/output/s1_verify_time_resolution_results.csv`

关键输出：

1. `../output_resolution_organized/`
2. `scripts_basin_test/output/s2_resolution_classification_details.csv`
3. `scripts_basin_test/output/s2_other_resolution_summary.csv`
4. `scripts_basin_test/output/s2_other_resolution_details.csv`

高影响注意事项：

1. 主目录只保留 `daily / monthly / annual / climatology / other`
2. `single_point -> daily`，`quarterly -> monthly`
3. 传了 `--dataset` 时默认不会清空整个 `output_resolution_organized/`

### s3_collect_qc_stations.py

作用：

1. 扫描整理后的 `nc`
2. 提取 basin 主线使用的站点元数据
3. 生成后续 `station_id` 基础表

关键输入：

1. `../output_resolution_organized/`

关键输出：

1. `scripts_basin_test/output/s3_collected_stations.csv`

高影响注意事项：

1. 当前默认排除 `climatology`
2. 扫描结果会先排序，以提高重跑稳定性
3. RiverSed 在 basin 主线下只保留 `lon/lat + 基本站点标识`，不再沿用源产品自带的 NHDPlus / upstream area 元数据

### s4_basin_trace_watch.py

推荐运行入口：

```bash
bash submit_s4_lsf.sh
bash submit_s4_lsf.sh 16
```

作用：

1. 为每个站点做 upstream basin tracing
2. 输出站点级 basin 匹配结果和诊断字段

关键输入：

1. `scripts_basin_test/output/s3_collected_stations.csv`
2. `MERIT_Hydro_v07_Basins_v01_bugfix1`

关键输出：

1. `scripts_basin_test/output/s4_upstream_basins.csv`
2. `scripts_basin_test/output/s4_upstream_basins.gpkg`
3. `scripts_basin_test/output/s4_local_catchments.gpkg`
4. `scripts_basin_test/output/s4_reported_area_check.csv`

高影响注意事项：

1. 当前推荐通过 `submit_s4_lsf.sh` 运行，而不是手工直接调用 `s4_basin_trace_watch.py`
2. `submit_s4_lsf.sh` 会提交两阶段 LSF 流程：先跑 `s4_trace[1-N]` 数组分片任务，再自动提交 finalize 合并任务
3. 常用环境变量包括 `S4_QUEUE`、`S4_NCORES`、`S4_MEM`、`S4_PTILE`、`PYTHON_BIN`
4. 日志目录在 `scripts_basin_test/output/logs/s4_lsf/`
5. 分片中间结果目录在 `scripts_basin_test/output/s4_shards/`
6. `s4_upstream_basins.csv` 会保留 `distance_m / match_quality / point_in_local / point_in_basin / basin_status / basin_flag`
7. RiverSed 在这一步只按坐标匹配 MERIT，不再使用其源产品自带的 `upstream_area` 或 NHDPlus basin 信息

### s5_basin_merge.py

作用：

1. 基于 `s4` basin 结果为站点分配 `cluster_id`
2. 输出 cluster 级站点表和 cluster 报告

关键输入：

1. `scripts_basin_test/output/s3_collected_stations.csv`
2. `scripts_basin_test/output/s4_upstream_basins.csv`

关键输出：

1. `scripts_basin_test/output/s5_basin_clustered_stations.csv`
2. `scripts_basin_test/output/s5_basin_cluster_report.csv`

高影响注意事项：

1. 只有 `basin_status=resolved` 且 `basin_id` 有效的站点才参与 basin cluster 合并
2. 同一 `basin_id` 内，只有所有跨组站点对都满足距离阈值和 `uparea_merit` 相对误差阈值时，两个候选 cluster 才能合并
3. 合并方式是 `complete-linkage`
4. 不满足条件的站点保留为 singleton，`cluster_id=station_id`
5. `s5` 会把 basin 元数据并回站点表，并对 `unresolved` 行屏蔽部分 release-facing basin 字段

### s6 主线输出

推荐运行入口：

```bash
bash submit_s6_fast.sh
```

当前 `s6` 是一组并行任务，而不是单一脚本。`submit_s6_fast.sh` 当前会并行提交：

1. `merge`：`s6_basin_merge_to_nc.py`
2. `daily`：`s6_export_daily_matrix_nc.py`
3. `monthly`：`s6_export_monthly_matrix_nc.py`
4. `annual`：`s6_export_annual_matrix_nc.py`
5. `clim`：`s6_export_climatology_to_nc.py`
6. `satellite`：`s6_export_satellite_validation_to_nc.py`

在未设置 `RUN_ONLY` 时，脚本还会额外提交一个依赖型 `check` 任务，用于检查关键输出是否齐全。

常用环境变量：

1. `RUN_ONLY`
2. `DRY_RUN`
3. `LSF_QUEUE`
4. `LSF_PROJECT`
5. `LSF_EXTRA`
6. `MERGE_N`、`MERGE_WORKERS`、`MERGE_METADATA_WORKERS`
7. `DAILY_N`、`DAILY_WORKERS`
8. `MONTHLY_N`、`MONTHLY_WORKERS`
9. `ANNUAL_N`、`ANNUAL_WORKERS`
10. `CLIM_N`
11. `SATVAL_N`

前置输入：

1. `scripts_basin_test/output/s5_basin_clustered_stations.csv`
2. `../output_resolution_organized/climatology`

#### s6_basin_merge_to_nc.py

作用：

1. 合并主线时间序列
2. 生成 `master nc`
3. 写出 cluster 级候选质量排序表

关键输出：

1. `scripts_basin_test/output/s6_basin_merged_all.nc`
2. `scripts_basin_test/output/s6_cluster_quality_order.csv`

高影响注意事项：

1. 默认不把 `climatology` 并入主 merge
2. `s7` 中使用的 `basin_status / basin_flag / basin_distance_m / point_in_local / point_in_basin` 来自这个 `master nc`

#### s6_export_daily_matrix_nc.py / s6_export_monthly_matrix_nc.py / s6_export_annual_matrix_nc.py

作用：

1. 分别导出 `daily / monthly / annual` 的 `station x time` 矩阵 `nc`
2. 在各自分辨率内部沿用主 merge 的质量排序逻辑

关键输出：

1. `scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_daily.nc`
2. `scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_monthly.nc`
3. `scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_annual.nc`

高影响注意事项：

1. 这些矩阵文件更适合最近站点匹配和模型直接对比
2. 只拿 matrix 也可以通过 `selected_source_station_uid` 回溯到 source station
3. `s6_export_resolution_matrix_ncs.py` 仍可作为兼容/聚合入口，但不是 `submit_s6_fast.sh` 的默认主入口

#### s6_export_climatology_to_nc.py

作用：

1. 单独扫描 `output_resolution_organized/climatology`
2. 不经过 basin tracing
3. 不经过 cluster merge

关键输出：

1. `scripts_basin_test/output/s6_climatology_only.nc`
2. `scripts_basin_test/output/s6_climatology_stations.shp`

高影响注意事项：

1. `climatology` 独立发布，不进入 basin 主线 merge
2. 当前会保留 `station_uid`、`source_station_path` 和 `temporal_span`

#### s6_export_satellite_validation_to_nc.py

作用：

1. 导出不进入主线 station-reference merge 的 satellite validation-only 数据

关键输出：

1. `scripts_basin_test/output/s6_satellite_validation_only.nc`
2. `scripts_basin_test/output/s6_satellite_validation_catalog.csv`

### s7 空间与 catalog 输出

#### s7_export_cluster_shp.py

作用：

1. 读取 `master nc + daily/monthly/annual matrix nc`
2. 导出 cluster 点位 `gpkg`
3. 导出 cluster 级 station / resolution catalog

关键输出：

1. `scripts_basin_test/output/s7_cluster_points.gpkg`
2. `scripts_basin_test/output/s7_cluster_station_catalog.csv`
3. `scripts_basin_test/output/s7_cluster_resolution_catalog.csv`

高影响注意事项：

1. `s7_cluster_points.gpkg` 包含 `cluster_summary / cluster_daily / cluster_monthly / cluster_annual`
2. `climatology` 不进入 `cluster_uid` 图层体系
3. `cluster_uid + resolution` 是发布层标准 join key

#### s7_export_source_station_shp.py

作用：

1. 读取 `master nc`
2. 按实际进入主线的 `source_station_uid + resolution` 聚合
3. 导出 source-station `gpkg` 和 catalog

关键输出：

1. `scripts_basin_test/output/s7_source_stations.gpkg`
2. `scripts_basin_test/output/s7_source_station_resolution_catalog.csv`

#### s7_export_cluster_basin_shp.py

作用：

1. 为每个 `cluster_id` 选代表 polygon
2. 按 `cluster_uid + resolution` 展开 basin 面
3. 导出 cluster basin `gpkg`

关键输出：

1. `scripts_basin_test/output/s7_cluster_basins.gpkg`
2. `scripts_basin_test/output/s7_cluster_basins_local.gpkg`

高影响注意事项：

1. `s7_cluster_basins.gpkg` 是发布前的主 polygon 产品
2. 只会为 `basin_status=resolved` 的记录导出 basin polygon
3. `unresolved` 记录即使保留在主数据表中，也不会进入 basin polygon sidecar

### s8_publish_reference_dataset.py

作用：

1. 把 `s6 / s7` 主线产物整理成标准发布包
2. 生成标准命名的 `sed_reference_*.nc`
3. 生成标准 catalog、GPKG sidecar、发布 README、验证报告和 inventory

关键输入：

1. `s6` 的 `master / daily / monthly / annual / climatology / satellite validation`
2. `s7_cluster_station_catalog.csv`
3. `s7_cluster_resolution_catalog.csv`
4. `s7_source_station_resolution_catalog.csv`
5. `s7_cluster_basins.gpkg`

关键输出：

1. `scripts_basin_test/output/sed_reference_release/`

高影响注意事项：

1. 发布层主合同包括 `master nc`、`daily/monthly/annual matrix`、`climatology nc`、`catalog`、`GPKG sidecar`
2. 脚本默认可生成 cluster/source GPKG，但默认不会发布 basin polygon GPKG；需要显式传 `--include-basin-polygons`
3. `run_s1_s8_basin_pipeline.py` 的内建默认会把 `--include-basin-polygons` 打开
4. 发布前会检查 `master / matrix / climatology / catalog` 的覆盖范围、记录数和时间范围是否一致，发现 mixed-run 会报错
5. `sed_reference_overlap_candidates.csv.gz` 是发布级 candidate-level overlap provenance sidecar

---

## 5. 空间文件说明

当前主线的空间产品统一使用 `GPKG`。发布层中的标准空间文件由 `s8_publish_reference_dataset.py` 生成。

### 5.1 cluster 点位文件

文件：

1. `s7_cluster_points.gpkg`
2. `sed_reference_release/sed_reference_cluster_points.gpkg`

作用：

1. 提供 `cluster_summary / cluster_daily / cluster_monthly / cluster_annual` 多图层
2. 作为 `nc`、catalog 与空间数据之间的多分辨率连接层

### 5.2 原始站点点位文件

文件：

1. `s7_source_stations.gpkg`
2. `sed_reference_release/sed_reference_source_stations.gpkg`

作用：

1. 查看某个 `cluster_uid + resolution` 内有哪些原始站点参与
2. 标准 join key 是 `source_station_uid + resolution`

### 5.3 cluster 级流域单元文件

文件：

1. `s7_cluster_basins.gpkg`
2. `sed_reference_release/sed_reference_cluster_basins.gpkg`

作用：

1. 查看每个 `cluster_uid + resolution` 对应的最终流域面
2. 与 `cluster` 点位文件和发布 `catalog` 按复合键做空间联动

---

## 6. 当前主要输出文件

默认都输出到 `scripts_basin_test/output/`。

如果是内部流程检查，最关键的主线产物是：

1. `s4_upstream_basins.csv`
2. `s5_basin_clustered_stations.csv`
3. `s6_basin_merged_all.nc`
4. `s6_matrix_by_resolution/s6_basin_matrix_daily.nc`
5. `s6_matrix_by_resolution/s6_basin_matrix_monthly.nc`
6. `s6_matrix_by_resolution/s6_basin_matrix_annual.nc`
7. `s7_cluster_points.gpkg`
8. `s7_cluster_station_catalog.csv`
9. `s7_cluster_resolution_catalog.csv`
10. `s7_source_stations.gpkg`
11. `s7_source_station_resolution_catalog.csv`
12. `s7_cluster_basins.gpkg`

如果是面向用户发布，当前推荐直接使用：

1. `output/sed_reference_release/sed_reference_master.nc`
2. `output/sed_reference_release/sed_reference_timeseries_daily.nc`
3. `output/sed_reference_release/sed_reference_timeseries_monthly.nc`
4. `output/sed_reference_release/sed_reference_timeseries_annual.nc`
5. `output/sed_reference_release/sed_reference_climatology.nc`
6. `output/sed_reference_release/station_catalog.csv`
7. `output/sed_reference_release/source_station_catalog.csv`
8. `output/sed_reference_release/source_dataset_catalog.csv`
9. `output/sed_reference_release/sed_reference_overlap_candidates.csv.gz`
10. `output/sed_reference_release/sed_reference_cluster_points.gpkg`
11. `output/sed_reference_release/sed_reference_source_stations.gpkg`
12. `output/sed_reference_release/sed_reference_cluster_basins.gpkg`
13. `output/sed_reference_release/README.md`
14. `output/sed_reference_release/release_validation_report.csv`

这套发布层的标准使用顺序是：

1. 先按模型输出时间分辨率选择对应的 matrix `nc`
2. 把 `station_catalog.csv` 过滤到对应 `resolution`
3. 用过滤后的 `lat/lon` 或 `sed_reference_cluster_points.gpkg` 找最近的 `cluster_uid`
4. 抽出参考时间序列并与模型结果对齐
5. 如需完整记录级 provenance，再查询 `sed_reference_master.nc`
6. 通过 `source_station_catalog.csv` 回查原始站点和原始路径
7. 如需真正的 source-pair overlap 指标，使用 `sed_reference_overlap_candidates.csv.gz`

---

## 7. 推荐运行顺序

如果是完整重跑，当前推荐顺序为：

1. `s1_verify_time_resolution.py`
2. `s2_reorganize_qc_by_resolution.py`
3. `s3_collect_qc_stations.py`
4. `submit_s4_lsf.sh`
5. `s5_basin_merge.py`
6. `submit_s6_fast.sh`
7. `s7_export_cluster_shp.py`
8. `s7_export_source_station_shp.py`
9. `s7_export_cluster_basin_shp.py`
10. `s8_publish_reference_dataset.py`

如果只是调试或单步运行，也可以按底层 Python 脚本顺序执行：

1. `s4_basin_trace_watch.py`
2. `s5_basin_merge.py`
3. `s6_basin_merge_to_nc.py`
4. `s6_export_daily_matrix_nc.py`
5. `s6_export_monthly_matrix_nc.py`
6. `s6_export_annual_matrix_nc.py`
7. `s6_export_climatology_to_nc.py`
8. `s6_export_satellite_validation_to_nc.py`
9. `s7_export_cluster_shp.py`
10. `s7_export_source_station_shp.py`
11. `s7_export_cluster_basin_shp.py`
12. `s8_publish_reference_dataset.py`

---

## 8. 什么时候必须重跑整条流程

下面这些变化通常需要较大范围重跑：

1. 时间分辨率规则变化：从 `s2` 起重跑
2. `single_point / quarterly / annual / climatology` 判定逻辑变化：从 `s2` 起重跑
3. basin tracing 或 `basin_status` 规则变化：从 `s4` 起重跑
4. cluster merge 规则变化：从 `s5` 起重跑
5. `s6` 输出字段或发布 contract 变化：至少重跑 `s6 -> s8`

原因是：

1. `s2` 会改变整理后的文件目录
2. `s3` 会重建站点列表
3. `s3` 的站点顺序会影响后续 `station_id`
4. `station_id` 又会影响 `s4 / s5 / s6 / s7`

---

## 9. 依赖说明

常见依赖包括：

1. `pandas`
2. `numpy`
3. `xarray`
4. `netCDF4`
5. `pyshp`
6. `geopandas`
7. `h5netcdf`

其中：

1. `s7_export_cluster_shp.py` 需要 `pyshp + geopandas`
2. `s7_export_source_station_shp.py` 和 `s7_export_cluster_basin_shp.py` 需要 `geopandas`
3. `s8_publish_reference_dataset.py` 在发布 `GPKG sidecar` 时需要 `geopandas`

---

## 10. 非主线脚本说明

当前目录下仍有一些历史脚本、兼容脚本或辅助脚本，例如：

1. `s4_cluster_qc_stations.py`
2. `s6_merge_timeseries_by_cluster.py`
3. `s7_merge_overlap_by_cluster.py`
4. `s8_merge_qc_csv_to_one_nc.py`
5. `s6_summarize_matrix_ncs.py`

这些脚本不是当前 `s1 -> s8` 主线构建流程的一部分。人工质检与审计脚本也不属于主线发布 contract，如需使用，请参考独立脚本和相关验证文档。

新增的发布层验证说明见：[`validation_results.md`](validation_results.md)。

---

## 11. 当前有效规则一句话总结

**当前主线按 `s1 -> s8` 构建 basin-based sediment reference dataset：`daily / monthly / annual` 进入 basin 主线，`climatology` 单独导出；`s4` 和 `s6` 优先通过 `submit_s4_lsf.sh` 与 `submit_s6_fast.sh` 运行；发布层以 `cluster_uid + resolution` 为标准连接键，保留 `master nc`、matrix、catalog、空间 sidecar 和 overlap provenance，并仅为 `resolved` 结果发布 basin polygon sidecar。**
