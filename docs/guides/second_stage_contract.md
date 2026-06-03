# 第二阶段数据 Contract

本文件定义 `scripts_basin_test` 第二阶段真正依赖、显式保留、以及会阻断主流程的数据接口。

## 1. 命名与读取 Contract

第二阶段统一通过共享模块 `qc_contract.py` 读取第一阶段 QC NetCDF。

当前稳定兼容别名：

1. 时间变量：
   - `time / Time / t / datetime / date / sample`
2. 坐标变量：
   - `lat / latitude / Latitude / LAT`
   - `lon / longitude / Longitude / LON`
3. 站点 ID 属性：
   - `station_id / Source_ID / Station_ID / source_id / stationID / ID / location_id`
4. 时间分辨率属性：
   - `temporal_resolution / Temporal_Resolution / time_resolution / resolution`
5. 数据集级来源属性：
   - `data_source_name / Data_Source_Name / dataset_name`
   - `creator_institution / contributor_institution / institution / insitiution`
   - `source_data_link / source_url / sediment_data_source / discharge_data_source`
   - `reference* / references`

第一阶段 canonical 写出键仍然是：

1. `station_id`
2. `temporal_resolution`

## 2. 时间分类 Contract

第二阶段不再信任第一阶段目录名 `daily / monthly / annually_climatology`。

`s1_verify_time_resolution.py` 现在会同时产出三类证据：

1. `time_axis_semantics`
2. `metadata_semantics`
3. `path_semantics`

并给出：

1. `final_semantics`
2. `classification_basis`
3. `review_required`
4. `review_reason`

自动兼容规则只保留：

1. `hourly -> daily`
2. `quarterly -> monthly`
3. `single_point + 明确长期平均证据 -> climatology`

硬门禁规则：

1. 只要 `time_axis_semantics` 与 `metadata_semantics` 冲突，就进入人工审核。
2. 未解决的审核项会写入 `s1_resolution_review_queue.csv`。
3. 人工结论必须写入 `s1_resolution_review_overrides.csv`：
   - `rel_path,resolved_semantics,review_note`
4. 只要 review queue 还有 unresolved 行：
   - `s1` 返回非零；
   - `run_s1_to_s7_basin_pipeline.py` 直接停止；
   - `s2_reorganize_qc_by_resolution.py` 不允许继续整理文件。

## 3. 必须保留的字段

第二阶段主线和发布层稳定依赖这些字段：

1. `time`
2. `lat/lon` 或其兼容别名
3. `Q`
4. `SSC`
5. `SSL`
6. 最终质量标志：
   - `Q_flag`
   - `SSC_flag`
   - `SSL_flag`
7. 标准化逐步 QC 标志：
   - `Q_qc1`
   - `SSC_qc1`
   - `SSL_qc1`
   - `Q_qc2`
   - `SSC_qc2`
   - `SSL_qc2`
   - `SSC_qc3`
   - `SSL_qc3`
8. 站点级元数据：
   - `station_name`
   - `river_name`
   - `station_id` 或兼容别名
9. 数据集级元数据：
   - `data_source_name`
   - `creator_institution`
   - `references / reference*`
   - `source_data_link / source_url`

## 4. 说明性元数据保留策略

以下字段现在被视为“精选保留元数据”：

1. `temporal_span`
2. `time_coverage_start`
3. `time_coverage_end`
4. `summary`
5. `comment`
6. `variables_provided`
7. `data_limitations`
8. `declared temporal_resolution`

保留位置：

1. `master.nc`
   - 写入 `n_source_stations` lookup：
   - `source_station_temporal_span`
   - `source_station_time_coverage_start`
   - `source_station_time_coverage_end`
   - `source_station_summary`
   - `source_station_comment`
   - `source_station_variables_provided`
   - `source_station_data_limitations`
   - `source_station_declared_temporal_resolution`
2. `s6_climatology_only.nc`
   - 按 `n_stations` 直接保留同一组字段
3. `s7_source_station_resolution_catalog.csv`
   - 重复输出同一组字段，供 matrix 用户通过 `selected_source_station_uid` 反查

`matrix nc` 仍保持轻量，不直接复制这些长文本到每个 cell。

## 5. 中间使用但最终不完整保留的字段

1. `upstream_area`
   - 在 `s3` 中抽取为 `reported_area`
   - 供 `s4` basin tracing 和面积一致性评分使用
2. legacy `path_resolution`
   - 仅作为历史目录证据保留在 `s1` 结果里

## 6. 设计上仍主动不扩展的字段

1. `altitude`
2. 全量 source attrs 原样透传

这些字段本轮不进入发布 contract。

## 7. 发布层额外要求

1. `master.nc` 必须保留记录级 provenance：
   - `cluster_uid`
   - `source_station_uid`
   - `source_station_index`
2. `matrix nc` 必须至少保留 cell 级 provenance：
   - `selected_source_index`
   - `selected_source_station_uid`
3. `climatology nc` 必须保留文件级 provenance：
   - `station_uid`
   - `source_station_path`
4. 所有 s6 产物应声明：
   - `classification_policy=manual_review_on_conflict`
   - `qc_stage_schema_version=1`

## 8. Mixed-run 防线

发布前必须保证下面几类产物来自同一轮主线运行：

1. `s6_basin_merged_all.nc`
2. `s6_matrix_by_resolution/*.nc`
3. `s6_climatology_only.nc`
4. `s7_cluster_station_catalog.csv`
5. `s7_cluster_resolution_catalog.csv`
6. `s7_source_station_resolution_catalog.csv`

如果这些产物的：

1. `resolution` 覆盖面
2. 各分辨率 `record_count`
3. 各分辨率 `time_start / time_end`

不一致，应直接停止发布，并要求重新执行：

`s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7 -> s8`
