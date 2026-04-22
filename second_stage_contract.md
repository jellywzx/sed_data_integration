# 第二阶段数据 Contract

本文件定义 `scripts_basin_test` 第二阶段真正依赖和承诺保留的数据接口。

## 1. 重要前提

1. 第二阶段**不信任**第一阶段的目录名 `daily / monthly / annually_climatology`。
2. 最终分类以 `s1_verify_time_resolution.py` 产生的 `temporal_semantics` 为准。
3. 当前归类规则：
   - `hourly -> daily`
   - `single_point -> daily`
   - `quarterly -> monthly`
   - `annual -> annual` 或 `climatology`
   - `climatology` 单独导出，不进入 basin 主线

## 2. 必须保留的字段

第二阶段主线和发布层稳定依赖这些字段：

1. `time`
2. `lat/lon` 或其兼容别名 `latitude/longitude`
3. `Q`
4. `SSC`
5. `SSL`
6. 最终质量标志：
   - `Q_flag`
   - `SSC_flag`
   - `SSL_flag`
7. 站点级元数据：
   - `station_name`
   - `river_name`
   - `station_id` 或兼容别名 `Source_ID`
8. 数据集级元数据：
   - `data_source_name`
   - `creator_institution`
   - `references / reference*`
   - `source_data_link / source_url`

## 3. 中间使用但最终不保留的字段

这些字段在第二阶段有作用，但不会被最终 `master / matrix / climatology` 产品完整保留：

1. `upstream_area`
   - 在 `s3` 中抽取为 `reported_area`
   - 供 `s4` basin tracing 和面积一致性评分使用
2. `temporal_resolution / Temporal_Resolution`
   - 在 `s1/s2` 用于时间语义判断和回写整理后的副本

## 4. 设计上主动丢弃的字段

这些字段如果在第一阶段存在，当前第二阶段仍视为“非发布 contract”的补充信息：

1. `altitude`
2. 逐步 QC 标志：
   - `*_qc1_*`
   - `*_qc2_*`
   - `*_qc3_*`
3. `time_coverage_start`
4. `time_coverage_end`
5. 说明性全局属性：
   - `summary`
   - `comment`
   - `data_limitations`
   - `variables_provided`
   - `station_location`
   - `country`
   - `continent_region`

补充说明：

1. `temporal_span` 现在允许作为 `climatology nc` 的站点级说明字段写出。
2. 它的用途是帮助解释单个 climatology 站点对应的时间覆盖范围。
3. 它不是发布 contract 必填项。
4. `s8_publish_reference_dataset.py` 不会因为缺少 `temporal_span` 而失败。

## 5. 发布层额外要求

1. `master.nc` 必须保留记录级 provenance：
   - `cluster_uid`
   - `source_station_uid`
   - `source_station_index`
2. `matrix nc` 必须至少保留 cell 级 source provenance：
   - `selected_source_index`
   - `selected_source_station_uid`
3. `climatology nc` 必须保留文件级 provenance：
   - `station_uid`
   - `source_station_path`

## 6. Mixed-run 防线

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
