# scripts_basin_test 最新说明

## 1. 这套流程现在在做什么

`scripts_basin_test` 是当前用于构建泥沙参考数据集的主线流程。

它的目标是生成一套以 `cluster` 为核心的参考数据集，包括：

1. 一个压缩的总 `NetCDF` 文件
2. 一个 `cluster` 点位 `shp`
3. 一个原始站点点位 `shp`
4. 一个最终 `cluster` 级流域单元 `shp`
5. 一套用于人工检查的 `csv` 表格

这里的 `cluster` 不是严格意义上的“同一个物理站点”，而是：

**同一个 90m 流域单元下的站点合并组。**

所以这套数据更准确地说是：

**一个以 90m 流域单元为合并基础、同时保留原始站点映射关系的泥沙参考数据集。**

---

## 2. 核心规则

### 2.1 cluster 规则

1. 同一个 90m 流域单元内的原始站点，归为同一个 `cluster`
2. 一个 `cluster` 下可以包含多个原始站点
3. 合并后不能丢掉原始站点信息
4. 最终记录仍需能追溯到原始站点和原始文件

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
| `single_point` | `daily` |
| `monthly` | `monthly` |
| `quarterly` | `monthly` |
| `annual` | `annual` 或 `climatology` |
| 其他不明确情况 | `other` |

补充说明：

1. `single_point` 如果从元数据判断其实是多年平均或气候态，则归为 `climatology`
2. `annual` 如果从元数据判断其实是多年平均或气候态，则归为 `climatology`
3. 同一个 `cluster` 下如果同时存在 `daily / monthly / annual / climatology`，这些都要保留
4. 它们不能混成一种普通时间序列，必须带清楚的时间类型标记

### 2.3 原始信息保留规则

最终数据集中必须能回答下面这些问题：

1. 一个 `cluster` 下面有哪些原始站点
2. 每个原始站点来自哪个数据源
3. 每条最终记录来自哪个原始站点
4. 每条最终记录来自哪个原始文件

### 2.4 多来源重叠规则

如果同一个 `cluster`、同一时间类型、同一时间点存在多个来源：

1. 允许多个来源同时进入候选
2. 最终记录层只保留一条胜出记录
3. 胜出规则按质量分数排序
4. `is_overlap = 1` 表示该条记录来自多来源竞争
5. 即使最终只选一条，原始站点映射关系仍然保留

---

## 3. 最终数据结构

### 3.1 cluster 层

每一行代表一个合并后的 `cluster`。

常见关键字段：

1. `cluster_uid`
2. `cluster_id`
3. `lat`
4. `lon`
5. `basin_area`
6. `pfaf_code`
7. `basin_match_quality`
8. `n_source_stations_in_cluster`

### 3.2 原始站点层

每一行代表一个原始站点。

常见关键字段：

1. `source_station_uid`
2. `source_station_cluster_index`
3. `source_station_native_id`
4. `source_station_name`
5. `source_station_river_name`
6. `source_station_lat`
7. `source_station_lon`
8. `source_station_paths`
9. `source_station_resolutions`

### 3.3 观测记录层

每一行代表一条最终保留的观测记录。

常见关键字段：

1. `station_index`
2. `source_station_index`
3. `time`
4. `resolution`
5. `Q`
6. `SSC`
7. `SSL`
8. `Q_flag`
9. `SSC_flag`
10. `SSL_flag`
11. `source`
12. `is_overlap`

---

## 4. 主流程脚本

当前主线流程按下面顺序运行。

### s1_verify_time_resolution.py

作用：

1. 检查各输入文件的时间分辨率
2. 输出时间分辨率验证结果

输出：

1. `scripts_basin_test/output/s1_verify_time_resolution_results.csv`

### s2_reorganize_qc_by_resolution.py

作用：

1. 按当前时间规则整理 `qc` 文件
2. 输出到 `output_resolution_organized/`

当前主目录只保留：

1. `daily`
2. `monthly`
3. `annual`
4. `climatology`
5. `other`

注意：

1. `single_point -> daily`
2. `quarterly -> monthly`

### s3_collect_qc_stations.py

作用：

1. 扫描整理后的 `nc`
2. 提取坐标、数据源、站名、河名、原始站点编号
3. 生成站点表

输出：

1. `scripts_basin_test/output/s3_collected_stations.csv`

说明：

1. 当前脚本会先对扫描结果排序，以提高重跑时的稳定性

### s4_basin_trace_watch.py

作用：

1. 为每个站点追溯上游流域
2. 输出站点级流域匹配结果

输出：

1. `scripts_basin_test/output/s4_upstream_basins.csv`
2. `scripts_basin_test/output/s4_upstream_basins.gpkg`

说明：

1. 这里的 `gpkg` 是站点级流域面结果
2. 它不是最终 `cluster` 级流域单元文件

### s5_basin_merge.py

作用：

1. 按流域单元将站点归入 `cluster`
2. 输出 `cluster` 级站点表

输出：

1. `scripts_basin_test/output/s5_basin_clustered_stations.csv`
2. `scripts_basin_test/output/s5_basin_cluster_report.csv`

### s6_basin_merge_to_nc.py

作用：

1. 合并时间序列
2. 生成最终压缩 `nc`
3. 同时保留 `cluster` 层、原始站点层和观测记录层

输出：

1. `scripts_basin_test/output/s6_basin_merged_all.nc`

### s7_export_cluster_shp.py

作用：

1. 导出最终 `cluster` 点位 `shp`

输出：

1. `scripts_basin_test/output/s7_cluster_stations.shp`

### s7_export_source_station_shp.py

作用：

1. 导出原始站点点位 `shp`

输出：

1. `scripts_basin_test/output/s7_source_stations.shp`

### s7_export_cluster_basin_shp.py

作用：

1. 从 `s4` 的站点级流域面和 `s5` 的 cluster 表导出最终 `cluster` 级流域单元 `shp`

输出：

1. `scripts_basin_test/output/s7_cluster_basins.shp`

说明：

1. 这个文件才是最终 `cluster` 级流域单元面文件
2. 它依赖 `s4_upstream_basins.gpkg`

### s9_generate_manual_review_tables.py

作用：

1. 自动生成适合人工检查的 `csv` 表格
2. 自动挑出优先检查的 `cluster`

输出目录：

1. `scripts_basin_test/output/manual_review/`

主要输出：

1. `00_dataset_summary.csv`
2. `01_linkage_summary.csv`
3. `02_resolution_summary.csv`
4. `03_priority_cluster_queue.csv`
5. `04_random_cluster_queue.csv`
6. `05_multi_resolution_clusters.csv`
7. `06_overlap_cluster_queue.csv`
8. `07_missing_basin_queue.csv`

---

## 5. 空间文件说明

### 5.1 cluster 点位文件

文件：

1. `s7_cluster_stations.shp`

作用：

1. 在 GIS 中查看合并后的 `cluster`
2. 作为 `nc` 和空间数据之间的连接层

### 5.2 原始站点点位文件

文件：

1. `s7_source_stations.shp`

作用：

1. 查看 `cluster` 内部包含哪些原始站点
2. 检查合并后是否仍保留原始站点信息

### 5.3 cluster 级流域单元文件

文件：

1. `s7_cluster_basins.shp`

作用：

1. 查看每个 `cluster` 对应的最终流域单元面
2. 与 `nc` 和 `cluster` 点位文件做空间联动

### 5.4 shapefile 字段名限制

由于 shapefile 字段名最多 10 个字符，部分字段会被截断。

常见例子：

1. `cluster_uid -> cluster_ui`
2. `source_station_uid -> src_uid`

这只是格式限制，不代表数据丢失。

---

## 6. 当前主要输出文件

默认都输出到：

`scripts_basin_test/output/`

最关键的主产物是：

1. `s5_basin_clustered_stations.csv`
2. `s6_basin_merged_all.nc`
3. `s7_cluster_stations.shp`
4. `s7_source_stations.shp`
5. `s7_cluster_basins.shp`

人工检查相关产物是：

1. `manual_review_checklist.csv`
2. `output/manual_review/*.csv`

---

## 7. 人工检查工作流

### 7.1 总检查表

文件：

1. `manual_review_checklist.csv`

用途：

1. 这是总的人工检查清单
2. 适合在 VSCode 里直接编辑
3. 可直接填写 `selected / done / status / notes`

### 7.2 自动抽查表

目录：

1. `output/manual_review/`

其中最推荐先看的几张：

1. `03_priority_cluster_queue.csv`
2. `06_overlap_cluster_queue.csv`
3. `05_multi_resolution_clusters.csv`

这些表适合优先筛查：

1. 多站点 cluster
2. 多来源重叠 cluster
3. 多时间类型 cluster
4. basin 信息缺失 cluster

---

## 8. 推荐运行顺序

如果是完整重跑，建议顺序为：

1. `s1_verify_time_resolution.py`
2. `s2_reorganize_qc_by_resolution.py`
3. `s3_collect_qc_stations.py`
4. `s4_basin_trace_watch.py`
5. `s5_basin_merge.py`
6. `s6_basin_merge_to_nc.py`
7. `s7_export_cluster_shp.py`
8. `s7_export_source_station_shp.py`
9. `s7_export_cluster_basin_shp.py`
10. `s9_generate_manual_review_tables.py`

---

## 9. 什么时候必须重跑整条流程

如果下面这些规则发生变化，建议从 `s2` 开始一直重跑到 `s9`：

1. 时间分辨率规则变化
2. `single_point` 或 `quarterly` 的归类规则变化
3. `annual / climatology` 的判定规则变化
4. `cluster` 合并规则变化
5. basin 匹配规则变化

原因是：

1. `s2` 会改变整理后的文件目录
2. `s3` 会重建站点列表
3. `s3` 的站点顺序会影响后续 `station_id`
4. `station_id` 又会影响 `s4 / s5 / s6 / s7`

---

## 10. 依赖说明

不同脚本的依赖不完全一样。

常见依赖包括：

1. `pandas`
2. `numpy`
3. `xarray`
4. `netCDF4`
5. `pyshp`
6. `geopandas`
7. `h5netcdf`

其中：

1. `s7_export_cluster_shp.py` 和 `s7_export_source_station_shp.py` 需要 `pyshp`
2. `s7_export_cluster_basin_shp.py` 需要 `geopandas`
3. `s9_generate_manual_review_tables.py` 使用 `xarray` 读取 `nc`，建议环境中可用 `h5netcdf`

---

## 11. 非主线脚本说明

当前目录下还有一些历史脚本或兼容脚本，例如：

1. `s4_cluster_qc_stations.py`
2. `s6_merge_timeseries_by_cluster.py`
3. `s7_merge_overlap_by_cluster.py`
4. `s8_merge_qc_csv_to_one_nc.py`

这些脚本不是当前主线构建流程的一部分。

当前主线应以：

1. `s1`
2. `s2`
3. `s3`
4. `s4_basin_trace_watch.py`
5. `s5_basin_merge.py`
6. `s6_basin_merge_to_nc.py`
7. `s7_*`
8. `s9_generate_manual_review_tables.py`

为准。

---

## 12. 当前有效规则一句话总结

**按 90m 流域单元合并站点为 `cluster`，保留所有原始站点映射关系，最终时间分辨率只保留 `daily / monthly / annual / climatology` 四类，其中 `single_point` 归为 `daily`，`quarterly` 归为 `monthly`，并额外导出 `cluster` 点位、原始站点点位、`cluster` 级流域单元面文件，以及人工检查表。**
