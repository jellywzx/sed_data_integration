# scripts_basin_test 最新说明

## 1. 这套流程现在在做什么

`scripts_basin_test` 是当前用于构建泥沙参考数据集的主线流程。

它的目标是生成一套以 `cluster` 为核心的参考数据集，包括：

1. 一个压缩的总 `NetCDF` 文件
2. 一个单独的 `climatology NetCDF` 文件
3. 一个 `cluster` 点位 `gpkg`
4. 一个原始站点点位 `gpkg`
5. 一个最终 `cluster` 级流域单元 `gpkg`
6. 一套用于人工检查的 `csv` 表格

这里的 `cluster` 不是严格意义上的“同一个物理站点”，而是：

**同一个 90m 流域单元下的站点合并组。**

所以这套数据更准确地说是：

**一个以 90m 流域单元为合并基础、同时保留原始站点映射关系的泥沙参考数据集。**

### 1.1 现在推荐的发布方式

当前不再建议在下面两种输出之间二选一：

1. `s6_basin_merged_all.nc`
2. `s6_matrix_by_resolution/*.nc`

更推荐把它们定义为同一套参考数据集的两个层级：

1. `master.nc` 层：
   以 `s6_basin_merged_all.nc` 为基础，保留完整记录级 provenance，适合追溯和审计
2. `timeseries matrix` 层：
   以 `daily / monthly / annual` 三个矩阵 `nc` 为基础，适合最近站点匹配、抽取时间序列、和模型直接对比
3. `climatology` 层：
   以 `s6_climatology_only.nc` 为基础，独立发布，不进入 basin 主线

当前目录里新增了：

1. `s8_publish_reference_dataset.py`
2. `example_reference_workflow.py`

它们的用途分别是：

1. 将现有主线产物整理成正式发布版 `sed_reference_release/`
2. 演示“最近站点匹配 -> 抽时间序列 -> 可选模型对比 -> 回查 provenance”的标准使用路径

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
3. `climatology` 在 `s2` 之后会被单独保留，但默认不进入 basin 主线
4. basin 主线默认只处理非 `climatology` 的站点
5. `climatology` 最后单独导出为一个独立 `nc`
6. 不同时间类型不能混成一种普通时间序列，必须带清楚的时间类型标记

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

### 2.5 basin 发布策略

当前 basin 发布策略采用保守规则：

1. 宁可少分配一些 basin，也不要把错误 basin 大规模写入正式发布数据
2. 发布层只区分两类结果：`resolved` 和 `unresolved`
3. `unresolved` 站点仍保留观测记录，但不强行发布 basin polygon

当前主线使用的站点级诊断字段包括：

1. `distance_m`
2. `match_quality`
3. `point_in_local`
4. `point_in_basin`
5. `basin_status`
6. `basin_flag`

自动判为 `resolved` 的条件保持简单：

1. `distance_m <= 300`
2. `distance_m <= 1000` 且 `match_quality in {area_matched, area_approximate}`
3. `distance_m <= 1000` 且 `point_in_local = True`
4. 对 `GSED / RiverSed` 这类 `reach-scale` 遥感产品，若 `distance_m > 1000` 但 `distance_m <= 5000` 且 `point_in_local = True`，则返回 `resolved / reach_product_offset_ok`

其余情况默认进入 `unresolved`，常见原因包括：

1. `distance_m > 1000`
2. `match_quality = area_mismatch`
3. `match_quality = failed`
4. 点面关系不一致

### 2.6 点是否在流域多边形内的判定过程

这里需要特别区分两件事：

1. `basin_tracer.py` 负责真正的几何判断，也就是算出 `point_in_local` 和 `point_in_basin`
2. `basin_policy.py` 不重新做点面计算，它只是读取这些诊断结果，再决定最终给 `resolved` 还是 `unresolved`

也就是说，`basin_policy.py` 本身不是用来“算点在不在 polygon 里”的，它是用来“解释这个点面关系该如何进入发布策略”的。

当前代码里的实际判定流程如下：

1. 先用 `find_best_reach()` 为站点找到最合适的 MERIT 河段 `COMID`
2. 用这个匹配到的单一 `COMID`，构建最小单元汇水区 `geometry_local`
3. 再沿河网向上游追溯全部 `COMID`，把所有上游汇水面合并成完整流域 `geometry`
4. 用原始站点坐标直接构造 `Point(lon, lat)`
5. 如果 `geometry_local` 存在且非空，则计算 `geometry_local.covers(point)`，结果写入 `point_in_local`
6. 如果完整上游流域 `geometry` 存在、非空，且当前方法是 `upstream_traced`，则计算 `geometry.covers(point)`，结果写入 `point_in_basin`

这两个判断都是直接基于原始点坐标完成的：

1. 当前这一步不做 snapping
2. 不修改站点原始经纬度
3. 不额外加 buffer 再判断 inside / outside

之所以使用 `covers()` 而不是更严格的 `contains()`，是因为：

1. `covers()` 会把恰好落在 polygon 边界上的点也视为“在面内”
2. 这对岸边站点、河道边界站点、以及由坐标精度造成的边界贴线情况更稳健
3. 这样可以减少“其实只是落在边界上，但被判成 outside”的伪问题

`point_in_local` 和 `point_in_basin` 的含义并不相同：

1. `point_in_local` 检查的是站点是否落在“匹配河段自身对应的最小局地汇水区”里
2. `point_in_basin` 检查的是站点是否落在“整条上游追溯后合并得到的完整流域”里

其中当前发布策略更看重 `point_in_local`，因为它更能反映“这个点是否真的属于当前匹配河段附近的局地汇水单元”；`point_in_basin` 更偏向辅助诊断，因为完整上游流域往往范围很大，单独用它来放宽匹配会过于宽松。

还有一个重要细节：

1. 如果 tracer 没能成功拼出完整上游 polygon，就会退回 `area_buffer_fallback`
2. 在这种 fallback 情况下，代码不会用这个圆形 buffer 去计算 `point_in_basin = True`
3. 也就是说，`point_in_basin` 只在真实的 `upstream_traced` polygon 存在时才会被赋值
4. 这样做是为了避免把“兜底几何”误当成真实流域边界证据

最后，`basin_policy.py` 对这些布尔值的使用方式是：

1. 先把 `point_in_local` 和 `point_in_basin` 统一转成布尔值
2. 当前自动接受规则里，`point_in_local = True` 可以作为 `distance_m <= 1000` 条件下的一个放行证据
3. 对 `GSED / RiverSed` 这类 `reach-scale` 遥感产品，`point_in_local = True` 还可以作为 `1000-5000 m` 偏移区间内的一个特例放行证据，结果标记为 `reach_product_offset_ok`
4. `basin_policy.py` 当前不再使用站名/河名关键词做额外分类
5. 如果点面关系明显不一致，且其他证据也不足，则最后会落到 `geometry_inconsistent`

因此，README 中提到的“点是否在流域 polygon 内”，在代码实现上应理解为：

1. 几何计算发生在 `basin_tracer.get_upstream_basin()`
2. 发布判定发生在 `basin_policy.classify_basin_result()`
3. `s4 / s5 / s6 / s7` 只是把这个结果逐步写出和传递，并不重复计算几何关系

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
8. `basin_status`
9. `basin_flag`
10. `basin_distance_m`
11. `point_in_local`
12. `point_in_basin`
13. `n_source_stations_in_cluster`

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
3. 生成 basin 主线使用的站点表

输出：

1. `scripts_basin_test/output/s3_collected_stations.csv`

说明：

1. 当前脚本会先对扫描结果排序，以提高重跑时的稳定性
2. 当前默认会排除 `climatology`，使其不进入 basin tracing 和 basin merge

### s4_basin_trace_watch.py

作用：

1. 为每个站点追溯上游流域
2. 输出站点级流域匹配结果

输出：

1. `scripts_basin_test/output/s4_upstream_basins.csv`
2. `scripts_basin_test/output/s4_upstream_basins.gpkg`
3. `scripts_basin_test/output/s4_reported_area_check.csv`

说明：

1. 这里的 `gpkg` 是站点级流域面结果
2. 它不是最终 `cluster` 级流域单元文件
3. `s4_upstream_basins.csv` 现在会保留 `distance_m / match_quality / point_in_local / point_in_basin / basin_status / basin_flag`
4. `s4_reported_area_check.csv` 用于单独检查 reported drainage area 与 tracer 结果的一致性

### s5_basin_merge.py

作用：

1. 按流域单元将站点归入 `cluster`
2. 输出 `cluster` 级站点表
3. 对 `unresolved` 站点保留观测，但不把 basin polygon 相关字段作为正式发布分配结果

输出：

1. `scripts_basin_test/output/s5_basin_clustered_stations.csv`
2. `scripts_basin_test/output/s5_basin_cluster_report.csv`

### s6_basin_merge_to_nc.py

作用：

1. 合并时间序列
2. 生成 basin 主线的最终压缩 `nc`
3. 同时保留 `cluster` 层、原始站点层和观测记录层
4. 将站点级 basin 诊断字段写入 `master nc`

输出：

1. `scripts_basin_test/output/s6_basin_merged_all.nc`
2. `scripts_basin_test/output/s6_cluster_quality_order.csv`

说明：

1. 当前默认会过滤掉 `climatology`
2. `climatology` 应由单独脚本导出
3. `s6_cluster_quality_order.csv` 会按 `cluster_id + resolution` 列出候选来源的质量排序、分数、rank 和路径
4. `s7` 使用的 `basin_status / basin_flag / basin_distance_m / point_in_local / point_in_basin` 都来自这个 `master nc`
5. 如果更新了 `s5_basin_clustered_stations.csv` 里的 basin 字段，但没有重跑本脚本，那么后续 `s7` catalog 里的这些字段可能仍然为空

### s6_export_resolution_matrix_ncs.py

作用：

1. 按 `daily / monthly / annual` 分别导出一个 `station × time` 矩阵 `nc`
2. 在每个分辨率内部仍沿用 `s6_basin_merge_to_nc.py` 的质量排序合并规则
3. 更适合直接查看某个时间分辨率下的二维数据矩阵

输出目录：

1. `scripts_basin_test/output/s6_matrix_by_resolution/`

### s6_summarize_matrix_ncs.py

作用：

1. 用 `xarray.Dataset` 风格输出矩阵版 `nc` 的可读文本摘要
2. 为每个矩阵 `nc` 输出一个基础统计表
3. 额外生成一个跨文件总览表，方便比较 `daily / monthly / annual`

输出目录：

1. `scripts_basin_test/output/s6_matrix_by_resolution/summary/`

### s6_export_climatology_to_nc.py

作用：

1. 直接扫描 `output_resolution_organized/climatology`
2. 不经过 basin tracing
3. 不经过 cluster merge
4. 将所有 climatology 站点单独导出为一个 `nc`

输出：

1. `scripts_basin_test/output/s6_climatology_only.nc`

### s7_export_cluster_shp.py

作用：

1. 读取 `master nc + daily/monthly/annual matrix nc`
2. 生成 `cluster` 级空间摘要目录
3. 导出主 GIS 产品 `cluster` 多图层 `gpkg`

输出：

1. `scripts_basin_test/output/s7_cluster_points.gpkg`
2. `scripts_basin_test/output/s7_cluster_station_catalog.csv`
3. `scripts_basin_test/output/s7_cluster_resolution_catalog.csv`

说明：

1. `s7_cluster_points.gpkg` 是主 GIS 产品，包含 `cluster_summary / cluster_daily / cluster_monthly / cluster_annual` 多图层
2. `climatology` 不进入 `cluster_uid` 图层体系，继续单独发布
3. `s7_cluster_station_catalog.csv` 和 `s7_cluster_resolution_catalog.csv` 中的 `basin_status` 等字段来自 `s6_basin_merged_all.nc`，不是直接从 `s5` 读

### s7_export_source_station_shp.py

作用：

1. 读取 `master nc`
2. 按实际进入最终主线的 `source_station_uid + resolution` 聚合
3. 导出多图层 `gpkg`
4. 导出 `source station` 分辨率 catalog

输出：

1. `scripts_basin_test/output/s7_source_stations.gpkg`
2. `scripts_basin_test/output/s7_source_station_resolution_catalog.csv`

### s7_export_cluster_basin_shp.py

作用：

1. 从 `s5` 中为每个 `cluster_id` 选代表站点 polygon
2. 读取 `s7_cluster_resolution_catalog.csv`
3. 将代表 polygon 按 `cluster_uid + resolution` 展开
4. 导出多图层 `gpkg`

输出：

1. `scripts_basin_test/output/s7_cluster_basins.gpkg`
2. `scripts_basin_test/output/s7_cluster_basins_local.gpkg`

说明：

1. `s7_cluster_basins.gpkg` 是主 polygon 产品，包含 `basin_daily / basin_monthly / basin_annual`
2. `cluster_basin` 的标准 join key 现在是 `cluster_uid + resolution`
3. 它仍依赖 `s4_upstream_basins.gpkg`
4. 当前只会为 `basin_status = resolved` 的站点导出 basin polygon
5. `unresolved` 站点即使保留在主数据表中，也不会进入 basin polygon sidecar

### s8_publish_reference_dataset.py

作用：

1. 将现有 `s6 / s7` 主线输出整理成用户发布版数据集
2. 生成标准命名的 `sed_reference_*.nc`
3. 复用 `s7` 产出的 cluster/source resolution catalog
4. 生成一行一个 `cluster_uid + resolution` 的 `station_catalog.csv`
5. 生成一行一个 `source_station_uid + resolution` 的 `source_station_catalog.csv`
6. 生成 `source_dataset_catalog.csv`
7. 生成多图层 `GPKG` 空间 sidecar
8. 生成发布版 `README.md` 和验证报告

输出目录：

1. `scripts_basin_test/output/sed_reference_release/`

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

当前主线的空间点产品统一使用 `GPKG`。
发布层中的标准空间文件由 `s8_publish_reference_dataset.py` 生成。

### 5.1 cluster 点位文件

文件：

1. `s7_cluster_points.gpkg`
2. `sed_reference_release/sed_reference_cluster_points.gpkg`

作用：

1. `s7_cluster_points.gpkg` 是主 GIS 产品，包含：
2. `cluster_summary`
3. `cluster_daily`
4. `cluster_monthly`
5. `cluster_annual`
6. 作为 `nc` 和空间数据之间的多分辨率连接层

### 5.2 原始站点点位文件

文件：

1. `s7_source_stations.gpkg`
2. `sed_reference_release/sed_reference_source_stations.gpkg`

作用：

1. 查看某个 `cluster_uid + resolution` 内部有哪些原始站点参与
2. 标准图层为 `source_daily / source_monthly / source_annual`
3. 标准 join key 是 `source_station_uid + resolution`

### 5.3 cluster 级流域单元文件

文件：

1. `s7_cluster_basins.gpkg`
2. `sed_reference_release/sed_reference_cluster_basins.gpkg`

作用：

1. 查看每个 `cluster_uid + resolution` 对应的最终流域单元面
2. 标准图层为 `basin_daily / basin_monthly / basin_annual`
3. 与 `nc` 和 `cluster` 点位文件按复合键做空间联动

## 6. 当前主要输出文件

默认都输出到：

`scripts_basin_test/output/`

如果是内部流程检查，最关键的主线产物是：

1. `s5_basin_clustered_stations.csv`
2. `s6_basin_merged_all.nc`
3. `s7_cluster_points.gpkg`
4. `s7_cluster_station_catalog.csv`
5. `s7_cluster_resolution_catalog.csv`
6. `s7_source_stations.gpkg`
7. `s7_source_station_resolution_catalog.csv`
8. `s7_cluster_basins.gpkg`

如果是面向用户发布，当前推荐直接使用：

1. `output/sed_reference_release/sed_reference_master.nc`
2. `output/sed_reference_release/sed_reference_timeseries_daily.nc`
3. `output/sed_reference_release/sed_reference_timeseries_monthly.nc`
4. `output/sed_reference_release/sed_reference_timeseries_annual.nc`
5. `output/sed_reference_release/sed_reference_climatology.nc`
6. `output/sed_reference_release/station_catalog.csv`
7. `output/sed_reference_release/source_station_catalog.csv`
8. `output/sed_reference_release/sed_reference_cluster_points.gpkg`
9. `output/sed_reference_release/sed_reference_source_stations.gpkg`
10. `output/sed_reference_release/sed_reference_cluster_basins.gpkg`
11. `output/sed_reference_release/README.md`

这套发布层的标准使用顺序是：

1. 先按模型输出时间分辨率选择对应的 matrix `nc`
2. 先把 `station_catalog.csv` 过滤到对应 `resolution`
3. 再用过滤后的 `lat/lon` 找最近的 `cluster_uid`
4. 抽出参考时间序列并与模型结果对齐
5. 如果需要追溯来源，再去 `sed_reference_master.nc`
6. 继续通过 `source_station_catalog.csv` 找到同一 `resolution` 下的原始站点和原始路径

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
4. `19_basin_distance_review_queue.csv`
5. `20_unresolved_basin_queue.csv`

这些表适合优先筛查：

1. 多站点 cluster
2. 多来源重叠 cluster
3. 多时间类型 cluster
4. basin 信息缺失 cluster
5. 大距离偏移站点
6. `unresolved` basin 站点

---

## 8. 推荐运行顺序

如果是完整重跑，建议顺序为：

1. `s1_verify_time_resolution.py`
2. `s2_reorganize_qc_by_resolution.py`
3. `s3_collect_qc_stations.py`
4. `s4_basin_trace_watch.py`
5. `s5_basin_merge.py`
6. `s6_basin_merge_to_nc.py`
7. `s6_export_resolution_matrix_ncs.py`
8. `s6_export_climatology_to_nc.py`
9. `s7_export_cluster_shp.py`
10. `s7_export_source_station_shp.py`
11. `s7_export_cluster_basin_shp.py`
12. `s8_publish_reference_dataset.py`
13. `s9_generate_manual_review_tables.py`
14. `s11_run_checklist_audit.py`

如果只是更新 basin 匹配规则或 `basin_status` 相关字段，至少需要重跑：

1. `s4_basin_trace_watch.py`
2. `s5_basin_merge.py`
3. `s6_basin_merge_to_nc.py`
4. `s6_export_resolution_matrix_ncs.py`
5. `s7_export_cluster_shp.py`
6. `s7_export_cluster_basin_shp.py`
7. `s8_publish_reference_dataset.py`
8. `s9_generate_manual_review_tables.py`
9. `s11_run_checklist_audit.py`

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

1. `s7_export_cluster_shp.py` 需要 `pyshp + geopandas`
2. `s7_export_source_station_shp.py` 和 `s7_export_cluster_basin_shp.py` 需要 `geopandas`
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
8. `s8_publish_reference_dataset.py`
9. `s9_generate_manual_review_tables.py`
10. `s11_run_checklist_audit.py`

为准。

---

## 12. 当前有效规则一句话总结

**按 90m 流域单元合并站点为 `cluster`，保留所有原始站点映射关系；其中 `daily / monthly / annual` 进入 basin 主线，`climatology` 不进入流域筛选环节而是单独导出为 `nc`。对 basin 分配采用保守发布策略：只把 `resolved` 站点发布为 basin polygon，`unresolved` 站点保留观测但不强行发布 basin 面。时间规则上 `single_point` 归为 `daily`，`quarterly` 归为 `monthly`，并额外导出 `cluster` 点位、原始站点点位、`cluster` 级流域单元面文件，以及人工检查表。**
