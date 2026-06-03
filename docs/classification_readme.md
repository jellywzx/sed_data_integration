# classification_readme

## 1. 这份文档在讲什么

这份文档专门梳理 `scripts_basin_test` 中不同类别数据的完整工作流程。

当前按三类理解最清楚：

1. `climatology`
2. `daily / monthly / annual`
3. `satellite`

它们共享同一个前处理入口，但会在 `s3` 和 `s6` 两个位置发生关键分流。

---

## 2. 总体流程图

可以先把整条主线理解成下面这个结构：

```text
原始 nc
  -> s1_verify_time_resolution.py
  -> s2_reorganize_qc_by_resolution.py
  -> output_resolution_organized/
       |- climatology/                 -> s6_export_climatology_to_nc.py -> s6_climatology_only.nc
       |- daily/monthly/annual/        -> s3 -> s4 -> s5 -> s6 mainline -> s7 -> s8
       |- satellite source files       -> 先随 daily/monthly/annual 进入 s3/s4/s5
                                         再在 s6 默认分流到 validation-only 支线
```

最关键的两个分流点是：

1. `s3_collect_qc_stations.py`
   这里默认把 `climatology` 排除在 basin 主线站点表之外
2. `s6_basin_merge_to_nc.py`
   这里默认把 `satellite` 从主 merge 中预过滤掉

---

## 3. 共同入口

三类数据在前两步是共用入口的。

### 3.1 s1_verify_time_resolution.py

作用：

1. 读取原始 `nc`
2. 判断每个文件的时间语义
3. 输出 `temporal_semantics`

这里最重要的不是原始目录名，而是 `s1` 最终给出的时间语义。后续 `s2` 会以这个结果为准。

当前主分类最终会收敛到：

1. `daily`
2. `monthly`
3. `annual`
4. `climatology`
5. `other`

### 3.2 s2_reorganize_qc_by_resolution.py

作用：

1. 根据 `s1` 输出的 `temporal_semantics` 重组文件
2. 把文件放进 `output_resolution_organized/`

结果可以理解为：

1. `climatology` 被放进 `output_resolution_organized/climatology/`
2. 普通时间序列被放进 `daily / monthly / annual/`
3. `satellite` 数据如果其时间语义是 `daily / monthly / annual`，也先进入这些普通分辨率目录

也就是说：

1. `climatology` 是按时间语义直接分出的一类
2. `satellite` 不是按时间语义分出来的，它首先仍然是一个时间分辨率文件，只是后面会按 `source_family` 再分流

---

## 4. 第一类：climatology

### 4.1 进入条件

文件在 `s1` 中被判定为 `climatology`，随后在 `s2` 中被放到：

1. `output_resolution_organized/climatology/`

常见来源包括：

1. 明确声明为 climatology 的数据
2. 虽然原始形式看起来像单点或 annual，但从元数据判断本质是多年平均或长期平均的数据

### 4.2 为什么它不进 basin 主线

`s3_collect_qc_stations.py` 默认会使用：

1. `--exclude-resolutions climatology`

因此：

1. `climatology` 不会进入 `s3_collected_stations.csv`
2. 不会参与 `s4` basin tracing
3. 不会参与 `s5` cluster merge
4. 不会进入 `s6_basin_merge_to_nc.py` 的主 merge

### 4.3 它走哪条支线

`climatology` 走专门的独立导出支线：

1. `s6_export_climatology_to_nc.py`

这个脚本会直接扫描：

1. `output_resolution_organized/climatology/`

设计原则是：

1. 每个 climatology 文件视为一个独立 source station
2. 不做 basin screening
3. 不做 cluster merge
4. 通过 `station_uid`、`source_station_path` 等字段保留 provenance

### 4.4 最终产物

内部产物：

1. `scripts_basin_test/output/s6_climatology_only.nc`
2. `scripts_basin_test/output/s6_climatology_stations.shp`

发布层产物：

1. `scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc`

### 4.5 一句话总结

`climatology` 的工作流是：

**在 `s1/s2` 被识别后，直接绕开 basin 主线，在 `s6` 独立导出，并在 `s8` 独立发布。**

---

## 5. 第二类：daily / monthly / annual

### 5.1 进入条件

文件在 `s1` 中被判定为：

1. `daily`
2. `monthly`
3. `annual`

然后在 `s2` 中被放入对应目录。

这类数据是标准 basin 主线数据。

### 5.2 s3：进入 basin 主线站点表

`s3_collect_qc_stations.py` 会扫描 `output_resolution_organized/` 中的非 `climatology` 文件，生成：

1. `scripts_basin_test/output/s3_collected_stations.csv`

这个表是后续主线的基础站点清单，包含：

1. `path`
2. `source`
3. `lat`
4. `lon`
5. `resolution`
6. `station_name`
7. `river_name`
8. `source_station_id`

### 5.3 s4：做 basin tracing

进入 `s4_basin_trace_watch.py` 后，每个站点会进行 upstream basin tracing，生成：

1. `s4_upstream_basins.csv`
2. `s4_upstream_basins.gpkg`
3. `s4_local_catchments.gpkg`
4. `s4_reported_area_check.csv`

这里会给每个站点补上关键诊断字段：

1. `distance_m`
2. `match_quality`
3. `point_in_local`
4. `point_in_basin`
5. `basin_status`
6. `basin_flag`

### 5.4 s5：分配 cluster_id

`s5_basin_merge.py` 会读取：

1. `s3_collected_stations.csv`
2. `s4_upstream_basins.csv`

然后为站点分配 `cluster_id`。

当前规则是：

1. 只有 `basin_status=resolved` 且 `basin_id` 有效的站点才参与合并
2. 同一 `basin_id` 内，还要满足距离阈值和 `uparea_merit` 相对误差阈值
3. 合并方式是 `complete-linkage`
4. 不满足条件的站点保留为 singleton，`cluster_id=station_id`

产物是：

1. `s5_basin_clustered_stations.csv`
2. `s5_basin_cluster_report.csv`

### 5.5 s6：进入主 merge

这是这类数据最核心的一步。

它们会进入：

1. `s6_basin_merge_to_nc.py`
2. `s6_export_daily_matrix_nc.py`
3. `s6_export_monthly_matrix_nc.py`
4. `s6_export_annual_matrix_nc.py`

结果会生成两层主产品：

1. `master` 层：
   `s6_basin_merged_all.nc`
2. `matrix` 层：
   `s6_basin_matrix_daily.nc`
   `s6_basin_matrix_monthly.nc`
   `s6_basin_matrix_annual.nc`

### 5.6 s7：生成空间文件和 catalog

这类数据会继续进入 `s7`，生成：

1. `s7_cluster_points.gpkg`
2. `s7_cluster_station_catalog.csv`
3. `s7_cluster_resolution_catalog.csv`
4. `s7_source_stations.gpkg`
5. `s7_source_station_resolution_catalog.csv`
6. `s7_cluster_basins.gpkg`

其中 `cluster_uid + resolution` 是后续发布层的标准连接键。

### 5.7 s8：进入正式 release

最后在 `s8_publish_reference_dataset.py` 中被整理为对外发布包，核心产物包括：

1. `sed_reference_master.nc`
2. `sed_reference_timeseries_daily.nc`
3. `sed_reference_timeseries_monthly.nc`
4. `sed_reference_timeseries_annual.nc`
5. `station_catalog.csv`
6. `source_station_catalog.csv`
7. `source_dataset_catalog.csv`
8. `sed_reference_cluster_points.gpkg`
9. `sed_reference_source_stations.gpkg`
10. `sed_reference_cluster_basins.gpkg`

### 5.8 一句话总结

`daily / monthly / annual` 的工作流是：

**完整进入 basin 主线，依次经过 `s3 -> s4 -> s5 -> s6 -> s7 -> s8`，最终形成 master、matrix、catalog 和空间 sidecar。**

---

## 6. 第三类：satellite

### 6.1 最容易混淆的点

`satellite` 不是一种时间分辨率，而是一种 `source_family`。

所以它和 `climatology` 最大的不同是：

1. `climatology` 是在 `s1/s2` 按时间语义分出来的
2. `satellite` 在 `s1/s2` 里仍然会先被当作 `daily / monthly / annual` 这类普通时间序列处理

### 6.2 前半段为什么它看起来和普通时间序列一样

如果一个 satellite 数据文件在 `s1` 中被判定为 `daily / monthly / annual`，那么它会：

1. 在 `s2` 进入对应分辨率目录
2. 在 `s3` 进入 `s3_collected_stations.csv`
3. 在 `s4` 参与 basin tracing
4. 在 `s5` 获得 `cluster_id`

也就是说，satellite 默认并不是在 `s3` 或 `s5` 被踢出主线。

### 6.3 真正分流点在 s6

真正的 satellite 分流发生在：

1. `s6_basin_merge_to_nc.py`

当前代码中，`source_family == satellite` 默认属于：

1. `MERGE_EXCLUDED_SOURCE_FAMILIES = {"satellite"}`
2. `VALIDATION_ONLY_SOURCE_FAMILIES = {"satellite"}`

默认行为是：

1. 在主 merge 之前就从 `s5_basin_clustered_stations.csv` 中预过滤掉 satellite 行
2. 不进入 `s6_basin_merged_all.nc`
3. 不进入 daily/monthly/annual 主 matrix

只有显式传入：

1. `--include-satellite-in-main-merge`

才会允许 satellite 混入主库。

### 6.4 它默认走哪条支线

默认情况下，satellite 会走独立的 validation-only 支线：

1. `s6_export_satellite_validation_to_nc.py`

这个脚本会：

1. 从 `s5_basin_clustered_stations.csv` 中筛出 `source_family=satellite`
2. 只保留 `daily / monthly / annual`
3. 读取对应 source 文件
4. 生成一份单独的 satellite validation-only 数据集

这里的关键点是：

1. satellite 仍然保留 `cluster_id` 和 `cluster_uid` 关联
2. 但它的 `merge_policy` 是 `validation_only`
3. 它的角色是用于 `satellite-vs-station` 对比和诊断，而不是参与主 station-reference merge

### 6.5 最终产物

内部产物：

1. `scripts_basin_test/output/s6_satellite_validation_only.nc`
2. `scripts_basin_test/output/s6_satellite_validation_catalog.csv`

发布层产物：

1. `scripts_basin_test/output/sed_reference_release/sed_reference_satellite_validation.nc`
2. `scripts_basin_test/output/sed_reference_release/satellite_validation_catalog.csv`

### 6.6 它在 release 里的地位

到了 `s8`，satellite validation-only 数据会被作为独立 sidecar 带入 release。

语义上它是：

1. release 的组成部分
2. 但不是主 station-reference merge 的组成部分
3. 主要用于验证和对比，不用于主库时间序列合并

### 6.7 一句话总结

`satellite` 的工作流是：

**前半段可以进入 `s3/s4/s5` 保留站点和流域元数据，但 `s5` 不再让 satellite 参与 basin cluster 合并；satellite 行保留为独立 singleton，随后在 `s6` 默认从主 merge 中分流出去，单独导出为 validation-only 产品，并在 `s8` 作为独立 sidecar 发布。**

---

## 7. 三类数据对照表

| 类别 | s3 是否进入主线站点表 | s4/s5 是否参与 | s6 是否进入主 merge | s8 是否进入 release |
|---|---|---|---|---|
| `climatology` | 否 | 否 | 否，走独立 climatology 导出 | 是，独立发布 |
| `daily/monthly/annual` | 是 | 是 | 是 | 是，属于主合同 |
| `satellite` | 是 | 保留行但不参与 basin cluster 合并 | 默认否，走 validation-only 支线 | 是，作为 validation sidecar |

---

## 8. 最短总结

可以把三类数据用一句话分别记成：

1. `climatology`：**时间语义上先分出去，绕开 basin 主线，独立导出**
2. `daily/monthly/annual`：**标准 basin 主线数据，完整走 `s3 -> s8`**
3. `satellite`：**保留为 S5 singleton，再在 `s6` 默认分流到 validation-only 支线**
