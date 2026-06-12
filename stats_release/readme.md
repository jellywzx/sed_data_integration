# Release-Only Statistics — 代码模块说明

本目录是沉积物参考数据集（sediment reference dataset）面向发布的统计代码模块包（`stats_release`）。所有模块**仅读取**发布包 `output/sed_reference_release/` 中的产品文件，输出写至 `output_other/stats_release/` 下各模块子目录。

---

## 运行方式

```bash
# 运行全部 8 个模块（推荐）
python3 -m stats_release.run_all_release_stats

# 运行单个模块，如 spatial
python3 -m stats_release.spatial --release-dir output/sed_reference_release

# 跳过图件生成
python3 -m stats_release.spatial --release-dir output/sed_reference_release --skip-figures

# 允许读取发布包外文件（仅本地调试）
python3 -m stats_release.spatial --release-dir output/sed_reference_release --allow-non-release-inputs
```

> `--strict-release-only` 默认启用；`--allow-non-release-inputs` 仅在本地调试时关闭路径守卫。

---

## 模块总览

| 模块 | 功能 | 关键输入 | 主要输出目录 |
|------|------|----------|-------------|
| `release_paths.py` | 路径与产品文件名注册中心 | 无（常量定义） | 无 |
| `release_io.py` | 共享 I/O 工具层 | 无（工具函数） | 无 |
| `common_stats.py` | 共享统计工具函数 | 无（工具函数） | 无 |
| `parity.py` | 旧版输出兼容性清单 | 无 | 根目录 `parity_manifest.csv` |
| `inventory.py` | 发布包文件清单与健康检查 | 所有产品文件 | `inventory/tables/` |
| `spatial.py` | 空间覆盖统计 | `station_catalog.csv` + GeoPackage | `spatial/tables/`, `spatial/figures/` |
| `temporal.py` | 时间覆盖统计 | 矩阵 NetCDF + CSV 目录 | `temporal/tables/`, `temporal/figures/` |
| `variable_summary.py` | 变量覆盖率 (Q/SSC/SSL) | 各 NetCDF 产品 | `variable_summary/tables/`, `variable_summary/figures/` |
| `source_contribution.py` | 数据源贡献统计 | 3 个目录 CSV + satellite | `source_contribution/tables/`, `source_contribution/figures/` |
| `source_dataset_layers.py` | 数据源目录分层隶属关系 | 同上 | `source_dataset_layers/tables/` |
| `basin_diagnostics.py` | 流域分配诊断 | `station_catalog.csv` | `basin_diagnostics/tables/`, `basin_diagnostics/figures/` |
| `qc_flags.py` | QC 标记统计 | `master_nc`, `climatology_nc`, `satellite_nc` | `qc_flags/tables/`, `qc_flags/figures/` |
| `run_all_release_stats.py` | 总控入口 | 无 | 根目录 `run_summary.csv` 等 |

---

# 各模块详细说明

---

## 1. `release_paths.py` — 路径与产品文件名注册中心

定义所有模块共享的默认路径常量与产品文件名映射。

**关键常量**:
- **`PRODUCT_FILES`**: 字典，映射产品标识 → 文件名。涵盖全系列发布产品：
  - NetCDF: `master_nc`, `daily_nc`, `monthly_nc`, `annual_nc`, `climatology_nc`, `satellite_nc`
  - CSV 目录: `station_catalog`, `source_station_catalog`, `source_dataset_catalog`, `satellite_catalog`, `satellite_validation_catalog`
  - GeoPackage: `cluster_points_gpkg`, `cluster_basins_gpkg`, `source_stations_gpkg`
  - Overlap/Satellite candidates: `.csv.gz` 和 `.parquet` sidecar
  - 其他: `inventory_csv`, `validation_csv`, `readme`, `application_readme`, `example_workflow`
- **`MATRIX_PRODUCTS`**: 日/月/年三个矩阵 NetCDF 产品的映射 `{daily→daily.nc, monthly→monthly.nc, annual→annual.nc}`
- **`CORE_PRODUCTS`**: 8 个核心产品名元组

---

## 2. `release_io.py` — 共享 I/O 工具层

所有统计模块的基础设施层，提供统一文件读取/写入/路径安全守卫。

**核心类 — `ReleaseContext`**:
- 提供对发布包的只读访问，自动检查路径是否在发布包内（`strict_release_only`）
- 主要方法:
  - `release_file(name)`: 拼接发布包内文件路径
  - `require_input(path)`: 校验文件存在且位于发布包内
  - `output_path(*parts)`: 生成输出路径并自动建目录
  - `read_csv(...)`: 读取 CSV（支持相对/绝对路径）
  - `open_dataset(...)`: 打开 NetCDF 数据集
  - `sqlite_connect(...)`: 连接 SQLite（用于读取 GeoPackage）

**核心工具函数**:
- `add_common_args(parser)`: 添加 `--release-dir`, `--out-dir`, `--strict-release-only`, `--skip-figures`, `--copy-reports`, `--dpi` 等公共 CLI 参数
- `numeric_series(frame, col)`: 安全转数值列，非法值变 NaN
- `text_series(frame, col)`: 安全转文本列
- `clean_text(value)`: 清理文本（处理 NaN/None/空值）
- `read_numeric_var(ds, name)`: 从 NetCDF 读取数值变量（处理掩码数组和填充值 -9999, 1e20）
- `read_text_var(ds, name)`: 从 NetCDF 读取文本变量
- `netcdf_record_count(ds)`: 获取 NetCDF 的记录维度大小（支持 `n_records`/`n_satellite_records`/`record` 三种维度名）
- `count_matrix_selected_cells(ds)`: 统计矩阵产品中有 `selected_source_index >= 0` 的网格数
- `setup_matplotlib()`: 初始化 matplotlib（设置 Agg 后端，返回 plt）

---

## 3. `common_stats.py` — 共享统计工具函数

所有模块共用的统计、枚举、分类和图形输出函数。

**关键常量**:
- `VARIABLES = ("Q", "SSC", "SSL")`: 核心变量
- `FLAG_VALUES = (0, 1, 2, 3, 8, 9)`: QC 标记值
- `FLAG_MEANINGS`: 标记含义字典

**核心函数**:
- `pct(numerator, denominator)`: 百分比计算
- `classify_source(source_name, source_family)` → `(source_type, source_group)`: 数据源分类
  - 卫星: "riversed", "gsed", "dethier", "shashi_jianli"
  - 气候态: "milliman", "vanmaercke", "hma", "ali_de_boer"
  - 国家机构: "usgs", "hydat", "bayern"
  - 区域数据集: "hybam"
  - 文献: "literature"
- `attach_source_classification(frame)`: 为 DataFrame 添加 `source_type` 和 `source_group` 列
- `numeric_stats(values)`: 完整数值统计量（mean/median/std/min/max/P05/P25/P75/P95/P99/log10_mean/log10_median）
- `decode_time_axis(ds)`: 从 NetCDF 时间轴解码为 `pd.DatetimeIndex`（支持多种 units 和 calendar）
- `decode_time_values(ds, values)`: 解码指定时间值
- `resolution_values(ds, key)`: 从 NetCDF 读取分辨率列，支持枚举映射（0=daily, 1=monthly, 2=annual, 3=climatology）
- `save_figure(fig, png_path, dpi, also_pdf=True)`: 保存图片（同步输出 PDF）
- `write_geojson_points(frame, path)`: 将 DataFrame 写出为 GeoJSON Point FeatureCollection

---

## 4. `parity.py` — 旧版输出兼容性清单

> 本模块不在 `run_all_release_stats.py` 的主循环内，而是由总控脚本在末尾单独调用 `build_parity_manifest()`。

**功能**: 维护一个从旧版统计脚本输出路径到新版 release-only 输出路径的映射表（`LEGACY_TARGETS`），并生成 `parity_manifest.csv` 清单，逐条记录每个旧版输出在新版下是否可生成（`release_only_capable`）以及实际存在状态。

**包含的版本对应**:
- 列出 inventory、spatial、temporal、source_dataset_layers、source_contribution、basin_diagnostics、variable_summary、qc_flags 等 8 个模块的全部旧版→新版输出映射
- 标出 `unsupported` 条目（如 S3/S4/S5/S6/S7 流水线中间产物），它们在 release-only 模式下无法生成，原因记录在 `unsupported_reason`

**输出**: `parity_manifest.csv`
- 每条记录包含: `module`, `legacy_script`, `legacy_output`, `new_output`, `release_only_capable`, `unsupported_reason`, `status` (generated/missing_release_capable/unsupported_release_only), `exists`, `size_bytes`

**如何看结果**:
- 检查 `status` 列：`generated` = 已生成；`missing_release_capable` = 理论上可生成但实际未找到；`unsupported_release_only` = 需要流水线中间产物，release-only 模式下不支持
- `unsupported_reason` 列给出不支持的具体原因

---

## 5. `inventory.py` — 发布包文件清单与健康检查

对发布包中所有注册产品和未注册文件进行完整清单、模式分析和一致性校验。

### 统计功能

#### (1) 发布包文件清单（`build_release_files`）
- 遍历发布包目录中所有文件，识别其对应的产品名（`PRODUCT_FILES` 映射）
- 记录文件类型（csv/nc/gpkg/parquet 等）、大小（bytes 和 MB）
- 对照 `release_inventory.csv` 标注是否在清单中列出

#### (2) NetCDF 模式分析（`build_netcdf_schema`）
- 对每个 NetCDF 产品提取完整模式信息：
  - Global attributes（个数）
  - Dimensions（名称和长度）
  - Variables（数据类型、维度、大小、单位、long_name、flag_values、flag_meanings）

#### (3) GeoPackage 图层分析（`build_gpkg_layers`）
- 对每个 GeoPackage 查询 `gpkg_contents` 表，列出所有要素图层
- 统计每个图层的要素数（feature_count）和列数（column_count）

#### (4) 发布包健康检查
- **release_inventory_mismatches**: 对比 `release_inventory.csv` 清单和磁盘上的实际文件，检测两者差异（磁盘有但清单无 / 清单有但磁盘无）
- **path_leaks**: 检查 CSV 和 NetCDF 中的路径字段是否包含本地绝对路径（`/share/home/`、`/home/` 等），这些信息不应出现在发布包中
- **active_metadata_consistency**: 检查 master NetCDF 中的 `cluster_uid`/`source_station_uid` 是否在对应 CSV 目录中有对应记录，以及未使用的（inactive）条目情况
- **validation_contradictions**: 检查 `release_validation_report.csv` 中标记为"not generated"或"not found"的文件是否实际存在于发布包中

### 输出说明

所有输出位于 `<out-dir>/inventory/tables/` 下（默认 `output_other/stats_release/inventory/tables/`）：

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `release_inventory_stats.csv` | 注册产品清单（存在性、大小、行数/维度/图层） + 未注册文件列表 | `exists`=0 表示缺失；`registration_status=unregistered` 表示发布包中有配置文件未记录的文件 |
| `release_inventory_stats_files.csv` | 发布包内每个文件的详细记录 | 查看各文件的类型、大小、描述 |
| `release_inventory_stats_summary.csv` | 长格式汇总指标 | 文件总数/总大小/注册数/各产品是否存/各 NetCDF 维度数/变量数 |
| `release_inventory_stats_summary_wide.csv` | 宽格式汇总（一行全指标） | 适合快速查看关键数值 |
| `release_inventory_stats_netcdf_schema.csv` | 全量 NetCDF 模式表格 | 了解每个 NetCDF 文件的结构 |
| `release_inventory_stats_gpkg_layers.csv` | GeoPackage 图层信息 | 了解空间数据的分层和要素数 |
| `release_inventory_stats_article_metrics.csv` | 论文级关键指标 | 文件数/总大小/各 NetCDF 变量数 |
| `release_inventory_mismatches.csv` | `release_inventory.csv` 与磁盘差异列表 | `on_disk_not_in_release_inventory` 或 `release_inventory_entry_missing_on_disk` |
| `path_leaks.csv` | 本地路径泄露检查 | `local_path_count` >0 表示存在路径泄露风险 |
| `active_metadata_consistency.csv` | cluster_uid/source_station_uid 一致性检查 | `nc_missing_from_catalog`、`catalog_missing_from_nc`、`inactive_nc_entries` 等 |
| `inactive_metadata_entries.csv` | 未使用的元数据条目细表 | 列出每个未使用的 UID 及是否在目录中存在 |
| `validation_contradictions.csv` | 验证报告与实况矛盾检查 | validation 说缺失但实际存在的文件 |

**Markdown 报告**: `<out-dir>/inventory/reports/release_inventory_stats.md`

---

## 6. `spatial.py` — 空间覆盖统计

从发布产品中计算测站/聚类的空间分布统计量。

### 统计功能

#### (1) 聚类空间属性（`_cluster_table` + `table_cluster_spatial_attributes`）
- 从 `station_catalog.csv` 构建每个 `cluster_uid` 一行的小表
- 聚合分辨率列表、记录数总和
- 提取国家、大洲、ISO 代码、流域状态/标记、经纬度
- 校验经纬度有效性

#### (2) 按分辨率统计（`by_resolution`）
- 按 resolution（daily/monthly/annual/climatology 等）聚合行数、聚类数、记录数、国家数

#### (3) 按国家/大洲统计（`by_country` / `by_region`）
- 按大洲和国家分组，统计聚类数、记录数、有效坐标数
- 排序由大到小

#### (4) 国家-地区别名消歧（`country_aliases`）
- 处理同一地理区域的不同名称表示（如 iso_a3 和 country 列不一致时）
- 标记 `has_alias_conflict`

#### (5) 流域状态（`basin_status`）
- 按 basin_status + basin_flag 分组统计聚类数和记录数

#### (6) GeoPackage 图层统计（`gpkg_layers`）
- 统计 `cluster_points.gpkg`、`cluster_basins.gpkg`、`source_stations.gpkg` 中每个图层的要素数

#### (7) 汇水面积分布（`upstream_area_distribution`）
- 按面积分箱统计：<10km², 10-100, 100-1000, 1000-10000, 10000-100000, >100000

#### (8) 卫星目录空间覆盖（`satellite_summary`）
- 按 source × resolution 统计卫星站点数、关联聚类数、记录数

#### (9) 源站点地理分布（`source_geo`）
- 按大洲 × 国家统计源站点行数和数据源数

#### (10) 源空间贡献（`table_spatial_coverage_by_source` / `by_source_type` / `by_region_source`）
- 按 source_name 统计聚类数、源站点行数、记录数、分辨率、经纬度范围、记录百分比
- 按 source_type/source_group 汇总

### 输出说明

输出位于 `<out-dir>/spatial/tables/` 和 `<out-dir>/spatial/figures/`：

**表格（tables/）**:

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_headline.csv` | 核心概览指标 | 直接看 `value` 列：聚类总数、有效经纬度比例、未知国家数 |
| `table_spatial_coverage_summary.csv` | 详细摘要 | 各维度分区汇总 |
| `table_spatial_coverage_by_resolution.csv` | 按分辨率统计 | 看每日/月/年聚类数和记录数的差异 |
| `table_spatial_coverage_by_region.csv` | 按大洲统计 | 了解全球分布格局 |
| `table_spatial_coverage_by_country.csv` | 按国家统计 | 最重要的空间分布表 |
| `table_spatial_coverage_by_region_resolution.csv` | 大洲 × 分辨率 | 了解各地区的时间分辨率覆盖 |
| `table_spatial_coverage_by_source.csv` | 按数据源统计空间贡献 | 对比各数据源的空间覆盖范围 |
| `table_spatial_coverage_by_region_source.csv` | 大洲 × 数据源 | 了解各数据源在哪些大洲有贡献 |
| `table_spatial_coverage_by_source_type.csv` | 按源类型汇总 | 卫星 vs 实测 vs 气候态的对比 |
| `table_basin_status.csv` | 流域状态统计 | `resolved` 比例越高越好 |
| `table_upstream_area_distribution.csv` | 汇水面积分布 | 看面积分箱中聚类分布 |
| `table_spatial_coverage_by_source.csv` 中 `record_percent_of_source_catalog` | 各源贡献占比 |
| `table_satellite_validation_spatial_coverage.csv` | 卫星数据空间覆盖 |
| `table_unknown_country_region_clusters.csv` | 国家/大洲不明的聚类 | 需要人工核验的条目 |
| `table_basin_polygon_layers.csv` | 流域多边形图层要素数 |
| `table_cluster_spatial_attributes.csv` | 聚类级完整空间属性 | 每一行是一个聚类，含经纬度/面积/状态/分辨率 |

**图件（figures/）**:
- `fig_spatial_coverage_by_resolution.png` — 分辨率柱状图
- `fig_spatial_coverage_by_region_country.png` — 国家 top 15 柱状图
- `fig_global_cluster_distribution.png` — 全球聚类散点图（点大小代表流域面积）
- `fig_spatial_coverage_by_region.png` — 各大洲柱状图
- `fig_upstream_area_distribution.png` — 汇水面积分布柱状图
- `fig_source_spatial_contribution.png` / `fig_spatial_coverage_by_region_source_records.png` — 源贡献
- `fig_spatial_coverage_by_region_resolution.png` — 大洲 × 分辨率堆叠柱状图
- `fig_satellite_validation_spatial_distribution.png` — 卫星源记录数对比
- `fig_global_cluster_status_and_basins.png` — 流域状态柱状图
- `global_cluster_distribution_points.geojson` — 空间点数据（可用于 GIS）

---

## 7. `temporal.py` — 时间覆盖统计

统计各产品的时间跨度、各分辨率下活跃单元数量、记录长度分布。

### 统计功能

#### (1) 基础时间范围（`summary` / `basic_summary`）
- 从矩阵 NetCDF（日/月/年）产品读取 `time` 变量的起止范围
- 记录长度（time steps）来自 NetCDF 的维度或矩阵产品的 `selected_source_index` 计数
- 覆盖 products：master、climatology、satellite

#### (2) 时间轴诊断（`time_axis_diagnostics`）
- 对日/月/年产品分别分析时间轴完整性：
  - `n_time`: 时间步数
  - `time_start` / `time_end`: 起止日期
  - `unique_years` / `unique_year_months`: 覆盖的年/月数
  - `expected_regular_periods`: 理论完整周期数（用于检测缺失）
  - `duplicate_periods`: 重复周期数
  - `axis_interpretation`: 自动判断时间轴类型——"regular_period_axis"（完整规则时间轴）或 "sparse_observation_date_axis"（稀疏观测日期轴）

#### (3) 矩阵产品时间扫描（`_scan_matrix_temporal` → `temporal_coverage_by_resolution`）
- 逐分辨率（日/月/年）扫描矩阵 NetCDF 产品，统计：
  - 按年份的活跃单元数、记录数（按变量 Q/SSC/SSL 细分）
  - 每个聚类的首末日期和记录长度
  - 长记录统计：记录长度超过 10/20/30/50/100 年的聚类数
  - 平均/中位数/最大记录长度

#### (4) 记录长度分布（`record_length_distribution`）
- 按分辨率分箱统计单位记录长度分布（0, 1-10, 11-30, 31-100, 101-365, 366-3650, >3650 天）

#### (5) 按变量统计（`temporal_coverage_by_variable`）
- 按 resolution × variable 统计活跃单元数和记录数

#### (6) 源时间覆盖（`temporal_coverage_by_source`）
- 从 station_catalog 的 `sources_used` 展开，按 source_name 统计首次/末次年份、记录数、活跃单元数、分辨率

#### (7) 气候态时间覆盖（`climatology_*`）
- 从 `source_station_catalog` 筛选 resolution=climatology 的记录
- 按 source_name 汇总气候态的时间范围

#### (8) 卫星时间覆盖（`satellite_*`）
- 从 `satellite_catalog` 统计卫星验证产品的时空覆盖
- 按年、按 source 汇总
- 按关联的 cluster_uid 汇总

### 输出说明

**表格（tables/）**:

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_temporal_summary.csv` | 各产品时间范围总览 | 起止年份越长越好；`record_count_nc` 是实际记录数 |
| `table_temporal_time_axis_diagnostics.csv` | 时间轴完整性诊断 | 关键！检查 `duplicate_periods` 是否 >0（有重复）、`axis_interpretation` 是否为 "sparse_observation_date_axis" |
| `table_temporal_coverage_by_resolution.csv` | 各分辨率详细时间覆盖 | 查看各时间分辨率的活跃单元数、记录数、起止年份、长记录比例 |
| `table_temporal_coverage_by_variable.csv` | 按变量 × 分辨率统计 | 对比 Q、SSC、SSL 在各时间分辨率下的覆盖率 |
| `table_active_units_by_year.csv` | 逐年活跃单元数 | 看各分辨率的活跃聚类随时间的变化趋势 |
| `table_record_length_distribution.csv` | 记录长度分布 | 聚类在各长度区间的分布——长记录（>3650天）越多越有价值 |
| `table_temporal_coverage_record_lengths_by_unit.csv` | 每个聚类的首末日期和记录长度 | 诊断单个聚类的时间覆盖完整性 |
| `table_long_records_by_resolution.csv` | 长记录统计 | `n_gt_50_years` 可帮助评估长期观测能力 |
| `table_temporal_coverage_by_source.csv` | 各数据源的时间跨度 | 对比不同数据源的时间覆盖范围 |
| `table_temporal_coverage_by_region_resolution.csv` | 地区 × 分辨率的时间覆盖 | 了解不同地区的观测时长 |
| `table_climatology_temporal_summary.csv` | 气候态时间覆盖 |
| `table_climatology_by_source.csv` | 气候态各源时间覆盖 |
| `table_satellite_temporal_summary.csv` | 卫星产品时间覆盖 |
| `table_satellite_by_year.csv` | 卫星逐年统计 |
| `table_satellite_by_source.csv` | 卫星各源时间覆盖 |
| `table_satellite_by_linked_cluster.csv` | 卫星关联聚类统计 |

**图件（figures/）**:
- `fig_temporal_coverage.png` — 各产品时间范围横幅图（横线=覆盖期，点的大小=记录数）
- `fig_active_units_by_year.png` — 逐年活跃单元折线图（不同分辨率不同颜色）
- `fig_records_by_year_variable.png` — 逐年记录数
- `fig_record_length_distribution.png` — 记录长度分布柱状图
- `fig_long_record_counts.png` — 长记录分组柱状图
- `fig_source_temporal_span.png` — 各数据源时间跨度图
- 卫星/气候态专用图: `fig_climatology_source_contribution.png`、`fig_satellite_source_contribution.png` 等

---

## 8. `variable_summary.py` — 变量覆盖率统计 (Q/SSC/SSL)

统计发布 NetCDF 产品中核心变量（Q=流量, SSC=悬沙浓度, SSL=悬沙输移量）的数据覆盖率和分布特征。

### 统计功能

#### (1) 基本覆盖率（`variable_coverage`）
- 对 master_nc、climatology_nc、satellite_nc 三个产品分别统计
- 每个变量统计：
  - `n_records`: 总记录数
  - `n_present`: 有值（finite）的记录数
  - `n_good`: 有值且 QC flag = 0 (good) 的记录数
  - `n_usable`: 有值且 QC flag ∈ {0,1} (good + estimated) 的记录数
  - 以及对应的百分比

#### (2) 按分辨率覆盖率（`variable_coverage_by_resolution`）
- 从 master NetCDF 中各变量数据覆盖率按分辨率（daily/monthly/annual）分解
- 统计每种分辨率的记录总数、变量记录数、变量聚类数和覆盖率百分比

#### (3) 变量汇总统计量（`variable_summary_statistics`）
- 对 master 产品按 resolution × variable 计算完整统计量：
  - 均值/中位数/标准差/最小/最大/P05/P25/P75/P95/P99
  - log10 均值/中位数（仅正值）
  - 单位标注

#### (4) 共定位变量覆盖（`colocated_variable_coverage`）
- 统计所有变量的共现模式——哪些记录同时有多个变量：
  - Q only, SSC only, SSL only
  - Q+SSC, Q+SSL, SSC+SSL
  - Q+SSC+SSL（三者齐全）
  - 按 resolution 拆分
- 这对于了解同时观测流量、悬沙浓度和输移量的记录比例非常有价值

#### (5) 极端值审查（`extreme_value_review_points`）
- 对每个 resolution × variable 提取最高的 20 个值，包含：
  - station_index、record_index
  - 标记为 `top_high_value`
- 用于人工审查极端值是否合理

#### (6) 卫星变量按源覆盖（`satellite_variable_by_source`）
- 对 satellite NetCDF 产品，按 source × variable 统计覆盖率

### 输出说明

**表格（tables/）**:

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_variable_coverage.csv` | 三产品变量覆盖率对比 | 关注 `present_percent`（有值比例）和 `usable_percent`（可用比例），各变量间的差异 |
| `table_variable_coverage_by_resolution.csv` | master 按分辨率分变量覆盖率 | 了解变量在不同时间分辨率下的覆盖完整度 |
| `table_variable_summary_statistics.csv` | 变量值分布统计量 | 均值/中位数/分位数等，了解变量的典型取值范围 |
| `table_colocated_variable_coverage.csv` | 变量共现统计 | 关注 `Q+SSC+SSL` 的记录占比——三者齐全的记录对分析最有价值 |
| `table_extreme_value_review_points.csv` | 极端值列表 | 人工审查这些极端值是否合理 |
| `table_satellite_variable_by_source.csv` | 卫星产品按数据源×变量覆盖 | 对比卫星数据各源的覆盖率 |

**图件（figures/）**:
- `fig_Q_distribution.png` — Q 的密度分布图（线性轴）
- `fig_SSC_distribution.png` — SSC 的密度分布图（对数轴，SSC/SSL 值较高时用 log10）
- `fig_SSL_distribution.png` — SSL 的密度分布图（对数轴）
- 每个图中叠加 master/climatology/satellite 三条曲线，便于对比

---

## 9. `source_contribution.py` — 数据源贡献统计

综合统计各数据源在发布产品中的贡献量，包括记录数、站点数、聚类数、时间覆盖和变量覆盖。

### 统计功能

#### (1) 多角度源贡献汇总（`source_summary`）
- 从 station_catalog 展开 `sources_used` 字段，按 source_name 统计主目录贡献
- 从 source_station_catalog 按 source_name 统计属性记录
- 从 satellite_catalog 按 source × resolution 统计卫星贡献
- 与 source_dataset_catalog 外连接确保覆盖所有注册源
- 计算 `cluster_attributed_record_count`（展开源关联的记录数）和 `record_attributed_record_count`（源站属性记录数）
- `over_attribution_record_count` = 两者之差——多源聚类导致的重复计数

#### (2) 数据源详细贡献表（`source_dataset_contribution`）
- 综合 source_station_catalog + satellite_catalog 的数据
- 按源汇总：站点数、聚类数、记录总数、各变量记录数、起止年份、分辨率列表
- 计算 `percentage_of_total_records`（占总记录百分比）
- 优先使用 source_station_catalog 的变量信息，对卫星源使用硬编码变量映射

#### (3) 按源类型/分组汇总（`source_type_contribution`）
- 分别按 `source_group` 和 `source_type` 两级汇总
- 统计源数据集数、站点数、聚类数、记录数、各变量记录数、时间范围

#### (4) 按分辨率汇总（`source_resolution_contribution`）
- 每个源在各分辨率下的记录数
- 计算：`percentage_of_total_records`（占总记录比）和 `percentage_within_source_records`（占该源记录比）

#### (5) 按变量汇总（`source_variable_contribution`）
- 每个源在各变量下的记录数
- 计算：占该变量总记录的百分比、占该源总记录的百分比

#### (6) 排名与累积（`top_source_contributors` / `source_contribution_cumulative`）
- Top 20 源（按 record / station / cluster 三个维度分别排名）
- 累积贡献曲线数据（`cumulative_percent`）：显示前 N 个源贡献了多少百分比

#### (7) 源分类模板（`source_classification_template`）
- 输出每个源的 `source_name`、`source_type`、`source_group`，可用于人工审核和修正分类

### 输出说明

**表格（tables/）**:

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_source_summary.csv` | 多角度源汇总（含主目录、源站目录、卫星目录） | 对比 `cluster_attributed_record_count` 和 `record_attributed_record_count`，差异大表明多源聚类多 |
| `table_source_resolution.csv` | 源×分辨率明细 | 了解每个源在何种时间分辨率下有数据 |
| `table_satellite_source_resolution.csv` | 卫星源×分辨率明细 |
| `table_source_dataset_contribution.csv` | 源级综合贡献表（推荐使用） | 最重要的源贡献表——百分比列直观显示各源占比 |
| `table_source_type_contribution.csv` | 按源类型/分组汇总 | 对比卫星 vs 实测 vs 气候态的总体贡献 |
| `table_source_resolution_contribution.csv` | 源×分辨率记录数 | 关注 `percentage_within_source_records` 了解源的数据时间分布 |
| `table_source_variable_contribution.csv` | 源×变量记录数 | 了解各源提供哪些变量 |
| `table_top_source_contributors.csv` | Top 20 源排名 | 三个维度：records、source_stations、clusters |
| `table_source_contribution_cumulative.csv` | 累积贡献数据 | `cumulative_percent` 显示少数源是否占主导 |
| `table_source_temporal_coverage.csv` | 各源时间跨度 | 起止年份和 `year_span` |
| `table_report_key_metrics.csv` | 关键指标汇总 | 快速了解总量 |
| `source_classification_template.csv` | 源分类清单 | 人工审核分类是否正确 |

**图件（figures/）**:
- `fig_source_contribution_records.png` / `fig_source_contribution_clusters.png` / `fig_source_contribution_stations.png` — 各源在三个维度上的水平柱状图
- `fig_source_cumulative_contribution.png` — 累积贡献曲线
- `fig_source_type_records.png` / `fig_source_group_records.png` — 按类型/分组汇总
- `fig_source_resolution_stacked.png` — 源×分辨率堆叠柱状图
- `fig_source_variable_stacked.png` — 源×变量堆叠柱状图
- `fig_source_temporal_coverage.png` — 各源时间跨度图
- 卫星和气候态专用图: `fig_satellite_*` / `fig_climatology_*` 系列

---

## 10. `source_dataset_layers.py` — 数据源目录分层隶属统计

追踪每个数据源在各个发布目录层中的出现情况，展示数据源在流水线各阶段的分布。

### 统计功能

#### (1) 构建分层隶属关系（`membership`）
将多个目录层汇总成一个统一的长表，每行代表一个源在一个层中的一条记录：
- **main_station_catalog 层**: 从 station_catalog 的 `sources_used` 字段展开
- **source_station_catalog 层**: 直接使用 source_name
- **satellite_catalog 层**: 使用 `source` 列
- **sidecar 层**: overlap_candidates（csv.gz/parquet）、satellite_candidates（csv.gz/parquet）、satellite_validation_catalog

#### (2) 按源×层聚合（`summary`）
- 每源每层的行数、聚类数、记录数、分辨率列表

#### (3) 源级总览（`source_rollup`）
- 每个源在多少层有出现、总行数、总聚类数、总记录数
- 与 source_dataset_catalog 外连接，确保覆盖所有注册源

### 输出说明

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_source_layer_membership.csv` | 源×层细表（源目录根目录也有输出） | 查看每个源在各层的出现情况 |
| `table_source_layer_summary.csv` | 源×层聚合（源目录根目录也有输出） | 每层的行/聚类/记录数 |
| `table_source_layer_source_rollup.csv` | 源级总览 | 一个源一行，看它涉及哪些层 |
| `table_source_layer_unsupported_pipeline_layers.csv` | 标记不可用的流水线层 | 解释哪些层在 release-only 模式下不可用 |

---

## 11. `basin_diagnostics.py` — 流域分配诊断

分析测站到流域的匹配质量，包括状态统计、距离分析、人工复查队列等。

### 统计功能

#### (1) 空间匹配错误表（`spatial_match_error_table`）
- 为 station_catalog 每条记录添加：
  - `spatial_error_class`: 空间错误分类（high_confidence, large_offset, area_mismatch, geometry_inconsistent, unresolved 等）
  - `match_quality`: 匹配质量（high/moderate/manual_review/excluded）
  - `distance_bin`: 距离分箱（0, 0-100, 100-1000, ..., >50000 m）
- 错误分类逻辑:
  - high_confidence: 已解决且距离 ≤1000m
  - large_offset: 距离 >50000m
  - area_mismatch: basin_flag 含 "area"
  - geometry_inconsistent: basin_flag 含 "geometry"
  - unresolved: basin_status=unresolved

#### (2) 状态/标记/质量汇总（`status_counts` / `spatial_match_*_counts`）
- 按 basin_status + basin_flag 统计行数、聚类数、记录数
- 按 match_quality、spatial_error_class 统计
- 按 resolution × basin_status、distance_bin × basin_status 交叉统计

#### (3) 未解决源和国家的专项统计（`unresolved_by_source` / `unresolved_by_country`）
- 按数据源统计未解决（unresolved）的行数和记录数及百分比
- 按国家统计未解决的情况

#### (4) 已解决点标记异常（`resolved_point_anomalies`）
- 查找已解决（resolved）但 point_in_local 或 point_in_basin 存在异常的记录

#### (5) 阈值敏感性分析（`spatial_match_threshold_sensitivity`）
- 不同距离阈值（0/100/1000/5000/10000/50000 m）下被"接受"的行数和百分比

#### (6) 人工复查队列（`manual_review_*`）
- 大偏移队列（top 200，按距离降序）
- 面积不匹配队列（top 200）
- 几何不一致队列（top 200）
- 高风险队列（match_quality=manual_review 或 unresolved，top 200）

#### (7) 有报告汇水面积的行子集统计（`reported_area_*`）
- 对 basin_area >0 的行子集重复上述分类统计

### 输出说明

**表格（tables/）**:

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_basin_status_counts.csv` | basin_status × basin_flag 统计 | 快速了解 resolved/unresolved 比例 |
| `table_basin_status_by_resolution.csv` | 分辨率×状态统计 | 了解不同时间分辨率下的匹配质量差异 |
| `table_basin_status_by_distance.csv` | 距离分箱×状态统计 | 了解匹配距离的分布 |
| `table_basin_spatial_match_error_table.csv` | 逐行空间匹配详情（大表） | 最重要的诊断表，含 spatial_error_class/match_quality 等 |
| `table_basin_spatial_match_status_counts.csv` | 按状态汇总 | resolved 百分比越高越好 |
| `table_basin_spatial_match_error_class_counts.csv` | 空间错误分类汇总 | high_confidence 比例越高越好 |
| `table_basin_spatial_match_quality_counts.csv` | 匹配质量汇总 |
| `table_basin_spatial_match_distance_bins.csv` | 距离分箱×状态 |
| `table_basin_spatial_match_threshold_sensitivity.csv` | 阈值敏感性分析 | 用于选择合理的距离接受阈值 |
| `table_basin_unresolved_by_source.csv` | 按数据源的未解决统计 | 了解哪些源的数据匹配困难 |
| `table_basin_unresolved_by_country.csv` | 按国家的未解决统计 | 了解哪些地区匹配困难 |
| `table_basin_resolved_point_anomalies.csv` | 已解决但位置标记异常 | 需要核验的 resolved 记录 |
| `table_basin_manual_review_top_large_offsets.csv` | 大偏移队列（top 200） |
| `table_basin_manual_review_area_mismatch.csv` | 面积不匹配队列 |
| `table_basin_manual_review_geometry_inconsistent.csv` | 几何不一致队列 |
| `table_basin_manual_review_high_risk.csv` | 高风险队列 |
| `table_basin_reported_area_*.csv` | 有面积数据的行子集统计 | 了解有面积数据的记录匹配质量是否更好 |
| `table_basin_remote_sensing_exclusion_summary.csv` | 遥测排除说明 | 说明卫星验证记录不在此目录中 |
| `table_basin_unknown_stations.csv` | 状态/坐标不明的记录 | 需要人工调查 |

**根目录下文本报告（兼容旧版）**:
- `spatial_match_error_summary.txt` | `spatial_match_error_summary_essd.md`
- `remote_sensing_exclusion_summary.txt`

**图件（figures/）**:
- `basin_flag_counts.png` — 流域标记条形图
- `spatial_error_class_counts.png` — 空间错误分类条形图
- `distance_hist_logx.png` — 匹配距离直方图（log10 轴）
- `unknown_points_map.png` — 未解决点全球分布散点图
- `threshold_sensitivity.png` — 阈值敏感度曲线
- `basin_status_by_reported_area_presence.png` / `reported_area_presence_counts.png`

---

## 12. `qc_flags.py` — QC 标记统计

统计发布 NetCDF 产品中所有 QC 标记变量的分布情况，评估数据质量。

### 统计功能

#### (1) 标记计数（`flag_counts`）
- 对 master/climatology/satellite 三个产品的 NetCDF 扫描所有 `_flag` / `_qc` 变量
- 按 product × flag_variable × flag_value 统计出现次数和百分比
- 自动读取 NetCDF 的 `flag_values` / `flag_meanings` 属性获取标记值定义
- 缺省含义: 0=good, 1=estimated, 2=suspect, 3=bad, 8=not_checked, 9=missing

#### (2) 健康度指标（`health`）
- 按 product × flag_variable 聚合健康汇总：
  - `good_count` / `usable_count` (= good + estimated)
  - `suspect_count` / `bad_count` / `missing_count`
  - `problem_count` (= suspect + bad)
  - `good_percent` / `usable_percent` / `problem_percent`

#### (3) 标记模式定义（`flag_schema`）
- 列出每个产品中每个 flag_variable 的声明标记值及其含义（从 NetCDF 元数据读取）

#### (4) 旧版兼容表（`flag_summary` → `yearly_trends`）
- `flag_summary`: 平展标记计数表，含 qc_level (final/stage)、qc_stage (physical_plausibility/log_iqr/ssc_q_consistency)、temporal_resolution
- `flag_by_variable`: 按 variable 汇总
- `flag_by_source`: 所有源聚合（标注为 `all_release_sources`，此处不做源级拆分）
- `flag_by_resolution`: 按 temporal_resolution 汇总
- `health_kpis`: 详尽健康指标，含各种比例（good_rate/derived_rate/suspect_rate/bad_rate/missing_rate/usable_rate/problem_rate/issue_rate）
- `stage_effectiveness`: QC 各阶段有效性评估
- `issue_hotspots`: 问题最集中的 top 100 条目
- `yearly_trends`: 逐年问题率趋势

### 输出说明

**表格（tables/）**:

| 表名 | 内容 | 如何看 |
|------|------|--------|
| `table_qc_flag_counts.csv` | 所有标记值的详细计数和百分比 | 关注 suspect(2) 和 bad(3) 的比例 |
| `table_qc_health.csv` | 健康度汇总 | 核心指标——`good_percent` 越高越好，`problem_percent` 越低越好 |
| `table_qc_flag_schema.csv` | 标记值定义 |
| `table_qc_flag_summary.csv` | 平展标记计数+QC阶段 | 查看各 QC 阶段（物理合理性/log_iqr/SSC-Q一致性）的问题率 |
| `table_qc_health_kpis.csv` | 详尽健康指标 | 含多种比例的成品表 |
| `table_qc_flag_by_variable.csv` | 按变量汇总标记分布 |
| `table_qc_stage_effectiveness.csv` | QC 各阶段有效性 | 了解各 QC 阶段检出的问题占比 |
| `table_qc_issue_hotspots.csv` | Top 100 问题热点 | 需要优先处理的问题区域 |
| `table_qc_yearly_trends.csv` | 逐年问题率趋势 | 了解数据质量是否随时间改善或恶化 |
| 此外还有 `climatology/` 和 `satellite/` 子目录各自独立的 QC 表集 | | |

**图件（figures/）**:
- `fig_qc_flag_distribution.png` — 各产品标记分布堆叠柱状图
- `fig_qc_health.png` — 各标记变量的 good % vs problem % 对比
- `fig_qc_health_by_resolution.png`
- `fig_qc_flag_by_source_type.png`
- `fig_qc_yearly_problem_trends.png` — 逐年问题趋势
- `fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`
- `fig_qc_top_problem_sources.png` / `fig_qc_top_problem_clusters.png`
- 此外 `climatology/` 和 `satellite/` 子目录各自有完整图件集

---

## 13. `run_all_release_stats.py` — 总控入口

按顺序运行所有统计模块，生成可复现性清单的总控脚本。

### 功能

- 通过 `subprocess.run` 依次调用各模块
- 默认顺序：inventory → spatial → temporal → source_dataset_layers → source_contribution → basin_diagnostics → variable_summary → qc_flags
- 可通过 `--modules` 参数指定子集
- `--continue-on-error` 忽略某模块失败继续运行后续模块
- 默认清理各模块输出目录（`--no-clean-output` 跳过）
- 将公共参数（release-dir、dpi、skip-figures、copy-reports、allow-non-release-inputs）传递给各模块

### 输出说明

| 文件名 | 内容 | 如何看 |
|--------|------|--------|
| `run_summary.csv` | 各模块运行状态（退出码、起止时间） | 0=成功，非0=失败 |
| `run_summary.md` | 可读性摘要 | 快速文字概览 |
| `run_manifest.csv` | 输出文件清单（含 release 指纹和脚本指纹） | 用于回溯哪个 release 版本生成了哪些文件 |
| `run_manifest.json` | 完整 JSON 清单（同 CSV 信息 + 结构化） | 适合程序化处理 |
| `parity_manifest.csv` | 旧版兼容性清单 | 见 parity.py 说明 |

### 如何解读运行结果

1. 检查 `run_summary.csv` 中所有模块的 return_code 是否为 0
2. 确认 `run_manifest.csv` 覆盖了所有预期输出文件
3. `release_fingerprint` 可用于确认统计使用的是哪个版本的 release
4. `stats_script_fingerprint` 可用于追踪统计脚本的版本

---

## 数据流总览

```
产品文件 (output/sed_reference_release/)
│
├── station_catalog.csv ──────────────┬── spatial.py    （聚类表、空间分组、面积分布）
│                                     ├── temporal.py   （按分辨率时间跨度、活跃单元）
│                                     ├── basin_diagnostics.py （流域分配质量诊断）
│                                     ├── source_contribution.py （展开sources_used）
│                                     └── source_dataset_layers.py （展开sources_used）
│
├── source_station_catalog.csv ───────┬── source_contribution.py
│                                     ├── source_dataset_layers.py
│                                     └── temporal.py（气候态时间覆盖）
│
├── source_dataset_catalog.csv ───────┬── source_contribution.py（外连接补充元数据）
│                                     └── source_dataset_layers.py（外连接补充元数据）
│
├── satellite_catalog.csv ────────────┬── source_contribution.py
│                                     ├── source_dataset_layers.py
│                                     ├── spatial.py（卫星空间覆盖）
│                                     └── temporal.py（卫星时间覆盖）
│
├── master_nc ────────────────────────┬── temporal.py      （时间范围）
│                                     ├── variable_summary.py（变量覆盖率、统计量、共现）
│                                     ├── qc_flags.py      （标记分布）
│                                     └── inventory.py     （模式信息）
│
├── daily/monthly/annual.nc ──────────┬── temporal.py      （时间轴诊断、逐年扫描、记录长度）
│                                     └── inventory.py     （模式信息）
│
├── climatology_nc ───────────────────┬── variable_summary.py（变量覆盖率）
│                                     ├── qc_flags.py      （标记分布）
│                                     └── inventory.py     （模式信息）
│
├── satellite_nc ─────────────────────┬── variable_summary.py（变量覆盖率+按源拆分）
│                                     ├── qc_flags.py      （标记分布）
│                                     └── inventory.py     （模式信息）
│
├── GeoPackage 产品 ──────────────────┬── spatial.py       （图层要素数）
│                                     └── inventory.py     （图层信息）
│
├── release_inventory.csv ────────────┬── inventory.py     （清单对比）
├── release_validation_report.csv ────┬── inventory.py     （矛盾检查）
│
└── 其他 sidecar（overlap/satellite candidates）── source_dataset_layers.py（层隶属）
```

## 输出目录结构示例

```
output_other/stats_release/
├── run_summary.csv
├── run_summary.md
├── run_manifest.csv
├── run_manifest.json
├── parity_manifest.csv
├── inventory/
│   ├── tables/  (16+ 个 CSV)
│   └── reports/ (Markdown 报告)
├── spatial/
│   ├── tables/  (20+ 个 CSV)
│   ├── figures/ (15+ 个 PNG/PDF + 1 GeoJSON)
│   └── reports/
├── temporal/
│   ├── tables/  (20+ 个 CSV)
│   ├── figures/ (30+ 个 PNG/PDF)
│   └── reports/
├── variable_summary/
│   ├── tables/  (12+ 个 CSV)
│   ├── figures/ (3 个变量分布图 PNG)
│   └── reports/
├── source_contribution/
│   ├── tables/  (14+ 个 CSV)
│   ├── figures/ (30+ 个 PNG)
│   └── reports/
├── source_dataset_layers/
│   ├── tables/  (4 个 CSV)
│   └── reports/
├── basin_diagnostics/
│   ├── tables/  (30+ 个 CSV)
│   ├── figures/ (8 个 PNG)
│   ├── reports/
│   └── *.txt / *.md （根目录下文本报告）
└── qc_flags/
    ├── tables/  (12+ 个 CSV + climatology/ + satellite/ 子目录)
    ├── figures/ (10+ 个 PNG + climatology/ + satellite/ 子目录)
    └── reports/
```
