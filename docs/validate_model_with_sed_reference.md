# validate_model_with_sed_reference.py

## 概述

该脚本用于将**网格化模型 NetCDF 输出**（Q / SSC / SSL）与 **sed_reference_release 基准数据集**进行站点级自动验证。它从 `station_catalog.csv` 中自动发现可用的参考站点，从参考矩阵中提取 Q/SSC/SSL 时间序列，将每个参考站点匹配到最近的模型网格单元，计算逐站逐变量的验证指标，并输出结构化的 CSV 结果（可选绘图）。

## 依赖

- Python ≥ 3.8
- `numpy`, `pandas`, `xarray`, `netCDF4`
- 绘图可选：`matplotlib`

## 命令行参数

### 必需参数

| 参数 | 说明 |
|------|------|
| `--model-nc` | 模型 NetCDF 文件路径 |
| `--reference-dir` | sed_reference_release 目录路径 |
| `--resolution` | 参考时间分辨率，可选：`daily` / `monthly` / `annual` |
| `--output-dir` | 验证结果输出目录 |

### 模型 NetCDF 坐标与变量名

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--model-time-name` | `time` | 模型时间坐标名 |
| `--model-lat-name` | `lat` | 模型纬度变量/坐标名 |
| `--model-lon-name` | `lon` | 模型经度变量/坐标名 |
| `--model-q-var` | `""` | 模型径流（Q）变量名 |
| `--model-ssc-var` | `""` | 模型悬沙浓度（SSC）变量名 |
| `--model-ssl-var` | `""` | 模型悬沙输沙率（SSL）变量名 |

> 至少需提供 `--model-q-var`、`--model-ssc-var`、`--model-ssl-var` 中的一个。

### 单位转换因子

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--model-q-factor` | `1.0` | 将模型 Q 转换为 m³/s 的乘数 |
| `--model-ssc-factor` | `1.0` | 将模型 SSC 转换为 mg/L 的乘数 |
| `--model-ssl-factor` | `1.0` | 将模型 SSL 转换为 ton/day 的乘数 |

### 筛选与控制参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--allowed-flags` | `0,1,2` | 保留的参考数据质量标记（逗号分隔） |
| `--min-reference-points` | `10` | 每变量最少有效参考数据点数 |
| `--min-paired-points` | `10` | 每指标最少模型-参考匹配数据点数 |
| `--max-grid-distance-km` | `50.0` | 站点到模型网格单元最大距离（km） |
| `--start-date` | `""` | 可选验证起始日期 |
| `--end-date` | `""` | 可选验证截止日期 |
| `--max-stations` | `0` | 可选最大处理站数（0 表示不限制） |
| `--make-plots` | `False` | 是否输出逐对比的 PNG 对比图 |

## 工作流程

```
station_catalog.csv ──► 筛选（分辨率、经纬度、时间范围、网格距离）──► 候选站点
                                                                 │
参考矩阵 NetCDF ──► 提取 Q/SSC/SSL ──► 时间匹配 ──► 逐变量对比 ──► 验证指标
                                                                 │
模型 NetCDF ──────► 提取最近网格单元变量 ──► 单位转换 ──────────►
```

### 1. 加载模型数据
- 通过 `xarray` 打开模型 NetCDF
- 获取时间范围，应用 `--start-date` / `--end-date` 窗口裁剪
- 解析经纬度坐标，计算模型域边界（lon 自动映射到 [-180, 180]）

### 2. 加载站点目录（`station_catalog.csv`）
- 读取 `station_catalog.csv`，按 `--resolution` 筛选
- 对经纬度、时间字段做类型清洗
- 根据以下条件剔除站点：
  - 经纬度无效
  - 站点落在模型域之外（`station_in_bbox`）
  - 时间与验证窗口无重叠（`time_overlap`）

### 3. 匹配最近网格单元
- 通过 `haversine_km` 计算大圆距离
- 支持 1D 经纬度（结构化网格）和 2D 经纬度（非结构化网格）
- 剔除距离 > `--max-grid-distance-km` 的站点

### 4. 提取参考数据
- 打开 `sed_reference_timeseries_{daily|monthly|annual}.nc`
- 按 `cluster_uid` 索引到矩阵行
- 提取 Q / SSC / SSL 及其 flag，按 `--allowed-flags` 过滤质量

### 5. 提取模型数据
- 从模型 NetCDF 取出对应网格单元（或面元求和）的时序
- 乘以对应的 `factor` 转换为标准单位

### 6. 聚合与配对
- 模型与参考数据按时间分辨率聚合（daily 按日、monthly 按月、annual 按年）
- 以 `time` 为键做内连接，形成配对数据

### 7. 计算验证指标
对每个站点-变量对（配对点 ≥ `--min-paired-points`）：

| 指标 | 名称 | 说明 |
|------|------|------|
| **n** | 配对点数 | 有效观测-模拟配对数量 |
| **Pearson r** | 皮尔逊相关系数 | 线性相关程度（-1 ~ 1） |
| **RMSE** | 均方根误差 | 均方根误差（单位同变量） |
| **Bias** | 偏差 | 模拟 - 观测的均值（Systematic bias） |
| **NSE** | Nash-Sutcliffe 效率系数 | 模型拟合优度（-∞ ~ 1，越接近 1 越好） |

### 8. 输出文件

输出到 `--output-dir`，包括：

| 文件 | 内容 |
|------|------|
| `candidate_station_catalog.csv` | 所有候选站点的筛选状态与匹配信息 |
| `variable_status_summary.csv` | 逐站点-变量的处理状态（validated / skipped） |
| `metrics_summary.csv` | 所有成功验证的站点-变量的指标汇总 |

**每个站点独立子目录**（`{cluster_uid}_{station_name}/`）：

| 文件 | 内容 |
|------|------|
| `station_match.csv` | 站点与其匹配网格单元的基本信息 |
| `reference_timeseries.csv` | 提取的参考时间序列 |
| `model_timeseries.csv` | 提取的模型时间序列 |
| `compare_{Q\|SSC\|SSL}_{resolution}.csv` | 配对后的模型-参考对比表 |
| `compare_{Q\|SSC\|SSL}_{resolution}.png` | 对比曲线图（仅在 `--make-plots` 时输出） |

## 使用示例

```bash
# 基础用法：验证径流
python validate_model_with_sed_reference.py \
    --model-nc output.nc \
    --reference-dir /path/to/sed_reference_release \
    --resolution daily \
    --output-dir ./validation_results \
    --model-q-var discharge

# 验证三个变量，带单位转换
python validate_model_with_sed_reference.py \
    --model-nc output.nc \
    --reference-dir /path/to/sed_reference_release \
    --resolution monthly \
    --output-dir ./validation_results \
    --model-q-var Q \
    --model-q-factor 1.0 \
    --model-ssc-var SSC \
    --model-ssc-factor 0.001 \
    --model-ssl-var SSL \
    --model-ssl-factor 1.0 \
    --allowed-flags 0,1 \
    --min-paired-points 20 \
    --start-date 2000-01-01 \
    --end-date 2010-12-31 \
    --max-grid-distance-km 25.0 \
    --make-plots
```

## 关键函数说明

| 函数 | 作用 |
|------|------|
| `haversine_km` | 计算两点之间的大圆距离（km） |
| `to_lon180` / `maybe_to_model_lon` | 经度归一化到 [-180, 180] 或匹配模型经度范围 |
| `find_nearest_model_cell` | 支持 1D/2D 网格的最近网格单元搜索 |
| `load_station_catalog` | 读取并按分辨率筛选站点目录 |
| `extract_reference_for_station` | 从参考矩阵中提取单个站点的 Q/SSC/SSL + flag |
| `extract_model_series` | 从模型 NetCDF 中提取单个变量的时序 |
| `compute_metrics` | 计算 Pearson r、RMSE、Bias、NSE |
| `aggregate_timeseries` | 将时序按分辨率聚合（均值），用于分辨率匹配 |
| `nse` | Nash-Sutcliffe 效率系数计算 |

## 输出表格列说明

### `metrics_summary.csv` 主要列

| 列名 | 含义 |
|------|------|
| `cluster_uid` | 站点簇唯一标识 |
| `source_station_id` | 原始测站编号 |
| `station_name` | 站点名称 |
| `river_name` | 河流名称 |
| `variable` | 变量标识（`Q_m3_s-1` / `SSC_mg_L` / `SSL_t_day`） |
| `unit` | 单位 |
| `n` | 配对数 |
| `pearson_r` | 皮尔逊相关系数 |
| `rmse` | 均方根误差 |
| `bias` | 平均偏差 |
| `nse` | Nash-Sutcliffe 效率系数 |
| `model_grid_distance_km` | 站点到匹配网格单元的距离 |
| `reference_points` | 该站该变量的有效参考数据点数 |
