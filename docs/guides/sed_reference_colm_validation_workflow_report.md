# 使用 sed_reference_release 验证 CoLM 泥沙输出的工作流程报告

本文根据 `validate_model_with_sed_reference.py` 的实际实现，归纳如何使用 `sed_reference_release` 发布级别泥沙参考数据，对 CoLM `unitcat` 历史输出进行站点尺度验证。该流程的核心思想是：从发布包中筛选可用参考站点，将站点匹配到最近的 CoLM 网格单元，提取同一时间窗口内的参考序列和模型序列，并计算逐站点、逐变量的统计评价指标。

## 1. 验证目标

该脚本用于评估 CoLM 泥沙模拟结果与发布级参考观测数据之间的一致性，当前支持三个变量：

| 变量 | 参考数据变量名 | CoLM 默认变量 | 统一单位 | 评价对象 |
| --- | --- | --- | --- | --- |
| Q | `Q` | `f_discharge` | `m3 s-1` | 河流流量 |
| SSC | `SSC` | `f_sedcon_1` + `f_sedcon_2` + `f_sedcon_3` | `mg L-1` | 悬浮泥沙浓度 |
| SSL | `SSL` | `f_sedout_1` + `f_sedout_2` + `f_sedout_3` | `ton day-1` | 悬浮泥沙输沙率 |

其中 SSC 和 SSL 在 CoLM `unitcat` 文件中按粒级分量存储，脚本会先对三个粒级分量求和，再乘以单位转换因子。

## 2. 输入数据

### 2.1 CoLM 模型输出

脚本默认读取以下目录中的 CoLM 历史输出：

```text
/share/home/dq134/wzx/CoLM/cases/sed_test_tune2/history
```

匹配文件模式为：

```text
*_hist_unitcat_*.nc
```

这些文件被作为 HDF5/NetCDF 文件读取，脚本使用：

| 字段 | 默认名称 | 用途 |
| --- | --- | --- |
| 时间 | `time` | 单位为 minutes since 1900-01-01 |
| 纬度 | `lat_ucat` | 模型 unitcat 纬度索引 |
| 经度 | `lon_ucat` | 模型 unitcat 经度索引 |
| 流量 | `f_discharge` | Q |
| 泥沙浓度分量 | `f_sedcon_1/2/3` | SSC |
| 输沙率分量 | `f_sedout_1/2/3` | SSL |

### 2.2 sed_reference_release 发布数据

脚本默认参考数据目录为：

```text
/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release
```

验证流程主要使用：

| 文件 | 用途 |
| --- | --- |
| `station_catalog.csv` | 按 `cluster_uid + resolution` 组织的参考站点目录，提供坐标、站名、河流名、记录数和时间范围 |
| `sed_reference_timeseries_daily.nc` | 日尺度 `station x time` 参考矩阵 |
| `sed_reference_timeseries_monthly.nc` | 月尺度 `station x time` 参考矩阵 |
| `sed_reference_timeseries_annual.nc` | 年尺度 `station x time` 参考矩阵 |

当前脚本默认使用日尺度参考数据：

```text
DEFAULT_RESOLUTION = "daily"
```

因此默认矩阵文件为：

```text
sed_reference_timeseries_daily.nc
```

矩阵文件中每个变量均带有质量标记，例如 `Q_flag`、`SSC_flag`、`SSL_flag`。脚本默认保留质量标记 `0,1,2`，剔除其他标记或缺测值。

## 3. 当前默认验证配置

脚本所有配置都写在文件顶部的 `DEFAULT_*` 常量中，运行前需要直接编辑这些默认值。

| 配置项 | 当前默认值 | 含义 |
| --- | --- | --- |
| `DEFAULT_RESOLUTION` | `daily` | 参考数据时间分辨率 |
| `DEFAULT_ALLOWED_FLAGS` | `0,1,2` | 保留的参考数据质量标记 |
| `DEFAULT_MIN_REFERENCE_POINTS` | `10` | 单变量最少有效参考点数 |
| `DEFAULT_MIN_PAIRED_POINTS` | `10` | 单指标最少模型-参考配对点数 |
| `DEFAULT_MAX_GRID_DISTANCE_KM` | `50.0` | 站点到最近模型网格的最大允许距离 |
| `DEFAULT_START_DATE` | `1995-01-01` | 用户指定验证起始日期 |
| `DEFAULT_END_DATE` | `1999-09-30` | 用户指定验证结束日期 |
| `DEFAULT_REGION_LAT_MIN/MAX` | `-20 / 5` | 默认区域纬度范围 |
| `DEFAULT_REGION_LON_MIN/MAX` | `-80 / -45` | 默认区域经度范围 |
| `DEFAULT_MAX_STATIONS` | `0` | 不限制站点数量 |
| `DEFAULT_MAKE_PLOTS` | `True` | 输出逐站时间序列对比图 |
| `DEFAULT_NUM_WORKERS` | `8` | 并行验证进程数 |

注意：`DEFAULT_OUTPUT_DIR = "../output_other/validate_model_with_sed_reference"` 是相对路径，最终输出位置取决于运行脚本时的当前工作目录。

## 4. 总体工作流程

完整验证流程可以概括为以下七个阶段。

### 4.1 发现模型文件并确定模型时空范围

脚本首先在 CoLM 输出目录中查找所有满足 `*_hist_unitcat_*.nc` 的文件，并按文件名排序。随后：

1. 从第一个文件读取 `lat_ucat` 和 `lon_ucat`。
2. 扫描所有模型文件中的 `time`。
3. 将模型时间从 minutes since 1900-01-01 转换为 `pandas.Timestamp`。
4. 得到模型输出的全局起止时间。
5. 将模型时间范围与用户指定时间窗口求交集，生成实际验证窗口 `valid_start` 到 `valid_end`。

如果用户设定的时间窗口与模型输出没有交集，脚本会直接报错退出。

### 4.2 读取参考站点目录

脚本读取 `station_catalog.csv`，并筛选与当前 `resolution` 一致的站点记录。随后将：

- `lat`、`lon` 转为数值；
- `record_count` 转为数值；
- `time_start`、`time_end` 转为时间戳；
- 保留 `cluster_uid`、`source_station_id`、`station_name`、`river_name` 等元信息。

`cluster_uid` 是后续从矩阵 NetCDF 中定位站点行号的关键字段。

### 4.3 候选站点筛选

每个参考站点会依次经过以下筛选：

| 筛选步骤 | 通过条件 | 未通过原因 |
| --- | --- | --- |
| 坐标有效性 | `lat/lon` 为有限数值 | `invalid_lat_lon` |
| 模型域范围 | 站点位于模型经纬度边界框内 | `outside_model_domain` |
| 用户区域范围 | 站点位于默认或用户设定区域内 | `outside_user_region` |
| 时间重叠 | 站点记录时间与验证窗口有交集 | `no_time_overlap` |
| 最近网格距离 | 站点到最近模型网格距离不超过阈值 | `model_grid_distance_gt_threshold` |
| 最大站点数 | 未超过 `DEFAULT_MAX_STATIONS` 限制 | `not_processed_max_stations` |

当前默认区域为南美亚马逊附近：

```text
lat: -20 to 5
lon: -80 to -45
```

因此全球参考站点目录中很多站点会因 `outside_user_region` 被跳过。

### 4.4 站点到 CoLM 网格匹配

对于通过筛选的参考站点，脚本使用最近邻方法匹配 CoLM `unitcat` 网格：

1. 根据模型经度范围判断是否需要将站点经度转换到 `0-360` 或 `-180-180`。
2. 分别寻找与站点纬度、经度最接近的 `lat_ucat` 和 `lon_ucat` 索引。
3. 使用 haversine 公式计算站点到网格中心的球面距离。
4. 若距离大于 `DEFAULT_MAX_GRID_DISTANCE_KM`，该站点不进入验证。

匹配结果会写入每个站点目录下的 `station_match.csv`，其中包括站点坐标、模型网格坐标、网格索引和距离。

### 4.5 批量预读取 CoLM 模型序列

为了避免“每个站点、每个变量、每个文件”重复打开模型文件，脚本对所有候选站点做一次批量预读取：

1. 收集所有候选站点对应的 `lat_ucat/lon_ucat` 索引。
2. 逐个打开模型文件。
3. 对每个变量提取所有候选站点的时间序列。
4. SSC 与 SSL 对三个粒级分量求和。
5. 将模型值乘以单位转换因子：

| 变量 | 转换因子 | 转换后单位 |
| --- | --- | --- |
| Q | `1.0` | `m3 s-1` |
| SSC | `2650000.0` | `mg L-1` |
| SSL | `228960.0` | `ton day-1` |

6. 按实际验证窗口裁剪模型时间序列。
7. 将结果缓存为字典：`(lat_idx, lon_idx) -> variable -> DataFrame(time, model_value)`。

脚本还会在输出目录下创建 `.model_cache/`，用模型文件路径、修改时间、变量列表和验证窗口生成哈希键。如果输入没有变化，下次运行可直接复用缓存。

### 4.6 提取参考序列并配对

每个候选站点在并行进程中独立验证。步骤如下：

1. 打开当前分辨率对应的参考矩阵 NetCDF。
2. 读取矩阵中的 `cluster_uid` 列表。
3. 找到当前站点 `cluster_uid` 对应的行号。
4. 读取 `time` 并裁剪到验证窗口。
5. 对 Q、SSC、SSL 分别读取参考值和质量标记。
6. 仅保留质量标记在 `allowed_flags` 中且数值有限的记录。
7. 如果矩阵中存在 `is_overlap`、`selected_source_index`、`selected_source_station_uid`，也一并输出到参考序列表。

参考序列输出为：

```text
<station_dir>/reference_timeseries.csv
```

模型序列输出为：

```text
<station_dir>/model_timeseries.csv
```

随后脚本按当前分辨率对时间进行标准化：

| 分辨率 | 时间标准化方式 |
| --- | --- |
| daily | floor 到日期 |
| monthly | 转为月份起始日期 |
| annual | 转为年份起始日期 |

参考序列和模型序列分别按标准化时间求平均，然后按时间做 inner join，得到有效配对样本。

### 4.7 计算验证指标和绘图

每个站点、每个变量均独立计算指标。若参考有效点数少于 `DEFAULT_MIN_REFERENCE_POINTS`，则跳过；若模型-参考配对点数少于 `DEFAULT_MIN_PAIRED_POINTS`，也跳过。

成功验证的变量会计算：

| 指标 | 公式含义 | 解释 |
| --- | --- | --- |
| `n` | 有效配对样本数 | 越多越稳健 |
| `pearson_r` | Pearson 相关系数 | 表征时间变化同步性 |
| `rmse` | 均方根误差 | 表征绝对误差大小，单位与变量一致 |
| `bias` | `mean(model - reference)` | 正值表示模型偏高，负值表示模型偏低 |
| `nse` | Nash-Sutcliffe efficiency | 越接近 1 越好，小于 0 表示劣于观测均值基准 |

若 `DEFAULT_MAKE_PLOTS=True`，脚本会为每个站点-变量输出时间序列对比图：

```text
compare_Q_daily.png
compare_SSC_daily.png
compare_SSL_daily.png
```

图中红色为参考数据，蓝色为模型输出。

## 5. 输出结果结构

脚本输出包括总表、空间图和逐站点目录。

```text
<output_dir>/
├── candidate_station_catalog.csv
├── variable_status_summary.csv
├── metrics_summary.csv
├── model_domain_overview.png
├── model_domain_overview_region.png
├── model_spatial_mean.png
├── .model_cache/
└── <cluster_uid>_<station_name>/
    ├── station_match.csv
    ├── reference_timeseries.csv
    ├── model_timeseries.csv
    ├── compare_Q_daily.csv
    ├── compare_SSC_daily.csv
    ├── compare_SSL_daily.csv
    ├── compare_Q_daily.png
    ├── compare_SSC_daily.png
    └── compare_SSL_daily.png
```

### 5.1 `candidate_station_catalog.csv`

该文件记录所有参考站点的筛选状态。它适合回答：

- 哪些站点进入了验证；
- 哪些站点被跳过；
- 被跳过的原因是什么；
- 站点匹配到的模型网格位置和距离是多少；
- 每个变量最终是否完成验证。

### 5.2 `variable_status_summary.csv`

该文件以“站点-变量”为单位记录处理状态。常见状态包括：

| status | reason | 含义 |
| --- | --- | --- |
| `validated` | 空 | 完成验证并输出指标 |
| `skipped` | `cluster_uid_not_in_matrix` | 站点不在参考矩阵中 |
| `skipped` | `var_not_in_preload_cache` | 模型缓存缺少该变量 |
| `skipped` | `insufficient_reference_points` | 参考有效点数不足 |
| `skipped` | `insufficient_paired_points` | 模型-参考配对点数不足 |

### 5.3 `metrics_summary.csv`

这是最核心的评价结果表。每行对应一个站点-变量组合，包含：

| 字段 | 含义 |
| --- | --- |
| `cluster_uid` | 参考站点簇 ID |
| `station_name` | 站点名称 |
| `variable` | 变量名和单位简写 |
| `unit` | 变量单位 |
| `n` | 有效配对点数 |
| `pearson_r` | 相关系数 |
| `rmse` | 均方根误差 |
| `bias` | 平均偏差 |
| `nse` | Nash-Sutcliffe 效率系数 |
| `model_grid_distance_km` | 站点到模型网格距离 |
| `reference_points` | 该变量参考有效点数 |

### 5.4 空间诊断图

脚本输出三类空间图：

| 文件 | 内容 |
| --- | --- |
| `model_domain_overview.png` | 全球范围模型网格、模型边界框和所有参考站点筛选状态 |
| `model_domain_overview_region.png` | 用户区域范围内的站点、模型网格和 MERIT Hydro 河网背景 |
| `model_spatial_mean.png` | Q、SSC、SSL 多年平均空间分布 |

这些图主要用于检查站点筛选是否合理、模型域是否覆盖目标区域、参考站点是否落在主河网附近，以及模型变量是否存在异常空间分布。

## 6. 运行方式

脚本没有命令行参数，运行前需要直接编辑 `DEFAULT_*` 常量。推荐从脚本所在目录运行，以便相对输出目录更容易解释：

```bash
cd /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/validate
/share/home/dq134/.conda/envs/wzx/bin/python3 validate_model_with_sed_reference.py
```

如果从其他目录运行，`DEFAULT_OUTPUT_DIR` 的相对路径会相对于当前工作目录解析。

## 7. 结果解读建议

建议按以下顺序检查结果：

1. 先看 `candidate_station_catalog.csv`，确认进入验证的站点数、区域筛选和网格距离是否合理。
2. 再看 `variable_status_summary.csv`，确认 Q、SSC、SSL 分别有多少站点完成验证，是否存在大量样本不足。
3. 重点分析 `metrics_summary.csv` 中的 `pearson_r`、`bias`、`rmse` 和 `nse`。
4. 对表现异常的站点，进入对应站点目录查看 `station_match.csv` 和 `compare_*.png`。
5. 结合 `model_domain_overview_region.png` 判断站点是否可能被匹配到非目标河道或过远网格。
6. 结合 `model_spatial_mean.png` 判断模型变量是否存在空间分布异常或单位转换异常。

对于泥沙模拟，SSC 和 SSL 往往比 Q 更敏感。若 Q 表现较好但 SSC/SSL 表现较差，可能说明水文过程基本可接受，但泥沙侵蚀、输移、粒级参数或单位转换仍需进一步检查。

## 8. 需要注意的问题

1. 当前脚本采用最近经纬度网格匹配，没有进行河网拓扑或上游面积约束匹配。对于大河干流附近的密集河网，最近网格不一定代表同一水文断面。
2. 默认 `allowed_flags=0,1,2` 会保留 suspect 数据。如果需要更严格的验证，可改为 `0,1` 或仅 `0`。
3. 当前默认时间窗口为 1995-01-01 至 1999-09-30；若模型或参考数据时间范围变化，需要同步修改。
4. 月尺度和年尺度验证需要将 `DEFAULT_RESOLUTION` 改为 `monthly` 或 `annual`，脚本会自动选择对应矩阵文件，但模型输出仍会按该分辨率聚合求平均。
5. SSC 和 SSL 的单位转换因子直接决定结果量级，若 CoLM 输出变量定义发生变化，需要重新核对 `DEFAULT_MODEL_SSC_FACTOR` 和 `DEFAULT_MODEL_SSL_FACTOR`。
6. 输出目录中的 `.model_cache/` 会复用模型预读取结果；若怀疑缓存与输入不一致，可删除该目录后重新运行。
7. 参考矩阵中的 `selected_source_station_uid` 可用于追踪每个站点-时间单元的参考值来源；如需完整记录级 provenance，应进一步查询 `sed_reference_master.nc` 或 `source_station_catalog.csv`。

## 9. 小结

该脚本将 `sed_reference_release` 的发布级参考数据转化为 CoLM 模型验证基准，形成了从站点目录筛选、矩阵型参考序列提取、模型网格匹配、单位统一、时间配对、指标计算到图表输出的一套完整流程。它的优势是输入数据结构清晰、批量预读取效率较高、输出结果可追溯；主要限制是空间匹配仍基于最近网格，后续若用于正式论文分析，建议进一步加入河网连通性、上游面积或流域 ID 约束，以提高站点-模型断面匹配的水文一致性。
