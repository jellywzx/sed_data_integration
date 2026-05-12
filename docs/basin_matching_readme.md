# Basin Matching Technical Roadmap

## 1. 总体目标

更新后的流域匹配主线以“全球一致、可复现、保守发布”为核心原则，目标不是把所有点都强行匹配到一个 basin，而是尽可能稳定地输出一套可解释的 basin assignment 结果。

这条技术路线将整个问题拆成两个层次：

1. `basin_tracer.py` 负责几何匹配和证据生成
2. `basin_policy.py` 负责发布级判定

最终发布层只区分：

1. `resolved`
2. `unresolved`

其中：

1. `resolved` 站点允许发布 basin polygon
2. `unresolved` 站点保留观测记录，但不强行发布 basin polygon

---

## 2. 输入准备阶段

### 2.1 输入来源

流域匹配主线的输入来自 `s3_collected_stations.csv`，其中已经整理好：

1. 站点经纬度
2. 时间分辨率
3. 站名与河名
4. 原始站点编号
5. 可选的 `reported_area`

补充规则：

1. `reported_area` 只用于源产品本身明确提供可比汇水面积的情况
2. RiverSed 在 basin 主线下不再使用其源产品中的 `upstream_area`
3. RiverSed 也不再把 NHDPlus reach/basin 元数据带入 basin tracing 输入表

### 2.2 时间分辨率约束

进入 basin 主线的仅包括：

1. `daily`
2. `monthly`
3. `annual`

`climatology` 不进入 basin tracing，而是走独立导出流程。

### 2.3 本阶段目标

1. 固定站点顺序
2. 固定 `station_id`
3. 固定元数据来源
4. 为后续 tracing 和 merge 提供稳定输入表

---

## 3. 站点级流域匹配阶段

### 3.1 匹配河段

对每个站点，首先在 MERIT 河网中寻找最合适的匹配河段 `COMID`。

输出的基础匹配证据包括：

1. `basin_id`
2. `distance_m`
3. `match_quality`
4. `uparea_merit`
5. `area_error`
6. `pfaf_code`

### 3.2 构建局地汇水区和完整流域

匹配到河段后，生成两类几何对象：

1. `geometry_local`
   只包含匹配河段自身对应的最小局地汇水区
2. `geometry`
   沿上游网络追溯全部上游 `COMID` 后合并得到的完整流域

### 3.3 点是否在 polygon 内的判定

当前实现中，点面关系由 `basin_tracer.py` 直接计算。

计算步骤如下：

1. 用原始站点坐标构造 `Point(lon, lat)`
2. 若 `geometry_local` 存在且非空，则计算：
   `point_in_local = geometry_local.covers(point)`
3. 若完整上游 `geometry` 存在、非空，且当前方法为 `upstream_traced`，则计算：
   `point_in_basin = geometry.covers(point)`

这里使用 `covers()` 而不是 `contains()`，原因是：

1. `covers()` 会把落在 polygon 边界上的点也视为“在面内”
2. 对岸边点、边界贴线点更稳健
3. 可以减少由坐标精度带来的伪 outside

### 3.4 当前不做的事情

在这一步，当前主线明确不做：

1. snapping
2. 修改原始站点坐标
3. 用 buffer 后再重新判断 inside / outside

### 3.5 fallback 规则

如果无法成功构建真实的完整上游 polygon，可退回 `area_buffer_fallback` 作为兜底几何。

但这里有一个重要约束：

1. fallback buffer 不作为真实 basin 几何证据
2. `point_in_basin` 只在真实的 `upstream_traced` polygon 存在时才可为真

---

## 4. 发布级判定阶段

### 4.1 角色分工

`basin_policy.py` 不做几何运算，只读取上一步生成的证据，并把它们转换成发布级标签：

1. `basin_status`
2. `basin_flag`

### 4.2 输入证据

进入 `basin_policy.py` 的核心字段包括：

1. `basin_id`
2. `match_quality`
3. `distance_m`
4. `source_name`
5. `point_in_local`
6. `point_in_basin`

### 4.3 判定结果

输出的发布级字段为：

1. `basin_status = resolved | unresolved`
2. `basin_flag = ok | large_offset | area_mismatch | geometry_inconsistent | no_match`

### 4.4 当前判定顺序

判定顺序是有优先级的，前面命中的分支会覆盖后面：

1. `source in {RiverSed, GSED, Dethier}` 时不发布 MERIT basin assignment，返回 `unresolved / no_match`
2. `basin_id` 缺失或 `match_quality=failed` 时返回 `unresolved / no_match`
3. `match_quality=area_mismatch` 时返回 `unresolved / area_mismatch`
4. `distance_m <= 300` 时返回 `resolved / ok`
5. `distance_m <= 1000` 且 `area_matched / area_approximate` 时返回 `resolved / ok`
6. `distance_m <= 1000` 且 `point_in_local=True` 时返回 `resolved / ok`
7. 普通 matched stations 若 `distance_m > 1000`，则返回 `unresolved / large_offset`
8. 其余未被接受的情况返回 `unresolved / geometry_inconsistent`

### 4.5 当前已删除的规则

当前主线已经删除：

1. 基于 `station_name / river_name` 关键词的 `coastal_complex` 自动判定
2. 名称字段中的 `estuary / delta / tidal / coastal` 等关键词，不再影响自动结果

---

## 5. cluster 合并阶段

### 5.1 合并目标

`s5_basin_merge.py` 负责把站点级 basin 匹配结果传播到 `cluster` 层。

### 5.2 合并原则

1. `resolved` 且有有效 `basin_id` 的站点，才允许作为正式 basin assignment 进入发布主线
2. `unresolved` 站点仍保留在主数据表中
3. `unresolved` 站点不作为正式 basin polygon 的发布依据

### 5.3 本阶段目标

1. 不丢观测
2. 不把不稳定 basin assignment 传播到正式发布结果

---

## 6. NetCDF 与 catalog 传播阶段

### 6.1 主 NetCDF

`s6_basin_merge_to_nc.py` 会将站点级诊断字段写入主 `master nc`。

必须稳定保留的字段包括：

1. `basin_match_quality`
2. `basin_status`
3. `basin_flag`
4. `basin_distance_m`
5. `point_in_local`
6. `point_in_basin`

### 6.2 catalog 导出

`s7_cluster_station_catalog.csv` 和 `s7_cluster_resolution_catalog.csv` 读取这些字段并继续传播。

这里有一个关键约束：

1. `s7` 里的 `basin_status` 等字段来自 `s6_basin_merged_all.nc`
2. 如果只更新了 `s5`，但没有重跑 `s6`，那么 `s7` 中这些字段可能为空

---

## 7. polygon 发布阶段

### 7.1 点位产品

点位 catalog 保留全部 cluster 和站点结果。

### 7.2 polygon 产品

正式 basin polygon sidecar 只保留：

1. `basin_status = resolved`

这意味着：

1. `resolved` 站点可以进入 basin polygon 产品
2. `unresolved` 站点即使保留在主表中，也不会进入 basin polygon sidecar

### 7.3 设计意图

这样设计的核心目的不是减少数据量，而是：

1. 防止错误 polygon 进入正式发布版
2. 让“保留观测”和“发布 basin”这两件事解耦

---

## 8. 人工审核与质控阶段

### 8.1 审核重点

自动审核和人工抽样应重点关注：

1. `large_offset`
2. `area_mismatch`
3. `geometry_inconsistent`
4. `no_match`
5. `unresolved`

### 8.2 推荐抽样分组

建议至少按距离分三组抽样：

1. `<= 300 m`
2. `300-1000 m`
3. `> 1000 m`

### 8.3 审核目标

审核的目标不是把所有 `unresolved` 强行改成 `resolved`，而是确认：

1. 自动规则没有系统性偏差
2. 发布版 polygon 不会批量引入错误 basin

---

## 9. 单元/函数级测试

1. `basin_id` 缺失或 `match_quality=failed` 时仍返回 `unresolved / no_match`
2. `match_quality=area_mismatch` 时仍返回 `unresolved / area_mismatch`
3. `distance_m <= 300` 时返回 `resolved / ok`
4. `distance_m <= 1000` 且 `area_matched / area_approximate` 时返回 `resolved / ok`
5. `distance_m <= 1000` 且 `point_in_local=True` 时返回 `resolved / ok`
6. `RiverSed / GSED / Dethier` 等 source 返回 `unresolved / no_match`
7. 普通 matched stations 若 `distance_m > 1000`，则返回 `unresolved / large_offset`
8. 名称字段中的 `estuary / delta / tidal / coastal` 等关键词，不再影响判定结果

---

## 10. 最终发布原则

1. 主数据集优先保证全球一致、可复现、可解释
2. 不再追求“每个站点都必须分到一个 basin polygon”
3. 正式发布时，保留观测优先于强行分配 basin
4. basin polygon 是附属空间产品，而不是每个站点都必须具备的强制字段
