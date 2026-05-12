# Basin Matching Flowchart

下面给出更新后流域匹配流程的流程图版本。该图对应当前保守发布主线：几何证据由 `basin_tracer.py` 生成，发布级判定由 `basin_policy.py` 负责，最终只有 `resolved` 对象进入 basin polygon 产品。`RiverSed / GSED / Dethier` 等 reach-scale remote-sensing products 保留 observation，但不发布 MERIT basin assignment。

```mermaid
flowchart TD
    A[输入站点表<br/>s3_collected_stations.csv] --> B[筛选进入 basin 主线的站点<br/>daily / monthly / annual]
    B --> C[为每个站点匹配 MERIT 河段 COMID]
    C --> D[输出基础匹配证据<br/>basin_id / distance_m / match_quality / uparea_merit / area_error / pfaf_code]
    D --> E[构建 geometry_local<br/>匹配河段的最小局地汇水区]
    E --> F[追溯全部上游 COMID<br/>并合并为完整流域 geometry]
    F --> G[用原始 Point(lon, lat) 计算点面关系]
    G --> H[point_in_local = geometry_local.covers(point)]
    G --> I[point_in_basin = geometry.covers(point)<br/>仅限 upstream_traced]
    H --> J[进入 basin_policy.py]
    I --> J
    D --> J

    J --> Z{source in RiverSed / GSED / Dethier?}
    Z -- 是 --> Z1[unresolved / no_match]
    Z -- 否 --> K{basin_id 缺失<br/>或 match_quality=failed?}
    K -- 是 --> K1[unresolved / no_match]
    K -- 否 --> L{match_quality=area_mismatch?}
    L -- 是 --> L1[unresolved / area_mismatch]
    L -- 否 --> M{distance_m <= 300?}
    M -- 是 --> M1[resolved / ok]
    M -- 否 --> N{distance_m <= 1000 且<br/>area_matched / area_approximate?}
    N -- 是 --> N1[resolved / ok]
    N -- 否 --> O{distance_m <= 1000 且<br/>point_in_local=True?}
    O -- 是 --> O1[resolved / ok]
    O -- 否 --> Q{distance_m > 1000?}
    Q -- 是 --> Q1[unresolved / large_offset]
    Q -- 否 --> R[unresolved / geometry_inconsistent]

    Z1 --> S[写入 s4 站点级结果]
    K1 --> S
    L1 --> S
    M1 --> S
    N1 --> S
    O1 --> S
    Q1 --> S
    R --> S

    S --> T[s5 cluster 合并]
    T --> U[s6 写入 master nc<br/>保留 basin_status / basin_flag / basin_distance_m / point_in_local / point_in_basin]
    U --> V[s7 导出点位 catalog]
    U --> W{basin_status = resolved?}
    W -- 是 --> X[进入 basin polygon sidecar]
    W -- 否 --> Y[保留观测<br/>不发布 basin polygon]
```

## 图例说明

1. `geometry_local`
   表示匹配河段自身对应的最小局地汇水区。
2. `geometry`
   表示沿上游网络追溯后合并得到的完整流域。
3. `point_in_local`
   是当前自动接受规则中最重要的局地几何一致性证据。
4. `point_in_basin`
   保留为辅助诊断字段，但本身不足以单独放宽自动接受。
5. `RiverSed / GSED / Dethier`
   这些 reach-scale remote-sensing products 不发布 MERIT basin assignment；observation 保留，`basin_status=unresolved`，`basin_flag=no_match`。
6. 名称字段中的 `estuary / delta / tidal / coastal` 等关键词，当前已不再参与自动判定。
7. `resolved`
   可发布 basin polygon。
8. `unresolved`
   保留观测记录，但不发布 basin polygon。

## `match_quality` 判定流程

`match_quality` 不是在 `basin_policy.py` 里决定的，而是在 `basin_tracer.find_best_reach()` 里先判出来，再传给发布级 policy。

```mermaid
flowchart TD
    A[输入站点点位<br/>lon / lat] --> B[搜索周围候选河段]
    B --> C{找到候选河段?}
    C -- 否 --> C1[match_quality = failed]
    C -- 是 --> D{reported_area 有效?}
    D -- 否 --> D1[按 dist_m 最近选最佳河段<br/>match_quality = distance_only]
    D -- 是 --> E[对每个候选河段计算<br/>area_ratio = uparea / reported_area]
    E --> F[计算 area_error = abs(log10(area_ratio))]
    F --> G[计算 dist_score = dist_m / SEARCH_RADIUS_M]
    G --> H[计算 score = area_error + dist_score]
    H --> I[选择 score 最小的候选河段]
    I --> J[读取最佳河段的 log_err = abs(log10(area_ratio))]
    J --> K{log_err < 0.1?}
    K -- 是 --> K1[match_quality = area_matched]
    K -- 否 --> L{log_err < 0.3?}
    L -- 是 --> L1[match_quality = area_approximate]
    L -- 否 --> L2[match_quality = area_mismatch]
```

### `match_quality` 具体含义

1. `failed`
   没有找到可用候选河段，或输入点位本身无效。
2. `distance_only`
   没有可用 `reported_area`，所以只能按点到河段的几何距离最近来匹配。
3. `area_matched`
   有 `reported_area`，且最佳河段的 `uparea` 与 `reported_area` 非常接近，判据是 `abs(log10(uparea / reported_area)) < 0.1`。
4. `area_approximate`
   有 `reported_area`，面积基本接近，但没有达到 `area_matched`，判据是 `0.1 <= abs(log10(uparea / reported_area)) < 0.3`。
5. `area_mismatch`
   有 `reported_area`，但最佳河段的 `uparea` 与 `reported_area` 偏差较大，判据是 `abs(log10(uparea / reported_area)) >= 0.3`。

### 额外说明

1. 当 `reported_area` 存在时，最佳候选河段不是单纯按最近距离选，而是按 `score = area_error + dist_score` 共同决定。
2. 因此，`match_quality` 同时反映了两类证据：
   一类是几何接近性 `dist_m`
   一类是面积一致性 `reported_area` vs `uparea`
3. `basin_policy.py` 不重新计算 `match_quality`，只消费 tracer 已经给出的结果。

## 快速判定摘要

1. `RiverSed / GSED / Dethier` 等 source -> `unresolved / no_match`
2. `basin_id` 缺失或 `failed` -> `unresolved / no_match`
3. `area_mismatch` -> `unresolved / area_mismatch`
4. `distance_m <= 300` -> `resolved / ok`
5. `distance_m <= 1000` 且 `area_matched / area_approximate` -> `resolved / ok`
6. `distance_m <= 1000` 且 `point_in_local=True` -> `resolved / ok`
7. 普通 matched stations 若 `distance_m > 1000` -> `unresolved / large_offset`
8. 其他剩余情况 -> `unresolved / geometry_inconsistent`
