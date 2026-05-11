# Basin Fallback Rule Explainer

这份说明只解释一个容易混淆的点：`area_buffer_fallback` 会生成一个兜底几何，但它不能把 `point_in_basin` 当成真实上游流域证据。

## 核心结论

1. `point_in_basin` 只在 `method == upstream_traced` 时才允许为真。
2. `_create_area_buffer()` 生成的圆以站点坐标为圆心。
3. 因此，fallback 场景里如果直接拿这个圆去判断 `covers(point)`，结果几乎总会是 `True`。
4. 这个 `True` 不能说明“点落在真实上游流域内”，只能说明“兜底圆把自己的圆心包住了”。
5. 所以当前实现故意把 fallback 几何排除在 `point_in_basin` 赋值逻辑之外。

## 流程图

```mermaid
flowchart TD
    A[station point] --> B[match MERIT reach]
    B --> C[build geometry_local]
    C --> D[trace upstream reaches]
    D --> E{real upstream polygon merged?}
    E -- yes --> F[method = upstream_traced]
    E -- yes --> G[point_in_basin = geometry.covers(point)]
    E -- no --> H[method = area_buffer_fallback]
    H --> I[build area-based circle around station]
    I --> J[do not expose circle coverage as point_in_basin]
    G --> K[policy consumes point_in_local and point_in_basin]
    J --> K
```

## 为什么 fallback 不能给 `point_in_basin=True`

设想一个最简单的 fallback 场景：

1. 真实上游 polygon 没拼出来。
2. tracer 用 `reported_area` 或 `uparea_merit` 生成一个面积大致匹配的圆。
3. 这个圆的圆心就是站点本身。

这时如果计算：

```text
fallback_circle.covers(station_point)
```

结果几乎一定是 `True`。但这个 `True` 的含义非常弱，因为它并不来自真实水文网络追溯，只是来自“圆心和点是同一个位置”。

也正因为如此，当前主线把 fallback 圆视为：

1. 可以输出的兜底几何；
2. 可以帮助人工理解面积尺度的辅助对象；
3. 但不是 `point_in_basin=True` 的合法证据。

## 和 policy 的关系

`basin_policy.py` 当前真正依赖的自动放行证据更偏向：

1. 小偏移距离；
2. `distance_m <= 1000` 时的 `point_in_local=True`；
3. `distance_m <= 1000` 时的 drainage-area evidence。

相反，`point_in_basin` 当前更像诊断字段，而不是主要放行开关。

当前 no-basin-match source 规则也独立于这些几何证据：

1. `RiverSed / GSED / Dethier` 等 reach-scale remote-sensing products 不发布 MERIT basin assignment；
2. 它们保留 observation；
3. 即使 `point_in_local=True`，也返回 `unresolved / no_match`。

普通 matched stations 则继续遵循距离阈值：

1. `distance_m <= 1000` 且证据足够时可以 `resolved / ok`；
2. `distance_m > 1000` 时返回 `unresolved / large_offset`；
3. `point_in_basin=True` 不会单独让大偏移站点自动 resolved。

## 对应脚本与图片

- 教学脚本：`python3 explain_basin_fallback_rule.py`
- 输出图片：`output/basin_fallback_rule_explainer.png`

脚本会做三件事：

1. 打印 5 个合成场景；
2. 用断言校验这些场景和当前 `classify_basin_result()` 一致；
3. 生成一张左右对照图。

图片含义如下：

1. 左图是 `upstream_traced`，`point_in_basin=True` 有真实几何意义。
2. 右图是 `area_buffer_fallback`，圆虽然包住站点，但这个 inside 不能被发布成 `point_in_basin=True`。

![Fallback rule explainer](output/basin_fallback_rule_explainer.png)
