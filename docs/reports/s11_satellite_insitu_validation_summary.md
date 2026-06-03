# Satellite/In-Situ Validation Summary

本页总结 `s11_satellite_insitu_validation_fast.py` 已有验证结果，基于现有输出目录：

`../output_other/validation_results/`

本次没有重新运行 s11，也没有使用 2026-05-28 后续更新的 release 文件。这里的目标不是写成论文正文，而是把最重要的验证发现、关键表格、图件解读和可用于 ESSD 的要点整理成一个清晰的结果说明。

## 1. Validation Setup

本验证使用 `candidate_sidecar` 模式。输入的 in-situ/non-satellite 候选表为：

`../output/sed_reference_release/sed_reference_overlap_candidates.parquet`

验证流程额外追加了 471 条 satellite validation 候选记录，来源为 `sed_reference_satellite_candidates.parquet`。标准化后共有 14,439,667 条 observation rows，并构建出 230 条 satellite/reach-scale 与 in-situ 配对记录。

配对逻辑如下：

- satellite/reach-scale 记录作为 anchor；
- in-situ 记录必须位于同一 `cluster` 和同一 `resolution`；
- 时间窗口包括 `exact`、`pm1d` 和 `pm2d`；
- 三个窗口是累计窗口：`exact` 包含在 `pm1d` 中，`pm1d` 包含在 `pm2d` 中；
- bias 和 residual 均定义为 `satellite - in-situ`；
- `R2` 为 Pearson correlation 的平方。

## 2. Key Numbers

| Item | Value |
|---|---:|
| Observation rows after normalization | 14,439,667 |
| Appended satellite candidate rows | 471 |
| Total pair records | 230 |
| Pairing windows | `exact`, `pm1d`, `pm2d` |
| Variables with metrics | `Q`, `SSC`, `SSL` |
| Main validation variable | `SSC` |

配对数量以 SSC 为主。Q 和 SSL 也有结果，但样本量明显更小，适合作为辅助诊断，不适合作为主要结论来源。

## 3. Overall Metrics

### 3.1 SSC Overall Metrics

SSC 是本次验证中最有解释价值的变量。三个时间窗口下，SSC 的 Spearman rank correlation 均保持在 0.72 以上，说明 satellite/reach-scale 记录能够较好保持浓度的相对变化顺序。

| Window | n pairs | n clusters | Bias | RMSE | MAE | Pearson | Spearman | R2 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `exact` | 54 | 13 | -20.63 | 50.07 | 24.13 | 0.716 | 0.797 | 0.513 |
| `pm1d` | 59 | 13 | -18.33 | 47.95 | 22.64 | 0.726 | 0.810 | 0.527 |
| `pm2d` | 69 | 17 | -15.51 | 47.78 | 23.33 | 0.597 | 0.722 | 0.356 |

主要现象：

- SSC 的 bias 在三个窗口中均为负值，表示 satellite/reach-scale 记录整体低于配对的 in-situ 记录。
- `pm1d` 的 Spearman 最高，为 0.810；这说明允许一天以内的时间差后，SSC 的相对排序仍然稳定。
- `pm2d` 增加了配对数量和 cluster 覆盖，但 Pearson、Spearman 和 R2 均下降，说明放宽时间窗口会引入额外差异。

### 3.2 Q and SSL Overall Metrics

Q 和 SSL 的配对数量较少，尤其 `exact` 窗口只有 2 个 pairs，因此这些结果更适合作为 consistency check，而不是强结论。

| Variable | Window | n pairs | n clusters | Bias | RMSE | Pearson | Spearman | R2 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Q | `exact` | 2 | 2 | 150.73 | 217.65 | 1.000 | 1.000 | 1.000 |
| Q | `pm1d` | 7 | 2 | 39.59 | 139.39 | 0.141 | 0.252 | 0.020 |
| Q | `pm2d` | 15 | 5 | 46.46 | 114.79 | 0.746 | 0.483 | 0.556 |
| SSL | `exact` | 2 | 2 | 207.61 | 289.48 | 1.000 | 1.000 | 1.000 |
| SSL | `pm1d` | 7 | 2 | 57.77 | 192.00 | 0.632 | 0.559 | 0.400 |
| SSL | `pm2d` | 15 | 5 | -93.66 | 394.60 | 0.554 | 0.548 | 0.307 |

需要特别注意：`exact` 窗口中 Q 和 SSL 的 correlation 为 1.000，是因为样本量只有 2，不能解读为模型或数据本身达到完美一致。

## 4. Source-Pair Results

### 4.1 Most Stable SSC Pair: RiverSed vs USGS

`RiverSed vs USGS` 是本次验证中最稳定、样本量最大的 SSC source-pair。三个窗口下结果完全一致，说明这些配对主要来自 exact same-day observations，放宽到 `pm1d` 或 `pm2d` 没有额外增加该 source-pair 的样本。

| Source pair | Variable | Window | n pairs | n clusters | Bias | RMSE | Pearson | Spearman | R2 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| RiverSed vs USGS | SSC | `exact` | 51 | 10 | -21.70 | 51.36 | 0.715 | 0.801 | 0.511 |
| RiverSed vs USGS | SSC | `pm1d` | 51 | 10 | -21.70 | 51.36 | 0.715 | 0.801 | 0.511 |
| RiverSed vs USGS | SSC | `pm2d` | 51 | 10 | -21.70 | 51.36 | 0.715 | 0.801 | 0.511 |

这组结果可以作为 ESSD 中最主要的 satellite/in-situ validation evidence：样本量相对较大，cluster 覆盖较稳定，秩相关较高，而且结果不依赖较宽的时间窗口。

### 4.2 Multi-Variable Pair: Dethier vs GFQA_v2

`Dethier vs GFQA_v2` 是唯一同时形成 Q、SSC 和 SSL 验证结果的主要 source-pair。它在 `pm2d` 下有 15 个 pairs 和 5 个 clusters，但在 `exact` 窗口下只有 2 个 pairs。

| Source pair | Variable | Window | n pairs | n clusters | Bias | RMSE | Pearson | Spearman | R2 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Dethier vs GFQA_v2 | Q | `pm2d` | 15 | 5 | 46.46 | 114.79 | 0.746 | 0.483 | 0.556 |
| Dethier vs GFQA_v2 | SSC | `pm2d` | 15 | 5 | -5.86 | 20.26 | -0.046 | -0.080 | 0.002 |
| Dethier vs GFQA_v2 | SSL | `pm2d` | 15 | 5 | -93.66 | 394.60 | 0.554 | 0.548 | 0.307 |

这组结果说明该 validation path 可以覆盖多变量，但 SSC 在该 source-pair 中相关性很弱，不能作为 SSC 一致性的主要证据。Q 和 SSL 的相关性有一定诊断价值，但样本量仍然偏小。

### 4.3 Minor Source-Pairs

少量 source-pair 的样本数非常低，例如 `RiverSed vs HYDAT` 只有 1 个 SSC pair，`GSED vs GFQA_v2` 在 `pm2d` 下只有 2 个 SSC pairs。这些结果可以保留在 supplementary diagnostics 中，但不建议在主文中展开解释。

## 5. Interpretation

本次验证最重要的结论是：SSC 的 satellite/reach-scale 与 in-situ 配对结果在总体上表现出较稳定的 rank consistency。尤其是 `RiverSed vs USGS`，三个窗口下均有 51 个 pairs、10 个 clusters，Spearman 为 0.801，说明 satellite/reach-scale 产品能够较好保留 SSC 的相对高低变化。

同时，SSC 的 bias 普遍为负，说明 satellite/reach-scale 记录相对于 in-situ 记录存在整体偏低倾向。这个偏差不一定只来自观测误差，也可能与采样时间差、空间代表性差异、河段尺度聚合、不同数据源的测量和处理方法有关。

时间窗口的影响也比较清楚：从 `exact` 放宽到 `pm2d` 后，SSC 的 pairs 从 54 增加到 69，clusters 从 13 增加到 17，但 Spearman 从 0.797 降到 0.722，R2 从 0.513 降到 0.356。这说明更宽的时间窗口可以提高覆盖度，但会引入更多时序错配或水文过程变化带来的差异。

对 ESSD 写作而言，建议把 SSC 作为主验证结果，把 Q 和 SSL 作为辅助诊断。Q 和 SSL 的 exact-window 样本量太小，不能支撑强结论；`pm2d` 下样本量有所增加，但仍应谨慎解释。

## 6. Figures

### 6.1 SSC Scatter by Pairing Window

![SSC scatter by pairing window](../output_other/validation_results/figures/satellite_insitu_scatter_by_window_SSC.png)

建议用途：展示 SSC 在不同 pairing windows 下的 satellite/reach-scale 与 in-situ 对比关系。该图适合支撑“SSC 具有较稳定 rank consistency，但存在离散度和负 bias”的叙述。

### 6.2 Residual by SSC Bin

![Residual by SSC bin](../output_other/validation_results/figures/satellite_insitu_residual_by_ssc_bin.png)

建议用途：检查 residual 是否随 SSC 浓度范围变化。若在高 SSC bin 中 residual 分布更宽，可用于说明高浊度或高输沙事件下不确定性更大。

### 6.3 Metric Heatmap

![Metric heatmap](../output_other/validation_results/figures/satellite_insitu_metric_heatmap.png)

建议用途：快速比较不同 window、variable 和 source-pair 的 validation performance。该图适合放在内部报告或 supplementary material 中，帮助读者看出 SSC 主结果和 Q/SSL 辅助结果的差异。

## 7. Limitations

- 本总结基于 2026-05-22 已有验证输出，不是重新运行 2026-05-28 release 得到的结果。
- `exact`、`pm1d` 和 `pm2d` 是累计窗口，因此不同窗口之间不是相互独立样本。
- Candidate sidecar 保存范围会影响 wider-window 配对完整性。如果 sidecar 只保存 overlap candidates，则较宽时间窗口可能低估可配对记录数。
- Q 和 SSL 的样本量明显小于 SSC，尤其 exact-window 结果不应被过度解释。
- Bias 定义为 `satellite - in-situ`。负 bias 表示 satellite/reach-scale 记录低于 in-situ 记录。
- 缺失 river width 被归为 `missing`，缺失 climate zone 被归为 `unknown`；因此 strata-level 结果需要谨慎使用。

## 8. ESSD-Ready Takeaways

- The satellite/in-situ validation produced 230 paired records from 14.44 million normalized observations using a candidate-sidecar workflow.
- SSC provides the strongest validation signal among the evaluated variables.
- For SSC, the overall Spearman correlation is 0.797 for exact pairs, 0.810 for +/-1 day pairs, and 0.722 for +/-2 day pairs.
- The dominant SSC source-pair is `RiverSed vs USGS`, with 51 pairs across 10 clusters and a stable Spearman correlation of 0.801.
- The negative SSC bias indicates that satellite/reach-scale records are generally lower than paired in-situ records.
- Expanding the pairing window increases spatial/temporal coverage but reduces correlation, highlighting a tradeoff between sample size and temporal comparability.
