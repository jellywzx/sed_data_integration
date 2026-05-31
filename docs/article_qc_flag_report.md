# S8 QC Flag Statistics Report

本报告由 `stats/qc_flag_statistics.py` 自动生成，用于总结 S8 发布级产品的 final QC flags 和阶段性 QC flags。

## 数据与方法

- 输入 master NetCDF：`/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/sed_reference_master.nc`
- source type 辅助表：`/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/s6_cluster_quality_order.csv`
- 输出表目录：`/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output_other/qc_flag_statistics/tables`
- 输出图目录：`/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output_other/qc_flag_statistics/figures`
- 统计口径：`usable_rate = flag 0 + flag 1`；`problem_rate = flag 2 + flag 3`；`missing_rate = flag 9`；`not_checked_rate = flag 8`。
- Final flag 含义：0=good，1=derived/estimated，2=suspect，3=bad，8=not checked，9=missing。

## 核心结论

- Q 的 final flag 以 good 为主：good rate 为 98.6%，usable rate 为 98.7%。
- SSC 和 SSL 的主要限制来自 missing：SSC missing rate 为 80.3%，SSL missing rate 为 79.7%。
- Final flag 8 的最高比例为 0.0%，说明最终发布层基本没有未检查记录残留。
- daily Q 的 usable rate 最高，为 98.7%。
- daily SSC 的 missing rate 最高，为 80.9%，适合在结果讨论中作为覆盖限制说明。

### Final QC by Variable

| Variable | N | Good | Derived | Usable | Problem | Missing | Not checked |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Q | 14,408,106 | 98.6% | 0.1% | 98.7% | 1.2% | 0.1% | 0.0% |
| SSC | 14,408,106 | 18.9% | 0.2% | 19.0% | 0.7% | 80.3% | 0.0% |
| SSL | 14,408,106 | 0.4% | 19.1% | 19.5% | 0.8% | 79.7% | 0.0% |

### Resolution Difference

| Resolution | Variable | N | Usable | Problem | Missing |
| --- | --- | --- | --- | --- | --- |
| daily | Q | 14,261,714 | 98.7% | 1.2% | 0.1% |
| daily | SSC | 14,261,714 | 18.4% | 0.7% | 80.9% |
| daily | SSL | 14,261,714 | 18.9% | 0.8% | 80.3% |
| monthly | Q | 145,663 | 98.7% | 1.3% | 0.0% |
| monthly | SSC | 145,663 | 79.4% | 3.8% | 16.8% |
| monthly | SSL | 145,663 | 81.6% | 1.7% | 16.7% |
| annual | Q | 729 | 81.5% | 2.1% | 16.5% |
| annual | SSC | 729 | 82.2% | 2.7% | 15.1% |
| annual | SSL | 729 | 67.1% | 1.4% | 31.6% |

## QC 阶段解释

阶段性 QC 用于解释 final flag 的来源：QC1 主要对应物理范围筛查，QC2 对应 log-IQR 异常筛查，QC3 对应 SSC-Q 一致性或 SSL 传播关系。

| Stage | Variable | Flag var | Flag 0 | Suspect | Bad | Not checked | Missing |
| --- | --- | --- | --- | --- | --- | --- | --- |
| log_iqr | Q | Q_qc2 | 3.7% | 0.1% | 0.0% | 0.0% | 96.2% |
| log_iqr | SSC | SSC_qc2 | 3.0% | 0.1% | 0.0% | 0.0% | 96.9% |
| log_iqr | SSL | SSL_qc2 | 0.1% | 0.0% | 0.0% | 3.7% | 96.2% |
| physical_plausibility | Q | Q_qc1 | 3.8% | 0.0% | 0.0% | 0.0% | 96.2% |
| physical_plausibility | SSC | SSC_qc1 | 3.1% | 0.0% | 0.0% | 0.0% | 96.9% |
| physical_plausibility | SSL | SSL_qc1 | 3.2% | 0.0% | 0.6% | 0.0% | 96.2% |
| ssc_q_consistency | SSC | SSC_qc3 | 0.4% | 0.0% | 0.0% | 0.1% | 99.5% |
| ssc_q_consistency | SSL | SSL_qc3 | 0.0% | 0.0% | 0.0% | 1.1% | 98.9% |

## 热点诊断

下面的热点表按 `issue_rate = problem_rate + missing_rate` 排序；其中 `problem_rate` 只包含 suspect+bad，missing 单独保留，便于区分质量异常和观测缺失。

### Top Sources

| Source | Type | Variable | N | Issue | Problem | Missing |
| --- | --- | --- | --- | --- | --- | --- |
| Huanghe | other | Q | 120 | 100.0% | 0.0% | 100.0% |
| Huanghe | other | SSL | 120 | 100.0% | 0.0% | 100.0% |
| HYDAT | other | SSC | 12,112,505 | 94.6% | 0.2% | 94.5% |
| HYDAT | other | SSL | 12,112,505 | 94.6% | 0.1% | 94.5% |
| Mekong_Delta | other | SSC | 11,921 | 92.2% | 0.0% | 92.2% |
| Mekong_Delta | other | Q | 11,921 | 88.5% | 1.2% | 87.2% |
| HYBAM | other | SSC | 96,938 | 88.1% | 0.3% | 87.8% |
| HYBAM | other | SSL | 96,938 | 87.9% | 87.9% | 0.1% |
| NERC | other | SSL | 3,131 | 82.4% | 0.5% | 81.9% |
| NERC | other | SSC | 3,131 | 81.0% | 0.9% | 80.1% |

### Top Clusters

| Cluster | Type | Variable | N | Issue | Problem | Missing |
| --- | --- | --- | --- | --- | --- | --- |
| SED000439 | nan | SSC | 42,043 | 100.0% | 0.0% | 100.0% |
| SED000439 | nan | SSL | 42,043 | 100.0% | 0.0% | 100.0% |
| SED000422 | nan | SSC | 40,969 | 100.0% | 0.0% | 100.0% |
| SED000422 | nan | SSL | 40,969 | 100.0% | 0.0% | 100.0% |
| SED000319 | nan | SSC | 39,905 | 100.0% | 0.0% | 100.0% |
| SED000319 | nan | SSL | 39,905 | 100.0% | 0.0% | 100.0% |
| SED000328 | nan | SSC | 39,830 | 100.0% | 0.0% | 100.0% |
| SED000328 | nan | SSL | 39,830 | 100.0% | 0.0% | 100.0% |
| SED000191 | nan | SSC | 39,537 | 100.0% | 0.0% | 100.0% |
| SED000191 | nan | SSL | 39,537 | 100.0% | 0.0% | 100.0% |
| SED000332 | nan | SSC | 38,809 | 100.0% | 0.0% | 100.0% |
| SED000332 | nan | SSL | 38,809 | 100.0% | 0.0% | 100.0% |
| SED000393 | nan | SSC | 38,231 | 100.0% | 0.0% | 100.0% |
| SED000393 | nan | SSL | 38,231 | 100.0% | 0.0% | 100.0% |
| SED000329 | nan | SSC | 37,078 | 100.0% | 0.0% | 100.0% |
| SED000329 | nan | SSL | 37,078 | 100.0% | 0.0% | 100.0% |
| SED000169 | nan | SSC | 35,237 | 100.0% | 0.0% | 100.0% |
| SED000169 | nan | SSL | 35,237 | 100.0% | 0.0% | 100.0% |
| SED000330 | nan | SSC | 34,676 | 100.0% | 0.0% | 100.0% |
| SED000330 | nan | SSL | 34,676 | 100.0% | 0.0% | 100.0% |
| SED000353 | nan | SSC | 33,852 | 100.0% | 0.0% | 100.0% |
| SED000353 | nan | SSL | 33,852 | 100.0% | 0.0% | 100.0% |
| SED000251 | nan | SSC | 33,838 | 100.0% | 0.0% | 100.0% |
| SED000251 | nan | SSL | 33,838 | 100.0% | 0.0% | 100.0% |
| SED000757 | nan | SSC | 33,161 | 100.0% | 0.0% | 100.0% |
| SED000757 | nan | SSL | 33,161 | 100.0% | 0.0% | 100.0% |
| SED000215 | nan | SSC | 32,870 | 100.0% | 0.0% | 100.0% |
| SED000215 | nan | SSL | 32,870 | 100.0% | 0.0% | 100.0% |
| SED000390 | nan | SSC | 32,724 | 100.0% | 0.0% | 100.0% |
| SED000390 | nan | SSL | 32,724 | 100.0% | 0.0% | 100.0% |
| SED000756 | nan | SSC | 31,498 | 100.0% | 0.0% | 100.0% |
| SED000756 | nan | SSL | 31,498 | 100.0% | 0.0% | 100.0% |
| SED000162 | nan | SSC | 29,176 | 100.0% | 0.0% | 100.0% |
| SED000162 | nan | SSL | 29,176 | 100.0% | 0.0% | 100.0% |
| SED000168 | nan | SSC | 28,367 | 100.0% | 0.0% | 100.0% |
| SED000168 | nan | SSL | 28,367 | 100.0% | 0.0% | 100.0% |
| SED000822 | nan | SSC | 27,668 | 100.0% | 0.0% | 100.0% |
| SED000822 | nan | SSL | 27,668 | 100.0% | 0.0% | 100.0% |
| SED000721 | nan | SSC | 27,606 | 100.0% | 0.0% | 100.0% |
| SED000721 | nan | SSL | 27,606 | 100.0% | 0.0% | 100.0% |
| SED000362 | nan | SSC | 27,414 | 100.0% | 0.0% | 100.0% |
| SED000362 | nan | SSL | 27,414 | 100.0% | 0.0% | 100.0% |
| SED000883 | nan | SSC | 27,389 | 100.0% | 0.0% | 100.0% |
| SED000883 | nan | SSL | 27,389 | 100.0% | 0.0% | 100.0% |
| SED000281 | nan | SSC | 27,371 | 100.0% | 0.0% | 100.0% |
| SED000281 | nan | SSL | 27,371 | 100.0% | 0.0% | 100.0% |
| SED000772 | nan | SSC | 26,958 | 100.0% | 0.0% | 100.0% |
| SED000772 | nan | SSL | 26,958 | 100.0% | 0.0% | 100.0% |
| SED000194 | nan | SSC | 26,573 | 100.0% | 0.0% | 100.0% |
| SED000194 | nan | SSL | 26,573 | 100.0% | 0.0% | 100.0% |
| SED000170 | nan | SSC | 26,071 | 100.0% | 0.0% | 100.0% |
| SED000170 | nan | SSL | 26,071 | 100.0% | 0.0% | 100.0% |
| SED000490 | nan | SSC | 26,045 | 100.0% | 0.0% | 100.0% |
| SED000490 | nan | SSL | 26,045 | 100.0% | 0.0% | 100.0% |
| SED000252 | nan | SSC | 26,025 | 100.0% | 0.0% | 100.0% |
| SED000252 | nan | SSL | 26,025 | 100.0% | 0.0% | 100.0% |
| SED000761 | nan | SSC | 25,990 | 100.0% | 0.0% | 100.0% |
| SED000761 | nan | SSL | 25,990 | 100.0% | 0.0% | 100.0% |
| SED000846 | nan | SSC | 25,159 | 100.0% | 0.0% | 100.0% |
| SED000846 | nan | SSL | 25,159 | 100.0% | 0.0% | 100.0% |
| SED000273 | nan | SSC | 24,841 | 100.0% | 0.0% | 100.0% |
| SED000273 | nan | SSL | 24,841 | 100.0% | 0.0% | 100.0% |
| SED000217 | nan | SSC | 24,731 | 100.0% | 0.0% | 100.0% |
| SED000217 | nan | SSL | 24,731 | 100.0% | 0.0% | 100.0% |
| SED000266 | nan | SSC | 24,685 | 100.0% | 0.0% | 100.0% |
| SED000266 | nan | SSL | 24,685 | 100.0% | 0.0% | 100.0% |
| SED000645 | nan | SSC | 24,644 | 100.0% | 0.0% | 100.0% |
| SED000645 | nan | SSL | 24,644 | 100.0% | 0.0% | 100.0% |
| SED000244 | nan | SSC | 24,634 | 100.0% | 0.0% | 100.0% |
| SED000244 | nan | SSL | 24,634 | 100.0% | 0.0% | 100.0% |
| SED000267 | nan | SSC | 24,501 | 100.0% | 0.0% | 100.0% |
| SED000267 | nan | SSL | 24,501 | 100.0% | 0.0% | 100.0% |
| SED000213 | nan | SSC | 24,370 | 100.0% | 0.0% | 100.0% |
| SED000213 | nan | SSL | 24,370 | 100.0% | 0.0% | 100.0% |
| SED000320 | nan | SSC | 24,242 | 100.0% | 0.0% | 100.0% |
| SED000320 | nan | SSL | 24,242 | 100.0% | 0.0% | 100.0% |
| SED000230 | nan | SSC | 24,055 | 100.0% | 0.0% | 100.0% |
| SED000230 | nan | SSL | 24,055 | 100.0% | 0.0% | 100.0% |
| SED000274 | nan | SSC | 24,049 | 100.0% | 0.0% | 100.0% |
| SED000274 | nan | SSL | 24,049 | 100.0% | 0.0% | 100.0% |
| SED000487 | nan | SSC | 24,000 | 100.0% | 0.0% | 100.0% |
| SED000487 | nan | SSL | 24,000 | 100.0% | 0.0% | 100.0% |
| SED000775 | nan | SSC | 23,863 | 100.0% | 0.0% | 100.0% |
| SED000775 | nan | SSL | 23,863 | 100.0% | 0.0% | 100.0% |
| SED000315 | nan | SSC | 23,828 | 100.0% | 0.0% | 100.0% |
| SED000315 | nan | SSL | 23,828 | 100.0% | 0.0% | 100.0% |
| SED000542 | nan | SSC | 23,741 | 100.0% | 0.0% | 100.0% |
| SED000542 | nan | SSL | 23,741 | 100.0% | 0.0% | 100.0% |
| SED000216 | nan | SSC | 23,635 | 100.0% | 0.0% | 100.0% |
| SED000216 | nan | SSL | 23,635 | 100.0% | 0.0% | 100.0% |
| SED000344 | nan | SSC | 23,437 | 100.0% | 0.0% | 100.0% |
| SED000344 | nan | SSL | 23,437 | 100.0% | 0.0% | 100.0% |
| SED000240 | nan | SSC | 23,318 | 100.0% | 0.0% | 100.0% |
| SED000240 | nan | SSL | 23,318 | 100.0% | 0.0% | 100.0% |
| SED000306 | nan | SSC | 23,291 | 100.0% | 0.0% | 100.0% |
| SED000306 | nan | SSL | 23,291 | 100.0% | 0.0% | 100.0% |
| SED000195 | nan | SSC | 23,286 | 100.0% | 0.0% | 100.0% |
| SED000195 | nan | SSL | 23,286 | 100.0% | 0.0% | 100.0% |
| SED000657 | nan | SSC | 23,286 | 100.0% | 0.0% | 100.0% |
| SED000657 | nan | SSL | 23,286 | 100.0% | 0.0% | 100.0% |

## 图表建议

- 正文 QC 概览：[figures/fig_qc_flag_distribution.png](figures/fig_qc_flag_distribution.png)
- 正文分辨率对比：[figures/fig_qc_health_by_resolution.png](figures/fig_qc_health_by_resolution.png)
- 时间变化讨论：[figures/fig_qc_yearly_problem_trends.png](figures/fig_qc_yearly_problem_trends.png) 和 [figures/fig_qc_missing_trends.png](figures/fig_qc_missing_trends.png)
- 方法/补充材料：[figures/fig_qc_stage_summary.png](figures/fig_qc_stage_summary.png)
- 补充材料热点诊断：[figures/fig_qc_top_problem_sources.png](figures/fig_qc_top_problem_sources.png) 和 [figures/fig_qc_top_problem_clusters.png](figures/fig_qc_top_problem_clusters.png)

## 输出数据索引

- [tables/table_qc_flag_summary.csv](tables/table_qc_flag_summary.csv)
- [tables/table_qc_health_kpis.csv](tables/table_qc_health_kpis.csv)
- [tables/table_qc_stage_effectiveness.csv](tables/table_qc_stage_effectiveness.csv)
- [tables/table_qc_issue_hotspots.csv](tables/table_qc_issue_hotspots.csv)
- [tables/table_qc_yearly_trends.csv](tables/table_qc_yearly_trends.csv)
- [tables/table_qc_flag_problem_clusters.csv](tables/table_qc_flag_problem_clusters.csv)

## 可支撑的论文表述

- Q 数据整体质量较稳定，final good/usable 比例可作为发布级流量数据可靠性的核心证据。
- SSC/SSL 的限制主要体现为可用观测覆盖不足，而不是大量 suspect/bad；因此讨论中应把质量异常和观测缺失分开表述。
- source、cluster 热点表适合放入 supplement，用于说明少数数据源或站点簇对缺失/问题比例的贡献。
