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

- Q 的 final flag 以 good 为主：good rate 为 97.0%，usable rate 为 97.7%。
- SSC 和 SSL 的主要限制来自 missing：SSC missing rate 为 3.3%，SSL missing rate 为 0.2%。
- Final flag 8 的最高比例为 0.0%，说明最终发布层基本没有未检查记录残留。
- monthly Q 的 usable rate 最高，为 98.4%。
- annual Q 的 missing rate 最高，为 19.4%，适合在结果讨论中作为覆盖限制说明。

### Final QC by Variable

| Variable | N | Good | Derived | Usable | Problem | Missing | Not checked |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Q | 2,935,580 | 97.0% | 0.7% | 97.7% | 1.7% | 0.6% | 0.0% |
| SSC | 2,935,580 | 92.6% | 0.8% | 93.4% | 3.4% | 3.3% | 0.0% |
| SSL | 2,935,580 | 2.1% | 93.6% | 95.7% | 4.1% | 0.2% | 0.0% |

### Resolution Difference

| Resolution | Variable | N | Usable | Problem | Missing |
| --- | --- | --- | --- | --- | --- |
| daily | Q | 2,813,598 | 97.7% | 1.8% | 0.6% |
| daily | SSC | 2,813,598 | 93.3% | 3.3% | 3.4% |
| daily | SSL | 2,813,598 | 95.6% | 4.2% | 0.2% |
| monthly | Q | 121,363 | 98.4% | 1.6% | 0.0% |
| monthly | SSC | 121,363 | 95.3% | 4.6% | 0.1% |
| monthly | SSL | 121,363 | 97.9% | 2.1% | 0.0% |
| annual | Q | 619 | 78.8% | 1.8% | 19.4% |
| annual | SSC | 619 | 96.8% | 3.2% | 0.0% |
| annual | SSL | 619 | 79.0% | 1.6% | 19.4% |

## QC 阶段解释

阶段性 QC 用于解释 final flag 的来源：QC1 主要对应物理范围筛查，QC2 对应 log-IQR 异常筛查，QC3 对应 SSC-Q 一致性或 SSL 传播关系。

| Stage | Variable | Flag var | Flag 0 | Suspect | Bad | Not checked | Missing |
| --- | --- | --- | --- | --- | --- | --- | --- |
| log_iqr | Q | Q_qc2 | 17.9% | 0.3% | 0.0% | 0.2% | 81.7% |
| log_iqr | SSC | SSC_qc2 | 14.8% | 0.5% | 0.0% | 0.1% | 84.6% |
| log_iqr | SSL | SSL_qc2 | 0.5% | 0.0% | 0.0% | 18.2% | 81.3% |
| physical_plausibility | Q | Q_qc1 | 18.3% | 0.0% | 0.0% | 0.0% | 81.7% |
| physical_plausibility | SSC | SSC_qc1 | 15.4% | 0.0% | 0.0% | 0.0% | 84.6% |
| physical_plausibility | SSL | SSL_qc1 | 15.8% | 0.0% | 2.9% | 0.0% | 81.3% |
| ssc_q_consistency | SSC | SSC_qc3 | 1.8% | 0.1% | 0.0% | 0.4% | 97.7% |
| ssc_q_consistency | SSL | SSL_qc3 | 0.0% | 0.1% | 0.0% | 5.5% | 94.4% |

## 热点诊断

下面的热点表按 `issue_rate = problem_rate + missing_rate` 排序；其中 `problem_rate` 只包含 suspect+bad，missing 单独保留，便于区分质量异常和观测缺失。

### Top Sources

| Source | Type | Variable | N | Issue | Problem | Missing |
| --- | --- | --- | --- | --- | --- | --- |
| Huanghe | other | Q | 120 | 100.0% | 0.0% | 100.0% |
| Huanghe | other | SSL | 120 | 100.0% | 0.0% | 100.0% |
| Mekong_Delta | other | Q | 11,323 | 92.9% | 1.1% | 91.8% |
| Mekong_Delta | other | SSC | 11,323 | 91.8% | 0.0% | 91.8% |
| HYBAM | other | SSC | 96,882 | 88.1% | 0.3% | 87.8% |
| HYBAM | other | SSL | 96,882 | 87.9% | 87.9% | 0.0% |
| Robotham | other | SSC | 3,432 | 37.1% | 37.1% | 0.0% |
| Robotham | other | SSL | 3,432 | 27.7% | 27.2% | 0.5% |
| NERC | other | SSL | 624 | 11.7% | 2.4% | 9.3% |
| NERC | other | Q | 624 | 9.5% | 0.2% | 9.3% |

### Top Clusters

| Cluster | Type | Variable | N | Issue | Problem | Missing |
| --- | --- | --- | --- | --- | --- | --- |
| SED000644 |  | Q | 401 | 100.0% | 0.0% | 100.0% |
| SED000644 |  | SSL | 401 | 100.0% | 0.0% | 100.0% |
| SED000627 |  | Q | 146 | 100.0% | 0.0% | 100.0% |
| SED000627 |  | SSL | 146 | 100.0% | 0.0% | 100.0% |
| SED000788 |  | Q | 131 | 100.0% | 0.0% | 100.0% |
| SED000788 |  | SSL | 131 | 100.0% | 0.0% | 100.0% |
| SED000341 |  | Q | 560 | 99.6% | 0.0% | 99.6% |
| SED000341 |  | SSL | 560 | 99.6% | 0.0% | 99.6% |
| SED000895 |  | Q | 2,557 | 96.8% | 0.1% | 96.7% |
| SED000895 |  | SSC | 2,557 | 96.7% | 0.0% | 96.7% |
| SED000108 |  | SSC | 9,340 | 96.6% | 0.4% | 96.2% |
| SED000108 |  | SSL | 9,340 | 96.3% | 96.3% | 0.0% |
| SED000893 |  | Q | 2,922 | 96.1% | 0.0% | 96.1% |
| SED000893 |  | SSC | 2,922 | 96.1% | 0.0% | 96.1% |
| SED000103 |  | SSC | 6,906 | 94.5% | 0.1% | 94.3% |
| SED000103 |  | SSL | 6,906 | 94.3% | 94.3% | 0.0% |
| SED000110 |  | SSC | 6,752 | 94.0% | 0.3% | 93.7% |
| SED000110 |  | SSL | 6,752 | 93.7% | 93.7% | 0.0% |
| SED000104 |  | SSC | 9,840 | 93.1% | 0.4% | 92.7% |
| SED000104 |  | SSL | 9,840 | 92.8% | 92.8% | 0.0% |
| SED000102 |  | SSC | 10,403 | 91.7% | 0.1% | 91.6% |
| SED000102 |  | SSL | 10,403 | 91.6% | 91.6% | 0.0% |
| SED000106 |  | SSC | 9,732 | 91.2% | 0.1% | 91.2% |
| SED000106 |  | SSL | 9,732 | 91.2% | 91.2% | 0.0% |
| SED000107 |  | SSC | 10,818 | 91.0% | 0.0% | 90.9% |
| SED000107 |  | SSL | 10,818 | 90.9% | 90.9% | 0.0% |
| SED000105 |  | SSC | 10,495 | 90.8% | 0.3% | 90.5% |
| SED000105 |  | SSL | 10,495 | 90.7% | 90.7% | 0.0% |
| SED000894 |  | Q | 2,922 | 89.6% | 1.7% | 87.9% |
| SED000896 |  | Q | 2,922 | 89.5% | 2.3% | 87.3% |
| SED000894 |  | SSC | 2,922 | 87.9% | 0.0% | 87.9% |
| SED000109 |  | SSC | 4,928 | 87.8% | 0.1% | 87.8% |
| SED000109 |  | SSL | 4,928 | 87.8% | 87.8% | 0.0% |
| SED000896 |  | SSC | 2,922 | 87.3% | 0.0% | 87.3% |
| SED000510 |  | Q | 533 | 81.2% | 0.2% | 81.1% |
| SED000510 |  | SSL | 533 | 81.1% | 0.0% | 81.1% |
| SED000100 |  | SSL | 3,552 | 70.2% | 70.2% | 0.0% |
| SED000100 |  | SSC | 3,552 | 70.2% | 1.2% | 69.0% |
| SED000099 |  | SSC | 6,543 | 69.1% | 0.6% | 68.6% |
| SED000099 |  | SSL | 6,543 | 68.8% | 68.8% | 0.0% |
| SED000101 |  | SSC | 7,573 | 68.0% | 0.7% | 67.3% |
| SED000101 |  | SSL | 7,573 | 67.8% | 67.8% | 0.0% |
| SED000537 |  | Q | 257 | 54.5% | 54.5% | 0.0% |
| SED043742 |  | SSC | 267 | 52.4% | 52.4% | 0.0% |
| SED000864 |  | Q | 237 | 46.0% | 0.0% | 46.0% |
| SED000864 |  | SSL | 237 | 46.0% | 0.0% | 46.0% |
| SED000791 |  | SSL | 4,597 | 41.3% | 1.6% | 39.7% |
| SED000791 |  | Q | 4,597 | 39.7% | 0.0% | 39.7% |
| SED043098 |  | SSC | 1,436 | 39.4% | 39.4% | 0.0% |
| SED043099 |  | SSC | 565 | 37.9% | 37.9% | 0.0% |
| SED043364 |  | SSC | 257 | 35.8% | 35.8% | 0.0% |
| SED043098 |  | SSL | 1,436 | 34.8% | 34.8% | 0.0% |
| SED043100 |  | SSC | 1,431 | 34.5% | 34.5% | 0.0% |
| SED043099 |  | SSL | 565 | 32.7% | 32.7% | 0.0% |
| SED000116 |  | Q | 649 | 32.7% | 0.0% | 32.7% |
| SED000116 |  | SSL | 649 | 32.7% | 0.0% | 32.7% |
| SED000906 |  | Q | 159 | 28.9% | 0.6% | 28.3% |
| SED000906 |  | SSL | 159 | 28.9% | 0.6% | 28.3% |
| SED043779 |  | SSC | 212 | 26.9% | 26.9% | 0.0% |
| SED000788 |  | SSC | 131 | 26.7% | 26.7% | 0.0% |
| SED043703 |  | SSC | 1,003 | 25.0% | 25.0% | 0.0% |
| SED045874 |  | SSC | 101 | 24.8% | 24.8% | 0.0% |
| SED043852 |  | SSC | 364 | 23.4% | 23.4% | 0.0% |
| SED000338 |  | SSL | 1,195 | 23.3% | 0.7% | 22.6% |
| SED000338 |  | Q | 1,195 | 22.6% | 0.0% | 22.6% |
| SED000536 |  | Q | 396 | 21.7% | 21.7% | 0.0% |
| SED043542 |  | SSC | 447 | 21.7% | 21.7% | 0.0% |
| SED043853 |  | SSC | 364 | 21.2% | 21.2% | 0.0% |
| SED043814 |  | Q | 212 | 20.8% | 20.8% | 0.0% |
| SED043703 |  | Q | 1,003 | 20.5% | 20.5% | 0.0% |
| SED043909 |  | SSC | 729 | 19.9% | 19.9% | 0.0% |
| SED043615 |  | SSC | 183 | 19.7% | 19.7% | 0.0% |
| SED000438 |  | Q | 153 | 19.6% | 0.0% | 19.6% |
| SED000438 |  | SSL | 153 | 19.6% | 0.0% | 19.6% |
| SED043711 |  | SSC | 128 | 19.5% | 19.5% | 0.0% |
| SED043378 |  | SSC | 185 | 19.5% | 19.5% | 0.0% |
| SED000754 |  | SSC | 192 | 19.3% | 19.3% | 0.0% |
| SED043874 |  | SSC | 122 | 18.9% | 18.9% | 0.0% |
| SED043712 |  | SSC | 329 | 18.8% | 18.8% | 0.0% |
| SED043961 |  | SSC | 729 | 18.8% | 18.8% | 0.0% |
| SED043100 |  | SSL | 1,431 | 18.6% | 17.3% | 1.3% |
| SED000857 |  | SSC | 135 | 17.8% | 17.8% | 0.0% |
| SED043911 |  | SSC | 1,459 | 17.7% | 17.7% | 0.0% |
| SED043559 |  | SSC | 104 | 17.3% | 17.3% | 0.0% |
| SED043711 |  | SSL | 128 | 17.2% | 17.2% | 0.0% |
| SED043402 |  | SSC | 261 | 16.5% | 16.5% | 0.0% |
| SED043716 |  | SSC | 365 | 16.4% | 16.4% | 0.0% |
| SED043668 |  | Q | 382 | 15.7% | 15.7% | 0.0% |
| SED043757 |  | Q | 199 | 15.6% | 15.6% | 0.0% |
| SED043177 |  | SSC | 456 | 15.6% | 15.6% | 0.0% |
| SED000272 |  | Q | 6,075 | 15.5% | 2.6% | 13.0% |
| SED043297 |  | SSC | 529 | 15.5% | 15.5% | 0.0% |
| SED043111 |  | SSC | 273 | 15.4% | 15.4% | 0.0% |
| SED043870 |  | SSC | 1,929 | 15.3% | 15.3% | 0.0% |
| SED043432 |  | SSC | 11,689 | 15.1% | 15.1% | 0.0% |
| SED000469 |  | SSC | 549 | 14.9% | 14.9% | 0.0% |
| SED000140 |  | Q | 488 | 14.8% | 14.8% | 0.0% |
| SED043789 |  | SSC | 152 | 14.5% | 14.5% | 0.0% |
| SED043632 |  | SSC | 112 | 14.3% | 14.3% | 0.0% |
| SED043977 |  | SSC | 2,556 | 14.1% | 14.1% | 0.0% |

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
