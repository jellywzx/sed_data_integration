# Climatology QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/qc_flags/tables/climatology`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 14,542
- Final flag rows: 7
- Stage flag rows: 18
- Usable flag count from health KPIs: 5,816
- Problem flag count from health KPIs: 6,061
- Stage-effectiveness rows available: 8

## Flag Schema

| product | flag variable | flag value | flag meaning | long name |
|---|---|---|---|---|
| climatology | Q_flag | 0 | good | quality flag for river discharge |
| climatology | Q_flag | 1 | estimated | quality flag for river discharge |
| climatology | Q_flag | 2 | suspect | quality flag for river discharge |
| climatology | Q_flag | 3 | bad | quality flag for river discharge |
| climatology | Q_flag | 9 | missing | quality flag for river discharge |
| climatology | Q_qc1 | 0 | pass | qc stage 1 physical screen for river discharge |
| climatology | Q_qc1 | 3 | bad | qc stage 1 physical screen for river discharge |
| climatology | Q_qc1 | 9 | missing | qc stage 1 physical screen for river discharge |
| climatology | Q_qc2 | 0 | pass | qc stage 2 log-iqr screen for river discharge |
| climatology | Q_qc2 | 2 | suspect | qc stage 2 log-iqr screen for river discharge |
| climatology | Q_qc2 | 8 | not_checked | qc stage 2 log-iqr screen for river discharge |
| climatology | Q_qc2 | 9 | missing | qc stage 2 log-iqr screen for river discharge |
| climatology | SSC_flag | 0 | good | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 1 | estimated | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 2 | suspect | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 3 | bad | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 9 | missing | quality flag for suspended sediment concentration |
| climatology | SSC_qc1 | 0 | pass | qc stage 1 physical screen for suspended sediment concentration |
| climatology | SSC_qc1 | 3 | bad | qc stage 1 physical screen for suspended sediment concentration |
| climatology | SSC_qc1 | 9 | missing | qc stage 1 physical screen for suspended sediment concentration |
| climatology | SSC_qc2 | 0 | pass | qc stage 2 log-iqr screen for suspended sediment concentration |
| climatology | SSC_qc2 | 2 | suspect | qc stage 2 log-iqr screen for suspended sediment concentration |
| climatology | SSC_qc2 | 8 | not_checked | qc stage 2 log-iqr screen for suspended sediment concentration |
| climatology | SSC_qc2 | 9 | missing | qc stage 2 log-iqr screen for suspended sediment concentration |

_Showing first 24 of 44 rows._

## Final Flag Summary

| temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|
| climatology | SSL | SSL_flag | 0 | good | 1,298 | 98.18% | 1,322 |
| climatology | SSC | SSC_flag | 0 | good | 787 | 59.53% | 1,322 |
| climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| climatology | Q | Q_flag | 9 | missing | 540 | 40.85% | 1,322 |
| climatology | SSC | SSC_flag | 9 | missing | 518 | 39.18% | 1,322 |
| climatology | SSL | SSL_flag | 9 | missing | 24 | 1.82% | 1,322 |
| climatology | SSC | SSC_flag | 1 | estimated | 17 | 1.29% | 1,322 |

## Stage Flag Summary

| temporal resolution | variable | qc stage | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|
| climatology | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 1,298 | 98.18% | 1,322 |
| climatology | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 804 | 60.82% | 1,322 |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 789 | 59.68% | 1,322 |
| climatology | Q | log_iqr | Q_qc2 | 9 | missing | 789 | 59.68% | 1,322 |
| climatology | SSL | log_iqr | SSL_qc2 | 9 | missing | 789 | 59.68% | 1,322 |
| climatology | Q | physical_plausibility | Q_qc1 | 0 | pass | 782 | 59.15% | 1,322 |
| climatology | SSC | log_iqr | SSC_qc2 | 9 | missing | 765 | 57.87% | 1,322 |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 765 | 57.87% | 1,322 |
| climatology | Q | physical_plausibility | Q_qc1 | 9 | missing | 540 | 40.85% | 1,322 |
| climatology | Q | log_iqr | Q_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| climatology | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 533 | 40.32% | 1,322 |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 533 | 40.32% | 1,322 |
| climatology | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| climatology | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 518 | 39.18% | 1,322 |
| climatology | SSC | log_iqr | SSC_qc2 | 0 | pass | 24 | 1.82% | 1,322 |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 24 | 1.82% | 1,322 |
| climatology | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 24 | 1.82% | 1,322 |

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| climatology | Q | Q_qc2 | 1,322 | 0 | 0 | 0 | 789 | 789 | 0% | 0% | 59.68% | 59.68% |
| climatology | SSL | SSL_qc3 | 1,322 | 0 | 0 | 0 | 789 | 789 | 0% | 0% | 59.68% | 59.68% |
| climatology | SSL | SSL_qc2 | 1,322 | 0 | 0 | 0 | 789 | 789 | 0% | 0% | 59.68% | 59.68% |
| climatology | SSC | SSC_qc3 | 1,322 | 24 | 0 | 24 | 765 | 765 | 1.82% | 1.82% | 57.87% | 57.87% |
| climatology | SSC | SSC_qc2 | 1,322 | 24 | 0 | 24 | 765 | 765 | 1.82% | 1.82% | 57.87% | 57.87% |
| climatology | Q | Q_qc1 | 1,322 | 782 | 0 | 782 | 540 | 540 | 59.15% | 59.15% | 40.85% | 40.85% |
| climatology | Q | Q_flag | 1,322 | 782 | 0 | 782 | 540 | 540 | 59.15% | 59.15% | 40.85% | 40.85% |
| climatology | SSC | SSC_flag | 1,322 | 787 | 17 | 804 | 518 | 518 | 59.53% | 60.82% | 39.18% | 39.18% |
| climatology | SSC | SSC_qc1 | 1,322 | 804 | 0 | 804 | 518 | 518 | 60.82% | 60.82% | 39.18% | 39.18% |
| climatology | SSL | SSL_qc1 | 1,322 | 1,298 | 0 | 1,298 | 24 | 24 | 98.18% | 98.18% | 1.82% | 1.82% |
| climatology | SSL | SSL_flag | 1,322 | 1,298 | 0 | 1,298 | 24 | 24 | 98.18% | 98.18% | 1.82% | 1.82% |

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,322 | 0 | 789 | 2,111 | 0% | 59.68% | 159.68% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,322 | 0 | 789 | 2,111 | 0% | 59.68% | 159.68% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,322 | 0 | 789 | 2,111 | 0% | 59.68% | 159.68% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,322 | 24 | 765 | 2,063 | 1.82% | 57.87% | 156.05% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,322 | 24 | 765 | 2,063 | 1.82% | 57.87% | 156.05% |
| product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 540 | 1,080 | 59.15% | 40.85% | 81.69% |
| product_variable | all_release_sources | climatology | Q | Q_qc1 | 1,322 | 782 | 540 | 1,080 | 59.15% | 40.85% | 81.69% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc1 | 1,322 | 804 | 518 | 1,036 | 60.82% | 39.18% | 78.37% |
| product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,322 | 804 | 518 | 1,036 | 60.82% | 39.18% | 78.37% |
| product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,322 | 1,298 | 24 | 48 | 98.18% | 1.82% | 3.63% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc1 | 1,322 | 1,298 | 24 | 48 | 98.18% | 1.82% | 3.63% |

## Stage Effectiveness

| temporal resolution | variable | qc stage | flag variable | n total | good count | bad count | not checked count | missing count | good rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| climatology | Q | log_iqr | Q_qc2 | 1,322 | 0 | 0 | 533 | 789 | 0% | 59.68% | 59.68% |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 1,322 | 0 | 0 | 533 | 789 | 0% | 59.68% | 59.68% |
| climatology | SSL | log_iqr | SSL_qc2 | 1,322 | 0 | 0 | 533 | 789 | 0% | 59.68% | 59.68% |
| climatology | SSC | log_iqr | SSC_qc2 | 1,322 | 24 | 0 | 533 | 765 | 1.82% | 57.87% | 57.87% |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 1,322 | 24 | 0 | 533 | 765 | 1.82% | 57.87% | 57.87% |
| climatology | Q | physical_plausibility | Q_qc1 | 1,322 | 782 | 0 | 0 | 540 | 59.15% | 40.85% | 40.85% |
| climatology | SSC | physical_plausibility | SSC_qc1 | 1,322 | 804 | 0 | 0 | 518 | 60.82% | 39.18% | 39.18% |
| climatology | SSL | physical_plausibility | SSL_qc1 | 1,322 | 1,298 | 0 | 0 | 24 | 98.18% | 1.82% | 1.82% |

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| all_release_sources | all | final | final | climatology | Q | Q_flag | 9 | missing | 540 | 40.85% | 1,322 |
| all_release_sources | all | stage | physical_plausibility | climatology | Q | Q_qc1 | 0 | pass | 782 | 59.15% | 1,322 |
| all_release_sources | all | stage | physical_plausibility | climatology | Q | Q_qc1 | 9 | missing | 540 | 40.85% | 1,322 |
| all_release_sources | all | stage | log_iqr | climatology | Q | Q_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| all_release_sources | all | stage | log_iqr | climatology | Q | Q_qc2 | 9 | missing | 789 | 59.68% | 1,322 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 0 | good | 787 | 59.53% | 1,322 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 1 | estimated | 17 | 1.29% | 1,322 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 9 | missing | 518 | 39.18% | 1,322 |
| all_release_sources | all | stage | physical_plausibility | climatology | SSC | SSC_qc1 | 0 | pass | 804 | 60.82% | 1,322 |
| all_release_sources | all | stage | physical_plausibility | climatology | SSC | SSC_qc1 | 9 | missing | 518 | 39.18% | 1,322 |
| all_release_sources | all | stage | log_iqr | climatology | SSC | SSC_qc2 | 0 | pass | 24 | 1.82% | 1,322 |
| all_release_sources | all | stage | log_iqr | climatology | SSC | SSC_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| all_release_sources | all | stage | log_iqr | climatology | SSC | SSC_qc2 | 9 | missing | 765 | 57.87% | 1,322 |
| all_release_sources | all | stage | ssc_q_consistency | climatology | SSC | SSC_qc3 | 0 | pass | 24 | 1.82% | 1,322 |
| all_release_sources | all | stage | ssc_q_consistency | climatology | SSC | SSC_qc3 | 8 | not_checked | 533 | 40.32% | 1,322 |

_Showing first 16 of 25 rows._

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| final | final | climatology | Q | Q_flag | 9 | missing | 540 | 40.85% | 1,322 |
| stage | physical_plausibility | climatology | Q | Q_qc1 | 0 | pass | 782 | 59.15% | 1,322 |
| stage | physical_plausibility | climatology | Q | Q_qc1 | 9 | missing | 540 | 40.85% | 1,322 |
| stage | log_iqr | climatology | Q | Q_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| stage | log_iqr | climatology | Q | Q_qc2 | 9 | missing | 789 | 59.68% | 1,322 |
| final | final | climatology | SSC | SSC_flag | 0 | good | 787 | 59.53% | 1,322 |
| final | final | climatology | SSC | SSC_flag | 1 | estimated | 17 | 1.29% | 1,322 |
| final | final | climatology | SSC | SSC_flag | 9 | missing | 518 | 39.18% | 1,322 |
| stage | physical_plausibility | climatology | SSC | SSC_qc1 | 0 | pass | 804 | 60.82% | 1,322 |
| stage | physical_plausibility | climatology | SSC | SSC_qc1 | 9 | missing | 518 | 39.18% | 1,322 |
| stage | log_iqr | climatology | SSC | SSC_qc2 | 0 | pass | 24 | 1.82% | 1,322 |
| stage | log_iqr | climatology | SSC | SSC_qc2 | 8 | not_checked | 533 | 40.32% | 1,322 |
| stage | log_iqr | climatology | SSC | SSC_qc2 | 9 | missing | 765 | 57.87% | 1,322 |
| stage | ssc_q_consistency | climatology | SSC | SSC_qc3 | 0 | pass | 24 | 1.82% | 1,322 |
| stage | ssc_q_consistency | climatology | SSC | SSC_qc3 | 8 | not_checked | 533 | 40.32% | 1,322 |

_Showing first 16 of 25 rows._

## Flag Counts by Variable

_No rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,322 | 0 | 0 | 0 | 0 | 533 | 789 | 0 | 789 | 2,111 | 0% | 0% | 0% | 0% | 40.32% | 59.68% | 0% | 59.68% | 159.68% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,322 | 0 | 0 | 0 | 0 | 533 | 789 | 0 | 789 | 2,111 | 0% | 0% | 0% | 0% | 40.32% | 59.68% | 0% | 59.68% | 159.68% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,322 | 0 | 0 | 0 | 0 | 533 | 789 | 0 | 789 | 2,111 | 0% | 0% | 0% | 0% | 40.32% | 59.68% | 0% | 59.68% | 159.68% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,322 | 24 | 0 | 0 | 0 | 533 | 765 | 24 | 765 | 2,063 | 1.82% | 0% | 0% | 0% | 40.32% | 57.87% | 1.82% | 57.87% | 156.05% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,322 | 24 | 0 | 0 | 0 | 533 | 765 | 24 | 765 | 2,063 | 1.82% | 0% | 0% | 0% | 40.32% | 57.87% | 1.82% | 57.87% | 156.05% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 0 | 0 | 0 | 0 | 540 | 782 | 540 | 1,080 | 59.15% | 0% | 0% | 0% | 0% | 40.85% | 59.15% | 40.85% | 81.69% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc1 | 1,322 | 782 | 0 | 0 | 0 | 0 | 540 | 782 | 540 | 1,080 | 59.15% | 0% | 0% | 0% | 0% | 40.85% | 59.15% | 40.85% | 81.69% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc1 | 1,322 | 804 | 0 | 0 | 0 | 0 | 518 | 804 | 518 | 1,036 | 60.82% | 0% | 0% | 0% | 0% | 39.18% | 60.82% | 39.18% | 78.37% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,322 | 787 | 17 | 0 | 0 | 0 | 518 | 804 | 518 | 1,036 | 59.53% | 1.29% | 0% | 0% | 0% | 39.18% | 60.82% | 39.18% | 78.37% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,322 | 1,298 | 0 | 0 | 0 | 0 | 24 | 1,298 | 24 | 48 | 98.18% | 0% | 0% | 0% | 0% | 1.82% | 98.18% | 1.82% | 3.63% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc1 | 1,322 | 1,298 | 0 | 0 | 0 | 0 | 24 | 1,298 | 24 | 48 | 98.18% | 0% | 0% | 0% | 0% | 1.82% | 98.18% | 1.82% | 3.63% |

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 3,966 | 7,932 | 50% |
| all | climatology | SSC | 5,288 | 14,542 | 36.36% |
| all | climatology | SSL | 5,288 | 10,576 | 50% |

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.
- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.

## Figures

- `fig_qc_flag_by_source_type.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_flag_by_source_type.png`
- `fig_qc_flag_distribution.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_flag_distribution.png`
- `fig_qc_health_by_resolution.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_health_by_resolution.png`
- `fig_qc_missing_trends.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_stage_summary.png`
- `fig_qc_top_problem_clusters.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_top_problem_clusters.png`
- `fig_qc_top_problem_sources.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/climatology/fig_qc_yearly_problem_trends.png`
