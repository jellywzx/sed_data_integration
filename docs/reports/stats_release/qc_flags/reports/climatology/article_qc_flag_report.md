# Climatology QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables/climatology`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 14,971
- Final flag rows: 7
- Stage flag rows: 19
- Usable flag count from health KPIs: 5,851
- Problem flag count from health KPIs: 6,408
- Stage-effectiveness rows available: 8

## Flag Schema

| product | flag variable | flag value | flag meaning | long name |
|---|---|---|---|---|
| climatology | Q_flag | 0 | good | quality flag for river discharge |
| climatology | Q_flag | 1 | derived | quality flag for river discharge |
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
| climatology | SSC_flag | 1 | derived | quality flag for suspended sediment concentration |
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
| climatology | SSL | SSL_flag | 0 | good | 1,337 | 98.24% | 1,361 |
| climatology | Q | Q_flag | 0 | good | 782 | 57.46% | 1,361 |
| climatology | SSC | SSC_flag | 0 | good | 759 | 55.77% | 1,361 |
| climatology | Q | Q_flag | 9 | missing | 579 | 42.54% | 1,361 |
| climatology | SSC | SSC_flag | 9 | missing | 555 | 40.78% | 1,361 |
| climatology | SSC | SSC_flag | 1 | derived | 47 | 3.45% | 1,361 |
| climatology | SSL | SSL_flag | 9 | missing | 24 | 1.76% | 1,361 |

## Stage Flag Summary

| temporal resolution | variable | qc stage | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|
| climatology | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 1,337 | 98.24% | 1,361 |
| climatology | Q | log_iqr | Q_qc2 | 9 | missing | 828 | 60.84% | 1,361 |
| climatology | SSL | log_iqr | SSL_qc2 | 9 | missing | 828 | 60.84% | 1,361 |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 828 | 60.84% | 1,361 |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 804 | 59.07% | 1,361 |
| climatology | SSC | log_iqr | SSC_qc2 | 9 | missing | 804 | 59.07% | 1,361 |
| climatology | Q | physical_plausibility | Q_qc1 | 0 | pass | 782 | 57.46% | 1,361 |
| climatology | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 776 | 57.02% | 1,361 |
| climatology | Q | physical_plausibility | Q_qc1 | 9 | missing | 579 | 42.54% | 1,361 |
| climatology | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 555 | 40.78% | 1,361 |
| climatology | Q | log_iqr | Q_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| climatology | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 533 | 39.16% | 1,361 |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 533 | 39.16% | 1,361 |
| climatology | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| climatology | SSC | physical_plausibility | SSC_qc1 | 1 | estimated | 30 | 2.20% | 1,361 |
| climatology | SSC | log_iqr | SSC_qc2 | 0 | pass | 24 | 1.76% | 1,361 |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 24 | 1.76% | 1,361 |
| climatology | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 24 | 1.76% | 1,361 |

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 0 | 828 | 828 | 0% | 0% | 60.84% | 60.84% |
| climatology | SSL | SSL_qc3 | 1,361 | 0 | 0 | 0 | 828 | 828 | 0% | 0% | 60.84% | 60.84% |
| climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 0 | 828 | 828 | 0% | 0% | 60.84% | 60.84% |
| climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 24 | 804 | 804 | 1.76% | 1.76% | 59.07% | 59.07% |
| climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 24 | 804 | 804 | 1.76% | 1.76% | 59.07% | 59.07% |
| climatology | Q | Q_qc1 | 1,361 | 782 | 0 | 782 | 579 | 579 | 57.46% | 57.46% | 42.54% | 42.54% |
| climatology | Q | Q_flag | 1,361 | 782 | 0 | 782 | 579 | 579 | 57.46% | 57.46% | 42.54% | 42.54% |
| climatology | SSC | SSC_flag | 1,361 | 759 | 0 | 759 | 555 | 555 | 55.77% | 55.77% | 40.78% | 40.78% |
| climatology | SSC | SSC_qc1 | 1,361 | 776 | 30 | 806 | 555 | 555 | 57.02% | 59.22% | 40.78% | 40.78% |
| climatology | SSL | SSL_qc1 | 1,361 | 1,337 | 0 | 1,337 | 24 | 24 | 98.24% | 98.24% | 1.76% | 1.76% |
| climatology | SSL | SSL_flag | 1,361 | 1,337 | 0 | 1,337 | 24 | 24 | 98.24% | 98.24% | 1.76% | 1.76% |

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,361 | 0 | 828 | 2,189 | 0% | 60.84% | 160.84% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,361 | 0 | 828 | 2,189 | 0% | 60.84% | 160.84% |
| product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,361 | 0 | 828 | 2,189 | 0% | 60.84% | 160.84% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,361 | 24 | 804 | 2,141 | 1.76% | 59.07% | 157.31% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,361 | 24 | 804 | 2,141 | 1.76% | 59.07% | 157.31% |
| product_variable | all_release_sources | climatology | Q | Q_qc1 | 1,361 | 782 | 579 | 1,158 | 57.46% | 42.54% | 85.08% |
| product_variable | all_release_sources | climatology | Q | Q_flag | 1,361 | 782 | 579 | 1,158 | 57.46% | 42.54% | 85.08% |
| product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,361 | 759 | 555 | 1,110 | 55.77% | 40.78% | 81.56% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc1 | 1,361 | 806 | 555 | 1,110 | 59.22% | 40.78% | 81.56% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc1 | 1,361 | 1,337 | 24 | 48 | 98.24% | 1.76% | 3.53% |
| product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,361 | 1,337 | 24 | 48 | 98.24% | 1.76% | 3.53% |

## Stage Effectiveness

| temporal resolution | variable | qc stage | flag variable | n total | good count | bad count | not checked count | missing count | good rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| climatology | Q | log_iqr | Q_qc2 | 1,361 | 0 | 0 | 533 | 828 | 0% | 60.84% | 60.84% |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 1,361 | 0 | 0 | 533 | 828 | 0% | 60.84% | 60.84% |
| climatology | SSL | log_iqr | SSL_qc2 | 1,361 | 0 | 0 | 533 | 828 | 0% | 60.84% | 60.84% |
| climatology | SSC | log_iqr | SSC_qc2 | 1,361 | 24 | 0 | 533 | 804 | 1.76% | 59.07% | 59.07% |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 1,361 | 24 | 0 | 533 | 804 | 1.76% | 59.07% | 59.07% |
| climatology | Q | physical_plausibility | Q_qc1 | 1,361 | 782 | 0 | 0 | 579 | 57.46% | 42.54% | 42.54% |
| climatology | SSC | physical_plausibility | SSC_qc1 | 1,361 | 776 | 0 | 0 | 555 | 57.02% | 40.78% | 40.78% |
| climatology | SSL | physical_plausibility | SSL_qc1 | 1,361 | 1,337 | 0 | 0 | 24 | 98.24% | 1.76% | 1.76% |

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | climatology | Q | Q_flag | 0 | good | 782 | 57.46% | 1,361 |
| all_release_sources | all | final | final | climatology | Q | Q_flag | 9 | missing | 579 | 42.54% | 1,361 |
| all_release_sources | all | stage | physical_plausibility | climatology | Q | Q_qc1 | 0 | pass | 782 | 57.46% | 1,361 |
| all_release_sources | all | stage | physical_plausibility | climatology | Q | Q_qc1 | 9 | missing | 579 | 42.54% | 1,361 |
| all_release_sources | all | stage | log_iqr | climatology | Q | Q_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| all_release_sources | all | stage | log_iqr | climatology | Q | Q_qc2 | 9 | missing | 828 | 60.84% | 1,361 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 0 | good | 759 | 55.77% | 1,361 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 1 | derived | 47 | 3.45% | 1,361 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 9 | missing | 555 | 40.78% | 1,361 |
| all_release_sources | all | stage | physical_plausibility | climatology | SSC | SSC_qc1 | 0 | pass | 776 | 57.02% | 1,361 |
| all_release_sources | all | stage | physical_plausibility | climatology | SSC | SSC_qc1 | 1 | estimated | 30 | 2.20% | 1,361 |
| all_release_sources | all | stage | physical_plausibility | climatology | SSC | SSC_qc1 | 9 | missing | 555 | 40.78% | 1,361 |
| all_release_sources | all | stage | log_iqr | climatology | SSC | SSC_qc2 | 0 | pass | 24 | 1.76% | 1,361 |
| all_release_sources | all | stage | log_iqr | climatology | SSC | SSC_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| all_release_sources | all | stage | log_iqr | climatology | SSC | SSC_qc2 | 9 | missing | 804 | 59.07% | 1,361 |
| all_release_sources | all | stage | ssc_q_consistency | climatology | SSC | SSC_qc3 | 0 | pass | 24 | 1.76% | 1,361 |

_Showing first 16 of 26 rows._

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | climatology | Q | Q_flag | 0 | good | 782 | 57.46% | 1,361 |
| final | final | climatology | Q | Q_flag | 9 | missing | 579 | 42.54% | 1,361 |
| stage | physical_plausibility | climatology | Q | Q_qc1 | 0 | pass | 782 | 57.46% | 1,361 |
| stage | physical_plausibility | climatology | Q | Q_qc1 | 9 | missing | 579 | 42.54% | 1,361 |
| stage | log_iqr | climatology | Q | Q_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| stage | log_iqr | climatology | Q | Q_qc2 | 9 | missing | 828 | 60.84% | 1,361 |
| final | final | climatology | SSC | SSC_flag | 0 | good | 759 | 55.77% | 1,361 |
| final | final | climatology | SSC | SSC_flag | 1 | derived | 47 | 3.45% | 1,361 |
| final | final | climatology | SSC | SSC_flag | 9 | missing | 555 | 40.78% | 1,361 |
| stage | physical_plausibility | climatology | SSC | SSC_qc1 | 0 | pass | 776 | 57.02% | 1,361 |
| stage | physical_plausibility | climatology | SSC | SSC_qc1 | 1 | estimated | 30 | 2.20% | 1,361 |
| stage | physical_plausibility | climatology | SSC | SSC_qc1 | 9 | missing | 555 | 40.78% | 1,361 |
| stage | log_iqr | climatology | SSC | SSC_qc2 | 0 | pass | 24 | 1.76% | 1,361 |
| stage | log_iqr | climatology | SSC | SSC_qc2 | 8 | not_checked | 533 | 39.16% | 1,361 |
| stage | log_iqr | climatology | SSC | SSC_qc2 | 9 | missing | 804 | 59.07% | 1,361 |
| stage | ssc_q_consistency | climatology | SSC | SSC_qc3 | 0 | pass | 24 | 1.76% | 1,361 |

_Showing first 16 of 26 rows._

## Flag Counts by Variable

_No rows._

## Problem Stations

| station uid | station reference id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 0 | 0 | 533 | 804 | 24 | 804 | 2,141 | 1.76% | 0% | 0% | 0% | 39.16% | 59.07% | 1.76% | 59.07% | 157.31% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 0 | 0 | 533 | 804 | 24 | 804 | 2,141 | 1.76% | 0% | 0% | 0% | 39.16% | 59.07% | 1.76% | 59.07% | 157.31% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc1 | 1,361 | 782 | 0 | 0 | 0 | 0 | 579 | 782 | 579 | 1,158 | 57.46% | 0% | 0% | 0% | 0% | 42.54% | 57.46% | 42.54% | 85.08% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_flag | 1,361 | 782 | 0 | 0 | 0 | 0 | 579 | 782 | 579 | 1,158 | 57.46% | 0% | 0% | 0% | 0% | 42.54% | 57.46% | 42.54% | 85.08% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,361 | 759 | 0 | 0 | 0 | 0 | 555 | 759 | 555 | 1,110 | 55.77% | 0% | 0% | 0% | 0% | 40.78% | 55.77% | 40.78% | 81.56% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc1 | 1,361 | 776 | 30 | 0 | 0 | 0 | 555 | 806 | 555 | 1,110 | 57.02% | 2.20% | 0% | 0% | 0% | 40.78% | 59.22% | 40.78% | 81.56% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc1 | 1,361 | 1,337 | 0 | 0 | 0 | 0 | 24 | 1,337 | 24 | 48 | 98.24% | 0% | 0% | 0% | 0% | 1.76% | 98.24% | 1.76% | 3.53% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,361 | 1,337 | 0 | 0 | 0 | 0 | 24 | 1,337 | 24 | 48 | 98.24% | 0% | 0% | 0% | 0% | 1.76% | 98.24% | 1.76% | 3.53% |

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 4,083 | 8,166 | 50% |
| all | climatology | SSC | 5,444 | 16,332 | 33.33% |
| all | climatology | SSL | 5,444 | 10,888 | 50% |

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.
- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.

## Figures

- `fig_qc_flag_by_source_type.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_flag_by_source_type.png`
- `fig_qc_flag_distribution.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_flag_distribution.png`
- `fig_qc_health_by_resolution.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_health_by_resolution.png`
- `fig_qc_missing_trends.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_stage_summary.png`
- `fig_qc_top_problem_reference_stations.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_top_problem_reference_stations.png`
- `fig_qc_top_problem_sources.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_yearly_problem_trends.png`
