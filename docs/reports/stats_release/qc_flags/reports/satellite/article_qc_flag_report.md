# Satellite QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables/satellite`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 49,434,828
- Final flag rows: 9
- Stage flag rows: 0
- Usable flag count from health KPIs: 15,386,399
- Problem flag count from health KPIs: 34,048,429
- Stage-effectiveness rows available: 0

## Flag Schema

| product | flag variable | flag value | flag meaning | long name |
|---|---|---|---|---|
| satellite | Q_flag | 0 | good |  |
| satellite | Q_flag | 1 | estimated |  |
| satellite | Q_flag | 2 | suspect |  |
| satellite | Q_flag | 3 | bad |  |
| satellite | Q_flag | 9 | missing |  |
| satellite | SSC_flag | 0 | good |  |
| satellite | SSC_flag | 1 | estimated |  |
| satellite | SSC_flag | 2 | suspect |  |
| satellite | SSC_flag | 3 | bad |  |
| satellite | SSC_flag | 9 | missing |  |
| satellite | SSL_flag | 0 | good |  |
| satellite | SSL_flag | 1 | estimated |  |
| satellite | SSL_flag | 2 | suspect |  |
| satellite | SSL_flag | 3 | bad |  |
| satellite | SSL_flag | 9 | missing |  |

## Final Flag Summary

| temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | SSL | SSL_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | SSC | SSC_flag | 0 | good | 15,121,092 | 91.76% | 16,478,276 |
| satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| satellite | Q | Q_flag | 2 | suspect | 1,209 | 0.01% | 16,478,276 |
| satellite | SSL | SSL_flag | 2 | suspect | 1,130 | 0.01% | 16,478,276 |

## Stage Flag Summary

_No rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 132,614 | 16,345,662 | 16,344,453 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 132,693 | 16,345,583 | 16,344,453 | 0.81% | 0.81% | 99.19% | 99.19% |
| satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 15,121,092 | 1,357,184 | 960,798 | 91.76% | 91.76% | 8.24% | 5.83% |

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 99.20% | 198.38% |
| product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 99.19% | 198.38% |
| product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 1,357,184 | 2,317,982 | 91.76% | 8.24% | 14.07% |

## Stage Effectiveness

_No rows._

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| all_release_sources | all | final | final | satellite | Q | Q_flag | 2 | suspect | 1,209 | 0.01% | 16,478,276 |
| all_release_sources | all | final | final | satellite | Q | Q_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| all_release_sources | all | final | final | satellite | SSC | SSC_flag | 0 | good | 15,121,092 | 91.76% | 16,478,276 |
| all_release_sources | all | final | final | satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| all_release_sources | all | final | final | satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| all_release_sources | all | final | final | satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| all_release_sources | all | final | final | satellite | SSL | SSL_flag | 2 | suspect | 1,130 | 0.01% | 16,478,276 |
| all_release_sources | all | final | final | satellite | SSL | SSL_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| final | final | satellite | Q | Q_flag | 2 | suspect | 1,209 | 0.01% | 16,478,276 |
| final | final | satellite | Q | Q_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| final | final | satellite | SSC | SSC_flag | 0 | good | 15,121,092 | 91.76% | 16,478,276 |
| final | final | satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| final | final | satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| final | final | satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| final | final | satellite | SSL | SSL_flag | 2 | suspect | 1,130 | 0.01% | 16,478,276 |
| final | final | satellite | SSL | SSL_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |

## Flag Counts by Variable

_No rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 1,209 | 0 | 0 | 16,344,453 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.38% |
|  |  | product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 1,130 | 0 | 0 | 16,344,453 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 0% | 0.01% | 0% | 0% | 99.19% | 0.81% | 99.19% | 198.38% |
|  |  | product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 396,386 | 0 | 0 | 960,798 | 15,121,092 | 1,357,184 | 2,317,982 | 91.76% | 0% | 2.41% | 0% | 0% | 5.83% | 91.76% | 8.24% | 14.07% |

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | satellite | Q | 16,478,276 | 49,434,828 | 33.33% |
| all | satellite | SSC | 16,478,276 | 49,434,828 | 33.33% |
| all | satellite | SSL | 16,478,276 | 49,434,828 | 33.33% |

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.
- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.

## Figures

- `fig_qc_flag_by_source_type.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_flag_by_source_type.png`
- `fig_qc_flag_distribution.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_flag_distribution.png`
- `fig_qc_health_by_resolution.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_health_by_resolution.png`
- `fig_qc_missing_trends.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_stage_summary.png`
- `fig_qc_top_problem_clusters.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_top_problem_clusters.png`
- `fig_qc_top_problem_sources.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `output_other/stats_release/qc_flags/figures/satellite/fig_qc_yearly_problem_trends.png`
