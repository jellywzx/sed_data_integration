# Climatology QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables/climatology`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 3,966
- Final flag rows: 7
- Stage flag rows: 0
- Usable flag count from health KPIs: 2,884
- Problem flag count from health KPIs: 1,082
- Stage-effectiveness rows available: 0

## Flag Schema

| product | flag variable | flag value | flag meaning | long name |
|---|---|---|---|---|
| climatology | Q_flag | 0 | good | quality flag for river discharge |
| climatology | Q_flag | 1 | estimated | quality flag for river discharge |
| climatology | Q_flag | 2 | suspect | quality flag for river discharge |
| climatology | Q_flag | 3 | bad | quality flag for river discharge |
| climatology | Q_flag | 9 | missing | quality flag for river discharge |
| climatology | SSC_flag | 0 | good | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 1 | estimated | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 2 | suspect | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 3 | bad | quality flag for suspended sediment concentration |
| climatology | SSC_flag | 9 | missing | quality flag for suspended sediment concentration |
| climatology | SSL_flag | 0 | good | quality flag for suspended sediment load |
| climatology | SSL_flag | 1 | estimated | quality flag for suspended sediment load |
| climatology | SSL_flag | 2 | suspect | quality flag for suspended sediment load |
| climatology | SSL_flag | 3 | bad | quality flag for suspended sediment load |
| climatology | SSL_flag | 9 | missing | quality flag for suspended sediment load |

## Final Flag Summary

| temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|
| climatology | SSL | SSL_flag | 0 | good | 1,298 | 98.18% | 1,322 |
| climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| climatology | SSC | SSC_flag | 0 | good | 759 | 57.41% | 1,322 |
| climatology | Q | Q_flag | 9 | missing | 540 | 40.85% | 1,322 |
| climatology | SSC | SSC_flag | 9 | missing | 518 | 39.18% | 1,322 |
| climatology | SSC | SSC_flag | 1 | estimated | 45 | 3.40% | 1,322 |
| climatology | SSL | SSL_flag | 9 | missing | 24 | 1.82% | 1,322 |

## Stage Flag Summary

_No rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| climatology | Q | Q_flag | 1,322 | 782 | 0 | 782 | 540 | 540 | 59.15% | 59.15% | 40.85% | 40.85% |
| climatology | SSC | SSC_flag | 1,322 | 759 | 45 | 804 | 518 | 518 | 57.41% | 60.82% | 39.18% | 39.18% |
| climatology | SSL | SSL_flag | 1,322 | 1,298 | 0 | 1,298 | 24 | 24 | 98.18% | 98.18% | 1.82% | 1.82% |

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 540 | 1,080 | 59.15% | 40.85% | 81.69% |
| product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,322 | 804 | 518 | 1,036 | 60.82% | 39.18% | 78.37% |
| product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,322 | 1,298 | 24 | 48 | 98.18% | 1.82% | 3.63% |

## Stage Effectiveness

_No rows._

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| all_release_sources | all | final | final | climatology | Q | Q_flag | 9 | missing | 540 | 40.85% | 1,322 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 0 | good | 759 | 57.41% | 1,322 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 1 | estimated | 45 | 3.40% | 1,322 |
| all_release_sources | all | final | final | climatology | SSC | SSC_flag | 9 | missing | 518 | 39.18% | 1,322 |
| all_release_sources | all | final | final | climatology | SSL | SSL_flag | 0 | good | 1,298 | 98.18% | 1,322 |
| all_release_sources | all | final | final | climatology | SSL | SSL_flag | 9 | missing | 24 | 1.82% | 1,322 |

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| final | final | climatology | Q | Q_flag | 9 | missing | 540 | 40.85% | 1,322 |
| final | final | climatology | SSC | SSC_flag | 0 | good | 759 | 57.41% | 1,322 |
| final | final | climatology | SSC | SSC_flag | 1 | estimated | 45 | 3.40% | 1,322 |
| final | final | climatology | SSC | SSC_flag | 9 | missing | 518 | 39.18% | 1,322 |
| final | final | climatology | SSL | SSL_flag | 0 | good | 1,298 | 98.18% | 1,322 |
| final | final | climatology | SSL | SSL_flag | 9 | missing | 24 | 1.82% | 1,322 |

## Flag Counts by Variable

_No rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 0 | 0 | 0 | 0 | 540 | 782 | 540 | 1,080 | 59.15% | 0% | 0% | 0% | 0% | 40.85% | 59.15% | 40.85% | 81.69% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,322 | 759 | 45 | 0 | 0 | 0 | 518 | 804 | 518 | 1,036 | 57.41% | 3.40% | 0% | 0% | 0% | 39.18% | 60.82% | 39.18% | 78.37% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,322 | 1,298 | 0 | 0 | 0 | 0 | 24 | 1,298 | 24 | 48 | 98.18% | 0% | 0% | 0% | 0% | 1.82% | 98.18% | 1.82% | 3.63% |

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 1,322 | 2,644 | 50% |
| all | climatology | SSC | 1,322 | 3,966 | 33.33% |
| all | climatology | SSL | 1,322 | 2,644 | 50% |

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
- `fig_qc_top_problem_clusters.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_top_problem_clusters.png`
- `fig_qc_top_problem_sources.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `output_other/stats_release/qc_flags/figures/climatology/fig_qc_yearly_problem_trends.png`
