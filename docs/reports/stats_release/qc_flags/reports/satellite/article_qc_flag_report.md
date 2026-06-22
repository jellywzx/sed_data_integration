# Satellite QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/qc_flags/tables/satellite`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 49,519,383
- Final flag rows: 9
- Stage flag rows: 0
- Usable flag count from health KPIs: 1,591,144
- Problem flag count from health KPIs: 47,928,239
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
| satellite | Q | Q_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |
| satellite | SSL | SSL_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |
| satellite | SSC | SSC_flag | 9 | missing | 15,160,652 | 91.85% | 16,506,461 |
| satellite | SSC | SSC_flag | 0 | good | 1,326,713 | 8.04% | 16,506,461 |
| satellite | SSL | SSL_flag | 0 | good | 132,259 | 0.80% | 16,506,461 |
| satellite | Q | Q_flag | 0 | good | 132,172 | 0.80% | 16,506,461 |
| satellite | SSC | SSC_flag | 2 | suspect | 19,096 | 0.12% | 16,506,461 |
| satellite | Q | Q_flag | 2 | suspect | 1,207 | 0.01% | 16,506,461 |
| satellite | SSL | SSL_flag | 2 | suspect | 1,120 | 0.01% | 16,506,461 |

## Stage Flag Summary

_No rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 16,506,461 | 132,172 | 0 | 132,172 | 16,374,289 | 16,373,082 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSL | SSL_flag | 16,506,461 | 132,259 | 0 | 132,259 | 16,374,202 | 16,373,082 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSC | SSC_flag | 16,506,461 | 1,326,713 | 0 | 1,326,713 | 15,179,748 | 15,160,652 | 8.04% | 8.04% | 91.96% | 91.85% |

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | satellite | Q | Q_flag | 16,506,461 | 132,172 | 16,374,289 | 32,747,371 | 0.80% | 99.20% | 198.39% |
| product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,506,461 | 132,259 | 16,374,202 | 32,747,284 | 0.80% | 99.20% | 198.39% |
| product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,506,461 | 1,326,713 | 15,179,748 | 30,340,400 | 8.04% | 91.96% | 183.81% |

## Stage Effectiveness

_No rows._

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | satellite | Q | Q_flag | 0 | good | 132,172 | 0.80% | 16,506,461 |
| all_release_sources | all | final | final | satellite | Q | Q_flag | 2 | suspect | 1,207 | 0.01% | 16,506,461 |
| all_release_sources | all | final | final | satellite | Q | Q_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |
| all_release_sources | all | final | final | satellite | SSC | SSC_flag | 0 | good | 1,326,713 | 8.04% | 16,506,461 |
| all_release_sources | all | final | final | satellite | SSC | SSC_flag | 2 | suspect | 19,096 | 0.12% | 16,506,461 |
| all_release_sources | all | final | final | satellite | SSC | SSC_flag | 9 | missing | 15,160,652 | 91.85% | 16,506,461 |
| all_release_sources | all | final | final | satellite | SSL | SSL_flag | 0 | good | 132,259 | 0.80% | 16,506,461 |
| all_release_sources | all | final | final | satellite | SSL | SSL_flag | 2 | suspect | 1,120 | 0.01% | 16,506,461 |
| all_release_sources | all | final | final | satellite | SSL | SSL_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | satellite | Q | Q_flag | 0 | good | 132,172 | 0.80% | 16,506,461 |
| final | final | satellite | Q | Q_flag | 2 | suspect | 1,207 | 0.01% | 16,506,461 |
| final | final | satellite | Q | Q_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |
| final | final | satellite | SSC | SSC_flag | 0 | good | 1,326,713 | 8.04% | 16,506,461 |
| final | final | satellite | SSC | SSC_flag | 2 | suspect | 19,096 | 0.12% | 16,506,461 |
| final | final | satellite | SSC | SSC_flag | 9 | missing | 15,160,652 | 91.85% | 16,506,461 |
| final | final | satellite | SSL | SSL_flag | 0 | good | 132,259 | 0.80% | 16,506,461 |
| final | final | satellite | SSL | SSL_flag | 2 | suspect | 1,120 | 0.01% | 16,506,461 |
| final | final | satellite | SSL | SSL_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |

## Flag Counts by Variable

_No rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | satellite | Q | Q_flag | 16,506,461 | 132,172 | 0 | 1,207 | 0 | 0 | 16,373,082 | 132,172 | 16,374,289 | 32,747,371 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.39% |
|  |  | product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,506,461 | 132,259 | 0 | 1,120 | 0 | 0 | 16,373,082 | 132,259 | 16,374,202 | 32,747,284 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.39% |
|  |  | product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,506,461 | 1,326,713 | 0 | 19,096 | 0 | 0 | 15,160,652 | 1,326,713 | 15,179,748 | 30,340,400 | 8.04% | 0% | 0.12% | 0% | 0% | 91.85% | 8.04% | 91.96% | 183.81% |

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | satellite | Q | 16,506,461 | 49,519,383 | 33.33% |
| all | satellite | SSC | 16,506,461 | 49,519,383 | 33.33% |
| all | satellite | SSL | 16,506,461 | 49,519,383 | 33.33% |

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.
- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.

## Figures

- `fig_qc_flag_by_source_type.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_flag_by_source_type.png`
- `fig_qc_flag_distribution.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_flag_distribution.png`
- `fig_qc_health_by_resolution.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_health_by_resolution.png`
- `fig_qc_missing_trends.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_stage_summary.png`
- `fig_qc_top_problem_clusters.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_top_problem_clusters.png`
- `fig_qc_top_problem_sources.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/satellite/fig_qc_yearly_problem_trends.png`
