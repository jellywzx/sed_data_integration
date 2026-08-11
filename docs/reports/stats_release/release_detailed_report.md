# Sediment Reference Release Detailed Statistics Report

## Run Identity

- Release package: `output/sed_reference_release`
- Stats output: `output_other/stats_release`
- Run started UTC: `2026-08-11T05:11:37+00:00`
- Run finished UTC: `2026-08-11T05:12:58+00:00`
- Clean output before run: `True`
- Release fingerprint: `72831f8bed7a978d19126b454714f4c2611d8a1f4217069ccb24c4b819175c17`
- Stats script fingerprint: `255922d041f8cb96a00c132d168c9fe62dd973913279bf6388bcc66428778c03`

## Run Status

- Modules requested: 8
- Module failures: 0
- Missing release-capable parity outputs: 0
- Unsupported release-only parity outputs: 7

| module | return code | started utc | finished utc |
|---|---|---|---|
| inventory | 0 | 2026-08-11T05:11:37+00:00 | 2026-08-11T05:11:42+00:00 |
| spatial | 0 | 2026-08-11T05:11:42+00:00 | 2026-08-11T05:12:06+00:00 |
| temporal | 0 | 2026-08-11T05:12:06+00:00 | 2026-08-11T05:12:25+00:00 |
| source_dataset_layers | 0 | 2026-08-11T05:12:25+00:00 | 2026-08-11T05:12:27+00:00 |
| source_contribution | 0 | 2026-08-11T05:12:27+00:00 | 2026-08-11T05:12:35+00:00 |
| basin_diagnostics | 0 | 2026-08-11T05:12:35+00:00 | 2026-08-11T05:12:38+00:00 |
| variable_summary | 0 | 2026-08-11T05:12:38+00:00 | 2026-08-11T05:12:52+00:00 |
| qc_flags | 0 | 2026-08-11T05:12:52+00:00 | 2026-08-11T05:12:58+00:00 |

## Parity Manifest Summary

| status | count |
|---|---|
| generated | 253 |
| unsupported_release_only | 7 |

## Detailed Module Reports

| module | report | exists | size bytes | description |
|---|---|---|---|---|
| inventory | inventory/reports/release_inventory_stats.md | 1 | 7,151 | Release inventory and health report |
| spatial | spatial/reports/spatial_coverage_stats.md | 1 | 11,025 | Spatial coverage report |
| spatial | spatial/article_spatial_coverage_summary.md | 1 | 7,375 | Article spatial coverage summary |
| temporal | temporal/reports/temporal_coverage_stats.md | 1 | 8,950 | Temporal coverage report |
| temporal | temporal/article_temporal_coverage_report.md | 1 | 9,088 | Article temporal coverage report |
| source_dataset_layers | source_dataset_layers/reports/source_dataset_layers.md | 1 | 5,712 | Source dataset layer report |
| source_contribution | source_contribution/reports/source_contribution_report.md | 1 | 20,290 | Source contribution report |
| basin_diagnostics | basin_diagnostics/spatial_match_error_detailed_report.md | 1 | 14,865 | Basin matching detailed report |
| variable_summary | variable_summary/variable_coverage_results_report_ESSD.md | 1 | 16,412 | Variable coverage report |
| qc_flags | qc_flags/article_qc_flag_report.md | 1 | 26,338 | QC flag report |

## Release Risks and QA Signals

- Inventory path-leak fields with host-local paths: 2
- NetCDF metadata dimensions with inactive entries: 0
- Validation/file-existence contradictions: 0
- Unresolved basin rows: 1,739
- Records affected by unresolved basin rows: 812,801
- Resolved basin point-flag anomalies: 179
- Satellite source-variable rows with less than 1% present values: 4
- Sparse time axes: annual, daily, monthly

## Inventory Path-Leak Fields

Raw examples stay in `inventory/tables/path_leaks.csv`; this report does not echo local machine paths.

| product | layer | field | n values | absolute path count | local path count |
|---|---|---|---|---|---|
| source_station_catalog | csv | source_station_paths | 7,717 | 7,717 | 7,717 |
| master_nc | netcdf | source_station_paths | 7,716 | 7,716 | 7,716 |

## Inactive Metadata Consistency

| entity | nc dimension | nc unique | catalog rows | catalog unique | used unique | inactive nc entries |
|---|---|---|---|---|---|---|
| station_uid | 7,379 | 7,379 | 7,463 | 7,379 | 7,379 | 0 |
| source_station_uid | 7,716 | 7,716 | 7,717 | 7,716 | 7,716 | 0 |

## Top Unresolved Basin Sources

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 889 | 295 | 1,690,433 | 543,790 | 33.18% | 32.17% |
| HYDAT | 548 | 124 | 676,024 | 182,255 | 22.63% | 26.96% |
| EUSEDcollab | 244 | 134 | 66,637 | 54,289 | 54.92% | 81.47% |
| GFQA_v2 | 5,583 | 1,160 | 236,513 | 24,906 | 20.78% | 10.53% |
| Robotham | 3 | 3 | 3,432 | 3,432 | 100% | 100% |
| HYBAM | 12 | 4 | 9,404 | 2,392 | 33.33% | 25.44% |
| Eurasian_River | 17 | 6 | 3,263 | 1,239 | 35.29% | 37.97% |
| NERC | 4 | 3 | 624 | 465 | 75% | 74.52% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| GloRiSe | 79 | 8 | 649 | 9 | 10.13% | 1.39% |

_Showing first 10 of 18 rows._

## Satellite Variable Coverage Watchlist

| source name | variable | n records | n present | present percent | usable percent |
|---|---|---|---|---|---|
| RiverSed | SSL | 14,199,854 | 0 | 0% | 0% |
| RiverSed | Q | 14,199,854 | 0 | 0% | 0% |
| GSED | SSL | 2,144,599 | 0 | 0% | 0% |
| GSED | Q | 2,144,599 | 0 | 0% | 0% |

## How to Read These Outputs

- Per-module reports are the authoritative narrative summaries; CSV tables remain the reproducible data source.
- `unsupported_release_only` means the legacy output requires non-release pipeline intermediates and is intentionally not recreated.
- Release-only reports do not change any dataset values or basin statuses; they expose QA priorities for the next release build.
