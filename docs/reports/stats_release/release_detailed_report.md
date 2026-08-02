# Sediment Reference Release Detailed Statistics Report

## Run Identity

- Release package: `output/sed_reference_release`
- Stats output: `output_other/stats_release`
- Run started UTC: `2026-08-01T17:10:53+00:00`
- Run finished UTC: `2026-08-01T17:12:14+00:00`
- Clean output before run: `True`
- Release fingerprint: `a86853e582e3c6ea155a99453a00243ef2f889b4c58b2a4eeda089fa2b9e1107`
- Stats script fingerprint: `004f9adaf6ed337361fbb3f9ca413dacc9fb7266f22756a659561e62a052ed9b`

## Run Status

- Modules requested: 8
- Module failures: 0
- Missing release-capable parity outputs: 0
- Unsupported release-only parity outputs: 7

| module | return code | started utc | finished utc |
|---|---|---|---|
| inventory | 0 | 2026-08-01T17:10:54+00:00 | 2026-08-01T17:11:03+00:00 |
| spatial | 0 | 2026-08-01T17:11:03+00:00 | 2026-08-01T17:11:20+00:00 |
| temporal | 0 | 2026-08-01T17:11:20+00:00 | 2026-08-01T17:11:37+00:00 |
| source_dataset_layers | 0 | 2026-08-01T17:11:37+00:00 | 2026-08-01T17:11:42+00:00 |
| source_contribution | 0 | 2026-08-01T17:11:42+00:00 | 2026-08-01T17:11:51+00:00 |
| basin_diagnostics | 0 | 2026-08-01T17:11:51+00:00 | 2026-08-01T17:11:54+00:00 |
| variable_summary | 0 | 2026-08-01T17:11:54+00:00 | 2026-08-01T17:12:08+00:00 |
| qc_flags | 0 | 2026-08-01T17:12:08+00:00 | 2026-08-01T17:12:14+00:00 |

## Parity Manifest Summary

| status | count |
|---|---|
| generated | 253 |
| unsupported_release_only | 7 |

## Detailed Module Reports

| module | report | exists | size bytes | description |
|---|---|---|---|---|
| inventory | inventory/reports/release_inventory_stats.md | 1 | 7,696 | Release inventory and health report |
| spatial | spatial/reports/spatial_coverage_stats.md | 1 | 10,602 | Spatial coverage report |
| spatial | spatial/article_spatial_coverage_summary.md | 1 | 7,202 | Article spatial coverage summary |
| temporal | temporal/reports/temporal_coverage_stats.md | 1 | 8,973 | Temporal coverage report |
| temporal | temporal/article_temporal_coverage_report.md | 1 | 9,024 | Article temporal coverage report |
| source_dataset_layers | source_dataset_layers/reports/source_dataset_layers.md | 1 | 5,805 | Source dataset layer report |
| source_contribution | source_contribution/reports/source_contribution_report.md | 1 | 21,735 | Source contribution report |
| basin_diagnostics | basin_diagnostics/spatial_match_error_detailed_report.md | 1 | 14,976 | Basin matching detailed report |
| variable_summary | variable_summary/variable_coverage_results_report_ESSD.md | 1 | 13,622 | Variable coverage report |
| qc_flags | qc_flags/article_qc_flag_report.md | 1 | 24,537 | QC flag report |

## Release Risks and QA Signals

- Inventory path-leak fields with host-local paths: 5
- NetCDF metadata dimensions with inactive entries: 0
- Validation/file-existence contradictions: 0
- Unresolved basin rows: 793
- Records affected by unresolved basin rows: 790,303
- Resolved basin point-flag anomalies: 102
- Satellite source-variable rows with less than 1% present values: 4
- Sparse time axes: annual, daily, monthly

## Inventory Path-Leak Fields

Raw examples stay in `inventory/tables/path_leaks.csv`; this report does not echo local machine paths.

| product | layer | field | n values | absolute path count | local path count |
|---|---|---|---|---|---|
| satellite_catalog | csv | resolved_candidate_path | 38,550 | 38,550 | 38,550 |
| satellite_validation_catalog | csv | resolved_candidate_path | 38,550 | 38,550 | 38,550 |
| satellite_nc | netcdf | resolved_candidate_path | 38,550 | 38,550 | 38,550 |
| source_station_catalog | csv | source_station_paths | 3,913 | 3,913 | 3,913 |
| master_nc | netcdf | source_station_paths | 3,913 | 3,913 | 3,913 |
| satellite_catalog | csv | candidate_path | 38,550 | 0 | 0 |
| satellite_validation_catalog | csv | candidate_path | 38,550 | 0 | 0 |
| satellite_nc | netcdf | candidate_path | 38,550 | 0 | 0 |

## Inactive Metadata Consistency

| entity | nc dimension | nc unique | catalog rows | catalog unique | used unique | inactive nc entries |
|---|---|---|---|---|---|---|
| cluster_uid | 3,762 | 3,762 | 3,771 | 3,762 | 3,762 | 0 |
| source_station_uid | 3,913 | 3,913 | 3,913 | 3,913 | 3,913 | 0 |

## Top Unresolved Basin Sources

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 887 | 297 | 1,662,326 | 540,624 | 33.48% | 32.52% |
| HYDAT | 505 | 119 | 671,068 | 182,149 | 23.56% | 27.14% |
| EUSEDcollab | 244 | 134 | 66,637 | 54,289 | 54.92% | 81.47% |
| GFQA_v2 | 1,910 | 217 | 56,457 | 5,431 | 11.36% | 9.62% |
| Robotham | 3 | 3 | 3,432 | 3,432 | 100% | 100% |
| HYBAM | 12 | 4 | 11,826 | 2,675 | 33.33% | 22.62% |
| Eurasian_River | 17 | 6 | 3,204 | 1,205 | 35.29% | 37.61% |
| NERC | 4 | 3 | 624 | 465 | 75% | 74.52% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| GloRiSe | 77 | 8 | 103 | 9 | 10.39% | 8.74% |

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
