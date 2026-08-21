# Sediment Reference Release Detailed Statistics Report

## Run Identity

- Release package: `output/sed_reference_release`
- Stats output: `output_other/stats_release`
- Run started UTC: `2026-08-14T06:56:04+00:00`
- Run finished UTC: `2026-08-14T06:58:58+00:00`
- Clean output before run: `True`
- Release fingerprint: `63bddc765563716ca24c5db2bb64b355045b54d168c978bf4f214b3e16adcc8a`
- Stats script fingerprint: `be515599bc9eca0951715d6cec9c25d0bec2e1048481e5aeb288d5d6d50c41d8`

## Run Status

- Modules requested: 8
- Module failures: 0
- Missing release-capable parity outputs: 3
- Unsupported release-only parity outputs: 7

| module | return code | started utc | finished utc |
|---|---|---|---|
| inventory | 0 | 2026-08-14T06:56:04+00:00 | 2026-08-14T06:56:10+00:00 |
| spatial | 0 | 2026-08-14T06:56:10+00:00 | 2026-08-14T06:56:38+00:00 |
| temporal | 0 | 2026-08-14T06:56:38+00:00 | 2026-08-14T06:57:06+00:00 |
| source_dataset_layers | 0 | 2026-08-14T06:57:06+00:00 | 2026-08-14T06:57:12+00:00 |
| source_contribution | 0 | 2026-08-14T06:57:12+00:00 | 2026-08-14T06:57:21+00:00 |
| basin_diagnostics | 0 | 2026-08-14T06:57:21+00:00 | 2026-08-14T06:57:25+00:00 |
| variable_summary | 0 | 2026-08-14T06:57:25+00:00 | 2026-08-14T06:57:48+00:00 |
| qc_flags | 0 | 2026-08-14T06:57:48+00:00 | 2026-08-14T06:58:58+00:00 |

## Parity Manifest Summary

| status | count |
|---|---|
| generated | 250 |
| unsupported_release_only | 7 |
| missing_release_capable | 3 |

## Detailed Module Reports

| module | report | exists | size bytes | description |
|---|---|---|---|---|
| inventory | inventory/reports/release_inventory_stats.md | 1 | 7,142 | Release inventory and health report |
| spatial | spatial/reports/spatial_coverage_stats.md | 1 | 10,483 | Spatial coverage report |
| spatial | spatial/article_spatial_coverage_summary.md | 1 | 7,121 | Article spatial coverage summary |
| temporal | temporal/reports/temporal_coverage_stats.md | 1 | 8,847 | Temporal coverage report |
| temporal | temporal/article_temporal_coverage_report.md | 1 | 8,972 | Article temporal coverage report |
| source_dataset_layers | source_dataset_layers/reports/source_dataset_layers.md | 1 | 5,733 | Source dataset layer report |
| source_contribution | source_contribution/reports/source_contribution_report.md | 1 | 19,787 | Source contribution report |
| basin_diagnostics | basin_diagnostics/spatial_match_error_detailed_report.md | 1 | 14,941 | Basin matching detailed report |
| variable_summary | variable_summary/variable_coverage_results_report_ESSD.md | 1 | 29,527 | Variable coverage report |
| qc_flags | qc_flags/article_qc_flag_report.md | 1 | 32,903 | QC flag report |

## Release Risks and QA Signals

- Inventory path-leak fields with host-local paths: 2
- NetCDF metadata dimensions with inactive entries: 0
- Validation/file-existence contradictions: 0
- Unresolved basin rows: 1,605
- Records affected by unresolved basin rows: 758,512
- Resolved basin point-flag anomalies: 168
- Satellite source-variable rows with less than 1% present values: 4
- Sparse time axes: annual, daily, monthly

## Inventory Path-Leak Fields

Raw examples stay in `inventory/tables/path_leaks.csv`; this report does not echo local machine paths.

| product | layer | field | n values | absolute path count | local path count |
|---|---|---|---|---|---|
| source_station_catalog | csv | source_station_paths | 7,469 | 7,469 | 7,469 |
| master_nc | netcdf | source_station_paths | 7,469 | 7,469 | 7,469 |

## Inactive Metadata Consistency

| entity | nc dimension | nc unique | catalog rows | catalog unique | used unique | inactive nc entries |
|---|---|---|---|---|---|---|
| cluster_uid | 7,135 | 7,135 | 7,135 | 7,135 | 7,135 | 0 |
| source_station_uid | 7,469 | 7,469 | 7,469 | 7,469 | 7,469 | 0 |

## Top Unresolved Basin Sources

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 889 | 295 | 1,690,433 | 543,790 | 33.18% | 32.17% |
| HYDAT | 543 | 124 | 676,024 | 182,255 | 22.84% | 26.96% |
| GFQA_v2 | 5,499 | 1,160 | 235,600 | 24,906 | 21.09% | 10.57% |
| Robotham | 3 | 3 | 3,432 | 3,432 | 100% | 100% |
| HYBAM | 12 | 4 | 9,404 | 2,392 | 33.33% | 25.44% |
| Eurasian_River | 17 | 6 | 3,263 | 1,239 | 35.29% | 37.97% |
| NERC | 4 | 3 | 624 | 465 | 75% | 74.52% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| GloRiSe | 77 | 8 | 649 | 9 | 10.39% | 1.39% |
| Yajiang | 23 | 1 | 23 | 1 | 4.35% | 4.35% |

_Showing first 10 of 17 rows._

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
