# Sediment Reference Release Detailed Statistics Report

## Run Identity

- Release package: `output/sed_reference_release`
- Stats output: `$WORKSPACE/output_other/stats_release_full`
- Run started UTC: `2026-06-19T11:05:33+00:00`
- Run finished UTC: `2026-06-19T11:06:44+00:00`
- Clean output before run: `True`
- Release fingerprint: `7287bc69a8629577e700ac4f9855569abe247c5488e1a9c270981ce89ead9dc9`
- Stats script fingerprint: `cdd86616435a53cc4b8c95b3dbabd8b7c075adf6f9f3b304695a51024bee8994`

## Run Status

- Modules requested: 8
- Module failures: 1
- Missing release-capable parity outputs: 0
- Unsupported release-only parity outputs: 7

| module | return code | started utc | finished utc |
|---|---|---|---|
| inventory | 2 | 2026-06-19T11:05:33+00:00 | 2026-06-19T11:05:37+00:00 |
| spatial | 0 | 2026-06-19T11:05:37+00:00 | 2026-06-19T11:05:55+00:00 |
| temporal | 0 | 2026-06-19T11:05:55+00:00 | 2026-06-19T11:06:14+00:00 |
| source_dataset_layers | 0 | 2026-06-19T11:06:14+00:00 | 2026-06-19T11:06:16+00:00 |
| source_contribution | 0 | 2026-06-19T11:06:16+00:00 | 2026-06-19T11:06:23+00:00 |
| basin_diagnostics | 0 | 2026-06-19T11:06:23+00:00 | 2026-06-19T11:06:26+00:00 |
| variable_summary | 0 | 2026-06-19T11:06:26+00:00 | 2026-06-19T11:06:38+00:00 |
| qc_flags | 0 | 2026-06-19T11:06:38+00:00 | 2026-06-19T11:06:44+00:00 |

## Parity Manifest Summary

| status | count |
|---|---|
| generated | 253 |
| unsupported_release_only | 7 |

## Detailed Module Reports

| module | report | exists | size bytes | description |
|---|---|---|---|---|
| inventory | inventory/reports/release_inventory_stats.md | 1 | 7,608 | Release inventory and health report |
| spatial | spatial/reports/spatial_coverage_stats.md | 1 | 11,162 | Spatial coverage report |
| spatial | spatial/article_spatial_coverage_summary.md | 1 | 11,162 | Article spatial coverage summary |
| temporal | temporal/reports/temporal_coverage_stats.md | 1 | 9,273 | Temporal coverage report |
| temporal | temporal/article_temporal_coverage_report.md | 1 | 9,273 | Article temporal coverage report |
| source_dataset_layers | source_dataset_layers/reports/source_dataset_layers.md | 1 | 5,823 | Source dataset layer report |
| source_contribution | source_contribution/reports/source_contribution_report.md | 1 | 22,030 | Source contribution report |
| basin_diagnostics | basin_diagnostics/spatial_match_error_detailed_report.md | 1 | 15,121 | Basin matching detailed report |
| variable_summary | variable_summary/variable_coverage_results_report_ESSD.md | 1 | 13,692 | Variable coverage report |
| qc_flags | qc_flags/article_qc_flag_report.md | 1 | 26,456 | QC flag report |

## Release Risks and QA Signals

- Inventory path-leak fields with host-local paths: 5
- NetCDF metadata dimensions with inactive entries: 0
- Validation/file-existence contradictions: 0
- Unresolved basin rows: 665
- Records affected by unresolved basin rows: 534,514
- Resolved basin point-flag anomalies: 101
- Satellite source-variable rows with less than 1% present values: 5
- Sparse time axes: annual, daily, monthly

## Inventory Path-Leak Fields

Raw examples stay in `inventory/tables/path_leaks.csv`; this report does not echo local machine paths.

| product | layer | field | n values | absolute path count | local path count |
|---|---|---|---|---|---|
| satellite_catalog | csv | resolved_candidate_path | 47,785 | 47,785 | 47,785 |
| satellite_validation_catalog | csv | resolved_candidate_path | 47,785 | 47,785 | 47,785 |
| satellite_nc | netcdf | resolved_candidate_path | 47,785 | 47,785 | 47,785 |
| source_station_catalog | csv | source_station_paths | 3,952 | 3,952 | 3,952 |
| master_nc | netcdf | source_station_paths | 3,952 | 3,952 | 3,952 |
| satellite_catalog | csv | candidate_path | 47,785 | 0 | 0 |
| satellite_validation_catalog | csv | candidate_path | 47,785 | 0 | 0 |
| satellite_nc | netcdf | candidate_path | 47,785 | 0 | 0 |

## Inactive Metadata Consistency

| entity | nc dimension | nc unique | catalog rows | catalog unique | used unique | inactive nc entries |
|---|---|---|---|---|---|---|
| cluster_uid | 3,528 | 3,528 | 3,540 | 3,528 | 3,528 | 0 |
| source_station_uid | 3,952 | 3,952 | 3,952 | 3,952 | 3,952 | 0 |

## Top Unresolved Basin Sources

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 877 | 239 | 1,651,807 | 423,222 | 27.25% | 25.62% |
| HYDAT | 481 | 88 | 664,497 | 88,547 | 18.30% | 13.33% |
| EUSEDcollab | 216 | 68 | 63,208 | 7,445 | 31.48% | 11.78% |
| HYBAM | 12 | 7 | 11,826 | 6,154 | 58.33% | 52.04% |
| GFQA_v2 | 1,704 | 213 | 69,144 | 5,740 | 12.50% | 8.30% |
| Robotham | 3 | 2 | 3,432 | 2,867 | 66.67% | 83.54% |
| Eurasian_River | 17 | 2 | 3,293 | 265 | 11.76% | 8.05% |
| NERC | 4 | 1 | 624 | 160 | 25% | 25.64% |
| Rhine | 12 | 2 | 312 | 49 | 16.67% | 15.71% |
| GloRiSe | 128 | 41 | 154 | 41 | 32.03% | 26.62% |

_Showing first 10 of 18 rows._

## Satellite Variable Coverage Watchlist

| source name | variable | n records | n present | present percent | usable percent |
|---|---|---|---|---|---|
| RiverSed | Q | 14,228,483 | 0 | 0% | 0% |
| RiverSed | SSL | 14,228,483 | 0 | 0% | 0% |
| RiverSed | SSC | 14,228,483 | 28,629 | 0.20% | 0.20% |
| GSED | Q | 2,144,599 | 0 | 0% | 0% |
| GSED | SSL | 2,144,599 | 0 | 0% | 0% |

## How to Read These Outputs

- Per-module reports are the authoritative narrative summaries; CSV tables remain the reproducible data source.
- `unsupported_release_only` means the legacy output requires non-release pipeline intermediates and is intentionally not recreated.
- Release-only reports do not change any dataset values or basin statuses; they expose QA priorities for the next release build.
