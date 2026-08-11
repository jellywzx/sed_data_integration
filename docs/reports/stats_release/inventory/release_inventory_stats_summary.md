# Release Inventory Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/inventory/tables`
- Inputs are limited to the published release package; pipeline intermediates are not read.

## Headline

- Files discovered in release package: 18
- Total release size: 9,947.587 MB
- Registered products checked: 22
- Missing registered products: 4
- Unregistered top-level files: 0
- Inventory/listing mismatches: 0
- Fields with local absolute-path values: 2
- NetCDF metadata dimensions with inactive entries: 0
- Validation/file-existence contradictions: 0

## Article-Ready Metrics

| section | metric | value | unit | source file | notes |
|---|---|---|---|---|---|
| release_inventory | file_count | 18 | files | release_dir |  |
| release_inventory | total_size_mb | 9,948 | MB | release_dir |  |
| netcdf_products | master_nc_variable_count | 113 | variables | sed_reference_master.nc |  |
| netcdf_products | daily_nc_variable_count | 69 | variables | sed_reference_timeseries_daily.nc |  |
| netcdf_products | monthly_nc_variable_count | 69 | variables | sed_reference_timeseries_monthly.nc |  |
| netcdf_products | annual_nc_variable_count | 69 | variables | sed_reference_timeseries_annual.nc |  |
| netcdf_products | climatology_nc_variable_count | 65 | variables | sed_reference_climatology.nc |  |
| netcdf_products | satellite_nc_variable_count | 48 | variables | sed_reference_satellite.nc |  |
| release_health | files_not_listed_in_release_inventory | 0 | files | release_inventory.csv |  |

## Release File Inventory

This table compares the physical release contents with both the code-side product registry and `release_inventory.csv`.

| file name | product | registered in product files | listed in release inventory | kind | file type | size mb | description |
|---|---|---|---|---|---|---|---|
| sed_reference_timeseries_daily.nc | daily_nc | 1 | 1 | core_netcdf | nc | 5,657 | Daily station x time matrix for validation |
| sed_reference_satellite.nc | satellite_nc | 1 | 1 | satellite_netcdf | nc | 2,549 | Required validation-only satellite observations excluded from the main station-reference merge. |
| sed_reference_timeseries_monthly.nc | monthly_nc | 1 | 1 | core_netcdf | nc | 708.21 | Monthly station x time matrix for validation |
| sed_reference_cluster_basins.gpkg | cluster_basins_gpkg | 1 | 1 | spatial | gpkg | 642.79 | Station basin polygon sidecar keyed by station_uid + resolution |
| sed_reference_master.nc | master_nc | 1 | 1 | core_netcdf | nc | 349.43 | Authoritative record-level reference dataset |
| satellite_catalog.csv | satellite_catalog | 1 | 1 | satellite_catalog | csv | 13.81 | Required catalog for the validation-only satellite release dataset. |
| sed_reference_climatology.nc | climatology_nc | 1 | 1 | core_netcdf | nc | 8.96 | Standalone climatology reference dataset |
| source_station_catalog.csv | source_station_catalog | 1 | 1 | catalog | csv | 5.88 | Resolution-aware source-station provenance catalog |
| sed_reference_cluster_points.gpkg | cluster_points_gpkg | 1 | 1 | spatial | gpkg | 5.60 | Station point sidecar keyed by station_uid + resolution |
| sed_reference_source_stations.gpkg | source_stations_gpkg | 1 | 1 | spatial | gpkg | 3.58 | Source-station sidecar keyed by source_station_uid + resolution |
| station_catalog.csv | station_catalog | 1 | 1 | catalog | csv | 2.33 | Resolution-aware station lookup catalog |
| sed_reference_timeseries_annual.nc | annual_nc | 1 | 1 | core_netcdf | nc | 0.74 | Annual station x time matrix for validation |
| sed_reference_overlap_candidates.csv.gz | overlap_candidates_csv_gz | 1 | 1 | provenance_sidecar | csv.gz | 0.33 | Candidate-level selected and non-selected values for multi-source overlap validation |
| source_dataset_catalog.csv | source_dataset_catalog | 1 | 1 | catalog | csv | 0.30 | Source-dataset metadata catalog |
| example_reference_workflow.py | example_workflow | 1 | 1 | support | py | 0.02 | Example workflow script |
| release_validation_report.csv | validation_csv | 1 | 1 | report | csv | 0.01 | Release validation report |
| README.md | readme | 1 | 1 | support | md | 0.01 | Release usage guide |
| release_inventory.csv | inventory_csv | 1 | 1 | inventory | csv | 0.00 | Release inventory CSV |

## NetCDF Schema Summary

Counts are derived from release NetCDF dimensions, variables, and global attributes.

| product | dimension | global | variable |
|---|---|---|---|
| annual_nc | 3 | 1 | 69 |
| climatology_nc | 3 | 1 | 65 |
| daily_nc | 3 | 1 | 69 |
| master_nc | 4 | 1 | 113 |
| monthly_nc | 3 | 1 | 69 |
| satellite_nc | 3 | 1 | 48 |

## GeoPackage Layers

| product | file name | layer name | feature count | column count |
|---|---|---|---|---|
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | station_summary | 7,379 | 40 |
| source_stations_gpkg | sed_reference_source_stations.gpkg | source_daily | 4,875 | 25 |
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | station_daily | 4,717 | 32 |
| cluster_basins_gpkg | sed_reference_cluster_basins.gpkg | basin_daily | 3,555 | 28 |
| source_stations_gpkg | sed_reference_source_stations.gpkg | source_monthly | 2,793 | 25 |
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | station_monthly | 2,697 | 32 |
| cluster_basins_gpkg | sed_reference_cluster_basins.gpkg | basin_monthly | 2,127 | 28 |
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | station_annual | 49 | 32 |
| source_stations_gpkg | sed_reference_source_stations.gpkg | source_annual | 49 | 25 |
| cluster_basins_gpkg | sed_reference_cluster_basins.gpkg | basin_annual | 42 | 28 |

## Release Inventory Mismatches

Rows here require release packaging cleanup or an explicit registry decision.

_No rows._

## Absolute Path Leak Diagnostics

Raw samples are intentionally kept only in `tables/path_leaks.csv`; Markdown reports avoid echoing host-local paths.

| product | layer | field | n values | absolute path count | local path count |
|---|---|---|---|---|---|
| source_station_catalog | csv | source_station_paths | 7,717 | 7,717 | 7,717 |
| master_nc | netcdf | source_station_paths | 7,716 | 7,716 | 7,716 |

## Active Metadata Consistency

Inactive entries are NetCDF metadata identifiers that are not used by active release records or catalogs.

| entity | nc dimension | nc unique | catalog rows | catalog unique | used unique | inactive nc entries | used missing from catalog |
|---|---|---|---|---|---|---|---|
| station_uid | 7,379 | 7,379 | 7,463 | 7,379 | 7,379 | 0 | 0 |
| source_station_uid | 7,716 | 7,716 | 7,717 | 7,716 | 7,716 | 0 | 0 |

## Validation Contradictions

_No rows._

## Recommended Follow-Up

- Rebuild or update `release_inventory.csv` when mismatch rows are present.
- Replace host-local paths in release CSV/NetCDF provenance fields with release-relative paths, public URLs, or stable provenance tokens.
- Either trim inactive NetCDF metadata dimensions or publish an explicit inactive metadata catalog with `is_active` semantics.
- Re-run validation after release sidecar registration changes so skip messages match actual file existence.
