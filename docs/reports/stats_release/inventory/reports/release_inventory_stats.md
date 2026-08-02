# Release Inventory Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/inventory/tables`
- Inputs are limited to the published release package; pipeline intermediates are not read.

## Headline

- Files discovered in release package: 23
- Total release size: 6,546.289 MB
- Registered products checked: 22
- Missing registered products: 0
- Unregistered top-level files: 1
- Inventory/listing mismatches: 1
- Fields with local absolute-path values: 5
- NetCDF metadata dimensions with inactive entries: 0
- Validation/file-existence contradictions: 0

## Article-Ready Metrics

| section | metric | value | unit | source file | notes |
|---|---|---|---|---|---|
| release_inventory | file_count | 23 | files | release_dir |  |
| release_inventory | total_size_mb | 6,546 | MB | release_dir |  |
| netcdf_products | master_nc_variable_count | 113 | variables | sed_reference_master.nc |  |
| netcdf_products | daily_nc_variable_count | 69 | variables | sed_reference_timeseries_daily.nc |  |
| netcdf_products | monthly_nc_variable_count | 69 | variables | sed_reference_timeseries_monthly.nc |  |
| netcdf_products | annual_nc_variable_count | 69 | variables | sed_reference_timeseries_annual.nc |  |
| netcdf_products | climatology_nc_variable_count | 23 | variables | sed_reference_climatology.nc |  |
| netcdf_products | satellite_nc_variable_count | 84 | variables | sed_reference_satellite.nc |  |
| release_health | files_not_listed_in_release_inventory | 1 | files | release_inventory.csv |  |

## Release File Inventory

This table compares the physical release contents with both the code-side product registry and `release_inventory.csv`.

| file name | product | registered in product files | listed in release inventory | kind | file type | size mb | description |
|---|---|---|---|---|---|---|---|
| sed_reference_satellite.nc | satellite_nc | 1 | 1 | satellite_netcdf | nc | 2,613 | Required validation-only satellite observations excluded from the main station-reference merge. |
| sed_reference_timeseries_daily.nc | daily_nc | 1 | 1 | core_netcdf | nc | 1,954 | Daily station x time matrix for validation |
| sed_reference_timeseries_monthly.nc | monthly_nc | 1 | 1 | core_netcdf | nc | 1,134 | Monthly station x time matrix for validation |
| sed_reference_cluster_basins.gpkg | cluster_basins_gpkg | 1 | 1 | spatial | gpkg | 429.33 | Cluster basin polygon sidecar keyed by cluster_uid + resolution |
| sed_reference_master.nc | master_nc | 1 | 1 | core_netcdf | nc | 300.76 | Authoritative record-level reference dataset |
| sed_reference_satellite_candidates.csv.gz | satellite_candidates_csv_gz | 1 | 1 | provenance_sidecar | csv.gz | 29.42 | Satellite validation-only candidate records (CSV) |
| sed_reference_satellite_candidates.parquet | satellite_candidates_parquet | 1 | 1 | provenance_sidecar | parquet | 25.40 | Parquet version of satellite candidates |
| satellite_validation_catalog.csv | satellite_validation_catalog | 1 | 1 | satellite_catalog | csv | 24.05 | Alias for satellite_catalog.csv (backward-compatible name) |
| satellite_catalog.csv | satellite_catalog | 1 | 1 | satellite_catalog | csv | 24.05 | Required catalog for the validation-only satellite release dataset. |
| source_station_catalog.csv | source_station_catalog | 1 | 1 | catalog | csv | 3.24 | Resolution-aware source-station provenance catalog |
| sed_reference_cluster_points.gpkg | cluster_points_gpkg | 1 | 1 | spatial | gpkg | 2.95 | Cluster point sidecar keyed by cluster_uid + resolution |
| sed_reference_source_stations.gpkg | source_stations_gpkg | 1 | 1 | spatial | gpkg | 1.91 | Source-station sidecar keyed by source_station_uid + resolution |
| station_catalog.csv | station_catalog | 1 | 1 | catalog | csv | 1.20 | Resolution-aware cluster lookup catalog |
| station_catalog.csv.bak |  | 0 | 0 |  | bak | 1.13 |  |
| sed_reference_timeseries_annual.nc | annual_nc | 1 | 1 | core_netcdf | nc | 1.04 | Annual station x time matrix for validation |
| sed_reference_climatology.nc | climatology_nc | 1 | 1 | core_netcdf | nc | 0.70 | Standalone climatology reference dataset |
| sed_reference_overlap_candidates.csv.gz | overlap_candidates_csv_gz | 1 | 1 | provenance_sidecar | csv.gz | 0.24 | Candidate-level selected and non-selected values for multi-source overlap validation |
| source_dataset_catalog.csv | source_dataset_catalog | 1 | 1 | catalog | csv | 0.14 | Source-dataset metadata catalog |

_Showing first 18 of 23 rows._

## NetCDF Schema Summary

Counts are derived from release NetCDF dimensions, variables, and global attributes.

| product | dimension | global | variable |
|---|---|---|---|
| annual_nc | 3 | 1 | 69 |
| climatology_nc | 3 | 1 | 23 |
| daily_nc | 3 | 1 | 69 |
| master_nc | 4 | 1 | 113 |
| monthly_nc | 3 | 1 | 69 |
| satellite_nc | 3 | 1 | 84 |

## GeoPackage Layers

| product | file name | layer name | feature count | column count |
|---|---|---|---|---|
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | cluster_summary | 3,762 | 40 |
| source_stations_gpkg | sed_reference_source_stations.gpkg | source_monthly | 2,257 | 25 |
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | cluster_monthly | 2,117 | 32 |
| cluster_basins_gpkg | sed_reference_cluster_basins.gpkg | basin_monthly | 1,785 | 28 |
| source_stations_gpkg | sed_reference_source_stations.gpkg | source_daily | 1,598 | 25 |
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | cluster_daily | 1,596 | 32 |
| cluster_basins_gpkg | sed_reference_cluster_basins.gpkg | basin_daily | 1,146 | 28 |
| cluster_points_gpkg | sed_reference_cluster_points.gpkg | cluster_annual | 58 | 32 |
| source_stations_gpkg | sed_reference_source_stations.gpkg | source_annual | 58 | 25 |
| cluster_basins_gpkg | sed_reference_cluster_basins.gpkg | basin_annual | 47 | 28 |

## Release Inventory Mismatches

Rows here require release packaging cleanup or an explicit registry decision.

| file name | issue |
|---|---|
| station_catalog.csv.bak | on_disk_not_in_release_inventory |

## Absolute Path Leak Diagnostics

Raw samples are intentionally kept only in `tables/path_leaks.csv`; Markdown reports avoid echoing host-local paths.

| product | layer | field | n values | absolute path count | local path count |
|---|---|---|---|---|---|
| satellite_catalog | csv | resolved_candidate_path | 38,550 | 38,550 | 38,550 |
| satellite_nc | netcdf | resolved_candidate_path | 38,550 | 38,550 | 38,550 |
| satellite_validation_catalog | csv | resolved_candidate_path | 38,550 | 38,550 | 38,550 |
| source_station_catalog | csv | source_station_paths | 3,913 | 3,913 | 3,913 |
| master_nc | netcdf | source_station_paths | 3,913 | 3,913 | 3,913 |

## Active Metadata Consistency

Inactive entries are NetCDF metadata identifiers that are not used by active release records or catalogs.

| entity | nc dimension | nc unique | catalog rows | catalog unique | used unique | inactive nc entries | used missing from catalog |
|---|---|---|---|---|---|---|---|
| cluster_uid | 3,762 | 3,762 | 3,771 | 3,762 | 3,762 | 0 | 0 |
| source_station_uid | 3,913 | 3,913 | 3,913 | 3,913 | 3,913 | 0 | 0 |

## Validation Contradictions

_No rows._

## Recommended Follow-Up

- Rebuild or update `release_inventory.csv` when mismatch rows are present.
- Replace host-local paths in release CSV/NetCDF provenance fields with release-relative paths, public URLs, or stable provenance tokens.
- Either trim inactive NetCDF metadata dimensions or publish an explicit inactive metadata catalog with `is_active` semantics.
- Re-run validation after release sidecar registration changes so skip messages match actual file existence.
