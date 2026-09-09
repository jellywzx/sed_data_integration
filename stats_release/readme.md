# Release-Only Statistics Module Guide

This directory contains the release-facing statistics modules for the sediment
reference dataset. The modules are designed for post-release analysis and
validation: they read product files from the release package and write
diagnostic tables, figures, reports, and manifests to the stats output tree.

All normal module inputs come from:

```text
output/sed_reference_release/
```

All normal module outputs are written under:

```text
output_other/stats_release/
```

## Run

Run the full release statistics suite:

```bash
python3 -m stats_release.run_all_release_stats
```

Run one module:

```bash
python3 -m stats_release.spatial --release-dir output/sed_reference_release
```

Skip figure generation:

```bash
python3 -m stats_release.spatial --release-dir output/sed_reference_release --skip-figures
```

Allow non-release inputs for local debugging only:

```bash
python3 -m stats_release.spatial --release-dir output/sed_reference_release --allow-non-release-inputs
```

`--strict-release-only` is enabled by default. Passing
`--allow-non-release-inputs` disables the path guard for debugging runs.

## Module Overview

| Module | Purpose | Main input | Main output |
| --- | --- | --- | --- |
| `release_paths.py` | Product names and default path registry | Constants | None |
| `release_io.py` | Shared release I/O and path guards | Helper functions | None |
| `common_stats.py` | Shared statistics, classification, and plotting helpers | Helper functions | None |
| `reporting.py` | Shared report and table writing helpers | Helper functions | Markdown reports |
| `parity.py` | Legacy-output compatibility manifest | Registered target list | `parity_manifest.csv` |
| `inventory.py` | Release inventory and health checks | All release products | `inventory/tables/` |
| `spatial.py` | Spatial coverage statistics | `station_catalog.csv` and GeoPackages | `spatial/tables/`, `spatial/figures/` |
| `temporal.py` | Time-span and record-length statistics | Matrix NetCDF products and catalogs | `temporal/tables/`, `temporal/figures/` |
| `variable_summary.py` | Q, SSC, and SSL coverage statistics | NetCDF products | `variable_summary/tables/`, `variable_summary/figures/` |
| `source_contribution.py` | Source contribution statistics | Release catalogs and satellite products | `source_contribution/tables/`, `source_contribution/figures/` |
| `source_dataset_layers.py` | Source membership across catalog layers | Release catalogs and sidecars | `source_dataset_layers/tables/` |
| `basin_diagnostics.py` | Basin assignment diagnostics | `station_catalog.csv` | `basin_diagnostics/tables/`, `basin_diagnostics/figures/` |
| `qc_flags.py` | QC flag summaries | `master_nc`, `climatology_nc`, `satellite_nc` | `qc_flags/tables/`, `qc_flags/figures/` |
| `run_all_release_stats.py` | Suite entry point and manifest builder | Module CLIs | `run_summary.*`, `run_manifest.*` |

## Shared Design

The release-only statistics suite follows these rules:

- Read release products through `ReleaseContext`.
- Keep all routine inputs inside the configured release directory.
- Keep module outputs inside the configured stats output directory.
- Generate CSV tables for reproducibility and figures for review.
- Record run status and file fingerprints through the all-module runner.
- Treat satellite and climatology products as first-class release outputs.
- Keep pipeline intermediate files out of release-only statistics unless a
  debug run explicitly allows non-release inputs.

## Shared Modules

### `release_paths.py`

This file defines default locations and product filename mappings.

Key constants:

- `PRODUCT_FILES`: maps product identifiers to filenames. It covers NetCDF
  products, CSV catalogs, GeoPackages, sidecar files, inventories, validation
  reports, README files, and workflow examples.
- `MATRIX_PRODUCTS`: maps `daily`, `monthly`, and `annual` to their matrix
  NetCDF products.
- `CORE_PRODUCTS`: lists the core release product names used by inventory and
  validation code.

### `release_io.py`

This file provides the shared I/O layer.

Primary class:

- `ReleaseContext`: stores the release directory, output directory, and
  release-only guard settings.

Important methods and helpers:

- `release_file(name)`: resolves a registered release product path.
- `require_input(path)`: validates that an input exists and passes the path
  guard.
- `output_path(*parts)`: builds and creates a module output path.
- `read_csv(...)`: reads CSV inputs with consistent path handling.
- `open_dataset(...)`: opens NetCDF products.
- `sqlite_connect(...)`: opens SQLite or GeoPackage inputs for read access.
- `add_common_args(parser)`: adds shared CLI flags.
- `numeric_series(frame, col)`: converts a column to numeric values.
- `text_series(frame, col)`: converts a column to text values.
- `clean_text(value)`: normalizes empty text values.
- `read_numeric_var(ds, name)`: reads numeric NetCDF variables safely.
- `read_text_var(ds, name)`: reads text NetCDF variables safely.
- `netcdf_record_count(ds)`: detects the record dimension length.
- `count_matrix_selected_cells(ds)`: counts selected matrix cells.
- `setup_matplotlib()`: configures matplotlib for non-interactive output.

### `common_stats.py`

This file holds shared statistics and classification helpers.

Key constants:

- `VARIABLES = ("Q", "SSC", "SSL")`
- `FLAG_VALUES = (0, 1, 2, 3, 8, 9)`
- `FLAG_MEANINGS`

Important helpers:

- `pct(numerator, denominator)`: computes percentages safely.
- `classify_source(source_name, source_family)`: returns source type and group.
- `attach_source_classification(frame)`: appends source classification columns.
- `numeric_stats(values)`: returns mean, median, standard deviation, percentiles,
  extrema, and log-scale summaries.
- `decode_time_axis(ds)`: decodes a NetCDF time axis.
- `decode_time_values(ds, values)`: decodes selected time values.
- `resolution_values(ds, key)`: reads and decodes resolution values.
- `save_figure(fig, png_path, dpi, also_pdf=True)`: writes PNG and optional PDF
  figures.
- `write_geojson_points(frame, path)`: writes point features as GeoJSON.

### `reporting.py`

This file centralizes Markdown report and table helpers used by stats modules.
It keeps report formatting consistent and makes module outputs easier to audit.

## Product Modules

### `parity.py`

`parity.py` builds a compatibility manifest between legacy statistics outputs
and release-only outputs.

Main output:

- `parity_manifest.csv`

Each row records:

- `module`
- `legacy_script`
- `legacy_output`
- `new_output`
- `release_only_capable`
- `unsupported_reason`
- `status`
- `exists`
- `size_bytes`

Use the `status` column to review whether a legacy output is generated, missing
even though it should be release-only capable, or unsupported because it depends
on pipeline intermediate files.

### `inventory.py`

`inventory.py` builds a complete inventory and health check for the release
package.

Main functions:

- File inventory for registered and unregistered release files.
- NetCDF schema extraction for dimensions, variables, attributes, units, and
  flag metadata.
- GeoPackage layer inspection through `gpkg_contents`.
- Inventory mismatch checks between `release_inventory.csv` and files on disk.
- Local path leak checks in CSV and NetCDF metadata.
- Active metadata consistency checks for `cluster_uid` and
  `source_station_uid`.
- Validation contradiction checks against `release_validation_report.csv`.

Main outputs under `inventory/tables/`:

- `release_inventory_stats.csv`
- `release_inventory_stats_files.csv`
- `release_inventory_stats_summary.csv`
- `release_inventory_stats_summary_wide.csv`
- `release_inventory_stats_netcdf_schema.csv`
- `release_inventory_stats_gpkg_layers.csv`
- `release_inventory_stats_article_metrics.csv`
- `release_inventory_mismatches.csv`
- `path_leaks.csv`
- `active_metadata_consistency.csv`
- `inactive_metadata_entries.csv`
- `validation_contradictions.csv`

The Markdown report is written to:

```text
inventory/reports/release_inventory_stats.md
```

### `spatial.py`

`spatial.py` summarizes the spatial coverage of stations, clusters, basins, and
source records.

Main analyses:

- Cluster-level spatial attributes from `station_catalog.csv`.
- Coverage by resolution.
- Coverage by country and region.
- Country and region alias checks.
- Basin status summaries.
- GeoPackage layer counts.
- Upstream area distribution.
- Satellite catalog spatial coverage.
- Source-station geographic distribution.
- Source spatial contribution by source, source type, and region.

Important table outputs:

- `table_headline.csv`
- `table_spatial_coverage_summary.csv`
- `table_spatial_coverage_by_resolution.csv`
- `table_spatial_coverage_by_region.csv`
- `table_spatial_coverage_by_country.csv`
- `table_spatial_coverage_by_region_resolution.csv`
- `table_spatial_coverage_by_source.csv`
- `table_spatial_coverage_by_region_source.csv`
- `table_spatial_coverage_by_source_type.csv`
- `table_basin_status.csv`
- `table_upstream_area_distribution.csv`
- `table_satellite_validation_spatial_coverage.csv`
- `table_unknown_country_region_clusters.csv`
- `table_basin_polygon_layers.csv`
- `table_cluster_spatial_attributes.csv`

Important figure outputs:

- `fig_spatial_coverage_by_resolution.png`
- `fig_spatial_coverage_by_region_country.png`
- `fig_global_cluster_distribution.png`
- `fig_spatial_coverage_by_region.png`
- `fig_upstream_area_distribution.png`
- `fig_source_spatial_contribution.png`
- `fig_spatial_coverage_by_region_source_records.png`
- `fig_spatial_coverage_by_region_resolution.png`
- `fig_satellite_validation_spatial_distribution.png`
- `fig_global_cluster_status_and_basins.png`
- `global_cluster_distribution_points.geojson`

### `temporal.py`

`temporal.py` summarizes time coverage, record counts, record lengths, and
active units.

Main analyses:

- Basic product time ranges for matrix, master, climatology, and satellite
  products.
- Time-axis diagnostics for daily, monthly, and annual matrix products.
- Matrix temporal scans by year, variable, and resolution.
- Per-cluster first date, last date, and record length.
- Long-record statistics using 10, 20, 30, 50, and 100 year thresholds.
- Record-length distributions by resolution.
- Temporal coverage by variable.
- Temporal coverage by source.
- Climatology temporal coverage.
- Satellite temporal coverage by year, source, and linked cluster.

Important table outputs:

- `table_temporal_summary.csv`
- `table_temporal_time_axis_diagnostics.csv`
- `table_temporal_coverage_by_resolution.csv`
- `table_temporal_coverage_by_variable.csv`
- `table_active_units_by_year.csv`
- `table_record_length_distribution.csv`
- `table_temporal_coverage_record_lengths_by_unit.csv`
- `table_long_records_by_resolution.csv`
- `table_temporal_coverage_by_source.csv`
- `table_temporal_coverage_by_region_resolution.csv`
- `table_climatology_temporal_summary.csv`
- `table_climatology_by_source.csv`
- `table_satellite_temporal_summary.csv`
- `table_satellite_by_year.csv`
- `table_satellite_by_source.csv`
- `table_satellite_by_linked_cluster.csv`

Important figure outputs:

- `fig_temporal_coverage.png`
- `fig_active_units_by_year.png`
- `fig_records_by_year_variable.png`
- `fig_record_length_distribution.png`
- `fig_long_record_counts.png`
- `fig_source_temporal_span.png`
- Climatology and satellite source contribution figures.

### `variable_summary.py`

`variable_summary.py` summarizes coverage and values for Q, SSC, and SSL across
release NetCDF products.

Main analyses:

- Variable coverage for master, climatology, and satellite products.
- Coverage by resolution from the master product.
- Summary statistics by resolution and variable.
- Co-located variable coverage patterns.
- Extreme-value review points.
- Satellite variable coverage by source.

Important table outputs:

- `table_variable_coverage.csv`
- `table_variable_coverage_by_resolution.csv`
- `table_variable_summary_statistics.csv`
- `table_colocated_variable_coverage.csv`
- `table_extreme_value_review_points.csv`
- `table_satellite_variable_by_source.csv`

Important figure outputs:

- `fig_Q_distribution.png`
- `fig_SSC_distribution.png`
- `fig_SSL_distribution.png`

### `source_contribution.py`

`source_contribution.py` summarizes how each source contributes records,
stations, clusters, variables, resolutions, and time coverage.

Main analyses:

- Source summary from `station_catalog.csv`, `source_station_catalog.csv`, and
  `satellite_catalog.csv`.
- Source dataset contribution table.
- Contribution by source group and source type.
- Contribution by resolution.
- Contribution by variable.
- Top-source rankings by record, station, and cluster counts.
- Cumulative contribution curves.
- Source classification template for manual review.

Important table outputs:

- `table_source_summary.csv`
- `table_source_resolution.csv`
- `table_satellite_source_resolution.csv`
- `table_source_dataset_contribution.csv`
- `table_source_type_contribution.csv`
- `table_source_resolution_contribution.csv`
- `table_source_variable_contribution.csv`
- `table_top_source_contributors.csv`
- `table_source_contribution_cumulative.csv`
- `table_source_temporal_coverage.csv`
- `table_report_key_metrics.csv`
- `source_classification_template.csv`

Important figure outputs:

- `fig_source_contribution_records.png`
- `fig_source_contribution_clusters.png`
- `fig_source_contribution_stations.png`
- `fig_source_cumulative_contribution.png`
- `fig_source_type_records.png`
- `fig_source_group_records.png`
- `fig_source_resolution_stacked.png`
- `fig_source_variable_stacked.png`
- `fig_source_temporal_coverage.png`
- Satellite and climatology source contribution figures.

### `source_dataset_layers.py`

`source_dataset_layers.py` tracks where each source appears across release
catalog layers.

Main analyses:

- Membership rows from `station_catalog.csv`, `source_station_catalog.csv`,
  `satellite_catalog.csv`, overlap sidecars, satellite sidecars, and satellite
  validation catalogs.
- Source-by-layer aggregation.
- Source-level rollup joined to `source_dataset_catalog.csv`.
- Unsupported pipeline-layer markers for release-only runs.

Important table outputs:

- `table_source_layer_membership.csv`
- `table_source_layer_summary.csv`
- `table_source_layer_source_rollup.csv`
- `table_source_layer_unsupported_pipeline_layers.csv`

### `basin_diagnostics.py`

`basin_diagnostics.py` evaluates basin assignment quality from the station
catalog.

Main analyses:

- Spatial match error classes and match-quality classes.
- Status, flag, quality, resolution, and distance-bin counts.
- Unresolved records by source and country.
- Resolved point anomalies.
- Distance-filter retention for already resolved assignments.
- Manual review queues for large offsets, area mismatches, geometry
  inconsistencies, and high-risk assignments.
- Remote-sensing exclusion summaries.
- Unknown-station tables.

Important table outputs:

- `table_basin_status_counts.csv`
- `table_basin_status_by_resolution.csv`
- `table_basin_status_by_distance.csv`
- `table_basin_spatial_match_error_table.csv`
- `table_basin_spatial_match_status_counts.csv`
- `table_basin_spatial_match_error_class_counts.csv`
- `table_basin_spatial_match_quality_counts.csv`
- `table_basin_spatial_match_distance_bins.csv`
- `table_basin_spatial_match_status_by_merit_basin_area_presence.csv`
- `table_basin_resolved_assignment_distance_filter_retention.csv`
- `table_basin_unresolved_by_source.csv`
- `table_basin_unresolved_by_country.csv`
- `table_basin_resolved_point_anomalies.csv`
- `table_basin_manual_review_largest_spatial_offsets.csv`
- `table_basin_manual_review_area_mismatch.csv`
- `table_basin_manual_review_geometry_inconsistent.csv`
- `table_basin_manual_review_high_risk.csv`
- `table_basin_remote_sensing_exclusion_summary.csv`
- `table_basin_unknown_stations.csv`

Important figure outputs:

- `basin_flag_counts.png`
- `spatial_error_class_counts.png`
- `distance_hist_logx.png`
- `unknown_points_map.png`
- `resolved_assignment_distance_filter_retention.png`

### `qc_flags.py`

`qc_flags.py` summarizes QC flag variables in release NetCDF products.

Main analyses:

- Flag counts for master, climatology, and satellite products.
- Product and flag-variable health summaries.
- Flag schema tables from NetCDF metadata.
- Legacy-compatible summary tables for variable, source, resolution, stage, and
  yearly trend review.
- Hotspot tables for records with the largest QC issue concentration.

Important table outputs:

- `table_qc_flag_counts.csv`
- `table_qc_health.csv`
- `table_qc_flag_schema.csv`
- `table_qc_flag_summary.csv`
- `table_qc_health_kpis.csv`
- `table_qc_flag_by_variable.csv`
- `table_qc_stage_effectiveness.csv`
- `table_qc_issue_hotspots.csv`
- `table_qc_yearly_trends.csv`
- Additional climatology and satellite subdirectory tables.

Important figure outputs:

- `fig_qc_flag_distribution.png`
- `fig_qc_health.png`
- `fig_qc_yearly_problem_trends.png`
- Additional climatology and satellite subdirectory figures.

### `run_all_release_stats.py`

`run_all_release_stats.py` is the suite entry point.

Default module order:

```text
inventory
spatial
temporal
source_dataset_layers
source_contribution
basin_diagnostics
variable_summary
qc_flags
```

Runner behavior:

- Calls module CLIs through `subprocess.run`.
- Accepts `--modules` to run a subset.
- Accepts `--continue-on-error` to continue after a module fails.
- Cleans managed module output directories by default.
- Accepts `--no-clean-output` to keep previous outputs.
- Forwards common flags such as release directory, DPI, figure skipping, report
  copying, and non-release input allowance.
- Builds `run_summary.csv`, `run_summary.md`, `run_manifest.csv`,
  `run_manifest.json`, and `parity_manifest.csv`.

## Data Flow

The normal data flow is:

```text
Release package
  station_catalog.csv
    -> spatial.py
    -> temporal.py
    -> basin_diagnostics.py
    -> source_contribution.py
    -> source_dataset_layers.py

  source_station_catalog.csv
    -> spatial.py
    -> source_contribution.py
    -> source_dataset_layers.py

  source_dataset_catalog.csv
    -> source_contribution.py
    -> source_dataset_layers.py

  satellite_catalog.csv
    -> spatial.py
    -> temporal.py
    -> source_contribution.py
    -> source_dataset_layers.py

  master_nc
    -> temporal.py
    -> variable_summary.py
    -> qc_flags.py
    -> inventory.py

  daily.nc, monthly.nc, annual.nc
    -> temporal.py
    -> inventory.py

  climatology_nc
    -> variable_summary.py
    -> qc_flags.py
    -> inventory.py

  satellite_nc
    -> variable_summary.py
    -> qc_flags.py
    -> inventory.py

  GeoPackage products
    -> spatial.py
    -> inventory.py

  release_inventory.csv
    -> inventory.py

  release_validation_report.csv
    -> inventory.py

  overlap and satellite sidecars
    -> source_dataset_layers.py
```

## Output Layout

A typical run writes:

```text
output_other/stats_release/
  run_summary.csv
  run_summary.md
  run_manifest.csv
  run_manifest.json
  parity_manifest.csv

  inventory/
    tables/
    reports/

  spatial/
    tables/
    figures/

  temporal/
    tables/
    figures/

  variable_summary/
    tables/
    figures/

  source_contribution/
    tables/
    figures/

  source_dataset_layers/
    tables/

  basin_diagnostics/
    tables/
    figures/
    reports/

  qc_flags/
    tables/
    figures/
```

## Review Checklist

After a run, review these files first:

1. `run_summary.csv`: all requested modules should have return code `0`.
2. `run_manifest.csv`: expected outputs should be present with file sizes and
   fingerprints.
3. `inventory/tables/release_inventory_mismatches.csv`: release inventory and
   files on disk should agree.
4. `inventory/tables/path_leaks.csv`: local absolute paths should not appear in
   release files.
5. `inventory/tables/active_metadata_consistency.csv`: active NetCDF metadata
   should align with release catalogs.
6. `spatial/tables/table_unknown_country_region_clusters.csv`: unknown spatial
   metadata should be reviewed.
7. `basin_diagnostics/tables/table_basin_manual_review_high_risk.csv`: high-risk
   basin assignments should be checked manually.
8. `variable_summary/tables/table_extreme_value_review_points.csv`: extreme
   values should be reviewed before publication.
9. `qc_flags/tables/table_qc_issue_hotspots.csv`: recurring QC issues should be
   investigated.

## Notes For Release-Only Interpretation

- Source-level records derived from `sources_used` can be non-exclusive because
  one merged cluster may reference multiple sources.
- Satellite validation records are analyzed through satellite-specific release
  products and are not treated as ordinary basin-assignment rows.
- Distance-filter retention in `basin_diagnostics.py` is a post hoc analysis of
  already resolved assignments. It is not a rerun of basin matching with
  different thresholds.
- The release-only suite intentionally marks intermediate pipeline products as
  unsupported when they are not included in the release package.

## Minimal Smoke Check

Use this command to check imports and CLI wiring without generating the full
statistics package:

```bash
python3 -m stats_release.run_all_release_stats --modules inventory --skip-figures
```
