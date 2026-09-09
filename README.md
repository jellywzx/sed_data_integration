# Sediment Reference Dataset Basin Pipeline

> This document describes the main `scripts_basin_test` pipeline.
>
> The pipeline turns multi-source sediment observations into a basin-based
> sediment reference dataset keyed by `cluster_uid + resolution`, then publishes
> it under `scripts_basin_test/output/sed_reference_release/`.

---

## 1. Project Overview

This repository builds a sediment-observation reference dataset. The workflow
standardizes quality-controlled NetCDF inputs from many sources, detects their
temporal resolution, traces upstream basins, merges stations with basin rules,
and produces a release package for model validation, nearest-station lookup, and
provenance tracing.

The final release package is written to:

```text
scripts_basin_test/output/sed_reference_release/
```

The primary release join key is:

```text
cluster_uid + resolution
```

A `cluster` is not always a single physical station. It is a station group
formed by the basin merge rules. A cluster can contain multiple source stations,
and the release keeps traceability back to source stations and original file
paths.

---

## 2. Release Contents

The release package mainly contains:

| Type | Standard files |
|---|---|
| Master NetCDF | `sed_reference_master.nc` |
| Matrix NetCDF | `sed_reference_timeseries_daily.nc`, `sed_reference_timeseries_monthly.nc`, `sed_reference_timeseries_annual.nc` |
| Climatology NetCDF | `sed_reference_climatology.nc` |
| Satellite NetCDF | `sed_reference_satellite.nc` |
| Catalogs | `station_catalog.csv`, `source_station_catalog.csv`, `source_dataset_catalog.csv`, `satellite_catalog.csv` |
| Overlap provenance | `sed_reference_overlap_candidates.csv.gz` |
| GIS sidecars | `sed_reference_cluster_points.gpkg`, `sed_reference_source_stations.gpkg`, optional `sed_reference_cluster_basins.gpkg` |
| Release validation | `release_validation_report.csv`, `release_inventory.csv`, release `README.md` |

The release can be understood as five layers:

1. `master`: based on `s6_basin_merged_all.nc`, preserving record-level provenance.
2. `matrix`: daily, monthly, and annual matrix NetCDF files for nearest-station lookup, time-series extraction, and model comparison.
3. `climatology`: based on `s6_climatology_only.nc`, published independently and excluded from the basin mainline merge.
4. `satellite`: based on the satellite source family, published as `sed_reference_satellite.nc` for satellite-vs-station validation, spatial diagnostics, and downstream comparison. It is excluded from the main station-reference merge by default.
5. `release`: a standard external package assembled by `s8_publish_reference_dataset.py`.

Notes:

1. Legacy names such as `sed_reference_satellite_validation.nc` and `satellite_validation_catalog.csv` are compatibility aliases if present. Prefer `sed_reference_satellite.nc` and `satellite_catalog.csv`.
2. `s8_publish_reference_dataset.py` requires the release-level satellite NetCDF and catalog to exist together. If either is missing, release generation should fail.
3. Satellite-only clusters may be absent from the main station catalog. Release validation checks that their `cluster_uid / cluster_id` values are self-consistent and reports how many can be linked to the main catalog.

---

## 3. Quick Start

### 3.1 Run The Main Pipeline

Use the unified entrypoint to run `s1 -> s8`:

```bash
python run_s1_s8_basin_pipeline.py --help
python run_s1_s8_basin_pipeline.py
```

Common examples:

```bash
# Run a continuous stage range.
python run_s1_s8_basin_pipeline.py --start-at s3 --end-at s6

# Run selected stages. --steps takes precedence over --start-at/--end-at.
python run_s1_s8_basin_pipeline.py --steps s4,s5,s8

# Print commands without executing them.
python run_s1_s8_basin_pipeline.py --steps s6,s7 --dry-run
```

### 3.2 Common Arguments

| Argument | Purpose |
|---|---|
| `--python` | Python 3 interpreter path |
| `--log-file` | Pipeline log file path |
| `--start-at` / `--end-at` | Run one continuous stage range |
| `--steps` | Run a comma-separated list of non-contiguous stages |
| `--dry-run` | Print commands without executing them |
| `--strict-s1` | Treat nonzero s1 exit status as fatal |
| `--s2-workers` | s2 worker count |
| `--s2-clear` | Pass `--clear-all` to s2 |
| `--s3-workers` | s3 worker count |
| `--s3-exclude-resolutions` | Exclude resolutions from the basin mainline, default `climatology` |
| `--s4-workers` | s4 basin-tracing worker count |
| `--s4-batch-size` | s4 batch size |
| `--s4-no-resume` | Disable s4 resume mode |
| `--s4-no-gpkg` | Disable s4 GPKG output |
| `--merit-dir` | MERIT Hydro data directory |
| `--s6-workers` | s6 master merge worker count |
| `--matrix-workers` | Total matrix-export worker budget |
| `--matrix-resolution-workers` | Per-resolution matrix worker setting |
| `--s6-include-climatology` | Include climatology in the s6 main merge |
| `--skip-climatology-export` | Skip independent climatology export |
| `--include-local-basins` | Also generate local-basin sidecars |
| `--s8-link-mode` | Release materialization mode: `hardlink`, `symlink`, or `copy` |
| `--s8-skip-gpkg` | Skip release-level GPKG files |
| `--s8-no-basin-polygons` | Do not publish the basin-polygon sidecar |
| `--s8-skip-validation` | Skip release validation |
| `--s8-no-force` | Do not overwrite an existing release directory |

---

## 4. Path Conventions

The repository is expected to live at:

```text
Output_r/scripts_basin_test/
```

The shared output directory is:

```text
scripts_basin_test/output/
```

The s2-organized resolution directory is:

```text
../output_resolution_organized/
```

The log directory is:

```text
scripts_basin_test/output/logs/
```

For cross-machine migration or execution from another directory, set:

```bash
export OUTPUT_R_ROOT=/path/to/Output_r
```

`s4` requires MERIT Hydro data. The default path is inferred by the scripts, or
it can be supplied explicitly:

```bash
python run_s1_s8_basin_pipeline.py \
  --steps s4 \
  --merit-dir /path/to/MERIT_Hydro_v07_Basins_v01_bugfix1
```

---

## 5. Main Pipeline

The current mainline is:

```text
s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7 -> s8
```

| Stage | Entrypoint | Main purpose | Key outputs |
|---|---|---|---|
| s1 | `s1_verify_time_resolution.py` | Verify input NetCDF temporal semantics and write the main classification result, manual review queue, and override template | `s1_verify_time_resolution_results.csv`, `s1_resolution_review_queue.csv`, `s1_resolution_review_overrides.csv` |
| s2 | `s2_reorganize_qc_by_resolution.py` | Reorganize QC files into standard resolution directories using s1 decisions | `../output_resolution_organized/`, `s2_resolution_classification_details.csv` |
| s3 | `s3_collect_qc_stations.py` | Scan organized NetCDF files, extract basin-mainline station metadata, and create stable internal `station_key` values | `s3_collected_stations.csv` |
| s4 | `s4_basin_trace_watch.py` or `submit_s4_lsf.sh` | Run upstream basin tracing for stations by `station_key` and write basin diagnostics | `s4_upstream_basins.csv`, `s4_upstream_basins.gpkg`, `s4_local_catchments.gpkg`, `s4_reported_area_check.csv` |
| s5 | `s5_basin_merge.py` | Merge s4 basin results by `station_key`, assign `cluster_id`, and write cluster-level station tables | `s5_basin_clustered_stations.csv`, `s5_basin_cluster_report.csv` |
| s6 | `submit_s6_fast.sh` or the s6 scripts | Build master, matrix, climatology, and satellite NetCDF products | `s6_basin_merged_all.nc`, `s6_matrix_by_resolution/*.nc`, `s6_climatology_only.nc`, `s6_satellite_validation_only.nc` |
| s7 | `s7_export_cluster_shp.py`, `s7_export_source_station_shp.py`, `s7_export_cluster_basin_shp.py` | Export cluster, source-station, and basin spatial sidecars and catalogs | `s7_cluster_points.gpkg`, `s7_source_stations.gpkg`, `s7_cluster_basins.gpkg`, related catalogs |
| s8 | `s8_publish_reference_dataset.py` | Assemble s6/s7 outputs into the standard release package and run release validation | `sed_reference_release/` |

---

## 6. Satellite Release Rules

Satellite data are a required part of a complete release:

```text
sed_reference_satellite.nc
satellite_catalog.csv
```

Design rules:

1. Select records from s5 candidates where `source_family == satellite`.
2. Keep only standard temporal resolutions: `daily / monthly / annual`.
3. Link to the main basin clusters through `cluster_uid / cluster_id`.
4. Do not include satellite records in `sed_reference_master.nc`.
5. Do not include satellite records in `sed_reference_timeseries_daily.nc`, `sed_reference_timeseries_monthly.nc`, or `sed_reference_timeseries_annual.nc`.
6. Use satellite data for satellite-vs-station validation, spatial diagnostics, and downstream comparison.
7. Do not treat the satellite NetCDF as a complete Q/SSC/SSL time-series product. Variable coverage differs by satellite source:
   - Dethier: Q/SSC/SSL are complete.
   - GSED: SSC only, with partial coverage; Q and SSL are absent or zero.
   - RiverSed: SSC only, with sparse coverage; Q and SSL are absent or zero.
   Filter by `source` and variable before using this product.
8. If the satellite NetCDF or satellite catalog is missing, release generation should fail instead of producing an incomplete release.

Current s6 satellite intermediate products:

```text
scripts_basin_test/output/s6_satellite_validation_only.nc
scripts_basin_test/output/s6_satellite_validation_catalog.csv
```

Expected s8 release outputs:

```text
scripts_basin_test/output/sed_reference_release/sed_reference_satellite.nc
scripts_basin_test/output/sed_reference_release/satellite_catalog.csv
```

Main satellite NetCDF dimensions:

```text
n_satellite_stations
n_satellite_records
n_sources
```

Common station-level fields:

```text
satellite_station_uid
cluster_uid
cluster_id_station
source
source_family
source_station_native_id
station_name
river_name
station_resolution
lat
lon
candidate_path
resolved_candidate_path
merge_policy
validation_only
```

Common record-level fields:

```text
satellite_station_index
cluster_id
time
date
resolution
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
```

Main satellite catalog fields:

```text
satellite_station_uid
cluster_uid
cluster_id
source
source_family
resolution
lat
lon
station_name
river_name
source_station_native_id
candidate_path
resolved_candidate_path
n_records
time_start
time_end
validation_only
merge_policy
```

---

## 7. Climatology Release Rules

Climatology is an independent reference layer in the release. It must be
published separately as:

```text
sed_reference_climatology.nc
```

Design rules:

1. Scan and export climatology files separately from `output_resolution_organized/climatology/`.
2. Do not include climatology records in basin tracing.
3. Do not include climatology records in basin cluster merging.
4. Do not include climatology records in `sed_reference_master.nc`.
5. Do not include climatology records in the daily, monthly, or annual matrix NetCDF products.
6. Do not use `cluster_uid + resolution` as the primary climatology index.
7. Use the climatology product's internal `station_uid` as the stable station key.
8. Use this layer for long-term means, climatological values, or records without clear daily, monthly, or annual time-series semantics.
9. Keep climatology analysis separate from daily, monthly, and annual matrix files unless the statistical meaning is explicitly aligned.

Current s6 climatology intermediate products:

```text
scripts_basin_test/output/s6_climatology_only.nc
scripts_basin_test/output/s6_climatology_stations.shp
```

Expected s8 release output:

```text
scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc
```

Release constraints:

1. `sed_reference_climatology.nc` is one of the core release NetCDF products.
2. Records in the file should correspond only to `climatology` resolution semantics.
3. Release validation should check climatology record counts, time coverage, and resolution-code consistency.
4. A complete release should fail if the climatology file is missing.
5. If the climatology file contains non-climatology resolution codes, treat it as a mixed run or upstream classification error and rerun from an earlier stage.

Common climatology NetCDF fields:

```text
station_uid
source_station_path
time
temporal_span
resolution
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
lat
lon
source
```

Recommended usage:

1. Read `sed_reference_climatology.nc` directly.
2. Use `station_uid` to locate a climatology station.
3. Use `source_station_path` to trace back to the original file.
4. Before model or station-matrix comparison, define the climatology statistic and the matching model-output window.
5. Do not automatically treat climatology as an observation at a daily, monthly, or annual time step.

---

## 8. Core Rules

### 8.1 Temporal-Resolution Rules

The final main categories are:

```text
daily / monthly / annual / climatology / other
```

Current mapping:

| Raw decision | Final category |
|---|---|
| `hourly` | `daily` |
| `daily` | `daily` |
| `single_point` | `daily` or `climatology` |
| `monthly` | `monthly` |
| `quarterly` | `monthly` |
| `annual` | `annual` or `climatology` |
| unclear cases | `other` |

Additional notes:

1. A `single_point` record is classified as `climatology` if metadata indicates a long-term mean or climatological statistic.
2. An `annual` record is classified as `climatology` if metadata indicates a long-term mean or climatological statistic.
3. `climatology` is kept separately after s2 and is excluded from the basin mainline by default.
4. The basin mainline processes non-climatology stations by default.
5. s2 does not trust the input directory name directly; it uses `temporal_semantics` from s1.

### 8.2 Cluster Rules

1. One `cluster` may contain multiple source stations.
2. The final mainline product must keep cluster, source-station, and record layers.
3. Final records must be traceable to `source_station_uid` and the original file path.
4. The release-level standard join key is `cluster_uid + resolution`.

### 8.3 Provenance Rules

The final data should support these tracebacks:

1. Which source stations are in each `cluster`.
2. Which source dataset each source station came from.
3. Which `source_station_uid` produced each final record.
4. Which original file path produced each final record.
5. When multiple sources compete, which candidates existed and which source won.

Main provenance files:

```text
source_station_catalog.csv
source_dataset_catalog.csv
sed_reference_overlap_candidates.csv.gz
```

### 8.4 Multi-Source Overlap Rules

When multiple sources exist for the same `cluster`, `resolution`, and time step:

1. Multiple sources may enter the candidate pool.
2. The final record layer keeps only one winning record.
3. Winners are selected by quality-score ordering.
4. `is_overlap = 1` means the record came from a multi-source competition.
5. The master and matrix NetCDF products store only winning records.
6. Use `sed_reference_overlap_candidates.csv.gz` for true source-pair overlap analysis.

### 8.5 Basin Release Policy

The current basin release policy is conservative:

1. The release layer only distinguishes `resolved` and `unresolved`.
2. `unresolved` records may remain in the main data.
3. Inclusion in the basin-polygon sidecar is an s7/s8 release-layer rule, not an s5 merge rule.
4. Only records with `basin_status=resolved` and valid basin polygons enter the basin-polygon sidecar.

Key basin diagnostic fields:

```text
distance_m
match_quality
point_in_local
point_in_basin
basin_status
basin_flag
```

Geometry and policy responsibilities:

1. `basin_tracer.py` computes geometry diagnostics and writes `point_in_local` and `point_in_basin`.
2. `basin_policy.py` reads diagnostics and returns the final `resolved / unresolved` decision.
3. Geometry checks use original station coordinates directly.
4. The pipeline does not snap or modify original latitude and longitude values.
5. Point-in-polygon checks use `covers()` rather than `contains()`, so boundary points count as inside.
6. `s4 / s5 / s6 / s7` pass through and write these diagnostics instead of recomputing them.

### 8.6 Basin Cluster Merge Rules

High-impact rules in `s5_basin_merge.py`:

1. Only stations with `basin_status=resolved` and valid `basin_id` values can participate in basin cluster merging.
2. Within the same `basin_id`, two candidate clusters merge only when all cross-cluster station pairs satisfy both the distance threshold and the `uparea_merit` relative-error threshold.
3. The merge style is `complete-linkage`.
4. Stations that fail the merge criteria remain singletons with `cluster_id=station_id`.
5. `s5` merges basin metadata back into the station table and masks selected release-facing basin fields for `unresolved` rows.

---

## 9. Data Structure

### 9.1 Cluster Layer

Common key fields:

```text
cluster_uid
cluster_id
lat
lon
basin_area
pfaf_code
basin_status
basin_flag
basin_distance_m
point_in_local
point_in_basin
n_source_stations_in_cluster
```

### 9.2 Source-Station Layer

Common key fields:

```text
source_station_uid
source_station_native_id
source_station_name
source_station_river_name
source_station_lat
source_station_lon
source_station_paths
source_station_resolutions
```

### 9.3 Observation-Record Layer

Common key fields:

```text
station_index
source_station_index
time
resolution
Q
SSC
SSL
source
is_overlap
```

---

## 10. Production Runs For s4 And s6

### 10.1 s4: Basin Tracing

Use the LSF submitter for production runs:

```bash
bash submit_s4_lsf.sh
bash submit_s4_lsf.sh 16
```

`submit_s4_lsf.sh` is a compatibility entrypoint. It calls
`submit_s4_lsf.py` and submits a three-stage LSF workflow:

1. Run `s4_trace[1-N]` array shards.
2. Submit the finalize job that merges all shard outputs.
3. Submit the summary job after finalize completes.

Common environment variables:

```text
S4_QUEUE
S4_NCORES
S4_MEM
S4_PTILE
PYTHON_BIN
```

Log directory:

```text
scripts_basin_test/output/logs/s4_lsf/
```

Shard intermediate directory:

```text
scripts_basin_test/output/s4_shards/
```

s4 shard resume is protected by an input-fingerprint manifest. Each shard writes
`s4_shard_XXX.meta.json`, recording the s3 CSV SHA256, s3 row count,
`shard_count`, `shard_index`, `MERIT_DIR`, the s4 script SHA256, and key runtime
settings. With `S4_RESUME=1`, existing work/shard files must match the manifest
exactly. Legacy shards without a manifest are rejected. Use `S4_RESUME=0` to
recompute the current shard; s4 only removes that shard's work CSV, completed
CSV, and manifest.

For local debugging:

```bash
python s4_basin_trace_watch.py
```

Or run through the unified entrypoint:

```bash
python run_s1_s8_basin_pipeline.py --steps s4
```

The unified entrypoint calls `submit_s4_lsf.py --wait` by default, submits S4 to
LSF, and waits for the summary job before the next stage. Use local execution
when needed:

```bash
python run_s1_s8_basin_pipeline.py --steps s4 --local-s4
```

### 10.2 s6: NetCDF Export

Recommended production command:

```bash
python submit_s6_fast.py --wait
```

`bash submit_s6_fast.sh` remains as a compatibility entrypoint and calls the
Python submitter. `s6` is a set of parallel jobs rather than a single script.
`submit_s6_fast.py` currently submits:

| Subtask | Script | Products |
|---|---|---|
| merge | `s6_basin_merge_to_nc.py` | `s6_basin_merged_all.nc`, `s6_cluster_quality_order.csv` |
| daily | `s6_export_daily_matrix_nc.py` | `s6_basin_matrix_daily.nc` |
| monthly | `s6_export_monthly_matrix_nc.py` | `s6_basin_matrix_monthly.nc` |
| annual | `s6_export_annual_matrix_nc.py` | `s6_basin_matrix_annual.nc` |
| clim | `s6_export_climatology_to_nc.py` | `s6_climatology_only.nc` |
| satellite | `s6_export_satellite_validation_to_nc.py` | `s6_satellite_validation_only.nc`, `s6_satellite_validation_catalog.csv` |

The submitter also submits a dependent `summary` job to check required outputs.
The unified entrypoint calls `submit_s6_fast.py --wait` by default and waits for
S6 cluster tasks before entering S7. Use `--local-s6` for local sequential runs.

Common environment variables:

```text
RUN_ONLY
DRY_RUN
LSF_QUEUE
LSF_PROJECT
LSF_EXTRA
MERGE_N
MERGE_WORKERS
MERGE_METADATA_WORKERS
DAILY_N
DAILY_WORKERS
MONTHLY_N
MONTHLY_WORKERS
ANNUAL_N
ANNUAL_WORKERS
CLIM_N
SATVAL_N
```

---

## 11. Spatial Files

The current mainline uses `GPKG` for spatial products. Standard release spatial
files are produced by `s8_publish_reference_dataset.py`.

### 11.1 Cluster Point Files

Files:

```text
s7_cluster_points.gpkg
sed_reference_release/sed_reference_cluster_points.gpkg
```

Purpose:

1. Provide `cluster_summary / cluster_daily / cluster_monthly / cluster_annual` layers.
2. Connect NetCDF, catalog, and spatial data across resolutions.
3. Use `cluster_uid + resolution` as the standard join key.

### 11.2 Source-Station Point Files

Files:

```text
s7_source_stations.gpkg
sed_reference_release/sed_reference_source_stations.gpkg
```

Purpose:

1. Show source stations participating in a `cluster_uid + resolution`.
2. Use `source_station_uid + resolution` as the standard join key.

### 11.3 Cluster-Level Basin Polygon Files

Files:

```text
s7_cluster_basins.gpkg
sed_reference_release/sed_reference_cluster_basins.gpkg
```

Purpose:

1. Show the final basin polygon for each `cluster_uid + resolution`.
2. Link spatially with cluster point files and release catalogs through the compound key.
3. Export basin polygons only for `basin_status=resolved` records.
4. Keep `unresolved` records in the main data while excluding them from the basin-polygon sidecar.

---

## 12. Release Package Layout

Example complete release directory:

```text
scripts_basin_test/output/sed_reference_release/
|-- sed_reference_master.nc
|-- sed_reference_timeseries_daily.nc
|-- sed_reference_timeseries_monthly.nc
|-- sed_reference_timeseries_annual.nc
|-- sed_reference_climatology.nc
|-- sed_reference_satellite.nc
|-- station_catalog.csv
|-- source_station_catalog.csv
|-- source_dataset_catalog.csv
|-- satellite_catalog.csv
|-- sed_reference_overlap_candidates.csv.gz
|-- sed_reference_cluster_points.gpkg
|-- sed_reference_source_stations.gpkg
|-- sed_reference_cluster_basins.gpkg
|-- release_validation_report.csv
|-- release_inventory.csv
`-- README.md
```

File roles:

1. `sed_reference_master.nc` preserves record-level provenance for auditing and traceback.
2. `sed_reference_timeseries_*.nc` are `station x time` matrices for nearest-station lookup, time-series extraction, and model comparison.
3. `sed_reference_climatology.nc` is published independently and excluded from the basin mainline merge.
4. `sed_reference_satellite.nc` is a required release-level satellite dataset, excluded from the main station-reference merge.
5. `station_catalog.csv` is the release main index, one row per `cluster_uid + resolution`.
6. `source_station_catalog.csv` supports source-station traceback.
7. `source_dataset_catalog.csv` supports source-dataset metadata lookup.
8. `satellite_catalog.csv` supports satellite station, source, time range, and original-path traceback.
9. `sed_reference_overlap_candidates.csv.gz` supports source-pair overlap-candidate analysis.
10. `sed_reference_cluster_points.gpkg` provides cluster point layers.
11. `sed_reference_source_stations.gpkg` provides source-station point layers.
12. `sed_reference_cluster_basins.gpkg` provides the resolved basin-polygon sidecar.
13. `release_validation_report.csv` and `release_inventory.csv` support release checks.

---

## 13. Standard Release Usage

Recommended downstream workflow:

1. Choose the target temporal resolution from model output or analysis goals: `daily / monthly / annual`.
2. Read the matching matrix NetCDF:
   - `sed_reference_timeseries_daily.nc`
   - `sed_reference_timeseries_monthly.nc`
   - `sed_reference_timeseries_annual.nc`
3. Read `station_catalog.csv` and filter to the target `resolution`.
4. Use filtered `lat/lon` values or `sed_reference_cluster_points.gpkg` to find the nearest `cluster_uid`.
5. Extract the `Q / SSC / SSL` time series for that `cluster_uid` from the matrix NetCDF.
6. Align the reference time series with model output by time.
7. Query `sed_reference_master.nc` when full record-level provenance is needed.
8. Query `source_station_catalog.csv` when source stations and original paths are needed.
9. Read `sed_reference_climatology.nc` separately for climatology values, and do not mix it automatically into matrix time series.
10. Read `sed_reference_satellite.nc` and `satellite_catalog.csv` for satellite-vs-station validation or spatial diagnostics.
11. Use `sed_reference_overlap_candidates.csv.gz` for true source-pair overlap metrics.

Example reference workflow:

```bash
python tools/example_reference_workflow.py \
  --release-dir /path/to/sed_reference_release \
  --resolution monthly \
  --lat 30.5 \
  --lon 114.3 \
  --variable SSC
```

Optional model comparison:

```bash
python tools/example_reference_workflow.py \
  --release-dir /path/to/sed_reference_release \
  --resolution monthly \
  --lat 30.5 \
  --lon 114.3 \
  --variable SSC \
  --model-nc /path/to/model.nc \
  --model-var sediment \
  --out-csv /tmp/aligned_timeseries.csv
```

---

## 14. Recommended Run Order

For a full rerun:

```text
s1_verify_time_resolution.py
s2_reorganize_qc_by_resolution.py
s3_collect_qc_stations.py
submit_s4_lsf.sh
submit_s4_lsf.py
s5_basin_merge.py
submit_s6_fast.sh
submit_s6_fast.py
s7_export_cluster_shp.py
s7_export_source_station_shp.py
s7_export_cluster_basin_shp.py
s8_publish_reference_dataset.py
```

For debugging or single-step execution:

```text
s4_basin_trace_watch.py
s5_basin_merge.py
s6_basin_merge_to_nc.py
s6_export_daily_matrix_nc.py
s6_export_monthly_matrix_nc.py
s6_export_annual_matrix_nc.py
s6_export_climatology_to_nc.py
s6_export_satellite_validation_to_nc.py
s7_export_cluster_shp.py
s7_export_source_station_shp.py
s7_export_cluster_basin_shp.py
s8_publish_reference_dataset.py
```

The unified entrypoint is preferred:

```bash
python run_s1_s8_basin_pipeline.py --start-at s1 --end-at s8
```

---

## 15. When To Rerun

| Change | Suggested rerun range | Reason |
|---|---|---|
| Temporal-resolution rules change | From s2 | s2 changes organized directories; later station tables, `station_key`, and run-local `station_id` values may change |
| `single_point / quarterly / annual / climatology` classification logic changes | From s2 | Files entering each resolution directory may change |
| Basin tracing or `basin_status` rules change | From s4 | s4 regenerates basin diagnostics used by all later stages |
| Cluster merge rules change | From s5 | `cluster_id / cluster_uid` values may change |
| s6 output fields or release contract changes | At least s6 -> s8 | Master, matrix, climatology, satellite, and release outputs must stay consistent |
| Climatology classification rules or schema change | At least s6 -> s8; from s2 if needed | The independent climatology product depends on s2 classification and s6 export |
| Satellite source-family or satellite schema changes | At least s6 -> s8 | Satellite NetCDF and catalog are required release products |
| Only release naming, link mode, or GPKG toggles change | Usually s8 only | s8 handles release materialization and validation |

Notes:

1. `s2` changes the organized file directory.
2. `s3` rebuilds the station list.
3. `s3` creates stable internal `station_key` values from normalized `source`, `resolution`, and relative `path`.
4. `station_id` is only the reproducible integer index in the current s3 output; s4/s5 do not recreate it from row numbers.
5. s4 shards can resume only when the s3 fingerprint, `MERIT_DIR`, s4 script fingerprint, and key runtime settings match.

When in doubt about upstream changes, rerun from the earlier stage.

---

## 16. Dependencies

Create the Conda environment with:

```bash
conda env create -f environment.yml
conda activate sed-reference-basin
```

Or install into an existing Python environment:

```bash
python -m pip install -r requirements.txt
```

Common Python dependencies:

```text
pandas
numpy
xarray
netCDF4
h5netcdf
h5py
geopandas
fiona
pyogrio
pyproj
shapely
pyshp
matplotlib
cartopy
scipy
PyYAML
```

Additional notes:

1. `s7_export_cluster_shp.py` needs `pyshp + geopandas`.
2. `s7_export_source_station_shp.py` and `s7_export_cluster_basin_shp.py` need `geopandas`.
3. `s8_publish_reference_dataset.py` needs `geopandas` when publishing GPKG sidecars.
4. Example workflows need `netCDF4`; model comparison usually needs `xarray`; plotting needs `matplotlib`.

---

## 17. Public Release And Citation

For ESSD/Zenodo publication, this repository should track code, documentation,
tests, configuration templates, and small static resources. Large intermediate
outputs, NetCDF files, GPKG files, logs, generated reports, and figures should
be published through Zenodo or another data repository, with the DOI cited in
the manuscript and release README.

Suggested pre-release checks:

```bash
git status --short
python -m py_compile *.py
pytest test
```

Also scan public files for local directory prefixes and credential keywords
required by your institution, and confirm that personal workspace paths or
credentials are not exposed.

Code citation metadata is in `CITATION.cff`. Source code uses the MIT license by
default. Released data, figures, and documentation should continue to use CC BY
4.0 and remain consistent with the license text in NetCDF global attributes.

---

## 18. Code Navigation

| File | Purpose |
|---|---|
| `run_s1_s8_basin_pipeline.py` | Unified s1-s8 entrypoint with range runs, selected-stage runs, and dry-run support |
| `pipeline_paths.py` | Central output, release-package, and log-path constants |
| `time_resolution.py` | Temporal-resolution classification logic |
| `basin_tracer.py` | Upstream basin tracing and point-in-polygon diagnostics |
| `basin_policy.py` | Release policy mapping basin diagnostics to `resolved / unresolved` |
| `s1_verify_time_resolution.py` | s1 temporal-resolution verification |
| `s2_reorganize_qc_by_resolution.py` | s2 input-file reorganization by resolution |
| `s3_collect_qc_stations.py` | s3 basin-mainline station collection |
| `s4_basin_trace_watch.py` | s4 basin-tracing main script |
| `submit_s4_lsf.py` | Python LSF submitter for S4 array/finalize/summary jobs |
| `s5_basin_merge.py` | s5 basin cluster merge |
| `s6_basin_merge_to_nc.py` | s6 master NetCDF export |
| `s6_export_daily_matrix_nc.py` | s6 daily matrix NetCDF export |
| `s6_export_monthly_matrix_nc.py` | s6 monthly matrix NetCDF export |
| `s6_export_annual_matrix_nc.py` | s6 annual matrix NetCDF export |
| `s6_export_climatology_to_nc.py` | Independent climatology NetCDF export |
| `s6_export_satellite_validation_to_nc.py` | Satellite-only NetCDF and catalog export |
| `submit_s6_fast.py` | Python LSF submitter for S6 master/matrix/climatology/satellite/summary jobs |
| `s7_export_cluster_shp.py` | Cluster point GPKG and catalog export |
| `s7_export_source_station_shp.py` | Source-station GPKG and catalog export |
| `s7_export_cluster_basin_shp.py` | Cluster basin polygon GPKG export |
| `s8_publish_reference_dataset.py` | Standard release package generation, release README, validation, and inventory |

---

## 19. Non-Mainline Scripts

Some legacy, compatibility, or helper scripts remain in this directory, for
example:

```text
s4_cluster_qc_stations.py
s6_merge_timeseries_by_cluster.py
s7_merge_overlap_by_cluster.py
s8_merge_qc_csv_to_one_nc.py
s6_summarize_matrix_ncs.py
```

These scripts are not part of the current `s1 -> s8` mainline build. Manual QA
and audit scripts are also outside the mainline release contract. Use their
script-level documentation and related validation notes when needed.

---

## 20. Summary

The main pipeline builds a basin-based sediment reference dataset with
`s1 -> s8`. `daily / monthly / annual` records enter the basin mainline,
`climatology` is exported as an independent release product, and `satellite` is
published as a required independent release-level NetCDF dataset. Mainline and
climatology release records must contain at least `SSC` or `SSL`; Q-only time
steps are not published. Production `s4` and `s6` runs should use
`submit_s4_lsf.sh` and `submit_s6_fast.sh`. The release layer uses
`cluster_uid + resolution` as the standard join key and preserves the master
NetCDF, matrix NetCDF, climatology, satellite, catalogs, spatial sidecars, and
overlap provenance. Basin polygon sidecars are published only for `resolved`
results.
