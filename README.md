# Sediment Reference Dataset Public Station Package

This repository builds the sediment reference dataset and prepares the
station-facing public package used for publication. The public package described
here is the final output of:

```text
s9_public_station_names.py
```

The default S9 input directory is:

```text
scripts_basin_test/output/sed_reference_release_minimal/
```

The default S9 final output directory is:

```text
scripts_basin_test/output/sed_reference_release_minimal_final/
```

S9 converts the public minimal release to station-facing schema names, copies
the minimal example workflow, writes a conversion report, and optionally fails
in strict mode if old public schema names remain.

## Quick Start

Run the main processing pipeline through S8:

```bash
python run_s1_s8_basin_pipeline.py
```

Build the final station-facing public package:

```bash
python s9_public_station_names.py --strict
```

Use custom input and output directories if needed:

```bash
python s9_public_station_names.py \
  --release-dir output/sed_reference_release_minimal \
  --output-dir output/sed_reference_release_minimal_final \
  --strict
```

The public example workflow copied by S9 can then be run from the final package:

```bash
python output/sed_reference_release_minimal_final/example_reference_workflow.py \
  --release-dir output/sed_reference_release_minimal_final \
  --resolution monthly \
  --lat 30.5 \
  --lon 114.3 \
  --variable SSC
```

## Final Package Contents

The S9 final package contains these release-facing files:

```text
README.md
example_reference_workflow.py
public_station_names_report.csv
release_inventory.csv
release_validation_report.csv
station_catalog.csv
source_station_catalog.csv
source_dataset_catalog.csv
climatology_catalog.csv
satellite_catalog.csv
sed_reference_timeseries_daily.nc
sed_reference_timeseries_monthly.nc
sed_reference_timeseries_annual.nc
sed_reference_climatology.nc
sed_reference_satellite.nc
```

This final minimal package does not publish the internal master NetCDF, overlap
candidate table, or spatial sidecar files. Those products may exist in internal
S8 outputs, but they are outside the S9 final public field contract described
below.

## Primary Public Keys

The public package uses station-facing identifiers:

- `station_uid`: released station identifier used by the main matrix products,
  `station_catalog.csv`, climatology products, and linked satellite products.
- `resolution`: temporal support class, normally `daily`, `monthly`, or
  `annual` for the main matrix products.
- `source_station_uid`: source-station identifier used for provenance lookup.
- `satellite_station_uid`: satellite-derived station identifier used inside the
  satellite auxiliary product.
- `linked_station_uid`: linked main-component station identifier for satellite
  records when a spatial link is available.

For the main station-reference component, the practical catalog key is:

```text
station_uid + resolution
```

For source-station provenance, join:

```text
selected_source_station_uid -> source_station_catalog.csv:source_station_uid
```

For source-dataset provenance, join:

```text
source_station_catalog.csv:source_name -> source_dataset_catalog.csv:source_name
```

## Core Variables And Flags

The physical variables are:

| Variable | Meaning | Release unit |
| --- | --- | --- |
| `Q` | River discharge | m3 s-1 |
| `SSC` | Suspended sediment concentration | mg L-1 |
| `SSL` | Suspended sediment load | t d-1 |

Each variable has a matching quality flag:

```text
Q_flag
SSC_flag
SSL_flag
```

The public flag meanings are:

| Flag | Meaning | Suggested use |
| ---: | --- | --- |
| 0 | Good | Analysis-ready reported value |
| 1 | Derived | Analysis-ready derived value |
| 2 | Suspect | Retain only for sensitivity checks |
| 3 | Bad | Exclude from normal analysis |
| 9 | Missing | No value available |

Flags `0` and `1` are the default analysis-ready set.

## CSV Catalog Fields

The following field lists are read from the S9 final output package.

### `station_catalog.csv`

One row per released station and temporal resolution.

```text
station_uid
resolution
lat
lon
country
time_start
time_end
record_count
n_valid_time_steps
basin_area
pfaf_code
n_upstream_reaches
station_name
river_name
```

Recommended use:

- Filter by `resolution`.
- Use `lat` and `lon` for nearest-station search.
- Use `station_uid` to extract the matching NetCDF row.
- Use `basin_area`, `pfaf_code`, and `n_upstream_reaches` only when basin
  assignment is resolved and values are present.

### `source_station_catalog.csv`

Source-station provenance table.

```text
source_station_uid
source_name
source_station_native_id
source_station_name
source_station_river_name
source_station_lat
source_station_lon
station_uid
resolution
n_records
time_start
time_end
```

Recommended use:

- Join matrix variable `selected_source_station_uid` to `source_station_uid`.
- Use `source_name` to join `source_dataset_catalog.csv`.
- Use native IDs and source coordinates for source-level traceback.

### `source_dataset_catalog.csv`

Source dataset metadata table.

```text
source_name
reference
source_url
n_source_stations
n_records
```

Recommended use:

- Join from `source_station_catalog.csv` or `satellite_catalog.csv` by
  `source_name` or `source`.
- Use `reference` and `source_url` for data-source acknowledgement and
  provenance review.

### `climatology_catalog.csv`

Query catalog for the climatology auxiliary component.

```text
station_uid
time
time_raw
time_start
time_end
resolution
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
station_name
river_name
lat
lon
geographic_coverage
source_name
```

Recommended use:

- Treat climatology records separately from daily, monthly, and annual matrix
  records.
- Use `station_uid`, `lat`, `lon`, and `geographic_coverage` for spatial
  lookup.
- Use `time_raw`, `time_start`, and `time_end` to interpret the climatological
  support period.

### `satellite_catalog.csv`

Query catalog for the satellite-derived auxiliary component.

```text
satellite_station_uid
station_name
river_name
source
resolution
time_start
time_end
n_records
lat
lon
geographic_coverage
station_uid
linked_station_uid
unlinked_reason
link_distance_m
```

Recommended use:

- Use `satellite_station_uid` as the satellite product station key.
- Use `linked_station_uid` when a satellite location is linked to a main
  station-reference location.
- Use `unlinked_reason` to separate unlinked satellite records.
- Use `link_distance_m` to screen satellite-to-station spatial matches.

### `release_inventory.csv`

Package inventory written during release assembly.

```text
package
file
source_path
source_exists
status
source_release_version
source_release_date_created
source_release_date_modified
packaging_script
schema_path
package_created_at
```

### `release_validation_report.csv`

Structural validation summary.

```text
check
status
message
evidence
```

### `public_station_names_report.csv`

S9 conversion and audit report.

```text
file
product_type
action
status
old_name
new_name
details
```

Review this report after every S9 run. In strict production runs, any residual
old public schema name should be treated as a release-blocking failure.

## NetCDF Product Fields

### `sed_reference_timeseries_daily.nc`

Dimensions:

```text
n_stations
time
```

Variables:

```text
lat
lon
station_uid
time
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
n_valid_time_steps
selected_source_station_uid
basin_area
station_name
river_name
```

The daily product currently contains 7,087 stations and 25,775 time steps in
the generated S9 final package inspected for this README.

### `sed_reference_timeseries_monthly.nc`

Dimensions:

```text
n_stations
time
```

Variables:

```text
lat
lon
station_uid
time
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
n_valid_time_steps
selected_source_station_uid
basin_area
station_name
river_name
```

The monthly product currently contains 17 stations and 690 time steps in the
generated S9 final package inspected for this README.

### `sed_reference_timeseries_annual.nc`

Dimensions:

```text
n_stations
time
```

Variables:

```text
lat
lon
station_uid
time
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
n_valid_time_steps
selected_source_station_uid
basin_area
station_name
river_name
```

The annual product currently contains 31 stations and 114 time steps in the
generated S9 final package inspected for this README.

### `sed_reference_climatology.nc`

Dimensions:

```text
n_stations
n_records
```

Variables:

```text
lat
lon
station_uid
station_name
river_name
geographic_coverage
station_index
time
time_coverage_start
time_coverage_end
resolution
Q
Q_flag
SSC
SSC_flag
SSL
SSL_flag
source
```

The climatology product currently contains 1,361 stations and 1,361 records in
the generated S9 final package inspected for this README.

### `sed_reference_satellite.nc`

Dimensions:

```text
n_satellite_stations
n_satellite_records
```

Variables:

```text
satellite_station_uid
station_uid
linked_station_uid
unlinked_reason
source
station_name
river_name
station_resolution
link_distance_m
lat
lon
satellite_station_index
time
Q
SSC
SSL
Q_flag
SSC_flag
SSL_flag
```

The satellite product currently contains 38,550 satellite stations and
16,478,276 records in the generated S9 final package inspected for this README.

## Component Notes

### Main Station-Reference Component

The main component is distributed as daily, monthly, and annual station-by-time
NetCDF matrix products. These products are intended for station-level time
series extraction, model comparison, and source-traceable benchmarking.

Do not combine daily, monthly, and annual records without first accounting for
their different temporal support.

### Climatology Auxiliary Component

The climatology component is published separately from the time-resolved matrix
products. Use it for long-term or climatological context. Do not treat
climatology records as direct replacements for daily, monthly, or annual
observations.

### Satellite-Derived Auxiliary Component

The satellite-derived component is published separately from the main
station-reference component. Use it for satellite-to-station comparison, spatial
coverage diagnostics, and complementary sediment context.

Satellite records may have no linked main station. Use `linked_station_uid`,
`unlinked_reason`, and `link_distance_m` before performing station comparisons.

## Recommended Downstream Workflow

1. Choose a target `resolution`: `daily`, `monthly`, or `annual`.
2. Open the matching `sed_reference_timeseries_*.nc` product.
3. Read `station_catalog.csv` and filter to the same `resolution`.
4. Find candidate stations with `lat`, `lon`, `station_name`, `river_name`, and
   any basin fields needed by the analysis.
5. Select a `station_uid`.
6. Extract `Q`, `SSC`, `SSL`, and their flags from the NetCDF row with the same
   `station_uid`.
7. Use `selected_source_station_uid` to join `source_station_catalog.csv` when
   record-level source provenance is needed.
8. Use `source_dataset_catalog.csv` for source references and URLs.
9. Read `sed_reference_climatology.nc` and `climatology_catalog.csv` separately
   for climatology context.
10. Read `sed_reference_satellite.nc` and `satellite_catalog.csv` separately for
   satellite-derived comparison.

## Pipeline Summary

The processing pipeline before S9 performs temporal-resolution classification,
QC-file organization, station metadata collection, basin tracing, basin-based
station consolidation, NetCDF export, optional spatial export, and release
assembly.

The public station-facing package is produced after these steps by S9. S9 is the
schema boundary for public users, so public documentation should use the field
names listed in this README.

## Main Entrypoints

| File | Purpose |
| --- | --- |
| `run_s1_s8_basin_pipeline.py` | Unified S1-S8 pipeline runner |
| `s1_verify_time_resolution.py` | Temporal-resolution verification |
| `s2_reorganize_qc_by_resolution.py` | QC-file organization by resolution |
| `s3_collect_qc_stations.py` | Station metadata collection |
| `s4_basin_trace_watch.py` | Basin tracing |
| `s5_basin_merge.py` | Basin-based station consolidation |
| `submit_s4_lsf.py` | S4 LSF submitter |
| `submit_s6_fast.py` | S6 LSF submitter |
| `s8_publish_reference_dataset.py` | Internal release package assembly |
| `s9_public_station_names.py` | Final public station-facing schema conversion |
| `release_public_station_names.py` | S9 conversion implementation |
| `tools/example_reference_workflow_minimal.py` | Source for the public example workflow copied by S9 |

## Verification Commands

Compile Python scripts:

```bash
python -m py_compile *.py stats_release/*.py tools/*.py
```

Run S9 in strict mode:

```bash
python s9_public_station_names.py --strict
```

Scan public documentation for non-English characters before publication:

```bash
rg -n "[\\x{4e00}-\\x{9fff}]|[\\x{3000}-\\x{303F}\\x{FF00}-\\x{FFEF}]" \
  README.md stats_release/*.md
```

Check Git whitespace issues:

```bash
git diff --check
```

## Citation And License

Code citation metadata is stored in `CITATION.cff`. Source code uses the MIT
license. Released data, figures, and documentation should remain consistent with
the dataset release license and the license metadata written into the public
products.
