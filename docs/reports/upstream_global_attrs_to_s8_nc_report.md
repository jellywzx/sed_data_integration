# Upstream Global Attributes in S8 Release NetCDF Products

## Purpose

This report documents how upstream NetCDF global attributes from the
`sediment_wzx_1111/Script` processing chain are represented in the S8 release
products, and records the code change made on branch
`test/upstream-global-attrs-s8` so users can query station metadata directly
from release-level NetCDF files.

## Current Gap

The upstream station NetCDF files keep many useful global attributes. Before
this change, the S8 release NetCDF products did not preserve the full upstream
attribute set at station level. Several fields were available in release CSV
catalogs, but users had to query CSV sidecars or trace `source_station_paths`
back to upstream files to recover complete metadata.

A read-only audit of the current release found:

- Upstream source NetCDF files scanned from `source_station_catalog.csv`: 3776
- Distinct upstream global attribute keys: 65
- Keys present with the same name in `sed_reference_master.nc` global attrs or variables: 7
- Keys not present with the same name in `sed_reference_master.nc`: 58

The same-name count is conservative. Some fields are carried under renamed
variables, for example `station_id` as `source_station_native_id`, and
`temporal_span` as `source_station_temporal_span`.

## Fields Already Carried Or Partially Carried

These fields are represented directly or semantically in current master output:

- `station_name` -> `station_name` / `source_station_name`
- `river_name` -> `river_name` / `source_station_river_name`
- `station_id` -> `source_station_id` / `source_station_native_id`
- `data_source_name`, `dataset_name` -> `source_name` / `source_long_name`
- `creator_institution`, `contributor_institution` -> `institution`
- `reference*` -> `reference`
- `source_data_link`, `source_url`, `sediment_data_source`, `discharge_data_source` -> `source_url`
- `temporal_span` -> `source_station_temporal_span`
- `time_coverage_start` -> `source_station_time_coverage_start`
- `time_coverage_end` -> `source_station_time_coverage_end`
- `summary` -> `source_station_summary`
- `comment` -> `source_station_comment`
- `variables_provided` -> `source_station_variables_provided`
- `data_limitations` -> `source_station_data_limitations`
- `temporal_resolution` -> `source_station_declared_temporal_resolution`

Product-level attributes such as `title`, `source`, `history`, and
`Conventions` are rewritten by S6/S8 as release-product metadata. Their
presence does not mean each upstream file's original value was preserved.

## Fields Missing From Release-Level NetCDF Direct Query

Important fields that were not directly queryable from release NetCDF products
before this change include:

- `country`
- `continent_region`
- `geographic_coverage`
- `iso_a3`
- `geospatial_lat_min`, `geospatial_lat_max`
- `geospatial_lon_min`, `geospatial_lon_max`
- `geospatial_vertical_min`, `geospatial_vertical_max`
- `creator_name`
- `creator_email`
- `observation_type`
- `processing_level`
- `featureType`
- `altitude`
- `upstream_area`
- `date_created`
- `date_modified`
- timezone-related fields
- contributor fields not mapped to `institution`

## New Release NetCDF Representation

The release NetCDF products now store upstream global attributes directly in
station-level variables. This avoids tracing back to upstream files while also
avoiding invalid product-level global attributes for multi-station products.

New full-payload variables:

- `sed_reference_master.nc`
  - `station_global_attrs_json`
  - `station_global_attr_names`
  - `station_global_attr_count`
  - `source_station_global_attrs_json`
  - `source_station_global_attr_names`
  - `source_station_global_attr_count`
- `sed_reference_timeseries_daily.nc`, `sed_reference_timeseries_monthly.nc`, `sed_reference_timeseries_annual.nc`
  - `station_global_attrs_json`
  - `station_global_attr_names`
  - `station_global_attr_count`
- `sed_reference_climatology.nc`
  - `station_global_attrs_json`
  - `station_global_attr_names`
  - `station_global_attr_count`
- `sed_reference_satellite.nc`
  - `satellite_station_global_attrs_json`
  - `satellite_station_global_attr_names`
  - `satellite_station_global_attr_count`

Common attributes are also promoted to plain station-level string variables,
including:

- `country`
- `continent_region`
- `geographic_coverage`
- `iso_a3`
- `station_id`
- `dataset_name`
- `data_source_name`
- `observation_type`
- `temporal_resolution`
- `time_coverage_start`
- `time_coverage_end`
- `creator_name`
- `creator_email`
- `creator_institution`
- `source_data_link`
- `processing_level`
- `featureType`
- `date_created`
- `date_modified`

For source-station-level variables in `sed_reference_master.nc`, the promoted
field names are prefixed with `source_station_` where needed to avoid dimension
conflicts.

## Query Policy

After S6/S8 regeneration, users can query each station's upstream global
attributes directly from the release NetCDF file:

- Use promoted variables for common fields such as `country` and
  `continent_region`.
- Use `*_global_attrs_json` for the complete upstream attribute set.
- `source_station_paths` remains provenance metadata, but it is no longer
  required for metadata queries.

## Validation Needed After Regeneration

After rerunning S6/S8, validate that:

1. Each release NetCDF contains its expected `*_global_attrs_json`,
   `*_global_attr_names`, and `*_global_attr_count` variables.
2. JSON payloads parse successfully.
3. For sampled stations, every upstream `ncattrs()` key appears in the release
   JSON payload.
4. Promoted fields such as `country`, `continent_region`, `geographic_coverage`,
   and `iso_a3` match the release CSV catalogs where applicable.

