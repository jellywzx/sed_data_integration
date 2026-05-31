# Temporal coverage results for the S8 ESSD release

## Overview

The temporal coverage statistics are reported for three product groups: the basin-cluster time-series matrices (daily, monthly, and annual), the standalone climatology stations, and the satellite-validation product. These groups use different statistical units and should therefore be described separately.

## Main Time-Series Products

| Product | Unit | First year | Last year | Units | Records | Median length (yr) | Max length (yr) | >50 yr | >100 yr |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| daily | cluster | 1901 | 2025 | 1,754 | 14,261,714 | 12.7 | 121.8 | 464 | 90 |
| monthly | cluster | 1938 | 2021 | 1,863 | 145,663 | 7.4 | 59.8 | 4 | 0 |
| annual | cluster | 1912 | 2021 | 58 | 729 | 5.0 | 109.0 | 5 | 3 |

Daily coverage is the strongest long-record component of the release, with 464 clusters longer than 50 years and 90 clusters longer than 100 years. Monthly coverage has many clusters but shorter median spans, while annual coverage contains fewer clusters but includes several very long records.

Peak active coverage occurs at 990 active units in 1980 for daily, 1,487 active units in 2013 for monthly, and 47 active units in 2018 for annual products.

## Climatology Product

The climatology product contains 1,359 standalone stations spanning 1884-2021. It is not a basin-cluster time-series matrix, so it is summarized separately from the daily/monthly/annual products.

Variable coverage in the climatology product includes 819 Q stations, 841 SSC stations, and 1,335 SSL stations across 6 sources.

## Satellite Validation Product

The satellite-validation product is summarized as `satellite_daily` to distinguish it from the main daily matrix. It contains 47,786 satellite stations linked to 27,491 basin clusters, with 16,506,905 records spanning 1984-2020.

The satellite summary reports 3 sources/source families and uses station-level catalog spans; when NetCDF scanning is enabled, Q/SSC/SSL record counts and annual active units are computed directly from `sed_reference_satellite.nc`.

## Recommended ESSD Use

- Main text: use `fig_active_units_by_year`, `fig_record_length_distribution`, and `fig_temporal_coverage_heatmap` for daily/monthly/annual coverage.
- Climatology: use `fig_climatology_variable_coverage` and `fig_climatology_record_length_distribution` in a separate climatology paragraph or supplement.
- Satellite validation: use `fig_satellite_active_units_by_year`, `fig_satellite_records_by_year_variable`, and `fig_satellite_source_contribution` in the validation/supplement section.
- Tables: use `table_temporal_coverage_by_resolution.csv` as the compact master table; use the climatology and satellite dedicated tables for supplementary material.

## Output Files

- `tables/table_climatology_temporal_summary.csv`
- `tables/table_climatology_by_source.csv`
- `tables/table_climatology_record_lengths_by_station.csv`
- `tables/table_satellite_temporal_summary.csv`
- `tables/table_satellite_by_year.csv`
- `tables/table_satellite_by_source.csv`
- `tables/table_satellite_record_lengths_by_station.csv`
- `tables/table_satellite_by_linked_cluster.csv`
