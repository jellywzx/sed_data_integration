# Release Temporal Coverage Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/temporal/tables`
- Product groups: in-situ matrix products, climatology product, and satellite validation product.

## Headline

- Matrix records with any Q/SSC/SSL value: 2,847,547
- Matrix resolutions: daily, monthly, annual
- Sparse time axes detected: annual, daily, monthly

## Product Summary

| product | resolution | station rows | cluster count | record count catalog | record count nc | time start | time end |
|---|---|---|---|---|---|---|---|
| matrix | daily | 1,607 | 1,607 | 2,724,382 | 2,724,382 | 1948-05-25 | 2025-10-21 |
| matrix | monthly | 1,875 | 1,875 | 122,546 | 122,546 | 1938-01-15 | 2021-12-24 |
| matrix | annual | 58 | 58 | 619 | 619 | 1912-01-01 | 2021-09-28 |
| climatology | climatology | 1,322 | 0 | 1,322 | 1,322 | 1912-07-01 | 2010-07-01 |
| satellite | all | 47,785 | 47,785 | 16,506,461 | 16,506,461 | 1984-01-15 | 2020-12-15 |

## Matrix Coverage by Resolution

| resolution | first date | last date | time steps | active units | active clusters | record count any | record count Q | record count SSC | record count SSL | median record length steps | max record length steps |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | 1948-05-25 | 2025-10-21 | 25,775 | 1,607 | 1,607 | 2,724,382 | 2,707,488 | 2,713,984 | 2,717,886 | 638 | 21,909 |
| monthly | 1938-01-15 | 2021-12-24 | 11,533 | 1,875 | 1,875 | 122,546 | 122,503 | 122,381 | 119,410 | 36 | 9,557 |
| annual | 1912-01-01 | 2021-09-28 | 239 | 58 | 58 | 619 | 499 | 619 | 499 | 5 | 109 |

## Variable Coverage by Resolution

| resolution | variable | active units | record count | first year | last year |
|---|---|---|---|---|---|
| daily | SSL | 1,571 | 2,717,886 | 1,948 | 2,025 |
| daily | SSC | 1,607 | 2,713,984 | 1,948 | 2,025 |
| daily | Q | 1,571 | 2,707,488 | 1,948 | 2,025 |
| monthly | Q | 1,875 | 122,503 | 1,938 | 2,021 |
| monthly | SSC | 1,875 | 122,381 | 1,938 | 2,021 |
| monthly | SSL | 1,875 | 119,410 | 1,938 | 2,021 |
| annual | SSC | 58 | 619 | 1,912 | 2,021 |
| annual | Q | 34 | 499 | 1,912 | 2,021 |
| annual | SSL | 34 | 499 | 1,912 | 2,021 |

## Time-Axis Diagnostics

Sparse axes mean the release matrix stores observation dates, not a dense regular calendar grid.

| resolution | file name | n time | time start | time end | unique years | unique year months | expected regular periods | duplicate periods | axis interpretation |
|---|---|---|---|---|---|---|---|---|---|
| daily | sed_reference_timeseries_daily.nc | 25,775 | 1948-05-25 | 2025-10-21 | 77 | 857 | 28,274 | 0 | sparse_observation_date_axis |
| monthly | sed_reference_timeseries_monthly.nc | 11,533 | 1938-01-15 | 2021-12-24 | 84 | 992 | 1,008 | 10,541 | sparse_observation_date_axis |
| annual | sed_reference_timeseries_annual.nc | 239 | 1912-01-01 | 2021-09-28 | 110 | 164 | 110 | 129 | sparse_observation_date_axis |

## Record-Length Distribution

| resolution | record length bin | unit count |
|---|---|---|
| annual | 0 | 0 |
| annual | 1-10 | 50 |
| annual | 11-30 | 5 |
| annual | 31-100 | 2 |
| annual | 101-365 | 1 |
| annual | 366-3650 | 0 |
| annual | >3650 | 0 |
| daily | 0 | 0 |
| daily | 1-10 | 285 |
| daily | 11-30 | 56 |
| daily | 31-100 | 101 |
| daily | 101-365 | 218 |
| daily | 366-3650 | 713 |
| daily | >3650 | 234 |
| monthly | 0 | 0 |
| monthly | 1-10 | 96 |

_Showing first 16 of 21 rows._

## Long Record Summary

| resolution | n gt 10 years | n gt 20 years | n gt 30 years | n gt 50 years | n gt 100 years |
|---|---|---|---|---|---|
| daily | 1,322 | 1,283 | 1,266 | 1,238 | 1,165 |
| monthly | 1,779 | 1,530 | 1,217 | 354 | 83 |
| annual | 8 | 4 | 3 | 3 | 1 |

## Top Source Temporal Coverage

| source name | active units | first year | last year |
|---|---|---|---|
| USGS | 873 | 1,972 | 2,024 |
| HYDAT | 481 | 1,948 | 2,012 |
| Bayern | 32 | 1,965 | 2,025 |
| GFQA_v2 | 1,692 | 1,940 | 2,021 |
| EUSEDcollab | 216 | 1,987 | 2,021 |
| HYBAM | 12 | 1,994 | 2,024 |
| Mekong_Delta | 4 | 2,005 | 2,012 |
| Robotham | 3 | 2,016 | 2,021 |
| Eurasian_River | 17 | 1,938 | 2,020 |
| Fukushima | 2 | 2,012 | 2,018 |
| NERC | 4 | 2,013 | 2,014 |
| Chao_Phraya_River | 7 | 1,912 | 2,020 |
| Rhine | 12 | 1,990 | 2,011 |
| GloRiSe | 128 | 1,979 | 2,012 |
| Shashi_Jianli | 2 | 2,016 | 2,023 |
| Huanghe | 24 | 2,015 | 2,019 |
| Yajiang | 23 | 2,019 | 2,020 |
| Myanmar | 6 | 2,017 | 2,019 |

## Region by Resolution

| continent region | resolution |
|---|---|
| North America | daily |
| North America | daily |
| Europe, Central Europe | daily |
| North America | monthly |
| Europe | monthly |
| Europe | monthly |
| Europe | monthly |
| Asia, Southeast Asia | daily |
| Europe | monthly |
| South America | daily |
| Europe, Western Europe | daily |
| South America | daily |
| Asia, East Asia | daily |
| Europe, Eastern Europe | monthly |
| North America\|Asia, Southeast Asia | daily |
| Europe | monthly |
| South America | daily |
| Europe | monthly |

_Showing first 18 of 39 rows._

## Climatology Temporal Summary

Climatology is reported as a standalone product rather than a basin-cluster matrix.

_No rows._

## Climatology by Source

_No rows._

## Satellite Temporal Summary

Satellite temporal coverage is validation-only and should be filtered by usable variables before analysis.

| resolution | unit type | first date | last date | first year | last year | active units | record count any | product |
|---|---|---|---|---|---|---|---|---|
| satellite_validation | satellite_station_uid | 1984-01-15 | 2020-12-15 | 1,984 | 2,020 | 47,785 | 16,506,461 | satellite_validation |

## Satellite by Source

| source name | first year | last year | active units | record count any |
|---|---|---|---|---|
| RiverSed | 1,984 | 2,019 | 42,177 | 14,228,483 |
| GSED | 1,985 | 2,020 | 5,237 | 2,144,599 |
| Dethier | 1,984 | 2,020 | 371 | 133,379 |

## Satellite by Year

| resolution | year | active units | record count any |
|---|---|---|---|
| daily | 1,984 | 29,807 | 13,774,471 |
| monthly | 1,984 | 370 | 133,169 |
| monthly | 1,985 | 3,419 | 1,459,807 |
| daily | 1,985 | 1,601 | 318,250 |
| daily | 1,986 | 757 | 69,012 |
| monthly | 1,986 | 844 | 348,235 |
| monthly | 1,987 | 265 | 105,042 |
| daily | 1,987 | 558 | 20,171 |
| daily | 1,988 | 220 | 8,410 |
| monthly | 1,988 | 174 | 68,117 |
| monthly | 1,989 | 139 | 52,290 |
| daily | 1,989 | 154 | 5,354 |
| daily | 1,990 | 106 | 2,017 |
| monthly | 1,990 | 22 | 7,727 |
| monthly | 1,991 | 45 | 15,671 |
| daily | 1,991 | 150 | 1,502 |
| daily | 1,992 | 80 | 2,145 |
| monthly | 1,992 | 23 | 7,808 |

_Showing first 18 of 54 rows._

## Interpretation Notes

- `record_count_any` counts rows where at least one sediment-reference variable is available.
- Long calendar span should be interpreted with record density; sparse series may span many years with few observations.
- The monthly and annual matrix time dimensions are not necessarily regular period indexes.

## Figures

- `fig_active_clusters_by_year.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_active_clusters_by_year.png`
- `fig_active_units_by_year.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_active_units_by_year.png`
- `fig_climatology_record_length_distribution.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_climatology_record_length_distribution.png`
- `fig_climatology_source_contribution.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_climatology_source_contribution.png`
- `fig_climatology_variable_coverage.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_climatology_variable_coverage.png`
- `fig_long_record_counts.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_long_record_counts.png`
- `fig_record_length_distribution.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_record_length_distribution.png`
- `fig_record_length_histogram.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_record_length_histogram.png`
- `fig_records_by_year_variable.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_records_by_year_variable.png`
- `fig_satellite_active_units_by_year.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_satellite_active_units_by_year.png`
- `fig_satellite_record_length_distribution.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_satellite_record_length_distribution.png`
- `fig_satellite_records_by_year_variable.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_satellite_records_by_year_variable.png`
- `fig_satellite_source_contribution.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_satellite_source_contribution.png`
- `fig_satellite_temporal_heatmap.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_satellite_temporal_heatmap.png`
- `fig_source_temporal_span.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_source_temporal_span.png`
- `fig_temporal_coverage.png`: `$WORKSPACE/output_other/stats_release_full/temporal/figures/fig_temporal_coverage.png`
- Additional figures: 1
