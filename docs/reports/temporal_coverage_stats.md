# Release Temporal Coverage Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/temporal/tables`
- Product groups: in-situ matrix products, climatology product, and satellite validation product.

## Headline

- Matrix records with any Q/SSC/SSL value: 2,997,121
- Matrix resolutions: daily, monthly, annual
- Sparse time axes detected: annual, daily, monthly

## Product Summary

| product | resolution | station rows | cluster count | record count catalog | record count nc | time start | time end |
|---|---|---|---|---|---|---|---|
| matrix | daily | 7,087 | 7,087 | 2,993,390 | 2,993,390 | 1948-05-25 | 2025-10-21 |
| matrix | monthly | 17 | 17 | 3,263 | 3,263 | 1938-01-15 | 2000-10-15 |
| matrix | annual | 31 | 31 | 468 | 468 | 1912-01-01 | 2020-01-01 |
| climatology | climatology | 1,361 | 0 | 1,361 | 1,361 | 1912-07-01 | 2010-07-01 |
| satellite | all | 38,550 | 38,550 | 16,478,276 | 16,478,276 | 1984-01-15 | 2020-12-15 |

## Matrix Coverage by Resolution

| resolution | first date | last date | time steps | active units | active clusters | record count any | record count Q | record count SSC | record count SSL | median record length steps | max record length steps |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | 1948-05-25 | 2025-10-21 | 25,775 | 7,087 | 7,087 | 2,993,390 | 2,781,406 | 2,982,992 | 2,791,804 | 37 | 21,909 |
| monthly | 1938-01-15 | 2000-10-15 | 690 | 17 | 17 | 3,263 | 3,204 | 3,193 | 3,263 | 142 | 528 |
| annual | 1912-01-01 | 2020-01-01 | 114 | 31 | 31 | 468 | 348 | 468 | 348 | 5 | 109 |

## Variable Coverage by Resolution

| resolution | variable | active units | record count | first year | last year |
|---|---|---|---|---|---|
| daily | SSC | 7,087 | 2,982,992 | 1,948 | 2,025 |
| daily | SSL | 3,427 | 2,791,804 | 1,948 | 2,025 |
| daily | Q | 3,427 | 2,781,406 | 1,948 | 2,025 |
| monthly | SSL | 17 | 3,263 | 1,938 | 2,000 |
| monthly | Q | 17 | 3,204 | 1,938 | 2,000 |
| monthly | SSC | 17 | 3,193 | 1,938 | 2,000 |
| annual | SSC | 31 | 468 | 1,912 | 2,020 |
| annual | Q | 7 | 348 | 1,912 | 2,020 |
| annual | SSL | 7 | 348 | 1,912 | 2,020 |

## Time-Axis Diagnostics

Sparse axes mean the release matrix stores observation dates, not a dense regular calendar grid.

| resolution | file name | n time | time start | time end | unique years | unique year months | expected regular periods | duplicate periods | axis interpretation |
|---|---|---|---|---|---|---|---|---|---|
| daily | sed_reference_timeseries_daily.nc | 25,775 | 1948-05-25 | 2025-10-21 | 77 | 857 | 28,274 | 0 | sparse_observation_date_axis |
| monthly | sed_reference_timeseries_monthly.nc | 690 | 1938-01-15 | 2000-10-15 | 62 | 690 | 754 | 0 | sparse_observation_date_axis |
| annual | sed_reference_timeseries_annual.nc | 114 | 1912-01-01 | 2020-01-01 | 109 | 114 | 109 | 5 | sparse_observation_date_axis |

## Record-Length Distribution

| resolution | record length bin | unit count |
|---|---|---|
| annual | 0 | 0 |
| annual | 1-10 | 26 |
| annual | 11-30 | 2 |
| annual | 31-100 | 2 |
| annual | 101-365 | 1 |
| annual | 366-3650 | 0 |
| annual | >3650 | 0 |
| daily | 0 | 0 |
| daily | 1-10 | 1,332 |
| daily | 11-30 | 1,846 |
| daily | 31-100 | 2,497 |
| daily | 101-365 | 432 |
| daily | 366-3650 | 737 |
| daily | >3650 | 243 |
| monthly | 0 | 0 |
| monthly | 1-10 | 0 |

_Showing first 16 of 21 rows._

## Long Record Summary

| resolution | n gt 10 years | n gt 20 years | n gt 30 years | n gt 50 years | n gt 100 years |
|---|---|---|---|---|---|
| daily | 5,755 | 4,658 | 3,909 | 1,949 | 1,412 |
| monthly | 17 | 17 | 16 | 16 | 16 |
| annual | 5 | 4 | 3 | 3 | 1 |

## Top Source Temporal Coverage

| source name | active units | first year | last year |
|---|---|---|---|
| USGS | 889 | 1,956 | 2,024 |
| HYDAT | 543 | 1,948 | 2,019 |
| Bayern | 37 | 1,965 | 2,025 |
| GFQA_v2 | 5,499 | 1,965 | 2,023 |
| Mekong_Delta | 4 | 2,005 | 2,017 |
| HYBAM | 12 | 1,994 | 2,024 |
| Robotham | 3 | 2,016 | 2,021 |
| Eurasian_River | 17 | 1,938 | 2,000 |
| Fukushima | 2 | 2,012 | 2,018 |
| GloRiSe | 77 | 1,979 | 2,015 |
| NERC | 4 | 2,013 | 2,014 |
| Chao_Phraya_River | 7 | 1,912 | 2,020 |
| Rhine | 12 | 1,990 | 2,011 |
| Shashi_Jianli | 2 | 2,016 | 2,023 |
| Huanghe | 24 | 2,015 | 2,019 |
| Yajiang | 23 | 2,019 | 2,020 |
| Myanmar | 5 | 2,017 | 2,019 |

## Region by Resolution

| continent region | resolution |
|---|---|
| North America | daily |
| North America | daily |
| Europe, Central Europe | daily |
| North America | daily |
| Europe | daily |
|  | daily |
| Asia, Southeast Asia | daily |
| Europe | daily |
| North America | daily |
| North America | daily |
| Asia, South Asia | daily |
| South America | daily |
| Europe | daily |
| Europe | daily |
| Europe, Western Europe | daily |
| Europe | daily |
| Europe | daily |
| Europe, Eastern Europe | monthly |

_Showing first 18 of 50 rows._

## Climatology Temporal Summary

Climatology is reported as a standalone product rather than a basin-cluster matrix.

_No rows._

## Climatology by Source

_No rows._

## Satellite Temporal Summary

Satellite temporal coverage is validation-only and should be filtered by usable variables before analysis.

| resolution | unit type | first date | last date | first year | last year | active units | record count any | product |
|---|---|---|---|---|---|---|---|---|
| satellite_validation | satellite_station_uid | 1984-01-15 | 2020-12-15 | 1,984 | 2,020 | 38,550 | 16,478,276 | satellite_validation |

## Satellite by Source

| source name | first year | last year | active units | record count any |
|---|---|---|---|---|
| RiverSed | 1,984 | 2,019 | 32,941 | 14,199,854 |
| GSED | 1,985 | 2,020 | 5,237 | 2,144,599 |
| Dethier | 1,984 | 2,020 | 372 | 133,823 |

## Satellite by Year

| resolution | year | active units | record count any |
|---|---|---|---|
| daily | 1,984 | 29,760 | 13,773,965 |
| monthly | 1,984 | 371 | 133,613 |
| monthly | 1,985 | 3,419 | 1,459,807 |
| daily | 1,985 | 1,531 | 317,392 |
| daily | 1,986 | 664 | 68,128 |
| monthly | 1,986 | 844 | 348,235 |
| daily | 1,987 | 460 | 19,201 |
| monthly | 1,987 | 265 | 105,042 |
| monthly | 1,988 | 174 | 68,117 |
| daily | 1,988 | 135 | 7,578 |
| daily | 1,989 | 90 | 5,044 |
| monthly | 1,989 | 139 | 52,290 |
| monthly | 1,990 | 22 | 7,727 |
| daily | 1,990 | 30 | 1,479 |
| daily | 1,991 | 43 | 789 |
| monthly | 1,991 | 45 | 15,671 |
| monthly | 1,992 | 23 | 7,808 |
| daily | 1,992 | 25 | 1,867 |

_Showing first 18 of 48 rows._

## Interpretation Notes

- `record_count_any` counts rows where at least one sediment-reference variable is available.
- Long calendar span should be interpreted with record density; sparse series may span many years with few observations.
- The monthly and annual matrix time dimensions are not necessarily regular period indexes.

## Figures

- `fig_active_clusters_by_year.png`: `output_other/stats_release/temporal/figures/fig_active_clusters_by_year.png`
- `fig_active_units_by_year.png`: `output_other/stats_release/temporal/figures/fig_active_units_by_year.png`
- `fig_climatology_record_length_distribution.png`: `output_other/stats_release/temporal/figures/fig_climatology_record_length_distribution.png`
- `fig_climatology_source_contribution.png`: `output_other/stats_release/temporal/figures/fig_climatology_source_contribution.png`
- `fig_climatology_variable_coverage.png`: `output_other/stats_release/temporal/figures/fig_climatology_variable_coverage.png`
- `fig_long_record_counts.png`: `output_other/stats_release/temporal/figures/fig_long_record_counts.png`
- `fig_record_length_distribution.png`: `output_other/stats_release/temporal/figures/fig_record_length_distribution.png`
- `fig_record_length_histogram.png`: `output_other/stats_release/temporal/figures/fig_record_length_histogram.png`
- `fig_records_by_year_variable.png`: `output_other/stats_release/temporal/figures/fig_records_by_year_variable.png`
- `fig_satellite_active_units_by_year.png`: `output_other/stats_release/temporal/figures/fig_satellite_active_units_by_year.png`
- `fig_satellite_record_length_distribution.png`: `output_other/stats_release/temporal/figures/fig_satellite_record_length_distribution.png`
- `fig_satellite_records_by_year_variable.png`: `output_other/stats_release/temporal/figures/fig_satellite_records_by_year_variable.png`
- `fig_satellite_source_contribution.png`: `output_other/stats_release/temporal/figures/fig_satellite_source_contribution.png`
- `fig_satellite_temporal_heatmap.png`: `output_other/stats_release/temporal/figures/fig_satellite_temporal_heatmap.png`
- `fig_source_temporal_span.png`: `output_other/stats_release/temporal/figures/fig_source_temporal_span.png`
- `fig_temporal_coverage.png`: `output_other/stats_release/temporal/figures/fig_temporal_coverage.png`
- Additional figures: 1
