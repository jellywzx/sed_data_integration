# Temporal coverage results for the S8 ESSD release

## Overview

The temporal coverage statistics are reported for three product groups: the basin-cluster time-series matrices (daily, monthly, and annual), the standalone climatology stations, and the satellite-validation product. These groups use different statistical units and should therefore be described separately.

## Main Time-Series Products

| Product | Unit | First year | Last year | Units | Records | Median length (yr) | Max length (yr) | >50 yr | >100 yr |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| daily | cluster | 1948 | 2025 | 1,607 | 2,724,382 | 2.9 | 60.0 | 15 | 0 |
| monthly | cluster | 1938 | 2021 | 1,875 | 122,546 | 7.5 | 72.8 | 5 | 0 |
| annual | cluster | 1912 | 2021 | 58 | 619 | 5.0 | 109.0 | 3 | 3 |

### Product-Level Coverage Detail

| Resolution | Product | Unit | Span | Units | Clusters | Records | Median yr | Max yr | >50 yr | >100 yr |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| daily |  | cluster | 1948-2025 | 1,607 | 0 | 2,724,382 | 2.9 | 60.0 | 15 | 0 |
| monthly |  | cluster | 1938-2021 | 1,875 | 0 | 122,546 | 7.5 | 72.8 | 5 | 0 |
| annual |  | cluster | 1912-2021 | 58 | 0 | 619 | 5.0 | 109.0 | 3 | 3 |
| climatology |  | climatology_station | 1884-2017 | 1,322 | 0 | 1,322 | 0.0 | 96.0 | 84 | 0 |
| satellite_daily | satellite_validation | satellite_station | 1984-2018 | 9,236 | 42,177 | 28,629 | 35.1 | 35.4 | 0 | 0 |
| satellite_monthly | satellite_validation | satellite_station | 1984-2020 | 5,608 | 5,608 | 1,317,180 | 35.4 | 37.0 | 0 | 0 |

### Variable Coverage

| Resolution | Variable | Span | Active units | Records | Peak active units | Peak records |
| --- | --- | --- | --- | --- | --- | --- |
| daily | Q | 1948-2025 | 1,571 | 2,707,488 | 553 in 1980 | 121,573 in 1980 |
| daily | SSC | 1948-2025 | 1,607 | 2,713,984 | 556 in 1980 | 121,639 in 1980 |
| daily | SSL | 1948-2025 | 1,571 | 2,717,886 | 553 in 1980 | 121,573 in 1980 |
| monthly | Q | 1938-2021 | 1,875 | 122,503 | 1,497 in 2013 | 12,723 in 2013 |
| monthly | SSC | 1938-2021 | 1,875 | 122,381 | 1,497 in 2013 | 12,708 in 2013 |
| monthly | SSL | 1938-2021 | 1,875 | 119,410 | 1,497 in 2013 | 12,491 in 2013 |
| annual | Q | 1912-2021 | 34 | 499 | 23 in 2018 | 39 in 2018 |
| annual | SSC | 1912-2021 | 58 | 619 | 47 in 2018 | 63 in 2018 |
| annual | SSL | 1912-2021 | 34 | 499 | 23 in 2018 | 39 in 2018 |
| climatology | Q | 1966-1995 | 782 | 782 | 738 in 1995 | 738 in 1995 |
| climatology | SSC | 1966-1995 | 804 | 804 | 736 in 1995 | 736 in 1995 |
| climatology | SSL | 1912-2010 | 1,298 | 1,298 | 741 in 1995 | 741 in 1995 |

### Yearly Peaks

| Resolution | Years | Peak active units | Peak records | Total records |
| --- | --- | --- | --- | --- |
| daily | 1948-2025 | 556 in 1980 | 121,639 in 1980 | 2,724,382 |
| monthly | 1938-2021 | 1,497 in 2013 | 12,723 in 2013 | 122,546 |
| annual | 1912-2021 | 47 in 2018 | 63 in 2018 | 619 |
| climatology | 1912-2010 | 741 in 1995 | 741 in 1995 | 1,322 |
| satellite_daily | 1984-2019 | 1,431 in 2006 | 1,968 in 2006 | 28,629 |
| satellite_monthly | 1984-2020 | 5,605 in 2001 | 43,568 in 2009 | 1,317,180 |

### Long-Record Diagnostics

| Resolution | Units | Median yr | Max yr | >10 yr | >30 yr | >50 yr | >100 yr |
| --- | --- | --- | --- | --- | --- | --- | --- |
| daily | 1,607 | 2.9 | 60.0 | 363 (22.6%) | 59 (3.7%) | 15 (0.9%) | 0 (0.0%) |
| monthly | 1,875 | 7.5 | 72.8 | 55 (2.9%) | 9 (0.5%) | 5 (0.3%) | 0 (0.0%) |
| annual | 58 | 5.0 | 109.0 | 6 (10.3%) | 5 (8.6%) | 3 (5.2%) | 3 (5.2%) |
| climatology | 1,322 | 0.0 | 96.0 | 302 (22.8%) | 144 (10.9%) | 84 (6.4%) | 0 (0.0%) |
| satellite_daily | 42,177 | 35.1 | 35.4 | 34,096 (80.8%) | 31,916 (75.7%) | 0 (0.0%) | 0 (0.0%) |
| satellite_monthly | 5,608 | 35.4 | 37.0 | 5,608 (100.0%) | 5,209 (92.9%) | 0 (0.0%) | 0 (0.0%) |

Daily coverage is the strongest long-record component of the release, with 15 clusters longer than 50 years and 0 clusters longer than 100 years. Monthly coverage has many clusters but shorter median spans, while annual coverage contains fewer clusters but includes several very long records.

Peak active coverage occurs at 556 active units in 1980 for daily, 1,497 active units in 2013 for monthly, and 47 active units in 2018 for annual products.

## Source and Regional Temporal Coverage

### Top Source-Resolution Contributions

| Source | Resolution | Span | Stations | Clusters | Records | Median yr | Max yr |
| --- | --- | --- | --- | --- | --- | --- | --- |
| USGS | daily | 1980-2024 | 885 | 873 | 1,651,590 | 2.8 | 44.8 |
| HYDAT | daily | 1948-1997 | 501 | 480 | 661,138 | 5.3 | 40.5 |
| Bayern | daily | 1965-2025 | 34 | 32 | 380,719 | 26.0 | 60.0 |
| EUSEDcollab | monthly | 1987-2021 | 226 | 216 | 63,208 | 6.0 | 26.2 |
| GFQA_v2 | monthly | 1995-2021 | 1,993 | 1,631 | 56,094 | 6.8 | 9.0 |
| HYBAM | daily | 1994-2024 | 12 | 12 | 11,826 | 23.5 | 29.6 |
| Mekong_Delta | daily | 2005-2012 | 4 | 4 | 11,323 | 8.0 | 8.0 |
| Robotham | daily | 2016-2021 | 3 | 3 | 3,432 | 4.0 | 4.1 |
| Eurasian_River | monthly | 1938-2000 | 17 | 17 | 3,204 | 26.4 | 59.8 |
| Fukushima | daily | 2012-2018 | 2 | 2 | 3,069 | 5.0 | 6.0 |
| NERC | daily | 2013-2014 | 4 | 4 | 624 | 1.0 | 1.0 |
| Chao_Phraya_River | annual | 1912-2020 | 7 | 7 | 348 | 40.0 | 109.0 |
| Rhine | daily | 1990-2011 | 12 | 12 | 312 | 19.2 | 21.7 |
| Shashi_Jianli | daily | 2016-2023 | 2 | 2 | 154 | 7.9 | 7.9 |
| GFQA_v2 | annual | 2012-2021 | 27 | 27 | 151 | 4.0 | 9.0 |

### Top Region-Resolution Contributions

| Region | Country | Resolution | Span | Clusters | Records | Median yr | Max yr |
| --- | --- | --- | --- | --- | --- | --- | --- |
| North America | United States | daily | 1980-2024 | 871 | 1,650,788 | 2.9 | 44.8 |
| North America | Canada | daily | 1948-1997 | 478 | 661,090 | 5.5 | 40.5 |
| Europe, Central Europe | Germany | daily | 1965-2025 | 32 | 380,719 | 39.9 | 60.0 |
| North America | Mexico | monthly | 2012-2021 | 1,627 | 56,001 | 7.7 | 9.1 |
| Europe | Spain | monthly | 1999-2021 | 7 | 21,402 | 10.0 | 15.8 |
| Europe | Denmark | monthly | 1989-2009 | 188 | 14,193 | 6.0 | 21.0 |
| Europe | Poland | monthly | 1987-2019 | 2 | 12,114 | 24.6 | 26.2 |
| Asia, Southeast Asia | Vietnam | daily | 2005-2012 | 3 | 8,766 | 8.0 | 8.0 |
| Europe | Spain\|France | monthly | 1993-2018 | 1 | 5,545 | 24.7 | 24.7 |
| South America | Brazil | daily | 1994-2024 | 7 | 5,169 | 27.1 | 29.6 |
| Europe, Western Europe | United Kingdom | daily | 2013-2021 | 7 | 4,056 | 1.0 | 4.1 |
| South America | Peru | daily | 2003-2021 | 2 | 3,157 | 13.8 | 17.9 |
| Asia, East Asia | Japan | daily | 2012-2018 | 2 | 3,069 | 5.0 | 6.0 |
| Europe, Eastern Europe | Russia | monthly | 1938-2000 | 14 | 2,574 | 25.9 | 59.8 |
| North America\|Asia, Southeast Asia | Canada\|Vietnam | daily | 2005-2012 | 1 | 2,557 | 8.0 | 8.0 |

These source and region tables separate record volume from span length. A source can dominate total records through dense daily sampling even when its spatial footprint is narrower than a source with many short station records.

## Climatology Product

The climatology product contains 1,322 standalone stations spanning 1884-2017. It is not a basin-cluster time-series matrix, so it is summarized separately from the daily/monthly/annual products.

Variable coverage in the climatology product includes 782 Q stations, 804 SSC stations, and 1,298 SSL stations across 5 sources.

## Satellite Validation Product

The satellite-validation product is summarized with satellite-prefixed resolution labels to distinguish it from the main matrices. It contains 14,844 satellite station-resolution rows linked to approximately 47,785 basin clusters, with 1,345,809 records spanning 1984-2020. Resolution-specific rows are: satellite_daily (9,236), satellite_monthly (5,608).

The satellite summary reports 1 sources/source families and uses station-level catalog spans; when NetCDF scanning is enabled, Q/SSC/SSL record counts and annual active units are computed directly from `sed_reference_satellite.nc`.

### Climatology Source Detail

| Source | Resolution | Span | Stations | Clusters | Records | Median yr | Max yr |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Milliman |  | 1995-1995 | 737 |  | 737 | 0.0 | 0.0 |
| Vanmaercke |  | 1884-2011 | 516 |  | 516 | 9.0 | 96.0 |
| HMA |  | 1956-2017 | 28 |  | 28 | 54.0 | 61.0 |
| Huanghe |  | 1950-2015 | 24 |  | 24 | 64.0 | 66.0 |
| ALi_De_Boer |  | 1960-1998 | 17 |  | 17 | 26.0 | 36.0 |

### Satellite Source Detail

| Source | Resolution | Span | Stations | Clusters | Records | Median yr | Max yr |
| --- | --- | --- | --- | --- | --- | --- | --- |
| RiverSed | daily | 1984-2019 | 42,177 | 42,177 | 14,228,483 | 35.1 | 35.4 |
| GSED | monthly | 1985-2020 | 5,237 | 5,237 | 2,144,599 | 35.4 | 36.0 |
| Dethier | monthly | 1984-2020 | 371 | 371 | 133,379 | 37.0 | 37.0 |

## Interpretation Notes

- Daily, monthly, annual, climatology, and satellite products use different units; compare trends within product groups before comparing across product groups.
- `active_units` measures whether a unit has at least one valid record in a year, while `records` measures sampling density.
- Long-record counts are useful evidence for model evaluation, but sparse annual records should be interpreted by record count and calendar span together.

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
