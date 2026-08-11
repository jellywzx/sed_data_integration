# S8 spatial coverage statistics for ESSD

## Manuscript-ready summary

The S8 release contains 7,379 final main-product stations. Resolution-specific coverage is 4,717 daily stations, 2,697 monthly stations, and 49 annual stations. Basin assignment resolved 5,640 stations (76.43%), while 1,739 stations (23.57%) remain unresolved and 0 stations (0%) have unknown or other basin status. The published basin sidecar contains polygons for 5,724 stations (77.57%).

The main-product coordinates span -34.86 to 81.94 degrees latitude and -159.47 to 158.72 degrees longitude. Valid upstream basin areas are available for 5,640 stations; the median area is 1,028.78 km2, with an interquartile range of 216.44-6,322.59 km2 and a maximum of 2,959,787.75 km2.

Main source contributions by station count: GFQA_v2: 5,499 stations, 186,867 records; USGS: 889 stations, 1,685,357 records; HYDAT: 540 stations, 671,979 records; EUSEDcollab: 244 stations, 66,637 records; GloRiSe: 77 stations, 103 records

The satellite-validation product contains 38,550 station-resolution rows linked to 181 stations.

## Key Metrics

- Final stations: 7,379
- Station catalog rows: 7,463
- Main-product record count: 3,064,671
- Basin-resolved stations: 5,640 (76.43%)
- Published basin polygons: 5,724 (77.57%)
- Unknown country stations: 75

## Resolution Coverage

| Resolution | Station rows | reference_station_count | Records | Record share | Countries |
|---|---|---|---|---|---|
| annual | 49 | 49 | 535 | 0.02% | 6 |
| daily | 4,717 | 4,717 | 2,963,235 | 96.69% | 37 |
| monthly | 2,697 | 2,697 | 100,901 | 3.29% | 29 |

Resolution-specific records are uneven, so spatial coverage should be interpreted together with temporal record volume.

## Upstream Basin Area

| Area bin | reference_station_count | Share of valid-area stations |
|---|---|---|
| <10 km2 | 0 | 0% |
| 10-100 km2 | 764 | 13.55% |
| 100-1,000 km2 | 2,031 | 36.01% |
| 1,000-10,000 km2 | 1,728 | 30.64% |
| 10,000-100,000 km2 | 861 | 15.27% |
| >100,000 km2 | 256 | 4.54% |

## Geographic Hotspots

### Regions by Station Count

| continent region | reference stations | record count | country count |
|---|---|---|---|
| North America | 5,213 | 2,465,226 | 4 |
| Europe | 1,325 | 552,987 | 27 |
| Unknown | 397 | 13,496 | 3 |
| Asia, South Asia | 208 | 5,383 | 1 |
| South America | 168 | 11,723 | 6 |
| Asia | 67 | 15,641 | 5 |
| Africa | 1 | 215 | 1 |

### Countries by Station Count

| country | iso a3 | continent region | reference stations | record count |
|---|---|---|---|---|
| Mexico | MEX | North America | 3,630 | 92,561 |
| United States | USA | North America | 896 | 1,684,297 |
| Canada | CAN | North America | 685 | 681,792 |
| Italy | ITA | Europe | 406 | 15,640 |
| Netherlands (the) |  | Unknown | 303 | 12,645 |
| Denmark | DNK | Europe | 211 | 15,561 |
| India | IND | Asia, South Asia | 208 | 5,383 |
| Uruguay | URY | South America | 156 | 2,523 |
| France | FRA | Europe | 108 | 9,313 |
| Unknown |  | Unknown | 75 | 97 |
| Romania | ROU | Europe | 74 | 2,672 |
| Norway | NOR | Europe | 68 | 4,634 |
| Sweden | SWE | Europe | 64 | 2,514 |
| Belgium | BEL | Europe | 55 | 3,895 |
| Germany | DEU | Europe | 49 | 421,364 |

_Showing first 15 of 47 rows._

### Region-Resolution Record Hotspots

| continent region | resolution | reference stations | record count |
|---|---|---|---|
| North America | daily | 3,828 | 2,417,359 |
| Europe, Central Europe | daily | 37 | 421,052 |
| Europe | daily | 417 | 84,575 |
| North America | monthly | 1,441 | 47,811 |
| Europe | monthly | 851 | 39,983 |
| Asia, Southeast Asia | daily | 9 | 11,927 |
| South America | daily | 59 | 9,633 |
|  | monthly | 235 | 7,222 |
|  | daily | 167 | 6,332 |
| Asia, South Asia | daily | 165 | 4,840 |
| Europe, Western Europe | daily | 7 | 4,056 |
| Europe, Eastern Europe | monthly | 17 | 3,263 |
| Asia, East Asia | daily | 27 | 3,246 |
| South America | monthly | 112 | 2,086 |
| Asia, South Asia | monthly | 41 | 536 |

_Showing first 15 of 21 rows._

## Source Spatial Contribution

| source name | reference stations | source station count | record count | available resolutions |
|---|---|---|---|---|
| GFQA_v2 | 5,499 | 5,812 | 186,867 | annual\|daily\|monthly |
| USGS | 889 | 890 | 1,685,357 | daily |
| HYDAT | 540 | 541 | 671,979 | daily |
| EUSEDcollab | 244 | 244 | 66,637 | daily\|monthly |
| GloRiSe | 77 | 77 | 103 | daily\|monthly |
| Bayern | 37 | 37 | 421,052 | daily |
| Huanghe | 24 | 24 | 120 | annual |
| Yajiang | 23 | 23 | 23 | daily |
| Eurasian_River | 17 | 17 | 3,263 | monthly |
| HYBAM | 12 | 12 | 9,404 | daily |
| Rhine | 12 | 12 | 312 | daily |
| Chao_Phraya_River | 7 | 7 | 348 | annual |

_Showing first 12 of 18 rows._

The station-based and record-based rankings answer different questions: the former describes spatial footprint, while the latter describes record volume.

## Satellite Validation Spatial Coverage

| source | resolution | satellite station count | linked reference stations | record count |
|---|---|---|---|---|
| RiverSed | daily | 32,941 | 161 | 14,199,854 |
| GSED | monthly | 5,237 | 14 | 2,144,599 |
| Dethier | monthly | 372 | 9 | 133,823 |

## Basin Polygon Layers

| file name | layer name | feature count |
|---|---|---|
| sed_reference_cluster_points.gpkg | station_summary | 7,379 |
| sed_reference_source_stations.gpkg | source_daily | 4,875 |
| sed_reference_cluster_points.gpkg | station_daily | 4,717 |
| sed_reference_cluster_basins.gpkg | basin_daily | 3,555 |
| sed_reference_source_stations.gpkg | source_monthly | 2,793 |
| sed_reference_cluster_points.gpkg | station_monthly | 2,697 |
| sed_reference_cluster_basins.gpkg | basin_monthly | 2,127 |
| sed_reference_cluster_points.gpkg | station_annual | 49 |
| sed_reference_source_stations.gpkg | source_annual | 49 |
| sed_reference_cluster_basins.gpkg | basin_annual | 42 |

## Diagnostics and Limitations

- Unknown country/region rows written for review: 397
- Regional summaries depend on release catalog geography; unknown geography should be reviewed before strong continent/country claims.
- Station counts by source are not additive across sources because multiple datasets can contribute to the same merged station.

## Output Tables

- `tables/table_spatial_coverage_summary.csv`
- `tables/table_spatial_coverage_by_resolution.csv`
- `tables/table_spatial_coverage_by_region.csv`
- `tables/table_spatial_coverage_by_country.csv`
- `tables/table_spatial_coverage_by_source.csv`
- `tables/table_spatial_coverage_by_region_source.csv`
- `tables/table_spatial_coverage_by_region_resolution.csv`
- `tables/table_upstream_area_distribution.csv`
- `tables/table_satellite_validation_spatial_coverage.csv`
- `tables/table_unknown_country_region_reference_stations.csv`

## Figure Suggestions

- Main text: `fig_spatial_coverage_by_resolution`, `fig_top_countries_by_reference_stations`, and `fig_upstream_area_distribution`.
- Supplement: source contribution, basin status, and satellite-validation spatial figures.

## Manuscript-Usable Statements

- The release provides broad river-basin coverage, but regional completeness should be interpreted together with unresolved basin and unknown-geography diagnostics.
- Polygon availability provides a direct release-side indicator of basin sidecar coverage.
- Source rankings should separate spatial footprint from record-volume contribution.
