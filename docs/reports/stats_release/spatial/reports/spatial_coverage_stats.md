# Release Spatial Coverage Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/spatial/tables`
- Spatial statistics use release catalogs, GeoPackages, and satellite validation catalogs only.

## Headline

- Reference stations: 7,379
- Release records represented by station catalog:
- Country/region rows needing canonicalization review: 0
- Stations with unknown country or region: 397

## Article-Ready Metrics

| section | metric | value | unit | source file | notes |
|---|---|---|---|---|---|
| main_release | station_catalog_rows | 7,463 | rows | station_catalog.csv |  |
| main_release | final_reference_station_count | 7,379 | reference_stations | station_catalog.csv |  |
| main_release | daily_reference_station_count | 4,717 | reference_stations | station_catalog.csv |  |
| main_release | monthly_reference_station_count | 2,697 | reference_stations | station_catalog.csv |  |
| main_release | annual_reference_station_count | 49 | reference_stations | station_catalog.csv |  |
| basin_assignment | resolved_reference_station_count | 5,640 | reference_stations | station_catalog.csv |  |
| basin_assignment | resolved_station_percent | 76.43 | percent | station_catalog.csv |  |
| basin_assignment | unresolved_reference_station_count | 1,739 | reference_stations | station_catalog.csv |  |
| basin_assignment | unresolved_station_percent | 23.57 | percent | station_catalog.csv |  |
| basin_assignment | unknown_status_reference_station_count | 0 | reference_stations | station_catalog.csv |  |
| basin_assignment | unknown_status_station_percent | 0 | percent | station_catalog.csv |  |
| basin_polygons | basin_polygon_reference_station_count | 5,724 | reference_stations | sed_reference_cluster_basins.gpkg |  |
| basin_polygons | basin_polygon_station_percent | 77.57 | percent | sed_reference_cluster_basins.gpkg |  |
| coordinates | reference_stations_with_valid_lat_lon | 7,379 | reference_stations | station_catalog.csv |  |
| coordinates | latitude_min | -34.86 | degrees_north | station_catalog.csv |  |
| coordinates | latitude_max | 81.94 | degrees_north | station_catalog.csv |  |
| coordinates | longitude_min | -159.47 | degrees_east | station_catalog.csv |  |
| coordinates | longitude_max | 158.72 | degrees_east | station_catalog.csv |  |

_Showing first 18 of 32 rows._

## Coverage by Temporal Resolution

| resolution | source station resolution rows | reference stations | record count | country count |
|---|---|---|---|---|
| daily | 4,717 | 4,717 | 2,963,235 | 37 |
| monthly | 2,697 | 2,697 | 100,901 | 29 |
| annual | 49 | 49 | 535 | 6 |

## Coverage by Region

| continent region | reference stations | record count | country count |
|---|---|---|---|
| North America | 5,213 | 2,465,226 | 4 |
| Europe | 1,325 | 552,987 | 27 |
| Unknown | 397 | 13,496 | 3 |
| Asia, South Asia | 208 | 5,383 | 1 |
| South America | 168 | 11,723 | 6 |
| Asia | 67 | 15,641 | 5 |
| Africa | 1 | 215 | 1 |

## Top Countries

Country statistics prefer canonical country names and ISO3 codes where available.

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

## Top Source Spatial Contributions

| source name | reference stations | record count |
|---|---|---|
| USGS | 889 | 1,685,357 |
| HYDAT | 540 | 671,979 |
| Bayern | 37 | 421,052 |
| GFQA_v2 | 5,499 | 186,867 |
| EUSEDcollab | 244 | 66,637 |
| Mekong_Delta | 4 | 11,921 |
| HYBAM | 12 | 9,404 |
| Robotham | 3 | 3,432 |
| Eurasian_River | 17 | 3,263 |
| Fukushima | 2 | 3,069 |
| NERC | 4 | 624 |
| Chao_Phraya_River | 7 | 348 |
| Rhine | 12 | 312 |
| Shashi_Jianli | 2 | 154 |
| Huanghe | 24 | 120 |

_Showing first 15 of 18 rows._

## Region by Resolution

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
| Asia, Southeast Asia | annual | 7 | 348 |
| Africa | daily | 1 | 215 |
| Asia, East Asia | annual | 24 | 120 |

_Showing first 18 of 21 rows._

## Source Type Footprint

| source type | source group | source count | reference stations | record count |
|---|---|---|---|---|
| in_situ | in_situ | 18 | 7,401 | 3,064,671 |

## Upstream Area Distribution

Area metrics describe release basin polygons or catalog basin attributes, not new basin matching.

| section | label | value km2 | reference stations | fraction of valid area reference stations |
|---|---|---|---|---|
| summary | valid_reference_station_count |  | 5,640 |  |
| summary | missing_or_invalid_reference_station_count |  | 1,739 |  |
| summary | min | 25.19 |  |  |
| summary | p05 | 49.78 |  |  |
| summary | p25 | 216.44 |  |  |
| summary | mean | 25,393 |  |  |
| summary | median | 1,029 |  |  |
| summary | p75 | 6,323 |  |  |
| summary | p95 | 87,249 |  |  |
| summary | max | 2,959,788 |  |  |
| bin | <10 km2 |  | 0 | 0 |
| bin | 10-100 km2 |  | 764 | 0.14 |
| bin | 100-1,000 km2 |  | 2,031 | 0.36 |
| bin | 1,000-10,000 km2 |  | 1,728 | 0.31 |
| bin | 10,000-100,000 km2 |  | 861 | 0.15 |
| bin | >100,000 km2 |  | 256 | 0.05 |

## Basin Assignment Status

| basin status | reference stations | record count |
|---|---|---|
| resolved | 5,640 | 2,251,870 |
| unresolved | 1,318 | 506,710 |
| unresolved | 200 | 9,292 |
| unresolved | 186 | 196,127 |
| unresolved | 35 | 100,672 |

## Satellite Validation Spatial Coverage

Satellite rows are validation-sidecar coverage; variable completeness is reported in the variable and QC modules.

| source | satellite station count | linked reference stations | record count |
|---|---|---|---|
| RiverSed | 32,941 | 161 | 14,199,854 |
| GSED | 5,237 | 14 | 2,144,599 |
| Dethier | 372 | 9 | 133,823 |

## Country Alias Review

| country aliases | country canonical | iso a3 | reference stations | has alias conflict |
|---|---|---|---|---|
| Mexico | Mexico | MEX | 3,630 | 0 |
| United States\|United States of America (the)\|United States of America (the)\|United States | United States | USA | 896 | 0 |
| Canada | Canada | CAN | 685 | 0 |
| Italy | Italy | ITA | 406 | 0 |
| Netherlands (the) | Netherlands (the) |  | 303 | 0 |
| Denmark | Denmark | DNK | 211 | 0 |
| India | India | IND | 208 | 0 |
| Uruguay | Uruguay | URY | 156 | 0 |
| France | France | FRA | 108 | 0 |
| Unknown | Unknown |  | 75 | 0 |
| Romania | Romania | ROU | 74 | 0 |
| Norway | Norway | NOR | 68 | 0 |
| Sweden | Sweden | SWE | 64 | 0 |
| Belgium\|Netherlands (the) | Belgium | BEL | 55 | 0 |
| China | China | CHN | 49 | 0 |

_Showing first 15 of 47 rows._

## GeoPackage Layer Counts

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

## Interpretation Notes

- Region and country statements should be made from canonical country/ISO3 tables rather than raw country text.
- Basin status here is descriptive; unresolved or lower-confidence matches are analyzed in `basin_diagnostics`.
- Satellite validation coverage is kept separate from the main in-situ matrix products.

## Figures

- `fig_climatology_spatial_coverage.png`: `output_other/stats_release/spatial/figures/fig_climatology_spatial_coverage.png`
- `fig_climatology_vs_timeseries_coverage.png`: `output_other/stats_release/spatial/figures/fig_climatology_vs_timeseries_coverage.png`
- `fig_composite_spatial_coverage.png`: `output_other/stats_release/spatial/figures/fig_composite_spatial_coverage.png`
- `fig_global_bubble_map.png`: `output_other/stats_release/spatial/figures/fig_global_bubble_map.png`
- `fig_global_station_distribution.png`: `output_other/stats_release/spatial/figures/fig_global_station_distribution.png`
- `fig_global_station_status_and_basins.png`: `output_other/stats_release/spatial/figures/fig_global_station_status_and_basins.png`
- `fig_main_vs_satellite_spatial_coverage.png`: `output_other/stats_release/spatial/figures/fig_main_vs_satellite_spatial_coverage.png`
- `fig_reference_stations_by_resolution.png`: `output_other/stats_release/spatial/figures/fig_reference_stations_by_resolution.png`
- `fig_satellite_upstream_area_distribution.png`: `output_other/stats_release/spatial/figures/fig_satellite_upstream_area_distribution.png`
- `fig_satellite_validation_spatial_distribution.png`: `output_other/stats_release/spatial/figures/fig_satellite_validation_spatial_distribution.png`
- `fig_source_spatial_contribution.png`: `output_other/stats_release/spatial/figures/fig_source_spatial_contribution.png`
- `fig_spatial_coverage_by_region.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region.png`
- `fig_spatial_coverage_by_region_country.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_country.png`
- `fig_spatial_coverage_by_region_resolution.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_resolution.png`
- `fig_spatial_coverage_by_region_source_records.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_source_records.png`
- `fig_spatial_coverage_by_region_source_reference_stations.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_source_reference_stations.png`
- Additional figures: 5
