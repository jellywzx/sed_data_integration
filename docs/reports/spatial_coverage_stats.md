# Release Spatial Coverage Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/spatial/tables`
- Spatial statistics use release catalogs, GeoPackages, and satellite validation catalogs only.

## Headline

- Reference clusters: 7,135
- Release records represented by station catalog:
- Country/region rows needing canonicalization review: 0
- Clusters with unknown country or region: 397

## Article-Ready Metrics

| section | metric | value | unit | source file | notes |
|---|---|---|---|---|---|
| main_release | station_catalog_rows | 7,135 | rows | station_catalog.csv |  |
| main_release | final_cluster_count | 7,135 | clusters | station_catalog.csv |  |
| main_release | daily_cluster_count | 7,087 | clusters | station_catalog.csv |  |
| main_release | monthly_cluster_count | 17 | clusters | station_catalog.csv |  |
| main_release | annual_cluster_count | 31 | clusters | station_catalog.csv |  |
| basin_assignment | resolved_cluster_count | 5,530 | clusters | station_catalog.csv |  |
| basin_assignment | resolved_cluster_percent | 77.51 | percent | station_catalog.csv |  |
| basin_assignment | unresolved_cluster_count | 1,605 | clusters | station_catalog.csv |  |
| basin_assignment | unresolved_cluster_percent | 22.49 | percent | station_catalog.csv |  |
| basin_assignment | unknown_status_cluster_count | 0 | clusters | station_catalog.csv |  |
| basin_assignment | unknown_status_cluster_percent | 0 | percent | station_catalog.csv |  |
| basin_polygons | basin_polygon_cluster_count | 5,530 | clusters | sed_reference_cluster_basins.gpkg |  |
| basin_polygons | basin_polygon_cluster_percent | 77.51 | percent | sed_reference_cluster_basins.gpkg |  |
| coordinates | clusters_with_valid_lat_lon | 7,135 | clusters | station_catalog.csv |  |
| coordinates | latitude_min | -34.86 | degrees_north | station_catalog.csv |  |
| coordinates | latitude_max | 81.94 | degrees_north | station_catalog.csv |  |
| coordinates | longitude_min | -159.47 | degrees_east | station_catalog.csv |  |
| coordinates | longitude_max | 158.72 | degrees_east | station_catalog.csv |  |

_Showing first 18 of 32 rows._

## Coverage by Temporal Resolution

| resolution | source station resolution rows | cluster count | record count | country count |
|---|---|---|---|---|
| daily | 7,087 | 7,087 | 2,993,390 | 40 |
| monthly | 17 | 17 | 3,263 | 1 |
| annual | 31 | 31 | 468 | 2 |

## Coverage by Region

| continent region | cluster count | record count | country count |
|---|---|---|---|
| North America | 5,213 | 2,464,519 | 4 |
| Europe | 1,081 | 486,294 | 25 |
| Unknown | 397 | 13,412 | 3 |
| Asia, South Asia | 208 | 5,339 | 1 |
| South America | 168 | 11,701 | 6 |
| Asia | 67 | 15,641 | 5 |
| Africa | 1 | 215 | 1 |

## Top Countries

Country statistics prefer canonical country names and ISO3 codes where available.

| country | iso a3 | continent region | cluster count | record count |
|---|---|---|---|---|
| Mexico | MEX | North America | 3,630 | 91,854 |
| United States | USA | North America | 896 | 1,684,297 |
| Canada | CAN | North America | 685 | 681,792 |
| Italy | ITA | Europe | 404 | 15,461 |
| Netherlands (the) |  | Unknown | 303 | 12,561 |
| India | IND | Asia, South Asia | 208 | 5,339 |
| Uruguay | URY | South America | 156 | 2,501 |
| France | FRA | Europe | 105 | 9,091 |
| Unknown |  | Unknown | 75 | 97 |
| Romania | ROU | Europe | 74 | 2,672 |
| Norway | NOR | Europe | 68 | 4,634 |
| Sweden | SWE | Europe | 64 | 2,514 |
| Germany | DEU | Europe | 49 | 421,364 |
| Belgium | BEL | Europe | 49 | 3,657 |
| China | CHN | Asia | 49 | 297 |

_Showing first 15 of 45 rows._

## Top Source Spatial Contributions

| source name | cluster count | record count |
|---|---|---|
| USGS | 889 | 1,685,357 |
| HYDAT | 540 | 671,979 |
| Bayern | 37 | 421,052 |
| GFQA_v2 | 5,499 | 185,954 |
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
| GloRiSe | 77 | 103 |

_Showing first 15 of 17 rows._

## Region by Resolution

| continent region | resolution | cluster count | record count |
|---|---|---|---|
| North America | daily | 5,213 | 2,464,519 |
| Europe, Central Europe | daily | 37 | 421,052 |
| Europe | daily | 1,020 | 57,923 |
|  | daily | 397 | 13,412 |
| Asia, Southeast Asia | daily | 9 | 11,927 |
| South America | daily | 168 | 11,701 |
| Asia, South Asia | daily | 208 | 5,339 |
| Europe, Western Europe | daily | 7 | 4,056 |
| Europe, Eastern Europe | monthly | 17 | 3,263 |
| Asia, East Asia | daily | 27 | 3,246 |
| Asia, Southeast Asia | annual | 7 | 348 |
| Africa | daily | 1 | 215 |
| Asia, East Asia | annual | 24 | 120 |

## Source Type Footprint

| source type | source group | source count | cluster count | record count |
|---|---|---|---|---|
| in_situ | in_situ | 17 | 7,157 | 2,997,121 |

## Upstream Area Distribution

Area metrics describe release basin polygons or catalog basin attributes, not new basin matching.

| section | label | value km2 | cluster count | fraction of valid area clusters |
|---|---|---|---|---|
| summary | valid_cluster_count |  | 5,530 |  |
| summary | missing_or_invalid_cluster_count |  | 1,605 |  |
| summary | min | 25.19 |  |  |
| summary | p05 | 50.58 |  |  |
| summary | p25 | 225.71 |  |  |
| summary | mean | 25,894 |  |  |
| summary | median | 1,100 |  |  |
| summary | p75 | 6,551 |  |  |
| summary | p95 | 87,679 |  |  |
| summary | max | 2,959,788 |  |  |
| bin | <10 km2 |  | 0 | 0 |
| bin | 10-100 km2 |  | 718 | 0.13 |
| bin | 100-1,000 km2 |  | 1,967 | 0.36 |
| bin | 1,000-10,000 km2 |  | 1,728 | 0.31 |
| bin | 10,000-100,000 km2 |  | 861 | 0.16 |
| bin | >100,000 km2 |  | 256 | 0.05 |

## Basin Assignment Status

| basin status | cluster count | record count |
|---|---|---|
| resolved | 5,530 | 2,238,609 |
| unresolved | 1,254 | 482,852 |
| unresolved | 200 | 9,292 |
| unresolved | 116 | 165,696 |
| unresolved | 35 | 100,672 |

## Satellite Validation Spatial Coverage

Satellite rows are validation-sidecar coverage; variable completeness is reported in the variable and QC modules.

| source | satellite station count | linked cluster count | record count |
|---|---|---|---|
| RiverSed | 32,941 | 32,941 | 14,199,854 |
| GSED | 5,237 | 5,237 | 2,144,599 |
| Dethier | 372 | 372 | 133,823 |

## Country Alias Review

| country aliases | country canonical | iso a3 | cluster count | has alias conflict |
|---|---|---|---|---|
| Mexico | Mexico | MEX | 3,630 | 0 |
| United States\|United States of America (the)\|United States of America (the)\|United States | United States | USA | 896 | 0 |
| Canada | Canada | CAN | 685 | 0 |
| Italy | Italy | ITA | 404 | 0 |
| Netherlands (the) | Netherlands (the) |  | 303 | 0 |
| India | India | IND | 208 | 0 |
| Uruguay | Uruguay | URY | 156 | 0 |
| France | France | FRA | 105 | 0 |
| Unknown | Unknown |  | 75 | 0 |
| Romania | Romania | ROU | 74 | 0 |
| Norway | Norway | NOR | 68 | 0 |
| Sweden | Sweden | SWE | 64 | 0 |
| China | China | CHN | 49 | 0 |
| Germany | Germany | DEU | 49 | 0 |
| Belgium\|Belgium\|Netherlands (the) | Belgium | BEL | 49 | 0 |

_Showing first 15 of 45 rows._

## GeoPackage Layer Counts

| file name | layer name | feature count |
|---|---|---|
| sed_reference_source_stations.gpkg | source_daily | 7,421 |
| sed_reference_cluster_points.gpkg | cluster_summary | 7,135 |
| sed_reference_cluster_points.gpkg | cluster_daily | 7,087 |
| sed_reference_cluster_basins.gpkg | basin_daily | 5,489 |
| sed_reference_cluster_points.gpkg | cluster_annual | 31 |
| sed_reference_source_stations.gpkg | source_annual | 31 |
| sed_reference_cluster_basins.gpkg | basin_annual | 30 |
| sed_reference_cluster_points.gpkg | cluster_monthly | 17 |
| sed_reference_source_stations.gpkg | source_monthly | 17 |
| sed_reference_cluster_basins.gpkg | basin_monthly | 11 |

## Interpretation Notes

- Region and country statements should be made from canonical country/ISO3 tables rather than raw country text.
- Basin status here is descriptive; unresolved or lower-confidence matches are analyzed in `basin_diagnostics`.
- Satellite validation coverage is kept separate from the main in-situ matrix products.

## Figures

- `fig_climatology_spatial_coverage.png`: `output_other/stats_release/spatial/figures/fig_climatology_spatial_coverage.png`
- `fig_climatology_vs_timeseries_coverage.png`: `output_other/stats_release/spatial/figures/fig_climatology_vs_timeseries_coverage.png`
- `fig_clusters_by_resolution.png`: `output_other/stats_release/spatial/figures/fig_clusters_by_resolution.png`
- `fig_composite_spatial_coverage.png`: `output_other/stats_release/spatial/figures/fig_composite_spatial_coverage.png`
- `fig_global_bubble_map.png`: `output_other/stats_release/spatial/figures/fig_global_bubble_map.png`
- `fig_global_cluster_distribution.png`: `output_other/stats_release/spatial/figures/fig_global_cluster_distribution.png`
- `fig_global_cluster_status_and_basins.png`: `output_other/stats_release/spatial/figures/fig_global_cluster_status_and_basins.png`
- `fig_main_vs_satellite_spatial_coverage.png`: `output_other/stats_release/spatial/figures/fig_main_vs_satellite_spatial_coverage.png`
- `fig_satellite_upstream_area_distribution.png`: `output_other/stats_release/spatial/figures/fig_satellite_upstream_area_distribution.png`
- `fig_satellite_validation_spatial_distribution.png`: `output_other/stats_release/spatial/figures/fig_satellite_validation_spatial_distribution.png`
- `fig_source_spatial_contribution.png`: `output_other/stats_release/spatial/figures/fig_source_spatial_contribution.png`
- `fig_spatial_coverage_by_region.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region.png`
- `fig_spatial_coverage_by_region_country.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_country.png`
- `fig_spatial_coverage_by_region_resolution.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_resolution.png`
- `fig_spatial_coverage_by_region_source_clusters.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_source_clusters.png`
- `fig_spatial_coverage_by_region_source_records.png`: `output_other/stats_release/spatial/figures/fig_spatial_coverage_by_region_source_records.png`
- Additional figures: 5
