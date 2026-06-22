# Release Spatial Coverage Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/spatial/tables`
- Spatial statistics use release catalogs, GeoPackages, and satellite validation catalogs only.

## Headline

- Reference clusters: 3,528
- Release records represented by station catalog:
- Country/region rows needing canonicalization review: 0
- Clusters with unknown country or region: 128

## Article-Ready Metrics

| section | metric | value | unit | source file | notes |
|---|---|---|---|---|---|
| main_release | station_catalog_rows | 3,540 | rows | station_catalog.csv |  |
| main_release | final_cluster_count | 3,528 | clusters | station_catalog.csv |  |
| main_release | daily_cluster_count | 1,607 | clusters | station_catalog.csv |  |
| main_release | monthly_cluster_count | 1,875 | clusters | station_catalog.csv |  |
| main_release | annual_cluster_count | 58 | clusters | station_catalog.csv |  |
| basin_assignment | resolved_cluster_count | 2,849 | clusters | station_catalog.csv |  |
| basin_assignment | resolved_cluster_percent | 80.75 | percent | station_catalog.csv |  |
| basin_assignment | unresolved_cluster_count | 665 | clusters | station_catalog.csv |  |
| basin_assignment | unresolved_cluster_percent | 18.85 | percent | station_catalog.csv |  |
| basin_assignment | unknown_status_cluster_count | 14 | clusters | station_catalog.csv |  |
| basin_assignment | unknown_status_cluster_percent | 0.40 | percent | station_catalog.csv |  |
| basin_polygons | basin_polygon_cluster_count | 2,455 | clusters | sed_reference_cluster_basins.gpkg |  |
| basin_polygons | basin_polygon_cluster_percent | 69.59 | percent | sed_reference_cluster_basins.gpkg |  |
| coordinates | clusters_with_valid_lat_lon | 3,528 | clusters | station_catalog.csv |  |
| coordinates | latitude_min | -10.61 | degrees_north | station_catalog.csv |  |
| coordinates | latitude_max | 80.60 | degrees_north | station_catalog.csv |  |
| coordinates | longitude_min | -159.47 | degrees_east | station_catalog.csv |  |
| coordinates | longitude_max | 158.72 | degrees_east | station_catalog.csv |  |

_Showing first 18 of 32 rows._

## Coverage by Temporal Resolution

| resolution | source station resolution rows | cluster count | record count | country count |
|---|---|---|---|---|
| daily | 1,607 | 1,607 | 2,724,382 | 16 |
| monthly | 1,875 | 1,875 | 122,546 | 17 |
| annual | 58 | 58 | 619 | 3 |

## Coverage by Region

| continent region | cluster count | record count | country count |
|---|---|---|---|
| North America | 3,036 | 2,368,936 | 4 |
| Europe | 281 | 450,869 | 16 |
| Unknown | 128 | 154 | 1 |
| Asia | 67 | 12,486 | 5 |
| South America | 11 | 11,398 | 4 |
| Europe, Eastern Europe\|North America | 3 | 719 | 1 |
| Africa | 1 | 428 | 1 |
| North America\|Asia, Southeast Asia | 1 | 2,557 | 1 |

## Top Countries

Country statistics prefer canonical country names and ISO3 codes where available.

| country | iso a3 | continent region | cluster count | record count |
|---|---|---|---|---|
| Mexico | MEX | North America | 1,684 | 56,035 |
| United States | USA | North America | 872 | 1,650,961 |
| Canada | CAN | North America | 478 | 661,090 |
| Denmark | DNK | Europe | 188 | 14,193 |
| Unknown |  | Unknown | 128 | 154 |
| China | CHN | Asia | 49 | 297 |
| Germany | DEU | Europe | 44 | 381,031 |
| Russia | RUS | Europe | 14 | 2,574 |
| Spain | ESP | Europe | 7 | 21,402 |
| Brazil | BRA | South America | 7 | 5,169 |
| United Kingdom | GBR | Europe | 7 | 4,056 |
| Thailand | THA | Asia | 7 | 348 |
| Belgium | BEL | Europe | 6 | 237 |
| Myanmar | MMR | Asia | 6 | 6 |
| Vietnam | VNM | Asia | 3 | 8,766 |

_Showing first 15 of 33 rows._

## Top Source Spatial Contributions

| source name | cluster count | record count |
|---|---|---|
| USGS | 873 | 1,651,590 |
| HYDAT | 480 | 661,138 |
| Bayern | 32 | 380,719 |
| EUSEDcollab | 216 | 63,208 |
| GFQA_v2 | 1,692 | 56,297 |
| HYBAM | 12 | 11,826 |
| Mekong_Delta | 4 | 11,323 |
| Robotham | 3 | 3,432 |
| Eurasian_River | 17 | 3,204 |
| Fukushima | 2 | 3,069 |
| NERC | 4 | 624 |
| Chao_Phraya_River | 7 | 348 |
| Rhine | 12 | 312 |
| GloRiSe | 128 | 154 |
| Shashi_Jianli | 2 | 154 |

_Showing first 15 of 18 rows._

## Region by Resolution

| continent region | resolution | cluster count | record count |
|---|---|---|---|
| North America | daily | 1,393 | 2,312,780 |
| Europe, Central Europe | daily | 32 | 380,719 |
| Europe | monthly | 216 | 63,208 |
| North America | monthly | 1,628 | 56,005 |
| South America | daily | 11 | 11,398 |
| Asia, Southeast Asia | daily | 9 | 8,772 |
| Europe, Western Europe | daily | 7 | 4,056 |
| Asia, East Asia | daily | 27 | 3,246 |
| Europe, Eastern Europe | monthly | 14 | 2,574 |
| North America\|Asia, Southeast Asia | daily | 1 | 2,557 |
| Europe, Eastern Europe\|North America | monthly | 3 | 719 |
| Africa | daily | 1 | 428 |
| Asia, Southeast Asia | annual | 7 | 348 |
| Europe | daily | 12 | 312 |
| North America | annual | 27 | 151 |
| Asia, East Asia | annual | 24 | 120 |
|  | daily | 114 | 114 |
|  | monthly | 14 | 40 |

## Source Type Footprint

| source type | source group | source count | cluster count | record count |
|---|---|---|---|---|
| in-situ | national agencies | 3 | 1,385 | 2,693,447 |
| literature | global compilations | 13 | 2,138 | 142,120 |
| in-situ | regional datasets | 1 | 12 | 11,826 |
| satellite | satellite products | 1 | 2 | 154 |

## Upstream Area Distribution

Area metrics describe release basin polygons or catalog basin attributes, not new basin matching.

| section | label | value km2 | cluster count | fraction of valid area clusters |
|---|---|---|---|---|
| summary | valid_cluster_count |  | 2,849 |  |
| summary | missing_or_invalid_cluster_count |  | 679 |  |
| summary | min | 26.12 |  |  |
| summary | p05 | 62.78 |  |  |
| summary | p25 | 443.72 |  |  |
| summary | mean | 36,873 |  |  |
| summary | median | 2,192 |  |  |
| summary | p75 | 15,375 |  |  |
| summary | p95 | 128,929 |  |  |
| summary | max | 5,200,432 |  |  |
| bin | <10 km2 |  | 0 | 0 |
| bin | 10-100 km2 |  | 240 | 0.08 |
| bin | 100-1,000 km2 |  | 810 | 0.28 |
| bin | 1,000-10,000 km2 |  | 946 | 0.33 |
| bin | 10,000-100,000 km2 |  | 651 | 0.23 |
| bin | >100,000 km2 |  | 202 | 0.07 |

## Basin Assignment Status

| basin status | cluster count | record count |
|---|---|---|
| resolved | 2,827 | 2,309,398 |
| unresolved | 476 | 369,561 |
| unresolved | 99 | 115,999 |
| unresolved | 55 | 14,826 |
| unresolved | 35 | 34,128 |
| resolved | 22 | 3,595 |
| unknown | 14 | 40 |

## Satellite Validation Spatial Coverage

Satellite rows are validation-sidecar coverage; variable completeness is reported in the variable and QC modules.

| source | satellite station count | linked cluster count | record count |
|---|---|---|---|
| RiverSed | 42,177 | 42,177 | 14,228,483 |
| GSED | 5,237 | 5,237 | 2,144,599 |
| Dethier | 371 | 371 | 133,379 |

## Country Alias Review

| country aliases | country canonical | iso a3 | cluster count | has alias conflict |
|---|---|---|---|---|
| Mexico | Mexico | MEX | 1,684 | 0 |
| United States\|United States of America (the) | United States | USA | 872 | 0 |
| Canada | Canada | CAN | 478 | 0 |
| Denmark | Denmark | DNK | 188 | 0 |
| Unknown | Unknown |  | 128 | 0 |
| China | China | CHN | 49 | 0 |
| Germany | Germany | DEU | 44 | 0 |
| Russia | Russia | RUS | 14 | 0 |
| Thailand | Thailand | THA | 7 | 0 |
| Brazil | Brazil | BRA | 7 | 0 |
| United Kingdom | United Kingdom | GBR | 7 | 0 |
| Spain | Spain | ESP | 7 | 0 |
| Myanmar | Myanmar | MMR | 6 | 0 |
| Belgium | Belgium | BEL | 6 | 0 |
| Vietnam | Vietnam | VNM | 3 | 0 |

_Showing first 15 of 33 rows._

## GeoPackage Layer Counts

| file name | layer name | feature count |
|---|---|---|
| sed_reference_cluster_points.gpkg | cluster_summary | 3,528 |
| sed_reference_source_stations.gpkg | source_monthly | 2,250 |
| sed_reference_cluster_points.gpkg | cluster_monthly | 1,875 |
| sed_reference_source_stations.gpkg | source_daily | 1,644 |
| sed_reference_cluster_points.gpkg | cluster_daily | 1,607 |
| sed_reference_cluster_basins.gpkg | basin_monthly | 1,451 |
| sed_reference_cluster_basins.gpkg | basin_daily | 957 |
| sed_reference_cluster_points.gpkg | cluster_annual | 58 |
| sed_reference_source_stations.gpkg | source_annual | 58 |
| sed_reference_cluster_basins.gpkg | basin_annual | 47 |

## Interpretation Notes

- Region and country statements should be made from canonical country/ISO3 tables rather than raw country text.
- Basin status here is descriptive; unresolved or lower-confidence matches are analyzed in `basin_diagnostics`.
- Satellite validation coverage is kept separate from the main in-situ matrix products.

## Figures

- `fig_climatology_spatial_coverage.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_climatology_spatial_coverage.png`
- `fig_climatology_vs_timeseries_coverage.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_climatology_vs_timeseries_coverage.png`
- `fig_clusters_by_resolution.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_clusters_by_resolution.png`
- `fig_composite_spatial_coverage.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_composite_spatial_coverage.png`
- `fig_global_bubble_map.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_global_bubble_map.png`
- `fig_global_cluster_distribution.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_global_cluster_distribution.png`
- `fig_global_cluster_status_and_basins.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_global_cluster_status_and_basins.png`
- `fig_main_vs_satellite_spatial_coverage.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_main_vs_satellite_spatial_coverage.png`
- `fig_satellite_upstream_area_distribution.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_satellite_upstream_area_distribution.png`
- `fig_satellite_validation_spatial_distribution.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_satellite_validation_spatial_distribution.png`
- `fig_source_spatial_contribution.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_source_spatial_contribution.png`
- `fig_spatial_coverage_by_region.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_spatial_coverage_by_region.png`
- `fig_spatial_coverage_by_region_country.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_spatial_coverage_by_region_country.png`
- `fig_spatial_coverage_by_region_resolution.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_spatial_coverage_by_region_resolution.png`
- `fig_spatial_coverage_by_region_source_clusters.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_spatial_coverage_by_region_source_clusters.png`
- `fig_spatial_coverage_by_region_source_records.png`: `$WORKSPACE/output_other/stats_release_full/spatial/figures/fig_spatial_coverage_by_region_source_records.png`
- Additional figures: 5
