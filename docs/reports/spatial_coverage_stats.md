# Release Spatial Coverage Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/spatial/tables`
- Spatial statistics use release catalogs, GeoPackages, and satellite validation catalogs only.

## Headline

- Reference clusters: 3,762
- Release records represented by station catalog:
- Country/region rows needing canonicalization review: 0
- Clusters with unknown country or region: 77

## Article-Ready Metrics

| section | metric | value | unit | source file | notes |
|---|---|---|---|---|---|
| main_release | station_catalog_rows | 3,771 | rows | station_catalog.csv |  |
| main_release | final_cluster_count | 3,762 | clusters | station_catalog.csv |  |
| main_release | daily_cluster_count | 1,596 | clusters | station_catalog.csv |  |
| main_release | monthly_cluster_count | 2,117 | clusters | station_catalog.csv |  |
| main_release | annual_cluster_count | 58 | clusters | station_catalog.csv |  |
| basin_assignment | resolved_cluster_count | 2,969 | clusters | station_catalog.csv |  |
| basin_assignment | resolved_cluster_percent | 78.92 | percent | station_catalog.csv |  |
| basin_assignment | unresolved_cluster_count | 793 | clusters | station_catalog.csv |  |
| basin_assignment | unresolved_cluster_percent | 21.08 | percent | station_catalog.csv |  |
| basin_assignment | unknown_status_cluster_count | 0 | clusters | station_catalog.csv |  |
| basin_assignment | unknown_status_cluster_percent | 0 | percent | station_catalog.csv |  |
| basin_polygons | basin_polygon_cluster_count | 2,978 | clusters | sed_reference_cluster_basins.gpkg |  |
| basin_polygons | basin_polygon_cluster_percent | 79.16 | percent | sed_reference_cluster_basins.gpkg |  |
| coordinates | clusters_with_valid_lat_lon | 3,762 | clusters | station_catalog.csv |  |
| coordinates | latitude_min | -10.61 | degrees_north | station_catalog.csv |  |
| coordinates | latitude_max | 80.60 | degrees_north | station_catalog.csv |  |
| coordinates | longitude_min | -159.47 | degrees_east | station_catalog.csv |  |
| coordinates | longitude_max | 158.72 | degrees_east | station_catalog.csv |  |

_Showing first 18 of 32 rows._

## Coverage by Temporal Resolution

| resolution | source station resolution rows | cluster count | record count | country count |
|---|---|---|---|---|
| daily | 1,596 | 1,596 | 2,746,665 | 15 |
| monthly | 2,117 | 2,117 | 126,136 | 13 |
| annual | 58 | 58 | 619 | 3 |

## Coverage by Region

| continent region | cluster count | record count | country count |
|---|---|---|---|
| North America | 3,291 | 2,383,275 | 4 |
| Europe | 314 | 463,173 | 13 |
| Unknown | 77 | 103 | 1 |
| Asia | 68 | 15,043 | 5 |
| South America | 11 | 11,398 | 4 |
| Africa | 1 | 428 | 1 |

## Top Countries

Country statistics prefer canonical country names and ISO3 codes where available.

| country | iso a3 | continent region | cluster count | record count |
|---|---|---|---|---|
| Mexico | MEX | North America | 1,900 | 56,453 |
| United States | USA | North America | 886 | 1,655,754 |
| Canada | CAN | North America | 503 | 664,492 |
| Denmark | DNK | Europe | 211 | 15,561 |
| Unknown |  | Unknown | 77 | 103 |
| China | CHN | Asia | 49 | 297 |
| Germany | DEU | Europe | 46 | 389,276 |
| Russia | RUS | Europe | 17 | 3,204 |
| Spain | ESP | Europe | 8 | 26,881 |
| Brazil | BRA | South America | 7 | 5,169 |
| United Kingdom | GBR | Europe | 7 | 4,056 |
| Thailand | THA | Asia | 7 | 348 |
| Belgium | BEL | Europe | 6 | 237 |
| Myanmar | MMR | Asia | 6 | 6 |
| Greece | GRC | Europe | 5 | 3,040 |

_Showing first 15 of 28 rows._

## Top Source Spatial Contributions

| source name | cluster count | record count |
|---|---|---|
| USGS | 887 | 1,657,251 |
| HYDAT | 505 | 669,567 |
| Bayern | 34 | 388,964 |
| EUSEDcollab | 244 | 66,637 |
| GFQA_v2 | 1,901 | 56,457 |
| HYBAM | 12 | 11,826 |
| Mekong_Delta | 4 | 11,323 |
| Robotham | 3 | 3,432 |
| Eurasian_River | 17 | 3,204 |
| Fukushima | 2 | 3,069 |
| NERC | 4 | 624 |
| Chao_Phraya_River | 7 | 348 |
| Rhine | 12 | 312 |
| Shashi_Jianli | 2 | 154 |
| Huanghe | 24 | 120 |

_Showing first 15 of 18 rows._

## Region by Resolution

| continent region | resolution | cluster count | record count |
|---|---|---|---|
| North America | daily | 1,431 | 2,326,869 |
| Europe, Central Europe | daily | 34 | 388,964 |
| Europe | monthly | 244 | 66,637 |
| North America | monthly | 1,842 | 56,255 |
| South America | daily | 11 | 11,398 |
| Asia, Southeast Asia | daily | 10 | 11,329 |
| Europe, Western Europe | daily | 7 | 4,056 |
| Asia, East Asia | daily | 27 | 3,246 |
| Europe, Eastern Europe | monthly | 17 | 3,204 |
| Africa | daily | 1 | 428 |
| Asia, Southeast Asia | annual | 7 | 348 |
| Europe | daily | 12 | 312 |
| North America | annual | 27 | 151 |
| Asia, East Asia | annual | 24 | 120 |
|  | daily | 63 | 63 |
|  | monthly | 14 | 40 |

## Source Type Footprint

| source type | source group | source count | cluster count | record count |
|---|---|---|---|---|
| in-situ | national agencies | 3 | 1,426 | 2,715,782 |
| literature | global compilations | 13 | 2,324 | 145,658 |
| in-situ | regional datasets | 1 | 12 | 11,826 |
| satellite | satellite products | 1 | 2 | 154 |

## Upstream Area Distribution

Area metrics describe release basin polygons or catalog basin attributes, not new basin matching.

| section | label | value km2 | cluster count | fraction of valid area clusters |
|---|---|---|---|---|
| summary | valid_cluster_count |  | 2,969 |  |
| summary | missing_or_invalid_cluster_count |  | 793 |  |
| summary | min | 26.12 |  |  |
| summary | p05 | 59.13 |  |  |
| summary | p25 | 347.44 |  |  |
| summary | mean | 34,036 |  |  |
| summary | median | 1,642 |  |  |
| summary | p75 | 9,622 |  |  |
| summary | p95 | 118,285 |  |  |
| summary | max | 2,959,788 |  |  |
| bin | <10 km2 |  | 0 | 0 |
| bin | 10-100 km2 |  | 289 | 0.10 |
| bin | 100-1,000 km2 |  | 926 | 0.31 |
| bin | 1,000-10,000 km2 |  | 1,032 | 0.35 |
| bin | 10,000-100,000 km2 |  | 540 | 0.18 |
| bin | >100,000 km2 |  | 182 | 0.06 |

## Basin Assignment Status

| basin status | cluster count | record count |
|---|---|---|
| resolved | 2,969 | 2,083,117 |
| unresolved | 524 | 488,091 |
| unresolved | 186 | 196,609 |
| unresolved | 48 | 5,586 |
| unresolved | 35 | 100,017 |

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
| Mexico | Mexico | MEX | 1,900 | 0 |
| United States\|United States of America (the) | United States | USA | 886 | 0 |
| Canada | Canada | CAN | 503 | 0 |
| Denmark | Denmark | DNK | 211 | 0 |
| Unknown | Unknown |  | 77 | 0 |
| China | China | CHN | 49 | 0 |
| Germany | Germany | DEU | 46 | 0 |
| Russia | Russia | RUS | 17 | 0 |
| Spain | Spain | ESP | 8 | 0 |
| Thailand | Thailand | THA | 7 | 0 |
| Brazil | Brazil | BRA | 7 | 0 |
| United Kingdom | United Kingdom | GBR | 7 | 0 |
| Myanmar | Myanmar | MMR | 6 | 0 |
| Belgium | Belgium | BEL | 6 | 0 |
| Greece | Greece | GRC | 5 | 0 |

_Showing first 15 of 28 rows._

## GeoPackage Layer Counts

| file name | layer name | feature count |
|---|---|---|
| sed_reference_cluster_points.gpkg | cluster_summary | 3,762 |
| sed_reference_source_stations.gpkg | source_monthly | 2,257 |
| sed_reference_cluster_points.gpkg | cluster_monthly | 2,117 |
| sed_reference_cluster_basins.gpkg | basin_monthly | 1,785 |
| sed_reference_source_stations.gpkg | source_daily | 1,598 |
| sed_reference_cluster_points.gpkg | cluster_daily | 1,596 |
| sed_reference_cluster_basins.gpkg | basin_daily | 1,146 |
| sed_reference_cluster_points.gpkg | cluster_annual | 58 |
| sed_reference_source_stations.gpkg | source_annual | 58 |
| sed_reference_cluster_basins.gpkg | basin_annual | 47 |

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
