# S8 spatial coverage statistics for ESSD

## Manuscript-ready summary

The S8 release contains 7,135 final main-product clusters. Resolution-specific coverage is 7,087 daily clusters, 17 monthly clusters, and 31 annual clusters. Basin assignment resolved 5,530 clusters (77.51%), while 1,605 clusters (22.49%) remain unresolved and 0 clusters (0%) have unknown or other basin status. The published basin sidecar contains polygons for 5,530 clusters (77.51%).

The main-product coordinates span -34.86 to 81.94 degrees latitude and -159.47 to 158.72 degrees longitude. Valid upstream basin areas are available for 5,530 clusters; the median area is 1,100.43 km2, with an interquartile range of 225.71-6,551.07 km2 and a maximum of 2,959,787.75 km2.

Main source contributions by cluster count: GFQA_v2: 5,499 clusters, 185,954 records; USGS: 889 clusters, 1,685,357 records; HYDAT: 540 clusters, 671,979 records; GloRiSe: 77 clusters, 103 records; Bayern: 37 clusters, 421,052 records

The satellite-validation product contains 38,550 station-resolution rows linked to 38,550 clusters.

## Key Metrics

- Final clusters: 7,135
- Station catalog rows: 7,135
- Main-product record count: 2,997,121
- Basin-resolved clusters: 5,530 (77.51%)
- Published basin polygons: 5,530 (77.51%)
- Unknown country clusters: 75

## Resolution Coverage

| Resolution | Station rows | Clusters | Records | Record share | Countries |
|---|---|---|---|---|---|
| annual | 31 | 31 | 468 | 0.02% | 2 |
| daily | 7,087 | 7,087 | 2,993,390 | 99.88% | 40 |
| monthly | 17 | 17 | 3,263 | 0.11% | 1 |

Resolution-specific records are uneven, so spatial coverage should be interpreted together with temporal record volume.

## Upstream Basin Area

| Area bin | Clusters | Share of valid-area clusters |
|---|---|---|
| <10 km2 | 0 | 0% |
| 10-100 km2 | 718 | 12.98% |
| 100-1,000 km2 | 1,967 | 35.57% |
| 1,000-10,000 km2 | 1,728 | 31.25% |
| 10,000-100,000 km2 | 861 | 15.57% |
| >100,000 km2 | 256 | 4.63% |

## Geographic Hotspots

### Regions by Cluster Count

| continent region | cluster count | record count | country count |
|---|---|---|---|
| North America | 5,213 | 2,464,519 | 4 |
| Europe | 1,081 | 486,294 | 25 |
| Unknown | 397 | 13,412 | 3 |
| Asia, South Asia | 208 | 5,339 | 1 |
| South America | 168 | 11,701 | 6 |
| Asia | 67 | 15,641 | 5 |
| Africa | 1 | 215 | 1 |

### Countries by Cluster Count

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

### Region-Resolution Record Hotspots

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

## Source Spatial Contribution

| source name | cluster count | source station count | record count | available resolutions |
|---|---|---|---|---|
| GFQA_v2 | 5,499 | 5,808 | 185,954 | daily |
| USGS | 889 | 890 | 1,685,357 | daily |
| HYDAT | 540 | 541 | 671,979 | daily |
| GloRiSe | 77 | 77 | 103 | daily |
| Bayern | 37 | 37 | 421,052 | daily |
| Huanghe | 24 | 24 | 120 | annual |
| Yajiang | 23 | 23 | 23 | daily |
| Eurasian_River | 17 | 17 | 3,263 | monthly |
| HYBAM | 12 | 12 | 9,404 | daily |
| Rhine | 12 | 12 | 312 | daily |
| Chao_Phraya_River | 7 | 7 | 348 | annual |
| Myanmar | 5 | 6 | 6 | daily |

_Showing first 12 of 17 rows._

The cluster-based and record-based rankings answer different questions: the former describes spatial footprint, while the latter describes record volume.

## Satellite Validation Spatial Coverage

| source | resolution | satellite station count | linked cluster count | record count |
|---|---|---|---|---|
| RiverSed | daily | 32,941 | 32,941 | 14,199,854 |
| GSED | monthly | 5,237 | 5,237 | 2,144,599 |
| Dethier | monthly | 372 | 372 | 133,823 |

## Basin Polygon Layers

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

## Diagnostics and Limitations

- Unknown country/region rows written for review: 397
- Regional summaries depend on release catalog geography; unknown geography should be reviewed before strong continent/country claims.
- Cluster counts by source are not additive across sources because multiple datasets can contribute to the same merged cluster.

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
- `tables/table_unknown_country_region_clusters.csv`

## Figure Suggestions

- Main text: `fig_spatial_coverage_by_resolution`, `fig_top_countries_by_clusters`, and `fig_upstream_area_distribution`.
- Supplement: source contribution, basin status, and satellite-validation spatial figures.

## Manuscript-Usable Statements

- The release provides broad river-basin coverage, but regional completeness should be interpreted together with unresolved basin and unknown-geography diagnostics.
- Polygon availability provides a direct release-side indicator of basin sidecar coverage.
- Source rankings should separate spatial footprint from record-volume contribution.
