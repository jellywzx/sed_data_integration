# S8 spatial coverage statistics for ESSD

## Manuscript-ready summary

The S8 release contains 3,762 final main-product clusters. Resolution-specific coverage is 1,596 daily clusters, 2,117 monthly clusters, and 58 annual clusters. Basin assignment resolved 2,969 clusters (78.92%), while 793 clusters (21.08%) remain unresolved and 0 clusters (0%) have unknown or other basin status. The published basin sidecar contains polygons for 2,978 clusters (79.16%).

The main-product coordinates span -10.61 to 80.60 degrees latitude and -159.47 to 158.72 degrees longitude. Valid upstream basin areas are available for 2,969 clusters; the median area is 1,641.78 km2, with an interquartile range of 347.44-9,621.79 km2 and a maximum of 2,959,787.75 km2.

Main source contributions by cluster count: GFQA_v2: 1,901 clusters, 56,457 records; USGS: 887 clusters, 1,657,251 records; HYDAT: 505 clusters, 669,567 records; EUSEDcollab: 244 clusters, 66,637 records; GloRiSe: 77 clusters, 103 records

The satellite-validation product contains 38,550 station-resolution rows linked to 38,550 clusters.

## Key Metrics

- Final clusters: 3,762
- Station catalog rows: 3,771
- Main-product record count: 2,873,420
- Basin-resolved clusters: 2,969 (78.92%)
- Published basin polygons: 2,978 (79.16%)
- Unknown country clusters: 77

## Resolution Coverage

| Resolution | Station rows | Clusters | Records | Record share | Countries |
|---|---|---|---|---|---|
| annual | 58 | 58 | 619 | 0.02% | 3 |
| daily | 1,596 | 1,596 | 2,746,665 | 95.59% | 15 |
| monthly | 2,117 | 2,117 | 126,136 | 4.39% | 13 |

Resolution-specific records are uneven, so spatial coverage should be interpreted together with temporal record volume.

## Upstream Basin Area

| Area bin | Clusters | Share of valid-area clusters |
|---|---|---|
| <10 km2 | 0 | 0% |
| 10-100 km2 | 289 | 9.73% |
| 100-1,000 km2 | 926 | 31.19% |
| 1,000-10,000 km2 | 1,032 | 34.76% |
| 10,000-100,000 km2 | 540 | 18.19% |
| >100,000 km2 | 182 | 6.13% |

## Geographic Hotspots

### Regions by Cluster Count

| continent region | cluster count | record count | country count |
|---|---|---|---|
| North America | 3,291 | 2,383,275 | 4 |
| Europe | 314 | 463,173 | 13 |
| Unknown | 77 | 103 | 1 |
| Asia | 68 | 15,043 | 5 |
| South America | 11 | 11,398 | 4 |
| Africa | 1 | 428 | 1 |

### Countries by Cluster Count

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

### Region-Resolution Record Hotspots

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

_Showing first 15 of 16 rows._

## Source Spatial Contribution

| source name | cluster count | source station count | record count | available resolutions |
|---|---|---|---|---|
| GFQA_v2 | 1,901 | 2,050 | 56,457 | annual\|daily\|monthly |
| USGS | 887 | 887 | 1,657,251 | daily |
| HYDAT | 505 | 505 | 669,567 | daily |
| EUSEDcollab | 244 | 244 | 66,637 | monthly |
| GloRiSe | 77 | 77 | 103 | daily\|monthly |
| Bayern | 34 | 34 | 388,964 | daily |
| Huanghe | 24 | 24 | 120 | annual |
| Yajiang | 23 | 23 | 23 | daily |
| Eurasian_River | 17 | 17 | 3,204 | monthly |
| HYBAM | 12 | 12 | 11,826 | daily |
| Rhine | 12 | 12 | 312 | daily |
| Chao_Phraya_River | 7 | 7 | 348 | annual |

_Showing first 12 of 18 rows._

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

## Diagnostics and Limitations

- Unknown country/region rows written for review: 77
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
