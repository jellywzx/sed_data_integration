# S8 spatial coverage statistics for ESSD

## Manuscript-ready summary

The S8 release contains 3,528 final main-product clusters. Resolution-specific coverage is 1,607 daily clusters, 1,875 monthly clusters, and 58 annual clusters. Basin assignment resolved 2,849 clusters (80.8% of all clusters), while 665 clusters (18.8%) remain unresolved and 14 clusters (0.4%) have unknown or other basin status. The published basin sidecar contains polygons for 2,447 clusters (69.4% of all clusters).

The main-product coordinates span -10.6 to 80.6 degrees latitude and -159.5 to 158.7 degrees longitude. Valid upstream basin areas are available for 2,849 clusters; the median area is 2191.8 km2, with an interquartile range of 443.7-15374.8 km2 and a maximum of 5200432.0 km2.

Main source contributions by cluster count: GFQA_v2: 1,692 clusters, 56,297 records; USGS: 873 clusters, 1,651,590 records; HYDAT: 480 clusters, 661,138 records; EUSEDcollab: 216 clusters, 63,208 records; GloRiSe: 128 clusters, 154 records

The satellite-validation product contains 47,785 station rows linked to 47,785 clusters, with coordinates spanning -50.2 to 75.6 degrees latitude and -163.8 to 175.8 degrees longitude.

## Key Metrics

- Final clusters: 3,528
- Station catalog rows: 3,540
- Main-product record count: 2,847,547
- Basin-resolved clusters: 2,849 (80.8%)
- Published basin polygons: 2,447 (69.4%)
- Unknown country clusters: 128 (3.6%)

## Resolution Coverage

| Resolution | Clusters | Records | Record share | Resolved | Polygons | Median area km2 | Latitude | Longitude |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| daily | 1,607 | 2,724,382 | 95.67% | 1,214 (59.6%) | 957 (59.6%) | 2265.7 | -10.6 to 80.6 | -159.5 to 144.8 |
| monthly | 1,875 | 122,546 | 4.30% | 1,600 (77.4%) | 1,451 (77.4%) | 2120.9 | 14.5 to 74.5 | -157.8 to 158.7 |
| annual | 58 | 619 | 0.02% | 47 (81.0%) | 47 (81.0%) | 12920.1 | 12.9 to 40.3 | -117.0 to 118.3 |

Resolution-specific records are highly uneven, so spatial coverage should be interpreted together with temporal record volume. Annual coverage is spatially narrow but can still contain long individual records.

## Upstream Basin Area

| Area bin | Clusters | Share of valid-area clusters |
| --- | --- | --- |
| <10 km2 | 0 | 0.0% |
| 10-100 km2 | 240 | 8.4% |
| 100-1,000 km2 | 810 | 28.4% |
| 1,000-10,000 km2 | 946 | 33.2% |
| 10,000-100,000 km2 | 651 | 22.9% |
| >100,000 km2 | 202 | 7.1% |

The basin-area distribution is right-skewed: most resolved clusters fall below 10,000 km2, while a smaller set of very large basins controls the upper tail.

## Geographic Hotspots

### Regions by Cluster Count

| Region | Clusters | Resolved | Polygons |
| --- | --- | --- | --- |
| North America | 3,036 | 82.2% | 70.5% |
| Europe | 228 | 69.3% | 64.9% |
| Unknown | 128 | 57.0% | 57.0% |
| Asia, East Asia | 51 | 100.0% | 54.9% |
| Europe, Central Europe | 32 | 100.0% | 100.0% |
| Asia, Southeast Asia | 16 | 87.5% | 37.5% |
| Europe, Eastern Europe | 14 | 85.7% | 85.7% |
| South America | 11 | 45.5% | 45.5% |
| Europe, Western Europe | 7 | 57.1% | 14.3% |
| Europe, Eastern Europe\|North America | 3 | 100.0% | 100.0% |

### Countries by Cluster Count

| Country | Clusters | Resolved | Polygons |
| --- | --- | --- | --- |
| Mexico | 1,684 | 87.4% | 78.8% |
| United States | 871 | 72.6% | 56.6% |
| Canada | 478 | 81.6% | 66.3% |
| Denmark | 188 | 66.0% | 66.0% |
| Unknown | 128 | 57.0% | 57.0% |
| China | 49 | 100.0% | 53.1% |
| Germany | 32 | 100.0% | 100.0% |
| Russia | 14 | 85.7% | 85.7% |
| Germany | 12 | 83.3% | 0.0% |
| Brazil | 7 | 42.9% | 42.9% |
| Spain | 7 | 85.7% | 85.7% |
| Thailand | 7 | 85.7% | 85.7% |
| United Kingdom | 7 | 57.1% | 14.3% |
| Belgium | 6 | 50.0% | 50.0% |
| Myanmar | 6 | 83.3% | 0.0% |

### Region-Resolution Record Hotspots

| Continent | Region | Resolution | Clusters | Source stations | Records |
| --- | --- | --- | --- | --- | --- |
| North America | North America | daily | 1,393 | 1,428 | 2,312,780 |
| Europe, Central Europe | Europe, Central Europe | daily | 32 | 34 | 380,719 |
| Europe | Europe | monthly | 216 | 226 | 63,208 |
| North America | North America | monthly | 1,628 | 1,989 | 56,005 |
| South America | South America | daily | 11 | 11 | 11,398 |
| Asia, Southeast Asia | Asia, Southeast Asia | daily | 9 | 9 | 8,772 |
| Europe, Western Europe | Europe, Western Europe | daily | 7 | 7 | 4,056 |
| Asia, East Asia | Asia, East Asia | daily | 27 | 27 | 3,246 |
| Europe, Eastern Europe | Europe, Eastern Europe | monthly | 14 | 14 | 2,574 |
| North America\|Asia, Southeast Asia | North America\|Asia, Southeast Asia | daily | 1 | 1 | 2,557 |
| Europe, Eastern Europe\|North America | Europe, Eastern Europe\|North America | monthly | 3 | 7 | 719 |
| Africa | Africa | daily | 1 | 1 | 428 |
| Asia, Southeast Asia | Asia, Southeast Asia | annual | 7 | 7 | 348 |
| Europe | Europe | daily | 12 | 12 | 312 |
| North America | North America | annual | 27 | 27 | 151 |

## Source Spatial Contribution

### Top Sources by Clusters

| Source | Clusters | Source stations | Records | Resolutions |
| --- | --- | --- | --- | --- |
| GFQA_v2 | 1,692 | 2,062 | 56,297 | annual\|daily\|monthly |
| USGS | 873 | 885 | 1,651,590 | daily |
| HYDAT | 480 | 501 | 661,138 | daily |
| EUSEDcollab | 216 | 226 | 63,208 | monthly |
| GloRiSe | 128 | 128 | 154 | daily\|monthly |
| Bayern | 32 | 34 | 380,719 | daily |
| Huanghe | 24 | 24 | 120 | annual |
| Yajiang | 23 | 23 | 23 | daily |
| Eurasian_River | 17 | 17 | 3,204 | monthly |
| HYBAM | 12 | 12 | 11,826 | daily |
| Rhine | 12 | 12 | 312 | daily |
| Chao_Phraya_River | 7 | 7 | 348 | annual |

### Top Sources by Records

| Source | Clusters | Source stations | Records | Resolutions |
| --- | --- | --- | --- | --- |
| USGS | 873 | 885 | 1,651,590 | daily |
| HYDAT | 480 | 501 | 661,138 | daily |
| Bayern | 32 | 34 | 380,719 | daily |
| EUSEDcollab | 216 | 226 | 63,208 | monthly |
| GFQA_v2 | 1,692 | 2,062 | 56,297 | annual\|daily\|monthly |
| HYBAM | 12 | 12 | 11,826 | daily |
| Mekong_Delta | 4 | 4 | 11,323 | daily |
| Robotham | 3 | 3 | 3,432 | daily |
| Eurasian_River | 17 | 17 | 3,204 | monthly |
| Fukushima | 2 | 2 | 3,069 | daily |
| NERC | 4 | 4 | 624 | daily |
| Chao_Phraya_River | 7 | 7 | 348 | annual |

The cluster-based and record-based rankings answer different questions: the former describes spatial footprint, while the latter describes the amount of time-series information contributed by each source.

## Satellite Validation Spatial Coverage

| Source | Station rows | Linked clusters | Records | Latitude | Longitude | Time span |
| --- | --- | --- | --- | --- | --- | --- |
| all | 47,785 | 47,785 | 16,506,461 | -50.2 to 75.6 | -163.8 to 175.8 | 1984-01-15 to 2020-12-15 |
| RiverSed | 42,177 | 42,177 | 14,228,483 | 25.1 to 49.4 | -124.4 to -67.1 | 1984-03-22 to 2019-08-23 |
| GSED | 5,237 | 5,237 | 2,144,599 | -50.2 to 71.8 | -162.9 to 175.8 | 1985-01-01 to 2020-12-01 |
| Dethier | 371 | 371 | 133,379 | -50.0 to 75.6 | -163.8 to 174.2 | 1984-01-15 to 2020-12-15 |

## Basin Polygon Layers

| Layer | Resolution | Polygon features | Polygon clusters |
| --- | --- | --- | --- |
| basin_daily | daily | 957 | 957 |
| basin_monthly | monthly | 1,451 | 1,451 |
| basin_annual | annual | 47 | 47 |

## Diagnostics and Limitations

- Unknown country/region rows written for review: 128
- Regional summaries depend on S8 release catalog geography; unknown geography should be reviewed before strong continent/country claims.
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

- Main text: `fig_global_cluster_distribution`, `fig_spatial_coverage_by_resolution`, and `fig_upstream_area_distribution`.
- Supplement: `fig_spatial_coverage_by_region_country`, `fig_source_spatial_contribution`, and satellite-validation spatial figures.

## Manuscript-Usable Statements

- The release provides broad river-basin coverage, but regional completeness should be interpreted together with unresolved basin and unknown-geography diagnostics.
- The published basin sidecar covers the same cluster count as the resolved-basin subset, making polygon availability a direct proxy for basin-resolution success in this release.
- Source rankings should be separated into spatial footprint and record-volume contribution, because a source can cover many clusters with few records or fewer clusters with dense long records.

<!-- Compact legacy paragraph values

Upstream-area bins: <10 km2: 0 clusters (0.0%); 10-100 km2: 240 clusters (8.4%); 100-1,000 km2: 810 clusters (28.4%); 1,000-10,000 km2: 946 clusters (33.2%); 10,000-100,000 km2: 651 clusters (22.9%); >100,000 km2: 202 clusters (7.1%)

Main source contributions by cluster count: GFQA_v2: 1,692 clusters, 56,297 records; USGS: 873 clusters, 1,651,590 records; HYDAT: 480 clusters, 661,138 records; EUSEDcollab: 216 clusters, 63,208 records; GloRiSe: 128 clusters, 154 records

-->
