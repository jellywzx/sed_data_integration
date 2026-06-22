# Spatial Match Error Detailed Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/tables`
- Diagnostics are computed from `station_catalog.csv` and release-side basin fields only.

## Headline

- Station catalog rows: 3,540
- Unresolved rows: 665 (18.79%)
- Records affected by unresolved rows: 534,514
- Resolved stations with point flags requiring review: 101
- High-risk manual review rows emitted: 200

## Status Summary

| basin status | rows | clusters | records | percent rows |
|---|---|---|---|---|
| resolved | 2,861 | 2,849 | 2,312,993 | 80.82% |
| unresolved | 665 | 665 | 534,514 | 18.79% |
|  | 14 | 14 | 40 | 0.40% |

## Flag and Match-Quality Summary

| basin flag | rows | clusters | records | percent rows |
|---|---|---|---|---|
| ok | 2,839 | 2,827 | 2,309,398 | 80.20% |
| large_offset | 476 | 476 | 369,561 | 13.45% |
| area_mismatch | 99 | 99 | 115,999 | 2.80% |
| geometry_inconsistent | 55 | 55 | 14,826 | 1.55% |
| no_match | 35 | 35 | 34,128 | 0.99% |
| reach_product_offset_ok | 22 | 22 | 3,595 | 0.62% |
|  | 14 | 14 | 40 | 0.40% |

## Match Quality

| match quality | rows | clusters | records | percent rows |
|---|---|---|---|---|
| high | 2,839 | 2,827 | 2,309,398 | 80.20% |
| excluded | 665 | 665 | 534,514 | 18.79% |
| moderate | 36 | 36 | 3,635 | 1.02% |

## Unresolved Priority by Source

Prioritize sources with both high unresolved rows and high affected record counts.

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 877 | 239 | 1,651,807 | 423,222 | 27.25% | 25.62% |
| HYDAT | 481 | 88 | 664,497 | 88,547 | 18.30% | 13.33% |
| EUSEDcollab | 216 | 68 | 63,208 | 7,445 | 31.48% | 11.78% |
| HYBAM | 12 | 7 | 11,826 | 6,154 | 58.33% | 52.04% |
| GFQA_v2 | 1,704 | 213 | 69,144 | 5,740 | 12.50% | 8.30% |
| Robotham | 3 | 2 | 3,432 | 2,867 | 66.67% | 83.54% |
| Eurasian_River | 17 | 2 | 3,293 | 265 | 11.76% | 8.05% |
| NERC | 4 | 1 | 624 | 160 | 25% | 25.64% |
| Rhine | 12 | 2 | 312 | 49 | 16.67% | 15.71% |
| GloRiSe | 128 | 41 | 154 | 41 | 32.03% | 26.62% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| Myanmar | 6 | 1 | 6 | 1 | 16.67% | 16.67% |
| Bayern | 32 | 0 | 380,719 | 0 | 0% | 0% |
| Fukushima | 2 | 0 | 3,069 | 0 | 0% | 0% |
| Huanghe | 24 | 0 | 120 | 0 | 0% | 0% |

_Showing first 15 of 18 rows._

## Unresolved Priority by Country

| country | iso a3 | rows | unresolved rows | records | unresolved records | unresolved record percent |
|---|---|---|---|---|---|---|
| United States | USA | 871 | 239 | 1,650,788 | 423,222 | 25.64% |
| Canada | CAN | 478 | 88 | 661,090 | 88,547 | 13.39% |
| Mexico | MEX | 1,696 | 213 | 56,204 | 5,740 | 10.21% |
| Denmark | DNK | 188 | 64 | 14,193 | 4,368 | 30.78% |
| Brazil | BRA | 7 | 4 | 5,169 | 3,067 | 59.33% |
| United Kingdom | GBR | 7 | 3 | 4,056 | 3,027 | 74.63% |
| Spain | ESP | 7 | 1 | 21,402 | 2,961 | 13.84% |
| Peru | PER | 2 | 1 | 3,157 | 2,056 | 65.13% |
| Venezuela | VEN | 1 | 1 | 603 | 603 | 100% |
| Republic of the Congo | COG | 1 | 1 | 428 | 428 | 100% |
| Russia | RUS | 14 | 2 | 2,574 | 265 | 10.30% |
| Belgium | BEL | 6 | 3 | 237 | 116 | 48.95% |
| Germany | DEU | 44 | 2 | 381,031 | 49 | 0.01% |
|  |  | 128 | 41 | 154 | 41 | 26.62% |
| Thailand | THA | 7 | 1 | 348 | 23 | 6.61% |

_Showing first 15 of 34 rows._

## Resolved Point-Flag Anomalies

These rows are resolved but have local/basin point flags that are not fully passing.

| cluster uid | resolution | record count | sources used | country | iso a3 | river name | basin match quality | basin distance m | point in local | point in basin |
|---|---|---|---|---|---|---|---|---|---|---|
| SED000732 | daily | 9,823 | HYDAT | Canada | CAN | SASKATCHEWAN RIVER | distance_only | 161.35 | 0 | 0 |
| SED043817 | daily | 7,374 | USGS | United States | USA |  | area_matched | 520.88 | 0 | 0 |
| SED044138 | daily | 5,027 | USGS | United States | USA |  | distance_only | 0.02 | 0 | 0 |
| SED043588 | daily | 4,854 | USGS | United States | USA |  | area_matched | 432.94 | 0 | 0 |
| SED000404 | daily | 3,532 | HYDAT | Canada | CAN | BIG OTTER CREEK | area_matched | 137.63 | 0 | 0 |
| SED044057 | daily | 3,303 | USGS | United States | USA |  | distance_only | 0.11 | 0 | 0 |
| SED043659 | daily | 3,286 | USGS | United States | USA |  | area_matched | 123.28 | 0 | 0 |
| SED000971 | daily | 2,922 | HYDAT | Canada | CAN | ELK RIVER | distance_only | 104.81 | 0 | 0 |
| SED043875 | daily | 2,696 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED000638 | daily | 2,666 | HYDAT | Canada | CAN | RED DEER RIVER | area_matched | 149.09 | 0 | 1 |
| SED043605 | daily | 2,413 | USGS | United States | USA |  | area_matched | 685.73 | 0 | 0 |
| SED000878 | daily | 1,726 | HYDAT | Canada | CAN | SPRING CREEK (UPPER) | area_approximate | 225.70 | 0 | 1 |
| SED043630 | daily | 1,368 | USGS | United States | USA |  | area_matched | 959.87 | 0 | 1 |
| SED000871 | daily | 1,304 | HYDAT | Canada | CAN |  | area_matched | 106.94 | 0 | 1 |
| SED000881 | daily | 1,236 | HYDAT | Canada | CAN | HORSE CREEK | area_matched | 193.02 | 0 | 0 |
| SED044126 | daily | 1,013 | USGS | United States | USA |  | distance_only | 0.22 | 0 | 0 |

_Showing first 16 of 101 rows._

## Distance Threshold Sensitivity

| distance threshold m | accepted rows | accepted clusters | accepted percent rows |
|---|---|---|---|
| 0 | 0 | 0 | 0% |
| 100 | 2,242 | 2,233 | 63.33% |
| 1,000 | 2,839 | 2,827 | 80.20% |
| 5,000 | 2,861 | 2,849 | 80.82% |
| 10,000 | 2,861 | 2,849 | 80.82% |
| 50,000 | 2,861 | 2,849 | 80.82% |

## Distance Bins

| distance bin | basin status | rows | clusters | records | percent rows |
|---|---|---|---|---|---|
| 0-100 | resolved | 2,242 | 2,233 | 1,932,955 | 63.33% |
| 100-1000 | resolved | 597 | 594 | 376,443 | 16.86% |
| 1000-5000 | unresolved | 306 | 306 | 141,914 | 8.64% |
| 10000-50000 | unresolved | 134 | 134 | 180,410 | 3.79% |
| 5000-10000 | unresolved | 132 | 132 | 155,401 | 3.73% |
| 100-1000 | unresolved | 56 | 56 | 15,173 | 1.58% |
| nan | unresolved | 35 | 35 | 34,128 | 0.99% |
| 1000-5000 | resolved | 22 | 22 | 3,595 | 0.62% |
| nan |  | 14 | 14 | 40 | 0.40% |
| >50000 | unresolved | 2 | 2 | 7,488 | 0.06% |

## Reported-Area Status

| basin status | rows | clusters | records | percent rows |
|---|---|---|---|---|
| resolved | 2,861 | 2,849 | 2,312,993 | 100% |

## Reported-Area Quality

| match quality | rows | clusters | records | percent rows |
|---|---|---|---|---|
| high | 2,839 | 2,827 | 2,309,398 | 99.23% |
| moderate | 22 | 22 | 3,595 | 0.77% |

## Area Error Bins

| area error bin | match quality | rows | clusters | records | percent rows |
|---|---|---|---|---|---|
| reported_area_available | high | 2,839 | 2,827 | 2,309,398 | 80.20% |
| no_reported_area | excluded | 665 | 665 | 534,514 | 18.79% |
| reported_area_available | moderate | 22 | 22 | 3,595 | 0.62% |
| no_reported_area | moderate | 14 | 14 | 40 | 0.40% |

## Manual Review Queue: Large Offsets

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED043988 | daily | 3,841 | USGS | United States |  | 40.91 | -123.82 | unresolved | area_mismatch | 68,634 |  | 0 | 0 | area_mismatch | excluded |
| SED043989 | daily | 3,647 | USGS | United States |  | 41.30 | -124.05 | unresolved | area_mismatch | 68,542 |  | 0 | 0 | area_mismatch | excluded |
| SED000771 | daily | 2 | HYDAT | Canada | PIPESTONE CREEK | 50.04 | -101.68 | unresolved | area_mismatch | 24,840 |  | 0 | 0 | area_mismatch | excluded |
| SED000136 | daily | 1 | GloRiSe |  |  | -3.21 | -59.94 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED043647 | daily | 2,416 | USGS | United States |  | 41.38 | -88.79 | unresolved | large_offset | 19,859 |  | 0 | 0 | large_offset | excluded |
| SED044621 | monthly | 72 | EUSEDcollab | Denmark |  | 56.15 | 10.03 | unresolved | large_offset | 19,829 |  | 0 | 0 | large_offset | excluded |
| SED043435 | daily | 1,434 | USGS | United States |  | 36.60 | -83.75 | unresolved | area_mismatch | 19,389 |  | 0 | 0 | area_mismatch | excluded |
| SED000137 | daily | 1 | GloRiSe |  |  | -3.21 | -59.94 | unresolved | large_offset | 19,305 |  | 0 | 0 | large_offset | excluded |
| SED043278 | daily | 186 | USGS | United States |  | 42.50 | -77.50 | unresolved | area_mismatch | 19,159 |  | 0 | 0 | area_mismatch | excluded |
| SED000178 | daily | 1 | GloRiSe |  |  | -3.23 | -59.01 | unresolved | large_offset | 18,681 |  | 0 | 0 | large_offset | excluded |
| SED000762 | daily | 156 | HYDAT | Canada | LONG CREEK | 49.00 | -103.35 | unresolved | area_mismatch | 18,436 |  | 0 | 0 | area_mismatch | excluded |
| SED043269 | daily | 1,009 | USGS | United States |  | 42.23 | -73.35 | unresolved | large_offset | 18,302 |  | 0 | 0 | large_offset | excluded |

_Showing first 12 of 200 rows._

## Manual Review Queue: Area Mismatch

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000113 | daily | 4 | GFQA_v2 | Mexico |  | 16.04 | -97.75 | unresolved | area_mismatch | 8,151 |  | 0 | 0 | area_mismatch | excluded |
| SED000119 | daily | 1 | GFQA_v2 | Mexico |  | 25.17 | -106.44 | unresolved | area_mismatch | 11,740 |  | 0 | 0 | area_mismatch | excluded |
| SED000135 | daily | 1 | GFQA_v2 | Mexico |  | 15.68 | -96.61 | unresolved | area_mismatch | 14,755 |  | 0 | 0 | area_mismatch | excluded |
| SED000136 | daily | 1 | GloRiSe |  |  | -3.21 | -59.94 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED000140 | daily | 1 | GloRiSe |  |  | -3.21 | -59.94 | unresolved | area_mismatch | 14,881 |  | 0 | 0 | area_mismatch | excluded |
| SED000149 | daily | 1 | GloRiSe |  |  | -3.22 | -59.94 | unresolved | area_mismatch | 3,434 |  | 1 | 1 | area_mismatch | excluded |
| SED000152 | daily | 1 | GloRiSe |  |  | -3.22 | -59.94 | unresolved | area_mismatch | 1,942 |  | 1 | 1 | area_mismatch | excluded |
| SED000161 | daily | 1 | GloRiSe |  |  | -3.07 | -60.27 | unresolved | area_mismatch | 6,211 |  | 0 | 0 | area_mismatch | excluded |
| SED000165 | daily | 1 | GloRiSe |  |  | -3.07 | -60.27 | unresolved | area_mismatch | 1,350 |  | 0 | 0 | area_mismatch | excluded |
| SED000179 | daily | 1 | GloRiSe |  |  | -3.23 | -59.01 | unresolved | area_mismatch | 8,275 |  | 0 | 0 | area_mismatch | excluded |
| SED000233 | daily | 1 | GloRiSe |  |  | 23.77 | 85.86 | unresolved | area_mismatch | 5,102 |  | 0 | 0 | area_mismatch | excluded |
| SED000261 | daily | 2,056 | HYBAM | Peru | Marañon | -4.47 | -77.55 | unresolved | area_mismatch | 3,899 |  | 0 | 0 | area_mismatch | excluded |

_Showing first 12 of 99 rows._

## Manual Review Queue: Geometry Inconsistent

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000024 | annual | 3 | GFQA_v2 | Mexico |  | 17.84 | -92.25 | unresolved | geometry_inconsistent | 492.97 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000028 | annual | 4 | GFQA_v2 | Mexico |  | 17.92 | -102.16 | unresolved | geometry_inconsistent | 792.27 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000032 | annual | 6 | GFQA_v2 | Mexico |  | 15.81 | -95.98 | unresolved | geometry_inconsistent | 359.71 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000094 | daily | 2 | GFQA_v2 | Mexico |  | 21.81 | -102.28 | unresolved | geometry_inconsistent | 746.61 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000102 | daily | 1 | GFQA_v2 | Mexico |  | 18.13 | -94.53 | unresolved | geometry_inconsistent | 609.64 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000110 | daily | 1 | GFQA_v2 | Mexico |  | 32.47 | -116.74 | unresolved | geometry_inconsistent | 829.43 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000858 | daily | 43 | HYDAT | Canada |  | 56.95 | -111.57 | unresolved | geometry_inconsistent | 514.94 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000865 | daily | 8 | HYDAT | Canada | HALFWAY RIVER | 56.23 | -121.48 | unresolved | geometry_inconsistent | 940.65 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000943 | daily | 5,329 | HYDAT | Canada | FRASER RIVER | 49.39 | -121.45 | unresolved | geometry_inconsistent | 633.78 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000963 | daily | 2,253 | HYDAT | Canada |  | 50.27 | -117.00 | unresolved | geometry_inconsistent | 633.78 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000964 | daily | 2,253 | HYDAT | Canada | JOHN CREEK | 50.26 | -117.00 | unresolved | geometry_inconsistent | 390.59 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000972 | daily | 2,831 | HYDAT | Canada | FORDING RIVER | 49.89 | -114.87 | unresolved | geometry_inconsistent | 424.76 |  | 0 | 0 | geometry_inconsistent | excluded |

_Showing first 12 of 55 rows._

## Remote-Sensing Exclusion Summary

| subset | rows | remote sensing rows excluded | note |
|---|---|---|---|
| release_station_catalog | 3,540 | 0 | Release station_catalog excludes satellite validation-only records; see satellite_catalog.csv for validation products. |

## Recommended Follow-Up

- Do not auto-resolve unresolved rows solely from this report; repair high-impact sources first and preserve status/quality fields.
- Review `large_offset`, `area_mismatch`, and geometry-inconsistent queues before publishing basin-sensitive analyses.
- Treat resolved point-flag anomalies as lower-confidence or manually reviewed basin assignments.

## Figures

- `basin_flag_counts.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/basin_flag_counts.png`
- `basin_status_by_reported_area_presence.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/basin_status_by_reported_area_presence.png`
- `distance_hist_logx.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/distance_hist_logx.png`
- `reported_area_presence_counts.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/reported_area_presence_counts.png`
- `spatial_error_class_counts.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/spatial_error_class_counts.png`
- `threshold_sensitivity.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/threshold_sensitivity.png`
- `unknown_points_map.png`: `$WORKSPACE/output_other/stats_release_full/basin_diagnostics/figures/unknown_points_map.png`
