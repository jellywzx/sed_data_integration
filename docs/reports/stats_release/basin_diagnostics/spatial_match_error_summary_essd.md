# Spatial Match Error Detailed Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/basin_diagnostics/tables`
- Diagnostics are computed from `station_catalog.csv` and release-side basin fields only.

## Headline

- Station catalog rows: 7,463
- Unresolved rows: 1,739 (23.30%)
- Records affected by unresolved rows: 812,801
- Resolved stations with point flags requiring review: 179
- High-risk manual review rows emitted: 200

## Status Summary

| basin status | rows | reference stations | records | percent rows |
|---|---|---|---|---|
| resolved | 5,724 | 5,640 | 2,251,870 | 76.70% |
| unresolved | 1,739 | 1,739 | 812,801 | 23.30% |

## Flag and Match-Quality Summary

| basin flag | rows | reference stations | records | percent rows |
|---|---|---|---|---|
| ok | 5,724 | 5,640 | 2,251,870 | 76.70% |
| large_offset | 1,318 | 1,318 | 506,710 | 17.66% |
| geometry_inconsistent | 200 | 200 | 9,292 | 2.68% |
| area_mismatch | 186 | 186 | 196,127 | 2.49% |
| no_match | 35 | 35 | 100,672 | 0.47% |

## Match Quality

| match quality | rows | reference stations | records | percent rows |
|---|---|---|---|---|
| high | 5,724 | 5,640 | 2,251,870 | 76.70% |
| excluded | 1,739 | 1,739 | 812,801 | 23.30% |

## Unresolved Priority by Source

Prioritize sources with both high unresolved rows and high affected record counts.

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 889 | 295 | 1,690,433 | 543,790 | 33.18% | 32.17% |
| HYDAT | 548 | 124 | 676,024 | 182,255 | 22.63% | 26.96% |
| EUSEDcollab | 244 | 134 | 66,637 | 54,289 | 54.92% | 81.47% |
| GFQA_v2 | 5,583 | 1,160 | 236,513 | 24,906 | 20.78% | 10.53% |
| Robotham | 3 | 3 | 3,432 | 3,432 | 100% | 100% |
| HYBAM | 12 | 4 | 9,404 | 2,392 | 33.33% | 25.44% |
| Eurasian_River | 17 | 6 | 3,263 | 1,239 | 35.29% | 37.97% |
| NERC | 4 | 3 | 624 | 465 | 75% | 74.52% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| GloRiSe | 79 | 8 | 649 | 9 | 10.13% | 1.39% |
| Yajiang | 23 | 1 | 23 | 1 | 4.35% | 4.35% |
| Bayern | 37 | 0 | 421,052 | 0 | 0% | 0% |
| Fukushima | 2 | 0 | 3,069 | 0 | 0% | 0% |
| Huanghe | 24 | 0 | 120 | 0 | 0% | 0% |
| Mekong_Delta | 4 | 0 | 11,921 | 0 | 0% | 0% |

_Showing first 15 of 18 rows._

## Unresolved Priority by Country

| country | iso a3 | rows | unresolved rows | records | unresolved records | unresolved record percent |
|---|---|---|---|---|---|---|
| United States | USA | 886 | 295 | 1,676,280 | 543,790 | 32.44% |
| Canada | CAN | 690 | 147 | 681,792 | 182,673 | 26.79% |
| Spain | ESP | 16 | 8 | 27,002 | 26,881 | 99.55% |
| Mexico | MEX | 3,695 | 907 | 92,561 | 15,769 | 17.04% |
| Poland | POL | 27 | 4 | 14,101 | 13,549 | 96.09% |
| Denmark | DNK | 211 | 105 | 15,561 | 7,077 | 45.48% |
| United Kingdom | GBR | 7 | 6 | 4,056 | 3,897 | 96.08% |
| Netherlands (the) |  | 306 | 111 | 12,697 | 3,763 | 29.64% |
| Brazil | BRA | 7 | 3 | 5,086 | 2,177 | 42.80% |
| Portugal | PRT | 3 | 2 | 2,073 | 2,065 | 99.61% |
| Greece | GRC | 38 | 7 | 3,356 | 1,855 | 55.27% |
| Italy | ITA | 408 | 46 | 15,640 | 1,792 | 11.46% |
| Russia | RUS | 17 | 6 | 3,263 | 1,239 | 37.97% |
| Czech Republic | CZE | 1 | 1 | 1,216 | 1,216 | 100% |
| Slovenia | SVN | 6 | 1 | 4,673 | 1,095 | 23.43% |

_Showing first 15 of 49 rows._

## Resolved Point-Flag Anomalies

These rows are resolved but have local/basin point flags that are not fully passing.

| station uid | resolution | record count | sources used | country | iso a3 | river name | basin match quality | basin distance m | point in local | point in basin |
|---|---|---|---|---|---|---|---|---|---|---|
| SED037945 | daily | 15,248 | USGS | United States | USA |  | area_matched | 397.22 | 0 | 0 |
| SED004211 | daily | 6,009 | HYDAT | Canada | CAN | PEACE RIVER | area_matched | 423.39 | 0 | 0 |
| SED038159 | daily | 5,720 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED004097 | daily | 5,054 | HYDAT | Canada | CAN | SOURIS RIVER | area_matched | 260.39 | 0 | 1 |
| SED003887 | daily | 4,697 | HYDAT | Canada | CAN | OLDMAN RIVER | area_matched | 137.63 | 0 | 0 |
| SED038056 | daily | 4,597 | USGS | United States | USA |  | area_matched | 541.68 | 0 | 0 |
| SED037616 | daily | 3,863 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED037943 | daily | 3,652 | USGS | United States | USA |  | area_matched | 123.28 | 0 | 0 |
| SED004268 | daily | 3,595 | HYDAT | Canada | CAN | FRASER RIVER | area_matched | 274.68 | 0 | 1 |
| SED003998 | daily | 3,487 | HYDAT | Canada | CAN | SOUTH SASKATCHEWAN RIVER | area_matched | 438.44 | 0 | 1 |
| SED004075 | daily | 2,844 | HYDAT | Canada | CAN | ASSINIBOINE RIVER | area_matched | 354.33 | 0 | 0 |
| SED038099 | daily | 2,092 | USGS | United States | USA |  | area_matched | 520.88 | 0 | 0 |
| SED004003 | daily | 2,009 | HYDAT | Canada | CAN | SWIFT CURRENT CREEK | area_matched | 319.78 | 0 | 0 |
| SED003685 | daily | 1,951 | HYDAT | Canada | CAN | NOTTAWASAGA RIVER | area_matched | 392.69 | 0 | 0 |
| SED037858 | daily | 1,825 | USGS | United States | USA |  | area_matched | 61.66 | 0 | 0 |
| SED003934 | daily | 1,622 | HYDAT | Canada | CAN | ELBOW RIVER | area_matched | 563.25 | 0 | 0 |

_Showing first 16 of 179 rows._

## Distance Threshold Sensitivity

| distance threshold m | accepted rows | accepted percent rows |
|---|---|---|
| 0 | 0 | 0% |
| 100 | 4,099 | 54.92% |
| 1,000 | 5,724 | 76.70% |
| 5,000 | 5,724 | 76.70% |
| 10,000 | 5,724 | 76.70% |
| 50,000 | 5,724 | 76.70% |

## Distance Bins

| distance bin | basin status | rows | reference stations | records | percent rows |
|---|---|---|---|---|---|
| 0-100 | resolved | 4,099 | 4,049 | 1,894,261 | 54.92% |
| 100-1000 | resolved | 1,625 | 1,591 | 357,609 | 21.77% |
| 1000-5000 | unresolved | 1,083 | 1,083 | 217,412 | 14.51% |
| 5000-10000 | unresolved | 227 | 227 | 244,324 | 3.04% |
| 100-1000 | unresolved | 202 | 202 | 10,079 | 2.71% |
| 10000-50000 | unresolved | 189 | 189 | 239,526 | 2.53% |
| nan | unresolved | 35 | 35 | 100,672 | 0.47% |
| >50000 | unresolved | 2 | 2 | 728 | 0.03% |
| 0-100 | unresolved | 1 | 1 | 60 | 0.01% |

## Reported-Area Status

| basin status | rows | reference stations | records | percent rows |
|---|---|---|---|---|
| resolved | 5,724 | 5,640 | 2,251,870 | 100% |

## Reported-Area Quality

| match quality | rows | reference stations | records | percent rows |
|---|---|---|---|---|
| high | 5,724 | 5,640 | 2,251,870 | 100% |

## Area Error Bins

| area error bin | match quality | rows | reference stations | records | percent rows |
|---|---|---|---|---|---|
| reported_area_available | high | 5,724 | 5,640 | 2,251,870 | 76.70% |
| no_reported_area | excluded | 1,739 | 1,739 | 812,801 | 23.30% |

## Manual Review Queue: Large Offsets

| station uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED038271 | daily | 364 | USGS | United States |  | 18.32 | -64.72 | unresolved | area_mismatch | 68,634 |  | 0 | 0 | area_mismatch | excluded |
| SED038272 | daily | 364 | USGS | United States |  | 18.33 | -64.76 | unresolved | area_mismatch | 68,542 |  | 0 | 0 | area_mismatch | excluded |
| SED004266 | daily | 4,703 | HYDAT | Canada | CARNATION CREEK | 48.92 | -125.00 | unresolved | area_mismatch | 24,840 |  | 0 | 0 | area_mismatch | excluded |
| SED038727 | monthly | 60 | EUSEDcollab | Denmark |  | 56.00 | 9.85 | unresolved | area_mismatch | 23,831 |  | 0 | 0 | area_mismatch | excluded |
| SED038824 | monthly | 204 | EUSEDcollab | Denmark |  | 55.12 | 10.75 | unresolved | area_mismatch | 23,265 |  | 0 | 0 | area_mismatch | excluded |
| SED004174 | daily | 10 | HYDAT | Canada | BEAVER RIVER | 57.10 | -111.63 | unresolved | large_offset | 22,549 |  | 0 | 0 | large_offset | excluded |
| SED038825 | monthly | 252 | EUSEDcollab | Denmark |  | 55.13 | 10.73 | unresolved | area_mismatch | 22,410 |  | 0 | 0 | area_mismatch | excluded |
| SED037438 | daily | 2,099 | USGS | United States |  | 39.77 | -79.61 | unresolved | large_offset | 20,652 |  | 0 | 0 | large_offset | excluded |
| SED003618 | daily | 488 | HYDAT | Canada | EAST BRANCH DUNK RIVER | 46.36 | -63.46 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED037930 | daily | 322 | USGS | United States |  | 38.41 | -106.06 | unresolved | large_offset | 19,859 |  | 0 | 0 | large_offset | excluded |
| SED000111 | daily | 9,557 | EUSEDcollab | Poland |  | 49.97 | 20.50 | unresolved | large_offset | 19,829 |  | 0 | 0 | large_offset | excluded |
| SED037433 | daily | 273 | USGS | United States |  | 38.90 | -79.69 | unresolved | large_offset | 19,767 |  | 0 | 0 | large_offset | excluded |

_Showing first 12 of 200 rows._

## Manual Review Queue: Area Mismatch

| station uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000086 | daily | 12 | EUSEDcollab | Belgium |  | 50.60 | 4.58 | unresolved | area_mismatch | 3,449 |  | 1 | 1 | area_mismatch | excluded |
| SED000087 | daily | 43 | EUSEDcollab | Belgium |  | 50.60 | 4.59 | unresolved | area_mismatch | 2,579 |  | 1 | 1 | area_mismatch | excluded |
| SED000088 | daily | 18 | EUSEDcollab | Belgium |  | 50.61 | 4.59 | unresolved | area_mismatch | 2,162 |  | 1 | 1 | area_mismatch | excluded |
| SED000089 | daily | 55 | EUSEDcollab | Belgium |  | 50.61 | 4.60 | unresolved | area_mismatch | 2,097 |  | 1 | 1 | area_mismatch | excluded |
| SED000090 | daily | 33 | EUSEDcollab | Belgium |  | 50.81 | 4.59 | unresolved | area_mismatch | 17,115 |  | 0 | 0 | area_mismatch | excluded |
| SED000091 | daily | 76 | EUSEDcollab | Belgium |  | 50.84 | 4.63 | unresolved | area_mismatch | 19,023 |  | 0 | 0 | area_mismatch | excluded |
| SED000092 | daily | 1,216 | EUSEDcollab | Czech Republic |  | 49.96 | 14.87 | unresolved | area_mismatch | 14,408 |  | 0 | 0 | area_mismatch | excluded |
| SED000093 | daily | 2,567 | EUSEDcollab | Spain |  | 42.64 | -0.59 | unresolved | area_mismatch | 13,324 |  | 0 | 0 | area_mismatch | excluded |
| SED000094 | daily | 4,703 | EUSEDcollab | Spain |  | 36.92 | -3.49 | unresolved | area_mismatch | 6,094 |  | 0 | 0 | area_mismatch | excluded |
| SED000095 | daily | 2,528 | EUSEDcollab | Spain |  | 42.74 | -1.95 | unresolved | area_mismatch | 1,509 |  | 0 | 0 | area_mismatch | excluded |
| SED000096 | daily | 2,961 | EUSEDcollab | Spain |  | 42.25 | -1.58 | unresolved | area_mismatch | 12,772 |  | 0 | 0 | area_mismatch | excluded |
| SED000097 | daily | 2,412 | EUSEDcollab | Spain |  | 42.78 | -1.44 | unresolved | area_mismatch | 3,901 |  | 0 | 0 | area_mismatch | excluded |

_Showing first 12 of 186 rows._

## Manual Review Queue: Geometry Inconsistent

| station uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000019 | annual | 2 | GFQA_v2 | Mexico |  | 17.98 | -102.38 | unresolved | geometry_inconsistent | 639.80 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000122 | daily | 73 | GFQA_v2 | Austria |  | 48.37 | 15.78 | unresolved | geometry_inconsistent | 974.49 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000133 | daily | 95 | GFQA_v2 | Belgium |  | 51.33 | 3.23 | unresolved | geometry_inconsistent | 319.71 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000135 | daily | 91 | GFQA_v2 | Belgium |  | 51.32 | 3.23 | unresolved | geometry_inconsistent | 399.75 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000137 | daily | 107 | GFQA_v2 | Belgium |  | 51.22 | 2.95 | unresolved | geometry_inconsistent | 514.20 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000168 | daily | 159 | GFQA_v2 | Canada |  | 56.02 | -130.07 | unresolved | geometry_inconsistent | 377.27 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000225 | daily | 16 | GFQA_v2 | Canada |  | 66.60 | -65.23 | unresolved | geometry_inconsistent | 472.81 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000333 | daily | 134 | GFQA_v2 | Croatia |  | 45.33 | 14.46 | unresolved | geometry_inconsistent | 657.54 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000421 | daily | 23 | GFQA_v2 | India |  | 29.76 | 77.13 | unresolved | geometry_inconsistent | 410.27 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000576 | daily | 74 | GFQA_v2 | Italy |  | 45.40 | 8.82 | unresolved | geometry_inconsistent | 935.87 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000577 | daily | 69 | GFQA_v2 | Italy |  | 45.03 | 8.63 | unresolved | geometry_inconsistent | 618.90 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000585 | daily | 76 | GFQA_v2 | Italy |  | 45.41 | 8.21 | unresolved | geometry_inconsistent | 619.27 |  | 0 | 0 | geometry_inconsistent | excluded |

_Showing first 12 of 200 rows._

## Remote-Sensing Exclusion Summary

| subset | rows | remote sensing rows excluded | note |
|---|---|---|---|
| release_station_catalog | 7,463 | 0 | Release station_catalog excludes satellite validation-only records; see satellite_catalog.csv for validation products. |

## Recommended Follow-Up

- Do not auto-resolve unresolved rows solely from this report; repair high-impact sources first and preserve status/quality fields.
- Review `large_offset`, `area_mismatch`, and geometry-inconsistent queues before publishing basin-sensitive analyses.
- Treat resolved point-flag anomalies as lower-confidence or manually reviewed basin assignments.

## Figures

- `basin_flag_counts.png`: `output_other/stats_release/basin_diagnostics/figures/basin_flag_counts.png`
- `basin_status_by_reported_area_presence.png`: `output_other/stats_release/basin_diagnostics/figures/basin_status_by_reported_area_presence.png`
- `distance_hist_logx.png`: `output_other/stats_release/basin_diagnostics/figures/distance_hist_logx.png`
- `reported_area_presence_counts.png`: `output_other/stats_release/basin_diagnostics/figures/reported_area_presence_counts.png`
- `spatial_error_class_counts.png`: `output_other/stats_release/basin_diagnostics/figures/spatial_error_class_counts.png`
- `threshold_sensitivity.png`: `output_other/stats_release/basin_diagnostics/figures/threshold_sensitivity.png`
- `unknown_points_map.png`: `output_other/stats_release/basin_diagnostics/figures/unknown_points_map.png`
