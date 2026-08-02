# Spatial Match Error Detailed Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/basin_diagnostics/tables`
- Diagnostics are computed from `station_catalog.csv` and release-side basin fields only.

## Headline

- Station catalog rows: 3,771
- Unresolved rows: 793 (21.03%)
- Records affected by unresolved rows: 790,303
- Resolved stations with point flags requiring review: 102
- High-risk manual review rows emitted: 200

## Status Summary

| basin status | rows | clusters | records | percent rows |
|---|---|---|---|---|
| resolved | 2,978 | 2,969 | 2,083,117 | 78.97% |
| unresolved | 793 | 793 | 790,303 | 21.03% |

## Flag and Match-Quality Summary

| basin flag | rows | clusters | records | percent rows |
|---|---|---|---|---|
| ok | 2,978 | 2,969 | 2,083,117 | 78.97% |
| large_offset | 524 | 524 | 488,091 | 13.90% |
| area_mismatch | 186 | 186 | 196,609 | 4.93% |
| geometry_inconsistent | 48 | 48 | 5,586 | 1.27% |
| no_match | 35 | 35 | 100,017 | 0.93% |

## Match Quality

| match quality | rows | clusters | records | percent rows |
|---|---|---|---|---|
| high | 2,978 | 2,969 | 2,083,117 | 78.97% |
| excluded | 793 | 793 | 790,303 | 21.03% |

## Unresolved Priority by Source

Prioritize sources with both high unresolved rows and high affected record counts.

| source name | rows | unresolved rows | records | unresolved records | unresolved row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 887 | 297 | 1,662,326 | 540,624 | 33.48% | 32.52% |
| HYDAT | 505 | 119 | 671,068 | 182,149 | 23.56% | 27.14% |
| EUSEDcollab | 244 | 134 | 66,637 | 54,289 | 54.92% | 81.47% |
| GFQA_v2 | 1,910 | 217 | 56,457 | 5,431 | 11.36% | 9.62% |
| Robotham | 3 | 3 | 3,432 | 3,432 | 100% | 100% |
| HYBAM | 12 | 4 | 11,826 | 2,675 | 33.33% | 22.62% |
| Eurasian_River | 17 | 6 | 3,204 | 1,205 | 35.29% | 37.61% |
| NERC | 4 | 3 | 624 | 465 | 75% | 74.52% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| GloRiSe | 77 | 8 | 103 | 9 | 10.39% | 8.74% |
| Yajiang | 23 | 1 | 23 | 1 | 4.35% | 4.35% |
| Bayern | 34 | 0 | 388,964 | 0 | 0% | 0% |
| Fukushima | 2 | 0 | 3,069 | 0 | 0% | 0% |
| Huanghe | 24 | 0 | 120 | 0 | 0% | 0% |
| Mekong_Delta | 4 | 0 | 11,323 | 0 | 0% | 0% |

_Showing first 15 of 18 rows._

## Unresolved Priority by Country

| country | iso a3 | rows | unresolved rows | records | unresolved records | unresolved record percent |
|---|---|---|---|---|---|---|
| United States | USA | 885 | 297 | 1,655,750 | 540,624 | 32.65% |
| Canada | CAN | 503 | 119 | 664,492 | 182,149 | 27.41% |
| Spain | ESP | 8 | 8 | 26,881 | 26,881 | 100% |
| Poland | POL | 3 | 3 | 13,544 | 13,544 | 100% |
| Denmark | DNK | 211 | 105 | 15,561 | 7,077 | 45.48% |
| Mexico | MEX | 1,909 | 217 | 56,453 | 5,431 | 9.62% |
| United Kingdom | GBR | 7 | 6 | 4,056 | 3,897 | 96.08% |
| Brazil | BRA | 7 | 3 | 5,169 | 2,247 | 43.47% |
| Portugal | PRT | 2 | 2 | 2,065 | 2,065 | 100% |
| Greece | GRC | 5 | 3 | 3,040 | 1,824 | 60% |
| Czech Republic | CZE | 1 | 1 | 1,216 | 1,216 | 100% |
| Russia | RUS | 17 | 6 | 3,204 | 1,205 | 37.61% |
| Slovenia | SVN | 3 | 1 | 3,743 | 1,095 | 29.25% |
| Republic of the Congo | COG | 1 | 1 | 428 | 428 | 100% |
| Belgium | BEL | 6 | 6 | 237 | 237 | 100% |

_Showing first 15 of 29 rows._

## Resolved Point-Flag Anomalies

These rows are resolved but have local/basin point flags that are not fully passing.

| cluster uid | resolution | record count | sources used | country | iso a3 | river name | basin match quality | basin distance m | point in local | point in basin |
|---|---|---|---|---|---|---|---|---|---|---|
| SED034587 | daily | 15,248 | USGS | United States | USA |  | area_matched | 397.22 | 0 | 0 |
| SED000884 | daily | 6,009 | HYDAT | Canada | CAN | PEACE RIVER | area_matched | 423.39 | 0 | 0 |
| SED034801 | daily | 5,720 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED000776 | daily | 5,054 | HYDAT | Canada | CAN | SOURIS RIVER | area_matched | 260.39 | 0 | 1 |
| SED000566 | daily | 4,697 | HYDAT | Canada | CAN | OLDMAN RIVER | area_matched | 137.63 | 0 | 0 |
| SED034700 | daily | 4,597 | USGS | United States | USA |  | area_matched | 541.68 | 0 | 0 |
| SED034262 | daily | 3,863 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED000935 | daily | 3,595 | HYDAT | Canada | CAN | FRASER RIVER | area_matched | 274.68 | 0 | 1 |
| SED000677 | daily | 3,487 | HYDAT | Canada | CAN | SOUTH SASKATCHEWAN RIVER | area_matched | 438.44 | 0 | 1 |
| SED034585 | daily | 3,286 | USGS | United States | USA |  | area_matched | 123.28 | 0 | 0 |
| SED000754 | daily | 2,844 | HYDAT | Canada | CAN | ASSINIBOINE RIVER | area_matched | 354.33 | 0 | 0 |
| SED034743 | daily | 2,092 | USGS | United States | USA |  | area_matched | 520.88 | 0 | 0 |
| SED000682 | daily | 2,009 | HYDAT | Canada | CAN | SWIFT CURRENT CREEK | area_matched | 319.78 | 0 | 0 |
| SED000364 | daily | 1,951 | HYDAT | Canada | CAN | NOTTAWASAGA RIVER | area_matched | 392.69 | 0 | 0 |
| SED001023 | daily | 1,641 | HYDAT | Canada | CAN | MACKENZIE RIVER | area_matched | 488.17 | 0 | 0 |
| SED000613 | daily | 1,622 | HYDAT | Canada | CAN | ELBOW RIVER | area_matched | 563.25 | 0 | 0 |

_Showing first 16 of 102 rows._

## Distance Threshold Sensitivity

| distance threshold m | accepted rows | accepted clusters | accepted percent rows |
|---|---|---|---|
| 0 | 0 | 0 | 0% |
| 100 | 2,380 | 2,375 | 63.11% |
| 1,000 | 2,978 | 2,969 | 78.97% |
| 5,000 | 2,978 | 2,969 | 78.97% |
| 10,000 | 2,978 | 2,969 | 78.97% |
| 50,000 | 2,978 | 2,969 | 78.97% |

## Distance Bins

| distance bin | basin status | rows | clusters | records | percent rows |
|---|---|---|---|---|---|
| 0-100 | resolved | 2,380 | 2,375 | 1,761,367 | 63.11% |
| 100-1000 | resolved | 598 | 594 | 321,750 | 15.86% |
| 1000-5000 | unresolved | 360 | 360 | 202,215 | 9.55% |
| 10000-50000 | unresolved | 178 | 178 | 239,586 | 4.72% |
| 5000-10000 | unresolved | 168 | 168 | 241,413 | 4.46% |
| 100-1000 | unresolved | 49 | 49 | 6,284 | 1.30% |
| nan | unresolved | 35 | 35 | 100,017 | 0.93% |
| >50000 | unresolved | 2 | 2 | 728 | 0.05% |
| 0-100 | unresolved | 1 | 1 | 60 | 0.03% |

## Reported-Area Status

| basin status | rows | clusters | records | percent rows |
|---|---|---|---|---|
| resolved | 2,978 | 2,969 | 2,083,117 | 100% |

## Reported-Area Quality

| match quality | rows | clusters | records | percent rows |
|---|---|---|---|---|
| high | 2,978 | 2,969 | 2,083,117 | 100% |

## Area Error Bins

| area error bin | match quality | rows | clusters | records | percent rows |
|---|---|---|---|---|---|
| reported_area_available | high | 2,978 | 2,969 | 2,083,117 | 78.97% |
| no_reported_area | excluded | 793 | 793 | 790,303 | 21.03% |

## Manual Review Queue: Large Offsets

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED034914 | daily | 364 | USGS | United States |  | 18.32 | -64.72 | unresolved | area_mismatch | 68,634 |  | 0 | 0 | area_mismatch | excluded |
| SED034915 | daily | 364 | USGS | United States |  | 18.33 | -64.76 | unresolved | area_mismatch | 68,542 |  | 0 | 0 | area_mismatch | excluded |
| SED000933 | daily | 4,703 | HYDAT | Canada | CARNATION CREEK | 48.92 | -125.00 | unresolved | area_mismatch | 24,840 |  | 0 | 0 | area_mismatch | excluded |
| SED035377 | monthly | 60 | EUSEDcollab | Denmark |  | 56.00 | 9.85 | unresolved | area_mismatch | 23,831 |  | 0 | 0 | area_mismatch | excluded |
| SED035474 | monthly | 204 | EUSEDcollab | Denmark |  | 55.12 | 10.75 | unresolved | area_mismatch | 23,265 |  | 0 | 0 | area_mismatch | excluded |
| SED000851 | daily | 10 | HYDAT | Canada | BEAVER RIVER | 57.10 | -111.63 | unresolved | large_offset | 22,549 |  | 0 | 0 | large_offset | excluded |
| SED035475 | monthly | 252 | EUSEDcollab | Denmark |  | 55.13 | 10.73 | unresolved | area_mismatch | 22,410 |  | 0 | 0 | area_mismatch | excluded |
| SED034085 | daily | 2,099 | USGS | United States |  | 39.77 | -79.61 | unresolved | large_offset | 20,652 |  | 0 | 0 | large_offset | excluded |
| SED000298 | daily | 488 | HYDAT | Canada | EAST BRANCH DUNK RIVER | 46.36 | -63.46 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED034573 | daily | 322 | USGS | United States |  | 38.41 | -106.06 | unresolved | large_offset | 19,859 |  | 0 | 0 | large_offset | excluded |
| SED035564 | monthly | 9,557 | EUSEDcollab | Poland |  | 49.97 | 20.50 | unresolved | large_offset | 19,829 |  | 0 | 0 | large_offset | excluded |
| SED034080 | daily | 273 | USGS | United States |  | 38.90 | -79.69 | unresolved | large_offset | 19,767 |  | 0 | 0 | large_offset | excluded |

_Showing first 12 of 200 rows._

## Manual Review Queue: Area Mismatch

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000275 | daily | 1,143 | HYDAT | Canada | HOLMES BROOK | 46.61 | -67.61 | unresolved | area_mismatch | 8,151 |  | 0 | 0 | area_mismatch | excluded |
| SED000281 | daily | 2,896 | HYDAT | Canada | NARROWS MOUNTAIN BROOK | 46.28 | -67.02 | unresolved | area_mismatch | 11,740 |  | 0 | 0 | area_mismatch | excluded |
| SED000297 | daily | 2,730 | HYDAT | Canada | EMERALD BROOK | 46.36 | -63.56 | unresolved | area_mismatch | 14,755 |  | 0 | 0 | area_mismatch | excluded |
| SED000298 | daily | 488 | HYDAT | Canada | EAST BRANCH DUNK RIVER | 46.36 | -63.46 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED000302 | daily | 488 | HYDAT | Canada | ELMO RIVER | 46.34 | -63.60 | unresolved | area_mismatch | 14,881 |  | 0 | 0 | area_mismatch | excluded |
| SED000311 | daily | 2,387 | HYDAT | Canada | SHARPE BROOK | 45.03 | -64.64 | unresolved | area_mismatch | 3,434 |  | 1 | 1 | area_mismatch | excluded |
| SED000314 | daily | 3,471 | HYDAT | Canada | FRASER BROOK | 45.34 | -63.17 | unresolved | area_mismatch | 1,942 |  | 1 | 1 | area_mismatch | excluded |
| SED000327 | daily | 3,956 | HYDAT | Canada | APRIL BROOK | 46.23 | -61.14 | unresolved | area_mismatch | 1,350 |  | 0 | 0 | area_mismatch | excluded |
| SED000395 | daily | 5,425 | HYDAT | Canada | O.A.C. FARM GAUGE NO. 5 | 43.53 | -80.31 | unresolved | area_mismatch | 5,102 |  | 0 | 0 | area_mismatch | excluded |
| SED000423 | daily | 1,988 | HYDAT | Canada | STURGEON CREEK | 42.05 | -82.57 | unresolved | area_mismatch | 3,899 |  | 0 | 0 | area_mismatch | excluded |
| SED000461 | daily | 547 | HYDAT | Canada | BROUGHAM CREEK | 43.92 | -79.11 | unresolved | area_mismatch | 4,018 |  | 0 | 0 | area_mismatch | excluded |
| SED000463 | daily | 853 | HYDAT | Canada |  | 43.96 | -79.18 | unresolved | area_mismatch | 8,620 |  | 0 | 0 | area_mismatch | excluded |

_Showing first 12 of 186 rows._

## Manual Review Queue: Geometry Inconsistent

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000024 | annual | 3 | GFQA_v2 | Mexico |  | 17.84 | -92.25 | unresolved | geometry_inconsistent | 492.97 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000028 | annual | 4 | GFQA_v2 | Mexico |  | 17.92 | -102.16 | unresolved | geometry_inconsistent | 792.27 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000032 | annual | 6 | GFQA_v2 | Mexico |  | 15.81 | -95.98 | unresolved | geometry_inconsistent | 359.71 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000094 | daily | 2 | GFQA_v2 | Mexico |  | 21.81 | -102.28 | unresolved | geometry_inconsistent | 746.61 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000112 | daily | 1 | GFQA_v2 | Mexico |  | 32.65 | -115.35 | unresolved | geometry_inconsistent | 895.35 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000122 | daily | 1 | GFQA_v2 | Mexico |  | 17.92 | -102.16 | unresolved | geometry_inconsistent | 815.17 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000135 | daily | 1 | GFQA_v2 | Mexico |  | 15.68 | -96.61 | unresolved | geometry_inconsistent | 645.89 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000264 | daily | 875 | HYBAM | Brazil | Amazon | -3.31 | -60.61 | unresolved | geometry_inconsistent | 609.64 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000272 | daily | 428 | HYBAM | Republic of the Congo | Congo | -4.27 | 15.32 | unresolved | geometry_inconsistent | 829.43 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED001020 | daily | 494 | HYDAT | Canada | CAMPBELL CREEK | 68.27 | -133.26 | unresolved | geometry_inconsistent | 514.94 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED001027 | daily | 97 | HYDAT | Canada |  | 68.26 | -135.05 | unresolved | geometry_inconsistent | 940.65 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED034735 | daily | 2,461 | USGS | United States |  | 38.68 | -121.67 | unresolved | geometry_inconsistent | 481.67 |  | 0 | 0 | geometry_inconsistent | excluded |

_Showing first 12 of 48 rows._

## Remote-Sensing Exclusion Summary

| subset | rows | remote sensing rows excluded | note |
|---|---|---|---|
| release_station_catalog | 3,771 | 0 | Release station_catalog excludes satellite validation-only records; see satellite_catalog.csv for validation products. |

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
