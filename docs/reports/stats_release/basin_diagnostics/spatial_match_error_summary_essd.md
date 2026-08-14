# Spatial Match Error Detailed Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/basin_diagnostics/tables`
- Diagnostics are computed from `station_catalog.csv` and release-side basin fields only.
- A station-catalog row is a cluster-resolution row (`cluster_uid` + `resolution`); `unique_cluster_uids` reports deduplicated `cluster_uid` counts where shown.
- `basin_area` is the MERIT-Basins-derived upstream drainage area in the release catalog; it is not a source-reported drainage area.

## Headline

- Station catalog cluster-resolution rows: 7,135
- Unique `cluster_uid` values in station catalog: 7,135
- Unresolved cluster-resolution rows: 1,605 (22.49%)
- Unresolved unique `cluster_uid` values: 1,605
- Records affected by unresolved cluster-resolution rows: 758,512
- Resolved cluster-resolution rows with point flags requiring review: 168
- High-risk manual review cluster-resolution rows emitted: 200

## Status Summary

| basin status | cluster resolution rows | unique cluster uids | records | percent cluster resolution rows |
|---|---|---|---|---|
| resolved | 5,530 | 5,530 | 2,238,609 | 77.51% |
| unresolved | 1,605 | 1,605 | 758,512 | 22.49% |

## Flag and Match-Quality Summary

| basin flag | cluster resolution rows | unique cluster uids | records | percent cluster resolution rows |
|---|---|---|---|---|
| ok | 5,530 | 5,530 | 2,238,609 | 77.51% |
| large_offset | 1,254 | 1,254 | 482,852 | 17.58% |
| geometry_inconsistent | 200 | 200 | 9,292 | 2.80% |
| area_mismatch | 116 | 116 | 165,696 | 1.63% |
| no_match | 35 | 35 | 100,672 | 0.49% |

## Match Quality

| match quality | cluster resolution rows | unique cluster uids | records | percent cluster resolution rows |
|---|---|---|---|---|
| high | 5,530 | 5,530 | 2,238,609 | 77.51% |
| excluded | 1,605 | 1,605 | 758,512 | 22.49% |

## Unresolved Priority by Source

Prioritize sources with both high unresolved cluster-resolution rows and high affected record counts. Source-level row and record counts are non-exclusive because a resolved cluster may contain source stations from more than one dataset; totals across sources should not be summed.

| source name | source cluster resolution rows | unresolved cluster resolution rows | records | unresolved records | unresolved cluster resolution row percent | unresolved record percent |
|---|---|---|---|---|---|---|
| USGS | 889 | 295 | 1,690,433 | 543,790 | 33.18% | 32.17% |
| HYDAT | 543 | 124 | 676,024 | 182,255 | 22.84% | 26.96% |
| GFQA_v2 | 5,499 | 1,160 | 235,600 | 24,906 | 21.09% | 10.57% |
| Robotham | 3 | 3 | 3,432 | 3,432 | 100% | 100% |
| HYBAM | 12 | 4 | 9,404 | 2,392 | 33.33% | 25.44% |
| Eurasian_River | 17 | 6 | 3,263 | 1,239 | 35.29% | 37.97% |
| NERC | 4 | 3 | 624 | 465 | 75% | 74.52% |
| Chao_Phraya_River | 7 | 1 | 348 | 23 | 14.29% | 6.61% |
| GloRiSe | 77 | 8 | 649 | 9 | 10.39% | 1.39% |
| Yajiang | 23 | 1 | 23 | 1 | 4.35% | 4.35% |
| Bayern | 37 | 0 | 421,052 | 0 | 0% | 0% |
| Fukushima | 2 | 0 | 3,069 | 0 | 0% | 0% |
| Huanghe | 24 | 0 | 120 | 0 | 0% | 0% |
| Mekong_Delta | 4 | 0 | 11,921 | 0 | 0% | 0% |
| Myanmar | 5 | 0 | 6 | 0 | 0% | 0% |

_Showing first 15 of 17 rows._

## Unresolved Priority by Country

Country names and ISO codes are release metadata as-is; normalize country aliases and missing ISO A3 values before final publication tables.

| country | iso a3 | cluster resolution rows | unresolved cluster resolution rows | records | unresolved records | unresolved record percent |
|---|---|---|---|---|---|---|
| United States | USA | 886 | 295 | 1,676,280 | 543,790 | 32.44% |
| Canada | CAN | 685 | 147 | 681,792 | 182,673 | 26.79% |
| Mexico | MEX | 3,630 | 907 | 91,854 | 15,769 | 17.17% |
| United Kingdom | GBR | 7 | 6 | 4,056 | 3,897 | 96.08% |
| Netherlands (the) |  | 303 | 111 | 12,561 | 3,763 | 29.96% |
| Brazil | BRA | 7 | 3 | 5,086 | 2,177 | 42.80% |
| Italy | ITA | 404 | 44 | 15,461 | 1,664 | 10.76% |
| Russia | RUS | 17 | 6 | 3,263 | 1,239 | 37.97% |
| Belgium | BEL | 48 | 6 | 3,558 | 533 | 14.98% |
| Serbia | SRB | 37 | 4 | 4,592 | 498 | 10.84% |
| Croatia | HRV | 35 | 4 | 3,130 | 284 | 9.07% |
| United States of America (the) | USA | 9 | 2 | 440 | 250 | 56.82% |
| France | FRA | 105 | 4 | 9,091 | 229 | 2.52% |
| Uruguay | URY | 156 | 11 | 2,501 | 217 | 8.68% |
| Republic of the Congo | COG | 1 | 1 | 215 | 215 | 100% |

_Showing first 15 of 48 rows._

## Resolved Point-Flag Anomalies

These cluster-resolution rows are resolved but have local/basin point flags that are not fully passing.

| cluster uid | resolution | record count | sources used | country | iso a3 | river name | basin match quality | basin distance m | point in local | point in basin |
|---|---|---|---|---|---|---|---|---|---|---|
| SED040498 | daily | 15,248 | USGS | United States | USA |  | area_matched | 397.22 | 0 | 0 |
| SED006764 | daily | 6,009 | HYDAT | Canada | CAN | PEACE RIVER | area_matched | 423.39 | 0 | 0 |
| SED040712 | daily | 5,720 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED006650 | daily | 5,054 | HYDAT | Canada | CAN | SOURIS RIVER | area_matched | 260.39 | 0 | 1 |
| SED006440 | daily | 4,697 | HYDAT | Canada | CAN | OLDMAN RIVER | area_matched | 137.63 | 0 | 0 |
| SED040609 | daily | 4,597 | USGS | United States | USA |  | area_matched | 541.68 | 0 | 0 |
| SED040169 | daily | 3,863 | USGS | United States | USA |  | area_matched | 140.61 | 0 | 0 |
| SED040496 | daily | 3,652 | USGS | United States | USA |  | area_matched | 123.28 | 0 | 0 |
| SED006821 | daily | 3,595 | HYDAT | Canada | CAN | FRASER RIVER | area_matched | 274.68 | 0 | 1 |
| SED006551 | daily | 3,487 | HYDAT | Canada | CAN | SOUTH SASKATCHEWAN RIVER | area_matched | 438.44 | 0 | 1 |
| SED006628 | daily | 2,844 | HYDAT | Canada | CAN | ASSINIBOINE RIVER | area_matched | 354.33 | 0 | 0 |
| SED040652 | daily | 2,092 | USGS | United States | USA |  | area_matched | 520.88 | 0 | 0 |
| SED006556 | daily | 2,009 | HYDAT | Canada | CAN | SWIFT CURRENT CREEK | area_matched | 319.78 | 0 | 0 |
| SED006238 | daily | 1,951 | HYDAT | Canada | CAN | NOTTAWASAGA RIVER | area_matched | 392.69 | 0 | 0 |
| SED040411 | daily | 1,825 | USGS | United States | USA |  | area_matched | 61.66 | 0 | 0 |
| SED006487 | daily | 1,622 | HYDAT | Canada | CAN | ELBOW RIVER | area_matched | 563.25 | 0 | 0 |

_Showing first 16 of 168 rows._

## Resolved Assignment Retention under Stricter Distance Filters

Post-hoc filter applied only to current production-resolved assignments. This does not rerun basin matching and must not be interpreted as maximum-distance threshold sensitivity.

| distance filter m | retained resolved cluster resolution rows | retained resolved unique cluster uids | retained percent of resolved rows | retained percent of catalog rows |
|---|---|---|---|---|
| 0 | 0 | 0 | 0% | 0% |
| 100 | 3,952 | 3,952 | 71.46% | 55.39% |
| 1,000 | 5,530 | 5,530 | 100% | 77.51% |
| 5,000 | 5,530 | 5,530 | 100% | 77.51% |
| 10,000 | 5,530 | 5,530 | 100% | 77.51% |
| 50,000 | 5,530 | 5,530 | 100% | 77.51% |

## Distance Bins

| distance bin | basin status | cluster resolution rows | unique cluster uids | records | percent cluster resolution rows |
|---|---|---|---|---|---|
| 0-100 | resolved | 3,952 | 3,952 | 1,882,163 | 55.39% |
| 100-1000 | resolved | 1,578 | 1,578 | 356,446 | 22.12% |
| 1000-5000 | unresolved | 1,038 | 1,038 | 202,120 | 14.55% |
| 100-1000 | unresolved | 202 | 202 | 10,079 | 2.83% |
| 5000-10000 | unresolved | 190 | 190 | 231,152 | 2.66% |
| 10000-50000 | unresolved | 138 | 138 | 213,761 | 1.93% |
| nan | unresolved | 35 | 35 | 100,672 | 0.49% |
| >50000 | unresolved | 2 | 2 | 728 | 0.03% |

## MERIT Basin-Area Availability

This table reports whether the release catalog has a MERIT-derived `basin_area`. It must not be interpreted as source-reported drainage-area availability; unresolved clusters are designed to have missing `basin_area`.

| has merit basin area | basin status | cluster resolution rows | unique cluster uids | records | percent cluster resolution rows |
|---|---|---|---|---|---|
| 1 | resolved | 5,530 | 5,530 | 2,238,609 | 77.51% |
| 0 | unresolved | 1,605 | 1,605 | 758,512 | 22.49% |

## Manual Review Queue: Largest Spatial Offsets

Rows are sorted by `basin_distance_m`; this queue is not restricted to `basin_flag == large_offset`.

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED040824 | daily | 364 | USGS | United States |  | 18.32 | -64.72 | unresolved | area_mismatch | 68,634 |  | 0 | 0 | area_mismatch | excluded |
| SED040825 | daily | 364 | USGS | United States |  | 18.33 | -64.76 | unresolved | area_mismatch | 68,542 |  | 0 | 0 | area_mismatch | excluded |
| SED006819 | daily | 4,703 | HYDAT | Canada | CARNATION CREEK | 48.92 | -125.00 | unresolved | area_mismatch | 24,840 |  | 0 | 0 | area_mismatch | excluded |
| SED006727 | daily | 10 | HYDAT | Canada | BEAVER RIVER | 57.10 | -111.63 | unresolved | large_offset | 22,549 |  | 0 | 0 | large_offset | excluded |
| SED039991 | daily | 2,099 | USGS | United States |  | 39.77 | -79.61 | unresolved | large_offset | 20,652 |  | 0 | 0 | large_offset | excluded |
| SED006171 | daily | 488 | HYDAT | Canada | EAST BRANCH DUNK RIVER | 46.36 | -63.46 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED040483 | daily | 322 | USGS | United States |  | 38.41 | -106.06 | unresolved | large_offset | 19,859 |  | 0 | 0 | large_offset | excluded |
| SED039986 | daily | 273 | USGS | United States |  | 38.90 | -79.69 | unresolved | large_offset | 19,767 |  | 0 | 0 | large_offset | excluded |
| SED040268 | daily | 8,758 | USGS | United States |  | 42.65 | -88.55 | unresolved | area_mismatch | 19,389 |  | 0 | 0 | area_mismatch | excluded |
| SED006172 | daily | 458 | HYDAT | Canada | DUNK RIVER | 46.35 | -63.49 | unresolved | large_offset | 19,305 |  | 0 | 0 | large_offset | excluded |
| SED040110 | daily | 118 | USGS | United States |  | 36.46 | -84.16 | unresolved | area_mismatch | 19,159 |  | 0 | 0 | area_mismatch | excluded |
| SED039990 | daily | 3,872 | USGS | United States |  | 39.76 | -79.59 | unresolved | area_mismatch | 19,127 |  | 0 | 0 | area_mismatch | excluded |

_Showing first 12 of 200 rows._

## Manual Review Queue: Area Mismatch

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED005992 | daily | 89 | GFQA_v2 | United States of America (the) |  | 30.69 | -91.74 | unresolved | area_mismatch | 181.44 |  | 1 | 1 | area_mismatch | excluded |
| SED006148 | daily | 1,143 | HYDAT | Canada | HOLMES BROOK | 46.61 | -67.61 | unresolved | area_mismatch | 8,151 |  | 0 | 0 | area_mismatch | excluded |
| SED006154 | daily | 2,896 | HYDAT | Canada | NARROWS MOUNTAIN BROOK | 46.28 | -67.02 | unresolved | area_mismatch | 11,740 |  | 0 | 0 | area_mismatch | excluded |
| SED006170 | daily | 2,730 | HYDAT | Canada | EMERALD BROOK | 46.36 | -63.56 | unresolved | area_mismatch | 14,755 |  | 0 | 0 | area_mismatch | excluded |
| SED006171 | daily | 488 | HYDAT | Canada | EAST BRANCH DUNK RIVER | 46.36 | -63.46 | unresolved | area_mismatch | 20,162 |  | 0 | 0 | area_mismatch | excluded |
| SED006176 | daily | 488 | HYDAT | Canada | ELMO RIVER | 46.34 | -63.60 | unresolved | area_mismatch | 14,881 |  | 0 | 0 | area_mismatch | excluded |
| SED006185 | daily | 2,387 | HYDAT | Canada | SHARPE BROOK | 45.03 | -64.64 | unresolved | area_mismatch | 3,434 |  | 1 | 1 | area_mismatch | excluded |
| SED006188 | daily | 3,471 | HYDAT | Canada | FRASER BROOK | 45.34 | -63.17 | unresolved | area_mismatch | 1,942 |  | 1 | 1 | area_mismatch | excluded |
| SED006201 | daily | 3,956 | HYDAT | Canada | APRIL BROOK | 46.23 | -61.14 | unresolved | area_mismatch | 1,350 |  | 0 | 0 | area_mismatch | excluded |
| SED006269 | daily | 5,425 | HYDAT | Canada | O.A.C. FARM GAUGE NO. 5 | 43.53 | -80.31 | unresolved | area_mismatch | 5,102 |  | 0 | 0 | area_mismatch | excluded |
| SED006297 | daily | 1,988 | HYDAT | Canada | STURGEON CREEK | 42.05 | -82.57 | unresolved | area_mismatch | 3,899 |  | 0 | 0 | area_mismatch | excluded |
| SED006335 | daily | 547 | HYDAT | Canada | BROUGHAM CREEK | 43.92 | -79.11 | unresolved | area_mismatch | 4,018 |  | 0 | 0 | area_mismatch | excluded |

_Showing first 12 of 116 rows._

## Manual Review Queue: Geometry Inconsistent

| cluster uid | resolution | record count | sources used | country | river name | lat | lon | basin status | basin flag | basin distance m | basin area | point in local | point in basin | spatial error class | match quality |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED000074 | daily | 73 | GFQA_v2 | Austria |  | 48.37 | 15.78 | unresolved | geometry_inconsistent | 974.49 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000107 | daily | 48 | GFQA_v2 | Belgium |  | 50.96 | 5.13 | unresolved | geometry_inconsistent | 770.12 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000115 | daily | 95 | GFQA_v2 | Belgium |  | 51.33 | 3.23 | unresolved | geometry_inconsistent | 319.71 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000119 | daily | 91 | GFQA_v2 | Belgium |  | 51.32 | 3.23 | unresolved | geometry_inconsistent | 399.75 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000121 | daily | 107 | GFQA_v2 | Belgium |  | 51.22 | 2.95 | unresolved | geometry_inconsistent | 514.20 |  | 0 | 1 | geometry_inconsistent | excluded |
| SED000167 | daily | 159 | GFQA_v2 | Canada |  | 56.02 | -130.07 | unresolved | geometry_inconsistent | 377.27 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000221 | daily | 2 | GFQA_v2 | Canada |  | 44.45 | -65.27 | unresolved | geometry_inconsistent | 561.83 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000273 | daily | 16 | GFQA_v2 | Canada |  | 66.60 | -65.23 | unresolved | geometry_inconsistent | 472.81 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000286 | daily | 89 | GFQA_v2 | Canada |  | 45.87 | -73.28 | unresolved | geometry_inconsistent | 674.30 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000475 | daily | 134 | GFQA_v2 | Croatia |  | 45.33 | 14.46 | unresolved | geometry_inconsistent | 657.54 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000597 | daily | 23 | GFQA_v2 | India |  | 29.76 | 77.13 | unresolved | geometry_inconsistent | 410.27 |  | 0 | 0 | geometry_inconsistent | excluded |
| SED000865 | daily | 74 | GFQA_v2 | Italy |  | 45.40 | 8.82 | unresolved | geometry_inconsistent | 935.87 |  | 0 | 0 | geometry_inconsistent | excluded |

_Showing first 12 of 200 rows._

## Remote-Sensing Exclusion Summary

| subset | cluster resolution rows | remote sensing cluster resolution rows excluded | note |
|---|---|---|---|
| release_station_catalog | 7,135 | 0 | Release station_catalog excludes satellite validation-only records; see satellite_catalog.csv for validation products. |

## Recommended Follow-Up

- Do not auto-resolve unresolved cluster-resolution rows solely from this report; repair high-impact sources first and preserve status/quality fields.
- Review `large_offset`, `area_mismatch`, and geometry-inconsistent queues before publishing basin-sensitive analyses.
- Treat resolved point-flag anomalies as lower-confidence or manually reviewed basin assignments.
- Harmonize source labels with manuscript naming before final tables (for example manuscript-style dataset names rather than raw release labels where needed).

## Figures

- `basin_flag_counts.png`: `output_other/stats_release/basin_diagnostics/figures/basin_flag_counts.png`
- `basin_status_by_merit_basin_area_presence.png`: `output_other/stats_release/basin_diagnostics/figures/basin_status_by_merit_basin_area_presence.png`
- `distance_hist_logx.png`: `output_other/stats_release/basin_diagnostics/figures/distance_hist_logx.png`
- `merit_basin_area_presence_counts.png`: `output_other/stats_release/basin_diagnostics/figures/merit_basin_area_presence_counts.png`
- `resolved_assignment_distance_filter_retention.png`: `output_other/stats_release/basin_diagnostics/figures/resolved_assignment_distance_filter_retention.png`
- `spatial_error_class_counts.png`: `output_other/stats_release/basin_diagnostics/figures/spatial_error_class_counts.png`
- `unknown_points_map.png`: `output_other/stats_release/basin_diagnostics/figures/unknown_points_map.png`
