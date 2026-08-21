# Source Dataset Layer Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/source_dataset_layers/tables`
- The report uses release catalogs and candidate sidecars only.

## Headline

- Release-visible source datasets: 21
- Release-visible membership rows: 53,180
- Release-visible attributed records: 22,531,831
- Pipeline-only layers marked unsupported: 4

## Release Layer Summary

| source name | layer | rows | clusters | records | resolutions |
|---|---|---|---|---|---|
| Bayern | main_station_catalog | 37 | 37 | 421,052 | daily |
| Bayern | source_station_catalog | 37 | 37 | 421,052 | daily |
| Chao_Phraya_River | main_station_catalog | 7 | 7 | 348 | annual |
| Chao_Phraya_River | source_station_catalog | 7 | 7 | 348 | annual |
| Dethier | satellite_catalog | 372 | 372 | 133,823 | monthly |
| Eurasian_River | main_station_catalog | 17 | 17 | 3,263 | monthly |
| Eurasian_River | source_station_catalog | 17 | 17 | 3,263 | monthly |
| Fukushima | main_station_catalog | 2 | 2 | 3,069 | daily |
| Fukushima | source_station_catalog | 2 | 2 | 3,069 | daily |
| GFQA_v2 | main_station_catalog | 5,499 | 5,499 | 235,600 | daily |
| GFQA_v2 | source_station_catalog | 5,808 | 5,499 | 185,954 | daily |
| GSED | satellite_catalog | 5,237 | 5,237 | 2,144,599 | monthly |
| GloRiSe | main_station_catalog | 77 | 77 | 649 | daily |
| GloRiSe | source_station_catalog | 77 | 77 | 103 | daily |
| HYBAM | main_station_catalog | 12 | 12 | 9,404 | daily |
| HYBAM | source_station_catalog | 12 | 12 | 9,404 | daily |
| HYDAT | main_station_catalog | 543 | 543 | 676,024 | daily |
| HYDAT | source_station_catalog | 541 | 540 | 671,979 | daily |

_Showing first 18 of 38 rows._

## Source Rollup

| source name | layers | total rows | total clusters | total records |
|---|---|---|---|---|
| RiverSed | satellite_catalog | 32,941 | 32,941 | 14,199,854 |
| USGS | main_station_catalog\|source_station_catalog | 1,779 | 1,778 | 3,375,790 |
| GSED | satellite_catalog | 5,237 | 5,237 | 2,144,599 |
| HYDAT | main_station_catalog\|source_station_catalog | 1,084 | 1,083 | 1,348,003 |
| Bayern | main_station_catalog\|source_station_catalog | 74 | 74 | 842,104 |
| GFQA_v2 | main_station_catalog\|source_station_catalog | 11,307 | 10,998 | 421,554 |
| Dethier | satellite_catalog | 372 | 372 | 133,823 |
| Mekong_Delta | main_station_catalog\|source_station_catalog | 8 | 8 | 23,842 |
| HYBAM | main_station_catalog\|source_station_catalog | 24 | 24 | 18,808 |
| Robotham | main_station_catalog\|source_station_catalog | 6 | 6 | 6,864 |
| Eurasian_River | main_station_catalog\|source_station_catalog | 34 | 34 | 6,526 |
| Fukushima | main_station_catalog\|source_station_catalog | 4 | 4 | 6,138 |
| NERC | main_station_catalog\|source_station_catalog | 8 | 8 | 1,248 |
| GloRiSe | main_station_catalog\|source_station_catalog | 154 | 154 | 752 |
| Chao_Phraya_River | main_station_catalog\|source_station_catalog | 14 | 14 | 696 |
| Rhine | main_station_catalog\|source_station_catalog | 24 | 24 | 624 |
| Shashi_Jianli | main_station_catalog\|source_station_catalog | 4 | 4 | 308 |
| Huanghe | main_station_catalog\|source_station_catalog | 48 | 48 | 240 |

_Showing first 18 of 21 rows._

## Membership Sample

Membership rows are catalog-derived. Multiple source layers can refer to the same cluster, so totals are diagnostic rather than unique release totals.

| source name | layer | resolution | cluster uid | row count | record count |
|---|---|---|---|---|---|
| Bayern | source_station_catalog | daily | SED000034 | 1 | 21,909 |
| Bayern | main_station_catalog | daily | SED000034 | 1 | 21,909 |
| Bayern | source_station_catalog | daily | SED000033 | 1 | 21,906 |
| Bayern | main_station_catalog | daily | SED000033 | 1 | 21,906 |
| Bayern | source_station_catalog | daily | SED000058 | 1 | 21,906 |
| Bayern | main_station_catalog | daily | SED000058 | 1 | 21,906 |
| Bayern | main_station_catalog | daily | SED000035 | 1 | 21,902 |
| Bayern | source_station_catalog | daily | SED000035 | 1 | 21,902 |
| Bayern | source_station_catalog | daily | SED000047 | 1 | 21,899 |
| Bayern | main_station_catalog | daily | SED000047 | 1 | 21,899 |
| Bayern | source_station_catalog | daily | SED000052 | 1 | 21,898 |
| Bayern | main_station_catalog | daily | SED000052 | 1 | 21,898 |
| Bayern | main_station_catalog | daily | SED000031 | 1 | 21,843 |
| Bayern | source_station_catalog | daily | SED000031 | 1 | 21,843 |
| Bayern | source_station_catalog | daily | SED000045 | 1 | 21,250 |
| Bayern | main_station_catalog | daily | SED000045 | 1 | 21,250 |
| Bayern | main_station_catalog | daily | SED000060 | 1 | 21,043 |
| Bayern | source_station_catalog | daily | SED000060 | 1 | 21,043 |

_Showing first 18 of 53,180 rows._

## Unsupported Pipeline Layers

| layer | release only status | reason |
|---|---|---|
| mainline_s3_collected_stations | unsupported_release_only | requires pipeline intermediate file outside release package |
| mainline_s5_clustered_stations | unsupported_release_only | requires pipeline intermediate file outside release package |
| mainline_s6_quality_order_candidates | unsupported_release_only | requires pipeline intermediate file outside release package |
| mainline_s7_source_station_catalog | unsupported_release_only | requires pipeline intermediate file outside release package |

## Interpretation Notes

- Release-only layers are suitable for published package QA and manuscript provenance summaries.
- S3/S5/S6/S7 pipeline-layer counts are not inferred from release files because that would require non-release intermediate outputs.
- Use `parity_manifest.csv` to see the same unsupported status in the legacy-output parity audit.
