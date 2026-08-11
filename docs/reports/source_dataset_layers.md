# Source Dataset Layer Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/source_dataset_layers/tables`
- The report uses release catalogs and candidate sidecars only.

## Headline

- Release-visible source datasets: 22
- Release-visible membership rows: 53,763
- Release-visible attributed records: 22,666,931
- Pipeline-only layers marked unsupported: 4

## Release Layer Summary

| source name | layer | rows | reference stations | records | resolutions |
|---|---|---|---|---|---|
| Bayern | main_station_catalog | 37 | 37 | 421,052 | daily |
| Bayern | source_station_catalog | 37 | 37 | 421,052 | daily |
| Chao_Phraya_River | main_station_catalog | 7 | 7 | 348 | annual |
| Chao_Phraya_River | source_station_catalog | 7 | 7 | 348 | annual |
| Dethier | satellite_catalog | 372 | 9 | 133,823 | monthly |
| EUSEDcollab | main_station_catalog | 244 | 244 | 66,637 | daily\|monthly |
| EUSEDcollab | source_station_catalog | 244 | 244 | 66,637 | daily\|monthly |
| Eurasian_River | main_station_catalog | 17 | 17 | 3,263 | monthly |
| Eurasian_River | source_station_catalog | 17 | 17 | 3,263 | monthly |
| Fukushima | main_station_catalog | 2 | 2 | 3,069 | daily |
| Fukushima | source_station_catalog | 2 | 2 | 3,069 | daily |
| GFQA_v2 | main_station_catalog | 5,583 | 5,499 | 236,513 | annual\|daily\|monthly |
| GFQA_v2 | source_station_catalog | 5,812 | 5,499 | 186,867 | annual\|daily\|monthly |
| GSED | satellite_catalog | 5,237 | 14 | 2,144,599 | monthly |
| GloRiSe | main_station_catalog | 79 | 77 | 649 | daily\|monthly |
| GloRiSe | source_station_catalog | 77 | 77 | 103 | daily\|monthly |
| HYBAM | main_station_catalog | 12 | 12 | 9,404 | daily |
| HYBAM | source_station_catalog | 12 | 12 | 9,404 | daily |

_Showing first 18 of 40 rows._

## Source Rollup

| source name | layers | total rows | total records |
|---|---|---|---|
| RiverSed | satellite_catalog | 32,941 | 14,199,854 |
| USGS | main_station_catalog\|source_station_catalog | 1,779 | 3,375,790 |
| GSED | satellite_catalog | 5,237 | 2,144,599 |
| HYDAT | main_station_catalog\|source_station_catalog | 1,089 | 1,348,003 |
| Bayern | main_station_catalog\|source_station_catalog | 74 | 842,104 |
| GFQA_v2 | main_station_catalog\|source_station_catalog | 11,395 | 423,380 |
| Dethier | satellite_catalog | 372 | 133,823 |
| EUSEDcollab | main_station_catalog\|source_station_catalog | 488 | 133,274 |
| Mekong_Delta | main_station_catalog\|source_station_catalog | 8 | 23,842 |
| HYBAM | main_station_catalog\|source_station_catalog | 24 | 18,808 |
| Robotham | main_station_catalog\|source_station_catalog | 6 | 6,864 |
| Eurasian_River | main_station_catalog\|source_station_catalog | 34 | 6,526 |
| Fukushima | main_station_catalog\|source_station_catalog | 4 | 6,138 |
| NERC | main_station_catalog\|source_station_catalog | 8 | 1,248 |
| GloRiSe | main_station_catalog\|source_station_catalog | 156 | 752 |
| Chao_Phraya_River | main_station_catalog\|source_station_catalog | 14 | 696 |
| Rhine | main_station_catalog\|source_station_catalog | 24 | 624 |
| Shashi_Jianli | main_station_catalog\|source_station_catalog | 4 | 308 |

_Showing first 18 of 22 rows._

## Membership Sample

Membership rows are catalog-derived. Multiple source layers can refer to the same reference station, so totals are diagnostic rather than unique release totals.

| source name | layer | resolution | station uid | row count | record count |
|---|---|---|---|---|---|
| Bayern | main_station_catalog | daily | SED000052 | 1 | 21,909 |
| Bayern | source_station_catalog | daily | SED000052 | 1 | 21,909 |
| Bayern | source_station_catalog | daily | SED000051 | 1 | 21,906 |
| Bayern | source_station_catalog | daily | SED000076 | 1 | 21,906 |
| Bayern | main_station_catalog | daily | SED000076 | 1 | 21,906 |
| Bayern | main_station_catalog | daily | SED000051 | 1 | 21,906 |
| Bayern | source_station_catalog | daily | SED000053 | 1 | 21,902 |
| Bayern | main_station_catalog | daily | SED000053 | 1 | 21,902 |
| Bayern | main_station_catalog | daily | SED000065 | 1 | 21,899 |
| Bayern | source_station_catalog | daily | SED000065 | 1 | 21,899 |
| Bayern | source_station_catalog | daily | SED000070 | 1 | 21,898 |
| Bayern | main_station_catalog | daily | SED000070 | 1 | 21,898 |
| Bayern | main_station_catalog | daily | SED000049 | 1 | 21,843 |
| Bayern | source_station_catalog | daily | SED000049 | 1 | 21,843 |
| Bayern | source_station_catalog | daily | SED000063 | 1 | 21,250 |
| Bayern | main_station_catalog | daily | SED000063 | 1 | 21,250 |
| Bayern | main_station_catalog | daily | SED000078 | 1 | 21,043 |
| Bayern | source_station_catalog | daily | SED000078 | 1 | 21,043 |

_Showing first 18 of 53,763 rows._

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
