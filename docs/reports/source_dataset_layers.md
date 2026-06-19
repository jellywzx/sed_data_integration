# Source Dataset Layer Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/source_dataset_layers/tables`
- The report uses release catalogs and candidate sidecars only.

## Headline

- Release-visible source datasets: 24
- Release-visible membership rows: 55,294
- Release-visible attributed records: 22,218,067
- Pipeline-only layers marked unsupported: 4

## Release Layer Summary

| source name | layer | rows | clusters | records | resolutions |
|---|---|---|---|---|---|
| Bayern | main_station_catalog | 32 | 32 | 380,719 | daily |
| Bayern | source_station_catalog | 34 | 32 | 380,719 | daily |
| Chao_Phraya_River | main_station_catalog | 7 | 7 | 348 | annual |
| Chao_Phraya_River | source_station_catalog | 7 | 7 | 348 | annual |
| Dethier | satellite_catalog | 371 | 371 | 133,379 | monthly |
| EUSEDcollab | main_station_catalog | 216 | 216 | 63,208 | monthly |
| EUSEDcollab | source_station_catalog | 226 | 216 | 63,208 | monthly |
| Eurasian_River | main_station_catalog | 17 | 17 | 3,293 | monthly |
| Eurasian_River | source_station_catalog | 17 | 17 | 3,204 | monthly |
| Fukushima | main_station_catalog | 2 | 2 | 3,069 | daily |
| Fukushima | source_station_catalog | 2 | 2 | 3,069 | daily |
| GFQA_v2 | main_station_catalog | 1,704 | 1,692 | 69,144 | annual\|daily\|monthly |
| GFQA_v2 | source_station_catalog | 2,062 | 1,692 | 56,297 | annual\|daily\|monthly |
| GSED | satellite_catalog | 5,237 | 5,237 | 2,144,599 | monthly |
| GloRiSe | main_station_catalog | 128 | 128 | 154 | daily\|monthly |
| GloRiSe | source_station_catalog | 128 | 128 | 154 | daily\|monthly |
| HYBAM | main_station_catalog | 12 | 12 | 11,826 | daily |
| HYBAM | source_station_catalog | 12 | 12 | 11,826 | daily |

_Showing first 18 of 42 rows._

## Source Rollup

| source name | layers | total rows | total clusters | total records |
|---|---|---|---|---|
| RiverSed | satellite_catalog | 42,177 | 42,177 | 14,228,483 |
| USGS | main_station_catalog\|source_station_catalog | 1,762 | 1,746 | 3,303,397 |
| GSED | satellite_catalog | 5,237 | 5,237 | 2,144,599 |
| HYDAT | main_station_catalog\|source_station_catalog | 982 | 961 | 1,325,635 |
| Bayern | main_station_catalog\|source_station_catalog | 66 | 64 | 761,438 |
| Dethier | satellite_catalog | 371 | 371 | 133,379 |
| EUSEDcollab | main_station_catalog\|source_station_catalog | 442 | 432 | 126,416 |
| GFQA_v2 | main_station_catalog\|source_station_catalog | 3,766 | 3,384 | 125,441 |
| HYBAM | main_station_catalog\|source_station_catalog | 24 | 24 | 23,652 |
| Mekong_Delta | main_station_catalog\|source_station_catalog | 8 | 8 | 22,646 |
| Robotham | main_station_catalog\|source_station_catalog | 6 | 6 | 6,864 |
| Eurasian_River | main_station_catalog\|source_station_catalog | 34 | 34 | 6,497 |
| Fukushima | main_station_catalog\|source_station_catalog | 4 | 4 | 6,138 |
| NERC | main_station_catalog\|source_station_catalog | 8 | 8 | 1,248 |
| Chao_Phraya_River | main_station_catalog\|source_station_catalog | 14 | 14 | 696 |
| Rhine | main_station_catalog\|source_station_catalog | 24 | 24 | 624 |
| GloRiSe | main_station_catalog\|source_station_catalog | 256 | 256 | 308 |
| Shashi_Jianli | main_station_catalog\|source_station_catalog | 4 | 4 | 308 |

_Showing first 18 of 24 rows._

## Membership Sample

Membership rows are catalog-derived. Multiple source layers can refer to the same cluster, so totals are diagnostic rather than unique release totals.

| source name | layer | resolution | cluster uid | row count | record count |
|---|---|---|---|---|---|
| Bayern | main_station_catalog | daily | SED000061 | 1 | 21,909 |
| Bayern | source_station_catalog | daily | SED000061 | 1 | 21,909 |
| Bayern | source_station_catalog | daily | SED000082 | 1 | 21,906 |
| Bayern | main_station_catalog | daily | SED000082 | 1 | 21,906 |
| Bayern | source_station_catalog | daily | SED000060 | 1 | 21,904 |
| Bayern | main_station_catalog | daily | SED000060 | 1 | 21,904 |
| Bayern | source_station_catalog | daily | SED000072 | 1 | 21,899 |
| Bayern | main_station_catalog | daily | SED000072 | 1 | 21,899 |
| Bayern | source_station_catalog | daily | SED000077 | 1 | 21,898 |
| Bayern | main_station_catalog | daily | SED000077 | 1 | 21,898 |
| Bayern | main_station_catalog | daily | SED000058 | 1 | 21,843 |
| Bayern | source_station_catalog | daily | SED000058 | 1 | 21,843 |
| Bayern | main_station_catalog | daily | SED000070 | 1 | 21,250 |
| Bayern | source_station_catalog | daily | SED000070 | 1 | 21,250 |
| Bayern | source_station_catalog | daily | SED000084 | 1 | 21,043 |
| Bayern | main_station_catalog | daily | SED000084 | 1 | 21,043 |
| Bayern | source_station_catalog | daily | SED000076 | 1 | 20,810 |
| Bayern | main_station_catalog | daily | SED000076 | 1 | 20,810 |

_Showing first 18 of 55,294 rows._

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
