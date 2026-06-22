# S8 Source Contribution Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/source_contribution/tables`
- Source contribution uses release catalogs and release NetCDF provenance only.
- **Dual-track reporting**: main in-situ/reference sources are reported separately from satellite validation sources.

## Counting Policy

- `record_attributed_record_count` is source-station based and avoids multi-source cluster over-counting.
- `cluster_attributed_record_count` preserves the historical exploded cluster attribution for parity with older reports.
- Cluster counts can sum above unique release clusters because multiple sources can contribute to the same reference cluster.
- Satellite percentages throughout this report are computed against satellite-only totals, not merged totals.

## Key Metrics (Main Track — In-Situ / Reference / Climatology)

- Source datasets: 17
- Source stations: 3,950
- Source-summed clusters: 3,543
- Total attributed records: 2,847,393
- Top source by records: `USGS`
- Over-attribution records in source summary: 16,512

| metric | value | detail |
|---|---|---|
| total_source_datasets | 17 |  |
| total_source_stations | 3,950 |  |
| total_clusters_source_sum | 3,543 |  |
| total_records | 2,847,393 |  |
| total_Q_records | 2,847,273 |  |
| total_SSC_records | 2,847,393 |  |
| total_SSL_records | 2,466,554 |  |
| top_source_by_records | USGS | 58.00% |
| earliest_year | 1,912 |  |
| latest_year | 2,025 |  |

## Main Source Contribution (In-Situ / Reference / Climatology)

Primary contribution table. This track excludes satellite-derived sources (RiverSed, GSED, Dethier, Shashi_Jianli) which are reported separately below.

| source name | source type | source group | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | first year | last year | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | in-situ | national agencies | 885 | 873 | 1,651,590 | 1,651,590 | 1,651,590 | 1,651,590 | 1,980 | 2,024 | daily | 58.00% |
| HYDAT | in-situ | national agencies | 501 | 480 | 661,138 | 661,138 | 661,138 | 661,138 | 1,948 | 1,997 | daily | 23.22% |
| Bayern | in-situ | national agencies | 34 | 32 | 380,719 | 380,719 | 380,719 | 0 | 1,965 | 2,025 | daily | 13.37% |
| EUSEDcollab | literature | global compilations | 226 | 216 | 63,208 | 63,208 | 63,208 | 63,208 | 1,987 | 2,021 | monthly | 2.22% |
| GFQA_v2 | literature | global compilations | 2,062 | 1,700 | 56,297 | 56,297 | 56,297 | 56,297 | 1,995 | 2,021 | annual\|daily\|monthly | 1.98% |
| HYBAM | in-situ | regional datasets | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | 1,994 | 2,024 | daily | 0.42% |
| Mekong_Delta | literature | global compilations | 4 | 4 | 11,323 | 11,323 | 11,323 | 11,323 | 2,005 | 2,012 | daily | 0.40% |
| Robotham | literature | global compilations | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 2,016 | 2,021 | daily | 0.12% |
| Eurasian_River | literature | global compilations | 17 | 17 | 3,204 | 3,204 | 3,204 | 3,204 | 1,938 | 2,000 | monthly | 0.11% |
| Fukushima | literature | global compilations | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 2,012 | 2,018 | daily | 0.11% |
| NERC | literature | global compilations | 4 | 4 | 624 | 624 | 624 | 624 | 2,013 | 2,014 | daily | 0.02% |
| Chao_Phraya_River | literature | global compilations | 7 | 7 | 348 | 348 | 348 | 348 | 1,912 | 2,020 | annual | 0.01% |
| Rhine | literature | global compilations | 12 | 12 | 312 | 312 | 312 | 312 | 1,990 | 2,011 | daily | 0.01% |
| GloRiSe | literature | global compilations | 128 | 128 | 154 | 154 | 154 | 154 | 1,979 | 2,012 | daily\|monthly | 0.01% |
| Huanghe | literature | global compilations | 24 | 24 | 120 | 0 | 120 | 0 | 2,015 | 2,019 | annual | 0.00% |

_Showing first 15 of 17 rows._

## Main Source Contribution by Type

| summary level | category | n source datasets | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_type | in-situ | 4 | 1,432 | 1,397 | 2,705,273 | 2,705,273 | 2,705,273 | 2,324,554 | daily | 95.01% |
| source_group | national agencies | 3 | 1,420 | 1,385 | 2,693,447 | 2,693,447 | 2,693,447 | 2,312,728 | daily | 94.59% |
| source_group | global compilations | 13 | 2,518 | 2,146 | 142,120 | 142,000 | 142,120 | 142,000 | annual\|daily\|monthly | 4.99% |
| source_type | literature | 13 | 2,518 | 2,146 | 142,120 | 142,000 | 142,120 | 142,000 | annual\|daily\|monthly | 4.99% |
| source_group | regional datasets | 1 | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | daily | 0.42% |

## Main Source by Resolution

| source name | product | resolution | source type | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | main | daily | in-situ | 885 | 873 | 1,651,590 | 1,651,590 | 1,651,590 | 1,651,590 | 58.00% | 100% |
| HYDAT | main | daily | in-situ | 501 | 480 | 661,138 | 661,138 | 661,138 | 661,138 | 23.22% | 100% |
| Bayern | main | daily | in-situ | 34 | 32 | 380,719 | 380,719 | 380,719 | 0 | 13.37% | 100% |
| EUSEDcollab | main | monthly | literature | 226 | 216 | 63,208 | 63,208 | 63,208 | 63,208 | 2.22% | 100% |
| GFQA_v2 | main | monthly | literature | 1,993 | 1,631 | 56,094 | 56,094 | 56,094 | 56,094 | 1.97% | 99.64% |
| HYBAM | main | daily | in-situ | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | 0.42% | 100% |
| Mekong_Delta | main | daily | literature | 4 | 4 | 11,323 | 11,323 | 11,323 | 11,323 | 0.40% | 100% |
| Robotham | main | daily | literature | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.12% | 100% |
| Eurasian_River | main | monthly | literature | 17 | 17 | 3,204 | 3,204 | 3,204 | 3,204 | 0.11% | 100% |
| Fukushima | main | daily | literature | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.11% | 100% |
| NERC | main | daily | literature | 4 | 4 | 624 | 624 | 624 | 624 | 0.02% | 100% |
| Chao_Phraya_River | main | annual | literature | 7 | 7 | 348 | 348 | 348 | 348 | 0.01% | 100% |
| Rhine | main | daily | literature | 12 | 12 | 312 | 312 | 312 | 312 | 0.01% | 100% |
| GFQA_v2 | main | annual | literature | 27 | 27 | 151 | 151 | 151 | 151 | 0.01% | 0.27% |
| Huanghe | main | annual | literature | 24 | 24 | 120 | 0 | 120 | 0 | 0.00% | 100% |
| GloRiSe | main | daily | literature | 114 | 114 | 114 | 114 | 114 | 114 | 0.00% | 74.03% |
| GFQA_v2 | main | daily | literature | 42 | 42 | 52 | 52 | 52 | 52 | 0.00% | 0.09% |
| GloRiSe | main | monthly | literature | 14 | 14 | 40 | 40 | 40 | 40 | 0.00% | 25.97% |

_Showing first 18 of 20 rows._

## Catalog Attribution Cross-Check

This table separates unique source-station attribution from cluster-exploded attribution.

| source name | n source stations | n clusters | available resolutions | main record count | record attributed record count | cluster attributed record count | over attribution record count |
|---|---|---|---|---|---|---|---|
| USGS | 885 | 873 | daily | 1,651,807 | 1,651,590 | 1,651,807 | 217 |
| HYDAT | 501 | 480 | daily | 664,497 | 661,138 | 664,497 | 3,359 |
| Bayern | 34 | 32 | daily | 380,719 | 380,719 | 380,719 | 0 |
| EUSEDcollab | 226 | 216 | monthly | 63,208 | 63,208 | 63,208 | 0 |
| GFQA_v2 | 2,062 | 1,692 | annual\|daily\|monthly | 69,144 | 56,297 | 69,144 | 12,847 |
| HYBAM | 12 | 12 | daily | 11,826 | 11,826 | 11,826 | 0 |
| Mekong_Delta | 4 | 4 | daily | 11,323 | 11,323 | 11,323 | 0 |
| Robotham | 3 | 3 | daily | 3,432 | 3,432 | 3,432 | 0 |
| Eurasian_River | 17 | 17 | monthly | 3,293 | 3,204 | 3,293 | 89 |
| Fukushima | 2 | 2 | daily | 3,069 | 3,069 | 3,069 | 0 |
| NERC | 4 | 4 | daily | 624 | 624 | 624 | 0 |
| Chao_Phraya_River | 7 | 7 | annual | 348 | 348 | 348 | 0 |
| Rhine | 12 | 12 | daily | 312 | 312 | 312 | 0 |
| GloRiSe | 128 | 128 | daily\|monthly | 154 | 154 | 154 | 0 |
| Shashi_Jianli | 2 | 2 | daily | 154 | 154 | 154 | 0 |

_Showing first 15 of 18 rows._

---

## Satellite Validation Contribution (Validation-Only Sidecar)

The satellite product concatenates records from multiple independent satellite-derived sources.
These sources are **not** equivalent to in-situ/reference data: their Q and SSL coverage is
typically zero or near-zero, and SSC values are derived from satellite algorithms, not direct
field measurements.  Percentages below are relative to satellite-only totals.

**Do not** merge satellite percentages with the main-track percentages above for
manuscript contribution claims.  See the variable coverage report (variable_summary)
for a detailed sparsity analysis of each satellite source.

## Satellite Source Datasets

| source name | source type | source group | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | first year | last year | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | satellite products | 42,177 | 42,177 | 14,228,483 | 0 | 0 | 0 | 1,984 | 2,019 | daily | 86.20% |
| GSED | satellite | satellite products | 5,237 | 5,237 | 2,144,599 | 0 | 2,144,599 | 0 | 1,985 | 2,020 | monthly | 12.99% |
| Dethier | satellite | satellite products | 371 | 371 | 133,379 | 133,379 | 133,379 | 133,379 | 1,984 | 2,020 | monthly | 0.81% |
| Shashi_Jianli | satellite | satellite products | 2 | 2 | 154 | 154 | 154 | 154 | 2,016 | 2,023 | daily | 0.00% |

## Satellite Source-Resolution Contribution (CSV catalog)

Satellite products remain validation-sidecar contributions and should be interpreted with variable coverage. Q/SSL are often entirely absent.

| source name | resolution | satellite station count | satellite cluster count | satellite record count |
|---|---|---|---|---|
| Dethier | monthly | 371 | 371 | 133,379 |
| GSED | monthly | 5,237 | 5,237 | 2,144,599 |
| RiverSed | daily | 42,177 | 42,177 | 14,228,483 |

---

## Legacy Merged Contribution (All Sources Combined)

The following sections merge all sources (main + satellite) into a single combined
framework for backward compatibility with earlier report versions.  **These combined
percentages mix satellite validation records with in-situ/reference data and may
overstate the contribution of satellite sources that dominate by record count but
contribute little usable Q/SSC/SSL data.**  For manuscript contribution claims,
refer to the main-track tables above.

## Contribution Concentration (Combined)

| rank | source name | source type | source group | n records | cumulative records | cumulative percent |
|---|---|---|---|---|---|---|
| 1 | RiverSed | satellite | satellite products | 14,228,483 | 14,228,483 | 73.52% |
| 2 | GSED | satellite | satellite products | 2,144,599 | 16,373,082 | 84.60% |
| 3 | USGS | in-situ | national agencies | 1,651,590 | 18,024,672 | 93.13% |
| 4 | HYDAT | in-situ | national agencies | 661,138 | 18,685,810 | 96.55% |
| 5 | Bayern | in-situ | national agencies | 380,719 | 19,066,529 | 98.51% |
| 6 | Dethier | satellite | satellite products | 133,379 | 19,199,908 | 99.20% |
| 7 | EUSEDcollab | literature | global compilations | 63,208 | 19,263,116 | 99.53% |
| 8 | GFQA_v2 | literature | global compilations | 56,297 | 19,319,413 | 99.82% |
| 9 | HYBAM | in-situ | regional datasets | 11,826 | 19,331,239 | 99.88% |
| 10 | Mekong_Delta | literature | global compilations | 11,323 | 19,342,562 | 99.94% |
| 11 | Robotham | literature | global compilations | 3,432 | 19,345,994 | 99.96% |
| 12 | Eurasian_River | literature | global compilations | 3,204 | 19,349,198 | 99.98% |
| 13 | Fukushima | literature | global compilations | 3,069 | 19,352,267 | 99.99% |
| 14 | NERC | literature | global compilations | 624 | 19,352,891 | 99.99% |
| 15 | Chao_Phraya_River | literature | global compilations | 348 | 19,353,239 | 100.00% |

_Showing first 15 of 21 rows._

## Contribution by Source Type and Group (Combined)

| summary level | category | n source datasets | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_group | satellite products | 4 | 47,787 | 47,787 | 16,506,615 | 133,533 | 2,278,132 | 133,533 | daily\|monthly | 85.29% |
| source_type | satellite | 4 | 47,787 | 47,787 | 16,506,615 | 133,533 | 2,278,132 | 133,533 | daily\|monthly | 85.29% |
| source_type | in-situ | 4 | 1,432 | 1,397 | 2,705,273 | 2,705,273 | 2,705,273 | 2,324,554 | daily | 13.98% |
| source_group | national agencies | 3 | 1,420 | 1,385 | 2,693,447 | 2,693,447 | 2,693,447 | 2,312,728 | daily | 13.92% |
| source_group | global compilations | 13 | 2,518 | 2,146 | 142,120 | 142,000 | 142,120 | 142,000 | annual\|daily\|monthly | 0.73% |
| source_type | literature | 13 | 2,518 | 2,146 | 142,120 | 142,000 | 142,120 | 142,000 | annual\|daily\|monthly | 0.73% |
| source_group | regional datasets | 1 | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | daily | 0.06% |

## Source by Resolution (Combined)

| source name | product | resolution | source type | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | daily | satellite | 42,177 | 42,177 | 14,228,483 | 0 | 0 | 0 | 73.52% | 100% |
| GSED | satellite | monthly | satellite | 5,237 | 5,237 | 2,144,599 | 0 | 2,144,599 | 0 | 11.08% | 100% |
| USGS | main | daily | in-situ | 885 | 873 | 1,651,590 | 1,651,590 | 1,651,590 | 1,651,590 | 8.53% | 100% |
| HYDAT | main | daily | in-situ | 501 | 480 | 661,138 | 661,138 | 661,138 | 661,138 | 3.42% | 100% |
| Bayern | main | daily | in-situ | 34 | 32 | 380,719 | 380,719 | 380,719 | 0 | 1.97% | 100% |
| Dethier | satellite | monthly | satellite | 371 | 371 | 133,379 | 133,379 | 133,379 | 133,379 | 0.69% | 100% |
| EUSEDcollab | main | monthly | literature | 226 | 216 | 63,208 | 63,208 | 63,208 | 63,208 | 0.33% | 100% |
| GFQA_v2 | main | monthly | literature | 1,993 | 1,631 | 56,094 | 56,094 | 56,094 | 56,094 | 0.29% | 99.64% |
| HYBAM | main | daily | in-situ | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | 0.06% | 100% |
| Mekong_Delta | main | daily | literature | 4 | 4 | 11,323 | 11,323 | 11,323 | 11,323 | 0.06% | 100% |
| Robotham | main | daily | literature | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.02% | 100% |
| Eurasian_River | main | monthly | literature | 17 | 17 | 3,204 | 3,204 | 3,204 | 3,204 | 0.02% | 100% |
| Fukushima | main | daily | literature | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.02% | 100% |
| NERC | main | daily | literature | 4 | 4 | 624 | 624 | 624 | 624 | 0.00% | 100% |
| Chao_Phraya_River | main | annual | literature | 7 | 7 | 348 | 348 | 348 | 348 | 0.00% | 100% |
| Rhine | main | daily | literature | 12 | 12 | 312 | 312 | 312 | 312 | 0.00% | 100% |
| Shashi_Jianli | main | daily | satellite | 2 | 2 | 154 | 154 | 154 | 154 | 0.00% | 100% |
| GFQA_v2 | main | annual | literature | 27 | 27 | 151 | 151 | 151 | 151 | 0.00% | 0.27% |

_Showing first 18 of 24 rows._

## Source by Variable (Combined)

| source name | source type | source group | variable | n variable records | n source records | percentage of total variable records | percentage within source records |
|---|---|---|---|---|---|---|---|
| GSED | satellite | satellite products | SSC | 2,144,599 | 2,144,599 | 41.84% | 100% |
| USGS | in-situ | national agencies | Q | 1,651,590 | 1,651,590 | 55.41% | 100% |
| USGS | in-situ | national agencies | SSC | 1,651,590 | 1,651,590 | 32.22% | 100% |
| USGS | in-situ | national agencies | SSL | 1,651,590 | 1,651,590 | 63.52% | 100% |
| HYDAT | in-situ | national agencies | Q | 661,138 | 661,138 | 22.18% | 100% |
| HYDAT | in-situ | national agencies | SSC | 661,138 | 661,138 | 12.90% | 100% |
| HYDAT | in-situ | national agencies | SSL | 661,138 | 661,138 | 25.43% | 100% |
| Bayern | in-situ | national agencies | SSC | 380,719 | 380,719 | 7.43% | 100% |
| Bayern | in-situ | national agencies | Q | 380,719 | 380,719 | 12.77% | 100% |
| Dethier | satellite | satellite products | SSC | 133,379 | 133,379 | 2.60% | 100% |
| Dethier | satellite | satellite products | Q | 133,379 | 133,379 | 4.47% | 100% |
| Dethier | satellite | satellite products | SSL | 133,379 | 133,379 | 5.13% | 100% |
| EUSEDcollab | literature | global compilations | Q | 63,208 | 63,208 | 2.12% | 100% |
| EUSEDcollab | literature | global compilations | SSL | 63,208 | 63,208 | 2.43% | 100% |
| EUSEDcollab | literature | global compilations | SSC | 63,208 | 63,208 | 1.23% | 100% |
| GFQA_v2 | literature | global compilations | SSL | 56,297 | 56,297 | 2.17% | 100% |
| GFQA_v2 | literature | global compilations | SSC | 56,297 | 56,297 | 1.10% | 100% |
| GFQA_v2 | literature | global compilations | Q | 56,297 | 56,297 | 1.89% | 100% |

_Showing first 18 of 63 rows._

## Temporal Span by Source (Combined)

| source name | source type | source group | first year | last year | year span | n records | n source stations | n clusters | resolutions |
|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | satellite products | 1,984 | 2,019 | 36 | 14,228,483 | 42,177 | 42,177 | daily |
| GSED | satellite | satellite products | 1,985 | 2,020 | 36 | 2,144,599 | 5,237 | 5,237 | monthly |
| USGS | in-situ | national agencies | 1,980 | 2,024 | 45 | 1,651,590 | 885 | 873 | daily |
| HYDAT | in-situ | national agencies | 1,948 | 1,997 | 50 | 661,138 | 501 | 480 | daily |
| Bayern | in-situ | national agencies | 1,965 | 2,025 | 61 | 380,719 | 34 | 32 | daily |
| Dethier | satellite | satellite products | 1,984 | 2,020 | 37 | 133,379 | 371 | 371 | monthly |
| EUSEDcollab | literature | global compilations | 1,987 | 2,021 | 35 | 63,208 | 226 | 216 | monthly |
| GFQA_v2 | literature | global compilations | 1,995 | 2,021 | 27 | 56,297 | 2,062 | 1,700 | annual\|daily\|monthly |
| HYBAM | in-situ | regional datasets | 1,994 | 2,024 | 31 | 11,826 | 12 | 12 | daily |
| Mekong_Delta | literature | global compilations | 2,005 | 2,012 | 8 | 11,323 | 4 | 4 | daily |
| Robotham | literature | global compilations | 2,016 | 2,021 | 6 | 3,432 | 3 | 3 | daily |
| Eurasian_River | literature | global compilations | 1,938 | 2,000 | 63 | 3,204 | 17 | 17 | monthly |
| Fukushima | literature | global compilations | 2,012 | 2,018 | 7 | 3,069 | 2 | 2 | daily |
| NERC | literature | global compilations | 2,013 | 2,014 | 2 | 624 | 4 | 4 | daily |
| Chao_Phraya_River | literature | global compilations | 1,912 | 2,020 | 109 | 348 | 7 | 7 | annual |

_Showing first 15 of 21 rows._

## Interpretation Notes

- **Main-track metrics** (Key Metrics, Main Source Contribution) are the primary reference for manuscript contribution claims.
- Record dominance in the merged table does not necessarily imply the broadest spatial footprint or the most scientifically useful data.
- Satellite source rows dominate the merged totals by record count, but their Q/SSL coverage is typically zero and SSC is sparse.
- Source classification is conservative; review `source_classification_template.csv` before using type/group proportions as final manuscript text.
- Satellite source datasets from Dethier and Shashi_Jianli report Q and SSC counts equal to total records as a best estimate; verify actual coverage in the NetCDF file.

## Figures

- `fig_climatology_contribution_clusters.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_climatology_contribution_clusters.png`
- `fig_climatology_contribution_records.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_climatology_contribution_records.png`
- `fig_climatology_contribution_stations.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_climatology_contribution_stations.png`
- `fig_climatology_resolution_stacked.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_climatology_resolution_stacked.png`
- `fig_climatology_temporal_coverage.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_climatology_temporal_coverage.png`
- `fig_climatology_variable_stacked.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_climatology_variable_stacked.png`
- `fig_satellite_contribution_clusters.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_satellite_contribution_clusters.png`
- `fig_satellite_contribution_records.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_satellite_contribution_records.png`
- `fig_satellite_contribution_stations.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_satellite_contribution_stations.png`
- `fig_satellite_resolution_stacked.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_satellite_resolution_stacked.png`
- `fig_satellite_temporal_coverage.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_satellite_temporal_coverage.png`
- `fig_satellite_variable_stacked.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_satellite_variable_stacked.png`
- `fig_source_contribution_clusters.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_source_contribution_clusters.png`
- `fig_source_contribution_records.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_source_contribution_records.png`
- `fig_source_contribution_stations.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_source_contribution_stations.png`
- `fig_source_cumulative_contribution.png`: `$WORKSPACE/output_other/stats_release_full/source_contribution/figures/fig_source_cumulative_contribution.png`
- Additional figures: 5
