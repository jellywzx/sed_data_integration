# S8 Source Contribution Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/source_contribution/tables`
- Source contribution uses release catalogs and release NetCDF provenance only.
- **Dual-track reporting**: main in-situ/reference sources are reported separately from satellite validation sources.

## Counting Policy

- `record_attributed_record_count` is source-station based and avoids multi-source cluster over-counting.
- `cluster_attributed_record_count` preserves the historical exploded cluster attribution for parity with older reports.
- Cluster counts can sum above unique release clusters because multiple sources can contribute to the same reference cluster.
- Satellite percentages throughout this report are computed against satellite-only totals, not merged totals.

## Key Metrics (Main Track — In-Situ / Reference / Climatology)

- Source datasets: 17
- Source stations: 3,911
- Source-summed clusters: 3,771
- Total attributed records: 2,873,266
- Top source by records: `USGS`
- Over-attribution records in source summary: 6,576

| metric | value | detail |
|---|---|---|
| total_source_datasets | 17 |  |
| total_source_stations | 3,911 |  |
| total_clusters_source_sum | 3,771 |  |
| total_records | 2,873,266 |  |
| total_Q_records | 2,873,146 |  |
| total_SSC_records | 2,873,266 |  |
| total_SSL_records | 2,484,182 |  |
| top_source_by_records | USGS | 57.68% |
| earliest_year | 1,912 |  |
| latest_year | 2,025 |  |

## Main Source Contribution (In-Situ / Reference / Climatology)

Primary contribution table. This track excludes satellite-derived sources (RiverSed, GSED, Dethier, Shashi_Jianli) which are reported separately below.

| source name | source type | source group | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | first year | last year | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | in-situ | national agencies | 887 | 887 | 1,657,251 | 1,657,251 | 1,657,251 | 1,657,251 | 1,980 | 2,024 | daily | 57.68% |
| HYDAT | in-situ | national agencies | 505 | 505 | 669,567 | 669,567 | 669,567 | 669,567 | 1,948 | 1,997 | daily | 23.30% |
| Bayern | in-situ | national agencies | 34 | 34 | 388,964 | 388,964 | 388,964 | 0 | 1,965 | 2,025 | daily | 13.54% |
| EUSEDcollab | literature | global compilations | 244 | 244 | 66,637 | 66,637 | 66,637 | 66,637 | 1,987 | 2,021 | monthly | 2.32% |
| GFQA_v2 | literature | global compilations | 2,050 | 1,910 | 56,457 | 56,457 | 56,457 | 56,457 | 1,995 | 2,021 | annual\|daily\|monthly | 1.96% |
| HYBAM | in-situ | regional datasets | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | 1,994 | 2,024 | daily | 0.41% |
| Mekong_Delta | literature | global compilations | 4 | 4 | 11,323 | 11,323 | 11,323 | 11,323 | 2,005 | 2,012 | daily | 0.39% |
| Robotham | literature | global compilations | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 2,016 | 2,021 | daily | 0.12% |
| Eurasian_River | literature | global compilations | 17 | 17 | 3,204 | 3,204 | 3,204 | 3,204 | 1,938 | 2,000 | monthly | 0.11% |
| Fukushima | literature | global compilations | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 2,012 | 2,018 | daily | 0.11% |
| NERC | literature | global compilations | 4 | 4 | 624 | 624 | 624 | 624 | 2,013 | 2,014 | daily | 0.02% |
| Chao_Phraya_River | literature | global compilations | 7 | 7 | 348 | 348 | 348 | 348 | 1,912 | 2,020 | annual | 0.01% |
| Rhine | literature | global compilations | 12 | 12 | 312 | 312 | 312 | 312 | 1,990 | 2,011 | daily | 0.01% |
| Huanghe | literature | global compilations | 24 | 24 | 120 | 0 | 120 | 0 | 2,015 | 2,019 | annual | 0.00% |
| GloRiSe | literature | global compilations | 77 | 77 | 103 | 103 | 103 | 103 | 1,979 | 2,012 | daily\|monthly | 0.00% |

_Showing first 15 of 17 rows._

## Main Source Contribution by Type

| summary level | category | n source datasets | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_type | in-situ | 4 | 1,438 | 1,438 | 2,727,608 | 2,727,608 | 2,727,608 | 2,338,644 | daily | 94.93% |
| source_group | national agencies | 3 | 1,426 | 1,426 | 2,715,782 | 2,715,782 | 2,715,782 | 2,326,818 | daily | 94.52% |
| source_group | global compilations | 13 | 2,473 | 2,333 | 145,658 | 145,538 | 145,658 | 145,538 | annual\|daily\|monthly | 5.07% |
| source_type | literature | 13 | 2,473 | 2,333 | 145,658 | 145,538 | 145,658 | 145,538 | annual\|daily\|monthly | 5.07% |
| source_group | regional datasets | 1 | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | daily | 0.41% |

## Main Source by Resolution

| source name | product | resolution | source type | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | main | daily | in-situ | 887 | 887 | 1,657,251 | 1,657,251 | 1,657,251 | 1,657,251 | 57.68% | 100% |
| HYDAT | main | daily | in-situ | 505 | 505 | 669,567 | 669,567 | 669,567 | 669,567 | 23.30% | 100% |
| Bayern | main | daily | in-situ | 34 | 34 | 388,964 | 388,964 | 388,964 | 0 | 13.54% | 100% |
| EUSEDcollab | main | monthly | literature | 244 | 244 | 66,637 | 66,637 | 66,637 | 66,637 | 2.32% | 100% |
| GFQA_v2 | main | monthly | literature | 1,982 | 1,842 | 56,255 | 56,255 | 56,255 | 56,255 | 1.96% | 99.64% |
| HYBAM | main | daily | in-situ | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | 0.41% | 100% |
| Mekong_Delta | main | daily | literature | 4 | 4 | 11,323 | 11,323 | 11,323 | 11,323 | 0.39% | 100% |
| Robotham | main | daily | literature | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.12% | 100% |
| Eurasian_River | main | monthly | literature | 17 | 17 | 3,204 | 3,204 | 3,204 | 3,204 | 0.11% | 100% |
| Fukushima | main | daily | literature | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.11% | 100% |
| NERC | main | daily | literature | 4 | 4 | 624 | 624 | 624 | 624 | 0.02% | 100% |
| Chao_Phraya_River | main | annual | literature | 7 | 7 | 348 | 348 | 348 | 348 | 0.01% | 100% |
| Rhine | main | daily | literature | 12 | 12 | 312 | 312 | 312 | 312 | 0.01% | 100% |
| GFQA_v2 | main | annual | literature | 27 | 27 | 151 | 151 | 151 | 151 | 0.01% | 0.27% |
| Huanghe | main | annual | literature | 24 | 24 | 120 | 0 | 120 | 0 | 0.00% | 100% |
| GloRiSe | main | daily | literature | 63 | 63 | 63 | 63 | 63 | 63 | 0.00% | 61.17% |
| GFQA_v2 | main | daily | literature | 41 | 41 | 51 | 51 | 51 | 51 | 0.00% | 0.09% |
| GloRiSe | main | monthly | literature | 14 | 14 | 40 | 40 | 40 | 40 | 0.00% | 38.83% |

_Showing first 18 of 20 rows._

## Catalog Attribution Cross-Check

This table separates unique source-station attribution from cluster-exploded attribution.

| source name | n source stations | n clusters | available resolutions | main record count | record attributed record count | cluster attributed record count | over attribution record count |
|---|---|---|---|---|---|---|---|
| USGS | 887 | 887 | daily | 1,662,326 | 1,657,251 | 1,662,326 | 5,075 |
| HYDAT | 505 | 505 | daily | 671,068 | 669,567 | 671,068 | 1,501 |
| Bayern | 34 | 34 | daily | 388,964 | 388,964 | 388,964 | 0 |
| EUSEDcollab | 244 | 244 | monthly | 66,637 | 66,637 | 66,637 | 0 |
| GFQA_v2 | 2,050 | 1,901 | annual\|daily\|monthly | 56,457 | 56,457 | 56,457 | 0 |
| HYBAM | 12 | 12 | daily | 11,826 | 11,826 | 11,826 | 0 |
| Mekong_Delta | 4 | 4 | daily | 11,323 | 11,323 | 11,323 | 0 |
| Robotham | 3 | 3 | daily | 3,432 | 3,432 | 3,432 | 0 |
| Eurasian_River | 17 | 17 | monthly | 3,204 | 3,204 | 3,204 | 0 |
| Fukushima | 2 | 2 | daily | 3,069 | 3,069 | 3,069 | 0 |
| NERC | 4 | 4 | daily | 624 | 624 | 624 | 0 |
| Chao_Phraya_River | 7 | 7 | annual | 348 | 348 | 348 | 0 |
| Rhine | 12 | 12 | daily | 312 | 312 | 312 | 0 |
| Shashi_Jianli | 2 | 2 | daily | 154 | 154 | 154 | 0 |
| Huanghe | 24 | 24 | annual | 120 | 120 | 120 | 0 |

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
| RiverSed | satellite | satellite products | 32,941 | 32,941 | 14,199,854 | 0 | 0 | 0 | 1,984 | 2,019 | daily | 86.17% |
| GSED | satellite | satellite products | 5,237 | 5,237 | 2,144,599 | 0 | 2,144,599 | 0 | 1,985 | 2,020 | monthly | 13.01% |
| Dethier | satellite | satellite products | 372 | 372 | 133,823 | 133,823 | 133,823 | 133,823 | 1,984 | 2,020 | monthly | 0.81% |
| Shashi_Jianli | satellite | satellite products | 2 | 2 | 154 | 154 | 154 | 154 | 2,016 | 2,023 | daily | 0.00% |

## Satellite Source-Resolution Contribution (CSV catalog)

Satellite products remain validation-sidecar contributions and should be interpreted with variable coverage. Q/SSL are often entirely absent.

| source name | resolution | satellite station count | satellite cluster count | satellite record count |
|---|---|---|---|---|
| Dethier | monthly | 372 | 372 | 133,823 |
| GSED | monthly | 5,237 | 5,237 | 2,144,599 |
| RiverSed | daily | 32,941 | 32,941 | 14,199,854 |

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
| 1 | RiverSed | satellite | satellite products | 14,199,854 | 14,199,854 | 73.38% |
| 2 | GSED | satellite | satellite products | 2,144,599 | 16,344,453 | 84.46% |
| 3 | USGS | in-situ | national agencies | 1,657,251 | 18,001,704 | 93.02% |
| 4 | HYDAT | in-situ | national agencies | 669,567 | 18,671,271 | 96.48% |
| 5 | Bayern | in-situ | national agencies | 388,964 | 19,060,235 | 98.49% |
| 6 | Dethier | satellite | satellite products | 133,823 | 19,194,058 | 99.19% |
| 7 | EUSEDcollab | literature | global compilations | 66,637 | 19,260,695 | 99.53% |
| 8 | GFQA_v2 | literature | global compilations | 56,457 | 19,317,152 | 99.82% |
| 9 | HYBAM | in-situ | regional datasets | 11,826 | 19,328,978 | 99.88% |
| 10 | Mekong_Delta | literature | global compilations | 11,323 | 19,340,301 | 99.94% |
| 11 | Robotham | literature | global compilations | 3,432 | 19,343,733 | 99.96% |
| 12 | Eurasian_River | literature | global compilations | 3,204 | 19,346,937 | 99.98% |
| 13 | Fukushima | literature | global compilations | 3,069 | 19,350,006 | 99.99% |
| 14 | NERC | literature | global compilations | 624 | 19,350,630 | 99.99% |
| 15 | Chao_Phraya_River | literature | global compilations | 348 | 19,350,978 | 100.00% |

_Showing first 15 of 21 rows._

## Contribution by Source Type and Group (Combined)

| summary level | category | n source datasets | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_group | satellite products | 4 | 38,552 | 38,552 | 16,478,430 | 133,977 | 2,278,576 | 133,977 | daily\|monthly | 85.15% |
| source_type | satellite | 4 | 38,552 | 38,552 | 16,478,430 | 133,977 | 2,278,576 | 133,977 | daily\|monthly | 85.15% |
| source_type | in-situ | 4 | 1,438 | 1,438 | 2,727,608 | 2,727,608 | 2,727,608 | 2,338,644 | daily | 14.09% |
| source_group | national agencies | 3 | 1,426 | 1,426 | 2,715,782 | 2,715,782 | 2,715,782 | 2,326,818 | daily | 14.03% |
| source_group | global compilations | 13 | 2,473 | 2,333 | 145,658 | 145,538 | 145,658 | 145,538 | annual\|daily\|monthly | 0.75% |
| source_type | literature | 13 | 2,473 | 2,333 | 145,658 | 145,538 | 145,658 | 145,538 | annual\|daily\|monthly | 0.75% |
| source_group | regional datasets | 1 | 12 | 12 | 11,826 | 11,826 | 11,826 | 11,826 | daily | 0.06% |

## Source by Resolution (Combined)

| source name | product | resolution | source type | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | daily | satellite | 32,941 | 32,941 | 14,199,854 | 0 | 0 | 0 | 73.38% | 100% |
| GSED | satellite | monthly | satellite | 5,237 | 5,237 | 2,144,599 | 0 | 2,144,599 | 0 | 11.08% | 100% |
| USGS | main | daily | in-situ | 887 | 887 | 1,657,251 | 1,657,251 | 1,657,251 | 1,657,251 | 8.56% | 100% |
| HYDAT | main | daily | in-situ | 505 | 505 | 669,567 | 669,567 | 669,567 | 669,567 | 3.46% | 100% |
| Bayern | main | daily | in-situ | 34 | 34 | 388,964 | 388,964 | 388,964 | 0 | 2.01% | 100% |
| Dethier | satellite | monthly | satellite | 372 | 372 | 133,823 | 133,823 | 133,823 | 133,823 | 0.69% | 100% |
| EUSEDcollab | main | monthly | literature | 244 | 244 | 66,637 | 66,637 | 66,637 | 66,637 | 0.34% | 100% |
| GFQA_v2 | main | monthly | literature | 1,982 | 1,842 | 56,255 | 56,255 | 56,255 | 56,255 | 0.29% | 99.64% |
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
| GSED | satellite | satellite products | SSC | 2,144,599 | 2,144,599 | 41.63% | 100% |
| USGS | in-situ | national agencies | Q | 1,657,251 | 1,657,251 | 55.11% | 100% |
| USGS | in-situ | national agencies | SSC | 1,657,251 | 1,657,251 | 32.17% | 100% |
| USGS | in-situ | national agencies | SSL | 1,657,251 | 1,657,251 | 63.30% | 100% |
| HYDAT | in-situ | national agencies | Q | 669,567 | 669,567 | 22.27% | 100% |
| HYDAT | in-situ | national agencies | SSC | 669,567 | 669,567 | 13.00% | 100% |
| HYDAT | in-situ | national agencies | SSL | 669,567 | 669,567 | 25.57% | 100% |
| Bayern | in-situ | national agencies | SSC | 388,964 | 388,964 | 7.55% | 100% |
| Bayern | in-situ | national agencies | Q | 388,964 | 388,964 | 12.93% | 100% |
| Dethier | satellite | satellite products | SSC | 133,823 | 133,823 | 2.60% | 100% |
| Dethier | satellite | satellite products | Q | 133,823 | 133,823 | 4.45% | 100% |
| Dethier | satellite | satellite products | SSL | 133,823 | 133,823 | 5.11% | 100% |
| EUSEDcollab | literature | global compilations | Q | 66,637 | 66,637 | 2.22% | 100% |
| EUSEDcollab | literature | global compilations | SSL | 66,637 | 66,637 | 2.55% | 100% |
| EUSEDcollab | literature | global compilations | SSC | 66,637 | 66,637 | 1.29% | 100% |
| GFQA_v2 | literature | global compilations | SSL | 56,457 | 56,457 | 2.16% | 100% |
| GFQA_v2 | literature | global compilations | SSC | 56,457 | 56,457 | 1.10% | 100% |
| GFQA_v2 | literature | global compilations | Q | 56,457 | 56,457 | 1.88% | 100% |

_Showing first 18 of 63 rows._

## Temporal Span by Source (Combined)

| source name | source type | source group | first year | last year | year span | n records | n source stations | n clusters | resolutions |
|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | satellite products | 1,984 | 2,019 | 36 | 14,199,854 | 32,941 | 32,941 | daily |
| GSED | satellite | satellite products | 1,985 | 2,020 | 36 | 2,144,599 | 5,237 | 5,237 | monthly |
| USGS | in-situ | national agencies | 1,980 | 2,024 | 45 | 1,657,251 | 887 | 887 | daily |
| HYDAT | in-situ | national agencies | 1,948 | 1,997 | 50 | 669,567 | 505 | 505 | daily |
| Bayern | in-situ | national agencies | 1,965 | 2,025 | 61 | 388,964 | 34 | 34 | daily |
| Dethier | satellite | satellite products | 1,984 | 2,020 | 37 | 133,823 | 372 | 372 | monthly |
| EUSEDcollab | literature | global compilations | 1,987 | 2,021 | 35 | 66,637 | 244 | 244 | monthly |
| GFQA_v2 | literature | global compilations | 1,995 | 2,021 | 27 | 56,457 | 2,050 | 1,910 | annual\|daily\|monthly |
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

- `fig_climatology_contribution_clusters.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_contribution_clusters.png`
- `fig_climatology_contribution_records.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_contribution_records.png`
- `fig_climatology_contribution_stations.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_contribution_stations.png`
- `fig_climatology_resolution_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_resolution_stacked.png`
- `fig_climatology_temporal_coverage.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_temporal_coverage.png`
- `fig_climatology_variable_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_variable_stacked.png`
- `fig_satellite_contribution_clusters.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_contribution_clusters.png`
- `fig_satellite_contribution_records.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_contribution_records.png`
- `fig_satellite_contribution_stations.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_contribution_stations.png`
- `fig_satellite_resolution_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_resolution_stacked.png`
- `fig_satellite_temporal_coverage.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_temporal_coverage.png`
- `fig_satellite_variable_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_variable_stacked.png`
- `fig_source_contribution_clusters.png`: `output_other/stats_release/source_contribution/figures/fig_source_contribution_clusters.png`
- `fig_source_contribution_records.png`: `output_other/stats_release/source_contribution/figures/fig_source_contribution_records.png`
- `fig_source_contribution_stations.png`: `output_other/stats_release/source_contribution/figures/fig_source_contribution_stations.png`
- `fig_source_cumulative_contribution.png`: `output_other/stats_release/source_contribution/figures/fig_source_cumulative_contribution.png`
- Additional figures: 5
