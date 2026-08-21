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
- Source stations: 7,469
- Source-summed clusters: 7,157
- Total attributed records: 2,997,121
- Top source by records: `USGS`
- Over-attribution records in source summary: 59,313

| metric | value | detail |
|---|---|---|
| total_source_datasets | 17 |  |
| total_source_stations | 7,469 |  |
| total_clusters_source_sum | 7,157 |  |
| total_records | 2,997,121 |  |
| total_Q_records | 2,965,371 |  |
| total_SSC_records | 2,997,121 |  |
| total_SSL_records | 2,572,712 |  |
| top_source_by_records | USGS | 56.23% |
| earliest_year | 1,912 |  |
| latest_year | 2,025 |  |

## Main Source Contribution (In-Situ / Reference / Climatology)

Primary contribution table. This track excludes satellite-derived sources (RiverSed, GSED, Dethier, Shashi_Jianli) which are reported separately below.

| source name | source type | source group | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | first year | last year | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | in_situ | in_situ | 890 | 889 | 1,685,357 | 1,685,357 | 1,685,357 | 1,685,357 | 1,980 | 2,024 | daily | 56.23% |
| HYDAT | in_situ | in_situ | 541 | 540 | 671,979 | 669,567 | 671,979 | 668,742 | 1,948 | 1,997 | daily | 22.42% |
| Bayern | in_situ | in_situ | 37 | 37 | 421,052 | 391,834 | 421,052 | 0 | 1,965 | 2,025 | daily | 14.05% |
| GFQA_v2 | in_situ | in_situ | 5,808 | 5,499 | 185,954 | 185,954 | 185,954 | 185,954 | 1,978 | 2,023 | daily | 6.20% |
| Mekong_Delta | in_situ | in_situ | 4 | 4 | 11,921 | 11,921 | 11,921 | 11,921 | 2,005 | 2,017 | daily | 0.40% |
| HYBAM | in_situ | in_situ | 12 | 12 | 9,404 | 9,404 | 9,404 | 9,404 | 1,994 | 2,024 | daily | 0.31% |
| Robotham | in_situ | in_situ | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 2,016 | 2,021 | daily | 0.11% |
| Eurasian_River | in_situ | in_situ | 17 | 17 | 3,263 | 3,263 | 3,263 | 3,263 | 1,938 | 2,000 | monthly | 0.11% |
| Fukushima | in_situ | in_situ | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 2,012 | 2,018 | daily | 0.10% |
| NERC | in_situ | in_situ | 4 | 4 | 624 | 624 | 624 | 624 | 2,013 | 2,014 | daily | 0.02% |
| Chao_Phraya_River | in_situ | in_situ | 7 | 7 | 348 | 348 | 348 | 348 | 1,912 | 2,020 | annual | 0.01% |
| Rhine | in_situ | in_situ | 12 | 12 | 312 | 312 | 312 | 312 | 1,990 | 2,011 | daily | 0.01% |
| Shashi_Jianli | in_situ | in_situ | 2 | 2 | 154 | 154 | 154 | 154 | 2,016 | 2,023 | daily | 0.01% |
| Huanghe | in_situ | in_situ | 24 | 24 | 120 | 0 | 120 | 0 | 2,015 | 2,019 | annual | 0.00% |
| GloRiSe | in_situ | in_situ | 77 | 77 | 103 | 103 | 103 | 103 | 1,979 | 2,012 | daily | 0.00% |

_Showing first 15 of 17 rows._

## Main Source Contribution by Type

| summary level | category | n source datasets | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_group | in_situ | 17 | 7,469 | 7,157 | 2,997,121 | 2,965,371 | 2,997,121 | 2,572,712 | annual\|daily\|monthly | 100% |
| source_type | in_situ | 17 | 7,469 | 7,157 | 2,997,121 | 2,965,371 | 2,997,121 | 2,572,712 | annual\|daily\|monthly | 100% |

## Main Source by Resolution

| source name | product | resolution | source type | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | main | daily | in_situ | 890 | 889 | 1,685,357 | 1,685,357 | 1,685,357 | 1,685,357 | 56.23% | 100% |
| HYDAT | main | daily | in_situ | 541 | 540 | 671,979 | 669,567 | 671,979 | 668,742 | 22.42% | 100% |
| Bayern | main | daily | in_situ | 37 | 37 | 421,052 | 391,834 | 421,052 | 0 | 14.05% | 100% |
| GFQA_v2 | main | daily | in_situ | 5,808 | 5,499 | 185,954 | 185,954 | 185,954 | 185,954 | 6.20% | 100% |
| Mekong_Delta | main | daily | in_situ | 4 | 4 | 11,921 | 11,921 | 11,921 | 11,921 | 0.40% | 100% |
| HYBAM | main | daily | in_situ | 12 | 12 | 9,404 | 9,404 | 9,404 | 9,404 | 0.31% | 100% |
| Robotham | main | daily | in_situ | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.11% | 100% |
| Eurasian_River | main | monthly | in_situ | 17 | 17 | 3,263 | 3,263 | 3,263 | 3,263 | 0.11% | 100% |
| Fukushima | main | daily | in_situ | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.10% | 100% |
| NERC | main | daily | in_situ | 4 | 4 | 624 | 624 | 624 | 624 | 0.02% | 100% |
| Chao_Phraya_River | main | annual | in_situ | 7 | 7 | 348 | 348 | 348 | 348 | 0.01% | 100% |
| Rhine | main | daily | in_situ | 12 | 12 | 312 | 312 | 312 | 312 | 0.01% | 100% |
| Shashi_Jianli | main | daily | in_situ | 2 | 2 | 154 | 154 | 154 | 154 | 0.01% | 100% |
| Huanghe | main | annual | in_situ | 24 | 24 | 120 | 0 | 120 | 0 | 0.00% | 100% |
| GloRiSe | main | daily | in_situ | 77 | 77 | 103 | 103 | 103 | 103 | 0.00% | 100% |
| Yajiang | main | daily | in_situ | 23 | 23 | 23 | 23 | 23 | 23 | 0.00% | 100% |
| Myanmar | main | daily | in_situ | 6 | 5 | 6 | 6 | 6 | 6 | 0.00% | 100% |

## Catalog Attribution Cross-Check

This table separates unique source-station attribution from cluster-exploded attribution.

| source name | n source stations | n clusters | available resolutions | main record count | record attributed record count | cluster attributed record count | over attribution record count |
|---|---|---|---|---|---|---|---|
| USGS | 890 | 889 | daily | 1,690,433 | 1,685,357 | 1,690,433 | 5,076 |
| HYDAT | 541 | 540 | daily | 676,024 | 671,979 | 676,024 | 4,045 |
| Bayern | 37 | 37 | daily | 421,052 | 421,052 | 421,052 | 0 |
| GFQA_v2 | 5,808 | 5,499 | daily | 235,600 | 185,954 | 235,600 | 49,646 |
| Mekong_Delta | 4 | 4 | daily | 11,921 | 11,921 | 11,921 | 0 |
| HYBAM | 12 | 12 | daily | 9,404 | 9,404 | 9,404 | 0 |
| Robotham | 3 | 3 | daily | 3,432 | 3,432 | 3,432 | 0 |
| Eurasian_River | 17 | 17 | monthly | 3,263 | 3,263 | 3,263 | 0 |
| Fukushima | 2 | 2 | daily | 3,069 | 3,069 | 3,069 | 0 |
| NERC | 4 | 4 | daily | 624 | 624 | 624 | 0 |
| Chao_Phraya_River | 7 | 7 | annual | 348 | 348 | 348 | 0 |
| Rhine | 12 | 12 | daily | 312 | 312 | 312 | 0 |
| Shashi_Jianli | 2 | 2 | daily | 154 | 154 | 154 | 0 |
| Huanghe | 24 | 24 | annual | 120 | 120 | 120 | 0 |
| GloRiSe | 77 | 77 | daily | 649 | 103 | 649 | 546 |

_Showing first 15 of 17 rows._

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
| RiverSed | satellite | satellite | 32,941 | 32,941 | 14,199,854 | 0 | 0 | 0 | 1,984 | 2,019 | daily | 86.17% |
| GSED | satellite | satellite | 5,237 | 5,237 | 2,144,599 | 0 | 2,144,599 | 0 | 1,985 | 2,020 | monthly | 13.01% |
| Dethier | satellite | satellite | 372 | 372 | 133,823 | 133,823 | 133,823 | 133,823 | 1,984 | 2,020 | monthly | 0.81% |

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
| 1 | RiverSed | satellite | satellite | 14,199,854 | 14,199,854 | 72.91% |
| 2 | GSED | satellite | satellite | 2,144,599 | 16,344,453 | 83.92% |
| 3 | USGS | in_situ | in_situ | 1,685,357 | 18,029,810 | 92.58% |
| 4 | HYDAT | in_situ | in_situ | 671,979 | 18,701,789 | 96.03% |
| 5 | Bayern | in_situ | in_situ | 421,052 | 19,122,841 | 98.19% |
| 6 | GFQA_v2 | in_situ | in_situ | 185,954 | 19,308,795 | 99.14% |
| 7 | Dethier | satellite | satellite | 133,823 | 19,442,618 | 99.83% |
| 8 | Mekong_Delta | in_situ | in_situ | 11,921 | 19,454,539 | 99.89% |
| 9 | HYBAM | in_situ | in_situ | 9,404 | 19,463,943 | 99.94% |
| 10 | Robotham | in_situ | in_situ | 3,432 | 19,467,375 | 99.96% |
| 11 | Eurasian_River | in_situ | in_situ | 3,263 | 19,470,638 | 99.98% |
| 12 | Fukushima | in_situ | in_situ | 3,069 | 19,473,707 | 99.99% |
| 13 | NERC | in_situ | in_situ | 624 | 19,474,331 | 99.99% |
| 14 | Chao_Phraya_River | in_situ | in_situ | 348 | 19,474,679 | 100.00% |
| 15 | Rhine | in_situ | in_situ | 312 | 19,474,991 | 100.00% |

_Showing first 15 of 20 rows._

## Contribution by Source Type and Group (Combined)

| summary level | category | n source datasets | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_group | satellite | 3 | 38,550 | 38,550 | 16,478,276 | 133,823 | 2,278,422 | 133,823 | daily\|monthly | 84.61% |
| source_type | satellite | 3 | 38,550 | 38,550 | 16,478,276 | 133,823 | 2,278,422 | 133,823 | daily\|monthly | 84.61% |
| source_group | in_situ | 17 | 7,469 | 7,157 | 2,997,121 | 2,965,371 | 2,997,121 | 2,572,712 | annual\|daily\|monthly | 15.39% |
| source_type | in_situ | 17 | 7,469 | 7,157 | 2,997,121 | 2,965,371 | 2,997,121 | 2,572,712 | annual\|daily\|monthly | 15.39% |

## Source by Resolution (Combined)

| source name | product | resolution | source type | n source stations | n clusters | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | daily | satellite | 32,941 | 32,941 | 14,199,854 | 0 | 0 | 0 | 72.91% | 100% |
| GSED | satellite | monthly | satellite | 5,237 | 5,237 | 2,144,599 | 0 | 2,144,599 | 0 | 11.01% | 100% |
| USGS | main | daily | in_situ | 890 | 889 | 1,685,357 | 1,685,357 | 1,685,357 | 1,685,357 | 8.65% | 100% |
| HYDAT | main | daily | in_situ | 541 | 540 | 671,979 | 669,567 | 671,979 | 668,742 | 3.45% | 100% |
| Bayern | main | daily | in_situ | 37 | 37 | 421,052 | 391,834 | 421,052 | 0 | 2.16% | 100% |
| GFQA_v2 | main | daily | in_situ | 5,808 | 5,499 | 185,954 | 185,954 | 185,954 | 185,954 | 0.95% | 100% |
| Dethier | satellite | monthly | satellite | 372 | 372 | 133,823 | 133,823 | 133,823 | 133,823 | 0.69% | 100% |
| Mekong_Delta | main | daily | in_situ | 4 | 4 | 11,921 | 11,921 | 11,921 | 11,921 | 0.06% | 100% |
| HYBAM | main | daily | in_situ | 12 | 12 | 9,404 | 9,404 | 9,404 | 9,404 | 0.05% | 100% |
| Robotham | main | daily | in_situ | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.02% | 100% |
| Eurasian_River | main | monthly | in_situ | 17 | 17 | 3,263 | 3,263 | 3,263 | 3,263 | 0.02% | 100% |
| Fukushima | main | daily | in_situ | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.02% | 100% |
| NERC | main | daily | in_situ | 4 | 4 | 624 | 624 | 624 | 624 | 0.00% | 100% |
| Chao_Phraya_River | main | annual | in_situ | 7 | 7 | 348 | 348 | 348 | 348 | 0.00% | 100% |
| Rhine | main | daily | in_situ | 12 | 12 | 312 | 312 | 312 | 312 | 0.00% | 100% |
| Shashi_Jianli | main | daily | in_situ | 2 | 2 | 154 | 154 | 154 | 154 | 0.00% | 100% |
| Huanghe | main | annual | in_situ | 24 | 24 | 120 | 0 | 120 | 0 | 0.00% | 100% |
| GloRiSe | main | daily | in_situ | 77 | 77 | 103 | 103 | 103 | 103 | 0.00% | 100% |

_Showing first 18 of 20 rows._

## Source by Variable (Combined)

| source name | source type | source group | variable | n variable records | n source records | percentage of total variable records | percentage within source records |
|---|---|---|---|---|---|---|---|
| GSED | satellite | satellite | SSC | 2,144,599 | 2,144,599 | 40.65% | 100% |
| USGS | in_situ | in_situ | SSL | 1,685,357 | 1,685,357 | 62.27% | 100% |
| USGS | in_situ | in_situ | Q | 1,685,357 | 1,685,357 | 54.38% | 100% |
| USGS | in_situ | in_situ | SSC | 1,685,357 | 1,685,357 | 31.95% | 100% |
| HYDAT | in_situ | in_situ | SSC | 671,979 | 671,979 | 12.74% | 100% |
| HYDAT | in_situ | in_situ | Q | 669,567 | 671,979 | 21.60% | 99.64% |
| HYDAT | in_situ | in_situ | SSL | 668,742 | 671,979 | 24.71% | 99.52% |
| Bayern | in_situ | in_situ | SSC | 421,052 | 421,052 | 7.98% | 100% |
| Bayern | in_situ | in_situ | Q | 391,834 | 421,052 | 12.64% | 93.06% |
| GFQA_v2 | in_situ | in_situ | Q | 185,954 | 185,954 | 6.00% | 100% |
| GFQA_v2 | in_situ | in_situ | SSC | 185,954 | 185,954 | 3.52% | 100% |
| GFQA_v2 | in_situ | in_situ | SSL | 185,954 | 185,954 | 6.87% | 100% |
| Dethier | satellite | satellite | SSC | 133,823 | 133,823 | 2.54% | 100% |
| Dethier | satellite | satellite | Q | 133,823 | 133,823 | 4.32% | 100% |
| Dethier | satellite | satellite | SSL | 133,823 | 133,823 | 4.94% | 100% |
| Mekong_Delta | in_situ | in_situ | SSC | 11,921 | 11,921 | 0.23% | 100% |
| Mekong_Delta | in_situ | in_situ | SSL | 11,921 | 11,921 | 0.44% | 100% |
| Mekong_Delta | in_situ | in_situ | Q | 11,921 | 11,921 | 0.38% | 100% |

_Showing first 18 of 60 rows._

## Temporal Span by Source (Combined)

| source name | source type | source group | first year | last year | year span | n records | n source stations | n clusters | resolutions |
|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | satellite | 1,984 | 2,019 | 36 | 14,199,854 | 32,941 | 32,941 | daily |
| GSED | satellite | satellite | 1,985 | 2,020 | 36 | 2,144,599 | 5,237 | 5,237 | monthly |
| USGS | in_situ | in_situ | 1,980 | 2,024 | 45 | 1,685,357 | 890 | 889 | daily |
| HYDAT | in_situ | in_situ | 1,948 | 1,997 | 50 | 671,979 | 541 | 540 | daily |
| Bayern | in_situ | in_situ | 1,965 | 2,025 | 61 | 421,052 | 37 | 37 | daily |
| GFQA_v2 | in_situ | in_situ | 1,978 | 2,023 | 46 | 185,954 | 5,808 | 5,499 | daily |
| Dethier | satellite | satellite | 1,984 | 2,020 | 37 | 133,823 | 372 | 372 | monthly |
| Mekong_Delta | in_situ | in_situ | 2,005 | 2,017 | 13 | 11,921 | 4 | 4 | daily |
| HYBAM | in_situ | in_situ | 1,994 | 2,024 | 31 | 9,404 | 12 | 12 | daily |
| Robotham | in_situ | in_situ | 2,016 | 2,021 | 6 | 3,432 | 3 | 3 | daily |
| Eurasian_River | in_situ | in_situ | 1,938 | 2,000 | 63 | 3,263 | 17 | 17 | monthly |
| Fukushima | in_situ | in_situ | 2,012 | 2,018 | 7 | 3,069 | 2 | 2 | daily |
| NERC | in_situ | in_situ | 2,013 | 2,014 | 2 | 624 | 4 | 4 | daily |
| Chao_Phraya_River | in_situ | in_situ | 1,912 | 2,020 | 109 | 348 | 7 | 7 | annual |
| Rhine | in_situ | in_situ | 1,990 | 2,011 | 22 | 312 | 12 | 12 | daily |

_Showing first 15 of 20 rows._

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
