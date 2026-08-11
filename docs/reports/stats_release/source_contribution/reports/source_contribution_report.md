# S8 Source Contribution Statistics

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/source_contribution/tables`
- Source contribution uses release catalogs and release NetCDF provenance only.
- **Dual-track reporting**: main in-situ/reference sources are reported separately from satellite validation sources.

## Counting Policy

- `record_attributed_record_count` is source-station based and avoids multi-source station over-counting.
- `reference_station_attributed_record_count` preserves the historical exploded station attribution for parity with older reports.
- Station counts can sum above unique release stations because multiple sources can contribute to the same reference station.
- Satellite percentages throughout this report are computed against satellite-only totals, not merged totals.

## Key Metrics (Main Track — In-Situ / Reference / Climatology)

- Source datasets: 18
- Source stations: 7,717
- Source-summed stations: 7,478
- Total attributed records: 3,064,671
- Top source by records: `USGS`
- Over-attribution records in source summary: 59,313

| metric | value | detail |
|---|---|---|
| total_source_datasets | 18 |  |
| total_source_stations | 7,717 |  |
| total_reference_stations_source_sum | 7,478 |  |
| total_records | 3,064,671 |  |
| total_Q_records | 3,032,921 |  |
| total_SSC_records | 3,064,671 |  |
| total_SSL_records | 2,640,262 |  |
| top_source_by_records | USGS | 54.99% |
| earliest_year | 1,912 |  |
| latest_year | 2,025 |  |

## Main Source Contribution (In-Situ / Reference / Climatology)

Primary contribution table. This track excludes satellite-derived sources (RiverSed, GSED, Dethier, Shashi_Jianli) which are reported separately below.

| source name | source type | source group | n source stations | reference stations | n records | n Q records | n SSC records | n SSL records | first year | last year | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | in_situ | in_situ | 890 | 889 | 1,685,357 | 1,685,357 | 1,685,357 | 1,685,357 | 1,980 | 2,024 | daily | 54.99% |
| HYDAT | in_situ | in_situ | 541 | 540 | 671,979 | 669,567 | 671,979 | 668,742 | 1,948 | 1,997 | daily | 21.93% |
| Bayern | in_situ | in_situ | 37 | 37 | 421,052 | 391,834 | 421,052 | 0 | 1,965 | 2,025 | daily | 13.74% |
| GFQA_v2 | in_situ | in_situ | 5,812 | 5,576 | 186,867 | 186,867 | 186,867 | 186,867 | 1,978 | 2,023 | annual\|daily\|monthly | 6.10% |
| EUSEDcollab | in_situ | in_situ | 244 | 244 | 66,637 | 66,637 | 66,637 | 66,637 | 1,987 | 2,021 | daily\|monthly | 2.17% |
| Mekong_Delta | in_situ | in_situ | 4 | 4 | 11,921 | 11,921 | 11,921 | 11,921 | 2,005 | 2,017 | daily | 0.39% |
| HYBAM | in_situ | in_situ | 12 | 12 | 9,404 | 9,404 | 9,404 | 9,404 | 1,994 | 2,024 | daily | 0.31% |
| Robotham | in_situ | in_situ | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 2,016 | 2,021 | daily | 0.11% |
| Eurasian_River | in_situ | in_situ | 17 | 17 | 3,263 | 3,263 | 3,263 | 3,263 | 1,938 | 2,000 | monthly | 0.11% |
| Fukushima | in_situ | in_situ | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 2,012 | 2,018 | daily | 0.10% |
| NERC | in_situ | in_situ | 4 | 4 | 624 | 624 | 624 | 624 | 2,013 | 2,014 | daily | 0.02% |
| Chao_Phraya_River | in_situ | in_situ | 7 | 7 | 348 | 348 | 348 | 348 | 1,912 | 2,020 | annual | 0.01% |
| Rhine | in_situ | in_situ | 12 | 12 | 312 | 312 | 312 | 312 | 1,990 | 2,011 | daily | 0.01% |
| Shashi_Jianli | in_situ | in_situ | 2 | 2 | 154 | 154 | 154 | 154 | 2,016 | 2,023 | daily | 0.01% |
| Huanghe | in_situ | in_situ | 24 | 24 | 120 | 0 | 120 | 0 | 2,015 | 2,019 | annual | 0.00% |

_Showing first 15 of 18 rows._

## Main Source Contribution by Type

| summary level | category | n source datasets | n source stations | reference stations | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_group | in_situ | 18 | 7,717 | 7,478 | 3,064,671 | 3,032,921 | 3,064,671 | 2,640,262 | annual\|daily\|monthly | 100% |
| source_type | in_situ | 18 | 7,717 | 7,478 | 3,064,671 | 3,032,921 | 3,064,671 | 2,640,262 | annual\|daily\|monthly | 100% |

## Main Source by Resolution

| source name | product | resolution | source type | n source stations | reference stations | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| USGS | main | daily | in_situ | 890 | 889 | 1,685,357 | 1,685,357 | 1,685,357 | 1,685,357 | 54.99% | 100% |
| HYDAT | main | daily | in_situ | 541 | 540 | 671,979 | 669,567 | 671,979 | 668,742 | 21.93% | 100% |
| Bayern | main | daily | in_situ | 37 | 37 | 421,052 | 391,834 | 421,052 | 0 | 13.74% | 100% |
| GFQA_v2 | main | daily | in_situ | 3,242 | 3,102 | 104,761 | 104,761 | 104,761 | 104,761 | 3.42% | 56.06% |
| GFQA_v2 | main | monthly | in_situ | 2,552 | 2,456 | 82,039 | 82,039 | 82,039 | 82,039 | 2.68% | 43.90% |
| EUSEDcollab | main | daily | in_situ | 33 | 33 | 51,076 | 51,076 | 51,076 | 51,076 | 1.67% | 76.65% |
| EUSEDcollab | main | monthly | in_situ | 211 | 211 | 15,561 | 15,561 | 15,561 | 15,561 | 0.51% | 23.35% |
| Mekong_Delta | main | daily | in_situ | 4 | 4 | 11,921 | 11,921 | 11,921 | 11,921 | 0.39% | 100% |
| HYBAM | main | daily | in_situ | 12 | 12 | 9,404 | 9,404 | 9,404 | 9,404 | 0.31% | 100% |
| Robotham | main | daily | in_situ | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.11% | 100% |
| Eurasian_River | main | monthly | in_situ | 17 | 17 | 3,263 | 3,263 | 3,263 | 3,263 | 0.11% | 100% |
| Fukushima | main | daily | in_situ | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.10% | 100% |
| NERC | main | daily | in_situ | 4 | 4 | 624 | 624 | 624 | 624 | 0.02% | 100% |
| Chao_Phraya_River | main | annual | in_situ | 7 | 7 | 348 | 348 | 348 | 348 | 0.01% | 100% |
| Rhine | main | daily | in_situ | 12 | 12 | 312 | 312 | 312 | 312 | 0.01% | 100% |
| Shashi_Jianli | main | daily | in_situ | 2 | 2 | 154 | 154 | 154 | 154 | 0.01% | 100% |
| Huanghe | main | annual | in_situ | 24 | 24 | 120 | 0 | 120 | 0 | 0.00% | 100% |
| GFQA_v2 | main | annual | in_situ | 18 | 18 | 67 | 67 | 67 | 67 | 0.00% | 0.04% |

_Showing first 18 of 22 rows._

## Catalog Attribution Cross-Check

This table separates unique source-station attribution from station-exploded attribution.

| source name | n source stations | reference stations | available resolutions | main record count | record attributed record count | reference station attributed record count | over attribution record count |
|---|---|---|---|---|---|---|---|
| USGS | 890 | 889 | daily | 1,690,433 | 1,685,357 | 1,690,433 | 5,076 |
| HYDAT | 541 | 540 | daily | 676,024 | 671,979 | 676,024 | 4,045 |
| Bayern | 37 | 37 | daily | 421,052 | 421,052 | 421,052 | 0 |
| GFQA_v2 | 5,811 | 5,499 | annual\|daily\|monthly | 236,513 | 186,867 | 236,513 | 49,646 |
| EUSEDcollab | 244 | 244 | daily\|monthly | 66,637 | 66,637 | 66,637 | 0 |
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

| source name | source type | source group | n source stations | reference stations | n records | n Q records | n SSC records | n SSL records | first year | last year | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | satellite | 32,941 | 161 | 14,199,854 | 0 | 0 | 0 | 1,984 | 2,019 | daily | 86.17% |
| GSED | satellite | satellite | 5,237 | 14 | 2,144,599 | 0 | 2,144,599 | 0 | 1,985 | 2,020 | monthly | 13.01% |
| Dethier | satellite | satellite | 372 | 9 | 133,823 | 133,823 | 133,823 | 133,823 | 1,984 | 2,020 | monthly | 0.81% |

## Satellite Source-Resolution Contribution (CSV catalog)

Satellite products remain validation-sidecar contributions and should be interpreted with variable coverage. Q/SSL are often entirely absent.

| source name | resolution | satellite station count | satellite reference stations | satellite record count |
|---|---|---|---|---|
| Dethier | monthly | 372 | 9 | 133,823 |
| GSED | monthly | 5,237 | 14 | 2,144,599 |
| RiverSed | daily | 32,941 | 161 | 14,199,854 |

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
| 1 | RiverSed | satellite | satellite | 14,199,854 | 14,199,854 | 72.66% |
| 2 | GSED | satellite | satellite | 2,144,599 | 16,344,453 | 83.63% |
| 3 | USGS | in_situ | in_situ | 1,685,357 | 18,029,810 | 92.26% |
| 4 | HYDAT | in_situ | in_situ | 671,979 | 18,701,789 | 95.70% |
| 5 | Bayern | in_situ | in_situ | 421,052 | 19,122,841 | 97.85% |
| 6 | GFQA_v2 | in_situ | in_situ | 186,867 | 19,309,708 | 98.81% |
| 7 | Dethier | satellite | satellite | 133,823 | 19,443,531 | 99.49% |
| 8 | EUSEDcollab | in_situ | in_situ | 66,637 | 19,510,168 | 99.83% |
| 9 | Mekong_Delta | in_situ | in_situ | 11,921 | 19,522,089 | 99.89% |
| 10 | HYBAM | in_situ | in_situ | 9,404 | 19,531,493 | 99.94% |
| 11 | Robotham | in_situ | in_situ | 3,432 | 19,534,925 | 99.96% |
| 12 | Eurasian_River | in_situ | in_situ | 3,263 | 19,538,188 | 99.98% |
| 13 | Fukushima | in_situ | in_situ | 3,069 | 19,541,257 | 99.99% |
| 14 | NERC | in_situ | in_situ | 624 | 19,541,881 | 99.99% |
| 15 | Chao_Phraya_River | in_situ | in_situ | 348 | 19,542,229 | 100.00% |

_Showing first 15 of 21 rows._

## Contribution by Source Type and Group (Combined)

| summary level | category | n source datasets | n source stations | reference stations | n records | n Q records | n SSC records | n SSL records | resolutions | percentage of total records |
|---|---|---|---|---|---|---|---|---|---|---|
| source_group | satellite | 3 | 38,550 | 184 | 16,478,276 | 133,823 | 2,278,422 | 133,823 | daily\|monthly | 84.32% |
| source_type | satellite | 3 | 38,550 | 184 | 16,478,276 | 133,823 | 2,278,422 | 133,823 | daily\|monthly | 84.32% |
| source_group | in_situ | 18 | 7,717 | 7,478 | 3,064,671 | 3,032,921 | 3,064,671 | 2,640,262 | annual\|daily\|monthly | 15.68% |
| source_type | in_situ | 18 | 7,717 | 7,478 | 3,064,671 | 3,032,921 | 3,064,671 | 2,640,262 | annual\|daily\|monthly | 15.68% |

## Source by Resolution (Combined)

| source name | product | resolution | source type | n source stations | reference stations | n records | n Q records | n SSC records | n SSL records | percentage of total records | percentage within source records |
|---|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | daily | satellite | 32,941 | 161 | 14,199,854 | 0 | 0 | 0 | 72.66% | 100% |
| GSED | satellite | monthly | satellite | 5,237 | 14 | 2,144,599 | 0 | 2,144,599 | 0 | 10.97% | 100% |
| USGS | main | daily | in_situ | 890 | 889 | 1,685,357 | 1,685,357 | 1,685,357 | 1,685,357 | 8.62% | 100% |
| HYDAT | main | daily | in_situ | 541 | 540 | 671,979 | 669,567 | 671,979 | 668,742 | 3.44% | 100% |
| Bayern | main | daily | in_situ | 37 | 37 | 421,052 | 391,834 | 421,052 | 0 | 2.15% | 100% |
| Dethier | satellite | monthly | satellite | 372 | 9 | 133,823 | 133,823 | 133,823 | 133,823 | 0.68% | 100% |
| GFQA_v2 | main | daily | in_situ | 3,242 | 3,102 | 104,761 | 104,761 | 104,761 | 104,761 | 0.54% | 56.06% |
| GFQA_v2 | main | monthly | in_situ | 2,552 | 2,456 | 82,039 | 82,039 | 82,039 | 82,039 | 0.42% | 43.90% |
| EUSEDcollab | main | daily | in_situ | 33 | 33 | 51,076 | 51,076 | 51,076 | 51,076 | 0.26% | 76.65% |
| EUSEDcollab | main | monthly | in_situ | 211 | 211 | 15,561 | 15,561 | 15,561 | 15,561 | 0.08% | 23.35% |
| Mekong_Delta | main | daily | in_situ | 4 | 4 | 11,921 | 11,921 | 11,921 | 11,921 | 0.06% | 100% |
| HYBAM | main | daily | in_situ | 12 | 12 | 9,404 | 9,404 | 9,404 | 9,404 | 0.05% | 100% |
| Robotham | main | daily | in_situ | 3 | 3 | 3,432 | 3,432 | 3,432 | 3,432 | 0.02% | 100% |
| Eurasian_River | main | monthly | in_situ | 17 | 17 | 3,263 | 3,263 | 3,263 | 3,263 | 0.02% | 100% |
| Fukushima | main | daily | in_situ | 2 | 2 | 3,069 | 3,069 | 3,069 | 3,069 | 0.02% | 100% |
| NERC | main | daily | in_situ | 4 | 4 | 624 | 624 | 624 | 624 | 0.00% | 100% |
| Chao_Phraya_River | main | annual | in_situ | 7 | 7 | 348 | 348 | 348 | 348 | 0.00% | 100% |
| Rhine | main | daily | in_situ | 12 | 12 | 312 | 312 | 312 | 312 | 0.00% | 100% |

_Showing first 18 of 25 rows._

## Source by Variable (Combined)

| source name | source type | source group | variable | n variable records | n source records | percentage of total variable records | percentage within source records |
|---|---|---|---|---|---|---|---|
| GSED | satellite | satellite | SSC | 2,144,599 | 2,144,599 | 40.14% | 100% |
| USGS | in_situ | in_situ | Q | 1,685,357 | 1,685,357 | 53.22% | 100% |
| USGS | in_situ | in_situ | SSC | 1,685,357 | 1,685,357 | 31.54% | 100% |
| USGS | in_situ | in_situ | SSL | 1,685,357 | 1,685,357 | 60.75% | 100% |
| HYDAT | in_situ | in_situ | SSC | 671,979 | 671,979 | 12.58% | 100% |
| HYDAT | in_situ | in_situ | Q | 669,567 | 671,979 | 21.14% | 99.64% |
| HYDAT | in_situ | in_situ | SSL | 668,742 | 671,979 | 24.11% | 99.52% |
| Bayern | in_situ | in_situ | SSC | 421,052 | 421,052 | 7.88% | 100% |
| Bayern | in_situ | in_situ | Q | 391,834 | 421,052 | 12.37% | 93.06% |
| GFQA_v2 | in_situ | in_situ | SSC | 186,867 | 186,867 | 3.50% | 100% |
| GFQA_v2 | in_situ | in_situ | Q | 186,867 | 186,867 | 5.90% | 100% |
| GFQA_v2 | in_situ | in_situ | SSL | 186,867 | 186,867 | 6.74% | 100% |
| Dethier | satellite | satellite | Q | 133,823 | 133,823 | 4.23% | 100% |
| Dethier | satellite | satellite | SSL | 133,823 | 133,823 | 4.82% | 100% |
| Dethier | satellite | satellite | SSC | 133,823 | 133,823 | 2.50% | 100% |
| EUSEDcollab | in_situ | in_situ | SSL | 66,637 | 66,637 | 2.40% | 100% |
| EUSEDcollab | in_situ | in_situ | SSC | 66,637 | 66,637 | 1.25% | 100% |
| EUSEDcollab | in_situ | in_situ | Q | 66,637 | 66,637 | 2.10% | 100% |

_Showing first 18 of 63 rows._

## Temporal Span by Source (Combined)

| source name | source type | source group | first year | last year | year span | n records | n source stations | reference stations | resolutions |
|---|---|---|---|---|---|---|---|---|---|
| RiverSed | satellite | satellite | 1,984 | 2,019 | 36 | 14,199,854 | 32,941 | 161 | daily |
| GSED | satellite | satellite | 1,985 | 2,020 | 36 | 2,144,599 | 5,237 | 14 | monthly |
| USGS | in_situ | in_situ | 1,980 | 2,024 | 45 | 1,685,357 | 890 | 889 | daily |
| HYDAT | in_situ | in_situ | 1,948 | 1,997 | 50 | 671,979 | 541 | 540 | daily |
| Bayern | in_situ | in_situ | 1,965 | 2,025 | 61 | 421,052 | 37 | 37 | daily |
| GFQA_v2 | in_situ | in_situ | 1,978 | 2,023 | 46 | 186,867 | 5,812 | 5,576 | annual\|daily\|monthly |
| Dethier | satellite | satellite | 1,984 | 2,020 | 37 | 133,823 | 372 | 9 | monthly |
| EUSEDcollab | in_situ | in_situ | 1,987 | 2,021 | 35 | 66,637 | 244 | 244 | daily\|monthly |
| Mekong_Delta | in_situ | in_situ | 2,005 | 2,017 | 13 | 11,921 | 4 | 4 | daily |
| HYBAM | in_situ | in_situ | 1,994 | 2,024 | 31 | 9,404 | 12 | 12 | daily |
| Robotham | in_situ | in_situ | 2,016 | 2,021 | 6 | 3,432 | 3 | 3 | daily |
| Eurasian_River | in_situ | in_situ | 1,938 | 2,000 | 63 | 3,263 | 17 | 17 | monthly |
| Fukushima | in_situ | in_situ | 2,012 | 2,018 | 7 | 3,069 | 2 | 2 | daily |
| NERC | in_situ | in_situ | 2,013 | 2,014 | 2 | 624 | 4 | 4 | daily |
| Chao_Phraya_River | in_situ | in_situ | 1,912 | 2,020 | 109 | 348 | 7 | 7 | annual |

_Showing first 15 of 21 rows._

## Interpretation Notes

- **Main-track metrics** (Key Metrics, Main Source Contribution) are the primary reference for manuscript contribution claims.
- Record dominance in the merged table does not necessarily imply the broadest spatial footprint or the most scientifically useful data.
- Satellite source rows dominate the merged totals by record count, but their Q/SSL coverage is typically zero and SSC is sparse.
- Source classification is conservative; review `source_classification_template.csv` before using type/group proportions as final manuscript text.
- Satellite source datasets from Dethier and Shashi_Jianli report Q and SSC counts equal to total records as a best estimate; verify actual coverage in the NetCDF file.

## Figures

- `fig_climatology_contribution_records.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_contribution_records.png`
- `fig_climatology_contribution_reference_stations.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_contribution_reference_stations.png`
- `fig_climatology_contribution_stations.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_contribution_stations.png`
- `fig_climatology_resolution_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_resolution_stacked.png`
- `fig_climatology_temporal_coverage.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_temporal_coverage.png`
- `fig_climatology_variable_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_climatology_variable_stacked.png`
- `fig_satellite_contribution_records.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_contribution_records.png`
- `fig_satellite_contribution_reference_stations.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_contribution_reference_stations.png`
- `fig_satellite_contribution_stations.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_contribution_stations.png`
- `fig_satellite_resolution_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_resolution_stacked.png`
- `fig_satellite_temporal_coverage.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_temporal_coverage.png`
- `fig_satellite_variable_stacked.png`: `output_other/stats_release/source_contribution/figures/fig_satellite_variable_stacked.png`
- `fig_source_contribution_records.png`: `output_other/stats_release/source_contribution/figures/fig_source_contribution_records.png`
- `fig_source_contribution_reference_stations.png`: `output_other/stats_release/source_contribution/figures/fig_source_contribution_reference_stations.png`
- `fig_source_contribution_stations.png`: `output_other/stats_release/source_contribution/figures/fig_source_contribution_stations.png`
- `fig_source_cumulative_contribution.png`: `output_other/stats_release/source_contribution/figures/fig_source_cumulative_contribution.png`
- Additional figures: 5
