# S8 Source Contribution Statistics

Generated: 2026-06-03 18:23:11

## Data Sources

- `master_nc`: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/sed_reference_master.nc`
- `satellite_nc`: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/sed_reference_satellite.nc`
- `satellite_catalog`: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/satellite_catalog.csv`
- `climatology_nc`: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc`
- `source_station_catalog`: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/source_station_catalog.csv`
- `source_dataset_catalog`: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/source_dataset_catalog.csv`

## Statistical Scope

- `n_records` counts release-level records with provenance in the master/satellite/climatology products.
- `n_Q_records`, `n_SSC_records`, and `n_SSL_records` count valid values for each variable.
- `n_source_stations` counts source station identifiers before reference clustering.
- `n_clusters` counts reference clusters touched by each source where cluster identifiers are available.
- `source_type` and `source_group` are rule-inferred unless overridden by `--source-classification-csv`.

## Key Metrics

- Source datasets: 24
- Source stations summed across datasets: 52,920
- Cluster counts summed across datasets: 51,174
- Total records: 19,443,844
- Top 1 record share: 73.18%
- Top 5 record share: 98.08%
- Top 10 record share: 99.94%

## Main Insights

- Top source by records is `RiverSed` with 14,228,483 records (73.18% of all records).
- The top 5 sources contribute 98.08% of all records.
- Dominant source type is `satellite` with 16,507,059 records (84.90%).
- Dominant resolution is `daily` with 17,042,081 records.
- Best-covered variable is `SSC` with 4,187,064 valid records.
- Most limited variable is `Q` with 3,053,721 valid records.
- Overall temporal coverage spans 1912 to 2025 across sources with parseable dates.

## Contribution Concentration

| Cumulative threshold | Rank reached | Source at threshold | Cumulative records | Cumulative share |
| --- | --- | --- | --- | --- |
| 50% | 1 | RiverSed | 14,228,483 | 73.18% |
| 75% | 2 | GSED | 16,373,082 | 84.21% |
| 90% | 3 | USGS | 18,020,707 | 92.68% |
| 95% | 4 | HYDAT | 18,690,121 | 96.12% |
| 99% | 7 | HYBAM | 19,301,545 | 99.27% |

This concentration table is useful for explaining how strongly the release is dominated by the largest source datasets. It should be read together with the cluster and station tables, because record dominance does not necessarily imply the broadest spatial footprint.

## Dataset Rankings

### Top Sources by Records

| Source | Type | Group | Stations | Clusters | Records | Share | Q | SSC | SSL | Span | Resolutions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RiverSed | satellite | satellite products | 42,177 | 42,177 | 14,228,483 | 73.18% | 0 | 28,629 | 0 | 1984-2019 | daily |
| GSED | satellite | satellite products | 5,237 | 5,237 | 2,144,599 | 11.03% | 0 | 1,183,801 | 0 | 1985-2020 | monthly |
| USGS | in-situ | national agencies | 882 | 871 | 1,647,625 | 8.47% | 1,647,625 | 1,647,625 | 1,647,625 | 1980-2024 | daily |
| HYDAT | in-situ | national agencies | 505 | 503 | 669,414 | 3.44% | 663,567 | 669,414 | 663,567 | 1948-1997 | daily |
| Bayern | in-situ | national agencies | 34 | 32 | 380,719 | 1.96% | 380,719 | 380,719 | 380,719 | 1965-2025 | daily |
| Dethier | satellite | satellite products | 372 | 372 | 133,823 | 0.69% | 133,823 | 133,823 | 133,823 | 1984-2020 | monthly |
| HYBAM | in-situ | regional datasets | 12 | 12 | 96,882 | 0.50% | 96,876 | 11,834 | 96,882 | 1994-2024 | daily |
| EUSEDcollab | literature | global compilations | 244 | 243 | 66,608 | 0.34% | 66,565 | 66,455 | 66,608 | 1987-2021 | monthly |
| GFQA_v2 | in-situ | global compilations | 2,031 | 1,625 | 51,754 | 0.27% | 51,754 | 51,754 | 51,754 | 1912-2021 | daily\|monthly\|annual\|climatology |
| Mekong_Delta | literature | global compilations | 4 | 4 | 11,323 | 0.06% | 925 | 925 | 11,323 | 2005-2012 | daily |
| Robotham | literature | global compilations | 3 | 3 | 3,432 | 0.02% | 3,414 | 3,432 | 3,414 | 2016-2021 | daily |
| Eurasian_River | literature | global compilations | 17 | 17 | 3,204 | 0.02% | 3,204 | 3,193 | 3,204 | 1938-2000 | monthly |

### Top Sources by Source Stations

| Source | Type | Group | Stations | Clusters | Records | Share | Q | SSC | SSL | Span | Resolutions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RiverSed | satellite | satellite products | 42,177 | 42,177 | 14,228,483 | 73.18% | 0 | 28,629 | 0 | 1984-2019 | daily |
| GSED | satellite | satellite products | 5,237 | 5,237 | 2,144,599 | 11.03% | 0 | 1,183,801 | 0 | 1985-2020 | monthly |
| GFQA_v2 | in-situ | global compilations | 2,031 | 1,625 | 51,754 | 0.27% | 51,754 | 51,754 | 51,754 | 1912-2021 | daily\|monthly\|annual\|climatology |
| USGS | in-situ | national agencies | 882 | 871 | 1,647,625 | 8.47% | 1,647,625 | 1,647,625 | 1,647,625 | 1980-2024 | daily |
| Milliman | climatology | global compilations | 737 | 0 | 737 | 0.00% | 737 | 735 | 737 | 1912-2021 | climatology |
| Vanmaercke | climatology | global compilations | 516 | 0 | 516 | 0.00% | 0 | 0 | 516 | 1912-2021 | climatology |
| HYDAT | in-situ | national agencies | 505 | 503 | 669,414 | 3.44% | 663,567 | 669,414 | 663,567 | 1948-1997 | daily |
| Dethier | satellite | satellite products | 372 | 372 | 133,823 | 0.69% | 133,823 | 133,823 | 133,823 | 1984-2020 | monthly |
| EUSEDcollab | literature | global compilations | 244 | 243 | 66,608 | 0.34% | 66,565 | 66,455 | 66,608 | 1987-2021 | monthly |
| Huanghe | literature | global compilations | 48 | 24 | 144 | 0.00% | 0 | 144 | 0 | 1912-2021 | annual\|climatology |
| Bayern | in-situ | national agencies | 34 | 32 | 380,719 | 1.96% | 380,719 | 380,719 | 380,719 | 1965-2025 | daily |
| HMA | climatology | global compilations | 28 | 0 | 28 | 0.00% | 28 | 28 | 28 | 1912-2021 | climatology |

### Top Sources by Clusters

| Source | Type | Group | Stations | Clusters | Records | Share | Q | SSC | SSL | Span | Resolutions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RiverSed | satellite | satellite products | 42,177 | 42,177 | 14,228,483 | 73.18% | 0 | 28,629 | 0 | 1984-2019 | daily |
| GSED | satellite | satellite products | 5,237 | 5,237 | 2,144,599 | 11.03% | 0 | 1,183,801 | 0 | 1985-2020 | monthly |
| GFQA_v2 | in-situ | global compilations | 2,031 | 1,625 | 51,754 | 0.27% | 51,754 | 51,754 | 51,754 | 1912-2021 | daily\|monthly\|annual\|climatology |
| USGS | in-situ | national agencies | 882 | 871 | 1,647,625 | 8.47% | 1,647,625 | 1,647,625 | 1,647,625 | 1980-2024 | daily |
| HYDAT | in-situ | national agencies | 505 | 503 | 669,414 | 3.44% | 663,567 | 669,414 | 663,567 | 1948-1997 | daily |
| Dethier | satellite | satellite products | 372 | 372 | 133,823 | 0.69% | 133,823 | 133,823 | 133,823 | 1984-2020 | monthly |
| EUSEDcollab | literature | global compilations | 244 | 243 | 66,608 | 0.34% | 66,565 | 66,455 | 66,608 | 1987-2021 | monthly |
| Bayern | in-situ | national agencies | 34 | 32 | 380,719 | 1.96% | 380,719 | 380,719 | 380,719 | 1965-2025 | daily |
| Huanghe | literature | global compilations | 48 | 24 | 144 | 0.00% | 0 | 144 | 0 | 1912-2021 | annual\|climatology |
| Yajiang | literature | global compilations | 23 | 23 | 23 | 0.00% | 14 | 23 | 14 | 2019-2020 | daily |
| Eurasian_River | literature | global compilations | 17 | 17 | 3,204 | 0.02% | 3,204 | 3,193 | 3,204 | 1938-2000 | monthly |
| HYBAM | in-situ | regional datasets | 12 | 12 | 96,882 | 0.50% | 96,876 | 11,834 | 96,882 | 1994-2024 | daily |

## Source Classes

### Source Type Contribution

| Type | Sources | Stations | Clusters | Records | Share | Q | SSC | SSL | Span | Resolutions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| satellite | 4 | 47,788 | 47,788 | 16,507,059 | 84.90% | 133,977 | 1,346,407 | 133,977 | 1984-2023 | daily\|monthly |
| in-situ | 5 | 3,464 | 3,043 | 2,846,394 | 14.64% | 2,840,541 | 2,761,346 | 2,840,547 | 1912-2025 | daily\|monthly\|annual\|climatology |
| literature | 11 | 370 | 343 | 89,093 | 0.46% | 78,421 | 78,531 | 88,862 | 1912-2021 | daily\|monthly\|annual\|climatology |
| climatology | 4 | 1,298 | 0 | 1,298 | 0.01% | 782 | 780 | 1,298 | 1912-2021 | climatology |

### Source Group Contribution

| Group | Sources | Stations | Clusters | Records | Share | Q | SSC | SSL | Span | Resolutions |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| satellite products | 4 | 47,788 | 47,788 | 16,507,059 | 84.90% | 133,977 | 1,346,407 | 133,977 | 1984-2023 | daily\|monthly |
| national agencies | 3 | 1,421 | 1,406 | 2,697,758 | 13.87% | 2,691,911 | 2,697,758 | 2,691,911 | 1948-2025 | daily |
| global compilations | 16 | 3,699 | 1,968 | 142,145 | 0.73% | 130,957 | 131,065 | 141,914 | 1912-2021 | daily\|monthly\|annual\|climatology |
| regional datasets | 1 | 12 | 12 | 96,882 | 0.50% | 96,876 | 11,834 | 96,882 | 1994-2024 | daily |

Classification is intentionally conservative. The generated `source_classification_template.csv` should be reviewed before using the type/group proportions as final manuscript statements.

## Resolution and Variable Structure

### Top Source-Resolution Rows

| Source | Resolution | Type | Stations | Clusters | Records | Global share | Within source | Span |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| RiverSed | daily | satellite | 42,177 | 42,177 | 14,228,483 | 73.18% | 100.00% | 1984-2019 |
| GSED | monthly | satellite | 5,237 | 5,237 | 2,144,599 | 11.03% | 100.00% | 1985-2020 |
| USGS | daily | in-situ | 882 | 871 | 1,647,625 | 8.47% | 100.00% | 1980-2024 |
| HYDAT | daily | in-situ | 505 | 503 | 669,414 | 3.44% | 100.00% | 1948-1997 |
| Bayern | daily | in-situ | 34 | 32 | 380,719 | 1.96% | 100.00% | 1965-2025 |
| Dethier | monthly | satellite | 372 | 372 | 133,823 | 0.69% | 100.00% | 1984-2020 |
| HYBAM | daily | in-situ | 12 | 12 | 96,882 | 0.50% | 100.00% | 1994-2024 |
| EUSEDcollab | monthly | literature | 244 | 243 | 66,608 | 0.34% | 100.00% | 1987-2021 |
| GFQA_v2 | monthly | in-situ | 1,962 | 1,601 | 51,551 | 0.27% | 99.61% | 1995-2021 |
| Mekong_Delta | daily | literature | 4 | 4 | 11,323 | 0.06% | 100.00% | 2005-2012 |
| Robotham | daily | literature | 3 | 3 | 3,432 | 0.02% | 100.00% | 2016-2021 |
| Eurasian_River | monthly | literature | 17 | 17 | 3,204 | 0.02% | 100.00% | 1938-2000 |
| Fukushima | daily | literature | 2 | 2 | 3,069 | 0.02% | 100.00% | 2012-2018 |
| Milliman | climatology | climatology | 737 | 0 | 737 | 0.00% | 100.00% | 1912-2021 |
| NERC | daily | literature | 4 | 4 | 624 | 0.00% | 100.00% | 2013-2014 |
| Vanmaercke | climatology | climatology | 516 | 0 | 516 | 0.00% | 100.00% | 1912-2021 |
| Chao_Phraya_River | annual | literature | 7 | 7 | 348 | 0.00% | 100.00% | 1912-2020 |
| Rhine | daily | literature | 12 | 12 | 312 | 0.00% | 100.00% | 1990-2011 |
| Shashi_Jianli | daily | satellite | 2 | 2 | 154 | 0.00% | 100.00% | 2016-2023 |
| GFQA_v2 | annual | in-situ | 27 | 27 | 151 | 0.00% | 0.29% | 2012-2021 |

### Top Source-Variable Rows

| Source | Variable | Type | Variable records | Source records | Variable share | Within source |
| --- | --- | --- | --- | --- | --- | --- |
| USGS | Q | in-situ | 1,647,625 | 1,647,625 | 53.95% | 100.00% |
| USGS | SSC | in-situ | 1,647,625 | 1,647,625 | 39.35% | 100.00% |
| USGS | SSL | in-situ | 1,647,625 | 1,647,625 | 53.76% | 100.00% |
| GSED | SSC | satellite | 1,183,801 | 2,144,599 | 28.27% | 55.20% |
| HYDAT | SSC | in-situ | 669,414 | 669,414 | 15.99% | 100.00% |
| HYDAT | Q | in-situ | 663,567 | 669,414 | 21.73% | 99.13% |
| HYDAT | SSL | in-situ | 663,567 | 669,414 | 21.65% | 99.13% |
| Bayern | Q | in-situ | 380,719 | 380,719 | 12.47% | 100.00% |
| Bayern | SSC | in-situ | 380,719 | 380,719 | 9.09% | 100.00% |
| Bayern | SSL | in-situ | 380,719 | 380,719 | 12.42% | 100.00% |
| Dethier | Q | satellite | 133,823 | 133,823 | 4.38% | 100.00% |
| Dethier | SSC | satellite | 133,823 | 133,823 | 3.20% | 100.00% |
| Dethier | SSL | satellite | 133,823 | 133,823 | 4.37% | 100.00% |
| HYBAM | SSL | in-situ | 96,882 | 96,882 | 3.16% | 100.00% |
| HYBAM | Q | in-situ | 96,876 | 96,882 | 3.17% | 99.99% |
| EUSEDcollab | SSL | literature | 66,608 | 66,608 | 2.17% | 100.00% |
| EUSEDcollab | Q | literature | 66,565 | 66,608 | 2.18% | 99.94% |
| EUSEDcollab | SSC | literature | 66,455 | 66,608 | 1.59% | 99.77% |
| GFQA_v2 | Q | in-situ | 51,754 | 51,754 | 1.69% | 100.00% |
| GFQA_v2 | SSC | in-situ | 51,754 | 51,754 | 1.24% | 100.00% |

The variable table helps distinguish sources that contribute dense discharge records from those that also carry SSC or SSL observations. This distinction is important because Q coverage can dominate total records even when sediment-variable coverage is much smaller.

## Temporal Span by Source

| Source | Type | Span | Years | Records | Stations | Clusters | Resolutions |
| --- | --- | --- | --- | --- | --- | --- | --- |
| RiverSed | satellite | 1984-2019 | 36 | 14,228,483 | 42,177 | 42,177 | daily |
| GSED | satellite | 1985-2020 | 36 | 2,144,599 | 5,237 | 5,237 | monthly |
| USGS | in-situ | 1980-2024 | 45 | 1,647,625 | 882 | 871 | daily |
| HYDAT | in-situ | 1948-1997 | 50 | 669,414 | 505 | 503 | daily |
| Bayern | in-situ | 1965-2025 | 61 | 380,719 | 34 | 32 | daily |
| Dethier | satellite | 1984-2020 | 37 | 133,823 | 372 | 372 | monthly |
| HYBAM | in-situ | 1994-2024 | 31 | 96,882 | 12 | 12 | daily |
| EUSEDcollab | literature | 1987-2021 | 35 | 66,608 | 244 | 243 | monthly |
| GFQA_v2 | in-situ | 1912-2021 | 110 | 51,754 | 2,031 | 1,625 | daily\|monthly\|annual\|climatology |
| Mekong_Delta | literature | 2005-2012 | 8 | 11,323 | 4 | 4 | daily |
| Robotham | literature | 2016-2021 | 6 | 3,432 | 3 | 3 | daily |
| Eurasian_River | literature | 1938-2000 | 63 | 3,204 | 17 | 17 | monthly |
| Fukushima | literature | 2012-2018 | 7 | 3,069 | 2 | 2 | daily |
| Milliman | climatology | 1912-2021 | 110 | 737 | 737 | 0 | climatology |
| NERC | literature | 2013-2014 | 2 | 624 | 4 | 4 | daily |

Temporal span is source-level and should be interpreted with record density. Long calendar span with few records is not equivalent to dense continuous sampling.

## Figures

### Source contribution by records

![Source contribution by records](../figures/fig_source_contribution_records.png)

### Source contribution by source stations

![Source contribution by source stations](../figures/fig_source_contribution_stations.png)

### Source contribution by clusters

![Source contribution by clusters](../figures/fig_source_contribution_clusters.png)

### Source type contribution by records

![Source type contribution by records](../figures/fig_source_type_records.png)

### Source group contribution by records

![Source group contribution by records](../figures/fig_source_group_records.png)

### Source resolution contribution

![Source resolution contribution](../figures/fig_source_resolution_stacked.png)

### Source variable coverage

![Source variable coverage](../figures/fig_source_variable_stacked.png)

### Cumulative source contribution

![Cumulative source contribution](../figures/fig_source_cumulative_contribution.png)

### Temporal coverage

![Temporal coverage](../figures/fig_source_temporal_coverage.png)

### Satellite contribution by records

![Satellite contribution by records](../figures/fig_satellite_contribution_records.png)

### Satellite contribution by source stations

![Satellite contribution by source stations](../figures/fig_satellite_contribution_stations.png)

### Satellite contribution by clusters

![Satellite contribution by clusters](../figures/fig_satellite_contribution_clusters.png)

### Satellite resolution contribution

![Satellite resolution contribution](../figures/fig_satellite_resolution_stacked.png)

### Satellite variable coverage

![Satellite variable coverage](../figures/fig_satellite_variable_stacked.png)

### Satellite temporal coverage

![Satellite temporal coverage](../figures/fig_satellite_temporal_coverage.png)

### Climatology contribution by records

![Climatology contribution by records](../figures/fig_climatology_contribution_records.png)

### Climatology contribution by source stations

![Climatology contribution by source stations](../figures/fig_climatology_contribution_stations.png)

### Climatology contribution by clusters

![Climatology contribution by clusters](../figures/fig_climatology_contribution_clusters.png)

### Climatology resolution contribution

![Climatology resolution contribution](../figures/fig_climatology_resolution_stacked.png)

### Climatology variable coverage

![Climatology variable coverage](../figures/fig_climatology_variable_stacked.png)

### Climatology temporal coverage

![Climatology temporal coverage](../figures/fig_climatology_temporal_coverage.png)

## Output Tables

- `table_source_dataset_contribution.csv`: `../tables/table_source_dataset_contribution.csv`
- `table_source_type_contribution.csv`: `../tables/table_source_type_contribution.csv`
- `table_source_resolution_contribution.csv`: `../tables/table_source_resolution_contribution.csv`
- `table_source_variable_contribution.csv`: `../tables/table_source_variable_contribution.csv`
- `table_top_source_contributors.csv`: `../tables/table_top_source_contributors.csv`
- `table_source_contribution_cumulative.csv`: `../tables/table_source_contribution_cumulative.csv`
- `table_source_temporal_coverage.csv`: `../tables/table_source_temporal_coverage.csv`
- `table_report_key_metrics.csv`: `../tables/table_report_key_metrics.csv`
- `source_classification_template.csv`: `../tables/source_classification_template.csv`

## Notes and Limitations

- Source classification is conservative and should be reviewed with `source_classification_template.csv` before manuscript use.
- Climatology is summarized as a standalone release product and assigned the `climatology` resolution.
- Satellite observations are summarized from the validation-only release sidecar and kept available as separate satellite figures.
- Cluster counts are source-level counts and can sum to more than the unique release cluster count because several sources can contribute to the same cluster.
- The report describes release-product statistics only and does not infer scientific causality.
