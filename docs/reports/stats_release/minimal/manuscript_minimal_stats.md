# Minimal Release Manuscript Statistics

## Run Identity

- Minimal release package: `output/sed_reference_release_minimal`
- Stats output: `output_other/stats_release_minimal`
- Run started UTC: `2026-08-11T04:08:22+00:00`
- Run finished UTC: `2026-08-11T04:11:15+00:00`
- Release fingerprint: `71527f6163ceaaee8796759b9b49391bc89be84f7e0040b7d53f432d673c0bef`
- Stats script fingerprint: `1b3c0e57dbb47fa2c15704ed4da6420b0b9436208fe1d85b6f0d4318b538563e`

## Headline

- Files in minimal package: 13
- Minimal package size: 6,599.83 MB
- Matrix resolutions: 3
- Matrix station rows across resolutions: 7,463
- Matrix nonempty station-time cells: 3,064,671
- Source datasets listed: 25
- Extension records: 16,479,637

## Overview Metrics

| metric | value |
|---|---|
| minimal_files | 13 |
| minimal_size_mb | 6,600 |
| matrix_resolutions | 3 |
| matrix_stations_sum | 7,463 |
| matrix_nonempty_cells_sum | 3,064,671 |
| source_datasets | 25 |
| source_dataset_records_sum | 19,544,308 |
| catalog_rows_total | 53,730 |
| extension_products | 2 |
| extension_records_sum | 16,479,637 |
| matrix_Q_present_sum | 2,852,106 |
| matrix_SSC_present_sum | 3,054,050 |
| matrix_SSL_present_sum | 2,862,606 |

## Matrix Resolution Summary

| resolution | n stations | n time steps | n cells | n nonempty cells | nonempty percent of cells | time start | time end |
|---|---|---|---|---|---|---|---|
| daily | 4,717 | 25,775 | 121,580,675 | 2,963,235 | 2.44% | 1948-05-25 | 2025-10-21 |
| monthly | 2,697 | 5,602 | 15,108,594 | 100,901 | 0.67% | 1938-01-15 | 2023-05-08 |
| annual | 49 | 157 | 7,693 | 535 | 6.95% | 1912-01-01 | 2022-02-10 |

## Matrix Variable Summary

| resolution | variable | n present | n good | n estimated | n usable | stations with present | present percent of cells | mean | min | max | unit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | SSC | 2,952,708 | 2,838,581 | 9,224 | 2,847,805 | 4,717 | 2.43% | 793.94 | 0 | 1,043,519,680 | mg L-1 |
| daily | SSL | 2,814,621 | 11,464 | 2,660,014 | 2,671,478 | 2,481 | 2.32% | 6,430 | 0 | 46,974,252 | t d-1 |
| daily | Q | 2,804,180 | 2,744,728 | 0 | 2,744,728 | 2,481 | 2.31% | 309.44 | 0 | 260,100 | m3 s-1 |
| monthly | SSC | 100,807 | 76,530 | 18,563 | 95,093 | 2,697 | 0.67% | 1,951 | 0 | 285,436 | mg L-1 |
| monthly | SSL | 47,637 | 3,296 | 40,485 | 43,781 | 1,240 | 0.32% | 2,224 | 0 | 4,296,256 | t d-1 |
| monthly | Q | 47,578 | 46,358 | 0 | 46,358 | 1,240 | 0.31% | 643.35 | 0 | 4,972,518 | m3 s-1 |
| annual | SSC | 535 | 176 | 336 | 512 | 49 | 6.95% | 2,527 | 0 | 141,000 | mg L-1 |
| annual | Q | 348 | 342 | 0 | 342 | 7 | 4.52% | 222.97 | 1.27 | 1,144 | m3 s-1 |
| annual | SSL | 348 | 340 | 0 | 340 | 7 | 4.52% | 3,755 | 0 | 55,688 | t d-1 |

## Matrix Co-Location Summary

| resolution | combination | n cells | stations with combination | percent of nonempty cells |
|---|---|---|---|---|
| daily | Q+SSC+SSL | 2,804,092 | 2,481 | 94.63% |
| daily | SSC only | 148,614 | 3,143 | 5.02% |
| monthly | SSC only | 53,264 | 2,178 | 52.79% |
| monthly | Q+SSC+SSL | 47,543 | 1,240 | 47.12% |
| daily | SSL only | 10,439 | 6 | 0.35% |
| annual | Q+SSC+SSL | 348 | 7 | 65.05% |
| annual | SSC only | 187 | 42 | 34.95% |
| daily | Q+SSL | 88 | 2 | 0.00% |
| monthly | SSL only | 59 | 8 | 0.06% |
| monthly | Q+SSL | 35 | 10 | 0.03% |
| daily | SSC+SSL | 2 | 1 | 0.00% |

## Catalog Summary

| table | resolution | rows | unique station uid | record count sum | time start | time end |
|---|---|---|---|---|---|---|
| satellite_catalog | daily | 38,550 | 181 | 16,478,276 | 1984-01-15 | 2020-12-15 |
| station_catalog | daily | 4,717 | 4,717 | 2,963,235 | 1948-05-25 | 2025-10-21 |
| source_station_catalog | daily | 4,875 | 4,717 | 2,963,235 | 1948-05-25 | 2025-10-21 |
| station_catalog | monthly | 2,697 | 2,697 | 100,901 | 1938-01-15 | 2023-05-08 |
| source_station_catalog | monthly | 2,793 | 2,697 | 100,901 | 1938-01-15 | 2023-05-08 |
| station_catalog | annual | 49 | 49 | 535 | 1912-01-01 | 2022-02-10 |
| source_station_catalog | annual | 49 | 49 | 535 | 1912-01-01 | 2022-02-10 |

## Top Source Datasets

| source name | n source stations | n records | source url |
|---|---|---|---|
| RiverSed (USA) | 32,941 | 14,199,854 | https://doi.org/10.5281/zenodo.7938267 |
| GSED | 5,237 | 2,144,599 | https://figshare.com/s/dde3bffd8e12227e2b26 |
| USGS NWIS | 890 | 1,685,357 | https://doi.org/10.5066/F7P55KJN |
| HYDAT | 541 | 671,979 | https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html |
| Bayern | 37 | 421,052 | https://www.gkd.bayern.de/ |
| GFQA_v2 | 5,811 | 186,867 | https://doi.org/10.5281/zenodo.14230628 |
| Dethier | 372 | 133,823 | https://doi.org/10.1126/science.abn7980 |
| EUSEDcollab | 244 | 66,637 | https://esdac.jrc.ec.europa.eu/content/european-sediment-collaboration-eusedcollab-database |
| Mekong Delta | 4 | 11,921 | https://doi.org/10.5285/ac5b28ca-e087-4aec-974a-5a9f84b06595 |
| HYBAM | 12 | 9,404 | https://hybam.obs-mip.fr/ |
| Robotham | 3 | 3,432 | https://doi.org/10.5285/9f80e349-0594-4ae1-bff3-b055638569f8 |
| Eurasian Dataset | 17 | 3,263 | https://doi.org/10.5065/D6F769PB |
| Fukushima | 2 | 3,069 | https://doi.org/10.34355/CRiED.U.Tsukuba.00147 |
| Milliman & Farnsworth | 776 | 776 | https://doi.org/10.1017/CBO9780511781247 |
| NERC-Hampshire Avon | 4 | 624 | https://doi.org/10.5285/0dd10858-7b96-41f1-8db5-e7b4c4168af5 |
| Vanmaercke et al. | 516 | 516 | https://doi.org/10.1016/j.earscirev.2014.06.004 |
| Chao Phraya River | 7 | 348 | https://doi.org/10.1594/PANGAEA.981111 |
| Rhine | 12 | 312 | https://doi.org/10.1002/hyp.70070 |
| Shashi_Jianli | 2 | 154 | https://doi.org/10.1007/s11600-025-01638-x |
| Huanghe (Yellow River) | 48 | 144 | https://doi.org/10.12072/ncdc.YRiver.db0054.2021 |

_Showing first 20 of 25 rows._

## Extension Products

| product | n stations | n records | time start | time end |
|---|---|---|---|---|
| satellite | 38,550 | 16,478,276 | 1984-01-15 | 2020-12-15 |
| climatology | 1,361 | 1,361 | 1912-07-01 | 2010-07-01 |

## Extension Variable Summary

| product | variable | n records | n present | n good | n estimated | n usable | present percent | unit |
|---|---|---|---|---|---|---|---|---|
| satellite | SSC | 16,478,276 | 15,517,478 | 15,121,092 | 0 | 15,121,092 | 94.17% | mg L-1 |
| satellite | Q | 16,478,276 | 133,823 | 132,614 | 0 | 132,614 | 0.81% | m3 s-1 |
| satellite | SSL | 16,478,276 | 133,823 | 132,693 | 0 | 132,693 | 0.81% | t d-1 |
| climatology | SSL | 1,361 | 1,337 | 1,337 | 0 | 1,337 | 98.24% | t d-1 |
| climatology | SSC | 1,361 | 806 | 759 | 47 | 806 | 59.22% | mg L-1 |
| climatology | Q | 1,361 | 782 | 782 | 0 | 782 | 57.46% | m3 s-1 |

## Satellite Source By Variable

| source name | variable | n records | n present | n good | n estimated | n usable | present percent | unit |
|---|---|---|---|---|---|---|---|---|
| RiverSed | Q | 14,199,854 | 0 | 0 | 0 | 0 | 0% | m3 s-1 |
| RiverSed | SSC | 14,199,854 | 14,199,854 | 13,821,824 | 0 | 13,821,824 | 100% | mg L-1 |
| RiverSed | SSL | 14,199,854 | 0 | 0 | 0 | 0 | 0% | t d-1 |
| GSED | Q | 2,144,599 | 0 | 0 | 0 | 0 | 0% | m3 s-1 |
| GSED | SSC | 2,144,599 | 1,183,801 | 1,169,955 | 0 | 1,169,955 | 55.20% | mg L-1 |
| GSED | SSL | 2,144,599 | 0 | 0 | 0 | 0 | 0% | t d-1 |
| Dethier | Q | 133,823 | 133,823 | 132,614 | 0 | 132,614 | 100% | m3 s-1 |
| Dethier | SSC | 133,823 | 133,823 | 129,313 | 0 | 129,313 | 100% | mg L-1 |
| Dethier | SSL | 133,823 | 133,823 | 132,693 | 0 | 132,693 | 100% | t d-1 |

## Output Tables

- `file_inventory.csv`
- `catalog_summary.csv`
- `source_dataset_summary.csv`
- `matrix_resolution_summary.csv`
- `matrix_variable_summary.csv`
- `matrix_colocation_summary.csv`
- `qc_flag_counts.csv`
- `extension_product_summary.csv`
- `extension_variable_summary.csv`
- `extension_source_variable_summary.csv`
- `minimal_release_overview.csv`
- `run_manifest.csv`

## Interpretation Notes

- Matrix denominators are station-time cells, not source-record rows.
- `n_nonempty_cells` counts cells where at least one of Q, SSC, or SSL is present.
- `n_usable` counts present values with flag 0 or 1, matching the good-or-estimated interpretation used elsewhere in release statistics.
- Satellite rows are validation-only and should be filtered by source and variable before analysis.
