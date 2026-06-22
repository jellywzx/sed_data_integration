# Minimal Release Manuscript Statistics

## Run Identity

- Minimal release package: `output/sed_reference_release_minimal`
- Stats output: `output_other/stats_release_minimal`
- Run started UTC: `2026-06-19T16:33:54+00:00`
- Run finished UTC: `2026-06-19T16:35:14+00:00`
- Release fingerprint: `460bca8dbbd12090178e55fdb16dda08c609ebe3ee576f06b04f1951a4d8f6d2`
- Stats script fingerprint: `bb07b16aa81017fe6d127fcb07f7a315b50e7557d73bed3c6ac78775a09fa236`

## Headline

- Files in minimal package: 12
- Minimal package size: 4,617.63 MB
- Matrix resolutions: 3
- Matrix station rows across resolutions: 3,540
- Matrix nonempty station-time cells: 2,847,547
- Source datasets listed: 25
- Extension records: 16,507,783

## Overview Metrics

| metric | value |
|---|---|
| minimal_files | 12 |
| minimal_size_mb | 4,618 |
| matrix_resolutions | 3 |
| matrix_stations_sum | 3,540 |
| matrix_nonempty_cells_sum | 2,847,547 |
| source_datasets | 25 |
| source_dataset_records_sum | 19,355,330 |
| catalog_rows_total | 55,277 |
| extension_products | 2 |
| extension_records_sum | 16,507,783 |
| matrix_Q_present_sum | 2,830,490 |
| matrix_SSC_present_sum | 2,836,984 |
| matrix_SSL_present_sum | 2,837,795 |

## Matrix Resolution Summary

| resolution | n stations | n time steps | n cells | n nonempty cells | nonempty percent of cells | time start | time end |
|---|---|---|---|---|---|---|---|
| daily | 1,607 | 25,775 | 41,420,425 | 2,724,382 | 6.58% | 1948-05-25 | 2025-10-21 |
| monthly | 1,875 | 11,533 | 21,624,375 | 122,546 | 0.57% | 1938-01-15 | 2021-12-24 |
| annual | 58 | 239 | 13,862 | 619 | 4.47% | 1912-01-01 | 2021-09-28 |

## Matrix Variable Summary

| resolution | variable | n present | n good | n estimated | n usable | stations with present | present percent of cells | mean | min | max | unit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | SSL | 2,717,886 | 11,383 | 2,594,090 | 2,605,473 | 1,571 | 6.56% | 6,865 | 0 | 46,974,252 | ton day-1 |
| daily | SSC | 2,713,984 | 2,619,527 | 925 | 2,620,452 | 1,607 | 6.55% | 458.65 | 0 | 4,300,000 | mg L-1 |
| daily | Q | 2,707,488 | 2,657,915 | 0 | 2,657,915 | 1,571 | 6.54% | 329.53 | 0 | 260,100 | m3 s-1 |
| monthly | Q | 122,503 | 120,366 | 0 | 120,366 | 1,875 | 0.57% | 271.18 | 0 | 4,972,518 | m3 s-1 |
| monthly | SSC | 122,381 | 89,521 | 26,835 | 116,356 | 1,875 | 0.57% | 1,670 | 0 | 424,904 | mg L-1 |
| monthly | SSL | 119,410 | 62,219 | 54,435 | 116,654 | 1,875 | 0.55% | 975.95 | 0 | 4,296,256 | ton day-1 |
| annual | SSC | 619 | 251 | 348 | 599 | 58 | 4.47% | 2,151 | 0 | 141,000 | mg L-1 |
| annual | Q | 499 | 488 | 0 | 488 | 34 | 3.60% | 169.65 | 0 | 1,144 | m3 s-1 |
| annual | SSL | 499 | 340 | 149 | 489 | 34 | 3.60% | 2,706 | 0 | 55,688 | ton day-1 |

## Matrix Co-Location Summary

| resolution | combination | n cells | stations with combination | percent of nonempty cells |
|---|---|---|---|---|
| daily | Q+SSC+SSL | 2,707,488 | 1,571 | 99.38% |
| monthly | Q+SSC+SSL | 119,243 | 1,875 | 97.30% |
| daily | SSL only | 10,398 | 4 | 0.38% |
| daily | SSC only | 6,496 | 113 | 0.24% |
| monthly | Q+SSC | 3,136 | 7 | 2.56% |
| annual | Q+SSC+SSL | 499 | 34 | 80.61% |
| monthly | Q+SSL | 124 | 11 | 0.10% |
| annual | SSC only | 120 | 24 | 19.39% |
| monthly | SSL only | 41 | 2 | 0.03% |
| monthly | SSC+SSL | 2 | 1 | 0.00% |

## Catalog Summary

| table | resolution | rows | unique cluster uid | record count sum | time start | time end |
|---|---|---|---|---|---|---|
| satellite_catalog | daily | 47,785 | 47,785 | 16,506,461 | 1984-01-15 | 2020-12-15 |
| station_catalog | daily | 1,607 | 1,607 | 2,724,382 | 1948-05-25 | 2025-10-21 |
| source_station_catalog | daily | 1,644 | 1,607 | 2,724,382 | 1948-05-25 | 2025-10-21 |
| station_catalog | monthly | 1,875 | 1,875 | 122,546 | 1938-01-15 | 2021-12-24 |
| source_station_catalog | monthly | 2,250 | 1,875 | 122,546 | 1938-01-15 | 2021-12-24 |
| station_catalog | annual | 58 | 58 | 619 | 1912-01-01 | 2021-09-28 |
| source_station_catalog | annual | 58 | 58 | 619 | 1912-01-01 | 2021-09-28 |

## Top Source Datasets

| source name | n source stations | n records | source url |
|---|---|---|---|
| RiverSed (USA) | 42,177 | 14,228,483 | https://doi.org/10.5281/zenodo.10842637 |
| GSED | 5,237 | 2,144,599 | https://eartharxiv.org/repository/view/ |
| USGS NWIS | 885 | 1,651,590 | https://waterdata.usgs.gov/nwis |
| HYDAT | 501 | 661,138 | https://www.canada.ca/en/environment-climate-change/services/water-overview/quantity/monitoring/survey/data-products-services/national-archive-hydat.html |
| Bayern | 34 | 380,719 | https://www.gkd.bayern.de/en/ |
| Dethier | 371 | 133,379 | https://doi.org/10.1038/s41561-022-01016-0 |
| EUSEDcollab | 226 | 63,208 | https://esdac.jrc.ec.europa.eu/content/european-sediment-collaboration-eusedcollab-database |
| GFQA_v2 | 2,062 | 56,297 | https://doi.org/10.5281/zenodo.14230628 |
| HYBAM | 12 | 11,826 | http://www.ore-hybam.org |
| Mekong Delta | 4 | 11,323 | https://doi.org/10.5285/ac5b28ca-e087-4aec-974a-5a9f84b06595 |
| Robotham | 3 | 3,432 | https://doi.org/10.5285/9f80e349-0594-4ae1-bff3-b055638569f8 |
| Eurasian Dataset | 17 | 3,204 | https://doi.org/10.5065/D6F769PB |
| Fukushima | 2 | 3,069 | https://doi.org/10.34355/CRiED.U.Tsukuba.00147 |
| Milliman & Farnsworth | 737 | 737 | https://doi.org/10.1126/science.abn7980 |
| NERC-Hampshire Avon | 4 | 624 | https://doi.org/10.5285/0dd10858-7b96-41f1-8db5-e7b4c4168af5 |
| Vanmaercke et al. | 516 | 516 | https://doi.org/10.1016/j.earscirev.2014.06.004 |
| Chao Phraya River | 7 | 348 | https://doi.org/10.1594/PANGAEA.981111 |
| Rhine | 12 | 312 | https://doi.org/10.1002/hyp.70070 |
| Shashi_Jianli | 2 | 154 | https://doi.org/10.1007/s11600-025-01638-x |
| GloRiSe v1.1 | 128 | 154 | https://doi.org/10.5281/zenodo.4485795 |

_Showing first 20 of 25 rows._

## Extension Products

| product | n stations | n records | time start | time end |
|---|---|---|---|---|
| satellite | 47,785 | 16,506,461 | 1984-01-15 | 2020-12-15 |
| climatology | 1,322 | 1,322 | 1912-07-01 | 2010-07-01 |

## Extension Variable Summary

| product | variable | n records | n present | n good | n estimated | n usable | present percent | unit |
|---|---|---|---|---|---|---|---|---|
| satellite | SSC | 16,506,461 | 1,345,809 | 1,326,713 | 0 | 1,326,713 | 8.15% | mg L-1 |
| satellite | Q | 16,506,461 | 133,379 | 132,172 | 0 | 132,172 | 0.81% | m3 s-1 |
| satellite | SSL | 16,506,461 | 133,379 | 132,259 | 0 | 132,259 | 0.81% | ton day-1 |
| climatology | SSL | 1,322 | 1,298 | 1,298 | 0 | 1,298 | 98.18% | ton day-1 |
| climatology | SSC | 1,322 | 804 | 787 | 17 | 804 | 60.82% | mg L-1 |
| climatology | Q | 1,322 | 782 | 782 | 0 | 782 | 59.15% | m3 s-1 |

## Satellite Source By Variable

| source name | variable | n records | n present | n good | n estimated | n usable | present percent | unit |
|---|---|---|---|---|---|---|---|---|
| RiverSed | Q | 14,228,483 | 0 | 0 | 0 | 0 | 0% | m3 s-1 |
| RiverSed | SSC | 14,228,483 | 28,629 | 27,872 | 0 | 27,872 | 0.20% | mg L-1 |
| RiverSed | SSL | 14,228,483 | 0 | 0 | 0 | 0 | 0% | ton day-1 |
| GSED | Q | 2,144,599 | 0 | 0 | 0 | 0 | 0% | m3 s-1 |
| GSED | SSC | 2,144,599 | 1,183,801 | 1,169,955 | 0 | 1,169,955 | 55.20% | mg L-1 |
| GSED | SSL | 2,144,599 | 0 | 0 | 0 | 0 | 0% | ton day-1 |
| Dethier | Q | 133,379 | 133,379 | 132,172 | 0 | 132,172 | 100% | m3 s-1 |
| Dethier | SSC | 133,379 | 133,379 | 128,886 | 0 | 128,886 | 100% | mg L-1 |
| Dethier | SSL | 133,379 | 133,379 | 132,259 | 0 | 132,259 | 100% | ton day-1 |

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
