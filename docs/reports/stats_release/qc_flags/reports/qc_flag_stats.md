# Release QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 81,046,414
- Final flag rows: 29
- Stage flag rows: 26
- Usable flag count from health KPIs: 30,696,255
- Problem flag count from health KPIs: 48,215,876
- Stage-effectiveness rows available: 8

## Flag Schema

| product | flag variable | flag value | flag meaning | long name |
|---|---|---|---|---|
| master | Q_flag | 0 | good | quality flag for river discharge |
| master | Q_flag | 1 | estimated | quality flag for river discharge |
| master | Q_flag | 2 | suspect | quality flag for river discharge |
| master | Q_flag | 3 | bad | quality flag for river discharge |
| master | Q_flag | 9 | missing | quality flag for river discharge |
| master | Q_qc1 | 0 | pass | qc stage 1 physical screen for river discharge |
| master | Q_qc1 | 3 | bad | qc stage 1 physical screen for river discharge |
| master | Q_qc1 | 9 | missing | qc stage 1 physical screen for river discharge |
| master | Q_qc2 | 0 | pass | qc stage 2 log-iqr screen for river discharge |
| master | Q_qc2 | 2 | suspect | qc stage 2 log-iqr screen for river discharge |
| master | Q_qc2 | 8 | not_checked | qc stage 2 log-iqr screen for river discharge |
| master | Q_qc2 | 9 | missing | qc stage 2 log-iqr screen for river discharge |
| master | SSC_flag | 0 | good | quality flag for suspended sediment concentration |
| master | SSC_flag | 1 | estimated | quality flag for suspended sediment concentration |
| master | SSC_flag | 2 | suspect | quality flag for suspended sediment concentration |
| master | SSC_flag | 3 | bad | quality flag for suspended sediment concentration |
| master | SSC_flag | 9 | missing | quality flag for suspended sediment concentration |
| master | SSC_qc1 | 0 | pass | qc stage 1 physical screen for suspended sediment concentration |
| master | SSC_qc1 | 3 | bad | qc stage 1 physical screen for suspended sediment concentration |
| master | SSC_qc1 | 9 | missing | qc stage 1 physical screen for suspended sediment concentration |
| master | SSC_qc2 | 0 | pass | qc stage 2 log-iqr screen for suspended sediment concentration |
| master | SSC_qc2 | 2 | suspect | qc stage 2 log-iqr screen for suspended sediment concentration |
| master | SSC_qc2 | 8 | not_checked | qc stage 2 log-iqr screen for suspended sediment concentration |
| master | SSC_qc2 | 9 | missing | qc stage 2 log-iqr screen for suspended sediment concentration |

_Showing first 24 of 74 rows._

## Final Flag Summary

| temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|
| satellite | SSL | SSL_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | Q | Q_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | SSC | SSC_flag | 0 | good | 15,121,092 | 91.76% | 16,478,276 |
| master | Q | Q_flag | 0 | good | 2,803,162 | 97.55% | 2,873,420 |
| master | SSC | SSC_flag | 0 | good | 2,732,153 | 95.08% | 2,873,420 |
| master | SSL | SSL_flag | 1 | estimated | 2,648,231 | 92.16% | 2,873,420 |
| satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| master | SSL | SSL_flag | 2 | suspect | 139,751 | 4.86% | 2,873,420 |
| satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| master | SSC | SSC_flag | 2 | suspect | 102,029 | 3.55% | 2,873,420 |
| master | SSL | SSL_flag | 0 | good | 75,679 | 2.63% | 2,873,420 |
| master | Q | Q_flag | 2 | suspect | 53,194 | 1.85% | 2,873,420 |
| master | SSC | SSC_flag | 1 | estimated | 28,676 | 1.00% | 2,873,420 |
| master | Q | Q_flag | 9 | missing | 16,509 | 0.57% | 2,873,420 |
| master | SSC | SSC_flag | 9 | missing | 10,562 | 0.37% | 2,873,420 |
| master | SSL | SSL_flag | 9 | missing | 9,204 | 0.32% | 2,873,420 |
| climatology | SSL | SSL_flag | 0 | good | 1,298 | 98.18% | 1,322 |
| satellite | Q | Q_flag | 2 | suspect | 1,209 | 0.01% | 16,478,276 |
| satellite | SSL | SSL_flag | 2 | suspect | 1,130 | 0.01% | 16,478,276 |
| climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| climatology | SSC | SSC_flag | 0 | good | 759 | 57.41% | 1,322 |
| master | SSL | SSL_flag | 3 | bad | 555 | 0.02% | 2,873,420 |

_Showing first 24 of 29 rows._

## Stage Flag Summary

| temporal resolution | variable | qc stage | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|
| master | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 2,061,949 | 71.76% | 2,873,420 |
| master | Q | log_iqr | Q_qc2 | 9 | missing | 1,677,086 | 58.37% | 2,873,420 |
| master | Q | physical_plausibility | Q_qc1 | 9 | missing | 1,674,409 | 58.27% | 2,873,420 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 1,674,355 | 58.27% | 2,873,420 |
| master | SSC | log_iqr | SSC_qc2 | 9 | missing | 1,671,162 | 58.16% | 2,873,420 |
| master | SSL | log_iqr | SSL_qc2 | 9 | missing | 1,670,351 | 58.13% | 2,873,420 |
| master | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 1,668,485 | 58.07% | 2,873,420 |
| master | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 1,667,104 | 58.02% | 2,873,420 |
| master | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 1,206,316 | 41.98% | 2,873,420 |
| master | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 1,204,935 | 41.93% | 2,873,420 |
| master | Q | physical_plausibility | Q_qc1 | 0 | pass | 1,199,011 | 41.73% | 2,873,420 |
| master | Q | log_iqr | Q_qc2 | 0 | pass | 1,151,046 | 40.06% | 2,873,420 |
| master | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 1,128,925 | 39.29% | 2,873,420 |
| master | SSC | log_iqr | SSC_qc2 | 0 | pass | 1,123,751 | 39.11% | 2,873,420 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 1,060,931 | 36.92% | 2,873,420 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 800,473 | 27.86% | 2,873,420 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 123,728 | 4.31% | 2,873,420 |
| master | SSL | log_iqr | SSL_qc2 | 0 | pass | 72,994 | 2.54% | 2,873,420 |
| master | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 51,894 | 1.81% | 2,873,420 |
| master | Q | log_iqr | Q_qc2 | 8 | not_checked | 29,263 | 1.02% | 2,873,420 |
| master | SSC | log_iqr | SSC_qc2 | 2 | suspect | 26,613 | 0.93% | 2,873,420 |
| master | Q | log_iqr | Q_qc2 | 2 | suspect | 16,025 | 0.56% | 2,873,420 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 2 | suspect | 14,406 | 0.50% | 2,873,420 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 2 | suspect | 10,911 | 0.38% | 2,873,420 |

_Showing first 24 of 26 rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 132,614 | 16,345,662 | 16,344,453 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 132,693 | 16,345,583 | 16,344,453 | 0.81% | 0.81% | 99.19% | 99.19% |
| master | SSL | SSL_qc3 | 2,873,420 | 87 | 0 | 87 | 2,072,860 | 2,061,949 | 0.00% | 0.00% | 72.14% | 71.76% |
| master | SSC | SSC_qc2 | 2,873,420 | 1,123,751 | 0 | 1,123,751 | 1,697,775 | 1,671,162 | 39.11% | 39.11% | 59.09% | 58.16% |
| master | Q | Q_qc2 | 2,873,420 | 1,151,046 | 0 | 1,151,046 | 1,693,111 | 1,677,086 | 40.06% | 40.06% | 58.92% | 58.37% |
| master | SSC | SSC_qc3 | 2,873,420 | 1,060,931 | 0 | 1,060,931 | 1,688,761 | 1,674,355 | 36.92% | 36.92% | 58.77% | 58.27% |
| master | Q | Q_qc1 | 2,873,420 | 1,199,011 | 0 | 1,199,011 | 1,674,409 | 1,674,409 | 41.73% | 41.73% | 58.27% | 58.27% |
| master | SSL | SSL_qc2 | 2,873,420 | 72,994 | 0 | 72,994 | 1,671,501 | 1,670,351 | 2.54% | 2.54% | 58.17% | 58.13% |
| master | SSC | SSC_qc1 | 2,873,420 | 1,204,935 | 0 | 1,204,935 | 1,668,485 | 1,668,485 | 41.93% | 41.93% | 58.07% | 58.07% |
| master | SSL | SSL_qc1 | 2,873,420 | 1,206,316 | 0 | 1,206,316 | 1,667,104 | 1,667,104 | 41.98% | 41.98% | 58.02% | 58.02% |
| satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 15,121,092 | 1,357,184 | 960,798 | 91.76% | 91.76% | 8.24% | 5.83% |
| master | SSL | SSL_flag | 2,873,420 | 75,679 | 2,648,231 | 2,723,910 | 149,510 | 9,204 | 2.63% | 94.80% | 5.20% | 0.32% |
| master | SSC | SSC_flag | 2,873,420 | 2,732,153 | 28,676 | 2,760,829 | 112,591 | 10,562 | 95.08% | 96.08% | 3.92% | 0.37% |
| master | Q | Q_flag | 2,873,420 | 2,803,162 | 0 | 2,803,162 | 70,258 | 16,509 | 97.55% | 97.55% | 2.45% | 0.57% |
| climatology | Q | Q_flag | 1,322 | 782 | 0 | 782 | 540 | 540 | 59.15% | 59.15% | 40.85% | 40.85% |
| climatology | SSC | SSC_flag | 1,322 | 759 | 45 | 804 | 518 | 518 | 57.41% | 60.82% | 39.18% | 39.18% |
| climatology | SSL | SSL_flag | 1,322 | 1,298 | 0 | 1,298 | 24 | 24 | 98.18% | 98.18% | 1.82% | 1.82% |

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 99.20% | 198.38% |
| product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 99.19% | 198.38% |
| product_variable | all_release_sources | master | SSL | SSL_qc3 | 2,873,420 | 87 | 2,072,860 | 4,935,282 | 0.00% | 72.14% | 171.76% |
| product_variable | all_release_sources | master | SSL | SSL_qc2 | 2,873,420 | 72,994 | 1,671,501 | 4,470,777 | 2.54% | 58.17% | 155.59% |
| product_variable | all_release_sources | master | SSC | SSC_qc3 | 2,873,420 | 1,060,931 | 1,688,761 | 3,486,844 | 36.92% | 58.77% | 121.35% |
| product_variable | all_release_sources | master | SSC | SSC_qc2 | 2,873,420 | 1,123,751 | 1,697,775 | 3,420,831 | 39.11% | 59.09% | 119.05% |
| product_variable | all_release_sources | master | Q | Q_qc2 | 2,873,420 | 1,151,046 | 1,693,111 | 3,399,460 | 40.06% | 58.92% | 118.31% |
| product_variable | all_release_sources | master | Q | Q_qc1 | 2,873,420 | 1,199,011 | 1,674,409 | 3,348,818 | 41.73% | 58.27% | 116.54% |
| product_variable | all_release_sources | master | SSC | SSC_qc1 | 2,873,420 | 1,204,935 | 1,668,485 | 3,336,970 | 41.93% | 58.07% | 116.13% |
| product_variable | all_release_sources | master | SSL | SSL_qc1 | 2,873,420 | 1,206,316 | 1,667,104 | 3,334,208 | 41.98% | 58.02% | 116.04% |
| product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 1,357,184 | 2,317,982 | 91.76% | 8.24% | 14.07% |
| product_variable | all_release_sources | master | SSL | SSL_flag | 2,873,420 | 2,723,910 | 149,510 | 158,714 | 94.80% | 5.20% | 5.52% |
| product_variable | all_release_sources | master | SSC | SSC_flag | 2,873,420 | 2,760,829 | 112,591 | 123,153 | 96.08% | 3.92% | 4.29% |
| product_variable | all_release_sources | master | Q | Q_flag | 2,873,420 | 2,803,162 | 70,258 | 86,767 | 97.55% | 2.45% | 3.02% |
| product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 540 | 1,080 | 59.15% | 40.85% | 81.69% |
| product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,322 | 804 | 518 | 1,036 | 60.82% | 39.18% | 78.37% |
| product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,322 | 1,298 | 24 | 48 | 98.18% | 1.82% | 3.63% |

## Stage Effectiveness

| temporal resolution | variable | qc stage | flag variable | n total | good count | bad count | not checked count | missing count | good rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| master | SSL | ssc_q_consistency | SSL_qc3 | 2,873,420 | 87 | 0 | 800,473 | 2,061,949 | 0.00% | 72.14% | 71.76% |
| master | SSC | log_iqr | SSC_qc2 | 2,873,420 | 1,123,751 | 0 | 51,894 | 1,671,162 | 39.11% | 59.09% | 58.16% |
| master | Q | log_iqr | Q_qc2 | 2,873,420 | 1,151,046 | 0 | 29,263 | 1,677,086 | 40.06% | 58.92% | 58.37% |
| master | SSC | ssc_q_consistency | SSC_qc3 | 2,873,420 | 1,060,931 | 0 | 123,728 | 1,674,355 | 36.92% | 58.77% | 58.27% |
| master | Q | physical_plausibility | Q_qc1 | 2,873,420 | 1,199,011 | 0 | 0 | 1,674,409 | 41.73% | 58.27% | 58.27% |
| master | SSL | log_iqr | SSL_qc2 | 2,873,420 | 72,994 | 0 | 1,128,925 | 1,670,351 | 2.54% | 58.17% | 58.13% |
| master | SSC | physical_plausibility | SSC_qc1 | 2,873,420 | 1,204,935 | 0 | 0 | 1,668,485 | 41.93% | 58.07% | 58.07% |
| master | SSL | physical_plausibility | SSL_qc1 | 2,873,420 | 1,206,316 | 0 | 0 | 1,667,104 | 41.98% | 58.02% | 58.02% |

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | master | Q | Q_flag | 0 | good | 2,803,162 | 97.55% | 2,873,420 |
| all_release_sources | all | final | final | master | Q | Q_flag | 2 | suspect | 53,194 | 1.85% | 2,873,420 |
| all_release_sources | all | final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 2,873,420 |
| all_release_sources | all | final | final | master | Q | Q_flag | 9 | missing | 16,509 | 0.57% | 2,873,420 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 1,199,011 | 41.73% | 2,873,420 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 1,674,409 | 58.27% | 2,873,420 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 1,151,046 | 40.06% | 2,873,420 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 16,025 | 0.56% | 2,873,420 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 29,263 | 1.02% | 2,873,420 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 1,677,086 | 58.37% | 2,873,420 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 0 | good | 2,732,153 | 95.08% | 2,873,420 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 1 | estimated | 28,676 | 1.00% | 2,873,420 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 2 | suspect | 102,029 | 3.55% | 2,873,420 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 9 | missing | 10,562 | 0.37% | 2,873,420 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 1,204,935 | 41.93% | 2,873,420 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 1,668,485 | 58.07% | 2,873,420 |

_Showing first 16 of 55 rows._

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | master | Q | Q_flag | 0 | good | 2,803,162 | 97.55% | 2,873,420 |
| final | final | master | Q | Q_flag | 2 | suspect | 53,194 | 1.85% | 2,873,420 |
| final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 2,873,420 |
| final | final | master | Q | Q_flag | 9 | missing | 16,509 | 0.57% | 2,873,420 |
| stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 1,199,011 | 41.73% | 2,873,420 |
| stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 1,674,409 | 58.27% | 2,873,420 |
| stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 1,151,046 | 40.06% | 2,873,420 |
| stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 16,025 | 0.56% | 2,873,420 |
| stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 29,263 | 1.02% | 2,873,420 |
| stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 1,677,086 | 58.37% | 2,873,420 |
| final | final | master | SSC | SSC_flag | 0 | good | 2,732,153 | 95.08% | 2,873,420 |
| final | final | master | SSC | SSC_flag | 1 | estimated | 28,676 | 1.00% | 2,873,420 |
| final | final | master | SSC | SSC_flag | 2 | suspect | 102,029 | 3.55% | 2,873,420 |
| final | final | master | SSC | SSC_flag | 9 | missing | 10,562 | 0.37% | 2,873,420 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 1,204,935 | 41.93% | 2,873,420 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 1,668,485 | 58.07% | 2,873,420 |

_Showing first 16 of 55 rows._

## Flag Counts by Variable

| qc level | qc stage | variable | flag variable | flag | meaning | count | n total | percentage |
|---|---|---|---|---|---|---|---|---|
| final | final | Q | Q_flag | 0 | good | 2,936,558 | 19,353,018 | 15.17% |
| final | final | Q | Q_flag | 2 | suspect | 54,403 | 19,351,696 | 0.28% |
| final | final | Q | Q_flag | 3 | bad | 555 | 2,873,420 | 0.02% |
| final | final | Q | Q_flag | 9 | missing | 16,361,502 | 19,353,018 | 84.54% |
| final | final | SSC | SSC_flag | 0 | good | 17,854,004 | 19,353,018 | 92.25% |
| final | final | SSC | SSC_flag | 1 | estimated | 28,721 | 2,874,742 | 1.00% |
| final | final | SSC | SSC_flag | 2 | suspect | 498,415 | 19,351,696 | 2.58% |
| final | final | SSC | SSC_flag | 9 | missing | 971,878 | 19,353,018 | 5.02% |
| final | final | SSL | SSL_flag | 0 | good | 209,670 | 19,353,018 | 1.08% |
| final | final | SSL | SSL_flag | 1 | estimated | 2,648,231 | 2,873,420 | 92.16% |
| final | final | SSL | SSL_flag | 2 | suspect | 140,881 | 19,351,696 | 0.73% |
| final | final | SSL | SSL_flag | 3 | bad | 555 | 2,873,420 | 0.02% |
| final | final | SSL | SSL_flag | 9 | missing | 16,353,681 | 19,353,018 | 84.50% |
| stage | log_iqr | Q | Q_qc2 | 0 | pass | 1,151,046 | 2,873,420 | 40.06% |
| stage | log_iqr | Q | Q_qc2 | 2 | suspect | 16,025 | 2,873,420 | 0.56% |
| stage | log_iqr | Q | Q_qc2 | 8 | not_checked | 29,263 | 2,873,420 | 1.02% |

_Showing first 16 of 39 rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 1,209 | 0 | 0 | 16,344,453 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.38% |
|  |  | product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 1,130 | 0 | 0 | 16,344,453 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 0% | 0.01% | 0% | 0% | 99.19% | 0.81% | 99.19% | 198.38% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc3 | 2,873,420 | 87 | 0 | 10,911 | 0 | 800,473 | 2,061,949 | 87 | 2,072,860 | 4,935,282 | 0.00% | 0% | 0.38% | 0% | 27.86% | 71.76% | 0.00% | 72.14% | 171.76% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc2 | 2,873,420 | 72,994 | 0 | 1,150 | 0 | 1,128,925 | 1,670,351 | 72,994 | 1,671,501 | 4,470,777 | 2.54% | 0% | 0.04% | 0% | 39.29% | 58.13% | 2.54% | 58.17% | 155.59% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc3 | 2,873,420 | 1,060,931 | 0 | 14,406 | 0 | 123,728 | 1,674,355 | 1,060,931 | 1,688,761 | 3,486,844 | 36.92% | 0% | 0.50% | 0% | 4.31% | 58.27% | 36.92% | 58.77% | 121.35% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc2 | 2,873,420 | 1,123,751 | 0 | 26,613 | 0 | 51,894 | 1,671,162 | 1,123,751 | 1,697,775 | 3,420,831 | 39.11% | 0% | 0.93% | 0% | 1.81% | 58.16% | 39.11% | 59.09% | 119.05% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc2 | 2,873,420 | 1,151,046 | 0 | 16,025 | 0 | 29,263 | 1,677,086 | 1,151,046 | 1,693,111 | 3,399,460 | 40.06% | 0% | 0.56% | 0% | 1.02% | 58.37% | 40.06% | 58.92% | 118.31% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc1 | 2,873,420 | 1,199,011 | 0 | 0 | 0 | 0 | 1,674,409 | 1,199,011 | 1,674,409 | 3,348,818 | 41.73% | 0% | 0% | 0% | 0% | 58.27% | 41.73% | 58.27% | 116.54% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc1 | 2,873,420 | 1,204,935 | 0 | 0 | 0 | 0 | 1,668,485 | 1,204,935 | 1,668,485 | 3,336,970 | 41.93% | 0% | 0% | 0% | 0% | 58.07% | 41.93% | 58.07% | 116.13% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc1 | 2,873,420 | 1,206,316 | 0 | 0 | 0 | 0 | 1,667,104 | 1,206,316 | 1,667,104 | 3,334,208 | 41.98% | 0% | 0% | 0% | 0% | 58.02% | 41.98% | 58.02% | 116.04% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 0 | 0 | 0 | 0 | 540 | 782 | 540 | 1,080 | 59.15% | 0% | 0% | 0% | 0% | 40.85% | 59.15% | 40.85% | 81.69% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_flag | 1,322 | 759 | 45 | 0 | 0 | 0 | 518 | 804 | 518 | 1,036 | 57.41% | 3.40% | 0% | 0% | 0% | 39.18% | 60.82% | 39.18% | 78.37% |
|  |  | product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 396,386 | 0 | 0 | 960,798 | 15,121,092 | 1,357,184 | 2,317,982 | 91.76% | 0% | 2.41% | 0% | 0% | 5.83% | 91.76% | 8.24% | 14.07% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_flag | 2,873,420 | 75,679 | 2,648,231 | 139,751 | 555 | 0 | 9,204 | 2,723,910 | 149,510 | 158,714 | 2.63% | 92.16% | 4.86% | 0.02% | 0% | 0.32% | 94.80% | 5.20% | 5.52% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_flag | 2,873,420 | 2,732,153 | 28,676 | 102,029 | 0 | 0 | 10,562 | 2,760,829 | 112,591 | 123,153 | 95.08% | 1.00% | 3.55% | 0% | 0% | 0.37% | 96.08% | 3.92% | 4.29% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_flag | 1,322 | 1,298 | 0 | 0 | 0 | 0 | 24 | 1,298 | 24 | 48 | 98.18% | 0% | 0% | 0% | 0% | 1.82% | 98.18% | 1.82% | 3.63% |

_Showing first 16 of 17 rows._

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 1,322 | 2,644 | 50% |
| all | climatology | SSC | 1,322 | 3,966 | 33.33% |
| all | climatology | SSL | 1,322 | 2,644 | 50% |
| all | master | Q | 8,620,260 | 28,734,200 | 30% |
| all | master | SSC | 11,493,680 | 40,227,880 | 28.57% |
| all | master | SSL | 11,493,680 | 43,101,300 | 26.67% |
| all | satellite | Q | 16,478,276 | 49,434,828 | 33.33% |
| all | satellite | SSC | 16,478,276 | 49,434,828 | 33.33% |
| all | satellite | SSL | 16,478,276 | 49,434,828 | 33.33% |

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.
- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.

## Figures

- `fig_qc_flag_by_source_type.png`: `output_other/stats_release/qc_flags/figures/fig_qc_flag_by_source_type.png`
- `fig_qc_flag_distribution.png`: `output_other/stats_release/qc_flags/figures/fig_qc_flag_distribution.png`
- `fig_qc_health.png`: `output_other/stats_release/qc_flags/figures/fig_qc_health.png`
- `fig_qc_health_by_resolution.png`: `output_other/stats_release/qc_flags/figures/fig_qc_health_by_resolution.png`
- `fig_qc_missing_trends.png`: `output_other/stats_release/qc_flags/figures/fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`: `output_other/stats_release/qc_flags/figures/fig_qc_stage_summary.png`
- `fig_qc_top_problem_clusters.png`: `output_other/stats_release/qc_flags/figures/fig_qc_top_problem_clusters.png`
- `fig_qc_top_problem_sources.png`: `output_other/stats_release/qc_flags/figures/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `output_other/stats_release/qc_flags/figures/fig_qc_yearly_problem_trends.png`
