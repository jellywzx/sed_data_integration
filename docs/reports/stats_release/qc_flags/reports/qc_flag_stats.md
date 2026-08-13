# Release QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 83,151,137
- Final flag rows: 29
- Stage flag rows: 45
- Usable flag count from health KPIs: 30,725,868
- Problem flag count from health KPIs: 789,059
- Missing flag count from health KPIs: 49,467,100
- Stage-effectiveness rows available: 16

## Flag Schema

| product | flag variable | flag value | flag meaning | long name |
|---|---|---|---|---|
| master | Q_flag | 0 | good | quality flag for river discharge |
| master | Q_flag | 1 | derived | quality flag for river discharge |
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
| master | SSC_flag | 1 | derived | quality flag for suspended sediment concentration |
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

_Showing first 24 of 103 rows._

## Final Flag Summary

| product group | release component | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|
| satellite | satellite | SSL | SSL_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | satellite | Q | Q_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | satellite | SSC | SSC_flag | 0 | good | 15,121,092 | 91.76% | 16,478,276 |
| main | master | SSC | SSC_flag | 0 | good | 2,914,450 | 95.13% | 3,063,758 |
| main | master | Q | Q_flag | 0 | good | 2,790,906 | 91.09% | 3,063,758 |
| main | master | SSL | SSL_flag | 1 | derived | 2,700,030 | 88.13% | 3,063,758 |
| satellite | satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| satellite | satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| main | master | Q | Q_flag | 9 | missing | 211,651 | 6.91% | 3,063,758 |
| main | master | SSL | SSL_flag | 9 | missing | 201,151 | 6.57% | 3,063,758 |
| main | master | SSL | SSL_flag | 2 | suspect | 146,922 | 4.80% | 3,063,758 |
| satellite | satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| satellite | satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| main | master | SSC | SSC_flag | 2 | suspect | 110,564 | 3.61% | 3,063,758 |
| main | master | Q | Q_flag | 2 | suspect | 60,646 | 1.98% | 3,063,758 |
| main | master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,063,758 |
| main | master | SSL | SSL_flag | 0 | good | 15,100 | 0.49% | 3,063,758 |
| main | master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,063,758 |
| climatology | climatology | SSL | SSL_flag | 0 | good | 1,337 | 98.24% | 1,361 |
| satellite | satellite | Q | Q_flag | 2 | suspect | 1,209 | 0.01% | 16,478,276 |
| satellite | satellite | SSL | SSL_flag | 2 | suspect | 1,130 | 0.01% | 16,478,276 |
| climatology | climatology | Q | Q_flag | 0 | good | 782 | 57.46% | 1,361 |
| climatology | climatology | SSC | SSC_flag | 0 | good | 759 | 55.77% | 1,361 |
| climatology | climatology | Q | Q_flag | 9 | missing | 579 | 42.54% | 1,361 |

_Showing first 24 of 29 rows._

## Stage Flag Summary

| product group | release component | variable | qc stage | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| main | master | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 2,326,872 | 75.95% | 3,063,758 |
| main | master | Q | log_iqr | Q_qc2 | 9 | missing | 1,937,068 | 63.23% | 3,063,758 |
| main | master | SSL | log_iqr | SSL_qc2 | 9 | missing | 1,935,215 | 63.16% | 3,063,758 |
| main | master | Q | physical_plausibility | Q_qc1 | 9 | missing | 1,934,391 | 63.14% | 3,063,758 |
| main | master | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 1,931,968 | 63.06% | 3,063,758 |
| main | master | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 1,777,038 | 58.00% | 3,063,758 |
| main | master | SSC | log_iqr | SSC_qc2 | 9 | missing | 1,773,845 | 57.90% | 3,063,758 |
| main | master | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 1,771,168 | 57.81% | 3,063,758 |
| main | master | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 1,292,590 | 42.19% | 3,063,758 |
| main | master | SSC | log_iqr | SSC_qc2 | 0 | pass | 1,209,900 | 39.49% | 3,063,758 |
| main | master | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 1,131,790 | 36.94% | 3,063,758 |
| main | master | Q | physical_plausibility | Q_qc1 | 0 | pass | 1,129,367 | 36.86% | 3,063,758 |
| main | master | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 1,113,643 | 36.35% | 3,063,758 |
| main | master | Q | log_iqr | Q_qc2 | 0 | pass | 1,087,364 | 35.49% | 3,063,758 |
| main | master | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 1,019,099 | 33.26% | 3,063,758 |
| main | master | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 727,497 | 23.75% | 3,063,758 |
| main | master | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 253,504 | 8.27% | 3,063,758 |
| main | master | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 46,632 | 1.52% | 3,063,758 |
| main | master | SSC | log_iqr | SSC_qc2 | 2 | suspect | 33,381 | 1.09% | 3,063,758 |
| main | master | Q | log_iqr | Q_qc2 | 8 | not_checked | 25,169 | 0.82% | 3,063,758 |
| main | master | SSL | log_iqr | SSL_qc2 | 0 | pass | 14,825 | 0.48% | 3,063,758 |
| main | master | Q | log_iqr | Q_qc2 | 2 | suspect | 14,157 | 0.46% | 3,063,758 |
| main | master | SSC | ssc_q_consistency | SSC_qc3 | 2 | suspect | 14,117 | 0.46% | 3,063,758 |
| main | master | SSL | ssc_q_consistency | SSL_qc3 | 2 | suspect | 9,362 | 0.31% | 3,063,758 |

_Showing first 24 of 45 rows._

## Health KPIs

Usable combines good and estimated/derived values (flags 0-1). Problem counts suspect/bad (flags 2-3) only; missing (flag 9) is reported separately.

| product group | release component | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 15,121,092 | 396,386 | 960,798 | 91.76% | 91.76% | 2.41% | 5.83% |
| main | master | SSL | SSL_flag | 3,063,758 | 15,100 | 2,700,030 | 2,715,130 | 147,477 | 201,151 | 0.49% | 88.62% | 4.81% | 6.57% |
| main | master | SSC | SSC_flag | 3,063,758 | 2,914,450 | 28,123 | 2,942,573 | 110,564 | 10,621 | 95.13% | 96.04% | 3.61% | 0.35% |
| main | master | Q | Q_flag | 3,063,758 | 2,790,906 | 0 | 2,790,906 | 61,201 | 211,651 | 91.09% | 91.09% | 2.00% | 6.91% |
| main | master | SSC | SSC_qc2 | 3,063,758 | 1,209,900 | 0 | 1,209,900 | 33,381 | 1,773,845 | 39.49% | 39.49% | 1.09% | 57.90% |
| main | master | Q | Q_qc2 | 3,063,758 | 1,087,364 | 0 | 1,087,364 | 14,157 | 1,937,068 | 35.49% | 35.49% | 0.46% | 63.23% |
| main | master | SSC | SSC_qc3 | 3,063,758 | 1,019,099 | 0 | 1,019,099 | 14,117 | 1,777,038 | 33.26% | 33.26% | 0.46% | 58.00% |
| main | master | SSL | SSL_qc3 | 3,063,758 | 27 | 0 | 27 | 9,362 | 2,326,872 | 0.00% | 0.00% | 0.31% | 75.95% |
| satellite | satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 132,614 | 1,209 | 16,344,453 | 0.80% | 0.80% | 0.01% | 99.19% |
| satellite | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 132,693 | 1,130 | 16,344,453 | 0.81% | 0.81% | 0.01% | 99.19% |
| main | master | SSL | SSL_qc2 | 3,063,758 | 14,825 | 0 | 14,825 | 75 | 1,935,215 | 0.48% | 0.48% | 0.00% | 63.16% |
| main | master | SSL | SSL_qc1 | 3,063,758 | 1,131,790 | 0 | 1,131,790 | 0 | 1,931,968 | 36.94% | 36.94% | 0% | 63.06% |
| main | master | SSC | SSC_qc1 | 3,063,758 | 1,292,590 | 0 | 1,292,590 | 0 | 1,771,168 | 42.19% | 42.19% | 0% | 57.81% |
| climatology | climatology | Q | Q_flag | 1,361 | 782 | 0 | 782 | 0 | 579 | 57.46% | 57.46% | 0% | 42.54% |
| climatology | climatology | Q | Q_qc1 | 1,361 | 782 | 0 | 782 | 0 | 579 | 57.46% | 57.46% | 0% | 42.54% |
| climatology | climatology | SSL | SSL_qc3 | 1,361 | 0 | 0 | 0 | 0 | 828 | 0% | 0% | 0% | 60.84% |
| climatology | climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 0 | 0 | 828 | 0% | 0% | 0% | 60.84% |
| climatology | climatology | SSL | SSL_qc1 | 1,361 | 1,337 | 0 | 1,337 | 0 | 24 | 98.24% | 98.24% | 0% | 1.76% |
| climatology | climatology | SSL | SSL_flag | 1,361 | 1,337 | 0 | 1,337 | 0 | 24 | 98.24% | 98.24% | 0% | 1.76% |
| climatology | climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 24 | 0 | 804 | 1.76% | 1.76% | 0% | 59.07% |
| climatology | climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 24 | 0 | 804 | 1.76% | 1.76% | 0% | 59.07% |
| climatology | climatology | SSC | SSC_qc1 | 1,361 | 776 | 30 | 806 | 0 | 555 | 57.02% | 59.22% | 0% | 40.78% |
| climatology | climatology | SSC | SSC_flag | 1,361 | 759 | 47 | 806 | 0 | 555 | 55.77% | 59.22% | 0% | 40.78% |
| climatology | climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 0 | 0 | 828 | 0% | 0% | 0% | 60.84% |

_Showing first 24 of 25 rows._

## Issue Hotspots

| grouping level | source dataset | product group | release component | variable | flag variable | n total | usable count | problem count | missing count | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | satellite | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 396,386 | 960,798 | 91.76% | 2.41% | 5.83% |
| product_variable | all_release_sources | main | master | SSL | SSL_flag | 3,063,758 | 2,715,130 | 147,477 | 201,151 | 88.62% | 4.81% | 6.57% |
| product_variable | all_release_sources | main | master | SSC | SSC_flag | 3,063,758 | 2,942,573 | 110,564 | 10,621 | 96.04% | 3.61% | 0.35% |
| product_variable | all_release_sources | main | master | Q | Q_flag | 3,063,758 | 2,790,906 | 61,201 | 211,651 | 91.09% | 2.00% | 6.91% |
| product_variable | all_release_sources | main | master | SSC | SSC_qc2 | 3,063,758 | 1,209,900 | 33,381 | 1,773,845 | 39.49% | 1.09% | 57.90% |
| product_variable | all_release_sources | main | master | Q | Q_qc2 | 3,063,758 | 1,087,364 | 14,157 | 1,937,068 | 35.49% | 0.46% | 63.23% |
| product_variable | all_release_sources | main | master | SSC | SSC_qc3 | 3,063,758 | 1,019,099 | 14,117 | 1,777,038 | 33.26% | 0.46% | 58.00% |
| product_variable | all_release_sources | main | master | SSL | SSL_qc3 | 3,063,758 | 27 | 9,362 | 2,326,872 | 0.00% | 0.31% | 75.95% |
| product_variable | all_release_sources | satellite | satellite | Q | Q_flag | 16,478,276 | 132,614 | 1,209 | 16,344,453 | 0.80% | 0.01% | 99.19% |
| product_variable | all_release_sources | satellite | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 1,130 | 16,344,453 | 0.81% | 0.01% | 99.19% |
| product_variable | all_release_sources | main | master | SSL | SSL_qc2 | 3,063,758 | 14,825 | 75 | 1,935,215 | 0.48% | 0.00% | 63.16% |
| product_variable | all_release_sources | climatology | climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 828 | 0% | 0% | 60.84% |
| product_variable | all_release_sources | climatology | climatology | SSL | SSL_flag | 1,361 | 1,337 | 0 | 24 | 98.24% | 0% | 1.76% |
| product_variable | all_release_sources | climatology | climatology | SSC | SSC_flag | 1,361 | 806 | 0 | 555 | 59.22% | 0% | 40.78% |
| product_variable | all_release_sources | climatology | climatology | SSC | SSC_qc1 | 1,361 | 806 | 0 | 555 | 59.22% | 0% | 40.78% |
| product_variable | all_release_sources | climatology | climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 804 | 1.76% | 0% | 59.07% |
| product_variable | all_release_sources | climatology | climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 804 | 1.76% | 0% | 59.07% |
| product_variable | all_release_sources | main | master | SSC | SSC_qc1 | 3,063,758 | 1,292,590 | 0 | 1,771,168 | 42.19% | 0% | 57.81% |
| product_variable | all_release_sources | climatology | climatology | SSL | SSL_qc1 | 1,361 | 1,337 | 0 | 24 | 98.24% | 0% | 1.76% |
| product_variable | all_release_sources | climatology | climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 828 | 0% | 0% | 60.84% |

_Showing first 20 of 25 rows._

## Stage Effectiveness

| product group | release component | variable | qc stage | flag variable | n total | good count | bad count | not checked count | missing count | good rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| main | master | SSC | log_iqr | SSC_qc2 | 3,063,758 | 1,209,900 | 0 | 46,632 | 1,773,845 | 39.49% | 1.09% | 57.90% |
| main | master | Q | log_iqr | Q_qc2 | 3,063,758 | 1,087,364 | 0 | 25,169 | 1,937,068 | 35.49% | 0.46% | 63.23% |
| main | master | SSC | ssc_q_consistency | SSC_qc3 | 3,063,758 | 1,019,099 | 0 | 253,504 | 1,777,038 | 33.26% | 0.46% | 58.00% |
| main | master | SSL | ssc_q_consistency | SSL_qc3 | 3,063,758 | 27 | 0 | 727,497 | 2,326,872 | 0.00% | 0.31% | 75.95% |
| main | master | SSL | log_iqr | SSL_qc2 | 3,063,758 | 14,825 | 0 | 1,113,643 | 1,935,215 | 0.48% | 0.00% | 63.16% |
| climatology | climatology | Q | physical_plausibility | Q_qc1 | 1,361 | 782 | 0 | 0 | 579 | 57.46% | 0% | 42.54% |
| climatology | climatology | Q | log_iqr | Q_qc2 | 1,361 | 0 | 0 | 533 | 828 | 0% | 0% | 60.84% |
| climatology | climatology | SSC | physical_plausibility | SSC_qc1 | 1,361 | 776 | 0 | 0 | 555 | 57.02% | 0% | 40.78% |
| climatology | climatology | SSC | log_iqr | SSC_qc2 | 1,361 | 24 | 0 | 533 | 804 | 1.76% | 0% | 59.07% |
| climatology | climatology | SSC | ssc_q_consistency | SSC_qc3 | 1,361 | 24 | 0 | 533 | 804 | 1.76% | 0% | 59.07% |
| climatology | climatology | SSL | physical_plausibility | SSL_qc1 | 1,361 | 1,337 | 0 | 0 | 24 | 98.24% | 0% | 1.76% |
| climatology | climatology | SSL | log_iqr | SSL_qc2 | 1,361 | 0 | 0 | 533 | 828 | 0% | 0% | 60.84% |
| climatology | climatology | SSL | ssc_q_consistency | SSL_qc3 | 1,361 | 0 | 0 | 533 | 828 | 0% | 0% | 60.84% |
| main | master | Q | physical_plausibility | Q_qc1 | 3,063,758 | 1,129,367 | 0 | 0 | 1,934,391 | 36.86% | 0% | 63.14% |
| main | master | SSC | physical_plausibility | SSC_qc1 | 3,063,758 | 1,292,590 | 0 | 0 | 1,771,168 | 42.19% | 0% | 57.81% |
| main | master | SSL | physical_plausibility | SSL_qc1 | 3,063,758 | 1,131,790 | 0 | 0 | 1,931,968 | 36.94% | 0% | 63.06% |

## Final Flag Health by True Temporal Resolution

Rows are main-product final flags split by record-level temporal resolution; `analysis_ready` combines flags 0 and 1.

| product group | temporal resolution | variable | flag variable | n total | good count | derived count | analysis ready count | problem count | missing count | good rate | derived rate | analysis ready rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| main | daily | Q | Q_flag | 3,044,466 | 2,771,862 | 0 | 2,771,862 | 61,132 | 211,472 | 91.05% | 0% | 91.05% | 2.01% | 6.95% |
| main | daily | SSC | SSC_flag | 3,044,466 | 2,914,341 | 9,224 | 2,923,565 | 110,374 | 10,527 | 95.73% | 0.30% | 96.03% | 3.63% | 0.35% |
| main | daily | SSL | SSL_flag | 3,044,466 | 11,502 | 2,684,633 | 2,696,135 | 147,300 | 201,031 | 0.38% | 88.18% | 88.56% | 4.84% | 6.60% |
| main | monthly | Q | Q_flag | 18,824 | 18,702 | 0 | 18,702 | 63 | 59 | 99.35% | 0% | 99.35% | 0.33% | 0.31% |
| main | monthly | SSC | SSC_flag | 18,824 | 0 | 18,563 | 18,563 | 167 | 94 | 0% | 98.61% | 98.61% | 0.89% | 0.50% |
| main | monthly | SSL | SSL_flag | 18,824 | 3,258 | 15,397 | 18,655 | 169 | 0 | 17.31% | 81.79% | 99.10% | 0.90% | 0% |
| main | annual | Q | Q_flag | 468 | 342 | 0 | 342 | 6 | 120 | 73.08% | 0% | 73.08% | 1.28% | 25.64% |
| main | annual | SSC | SSC_flag | 468 | 109 | 336 | 445 | 23 | 0 | 23.29% | 71.79% | 95.09% | 4.91% | 0% |
| main | annual | SSL | SSL_flag | 468 | 340 | 0 | 340 | 8 | 120 | 72.65% | 0% | 72.65% | 1.71% | 25.64% |

## Final Good With Missing Stage QC

Rows are restricted to final good records (`*_flag == 0`) in the master release product; rates use `final_good_count` as denominator.

| product group | release component | source dataset | temporal resolution | variable | qc stage | stage flag variable | final good count | stage missing count | stage missing rate | stage not checked count | stage not checked rate | stage pass count | stage pass rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| main | master | USGS | daily | SSC | log_iqr | SSC_qc2 | 1,624,112 | 1,624,112 | 100% | 0 | 0% | 0 | 0% |
| main | master | USGS | daily | SSC | physical_plausibility | SSC_qc1 | 1,624,112 | 1,624,112 | 100% | 0 | 0% | 0 | 0% |
| main | master | USGS | daily | SSC | ssc_q_consistency | SSC_qc3 | 1,624,112 | 1,624,112 | 100% | 0 | 0% | 0 | 0% |
| main | master | USGS | daily | Q | log_iqr | Q_qc2 | 1,609,073 | 1,609,073 | 100% | 0 | 0% | 0 | 0% |
| main | master | USGS | daily | Q | physical_plausibility | Q_qc1 | 1,609,073 | 1,609,073 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | daily | Q | log_iqr | Q_qc2 | 49,919 | 49,919 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | daily | Q | physical_plausibility | Q_qc1 | 49,919 | 49,919 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | daily | SSC | ssc_q_consistency | SSC_qc3 | 40,307 | 40,307 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | daily | SSC | physical_plausibility | SSC_qc1 | 40,307 | 40,307 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | daily | SSC | log_iqr | SSC_qc2 | 40,307 | 40,307 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | monthly | Q | log_iqr | Q_qc2 | 15,499 | 15,499 | 100% | 0 | 0% | 0 | 0% |
| main | master | EUSEDcollab | monthly | Q | physical_plausibility | Q_qc1 | 15,499 | 15,499 | 100% | 0 | 0% | 0 | 0% |
| main | master | HYBAM | daily | SSC | log_iqr | SSC_qc2 | 9,214 | 7,864 | 85.35% | 1 | 0.01% | 1,336 | 14.50% |
| main | master | HYBAM | daily | SSC | physical_plausibility | SSC_qc1 | 9,214 | 7,864 | 85.35% | 0 | 0% | 1,350 | 14.65% |
| main | master | HYBAM | daily | SSC | ssc_q_consistency | SSC_qc3 | 9,214 | 7,864 | 85.35% | 926 | 10.05% | 421 | 4.57% |
| main | master | Eurasian_River | monthly | SSL | ssc_q_consistency | SSL_qc3 | 3,236 | 3,236 | 100% | 0 | 0% | 0 | 0% |
| main | master | Fukushima | daily | Q | log_iqr | Q_qc2 | 3,034 | 3,034 | 100% | 0 | 0% | 0 | 0% |
| main | master | Fukushima | daily | SSC | ssc_q_consistency | SSC_qc3 | 3,023 | 3,023 | 100% | 0 | 0% | 0 | 0% |
| main | master | Fukushima | daily | SSC | log_iqr | SSC_qc2 | 3,023 | 3,023 | 100% | 0 | 0% | 0 | 0% |
| main | master | HYBAM | daily | Q | physical_plausibility | Q_qc1 | 9,054 | 735 | 8.12% | 0 | 0% | 8,319 | 91.88% |
| main | master | HYBAM | daily | Q | log_iqr | Q_qc2 | 9,054 | 735 | 8.12% | 0 | 0% | 8,277 | 91.42% |
| main | master | Fukushima | daily | Q | physical_plausibility | Q_qc1 | 3,034 | 387 | 12.76% | 0 | 0% | 2,647 | 87.24% |
| main | master | Fukushima | daily | SSC | physical_plausibility | SSC_qc1 | 3,023 | 386 | 12.77% | 0 | 0% | 2,637 | 87.23% |
| main | master | Shashi_Jianli | daily | Q | log_iqr | Q_qc2 | 154 | 154 | 100% | 0 | 0% | 0 | 0% |

_Showing first 24 of 102 rows._

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | product group | release component | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | main | master | Q | Q_flag | 0 | good | 2,790,906 | 91.09% | 3,063,758 |
| all_release_sources | all | final | final | main | master | Q | Q_flag | 2 | suspect | 60,646 | 1.98% | 3,063,758 |
| all_release_sources | all | final | final | main | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 3,063,758 |
| all_release_sources | all | final | final | main | master | Q | Q_flag | 9 | missing | 211,651 | 6.91% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | main | master | Q | Q_qc1 | 0 | pass | 1,129,367 | 36.86% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | main | master | Q | Q_qc1 | 9 | missing | 1,934,391 | 63.14% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | main | master | Q | Q_qc2 | 0 | pass | 1,087,364 | 35.49% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | main | master | Q | Q_qc2 | 2 | suspect | 14,157 | 0.46% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | main | master | Q | Q_qc2 | 8 | not_checked | 25,169 | 0.82% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | main | master | Q | Q_qc2 | 9 | missing | 1,937,068 | 63.23% | 3,063,758 |
| all_release_sources | all | final | final | main | master | SSC | SSC_flag | 0 | good | 2,914,450 | 95.13% | 3,063,758 |
| all_release_sources | all | final | final | main | master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,063,758 |
| all_release_sources | all | final | final | main | master | SSC | SSC_flag | 2 | suspect | 110,564 | 3.61% | 3,063,758 |
| all_release_sources | all | final | final | main | master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | main | master | SSC | SSC_qc1 | 0 | pass | 1,292,590 | 42.19% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | main | master | SSC | SSC_qc1 | 9 | missing | 1,771,168 | 57.81% | 3,063,758 |

_Showing first 16 of 74 rows._

## Flag Counts by True Temporal Resolution

This table uses the main/master record-level `resolution` variable (`daily`, `monthly`, `annual`), not the release component name.

| product group | temporal resolution | qc level | qc stage | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|
| main | daily | final | final | Q | Q_flag | 0 | good | 2,771,862 | 91.05% | 3,044,466 |
| main | daily | final | final | Q | Q_flag | 2 | suspect | 60,577 | 1.99% | 3,044,466 |
| main | daily | final | final | Q | Q_flag | 3 | bad | 555 | 0.02% | 3,044,466 |
| main | daily | final | final | Q | Q_flag | 9 | missing | 211,472 | 6.95% | 3,044,466 |
| main | daily | stage | physical_plausibility | Q | Q_qc1 | 0 | pass | 1,125,815 | 36.98% | 3,044,466 |
| main | daily | stage | physical_plausibility | Q | Q_qc1 | 9 | missing | 1,918,651 | 63.02% | 3,044,466 |
| main | daily | stage | log_iqr | Q | Q_qc2 | 0 | pass | 1,083,830 | 35.60% | 3,044,466 |
| main | daily | stage | log_iqr | Q | Q_qc2 | 2 | suspect | 14,150 | 0.46% | 3,044,466 |
| main | daily | stage | log_iqr | Q | Q_qc2 | 8 | not_checked | 25,158 | 0.83% | 3,044,466 |
| main | daily | stage | log_iqr | Q | Q_qc2 | 9 | missing | 1,921,328 | 63.11% | 3,044,466 |
| main | daily | final | final | SSC | SSC_flag | 0 | good | 2,914,341 | 95.73% | 3,044,466 |
| main | daily | final | final | SSC | SSC_flag | 1 | derived | 9,224 | 0.30% | 3,044,466 |
| main | daily | final | final | SSC | SSC_flag | 2 | suspect | 110,374 | 3.63% | 3,044,466 |
| main | daily | final | final | SSC | SSC_flag | 9 | missing | 10,527 | 0.35% | 3,044,466 |
| main | daily | stage | physical_plausibility | SSC | SSC_qc1 | 0 | pass | 1,288,929 | 42.34% | 3,044,466 |
| main | daily | stage | physical_plausibility | SSC | SSC_qc1 | 9 | missing | 1,755,537 | 57.66% | 3,044,466 |

_Showing first 16 of 93 rows._

## Flag Counts by Variable

| qc level | qc stage | variable | flag variable | flag | meaning | count | n total | percentage |
|---|---|---|---|---|---|---|---|---|
| final | final | Q | Q_flag | 0 | good | 2,924,302 | 19,543,395 | 14.96% |
| final | final | Q | Q_flag | 2 | suspect | 61,855 | 19,543,395 | 0.32% |
| final | final | Q | Q_flag | 3 | bad | 555 | 19,543,395 | 0.00% |
| final | final | Q | Q_flag | 9 | missing | 16,556,683 | 19,543,395 | 84.72% |
| final | final | SSC | SSC_flag | 0 | good | 18,036,301 | 19,543,395 | 92.29% |
| final | final | SSC | SSC_flag | 1 | derived | 28,170 | 19,543,395 | 0.14% |
| final | final | SSC | SSC_flag | 2 | suspect | 506,950 | 19,543,395 | 2.59% |
| final | final | SSC | SSC_flag | 9 | missing | 971,974 | 19,543,395 | 4.97% |
| final | final | SSL | SSL_flag | 0 | good | 149,130 | 19,543,395 | 0.76% |
| final | final | SSL | SSL_flag | 1 | derived | 2,700,030 | 19,543,395 | 13.82% |
| final | final | SSL | SSL_flag | 2 | suspect | 148,052 | 19,543,395 | 0.76% |
| final | final | SSL | SSL_flag | 3 | bad | 555 | 19,543,395 | 0.00% |
| final | final | SSL | SSL_flag | 9 | missing | 16,545,628 | 19,543,395 | 84.66% |
| stage | log_iqr | Q | Q_qc2 | 0 | pass | 1,087,364 | 3,065,119 | 35.48% |
| stage | log_iqr | Q | Q_qc2 | 2 | suspect | 14,157 | 3,065,119 | 0.46% |
| stage | log_iqr | Q | Q_qc2 | 8 | not_checked | 25,702 | 3,065,119 | 0.84% |

_Showing first 16 of 40 rows._

## Problem Clusters

Rows are true cluster-level final-flag summaries for release products that carry SED cluster identifiers; `suspect_bad` combines flags 2 and 3.

| cluster uid | cluster id | grouping level | product group | release component | temporal resolution | variable | flag variable | n records | analysis ready count | suspect bad count | missing count | analysis ready rate | suspect bad rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| SED040298 | 40,298 | cluster_variable_resolution | main | master | daily | SSC | SSC_flag | 11,689 | 9,920 | 1,769 | 0 | 84.87% | 15.13% | 0% |
| SED040298 | 40,298 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 11,689 | 9,920 | 1,769 | 0 | 84.87% | 15.13% | 0% |
| SED000062 | 62 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 15,179 | 12,508 | 1,574 | 1,097 | 82.40% | 10.37% | 7.23% |
| SED040648 | 40,648 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 15,979 | 14,566 | 1,413 | 0 | 91.16% | 8.84% | 0% |
| SED040530 | 40,530 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 5,386 | 4,017 | 1,369 | 0 | 74.58% | 25.42% | 0% |
| SED040528 | 40,528 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 15,957 | 14,668 | 1,288 | 1 | 91.92% | 8.07% | 0.01% |
| SED040717 | 40,717 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 10,451 | 9,229 | 1,222 | 0 | 88.31% | 11.69% | 0% |
| SED040685 | 40,685 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 2,092 | 898 | 1,194 | 0 | 42.93% | 57.07% | 0% |
| SED040522 | 40,522 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 16,323 | 15,133 | 1,190 | 0 | 92.71% | 7.29% | 0% |
| SED000052 | 52 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 21,898 | 20,717 | 1,181 | 0 | 94.61% | 5.39% | 0% |
| SED000061 | 61 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 15,223 | 14,047 | 1,176 | 0 | 92.27% | 7.73% | 0% |
| SED040685 | 40,685 | cluster_variable_resolution | main | master | daily | Q | Q_flag | 2,092 | 928 | 1,164 | 0 | 44.36% | 55.64% | 0% |
| SED040518 | 40,518 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 16,323 | 15,159 | 1,164 | 0 | 92.87% | 7.13% | 0% |
| SED000034 | 34 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 21,909 | 20,777 | 1,132 | 0 | 94.83% | 5.17% | 0% |
| SED000031 | 31 | cluster_variable_resolution | main | master | daily | SSL | SSL_flag | 21,843 | 20,711 | 1,132 | 0 | 94.82% | 5.18% | 0% |
| SED000034 | 34 | cluster_variable_resolution | main | master | daily | SSC | SSC_flag | 21,909 | 20,780 | 1,129 | 0 | 94.85% | 5.15% | 0% |

_Showing first 16 of 41,645 rows._

## Yearly Trends

Rows use final flag variables only (`*_flag`), grouped by `year x temporal_resolution x variable`; stage QC flags are excluded from the denominator.

| product group | release component | year | temporal resolution | variable | flag variable | n records | analysis ready count | suspect bad count | missing count | analysis ready rate | suspect bad rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| main | master | 2,025 | daily | SSC | SSC_flag | 10,471 | 8,818 | 1,653 | 0 | 84.21% | 15.79% | 0% |
| main | master | 2,025 | daily | SSL | SSL_flag | 10,471 | 8,057 | 1,238 | 1,176 | 76.95% | 11.82% | 11.23% |
| main | master | 2,025 | daily | Q | Q_flag | 10,471 | 9,264 | 31 | 1,176 | 88.47% | 0.30% | 11.23% |
| main | master | 2,024 | daily | SSL | SSL_flag | 29,800 | 25,792 | 1,964 | 2,044 | 86.55% | 6.59% | 6.86% |
| main | master | 2,024 | daily | SSC | SSC_flag | 29,800 | 28,383 | 1,417 | 0 | 95.24% | 4.76% | 0% |
| main | master | 2,024 | daily | Q | Q_flag | 29,800 | 26,981 | 775 | 2,044 | 90.54% | 2.60% | 6.86% |
| main | master | 2,023 | daily | SSC | SSC_flag | 37,323 | 36,033 | 1,290 | 0 | 96.54% | 3.46% | 0% |
| main | master | 2,023 | daily | Q | Q_flag | 37,323 | 33,305 | 1,448 | 2,570 | 89.23% | 3.88% | 6.89% |
| main | master | 2,023 | daily | SSL | SSL_flag | 37,323 | 32,284 | 2,469 | 2,570 | 86.50% | 6.62% | 6.89% |
| main | master | 2,022 | daily | Q | Q_flag | 37,061 | 33,435 | 511 | 3,115 | 90.22% | 1.38% | 8.41% |
| main | master | 2,022 | daily | SSC | SSC_flag | 37,061 | 35,565 | 1,496 | 0 | 95.96% | 4.04% | 0% |
| main | master | 2,022 | daily | SSL | SSL_flag | 37,061 | 32,178 | 1,768 | 3,115 | 86.82% | 4.77% | 8.41% |
| main | master | 2,021 | daily | SSC | SSC_flag | 46,479 | 44,920 | 1,559 | 0 | 96.65% | 3.35% | 0% |
| main | master | 2,021 | daily | Q | Q_flag | 46,479 | 37,063 | 559 | 8,857 | 79.74% | 1.20% | 19.06% |
| main | master | 2,021 | daily | SSL | SSL_flag | 46,479 | 35,918 | 1,704 | 8,857 | 77.28% | 3.67% | 19.06% |
| main | master | 2,020 | annual | SSL | SSL_flag | 7 | 5 | 2 | 0 | 71.43% | 28.57% | 0% |
| main | master | 2,020 | annual | SSC | SSC_flag | 7 | 5 | 2 | 0 | 71.43% | 28.57% | 0% |
| main | master | 2,020 | annual | Q | Q_flag | 7 | 6 | 1 | 0 | 85.71% | 14.29% | 0% |

_Showing first 18 of 1,233 rows._

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Flag values partition into analysis-ready (0-1: good + derived/estimated), suspect/bad (2-3), and missing (9); `problem_count` covers suspect/bad only.
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
