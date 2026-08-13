# Release QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 83,151,137
- Final flag rows: 29
- Stage flag rows: 45
- Usable flag count from health KPIs: 27,997,668
- Problem flag count from health KPIs: 50,256,159
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

| temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|
| satellite | SSL | SSL_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | Q | Q_flag | 9 | missing | 16,344,453 | 99.19% | 16,478,276 |
| satellite | SSC | SSC_flag | 0 | good | 15,121,092 | 91.76% | 16,478,276 |
| master | SSC | SSC_flag | 0 | good | 2,914,450 | 95.13% | 3,063,758 |
| master | Q | Q_flag | 0 | good | 2,790,906 | 91.09% | 3,063,758 |
| master | SSL | SSL_flag | 1 | derived | 2,700,030 | 88.13% | 3,063,758 |
| satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| master | Q | Q_flag | 9 | missing | 211,651 | 6.91% | 3,063,758 |
| master | SSL | SSL_flag | 9 | missing | 201,151 | 6.57% | 3,063,758 |
| master | SSL | SSL_flag | 2 | suspect | 146,922 | 4.80% | 3,063,758 |
| satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| master | SSC | SSC_flag | 2 | suspect | 110,564 | 3.61% | 3,063,758 |
| master | Q | Q_flag | 2 | suspect | 60,646 | 1.98% | 3,063,758 |
| master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,063,758 |
| master | SSL | SSL_flag | 0 | good | 15,100 | 0.49% | 3,063,758 |
| master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,063,758 |
| climatology | SSL | SSL_flag | 0 | good | 1,337 | 98.24% | 1,361 |
| satellite | Q | Q_flag | 2 | suspect | 1,209 | 0.01% | 16,478,276 |
| satellite | SSL | SSL_flag | 2 | suspect | 1,130 | 0.01% | 16,478,276 |
| climatology | Q | Q_flag | 0 | good | 782 | 57.46% | 1,361 |
| climatology | SSC | SSC_flag | 0 | good | 759 | 55.77% | 1,361 |
| climatology | Q | Q_flag | 9 | missing | 579 | 42.54% | 1,361 |

_Showing first 24 of 29 rows._

## Stage Flag Summary

| temporal resolution | variable | qc stage | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|
| master | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 2,326,872 | 75.95% | 3,063,758 |
| master | Q | log_iqr | Q_qc2 | 9 | missing | 1,937,068 | 63.23% | 3,063,758 |
| master | SSL | log_iqr | SSL_qc2 | 9 | missing | 1,935,215 | 63.16% | 3,063,758 |
| master | Q | physical_plausibility | Q_qc1 | 9 | missing | 1,934,391 | 63.14% | 3,063,758 |
| master | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 1,931,968 | 63.06% | 3,063,758 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 1,777,038 | 58.00% | 3,063,758 |
| master | SSC | log_iqr | SSC_qc2 | 9 | missing | 1,773,845 | 57.90% | 3,063,758 |
| master | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 1,771,168 | 57.81% | 3,063,758 |
| master | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 1,292,590 | 42.19% | 3,063,758 |
| master | SSC | log_iqr | SSC_qc2 | 0 | pass | 1,209,900 | 39.49% | 3,063,758 |
| master | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 1,131,790 | 36.94% | 3,063,758 |
| master | Q | physical_plausibility | Q_qc1 | 0 | pass | 1,129,367 | 36.86% | 3,063,758 |
| master | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 1,113,643 | 36.35% | 3,063,758 |
| master | Q | log_iqr | Q_qc2 | 0 | pass | 1,087,364 | 35.49% | 3,063,758 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 1,019,099 | 33.26% | 3,063,758 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 727,497 | 23.75% | 3,063,758 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 253,504 | 8.27% | 3,063,758 |
| master | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 46,632 | 1.52% | 3,063,758 |
| master | SSC | log_iqr | SSC_qc2 | 2 | suspect | 33,381 | 1.09% | 3,063,758 |
| master | Q | log_iqr | Q_qc2 | 8 | not_checked | 25,169 | 0.82% | 3,063,758 |
| master | SSL | log_iqr | SSL_qc2 | 0 | pass | 14,825 | 0.48% | 3,063,758 |
| master | Q | log_iqr | Q_qc2 | 2 | suspect | 14,157 | 0.46% | 3,063,758 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 2 | suspect | 14,117 | 0.46% | 3,063,758 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 2 | suspect | 9,362 | 0.31% | 3,063,758 |

_Showing first 24 of 45 rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 132,614 | 16,345,662 | 16,344,453 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 132,693 | 16,345,583 | 16,344,453 | 0.81% | 0.81% | 99.19% | 99.19% |
| master | SSL | SSL_qc3 | 3,063,758 | 27 | 0 | 27 | 2,336,234 | 2,326,872 | 0.00% | 0.00% | 76.25% | 75.95% |
| master | Q | Q_qc2 | 3,063,758 | 1,087,364 | 0 | 1,087,364 | 1,951,225 | 1,937,068 | 35.49% | 35.49% | 63.69% | 63.23% |
| master | SSL | SSL_qc2 | 3,063,758 | 14,825 | 0 | 14,825 | 1,935,290 | 1,935,215 | 0.48% | 0.48% | 63.17% | 63.16% |
| master | Q | Q_qc1 | 3,063,758 | 1,129,367 | 0 | 1,129,367 | 1,934,391 | 1,934,391 | 36.86% | 36.86% | 63.14% | 63.14% |
| master | SSL | SSL_qc1 | 3,063,758 | 1,131,790 | 0 | 1,131,790 | 1,931,968 | 1,931,968 | 36.94% | 36.94% | 63.06% | 63.06% |
| master | SSC | SSC_qc2 | 3,063,758 | 1,209,900 | 0 | 1,209,900 | 1,807,226 | 1,773,845 | 39.49% | 39.49% | 58.99% | 57.90% |
| master | SSC | SSC_qc3 | 3,063,758 | 1,019,099 | 0 | 1,019,099 | 1,791,155 | 1,777,038 | 33.26% | 33.26% | 58.46% | 58.00% |
| master | SSC | SSC_qc1 | 3,063,758 | 1,292,590 | 0 | 1,292,590 | 1,771,168 | 1,771,168 | 42.19% | 42.19% | 57.81% | 57.81% |
| satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 15,121,092 | 1,357,184 | 960,798 | 91.76% | 91.76% | 8.24% | 5.83% |
| master | SSL | SSL_flag | 3,063,758 | 15,100 | 0 | 15,100 | 348,628 | 201,151 | 0.49% | 0.49% | 11.38% | 6.57% |
| master | Q | Q_flag | 3,063,758 | 2,790,906 | 0 | 2,790,906 | 272,852 | 211,651 | 91.09% | 91.09% | 8.91% | 6.91% |
| master | SSC | SSC_flag | 3,063,758 | 2,914,450 | 0 | 2,914,450 | 121,185 | 10,621 | 95.13% | 95.13% | 3.96% | 0.35% |
| climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 0 | 828 | 828 | 0% | 0% | 60.84% | 60.84% |
| climatology | SSL | SSL_qc3 | 1,361 | 0 | 0 | 0 | 828 | 828 | 0% | 0% | 60.84% | 60.84% |
| climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 0 | 828 | 828 | 0% | 0% | 60.84% | 60.84% |
| climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 24 | 804 | 804 | 1.76% | 1.76% | 59.07% | 59.07% |
| climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 24 | 804 | 804 | 1.76% | 1.76% | 59.07% | 59.07% |
| climatology | Q | Q_qc1 | 1,361 | 782 | 0 | 782 | 579 | 579 | 57.46% | 57.46% | 42.54% | 42.54% |
| climatology | Q | Q_flag | 1,361 | 782 | 0 | 782 | 579 | 579 | 57.46% | 57.46% | 42.54% | 42.54% |
| climatology | SSC | SSC_flag | 1,361 | 759 | 0 | 759 | 555 | 555 | 55.77% | 55.77% | 40.78% | 40.78% |
| climatology | SSC | SSC_qc1 | 1,361 | 776 | 30 | 806 | 555 | 555 | 57.02% | 59.22% | 40.78% | 40.78% |
| climatology | SSL | SSL_qc1 | 1,361 | 1,337 | 0 | 1,337 | 24 | 24 | 98.24% | 98.24% | 1.76% | 1.76% |

_Showing first 24 of 25 rows._

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 99.20% | 198.38% |
| product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 99.19% | 198.38% |
| product_variable | all_release_sources | master | SSL | SSL_qc3 | 3,063,758 | 27 | 2,336,234 | 5,390,603 | 0.00% | 76.25% | 175.95% |
| product_variable | all_release_sources | master | SSL | SSL_qc2 | 3,063,758 | 14,825 | 1,935,290 | 4,984,148 | 0.48% | 63.17% | 162.68% |
| product_variable | all_release_sources | master | Q | Q_qc2 | 3,063,758 | 1,087,364 | 1,951,225 | 3,913,462 | 35.49% | 63.69% | 127.73% |
| product_variable | all_release_sources | master | Q | Q_qc1 | 3,063,758 | 1,129,367 | 1,934,391 | 3,868,782 | 36.86% | 63.14% | 126.28% |
| product_variable | all_release_sources | master | SSL | SSL_qc1 | 3,063,758 | 1,131,790 | 1,931,968 | 3,863,936 | 36.94% | 63.06% | 126.12% |
| product_variable | all_release_sources | master | SSC | SSC_qc3 | 3,063,758 | 1,019,099 | 1,791,155 | 3,821,697 | 33.26% | 58.46% | 124.74% |
| product_variable | all_release_sources | master | SSC | SSC_qc2 | 3,063,758 | 1,209,900 | 1,807,226 | 3,627,703 | 39.49% | 58.99% | 118.41% |
| product_variable | all_release_sources | master | SSC | SSC_qc1 | 3,063,758 | 1,292,590 | 1,771,168 | 3,542,336 | 42.19% | 57.81% | 115.62% |
| product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 1,357,184 | 2,317,982 | 91.76% | 8.24% | 14.07% |
| product_variable | all_release_sources | master | SSL | SSL_flag | 3,063,758 | 15,100 | 348,628 | 549,779 | 0.49% | 11.38% | 17.94% |
| product_variable | all_release_sources | master | Q | Q_flag | 3,063,758 | 2,790,906 | 272,852 | 484,503 | 91.09% | 8.91% | 15.81% |
| product_variable | all_release_sources | master | SSC | SSC_flag | 3,063,758 | 2,914,450 | 121,185 | 131,806 | 95.13% | 3.96% | 4.30% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,361 | 0 | 828 | 2,189 | 0% | 60.84% | 160.84% |
| product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,361 | 0 | 828 | 2,189 | 0% | 60.84% | 160.84% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,361 | 0 | 828 | 2,189 | 0% | 60.84% | 160.84% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,361 | 24 | 804 | 2,141 | 1.76% | 59.07% | 157.31% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,361 | 24 | 804 | 2,141 | 1.76% | 59.07% | 157.31% |
| product_variable | all_release_sources | climatology | Q | Q_flag | 1,361 | 782 | 579 | 1,158 | 57.46% | 42.54% | 85.08% |

_Showing first 20 of 25 rows._

## Stage Effectiveness

| temporal resolution | variable | qc stage | flag variable | n total | good count | bad count | not checked count | missing count | good rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| master | SSL | ssc_q_consistency | SSL_qc3 | 3,063,758 | 27 | 0 | 727,497 | 2,326,872 | 0.00% | 76.25% | 75.95% |
| master | Q | log_iqr | Q_qc2 | 3,063,758 | 1,087,364 | 0 | 25,169 | 1,937,068 | 35.49% | 63.69% | 63.23% |
| master | SSL | log_iqr | SSL_qc2 | 3,063,758 | 14,825 | 0 | 1,113,643 | 1,935,215 | 0.48% | 63.17% | 63.16% |
| master | Q | physical_plausibility | Q_qc1 | 3,063,758 | 1,129,367 | 0 | 0 | 1,934,391 | 36.86% | 63.14% | 63.14% |
| master | SSL | physical_plausibility | SSL_qc1 | 3,063,758 | 1,131,790 | 0 | 0 | 1,931,968 | 36.94% | 63.06% | 63.06% |
| master | SSC | log_iqr | SSC_qc2 | 3,063,758 | 1,209,900 | 0 | 46,632 | 1,773,845 | 39.49% | 58.99% | 57.90% |
| master | SSC | ssc_q_consistency | SSC_qc3 | 3,063,758 | 1,019,099 | 0 | 253,504 | 1,777,038 | 33.26% | 58.46% | 58.00% |
| master | SSC | physical_plausibility | SSC_qc1 | 3,063,758 | 1,292,590 | 0 | 0 | 1,771,168 | 42.19% | 57.81% | 57.81% |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 1,361 | 0 | 0 | 533 | 828 | 0% | 60.84% | 60.84% |
| climatology | SSL | log_iqr | SSL_qc2 | 1,361 | 0 | 0 | 533 | 828 | 0% | 60.84% | 60.84% |
| climatology | Q | log_iqr | Q_qc2 | 1,361 | 0 | 0 | 533 | 828 | 0% | 60.84% | 60.84% |
| climatology | SSC | log_iqr | SSC_qc2 | 1,361 | 24 | 0 | 533 | 804 | 1.76% | 59.07% | 59.07% |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 1,361 | 24 | 0 | 533 | 804 | 1.76% | 59.07% | 59.07% |
| climatology | Q | physical_plausibility | Q_qc1 | 1,361 | 782 | 0 | 0 | 579 | 57.46% | 42.54% | 42.54% |
| climatology | SSC | physical_plausibility | SSC_qc1 | 1,361 | 776 | 0 | 0 | 555 | 57.02% | 40.78% | 40.78% |
| climatology | SSL | physical_plausibility | SSL_qc1 | 1,361 | 1,337 | 0 | 0 | 24 | 98.24% | 1.76% | 1.76% |

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | master | Q | Q_flag | 0 | good | 2,790,906 | 91.09% | 3,063,758 |
| all_release_sources | all | final | final | master | Q | Q_flag | 2 | suspect | 60,646 | 1.98% | 3,063,758 |
| all_release_sources | all | final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 3,063,758 |
| all_release_sources | all | final | final | master | Q | Q_flag | 9 | missing | 211,651 | 6.91% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 1,129,367 | 36.86% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 1,934,391 | 63.14% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 1,087,364 | 35.49% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 14,157 | 0.46% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 25,169 | 0.82% | 3,063,758 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 1,937,068 | 63.23% | 3,063,758 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 0 | good | 2,914,450 | 95.13% | 3,063,758 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,063,758 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 2 | suspect | 110,564 | 3.61% | 3,063,758 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 1,292,590 | 42.19% | 3,063,758 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 1,771,168 | 57.81% | 3,063,758 |

_Showing first 16 of 74 rows._

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | master | Q | Q_flag | 0 | good | 2,790,906 | 91.09% | 3,063,758 |
| final | final | master | Q | Q_flag | 2 | suspect | 60,646 | 1.98% | 3,063,758 |
| final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 3,063,758 |
| final | final | master | Q | Q_flag | 9 | missing | 211,651 | 6.91% | 3,063,758 |
| stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 1,129,367 | 36.86% | 3,063,758 |
| stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 1,934,391 | 63.14% | 3,063,758 |
| stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 1,087,364 | 35.49% | 3,063,758 |
| stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 14,157 | 0.46% | 3,063,758 |
| stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 25,169 | 0.82% | 3,063,758 |
| stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 1,937,068 | 63.23% | 3,063,758 |
| final | final | master | SSC | SSC_flag | 0 | good | 2,914,450 | 95.13% | 3,063,758 |
| final | final | master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,063,758 |
| final | final | master | SSC | SSC_flag | 2 | suspect | 110,564 | 3.61% | 3,063,758 |
| final | final | master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,063,758 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 1,292,590 | 42.19% | 3,063,758 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 1,771,168 | 57.81% | 3,063,758 |

_Showing first 16 of 74 rows._

## Flag Counts by Variable

| qc level | qc stage | variable | flag variable | flag | meaning | count | n total | percentage |
|---|---|---|---|---|---|---|---|---|
| final | final | Q | Q_flag | 0 | good | 2,924,302 | 19,543,395 | 14.96% |
| final | final | Q | Q_flag | 2 | suspect | 61,855 | 19,542,034 | 0.32% |
| final | final | Q | Q_flag | 3 | bad | 555 | 3,063,758 | 0.02% |
| final | final | Q | Q_flag | 9 | missing | 16,556,683 | 19,543,395 | 84.72% |
| final | final | SSC | SSC_flag | 0 | good | 18,036,301 | 19,543,395 | 92.29% |
| final | final | SSC | SSC_flag | 1 | derived | 28,170 | 3,065,119 | 0.92% |
| final | final | SSC | SSC_flag | 2 | suspect | 506,950 | 19,542,034 | 2.59% |
| final | final | SSC | SSC_flag | 9 | missing | 971,974 | 19,543,395 | 4.97% |
| final | final | SSL | SSL_flag | 0 | good | 149,130 | 19,543,395 | 0.76% |
| final | final | SSL | SSL_flag | 1 | derived | 2,700,030 | 3,063,758 | 88.13% |
| final | final | SSL | SSL_flag | 2 | suspect | 148,052 | 19,542,034 | 0.76% |
| final | final | SSL | SSL_flag | 3 | bad | 555 | 3,063,758 | 0.02% |
| final | final | SSL | SSL_flag | 9 | missing | 16,545,628 | 19,543,395 | 84.66% |
| stage | log_iqr | Q | Q_qc2 | 0 | pass | 1,087,364 | 3,063,758 | 35.49% |
| stage | log_iqr | Q | Q_qc2 | 2 | suspect | 14,157 | 3,063,758 | 0.46% |
| stage | log_iqr | Q | Q_qc2 | 8 | not_checked | 25,702 | 3,065,119 | 0.84% |

_Showing first 16 of 40 rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 1,209 | 0 | 0 | 16,344,453 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.38% |
|  |  | product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 1,130 | 0 | 0 | 16,344,453 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 0% | 0.01% | 0% | 0% | 99.19% | 0.81% | 99.19% | 198.38% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc3 | 3,063,758 | 27 | 0 | 9,362 | 0 | 727,497 | 2,326,872 | 27 | 2,336,234 | 5,390,603 | 0.00% | 0% | 0.31% | 0% | 23.75% | 75.95% | 0.00% | 76.25% | 175.95% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc2 | 3,063,758 | 14,825 | 0 | 75 | 0 | 1,113,643 | 1,935,215 | 14,825 | 1,935,290 | 4,984,148 | 0.48% | 0% | 0.00% | 0% | 36.35% | 63.16% | 0.48% | 63.17% | 162.68% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 0 | 0 | 533 | 804 | 24 | 804 | 2,141 | 1.76% | 0% | 0% | 0% | 39.16% | 59.07% | 1.76% | 59.07% | 157.31% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 0 | 0 | 533 | 804 | 24 | 804 | 2,141 | 1.76% | 0% | 0% | 0% | 39.16% | 59.07% | 1.76% | 59.07% | 157.31% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc2 | 3,063,758 | 1,087,364 | 0 | 14,157 | 0 | 25,169 | 1,937,068 | 1,087,364 | 1,951,225 | 3,913,462 | 35.49% | 0% | 0.46% | 0% | 0.82% | 63.23% | 35.49% | 63.69% | 127.73% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc1 | 3,063,758 | 1,129,367 | 0 | 0 | 0 | 0 | 1,934,391 | 1,129,367 | 1,934,391 | 3,868,782 | 36.86% | 0% | 0% | 0% | 0% | 63.14% | 36.86% | 63.14% | 126.28% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc1 | 3,063,758 | 1,131,790 | 0 | 0 | 0 | 0 | 1,931,968 | 1,131,790 | 1,931,968 | 3,863,936 | 36.94% | 0% | 0% | 0% | 0% | 63.06% | 36.94% | 63.06% | 126.12% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc3 | 3,063,758 | 1,019,099 | 0 | 14,117 | 0 | 253,504 | 1,777,038 | 1,019,099 | 1,791,155 | 3,821,697 | 33.26% | 0% | 0.46% | 0% | 8.27% | 58.00% | 33.26% | 58.46% | 124.74% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc2 | 3,063,758 | 1,209,900 | 0 | 33,381 | 0 | 46,632 | 1,773,845 | 1,209,900 | 1,807,226 | 3,627,703 | 39.49% | 0% | 1.09% | 0% | 1.52% | 57.90% | 39.49% | 58.99% | 118.41% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc1 | 3,063,758 | 1,292,590 | 0 | 0 | 0 | 0 | 1,771,168 | 1,292,590 | 1,771,168 | 3,542,336 | 42.19% | 0% | 0% | 0% | 0% | 57.81% | 42.19% | 57.81% | 115.62% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc1 | 1,361 | 782 | 0 | 0 | 0 | 0 | 579 | 782 | 579 | 1,158 | 57.46% | 0% | 0% | 0% | 0% | 42.54% | 57.46% | 42.54% | 85.08% |

_Showing first 16 of 25 rows._

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 4,083 | 8,166 | 50% |
| all | climatology | SSC | 5,444 | 16,332 | 33.33% |
| all | climatology | SSL | 5,444 | 10,888 | 50% |
| all | master | Q | 9,191,274 | 30,637,580 | 30% |
| all | master | SSC | 12,255,032 | 42,892,612 | 28.57% |
| all | master | SSL | 12,255,032 | 45,956,370 | 26.67% |
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
