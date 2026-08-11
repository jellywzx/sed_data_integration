# Release QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/qc_flags/tables`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 83,161,180
- Final flag rows: 29
- Stage flag rows: 45
- Usable flag count from health KPIs: 28,002,761
- Problem flag count from health KPIs: 50,258,995
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
| master | SSC | SSC_flag | 0 | good | 2,915,287 | 95.13% | 3,064,671 |
| master | Q | Q_flag | 0 | good | 2,791,428 | 91.08% | 3,064,671 |
| master | SSL | SSL_flag | 1 | derived | 2,700,499 | 88.12% | 3,064,671 |
| satellite | SSC | SSC_flag | 9 | missing | 960,798 | 5.83% | 16,478,276 |
| satellite | SSC | SSC_flag | 2 | suspect | 396,386 | 2.41% | 16,478,276 |
| master | Q | Q_flag | 9 | missing | 212,010 | 6.92% | 3,064,671 |
| master | SSL | SSL_flag | 9 | missing | 201,510 | 6.58% | 3,064,671 |
| master | SSL | SSL_flag | 2 | suspect | 147,007 | 4.80% | 3,064,671 |
| satellite | SSL | SSL_flag | 0 | good | 132,693 | 0.81% | 16,478,276 |
| satellite | Q | Q_flag | 0 | good | 132,614 | 0.80% | 16,478,276 |
| master | SSC | SSC_flag | 2 | suspect | 110,640 | 3.61% | 3,064,671 |
| master | Q | Q_flag | 2 | suspect | 60,678 | 1.98% | 3,064,671 |
| master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,064,671 |
| master | SSL | SSL_flag | 0 | good | 15,100 | 0.49% | 3,064,671 |
| master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,064,671 |
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
| master | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 2,327,231 | 75.94% | 3,064,671 |
| master | Q | log_iqr | Q_qc2 | 9 | missing | 1,937,427 | 63.22% | 3,064,671 |
| master | SSL | log_iqr | SSL_qc2 | 9 | missing | 1,935,574 | 63.16% | 3,064,671 |
| master | Q | physical_plausibility | Q_qc1 | 9 | missing | 1,934,750 | 63.13% | 3,064,671 |
| master | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 1,932,327 | 63.05% | 3,064,671 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 1,777,038 | 57.98% | 3,064,671 |
| master | SSC | log_iqr | SSC_qc2 | 9 | missing | 1,773,845 | 57.88% | 3,064,671 |
| master | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 1,771,168 | 57.79% | 3,064,671 |
| master | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 1,293,503 | 42.21% | 3,064,671 |
| master | SSC | log_iqr | SSC_qc2 | 0 | pass | 1,210,756 | 39.51% | 3,064,671 |
| master | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 1,132,344 | 36.95% | 3,064,671 |
| master | Q | physical_plausibility | Q_qc1 | 0 | pass | 1,129,921 | 36.87% | 3,064,671 |
| master | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 1,114,197 | 36.36% | 3,064,671 |
| master | Q | log_iqr | Q_qc2 | 0 | pass | 1,087,819 | 35.50% | 3,064,671 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 1,019,501 | 33.27% | 3,064,671 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 728,029 | 23.76% | 3,064,671 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 253,993 | 8.29% | 3,064,671 |
| master | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 46,635 | 1.52% | 3,064,671 |
| master | SSC | log_iqr | SSC_qc2 | 2 | suspect | 33,435 | 1.09% | 3,064,671 |
| master | Q | log_iqr | Q_qc2 | 8 | not_checked | 25,236 | 0.82% | 3,064,671 |
| master | SSL | log_iqr | SSL_qc2 | 0 | pass | 14,825 | 0.48% | 3,064,671 |
| master | Q | log_iqr | Q_qc2 | 2 | suspect | 14,189 | 0.46% | 3,064,671 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 2 | suspect | 14,139 | 0.46% | 3,064,671 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 2 | suspect | 9,384 | 0.31% | 3,064,671 |

_Showing first 24 of 45 rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 132,614 | 16,345,662 | 16,344,453 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 132,693 | 16,345,583 | 16,344,453 | 0.81% | 0.81% | 99.19% | 99.19% |
| master | SSL | SSL_qc3 | 3,064,671 | 27 | 0 | 27 | 2,336,615 | 2,327,231 | 0.00% | 0.00% | 76.24% | 75.94% |
| master | Q | Q_qc2 | 3,064,671 | 1,087,819 | 0 | 1,087,819 | 1,951,616 | 1,937,427 | 35.50% | 35.50% | 63.68% | 63.22% |
| master | SSL | SSL_qc2 | 3,064,671 | 14,825 | 0 | 14,825 | 1,935,649 | 1,935,574 | 0.48% | 0.48% | 63.16% | 63.16% |
| master | Q | Q_qc1 | 3,064,671 | 1,129,921 | 0 | 1,129,921 | 1,934,750 | 1,934,750 | 36.87% | 36.87% | 63.13% | 63.13% |
| master | SSL | SSL_qc1 | 3,064,671 | 1,132,344 | 0 | 1,132,344 | 1,932,327 | 1,932,327 | 36.95% | 36.95% | 63.05% | 63.05% |
| master | SSC | SSC_qc2 | 3,064,671 | 1,210,756 | 0 | 1,210,756 | 1,807,280 | 1,773,845 | 39.51% | 39.51% | 58.97% | 57.88% |
| master | SSC | SSC_qc3 | 3,064,671 | 1,019,501 | 0 | 1,019,501 | 1,791,177 | 1,777,038 | 33.27% | 33.27% | 58.45% | 57.98% |
| master | SSC | SSC_qc1 | 3,064,671 | 1,293,503 | 0 | 1,293,503 | 1,771,168 | 1,771,168 | 42.21% | 42.21% | 57.79% | 57.79% |
| satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 0 | 15,121,092 | 1,357,184 | 960,798 | 91.76% | 91.76% | 8.24% | 5.83% |
| master | SSL | SSL_flag | 3,064,671 | 15,100 | 0 | 15,100 | 349,072 | 201,510 | 0.49% | 0.49% | 11.39% | 6.58% |
| master | Q | Q_flag | 3,064,671 | 2,791,428 | 0 | 2,791,428 | 273,243 | 212,010 | 91.08% | 91.08% | 8.92% | 6.92% |
| master | SSC | SSC_flag | 3,064,671 | 2,915,287 | 0 | 2,915,287 | 121,261 | 10,621 | 95.13% | 95.13% | 3.96% | 0.35% |
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
| product_variable | all_release_sources | master | SSL | SSL_qc3 | 3,064,671 | 27 | 2,336,615 | 5,391,875 | 0.00% | 76.24% | 175.94% |
| product_variable | all_release_sources | master | SSL | SSL_qc2 | 3,064,671 | 14,825 | 1,935,649 | 4,985,420 | 0.48% | 63.16% | 162.67% |
| product_variable | all_release_sources | master | Q | Q_qc2 | 3,064,671 | 1,087,819 | 1,951,616 | 3,914,279 | 35.50% | 63.68% | 127.72% |
| product_variable | all_release_sources | master | Q | Q_qc1 | 3,064,671 | 1,129,921 | 1,934,750 | 3,869,500 | 36.87% | 63.13% | 126.26% |
| product_variable | all_release_sources | master | SSL | SSL_qc1 | 3,064,671 | 1,132,344 | 1,932,327 | 3,864,654 | 36.95% | 63.05% | 126.10% |
| product_variable | all_release_sources | master | SSC | SSC_qc3 | 3,064,671 | 1,019,501 | 1,791,177 | 3,822,208 | 33.27% | 58.45% | 124.72% |
| product_variable | all_release_sources | master | SSC | SSC_qc2 | 3,064,671 | 1,210,756 | 1,807,280 | 3,627,760 | 39.51% | 58.97% | 118.37% |
| product_variable | all_release_sources | master | SSC | SSC_qc1 | 3,064,671 | 1,293,503 | 1,771,168 | 3,542,336 | 42.21% | 57.79% | 115.59% |
| product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,478,276 | 15,121,092 | 1,357,184 | 2,317,982 | 91.76% | 8.24% | 14.07% |
| product_variable | all_release_sources | master | SSL | SSL_flag | 3,064,671 | 15,100 | 349,072 | 550,582 | 0.49% | 11.39% | 17.97% |
| product_variable | all_release_sources | master | Q | Q_flag | 3,064,671 | 2,791,428 | 273,243 | 485,253 | 91.08% | 8.92% | 15.83% |
| product_variable | all_release_sources | master | SSC | SSC_flag | 3,064,671 | 2,915,287 | 121,261 | 131,882 | 95.13% | 3.96% | 4.30% |
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
| master | SSL | ssc_q_consistency | SSL_qc3 | 3,064,671 | 27 | 0 | 728,029 | 2,327,231 | 0.00% | 76.24% | 75.94% |
| master | Q | log_iqr | Q_qc2 | 3,064,671 | 1,087,819 | 0 | 25,236 | 1,937,427 | 35.50% | 63.68% | 63.22% |
| master | SSL | log_iqr | SSL_qc2 | 3,064,671 | 14,825 | 0 | 1,114,197 | 1,935,574 | 0.48% | 63.16% | 63.16% |
| master | Q | physical_plausibility | Q_qc1 | 3,064,671 | 1,129,921 | 0 | 0 | 1,934,750 | 36.87% | 63.13% | 63.13% |
| master | SSL | physical_plausibility | SSL_qc1 | 3,064,671 | 1,132,344 | 0 | 0 | 1,932,327 | 36.95% | 63.05% | 63.05% |
| master | SSC | log_iqr | SSC_qc2 | 3,064,671 | 1,210,756 | 0 | 46,635 | 1,773,845 | 39.51% | 58.97% | 57.88% |
| master | SSC | ssc_q_consistency | SSC_qc3 | 3,064,671 | 1,019,501 | 0 | 253,993 | 1,777,038 | 33.27% | 58.45% | 57.98% |
| master | SSC | physical_plausibility | SSC_qc1 | 3,064,671 | 1,293,503 | 0 | 0 | 1,771,168 | 42.21% | 57.79% | 57.79% |
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
| all_release_sources | all | final | final | master | Q | Q_flag | 0 | good | 2,791,428 | 91.08% | 3,064,671 |
| all_release_sources | all | final | final | master | Q | Q_flag | 2 | suspect | 60,678 | 1.98% | 3,064,671 |
| all_release_sources | all | final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 3,064,671 |
| all_release_sources | all | final | final | master | Q | Q_flag | 9 | missing | 212,010 | 6.92% | 3,064,671 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 1,129,921 | 36.87% | 3,064,671 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 1,934,750 | 63.13% | 3,064,671 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 1,087,819 | 35.50% | 3,064,671 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 14,189 | 0.46% | 3,064,671 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 25,236 | 0.82% | 3,064,671 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 1,937,427 | 63.22% | 3,064,671 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 0 | good | 2,915,287 | 95.13% | 3,064,671 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,064,671 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 2 | suspect | 110,640 | 3.61% | 3,064,671 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,064,671 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 1,293,503 | 42.21% | 3,064,671 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 1,771,168 | 57.79% | 3,064,671 |

_Showing first 16 of 74 rows._

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | master | Q | Q_flag | 0 | good | 2,791,428 | 91.08% | 3,064,671 |
| final | final | master | Q | Q_flag | 2 | suspect | 60,678 | 1.98% | 3,064,671 |
| final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 3,064,671 |
| final | final | master | Q | Q_flag | 9 | missing | 212,010 | 6.92% | 3,064,671 |
| stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 1,129,921 | 36.87% | 3,064,671 |
| stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 1,934,750 | 63.13% | 3,064,671 |
| stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 1,087,819 | 35.50% | 3,064,671 |
| stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 14,189 | 0.46% | 3,064,671 |
| stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 25,236 | 0.82% | 3,064,671 |
| stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 1,937,427 | 63.22% | 3,064,671 |
| final | final | master | SSC | SSC_flag | 0 | good | 2,915,287 | 95.13% | 3,064,671 |
| final | final | master | SSC | SSC_flag | 1 | derived | 28,123 | 0.92% | 3,064,671 |
| final | final | master | SSC | SSC_flag | 2 | suspect | 110,640 | 3.61% | 3,064,671 |
| final | final | master | SSC | SSC_flag | 9 | missing | 10,621 | 0.35% | 3,064,671 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 1,293,503 | 42.21% | 3,064,671 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 1,771,168 | 57.79% | 3,064,671 |

_Showing first 16 of 74 rows._

## Flag Counts by Variable

| qc level | qc stage | variable | flag variable | flag | meaning | count | n total | percentage |
|---|---|---|---|---|---|---|---|---|
| final | final | Q | Q_flag | 0 | good | 2,924,824 | 19,544,308 | 14.97% |
| final | final | Q | Q_flag | 2 | suspect | 61,887 | 19,542,947 | 0.32% |
| final | final | Q | Q_flag | 3 | bad | 555 | 3,064,671 | 0.02% |
| final | final | Q | Q_flag | 9 | missing | 16,557,042 | 19,544,308 | 84.72% |
| final | final | SSC | SSC_flag | 0 | good | 18,037,138 | 19,544,308 | 92.29% |
| final | final | SSC | SSC_flag | 1 | derived | 28,170 | 3,066,032 | 0.92% |
| final | final | SSC | SSC_flag | 2 | suspect | 507,026 | 19,542,947 | 2.59% |
| final | final | SSC | SSC_flag | 9 | missing | 971,974 | 19,544,308 | 4.97% |
| final | final | SSL | SSL_flag | 0 | good | 149,130 | 19,544,308 | 0.76% |
| final | final | SSL | SSL_flag | 1 | derived | 2,700,499 | 3,064,671 | 88.12% |
| final | final | SSL | SSL_flag | 2 | suspect | 148,137 | 19,542,947 | 0.76% |
| final | final | SSL | SSL_flag | 3 | bad | 555 | 3,064,671 | 0.02% |
| final | final | SSL | SSL_flag | 9 | missing | 16,545,987 | 19,544,308 | 84.66% |
| stage | log_iqr | Q | Q_qc2 | 0 | pass | 1,087,819 | 3,064,671 | 35.50% |
| stage | log_iqr | Q | Q_qc2 | 2 | suspect | 14,189 | 3,064,671 | 0.46% |
| stage | log_iqr | Q | Q_qc2 | 8 | not_checked | 25,769 | 3,066,032 | 0.84% |

_Showing first 16 of 40 rows._

## Problem Stations

| station uid | station reference id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | satellite | Q | Q_flag | 16,478,276 | 132,614 | 0 | 1,209 | 0 | 0 | 16,344,453 | 132,614 | 16,345,662 | 32,690,115 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.38% |
|  |  | product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,478,276 | 132,693 | 0 | 1,130 | 0 | 0 | 16,344,453 | 132,693 | 16,345,583 | 32,690,036 | 0.81% | 0% | 0.01% | 0% | 0% | 99.19% | 0.81% | 99.19% | 198.38% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc3 | 3,064,671 | 27 | 0 | 9,384 | 0 | 728,029 | 2,327,231 | 27 | 2,336,615 | 5,391,875 | 0.00% | 0% | 0.31% | 0% | 23.76% | 75.94% | 0.00% | 76.24% | 175.94% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc2 | 3,064,671 | 14,825 | 0 | 75 | 0 | 1,114,197 | 1,935,574 | 14,825 | 1,935,649 | 4,985,420 | 0.48% | 0% | 0.00% | 0% | 36.36% | 63.16% | 0.48% | 63.16% | 162.67% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,361 | 0 | 0 | 0 | 0 | 533 | 828 | 0 | 828 | 2,189 | 0% | 0% | 0% | 0% | 39.16% | 60.84% | 0% | 60.84% | 160.84% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,361 | 24 | 0 | 0 | 0 | 533 | 804 | 24 | 804 | 2,141 | 1.76% | 0% | 0% | 0% | 39.16% | 59.07% | 1.76% | 59.07% | 157.31% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,361 | 24 | 0 | 0 | 0 | 533 | 804 | 24 | 804 | 2,141 | 1.76% | 0% | 0% | 0% | 39.16% | 59.07% | 1.76% | 59.07% | 157.31% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc2 | 3,064,671 | 1,087,819 | 0 | 14,189 | 0 | 25,236 | 1,937,427 | 1,087,819 | 1,951,616 | 3,914,279 | 35.50% | 0% | 0.46% | 0% | 0.82% | 63.22% | 35.50% | 63.68% | 127.72% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc1 | 3,064,671 | 1,129,921 | 0 | 0 | 0 | 0 | 1,934,750 | 1,129,921 | 1,934,750 | 3,869,500 | 36.87% | 0% | 0% | 0% | 0% | 63.13% | 36.87% | 63.13% | 126.26% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc1 | 3,064,671 | 1,132,344 | 0 | 0 | 0 | 0 | 1,932,327 | 1,132,344 | 1,932,327 | 3,864,654 | 36.95% | 0% | 0% | 0% | 0% | 63.05% | 36.95% | 63.05% | 126.10% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc3 | 3,064,671 | 1,019,501 | 0 | 14,139 | 0 | 253,993 | 1,777,038 | 1,019,501 | 1,791,177 | 3,822,208 | 33.27% | 0% | 0.46% | 0% | 8.29% | 57.98% | 33.27% | 58.45% | 124.72% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc2 | 3,064,671 | 1,210,756 | 0 | 33,435 | 0 | 46,635 | 1,773,845 | 1,210,756 | 1,807,280 | 3,627,760 | 39.51% | 0% | 1.09% | 0% | 1.52% | 57.88% | 39.51% | 58.97% | 118.37% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc1 | 3,064,671 | 1,293,503 | 0 | 0 | 0 | 0 | 1,771,168 | 1,293,503 | 1,771,168 | 3,542,336 | 42.21% | 0% | 0% | 0% | 0% | 57.79% | 42.21% | 57.79% | 115.59% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc1 | 1,361 | 782 | 0 | 0 | 0 | 0 | 579 | 782 | 579 | 1,158 | 57.46% | 0% | 0% | 0% | 0% | 42.54% | 57.46% | 42.54% | 85.08% |

_Showing first 16 of 25 rows._

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 4,083 | 8,166 | 50% |
| all | climatology | SSC | 5,444 | 16,332 | 33.33% |
| all | climatology | SSL | 5,444 | 10,888 | 50% |
| all | master | Q | 9,194,013 | 30,646,710 | 30% |
| all | master | SSC | 12,258,684 | 42,905,394 | 28.57% |
| all | master | SSL | 12,258,684 | 45,970,065 | 26.67% |
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
- `fig_qc_top_problem_reference_stations.png`: `output_other/stats_release/qc_flags/figures/fig_qc_top_problem_reference_stations.png`
- `fig_qc_top_problem_sources.png`: `output_other/stats_release/qc_flags/figures/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `output_other/stats_release/qc_flags/figures/fig_qc_yearly_problem_trends.png`
