# Release QC Flag Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/qc_flags/tables`
- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.

## Headline

- Flag observations summarized: 80,856,942
- Final flag rows: 28
- Stage flag rows: 44
- Usable flag count from health KPIs: 12,975,937
- Problem flag count from health KPIs: 67,239,041
- Stage-effectiveness rows available: 16

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

_Showing first 24 of 103 rows._

## Final Flag Summary

| temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |
| satellite | SSL | SSL_flag | 9 | missing | 16,373,082 | 99.19% | 16,506,461 |
| satellite | SSC | SSC_flag | 9 | missing | 15,160,652 | 91.85% | 16,506,461 |
| master | Q | Q_flag | 0 | good | 2,778,769 | 97.58% | 2,847,547 |
| master | SSC | SSC_flag | 0 | good | 2,709,299 | 95.15% | 2,847,547 |
| master | SSL | SSL_flag | 1 | estimated | 2,648,674 | 93.02% | 2,847,547 |
| satellite | SSC | SSC_flag | 0 | good | 1,326,713 | 8.04% | 16,506,461 |
| satellite | SSL | SSL_flag | 0 | good | 132,259 | 0.80% | 16,506,461 |
| satellite | Q | Q_flag | 0 | good | 132,172 | 0.80% | 16,506,461 |
| master | SSL | SSL_flag | 2 | suspect | 115,179 | 4.04% | 2,847,547 |
| master | SSC | SSC_flag | 2 | suspect | 99,577 | 3.50% | 2,847,547 |
| master | SSL | SSL_flag | 0 | good | 73,942 | 2.60% | 2,847,547 |
| master | Q | Q_flag | 2 | suspect | 51,721 | 1.82% | 2,847,547 |
| master | SSC | SSC_flag | 1 | estimated | 28,108 | 0.99% | 2,847,547 |
| satellite | SSC | SSC_flag | 2 | suspect | 19,096 | 0.12% | 16,506,461 |
| master | Q | Q_flag | 9 | missing | 16,502 | 0.58% | 2,847,547 |
| master | SSC | SSC_flag | 9 | missing | 10,563 | 0.37% | 2,847,547 |
| master | SSL | SSL_flag | 9 | missing | 9,752 | 0.34% | 2,847,547 |
| climatology | SSL | SSL_flag | 0 | good | 1,298 | 98.18% | 1,322 |
| satellite | Q | Q_flag | 2 | suspect | 1,207 | 0.01% | 16,506,461 |
| satellite | SSL | SSL_flag | 2 | suspect | 1,120 | 0.01% | 16,506,461 |
| climatology | SSC | SSC_flag | 0 | good | 787 | 59.53% | 1,322 |
| climatology | Q | Q_flag | 0 | good | 782 | 59.15% | 1,322 |
| master | Q | Q_flag | 3 | bad | 555 | 0.02% | 2,847,547 |

_Showing first 24 of 28 rows._

## Stage Flag Summary

| temporal resolution | variable | qc stage | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|
| master | SSL | ssc_q_consistency | SSL_qc3 | 9 | missing | 2,700,249 | 94.83% | 2,847,547 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 9 | missing | 2,329,730 | 81.82% | 2,847,547 |
| master | Q | log_iqr | Q_qc2 | 9 | missing | 2,326,724 | 81.71% | 2,847,547 |
| master | SSC | log_iqr | SSC_qc2 | 9 | missing | 2,326,691 | 81.71% | 2,847,547 |
| master | Q | physical_plausibility | Q_qc1 | 9 | missing | 2,324,090 | 81.62% | 2,847,547 |
| master | SSC | physical_plausibility | SSC_qc1 | 9 | missing | 2,324,014 | 81.61% | 2,847,547 |
| master | SSL | log_iqr | SSL_qc2 | 9 | missing | 2,320,032 | 81.47% | 2,847,547 |
| master | SSL | physical_plausibility | SSL_qc1 | 9 | missing | 2,316,785 | 81.36% | 2,847,547 |
| master | SSL | physical_plausibility | SSL_qc1 | 0 | pass | 530,762 | 18.64% | 2,847,547 |
| master | SSC | physical_plausibility | SSC_qc1 | 0 | pass | 523,533 | 18.39% | 2,847,547 |
| master | Q | physical_plausibility | Q_qc1 | 0 | pass | 523,457 | 18.38% | 2,847,547 |
| master | SSC | log_iqr | SSC_qc2 | 0 | pass | 501,352 | 17.61% | 2,847,547 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 0 | pass | 483,893 | 16.99% | 2,847,547 |
| master | SSL | log_iqr | SSL_qc2 | 8 | not_checked | 452,602 | 15.89% | 2,847,547 |
| master | Q | log_iqr | Q_qc2 | 0 | pass | 443,340 | 15.57% | 2,847,547 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 8 | not_checked | 85,307 | 3.00% | 2,847,547 |
| master | SSL | log_iqr | SSL_qc2 | 0 | pass | 73,776 | 2.59% | 2,847,547 |
| master | Q | log_iqr | Q_qc2 | 8 | not_checked | 69,514 | 2.44% | 2,847,547 |
| master | SSL | ssc_q_consistency | SSL_qc3 | 0 | not_propagated | 60,072 | 2.11% | 2,847,547 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 8 | not_checked | 27,209 | 0.96% | 2,847,547 |
| master | SSC | log_iqr | SSC_qc2 | 2 | suspect | 14,837 | 0.52% | 2,847,547 |
| master | Q | log_iqr | Q_qc2 | 2 | suspect | 7,969 | 0.28% | 2,847,547 |
| master | SSC | ssc_q_consistency | SSC_qc3 | 2 | suspect | 6,715 | 0.24% | 2,847,547 |
| master | SSC | log_iqr | SSC_qc2 | 8 | not_checked | 4,667 | 0.16% | 2,847,547 |

_Showing first 24 of 44 rows._

## Health KPIs

Usable combines good and estimated/derived values when represented by release flags.

| temporal resolution | variable | flag variable | n total | good count | derived count | usable count | problem count | missing count | good rate | usable rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| satellite | Q | Q_flag | 16,506,461 | 132,172 | 0 | 132,172 | 16,374,289 | 16,373,082 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSL | SSL_flag | 16,506,461 | 132,259 | 0 | 132,259 | 16,374,202 | 16,373,082 | 0.80% | 0.80% | 99.20% | 99.19% |
| satellite | SSC | SSC_flag | 16,506,461 | 1,326,713 | 0 | 1,326,713 | 15,179,748 | 15,160,652 | 8.04% | 8.04% | 91.96% | 91.85% |
| master | SSL | SSL_qc3 | 2,847,547 | 60,072 | 0 | 60,072 | 2,702,168 | 2,700,249 | 2.11% | 2.11% | 94.89% | 94.83% |
| master | SSC | SSC_qc2 | 2,847,547 | 501,352 | 0 | 501,352 | 2,341,528 | 2,326,691 | 17.61% | 17.61% | 82.23% | 81.71% |
| master | SSC | SSC_qc3 | 2,847,547 | 483,893 | 0 | 483,893 | 2,336,445 | 2,329,730 | 16.99% | 16.99% | 82.05% | 81.82% |
| master | Q | Q_qc2 | 2,847,547 | 443,340 | 0 | 443,340 | 2,334,693 | 2,326,724 | 15.57% | 15.57% | 81.99% | 81.71% |
| master | Q | Q_qc1 | 2,847,547 | 523,457 | 0 | 523,457 | 2,324,090 | 2,324,090 | 18.38% | 18.38% | 81.62% | 81.62% |
| master | SSC | SSC_qc1 | 2,847,547 | 523,533 | 0 | 523,533 | 2,324,014 | 2,324,014 | 18.39% | 18.39% | 81.61% | 81.61% |
| master | SSL | SSL_qc2 | 2,847,547 | 73,776 | 0 | 73,776 | 2,321,169 | 2,320,032 | 2.59% | 2.59% | 81.51% | 81.47% |
| master | SSL | SSL_qc1 | 2,847,547 | 530,762 | 0 | 530,762 | 2,316,785 | 2,316,785 | 18.64% | 18.64% | 81.36% | 81.36% |
| master | SSL | SSL_flag | 2,847,547 | 73,942 | 2,648,674 | 2,722,616 | 124,931 | 9,752 | 2.60% | 95.61% | 4.39% | 0.34% |
| master | SSC | SSC_flag | 2,847,547 | 2,709,299 | 28,108 | 2,737,407 | 110,140 | 10,563 | 95.15% | 96.13% | 3.87% | 0.37% |
| master | Q | Q_flag | 2,847,547 | 2,778,769 | 0 | 2,778,769 | 68,778 | 16,502 | 97.58% | 97.58% | 2.42% | 0.58% |
| climatology | Q | Q_qc2 | 1,322 | 0 | 0 | 0 | 789 | 789 | 0% | 0% | 59.68% | 59.68% |
| climatology | SSL | SSL_qc3 | 1,322 | 0 | 0 | 0 | 789 | 789 | 0% | 0% | 59.68% | 59.68% |
| climatology | SSL | SSL_qc2 | 1,322 | 0 | 0 | 0 | 789 | 789 | 0% | 0% | 59.68% | 59.68% |
| climatology | SSC | SSC_qc2 | 1,322 | 24 | 0 | 24 | 765 | 765 | 1.82% | 1.82% | 57.87% | 57.87% |
| climatology | SSC | SSC_qc3 | 1,322 | 24 | 0 | 24 | 765 | 765 | 1.82% | 1.82% | 57.87% | 57.87% |
| climatology | Q | Q_qc1 | 1,322 | 782 | 0 | 782 | 540 | 540 | 59.15% | 59.15% | 40.85% | 40.85% |
| climatology | Q | Q_flag | 1,322 | 782 | 0 | 782 | 540 | 540 | 59.15% | 59.15% | 40.85% | 40.85% |
| climatology | SSC | SSC_flag | 1,322 | 787 | 17 | 804 | 518 | 518 | 59.53% | 60.82% | 39.18% | 39.18% |
| climatology | SSC | SSC_qc1 | 1,322 | 804 | 0 | 804 | 518 | 518 | 60.82% | 60.82% | 39.18% | 39.18% |
| climatology | SSL | SSL_qc1 | 1,322 | 1,298 | 0 | 1,298 | 24 | 24 | 98.18% | 98.18% | 1.82% | 1.82% |

_Showing first 24 of 25 rows._

## Issue Hotspots

| grouping level | source dataset | temporal resolution | variable | flag variable | n total | usable count | problem count | issue count | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| product_variable | all_release_sources | satellite | Q | Q_flag | 16,506,461 | 132,172 | 16,374,289 | 32,747,371 | 0.80% | 99.20% | 198.39% |
| product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,506,461 | 132,259 | 16,374,202 | 32,747,284 | 0.80% | 99.20% | 198.39% |
| product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,506,461 | 1,326,713 | 15,179,748 | 30,340,400 | 8.04% | 91.96% | 183.81% |
| product_variable | all_release_sources | master | SSL | SSL_qc3 | 2,847,547 | 60,072 | 2,702,168 | 5,487,724 | 2.11% | 94.89% | 192.72% |
| product_variable | all_release_sources | master | SSL | SSL_qc2 | 2,847,547 | 73,776 | 2,321,169 | 5,093,803 | 2.59% | 81.51% | 178.88% |
| product_variable | all_release_sources | master | Q | Q_qc2 | 2,847,547 | 443,340 | 2,334,693 | 4,730,931 | 15.57% | 81.99% | 166.14% |
| product_variable | all_release_sources | master | SSC | SSC_qc3 | 2,847,547 | 483,893 | 2,336,445 | 4,693,384 | 16.99% | 82.05% | 164.82% |
| product_variable | all_release_sources | master | SSC | SSC_qc2 | 2,847,547 | 501,352 | 2,341,528 | 4,672,886 | 17.61% | 82.23% | 164.10% |
| product_variable | all_release_sources | master | Q | Q_qc1 | 2,847,547 | 523,457 | 2,324,090 | 4,648,180 | 18.38% | 81.62% | 163.23% |
| product_variable | all_release_sources | master | SSC | SSC_qc1 | 2,847,547 | 523,533 | 2,324,014 | 4,648,028 | 18.39% | 81.61% | 163.23% |
| product_variable | all_release_sources | master | SSL | SSL_qc1 | 2,847,547 | 530,762 | 2,316,785 | 4,633,570 | 18.64% | 81.36% | 162.72% |
| product_variable | all_release_sources | master | SSL | SSL_flag | 2,847,547 | 2,722,616 | 124,931 | 134,683 | 95.61% | 4.39% | 4.73% |
| product_variable | all_release_sources | master | SSC | SSC_flag | 2,847,547 | 2,737,407 | 110,140 | 120,703 | 96.13% | 3.87% | 4.24% |
| product_variable | all_release_sources | master | Q | Q_flag | 2,847,547 | 2,778,769 | 68,778 | 85,280 | 97.58% | 2.42% | 2.99% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,322 | 0 | 789 | 2,111 | 0% | 59.68% | 159.68% |
| product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,322 | 0 | 789 | 2,111 | 0% | 59.68% | 159.68% |
| product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,322 | 0 | 789 | 2,111 | 0% | 59.68% | 159.68% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,322 | 24 | 765 | 2,063 | 1.82% | 57.87% | 156.05% |
| product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,322 | 24 | 765 | 2,063 | 1.82% | 57.87% | 156.05% |
| product_variable | all_release_sources | climatology | Q | Q_flag | 1,322 | 782 | 540 | 1,080 | 59.15% | 40.85% | 81.69% |

_Showing first 20 of 25 rows._

## Stage Effectiveness

| temporal resolution | variable | qc stage | flag variable | n total | good count | bad count | not checked count | missing count | good rate | problem rate | missing rate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| master | SSL | ssc_q_consistency | SSL_qc3 | 2,847,547 | 60,072 | 0 | 85,307 | 2,700,249 | 2.11% | 94.89% | 94.83% |
| master | SSC | log_iqr | SSC_qc2 | 2,847,547 | 501,352 | 0 | 4,667 | 2,326,691 | 17.61% | 82.23% | 81.71% |
| master | SSC | ssc_q_consistency | SSC_qc3 | 2,847,547 | 483,893 | 0 | 27,209 | 2,329,730 | 16.99% | 82.05% | 81.82% |
| master | Q | log_iqr | Q_qc2 | 2,847,547 | 443,340 | 0 | 69,514 | 2,326,724 | 15.57% | 81.99% | 81.71% |
| master | Q | physical_plausibility | Q_qc1 | 2,847,547 | 523,457 | 0 | 0 | 2,324,090 | 18.38% | 81.62% | 81.62% |
| master | SSC | physical_plausibility | SSC_qc1 | 2,847,547 | 523,533 | 0 | 0 | 2,324,014 | 18.39% | 81.61% | 81.61% |
| master | SSL | log_iqr | SSL_qc2 | 2,847,547 | 73,776 | 0 | 452,602 | 2,320,032 | 2.59% | 81.51% | 81.47% |
| master | SSL | physical_plausibility | SSL_qc1 | 2,847,547 | 530,762 | 0 | 0 | 2,316,785 | 18.64% | 81.36% | 81.36% |
| climatology | SSL | ssc_q_consistency | SSL_qc3 | 1,322 | 0 | 0 | 533 | 789 | 0% | 59.68% | 59.68% |
| climatology | SSL | log_iqr | SSL_qc2 | 1,322 | 0 | 0 | 533 | 789 | 0% | 59.68% | 59.68% |
| climatology | Q | log_iqr | Q_qc2 | 1,322 | 0 | 0 | 533 | 789 | 0% | 59.68% | 59.68% |
| climatology | SSC | log_iqr | SSC_qc2 | 1,322 | 24 | 0 | 533 | 765 | 1.82% | 57.87% | 57.87% |
| climatology | SSC | ssc_q_consistency | SSC_qc3 | 1,322 | 24 | 0 | 533 | 765 | 1.82% | 57.87% | 57.87% |
| climatology | Q | physical_plausibility | Q_qc1 | 1,322 | 782 | 0 | 0 | 540 | 59.15% | 40.85% | 40.85% |
| climatology | SSC | physical_plausibility | SSC_qc1 | 1,322 | 804 | 0 | 0 | 518 | 60.82% | 39.18% | 39.18% |
| climatology | SSL | physical_plausibility | SSL_qc1 | 1,322 | 1,298 | 0 | 0 | 24 | 98.18% | 1.82% | 1.82% |

## Flag Counts by Source

| source dataset | source type | qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|---|---|
| all_release_sources | all | final | final | master | Q | Q_flag | 0 | good | 2,778,769 | 97.58% | 2,847,547 |
| all_release_sources | all | final | final | master | Q | Q_flag | 2 | suspect | 51,721 | 1.82% | 2,847,547 |
| all_release_sources | all | final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 2,847,547 |
| all_release_sources | all | final | final | master | Q | Q_flag | 9 | missing | 16,502 | 0.58% | 2,847,547 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 523,457 | 18.38% | 2,847,547 |
| all_release_sources | all | stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 2,324,090 | 81.62% | 2,847,547 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 443,340 | 15.57% | 2,847,547 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 7,969 | 0.28% | 2,847,547 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 69,514 | 2.44% | 2,847,547 |
| all_release_sources | all | stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 2,326,724 | 81.71% | 2,847,547 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 0 | good | 2,709,299 | 95.15% | 2,847,547 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 1 | estimated | 28,108 | 0.99% | 2,847,547 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 2 | suspect | 99,577 | 3.50% | 2,847,547 |
| all_release_sources | all | final | final | master | SSC | SSC_flag | 9 | missing | 10,563 | 0.37% | 2,847,547 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 523,533 | 18.39% | 2,847,547 |
| all_release_sources | all | stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 2,324,014 | 81.61% | 2,847,547 |

_Showing first 16 of 72 rows._

## Flag Counts by Resolution

| qc level | qc stage | temporal resolution | variable | flag variable | flag | meaning | count | percentage | n total |
|---|---|---|---|---|---|---|---|---|---|
| final | final | master | Q | Q_flag | 0 | good | 2,778,769 | 97.58% | 2,847,547 |
| final | final | master | Q | Q_flag | 2 | suspect | 51,721 | 1.82% | 2,847,547 |
| final | final | master | Q | Q_flag | 3 | bad | 555 | 0.02% | 2,847,547 |
| final | final | master | Q | Q_flag | 9 | missing | 16,502 | 0.58% | 2,847,547 |
| stage | physical_plausibility | master | Q | Q_qc1 | 0 | pass | 523,457 | 18.38% | 2,847,547 |
| stage | physical_plausibility | master | Q | Q_qc1 | 9 | missing | 2,324,090 | 81.62% | 2,847,547 |
| stage | log_iqr | master | Q | Q_qc2 | 0 | pass | 443,340 | 15.57% | 2,847,547 |
| stage | log_iqr | master | Q | Q_qc2 | 2 | suspect | 7,969 | 0.28% | 2,847,547 |
| stage | log_iqr | master | Q | Q_qc2 | 8 | not_checked | 69,514 | 2.44% | 2,847,547 |
| stage | log_iqr | master | Q | Q_qc2 | 9 | missing | 2,326,724 | 81.71% | 2,847,547 |
| final | final | master | SSC | SSC_flag | 0 | good | 2,709,299 | 95.15% | 2,847,547 |
| final | final | master | SSC | SSC_flag | 1 | estimated | 28,108 | 0.99% | 2,847,547 |
| final | final | master | SSC | SSC_flag | 2 | suspect | 99,577 | 3.50% | 2,847,547 |
| final | final | master | SSC | SSC_flag | 9 | missing | 10,563 | 0.37% | 2,847,547 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 0 | pass | 523,533 | 18.39% | 2,847,547 |
| stage | physical_plausibility | master | SSC | SSC_qc1 | 9 | missing | 2,324,014 | 81.61% | 2,847,547 |

_Showing first 16 of 72 rows._

## Flag Counts by Variable

| qc level | qc stage | variable | flag variable | flag | meaning | count | n total | percentage |
|---|---|---|---|---|---|---|---|---|
| final | final | Q | Q_flag | 0 | good | 2,911,723 | 19,355,330 | 15.04% |
| final | final | Q | Q_flag | 2 | suspect | 52,928 | 19,354,008 | 0.27% |
| final | final | Q | Q_flag | 3 | bad | 555 | 2,847,547 | 0.02% |
| final | final | Q | Q_flag | 9 | missing | 16,390,124 | 19,355,330 | 84.68% |
| final | final | SSC | SSC_flag | 0 | good | 4,036,799 | 19,355,330 | 20.86% |
| final | final | SSC | SSC_flag | 1 | estimated | 28,125 | 2,848,869 | 0.99% |
| final | final | SSC | SSC_flag | 2 | suspect | 118,673 | 19,354,008 | 0.61% |
| final | final | SSC | SSC_flag | 9 | missing | 15,171,733 | 19,355,330 | 78.39% |
| final | final | SSL | SSL_flag | 0 | good | 207,499 | 19,355,330 | 1.07% |
| final | final | SSL | SSL_flag | 1 | estimated | 2,648,674 | 2,847,547 | 93.02% |
| final | final | SSL | SSL_flag | 2 | suspect | 116,299 | 19,354,008 | 0.60% |
| final | final | SSL | SSL_flag | 9 | missing | 16,382,858 | 19,355,330 | 84.64% |
| stage | log_iqr | Q | Q_qc2 | 0 | pass | 443,340 | 2,847,547 | 15.57% |
| stage | log_iqr | Q | Q_qc2 | 2 | suspect | 7,969 | 2,847,547 | 0.28% |
| stage | log_iqr | Q | Q_qc2 | 8 | not_checked | 70,047 | 2,848,869 | 2.46% |
| stage | log_iqr | Q | Q_qc2 | 9 | missing | 2,327,513 | 2,848,869 | 81.70% |

_Showing first 16 of 38 rows._

## Problem Clusters

| cluster uid | cluster id | grouping level | source dataset | temporal resolution | variable | flag variable | n total | good count | derived count | suspect count | bad count | not checked count | missing count | usable count | problem count | issue count | good rate | derived rate | suspect rate | bad rate | not checked rate | missing rate | usable rate | problem rate | issue rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | product_variable | all_release_sources | satellite | Q | Q_flag | 16,506,461 | 132,172 | 0 | 1,207 | 0 | 0 | 16,373,082 | 132,172 | 16,374,289 | 32,747,371 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.39% |
|  |  | product_variable | all_release_sources | satellite | SSL | SSL_flag | 16,506,461 | 132,259 | 0 | 1,120 | 0 | 0 | 16,373,082 | 132,259 | 16,374,202 | 32,747,284 | 0.80% | 0% | 0.01% | 0% | 0% | 99.19% | 0.80% | 99.20% | 198.39% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc3 | 2,847,547 | 60,072 | 0 | 1,919 | 0 | 85,307 | 2,700,249 | 60,072 | 2,702,168 | 5,487,724 | 2.11% | 0% | 0.07% | 0% | 3.00% | 94.83% | 2.11% | 94.89% | 192.72% |
|  |  | product_variable | all_release_sources | satellite | SSC | SSC_flag | 16,506,461 | 1,326,713 | 0 | 19,096 | 0 | 0 | 15,160,652 | 1,326,713 | 15,179,748 | 30,340,400 | 8.04% | 0% | 0.12% | 0% | 0% | 91.85% | 8.04% | 91.96% | 183.81% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc2 | 2,847,547 | 73,776 | 0 | 1,137 | 0 | 452,602 | 2,320,032 | 73,776 | 2,321,169 | 5,093,803 | 2.59% | 0% | 0.04% | 0% | 15.89% | 81.47% | 2.59% | 81.51% | 178.88% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc2 | 2,847,547 | 443,340 | 0 | 7,969 | 0 | 69,514 | 2,326,724 | 443,340 | 2,334,693 | 4,730,931 | 15.57% | 0% | 0.28% | 0% | 2.44% | 81.71% | 15.57% | 81.99% | 166.14% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc3 | 2,847,547 | 483,893 | 0 | 6,715 | 0 | 27,209 | 2,329,730 | 483,893 | 2,336,445 | 4,693,384 | 16.99% | 0% | 0.24% | 0% | 0.96% | 81.82% | 16.99% | 82.05% | 164.82% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc2 | 2,847,547 | 501,352 | 0 | 14,837 | 0 | 4,667 | 2,326,691 | 501,352 | 2,341,528 | 4,672,886 | 17.61% | 0% | 0.52% | 0% | 0.16% | 81.71% | 17.61% | 82.23% | 164.10% |
|  |  | product_variable | all_release_sources | master | Q | Q_qc1 | 2,847,547 | 523,457 | 0 | 0 | 0 | 0 | 2,324,090 | 523,457 | 2,324,090 | 4,648,180 | 18.38% | 0% | 0% | 0% | 0% | 81.62% | 18.38% | 81.62% | 163.23% |
|  |  | product_variable | all_release_sources | master | SSC | SSC_qc1 | 2,847,547 | 523,533 | 0 | 0 | 0 | 0 | 2,324,014 | 523,533 | 2,324,014 | 4,648,028 | 18.39% | 0% | 0% | 0% | 0% | 81.61% | 18.39% | 81.61% | 163.23% |
|  |  | product_variable | all_release_sources | master | SSL | SSL_qc1 | 2,847,547 | 530,762 | 0 | 0 | 0 | 0 | 2,316,785 | 530,762 | 2,316,785 | 4,633,570 | 18.64% | 0% | 0% | 0% | 0% | 81.36% | 18.64% | 81.36% | 162.72% |
|  |  | product_variable | all_release_sources | climatology | Q | Q_qc2 | 1,322 | 0 | 0 | 0 | 0 | 533 | 789 | 0 | 789 | 2,111 | 0% | 0% | 0% | 0% | 40.32% | 59.68% | 0% | 59.68% | 159.68% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc2 | 1,322 | 0 | 0 | 0 | 0 | 533 | 789 | 0 | 789 | 2,111 | 0% | 0% | 0% | 0% | 40.32% | 59.68% | 0% | 59.68% | 159.68% |
|  |  | product_variable | all_release_sources | climatology | SSL | SSL_qc3 | 1,322 | 0 | 0 | 0 | 0 | 533 | 789 | 0 | 789 | 2,111 | 0% | 0% | 0% | 0% | 40.32% | 59.68% | 0% | 59.68% | 159.68% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc3 | 1,322 | 24 | 0 | 0 | 0 | 533 | 765 | 24 | 765 | 2,063 | 1.82% | 0% | 0% | 0% | 40.32% | 57.87% | 1.82% | 57.87% | 156.05% |
|  |  | product_variable | all_release_sources | climatology | SSC | SSC_qc2 | 1,322 | 24 | 0 | 0 | 0 | 533 | 765 | 24 | 765 | 2,063 | 1.82% | 0% | 0% | 0% | 40.32% | 57.87% | 1.82% | 57.87% | 156.05% |

_Showing first 16 of 25 rows._

## Yearly Trends

| year | temporal resolution | variable | issue count | n total | issue rate |
|---|---|---|---|---|---|
| all | climatology | Q | 3,966 | 7,932 | 50% |
| all | climatology | SSC | 5,288 | 14,542 | 36.36% |
| all | climatology | SSL | 5,288 | 10,576 | 50% |
| all | master | Q | 8,542,641 | 28,475,470 | 30% |
| all | master | SSC | 11,390,188 | 39,865,658 | 28.57% |
| all | master | SSL | 11,390,188 | 39,865,658 | 28.57% |
| all | satellite | Q | 16,506,461 | 49,519,383 | 33.33% |
| all | satellite | SSC | 16,506,461 | 49,519,383 | 33.33% |
| all | satellite | SSL | 16,506,461 | 49,519,383 | 33.33% |

## Interpretation Notes

- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.
- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.
- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.

## Figures

- `fig_qc_flag_by_source_type.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_flag_by_source_type.png`
- `fig_qc_flag_distribution.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_flag_distribution.png`
- `fig_qc_health.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_health.png`
- `fig_qc_health_by_resolution.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_health_by_resolution.png`
- `fig_qc_missing_trends.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_missing_trends.png`
- `fig_qc_stage_summary.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_stage_summary.png`
- `fig_qc_top_problem_clusters.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_top_problem_clusters.png`
- `fig_qc_top_problem_sources.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_top_problem_sources.png`
- `fig_qc_yearly_problem_trends.png`: `$WORKSPACE/output_other/stats_release_full/qc_flags/figures/fig_qc_yearly_problem_trends.png`
