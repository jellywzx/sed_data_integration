# Variable Coverage Results Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/variable_summary/tables`
- Variables covered: Q, SSC, SSL.

## Headline

- Product groups summarized: 3
- Product-variable denominator rows: 58,430,274
- Satellite source-variable rows with less than 1% present values: 4
- Extreme review points emitted: 180

## Product by Variable Coverage

| product | variable | n records | n present | n good | n estimated | n usable | present percent | good percent | estimated percent | usable percent |
|---|---|---|---|---|---|---|---|---|---|---|
| master | Q | 2,997,121 | 2,784,958 | 2,725,488 | 0 | 2,725,488 | 92.92% | 90.94% | 0% | 90.94% |
| master | SSC | 2,997,121 | 2,986,653 | 2,874,143 | 3,502 | 2,877,645 | 99.65% | 95.90% | 0.12% | 96.01% |
| master | SSL | 2,997,121 | 2,795,415 | 14,948 | 2,635,837 | 2,650,785 | 93.27% | 0.50% | 87.95% | 88.44% |
| climatology | Q | 1,361 | 782 | 782 | 0 | 782 | 57.46% | 57.46% | 0% | 57.46% |
| climatology | SSC | 1,361 | 806 | 759 | 47 | 806 | 59.22% | 55.77% | 3.45% | 59.22% |
| climatology | SSL | 1,361 | 1,337 | 1,337 | 0 | 1,337 | 98.24% | 98.24% | 0% | 98.24% |
| satellite | Q | 16,478,276 | 133,823 | 132,614 | 0 | 132,614 | 0.81% | 0.80% | 0% | 0.80% |
| satellite | SSC | 16,478,276 | 15,517,478 | 15,121,092 | 0 | 15,121,092 | 94.17% | 91.76% | 0% | 91.76% |
| satellite | SSL | 16,478,276 | 133,823 | 132,693 | 0 | 132,693 | 0.81% | 0.81% | 0% | 0.81% |

## Matrix Coverage by Resolution

Includes all finite (non-NaN) values regardless of quality flag (flags 0–8). Does not filter to flags 0–3.

| resolution | n records total | n clusters total | Q records | Q record coverage pct | SSC records | SSC record coverage pct | SSL records | SSL record coverage pct |
|---|---|---|---|---|---|---|---|---|
| daily | 2,993,390 | 7,087 | 2,781,406 | 92.92% | 2,982,992 | 99.65% | 2,791,804 | 93.27% |
| monthly | 3,263 | 17 | 3,204 | 98.19% | 3,193 | 97.85% | 3,263 | 100% |
| annual | 468 | 31 | 348 | 74.36% | 468 | 100% | 348 | 74.36% |

## Variable Summary Statistics

Statistics computed on all finite (non-NaN) values regardless of quality flag (flags 0–8).

| resolution | variable | n nonmissing records | n nonmissing clusters | mean | median | min | max | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | SSC | 2,982,992 | 7,087 | 429.37 | 22 | 0 | 4,300,000 | 1 | 669 | 6,530 | mg L-1 |
| daily | SSL | 2,791,804 | 3,427 | 6,490 | 7.81 | 0 | 46,974,252 | 0 | 9,400 | 136,000 | t d-1 |
| daily | Q | 2,781,406 | 3,427 | 314.54 | 3.28 | 0 | 4,972,518 | 0.00 | 586.16 | 6,031 | m3 s-1 |
| monthly | SSL | 3,263 | 17 | 26,356 | 1,901 | 0 | 803,520 | 6.68 | 146,880 | 279,763 | t d-1 |
| monthly | Q | 3,204 | 17 | 7,284 | 2,470 | 0 | 106,873 | 20.73 | 31,500 | 74,899 | m3 s-1 |
| monthly | SSC | 3,193 | 17 | 43.88 | 13.77 | 0 | 17,857 | 0.77 | 162.48 | 322.82 | mg L-1 |
| annual | SSC | 468 | 31 | 2,817 | 207.94 | 0 | 141,000 | 64.34 | 10,359 | 73,623 | mg L-1 |
| annual | Q | 348 | 7 | 222.97 | 157.81 | 1.27 | 1,144 | 4.86 | 631.50 | 891.03 | m3 s-1 |
| annual | SSL | 348 | 7 | 3,755 | 1,766 | 0 | 55,688 | 119.10 | 10,987 | 17,149 | t d-1 |

## Flag 0–1 Summary Statistics (master product)

Statistics computed only on values where the variable flag is 0 (good) or 1 (estimated/derived).

| resolution | variable | n flag01 records | mean | median | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|
| daily | SSC | 2,874,034 | 383.98 | 21 | 1 | 552 | 5,630 | mg L-1 |
| daily | Q | 2,721,943 | 315.52 | 3.20 | 0.00 | 580 | 6,090 | m3 s-1 |
| daily | SSL | 2,647,209 | 6,210 | 7.10 | 0 | 8,219 | 130,542 | t d-1 |
| monthly | SSL | 3,236 | 26,086 | 1,987 | 8.21 | 146,880 | 264,816 | t d-1 |
| monthly | Q | 3,203 | 7,287 | 2,470 | 20.94 | 31,500 | 74,906 | m3 s-1 |
| monthly | SSC | 3,166 | 44.14 | 13.83 | 0.78 | 163.58 | 322.84 | mg L-1 |
| annual | SSC | 445 | 2,384 | 207.93 | 72.20 | 9,130 | 49,988 | mg L-1 |
| annual | Q | 342 | 224.69 | 158.12 | 5.07 | 632.35 | 896.33 | m3 s-1 |
| annual | SSL | 340 | 3,654 | 1,793 | 109.51 | 10,954 | 16,013 | t d-1 |

## Zero-Value Flag Distribution (master product)

For exact-zero variable values, the distribution of their associated flags.

| resolution | variable | flag value | flag meaning | n zero | unit |
|---|---|---|---|---|---|
| daily | SSL | 1 | derived/estimated | 154,246 | t d-1 |
| daily | SSC | 0 | good | 126,785 | mg L-1 |
| daily | Q | 0 | good | 101,335 | m3 s-1 |
| daily | SSL | 2 | suspect | 2,336 | t d-1 |
| monthly | SSL | 0 | good | 32 | t d-1 |
| monthly | SSC | 1 | derived/estimated | 28 | mg L-1 |
| monthly | Q | 0 | good | 11 | m3 s-1 |
| annual | SSC | 0 | good | 10 | mg L-1 |
| annual | SSL | 0 | good | 2 | t d-1 |
| annual | SSC | 2 | suspect | 1 | mg L-1 |
| annual | SSC | 1 | derived/estimated | 1 | mg L-1 |

## Zero-Source Audit (master product)

SSC=0 records broken down by resolution x source, paired Q status, and direct (flag=0) vs derived (flag=1).

| resolution | source | SSC=0 total | Q=0 & SSC=0 | Q>0 & SSC=0 | direct SSC=0 | derived SSC=0 |
|---|---|---|---|---|---|---|
| daily | USGS | 84,950 | 67,957 | 15,142 | 84,950 | 0 |
| daily | HYDAT | 40,401 | 18,439 | 21,940 | 40,401 | 0 |
| daily | GFQA_v2 | 1,404 | 0 | 0 | 1,404 | 0 |
| monthly | Eurasian_River | 28 | 0 | 28 | 0 | 28 |
| daily | HYBAM | 27 | 0 | 27 | 27 | 0 |
| annual | Huanghe | 10 | 0 | 0 | 10 | 0 |
| daily | Bayern | 3 | 0 | 0 | 3 | 0 |
| annual | Chao_Phraya_River | 2 | 0 | 2 | 0 | 1 |

## Monthly Flag 0–1 Source × Variable Statistics (master product)

Monthly-resolution records only. Values restricted to the variable's own flag 0 or 1. `ssc_flag` splits each source × variable into 0 (direct measurement), 1 (derived/estimated), and 0-1 (combined Flag 0-1 subset).

| source | variable | ssc flag | n | median | p05 | p95 | p99 | min | max | unit |
|---|---|---|---|---|---|---|---|---|---|---|
| Eurasian_River | SSL | 0-1 | 3,236 | 1,987 | 8.21 | 146,880 | 264,816 | 0 | 803,520 | t d-1 |
| Eurasian_River | Q | 0-1 | 3,203 | 2,470 | 20.94 | 31,500 | 74,906 | 0 | 106,873 | m3 s-1 |
| Eurasian_River | Q | 1 | 3,166 | 2,512 | 30.00 | 31,355 | 72,127 | 0.03 | 104,000 | m3 s-1 |
| Eurasian_River | SSC | 0-1 | 3,166 | 13.83 | 0.78 | 163.58 | 322.84 | 0 | 17,857 | mg L-1 |
| Eurasian_River | SSC | 1 | 3,166 | 13.83 | 0.78 | 163.58 | 322.84 | 0 | 17,857 | mg L-1 |
| Eurasian_River | SSL | 1 | 3,166 | 2,074 | 10.37 | 146,880 | 262,224 | 0 | 803,520 | t d-1 |

## Master Source by Variable

Master product (`sed_reference_master.nc`). Presence counts include all finite values regardless of flag; `n_good`/`n_estimated` use the variable's own flag 0/1. A 0-present row means that source does not provide that variable.

| source name | variable | n records | n present | n good | n estimated | n usable | present percent | good percent | estimated percent | usable percent |
|---|---|---|---|---|---|---|---|---|---|---|
| USGS | Q | 1,685,357 | 1,654,322 | 1,609,073 | 0 | 1,609,073 | 98.16% | 95.47% | 0% | 95.47% |
| USGS | SSC | 1,685,357 | 1,685,357 | 1,624,112 | 0 | 1,624,112 | 100% | 96.37% | 0% | 96.37% |
| USGS | SSL | 1,685,357 | 1,654,322 | 0 | 1,559,113 | 1,559,113 | 98.16% | 0% | 92.51% | 92.51% |
| HYDAT | SSC | 671,979 | 671,979 | 653,370 | 0 | 653,370 | 100% | 97.23% | 0% | 97.23% |
| HYDAT | SSL | 671,979 | 663,720 | 0 | 640,454 | 640,454 | 98.77% | 0% | 95.31% | 95.31% |
| HYDAT | Q | 671,979 | 663,720 | 656,910 | 0 | 656,910 | 98.77% | 97.76% | 0% | 97.76% |
| Bayern | Q | 421,052 | 388,964 | 384,049 | 0 | 384,049 | 92.38% | 91.21% | 0% | 91.21% |
| Bayern | SSC | 421,052 | 421,052 | 405,967 | 0 | 405,967 | 100% | 96.42% | 0% | 96.42% |
| Bayern | SSL | 421,052 | 388,964 | 0 | 371,581 | 371,581 | 92.38% | 0% | 88.25% | 88.25% |
| GFQA_v2 | SSC | 185,954 | 185,954 | 173,610 | 0 | 173,610 | 100% | 93.36% | 0% | 93.36% |
| GFQA_v2 | SSL | 185,954 | 56,161 | 0 | 49,254 | 49,254 | 30.20% | 0% | 26.49% | 26.49% |
| GFQA_v2 | Q | 185,954 | 56,161 | 54,071 | 0 | 54,071 | 30.20% | 29.08% | 0% | 29.08% |
| Mekong_Delta | SSC | 11,921 | 1,523 | 1,405 | 0 | 1,405 | 12.78% | 11.79% | 0% | 11.79% |
| Mekong_Delta | Q | 11,921 | 1,523 | 1,374 | 0 | 1,374 | 12.78% | 11.53% | 0% | 11.53% |
| Mekong_Delta | SSL | 11,921 | 11,921 | 11,283 | 482 | 11,765 | 100% | 94.65% | 4.04% | 98.69% |
| HYBAM | SSC | 9,404 | 9,404 | 9,214 | 0 | 9,214 | 100% | 97.98% | 0% | 97.98% |
| HYBAM | SSL | 9,404 | 9,094 | 0 | 8,870 | 8,870 | 96.70% | 0% | 94.32% | 94.32% |
| HYBAM | Q | 9,404 | 9,094 | 9,054 | 0 | 9,054 | 96.70% | 96.28% | 0% | 96.28% |
| Robotham | SSC | 3,432 | 3,432 | 2,159 | 0 | 2,159 | 100% | 62.91% | 0% | 62.91% |
| Robotham | SSL | 3,432 | 3,414 | 0 | 2,089 | 2,089 | 99.48% | 0% | 60.87% | 60.87% |
| Robotham | Q | 3,432 | 3,414 | 3,249 | 0 | 3,249 | 99.48% | 94.67% | 0% | 94.67% |
| Eurasian_River | SSC | 3,263 | 3,193 | 0 | 3,166 | 3,166 | 97.85% | 0% | 97.03% | 97.03% |
| Eurasian_River | Q | 3,263 | 3,204 | 3,203 | 0 | 3,203 | 98.19% | 98.16% | 0% | 98.16% |
| Eurasian_River | SSL | 3,263 | 3,263 | 3,236 | 0 | 3,236 | 100% | 99.17% | 0% | 99.17% |
| Fukushima | Q | 3,069 | 3,069 | 3,034 | 0 | 3,034 | 100% | 98.86% | 0% | 98.86% |
| Fukushima | SSC | 3,069 | 3,069 | 3,023 | 0 | 3,023 | 100% | 98.50% | 0% | 98.50% |
| Fukushima | SSL | 3,069 | 3,069 | 0 | 2,991 | 2,991 | 100% | 0% | 97.46% | 97.46% |
| NERC | SSL | 624 | 566 | 0 | 546 | 546 | 90.71% | 0% | 87.50% | 87.50% |
| NERC | SSC | 624 | 624 | 596 | 0 | 596 | 100% | 95.51% | 0% | 95.51% |
| NERC | Q | 624 | 566 | 565 | 0 | 565 | 90.71% | 90.54% | 0% | 90.54% |
| Chao_Phraya_River | SSL | 348 | 348 | 340 | 0 | 340 | 100% | 97.70% | 0% | 97.70% |
| Chao_Phraya_River | Q | 348 | 348 | 342 | 0 | 342 | 100% | 98.28% | 0% | 98.28% |
| Chao_Phraya_River | SSC | 348 | 348 | 0 | 336 | 336 | 100% | 0% | 96.55% | 96.55% |
| Rhine | SSC | 312 | 312 | 299 | 0 | 299 | 100% | 95.83% | 0% | 95.83% |
| Rhine | Q | 312 | 312 | 303 | 0 | 303 | 100% | 97.12% | 0% | 97.12% |
| Rhine | SSL | 312 | 312 | 0 | 292 | 292 | 100% | 0% | 93.59% | 93.59% |
| Shashi_Jianli | SSC | 154 | 154 | 147 | 0 | 147 | 100% | 95.45% | 0% | 95.45% |
| Shashi_Jianli | Q | 154 | 154 | 154 | 0 | 154 | 100% | 100% | 0% | 100% |
| Shashi_Jianli | SSL | 154 | 154 | 0 | 147 | 147 | 100% | 0% | 95.45% | 95.45% |
| Huanghe | SSC | 120 | 120 | 109 | 0 | 109 | 100% | 90.83% | 0% | 90.83% |
| Huanghe | SSL | 120 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| Huanghe | Q | 120 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| GloRiSe | Q | 103 | 89 | 89 | 0 | 89 | 86.41% | 86.41% | 0% | 86.41% |
| GloRiSe | SSL | 103 | 89 | 89 | 0 | 89 | 86.41% | 86.41% | 0% | 86.41% |
| GloRiSe | SSC | 103 | 103 | 103 | 0 | 103 | 100% | 100% | 0% | 100% |
| Yajiang | SSC | 23 | 23 | 23 | 0 | 23 | 100% | 100% | 0% | 100% |
| Yajiang | Q | 23 | 14 | 14 | 0 | 14 | 60.87% | 60.87% | 0% | 60.87% |
| Yajiang | SSL | 23 | 14 | 0 | 14 | 14 | 60.87% | 0% | 60.87% | 60.87% |
| Myanmar | SSC | 6 | 6 | 6 | 0 | 6 | 100% | 100% | 0% | 100% |
| Myanmar | Q | 6 | 4 | 4 | 0 | 4 | 66.67% | 66.67% | 0% | 66.67% |
| Myanmar | SSL | 6 | 4 | 0 | 4 | 4 | 66.67% | 0% | 66.67% | 66.67% |

## Master Source × Variable Value Distribution

Master product. Value statistics over all finite values (all resolutions, flags 0–8), complementing the monthly Flag 0–1 table above.

| source | variable | n | mean | median | min | max | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|---|
| USGS | SSC | 1,685,357 | 704.79 | 30 | 0 | 4,300,000 | 0 | 1,290 | 15,000 | mg L-1 |
| USGS | Q | 1,654,322 | 146.38 | 1.39 | 0 | 29,733 | 0.00 | 272.69 | 4,417 | m3 s-1 |
| USGS | SSL | 1,654,322 | 5,280 | 3.67 | 0 | 46,974,252 | 0 | 7,507 | 103,299 | t d-1 |
| HYDAT | SSC | 671,979 | 82.70 | 16 | 0 | 33,300 | 0 | 335 | 1,050 | mg L-1 |
| HYDAT | SSL | 663,720 | 6,372 | 6.86 | 0 | 999,000 | 0 | 18,100 | 162,000 | t d-1 |
| HYDAT | Q | 663,720 | 359.08 | 4.55 | 0 | 32,100 | 0.00 | 1,960 | 7,300 | m3 s-1 |
| Bayern | SSC | 421,052 | 52.67 | 16.98 | 0 | 45,954 | 2.96 | 162.08 | 635.44 | mg L-1 |
| Bayern | Q | 388,964 | 77.12 | 28.20 | 0.27 | 2,920 | 1.76 | 316 | 566 | m3 s-1 |
| Bayern | SSL | 388,964 | 617.36 | 37.75 | 0.04 | 1,351,396 | 1.64 | 1,596 | 10,200 | t d-1 |
| GFQA_v2 | SSC | 185,954 | 64.25 | 13 | 0 | 39,780 | 1.60 | 206.67 | 840 | mg L-1 |
| GFQA_v2 | Q | 56,161 | 172.53 | 1.80 | 0 | 4,972,518 | 0 | 254.40 | 993.24 | m3 s-1 |
| GFQA_v2 | SSL | 56,161 | 539.47 | 4.15 | 0 | 4,296,256 | 0 | 1,035 | 8,224 | t d-1 |
| Mekong_Delta | SSL | 11,921 | 58,928 | 26,283 | 0.01 | 498,423 | 1,583 | 210,131 | 366,577 | t d-1 |
| HYBAM | SSC | 9,404 | 271.17 | 93.40 | 0 | 7,170 | 6 | 1,152 | 1,980 | mg L-1 |
| HYBAM | SSL | 9,094 | 454,724 | 137,274 | 0 | 6,933,246 | 1,185 | 1,872,538 | 2,907,066 | t d-1 |
| HYBAM | Q | 9,094 | 37,319 | 8,996 | 56.07 | 260,100 | 705.53 | 184,475 | 246,400 | m3 s-1 |
| Robotham | SSC | 3,432 | 34.79 | 19.20 | 3.05 | 746.95 | 7.50 | 122.78 | 280.35 | mg L-1 |
| Robotham | Q | 3,414 | 0.05 | 0.04 | 0.00 | 0.95 | 0.01 | 0.13 | 0.23 | m3 s-1 |
| Robotham | SSL | 3,414 | 0.30 | 0.05 | 0.00 | 39.58 | 0.01 | 1.16 | 4.79 | t d-1 |
| Eurasian_River | SSL | 3,263 | 26,356 | 1,901 | 0 | 803,520 | 6.68 | 146,880 | 279,763 | t d-1 |
| Eurasian_River | Q | 3,204 | 7,284 | 2,470 | 0 | 106,873 | 20.73 | 31,500 | 74,899 | m3 s-1 |
| Eurasian_River | SSC | 3,193 | 43.88 | 13.77 | 0 | 17,857 | 0.77 | 162.48 | 322.82 | mg L-1 |
| Fukushima | Q | 3,069 | 6.38 | 3.43 | 0.01 | 471.92 | 0.53 | 21.36 | 52.16 | m3 s-1 |
| Fukushima | SSC | 3,069 | 80.46 | 75 | 4.52 | 1,391 | 13.86 | 167.78 | 310.40 | mg L-1 |
| Fukushima | SSL | 3,069 | 89.54 | 18.37 | 0.06 | 56,709 | 1.15 | 206.46 | 782.95 | t d-1 |
| Mekong_Delta | SSC | 1,523 | 105.35 | 103.87 | 5.48 | 379.34 | 17.62 | 227.98 | 271.73 | mg L-1 |
| Mekong_Delta | Q | 1,523 | 8,149 | 6,710 | 194.31 | 19,844 | 1,679 | 15,507 | 17,406 | m3 s-1 |
| NERC | SSC | 624 | 181.71 | 39.50 | 1 | 3,901 | 8 | 739.40 | 2,063 | mg L-1 |
| NERC | SSL | 566 | 9.37 | 2.01 | 0.04 | 293.76 | 0.11 | 46.69 | 131.54 | t d-1 |
| NERC | Q | 566 | 0.63 | 0.40 | 0.01 | 2.76 | 0.04 | 2.23 | 2.50 | m3 s-1 |
| Chao_Phraya_River | Q | 348 | 222.97 | 157.81 | 1.27 | 1,144 | 4.86 | 631.50 | 891.03 | m3 s-1 |
| Chao_Phraya_River | SSC | 348 | 232.14 | 207.65 | 0 | 1,156 | 82.50 | 399.04 | 519.98 | mg L-1 |
| Chao_Phraya_River | SSL | 348 | 3,755 | 1,766 | 0 | 55,688 | 119.10 | 10,987 | 17,149 | t d-1 |
| Rhine | Q | 312 | 1,886 | 1,645 | 520.17 | 7,430 | 811.80 | 3,933 | 5,346 | m3 s-1 |
| Rhine | SSL | 312 | 5,590 | 2,924 | 250.13 | 148,933 | 767.53 | 17,295 | 34,341 | t d-1 |
| Rhine | SSC | 312 | 26.72 | 21.09 | 3 | 232 | 8 | 60.33 | 103.37 | mg L-1 |
| Shashi_Jianli | SSC | 154 | 78.19 | 47.50 | 7 | 1,040 | 9.65 | 178.15 | 812.37 | mg L-1 |
| Shashi_Jianli | Q | 154 | 13,265 | 11,000 | 6,150 | 35,400 | 6,940 | 26,470 | 31,287 | m3 s-1 |
| Shashi_Jianli | SSL | 154 | 129,413 | 37,935 | 5,050 | 2,228,429 | 6,234 | 395,515 | 1,888,424 | t d-1 |
| Huanghe | SSC | 120 | 10,311 | 2,805 | 0 | 141,000 | 0 | 59,395 | 88,600 | mg L-1 |
| GloRiSe | SSC | 103 | 471.93 | 66.67 | 0.80 | 26,830 | 2.72 | 1,036 | 2,476 | mg L-1 |
| GloRiSe | Q | 89 | 1,385 | 160 | 0.01 | 29,427 | 0.28 | 8,208 | 16,563 | m3 s-1 |
| GloRiSe | SSL | 89 | 70,793 | 253.81 | 0.61 | 3,013,780 | 3.12 | 237,600 | 1,518,374 | t d-1 |
| Yajiang | SSC | 23 | 548.70 | 340 | 40 | 3,500 | 62 | 1,552 | 3,078 | mg L-1 |
| Yajiang | Q | 14 | 2,092 | 321 | 9 | 13,000 | 26.49 | 7,969 | 11,994 | m3 s-1 |
| Yajiang | SSL | 14 | 329,487 | 5,089 | 264.38 | 3,931,200 | 597.97 | 1,569,358 | 3,458,832 | t d-1 |
| Myanmar | SSC | 6 | 1,102 | 756.37 | 55 | 2,673 | 86.30 | 2,553 | 2,649 | mg L-1 |
| Myanmar | SSL | 4 | 3,645,174 | 3,294,222 | 14,256 | 7,977,997 | 505,021 | 7,276,661 | 7,837,730 | t d-1 |
| Myanmar | Q | 4 | 22,875 | 23,200 | 3,000 | 42,100 | 4,695 | 40,600 | 41,800 | m3 s-1 |

## Master Source × Variable Value Distribution (Flag 0–1)

Master product. Value statistics over finite values restricted to the variable's own flag 0 (good) or 1 (derived/estimated), all resolutions.

| source | variable | n | mean | median | min | max | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|---|
| USGS | SSC | 1,624,112 | 634.11 | 28.40 | 0 | 2,360,000 | 0 | 1,080 | 13,000 | mg L-1 |
| USGS | Q | 1,609,073 | 147.69 | 1.33 | 0 | 29,733 | 0.00 | 267.03 | 4,559 | m3 s-1 |
| USGS | SSL | 1,559,113 | 4,833 | 3.24 | 0 | 3,831,851 | 0 | 6,287 | 94,386 | t d-1 |
| HYDAT | Q | 656,910 | 358.27 | 4.50 | 0 | 32,100 | 0.00 | 1,960 | 7,250 | m3 s-1 |
| HYDAT | SSC | 653,370 | 71.75 | 16 | 0 | 16,400 | 0 | 298 | 837 | mg L-1 |
| HYDAT | SSL | 640,454 | 6,034 | 6.18 | 0 | 999,000 | 0 | 16,500 | 154,000 | t d-1 |
| Bayern | SSC | 405,967 | 35.66 | 16.55 | 0 | 4,629 | 3.06 | 117.03 | 364.67 | mg L-1 |
| Bayern | Q | 384,049 | 76.02 | 28.20 | 0.27 | 1,640 | 1.75 | 311 | 555 | m3 s-1 |
| Bayern | SSL | 371,581 | 460.60 | 35.74 | 0.06 | 567,895 | 1.68 | 1,194 | 6,996 | t d-1 |
| GFQA_v2 | SSC | 173,610 | 53.01 | 12.12 | 0 | 18,900 | 1.56 | 178.42 | 639.91 | mg L-1 |
| GFQA_v2 | Q | 54,071 | 51.90 | 1.70 | 0 | 39,016 | 0 | 240 | 941.38 | m3 s-1 |
| GFQA_v2 | SSL | 49,254 | 315.11 | 3.70 | 0 | 269,677 | 0 | 828.07 | 5,913 | t d-1 |
| Mekong_Delta | SSL | 11,765 | 59,582 | 27,058 | 117.47 | 498,423 | 1,655 | 211,566 | 367,934 | t d-1 |
| HYBAM | SSC | 9,214 | 275.08 | 97.10 | 0 | 4,648 | 7 | 1,160 | 1,981 | mg L-1 |
| HYBAM | Q | 9,054 | 37,448 | 9,039 | 194.40 | 260,100 | 714.80 | 185,210 | 246,400 | m3 s-1 |
| HYBAM | SSL | 8,870 | 463,558 | 152,355 | 0 | 6,009,596 | 1,508 | 1,892,870 | 2,908,635 | t d-1 |
| Robotham | Q | 3,249 | 0.05 | 0.04 | 0.00 | 0.95 | 0.01 | 0.13 | 0.21 | m3 s-1 |
| Eurasian_River | SSL | 3,236 | 26,086 | 1,987 | 0 | 803,520 | 8.21 | 146,880 | 264,816 | t d-1 |
| Eurasian_River | Q | 3,203 | 7,287 | 2,470 | 0 | 106,873 | 20.94 | 31,500 | 74,906 | m3 s-1 |
| Eurasian_River | SSC | 3,166 | 44.14 | 13.83 | 0 | 17,857 | 0.78 | 163.58 | 322.84 | mg L-1 |
| Fukushima | Q | 3,034 | 5.47 | 3.43 | 0.12 | 72.53 | 0.55 | 20.09 | 40.60 | m3 s-1 |
| Fukushima | SSC | 3,023 | 76.06 | 73.32 | 4.52 | 663.52 | 13.79 | 154.59 | 218.88 | mg L-1 |
| Fukushima | SSL | 2,991 | 46.32 | 18.05 | 0.18 | 1,871 | 1.21 | 179.38 | 554.04 | t d-1 |
| Robotham | SSC | 2,159 | 19.18 | 16.28 | 5.72 | 124.89 | 7.91 | 39.81 | 73.68 | mg L-1 |
| Robotham | SSL | 2,089 | 0.09 | 0.05 | 0.00 | 4.49 | 0.01 | 0.28 | 0.69 | t d-1 |
| Mekong_Delta | SSC | 1,405 | 109.50 | 109.07 | 11.98 | 294.68 | 19.37 | 227.97 | 268.79 | mg L-1 |
| Mekong_Delta | Q | 1,374 | 8,666 | 7,828 | 1,452 | 19,012 | 2,631 | 15,538 | 17,400 | m3 s-1 |
| NERC | SSC | 596 | 161.18 | 40 | 3 | 2,931 | 9 | 719 | 1,230 | mg L-1 |
| NERC | Q | 565 | 0.64 | 0.40 | 0.01 | 2.76 | 0.04 | 2.23 | 2.50 | m3 s-1 |
| NERC | SSL | 546 | 7.67 | 1.94 | 0.04 | 293.76 | 0.13 | 34.81 | 89.34 | t d-1 |
| Chao_Phraya_River | Q | 342 | 224.69 | 158.12 | 1.58 | 1,144 | 5.07 | 632.35 | 896.33 | m3 s-1 |
| Chao_Phraya_River | SSL | 340 | 3,654 | 1,793 | 0 | 32,799 | 109.51 | 10,954 | 16,013 | t d-1 |
| Chao_Phraya_River | SSC | 336 | 232.57 | 207.67 | 0 | 1,000 | 111.51 | 397.17 | 438.89 | mg L-1 |
| Rhine | Q | 303 | 1,841 | 1,640 | 520.17 | 5,363 | 827.86 | 3,421 | 4,948 | m3 s-1 |
| Rhine | SSC | 299 | 25.52 | 21.17 | 4 | 150.58 | 8.89 | 52.32 | 75.81 | mg L-1 |
| Rhine | SSL | 292 | 4,718 | 2,951 | 419.73 | 56,091 | 864.11 | 14,716 | 25,563 | t d-1 |
| Shashi_Jianli | Q | 154 | 13,265 | 11,000 | 6,150 | 35,400 | 6,940 | 26,470 | 31,287 | m3 s-1 |
| Shashi_Jianli | SSC | 147 | 57.79 | 44 | 7 | 244 | 9.30 | 152.80 | 224.54 | mg L-1 |
| Shashi_Jianli | SSL | 147 | 81,618 | 37,111 | 5,050 | 505,958 | 6,227 | 333,697 | 485,885 | t d-1 |
| Huanghe | SSC | 109 | 9,016 | 2,720 | 0 | 88,600 | 0 | 37,980 | 87,568 | mg L-1 |
| GloRiSe | SSC | 103 | 471.93 | 66.67 | 0.80 | 26,830 | 2.72 | 1,036 | 2,476 | mg L-1 |
| GloRiSe | Q | 89 | 1,385 | 160 | 0.01 | 29,427 | 0.28 | 8,208 | 16,563 | m3 s-1 |
| GloRiSe | SSL | 89 | 70,793 | 253.81 | 0.61 | 3,013,780 | 3.12 | 237,600 | 1,518,374 | t d-1 |
| Yajiang | SSC | 23 | 548.70 | 340 | 40 | 3,500 | 62 | 1,552 | 3,078 | mg L-1 |
| Yajiang | Q | 14 | 2,092 | 321 | 9 | 13,000 | 26.49 | 7,969 | 11,994 | m3 s-1 |
| Yajiang | SSL | 14 | 329,487 | 5,089 | 264.38 | 3,931,200 | 597.97 | 1,569,358 | 3,458,832 | t d-1 |
| Myanmar | SSC | 6 | 1,102 | 756.37 | 55 | 2,673 | 86.30 | 2,553 | 2,649 | mg L-1 |
| Myanmar | SSL | 4 | 3,645,174 | 3,294,222 | 14,256 | 7,977,997 | 505,021 | 7,276,661 | 7,837,730 | t d-1 |
| Myanmar | Q | 4 | 22,875 | 23,200 | 3,000 | 42,100 | 4,695 | 40,600 | 41,800 | m3 s-1 |

## Co-Located Variable Coverage

Co-location counts include all finite (non-NaN) values regardless of quality flag (flags 0–8).

| resolution | combination | combination type | n records | n clusters | pct of all records | pct of nonempty records | pct of clusters |
|---|---|---|---|---|---|---|---|
| daily | Any | any | 2,993,390 | 7,087 | 100% | 100% | 100% |
| daily | Q+SSC+SSL | exact | 2,781,406 | 3,427 | 92.92% | 92.92% | 48.36% |
| daily | SSC only | exact | 201,586 | 5,278 | 6.73% | 6.73% | 74.47% |
| daily | SSL only | exact | 10,398 | 4 | 0.35% | 0.35% | 0.06% |
| monthly | Any | any | 3,263 | 17 | 100% | 100% | 100% |
| monthly | Q+SSC+SSL | exact | 3,193 | 17 | 97.85% | 97.85% | 100% |
| annual | Any | any | 468 | 31 | 100% | 100% | 100% |
| annual | Q+SSC+SSL | exact | 348 | 7 | 74.36% | 74.36% | 22.58% |
| annual | SSC only | exact | 120 | 24 | 25.64% | 25.64% | 77.42% |
| monthly | SSL only | exact | 59 | 8 | 1.81% | 1.81% | 47.06% |
| monthly | Q+SSL | exact | 11 | 2 | 0.34% | 0.34% | 11.76% |
| annual | SSL only | exact | 0 | 0 | 0% | 0% | 0% |
| annual | Q only | exact | 0 | 0 | 0% | 0% | 0% |
| annual | SSC+SSL | exact | 0 | 0 | 0% | 0% | 0% |
| annual | Q+SSL | exact | 0 | 0 | 0% | 0% | 0% |
| annual | Q+SSC | exact | 0 | 0 | 0% | 0% | 0% |
| daily | SSC+SSL | exact | 0 | 0 | 0% | 0% | 0% |
| daily | Q+SSL | exact | 0 | 0 | 0% | 0% | 0% |

_Showing first 18 of 24 rows._

## Flag 0–1 Co-Located Variable Coverage (master product)

Only records where Q, SSC, and SSL all have flag 0 (good) or 1 (estimated/derived) simultaneously.

| resolution | combination | n records | pct records | n clusters |
|---|---|---|---|---|
| daily | Q+SSC+SSL all flag 0–1 | 2,636,700 | 88.08% | 3,422 |
| monthly | Q+SSC+SSL all flag 0–1 | 3,166 | 97.03% | 17 |
| annual | Q+SSC+SSL all flag 0–1 | 336 | 71.79% | 7 |

## Satellite Source by Variable

Validation-only satellite products may contain many rows with no Q or SSL values; keep this table near any satellite analysis.

| source name | variable | n records | n present | n good | n estimated | n usable | present percent | good percent | estimated percent | usable percent |
|---|---|---|---|---|---|---|---|---|---|---|
| RiverSed | SSC | 14,199,854 | 14,199,854 | 13,821,824 | 0 | 13,821,824 | 100% | 97.34% | 0% | 97.34% |
| RiverSed | Q | 14,199,854 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| RiverSed | SSL | 14,199,854 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| GSED | Q | 2,144,599 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| GSED | SSC | 2,144,599 | 1,183,801 | 1,169,955 | 0 | 1,169,955 | 55.20% | 54.55% | 0% | 54.55% |
| GSED | SSL | 2,144,599 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| Dethier | Q | 133,823 | 133,823 | 132,614 | 0 | 132,614 | 100% | 99.10% | 0% | 99.10% |
| Dethier | SSC | 133,823 | 133,823 | 129,313 | 0 | 129,313 | 100% | 96.63% | 0% | 96.63% |
| Dethier | SSL | 133,823 | 133,823 | 132,693 | 0 | 132,693 | 100% | 99.16% | 0% | 99.16% |

## Satellite Low-Coverage Rows

| source name | variable | n records | n present | present percent | usable percent |
|---|---|---|---|---|---|
| RiverSed | SSL | 14,199,854 | 0 | 0% | 0% |
| RiverSed | Q | 14,199,854 | 0 | 0% | 0% |
| GSED | SSL | 2,144,599 | 0 | 0% | 0% |
| GSED | Q | 2,144,599 | 0 | 0% | 0% |

## Satellite Product Coverage Warning

**The satellite product (``sed_reference_satellite.nc``) is validation-only and should not be interpreted as a uniformly complete Q–SSC–SSL time-series product because variable availability differs among source datasets.**

It concatenates records from multiple independent satellite-derived sources (Dethier, GSED, RiverSed) that each cover different variables.  Reading a variable column (e.g. ``Q`` or ``SSL``) directly from the file will return mostly NaN because the source that produced those rows does not carry that variable.

### Per-source variable availability

- **Dethier**:
- Q: 133,823 present (100.0)
- SSC: 133,823 present (100.0)
- SSL: 133,823 present (100.0)
- **GSED**:
- Q: 0 present (0.0)
- SSC: 1,183,801 present (55.199177)
- SSL: 0 present (0.0)
- **RiverSed**:
- Q: 0 present (0.0)
- SSC: 14,199,854 present (100.0)
- SSL: 0 present (0.0)

### Product-level summary

| variable | total records | n present | present % |
|---|---|---|---|
| Q | 16,478,276 | 133,823 | 0.812% |
| SSC | 16,478,276 | 15,517,478 | 94.169% |
| SSL | 16,478,276 | 133,823 | 0.812% |

### Recommended usage

1. **Always filter by source before reading variable values.** Join the satellite file with ``satellite_catalog.csv`` on ``satellite_station_uid`` to resolve the ``source`` name for each row.
2. **Filter rows where the target variable is present for that source:**

```python
# Python / xarray example — keep only non-missing SSC
ds = xr.open_dataset('sed_reference_satellite.nc')
ssc_valid = ds['SSC'].where(ds['SSC'].notnull())

# Or filter by source × variable combination in pandas
df = ds.to_dataframe()
# Keep only Dethier rows for Q, GSED+RiverSed rows for SSC, etc.
dethier_q = df[df['source'] == 'Dethier'][['Q']].dropna()
gsed_ssc  = df[df['source'] == 'GSED'][['SSC']].dropna()
```

3. **Use ``usable_percent`` as a guidance threshold.**  For any source × variable combination with ``present_percent < 1 %``, treat the column as effectively empty for that source.
4. **Do not use ``sed_reference_satellite.nc`` as input to model training or as a continuous forcing dataset.**  It is designed for cross-validation between satellite retrievals and in-situ reference records.


## Extreme Value Review Points

Extreme values selected from all finite (non-NaN) records regardless of quality flag.

| resolution | variable | value | station index | record index | review reason | unit |
|---|---|---|---|---|---|---|
| daily | SSL | 46,974,252 | 6,985 | 2,694,532 | top_high_value | t d-1 |
| daily | SSL | 12,110,549 | 6,985 | 2,694,531 | top_high_value | t d-1 |
| daily | SSL | 8,911,015 | 6,670 | 2,106,945 | top_high_value | t d-1 |
| daily | SSL | 8,505,740 | 6,670 | 2,106,946 | top_high_value | t d-1 |
| daily | SSL | 7,977,997 | 6,183 | 1,300,888 | top_high_value | t d-1 |
| daily | SSL | 6,933,246 | 5,647 | 673,032 | top_high_value | t d-1 |
| daily | SSL | 6,009,596 | 5,645 | 662,471 | top_high_value | t d-1 |
| daily | SSL | 5,870,118 | 6,670 | 2,106,944 | top_high_value | t d-1 |
| daily | SSL | 5,392,687 | 5,645 | 662,469 | top_high_value | t d-1 |
| daily | SSL | 5,369,010 | 6,670 | 2,110,280 | top_high_value | t d-1 |
| daily | SSL | 5,173,767 | 5,645 | 662,470 | top_high_value | t d-1 |
| daily | SSL | 5,078,504 | 6,670 | 2,110,279 | top_high_value | t d-1 |
| daily | SSL | 5,069,019 | 5,647 | 673,038 | top_high_value | t d-1 |
| daily | SSL | 4,982,668 | 5,645 | 662,405 | top_high_value | t d-1 |
| daily | SSL | 4,974,426 | 6,670 | 2,106,947 | top_high_value | t d-1 |
| daily | Q | 4,972,518 | 1,428 | 534,611 | top_high_value | m3 s-1 |
| daily | SSL | 4,935,461 | 5,644 | 667,574 | top_high_value | t d-1 |
| daily | SSL | 4,682,162 | 5,647 | 673,036 | top_high_value | t d-1 |
| daily | SSL | 4,645,106 | 5,650 | 679,502 | top_high_value | t d-1 |
| daily | SSL | 4,608,520 | 5,645 | 662,532 | top_high_value | t d-1 |

_Showing first 20 of 180 rows._

## Interpretation Notes

- `good_percent` can be misleading when a release intentionally marks derived SSL as estimated; always check `estimated_percent` to distinguish estimated data (acceptable) from truly missing/problematic data. The gap `usable_percent - good_percent` is explained by `estimated_percent`.
- Satellite rows MUST be filtered by source and variable before use because validation-sidecar variable density is source-dependent and highly variable (see Satellite Product Coverage Warning above).
- Cluster percentages across variable combinations are non-exclusive because the same cluster may contain different variable combinations at different time steps.
- Extreme review points are candidates for manual inspection, not automatic removal rules.

### Quality Flag Reference

| Flag | Meaning |
|------|---------|
| 0 | good (direct measurement) |
| 1 | derived / estimated |
| 2 | suspect |
| 3 | bad |
| 8 | not checked |
| 9 | missing (NaN in data variables) |

Tables without "Flag" in the title (Matrix Coverage, Summary Statistics, Co-Located Coverage, Extreme Values) include **all finite values regardless of flag** (flags 0–8). Use the Flag 0–1 and Zero-Value Flag Distribution tables to assess data quality.


## Figures

- `fig_Q_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_Q_distribution.png`
- `fig_SSC_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_SSC_distribution.png`
- `fig_SSL_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_SSL_distribution.png`
