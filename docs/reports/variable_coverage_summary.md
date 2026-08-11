# Variable Coverage Results Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/variable_summary/tables`
- Variables covered: Q, SSC, SSL.

## Headline

- Product groups summarized: 3
- Product-variable denominator rows: 58,632,924
- Satellite source-variable rows with less than 1% present values: 4
- Extreme review points emitted: 180

## Product by Variable Coverage

| product | variable | n records | n present | n good | n estimated | n usable | present percent | good percent | estimated percent | usable percent |
|---|---|---|---|---|---|---|---|---|---|---|
| master | Q | 3,064,671 | 2,852,106 | 2,791,428 | 0 | 2,791,428 | 93.06% | 91.08% | 0% | 91.08% |
| master | SSC | 3,064,671 | 3,054,050 | 2,915,287 | 28,123 | 2,943,410 | 99.65% | 95.13% | 0.92% | 96.04% |
| master | SSL | 3,064,671 | 2,862,606 | 15,100 | 2,700,499 | 2,715,599 | 93.41% | 0.49% | 88.12% | 88.61% |
| climatology | Q | 1,361 | 782 | 782 | 0 | 782 | 57.46% | 57.46% | 0% | 57.46% |
| climatology | SSC | 1,361 | 806 | 759 | 47 | 806 | 59.22% | 55.77% | 3.45% | 59.22% |
| climatology | SSL | 1,361 | 1,337 | 1,337 | 0 | 1,337 | 98.24% | 98.24% | 0% | 98.24% |
| satellite | Q | 16,478,276 | 133,823 | 132,614 | 0 | 132,614 | 0.81% | 0.80% | 0% | 0.80% |
| satellite | SSC | 16,478,276 | 15,517,478 | 15,121,092 | 0 | 15,121,092 | 94.17% | 91.76% | 0% | 91.76% |
| satellite | SSL | 16,478,276 | 133,823 | 132,693 | 0 | 132,693 | 0.81% | 0.81% | 0% | 0.81% |

## Matrix Coverage by Resolution

| resolution | n records total | reference stations total | Q records | Q record coverage pct | SSC records | SSC record coverage pct | SSL records | SSL record coverage pct |
|---|---|---|---|---|---|---|---|---|
| daily | 2,963,235 | 4,717 | 2,804,180 | 94.63% | 2,952,708 | 99.64% | 2,814,621 | 94.98% |
| monthly | 100,901 | 2,697 | 47,578 | 47.15% | 100,807 | 99.91% | 47,637 | 47.21% |
| annual | 535 | 49 | 348 | 65.05% | 535 | 100% | 348 | 65.05% |

## Analysis-Grade Coverage by Resolution

Analysis-grade rows use the release filter emitted by this module; no non-release QC intermediates are read.

| resolution | analysis grade | n records total | Q record coverage pct | SSC record coverage pct | SSL record coverage pct |
|---|---|---|---|---|---|
| daily | release_nonmissing | 2,963,235 | 94.63% | 99.64% | 94.98% |
| monthly | release_nonmissing | 100,901 | 47.15% | 99.91% | 47.21% |
| annual | release_nonmissing | 535 | 65.05% | 100% | 65.05% |

## Variable Summary Statistics

| resolution | variable | n nonmissing records | n nonmissing reference stations | mean | median | min | max | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | SSC | 2,952,708 | 4,717 | 793.94 | 22 | 0 | 1,043,519,680 | 1 | 707 | 6,700 | mg L-1 |
| daily | SSL | 2,814,621 | 2,481 | 6,430 | 7.19 | 0 | 46,974,252 | 0 | 9,219 | 134,305 | t d-1 |
| daily | Q | 2,804,180 | 2,481 | 309.44 | 3.06 | 0 | 260,100 | 0.00 | 572.00 | 5,975 | m3 s-1 |
| monthly | SSC | 100,807 | 2,697 | 1,951 | 21 | 0 | 285,436 | 2.10 | 12,353 | 31,000 | mg L-1 |
| monthly | SSL | 47,637 | 1,240 | 2,224 | 1.75 | 0 | 4,296,256 | 0 | 2,352 | 57,888 | t d-1 |
| monthly | Q | 47,578 | 1,240 | 643.35 | 0.40 | 0 | 4,972,518 | 0 | 924 | 14,769 | m3 s-1 |
| annual | SSC | 535 | 49 | 2,527 | 207.85 | 0 | 141,000 | 5.40 | 8,968 | 67,874 | mg L-1 |
| annual | Q | 348 | 7 | 222.97 | 157.81 | 1.27 | 1,144 | 4.86 | 631.50 | 891.03 | m3 s-1 |
| annual | SSL | 348 | 7 | 3,755 | 1,766 | 0 | 55,688 | 119.10 | 10,987 | 17,149 | t d-1 |

## Analysis-Grade Summary Statistics

| resolution | variable | analysis grade | n nonmissing records | mean | median | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|
| daily | SSC | release_nonmissing | 2,952,708 | 793.94 | 22 | 1 | 707 | 6,700 | mg L-1 |
| daily | SSL | release_nonmissing | 2,814,621 | 6,430 | 7.19 | 0 | 9,219 | 134,305 | t d-1 |
| daily | Q | release_nonmissing | 2,804,180 | 309.44 | 3.06 | 0.00 | 572.00 | 5,975 | m3 s-1 |
| monthly | SSC | release_nonmissing | 100,807 | 1,951 | 21 | 2.10 | 12,353 | 31,000 | mg L-1 |
| monthly | SSL | release_nonmissing | 47,637 | 2,224 | 1.75 | 0 | 2,352 | 57,888 | t d-1 |
| monthly | Q | release_nonmissing | 47,578 | 643.35 | 0.40 | 0 | 924 | 14,769 | m3 s-1 |
| annual | SSC | release_nonmissing | 535 | 2,527 | 207.85 | 5.40 | 8,968 | 67,874 | mg L-1 |
| annual | Q | release_nonmissing | 348 | 222.97 | 157.81 | 4.86 | 631.50 | 891.03 | m3 s-1 |
| annual | SSL | release_nonmissing | 348 | 3,755 | 1,766 | 119.10 | 10,987 | 17,149 | t d-1 |

## Flag 0–1 Summary Statistics (master product)

Statistics computed only on values where the variable flag is 0 (good) or 1 (estimated/derived).

| resolution | variable | n flag01 records | mean | median | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|
| daily | SSC | 2,847,805 | 393.90 | 21.41 | 1 | 588 | 5,880 | mg L-1 |
| daily | Q | 2,744,728 | 312.43 | 3 | 0.00 | 566.34 | 6,050 | m3 s-1 |
| daily | SSL | 2,671,478 | 6,149 | 6.53 | 0 | 8,040 | 129,000 | t d-1 |
| monthly | SSC | 95,093 | 2,016 | 20.40 | 2.10 | 12,721 | 31,389 | mg L-1 |
| monthly | Q | 46,358 | 532.79 | 0.30 | 0 | 950.30 | 14,943 | m3 s-1 |
| monthly | SSL | 43,781 | 2,139 | 1.56 | 0 | 2,246 | 60,480 | t d-1 |
| annual | SSC | 512 | 2,138 | 207.84 | 8.20 | 8,774 | 38,647 | mg L-1 |
| annual | Q | 342 | 224.69 | 158.12 | 5.07 | 632.35 | 896.33 | m3 s-1 |
| annual | SSL | 340 | 3,654 | 1,793 | 109.51 | 10,954 | 16,013 | t d-1 |

## Zero-Value Flag Distribution (master product)

For exact-zero variable values, the distribution of their associated flags.

| resolution | variable | flag value | flag meaning | n zero | unit |
|---|---|---|---|---|---|
| daily | SSL | 1 | derived/estimated | 158,569 | t d-1 |
| daily | SSC | 0 | good | 133,112 | mg L-1 |
| daily | Q | 0 | good | 102,567 | m3 s-1 |
| monthly | Q | 0 | good | 2,893 | m3 s-1 |
| monthly | SSL | 1 | derived/estimated | 2,723 | t d-1 |
| daily | SSL | 2 | suspect | 2,443 | t d-1 |
| monthly | SSC | 0 | good | 389 | mg L-1 |
| monthly | SSL | 2 | suspect | 135 | t d-1 |
| daily | SSL | 0 | good | 89 | t d-1 |
| monthly | SSL | 0 | good | 53 | t d-1 |
| monthly | SSC | 1 | derived/estimated | 28 | mg L-1 |
| annual | SSC | 0 | good | 16 | mg L-1 |
| annual | SSL | 0 | good | 2 | t d-1 |
| annual | SSC | 1 | derived/estimated | 1 | mg L-1 |
| annual | SSC | 2 | suspect | 1 | mg L-1 |

## Zero-Source Audit (master product)

SSC=0 records broken down by resolution x source, paired Q status, and direct (flag=0) vs derived (flag=1).

| resolution | source | SSC=0 total | Q=0 & SSC=0 | Q>0 & SSC=0 | direct SSC=0 | derived SSC=0 |
|---|---|---|---|---|---|---|
| daily | USGS | 115,976 | 90,518 | 23,114 | 115,976 | 0 |
| daily | HYDAT | 40,495 | 18,439 | 22,034 | 40,495 | 0 |
| daily | EUSEDcollab | 13,444 | 6,868 | 6,572 | 13,444 | 0 |
| daily | GFQA_v2 | 1,009 | 0 | 0 | 1,009 | 0 |
| monthly | GFQA_v2 | 777 | 0 | 0 | 777 | 0 |
| monthly | Eurasian_River | 56 | 0 | 56 | 0 | 56 |
| daily | HYBAM | 27 | 0 | 27 | 27 | 0 |
| annual | Huanghe | 20 | 0 | 0 | 20 | 0 |
| annual | GFQA_v2 | 12 | 0 | 0 | 12 | 0 |
| daily | Bayern | 6 | 0 | 0 | 6 | 0 |
| annual | Chao_Phraya_River | 4 | 0 | 4 | 0 | 2 |

## Co-Located Variable Coverage

| resolution | combination | combination type | n records | reference stations | pct of all records | pct of nonempty records | pct of reference stations |
|---|---|---|---|---|---|---|---|
| daily | Any | any | 2,535,695 | 5,180 | 85.57% | 100% | 109.82% |
| daily | Q+SSC+SSL | exact | 2,364,623 | 2,506 | 79.80% | 93.25% | 53.13% |
| annual | Any | any | 499,465 | 213 | 93,357.94% | 100% | 434.69% |
| annual | Q+SSC+SSL | exact | 452,619 | 74 | 84,601.68% | 90.62% | 151.02% |
| daily | SSC only | exact | 160,449 | 3,564 | 5.41% | 6.33% | 75.56% |
| monthly | Any | any | 100,901 | 2,697 | 100% | 100% | 100% |
| monthly | SSC only | exact | 53,264 | 2,178 | 52.79% | 52.79% | 80.76% |
| monthly | Q+SSC+SSL | exact | 47,543 | 1,240 | 47.12% | 47.12% | 45.98% |
| annual | SSC only | exact | 46,715 | 152 | 8,731.78% | 9.35% | 310.20% |
| daily | SSL only | exact | 10,498 | 14 | 0.35% | 0.41% | 0.30% |
| daily | Q+SSL | exact | 123 | 12 | 0.00% | 0.00% | 0.25% |
| annual | Q+SSL | exact | 88 | 2 | 16.45% | 0.02% | 4.08% |
| monthly | SSL only | exact | 59 | 8 | 0.06% | 0.06% | 0.30% |
| annual | SSL only | exact | 41 | 2 | 7.66% | 0.01% | 4.08% |
| monthly | Q+SSL | exact | 35 | 10 | 0.03% | 0.03% | 0.37% |
| annual | SSC+SSL | exact | 2 | 1 | 0.37% | 0.00% | 2.04% |
| daily | SSC+SSL | exact | 2 | 1 | 0.00% | 0.00% | 0.02% |
| annual | Q+SSC | exact | 0 | 0 | 0% | 0% | 0% |

_Showing first 18 of 24 rows._

## Analysis-Grade Co-Located Coverage

| resolution | analysis grade | combination | n records | reference stations | pct of nonempty records | pct of reference stations |
|---|---|---|---|---|---|---|
| daily | release_nonmissing | Any | 2,535,695 | 5,180 | 100% | 109.82% |
| daily | release_nonmissing | Q+SSC+SSL | 2,364,623 | 2,506 | 93.25% | 53.13% |
| annual | release_nonmissing | Any | 499,465 | 213 | 100% | 434.69% |
| annual | release_nonmissing | Q+SSC+SSL | 452,619 | 74 | 90.62% | 151.02% |
| daily | release_nonmissing | SSC only | 160,449 | 3,564 | 6.33% | 75.56% |
| monthly | release_nonmissing | Any | 100,901 | 2,697 | 100% | 100% |
| monthly | release_nonmissing | SSC only | 53,264 | 2,178 | 52.79% | 80.76% |
| monthly | release_nonmissing | Q+SSC+SSL | 47,543 | 1,240 | 47.12% | 45.98% |
| annual | release_nonmissing | SSC only | 46,715 | 152 | 9.35% | 310.20% |
| daily | release_nonmissing | SSL only | 10,498 | 14 | 0.41% | 0.30% |
| daily | release_nonmissing | Q+SSL | 123 | 12 | 0.00% | 0.25% |
| annual | release_nonmissing | Q+SSL | 88 | 2 | 0.02% | 4.08% |
| monthly | release_nonmissing | SSL only | 59 | 8 | 0.06% | 0.30% |
| annual | release_nonmissing | SSL only | 41 | 2 | 0.01% | 4.08% |
| monthly | release_nonmissing | Q+SSL | 35 | 10 | 0.03% | 0.37% |
| annual | release_nonmissing | SSC+SSL | 2 | 1 | 0.00% | 2.04% |
| daily | release_nonmissing | SSC+SSL | 2 | 1 | 0.00% | 0.02% |
| annual | release_nonmissing | Q+SSC | 0 | 0 | 0% | 0% |

_Showing first 18 of 24 rows._

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

**The satellite product (``sed_reference_satellite.nc``) is validation-only and MUST NOT be treated as a complete Q/SSC/SSL time series for any station.**

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

| resolution | variable | value | station index | record index | review reason | unit |
|---|---|---|---|---|---|---|
| daily | SSC | 1,043,519,680 | 89 | 151,101 | top_high_value | mg L-1 |
| daily | SSL | 46,974,252 | 4,633 | 2,651,841 | top_high_value | t d-1 |
| daily | SSL | 12,110,549 | 4,633 | 2,651,840 | top_high_value | t d-1 |
| daily | SSL | 8,911,015 | 4,318 | 2,077,185 | top_high_value | t d-1 |
| daily | SSL | 8,505,740 | 4,318 | 2,077,186 | top_high_value | t d-1 |
| daily | SSL | 7,977,997 | 3,831 | 1,276,711 | top_high_value | t d-1 |
| daily | SSL | 6,933,246 | 3,289 | 631,179 | top_high_value | t d-1 |
| daily | SSL | 6,009,596 | 3,287 | 617,820 | top_high_value | t d-1 |
| daily | SSL | 5,870,118 | 4,318 | 2,077,184 | top_high_value | t d-1 |
| daily | SSL | 5,392,687 | 3,287 | 617,818 | top_high_value | t d-1 |
| daily | SSL | 5,369,010 | 4,318 | 2,080,520 | top_high_value | t d-1 |
| daily | SSL | 5,173,767 | 3,287 | 617,819 | top_high_value | t d-1 |
| daily | SSL | 5,078,504 | 4,318 | 2,080,519 | top_high_value | t d-1 |
| daily | SSL | 5,069,019 | 3,289 | 631,185 | top_high_value | t d-1 |
| daily | SSL | 4,982,668 | 3,287 | 617,754 | top_high_value | t d-1 |
| daily | SSL | 4,974,426 | 4,318 | 2,077,187 | top_high_value | t d-1 |
| monthly | Q | 4,972,518 | 5,602 | 3,007,849 | top_high_value | m3 s-1 |
| daily | SSL | 4,935,461 | 3,286 | 630,499 | top_high_value | t d-1 |
| daily | SSL | 4,682,162 | 3,289 | 631,183 | top_high_value | t d-1 |
| daily | SSL | 4,645,106 | 3,292 | 634,481 | top_high_value | t d-1 |

_Showing first 20 of 180 rows._

## Interpretation Notes

- `good_percent` can be misleading when a release intentionally marks derived SSL as estimated; always check `estimated_percent` to distinguish estimated data (acceptable) from truly missing/problematic data. The gap `usable_percent - good_percent` is explained by `estimated_percent`.
- Satellite rows MUST be filtered by source and variable before use because validation-sidecar variable density is source-dependent and highly variable (see Satellite Product Coverage Warning above).
- Extreme review points are candidates for manual inspection, not automatic removal rules.

## Figures

- `fig_Q_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_Q_distribution.png`
- `fig_SSC_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_SSC_distribution.png`
- `fig_SSL_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_SSL_distribution.png`
