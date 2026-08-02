# Variable Coverage Results Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `output_other/stats_release/variable_summary/tables`
- Variables covered: Q, SSC, SSL.

## Headline

- Product groups summarized: 3
- Product-variable denominator rows: 58,059,054
- Satellite source-variable rows with less than 1% present values: 4
- Extreme review points emitted: 180

## Product by Variable Coverage

| product | variable | n records | n present | n good | n estimated | n usable | present percent | good percent | estimated percent | usable percent |
|---|---|---|---|---|---|---|---|---|---|---|
| master | Q | 2,873,420 | 2,856,356 | 2,803,162 | 0 | 2,803,162 | 99.41% | 97.55% | 0% | 97.55% |
| master | SSC | 2,873,420 | 2,862,858 | 2,732,153 | 28,676 | 2,760,829 | 99.63% | 95.08% | 1.00% | 96.08% |
| master | SSL | 2,873,420 | 2,863,661 | 75,679 | 2,648,231 | 2,723,910 | 99.66% | 2.63% | 92.16% | 94.80% |
| climatology | Q | 1,322 | 782 | 782 | 0 | 782 | 59.15% | 59.15% | 0% | 59.15% |
| climatology | SSC | 1,322 | 804 | 759 | 45 | 804 | 60.82% | 57.41% | 3.40% | 60.82% |
| climatology | SSL | 1,322 | 1,298 | 1,298 | 0 | 1,298 | 98.18% | 98.18% | 0% | 98.18% |
| satellite | Q | 16,478,276 | 133,823 | 132,614 | 0 | 132,614 | 0.81% | 0.80% | 0% | 0.80% |
| satellite | SSC | 16,478,276 | 15,517,478 | 15,121,092 | 0 | 15,121,092 | 94.17% | 91.76% | 0% | 91.76% |
| satellite | SSL | 16,478,276 | 133,823 | 132,693 | 0 | 132,693 | 0.81% | 0.81% | 0% | 0.81% |

## Matrix Coverage by Resolution

| resolution | n records total | n clusters total | Q records | Q record coverage pct | SSC records | SSC record coverage pct | SSL records | SSL record coverage pct |
|---|---|---|---|---|---|---|---|---|
| daily | 2,746,665 | 1,596 | 2,729,764 | 99.38% | 2,736,267 | 99.62% | 2,740,162 | 99.76% |
| monthly | 126,136 | 2,117 | 126,093 | 99.97% | 125,972 | 99.87% | 123,000 | 97.51% |
| annual | 619 | 58 | 499 | 80.61% | 619 | 100% | 499 | 80.61% |

## Analysis-Grade Coverage by Resolution

Analysis-grade rows use the release filter emitted by this module; no non-release QC intermediates are read.

| resolution | analysis grade | n records total | Q record coverage pct | SSC record coverage pct | SSL record coverage pct |
|---|---|---|---|---|---|
| daily | release_nonmissing | 2,746,665 | 99.38% | 99.62% | 99.76% |
| monthly | release_nonmissing | 126,136 | 99.97% | 99.87% | 97.51% |
| annual | release_nonmissing | 619 | 80.61% | 100% | 80.61% |

## Variable Summary Statistics

| resolution | variable | n nonmissing records | n nonmissing clusters | mean | median | min | max | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | SSL | 2,740,162 | 1,558 | 6,876 | 7.97 | 0 | 46,974,252 | 0 | 9,920 | 145,000 | ton day-1 |
| daily | SSC | 2,736,267 | 1,596 | 456.08 | 23 | 0 | 4,300,000 | 1 | 705 | 7,200 | mg L-1 |
| daily | Q | 2,729,764 | 1,558 | 329.89 | 3.37 | 0 | 260,100 | 0.00 | 614.48 | 6,258 | m3 s-1 |
| monthly | Q | 126,093 | 2,117 | 263.96 | 0.11 | 0 | 4,972,518 | 0 | 186.44 | 4,225 | m3 s-1 |
| monthly | SSC | 125,972 | 2,117 | 10,051 | 26.95 | 0 | 1,043,519,680 | 0 | 10,491 | 28,674 | mg L-1 |
| monthly | SSL | 123,000 | 2,117 | 946.13 | 0.76 | 0 | 4,296,256 | 0 | 608.08 | 13,824 | ton day-1 |
| annual | SSC | 619 | 58 | 2,151 | 207.52 | 0 | 141,000 | 10 | 8,544 | 55,136 | mg L-1 |
| annual | Q | 499 | 34 | 169.65 | 59.20 | 0 | 1,144 | 0.40 | 622.71 | 845.94 | m3 s-1 |
| annual | SSL | 499 | 34 | 2,706 | 985.63 | 0 | 55,688 | 0.48 | 10,160 | 16,744 | ton day-1 |

## Analysis-Grade Summary Statistics

| resolution | variable | analysis grade | n nonmissing records | mean | median | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|
| daily | SSL | release_nonmissing | 2,740,162 | 6,876 | 7.97 | 0 | 9,920 | 145,000 | ton day-1 |
| daily | SSC | release_nonmissing | 2,736,267 | 456.08 | 23 | 1 | 705 | 7,200 | mg L-1 |
| daily | Q | release_nonmissing | 2,729,764 | 329.89 | 3.37 | 0.00 | 614.48 | 6,258 | m3 s-1 |
| monthly | Q | release_nonmissing | 126,093 | 263.96 | 0.11 | 0 | 186.44 | 4,225 | m3 s-1 |
| monthly | SSC | release_nonmissing | 125,972 | 10,051 | 26.95 | 0 | 10,491 | 28,674 | mg L-1 |
| monthly | SSL | release_nonmissing | 123,000 | 946.13 | 0.76 | 0 | 608.08 | 13,824 | ton day-1 |
| annual | SSC | release_nonmissing | 619 | 2,151 | 207.52 | 10 | 8,544 | 55,136 | mg L-1 |
| annual | Q | release_nonmissing | 499 | 169.65 | 59.20 | 0.40 | 622.71 | 845.94 | m3 s-1 |
| annual | SSL | release_nonmissing | 499 | 2,706 | 985.63 | 0.48 | 10,160 | 16,744 | ton day-1 |

## Co-Located Variable Coverage

| resolution | combination | combination type | n records | n clusters | pct of all records | pct of nonempty records | pct of clusters |
|---|---|---|---|---|---|---|---|
| daily | Any | any | 2,746,665 | 1,596 | 100% | 100% | 100% |
| daily | Q+SSC+SSL | exact | 2,729,764 | 1,558 | 99.38% | 99.38% | 97.62% |
| monthly | Any | any | 126,136 | 2,117 | 100% | 100% | 100% |
| monthly | Q+SSC+SSL | exact | 122,834 | 2,117 | 97.38% | 97.38% | 100% |
| daily | SSL only | exact | 10,398 | 4 | 0.38% | 0.38% | 0.25% |
| daily | SSC only | exact | 6,503 | 115 | 0.24% | 0.24% | 7.21% |
| monthly | Q+SSC | exact | 3,136 | 8 | 2.49% | 2.49% | 0.38% |
| annual | Any | any | 619 | 58 | 100% | 100% | 100% |
| annual | Q+SSC+SSL | exact | 499 | 34 | 80.61% | 80.61% | 58.62% |
| monthly | Q+SSL | exact | 123 | 12 | 0.10% | 0.10% | 0.57% |
| annual | SSC only | exact | 120 | 24 | 19.39% | 19.39% | 41.38% |
| monthly | SSL only | exact | 41 | 2 | 0.03% | 0.03% | 0.09% |
| monthly | SSC+SSL | exact | 2 | 1 | 0.00% | 0.00% | 0.05% |
| annual | Q only | exact | 0 | 0 | 0% | 0% | 0% |
| annual | Q+SSL | exact | 0 | 0 | 0% | 0% | 0% |
| annual | Q+SSC | exact | 0 | 0 | 0% | 0% | 0% |
| annual | SSL only | exact | 0 | 0 | 0% | 0% | 0% |
| annual | SSC+SSL | exact | 0 | 0 | 0% | 0% | 0% |

_Showing first 18 of 24 rows._

## Analysis-Grade Co-Located Coverage

| resolution | analysis grade | combination | n records | n clusters | pct of nonempty records | pct of clusters |
|---|---|---|---|---|---|---|
| daily | release_nonmissing | Any | 2,746,665 | 1,596 | 100% | 100% |
| daily | release_nonmissing | Q+SSC+SSL | 2,729,764 | 1,558 | 99.38% | 97.62% |
| monthly | release_nonmissing | Any | 126,136 | 2,117 | 100% | 100% |
| monthly | release_nonmissing | Q+SSC+SSL | 122,834 | 2,117 | 97.38% | 100% |
| daily | release_nonmissing | SSL only | 10,398 | 4 | 0.38% | 0.25% |
| daily | release_nonmissing | SSC only | 6,503 | 115 | 0.24% | 7.21% |
| monthly | release_nonmissing | Q+SSC | 3,136 | 8 | 2.49% | 0.38% |
| annual | release_nonmissing | Any | 619 | 58 | 100% | 100% |
| annual | release_nonmissing | Q+SSC+SSL | 499 | 34 | 80.61% | 58.62% |
| monthly | release_nonmissing | Q+SSL | 123 | 12 | 0.10% | 0.57% |
| annual | release_nonmissing | SSC only | 120 | 24 | 19.39% | 41.38% |
| monthly | release_nonmissing | SSL only | 41 | 2 | 0.03% | 0.09% |
| monthly | release_nonmissing | SSC+SSL | 2 | 1 | 0.00% | 0.05% |
| annual | release_nonmissing | Q only | 0 | 0 | 0% | 0% |
| annual | release_nonmissing | Q+SSL | 0 | 0 | 0% | 0% |
| annual | release_nonmissing | Q+SSC | 0 | 0 | 0% | 0% |
| annual | release_nonmissing | SSL only | 0 | 0 | 0% | 0% |
| annual | release_nonmissing | SSC+SSL | 0 | 0 | 0% | 0% |

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
| monthly | SSC | 1,043,519,680 | 1,674 | 2,738,785 | top_high_value | mg L-1 |
| daily | SSL | 46,974,252 | 1,520 | 2,371,194 | top_high_value | ton day-1 |
| daily | SSL | 12,110,549 | 1,520 | 2,371,193 | top_high_value | ton day-1 |
| daily | SSL | 8,911,015 | 1,209 | 1,923,367 | top_high_value | ton day-1 |
| daily | SSL | 8,505,740 | 1,209 | 1,923,368 | top_high_value | ton day-1 |
| daily | SSL | 7,999,617 | 199 | 394,754 | top_high_value | ton day-1 |
| daily | SSL | 7,977,997 | 719 | 1,072,497 | top_high_value | ton day-1 |
| daily | SSL | 6,933,246 | 201 | 405,320 | top_high_value | ton day-1 |
| daily | SSL | 6,009,596 | 199 | 394,872 | top_high_value | ton day-1 |
| daily | SSL | 5,870,118 | 1,209 | 1,923,366 | top_high_value | ton day-1 |
| daily | SSL | 5,823,018 | 199 | 394,871 | top_high_value | ton day-1 |
| daily | SSL | 5,392,687 | 199 | 394,868 | top_high_value | ton day-1 |
| daily | SSL | 5,369,010 | 1,209 | 1,926,702 | top_high_value | ton day-1 |
| daily | SSL | 5,173,767 | 199 | 394,870 | top_high_value | ton day-1 |
| daily | SSL | 5,120,672 | 199 | 394,869 | top_high_value | ton day-1 |
| daily | SSL | 5,078,504 | 1,209 | 1,926,701 | top_high_value | ton day-1 |
| daily | SSL | 5,069,019 | 201 | 405,327 | top_high_value | ton day-1 |
| daily | SSL | 4,982,668 | 199 | 394,740 | top_high_value | ton day-1 |
| daily | SSL | 4,974,426 | 1,209 | 1,923,369 | top_high_value | ton day-1 |
| monthly | Q | 4,972,518 | 2,028 | 2,820,758 | top_high_value | m3 s-1 |

_Showing first 20 of 180 rows._

## Interpretation Notes

- `good_percent` can be misleading when a release intentionally marks derived SSL as estimated; always check `estimated_percent` to distinguish estimated data (acceptable) from truly missing/problematic data. The gap `usable_percent - good_percent` is explained by `estimated_percent`.
- Satellite rows MUST be filtered by source and variable before use because validation-sidecar variable density is source-dependent and highly variable (see Satellite Product Coverage Warning above).
- Extreme review points are candidates for manual inspection, not automatic removal rules.

## Figures

- `fig_Q_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_Q_distribution.png`
- `fig_SSC_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_SSC_distribution.png`
- `fig_SSL_distribution.png`: `output_other/stats_release/variable_summary/figures/fig_SSL_distribution.png`
