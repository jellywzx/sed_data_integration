# Variable Coverage Results Report

## Scope

- Release package: `output/sed_reference_release`
- Output tables: `$WORKSPACE/output_other/stats_release_full/variable_summary/tables`
- Variables covered: Q, SSC, SSL.

## Headline

- Product groups summarized: 3
- Product-variable denominator rows: 58,065,990
- Satellite source-variable rows with less than 1% present values: 5
- Extreme review points emitted: 180

## Product by Variable Coverage

| product | variable | n records | n present | n good | n estimated | n usable | present percent | good percent | estimated percent | usable percent |
|---|---|---|---|---|---|---|---|---|---|---|
| master | Q | 2,847,547 | 2,830,490 | 2,778,769 | 0 | 2,778,769 | 99.40% | 97.58% | 0% | 97.58% |
| master | SSC | 2,847,547 | 2,836,984 | 2,709,299 | 28,108 | 2,737,407 | 99.63% | 95.15% | 0.99% | 96.13% |
| master | SSL | 2,847,547 | 2,837,795 | 73,942 | 2,648,674 | 2,722,616 | 99.66% | 2.60% | 93.02% | 95.61% |
| climatology | Q | 1,322 | 782 | 782 | 0 | 782 | 59.15% | 59.15% | 0% | 59.15% |
| climatology | SSC | 1,322 | 804 | 787 | 17 | 804 | 60.82% | 59.53% | 1.29% | 60.82% |
| climatology | SSL | 1,322 | 1,298 | 1,298 | 0 | 1,298 | 98.18% | 98.18% | 0% | 98.18% |
| satellite | Q | 16,506,461 | 133,379 | 132,172 | 0 | 132,172 | 0.81% | 0.80% | 0% | 0.80% |
| satellite | SSC | 16,506,461 | 1,345,809 | 1,326,713 | 0 | 1,326,713 | 8.15% | 8.04% | 0% | 8.04% |
| satellite | SSL | 16,506,461 | 133,379 | 132,259 | 0 | 132,259 | 0.81% | 0.80% | 0% | 0.80% |

## Matrix Coverage by Resolution

| resolution | n records total | n clusters total | Q records | Q record coverage pct | SSC records | SSC record coverage pct | SSL records | SSL record coverage pct |
|---|---|---|---|---|---|---|---|---|
| daily | 2,724,382 | 1,607 | 2,707,488 | 99.38% | 2,713,984 | 99.62% | 2,717,886 | 99.76% |
| monthly | 122,546 | 1,875 | 122,503 | 99.96% | 122,381 | 99.87% | 119,410 | 97.44% |
| annual | 619 | 58 | 499 | 80.61% | 619 | 100% | 499 | 80.61% |

## Analysis-Grade Coverage by Resolution

Analysis-grade rows use the release filter emitted by this module; no non-release QC intermediates are read.

| resolution | analysis grade | n records total | Q record coverage pct | SSC record coverage pct | SSL record coverage pct |
|---|---|---|---|---|---|
| daily | release_nonmissing | 2,724,382 | 99.38% | 99.62% | 99.76% |
| monthly | release_nonmissing | 122,546 | 99.96% | 99.87% | 97.44% |
| annual | release_nonmissing | 619 | 80.61% | 100% | 80.61% |

## Variable Summary Statistics

| resolution | variable | n nonmissing records | n nonmissing clusters | mean | median | min | max | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|---|---|
| daily | SSL | 2,717,886 | 1,571 | 6,865 | 7.92 | 0 | 46,974,252 | 0 | 9,647 | 144,715 | ton day-1 |
| daily | SSC | 2,713,984 | 1,607 | 458.65 | 23 | 0 | 4,300,000 | 1 | 708 | 7,280 | mg L-1 |
| daily | Q | 2,707,488 | 1,571 | 329.53 | 3.34 | 0 | 260,100 | 0.00 | 589 | 6,290 | m3 s-1 |
| monthly | Q | 122,503 | 1,875 | 271.18 | 0.11 | 0 | 4,972,518 | 0 | 190.90 | 4,370 | m3 s-1 |
| monthly | SSC | 122,381 | 1,875 | 1,670 | 27 | 0 | 424,904 | 0 | 9,957 | 28,027 | mg L-1 |
| monthly | SSL | 119,410 | 1,875 | 975.95 | 0.79 | 0 | 4,296,256 | 0 | 622.08 | 14,688 | ton day-1 |
| annual | SSC | 619 | 58 | 2,151 | 207.52 | 0 | 141,000 | 10 | 8,544 | 55,136 | mg L-1 |
| annual | Q | 499 | 34 | 169.65 | 59.20 | 0 | 1,144 | 0.40 | 622.71 | 845.94 | m3 s-1 |
| annual | SSL | 499 | 34 | 2,706 | 985.63 | 0 | 55,688 | 0.48 | 10,160 | 16,744 | ton day-1 |

## Analysis-Grade Summary Statistics

| resolution | variable | analysis grade | n nonmissing records | mean | median | p05 | p95 | p99 | unit |
|---|---|---|---|---|---|---|---|---|---|
| daily | SSL | release_nonmissing | 2,717,886 | 6,865 | 7.92 | 0 | 9,647 | 144,715 | ton day-1 |
| daily | SSC | release_nonmissing | 2,713,984 | 458.65 | 23 | 1 | 708 | 7,280 | mg L-1 |
| daily | Q | release_nonmissing | 2,707,488 | 329.53 | 3.34 | 0.00 | 589 | 6,290 | m3 s-1 |
| monthly | Q | release_nonmissing | 122,503 | 271.18 | 0.11 | 0 | 190.90 | 4,370 | m3 s-1 |
| monthly | SSC | release_nonmissing | 122,381 | 1,670 | 27 | 0 | 9,957 | 28,027 | mg L-1 |
| monthly | SSL | release_nonmissing | 119,410 | 975.95 | 0.79 | 0 | 622.08 | 14,688 | ton day-1 |
| annual | SSC | release_nonmissing | 619 | 2,151 | 207.52 | 10 | 8,544 | 55,136 | mg L-1 |
| annual | Q | release_nonmissing | 499 | 169.65 | 59.20 | 0.40 | 622.71 | 845.94 | m3 s-1 |
| annual | SSL | release_nonmissing | 499 | 2,706 | 985.63 | 0.48 | 10,160 | 16,744 | ton day-1 |

## Co-Located Variable Coverage

| resolution | combination | combination type | n records | n clusters | pct of all records | pct of nonempty records | pct of clusters |
|---|---|---|---|---|---|---|---|
| daily | Any | any | 2,724,382 | 1,607 | 100% | 100% | 100% |
| daily | Q+SSC+SSL | exact | 2,707,488 | 1,571 | 99.38% | 99.38% | 97.76% |
| monthly | Any | any | 122,546 | 1,875 | 100% | 100% | 100% |
| monthly | Q+SSC+SSL | exact | 119,243 | 1,875 | 97.30% | 97.30% | 100% |
| daily | SSL only | exact | 10,398 | 4 | 0.38% | 0.38% | 0.25% |
| daily | SSC only | exact | 6,496 | 113 | 0.24% | 0.24% | 7.03% |
| monthly | Q+SSC | exact | 3,136 | 7 | 2.56% | 2.56% | 0.37% |
| annual | Any | any | 619 | 58 | 100% | 100% | 100% |
| annual | Q+SSC+SSL | exact | 499 | 34 | 80.61% | 80.61% | 58.62% |
| monthly | Q+SSL | exact | 124 | 11 | 0.10% | 0.10% | 0.59% |
| annual | SSC only | exact | 120 | 24 | 19.39% | 19.39% | 41.38% |
| monthly | SSL only | exact | 41 | 2 | 0.03% | 0.03% | 0.11% |
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
| daily | release_nonmissing | Any | 2,724,382 | 1,607 | 100% | 100% |
| daily | release_nonmissing | Q+SSC+SSL | 2,707,488 | 1,571 | 99.38% | 97.76% |
| monthly | release_nonmissing | Any | 122,546 | 1,875 | 100% | 100% |
| monthly | release_nonmissing | Q+SSC+SSL | 119,243 | 1,875 | 97.30% | 100% |
| daily | release_nonmissing | SSL only | 10,398 | 4 | 0.38% | 0.25% |
| daily | release_nonmissing | SSC only | 6,496 | 113 | 0.24% | 7.03% |
| monthly | release_nonmissing | Q+SSC | 3,136 | 7 | 2.56% | 0.37% |
| annual | release_nonmissing | Any | 619 | 58 | 100% | 100% |
| annual | release_nonmissing | Q+SSC+SSL | 499 | 34 | 80.61% | 58.62% |
| monthly | release_nonmissing | Q+SSL | 124 | 11 | 0.10% | 0.59% |
| annual | release_nonmissing | SSC only | 120 | 24 | 19.39% | 41.38% |
| monthly | release_nonmissing | SSL only | 41 | 2 | 0.03% | 0.11% |
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
| RiverSed | SSC | 14,228,483 | 28,629 | 27,872 | 0 | 27,872 | 0.20% | 0.20% | 0% | 0.20% |
| RiverSed | Q | 14,228,483 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| RiverSed | SSL | 14,228,483 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| GSED | Q | 2,144,599 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| GSED | SSC | 2,144,599 | 1,183,801 | 1,169,955 | 0 | 1,169,955 | 55.20% | 54.55% | 0% | 54.55% |
| GSED | SSL | 2,144,599 | 0 | 0 | 0 | 0 | 0% | 0% | 0% | 0% |
| Dethier | Q | 133,379 | 133,379 | 132,172 | 0 | 132,172 | 100% | 99.10% | 0% | 99.10% |
| Dethier | SSC | 133,379 | 133,379 | 128,886 | 0 | 128,886 | 100% | 96.63% | 0% | 96.63% |
| Dethier | SSL | 133,379 | 133,379 | 132,259 | 0 | 132,259 | 100% | 99.16% | 0% | 99.16% |

## Satellite Low-Coverage Rows

| source name | variable | n records | n present | present percent | usable percent |
|---|---|---|---|---|---|
| RiverSed | Q | 14,228,483 | 0 | 0% | 0% |
| RiverSed | SSL | 14,228,483 | 0 | 0% | 0% |
| RiverSed | SSC | 14,228,483 | 28,629 | 0.20% | 0.20% |
| GSED | Q | 2,144,599 | 0 | 0% | 0% |
| GSED | SSL | 2,144,599 | 0 | 0% | 0% |

## Satellite Product Coverage Warning

**The satellite product (``sed_reference_satellite.nc``) is validation-only and MUST NOT be treated as a complete Q/SSC/SSL time series for any station.**

It concatenates records from multiple independent satellite-derived sources (Dethier, GSED, RiverSed) that each cover different variables.  Reading a variable column (e.g. ``Q`` or ``SSL``) directly from the file will return mostly NaN because the source that produced those rows does not carry that variable.

### Per-source variable availability

- **Dethier**:
- Q: 133,379 present (100.0)
- SSC: 133,379 present (100.0)
- SSL: 133,379 present (100.0)
- **GSED**:
- Q: 0 present (0.0)
- SSC: 1,183,801 present (55.199177)
- SSL: 0 present (0.0)
- **RiverSed**:
- Q: 0 present (0.0)
- SSC: 28,629 present (0.201209)
- SSL: 0 present (0.0)

### Product-level summary

| variable | total records | n present | present % |
|---|---|---|---|
| Q | 16,506,461 | 133,379 | 0.808% |
| SSC | 16,506,461 | 1,345,809 | 8.153% |
| SSL | 16,506,461 | 133,379 | 0.808% |

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
| daily | SSL | 46,974,252 | 1,530 | 2,372,093 | top_high_value | ton day-1 |
| daily | SSL | 12,110,549 | 1,530 | 2,372,092 | top_high_value | ton day-1 |
| daily | SSL | 8,911,015 | 1,227 | 1,915,817 | top_high_value | ton day-1 |
| daily | SSL | 8,505,740 | 1,227 | 1,915,818 | top_high_value | ton day-1 |
| daily | SSL | 7,999,617 | 248 | 386,565 | top_high_value | ton day-1 |
| daily | SSL | 7,977,997 | 743 | 1,040,498 | top_high_value | ton day-1 |
| daily | SSL | 6,933,246 | 250 | 396,777 | top_high_value | ton day-1 |
| daily | SSL | 6,009,596 | 248 | 386,683 | top_high_value | ton day-1 |
| daily | SSL | 5,870,118 | 1,227 | 1,915,816 | top_high_value | ton day-1 |
| daily | SSL | 5,823,018 | 248 | 386,682 | top_high_value | ton day-1 |
| daily | SSL | 5,392,687 | 248 | 386,679 | top_high_value | ton day-1 |
| daily | SSL | 5,369,010 | 1,227 | 1,919,152 | top_high_value | ton day-1 |
| daily | SSL | 5,173,767 | 248 | 386,681 | top_high_value | ton day-1 |
| daily | SSL | 5,120,672 | 248 | 386,680 | top_high_value | ton day-1 |
| daily | SSL | 5,078,504 | 1,227 | 1,919,151 | top_high_value | ton day-1 |
| daily | SSL | 5,069,019 | 250 | 396,784 | top_high_value | ton day-1 |
| daily | SSL | 4,982,668 | 248 | 386,551 | top_high_value | ton day-1 |
| daily | SSL | 4,974,426 | 1,227 | 1,915,819 | top_high_value | ton day-1 |
| monthly | Q | 4,972,518 | 1,997 | 2,795,549 | top_high_value | m3 s-1 |
| daily | SSL | 4,935,461 | 247 | 391,169 | top_high_value | ton day-1 |

_Showing first 20 of 180 rows._

## Interpretation Notes

- `good_percent` can be misleading when a release intentionally marks derived SSL as estimated; always check `estimated_percent` to distinguish estimated data (acceptable) from truly missing/problematic data. The gap `usable_percent - good_percent` is explained by `estimated_percent`.
- Satellite rows MUST be filtered by source and variable before use because validation-sidecar variable density is source-dependent and highly variable (see Satellite Product Coverage Warning above).
- Extreme review points are candidates for manual inspection, not automatic removal rules.

## Figures

- `fig_Q_distribution.png`: `$WORKSPACE/output_other/stats_release_full/variable_summary/figures/fig_Q_distribution.png`
- `fig_SSC_distribution.png`: `$WORKSPACE/output_other/stats_release_full/variable_summary/figures/fig_SSC_distribution.png`
- `fig_SSL_distribution.png`: `$WORKSPACE/output_other/stats_release_full/variable_summary/figures/fig_SSL_distribution.png`
