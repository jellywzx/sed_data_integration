# S8 temporal coverage statistics for ESSD

## Manuscript-ready summary

The release provides daily, monthly, annual, and climatological sediment-reference products with temporal coverage spanning from 1912 to 2025 across available products.

The main time-series products contain 7,087 daily clusters, 17 monthly clusters, and 31 annual clusters.

Daily records span 1948-2025, with 2,993,390 valid cluster-time observations across 7,087 clusters and a median record length of 37 time steps.

Monthly records span 1938-2000, with 3,263 valid cluster-time observations across 17 clusters and a median record length of 142 time steps.

Annual records span 1912-2020, with 468 valid cluster-time observations across 31 clusters and a median record length of 5 time steps. Annual coverage is described by observed records and calendar span, rather than by a regular-grid coverage ratio, because the annual time axis may be sparse.

Long daily records are a major strength of the release: 1,949 daily clusters are longer than 50 time steps and 1,412 daily clusters are longer than 100 time steps.

Peak temporal coverage differs by product: daily: 4,098 active units in 2018; monthly: 16 active units in 1979; annual: 31 active units in 2018.

## Output tables

- `tables/table_temporal_coverage_by_resolution.csv`
- `tables/table_temporal_coverage_by_variable.csv`
- `tables/table_active_units_by_year.csv`
- `tables/table_record_length_distribution.csv`
- `tables/table_temporal_coverage_record_lengths_by_unit.csv`
- `tables/table_long_records_by_resolution.csv`
- `tables/table_temporal_coverage_by_source.csv`
- `tables/table_temporal_coverage_by_region_resolution.csv`

## Output figures

- `figures/fig_active_units_by_year.png` and `.pdf`
- `figures/fig_records_by_year_variable.png` and `.pdf`
- `figures/fig_record_length_distribution.png` and `.pdf`
- `figures/fig_long_record_counts.png` and `.pdf`
- `figures/fig_temporal_coverage_heatmap.png` and `.pdf`
- `figures/fig_source_temporal_span.png` and `.pdf`
