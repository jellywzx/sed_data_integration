# S8 temporal coverage statistics for ESSD

## Manuscript-ready summary

The release provides daily, monthly, annual, and climatological sediment-reference products with temporal coverage spanning from 1884 to 2025 across all products.

The main time-series products contain 1,475 daily clusters, 1,861 monthly clusters, and 58 annual clusters.

Daily records span 1948-2025, with 2,813,598 valid cluster-time observations across 1,475 clusters and a median record length of 3.7 years.

Monthly records span 1938-2021, with 121,363 valid cluster-time observations across 1,861 clusters and a median record length of 7.3 years.

Annual records span 1912-2021, with 619 valid cluster-time observations across 58 clusters and a median record length of 5.0 years. Annual coverage is described by observed records and calendar span, rather than by a regular-grid coverage ratio, because the annual time axis is irregular.

Long daily records are a major strength of the release: 15 daily clusters are longer than 50 years and 0 daily clusters are longer than 100 years.

Peak temporal coverage differs by product: daily: 562 active units in 1980; monthly: 1,485 active units in 2013; annual: 47 active units in 2018.

The climatology product is reported separately as 1,359 standalone climatology stations, because it is not a basin-cluster time-series matrix.

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
