# S8 temporal coverage statistics for ESSD

## Manuscript-ready summary

The release provides daily, monthly, annual, and climatological sediment-reference products with temporal coverage spanning from 1912 to 2025 across available products.

The main time-series products contain 4,717 daily stations, 2,697 monthly stations, and 49 annual stations.

Daily records span 1948-2025, with 2,963,235 valid station-time observations across 4,717 stations and a median record length of 39 time steps.

Monthly records span 1938-2023, with 100,901 valid station-time observations across 2,697 stations and a median record length of 35 time steps.

Annual records span 1912-2022, with 535 valid station-time observations across 49 stations and a median record length of 5 time steps. Annual coverage is described by observed records and calendar span, rather than by a regular-grid coverage ratio, because the annual time axis may be sparse.

Long daily records are a major strength of the release: 1,725 daily stations are longer than 50 time steps and 1,390 daily stations are longer than 100 time steps.

Peak temporal coverage differs by product: daily: 2,256 active units in 2015; monthly: 1,907 active units in 2018; annual: 35 active units in 2019.

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
