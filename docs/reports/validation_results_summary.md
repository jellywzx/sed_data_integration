# Validation Results Summary

## 1. Input files
- `README.md`
- `application_sed_reference_release.md`
- `example_reference_workflow.py`
- `release_inventory.csv`
- `release_validation_report.csv`
- `satellite_catalog.csv`
- `satellite_validation_catalog.csv`
- `sed_reference_climatology.nc`
- `sed_reference_cluster_basins.gpkg`
- `sed_reference_cluster_points.gpkg`
- `sed_reference_master.nc`
- `sed_reference_overlap_candidates.csv.gz`
- `sed_reference_satellite.nc`
- `sed_reference_satellite_candidates.csv.gz`
- `sed_reference_satellite_candidates.parquet`
- `sed_reference_source_stations.gpkg`
- `sed_reference_timeseries_annual.nc`
- `sed_reference_timeseries_daily.nc`
- `sed_reference_timeseries_monthly.nc`
- `source_dataset_catalog.csv`
- `source_station_catalog.csv`
- `station_catalog.csv`
- `station_catalog.csv.bak`

## 2. Product schema inspection
- Key validation fields found by exact name: Q, SSC, SSL, candidate_quality_score, candidate_rank, cluster_id, cluster_uid, date, is_overlap, n_candidates_at_time, resolution, selected_flag, source, source_family, source_station_uid, time.
- Inspected NetCDF dimensions, variables, global attributes, catalog CSV columns, and GPKG sidecar layers/fields when readable.
- Master-record loading note: master release records loaded without Q/SSC/SSL values; 45496 rows loaded from sed_reference_overlap_candidates.csv.gz.

## 3. Method
- This script reads only s8 release products in `--release-dir`; it does not read s6/s7 intermediate files, source station NC, or raw source datasets.
- Source-pair metrics are computed from `sed_reference_overlap_candidates.csv.gz` when it contains candidate-level values for at least two distinct sources at the same cluster, resolution, and time/date key.
- Source taxonomy uses lightweight substring rules: USGS, HYDAT, satellite, in_situ, secondary_compilation, and other.
- Source pairs and family pairs are ordered stably by source family/source/source station uid; bias is `source_b - source_a`.

## 4. Key numeric results
- `HYDAT vs USGS` / `other vs other` / `all` / `Q`: n_pairs=731, bias=0.01848438435148899, RMSE=0.37564499791383155, MAPE=0.07965764216185091, Spearman=0.9999983564100402.
- `HYDAT vs USGS` / `other vs other` / `all` / `SSC`: n_pairs=731, bias=0.0013679890560875513, RMSE=0.08270396169735617, MAPE=0.115206137174432, Spearman=0.9996958632283435.
- `HYDAT vs USGS` / `other vs other` / `all` / `SSL`: n_pairs=731, bias=0.26420080808543017, RMSE=5.933168618487033, MAPE=0.24321344765681363, Spearman=0.9997872895488499.
- Candidate sidecar summary rows: 7; candidate rows summarized: 45496.

## 5. Limitations
- If s8 release products keep only selected records, non-selected candidate values cannot be validated.
- `is_overlap=1` indicates overlap or competition, but it does not mean all candidate source values were preserved.
- True same cluster-time multi-source candidate consistency requires a candidate-level provenance sidecar, or a separate upstream candidate-validation script.
- This validation is constrained to s8 release products only.

## 6. Generated tables and figures
- `validation_product_schema_inventory.csv`: generated
- `validation_overlap_availability_diagnostic.csv`: generated
- `validation_selected_source_summary.csv`: generated
- `validation_overlap_flag_summary.csv`: generated
- `validation_overlap_candidate_summary.csv`: generated
- `validation_overlap_pair_records.csv`: generated
- `validation_overlap_source_pairs.csv`: generated
- `validation_overlap_source_pairs_by_variable.csv`: generated
- `figures/overlap_pair_scatter_Q.png`: generated
- `figures/overlap_pair_bias_box_Q.png`: generated
- `figures/overlap_pair_scatter_SSC.png`: generated
- `figures/overlap_pair_bias_box_SSC.png`: generated
- `figures/overlap_pair_scatter_SSL.png`: generated
- `figures/overlap_pair_bias_box_SSL.png`: generated
- `validation_resolution_source_family.csv`: generated
- `validation_source_by_resolution.csv`: generated
- `validation_overlap_by_source.csv`: generated
- `validation_overlap_by_resolution.csv`: generated
- `validation_summary_data.json`: generated
- `validation_results_report.md`: generated
- `validation_results_summary.md`: generated

## 7. Skipped validations
- satellite vs in-situ multi-window validation: skipped by scope
- long-term benchmark comparison: skipped by scope
- coverage bias and representativeness validation: skipped by scope
