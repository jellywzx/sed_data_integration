# Satellite / In-Situ Validation Summary

## 1. Inputs
- Input mode: `candidate_sidecar`.
- Input file: `/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/sed_reference_overlap_candidates.csv.gz`.
- Load note: candidate sidecar loaded; appended 147 satellite CSV rows from sed_reference_satellite_candidates.parquet.
- Observation rows after normalization: 45643.

## 2. Method
- Satellite/reach-scale records are anchors; in-situ records are selected within the same cluster and resolution.
- Windows are cumulative: `exact` is included in `pm1d`, and `pm1d` is included in `pm2d`; `window_exclusive=false`.
- Bias and residuals are `satellite - in-situ`; MAPE skips pairs where the in-situ denominator is zero.
- R2 is `Pearson^2` when Pearson is finite.

## 3. Key Results
- Pair rows: 20.
- `pm2d` / `SSC` / `GSED vs GFQA_v2`: n_pairs=10, n_clusters=5, bias=53.73199882507324, RMSE=100.35395801268612, Spearman=0.2938935377559395.
- `pm1d` / `SSC` / `GSED vs GFQA_v2`: n_pairs=8, n_clusters=5, bias=71.7837483882904, RMSE=110.36441444788888, Spearman=0.21957751641341997.
- `exact` / `SSC` / `GSED vs GFQA_v2`: n_pairs=2, n_clusters=2, bias=20.955001831054688, RMSE=24.768612700304047, Spearman=0.9999999999999999.

## 4. Limitations
- Candidate-sidecar results depend on what the sidecar preserved; if it only contains overlap candidates, wider windows may be incomplete.
- Missing river width is reported as `missing`; missing climate zone is reported as `unknown`.

## 5. Generated Outputs
- `validation_satellite_insitu_pairs.csv`: generated
- `validation_satellite_insitu_metrics.csv`: generated
- `figures/satellite_insitu_scatter_by_window_SSC.png`: generated
- `figures/satellite_insitu_residual_by_ssc_bin.png`: generated
- `figures/satellite_insitu_metric_heatmap.png`: generated
- `validation_satellite_insitu_summary.md`: generated
