# Sediment Reference Dataset Integration and Assessment Workflow

This repository contains the workflow corresponding to **Sections 3.4–5.4** of the manuscript:

> *A harmonized global station-reference dataset of river discharge, suspended sediment concentration, and suspended sediment load*

The repository focuses on the stages after source-level preprocessing: temporal screening, station consolidation, time-series integration, release generation and characterization, structural and cross-product assessment, uncertainty diagnostics, and the model-evaluation demonstration. Source-specific data acquisition, harmonization, georeferencing, and quality-control procedures described earlier in the manuscript are outside the scope of this README.

## Manuscript-to-code map

| Manuscript section | Role in the manuscript | Main scripts/modules |
| --- | --- | --- |
| **Sect. 3.4** Temporal screening, station consolidation, and time-series integration | Classify temporal support, organize records by resolution, consolidate source stations, and integrate overlapping source time series into the main station-reference component | `s1_verify_time_resolution.py`, `time_resolution.py`, `s2_reorganize_qc_by_resolution.py`, `s5_basin_merge.py`, `basin_station_merge.py`, `s6_basin_merge_to_nc.py` |
| **Sect. 4.1** Dataset structure and products | Generate the daily, monthly, annual, climatology, and satellite-derived products and assemble the public release | `s6_*`, `s7_*`, `s8_publish_reference_dataset.py`, `s8_publish_minimal_release_package.py`, `s9_public_station_names.py` |
| **Sect. 4.2** Source contributions | Summarize source-level contributions to released stations and records | `stats_release/source_contribution.py`, `stats_release/source_dataset_layers.py`, `figures/scripts/plot_fig5_combined_source_contribution_direct_release.py` |
| **Sect. 4.3** Spatial coverage and basin attributes | Summarize station distribution, basin-assignment status, and spatial coverage of the three release components | `stats_release/spatial.py`, `stats_release/basin_diagnostics.py`, `figures/scripts/plot_fig6_composite_spatial_coverage_manu_order.py`, `figures/scripts/plot_figs1_source_map_insitu_clim_sat.py` |
| **Sect. 4.4** Temporal coverage and record availability | Summarize active stations, record counts, temporal coverage, and source contributions through time | `stats_release/temporal.py`, `figures/scripts/plot_fig7_active_records_panels.py`, `figures/scripts/plot_figs2_annual_matrix_by_source_by_resolution.py` |
| **Sect. 4.5** Variable availability, distributions, and quality flags | Summarize Q, SSC, and SSL availability, distributions, and quality-flag composition | `stats_release/variable_summary.py`, `stats_release/qc_flags.py`, `figures/scripts/plot_fig8_variable_distribution.py` |
| **Sect. 5.1.1** Structural consistency | Assess whether station consolidation and time-series integration conform to the implemented rules and examine source-station overlap within released stations | `validate/s13_validate_hydrological_clustering.py`, `validate/s12_analyze_cluster_quality_order.py` |
| **Sect. 5.1.2** Main–satellite SSC comparison | Link satellite-derived locations to main-component stations and perform temporal-support-aware SSC comparisons | `s5b_link_satellite_to_main_clusters_v2.py`, `validate/s11_satellite_insitu_validation_from_s5b_master.py`, `validate/satellite_insitu_validation_from_release.py`, `figures/scripts/plot_fig9_new.py`, `figures/scripts/plot_fig9_s5b_4x3_grid.py` |
| **Sect. 5.2** Sources of uncertainty | Provide sensitivity and diagnostic analyses for basin matching, station consolidation, temporal support, cross-source overlap, and derived sediment variables | `validate/s15_basin_matching_threshold_sensitivity.py`, `validate/s16_merging_threshold_sensivity_release.py`, `validate/s18_validate_cross_resolution_consistency.py`, `validate/s19_cross_source_overlap_intermidiate.py`, `validate/validate_derived_SSL.py` |
| **Sect. 5.3** Dataset limitations and recommended interpretation | Interpret the released products using the statistics and validation results; this section does not correspond to a single executable script | `stats_release/*`, `validate/*` |
| **Sect. 5.4** Demonstration of use in the Amazon Basin | Demonstrate extraction of reference observations and evaluation of modelled Q, SSC, and SSL | `validate/validate_model_with_sed_reference.py`, `figures/scripts/plot_fig10_validate_model_with_sed_reference.py` |

## Notes on the workflow

The code is organized by function rather than by manuscript section number. Scripts beginning with `s1`–`s9` form the main integration and release workflow, `stats_release/` reproduces the release-level statistics used mainly in Sect. 4, `validate/` contains the structural checks, sensitivity analyses, and comparison workflows used in Sect. 5, and `figures/scripts/` contains the scripts used to generate the corresponding manuscript and supplementary figures.

Sect. 3.4 is therefore represented by several sequential processing stages rather than a single script. Similarly, Sect. 5.2 draws on multiple diagnostics because the manuscript discusses uncertainty arising from different parts of the workflow. Sect. 5.3 is primarily an interpretation section and consequently has no dedicated standalone program; its statements are supported by the release statistics and validation outputs listed above.

The main pipeline can be coordinated with `run_s1_s8_basin_pipeline.py`, while the final station-facing public schema is produced by `s9_public_station_names.py`. Paths and computational settings should be configured for the local environment before reproducing the workflow.
