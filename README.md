# Sediment Reference Dataset Integration and Assessment Workflow

This repository contains the workflow corresponding to **Sections 3.4–5.4** of the manuscript:

> *A harmonized global station-reference dataset of river discharge, suspended sediment concentration, and suspended sediment load*

The repository focuses on the stages after source-level preprocessing: temporal screening, station consolidation, time-series integration, release generation and characterization, structural and cross-product assessment, uncertainty diagnostics, and the model-evaluation demonstration. Source-specific data acquisition, harmonization, georeferencing, and quality-control procedures described earlier in the manuscript are outside the scope of this README.

## Environment

The workflow is developed for **Python 3.10**. The recommended environment is provided in `environment.yml` and includes the scientific, NetCDF, geospatial, plotting, and utility packages required by the pipeline.

```bash
conda env create -f environment.yml
conda activate sed-reference-basin
```

Alternatively, the Python dependencies can be installed from:

```bash
pip install -r requirements.txt
```

The main dependencies include `numpy`, `pandas`, `xarray`, `scipy`, `netCDF4`, `h5netcdf`, `geopandas`, `pyproj`, `shapely`, `cartopy`, `matplotlib`, and `PyYAML`.

S4 and S6 can either be submitted to an **LSF** computing environment or run locally. For systems without LSF, use the `--local-s4` and `--local-s6` options described below.

## Required inputs

The integration pipeline assumes that the source-specific preprocessing described before Sect. 3.4 has already been completed. The principal inputs are:

- **Source-level QC NetCDF files.** These files should contain the harmonized station metadata, time information, Q/SSC/SSL variables, and associated quality-control fields required by the integration scripts. S1 searches the parent workspace for NetCDF files under resolution/source-specific `qc/` directories, for example:

  ```text
  <workspace>/daily/<dataset>/.../qc/*.nc
  <workspace>/monthly/<dataset>/.../qc/*.nc
  <workspace>/annually_climatology/<dataset>/.../qc/*.nc
  <workspace>/sed_data_integration/
  ```

  The repository is expected to be located inside this workspace so that the existing path logic can discover the source QC files.

- **MERIT Hydro / MERIT-Basins data.** The river-network and basin data are required by the basin-tracing and satellite-linkage stages. Set the local path in `pipeline_config.yaml`:

  ```yaml
  cli:
    merit_dir: "/path/to/MERIT_Hydro_v07_Basins_v01_bugfix1"
  ```

  or provide it at runtime with `--merit-dir`.

- **Optional geographic boundary data.** If geographic metadata enrichment is required, an administrative boundary file can be supplied through the `SED_GEO_BOUNDARY_*` settings in `pipeline_config.yaml`.

- **Model output for Sect. 5.4.** Model NetCDF files are required only for the Amazon Basin demonstration and are not required to build the sediment reference dataset itself.

## Running the integration pipeline

`run_s1_s8_basin_pipeline.py` is the main dependency-aware runner. Despite its historical filename, the current runner supports stages **S1–S9**.

First review `pipeline_config.yaml` and replace machine-specific paths, especially `merit_dir`, with paths available on the local system. The planned commands can be checked without executing them:

```bash
python run_s1_s8_basin_pipeline.py \
  --config-file pipeline_config.yaml \
  --dry-run \
  --yes
```

To run the complete workflow locally:

```bash
python run_s1_s8_basin_pipeline.py \
  --config-file pipeline_config.yaml \
  --start-at s1 \
  --end-at s9 \
  --local-s4 \
  --local-s6 \
  --yes
```

On an LSF system, omit `--local-s4` and `--local-s6` after configuring the corresponding scheduler settings in `pipeline_config.yaml`.

Individual stages or a continuous subset can also be rerun, for example:

```bash
python run_s1_s8_basin_pipeline.py --steps s5,s6,s7
python run_s1_s8_basin_pipeline.py --start-at s5 --end-at s8
```

The runner coordinates the following sequence:

```text
S1  temporal-resolution verification
 -> S2  organization by temporal support
 -> S3  collection of station metadata
 -> S4  basin/reach tracing
 -> S5  station consolidation and satellite-main linkage
 -> S6  time-series integration and NetCDF generation
 -> S7  spatial/catalogue exports
 -> S8  release-package assembly
 -> S9  final station-facing public schema
```

S3–S4 provide basin/reach information required by the later station-consolidation workflow. The manuscript-to-code map below focuses on the scientific workflow from Sect. 3.4 onward.

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

Paths and computational settings are machine dependent. Users should review `pipeline_config.yaml` and run the pipeline in `--dry-run` mode before executing a full reproduction run.
