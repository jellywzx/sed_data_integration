# Sediment Reference Dataset Integration and Assessment Workflow

This repository contains the integration, release-characterization, validation, sensitivity-analysis, and model-evaluation workflow corresponding to **Sections 3.4–5.4** of the manuscript:

> *A harmonized global station-reference dataset of river discharge, suspended sediment concentration, and suspended sediment load*

The repository assumes that source-level harmonization, georeferencing/basin matching, and quality-control products required by the integration workflow have already been prepared. This README therefore focuses only on the workflow beginning with **temporal screening, station consolidation, and time-series integration (Sect. 3.4)** and continuing through **dataset characterization and product assessment (Sects. 4–5.4)**.

---

## Manuscript-to-code map

| Manuscript section | Main purpose | Main scripts/modules |
| --- | --- | --- |
| **Sect. 3.4** Temporal screening, station consolidation, and time-series integration | Assign release temporal classes, consolidate hydrologically comparable source stations, and resolve overlapping source records | `s1_verify_time_resolution.py`, `time_resolution.py`, `s2_reorganize_qc_by_resolution.py`, `s5_basin_merge.py`, `basin_station_merge.py`, `s6_basin_merge_to_nc.py` |
| **Sect. 4.1** Dataset structure and products | Export daily/monthly/annual matrices, climatology and satellite products, catalogues, spatial sidecars, and the final public package | `s6_*`, `s7_*`, `s8_publish_reference_dataset.py`, `s8_publish_minimal_release_package.py`, `s9_public_station_names.py` |
| **Sect. 4.2** Source contributions | Quantify source-level station and record contributions | `stats_release/source_contribution.py`, `stats_release/source_dataset_layers.py`, `figures/scripts/plot_fig5_combined_source_contribution_direct_release.py` |
| **Sect. 4.3** Spatial coverage and basin attributes | Summarize spatial coverage, resolved/unresolved basin status, and upstream-area attributes | `stats_release/spatial.py`, `stats_release/basin_diagnostics.py`, `figures/scripts/plot_fig6_composite_spatial_coverage_manu_order.py`, `figures/scripts/plot_figs1_source_map_insitu_clim_sat.py` |
| **Sect. 4.4** Temporal coverage and record availability | Summarize active stations, record counts, complete Q–SSC–SSL triplets, and source contributions through time | `stats_release/temporal.py`, `figures/scripts/plot_fig7_active_records_panels.py`, `figures/scripts/plot_figs2_annual_matrix_by_source_by_resolution.py` |
| **Sect. 4.5** Variable availability, distributions, and quality flags | Summarize Q, SSC, SSL availability, distributions, and final quality-flag composition | `stats_release/variable_summary.py`, `stats_release/qc_flags.py`, `figures/scripts/plot_fig8_variable_distribution.py` |
| **Sect. 5.1.1** Structural consistency | Test whether station consolidation and time-series integration follow the implemented rules | `validate/s13_validate_hydrological_clustering.py`, `validate/s12_analyze_cluster_quality_order.py` |
| **Sect. 5.1.2** Main–satellite SSC comparison | Link satellite-derived locations to main stations and perform temporal-support-aware SSC comparison | `s5b_link_satellite_to_main_clusters_v2.py`, `validate/s11_satellite_insitu_validation_from_s5b_master.py`, `validate/satellite_insitu_validation_from_release.py`, Fig. 9 scripts |
| **Sect. 5.2** Sources of uncertainty | Supporting diagnostics for matching, consolidation, temporal support, source overlap, and derived sediment variables | `validate/s15_basin_matching_threshold_sensitivity.py`, `validate/s16_merging_threshold_sensivity_release.py`, `validate/s18_validate_cross_resolution_consistency.py`, `validate/s19_cross_source_overlap_intermidiate.py`, `validate/validate_derived_SSL.py` |
| **Sect. 5.3** Limitations and recommended interpretation | Interpretation is based on release statistics and validation outputs rather than a single executable stage | `stats_release/*`, `validate/*` |
| **Sect. 5.4** Demonstration in the Amazon Basin | Evaluate model Q, SSC, and SSL against the main station-reference component | `validate/validate_model_with_sed_reference.py`, `figures/scripts/plot_fig10_validate_model_with_sed_reference.py` |

---

# 3.4 Temporal screening, station consolidation, and time-series integration

The integration workflow begins by assigning harmonized source observations to release temporal-support classes and then constructing the main station-reference component.

## Temporal screening

```text
s1_verify_time_resolution.py
        |
        v
s2_reorganize_qc_by_resolution.py
```

The temporal classes follow source-reported temporal support:

- **daily**: source-reported daily observations, discrete observations assigned to individual dates, and daily values aggregated from higher-frequency measurements;
- **monthly**: observations explicitly reported at monthly temporal support;
- **annual**: observations explicitly reported at annual temporal support;
- **climatology**: climatological or multi-year mean observations retained separately from the main time-resolved products.

`time_resolution.py` contains shared temporal-classification helpers used by S1 and S2.

## Station consolidation

```text
s5_basin_merge.py
basin_station_merge.py
```

Station consolidation groups source stations that represent hydrologically comparable locations into release-level stations. Following the manuscript workflow, resolved source stations are consolidated when they are associated with the same MERIT-Basins reach and satisfy the configured pairwise-distance criterion. The baseline release uses a maximum pairwise distance of **1000 m**.

Unresolved source stations are not merged on the basis of uncertain basin assignments. Usable unresolved locations are retained as independent stations when valid station/time information and at least one sediment variable (SSC or SSL) are available.

The current `basin_station_merge.py` implementation also exposes a pairwise upstream-area relative-error control (`max_upstream_rel_error`, default `0.10`). This code-level setting should be kept synchronized with the consolidation criteria documented for the release.

The release-oriented sensitivity analysis for consolidation distance is implemented in:

```text
validate/s16_merging_threshold_sensivity_release.py
```

with default tested distances of 500, 750, 1000, 1250, and 1500 m.

## Time-series integration

```text
s6_basin_merge_to_nc.py
```

Time-series integration is performed within each release station and temporal-resolution class.

The script:

1. aligns source records using the union of available timestamps;
2. retains records with at least one eligible sediment observation (SSC or SSL);
3. ranks contributing source stations by the proportion of good-quality flags (`flag = 0`) among non-missing Q, SSC, and SSL flags over their available time series;
4. selects the record from the highest-ranked source station available at each timestamp;
5. writes the selected record as a whole, preserving its Q, SSC, SSL values and corresponding flags.

Values from different source records are **not combined within one output record**, preventing artificial Q–SSC–SSL triplets that were not observed together.

The record-level source identifier written to the released matrices allows the selected observation to be traced back to its contributing source station.

---

# 4. Dataset description and characteristics

## 4.1 Dataset structure and products

The main station-reference component is exported as separate daily, monthly, and annual station-by-time NetCDF products. Climatology and satellite-derived observations remain separate auxiliary products.

### Product-generation scripts

```text
s6_basin_merge_to_nc.py
s6_export_resolution_matrix_ncs.py
s6_export_daily_matrix_nc.py
s6_export_monthly_matrix_nc.py
s6_export_annual_matrix_nc.py
s6_export_climatology_to_nc.py
s6_export_satellite_validation_to_nc.py
```

Spatial and catalogue sidecars are generated by:

```text
s7_export_cluster_shp.py
s7_export_source_station_shp.py
s7_export_cluster_basin_shp.py
```

Release assembly is performed by:

```text
s8_publish_reference_dataset.py
s8_publish_minimal_release_package.py
s9_public_station_names.py
```

S9 is the final station-facing public-schema conversion stage.

### Final public products

```text
station_catalog.csv
source_station_catalog.csv
source_dataset_catalog.csv
climatology_catalog.csv
satellite_catalog.csv

sed_reference_timeseries_daily.nc
sed_reference_timeseries_monthly.nc
sed_reference_timeseries_annual.nc
sed_reference_climatology.nc
sed_reference_satellite.nc
```

The main products contain Q, SSC, SSL, their quality flags, station identifiers, basin attributes where available, and record-level source-station provenance. The climatology and satellite-derived products are distributed separately because their temporal and spatial support differs from gauge-based time-resolved observations.

## 4.2 Source contributions

Release-facing source-contribution statistics are generated from the assembled release by:

```bash
python -m stats_release.source_contribution
python -m stats_release.source_dataset_layers
```

The manuscript release contains:

| Component | Stations | Records | Temporal coverage |
| --- | ---: | ---: | --- |
| Main station-reference | 7,135 unique stations | 2,997,121 | 1912–2025 |
| Climatology auxiliary | 1,361 | 1,361 | long-term/climatological; reported source coverage includes 1884–2017 |
| Satellite-derived auxiliary | 38,550 | 16,478,276 | 1984–2020 |

The corresponding manuscript figure is generated with:

```text
figures/scripts/plot_fig5_combined_source_contribution_direct_release.py
```

## 4.3 Spatial coverage and basin attributes

Spatial statistics and basin diagnostics are generated by:

```bash
python -m stats_release.spatial
python -m stats_release.basin_diagnostics
```

These modules summarize:

- station coverage by resolution, source, country, and region;
- resolved and unresolved basin assignments;
- upstream-area distributions for resolved stations;
- satellite-derived spatial coverage;
- spatial source contributions and supporting geospatial layers.

For the manuscript release, the main component contains **7,135 unique stations**, of which **5,530 (77.51%)** have resolved basin assignments and **1,605 (22.49%)** remain unresolved.

Figure scripts:

```text
figures/scripts/plot_fig6_composite_spatial_coverage_manu_order.py
figures/scripts/plot_figs1_source_map_insitu_clim_sat.py
```

## 4.4 Temporal coverage and record availability

Temporal statistics are generated by:

```bash
python -m stats_release.temporal
```

The main products are:

| Product | Stations | Records | Temporal span |
| --- | ---: | ---: | --- |
| Daily | 7,087 | 2,993,390 | 1948–2025 |
| Monthly | 17 | 3,263 | 1938–2000 |
| Annual | 31 | 468 | 1912–2020 |
| All main products | 7,135 unique stations | 2,997,121 | 1912–2025 |

The temporal analysis includes yearly active-station counts, record counts, complete Q–SSC–SSL triplet fractions, and source contributions through time.

Figure scripts:

```text
figures/scripts/plot_fig7_active_records_panels.py
figures/scripts/plot_figs2_annual_matrix_by_source_by_resolution.py
```

Daily, monthly, and annual products have different temporal support and should not be treated as interchangeable time series.

## 4.5 Variable availability, distributions, and quality flags

Variable and flag statistics are generated by:

```bash
python -m stats_release.variable_summary
python -m stats_release.qc_flags
```

The release statistics characterize:

- non-missing Q, SSC, and SSL values by temporal resolution;
- analysis-ready distributions and percentile summaries;
- proportions of good, derived, suspect/bad, and missing values;
- differences in variable distributions among daily, monthly, and annual products.

For the main component, analysis-ready values (`flag 0–1`) account for **90.9% of Q**, **96.0% of SSC**, and **88.4% of SSL** values in the manuscript release.

The distribution figure is generated with:

```text
figures/scripts/plot_fig8_variable_distribution.py
```

---

# 5. Product assessment, uncertainty, limitations, and demonstration of use

## 5.1.1 Structural consistency of station consolidation and time-series integration

Structural validation is implemented by:

```bash
python validate/s13_validate_hydrological_clustering.py
python validate/s12_analyze_cluster_quality_order.py
```

The assessment checks whether the integrated structure follows the station-consolidation and record-selection logic, including station membership, within-station spatial consistency, source-candidate composition, and temporal overlap among contributing source stations.

For the manuscript release, **7,925 source stations** are grouped into **7,135 release stations**. Of these, **6,748 (94.6%)** are single-source stations and **387 (5.4%)** are multi-source stations. Among the 387 multi-source stations, 349 are fully overlapping at the year level, four are partially overlapping, and 34 contain non-overlapping source time series.

This is a **structural consistency assessment**: it tests conformity with the implemented integration rules rather than providing an independent estimate of observational accuracy.

## 5.1.2 Comparison of main station-reference and satellite-derived SSC

Spatial linkage is implemented by:

```text
s5b_link_satellite_to_main_clusters_v2.py
```

The linkage prioritizes the same MERIT-Basins reach. When a same-reach main station is unavailable, the script evaluates connected nearby reaches using river-network topology, upstream-area agreement, and bidirectional point-to-reach distance. Ambiguous competing candidates are not forced into a unique high-confidence link.

Satellite/station temporal pairing and SSC comparison are implemented by:

```text
validate/s11_satellite_insitu_validation_from_s5b_master.py
validate/satellite_insitu_validation_from_release.py
```

The comparison respects temporal support rather than pairing all products solely by encoded timestamps. Examples include:

- exact-date daily satellite versus daily station observations, with ±1 d and ±2 d sensitivity windows;
- monthly satellite products versus eligible daily station observations aggregated within the same calendar month;
- monthly satellite versus monthly station-reference observations paired by calendar month.

For monthly satellite versus daily station-reference comparisons, discrete daily observations are excluded and daily values are aggregated only when within-month coverage satisfies the implemented coverage requirement.

The representative manuscript comparison includes a large RivSed–USGS NWIS sample with 37,237 exact-date SSC pairs from 82 stations and additional GSED and Dethier comparisons with temporally compatible station records.

Figure scripts:

```text
figures/scripts/plot_fig9_new.py
figures/scripts/plot_fig9_s5b_4x3_grid.py
```

## 5.2 Sources of uncertainty

There is no single uncertainty-propagation script because the manuscript identifies uncertainty from several distinct stages. Supporting diagnostics include:

```text
validate/s15_basin_matching_threshold_sensitivity.py
validate/s16_merging_threshold_sensivity_release.py
validate/s18_validate_cross_resolution_consistency.py
validate/s19_cross_source_overlap_intermidiate.py
validate/validate_derived_SSL.py
```

These scripts help quantify or diagnose uncertainty associated with:

- basin/reach matching thresholds;
- station-consolidation distance choices;
- temporal-support differences among release products;
- overlapping observations from different sources;
- derived SSL and related variable propagation.

Other uncertainties discussed in the manuscript, including source measurement error, SSC sampling representativeness, differences between TSS and SSC, and spatial-support differences between satellite and gauge observations, originate in the underlying observations and cannot be represented by a single deterministic pipeline diagnostic.

## 5.3 Dataset limitations and recommended interpretation

The following interpretation rules should be applied when using the released products:

- Select the daily, monthly, or annual product according to the temporal support required by the analysis rather than directly combining different resolution classes.
- When aggregating daily observations to coarser scales, account for observation coverage, especially for discrete or sparse records.
- Use climatology and satellite-derived products as complementary long-term or spatial context rather than substitutes for time-resolved station-reference observations.
- Treat quality flags as processing/screening indicators rather than uniform quantitative uncertainty estimates.
- Values with flags `0–1` are considered analysis-ready; derived values (`flag 1`), especially derived SSL, should remain distinguishable from directly reported observations.
- Suspect values (`flag 2`) may be retained for sensitivity analysis; bad values (`flag 3`) should normally be excluded.
- For provenance-sensitive analyses, use `station_uid`, `resolution`, and `selected_source_station_uid` together with the release catalogues.
- Basin-dependent analyses should generally use resolved stations because unresolved stations do not have confirmed river-reach or upstream-basin assignments.

The release is sediment-oriented rather than a complete discharge archive: main-component records are retained when SSC or SSL is available, and the dataset represents suspended sediment rather than total sediment transport.

## 5.4 Demonstration of use in the Amazon Basin

The model-evaluation workflow is implemented by:

```text
validate/validate_model_with_sed_reference.py
```

The script discovers usable reference stations, extracts Q/SSC/SSL observations from the main station-reference products, matches stations to model output, aligns temporally overlapping records, and calculates station-variable evaluation statistics.

The manuscript demonstrates this workflow using CoLM2024 river-sediment simulations in the Amazon Basin. Six stations with overlapping simulations and observations are retained in the example. The purpose is to demonstrate how co-located Q, SSC, and SSL observations can support model benchmarking and diagnosis; it is **not** presented as a formal assessment of CoLM2024 performance.

The manuscript Figure 10 is generated with:

```text
figures/scripts/plot_fig10_validate_model_with_sed_reference.py
```

The same evaluation approach can be adapted to hydrological, land-surface, river-routing, erosion, sediment-transport, and Earth system model outputs.

---

## Running the relevant workflow

Create the supplied environment:

```bash
conda env create -f environment.yml
conda activate sed-reference-basin
```

The unified runner can execute the dependency-aware integration and release stages:

```bash
python run_s1_s8_basin_pipeline.py --help
```

Preview a configured run:

```bash
python run_s1_s8_basin_pipeline.py \
  --config-file pipeline_config.yaml \
  --dry-run \
  --yes
```

Run the release-facing statistics suite used for Sect. 4:

```bash
python -m stats_release.run_all_release_stats
```

Individual Sect. 5 validation and sensitivity scripts can then be run from `validate/` as listed above.
