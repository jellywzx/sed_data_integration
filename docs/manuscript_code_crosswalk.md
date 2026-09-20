# Submitted ESSD manuscript ↔ code/release crosswalk

This document records the reproducibility contract for the ESSD manuscript submitted in September 2026 and the v1.0.0 sediment-reference release. It is intended to prevent later development changes on `master` from silently changing the meaning of manuscript numbers.

The working rule is:

1. Release statistics are authoritative for release counts and manuscript Tables 5–7.
2. Intermediate validation outputs are diagnostics unless explicitly named below.
3. A code condition that is stricter than the manuscript but does not change the v1.0.0 result is retained for provenance and documented with an equivalence check rather than silently removed.
4. Any future algorithm change that changes a submitted result requires a versioned rerun and corresponding manuscript revision; it must not be presented as the original v1.0.0 result.

## Numerical contract

The machine-readable submitted-manuscript counts are stored in:

```text
config/manuscript_expected_v1.0.0.json
```

They freeze the core matrix counts:

| Resolution | Stations | Records |
| --- | ---: | ---: |
| Daily | 7,087 | 2,993,390 |
| Monthly | 17 | 3,263 |
| Annual | 31 | 468 |
| All main matrices | 7,135 unique stations | 2,997,121 |

The submitted manuscript also describes 17 contributing main datasets.

Run the publication audit with the submitted-manuscript contract explicitly:

```bash
python tools/audit_release_for_zenodo.py \
  --expected-version 1.0.0 \
  --expected-stats config/manuscript_expected_v1.0.0.json
```

Do not replace this contract in place when preparing a later data release. Create a new versioned expected-statistics file instead.

## Crosswalk

| Manuscript item | Authoritative code / input | Manuscript-facing output or check | Current status / rule |
| --- | --- | --- | --- |
| Sect. 3.2 basin matching | `basin_tracer.py`, `basin_policy.py`, `s4_basin_trace_watch.py` | s4 basin-match table and `validate/s15_basin_matching_threshold_sensitivity.py` outputs | Production baseline uses the legacy 1 degree candidate bbox and 120 km score normalization. Fixed-km search modes in s15 are sensitivity scenarios, not a silent replacement of the production rule. |
| Sect. 3.4 station consolidation | `s5_basin_merge.py`, `basin_station_merge.py` | `s5_basin_clustered_stations.csv` | Effective manuscript rule is same resolved MERIT reach plus complete-linkage pairwise distance <= 1000 m. Production code additionally retains upstream-area symmetric relative error <= 0.10. |
| Sect. 3.4 upstream-area safeguard | `validate/s14_validate_upstream_area_merge_equivalence.py` | `docs/reports/upstream_area_merge_equivalence_v1.0.0.md` after rerun | Treat the safeguard as non-binding only if the audit reports zero changed assignments, pairwise Jaccard = 1, and adjusted Rand index = 1 for the release inputs. |
| Sect. 3.4 time-series arbitration | `s6_basin_merge_to_nc.py` | s6 master/matrix products and quality-order provenance | Candidate source stations are ranked by the proportion of final flag 0 values among non-missing Q/SSC/SSL values; one whole source record is selected per cluster/time rather than mixing variables across sources. |
| Sect. 4.2–4.4 release counts | `stats_release/spatial.py`, `stats_release/temporal.py`, `stats_release/source_contribution.py` | release statistics reports and tables | Release-level counts should be taken from final release products, not from S3/S4/S5 intermediate row counts. |
| Table 5 | `stats_release_to_manu/manuscript_tables_5_7.py` using spatial + temporal statistics | `stats_release_to_manu/docs/tables/table_manuscript_table5.csv` by default | Authoritative manuscript table builder. Output path is portable through `--out-dir`. |
| Table 6 | same builder using `stats_release/variable_summary` | `table_manuscript_table6.csv` | Distribution statistics use the flag 0–1 analysis-ready summaries; non-missing counts use the matrix record denominator. |
| Table 7 | same builder using `table_qc_matrix_final_flags_by_resolution.csv` | `table_manuscript_table7.csv` | Must use pooled daily/monthly/annual matrix QC counts. Do not substitute the master long-table QC denominator. |
| Sect. 5.1.1 structural consistency | `validate/s13_validate_hydrological_clustering.py`, `validate/s12_analyze_cluster_quality_order.py`, final release catalogs | structural-validation diagnostics | The manuscript's 7,925 source-station assessment population is a pipeline/candidate population, not the final `source_station_catalog.csv` population. Current release statistics report 7,469 source stations and 7,135 final stations; preserve these population labels when reporting or revising text. |
| Sect. 5.1.2 satellite linkage | `s5b_link_satellite_to_main_clusters_v2.py` | s5b linkage tables | Same-reach links are prioritized. Fallback implementation additionally uses Pfaf-region/network-connectivity details and a finite topology-hop search; these implementation details must be preserved or explicitly revalidated before changing manuscript wording. |
| Sect. 5.1.2 / Fig. 9 | `validate/s11_satellite_insitu_validation_from_s5b_master.py`, `figures/scripts/plot_fig9_new.py` | S11 pair/metric outputs and submitted six-panel Fig. 9 | The submitted validation is not regenerated from the old S10/S11 promotion logic in `tools/sync_stats_release_to_manuscript_assets.py`. That legacy promotion is opt-in only. |
| Fig. 9 QC policy | S11 pair-selection/aggregation code | pair-table `satellite_flag` and `insitu_flag` fields | The submitted S11 workflow does not impose a global flag-0/1-only filter. Do not add such a filter to v1.0.0 reproduction without rerunning metrics and updating the manuscript. |
| Merge-distance sensitivity | `validate/s16_merging_threshold_sensivity_release.py` | s16 sensitivity report | The 1000 m release projection gate must reproduce 7,135 unique released stations: 7,087 daily, 17 monthly, and 31 annual. |
| Satellite source family | `source_family.py` | release/source-contribution statistics | Satellite sources are RiverSed/RivSed, GSED, and Dethier. `Shashi_Jianli` is an in-situ source and must not be described as satellite-derived. |

## Manuscript-facing asset policy

`tools/sync_stats_release_to_manuscript_assets.py` now builds release-derived tables, figures, and constants without importing historical S10/S11 validation summaries by default. The old validation promotion can be requested only with:

```bash
python tools/sync_stats_release_to_manuscript_assets.py \
  --include-legacy-validation-assets
```

That option is for historical comparison and must not be used as the authoritative source of the submitted Sect. 5.1 validation or Fig. 9 numbers.

For Tables 5–7, use:

```bash
python stats_release_to_manu/manuscript_tables_5_7.py
```

or redirect its output portably:

```bash
python stats_release_to_manu/manuscript_tables_5_7.py \
  --out-dir /path/to/manuscript_table_outputs
```

## Change policy after submission

Safe post-submission changes are documentation fixes, portability fixes, additional audits, and tests that leave the v1.0.0 release partition and published values unchanged.

If a change alters cluster membership, station counts, temporal arbitration, QC inclusion, satellite links, Fig. 9 pair counts/metrics, or manuscript table values, preserve the submitted implementation and create a versioned revision workflow instead of silently redefining v1.0.0.
