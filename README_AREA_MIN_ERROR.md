# Experimental S4: upstream-area minimum relative error

Repository: `jellywzx/sed_data_integration`
Branch: `experiment/s4-area-min-error-20261023` (based on the 2026-10-23 master snapshot `027b74ef0b2bf337a01f9e8b8788d0d0d00c5ab0`).

This bundle consists of **new files only**. It does not replace the mainline
`basin_tracer.py`, `basin_policy.py` or `s4_basin_trace_watch.py`.

## 1. Definition

For a station with a positive, finite, externally sourced `reported_area` (km2):

```
area_rel_error_i = abs(MERIT_uparea_i - reported_area) / reported_area
selected_COMID  = argmin_i(area_rel_error_i)
```

Only exact equality of area error is broken by distance, then by COMID.
`distance_m` **does not appear in the primary score**.

The candidate pool is unchanged from `UpstreamBasinTracer`:
spatial-index bbox and distance < 120 km. The station-to-reach **1 km acceptance
cap** is removed only in scenario D and only when relative area error <= 10%.
An unmatched station or a station with area error > 10% remains unresolved.

The S4 `area_error` field remains the repository's legacy SIGNED LOG10 ratio.
A new `experimental_area_rel_error` contains the absolute relative error.

When S3 has valid `reported_area`, it takes precedence. The external evidence
CSV is used only to fill missing S3 values. Existing GSED/RiverSed reach-hint
processing is untouched and their original S4 rows are preserved.

## 2. Installation into a new experiment worktree

Copy the ZIP to the HPC server. In the new branch's worktree:

```bash
cd /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/sed_data_integration_area_min_error

git branch --show-current
git status --short

# Extract the ZIP into the current worktree. -n never overwrites an existing file.
unzip -n /path/to/sed_data_area_min_error_20261023.zip -d .
```

Expected branch:

```
experiment/s4-area-min-error-20261023
```

## 3. Tests and full valid-area station run

```bash
python -m pytest -q test/test_area_min_error_matching.py
python validate/s20_area_min_error_s4.py --dry-run
python validate/s20_area_min_error_s4.py --retrace-geometry
```

All ordinary inputs and output paths default to the user's established HPC
layout. Override using `--s3-csv`, `--s4-csv`, `--merit-dir`,
`--manual-areas`, `--out-dir` as needed.

## 4. Main A/B/C/D comparisons

- **A:** Production S4 COMID and production acceptance.
- **B:** Area-only COMID; production 300 m / 1 km acceptance.
- **C:** Production S4 COMID; independent-area acceptance without 1 km cap.
- **D:** Area-only COMID and independent-area acceptance without 1 km cap.

D is the requested new rule. The experiment still flags reviews when a station
is outside its local polygon, far away, ambiguous, or its area is unverified.

## 5. Files written (never under the production S4 output directory)

Under:

```
/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/area_min_error_20261023/
```

Generated files:

```
sandbox/scripts_basin_test/output/s4_upstream_basins.csv
reports/station_comparison.csv
reports/candidate_top10.csv
reports/scenario_summary.csv
reports/review_required.csv
reports/experiment_report.md
reports/provenance.json
```

The S4-compatible CSV contains **ALL** baseline S4 rows, with only the eligible
rerun rows changed. Unchanged source rows preserve station IDs and coordinates.
No full S4 GPKG is produced; this experiment is not release-ready.

No outputs are overwritten unless `--overwrite` is specified.

## 6. Global-scale test

Start with a cap to keep runtime manageable:

```bash
python validate/s20_area_min_error_s4.py \
  --max-stations 50 \
  --out-dir /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/area_min_error_20261023/global_smoke
```

Then increase the cap or use `--max-stations 0` for the full set. Do **not**
turn on `--retrace-geometry` for all global stations before reviewing the
runtime and memory requirements. The selected candidates are still ranked
using *all* nearby MERIT reaches, not S16's legacy-score top-five.

## 7. Optional isolated S5 comparison

Execute this only after verifying the S4 diagnostics:

```bash
PROD=/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output
EXP=/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/area_min_error_20261023/sandbox/scripts_basin_test/output

python s5_basin_merge.py \
  --s3-csv "$PROD/s3_collected_stations.csv" \
  --basin-csv "$EXP/s4_upstream_basins.csv" \
  --out "$EXP/s5_basin_clustered_stations.csv" \
  --report "$EXP/s5_basin_cluster_report.csv"
```

The S5 merge constraints are not modified. The new experiment's source CSV
retains S3 station keys/IDs; no S3 rerun is needed. S5 writes only to the
experimental sandbox when these explicit arguments are used.

## 8. Scientific and technical limitations

1. The independent-area provenance and units require verification. The Amazon
   manual values have `evidence_verified=0` and are **not** ready for release.
2. Exact minimum area error may prefer a reach many kilometres away. Such
   cases are reported for manual review, not presented as validated matches.
3. Adjacent reaches with nearly identical upstream areas may have ambiguous
   station localisation. No automatic branch-confluence rejection is performed.
4. When a newly chosen COMID is not polygon-retraced, geometry flags are
   recorded as unknown rather than false; trial S5 may still consume the
   provisional experimental `resolved` statuses.
5. Running an experiment on a subset changes only that subset; all other
   original S4 rows are copied unchanged. This is intentional.
6. `--overwrite` is restricted to the named experiment outputs; never point
   `--out-dir` at a production folder.
7. End-to-end performance and real MERIT tracing have not yet been validated
   on the HPC filesystem. Unit and synthetic integration tests are provided.
8. This ZIP has not been pushed or committed to GitHub.
