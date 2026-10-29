# S4 experimental: MERIT upstream area only

This package **adds** `s4_area_only_upstream_area.py` and `test/test_s4_area_only_upstream_area.py` to the *existing* Git worktree `experiment/s4-area-min-error-20261023`. It does **not** replace any production S4/S5 file, and does not overwrite the earlier S20/E experiments.

## Definition

For each eligible S3 station with valid, independently supplied `reported_area` in km²:

```text
E = |MERIT.uparea - reported_area| / reported_area
best_COMID = argmin(E) among the existing nearby MERIT candidates
```

- No distance term or distance tiebreaker, no 1 km rejection, no `point_in_local`, no `point_in_basin`, no upstream polygon or BFS.
- COMID is a deterministic ID of the minimum-error candidate; exact ties use numeric COMID for stable ordering.
- Existing `UpstreamBasinTracer.get_nearby_candidate_reaches()` still **limits candidate discovery** to the original +/-1 degree bounding box, Pfaf regions enclosing the station, and **less than 120 km** from the station. Removing this spatial search would make it trivial to find a matching river elsewhere on Earth and would invalidate station matching.
- `MERIT.uparea` is a **precomputed upstream area attribute**, not a newly measured upstream polygon area and **not** CoLM 0.25-degree routing upstream area.
- The `--area-threshold` value (default 0.10) **labels** matches above or below the tolerance but does **not** change the selected COMID or suppress output.
- Valid S3 `reported_area` has priority. For the Amazon three, `config/amazon_area_evidence.csv` is used as an optional fallback only when S3 lacks area. The fallback values are still **unverified**, not suitable as published independent evidence.
- RiverSed and GSED remain excluded from this gauge-point analysis due to the special reach-product matching in production S4.
- Stations without valid area are not selected; those stations are not automatically matched by another policy in this experiment.

## Install (at the HPC, from the project Git worktree)

```bash
cd /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/sed_data_integration_area_min_error
unzip -n /path/to/sed_data_s4_area_only_20261023.zip -d .
git status --short
python -m pytest -q test/test_s4_area_only_upstream_area.py
```

Two new code/test files plus this README are installed. The repository's existing `config/amazon_area_evidence.csv` is reused.

## Run all gauge stations with valid `reported_area`

```bash
python s4_area_only_upstream_area.py --max-stations 20 \
  --out-dir /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/area_min_error_20261023/s4_area_only_sample20

python s4_area_only_upstream_area.py \
  --out-dir /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/experiments/area_min_error_20261023/s4_area_only_global
```

### Outputs

Default path:

```text
Output_r/experiments/area_min_error_20261023/s4_area_only/
  s4_area_only_upstream_area.csv     # desired station-to-upstream-area results
  s4_area_only_candidate_top10.csv   # top candidates ranked only by area error
  s4_area_only_summary.csv           # counts and distribution by source
  s4_area_only_run.json              # paths, hashes and assumptions
```

Key columns: `reported_area_km2`, `uparea_merit_km2`, `absolute_area_error_km2`, `area_rel_error`, `area_error_percent`, `basin_id` and `distance_m_diagnostic` (not used in ranking). Values with `area_match_status=above_area_threshold` are still output for assessment; do not call them reliable.

This is a **diagnostic upstream-area table**. It is **not S5/S6-compatible production `s4_upstream_basins.csv`**, because no basin polygons were generated or verified. It does not itself extract CoLM 0.25-degree model unitcatchment indices or upstream areas. To evaluate model 0.25-degree routing, that mapping must be handled separately.

Use `--overwrite` only to overwrite the four files produced by this experiment in the same output directory. The code refuses output paths outside `Output_r/experiments` and never writes into the production S3/S4 tree.
