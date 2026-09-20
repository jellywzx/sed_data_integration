#!/usr/bin/env python3
"""Validate whether the 10% upstream-area merge safeguard is non-binding.

The submitted manuscript describes the effective station-consolidation rule as
same resolved MERIT-Basins reach plus complete-linkage pairwise station distance
<= 1000 m. The production s5 implementation additionally retains a conservative
upstream-area symmetric-relative-error safeguard (default <= 0.10).

This read-only validation reruns the s5 clustering twice on the same s3/s4
inputs:

1. production baseline: distance <= 1000 m and area error <= 0.10;
2. manuscript-effective comparison: distance <= 1000 m with the area threshold
   disabled by setting max_upstream_rel_error to infinity.

It then compares the complete station partition, not only the number of
clusters. If the current s5 output exists, the script also checks that the
production-baseline rerun exactly reproduces its station_id -> cluster_id map.

Exit codes
----------
0 : baseline and no-area partitions are exactly equivalent, and the optional
    current-s5 gate (when performed) passes.
2 : the upstream-area safeguard changes at least one station membership.
3 : the production-baseline rerun does not reproduce the supplied/current s5.
1 : invalid or missing inputs.

The report is intended as reproducibility evidence for release v1.0.0; the
script does not modify pipeline data products.
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from basin_station_merge import load_station_to_basin_cluster_map  # noqa: E402
from pipeline_paths import (  # noqa: E402
    S3_COLLECTED_CSV,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    get_output_r_root,
)
from sensitivity_merge_distance_s5 import (  # noqa: E402
    build_assignments,
    compare_generated_baseline_to_s5,
    normalize_station_table,
    partition_agreement,
)

PROJECT_ROOT = get_output_r_root(REPO_ROOT)

DEFAULT_DISTANCE_M = 1000.0
DEFAULT_BASELINE_AREA_ERROR = 0.10
DEFAULT_RELAXED_AREA_ERROR = math.inf
DEFAULT_UPSTREAM_AREA_COL = "uparea_merit"

DEFAULT_S3 = PROJECT_ROOT / S3_COLLECTED_CSV
DEFAULT_S4 = PROJECT_ROOT / S4_UPSTREAM_CSV
DEFAULT_S5 = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_REPORT = REPO_ROOT / "docs/reports/upstream_area_merge_equivalence_v1.0.0.md"
DEFAULT_CHANGED = (
    REPO_ROOT
    / "validate/output/s14_upstream_area_merge_equivalence/changed_station_assignments.csv"
)


def _build_mapping(
    s3: pd.DataFrame,
    s4_path: Path,
    distance_m: float,
    area_error: float,
    area_col: str,
):
    mapping, stats = load_station_to_basin_cluster_map(
        s4_path,
        station_df=s3,
        max_station_distance_m=float(distance_m),
        max_upstream_rel_error=float(area_error),
        upstream_area_col=area_col,
    )
    assignments = build_assignments(s3, mapping, float(distance_m))
    return assignments, stats


def _exact_mapping_comparison(
    baseline: pd.DataFrame,
    relaxed: pd.DataFrame,
) -> pd.DataFrame:
    metadata_cols = [
        column
        for column in [
            "station_key",
            "station_id",
            "source",
            "resolution",
            "observation_type",
            "lat",
            "lon",
        ]
        if column in baseline.columns
    ]
    left = baseline[metadata_cols + ["cluster_id"]].rename(
        columns={"cluster_id": "baseline_cluster_id"}
    )
    right = relaxed[["station_id", "cluster_id"]].rename(
        columns={"cluster_id": "no_area_cluster_id"}
    )
    joined = left.merge(right, on="station_id", how="outer", validate="one_to_one")
    joined["mapping_changed"] = (
        joined["baseline_cluster_id"] != joined["no_area_cluster_id"]
    )
    return joined


def _fmt(value, digits=6):
    if value is None:
        return "NA"
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return "NA"
        if np.isinf(value):
            return "disabled (infinity)"
        return ("{:." + str(digits) + "f}").format(float(value))
    return str(value)


def _write_report(
    path: Path,
    args,
    baseline: pd.DataFrame,
    relaxed: pd.DataFrame,
    agreement,
    mapping_table: pd.DataFrame,
    baseline_stats,
    relaxed_stats,
    s5_gate,
):
    changed_mapping = int(mapping_table["mapping_changed"].fillna(True).sum())
    membership_changed = int(
        agreement["n_stations_membership_changed_vs_baseline"]
    )
    exact_mapping = changed_mapping == 0 and len(baseline) == len(relaxed)
    equivalent = exact_mapping and membership_changed == 0

    gate_performed = bool(s5_gate["baseline_s5_check_performed"])
    gate_exact = s5_gate["baseline_s5_exact_match"]
    if gate_performed:
        gate_status = "PASS" if bool(gate_exact) else "FAIL"
    else:
        gate_status = "SKIPPED (s5 file not available)"

    result_status = "PASS" if equivalent else "FAIL"
    interpretation = (
        "For these inputs, the 10% upstream-area safeguard is non-binding: "
        "disabling it does not change any station-to-cluster assignment."
        if equivalent
        else
        "For these inputs, the upstream-area safeguard is binding and changes "
        "the station partition; it must not be described as non-binding."
    )

    lines = [
        "# Upstream-area merge safeguard equivalence check (release v1.0.0)",
        "",
        "This report compares the production s5 station-consolidation rule with",
        "the manuscript-effective rule while holding all other inputs and rules fixed.",
        "",
        "## Inputs",
        "",
        "- s3 station table: {}".format(args.s3_csv),
        "- s4 basin table: {}".format(args.s4_csv),
        "- current s5 table: {}".format(args.s5_csv),
        "",
        "## Rules compared",
        "",
        "| Setting | Production baseline | Manuscript-effective comparison |",
        "| --- | ---: | ---: |",
        "| Same resolved MERIT reach | required | required |",
        "| Complete-linkage pairwise distance | <= {:.0f} m | <= {:.0f} m |".format(
            args.distance_m, args.distance_m
        ),
        "| Upstream-area symmetric relative error | <= {:.2f} | disabled |".format(
            args.baseline_area_error
        ),
        "",
        "The comparison disables only the upstream-area rejection by setting",
        "max_upstream_rel_error to infinity. Satellite observations remain singleton",
        "clusters exactly as in the production s5 implementation.",
        "",
        "## Reproduction gate",
        "",
        "| Check | Result |",
        "| --- | --- |",
        "| Production-baseline rerun vs current s5 | {} |".format(gate_status),
        "| s5 cluster-id mismatches | {} |".format(
            _fmt(s5_gate["baseline_s5_cluster_id_mismatch_count"], 0)
            if gate_performed
            else "NA"
        ),
        "",
        "## Equivalence results",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        "| Stations compared | {:,} |".format(len(mapping_table)),
        "| Baseline clusters | {:,} |".format(baseline["cluster_id"].nunique()),
        "| No-area clusters | {:,} |".format(relaxed["cluster_id"].nunique()),
        "| Exact station_id -> cluster_id changes | {:,} |".format(changed_mapping),
        "| Stations with changed cluster membership | {:,} |".format(membership_changed),
        "| Baseline clusters split | {:,} |".format(
            int(agreement["n_baseline_clusters_split"])
        ),
        "| No-area clusters merging baseline clusters | {:,} |".format(
            int(agreement["n_candidate_clusters_merging_baseline_clusters"])
        ),
        "| Pairwise Jaccard | {} |".format(
            _fmt(agreement["pairwise_jaccard_vs_baseline"])
        ),
        "| Adjusted Rand Index | {} |".format(
            _fmt(agreement["adjusted_rand_index_vs_baseline"])
        ),
        "| Exact mapping identical | {} |".format("Yes" if exact_mapping else "No"),
        "| Equivalence check | {} |".format(result_status),
        "",
        "## Interpretation",
        "",
        interpretation,
        "",
        "The 0.10 threshold is therefore retained in the production implementation",
        "for provenance and reproducibility rather than removed solely to make the",
        "source code text match the abbreviated manuscript description.",
        "",
        "## Diagnostic counts",
        "",
        "- Baseline resolved stations: {:,}".format(int(baseline_stats.get("n_success", 0))),
        "- No-area resolved stations: {:,}".format(int(relaxed_stats.get("n_success", 0))),
        "- Baseline candidate-basin clusters: {:,}".format(
            int(baseline_stats.get("n_clusters_from_basins", 0))
        ),
        "- No-area candidate-basin clusters: {:,}".format(
            int(relaxed_stats.get("n_clusters_from_basins", 0))
        ),
        "",
        "Changed station assignments, if any, are written to:",
        "",
        str(args.changed_csv),
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return equivalent, gate_performed, bool(gate_exact) if gate_performed else True


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check whether the production 10% upstream-area s5 safeguard changes "
            "the station partition relative to the manuscript-effective rule."
        )
    )
    parser.add_argument("--s3-csv", type=Path, default=DEFAULT_S3)
    parser.add_argument("--s4-csv", type=Path, default=DEFAULT_S4)
    parser.add_argument("--s5-csv", type=Path, default=DEFAULT_S5)
    parser.add_argument("--distance-m", type=float, default=DEFAULT_DISTANCE_M)
    parser.add_argument(
        "--baseline-area-error",
        type=float,
        default=DEFAULT_BASELINE_AREA_ERROR,
    )
    parser.add_argument(
        "--upstream-area-col",
        default=DEFAULT_UPSTREAM_AREA_COL,
    )
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--changed-csv", type=Path, default=DEFAULT_CHANGED)
    args = parser.parse_args()

    if not args.s3_csv.is_file():
        print("ERROR: s3 CSV not found: {}".format(args.s3_csv), file=sys.stderr)
        return 1
    if not args.s4_csv.is_file():
        print("ERROR: s4 CSV not found: {}".format(args.s4_csv), file=sys.stderr)
        return 1

    s3 = normalize_station_table(
        pd.read_csv(args.s3_csv, low_memory=False),
        "s3 station CSV",
    )

    baseline, baseline_stats = _build_mapping(
        s3,
        args.s4_csv,
        args.distance_m,
        args.baseline_area_error,
        args.upstream_area_col,
    )
    relaxed, relaxed_stats = _build_mapping(
        s3,
        args.s4_csv,
        args.distance_m,
        DEFAULT_RELAXED_AREA_ERROR,
        args.upstream_area_col,
    )

    agreement = partition_agreement(baseline, relaxed)
    mapping_table = _exact_mapping_comparison(baseline, relaxed)
    changed = mapping_table.loc[mapping_table["mapping_changed"].fillna(True)].copy()
    args.changed_csv.parent.mkdir(parents=True, exist_ok=True)
    changed.to_csv(args.changed_csv, index=False)

    s5_gate = compare_generated_baseline_to_s5(
        baseline,
        args.s5_csv if args.s5_csv.is_file() else None,
    )

    equivalent, gate_performed, gate_ok = _write_report(
        args.report,
        args,
        baseline,
        relaxed,
        agreement,
        mapping_table,
        baseline_stats,
        relaxed_stats,
        s5_gate,
    )

    print("Report: {}".format(args.report))
    print("Changed assignments: {}".format(args.changed_csv))
    print(
        "Equivalence: {} (changed mappings={}, ARI={:.6f}, Jaccard={:.6f})".format(
            "PASS" if equivalent else "FAIL",
            int(mapping_table["mapping_changed"].fillna(True).sum()),
            float(agreement["adjusted_rand_index_vs_baseline"]),
            float(agreement["pairwise_jaccard_vs_baseline"]),
        )
    )
    if gate_performed:
        print("Production baseline vs current s5: {}".format("PASS" if gate_ok else "FAIL"))
    else:
        print("Production baseline vs current s5: SKIPPED (s5 not found)")

    if not gate_ok:
        return 3
    if not equivalent:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
