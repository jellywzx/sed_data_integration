#!/usr/bin/env python3
"""Sensitivity analysis for the s5 station-merge distance threshold.

This script is read-only with respect to s3/s4/s5 pipeline outputs. It reruns
only the s5 hydrological clustering step for several distance thresholds while
holding all other merge rules fixed, including:

- basin_status must be ``resolved``;
- basin_id must be valid and shared;
- satellite observations remain singleton clusters;
- upstream-area symmetric relative error must not exceed the configured limit;
- cluster formation uses the same complete-linkage implementation as s5.

The implementation deliberately calls
``basin_station_merge.load_station_to_basin_cluster_map`` rather than
reimplementing the clustering algorithm. This keeps the sensitivity analysis
consistent with the production s5 workflow.

Default thresholds are 500, 750, 1000, 1250, and 1500 m, corresponding to a
+/-50% test around the 1000 m baseline with intermediate values.

Outputs
-------
- ``s5_merge_distance_sensitivity_summary.csv``
- ``s5_merge_distance_sensitivity_station_membership.csv.gz``
- ``s5_merge_distance_sensitivity_cluster_changes.csv``
- ``s5_merge_distance_sensitivity_report.md``

Example
-------
python validate/s13_sensitivity_s5_merge_distance.py

python validate/s13_sensitivity_s5_merge_distance.py \
  --distance-thresholds-m 300 500 750 1000 1250 1500 2000 \
  --baseline-distance-m 1000 \
  --max-upstream-rel-error 0.10
"""

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from basin_station_merge import load_station_to_basin_cluster_map  # noqa: E402
from pipeline_paths import (  # noqa: E402
    S3_COLLECTED_CSV,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    get_output_r_root,
)

DEFAULT_DISTANCE_THRESHOLDS_M = (500.0, 750.0, 1000.0, 1250.0, 1500.0)
DEFAULT_BASELINE_DISTANCE_M = 1000.0
DEFAULT_MAX_UPSTREAM_REL_ERROR = 0.10
DEFAULT_UPSTREAM_AREA_COL = "uparea_merit"


class Config:
    def __init__(
        self,
        s3_csv: Path,
        s4_csv: Path,
        s5_csv: Optional[Path],
        out_dir: Path,
        distance_thresholds_m: Sequence[float],
        baseline_distance_m: float,
        max_upstream_rel_error: float,
        upstream_area_col: str,
        require_baseline_match: bool,
    ) -> None:
        self.s3_csv = Path(s3_csv)
        self.s4_csv = Path(s4_csv)
        self.s5_csv = Path(s5_csv) if s5_csv is not None else None
        self.out_dir = Path(out_dir)
        self.distance_thresholds_m = tuple(float(x) for x in distance_thresholds_m)
        self.baseline_distance_m = float(baseline_distance_m)
        self.max_upstream_rel_error = float(max_upstream_rel_error)
        self.upstream_area_col = str(upstream_area_col)
        self.require_baseline_match = bool(require_baseline_match)


def require_columns(df: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError("{} missing required columns: {}".format(label, ", ".join(missing)))


def normalize_station_table(df: pd.DataFrame, label: str) -> pd.DataFrame:
    work = df.copy()
    require_columns(work, ["station_key", "station_id", "lat", "lon"], label)

    work["station_key"] = work["station_key"].fillna("").astype(str).str.strip()
    if work["station_key"].eq("").any():
        raise ValueError("{} contains blank station_key values".format(label))
    if work["station_key"].duplicated(keep=False).any():
        raise ValueError("{} contains duplicate station_key values".format(label))

    station_id = pd.to_numeric(work["station_id"], errors="coerce")
    invalid = station_id.isna() | (station_id % 1 != 0)
    if invalid.any():
        raise ValueError("{} contains invalid station_id values".format(label))
    work["station_id"] = station_id.astype("int64")
    if work["station_id"].duplicated(keep=False).any():
        raise ValueError("{} contains duplicate station_id values".format(label))

    return work


def parse_thresholds(values: Sequence[float], baseline: float) -> Tuple[float, ...]:
    parsed: List[float] = []
    for value in values:
        number = float(value)
        if not np.isfinite(number) or number <= 0:
            raise ValueError("distance thresholds must be finite and > 0; got {}".format(value))
        parsed.append(number)

    baseline = float(baseline)
    if not np.isfinite(baseline) or baseline <= 0:
        raise ValueError("baseline distance must be finite and > 0")
    parsed.append(baseline)
    return tuple(sorted(set(parsed)))


def n_choose_2(value: int) -> int:
    value = int(value)
    return value * (value - 1) // 2 if value >= 2 else 0


def cluster_sizes(assignments: pd.DataFrame) -> pd.Series:
    return assignments.groupby("cluster_id", sort=True).size().astype("int64")


def build_assignments(
    s3: pd.DataFrame,
    mapping: Mapping[int, int],
    threshold_m: float,
) -> pd.DataFrame:
    keep_columns = [
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
        if column in s3.columns
    ]
    out = s3[keep_columns].copy()
    out["threshold_m"] = float(threshold_m)
    out["cluster_id"] = out["station_id"].map(
        lambda station_id: int(mapping.get(int(station_id), int(station_id)))
    )
    sizes = out.groupby("cluster_id")["station_id"].transform("size")
    out["cluster_size"] = sizes.astype("int64")
    out["is_multi_station_cluster"] = out["cluster_size"] > 1
    out["is_cluster_representative"] = out["station_id"] == out["cluster_id"]
    return out


def summarize_partition(
    assignments: pd.DataFrame,
    threshold_m: float,
    merge_stats: Mapping[str, object],
) -> Dict[str, object]:
    sizes = cluster_sizes(assignments)
    n_stations = int(len(assignments))
    n_clusters = int(len(sizes))
    n_singletons = int((sizes == 1).sum())
    n_multi = int((sizes > 1).sum())
    n_stations_multi = int(sizes.loc[sizes > 1].sum()) if n_multi else 0
    same_cluster_pairs = int(sum(n_choose_2(int(size)) for size in sizes.tolist()))

    return {
        "threshold_m": float(threshold_m),
        "is_baseline": False,
        "n_source_stations": n_stations,
        "n_clusters": n_clusters,
        "n_singleton_clusters": n_singletons,
        "singleton_cluster_fraction": n_singletons / n_clusters if n_clusters else np.nan,
        "n_multi_station_clusters": n_multi,
        "multi_station_cluster_fraction": n_multi / n_clusters if n_clusters else np.nan,
        "n_stations_in_multi_clusters": n_stations_multi,
        "stations_in_multi_clusters_fraction": n_stations_multi / n_stations if n_stations else np.nan,
        "cluster_reduction_from_singletons": n_stations - n_clusters,
        "max_cluster_size": int(sizes.max()) if len(sizes) else 0,
        "n_same_cluster_station_pairs": same_cluster_pairs,
        "n_resolved_stations": int(merge_stats.get("n_success", 0)),
        "n_satellite_excluded_from_merge": int(
            merge_stats.get("n_satellite_excluded_from_merge", 0)
        ),
        "n_basins": int(merge_stats.get("n_basins", 0)),
        "n_clusters_from_candidate_basins": int(
            merge_stats.get("n_clusters_from_basins", 0)
        ),
        "n_station_ids_remapped": int(merge_stats.get("n_changed", 0)),
    }


def membership_sets(assignments: pd.DataFrame) -> Dict[int, frozenset]:
    cluster_members = {
        int(cluster_id): frozenset(int(value) for value in group["station_id"].tolist())
        for cluster_id, group in assignments.groupby("cluster_id", sort=False)
    }
    return {
        int(row.station_id): cluster_members[int(row.cluster_id)]
        for row in assignments[["station_id", "cluster_id"]].itertuples(index=False)
    }


def partition_agreement(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
) -> Dict[str, object]:
    left = baseline[["station_id", "cluster_id"]].rename(
        columns={"cluster_id": "baseline_cluster_id"}
    )
    right = candidate[["station_id", "cluster_id"]].rename(
        columns={"cluster_id": "candidate_cluster_id"}
    )
    joined = left.merge(right, on="station_id", how="inner", validate="one_to_one")
    if len(joined) != len(baseline) or len(joined) != len(candidate):
        raise ValueError("baseline and candidate assignments do not cover the same stations")

    n = int(len(joined))
    total_pairs = n_choose_2(n)
    contingency = joined.groupby(
        ["baseline_cluster_id", "candidate_cluster_id"], sort=False
    ).size()
    same_both = int(sum(n_choose_2(int(value)) for value in contingency.tolist()))

    baseline_sizes = joined.groupby("baseline_cluster_id").size()
    candidate_sizes = joined.groupby("candidate_cluster_id").size()
    same_baseline = int(sum(n_choose_2(int(value)) for value in baseline_sizes.tolist()))
    same_candidate = int(sum(n_choose_2(int(value)) for value in candidate_sizes.tolist()))

    same_baseline_only = same_baseline - same_both
    same_candidate_only = same_candidate - same_both
    different_both = total_pairs - same_both - same_baseline_only - same_candidate_only

    pair_union = same_both + same_baseline_only + same_candidate_only
    pairwise_jaccard = same_both / pair_union if pair_union else 1.0
    rand_index = (same_both + different_both) / total_pairs if total_pairs else 1.0

    if total_pairs:
        expected_index = (same_baseline * same_candidate) / total_pairs
        max_index = 0.5 * (same_baseline + same_candidate)
        denominator = max_index - expected_index
        if math.isclose(denominator, 0.0, rel_tol=0.0, abs_tol=1e-15):
            adjusted_rand = 1.0 if same_baseline == same_candidate == same_both else 0.0
        else:
            adjusted_rand = (same_both - expected_index) / denominator
    else:
        adjusted_rand = 1.0

    baseline_members = membership_sets(baseline)
    candidate_members = membership_sets(candidate)
    changed_station_ids = [
        station_id
        for station_id in sorted(baseline_members)
        if baseline_members[station_id] != candidate_members[station_id]
    ]

    baseline_to_candidate = joined.groupby("baseline_cluster_id")[
        "candidate_cluster_id"
    ].nunique()
    candidate_to_baseline = joined.groupby("candidate_cluster_id")[
        "baseline_cluster_id"
    ].nunique()

    return {
        "n_stations_membership_changed_vs_baseline": int(len(changed_station_ids)),
        "stations_membership_changed_fraction_vs_baseline": (
            len(changed_station_ids) / n if n else np.nan
        ),
        "n_baseline_clusters_split": int((baseline_to_candidate > 1).sum()),
        "n_candidate_clusters_merging_baseline_clusters": int(
            (candidate_to_baseline > 1).sum()
        ),
        "pairwise_jaccard_vs_baseline": float(pairwise_jaccard),
        "rand_index_vs_baseline": float(rand_index),
        "adjusted_rand_index_vs_baseline": float(adjusted_rand),
        "same_cluster_pairs_in_both": same_both,
        "same_cluster_pairs_baseline_only": same_baseline_only,
        "same_cluster_pairs_candidate_only": same_candidate_only,
    }


def compare_generated_baseline_to_s5(
    baseline: pd.DataFrame,
    s5_path: Optional[Path],
) -> Dict[str, object]:
    result: Dict[str, object] = {
        "baseline_s5_check_performed": False,
        "baseline_s5_station_coverage_match": np.nan,
        "baseline_s5_cluster_id_mismatch_count": np.nan,
        "baseline_s5_cluster_id_mismatch_fraction": np.nan,
        "baseline_s5_exact_match": np.nan,
    }
    if s5_path is None or not s5_path.is_file():
        return result

    s5 = pd.read_csv(s5_path, low_memory=False)
    require_columns(s5, ["station_id", "cluster_id"], "s5 clustered station CSV")
    s5_ids = pd.to_numeric(s5["station_id"], errors="coerce")
    s5_clusters = pd.to_numeric(s5["cluster_id"], errors="coerce")
    valid = s5_ids.notna() & s5_clusters.notna() & (s5_ids % 1 == 0) & (s5_clusters % 1 == 0)
    if not bool(valid.all()):
        raise ValueError("s5 clustered station CSV contains invalid station_id/cluster_id values")

    observed = pd.DataFrame(
        {
            "station_id": s5_ids.astype("int64"),
            "s5_cluster_id": s5_clusters.astype("int64"),
        }
    )
    if observed["station_id"].duplicated(keep=False).any():
        raise ValueError("s5 clustered station CSV contains duplicate station_id values")

    generated = baseline[["station_id", "cluster_id"]].rename(
        columns={"cluster_id": "generated_cluster_id"}
    )
    merged = generated.merge(observed, on="station_id", how="outer", indicator=True)
    coverage_match = bool(merged["_merge"].eq("both").all())
    compared = merged.loc[merged["_merge"].eq("both")].copy()
    mismatch_count = int(
        (compared["generated_cluster_id"] != compared["s5_cluster_id"]).sum()
    )
    mismatch_fraction = mismatch_count / len(compared) if len(compared) else np.nan

    result.update(
        {
            "baseline_s5_check_performed": True,
            "baseline_s5_station_coverage_match": coverage_match,
            "baseline_s5_cluster_id_mismatch_count": mismatch_count,
            "baseline_s5_cluster_id_mismatch_fraction": mismatch_fraction,
            "baseline_s5_exact_match": bool(coverage_match and mismatch_count == 0),
        }
    )
    return result


def build_station_membership_table(
    assignments_by_threshold: Mapping[float, pd.DataFrame],
    baseline_threshold_m: float,
) -> pd.DataFrame:
    baseline = assignments_by_threshold[baseline_threshold_m]
    baseline_members = membership_sets(baseline)
    baseline_small = baseline[["station_id", "cluster_id", "cluster_size"]].rename(
        columns={
            "cluster_id": "baseline_cluster_id",
            "cluster_size": "baseline_cluster_size",
        }
    )

    rows: List[pd.DataFrame] = []
    for threshold_m in sorted(assignments_by_threshold):
        current = assignments_by_threshold[threshold_m].copy()
        current = current.merge(
            baseline_small,
            on="station_id",
            how="left",
            validate="one_to_one",
        )
        current_members = membership_sets(current[["station_id", "cluster_id"]])
        current["membership_changed_vs_baseline"] = current["station_id"].map(
            lambda station_id: baseline_members[int(station_id)]
            != current_members[int(station_id)]
        )
        current["representative_changed_vs_baseline"] = (
            current["cluster_id"] != current["baseline_cluster_id"]
        )
        current = current.rename(
            columns={
                "cluster_id": "sensitivity_cluster_id",
                "cluster_size": "sensitivity_cluster_size",
            }
        )
        rows.append(current)

    combined = pd.concat(rows, ignore_index=True)
    first_columns = [
        "threshold_m",
        "station_key",
        "station_id",
        "baseline_cluster_id",
        "sensitivity_cluster_id",
        "baseline_cluster_size",
        "sensitivity_cluster_size",
        "membership_changed_vs_baseline",
        "representative_changed_vs_baseline",
    ]
    ordered = [column for column in first_columns if column in combined.columns]
    ordered += [column for column in combined.columns if column not in ordered]
    return combined[ordered]


def unique_join(values: Iterable[object]) -> str:
    cleaned = sorted(
        {
            str(value).strip()
            for value in values
            if value is not None and not pd.isna(value) and str(value).strip()
        }
    )
    return "|".join(cleaned)


def build_cluster_change_table(
    assignments_by_threshold: Mapping[float, pd.DataFrame],
    baseline_threshold_m: float,
) -> pd.DataFrame:
    baseline = assignments_by_threshold[baseline_threshold_m][
        ["station_id", "cluster_id"]
    ].rename(columns={"cluster_id": "baseline_cluster_id"})
    rows: List[Dict[str, object]] = []

    for threshold_m in sorted(assignments_by_threshold):
        current = assignments_by_threshold[threshold_m].merge(
            baseline,
            on="station_id",
            how="left",
            validate="one_to_one",
        )
        baseline_fragment_counts = current.groupby("baseline_cluster_id")[
            "cluster_id"
        ].nunique()

        for cluster_id, group in current.groupby("cluster_id", sort=True):
            baseline_ids = sorted(int(value) for value in group["baseline_cluster_id"].unique())
            has_merge = len(baseline_ids) > 1
            has_split_origin = any(
                int(baseline_fragment_counts.loc[baseline_id]) > 1
                for baseline_id in baseline_ids
            )
            if has_merge and has_split_origin:
                change_type = "reconfigured"
            elif has_merge:
                change_type = "merged_baseline_clusters"
            elif has_split_origin:
                change_type = "split_fragment"
            else:
                baseline_id = baseline_ids[0]
                baseline_members = set(
                    baseline.loc[
                        baseline["baseline_cluster_id"] == baseline_id,
                        "station_id",
                    ].astype(int)
                )
                current_members = set(group["station_id"].astype(int))
                change_type = "unchanged" if current_members == baseline_members else "reconfigured"

            rows.append(
                {
                    "threshold_m": float(threshold_m),
                    "sensitivity_cluster_id": int(cluster_id),
                    "sensitivity_cluster_size": int(len(group)),
                    "baseline_cluster_ids": "|".join(str(value) for value in baseline_ids),
                    "n_baseline_clusters": int(len(baseline_ids)),
                    "change_type_vs_baseline": change_type,
                    "station_ids": "|".join(
                        str(value) for value in sorted(group["station_id"].astype(int).tolist())
                    ),
                    "sources": unique_join(group["source"]) if "source" in group.columns else "",
                    "resolutions": (
                        unique_join(group["resolution"])
                        if "resolution" in group.columns
                        else ""
                    ),
                }
            )

    return pd.DataFrame(rows)


def format_number(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (bool, np.bool_)):
        return "yes" if bool(value) else "no"
    if isinstance(value, (int, np.integer)):
        return "{:,}".format(int(value))
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if abs(number) >= 100:
            return "{:.0f}".format(number)
        if 0 <= abs(number) <= 1:
            return "{:.4f}".format(number)
        return "{:.2f}".format(number)
    return str(value)


def markdown_table(df: pd.DataFrame, columns: Sequence[str]) -> str:
    if len(df) == 0:
        return "(no rows)"
    view = df.loc[:, list(columns)].copy()
    headers = [str(column) for column in view.columns]
    integer_columns = {
        "threshold_m",
        "n_clusters",
        "n_singleton_clusters",
        "n_multi_station_clusters",
        "n_stations_in_multi_clusters",
        "cluster_reduction_from_singletons",
        "n_stations_membership_changed_vs_baseline",
        "n_baseline_clusters_split",
        "n_candidate_clusters_merging_baseline_clusters",
    }
    fraction_columns = {
        "stations_membership_changed_fraction_vs_baseline",
    }
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in view.iterrows():
        values = []
        for column in view.columns:
            value = row[column]
            if column in integer_columns and not pd.isna(value):
                rendered = "{:,}".format(int(round(float(value))))
            elif column in fraction_columns and not pd.isna(value):
                rendered = "{:.2%}".format(float(value))
            else:
                rendered = format_number(value)
            values.append(rendered.replace("|", "\\|"))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_report(
    path: Path,
    cfg: Config,
    summary: pd.DataFrame,
    baseline_check: Mapping[str, object],
) -> None:
    baseline_row = summary.loc[summary["is_baseline"]].iloc[0]
    alternatives = summary.loc[~summary["is_baseline"]].copy()

    if len(alternatives):
        max_changed = int(alternatives["n_stations_membership_changed_vs_baseline"].max())
        max_changed_fraction = float(
            alternatives["stations_membership_changed_fraction_vs_baseline"].max()
        )
        cluster_min = int(summary["n_clusters"].min())
        cluster_max = int(summary["n_clusters"].max())
        multi_min = int(summary["n_multi_station_clusters"].min())
        multi_max = int(summary["n_multi_station_clusters"].max())
    else:
        max_changed = 0
        max_changed_fraction = 0.0
        cluster_min = cluster_max = int(baseline_row["n_clusters"])
        multi_min = multi_max = int(baseline_row["n_multi_station_clusters"])

    baseline_check_text = "not performed"
    if bool(baseline_check.get("baseline_s5_check_performed", False)):
        baseline_check_text = (
            "PASS"
            if bool(baseline_check.get("baseline_s5_exact_match", False))
            else "FAIL"
        )

    report_columns = [
        "threshold_m",
        "n_clusters",
        "n_singleton_clusters",
        "n_multi_station_clusters",
        "n_stations_in_multi_clusters",
        "cluster_reduction_from_singletons",
        "n_stations_membership_changed_vs_baseline",
        "stations_membership_changed_fraction_vs_baseline",
        "n_baseline_clusters_split",
        "n_candidate_clusters_merging_baseline_clusters",
        "adjusted_rand_index_vs_baseline",
    ]

    lines = [
        "# S5 Merge-Distance Sensitivity Analysis",
        "",
        "This is a read-only sensitivity analysis of the maximum station-to-station distance used by s5 hydrological clustering.",
        "",
        "## Method",
        "",
        "- Production clustering function: `basin_station_merge.load_station_to_basin_cluster_map`",
        "- Distance thresholds (m): {}".format(
            ", ".join("{:g}".format(value) for value in cfg.distance_thresholds_m)
        ),
        "- Baseline distance (m): {:g}".format(cfg.baseline_distance_m),
        "- Upstream-area symmetric relative-error threshold: {:.3f}".format(
            cfg.max_upstream_rel_error
        ),
        "- Upstream-area column: `{}`".format(cfg.upstream_area_col),
        "- All non-distance s5 rules were held fixed.",
        "- Existing 1 km s5 output consistency check: {}".format(baseline_check_text),
        "",
        "## Summary",
        "",
        markdown_table(summary, report_columns),
        "",
        "## Main diagnostics",
        "",
        "- Baseline clusters at {:g} m: {:,}".format(
            cfg.baseline_distance_m, int(baseline_row["n_clusters"])
        ),
        "- Baseline multi-station clusters: {:,}".format(
            int(baseline_row["n_multi_station_clusters"])
        ),
        "- Cluster-count range across tested thresholds: {:,} to {:,}".format(
            cluster_min, cluster_max
        ),
        "- Multi-station-cluster range across tested thresholds: {:,} to {:,}".format(
            multi_min, multi_max
        ),
        "- Largest station-membership change relative to 1 km: {:,} stations ({:.2%})".format(
            max_changed, max_changed_fraction
        ),
        "",
        "## Baseline reproduction check",
        "",
        "- Check performed: {}".format(
            format_number(baseline_check.get("baseline_s5_check_performed"))
        ),
        "- Station coverage matches current s5: {}".format(
            format_number(baseline_check.get("baseline_s5_station_coverage_match"))
        ),
        "- Cluster-ID mismatches: {}".format(
            format_number(baseline_check.get("baseline_s5_cluster_id_mismatch_count"))
        ),
        "- Exact match: {}".format(
            format_number(baseline_check.get("baseline_s5_exact_match"))
        ),
        "",
        "## Manuscript-ready result template",
        "",
        (
            "Holding the basin-status, basin identifier, satellite-exclusion, upstream-area, "
            "and complete-linkage criteria fixed, changing the maximum station-separation "
            "threshold from {low:g} to {high:g} m produced {cluster_min:,}-{cluster_max:,} "
            "clusters, compared with {baseline_clusters:,} clusters at the {baseline:g} m baseline. "
            "Across the tested alternatives, at most {changed:,} source stations "
            "({changed_fraction:.2%}) changed cluster membership relative to the baseline."
        ).format(
            low=min(cfg.distance_thresholds_m),
            high=max(cfg.distance_thresholds_m),
            cluster_min=cluster_min,
            cluster_max=cluster_max,
            baseline_clusters=int(baseline_row["n_clusters"]),
            baseline=cfg.baseline_distance_m,
            changed=max_changed,
            changed_fraction=max_changed_fraction,
        ),
        "",
        "The template is descriptive only. Interpret sensitivity together with the detailed cluster-change table, especially when a small number of large clusters accounts for most changes.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def default_config() -> Config:
    output_r_root = get_output_r_root(REPO_ROOT)
    return Config(
        s3_csv=output_r_root / S3_COLLECTED_CSV,
        s4_csv=output_r_root / S4_UPSTREAM_CSV,
        s5_csv=output_r_root / S5_BASIN_CLUSTERED_CSV,
        out_dir=REPO_ROOT / "validate" / "output" / "s5_merge_distance_sensitivity",
        distance_thresholds_m=DEFAULT_DISTANCE_THRESHOLDS_M,
        baseline_distance_m=DEFAULT_BASELINE_DISTANCE_M,
        max_upstream_rel_error=DEFAULT_MAX_UPSTREAM_REL_ERROR,
        upstream_area_col=DEFAULT_UPSTREAM_AREA_COL,
        require_baseline_match=False,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    defaults = default_config()
    parser = argparse.ArgumentParser(
        description=(
            "Rerun the s5 clustering function across distance thresholds while "
            "holding all other merge criteria fixed."
        )
    )
    parser.add_argument("--s3-csv", default=str(defaults.s3_csv))
    parser.add_argument("--s4-csv", default=str(defaults.s4_csv))
    parser.add_argument(
        "--s5-csv",
        default=str(defaults.s5_csv) if defaults.s5_csv is not None else "",
        help="Existing s5 output used only to verify the 1 km baseline; pass an empty string to skip.",
    )
    parser.add_argument("--out-dir", default=str(defaults.out_dir))
    parser.add_argument(
        "--distance-thresholds-m",
        type=float,
        nargs="+",
        default=list(DEFAULT_DISTANCE_THRESHOLDS_M),
        help="Distance thresholds in metres. Default: 500 750 1000 1250 1500.",
    )
    parser.add_argument(
        "--baseline-distance-m",
        type=float,
        default=DEFAULT_BASELINE_DISTANCE_M,
    )
    parser.add_argument(
        "--max-upstream-rel-error",
        type=float,
        default=DEFAULT_MAX_UPSTREAM_REL_ERROR,
    )
    parser.add_argument(
        "--upstream-area-col",
        default=DEFAULT_UPSTREAM_AREA_COL,
    )
    parser.add_argument(
        "--require-baseline-match",
        action="store_true",
        help="Exit with an error when the regenerated 1 km assignments differ from the current s5 CSV.",
    )
    args = parser.parse_args(argv)

    thresholds = parse_thresholds(
        args.distance_thresholds_m,
        args.baseline_distance_m,
    )
    if not np.isfinite(args.max_upstream_rel_error) or args.max_upstream_rel_error < 0:
        raise ValueError("max upstream relative error must be finite and >= 0")

    s5_text = str(args.s5_csv).strip()
    return Config(
        s3_csv=Path(args.s3_csv).expanduser().resolve(),
        s4_csv=Path(args.s4_csv).expanduser().resolve(),
        s5_csv=Path(s5_text).expanduser().resolve() if s5_text else None,
        out_dir=Path(args.out_dir).expanduser().resolve(),
        distance_thresholds_m=thresholds,
        baseline_distance_m=float(args.baseline_distance_m),
        max_upstream_rel_error=float(args.max_upstream_rel_error),
        upstream_area_col=str(args.upstream_area_col),
        require_baseline_match=bool(args.require_baseline_match),
    )


def run_sensitivity(cfg: Config) -> Dict[str, object]:
    if not cfg.s3_csv.is_file():
        raise FileNotFoundError("s3 CSV not found: {}".format(cfg.s3_csv))
    if not cfg.s4_csv.is_file():
        raise FileNotFoundError("s4 CSV not found: {}".format(cfg.s4_csv))

    s3 = normalize_station_table(
        pd.read_csv(cfg.s3_csv, low_memory=False),
        "s3 collected station CSV",
    )

    assignments_by_threshold: Dict[float, pd.DataFrame] = {}
    summary_rows: List[Dict[str, object]] = []

    for threshold_m in cfg.distance_thresholds_m:
        mapping, merge_stats = load_station_to_basin_cluster_map(
            cfg.s4_csv,
            station_df=s3,
            max_station_distance_m=float(threshold_m),
            max_upstream_rel_error=float(cfg.max_upstream_rel_error),
            upstream_area_col=cfg.upstream_area_col,
        )
        assignments = build_assignments(s3, mapping, threshold_m)
        assignments_by_threshold[float(threshold_m)] = assignments
        summary_rows.append(
            summarize_partition(assignments, threshold_m, merge_stats)
        )

    baseline_threshold = float(cfg.baseline_distance_m)
    baseline = assignments_by_threshold[baseline_threshold]
    for row in summary_rows:
        threshold_m = float(row["threshold_m"])
        row["is_baseline"] = bool(math.isclose(threshold_m, baseline_threshold))
        row.update(
            partition_agreement(
                baseline,
                assignments_by_threshold[threshold_m],
            )
        )

    baseline_check = compare_generated_baseline_to_s5(baseline, cfg.s5_csv)
    if cfg.require_baseline_match:
        if not bool(baseline_check.get("baseline_s5_check_performed", False)):
            raise RuntimeError(
                "--require-baseline-match was set, but the existing s5 CSV was not available"
            )
        if not bool(baseline_check.get("baseline_s5_exact_match", False)):
            raise RuntimeError(
                "regenerated baseline does not exactly match the existing s5 clustered station CSV"
            )

    summary = pd.DataFrame(summary_rows).sort_values("threshold_m").reset_index(drop=True)
    for key, value in baseline_check.items():
        summary[key] = value

    membership = build_station_membership_table(
        assignments_by_threshold,
        baseline_threshold,
    )
    cluster_changes = build_cluster_change_table(
        assignments_by_threshold,
        baseline_threshold,
    )

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary": cfg.out_dir / "s5_merge_distance_sensitivity_summary.csv",
        "station_membership": cfg.out_dir
        / "s5_merge_distance_sensitivity_station_membership.csv.gz",
        "cluster_changes": cfg.out_dir
        / "s5_merge_distance_sensitivity_cluster_changes.csv",
        "report": cfg.out_dir / "s5_merge_distance_sensitivity_report.md",
    }
    summary.to_csv(paths["summary"], index=False)
    membership.to_csv(
        paths["station_membership"],
        index=False,
        compression="gzip",
    )
    cluster_changes.to_csv(paths["cluster_changes"], index=False)
    write_report(paths["report"], cfg, summary, baseline_check)

    return {
        "paths": paths,
        "summary": summary,
        "station_membership": membership,
        "cluster_changes": cluster_changes,
        "baseline_check": baseline_check,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    result = run_sensitivity(cfg)
    summary = result["summary"]
    baseline = summary.loc[summary["is_baseline"]].iloc[0]
    print("S5 merge-distance sensitivity analysis complete")
    print("Output directory: {}".format(cfg.out_dir))
    print("Thresholds (m): {}".format(
        ", ".join("{:g}".format(value) for value in cfg.distance_thresholds_m)
    ))
    print("Baseline clusters: {:,}".format(int(baseline["n_clusters"])))
    print(
        "Baseline multi-station clusters: {:,}".format(
            int(baseline["n_multi_station_clusters"])
        )
    )
    print(
        "Maximum membership change: {:,} stations ({:.2%})".format(
            int(summary["n_stations_membership_changed_vs_baseline"].max()),
            float(summary["stations_membership_changed_fraction_vs_baseline"].max()),
        )
    )
    if bool(result["baseline_check"].get("baseline_s5_check_performed", False)):
        print(
            "Existing s5 baseline exact match: {}".format(
                bool(result["baseline_check"].get("baseline_s5_exact_match", False))
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

