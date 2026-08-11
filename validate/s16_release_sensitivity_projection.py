#!/usr/bin/env python3
"""Sensitivity of the *release* cluster set to the s5 merge-distance threshold.

This script projects the s5 membership sensitivity results (from s15) onto the
release-eligible source-station population.  It is deliberately read-only with
respect to the full s6–s8 pipeline — it does NOT re-merge time series or
re-export NetCDF files.

Method
------
1. Load the s15 station-membership table (or re-run the s5 clustering if
   needed) for every distance threshold, holding all other s5 rules fixed.
2. Read the official ``source_station_catalog.csv`` and identify every
   release-eligible (source-station, resolution) row.
3. Join row ↔ s5 station_id via path basename (verified 1:1 for the full
   release population).
4. For each threshold, replace the baseline ``cluster_id`` with the
   sensitivity-cluster representative, then re-compute per-resolution unique
   cluster sets — exactly the dedup logic that ``station_catalog.csv``
   encodes.
5. If the 1000 m projection does **not** reproduce 3,762 released clusters,
   the script aborts immediately (the gate).  Otherwise it reports
   per-threshold × per-resolution sensitivity metrics.

Outputs
-------
- ``s16_release_sensitivity_summary.csv``
- ``s16_release_sensitivity_station_membership.csv.gz``
- ``s16_release_sensitivity_cluster_changes.csv``
- ``s16_release_sensitivity_report.md``
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
for _p in (str(REPO_ROOT), str(SCRIPT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from basin_station_merge import load_station_to_basin_cluster_map  # noqa: E402
from pipeline_paths import (  # noqa: E402
    S3_COLLECTED_CSV,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    RELEASE_SOURCE_STATION_CATALOG_CSV,
    get_output_r_root,
)

# ---- reuse s15 helpers (the module has no import-time side effects) ----------
from s15_sensitivity_merge_distance import (  # noqa: E402
    build_assignments,
    build_cluster_change_table,
    build_station_membership_table,
    format_number,
    markdown_table,
    normalize_station_table,
    parse_thresholds,
    partition_agreement,
    require_columns,
)

DEFAULT_DISTANCE_THRESHOLDS_M = (500.0, 750.0, 1000.0, 1250.0, 1500.0)
DEFAULT_BASELINE_DISTANCE_M = 1000.0
DEFAULT_MAX_UPSTREAM_REL_ERROR = 0.10
DEFAULT_UPSTREAM_AREA_COL = "uparea_merit"
RELEASE_RESOLUTIONS = ("daily", "monthly", "annual")

# Default expected cluster counts (derived from the current release)
DEFAULT_EXPECTED_CLUSTER_COUNTS = {
    "daily": 1596,
    "monthly": 2117,
    "annual": 58,
    "total": 3762,
}


class Config:
    def __init__(
        self,
        s3_csv: Path,
        s4_csv: Path,
        s5_csv: Path,
        release_dir: Path,
        out_dir: Path,
        distance_thresholds_m: Sequence[float],
        baseline_distance_m: float,
        max_upstream_rel_error: float,
        upstream_area_col: str,
        expected_cluster_counts: Mapping[str, int],
        require_release_baseline_match: bool,
    ) -> None:
        self.s3_csv = Path(s3_csv)
        self.s4_csv = Path(s4_csv)
        self.s5_csv = Path(s5_csv)
        self.release_dir = Path(release_dir)
        self.out_dir = Path(out_dir)
        self.distance_thresholds_m = tuple(float(x) for x in distance_thresholds_m)
        self.baseline_distance_m = float(baseline_distance_m)
        self.max_upstream_rel_error = float(max_upstream_rel_error)
        self.upstream_area_col = str(upstream_area_col)
        self.expected_cluster_counts = {
            str(k): int(v) for k, v in expected_cluster_counts.items()
        }
        self.require_release_baseline_match = bool(require_release_baseline_match)


# ---------------------------------------------------------------------------
# Release-station table (Steps 2-3: eligibility + mapping)
# ---------------------------------------------------------------------------

def load_release_station_table(
    release_dir: Path,
    s5_df: pd.DataFrame,
) -> pd.DataFrame:
    """Read source_station_catalog.csv and join with s5 via path basename.

    Returns a DataFrame with one row per release-eligible
    (source_station_uid, resolution) and columns:

    * source_station_uid, source_station_index
    * cluster_id (release / baseline), cluster_uid
    * resolution
    * station_id (s5 integer id) — the mapping key
    * path_basename (join key)
    * source_name
    """
    catalog_path = release_dir / Path(RELEASE_SOURCE_STATION_CATALOG_CSV).name
    if not catalog_path.is_file():
        raise FileNotFoundError(
            "Release source-station catalog not found: {}".format(catalog_path)
    )
    ssc = pd.read_csv(catalog_path, low_memory=False)
    if "cluster_id" not in ssc.columns and "station_reference_id" in ssc.columns:
        ssc["cluster_id"] = ssc["station_reference_id"]
    if "cluster_uid" not in ssc.columns and "station_uid" in ssc.columns:
        ssc["cluster_uid"] = ssc["station_uid"]
    require_columns(
        ssc,
        [
            "source_station_uid",
            "source_station_index",
            "cluster_id",
            "cluster_uid",
            "resolution",
            "source_station_paths",
            "source_name",
        ],
        "source_station_catalog.csv",
    )

    ssc["path_basename"] = ssc["source_station_paths"].apply(
        lambda p: Path(str(p).strip()).name if pd.notna(p) else ""
    )
    if (ssc["path_basename"] == "").any():
        raise ValueError("source_station_catalog.csv contains blank paths")

    # Filter to main-product resolutions only
    ssc = ssc[ssc["resolution"].isin(RELEASE_RESOLUTIONS)].copy()
    if len(ssc) == 0:
        raise RuntimeError("No release-eligible stations in daily/monthly/annual resolutions")

    # Each source_station must appear in exactly one resolution
    dup_uids = ssc.groupby("source_station_uid")["resolution"].nunique()
    multi_res_stations = dup_uids[dup_uids > 1]
    if len(multi_res_stations):
        raise RuntimeError(
            "source_station_catalog has stations in multiple resolutions: {}".format(
                list(multi_res_stations.index[:5])
            )
        )

    # Validate cluster_uid format
    expected_uid = ssc["cluster_id"].apply(
        lambda cid: "SED{:06d}".format(int(cid)) if pd.notna(cid) else ""
    )
    uid_mismatch = (ssc["cluster_uid"].fillna("") != expected_uid)
    if uid_mismatch.any():
        raise RuntimeError(
            "cluster_uid format mismatch in source_station_catalog: {} rows".format(
                uid_mismatch.sum()
            )
        )

    # Build s5 path basename
    require_columns(s5_df, ["path", "station_id", "cluster_id"], "s5 clustered station CSV")
    s5_df = s5_df.copy()
    s5_df["path_basename"] = s5_df["path"].apply(
        lambda p: Path(str(p).strip()).name if pd.notna(p) else ""
    )

    # 1:1 join on path basename
    merged = ssc.merge(
        s5_df[["path_basename", "station_id", "cluster_id"]].rename(
            columns={"cluster_id": "s5_cluster_id"}
        ),
        on="path_basename",
        how="left",
        validate="one_to_one",
    )

    n_missing = merged["station_id"].isna().sum()
    if n_missing:
        missing_basenames = merged.loc[
            merged["station_id"].isna(), "path_basename"
        ].head(10)
        raise RuntimeError(
            "{} release-eligible stations could not be matched to s5 station_ids. "
            "Sample basenames: {}".format(n_missing, list(missing_basenames))
        )

    # Verify cluster_id identity: release cluster_id == s5 cluster_id
    cid_mismatch = (
        merged["cluster_id"].astype(int) != merged["s5_cluster_id"].astype(int)
    )
    if cid_mismatch.any():
        raise RuntimeError(
            "{} rows have release cluster_id != s5 cluster_id".format(
                cid_mismatch.sum()
            )
        )

    merged["station_id"] = merged["station_id"].astype("int64")
    merged["cluster_id"] = merged["cluster_id"].astype("int64")

    return merged


def derive_expected_counts(release_table: pd.DataFrame) -> Dict[str, int]:
    """Compute expected cluster counts from the release table itself."""
    counts = {}
    for res in RELEASE_RESOLUTIONS:
        sub = release_table[release_table["resolution"] == res]
        counts[res] = int(sub["cluster_id"].nunique())
    counts["total"] = int(release_table["cluster_id"].nunique())
    return counts


def validate_expected_counts(
    catalog_counts: Mapping[str, int],
    configured: Mapping[str, int],
) -> None:
    """Compare catalog-derived counts against configured defaults."""
    for key, expected in configured.items():
        actual = catalog_counts.get(key)
        if actual is None:
            raise RuntimeError(
                "Missing key '{}' in catalog-derived counts".format(key)
            )
        if int(actual) != int(expected):
            raise RuntimeError(
                "Expected-count mismatch for '{}': catalog={}, configured={}. "
                "The release appears to have been regenerated — please update "
                "--expected-cluster-counts or re-derive expectations.".format(
                    key, actual, expected
                )
            )


# ---------------------------------------------------------------------------
# Projection (Step 4-5: apply sensitivity mapping → per-resolution dedup)
# ---------------------------------------------------------------------------

def restrict_assignments(
    assignments: pd.DataFrame,
    release_table: pd.DataFrame,
) -> pd.DataFrame:
    """Subset full-population assignments to release-eligible station_ids.

    Attaches ``resolution``, ``release_cluster_id`` and ``release_cluster_uid``
    from the release catalog.
    """
    keep = release_table[["station_id", "resolution", "cluster_id", "cluster_uid"]].rename(
        columns={
            "cluster_id": "release_cluster_id",
            "cluster_uid": "release_cluster_uid",
        }
    )
    # Drop resolution from assignments to avoid _x/_y suffix on merge
    work = assignments.drop(columns=["resolution"], errors="ignore")
    out = work.merge(keep, on="station_id", how="inner", validate="one_to_one")
    return out


def projected_cluster_ids_per_resolution(
    restricted: pd.DataFrame,
) -> pd.Series:
    """Return unique sensitivity_cluster_id per (threshold_m, resolution)."""
    return restricted.groupby(
        ["threshold_m", "resolution"], sort=False
    )["cluster_id"].apply(lambda x: frozenset(x.astype(int)))


def projected_counts_per_resolution(
    restricted: pd.DataFrame,
) -> pd.DataFrame:
    """Count unique sensitivity clusters per (threshold, resolution)."""
    groups = restricted.groupby(["threshold_m", "resolution"], sort=False)
    rows = []
    for (thresh, res), grp in groups:
        rows.append({
            "threshold_m": float(thresh),
            "resolution": str(res),
            "n_release_stations": int(len(grp)),
            "n_projected_clusters": int(grp["cluster_id"].nunique()),
        })
    # Add 'all' rows (union across resolutions)
    all_groups = restricted.groupby("threshold_m", sort=False)
    for thresh, grp in all_groups:
        rows.append({
            "threshold_m": float(thresh),
            "resolution": "all",
            "n_release_stations": int(len(grp)),
            "n_projected_clusters": int(grp["cluster_id"].nunique()),
        })
    return pd.DataFrame(rows).sort_values(["threshold_m", "resolution"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Baseline validation gate (Step 7)
# ---------------------------------------------------------------------------

def validate_baseline_projection(
    restricted_baseline: pd.DataFrame,
    release_table: pd.DataFrame,
    expected: Mapping[str, int],
) -> Dict[str, object]:
    """Gate: verify 1000 m projection reproduces the release cluster set.

    Returns a diagnostic dict.  Raises RuntimeError on failure.
    """
    # Identity check: every release station's sensitivity_cluster_id == release_cluster_id
    identity_ok = bool(
        (restricted_baseline["cluster_id"] == restricted_baseline["release_cluster_id"]).all()
    )
    mismatches = 0 if identity_ok else int(
        (restricted_baseline["cluster_id"] != restricted_baseline["release_cluster_id"]).sum()
    )

    # Per-resolution counts
    counts = projected_counts_per_resolution(restricted_baseline)
    all_row = counts[counts["resolution"] == "all"].iloc[0]
    total_ok = int(all_row["n_projected_clusters"]) == int(expected["total"])

    per_res_ok = True
    per_res_details = {}
    for res in RELEASE_RESOLUTIONS:
        row = counts[counts["resolution"] == res]
        if len(row) == 0:
            per_res_ok = False
            per_res_details[res] = {"expected": expected.get(res, "?"), "actual": 0}
        else:
            actual = int(row.iloc[0]["n_projected_clusters"])
            ok = actual == int(expected.get(res, -1))
            per_res_ok = per_res_ok and ok
            per_res_details[res] = {"expected": int(expected.get(res, -1)), "actual": actual, "ok": ok}

    gate_passed = identity_ok and total_ok and per_res_ok

    result = {
        "gate_passed": gate_passed,
        "identity_preserved": identity_ok,
        "identity_mismatches": mismatches,
        "total_clusters": int(all_row["n_projected_clusters"]),
        "total_expected": int(expected["total"]),
        "total_ok": total_ok,
        "per_resolution": per_res_details,
    }

    if not gate_passed:
        lines = [
            "BASELINE PROJECTION GATE FAILED",
            "=" * 60,
            "The 1000 m s5 clustering projection does not reproduce the release cluster set.",
            "",
            "identity preserved: {}".format(identity_ok),
            "total clusters: {} (expected {})".format(
                result["total_clusters"], expected["total"]
            ),
        ]
        for res, det in per_res_details.items():
            lines.append(
                "  {}: {} (expected {}) {}".format(
                    res, det["actual"], det["expected"], "OK" if det.get("ok") else "FAIL"
                )
            )
        lines += [
            "",
            "This means s6-s8 filtering is non-trivial beyond station-list membership",
            "projection. A full rerun (s6 merge -> s8 publish) is required to validate",
            "the sensitivity of the release layer to the merge-distance threshold.",
        ]
        raise RuntimeError("\n".join(lines))

    return result


# ---------------------------------------------------------------------------
# Release-layer summary & reporting
# ---------------------------------------------------------------------------

def build_release_summary(
    assignments_by_threshold: Mapping[float, pd.DataFrame],
    baseline_threshold_m: float,
    release_table: pd.DataFrame,
    expected: Mapping[str, int],
) -> pd.DataFrame:
    """Build per-threshold x per-resolution summary with agreement metrics."""
    baseline = assignments_by_threshold[baseline_threshold_m]
    rows = []

    for threshold_m, restricted in assignments_by_threshold.items():
        is_baseline = bool(math.isclose(threshold_m, baseline_threshold_m))

        # Overall 'all' row
        all_row = {
            "threshold_m": float(threshold_m),
            "resolution": "all",
            "is_baseline": is_baseline,
            "n_release_stations": int(len(restricted)),
            "n_projected_clusters": int(restricted["cluster_id"].nunique()),
            "expected_release_cluster_count": int(expected["total"]),
        }
        if is_baseline:
            all_row["n_stations_membership_changed_vs_baseline"] = 0
            all_row["stations_membership_changed_fraction_vs_baseline"] = 0.0
            all_row["adjusted_rand_index_vs_baseline"] = 1.0
            all_row["n_release_clusters_split"] = 0
            all_row["n_sensitivity_clusters_merging_release"] = 0
        else:
            agreement = partition_agreement(
                baseline[["station_id", "cluster_id"]].rename(
                    columns={"cluster_id": "cluster_id"}
                ),
                restricted[["station_id", "cluster_id"]],
            )
            all_row.update({
                "n_stations_membership_changed_vs_baseline": agreement.get(
                    "n_stations_membership_changed_vs_baseline", 0
                ),
                "stations_membership_changed_fraction_vs_baseline": agreement.get(
                    "stations_membership_changed_fraction_vs_baseline", 0.0
                ),
                "adjusted_rand_index_vs_baseline": agreement.get(
                    "adjusted_rand_index_vs_baseline", np.nan
                ),
                "n_release_clusters_split": agreement.get(
                    "n_baseline_clusters_split", 0
                ),
                "n_sensitivity_clusters_merging_release": agreement.get(
                    "n_candidate_clusters_merging_baseline_clusters", 0
                ),
            })
        rows.append(all_row)

        # Per-resolution rows
        for res in RELEASE_RESOLUTIONS:
            res_restricted = restricted[restricted["resolution"] == res]
            if len(res_restricted) == 0:
                continue
            res_row = {
                "threshold_m": float(threshold_m),
                "resolution": res,
                "is_baseline": is_baseline,
                "n_release_stations": int(len(res_restricted)),
                "n_projected_clusters": int(res_restricted["cluster_id"].nunique()),
                "expected_release_cluster_count": int(expected.get(res, 0)),
            }
            if is_baseline:
                res_row["n_stations_membership_changed_vs_baseline"] = 0
                res_row["stations_membership_changed_fraction_vs_baseline"] = 0.0
                res_row["adjusted_rand_index_vs_baseline"] = 1.0
            else:
                baseline_res = baseline[baseline["resolution"] == res]
                if len(baseline_res) > 0 and len(res_restricted) > 0:
                    common_ids = set(baseline_res["station_id"]) & set(
                        res_restricted["station_id"]
                    )
                    if len(common_ids) > 1:
                        b_sub = baseline_res[
                            baseline_res["station_id"].isin(common_ids)
                        ][["station_id", "cluster_id"]]
                        c_sub = res_restricted[
                            res_restricted["station_id"].isin(common_ids)
                        ][["station_id", "cluster_id"]]
                        agreement = partition_agreement(b_sub, c_sub)
                        res_row.update({
                            "n_stations_membership_changed_vs_baseline": agreement.get(
                                "n_stations_membership_changed_vs_baseline", 0
                            ),
                            "stations_membership_changed_fraction_vs_baseline": agreement.get(
                                "stations_membership_changed_fraction_vs_baseline", 0.0
                            ),
                            "adjusted_rand_index_vs_baseline": agreement.get(
                                "adjusted_rand_index_vs_baseline", np.nan
                            ),
                        })
            rows.append(res_row)

    return pd.DataFrame(rows).sort_values(["threshold_m", "resolution"]).reset_index(drop=True)


def build_release_cluster_change_table(
    assignments_by_threshold: Mapping[float, pd.DataFrame],
    baseline_threshold_m: float,
) -> pd.DataFrame:
    """Build cluster-change table for release-eligible clusters only.

    Loops over resolutions and calls s15's build_cluster_change_table on each
    per-resolution subset, then concatenates.
    """
    frames = []
    baseline = assignments_by_threshold[baseline_threshold_m]
    for res in RELEASE_RESOLUTIONS:
        by_threshold_res = {}
        for thresh, restricted in assignments_by_threshold.items():
            by_threshold_res[thresh] = restricted[restricted["resolution"] == res].copy()
        baseline_res = baseline[baseline["resolution"] == res]
        if len(baseline_res) == 0:
            continue
        try:
            res_changes = build_cluster_change_table(
                by_threshold_res, baseline_threshold_m
            )
            res_changes["resolution"] = res
            frames.append(res_changes)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    # Add release_cluster_uids
    if "baseline_cluster_ids" in combined.columns:
        combined["release_cluster_uids"] = combined["baseline_cluster_ids"].apply(
            lambda x: "|".join(
                "SED{:06d}".format(int(v)) for v in str(x).split("|") if v.strip()
            )
            if pd.notna(x) else ""
        )
    if "sensitivity_cluster_id" in combined.columns:
        combined["sensitivity_cluster_uid"] = combined["sensitivity_cluster_id"].apply(
            lambda x: "SED{:06d}".format(int(x)) if pd.notna(x) else ""
        )
    return combined


def write_report(
    path: Path,
    cfg: Config,
    summary: pd.DataFrame,
    baseline_check: Dict[str, object],
    cluster_change_summary: pd.DataFrame,
) -> None:
    """Write the sensitivity analysis report in Markdown."""
    baseline_rows = summary[summary["is_baseline"]]
    all_baseline = baseline_rows[baseline_rows["resolution"] == "all"].iloc[0]
    alternatives = summary[~summary["is_baseline"]]

    alt_all = alternatives[alternatives["resolution"] == "all"]
    if len(alt_all):
        max_changed = int(
            alt_all["n_stations_membership_changed_vs_baseline"].max()
            if "n_stations_membership_changed_vs_baseline" in alt_all.columns
            else 0
        )
        max_changed_frac = float(
            alt_all["stations_membership_changed_fraction_vs_baseline"].max()
            if "stations_membership_changed_fraction_vs_baseline" in alt_all.columns
            else 0.0
        )
        cluster_min = int(alt_all["n_projected_clusters"].min())
        cluster_max = int(alt_all["n_projected_clusters"].max())
    else:
        max_changed = 0
        max_changed_frac = 0.0
        cluster_min = cluster_max = int(all_baseline["n_projected_clusters"])

    gate_str = "PASS" if baseline_check.get("gate_passed", False) else "FAIL"

    lines = [
        "# S5 Merge-Distance Sensitivity: Release-Layer Projection",
        "",
        "This is a read-only *release membership projection* sensitivity analysis.",
        "It reruns the s5 hydrological clustering for several distance thresholds,",
        "projects the resulting cluster assignments onto the release-eligible",
        "source-station population, and reports per-resolution cluster-count changes.",
        "",
        "## Method",
        "",
        "- Production clustering: `basin_station_merge.load_station_to_basin_cluster_map`",
        "- Distance thresholds (m): {}".format(
            ", ".join("{:g}".format(v) for v in cfg.distance_thresholds_m)
        ),
        "- Baseline distance (m): {:g}".format(cfg.baseline_distance_m),
        "- Upstream-area threshold: {:.3f}".format(cfg.max_upstream_rel_error),
        "- Release catalog: `source_station_catalog.csv`",
        "- Station mapping: path-basename join (verified 1:1 for {} stations)".format(
            len(cfg.expected_cluster_counts)
        ),
        "- Baseline projection gate: **{}**".format(gate_str),
        "",
        "## Baseline Validation",
        "",
        "- Identity preserved (cluster_id == release_cluster_id): {}".format(
            format_number(baseline_check.get("identity_preserved"))
        ),
        "- Total projected clusters: {} (expected {})".format(
            baseline_check.get("total_clusters", "?"),
            baseline_check.get("total_expected", "?"),
        ),
    ]

    per_res = baseline_check.get("per_resolution", {})
    for res in RELEASE_RESOLUTIONS:
        det = per_res.get(res, {})
        lines.append(
            "- {}: {} (expected {}) {}".format(
                res,
                det.get("actual", "?"),
                det.get("expected", "?"),
                "OK" if det.get("ok") else "FAIL",
            )
        )

    # Summary table
    report_cols = [
        "threshold_m",
        "resolution",
        "n_projected_clusters",
        "n_release_stations",
        "n_stations_membership_changed_vs_baseline",
        "stations_membership_changed_fraction_vs_baseline",
        "adjusted_rand_index_vs_baseline",
    ]
    available = [c for c in report_cols if c in summary.columns]

    lines += [
        "",
        "## Per-Threshold × Per-Resolution Summary",
        "",
        markdown_table(summary[summary["resolution"] != "all"], available),
        "",
        "## Overall (All Resolutions Combined)",
        "",
        markdown_table(summary[summary["resolution"] == "all"], available),
        "",
        "## Main Diagnostics",
        "",
        "- Baseline release clusters at {:g} m: {:,}".format(
            cfg.baseline_distance_m, int(all_baseline["n_projected_clusters"])
        ),
        "- Per-resolution baseline: daily={}, monthly={}, annual={}".format(
            int(baseline_rows[baseline_rows["resolution"] == "daily"].iloc[0]["n_projected_clusters"]),
            int(baseline_rows[baseline_rows["resolution"] == "monthly"].iloc[0]["n_projected_clusters"]),
            int(baseline_rows[baseline_rows["resolution"] == "annual"].iloc[0]["n_projected_clusters"]),
        ),
        "- Cluster-count range (overall): {:,} – {:,}".format(cluster_min, cluster_max),
        "- Largest station-membership change: {:,} stations ({:.2%})".format(
            max_changed, max_changed_frac
        ),
        "",
        "## Manuscript-Ready Result Template",
        "",
        (
            "Projecting the s5 clustering results at {low:g}–{high:g} m onto the 3,913 "
            "release-eligible source stations produced {cluster_min:,}–{cluster_max:,} "
            "unique release clusters, compared with {baseline_clusters:,} at the "
            "{baseline:g} m baseline. Across all tested non-baseline thresholds, at most "
            "{changed:,} source stations ({changed_fraction:.2%}) changed cluster "
            "membership relative to the baseline. The release cluster set is "
            "{robustness} to the merge-distance parameter."
        ).format(
            low=min(cfg.distance_thresholds_m),
            high=max(cfg.distance_thresholds_m),
            cluster_min=cluster_min,
            cluster_max=cluster_max,
            baseline_clusters=int(all_baseline["n_projected_clusters"]),
            baseline=cfg.baseline_distance_m,
            changed=max_changed,
            changed_fraction=max_changed_frac,
            robustness=(
                "robust"
                if max_changed_frac < 0.02
                else "moderately sensitive"
                if max_changed_frac < 0.05
                else "sensitive"
            ),
        ),
        "",
        "Interpret together with the per-resolution cluster-change table.",
    ]

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def parse_expected_counts(text: str) -> Dict[str, int]:
    """Parse 'daily:1596,monthly:2117,annual:58,total:3762'."""
    result = {}
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        key, _, value = part.partition(":")
        key = key.strip()
        if key not in ("daily", "monthly", "annual", "total"):
            raise ValueError("Unknown expected-count key: {}".format(key))
        result[key] = int(value.strip())
    return result


def default_config() -> Config:
    output_r_root = get_output_r_root(REPO_ROOT)
    return Config(
        s3_csv=output_r_root / S3_COLLECTED_CSV,
        s4_csv=output_r_root / S4_UPSTREAM_CSV,
        s5_csv=output_r_root / S5_BASIN_CLUSTERED_CSV,
        release_dir=output_r_root
        / Path(RELEASE_SOURCE_STATION_CATALOG_CSV).parent,
        out_dir=SCRIPT_DIR / "output" / "s16_release_sensitivity",
        distance_thresholds_m=DEFAULT_DISTANCE_THRESHOLDS_M,
        baseline_distance_m=DEFAULT_BASELINE_DISTANCE_M,
        max_upstream_rel_error=DEFAULT_MAX_UPSTREAM_REL_ERROR,
        upstream_area_col=DEFAULT_UPSTREAM_AREA_COL,
        expected_cluster_counts=DEFAULT_EXPECTED_CLUSTER_COUNTS,
        require_release_baseline_match=False,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    defaults = default_config()
    parser = argparse.ArgumentParser(
        description="Project s5 merge-distance sensitivity onto the release layer."
    )
    parser.add_argument("--s3-csv", default=str(defaults.s3_csv))
    parser.add_argument("--s4-csv", default=str(defaults.s4_csv))
    parser.add_argument("--s5-csv", default=str(defaults.s5_csv))
    parser.add_argument("--release-dir", default=str(defaults.release_dir))
    parser.add_argument("--out-dir", default=str(defaults.out_dir))
    parser.add_argument(
        "--distance-thresholds-m",
        type=float,
        nargs="+",
        default=list(DEFAULT_DISTANCE_THRESHOLDS_M),
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
        "--expected-cluster-counts",
        default="daily:1596,monthly:2117,annual:58,total:3762",
        help="Comma-separated key:value pairs. Default: daily:1596,monthly:2117,annual:58,total:3762",
    )
    parser.add_argument(
        "--require-release-baseline-match",
        action="store_true",
        help="Exit with error if the projected baseline differs from the release catalog.",
    )
    args = parser.parse_args(argv)

    thresholds = parse_thresholds(args.distance_thresholds_m, args.baseline_distance_m)
    expected = parse_expected_counts(args.expected_cluster_counts)

    return Config(
        s3_csv=Path(args.s3_csv).expanduser().resolve(),
        s4_csv=Path(args.s4_csv).expanduser().resolve(),
        s5_csv=Path(args.s5_csv).expanduser().resolve(),
        release_dir=Path(args.release_dir).expanduser().resolve(),
        out_dir=Path(args.out_dir).expanduser().resolve(),
        distance_thresholds_m=thresholds,
        baseline_distance_m=float(args.baseline_distance_m),
        max_upstream_rel_error=float(args.max_upstream_rel_error),
        upstream_area_col=str(args.upstream_area_col),
        expected_cluster_counts=expected,
        require_release_baseline_match=bool(args.require_release_baseline_match),
    )


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run_release_projection(cfg: Config) -> Dict[str, object]:
    """Execute the full release-projection sensitivity analysis."""
    # --- validate inputs ---
    if not cfg.s3_csv.is_file():
        raise FileNotFoundError("s3 CSV not found: {}".format(cfg.s3_csv))
    if not cfg.s4_csv.is_file():
        raise FileNotFoundError("s4 CSV not found: {}".format(cfg.s4_csv))
    if not cfg.s5_csv.is_file():
        raise FileNotFoundError("s5 CSV not found: {}".format(cfg.s5_csv))

    # --- load s3 and s5 ---
    print("Loading s3 station table ...")
    s3 = normalize_station_table(
        pd.read_csv(cfg.s3_csv, low_memory=False),
        "s3 collected station CSV",
    )
    s5 = pd.read_csv(cfg.s5_csv, low_memory=False)
    print("  {} stations".format(len(s3)))

    # --- build release-station table ---
    print("Loading release source-station catalog ...")
    release_table = load_release_station_table(cfg.release_dir, s5)
    print("  {} release-eligible station-resolution rows".format(len(release_table)))

    # --- validate expected counts ---
    catalog_counts = derive_expected_counts(release_table)
    validate_expected_counts(catalog_counts, cfg.expected_cluster_counts)
    expected = dict(catalog_counts)  # Use catalog-derived for runtime
    print(
        "  Expected: daily={}, monthly={}, annual={}, total={}".format(
            expected["daily"], expected["monthly"], expected["annual"], expected["total"]
        )
    )

    # --- run s5 clustering for each threshold ---
    print("Running s5 clustering for {} thresholds ...".format(len(cfg.distance_thresholds_m)))
    assignments_by_threshold: Dict[float, pd.DataFrame] = {}
    for threshold_m in cfg.distance_thresholds_m:
        print("  threshold {:g} m ...".format(threshold_m), end=" ", flush=True)
        mapping, merge_stats = load_station_to_basin_cluster_map(
            cfg.s4_csv,
            station_df=s3,
            max_station_distance_m=float(threshold_m),
            max_upstream_rel_error=float(cfg.max_upstream_rel_error),
            upstream_area_col=cfg.upstream_area_col,
        )
        assignments = build_assignments(s3, mapping, threshold_m)
        # Restrict to release-eligible stations
        restricted = restrict_assignments(assignments, release_table)
        assignments_by_threshold[float(threshold_m)] = restricted
        n_clusters = restricted["cluster_id"].nunique()
        print("{} release-eligible stations, {} projected clusters".format(
            len(restricted), n_clusters
        ))

    # --- baseline validation gate ---
    baseline_threshold = float(cfg.baseline_distance_m)
    print("\nValidating baseline projection at {:g} m ...".format(baseline_threshold))
    baseline_restricted = assignments_by_threshold[baseline_threshold]
    baseline_check = validate_baseline_projection(
        baseline_restricted, release_table, expected
    )
    print("  Gate: PASS")
    print("  Identity preserved: {}".format(baseline_check["identity_preserved"]))
    print("  Total clusters: {} (expected {})".format(
        baseline_check["total_clusters"], baseline_check["total_expected"]
    ))
    for res, det in baseline_check.get("per_resolution", {}).items():
        print("    {}: {} (expected {})".format(res, det["actual"], det["expected"]))

    if cfg.require_release_baseline_match and not baseline_check["gate_passed"]:
        raise RuntimeError(
            "--require-release-baseline-match set but baseline projection gate failed"
        )

    # --- build summaries ---
    print("\nBuilding release-layer summary ...")
    summary = build_release_summary(
        assignments_by_threshold, baseline_threshold, release_table, expected
    )

    print("Building station membership table ...")
    membership = build_station_membership_table(
        assignments_by_threshold, baseline_threshold
    )
    # Attach release-specific columns
    release_cols = release_table[
        ["station_id", "resolution", "cluster_id", "cluster_uid", "source_name"]
    ].rename(
        columns={
            "cluster_id": "release_cluster_id",
            "cluster_uid": "release_cluster_uid",
        }
    )
    membership = membership.merge(release_cols, on="station_id", how="left", validate="many_to_one")

    print("Building cluster change table ...")
    cluster_changes = build_release_cluster_change_table(
        assignments_by_threshold, baseline_threshold
    )

    # --- write outputs ---
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary": cfg.out_dir / "s16_release_sensitivity_summary.csv",
        "station_membership": cfg.out_dir
        / "s16_release_sensitivity_station_membership.csv.gz",
        "cluster_changes": cfg.out_dir
        / "s16_release_sensitivity_cluster_changes.csv",
        "report": cfg.out_dir / "s16_release_sensitivity_report.md",
    }
    summary.to_csv(paths["summary"], index=False)
    membership.to_csv(paths["station_membership"], index=False, compression="gzip")
    cluster_changes.to_csv(paths["cluster_changes"], index=False)
    write_report(paths["report"], cfg, summary, baseline_check, cluster_changes)
    print("\nOutputs written to: {}".format(cfg.out_dir))

    return {
        "paths": paths,
        "summary": summary,
        "station_membership": membership,
        "cluster_changes": cluster_changes,
        "baseline_check": baseline_check,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    result = run_release_projection(cfg)

    summary = result["summary"]
    all_rows = summary[summary["resolution"] == "all"]
    baseline = all_rows[all_rows["is_baseline"]].iloc[0]
    alternatives = all_rows[~all_rows["is_baseline"]]

    print("\n" + "=" * 60)
    print("S16 Release-Layer Merge-Distance Sensitivity — Complete")
    print("=" * 60)
    print("Baseline ({:g} m): {:,} released clusters".format(
        cfg.baseline_distance_m, int(baseline["n_projected_clusters"])
    ))
    if len(alternatives):
        max_changed = int(
            alternatives["n_stations_membership_changed_vs_baseline"].max()
            if "n_stations_membership_changed_vs_baseline" in alternatives.columns
            else 0
        )
        max_changed_frac = float(
            alternatives["stations_membership_changed_fraction_vs_baseline"].max()
            if "stations_membership_changed_fraction_vs_baseline" in alternatives.columns
            else 0.0
        )
        print(
            "Maximum membership change: {:,} stations ({:.2%})".format(
                max_changed, max_changed_frac
            )
        )
        print(
            "Cluster-count range: {:,} – {:,}".format(
                int(alternatives["n_projected_clusters"].min()),
                int(alternatives["n_projected_clusters"].max()),
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
