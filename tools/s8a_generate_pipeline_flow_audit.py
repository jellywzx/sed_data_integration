#!/usr/bin/env python3
"""
可以输出每一步骤每个标签的数据，判断是否正确传递到下一个步骤
Generate a pipeline flow audit for the sediment basin reference pipeline.

This script is intentionally independent from s8_publish_reference_dataset.py.
It can be run after any partial pipeline execution. Missing upstream files are
reported as status=missing instead of causing the whole audit to fail.

Default outputs:
  scripts_basin_test/output/pipeline_flow_audit/
    - pipeline_stage_flow_summary.csv
    - pipeline_classification_summary.csv
    - pipeline_transition_summary.csv
    - pipeline_dataset_change_summary.csv
    - pipeline_satellite_audit_summary.csv
    - pipeline_audit_warnings.csv

Example:
  python s8a_generate_pipeline_flow_audit.py
  python s8a_generate_pipeline_flow_audit.py --root /path/to/Output_r
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


SATELLITE_TOKENS = ("riversed", "gsed", "dethier", "aquasat", "satellite", "remote")
RESOLUTIONS = ("daily", "monthly", "annual", "climatology", "other")
MAINLINE_RESOLUTIONS = ("daily", "monthly", "annual")


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def boolish(value) -> bool:
    if isinstance(value, bool):
        return value
    text = clean_text(value).lower()
    return text in {"1", "true", "yes", "y", "t"}


def source_family(source: str) -> str:
    low = clean_text(source).lower()
    if not low:
        return "unknown"
    if "usgs" in low:
        return "USGS"
    if "hydat" in low:
        return "HYDAT"
    if any(token in low for token in SATELLITE_TOKENS):
        return "satellite"
    if any(token in low for token in ("grdc", "hybam", "in situ", "insitu")):
        return "in_situ"
    if any(token in low for token in ("compiled", "compilation", "secondary")):
        return "secondary_compilation"
    return "other"


def distance_bin(value) -> str:
    try:
        x = float(value)
    except Exception:
        return "missing"
    if not math.isfinite(x):
        return "missing"
    if x <= 300:
        return "000_300m"
    if x <= 1000:
        return "300_1000m"
    if x <= 5000:
        return "1000_5000m"
    return "gt_5000m"


def cluster_size_bin(n: int) -> str:
    try:
        n = int(n)
    except Exception:
        return "missing"
    if n <= 1:
        return "1"
    if n == 2:
        return "2"
    if n <= 5:
        return "3_5"
    if n <= 10:
        return "6_10"
    return "gt_10"


def read_csv_if_exists(path: Path, **kwargs) -> Tuple[Optional[pd.DataFrame], str]:
    if not path.is_file():
        return None, "missing"
    try:
        return pd.read_csv(path, **kwargs), "ok"
    except Exception as exc:
        return None, f"read_error: {exc}"


def add_stage(rows: List[dict], stage: str, artifact: str, status: str, count_name: str, count_value,
              details: str = "") -> None:
    rows.append({
        "stage": stage,
        "artifact": artifact,
        "status": status,
        "count_name": count_name,
        "count_value": int(count_value) if pd.notna(count_value) else 0,
        "details": details,
    })


def add_class(rows: List[dict], stage: str, category_type: str, category: str, count: int,
              source_family_value: str = "", resolution: str = "", notes: str = "") -> None:
    rows.append({
        "stage": stage,
        "category_type": category_type,
        "category": clean_text(category) or "(blank)",
        "source_family": clean_text(source_family_value),
        "resolution": clean_text(resolution),
        "count": int(count),
        "notes": notes,
    })


def summarize_value_counts(df: pd.DataFrame, stage: str, column: str, class_rows: List[dict],
                           category_type: Optional[str] = None) -> None:
    if column not in df.columns:
        return
    vc = df[column].fillna("(missing)").astype(str).value_counts(dropna=False)
    for category, count in vc.items():
        add_class(class_rows, stage, category_type or column, category, int(count))


def summarize_group_counts(df: pd.DataFrame, stage: str, group_cols: Sequence[str], class_rows: List[dict],
                           category_type: str, notes: str = "") -> None:
    missing = [c for c in group_cols if c not in df.columns]
    if missing:
        return
    work = df.copy()
    for c in group_cols:
        work[c] = work[c].fillna("(missing)").astype(str)
    grouped = work.groupby(list(group_cols), dropna=False).size().reset_index(name="count")
    for _, row in grouped.iterrows():
        parts = [f"{c}={row[c]}" for c in group_cols]
        add_class(class_rows, stage, category_type, " | ".join(parts), int(row["count"]), notes=notes)


def count_organized_nc_files(organized_dir: Path) -> Tuple[Dict[str, int], int]:
    by_resolution = {}
    total = 0
    for res in RESOLUTIONS:
        folder = organized_dir / res
        count = len(list(folder.rglob("*.nc"))) if folder.is_dir() else 0
        by_resolution[res] = count
        total += count
    return by_resolution, total


def transition_row(transition: str, from_stage: str, to_stage: str, from_count, to_count,
                   status: str = "ok", details: str = "") -> dict:
    from_count = int(from_count or 0)
    to_count = int(to_count or 0)
    return {
        "transition": transition,
        "from_stage": from_stage,
        "to_stage": to_stage,
        "from_count": from_count,
        "to_count": to_count,
        "delta": to_count - from_count,
        "retention_ratio": round(to_count / from_count, 6) if from_count else np.nan,
        "status": status,
        "details": details,
    }


def add_warning(rows: List[dict], severity: str, stage: str, check: str, details: str) -> None:
    rows.append({
        "severity": severity,
        "stage": stage,
        "check": check,
        "details": details,
    })


def safe_nunique(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return int(df[col].dropna().astype(str).nunique())


def infer_paths(root: Path) -> Dict[str, Path]:
    output = root / "scripts_basin_test" / "output"
    release = output / "sed_reference_release"
    return {
        "output": output,
        "organized": (root / "../output_resolution_organized").resolve(),
        "s1": output / "s1_verify_time_resolution_results.csv",
        "s1_review": output / "s1_resolution_review_queue.csv",
        "s1_overrides": output / "s1_resolution_review_overrides.csv",
        "s2_details": output / "s2_resolution_classification_details.csv",
        "s2_other_summary": output / "s2_other_resolution_summary.csv",
        "s3": output / "s3_collected_stations.csv",
        "s4": output / "s4_upstream_basins.csv",
        "s4_area": output / "s4_reported_area_check.csv",
        "s5": output / "s5_basin_clustered_stations.csv",
        "s5_report": output / "s5_basin_cluster_report.csv",
        "s6_quality": output / "s6_cluster_quality_order.csv",
        "s7_cluster_station_catalog": output / "s7_cluster_station_catalog.csv",
        "s7_cluster_resolution_catalog": output / "s7_cluster_resolution_catalog.csv",
        "s7_source_station_catalog": output / "s7_source_station_resolution_catalog.csv",
        "release_inventory": release / "release_inventory.csv",
        "release_validation": release / "release_validation_report.csv",
        "release_station_catalog": release / "station_catalog.csv",
        "release_source_station_catalog": release / "source_station_catalog.csv",
        "release_source_dataset_catalog": release / "source_dataset_catalog.csv",
        "release_overlap_candidates": release / "sed_reference_overlap_candidates.csv.gz",
    }


def audit(root: Path, out_dir: Path) -> None:
    paths = infer_paths(root)
    stage_rows: List[dict] = []
    class_rows: List[dict] = []
    transition_rows: List[dict] = []
    change_rows: List[dict] = []
    satellite_rows: List[dict] = []
    warnings: List[dict] = []

    dfs: Dict[str, Optional[pd.DataFrame]] = {}

    # s1
    s1, status = read_csv_if_exists(paths["s1"])
    dfs["s1"] = s1
    add_stage(stage_rows, "s1", str(paths["s1"]), status, "classified_files", 0 if s1 is None else len(s1))
    if s1 is not None:
        for col in ("final_semantics", "classification_basis", "review_required", "review_reason", "consistent"):
            summarize_value_counts(s1, "s1", col, class_rows)
        if "final_semantics" in s1.columns:
            for sem, count in s1["final_semantics"].fillna("(missing)").astype(str).value_counts().items():
                change_rows.append({
                    "metric": "s1_final_semantics",
                    "group": sem,
                    "count": int(count),
                    "notes": "File-level time semantics after s1 classification.",
                })
        review_col = "review_required"
        if review_col in s1.columns:
            n_review = int(s1[review_col].map(boolish).sum())
            if n_review:
                add_warning(warnings, "error", "s1", "review_required_remaining",
                            f"{n_review} rows still require manual review before trusting s2.")

    s1_review, status_review = read_csv_if_exists(paths["s1_review"], keep_default_na=False)
    add_stage(stage_rows, "s1", str(paths["s1_review"]), status_review, "review_queue_rows",
              0 if s1_review is None else len(s1_review))
    if s1_review is not None and len(s1_review) > 0:
        add_warning(warnings, "error", "s1", "nonempty_review_queue",
                    f"{len(s1_review)} unresolved review rows found.")

    # s2
    s2, status = read_csv_if_exists(paths["s2_details"])
    dfs["s2"] = s2
    organized_counts, organized_total = count_organized_nc_files(paths["organized"])
    add_stage(stage_rows, "s2", str(paths["s2_details"]), status, "classification_detail_rows",
              0 if s2 is None else len(s2))
    add_stage(stage_rows, "s2", str(paths["organized"]), "ok" if paths["organized"].is_dir() else "missing",
              "organized_nc_files", organized_total)
    for res, count in organized_counts.items():
        add_class(class_rows, "s2", "organized_resolution_dir", res, count, resolution=res)
    if s2 is not None:
        for col in ("final_resolution_dir", "final_semantics", "s2_copy_status", "s2_attr_status"):
            summarize_value_counts(s2, "s2", col, class_rows)
        if "final_resolution_dir" in s2.columns:
            for res, count in s2["final_resolution_dir"].fillna("(missing)").astype(str).value_counts().items():
                change_rows.append({
                    "metric": "s2_final_resolution_dir",
                    "group": res,
                    "count": int(count),
                    "notes": "File-level resolution directory assigned by s2.",
                })
        if "s2_copy_status" in s2.columns:
            bad = s2[~s2["s2_copy_status"].fillna("").astype(str).str.lower().isin({"copied", "ok", "exists", "skipped_existing"})]
            if len(bad):
                add_warning(warnings, "warning", "s2", "copy_status_unusual",
                            f"{len(bad)} rows have unusual s2_copy_status.")

    # s3
    s3, status = read_csv_if_exists(paths["s3"])
    dfs["s3"] = s3
    add_stage(stage_rows, "s3", str(paths["s3"]), status, "station_rows", 0 if s3 is None else len(s3))
    if s3 is not None:
        if "source" in s3.columns:
            s3["source_family"] = s3["source"].map(source_family)
        summarize_value_counts(s3, "s3", "resolution", class_rows)
        summarize_value_counts(s3, "s3", "source_family", class_rows)
        summarize_group_counts(s3, "s3", ("source_family", "resolution"), class_rows, "source_family_by_resolution")
        if "reported_area" in s3.columns:
            add_class(class_rows, "s3", "reported_area_presence", "has_reported_area",
                      int(s3["reported_area"].notna().sum()))
            add_class(class_rows, "s3", "reported_area_presence", "missing_reported_area",
                      int(s3["reported_area"].isna().sum()))
        reach_cols = [c for c in s3.columns if c.startswith("reach_")]
        if reach_cols:
            has_hint = s3[reach_cols].notna().any(axis=1)
            add_class(class_rows, "s3", "reach_hint_presence", "has_any_reach_hint", int(has_hint.sum()))
            add_class(class_rows, "s3", "reach_hint_presence", "missing_all_reach_hints", int((~has_hint).sum()))
        if {"lat", "lon"}.issubset(s3.columns):
            bad_coord = s3[
                s3["lat"].isna() | s3["lon"].isna()
                | ~pd.to_numeric(s3["lat"], errors="coerce").between(-90, 90)
                | ~pd.to_numeric(s3["lon"], errors="coerce").between(-180, 180)
            ]
            if len(bad_coord):
                add_warning(warnings, "error", "s3", "bad_coordinates",
                            f"{len(bad_coord)} rows have missing or invalid lat/lon.")
        for col in ("source_family", "resolution"):
            if col in s3.columns:
                for cat, count in s3[col].fillna("(missing)").astype(str).value_counts().items():
                    change_rows.append({
                        "metric": f"s3_{col}",
                        "group": cat,
                        "count": int(count),
                        "notes": "Station-level distribution after s3 collection.",
                    })

    # s4
    s4, status = read_csv_if_exists(paths["s4"])
    dfs["s4"] = s4
    add_stage(stage_rows, "s4", str(paths["s4"]), status, "basin_rows", 0 if s4 is None else len(s4))
    if s4 is not None:
        for col in ("basin_status", "basin_flag", "match_quality", "point_in_local", "point_in_basin",
                    "reach_hint_used", "reach_anchor_source"):
            summarize_value_counts(s4, "s4", col, class_rows)
        if "distance_m" in s4.columns:
            bins = s4["distance_m"].map(distance_bin)
            for cat, count in bins.value_counts(dropna=False).items():
                add_class(class_rows, "s4", "distance_bin", cat, int(count))
        if "basin_status" in s4.columns:
            resolved = int(s4["basin_status"].fillna("").astype(str).str.lower().eq("resolved").sum())
            unresolved = len(s4) - resolved
            change_rows.extend([
                {"metric": "s4_basin_status", "group": "resolved", "count": resolved,
                 "notes": "Rows with publishable basin assignment."},
                {"metric": "s4_basin_status", "group": "unresolved", "count": unresolved,
                 "notes": "Rows kept as observations but basin assignment is not publishable."},
            ])
            if unresolved:
                add_warning(warnings, "info", "s4", "unresolved_basin_rows",
                            f"{unresolved} rows are unresolved and should not publish basin polygons.")
        if "station_id" in s4.columns:
            duplicated = int(s4["station_id"].duplicated().sum())
            if duplicated:
                add_warning(warnings, "error", "s4", "duplicated_station_id",
                            f"{duplicated} duplicated station_id rows in s4 output.")

    s4_area, status = read_csv_if_exists(paths["s4_area"])
    add_stage(stage_rows, "s4", str(paths["s4_area"]), status, "reported_area_check_rows",
              0 if s4_area is None else len(s4_area))
    if s4_area is not None:
        summarize_value_counts(s4_area, "s4_area_check", "match_quality", class_rows)
        if "area_error" in s4_area.columns:
            area_error = pd.to_numeric(s4_area["area_error"], errors="coerce")
            large = int((area_error.abs() > 0.5).sum())
            add_class(class_rows, "s4_area_check", "area_error_abs_gt_0_5", "true", large)
            if large:
                add_warning(warnings, "warning", "s4", "large_reported_area_error",
                            f"{large} reported-area rows have abs(log10(MERIT/reported)) > 0.5.")

    # s5
    s5, status = read_csv_if_exists(paths["s5"])
    dfs["s5"] = s5
    add_stage(stage_rows, "s5", str(paths["s5"]), status, "clustered_station_rows", 0 if s5 is None else len(s5))
    if s5 is not None:
        if "source" in s5.columns:
            s5["source_family"] = s5["source"].map(source_family)
        n_clusters = safe_nunique(s5, "cluster_id")
        add_stage(stage_rows, "s5", str(paths["s5"]), status, "unique_clusters", n_clusters)
        summarize_value_counts(s5, "s5", "resolution", class_rows)
        summarize_value_counts(s5, "s5", "source_family", class_rows)
        summarize_value_counts(s5, "s5", "basin_status", class_rows)
        summarize_group_counts(s5, "s5", ("source_family", "resolution"), class_rows, "source_family_by_resolution")
        if "cluster_id" in s5.columns:
            g = s5.groupby("cluster_id", dropna=False).agg(
                n_rows=("cluster_id", "size"),
                n_sources=("source", "nunique") if "source" in s5.columns else ("cluster_id", "size"),
                n_resolutions=("resolution", "nunique") if "resolution" in s5.columns else ("cluster_id", "size"),
                n_source_families=("source_family", "nunique") if "source_family" in s5.columns else ("cluster_id", "size"),
            ).reset_index()
            for cat, count in g["n_rows"].map(cluster_size_bin).value_counts().items():
                add_class(class_rows, "s5", "cluster_size_bin", cat, int(count))
            add_class(class_rows, "s5", "cluster_merge", "multi_station_clusters", int((g["n_rows"] > 1).sum()))
            add_class(class_rows, "s5", "cluster_merge", "single_station_clusters", int((g["n_rows"] == 1).sum()))
            add_class(class_rows, "s5", "cluster_merge", "multi_source_clusters", int((g["n_sources"] > 1).sum()))
            add_class(class_rows, "s5", "cluster_merge", "multi_resolution_clusters", int((g["n_resolutions"] > 1).sum()))
            change_rows.extend([
                {"metric": "s5_unique_clusters", "group": "all", "count": n_clusters,
                 "notes": "Number of basin clusters after s5 merge."},
                {"metric": "s5_multi_station_clusters", "group": "cluster_count", "count": int((g["n_rows"] > 1).sum()),
                 "notes": "Clusters containing multiple s3 station rows."},
                {"metric": "s5_multi_source_clusters", "group": "cluster_count", "count": int((g["n_sources"] > 1).sum()),
                 "notes": "Clusters containing multiple source names."},
            ])

            if "source_family" in s5.columns:
                mix = s5.groupby("cluster_id")["source_family"].agg(lambda x: set(x.dropna().astype(str))).reset_index()
                mix["has_satellite"] = mix["source_family"].map(lambda s: "satellite" in s)
                mix["has_non_satellite"] = mix["source_family"].map(lambda s: any(v != "satellite" for v in s))
                sat_only = int((mix["has_satellite"] & ~mix["has_non_satellite"]).sum())
                sat_mixed = int((mix["has_satellite"] & mix["has_non_satellite"]).sum())
                satellite_rows.extend([
                    {"stage": "s5", "metric": "satellite_only_clusters", "count": sat_only,
                     "details": "Clusters whose sources are all classified as satellite."},
                    {"stage": "s5", "metric": "satellite_mixed_with_non_satellite_clusters", "count": sat_mixed,
                     "details": "Clusters containing both satellite and non-satellite source families."},
                ])
                if sat_mixed:
                    add_warning(warnings, "warning", "s5", "satellite_in_situ_mixed_clusters",
                                f"{sat_mixed} clusters contain both satellite and non-satellite sources.")

    s5_report, status = read_csv_if_exists(paths["s5_report"])
    add_stage(stage_rows, "s5", str(paths["s5_report"]), status, "cluster_report_rows",
              0 if s5_report is None else len(s5_report))

    # s6 quality order
    q, status = read_csv_if_exists(paths["s6_quality"])
    dfs["s6_quality"] = q
    add_stage(stage_rows, "s6", str(paths["s6_quality"]), status, "quality_order_rows", 0 if q is None else len(q))
    if q is not None:
        if "source" in q.columns:
            q["source_family"] = q["source"].map(source_family)
        summarize_value_counts(q, "s6_quality", "resolution", class_rows)
        summarize_value_counts(q, "s6_quality", "source_family", class_rows)
        summarize_value_counts(q, "s6_quality", "is_top_ranked", class_rows)
        summarize_group_counts(q, "s6_quality", ("source_family", "resolution"), class_rows,
                               "source_family_by_resolution")
        if "is_top_ranked" in q.columns:
            top = q[q["is_top_ranked"].map(boolish)]
            add_stage(stage_rows, "s6", str(paths["s6_quality"]), status, "top_ranked_candidate_rows", len(top))
            if "source_family" in top.columns:
                for fam, count in top["source_family"].fillna("(missing)").astype(str).value_counts().items():
                    add_class(class_rows, "s6_quality", "top_ranked_source_family", fam, int(count))
                sat_top = int(top["source_family"].eq("satellite").sum())
                satellite_rows.append({
                    "stage": "s6",
                    "metric": "top_ranked_satellite_rows",
                    "count": sat_top,
                    "details": "Rows where a satellite source is selected as top-ranked candidate.",
                })
                if sat_top:
                    add_warning(warnings, "warning", "s6", "satellite_top_ranked",
                                f"{sat_top} s6 quality-order rows are top-ranked satellite rows.")
        if "n_candidates" in q.columns:
            n_candidates = pd.to_numeric(q["n_candidates"], errors="coerce")
            add_class(class_rows, "s6_quality", "candidate_groups_multi_candidate_rows",
                      "n_candidates_gt_1", int((n_candidates > 1).sum()))

    # s7 catalogs
    for key, label in (
        ("s7_cluster_station_catalog", "s7_cluster_station_catalog"),
        ("s7_cluster_resolution_catalog", "s7_cluster_resolution_catalog"),
        ("s7_source_station_catalog", "s7_source_station_catalog"),
    ):
        df, status = read_csv_if_exists(paths[key])
        dfs[key] = df
        add_stage(stage_rows, "s7", str(paths[key]), status, f"{label}_rows", 0 if df is None else len(df))
        if df is not None:
            if "resolution" in df.columns:
                summarize_value_counts(df, "s7", "resolution", class_rows)
            if "basin_status" in df.columns:
                summarize_value_counts(df, "s7", "basin_status", class_rows)
            if "source" in df.columns:
                df["source_family"] = df["source"].map(source_family)
                summarize_value_counts(df, "s7", "source_family", class_rows)

    # s8 release
    for key, label in (
        ("release_inventory", "release_inventory_rows"),
        ("release_validation", "release_validation_rows"),
        ("release_station_catalog", "release_station_catalog_rows"),
        ("release_source_station_catalog", "release_source_station_catalog_rows"),
        ("release_source_dataset_catalog", "release_source_dataset_catalog_rows"),
    ):
        df, status = read_csv_if_exists(paths[key])
        dfs[key] = df
        add_stage(stage_rows, "s8", str(paths[key]), status, label, 0 if df is None else len(df))
        if df is not None:
            if key == "release_inventory" and "kind" in df.columns:
                summarize_value_counts(df, "s8", "kind", class_rows)
            if key == "release_validation" and "status" in df.columns:
                summarize_value_counts(df, "s8", "status", class_rows)
                failed = int(df["status"].fillna("").astype(str).str.lower().eq("fail").sum())
                if failed:
                    add_warning(warnings, "error", "s8", "release_validation_failures",
                                f"{failed} release validation checks failed.")
            if key in {"release_source_station_catalog", "release_source_dataset_catalog"} and "source" in df.columns:
                df["source_family"] = df["source"].map(source_family)
                summarize_value_counts(df, "s8", "source_family", class_rows)

    overlap_path = paths["release_overlap_candidates"]
    if overlap_path.is_file():
        try:
            # Count rows cheaply without loading full large file into memory.
            overlap_count = sum(1 for _ in pd.read_csv(overlap_path, chunksize=250000))
            # The line above counts chunks, not rows; use chunk lengths.
            overlap_count = 0
            for chunk in pd.read_csv(overlap_path, chunksize=250000, usecols=["source_family"]):
                overlap_count += len(chunk)
                vc = chunk["source_family"].fillna("(missing)").astype(str).value_counts()
                for fam, count in vc.items():
                    add_class(class_rows, "s8_overlap_candidates", "source_family", fam, int(count))
            add_stage(stage_rows, "s8", str(overlap_path), "ok", "overlap_candidate_rows", overlap_count)
        except Exception as exc:
            add_stage(stage_rows, "s8", str(overlap_path), f"read_error: {exc}", "overlap_candidate_rows", 0)
    else:
        add_stage(stage_rows, "s8", str(overlap_path), "missing", "overlap_candidate_rows", 0)

    # Transitions
    s1_count = 0 if s1 is None else len(s1)
    s2_count = 0 if s2 is None else len(s2)
    s3_count = 0 if s3 is None else len(s3)
    s4_count = 0 if s4 is None else len(s4)
    s5_count = 0 if s5 is None else len(s5)
    s5_clusters = safe_nunique(s5, "cluster_id") if s5 is not None else 0
    q_count = 0 if q is None else len(q)
    q_top_count = 0
    if q is not None and "is_top_ranked" in q.columns:
        q_top_count = int(q["is_top_ranked"].map(boolish).sum())

    transition_rows.extend([
        transition_row("s1_classified_to_s2_details", "s1", "s2", s1_count, s2_count,
                       details="File-level classification rows compared to s2 detail rows."),
        transition_row("s2_organized_nc_to_s3_station_rows", "s2", "s3", organized_total, s3_count,
                       details="Organized .nc files compared to collected station rows."),
        transition_row("s3_station_rows_to_s4_basin_rows", "s3", "s4", s3_count, s4_count,
                       details="s3 station rows compared to s4 basin rows."),
        transition_row("s4_basin_rows_to_s5_station_rows", "s4", "s5", s4_count, s5_count,
                       details="s4 basin rows compared to s5 clustered station rows."),
        transition_row("s5_station_rows_to_s5_clusters", "s5", "s5", s5_count, s5_clusters,
                       details="Many-to-one basin cluster merge."),
        transition_row("s5_clusters_to_s6_top_ranked_groups", "s5", "s6", s5_clusters, q_top_count,
                       details="Approximate comparison: clusters vs top-ranked cluster-resolution candidates."),
        transition_row("s6_quality_candidates_to_s6_top_ranked", "s6", "s6", q_count, q_top_count,
                       details="Candidate rows compared to top-ranked selected rows in s6 quality order."),
    ])

    # Transition warnings
    if s3 is not None and s4 is not None and "station_id" in s4.columns:
        expected = set(range(len(s3)))
        actual = set(pd.to_numeric(s4["station_id"], errors="coerce").dropna().astype(int))
        missing = len(expected - actual)
        extra = len(actual - expected)
        if missing or extra:
            add_warning(warnings, "error", "s3_to_s4", "station_id_mismatch",
                        f"missing_station_ids={missing}, extra_station_ids={extra}.")
    if s3 is not None and s5 is not None and "station_id" in s5.columns:
        expected = set(range(len(s3)))
        actual = set(pd.to_numeric(s5["station_id"], errors="coerce").dropna().astype(int))
        missing = len(expected - actual)
        extra = len(actual - expected)
        if missing or extra:
            add_warning(warnings, "error", "s3_to_s5", "station_id_mismatch",
                        f"missing_station_ids={missing}, extra_station_ids={extra}.")

    # Dataset change summary: compact high-level counts
    if s3 is not None:
        change_rows.append({"metric": "s3_station_rows", "group": "all", "count": len(s3),
                            "notes": "Station rows entering basin mainline."})
    if s4 is not None:
        change_rows.append({"metric": "s4_basin_rows", "group": "all", "count": len(s4),
                            "notes": "Basin matching rows."})
    if s5 is not None:
        change_rows.append({"metric": "s5_station_rows", "group": "all", "count": len(s5),
                            "notes": "Station rows after cluster assignment."})
        change_rows.append({"metric": "s5_cluster_count", "group": "all", "count": s5_clusters,
                            "notes": "Unique cluster_id count after s5."})
    if q is not None:
        change_rows.append({"metric": "s6_quality_candidate_rows", "group": "all", "count": len(q),
                            "notes": "Candidate rows used for quality-order selection."})
        change_rows.append({"metric": "s6_quality_top_ranked_rows", "group": "all", "count": q_top_count,
                            "notes": "Rows selected as top-ranked candidates."})

    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(stage_rows).to_csv(out_dir / "pipeline_stage_flow_summary.csv", index=False)
    pd.DataFrame(class_rows).to_csv(out_dir / "pipeline_classification_summary.csv", index=False)
    pd.DataFrame(transition_rows).to_csv(out_dir / "pipeline_transition_summary.csv", index=False)
    pd.DataFrame(change_rows).to_csv(out_dir / "pipeline_dataset_change_summary.csv", index=False)
    pd.DataFrame(satellite_rows).to_csv(out_dir / "pipeline_satellite_audit_summary.csv", index=False)
    pd.DataFrame(warnings).to_csv(out_dir / "pipeline_audit_warnings.csv", index=False)

    print(f"Wrote pipeline flow audit to: {out_dir}")
    print("Key files:")
    print(f"  - {out_dir / 'pipeline_stage_flow_summary.csv'}")
    print(f"  - {out_dir / 'pipeline_classification_summary.csv'}")
    print(f"  - {out_dir / 'pipeline_transition_summary.csv'}")
    print(f"  - {out_dir / 'pipeline_dataset_change_summary.csv'}")
    print(f"  - {out_dir / 'pipeline_satellite_audit_summary.csv'}")
    print(f"  - {out_dir / 'pipeline_audit_warnings.csv'}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate sediment pipeline flow audit CSVs.")
    parser.add_argument(
        "--root",
        default=None,
        help="Output_r root. Default: parent of this script's directory, matching pipeline_paths.get_output_r_root().",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Audit output directory. Default: scripts_basin_test/output/pipeline_flow_audit under root.",
    )
    args = parser.parse_args()

    if args.root:
        root = Path(args.root).expanduser().resolve()
    else:
        root = Path(__file__).resolve().parent.parent.resolve()

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else (
        root / "scripts_basin_test" / "output" / "pipeline_flow_audit"
    )

    audit(root=root, out_dir=out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
