#!/usr/bin/env python3
"""Experimental S4 rerun: minimize upstream-area relative error when provided.

INPUT: existing read-only S3 + S4 CSV, independent-area evidence, MERIT Hydro.
OUTPUT: a COMPLETE S4-compatible CSV in an isolated experiment sandbox,
        plus A/B/C/D comparison, all-candidate-derived top-10 and provenance.

The original S4 rows are retained for all stations without eligible reported
area and for reach-hint products (GSED / RiverSed). For eligible rows, re-run
candidate discovery and optionally basin polygon tracing using the repository's
UpstreamBasinTracer, never using pre-sorted S16 top-five as candidates.

This is NOT a publication-ready S4 result. 'resolved' under the experimental
area-only rule requires independent evidence and geometry/topology QA.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from experimental.area_min_error_selector import (  # noqa: E402
    choose_from_tracer,
    positive_finite,
    relative_area_error,
    select_area_geometry_priority,
)
from experimental.area_min_error_policy import (  # noqa: E402
    REACH_HINT_SOURCES,
    area_policy,
    geometry_priority_policy,
)

OUTPUT_R_ROOT = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r")
PRODUCTION_OUTPUT = OUTPUT_R_ROOT / "scripts_basin_test" / "output"
DEFAULT_S3 = PRODUCTION_OUTPUT / "s3_collected_stations.csv"
DEFAULT_S4 = PRODUCTION_OUTPUT / "s4_upstream_basins.csv"
DEFAULT_MERIT = Path("/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1")
DEFAULT_AREAS = REPO_ROOT / "config" / "amazon_area_evidence.csv"
DEFAULT_OUT = OUTPUT_R_ROOT / "experiments" / "area_min_error_20261023"
DEFAULT_D_COMPARISON = DEFAULT_OUT / "reports" / "station_comparison.csv"
S4_BASENAME = "s4_upstream_basins.csv"
CANDIDATE_BASENAME = "candidate_top10.csv"
SCENARIO_KEYS = (
    ("A_production_S4", "A_status", "baseline_distance_m"),
    ("B_area_select_keep_1km", "B_status", "new_distance_m"),
    ("C_old_select_no_1km", "C_status", "baseline_distance_m"),
    ("D_area_select_no_1km", "D_status", "new_distance_m"),
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--s3-csv", type=Path, default=DEFAULT_S3)
    p.add_argument("--s4-csv", type=Path, default=DEFAULT_S4)
    p.add_argument("--merit-dir", type=Path, default=DEFAULT_MERIT)
    p.add_argument("--manual-areas", type=Path, default=DEFAULT_AREAS)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    p.add_argument("--area-threshold", type=float, default=0.10)
    p.add_argument("--ambiguity-gap", type=float, default=0.01, help="Absolute relative-error gap for review only")
    p.add_argument("--ambiguity-distance-m", type=float, default=5000.0)
    p.add_argument("--candidate-top", type=int, default=10)
    p.add_argument("--max-stations", type=int, default=0, help="0 means no cap")
    p.add_argument("--retrace-geometry", action="store_true", help="Trace upstream polygon for changed COMIDs; can be expensive")
    p.add_argument("--dry-run", action="store_true", help="Validate inputs and list eligible stations; no writes or MERIT reads")
    p.add_argument("--overwrite", action="store_true", help="Overwrite ONLY named outputs in the isolated experiment directory")
    p.add_argument("--geometry-priority", action="store_true", help="Run only E and write the isolated D/E comparison")
    p.add_argument("--d-comparison", type=Path, default=DEFAULT_D_COMPARISON)
    p.add_argument("--basin-check-limit", type=int, default=10)
    return p


def as_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def source_id(value: object) -> str:
    value = as_text(value)
    return value[:-2] if value.endswith(".0") else value


def as_bool(value: object) -> bool:
    return as_text(value).lower() in {"true", "t", "yes", "y", "1"}


def valid_comid(value: object):
    number = positive_finite(value)
    return int(number) if number is not None and number.is_integer() else None


def safe_num(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ensure_unique_key(frame: pd.DataFrame, name: str) -> pd.DataFrame:
    if "station_key" not in frame or "station_id" not in frame:
        raise ValueError(f"{name} requires station_key and station_id")
    work = frame.copy()
    work["station_key"] = work["station_key"].fillna("").astype(str).str.strip()
    if (work["station_key"] == "").any() or work["station_key"].duplicated().any():
        raise ValueError(f"{name}: missing or duplicated station_key")
    if pd.to_numeric(work["station_id"], errors="coerce").isna().any():
        raise ValueError(f"{name}: invalid station_id")
    return work


def load_inputs(s3_path: Path, s4_path: Path, manual_path: Path):
    for p in (s3_path, s4_path, manual_path):
        if not p.is_file():
            raise FileNotFoundError(p)
    s3 = ensure_unique_key(
        pd.read_csv(s3_path, dtype={"station_key": "string", "source_station_id": "string"}, low_memory=False), "S3"
    )
    s4 = ensure_unique_key(
        pd.read_csv(s4_path, dtype={"station_key": "string", "source_station_id": "string"}, low_memory=False), "S4"
    )
    for name, frame, fields in (
        ("S3", s3, ("source", "source_station_id", "lon", "lat")),
        ("S4", s4, ("basin_id", "distance_m", "uparea_merit", "match_quality", "basin_status", "basin_flag")),
    ):
        absent = set(fields) - set(frame.columns)
        if absent:
            raise ValueError(f"{name} missing columns: {sorted(absent)}")
    s3_keys, s4_keys = set(s3["station_key"]), set(s4["station_key"])
    if s3_keys != s4_keys:
        raise ValueError(f"S3/S4 station_key sets disagree: S3_only={len(s3_keys - s4_keys)}, S4_only={len(s4_keys - s3_keys)}")
    ids = s3[["station_key", "station_id"]].merge(s4[["station_key", "station_id"]], on="station_key", validate="one_to_one")
    if not np.array_equal(pd.to_numeric(ids["station_id_x"]).to_numpy(), pd.to_numeric(ids["station_id_y"]).to_numpy()):
        raise ValueError("S3/S4 station_id mismatch; use outputs from the same run")
    if {"lon", "lat"}.issubset(s4.columns):
        coords = s3[["station_key", "lon", "lat"]].merge(
            s4[["station_key", "lon", "lat"]], on="station_key", validate="one_to_one"
        )
        for coord in ("lon", "lat"):
            s3_values = pd.to_numeric(coords[coord + "_x"], errors="coerce").to_numpy(dtype=float)
            s4_values = pd.to_numeric(coords[coord + "_y"], errors="coerce").to_numpy(dtype=float)
            if not np.isclose(s3_values, s4_values, rtol=0, atol=1e-10, equal_nan=True).all():
                raise ValueError(f"S3/S4 {coord} mismatch; use outputs from the same run")

    manual = pd.read_csv(manual_path, dtype={"source_station_id": "string"}, keep_default_na=False)
    required = {"source", "source_station_id", "reported_area_km2", "evidence_verified"}
    if required - set(manual.columns):
        raise ValueError(f"Manual area evidence missing columns: {sorted(required - set(manual.columns))}")
    manual["source"] = manual["source"].astype(str).str.strip().str.lower()
    manual["source_station_id"] = manual["source_station_id"].map(source_id)
    if manual.duplicated(subset=["source", "source_station_id"]).any():
        raise ValueError("Duplicate source/source_station_id in manual area evidence")
    manual["reported_area_km2"] = pd.to_numeric(manual["reported_area_km2"], errors="coerce")
    return s3, s4, manual


def add_area_metadata(s3: pd.DataFrame, manual: pd.DataFrame) -> pd.DataFrame:
    work = s3.copy()
    work["source"] = work["source"].map(as_text)
    work["source_station_id"] = work["source_station_id"].map(source_id)
    work["_source_match"] = work["source"].str.lower()
    manual_columns = ["source", "source_station_id", "reported_area_km2", "evidence_verified"]
    for optional in ("evidence_kind", "reference"):
        if optional in manual.columns:
            manual_columns.append(optional)
    manual = manual[manual_columns].rename(columns={
        "source": "_source_match",
        "reported_area_km2": "_manual_reported_area_km2",
        "evidence_verified": "_manual_verified",
        "evidence_kind": "_manual_evidence_kind",
        "reference": "_manual_reference",
    })
    work = work.merge(manual, on=["_source_match", "source_station_id"], how="left", validate="many_to_one")
    if "reported_area" not in work:
        work["reported_area"] = math.nan
    s3_area = pd.to_numeric(work["reported_area"], errors="coerce")
    manual_area = pd.to_numeric(work["_manual_reported_area_km2"], errors="coerce")
    has_s3 = np.isfinite(s3_area) & (s3_area > 0)
    has_manual = np.isfinite(manual_area) & (manual_area > 0)
    # Prefer native S3 reported_area, use external CSV only to fill missing area.
    work["reference_area_km2"] = s3_area.where(has_s3, manual_area.where(has_manual, math.nan))
    work["evidence_origin"] = np.where(has_s3, "s3_reported_area", np.where(has_manual, "manual_area_fallback", "missing"))
    manual_verified = work["_manual_verified"].map(as_bool)
    work["evidence_verified"] = np.where(has_s3, False, manual_verified)
    work["evidence_reference"] = np.where(has_s3, "S3 original source area: verify units/provenance", work.get("_manual_reference", ""))
    work["manual_s3_disagreement"] = has_s3 & has_manual & ((s3_area - manual_area).abs() / s3_area > 0.01)
    return work


def select_targets(metadata: pd.DataFrame, max_stations: int) -> pd.DataFrame:
    work = metadata.copy()
    work = work[~work["source"].str.lower().isin(REACH_HINT_SOURCES)].copy()
    work = work[pd.to_numeric(work["reference_area_km2"], errors="coerce").gt(0)].copy()
    work = work.sort_values(["source", "source_station_id", "station_key"], kind="mergesort")
    if max_stations > 0:
        work = work.head(max_stations)
    if work.empty:
        raise ValueError("No eligible reported-area stations")
    return work


def legacy_classification(reach, source, local=False, basin=False):
    from basin_policy import classify_basin_result
    if reach is None:
        return "unresolved", "no_match"
    return classify_basin_result(
        basin_id=reach.get("COMID"), match_quality=reach.get("match_quality", "failed"),
        distance_m=reach.get("distance"), source_name=source,
        point_in_local=local, point_in_basin=basin,
    )


def baseline_reach(row: pd.Series) -> dict:
    return {
        "COMID": valid_comid(row.get("basin_id")),
        "uparea": safe_num(row.get("uparea_merit")),
        "distance": safe_num(row.get("distance_m")),
        "pfaf_code": as_text(row.get("pfaf_code")),
        "match_quality": as_text(row.get("match_quality")),
        "area_error": safe_num(row.get("area_error")),
    }


def empty_reach_update():
    return {
        "basin_id": math.nan, "basin_area": math.nan, "uparea_merit": math.nan,
        "distance_m": math.nan, "pfaf_code": "", "area_error": math.nan,
        "match_quality": "failed", "basin_status": "unresolved", "basin_flag": "no_match",
        "method": "no_valid_candidate", "point_in_local": "", "point_in_basin": "",
        "n_upstream_reaches": math.nan,
    }


def compare_one(station: pd.Series, s4_row: pd.Series, tracer, args):
    area = float(station["reference_area_km2"])
    lon, lat = safe_num(station["lon"]), safe_num(station["lat"])
    source = as_text(station["source"])
    key = as_text(station["station_key"])
    if not (math.isfinite(lon) and -180 <= lon <= 180 and math.isfinite(lat) and -90 <= lat <= 90):
        raise ValueError(f"Invalid lon/lat for station_key={key}")

    old_reach = baseline_reach(s4_row)
    old_status = as_text(s4_row.get("basin_status"))
    old_flag = as_text(s4_row.get("basin_flag"))
    old_local, old_basin = as_bool(s4_row.get("point_in_local")), as_bool(s4_row.get("point_in_basin"))
    old_method = as_text(s4_row.get("method"))

    old_accepted = area_policy(
        reach=old_reach, reported_area=area, source=source,
        threshold=args.area_threshold,
        evidence_verified=bool(station["evidence_verified"]),
        geometry_rechecked=True, geometry_method=old_method,
        point_in_local=old_local, point_in_basin=old_basin,
    )
    selected = choose_from_tracer(
        tracer, lon, lat, area,
        ambiguity_gap=args.ambiguity_gap,
        ambiguity_distance_m=args.ambiguity_distance_m,
    )
    fresh = selected.reach_info
    changed = (fresh is not None and fresh["COMID"] != old_reach["COMID"])
    geometry_checked = False
    geom = {}
    if fresh is not None:
        if fresh["COMID"] == old_reach["COMID"]:
            geometry_checked = True
            geom = {
                "method": old_method,
                "point_in_local": old_local,
                "point_in_basin": old_basin,
                "n_upstream_reaches": s4_row.get("n_upstream_reaches", math.nan),
            }
        elif args.retrace_geometry:
            geom = tracer.get_upstream_basin_from_reach(lon, lat, fresh)
            geometry_checked = True

    new_local = geom.get("point_in_local") if geometry_checked else None
    new_basin = geom.get("point_in_basin") if geometry_checked else None
    new_method = as_text(geom.get("method")) if geometry_checked else "not_retraced"
    B_status, B_flag = legacy_classification(fresh, source, local=new_local is True, basin=new_basin is True)
    D = area_policy(
        reach=fresh, reported_area=area, source=source,
        threshold=args.area_threshold, evidence_verified=bool(station["evidence_verified"]),
        geometry_rechecked=geometry_checked, geometry_method=new_method,
        point_in_local=new_local, point_in_basin=new_basin,
        ambiguous=selected.ambiguous,
    )

    update = empty_reach_update()
    if fresh is not None:
        # Preserve original S4 legacy area_error as signed log10; experimental
        # absolute relative error is an additional column below.
        update.update({
            "basin_id": fresh["COMID"],
            "basin_area": fresh["uparea"],
            "uparea_merit": fresh["uparea"],
            "distance_m": fresh["distance"],
            "pfaf_code": fresh["pfaf_code"],
            "area_error": fresh["area_error"],
            "match_quality": "area_matched" if D.area_rel_error <= args.area_threshold else "area_mismatch",
            "basin_status": D.status,
            "basin_flag": D.flag,
            "method": new_method,
            "point_in_local": new_local if new_local is not None else "",
            "point_in_basin": new_basin if new_basin is not None else "",
            "n_upstream_reaches": geom.get("n_upstream_reaches", math.nan),
        })
    # Keep station identity and original coordinates unchanged.
    update["reported_area"] = area
    update["experimental_area_rel_error"] = D.area_rel_error
    update["experimental_selection_rule"] = "min_relative_upstream_area_error"
    update["experimental_ambiguous"] = selected.ambiguous
    update["experimental_nearby_competitors"] = selected.nearby_competitors
    update["experimental_geometry_rechecked"] = geometry_checked
    update["experimental_evidence_origin"] = as_text(station["evidence_origin"])
    update["experimental_evidence_verified"] = bool(station["evidence_verified"])
    review = D.review_reasons
    if bool(station["manual_s3_disagreement"]):
        review = ";".join(filter(None, [review, "s3_manual_area_disagreement_gt_1pct"]))
    update["experimental_review_required"] = bool(review)
    update["experimental_review_reason"] = review

    comparison = {
        "station_key": key, "station_id": station["station_id"],
        "source": source, "source_station_id": as_text(station["source_station_id"]),
        "station_name": as_text(station.get("source_station_name", station.get("station_name", ""))),
        "lon": lon, "lat": lat, "reported_area_km2": area,
        "evidence_origin": as_text(station["evidence_origin"]),
        "evidence_verified": bool(station["evidence_verified"]),
        "manual_s3_disagreement": bool(station["manual_s3_disagreement"]),
        "baseline_comid": old_reach["COMID"],
        "baseline_area_km2": old_reach["uparea"],
        "baseline_distance_m": old_reach["distance"],
        "A_status": old_status, "A_flag": old_flag,
        "B_status": B_status, "B_flag": B_flag,
        "C_status": old_accepted.status, "C_flag": old_accepted.flag,
        "D_status": D.status, "D_flag": D.flag,
        "new_comid": fresh["COMID"] if fresh else math.nan,
        "new_area_km2": fresh["uparea"] if fresh else math.nan,
        "new_distance_m": fresh["distance"] if fresh else math.nan,
        "new_area_rel_error": D.area_rel_error,
        "comid_changed": changed,
        "geometry_rechecked": geometry_checked,
        "geometry_method": new_method,
        "point_in_local": new_local if new_local is not None else "",
        "point_in_basin": new_basin if new_basin is not None else "",
        "ambiguous": selected.ambiguous,
        "nearby_competitors": selected.nearby_competitors,
        "review_required": bool(review), "review_reasons": review,
        "candidate_count": len(selected.ranked),
    }
    candidates = []
    for rank, c in enumerate(selected.ranked.head(args.candidate_top).itertuples(index=False), 1):
        candidates.append({
            "station_key": key, "source": source, "source_station_id": as_text(station["source_station_id"]),
            "rank_area_only": rank, "COMID": int(c.COMID), "uparea_km2": float(c.uparea),
            "dist_m": float(c.dist_m), "area_rel_error": float(c.area_rel_error),
            "legacy_combined_score": float(c.legacy_combined_score),
            "pfaf_code": str(c.pfaf_code),
        })
    return update, comparison, candidates


def scenario_summary(comparison: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for label, status_col, distance_col in SCENARIO_KEYS:
        accepted = comparison[status_col].eq("resolved")
        distances = pd.to_numeric(comparison.loc[accepted, distance_col], errors="coerce")
        rows.append({
            "scenario": label,
            "evaluated": len(comparison),
            "resolved": int(accepted.sum()),
            "unresolved": int((~accepted).sum()),
            "resolved_fraction": round(float(accepted.mean()), 6),
            "accepted_over_1km": int((distances > 1000).sum()),
            "accepted_over_10km": int((distances > 10000).sum()),
            "accepted_dist_p50_m": float(distances.median()) if len(distances) else math.nan,
        })
    return pd.DataFrame(rows)


def compare_geometry_one(station: pd.Series, d_row: pd.Series, tracer, args):
    area = float(station["reference_area_km2"])
    lon, lat = safe_num(station["lon"]), safe_num(station["lat"])
    source = as_text(station["source"])
    selected = select_area_geometry_priority(
        tracer, lon, lat, area,
        threshold=args.area_threshold,
        basin_check_limit=args.basin_check_limit,
        ambiguity_gap=args.ambiguity_gap,
    )
    reach = selected.reach_info
    decision = geometry_priority_policy(
        reach=reach, reported_area=area, source=source,
        threshold=args.area_threshold,
        evidence_verified=bool(station["evidence_verified"]),
        ambiguous=selected.ambiguous,
        selection_complete=selected.selection_complete,
    )
    row = {
        "station_key": as_text(station["station_key"]),
        "source": source,
        "source_station_id": as_text(station["source_station_id"]),
        "station_name": as_text(station.get("source_station_name", station.get("station_name", ""))),
        "reported_area_km2": area,
        "evidence_origin": as_text(station["evidence_origin"]),
        "evidence_verified": bool(station["evidence_verified"]),
        "D_comid": valid_comid(d_row.get("new_comid")),
        "E_comid": reach["COMID"] if reach else math.nan,
        "D_area_rel_error": safe_num(d_row.get("new_area_rel_error")),
        "E_area_rel_error": decision.area_rel_error,
        "D_distance_m": safe_num(d_row.get("new_distance_m")),
        "E_distance_m": reach["distance"] if reach else math.nan,
        "D_point_in_local": d_row.get("point_in_local", ""),
        "E_point_in_local": reach.get("point_in_local") if reach else "",
        "D_point_in_basin": d_row.get("point_in_basin", ""),
        "E_point_in_basin": reach.get("point_in_basin") if reach else "",
        "D_basin_status": as_text(d_row.get("D_status")),
        "E_basin_status": decision.status,
        "E_geometry_method": reach.get("geometry_method", "") if reach else "",
        "E_geometry_priority": reach.get("geometry_priority", math.nan) if reach else math.nan,
        "E_ambiguous": selected.ambiguous,
        "E_review_required": decision.review_required,
        "E_review_reasons": decision.review_reasons,
        "E_competitor_comids": ";".join(map(str, selected.competitor_comids)),
        "E_selection_complete": selected.selection_complete,
        "E_local_checked_count": selected.local_checked_count,
        "E_local_unchecked_count": selected.local_unchecked_count,
        "E_basin_checked_count": selected.basin_checked_count,
        "E_basin_unchecked_count": selected.basin_unchecked_count,
        "E_area_qualified_count": len(selected.candidates),
    }
    candidate_rows = []
    for candidate in selected.candidates.itertuples(index=False):
        candidate_rows.append({
            "station_key": row["station_key"],
            "source_station_id": row["source_station_id"],
            "rank_geometry_priority": int(candidate.rank_geometry_priority),
            "COMID": int(candidate.COMID),
            "uparea_km2": float(candidate.uparea),
            "area_rel_error": float(candidate.area_rel_error),
            "distance_m": float(candidate.dist_m),
            "geometry_priority": int(candidate.geometry_priority),
            "point_in_local": candidate.point_in_local,
            "local_check_status": candidate.local_check_status,
            "point_in_basin": candidate.point_in_basin,
            "basin_check_status": candidate.basin_check_status,
            "geometry_method": candidate.geometry_method,
            "selected_E": int(candidate.COMID) == row["E_comid"],
            "ambiguity_competitor": int(candidate.COMID) in selected.competitor_comids,
        })
    return row, candidate_rows


def geometry_output_paths(out_root: Path) -> dict:
    directory = out_root / "geometry_priority"
    return {
        "comparison": directory / "station_comparison_D_E.csv",
        "candidates": directory / "candidate_geometry_comparison.csv",
        "summary": directory / "scenario_summary_D_E.csv",
        "report": directory / "geometry_priority_report.md",
    }


def geometry_summary(comparison: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for scenario in ("D", "E"):
        status = comparison[f"{scenario}_basin_status"].eq("resolved")
        distance = pd.to_numeric(comparison.loc[status, f"{scenario}_distance_m"], errors="coerce")
        rows.append({
            "scenario": "D_area_select_no_1km" if scenario == "D" else "E_area_geometry_priority",
            "evaluated": len(comparison),
            "resolved": int(status.sum()),
            "unresolved": int((~status).sum()),
            "ambiguous": int(comparison["E_ambiguous"].sum()) if scenario == "E" else int(pd.Series(False, index=comparison.index).sum()),
            "resolved_over_10km": int((distance > 10000).sum()),
        })
    return pd.DataFrame(rows)


def build_geometry_report(comparison: pd.DataFrame, summary: pd.DataFrame) -> str:
    display = comparison[[
        "station_name", "source_station_id", "D_comid", "E_comid",
        "D_area_rel_error", "E_area_rel_error", "D_distance_m", "E_distance_m",
        "D_point_in_local", "E_point_in_local", "D_point_in_basin", "E_point_in_basin",
        "D_basin_status", "E_basin_status", "E_ambiguous", "E_review_reasons",
    ]]
    return "\n".join([
        "# S20 E area-constrained geometry-priority comparison", "",
        "E filters the unchanged MERIT candidate search to area_rel_error <= 0.10, then ranks verified local catchment coverage before verified full-upstream coverage, followed by unverified/non-covering candidates.",
        "A bounded full-upstream check is used only if no local catchment covers the station. Unchecked candidates remain unknown; area_buffer_fallback is never geometry evidence.",
        "The three manual_area_fallback values remain evidence_verified=False.",
        "", "## Scenario summary", "", "```", summary.to_string(index=False), "```",
        "", "## Station comparison", "", "```", display.to_string(index=False), "```", "",
    ])


def run_geometry_priority(args, stations: pd.DataFrame) -> int:
    if not args.d_comparison.is_file():
        raise FileNotFoundError(args.d_comparison)
    d_comparison = pd.read_csv(args.d_comparison, dtype={"source_station_id": "string"})
    d_comparison["source_station_id"] = d_comparison["source_station_id"].map(source_id)
    d_rows = d_comparison.set_index([d_comparison["source"].map(as_text).str.lower(), "source_station_id"])
    paths = geometry_output_paths(args.out_dir)
    existing = [str(path) for path in paths.values() if path.exists()]
    if existing and not args.overwrite:
        raise FileExistsError("Geometry-priority output(s) already exist: " + str(existing))

    from basin_tracer import UpstreamBasinTracer
    tracer = UpstreamBasinTracer(str(args.merit_dir))
    comparison_rows, candidate_rows = [], []
    for _, station in stations.iterrows():
        lookup = (as_text(station["source"]).lower(), as_text(station["source_station_id"]))
        if lookup not in d_rows.index:
            raise ValueError(f"D comparison missing station {lookup}")
        row, candidates = compare_geometry_one(station, d_rows.loc[lookup], tracer, args)
        comparison_rows.append(row)
        candidate_rows.extend(candidates)
        print(f"E {row['station_name']}: {row['D_comid']} -> {row['E_comid']} ({row['E_basin_status']})", flush=True)
    comparison = pd.DataFrame(comparison_rows)
    candidates = pd.DataFrame(candidate_rows)
    summary = geometry_summary(comparison)
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(paths["comparison"], index=False)
    candidates.to_csv(paths["candidates"], index=False)
    summary.to_csv(paths["summary"], index=False)
    paths["report"].write_text(build_geometry_report(comparison, summary), encoding="utf-8")
    print("ALL E outputs written only under:", paths["comparison"].parent)
    return 0


def target_paths(out_dir: Path) -> dict:
    return {
        "s4": out_dir / "sandbox" / "scripts_basin_test" / "output" / S4_BASENAME,
        "comparison": out_dir / "reports" / "station_comparison.csv",
        "candidates": out_dir / "reports" / CANDIDATE_BASENAME,
        "summary": out_dir / "reports" / "scenario_summary.csv",
        "review": out_dir / "reports" / "review_required.csv",
        "report": out_dir / "reports" / "experiment_report.md",
        "provenance": out_dir / "reports" / "provenance.json",
    }


def assert_safe_outputs(out_dir: Path, s3: Path, s4: Path, areas: Path, overwrite: bool):
    sources = {p.resolve() for p in (s3, s4, areas)}
    if any(
        out_dir.resolve() == input_file.resolve().parent
        or out_dir.resolve().is_relative_to(input_file.resolve().parent)
        for input_file in (s3, s4)
    ):
        raise ValueError("Refusing to write outputs anywhere inside production S3/S4 directories")
    files = target_paths(out_dir)
    if any(f.resolve() in sources for f in files.values()):
        raise ValueError("One or more output paths collide with an input file")
    existing = [str(f) for f in files.values() if f.exists()]
    if existing and not overwrite:
        raise FileExistsError("Experimental output(s) already exist (use --overwrite only if intentional): " + str(existing[:3]))
    return files


def build_report(comparison, summary, args, paths):
    changed = int(comparison["comid_changed"].sum())
    review = int(comparison["review_required"].sum())
    return "\n".join([
        "# S20 area-minimum-error S4 experiment", "",
        f"- UTC: {datetime.now(timezone.utc).isoformat()}",
        f"- Selection: all stations with valid reported area; stations rerun: {len(comparison)}",
        f"- S3: `{args.s3_csv}` (read-only)",
        f"- Original S4: `{args.s4_csv}` (read-only)",
        f"- MERIT: `{args.merit_dir}` (read-only)",
        f"- Area acceptance threshold: {args.area_threshold:.1%}",
        f"- Geometry retraced for changed COMIDs: {args.retrace_geometry}",
        "", "## Experimental scenarios", "",
        "A: original S4 selection and original acceptance.",
        "B: minimize area relative error for COMID; original acceptance (<=1 km rules).",
        "C: original S4 COMID; new area-only acceptance with no 1 km hard cap.",
        "D: minimize area relative error for COMID; new area-only acceptance (main experiment).",
        "", "## Summary", "", "```", summary.to_string(index=False), "```", "",
        f"- Changed COMIDs: {changed} / {len(comparison)}",
        f"- Review required: {review} / {len(comparison)}", "",
        "## Critical interpretation", "",
        "- Only stations with independent, positive S3 reported_area or experimental manual evidence are reselected.",
        "- S3 reported_area has priority; manual evidence is ONLY a fallback for missing S3 area.",
        "- Original MERIT candidate search range (<120 km and the existing coarse bbox) is NOT relaxed.",
        "- Strict primary ranking is absolute area relative error; distance breaks exact ties only.",
        "- area_error in S4 remains signed log10(MERIT/reported); experimental_area_rel_error is absolute relative error.",
        "- All records without eligible area and GSED/RiverSed retain their ORIGINAL production S4 rows.",
        "- Adjacent candidates with similar upstream areas can remain ambiguous; geometry/topology is separate QA.",
        "- A selected COMID may be over 10 km away; this is experimental, NOT approved for global publication.",
        "- If geometry was not retraced for a new COMID, local/upstream coverage is UNKNOWN, not False.",
        "- Output S4 CSV can be trialed with S5 using explicit --s3-csv/--basin-csv/--out/--report arguments.",
        "- This experiment does not create S4 GPKGs or publication-ready basin polygons.",
        "- No production S3/S4/S5 files were written by this tool.",
        "", "## Outputs", "",
        *[f"- {name}: `{path}`" for name, path in paths.items()], "",
    ])


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    for field in ("s3_csv", "s4_csv", "merit_dir", "manual_areas", "out_dir", "d_comparison"):
        setattr(args, field, getattr(args, field).expanduser().resolve())
    if not math.isfinite(args.area_threshold) or not (0 < args.area_threshold < 1):
        raise ValueError("--area-threshold must be between 0 and 1")
    if args.candidate_top < 1 or args.max_stations < 0 or args.basin_check_limit < 1:
        raise ValueError("--candidate-top >=1, --max-stations >=0, --basin-check-limit >=1 required")
    s3, s4, manual = load_inputs(args.s3_csv, args.s4_csv, args.manual_areas)
    stations = select_targets(add_area_metadata(s3, manual), args.max_stations)
    if args.dry_run:
        print("DRY RUN (no writes; MERIT not opened)")
        print("S3:", args.s3_csv)
        print("S4:", args.s4_csv)
        print("Eligible station rows:", len(stations))
        columns = ["station_key", "source", "source_station_id", "reference_area_km2", "evidence_origin", "evidence_verified"]
        print(stations[columns].to_string(index=False, max_rows=30))
        return 0
    if not (args.merit_dir / "pfaf_level_01").is_dir():
        raise FileNotFoundError("Missing MERIT pfaf_level_01 at: {}".format(args.merit_dir))
    if args.geometry_priority:
        return run_geometry_priority(args, stations)
    paths = assert_safe_outputs(args.out_dir, args.s3_csv, args.s4_csv, args.manual_areas, args.overwrite)

    from basin_tracer import UpstreamBasinTracer
    tracer = UpstreamBasinTracer(str(args.merit_dir))
    # Full original S4 copied in memory, then ONLY eligible matching rows changed.
    exp_s4 = s4.copy()
    # These fields may contain an unknown/empty value in the experiment. Cast
    # to object to avoid writing strings into pandas integer/bool columns.
    for key in ("pfaf_code", "point_in_local", "point_in_basin", "n_upstream_reaches"):
        if key in exp_s4:
            exp_s4[key] = exp_s4[key].astype("object")
    comparison_rows, candidate_rows = [], []
    s4_index = {as_text(key): idx for idx, key in exp_s4["station_key"].items()}
    for index, (_, station) in enumerate(stations.iterrows(), 1):
        key = as_text(station["station_key"])
        i = s4_index[key]
        update, compare, candidates = compare_one(station, exp_s4.loc[i].copy(), tracer, args)
        for column, value in update.items():
            if column not in exp_s4.columns:
                exp_s4[column] = pd.Series([None] * len(exp_s4), dtype="object")
            exp_s4.at[i, column] = value
        comparison_rows.append(compare)
        candidate_rows.extend(candidates)
        print(f"[{index}/{len(stations)}] {compare['source']} {compare['source_station_id']} "
              f"A={compare['A_status']} D={compare['D_status']} "
              f"{compare['baseline_comid']} -> {compare['new_comid']}", flush=True)

    comparison_df = pd.DataFrame(comparison_rows)
    summary_df = scenario_summary(comparison_df)
    candidate_df = pd.DataFrame(candidate_rows)
    # Original identities and point coordinates must be preserved for S5 joins.
    if len(exp_s4) != len(s4) or not exp_s4["station_key"].equals(s4["station_key"]):
        raise AssertionError("Experimental S4 changed station count/identity")
    if not exp_s4["station_id"].equals(s4["station_id"]):
        raise AssertionError("Experimental S4 changed station_id")
    if any(not exp_s4[c].equals(s4[c]) for c in ("lon", "lat") if c in s4):
        raise AssertionError("Experimental S4 changed source coordinates")
    for dest in paths.values():
        dest.parent.mkdir(parents=True, exist_ok=True)
    exp_s4.to_csv(paths["s4"], index=False)
    comparison_df.to_csv(paths["comparison"], index=False)
    candidate_df.to_csv(paths["candidates"], index=False)
    summary_df.to_csv(paths["summary"], index=False)
    comparison_df.loc[comparison_df["review_required"]].to_csv(paths["review"], index=False)
    paths["report"].write_text(build_report(comparison_df, summary_df, args, paths), encoding="utf-8")
    try:
        git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        git_sha = "unknown"
    provenance = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": git_sha,
        "station_selection": "all_non_reach_product_stations_with_valid_reported_area",
        "area_threshold": args.area_threshold,
        "ambiguity_gap": args.ambiguity_gap,
        "ambiguity_distance_m": args.ambiguity_distance_m,
        "retrace_geometry": args.retrace_geometry,
        "n_rerun": len(stations),
        "n_all_s4": len(s4),
        "merit_dir": str(args.merit_dir),
        "input_sha256": {str(path): file_sha256(path) for path in (args.s3_csv, args.s4_csv, args.manual_areas)},
    }
    paths["provenance"].write_text(json.dumps(provenance, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("\n" + summary_df.to_string(index=False))
    print("\nALL outputs written only under:", args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
