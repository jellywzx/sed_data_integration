#!/usr/bin/env python3
"""Experimental S4: assign MERIT upstream area by minimum relative area error.

Only MERIT ``uparea`` (km2) is needed.  This deliberately DOES NOT create basin
polygons, run upstream BFS, or use the 1 km acceptance rule.  The distance
computed during candidate retrieval is diagnostic ONLY and is never used to
rank the retrieved candidates (even as a tiebreaker).

The diagnostic table can also be overlaid on a complete standard-S4 fallback
table to produce the full ``s4_upstream_basins.csv`` contract expected by S5.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_S3 = REPO_ROOT / "output/s3_collected_stations.csv"
MERIT_DIR = Path("/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1")
MANUAL_AREA_CSV = REPO_ROOT / "config/amazon_area_evidence.csv"
EXPERIMENTS_ROOT = REPO_ROOT.parent
DEFAULT_OUTPUT = REPO_ROOT / "output"
REACH_PRODUCTS = frozenset({"gsed", "riversed"})

RESULT_NAME = "s4_area_only_upstream_area.csv"
CANDIDATE_NAME = "s4_area_only_candidate_top10.csv"
SUMMARY_NAME = "s4_area_only_summary.csv"
METADATA_NAME = "s4_area_only_run.json"
RESULT_COLUMNS = [
    "station_key", "station_id", "source", "source_station_id", "station_name",
    "lon", "lat", "reported_area_km2", "area_origin", "evidence_verified",
    "manual_s3_disagreement", "basin_id", "pfaf_code", "uparea_merit_km2",
    "absolute_area_error_km2", "area_rel_error", "area_error_percent",
    "legacy_area_error_log10", "distance_m_diagnostic", "candidate_count",
    "min_error_tied_comids", "within_area_threshold", "area_match_status",
    "selection_method",
]
CANDIDATE_COLUMNS = [
    "station_key", "source", "source_station_id", "rank_area_only", "COMID",
    "pfaf_code", "uparea_merit_km2", "area_rel_error", "distance_m_diagnostic",
]


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--s3-csv", type=Path, default=DEFAULT_S3)
    parser.add_argument("--merit-dir", type=Path, default=MERIT_DIR)
    parser.add_argument("--manual-areas", type=Path, default=MANUAL_AREA_CSV,
                        help="Optional fallback area CSV; S3 area always takes precedence")
    parser.add_argument("--fallback-s4-csv", type=Path,
                        help="Complete standard-S4 CSV used for stations without valid area")
    parser.add_argument("--pipeline-s4-out", type=Path,
                        help="Write a complete S5-compatible S4 CSV by overlaying area-only results")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--area-threshold", type=float, default=0.10,
                        help="Report a match as within tolerance when relative error <= this value")
    parser.add_argument("--candidate-top", type=int, default=10)
    parser.add_argument("--max-stations", type=int, default=0,
                        help="Only first N eligible stations, 0 means all")
    parser.add_argument("--dry-run", action="store_true",
                        help="Validate inputs and show targets without opening MERIT or writing files")
    parser.add_argument("--overwrite", action="store_true",
                        help="Permit overwriting only the four named experimental outputs")
    return parser.parse_args(argv)


def _text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _station_id(value):
    value = _text(value)
    return value[:-2] if value.endswith(".0") else value


def _valid_number(value):
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) and v > 0 else None


def _truthy(value):
    return _text(value).lower() in {"1", "true", "t", "yes", "y"}


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def load_input(s3_csv, manual_csv):
    s3 = pd.read_csv(
        s3_csv, dtype={"station_key": "string", "source_station_id": "string"},
        low_memory=False,
    )
    required = {"station_key", "station_id", "source", "source_station_id", "lat", "lon"}
    if required - set(s3.columns):
        raise ValueError("S3 missing columns: {}".format(sorted(required - set(s3.columns))))
    if s3["station_key"].isna().any():
        raise ValueError("S3 station_key contains missing values")
    s3["station_key"] = s3["station_key"].astype(str).str.strip()
    if s3["station_key"].eq("").any() or s3["station_key"].duplicated().any():
        raise ValueError("S3 station_key is empty or duplicated")
    s3["source"] = s3["source"].map(_text)
    s3["source_station_id"] = s3["source_station_id"].map(_station_id)
    s3["_source_join"] = s3["source"].str.lower()
    if "reported_area" not in s3:
        s3["reported_area"] = np.nan

    if manual_csv is not None and manual_csv.is_file():
        manual = pd.read_csv(manual_csv, dtype={"source_station_id": "string"})
        m_required = {"source", "source_station_id", "reported_area_km2"}
        if m_required - set(manual.columns):
            raise ValueError("Manual area CSV missing columns: {}".format(sorted(m_required - set(manual.columns))))
        manual["source"] = manual["source"].map(_text).str.lower()
        manual["source_station_id"] = manual["source_station_id"].map(_station_id)
        if manual.duplicated(["source", "source_station_id"]).any():
            raise ValueError("Duplicate (source, source_station_id) in manual areas")
        if "evidence_verified" not in manual:
            manual["evidence_verified"] = False
        manual = manual.rename(columns={
            "source": "_source_join",
            "reported_area_km2": "_manual_area",
            "evidence_verified": "_manual_verified",
        })[["_source_join", "source_station_id", "_manual_area", "_manual_verified"]]
    else:
        manual = pd.DataFrame(columns=[
            "_source_join", "source_station_id", "_manual_area", "_manual_verified"
        ])

    work = s3.merge(manual, how="left", on=["_source_join", "source_station_id"],
                    validate="many_to_one")
    native = pd.to_numeric(work["reported_area"], errors="coerce")
    fallback = pd.to_numeric(work["_manual_area"], errors="coerce")
    native_valid = np.isfinite(native) & (native > 0)
    fallback_valid = np.isfinite(fallback) & (fallback > 0)
    work["reference_area_km2"] = native.where(native_valid, fallback.where(fallback_valid, np.nan))
    work["area_origin"] = np.where(
        native_valid, "s3_reported_area",
        np.where(fallback_valid, "manual_area_fallback", "missing"),
    )
    work["evidence_verified"] = np.where(
        native_valid, False, work["_manual_verified"].map(_truthy),
    )
    work["manual_s3_disagreement"] = native_valid & fallback_valid & (
        (native - fallback).abs() / native > 0.01
    )
    return work


def eligible_stations(frame, max_stations):
    valid_area = pd.to_numeric(frame["reference_area_km2"], errors="coerce")
    work = frame.loc[
        np.isfinite(valid_area) & (valid_area > 0)
        & ~frame["_source_join"].isin(REACH_PRODUCTS)
    ].copy()
    work = work.sort_values(["source", "source_station_id", "station_key"], kind="mergesort")
    if max_stations > 0:
        work = work.head(max_stations)
    if work.empty:
        raise ValueError("No eligible valid reported_area stations")
    # Reject bad coordinates for selected stations instead of silently matching wrong location.
    for col, lo, hi in (("lon", -180, 180), ("lat", -90, 90)):
        values = pd.to_numeric(work[col], errors="coerce")
        if (values.isna() | ~np.isfinite(values) | (values < lo) | (values > hi)).any():
            raise ValueError("Invalid {} among target stations".format(col))
    return work


def rank_candidates_area_only(candidates, reported_area):
    """Rank all already-nearby MERIT reaches by area ONLY, tie by COMID.

    Candidates are found using the original MERIT search bounding box/radius.
    `dist_m` is copied for diagnostics, but NOT used for ranking or tiebreaks.
    """
    if _valid_number(reported_area) is None:
        raise ValueError("reported_area must be finite and positive")
    columns = ["COMID", "uparea", "pfaf_code"]
    if candidates is None or len(candidates) == 0:
        return pd.DataFrame(columns=columns + ["dist_m", "area_rel_error", "rank_area_only"])
    if not set(columns).issubset(candidates.columns):
        raise ValueError("MERIT candidates must include COMID/uparea/pfaf_code")
    work = candidates.loc[:, columns + (["dist_m"] if "dist_m" in candidates else [])].copy()
    if "dist_m" not in work:
        work["dist_m"] = np.nan
    work["COMID"] = pd.to_numeric(work["COMID"], errors="coerce")
    work["uparea"] = pd.to_numeric(work["uparea"], errors="coerce")
    work["dist_m"] = pd.to_numeric(work["dist_m"], errors="coerce")
    work["pfaf_code"] = work["pfaf_code"].astype("string")
    valid = (
        np.isfinite(work["COMID"]) & (work["COMID"] > 0)
        & (work["COMID"] % 1 == 0) & np.isfinite(work["uparea"])
        & (work["uparea"] > 0) & work["pfaf_code"].notna()
        & work["pfaf_code"].ne("")
    )
    work = work.loc[valid].copy()
    if work.empty:
        return pd.DataFrame(columns=columns + ["dist_m", "area_rel_error", "rank_area_only"])
    work["COMID"] = work["COMID"].astype("int64")
    area = float(reported_area)
    work["area_rel_error"] = (work["uparea"] / area - 1.0).abs()
    work = work.loc[np.isfinite(work["area_rel_error"])].copy()
    work = work.sort_values(
        ["area_rel_error", "COMID", "pfaf_code"], kind="mergesort"
    ).drop_duplicates("COMID", keep="first").reset_index(drop=True)
    work.insert(0, "rank_area_only", np.arange(1, len(work) + 1))
    return work


def match_station(station, tracer, threshold, candidate_top):
    area = float(station["reference_area_km2"])
    ranked = rank_candidates_area_only(
        tracer.get_nearby_candidate_reaches(float(station["lon"]), float(station["lat"])),
        area,
    )
    result = {
        "station_key": _text(station["station_key"]),
        "station_id": station["station_id"],
        "source": station["source"],
        "source_station_id": station["source_station_id"],
        "station_name": _text(station.get("source_station_name", "")),
        "lon": float(station["lon"]),
        "lat": float(station["lat"]),
        "reported_area_km2": area,
        "area_origin": station["area_origin"],
        "evidence_verified": bool(station["evidence_verified"]),
        "manual_s3_disagreement": bool(station["manual_s3_disagreement"]),
        "basin_id": np.nan,
        "pfaf_code": "",
        "uparea_merit_km2": np.nan,
        "absolute_area_error_km2": np.nan,
        "area_rel_error": np.nan,
        "area_error_percent": np.nan,
        "legacy_area_error_log10": np.nan,
        "distance_m_diagnostic": np.nan,
        "candidate_count": int(len(ranked)),
        "min_error_tied_comids": 0,
        "within_area_threshold": False,
        "area_match_status": "no_merit_candidate",
        "selection_method": "min_uparea_relative_error_no_geometry",
    }
    cand_records = []
    if ranked.empty:
        return result, cand_records

    best = ranked.iloc[0]
    uparea = float(best["uparea"])
    error = float(best["area_rel_error"])
    ties = int(np.isclose(ranked["area_rel_error"], error, rtol=0.0, atol=1e-12).sum())
    # Include the exact threshold despite ordinary binary floating-point error.
    within = error <= threshold or math.isclose(error, threshold, rel_tol=0.0, abs_tol=1e-12)
    result.update({
        "basin_id": int(best["COMID"]),
        "pfaf_code": str(best["pfaf_code"]),
        "uparea_merit_km2": uparea,
        "absolute_area_error_km2": abs(uparea - area),
        "area_rel_error": error,
        "area_error_percent": error * 100.0,
        "legacy_area_error_log10": math.log10(uparea / area),
        "distance_m_diagnostic": float(best["dist_m"]),
        "min_error_tied_comids": ties,
        "within_area_threshold": bool(within),
        "area_match_status": "within_area_threshold" if within else "above_area_threshold",
    })
    for c in ranked.head(candidate_top).itertuples(index=False):
        cand_records.append({
            "station_key": result["station_key"],
            "source": result["source"],
            "source_station_id": result["source_station_id"],
            "rank_area_only": int(c.rank_area_only),
            "COMID": int(c.COMID),
            "pfaf_code": str(c.pfaf_code),
            "uparea_merit_km2": float(c.uparea),
            "area_rel_error": float(c.area_rel_error),
            "distance_m_diagnostic": float(c.dist_m),
        })
    return result, cand_records


def summarize(results):
    rows = []
    for source, grp in [("ALL", results)] + list(results.groupby("source", sort=True)):
        errors = grp.loc[grp["area_match_status"].eq("within_area_threshold"), "area_error_percent"]
        rows.append({
            "source": source,
            "n_matched": int(grp["basin_id"].notna().sum()),
            "n_selected": len(grp),
            "n_within_area_threshold": int(grp["within_area_threshold"].sum()),
            "n_above_area_threshold": int(grp["area_match_status"].eq("above_area_threshold").sum()),
            "n_without_candidate": int(grp["area_match_status"].eq("no_merit_candidate").sum()),
            "median_error_pct_accepted": float(errors.median()) if len(errors) else np.nan,
            "max_error_pct_accepted": float(errors.max()) if len(errors) else np.nan,
        })
    return pd.DataFrame(rows)


def build_pipeline_s4(s3, fallback_s4, area_results):
    """Overlay area-only matches on a complete standard-S4 fallback table."""
    required = {
        "station_key", "station_id", "basin_id", "basin_area", "match_quality",
        "reported_area", "area_error", "uparea_merit", "pfaf_code", "method",
        "distance_m", "basin_status", "basin_flag",
    }
    missing = required - set(fallback_s4.columns)
    if missing:
        raise ValueError("Fallback S4 missing columns: {}".format(sorted(missing)))

    s3_keys = s3[["station_key", "station_id"]].copy()
    s3_keys["station_key"] = s3_keys["station_key"].astype(str).str.strip()
    fallback = fallback_s4.copy()
    fallback["station_key"] = fallback["station_key"].astype(str).str.strip()
    if fallback["station_key"].eq("").any() or fallback["station_key"].duplicated().any():
        raise ValueError("Fallback S4 station_key is empty or duplicated")
    if set(fallback["station_key"]) != set(s3_keys["station_key"]):
        raise ValueError("Fallback S4 station_key set must exactly match S3")

    expected_ids = s3_keys.set_index("station_key")["station_id"].astype("int64")
    actual_ids = fallback.set_index("station_key")["station_id"].astype("int64")
    if not actual_ids.sort_index().equals(expected_ids.sort_index()):
        raise ValueError("Fallback S4 station_id values do not match S3")

    for name in (
        "area_only_applied", "area_only_area_origin", "area_only_area_rel_error",
        "area_only_within_threshold", "area_only_candidate_count",
    ):
        if name not in fallback:
            fallback[name] = np.nan

    row_by_key = {str(key).strip(): idx for idx, key in fallback["station_key"].items()}
    for result in area_results.to_dict("records"):
        idx = row_by_key[result["station_key"]]
        has_candidate = pd.notna(result["basin_id"])
        within = bool(result["within_area_threshold"])
        updates = {
            "reported_area": result["reported_area_km2"],
            "basin_id": result["basin_id"] if has_candidate else np.nan,
            "basin_area": result["uparea_merit_km2"] if has_candidate else np.nan,
            "uparea_merit": result["uparea_merit_km2"] if has_candidate else np.nan,
            "area_error": result["legacy_area_error_log10"] if has_candidate else np.nan,
            "pfaf_code": result["pfaf_code"] if has_candidate else "",
            "distance_m": result["distance_m_diagnostic"] if has_candidate else np.nan,
            "match_quality": "area_matched" if within else ("area_mismatch" if has_candidate else "failed"),
            "method": "area_only_min_relative_error" if has_candidate else "area_only_no_match",
            "basin_status": "resolved" if within else "unresolved",
            "basin_flag": "ok" if within else ("area_mismatch" if has_candidate else "no_match"),
            "point_in_local": False,
            "point_in_basin": False,
            "n_upstream_reaches": 0,
            "area_only_applied": True,
            "area_only_area_origin": result["area_origin"],
            "area_only_area_rel_error": result["area_rel_error"],
            "area_only_within_threshold": within,
            "area_only_candidate_count": result["candidate_count"],
        }
        for column, value in updates.items():
            if column not in fallback:
                fallback[column] = np.nan
            fallback.at[idx, column] = value

    fallback["area_only_applied"] = fallback["area_only_applied"].fillna(False).astype(bool)
    return fallback


def _checked_paths(out_dir):
    root = EXPERIMENTS_ROOT.resolve()
    target = out_dir.resolve()
    if target == root or root not in target.parents:
        raise ValueError("Refusing output outside Output_r/experiments: {}".format(out_dir))
    return {
        "results": target / RESULT_NAME,
        "candidates": target / CANDIDATE_NAME,
        "summary": target / SUMMARY_NAME,
        "metadata": target / METADATA_NAME,
    }


def run(args, *, tracer_factory=None):
    if not (0 < args.area_threshold < 1 and math.isfinite(args.area_threshold)):
        raise ValueError("--area-threshold must be finite and between 0 and 1")
    if args.candidate_top < 1 or args.max_stations < 0:
        raise ValueError("--candidate-top >=1 and --max-stations >=0 required")
    if bool(args.fallback_s4_csv) != bool(args.pipeline_s4_out):
        raise ValueError("--fallback-s4-csv and --pipeline-s4-out must be provided together")
    targets = _checked_paths(args.out_dir)
    s3 = load_input(args.s3_csv, args.manual_areas)
    stations = eligible_stations(s3, args.max_stations)
    print("S3: {}".format(args.s3_csv))
    print("MERIT: {}".format(args.merit_dir))
    print("Experiment output: {}".format(args.out_dir))
    print("Eligible area stations: {}".format(len(stations)))
    preview_cols = ["source", "source_station_id", "reference_area_km2", "area_origin", "evidence_verified"]
    print(stations[preview_cols].head(25).to_string(index=False))
    if len(stations) > 25:
        print("... omitted {} additional station rows from preview".format(len(stations) - 25))
    if args.dry_run:
        print("DRY RUN: no MERIT opened, no writes")
        return None

    write_targets = list(targets.values())
    if args.pipeline_s4_out:
        write_targets.append(args.pipeline_s4_out)
    for p in write_targets:
        if p.exists() and not args.overwrite:
            raise FileExistsError("Output already exists (use --overwrite to replace named outputs): {}".format(p))
    if not args.merit_dir.joinpath("pfaf_level_01").is_dir():
        raise FileNotFoundError("MERIT pfaf_level_01 not found: {}".format(args.merit_dir))

    if tracer_factory is None:
        if str(REPO_ROOT) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT))
        from basin_tracer import UpstreamBasinTracer
        tracer_factory = UpstreamBasinTracer
    tracer = tracer_factory(str(args.merit_dir))
    rows, candidates = [], []
    for i, (_, station) in enumerate(stations.iterrows(), start=1):
        result, possible = match_station(station, tracer, args.area_threshold, args.candidate_top)
        rows.append(result)
        candidates.extend(possible)
        if i <= 10 or i % 100 == 0 or i == len(stations):
            print("[{}/{}] {} {} -> COMID={} uparea={:.3f} km2 error={:.4f}%".format(
                i, len(stations), result["source"], result["source_station_id"],
                result["basin_id"], result["uparea_merit_km2"], result["area_error_percent"]
            ))

    result_df = pd.DataFrame(rows, columns=RESULT_COLUMNS)
    candidate_df = pd.DataFrame(candidates, columns=CANDIDATE_COLUMNS)
    summary_df = summarize(result_df)
    meta = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "method": "min_relative_upstream_area_error_without_geometry_or_distance_ranking",
        "candidate_search": "production UpstreamBasinTracer.get_nearby_candidate_reaches (bbox +/-1 deg and distance <120 km)",
        "s3_csv": str(args.s3_csv.resolve()),
        "s3_sha256": sha256(args.s3_csv),
        "manual_area_csv": str(args.manual_areas.resolve()) if args.manual_areas.is_file() else None,
        "manual_area_sha256": sha256(args.manual_areas) if args.manual_areas.is_file() else None,
        "merit_dir": str(args.merit_dir.resolve()),
        "station_selection": "all_non_reach_product_stations_with_valid_reported_area",
        "area_threshold": args.area_threshold,
        "station_count": len(stations),
        "fallback_s4_csv": str(args.fallback_s4_csv.resolve()) if args.fallback_s4_csv else None,
        "pipeline_s4_out": str(args.pipeline_s4_out.resolve()) if args.pipeline_s4_out else None,
        "no_basin_polygon_generated": True,
        "not_s5_compatible": not bool(args.pipeline_s4_out),
    }
    target_dir = args.out_dir.resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(targets["results"], index=False)
    candidate_df.to_csv(targets["candidates"], index=False)
    summary_df.to_csv(targets["summary"], index=False)
    if args.pipeline_s4_out:
        fallback_s4 = pd.read_csv(args.fallback_s4_csv, low_memory=False)
        pipeline_s4 = build_pipeline_s4(s3, fallback_s4, result_df)
        args.pipeline_s4_out.parent.mkdir(parents=True, exist_ok=True)
        pipeline_s4.to_csv(args.pipeline_s4_out, index=False)
        print("S5-compatible S4 written to {}".format(args.pipeline_s4_out))
    targets["metadata"].write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("Outputs written only to {}".format(target_dir))
    print(summary_df.to_string(index=False))
    return result_df


def main(argv=None):
    args = parse_args(argv)
    for name in ("s3_csv", "merit_dir", "manual_areas", "out_dir", "fallback_s4_csv", "pipeline_s4_out"):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, value.expanduser().resolve())
    try:
        run(args)
    except (OSError, ValueError) as exc:
        print("ERROR: {}".format(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
