#!/usr/bin/env python3
"""Summarize USGS vs RiverSed daily SSC matches with MERIT COMID checks.

This script answers:

  1. How many RiverSed daily SSC records matched USGS?
  2. Among matched records, how many are on the same MERIT COMID?
  3. Among high-error records, are they same-COMID value suspects or
     different-COMID spatial mismatch suspects?

Inputs expected:
  scripts_basin_test/output/early_validation_results/early_overlap_pair_records.csv
  scripts_basin_test/output/early_validation_results/early_overlap_candidates.csv.gz

Intermediate output always written by this script:
  scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_qc_flags.csv

MERIT input:
  MERIT_DIR/pfaf_level_01/riv_pfaf_*.shp

Outputs:
  scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_qc_with_merit_comid.csv
  scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_comid_summary.csv
  scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_threshold_summary.csv
  scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_comid_report.md

No command-line arguments are required. Edit DEFAULT_* constants below if needed.
"""

from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import geopandas as gpd
except ImportError:
    gpd = None

try:
    from shapely.geometry import Point
except ImportError:
    Point = None

try:
    from pipeline_paths import get_output_r_root
except Exception:
    def get_output_r_root(script_dir: Path) -> Path:
        return script_dir.parent.resolve()


# ---------------------------------------------------------------------------
# Built-in config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent


def find_output_r_root(script_dir: Path) -> Path:
    """Find Output_r even if this script lives under scripts_basin_test/test."""
    candidates = [script_dir.resolve()] + list(script_dir.resolve().parents)

    for parent in candidates:
        early_dir = parent / "scripts_basin_test" / "output" / "early_validation_results"
        if early_dir.exists():
            return parent

    for parent in candidates:
        output_dir = parent / "scripts_basin_test" / "output"
        if output_dir.exists():
            return parent

    for parent in candidates:
        if parent.name == "Output_r":
            return parent

    return get_output_r_root(script_dir)


PROJECT_ROOT = find_output_r_root(SCRIPT_DIR)
EARLY_DIR = PROJECT_ROOT / "scripts_basin_test" / "output" / "early_validation_results"

PAIR_RECORDS_CSV = EARLY_DIR / "early_overlap_pair_records.csv"
CANDIDATES_CSV_GZ = EARLY_DIR / "early_overlap_candidates.csv.gz"
OUT_QC_FLAGS_CSV = EARLY_DIR / "usgs_riversed_daily_ssc_qc_flags.csv"

DEFAULT_MERIT_DIR = PROJECT_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
MERIT_DIR = Path(os.environ.get("MERIT_DIR", str(DEFAULT_MERIT_DIR))).expanduser().resolve()

OUT_DETAIL_CSV = EARLY_DIR / "usgs_riversed_daily_ssc_qc_with_merit_comid.csv"
OUT_COMID_SUMMARY_CSV = EARLY_DIR / "usgs_riversed_daily_ssc_comid_summary.csv"
OUT_THRESHOLD_SUMMARY_CSV = EARLY_DIR / "usgs_riversed_daily_ssc_threshold_summary.csv"
OUT_REPORT_MD = EARLY_DIR / "usgs_riversed_daily_ssc_comid_report.md"

TARGET_SOURCE_PAIR = "USGS vs RiverSed"
TARGET_RESOLUTION = "daily"
TARGET_VARIABLE = "SSC"

# MERIT matching search.
INITIAL_SEARCH_PADDING_DEG = 0.20
SEARCH_EXPANSION_FACTORS = [1.0, 2.0, 4.0, 8.0]

# Distance thresholds for interpreting point-to-reach matches.
GOOD_DISTANCE_M = 1000.0
WEAK_DISTANCE_M = 5000.0

# Error thresholds.
REVIEW_PCT_ERROR = 50.0
SUSPECT_PCT_ERROR = 100.0
HIGH_SUSPECT_PCT_ERROR = 200.0
SUSPECT_ABS_DIFF = 10.0
LOW_RATIO_THRESHOLD = 0.25
HIGH_RATIO_THRESHOLD = 4.0
ROBUST_Z_THRESHOLD = 3.0

# Projected CRS used to compute diagnostic distances to MERIT lines.
DISTANCE_CRS = "EPSG:3857"


def clean_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def safe_float(value) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def normalize_date(value) -> str:
    text = clean_text(value)
    if not text:
        return ""
    try:
        return pd.Timestamp(text).strftime("%Y-%m-%d")
    except Exception:
        return text


def fmt_num(value, digits: int = 5) -> str:
    x = safe_float(value)
    if not np.isfinite(x):
        return "NA"
    return "{:.{}g}".format(x, digits)


def read_inputs() -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not PAIR_RECORDS_CSV.is_file():
        raise FileNotFoundError("Missing pair records CSV: {}".format(PAIR_RECORDS_CSV))
    if not CANDIDATES_CSV_GZ.is_file():
        raise FileNotFoundError("Missing candidates CSV: {}".format(CANDIDATES_CSV_GZ))

    pairs = pd.read_csv(PAIR_RECORDS_CSV)
    candidates = pd.read_csv(CANDIDATES_CSV_GZ)

    if "date" in pairs.columns:
        pairs["date"] = pairs["date"].map(normalize_date)
    if "date" in candidates.columns:
        candidates["date"] = candidates["date"].map(normalize_date)

    return pairs, candidates


def filter_target_pairs(pairs: pd.DataFrame) -> pd.DataFrame:
    required = {"source_pair", "resolution", "variable", "cluster_id", "date"}
    missing = sorted(required - set(pairs.columns))
    if missing:
        raise ValueError("pair records missing required columns: {}".format(", ".join(missing)))

    target = pairs[
        (pairs["source_pair"].astype(str) == TARGET_SOURCE_PAIR)
        & (pairs["resolution"].astype(str) == TARGET_RESOLUTION)
        & (pairs["variable"].astype(str) == TARGET_VARIABLE)
    ].copy()

    if target.empty:
        raise ValueError(
            "No rows found for source_pair={!r}, resolution={!r}, variable={!r}".format(
                TARGET_SOURCE_PAIR,
                TARGET_RESOLUTION,
                TARGET_VARIABLE,
            )
        )

    for col in [
        "cluster_id",
        "value_a",
        "value_b",
        "diff_b_minus_a",
        "abs_diff",
        "pct_error",
        "time",
    ]:
        if col in target.columns:
            target[col] = pd.to_numeric(target[col], errors="coerce")

    target["date"] = target["date"].map(normalize_date)

    # In source_pair = USGS vs RiverSed, source_a should usually be USGS and
    # source_b should usually be RiverSed. Keep explicit aliases for readability.
    target["USGS_SSC"] = target["value_a"]
    target["RiverSed_SSC"] = target["value_b"]

    eps = 1e-6
    target["ratio_RiverSed_to_USGS"] = (
        (target["RiverSed_SSC"] + eps) / (target["USGS_SSC"] + eps)
    )
    target["log_ratio"] = np.log(target["ratio_RiverSed_to_USGS"])

    median_log_ratio = target["log_ratio"].median()
    mad = np.median(np.abs(target["log_ratio"] - median_log_ratio))
    robust_sigma = 1.4826 * mad if mad > 0 else np.nan
    if np.isfinite(robust_sigma) and robust_sigma > 0:
        target["robust_log_ratio_z"] = (
            (target["log_ratio"] - median_log_ratio).abs() / robust_sigma
        )
    else:
        target["robust_log_ratio_z"] = np.nan

    if "qc_level" not in target.columns:
        target["qc_level"] = target.apply(classify_error_only, axis=1)

    return target


def classify_error_only(row: pd.Series) -> str:
    pct = safe_float(row.get("pct_error"))
    abs_diff = safe_float(row.get("abs_diff"))
    ratio = safe_float(row.get("ratio_RiverSed_to_USGS"))
    rz = safe_float(row.get("robust_log_ratio_z"))

    if not np.isfinite(pct):
        return "unknown"

    if (
        pct >= HIGH_SUSPECT_PCT_ERROR
        or ratio <= LOW_RATIO_THRESHOLD
        or ratio >= HIGH_RATIO_THRESHOLD
        or (np.isfinite(rz) and rz >= ROBUST_Z_THRESHOLD)
    ):
        return "high_suspect"

    if pct >= SUSPECT_PCT_ERROR and abs_diff >= SUSPECT_ABS_DIFF:
        return "suspect"

    if pct >= REVIEW_PCT_ERROR:
        return "review"

    return "OK"


def build_point_record(pair_row: pd.Series, candidate_rec: Dict[str, object], side: str) -> Dict[str, object]:
    source_col = "source_{}".format(side)
    uid_col = "source_station_uid_{}".format(side)
    value_col = "value_{}".format(side)

    return {
        "side": side,
        "source": clean_text(pair_row.get(source_col, candidate_rec.get("source", ""))),
        "uid": clean_text(pair_row.get(uid_col, candidate_rec.get("source_station_uid", ""))),
        "lat": safe_float(candidate_rec.get("source_station_lat")),
        "lon": safe_float(candidate_rec.get("source_station_lon")),
        "value": safe_float(pair_row.get(value_col, candidate_rec.get(TARGET_VARIABLE))),
        "candidate_path": clean_text(candidate_rec.get("candidate_path")),
        "resolved_candidate_path": clean_text(candidate_rec.get("resolved_candidate_path")),
    }


def find_candidate_point(candidates: pd.DataFrame, pair_row: pd.Series, side: str) -> Dict[str, object]:
    source_col = "source_{}".format(side)
    uid_col = "source_station_uid_{}".format(side)
    value_col = "value_{}".format(side)

    cluster_id = safe_float(pair_row.get("cluster_id"))
    date = normalize_date(pair_row.get("date"))
    resolution = clean_text(pair_row.get("resolution"))
    source = clean_text(pair_row.get(source_col))
    uid = clean_text(pair_row.get(uid_col))

    cand = candidates.copy()
    cand["_cluster_id_num"] = pd.to_numeric(cand.get("cluster_id", np.nan), errors="coerce")

    mask = (
        cand["_cluster_id_num"].eq(cluster_id)
        & cand["resolution"].astype(str).eq(resolution)
        & cand["date"].astype(str).eq(date)
    )

    if uid and "source_station_uid" in cand.columns:
        exact = cand[mask & cand["source_station_uid"].astype(str).eq(uid)]
        if not exact.empty:
            return build_point_record(pair_row, exact.iloc[0].to_dict(), side)

    if source and "source" in cand.columns:
        exact = cand[mask & cand["source"].astype(str).eq(source)]
        if not exact.empty:
            return build_point_record(pair_row, exact.iloc[0].to_dict(), side)

    expected = safe_float(pair_row.get(value_col))
    if mask.any() and TARGET_VARIABLE in cand.columns and np.isfinite(expected):
        sub = cand[mask].copy()
        sub["_value_diff"] = (
            pd.to_numeric(sub[TARGET_VARIABLE], errors="coerce") - expected
        ).abs()
        sub = sub.sort_values("_value_diff", kind="mergesort")
        if not sub.empty:
            return build_point_record(pair_row, sub.iloc[0].to_dict(), side)

    return {
        "side": side,
        "source": source,
        "uid": uid,
        "lat": float("nan"),
        "lon": float("nan"),
        "value": safe_float(pair_row.get(value_col)),
        "candidate_path": "",
        "resolved_candidate_path": "",
    }


def bbox_from_points(points: Sequence[Dict[str, object]], padding: float) -> Optional[Tuple[float, float, float, float]]:
    xs = [safe_float(p.get("lon")) for p in points]
    ys = [safe_float(p.get("lat")) for p in points]
    xs = [x for x in xs if np.isfinite(x)]
    ys = [y for y in ys if np.isfinite(y)]
    if not xs or not ys:
        return None

    minx, maxx = min(xs), max(xs)
    miny, maxy = min(ys), max(ys)

    if minx == maxx:
        minx -= padding
        maxx += padding
    else:
        width = maxx - minx
        minx -= max(padding, 0.25 * width)
        maxx += max(padding, 0.25 * width)

    if miny == maxy:
        miny -= padding
        maxy += padding
    else:
        height = maxy - miny
        miny -= max(padding, 0.25 * height)
        maxy += max(padding, 0.25 * height)

    return (minx, miny, maxx, maxy)


def expand_bbox(bbox: Tuple[float, float, float, float], factor: float) -> Tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bbox
    cx = (minx + maxx) / 2.0
    cy = (miny + maxy) / 2.0
    half_w = max((maxx - minx) * factor / 2.0, 0.01)
    half_h = max((maxy - miny) * factor / 2.0, 0.01)
    return (cx - half_w, cy - half_h, cx + half_w, cy + half_h)


def find_merit_river_files() -> List[Path]:
    roots = [MERIT_DIR / "pfaf_level_01", MERIT_DIR]
    files: List[Path] = []

    for root in roots:
        if root.is_dir():
            files.extend(sorted(root.glob("riv_pfaf_*.shp")))

    if not files and MERIT_DIR.is_dir():
        files.extend(sorted(MERIT_DIR.rglob("riv_pfaf_*.shp")))

    seen = set()
    unique = []
    for path in files:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)

    return unique


def normalize_crs(gdf):
    if gdf is None or gdf.empty:
        return gdf
    if gdf.crs is None:
        # MERIT Hydro vector data is distributed in lon/lat in normal setups.
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def read_merit_reaches_for_bbox(
    river_files: Sequence[Path],
    bbox: Tuple[float, float, float, float],
    notes: List[str],
):
    if gpd is None:
        notes.append("geopandas is not installed")
        return None

    frames = []
    for shp in river_files:
        try:
            gdf = gpd.read_file(shp, bbox=bbox)
            if gdf.empty:
                continue
            gdf = normalize_crs(gdf)
            gdf["_merit_file"] = shp.name
            frames.append(gdf)
        except Exception as exc:
            notes.append("could not read {}: {}".format(shp, exc))

    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True)
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")

    dedupe = []
    for col in ["COMID", "comid"]:
        if col in merged.columns:
            dedupe.append(col)
            break
    if "_merit_file" in merged.columns:
        dedupe.append("_merit_file")
    if dedupe:
        merged = merged.drop_duplicates(subset=dedupe, keep="first")

    return merged


def read_merit_reaches_near_points(
    river_files: Sequence[Path],
    points: Sequence[Dict[str, object]],
    notes: List[str],
):
    base_bbox = bbox_from_points(points, INITIAL_SEARCH_PADDING_DEG)
    if base_bbox is None:
        notes.append("cannot build bbox because station coordinates are missing")
        return None

    for factor in SEARCH_EXPANSION_FACTORS:
        bbox = expand_bbox(base_bbox, factor)
        gdf = read_merit_reaches_for_bbox(river_files, bbox, notes)
        if gdf is not None and not gdf.empty:
            return gdf

    notes.append("no MERIT reaches found after bbox expansion")
    return None


def nearest_reach(point: Dict[str, object], reaches) -> Dict[str, object]:
    info = {
        "COMID": "",
        "distance_m": np.nan,
        "uparea": np.nan,
        "merit_file": "",
    }

    if reaches is None or reaches.empty or gpd is None or Point is None:
        return info

    lon = safe_float(point.get("lon"))
    lat = safe_float(point.get("lat"))
    if not np.isfinite(lon) or not np.isfinite(lat):
        return info

    try:
        point_gdf = gpd.GeoDataFrame(
            [{"geometry": Point(lon, lat)}],
            geometry="geometry",
            crs="EPSG:4326",
        ).to_crs(DISTANCE_CRS)

        reaches_proj = reaches.to_crs(DISTANCE_CRS)
        distances = reaches_proj.geometry.distance(point_gdf.geometry.iloc[0])
        idx = distances.idxmin()
        rec = reaches.loc[idx]

        comid = ""
        for col in ["COMID", "comid"]:
            if col in reaches.columns:
                comid = clean_text(rec.get(col))
                break

        uparea = np.nan
        for col in ["uparea", "UPAREA", "upa", "UPLAND_SKM"]:
            if col in reaches.columns:
                uparea = safe_float(rec.get(col))
                break

        info.update(
            {
                "COMID": comid,
                "distance_m": float(distances.loc[idx]),
                "uparea": uparea,
                "merit_file": clean_text(rec.get("_merit_file", "")),
            }
        )
    except Exception:
        pass

    return info


def comid_relation(comid_a: str, comid_b: str) -> str:
    a = clean_text(comid_a)
    b = clean_text(comid_b)
    if not a or not b:
        return "missing_COMID"
    if a == b:
        return "same_COMID"
    return "different_COMID"


def distance_level(distance_a: float, distance_b: float) -> str:
    distances = [safe_float(distance_a), safe_float(distance_b)]
    finite = [d for d in distances if np.isfinite(d)]
    if len(finite) < 2:
        return "missing_distance"

    max_dist = max(finite)
    if max_dist <= GOOD_DISTANCE_M:
        return "good_distance"
    if max_dist <= WEAK_DISTANCE_M:
        return "moderate_distance"
    return "far_from_merit"


def final_qc_reason(row: pd.Series) -> str:
    """Combine error level and COMID relation.

    This does not declare RiverSed wrong automatically. It separates likely
    value anomalies from likely spatial mismatch cases.
    """
    qc = clean_text(row.get("qc_level"))
    relation = clean_text(row.get("comid_relation"))
    dist_level = clean_text(row.get("distance_level"))

    if qc == "OK":
        if relation == "same_COMID":
            return "ok_same_comid"
        if relation == "different_COMID":
            return "ok_but_different_comid"
        return "ok_missing_comid"

    if relation == "different_COMID":
        return "spatial_mismatch_different_comid"

    if relation == "missing_COMID":
        return "needs_review_missing_comid"

    if relation == "same_COMID":
        if dist_level in {"far_from_merit", "missing_distance"}:
            return "needs_review_same_comid_weak_distance"
        if qc in {"high_suspect", "suspect"}:
            return "value_suspect_same_comid"
        if qc == "review":
            return "review_same_comid"

    return "needs_review"


def add_merit_matches(target: pd.DataFrame, candidates: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    if gpd is None or Point is None:
        raise ImportError("geopandas and shapely are required for MERIT COMID matching")

    river_files = find_merit_river_files()
    if not river_files:
        raise FileNotFoundError("No riv_pfaf_*.shp files found under MERIT_DIR={}".format(MERIT_DIR))

    print("Found {} MERIT river shapefile(s)".format(len(river_files)))

    rows = []
    notes = []

    for i, (_, pair_row) in enumerate(target.iterrows(), start=1):
        if i % 10 == 0 or i == 1 or i == len(target):
            print("Matching MERIT COMIDs: {}/{}".format(i, len(target)))

        point_a = find_candidate_point(candidates, pair_row, "a")
        point_b = find_candidate_point(candidates, pair_row, "b")
        points = [point_a, point_b]

        reaches = read_merit_reaches_near_points(river_files, points, notes)
        match_a = nearest_reach(point_a, reaches)
        match_b = nearest_reach(point_b, reaches)

        row = pair_row.to_dict()

        row.update(
            {
                "source_a_label": point_a.get("source", ""),
                "source_b_label": point_b.get("source", ""),
                "lat_a": point_a.get("lat", np.nan),
                "lon_a": point_a.get("lon", np.nan),
                "lat_b": point_b.get("lat", np.nan),
                "lon_b": point_b.get("lon", np.nan),
                "candidate_path_a": point_a.get("candidate_path", ""),
                "candidate_path_b": point_b.get("candidate_path", ""),
                "merit_comid_a": match_a.get("COMID", ""),
                "merit_distance_m_a": match_a.get("distance_m", np.nan),
                "merit_uparea_a": match_a.get("uparea", np.nan),
                "merit_file_a": match_a.get("merit_file", ""),
                "merit_comid_b": match_b.get("COMID", ""),
                "merit_distance_m_b": match_b.get("distance_m", np.nan),
                "merit_uparea_b": match_b.get("uparea", np.nan),
                "merit_file_b": match_b.get("merit_file", ""),
            }
        )

        row["comid_relation"] = comid_relation(row["merit_comid_a"], row["merit_comid_b"])
        row["same_COMID"] = int(row["comid_relation"] == "same_COMID")
        row["distance_level"] = distance_level(row["merit_distance_m_a"], row["merit_distance_m_b"])
        row["qc_reason"] = final_qc_reason(pd.Series(row))

        rows.append(row)

    return pd.DataFrame(rows), notes


def summarize(detail: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Summary by COMID relation and QC reason.
    grouped = (
        detail.groupby(["comid_relation", "qc_level", "qc_reason"], dropna=False)
        .agg(
            n_records=("pct_error", "size"),
            n_clusters=("cluster_id", pd.Series.nunique),
            pct_error_median=("pct_error", "median"),
            pct_error_p90=("pct_error", lambda s: s.quantile(0.90)),
            pct_error_max=("pct_error", "max"),
            abs_diff_median=("abs_diff", "median"),
            abs_diff_max=("abs_diff", "max"),
            max_merit_distance_m_a=("merit_distance_m_a", "max"),
            max_merit_distance_m_b=("merit_distance_m_b", "max"),
        )
        .reset_index()
        .sort_values(["comid_relation", "qc_level", "n_records"], ascending=[True, True, False])
    )

    # Threshold summary.
    threshold_rows = []
    thresholds = [25, 50, 75, 100, 150, 200, 300]
    for relation in ["all", "same_COMID", "different_COMID", "missing_COMID"]:
        if relation == "all":
            sub = detail
        else:
            sub = detail[detail["comid_relation"] == relation]

        n = len(sub)
        for th in thresholds:
            count = int((sub["pct_error"] >= th).sum()) if n else 0
            threshold_rows.append(
                {
                    "comid_relation": relation,
                    "threshold": "pct_error >= {}%".format(th),
                    "n_records": count,
                    "total_records": n,
                    "fraction": count / n if n else np.nan,
                }
            )

        for th in [10, 25, 50, 100]:
            count = int((sub["abs_diff"] >= th).sum()) if n else 0
            threshold_rows.append(
                {
                    "comid_relation": relation,
                    "threshold": "abs_diff >= {}".format(th),
                    "n_records": count,
                    "total_records": n,
                    "fraction": count / n if n else np.nan,
                }
            )

    threshold_summary = pd.DataFrame(threshold_rows)

    return grouped, threshold_summary


def write_report(detail: pd.DataFrame, comid_summary: pd.DataFrame, threshold_summary: pd.DataFrame, notes: Sequence[str]) -> None:
    total = len(detail)
    n_clusters = detail["cluster_id"].nunique() if total else 0
    n_dates = detail["date"].nunique() if total else 0

    relation_counts = detail["comid_relation"].value_counts(dropna=False).to_dict()
    qc_counts = detail["qc_level"].value_counts(dropna=False).to_dict()
    reason_counts = detail["qc_reason"].value_counts(dropna=False).to_dict()

    pct_desc = detail["pct_error"].describe(percentiles=[0.5, 0.75, 0.9, 0.95]) if total else pd.Series(dtype=float)
    abs_desc = detail["abs_diff"].describe(percentiles=[0.5, 0.75, 0.9, 0.95]) if total else pd.Series(dtype=float)

    lines = [
        "# USGS vs RiverSed daily SSC MERIT COMID QC report",
        "",
        "## Inputs",
        "",
        "- pair records: `{}`".format(PAIR_RECORDS_CSV),
        "- candidates: `{}`".format(CANDIDATES_CSV_GZ),
        "- MERIT_DIR: `{}`".format(MERIT_DIR),
        "",
        "## Match counts",
        "",
        "- matched pair records: {:,}".format(total),
        "- unique clusters: {:,}".format(n_clusters),
        "- unique dates: {:,}".format(n_dates),
        "",
        "## COMID relation counts",
        "",
    ]

    for key, value in relation_counts.items():
        lines.append("- {}: {:,}".format(key, int(value)))

    lines.extend(["", "## Error-level counts", ""])
    for key, value in qc_counts.items():
        lines.append("- {}: {:,}".format(key, int(value)))

    lines.extend(["", "## QC reason counts", ""])
    for key, value in reason_counts.items():
        lines.append("- {}: {:,}".format(key, int(value)))

    lines.extend(["", "## pct_error distribution", ""])
    for key in ["count", "mean", "std", "min", "50%", "75%", "90%", "95%", "max"]:
        if key in pct_desc.index:
            lines.append("- {}: {}".format(key, fmt_num(pct_desc.loc[key], 6)))

    lines.extend(["", "## abs_diff distribution", ""])
    for key in ["count", "mean", "std", "min", "50%", "75%", "90%", "95%", "max"]:
        if key in abs_desc.index:
            lines.append("- {}: {}".format(key, fmt_num(abs_desc.loc[key], 6)))

    lines.extend(
        [
            "",
            "## Interpretation guide",
            "",
            "- `same_COMID` + high error: stronger evidence for `value_suspect_same_comid`.",
            "- `different_COMID` + high error: prioritize `spatial_mismatch_different_comid`; do not directly call RiverSed wrong.",
            "- `missing_COMID`: inspect MERIT matching distance/path manually.",
            "- `far_from_merit`: the COMID match itself is weak, so treat the row as needs review.",
            "",
            "## Output files",
            "",
            "- `{}`".format(OUT_DETAIL_CSV),
            "- `{}`".format(OUT_COMID_SUMMARY_CSV),
            "- `{}`".format(OUT_THRESHOLD_SUMMARY_CSV),
            "",
            "## Notes",
            "",
        ]
    )

    if notes:
        for note in notes[:100]:
            lines.append("- {}".format(note))
        if len(notes) > 100:
            lines.append("- ... {} more notes omitted".format(len(notes) - 100))
    else:
        lines.append("- No warnings.")

    OUT_REPORT_MD.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    print("PROJECT_ROOT:", PROJECT_ROOT)
    print("EARLY_DIR:", EARLY_DIR)
    print("MERIT_DIR:", MERIT_DIR)

    if gpd is None or Point is None:
        print("Error: geopandas and shapely are required.")
        print("Install example: pip install geopandas shapely pyproj pyogrio")
        return 1

    pairs, candidates = read_inputs()
    target = filter_target_pairs(pairs)
    target.to_csv(OUT_QC_FLAGS_CSV, index=False)
    print("Wrote:", OUT_QC_FLAGS_CSV)

    print("Target rows:", len(target))
    print("Unique clusters:", target["cluster_id"].nunique())
    print("Unique dates:", target["date"].nunique())

    detail, notes = add_merit_matches(target, candidates)
    detail.to_csv(OUT_DETAIL_CSV, index=False)
    print("Wrote:", OUT_DETAIL_CSV)

    comid_summary, threshold_summary = summarize(detail)
    comid_summary.to_csv(OUT_COMID_SUMMARY_CSV, index=False)
    threshold_summary.to_csv(OUT_THRESHOLD_SUMMARY_CSV, index=False)
    print("Wrote:", OUT_COMID_SUMMARY_CSV)
    print("Wrote:", OUT_THRESHOLD_SUMMARY_CSV)

    write_report(detail, comid_summary, threshold_summary, notes)
    print("Wrote:", OUT_REPORT_MD)

    print("")
    print("=== COMID relation counts ===")
    print(detail["comid_relation"].value_counts(dropna=False))

    print("")
    print("=== QC reason counts ===")
    print(detail["qc_reason"].value_counts(dropna=False))

    print("")
    print("=== high_suspect rows by COMID relation ===")
    high = detail[detail["qc_level"] == "high_suspect"]
    if high.empty:
        print("No high_suspect rows.")
    else:
        print(high["comid_relation"].value_counts(dropna=False))

    print("")
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
