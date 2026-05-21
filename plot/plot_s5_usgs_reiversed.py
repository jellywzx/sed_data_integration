#!/usr/bin/env python3
"""Zoomed s5 early-validation plot using MERIT Hydro river reaches only.

Purpose
-------
Make PNG maps for USGS vs RiverSed daily SSC overlap cases. The map is zoomed
so the MERIT river segment(s) and the two source-station points are visible.

This script does NOT require s4/s7 GPKG files.

Inputs from s5_early_validation_results.py:
  scripts_basin_test/output/early_validation_results/early_overlap_pair_records.csv
  scripts_basin_test/output/early_validation_results/early_overlap_candidates.csv.gz

MERIT input:
  MERIT_DIR/pfaf_level_01/riv_pfaf_*.shp

Default behavior:
  - target source_pair = USGS vs RiverSed
  - target resolution = daily
  - target variable = SSC
  - choose the two cases with largest pct_error, preferring different clusters
  - find nearest MERIT river reach for each source point
  - zoom to the two points plus their matched MERIT reaches
  - label SSC values and COMIDs directly on the figure
"""

from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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
    """Find Output_r even if this script lives under scripts_basin_test/plot."""
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

    guessed = get_output_r_root(script_dir)
    return guessed


PROJECT_ROOT = find_output_r_root(SCRIPT_DIR)
EARLY_DIR = PROJECT_ROOT / "scripts_basin_test" / "output" / "early_validation_results"
PAIR_RECORDS_CSV = EARLY_DIR / "early_overlap_pair_records.csv"
CANDIDATES_CSV_GZ = EARLY_DIR / "early_overlap_candidates.csv.gz"
OUT_DIR = EARLY_DIR / "plots_s5_merit_zoom"

DEFAULT_MERIT_DIR = PROJECT_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
MERIT_DIR = Path(os.environ.get("MERIT_DIR", str(DEFAULT_MERIT_DIR))).expanduser().resolve()

TARGET_SOURCE_PAIR = "USGS vs RiverSed"
TARGET_RESOLUTION = "daily"
TARGET_VARIABLE = "SSC"

N_CASES_TO_PLOT = 2
CASE_SELECTION = "largest_pct_error"  # or "largest_abs_diff"

# Initial search box around source points. If no MERIT reaches are found, this
# is expanded automatically.
INITIAL_SEARCH_PADDING_DEG = 0.35
SEARCH_EXPANSION_FACTORS = [1.0, 2.0, 4.0, 8.0]

# Final zoom padding after matched reaches have been found.
ZOOM_PADDING_DEG = 0.055
ZOOM_PADDING_FRACTION = 0.35

# Cap plotted river features for speed/readability.
MAX_MERIT_REACHES_TO_PLOT = 250

# Projected CRS for nearest distance. This is a diagnostic plot; for exact
# geodesic distances, use a local projected CRS.
DISTANCE_CRS = "EPSG:3857"


def clean_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def safe_float(value) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def fmt_num(value, digits: int = 5) -> str:
    x = safe_float(value)
    if not np.isfinite(x):
        return "NA"
    return "{:.{}g}".format(x, digits)


def normalize_date(value) -> str:
    text = clean_text(value)
    if not text:
        return ""
    try:
        return pd.Timestamp(text).strftime("%Y-%m-%d")
    except Exception:
        return text


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
    required = {"source_pair", "resolution", "variable", "pct_error", "cluster_id", "date"}
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

    target["pct_error"] = pd.to_numeric(target["pct_error"], errors="coerce")
    target["abs_diff"] = pd.to_numeric(target.get("abs_diff", np.nan), errors="coerce")
    target["cluster_id"] = pd.to_numeric(target["cluster_id"], errors="coerce")
    return target


def select_cases(target: pd.DataFrame) -> pd.DataFrame:
    if CASE_SELECTION == "largest_abs_diff":
        sort_cols = ["abs_diff", "pct_error"]
    else:
        sort_cols = ["pct_error", "abs_diff"]

    work = target.sort_values(sort_cols, ascending=[False, False], kind="mergesort").copy()

    selected = []
    used_clusters = set()
    for _, row in work.iterrows():
        cluster_key = clean_text(row.get("cluster_id", ""))
        if cluster_key in used_clusters:
            continue
        selected.append(row)
        used_clusters.add(cluster_key)
        if len(selected) >= N_CASES_TO_PLOT:
            break

    if len(selected) < N_CASES_TO_PLOT:
        used_idx = {int(row.name) for row in selected}
        for idx, row in work.iterrows():
            if int(idx) in used_idx:
                continue
            selected.append(row)
            if len(selected) >= N_CASES_TO_PLOT:
                break

    return pd.DataFrame(selected).reset_index(drop=True)


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
        "candidate_rank": candidate_rec.get("candidate_rank", ""),
    }


def find_candidate_point(candidates: pd.DataFrame, pair_row: pd.Series, side: str) -> Dict[str, object]:
    source_col = "source_{}".format(side)
    uid_col = "source_station_uid_{}".format(side)
    value_col = "value_{}".format(side)

    cluster_id = safe_float(pair_row.get("cluster_id"))
    date = normalize_date(pair_row.get("date"))
    source = clean_text(pair_row.get(source_col))
    uid = clean_text(pair_row.get(uid_col))

    cand = candidates.copy()
    cand["_cluster_id_num"] = pd.to_numeric(cand.get("cluster_id", np.nan), errors="coerce")

    mask = (
        cand["_cluster_id_num"].eq(cluster_id)
        & cand["resolution"].astype(str).eq(clean_text(pair_row.get("resolution")))
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
        sub["_value_diff"] = (pd.to_numeric(sub[TARGET_VARIABLE], errors="coerce") - expected).abs()
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
        "candidate_rank": "",
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
        w = maxx - minx
        minx -= max(padding, 0.25 * w)
        maxx += max(padding, 0.25 * w)

    if miny == maxy:
        miny -= padding
        maxy += padding
    else:
        h = maxy - miny
        miny -= max(padding, 0.25 * h)
        maxy += max(padding, 0.25 * h)

    return (minx, miny, maxx, maxy)


def expand_bbox(bbox: Tuple[float, float, float, float], factor: float) -> Tuple[float, float, float, float]:
    minx, miny, maxx, maxy = bbox
    cx = (minx + maxx) / 2.0
    cy = (miny + maxy) / 2.0
    half_w = max((maxx - minx) * factor / 2.0, 0.01)
    half_h = max((maxy - miny) * factor / 2.0, 0.01)
    return (cx - half_w, cy - half_h, cx + half_w, cy + half_h)


def bbox_from_bounds_and_points(bounds_list: Sequence[Tuple[float, float, float, float]], points: Sequence[Dict[str, object]]) -> Optional[Tuple[float, float, float, float]]:
    xs = []
    ys = []

    for minx, miny, maxx, maxy in bounds_list:
        if all(np.isfinite([minx, miny, maxx, maxy])):
            xs.extend([minx, maxx])
            ys.extend([miny, maxy])

    for p in points:
        lon = safe_float(p.get("lon"))
        lat = safe_float(p.get("lat"))
        if np.isfinite(lon) and np.isfinite(lat):
            xs.append(lon)
            ys.append(lat)

    if not xs or not ys:
        return None

    minx, maxx = min(xs), max(xs)
    miny, maxy = min(ys), max(ys)
    w = max(maxx - minx, 0.0)
    h = max(maxy - miny, 0.0)

    pad_x = max(ZOOM_PADDING_DEG, ZOOM_PADDING_FRACTION * w)
    pad_y = max(ZOOM_PADDING_DEG, ZOOM_PADDING_FRACTION * h)

    return (minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y)


def find_merit_river_files() -> List[Path]:
    roots = [MERIT_DIR / "pfaf_level_01", MERIT_DIR]
    files = []
    for root in roots:
        if root.is_dir():
            files.extend(sorted(root.glob("riv_pfaf_*.shp")))

    if not files and MERIT_DIR.is_dir():
        files.extend(sorted(MERIT_DIR.rglob("riv_pfaf_*.shp")))

    seen = set()
    unique = []
    for f in files:
        r = f.resolve()
        if r in seen:
            continue
        seen.add(r)
        unique.append(f)
    return unique


def normalize_crs(gdf):
    if gdf is None or gdf.empty:
        return gdf
    if gdf.crs is None:
        # MERIT Hydro shapefiles are lon/lat in normal distributions.
        gdf = gdf.set_crs("EPSG:4326")
    else:
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def read_merit_reaches_for_bbox(bbox: Tuple[float, float, float, float], notes: List[str]):
    if gpd is None:
        notes.append("geopandas is not installed")
        return None

    files = find_merit_river_files()
    if not files:
        notes.append("No riv_pfaf_*.shp found under MERIT_DIR={}".format(MERIT_DIR))
        return None

    frames = []
    for shp in files:
        try:
            gdf = gpd.read_file(shp, bbox=bbox)
            if gdf.empty:
                continue
            gdf = normalize_crs(gdf)
            gdf["_merit_file"] = shp.name
            frames.append(gdf)
        except Exception as exc:
            notes.append("Could not read {}: {}".format(shp, exc))

    if not frames:
        return None

    merged = pd.concat(frames, ignore_index=True)
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")

    dedupe = []
    for c in ["COMID", "comid"]:
        if c in merged.columns:
            dedupe.append(c)
            break
    if "_merit_file" in merged.columns:
        dedupe.append("_merit_file")
    if dedupe:
        merged = merged.drop_duplicates(subset=dedupe, keep="first")

    return merged


def read_merit_reaches_near_points(points: Sequence[Dict[str, object]], notes: List[str]):
    base_bbox = bbox_from_points(points, INITIAL_SEARCH_PADDING_DEG)
    if base_bbox is None:
        notes.append("Cannot build MERIT search bbox because station coordinates are missing")
        return None, None

    last_bbox = base_bbox
    for factor in SEARCH_EXPANSION_FACTORS:
        bbox = expand_bbox(base_bbox, factor)
        last_bbox = bbox
        gdf = read_merit_reaches_for_bbox(bbox, notes)
        if gdf is not None and not gdf.empty:
            notes.append("Loaded {} MERIT reaches with search factor {}".format(len(gdf), factor))
            return gdf, bbox

    notes.append("No MERIT reaches found after bbox expansion")
    return None, last_bbox


def nearest_reach(point: Dict[str, object], reaches) -> Dict[str, object]:
    info = {
        "COMID": "",
        "distance_m": np.nan,
        "uparea": np.nan,
        "merit_file": "",
        "geometry_index": None,
        "bounds": None,
    }
    if reaches is None or reaches.empty or gpd is None or Point is None:
        return info

    lon = safe_float(point.get("lon"))
    lat = safe_float(point.get("lat"))
    if not np.isfinite(lon) or not np.isfinite(lat):
        return info

    try:
        pnt = gpd.GeoDataFrame([{"geometry": Point(lon, lat)}], geometry="geometry", crs="EPSG:4326").to_crs(DISTANCE_CRS)
        proj = reaches.to_crs(DISTANCE_CRS)
        dist = proj.geometry.distance(pnt.geometry.iloc[0])
        idx = dist.idxmin()
        rec = reaches.loc[idx]

        comid = ""
        for c in ["COMID", "comid"]:
            if c in reaches.columns:
                comid = clean_text(rec.get(c))
                break

        uparea = np.nan
        for c in ["uparea", "UPAREA", "upa", "UPLAND_SKM"]:
            if c in reaches.columns:
                uparea = safe_float(rec.get(c))
                break

        geom_bounds = rec.geometry.bounds
        info.update(
            {
                "COMID": comid,
                "distance_m": float(dist.loc[idx]),
                "uparea": uparea,
                "merit_file": clean_text(rec.get("_merit_file", "")),
                "geometry_index": idx,
                "bounds": geom_bounds,
            }
        )
    except Exception:
        pass

    return info


def sort_and_trim_reaches(reaches):
    if reaches is None or reaches.empty:
        return reaches

    work = reaches.copy()
    uparea_col = None
    for c in ["uparea", "UPAREA", "upa", "UPLAND_SKM"]:
        if c in work.columns:
            uparea_col = c
            break

    if uparea_col is not None:
        work["_uparea_sort"] = pd.to_numeric(work[uparea_col], errors="coerce")
        work = work.sort_values("_uparea_sort", ascending=False, kind="mergesort")

    if len(work) > MAX_MERIT_REACHES_TO_PLOT:
        work = work.head(MAX_MERIT_REACHES_TO_PLOT).copy()
    return work


def plot_case(case_number: int, pair_row: pd.Series, candidates: pd.DataFrame) -> Tuple[Path, Dict[str, object], List[str]]:
    notes: List[str] = []

    point_a = find_candidate_point(candidates, pair_row, "a")
    point_b = find_candidate_point(candidates, pair_row, "b")
    points = [point_a, point_b]

    broad_reaches, search_bbox = read_merit_reaches_near_points(points, notes)
    near_a = nearest_reach(point_a, broad_reaches)
    near_b = nearest_reach(point_b, broad_reaches)

    bounds = []
    if near_a.get("bounds") is not None:
        bounds.append(near_a["bounds"])
    if near_b.get("bounds") is not None:
        bounds.append(near_b["bounds"])

    zoom_bbox = bbox_from_bounds_and_points(bounds, points)
    if zoom_bbox is None:
        zoom_bbox = search_bbox

    # Re-read reaches only for the zoomed region so the plotted map is local.
    zoom_reaches = None
    if zoom_bbox is not None:
        zoom_reaches = read_merit_reaches_for_bbox(zoom_bbox, notes)

    if zoom_reaches is None or zoom_reaches.empty:
        zoom_reaches = broad_reaches

    zoom_reaches = sort_and_trim_reaches(zoom_reaches)

    fig, ax = plt.subplots(figsize=(11, 8.5))

    if zoom_reaches is not None and not zoom_reaches.empty:
        zoom_reaches.plot(ax=ax, linewidth=1.0, alpha=0.55, label="MERIT reaches in zoom")
    else:
        notes.append("No MERIT reaches available to plot")

    # Highlight nearest reaches explicitly.
    for info, label in [
        (near_a, "nearest MERIT reach to USGS"),
        (near_b, "nearest MERIT reach to RiverSed"),
    ]:
        idx = info.get("geometry_index")
        if broad_reaches is not None and idx is not None and idx in broad_reaches.index:
            broad_reaches.loc[[idx]].plot(ax=ax, linewidth=4.0, alpha=0.95, label=label)

    # Draw and label points.
    # Labels are deliberately offset in opposite directions with connector
    # arrows so USGS and RiverSed text does not overlap when the two stations
    # are very close together.
    label_styles = [
        {
            "xytext": (-95, 42),
            "ha": "right",
            "va": "bottom",
            "bbox_fc": "white",
        },
        {
            "xytext": (95, -52),
            "ha": "left",
            "va": "top",
            "bbox_fc": "white",
        },
    ]

    for point, marker, label_style in zip(points, ["o", "^"], label_styles):
        lon = safe_float(point.get("lon"))
        lat = safe_float(point.get("lat"))
        value = safe_float(point.get("value"))
        source = clean_text(point.get("source"))

        if np.isfinite(lon) and np.isfinite(lat):
            ax.scatter(
                [lon],
                [lat],
                s=170,
                marker=marker,
                edgecolors="black",
                linewidths=0.9,
                label="{} station".format(source),
                zorder=7,
            )
            label = "{}\nSSC={}\nlat={}\nlon={}".format(
                source,
                fmt_num(value, 6),
                fmt_num(lat, 6),
                fmt_num(lon, 6),
            )
            ax.annotate(
                label,
                xy=(lon, lat),
                xytext=label_style["xytext"],
                textcoords="offset points",
                fontsize=9,
                ha=label_style["ha"],
                va=label_style["va"],
                zorder=8,
                bbox=dict(
                    boxstyle="round,pad=0.35",
                    fc=label_style["bbox_fc"],
                    ec="0.35",
                    lw=0.8,
                    alpha=0.92,
                ),
                arrowprops=dict(
                    arrowstyle="->",
                    lw=0.8,
                    color="0.25",
                    shrinkA=4,
                    shrinkB=5,
                    connectionstyle="arc3,rad=0.15",
                ),
            )
        else:
            notes.append("Missing coordinates for {}".format(source))

    # Connect source points.
    lons = [safe_float(p.get("lon")) for p in points]
    lats = [safe_float(p.get("lat")) for p in points]
    if all(np.isfinite(v) for v in lons + lats):
        ax.plot(lons, lats, linestyle=":", linewidth=1.5, label="USGS-RiverSed link", zorder=4)

    if zoom_bbox is not None:
        minx, miny, maxx, maxy = zoom_bbox
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

    cluster_id = clean_text(pair_row.get("cluster_id"))
    date = normalize_date(pair_row.get("date"))
    pct_error = fmt_num(pair_row.get("pct_error"), 6)
    diff = fmt_num(pair_row.get("diff_b_minus_a"), 6)
    value_a = fmt_num(pair_row.get("value_a"), 6)
    value_b = fmt_num(pair_row.get("value_b"), 6)

    title = (
        "Zoomed MERIT river map | USGS vs RiverSed daily SSC | case {}\n"
        "cluster_id={}, date={}, USGS={}, RiverSed={}, RiverSed-USGS={}, pct_error={}%" 
    ).format(case_number, cluster_id, date, value_a, value_b, diff, pct_error)
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linewidth=0.4, alpha=0.35)

    merit_text = (
        "Nearest MERIT reaches\n"
        "USGS: COMID={a_comid}, dist={a_dist} m, uparea={a_uparea}, file={a_file}\n"
        "RiverSed: COMID={b_comid}, dist={b_dist} m, uparea={b_uparea}, file={b_file}"
    ).format(
        a_comid=near_a.get("COMID", ""),
        a_dist=fmt_num(near_a.get("distance_m"), 6),
        a_uparea=fmt_num(near_a.get("uparea"), 6),
        a_file=near_a.get("merit_file", ""),
        b_comid=near_b.get("COMID", ""),
        b_dist=fmt_num(near_b.get("distance_m"), 6),
        b_uparea=fmt_num(near_b.get("uparea"), 6),
        b_file=near_b.get("merit_file", ""),
    )
    ax.text(0.02, 0.02, merit_text, transform=ax.transAxes, fontsize=9, va="bottom")

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        unique = {}
        for handle, label in zip(handles, labels):
            if label not in unique:
                unique[label] = handle
        ax.legend(unique.values(), unique.keys(), loc="best", fontsize=8)

    try:
        ax.set_aspect("equal", adjustable="box")
    except Exception:
        pass

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "plot_s5_usgs_riversed_daily_ssc_merit_zoom_case_{:02d}.png".format(case_number)
    fig.tight_layout()
    fig.savefig(out_path, dpi=260)
    plt.close(fig)

    record = {
        "case_number": case_number,
        "cluster_id": pair_row.get("cluster_id"),
        "date": date,
        "resolution": pair_row.get("resolution"),
        "variable": pair_row.get("variable"),
        "source_pair": pair_row.get("source_pair"),
        "value_a": pair_row.get("value_a"),
        "value_b": pair_row.get("value_b"),
        "diff_b_minus_a": pair_row.get("diff_b_minus_a"),
        "abs_diff": pair_row.get("abs_diff"),
        "pct_error": pair_row.get("pct_error"),
        "source_a": pair_row.get("source_a"),
        "source_b": pair_row.get("source_b"),
        "lat_a": point_a.get("lat"),
        "lon_a": point_a.get("lon"),
        "lat_b": point_b.get("lat"),
        "lon_b": point_b.get("lon"),
        "candidate_path_a": point_a.get("candidate_path"),
        "candidate_path_b": point_b.get("candidate_path"),
        "merit_comid_a": near_a.get("COMID"),
        "merit_distance_m_a": near_a.get("distance_m"),
        "merit_uparea_a": near_a.get("uparea"),
        "merit_file_a": near_a.get("merit_file"),
        "merit_comid_b": near_b.get("COMID"),
        "merit_distance_m_b": near_b.get("distance_m"),
        "merit_uparea_b": near_b.get("uparea"),
        "merit_file_b": near_b.get("merit_file"),
        "plot_path": str(out_path),
    }

    return out_path, record, notes


def main() -> int:
    print("PROJECT_ROOT: {}".format(PROJECT_ROOT))
    print("EARLY_DIR: {}".format(EARLY_DIR))
    print("MERIT_DIR: {}".format(MERIT_DIR))
    print("GPKG context: disabled; using MERIT river reaches only")

    if gpd is None or Point is None:
        print("Error: geopandas and shapely are required.")
        print("Install example: pip install geopandas shapely pyproj pyogrio")
        return 1

    river_files = find_merit_river_files()
    print("Found {} MERIT river shapefile(s)".format(len(river_files)))
    if not river_files:
        print("Error: no MERIT river shapefiles found. Check MERIT_DIR.")
        return 1

    pairs, candidates = read_inputs()
    target = filter_target_pairs(pairs)
    print(
        "Found {:,} rows for {} / {} / {}".format(
            len(target), TARGET_SOURCE_PAIR, TARGET_RESOLUTION, TARGET_VARIABLE
        )
    )

    selected = select_cases(target)
    print("Selected {} case(s) using strategy: {}".format(len(selected), CASE_SELECTION))

    records = []
    all_notes = []

    for i, (_, row) in enumerate(selected.iterrows(), start=1):
        out_path, record, notes = plot_case(i, row, candidates)
        records.append(record)
        all_notes.extend(["case {}: {}".format(i, note) for note in notes])
        print("Wrote {}".format(out_path))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    selected_csv = OUT_DIR / "plot_s5_usgs_riversed_daily_ssc_merit_zoom_selected_records.csv"
    pd.DataFrame(records).to_csv(selected_csv, index=False)
    print("Wrote {}".format(selected_csv))

    notes_path = OUT_DIR / "plot_s5_usgs_riversed_daily_ssc_merit_zoom_notes.txt"
    lines = [
        "Zoomed MERIT plot notes",
        "",
        "No s4/s7 GPKG files are required.",
        "MERIT_DIR: {}".format(MERIT_DIR),
        "Output directory: {}".format(OUT_DIR),
        "",
        "Target rows: {:,}".format(len(target)),
        "Selected cases: {:,}".format(len(records)),
        "Case selection: {}".format(CASE_SELECTION),
        "",
        "Zoom settings:",
        "  INITIAL_SEARCH_PADDING_DEG = {}".format(INITIAL_SEARCH_PADDING_DEG),
        "  ZOOM_PADDING_DEG = {}".format(ZOOM_PADDING_DEG),
        "  ZOOM_PADDING_FRACTION = {}".format(ZOOM_PADDING_FRACTION),
        "",
        "Notes:",
    ]
    if all_notes:
        lines.extend("- {}".format(note) for note in all_notes)
    else:
        lines.append("- No warnings.")
    notes_path.write_text("\n".join(lines), encoding="utf-8")
    print("Wrote {}".format(notes_path))

    print("")
    print("Done. Open PNG files in:")
    print("  {}".format(OUT_DIR))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
