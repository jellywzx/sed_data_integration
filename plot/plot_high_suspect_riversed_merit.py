#!/usr/bin/env python3
"""Standalone MERIT plots for high-suspect USGS vs RiverSed daily SSC records.

This script is fully standalone. It does NOT import or call any other plot_s5
script.

Inputs:
  1. scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_high_suspect_only.csv
     If this file is missing, it falls back to:
     scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_qc_flags.csv
     and filters qc_level == high_suspect.

  2. scripts_basin_test/output/early_validation_results/early_overlap_candidates.csv.gz

  3. MERIT Hydro river shapefiles:
     MERIT_DIR/pfaf_level_01/riv_pfaf_*.shp

Outputs:
  scripts_basin_test/output/early_validation_results/plots_high_suspect_merit/
    high_suspect_01_cluster_27981_2007-10-03.png
    ...
    high_suspect_plot_records.csv
    high_suspect_plot_notes.txt

Run:
  python plot_high_suspect_riversed_merit_standalone.py

If MERIT is not in the default location:
  export MERIT_DIR=/path/to/MERIT_Hydro_v07_Basins_v01_bugfix1
  python plot_high_suspect_riversed_merit_standalone.py
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
    """Find Output_r even if this script is stored in scripts_basin_test/plot/."""
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
    if (guessed / "scripts_basin_test" / "output").exists():
        return guessed

    return guessed


PROJECT_ROOT = find_output_r_root(SCRIPT_DIR)
EARLY_DIR = PROJECT_ROOT / "scripts_basin_test" / "output" / "early_validation_results"

HIGH_SUSPECT_CSV = EARLY_DIR / "usgs_riversed_daily_ssc_high_suspect_only.csv"
QC_FLAGS_CSV = EARLY_DIR / "usgs_riversed_daily_ssc_qc_flags.csv"
CANDIDATES_CSV_GZ = EARLY_DIR / "early_overlap_candidates.csv.gz"

OUT_DIR = EARLY_DIR / "plots_high_suspect_merit"

DEFAULT_MERIT_DIR = PROJECT_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
MERIT_DIR = Path(os.environ.get("MERIT_DIR", str(DEFAULT_MERIT_DIR))).expanduser().resolve()

TARGET_RESOLUTION = "daily"
TARGET_VARIABLE = "SSC"

# Plot order: largest abs_diff first, then pct_error.
SORT_BY_PRIORITY = True

# MERIT search / zoom settings.
INITIAL_SEARCH_PADDING_DEG = 0.35
SEARCH_EXPANSION_FACTORS = [1.0, 2.0, 4.0, 8.0]
ZOOM_PADDING_DEG = 0.055
ZOOM_PADDING_FRACTION = 0.35
MAX_MERIT_REACHES_TO_PLOT = 250

# Diagnostic distance CRS. For exact geodesic distances, use a local projected CRS.
DISTANCE_CRS = "EPSG:3857"


# ---------------------------------------------------------------------------
# Small utilities
# ---------------------------------------------------------------------------
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


def fmt_num(value, digits: int = 6) -> str:
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


# ---------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------
def load_high_suspect_table() -> pd.DataFrame:
    """Load the high-suspect rows.

    Preferred input is usgs_riversed_daily_ssc_high_suspect_only.csv. If it is
    missing, use usgs_riversed_daily_ssc_qc_flags.csv and filter qc_level.
    """
    if HIGH_SUSPECT_CSV.is_file():
        df = pd.read_csv(HIGH_SUSPECT_CSV)
        input_path = HIGH_SUSPECT_CSV
    elif QC_FLAGS_CSV.is_file():
        df = pd.read_csv(QC_FLAGS_CSV)
        input_path = QC_FLAGS_CSV
        if "qc_level" not in df.columns:
            raise ValueError("QC flags CSV has no qc_level column: {}".format(QC_FLAGS_CSV))
        df = df[df["qc_level"].astype(str).eq("high_suspect")].copy()
    else:
        raise FileNotFoundError(
            "Cannot find high-suspect input. Expected one of:\n"
            "  {}\n"
            "  {}".format(HIGH_SUSPECT_CSV, QC_FLAGS_CSV)
        )

    if df.empty:
        raise ValueError("High-suspect table is empty: {}".format(input_path))

    # Normalize value columns.
    if "USGS_SSC" not in df.columns and "value_a" in df.columns:
        df["USGS_SSC"] = df["value_a"]
    if "RiverSed_SSC" not in df.columns and "value_b" in df.columns:
        df["RiverSed_SSC"] = df["value_b"]

    if "diff_b_minus_a" not in df.columns:
        df["diff_b_minus_a"] = pd.to_numeric(df["RiverSed_SSC"], errors="coerce") - pd.to_numeric(df["USGS_SSC"], errors="coerce")
    if "abs_diff" not in df.columns:
        df["abs_diff"] = pd.to_numeric(df["diff_b_minus_a"], errors="coerce").abs()
    if "pct_error" not in df.columns:
        usgs = pd.to_numeric(df["USGS_SSC"], errors="coerce")
        riversed = pd.to_numeric(df["RiverSed_SSC"], errors="coerce")
        df["pct_error"] = (riversed - usgs).abs() / usgs.replace(0, np.nan).abs() * 100.0
    if "ratio_RiverSed_to_USGS" not in df.columns:
        eps = 1e-6
        df["ratio_RiverSed_to_USGS"] = (
            pd.to_numeric(df["RiverSed_SSC"], errors="coerce") + eps
        ) / (pd.to_numeric(df["USGS_SSC"], errors="coerce") + eps)

    for col in [
        "cluster_id",
        "USGS_SSC",
        "RiverSed_SSC",
        "diff_b_minus_a",
        "abs_diff",
        "pct_error",
        "ratio_RiverSed_to_USGS",
        "robust_log_ratio_z",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "date" in df.columns:
        df["date"] = df["date"].map(normalize_date)

    required = [
        "cluster_id",
        "date",
        "USGS_SSC",
        "RiverSed_SSC",
        "source_station_uid_a",
        "source_station_uid_b",
    ]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError("High-suspect table missing columns: {}".format(", ".join(missing)))

    if SORT_BY_PRIORITY:
        df = df.sort_values(["abs_diff", "pct_error"], ascending=[False, False], kind="mergesort")

    return df.reset_index(drop=True)


def load_candidates() -> pd.DataFrame:
    if not CANDIDATES_CSV_GZ.is_file():
        raise FileNotFoundError("Missing candidates file: {}".format(CANDIDATES_CSV_GZ))

    df = pd.read_csv(CANDIDATES_CSV_GZ)
    if "date" in df.columns:
        df["date"] = df["date"].map(normalize_date)
    df["_cluster_id_num"] = pd.to_numeric(df.get("cluster_id", np.nan), errors="coerce")
    return df


def find_candidate_point(candidates: pd.DataFrame, row: pd.Series, side: str) -> Dict[str, object]:
    """Find point coordinates from early_overlap_candidates.csv.gz.

    side='a' is USGS; side='b' is RiverSed.
    """
    uid_col = "source_station_uid_{}".format(side)
    value_col = "USGS_SSC" if side == "a" else "RiverSed_SSC"
    default_source = "USGS" if side == "a" else "RiverSed"

    cluster_id = safe_float(row.get("cluster_id"))
    date = normalize_date(row.get("date"))
    uid = clean_text(row.get(uid_col))

    mask = (
        candidates["_cluster_id_num"].eq(cluster_id)
        & candidates["resolution"].astype(str).eq(TARGET_RESOLUTION)
        & candidates["date"].astype(str).eq(date)
    )

    if uid and "source_station_uid" in candidates.columns:
        exact = candidates[mask & candidates["source_station_uid"].astype(str).eq(uid)]
        if not exact.empty:
            rec = exact.iloc[0].to_dict()
            return {
                "side": side,
                "source": clean_text(rec.get("source", default_source)) or default_source,
                "uid": uid,
                "lat": safe_float(rec.get("source_station_lat")),
                "lon": safe_float(rec.get("source_station_lon")),
                "value": safe_float(row.get(value_col)),
                "candidate_path": clean_text(rec.get("candidate_path")),
                "resolved_candidate_path": clean_text(rec.get("resolved_candidate_path")),
            }

    # Fallback: source-name match.
    if "source" in candidates.columns:
        exact = candidates[mask & candidates["source"].astype(str).str.lower().eq(default_source.lower())]
        if not exact.empty:
            rec = exact.iloc[0].to_dict()
            return {
                "side": side,
                "source": clean_text(rec.get("source", default_source)) or default_source,
                "uid": uid or clean_text(rec.get("source_station_uid")),
                "lat": safe_float(rec.get("source_station_lat")),
                "lon": safe_float(rec.get("source_station_lon")),
                "value": safe_float(row.get(value_col)),
                "candidate_path": clean_text(rec.get("candidate_path")),
                "resolved_candidate_path": clean_text(rec.get("resolved_candidate_path")),
            }

    return {
        "side": side,
        "source": default_source,
        "uid": uid,
        "lat": float("nan"),
        "lon": float("nan"),
        "value": safe_float(row.get(value_col)),
        "candidate_path": "",
        "resolved_candidate_path": "",
    }


# ---------------------------------------------------------------------------
# MERIT reading and nearest-reach matching
# ---------------------------------------------------------------------------
def find_merit_river_files() -> List[Path]:
    roots = [MERIT_DIR / "pfaf_level_01", MERIT_DIR]
    files = []
    for root in roots:
        if root.is_dir():
            files.extend(sorted(root.glob("riv_pfaf_*.shp")))

    if not files and MERIT_DIR.is_dir():
        files.extend(sorted(MERIT_DIR.rglob("riv_pfaf_*.shp")))

    seen = set()
    out = []
    for f in files:
        r = f.resolve()
        if r in seen:
            continue
        seen.add(r)
        out.append(f)
    return out


def normalize_crs(gdf):
    if gdf is None or gdf.empty:
        return gdf
    if gdf.crs is None:
        # MERIT Hydro vectors are normally lon/lat.
        return gdf.set_crs("EPSG:4326")
    return gdf.to_crs("EPSG:4326")


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


def read_merit_reaches_for_bbox(bbox: Tuple[float, float, float, float], notes: List[str]):
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
    for col in ["COMID", "comid"]:
        if col in merged.columns:
            dedupe.append(col)
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

    if reaches is None or reaches.empty:
        return info

    lon = safe_float(point.get("lon"))
    lat = safe_float(point.get("lat"))
    if not np.isfinite(lon) or not np.isfinite(lat):
        return info

    try:
        pnt = gpd.GeoDataFrame([{"geometry": Point(lon, lat)}], geometry="geometry", crs="EPSG:4326").to_crs(DISTANCE_CRS)
        reaches_proj = reaches.to_crs(DISTANCE_CRS)
        dist = reaches_proj.geometry.distance(pnt.geometry.iloc[0])
        idx = dist.idxmin()
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
                "distance_m": float(dist.loc[idx]),
                "uparea": uparea,
                "merit_file": clean_text(rec.get("_merit_file", "")),
                "geometry_index": idx,
                "bounds": rec.geometry.bounds,
            }
        )
    except Exception:
        pass

    return info


def bbox_from_bounds_and_points(bounds_list: Sequence[Tuple[float, float, float, float]], points: Sequence[Dict[str, object]]) -> Optional[Tuple[float, float, float, float]]:
    xs = []
    ys = []

    for bounds in bounds_list:
        if bounds is None:
            continue
        minx, miny, maxx, maxy = bounds
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


def trim_reaches(reaches):
    if reaches is None or reaches.empty:
        return reaches

    work = reaches.copy()
    uparea_col = None
    for col in ["uparea", "UPAREA", "upa", "UPLAND_SKM"]:
        if col in work.columns:
            uparea_col = col
            break

    if uparea_col is not None:
        work["_uparea_sort"] = pd.to_numeric(work[uparea_col], errors="coerce")
        work = work.sort_values("_uparea_sort", ascending=False, kind="mergesort")

    if len(work) > MAX_MERIT_REACHES_TO_PLOT:
        work = work.head(MAX_MERIT_REACHES_TO_PLOT).copy()

    return work


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------
def safe_filename(case_number: int, row: pd.Series) -> str:
    cluster = clean_text(row.get("cluster_id")).replace(".", "_")
    date = normalize_date(row.get("date"))
    return "high_suspect_{:02d}_cluster_{}_{}.png".format(case_number, cluster, date)


def plot_case(case_number: int, row: pd.Series, candidates: pd.DataFrame) -> Tuple[Path, Dict[str, object], List[str]]:
    notes: List[str] = []

    point_a = find_candidate_point(candidates, row, "a")
    point_b = find_candidate_point(candidates, row, "b")
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

    zoom_reaches = None
    if zoom_bbox is not None:
        zoom_reaches = read_merit_reaches_for_bbox(zoom_bbox, notes)
    if zoom_reaches is None or zoom_reaches.empty:
        zoom_reaches = broad_reaches
    zoom_reaches = trim_reaches(zoom_reaches)

    fig, ax = plt.subplots(figsize=(11.5, 8.8))

    if zoom_reaches is not None and not zoom_reaches.empty:
        zoom_reaches.plot(ax=ax, linewidth=1.0, alpha=0.55, label="MERIT reaches in zoom")
    else:
        notes.append("No MERIT reaches available to plot")

    # Highlight nearest reaches.
    for info, label in [
        (near_a, "nearest MERIT reach to USGS"),
        (near_b, "nearest MERIT reach to RiverSed"),
    ]:
        idx = info.get("geometry_index")
        if broad_reaches is not None and idx is not None and idx in broad_reaches.index:
            broad_reaches.loc[[idx]].plot(ax=ax, linewidth=4.0, alpha=0.95, label=label)

    # Label styles avoid overlap.
    label_styles = [
        {"xytext": (-105, 48), "ha": "right", "va": "bottom"},
        {"xytext": (105, -58), "ha": "left", "va": "top"},
    ]

    for point, marker, style in zip(points, ["o", "^"], label_styles):
        lon = safe_float(point.get("lon"))
        lat = safe_float(point.get("lat"))
        value = safe_float(point.get("value"))
        source = clean_text(point.get("source"))

        if np.isfinite(lon) and np.isfinite(lat):
            ax.scatter(
                [lon],
                [lat],
                s=180,
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
                xytext=style["xytext"],
                textcoords="offset points",
                fontsize=9,
                ha=style["ha"],
                va=style["va"],
                zorder=8,
                bbox=dict(boxstyle="round,pad=0.35", fc="white", ec="0.35", lw=0.8, alpha=0.92),
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
            notes.append("Missing coordinates for {} uid={}".format(source, point.get("uid")))

    lons = [safe_float(p.get("lon")) for p in points]
    lats = [safe_float(p.get("lat")) for p in points]
    if all(np.isfinite(v) for v in lons + lats):
        ax.plot(lons, lats, linestyle=":", linewidth=1.5, label="USGS-RiverSed link", zorder=4)

    if zoom_bbox is not None:
        minx, miny, maxx, maxy = zoom_bbox
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

    cluster_id = clean_text(row.get("cluster_id"))
    date = normalize_date(row.get("date"))
    usgs = fmt_num(row.get("USGS_SSC"), 6)
    riversed = fmt_num(row.get("RiverSed_SSC"), 6)
    abs_diff = fmt_num(row.get("abs_diff"), 6)
    pct_error = fmt_num(row.get("pct_error"), 6)
    ratio = fmt_num(row.get("ratio_RiverSed_to_USGS"), 6)

    title = (
        "High-suspect RiverSed check | USGS vs RiverSed daily SSC | case {}\n"
        "cluster_id={}, date={}, USGS={}, RiverSed={}, abs_diff={}, pct_error={}%, ratio={}"
    ).format(case_number, cluster_id, date, usgs, riversed, abs_diff, pct_error, ratio)
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
    out_path = OUT_DIR / safe_filename(case_number, row)
    fig.tight_layout()
    fig.savefig(out_path, dpi=260)
    plt.close(fig)

    rec = {
        "case_number": case_number,
        "cluster_id": row.get("cluster_id"),
        "date": date,
        "USGS_SSC": row.get("USGS_SSC"),
        "RiverSed_SSC": row.get("RiverSed_SSC"),
        "diff_b_minus_a": row.get("diff_b_minus_a"),
        "abs_diff": row.get("abs_diff"),
        "pct_error": row.get("pct_error"),
        "ratio_RiverSed_to_USGS": row.get("ratio_RiverSed_to_USGS"),
        "robust_log_ratio_z": row.get("robust_log_ratio_z"),
        "source_station_uid_a": row.get("source_station_uid_a"),
        "source_station_uid_b": row.get("source_station_uid_b"),
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

    return out_path, rec, notes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
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

    high = load_high_suspect_table()
    candidates = load_candidates()

    print("Loaded {} high-suspect records".format(len(high)))
    print("Output directory: {}".format(OUT_DIR))

    records = []
    all_notes = []

    for i, (_, row) in enumerate(high.iterrows(), start=1):
        out_path, rec, notes = plot_case(i, row, candidates)
        records.append(rec)
        all_notes.extend(["case {}: {}".format(i, n) for n in notes])
        print("Wrote {}".format(out_path))

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    records_csv = OUT_DIR / "high_suspect_plot_records.csv"
    pd.DataFrame(records).to_csv(records_csv, index=False)
    print("Wrote {}".format(records_csv))

    notes_path = OUT_DIR / "high_suspect_plot_notes.txt"
    lines = [
        "High-suspect RiverSed MERIT plots",
        "",
        "Standalone script: no other plotter imported.",
        "No s4/s7 GPKG files are required.",
        "MERIT_DIR: {}".format(MERIT_DIR),
        "Input high-suspect CSV: {}".format(HIGH_SUSPECT_CSV if HIGH_SUSPECT_CSV.is_file() else QC_FLAGS_CSV),
        "Candidate CSV: {}".format(CANDIDATES_CSV_GZ),
        "Output directory: {}".format(OUT_DIR),
        "",
        "Records plotted: {:,}".format(len(records)),
        "",
        "Notes:",
    ]
    if all_notes:
        lines.extend("- {}".format(n) for n in all_notes)
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
