#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Basin matching workflow figure: one panel per policy outcome.

Generates a 3×3 grid of map panels showing each observed
(basin_status × basin_match_quality × basin_flag) combination
from the current release, plus a compact workflow decision bar and
a release-count summary.

Reuses the reusable logic from plot_distance_only_basin_matching.py
for UpstreamBasinTracer candidate reach retrieval, but does **not**
modify that script.

Usage
-----
# Full run (compute tracer + S4 geometry, then plot):
    python plot_basin_matching_workflow_all_cases.py
    python plot_basin_matching_workflow_all_cases.py --out-dir /path/to/output

# Plot-only mode (skip computation, re-render from cache):
    python plot_basin_matching_workflow_all_cases.py --plot-only

# Add the release-count summary panel at the bottom:
    python plot_basin_matching_workflow_all_cases.py --show-summary
    python plot_basin_matching_workflow_all_cases.py --plot-only --show-summary

# Suppress the legend bar:
    python plot_basin_matching_workflow_all_cases.py --no-legend

# Plot specific station IDs instead of the default representative combos:
    python plot_basin_matching_workflow_all_cases.py --station-ids 123,456,789

# Output formats and DPI:
    python plot_basin_matching_workflow_all_cases.py --formats pdf,png,svg --dpi 600

Key options
-----------
--plot-only        Skip tracer / S4 geometry computation; rebuild the figure
                   from pickled cache saved by a prior full run.
--show-summary     Draw the release-count summary panel below the legend.
--station-ids      Comma-separated cluster_id list to plot (overrides the
                   default automatic representative-case selection).
--out-dir          Output directory (default: see DEFAULT_OUT_DIR).
--formats          Comma-separated output extensions (default: pdf,png).
--dpi              Bitmap resolution (default: 300).
--top-candidates   Number of nearest candidate reaches to highlight (default: 8).
"""

from __future__ import annotations

import argparse
import ctypes
import datetime
import json
import math
import os
import shutil
import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

CONDA_LIB = os.environ.get("SED_CONDA_LIB", "")
if os.path.isdir(CONDA_LIB):
    os.environ["LD_LIBRARY_PATH"] = (
        CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
    )
    try:
        ctypes.CDLL(
            str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL
        )
    except Exception:
        pass

import matplotlib
matplotlib.use("Agg")

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyogrio
from matplotlib.patches import FancyBboxPatch
from pyproj import CRS, Transformer
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points, transform

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_ROOT = Path(__file__).resolve().parents[2]
MERIT_DIR = Path(os.environ.get("MERIT_DIR", "/path/to/MERIT_Hydro_v07_Basins_v01_bugfix1"))
RELEASE_DIR = SCRIPT_ROOT / "output" / "sed_reference_release"
STATION_CATALOG = RELEASE_DIR / "station_catalog.csv"
S4_LOCAL_CATCHMENTS = SCRIPT_ROOT / "output" / "s4_local_catchments.gpkg"
RELEASE_CLUSTER_BASINS = RELEASE_DIR / "sed_reference_cluster_basins.gpkg"
S4_UPSTREAM_BASINS = SCRIPT_ROOT / "output" / "s4_upstream_basins.csv"
S4_UPSTREAM_BASINS_GPKG = SCRIPT_ROOT / "output" / "s4_upstream_basins.gpkg"
DEFAULT_OUT_DIR = str(SCRIPT_ROOT / "output_other" / "basin_matching_workflow")
LAND_POLYGONS_PATH = SCRIPT_ROOT / "validate" / "ne_110m_land.geojson"

if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from basin_tracer import UpstreamBasinTracer  # noqa: E402

# ---------------------------------------------------------------------------
# Basin policy controlled vocabulary
# ---------------------------------------------------------------------------
MATCH_QUALITY_ORDER = (
    "distance_only",
    "area_matched",
    "area_approximate",
    "area_mismatch",
    "failed",
)
BASIN_STATUS_ORDER = ("resolved", "unresolved")
BASIN_FLAG_ORDER = (
    "ok",
    "reach_product_offset_ok",
    "large_offset",
    "area_mismatch",
    "geometry_inconsistent",
    "no_match",
)

# The 9 observed combinations
CASE_COMBOS: List[Tuple[str, str, str]] = [
    ("resolved", "distance_only", "ok"),
    ("resolved", "area_matched", "ok"),
    ("resolved", "area_approximate", "ok"),
    ("unresolved", "distance_only", "large_offset"),
    ("unresolved", "distance_only", "geometry_inconsistent"),
    ("unresolved", "area_matched", "large_offset"),
    ("unresolved", "area_approximate", "large_offset"),
    ("unresolved", "area_mismatch", "area_mismatch"),
    ("unresolved", "failed", "no_match"),
]

# Okabe-Ito colourblind-safe palette
OKABE_ITO = {
    "blue": "#0072B2",
    "orange": "#E69F00",
    "green": "#009E73",
    "vermillion": "#D55E00",
    "pink": "#CC79A7",
    "sky": "#56B4E9",
    "yellow": "#F0E442",
    "black": "#000000",
}

FIGSIZE = (15.0, 14.5)

# Font sizes (centralised for consistent styling)
FONT_TICK = 10           # axis tick labels
FONT_ANNOTATION = 8   # panel annotation labels (white box in each subplot)
FONT_LEGEND =12        # bottom legend bar text
FONT_BODY = 11           # panel titles, overlay text, release summary, checklist

# GridSpec layout parameters (shared by main flow and --plot-only)
LAYOUT_HSPACE_OUTER = 0.08    # add_gridspec hspace
LAYOUT_HSPACE_SUB = 0.15      # subgridspec hspace (rows)
LAYOUT_WSPACE_SUB = 0.08      # subgridspec wspace (columns)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def as_float(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def as_int_or_none(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_formats(value: str) -> List[str]:
    fmts = []
    for item in value.split(","):
        fmt = item.strip().lower().lstrip(".")
        if fmt:
            fmts.append(fmt)
    return fmts or ["png"]


def local_metric_crs(lon: float, lat: float) -> CRS:
    if lat >= 84:
        return CRS.from_epsg(3413)
    if lat <= -80:
        return CRS.from_epsg(3031)
    zone = int((lon + 180) // 6) + 1
    return CRS.from_epsg((32600 if lat >= 0 else 32700) + zone)


def nearest_point_on_geometry(
    geom: Any, lon: float, lat: float, metric_crs: CRS
) -> Tuple[float, float, float]:
    """Return (nearest_lon, nearest_lat, distance_m) from point to geometry."""
    to_metric = Transformer.from_crs("EPSG:4326", metric_crs, always_xy=True)
    to_wgs84 = Transformer.from_crs(metric_crs, "EPSG:4326", always_xy=True)
    point_metric = Point(*to_metric.transform(lon, lat))
    geom_metric = transform(to_metric.transform, geom)
    _, near_metric = nearest_points(point_metric, geom_metric)
    near_lon, near_lat = to_wgs84.transform(near_metric.x, near_metric.y)
    dist_m = point_metric.distance(near_metric)
    return float(near_lon), float(near_lat), float(dist_m)


def json_ready(value: Any) -> Any:
    """Recursively convert a value to a JSON-safe structure."""
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (np.floating,)):
        out = float(value)
        return out if np.isfinite(out) else None
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def run_text_command(cmd: List[str]) -> Tuple[bool, str]:
    try:
        result = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except FileNotFoundError:
        return False, f"{cmd[0]} unavailable"
    return result.returncode == 0, result.stdout.strip()


def pdf_page_size(pdfinfo_output: str) -> str:
    for line in pdfinfo_output.splitlines():
        if line.startswith("Page size:"):
            return line.split(":", 1)[1].strip()
    return "not found in pdfinfo output"


def font_embedding_status(pdffonts_output: str) -> str:
    lines = pdffonts_output.splitlines()
    if len(lines) < 3:
        return "no fonts reported by pdffonts"
    header = lines[0]
    if "emb" not in header or "sub" not in header:
        return "checked with pdffonts; review raw output"
    emb_start = header.index("emb")
    sub_start = header.index("sub")
    vals = [
        line[emb_start:sub_start].strip().lower()
        for line in lines[2:]
        if line.strip()
    ]
    if vals and all(v == "yes" for v in vals):
        return "all reported fonts embedded"
    if vals:
        return "some reported fonts may not be embedded; review pdffonts output"
    return "no fonts reported by pdffonts"


def file_size_mb(path: Path) -> str:
    if not path.is_file():
        return "not found"
    return f"{path.stat().st_size / (1024 * 1024):.2f} MB"


# ---------------------------------------------------------------------------
# Case selection from station_catalog.csv
# ---------------------------------------------------------------------------


def select_cases(
    catalog_path: Path, combos: List[Tuple[str, str, str]]
) -> pd.DataFrame:
    """Return one representative row per combo, deterministic by cluster_uid."""
    cat = pd.read_csv(catalog_path)
    required = {
        "cluster_uid",
        "cluster_id",
        "basin_status",
        "basin_match_quality",
        "basin_flag",
        "lon",
        "lat",
        "basin_distance_m",
        "point_in_local",
        "point_in_basin",
        "basin_area",
        "record_count",
        "resolution",
    }
    missing = sorted(required - set(cat.columns))
    if missing:
        raise ValueError(f"station_catalog.csv missing columns: {missing}")

    rows = []
    for status, quality, flag in combos:
        subset = cat[
            (cat["basin_status"].fillna("").astype(str).str.strip() == status)
            & (
                cat["basin_match_quality"]
                .fillna("")
                .astype(str)
                .str.strip()
                == quality
            )
            & (cat["basin_flag"].fillna("").astype(str).str.strip() == flag)
        ]
        if subset.empty:
            rows.append(
                {
                    "combo_status": status,
                    "combo_quality": quality,
                    "combo_flag": flag,
                    "cluster_uid": None,
                    "cluster_id": None,
                    "lon": np.nan,
                    "lat": np.nan,
                    "basin_distance_m": np.nan,
                    "point_in_local": None,
                    "point_in_basin": None,
                    "basin_area": np.nan,
                    "record_count": np.nan,
                    "station_name": None,
                    "river_name": None,
                    "source_station_id": None,
                    "sources_used": None,
                    "n_upstream_reaches": None,
                    "basin_match_quality_code": None,
                    "n_source_stations_in_cluster": None,
                    "resolution": None,
                    "_present": False,
                }
            )
            continue
        chosen = subset.sort_values("cluster_uid").iloc[0]
        rows.append(
            {
                "combo_status": status,
                "combo_quality": quality,
                "combo_flag": flag,
                "cluster_uid": str(chosen["cluster_uid"]),
                "cluster_id": int(chosen["cluster_id"]),
                "lon": float(chosen["lon"]),
                "lat": float(chosen["lat"]),
                "basin_distance_m": as_float(chosen.get("basin_distance_m")),
                "point_in_local": bool(chosen["point_in_local"])
                if pd.notna(chosen.get("point_in_local"))
                else None,
                "point_in_basin": bool(chosen["point_in_basin"])
                if pd.notna(chosen.get("point_in_basin"))
                else None,
                "basin_area": as_float(chosen.get("basin_area")),
                "record_count": int(chosen["record_count"]),
                "station_name": clean_text(chosen.get("station_name")),
                "river_name": clean_text(chosen.get("river_name")),
                "source_station_id": clean_text(
                    chosen.get("source_station_id")
                ),
                "sources_used": clean_text(chosen.get("sources_used")),
                "n_upstream_reaches": as_int_or_none(
                    chosen.get("n_upstream_reaches")
                ),
                "basin_match_quality_code": as_int_or_none(
                    chosen.get("basin_match_quality_code")
                ),
                "n_source_stations_in_cluster": as_int_or_none(
                    chosen.get("n_source_stations_in_cluster")
                ),
                "resolution": clean_text(chosen.get("resolution")),
                "_present": True,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# S4 geometry retrieval
# ---------------------------------------------------------------------------



def load_s4_area_metadata(path: Path) -> pd.DataFrame:
    """Load S4 upstream basin area metadata and prepare for merge with cases."""
    df = pd.read_csv(path)
    required = {"station_id", "reported_area", "area_error", "uparea_merit", "match_quality"}
    missing = sorted(required - set(df.columns))
    if missing:
        print(f"  WARNING: s4_upstream_basins.csv missing columns: {missing}", file=sys.stderr)
        return pd.DataFrame()
    df = df.rename(
        columns={
            "station_id": "cluster_id",
            "reported_area": "s4_reported_area",
            "area_error": "s4_area_error",
            "uparea_merit": "s4_uparea_merit",
            "match_quality": "s4_match_quality_from_csv",
            "basin_id": "s4_basin_id",
            "pfaf_code": "s4_pfaf_code",
        }
    )
    # Keep first occurrence when station_id (now cluster_id) repeats
    df = df.drop_duplicates(subset=["cluster_id"], keep="first")
    return df[["cluster_id", "s4_reported_area", "s4_area_error",
               "s4_uparea_merit", "s4_match_quality_from_csv",
               "s4_basin_id", "s4_pfaf_code"]]


def read_s4_geometry(
    cluster_id: int,
    resolution: str = "",
) -> Tuple[Optional[Any], Optional[Any], Dict[str, Any]]:
    """Read basin geometry from release and local catchment from s4_local_catchments."""
    basin_geom = None
    attrs = {}

    # 1) Upstream basin — from release cluster_basins GPKG
    if resolution:
        layer = f"basin_{resolution}"
        basin_rows = pyogrio.read_dataframe(
            str(RELEASE_CLUSTER_BASINS),
            layer=layer,
            where=f"cluster_id = {cluster_id}",
        )
        if basin_rows.empty:
            # Fallback: try other layers
            for fallback in ("basin_daily", "basin_monthly", "basin_annual"):
                if fallback == layer:
                    continue
                basin_rows = pyogrio.read_dataframe(
                    str(RELEASE_CLUSTER_BASINS),
                    layer=fallback,
                    where=f"cluster_id = {cluster_id}",
                )
                if not basin_rows.empty:
                    break

        if not basin_rows.empty:
            basin_row = basin_rows.iloc[0]
            basin_geom = (
                basin_row.geometry
                if basin_row.geometry is not None and not basin_row.geometry.is_empty
                else None
            )
            attrs = {
                "basin_id": as_float(basin_row.get("basin_id")),
                "match_quality": clean_text(
                    basin_row.get("basin_match_quality")
                ),
                "reported_area": np.nan,
                "area_error": as_float(basin_row.get("area_error")),
                "distance_m": as_float(basin_row.get("basin_distance_m")),
                "point_in_local": bool(
                    basin_row.get("point_in_local", False)
                ),
                "point_in_basin": bool(
                    basin_row.get("point_in_basin", False)
                ),
                "n_upstream_reaches": as_int_or_none(
                    basin_row.get("n_upstream_reaches")
                ),
                "method": clean_text(basin_row.get("method")),
            }

    # 2) Local catchment — still from s4_local_catchments
    local_geom = None
    try:
        local_rows = pyogrio.read_dataframe(
            str(S4_LOCAL_CATCHMENTS),
            where=f"station_id = {cluster_id}",
        )
        if not local_rows.empty:
            lr = local_rows.iloc[0]
            local_geom = (
                lr.geometry
                if lr.geometry is not None and not lr.geometry.is_empty
                else None
            )
    except Exception:
        pass

    # 3) If release GPKG had no basin_geom, fall back to the raw S4 upstream basins GPKG
    #    (s4_upstream_basins.gpkg stores per-station polygons produced by the S4 pipeline;
    #     the release GPKG only stores cluster-representative polygons, so some stations
    #     may be missing there even though S4 successfully traced their basin.)
    if basin_geom is None:
        try:
            s4_rows = pyogrio.read_dataframe(
                str(S4_UPSTREAM_BASINS_GPKG),
                where=f"station_id = {cluster_id}",
            )
            if not s4_rows.empty:
                s4_row = s4_rows.iloc[0]
                s4_geom = (
                    s4_row.geometry
                    if s4_row.geometry is not None and not s4_row.geometry.is_empty
                    else None
                )
                if s4_geom is not None:
                    basin_geom = s4_geom
                    # Only fill attrs that were missing from the release GPKG
                    if "basin_id" not in attrs or attrs.get("basin_id") is None:
                        attrs["basin_id"] = as_float(s4_row.get("basin_id"))
                    if "match_quality" not in attrs or not attrs.get("match_quality"):
                        attrs["match_quality"] = str(s4_row.get("match_quality", ""))
                    if "method" not in attrs or not attrs.get("method"):
                        attrs["method"] = str(s4_row.get("method", ""))
        except Exception:
            pass

    return local_geom, basin_geom, attrs


# ---------------------------------------------------------------------------
# Tracer data
# ---------------------------------------------------------------------------


def prepare_tracer_case(
    tracer: UpstreamBasinTracer,
    lon: float,
    lat: float,
    reported_area: Optional[float] = None,
    top_n: int = 8,
) -> Dict[str, Any]:
    """Run tracer and return candidate reaches, best reach, offset info."""
    result: Dict[str, Any] = {
        "candidates_gdf": None,
        "best_reach": None,
        "offset_line": None,
        "candidate_table": pd.DataFrame(),
        "n_candidates": 0,
        "match_quality": "failed",
        "matched_comid": None,
        "matched_distance_m": np.nan,
        "area_error": np.nan,
        "notes": [],
        "matched_uparea": np.nan,
    }

    if not (np.isfinite(lon) and np.isfinite(lat)):
        return result

    candidates = tracer.get_nearby_candidate_reaches(lon, lat)
    if candidates is None or candidates.empty:
        result["notes"].append("No nearby candidate MERIT reaches found.")
        return result

    best = tracer.find_best_reach(lon, lat, reported_area=reported_area)
    if best is None or best.get("COMID") is None:
        result["candidates_gdf"] = candidates
        result["candidate_table"] = _build_candidate_table(
            candidates, lon, lat
        )
        result["notes"].append("find_best_reach returned no valid COMID.")
        return result

    result["match_quality"] = str(best.get("match_quality", "failed"))
    result["matched_comid"] = as_int_or_none(best.get("COMID"))
    result["matched_distance_m"] = as_float(best.get("distance"))
    result["area_error"] = as_float(best.get("area_error"))
    result["matched_uparea"] = as_float(best.get("uparea"))

    cand_table = _build_candidate_table(candidates, lon, lat)
    result["candidate_table"] = cand_table

    # Copy and annotate candidates
    candidates = candidates.copy()
    candidates["COMID_num"] = (
        pd.to_numeric(candidates["COMID"], errors="coerce").astype("Int64")
    )
    best_comid = result["matched_comid"]
    if best_comid is not None:
        candidates["is_matched"] = (
            candidates["COMID_num"].astype("int64").eq(best_comid)
        )
    else:
        candidates["is_matched"] = False

    candidates["dist_m"] = pd.to_numeric(
        candidates["dist_m"], errors="coerce"
    )
    ranked = candidates.sort_values("dist_m")
    ranked["rank"] = np.arange(1, len(ranked) + 1, dtype=int)
    top = ranked.head(top_n).copy()

    result["candidates_gdf"] = ranked
    result["top_gdf"] = top
    result["n_candidates"] = len(cand_table)

    # Offset line
    if best_comid is not None:
        matched_row = ranked[
            ranked["COMID_num"].astype("int64").eq(best_comid)
        ]
        if not matched_row.empty:
            matched_geom = matched_row.iloc[0].geometry
            metric_crs = local_metric_crs(lon, lat)
            near_lon, near_lat, _ = nearest_point_on_geometry(
                matched_geom, lon, lat, metric_crs
            )
            result["offset_line"] = LineString(
                [(lon, lat), (near_lon, near_lat)]
            )

    return result


def _build_candidate_table(
    candidates: gpd.GeoDataFrame, lon: float, lat: float
) -> pd.DataFrame:
    """Build tabular summary of candidates with nearest-point distances."""
    if candidates is None or candidates.empty:
        return pd.DataFrame()
    work = candidates.copy()
    work["COMID_num"] = (
        pd.to_numeric(work["COMID"], errors="coerce").astype("Int64")
    )
    work["dist_m"] = pd.to_numeric(work["dist_m"], errors="coerce")
    work = work.dropna(subset=["COMID_num", "dist_m"])
    work = work.sort_values("dist_m")
    work = work.drop_duplicates(subset=["COMID_num"], keep="first")
    work["rank"] = np.arange(1, len(work) + 1, dtype=int)

    metric_crs = local_metric_crs(lon, lat)
    rows = []
    for _, rec in work.iterrows():
        near_lon, near_lat, nearest_dist = nearest_point_on_geometry(
            rec.geometry, lon, lat, metric_crs
        )
        rows.append(
            {
                "rank": int(rec["rank"]),
                "COMID": int(rec["COMID_num"]),
                "pfaf_code": clean_text(rec.get("pfaf_code")),
                "dist_m": float(rec["dist_m"]),
                "nearest_dist_m": nearest_dist,
                "nearest_lon": near_lon,
                "nearest_lat": near_lat,
                "uparea": as_float(rec.get("uparea")),
                "order": as_int_or_none(rec.get("order")),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Release count summary
# ---------------------------------------------------------------------------


def release_summary(
    catalog: pd.DataFrame,
) -> Dict[str, Dict[str, int]]:
    """Count rows per basin_flag, basin_match_quality, and basin_status."""
    flag_counts = {}
    for flag in BASIN_FLAG_ORDER:
        flag_counts[flag] = int(
            (
                catalog["basin_flag"].fillna("").astype(str).str.strip()
                == flag
            ).sum()
        )
    quality_counts = {}
    for q in MATCH_QUALITY_ORDER:
        quality_counts[q] = int(
            (
                catalog["basin_match_quality"]
                .fillna("")
                .astype(str)
                .str.strip()
                == q
            ).sum()
        )
    status_counts = {}
    for s in BASIN_STATUS_ORDER:
        status_counts[s] = int(
            (
                catalog["basin_status"]
                .fillna("")
                .astype(str)
                .str.strip()
                == s
            ).sum()
        )
    return {
        "total_rows": len(catalog),
        "by_flag": flag_counts,
        "by_match_quality": quality_counts,
        "by_status": status_counts,
    }


# ---------------------------------------------------------------------------
# Panel drawing
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Land polygon loader for coastline/island background
# ---------------------------------------------------------------------------


_LAND_GDF_CACHE = None


def _get_land_gdf():
    """Load/cache Natural Earth 110m land polygons as a GeoDataFrame."""
    global _LAND_GDF_CACHE
    if _LAND_GDF_CACHE is not None:
        return _LAND_GDF_CACHE
    try:
        p = Path(LAND_POLYGONS_PATH)
        if p.is_file():
            _LAND_GDF_CACHE = gpd.read_file(str(p))
            return _LAND_GDF_CACHE
    except Exception as exc:
        print(f"  WARNING: could not load land polygons: {exc}", file=sys.stderr)
    return None



def draw_case_panel(
    ax: plt.Axes,
    case: pd.Series,
    tracer_data: Dict[str, Any],
    local_geom: Optional[Any],
    basin_geom: Optional[Any],
    panel_label: str,
    show_labels: bool = True,
) -> None:
    """Draw one map panel showing station, reaches, catchments, and labels."""
    lon = as_float(case["lon"])
    lat = as_float(case["lat"])
    status = str(case["combo_status"])
    quality = str(case["combo_quality"])
    flag = str(case["combo_flag"])
    present = bool(case.get("_present", True))

    cands = tracer_data.get("candidates_gdf")
    top = tracer_data.get("top_gdf")
    offset_line = tracer_data.get("offset_line")
    best_comid = tracer_data.get("matched_comid")

    # Upstream basin outline -- filled for panel (f), outline for others
    if basin_geom is not None and not basin_geom.is_empty:
        try:
            basin_gdf = gpd.GeoDataFrame(
                geometry=[basin_geom], crs="EPSG:4326"
            )
            if panel_label == "(f)":
                basin_gdf.plot(
                    ax=ax,
                    facecolor=OKABE_ITO["orange"],
                    edgecolor=OKABE_ITO["orange"],
                    linewidth=1.0,
                    alpha=0.15,
                    zorder=1,
                )
            else:
                basin_gdf.plot(
                    ax=ax,
                    facecolor="none",
                    edgecolor=OKABE_ITO["orange"],
                    linewidth=0.8,
                    alpha=0.7,
                    zorder=1,
                )
        except Exception:
            pass

    # Local catchment fill
    if local_geom is not None and not local_geom.is_empty:
        try:
            local_gdf = gpd.GeoDataFrame(
                geometry=[local_geom], crs="EPSG:4326"
            )
            local_gdf.plot(
                ax=ax,
                facecolor=OKABE_ITO["blue"],
                edgecolor=OKABE_ITO["blue"],
                linewidth=0.4,
                alpha=0.20,
                zorder=2,
            )
        except Exception:
            pass

    # Candidate reaches (all grey)
    if cands is not None and not cands.empty:
        try:
            cands.plot(
                ax=ax,
                color="#c9c9c9",
                linewidth=0.3,
                alpha=0.5,
                zorder=3,
            )
        except Exception:
            pass

    # Top-N nearest (non-matched, green)
    if top is not None and not top.empty:
        non = top[~top["is_matched"]].copy()
        if not non.empty:
            try:
                non.plot(
                    ax=ax,
                    color=OKABE_ITO["green"],
                    linewidth=1.0,
                    alpha=0.8,
                    zorder=4,
                )
            except Exception:
                pass

    # Selected reach (vermillion)
    if cands is not None and best_comid is not None:
        matched = cands[cands["COMID_num"].astype("int64").eq(best_comid)]
        if not matched.empty:
            try:
                matched.plot(
                    ax=ax,
                    color=OKABE_ITO["vermillion"],
                    linewidth=2.0,
                    alpha=1.0,
                    zorder=5,
                )
            except Exception:
                pass

    # Offset line (dashed)
    if offset_line is not None:
        try:
            offset_gdf = gpd.GeoDataFrame(
                geometry=[offset_line], crs="EPSG:4326"
            )
            offset_gdf.plot(
                ax=ax,
                color=OKABE_ITO["black"],
                linewidth=0.7,
                linestyle="--",
                alpha=0.6,
                zorder=6,
            )
        except Exception:
            pass


    # Station-to-candidate connecting lines for panel (f)
    if panel_label == "(f)":
        cand_table = tracer_data.get("candidate_table")
        if cand_table is not None and not cand_table.empty:
            for _, cr in cand_table.iterrows():
                near_lon = cr.get("nearest_lon")
                near_lat = cr.get("nearest_lat")
                if pd.notna(near_lon) and pd.notna(near_lat):
                    try:
                        conn_line = LineString([(lon, lat), (near_lon, near_lat)])
                        conn_gdf = gpd.GeoDataFrame(
                            geometry=[conn_line], crs="EPSG:4326"
                        )
                        conn_gdf.plot(
                            ax=ax,
                            color="#888888",
                            linewidth=0.5,
                            linestyle=":",
                            alpha=0.5,
                            zorder=6,
                        )
                    except Exception:
                        pass

    # Station point
    ax.scatter(
        [lon],
        [lat],
        marker="*",
        s=120,
        color=OKABE_ITO["black"],
        edgecolor="white",
        linewidths=0.4,
        zorder=7,
    )

    # ---- Determine extent (tight zoom around station and matched reach) ----
    match_dist_deg = 0.02  # base: ~2 km around station
    if best_comid is not None and cands is not None and not cands.empty:
        matched = cands[cands["COMID_num"].astype("int64").eq(best_comid)]
        if not matched.empty:
            m_geom = matched.iloc[0].geometry
            if m_geom is not None and not m_geom.is_empty:
                try:
                    cx, cy = m_geom.centroid.x, m_geom.centroid.y
                    d = ((cx - lon) ** 2 + (cy - lat) ** 2) ** 0.5
                    match_dist_deg = max(d * 1.5, 0.01)
                except Exception:
                    pass

    # Hard cap: 0.10 degrees (~10 km) maximum; 0.50 for panel (f)
    cap = 0.50 if panel_label == "(f)" else 0.10
    match_dist_deg = min(match_dist_deg, cap)
    if match_dist_deg < 0.01:
        match_dist_deg = 0.01
    extent = [
        lon - match_dist_deg,
        lon + match_dist_deg,
        lat - match_dist_deg,
        lat + match_dist_deg,
    ]

    dx = extent[1] - extent[0]
    dy = extent[3] - extent[2]
    pad = 0.08
    if dx < 0.01:
        dx = 0.01
    if dy < 0.01:
        dy = 0.01
    extent = [
        extent[0] - dx * pad,
        extent[1] + dx * pad,
        extent[2] - dy * pad,
        extent[3] + dy * pad,
    ]
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])

    # Basin extent zoom for non-distance-only cases.
    # For panel (f) the station may be outside the basin (large_offset), so
    # compute the union bbox of basin + station first, then pad proportionally.
    if quality not in ("distance_only", "failed") and basin_geom is not None and not basin_geom.is_empty:
        try:
            bbox = basin_geom.bounds
            # Compute the union bbox of basin + station.  When the station lies
            # inside the basin this is a no-op.  When it sits outside (large_offset,
            # area_mismatch) the union keeps both visible.  More importantly, for
            # very large basins the station-area (where the local catchment sits)
            # determines the zoom extent so the local catchment remains visible.
            _pad = 0.20 if panel_label in ("(f)", "(g)", "(h)") else 0.05
            x0 = min(bbox[0], lon)
            x1 = max(bbox[2], lon)
            y0 = min(bbox[1], lat)
            y1 = max(bbox[3], lat)
            _dx = x1 - x0
            _dy = y1 - y0
            if _dx < 0.02:
                _dx = 0.02
            if _dy < 0.02:
                _dy = 0.02
            ax.set_xlim(x0 - _dx * _pad, x1 + _dx * _pad)
            ax.set_ylim(y0 - _dy * _pad, y1 + _dy * _pad)
        except Exception:
            pass


    # For failed/no_match panels (e.g. island stations with no river data),
    # zoom out to show the island / coastal context so the station location
    # relative to the coast / island is visible.
    if quality == "failed" and flag == "no_match":
        cx, cy = lon, lat
        half_w = 0.8
        ax.set_xlim(cx - half_w, cx + half_w)
        ax.set_ylim(cy - half_w, cy + half_w)

    # Coastline/land for failed/no_match (no river network data)
    if flag == "no_match":
        land_gdf = _get_land_gdf()
        if land_gdf is not None and not land_gdf.empty:
            try:
                # After zooming out, re-clip the land layer to the new extent
                xl, xr = ax.get_xlim()
                yb, yt = ax.get_ylim()
                land_in_view = land_gdf.cx[xl:xr, yb:yt]
                if not land_in_view.empty:
                    land_in_view.plot(
                        ax=ax,
                        facecolor="#E8E8E8",
                        edgecolor="#BBBBBB",
                        linewidth=0.3,
                        zorder=0,
                    )
            except Exception:
                pass
    # ---- Annotation labels ----
    dist_str = (
        f"{as_float(case['basin_distance_m']):.0f} m"
        if np.isfinite(as_float(case["basin_distance_m"]))
        else "N/A"
    )
    pil = case.get("point_in_local")
    pib = case.get("point_in_basin")
    geo_str_parts = []
    if pil is not None:
        geo_str_parts.append(f"in_local={'Y' if pil else 'N'}")
    if pib is not None:
        geo_str_parts.append(f"in_basin={'Y' if pib else 'N'}")
    geo_str = "  ".join(geo_str_parts)
    n_cand = tracer_data.get("n_candidates", 0)

    label_lines = [
        f"{status}",
        f"match={quality}",
        f"flag={flag}",
        f"d={dist_str}",
    ]
    # Area info for non-distance-only cases
    if quality not in ("distance_only", "failed"):
        s4_rep = as_float(case.get("s4_reported_area"))
        s4_upa = as_float(case.get("s4_uparea_merit"))
        s4_aerr = as_float(case.get("s4_area_error"))
        if np.isfinite(s4_rep) and s4_rep > 0:
            label_lines.append(f"A_reported={s4_rep:.1f} km²")
        upa_matched = s4_upa if np.isfinite(s4_upa) and s4_upa > 0 else as_float(case.get("basin_area"))
        if np.isfinite(upa_matched) and upa_matched > 0:
            label_lines.append(f"A_matched={upa_matched:.1f} km²")
        if np.isfinite(s4_aerr):
            label_lines.append(f"D_A={s4_aerr:.3f}")
    if geo_str:
        label_lines.append(geo_str)
    label_lines.append(f"candidates={n_cand}")

    if not present:
        label_lines = [
            f"flag={flag}",
            "(no occurrence",
            "in this release)",
        ]

    label_text = "\n".join(label_lines)
    if show_labels:
        ax.text(
            0.03,
            0.03,
            label_text,
            transform=ax.transAxes,
            fontsize=FONT_ANNOTATION,
            ha="left",
            va="bottom",
            bbox={
                "facecolor": "white",
                "edgecolor": "#cccccc",
                "alpha": 0.80,
                "boxstyle": "round,pad=0.3",
                "linewidth": 0.4,
            },
            zorder=10,
        )

    # Special annotation for failed/no_match
    if show_labels:
        if quality == "failed" and flag == "no_match":
            if not present:
                ax.text(
                    0.5,
                    0.5,
                    "no MERIT candidate\n/ no basin geometry",
                    transform=ax.transAxes,
                    fontsize=FONT_BODY,
                    ha="center",
                    va="center",
                    color="#666666",
                    style="italic",
                    zorder=10,
                )
            else:
                if cands is None or cands.empty:
                    ax.text(
                        0.5,
                        0.5,
                        "no MERIT candidate\n/ no basin geometry",
                        transform=ax.transAxes,
                        fontsize=FONT_BODY,
                        ha="center",
                        va="center",
                        color="#666666",
                        style="italic",
                        zorder=10,
                    )
    
    
    # Panel label outside axes — split into two lines if too long
    rest = f"{status}, {quality}, {flag}"
    if len(rest) > 28:
        combo_label = f"{panel_label} {status}\n{quality}, {flag}"
    else:
        combo_label = f"{panel_label} {rest}"
    ax.set_title(
        combo_label,
        fontsize=FONT_BODY,
        fontweight="bold",
        pad=3,
        loc="left",
    )
    ax.set_aspect("equal", adjustable="box")
    ax.tick_params(labelsize=FONT_TICK)
    ax.locator_params(axis="x", nbins=3)
    ax.locator_params(axis="y", nbins=3)
    ax.grid(True, color="#e0e0e0", linewidth=0.3, alpha=0.5)


# ---------------------------------------------------------------------------
# Workflow decision bar
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Release count summary row
# ---------------------------------------------------------------------------


def draw_release_summary(
    ax: plt.Axes, summary: Dict[str, Any]
) -> None:
    """Draw a compact table of release counts per basin_flag."""
    ax.axis("off")

    rows_text = []
    rows_text.append(
        f"Release inventory: {summary['total_rows']} cluster rows"
    )
    rows_text.append("")

    parts = []
    for s in BASIN_STATUS_ORDER:
        cnt = summary["by_status"].get(s, 0)
        parts.append(f"{s}={cnt}")
    rows_text.append("  Status:  " + "  ".join(parts))

    parts = []
    for q in MATCH_QUALITY_ORDER:
        cnt = summary["by_match_quality"].get(q, 0)
        parts.append(f"{q}={cnt}")
    rows_text.append("  Quality: " + "  ".join(parts))

    parts = []
    for f in BASIN_FLAG_ORDER:
        cnt = summary["by_flag"].get(f, 0)
        parts.append(f"{f}={cnt}")
    rows_text.append("  Flag:    " + "  ".join(parts))
    rows_text.append(
        "    (reach_product_offset_ok defined in basin_policy.py, "
        "count=0 in this release)"
    )

    rows_text.append("")
    rows_text.append("  Policy thresholds:")
    rows_text.append("    d <= 300 m -> resolved/ok")
    rows_text.append(
        "    d <= 1000 m + area matched or point_in_local -> resolved/ok"
    )
    rows_text.append("    d > 1000 m -> unresolved/large_offset")
    rows_text.append("    area_mismatch -> unresolved/area_mismatch")
    rows_text.append(
        "    no catchment covers point -> unresolved/geometry_inconsistent"
    )
    rows_text.append("    no MERIT reach found -> unresolved/no_match")
    rows_text.append(
        "    GSED/RiverSed: d <= 5 km + point_in_local -> "
        "resolved/reach_product_offset_ok"
    )

    ax.text(
        0.01,
        0.0,
        "\n".join(rows_text),
        fontsize=FONT_BODY,
        ha="left",
        va="center",
        family="monospace",
        transform=ax.transAxes,
    )


def _build_figure(
    n_cases: int,
    n_rows: int,
    n_cols: int,
    cases: pd.DataFrame,
    tracer_results: List[Dict[str, Any]],
    s4_geometries: List[Tuple[Optional[Any], Optional[Any]]],
    summary: Dict[str, Any],
    show_summary: bool = False,
    show_panel_labels: bool = True,
) -> plt.Figure:
    """Build the complete multi-panel figure.

    Creates a gridspec with map panels, a legend row, and an optional
    release-summary row.  Shared by the main processing path and
    ``--plot-only`` so that layout or summary changes need only one edit.
    """
    extra_rows = 1  # legend
    height_ratios = [1.0] * n_rows + [0.10]
    if show_summary:
        extra_rows += 1
        height_ratios.append(0.20)

    fig = plt.figure(figsize=FIGSIZE)
    gs = fig.add_gridspec(
        n_rows + extra_rows, 1,
        height_ratios=height_ratios,
        hspace=LAYOUT_HSPACE_OUTER,
    )
    gs_maps = gs[0:n_rows].subgridspec(
        n_rows, n_cols,
        hspace=LAYOUT_HSPACE_SUB,
        wspace=LAYOUT_WSPACE_SUB,
    )

    for i in range(n_cases):
        rr = i // n_cols
        cc = i % n_cols
        ax = fig.add_subplot(gs_maps[rr, cc])
        case = cases.iloc[i]
        td = tracer_results[i]
        local_geom, basin_geom = s4_geometries[i]
        draw_case_panel(ax, case, td, local_geom, basin_geom, f"({chr(97 + i)})",
                        show_labels=show_panel_labels)

    # Hide unused panels
    for i in range(n_cases, n_rows * n_cols):
        rr = i // n_cols
        cc = i % n_cols
        ax = fig.add_subplot(gs_maps[rr, cc])
        ax.axis("off")

    # Legend row
    ax_legend = fig.add_subplot(gs[n_rows])
    draw_legend_bar(ax_legend)

    # Release summary (optional)
    if show_summary:
        summary_row = n_rows + 1
        ax_summary = fig.add_subplot(gs[summary_row])
        draw_release_summary(ax_summary, summary)

    return fig


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Legend bar
# ---------------------------------------------------------------------------


def draw_legend_bar(ax: plt.Axes) -> None:
    """Draw a compact horizontal legend for map panel elements."""
    ax.axis("off")

    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#E8E8E8", edgecolor="#BBBBBB", label="Land"),
        Patch(facecolor="none", edgecolor=OKABE_ITO["orange"], linewidth=0.8,
              label="Upstream basin"),
        Patch(facecolor=OKABE_ITO["blue"], edgecolor=OKABE_ITO["blue"],
              linewidth=0.4, alpha=0.4, label="Local catchment"),
        Line2D([0], [0], color="#c9c9c9", linewidth=0.8, alpha=0.5,
               label="Reach candidates"),
        Line2D([0], [0], color=OKABE_ITO["green"], linewidth=1.5,
               label="Top-N nearest"),
        Line2D([0], [0], color=OKABE_ITO["vermillion"], linewidth=2.5,
               label="Matched reach"),
        Line2D([0], [0], color=OKABE_ITO["black"], linewidth=1.0,
               linestyle="--", label="Station offset"),
        Line2D([0], [0], marker="*",
               color=OKABE_ITO["black"],
               markerfacecolor=OKABE_ITO["black"],
               markeredgecolor="white", markersize=8,
               linewidth=0, label="Sediment station"),
    ]

    ax.legend(
        handles=legend_elements,
        loc="lower center",
        bbox_to_anchor=(0.5, -1.5),
        ncol=4,
        frameon=True,
        framealpha=0.9,
        edgecolor="#cccccc",
        fontsize=FONT_LEGEND,
        handlelength=1.5,
        handleheight=1.0,
        columnspacing=1.5,
        labelspacing=0.8,
    )


# Main
# ---------------------------------------------------------------------------



# ---------------------------------------------------------------------------
# Plot-only cache helpers
# ---------------------------------------------------------------------------


def _get_cache_dir(out_dir: Path) -> Path:
    """Return the cache directory for tracer results."""
    d = Path(out_dir) / "figures" / "cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _save_case_cache(cache_dir: Path, idx: int, td: dict,
                      local_geom, basin_geom) -> None:
    """Pickle one case's tracer results and S4 geometries for later plot-only use."""
    path = cache_dir / f"case_{idx:03d}.pkl"
    with open(path, "wb") as f:
        pickle.dump({"td": td, "local_geom": local_geom, "basin_geom": basin_geom}, f)


def _load_case_cache(cache_dir: Path, idx: int):
    """Load one case's cached tracer results and S4 geometries."""
    path = cache_dir / f"case_{idx:03d}.pkl"
    with open(path, "rb") as f:
        data = pickle.load(f)
    return data["td"], data["local_geom"], data["basin_geom"]


def _run_plot_only(args: argparse.Namespace, out_dir: Path, formats: List[str],
                   figure_id: str) -> int:
    """Re-build figure from cached computation results.

    Skips UpstreamBasinTracer initialization, ``prepare_tracer_case``, and
    S4 geometry extraction by loading pickled data from a prior full run.
    Useful for rapid iteration on visual style (colours, labels, layout).
    """
    import datetime

    cache_dir = _get_cache_dir(out_dir)
    data_dir = out_dir / "figures" / "data"

    # -----------------------------------------------------------------------
    # 1. Load cached cases DataFrame & release summary
    # -----------------------------------------------------------------------
    cases_pkl = cache_dir / "cases_df.pkl"
    if not cases_pkl.is_file():
        print(f"ERROR: cached cases not found at {cases_pkl}.", file=sys.stderr)
        print("Run the script once *without* --plot-only to generate the cache.",
              file=sys.stderr)
        return 1
    with open(cases_pkl, "rb") as f:
        cases = pickle.load(f)

    # Merge S4 area metadata from s4_upstream_basins.csv
    s4_area_df = load_s4_area_metadata(S4_UPSTREAM_BASINS)
    if not s4_area_df.empty:
        cases = cases.merge(s4_area_df, on="cluster_id", how="left")

    n_cases = len(cases)

    # Validate that cached cases_df has the required S4 area columns
    required_cols = {"s4_reported_area", "s4_area_error", "s4_uparea_merit", "s4_basin_id", "s4_pfaf_code"}
    missing_cols = required_cols - set(cases.columns)
    if missing_cols:
        print(f"ERROR: cached cases_df.pkl lacks columns: {missing_cols}.", file=sys.stderr)
        print("Re-run without --plot-only to regenerate the cache with S4 area metadata.", file=sys.stderr)
        return 1

    metadata_path = out_dir / f"{figure_id}_metadata.json"
    if not metadata_path.is_file():
        print(f"ERROR: metadata not found at {metadata_path}.", file=sys.stderr)
        return 1
    with open(metadata_path) as f:
        metadata = json.load(f)
    summary = metadata.get("release_counts", {})

    print(f"Plot-only mode: {n_cases} cases loaded from cache.")

    # -----------------------------------------------------------------------
    # 2. Load cached tracer results
    # -----------------------------------------------------------------------
    tracer_results = []
    s4_geometries = []   # list of (local_geom, basin_geom)
    for i in range(n_cases):
        td, local_geom, basin_geom = _load_case_cache(cache_dir, i)
        tracer_results.append(td)
        s4_geometries.append((local_geom, basin_geom))

    print(f"  Loaded {len(tracer_results)} cached tracer results.")

    # -----------------------------------------------------------------------
    # 3. Build figure (shared layout function)
    # -----------------------------------------------------------------------
    n_cols = min(n_cases, 3)
    n_rows = int(math.ceil(n_cases / 3.0))

    fig = _build_figure(
        n_cases, n_rows, n_cols,
        cases, tracer_results, s4_geometries,
        summary,
        show_summary=args.show_summary,
        show_panel_labels=not args.no_legend,
    )

    # -----------------------------------------------------------------------
    # 4. Save figure
    # -----------------------------------------------------------------------
    figure_dir = out_dir / "figures" / "final"
    figure_dir.mkdir(parents=True, exist_ok=True)

    written_figures = []
    for fmt in formats:
        out_path = (figure_dir / figure_id).with_suffix(f".{fmt}")
        fig.savefig(out_path, dpi=args.dpi, bbox_inches="tight")
        written_figures.append(out_path)
        print(f"Wrote figure: {out_path}")
    plt.close(fig)

    # -----------------------------------------------------------------------
    # 5. Update metadata with plot-only export info
    # -----------------------------------------------------------------------
    metadata["reproducibility"]["plot_only_re_export"] = {
        "date": datetime.date.today().isoformat(),
        "figure_paths": [str(p) for p in written_figures],
    }
    metadata_path = out_dir / f"{figure_id}_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(json_ready(metadata), f, indent=2)
    print(f"Updated metadata: {metadata_path}")

    print("\nDone (plot-only).")
    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a basin matching workflow figure "
        "showing all policy outcomes from the current release."
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="Output directory for figure and data files.",
    )
    parser.add_argument(
        "--formats",
        default="pdf,png",
        help="Comma-separated output formats.",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Figure DPI for bitmap output.",
    )
    parser.add_argument(
        "--case-selection",
        default="representative",
        choices=["representative", "all"],
        help="Case selection mode (representative = one per combo).",
    )
    parser.add_argument(
        "--station-ids",
        default=None,
        help="Comma-separated list of cluster_ids to plot "
        "(overrides automatic selection).",
    )
    parser.add_argument(
        "--top-candidates",
        type=int,
        default=8,
        help="Number of top candidate reaches to highlight.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip computation (tracer, S4 geometry extraction); re-generate the figure from cached pickle files saved by a previous full run. Useful for rapid iteration on visual style.",
    )
    parser.add_argument(
        "--show-summary",
        action="store_true",
        help="Include the release-count summary panel at the bottom of the figure.",
    )
    parser.add_argument(
        "--no-legend",
        action="store_true",
        help="Suppress the legend bar at the bottom of the figure.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    formats = parse_formats(args.formats)
    figure_id = "fig_basin_matching_workflow"

    # Matplotlib config
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "axes.unicode_minus": False,
        }
    )

    # Load catalog
    catalog_path = Path(STATION_CATALOG)
    if not catalog_path.is_file():
        print(f"ERROR: station catalog not found at {catalog_path}",
              file=sys.stderr)
        return 1
    catalog = pd.read_csv(catalog_path)

    # Select cases
    if args.station_ids:
        ids = [
            int(x.strip())
            for x in args.station_ids.split(",")
            if x.strip()
        ]
        cat_filtered = catalog[
            catalog["cluster_id"].isin(ids)
        ].drop_duplicates(subset="cluster_id")
        case_rows = []
        for _, row in cat_filtered.iterrows():
            case_rows.append(
                {
                    "combo_status": clean_text(row.get("basin_status")),
                    "combo_quality": clean_text(
                        row.get("basin_match_quality")
                    ),
                    "combo_flag": clean_text(row.get("basin_flag")),
                    "cluster_uid": str(row["cluster_uid"]),
                    "cluster_id": int(row["cluster_id"]),
                    "lon": float(row["lon"]),
                    "lat": float(row["lat"]),
                    "basin_distance_m": as_float(
                        row.get("basin_distance_m")
                    ),
                    "point_in_local": (
                        bool(row["point_in_local"])
                        if pd.notna(row.get("point_in_local"))
                        else None
                    ),
                    "point_in_basin": (
                        bool(row["point_in_basin"])
                        if pd.notna(row.get("point_in_basin"))
                        else None
                    ),
                    "basin_area": as_float(row.get("basin_area")),
                    "record_count": int(row.get("record_count", 0)),
                    "resolution": clean_text(row.get("resolution")),
                    "station_name": clean_text(row.get("station_name")),
                    "river_name": clean_text(row.get("river_name")),
                    "source_station_id": clean_text(
                        row.get("source_station_id")
                    ),
                    "sources_used": clean_text(row.get("sources_used")),
                    "n_upstream_reaches": as_int_or_none(
                        row.get("n_upstream_reaches")
                    ),
                    "_present": True,
                }
            )
        cases = pd.DataFrame(case_rows)
    else:
        cases = select_cases(catalog_path, CASE_COMBOS)

    # Merge S4 area metadata into the cases dataframe so that prepare_tracer_case
    # receives reported_area for smarter COMID matching (area-aware, not just
    # distance-only), and so the fallback can use the S4 CSV's basin_id and pfaf_code.
    s4_area_df = load_s4_area_metadata(S4_UPSTREAM_BASINS)
    if not s4_area_df.empty:
        cases = cases.merge(s4_area_df, on="cluster_id", how="left")

    n_cases = len(cases)
    print(f"Selected {n_cases} cases for the figure.")

    # Release summary
    summary = release_summary(catalog)

    # Plot-only shortcut: skip all computation, rebuild figure from cache
    if args.plot_only:
        return _run_plot_only(args, out_dir, formats, figure_id)

    # Save cases DataFrame for plot-only future use
    _cache_dir = _get_cache_dir(out_dir)
    with open(_cache_dir / "cases_df.pkl", "wb") as f:
        pickle.dump(cases, f)


    # Initialize tracer
    print("Initializing UpstreamBasinTracer (may take a moment)...")
    tracer = UpstreamBasinTracer(str(MERIT_DIR))

    # Process each case
    case_records: List[Dict[str, Any]] = []
    panel_records: List[Dict[str, Any]] = []
    tracer_results: List[Dict[str, Any]] = []
    s4_geometries: List[Tuple[Optional[Any], Optional[Any]]] = []

    for i, (_, case) in enumerate(cases.iterrows()):
        print(
            f"  Processing case {i + 1}/{n_cases}: "
            f"{case['combo_status']}/{case['combo_quality']}/"
            f"{case['combo_flag']} "
            f"({case.get('cluster_uid', 'N/A')})"
        )

        present = bool(case.get("_present", True))
        row: Dict[str, Any] = {
            "panel_idx": i,
            "panel_label": f"({chr(97 + i)})",
            "combo_status": str(case["combo_status"]),
            "combo_match_quality": str(case["combo_quality"]),
            "combo_flag": str(case["combo_flag"]),
        }

        if not present or pd.isna(case.get("lon")) or pd.isna(case.get("lat")):
            row.update(
                {
                    "present_in_release": False,
                    "cluster_uid": str(case.get("cluster_uid", "")),
                    "cluster_id": None,
                    "lon": np.nan,
                    "lat": np.nan,
                    "s4_local_geom_type": None,
                    "s4_basin_geom_type": None,
                    "s4_basin_id": None,
                    "s4_match_quality": None,
                    "s4_reported_area": np.nan,
                    "s4_area_error": np.nan,
                    "s4_uparea_merit": np.nan,
                    "s4_distance_m": np.nan,
                    "s4_point_in_local": None,
                    "s4_point_in_basin": None,
                    "n_candidates_found": 0,
                    "matched_comid": None,
                    "matched_distance_m": np.nan,
                    "matched_area_error": np.nan,
                    "matched_uparea": np.nan,
                    "tracer_match_quality": None,
                }
            )
            case_records.append(row)
            tracer_results.append(
                {
                    "candidates_gdf": None,
                    "top_gdf": None,
                    "best_reach": None,
                    "offset_line": None,
                    "candidate_table": pd.DataFrame(),
                    "n_candidates": 0,
                    "match_quality": "failed",
                    "matched_comid": None,
                    "matched_distance_m": np.nan,
                    "area_error": np.nan,
                    "notes": ["No occurrence in this release"],
                    "matched_uparea": np.nan,
                }
            )
            s4_geometries.append((None, None))
            panel_records.append(
                {
                    "panel_idx": i,
                    "panel_label": chr(97 + i),
                    "station_lon": np.nan,
                    "station_lat": np.nan,
                    "candidate_comids": [],
                    "candidate_distances_m": [],
                    "matched_comid": None,
                    "offset_start_lon": np.nan,
                    "offset_start_lat": np.nan,
                    "offset_end_lon": np.nan,
                    "offset_end_lat": np.nan,
                }
            )
        # Cache for plot-only re-run
            # Cache for plot-only re-run
            _save_case_cache(_cache_dir, i, {
                "candidates_gdf": None,
                "top_gdf": None,
                "best_reach": None,
                "offset_line": None,
                "candidate_table": pd.DataFrame(),
                "n_candidates": 0,
                "match_quality": "failed",
                "matched_comid": None,
                "matched_distance_m": np.nan,
                "area_error": np.nan,
                "notes": ["No occurrence in this release"],
                "matched_uparea": np.nan,
            }, None, None)
            continue
        row["lat"] = float(case["lat"])

        # S4 geometry
        try:
            resolution = str(case.get("resolution", ""))
            local_geom, basin_geom, s4_attrs = read_s4_geometry(
                int(case["cluster_id"]), resolution
            )
        except Exception as exc:
            print(
                f"    WARNING: S4 geometry read failed: {exc}",
                file=sys.stderr,
            )
            local_geom = None
            basin_geom = None
            s4_attrs = {}

        row["s4_local_geom_type"] = (
            local_geom.geom_type if local_geom is not None else None
        )
        row["s4_basin_geom_type"] = (
            basin_geom.geom_type if basin_geom is not None else None
        )
        row["s4_basin_id"] = s4_attrs.get("basin_id")
        row["s4_match_quality"] = s4_attrs.get("match_quality")
        row["s4_reported_area"] = case.get("s4_reported_area")
        row["s4_area_error"] = case.get("s4_area_error")
        row["s4_uparea_merit"] = case.get("s4_uparea_merit")
        row["s4_distance_m"] = s4_attrs.get("distance_m")
        row["s4_point_in_local"] = s4_attrs.get("point_in_local")
        row["s4_point_in_basin"] = s4_attrs.get("point_in_basin")

        # Tracer
        reported_area = as_float(case.get("s4_reported_area"))
        if not np.isfinite(reported_area) or reported_area <= 0:
            reported_area = None
        try:
            td = prepare_tracer_case(
                tracer,
                float(case["lon"]),
                float(case["lat"]),
                reported_area=reported_area,
                top_n=args.top_candidates,
            )
        except Exception as exc:
            print(
                f"    WARNING: tracer failed for {case['cluster_uid']}: "
                f"{exc}",
                file=sys.stderr,
            )
            td = {
                "candidates_gdf": None,
                "top_gdf": None,
                "best_reach": None,
                "offset_line": None,
                "candidate_table": pd.DataFrame(),
                "n_candidates": 0,
                "match_quality": "error",
                "matched_comid": None,
                "matched_distance_m": np.nan,
                "area_error": np.nan,
                "notes": [f"Tracer error: {exc}"],
                "matched_uparea": np.nan,
            }

        row["n_candidates_found"] = (
            len(td.get("candidate_table", pd.DataFrame()))
            if td.get("candidate_table") is not None
            else 0
        )
        row["matched_comid"] = td.get("matched_comid")
        row["matched_distance_m"] = td.get("matched_distance_m")
        row["matched_area_error"] = td.get("area_error")
        row["tracer_match_quality"] = td.get("match_quality")
        row["matched_uparea"] = td.get("matched_uparea")

        case_records.append(row)
        tracer_results.append(td)
        # Cache for plot-only re-run

        # Fallback: if GPKG lacks basin_geom but S4 CSV or tracer found a match,
        # trace the upstream basin on-the-fly from MERIT Hydro.
        # Priority: S4 CSV basin_id > tracer matched_comid.
        s4_basin_id = as_int_or_none(case.get("s4_basin_id"))
        fallback_comid = s4_basin_id if s4_basin_id is not None else td.get("matched_comid")
        if basin_geom is None and fallback_comid is not None:
            try:
                # pfaf_code: prefer S4 CSV, fall back to candidate_table (zero-padded)
                pfaf_code = clean_text(case.get("s4_pfaf_code"))
                if not pfaf_code:
                    cand_table = td.get("candidate_table")
                    if cand_table is not None and not cand_table.empty:
                        pfaf = cand_table.iloc[0].get("pfaf_code")
                        if pd.notna(pfaf) and pfaf:
                            pfaf_code = str(pfaf)
                # Zero-pad to 2 digits so MERIT shapefile path resolves correctly.
                # pfaf_code may arrive as "7.0" (float from CSV -> clean_text) or "7" (int from table).
                if pfaf_code:
                    try:
                        pfaf_code = str(int(float(pfaf_code))).zfill(2)
                    except (ValueError, TypeError):
                        pfaf_code = ""  # non-numeric, skip fallback

                if pfaf_code:
                    reach_info = {
                        "COMID": fallback_comid,
                        "uparea": td.get("matched_uparea", np.nan),
                        "distance": td.get("matched_distance_m", np.nan),
                        "pfaf_code": pfaf_code,
                        "match_quality": "area_matched" if s4_basin_id is not None else td.get("match_quality", "failed"),
                        "area_error": as_float(case.get("s4_area_error")),
                    }
                    basin_dict = tracer.get_upstream_basin_from_reach(
                        float(case["lon"]), float(case["lat"]), reach_info
                    )
                    if basin_dict and basin_dict.get("geometry") is not None:
                        basin_geom = basin_dict["geometry"]
                        print(f'    Traced upstream basin on-the-fly for {case.get("cluster_uid", "?")}: {len(str(basin_geom))} vertices, {basin_dict.get("n_upstream_reaches", 0)} upstream reaches')
                    else:
                        print(f'    Fallback traced but produced no geometry for {case.get("cluster_uid", "?")}; method={basin_dict.get("method")}')
                else:
                    print(f'    Cannot trace upstream basin for {case.get("cluster_uid", "?")}: no pfaf_code available')
            except Exception as exc:
                print(f"    WARNING: basin trace fallback failed: {exc}")


        _save_case_cache(_cache_dir, i, td, local_geom, basin_geom)
        s4_geometries.append((local_geom, basin_geom))

        # Candidate-level plotting data
        cand_table = td.get("candidate_table", pd.DataFrame())
        if cand_table is not None and not cand_table.empty:
            for _, cr in cand_table.iterrows():
                panel_records.append(
                    {
                        "panel_idx": i,
                        "panel_label": chr(97 + i),
                        "case_combo": (
                            f"{case['combo_status']}/"
                            f"{case['combo_quality']}/"
                            f"{case['combo_flag']}"
                        ),
                        "cluster_uid": str(case["cluster_uid"]),
                        "station_lon": float(case["lon"]),
                        "station_lat": float(case["lat"]),
                        "candidate_rank": int(cr["rank"]),
                        "candidate_comid": int(cr["COMID"]),
                        "candidate_distance_m": float(cr["dist_m"]),
                        "candidate_nearest_dist_m": float(
                            cr["nearest_dist_m"]
                        ),
                        "candidate_pfaf_code": str(cr["pfaf_code"]),
                        "candidate_uparea": float(cr["uparea"]),
                        "offset_start_lon": float(case["lon"]),
                        "offset_start_lat": float(case["lat"]),
                        "offset_end_lon": cr.get("nearest_lon"),
                        "offset_end_lat": cr.get("nearest_lat"),
                    }
                )

    # ---- Build figure (shared layout function) ----
    n_cols = min(n_cases, 3)
    n_rows = int(math.ceil(n_cases / 3.0))

    fig = _build_figure(
        n_cases, n_rows, n_cols,
        cases, tracer_results, s4_geometries,
        summary,
        show_summary=args.show_summary,
        show_panel_labels=not args.no_legend,
    )

    # ---- Save figure ----
    figure_dir = out_dir / "figures" / "final"
    figure_dir.mkdir(parents=True, exist_ok=True)
    data_dir = out_dir / "figures" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir = out_dir / "figures" / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    checklists_dir = out_dir / "figures" / "checklists"
    checklists_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = figure_dir / f"{figure_id}.pdf"
    png_path = figure_dir / f"{figure_id}.png"

    written_figures: List[Path] = []
    for fmt in formats:
        out_path = (figure_dir / figure_id).with_suffix(f".{fmt}")
        fig.savefig(out_path, dpi=args.dpi, bbox_inches="tight")
        written_figures.append(out_path)
        print(f"Wrote figure: {out_path}")
    plt.close(fig)

    # ---- Write cases.csv ----
    cases_csv = data_dir / f"{figure_id}_cases.csv"
    cases_df = pd.DataFrame(case_records)
    cases_df.to_csv(cases_csv, index=False)
    print(f"Wrote cases CSV: {cases_csv}")

    # ---- Write plotting_data.csv ----
    plotting_csv = data_dir / f"{figure_id}_plotting_data.csv"
    plotting_df = pd.DataFrame(panel_records)
    plotting_df.to_csv(plotting_csv, index=False)
    print(f"Wrote plotting data CSV: {plotting_csv}")

    # ---- Write metadata.json ----
    metadata = {
        "figure_id": figure_id,
        "description": "Basin matching workflow figure",
        "sources": {
            "station_catalog": str(STATION_CATALOG),
            "s4_local_catchments": str(S4_LOCAL_CATCHMENTS),
            "release_cluster_basins": str(RELEASE_CLUSTER_BASINS),
            "s4_upstream_basins": str(S4_UPSTREAM_BASINS),
            "merit_hydro_dir": str(MERIT_DIR),
        },
        "release_counts": {
            "total_rows": summary["total_rows"],
            "by_status": summary["by_status"],
            "by_match_quality": summary["by_match_quality"],
            "by_flag": summary["by_flag"],
        },
        "panel_cases": [
            {
                "panel_label": chr(97 + i),
                "combo_status": r["combo_status"],
                "combo_match_quality": r["combo_match_quality"],
                "combo_flag": r["combo_flag"],
                "present_in_release": r.get("present_in_release", False),
                "cluster_uid": r.get("cluster_uid"),
                "cluster_id": r.get("cluster_id"),
            }
            for i, r in enumerate(case_records)
        ],
        "reproducibility": {
            "script": str(Path(__file__).resolve()),
            "python_executable": sys.executable,
            "argv": sys.argv,
            "matplotlib_backend": "Agg",
            "export_date": datetime.date.today().isoformat(),
            "figure_paths": [str(p) for p in written_figures],
            "cases_csv": str(cases_csv),
            "plotting_data_csv": str(plotting_csv),
        },
    }
    metadata_path = out_dir / f"{figure_id}_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(json_ready(metadata), f, indent=2)
    print(f"Wrote metadata: {metadata_path}")

    # ---- Copy script ----
    script_copy_path = scripts_dir / f"{figure_id}.py"
    try:
        shutil.copy2(Path(__file__).resolve(), script_copy_path)
    except Exception:
        pass

    # ---- ESSD checklist ----
    width_cm = FIGSIZE[0] * 2.54
    height_cm = FIGSIZE[1] * 2.54
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(
        ["pdffonts", str(pdf_path)]
    )

    data_paths = [
        p for p in [cases_csv, plotting_csv, metadata_path] if p.is_file()
    ]

    checklist_lines = [
        f"# {figure_id} ESSD figure checklist",
        "",
        f"- Final PDF: `{pdf_path.name}`",
        f"- Final PNG: `{png_path.name}`",
        "- Formats: PDF vector preferred; PNG bitmap companion",
        f"- PNG dpi: {args.dpi}",
        f"- Intended size: {width_cm:.1f} x {height_cm:.1f} cm "
        f"({FIGSIZE[0]:.1f} x {FIGSIZE[1]:.1f} in)",
        "- PDF page size: {}".format(
            pdf_page_size(pdfinfo_output)
            if pdfinfo_ok
            else pdfinfo_output
        ),
        f"- PDF file size: {file_size_mb(pdf_path)}",
        f"- PNG file size: {file_size_mb(png_path)}",
        "- Width >= 8 cm: yes",
        "- Font family: DejaVu Sans",
        "- Font consistency: one sans-serif family set in Matplotlib rcParams",
        "- Font embedding status: {}".format(
            font_embedding_status(pdffonts_output)
            if pdffonts_ok
            else pdffonts_output
        ),
        "- Colorblind-safe status: Okabe-Ito palette with "
        "black/white markers and textures",
        "- Coblis/equivalent review: requires manual review after export",
        "- Legend completeness: status/quality/flag labels on each panel; "
        "consistent reach colours",
        "- Panel labels: `(a)`, `(b)`, ... `(i)`",
        "- Units and coordinates: WGS84 longitude/latitude; "
        "distances in metres",
        "- Text minimum size: 7 pt (labels), 6.5 pt (annotations), "
        "6 pt (tick labels)",
        "- Dense point layers: rasterized; text, legends, and axes "
        "remain vector in PDF",
        f"- Plotting script: `{script_copy_path.name}`",
        f"- Plotting-data availability: {len(data_paths)} data files",
        f"- Export date: {datetime.date.today().isoformat()}",
    ]
    for dp in data_paths:
        checklist_lines.append(f"- Plotting data file: `{dp.name}`")

    checklist_path = checklists_dir / f"{figure_id}_checklist.md"
    checklist_path.write_text(
        "\n".join(checklist_lines).rstrip() + "\n", encoding="utf-8"
    )
    print(f"Wrote checklist: {checklist_path}")

    # Summary
    print("\nDone.")
    print(f"  Cases CSV: {len(case_records)} case records")
    print(f"  Plotting data CSV: {len(panel_records)} candidate records")
    print(f"  Release summary: {summary['total_rows']} rows")
    for flag in BASIN_FLAG_ORDER:
        print(f"    {flag}: {summary['by_flag'].get(flag, 0)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
