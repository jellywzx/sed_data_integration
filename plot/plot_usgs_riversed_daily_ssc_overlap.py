#!/usr/bin/env python3
"""Plot two USGS vs RiverSed daily SSC overlap cases with MERIT river reaches.

Run from the sed_data_integration / Output_r script directory:

    python plot_usgs_riversed_daily_ssc_overlap_merit.py

Inputs expected from s5_early_validation_results.py:

    scripts_basin_test/output/early_validation_results/early_overlap_pair_records.csv
    scripts_basin_test/output/early_validation_results/early_overlap_candidates.csv.gz

MERIT input:

    MERIT_DIR environment variable, or default:
    Output_r/../../MERIT_Hydro_v07_Basins_v01_bugfix1

Outputs:

    scripts_basin_test/output/early_validation_results/plots_merit/
        usgs_riversed_daily_ssc_merit_case_01.png
        usgs_riversed_daily_ssc_merit_case_02.png
        usgs_riversed_daily_ssc_merit_selected_records.csv
        usgs_riversed_daily_ssc_merit_plot_notes.txt

What each plot shows:
  - USGS and RiverSed station points
  - SSC value at the overlap date for both sources
  - nearby MERIT river reaches
  - the nearest MERIT reach selected for each point, with COMID and distance
  - optional local/upstream basin geometries if already exported by s4/s7
"""

from __future__ import annotations

import math
import os
import sys
import warnings
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
    import fiona
except ImportError:
    fiona = None

try:
    from shapely.geometry import box
except ImportError:
    box = None

try:
    from pipeline_paths import (
        S4_LOCAL_GPKG,
        S4_UPSTREAM_GPKG,
        S7_CLUSTER_BASINS_GPKG,
        S7_SOURCE_STATIONS_GPKG,
        get_output_r_root,
    )
except Exception:
    S4_LOCAL_GPKG = "scripts_basin_test/output/s4_local_catchments.gpkg"
    S4_UPSTREAM_GPKG = "scripts_basin_test/output/s4_upstream_basins.gpkg"
    S7_CLUSTER_BASINS_GPKG = "scripts_basin_test/output/s7_cluster_basins.gpkg"
    S7_SOURCE_STATIONS_GPKG = "scripts_basin_test/output/s7_source_stations.gpkg"

    def get_output_r_root(script_dir: Path) -> Path:
        return script_dir.parent.resolve()


# ---------------------------------------------------------------------------
# Built-in config
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Built-in config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent


def find_output_r_root(script_dir: Path) -> Path:
    """Find the real Output_r root even if this script is inside subfolders."""
    candidates = [script_dir.resolve()] + list(script_dir.resolve().parents)

    # Best case: find existing early validation output.
    for parent in candidates:
        early_dir = parent / "scripts_basin_test" / "output" / "early_validation_results"
        if early_dir.exists():
            return parent

    # Second best: find scripts_basin_test/output.
    for parent in candidates:
        output_dir = parent / "scripts_basin_test" / "output"
        if output_dir.exists():
            return parent

    # If current path is Output_r/scripts_basin_test/plot, walk up to Output_r.
    for parent in candidates:
        if parent.name == "Output_r":
            return parent

    # Last fallback.
    return get_output_r_root(script_dir)


PROJECT_ROOT = find_output_r_root(SCRIPT_DIR)

EARLY_DIR = PROJECT_ROOT / "scripts_basin_test" / "output" / "early_validation_results"
PAIR_RECORDS_CSV = EARLY_DIR / "early_overlap_pair_records.csv"
CANDIDATES_CSV_GZ = EARLY_DIR / "early_overlap_candidates.csv.gz"
OUT_DIR = EARLY_DIR / "plots_merit"

MERIT_DIR = Path(
    os.environ.get(
        "MERIT_DIR",
        str(PROJECT_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"),
    )
)

TARGET_SOURCE_PAIR = "USGS vs RiverSed"
TARGET_RESOLUTION = "daily"
TARGET_VARIABLE = "SSC"
N_CASES_TO_PLOT = 2

# Strategy:
#   "largest_pct_error": pick the largest pct_error cases, preferring different clusters.
#   "largest_abs_diff": pick the largest absolute differences, preferring different clusters.
CASE_SELECTION = "largest_pct_error"

# Plot extent around the two station points.
BBOX_PADDING_DEGREES = 0.25

# Nearby MERIT reaches can be numerous. Keep the closest N in each case.
MAX_NEARBY_MERIT_REACHES = 250

# Optional precomputed geometries from s4/s7. These are plotted as context when
# available, but MERIT river reaches are loaded directly from MERIT_DIR.
OPTIONAL_CONTEXT_GPKGS = [
    PROJECT_ROOT / S7_CLUSTER_BASINS_GPKG,
    PROJECT_ROOT / S4_UPSTREAM_GPKG,
    PROJECT_ROOT / S4_LOCAL_GPKG,
    PROJECT_ROOT / S7_SOURCE_STATIONS_GPKG,
]


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

    selected_rows = []
    used_clusters = set()
    for _, row in work.iterrows():
        cluster_key = clean_text(row.get("cluster_id", ""))
        if cluster_key in used_clusters:
            continue
        selected_rows.append(row)
        used_clusters.add(cluster_key)
        if len(selected_rows) >= N_CASES_TO_PLOT:
            break

    if len(selected_rows) < N_CASES_TO_PLOT:
        used_index = {int(row.name) for row in selected_rows}
        for idx, row in work.iterrows():
            if int(idx) in used_index:
                continue
            selected_rows.append(row)
            if len(selected_rows) >= N_CASES_TO_PLOT:
                break

    return pd.DataFrame(selected_rows).reset_index(drop=True)


def find_candidate_point(
    candidates: pd.DataFrame,
    pair_row: pd.Series,
    side: str,
) -> Dict[str, object]:
    source_col = "source_{}".format(side)
    uid_col = "source_station_uid_{}".format(side)
    value_col = "value_{}".format(side)

    cluster_id = safe_float(pair_row.get("cluster_id"))
    date = normalize_date(pair_row.get("date"))
    source = clean_text(pair_row.get(source_col))
    uid = clean_text(pair_row.get(uid_col))

    cand = candidates.copy()
    if "cluster_id" in cand.columns:
        cand["_cluster_id_num"] = pd.to_numeric(cand["cluster_id"], errors="coerce")
    else:
        cand["_cluster_id_num"] = np.nan

    mask = (
        cand["_cluster_id_num"].eq(cluster_id)
        & cand["resolution"].astype(str).eq(clean_text(pair_row.get("resolution")))
        & cand["date"].astype(str).eq(date)
    )

    if uid and "source_station_uid" in cand.columns:
        mask_uid = mask & cand["source_station_uid"].astype(str).eq(uid)
        if mask_uid.any():
            rec = cand[mask_uid].iloc[0].to_dict()
            return build_point_record(pair_row, rec, side)

    if source and "source" in cand.columns:
        mask_source = mask & cand["source"].astype(str).eq(source)
        if mask_source.any():
            rec = cand[mask_source].iloc[0].to_dict()
            return build_point_record(pair_row, rec, side)

    expected_value = safe_float(pair_row.get(value_col))
    if mask.any() and TARGET_VARIABLE in cand.columns and np.isfinite(expected_value):
        sub = cand[mask].copy()
        sub["_value_diff"] = (
            pd.to_numeric(sub[TARGET_VARIABLE], errors="coerce") - expected_value
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
        "note": "candidate point not found in early_overlap_candidates.csv.gz",
    }


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
        "note": "",
    }


def build_bbox(points: Sequence[Dict[str, object]], padding: float = BBOX_PADDING_DEGREES) -> Optional[Tuple[float, float, float, float]]:
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


def init_merit_tracer() -> Tuple[object, List[str]]:
    notes = []
    if gpd is None:
        notes.append("geopandas is not installed; MERIT river reaches cannot be plotted")
        return None, notes

    if not MERIT_DIR.is_dir():
        notes.append("MERIT_DIR not found: {}".format(MERIT_DIR))
        return None, notes

    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    try:
        from basin_tracer import UpstreamBasinTracer
    except Exception as exc:
        notes.append("could not import UpstreamBasinTracer from basin_tracer.py: {}".format(exc))
        return None, notes

    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*geographic CRS.*")
            tracer = UpstreamBasinTracer(str(MERIT_DIR))
        return tracer, notes
    except Exception as exc:
        notes.append("could not initialize MERIT tracer from {}: {}".format(MERIT_DIR, exc))
        return None, notes


def crop_gdf_to_bbox(gdf, bbox: Optional[Tuple[float, float, float, float]]):
    if gdf is None or gdf.empty or bbox is None:
        return gdf
    minx, miny, maxx, maxy = bbox
    try:
        return gdf.cx[minx:maxx, miny:maxy].copy()
    except Exception:
        try:
            bounds = gdf.bounds
            mask = (
                bounds["maxx"].ge(minx)
                & bounds["minx"].le(maxx)
                & bounds["maxy"].ge(miny)
                & bounds["miny"].le(maxy)
            )
            return gdf[mask].copy()
        except Exception:
            return gdf


def find_merit_reach_for_point(tracer, point: Dict[str, object]) -> Tuple[Optional[object], Dict[str, object], List[str]]:
    notes = []
    info = {
        "COMID": np.nan,
        "pfaf_code": "",
        "distance_m": np.nan,
        "uparea": np.nan,
        "match_quality": "",
    }

    if tracer is None:
        return None, info, notes

    lon = safe_float(point.get("lon"))
    lat = safe_float(point.get("lat"))
    if not np.isfinite(lon) or not np.isfinite(lat):
        notes.append("missing lon/lat for MERIT matching: {}".format(point.get("source", "")))
        return None, info, notes

    try:
        best = tracer.find_best_reach(lon, lat, reported_area=None)
        if not best or best.get("COMID") is None or not best.get("pfaf_code"):
            notes.append("no MERIT reach found for {}".format(point.get("source", "")))
            return None, info, notes

        comid = int(best["COMID"])
        pfaf_code = str(best["pfaf_code"])
        info = {
            "COMID": comid,
            "pfaf_code": pfaf_code,
            "distance_m": safe_float(best.get("distance")),
            "uparea": safe_float(best.get("uparea")),
            "match_quality": clean_text(best.get("match_quality")),
        }

        riv_gdf = tracer._load_level1_rivers(pfaf_code)
        if riv_gdf is None or riv_gdf.empty:
            notes.append("could not load MERIT river file for pfaf_code={}".format(pfaf_code))
            return None, info, notes

        if comid not in set(riv_gdf.index):
            notes.append("COMID={} not found in MERIT river index for pfaf_code={}".format(comid, pfaf_code))
            return None, info, notes

        reach = riv_gdf.loc[[comid]].copy()
        if reach.crs is None:
            reach = reach.set_crs("EPSG:4326")
        else:
            reach = reach.to_crs("EPSG:4326")
        reach["_plot_source"] = point.get("source", "")
        return reach, info, notes
    except Exception as exc:
        notes.append("MERIT matching failed for {}: {}".format(point.get("source", ""), exc))
        return None, info, notes


def load_nearby_merit_reaches(tracer, points: Sequence[Dict[str, object]], bbox: Optional[Tuple[float, float, float, float]]):
    notes = []
    if tracer is None or gpd is None:
        return None, notes

    frames = []
    for point in points:
        lon = safe_float(point.get("lon"))
        lat = safe_float(point.get("lat"))
        if not np.isfinite(lon) or not np.isfinite(lat):
            continue
        try:
            nearby = tracer.get_nearby_candidate_reaches(lon, lat)
            if nearby is None or nearby.empty:
                notes.append("no nearby MERIT candidate reaches for {}".format(point.get("source", "")))
                continue
            if nearby.crs is None:
                nearby = nearby.set_crs("EPSG:4326")
            else:
                nearby = nearby.to_crs("EPSG:4326")
            frames.append(nearby)
        except Exception as exc:
            notes.append("could not load nearby MERIT reaches for {}: {}".format(point.get("source", ""), exc))

    if not frames:
        return None, notes

    merged = pd.concat(frames, ignore_index=True)
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs="EPSG:4326")
    if "COMID" in merged.columns and "pfaf_code" in merged.columns:
        merged = merged.drop_duplicates(subset=["pfaf_code", "COMID"], keep="first")

    merged = crop_gdf_to_bbox(merged, bbox)

    if "dist_m" in merged.columns:
        merged["_dist_sort"] = pd.to_numeric(merged["dist_m"], errors="coerce")
        merged = merged.sort_values("_dist_sort", kind="mergesort")

    if len(merged) > MAX_NEARBY_MERIT_REACHES:
        merged = merged.head(MAX_NEARBY_MERIT_REACHES).copy()
        notes.append("nearby MERIT reaches truncated to {}".format(MAX_NEARBY_MERIT_REACHES))

    return merged, notes


def get_gpkg_layers(path: Path) -> List[Optional[str]]:
    if fiona is None:
        return [None]
    try:
        return list(fiona.listlayers(str(path)))
    except Exception:
        return [None]


def load_optional_context_geometries(cluster_id, bbox: Optional[Tuple[float, float, float, float]]):
    frames = []
    notes = []

    if gpd is None:
        notes.append("geopandas is not installed; optional GPKG context was not plotted")
        return frames, notes

    for path in OPTIONAL_CONTEXT_GPKGS:
        if not Path(path).is_file():
            continue

        layers = get_gpkg_layers(Path(path))
        for layer in layers:
            label = "{}{}".format(Path(path).name, "" if layer is None else "::{}".format(layer))
            try:
                read_kwargs = {}
                if layer is not None:
                    read_kwargs["layer"] = layer
                if bbox is not None:
                    read_kwargs["bbox"] = bbox

                gdf = gpd.read_file(path, **read_kwargs)
                if gdf.empty:
                    continue
                if gdf.crs is None:
                    gdf = gdf.set_crs("EPSG:4326")
                else:
                    gdf = gdf.to_crs("EPSG:4326")

                # Prefer the matching cluster_id where the layer has one.
                for col in ["cluster_id", "station_id", "basin_id"]:
                    if col in gdf.columns:
                        numeric = pd.to_numeric(gdf[col], errors="coerce")
                        matched = gdf[numeric.eq(safe_float(cluster_id))].copy()
                        if not matched.empty:
                            gdf = matched
                            break

                gdf = crop_gdf_to_bbox(gdf, bbox)
                if gdf is None or gdf.empty:
                    continue

                if len(gdf) > 300:
                    gdf = gdf.head(300).copy()
                    notes.append("optional context truncated to first 300 features: {}".format(label))

                frames.append((label, gdf))
            except Exception as exc:
                notes.append("could not read optional context {}: {}".format(label, exc))

    return frames, notes


def plot_case(
    case_number: int,
    pair_row: pd.Series,
    candidates: pd.DataFrame,
    tracer,
) -> Tuple[Path, Dict[str, object], List[str]]:
    point_a = find_candidate_point(candidates, pair_row, "a")
    point_b = find_candidate_point(candidates, pair_row, "b")
    points = [point_a, point_b]

    bbox = build_bbox(points)

    nearby_merit, merit_notes = load_nearby_merit_reaches(tracer, points, bbox)

    reach_a, merit_a, notes_a = find_merit_reach_for_point(tracer, point_a)
    reach_b, merit_b, notes_b = find_merit_reach_for_point(tracer, point_b)

    context_frames, context_notes = load_optional_context_geometries(pair_row.get("cluster_id"), bbox)

    fig, ax = plt.subplots(figsize=(11, 8.5))

    if nearby_merit is not None and not nearby_merit.empty:
        try:
            nearby_merit.plot(ax=ax, linewidth=0.7, alpha=0.35, label="nearby MERIT reaches")
        except Exception as exc:
            merit_notes.append("could not plot nearby MERIT reaches: {}".format(exc))

    for label, gdf in context_frames:
        try:
            geom_types = set(gdf.geometry.geom_type.dropna().astype(str))
            if any(gt in geom_types for gt in ["Polygon", "MultiPolygon"]):
                gdf.boundary.plot(ax=ax, linewidth=1.0, alpha=0.45, label=label)
            else:
                gdf.plot(ax=ax, linewidth=1.0, alpha=0.45, markersize=12, label=label)
        except Exception as exc:
            context_notes.append("could not plot optional context {}: {}".format(label, exc))

    if reach_a is not None and not reach_a.empty:
        try:
            reach_a.plot(ax=ax, linewidth=3.0, alpha=0.9, label="USGS matched MERIT reach")
        except Exception as exc:
            merit_notes.append("could not plot USGS matched MERIT reach: {}".format(exc))

    if reach_b is not None and not reach_b.empty:
        try:
            reach_b.plot(ax=ax, linewidth=3.0, alpha=0.9, linestyle="--", label="RiverSed matched MERIT reach")
        except Exception as exc:
            merit_notes.append("could not plot RiverSed matched MERIT reach: {}".format(exc))

    for point, marker in zip(points, ["o", "^"]):
        lon = safe_float(point["lon"])
        lat = safe_float(point["lat"])
        value = safe_float(point["value"])
        source = point["source"]

        if np.isfinite(lon) and np.isfinite(lat):
            ax.scatter([lon], [lat], s=100, marker=marker, label="{} station".format(source))
            label = "{}\nSSC={}\nlat={}, lon={}".format(
                source,
                fmt_num(value, 5),
                fmt_num(lat, 6),
                fmt_num(lon, 6),
            )
            ax.annotate(
                label,
                xy=(lon, lat),
                xytext=(8, 8),
                textcoords="offset points",
                fontsize=9,
            )
        else:
            merit_notes.append("missing coordinates for {} / uid={}".format(source, point.get("uid", "")))

    lon_values = [safe_float(p["lon"]) for p in points]
    lat_values = [safe_float(p["lat"]) for p in points]
    if all(np.isfinite(x) for x in lon_values + lat_values):
        ax.plot(lon_values, lat_values, linestyle=":", linewidth=1.2, label="station-to-station link")

    if bbox is not None:
        minx, miny, maxx, maxy = bbox
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

    cluster_id = clean_text(pair_row.get("cluster_id"))
    date = normalize_date(pair_row.get("date"))
    pct_error = fmt_num(pair_row.get("pct_error"), 5)
    diff = fmt_num(pair_row.get("diff_b_minus_a"), 5)
    value_a = fmt_num(pair_row.get("value_a"), 5)
    value_b = fmt_num(pair_row.get("value_b"), 5)

    title = (
        "USGS vs RiverSed daily SSC with MERIT reaches | case {}\n"
        "cluster_id={}, date={}, USGS={}, RiverSed={}, RiverSed-USGS={}, pct_error={}%" 
    ).format(case_number, cluster_id, date, value_a, value_b, diff, pct_error)
    ax.set_title(title)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linewidth=0.5, alpha=0.4)

    # Keep legend readable even with many layers.
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

    merit_text = (
        "MERIT match\n"
        "USGS: COMID={a_comid}, dist={a_dist} m, uparea={a_uparea} km2\n"
        "RiverSed: COMID={b_comid}, dist={b_dist} m, uparea={b_uparea} km2"
    ).format(
        a_comid=clean_text(merit_a.get("COMID")),
        a_dist=fmt_num(merit_a.get("distance_m"), 5),
        a_uparea=fmt_num(merit_a.get("uparea"), 5),
        b_comid=clean_text(merit_b.get("COMID")),
        b_dist=fmt_num(merit_b.get("distance_m"), 5),
        b_uparea=fmt_num(merit_b.get("uparea"), 5),
    )
    ax.text(
        0.02,
        0.02,
        merit_text,
        transform=ax.transAxes,
        fontsize=9,
        va="bottom",
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "usgs_riversed_daily_ssc_merit_case_{:02d}.png".format(case_number)
    fig.tight_layout()
    fig.savefig(out_path, dpi=220)
    plt.close(fig)

    selected_info = {
        "case_number": case_number,
        "cluster_id": pair_row.get("cluster_id"),
        "date": date,
        "resolution": pair_row.get("resolution"),
        "variable": pair_row.get("variable"),
        "source_pair": pair_row.get("source_pair"),
        "source_a": pair_row.get("source_a"),
        "source_b": pair_row.get("source_b"),
        "source_station_uid_a": pair_row.get("source_station_uid_a"),
        "source_station_uid_b": pair_row.get("source_station_uid_b"),
        "value_a": pair_row.get("value_a"),
        "value_b": pair_row.get("value_b"),
        "diff_b_minus_a": pair_row.get("diff_b_minus_a"),
        "abs_diff": pair_row.get("abs_diff"),
        "pct_error": pair_row.get("pct_error"),
        "lat_a": point_a.get("lat"),
        "lon_a": point_a.get("lon"),
        "lat_b": point_b.get("lat"),
        "lon_b": point_b.get("lon"),
        "candidate_path_a": point_a.get("candidate_path"),
        "candidate_path_b": point_b.get("candidate_path"),
        "merit_comid_a": merit_a.get("COMID"),
        "merit_pfaf_code_a": merit_a.get("pfaf_code"),
        "merit_distance_m_a": merit_a.get("distance_m"),
        "merit_uparea_km2_a": merit_a.get("uparea"),
        "merit_match_quality_a": merit_a.get("match_quality"),
        "merit_comid_b": merit_b.get("COMID"),
        "merit_pfaf_code_b": merit_b.get("pfaf_code"),
        "merit_distance_m_b": merit_b.get("distance_m"),
        "merit_uparea_km2_b": merit_b.get("uparea"),
        "merit_match_quality_b": merit_b.get("match_quality"),
        "plot_path": str(out_path),
    }

    notes = []
    notes.extend(merit_notes)
    notes.extend(notes_a)
    notes.extend(notes_b)
    notes.extend(context_notes)

    return out_path, selected_info, notes


def main() -> int:
    print("Reading inputs from: {}".format(EARLY_DIR))
    print("Reading inputs from: {}".format(EARLY_DIR))
    pairs, candidates = read_inputs()

    target = filter_target_pairs(pairs)
    print(
        "Found {:,} rows for {} / {} / {}".format(
            len(target),
            TARGET_SOURCE_PAIR,
            TARGET_RESOLUTION,
            TARGET_VARIABLE,
        )
    )

    selected = select_cases(target)
    if selected.empty:
        raise RuntimeError("No selected cases to plot")

    print("Selected {} case(s) using strategy: {}".format(len(selected), CASE_SELECTION))
    print("MERIT_DIR: {}".format(MERIT_DIR))

    tracer, tracer_notes = init_merit_tracer()
    if tracer is None:
        print("Warning: MERIT tracer is unavailable; point plots will still be created without MERIT reaches.")

    selected_records = []
    all_notes = list(tracer_notes)

    for i, (_, row) in enumerate(selected.iterrows(), start=1):
        out_path, info, notes = plot_case(i, row, candidates, tracer)
        selected_records.append(info)
        all_notes.extend(["case {}: {}".format(i, note) for note in notes])
        print("Wrote {}".format(out_path))

    selected_df = pd.DataFrame(selected_records)
    selected_csv = OUT_DIR / "usgs_riversed_daily_ssc_merit_selected_records.csv"
    selected_df.to_csv(selected_csv, index=False)
    print("Wrote {}".format(selected_csv))

    notes_path = OUT_DIR / "usgs_riversed_daily_ssc_merit_plot_notes.txt"
    notes_lines = [
        "USGS vs RiverSed daily SSC MERIT plot notes",
        "",
        "Target rows in pair table: {:,}".format(len(target)),
        "Selected cases: {:,}".format(len(selected_df)),
        "MERIT_DIR: {}".format(MERIT_DIR),
        "",
        "The script matches each plotted point to the nearest MERIT reach using",
        "basin_tracer.UpstreamBasinTracer.find_best_reach(lon, lat, reported_area=None).",
        "",
        "Geometry/read notes:",
        "",
    ]
    if all_notes:
        notes_lines.extend("- {}".format(note) for note in all_notes)
    else:
        notes_lines.append("- No geometry/read warnings.")
    notes_path.write_text("\n".join(notes_lines), encoding="utf-8")
    print("Wrote {}".format(notes_path))

    print("")
    print("Done. Open the PNG files in:")
    print("  {}".format(OUT_DIR))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

