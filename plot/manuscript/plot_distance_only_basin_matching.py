#!/usr/bin/env python3
"""Plot one distance-only MERIT basin matching example.

The script uses project-relative defaults and reads MERIT_DIR from the
environment when local MERIT Hydro files live outside the repository.
It reuses basin_tracer.UpstreamBasinTracer so candidate reach distances match
the production S4 basin-matching logic.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import matplotlib

matplotlib.use("Agg")

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyproj import CRS, Transformer
from shapely.geometry import Point
from shapely.ops import nearest_points, transform


SCRIPT_ROOT = Path(__file__).resolve().parents[2]
MERIT_DIR = Path(os.environ.get("MERIT_DIR", "/path/to/MERIT_Hydro_v07_Basins_v01_bugfix1"))
RELEASE_DIR = SCRIPT_ROOT / "output" / "sed_reference_release"
STATION_CATALOG = RELEASE_DIR / "station_catalog.csv"
OUT_DIR = RELEASE_DIR / "figures" / "distance_only_matching"

DISTANCE_WARNING_TOLERANCE_M = 5.0

if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from basin_tracer import UpstreamBasinTracer  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot a distance-only basin matching diagnostic figure."
    )
    parser.add_argument("--cluster-uid", default="", help="Release cluster_uid to plot.")
    parser.add_argument(
        "--master-station-index",
        type=int,
        default=None,
        help="Release master_station_index to plot.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--top-candidates",
        type=int,
        default=10,
        help="Number of nearest candidate reaches to annotate.",
    )
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR, help="Output directory.")
    parser.add_argument("--merit-dir", type=Path, default=MERIT_DIR, help="MERIT Hydro root.")
    parser.add_argument(
        "--station-catalog",
        type=Path,
        default=STATION_CATALOG,
        help="Release station_catalog.csv path.",
    )
    parser.add_argument(
        "--min-distance-m",
        type=float,
        default=1.0,
        help="Preferred random-case minimum basin distance in meters.",
    )
    parser.add_argument(
        "--max-distance-m",
        type=float,
        default=1000.0,
        help="Preferred random-case maximum basin distance in meters.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Output figure DPI.")
    parser.add_argument(
        "--formats",
        default="png,pdf",
        help="Comma-separated output figure formats, e.g. png,pdf.",
    )
    return parser.parse_args()


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


def as_int_or_none(value: Any) -> int | None:
    try:
        if pd.isna(value):
            return None
        return int(value)
    except Exception:
        return None


def parse_formats(value: str) -> List[str]:
    formats = []
    for item in value.split(","):
        fmt = item.strip().lower().lstrip(".")
        if fmt:
            formats.append(fmt)
    return formats or ["png"]


def select_station(args: argparse.Namespace) -> Tuple[pd.Series, List[str]]:
    catalog = pd.read_csv(args.station_catalog)
    required = {
        "cluster_uid",
        "master_station_index",
        "basin_status",
        "basin_match_quality",
        "basin_distance_m",
        "lon",
        "lat",
    }
    missing = sorted(required - set(catalog.columns))
    if missing:
        raise ValueError("station catalog missing required columns: {}".format(missing))

    work = catalog.copy()
    work["_basin_status_norm"] = work["basin_status"].fillna("").astype(str).str.strip()
    work["_match_quality_norm"] = (
        work["basin_match_quality"].fillna("").astype(str).str.strip()
    )
    work["_distance_num"] = pd.to_numeric(work["basin_distance_m"], errors="coerce")

    eligible = work[
        work["_basin_status_norm"].eq("resolved")
        & work["_match_quality_norm"].eq("distance_only")
        & work["_distance_num"].notna()
    ].copy()
    if eligible.empty:
        raise ValueError("no resolved distance_only stations found")

    notes: List[str] = []
    if args.cluster_uid:
        selected = eligible[eligible["cluster_uid"].astype(str).eq(args.cluster_uid)]
        if selected.empty:
            raise ValueError("no eligible station found for cluster_uid={}".format(args.cluster_uid))
    elif args.master_station_index is not None:
        selected = eligible[
            pd.to_numeric(eligible["master_station_index"], errors="coerce").eq(
                args.master_station_index
            )
        ]
        if selected.empty:
            raise ValueError(
                "no eligible station found for master_station_index={}".format(
                    args.master_station_index
                )
            )
    else:
        preferred = eligible[
            eligible["_distance_num"].between(
                float(args.min_distance_m), float(args.max_distance_m), inclusive="both"
            )
        ]
        if preferred.empty:
            notes.append(
                "No eligible station in preferred distance range; sampled from all eligible rows."
            )
            preferred = eligible
        selected = preferred.sample(n=1, random_state=int(args.seed))

    selected = selected.sort_values(
        by=["master_station_index", "cluster_uid", "resolution"],
        kind="stable",
    )
    return selected.iloc[0], notes


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
    to_metric = Transformer.from_crs("EPSG:4326", metric_crs, always_xy=True)
    to_wgs84 = Transformer.from_crs(metric_crs, "EPSG:4326", always_xy=True)
    point_metric = Point(*to_metric.transform(lon, lat))
    geom_metric = transform(to_metric.transform, geom)
    _, near_metric = nearest_points(point_metric, geom_metric)
    near_lon, near_lat = to_wgs84.transform(near_metric.x, near_metric.y)
    dist_m = point_metric.distance(near_metric)
    return float(near_lon), float(near_lat), float(dist_m)


def candidate_extent(
    reaches: gpd.GeoDataFrame,
    lon: float,
    lat: float,
    pad_fraction: float = 0.18,
) -> Tuple[float, float, float, float]:
    minx, miny, maxx, maxy = reaches.total_bounds
    minx = min(float(minx), lon)
    maxx = max(float(maxx), lon)
    miny = min(float(miny), lat)
    maxy = max(float(maxy), lat)

    width = max(maxx - minx, 0.01)
    height = max(maxy - miny, 0.01)
    pad_x = max(0.02, width * pad_fraction)
    pad_y = max(0.02, height * pad_fraction)
    return minx - pad_x, miny - pad_y, maxx + pad_x, maxy + pad_y


def label_positions(
    label_rows: pd.DataFrame,
    extent: Tuple[float, float, float, float],
) -> pd.DataFrame:
    if label_rows.empty:
        return label_rows

    xmin, ymin, xmax, ymax = extent
    width = xmax - xmin
    height = ymax - ymin
    work = label_rows.sort_values("rank").copy()
    n_rows = len(work)
    split = int(math.ceil(n_rows / 2.0))
    right_idx = list(work.index[:split])
    left_idx = list(work.index[split:])

    work["label_x"] = np.nan
    work["label_y"] = np.nan
    work["label_ha"] = "left"

    def y_slots(count: int) -> np.ndarray:
        if count <= 1:
            return np.array([(ymin + ymax) / 2.0])
        return np.linspace(ymax - 0.14 * height, ymin + 0.14 * height, count)

    for idx, y in zip(right_idx, y_slots(len(right_idx))):
        work.loc[idx, "label_x"] = xmax - 0.04 * width
        work.loc[idx, "label_y"] = y
        work.loc[idx, "label_ha"] = "right"

    for idx, y in zip(left_idx, y_slots(len(left_idx))):
        work.loc[idx, "label_x"] = xmin + 0.04 * width
        work.loc[idx, "label_y"] = y
        work.loc[idx, "label_ha"] = "left"

    return work


def prepare_candidates(
    candidates: gpd.GeoDataFrame,
    station: pd.Series,
    best: Dict[str, Any],
    top_n: int,
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, List[str]]:
    notes: List[str] = []
    if candidates is None or candidates.empty:
        raise ValueError("no nearby candidate reaches found")

    work = candidates.copy()
    work["COMID_num"] = pd.to_numeric(work["COMID"], errors="coerce").astype("Int64")
    work["dist_m"] = pd.to_numeric(work["dist_m"], errors="coerce")
    work = work.dropna(subset=["COMID_num", "dist_m"]).copy()
    work = work.sort_values("dist_m", kind="stable")
    work = work.drop_duplicates(subset=["COMID_num"], keep="first").copy()
    work["rank"] = np.arange(1, len(work) + 1, dtype=int)

    best_comid = as_int_or_none(best.get("COMID"))
    if best_comid is None:
        raise ValueError("find_best_reach did not return a COMID")
    work["is_matched"] = work["COMID_num"].astype("int64").eq(best_comid)

    if not bool(work["is_matched"].any()):
        notes.append("Matched COMID was not present in nearby candidate table.")

    top_n = max(1, int(top_n))
    top = work.head(top_n).copy()
    if bool(work["is_matched"].any()) and not bool(top["is_matched"].any()):
        matched = work[work["is_matched"]].head(1)
        top = pd.concat([top, matched], ignore_index=False)
        top = top.sort_values("rank", kind="stable")

    lon = as_float(station["lon"])
    lat = as_float(station["lat"])
    metric_crs = local_metric_crs(lon, lat)
    rows = []
    for _, rec in work.iterrows():
        near_lon, near_lat, nearest_dist_m = nearest_point_on_geometry(
            rec.geometry, lon, lat, metric_crs
        )
        rows.append(
            {
                "rank": int(rec["rank"]),
                "COMID": int(rec["COMID_num"]),
                "pfaf_code": clean_text(rec.get("pfaf_code")),
                "dist_m": float(rec["dist_m"]),
                "nearest_dist_m_recomputed": nearest_dist_m,
                "nearest_lon": near_lon,
                "nearest_lat": near_lat,
                "uparea": as_float(rec.get("uparea")),
                "order": as_int_or_none(rec.get("order")),
                "is_matched": bool(rec["is_matched"]),
            }
        )
    table = pd.DataFrame(rows)

    catalog_distance = as_float(station.get("basin_distance_m"))
    if not table.empty:
        table["catalog_basin_distance_m"] = catalog_distance

    rank1 = table[table["rank"].eq(1)].head(1)
    if not rank1.empty:
        delta = abs(float(rank1["dist_m"].iloc[0]) - catalog_distance)
        if np.isfinite(delta) and delta > DISTANCE_WARNING_TOLERANCE_M:
            notes.append(
                "Rank-1 candidate distance differs from catalog by {:.3f} m.".format(delta)
            )

    return top, table, notes


def plot_case(
    station: pd.Series,
    candidates: gpd.GeoDataFrame,
    top: gpd.GeoDataFrame,
    candidate_table: pd.DataFrame,
    best: Dict[str, Any],
    out_base: Path,
    formats: Iterable[str],
    dpi: int,
) -> List[Path]:
    lon = as_float(station["lon"])
    lat = as_float(station["lat"])
    cluster_uid = clean_text(station["cluster_uid"])
    station_name = clean_text(station.get("station_name")) or "unnamed station"
    river_name = clean_text(station.get("river_name")) or "unknown river"
    best_comid = as_int_or_none(best.get("COMID"))
    catalog_distance = as_float(station.get("basin_distance_m"))

    fig, ax = plt.subplots(figsize=(9.0, 8.0))

    candidates.plot(ax=ax, color="#c9c9c9", linewidth=0.35, alpha=0.7, zorder=1)

    nonmatched_top = top[~top["is_matched"]].copy()
    if not nonmatched_top.empty:
        nonmatched_top.plot(ax=ax, color="#f2a23a", linewidth=1.2, alpha=0.95, zorder=3)

    matched = top[top["is_matched"]].copy()
    if not matched.empty:
        matched.plot(ax=ax, color="#d7191c", linewidth=2.6, alpha=1.0, zorder=4)

    ax.scatter([lon], [lat], marker="*", s=170, color="black", edgecolor="white", zorder=6)
    ax.text(lon, lat, "  station", fontsize=9, va="center", ha="left", zorder=7)

    top_ranks = set(int(v) for v in top["rank"].tolist())
    label_rows = candidate_table[candidate_table["rank"].isin(top_ranks)].copy()
    label_rows = label_rows.sort_values("rank")

    xmin, ymin, xmax, ymax = candidate_extent(top, lon, lat)
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)

    label_rows = label_positions(label_rows, (xmin, ymin, xmax, ymax))
    for rec in label_rows.itertuples(index=False):
        color = "#d7191c" if bool(rec.is_matched) else "#9a6500"
        label = "#{:d}: {:.1f} m".format(int(rec.rank), float(rec.dist_m))
        ax.annotate(
            label,
            xy=(rec.nearest_lon, rec.nearest_lat),
            xytext=(rec.label_x, rec.label_y),
            fontsize=7.5,
            color=color,
            ha=rec.label_ha,
            va="center",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.72, "pad": 1.0},
            arrowprops={
                "arrowstyle": "-",
                "color": color,
                "alpha": 0.55,
                "linewidth": 0.9,
                "shrinkA": 1.5,
                "shrinkB": 1.5,
            },
            zorder=8,
        )

    subtitle = (
        "matched COMID={comid}, catalog basin_distance={dist:.2f} m\n"
        "candidate rank=1 by distance"
    ).format(comid=best_comid, dist=catalog_distance)
    ax.set_title(
        "{} distance-only basin match\n{} / {}".format(cluster_uid, station_name, river_name),
        fontsize=13,
    )
    ax.text(
        0.01,
        0.01,
        subtitle,
        transform=ax.transAxes,
        fontsize=8,
        ha="left",
        va="bottom",
        bbox={"facecolor": "white", "edgecolor": "#bbbbbb", "alpha": 0.88, "pad": 4.0},
        zorder=10,
    )

    legend_handles = [
        plt.Line2D([0], [0], color="#c9c9c9", lw=1.5, label="candidate MERIT reaches"),
        plt.Line2D([0], [0], color="#f2a23a", lw=2.0, label="nearest candidate reaches"),
        plt.Line2D([0], [0], color="#d7191c", lw=2.8, label="selected reach"),
        plt.Line2D([0], [0], color="black", marker="*", lw=0, markersize=11, label="station"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8, framealpha=0.92)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_aspect("equal", adjustable="box")
    ax.grid(True, color="#e6e6e6", linewidth=0.5)

    written: List[Path] = []
    for fmt in formats:
        out_path = out_base.with_suffix("." + fmt)
        fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
        written.append(out_path)
    plt.close(fig)
    return written


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        out = float(value)
        return out if np.isfinite(out) else None
    if isinstance(value, float):
        return value if np.isfinite(value) else None
    if pd.isna(value):
        return None
    return value


def main() -> int:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    formats = parse_formats(args.formats)

    station, notes = select_station(args)
    lon = as_float(station["lon"])
    lat = as_float(station["lat"])
    if not (np.isfinite(lon) and np.isfinite(lat)):
        raise ValueError("selected station has invalid coordinates")

    tracer = UpstreamBasinTracer(str(args.merit_dir))
    candidates = tracer.get_nearby_candidate_reaches(lon, lat)
    best = tracer.find_best_reach(lon, lat, reported_area=None)
    top, candidate_table, candidate_notes = prepare_candidates(
        candidates,
        station,
        best,
        args.top_candidates,
    )
    notes.extend(candidate_notes)

    cluster_uid = clean_text(station["cluster_uid"]) or "unknown_cluster"
    out_base = args.out_dir / "distance_only_matching_{}".format(cluster_uid)
    figure_paths = plot_case(
        station=station,
        candidates=candidates,
        top=top,
        candidate_table=candidate_table,
        best=best,
        out_base=out_base,
        formats=formats,
        dpi=args.dpi,
    )

    csv_path = out_base.with_name(out_base.name + "_candidates.csv")
    candidate_table.to_csv(csv_path, index=False)

    rank1 = candidate_table[candidate_table["rank"].eq(1)].head(1)
    rank1_comid = int(rank1["COMID"].iloc[0]) if not rank1.empty else None
    matched_comid = as_int_or_none(best.get("COMID"))
    rank1_distance = float(rank1["dist_m"].iloc[0]) if not rank1.empty else np.nan
    distance_delta = abs(rank1_distance - as_float(station.get("basin_distance_m")))
    if matched_comid is not None and rank1_comid != matched_comid:
        notes.append("Rank-1 COMID does not match find_best_reach COMID.")

    metadata = {
        "station_catalog": str(args.station_catalog),
        "merit_dir": str(args.merit_dir),
        "cluster_uid": cluster_uid,
        "master_station_index": as_int_or_none(station.get("master_station_index")),
        "resolution": clean_text(station.get("resolution")),
        "station_name": clean_text(station.get("station_name")),
        "river_name": clean_text(station.get("river_name")),
        "lat": lat,
        "lon": lon,
        "catalog_basin_distance_m": as_float(station.get("basin_distance_m")),
        "matched_comid": matched_comid,
        "matched_distance_m": as_float(best.get("distance")),
        "matched_pfaf_code": clean_text(best.get("pfaf_code")),
        "matched_quality": clean_text(best.get("match_quality")),
        "rank1_comid": rank1_comid,
        "rank1_distance_m": rank1_distance,
        "rank1_catalog_distance_delta_m": distance_delta,
        "n_candidates": int(len(candidate_table)),
        "top_candidates_plotted": int(len(top)),
        "plot_extent_source": "plotted_candidate_reach_bounds",
        "warnings": notes,
        "figure_paths": [str(path) for path in figure_paths],
        "candidate_csv": str(csv_path),
    }
    metadata_path = out_base.with_name(out_base.name + "_metadata.json")
    metadata_path.write_text(json.dumps(json_ready(metadata), indent=2), encoding="utf-8")

    print("Selected cluster_uid: {}".format(cluster_uid))
    print("Matched COMID: {}".format(matched_comid))
    print("Rank-1 COMID: {}".format(rank1_comid))
    print("Catalog/rank-1 distance delta: {:.3f} m".format(distance_delta))
    for path in figure_paths:
        print("Wrote figure: {}".format(path))
    print("Wrote candidates: {}".format(csv_path))
    print("Wrote metadata: {}".format(metadata_path))
    if notes:
        print("Warnings:")
        for note in notes:
            print("  - {}".format(note))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
