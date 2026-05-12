#!/usr/bin/env python3
"""Plot a station-level basin diagnostic figure from s4 GeoPackage outputs.

This helper reads one station from:
  - output/s4_local_catchments.gpkg
  - output/s4_upstream_basins.gpkg

It avoids heavy GIS dependencies by decoding the GeoPackage geometry blob and
parsing the embedded WKB directly. The output figure is meant for manual review:
the left panel shows the matched local catchment and the station point; the
right panel shows the traced upstream basin and the same station point.
"""

import math
import sqlite3
import struct
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parent / "output"
DEFAULT_LOCAL_GPKG = ROOT / "s4_local_catchments.gpkg"
DEFAULT_UPSTREAM_GPKG = ROOT / "s4_upstream_basins.gpkg"
DEFAULT_OUT = ROOT / "station_1825_basin_diagnostic.png"

# User-editable defaults.
# Change these values when you want to inspect another station, then run:
#   python3 plot_station_basin_diagnostic.py
REVIEW_STATION_ID = "1825"
REVIEW_LOCAL_GPKG = DEFAULT_LOCAL_GPKG
REVIEW_UPSTREAM_GPKG = DEFAULT_UPSTREAM_GPKG
REVIEW_OUT = DEFAULT_OUT


Coord = Tuple[float, float]
Ring = List[Coord]
PolygonRings = List[Ring]


def _read_uint32(blob: bytes, offset: int, endian: str) -> Tuple[int, int]:
    return struct.unpack_from(endian + "I", blob, offset)[0], offset + 4


def _read_double(blob: bytes, offset: int, endian: str) -> Tuple[float, int]:
    return struct.unpack_from(endian + "d", blob, offset)[0], offset + 8


def _normalize_wkb_type(raw_type: int) -> int:
    """Collapse EWKB/ISO dimensional flags to the 2D base geometry type."""
    # ISO WKB can encode Z/M/ZM as +1000/+2000/+3000; EWKB may use high bits.
    base = raw_type
    if raw_type >= 1000 and raw_type < 4000:
        base = raw_type % 1000
    # Strip common EWKB high-bit flags when present.
    base = base & 0x000000FF if base & 0xF0000000 else base
    return base


def _parse_linear_ring(blob: bytes, offset: int, endian: str) -> Tuple[Ring, int]:
    n_points, offset = _read_uint32(blob, offset, endian)
    ring: Ring = []
    for _ in range(n_points):
        x, offset = _read_double(blob, offset, endian)
        y, offset = _read_double(blob, offset, endian)
        ring.append((x, y))
    return ring, offset


def _parse_polygon_wkb(blob: bytes, offset: int = 0) -> Tuple[List[PolygonRings], int]:
    byte_order = blob[offset]
    endian = "<" if byte_order == 1 else ">"
    offset += 1
    raw_type, offset = _read_uint32(blob, offset, endian)
    geom_type = _normalize_wkb_type(raw_type)

    if geom_type == 3:  # Polygon
        n_rings, offset = _read_uint32(blob, offset, endian)
        rings: PolygonRings = []
        for _ in range(n_rings):
            ring, offset = _parse_linear_ring(blob, offset, endian)
            rings.append(ring)
        return [rings], offset

    if geom_type == 6:  # MultiPolygon
        n_polygons, offset = _read_uint32(blob, offset, endian)
        polygons: List[PolygonRings] = []
        for _ in range(n_polygons):
            sub_polygons, offset = _parse_polygon_wkb(blob, offset)
            polygons.extend(sub_polygons)
        return polygons, offset

    raise ValueError(f"Unsupported WKB geometry type: {raw_type} (base={geom_type})")


def gpkg_blob_to_polygons(blob: bytes) -> List[PolygonRings]:
    """Extract polygon coordinates from a GeoPackage geometry blob."""
    if not blob or blob[:2] != b"GP":
        raise ValueError("Not a valid GeoPackage geometry blob")

    flags = blob[3]
    endian = "<" if (flags & 1) else ">"
    envelope_code = (flags >> 1) & 0x07
    envelope_bytes = {
        0: 0,
        1: 32,  # minx, maxx, miny, maxy
        2: 48,  # + minz, maxz
        3: 48,  # + minm, maxm
        4: 64,  # + minz, maxz, minm, maxm
    }.get(envelope_code)
    if envelope_bytes is None:
        raise ValueError(f"Unsupported GeoPackage envelope code: {envelope_code}")

    # Header layout: magic(2) + version(1) + flags(1) + srs_id(4) + envelope + wkb
    _ = struct.unpack_from(endian + "I", blob, 4)[0]
    wkb_offset = 8 + envelope_bytes
    polygons, _ = _parse_polygon_wkb(blob, wkb_offset)
    return polygons


def _polygon_bounds(polygons: Sequence[PolygonRings]) -> Tuple[float, float, float, float]:
    xs: List[float] = []
    ys: List[float] = []
    for polygon in polygons:
        for ring in polygon:
            for x, y in ring:
                xs.append(x)
                ys.append(y)
    return min(xs), min(ys), max(xs), max(ys)


def _combined_bounds(
    polygons: Sequence[PolygonRings],
    point: Coord,
    min_span: float = 0.02,
) -> Tuple[float, float, float, float]:
    minx, miny, maxx, maxy = _polygon_bounds(polygons)
    px, py = point
    minx = min(minx, px)
    maxx = max(maxx, px)
    miny = min(miny, py)
    maxy = max(maxy, py)

    span_x = max(maxx - minx, min_span)
    span_y = max(maxy - miny, min_span)
    pad_x = span_x * 0.08
    pad_y = span_y * 0.08
    cx = 0.5 * (minx + maxx)
    cy = 0.5 * (miny + maxy)
    half_x = 0.5 * span_x + pad_x
    half_y = 0.5 * span_y + pad_y
    return cx - half_x, cy - half_y, cx + half_x, cy + half_y


def _plot_polygons(ax, polygons: Sequence[PolygonRings], facecolor: str, edgecolor: str) -> None:
    for polygon in polygons:
        if not polygon:
            continue
        for ring_index, ring in enumerate(polygon):
            if len(ring) < 3:
                continue
            xs = [pt[0] for pt in ring]
            ys = [pt[1] for pt in ring]
            if ring_index == 0:
                ax.fill(xs, ys, facecolor=facecolor, edgecolor=edgecolor, alpha=0.35, linewidth=1.0)
            else:
                ax.fill(xs, ys, facecolor="white", edgecolor=edgecolor, alpha=1.0, linewidth=0.8)


def _fetch_station_row(gpkg_path: Path, layer: str, station_id: str) -> Dict:
    con = sqlite3.connect(str(gpkg_path))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    row = cur.execute(
        "SELECT * FROM '{layer}' WHERE CAST(station_id AS TEXT)=? LIMIT 1".format(layer=layer),
        (station_id,),
    ).fetchone()
    con.close()
    if row is None:
        raise KeyError(f"station_id={station_id} not found in {gpkg_path.name}:{layer}")
    return dict(row)


def _format_value(value) -> str:
    if value is None:
        return "NA"
    if isinstance(value, float):
        if math.isnan(value):
            return "NA"
        return f"{value:.3f}"
    return str(value)


def plot_station_diagnostic(
    station_id: str,
    local_gpkg: Path,
    upstream_gpkg: Path,
    out_png: Path,
) -> Path:
    local_row = _fetch_station_row(local_gpkg, "s4_local_catchments", station_id)
    upstream_row = _fetch_station_row(upstream_gpkg, "s4_upstream_basins", station_id)

    local_polygons = gpkg_blob_to_polygons(local_row["geom"])
    upstream_polygons = gpkg_blob_to_polygons(upstream_row["geom"])
    point = (float(upstream_row["lon"]), float(upstream_row["lat"]))

    fig, axes = plt.subplots(1, 2, figsize=(15, 7), constrained_layout=True)
    configs = [
        (
            axes[0],
            local_polygons,
            "Local Catchment",
            "#87bfff",
            "#2d6cdf",
            bool(upstream_row.get("point_in_local")),
        ),
        (
            axes[1],
            upstream_polygons,
            "Upstream Basin",
            "#b6d7a8",
            "#38761d",
            bool(upstream_row.get("point_in_basin")),
        ),
    ]

    for ax, polygons, title, facecolor, edgecolor, covers in configs:
        _plot_polygons(ax, polygons, facecolor, edgecolor)
        ax.scatter([point[0]], [point[1]], marker="*", s=180, color="#d62728", edgecolors="black", linewidths=0.8, zorder=5)
        minx, miny, maxx, maxy = _combined_bounds(polygons, point)
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)
        ax.set_aspect("equal", adjustable="box")
        ax.grid(True, color="#d9d9d9", linewidth=0.5, linestyle="--", alpha=0.8)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title(
            "{}\npoint_in_{} = {}".format(
                title,
                "local" if "Local" in title else "basin",
                "True" if covers else "False",
            )
        )

    station_name = upstream_row.get("source_station_name") or "NA"
    source_id = upstream_row.get("source_station_id") or "NA"
    summary_lines = [
        f"station_id = {station_id}",
        f"station_name = {station_name}",
        f"source_station_id = {source_id}",
        f"basin_id = {_format_value(upstream_row.get('basin_id'))}",
        f"match_quality = {_format_value(upstream_row.get('match_quality'))}",
        f"basin_status = {_format_value(upstream_row.get('basin_status'))}",
        f"basin_flag = {_format_value(upstream_row.get('basin_flag'))}",
        f"distance_m = {_format_value(upstream_row.get('distance_m'))}",
        f"reported_area_km2 = {_format_value(upstream_row.get('reported_area'))}",
        f"uparea_merit_km2 = {_format_value(upstream_row.get('uparea_merit'))}",
        f"area_error_log10 = {_format_value(upstream_row.get('area_error'))}",
        f"n_upstream_reaches = {_format_value(upstream_row.get('n_upstream_reaches'))}",
    ]
    fig.suptitle("Station Basin Diagnostic", fontsize=15, y=1.02)
    fig.text(
        0.5,
        -0.03,
        "\n".join(summary_lines),
        ha="center",
        va="top",
        fontsize=10,
        family="monospace",
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "#f7f7f7", "edgecolor": "#c8c8c8"},
    )

    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_png


def main() -> None:
    out_path = plot_station_diagnostic(
        station_id=str(REVIEW_STATION_ID),
        local_gpkg=REVIEW_LOCAL_GPKG,
        upstream_gpkg=REVIEW_UPSTREAM_GPKG,
        out_png=REVIEW_OUT,
    )
    print(f"Wrote diagnostic plot: {out_path}")


if __name__ == "__main__":
    main()
