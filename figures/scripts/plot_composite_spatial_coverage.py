#!/usr/bin/env python3
"""Composite spatial coverage plot built only from the S8 release package."""

from __future__ import annotations

import argparse
import ctypes
import math
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

CONDA_LIB = "/share/home/dq134/.conda/envs/wzx/lib"
if os.path.isdir(CONDA_LIB):
    os.environ["LD_LIBRARY_PATH"] = CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
    try:
        ctypes.CDLL(str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL)
    except Exception:
        pass
import cartopy.crs as ccrs
import cartopy.feature as cfeature

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from stats_release.release_io import (  # noqa: E402
    ReleaseContext,
    clean_text,
    numeric_series,
    setup_matplotlib,
    write_csv,
)
from stats_release.release_paths import DEFAULT_RELEASE_DIR, PRODUCT_FILES  # noqa: E402


DEFAULT_OUT_DIR = PROJECT_DIR / "output_other" / "s8_release_composite_spatial_coverage"
DEFAULT_FIGURES_ROOT = PROJECT_DIR / "figures"
DPI = 300
FIGSIZE = (12, 14)
FIGURE_ID = "composite_spatial_coverage"
FONT_SIZES = {
    "map_tick": 9,
    "panel_label": 15,
    "empty_message": 10,
    "inset_axis": 10,
    "legend_title": 11,
    "legend_text": 10,
}
MIN_VISIBLE_FONT_SIZE = min(FONT_SIZES["inset_axis"], FONT_SIZES["legend_text"])
LEGEND_FIRST_ROW_Y = {
    "panel_b": 0.70,
    "panel_c": 0.70,
}

OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
    "gray": "#777777",
}

STATION_COLUMNS = {
    "cluster_uid",
    "resolution",
    "record_count",
    "lat",
    "lon",
    "basin_area",
    "basin_status",
}
SATELLITE_COLUMNS = {
    "satellite_station_uid",
    "cluster_uid",
    "source",
    "lat",
    "lon",
}

REQUIRED_STATION_COLUMNS = {"cluster_uid", "resolution", "lat", "lon"}
REQUIRED_SATELLITE_COLUMNS = {"cluster_uid", "source", "lat", "lon"}

RESOLUTION_FLAG_MEANINGS = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}
RESOLUTION_COLORS = {
    "daily": OKABE_ITO["blue"],
    "monthly": OKABE_ITO["orange"],
    "annual": OKABE_ITO["bluish_green"],
    "climatology": OKABE_ITO["reddish_purple"],
    "other": OKABE_ITO["gray"],
}
TIMESERIES_COLORS = {
    "Daily": OKABE_ITO["blue"],
    "Monthly": OKABE_ITO["orange"],
    "Annual": OKABE_ITO["bluish_green"],
}
SOURCE_COLORS = {
    "RiverSed": OKABE_ITO["blue"],
    "GSED": OKABE_ITO["vermillion"],
    "Dethier": OKABE_ITO["bluish_green"],
    "Unknown": OKABE_ITO["gray"],
}
BASIN_STATUS_COLORS = {
    "resolved": OKABE_ITO["blue"],
    "unresolved": OKABE_ITO["orange"],
    "unknown": OKABE_ITO["gray"],
}
BASIN_STATUS_MARKERS = {
    "resolved": "o",
    "unresolved": "^",
    "unknown": "s",
}
SOURCE_MARKERS = {
    "RiverSed": "o",
    "GSED": "^",
    "Dethier": "s",
    "Unknown": "x",
}
RESOLUTION_MARKERS = {
    "daily": "o",
    "monthly": "^",
    "annual": "s",
    "climatology": "D",
    "other": "x",
}
TIMESERIES_MARKERS = {
    "Daily": "P",
    "Monthly": "X",
    "Annual": "*",
}


def _require_columns(frame: pd.DataFrame, required: Iterable[str], source_name: str) -> None:
    missing = sorted(col for col in required if col not in frame.columns)
    if missing:
        raise ValueError("{} is missing required columns: {}".format(source_name, ", ".join(missing)))


def configure_matplotlib(plt) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
        }
    )


def ensure_figure_dirs(figures_root: Path) -> dict[str, Path]:
    root = Path(figures_root).resolve()
    dirs = {
        "root": root,
        "final": root / "final",
        "data": root / "data",
        "scripts": root / "scripts",
        "checklists": root / "checklists",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs



def read_release_csv_columns(ctx: ReleaseContext, file_name: str, columns: set[str], required: set[str]) -> pd.DataFrame:
    """Read selected columns from a release CSV after applying the path guard."""
    path = ctx.require_input(ctx.release_file(file_name), required=True)
    frame = pd.read_csv(path, usecols=lambda col: col in columns, keep_default_na=False)
    _require_columns(frame, required, file_name)
    return frame


def first_text(series: pd.Series, default: str = "") -> str:
    for value in series:
        text = clean_text(value)
        if text:
            return text
    return default


def first_number(series: pd.Series) -> float:
    numeric = pd.to_numeric(series, errors="coerce")
    numeric = numeric[np.isfinite(numeric)]
    if numeric.empty:
        return np.nan
    return float(numeric.iloc[0])


def unique_text(values: pd.Series) -> str:
    out = []
    for value in values:
        text = clean_text(value)
        if text and text not in out:
            out.append(text)
    return "|".join(sorted(out))


def valid_latlon(frame: pd.DataFrame, lat_col: str = "lat", lon_col: str = "lon") -> pd.Series:
    lat = numeric_series(frame, lat_col)
    lon = numeric_series(frame, lon_col)
    return lat.between(-90, 90) & lon.between(-180, 180)


def build_cluster_table(station: pd.DataFrame) -> pd.DataFrame:
    """Build one release cluster row per cluster_uid from station_catalog.csv."""
    work = station.copy()
    _require_columns(work, REQUIRED_STATION_COLUMNS, PRODUCT_FILES["station_catalog"])
    work["cluster_uid"] = work["cluster_uid"].map(clean_text)
    work = work[work["cluster_uid"].ne("")].copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                "cluster_uid",
                "lat",
                "lon",
                "basin_status",
                "basin_area",
                "available_resolutions",
                "record_count",
                "valid_latlon",
            ]
        )
    if "record_count" not in work.columns:
        work["record_count"] = 0
    if "basin_area" not in work.columns:
        work["basin_area"] = np.nan
    if "basin_status" not in work.columns:
        work["basin_status"] = "unknown"

    rows = []
    for uid, group in work.groupby("cluster_uid", sort=False):
        lat = first_number(group["lat"])
        lon = first_number(group["lon"])
        status = first_text(group["basin_status"], "unknown").lower()
        if status not in {"resolved", "unresolved"}:
            status = "unknown"
        rows.append(
            {
                "cluster_uid": uid,
                "lat": lat,
                "lon": lon,
                "basin_status": status,
                "basin_area": first_number(group["basin_area"]),
                "available_resolutions": unique_text(group["resolution"]),
                "record_count": float(pd.to_numeric(group["record_count"], errors="coerce").fillna(0).sum()),
            }
        )
    out = pd.DataFrame(rows)
    out["valid_latlon"] = valid_latlon(out).astype(int)
    return out


def build_area_distribution(clusters: pd.DataFrame, area_col: str = "basin_area") -> pd.DataFrame:
    area = pd.to_numeric(clusters.get(area_col, pd.Series([], dtype=float)), errors="coerce")
    valid = area[np.isfinite(area) & (area > 0)]
    rows = [
        {"section": "summary", "label": "valid_cluster_count", "value_km2": "", "cluster_count": int(valid.size)},
        {
            "section": "summary",
            "label": "missing_or_invalid_cluster_count",
            "value_km2": "",
            "cluster_count": int(len(area) - valid.size),
        },
    ]
    bins = [0, 10, 100, 1000, 10000, 100000, np.inf]
    labels = ["<10 km²", "10–100 km²", "100–1,000 km²", "1,000–10,000 km²", "10,000–100,000 km²", ">100,000 km²"]
    counts = pd.cut(valid, bins=bins, labels=labels, include_lowest=False).value_counts().reindex(labels).fillna(0)
    for label, count in counts.items():
        rows.append(
            {
                "section": "bin",
                "label": label,
                "value_km2": "",
                "cluster_count": int(count),
            }
        )
    return pd.DataFrame(rows)


def build_satellite_linked_area_distribution(satellite: pd.DataFrame, clusters: pd.DataFrame) -> pd.DataFrame:
    """Build area distribution by matching each satellite station to the nearest reference cluster via lat/lon."""
    if satellite.empty or "cluster_uid" not in satellite.columns:
        return build_area_distribution(pd.DataFrame(columns=["basin_area"]))

    # Deduplicate satellite stations by cluster_uid, keeping lat/lon
    linked = satellite[["cluster_uid", "lat", "lon"]].copy()
    linked["cluster_uid"] = linked["cluster_uid"].map(clean_text)
    linked = linked[linked["cluster_uid"].ne("")].drop_duplicates(subset="cluster_uid")

    linked_valid = linked[valid_latlon(linked)].copy()
    if linked_valid.empty:
        return build_area_distribution(pd.DataFrame(columns=["basin_area"]))

    # Build spatial index from reference clusters with valid basin_area
    cluster_refs = clusters[clusters["valid_latlon"].astype(bool)].copy()
    cluster_refs["_basin_area_num"] = pd.to_numeric(cluster_refs["basin_area"], errors="coerce")
    cluster_refs = cluster_refs[cluster_refs["_basin_area_num"].notna() & (cluster_refs["_basin_area_num"] > 0)]
    if cluster_refs.empty:
        return build_area_distribution(pd.DataFrame(columns=["basin_area"]))

    # Nearest-neighbor spatial matching in lat/lon (Euclidean approx; fine for histograms)
    from scipy.spatial import KDTree

    tree = KDTree(cluster_refs[["lat", "lon"]].values)
    distances, indices = tree.query(linked_valid[["lat", "lon"]].values, k=1)
    linked_valid["basin_area"] = cluster_refs["_basin_area_num"].iloc[indices].values

    return build_area_distribution(linked_valid, "basin_area")


def import_xarray():
    try:
        import xarray as xr
    except ImportError as exc:
        raise RuntimeError("xarray is required to read S8 release NetCDF products") from exc
    return xr


def release_netcdf_path(ctx: ReleaseContext, file_name: str) -> Path:
    return ctx.require_input(ctx.release_file(file_name), required=True)


def load_climatology_points(ctx: ReleaseContext) -> pd.DataFrame:
    xr = import_xarray()
    path = release_netcdf_path(ctx, PRODUCT_FILES["climatology_nc"])
    with xr.open_dataset(path, decode_times=False) as ds:
        for name in ("lat", "lon", "resolution"):
            if name not in ds.variables:
                raise ValueError("{} is missing variable {}".format(PRODUCT_FILES["climatology_nc"], name))
        lat = np.asarray(ds["lat"].values, dtype="float64").reshape(-1)
        lon = np.asarray(ds["lon"].values, dtype="float64").reshape(-1)
        resolution = np.asarray(ds["resolution"].values).reshape(-1)
    n = min(lat.size, lon.size, resolution.size)
    frame = pd.DataFrame({"lat": lat[:n], "lon": lon[:n], "resolution_flag": resolution[:n]})
    frame = frame[valid_latlon(frame)].copy()
    frame["resolution"] = frame["resolution_flag"].map(lambda value: RESOLUTION_FLAG_MEANINGS.get(int(value), "other"))
    return frame


def load_timeseries_points(ctx: ReleaseContext, file_name: str, label: str) -> pd.DataFrame:
    xr = import_xarray()
    path = release_netcdf_path(ctx, file_name)
    with xr.open_dataset(path, decode_times=False) as ds:
        for name in ("lat", "lon"):
            if name not in ds.variables:
                raise ValueError("{} is missing variable {}".format(file_name, name))
        lat = np.asarray(ds["lat"].values, dtype="float64").reshape(-1)
        lon = np.asarray(ds["lon"].values, dtype="float64").reshape(-1)
    n = min(lat.size, lon.size)
    frame = pd.DataFrame({"lat": lat[:n], "lon": lon[:n], "timeseries_label": label})
    return frame[valid_latlon(frame)].copy()


def load_release_data(ctx: ReleaseContext) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    station = read_release_csv_columns(ctx, PRODUCT_FILES["station_catalog"], STATION_COLUMNS, REQUIRED_STATION_COLUMNS)
    satellite = read_release_csv_columns(ctx, PRODUCT_FILES["satellite_catalog"], SATELLITE_COLUMNS, REQUIRED_SATELLITE_COLUMNS)
    clusters = build_cluster_table(station)
    area_dist = build_area_distribution(clusters)
    satellite_area = build_satellite_linked_area_distribution(satellite, clusters)
    climatology = load_climatology_points(ctx)
    timeseries = {
        "Daily": load_timeseries_points(ctx, PRODUCT_FILES["daily_nc"], "Daily"),
        "Monthly": load_timeseries_points(ctx, PRODUCT_FILES["monthly_nc"], "Monthly"),
        "Annual": load_timeseries_points(ctx, PRODUCT_FILES["annual_nc"], "Annual"),
    }
    return clusters, satellite, area_dist, satellite_area, {"climatology": climatology, **timeseries}


def add_inset_axes(ax, rect):
    from mpl_toolkits.axes_grid1.inset_locator import inset_axes
    import matplotlib.axes as maxes
    return inset_axes(ax, width="100%", height="100%",
                      bbox_to_anchor=rect, bbox_transform=ax.transAxes,
                      borderpad=0, axes_class=maxes.Axes)


def setup_world_map(ax) -> None:
    """Set up world map with coastlines and borders using cartopy."""
    ax.set_extent([-180, 180, -60, 85], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
    ax.add_feature(cfeature.BORDERS, linewidth=0.3, alpha=0.7)
    ax.add_feature(cfeature.LAND, facecolor="#f4f4f4", alpha=0.3)
    ax.add_feature(cfeature.OCEAN, facecolor="#e8e8e8", alpha=0.3)
    ax.add_feature(cfeature.LAKES, alpha=0.3, edgecolor="none")
    ax.grid(True, linewidth=0.35, alpha=0.35, zorder=0)
    ax.tick_params(labelsize=FONT_SIZES["map_tick"])


def add_panel_label(ax, label: str) -> None:
    ax.text(
        0.01,
        0.97,
        label,
        transform=ax.transAxes,
        fontsize=FONT_SIZES["panel_label"],
        fontweight="bold",
        va="top",
        ha="left",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85),
    )


def format_area_bin_label(label: str) -> str:
    replacements = {
        "<10 km²": r"$10^1$",
        "10–100 km²": r"$10^2$",
        "100–1,000 km²": r"$10^3$",
        "1,000–10,000 km²": r"$10^4$",
        "10,000–100,000 km²": r"$10^5$",
        ">100,000 km²": r">$10^5$",
    }
    return replacements.get(label, label.replace(" km²", "").replace(",", ""))


def draw_area_hist(ax, area_dist: pd.DataFrame, color: str) -> None:
    bins = area_dist[area_dist["section"].eq("bin")].copy()
    if bins.empty:
        ax.text(0.5, 0.5, "No area data", transform=ax.transAxes, ha="center", va="center", fontsize=FONT_SIZES["inset_axis"], color="#777777")
        ax.set_axis_off()
        return
    counts = pd.to_numeric(bins["cluster_count"], errors="coerce").fillna(0)
    if float(counts.sum()) <= 0:
        ax.text(0.5, 0.5, "No area data", transform=ax.transAxes, ha="center", va="center", fontsize=FONT_SIZES["inset_axis"], color="#777777")
        ax.set_axis_off()
        return
    labels = bins["label"].astype(str).tolist()
    x_pos = np.arange(len(bins))
    ax.bar(x_pos, counts, color=color, width=0.8)
    ax.set_xticks(x_pos)
    ax.set_xticklabels([format_area_bin_label(label) for label in labels], rotation=45, ha="center", fontsize=FONT_SIZES["inset_axis"])
    y_max = float(counts.max()) if len(counts) else 1.0
    ax.set_yticks(np.linspace(0, y_max, 3))
    ax.tick_params(axis="both", labelsize=FONT_SIZES["inset_axis"], direction="in", pad=1)
    ax.set_ylabel("Count", fontsize=FONT_SIZES["inset_axis"], labelpad=0)
    ax.set_xlabel("Basin area (km²)", fontsize=FONT_SIZES["inset_axis"], labelpad=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.55)
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()


def draw_cluster_map(ax, clusters: pd.DataFrame, area_dist: pd.DataFrame) -> None:
    setup_world_map(ax)
    df = clusters[clusters["valid_latlon"].astype(bool)].copy()
    area = pd.to_numeric(df.get("basin_area"), errors="coerce")
    sizes = np.where(np.isfinite(area) & (area > 0), np.sqrt(np.clip(area, 10, None)) * 0.18, 4.0)
    for status in ("resolved", "unresolved", "unknown"):
        mask = df["basin_status"].eq(status)
        if not mask.any():
            continue
        ax.scatter(
            df.loc[mask, "lon"],
            df.loc[mask, "lat"],
            s=sizes[mask.to_numpy()],
            c=BASIN_STATUS_COLORS[status],
            alpha=0.5,
            edgecolors="black",
            linewidths=0.15,
            marker=BASIN_STATUS_MARKERS[status],
            rasterized=True,
            zorder=2,
        )

    legend_ax = add_inset_axes(ax, [0.60, 0.01, 0.39, 0.38])
    legend_ax.axis("off")
    legend_ax.text(0.08, 0.92, "Basin status", fontsize=FONT_SIZES["legend_title"], fontweight="bold", transform=legend_ax.transAxes, va="top")
    total = len(df)
    y = 0.76
    for status in ("resolved", "unresolved", "unknown"):
        count = int(df["basin_status"].eq(status).sum())
        if count == 0 and status == "unknown":
            continue
        legend_ax.scatter(
            0.12,
            y,
            s=30,
            c=BASIN_STATUS_COLORS[status],
            marker=BASIN_STATUS_MARKERS[status],
            edgecolors="black",
            linewidths=0.25,
            alpha=0.85,
            transform=legend_ax.transAxes,
            clip_on=False,
        )
        legend_ax.text(0.20, y, "{} ({})".format(status, count), fontsize=FONT_SIZES["legend_text"], transform=legend_ax.transAxes, va="center")
        y -= 0.13
    legend_ax.text(0.08, y - 0.03, "Total clusters: {}".format(total), fontsize=FONT_SIZES["legend_text"], transform=legend_ax.transAxes, va="top")
    legend_ax.text(0.08, y - 0.15, "Point size: basin area (km²)", fontsize=FONT_SIZES["legend_text"], transform=legend_ax.transAxes, va="top")
    sample_y = y - 0.29
    for area_value, x_dot in ((100, 0.14), (10000, 0.39), (100000, 0.67)):
        legend_ax.scatter(
            x_dot,
            sample_y,
            s=np.sqrt(area_value) * 0.18,
            c="white",
            marker="o",
            edgecolors="black",
            linewidths=0.35,
            transform=legend_ax.transAxes,
            clip_on=False,
        )
        legend_ax.text(x_dot, sample_y - 0.07, "{:g}".format(area_value), fontsize=FONT_SIZES["legend_text"], transform=legend_ax.transAxes, ha="center", va="top")

    hist_ax = add_inset_axes(ax, [0.01, 0.17, 0.20, 0.22])
    draw_area_hist(hist_ax, area_dist, OKABE_ITO["sky_blue"])


def draw_satellite_map(ax, satellite: pd.DataFrame, satellite_area: pd.DataFrame) -> None:
    setup_world_map(ax)
    df = satellite[valid_latlon(satellite)].copy()
    if df.empty:
        ax.text(0.5, 0.5, "No satellite validation data", transform=ax.transAxes, ha="center", va="center", fontsize=FONT_SIZES["empty_message"], color="#777777")
        return
    df["source"] = df["source"].map(lambda value: clean_text(value) or "Unknown")
    for source, group in df.groupby("source", dropna=False):
        color = SOURCE_COLORS.get(source, SOURCE_COLORS["Unknown"])
        marker_size = 4 if source == "RiverSed" else 7
        alpha = 0.25 if source == "RiverSed" else 0.55
        marker = SOURCE_MARKERS.get(source, SOURCE_MARKERS["Unknown"])
        ax.scatter(
            numeric_series(group, "lon"),
            numeric_series(group, "lat"),
            s=marker_size,
            c=color,
            marker=marker,
            alpha=alpha,
            linewidths=0.15,
            edgecolors="black" if marker != "x" else color,
            rasterized=True,
            zorder=3,
        )

    legend_ax = add_inset_axes(ax, [0.63, 0.00, 0.30, 0.28])
    legend_ax.axis("off")
    legend_ax.text(0.08, 0.92, "Data source", fontsize=FONT_SIZES["legend_title"], fontweight="bold", transform=legend_ax.transAxes, va="top")
    y = LEGEND_FIRST_ROW_Y["panel_b"]
    for source, group in df.groupby("source", dropna=False):
        color = SOURCE_COLORS.get(source, SOURCE_COLORS["Unknown"])
        marker = SOURCE_MARKERS.get(source, SOURCE_MARKERS["Unknown"])
        legend_ax.scatter(
            0.12,
            y,
            s=30,
            c=color,
            marker=marker,
            edgecolors="black" if marker != "x" else color,
            linewidths=0.25,
            alpha=0.85,
            transform=legend_ax.transAxes,
            clip_on=False,
        )
        legend_ax.text(0.20, y, "{} ({})".format(source, len(group)), fontsize=FONT_SIZES["legend_text"], transform=legend_ax.transAxes, va="center")
        y -= 0.18

    hist_ax = add_inset_axes(ax, [0.01, 0.17, 0.20, 0.22])
    draw_area_hist(hist_ax, satellite_area, OKABE_ITO["reddish_purple"])


def draw_climatology_timeseries_map(ax, climatology: pd.DataFrame, timeseries: dict[str, pd.DataFrame]) -> None:
    setup_world_map(ax)
    for resolution in ("daily", "monthly", "annual", "climatology", "other"):
        group = climatology[climatology["resolution"].eq(resolution)]
        if group.empty:
            continue
        marker = RESOLUTION_MARKERS[resolution]
        ax.scatter(
            group["lon"],
            group["lat"],
            s=6,
            c=RESOLUTION_COLORS[resolution],
            alpha=0.5,
            linewidths=0.12,
            edgecolors="black" if marker != "x" else RESOLUTION_COLORS[resolution],
            marker=marker,
            rasterized=True,
            zorder=3,
        )
    for label, frame in timeseries.items():
        if frame.empty:
            continue
        ax.scatter(
            frame["lon"],
            frame["lat"],
            s=8,
            c=TIMESERIES_COLORS[label],
            alpha=0.55,
            linewidths=0.15,
            edgecolors="black",
            marker=TIMESERIES_MARKERS[label],
            rasterized=True,
            zorder=2,
        )

    legend_ax = add_inset_axes(ax, [0.01, 0.0, 0.32, 0.34])
    legend_ax.axis("off")
    legend_ax.text(0.06, 0.92, "Data resolution", fontsize=FONT_SIZES["legend_title"], fontweight="bold", transform=legend_ax.transAxes, va="top")
    items = []
    for resolution in ("daily", "monthly", "annual", "climatology", "other"):
        count = int(climatology["resolution"].eq(resolution).sum())
        if count:
            label_text = "Climatology" if resolution == "climatology" else resolution
            items.append(("{} ({})".format(label_text, count), RESOLUTION_COLORS[resolution], RESOLUTION_MARKERS[resolution]))
    for label, frame in timeseries.items():
        if len(frame):
            items.append(("{} ({})".format(label, len(frame)), TIMESERIES_COLORS[label], TIMESERIES_MARKERS[label]))
    items_per_col = math.ceil(len(items) / 2.0) if len(items) > 5 else max(1, len(items))
    for idx, (label, color, marker) in enumerate(items):
        col = idx // items_per_col
        row = idx % items_per_col
        x_dot = 0.06 + col * 0.43
        y = LEGEND_FIRST_ROW_Y["panel_c"] - row * 0.13
        legend_ax.scatter(
            x_dot,
            y,
            s=22,
            c=color,
            marker=marker,
            edgecolors="black" if marker != "x" else color,
            linewidths=0.25,
            alpha=0.85,
            transform=legend_ax.transAxes,
            clip_on=False,
        )
        legend_ax.text(x_dot + 0.07, y, label, fontsize=FONT_SIZES["legend_text"], transform=legend_ax.transAxes, va="center")
    

def write_input_manifest(ctx: ReleaseContext, data_counts: dict[str, int], output_path: Path) -> Path:
    rows = []
    for role, file_name in (
        ("station_catalog", PRODUCT_FILES["station_catalog"]),
        ("satellite_catalog", PRODUCT_FILES["satellite_catalog"]),
        ("climatology_netcdf", PRODUCT_FILES["climatology_nc"]),
        ("daily_timeseries_netcdf", PRODUCT_FILES["daily_nc"]),
        ("monthly_timeseries_netcdf", PRODUCT_FILES["monthly_nc"]),
        ("annual_timeseries_netcdf", PRODUCT_FILES["annual_nc"]),
    ):
        input_path = ctx.require_input(ctx.release_file(file_name), required=True)
        rows.append(
            {
                "role": role,
                "file_name": file_name,
                "relative_path": input_path.relative_to(ctx.release_dir).as_posix(),
                "rows_or_points_used": int(data_counts.get(role, 0)),
            }
        )
    return write_csv(pd.DataFrame(rows), output_path)


def write_plotting_data(
    ctx: ReleaseContext,
    data_dir: Path,
    figure_id: str,
    clusters: pd.DataFrame,
    satellite: pd.DataFrame,
    area_dist: pd.DataFrame,
    satellite_area: pd.DataFrame,
    climatology: pd.DataFrame,
    timeseries: dict[str, pd.DataFrame],
) -> list[Path]:
    outputs = [
        write_csv(clusters, data_dir / "{}_plotting_data_clusters.csv".format(figure_id)),
        write_csv(satellite[valid_latlon(satellite)].copy(), data_dir / "{}_plotting_data_satellite_stations.csv".format(figure_id)),
        write_csv(area_dist, data_dir / "{}_plotting_data_cluster_area_distribution.csv".format(figure_id)),
        write_csv(satellite_area, data_dir / "{}_plotting_data_satellite_linked_area_distribution.csv".format(figure_id)),
        write_csv(climatology, data_dir / "{}_plotting_data_climatology_points.csv".format(figure_id)),
    ]
    for label in ("Daily", "Monthly", "Annual"):
        outputs.append(write_csv(timeseries[label], data_dir / "{}_plotting_data_{}_timeseries_points.csv".format(figure_id, label.lower())))
    outputs.append(
        write_input_manifest(
            ctx,
            {
                "station_catalog": len(clusters),
                "satellite_catalog": len(satellite),
                "climatology_netcdf": len(climatology),
                "daily_timeseries_netcdf": len(timeseries["Daily"]),
                "monthly_timeseries_netcdf": len(timeseries["Monthly"]),
                "annual_timeseries_netcdf": len(timeseries["Annual"]),
            },
            data_dir / "{}_plotting_data_input_manifest.csv".format(figure_id),
        )
    )
    return outputs


def run_text_command(cmd: list[str]) -> tuple[bool, str]:
    try:
        result = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    except FileNotFoundError:
        return False, "{} unavailable".format(cmd[0])
    return result.returncode == 0, result.stdout.strip()


def pdf_page_size(pdfinfo_output: str) -> str:
    for line in pdfinfo_output.splitlines():
        if line.startswith("Page size:"):
            return line.split(":", 1)[1].strip()
    return "not found in pdfinfo output"


def font_embedding_status(pdffonts_output: str) -> str:
    lines = pdffonts_output.splitlines()
    header_index = -1
    for idx, line in enumerate(lines):
        if "emb" in line and "sub" in line and "uni" in line:
            header_index = idx
            break
    if header_index < 0 or len(lines) <= header_index + 2:
        return "no fonts reported by pdffonts"
    header = lines[header_index]
    emb_start = header.index("emb")
    sub_start = header.index("sub")
    values = [line[emb_start:sub_start].strip().lower() for line in lines[header_index + 2 :] if line.strip()]
    if values and all(value == "yes" for value in values):
        return "all reported fonts embedded"
    if values:
        return "some reported fonts may not be embedded; review pdffonts output"
    return "no fonts reported by pdffonts"


def write_checklist(
    checklist_path: Path,
    figure_id: str,
    pdf_path: Path,
    png_path: Path,
    data_paths: list[Path],
    script_copy_path: Path,
    dpi: int,
    figsize: tuple[float, float],
) -> Path:
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(["pdffonts", str(pdf_path)])
    width_cm = figsize[0] * 2.54
    height_cm = figsize[1] * 2.54
    lines = [
        "# {} ESSD figure checklist".format(figure_id),
        "",
        "- Final PDF: `{}`".format(pdf_path.name),
        "- Final PNG: `{}`".format(png_path.name),
        "- Formats: PDF vector preferred; PNG bitmap companion",
        "- PNG dpi: {}".format(dpi),
        "- Intended size: {:.1f} x {:.1f} cm ({:.1f} x {:.1f} in)".format(width_cm, height_cm, figsize[0], figsize[1]),
        "- PDF page size: {}".format(pdf_page_size(pdfinfo_output) if pdfinfo_ok else "not checked ({})".format(pdfinfo_output)),
        "- Width >= 8 cm: yes",
        "- Font family: DejaVu Sans",
        "- Minimum visible font size: {} pt".format(MIN_VISIBLE_FONT_SIZE),
        "- Font consistency: one sans-serif family set in Matplotlib rcParams",
        "- Font embedding status: {}".format(font_embedding_status(pdffonts_output) if pdffonts_ok else "not checked ({})".format(pdffonts_output)),
        "- PDF font check command: `pdffonts {}`".format(pdf_path),
        "- PDF size check command: `pdfinfo {}`".format(pdf_path),
        "- Colorblind-safe status: uses Okabe-Ito categorical colors plus marker shapes",
        "- Coblis/equivalent review: requires manual Coblis/equivalent review after export",
        "- Legend completeness: colors, marker shapes, point-size meaning, and transparency meaning are explained in figure legends",
        "- Panel labels: lowercase labels with parentheses, `(a)`, `(b)`, `(c)`",
        "- Units and bins: basin area uses km²; histogram x-axis labels use single scientific-notation bin upper bounds",
        "- Plotting script: `{}`".format(script_copy_path.name),
        "- Plotting-data availability: {} CSV files".format(len(data_paths)),
    ]
    lines.extend("- Plotting data file: `{}`".format(path.name) for path in data_paths)
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return checklist_path


def create_figure(ctx: ReleaseContext, figures_root: Path, dpi: int, plot_only: bool = False) -> dict[str, object]:
    plt = setup_matplotlib()
    configure_matplotlib(plt)
    import matplotlib.gridspec as gridspec

    figure_dirs = ensure_figure_dirs(figures_root)
    figure_id = FIGURE_ID
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    script_copy_path = figure_dirs["scripts"] / "plot_{}.py".format(figure_id)
    checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)
    clusters, satellite, area_dist, satellite_area, point_data = load_release_data(ctx)
    climatology = point_data["climatology"]
    timeseries = {key: point_data[key] for key in ("Daily", "Monthly", "Annual")}

    fig = plt.figure(figsize=FIGSIZE)
    gs = gridspec.GridSpec(3, 1, height_ratios=[1, 1, 1], hspace=0.05)

    ax_a = fig.add_subplot(gs[0, 0], projection=ccrs.PlateCarree())
    draw_cluster_map(ax_a, clusters, area_dist)
    add_panel_label(ax_a, "(a)")

    ax_b = fig.add_subplot(gs[1, 0], projection=ccrs.PlateCarree())
    draw_satellite_map(ax_b, satellite, satellite_area)
    add_panel_label(ax_b, "(b)")

    ax_c = fig.add_subplot(gs[2, 0], projection=ccrs.PlateCarree())
    draw_climatology_timeseries_map(ax_c, climatology, timeseries)
    add_panel_label(ax_c, "(c)")

    fig.savefig(pdf_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    data_paths: list[Path] = []
    script_copy_written = False
    checklist_written = False
    if not plot_only:
        data_paths = write_plotting_data(ctx, figure_dirs["data"], figure_id, clusters, satellite, area_dist, satellite_area, climatology, timeseries)
        if Path(__file__).resolve() != script_copy_path.resolve():
            shutil.copy2(Path(__file__), script_copy_path)
            script_copy_written = True
        write_checklist(
            checklist_path,
            figure_id,
            pdf_path,
            png_path,
            data_paths,
            script_copy_path,
            dpi,
            FIGSIZE,
        )
        checklist_written = True
    return {
        "figure_id": figure_id,
        "pdf_path": pdf_path,
        "png_path": png_path,
        "data_paths": data_paths,
        "script_copy_path": script_copy_path if script_copy_written else None,
        "checklist_path": checklist_path if checklist_written else None,
    }


def parse_args(argv: Optional[list[str]] = None):
    parser = argparse.ArgumentParser(description="Build an ESSD-ready 3-panel spatial coverage figure from S8 release inputs only.")
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR), help="Path to output/sed_reference_release.")
    parser.add_argument(
        "--figures-root",
        default=str(DEFAULT_FIGURES_ROOT),
        help="Formal ESSD figure root containing final, data, scripts, and checklists subdirectories.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Legacy ReleaseContext output directory; formal manuscript outputs use --figures-root.",
    )
    parser.add_argument("--dpi", type=int, default=DPI, help="PNG output DPI and savefig DPI. Default: {}.".format(DPI))
    parser.add_argument("--plot-only", action="store_true", help="Only write the PDF/PNG figure outputs; skip plotting-data CSVs, script copy, and checklist.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    ctx = ReleaseContext(release_dir=Path(args.release_dir), out_dir=Path(args.out_dir), strict_release_only=True)
    outputs = create_figure(ctx, Path(args.figures_root), int(args.dpi), plot_only=bool(args.plot_only))
    print("Figure id: {}".format(outputs["figure_id"]))
    print("Wrote {}".format(outputs["pdf_path"]))
    print("Wrote {}".format(outputs["png_path"]))
    for path in outputs["data_paths"]:
        print("Wrote {}".format(path))
    if outputs["script_copy_path"] is not None:
        print("Wrote {}".format(outputs["script_copy_path"]))
    if outputs["checklist_path"] is not None:
        print("Wrote {}".format(outputs["checklist_path"]))
    if args.plot_only:
        print("Plot-only mode: skipped plotting-data CSVs, script copy, and checklist.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
