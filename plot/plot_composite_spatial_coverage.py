#!/usr/bin/env python3
"""Standalone 3-panel composite figure script.

Panel (a): Global bubble map + upstream area distribution inset
Panel (b): Climatology spatial coverage map
Panel (c): Satellite upstream area distribution (pre-generated PNG)
"""

import json
import math
import os
import sys
import ctypes
from pathlib import Path
from typing import Tuple

CONDA_LIB = "/share/home/dq134/.conda/envs/wzx/lib"
os.environ["LD_LIBRARY_PATH"] = CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
ctypes.CDLL(str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL)

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

try:
    import geopandas as gpd
    HAS_GPD = True
except ImportError:
    gpd = None
    HAS_GPD = False

try:
    import xarray as xr
    HAS_XARRAY = True
except ImportError:
    xr = None
    HAS_XARRAY = False


# ── Paths ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

RELEASE_DIR = PROJECT_DIR / "output/sed_reference_release"
OUTPUT_DIR = PROJECT_DIR / "output_other/spatial_coverage_stats"
TABLES_DIR = OUTPUT_DIR / "tables"
FIGURES_DIR = OUTPUT_DIR / "figures"

WORLD_BOUNDARIES = Path(
    "/share/home/dq134/.conda/envs/wzx/lib/python3.9/site-packages/"
    "pyogrio/tests/fixtures/naturalearth_lowres/naturalearth_lowres.shp"
)
COASTLINE_GEOJSON = SCRIPT_DIR / "ne_110m_coastline.geojson"

CLIMATOLOGY_NC = RELEASE_DIR / "sed_reference_climatology.nc"
CLUSTER_TABLE = TABLES_DIR / "table_cluster_spatial_attributes.csv"
AREA_TABLE = TABLES_DIR / "table_upstream_area_distribution.csv"

SATELLITE_AREA_TABLE = TABLES_DIR / "table_satellite_upstream_area_distribution_s4.csv"
SATELLITE_CATALOG = RELEASE_DIR / "satellite_catalog.csv"

RESOLUTION_FLAG_MEANINGS = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"}
RESOLUTION_MAP_COLORS = {
    "daily": "#4c78a8",
    "monthly": "#f58518",
    "annual": "#54a24b",
    "climatology": "#e45756",
    "other": "#777777",
}

SOURCE_COLORS = {
    "RiverSed": "#377eb8",
    "GSED": "#e41a1c",
    "Dethier": "#4daf4a",
    "Unknown": "#777777",
}


BUBBLE_COLORS = {
    "resolved":   "#2196F3",
    "unresolved": "#FF9800",
    "unknown":    "#9E9E9E",
}


# ── Helpers ────────────────────────────────────────────────────────────

def read_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError("Required input not found: {}".format(path))
    return pd.read_csv(path, keep_default_na=False)


def numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def valid_latlon(df: pd.DataFrame, lat_col: str = "lat", lon_col: str = "lon") -> pd.Series:
    lat = numeric(df, lat_col)
    lon = numeric(df, lon_col)
    return lat.between(-90, 90) & lon.between(-180, 180)


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def draw_background(ax: plt.Axes) -> None:
    if WORLD_BOUNDARIES and HAS_GPD and WORLD_BOUNDARIES.is_file():
        try:
            world = gpd.read_file(WORLD_BOUNDARIES)
            world = world.set_crs("EPSG:4326") if world.crs is None else world.to_crs("EPSG:4326")
            world.boundary.plot(ax=ax, linewidth=0.35, color="#888888", zorder=1)
            return
        except Exception as exc:
            print("Warning: failed to draw WORLD_BOUNDARIES: {}".format(exc), file=sys.stderr)
    if COASTLINE_GEOJSON.is_file():
        try:
            with COASTLINE_GEOJSON.open(encoding="utf-8") as fh:
                coast = json.load(fh)
            for feat in coast.get("features", []):
                geom = feat.get("geometry", {})
                coords = geom.get("coordinates", [])
                segments = coords if geom.get("type") == "MultiLineString" else [coords]
                for seg in segments:
                    if len(seg) < 2:
                        continue
                    xs, ys = zip(*seg)
                    ax.plot(xs, ys, color="#777777", linewidth=0.45, zorder=1)
        except Exception as exc:
            print("Warning: failed to draw cached coastline: {}".format(exc), file=sys.stderr)


def save_figure(fig: plt.Figure, stem: str) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in (".png", ".pdf"):
        fig.savefig(FIGURES_DIR / "{}{}".format(stem, suffix), dpi=300, bbox_inches="tight")
    plt.close(fig)


# ── Panel helpers ──────────────────────────────────────────────────────

def draw_bubble_map(ax: plt.Axes, clusters: pd.DataFrame) -> None:
    """Draw global bubble map on *ax* (adapted from plot_global_bubble_map.py)."""
    df = clusters[valid_latlon(clusters)].copy()
    has_area = numeric(df, "area_km2").notna() & (numeric(df, "area_km2") > 0)

    draw_background(ax)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linewidth=0.3, alpha=0.35, zorder=0)

    sizes = np.where(
        has_area,
        np.sqrt(np.clip(numeric(df, "area_km2"), 10, None)) * 0.18,
        4.0,
    )

    for status in ["resolved", "unresolved", "unknown"]:
        if status != "unknown":
            mask = df["basin_status"] == status
        else:
            mask = ~df["basin_status"].isin(["resolved", "unresolved"])
        if not mask.any():
            continue
        ax.scatter(
            df.loc[mask, "lon"],
            df.loc[mask, "lat"],
            s=sizes[mask.values],
            c=BUBBLE_COLORS.get(status, "#777777"),
            alpha=0.5,
            label=status,
            edgecolors="none",
            zorder=2,
        )

    ax.legend(loc="lower left", markerscale=1.8, framealpha=0.85,
              title="Basin status", fontsize=8)

    n_total = len(df)
    n_resolved = int((df["basin_status"] == "resolved").sum())
    n_unresolved = int((df["basin_status"] == "unresolved").sum())
    n_unknown = n_total - n_resolved - n_unresolved
    stats_lines = [
        "Total clusters:  {}".format(n_total),
        "Resolved:        {}".format(n_resolved),
        "Unresolved:     {}".format(n_unresolved),
    ]
    if n_unknown > 0:
        stats_lines.append("Unknown:         {}".format(n_unknown))

    ax.text(0.98, 0.02, "\n".join(stats_lines),
            transform=ax.transAxes, fontsize=8,
            verticalalignment="bottom", horizontalalignment="right",
            bbox=dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.85))


def draw_upstream_inset(ax: plt.Axes, area_dist: pd.DataFrame) -> None:
    """Draw upstream area distribution bar chart on *ax* with scientific x-axis."""
    bins = area_dist[area_dist["section"].eq("bin")].copy()
    bins["cluster_count"] = numeric(bins, "cluster_count")

    # Convert bin labels to numeric upper-bound values
    bin_vals = []
    for label in bins["label"]:
        clean = label.replace(",", "").replace(" km2", "")
        if "<" in clean:
            val = float(clean.replace("<", ""))
        elif ">" in clean:
            val = float(clean.replace(">", ""))
        elif "-" in clean:
            val = float(clean.split("-")[1])
        else:
            val = float(clean)
        bin_vals.append(val)

    x_pos = np.arange(len(bins))
    ax.bar(x_pos, bins["cluster_count"], color="#6f9eaf")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(
        ["{:.0e}".format(v).replace("e+0", "e").replace("e+", "e").replace("e0", "e")
         for v in bin_vals]
    )
    ax.tick_params(axis="both", labelsize=6)
    ax.set_ylabel("Cluster count", fontsize=6)
    ax.grid(axis="y", linewidth=0.3, alpha=0.55)
    ax.set_title("Upstream area distribution", fontsize=8)


def draw_climatology_map(ax: plt.Axes) -> None:
    """Map of climatology stations colored by record temporal resolution."""
    if not HAS_XARRAY or not CLIMATOLOGY_NC.is_file():
        ax.text(0.5, 0.5, "Climatology data unavailable",
                transform=ax.transAxes, ha="center", va="center", fontsize=10, color="#888888")
        return

    ds = xr.open_dataset(str(CLIMATOLOGY_NC))
    lat = ds["lat"].values
    lon = ds["lon"].values
    res_flags = ds["resolution"].values
    res_labels = [RESOLUTION_FLAG_MEANINGS.get(int(f), "other") for f in res_flags]

    draw_background(ax)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linewidth=0.3, color="#cccccc", alpha=0.6)

    for res_type in ["daily", "monthly", "annual", "climatology", "other"]:
        mask = [r == res_type for r in res_labels]
        if not any(mask):
            continue
        lats = lat[mask]
        lons = lon[mask]
        color = RESOLUTION_MAP_COLORS.get(res_type, "#777777")
        marker = "o" if res_type != "climatology" else "s"
        size = 12 if res_type == "climatology" else 8
        ax.scatter(lons, lats, s=size, c=color, alpha=0.55,
                   linewidths=0, marker=marker,
                   label="{} ({})".format(res_type, sum(mask)),
                   zorder=3)
    ax.legend(loc="lower left", fontsize=8, frameon=True, markerscale=1.5)
    ds.close()


def draw_satellite_validation_map(ax: plt.Axes, satellite: pd.DataFrame) -> None:
    """Draw satellite validation station locations on *ax* colored by source."""
    df = satellite[valid_latlon(satellite)].copy()
    if df.empty:
        ax.text(0.5, 0.5, "No satellite validation data",
                transform=ax.transAxes, ha="center", va="center", fontsize=10, color="#888888")
        return

    draw_background(ax)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linewidth=0.3, color="#cccccc", alpha=0.6)

    for source, group in df.groupby("source", dropna=False):
        src = clean_text(source) or "Unknown"
        color = SOURCE_COLORS.get(src, "#777777")
        ax.scatter(
            numeric(group, "lon"),
            numeric(group, "lat"),
            s=5, c=color, alpha=0.38,
            linewidths=0,
            label="{} ({})".format(src, len(group)),
            zorder=3,
        )
    ax.legend(loc="lower left", fontsize=8, frameon=True, markerscale=2.3)


def draw_satellite_upstream_inset(ax: plt.Axes, area_table: pd.DataFrame) -> None:
    """Draw satellite upstream area distribution bar chart on *ax* with scientific x-axis."""
    if area_table.empty:
        ax.text(0.5, 0.5, "Satellite area data not found",
                transform=ax.transAxes, ha="center", va="center", fontsize=8, color="#888888")
        return

    bins = area_table[area_table["section"].eq("bin")].copy()
    if "count" in bins.columns:
        bins["cluster_count"] = numeric(bins, "count")
    elif "cluster_count" in bins.columns:
        bins["cluster_count"] = numeric(bins, "cluster_count")
    else:
        ax.text(0.5, 0.5, "No count column",
                transform=ax.transAxes, ha="center", va="center", fontsize=8, color="#888888")
        return

    # Use bin_right_km2 as x position for each bar
    bin_rights = bins["bin_right_km2"].values.astype(float)
    x_pos = np.arange(len(bins))
    ax.bar(x_pos, bins["cluster_count"], color="#e45756")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(
        ["{:.0e}".format(v).replace("e+0", "e").replace("e+", "e").replace("e0", "e")
         for v in bin_rights]
    )
    ax.tick_params(axis="both", labelsize=6)
    ax.set_ylabel("Station count", fontsize=6)
    ax.grid(axis="y", linewidth=0.3, alpha=0.55)
    ax.set_title("Satellite upstream area", fontsize=8)


# ── Composite figure ───────────────────────────────────────────────────

def main() -> int:
    print("Reading input data ...")
    clusters = read_csv(CLUSTER_TABLE)
    area_dist = read_csv(AREA_TABLE)
    try:
        satellite = read_csv(SATELLITE_CATALOG)
    except FileNotFoundError:
        print("Warning: satellite catalog not found at {}".format(SATELLITE_CATALOG))
        satellite = pd.DataFrame()
    try:
        satellite_area = read_csv(SATELLITE_AREA_TABLE)
    except FileNotFoundError:
        print("Warning: satellite area table not found at {}".format(SATELLITE_AREA_TABLE))
        satellite_area = pd.DataFrame()

    print("Creating composite figure ...")
    fig = plt.figure(figsize=(11, 14))
    gs = gridspec.GridSpec(3, 1, height_ratios=[1, 1, 1], hspace=0.25)

    # ── Panel (a): Global bubble map + upstream area inset ──────────
    ax_a = fig.add_subplot(gs[0])
    draw_bubble_map(ax_a, clusters)
    ax_a.text(0.01, 0.97, "(a)", transform=ax_a.transAxes, fontsize=14, fontweight="bold",
              va="top", ha="left", bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    from mpl_toolkits.axes_grid1.inset_locator import inset_axes
    ax_inset = inset_axes(ax_a, width="32%", height="30%", loc="lower right",
                          bbox_to_anchor=(0, 0, 1, 1), bbox_transform=ax_a.transAxes)
    draw_upstream_inset(ax_inset, area_dist)

    # ── Panel (b): Climatology spatial coverage ────────────────────
    ax_b = fig.add_subplot(gs[1])
    draw_climatology_map(ax_b)
    ax_b.text(0.01, 0.97, "(b)", transform=ax_b.transAxes, fontsize=14, fontweight="bold",
              va="top", ha="left", bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    # ── Panel (c): Satellite validation map + upstream area inset ──────
    ax_c = fig.add_subplot(gs[2])
    draw_satellite_validation_map(ax_c, satellite)
    ax_c.text(0.01, 0.97, "(c)", transform=ax_c.transAxes, fontsize=14, fontweight="bold",
              va="top", ha="left", bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    # Inset: satellite upstream area distribution
    from mpl_toolkits.axes_grid1.inset_locator import inset_axes
    ax_c_inset = inset_axes(ax_c, width="32%", height="30%", loc="lower right",
                            bbox_to_anchor=(0, 0, 1, 1), bbox_transform=ax_c.transAxes)
    draw_satellite_upstream_inset(ax_c_inset, satellite_area)

    print("Saving composite figure ...")
    save_figure(fig, "fig_composite_spatial_coverage")
    print("Done -- wrote fig_composite_spatial_coverage.png and .pdf to {}".format(FIGURES_DIR))
    return 0


if __name__ == "__main__":
    sys.exit(main())
