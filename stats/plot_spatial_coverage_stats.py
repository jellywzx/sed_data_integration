#!/usr/bin/env python3
"""Plot S8 release-level spatial coverage figures from precomputed tables.

This script is intentionally fixed-configuration. Run spatial_coverage_stats.py
first, then run this script on node113 to write figures under
output_other/spatial_coverage_stats/figures.
"""

import json
import math
import os
import socket
import sys
import ctypes
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

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
except ImportError:  # pragma: no cover - optional runtime dependency
    gpd = None
    HAS_GPD = False

try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:  # pragma: no cover - optional runtime dependency
    xr = None
    HAS_XARRAY = False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

RELEASE_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
    "scripts_basin_test/output/sed_reference_release"
)
OUTPUT_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
    "scripts_basin_test/output_other/spatial_coverage_stats"
)
TABLES_DIR = OUTPUT_DIR / "tables"
FIGURES_DIR = OUTPUT_DIR / "figures"
WORLD_BOUNDARIES = (
    "/share/home/dq134/.conda/envs/wzx/lib/python3.9/site-packages/"
    "pyogrio/tests/fixtures/naturalearth_lowres/naturalearth_lowres.shp"
)

SATELLITE_CATALOG = RELEASE_DIR / "satellite_catalog.csv"
CLUSTER_BASINS_GPKG = RELEASE_DIR / "sed_reference_cluster_basins.gpkg"
COASTLINE_GEOJSON = PROJECT_DIR / "plot/ne_110m_coastline.geojson"

CLIMATOLOGY_NC = RELEASE_DIR / "sed_reference_climatology.nc"
TIMESERIES_DAILY_NC = RELEASE_DIR / "sed_reference_timeseries_daily.nc"
TIMESERIES_MONTHLY_NC = RELEASE_DIR / "sed_reference_timeseries_monthly.nc"
TIMESERIES_ANNUAL_NC = RELEASE_DIR / "sed_reference_timeseries_annual.nc"

RESOLUTION_FLAG_MEANINGS = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"}
RESOLUTION_MAP_COLORS = {
    "daily": "#4c78a8",
    "monthly": "#f58518",
    "annual": "#54a24b",
    "climatology": "#e45756",
    "other": "#777777",
}

REQUIRED_HOST = "node113"
PYTHON = "/share/home/dq134/.conda/envs/wzx/bin/python3"
RUN_HINT = (
    "ssh node113 'cd /share/home/dq134/wzx/sed_data/sediment_wzx_1111/"
    "Output_r/scripts_basin_test && {py} stats/spatial_coverage_stats.py && "
    "{py} stats/plot_spatial_coverage_stats.py'"
).format(
    py=PYTHON
)

RESOLUTION_ORDER = ("daily", "monthly", "annual")
STATUS_COLORS = {
    "resolved": "#1b9e77",
    "unresolved": "#d95f02",
    "unknown": "#7570b3",
}
SOURCE_COLORS = {
    "RiverSed": "#377eb8",
    "GSED": "#e41a1c",
    "Dethier": "#4daf4a",
    "Unknown": "#777777",
}
SOURCE_STACK_TOP_N = 10
OTHER_SOURCE_LABEL = "Other sources"
RESOLUTION_COLORS = {
    "daily": "#4c78a8",
    "monthly": "#f58518",
    "annual": "#54a24b",
    "unknown": "#777777",
}


def require_node113() -> None:
    host = socket.gethostname().split(".")[0]
    if host != REQUIRED_HOST:
        raise SystemExit(
            "This spatial plotting script must run on node113, not {}.\n"
            "Use:\n  {}".format(host, RUN_HINT)
        )


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


def read_csv(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.is_file():
        if required:
            raise FileNotFoundError("Required input not found: {}".format(path))
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def valid_latlon(df: pd.DataFrame, lat_col: str = "lat", lon_col: str = "lon") -> pd.Series:
    lat = numeric(df, lat_col)
    lon = numeric(df, lon_col)
    return lat.between(-90, 90) & lon.between(-180, 180)


def save_figure(fig: plt.Figure, stem: str) -> None:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    for suffix in (".png", ".pdf"):
        fig.savefig(FIGURES_DIR / "{}{}".format(stem, suffix), dpi=300, bbox_inches="tight")
    plt.close(fig)


def setup_map_ax(title: str) -> Tuple[plt.Figure, plt.Axes]:
    fig, ax = plt.subplots(figsize=(11, 5.7))
    draw_background(ax)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(title)
    ax.grid(True, linewidth=0.3, color="#cccccc", alpha=0.6)
    return fig, ax


def draw_background(ax: plt.Axes) -> None:
    if WORLD_BOUNDARIES and HAS_GPD and Path(WORLD_BOUNDARIES).is_file():
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


def marker_sizes(area: pd.Series, minimum: float = 8.0, maximum: float = 45.0) -> np.ndarray:
    values = pd.to_numeric(area, errors="coerce")
    values = values.where(np.isfinite(values) & (values > 0), np.nan)
    if values.notna().sum() == 0:
        return np.full(len(values), minimum)
    logged = np.log10(values.fillna(values.median()))
    lo = float(logged.quantile(0.05))
    hi = float(logged.quantile(0.95))
    if not math.isfinite(lo) or not math.isfinite(hi) or hi <= lo:
        return np.full(len(values), (minimum + maximum) / 2.0)
    scaled = (logged.clip(lo, hi) - lo) / (hi - lo)
    return minimum + scaled.to_numpy() * (maximum - minimum)


def fig_global_cluster_distribution(clusters: pd.DataFrame) -> None:
    df = clusters[valid_latlon(clusters)].copy()
    fig, ax = setup_map_ax("S8 main-product cluster distribution")
    for status, group in df.groupby("basin_status", dropna=False):
        label = clean_text(status) or "unknown"
        color = STATUS_COLORS.get(label, "#666666")
        ax.scatter(
            numeric(group, "lon"),
            numeric(group, "lat"),
            s=marker_sizes(group["area_km2"], 7, 34),
            c=color,
            alpha=0.62,
            linewidths=0,
            label="{} ({})".format(label, len(group)),
            zorder=3,
        )
    ax.legend(loc="lower left", fontsize=8, frameon=True, markerscale=1.4)
    save_figure(fig, "fig_global_cluster_distribution")


def read_basin_layers() -> List[object]:
    if not HAS_GPD or not CLUSTER_BASINS_GPKG.is_file():
        return []
    layers = ["basin_annual", "basin_daily", "basin_monthly"]
    out = []
    for layer in layers:
        try:
            gdf = gpd.read_file(CLUSTER_BASINS_GPKG, layer=layer)
            if len(gdf):
                out.append(gdf)
        except Exception as exc:
            print("Warning: failed to read {} from {}: {}".format(layer, CLUSTER_BASINS_GPKG, exc), file=sys.stderr)
    return out


def fig_global_cluster_status_and_basins(clusters: pd.DataFrame) -> None:
    df = clusters[valid_latlon(clusters)].copy()
    fig, ax = setup_map_ax("S8 basin assignment and published basin polygons")
    for gdf in read_basin_layers():
        try:
            gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
            gdf.boundary.plot(ax=ax, linewidth=0.18, color="#3b6ea8", alpha=0.35, zorder=2)
        except Exception as exc:
            print("Warning: failed to plot basin polygons: {}".format(exc), file=sys.stderr)
    for status in ("resolved", "unresolved", "unknown"):
        group = df[df["basin_status"].eq(status)] if status != "unknown" else df[~df["basin_status"].isin(["resolved", "unresolved"])]
        if group.empty:
            continue
        ax.scatter(
            numeric(group, "lon"),
            numeric(group, "lat"),
            s=10,
            c=STATUS_COLORS.get(status, "#666666"),
            alpha=0.72,
            linewidths=0,
            label="{} ({})".format(status, len(group)),
            zorder=4,
        )
    ax.legend(loc="lower left", fontsize=8, frameon=True, markerscale=1.8)
    save_figure(fig, "fig_global_cluster_status_and_basins")


def fig_spatial_coverage_by_resolution(by_resolution: pd.DataFrame) -> None:
    df = by_resolution.copy()
    df["resolution"] = pd.Categorical(df["resolution"], categories=RESOLUTION_ORDER, ordered=True)
    df = df.sort_values("resolution")
    x = np.arange(len(df))
    width = 0.26
    fig, ax = plt.subplots(figsize=(7.6, 4.8))
    ax.bar(x - width, numeric(df, "cluster_count"), width=width, label="clusters", color="#4c78a8")
    ax.bar(x, numeric(df, "resolved_cluster_count"), width=width, label="resolved", color="#54a24b")
    ax.bar(x + width, numeric(df, "basin_polygon_cluster_count"), width=width, label="basin polygons", color="#f58518")
    ax.set_xticks(x)
    ax.set_xticklabels(df["resolution"])
    ax.set_ylabel("Cluster count")
    ax.set_title("Spatial coverage by temporal resolution")
    ax.legend(frameon=False)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    save_figure(fig, "fig_spatial_coverage_by_resolution")


def _top_coverage_rows(df: pd.DataFrame, label_col: str, top_n: int) -> pd.DataFrame:
    out = df.copy()
    out["cluster_count"] = numeric(out, "cluster_count").fillna(0).astype(int)
    out[label_col] = out[label_col].map(lambda x: clean_text(x) or "Unknown")
    out = out.sort_values(["cluster_count", label_col], ascending=[False, True]).head(top_n)
    return out.sort_values("cluster_count", ascending=True)


def _add_bar_labels(ax: plt.Axes, values: Sequence[int]) -> None:
    xmax = max(values) if len(values) else 0
    offset = max(xmax * 0.012, 3)
    for i, value in enumerate(values):
        ax.text(value + offset, i, "{:,}".format(int(value)), va="center", ha="left", fontsize=8)
    ax.set_xlim(0, xmax * 1.16 if xmax else 1)


def fig_spatial_coverage_by_region_country(by_region: pd.DataFrame, by_country: pd.DataFrame) -> None:
    regions = _top_coverage_rows(by_region, "region", 10)
    countries = _top_coverage_rows(by_country, "country", 12)

    fig, axes = plt.subplots(1, 2, figsize=(12.2, 5.4))
    panels = [
        (axes[0], regions, "region", "Top regions by cluster count", "#4c78a8"),
        (axes[1], countries, "country", "Top countries by cluster count", "#59a14f"),
    ]
    for ax, data, label_col, title, color in panels:
        labels = data[label_col].astype(str).tolist()
        values = data["cluster_count"].astype(int).tolist()
        ax.barh(labels, values, color=color)
        ax.set_xlabel("Cluster count")
        ax.set_title(title)
        ax.grid(axis="x", linewidth=0.3, alpha=0.55)
        _add_bar_labels(ax, values)

    fig.suptitle("Regional and national spatial coverage of S8 main-product clusters", y=1.02)
    fig.tight_layout()
    save_figure(fig, "fig_spatial_coverage_by_region_country")

    # Compatibility name from the original manuscript plan.
    fig, axes = plt.subplots(1, 2, figsize=(12.2, 5.4))
    for ax, data, label_col, title, color in panels:
        labels = data[label_col].astype(str).tolist()
        values = data["cluster_count"].astype(int).tolist()
        ax.barh(labels, values, color=color)
        ax.set_xlabel("Cluster count")
        ax.set_title(title)
        ax.grid(axis="x", linewidth=0.3, alpha=0.55)
        _add_bar_labels(ax, values)
    fig.suptitle("Regional and national spatial coverage of S8 main-product clusters", y=1.02)
    fig.tight_layout()
    save_figure(fig, "fig_spatial_coverage_by_region")


def _ordered_region_labels(df: pd.DataFrame, value_col: str = "cluster_count") -> List[str]:
    tmp = df.copy()
    tmp[value_col] = numeric(tmp, value_col).fillna(0)
    tmp["region"] = tmp["region"].map(lambda x: clean_text(x) or "Unknown")
    totals = tmp.groupby("region", dropna=False)[value_col].sum().reset_index()
    totals["_unknown"] = totals["region"].eq("Unknown").astype(int)
    totals = totals.sort_values(["_unknown", value_col, "region"], ascending=[True, False, True])
    return totals["region"].tolist()


def _collapse_minor_sources(df: pd.DataFrame, value_col: str, top_n: int = SOURCE_STACK_TOP_N) -> pd.DataFrame:
    out = df.copy()
    out["source_name"] = out["source_name"].map(lambda x: clean_text(x) or "Unknown")
    out[value_col] = numeric(out, value_col).fillna(0)
    top = (
        out.groupby("source_name", dropna=False)[value_col]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .index
        .tolist()
    )
    out["source_group"] = out["source_name"].where(out["source_name"].isin(top), OTHER_SOURCE_LABEL)
    grouped = (
        out.groupby(["region", "source_group"], dropna=False)[value_col]
        .sum()
        .reset_index()
        .rename(columns={"source_group": "source_name"})
    )
    return grouped


def _stacked_bar(ax: plt.Axes, pivot: pd.DataFrame, colors: Sequence[str]) -> None:
    bottoms = np.zeros(len(pivot), dtype=float)
    x = np.arange(len(pivot.index))
    for i, col in enumerate(pivot.columns):
        values = pivot[col].to_numpy(dtype=float)
        ax.bar(x, values, bottom=bottoms, label=col, color=colors[i % len(colors)], width=0.68)
        bottoms += values
    ax.set_xticks(x)
    ax.set_xticklabels(pivot.index, rotation=25, ha="right")
    ax.grid(axis="y", linewidth=0.3, alpha=0.55)


def fig_spatial_coverage_by_region_source_clusters(by_region_source: pd.DataFrame) -> None:
    df = _collapse_minor_sources(by_region_source, "cluster_count")
    region_order = _ordered_region_labels(by_region_source, "cluster_count")
    source_order = (
        df.groupby("source_name")["cluster_count"]
        .sum()
        .sort_values(ascending=False)
        .index
        .tolist()
    )
    if OTHER_SOURCE_LABEL in source_order:
        source_order = [s for s in source_order if s != OTHER_SOURCE_LABEL] + [OTHER_SOURCE_LABEL]
    pivot = (
        df.pivot_table(index="region", columns="source_name", values="cluster_count", aggfunc="sum", fill_value=0)
        .reindex(region_order)
        .fillna(0)
    )
    pivot = pivot[[s for s in source_order if s in pivot.columns]]
    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    colors = plt.get_cmap("tab20").colors
    _stacked_bar(ax, pivot, colors)
    ax.set_ylabel("Cluster count")
    ax.set_title("Regional source contributions by cluster count")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8, frameon=False)
    fig.tight_layout()
    save_figure(fig, "fig_spatial_coverage_by_region_source_clusters")


def fig_spatial_coverage_by_region_source_records(by_region_source: pd.DataFrame) -> None:
    df = _collapse_minor_sources(by_region_source, "record_count")
    region_order = _ordered_region_labels(by_region_source, "record_count")
    source_order = (
        df.groupby("source_name")["record_count"]
        .sum()
        .sort_values(ascending=False)
        .index
        .tolist()
    )
    if OTHER_SOURCE_LABEL in source_order:
        source_order = [s for s in source_order if s != OTHER_SOURCE_LABEL] + [OTHER_SOURCE_LABEL]
    pivot = (
        df.pivot_table(index="region", columns="source_name", values="record_count", aggfunc="sum", fill_value=0)
        .reindex(region_order)
        .fillna(0)
    )
    pivot = pivot[[s for s in source_order if s in pivot.columns]]
    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    colors = plt.get_cmap("tab20").colors
    _stacked_bar(ax, pivot, colors)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1)
    ax.set_ylabel("Record count (log scale)")
    ax.set_title("Regional source contributions by record count")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8, frameon=False)
    fig.tight_layout()
    save_figure(fig, "fig_spatial_coverage_by_region_source_records")


def fig_spatial_coverage_by_region_resolution(by_region_resolution: pd.DataFrame) -> None:
    df = by_region_resolution.copy()
    df["region"] = df["region"].map(lambda x: clean_text(x) or "Unknown")
    df["resolution"] = df["resolution"].map(lambda x: clean_text(x).lower() or "unknown")
    df["cluster_count"] = numeric(df, "cluster_count").fillna(0)
    region_order = _ordered_region_labels(df, "cluster_count")
    resolution_order = [r for r in list(RESOLUTION_ORDER) + ["unknown"] if r in set(df["resolution"])]
    pivot = (
        df.pivot_table(index="region", columns="resolution", values="cluster_count", aggfunc="sum", fill_value=0)
        .reindex(region_order)
        .fillna(0)
    )
    pivot = pivot[[r for r in resolution_order if r in pivot.columns]]
    fig, ax = plt.subplots(figsize=(8.8, 5.1))
    colors = [RESOLUTION_COLORS.get(col, "#777777") for col in pivot.columns]
    _stacked_bar(ax, pivot, colors)
    ax.set_ylabel("Cluster count")
    ax.set_title("Regional spatial coverage by temporal resolution")
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1), fontsize=8, frameon=False)
    fig.tight_layout()
    save_figure(fig, "fig_spatial_coverage_by_region_resolution")


def fig_upstream_area_distribution(area_dist: pd.DataFrame, ax: plt.Axes = None, scientific_notation: bool = False) -> None:
    bins = area_dist[area_dist["section"].eq("bin")].copy()
    bins["cluster_count"] = numeric(bins, "cluster_count")
    if ax is None:
        fig, ax = plt.subplots(figsize=(8.4, 4.9))
        standalone = True
    else:
        standalone = False

    if scientific_notation:
        # Extract numeric upper bounds from bin labels for scientific x-axis labels
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
        ax.set_xticklabels(["{:.0e}".format(v).replace("e+0", "e").replace("e+", "e").replace("e0", "e") for v in bin_vals])
    else:
        ax.bar(bins["label"], bins["cluster_count"], color="#6f9eaf")
        ax.tick_params(axis="x", rotation=25)
        for tick in ax.get_xticklabels():
            tick.set_ha("right")

    if standalone:
        ax.set_ylabel("Cluster count")
        ax.set_xlabel("Upstream basin area")
        ax.set_title("Distribution of upstream basin area")
    else:
        ax.tick_params(axis="both", labelsize=6)
        ax.set_ylabel("Cluster count", fontsize=6)
    ax.grid(axis="y", linewidth=0.3, alpha=0.55)

    if standalone:
        save_figure(fig, "fig_upstream_area_distribution")


def fig_source_spatial_contribution(by_source: pd.DataFrame) -> None:
    df = by_source.head(12).copy().sort_values("cluster_count", ascending=True)
    fig, ax = plt.subplots(figsize=(8.4, 5.5))
    ax.barh(df["source_name"], numeric(df, "cluster_count"), color="#4c78a8", label="clusters")
    ax.set_xlabel("Cluster count")
    ax.set_title("Top source datasets by spatial contribution")
    ax.grid(axis="x", linewidth=0.3, alpha=0.55)
    ax2 = ax.twiny()
    ax2.plot(numeric(df, "record_count"), df["source_name"], color="#d95f02", marker="o", linewidth=1.2, label="records")
    ax2.set_xlabel("Record count")
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc="lower right", frameon=False)
    save_figure(fig, "fig_source_spatial_contribution")


def fig_satellite_validation_spatial_distribution(satellite: pd.DataFrame, ax: plt.Axes = None) -> None:
    df = satellite[valid_latlon(satellite)].copy()
    if "source" not in df.columns:
        df["source"] = "Unknown"
    if ax is None:
        fig, ax = setup_map_ax("Satellite-validation station distribution")
        standalone = True
    else:
        standalone = False
        draw_background(ax)
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 85)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.grid(True, linewidth=0.3, color="#cccccc", alpha=0.6)

    for source, group in df.groupby("source", dropna=False):
        source = clean_text(source) or "Unknown"
        color = SOURCE_COLORS.get(source, "#777777")
        ax.scatter(
            numeric(group, "lon"),
            numeric(group, "lat"),
            s=5,
            c=color,
            alpha=0.38,
            linewidths=0,
            label="{} ({})".format(source, len(group)),
            zorder=3,
        )
    ax.legend(loc="lower left", fontsize=8, frameon=True, markerscale=2.3)
    if standalone:
        save_figure(fig, "fig_satellite_validation_spatial_distribution")


def fig_main_vs_satellite_spatial_coverage(clusters: pd.DataFrame, satellite: pd.DataFrame) -> None:
    main = clusters[valid_latlon(clusters)].copy()
    sat = satellite[valid_latlon(satellite)].copy()
    fig, ax = setup_map_ax("Main-product clusters and satellite-validation stations")
    ax.scatter(
        numeric(main, "lon"),
        numeric(main, "lat"),
        s=10,
        c="#303030",
        alpha=0.55,
        linewidths=0,
        label="main clusters ({})".format(len(main)),
        zorder=3,
    )
    ax.scatter(
        numeric(sat, "lon"),
        numeric(sat, "lat"),
        s=4,
        c="#d95f02",
        alpha=0.25,
        linewidths=0,
        label="satellite validation ({})".format(len(sat)),
        zorder=2,
    )
    ax.legend(loc="lower left", fontsize=8, frameon=True, markerscale=2.0)
    save_figure(fig, "fig_main_vs_satellite_spatial_coverage")



def fig_climatology_spatial_coverage(ax: plt.Axes = None) -> None:
    """Map of climatology stations colored by record temporal resolution."""
    if not HAS_XARRAY or not CLIMATOLOGY_NC.is_file():
        print('Warning: xarray unavailable or climatology NC not found; skipping climatology map')
        return
    ds = xr.open_dataset(str(CLIMATOLOGY_NC))
    lat = ds['lat'].values
    lon = ds['lon'].values
    res_flags = ds['resolution'].values
    res_labels = [RESOLUTION_FLAG_MEANINGS.get(int(f), 'other') for f in res_flags]

    if ax is None:
        fig, ax = setup_map_ax('Climatology stations by record temporal resolution')
        standalone = True
    else:
        standalone = False
        draw_background(ax)
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 85)
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.grid(True, linewidth=0.3, color='#cccccc', alpha=0.6)

    for res_type in ['daily', 'monthly', 'annual', 'climatology', 'other']:
        mask = [r == res_type for r in res_labels]
        if not any(mask):
            continue
        lats = lat[mask]
        lons = lon[mask]
        color = RESOLUTION_MAP_COLORS.get(res_type, '#777777')
        marker = 'o' if res_type != 'climatology' else 's'
        size = 12 if res_type == 'climatology' else 8
        ax.scatter(
            lons, lats,
            s=size, c=color, alpha=0.55,
            linewidths=0, marker=marker,
            label='{} ({})'.format(res_type, sum(mask)),
            zorder=3,
        )
    ax.legend(loc='lower left', fontsize=8, frameon=True, markerscale=1.5)
    ds.close()
    if standalone:
        save_figure(fig, 'fig_climatology_spatial_coverage')


def fig_timeseries_spatial_coverage() -> None:
    """Three-panel map of stations at each temporal resolution (daily, monthly, annual)."""
    if not HAS_XARRAY:
        print('Warning: xarray unavailable; skipping timeseries spatial coverage map')
        return

    nc_files = [
        ('daily', TIMESERIES_DAILY_NC),
        ('monthly', TIMESERIES_MONTHLY_NC),
        ('annual', TIMESERIES_ANNUAL_NC),
    ]
    datasets = []
    for label, path in nc_files:
        if not path.is_file():
            print('Warning: {} not found; skipping'.format(path))
            continue
        ds = xr.open_dataset(str(path))
        datasets.append((label, ds['lat'].values, ds['lon'].values, ds))

    n = len(datasets)
    if n == 0:
        print('Warning: no timeseries NetCDF files found')
        return

    fig, axes = plt.subplots(1, n, figsize=(5.5 * n, 4.5))
    if n == 1:
        axes = [axes]
    for ax, (label, lats, lons, ds) in zip(axes, datasets):
        draw_background(ax)
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 85)
        ax.set_xlabel('Longitude')
        ax.set_ylabel('Latitude')
        ax.set_title('{} stations (n={})'.format(label.capitalize(), len(lats)))
        ax.grid(True, linewidth=0.3, color='#cccccc', alpha=0.6)
        color = RESOLUTION_MAP_COLORS.get(label, '#4c78a8')
        ax.scatter(
            lons, lats,
            s=8, c=color, alpha=0.5,
            linewidths=0, zorder=3,
        )
        ds.close()

    fig.suptitle('S8 stations by temporal resolution', y=1.02, fontsize=13)
    fig.tight_layout()
    save_figure(fig, 'fig_timeseries_spatial_coverage')


def fig_climatology_vs_timeseries_coverage() -> None:
    """Overlay map showing climatology vs all timeseries stations combined."""
    if not HAS_XARRAY:
        print('Warning: xarray unavailable; skipping combined coverage map')
        return

    ts_lats = []
    ts_lons = []
    for path in [TIMESERIES_DAILY_NC, TIMESERIES_MONTHLY_NC, TIMESERIES_ANNUAL_NC]:
        if path.is_file():
            ds = xr.open_dataset(str(path))
            ts_lats.extend(ds['lat'].values.tolist())
            ts_lons.extend(ds['lon'].values.tolist())
            ds.close()

    has_clim = CLIMATOLOGY_NC.is_file()
    if has_clim:
        ds = xr.open_dataset(str(CLIMATOLOGY_NC))
        clim_lats = ds['lat'].values
        clim_lons = ds['lon'].values
        ds.close()
    else:
        clim_lats, clim_lons = [], []

    if not ts_lats and not clim_lats:
        print('Warning: no data for combined coverage map')
        return

    fig, ax = setup_map_ax('Climatology stations and timeseries stations')
    if ts_lats:
        ax.scatter(
            ts_lons, ts_lats,
            s=6, c='#4c78a8', alpha=0.35,
            linewidths=0, label='timeseries (n={})'.format(len(ts_lats)),
            zorder=2,
        )
    if len(clim_lats):
        ax.scatter(
            clim_lons, clim_lats,
            s=14, c='#e45756', alpha=0.6,
            linewidths=0, marker='s',
            label='climatology (n={})'.format(len(clim_lats)),
            zorder=3,
        )
    ax.legend(loc='lower left', fontsize=8, frameon=True, markerscale=1.5)
    save_figure(fig, 'fig_climatology_vs_timeseries_coverage')




def draw_bubble_map(ax: plt.Axes, clusters: pd.DataFrame) -> None:
    """Draw a global bubble map on the given axes, adapted from plot_global_bubble_map.py."""
    df = clusters[valid_latlon(clusters)].copy()
    has_area = numeric(df, "area_km2").notna() & (numeric(df, "area_km2") > 0)

    draw_background(ax)
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.grid(True, linewidth=0.3, alpha=0.35, zorder=0)

    # Marker size proportional to sqrt(area)
    sizes = np.where(
        has_area,
        np.sqrt(np.clip(numeric(df, "area_km2"), 10, None)) * 0.18,
        4.0,
    )

    BUBBLE_COLORS = {
        "resolved":   "#2196F3",
        "unresolved": "#FF9800",
        "unknown":    "#9E9E9E",
    }

    for status in ["resolved", "unresolved", "unknown"]:
        mask = (df["basin_status"] == status) if status != "unknown" else ~df["basin_status"].isin(["resolved", "unresolved"])
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

    ax.legend(
        loc="lower left", markerscale=1.8, framealpha=0.85,
        title="Basin status", fontsize=8,
    )

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
    stats_text = "\n".join(stats_lines)

    ax.text(
        0.98, 0.02, stats_text,
        transform=ax.transAxes, fontsize=8,
        verticalalignment="bottom", horizontalalignment="right",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.85),
    )


def fig_composite_spatial_coverage(clusters: pd.DataFrame, area_dist: pd.DataFrame, satellite: pd.DataFrame) -> None:
    """3-panel composite figure: (a) bubble map + upstream inset, (b) climatology, (c) satellite."""
    fig = plt.figure(figsize=(14, 10))
    gs = gridspec.GridSpec(2, 2, height_ratios=[1.3, 1], wspace=0.12, hspace=0.18)

    # --- Panel (a): global bubble map + upstream area inset ---
    ax_a = fig.add_subplot(gs[0, :])
    draw_bubble_map(ax_a, clusters)
    ax_a.text(0.01, 0.97, "(a)", transform=ax_a.transAxes, fontsize=14, fontweight="bold",
              va="top", ha="left", bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    # Inset: upstream area distribution
    from mpl_toolkits.axes_grid1.inset_locator import inset_axes
    ax_inset = inset_axes(ax_a, width="32%", height="30%", loc="lower right",
                          bbox_to_anchor=(0, 0, 1, 1), bbox_transform=ax_a.transAxes)
    fig_upstream_area_distribution(area_dist, ax=ax_inset, scientific_notation=True)
    ax_inset.set_title("Upstream area distribution", fontsize=8)

    # --- Panel (b): climatology spatial coverage ---
    ax_b = fig.add_subplot(gs[1, 0])
    fig_climatology_spatial_coverage(ax=ax_b)
    ax_b.text(0.01, 0.97, "(b)", transform=ax_b.transAxes, fontsize=14, fontweight="bold",
              va="top", ha="left", bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    # --- Panel (c): satellite validation spatial distribution ---
    ax_c = fig.add_subplot(gs[1, 1])
    if satellite.empty:
        ax_c.text(0.5, 0.5, "No satellite validation data", transform=ax_c.transAxes,
                  ha="center", va="center", fontsize=12, color="#888888")
    else:
        fig_satellite_validation_spatial_distribution(satellite, ax=ax_c)
    ax_c.text(0.01, 0.97, "(c)", transform=ax_c.transAxes, fontsize=14, fontweight="bold",
              va="top", ha="left", bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    save_figure(fig, "fig_composite_spatial_coverage")


def main() -> int:
    require_node113()
    clusters = read_csv(TABLES_DIR / "table_cluster_spatial_attributes.csv", required=True)
    by_resolution = read_csv(TABLES_DIR / "table_spatial_coverage_by_resolution.csv", required=True)
    by_region = read_csv(TABLES_DIR / "table_spatial_coverage_by_region.csv", required=True)
    by_country = read_csv(TABLES_DIR / "table_spatial_coverage_by_country.csv", required=True)
    by_region_source = read_csv(TABLES_DIR / "table_spatial_coverage_by_region_source.csv", required=True)
    by_region_resolution = read_csv(TABLES_DIR / "table_spatial_coverage_by_region_resolution.csv", required=True)
    area_dist = read_csv(TABLES_DIR / "table_upstream_area_distribution.csv", required=True)
    by_source = read_csv(TABLES_DIR / "table_spatial_coverage_by_source.csv", required=True)
    satellite = read_csv(SATELLITE_CATALOG, required=False)

    fig_global_cluster_distribution(clusters)
    fig_global_cluster_status_and_basins(clusters)
    fig_spatial_coverage_by_resolution(by_resolution)
    fig_spatial_coverage_by_region_country(by_region, by_country)
    fig_spatial_coverage_by_region_source_clusters(by_region_source)
    fig_spatial_coverage_by_region_source_records(by_region_source)
    fig_spatial_coverage_by_region_resolution(by_region_resolution)
    fig_upstream_area_distribution(area_dist)
    fig_source_spatial_contribution(by_source)
    if not satellite.empty:
        fig_satellite_validation_spatial_distribution(satellite)
        fig_main_vs_satellite_spatial_coverage(clusters, satellite)

    fig_composite_spatial_coverage(clusters, area_dist, satellite)
    fig_climatology_spatial_coverage()
    fig_timeseries_spatial_coverage()
    fig_climatology_vs_timeseries_coverage()

    print("Wrote S8 spatial coverage figures to {}".format(FIGURES_DIR))
    return 0


if __name__ == "__main__":
    sys.exit(main())
