#!/usr/bin/env python3
"""
Diagnose spatial overlap between RiverSed / Dethier satellite stations
and main cluster references.

Produces per-source outputs in separate sub-directories:
  validate/output/riversed/   ← RiverSed outputs
  validate/output/dethier/    ← Dethier outputs

Each sub-directory contains:
  1. global_overview_{source}.png — global / CONUS overview
  2. Regional zoom maps (source-specific bounding boxes)
  3. Basin diagnostic maps for representative cases
  4. {source}_spatial_diagnostics.csv — per-station spatial metrics

Usage:
  conda activate wzx
  python validate/diagnose_riversed_dethier_spatial_overlap_maps.py
  python validate/diagnose_riversed_dethier_spatial_overlap_maps.py --csv-only
  python validate/diagnose_riversed_dethier_spatial_overlap_maps.py \
  python validate/diagnose_riversed_spatial_overlap_maps.py \

This script only diagnoses — it does not modify any pipeline output.
"""



import argparse
import math
import os
import sqlite3
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# ── sys.path setup for sibling imports ──────────────────────────────────────
_SCRIPT_DIR = Path(__file__).resolve().parent
_PARENT_DIR = _SCRIPT_DIR.parent  # scripts_basin_test/
if str(_PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(_PARENT_DIR))

from pipeline_paths import (
    S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV,
    RELEASE_CLUSTER_POINTS_GPKG,
    RELEASE_CLUSTER_BASINS_GPKG,
    RELEASE_SATELLITE_CATALOG_CSV,
    get_output_r_root,
)

warnings.filterwarnings("ignore", category=RuntimeWarning)

# ── Path setup ──────────────────────────────────────────────────────────────
OUTPUT_R_ROOT = get_output_r_root(_PARENT_DIR)
OUTPUT_DIR = _SCRIPT_DIR / "output" / "riversed"

DEFAULT_LINKAGE_CSV = OUTPUT_R_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV
DEFAULT_CLUSTER_POINTS_GPKG = OUTPUT_R_ROOT / RELEASE_CLUSTER_POINTS_GPKG
DEFAULT_CLUSTER_BASINS_GPKG = OUTPUT_R_ROOT / RELEASE_CLUSTER_BASINS_GPKG
DEFAULT_SATELLITE_CATALOG_CSV = OUTPUT_R_ROOT / RELEASE_SATELLITE_CATALOG_CSV
DEFAULT_MERIT_DIR = (
    OUTPUT_R_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
)

# ── RiverSed regional bounding boxes (CONUS focus: minx, miny, maxx, maxy) ──
RIVERSED_REGIONS = {
    "CONUS": (-125.0, 24.0, -65.0, 50.0),
    "PacificNorthwest": (-125.0, 41.0, -112.0, 50.0),
    "California": (-125.0, 32.0, -113.0, 42.0),
    "SouthwestUS": (-120.0, 30.0, -100.0, 39.0),
    "Midwest": (-105.0, 36.0, -90.0, 49.0),
    "Northeast": (-82.0, 39.0, -67.0, 48.0),
    "Southeast": (-92.0, 24.0, -75.0, 40.0),
}


# ── Helpers ─────────────────────────────────────────────────────────────────


def _haversine_km(lat1, lon1, lat2, lon2):
    """Vectorised haversine distance in km between two coordinate arrays."""
    lat1_r = np.radians(lat1)
    lon1_r = np.radians(lon1)
    lat2_r = np.radians(lat2)
    lon2_r = np.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2.0) ** 2
    )
    return 6371.0088 * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(np.maximum(0.0, 1.0 - a)))


def _clean_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


# ── Data loading ────────────────────────────────────────────────────────────


def load_linkage_csv(path):
    """Load s5b satellite-main-cluster linkage CSV."""
    path = Path(path)
    if not path.is_file():
        print("WARNING: linkage CSV not found: {}".format(path))
        return None
    df = pd.read_csv(path, low_memory=False)
    print("  Loaded linkage CSV: {} rows".format(len(df)))
    return df


def load_cluster_summary_sqlite(gpkg_path):
    """Load cluster summary table from GPKG via sqlite3 (no geopandas needed).

    Returns a DataFrame with columns: cluster_uid, lat, lon, and any other
    numeric/string columns present.
    """
    gpkg_path = Path(gpkg_path)
    if not gpkg_path.is_file():
        print("WARNING: cluster points GPKG not found: {}".format(gpkg_path))
        return None

    try:
        conn = sqlite3.connect(str(gpkg_path))
        # Read the cluster_summary table
        df = pd.read_sql_query("SELECT * FROM cluster_summary", conn)
        conn.close()

        # GPKG stores geometry as BLOB; drop it, we use lat/lon columns
        if "geom" in df.columns:
            df = df.drop(columns=["geom"])

        print("  Loaded cluster_summary via sqlite3: {} rows, {} cols".format(
            len(df), len(df.columns)))
        return df
    except Exception as exc:
        print("WARNING: failed to read cluster_summary GPKG: {}".format(exc))
        return None


def load_satellite_catalog(path):
    """Load satellite validation catalog for station_uid lookups."""
    path = Path(path)
    if not path.is_file():
        print("WARNING: satellite catalog not found: {}".format(path))
        return None
    cat = pd.read_csv(path, low_memory=False)
    print("  Loaded satellite catalog: {} rows".format(len(cat)))
    return cat


# ── Region tag assignment ───────────────────────────────────────────────────


def assign_riversed_region(lat, lon):
    """Assign a coarse CONUS region tag for RiverSed stations."""
    if -125 <= lon <= -112 and 41 <= lat <= 50:
        return "PacificNorthwest"
    if -125 <= lon <= -113 and 32 <= lat <= 42:
        return "California"
    if -120 <= lon <= -100 and 30 <= lat <= 39:
        return "SouthwestUS"
    if -105 <= lon <= -90 and 36 <= lat <= 49:
        return "Midwest"
    if -82 <= lon <= -67 and 39 <= lat <= 48:
        return "Northeast"
    if -92 <= lon <= -75 and 24 <= lat <= 40:
        return "Southeast"
    return "other_conus"



# ── Nearest main cluster computation ───────────────────────────────────────


def compute_nearest_main(sat_df, main_df):
    """For each satellite point, find the nearest main cluster point.

    Returns lists of (cluster_uid, distance_km).
    """
    sat_coords = sat_df[["lat", "lon"]].to_numpy(dtype=float)
    main_coords = main_df[["lat", "lon"]].to_numpy(dtype=float)
    main_uids = main_df["cluster_uid"].to_numpy(dtype=str)

    if len(sat_coords) == 0 or len(main_coords) == 0:
        return [], []

    nearest_uids = []
    nearest_dists = []
    for i in range(len(sat_coords)):
        glat, glon = sat_coords[i]
        dists = _haversine_km(glat, glon, main_coords[:, 0], main_coords[:, 1])
        idx = np.nanargmin(dists)
        nearest_uids.append(main_uids[idx] if np.isfinite(dists[idx]) else "")
        nearest_dists.append(dists[idx] if np.isfinite(dists[idx]) else math.nan)

    return nearest_uids, nearest_dists


# ── Diagnostic table builder ────────────────────────────────────────────────


def build_diagnostic_table(source_df, source_name, cluster_summary_df):
    """Assemble a per-station diagnostic DataFrame.

    Parameters
    ----------
    source_df : DataFrame
        Satellite records for one source, with nearest_main columns already added.
    source_name : str
        "RiverSed" or "Dethier" — used for region tagging.
    cluster_summary_df : DataFrame or None
        Main cluster summary (for optional lookups).
    """
    print("\nBuilding {} spatial diagnostic table ...".format(source_name))

    rows = []
    for _, row in source_df.iterrows():
        lat = row.get("lat", math.nan)
        lon = row.get("lon", math.nan)
        if not np.isfinite(lat) or not np.isfinite(lon):
            continue

        uid = _clean_text(row.get("satellite_location_uid", ""))
        res = _clean_text(row.get("resolution", ""))
        basin_id = row.get("basin_id", math.nan)
        link_status = _clean_text(row.get("link_status", ""))
        linked_cluster = _clean_text(row.get("linked_cluster_uid", ""))
        linked_res = _clean_text(row.get("linked_resolution", ""))
        unlinked_reason = _clean_text(row.get("unlinked_reason", ""))

        region = assign_riversed_region(lat, lon)

        nn_uid = _clean_text(row.get("nearest_main_cluster_uid", ""))
        nn_dst = row.get("nearest_main_distance_km", math.nan)

        # same_reach from linkage
        same_reach = False
        if link_status == "linked":
            sr = row.get("same_reach")
            same_reach = bool(sr) if pd.notna(sr) else False

        # point_distance_m from linkage
        point_dist_m = row.get("point_distance_m", math.nan)
        network_dist_m = row.get("network_distance_m", math.nan)

        rows.append({
            "satellite_station_uid": uid,
            "source": source_name,
            "resolution": res,
            "lat": lat,
            "lon": lon,
            "basin_id": basin_id if pd.notna(basin_id) else None,
            "link_status": link_status,
            "unlinked_reason": unlinked_reason,
            "linked_cluster_uid": linked_cluster,
            "linked_resolution": linked_res,
            "nearest_main_cluster_uid": nn_uid,
            "nearest_main_distance_km": nn_dst,
            "same_reach": same_reach,
            "point_distance_m": point_dist_m,
            "network_distance_m": network_dist_m,
            "region_tag": region,
        })

    result = pd.DataFrame(rows)
    print("  {} diagnostic table: {} rows".format(source_name, len(result)))
    return result


# ── Map generation ──────────────────────────────────────────────────────────


def _setup_map_ax_basic(ax, extent):
    """Configure axes with simple background (no cartopy)."""
    ax.set_xlim(extent[0], extent[2])
    ax.set_ylim(extent[1], extent[3])
    ax.set_facecolor("#e0f0ff")
    ax.grid(True, linestyle=":", alpha=0.3, color="#888888")


def _setup_map_ax_cartopy(ax, extent, basemap=True):
    """Configure a cartopy axes with features."""
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # NOTE: extent is (min_lon, min_lat, max_lon, max_lat) but cartopy expects (min_lon, max_lon, min_lat, max_lat)
    ax.set_extent([extent[0], extent[2], extent[1], extent[3]], crs=ccrs.PlateCarree())
    if basemap:
        ax.add_feature(cfeature.LAND, color="#f0f0f0", zorder=0)
        ax.add_feature(cfeature.OCEAN, color="#e0f0ff", zorder=0)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5, edgecolor="#555555", zorder=1)
        ax.add_feature(cfeature.BORDERS, linewidth=0.3, edgecolor="#888888", zorder=1)
        ax.add_feature(cfeature.LAKES, color="#d0e8ff", edgecolor="#888888",
                        linewidth=0.3, zorder=1)
        ax.add_feature(cfeature.RIVERS, linewidth=0.3, edgecolor="#a0c8f0", zorder=1)


def _has_cartopy():
    try:
        import cartopy  # noqa: F401
        return True
    except ImportError:
        return False


def _add_scale_bar(ax, extent, length_km=200):
    """Add a simple scale bar to a cartopy axis."""
    import cartopy.crs as ccrs

    minx, miny, maxx, maxy = extent
    mid_lat = (miny + maxy) / 2.0
    left_lon = minx + (maxx - minx) * 0.05
    right_lon = left_lon + length_km / (111.32 * math.cos(math.radians(mid_lat)))
    bar_y = miny + (maxy - miny) * 0.05

    ax.plot(
        [left_lon, right_lon], [bar_y, bar_y],
        transform=ccrs.PlateCarree(),
        color="k", linewidth=2, solid_capstyle="butt",
    )
    ax.plot(
        [left_lon, left_lon], [bar_y - 0.5, bar_y + 0.5],
        transform=ccrs.PlateCarree(),
        color="k", linewidth=1.5,
    )
    ax.plot(
        [right_lon, right_lon], [bar_y - 0.5, bar_y + 0.5],
        transform=ccrs.PlateCarree(),
        color="k", linewidth=1.5,
    )
    ax.text(
        (left_lon + right_lon) / 2.0, bar_y - 0.8,
        "{} km".format(length_km),
        transform=ccrs.PlateCarree(),
        ha="center", va="top", fontsize=8,
    )


def _buffer_circle_deg(lat, lon, radius_km, n_pts=36):
    """Return (lon_array, lat_array) of a circle in geographic degrees."""
    r_deg_lat = radius_km / 111.32
    r_deg_lon = radius_km / (111.32 * math.cos(math.radians(lat)))
    angles = np.linspace(0, 2 * math.pi, n_pts)
    circ_lon = lon + r_deg_lon * np.cos(angles)
    circ_lat = lat + r_deg_lat * np.sin(angles)
    return circ_lon, circ_lat


def _points_in_bbox(df, bbox):
    """Filter DataFrame rows whose (lat, lon) fall inside *bbox*."""
    minx, miny, maxx, maxy = bbox
    mask = df["lon"].between(minx, maxx) & df["lat"].between(miny, maxy)
    return df.loc[mask].copy()


def _load_merit_reaches_for_bbox(merit_dir, bbox):
    """Load MERIT river reach geometries for a bounding box."""
    try:
        import geopandas as gpd
        import pyogrio
    except ImportError:
        return None, "need pyogrio+geopandas"

    riv_dir = Path(merit_dir) / "pfaf_level_02"
    if not riv_dir.is_dir():
        return None, "pfaf_level_02 dir not found"

    pattern = "riv_pfaf_*_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"
    riv_files = sorted(riv_dir.glob(pattern))
    if not riv_files:
        return None, "no riv shapefiles found"

    parts = []
    for fpath in riv_files:
        try:
            part = pyogrio.read_dataframe(
                str(fpath), columns=["COMID"], bbox=bbox,
            )
            if part is not None and len(part) > 0:
                parts.append(part)
        except Exception:
            pass

    if not parts:
        return None, "no reaches in bbox"

    combined = pd.concat(parts, ignore_index=True)
    gdf = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")
    return gdf, None


def generate_global_overview(source_name, source_df, main_pts, out_dir,
                             global_extent=None):
    """Global / CONUS overview map for one source."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    if global_extent is None:
        global_extent = (-128, 24, -64, 50)
    title_base = f"{source_name} — Main Cluster Spatial Overview (CONUS)"
    figsize = (14, 8)

    print("\n  Generating global_overview_{}.png ...".format(source_name.lower()))

    if use_cartopy:
        proj = ccrs.PlateCarree(central_longitude=-100)
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(1, 1, 1, projection=proj)
        _setup_map_ax_cartopy(ax, extent=global_extent, basemap=True)
    else:
        fig, ax = plt.subplots(figsize=figsize)
        _setup_map_ax_basic(ax, extent=global_extent)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    # Main clusters
    if main_pts is not None and len(main_pts) > 0:
        ax.scatter(
            main_pts["lon"].values, main_pts["lat"].values,
            s=3, c="#1976D2", alpha=0.6, edgecolors="none",
            **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
            label="Main clusters ({})".format(len(main_pts)),
            zorder=3,
        )

    # Source points
    if source_df is not None and len(source_df) > 0:
        linked = source_df[source_df["link_status"] == "linked"]
        if len(linked) > 0:
            ax.scatter(
                linked["lon"].values, linked["lat"].values,
                s=30, marker="^", c="#388E3C", edgecolors="k", linewidth=0.5,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="{} linked ({})".format(source_name, len(linked)),
                zorder=5,
            )

        unlinked = source_df[source_df["link_status"] != "linked"]
        if len(unlinked) > 0:
            ax.scatter(
                unlinked["lon"].values, unlinked["lat"].values,
                s=15, marker="v", c="#D32F2F", alpha=0.7,
                edgecolors="k", linewidth=0.3,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="{} unlinked ({})".format(source_name, len(unlinked)),
                zorder=4,
            )

    ax.legend(loc="lower left", fontsize=9, framealpha=0.9,
              markerscale=1, handletextpad=0.5)
    ax.set_title(title_base, fontsize=13)

    fname = f"global_overview_{source_name.lower()}.png"
    out_path = out_dir / fname
    fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("    -> {}".format(out_path))


def generate_regional_zoom(source_name, region_name, bbox, source_df,
                           main_pts, merit_dir, out_dir):
    """Regional zoom map for one source with MERIT reaches and link lines."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    print("\n  Generating {}_{}_zoom.png ...".format(source_name.lower(), region_name))

    merit_gdf = None
    if source_name == "RiverSed":
        merit_gdf, _ = _load_merit_reaches_for_bbox(merit_dir, bbox)

    if use_cartopy:
        fig = plt.figure(figsize=(12, 10))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        _setup_map_ax_cartopy(ax, extent=bbox, basemap=True)
    else:
        fig, ax = plt.subplots(figsize=(12, 10))
        _setup_map_ax_basic(ax, extent=bbox)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    # MERIT reaches (RiverSed only)
    if merit_gdf is not None and len(merit_gdf) > 0:
        merit_gdf.plot(
            ax=ax, color="#999999", linewidth=0.4, alpha=0.6,
            **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
            label="MERIT reaches", zorder=2,
        )

    # Main clusters
    if main_pts is not None and len(main_pts) > 0:
        m_in = _points_in_bbox(main_pts, bbox)
        if len(m_in) > 0:
            ax.scatter(
                m_in["lon"].values, m_in["lat"].values,
                s=40, facecolors="none", edgecolors="#1976D2",
                linewidths=1.2,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="Main clusters", zorder=4,
            )

    # Source points
    if source_df is not None and len(source_df) > 0:
        g_in = _points_in_bbox(source_df, bbox)
        linked = g_in[g_in["link_status"] == "linked"]
        unlinked = g_in[g_in["link_status"] != "linked"]

        if len(linked) > 0:
            ax.scatter(
                linked["lon"].values, linked["lat"].values,
                s=80, marker="^", c="#388E3C", edgecolors="k", linewidth=0.8,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="{} linked".format(source_name), zorder=7,
            )
            for _, row_l in linked.iterrows():
                mc_uid = row_l.get("linked_cluster_uid", "")
                if mc_uid and _clean_text(mc_uid) and main_pts is not None:
                    mc_rows = main_pts.loc[main_pts["cluster_uid"] == mc_uid]
                    if len(mc_rows) > 0:
                        ax.plot(
                            [row_l["lon"], mc_rows.iloc[0]["lon"]],
                            [row_l["lat"], mc_rows.iloc[0]["lat"]],
                            **({} if not use_cartopy
                               else {"transform": ccrs.PlateCarree()}),
                            color="#388E3C", linewidth=1.2, linestyle="-",
                            alpha=0.8, zorder=5,
                        )

        if len(unlinked) > 0:
            ax.scatter(
                unlinked["lon"].values, unlinked["lat"].values,
                s=60, marker="v", c="#D32F2F", edgecolors="k", linewidth=0.5,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="{} unlinked".format(source_name), zorder=6,
            )
            for _, row_u in unlinked.iterrows():
                nn_uid = row_u.get("nearest_main_cluster_uid", "")
                if nn_uid and _clean_text(nn_uid) and main_pts is not None:
                    mc_rows = main_pts.loc[main_pts["cluster_uid"] == nn_uid]
                    if len(mc_rows) > 0:
                        ax.plot(
                            [row_u["lon"], mc_rows.iloc[0]["lon"]],
                            [row_u["lat"], mc_rows.iloc[0]["lat"]],
                            **({} if not use_cartopy
                               else {"transform": ccrs.PlateCarree()}),
                            color="#F57C00", linewidth=0.8, linestyle="--",
                            alpha=0.6, zorder=4,
                        )

        # 5 km buffer circles
        for _, row_g in g_in.iterrows():
            circ = _buffer_circle_deg(row_g["lat"], row_g["lon"], 5.0)
            ax.plot(
                circ[0], circ[1],
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                color=("#D32F2F" if row_g["link_status"] != "linked"
                       else "#388E3C"),
                linewidth=0.6, linestyle="--", alpha=0.5, zorder=3,
            )

    ax.legend(loc="lower left", fontsize=8, framealpha=0.9, markerscale=0.8)
    ax.set_title("{} — {} spatial overlap".format(region_name, source_name),
                 fontsize=12)

    if use_cartopy and bbox[2] - bbox[0] < 50:
        _add_scale_bar(ax, bbox, length_km=100)

    fname = "{}_{}_zoom.png".format(source_name.lower(), region_name.lower())
    out_path = out_dir / fname
    fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("    -> {}".format(out_path))


def generate_regional_main_clusters(region_name, bbox, main_pts, out_dir):
    """Regional map showing ONLY main clusters, no satellite points, so
    the spatial distribution of main clusters is clearly visible."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    print("\n  Generating main_clusters_{}_zoom.png ...".format(region_name))

    if use_cartopy:
        fig = plt.figure(figsize=(12, 10))
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        _setup_map_ax_cartopy(ax, extent=bbox, basemap=True)
    else:
        fig, ax = plt.subplots(figsize=(12, 10))
        _setup_map_ax_basic(ax, extent=bbox)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    # ONLY main clusters — no satellite points, no MERIT reaches
    if main_pts is not None and len(main_pts) > 0:
        m_in = _points_in_bbox(main_pts, bbox)
        if len(m_in) > 0:
            ax.scatter(
                m_in["lon"].values, m_in["lat"].values,
                s=40, facecolors="#1976D2", edgecolors="k",
                linewidths=0.5,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="Main clusters ({})".format(len(m_in)),
                zorder=4,
            )

    ax.legend(loc="lower left", fontsize=8, framealpha=0.9, markerscale=0.8)
    ax.set_title("{} — Main Clusters Only".format(region_name), fontsize=12)

    if use_cartopy and bbox[2] - bbox[0] < 50:
        _add_scale_bar(ax, bbox, length_km=100)

    fname = "main_clusters_{}_zoom.png".format(region_name.lower())
    out_path = out_dir / fname
    fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("    -> {}".format(out_path))



def generate_basin_diagnostics(source_name, source_df, main_pts,
                               cluster_basins_gpkg, merit_dir, out_dir):
    """Basin polygon diagnostic maps for representative cases."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    print("\n  Generating basin diagnostic maps for {} ...".format(source_name))

    # Categorize stations
    cat_linked = source_df[source_df["link_status"] == "linked"]
    near_mask = (
        (source_df["link_status"] != "linked")
        & source_df["nearest_main_distance_km"].notna()
        & (source_df["nearest_main_distance_km"] <= 10.0)
    )
    cat_near = source_df.loc[near_mask]
    far_mask = (
        (source_df["link_status"] != "linked")
        & (source_df["nearest_main_distance_km"].isna()
           | (source_df["nearest_main_distance_km"] > 10.0))
    )
    cat_far = source_df.loc[far_mask]

    categories = [
        ("linked", cat_linked, "#388E3C"),
        ("unlinked_near", cat_near, "#F57C00"),
        ("unlinked_far", cat_far, "#D32F2F"),
    ]

    for cat_name, cat_df_cat, color in categories:
        n_examples = min(3, len(cat_df_cat))
        if n_examples == 0:
            continue

        indices = np.linspace(0, len(cat_df_cat) - 1, n_examples, dtype=int)
        examples = cat_df_cat.iloc[indices]

        for idx_i, (_, row) in enumerate(examples.iterrows()):
            if use_cartopy:
                fig, ax = plt.subplots(
                    1, 1, figsize=(8, 7),
                    subplot_kw={"projection": ccrs.PlateCarree()},
                )
            else:
                fig, ax = plt.subplots(figsize=(8, 7))

            cluster_uid = row.get("linked_cluster_uid", "")
            if not cluster_uid or _clean_text(cluster_uid) == "":
                cluster_uid = row.get("nearest_main_cluster_uid", "")

            buffer_deg = 0.5
            local_extent = (
                row["lon"] - buffer_deg, row["lat"] - buffer_deg,
                row["lon"] + buffer_deg, row["lat"] + buffer_deg,
            )

            if use_cartopy:
                _setup_map_ax_cartopy(ax, extent=local_extent, basemap=True)
            else:
                _setup_map_ax_basic(ax, extent=local_extent)
                ax.set_xlabel("Longitude")
                ax.set_ylabel("Latitude")

            # MERIT reaches nearby
            sub_bbox = (
                row["lon"] - 0.5, row["lat"] - 0.5,
                row["lon"] + 0.5, row["lat"] + 0.5,
            )
            merit_local, _ = _load_merit_reaches_for_bbox(
                Path(merit_dir), sub_bbox
            )
            if merit_local is not None and len(merit_local) > 0:
                merit_local.plot(
                    ax=ax, color="#666666", linewidth=0.8, alpha=0.7,
                    **({} if not use_cartopy
                       else {"transform": ccrs.PlateCarree()}),
                    zorder=3,
                )

            # Main cluster point
            if cluster_uid and _clean_text(cluster_uid) and main_pts is not None:
                mc_sub = main_pts.loc[main_pts["cluster_uid"] == cluster_uid]
                if len(mc_sub) > 0:
                    ax.scatter(
                        mc_sub.iloc[0]["lon"], mc_sub.iloc[0]["lat"],
                        s=80, c="#1976D2", edgecolors="k", linewidth=0.8,
                        **({} if not use_cartopy
                           else {"transform": ccrs.PlateCarree()}),
                        label="Main cluster", zorder=5,
                    )

            # Source point
            marker = "^" if row["link_status"] == "linked" else "v"
            ax.scatter(
                row["lon"], row["lat"],
                s=100, marker=marker, c=color, edgecolors="k", linewidth=0.8,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="{} ({})".format(source_name, cat_name), zorder=6,
            )

            # Link line
            if cluster_uid and _clean_text(cluster_uid) and main_pts is not None:
                mc_sub = main_pts.loc[main_pts["cluster_uid"] == cluster_uid]
                if len(mc_sub) > 0:
                    ls = "-" if row["link_status"] == "linked" else "--"
                    ax.plot(
                        [row["lon"], mc_sub.iloc[0]["lon"]],
                        [row["lat"], mc_sub.iloc[0]["lat"]],
                        **({} if not use_cartopy
                           else {"transform": ccrs.PlateCarree()}),
                        color=color, linewidth=1.5, linestyle=ls,
                        alpha=0.8, zorder=4,
                    )

            # 5 km buffer
            circ = _buffer_circle_deg(row["lat"], row["lon"], 5.0)
            ax.plot(
                circ[0], circ[1],
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                color=color, linewidth=0.8, linestyle="--", alpha=0.6, zorder=3,
            )

            dist_km = row.get("nearest_main_distance_km", math.nan)
            title = "{} -- {:.4f}, {:.4f}\n{} (dist={:.1f} km)".format(
                cat_name, row["lat"], row["lon"], cluster_uid, dist_km,
            )
            ax.set_title(title, fontsize=10)
            ax.legend(loc="lower left", fontsize=8, framealpha=0.9)

            fname = "basin_diag_{}_{}_{}.png".format(source_name.lower(), cat_name, idx_i)
            out_path = out_dir / fname
            fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
            plt.close(fig)
            print("    -> {}".format(out_path))


# ── Summary ─────────────────────────────────────────────────────────────────


def print_summary(diag, source_name):
    """Print terminal summary statistics for one source."""
    print("\n" + "=" * 60)
    print("{} SPATIAL DIAGNOSTIC SUMMARY".format(source_name.upper()))
    print("=" * 60)

    total = len(diag)
    linked = int(diag["link_status"].eq("linked").sum())
    unlinked = total - linked
    same_reach = int(diag["same_reach"].eq(True).sum())
    near_5 = int(
        (diag["nearest_main_distance_km"].notna()
         & (diag["nearest_main_distance_km"] <= 5.0)).sum()
    )
    near_10 = int(
        (diag["nearest_main_distance_km"].notna()
         & (diag["nearest_main_distance_km"] <= 10.0)).sum()
    )
    near_25 = int(
        (diag["nearest_main_distance_km"].notna()
         & (diag["nearest_main_distance_km"] <= 25.0)).sum()
    )

    print("  1. Total {} stations:              {:>7d}".format(source_name, total))
    print("  2. Linked {} stations:             {:>7d}".format(source_name, linked))
    print("  3. Unlinked {} stations:           {:>7d}".format(source_name, unlinked))
    print("  4. Same MERIT reach:                {:>7d}".format(same_reach))
    print("  5. nearest_main_distance <=  5 km:  {:>7d}".format(near_5))
    print("     nearest_main_distance <= 10 km:  {:>7d}".format(near_10))
    print("     nearest_main_distance <= 25 km:  {:>7d}".format(near_25))

    if "region_tag" in diag.columns:
        print("\n  --- Regional distribution ---")
        for reg, count in diag["region_tag"].value_counts().items():
            print("    {:20s}: {:>7d}".format(reg, count))

    print("=" * 60)


# ── Per-source pipeline ────────────────────────────────────────────────────


def process_source(source_name, linkage_df, cluster_summary_df,
                   cluster_basins_gpkg, satellite_catalog, merit_dir,
                   out_dir, csv_only):
    """Run the full diagnostic pipeline for one source."""
    print("\n" + "#" * 60)
    print("PROCESSING SOURCE: {}".format(source_name))
    print("#" * 60)

    # ── 1. Filter source records from linkage CSV ──────────────────────────
    src_df = linkage_df[
        linkage_df["source"].str.upper() == source_name.upper()
    ].copy()
    if len(src_df) == 0:
        print("WARNING: no {} records found. Skipping.".format(source_name))
        return
    print("  {} records in linkage: {}".format(source_name, len(src_df)))

    # ── 2. Compute nearest main cluster for all points ─────────────────────
    if cluster_summary_df is not None and len(cluster_summary_df) > 0:
        nn_uids, nn_dists = compute_nearest_main(src_df, cluster_summary_df)
        src_df["nearest_main_cluster_uid"] = nn_uids
        src_df["nearest_main_distance_km"] = nn_dists
    else:
        src_df["nearest_main_cluster_uid"] = ""
        src_df["nearest_main_distance_km"] = math.nan

    # ── 3. Build diagnostic table ──────────────────────────────────────────
    diag = build_diagnostic_table(src_df, source_name, cluster_summary_df)

    csv_path = out_dir / "{}_spatial_diagnostics.csv".format(source_name.lower())
    diag.to_csv(csv_path, index=False)
    print("\n  Diagnostic CSV -> {}".format(csv_path))

    # ── 4. Print summary ───────────────────────────────────────────────────
    print_summary(diag, source_name)

    # ── 5. Generate maps (if not --csv-only) ───────────────────────────────
    if csv_only:
        print("\n  --csv-only: maps skipped for {}.".format(source_name))
        return

    import os as _mpl_os
    _mpl_os.environ["MPLBACKEND"] = "Agg"
    import matplotlib
    if matplotlib.get_backend() != "agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.close("all")

    # ── 5a. Global overview ────────────────────────────────────────────────
    generate_global_overview(source_name, src_df, cluster_summary_df, out_dir)

    # ── 5b. Regional zooms ─────────────────────────────────────────────────
    for region_name, region_bbox in RIVERSED_REGIONS.items():
        # Skip the global overview-level regions (already generated above)
        if region_name in ("Global", "CONUS"):
            continue
        generate_regional_zoom(
            source_name, region_name, region_bbox,
            src_df,
            cluster_summary_df, merit_dir, out_dir,
        )
        # Also generate a main-clusters-only version so the spatial
        # distribution of main clusters is clearly visible.
        generate_regional_main_clusters(
            region_name, region_bbox,
            cluster_summary_df, out_dir,
        )

    # ── 5c. Basin diagnostics ──────────────────────────────────────────────
    generate_basin_diagnostics(
        source_name, src_df, cluster_summary_df,
        cluster_basins_gpkg, merit_dir, out_dir,
    )


# ── CLI ─────────────────────────────────────────────────────────────────────


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--linkage-csv", default=str(DEFAULT_LINKAGE_CSV),
        help="s5b satellite-main-cluster linkage CSV",
    )
    parser.add_argument(
        "--cluster-points-gpkg", default=str(DEFAULT_CLUSTER_POINTS_GPKG),
        help="sed_reference_cluster_points.gpkg",
    )
    parser.add_argument(
        "--cluster-basins-gpkg", default=str(DEFAULT_CLUSTER_BASINS_GPKG),
        help="sed_reference_cluster_basins.gpkg",
    )
    parser.add_argument(
        "--satellite-catalog-csv", default=str(DEFAULT_SATELLITE_CATALOG_CSV),
        help="satellite_catalog.csv",
    )
    parser.add_argument(
        "--merit-dir", default=str(DEFAULT_MERIT_DIR),
        help="MERIT Hydro Basins root directory",
    )
    parser.add_argument(
        "--out-dir", default=str(OUTPUT_DIR),
        help="Output directory for PNGs and CSV",
    )
    parser.add_argument(
        "--csv-only", action="store_true",
        help="Only generate the diagnostic CSV, skip all maps",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── 1. Load input data ────────────────────────────────────────────────
    print("Loading inputs ...")

    linkage_df = load_linkage_csv(args.linkage_csv)
    if linkage_df is None:
        print("ERROR: linkage CSV is required. Aborting.")
        return 1

    # Load cluster summary via sqlite3 (no geopandas needed)
    cluster_summary_df = load_cluster_summary_sqlite(args.cluster_points_gpkg)

    cat_df = load_satellite_catalog(args.satellite_catalog_csv)

    # ── 2. Process RiverSed ────────────────────────────────────────────────
    process_source(
        source_name="RiverSed",
        linkage_df=linkage_df,
        cluster_summary_df=cluster_summary_df,
        cluster_basins_gpkg=args.cluster_basins_gpkg,
        satellite_catalog=cat_df,
        merit_dir=args.merit_dir,
        out_dir=out_dir,
        csv_only=args.csv_only,
    )
    print("\nDone. All outputs in {}".format(out_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
