#!/usr/bin/env python3
"""
Diagnose spatial overlap between GSED satellite stations and main cluster references.

Produces outputs in:
  validate/output/gsed/

  1. global_overview.png — global map of main clusters, linked & unlinked GSED points
  2. Regional zoom maps (GreatLakes, WesternUS, EasternCanada, SouthwestUS)
  3. Basin diagnostic maps for representative cases
  4. gsed_spatial_diagnostics.csv — per-station spatial metrics

Usage:
  conda activate wzx
  python validate/diagnose_gsed_spatial_overlap_maps.py
  python validate/diagnose_gsed_spatial_overlap_maps.py --csv-only  # skip maps

This script only diagnoses — it does not modify any pipeline output.
"""

from __future__ import annotations

import argparse
import math
import os
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
OUTPUT_DIR = _SCRIPT_DIR / "output" / "gsed"

DEFAULT_LINKAGE_CSV = OUTPUT_R_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV
DEFAULT_CLUSTER_POINTS_GPKG = OUTPUT_R_ROOT / RELEASE_CLUSTER_POINTS_GPKG
DEFAULT_CLUSTER_BASINS_GPKG = OUTPUT_R_ROOT / RELEASE_CLUSTER_BASINS_GPKG
DEFAULT_SATELLITE_CATALOG_CSV = OUTPUT_R_ROOT / RELEASE_SATELLITE_CATALOG_CSV
DEFAULT_MERIT_DIR = (
    OUTPUT_R_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
)

# ── Regional bounding boxes (minx, miny, maxx, maxy) ────────────────────────
REGIONS = {
    "GreatLakes": (-92.0, 40.0, -73.0, 50.0),
    "WesternUS": (-125.0, 30.0, -105.0, 50.0),
    "EasternCanada": (-80.0, 42.0, -55.0, 60.0),
    "SouthwestUS": (-120.0, 25.0, -95.0, 40.0),
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


def load_cluster_points_gpkg(path):
    """Load cluster point summary and per-resolution layers from GPKG."""
    path = Path(path)
    if not path.is_file():
        print("WARNING: cluster points GPKG not found: {}".format(path))
        return None, {}

    try:
        import geopandas as gpd
    except ImportError:
        print("WARNING: geopandas required for GPKG reading. Skipping.")
        return None, {}

    print("  Loading cluster points GPKG layers...")
    summary = gpd.read_file(str(path), layer="cluster_summary")
    print("    cluster_summary: {} rows".format(len(summary)))

    resolution_layers = {}
    for layer_name in ("cluster_daily", "cluster_monthly", "cluster_annual"):
        try:
            df_layer = gpd.read_file(str(path), layer=layer_name)
            resolution_layers[layer_name] = df_layer
            print("    {}: {} rows".format(layer_name, len(df_layer)))
        except Exception as exc:
            print("    {}: skipped ({})".format(layer_name, exc))

    return summary, resolution_layers


def load_cluster_basins_gpkg(path, columns=None):
    """Load basin polygons from the cluster basins GPKG.

    Note: This file is ~408 MB and loading it is expensive.
    Use *columns* to load a subset of columns when full geometry is not needed.
    """
    path = Path(path)
    if not path.is_file():
        print("WARNING: cluster basins GPKG not found: {}".format(path))
        return {}

    try:
        import geopandas as gpd
    except ImportError:
        print("WARNING: geopandas required for GPKG reading. Skipping.")
        return {}

    kw = {}
    if columns:
        kw["columns"] = columns

    print("  Loading cluster basins GPKG layers (may be slow)...")
    basin_layers = {}
    for layer_name in ("basin_daily", "basin_monthly", "basin_annual"):
        try:
            df_layer = gpd.read_file(str(path), layer=layer_name, **kw)
            basin_layers[layer_name] = df_layer
            print("    {}: {} rows, {} cols".format(
                layer_name, len(df_layer), len(df_layer.columns)))
        except Exception as exc:
            print("    {}: skipped ({})".format(layer_name, exc))
    return basin_layers


def load_satellite_catalog(path):
    """Load satellite validation catalog for station_uid lookups."""
    path = Path(path)
    if not path.is_file():
        print("WARNING: satellite catalog not found: {}".format(path))
        return None
    cat = pd.read_csv(path, low_memory=False)
    print("  Loaded satellite catalog: {} rows".format(len(cat)))
    return cat


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


# ── Spatial analysis ───────────────────────────────────────────────────────


def compute_nearest_main(gsed_df, main_df):
    """For each GSED point, find the nearest main cluster point.

    Returns lists of (cluster_uid, distance_km).
    """
    gsed_coords = gsed_df[["lat", "lon"]].to_numpy(dtype=float)
    main_coords = main_df[["lat", "lon"]].to_numpy(dtype=float)
    main_uids = main_df["cluster_uid"].to_numpy(dtype=str)

    if len(gsed_coords) == 0 or len(main_coords) == 0:
        return [], []

    nearest_uids = []
    nearest_dists = []
    for i in range(len(gsed_coords)):
        glat, glon = gsed_coords[i]
        dists = _haversine_km(glat, glon, main_coords[:, 0], main_coords[:, 1])
        idx = np.nanargmin(dists)
        nearest_uids.append(main_uids[idx] if np.isfinite(dists[idx]) else "")
        nearest_dists.append(dists[idx] if np.isfinite(dists[idx]) else math.nan)

    return nearest_uids, nearest_dists


def assign_region_tag(lat, lon):
    """Assign a coarse region tag based on lat/lon."""
    if -92 <= lon <= -73 and 40 <= lat <= 50:
        return "GreatLakes"
    if -125 <= lon <= -105 and 30 <= lat <= 50:
        return "WesternUS"
    if -80 <= lon <= -55 and 42 <= lat <= 60:
        return "EasternCanada"
    if -120 <= lon <= -95 and 25 <= lat <= 40:
        return "SouthwestUS"
    return "other"


def _find_cluster_preferred_resolution(cluster_uid, resolution_layers):
    """Look up a cluster's resolution from the per-resolution GPKG layers."""
    if resolution_layers is None:
        return ""
    for layer_name in ("cluster_daily", "cluster_monthly", "cluster_annual"):
        layer = resolution_layers.get(layer_name)
        if layer is not None and "resolution" in layer.columns:
            sub = layer.loc[layer["cluster_uid"] == cluster_uid]
            if len(sub) > 0:
                vals = sub["resolution"].dropna().unique()
                if len(vals) > 0:
                    return str(vals[0])
    return ""


# ── Diagnostic table builder (efficient, uses sjoin) ────────────────────────


def build_diagnostic_table(linkage_df, cluster_df, basin_dict, cat_df):
    """Assemble the full diagnostic CSV as a DataFrame using spatial joins."""
    import geopandas as gpd
    from shapely.geometry import Point as ShapelyPoint

    print("\nBuilding spatial diagnostic table ...")

    gsed = linkage_df[linkage_df["source"].str.upper() == "GSED"].copy()
    print("  GSED rows in linkage: {}".format(len(gsed)))

    # ── Build per-resolution basin_id lookup from cluster_points GPKG ──
    cluster_basin_ids = {}
    if cat_df is not None:
        for layer_name in ("cluster_daily", "cluster_monthly", "cluster_annual"):
            if layer_name in cat_df:
                dfr = cat_df[layer_name]
                if "basin_id" in dfr.columns:
                    for _, r in dfr.iterrows():
                        uid = r.get("cluster_uid", "")
                        bid = r.get("basin_id", None)
                        if uid and pd.notna(bid):
                            cluster_basin_ids[uid] = int(bid)

    # ── inside_any_main_cluster_basin with sjoin (fast) ──
    inside_any_map = {}

    if basin_dict is not None:
        basin_layer = basin_dict.get("basin_daily")
        if basin_layer is not None and len(basin_layer) > 0:
            valid_pts = gsed[gsed["lat"].notna() & gsed["lon"].notna()].copy()
            valid_pts["geometry"] = valid_pts.apply(
                lambda r: ShapelyPoint(r["lon"], r["lat"]), axis=1
            )
            gsed_gdf = gpd.GeoDataFrame(
                valid_pts[["satellite_location_uid", "geometry"]],
                geometry="geometry", crs="EPSG:4326",
            )

            # inner join to find matched points only
            matched = gpd.sjoin(
                gsed_gdf, basin_layer[["geometry"]],
                how="inner", predicate="within",
            )
            for _, r in matched.iterrows():
                inside_any_map[r["satellite_location_uid"]] = True

            for uid in gsed["satellite_location_uid"]:
                inside_any_map.setdefault(uid, False)

            n_inside = sum(1 for v in inside_any_map.values() if v)
            print("  sjoin: {} / {} GSED points inside any basin".format(
                n_inside, len(inside_any_map),
            ))

    # ── inside_linked_cluster_basin ──
    inside_linked_map = {}

    if basin_dict is not None:
        basin_layer = basin_dict.get("basin_daily")
        if basin_layer is not None and len(basin_layer) > 0:
            has_linked = gsed[
                gsed["linked_cluster_uid"].notna()
                & (gsed["linked_cluster_uid"] != "")
                & gsed["lat"].notna()
            ].copy()
            if len(has_linked) > 0:
                for _, row in has_linked.iterrows():
                    uid = _clean_text(row.get("satellite_location_uid", ""))
                    lu = _clean_text(row.get("linked_cluster_uid", ""))
                    if not uid or not lu:
                        continue
                    sub = basin_layer.loc[basin_layer["cluster_uid"] == lu]
                    if len(sub) > 0:
                        pt = ShapelyPoint(row["lon"], row["lat"])
                        inside_linked_map[uid] = any(
                            geom.contains(pt) for geom in sub.geometry
                        )
                    else:
                        inside_linked_map[uid] = False

    # ── Assemble rows ──
    rows = []
    for _, row in gsed.iterrows():
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
        region = assign_region_tag(lat, lon)

        nn_uid = _clean_text(row.get("nearest_main_cluster_uid", ""))
        nn_dst = row.get("nearest_main_distance_km", math.nan)
        nn_res = ""
        if nn_uid and cat_df is not None:
            nn_res = _find_cluster_preferred_resolution(nn_uid, cat_df)

        # same_reach
        same_reach = False
        if link_status == "linked":
            sr = row.get("same_reach")
            same_reach = bool(sr) if pd.notna(sr) else False
        else:
            sat_basin = basin_id if pd.notna(basin_id) else None
            if sat_basin is not None and nn_uid and nn_uid in cluster_basin_ids:
                main_basin = cluster_basin_ids[nn_uid]
                same_reach = bool(int(sat_basin) == int(main_basin))

        inside_any = inside_any_map.get(uid, False)
        inside_linked = inside_linked_map.get(uid, False)

        rows.append({
            "satellite_station_uid": uid,
            "source": "GSED",
            "resolution": res,
            "lat": lat,
            "lon": lon,
            "basin_id": basin_id if pd.notna(basin_id) else None,
            "link_status": link_status,
            "linked_cluster_uid": linked_cluster,
            "linked_resolution": linked_res,
            "nearest_main_cluster_uid": nn_uid,
            "nearest_main_resolution": nn_res,
            "nearest_main_distance_km": nn_dst,
            "same_reach": same_reach,
            "inside_any_main_cluster_basin": inside_any,
            "inside_linked_cluster_basin": inside_linked,
            "region_tag": region,
        })

    result = pd.DataFrame(rows)
    print("  Diagnostic table: {} rows".format(len(result)))
    return result


# ── Map generation (matplotlib + cartopy) ───────────────────────────────────


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
        ax.add_feature(cfeature.LAKES, color="#d0e8ff", edgecolor="#888888", linewidth=0.3, zorder=1)
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


def generate_global_overview(main_pts, gsed_pts, out_dir):
    """Figure A: global overview map."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    extent = (-180, -60, 180, 80)
    figsize = (14, 8)

    print("\n  Generating global_overview.png ...")

    if use_cartopy:
        proj = ccrs.PlateCarree(central_longitude=0)
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(1, 1, 1, projection=proj)
        _setup_map_ax_cartopy(ax, extent=extent, basemap=True)
    else:
        fig, ax = plt.subplots(figsize=figsize)
        _setup_map_ax_basic(ax, extent=extent)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    if main_pts is not None and len(main_pts) > 0:
        ax.scatter(
            main_pts["lon"].values, main_pts["lat"].values,
            s=3, c="#1976D2", alpha=0.6, edgecolors="none",
            **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
            label="Main clusters ({})".format(len(main_pts)),
            zorder=3,
        )

    if gsed_pts is not None:
        linked = gsed_pts[gsed_pts["link_status"] == "linked"]
        if len(linked) > 0:
            ax.scatter(
                linked["lon"].values, linked["lat"].values,
                s=30, marker="^", c="#388E3C", edgecolors="k", linewidth=0.5,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="GSED linked ({})".format(len(linked)),
                zorder=5,
            )

        unlinked = gsed_pts[gsed_pts["link_status"] != "linked"]
        if len(unlinked) > 0:
            ax.scatter(
                unlinked["lon"].values, unlinked["lat"].values,
                s=15, marker="v", c="#D32F2F", alpha=0.7, edgecolors="k", linewidth=0.3,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="GSED unlinked ({})".format(len(unlinked)),
                zorder=4,
            )

    ax.legend(loc="lower left", fontsize=9, framealpha=0.9,
              markerscale=1, handletextpad=0.5)
    ax.set_title("GSED Satellite — Main Cluster Spatial Overview", fontsize=13)

    out_path = out_dir / "global_overview.png"
    fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("    -> {}".format(out_path))


def _points_in_bbox(df, bbox):
    """Filter DataFrame rows whose (lat, lon) fall inside *bbox*."""
    minx, miny, maxx, maxy = bbox
    mask = df["lon"].between(minx, maxx) & df["lat"].between(miny, maxy)
    return df.loc[mask].copy()


def _buffer_circle_deg(lat, lon, radius_km, n_pts=36):
    """Return (lon_array, lat_array) of a circle in geographic degrees."""
    r_deg_lat = radius_km / 111.32
    r_deg_lon = radius_km / (111.32 * math.cos(math.radians(lat)))
    angles = np.linspace(0, 2 * math.pi, n_pts)
    circ_lon = lon + r_deg_lon * np.cos(angles)
    circ_lat = lat + r_deg_lat * np.sin(angles)
    return circ_lon, circ_lat


def generate_regional_zoom(name, bbox, main_pts, gsed_pts, merit_dir, out_dir):
    """Figure B: regional zoom map with MERIT reaches, buffers, link lines."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    print("\n  Generating {} ...".format(name))

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

    # MERIT reaches
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

    # GSED points in region
    if gsed_pts is not None and len(gsed_pts) > 0:
        g_in = _points_in_bbox(gsed_pts, bbox)
        linked = g_in[g_in["link_status"] == "linked"]
        unlinked = g_in[g_in["link_status"] != "linked"]

        if len(linked) > 0:
            ax.scatter(
                linked["lon"].values, linked["lat"].values,
                s=80, marker="^", c="#388E3C", edgecolors="k", linewidth=0.8,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="GSED linked", zorder=6,
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
                label="GSED unlinked", zorder=6,
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

        # 5 km circles
        for _, row_g in g_in.iterrows():
            circ = _buffer_circle_deg(row_g["lat"], row_g["lon"], 5.0)
            ax.plot(
                circ[0], circ[1],
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                color="#D32F2F" if row_g["link_status"] != "linked" else "#388E3C",
                linewidth=0.6, linestyle="--", alpha=0.5, zorder=3,
            )

    ax.legend(loc="lower left", fontsize=8, framealpha=0.9, markerscale=0.8)
    ax.set_title("{} — GSED spatial overlap".format(name), fontsize=12)

    if use_cartopy and bbox[2] - bbox[0] < 50:
        _add_scale_bar(ax, bbox, length_km=100)

    out_path = out_dir / "{}_zoom.png".format(name.lower())
    fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("    -> {}".format(out_path))


def generate_basin_diagnostics(gsed_df, main_pts, cluster_basins_dict, out_dir):
    """Figure C: basin polygon diagnostic maps for representative cases."""
    import matplotlib
    import os as _m
    if not "MPLBACKEND" in _m.environ and matplotlib.get_backend() != "Agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    use_cartopy = _has_cartopy()
    if use_cartopy:
        import cartopy.crs as ccrs

    print("\n  Generating basin diagnostic maps ...")

    if cluster_basins_dict is None or not cluster_basins_dict:
        print("    SKIP: no basin polygons available")
        return

    basin_layer = cluster_basins_dict.get("basin_daily")
    if basin_layer is None or len(basin_layer) == 0:
        print("    SKIP: basin_daily layer empty or missing")
        return

    cat_linked = gsed_df[gsed_df["link_status"] == "linked"]
    near_mask = (
        (gsed_df["link_status"] != "linked")
        & gsed_df["nearest_main_distance_km"].notna()
        & (gsed_df["nearest_main_distance_km"] <= 10.0)
    )
    cat_near = gsed_df.loc[near_mask]
    far_mask = (
        (gsed_df["link_status"] != "linked")
        & (gsed_df["nearest_main_distance_km"].isna()
           | (gsed_df["nearest_main_distance_km"] > 10.0))
    )
    cat_far = gsed_df.loc[far_mask]

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

            # Basin polygon
            if cluster_uid and _clean_text(cluster_uid):
                basin_sub = basin_layer.loc[
                    basin_layer["cluster_uid"] == cluster_uid
                ]
                if len(basin_sub) > 0:
                    basin_sub.plot(
                        ax=ax, facecolor=color, alpha=0.15,
                        edgecolor=color, linewidth=1.5,
                        **({} if not use_cartopy
                           else {"transform": ccrs.PlateCarree()}),
                        zorder=2,
                    )

            # Nearby MERIT reaches
            sub_bbox = (
                row["lon"] - 0.5, row["lat"] - 0.5,
                row["lon"] + 0.5, row["lat"] + 0.5,
            )
            merit_gdf_local, _ = _load_merit_reaches_for_bbox(
                Path(DEFAULT_MERIT_DIR), sub_bbox
            )
            if merit_gdf_local is not None and len(merit_gdf_local) > 0:
                merit_gdf_local.plot(
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

            # GSED point
            marker = "^" if row["link_status"] == "linked" else "v"
            ax.scatter(
                row["lon"], row["lat"],
                s=100, marker=marker, c=color, edgecolors="k", linewidth=0.8,
                **({} if not use_cartopy else {"transform": ccrs.PlateCarree()}),
                label="GSED ({})".format(cat_name), zorder=6,
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
                        color=color, linewidth=1.5, linestyle=ls, alpha=0.8, zorder=4,
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

            fname = "basin_diag_{}_{}.png".format(cat_name, idx_i)
            out_path = out_dir / fname
            fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
            plt.close(fig)
            print("    -> {}".format(out_path))


# ── Terminal summary ────────────────────────────────────────────────────────


def print_summary(diag):
    """Print terminal summary statistics."""
    print("\n" + "=" * 60)
    print("GSED SPATIAL DIAGNOSTIC SUMMARY")
    print("=" * 60)

    total = len(diag)
    linked = int(diag["link_status"].eq("linked").sum())
    unlinked = total - linked
    same_reach = int(diag["same_reach"].eq(True).sum())
    inside_any = int(diag["inside_any_main_cluster_basin"].eq(True).sum())
    unlinked_inside = int(
        diag.loc[diag["link_status"].ne("linked"),
                 "inside_any_main_cluster_basin"].sum()
    )
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

    print("  1. Total GSED stations:                 {:>7d}".format(total))
    print("  2. Linked GSED stations:                {:>7d}".format(linked))
    print("  3. Unlinked GSED stations:              {:>7d}".format(unlinked))
    print("  4. Same MERIT reach:                    {:>7d}".format(same_reach))
    print("  5. Inside any main cluster basin:       {:>7d}".format(inside_any))
    print("  6. Unlinked but inside any basin:       {:>7d}".format(unlinked_inside))
    print("  7. nearest_main_distance <=  5 km:      {:>7d}".format(near_5))
    print("     nearest_main_distance <= 10 km:      {:>7d}".format(near_10))
    print("     nearest_main_distance <= 25 km:      {:>7d}".format(near_25))
    print("=" * 60)


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

    cluster_summary, cluster_res_layers = load_cluster_points_gpkg(
        args.cluster_points_gpkg,
    )

    # Load basin GPKG (only needed columns for CSV-mode)
    cluster_basins_dict = {}
    bp = Path(args.cluster_basins_gpkg)
    load_full = not args.csv_only
    if load_full:
        cluster_basins_dict = load_cluster_basins_gpkg(str(bp))
    else:
        if bp.is_file():
            print("  Loading basin_daily (minimal) for point-in-polygon...")
            try:
                import geopandas as gpd
                daily = gpd.read_file(
                    str(bp), layer="basin_daily",
                    columns=["cluster_uid"],
                )
                cluster_basins_dict["basin_daily"] = daily
                print("    basin_daily: {} rows".format(len(daily)))
            except Exception as exc:
                print("    basin_daily: skipped ({})".format(exc))
        else:
            print("WARNING: cluster basins GPKG not found: {}".format(bp))

    cat_df = load_satellite_catalog(args.satellite_catalog_csv)

    gsed_raw = linkage_df[linkage_df["source"].str.upper() == "GSED"].copy()
    if len(gsed_raw) == 0:
        print("ERROR: no GSED records found in linkage CSV.")
        return 1
    print("  GSED records: {}".format(len(gsed_raw)))

    # ── 2. Nearest main cluster computation ───────────────────────────────
    if cluster_summary is not None and len(cluster_summary) > 0:
        nn_uids, nn_dists = compute_nearest_main(gsed_raw, cluster_summary)
        gsed_raw["nearest_main_cluster_uid"] = nn_uids
        gsed_raw["nearest_main_distance_km"] = nn_dists
    else:
        gsed_raw["nearest_main_cluster_uid"] = ""
        gsed_raw["nearest_main_distance_km"] = math.nan

    # ── 3. Build diagnostic table ─────────────────────────────────────────
    diag = build_diagnostic_table(
        gsed_raw, cluster_summary, cluster_basins_dict, cluster_res_layers,
    )

    csv_path = out_dir / "gsed_spatial_diagnostics.csv"
    diag.to_csv(csv_path, index=False)
    print("\n  Diagnostic CSV -> {}".format(csv_path))

    # ── 4. Print summary statistics ───────────────────────────────────────
    print_summary(diag)

    # ── 5. Generate maps (if not --csv-only) ──────────────────────────────
    if args.csv_only:
        print("\n--csv-only: maps skipped. Omit --csv-only to generate maps.")
        return 0

    try:
        import geopandas  # noqa: F401
        import cartopy  # noqa: F401
    except ImportError as exc:
        print("\nWARNING: map dependencies missing: {}".format(exc))
        return 0

    import matplotlib
    matplotlib.use("Agg")

    generate_global_overview(cluster_summary, gsed_raw, out_dir)

    merit_dir = Path(args.merit_dir)
    for region_name, region_bbox in REGIONS.items():
        generate_regional_zoom(
            region_name, region_bbox,
            cluster_summary, gsed_raw,
            merit_dir, out_dir,
        )

    generate_basin_diagnostics(
        gsed_raw, cluster_summary, cluster_basins_dict, out_dir,
    )

    print("\nDone. All outputs in {}".format(out_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
