#!/usr/bin/env python3
"""Plot spatial coverage figures from the s8 release package.

This script is intentionally release-product oriented: it reads the files that
s8_publish_reference_dataset.py publishes under sed_reference_release/ rather
than upstream intermediate outputs.

Figures produced by default:
  1. sed_reference_climatology spatial distribution from the published NetCDF;
  2. sed_reference_satellite_validation spatial distribution from the published NetCDF;
  3. main release product cluster map showing resolved/unresolved points, with
     optional basin polygon sidecar overlay.

The basin polygon sidecar should only contain records with basin_status=resolved
and a valid exported basin polygon. The script treats polygons as optional and
never assumes every resolved cluster has a published polygon.
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_SCRIPT_DIR = SCRIPT_DIR.parent
if str(PROJECT_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_SCRIPT_DIR))

from pipeline_paths import (  # noqa: E402
    RELEASE_CLUSTER_BASINS_GPKG,
    RELEASE_CLUSTER_POINTS_GPKG,
    RELEASE_CLIMATOLOGY_NC,
    RELEASE_DATASET_DIR,
    RELEASE_SATELLITE_VALIDATION_NC,
    RELEASE_STATION_CATALOG_CSV,
    get_output_r_root,
)

try:
    import matplotlib.pyplot as plt

    HAS_MPL = True
except ImportError:  # pragma: no cover - runtime dependency check
    plt = None
    HAS_MPL = False

try:
    import geopandas as gpd

    HAS_GPD = True
except ImportError:  # pragma: no cover - runtime dependency check
    gpd = None
    HAS_GPD = False

try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:  # pragma: no cover - optional dependency check
    xr = None
    HAS_XARRAY = False

try:
    import netCDF4 as nc4

    HAS_NETCDF4 = True
except ImportError:  # pragma: no cover - optional dependency check
    nc4 = None
    HAS_NETCDF4 = False

ROOT = get_output_r_root(PROJECT_SCRIPT_DIR)
DEFAULT_RELEASE_DIR = ROOT / RELEASE_DATASET_DIR
DEFAULT_CLIMATOLOGY_NC = ROOT / RELEASE_CLIMATOLOGY_NC
DEFAULT_SATELLITE_NC = ROOT / RELEASE_SATELLITE_VALIDATION_NC
DEFAULT_STATION_CATALOG = ROOT / RELEASE_STATION_CATALOG_CSV
DEFAULT_CLUSTER_POINTS_GPKG = ROOT / RELEASE_CLUSTER_POINTS_GPKG
DEFAULT_CLUSTER_BASINS_GPKG = ROOT / RELEASE_CLUSTER_BASINS_GPKG
DEFAULT_FIGURES_DIR = DEFAULT_RELEASE_DIR / "figures"

LAT_CANDIDATES = (
    "lat",
    "latitude",
    "station_lat",
    "station_latitude",
    "source_station_lat",
    "cluster_lat",
    "y",
)
LON_CANDIDATES = (
    "lon",
    "longitude",
    "station_lon",
    "station_longitude",
    "source_station_lon",
    "cluster_lon",
    "x",
)
STATUS_CANDIDATES = ("basin_status", "status", "cluster_status")
UID_CANDIDATES = ("cluster_uid", "cluster_id", "cluster_key")


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


def first_col(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    columns = list(columns)
    lower = {str(c).lower(): c for c in columns}
    for col in candidates:
        if col in columns:
            return col
        if col.lower() in lower:
            return lower[col.lower()]
    return None


def valid_latlon(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
    return np.isfinite(lat) & np.isfinite(lon) & (lat >= -90) & (lat <= 90) & (lon >= -180) & (lon <= 180)


def flatten_pair(lat: np.ndarray, lon: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    lat = np.asarray(lat, dtype="float64")
    lon = np.asarray(lon, dtype="float64")
    if lat.ndim == 1 and lon.ndim == 1 and lat.size != lon.size:
        lon_grid, lat_grid = np.meshgrid(lon, lat)
        return lat_grid.ravel(), lon_grid.ravel()
    try:
        lat_b, lon_b = np.broadcast_arrays(lat, lon)
    except ValueError:
        n = min(lat.size, lon.size)
        return lat.ravel()[:n], lon.ravel()[:n]
    return lat_b.ravel(), lon_b.ravel()


def read_netcdf_points_xarray(path: Path, lat_name: Optional[str], lon_name: Optional[str]) -> pd.DataFrame:
    with xr.open_dataset(path, decode_times=False) as ds:
        lat_name = lat_name or first_col(ds.variables, LAT_CANDIDATES)
        lon_name = lon_name or first_col(ds.variables, LON_CANDIDATES)
        if not lat_name or not lon_name:
            raise ValueError("Could not find latitude/longitude variables in {}".format(path))
        lat, lon = flatten_pair(ds[lat_name].values, ds[lon_name].values)
        keep = valid_latlon(lat, lon)
        out = pd.DataFrame({"lat": lat[keep], "lon": lon[keep]})
        out["source_file"] = path.name
        out["lat_var"] = lat_name
        out["lon_var"] = lon_name
        return out.drop_duplicates(["lat", "lon"]).reset_index(drop=True)


def read_netcdf_points_netcdf4(path: Path, lat_name: Optional[str], lon_name: Optional[str]) -> pd.DataFrame:
    with nc4.Dataset(str(path), "r") as ds:
        names = list(ds.variables.keys())
        lat_name = lat_name or first_col(names, LAT_CANDIDATES)
        lon_name = lon_name or first_col(names, LON_CANDIDATES)
        if not lat_name or not lon_name:
            raise ValueError("Could not find latitude/longitude variables in {}".format(path))
        lat, lon = flatten_pair(np.asarray(ds.variables[lat_name][:]), np.asarray(ds.variables[lon_name][:]))
        keep = valid_latlon(lat, lon)
        out = pd.DataFrame({"lat": lat[keep], "lon": lon[keep]})
        out["source_file"] = path.name
        out["lat_var"] = lat_name
        out["lon_var"] = lon_name
        return out.drop_duplicates(["lat", "lon"]).reset_index(drop=True)


def read_netcdf_points(path: Path, lat_name: Optional[str] = None, lon_name: Optional[str] = None) -> pd.DataFrame:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError("NetCDF input not found: {}".format(path))
    if HAS_XARRAY:
        return read_netcdf_points_xarray(path, lat_name, lon_name)
    if HAS_NETCDF4:
        return read_netcdf_points_netcdf4(path, lat_name, lon_name)
    raise ImportError("Install xarray or netCDF4 to read NetCDF release products")


def maybe_world_boundaries(world_boundaries: str):
    if not world_boundaries or not HAS_GPD:
        return None
    path = Path(world_boundaries)
    if not path.is_file():
        print("Warning: world boundaries not found: {}".format(path))
        return None
    try:
        world = gpd.read_file(path)
        return world.set_crs("EPSG:4326") if world.crs is None else world.to_crs("EPSG:4326")
    except Exception as exc:
        print("Warning: failed to read world boundaries {}: {}".format(path, exc))
        return None


def setup_map_axes(ax, title: str, world=None) -> None:
    if world is not None:
        try:
            world.boundary.plot(ax=ax, linewidth=0.3)
        except Exception as exc:
            print("Warning: failed to draw world boundaries: {}".format(exc))
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(title)
    ax.grid(True, linewidth=0.3, alpha=0.35)


def save_point_map(points: pd.DataFrame, path: Path, title: str, label: str, world=None, marker_size: float = 8.0) -> None:
    if not HAS_MPL:
        raise ImportError("Install matplotlib to create figures")
    if points.empty:
        print("Warning: no valid coordinates for {}; skipping {}".format(title, path))
        return
    fig, ax = plt.subplots(figsize=(11, 5.5))
    setup_map_axes(ax, title, world=world)
    ax.scatter(points["lon"], points["lat"], s=marker_size, alpha=0.55, label=label)
    ax.legend(loc="lower left", markerscale=2)
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def read_cluster_points_from_catalog(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError("Station catalog not found: {}".format(path))
    df = pd.read_csv(path, keep_default_na=False)
    lat_col = first_col(df.columns, LAT_CANDIDATES)
    lon_col = first_col(df.columns, LON_CANDIDATES)
    if not lat_col or not lon_col:
        raise ValueError("Could not find latitude/longitude columns in {}".format(path))
    out = df.copy()
    out["lat"] = pd.to_numeric(out[lat_col], errors="coerce")
    out["lon"] = pd.to_numeric(out[lon_col], errors="coerce")
    status_col = first_col(out.columns, STATUS_CANDIDATES)
    if status_col:
        out["basin_status"] = out[status_col].map(clean_text).str.lower()
    else:
        out["basin_status"] = "unknown"
    uid_col = first_col(out.columns, UID_CANDIDATES)
    out["cluster_key"] = out[uid_col].map(clean_text) if uid_col else ["row_index:{}".format(i) for i in range(len(out))]
    keep = valid_latlon(out["lat"].to_numpy(dtype="float64"), out["lon"].to_numpy(dtype="float64"))
    return out.loc[keep].copy()


def read_cluster_points(path: Path, fallback_catalog: Path) -> pd.DataFrame:
    path = Path(path)
    if path.is_file() and HAS_GPD:
        try:
            gdf = gpd.read_file(path)
            if gdf.empty or gdf.geometry.is_empty.all():
                raise ValueError("empty point layer")
            gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
            geom = gdf.geometry
            gdf["lon"] = geom.x
            gdf["lat"] = geom.y
            status_col = first_col(gdf.columns, STATUS_CANDIDATES)
            gdf["basin_status"] = gdf[status_col].map(clean_text).str.lower() if status_col else "unknown"
            uid_col = first_col(gdf.columns, UID_CANDIDATES)
            gdf["cluster_key"] = gdf[uid_col].map(clean_text) if uid_col else ["row_index:{}".format(i) for i in range(len(gdf))]
            keep = valid_latlon(gdf["lat"].to_numpy(dtype="float64"), gdf["lon"].to_numpy(dtype="float64"))
            return pd.DataFrame(gdf.loc[keep].drop(columns="geometry"))
        except Exception as exc:
            print("Warning: failed to read cluster points GPKG {}; using catalog fallback: {}".format(path, exc))
    elif path.is_file() and not HAS_GPD:
        print("Warning: geopandas unavailable; using station catalog for cluster points")
    return read_cluster_points_from_catalog(fallback_catalog)


def read_basin_polygons(path: Path):
    path = Path(path)
    if not path.is_file():
        print("Warning: basin polygon sidecar not found; polygon overlay will be skipped: {}".format(path))
        return None
    if not HAS_GPD:
        print("Warning: geopandas unavailable; polygon overlay will be skipped")
        return None
    try:
        gdf = gpd.read_file(path)
        if gdf.empty:
            print("Warning: basin polygon sidecar is empty: {}".format(path))
            return None
        gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
        if "basin_status" in gdf.columns:
            unexpected = gdf[~gdf["basin_status"].map(clean_text).str.lower().eq("resolved")]
            if len(unexpected):
                print("Warning: basin sidecar includes {} non-resolved rows".format(len(unexpected)))
        return gdf
    except Exception as exc:
        print("Warning: failed to read basin polygon sidecar {}: {}".format(path, exc))
        return None


def plot_release_cluster_map(points: pd.DataFrame, path: Path, basins=None, world=None) -> None:
    if not HAS_MPL:
        raise ImportError("Install matplotlib to create figures")
    if points.empty:
        print("Warning: no valid cluster coordinates; skipping {}".format(path))
        return
    fig, ax = plt.subplots(figsize=(11, 5.5))
    setup_map_axes(ax, "S8 release main product: cluster status and basin polygon sidecar", world=world)
    if basins is not None and len(basins):
        try:
            basins.boundary.plot(ax=ax, linewidth=0.45, alpha=0.35, label="published basin polygon sidecar")
        except TypeError:
            basins.boundary.plot(ax=ax, linewidth=0.45, alpha=0.35)
    status = points["basin_status"].fillna("unknown").astype(str).str.lower()
    resolved = points[status.eq("resolved")]
    unresolved = points[~status.eq("resolved")]
    if len(unresolved):
        ax.scatter(unresolved["lon"], unresolved["lat"], s=8, alpha=0.45, label="unresolved/other clusters")
    if len(resolved):
        ax.scatter(resolved["lon"], resolved["lat"], s=8, alpha=0.65, label="resolved clusters")
    ax.legend(loc="lower left", markerscale=2)
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def write_plot_manifest(path: Path, rows: Sequence[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot spatial maps from the s8 sed_reference_release products.")
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR), help="Published s8 release directory")
    parser.add_argument("--climatology-nc", default=str(DEFAULT_CLIMATOLOGY_NC))
    parser.add_argument("--satellite-nc", default=str(DEFAULT_SATELLITE_NC))
    parser.add_argument("--station-catalog", default=str(DEFAULT_STATION_CATALOG))
    parser.add_argument("--cluster-points-gpkg", default=str(DEFAULT_CLUSTER_POINTS_GPKG))
    parser.add_argument("--cluster-basins-gpkg", default=str(DEFAULT_CLUSTER_BASINS_GPKG))
    parser.add_argument("--figures-dir", default=str(DEFAULT_FIGURES_DIR))
    parser.add_argument("--world-boundaries", default="", help="Optional country/continent boundary file to draw as context")
    parser.add_argument("--climatology-lat-var", default="")
    parser.add_argument("--climatology-lon-var", default="")
    parser.add_argument("--satellite-lat-var", default="")
    parser.add_argument("--satellite-lon-var", default="")
    parser.add_argument("--skip-nc", action="store_true", help="Skip climatology and satellite NetCDF maps")
    parser.add_argument("--skip-basins", action="store_true", help="Do not overlay the optional basin polygon sidecar")
    args = parser.parse_args()

    figures_dir = Path(args.figures_dir).resolve()
    figures_dir.mkdir(parents=True, exist_ok=True)
    world = maybe_world_boundaries(args.world_boundaries.strip())
    manifest = []

    if not args.skip_nc:
        clim = read_netcdf_points(
            Path(args.climatology_nc),
            args.climatology_lat_var.strip() or None,
            args.climatology_lon_var.strip() or None,
        )
        clim_path = figures_dir / "fig_s8_climatology_spatial_distribution.png"
        save_point_map(clim, clim_path, "S8 release climatology NetCDF spatial distribution", "climatology records", world=world)
        manifest.append({"figure": clim_path.name, "source": str(Path(args.climatology_nc)), "n_points": len(clim), "notes": "unique valid lat/lon coordinates"})

        sat = read_netcdf_points(
            Path(args.satellite_nc),
            args.satellite_lat_var.strip() or None,
            args.satellite_lon_var.strip() or None,
        )
        sat_path = figures_dir / "fig_s8_satellite_validation_spatial_distribution.png"
        save_point_map(sat, sat_path, "S8 release satellite-validation NetCDF spatial distribution", "satellite-validation records", world=world)
        manifest.append({"figure": sat_path.name, "source": str(Path(args.satellite_nc)), "n_points": len(sat), "notes": "unique valid lat/lon coordinates"})

    points = read_cluster_points(Path(args.cluster_points_gpkg), Path(args.station_catalog))
    basins = None if args.skip_basins else read_basin_polygons(Path(args.cluster_basins_gpkg))
    main_path = figures_dir / "fig_s8_release_cluster_status_and_basins.png"
    plot_release_cluster_map(points, main_path, basins=basins, world=world)
    manifest.append({
        "figure": main_path.name,
        "source": "{}; {}".format(Path(args.cluster_points_gpkg), Path(args.cluster_basins_gpkg)),
        "n_points": len(points),
        "notes": "cluster points colored by basin_status; basin polygon sidecar is optional and expected only for resolved records with valid polygons",
    })

    manifest_path = figures_dir / "s8_release_spatial_plot_manifest.csv"
    write_plot_manifest(manifest_path, manifest)
    print("Wrote S8 release spatial figures to {}".format(figures_dir))
    print("Wrote manifest: {}".format(manifest_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
