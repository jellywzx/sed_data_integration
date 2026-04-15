#!/usr/bin/env python3
"""
s7: export cluster spatial layers for the basin mainline.

This step now depends on:
  - the master basin NetCDF; and
  - the daily / monthly / annual matrix NetCDF files.

Outputs:
  - a compatibility shapefile with one row per cluster_uid;
  - a multi-layer GPKG for GIS use;
  - two CSV catalogs reused by the release step.
"""

import argparse
import sys
from pathlib import Path

import numpy as np

from cluster_spatial_catalog import (
    HAS_GPD,
    HAS_NC,
    build_cluster_resolution_catalog,
    build_cluster_station_catalog,
    write_cluster_points_gpkg,
)
from pipeline_paths import (
    S6_MATRIX_DIR,
    S6_MERGED_NC,
    S7_CLUSTER_POINTS_GPKG,
    S7_CLUSTER_RESOLUTION_CATALOG_CSV,
    S7_CLUSTER_SHP,
    S7_CLUSTER_STATION_CATALOG_CSV,
    get_output_r_root,
)

try:
    import shapefile

    HAS_SHP = True
except ImportError:
    HAS_SHP = False


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_MASTER_NC = ROOT / S6_MERGED_NC
_DEFAULT_DAILY_NC = ROOT / S6_MATRIX_DIR / "s6_basin_matrix_daily.nc"
_DEFAULT_MONTHLY_NC = ROOT / S6_MATRIX_DIR / "s6_basin_matrix_monthly.nc"
_DEFAULT_ANNUAL_NC = ROOT / S6_MATRIX_DIR / "s6_basin_matrix_annual.nc"
_DEFAULT_SHP = ROOT / S7_CLUSTER_SHP
_DEFAULT_GPKG = ROOT / S7_CLUSTER_POINTS_GPKG
_DEFAULT_STATION_CATALOG = ROOT / S7_CLUSTER_STATION_CATALOG_CSV
_DEFAULT_RESOLUTION_CATALOG = ROOT / S7_CLUSTER_RESOLUTION_CATALOG_CSV

_WGS84_PRJ = (
    'GEOGCS["GCS_WGS_1984",'
    'DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],'
    'UNIT["Degree",0.0174532925199433]]'
)


def _safe_float(value, fill=-9999.0):
    try:
        val = float(value)
        return fill if not np.isfinite(val) else val
    except Exception:
        return fill


def _safe_int(value, fill=-9999):
    try:
        return int(value)
    except Exception:
        return fill


def _safe_str(value, maxlen=80):
    if value is None:
        return ""
    text = str(value).strip()
    return text[:maxlen]


def _write_cluster_summary_shp(station_catalog, out_path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    writer = shapefile.Writer(str(out_path), shapeType=shapefile.POINT)
    writer.field("cluster_uid", "C", size=12)
    writer.field("cluster_id", "N", size=10, decimal=0)
    writer.field("lat", "F", size=14, decimal=6)
    writer.field("lon", "F", size=14, decimal=6)
    writer.field("basin_area", "F", size=14, decimal=2)
    writer.field("pfaf_code", "F", size=10, decimal=1)
    writer.field("n_up_reach", "N", size=10, decimal=0)
    writer.field("match_qual", "N", size=5, decimal=0)
    writer.field("n_src_stn", "N", size=10, decimal=0)
    writer.field("stn_name", "C", size=80)
    writer.field("river_name", "C", size=80)
    writer.field("src_stn_id", "C", size=80)
    writer.field("avail_res", "C", size=40)
    writer.field("n_avail", "N", size=5, decimal=0)
    writer.field("d_cnt", "N", size=12, decimal=0)
    writer.field("m_cnt", "N", size=12, decimal=0)
    writer.field("a_cnt", "N", size=12, decimal=0)

    for row in station_catalog.itertuples(index=False):
        lon = _safe_float(row.lon)
        lat = _safe_float(row.lat)
        writer.point(lon, lat)
        writer.record(
            cluster_uid=_safe_str(row.cluster_uid, 12),
            cluster_id=_safe_int(row.cluster_id, -1),
            lat=lat,
            lon=lon,
            basin_area=_safe_float(row.basin_area),
            pfaf_code=_safe_float(row.pfaf_code),
            n_up_reach=_safe_int(row.n_upstream_reaches, -9999),
            match_qual=_safe_int(row.basin_match_quality_code, -1),
            n_src_stn=_safe_int(row.n_source_stations_in_cluster, 0),
            stn_name=_safe_str(row.station_name, 80),
            river_name=_safe_str(row.river_name, 80),
            src_stn_id=_safe_str(row.source_station_id, 80),
            avail_res=_safe_str(row.available_resolutions, 40),
            n_avail=_safe_int(row.n_available_resolutions, 0),
            d_cnt=_safe_int(row.daily_record_count, 0),
            m_cnt=_safe_int(row.monthly_record_count, 0),
            a_cnt=_safe_int(row.annual_record_count, 0),
        )

    writer.close()
    out_path.with_suffix(".prj").write_text(_WGS84_PRJ, encoding="utf-8")
    out_path.with_suffix(".cpg").write_text("UTF-8", encoding="utf-8")
    return out_path


def main():
    ap = argparse.ArgumentParser(
        description="s7: export cluster compatibility SHP plus multi-layer GPKG"
    )
    ap.add_argument(
        "--nc",
        "--master-nc",
        dest="master_nc",
        default=str(_DEFAULT_MASTER_NC),
        help="master basin NetCDF path",
    )
    ap.add_argument("--daily-nc", default=str(_DEFAULT_DAILY_NC), help="daily matrix NetCDF path")
    ap.add_argument("--monthly-nc", default=str(_DEFAULT_MONTHLY_NC), help="monthly matrix NetCDF path")
    ap.add_argument("--annual-nc", default=str(_DEFAULT_ANNUAL_NC), help="annual matrix NetCDF path")
    ap.add_argument("--out", default=str(_DEFAULT_SHP), help="compatibility shapefile path")
    ap.add_argument("--out-gpkg", default=str(_DEFAULT_GPKG), help="multi-layer cluster GPKG path")
    ap.add_argument(
        "--out-station-catalog",
        default=str(_DEFAULT_STATION_CATALOG),
        help="cluster station catalog CSV path",
    )
    ap.add_argument(
        "--out-resolution-catalog",
        default=str(_DEFAULT_RESOLUTION_CATALOG),
        help="cluster resolution catalog CSV path",
    )
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1
    if not HAS_GPD:
        print("Error: geopandas is required. pip install geopandas")
        return 1
    if not HAS_SHP:
        print("Error: pyshp is required. pip install pyshp")
        return 1

    master_nc = Path(args.master_nc).resolve()
    daily_nc = Path(args.daily_nc).resolve()
    monthly_nc = Path(args.monthly_nc).resolve()
    annual_nc = Path(args.annual_nc).resolve()
    out_shp = Path(args.out).resolve()
    out_gpkg = Path(args.out_gpkg).resolve()
    station_catalog_path = Path(args.out_station_catalog).resolve()
    resolution_catalog_path = Path(args.out_resolution_catalog).resolve()

    required_inputs = [master_nc, daily_nc, monthly_nc, annual_nc]
    missing = [str(path) for path in required_inputs if not path.is_file()]
    if missing:
        print("Error: required inputs missing:")
        for item in missing:
            print("  - {}".format(item))
        return 1

    station_catalog = build_cluster_station_catalog(
        master_nc,
        matrix_paths={
            "daily": daily_nc,
            "monthly": monthly_nc,
            "annual": annual_nc,
        },
    )
    resolution_catalog = build_cluster_resolution_catalog(station_catalog)

    station_catalog_path.parent.mkdir(parents=True, exist_ok=True)
    resolution_catalog_path.parent.mkdir(parents=True, exist_ok=True)
    station_catalog.to_csv(station_catalog_path, index=False)
    resolution_catalog.to_csv(resolution_catalog_path, index=False)

    shp_path = _write_cluster_summary_shp(station_catalog, out_shp)
    gpkg_path = write_cluster_points_gpkg(station_catalog, resolution_catalog, out_gpkg)

    print("Wrote compatibility SHP: {}".format(shp_path))
    print("Wrote cluster GPKG: {}".format(gpkg_path))
    print("Wrote cluster station catalog: {}".format(station_catalog_path))
    print("Wrote cluster resolution catalog: {}".format(resolution_catalog_path))
    print("cluster_summary rows = {}".format(len(station_catalog)))

    for resolution in ("daily", "monthly", "annual"):
        count = int((resolution_catalog["resolution"] == resolution).sum())
        print("cluster_{} rows = {}".format(resolution, count))

    print("Join key: cluster_uid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
