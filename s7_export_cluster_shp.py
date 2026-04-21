#!/usr/bin/env python3
"""
s7: export cluster spatial layers for the basin mainline.

This step now depends on:
  - the master basin NetCDF; and
  - the daily / monthly / annual matrix NetCDF files.

Outputs:
  - a multi-layer GPKG for GIS use;
  - two CSV catalogs reused by the release step.
"""

import argparse
import sys
from pathlib import Path

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
    S7_CLUSTER_STATION_CATALOG_CSV,
    get_output_r_root,
)


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_MASTER_NC = ROOT / S6_MERGED_NC
_DEFAULT_DAILY_NC = ROOT / S6_MATRIX_DIR / "s6_basin_matrix_daily.nc"
_DEFAULT_MONTHLY_NC = ROOT / S6_MATRIX_DIR / "s6_basin_matrix_monthly.nc"
_DEFAULT_ANNUAL_NC = ROOT / S6_MATRIX_DIR / "s6_basin_matrix_annual.nc"
_DEFAULT_GPKG = ROOT / S7_CLUSTER_POINTS_GPKG
_DEFAULT_STATION_CATALOG = ROOT / S7_CLUSTER_STATION_CATALOG_CSV
_DEFAULT_RESOLUTION_CATALOG = ROOT / S7_CLUSTER_RESOLUTION_CATALOG_CSV


def main():
    ap = argparse.ArgumentParser(
        description="s7: export cluster multi-layer GPKG plus CSV catalogs"
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

    master_nc = Path(args.master_nc).resolve()
    daily_nc = Path(args.daily_nc).resolve()
    monthly_nc = Path(args.monthly_nc).resolve()
    annual_nc = Path(args.annual_nc).resolve()
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

    gpkg_path = write_cluster_points_gpkg(station_catalog, resolution_catalog, out_gpkg)

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
