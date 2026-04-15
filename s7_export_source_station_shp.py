#!/usr/bin/env python3
"""
s7: export source-station spatial layers for the basin mainline.

This step now depends on the master basin NetCDF and expands source stations
by the resolutions that are actually retained in the final mainline records.

Outputs:
  - a resolution-aware source-station catalog CSV;
  - a multi-layer GPKG with source_daily / source_monthly / source_annual.
"""

import argparse
import sys
from pathlib import Path

from cluster_spatial_catalog import (
    CLUSTER_RESOLUTIONS,
    HAS_GPD,
    HAS_NC,
    build_source_station_resolution_catalog,
    write_source_stations_gpkg,
)
from pipeline_paths import (
    S6_MERGED_NC,
    S7_SOURCE_STATIONS_GPKG,
    S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV,
    get_output_r_root,
)


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_MASTER_NC = ROOT / S6_MERGED_NC
_DEFAULT_GPKG = ROOT / S7_SOURCE_STATIONS_GPKG
_DEFAULT_CATALOG = ROOT / S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV


def main():
    ap = argparse.ArgumentParser(
        description="s7: export resolution-aware source-station catalogs and multi-layer GPKG"
    )
    ap.add_argument(
        "--nc",
        "--master-nc",
        dest="master_nc",
        default=str(_DEFAULT_MASTER_NC),
        help="master basin NetCDF path",
    )
    ap.add_argument(
        "--out",
        default=str(_DEFAULT_GPKG),
        help="output multi-layer source-station GPKG path",
    )
    ap.add_argument(
        "--out-catalog",
        default=str(_DEFAULT_CATALOG),
        help="output source-station resolution catalog CSV path",
    )
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1
    if not HAS_GPD:
        print("Error: geopandas is required. pip install geopandas")
        return 1

    master_nc = Path(args.master_nc).resolve()
    out_gpkg = Path(args.out).resolve()
    out_catalog = Path(args.out_catalog).resolve()

    if not master_nc.is_file():
        print("Error: master NC not found: {}".format(master_nc))
        return 1
    if out_gpkg.suffix.lower() != ".gpkg":
        print("Error: --out must be a .gpkg path for multi-layer source output")
        return 1

    source_catalog = build_source_station_resolution_catalog(master_nc)
    out_catalog.parent.mkdir(parents=True, exist_ok=True)
    source_catalog.to_csv(out_catalog, index=False)
    gpkg_path = write_source_stations_gpkg(source_catalog, out_gpkg)

    print("Wrote source-station GPKG: {}".format(gpkg_path))
    print("Wrote source-station resolution catalog: {}".format(out_catalog))
    print("source_station_resolution rows = {}".format(len(source_catalog)))
    for resolution in CLUSTER_RESOLUTIONS:
        count = int((source_catalog["resolution"] == resolution).sum())
        print("source_{} rows = {}".format(resolution, count))
    print("Join keys: source_station_uid + resolution ; cluster_uid + resolution")
    return 0


if __name__ == "__main__":
    sys.exit(main())
