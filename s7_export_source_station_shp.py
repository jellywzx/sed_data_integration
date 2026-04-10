#!/usr/bin/env python3
"""
s7：从 s6 NC 导出原始 source-station 点 Shapefile。

每行代表一个原始站点，并通过 cluster_uid 与合并后的 cluster 连接。

输入：
  scripts_basin_test/output/s6_basin_merged_all.nc

输出：
  scripts_basin_test/output/s7_source_stations.shp
"""

import argparse
import sys
from pathlib import Path

import numpy as np

from pipeline_paths import (
    S6_MERGED_NC,
    S7_SOURCE_STATION_SHP,
    get_output_r_root,
)

try:
    import netCDF4 as nc4
    HAS_NC = True
except ImportError:
    HAS_NC = False

try:
    import shapefile
    HAS_SHP = True
except ImportError:
    HAS_SHP = False

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_NC = ROOT / S6_MERGED_NC
_DEFAULT_SHP = ROOT / S7_SOURCE_STATION_SHP

_WGS84_PRJ = (
    'GEOGCS["GCS_WGS_1984",'
    'DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],'
    'UNIT["Degree",0.0174532925199433]]'
)


def _safe_float(val, fill=-9999.0):
    try:
        if np.ma.is_masked(val):
            return fill
        v = float(val)
        return fill if np.isnan(v) else v
    except Exception:
        return fill


def _safe_int(val, fill=-9999):
    try:
        if np.ma.is_masked(val):
            return fill
        return int(val)
    except Exception:
        return fill


def _safe_str(val, maxlen=120):
    if val is None:
        return ""
    return str(val).strip()[:maxlen]


def main():
    ap = argparse.ArgumentParser(
        description="s7：从 s6 NC 导出 source-station 点 shapefile"
    )
    ap.add_argument("--nc", default=str(_DEFAULT_NC), help="s6 NC 文件路径")
    ap.add_argument("--out", default=str(_DEFAULT_SHP), help="输出 shapefile 路径")
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1
    if not HAS_SHP:
        print("Error: pyshp is required. pip install pyshp")
        return 1

    nc_path = Path(args.nc)
    out_path = Path(args.out)
    if not nc_path.is_file():
        print("Error: NC file not found: {}".format(nc_path))
        return 1

    with nc4.Dataset(nc_path, "r") as ds:
        if "n_source_stations" not in ds.dimensions:
            print("Error: n_source_stations dimension not found in {}".format(nc_path))
            return 1

        n = len(ds.dimensions["n_source_stations"])
        cluster_uids = [_safe_str(s, 12) for s in ds["cluster_uid"][:]] if "cluster_uid" in ds.variables else []
        source_names = [_safe_str(s, 40) for s in ds["source_name"][:]] if "source_name" in ds.variables else []

        src_uid = [_safe_str(s, 12) for s in ds["source_station_uid"][:]] if "source_station_uid" in ds.variables else [""] * n
        src_cluster_idx = ds["source_station_cluster_index"][:] if "source_station_cluster_index" in ds.variables else np.full(n, -1)
        src_source_idx = ds["source_station_source_index"][:] if "source_station_source_index" in ds.variables else np.full(n, -1)
        src_native_id = [_safe_str(s, 80) for s in ds["source_station_native_id"][:]] if "source_station_native_id" in ds.variables else [""] * n
        src_station_name = [_safe_str(s, 80) for s in ds["source_station_name"][:]] if "source_station_name" in ds.variables else [""] * n
        src_river_name = [_safe_str(s, 80) for s in ds["source_station_river_name"][:]] if "source_station_river_name" in ds.variables else [""] * n
        src_lats = ds["source_station_lat"][:] if "source_station_lat" in ds.variables else np.full(n, -9999.0)
        src_lons = ds["source_station_lon"][:] if "source_station_lon" in ds.variables else np.full(n, -9999.0)
        src_resolutions = [_safe_str(s, 120) for s in ds["source_station_resolutions"][:]] if "source_station_resolutions" in ds.variables else [""] * n

    out_path.parent.mkdir(parents=True, exist_ok=True)

    w = shapefile.Writer(str(out_path), shapeType=shapefile.POINT)
    w.field("src_uid", "C", size=12)
    w.field("cluster_uid", "C", size=12)
    w.field("src_name", "C", size=40)
    w.field("src_stn_id", "C", size=80)
    w.field("stn_name", "C", size=80)
    w.field("river_name", "C", size=80)
    w.field("lat", "F", size=14, decimal=6)
    w.field("lon", "F", size=14, decimal=6)
    w.field("resols", "C", size=120)

    for i in range(n):
        lon_f = _safe_float(src_lons[i])
        lat_f = _safe_float(src_lats[i])
        cluster_idx = _safe_int(src_cluster_idx[i], fill=-1)
        source_idx = _safe_int(src_source_idx[i], fill=-1)
        cluster_uid = cluster_uids[cluster_idx] if 0 <= cluster_idx < len(cluster_uids) else ""
        source_name = source_names[source_idx] if 0 <= source_idx < len(source_names) else ""

        w.point(lon_f, lat_f)
        w.record(
            src_uid=src_uid[i],
            cluster_uid=cluster_uid,
            src_name=source_name,
            src_stn_id=src_native_id[i],
            stn_name=src_station_name[i],
            river_name=src_river_name[i],
            lat=lat_f,
            lon=lon_f,
            resols=src_resolutions[i],
        )

    w.close()
    out_path.with_suffix(".prj").write_text(_WGS84_PRJ)
    out_path.with_suffix(".cpg").write_text("UTF-8")

    print("Wrote {} source stations -> {}".format(n, out_path))
    print("Join key: cluster_uid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
