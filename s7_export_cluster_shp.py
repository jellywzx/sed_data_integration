#!/usr/bin/env python3
"""
s7：从 s6 NC 导出 cluster 点 Shapefile。

每行代表一个虚拟站点（cluster），几何为代表站点坐标（点）。
cluster_uid（格式：SED000042）作为 NC ↔ SHP 跨文件连接键。

输入：
  scripts_basin_test/output/s6_basin_merged_all.nc（s6 输出）

输出：
  scripts_basin_test/output/s7_cluster_stations.shp（及 .dbf / .prj / .shx / .cpg）

依赖：
  pip install pyshp netCDF4

用法：
  python s7_export_cluster_shp.py
  python s7_export_cluster_shp.py --nc /path/to/s6.nc --out /path/to/out.shp
"""

import argparse
import sys
from pathlib import Path

import numpy as np

from pipeline_paths import S6_MERGED_NC, S7_CLUSTER_SHP, get_output_r_root

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
ROOT       = get_output_r_root(SCRIPT_DIR)

_DEFAULT_NC  = ROOT / S6_MERGED_NC
_DEFAULT_SHP = ROOT / S7_CLUSTER_SHP

# WGS84 投影定义（写入 .prj）
_WGS84_PRJ = (
    'GEOGCS["GCS_WGS_1984",'
    'DATUM["D_WGS_1984",'
    'SPHEROID["WGS_1984",6378137.0,298.257223563]],'
    'PRIMEM["Greenwich",0.0],'
    'UNIT["Degree",0.0174532925199433]]'
)


def _safe_float(val, fill=-9999.0):
    """将 masked / nan 值统一转为 fill，其余转 float。"""
    try:
        if np.ma.is_masked(val):
            return fill
        v = float(val)
        return fill if np.isnan(v) else v
    except Exception:
        return fill


def _safe_int(val, fill=-9999):
    """将 masked / nan 值统一转为 fill，其余转 int。"""
    try:
        if np.ma.is_masked(val):
            return fill
        return int(val)
    except Exception:
        return fill


def _safe_str(val, maxlen=80):
    """将 NC 字符串变量值安全转为 Python str，截断至 maxlen 字符。"""
    if val is None:
        return ""
    s = str(val).strip()
    return s[:maxlen]


def main():
    ap = argparse.ArgumentParser(
        description="s7：从 s6 NC 导出 cluster 点 shapefile，cluster_uid 作为跨文件连接键"
    )
    ap.add_argument("--nc",  default=str(_DEFAULT_NC),
                    help="s6 NC 文件路径。默认: {}".format(_DEFAULT_NC))
    ap.add_argument("--out", default=str(_DEFAULT_SHP),
                    help="输出 shapefile 路径（.shp）。默认: {}".format(_DEFAULT_SHP))
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1
    if not HAS_SHP:
        print("Error: pyshp is required. pip install pyshp")
        return 1

    nc_path  = Path(args.nc)
    out_path = Path(args.out)

    if not nc_path.is_file():
        print("Error: NC file not found: {}".format(nc_path))
        return 1

    # ── 读取 NC n_stations 变量 ───────────────────────────────────────────
    print("Reading NC: {}".format(nc_path))
    with nc4.Dataset(nc_path, "r") as ds:
        n = len(ds.dimensions["n_stations"])
        lats        = ds["lat"][:]
        lons        = ds["lon"][:]
        cluster_ids = ds["cluster_id"][:]

        # cluster_uid：新增变量（旧 NC 可能没有，则自动构造）
        if "cluster_uid" in ds.variables:
            cluster_uids = [_safe_str(s, 12) for s in ds["cluster_uid"][:]]
        else:
            print("Warning: 'cluster_uid' not found in NC; constructing from cluster_id")
            cluster_uids = ["SED{:06d}".format(int(cid)) for cid in cluster_ids]

        basin_areas = ds["basin_area"][:]   if "basin_area"          in ds.variables else np.full(n, -9999.0)
        pfaf_codes  = ds["pfaf_code"][:]    if "pfaf_code"           in ds.variables else np.full(n, -9999.0)
        n_reaches   = ds["n_upstream_reaches"][:] if "n_upstream_reaches" in ds.variables else np.full(n, -9999)
        match_qual  = ds["basin_match_quality"][:] if "basin_match_quality" in ds.variables else np.full(n, -1)
        n_src_stn   = ds["n_source_stations_in_cluster"][:] if "n_source_stations_in_cluster" in ds.variables else np.full(n, -9999)
        stn_names   = [_safe_str(s) for s in ds["station_name"][:]]  if "station_name"   in ds.variables else [""] * n
        river_names = [_safe_str(s) for s in ds["river_name"][:]]    if "river_name"     in ds.variables else [""] * n
        src_ids     = [_safe_str(s) for s in ds["source_station_id"][:]] if "source_station_id" in ds.variables else [""] * n

    print("n_stations = {}".format(n))

    # ── 写 Shapefile ──────────────────────────────────────────────────────
    out_path.parent.mkdir(parents=True, exist_ok=True)

    w = shapefile.Writer(str(out_path), shapeType=shapefile.POINT)
    w.field("cluster_uid", "C", size=12)
    w.field("cluster_id",  "N", size=10, decimal=0)
    w.field("lat",         "F", size=14, decimal=6)
    w.field("lon",         "F", size=14, decimal=6)
    w.field("basin_area",  "F", size=14, decimal=2)
    w.field("pfaf_code",   "F", size=10, decimal=1)
    w.field("n_up_reach",  "N", size=10, decimal=0)
    w.field("match_qual",  "N", size=5,  decimal=0)
    w.field("n_src_stn",   "N", size=10, decimal=0)
    w.field("stn_name",    "C", size=80)
    w.field("river_name",  "C", size=80)
    w.field("src_stn_id",  "C", size=80)

    for i in range(n):
        lon_f = _safe_float(lons[i])
        lat_f = _safe_float(lats[i])
        w.point(lon_f, lat_f)
        w.record(
            cluster_uid = cluster_uids[i],
            cluster_id  = _safe_int(cluster_ids[i]),
            lat         = lat_f,
            lon         = lon_f,
            basin_area  = _safe_float(basin_areas[i]),
            pfaf_code   = _safe_float(pfaf_codes[i]),
            n_up_reach  = _safe_int(n_reaches[i]),
            match_qual  = _safe_int(match_qual[i]),
            n_src_stn   = _safe_int(n_src_stn[i]),
            stn_name    = stn_names[i],
            river_name  = river_names[i],
            src_stn_id  = src_ids[i],
        )

    w.close()

    # 写 .prj（WGS84）
    out_path.with_suffix(".prj").write_text(_WGS84_PRJ)

    # 写 .cpg（UTF-8 编码声明，GIS 软件正确解析中文/特殊字符）
    out_path.with_suffix(".cpg").write_text("UTF-8")

    print("Wrote {} records → {}".format(n, out_path))
    print("Files: {}.shp / .dbf / .shx / .prj / .cpg".format(out_path.stem))
    print("\nJoin key: cluster_uid  (matches NC variable 'cluster_uid')")
    print("Example:  SED000042  ↔  nc['cluster_uid'][i] == 'SED000042'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
