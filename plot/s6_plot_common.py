#!/usr/bin/env python3
"""
步骤 s6（出图）公共模块：路径常量与从 s8_merged_all.nc / s6_plot_stats.csv 加载站点统计。
供 plot/plot_merged_stations_*.py 与 s6_plot_merged_nc_stations.py 复用。

默认路径（步骤对应，均位于 output/ 下无子文件夹）：
  DEFAULT_NC：output/s8_merged_all.nc（步骤 8 输出）
  DEFAULT_INPUT_DIR：output（含 s4_clustered_stations.csv，用于按来源出图）
  DEFAULT_OUT_DIR：output（出图输出，文件名为 s6_plot_*.png / s6_plot_*.csv）
"""

import csv
from pathlib import Path

import numpy as np

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_R_ROOT = SCRIPT_DIR.parent  # Output_r（脚本在 scripts/ 下）

DEFAULT_NC = OUTPUT_R_ROOT / "output/s8_merged_all.nc"
DEFAULT_INPUT_DIR = OUTPUT_R_ROOT / "output"
DEFAULT_OUT_DIR = OUTPUT_R_ROOT / "output"


def load_station_stats(nc_path=None, csv_path=None, out_dir=None):
    """
    从 merged_all.nc 或已有 stats CSV 加载站点统计。
    返回 dict: lat, lon, rec_count, span_years, median_interval_days, n_stations。
    若从 nc 加载且 out_dir 给定，会写出 s6_plot_stats.csv 到 out_dir。
    """
    if csv_path and Path(csv_path).is_file():
        with open(csv_path) as f:
            r = csv.DictReader(f)
            rows = list(r)
        lat = np.array([float(x["lat"]) for x in rows])
        lon = np.array([float(x["lon"]) for x in rows])
        rec_count = np.array([int(x["n_records"]) for x in rows])
        span_years = np.array([float(x["span_years"]) for x in rows])
        if rows and "median_interval_days" in rows[0]:
            median_interval_days = np.array([
                float(x["median_interval_days"]) if x["median_interval_days"].strip() else np.nan
                for x in rows
            ])
        else:
            median_interval_days = np.full(len(lat), np.nan)
        n_stations = len(lat)
        return {
            "lat": lat, "lon": lon, "rec_count": rec_count,
            "span_years": span_years, "median_interval_days": median_interval_days,
            "n_stations": n_stations,
        }

    if nc4 is None:
        raise RuntimeError("netCDF4 is required when loading from NC.")
    nc_path = Path(nc_path or DEFAULT_NC)
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    with nc4.Dataset(nc_path, "r") as nc:
        lat = np.asarray(nc.variables["lat"][:])
        lon = np.asarray(nc.variables["lon"][:])
        sid = np.asarray(nc.variables["station_index"][:])
        time = np.asarray(nc.variables["time"][:])

    n_stations = len(lat)
    n_records = len(sid)
    rec_count = np.bincount(sid, minlength=n_stations)

    order = np.argsort(sid)
    sid_sorted = sid[order]
    time_sorted = time[order]
    boundaries = np.concatenate([[0], np.where(np.diff(sid_sorted) != 0)[0] + 1, [len(sid_sorted)]])
    station_ids_in_order = sid_sorted[boundaries[:-1]]
    t_min = np.full(n_stations, np.nan)
    t_max = np.full(n_stations, np.nan)
    for k in range(len(boundaries) - 1):
        i = station_ids_in_order[k]
        seg = time_sorted[boundaries[k] : boundaries[k + 1]]
        t_valid = seg[(seg > -1e9) & (seg < 1e9)]
        if len(t_valid) > 0:
            t_min[i] = np.min(t_valid)
            t_max[i] = np.max(t_valid)

    span_days = np.where(np.isnan(t_min) | np.isnan(t_max), np.nan, t_max - t_min)
    span_years = span_days / 365.25
    span_years = np.where(np.isnan(span_years), 0.0, span_years)

    median_interval_days = np.full(n_stations, np.nan)
    for k in range(len(boundaries) - 1):
        i = station_ids_in_order[k]
        seg = time_sorted[boundaries[k] : boundaries[k + 1]]
        t_valid = seg[(seg > -1e9) & (seg < 1e9)]
        if len(t_valid) >= 2:
            t_valid = np.sort(np.unique(t_valid))
            diffs = np.diff(t_valid)
            diffs = diffs[diffs > 0]
            if len(diffs) > 0:
                median_interval_days[i] = np.median(diffs)

    if out_dir is not None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stats_csv = out_dir / "s6_plot_stats.csv"
        with open(stats_csv, "w") as f:
            f.write("station_index,lat,lon,n_records,span_years,median_interval_days\n")
            for i in range(n_stations):
                sy = span_years[i] if not np.isnan(span_years[i]) else 0.0
                mi = median_interval_days[i] if np.isfinite(median_interval_days[i]) else ""
                f.write("{},{},{},{},{:.4f},{}\n".format(i, lat[i], lon[i], rec_count[i], sy, mi))

    return {
        "lat": lat, "lon": lon, "rec_count": rec_count,
        "span_years": span_years, "median_interval_days": median_interval_days,
        "n_stations": n_stations,
    }
