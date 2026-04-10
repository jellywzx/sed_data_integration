#!/usr/bin/env python3
"""
步骤 s6（出图）公共模块：路径常量与从 s6_basin_merged_all.nc 加载站点统计。
供 plot/ 下所有 plot_merged_stations_*.py 复用。

默认路径（均通过 pipeline_paths 解析，相对于 Output_r 根目录）：
  DEFAULT_NC    : scripts_basin_test/output/s6_basin_merged_all.nc
  DEFAULT_S5_CSV: scripts_basin_test/output/s5_basin_clustered_stations.csv
  DEFAULT_OUT_DIR: scripts_basin_test/output/
"""

import sys
from pathlib import Path

import numpy as np

<<<<<<< HEAD
# 将 scripts_basin_test/ 加入 sys.path，用于导入 pipeline_paths
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent   # .../scripts_basin_test/
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from pipeline_paths import (
    get_output_r_root,
    S6_MERGED_NC,
    S5_BASIN_CLUSTERED_CSV,
    PIPELINE_OUTPUT_DIR,
)

=======
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

<<<<<<< HEAD
SCRIPT_DIR      = Path(__file__).resolve().parent        # .../plot/
OUTPUT_R_ROOT   = get_output_r_root(_SCRIPTS_DIR)        # .../sed_data/  (= Output_r)
DEFAULT_NC      = OUTPUT_R_ROOT / S6_MERGED_NC
DEFAULT_S5_CSV  = OUTPUT_R_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUT_DIR = OUTPUT_R_ROOT / PIPELINE_OUTPUT_DIR

# 数据填充值（与 s6_basin_merge_to_nc.py 保持一致）
FILL_F  = -9999.0
FILL_I  = -9999

# resolution 编码（与 s6_basin_merge_to_nc.py 保持一致）
RES_CODES = {0: "daily", 1: "monthly", 2: "annually_clim", 3: "other"}
RES_COLORS = {0: "#2196F3", 1: "#4CAF50", 2: "#FF9800", 3: "#9E9E9E"}


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _require_nc4():
    if nc4 is None:
        raise RuntimeError("netCDF4 is required. Install with: pip install netCDF4")


def _filled(arr, fill=FILL_F):
    """将 masked array 转为普通 ndarray，用 fill 替换 masked 值。"""
    if np.ma.isMaskedArray(arr):
        return arr.filled(fill)
    return np.asarray(arr)


# ─────────────────────────────────────────────────────────────────────────────
# 加载函数
# ─────────────────────────────────────────────────────────────────────────────

def load_station_stats(nc_path=None, csv_path=None, out_dir=None):
    """
    从 s6_basin_merged_all.nc 或已有 stats CSV 加载每站点的基础统计量。

    返回 dict:
      lat, lon            : 站点坐标 (n_stations,)
      rec_count           : 记录数 (n_stations,)
      span_years          : 时间跨度（年）(n_stations,)
      median_interval_days: 中位时间间隔（天）(n_stations,)
      n_stations          : 站点数

    参数:
      nc_path : NC 文件路径；为 None 时使用 DEFAULT_NC
      csv_path: 已有 stats CSV（若给定则跳过 NC 解析）
      out_dir : 若从 NC 加载且给定，则将 stats 写入 out_dir/s6_plot_stats.csv
    """
    import csv as csv_mod

    if csv_path and Path(csv_path).is_file():
        with open(csv_path, encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))
        lat  = np.array([float(r["lat"])       for r in rows])
        lon  = np.array([float(r["lon"])       for r in rows])
        rec  = np.array([int(r["n_records"])   for r in rows])
        span = np.array([float(r["span_years"]) for r in rows])
        mid  = np.array([
            float(r["median_interval_days"]) if r.get("median_interval_days", "").strip() else np.nan
            for r in rows
        ])
        return {"lat": lat, "lon": lon, "rec_count": rec,
                "span_years": span, "median_interval_days": mid,
                "n_stations": len(lat)}

    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    with nc4.Dataset(nc_path, "r") as ds:
        lat = _filled(ds.variables["lat"][:])
        lon = _filled(ds.variables["lon"][:])
        sid = _filled(ds.variables["station_index"][:]).astype(int)
        time_arr = _filled(ds.variables["time"][:])

    n_stations = len(lat)
    rec_count  = np.bincount(sid, minlength=n_stations)

    # 时间跨度 & 中位间隔
    order        = np.argsort(sid)
    sid_s        = sid[order]
    time_s       = time_arr[order]
    bounds       = np.concatenate([[0], np.where(np.diff(sid_s) != 0)[0] + 1, [len(sid_s)]])
    sids_uniq    = sid_s[bounds[:-1]]

    t_min = np.full(n_stations, np.nan)
    t_max = np.full(n_stations, np.nan)
    med_interval = np.full(n_stations, np.nan)

    for k in range(len(bounds) - 1):
        i   = sids_uniq[k]
        seg = time_s[bounds[k]: bounds[k + 1]]
        valid = seg[(seg > -1e9) & (seg < 1e9)]
        if len(valid) == 0:
            continue
        t_min[i] = valid.min()
        t_max[i] = valid.max()
        if len(valid) >= 2:
            diffs = np.diff(np.sort(np.unique(valid)))
            diffs = diffs[diffs > 0]
            if len(diffs):
                med_interval[i] = np.median(diffs)

    span_days  = np.where(np.isnan(t_min) | np.isnan(t_max), np.nan, t_max - t_min)
    span_years = np.where(np.isnan(span_days), 0.0, span_days / 365.25)
=======
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
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538

    if out_dir is not None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stats_csv = out_dir / "s6_plot_stats.csv"
<<<<<<< HEAD
        with open(stats_csv, "w", encoding="utf-8") as f:
            f.write("station_index,lat,lon,n_records,span_years,median_interval_days\n")
            for i in range(n_stations):
                sy = span_years[i] if np.isfinite(span_years[i]) else 0.0
                mi = "{:.4f}".format(med_interval[i]) if np.isfinite(med_interval[i]) else ""
                f.write("{},{},{},{},{:.4f},{}\n".format(i, lat[i], lon[i], rec_count[i], sy, mi))

    return {"lat": lat, "lon": lon, "rec_count": rec_count,
            "span_years": span_years, "median_interval_days": med_interval,
            "n_stations": n_stations}


def load_variable_availability(nc_path=None):
    """
    从 NC 读取每站点的 Q / SSC / SSL 变量可用性与主要分辨率。

    返回 dict:
      has_Q, has_SSC, has_SSL   : bool 数组 (n_stations,)
      combo                     : str 数组，值如 "Q+SSC+SSL" / "Q+SSC" / "Q" / "SSC" …
      primary_res               : int 数组，各站点出现最多的 resolution 编码 (0-3)
      lat, lon                  : 坐标 (n_stations,)
      n_stations                : 站点数
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    with nc4.Dataset(nc_path, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        lat  = _filled(ds.variables["lat"][:])
        lon  = _filled(ds.variables["lon"][:])
        sid  = _filled(ds.variables["station_index"][:]).astype(int)
        q    = _filled(ds.variables["Q"][:])
        ssc  = _filled(ds.variables["SSC"][:])
        ssl  = _filled(ds.variables["SSL"][:])
        res  = _filled(ds.variables["resolution"][:], fill=3).astype(int)

    thr = FILL_F * 0.5   # > -4999.5 → 有效值
    valid_q   = (q   > thr) & np.isfinite(q)
    valid_ssc = (ssc > thr) & np.isfinite(ssc)
    valid_ssl = (ssl > thr) & np.isfinite(ssl)

    # 每站点累加有效记录数
    cnt_q   = np.zeros(n_stations, dtype=np.int32)
    cnt_ssc = np.zeros(n_stations, dtype=np.int32)
    cnt_ssl = np.zeros(n_stations, dtype=np.int32)
    np.add.at(cnt_q,   sid, valid_q.astype(np.int32))
    np.add.at(cnt_ssc, sid, valid_ssc.astype(np.int32))
    np.add.at(cnt_ssl, sid, valid_ssl.astype(np.int32))

    has_Q   = cnt_q   > 0
    has_SSC = cnt_ssc > 0
    has_SSL = cnt_ssl > 0

    # 每站点主分辨率（出现次数最多的编码）
    res_hist = np.zeros((n_stations, 4), dtype=np.int32)
    res_clip = np.clip(res, 0, 3)
    np.add.at(res_hist, (sid, res_clip), 1)
    primary_res = res_hist.argmax(axis=1)

    # 组合标签
    combo = np.empty(n_stations, dtype=object)
    for i in range(n_stations):
        parts = []
        if has_Q[i]:   parts.append("Q")
        if has_SSC[i]: parts.append("SSC")
        if has_SSL[i]: parts.append("SSL")
        combo[i] = "+".join(parts) if parts else "none"

    return {"has_Q": has_Q, "has_SSC": has_SSC, "has_SSL": has_SSL,
            "combo": combo, "primary_res": primary_res,
            "lat": lat, "lon": lon, "n_stations": n_stations}


def load_yearly_coverage(nc_path=None):
    """
    统计每年拥有至少一条有效（Q 或 SSC 或 SSL 非填充）记录的站点数。

    返回 dict:
      years        : int 数组，覆盖 NC 中实际出现的最小-最大年份
      total        : 该年有记录的站点数（无论变量）
      by_res       : dict {res_label: count_array}，按 resolution 分层
      year_range   : (y_min, y_max)
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    with nc4.Dataset(nc_path, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        sid      = _filled(ds.variables["station_index"][:]).astype(int)
        time_arr = _filled(ds.variables["time"][:])
        q        = _filled(ds.variables["Q"][:])
        ssc      = _filled(ds.variables["SSC"][:])
        ssl      = _filled(ds.variables["SSL"][:])
        res      = _filled(ds.variables["resolution"][:], fill=3).astype(int)

    # 只保留至少一个变量有效的记录
    thr = FILL_F * 0.5
    any_valid = (
        ((q   > thr) & np.isfinite(q)) |
        ((ssc > thr) & np.isfinite(ssc)) |
        ((ssl > thr) & np.isfinite(ssl))
    )
    mask = any_valid & (time_arr > -1e9) & (time_arr < 1e9)

    time_valid = time_arr[mask]
    sid_valid  = sid[mask]
    res_valid  = res[mask]

    if len(time_valid) == 0:
        return {"years": np.array([]), "total": np.array([]),
                "by_res": {}, "year_range": (0, 0)}

    # days since 1970-01-01 → year
    t64   = (np.datetime64("1970-01-01") +
             time_valid.astype(np.int64).astype("timedelta64[D]"))
    years = t64.astype("datetime64[Y]").astype(int) + 1970

    y_min, y_max = int(years.min()), int(years.max())
    year_range   = np.arange(y_min, y_max + 1)
    n_years      = len(year_range)
    y_idx        = years - y_min   # 0-based index into year_range

    # 每年有有效记录的唯一站点数（total）
    total = np.zeros(n_years, dtype=np.int32)
    for yi in range(n_years):
        mask_y = (y_idx == yi)
        if mask_y.any():
            total[yi] = np.unique(sid_valid[mask_y]).shape[0]

    # 按 resolution 分层
    by_res = {}
    for code, label in RES_CODES.items():
        rm = (res_valid == code)
        counts = np.zeros(n_years, dtype=np.int32)
        if rm.any():
            y_idx_r = y_idx[rm]
            sid_r   = sid_valid[rm]
            for yi in range(n_years):
                mk = (y_idx_r == yi)
                if mk.any():
                    counts[yi] = np.unique(sid_r[mk]).shape[0]
        by_res[label] = counts

    return {"years": year_range, "total": total,
            "by_res": by_res, "year_range": (y_min, y_max)}


def load_basin_stats(nc_path=None):
    """
    从 NC 读取流域属性数组。

    返回 dict:
      lat, lon          : 坐标 (n_stations,)
      basin_area        : 集水面积 km² (n_stations,)，-9999 = 无效
      pfaf_code         : Pfafstetter 编码 (n_stations,)，-9999 = 无效
      match_quality     : 匹配质量 0–3，-1 = 未知 (n_stations,)
      n_upstream_reaches: 上游河段数 (n_stations,)
      n_stations        : 站点数
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    with nc4.Dataset(nc_path, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        lat  = _filled(ds.variables["lat"][:])
        lon  = _filled(ds.variables["lon"][:])

        def _load(name, fill=FILL_F):
            if name in ds.variables:
                return _filled(ds.variables[name][:], fill=fill)
            return np.full(n_stations, fill)

        basin_area = _load("basin_area")
        pfaf_code  = _load("pfaf_code")
        match_qual = _load("basin_match_quality", fill=-1).astype(float)
        n_reaches  = _load("n_upstream_reaches",  fill=FILL_I).astype(float)

    return {"lat": lat, "lon": lon,
            "basin_area": basin_area, "pfaf_code": pfaf_code,
            "match_quality": match_qual, "n_upstream_reaches": n_reaches,
            "n_stations": n_stations}
=======
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
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
