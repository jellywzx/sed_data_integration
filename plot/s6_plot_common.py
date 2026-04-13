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

# 将 scripts_basin_test/ 加入 sys.path，用于导入 pipeline_paths
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from pipeline_paths import (
    get_output_r_root,
    S6_MERGED_NC,
    S5_BASIN_CLUSTERED_CSV,
    PIPELINE_OUTPUT_DIR,
)

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

try:
    import xarray as xr
except ImportError:
    xr = None

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_R_ROOT = get_output_r_root(_SCRIPTS_DIR)
DEFAULT_NC = OUTPUT_R_ROOT / S6_MERGED_NC
DEFAULT_S5_CSV = OUTPUT_R_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUT_DIR = OUTPUT_R_ROOT / PIPELINE_OUTPUT_DIR

# 数据填充值（与 s6_basin_merge_to_nc.py 保持一致）
FILL_F = -9999.0
FILL_I = -9999

# 当前 s6_basin_merge_to_nc.py 的时间类型编码
RESOLUTION_INFO = [
    (0, "daily", "#2196F3"),
    (1, "monthly", "#43A047"),
    (2, "annual", "#FB8C00"),
    (3, "climatology", "#8E24AA"),
    (4, "other", "#9E9E9E"),
]
RES_CODES = dict((code, label) for code, label, _ in RESOLUTION_INFO)
RES_COLORS = dict((code, color) for code, _, color in RESOLUTION_INFO)
RES_ORDER = [code for code, _, _ in RESOLUTION_INFO]
DEFAULT_RES_CODE = max(RES_CODES.keys())


def _require_nc4():
    if nc4 is None and xr is None:
        raise RuntimeError("Either netCDF4 or xarray+h5netcdf is required.")


def _filled(arr, fill=FILL_F):
    if np.ma.isMaskedArray(arr):
        return arr.filled(fill)
    return np.asarray(arr)


def _decode_text(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(value)


def _decode_text_array(values):
    return [_decode_text(v) for v in values]


def annotate_bars(ax, bars, fontsize=9, padding=2, fmt="{:,.0f}"):
    heights = [bar.get_height() for bar in bars]
    ymax = max(heights) if heights else 0.0
    offset = ymax * 0.01 + padding
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + offset,
            fmt.format(height),
            ha="center",
            va="bottom",
            fontsize=fontsize,
        )


def _compute_primary_resolution(station_index, resolution, n_stations):
    res_hist = np.zeros((n_stations, len(RES_ORDER)), dtype=np.int32)
    sid = np.asarray(station_index, dtype=np.int64)
    res = np.asarray(resolution, dtype=np.int64)

    valid = (sid >= 0) & (sid < n_stations) & (res >= 0) & (res <= DEFAULT_RES_CODE)
    if valid.any():
        np.add.at(res_hist, (sid[valid], res[valid]), 1)

    primary_res = np.full(n_stations, DEFAULT_RES_CODE, dtype=np.int32)
    has_any = res_hist.sum(axis=1) > 0
    if has_any.any():
        primary_res[has_any] = res_hist[has_any].argmax(axis=1)
    return primary_res, res_hist


def load_station_stats(nc_path=None, csv_path=None, out_dir=None):
    """
    从 s6_basin_merged_all.nc 或已有 stats CSV 加载每站点的基础统计量。

    返回 dict:
      lat, lon            : 站点坐标 (n_stations,)
      rec_count           : 记录数 (n_stations,)
      span_years          : 时间跨度（年）(n_stations,)
      median_interval_days: 中位时间间隔（天）(n_stations,)
      primary_resolution  : 主时间类型编码 (n_stations,)
      n_stations          : 站点数
    """
    import csv as csv_mod

    if csv_path and Path(csv_path).is_file():
        with open(csv_path, encoding="utf-8") as f:
            rows = list(csv_mod.DictReader(f))
        lat = np.array([float(r["lat"]) for r in rows])
        lon = np.array([float(r["lon"]) for r in rows])
        rec = np.array([int(r["n_records"]) for r in rows])
        span = np.array([float(r["span_years"]) for r in rows])
        mid = np.array([
            float(r["median_interval_days"]) if r.get("median_interval_days", "").strip() else np.nan
            for r in rows
        ])
        primary = np.array([
            int(r["primary_resolution"]) if r.get("primary_resolution", "").strip() else -1
            for r in rows
        ], dtype=np.int32)
        return {
            "lat": lat,
            "lon": lon,
            "rec_count": rec,
            "span_years": span,
            "median_interval_days": mid,
            "primary_resolution": primary,
            "n_stations": len(lat),
        }

    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    if nc4 is not None:
        with nc4.Dataset(nc_path, "r") as ds:
            lat = _filled(ds.variables["lat"][:])
            lon = _filled(ds.variables["lon"][:])
            sid = _filled(ds.variables["station_index"][:]).astype(int)
            time_arr = _filled(ds.variables["time"][:])
            if "resolution" in ds.variables:
                res_arr = _filled(ds.variables["resolution"][:], fill=DEFAULT_RES_CODE).astype(int)
            else:
                res_arr = np.full(len(sid), DEFAULT_RES_CODE, dtype=np.int32)
    else:
        ds = xr.open_dataset(str(nc_path), engine="h5netcdf", decode_cf=False, mask_and_scale=False)
        try:
            lat = _filled(ds["lat"].values)
            lon = _filled(ds["lon"].values)
            sid = _filled(ds["station_index"].values).astype(int)
            time_arr = _filled(ds["time"].values)
            if "resolution" in ds.variables:
                res_arr = _filled(ds["resolution"].values, fill=DEFAULT_RES_CODE).astype(int)
            else:
                res_arr = np.full(len(sid), DEFAULT_RES_CODE, dtype=np.int32)
        finally:
            ds.close()

    n_stations = len(lat)
    rec_count = np.bincount(sid, minlength=n_stations)

    order = np.argsort(sid)
    sid_s = sid[order]
    time_s = time_arr[order]
    bounds = np.concatenate([[0], np.where(np.diff(sid_s) != 0)[0] + 1, [len(sid_s)]])
    sids_uniq = sid_s[bounds[:-1]]

    t_min = np.full(n_stations, np.nan)
    t_max = np.full(n_stations, np.nan)
    med_interval = np.full(n_stations, np.nan)

    for k in range(len(bounds) - 1):
        i = sids_uniq[k]
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

    span_days = np.where(np.isnan(t_min) | np.isnan(t_max), np.nan, t_max - t_min)
    span_years = np.where(np.isnan(span_days), 0.0, span_days / 365.25)
    primary_res, _ = _compute_primary_resolution(sid, res_arr, n_stations)

    if out_dir is not None:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stats_csv = out_dir / "s6_plot_stats.csv"
        with open(stats_csv, "w", encoding="utf-8") as f:
            f.write("station_index,lat,lon,n_records,span_years,median_interval_days,primary_resolution\n")
            for i in range(n_stations):
                sy = span_years[i] if np.isfinite(span_years[i]) else 0.0
                mi = "{:.4f}".format(med_interval[i]) if np.isfinite(med_interval[i]) else ""
                f.write(
                    "{},{},{},{},{:.4f},{},{}\n".format(
                        i, lat[i], lon[i], rec_count[i], sy, mi, int(primary_res[i])
                    )
                )

    return {
        "lat": lat,
        "lon": lon,
        "rec_count": rec_count,
        "span_years": span_years,
        "median_interval_days": med_interval,
        "primary_resolution": primary_res,
        "n_stations": n_stations,
    }


def load_station_sources(nc_path=None):
    """
    从 station 层读取每个 cluster 的来源摘要。

    返回 dict:
      lat, lon, cluster_uid, primary_source, all_sources, n_sources, n_stations
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    if nc4 is not None:
        with nc4.Dataset(nc_path, "r") as ds:
            n_stations = len(ds.dimensions["n_stations"])
            lat = _filled(ds.variables["lat"][:])
            lon = _filled(ds.variables["lon"][:])
            cluster_uid = _decode_text_array(ds.variables["cluster_uid"][:]) if "cluster_uid" in ds.variables else [
                "SED{:06d}".format(i) for i in range(n_stations)
            ]
            if "sources_used" in ds.variables:
                sources_used = _decode_text_array(ds.variables["sources_used"][:])
            else:
                sources_used = [""] * n_stations
    else:
        ds = xr.open_dataset(str(nc_path), engine="h5netcdf", decode_cf=False, mask_and_scale=False)
        try:
            n_stations = int(ds.sizes["n_stations"])
            lat = _filled(ds["lat"].values)
            lon = _filled(ds["lon"].values)
            cluster_uid = _decode_text_array(ds["cluster_uid"].values) if "cluster_uid" in ds.variables else [
                "SED{:06d}".format(i) for i in range(n_stations)
            ]
            if "sources_used" in ds.variables:
                sources_used = _decode_text_array(ds["sources_used"].values)
            else:
                sources_used = [""] * n_stations
        finally:
            ds.close()

    primary_source = []
    all_sources = []
    n_sources = np.zeros(n_stations, dtype=np.int32)

    for i, raw in enumerate(sources_used):
        parts = [p.strip() for p in raw.split("|") if p.strip()]
        parts = sorted(set(parts))
        n_sources[i] = len(parts)
        if len(parts) == 0:
            primary_source.append("unknown")
            all_sources.append("")
        elif len(parts) == 1:
            primary_source.append(parts[0])
            all_sources.append(parts[0])
        else:
            primary_source.append("mixed")
            all_sources.append(",".join(parts))

    return {
        "lat": lat,
        "lon": lon,
        "cluster_uid": np.array(cluster_uid, dtype=object),
        "primary_source": np.array(primary_source, dtype=object),
        "all_sources": np.array(all_sources, dtype=object),
        "n_sources": n_sources,
        "n_stations": n_stations,
    }


def load_variable_availability(nc_path=None):
    """
    从 NC 读取每站点的 Q / SSC / SSL 变量可用性与主要分辨率。
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    if nc4 is not None:
        with nc4.Dataset(nc_path, "r") as ds:
            n_stations = len(ds.dimensions["n_stations"])
            lat = _filled(ds.variables["lat"][:])
            lon = _filled(ds.variables["lon"][:])
            sid = _filled(ds.variables["station_index"][:]).astype(int)
            q = _filled(ds.variables["Q"][:])
            ssc = _filled(ds.variables["SSC"][:])
            ssl = _filled(ds.variables["SSL"][:])
            res = _filled(ds.variables["resolution"][:], fill=DEFAULT_RES_CODE).astype(int)
    else:
        ds = xr.open_dataset(str(nc_path), engine="h5netcdf", decode_cf=False, mask_and_scale=False)
        try:
            n_stations = int(ds.sizes["n_stations"])
            lat = _filled(ds["lat"].values)
            lon = _filled(ds["lon"].values)
            sid = _filled(ds["station_index"].values).astype(int)
            q = _filled(ds["Q"].values)
            ssc = _filled(ds["SSC"].values)
            ssl = _filled(ds["SSL"].values)
            res = _filled(ds["resolution"].values, fill=DEFAULT_RES_CODE).astype(int)
        finally:
            ds.close()

    thr = FILL_F * 0.5
    valid_q = (q > thr) & np.isfinite(q)
    valid_ssc = (ssc > thr) & np.isfinite(ssc)
    valid_ssl = (ssl > thr) & np.isfinite(ssl)

    cnt_q = np.zeros(n_stations, dtype=np.int32)
    cnt_ssc = np.zeros(n_stations, dtype=np.int32)
    cnt_ssl = np.zeros(n_stations, dtype=np.int32)
    np.add.at(cnt_q, sid, valid_q.astype(np.int32))
    np.add.at(cnt_ssc, sid, valid_ssc.astype(np.int32))
    np.add.at(cnt_ssl, sid, valid_ssl.astype(np.int32))

    has_Q = cnt_q > 0
    has_SSC = cnt_ssc > 0
    has_SSL = cnt_ssl > 0

    primary_res, _ = _compute_primary_resolution(sid, res, n_stations)

    combo = np.empty(n_stations, dtype=object)
    for i in range(n_stations):
        parts = []
        if has_Q[i]:
            parts.append("Q")
        if has_SSC[i]:
            parts.append("SSC")
        if has_SSL[i]:
            parts.append("SSL")
        combo[i] = "+".join(parts) if parts else "none"

    return {
        "has_Q": has_Q,
        "has_SSC": has_SSC,
        "has_SSL": has_SSL,
        "combo": combo,
        "primary_res": primary_res,
        "lat": lat,
        "lon": lon,
        "n_stations": n_stations,
    }


def load_yearly_coverage(nc_path=None):
    """
    统计每年拥有至少一条有效（Q 或 SSC 或 SSL 非填充）记录的站点数。
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    if nc4 is not None:
        with nc4.Dataset(nc_path, "r") as ds:
            sid = _filled(ds.variables["station_index"][:]).astype(int)
            time_arr = _filled(ds.variables["time"][:])
            q = _filled(ds.variables["Q"][:])
            ssc = _filled(ds.variables["SSC"][:])
            ssl = _filled(ds.variables["SSL"][:])
            res = _filled(ds.variables["resolution"][:], fill=DEFAULT_RES_CODE).astype(int)
    else:
        ds = xr.open_dataset(str(nc_path), engine="h5netcdf", decode_cf=False, mask_and_scale=False)
        try:
            sid = _filled(ds["station_index"].values).astype(int)
            time_arr = _filled(ds["time"].values)
            q = _filled(ds["Q"].values)
            ssc = _filled(ds["SSC"].values)
            ssl = _filled(ds["SSL"].values)
            res = _filled(ds["resolution"].values, fill=DEFAULT_RES_CODE).astype(int)
        finally:
            ds.close()

    thr = FILL_F * 0.5
    any_valid = (
        ((q > thr) & np.isfinite(q)) |
        ((ssc > thr) & np.isfinite(ssc)) |
        ((ssl > thr) & np.isfinite(ssl))
    )
    mask = any_valid & (time_arr > -1e9) & (time_arr < 1e9)

    time_valid = time_arr[mask]
    sid_valid = sid[mask]
    res_valid = res[mask]

    if len(time_valid) == 0:
        return {"years": np.array([]), "total": np.array([]), "by_res": {}, "year_range": (0, 0)}

    t64 = np.datetime64("1970-01-01") + time_valid.astype(np.int64).astype("timedelta64[D]")
    years = t64.astype("datetime64[Y]").astype(int) + 1970

    y_min = int(years.min())
    y_max = int(years.max())
    year_range = np.arange(y_min, y_max + 1)
    n_years = len(year_range)
    y_idx = years - y_min

    total = np.zeros(n_years, dtype=np.int32)
    for yi in range(n_years):
        mask_y = (y_idx == yi)
        if mask_y.any():
            total[yi] = np.unique(sid_valid[mask_y]).shape[0]

    by_res = {}
    for code in RES_ORDER:
        label = RES_CODES[code]
        rm = (res_valid == code)
        counts = np.zeros(n_years, dtype=np.int32)
        if rm.any():
            y_idx_r = y_idx[rm]
            sid_r = sid_valid[rm]
            for yi in range(n_years):
                mk = (y_idx_r == yi)
                if mk.any():
                    counts[yi] = np.unique(sid_r[mk]).shape[0]
        by_res[label] = counts

    return {"years": year_range, "total": total, "by_res": by_res, "year_range": (y_min, y_max)}


def load_basin_stats(nc_path=None):
    """
    从 NC 读取流域属性数组。
    """
    _require_nc4()
    nc_path = Path(nc_path) if nc_path else DEFAULT_NC
    if not nc_path.is_file():
        raise FileNotFoundError("NC not found: {}".format(nc_path))

    if nc4 is not None:
        with nc4.Dataset(nc_path, "r") as ds:
            n_stations = len(ds.dimensions["n_stations"])
            lat = _filled(ds.variables["lat"][:])
            lon = _filled(ds.variables["lon"][:])

            def _load(name, fill=FILL_F):
                if name in ds.variables:
                    return _filled(ds.variables[name][:], fill=fill)
                return np.full(n_stations, fill)

            basin_area = _load("basin_area")
            pfaf_code = _load("pfaf_code")
            match_qual = _load("basin_match_quality", fill=-1).astype(float)
            n_reaches = _load("n_upstream_reaches", fill=FILL_I).astype(float)

            match_quality_labels = {}
            if "basin_match_quality" in ds.variables:
                mq_var = ds.variables["basin_match_quality"]
                values = getattr(mq_var, "flag_values", None)
                meanings = str(getattr(mq_var, "flag_meanings", "")).split()
                if values is not None and len(meanings) == len(values):
                    for value, meaning in zip(values, meanings):
                        match_quality_labels[int(value)] = meaning
    else:
        ds = xr.open_dataset(str(nc_path), engine="h5netcdf", decode_cf=False, mask_and_scale=False)
        try:
            n_stations = int(ds.sizes["n_stations"])
            lat = _filled(ds["lat"].values)
            lon = _filled(ds["lon"].values)

            def _load(name, fill=FILL_F):
                if name in ds.variables:
                    return _filled(ds[name].values, fill=fill)
                return np.full(n_stations, fill)

            basin_area = _load("basin_area")
            pfaf_code = _load("pfaf_code")
            match_qual = _load("basin_match_quality", fill=-1).astype(float)
            n_reaches = _load("n_upstream_reaches", fill=FILL_I).astype(float)

            match_quality_labels = {}
            if "basin_match_quality" in ds.variables:
                values = ds["basin_match_quality"].attrs.get("flag_values")
                meanings = str(ds["basin_match_quality"].attrs.get("flag_meanings", "")).split()
                if values is not None and len(meanings) == len(values):
                    for value, meaning in zip(values, meanings):
                        match_quality_labels[int(value)] = meaning
        finally:
            ds.close()

    if not match_quality_labels:
        match_quality_labels = {0: "distance_only", 1: "area_matched", 2: "failed", -1: "unknown"}

    return {
        "lat": lat,
        "lon": lon,
        "basin_area": basin_area,
        "pfaf_code": pfaf_code,
        "match_quality": match_qual,
        "match_quality_labels": match_quality_labels,
        "n_upstream_reaches": n_reaches,
        "n_stations": n_stations,
    }
