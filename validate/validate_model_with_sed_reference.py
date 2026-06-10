#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Validate gridded model NetCDF output against sed_reference_release.

The script discovers usable reference stations automatically from
station_catalog.csv, extracts Q/SSC/SSL from the reference matrix, matches each
reference station to the nearest model grid cell, and computes station-variable
validation metrics.

All configuration is defined via DEFAULT_* constants at the top of the file and
assembled into a dict inside main(). Edit the default values before running.
"""

import math
import os
import re
import concurrent.futures
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
import json
import urllib.request

import numpy as np
import pandas as pd
import xarray as xr

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None


MATRIX_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}

VARIABLES = {
    "Q": {
        "ref_var": "Q",
        "flag_var": "Q_flag",
        "unit": "m3 s-1",
        "metric_name": "Q_m3_s-1",
        "ref_col": "Q_reference_m3_s-1",
        "model_col": "Q_model_m3_s-1",
    },
    "SSC": {
        "ref_var": "SSC",
        "flag_var": "SSC_flag",
        "unit": "mg L-1",
        "metric_name": "SSC_mg_L",
        "ref_col": "SSC_reference_mg_L",
        "model_col": "SSC_model_mg_L",
    },
    "SSL": {
        "ref_var": "SSL",
        "flag_var": "SSL_flag",
        "unit": "ton day-1",
        "metric_name": "SSL_t_day",
        "ref_col": "SSL_reference_t_day",
        "model_col": "SSL_model_t_day",
    },
}

# Grain-fraction variable names per short name in unitcat files
UNITCAT_GRAIN_VARS = {
    "Q":   ["f_discharge"],
    "SSC": ["f_sedcon_1", "f_sedcon_2", "f_sedcon_3"],
    "SSL": ["f_sedout_1",  "f_sedout_2",  "f_sedout_3"],
}


# 
#  Default configuration constants
#  Edit these values before running.
# 

# --- 模型 NetCDF ---
DEFAULT_MODEL_NC = "/share/home/dq134/wzx/CoLM/cases/sed_test_tune2/history"                           # 模型 NetCDF 文件路径（必填）
DEFAULT_MODEL_NC_PATTERN = "*_hist_unitcat_*.nc"        # 模型 NC 文件 glob 匹配模式
DEFAULT_MODEL_TIME_NAME = "time"                # 模型时间坐标名
DEFAULT_MODEL_LAT_NAME = "lat_ucat"                  # 模型纬度变量/坐标名
DEFAULT_MODEL_LON_NAME = "lon_ucat"                  # 模型经度变量/坐标名

# --- 模型变量名（至少提供一个） ---
DEFAULT_MODEL_Q_VAR = "f_discharge"                        # 模型径流 Q 变量名
DEFAULT_MODEL_SSC_VAR = "f_sedcon"              # 模型悬沙浓度 SSC 变量名
DEFAULT_MODEL_SSL_VAR = "f_sedout"              # 模型悬沙输沙率 SSL 变量名

# --- 单位转换因子（乘数，将模型值转换到标准单位） ---
DEFAULT_MODEL_Q_FACTOR = 1.0                    # → m³/s
DEFAULT_MODEL_SSC_FACTOR = 2650000.0            # → mg/L
DEFAULT_MODEL_SSL_FACTOR = 228960.0             # → ton/day

# --- 参考数据 ---
DEFAULT_REFERENCE_DIR = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release"
DEFAULT_RESOLUTION = "daily"                    # 参考时间分辨率：daily / monthly / annual
DEFAULT_ALLOWED_FLAGS = "0,1,2"                 # 保留的参考数据质量标记（逗号分隔）

# --- 筛选控制 ---
DEFAULT_MIN_REFERENCE_POINTS = 10               # 每变量最少有效参考数据点数
DEFAULT_MIN_PAIRED_POINTS = 10                  # 每指标最少模型-参考匹配数据点数
DEFAULT_MAX_GRID_DISTANCE_KM = 50.0             # 站点到模型网格单元最大距离（km）

# --- 空间区域筛选（可选；留空不限） ---
DEFAULT_REGION_LAT_MIN = -20                    # 最小纬度（如 -30.0）
DEFAULT_REGION_LAT_MAX = 5                    # 最大纬度（如 -10.0）
DEFAULT_REGION_LON_MIN = -80                    # 最小经度（如 -60.0）
DEFAULT_REGION_LON_MAX = -45                    # 最大经度（如 -40.0）

# --- 时间窗口 ---
DEFAULT_START_DATE = "1995-01-01"               # 验证起始日期（空 = 不限制）
DEFAULT_END_DATE = "1999-09-30"                 # 验证截止日期（空 = 不限制）

# --- 其他 ---
DEFAULT_OUTPUT_DIR = "../output_other/validate_model_with_sed_reference"     # 输出目录路径（必填）
DEFAULT_MAX_STATIONS = 0                        # 最大处理站数（0 = 不限制）
DEFAULT_MAKE_PLOTS = True                      # 是否输出逐对比 PNG 图
DEFAULT_NUM_WORKERS = 8                        # 并行进程数（0 = 自动选 CPU 核心数的一半）

DEFAULT_MERIT_HYDRO_DIR = "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1"  # MERIT Hydro 河网数据目录（区域图使用）


def parse_allowed_flags(text: str) -> Tuple[int, ...]:
    flags = []
    for part in text.split(","):
        part = part.strip()
        if part:
            flags.append(int(part))
    if not flags:
        raise ValueError("allowed_flags must contain at least one integer flag.")
    return tuple(flags)


def parse_start_date(text: str) -> Optional[pd.Timestamp]:
    return pd.to_datetime(text) if text else None


def parse_end_date(text: str, resolution: str) -> Optional[pd.Timestamp]:
    if not text:
        return None
    stripped = text.strip()
    ts = pd.to_datetime(stripped)
    has_time = (" " in stripped) or ("T" in stripped)
    if has_time:
        return ts
    if resolution == "annual" and re.match(r"^\d{4}$", stripped):
        return ts + pd.DateOffset(years=1) - pd.Timedelta(nanoseconds=1)
    if resolution == "monthly" and re.match(r"^\d{4}-\d{1,2}$", stripped):
        return ts + pd.DateOffset(months=1) - pd.Timedelta(nanoseconds=1)
    return ts + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)


def clean_text(value: object) -> str:
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def safe_name(value: object) -> str:
    text = clean_text(value)
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
    text = text.strip("_")
    return text or "unknown"


def haversine_km(lat1, lon1, lat2, lon2):
    lat1 = np.deg2rad(np.asarray(lat1, dtype=np.float64))
    lon1 = np.deg2rad(np.asarray(lon1, dtype=np.float64))
    lat2 = np.deg2rad(np.asarray(lat2, dtype=np.float64))
    lon2 = np.deg2rad(np.asarray(lon2, dtype=np.float64))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def to_lon180(lon):
    lon = np.asarray(lon, dtype=np.float64)
    return ((lon + 180.0) % 360.0) - 180.0


def maybe_to_model_lon(target_lon: float, model_lon_values: np.ndarray) -> float:
    vals = np.asarray(model_lon_values, dtype=float)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return target_lon
    if np.nanmin(vals) >= 0.0 and np.nanmax(vals) > 180.0:
        return target_lon % 360.0
    return float(to_lon180(target_lon))


def nc_time_to_datetime(time_var) -> pd.DatetimeIndex:
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to read sediment reference NetCDF files.")
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        times = nc4.num2date(time_var[:], units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        times = nc4.num2date(time_var[:], units, calendar=calendar)
    return pd.to_datetime(times)


def list_model_files(directory: Path, pattern: str) -> List[Path]:
    """Glob and sort model NC files matching pattern."""
    files = sorted(directory.glob(pattern))
    if not files:
        raise FileNotFoundError("No model files matching '%s' in %s" % (pattern, directory))
    return files


def read_unitcat_time(h5ds) -> pd.DatetimeIndex:
    """Convert 'minutes since 1900-1-1 0:0:0' time variable to pd.DatetimeIndex."""
    minutes = np.asarray(h5ds["time"][:], dtype=np.float64)
    return pd.Timestamp("1900-01-01") + pd.to_timedelta(minutes, unit="m")


def get_unitcat_model_time_range(files: List[Path]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Scan all unitcat files and return global min/max timestamps."""
    all_starts, all_ends = [], []
    for f in files:
        import h5py as _h5
        with _h5.File(str(f), "r") as h5ds:
            times = read_unitcat_time(h5ds)
            all_starts.append(times.min())
            all_ends.append(times.max())
    return min(all_starts), max(all_ends)


def load_unitcat_coords(first_file: Path) -> Tuple[np.ndarray, np.ndarray]:
    """Read lat_ucat and lon_ucat from the first unitcat file."""
    import h5py as _h5
    with _h5.File(str(first_file), "r") as h5ds:
        lat = np.asarray(h5ds["lat_ucat"][:], dtype=np.float64)
        lon = np.asarray(h5ds["lon_ucat"][:], dtype=np.float64)
    return lat, lon


def model_domain_bounds(lat: np.ndarray, lon: np.ndarray) -> Dict[str, float]:
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    lat = lat[np.isfinite(lat)]
    lon = lon[np.isfinite(lon)]
    if lat.size == 0 or lon.size == 0:
        raise ValueError("Model lat/lon coordinates contain no finite values.")
    lon180 = to_lon180(lon)
    return {
        "lat_min": float(np.nanmin(lat)),
        "lat_max": float(np.nanmax(lat)),
        "lon_min_180": float(np.nanmin(lon180)),
        "lon_max_180": float(np.nanmax(lon180)),
    }


def station_in_bbox(lat: float, lon: float, bounds: Dict[str, float]) -> bool:
    lon180 = float(to_lon180(lon))
    return (
        bounds["lat_min"] <= lat <= bounds["lat_max"]
        and bounds["lon_min_180"] <= lon180 <= bounds["lon_max_180"]
    )


def find_nearest_model_cell(
    lat: np.ndarray,
    lon: np.ndarray,
    station_lat: float,
    station_lon: float,
) -> Dict[str, object]:
    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)

    # Always treat as 1D grids for unitcat files
    target_lon = maybe_to_model_lon(station_lon, lon)
    lat_idx = int(np.nanargmin(np.abs(lat - station_lat)))
    lon_idx = int(np.nanargmin(np.abs(lon - target_lon)))
    grid_lat = float(lat[lat_idx])
    grid_lon = float(lon[lon_idx])
    distance = float(haversine_km(station_lat, station_lon, grid_lat, float(to_lon180(grid_lon))))
    return {
        "indexers": {"lat_ucat": lat_idx, "lon_ucat": lon_idx},
        "model_grid_lat": grid_lat,
        "model_grid_lon": grid_lon,
        "model_grid_distance_km": distance,
        "model_grid_index": "lat_ucat=%d,lon_ucat=%d" % (lat_idx, lon_idx),
    }


def load_station_catalog(reference_dir: Path, resolution: str) -> pd.DataFrame:
    path = reference_dir / "station_catalog.csv"
    if not path.exists():
        raise FileNotFoundError("station_catalog.csv not found in %s" % reference_dir)
    df = pd.read_csv(path, keep_default_na=False)
    df = df[df["resolution"].astype(str).str.strip() == resolution].copy()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["record_count"] = pd.to_numeric(df.get("record_count", np.nan), errors="coerce")
    df["time_start_ts"] = pd.to_datetime(df.get("time_start", ""), errors="coerce")
    df["time_end_ts"] = pd.to_datetime(df.get("time_end", ""), errors="coerce")
    return df.reset_index(drop=True)


def time_overlap(start_a: pd.Timestamp, end_a: pd.Timestamp, start_b, end_b) -> bool:
    if pd.isna(start_b) or pd.isna(end_b):
        return False
    return pd.Timestamp(start_b) <= end_a and pd.Timestamp(end_b) >= start_a


def normalize_time(values: pd.Series, resolution: str) -> pd.Series:
    values = pd.to_datetime(values, errors="coerce")
    if resolution == "daily":
        return values.dt.floor("D")
    if resolution == "monthly":
        return values.dt.to_period("M").dt.to_timestamp()
    if resolution == "annual":
        return values.dt.to_period("Y").dt.to_timestamp()
    raise ValueError("Unsupported resolution: %s" % resolution)


def aggregate_timeseries(df: pd.DataFrame, value_col: str, resolution: str, out_col: str) -> pd.DataFrame:
    work = df[["time", value_col]].copy()
    work["time"] = normalize_time(work["time"], resolution)
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=["time", value_col])
    if work.empty:
        return pd.DataFrame(columns=["time", out_col])
    work = work.groupby("time", as_index=False)[value_col].mean()
    return work.rename(columns={value_col: out_col})


def nse(obs: np.ndarray, sim: np.ndarray) -> float:
    denom = np.sum((obs - np.mean(obs)) ** 2)
    if denom == 0:
        return np.nan
    return float(1.0 - np.sum((sim - obs) ** 2) / denom)


def compute_metrics(obs: np.ndarray, sim: np.ndarray) -> Dict[str, float]:
    if len(obs) == 0:
        return {"n": 0, "pearson_r": np.nan, "rmse": np.nan, "bias": np.nan, "nse": np.nan}
    pearson_r = np.nan
    if len(obs) > 1 and np.std(obs) > 0 and np.std(sim) > 0:
        pearson_r = float(np.corrcoef(obs, sim)[0, 1])
    return {
        "n": int(len(obs)),
        "pearson_r": pearson_r,
        "rmse": float(np.sqrt(np.mean((sim - obs) ** 2))),
        "bias": float(np.mean(sim - obs)),
        "nse": nse(obs, sim),
    }


def extract_reference_for_station(
    ds,
    row_idx: int,
    times: pd.DatetimeIndex,
    allowed_flags: Sequence[int],
    time_mask: np.ndarray,
) -> pd.DataFrame:
    data: Dict[str, object] = {"time": times[time_mask]}
    for short_name, vcfg in VARIABLES.items():
        value = np.ma.asarray(ds.variables[vcfg["ref_var"]][row_idx, :]).filled(np.nan).astype(float)
        flag = np.ma.asarray(ds.variables[vcfg["flag_var"]][row_idx, :]).filled(9).astype(int)
        keep = np.isfinite(value) & np.isin(flag, allowed_flags) & time_mask
        filtered = np.where(keep, value, np.nan)
        data[vcfg["ref_col"]] = filtered[time_mask]
        data["%s_flag" % short_name] = flag[time_mask]

    if "is_overlap" in ds.variables:
        data["is_overlap"] = np.ma.asarray(ds.variables["is_overlap"][row_idx, :]).filled(0).astype(int)[time_mask]
    if "selected_source_index" in ds.variables:
        data["selected_source_index"] = (
            np.ma.asarray(ds.variables["selected_source_index"][row_idx, :]).filled(-1).astype(int)[time_mask]
        )
    if "selected_source_station_uid" in ds.variables:
        selected_uid = np.asarray(ds.variables["selected_source_station_uid"][row_idx, :], dtype=object).reshape(-1)
        data["selected_source_station_uid"] = [clean_text(x) for x in selected_uid[time_mask]]
    return pd.DataFrame(data)


def preload_model_all_stations(
    files: List[Path],
    station_tasks: List,
    active_vars: List[str],
    model_vars: Dict,
    valid_start: pd.Timestamp,
    valid_end: pd.Timestamp,
) -> Dict[Tuple[int, int], Dict[str, pd.DataFrame]]:
    """Pre-load model time series for ALL station grid cells in one pass.
    
    Reads all station positions at once using vectorised h5py indexing,
    reducing file opens from O(N_stations × N_vars × N_files) to O(N_files).
    The rest of the code then looks up from an in-memory dict.
    
    Returns dict[(lat_idx, lon_idx)] -> {var_name: pd.DataFrame(time, model_col)}
    """
    import h5py as _h5

    n_stations = len(station_tasks)
    lat_idxs = np.array([t[1]["indexers"]["lat_ucat"] for t in station_tasks], dtype=int)
    lon_idxs = np.array([t[1]["indexers"]["lon_ucat"] for t in station_tasks], dtype=int)

    # Accumulate per-file data: {var_name: list of (31, n_stations) arrays}
    all_times = []
    var_raw = {v: [] for v in active_vars}

    print("[INFO] Pre-loading model data for %d stations from %d files in one pass ..." % (n_stations, len(files)), flush=True)

    for fi, fpath in enumerate(files):
        with _h5.File(str(fpath), "r") as h5ds:
            # Store raw minutes since 1900-01-01 (avoids double-conversion bug)
            raw_minutes = np.asarray(h5ds["time"][:], dtype=np.float64)
            all_times.append(raw_minutes)

            for var_name in active_vars:
                grain_vars = UNITCAT_GRAIN_VARS[var_name]
                _, factor = model_vars[var_name]

                total = np.zeros((len(raw_minutes), n_stations), dtype=np.float64)
                for gv in grain_vars:
                    dset = h5ds[gv]

                    if n_stations == 1:
                        # Fast path: single station
                        arr = np.asarray(dset[:, lat_idxs[0], lon_idxs[0]], dtype=np.float64)
                        arr[arr < -1e30] = np.nan
                        total[:, 0] += arr
                    else:
                        # Group by unique lat_idx for efficient slab reads
                        unique_lats, inv_indices = np.unique(lat_idxs, return_inverse=True)
                        for ui, ulat in enumerate(unique_lats):
                            slab = np.asarray(dset[:, ulat, :], dtype=np.float64)  # (31, 1440)
                            slab[slab < -1e30] = np.nan
                            # Vectorized scatter into all stations sharing this lat
                            station_mask = (inv_indices == ui)
                            sidxs = np.where(station_mask)[0]
                            total[:, sidxs] += slab[:, lon_idxs[sidxs]]
                total *= float(factor)
                var_raw[var_name].append(total)

        if (fi + 1) % 12 == 0:
            print("[INFO]   ... loaded %d / %d files" % (fi + 1, len(files)), flush=True)

    # Concatenate time across all files, then convert to datetime ONCE
    all_minutes = np.concatenate(all_times)
    time_ts = pd.Timestamp("1900-01-01") + pd.to_timedelta(all_minutes, unit="m")
    time_mask = (time_ts >= valid_start) & (time_ts <= valid_end)

    # Pre-concatenate per-variable data (avoids N_stations concatenations)
    var_concat = {}
    for var_name in active_vars:
        var_concat[var_name] = np.concatenate(var_raw[var_name], axis=0)  # (total_t, n_stations)

    # Build result dict
    result = {}
    for si in range(n_stations):
        key = (lat_idxs[si], lon_idxs[si])
        result[key] = {}
        for var_name in active_vars:
            vcfg = VARIABLES[var_name]
            full_valid = var_concat[var_name][time_mask, si]
            result[key][var_name] = pd.DataFrame({
                "time": time_ts[time_mask],
                vcfg["model_col"]: full_valid,
            })

    print("[INFO] Model pre-load complete.", flush=True)
    return result


def compare_pair(
    reference_df: pd.DataFrame,
    model_df: pd.DataFrame,
    ref_col: str,
    model_col: str,
    resolution: str,
) -> pd.DataFrame:
    ref = aggregate_timeseries(reference_df, ref_col, resolution, "reference")
    mod = aggregate_timeseries(model_df, model_col, resolution, "model")
    return pd.merge(ref, mod, on="time", how="inner").dropna()


def maybe_plot_compare(compare_df: pd.DataFrame, title: str, ylabel: str, out_png: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(compare_df["time"], compare_df["reference"], color="tab:red", linewidth=2, label="Reference")
    ax.plot(compare_df["time"], compare_df["model"], color="tab:blue", linewidth=2, label="Model")
    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best")
    plt.xticks(rotation=45)
    plt.tight_layout()
    fig.savefig(out_png, dpi=200, bbox_inches="tight")
    plt.close(fig)


# ============================================================
# Parallel worker for station validation
# ============================================================
def _validate_one_station(args_tuple):
    """Process one station in a subprocess.
    
    Each subprocess independently opens the (OS-cached) reference matrix file.
    Returns (metrics_rows_list, variable_status_rows_list) for this station.
    """
    (
        row_dict,
        nearest,
        matrix_path_str,
        allowed_flags_tuple,
        valid_start,
        valid_end,
        resolution,
        active_vars_names,
        model_vars_map,
        model_cache_item,  # {(lat,lon): {var_name: DataFrame}}
        output_dir_str,
        make_plots,
        cfg_min_reference_points,
        cfg_min_paired_points,
    ) = args_tuple

    import numpy as np
    import pandas as pd
    from pathlib import Path
    import netCDF4 as nc4

    # Import module-level globals (they're accessible from the module)
    # Module-level names (VARIABLES, UNITCAT_GRAIN_VARS, etc.) are accessible
    # because multiprocessing fork re-imports this module in each child

    cluster_uid = clean_text(row_dict.get("cluster_uid", ""))
    station_name = clean_text(row_dict.get("station_name", ""))
    metrics_list = []
    var_status_list = []

    # Build station key for model cache
    lat_idx = nearest["indexers"]["lat_ucat"]
    lon_idx = nearest["indexers"]["lon_ucat"]
    cache_key = (lat_idx, lon_idx)

    # Open reference matrix (read-only, OS-cached)
    matrix_ds = nc4.Dataset(str(matrix_path_str), "r")
    try:
        # Build cluster_uid lookup for this file
        cluster_uids = [
            clean_text(x)
            for x in np.asarray(matrix_ds.variables["cluster_uid"][:], dtype=object).reshape(-1)
        ]
        if cluster_uid not in cluster_uids:
            for var_name in active_vars_names:
                var_status_list.append({
                    "cluster_uid": cluster_uid,
                    "station_name": station_name,
                    "variable": VARIABLES[var_name]["metric_name"],
                    "status": "skipped",
                    "reason": "cluster_uid_not_in_matrix",
                })
            return metrics_list, var_status_list

        row_idx = cluster_uids.index(cluster_uid)
        ref_times = nc_time_to_datetime(matrix_ds.variables["time"])
        ref_time_mask = (ref_times >= valid_start) & (ref_times <= valid_end)

        reference_df = extract_reference_for_station(
            matrix_ds, row_idx, ref_times, allowed_flags_tuple, ref_time_mask
        )
    finally:
        matrix_ds.close()

    # Look up model data from cache
    model_frames = []
    cached_station = model_cache_item.get(cache_key, {})
    for var_name in active_vars_names:
        vcfg = VARIABLES[var_name]
        if var_name in cached_station:
            model_frames.append(cached_station[var_name].copy())
        else:
            var_status_list.append({
                "cluster_uid": cluster_uid,
                "station_name": station_name,
                "variable": vcfg["metric_name"],
                "status": "skipped",
                "reason": "var_not_in_preload_cache",
            })

    # Write outputs
    output_dir = Path(output_dir_str)
    station_dir = output_dir / ("%s_%s" % (safe_name(cluster_uid), safe_name(station_name)))
    station_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame([{
        "cluster_uid": cluster_uid,
        "station_name": station_name,
        "station_lat": row_dict.get("lat", np.nan),
        "station_lon": row_dict.get("lon", np.nan),
        "model_grid_lat": nearest["model_grid_lat"],
        "model_grid_lon": nearest["model_grid_lon"],
        "model_grid_distance_km": nearest["model_grid_distance_km"],
        "model_grid_index": nearest["model_grid_index"],
        "validation_start": valid_start,
        "validation_end": valid_end,
    }]).to_csv(str(station_dir / "station_match.csv"), index=False)

    reference_df.to_csv(str(station_dir / "reference_timeseries.csv"), index=False)

    if model_frames:
        model_df = model_frames[0]
        for frame in model_frames[1:]:
            model_df = pd.merge(model_df, frame, on="time", how="outer")
        model_df = model_df.sort_values("time")
    else:
        model_df = pd.DataFrame({"time": []})
    model_df.to_csv(str(station_dir / "model_timeseries.csv"), index=False)

    # Per-variable validation
    for var_name in active_vars_names:
        vcfg = VARIABLES[var_name]
        ref_col = vcfg["ref_col"]
        model_col = vcfg["model_col"]
        reference_points = int(reference_df[ref_col].notna().sum()) if ref_col in reference_df else 0

        if reference_points < cfg_min_reference_points:
            var_status_list.append({
                "cluster_uid": cluster_uid,
                "station_name": station_name,
                "variable": vcfg["metric_name"],
                "status": "skipped",
                "reason": "insufficient_reference_points",
                "reference_points": reference_points,
                "paired_points": 0,
            })
            continue
        if model_col not in model_df.columns:
            continue

        compare_df = compare_pair(reference_df, model_df, ref_col, model_col, resolution)
        paired_points = int(len(compare_df))
        compare_file = station_dir / ("compare_%s_%s.csv" % (var_name, resolution))
        compare_df.rename(columns={"reference": ref_col, "model": model_col}).to_csv(str(compare_file), index=False)

        if paired_points < cfg_min_paired_points:
            var_status_list.append({
                "cluster_uid": cluster_uid,
                "station_name": station_name,
                "variable": vcfg["metric_name"],
                "status": "skipped",
                "reason": "insufficient_paired_points",
                "reference_points": reference_points,
                "paired_points": paired_points,
            })
            continue

        stats = compute_metrics(
            compare_df["reference"].to_numpy(dtype=float),
            compare_df["model"].to_numpy(dtype=float),
        )
        metrics_list.append({
            "cluster_uid": cluster_uid,
            "station_name": station_name,
            "variable": vcfg["metric_name"],
            "unit": vcfg["unit"],
            "n": stats["n"],
            "pearson_r": stats["pearson_r"],
            "rmse": stats["rmse"],
            "bias": stats["bias"],
            "nse": stats["nse"],
            "model_grid_distance_km": nearest["model_grid_distance_km"],
            "reference_points": reference_points,
        })
        var_status_list.append({
            "cluster_uid": cluster_uid,
            "station_name": station_name,
            "variable": vcfg["metric_name"],
            "status": "validated",
            "reason": "",
            "reference_points": reference_points,
            "paired_points": paired_points,
        })

        if make_plots:
            maybe_plot_compare(
                compare_df,
                "%s %s" % (station_name, vcfg["metric_name"]),
                "%s (%s)" % (var_name, vcfg["unit"]),
                station_dir / ("compare_%s_%s.png" % (var_name, resolution)),
            )

    return metrics_list, var_status_list



_LAND_POLYGONS_CACHE: Optional[List[Tuple[np.ndarray, np.ndarray]]] = None
_LAND_POLYGON_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_land.geojson"
_LAND_POLYGON_PATH = Path(__file__).resolve().parent / "ne_110m_land.geojson"


def _load_land_polygons() -> List[Tuple[np.ndarray, np.ndarray]]:
    """Download/cache Natural Earth 110m land polygons from GeoJSON.

    Returns a list of (lon_array, lat_array) for each polygon.
    On failure prints a warning and returns an empty list.
    """
    global _LAND_POLYGONS_CACHE
    if _LAND_POLYGONS_CACHE is not None:
        return _LAND_POLYGONS_CACHE

    # Try local file first (for offline compute nodes)
    data = None
    local_path = _LAND_POLYGON_PATH
    if local_path.is_file():
        try:
            with open(local_path, "r") as f:
                data = json.load(f)
            print("[INFO] Loaded land polygons from %s" % local_path)
        except Exception as exc:
            print("[WARN] Failed to load local land polygons: %s" % exc)
            data = None
    # Fall back to download
    if data is None:
        try:
            req = urllib.request.Request(
                _LAND_POLYGON_URL, headers={"User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            print("[WARN] Failed to download land polygon data: %s" % exc)
            print("[WARN] Map will be rendered without land outlines.")
            _LAND_POLYGONS_CACHE = []
            return _LAND_POLYGONS_CACHE

    polygons: List[Tuple[np.ndarray, np.ndarray]] = []
    for feature in data.get("features", []):
        geom = feature.get("geometry", {})
        geom_type = geom.get("type")
        coords = geom.get("coordinates", [])
        if geom_type == "Polygon":
            ring = np.asarray(coords[0], dtype=np.float64)
            polygons.append((ring[:, 0], ring[:, 1]))
        elif geom_type == "MultiPolygon":
            for poly_coords in coords:
                ring = np.asarray(poly_coords[0], dtype=np.float64)
                polygons.append((ring[:, 0], ring[:, 1]))

    _LAND_POLYGONS_CACHE = polygons
    return polygons


def _plot_land(ax, polygons: List[Tuple[np.ndarray, np.ndarray]]) -> None:
    """Fill land polygons on a matplotlib Axes."""
    for lon, lat in polygons:
        ax.fill(lon, lat, color="#EEEEEE", edgecolor="#CCCCCC", linewidth=0.3, zorder=0)


def _load_river_network(
    merit_dir: str,
    view_lon_min: float,
    view_lon_max: float,
    view_lat_min: float,
    view_lat_max: float,
    min_order: int = 1,
) -> "List[np.ndarray]":
    """Load river line segments from MERIT Hydro pfaf_level_02 shapefiles
    that overlap the given viewport.

    Parameters
    ----------
    min_order : int
        Minimum Strahler stream order to include.  Use 5+ for main-stem-only,
        3+ for medium rivers, 1 (default) for all.

    Returns a list of (N,2) numpy arrays, each a single PolyLine segment.
    On failure prints a warning and returns [].
    """
    import shapefile as _sf

    _PFAF_CODES = ["61", "62", "63", "64", "66", "67"]
    _SUB_DIR = "pfaf_level_02"
    _PREFIX = "riv_pfaf_"
    _SUFFIX = "_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"

    segments = []
    base = Path(merit_dir) / _SUB_DIR

    for code in _PFAF_CODES:
        shp_path = base / f"{_PREFIX}{code}{_SUFFIX}"
        if not shp_path.exists():
            continue
        try:
            sf = _sf.Reader(str(shp_path))
        except Exception as exc:
            print(f"[WARN] Failed to open river shapefile {shp_path}: {exc}", flush=True)
            continue

        # Find order field index (skip DeletionFlag)
        _field_names = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
        if "order" in _field_names:
            _order_idx = _field_names.index("order")
        else:
            _order_idx = -1

        for shape, rec in zip(sf.iterShapes(), sf.iterRecords()):
            pts = np.asarray(shape.points, dtype=np.float64)
            if len(pts) < 2:
                continue
            lons = pts[:, 0]
            lats = pts[:, 1]
            if np.all(lons < view_lon_min) or np.all(lons > view_lon_max)                or np.all(lats < view_lat_min) or np.all(lats > view_lat_max):
                continue
            # Filter by stream order
            if min_order > 1 and _order_idx >= 0:
                seg_order = int(rec[_order_idx])
                if seg_order < min_order:
                    continue
            segments.append(pts)

        sf.close()

    if not segments:
        print(f"[WARN] No river segments loaded from {base}", flush=True)
    else:
        print(f"[INFO] Loaded {len(segments)} river segments (order >= {min_order}) from MERIT Hydro", flush=True)
    return segments


def plot_model_domain(
    lat_arr: np.ndarray,
    lon_arr: np.ndarray,
    bounds: Dict[str, float],
    candidate_rows: List[Dict[str, object]],
    station_tasks: List[Tuple],
    valid_start: pd.Timestamp,
    valid_end: pd.Timestamp,
    output_dir: Path,
    region_lat_min: Optional[float] = None,
    region_lat_max: Optional[float] = None,
    region_lon_min: Optional[float] = None,
    region_lon_max: Optional[float] = None,
    map_type: str = "global",
    merit_hydro_dir: Optional[str] = None,
) -> None:
    """Generate a spatial overview map of model domain and reference stations.

    Parameters
    ----------
    map_type : "global" or "region"
        * "global" — full global view with subsampled grid (default).
        * "region" — zoomed to the user-specified region with river overlay
          and station labels.
    merit_hydro_dir : str or None
        Path to MERIT Hydro shapefile directory (required for map_type="region").

    The base map has three layers:
      1. Land polygons (if GeoJSON is reachable)
      2. Model bounding box + subsampled grid points
      3. Reference stations colored by their filter status
    Saved to <output_dir>/model_domain_overview.png  (global)
    or   <output_dir>/model_domain_overview_region.png  (region)
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if map_type == "region":
        fig, ax = plt.subplots(figsize=(12, 10))
    else:
        fig, ax = plt.subplots(figsize=(16, 8))

    # --- 1. Land polygons ---
    polygons = _load_land_polygons()
    if polygons:
        _plot_land(ax, polygons)

    # --- 2. Model domain: subsampled grid points + bounding box ---
    step = 2 if map_type == "region" else 10
    lat_subsampled = lat_arr[::step]
    lon_subsampled = lon_arr[::step]
    # convert lon to lon180 for display
    lon180 = ((lon_subsampled + 180.0) % 360.0) - 180.0
    # Create meshgrid for scatter
    lon_grid, lat_grid = np.meshgrid(lon180, lat_subsampled)
    ax.scatter(
        lon_grid.ravel(), lat_grid.ravel(),
        s=1, color="gray", alpha=0.3, linewidths=0, zorder=1,
        label="Model grid (every %dth)" % step,
    )
    # Bounding box
    bbox_lon = [bounds["lon_min_180"], bounds["lon_max_180"],
                bounds["lon_max_180"], bounds["lon_min_180"], bounds["lon_min_180"]]
    bbox_lat = [bounds["lat_min"], bounds["lat_min"],
                bounds["lat_max"], bounds["lat_max"], bounds["lat_min"]]
    ax.plot(bbox_lon, bbox_lat, color="red", linestyle="--", linewidth=1.5,
            alpha=0.7, zorder=2, label="Model domain bbox")

    # --- 3. Reference stations colored by filter status ---
    status_style = {
        "candidate": {"color": "#2ca02c", "marker": "o", "s": 15, "label": "Candidate"},
        "outside_model_domain": {"color": "#d62728", "marker": "x", "s": 8, "label": "Skipped: outside model domain"},
        "outside_user_region": {"color": "#1f77b4", "marker": "x", "s": 8, "label": "Skipped: outside user region"},
        "model_grid_distance_gt_threshold": {"color": "#ff7f0e", "marker": "x", "s": 8, "label": "Skipped: distance > %d km" % DEFAULT_MAX_GRID_DISTANCE_KM},
        "no_time_overlap": {"color": "#9467bd", "marker": "x", "s": 8, "label": "Skipped: no time overlap"},
        "invalid_lat_lon": {"color": "#7f7f7f", "marker": "x", "s": 8, "label": "Skipped: invalid lat/lon"},
        "not_processed_max_stations": {"color": "#cccccc", "marker": "x", "s": 8, "label": "Skipped: max stations limit"},
    }
    # Collect lon180 for each candidate row
    lon180_station = np.array([
        float(to_lon180(r["lon"])) if np.isfinite(r.get("lon", np.nan)) else np.nan
        for r in candidate_rows
    ])
    lats = np.array([float(r["lat"]) if np.isfinite(r.get("lat", np.nan)) else np.nan for r in candidate_rows])
    statuses = [r.get("candidate_status", "") for r in candidate_rows]
    reasons = [r.get("filter_reason", "") for r in candidate_rows]

    # Determine effective status key for legend
    def _status_key(status: str, reason: str) -> str:
        if status == "candidate":
            return "candidate"
        return reason if reason in status_style else "invalid_lat_lon"

    for key, style in status_style.items():
        mask = np.array([_status_key(s, r) == key for s, r in zip(statuses, reasons)])
        if not mask.any():
            continue
        ax.scatter(
            lon180_station[mask], lats[mask],
            color=style["color"], marker=style["marker"], s=style["s"],
            alpha=0.6, linewidths=0.5 if style["marker"] == "o" else 1,
            zorder=3, label=style["label"], 
        )

    # --- 4. River network (region map only) ---
    if map_type == "region" and merit_hydro_dir:
        _rl_min = region_lon_min if region_lon_min is not None else -80.0
        _rl_max = region_lon_max if region_lon_max is not None else -45.0
        _rb_min = region_lat_min if region_lat_min is not None else -20.0
        _rb_max = region_lat_max if region_lat_max is not None else 5.0
        river_segs = _load_river_network(merit_hydro_dir, _rl_min, _rl_max, _rb_min, _rb_max, min_order=5)
        if river_segs:
            from matplotlib.collections import LineCollection
            lc = LineCollection(river_segs, colors="#3366CC", linewidths=0.4, alpha=0.6, zorder=1.5)
            ax.add_collection(lc)

    # --- 5. Station labels (region map only) ---
    if map_type == "region":
        for r in candidate_rows:
            if r.get("candidate_status") != "candidate":
                continue
            s_lat = r.get("lat", np.nan)
            s_lon = r.get("lon", np.nan)
            if not np.isfinite(s_lat) or not np.isfinite(s_lon):
                continue
            s_lon180 = float(to_lon180(s_lon))
            s_name = clean_text(r.get("station_name", ""))
            s_river = clean_text(r.get("river_name", ""))
            label = s_name
            if s_river:
                label += "\n(%s)" % s_river
            ax.annotate(
                label,
                (s_lon180, s_lat),
                fontsize=6,
                xytext=(5, 4),
                textcoords="offset points",
                alpha=0.85,
                zorder=4,
            )

    # --- Labels, legend, grid ---
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(
        "Model Domain Overview\nValidation: %s to %s" % (valid_start.date(), valid_end.date()) if map_type == "global" \
            else "Model Domain Overview \u2014 Regional\nValidation: %s to %s" % (valid_start.date(), valid_end.date()), fontsize=13
    )
    # Apply region cropping: only zoom for regional map, global map always shows full globe
    if map_type == "region" and (region_lat_min is not None or region_lat_max is not None or region_lon_min is not None or region_lon_max is not None):
        # Convert user lon to -180..180 range
        lon_min = to_lon180(region_lon_min) if region_lon_min is not None else -80.0
        lon_max = to_lon180(region_lon_max) if region_lon_max is not None else -45.0
        lat_min = region_lat_min if region_lat_min is not None else -20.0
        lat_max = region_lat_max if region_lat_max is not None else 5.0
        # Add a small padding (5%) so stations near edges are visible
        lat_pad = (lat_max - lat_min) * 0.05
        lon_pad = (lon_max - lon_min) * 0.05
        ax.set_xlim(lon_min - lon_pad, lon_max + lon_pad)
        ax.set_ylim(lat_min - lat_pad, lat_max + lat_pad)
    else:
        ax.set_xlim(-180, 180)
        ax.set_ylim(-90, 90)
    ax.grid(True, alpha=0.2, linewidth=0.3)
    legend = ax.legend(loc="lower left", fontsize=7, markerscale=0.8, framealpha=0.8)
    for lh in legend.legend_handles:
        lh._sizes = [20]

    plt.tight_layout()
    if map_type == "region":
        out_path = output_dir / "model_domain_overview_region.png"
    else:
        out_path = output_dir / "model_domain_overview.png"
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    print("[INFO] Model domain overview saved: %s" % out_path)




def plot_model_spatial_mean(
    files: List[Path],
    lat: np.ndarray,
    lon: np.ndarray,
    active_vars: List[str],
    model_vars: Dict,
    output_dir: Path,
    region_lat_min: Optional[float] = None,
    region_lat_max: Optional[float] = None,
    region_lon_min: Optional[float] = None,
    region_lon_max: Optional[float] = None,
) -> None:
    """Compute and plot the multi-year time-mean spatial distribution for each active variable.

    Reads all unitcat files once, computes the time-mean for each grid cell,
    and saves a multi-panel figure to output_dir/model_spatial_mean.png.
    """
    import h5py as _h5
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize

    n_vars = len(active_vars)
    ncols = max(n_vars, 1)

    fig, axes = plt.subplots(1, ncols, figsize=(ncols * 7, 6))
    if ncols == 1:
        axes = [axes]

    # Determine region slice for imshow extent
    if region_lat_min is not None or region_lat_max is not None or region_lon_min is not None or region_lon_max is not None:
        extent_lon_min = to_lon180(region_lon_min) if region_lon_min is not None else -180.0
        extent_lon_max = to_lon180(region_lon_max) if region_lon_max is not None else 180.0
        extent_lat_min = region_lat_min if region_lat_min is not None else -90.0
        extent_lat_max = region_lat_max if region_lat_max is not None else 90.0
    else:
        extent_lon_min, extent_lon_max = -180.0, 180.0
        extent_lat_min, extent_lat_max = -90.0, 90.0

    print("[INFO] Computing time-mean spatial distribution for %d variable(s) ..." % n_vars, flush=True)

    # Accumulate time-mean fields: {var_name -> 2D (lat, lon) array}
    accum = {}
    for var_name in active_vars:
        accum[var_name] = None
    n_files = 0

    total_files = len(files)
    for fi, fpath in enumerate(files):
        with _h5.File(str(fpath), "r") as h5ds:
            for var_name in active_vars:
                grain_vars = UNITCAT_GRAIN_VARS[var_name]
                _, factor = model_vars[var_name]

                # Sum across grain fractions, then time-mean
                total = None
                for gv in grain_vars:
                    arr = np.asarray(h5ds[gv][:, :, :], dtype=np.float64)  # (ntime, lat, lon)
                    arr[arr < -1e30] = np.nan
                    if total is None:
                        total = np.nanmean(arr, axis=0)  # time-mean -> (lat, lon)
                    else:
                        total += np.nanmean(arr, axis=0)
                total *= float(factor)

                if accum[var_name] is None:
                    accum[var_name] = total
                else:
                    accum[var_name] += total

        n_files += 1
        if (fi + 1) % 24 == 0:
            print("[INFO]   ... processed %d / %d files" % (fi + 1, total_files), flush=True)

    print("[INFO]   ... done. Computing color scales and plotting ...", flush=True)

    # Determine common color limits across variables (2nd–98th percentile)
    all_valid_values = []
    for var_name in active_vars:
        fld = accum[var_name] / n_files
        valid = fld[np.isfinite(fld) & (fld > 0)]
        if len(valid) > 0:
            all_valid_values.extend([np.percentile(valid, 2), np.percentile(valid, 98)])

    vmin_all = min(all_valid_values) if all_valid_values else 0
    vmax_all = max(all_valid_values) if all_valid_values else 1

    # Map lon to -180..180 for display
    lon_display = to_lon180(lon)
    # imshow extent maps data coords: [left, right, bottom, top] 
    # (lat is descending, origin='upper' so first pixel = lat[0])
    imshow_extent = [lon_display[0], lon_display[-1], lat[-1], lat[0]]

    for vi, var_name in enumerate(active_vars):
        ax = axes[vi]
        fld = accum[var_name] / n_files

        # Mask NaN and zeros for display
        fld_display = np.where(np.isfinite(fld) & (fld > 0), fld, np.nan)

        im = ax.imshow(
            fld_display,
            extent=imshow_extent,
            origin='upper', aspect='auto',
            cmap='YlOrRd',
            norm=Normalize(vmin=vmin_all, vmax=vmax_all),
            interpolation='nearest',
        )
        ax.set_xlim(extent_lon_min, extent_lon_max)
        ax.set_ylim(extent_lat_min, extent_lat_max)
        ax.set_title(VARIABLES[var_name]["metric_name"], fontsize=12, fontweight='bold')
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.grid(True, linestyle='--', alpha=0.3, linewidth=0.5)
        cbar = fig.colorbar(im, ax=ax, shrink=0.85)
        cbar.set_label(VARIABLES[var_name]["unit"], fontsize=10)

    fig.suptitle("Multi-year time-mean spatial distribution (1995-2000)", fontsize=14, fontweight='bold')
    plt.tight_layout()
    out_path = output_dir / "model_spatial_mean.png"
    fig.savefig(str(out_path), dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("[INFO] Time-mean spatial map saved: %s" % out_path, flush=True)




def main() -> None:
    #  Assemble configuration 
    cfg = {                                       
        "model_nc": DEFAULT_MODEL_NC,             
        "model_nc_pattern": DEFAULT_MODEL_NC_PATTERN,
        "model_time_name": DEFAULT_MODEL_TIME_NAME,
        "model_lat_name": DEFAULT_MODEL_LAT_NAME, 
        "model_lon_name": DEFAULT_MODEL_LON_NAME, 
        "model_q_var": DEFAULT_MODEL_Q_VAR,       
        "model_ssc_var": DEFAULT_MODEL_SSC_VAR,   
        "model_ssl_var": DEFAULT_MODEL_SSL_VAR,   
        "model_q_factor": DEFAULT_MODEL_Q_FACTOR, 
        "model_ssc_factor": DEFAULT_MODEL_SSC_FACTOR,
        "model_ssl_factor": DEFAULT_MODEL_SSL_FACTOR,
        "reference_dir": DEFAULT_REFERENCE_DIR,   
        "resolution": DEFAULT_RESOLUTION,         
        "allowed_flags": DEFAULT_ALLOWED_FLAGS,   
        "min_reference_points": DEFAULT_MIN_REFERENCE_POINTS,
        "min_paired_points": DEFAULT_MIN_PAIRED_POINTS,
        "max_grid_distance_km": DEFAULT_MAX_GRID_DISTANCE_KM,
        "start_date": DEFAULT_START_DATE,         
        "end_date": DEFAULT_END_DATE,             
        "output_dir": DEFAULT_OUTPUT_DIR,         
        "max_stations": DEFAULT_MAX_STATIONS,     
        "region_lat_min": DEFAULT_REGION_LAT_MIN,
        "region_lat_max": DEFAULT_REGION_LAT_MAX,
        "region_lon_min": DEFAULT_REGION_LON_MIN,
        "region_lon_max": DEFAULT_REGION_LON_MAX,
        "make_plots": DEFAULT_MAKE_PLOTS,         
        "num_workers": DEFAULT_NUM_WORKERS,         
        "merit_hydro_dir": DEFAULT_MERIT_HYDRO_DIR,
    }                                              
    # 

    #  基本校验 
    if not cfg["model_nc"]:
        raise ValueError("DEFAULT_MODEL_NC must be set.")
    if not cfg["reference_dir"]:
        raise ValueError("DEFAULT_REFERENCE_DIR must be set.")
    if not cfg["output_dir"]:
        raise ValueError("DEFAULT_OUTPUT_DIR must be set.")
    if not any([cfg["model_q_var"], cfg["model_ssc_var"], cfg["model_ssl_var"]]):
        raise ValueError(
            "At least one model variable must be provided: "
            "DEFAULT_MODEL_Q_VAR / DEFAULT_MODEL_SSC_VAR / DEFAULT_MODEL_SSL_VAR."
        )

    if cfg["resolution"] not in MATRIX_FILES:
        raise ValueError("resolution must be one of: %s" % ", ".join(sorted(MATRIX_FILES)))

    allowed_flags = parse_allowed_flags(cfg["allowed_flags"])
    reference_dir = Path(cfg["reference_dir"]).resolve()
    output_dir = Path(cfg["output_dir"]).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    model_vars = {
        "Q": (cfg["model_q_var"], cfg["model_q_factor"]),
        "SSC": (cfg["model_ssc_var"], cfg["model_ssc_factor"]),
        "SSL": (cfg["model_ssl_var"], cfg["model_ssl_factor"]),
    }
    active_vars = [name for name, (var_name, _) in model_vars.items() if var_name]

    model_dir = Path(cfg["model_nc"]).resolve()
    nc_files = list_model_files(model_dir, cfg["model_nc_pattern"])
    lat_arr, lon_arr = load_unitcat_coords(nc_files[0])
    model_start, model_end = get_unitcat_model_time_range(nc_files)
    user_start = parse_start_date(cfg["start_date"])
    user_end = parse_end_date(cfg["end_date"], cfg["resolution"])
    valid_start = max([x for x in [model_start, user_start] if x is not None])
    valid_end = min([x for x in [model_end, user_end] if x is not None])
    if valid_start > valid_end:
        raise ValueError("Requested validation window has no overlap with model time range.")

    bounds = model_domain_bounds(lat_arr, lon_arr)

    catalog = load_station_catalog(reference_dir, cfg["resolution"])
    matrix_path = reference_dir / MATRIX_FILES[cfg["resolution"]]
    if not matrix_path.exists():
        raise FileNotFoundError("Reference matrix file not found: %s" % matrix_path)
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to read reference matrix files.")

    candidate_rows: List[Dict[str, object]] = []
    station_tasks: List[Tuple[pd.Series, Dict[str, object]]] = []
    for _, row in catalog.iterrows():
        base = {
            "cluster_uid": clean_text(row.get("cluster_uid", "")),
            "source_station_id": clean_text(row.get("source_station_id", "")),
            "station_name": clean_text(row.get("station_name", "")),
            "river_name": clean_text(row.get("river_name", "")),
            "lat": row.get("lat", np.nan),
            "lon": row.get("lon", np.nan),
            "record_count": row.get("record_count", np.nan),
            "time_start": row.get("time_start", ""),
            "time_end": row.get("time_end", ""),
            "candidate_status": "candidate",
            "filter_reason": "",
            "model_grid_lat": np.nan,
            "model_grid_lon": np.nan,
            "model_grid_distance_km": np.nan,
            "model_grid_index": "",
        }
        if not np.isfinite(row["lat"]) or not np.isfinite(row["lon"]):
            base["candidate_status"] = "skipped"
            base["filter_reason"] = "invalid_lat_lon"
            candidate_rows.append(base)
            continue
        if not station_in_bbox(float(row["lat"]), float(row["lon"]), bounds):
            base["candidate_status"] = "skipped"
            base["filter_reason"] = "outside_model_domain"
            candidate_rows.append(base)
            continue

        # --- 用户自定义区域筛选 ---
        region_lat_min = cfg.get("region_lat_min")
        region_lat_max = cfg.get("region_lat_max")
        region_lon_min = cfg.get("region_lon_min")
        region_lon_max = cfg.get("region_lon_max")
        if any(x is not None for x in [region_lat_min, region_lat_max, region_lon_min, region_lon_max]):
            station_lat = float(row["lat"])
            station_lon = float(row["lon"])
            in_region = True
            if region_lat_min is not None and station_lat < region_lat_min:
                in_region = False
            if region_lat_max is not None and station_lat > region_lat_max:
                in_region = False
            if region_lon_min is not None and station_lon < region_lon_min:
                in_region = False
            if region_lon_max is not None and station_lon > region_lon_max:
                in_region = False
            if not in_region:
                base["candidate_status"] = "skipped"
                base["filter_reason"] = "outside_user_region"
                candidate_rows.append(base)
                continue

        if not time_overlap(valid_start, valid_end, row["time_start_ts"], row["time_end_ts"]):

            base["candidate_status"] = "skipped"
            base["filter_reason"] = "no_time_overlap"
            candidate_rows.append(base)
            continue

        nearest = find_nearest_model_cell(lat_arr, lon_arr, float(row["lat"]), float(row["lon"]))
        base.update({k: nearest[k] for k in ["model_grid_lat", "model_grid_lon", "model_grid_distance_km", "model_grid_index"]})
        if float(nearest["model_grid_distance_km"]) > float(cfg["max_grid_distance_km"]):
            base["candidate_status"] = "skipped"
            base["filter_reason"] = "model_grid_distance_gt_threshold"
            candidate_rows.append(base)
            continue

        station_tasks.append((row, nearest))
        candidate_rows.append(base)

    if cfg["max_stations"] and cfg["max_stations"] > 0:
        allowed_clusters = {clean_text(row.get("cluster_uid", "")) for row, _ in station_tasks[: cfg["max_stations"]]}
        station_tasks = station_tasks[: cfg["max_stations"]]
        for item in candidate_rows:
            if item["candidate_status"] == "candidate" and item["cluster_uid"] not in allowed_clusters:
                item["candidate_status"] = "skipped"
                item["filter_reason"] = "not_processed_max_stations"

    # --- Plot model domain overview ---
    plot_model_domain(
        lat_arr=lat_arr, lon_arr=lon_arr, bounds=bounds,
        candidate_rows=candidate_rows, station_tasks=station_tasks,
        valid_start=valid_start, valid_end=valid_end,
        output_dir=output_dir,
        region_lat_min=cfg.get("region_lat_min"),
        region_lat_max=cfg.get("region_lat_max"),
        region_lon_min=cfg.get("region_lon_min"),
        region_lon_max=cfg.get("region_lon_max"),
        map_type="global",
        merit_hydro_dir=None,
    )
    # --- Plot regional zoomed map ---
    plot_model_domain(
        lat_arr=lat_arr, lon_arr=lon_arr, bounds=bounds,
        candidate_rows=candidate_rows, station_tasks=station_tasks,
        valid_start=valid_start, valid_end=valid_end,
        output_dir=output_dir,
        region_lat_min=cfg.get("region_lat_min"),
        region_lat_max=cfg.get("region_lat_max"),
        region_lon_min=cfg.get("region_lon_min"),
        region_lon_max=cfg.get("region_lon_max"),
        map_type="region",
        merit_hydro_dir=cfg.get("merit_hydro_dir"),
    )

    print("[INFO] Candidate stations in region: %d" % sum(1 for r in candidate_rows if r.get("candidate_status") == "candidate"))

    # --- Plot time-mean spatial distribution ---
    plot_model_spatial_mean(
        files=nc_files,
        lat=lat_arr, lon=lon_arr,
        active_vars=active_vars,
        model_vars=model_vars,
        output_dir=output_dir,
        region_lat_min=cfg.get("region_lat_min"),
        region_lat_max=cfg.get("region_lat_max"),
        region_lon_min=cfg.get("region_lon_min"),
        region_lon_max=cfg.get("region_lon_max"),
    )

    print("[INFO] Loading reference matrix (%.1f GB)... " % (1.8), end="", flush=True)

    matrix_ds = nc4.Dataset(str(matrix_path), "r")
    print("[INFO] Matrix loaded (%d stations, %d time steps)." % (
        len(matrix_ds.variables["cluster_uid"]),
        len(matrix_ds.variables["time"]),
    ))

    # --- Model cache with disk persist (skip pre-load if unchanged) ---
    import hashlib
    import pickle
    _cache_dir = output_dir / ".model_cache"
    _cache_dir.mkdir(parents=True, exist_ok=True)
    # Hash of (model file list + mod times + active vars + time window) for cache key
    _cache_key_input = (
        [(str(f), os.path.getmtime(f)) for f in nc_files],
        sorted(active_vars),
        str(valid_start), str(valid_end),
    )
    _cache_key = hashlib.md5(repr(_cache_key_input).encode()).hexdigest()
    _cache_pkl = _cache_dir / f"model_cache_{_cache_key}.pkl"

    if _cache_pkl.exists():
        print("[INFO] Loading model cache from %s" % _cache_pkl, flush=True)
        with open(_cache_pkl, "rb") as _fh:
            model_cache = pickle.load(_fh)
    else:
        model_cache = preload_model_all_stations(
            files=nc_files,
            station_tasks=station_tasks,
            active_vars=active_vars,
            model_vars=model_vars,
            valid_start=valid_start,
            valid_end=valid_end,
        )
        # Save cache (clean up old caches if more than 5)
        with open(_cache_pkl, "wb") as _fh:
            pickle.dump(model_cache, _fh, protocol=pickle.HIGHEST_PROTOCOL)
        _old_caches = sorted(_cache_dir.glob("model_cache_*.pkl"))
        for _old in _old_caches[:-5]:
            _old.unlink()
        print("[INFO] Model cache saved to %s" % _cache_pkl, flush=True)

    print("[INFO] Starting parallel station validation with %d workers..." % cfg["num_workers"], flush=True)

    # Close the reference matrix in the main process (workers will re-open it)
    matrix_ds.close()

    n_stations = len(station_tasks)
    num_workers = cfg["num_workers"]
    if num_workers <= 0:
        num_workers = min(32, (os.cpu_count() or 4) // 2)

    # Prepare args tuples
    station_args_list = []
    for row, nearest in station_tasks:
        row_dict = {
            "cluster_uid": clean_text(row.get("cluster_uid", "")),
            "station_name": clean_text(row.get("station_name", "")),
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "source_station_id": clean_text(row.get("source_station_id", "")),
            "river_name": clean_text(row.get("river_name", "")),
        }
        cache_key = (nearest["indexers"]["lat_ucat"], nearest["indexers"]["lon_ucat"])
        station_args_list.append((
            row_dict,
            nearest,
            str(matrix_path),
            allowed_flags,
            valid_start,
            valid_end,
            cfg["resolution"],
            active_vars,
            model_vars,
            {cache_key: model_cache.get(cache_key, {})},
            str(output_dir),
            cfg["make_plots"],
            int(cfg["min_reference_points"]),
            int(cfg["min_paired_points"]),
        ))

    metrics_rows = []
    variable_status_rows = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        fut_list = [executor.submit(_validate_one_station, a) for a in station_args_list]
        done = 0
        for fut in concurrent.futures.as_completed(fut_list):
            done += 1
            if done == 1 or done % 10 == 0 or done == n_stations:
                print("[INFO]   ... completed station %d/%d" % (done, n_stations), flush=True)
            try:
                m, v = fut.result()
                metrics_rows.extend(m)
                variable_status_rows.extend(v)
            except Exception as exc:
                print("[WARN] Station task failed: %s" % exc, flush=True)


    print("[INFO] Exporting CSV results...", flush=True)
    candidate_df = pd.DataFrame(candidate_rows)
    status_df = pd.DataFrame(variable_status_rows)
    if not status_df.empty:
        status_pivot = status_df.pivot_table(
            index="cluster_uid",
            columns="variable",
            values="status",
            aggfunc="first",
        )
        status_pivot.columns = ["%s_status" % c for c in status_pivot.columns]
        status_pivot = status_pivot.reset_index()
        candidate_df = pd.merge(candidate_df, status_pivot, on="cluster_uid", how="left")

    candidate_df.to_csv(output_dir / "candidate_station_catalog.csv", index=False)
    status_df.to_csv(output_dir / "variable_status_summary.csv", index=False)
    pd.DataFrame(metrics_rows).to_csv(output_dir / "metrics_summary.csv", index=False)

    print("[INFO] reference dir: %s" % reference_dir)
    print("[INFO] model nc: %s" % Path(cfg["model_nc"]).resolve())
    print("[INFO] validation window: %s -> %s" % (valid_start, valid_end))
    print("[INFO] candidate rows: %d" % len(candidate_df))
    print("[INFO] processed stations: %d" % len(station_tasks))
    print("[INFO] metric rows: %d" % len(metrics_rows))
    print("[INFO] output dir: %s" % output_dir)

if __name__ == "__main__":
    main()
