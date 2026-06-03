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
import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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


# ──────────────────────────────────────────────
#  Default configuration constants
#  Edit these values before running.
# ──────────────────────────────────────────────

# --- 模型 NetCDF ---
DEFAULT_MODEL_NC = "/share/home/dq134/wzx/CoLM/cases/sed_test_tune2/history"                           # 模型 NetCDF 文件路径（必填）
DEFAULT_MODEL_TIME_NAME = "time"                # 模型时间坐标名
DEFAULT_MODEL_LAT_NAME = "lat"                  # 模型纬度变量/坐标名
DEFAULT_MODEL_LON_NAME = "lon"                  # 模型经度变量/坐标名

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

# --- 时间窗口 ---
DEFAULT_START_DATE = "1995-01-01"               # 验证起始日期（空 = 不限制）
DEFAULT_END_DATE = "1999-09-30"                 # 验证截止日期（空 = 不限制）

# --- 其他 ---
DEFAULT_OUTPUT_DIR = "./validation_results"     # 输出目录路径（必填）
DEFAULT_MAX_STATIONS = 0                        # 最大处理站数（0 = 不限制）
DEFAULT_MAKE_PLOTS = False                      # 是否输出逐对比 PNG 图


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


def open_model_dataset(model_nc: str) -> xr.Dataset:
    path = Path(model_nc).expanduser()
    if not path.exists():
        raise FileNotFoundError("Model NetCDF not found: %s" % path)
    return xr.open_dataset(str(path))


def get_model_time_range(ds: xr.Dataset, time_name: str) -> Tuple[pd.Timestamp, pd.Timestamp, pd.DatetimeIndex]:
    if time_name not in ds:
        raise KeyError("Model time coordinate not found: %s" % time_name)
    times = pd.to_datetime(ds[time_name].values)
    times = pd.DatetimeIndex(times).dropna()
    if len(times) == 0:
        raise ValueError("Model time coordinate has no valid timestamps.")
    return pd.Timestamp(times.min()), pd.Timestamp(times.max()), times


def get_coord_dataarray(ds: xr.Dataset, name: str) -> xr.DataArray:
    if name not in ds:
        raise KeyError("Model coordinate/variable not found: %s" % name)
    return ds[name]


def model_domain_bounds(lat_da: xr.DataArray, lon_da: xr.DataArray) -> Dict[str, float]:
    lat = np.asarray(lat_da.values, dtype=float)
    lon = np.asarray(lon_da.values, dtype=float)
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
    lat_da: xr.DataArray,
    lon_da: xr.DataArray,
    station_lat: float,
    station_lon: float,
) -> Dict[str, object]:
    lat = np.asarray(lat_da.values, dtype=float)
    lon = np.asarray(lon_da.values, dtype=float)

    if lat_da.ndim == 1 and lon_da.ndim == 1:
        target_lon = maybe_to_model_lon(station_lon, lon)
        lat_idx = int(np.nanargmin(np.abs(lat - station_lat)))
        lon_idx = int(np.nanargmin(np.abs(lon - target_lon)))
        grid_lat = float(lat[lat_idx])
        grid_lon = float(lon[lon_idx])
        distance = float(haversine_km(station_lat, station_lon, grid_lat, float(to_lon180(grid_lon))))
        return {
            "indexers": {lat_da.dims[0]: lat_idx, lon_da.dims[0]: lon_idx},
            "model_grid_lat": grid_lat,
            "model_grid_lon": grid_lon,
            "model_grid_distance_km": distance,
            "model_grid_index": "%s=%d,%s=%d" % (lat_da.dims[0], lat_idx, lon_da.dims[0], lon_idx),
        }

    if lat.shape != lon.shape:
        raise ValueError("2D model lat/lon variables must have the same shape.")
    distance_grid = haversine_km(station_lat, station_lon, lat, to_lon180(lon))
    flat_idx = int(np.nanargmin(distance_grid))
    multi_idx = np.unravel_index(flat_idx, distance_grid.shape)
    indexers = {dim: int(idx) for dim, idx in zip(lat_da.dims, multi_idx)}
    return {
        "indexers": indexers,
        "model_grid_lat": float(lat[multi_idx]),
        "model_grid_lon": float(lon[multi_idx]),
        "model_grid_distance_km": float(distance_grid[multi_idx]),
        "model_grid_index": ",".join("%s=%d" % (d, i) for d, i in indexers.items()),
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


def extract_model_series(
    ds: xr.Dataset,
    variable_name: str,
    factor: float,
    time_name: str,
    indexers: Dict[str, int],
    output_col: str,
) -> pd.DataFrame:
    if variable_name not in ds.data_vars:
        raise KeyError("Model variable not found: %s" % variable_name)
    arr = ds[variable_name]
    valid_indexers = {dim: idx for dim, idx in indexers.items() if dim in arr.dims}
    arr = arr.isel(valid_indexers)
    extra_dims = [dim for dim in arr.dims if dim != time_name]
    for dim in extra_dims:
        arr = arr.sum(dim=dim, skipna=True)
    if time_name not in arr.dims:
        raise ValueError("Model variable '%s' has no time dimension after extraction." % variable_name)
    return pd.DataFrame(
        {
            "time": pd.to_datetime(arr[time_name].values, errors="coerce"),
            output_col: np.asarray(arr.values, dtype=float) * float(factor),
        }
    ).dropna(subset=["time"])


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


def main() -> None:
    # ── Assemble configuration ─────────────────┐
    cfg = {                                       │
        "model_nc": DEFAULT_MODEL_NC,             │
        "model_time_name": DEFAULT_MODEL_TIME_NAME,│
        "model_lat_name": DEFAULT_MODEL_LAT_NAME, │
        "model_lon_name": DEFAULT_MODEL_LON_NAME, │
        "model_q_var": DEFAULT_MODEL_Q_VAR,       │
        "model_ssc_var": DEFAULT_MODEL_SSC_VAR,   │
        "model_ssl_var": DEFAULT_MODEL_SSL_VAR,   │
        "model_q_factor": DEFAULT_MODEL_Q_FACTOR, │
        "model_ssc_factor": DEFAULT_MODEL_SSC_FACTOR,│
        "model_ssl_factor": DEFAULT_MODEL_SSL_FACTOR,│
        "reference_dir": DEFAULT_REFERENCE_DIR,   │
        "resolution": DEFAULT_RESOLUTION,         │
        "allowed_flags": DEFAULT_ALLOWED_FLAGS,   │
        "min_reference_points": DEFAULT_MIN_REFERENCE_POINTS,│
        "min_paired_points": DEFAULT_MIN_PAIRED_POINTS,│
        "max_grid_distance_km": DEFAULT_MAX_GRID_DISTANCE_KM,│
        "start_date": DEFAULT_START_DATE,         │
        "end_date": DEFAULT_END_DATE,             │
        "output_dir": DEFAULT_OUTPUT_DIR,         │
        "max_stations": DEFAULT_MAX_STATIONS,     │
        "make_plots": DEFAULT_MAKE_PLOTS,         │
    }                                              │
    # ────────────────────────────────────────────┘

    # ── 基本校验 ────────────────────────────
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

    ds_model = open_model_dataset(cfg["model_nc"])
    try:
        model_start, model_end, _ = get_model_time_range(ds_model, cfg["model_time_name"])
        user_start = parse_start_date(cfg["start_date"])
        user_end = parse_end_date(cfg["end_date"], cfg["resolution"])
        valid_start = max([x for x in [model_start, user_start] if x is not None])
        valid_end = min([x for x in [model_end, user_end] if x is not None])
        if valid_start > valid_end:
            raise ValueError("Requested validation window has no overlap with model time range.")

        lat_da = get_coord_dataarray(ds_model, cfg["model_lat_name"])
        lon_da = get_coord_dataarray(ds_model, cfg["model_lon_name"])
        bounds = model_domain_bounds(lat_da, lon_da)

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
            if not time_overlap(valid_start, valid_end, row["time_start_ts"], row["time_end_ts"]):
                base["candidate_status"] = "skipped"
                base["filter_reason"] = "no_time_overlap"
                candidate_rows.append(base)
                continue

            nearest = find_nearest_model_cell(lat_da, lon_da, float(row["lat"]), float(row["lon"]))
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

        matrix_ds = nc4.Dataset(str(matrix_path), "r")
        try:
            cluster_uids = [
                clean_text(x)
                for x in np.asarray(matrix_ds.variables["cluster_uid"][:], dtype=object).reshape(-1)
            ]
            cluster_lookup = {cluster_uid: idx for idx, cluster_uid in enumerate(cluster_uids)}
            ref_times = nc_time_to_datetime(matrix_ds.variables["time"])
            ref_time_mask = (ref_times >= valid_start) & (ref_times <= valid_end)

            metrics_rows: List[Dict[str, object]] = []
            variable_status_rows: List[Dict[str, object]] = []

            for row, nearest in station_tasks:
                cluster_uid = clean_text(row.get("cluster_uid", ""))
                if cluster_uid not in cluster_lookup:
                    for var_name in active_vars:
                        variable_status_rows.append({
                            "cluster_uid": cluster_uid,
                            "station_name": clean_text(row.get("station_name", "")),
                            "variable": VARIABLES[var_name]["metric_name"],
                            "status": "skipped",
                            "reason": "cluster_uid_not_in_matrix",
                        })
                    continue

                row_idx = int(cluster_lookup[cluster_uid])
                reference_df = extract_reference_for_station(matrix_ds, row_idx, ref_times, allowed_flags, ref_time_mask)

                model_frames = []
                for var_name in active_vars:
                    vcfg = VARIABLES[var_name]
                    model_var, factor = model_vars[var_name]
                    try:
                        model_frames.append(
                            extract_model_series(
                                ds_model,
                                model_var,
                                factor,
                                cfg["model_time_name"],
                                nearest["indexers"],
                                vcfg["model_col"],
                            )
                        )
                    except Exception as exc:
                        variable_status_rows.append({
                            "cluster_uid": cluster_uid,
                            "station_name": clean_text(row.get("station_name", "")),
                            "variable": vcfg["metric_name"],
                            "status": "skipped",
                            "reason": "model_extract_failed: %s" % exc,
                        })

                station_dir = output_dir / ("%s_%s" % (safe_name(cluster_uid), safe_name(row.get("station_name", ""))))
                station_dir.mkdir(parents=True, exist_ok=True)
                pd.DataFrame([{
                    "cluster_uid": cluster_uid,
                    "source_station_id": clean_text(row.get("source_station_id", "")),
                    "station_name": clean_text(row.get("station_name", "")),
                    "river_name": clean_text(row.get("river_name", "")),
                    "station_lat": float(row["lat"]),
                    "station_lon": float(row["lon"]),
                    "model_grid_lat": nearest["model_grid_lat"],
                    "model_grid_lon": nearest["model_grid_lon"],
                    "model_grid_distance_km": nearest["model_grid_distance_km"],
                    "model_grid_index": nearest["model_grid_index"],
                    "validation_start": valid_start,
                    "validation_end": valid_end,
                }]).to_csv(station_dir / "station_match.csv", index=False)

                reference_df.to_csv(station_dir / "reference_timeseries.csv", index=False)
                if model_frames:
                    model_df = model_frames[0]
                    for frame in model_frames[1:]:
                        model_df = pd.merge(model_df, frame, on="time", how="outer")
                    model_df = model_df.sort_values("time")
                else:
                    model_df = pd.DataFrame({"time": []})
                model_df.to_csv(station_dir / "model_timeseries.csv", index=False)

                for var_name in active_vars:
                    vcfg = VARIABLES[var_name]
                    ref_col = vcfg["ref_col"]
                    model_col = vcfg["model_col"]
                    reference_points = int(reference_df[ref_col].notna().sum()) if ref_col in reference_df else 0
                    if reference_points < int(cfg["min_reference_points"]):
                        variable_status_rows.append({
                            "cluster_uid": cluster_uid,
                            "station_name": clean_text(row.get("station_name", "")),
                            "variable": vcfg["metric_name"],
                            "status": "skipped",
                            "reason": "insufficient_reference_points",
                            "reference_points": reference_points,
                            "paired_points": 0,
                        })
                        continue
                    if model_col not in model_df.columns:
                        continue

                    compare_df = compare_pair(reference_df, model_df, ref_col, model_col, cfg["resolution"])
                    paired_points = int(len(compare_df))
                    compare_file = station_dir / ("compare_%s_%s.csv" % (var_name, cfg["resolution"]))
                    compare_df.rename(columns={"reference": ref_col, "model": model_col}).to_csv(compare_file, index=False)
                    if paired_points < int(cfg["min_paired_points"]):
                        variable_status_rows.append({
                            "cluster_uid": cluster_uid,
                            "station_name": clean_text(row.get("station_name", "")),
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
                    metrics_rows.append({
                        "cluster_uid": cluster_uid,
                        "source_station_id": clean_text(row.get("source_station_id", "")),
                        "station_name": clean_text(row.get("station_name", "")),
                        "river_name": clean_text(row.get("river_name", "")),
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
                    variable_status_rows.append({
                        "cluster_uid": cluster_uid,
                        "station_name": clean_text(row.get("station_name", "")),
                        "variable": vcfg["metric_name"],
                        "status": "validated",
                        "reason": "",
                        "reference_points": reference_points,
                        "paired_points": paired_points,
                    })
                    if cfg["make_plots"]:
                        maybe_plot_compare(
                            compare_df,
                            "%s %s" % (clean_text(row.get("station_name", "")), vcfg["metric_name"]),
                            "%s (%s)" % (var_name, vcfg["unit"]),
                            station_dir / ("compare_%s_%s.png" % (var_name, cfg["resolution"])),
                        )
        finally:
            matrix_ds.close()

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
    finally:
        ds_model.close()


if __name__ == "__main__":
    main()
