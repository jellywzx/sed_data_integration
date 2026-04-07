#!/usr/bin/env python3
"""
s6（流域版）：将 s5_basin_clustered_stations.csv 中各 cluster 的时间序列合并为单个 NetCDF。

基于 s8_merge_qc_csv_to_one_nc.py 改写，直接从 s5 basin 输出读取站点列表，
无需 s6 报告或 s7 重叠解析。

合并规则：
  - 同一 cluster_id 的多个站点视为同一虚拟站点；
  - 同一 (cluster_id, resolution) 下若有多个文件，按文件顺序取第一个有值的数据；

输入：
  - scripts_basin_test/output/s5_basin_clustered_stations.csv（s5 输出）

输出：
  - scripts_basin_test/output/s6_basin_merged_all.nc

NetCDF 结构：
  n_stations   维度：唯一 cluster 数（按 cluster_id 排序后的 0-based 索引）
  n_records    维度：所有 (cluster, resolution) 时间序列的总记录数
  lat, lon     代表站点经纬度（station_id == cluster_id 的行）
  cluster_id   n_stations 维度查找表（第 i 位 = 第 i 个虚拟站点的原始 cluster_id）
  station_index  0-based 索引（指向 n_stations 维度）
  time         days since 1970-01-01
  resolution   0=daily, 1=monthly, 2=annually_climatology, 3=other
  Q, SSC, SSL  径流量、悬沙浓度、悬沙通量

用法：
  python s6_basin_merge_to_nc.py
  python s6_basin_merge_to_nc.py --input /path/to/s5.csv --output /path/to/out.nc --workers 16
"""

import os
import argparse
import atexit
import sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
from tqdm import tqdm

from pipeline_paths import get_output_r_root, S5_BASIN_CLUSTERED_CSV, S6_MERGED_NC, S2_ORGANIZED_DIR

try:
    import netCDF4 as nc4
    from netCDF4 import num2date
    HAS_NC = True
except ImportError:
    HAS_NC = False

SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_INPUT  = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
_DEFAULT_OUTPUT = PROJECT_ROOT / S6_MERGED_NC

# output_resolution_organized/ 根目录，用于将 s3 CSV 中的相对路径还原为绝对路径
_ORGANIZED_ROOT = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()

FILL = -9999.0
TIME_NAMES    = ["time", "Time", "t", "sample"]
Q_NAMES       = ["Q", "discharge", "Discharge_m3_s", "Discharge"]
SSC_NAMES     = ["SSC", "ssc", "TSS_mg_L", "TSS"]
SSL_NAMES     = ["SSL", "sediment_load", "Sediment_load"]
Q_FLAG_NAMES  = ["Q_flag", "discharge_flag", "q_flag"]
SSC_FLAG_NAMES= ["SSC_flag", "ssc_flag", "TSS_flag", "tss_flag"]
SSL_FLAG_NAMES= ["SSL_flag", "ssl_flag", "sediment_load_flag"]
FLAG_GOOD     = 0    # flag==0 表示好数据
FLAG_FILL_BYTE= -127 # NC 中 byte flag 的 _FillValue

RESOLUTION_CODES = {
    "daily":                0,
    "monthly":              1,
    "annually_climatology": 2,
    "other":                3,
}

# 默认并行进程数，None = 自动取 CPU 核数，命令行 --workers 可覆盖
_DEFAULT_WORKERS = 24

_PROC = psutil.Process(os.getpid())


# ── 内存工具 ───────────────────────────────────────────────────────────────
def _mem_mb() -> float:
    """返回当前进程 RSS 内存（MB）。"""
    return _PROC.memory_info().rss / 1024 / 1024


def _mem_str() -> str:
    return "{:.0f} MB".format(_mem_mb())


def _mem_available_gb() -> float:
    return psutil.virtual_memory().available / 1024 / 1024 / 1024


def _check_memory(stage: str, warn_gb: float = 1.0):
    """在关键步骤打印内存状态，可用内存低于 warn_gb 时发出警告。"""
    avail = _mem_available_gb()
    msg = "[{}] 进程内存: {}  系统可用: {:.1f} GB".format(stage, _mem_str(), avail)
    print(msg)
    if avail < warn_gb:
        print("  !! 警告：系统可用内存不足 {:.1f} GB，可能引发 OOM".format(warn_gb))


# ── 日志 tee（仅 stdout，避免污染 tqdm 的 stderr 输出）──────────────────────
_LOG_TEE_ENABLED = False


def _enable_script_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    log_path = Path(__file__).resolve().with_name("{}_log.txt".format(Path(__file__).stem))
    if log_path.exists():
        try:
            log_path.unlink()
        except Exception:
            pass
    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("===== Run started {} =====\n".format(datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()

    class _Tee:
        def __init__(self, stream, log_file):
            self._s = stream
            self._f = log_file
        def write(self, data):
            self._s.write(data)
            self._f.write(data)
            self._f.flush()
        def flush(self):
            self._s.flush()
            self._f.flush()

    sys.stdout = _Tee(sys.stdout, log_fp)
    atexit.register(log_fp.close)
    _LOG_TEE_ENABLED = True


# ── 站点元数据全局属性键（按优先级排列）──────────────────────────────────────
_STATION_NAME_KEYS = ["station_name", "Station_Name", "stationName", "name"]
_RIVER_NAME_KEYS   = ["river_name",   "River_Name",   "riverName",   "river"]
_STATION_ID_KEYS   = ["station_id",   "Station_ID",   "stationID",   "ID"]


def _read_station_meta_from_nc(path):
    """从单个 NC 文件的全局属性读取站点名/河流名/原始站点ID。
    找不到对应属性时返回空字符串；任何异常都静默处理。
    """
    try:
        with nc4.Dataset(path, "r") as ds:
            def _get_attr(keys):
                for k in keys:
                    v = getattr(ds, k, None)
                    if v is not None and str(v).strip():
                        return str(v).strip()[:256]
                return ""
            return (
                _get_attr(_STATION_NAME_KEYS),
                _get_attr(_RIVER_NAME_KEYS),
                _get_attr(_STATION_ID_KEYS),
            )
    except Exception:
        return ("", "", "")


# ── 数据集级元数据属性键（大小写不敏感，按优先级排列）──────────────────────────
_SOURCE_NAME_KEYS = ["data_source_name", "Data_Source_Name"]
_INST_KEYS        = ["creator_institution", "contributor_institution", "institution"]
_URL_KEYS         = ["source_data_link", "sediment_data_source", "source_url"]


def _read_source_meta_from_nc(path):
    """从 NC 文件全局属性读取数据集级元数据：长名、机构、引用文献、数据链接。
    所有 reference* 属性均被合并（" | " 分隔）；找不到则返回空字符串。
    """
    try:
        with nc4.Dataset(path, "r") as ds:
            attr_lower = {k.lower(): k for k in ds.ncattrs()}  # 小写→原始名 映射

            def _get(keys):
                for k in keys:
                    orig = attr_lower.get(k.lower())
                    if orig:
                        v = str(getattr(ds, orig, "")).strip()
                        if v:
                            return v[:512]
                return ""

            # 收集所有以 "reference" 开头的属性（reference / references / reference1 / Reference 等）
            ref_parts = []
            for orig_key in ds.ncattrs():
                if orig_key.lower().startswith("reference"):
                    v = str(getattr(ds, orig_key, "")).strip()
                    if v and v not in ref_parts:
                        ref_parts.append(v)

            return (
                _get(_SOURCE_NAME_KEYS),        # 数据集长名
                _get(_INST_KEYS),               # 机构
                " | ".join(ref_parts)[:1024],   # 引用文献（合并）
                _get(_URL_KEYS),                # 数据链接/DOI
            )
    except Exception:
        return ("", "", "", "")


# ── NC 读取 ────────────────────────────────────────────────────────────────
def _get_var(ds, names, default=np.nan):
    for n in names:
        if n in ds.variables:
            return np.asarray(ds.variables[n][:]).flatten()
    return np.full(1, default)


def _read_flag_var(nc, names, size):
    """读取 flag 变量，返回 int8 数组；找不到则返回全 9（missing）。"""
    for n in names:
        if n in nc.variables:
            raw = np.asarray(nc.variables[n][:]).flatten()
            raw = raw[:size] if len(raw) >= size else np.concatenate(
                [raw, np.full(size - len(raw), FLAG_FILL_BYTE, dtype=np.int8)])
            # 将 fill_value (-127) 替换为 9（逻辑 missing）
            result = raw.astype(np.int16)
            result[result == FLAG_FILL_BYTE] = 9
            return result.astype(np.int8)
    return np.full(size, 9, dtype=np.int8)


def compute_quality_score(df):
    """计算 DataFrame 的质量分数：flag==0（好数据）占所有有效 flag 的比例。
    有效 flag 定义为值在 {0,1,2,3} 内（不含 9/missing）。
    """
    total = 0
    good  = 0
    for col in ["Q_flag", "SSC_flag", "SSL_flag"]:
        if col not in df.columns:
            continue
        flags = df[col].values.astype(np.int16)
        valid_mask = (flags >= 0) & (flags <= 3)  # 排除 9=missing
        total += int(valid_mask.sum())
        good  += int(((flags == FLAG_GOOD) & valid_mask).sum())
    if total == 0:
        return 0.0
    return good / total


def load_nc_series(path):
    """从 NC 读取时间序列，返回 DataFrame：date, Q, SSC, SSL, Q_flag, SSC_flag, SSL_flag。"""
    if not HAS_NC:
        return None
    try:
        with nc4.Dataset(path, "r") as nc:
            time_var = next((x for x in TIME_NAMES if x in nc.variables), None)
            if time_var is None:
                return None
            t = nc.variables[time_var]
            t_vals   = np.asarray(t[:]).flatten()
            units    = getattr(t, "units",    "days since 1970-01-01")
            calendar = getattr(t, "calendar", "gregorian")
            try:
                times = num2date(t_vals, units, calendar=calendar)
            except TypeError:
                try:
                    times = num2date(t_vals, units, calendar=calendar,
                                     only_use_cftime_datetimes=False)
                except Exception:
                    times = pd.to_datetime(t_vals, unit="D", origin="1970-01-01")
            except Exception:
                times = pd.to_datetime(t_vals, unit="D", origin="1970-01-01")

            try:
                times = pd.to_datetime(times)
                if hasattr(times, "date"):
                    dates = [pd.Timestamp(tt).date() for tt in times]
                else:
                    dates = [pd.Timestamp(tt).date() for tt in times.tolist()]
            except (TypeError, ValueError):
                dates = []
                for tt in times:
                    if hasattr(tt, "isoformat"):
                        dates.append(pd.Timestamp(tt.isoformat()).date())
                    elif hasattr(tt, "year"):
                        dates.append(pd.Timestamp(tt.year, tt.month, tt.day).date())
                    else:
                        dates.append(pd.Timestamp(str(tt)).date())

            q   = _get_var(nc, Q_NAMES)
            ssc = _get_var(nc, SSC_NAMES)
            ssl = _get_var(nc, SSL_NAMES)
            n   = len(dates)
            if n == 0:
                return None

            def pad(a, size):
                a = np.asarray(a).flatten()
                if len(a) >= size:
                    return a[:size]
                return np.concatenate([a, np.full(size - len(a), np.nan)])

            q   = pad(q,   n)
            ssc = pad(ssc, n)
            ssl = pad(ssl, n)

            q_flag   = _read_flag_var(nc, Q_FLAG_NAMES,   n)
            ssc_flag = _read_flag_var(nc, SSC_FLAG_NAMES, n)
            ssl_flag = _read_flag_var(nc, SSL_FLAG_NAMES, n)

            df = pd.DataFrame({
                "date":     dates,
                "Q":        q,   "SSC":      ssc,   "SSL":      ssl,
                "Q_flag":   q_flag, "SSC_flag": ssc_flag, "SSL_flag": ssl_flag,
            })
            df["date"] = pd.to_datetime(df["date"]).dt.date
            for col in ["Q", "SSC", "SSL"]:
                df.loc[df[col] == FILL,    col] = np.nan
                df.loc[df[col] == -9999.0, col] = np.nan
            return df
    except Exception:
        return None


# ── 时间序列构建（worker 函数，运行在子进程）─────────────────────────────────
def build_cluster_series(cid, resolution, recs):
    """
    为单个 (cluster_id, resolution) 构建合并时间序列。
    recs: list of (source, path)

    合并规则（相同分辨率内）：
      - 按文件整体质量分数（flag==0 好数据占比）从高到低排序；
      - 对每个时间点，优先使用质量最高的文件数据；
      - 若最高质量文件在该时间点无数据，则依次尝试次优文件。

    返回 (dates_arr, q_arr, ssc_arr, ssl_arr,
           q_flag_arr, ssc_flag_arr, ssl_flag_arr, is_overlap_arr, quality_log)
    或 None。
    """
    scored = []   # list of (quality_score, source, df)
    all_dates = set()
    for source, path in recs:
        df = load_nc_series(path)
        if df is not None and len(df) > 0:
            score = compute_quality_score(df)
            df["_source"] = source
            scored.append((score, source, df))
            all_dates.update(df["date"].tolist())
    if not scored:
        return None

    # 按质量分数降序排列（相同分辨率时，好数据比例高的优先）
    scored.sort(key=lambda x: x[0], reverse=True)
    quality_log = None
    if len(scored) > 1:
        score_info = ", ".join(
            "{} {:.1%}".format(src, sc) for sc, src, _ in scored
        )
        quality_log = "  cluster {} [{}] quality order: [{}]".format(
            cid, resolution, score_info
        )

    # 将 date 列设为 index 以加速查找，按质量顺序排列
    indexed = [df.set_index("date") for _, _, df in scored]
    all_dates = sorted(all_dates)
    n = len(all_dates)

    dates_arr    = pd.to_datetime(all_dates)
    q_arr        = np.full(n, FILL, dtype=np.float32)
    ssc_arr      = np.full(n, FILL, dtype=np.float32)
    ssl_arr      = np.full(n, FILL, dtype=np.float32)
    q_flag_arr   = np.full(n, 9,    dtype=np.int8)
    ssc_flag_arr = np.full(n, 9,    dtype=np.int8)
    ssl_flag_arr = np.full(n, 9,    dtype=np.int8)
    is_overlap_arr = np.zeros(n,    dtype=np.int8)
    source_arr   = np.empty(n,      dtype=object)
    source_arr[:] = ""

    for i, d in enumerate(all_dates):
        # 统计在该日期有数据的来源数，>1 则标记重叠
        sources_with_date = sum(1 for s in indexed if d in s.index)
        if sources_with_date > 1:
            is_overlap_arr[i] = 1

        for s in indexed:   # 按质量从高到低尝试
            if d not in s.index:
                continue
            row = s.loc[d]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
            q   = row.get("Q",   np.nan)
            ssc = row.get("SSC", np.nan)
            ssl = row.get("SSL", np.nan)
            if pd.isna(q) and pd.isna(ssc) and pd.isna(ssl):
                continue
            q_arr[i]        = q   if pd.notna(q)   else FILL
            ssc_arr[i]      = ssc if pd.notna(ssc) else FILL
            ssl_arr[i]      = ssl if pd.notna(ssl) else FILL
            # 同时读取所选来源的质量标记及来源名
            q_flag_arr[i]   = int(row.get("Q_flag",   9))
            ssc_flag_arr[i] = int(row.get("SSC_flag", 9))
            ssl_flag_arr[i] = int(row.get("SSL_flag", 9))
            source_arr[i]   = str(row.get("_source", ""))
            break

    return (dates_arr, q_arr, ssc_arr, ssl_arr,
            q_flag_arr, ssc_flag_arr, ssl_flag_arr, is_overlap_arr, source_arr, quality_log)


def _worker_build_cluster(args):
    cid, resolution, recs = args
    result = build_cluster_series(cid, resolution, recs)
    if result is None:
        return (cid, resolution, None, None)
    dates_arr, q_arr, ssc_arr, ssl_arr, q_flag, ssc_flag, ssl_flag, is_overlap, source_arr, quality_log = result
    return (cid, resolution,
            (dates_arr, q_arr, ssc_arr, ssl_arr, q_flag, ssc_flag, ssl_flag, is_overlap, source_arr),
            quality_log)


# ── main ───────────────────────────────────────────────────────────────────
def main():
    _enable_script_logging()

    ap = argparse.ArgumentParser(
        description="s6：将 s5_basin_clustered_stations.csv 合并为单 NC"
    )
    ap.add_argument("--input",  "-i", default=str(_DEFAULT_INPUT),
                    help="s5_basin_clustered_stations.csv 路径。默认: {}".format(_DEFAULT_INPUT))
    ap.add_argument("--output", "-o", default=str(_DEFAULT_OUTPUT),
                    help="输出 NC 路径。默认: {}".format(_DEFAULT_OUTPUT))
    ap.add_argument("--workers", "-w", type=int, default=_DEFAULT_WORKERS,
                    help="并行进程数（0 = 自动取 CPU 核数）。默认: {}".format(_DEFAULT_WORKERS))
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1

    inp_path = Path(args.input)
    out_path = Path(args.output)

    if not inp_path.is_file():
        print("Error: not found: {}".format(inp_path))
        return 1

    t0 = datetime.now()
    _check_memory("启动")

    # ── 1. 读取 s5 站点表 ─────────────────────────────────────────────────
    stations = pd.read_csv(inp_path)
    for col in ["path", "source", "lat", "lon", "cluster_id", "station_id", "resolution"]:
        if col not in stations.columns:
            print("Error: s5 CSV 缺少列 '{}'".format(col))
            return 1

    print("Loaded s5 stations: {} rows".format(len(stations)))

    # ── 相对路径 → 绝对路径 ───────────────────────────────────────────────────
    # s3 存储相对于 output_resolution_organized/ 的相对路径（如 daily/xxx.nc）
    # 若 CSV 中仍为旧机器绝对路径（含 /），则保持不变
    def _resolve_station_path(p):
        path = Path(p)
        if not path.is_absolute():
            return str(_ORGANIZED_ROOT / path)
        if path.is_file():
            return str(path)
        # 绝对路径不存在（跨机器迁移）：从路径中提取 output_resolution_organized/ 之后的相对部分
        try:
            parts = path.resolve().parts
            marker = "output_resolution_organized"
            for i, part in enumerate(parts):
                if part == marker and i + 1 < len(parts):
                    rel = Path(*parts[i + 1:])
                    candidate = _ORGANIZED_ROOT / rel
                    if candidate.is_file():
                        return str(candidate)
        except Exception:
            pass
        return str(path)  # fallback（文件不存在时返回原路径，由调用方处理）

    # 始终尝试路径解析（处理相对路径 + 跨机器迁移的旧绝对路径）
    stations["path"] = stations["path"].apply(_resolve_station_path)
    n_exist = int(stations["path"].apply(lambda p: Path(p).is_file()).sum())
    print("Path resolution: {}/{} files found under {}".format(
        n_exist, len(stations), _ORGANIZED_ROOT))
    _check_memory("读取CSV后")

    # ── 2. 构建 cluster 元数据（代表站点的经纬度）────────────────────────
    rep = (
        stations[stations["station_id"] == stations["cluster_id"]]
        .drop_duplicates(subset=["cluster_id"])
        .set_index("cluster_id")
    )
    all_cluster_ids = sorted(stations["cluster_id"].unique().tolist())
    n_stations      = len(all_cluster_ids)
    cluster_to_idx  = {cid: i for i, cid in enumerate(all_cluster_ids)}
    cluster_uids    = ["SED{:06d}".format(int(cid)) for cid in all_cluster_ids]
    print("Unique clusters: {}".format(n_stations))

    # 每个 cluster 所涉及的数据源列表（管道分隔，写入 n_stations 变量）
    cluster_sources_used = [""] * n_stations
    for cid, idx in cluster_to_idx.items():
        srcs = sorted(stations[stations["cluster_id"] == cid]["source"].unique())
        cluster_sources_used[idx] = "|".join(srcs)

    lats = np.full(n_stations, np.nan, dtype=np.float64)
    lons = np.full(n_stations, np.nan, dtype=np.float64)
    for cid, idx in cluster_to_idx.items():
        if cid in rep.index:
            lats[idx] = float(rep.loc[cid, "lat"])
            lons[idx] = float(rep.loc[cid, "lon"])
        else:
            grp = stations[stations["cluster_id"] == cid]
            lats[idx] = float(grp["lat"].mean())
            lons[idx] = float(grp["lon"].mean())

    # ── 2b. 提取流域元数据（每个 cluster 代表站点的 basin 属性）────────────
    MATCH_QUALITY_CODES = {"distance_only": 0, "area_matched": 1, "failed": 2}
    basin_areas       = np.full(n_stations, FILL,  dtype=np.float32)
    pfaf_codes        = np.full(n_stations, FILL,  dtype=np.float32)
    n_reaches_arr     = np.full(n_stations, -9999, dtype=np.int32)
    match_quality_arr = np.full(n_stations, -1,    dtype=np.int8)

    for cid, idx in cluster_to_idx.items():
        if cid in rep.index:
            row = rep.loc[cid]
            ba = row.get("basin_area",        None)
            pf = row.get("pfaf_code",         None)
            nr = row.get("n_upstream_reaches",None)
            mq = str(row.get("match_quality", "unknown") or "unknown").strip()
            basin_areas[idx]       = float(ba) if ba is not None and not pd.isna(ba) else FILL
            pfaf_codes[idx]        = float(pf) if pf is not None and not pd.isna(pf) else FILL
            n_reaches_arr[idx]     = int(nr)   if nr is not None and not pd.isna(nr) else -9999
            match_quality_arr[idx] = np.int8(MATCH_QUALITY_CODES.get(mq, -1))

    # ── 2c. 串行读取代表站点 NC 的全局属性（station_name / river_name / source_station_id）
    # Note: HDF5 is not thread-safe; do NOT use ThreadPoolExecutor here.

    # 构建 cid → 代表站点绝对路径映射
    _rep_paths = {}
    for cid in all_cluster_ids:
        if cid in rep.index:
            _rep_paths[cid] = _resolve_station_path(str(rep.loc[cid, "path"]))

    station_names      = [""] * n_stations
    river_names        = [""] * n_stations
    source_station_ids = [""] * n_stations

    def _fetch_meta(item):
        cid, path = item
        return cid, _read_station_meta_from_nc(path)

    # HDF5 is not thread-safe by default; serialize NC metadata reads to avoid
    # null type-ID in nc4_HDF5_close_att when multiple threads close files simultaneously.
    for cid, path in _rep_paths.items():
        sname, rname, sid = _read_station_meta_from_nc(path)
        _idx = cluster_to_idx[cid]
        station_names[_idx]      = sname
        river_names[_idx]        = rname
        source_station_ids[_idx] = sid

    print("Station metadata: {} station_name, {} river_name, {} source_station_id filled".format(
        sum(1 for s in station_names if s),
        sum(1 for s in river_names if s),
        sum(1 for s in source_station_ids if s),
    ))

    # ── 2d. 收集数据集级元数据（机构、引用、URL），写入 n_sources 查找表 ──────────
    unique_sources  = sorted(stations["source"].unique().tolist())
    n_src           = len(unique_sources)
    source_to_idx   = {s: i for i, s in enumerate(unique_sources)}
    print("Unique source datasets: {}".format(n_src))

    # 每个数据源取第一个文件作为代表
    src_rep_paths = {}
    for src in unique_sources:
        sub = stations[stations["source"] == src]
        src_rep_paths[src] = _resolve_station_path(str(sub.iloc[0]["path"]))

    src_long_names   = [""] * n_src
    src_institutions = [""] * n_src
    src_references   = [""] * n_src
    src_urls         = [""] * n_src

    def _fetch_source_meta(item):
        src, path = item
        return src, _read_source_meta_from_nc(path)

    for src, path in src_rep_paths.items():
        lname, inst, ref, url = _read_source_meta_from_nc(path)
        _sidx = source_to_idx[src]
        src_long_names[_sidx]   = lname if lname else src  # 无长名则用短名回退
        src_institutions[_sidx] = inst
        src_references[_sidx]   = ref
        src_urls[_sidx]         = url

    print("Source metadata: {}/{} institution, {}/{} reference, {}/{} url filled".format(
        sum(1 for s in src_institutions if s), n_src,
        sum(1 for s in src_references  if s), n_src,
        sum(1 for s in src_urls        if s), n_src,
    ))

    # ── 3. 按 (cluster_id, resolution) 分组 ── 用 groupby，避免 iterrows ──
    by_cluster_res = defaultdict(list)
    for (cid, res), grp in stations.groupby(["cluster_id", "resolution"], sort=False):
        by_cluster_res[(int(cid), str(res))] = list(zip(grp["source"], grp["path"]))

    tasks     = [(cid, res, recs) for (cid, res), recs in by_cluster_res.items()]
    n_tasks   = len(tasks)
    n_workers = args.workers if args.workers > 0 else (os.cpu_count() or 4)
    n_workers = max(1, min(n_workers, n_tasks))
    use_parallel = n_workers > 1 and n_tasks >= 2

    print("Tasks: {}  Workers: {}  Mode: {}".format(
        n_tasks, n_workers, "parallel" if use_parallel else "serial"))
    _check_memory("准备任务后")

    # ── 4+5. 并行读取 NC + 实时展平为 numpy 数组（边完成边处理）─────────────
    ref = pd.Timestamp("1970-01-01")

    # 用 list of arrays 累积，最后 np.concatenate —— 避免 Python list .extend
    parts_idx      = []  # list of int32 arrays
    parts_time     = []  # list of float64 arrays
    parts_res      = []  # list of int8 arrays
    parts_q        = []  # list of float32 arrays
    parts_ssc      = []
    parts_ssl      = []
    parts_qflag    = []  # list of int8 arrays
    parts_sscflag  = []
    parts_sslflag  = []
    parts_overlap  = []  # list of int8 arrays
    parts_source   = []  # list of object arrays (str)
    n_empty    = 0
    n_done     = 0

    def _flush_result(pbar, cid, resolution, res, quality_log):
        """将单个任务结果展平并追加到 parts_* 列表，同时更新进度条。"""
        nonlocal n_empty, n_done
        n_done += 1
        # 质量排序日志通过 tqdm.write 输出，不破坏进度条
        if quality_log:
            tqdm.write(quality_log)
        if res is None:
            n_empty += 1
        else:
            dates_arr, q_arr, ssc_arr, ssl_arr, q_flag, ssc_flag, ssl_flag, is_overlap, source_arr = res
            n    = len(dates_arr)
            days = ((dates_arr - ref).total_seconds().values / 86400.0).astype(np.float64)
            rc   = RESOLUTION_CODES.get(str(resolution).lower(), 3)
            idx  = cluster_to_idx[cid]
            parts_idx.append(    np.full(n, idx, dtype=np.int32))
            parts_time.append(   days)
            parts_res.append(    np.full(n, rc,  dtype=np.int8))
            parts_q.append(      q_arr)
            parts_ssc.append(    ssc_arr)
            parts_ssl.append(    ssl_arr)
            parts_qflag.append(  q_flag)
            parts_sscflag.append(ssc_flag)
            parts_sslflag.append(ssl_flag)
            parts_overlap.append(is_overlap)
            parts_source.append( source_arr)

        total_rec = sum(len(x) for x in parts_time)
        elapsed   = (datetime.now() - t0).total_seconds()
        pbar.set_postfix(
            records  = "{:,}".format(total_rec),
            empty    = n_empty,
            mem      = _mem_str(),
            avail_gb = "{:.1f}GB".format(_mem_available_gb()),
            elapsed  = "{:.0f}s".format(elapsed),
            refresh  = False,
        )
        pbar.update(1)

    _pbar_fmt = dict(total=n_tasks, desc="Merging NC", unit="task",
                     ncols=120, file=sys.stderr, dynamic_ncols=False)

    if use_parallel:
        with ProcessPoolExecutor(max_workers=n_workers) as ex:
            futures = {ex.submit(_worker_build_cluster, t): t for t in tasks}
            with tqdm(**_pbar_fmt) as pbar:
                for future in as_completed(futures):
                    cid, resolution, res, quality_log = future.result()
                    _flush_result(pbar, cid, resolution, res, quality_log)
    else:
        with tqdm(**_pbar_fmt) as pbar:
            for (cid, res, recs) in tasks:
                result = build_cluster_series(cid, res, recs)
                if result is None:
                    _flush_result(pbar, cid, res, None, None)
                else:
                    (dates_arr, q_arr, ssc_arr, ssl_arr,
                     q_flag, ssc_flag, ssl_flag, is_overlap, source_arr, quality_log) = result
                    _flush_result(pbar, cid, res,
                                  (dates_arr, q_arr, ssc_arr, ssl_arr,
                                   q_flag, ssc_flag, ssl_flag, is_overlap, source_arr),
                                  quality_log)

    n_series = n_done - n_empty
    print("Series: {}/{} non-empty  ({} empty/missing)".format(n_series, n_tasks, n_empty))
    _check_memory("时间序列读取完成后")

    # ── 5b. 合并 parts → 最终数组 ────────────────────────────────────────
    if not parts_time:
        print("Error: no records collected. Check NC file paths.")
        return 1

    print("Concatenating arrays ...")
    station_index_arr = np.concatenate(parts_idx)
    time_arr          = np.concatenate(parts_time)
    resolution_arr    = np.concatenate(parts_res)
    q_arr             = np.concatenate(parts_q)
    ssc_arr           = np.concatenate(parts_ssc)
    ssl_arr           = np.concatenate(parts_ssl)
    q_flag_arr        = np.concatenate(parts_qflag)
    ssc_flag_arr      = np.concatenate(parts_sscflag)
    ssl_flag_arr      = np.concatenate(parts_sslflag)
    is_overlap_arr    = np.concatenate(parts_overlap)
    record_source_arr = np.concatenate(parts_source)

    # 释放 parts 列表
    del (parts_idx, parts_time, parts_res, parts_q, parts_ssc, parts_ssl,
         parts_qflag, parts_sscflag, parts_sslflag, parts_overlap, parts_source)

    n_records = len(time_arr)
    _check_memory("数组合并后")

    # ── 6. 写 NC ──────────────────────────────────────────────────────────
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print("Writing {} (n_stations={}, n_records={:,}) ...".format(
        out_path, n_stations, n_records))

    with nc4.Dataset(out_path, "w", format="NETCDF4") as nc:
        nc.createDimension("n_stations", n_stations)
        nc.createDimension("n_records",  n_records)
        nc.createDimension("n_sources",  n_src)

        lat_v = nc.createVariable("lat", "f8", ("n_stations",))
        lat_v.long_name = "latitude of virtual station (cluster representative)"
        lat_v.units     = "degrees_north"
        lat_v[:]        = lats

        lon_v = nc.createVariable("lon", "f8", ("n_stations",))
        lon_v.long_name = "longitude of virtual station (cluster representative)"
        lon_v.units     = "degrees_east"
        lon_v[:]        = lons

        cid_v = nc.createVariable("cluster_id", "i4", ("n_stations",))
        cid_v.long_name = "original cluster_id from s5_basin_clustered_stations"
        cid_v[:]        = np.array(all_cluster_ids, dtype=np.int32)

        ba_v = nc.createVariable("basin_area", "f4", ("n_stations",), fill_value=FILL)
        ba_v.long_name = "basin drainage area"
        ba_v.units     = "km2"
        ba_v.comment   = "from MERIT-Hydro via basin tracer (uparea_merit column)"
        ba_v[:]        = basin_areas

        pf_v = nc.createVariable("pfaf_code", "f4", ("n_stations",), fill_value=FILL)
        pf_v.long_name = "Pfafstetter basin code"
        pf_v.comment   = "hierarchical basin identifier; integer part is the level-based code"
        pf_v[:]        = pfaf_codes

        nr_v = nc.createVariable("n_upstream_reaches", "i4", ("n_stations",), fill_value=-9999)
        nr_v.long_name = "number of upstream river reaches"
        nr_v[:]        = n_reaches_arr

        bm_v = nc.createVariable("basin_match_quality", "i1", ("n_stations",),
                                  fill_value=np.int8(-1))
        bm_v.long_name    = "basin matching quality from basin tracer"
        bm_v.flag_values  = np.array([0, 1, 2, -1], dtype=np.int8)
        bm_v.flag_meanings = "distance_only area_matched failed unknown"
        bm_v[:]           = match_quality_arr

        sn_v = nc.createVariable("station_name", str, ("n_stations",))
        sn_v.long_name = "station name from source NC global attribute"
        sn_v.comment   = "populated where available; empty string if not present in source"
        sn_v[:]        = np.array(station_names, dtype=object)

        rn_v = nc.createVariable("river_name", str, ("n_stations",))
        rn_v.long_name = "river name from source NC global attribute"
        rn_v.comment   = "primarily from Dethier and similar datasets; empty string if unavailable"
        rn_v[:]        = np.array(river_names, dtype=object)

        si_v = nc.createVariable("source_station_id", str, ("n_stations",))
        si_v.long_name = "original station identifier in source dataset"
        si_v.comment   = "e.g. WQX station code (RiverSed), HYDAT station ID; empty string if unavailable"
        si_v[:]        = np.array(source_station_ids, dtype=object)

        uid_v = nc.createVariable("cluster_uid", str, ("n_stations",))
        uid_v.long_name = "unique cluster identifier for cross-file referencing"
        uid_v.comment   = "format: SED + 6-digit zero-padded cluster_id; join key with companion shapefile"
        uid_v[:]        = np.array(cluster_uids, dtype=object)

        su_v = nc.createVariable("sources_used", str, ("n_stations",))
        su_v.long_name = "pipe-separated list of source datasets contributing to this cluster"
        su_v.comment   = "lookup full metadata via n_sources dimension using source_name variable"
        su_v[:]        = np.array(cluster_sources_used, dtype=object)

        # ── n_sources 查找表（每个数据集一行，供引用/机构信息查询）──────────────
        sn_lk = nc.createVariable("source_name", str, ("n_sources",))
        sn_lk.long_name = "short dataset identifier (matches 'source' variable in n_records and 'sources_used' in n_stations)"
        sn_lk[:]        = np.array(unique_sources, dtype=object)

        sl_lk = nc.createVariable("source_long_name", str, ("n_sources",))
        sl_lk.long_name = "full dataset name (from data_source_name global attribute of source NC files)"
        sl_lk[:]        = np.array(src_long_names, dtype=object)

        si_lk = nc.createVariable("institution", str, ("n_sources",))
        si_lk.long_name = "data provider institution (from creator_institution / institution global attribute)"
        si_lk[:]        = np.array(src_institutions, dtype=object)

        sr_lk = nc.createVariable("reference", str, ("n_sources",))
        sr_lk.long_name = "citation(s) for this dataset (all reference* attributes merged with ' | ')"
        sr_lk[:]        = np.array(src_references, dtype=object)

        su_lk = nc.createVariable("source_url", str, ("n_sources",))
        su_lk.long_name = "data access URL or DOI (from source_data_link / sediment_data_source / source_url)"
        su_lk[:]        = np.array(src_urls, dtype=object)

        sid_v = nc.createVariable("station_index", "i4", ("n_records",))
        sid_v.long_name = "0-based index into n_stations dimension"
        sid_v[:]        = station_index_arr

        res_v = nc.createVariable("resolution", "i1", ("n_records",))
        res_v.long_name = "time resolution: 0=daily, 1=monthly, 2=annually_climatology, 3=other"
        res_v[:]        = resolution_arr

        t_v = nc.createVariable("time", "f8", ("n_records",))
        t_v.long_name = "time"
        t_v.units     = "days since 1970-01-01"
        t_v.calendar  = "gregorian"
        t_v[:]        = time_arr

        q_v = nc.createVariable("Q", "f4", ("n_records",), fill_value=FILL)
        q_v.long_name = "river discharge"
        q_v.units     = "m3 s-1"
        q_v[:]        = q_arr

        ssc_v = nc.createVariable("SSC", "f4", ("n_records",), fill_value=FILL)
        ssc_v.long_name = "suspended sediment concentration"
        ssc_v.units     = "mg L-1"
        ssc_v[:]        = ssc_arr

        ssl_v = nc.createVariable("SSL", "f4", ("n_records",), fill_value=FILL)
        ssl_v.long_name = "suspended sediment load"
        ssl_v.units     = "ton day-1"
        ssl_v.comment   = ("Daily load for daily data; representative-day load for "
                           "monthly/annually_climatology. See 'resolution' variable.")
        ssl_v[:]        = ssl_arr

        _flag_kw = dict(flag_values=np.array([0, 1, 2, 3, 9], dtype=np.int8),
                        flag_meanings="good estimated suspect bad missing")

        qf_v = nc.createVariable("Q_flag", "i1", ("n_records",), fill_value=np.int8(9))
        qf_v.long_name = "quality flag for river discharge"
        qf_v.standard_name = "status_flag"
        for k, v in _flag_kw.items():
            setattr(qf_v, k, v)
        qf_v[:]        = q_flag_arr

        sf_v = nc.createVariable("SSC_flag", "i1", ("n_records",), fill_value=np.int8(9))
        sf_v.long_name = "quality flag for suspended sediment concentration"
        sf_v.standard_name = "status_flag"
        for k, v in _flag_kw.items():
            setattr(sf_v, k, v)
        sf_v[:]        = ssc_flag_arr

        lf_v = nc.createVariable("SSL_flag", "i1", ("n_records",), fill_value=np.int8(9))
        lf_v.long_name = "quality flag for suspended sediment load"
        lf_v.standard_name = "status_flag"
        for k, v in _flag_kw.items():
            setattr(lf_v, k, v)
        lf_v[:]        = ssl_flag_arr

        io_v = nc.createVariable("is_overlap", "i1", ("n_records",))
        io_v.long_name     = "multi-source overlap flag"
        io_v.flag_values   = np.array([0, 1], dtype=np.int8)
        io_v.flag_meanings = "single_source quality_score_selection_applied"
        io_v.comment       = ("1 = multiple source files provided data for this "
                              "(cluster_id, resolution, date); best quality-score source selected")
        io_v[:]            = is_overlap_arr

        rec_src_v = nc.createVariable("source", str, ("n_records",))
        rec_src_v.long_name = "source dataset name for this record"
        rec_src_v.comment   = ("matches source_name in n_sources dimension; "
                               "look up institution/reference/source_url via that table")
        rec_src_v[:]        = record_source_arr

        nc.title         = "Global river suspended sediment dataset (basin-merged)"
        nc.Conventions   = "CF-1.8"
        nc.source        = ("s6_basin_merge_to_nc.py: merged from QC-passed NC files via "
                            "s5_basin_clustered_stations.csv; "
                            "multi-source dates resolved by quality-score ranking (flag==0 fraction)")
        nc.history       = "Created {} by s6_basin_merge_to_nc.py".format(
                               datetime.now().isoformat(timespec="seconds"))
        nc.overlap_policy = ("Multi-source dates: source with highest flag==0 fraction selected; "
                             "is_overlap=1 marks records with competing sources")
        nc.basin_csv      = str(inp_path)
        nc.n_source_stations = str(len(stations))
        nc.n_clusters     = str(n_stations)
        nc.created        = datetime.now().isoformat(timespec="seconds")

        # Flush all data to disk before close to avoid HDF5 segfault when
        # freeing VL-string type IDs in nc4_HDF5_close_att.
        nc.sync()

    elapsed = (datetime.now() - t0).total_seconds()
    _check_memory("写入NC后")
    print("Wrote {} in {:.1f}s".format(out_path, elapsed))
    return 0


if __name__ == "__main__":
    exit(main())
