#!/usr/bin/env python3
"""
s6（流域版）：将 s5_basin_clustered_stations.csv 中各 cluster 的时间序列合并为单个 NetCDF。

基于 s8_merge_qc_csv_to_one_nc.py 改写，直接从 s5 basin 输出读取站点列表，
无需 s6 报告或 s7 重叠解析。

合并规则：
  - 同一 cluster_id 的多个站点视为同一虚拟站点；
  - 同一 (cluster_id, resolution) 下若有多个文件，按质量分数优先选择；
  - cluster 层与 source-station 层同时保留，避免合并后丢失原始站点信息。

输入：
  - scripts_basin_test/output/s5_basin_clustered_stations.csv（s5 输出）

当前默认规则：
  - climatology 不进入 basin 主线；
  - 若输入 s5 中仍混入 climatology 行，本脚本默认会将其过滤掉；
  - climatology 应通过独立脚本单独导出为 climatology NC。
  - 默认会校验输入 Q/SSC/SSL 的 units；若缺失或不在白名单中会记 warning，
    `--strict-units` 下则直接报错停止写出。

输出：
  - scripts_basin_test/output/s6_basin_merged_all.nc
  - scripts_basin_test/output/s6_cluster_quality_order.csv

NetCDF 结构：
  n_stations   维度：唯一 cluster 数（按 cluster_id 排序后的 0-based 索引）
  n_source_stations 维度：原始站点映射表（一个 cluster 下可有多个 source station）
  n_records    维度：所有 (cluster, resolution) 时间序列的总记录数
  lat, lon     代表站点经纬度（station_id == cluster_id 的行）
  cluster_id   n_stations 维度查找表（第 i 位 = 第 i 个虚拟站点的原始 cluster_id）
  station_index  0-based 索引（指向 n_stations 维度）
  source_station_index n_records 维度查找表（指向 n_source_stations）
  time         days since 1970-01-01
  resolution   0=daily, 1=monthly, 2=annual, 3=climatology, 4=other
  Q, SSC, SSL  径流量、悬沙浓度、悬沙通量

用法：
  python s6_basin_merge_to_nc.py
  python s6_basin_merge_to_nc.py --input /path/to/s5.csv --output /path/to/out.nc --workers 16
"""

import os
import argparse
import atexit
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
from tqdm import tqdm

from basin_policy import (
    MATCH_QUALITY_CODES,
    MATCH_QUALITY_MEANINGS,
)
from pipeline_paths import (
    get_output_r_root,
    S5_BASIN_CLUSTERED_CSV,
    S6_MERGED_NC,
    S6_QUALITY_ORDER_CSV,
    S2_ORGANIZED_DIR,
)

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
_DEFAULT_QUALITY_ORDER = PROJECT_ROOT / S6_QUALITY_ORDER_CSV

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
STRICT_UNIT_CHECK = False
UNIT_SUMMARY_MAX_EXAMPLES = 12
UNIT_WHITELISTS = {
    "Q": frozenset(["m3 s-1", "m^3 s-1", "m3/s", "m^3/s"]),
    "SSC": frozenset(["mg l-1", "mg/l"]),
    "SSL": frozenset(["ton day-1", "ton/day-1", "ton/day", "ton d-1", "t day-1", "t/day"]),
}
UNIT_VAR_SPECS = (
    ("Q", Q_NAMES),
    ("SSC", SSC_NAMES),
    ("SSL", SSL_NAMES),
)
QUALITY_ORDER_COLUMNS = [
    "cluster_id",
    "cluster_uid",
    "cluster_index",
    "resolution",
    "quality_rank",
    "n_candidates",
    "is_top_ranked",
    "source",
    "source_station_index",
    "source_station_uid",
    "path",
    "quality_score",
    "good_flag_count",
    "valid_flag_count",
    "n_time_rows",
    "n_nonempty_rows",
]

RESOLUTION_CODES = {
    "daily": 0,
    "monthly": 1,
    "annual": 2,
    "climatology": 3,
    "other": 4,
    # 用户规则：季度并入 monthly，single_point 并入 daily
    "quarterly": 1,
    "single_point": 0,
    # 兼容旧输出目录名
    "annually_climatology": 3,
}

# 默认并行进程数，None = 自动取 CPU 核数，命令行 --workers 可覆盖
_DEFAULT_WORKERS = 24

_PROC = psutil.Process(os.getpid())


class UnitValidationError(ValueError):
    """Raised when a source variable unit fails validation in strict mode."""

    def __init__(self, issues):
        self.issues = list(issues or [])
        message = self.issues[0]["message"] if self.issues else "unit validation failed"
        super(UnitValidationError, self).__init__(message)


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
    orig_stdout = sys.stdout

    class _Tee:
        def __init__(self, stream, log_file):
            self._s = stream
            self._f = log_file
        def write(self, data):
            self._s.write(data)
            try:
                self._f.write(data)
                self._f.flush()
            except (ValueError, OSError):
                # Interpreter shutdown may close the log file before stdout flushes.
                pass
        def flush(self):
            self._s.flush()
            try:
                self._f.flush()
            except (ValueError, OSError):
                pass

    def _close_log_file():
        try:
            sys.stdout = orig_stdout
        except Exception:
            pass
        try:
            log_fp.close()
        except Exception:
            pass

    sys.stdout = _Tee(orig_stdout, log_fp)
    atexit.register(_close_log_file)
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


def _clean_text(value):
    if value is None:
        return ""
    if isinstance(value, float) and np.isnan(value):
        return ""
    return str(value).strip()


def _clean_coord(value):
    try:
        if value is None or pd.isna(value):
            return None
        return round(float(value), 6)
    except Exception:
        return None


def _clean_bool(value):
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    if not text:
        return False
    return text in {"1", "true", "yes", "y", "t"}


def _normalize_units_text(value):
    text = _clean_text(value).lower()
    if not text:
        return ""
    replacements = {
        "㎥": "m3",
        "m³": "m3",
        "−": "-",
        "–": "-",
        "_": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = text.replace("per", "/")
    text = " ".join(text.split())
    return text


def _build_unit_issue(path, canonical_name, var_name, units_raw, accepted_units, reason, strict):
    normalized_units = _normalize_units_text(units_raw)
    shown_units = normalized_units or _clean_text(units_raw)
    if not shown_units:
        shown_units = "(missing)"
    severity = "error" if strict else "warning"
    message = (
        "[units:{}] {} variable '{}' in {} uses units '{}' (accepted: {})".format(
            severity,
            canonical_name,
            var_name,
            path,
            shown_units,
            ", ".join(sorted(accepted_units)),
        )
    )
    if reason:
        message = "{} [{}]".format(message, reason)
    return {
        "path": str(path),
        "canonical_name": str(canonical_name),
        "var_name": str(var_name),
        "units": shown_units,
        "accepted_units": tuple(sorted(accepted_units)),
        "reason": str(reason or ""),
        "severity": severity,
        "message": message,
    }


def _validate_present_variable_units(nc, path, strict=False):
    issues = []
    for canonical_name, aliases in UNIT_VAR_SPECS:
        var_name = next((name for name in aliases if name in nc.variables), None)
        if var_name is None:
            continue
        units_raw = getattr(nc.variables[var_name], "units", "")
        normalized_units = _normalize_units_text(units_raw)
        accepted_units = UNIT_WHITELISTS[canonical_name]
        if not normalized_units:
            issues.append(
                _build_unit_issue(
                    path=path,
                    canonical_name=canonical_name,
                    var_name=var_name,
                    units_raw=units_raw,
                    accepted_units=accepted_units,
                    reason="missing_units_attr",
                    strict=strict,
                )
            )
            continue
        if normalized_units not in accepted_units:
            issues.append(
                _build_unit_issue(
                    path=path,
                    canonical_name=canonical_name,
                    var_name=var_name,
                    units_raw=units_raw,
                    accepted_units=accepted_units,
                    reason="unit_not_in_whitelist",
                    strict=strict,
                )
            )
    if strict and issues:
        raise UnitValidationError(issues)
    return issues


def _summarize_unit_issues(unit_issues, strict_mode=False, label="Unit validation"):
    issues = list(unit_issues or [])
    if not issues:
        print("{}: no unit issues found.".format(label))
        return

    severity_counter = Counter(issue.get("severity", "warning") for issue in issues)
    reason_counter = Counter(issue.get("reason", "") for issue in issues)
    affected_files = len(set(issue.get("path", "") for issue in issues if issue.get("path", "")))
    print(
        "{}: {} issues across {} files (warnings={}, errors={})".format(
            label,
            len(issues),
            affected_files,
            severity_counter.get("warning", 0),
            severity_counter.get("error", 0),
        )
    )
    if reason_counter:
        print("  issue types: {}".format(
            ", ".join(
                "{}={}".format(reason or "unknown", count)
                for reason, count in reason_counter.most_common()
            )
        ))
    seen = set()
    shown = 0
    for issue in issues:
        message = issue.get("message", "")
        if not message or message in seen:
            continue
        print("  {}".format(message))
        seen.add(message)
        shown += 1
        if shown >= UNIT_SUMMARY_MAX_EXAMPLES:
            break
    if len(seen) < len({issue.get("message", "") for issue in issues if issue.get("message", "")}):
        print("  ... additional unit issues omitted from console summary; see script log for full details.")
    if strict_mode and severity_counter.get("error", 0) > 0:
        print(
            "  strict unit validation is enabled; rerun after fixing source units or disable --strict-units for diagnostics only."
        )


def _build_source_station_key(row):
    """为原始站点构建稳定键。

    优先使用 source + source_station_id / station_name / river_name / 坐标；
    若这些都缺失，则回退到文件名，避免把同源同坐标但无法识别的不同文件错误合并。
    """
    source = _clean_text(row.get("source"))
    cluster_id = _clean_text(row.get("cluster_id"))
    native_id = _clean_text(row.get("source_station_id"))
    station_name = _clean_text(row.get("station_name"))
    river_name = _clean_text(row.get("river_name"))
    lat = _clean_coord(row.get("lat"))
    lon = _clean_coord(row.get("lon"))
    key = (cluster_id, source, native_id, station_name, river_name, lat, lon)
    if native_id or station_name or river_name:
        return key
    return key + (Path(str(row.get("path", ""))).name,)


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
    return compute_quality_metrics(df)["quality_score"]


def compute_quality_metrics(df):
    total = 0
    good = 0
    for col in ["Q_flag", "SSC_flag", "SSL_flag"]:
        if col not in df.columns:
            continue
        flags = df[col].values.astype(np.int16)
        valid_mask = (flags >= 0) & (flags <= 3)
        total += int(valid_mask.sum())
        good += int(((flags == FLAG_GOOD) & valid_mask).sum())

    data_cols = [col for col in ["Q", "SSC", "SSL"] if col in df.columns]
    if data_cols:
        nonempty_rows = int(df[data_cols].notna().any(axis=1).sum())
    else:
        nonempty_rows = 0

    return {
        "quality_score": (good / total) if total > 0 else 0.0,
        "good_flag_count": good,
        "valid_flag_count": total,
        "n_time_rows": int(len(df)),
        "n_nonempty_rows": nonempty_rows,
    }


def _write_quality_order_csv(rows, out_path, cluster_to_idx, source_station_uids):
    if rows:
        df = pd.DataFrame(rows)
        df["cluster_index"] = df["cluster_id"].map(
            lambda cid: int(cluster_to_idx.get(int(cid), -1))
        )
        df["cluster_uid"] = df["cluster_id"].map(
            lambda cid: "SED{:06d}".format(int(cid))
        )
        df["source_station_uid"] = df["source_station_index"].map(
            lambda idx: source_station_uids[int(idx)] if 0 <= int(idx) < len(source_station_uids) else ""
        )
        df = df.sort_values(["cluster_id", "resolution", "quality_rank", "source_station_index"])
        df = df.reindex(columns=QUALITY_ORDER_COLUMNS)
    else:
        df = pd.DataFrame(columns=QUALITY_ORDER_COLUMNS)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return len(df)


def load_nc_series(path):
    """从 NC 读取时间序列，返回 DataFrame：date, Q, SSC, SSL, Q_flag, SSC_flag, SSL_flag。"""
    if not HAS_NC:
        return None, []
    try:
        with nc4.Dataset(path, "r") as nc:
            unit_issues = _validate_present_variable_units(
                nc,
                path=path,
                strict=STRICT_UNIT_CHECK,
            )
            time_var = next((x for x in TIME_NAMES if x in nc.variables), None)
            if time_var is None:
                return None, unit_issues
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
                return None, unit_issues

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
            return df, unit_issues
    except UnitValidationError as exc:
        return None, list(exc.issues)
    except Exception:
        return None, []


# ── 时间序列构建（worker 函数，运行在子进程）─────────────────────────────────
def build_cluster_series(cid, resolution, recs):
    """
    为单个 (cluster_id, resolution) 构建合并时间序列。
    recs: list of (source, path, source_station_index)

    合并规则（相同分辨率内）：
      - 按文件整体质量分数（flag==0 好数据占比）从高到低排序；
      - 对每个时间点，优先使用质量最高的文件数据；
      - 若最高质量文件在该时间点无数据，则依次尝试次优文件。

    返回 (dates_arr, q_arr, ssc_arr, ssl_arr,
           q_flag_arr, ssc_flag_arr, ssl_flag_arr, is_overlap_arr,
           source_arr, source_station_idx_arr, quality_rows, quality_log)
    或 None。
    """
    scored = []   # list of metadata dict with df
    all_dates = set()
    unit_issues = []
    for source, path, source_station_index in recs:
        df, file_unit_issues = load_nc_series(path)
        if file_unit_issues:
            unit_issues.extend(file_unit_issues)
        if df is not None and len(df) > 0:
            metrics = compute_quality_metrics(df)
            df["_source"] = source
            df["_source_station_index"] = int(source_station_index)
            scored.append(
                {
                    "quality_score": metrics["quality_score"],
                    "source": str(source),
                    "source_station_index": int(source_station_index),
                    "path": str(path),
                    "good_flag_count": metrics["good_flag_count"],
                    "valid_flag_count": metrics["valid_flag_count"],
                    "n_time_rows": metrics["n_time_rows"],
                    "n_nonempty_rows": metrics["n_nonempty_rows"],
                    "df": df,
                }
            )
            all_dates.update(df["date"].tolist())
    if not scored:
        return (None, unit_issues)

    # 按质量分数降序排列（相同分辨率时，好数据比例高的优先）
    scored.sort(key=lambda item: item["quality_score"], reverse=True)
    quality_log = None
    quality_rows = []
    for rank, item in enumerate(scored, start=1):
        quality_rows.append(
            {
                "cluster_id": int(cid),
                "resolution": str(resolution),
                "quality_rank": int(rank),
                "n_candidates": int(len(scored)),
                "is_top_ranked": int(rank == 1),
                "source": item["source"],
                "source_station_index": int(item["source_station_index"]),
                "path": item["path"],
                "quality_score": float(item["quality_score"]),
                "good_flag_count": int(item["good_flag_count"]),
                "valid_flag_count": int(item["valid_flag_count"]),
                "n_time_rows": int(item["n_time_rows"]),
                "n_nonempty_rows": int(item["n_nonempty_rows"]),
            }
        )

    if len(scored) > 1:
        score_info = ", ".join(
            "{} {:.1%}".format(item["source"], item["quality_score"]) for item in scored
        )
        quality_log = "  cluster {} [{}] quality order: [{}]".format(
            cid, resolution, score_info
        )

    # 将 date 列设为 index 以加速查找，按质量顺序排列
    indexed = [item["df"].set_index("date") for item in scored]
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
    source_station_idx_arr = np.full(n, -1, dtype=np.int32)

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
            source_station_idx_arr[i] = int(row.get("_source_station_index", -1))
            break

    return (
        dates_arr,
        q_arr,
        ssc_arr,
        ssl_arr,
        q_flag_arr,
        ssc_flag_arr,
        ssl_flag_arr,
        is_overlap_arr,
        source_arr,
        source_station_idx_arr,
        quality_rows,
        quality_log,
        unit_issues,
    )


def _worker_build_cluster(args):
    cid, resolution, recs = args
    result = build_cluster_series(cid, resolution, recs)
    if result is None:
        return (cid, resolution, None, [], None, [])
    if len(result) == 2 and result[0] is None:
        return (cid, resolution, None, [], None, list(result[1]))
    (dates_arr, q_arr, ssc_arr, ssl_arr,
     q_flag, ssc_flag, ssl_flag, is_overlap, source_arr,
     source_station_idx_arr, quality_rows, quality_log, unit_issues) = result
    return (cid, resolution,
            (dates_arr, q_arr, ssc_arr, ssl_arr, q_flag, ssc_flag, ssl_flag,
             is_overlap, source_arr, source_station_idx_arr),
            quality_rows, quality_log, unit_issues)


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
    ap.add_argument(
        "--quality-order-csv",
        default=str(_DEFAULT_QUALITY_ORDER),
        help="输出每个 cluster/resolution 候选质量排序表。默认: {}".format(_DEFAULT_QUALITY_ORDER),
    )
    ap.add_argument("--workers", "-w", type=int, default=_DEFAULT_WORKERS,
                    help="并行进程数（0 = 自动取 CPU 核数）。默认: {}".format(_DEFAULT_WORKERS))
    ap.add_argument(
        "--include-climatology",
        action="store_true",
        help="默认会过滤掉 climatology 行；如确实需要将 climatology 混入主库，可显式开启此选项",
    )
    ap.add_argument(
        "--strict-units",
        action="store_true",
        help="对输入 Q/SSC/SSL units 启用严格校验；若缺失或不在白名单中则直接失败并停止写出。",
    )
    args = ap.parse_args()

    global STRICT_UNIT_CHECK
    STRICT_UNIT_CHECK = bool(args.strict_units)

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1

    inp_path = Path(args.input)
    out_path = Path(args.output)
    quality_order_path = Path(args.quality_order_csv)

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

    if not args.include_climatology:
        res_lower = stations["resolution"].fillna("").astype(str).str.strip().str.lower()
        n_clim = int((res_lower == "climatology").sum())
        if n_clim > 0:
            stations = stations[res_lower != "climatology"].copy()
            print("Filtered out {} climatology rows from basin mainline input.".format(n_clim))
        if len(stations) == 0:
            print("Error: no non-climatology rows remain after filtering.")
            return 1

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
    basin_areas       = np.full(n_stations, FILL,  dtype=np.float32)
    pfaf_codes        = np.full(n_stations, FILL,  dtype=np.float32)
    n_reaches_arr     = np.full(n_stations, -9999, dtype=np.int32)
    match_quality_arr = np.full(n_stations, -1,    dtype=np.int8)
    basin_distance_arr = np.full(n_stations, FILL, dtype=np.float32)
    point_in_local_arr = np.zeros(n_stations, dtype=np.int8)
    point_in_basin_arr = np.zeros(n_stations, dtype=np.int8)
    basin_status_arr = ["unknown"] * n_stations
    basin_flag_arr = ["unknown"] * n_stations

    for cid, idx in cluster_to_idx.items():
        if cid in rep.index:
            row = rep.loc[cid]
            ba = row.get("basin_area",        None)
            pf = row.get("pfaf_code",         None)
            nr = row.get("n_upstream_reaches",None)
            mq = str(row.get("match_quality", "unknown") or "unknown").strip()
            dist = row.get("distance_m", None)
            basin_status = str(row.get("basin_status", "unknown") or "unknown").strip()
            basin_flag = str(row.get("basin_flag", "unknown") or "unknown").strip()
            basin_areas[idx]       = float(ba) if ba is not None and not pd.isna(ba) else FILL
            pfaf_codes[idx]        = float(pf) if pf is not None and not pd.isna(pf) else FILL
            n_reaches_arr[idx]     = int(nr)   if nr is not None and not pd.isna(nr) else -9999
            match_quality_arr[idx] = np.int8(MATCH_QUALITY_CODES.get(mq, -1))
            basin_distance_arr[idx] = float(dist) if dist is not None and not pd.isna(dist) else FILL
            point_in_local_arr[idx] = np.int8(1 if _clean_bool(row.get("point_in_local", False)) else 0)
            point_in_basin_arr[idx] = np.int8(1 if _clean_bool(row.get("point_in_basin", False)) else 0)
            basin_status_arr[idx] = basin_status
            basin_flag_arr[idx] = basin_flag

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

    # ── 2e. 构建完整的 source-station 映射表（同一 cluster 下可保留多个原始站点）────
    for col in ("station_name", "river_name", "source_station_id"):
        if col not in stations.columns:
            stations[col] = ""
        stations[col] = stations[col].fillna("").astype(str)

    source_station_rows = []
    source_station_lookup = {}
    row_source_station_index = []

    for row in stations.itertuples(index=False):
        row_dict = {
            "path": getattr(row, "path", ""),
            "source": getattr(row, "source", ""),
            "cluster_id": getattr(row, "cluster_id", -1),
            "lat": getattr(row, "lat", np.nan),
            "lon": getattr(row, "lon", np.nan),
            "resolution": getattr(row, "resolution", "other"),
            "station_name": getattr(row, "station_name", ""),
            "river_name": getattr(row, "river_name", ""),
            "source_station_id": getattr(row, "source_station_id", ""),
        }
        key = _build_source_station_key(row_dict)
        idx = source_station_lookup.get(key)
        if idx is None:
            idx = len(source_station_rows)
            source_station_lookup[key] = idx
            source_station_rows.append(
                {
                    "cluster_idx": cluster_to_idx[int(row_dict["cluster_id"])],
                    "source_idx": source_to_idx[str(row_dict["source"])],
                    "source_name": _clean_text(row_dict["source"]),
                    "native_id": _clean_text(row_dict["source_station_id"]),
                    "station_name": _clean_text(row_dict["station_name"]),
                    "river_name": _clean_text(row_dict["river_name"]),
                    "lat": _clean_coord(row_dict["lat"]),
                    "lon": _clean_coord(row_dict["lon"]),
                    "paths": set(),
                    "resolutions": set(),
                }
            )
        source_station_rows[idx]["paths"].add(str(row_dict["path"]))
        source_station_rows[idx]["resolutions"].add(str(row_dict["resolution"]))
        row_source_station_index.append(idx)

    stations["_source_station_index"] = np.asarray(row_source_station_index, dtype=np.int32)
    n_source_stations = len(source_station_rows)
    source_station_uids = ["SRC{:06d}".format(i) for i in range(n_source_stations)]
    source_station_cluster_index = np.full(n_source_stations, -1, dtype=np.int32)
    source_station_source_index = np.full(n_source_stations, -1, dtype=np.int32)
    source_station_native_ids = [""] * n_source_stations
    source_station_names = [""] * n_source_stations
    source_station_rivers = [""] * n_source_stations
    source_station_lats = np.full(n_source_stations, FILL, dtype=np.float32)
    source_station_lons = np.full(n_source_stations, FILL, dtype=np.float32)
    source_station_paths = [""] * n_source_stations
    source_station_resolutions = [""] * n_source_stations
    cluster_source_station_counts = np.zeros(n_stations, dtype=np.int32)

    for idx, info in enumerate(source_station_rows):
        source_station_cluster_index[idx] = int(info["cluster_idx"])
        source_station_source_index[idx] = int(info["source_idx"])
        source_station_native_ids[idx] = info["native_id"]
        source_station_names[idx] = info["station_name"]
        source_station_rivers[idx] = info["river_name"]
        if info["lat"] is not None:
            source_station_lats[idx] = info["lat"]
        if info["lon"] is not None:
            source_station_lons[idx] = info["lon"]
        source_station_paths[idx] = "|".join(sorted(info["paths"]))[:2048]
        source_station_resolutions[idx] = "|".join(sorted(info["resolutions"]))
        cluster_source_station_counts[int(info["cluster_idx"])] += 1

    print("Source-station map: {} unique source stations across {} clusters".format(
        n_source_stations, n_stations
    ))

    # ── 3. 按 (cluster_id, resolution) 分组 ── 用 groupby，避免 iterrows ──
    by_cluster_res = defaultdict(list)
    for (cid, res), grp in stations.groupby(["cluster_id", "resolution"], sort=False):
        by_cluster_res[(int(cid), str(res))] = list(
            zip(grp["source"], grp["path"], grp["_source_station_index"])
        )

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
    parts_source_station_idx = []  # list of int32 arrays
    quality_order_rows = []
    unit_issue_rows = []
    n_empty    = 0
    n_done     = 0

    def _flush_result(pbar, cid, resolution, res, quality_rows, quality_log, unit_issues):
        """将单个任务结果展平并追加到 parts_* 列表，同时更新进度条。"""
        nonlocal n_empty, n_done
        n_done += 1
        if quality_rows:
            quality_order_rows.extend(quality_rows)
        if unit_issues:
            unit_issue_rows.extend(unit_issues)
        # 质量排序日志通过 tqdm.write 输出，不破坏进度条
        if quality_log:
            tqdm.write(quality_log)
        if res is None:
            n_empty += 1
        else:
            (dates_arr, q_arr, ssc_arr, ssl_arr,
             q_flag, ssc_flag, ssl_flag, is_overlap,
             source_arr, source_station_idx_arr) = res
            n    = len(dates_arr)
            days = ((dates_arr - ref).total_seconds().values / 86400.0).astype(np.float64)
            rc   = RESOLUTION_CODES.get(str(resolution).lower(), RESOLUTION_CODES["other"])
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
            parts_source_station_idx.append(source_station_idx_arr.astype(np.int32))

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
                    cid, resolution, res, quality_rows, quality_log, unit_issues = future.result()
                    _flush_result(pbar, cid, resolution, res, quality_rows, quality_log, unit_issues)
    else:
        with tqdm(**_pbar_fmt) as pbar:
            for (cid, res, recs) in tasks:
                result = build_cluster_series(cid, res, recs)
                if result is None:
                    _flush_result(pbar, cid, res, None, [], None, [])
                elif len(result) == 2 and result[0] is None:
                    _flush_result(pbar, cid, res, None, [], None, list(result[1]))
                else:
                    (dates_arr, q_arr, ssc_arr, ssl_arr,
                     q_flag, ssc_flag, ssl_flag, is_overlap, source_arr,
                     source_station_idx_arr, quality_rows, quality_log, unit_issues) = result
                    _flush_result(pbar, cid, res,
                                  (dates_arr, q_arr, ssc_arr, ssl_arr,
                                   q_flag, ssc_flag, ssl_flag, is_overlap,
                                   source_arr, source_station_idx_arr),
                                  quality_rows, quality_log, unit_issues)

    n_series = n_done - n_empty
    print("Series: {}/{} non-empty  ({} empty/missing)".format(n_series, n_tasks, n_empty))
    _check_memory("时间序列读取完成后")

    _summarize_unit_issues(
        unit_issue_rows,
        strict_mode=STRICT_UNIT_CHECK,
        label="Input unit validation",
    )
    if STRICT_UNIT_CHECK and any(issue.get("severity") == "error" for issue in unit_issue_rows):
        print(
            "Error: strict unit validation failed; aborting write. Please fix source units and rerun the full chain before publishing."
        )
        return 1

    quality_row_count = _write_quality_order_csv(
        quality_order_rows,
        quality_order_path,
        cluster_to_idx,
        source_station_uids,
    )
    print("Wrote quality order CSV: {} ({} rows)".format(
        quality_order_path, quality_row_count
    ))

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
    record_source_station_idx_arr = np.concatenate(parts_source_station_idx)

    # 释放 parts 列表
    del (parts_idx, parts_time, parts_res, parts_q, parts_ssc, parts_ssl,
         parts_qflag, parts_sscflag, parts_sslflag, parts_overlap, parts_source,
         parts_source_station_idx)

    n_records = len(time_arr)
    _check_memory("数组合并后")

    # ── 6. 写 NC ──────────────────────────────────────────────────────────
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print("Writing {} (n_stations={}, n_records={:,}) ...".format(
        out_path, n_stations, n_records))

    with nc4.Dataset(out_path, "w", format="NETCDF4") as nc:
        nc.createDimension("n_stations", n_stations)
        nc.createDimension("n_source_stations", n_source_stations)
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
        bm_v.flag_values  = np.array([0, 1, 2, 3, 4, -1], dtype=np.int8)
        bm_v.flag_meanings = MATCH_QUALITY_MEANINGS
        bm_v[:]           = match_quality_arr

        bd_v = nc.createVariable("basin_distance_m", "f4", ("n_stations",), fill_value=FILL)
        bd_v.long_name = "distance from original station point to matched reach"
        bd_v.units = "m"
        bd_v[:] = basin_distance_arr

        pil_v = nc.createVariable("point_in_local", "i1", ("n_stations",), fill_value=np.int8(0))
        pil_v.long_name = "whether the original station point is covered by the matched local catchment"
        pil_v.flag_values = np.array([0, 1], dtype=np.int8)
        pil_v.flag_meanings = "false true"
        pil_v[:] = point_in_local_arr

        pib_v = nc.createVariable("point_in_basin", "i1", ("n_stations",), fill_value=np.int8(0))
        pib_v.long_name = "whether the original station point is covered by the traced upstream basin"
        pib_v.flag_values = np.array([0, 1], dtype=np.int8)
        pib_v.flag_meanings = "false true"
        pib_v[:] = point_in_basin_arr

        bs_v = nc.createVariable("basin_status", str, ("n_stations",))
        bs_v.long_name = "release-facing basin assignment status"
        bs_v.comment = "resolved stations may keep basin polygons; unresolved stations retain observations without a published basin assignment"
        bs_v[:] = np.array(basin_status_arr, dtype=object)

        bf_v = nc.createVariable("basin_flag", str, ("n_stations",))
        bf_v.long_name = "release-facing basin status reason flag"
        bf_v[:] = np.array(basin_flag_arr, dtype=object)

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

        nss_v = nc.createVariable("n_source_stations_in_cluster", "i4", ("n_stations",))
        nss_v.long_name = "number of unique source stations mapped into this cluster"
        nss_v[:] = cluster_source_station_counts

        # ── n_source_stations 查找表（原始站点完整映射）───────────────────────
        ss_uid_v = nc.createVariable("source_station_uid", str, ("n_source_stations",))
        ss_uid_v.long_name = "stable source-station identifier used inside the merged reference dataset"
        ss_uid_v.comment = "format: SRC + 6-digit index; use source_station_index in n_records to join"
        ss_uid_v[:] = np.array(source_station_uids, dtype=object)

        ss_cluster_v = nc.createVariable("source_station_cluster_index", "i4", ("n_source_stations",))
        ss_cluster_v.long_name = "0-based index into n_stations dimension for the cluster containing this source station"
        ss_cluster_v[:] = source_station_cluster_index

        ss_source_v = nc.createVariable("source_station_source_index", "i4", ("n_source_stations",))
        ss_source_v.long_name = "0-based index into n_sources dimension for the dataset owning this source station"
        ss_source_v[:] = source_station_source_index

        ss_native_v = nc.createVariable("source_station_native_id", str, ("n_source_stations",))
        ss_native_v.long_name = "original station identifier in the native source dataset"
        ss_native_v[:] = np.array(source_station_native_ids, dtype=object)

        ss_name_v = nc.createVariable("source_station_name", str, ("n_source_stations",))
        ss_name_v.long_name = "original station name"
        ss_name_v[:] = np.array(source_station_names, dtype=object)

        ss_river_v = nc.createVariable("source_station_river_name", str, ("n_source_stations",))
        ss_river_v.long_name = "original river name"
        ss_river_v[:] = np.array(source_station_rivers, dtype=object)

        ss_lat_v = nc.createVariable("source_station_lat", "f4", ("n_source_stations",), fill_value=FILL)
        ss_lat_v.long_name = "latitude of the source station"
        ss_lat_v.units = "degrees_north"
        ss_lat_v[:] = source_station_lats

        ss_lon_v = nc.createVariable("source_station_lon", "f4", ("n_source_stations",), fill_value=FILL)
        ss_lon_v.long_name = "longitude of the source station"
        ss_lon_v.units = "degrees_east"
        ss_lon_v[:] = source_station_lons

        ss_path_v = nc.createVariable("source_station_paths", str, ("n_source_stations",))
        ss_path_v.long_name = "pipe-separated list of organized NC file paths contributing to this source station"
        ss_path_v[:] = np.array(source_station_paths, dtype=object)

        ss_res_v = nc.createVariable("source_station_resolutions", str, ("n_source_stations",))
        ss_res_v.long_name = "pipe-separated list of time types available for this source station"
        ss_res_v[:] = np.array(source_station_resolutions, dtype=object)

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

        ssid_v = nc.createVariable("source_station_index", "i4", ("n_records",), fill_value=-1)
        ssid_v.long_name = "0-based index into n_source_stations dimension for the chosen source-station record"
        ssid_v.comment = "links every merged record back to one source station in the provenance table"
        ssid_v[:] = record_source_station_idx_arr

        res_v = nc.createVariable("resolution", "i1", ("n_records",))
        res_v.long_name = "time type code for this record"
        res_v.flag_values = np.array([0, 1, 2, 3, 4], dtype=np.int8)
        res_v.flag_meanings = "daily monthly annual climatology other"
        res_v.comment = ("single_point inputs are mapped to daily, quarterly inputs are mapped to monthly; "
                         "legacy annually_climatology inputs are mapped to climatology")
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
                           "monthly/annual/climatology records. See 'resolution' variable.")
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
        nc.time_type_policy = ("Only four main time types are retained in the final product: "
                               "daily/monthly/annual/climatology; single_point is mapped to daily, "
                               "quarterly is mapped to monthly. In the basin mainline, climatology is "
                               "normally exported separately and therefore filtered out by default.")
        nc.provenance_policy = ("Each merged record links back to one source_station_index, while the full "
                                "source-station mapping is preserved in n_source_stations")
        nc.basin_csv      = str(inp_path)
        nc.n_input_station_rows = str(len(stations))
        nc.n_source_stations = str(n_source_stations)
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
