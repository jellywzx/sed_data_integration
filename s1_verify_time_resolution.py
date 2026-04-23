#!/usr/bin/env python3
"""
对默认输入根目录 Output_r 下所有数据集中的 .nc 文件应用 time_resolution 分类逻辑，
并检查是否与路径中的分类（daily / monthly / annually_climatology）一致；
不一致时输出报告。

对 single_point 会进一步查看元数据，识别是否为长时间历史平均（如 1970-2021）。

依赖: numpy, pandas, xarray

用法（在 Output_r 根目录下运行）：
    python scripts/s1_verify_time_resolution.py
    python scripts/s1_verify_time_resolution.py --dataset RiverSed
    python scripts/s1_verify_time_resolution.py --dataset RiverSed --dataset GloRiC

输入（默认）：
  - Output_r 下原始 .nc 数据（脚本会递归扫描）
输出（默认）：
  - scripts/output/s1_verify_time_resolution_results.csv（步骤 s1 输出，来自 pipeline_paths.S1_VERIFY_CSV，供 s2 使用）

---
判断过程说明
---
1) 路径分类（path_resolution）
   根据文件相对 Output_r 的路径，取第一级目录名作为「路径分类」：
   - 目录名为 daily          -> path_resolution = "daily"
   - 目录名为 monthly        -> path_resolution = "monthly"
   - 目录名含 annually 或 climatology -> path_resolution = "annually_climatology"
   - 其他或无法解析          -> path_resolution = None（后续不参与一致性判定）

2) 时间频率检测（detected_frequency）
   打开 .nc，找到时间变量（time / Time / t / datetime / date 之一），读取时间序列：
   - 若无时间变量或读取异常   -> detected_frequency = "no_time_var" 或 "error:..."
   - 若时间点个数 < 2         -> detected_frequency = "single_point"
   - 否则计算相邻时间间隔（小时）的中位数 median_diff，按 time_resolution.classify_frequency 规则：
     median_diff < 2 小时        -> "hourly"
     < 36 小时                   -> "daily"
     < 24*45 小时（约 45 天）    -> "monthly"
     < 24*120 小时（约 4 个月）  -> "quarterly"
     < 24*500 小时（约 20 个月） -> "annual"
     更大或间隔差异大            -> "irregular"

3) single_point 时的元数据解释（single_point_interpretation）
   仅当 detected_frequency = "single_point" 时执行。从 nc 的全局属性、时间变量属性及 time_bounds 等
   收集文本，并做以下判断（按顺序，命中即返回）：
   - 无时间变量               -> "single_point_no_time_var"
   - 源数据属性 temporal_resolution 为 daily，且 temporal_span 与单时间点一致 -> 视为 daily（detected_frequency 记为 daily）
   - 源数据属性 temporal_resolution 为 climatology/climatological，且 temporal_span（或 time_coverage）与单时间点一致
     -> 视为 annual（气候态），detected_frequency 记为 annual，解释为 "single_point_upgraded_to_annual_by_temporal_resolution_climatology"
   - 在属性中匹配 19xx-20xx 或 20xx-20xx 的年份范围，且文本含 climatology/average/mean/long-term/historical 等
     -> "long_term_average_起始年_结束年"（视为长时间历史平均）
   - 有上述关键词但无年份范围  -> "single_point_likely_climatology_year_单点年份"
   - 仅有年份范围（如 time_coverage_start/end 或 bounds） -> "long_term_average_起始年_结束年"
   - 否则                    -> "single_point_time_具体时间" 或 "single_point_interpret_error_..."

4) 一致性判断（consistent）
   仅对路径分类为 daily / monthly / annually_climatology 的文件做一致性判定：
   - 路径 daily               -> 仅当 detected_frequency = "daily" 时 consistent = True
   - 路径 monthly             -> 仅当 detected_frequency = "monthly" 时 consistent = True
   - 路径 annually_climatology-> 仅当 detected_frequency 为 "annual" 或 "quarterly" 时 consistent = True
   - detected_frequency 为 "error:..." 或 "no_time_var" 时一律判为不一致（consistent = False）
   - 路径为其他或 None        -> 不判定，consistent = True（不纳入不一致统计）

---
输出文件说明
---
默认输出路径：scripts/output/s1_verify_time_resolution_results.csv（步骤 s1 对应输出，无子文件夹）

CSV 列说明：
  - path                 : 该 .nc 文件的绝对路径
  - rel_path             : 相对于 Output_r 根目录的路径（便于定位数据集）
  - path_resolution      : 从路径第一级目录解析出的分类
                           取值为 daily / monthly / annually_climatology 或 (none)
  - detected_frequency    : 根据时间轴间隔检测出的频率
                           取值为 hourly / daily / monthly / quarterly / annual / irregular /
                           single_point / no_time_var / error:...
  - temporal_semantics   : 在 detected_frequency 基础上进一步解释出的时间语义
                           取值为 daily / monthly / annual / climatology / quarterly /
                           single_point / irregular / no_time_var / error / other
  - single_point_interpretation : 仅当 detected_frequency 为 single_point 时有内容
                           可能为 long_term_average_YYYY_YYYY、single_point_time_...、
                           single_point_likely_climatology_year_YYYY 等，用于判断是否长时间历史平均
  - consistent            : True 表示路径分类与检测结果一致，False 表示不一致需人工核查

筛选不一致记录：在 Excel 或 pandas 中对 consistent 列筛 False 即可得到需检查的文件列表。
"""

import os
import re
import sys
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from pipeline_paths import S1_REVIEW_OVERRIDES_CSV, S1_REVIEW_QUEUE_CSV, S1_VERIFY_CSV
import time
from tqdm import tqdm
from xarray.coding.times import decode_cf_datetime
import argparse
from qc_contract import (
    TEMPORAL_RESOLUTION_ATTR_KEYS,
    TEMPORAL_SPAN_ATTR_KEYS,
    TIME_COVERAGE_END_ATTR_KEYS,
    TIME_COVERAGE_START_ATTR_KEYS,
    TIME_VAR_NAMES,
    ensure_stage1_alias_parity,
    get_first_attr_value,
    normalize_declared_temporal_resolution,
)

# 根目录固定为脚本所在目录的上一级（即 Output_r）
SCRIPT_DIR = Path(__file__).resolve().parent
# 结果 CSV 路径（相对 ROOT_DIR）；列说明见本文件顶部 docstring；步骤 s1 对应输出
OUT_CSV = S1_VERIFY_CSV
REVIEW_QUEUE_CSV = S1_REVIEW_QUEUE_CSV
REVIEW_OVERRIDES_CSV = S1_REVIEW_OVERRIDES_CSV
WORKERS = 32

from time_resolution import classify_frequency

ROOT_DIR = SCRIPT_DIR.parent

def _parse_args():
    parser = argparse.ArgumentParser(
        description="检查 nc 文件时间分辨率是否与路径分类一致，支持按数据集过滤。"
    )
    parser.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        default=[],
        help=(
            "仅扫描指定数据集，可重复传入多次，例如 "
            "--dataset RiverSed --dataset GloRiC。"
        ),
    )
    return parser.parse_args()


def _normalize_dataset_filters(datasets):
    normalized = []
    for item in datasets or []:
        text = str(item).strip()
        if text:
            normalized.append(text.lower())
    return sorted(set(normalized))


def _match_dataset_filter(rel_parts, dataset_filters):
    if len(rel_parts) < 3:
        return False

    dataset_name = str(rel_parts[1]).strip().lower()
    folder_name = str(rel_parts[2]).strip().lower()

    if folder_name != "qc":
        return False

    if not dataset_filters:
        return True

    return dataset_name in dataset_filters

def get_resolution_from_path(filepath, root_dir):
    """从相对路径的第一级目录解析：daily, monthly, annually_climatology。"""
    try:
        root = Path(root_dir).resolve()
        path = Path(filepath).resolve()
        rel = path.relative_to(root)
        parts = rel.parts
        if parts:
            res = parts[0].strip().lower()
            if res == "daily":
                return "daily"
            if res == "monthly":
                return "monthly"
            if "annually" in res or "climatology" in res:
                return "annually_climatology"
            return res
    except Exception:
        pass
    return None


def _year_range_in_text(text):
    if not text or not isinstance(text, str):
        return None
    m = re.search(r"(19\d{2})\s*[-–to]+\s*(20\d{2})", text, re.I)
    if m:
        return (m.group(1), m.group(2))
    m = re.search(r"(20\d{2})\s*[-–to]+\s*(20\d{2})", text, re.I)
    if m:
        return (m.group(1), m.group(2))
    return None


def _parse_span_to_dates(span_str):
    """从 temporal_span 等字符串解析起止日期，返回 (start_date, end_date) 或 None。"""
    if not span_str or not isinstance(span_str, str):
        return None
    s = span_str.strip()
    # 支持纯年份范围 "1962-1971" 或 "1962 - 1971"
    m = re.match(r"^(\d{4})\s*[-–]\s*(\d{4})$", s)
    if m:
        try:
            y1, y2 = int(m.group(1)), int(m.group(2))
            return (pd.Timestamp(f"{y1}-01-01"), pd.Timestamp(f"{y2}-12-31"))
        except Exception:
            pass
    # 支持两段 ISO 日期用空格分隔（如 time_coverage_start + time_coverage_end）
    if " " in s and s.count(" ") == 1:
        parts = s.split(" ", 1)
        try:
            d1, d2 = pd.to_datetime(parts[0].strip()), pd.to_datetime(parts[1].strip())
            return (d1, d2)
        except Exception:
            pass
    # 支持 "start end" 或 "start/end" 或 "start to end"
    for sep in ["/", " to ", " - ", "\t"]:
        if sep in s:
            parts = re.split(re.escape(sep) if sep != " - " else r"\s*-\s*", s, 1)
            if len(parts) == 2:
                try:
                    d1 = pd.to_datetime(parts[0].strip())
                    d2 = pd.to_datetime(parts[1].strip())
                    return (d1, d2)
                except Exception:
                    pass
            break
    try:
        d = pd.to_datetime(s)
        return (d, d)
    except Exception:
        pass
    return None


def _first_nonempty_attr(attrs, *keys):
    for key in keys:
        if key not in attrs:
            continue
        value = attrs.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in ("none", "nan"):
            return value
    return None


def _single_time_matches_span(single_time, attrs):
    """判断 single_time 是否与 temporal_span / time_coverage 描述相符。"""
    span_raw = _first_nonempty_attr(attrs, *TEMPORAL_SPAN_ATTR_KEYS)
    if span_raw is None:
        start = _first_nonempty_attr(attrs, *TIME_COVERAGE_START_ATTR_KEYS)
        end = _first_nonempty_attr(attrs, *TIME_COVERAGE_END_ATTR_KEYS)
        if start and end:
            span_raw = "{} {}".format(start, end)
        elif start:
            span_raw = start
        elif end:
            span_raw = end

    if span_raw is None:
        return True, None, None

    parsed = _parse_span_to_dates(str(span_raw))
    if not parsed:
        return True, span_raw, None

    try:
        single_ts = pd.Timestamp(single_time)
        start_ts = pd.Timestamp(parsed[0])
        end_ts = pd.Timestamp(parsed[1])
    except Exception:
        return False, span_raw, None

    return start_ts <= single_ts <= end_ts, span_raw, (start_ts, end_ts)


def _collect_text_evidence(global_attrs, time_attrs, bounds_data):
    texts = []
    for attrs in (global_attrs, time_attrs):
        for _, value in attrs.items():
            if isinstance(value, str) and value.strip():
                texts.append(value)

    if bounds_data is not None:
        try:
            bnds_flat = np.atleast_1d(bounds_data).flatten()
            if len(bnds_flat) >= 2:
                t_start = pd.to_datetime(bnds_flat[0])
                t_end = pd.to_datetime(bnds_flat[-1])
                if hasattr(t_start, "year") and hasattr(t_end, "year"):
                    texts.append("{}-{}".format(t_start.year, t_end.year))
        except Exception:
            pass

    t_start = _first_nonempty_attr(global_attrs, *TIME_COVERAGE_START_ATTR_KEYS)
    t_end = _first_nonempty_attr(global_attrs, *TIME_COVERAGE_END_ATTR_KEYS)
    if t_start and t_end:
        try:
            y1 = re.search(r"(\d{4})", str(t_start))
            y2 = re.search(r"(\d{4})", str(t_end))
            if y1 and y2:
                texts.append("{}-{}".format(y1.group(1), y2.group(1)))
        except Exception:
            pass
    return texts


def _single_point_declared_resolution(attrs, single_time):
    if single_time is None or not isinstance(attrs, dict):
        return ""

    res_raw = get_first_attr_value(attrs, TEMPORAL_RESOLUTION_ATTR_KEYS)
    if res_raw is None:
        return ""

    res = normalize_declared_temporal_resolution(res_raw)
    span_matches, span_raw, parsed_span = _single_time_matches_span(single_time, attrs)

    if res == "daily":
        if not span_matches:
            return ""
        if parsed_span is not None:
            start_ts, end_ts = parsed_span
            if abs(end_ts - start_ts) > pd.Timedelta(hours=36):
                return ""
        return "daily"

    if res == "climatology":
        if span_matches:
            return "climatology"
        if span_raw is not None and _year_range_in_text(str(span_raw)):
            return "climatology"
        return ""

    return res


def _interpret_sp_from_data(single_time, global_attrs, time_attrs, bounds_data):
    """纯数据版本：从已提取的 attrs 和 bounds 判断 single_point 元数据，无文件 I/O。"""
    if single_time is None:
        return "single_point_no_time_var"

    merged_attrs = dict(global_attrs)
    for k, v in time_attrs.items():
        if k not in merged_attrs and v is not None:
            merged_attrs[k] = v

    texts = _collect_text_evidence(global_attrs, time_attrs, bounds_data)

    for text in texts:
        low = text.lower()
        if any(kw in low for kw in ("climatology", "climatological", "average", "mean", "long-term", "long term", "historical")):
            yr = _year_range_in_text(text)
            if yr:
                return "long_term_average_{}_{}".format(yr[0], yr[1])
            if hasattr(single_time, "year"):
                return "single_point_likely_climatology_year_{}".format(single_time.year)

    for text in texts:
        yr = _year_range_in_text(text)
        if yr:
            return "long_term_average_{}_{}".format(yr[0], yr[1])

    return "single_point_time_{}".format(str(single_time))


def _time_axis_semantics_from_frequency(raw_freq):
    freq = str(raw_freq or "").strip().lower()
    if freq in ("hourly", "daily"):
        return "daily"
    if freq in ("monthly", "quarterly"):
        return "monthly"
    if freq == "annual":
        return "annual"
    if freq in ("single_point", "irregular", "no_time_var"):
        return freq
    if freq.startswith("error:"):
        return "error"
    return "other"


def _metadata_semantics_from_attrs(single_time, global_attrs, time_attrs, bounds_data, single_point_interpretation):
    merged_attrs = dict(global_attrs)
    for key, value in time_attrs.items():
        if key not in merged_attrs and value is not None:
            merged_attrs[key] = value

    declared = normalize_declared_temporal_resolution(
        get_first_attr_value(merged_attrs, TEMPORAL_RESOLUTION_ATTR_KEYS)
    )
    if single_time is not None:
        declared = _single_point_declared_resolution(merged_attrs, single_time) or declared

    if declared in ("daily", "monthly", "annual", "climatology"):
        return declared

    interp = str(single_point_interpretation or "").strip().lower()
    if interp.startswith("long_term_average_") or interp.startswith("single_point_likely_climatology_year_"):
        return "climatology"

    for text in _collect_text_evidence(global_attrs, time_attrs, bounds_data):
        low = text.lower()
        if any(kw in low for kw in ("climatology", "climatological", "long-term", "long term", "historical")):
            return "climatology"
        if any(kw in low for kw in ("average", "mean")) and _year_range_in_text(text):
            return "climatology"
    return ""


def interpret_single_point_metadata(filepath):
    """原接口保留，现在打开文件并调用无 I/O 的内部版本。"""
    try:
        with xr.open_dataset(filepath) as ds:
            time_var = None
            for c in TIME_VAR_NAMES:
                if c in ds.variables:
                    time_var = c
                    break
            if time_var is None:
                return "single_point_no_time_var"
            t = ds[time_var]
            time_vals = np.atleast_1d(pd.to_datetime(t.values))
            if len(time_vals) != 1:
                return "single_point"
            single_time = time_vals[0]
            global_attrs = dict(ds.attrs)
            time_attrs = dict(t.attrs)
            bounds_data = None
            bv = time_attrs.get("bounds")
            if bv and bv in ds.variables:
                try:
                    bounds_data = ds[bv].values
                except Exception:
                    pass
        return _interpret_sp_from_data(single_time, global_attrs, time_attrs, bounds_data)
    except Exception as e:
        return f"single_point_interpret_error_{e}"

def detect_frequency_for_nc(filepath):
    """对单个 nc 检测时间频率和多证据语义。每个文件只打开一次。"""
    try:
        with xr.open_dataset(filepath, decode_times=False) as ds:
            time_var = None
            for cand in TIME_VAR_NAMES:
                if cand in ds.variables:
                    time_var = cand
                    break
            if time_var is None:
                return {
                    "path": filepath,
                    "raw_detected_frequency": "no_time_var",
                    "detected_frequency": "no_time_var",
                    "single_point_interpretation": "",
                    "time_axis_semantics": "no_time_var",
                    "metadata_semantics": "",
                }

            t = ds[time_var]
            time_raw = t.values
            time_attrs = dict(t.attrs)
            global_attrs = dict(ds.attrs)

            # 提取 bounds（如有）
            bounds_data = None
            bv = time_attrs.get("bounds")
            if bv and bv in ds.variables:
                try:
                    bounds_data = ds[bv].values
                except Exception:
                    pass

        # 在文件关闭后解码时间（避免持续占用文件句柄）
        try:
            units = time_attrs.get("units", "")
            calendar = time_attrs.get("calendar", "standard")
            if units:
                decoded = decode_cf_datetime(time_raw, units=units, calendar=calendar)
                time_values = pd.DatetimeIndex(decoded)
            else:
                time_values = pd.to_datetime(time_raw)
        except Exception:
            try:
                time_values = pd.to_datetime(time_raw)
            except Exception as e:
                err = f"error: time decode failed: {e}"
                return {
                    "path": filepath,
                    "raw_detected_frequency": err,
                    "detected_frequency": err,
                    "single_point_interpretation": "",
                    "time_axis_semantics": "error",
                    "metadata_semantics": "",
                }

        raw_freq = classify_frequency(time_values)
        single_interp = ""
        single_time = None

        if raw_freq == "single_point":
            single_time = time_values[0] if len(time_values) > 0 else None
            single_interp = _interpret_sp_from_data(
                single_time, global_attrs, time_attrs, bounds_data
            )
        return {
            "path": filepath,
            "raw_detected_frequency": raw_freq,
            "detected_frequency": raw_freq,
            "single_point_interpretation": single_interp or "",
            "time_axis_semantics": _time_axis_semantics_from_frequency(raw_freq),
            "metadata_semantics": _metadata_semantics_from_attrs(
                single_time, global_attrs, time_attrs, bounds_data, single_interp
            ),
        }
    except Exception as e:
        err = f"error: {str(e)}"
        return {
            "path": filepath,
            "raw_detected_frequency": err,
            "detected_frequency": err,
            "single_point_interpretation": "",
            "time_axis_semantics": "error",
            "metadata_semantics": "",
        }


PATH_TO_EXPECTED = {
    "daily": ["daily"],
    "monthly": ["monthly"],
    "annually_climatology": ["annual", "climatology"],
}


def _path_semantics(path_resolution):
    text = str(path_resolution or "").strip().lower()
    if text in ("daily", "monthly", "annually_climatology"):
        return text
    return ""


def is_consistent(path_resolution, final_semantics):
    if path_resolution is None or path_resolution not in PATH_TO_EXPECTED:
        return True
    allowed = PATH_TO_EXPECTED[path_resolution]
    freq = str(final_semantics or "").strip().lower()
    if freq.startswith("error:") or freq in ("no_time_var", "single_point", "irregular", ""):
        return False
    return freq in allowed


VALID_OVERRIDE_SEMANTICS = {"daily", "monthly", "annual", "climatology", "other"}


def _path_is_qc(rel_path):
    try:
        return "qc" in Path(rel_path).parts
    except Exception:
        return False


def _write_override_template(path_obj):
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    if path_obj.exists():
        return
    pd.DataFrame(columns=["rel_path", "resolved_semantics", "review_note"]).to_csv(path_obj, index=False)


def _load_manual_overrides(path_obj):
    _write_override_template(path_obj)
    try:
        df = pd.read_csv(path_obj, keep_default_na=False)
    except Exception as exc:
        raise RuntimeError("无法读取人工审核 override 文件 {}: {}".format(path_obj, exc))

    required = ["rel_path", "resolved_semantics", "review_note"]
    for col in required:
        if col not in df.columns:
            raise RuntimeError("人工审核 override 文件缺少列 '{}'：{}".format(col, path_obj))

    overrides = {}
    for row in df.to_dict(orient="records"):
        rel_path = str(row.get("rel_path", "")).strip()
        if not rel_path:
            continue
        resolved = str(row.get("resolved_semantics", "")).strip().lower()
        if resolved not in VALID_OVERRIDE_SEMANTICS:
            raise RuntimeError(
                "override 语义非法: {} -> {} (允许: {})".format(
                    rel_path, resolved, ", ".join(sorted(VALID_OVERRIDE_SEMANTICS))
                )
            )
        overrides[rel_path] = {
            "resolved_semantics": resolved,
            "review_note": str(row.get("review_note", "")).strip(),
        }
    return overrides


def _resolve_final_semantics(row, override):
    time_axis = str(row.get("time_axis_semantics", "") or "").strip().lower()
    metadata = str(row.get("metadata_semantics", "") or "").strip().lower()
    if override is not None:
        note = str(override.get("review_note", "")).strip()
        return {
            "final_semantics": str(override.get("resolved_semantics", "")).strip().lower(),
            "classification_basis": "manual_override",
            "review_required": False,
            "review_reason": note or "resolved by manual override",
        }

    if time_axis in ("daily", "monthly", "annual"):
        if metadata and metadata != time_axis:
            return {
                "final_semantics": time_axis,
                "classification_basis": "time_axis_conflict_pending_review",
                "review_required": True,
                "review_reason": "time_axis={} conflicts with metadata={}".format(time_axis, metadata),
            }
        return {
            "final_semantics": time_axis,
            "classification_basis": "time_axis",
            "review_required": False,
            "review_reason": "",
        }

    if time_axis == "single_point":
        if metadata == "climatology":
            return {
                "final_semantics": "climatology",
                "classification_basis": "single_point_long_term_average_metadata",
                "review_required": False,
                "review_reason": "",
            }
        return {
            "final_semantics": "single_point",
            "classification_basis": "single_point_deferred_to_s2",
            "review_required": False,
            "review_reason": "",
        }

    if time_axis in ("irregular", "no_time_var", "error"):
        final_semantics = "other" if time_axis == "irregular" else time_axis
        return {
            "final_semantics": final_semantics,
            "classification_basis": "time_axis",
            "review_required": False,
            "review_reason": "",
        }

    if metadata in VALID_OVERRIDE_SEMANTICS:
        return {
            "final_semantics": metadata,
            "classification_basis": "metadata_hint",
            "review_required": False,
            "review_reason": "",
        }

    return {
        "final_semantics": "other",
        "classification_basis": "fallback_other",
        "review_required": False,
        "review_reason": "",
    }


_LOG_TEE_ENABLED = False


def _enable_script_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    import atexit
    import sys
    from datetime import datetime

    log_path = Path(__file__).resolve().with_name("{}_log.txt".format(Path(__file__).stem))
    if log_path.exists():
        try:
            log_path.unlink()
        except Exception:
            pass
    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("\n===== Run started {} =====\n".format(datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr

    class _TeeStream:
        def __init__(self, stream, log_file):
            self._stream = stream
            self._log_file = log_file

        def write(self, data):
            self._stream.write(data)
            try:
                self._log_file.write(data)
                self._log_file.flush()
            except (ValueError, OSError):
                pass

        def flush(self):
            self._stream.flush()
            try:
                self._log_file.flush()
            except (ValueError, OSError):
                pass

    def _close_log_file():
        if sys.stdout is not orig_stdout:
            sys.stdout = orig_stdout
        if sys.stderr is not orig_stderr:
            sys.stderr = orig_stderr
        try:
            log_fp.close()
        except (ValueError, OSError):
            pass

    sys.stdout = _TeeStream(sys.stdout, log_fp)
    sys.stderr = _TeeStream(sys.stderr, log_fp)
    atexit.register(_close_log_file)
    _LOG_TEE_ENABLED = True


def main():
    args = _parse_args()
    dataset_filters = _normalize_dataset_filters(args.datasets)

    _enable_script_logging()
    root_dir = Path(ROOT_DIR).resolve()
    if not root_dir.is_dir():
        print(f"错误：根目录不存在: {root_dir}", file=sys.stderr)
        sys.exit(1)

    ensure_stage1_alias_parity()
    overrides_path = root_dir / REVIEW_OVERRIDES_CSV
    review_queue_path = root_dir / REVIEW_QUEUE_CSV
    overrides = _load_manual_overrides(overrides_path)

    nc_files = []
    for p in root_dir.rglob("*.nc"):
        try:
            rel = p.relative_to(root_dir)
            if rel.parts and rel.parts[0] in ("scripts_basin_test","merged_qc_output", "output", "output_resolution_organized"):
                continue
            if not _match_dataset_filter(rel.parts, dataset_filters):
                continue
        except ValueError:
            continue
        nc_files.append(str(p))

    print(f"根目录: {root_dir}")
    if dataset_filters:
        print("数据集过滤: {}".format(", ".join(dataset_filters)))
    else:
        print("数据集过滤: (全部数据集)")
    print(f"找到 {len(nc_files)} 个 .nc 文件，开始检测时间分辨率并与路径分类比对...")

    if not nc_files:
        print("没有找到 .nc 文件，退出。")
        return
    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(detect_frequency_for_nc, fp): fp for fp in nc_files}
        with tqdm(total=len(nc_files), desc="检测时间分辨率", unit="文件") as pbar:
            for fut in as_completed(futures):
                record = fut.result()
                pbar.update(1)
                filepath = str(record.get("path", ""))
                path_resolution = get_resolution_from_path(filepath, root_dir)
                rel_path = Path(filepath).relative_to(root_dir) if filepath.startswith(str(root_dir)) else filepath
                rel_path = str(rel_path)
                override = overrides.get(rel_path)
                resolution_result = _resolve_final_semantics(record, override)
                final_semantics = resolution_result["final_semantics"]
                consistent = is_consistent(path_resolution, final_semantics)
                results.append({
                    "path": filepath,
                    "rel_path": rel_path,
                    "path_resolution": path_resolution or "(none)",
                    "path_semantics": _path_semantics(path_resolution),
                    "raw_detected_frequency": record.get("raw_detected_frequency", ""),
                    "detected_frequency": record.get("detected_frequency", ""),
                    "time_axis_semantics": record.get("time_axis_semantics", ""),
                    "metadata_semantics": record.get("metadata_semantics", ""),
                    "final_semantics": final_semantics,
                    "temporal_semantics": final_semantics,
                    "classification_basis": resolution_result["classification_basis"],
                    "review_required": bool(resolution_result["review_required"]),
                    "review_reason": resolution_result["review_reason"],
                    "single_point_interpretation": record.get("single_point_interpretation", ""),
                    "is_qc_path": bool(_path_is_qc(rel_path)),
                    "consistent": consistent,
                })
    elapsed = time.time() - t0
    print(f"\n扫描完成，耗时: {elapsed:.1f} 秒 ({elapsed/60:.1f} 分钟)")

    df = pd.DataFrame(results).sort_values(["rel_path", "path"]).reset_index(drop=True)

    out_path = root_dir / OUT_CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"\n判断结果已写入: {out_path}")

    review_queue = df[(df["is_qc_path"]) & (df["review_required"])].copy()
    review_queue.to_csv(review_queue_path, index=False)
    print(f"人工审核队列已写入: {review_queue_path} (共 {len(review_queue)} 条)")

    inconsistent = df[~df["consistent"]]
    n_inconsistent = len(inconsistent)

    if n_inconsistent > 0:
        print("\n" + "=" * 80)
        print("以下文件的最终语义与路径目录不一致（仅提示，不再直接阻断主流程）：")
        print("=" * 80)
        for _, row in inconsistent.iterrows():
            print(
                "  路径分类: {}  |  final_semantics: {}  |  basis: {}".format(
                    row["path_resolution"],
                    row["final_semantics"],
                    row["classification_basis"],
                )
            )
            if row.get("single_point_interpretation"):
                print(f"    single_point 解释: {row['single_point_interpretation']}")
            print(f"    文件: {row['rel_path']}")
        print("=" * 80)
        print(f"共 {n_inconsistent} 个文件不一致。")
    else:
        print("\n所有文件的时间分辨率与路径分类一致。")

    single_point_rows = df[df["detected_frequency"] == "single_point"]
    if len(single_point_rows) > 0:
        print("\n" + "=" * 80)
        print("single_point 文件的元数据解释（是否长时间历史平均等）：")
        print("=" * 80)
        for _, row in single_point_rows.iterrows():
            interp = row.get("single_point_interpretation") or ""
            print(f"  {row['rel_path']}")
            print(f"    -> {interp}")
        print("=" * 80)

    print("\n=== 路径分类统计 ===")
    print(df["path_resolution"].value_counts())
    print("\n=== 检测频率统计 ===")
    print(df["detected_frequency"].value_counts())
    print("\n=== 时间轴语义统计 ===")
    print(df["time_axis_semantics"].value_counts())
    print("\n=== 元数据语义统计 ===")
    print(df["metadata_semantics"].replace("", "(empty)").value_counts())
    if "temporal_semantics" in df.columns:
        print("\n=== 时间语义统计 ===")
        print(df["temporal_semantics"].value_counts())
    if "single_point_interpretation" in df.columns and df["single_point_interpretation"].str.len().gt(0).any():
        interp_counts = df[df["single_point_interpretation"].str.len() > 0]["single_point_interpretation"].value_counts()
        print("\n=== single_point 解释统计 ===")
        print(interp_counts)
    if len(review_queue) > 0:
        print("\n" + "=" * 80)
        print("存在需要人工审核的 QC 文件，主线应在处理 review queue 后继续。")
        print("人工 override 文件: {}".format(overrides_path))
        print("=" * 80)
        for _, row in review_queue.iterrows():
            print(
                "  {} | time_axis={} | metadata={} | 建议={} | reason={}".format(
                    row["rel_path"],
                    row["time_axis_semantics"],
                    row["metadata_semantics"] or "(empty)",
                    row["final_semantics"],
                    row["review_reason"],
                )
            )
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
