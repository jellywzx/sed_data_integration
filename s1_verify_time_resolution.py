#!/usr/bin/env python3
"""
对默认输入根目录（优先 Output_r_attr_fixed，其次 Output_r）下所有数据集中的 .nc 文件应用 time_resolution 分类逻辑，
并检查是否与路径中的分类（daily / monthly / annually_climatology）一致；
不一致时输出报告。

对 single_point 会进一步查看元数据，识别是否为长时间历史平均（如 1970-2021）。

依赖: numpy, pandas, xarray

用法（在 Output_r 根目录下运行）：
    python scripts/s1_verify_time_resolution.py

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
from pipeline_paths import S1_VERIFY_CSV

# 根目录默认优先使用 Output_r_attr_fixed；可通过 OUTPUT_R_ROOT 覆盖
SCRIPT_DIR = Path(__file__).resolve().parent
# 结果 CSV 路径（相对 ROOT_DIR）；列说明见本文件顶部 docstring；步骤 s1 对应输出
OUT_CSV = S1_VERIFY_CSV
WORKERS = None

# 从 01_verify_reorganize 加载 time_resolution
_verify_dir = SCRIPT_DIR / "01_verify_reorganize"
if str(_verify_dir) not in sys.path:
    sys.path.insert(0, str(_verify_dir))
from time_resolution import (
    classify_frequency,
    get_preferred_output_root,
    infer_temporal_semantics,
    sync_temporal_resolution_attrs,
)


ROOT_DIR = get_preferred_output_root(SCRIPT_DIR)


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


def check_single_point_temporal_resolution(filepath, single_time):
    """
    对 single_point 的 nc 检查源数据属性 temporal_resolution、temporal_span。
    支持的属性名：Temporal_Resolution / temporal_resolution / time_resolution / resolution；
                Temporal_Span / temporal_span，或 time_coverage_start + time_coverage_end。
    - 若 temporal_resolution 为 daily 且与单时间点一致，则视为 daily，
      返回 ("daily", "single_point_upgraded_to_daily_by_temporal_resolution")。
    - 若 temporal_resolution 为 climatology/climatological 且（无 span 或 span 包含单点），
      则视为 annual（气候态），返回 ("annual", "single_point_upgraded_to_annual_by_temporal_resolution_climatology")。
    否则返回 None。
    """
    try:
        with xr.open_dataset(filepath) as ds:
            # 优先全局属性，再查时间变量属性（支持 Temporal_Resolution / temporal_resolution 等命名）
            attrs = dict(ds.attrs)
            time_candidates = ["time", "Time", "t", "datetime", "date"]
            for c in time_candidates:
                if c in ds.variables:
                    for k, v in ds.variables[c].attrs.items():
                        if k not in attrs and v is not None:
                            attrs[k] = v
                    break

            # 兼容 Temporal_Resolution（首字母大写）与 temporal_resolution 等
            res_raw = None
            for key in ("Temporal_Resolution", "temporal_resolution", "time_resolution", "resolution"):
                if key in attrs and attrs[key] is not None:
                    res_raw = attrs[key]
                    break
            if res_raw is None:
                return None
            res = str(res_raw).strip().lower()

            def span_contains_single_time():
                span_raw = attrs.get("Temporal_Span") or attrs.get("temporal_span")
                if span_raw is None and attrs.get("time_coverage_start") and attrs.get("time_coverage_end"):
                    span_raw = "{} {}".format(attrs["time_coverage_start"], attrs["time_coverage_end"])
                if span_raw is None:
                    return True
                parsed = _parse_span_to_dates(str(span_raw))
                if not parsed:
                    return True
                start, end = parsed
                try:
                    st = pd.Timestamp(single_time)
                    start = pd.Timestamp(start)
                    end = pd.Timestamp(end)
                    return start <= st <= end
                except Exception:
                    return False

            # daily：与单时间点一致
            if "daily" in res or res == "day" or re.match(r"^1\s*day", res):
                if not span_contains_single_time():
                    return None
                return ("daily", "single_point_upgraded_to_daily_by_temporal_resolution")

            # climatology：气候态/多年平均，视为 annual
            if "climatology" in res or "climatological" in res:
                if not span_contains_single_time():
                    return None
                return ("annual", "single_point_upgraded_to_annual_by_temporal_resolution_climatology")

            return None
    except Exception:
        return None


def interpret_single_point_metadata(filepath):
    """当时间为单点时，从元数据识别是否为长时间历史平均，或由 temporal_resolution 判定为 daily。"""
    try:
        with xr.open_dataset(filepath) as ds:
            time_candidates = ["time", "Time", "t", "datetime", "date"]
            time_var = None
            for c in time_candidates:
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
            # 先检查源数据 temporal_resolution / temporal_span，若为 daily 且与单点一致则视为 daily
            override = check_single_point_temporal_resolution(filepath, single_time)
            if override is not None:
                return override  # ("daily", "single_point_upgraded_to_daily_by_temporal_resolution")

            texts = []

            for k, v in ds.attrs.items():
                if isinstance(v, str):
                    texts.append(v)
            for k, v in t.attrs.items():
                if isinstance(v, str):
                    texts.append(v)
                elif k == "bounds" and v in ds.variables:
                    bnds_var = ds[v]
                    try:
                        bnds_vals = bnds_var.values
                        if bnds_vals is not None and bnds_vals.size >= 2:
                            bnds_flat = np.atleast_1d(bnds_vals).flatten()
                            if len(bnds_flat) >= 2:
                                t_start = pd.to_datetime(bnds_flat[0])
                                t_end = pd.to_datetime(bnds_flat[-1])
                                if hasattr(t_start, "year") and hasattr(t_end, "year"):
                                    texts.append(f"{t_start.year}-{t_end.year}")
                    except Exception:
                        pass

            t_start = ds.attrs.get("time_coverage_start") or ds.attrs.get("start_date")
            t_end = ds.attrs.get("time_coverage_end") or ds.attrs.get("end_date")
            if t_start and t_end:
                try:
                    y1 = re.search(r"(\d{4})", str(t_start))
                    y2 = re.search(r"(\d{4})", str(t_end))
                    if y1 and y2:
                        texts.append(f"{y1.group(1)}-{y2.group(1)}")
                except Exception:
                    pass

            for text in texts:
                low = text.lower()
                if any(kw in low for kw in ("climatology", "climatological", "average", "mean", "long-term", "long term", "historical")):
                    yr = _year_range_in_text(text)
                    if yr:
                        return f"long_term_average_{yr[0]}_{yr[1]}"
                    if hasattr(single_time, "year"):
                        return f"single_point_likely_climatology_year_{single_time.year}"

            for text in texts:
                yr = _year_range_in_text(text)
                if yr:
                    return f"long_term_average_{yr[0]}_{yr[1]}"

            return f"single_point_time_{str(single_time)}"
    except Exception as e:
        return f"single_point_interpret_error_{e}"


def detect_frequency_for_nc(filepath):
    """对单个 nc 检测时间频率。

    返回：
      (filepath, raw_detected_freq, detected_freq, single_point_interpretation)
    """
    try:
        with xr.open_dataset(filepath) as ds:
            time_candidates = ["time", "Time", "t", "datetime", "date"]
            time_var = None
            for cand in time_candidates:
                if cand in ds.variables:
                    time_var = cand
                    break
            if time_var is None:
                return filepath, "no_time_var", "no_time_var", None
            time_values = pd.to_datetime(ds[time_var].values)
        raw_freq = classify_frequency(time_values)
        freq = raw_freq
        single_interp = None
        if raw_freq == "single_point":
            single_interp = interpret_single_point_metadata(filepath)
            if isinstance(single_interp, (list, tuple)) and len(single_interp) == 2:
                freq, single_interp = single_interp[0], single_interp[1]
        return filepath, raw_freq, freq, single_interp
    except Exception as e:
        err = f"error: {str(e)}"
        return filepath, err, err, None


def _get_s1_attr_sync_target(raw_detected_frequency, detected_frequency, temporal_semantics, single_point_interpretation):
    """仅对 s1 明确做出的元数据级重判回写镜像 qc 属性。"""
    raw_freq = str(raw_detected_frequency or "").strip().lower()
    detected = str(detected_frequency or "").strip().lower()
    semantics = str(temporal_semantics or "").strip().lower()
    interp = str(single_point_interpretation or "").strip().lower()

    if raw_freq != "single_point":
        return ""

    if detected and detected != raw_freq:
        return detected

    if semantics == "climatology" and (
        "long_term_average" in interp
        or "climatology" in interp
        or "historical" in interp
        or "average" in interp
        or "mean" in interp
    ):
        return "climatology"

    return ""


def _sync_s1_one(item):
    filepath, target_resolution, reason = item
    try:
        result = sync_temporal_resolution_attrs(
            filepath,
            target_resolution=target_resolution,
            stage="s1",
            reason=reason,
        )
        return filepath, bool(result.get("changed")), ""
    except Exception as exc:
        return filepath, False, str(exc)


PATH_TO_EXPECTED = {
    "daily": ["daily"],
    "monthly": ["monthly"],
    "annually_climatology": ["annual", "quarterly"],
}


def is_consistent(path_resolution, detected_freq):
    if path_resolution is None or path_resolution not in PATH_TO_EXPECTED:
        return True
    allowed = PATH_TO_EXPECTED[path_resolution]
    if detected_freq.startswith("error:") or detected_freq == "no_time_var":
        return False
    return detected_freq in allowed


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

    class _TeeStream:
        def __init__(self, stream, log_file):
            self._stream = stream
            self._log_file = log_file

        def write(self, data):
            self._stream.write(data)
            self._log_file.write(data)
            self._log_file.flush()

        def flush(self):
            self._stream.flush()
            self._log_file.flush()

    sys.stdout = _TeeStream(sys.stdout, log_fp)
    sys.stderr = _TeeStream(sys.stderr, log_fp)
    atexit.register(log_fp.close)
    _LOG_TEE_ENABLED = True


def main():
    _enable_script_logging()
    root_dir = Path(ROOT_DIR).resolve()
    if not root_dir.is_dir():
        print(f"错误：根目录不存在: {root_dir}", file=sys.stderr)
        sys.exit(1)

    nc_files = []
    for p in root_dir.rglob("*.nc"):
        try:
            rel = p.relative_to(root_dir)
            if rel.parts and rel.parts[0] in ("merged_qc_output", "output", "output_resolution_organized"):
                continue
        except ValueError:
            pass
        nc_files.append(str(p))

    print(f"根目录: {root_dir}")
    print(f"找到 {len(nc_files)} 个 .nc 文件，开始检测时间分辨率并与路径分类比对...")

    if not nc_files:
        print("没有找到 .nc 文件，退出。")
        return

    results = []
    with ProcessPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(detect_frequency_for_nc, fp): fp for fp in nc_files}
        for fut in as_completed(futures):
            filepath, raw_detected_freq, detected_freq, single_point_interpretation = fut.result()
            path_resolution = get_resolution_from_path(filepath, root_dir)
            consistent = is_consistent(path_resolution, detected_freq)
            temporal_semantics = infer_temporal_semantics(
                detected_freq, single_point_interpretation
            )
            rel_path = Path(filepath).relative_to(root_dir) if filepath.startswith(str(root_dir)) else filepath
            attr_sync_target = _get_s1_attr_sync_target(
                raw_detected_freq,
                detected_freq,
                temporal_semantics,
                single_point_interpretation,
            )
            results.append({
                "path": filepath,
                "rel_path": str(rel_path),
                "path_resolution": path_resolution or "(none)",
                "raw_detected_frequency": raw_detected_freq,
                "detected_frequency": detected_freq,
                "temporal_semantics": temporal_semantics,
                "single_point_interpretation": single_point_interpretation if single_point_interpretation else "",
                "attr_sync_target": attr_sync_target,
                "attr_sync_applied": False,
                "attr_sync_error": "",
                "consistent": consistent,
            })

    df = pd.DataFrame(results)

    sync_tasks = []
    for _, row in df.iterrows():
        target = str(row.get("attr_sync_target", "") or "").strip()
        if not target:
            continue
        reason = "single_point metadata reinterpretation"
        sync_tasks.append((str(row["path"]), target, reason))

    enable_attr_sync = root_dir.name == "Output_r_attr_fixed"
    if sync_tasks and enable_attr_sync:
        print(f"\n开始同步 s1 重判后的 temporal_resolution 属性: {len(sync_tasks)} 个文件")
        sync_changed = set()
        sync_errors = {}
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(_sync_s1_one, item): item[0] for item in sync_tasks}
            for fut in as_completed(futures):
                filepath, changed, err = fut.result()
                if changed:
                    sync_changed.add(filepath)
                if err:
                    sync_errors[filepath] = err

        if sync_changed:
            df.loc[df["path"].isin(sync_changed), "attr_sync_applied"] = True
        if sync_errors:
            for filepath, err in sync_errors.items():
                df.loc[df["path"] == filepath, "attr_sync_error"] = err
            print(f"s1 属性同步失败 {len(sync_errors)} 个文件")
        else:
            print("s1 属性同步完成。")
    elif sync_tasks:
        print(
            "\n检测到 {} 个 s1 重判文件，但当前根目录不是 Output_r_attr_fixed，"
            "默认跳过 temporal_resolution 属性回写。".format(len(sync_tasks))
        )

    out_path = root_dir / OUT_CSV
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"\n判断结果已写入: {out_path}")

    inconsistent = df[~df["consistent"]]
    n_inconsistent = len(inconsistent)

    if n_inconsistent > 0:
        print("\n" + "=" * 80)
        print("以下文件的时间分辨率与路径分类不一致，请检查：")
        print("=" * 80)
        for _, row in inconsistent.iterrows():
            print(f"  路径分类: {row['path_resolution']}  |  检测结果: {row['detected_frequency']}")
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
    if "temporal_semantics" in df.columns:
        print("\n=== 时间语义统计 ===")
        print(df["temporal_semantics"].value_counts())
    if "single_point_interpretation" in df.columns and df["single_point_interpretation"].str.len().gt(0).any():
        interp_counts = df[df["single_point_interpretation"].str.len() > 0]["single_point_interpretation"].value_counts()
        print("\n=== single_point 解释统计 ===")
        print(interp_counts)

    sys.exit(1 if n_inconsistent > 0 else 0)


if __name__ == "__main__":
    main()
