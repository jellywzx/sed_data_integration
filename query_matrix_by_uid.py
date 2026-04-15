#!/usr/bin/env python3
"""
query_matrix_by_uid.py
按 cluster_uid 从不同时间分辨率的 matrix NC 文件中查询并输出结果。

功能：
- 支持 daily / monthly / annual / all
- 支持 Q / SSC / SSL / all
- 输出：
  1) 终端彩色预览
  2) TXT 报告
  3) CSV 时序数据

适用对象：
- s6_basin_matrix_daily.nc
- s6_basin_matrix_monthly.nc
- s6_basin_matrix_annual.nc
"""

# ══════════════════════════════════════════════════════════════════════
# 用户配置区
# ══════════════════════════════════════════════════════════════════════

from pathlib import Path

CLUSTER_UID = "SED000183"   # 支持 "SED000183" / "183" / "000183"
OUTPUT_ROOT = None          # Output_r 根目录；None=自动推导
RESOLUTION  = "all"         # all | daily | monthly | annual
VARIABLE    = "all"         # all | Q | SSC | SSL
PREVIEW_ROWS = 20           # 预览行数；0=显示全部

OUT_CSV = None              # None=自动命名
OUT_TXT = None              # None=自动命名

ENABLE_COLOR = True

# ══════════════════════════════════════════════════════════════════════

import re
import sys
import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(SCRIPT_DIR.parent))

from pipeline_paths import (
    S6_MATRIX_DIR,
    get_output_r_root,
)

# ─────────────────────────────────────────────────────────────────────
# 常量
# ─────────────────────────────────────────────────────────────────────
VAR_UNITS   = {"Q": "m³/s", "SSC": "mg/L", "SSL": "ton/day"}
FLAG_LABELS = {0: "good", 1: "est", 2: "suspect", 3: "bad", 9: "missing"}
MATRIX_FILES = {
    "daily":   "s6_basin_matrix_daily.nc",
    "monthly": "s6_basin_matrix_monthly.nc",
    "annual":  "s6_basin_matrix_annual.nc",
}

# ─────────────────────────────────────────────────────────────────────
# 颜色工具
# ─────────────────────────────────────────────────────────────────────
class _C:
    BCYAN   = "\033[1;36m"
    CYAN    = "\033[36m"
    BYELLOW = "\033[1;33m"
    YELLOW  = "\033[33m"
    GREEN   = "\033[32m"
    RED     = "\033[31m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RESET   = "\033[0m"

def _c(text, code=""):
    if not ENABLE_COLOR or not code:
        return str(text)
    return "{}{}{}".format(code, text, _C.RESET)

def _section(title):
    bar = "═" * 72
    print()
    print(_c(bar, _C.BCYAN))
    print(_c("  " + title, _C.BCYAN))
    print(_c(bar, _C.BCYAN))

def _row(label, value, lw=24):
    lbl = _c("{:<{}}".format(label, lw), _C.YELLOW)
    print("  {} : {}".format(lbl, value))

def _dash(text=""):
    return _c(text if text else "—", _C.DIM)

# ─────────────────────────────────────────────────────────────────────
# 文本与 UID 工具
# ─────────────────────────────────────────────────────────────────────
def _txt(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    s = str(value).strip()
    return "" if s.lower() == "nan" else s

def _txt_arr(arr):
    return [_txt(v) for v in np.asarray(arr, dtype=object).reshape(-1)]

def _normalize_uid(query):
    q = str(query).strip().upper()
    if q.startswith("SED"):
        num = q[3:]
    else:
        num = q
    try:
        return "SED{:06d}".format(int(num))
    except ValueError:
        return q

def _pct_str(n, total):
    if total == 0:
        return _dash()
    pct = 100.0 * n / total
    s = "{:.1f}%".format(pct)
    if pct >= 80:
        return _c(s, _C.GREEN)
    if pct >= 20:
        return _c(s, _C.YELLOW)
    return _c(s, _C.RED)

# ─────────────────────────────────────────────────────────────────────
# 路径
# ─────────────────────────────────────────────────────────────────────
def _paths():
    root = Path(OUTPUT_ROOT).expanduser().resolve() if OUTPUT_ROOT else get_output_r_root(SCRIPT_DIR.parent)
    matrix_dir = root / S6_MATRIX_DIR
    return {
        "root": root,
        "matrix_dir": matrix_dir,
        **{"matrix_" + res: matrix_dir / fname for res, fname in MATRIX_FILES.items()},
    }

# ─────────────────────────────────────────────────────────────────────
# matrix 文件读取
# ─────────────────────────────────────────────────────────────────────
def _find_station_index_and_meta(nc_path, uid):
    with nc4.Dataset(str(nc_path), "r") as ds:
        if "cluster_uid" not in ds.variables:
            return None, None, []

        uids = _txt_arr(ds.variables["cluster_uid"][:])
        if uid not in uids:
            return None, None, uids

        idx = uids.index(uid)

        def _read_scalar(name, cast=float, fill=np.nan):
            if name not in ds.variables:
                return fill
            arr = np.ma.asarray(ds.variables[name][idx])
            try:
                val = arr.filled(fill)
            except Exception:
                val = arr
            try:
                return cast(val)
            except Exception:
                return fill

        meta = {
            "cluster_uid": uid,
            "station_idx": idx,
            "lat": _read_scalar("lat", float, np.nan),
            "lon": _read_scalar("lon", float, np.nan),
            "basin_area": _read_scalar("basin_area", float, np.nan),
            "n_valid_time_steps": _read_scalar("n_valid_time_steps", int, -1),
            "n_sources_in_resolution": _read_scalar("n_sources_in_resolution", int, -1),
        }

        # 如果文件里包含 source_name，可作为 source 索引映射
        if "source_name" in ds.variables:
            meta["source_names"] = _txt_arr(ds.variables["source_name"][:])
        else:
            meta["source_names"] = []

        # 全局属性
        meta["time_type"] = _txt(getattr(ds, "time_type", ""))
        meta["history"] = _txt(getattr(ds, "history", ""))

    return meta, idx, uids


def _read_matrix_timeseries(nc_path, uid, variables):
    with nc4.Dataset(str(nc_path), "r") as ds:
        uids = _txt_arr(ds.variables["cluster_uid"][:])
        if uid not in uids:
            return None, None

        row = uids.index(uid)

        # 时间
        if "time" not in ds.variables:
            return None, None

        time_var = ds.variables["time"]
        units = getattr(time_var, "units", "days since 1970-01-01")
        cal   = getattr(time_var, "calendar", "gregorian")

        try:
            times = nc4.num2date(
                time_var[:], units, calendar=cal,
                only_use_cftime_datetimes=False
            )
        except TypeError:
            times = nc4.num2date(time_var[:], units, calendar=cal)

        data = {"time": pd.to_datetime(list(times))}

        # 主变量与 flag
        for var in variables:
            if var in ds.variables:
                data[var] = np.ma.asarray(ds.variables[var][row, :]).filled(np.nan)
            fvar = "{}_flag".format(var)
            if fvar in ds.variables:
                data[fvar] = np.ma.asarray(ds.variables[fvar][row, :]).filled(9).astype(np.int16)

        # overlap
        if "is_overlap" in ds.variables:
            data["is_overlap"] = np.ma.asarray(ds.variables["is_overlap"][row, :]).filled(0).astype(np.int16)

        # selected source
        if "selected_source_index" in ds.variables:
            src_idx = np.ma.asarray(ds.variables["selected_source_index"][row, :]).filled(-1).astype(np.int32)
            src_names = _txt_arr(ds.variables["source_name"][:]) if "source_name" in ds.variables else []
            data["selected_source_index"] = src_idx
            data["source"] = [
                src_names[int(i)] if 0 <= int(i) < len(src_names) else ""
                for i in src_idx
            ]

        meta = {
            "time_type": _txt(getattr(ds, "time_type", "")),
            "history": _txt(getattr(ds, "history", "")),
            "n_stations": int(len(ds.dimensions["n_stations"])) if "n_stations" in ds.dimensions else -1,
            "n_time": int(len(ds.dimensions["time"])) if "time" in ds.dimensions else -1,
            "n_sources": int(len(ds.dimensions["n_sources"])) if "n_sources" in ds.dimensions else -1,
        }

    return pd.DataFrame(data), meta

# ─────────────────────────────────────────────────────────────────────
# 统计
# ─────────────────────────────────────────────────────────────────────
def _availability(df, variables):
    if df is None or len(df) == 0:
        return None

    cols = [v for v in variables if v in df.columns]
    has_any = df[cols].notna().any(axis=1) if cols else pd.Series(False, index=df.index)
    sub = df[has_any]

    out = {
        "n_total": len(df),
        "n_valid": len(sub),
        "t_min": sub["time"].min() if len(sub) else None,
        "t_max": sub["time"].max() if len(sub) else None,
        "overlap_count": int(df["is_overlap"].sum()) if "is_overlap" in df.columns else 0,
    }

    for var in variables:
        out["{}_valid".format(var)] = int(df[var].notna().sum()) if var in df.columns else 0

    return out

def _fmt_time(t, res):
    if t is None:
        return "—"
    ts = pd.Timestamp(t)
    if res == "monthly":
        return ts.strftime("%Y-%m")
    if res == "annual":
        return ts.strftime("%Y")
    return ts.strftime("%Y-%m-%d")

# ─────────────────────────────────────────────────────────────────────
# 打印
# ─────────────────────────────────────────────────────────────────────
def _print_station_summary(uid, all_meta):
    _section("查询对象信息  ·  {}".format(_c(uid, _C.BCYAN)))

    _row("cluster_uid", _c(uid, _C.BCYAN))

    # 优先从第一个可用分辨率读基础信息
    sample = None
    for _, m in all_meta.items():
        if m:
            sample = m
            break

    if not sample:
        _row("说明", _dash("未找到任何分辨率数据"))
        return

    lat = sample.get("lat", np.nan)
    lon = sample.get("lon", np.nan)
    basin_area = sample.get("basin_area", np.nan)

    if not np.isnan(lat) and not np.isnan(lon):
        coord = "{:.6f}°{} / {:.6f}°{}".format(
            abs(lat), "N" if lat >= 0 else "S",
            abs(lon), "E" if lon >= 0 else "W"
        )
    else:
        coord = _dash()

    _row("坐标 (lat/lon)", coord)
    _row("流域面积", "{:,.1f} km²".format(basin_area) if not np.isnan(basin_area) else _dash())

    avail_res = [k for k, v in all_meta.items() if v]
    _row("可用分辨率", " | ".join(avail_res) if avail_res else _dash())

def _print_resolution_file_info(res, file_path, meta):
    _section("分辨率文件信息  ·  {}".format(res))
    _row("文件", str(file_path))
    if not meta:
        _row("状态", _dash("未找到该 UID"))
        return

    _row("time_type", meta.get("time_type", "") or _dash())
    _row("n_stations", meta.get("n_stations", -1))
    _row("n_time", meta.get("n_time", -1))
    _row("n_sources", meta.get("n_sources", -1))
    _row("n_valid_time_steps", meta.get("n_valid_time_steps", -1))
    _row("n_sources_in_resolution", meta.get("n_sources_in_resolution", -1))

def _print_availability(avail_map, variables):
    _section("数据可用性摘要")

    RW, NW, TW = 12, 10, 28
    VW = 10

    hdr = "{:<{}}{:<{}}{:<{}}".format("分辨率", RW, "有效记录", NW, "时间范围", TW)
    hdr += "".join("{:<{}}".format("{}有效率".format(v), VW) for v in variables)
    print("  " + _c(hdr, _C.BOLD))
    print("  " + _c("─" * (RW + NW + TW + VW * len(variables)), _C.DIM))

    found = False
    for res in ["daily", "monthly", "annual"]:
        a = avail_map.get(res)
        if not a:
            continue
        found = True
        rng = "{} ~ {}".format(_fmt_time(a["t_min"], res), _fmt_time(a["t_max"], res)) if a["t_min"] is not None else _dash()
        line = "{:<{}}{:<{},}{:<{}}".format(res, RW, a["n_valid"], NW, rng, TW)
        for var in variables:
            line += "{:<{}}".format(_pct_str(a.get("{}_valid".format(var), 0), a["n_valid"]), VW)
        print("  " + line)

    if not found:
        print("  " + _dash("（所有分辨率均无数据）"))

    print()
    print("  " + _c("标记说明: 0=good 1=estimated 2=suspect 3=bad 9=missing", _C.DIM))

def _print_timeseries(df, variables, resolution, max_rows):
    if df is None or len(df) == 0:
        return

    cols = [v for v in variables if v in df.columns]
    has_any = df[cols].notna().any(axis=1) if cols else pd.Series(False, index=df.index)
    sub = df[has_any].copy()
    if len(sub) == 0:
        return

    n_total = len(sub)
    if max_rows != 0:
        sub = sub.head(max_rows)

    _section("时序数据  ·  {}  (共 {:,} 行，显示 {:,} 行)".format(
        resolution, n_total, len(sub)
    ))

    TW, VW, FW, SW, OW = 13, 12, 10, 16, 6

    hdr = "{:<{}}".format("时间", TW)
    for var in variables:
        hdr += "{:<{}}".format("{}({})".format(var, VAR_UNITS.get(var, "")), VW)
    for var in variables:
        hdr += "{:<{}}".format("{}_标记".format(var), FW)
    hdr += "{:<{}}".format("来源", SW)
    hdr += "{:<{}}".format("竞争", OW)

    print("  " + _c(hdr, _C.BOLD))
    print("  " + _c("─" * (TW + VW * len(variables) + FW * len(variables) + SW + OW), _C.DIM))

    for _, row in sub.iterrows():
        line = "{:<{}}".format(_fmt_time(row["time"], resolution), TW)

        for var in variables:
            val = row.get(var, np.nan)
            if pd.isna(val):
                line += _c("{:<{}}".format("—", VW), _C.DIM)
            else:
                line += "{:<{}.4g}".format(float(val), VW)

        for var in variables:
            fv = int(row.get("{}_flag".format(var), 9))
            fs = FLAG_LABELS.get(fv, str(fv))
            if fv == 0:
                line += _c("{:<{}}".format(fs, FW), _C.GREEN)
            elif fv in (1, 2):
                line += _c("{:<{}}".format(fs, FW), _C.YELLOW)
            else:
                line += _c("{:<{}}".format(fs, FW), _C.RED)

        src = str(row.get("source", ""))[:SW - 1]
        line += "{:<{}}".format(src, SW)

        ovlp = int(row.get("is_overlap", 0))
        line += _c("{:<{}}".format("是" if ovlp else "否", OW), _C.YELLOW if ovlp else _C.DIM)

        print("  " + line)

    if max_rows != 0 and n_total > max_rows:
        print("  " + _c("  … 还有 {:,} 行（设置 PREVIEW_ROWS=0 显示全部）".format(
            n_total - max_rows
        ), _C.DIM))

def _print_basic_stats(df, variables, resolution):
    if df is None or len(df) == 0:
        return

    _section("统计摘要  ·  {}".format(resolution))

    for var in variables:
        if var not in df.columns:
            continue
        s = pd.to_numeric(df[var], errors="coerce")
        valid = s.notna().sum()
        _row("{}_non_missing".format(var), valid)
        if valid > 0:
            _row("{}_min".format(var), "{:.6g}".format(float(s.min())))
            _row("{}_mean".format(var), "{:.6g}".format(float(s.mean())))
            _row("{}_max".format(var), "{:.6g}".format(float(s.max())))
            _row("{}_negative_count".format(var), int((s < 0).sum()))

# ─────────────────────────────────────────────────────────────────────
# TXT tee
# ─────────────────────────────────────────────────────────────────────
def _strip_ansi(text):
    return re.sub(r"\033\[[0-9;]*m", "", text)

class _Tee:
    def __init__(self, file):
        self._file = file
        self._stdout = sys.stdout
    def write(self, text):
        self._stdout.write(text)
        self._file.write(_strip_ansi(text))
    def flush(self):
        self._stdout.flush()
        self._file.flush()

# ─────────────────────────────────────────────────────────────────────
# 输出文件命名
# ─────────────────────────────────────────────────────────────────────
def _ensure_output_names(uid):
    global OUT_CSV, OUT_TXT
    if OUT_CSV is None:
        OUT_CSV = str(SCRIPT_DIR / "{}_matrix_query.csv".format(uid))
    if OUT_TXT is None:
        OUT_TXT = str(SCRIPT_DIR / "{}_matrix_query.txt".format(uid))

# ─────────────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────────────
def _main():
    if not HAS_NC:
        print("错误：需要安装 netCDF4。请运行: pip install netCDF4")
        return 1

    uid = _normalize_uid(CLUSTER_UID)
    _ensure_output_names(uid)

    paths = _paths()
    matrix_dir = paths["matrix_dir"]

    print(_c("\n正在查询 matrix NC: {}  (from {})".format(
        uid, matrix_dir
    ), _C.BOLD))

    if not matrix_dir.is_dir():
        print(_c("错误：找不到 matrix 目录", _C.RED))
        print("  期望路径: {}".format(matrix_dir))
        return 1

    res_targets = ["daily", "monthly", "annual"] if RESOLUTION == "all" else [RESOLUTION]
    var_targets = list(VAR_UNITS.keys()) if VARIABLE == "all" else [VARIABLE]

    all_meta = {}
    all_dfs = {}
    avail_map = {}

    # 先逐个分辨率查
    for res in res_targets:
        nc_path = paths.get("matrix_" + res)
        if nc_path is None or not nc_path.is_file():
            all_meta[res] = None
            continue

        station_meta, _, all_uids = _find_station_index_and_meta(nc_path, uid)
        if station_meta is None:
            all_meta[res] = None
            continue

        df, file_meta = _read_matrix_timeseries(nc_path, uid, var_targets)
        merged_meta = {}
        merged_meta.update(station_meta or {})
        merged_meta.update(file_meta or {})
        all_meta[res] = merged_meta

        if df is not None and len(df) > 0:
            df = df.copy()
            df.insert(0, "cluster_uid", uid)
            df.insert(1, "resolution", res)
            all_dfs[res] = df
            avail_map[res] = _availability(df, var_targets)

    # 如果所有分辨率都没找到，给出提示
    if not any(v is not None for v in all_meta.values()):
        print(_c("错误：在目标 matrix 文件中找不到 cluster_uid: {}".format(uid), _C.RED))

        # 尝试从任意一个存在的文件里拿些 UID 示例
        sample_uids = []
        for res in ["daily", "monthly", "annual"]:
            p = paths.get("matrix_" + res)
            if p and p.is_file():
                _, _, uids = _find_station_index_and_meta(p, uid="__NOT_EXISTS__")
                if uids:
                    sample_uids = uids[:5]
                    break

        if sample_uids:
            print("  示例 UID: {}".format(", ".join(sample_uids)))
        return 1

    # 打印基础信息
    _print_station_summary(uid, all_meta)

    # 每个分辨率输出文件信息
    for res in res_targets:
        _print_resolution_file_info(res, paths.get("matrix_" + res), all_meta.get(res))

    # 可用性摘要
    _print_availability(avail_map, var_targets)

    # 每个分辨率详细输出
    for res in res_targets:
        df = all_dfs.get(res)
        if df is None or len(df) == 0:
            continue
        _print_basic_stats(df, var_targets, res)
        _print_timeseries(df, var_targets, res, PREVIEW_ROWS)

    # 导出 CSV
    if all_dfs and OUT_CSV:
        frames = []
        for res, df in all_dfs.items():
            tmp = df.copy()
            meta = all_meta.get(res, {})
            tmp["lat"] = meta.get("lat", np.nan)
            tmp["lon"] = meta.get("lon", np.nan)
            tmp["basin_area"] = meta.get("basin_area", np.nan)
            tmp["n_valid_time_steps"] = meta.get("n_valid_time_steps", -1)
            tmp["n_sources_in_resolution"] = meta.get("n_sources_in_resolution", -1)
            frames.append(tmp)

        combined = pd.concat(frames, ignore_index=True)
        out_path = Path(OUT_CSV).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_csv(out_path, index=False)
        print(_c("\n已导出 CSV ({:,} 行): {}".format(len(combined), out_path), _C.GREEN))

    print()
    return 0

def main():
    global OUT_TXT

    uid = _normalize_uid(CLUSTER_UID)
    _ensure_output_names(uid)

    txt_file = None
    if OUT_TXT:
        out_path = Path(OUT_TXT).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        txt_file = open(out_path, "w", encoding="utf-8")
        sys.stdout = _Tee(txt_file)

    try:
        return _main()
    finally:
        if txt_file:
            sys.stdout = sys.stdout._stdout
            txt_file.close()
            print("已保存输出到: {}".format(OUT_TXT))

if __name__ == "__main__":
    sys.exit(main())
