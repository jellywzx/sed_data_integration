#!/usr/bin/env python3
"""
步骤 s3：从 s2 重组目录中扫描 .nc 文件，
读取经纬度与数据源，输出站点列表 CSV。

输入（默认）：
  - {S2_ORGANIZED_DIR}/ 下的 .nc（步骤 s2 输出目录，目录名由 pipeline_paths.S2_ORGANIZED_DIR 指定）
输出（默认）：
  - scripts/output/s3_collected_stations.csv（步骤 s3 输出，来自 pipeline_paths.S3_COLLECTED_CSV；
    列 path, source, lat, lon, resolution, station_name, river_name, source_station_id，
    以及可选的 source_comid/source_reach_code/source_vpu_id/source_rpu_id）
resolution 来自路径第一级目录。供步骤 s4/s5 聚类使用。

当前默认规则：
  - basin 主线默认不收集 climatology；
  - climatology 文件保留在 output_resolution_organized/climatology 下，
    供独立的 climatology NC 导出脚本使用。

根目录说明：
  - 默认根目录由 pipeline_paths.get_output_r_root() 解析；
  - 可通过环境变量 OUTPUT_R_ROOT 覆盖 Output_r 根目录。

用法（在 Output_r 根目录下运行）：
  从 s2 重组目录扫描 .nc：
  python scripts/s3_collect_qc_stations.py [--root .] [--out scripts/output/s3_collected_stations.csv] [-j 32]
"""

import re
import argparse
from pathlib import Path
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
from pipeline_paths import S2_ORGANIZED_DIR, S3_COLLECTED_CSV, RESOLUTION_DIRS, get_output_r_root

try:
    import netCDF4 as nc4
    HAS_NC = True
except ImportError:
    HAS_NC = False


LAT_NAMES = ["lat", "latitude", "Latitude"]
LON_NAMES = ["lon", "longitude", "Longitude"]
FILL = -9999.0
_STATION_NAME_KEYS = ["station_name", "Station_Name", "stationName", "name"]
_RIVER_NAME_KEYS = ["river_name", "River_Name", "riverName", "river"]
_STATION_ID_KEYS = ["station_id", "Station_ID", "stationID", "ID"]
_COMID_KEYS = ["comid", "COMID"]
_REACH_CODE_KEYS = ["reach_code", "REACHCODE", "REACHCO", "reachcode"]
_VPU_ID_KEYS = ["vpu_id", "VPUID", "vpu"]
_RPU_ID_KEYS = ["rpu_id", "RPUID", "rpu"]
# 各数据集中存储上游汇水面积的字段名（NC 变量或全局属性）
# 优先级：先找 NC 变量，再找全局属性；全局属性按列表顺序匹配
_AREA_VAR_NAMES = [
    "upstream_area",   # HYBAM：直接写入 NC 变量
    "drainage_area",
    "basin_area",
    "catchment_area",
]
_AREA_ATTR_NAMES = [
    "Drainage area (km2)",  # ALi_De_Boer 原始列名
    "drainage_area_km2",
    "drainage_area",
    "upstream_area",        # HMA 标量元数据
    "Area",                 # Milliman 原始列名
    "area",
    "basin_area",
    "catchment_area",
]


def _get_scalar(var):
    if var is None:
        return None
    # 先按 masked 处理，再 asarray，否则 np.asarray(masked) 会变成 0
    if np.ma.isMaskedArray(var):
        v = var.flatten()
        if v.size == 0:
            return None
        v = v.flat[0]
        if np.ma.is_masked(v):
            return np.nan
        v = float(v)
    else:
        arr = np.asarray(var).flatten()
        if arr.size == 0:
            return None
        v = float(arr.flat[0])
    if np.isnan(v) or v == FILL or v == -9999:
        return np.nan
    return v

def get_reported_area_from_nc(path):
    """从 NC 文件提取上游汇水面积（km²），失败返回 None。

    检索顺序：
    1. NC 变量（upstream_area / drainage_area / …）
    2. 全局属性（Drainage area (km2) / Area / upstream_area / …）
    3. 全局属性名中含 "area"（兜底）
    """
    if not HAS_NC:
        return None
    try:
        with nc4.Dataset(path, "r") as nc:
            # ── 1. 先找 NC 变量 ──────────────────────────────────────────
            for var_name in _AREA_VAR_NAMES:
                if var_name in nc.variables:
                    val = _get_scalar(nc.variables[var_name][:])
                    if val is not None and not np.isnan(val) and val > 0:
                        return float(val)

            # ── 2. 再找全局属性（按优先列表） ─────────────────────────────
            for attr_name in _AREA_ATTR_NAMES:
                raw = getattr(nc, attr_name, None)
                if raw is not None:
                    try:
                        v = float(raw)
                        if v > 0 and not np.isnan(v):
                            return v
                    except (ValueError, TypeError):
                        pass

            # ── 3. 兜底：搜索所有属性名中含 "area" 的条目 ─────────────────
            for attr_name in nc.ncattrs():
                if "area" in attr_name.lower() and "ratio" not in attr_name.lower():
                    raw = getattr(nc, attr_name, None)
                    if raw is not None:
                        try:
                            v = float(raw)
                            if v > 0 and not np.isnan(v):
                                return v
                        except (ValueError, TypeError):
                            pass
    except Exception:
        pass
    return None


def get_lat_lon_from_nc(path):
    """从 nc 文件读取标量 lat, lon。失败返回 (None, None)。"""
    if not HAS_NC:
        return None, None
    try:
        with nc4.Dataset(path, "r") as nc:
            lat_var = next((x for x in LAT_NAMES if x in nc.variables), None)
            lon_var = next((x for x in LON_NAMES if x in nc.variables), None)
            if lat_var is None or lon_var is None:
                return None, None
            lat = _get_scalar(nc.variables[lat_var][:])
            lon = _get_scalar(nc.variables[lon_var][:])
            if lat is None or lon is None or (np.isnan(lat) or np.isnan(lon)):
                return None, None
            return float(lat), float(lon)
    except Exception:
        return None, None


def get_station_meta_from_nc(path):
    """从 nc 全局属性读取站点级元数据。"""
    if not HAS_NC:
        return {
            "station_name": "",
            "river_name": "",
            "source_station_id": "",
            "source_comid": "",
            "source_reach_code": "",
            "source_vpu_id": "",
            "source_rpu_id": "",
        }
    try:
        with nc4.Dataset(path, "r") as nc:
            def _get_attr(keys):
                for key in keys:
                    val = getattr(nc, key, None)
                    if val is not None and str(val).strip():
                        return str(val).strip()[:256]
                return ""
            return {
                "station_name": _get_attr(_STATION_NAME_KEYS),
                "river_name": _get_attr(_RIVER_NAME_KEYS),
                "source_station_id": _get_attr(_STATION_ID_KEYS),
                "source_comid": _get_attr(_COMID_KEYS),
                "source_reach_code": _get_attr(_REACH_CODE_KEYS),
                "source_vpu_id": _get_attr(_VPU_ID_KEYS),
                "source_rpu_id": _get_attr(_RPU_ID_KEYS),
            }
    except Exception:
        return {
            "station_name": "",
            "river_name": "",
            "source_station_id": "",
            "source_comid": "",
            "source_reach_code": "",
            "source_vpu_id": "",
            "source_rpu_id": "",
        }


def get_resolution_from_path(path, root_dir):
    """从路径第一级目录解析时间分辨率。"""
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if parts:
            res = parts[0].strip().lower()
            if res in RESOLUTION_DIRS:
                return res
            return res
    except Exception:
        pass
    return "unknown"


def get_source_from_path(path, root_dir):
    """从相对路径解析数据源名，如 daily/GloRiSe/SS/qc/xxx.nc -> GloRiSe_SS。"""
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if "qc" in parts:
            idx = parts.index("qc")
            before = parts[:idx]
            if len(before) >= 2:
                source = "_".join(before[1:])
            else:
                source = before[0] if before else "unknown"
        else:
            source = parts[0] if parts else "unknown"
        return re.sub(r"[^\w\-]", "_", source)
    except Exception:
        return "unknown"


def get_source_from_organized_path(path, root_dir):
    """从 s2 重组目录下的文件名解析数据源。文件名格式：{source}_{resolution}_{stem}.nc。"""
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if not parts:
            return "unknown"
        resolution = get_resolution_from_path(path, root_dir)
        stem = Path(parts[-1]).stem
        stem_parts = stem.split("_")
        for i, seg in enumerate(stem_parts):
            if seg == resolution:
                return "_".join(stem_parts[:i]) if i > 0 else (stem_parts[0] if stem_parts else "unknown")
        return stem_parts[0] if stem_parts else "unknown"
    except Exception:
        return "unknown"


def _collect_one_nc(path, root_dir):
    """Worker: 读单个 nc 的 path/source/lat/lon/resolution；source 从 s2 重组文件名解析。
    path 列存储相对于 root_dir（output_resolution_organized/）的相对路径，
    便于跨机器迁移时无需修改 CSV。
    """
    try:
        lat, lon = get_lat_lon_from_nc(path)
        if lat is None or lon is None:
            return None
        station_meta = get_station_meta_from_nc(path)
        source = get_source_from_organized_path(path, root_dir)
        resolution = get_resolution_from_path(path, root_dir)
        reported_area = get_reported_area_from_nc(path)
        # 存相对路径，跨平台可移植
        rel_path = str(Path(path).relative_to(root_dir))
        return {
            "path": rel_path,
            "source": source,
            "lat": lat,
            "lon": lon,
            "resolution": resolution,
            "station_name": station_meta["station_name"],
            "river_name": station_meta["river_name"],
            "source_station_id": station_meta["source_station_id"],
            "source_comid": station_meta["source_comid"],
            "source_reach_code": station_meta["source_reach_code"],
            "source_vpu_id": station_meta["source_vpu_id"],
            "source_rpu_id": station_meta["source_rpu_id"],
            "reported_area": reported_area if reported_area is not None else float("nan"),
        }
    except (ValueError, OSError):
        return None


ORGANIZED_DIR = S2_ORGANIZED_DIR


def collect_qc_nc_stations(root_dir, workers=1, excluded_resolutions=None):
    """收集 root/{S2_ORGANIZED_DIR} 下全部 .nc 文件。"""
    root = Path(root_dir).resolve()
    scan_root = root / ORGANIZED_DIR
    excluded = {str(x).strip().lower() for x in (excluded_resolutions or []) if str(x).strip()}
    paths = []
    for p in scan_root.rglob("*.nc"):
        try:
            rel = p.relative_to(scan_root)
        except ValueError:
            continue
        if not rel.parts or rel.parts[0] not in RESOLUTION_DIRS:
            continue
        if rel.parts[0].strip().lower() in excluded:
            continue
        paths.append(str(p))
    paths = sorted(paths)
    if not paths:
        return pd.DataFrame()
    root_str = str(scan_root)
    if workers <= 1:
        rows = [_collect_one_nc(p, root_str) for p in paths]
    else:
        with Pool(min(workers, len(paths), cpu_count() or 1)) as pool:
            rows = pool.starmap(_collect_one_nc, [(p, root_str) for p in paths])
    rows = [r for r in rows if r is not None]
    return pd.DataFrame(rows)


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
    # 默认根目录为脚本所在目录的上一级（Output_r），便于在 scripts 下直接运行
    _default_root = str(get_output_r_root(Path(__file__).resolve().parent))
    ap = argparse.ArgumentParser(description="步骤 s3：收集 nc 站点 (path, source, lat, lon, resolution) 输出 s3_collected_stations.csv")
    ap.add_argument("--root", default=_default_root, help="根目录，默认脚本所在目录的上一级 (Output_r)")
    ap.add_argument("--out", default=S3_COLLECTED_CSV, help="步骤 s3 输出 CSV 路径")
    ap.add_argument("--workers", "-j", type=int, default=0,
                    help="Parallel workers; 0=auto (cpu_count-1, max 32)")
    ap.add_argument(
        "--exclude-resolutions",
        default="climatology",
        help="逗号分隔的分辨率目录名，默认排除 climatology，使其不进入 basin 主线",
    )
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. Install with: pip install netCDF4")
        return

    root_dir = Path(args.root).resolve()
    workers = args.workers if args.workers > 0 else min(32, max(1, (cpu_count() or 2) - 1))
    excluded_resolutions = [
        x.strip().lower()
        for x in str(args.exclude_resolutions).split(",")
        if x.strip()
    ]

    print(
        "Collecting .nc stations from {} (workers={}, excluded={}) ...".format(
            ORGANIZED_DIR,
            workers,
            ",".join(excluded_resolutions) if excluded_resolutions else "(none)",
        )
    )
    stations = collect_qc_nc_stations(root_dir, workers=workers, excluded_resolutions=excluded_resolutions)
    if len(stations) == 0:
        print("No organized .nc files found with valid lat/lon.")
        return
    print("Found {} organized .nc files.".format(len(stations)))

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = root_dir / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stations.to_csv(out_path, index=False)
    print("Saved to {}.".format(out_path))
    if "climatology" in excluded_resolutions:
        print("Note: climatology files were excluded from the basin mainline collection.")
        print("Next: run s4_basin_trace_watch.py with input {}".format(out_path))
        print("Climatology can be exported separately with s6_export_climatology_to_nc.py")
    else:
        print("Next: run s4_basin_trace_watch.py with input {}".format(out_path))


if __name__ == "__main__":
    main()
