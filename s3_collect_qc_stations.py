#!/usr/bin/env python3
"""
步骤 s3：从 s2 重组目录中扫描 .nc 文件，
读取经纬度与数据源，输出站点列表 CSV。

输入（默认）：
  - {S2_ORGANIZED_DIR}/ 下的 .nc（步骤 s2 输出目录，目录名由 pipeline_paths.S2_ORGANIZED_DIR 指定）
输出（默认）：
  - scripts/output/s3_collected_stations.csv（步骤 s3 输出，来自 pipeline_paths.S3_COLLECTED_CSV；
    列 path, source, lat, lon, resolution）
resolution 来自路径第一级：daily, monthly, annually_climatology。供步骤 s4 聚类使用。

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
from pipeline_paths import S2_ORGANIZED_DIR, S3_COLLECTED_CSV, get_output_r_root

try:
    import netCDF4 as nc4
    HAS_NC = True
except ImportError:
    HAS_NC = False


LAT_NAMES = ["lat", "latitude", "Latitude"]
LON_NAMES = ["lon", "longitude", "Longitude"]
FILL = -9999.0


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


def get_resolution_from_path(path, root_dir):
    """从路径第一级目录解析时间分辨率：daily, monthly, annually_climatology, other。"""
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if parts:
            res = parts[0].strip().lower()
            if res in ("daily", "monthly"):
                return res
            if "annually" in res or "climatology" in res:
                return "annually_climatology"
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
        source = get_source_from_organized_path(path, root_dir)
        resolution = get_resolution_from_path(path, root_dir)
        # 存相对路径，跨平台可移植
        rel_path = str(Path(path).relative_to(root_dir))
        return {"path": rel_path, "source": source, "lat": lat, "lon": lon, "resolution": resolution}
    except (ValueError, OSError):
        return None


ORGANIZED_DIR = S2_ORGANIZED_DIR


def collect_qc_nc_stations(root_dir, workers=1):
    """收集 root/{S2_ORGANIZED_DIR} 下全部 .nc 文件。"""
    root = Path(root_dir).resolve()
    scan_root = root / ORGANIZED_DIR
    paths = []
    for p in scan_root.rglob("*.nc"):
        paths.append(str(p))
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
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. Install with: pip install netCDF4")
        return

    root_dir = Path(args.root).resolve()
    workers = args.workers if args.workers > 0 else min(32, max(1, (cpu_count() or 2) - 1))

    print("Collecting .nc stations from {} (workers={}) ...".format(ORGANIZED_DIR, workers))
    stations = collect_qc_nc_stations(root_dir, workers=workers)
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
    print("Next: run s4_cluster_qc_stations.py with input {}".format(out_path))


if __name__ == "__main__":
    main()
