#!/usr/bin/env python3
"""
步骤 s6（合并前）：按 (cluster_id, resolution) 检测同位置同分辨率下多源的时间重叠，输出供人工选择。

逻辑：
  - 读取 s4_clustered_stations.csv（含 path, source, lat, lon, cluster_id, resolution）；
  - 按 (cluster_id, resolution) 分组：同一 cluster、同一分辨率下若同一日期有多源则视为重叠；
  - 仅输出这些重叠的 (cluster_id, resolution, date) 的多源候选行。

输入（默认）：
  - scripts/output/s4_clustered_stations.csv（步骤 s4 输出，来自 pipeline_paths.S4_CLUSTERED_CSV）
输出（步骤 6 对应文件）：
  - scripts/output/s6_merge_qc_nc_report.csv：各 cluster 合并摘要（供 s8 使用）
  - scripts/output/s6_overlap_for_manual_choice.csv：重叠候选，人工对每个 (cluster_id, resolution, date) 保留一条后另存为 scripts/output/s7_overlap_resolved.csv（供 s7/s8 使用）

用法（在 Output_r 根目录下运行）：
  python scripts/s6_merge_timeseries_by_cluster.py
  python scripts/s6_merge_timeseries_by_cluster.py --clustered scripts/output/s4_clustered_stations.csv --out-dir scripts/output [-j 8]
"""
import argparse
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd
from pipeline_paths import S4_CLUSTERED_CSV, S6_OVERLAP_CSV, S6_REPORT_CSV, PIPELINE_OUTPUT_DIR, get_output_r_root

try:
    import netCDF4 as nc4
    from netCDF4 import num2date
    HAS_NC = True
except ImportError:
    HAS_NC = False

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(it, **kw):
        return it

# 与 3_merge_qc_nc_by_location 一致的变量名与常量
TIME_NAMES = ["time", "Time", "t", "sample"]
Q_NAMES = ["Q", "discharge", "Discharge_m3_s", "Discharge"]
SSC_NAMES = ["SSC", "ssc", "TSS_mg_L", "TSS"]
SSL_NAMES = ["SSL", "sediment_load", "Sediment_load"]
FILL = -9999.0

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)  # Output_r（支持 OUTPUT_R_ROOT 覆盖）
DEFAULT_CLUSTERED = PROJECT_ROOT / S4_CLUSTERED_CSV
DEFAULT_OUT_DIR = PROJECT_ROOT / PIPELINE_OUTPUT_DIR


def _get_var(ds, names, default=np.nan):
    for n in names:
        if n in ds.variables:
            return np.asarray(ds.variables[n][:]).flatten()
    return np.full(1, default)


def load_nc_series(path):
    """
    从 nc 读取时间序列，返回 DataFrame：date, Q, SSC, SSL。
    """
    if not HAS_NC:
        return None
    try:
        with nc4.Dataset(path, "r") as nc:
            time_var = next((x for x in TIME_NAMES if x in nc.variables), None)
            if time_var is None:
                return None
            t = nc.variables[time_var]
            t_vals = np.asarray(t[:]).flatten()
            units = getattr(t, "units", "days since 1970-01-01")
            calendar = getattr(t, "calendar", "gregorian")
            try:
                times = num2date(t_vals, units, calendar=calendar)
            except TypeError:
                try:
                    times = num2date(t_vals, units, calendar=calendar, only_use_cftime_datetimes=False)
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
                    elif hasattr(tt, "year") and hasattr(tt, "month") and hasattr(tt, "day"):
                        dates.append(pd.Timestamp(tt.year, tt.month, tt.day).date())
                    else:
                        dates.append(pd.Timestamp(str(tt)).date())

            q = _get_var(nc, Q_NAMES)
            ssc = _get_var(nc, SSC_NAMES)
            ssl = _get_var(nc, SSL_NAMES)
            n = len(dates)
            if n == 0:
                return None

            def pad(a, size, fill=np.nan):
                a = np.asarray(a).flatten()
                if len(a) >= size:
                    return a[:size]
                return np.concatenate([a, np.full(size - len(a), fill)])

            q = pad(q, n)
            ssc = pad(ssc, n)
            ssl = pad(ssl, n)
            df = pd.DataFrame({
                "date": dates,
                "Q": q,
                "SSC": ssc,
                "SSL": ssl,
            })
            df["date"] = pd.to_datetime(df["date"]).dt.date
            for col in ["Q", "SSC", "SSL"]:
                df.loc[df[col] == FILL, col] = np.nan
                df.loc[df[col] == -9999, col] = np.nan
            return df
    except Exception:
        return None


def collect_overlap_rows_only(cid, resolution, recs, rep_lat, rep_lon):
    """
    只收集“同一 (cluster_id, resolution) 内时间重叠”的日期的多源候选行。
    recs: list of (source, path)
    返回 list of dict: cluster_id, lat, lon, resolution, date, source, Q, SSC, SSL, path
    """
    records_with_df = []
    for source, path in recs:
        df = load_nc_series(path)
        if df is not None and len(df) > 0:
            records_with_df.append((source, path, df))

    if not records_with_df:
        return []

    date_to_sources = defaultdict(list)
    for source, path, df in records_with_df:
        for r in df.itertuples(index=False):
            d = r.date
            q = getattr(r, "Q", np.nan)
            ssc = getattr(r, "SSC", np.nan)
            ssl = getattr(r, "SSL", np.nan)
            if pd.isna(q) and pd.isna(ssc) and pd.isna(ssl):
                continue
            date_to_sources[d].append((source, path, q, ssc, ssl))

    overlap_rows = []
    for d, candidates in date_to_sources.items():
        if len(candidates) <= 1:
            continue
        for source, path, q, ssc, ssl in candidates:
            overlap_rows.append({
                "cluster_id": cid,
                "lat": rep_lat,
                "lon": rep_lon,
                "resolution": resolution,
                "date": d,
                "source": source,
                "Q": q,
                "SSC": ssc,
                "SSL": ssl,
                "path": path,
            })
    return overlap_rows


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
    # 强制刷新输出，避免无 TTY 时看不到打印
    import sys
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except Exception:
            pass
    ap = argparse.ArgumentParser(
        description="Step s6: detect time-overlap only, output for manual choice; full merge in next step"
    )
    ap.add_argument("--clustered", "-c", default=str(DEFAULT_CLUSTERED),
                    help="步骤 s4 输出 s4_clustered_stations.csv 路径")
    ap.add_argument("--out-dir", "-o", default=str(DEFAULT_OUT_DIR),
                    help="Output directory for report and overlap CSV")
    ap.add_argument("--workers", "-j", type=int, default=32,
                    help="Parallel workers (1=sequential)")
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4", flush=True)
        return

    clustered_path = Path(args.clustered)
    if not clustered_path.is_file():
        print("Error: not found: {}".format(clustered_path), flush=True)
        return

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading {} ...".format(clustered_path), flush=True)
    stations = pd.read_csv(clustered_path)
    for col in ["path", "source", "lat", "lon", "cluster_id"]:
        if col not in stations.columns:
            print("Error: CSV must have columns: path, source, lat, lon, cluster_id", flush=True)
            return
    if "resolution" not in stations.columns:
        stations["resolution"] = "unknown"
    n_clusters = int(stations["cluster_id"].nunique())
    print("Loaded {} stations, {} clusters.".format(len(stations), n_clusters), flush=True)

    # 按 (cluster_id, resolution) 分组
    by_cluster_resolution = defaultdict(list)
    g = stations.groupby(["cluster_id", "resolution"], sort=True)
    for (cid, res), grp in g:
        cid = int(cid)
        res = str(res)
        for _, row in grp.iterrows():
            by_cluster_resolution[(cid, res)].append((str(row["source"]), str(row["path"])))

    cluster_ids = sorted({k[0] for k in by_cluster_resolution.keys()})

    report_rows = []
    for cid in cluster_ids:
        members = stations[stations["cluster_id"] == cid]
        rep = members.iloc[0]
        resolutions = sorted(members["resolution"].unique())
        report_rows.append({
            "cluster_id": cid,
            "lat": rep["lat"],
            "lon": rep["lon"],
            "n_stations": len(members),
            "resolutions": ",".join(resolutions),
            "sources": ",".join(sorted(members["source"].unique())),
            "paths": "; ".join(members["path"].tolist()[:3]) + (" ..." if len(members) > 3 else ""),
        })
    report_path = out_dir / Path(S6_REPORT_CSV).name
    pd.DataFrame(report_rows).to_csv(report_path, index=False)
    print("Saved {}.".format(report_path), flush=True)

    # 仅检测时间重叠，按 (cluster_id, resolution) 收集候选行
    tasks = []
    for (cid, res), recs in by_cluster_resolution.items():
        if not recs:
            continue
        members = stations[(stations["cluster_id"] == cid)]
        if len(members) == 0:
            continue
        rep_lat = float(members.iloc[0]["lat"])
        rep_lon = float(members.iloc[0]["lon"])
        tasks.append((int(cid), str(res), recs, rep_lat, rep_lon))

    if args.workers <= 1:
        all_overlap = []
        for t in tqdm(tasks, desc="Detect overlaps", unit="(cid,res)"):
            all_overlap.extend(collect_overlap_rows_only(*t))
    else:
        from multiprocessing import Pool, cpu_count
        workers = min(args.workers, len(tasks), cpu_count() or 1)
        with Pool(workers) as pool:
            chunk_results = list(tqdm(
                pool.starmap(collect_overlap_rows_only, tasks),
                total=len(tasks), desc="Detect overlaps", unit="(cid,res)",
            ))
        all_overlap = []
        for chunk in chunk_results:
            all_overlap.extend(chunk)

    overlap_path = out_dir / Path(S6_OVERLAP_CSV).name
    overlap_columns = ["cluster_id", "lat", "lon", "resolution", "date", "source", "Q", "SSC", "SSL", "path"]
    if all_overlap:
        out_df = pd.DataFrame(all_overlap)
        out_df = out_df.sort_values(["cluster_id", "resolution", "date", "source"]).reset_index(drop=True)
        out_df.to_csv(overlap_path, index=False)
        n_overlap_keys = out_df.groupby(["cluster_id", "resolution", "date"]).ngroups
        print("Saved {} ({} rows, {} (cluster_id, resolution, date) with overlap).".format(
            overlap_path, len(out_df), n_overlap_keys), flush=True)
    else:
        pd.DataFrame(columns=overlap_columns).to_csv(overlap_path, index=False)
        print("Saved {} (no overlapping dates found).".format(overlap_path), flush=True)

    print("\n人工选择说明：", flush=True)
    print("  1. 打开 {} ，表中每一行是「某 cluster 某分辨率 某日期」的一个数据源候选。".format(overlap_path.name), flush=True)
    print("  2. 对每个 (cluster_id, resolution, date) 只保留你信任的一条，另存为 s7_overlap_resolved.csv（需含 resolution 列）。", flush=True)
    print("  3. 下一步运行 s7_merge_overlap_by_cluster.py 可自动合并；再运行 s8_merge_qc_csv_to_one_nc.py 生成最终 NC。", flush=True)


if __name__ == "__main__":
    main()
