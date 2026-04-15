#!/usr/bin/env python3
"""
s4：站点流域匹配脚本（并行版）

流程：
  1. 读取 s3_collected_stations.csv（列：path, source, lat, lon, resolution）
  2. 以行号（0 起）作为 station_id
  3. 将站点按经度排序后分块，多进程并行追溯上游流域
     （按经度分块使同一 worker 内的站点集中在同一 pfaf 区，减少重复 I/O）
  4. 汇总结果，输出 basin CSV 和可选 GPKG，供 s5_basin_merge.py 使用

输入：
  scripts_basin_test/output/s3_collected_stations.csv（s3 输出）

输出：
  scripts_basin_test/output/s4_upstream_basins.csv   ← s5_basin_merge 的默认输入
  scripts_basin_test/output/s4_upstream_basins.gpkg  ← 可选几何文件

环境变量：
  OUTPUT_R_ROOT  — 覆盖 Output_r 根目录（跨机器迁移时使用）
  MERIT_DIR      — MERIT Hydro 数据集根目录
                   默认为 Output_r/../../MERIT_Hydro_v07_Basins_v01_bugfix1
                   （即与 sediment_wzx_1111 同级的目录）
"""

import logging
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
from tqdm import tqdm

# ── 路径设置 ────────────────────────────────────────────────────────────────
from pipeline_paths import get_output_r_root, S3_COLLECTED_CSV, S4_UPSTREAM_CSV, S4_UPSTREAM_GPKG, S4_LOCAL_GPKG, S4_REPORTED_AREA_CHECK_CSV


SCRIPT_DIR    = Path(__file__).resolve().parent
OUTPUT_R_ROOT = get_output_r_root(SCRIPT_DIR)   # Output_r，支持 OUTPUT_R_ROOT 环境变量覆盖

# MERIT Hydro 数据目录：优先环境变量，默认为 sediment_wzx_1111 同级目录
MERIT_DIR  = Path(os.environ.get(
    "MERIT_DIR",
    str(OUTPUT_R_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1")
))
S3_CSV     = OUTPUT_R_ROOT / S3_COLLECTED_CSV
OUT_DIR    = (OUTPUT_R_ROOT / S4_UPSTREAM_CSV).parent
OUT_CSV    = OUTPUT_R_ROOT / S4_UPSTREAM_CSV
OUT_GPKG   = OUTPUT_R_ROOT / S4_UPSTREAM_GPKG
OUT_REPORTED_AREA_CSV = OUTPUT_R_ROOT / S4_REPORTED_AREA_CHECK_CSV
OUT_LOCAL_GPKG = OUTPUT_R_ROOT / S4_LOCAL_GPKG 

LOG_LEVEL  = "INFO"
PARTIAL_CSV = OUT_CSV.with_suffix(".partial.csv")
BASIN_TRACER_DIR = SCRIPT_DIR


def _env_bool(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _env_int(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    return int(value)


SAVE_GPKG = _env_bool("S4_SAVE_GPKG", True)
RESUME    = _env_bool("S4_RESUME", True)
N_WORKERS = _env_int("S4_N_WORKERS", 24)
BATCH_SIZE = _env_int("S4_BATCH_SIZE", 50)  # 每个任务处理的站点数（小 batch 让 tracer 及时释放）
MAX_TASKS_PER_CHILD = _env_int("S4_MAXTASKSPERCHILD", 10)
CSV_COLUMNS = [
    "station_id",
    "lon",
    "lat",
    "basin_id",
    "basin_area",
    "match_quality",
    "reported_area",  
    "area_error",
    "uparea_merit",
    "pfaf_code",
    "method",
    "n_upstream_reaches",
]
CSV_COLUMNS_WITH_GEOM = CSV_COLUMNS + ["geometry_wkt", "geometry_local_wkt"]


# ── worker 函数（必须在模块顶层，才能被 multiprocessing pickle）────────────
def _get_memory_info():
    """获取当前进程及所有子进程的内存使用信息（MB）。"""
    proc = psutil.Process(os.getpid())
    main_rss = proc.memory_info().rss / (1024 * 1024)
    children = proc.children(recursive=True)
    children_rss = sum(c.memory_info().rss for c in children) / (1024 * 1024)
    total = main_rss + children_rss
    return main_rss, children_rss, total


# 进程间共享计数器，用于精确追踪每个站点的进度
_shared_counter = None


def _init_worker(counter):
    """worker 初始化函数，设置共享计数器。"""
    global _shared_counter
    _shared_counter = counter

def _trace_chunk(args):
    """单个 worker：为一批站点追溯流域，返回 result dict 列表。

    args = (merit_dir_str, basin_tracer_dir_str, chunk)
    chunk: list of (station_id, lon, lat, reported_area)
    """
    import gc

    merit_dir_str, basin_tracer_dir_str, chunk = args

    import warnings
    warnings.filterwarnings("ignore", message=".*geographic CRS.*")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s | %(levelname)s | worker | %(message)s",
    )

    if basin_tracer_dir_str not in sys.path:
        sys.path.insert(0, basin_tracer_dir_str)
    from basin_tracer import UpstreamBasinTracer  # noqa: E402

    tracer = UpstreamBasinTracer(merit_dir_str)
    results = []

    for station_id, lon, lat, reported_area in chunk:
        basin_result = tracer.get_upstream_basin(
            lon, lat, reported_area=reported_area
        )
        results.append(
            {
                "station_id": station_id,
                "lon": lon,
                "lat": lat,
                "reported_area": reported_area,
                "basin_id": basin_result["basin_id"],
                "basin_area": basin_result["basin_area"],
                "match_quality": basin_result["match_quality"],
                "area_error": basin_result["area_error"],
                "uparea_merit": basin_result["uparea_merit"],
                "pfaf_code": basin_result["pfaf_code"],
                "method": basin_result["method"],
                "n_upstream_reaches": basin_result["n_upstream_reaches"],
                "geometry": basin_result["geometry"],
                "geometry_local": basin_result["geometry_local"],
            }
        )
        if _shared_counter is not None:
            with _shared_counter.get_lock():
                _shared_counter.value += 1

    del tracer
    gc.collect()
    return results

def _chunk_to_partial_df(chunk_results, include_geometry):
    rows = []
    for row in chunk_results:
        out_row = {
            "station_id": row["station_id"],
            "lon": row["lon"],
            "lat": row["lat"],
            "basin_id": row["basin_id"],
            "basin_area": row["basin_area"],
            "match_quality": row["match_quality"],
            "reported_area":    row.get("reported_area"),
            "area_error": row["area_error"],
            "uparea_merit": row["uparea_merit"],
            "pfaf_code": row["pfaf_code"],
            "method": row["method"],
            "n_upstream_reaches": row["n_upstream_reaches"],
        }
        if include_geometry:
            geometry = row.get("geometry")
            out_row["geometry_wkt"] = geometry.wkt if geometry is not None else ""
            geometry_local = row.get("geometry_local")                                          # ← 新增
            out_row["geometry_local_wkt"] = geometry_local.wkt if geometry_local is not None else ""  # ← 新增
        rows.append(out_row)
    columns = CSV_COLUMNS_WITH_GEOM if include_geometry else CSV_COLUMNS
    return pd.DataFrame(rows, columns=columns)


def _drop_geometry_export_columns(df):
    return df.drop(columns=["geometry_wkt", "geometry_local_wkt"], errors="ignore")


def _write_gpkg_from_wkt(result_df, wkt_column, out_path, label, logger):
    import geopandas as gpd

    if wkt_column not in result_df.columns:
        raise ValueError(f"{wkt_column} not found in partial CSV")

    base_df = _drop_geometry_export_columns(result_df)
    wkt_values = result_df[wkt_column].where(
        result_df[wkt_column].notna() & result_df[wkt_column].ne(""),
        None,
    )
    geometry = gpd.GeoSeries.from_wkt(wkt_values, crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(base_df.copy(), geometry=geometry, crs="EPSG:4326")

    started_at = time.perf_counter()
    gdf.to_file(out_path, driver="GPKG", engine="pyogrio")
    logger.info("Saved %s -> %s (%.1fs)", label, out_path, time.perf_counter() - started_at)


def main():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info(
        "s4 config | workers=%d | batch_size=%d | resume=%s | save_gpkg=%s | maxtasksperchild=%d",
        N_WORKERS, BATCH_SIZE, RESUME, SAVE_GPKG, MAX_TASKS_PER_CHILD,
    )

    # ── 1. 检查路径 ──────────────────────────────────────────────────────────
    if not S3_CSV.is_file():
        logger.error("s3 CSV not found: %s", S3_CSV)
        return 1
    if not MERIT_DIR.is_dir():
        logger.error("MERIT dir not found: %s", MERIT_DIR)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── 2. 读取 s3 站点 ──────────────────────────────────────────────────────
    stations = pd.read_csv(S3_CSV)
    stations = stations.reset_index(drop=True)
    stations.insert(0, "station_id", stations.index)
    stations = stations.dropna(subset=["lon", "lat"]).copy()

    if not RESUME and PARTIAL_CSV.exists():
        PARTIAL_CSV.unlink()
        logger.info("Removed stale partial CSV: %s", PARTIAL_CSV)

    completed_station_ids = set()
    if RESUME and PARTIAL_CSV.is_file():
        partial_df = pd.read_csv(PARTIAL_CSV, usecols=["station_id"])
        completed_station_ids = set(partial_df["station_id"].dropna().astype(int).tolist())
        logger.info("Resume mode: found %d completed stations in %s", len(completed_station_ids), PARTIAL_CSV)
        stations = stations[~stations["station_id"].isin(completed_station_ids)].copy()

    n_pending = len(stations)
    n_total = len(pd.read_csv(S3_CSV).dropna(subset=["lon", "lat"]))
    logger.info("Loaded %d stations (%d pending)", n_total, n_pending)
    if n_pending == 0:
        logger.info("No pending stations to process")

    # ── 3. 按经度排序后分块（同 worker 内站点集中在相近 pfaf 区，提升缓存命中率）
    stations_sorted = stations.sort_values("lon").reset_index(drop=True)
    has_reported_area = "reported_area" in stations_sorted.columns
    reported_areas = (
        stations_sorted["reported_area"].where(stations_sorted["reported_area"].notna(), None).tolist()
        if has_reported_area else [None] * len(stations_sorted)
    )
    tuples = list(zip(
        stations_sorted["station_id"].astype(int),
        stations_sorted["lon"].astype(float),
        stations_sorted["lat"].astype(float),
        reported_areas,
    ))

    chunk_size = BATCH_SIZE
    chunks = [tuples[i: i + chunk_size] for i in range(0, len(tuples), chunk_size)]
    actual_workers = min(N_WORKERS, len(chunks))
    logger.info("Splitting into %d batches (size=%d) for %d workers", len(chunks), chunk_size, actual_workers)

    # ── 4. 并行追溯 ──────────────────────────────────────────────────────────
    args_list = [(str(MERIT_DIR), str(BASIN_TRACER_DIR), chunk) for chunk in chunks]

    # 共享计数器：跨进程追踪已完成的站点数
    counter = mp.Value("i", 0)

    pbar = tqdm(total=n_total, desc="追溯流域", unit="站点",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]")
    pbar.n = len(completed_station_ids)
    pbar.refresh()

    # 内存监控间隔（秒）
    MEM_LOG_INTERVAL = 30
    last_mem_log = time.time()
    peak_total_mb = 0.0

    with counter.get_lock():
        counter.value = len(completed_station_ids)

    if args_list:
        with mp.Pool(processes=actual_workers,
                     initializer=_init_worker, initargs=(counter,),
                     maxtasksperchild=MAX_TASKS_PER_CHILD) as pool:
            async_results = pool.imap_unordered(_trace_chunk, args_list)
            chunks_done = 0
            while chunks_done < len(args_list):
                try:
                    chunk_results = async_results.__next__()
                except StopIteration:
                    break

                chunk_df = _chunk_to_partial_df(chunk_results, include_geometry=SAVE_GPKG)
                chunk_df.to_csv(
                    PARTIAL_CSV,
                    mode="a",
                    index=False,
                    header=not PARTIAL_CSV.exists(),
                )

                chunks_done += 1

                with counter.get_lock():
                    done_count = counter.value
                pbar.n = done_count
                pbar.refresh()

                now = time.time()
                if now - last_mem_log >= MEM_LOG_INTERVAL or chunks_done == len(args_list):
                    main_mb, children_mb, total_mb = _get_memory_info()
                    peak_total_mb = max(peak_total_mb, total_mb)
                    logger.info(
                        "内存监控 | 主进程: %.1f MB | 子进程合计: %.1f MB | "
                        "总计: %.1f MB | 峰值: %.1f MB | 进度: %d/%d 站点",
                        main_mb, children_mb, total_mb, peak_total_mb, done_count, n_total,
                    )
                    last_mem_log = now

    pbar.n = len(completed_station_ids) + n_pending
    pbar.refresh()
    pbar.close()

    # 最终内存报告
    main_mb, children_mb, total_mb = _get_memory_info()
    peak_total_mb = max(peak_total_mb, total_mb)
    logger.info("All %d stations processed", len(completed_station_ids) + n_pending)
    logger.info("最终内存: %.1f MB | 运行峰值: %.1f MB", total_mb, peak_total_mb)

    # ── 5. 输出 CSV ──────────────────────────────────────────────────────────
    if not PARTIAL_CSV.is_file():
        logger.error("Partial CSV not found: %s", PARTIAL_CSV)
        return 1

    result_df = pd.read_csv(PARTIAL_CSV)
    result_df = result_df.sort_values("station_id").drop_duplicates(subset=["station_id"], keep="last")
    csv_df = _drop_geometry_export_columns(result_df)
    csv_df.to_csv(OUT_CSV, index=False)
    # ── 5b. 输出 reported_area 检查 CSV ─────────────────────────────────
    if "reported_area" in result_df.columns:
        reported_mask = result_df["reported_area"].notna()
        n_with_area = int(reported_mask.sum())
        if n_with_area > 0:
            check_cols = ["station_id", "lon", "lat", "reported_area",
                        "uparea_merit", "area_error", "match_quality",
                        "basin_id", "pfaf_code", "method"]
            check_df = result_df.loc[reported_mask, [c for c in check_cols if c in result_df.columns]]
            check_df = check_df.sort_values("station_id")
            check_df.to_csv(OUT_REPORTED_AREA_CSV, index=False)
            logger.info("Saved reported_area check CSV (%d stations) -> %s", n_with_area, OUT_REPORTED_AREA_CSV)
        else:
            logger.info("No stations with reported_area; skipping check CSV")
    logger.info("Saved basin CSV -> %s", OUT_CSV)

    # ── 6. 输出 GPKG（可选）─────────────────────────────────────────────────
    if SAVE_GPKG:
        try:
            _write_gpkg_from_wkt(
                result_df=result_df,
                wkt_column="geometry_wkt",
                out_path=OUT_GPKG,
                label="basin GPKG",
                logger=logger,
            )

            # 最小单元集水区 GPKG（新增）
            if "geometry_local_wkt" in result_df.columns:
                _write_gpkg_from_wkt(
                    result_df=result_df,
                    wkt_column="geometry_local_wkt",
                    out_path=OUT_LOCAL_GPKG,
                    label="local catchment GPKG",
                    logger=logger,
                )
        except Exception as e:
            logger.warning("GPKG save failed (skipping): %s", e)

    PARTIAL_CSV.unlink(missing_ok=True)
    logger.info("Removed partial CSV -> %s", PARTIAL_CSV)
    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
