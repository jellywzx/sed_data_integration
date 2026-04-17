#!/usr/bin/env python3
"""
test_basin_matching_all_reported_area.py

对 s3 CSV 中所有 reported_area 有值且坐标完整的站点进行全量流域匹配测试，
比较“面积辅助匹配”和“纯坐标最近邻”两种策略的差异。

输出：
  - 控制台打印各数据集的匹配质量统计
  - CSV 文件：每行一个站点，包含人工复核所需的关键对比字段
"""

import logging
import math
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent

# =============================================================================
# ★ 用户配置区：按需修改以下变量
# =============================================================================

# s3 CSV 的路径
S3_CSV = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/s3_collected_stations.csv"

# MERIT Hydro 数据集根目录（含 pfaf_level_01/ 子目录）
MERIT_DIR = "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1"

# 结果 CSV 输出路径
OUT_CSV = str(SCRIPT_DIR / "output" / "test_basin_matching_all_reported_area.csv")

# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _run_find_best_reach(tracer, lon, lat, reported_area):
    """分别用面积辅助和纯坐标两种方式调用 find_best_reach，各返回一个结果 dict。"""
    res_area = tracer.find_best_reach(lon, lat, reported_area=reported_area)
    res_dist = tracer.find_best_reach(lon, lat, reported_area=None)
    return res_area or {}, res_dist or {}


def _safe_float(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _rel_err(a, b):
    """相对误差：(a-b)/b。b 缺失或为 0 时返回 None。"""
    a_f = _safe_float(a)
    b_f = _safe_float(b)
    if a_f is None or b_f in (None, 0):
        return None
    return (a_f - b_f) / b_f


def _abs_rel_err(a, b):
    v = _rel_err(a, b)
    return abs(v) if v is not None else None


def _extract_station_identifiers(row):
    """尽量保留原始表中的标识列，便于人工回查。"""
    candidates = [
        "source", "station_name", "station_id", "site_no", "site_id",
        "gauge_id", "gauge_name", "id", "fid", "object_id"
    ]
    out = {}
    for col in candidates:
        if col in row.index:
            out[col] = row.get(col)
    return out


def _build_result_row(row, res_area, res_dist):
    reported_area = _safe_float(row.get("reported_area"))
    area_up = _safe_float(res_area.get("uparea"))
    dist_up = _safe_float(res_dist.get("uparea"))
    area_dist = _safe_float(res_area.get("distance"))
    dist_dist = _safe_float(res_dist.get("distance"))

    out = {
        **_extract_station_identifiers(row),
        "lon": _safe_float(row.get("lon")),
        "lat": _safe_float(row.get("lat")),
        "reported_area_km2": reported_area,
        # 面积辅助匹配
        "area_COMID": res_area.get("COMID"),
        "area_uparea_km2": area_up,
        "area_match_quality": res_area.get("match_quality"),
        "area_error": _safe_float(res_area.get("area_error")),
        "area_abs_error": abs(_safe_float(res_area.get("area_error"))) if _safe_float(res_area.get("area_error")) is not None else None,
        "area_distance_deg": area_dist,
        # 纯距离匹配
        "dist_COMID": res_dist.get("COMID"),
        "dist_uparea_km2": dist_up,
        "dist_match_quality": res_dist.get("match_quality"),
        "dist_distance_deg": dist_dist,
        # 方便人工核对的对比字段
        "same_reach": res_area.get("COMID") == res_dist.get("COMID"),
        "same_uparea": area_up == dist_up,
        "uparea_diff_km2": (area_up - dist_up) if area_up is not None and dist_up is not None else None,
        "uparea_diff_pct_vs_dist": _rel_err(area_up, dist_up),
        "reported_vs_area_uparea_rel_err": _rel_err(area_up, reported_area),
        "reported_vs_area_uparea_abs_rel_err": _abs_rel_err(area_up, reported_area),
        "reported_vs_dist_uparea_rel_err": _rel_err(dist_up, reported_area),
        "reported_vs_dist_uparea_abs_rel_err": _abs_rel_err(dist_up, reported_area),
        "distance_diff_deg": (area_dist - dist_dist) if area_dist is not None and dist_dist is not None else None,
    }

    # 保留原始记录中的其余常见辅助列，便于人工检查
    extra_cols = [
        "river_name", "country", "continent", "agency", "source_url",
        "reported_area_unit", "drainage_area", "basin_area"
    ]
    for col in extra_cols:
        if col in row.index and col not in out:
            out[col] = row.get(col)

    return out


def _print_summary(result_df):
    """控制台输出各数据集的匹配质量统计。"""
    print("\n" + "=" * 72)
    print("Basin Matching Test — Full Scan of All Rows with reported_area")
    print("=" * 72)

    for src, grp in result_df.groupby("source", dropna=False):
        if grp.empty:
            continue

        n = len(grp)
        print(f"\n[{src}]  tested={n}")

        area_qual = grp["area_match_quality"].fillna("<NA>").value_counts(dropna=False)
        print("  area-assisted  match_quality:")
        for q, c in area_qual.items():
            print(f"    {str(q):<24} {c:>6}  ({100*c/n:.1f}%)")

        dist_qual = grp["dist_match_quality"].fillna("<NA>").value_counts(dropna=False)
        print("  distance-only  match_quality:")
        for q, c in dist_qual.items():
            print(f"    {str(q):<24} {c:>6}  ({100*c/n:.1f}%)")

        same = int(grp["same_reach"].fillna(False).sum())
        print(f"  same reach selected: {same}/{n}  ({100*same/n:.1f}%)")

        sub_area = grp[grp["reported_vs_area_uparea_abs_rel_err"].notna()]
        if not sub_area.empty:
            med = sub_area["reported_vs_area_uparea_abs_rel_err"].median()
            mn = sub_area["reported_vs_area_uparea_abs_rel_err"].mean()
            mx = sub_area["reported_vs_area_uparea_abs_rel_err"].max()
            print(
                "  reported vs area-assisted uparea abs rel err: "
                f"median={med:.2%}  mean={mn:.2%}  max={mx:.2%}"
            )

        sub_dist = grp[grp["reported_vs_dist_uparea_abs_rel_err"].notna()]
        if not sub_dist.empty:
            med = sub_dist["reported_vs_dist_uparea_abs_rel_err"].median()
            mn = sub_dist["reported_vs_dist_uparea_abs_rel_err"].mean()
            mx = sub_dist["reported_vs_dist_uparea_abs_rel_err"].max()
            print(
                "  reported vs distance-only uparea abs rel err: "
                f"median={med:.2%}  mean={mn:.2%}  max={mx:.2%}"
            )

    print("\n" + "=" * 72)


def run_test(s3_csv, merit_dir, out_csv):
    # 读取 s3 CSV
    df = pd.read_csv(s3_csv)
    logger.info("Loaded S3 CSV: %d rows", len(df))

    required_cols = ["lon", "lat", "reported_area"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error("Missing required columns: %s", ", ".join(missing_cols))
        sys.exit(1)

    # 仅保留 reported_area 有值、经纬度完整的记录
    df_target = df.dropna(subset=["lon", "lat", "reported_area"]).copy()
    if df_target.empty:
        logger.error("No rows with non-null lon/lat/reported_area were found.")
        sys.exit(1)

    logger.info(
        "Target rows for full scan: %d (all rows with non-null reported_area)",
        len(df_target),
    )

    if "source" in df_target.columns:
        src_stats = (
            df_target.groupby("source", dropna=False)
            .size()
            .sort_values(ascending=False)
        )
        logger.info("Source breakdown (top 20):")
        for src, cnt in src_stats.head(20).items():
            logger.info("  %-30s %8d", str(src), int(cnt))

    # 加载 UpstreamBasinTracer
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    import warnings
    warnings.filterwarnings("ignore", message=".*geographic CRS.*")

    from basin_tracer import UpstreamBasinTracer  # noqa: E402
    logger.info("Loading MERIT Hydro from: %s", merit_dir)
    tracer = UpstreamBasinTracer(str(merit_dir))

    # 全量匹配
    all_rows = []
    failed_rows = []
    total = len(df_target)

    for i, (_, row) in enumerate(df_target.iterrows(), 1):
        lon = _safe_float(row.get("lon"))
        lat = _safe_float(row.get("lat"))
        ra = _safe_float(row.get("reported_area"))

        try:
            res_area, res_dist = _run_find_best_reach(tracer, lon, lat, ra)
            all_rows.append(_build_result_row(row, res_area, res_dist))
        except Exception as exc:
            failed = _extract_station_identifiers(row)
            failed.update({
                "lon": lon,
                "lat": lat,
                "reported_area_km2": ra,
                "error": str(exc),
            })
            failed_rows.append(failed)
            logger.warning("Row %d/%d failed: %s", i, total, exc)

        if i % 100 == 0 or i == total:
            logger.info(
                "Progress: %d/%d processed, success=%d, failed=%d",
                i, total, len(all_rows), len(failed_rows)
            )

    if not all_rows:
        logger.error("No matching results generated.")
        sys.exit(1)

    result_df = pd.DataFrame(all_rows)

    # 排序，便于人工检查
    sort_cols = [c for c in [
        "source",
        "reported_vs_area_uparea_abs_rel_err",
        "reported_vs_dist_uparea_abs_rel_err",
        "station_name"
    ] if c in result_df.columns]
    ascending = [True, False, False, True][:len(sort_cols)]
    if sort_cols:
        result_df = result_df.sort_values(sort_cols, ascending=ascending, na_position="last")

    _print_summary(result_df)

    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(out_path, index=False)
    logger.info("Results saved to %s  (%d rows)", out_path, len(result_df))

    if failed_rows:
        fail_path = out_path.with_name(out_path.stem + "_failed.csv")
        pd.DataFrame(failed_rows).to_csv(fail_path, index=False)
        logger.info("Failed rows saved to %s  (%d rows)", fail_path, len(failed_rows))


def main():
    run_test(
        s3_csv=S3_CSV,
        merit_dir=MERIT_DIR,
        out_csv=OUT_CSV,
    )


if __name__ == "__main__":
    main()

