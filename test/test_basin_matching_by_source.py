#!/usr/bin/env python3
"""
test_basin_matching_by_source.py

自动从 s3_collected_stations.csv 中找出含 reported_area 的数据集，
抽样进行流域匹配测试，比较"面积辅助匹配"与"纯坐标最近邻"两种策略的差异。

注意：
  RiverSed、GSED、Dethier 等不参与 basin matching 的源不允许携带
  reported_area 进入本测试；若检测到这些源仍有非空 reported_area，
  脚本会直接报错。

输出：
  - 控制台打印各数据集的匹配质量统计
  - CSV 文件：每行一个站点，含两种匹配结果的对比列
"""

import logging
import sys
from pathlib import Path

import pandas as pd

from basin_policy import should_skip_basin_matching

SCRIPT_DIR = Path(__file__).resolve().parent

# =============================================================================
# ★ 用户配置区：按需修改以下变量
# =============================================================================

# s3_collected_stations.csv 的路径
S3_CSV = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/s3_collected_stations.csv"

# MERIT Hydro 数据集根目录（含 pfaf_level_01/ 子目录）
MERIT_DIR = "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1"

# 每个数据集最多测试的站点数
MAX_PER_SOURCE = 20

# 最多测试几个数据集（按 reported_area 站点数从多到少排序，取前 N 个）
MAX_SOURCES = 5

# reported_area 覆盖率阈值：至少有这个比例的站点有面积数据，才纳入测试（0~1）
MIN_AREA_COVERAGE = 0.1

# 结果 CSV 输出路径
OUT_CSV = str(SCRIPT_DIR / "output" / "test_basin_matching_by_source.csv")

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
    return res_area, res_dist


def _sample_group(grp, max_n, random_state=42):
    """优先采样有 reported_area 的站点，不足时补充无 reported_area 的站点。"""
    grp_with = grp[grp["reported_area"].notna()]
    grp_no   = grp[grp["reported_area"].isna()]
    n_with = min(max_n, len(grp_with))
    n_no   = min(max(0, max_n - n_with), len(grp_no))
    parts = []
    if n_with > 0:
        parts.append(grp_with.sample(n=n_with, random_state=random_state))
    if n_no > 0:
        parts.append(grp_no.sample(n=n_no, random_state=random_state))
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _select_sources(df, min_coverage, max_sources):
    """从 CSV 中自动筛选有足够 reported_area 覆盖率的数据集，按覆盖站点数降序取前 N 个。"""
    stats = []
    for src, grp in df.dropna(subset=["lon", "lat"]).groupby("source"):
        n_total = len(grp)
        n_area  = grp["reported_area"].notna().sum()
        coverage = n_area / n_total if n_total else 0
        if coverage >= min_coverage:
            stats.append({"source": src, "n_total": n_total,
                          "n_area": n_area, "coverage": coverage})

    if not stats:
        return []

    stats.sort(key=lambda x: x["n_area"], reverse=True)
    selected = stats[:max_sources]

    logger.info("\n─── 自动选中的测试数据集（reported_area 覆盖率 ≥ %.0f%%）───", min_coverage * 100)
    for s in selected:
        logger.info("  %-30s  %d/%d 站点有面积数据 (%.0f%%)",
                    s["source"], s["n_area"], s["n_total"], s["coverage"] * 100)
    return [s["source"] for s in selected]


def _print_summary(result_df, sources):
    """控制台输出各数据集的匹配质量统计。"""
    print("\n" + "=" * 72)
    print("Basin Matching Test — Summary by Dataset")
    print("=" * 72)

    for src in sources:
        grp = result_df[result_df["dataset"] == src]
        if grp.empty:
            print(f"\n[{src}]  (no results)")
            continue

        n = len(grp)
        n_area_input = grp["reported_area_km2"].notna().sum()
        print(f"\n[{src}]  tested={n}  with_reported_area={n_area_input}")

        area_qual = grp["area_match_quality"].value_counts()
        print("  area-assisted  match_quality:")
        for q, c in area_qual.items():
            print(f"    {q:<24} {c:>4}  ({100*c/n:.0f}%)")

        dist_qual = grp["dist_match_quality"].value_counts()
        print("  distance-only  match_quality:")
        for q, c in dist_qual.items():
            print(f"    {q:<24} {c:>4}  ({100*c/n:.0f}%)")

        same = grp["same_reach"].sum()
        print(f"  same reach selected: {same}/{n}  ({100*same/n:.0f}%)")

        sub = grp[
            grp["reported_area_km2"].notna()
            & grp["area_error"].notna()
            & (grp["area_match_quality"] != "distance_only")
        ]
        if not sub.empty:
            med = sub["area_error"].median()
            mn  = sub["area_error"].mean()
            mx  = sub["area_error"].abs().max()
            print(f"  area_error (relative, n={len(sub)}):  "
                  f"median={med:.2%}  mean={mn:.2%}  |max|={mx:.2%}")

    print("\n" + "=" * 72)


def run_test(s3_csv, merit_dir, max_per_source, max_sources, min_coverage, out_csv):
    # ── 读取 s3 CSV ────────────────────────────────────────────────────────
    df = pd.read_csv(s3_csv)
    logger.info("Loaded S3 CSV: %d rows", len(df))

    if "reported_area" not in df.columns:
        logger.error(
            "'reported_area' column not found. "
            "Please re-run s3_collect_qc_stations.py first."
        )
        sys.exit(1)

    skip_mask = df["source"].map(should_skip_basin_matching)
    skip_with_area = df.loc[skip_mask & df["reported_area"].notna()]
    if not skip_with_area.empty:
        logger.error(
            "No-basin-match source rows must not carry reported_area in the basin mainline. "
            "Please re-run s3_collect_qc_stations.py after applying the source skip policy."
        )
        sys.exit(1)

    # ── 自动选出有 reported_area 的数据集 ─────────────────────────────────
    sources = _select_sources(df, min_coverage, max_sources)
    if not sources:
        logger.error(
            "No source has reported_area coverage >= %.0f%%. "
            "Try lowering MIN_AREA_COVERAGE.", min_coverage * 100
        )
        sys.exit(1)

    df_target = df[df["source"].isin(sources)].dropna(subset=["lon", "lat"]).copy()

    # ── 加载 UpstreamBasinTracer ───────────────────────────────────────────
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))

    import warnings
    warnings.filterwarnings("ignore", message=".*geographic CRS.*")

    from basin_tracer import UpstreamBasinTracer  # noqa: E402
    logger.info("Loading MERIT Hydro from: %s", merit_dir)
    tracer = UpstreamBasinTracer(str(merit_dir))

    # ── 逐数据集匹配 ──────────────────────────────────────────────────────
    all_rows = []
    for src in sources:
        grp = df_target[df_target["source"] == src]
        sample = _sample_group(grp, max_per_source)
        n_with = sample["reported_area"].notna().sum()
        n_no   = sample["reported_area"].isna().sum()
        logger.info("[%s] Testing %d stations  (with_area=%d, no_area=%d)",
                    src, len(sample), n_with, n_no)

        for i, (_, row) in enumerate(sample.iterrows(), 1):
            lon  = float(row["lon"])
            lat  = float(row["lat"])
            ra   = row["reported_area"]
            ra_f = float(ra) if pd.notna(ra) else None

            try:
                res_area, res_dist = _run_find_best_reach(tracer, lon, lat, ra_f)
            except Exception as exc:
                logger.warning("[%s] station %d failed: %s", src, i, exc)
                continue

            all_rows.append({
                "dataset":            src,
                "station_name":       row.get("station_name", ""),
                "lon":                lon,
                "lat":                lat,
                "reported_area_km2":  ra_f,
                # ── 面积辅助匹配结果 ──────────────────────────────────────
                "area_COMID":         res_area.get("COMID"),
                "area_uparea_km2":    res_area.get("uparea"),
                "area_match_quality": res_area.get("match_quality"),
                "area_error":         res_area.get("area_error"),
                "area_distance_deg":  res_area.get("distance"),
                # ── 纯坐标匹配结果 ────────────────────────────────────────
                "dist_COMID":         res_dist.get("COMID"),
                "dist_uparea_km2":    res_dist.get("uparea"),
                "dist_match_quality": res_dist.get("match_quality"),
                "dist_distance_deg":  res_dist.get("distance"),
                # ── 两种方法是否命中同一河段 ──────────────────────────────
                "same_reach":         (res_area.get("COMID") == res_dist.get("COMID")),
            })

        logger.info("[%s] Done.", src)

    if not all_rows:
        logger.error("No matching results generated.")
        sys.exit(1)

    result_df = pd.DataFrame(all_rows)
    _print_summary(result_df, sources)

    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(out_path, index=False)
    logger.info("Results saved to %s  (%d rows)", out_path, len(result_df))


def main():
    run_test(
        s3_csv=S3_CSV,
        merit_dir=MERIT_DIR,
        max_per_source=MAX_PER_SOURCE,
        max_sources=MAX_SOURCES,
        min_coverage=MIN_AREA_COVERAGE,
        out_csv=OUT_CSV,
    )


if __name__ == "__main__":
    main()
