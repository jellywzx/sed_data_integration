#!/usr/bin/env python3
"""
<<<<<<< HEAD
步骤 s6（出图主入口）：一次性调用所有 plot_merged_stations_*.py 子脚本，
生成完整的泥沙参考数据集可视化图集。

生成图像（默认输出至 scripts_basin_test/output/）：
  s6_plot_records_map.png         — 全球站点分布（按记录数着色）
  s6_plot_records_hist.png        — 记录数直方图
  s6_plot_span_map.png            — 全球站点分布（按时间跨度着色）
  s6_plot_span_hist.png           — 时间跨度直方图
  s6_plot_frequency_map.png       — 全球站点分布（按采样频率分类着色）
  s6_plot_frequency_hist.png      — 采样频率直方图（log 轴）
  s6_plot_sources_map.png         — 全球站点分布（按数据来源着色）
  s6_plot_sources_bar.png         — 各来源站点数柱状图
  s6_plot_sources.csv             — 每站点来源明细
  s6_plot_variables_map.png       — 全球站点分布（按 Q/SSC/SSL 组合着色）
  s6_plot_variables_bar.png       — 变量组合柱状图 + 按分辨率分层柱状图
  s6_plot_yearly_coverage.png     — 逐年有效站点数堆叠面积图
  s6_plot_basin_area.png          — 集水面积分布（地图 + 直方图）
  s6_plot_basin_region.png        — Pfafstetter 一级大区站点数柱状图
  s6_plot_basin_quality.png       — 流域匹配质量分布柱状图

依赖：
  pip install matplotlib cartopy netCDF4 numpy pandas

用法（在 scripts_basin_test/ 下运行）：
  python plot/s6_plot_merged_nc_stations.py
  python plot/s6_plot_merged_nc_stations.py --nc /path/to/s6.nc --dpi 200
  python plot/s6_plot_merged_nc_stations.py --only records span   # 只运行指定子图
"""

import argparse
import atexit
import sys
from datetime import datetime
from pathlib import Path

# 确保 plot/ 和 scripts_basin_test/ 均在 sys.path
_PLOT_DIR    = Path(__file__).resolve().parent
_SCRIPTS_DIR = _PLOT_DIR.parent
for _p in (_PLOT_DIR, _SCRIPTS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from s6_plot_common import DEFAULT_NC, DEFAULT_S5_CSV, DEFAULT_OUT_DIR

# ─────────────────────────────────────────────────────────────────────────────
# 日志 tee
# ─────────────────────────────────────────────────────────────────────────────
_LOG_TEE_ENABLED = False


def _enable_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    log_path = Path(__file__).resolve().with_name("{}_log.txt".format(Path(__file__).stem))
    try:
        log_path.unlink(missing_ok=True)
    except Exception:
        pass
    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("===== Run started {} =====\n".format(
        datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()

    class _Tee:
        def __init__(self, s, f):
            self._s, self._f = s, f
        def write(self, d):
            self._s.write(d); self._f.write(d); self._f.flush()
        def flush(self):
            self._s.flush(); self._f.flush()

    sys.stdout = _Tee(sys.stdout, log_fp)
    sys.stderr = _Tee(sys.stderr, log_fp)
=======
步骤 s6（出图）：从 s8_merged_all.nc 统计并绘图；本脚本可一次性生成全部 s6_plot_* 图。

各图也可单独运行 plot/ 下脚本：
  plot_merged_stations_records.py    -> s6_plot_records.png
  plot_merged_stations_span.py       -> s6_plot_span_map.png, s6_plot_span_hist.png
  plot_merged_stations_frequency.py -> s6_plot_frequency_map.png, s6_plot_frequency_hist.png
  plot_merged_stations_sources.py   -> s6_plot_sources_map.png, s6_plot_sources_bar.png, s6_plot_sources.csv

输出（步骤 6 出图）：s6_plot.png、s6_plot_span.png、s6_plot_frequency.png、s6_plot_sources.png、s6_plot_sources.csv、s6_plot_stats.csv（默认在 output/ 下，无子文件夹）。

用法（在 Output_r 根目录下运行）：
  python scripts/s6_plot_merged_nc_stations.py [--nc output/s8_merged_all.nc] [--out output/s6_plot.png]
"""

import argparse
import csv
from pathlib import Path
from collections import defaultdict

import numpy as np
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

# Output_r 根目录（脚本在 scripts/06_plot 下），默认路径基于此绝对路径
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_R_ROOT = SCRIPT_DIR.parent  # Output_r（脚本在 scripts/ 下）


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
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
    atexit.register(log_fp.close)
    _LOG_TEE_ENABLED = True


<<<<<<< HEAD
# ─────────────────────────────────────────────────────────────────────────────
# 子图任务注册表
# ─────────────────────────────────────────────────────────────────────────────

def _run_records(nc, out_dir, dpi):
    from plot_merged_stations_records import main as _main
    sys.argv = ["plot_merged_stations_records.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_records"),
                "--dpi", str(dpi)]
    _main()


def _run_span(nc, out_dir, dpi):
    from plot_merged_stations_span import main as _main
    sys.argv = ["plot_merged_stations_span.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_span"),
                "--dpi", str(dpi)]
    _main()


def _run_frequency(nc, out_dir, dpi):
    from plot_merged_stations_frequency import main as _main
    sys.argv = ["plot_merged_stations_frequency.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_frequency"),
                "--dpi", str(dpi)]
    _main()


def _run_sources(nc, s5_csv, out_dir, dpi):
    from plot_merged_stations_sources import main as _main
    sys.argv = ["plot_merged_stations_sources.py",
                "--nc", nc, "--s5-csv", str(s5_csv),
                "--out", str(out_dir / "s6_plot_sources"),
                "--dpi", str(dpi)]
    _main()


def _run_variables(nc, out_dir, dpi):
    from plot_merged_stations_variables import main as _main
    sys.argv = ["plot_merged_stations_variables.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_variables"),
                "--dpi", str(dpi)]
    _main()


def _run_yearly(nc, out_dir, dpi):
    from plot_merged_stations_yearly_coverage import main as _main
    sys.argv = ["plot_merged_stations_yearly_coverage.py",
                "--nc", nc,
                "--out", str(out_dir / "s6_plot_yearly_coverage.png"),
                "--dpi", str(dpi)]
    _main()


def _run_basin(nc, out_dir, dpi):
    from plot_merged_stations_basin import main as _main
    sys.argv = ["plot_merged_stations_basin.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_basin"),
                "--dpi", str(dpi)]
    _main()


# 所有子任务：(key, 描述, 调用函数)
_ALL_TASKS = [
    ("records",   "记录数分布（地图 + 直方图）"),
    ("span",      "时间跨度分布（地图 + 直方图）"),
    ("frequency", "采样频率分布（地图 + 直方图）"),
    ("sources",   "数据来源分布（地图 + 柱状图）"),
    ("variables", "Q/SSC/SSL 变量可用性（地图 + 柱状图）"),
    ("yearly",    "逐年覆盖度（堆叠面积图）"),
    ("basin",     "流域特征（面积地图 + Pfaf 大区 + 匹配质量）"),
]


def main():
    _enable_logging()

    ap = argparse.ArgumentParser(
        description="步骤 s6 出图主入口：一次生成全部可视化图集",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--nc",     "-n", default=str(DEFAULT_NC),    help="s6 NetCDF 路径")
    ap.add_argument("--s5-csv",       default=str(DEFAULT_S5_CSV),help="s5 clustered CSV 路径（来源出图用）")
    ap.add_argument("--out-dir", "-o",default=str(DEFAULT_OUT_DIR),help="图像输出目录")
    ap.add_argument("--dpi",          type=int, default=150,       help="图像 DPI")
    ap.add_argument("--only", nargs="+",
                    choices=[t[0] for t in _ALL_TASKS],
                    help="只运行指定子图（空格分隔），如 --only records span")
    args = ap.parse_args()

    nc      = args.nc
    s5_csv  = Path(args.s5_csv)
    out_dir = Path(args.out_dir)
    dpi     = args.dpi
    out_dir.mkdir(parents=True, exist_ok=True)

    # 检查 NC
    if not Path(nc).is_file():
        print("ERROR: NC file not found: {}".format(nc))
        print("  请先运行 s6_basin_merge_to_nc.py 生成该文件。")
        sys.exit(1)

    run_set = set(args.only) if args.only else {t[0] for t in _ALL_TASKS}

    print("=" * 68)
    print("s6 出图主入口  NC={}".format(nc))
    print("输出目录: {}".format(out_dir))
    print("=" * 68)

    results = {}

    def _run(key, fn, *fn_args):
        if key not in run_set:
            return
        print("\n── {} ──".format(dict(_ALL_TASKS)[key]))
        try:
            fn(*fn_args)
            results[key] = "OK"
        except Exception as e:
            import traceback
            print("  [WARN] {} 出图失败: {}".format(key, e))
            traceback.print_exc()
            results[key] = "FAILED: {}".format(e)

    _run("records",   _run_records,   nc, out_dir, dpi)
    _run("span",      _run_span,      nc, out_dir, dpi)
    _run("frequency", _run_frequency, nc, out_dir, dpi)
    _run("sources",   _run_sources,   nc, s5_csv, out_dir, dpi)
    _run("variables", _run_variables, nc, out_dir, dpi)
    _run("yearly",    _run_yearly,    nc, out_dir, dpi)
    _run("basin",     _run_basin,     nc, out_dir, dpi)

    print("\n" + "=" * 68)
    print("完成汇总：")
    for key, desc in _ALL_TASKS:
        if key in results:
            status = "✓" if results[key] == "OK" else "✗"
            print("  {} {:12s} {}".format(status, key, results[key]))
    print("输出目录: {}".format(out_dir))
    print("=" * 68)
=======
def main():
    _enable_script_logging()
    ap = argparse.ArgumentParser(description="步骤 s6 出图：从 s8_merged_all.nc 生成 s6_plot_* 图与 s6_plot_stats.csv")
    ap.add_argument("--nc", "-n", default=str(OUTPUT_R_ROOT / "output/s8_merged_all.nc"), help="步骤 s8 输出 s8_merged_all.nc")
    ap.add_argument("--input-dir", "-I", default=str(OUTPUT_R_ROOT / "output"), help="含 s4_clustered_stations.csv 的目录（按来源出图）")
    ap.add_argument("--from-csv", "-c", default=None, help="使用已生成的 s6_plot_stats.csv 代替 NC")
    ap.add_argument("--out", "-o", default=str(OUTPUT_R_ROOT / "output/s6_plot.png"), help="步骤 s6 出图主文件名（将生成 s6_plot_span.png 等）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if args.from_csv:
        csv_path = Path(args.from_csv)
        if not csv_path.is_file():
            print("Error: file not found: {}".format(csv_path))
            return
        with open(csv_path) as f:
            r = csv.DictReader(f)
            rows = list(r)
        lat = np.array([float(x["lat"]) for x in rows])
        lon = np.array([float(x["lon"]) for x in rows])
        rec_count = np.array([int(x["n_records"]) for x in rows])
        span_years = np.array([float(x["span_years"]) for x in rows])
        if rows and "median_interval_days" in rows[0]:
            median_interval_days = np.array([float(x["median_interval_days"]) if x["median_interval_days"].strip() else np.nan for x in rows])
        else:
            median_interval_days = np.full(len(lat), np.nan)
        n_stations = len(lat)
        print("Loaded {} stations from {}".format(n_stations, csv_path))
        print("Records per station: min={}, max={}, mean={:.1f}".format(
            rec_count.min(), rec_count.max(), rec_count.mean()))
        valid_span = span_years[span_years > 0]
        if len(valid_span) > 0:
            print("Time span (years): min={:.2f}, max={:.2f}, mean={:.2f}".format(
                valid_span.min(), valid_span.max(), valid_span.mean()))
        valid_interval = median_interval_days[np.isfinite(median_interval_days) & (median_interval_days > 0)]
        if len(valid_interval) > 0:
            print("Median time interval (days): min={:.2f}, max={:.2f}, mean={:.2f}".format(
                valid_interval.min(), valid_interval.max(), valid_interval.mean()))
    else:
        if nc4 is None:
            print("Error: netCDF4 is required.")
            return

        nc_path = Path(args.nc)
        if not nc_path.is_file():
            print("Error: file not found: {}".format(nc_path))
            return

        print("Loading {} ...".format(nc_path))
        with nc4.Dataset(nc_path, "r") as nc:
            lat = np.asarray(nc.variables["lat"][:])
            lon = np.asarray(nc.variables["lon"][:])
            sid = np.asarray(nc.variables["station_index"][:])
            time = np.asarray(nc.variables["time"][:])

        n_stations = len(lat)
        n_records = len(sid)
        print("n_stations={}, n_records={}".format(n_stations, n_records))

        rec_count = np.bincount(sid, minlength=n_stations)

        order = np.argsort(sid)
        sid_sorted = sid[order]
        time_sorted = time[order]
        boundaries = np.concatenate([[0], np.where(np.diff(sid_sorted) != 0)[0] + 1, [len(sid_sorted)]])
        station_ids_in_order = sid_sorted[boundaries[:-1]]
        t_min = np.full(n_stations, np.nan)
        t_max = np.full(n_stations, np.nan)
        for k in range(len(boundaries) - 1):
            i = station_ids_in_order[k]
            seg = time_sorted[boundaries[k] : boundaries[k + 1]]
            t_valid = seg[(seg > -1e9) & (seg < 1e9)]
            if len(t_valid) > 0:
                t_min[i] = np.min(t_valid)
                t_max[i] = np.max(t_valid)

        span_days = np.where(np.isnan(t_min) | np.isnan(t_max), np.nan, t_max - t_min)
        span_years = span_days / 365.25
        span_years = np.where(np.isnan(span_years), 0.0, span_years)

        median_interval_days = np.full(n_stations, np.nan)
        for k in range(len(boundaries) - 1):
            i = station_ids_in_order[k]
            seg = time_sorted[boundaries[k] : boundaries[k + 1]]
            t_valid = seg[(seg > -1e9) & (seg < 1e9)]
            if len(t_valid) >= 2:
                t_valid = np.sort(np.unique(t_valid))
                diffs = np.diff(t_valid)
                diffs = diffs[diffs > 0]
                if len(diffs) > 0:
                    median_interval_days[i] = np.median(diffs)

        print("Records per station: min={}, max={}, mean={:.1f}".format(
            rec_count.min(), rec_count.max(), rec_count.mean()))
        valid_span = span_years[span_years > 0]
        if len(valid_span) > 0:
            print("Time span (years): min={:.2f}, max={:.2f}, mean={:.2f}".format(
                valid_span.min(), valid_span.max(), valid_span.mean()))
        valid_interval = median_interval_days[np.isfinite(median_interval_days) & (median_interval_days > 0)]
        if len(valid_interval) > 0:
            print("Median time interval (days): min={:.2f}, max={:.2f}, mean={:.2f}".format(
                valid_interval.min(), valid_interval.max(), valid_interval.mean()))

        csv_path = out_path.parent / (out_path.stem + "_stats.csv")
        header = "station_index,lat,lon,n_records,span_years,median_interval_days\n"
        with open(csv_path, "w") as f:
            f.write(header)
            for i in range(n_stations):
                sy = span_years[i] if not np.isnan(span_years[i]) else 0.0
                mi = median_interval_days[i] if np.isfinite(median_interval_days[i]) else ""
                f.write("{},{},{},{},{:.4f},{}\n".format(i, lat[i], lon[i], rec_count[i], sy, mi))
        print("Saved station stats: {}".format(csv_path))

    if not HAS_MPL:
        print("matplotlib not found. Install with: pip install matplotlib")
        print("Re-run to generate figures. Station stats CSV is already saved.")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    sc = ax1.scatter(lon, lat, c=np.log10(rec_count + 1), s=4, cmap="viridis", alpha=0.7)
    ax1.set_xlabel("Longitude")
    ax1.set_ylabel("Latitude")
    ax1.set_title("Stations (color = log10(record count + 1))")
    ax1.set_aspect("equal")
    plt.colorbar(sc, ax=ax1, label="log10(N_records + 1)")
    ax2.hist(rec_count, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
    ax2.set_xlabel("Records per station")
    ax2.set_ylabel("Number of stations")
    ax2.set_title("Distribution of time series length (record count)")
    ax2.axvline(rec_count.mean(), color="red", ls="--", label="mean={:.0f}".format(rec_count.mean()))
    ax2.legend()
    plt.tight_layout()
    plt.savefig(out_path, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_path))

    fig2, (ax3, ax4) = plt.subplots(1, 2, figsize=(14, 5))
    sc2 = ax3.scatter(lon, lat, c=np.clip(span_years, 0, 50), s=4, cmap="plasma", alpha=0.7)
    ax3.set_xlabel("Longitude")
    ax3.set_ylabel("Latitude")
    ax3.set_title("Stations (color = time span in years, capped at 50)")
    ax3.set_aspect("equal")
    plt.colorbar(sc2, ax=ax3, label="Time span (years)")
    ax4.hist(span_years[span_years > 0], bins=50, color="coral", edgecolor="white", alpha=0.8)
    ax4.set_xlabel("Time span (years)")
    ax4.set_ylabel("Number of stations")
    ax4.set_title("Distribution of time series span")
    plt.tight_layout()
    out_span = out_path.parent / (out_path.stem + "_span.png")
    plt.savefig(out_span, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_span))

    valid_interval = np.isfinite(median_interval_days) & (median_interval_days > 0)
    if np.any(valid_interval):
        fig3, (ax5, ax6) = plt.subplots(1, 2, figsize=(14, 5))
        mi = np.where(valid_interval, median_interval_days, np.nan)
        sc3 = ax5.scatter(lon, lat, c=mi, s=4, cmap="coolwarm", alpha=0.7, vmin=0, vmax=365)
        ax5.set_xlabel("Longitude")
        ax5.set_ylabel("Latitude")
        ax5.set_title("Stations (color = median time interval in days)")
        ax5.set_aspect("equal")
        plt.colorbar(sc3, ax=ax5, label="Median time interval (days)")
        mi_vals = median_interval_days[valid_interval]
        xmax = 365
        mi_capped = np.minimum(mi_vals, xmax)
        ax6.hist(mi_capped, bins=np.linspace(0, xmax, 51), color="seagreen", edgecolor="white", alpha=0.8)
        ax6.set_xlim(0, xmax)
        ax6.set_xlabel("Median time interval (days)")
        ax6.set_ylabel("Number of stations")
        ax6.set_title("Distribution of time series frequency (median interval)")
        n_over = np.sum(mi_vals > xmax)
        if n_over > 0:
            ax6.text(0.98, 0.98, "{} stations > {} d".format(n_over, xmax), transform=ax6.transAxes, ha="right", va="top", fontsize=9)
        ax6.axvline(np.median(mi_vals), color="red", ls="--", label="median={:.2f} d".format(np.median(mi_vals)))
        ax6.legend()
        plt.tight_layout()
        out_freq = out_path.parent / (out_path.stem + "_frequency.png")
        plt.savefig(out_freq, dpi=args.dpi, bbox_inches="tight")
        plt.close()
        print("Saved: {}".format(out_freq))
    else:
        print("No valid time intervals for frequency plot; skip _frequency.png")

    # 按数据来源出图：需要 clustered_stations.csv
    indir = Path(args.input_dir)
    clustered_path = indir / "s4_clustered_stations.csv"
    if clustered_path.is_file():
        import pandas as pd
        st = pd.read_csv(clustered_path)
        if "cluster_id" in st.columns and "source" in st.columns:
            # 每个 cluster_id（= station_index）对应的来源集合
            by_cid = defaultdict(set)
            for _, row in st.iterrows():
                cid = int(row["cluster_id"])
                by_cid[cid].add(str(row["source"]).strip())
            primary_source = []
            all_sources_list = []
            for i in range(n_stations):
                srcs = by_cid.get(i, set())
                srcs = sorted(srcs)
                if len(srcs) == 0:
                    primary_source.append("unknown")
                    all_sources_list.append("")
                elif len(srcs) == 1:
                    primary_source.append(srcs[0])
                    all_sources_list.append(srcs[0])
                else:
                    primary_source.append("mixed")
                    all_sources_list.append(",".join(srcs))
            unique_sources = sorted(set(primary_source))
            src2idx = {s: i for i, s in enumerate(unique_sources)}
            cidx = np.array([src2idx[s] for s in primary_source], dtype=np.int32)
            n_cats = len(unique_sources)
            if n_cats <= 20:
                cmap = plt.colormaps.get_cmap("tab20").resampled(n_cats)
            else:
                from matplotlib.colors import ListedColormap
                base = plt.colormaps.get_cmap("tab20b").resampled(20)
                cols = [base(i % 20) for i in range(n_cats)]
                cmap = ListedColormap(cols)

            fig4, (ax_map, ax_bar) = plt.subplots(1, 2, figsize=(16, 6))
            sc4 = ax_map.scatter(lon, lat, c=cidx, s=4, cmap=cmap, alpha=0.7, vmin=0, vmax=max(0, n_cats - 1))
            ax_map.set_xlabel("Longitude")
            ax_map.set_ylabel("Latitude")
            ax_map.set_title("Stations by data source (primary)")
            ax_map.set_aspect("equal")
            patches = [plt.matplotlib.patches.Patch(color=cmap(i), label=unique_sources[i]) for i in range(n_cats)]
            ax_map.legend(handles=patches, loc="upper left", fontsize=7, ncol=2)
            counts = [primary_source.count(s) for s in unique_sources]
            bars = ax_bar.bar(range(n_cats), counts, color=[cmap(i) for i in range(n_cats)], edgecolor="white")
            ax_bar.set_xticks(range(n_cats))
            ax_bar.set_xticklabels(unique_sources, rotation=45, ha="right", fontsize=8)
            ax_bar.set_ylabel("Number of stations")
            ax_bar.set_title("Stations per source (primary)")
            plt.tight_layout()
            out_sources = out_path.parent / (out_path.stem + "_sources.png")
            plt.savefig(out_sources, dpi=args.dpi, bbox_inches="tight")
            plt.close()
            print("Saved: {}".format(out_sources))

            # 写出每个站点的来源 CSV
            src_csv = out_path.parent / (out_path.stem + "_sources.csv")
            with open(src_csv, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                w.writerow(["station_index", "lat", "lon", "primary_source", "n_sources", "all_sources"])
                for i in range(n_stations):
                    n_src = len(by_cid.get(i, set()))
                    w.writerow([i, lat[i], lon[i], primary_source[i], n_src, all_sources_list[i]])
            print("Saved: {}".format(src_csv))
        else:
            print("s4_clustered_stations.csv missing 'cluster_id' or 'source'; skip source plot.")
    else:
        print("No s4_clustered_stations.csv at {}; skip source plot.".format(clustered_path))
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538


if __name__ == "__main__":
    main()
