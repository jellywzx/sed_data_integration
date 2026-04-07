#!/usr/bin/env python3
"""
<<<<<<< HEAD
步骤 s6 出图之一：站点记录数分布（全球地图 + 直方图）。
输出：s6_plot_records_map.png、s6_plot_records_hist.png

用法：
  python plot/plot_merged_stations_records.py
  python plot/plot_merged_stations_records.py --nc /path/to/s6.nc --out output/s6_plot_records
=======
步骤 s6 出图之一：从 s8_merged_all.nc 统计站点记录数，出图（地图 + 直方图）。
输出：s6_plot_records.png（默认在 output/）；若从 NC 首次加载会同时生成 s6_plot_stats.csv。
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
"""

import argparse
import sys
from pathlib import Path

import numpy as np

<<<<<<< HEAD
_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR, load_station_stats
=======
# s6_plot_common 已移至 scripts/，确保可从父目录导入
_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import OUTPUT_R_ROOT, DEFAULT_NC, DEFAULT_OUT_DIR, load_station_stats
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

<<<<<<< HEAD
try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False

_EDGE_COLOR = (0.35, 0.35, 0.35)


def _draw_basemap(ax):
    """为 cartopy 子图添加海岸线与国界。"""
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot station map/histogram by record count")
    ap.add_argument("--nc",       "-n", default=str(DEFAULT_NC),  help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None,             help="Use pre-computed stats CSV")
    ap.add_argument("--out",      "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_records"),
                    help="输出前缀（将生成 _map.png 与 _hist.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    data = load_station_stats(
        nc_path  = args.nc  if not args.from_csv else None,
        csv_path = args.from_csv,
        out_dir  = out_prefix.parent if not args.from_csv else None,
    )
    lat       = data["lat"]
    lon       = data["lon"]
    rec_count = data["rec_count"]
    n_stat    = data["n_stations"]

    print("n_stations={:,}  records: min={} max={} mean={:.1f} median={:.0f}".format(
        n_stat, rec_count.min(), rec_count.max(),
        rec_count.mean(), np.median(rec_count)))

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    log_rc = np.log10(rec_count.astype(float) + 1)

    # ── 图1：全球地图 ──────────────────────────────────────────────────────────
    out_map = out_prefix.parent / (out_prefix.name + "_map.png")
    fig = plt.figure(figsize=(10, 5))
    if HAS_CARTOPY:
        ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
        _draw_basemap(ax)
        sc = ax.scatter(lon, lat, c=log_rc, s=4, cmap="viridis", alpha=0.8,
                        transform=ccrs.PlateCarree())
    else:
        ax = fig.add_subplot(111)
        ax.set_xlim(-180, 180); ax.set_ylim(-60, 90)
        ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")
        sc = ax.scatter(lon, lat, c=log_rc, s=4, cmap="viridis", alpha=0.8)

    ax.set_title("Sediment stations — record count  (n={:,})".format(n_stat), fontsize=11)
    cb = fig.colorbar(sc, ax=ax, label="log₁₀(N records + 1)", shrink=0.7,
                      aspect=25, pad=0.04, fraction=0.022)
    cb.ax.tick_params(labelsize=8)
    # 自定义刻度：显示原始值
    ticks = [0, 1, 2, 3, 4]
    cb.set_ticks(ticks)
    cb.set_ticklabels(["1", "10", "100", "1 k", "10 k"])
    plt.tight_layout()
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

    # ── 图2：直方图 ────────────────────────────────────────────────────────────
    out_hist = out_prefix.parent / (out_prefix.name + "_hist.png")
    fig2, ax2 = plt.subplots(figsize=(7, 5))
    bins = np.linspace(0, np.percentile(rec_count, 99), 51)
    ax2.hist(rec_count, bins=bins, color="#2196F3", edgecolor="white", alpha=0.85)
    ax2.axvline(rec_count.mean(),   color="red",    ls="--", lw=1.5,
                label="mean = {:.0f}".format(rec_count.mean()))
    ax2.axvline(np.median(rec_count), color="orange", ls=":",  lw=1.5,
                label="median = {:.0f}".format(np.median(rec_count)))
    n_over = int((rec_count > bins[-1]).sum())
    if n_over:
        ax2.text(0.98, 0.97, "+{} stations > {:.0f}".format(n_over, bins[-1]),
                 transform=ax2.transAxes, ha="right", va="top", fontsize=9, color="gray")
    ax2.set_xlabel("Records per station")
    ax2.set_ylabel("Number of stations")
    ax2.set_title("Distribution of time-series length  (n={:,})".format(n_stat))
    ax2.legend(fontsize=9)
    plt.tight_layout()
    plt.savefig(out_hist, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_hist))
=======

def main():
    ap = argparse.ArgumentParser(description="Plot station map by record count + histogram")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC), help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None, help="Use pre-computed stats CSV instead of NC")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_records.png"), help="步骤 s6 出图：s6_plot_records.png")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = load_station_stats(
        nc_path=args.nc if not args.from_csv else None,
        csv_path=args.from_csv,
        out_dir=out_path.parent if not args.from_csv else None,
    )
    lat = data["lat"]
    lon = data["lon"]
    rec_count = data["rec_count"]
    n_stations = data["n_stations"]

    print("n_stations={}, records: min={}, max={}, mean={:.1f}".format(
        n_stations, rec_count.min(), rec_count.max(), rec_count.mean()))

    if not HAS_MPL:
        print("matplotlib not found. Install with: pip install matplotlib")
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
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538


if __name__ == "__main__":
    main()
