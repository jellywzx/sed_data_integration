#!/usr/bin/env python3
"""
<<<<<<< HEAD
步骤 s6 出图之一：站点时间采样频率（中位间隔）分布（全球地图 + 直方图）。
输出：s6_plot_frequency_map.png、s6_plot_frequency_hist.png

用法：
  python plot/plot_merged_stations_frequency.py
  python plot/plot_merged_stations_frequency.py --nc /path/to/s6.nc --out output/s6_plot_frequency
=======
步骤 s6 出图之一：从 s8_merged_all.nc 统计站点时间间隔（中位数），出图（地图一张、直方图一张）。
输出：s6_plot_frequency_map.png、s6_plot_frequency_hist.png（默认在 output_bf/）。若无有效间隔则跳过出图。
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
"""

import argparse
import sys
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
<<<<<<< HEAD
from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR, load_station_stats
=======
from s6_plot_common import load_station_stats

# 直观绝对路径（按需修改根目录即可）
OUTPUT_R_ROOT = Path("/media/zhwei/data02/weizx/sediment_wzx_1111/Output_r")
DEFAULT_NC_ABS = OUTPUT_R_ROOT / "output_bf/03_merge/merged_all.nc"
DEFAULT_OUT_PREFIX_ABS = OUTPUT_R_ROOT / "output_bf/s6_plot_frequency"
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
<<<<<<< HEAD
    from matplotlib.colors import BoundaryNorm
=======
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False

<<<<<<< HEAD
_EDGE_COLOR = (0.35, 0.35, 0.35)

# 分段配色：daily(≤2d) / monthly(3-40d) / annual(40-400d) / irregular(>400d)
_BINS   = [0, 2, 40, 400, 1e6]
_LABELS = ["daily (≤2 d)", "monthly (3–40 d)", "annual (40–400 d)", "irregular (>400 d)"]
_COLORS = ["#2196F3", "#4CAF50", "#FF9800", "#9E9E9E"]


def _classify(mi_days):
    """将中位间隔天数映射到 0–3 类别。"""
    cats = np.full(len(mi_days), -1, dtype=int)
    for k in range(len(_BINS) - 1):
        cats = np.where((mi_days > _BINS[k]) & (mi_days <= _BINS[k + 1]), k, cats)
    return cats


def _draw_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot station map/histogram by median time interval")
    ap.add_argument("--nc",       "-n", default=str(DEFAULT_NC),  help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None,             help="Use pre-computed stats CSV")
    ap.add_argument("--out",      "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_frequency"),
                    help="输出前缀（将生成 _map.png 与 _hist.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
=======

def main():
    ap = argparse.ArgumentParser(description="Plot station map by median time interval + histogram")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC_ABS), help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None, help="Use pre-computed stats CSV instead of NC")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_PREFIX_ABS), help="步骤 s6 出图前缀（将生成 _map.png 与 _hist.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out).resolve()
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

<<<<<<< HEAD
    data = load_station_stats(
        nc_path  = args.nc  if not args.from_csv else None,
        csv_path = args.from_csv,
        out_dir  = out_prefix.parent if not args.from_csv else None,
    )
    lat     = data["lat"]
    lon     = data["lon"]
    mi      = data["median_interval_days"]
    n_stat  = data["n_stations"]

    valid = np.isfinite(mi) & (mi > 0)
    if not valid.any():
        print("No valid time intervals found; skipping frequency plots.")
        return

    print("Median interval (days): n_valid={:,}  min={:.1f}  max={:.1f}  median={:.1f}".format(
        int(valid.sum()), mi[valid].min(), mi[valid].max(), np.median(mi[valid])))

    cats = _classify(mi)

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    # ── 图1：全球地图（分类着色）─────────────────────────────────────────────
    out_map = out_prefix.parent / (out_prefix.name + "_map.png")
    fig = plt.figure(figsize=(10, 5))
    if HAS_CARTOPY:
        ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
        _draw_basemap(ax)
        for k, (label, color) in enumerate(zip(_LABELS, _COLORS)):
            mk = valid & (cats == k)
            if mk.any():
                ax.scatter(lon[mk], lat[mk], color=color, s=4, alpha=0.85,
                           label="{} ({:,})".format(label, int(mk.sum())),
                           transform=ccrs.PlateCarree())
    else:
        ax = fig.add_subplot(111)
        ax.set_xlim(-180, 180); ax.set_ylim(-60, 90)
        ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")
        for k, (label, color) in enumerate(zip(_LABELS, _COLORS)):
            mk = valid & (cats == k)
            if mk.any():
                ax.scatter(lon[mk], lat[mk], color=color, s=4, alpha=0.85,
                           label="{} ({:,})".format(label, int(mk.sum())))

    ax.set_title("Sediment stations — temporal resolution  (n={:,})".format(n_stat), fontsize=11)
    ax.legend(loc="lower left", fontsize=8, markerscale=2,
              framealpha=0.85, handletextpad=0.4)
=======
    nc_path_abs = Path(args.nc).resolve() if args.nc else None
    csv_path_abs = Path(args.from_csv).resolve() if args.from_csv else None
    data = load_station_stats(
        nc_path=nc_path_abs if not args.from_csv else None,
        csv_path=csv_path_abs,
        out_dir=out_prefix.parent if not args.from_csv else None,
    )
    lat = data["lat"]
    lon = data["lon"]
    median_interval_days = data["median_interval_days"]

    valid_interval = np.isfinite(median_interval_days) & (median_interval_days > 0)
    if not np.any(valid_interval):
        print("No valid time intervals for frequency plot; skip.")
        return

    if not HAS_MPL:
        print("matplotlib not found. Install with: pip install matplotlib")
        return

    # 图1：地图（海岸线 + 与左图适配的缩短 colorbar）
    out_map = (out_prefix.parent / (out_prefix.name + "_map.png")).resolve()
    fig1 = plt.figure(figsize=(7, 5))
    mi = np.where(valid_interval, median_interval_days, np.nan)
    if HAS_CARTOPY:
        ax5 = fig1.add_subplot(111, projection=ccrs.PlateCarree())
        ax5.add_feature(cfeature.LAND, facecolor="none", edgecolor="none")
        _edgecolor = (0.35, 0.35, 0.35)
        ax5.add_feature(cfeature.COASTLINE, linewidth=1.0, edgecolor=_edgecolor)
        ax5.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle="-", edgecolor=_edgecolor)
        ax5.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())
        sc3 = ax5.scatter(lon, lat, c=mi, s=4, cmap="coolwarm", alpha=0.7, vmin=0, vmax=365, transform=ccrs.PlateCarree())
    else:
        ax5 = fig1.add_subplot(111)
        ax5.set_xlim(-180, 180)
        ax5.set_ylim(-60, 90)
        sc3 = ax5.scatter(lon, lat, c=mi, s=4, cmap="coolwarm", alpha=0.7, vmin=0, vmax=365)
        ax5.set_xlabel("Longitude")
        ax5.set_ylabel("Latitude")
        ax5.set_aspect("equal")
    ax5.set_title("Stations (color = median time interval in days)")
    cbar = fig1.colorbar(sc3, ax=ax5, label="Median time interval (days)", shrink=0.75, aspect=28, pad=0.06, fraction=0.022)
    cbar.ax.tick_params(labelsize=8)
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
    plt.tight_layout()
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

<<<<<<< HEAD
    # ── 图2：直方图（log 轴，分类上色）──────────────────────────────────────
    out_hist = out_prefix.parent / (out_prefix.name + "_hist.png")
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    bins = np.logspace(np.log10(0.5), np.log10(max(mi[valid].max() * 1.2, 2000)), 60)
    for k, (label, color) in enumerate(zip(_LABELS, _COLORS)):
        mk = valid & (cats == k)
        if mk.any():
            ax2.hist(mi[mk], bins=bins, color=color, edgecolor="white",
                     alpha=0.85, label="{} ({:,})".format(label, int(mk.sum())))
    ax2.set_xscale("log")
    ax2.set_xlabel("Median time interval (days, log scale)")
    ax2.set_ylabel("Number of stations")
    ax2.set_title("Distribution of temporal resolution  (n valid={:,})".format(int(valid.sum())))
    for b in _BINS[1:-1]:
        ax2.axvline(b, color="black", ls="--", lw=0.8, alpha=0.5)
    ax2.legend(fontsize=9)
=======
    # 图2：直方图
    out_hist = (out_prefix.parent / (out_prefix.name + "_hist.png")).resolve()
    fig2, ax6 = plt.subplots(figsize=(7, 5))
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
>>>>>>> 6296cf2afe3b4a9aa5abe5540ed519e1eeb66538
    plt.tight_layout()
    plt.savefig(out_hist, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_hist))


if __name__ == "__main__":
    main()
