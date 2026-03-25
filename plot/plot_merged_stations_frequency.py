#!/usr/bin/env python3
"""
步骤 s6 出图之一：从 s8_merged_all.nc 统计站点时间间隔（中位数），出图（地图一张、直方图一张）。
输出：s6_plot_frequency_map.png、s6_plot_frequency_hist.png（默认在 output_bf/）。若无有效间隔则跳过出图。
"""

import argparse
import sys
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import load_station_stats

# 直观绝对路径（按需修改根目录即可）
OUTPUT_R_ROOT = Path("/media/zhwei/data02/weizx/sediment_wzx_1111/Output_r")
DEFAULT_NC_ABS = OUTPUT_R_ROOT / "output_bf/03_merge/merged_all.nc"
DEFAULT_OUT_PREFIX_ABS = OUTPUT_R_ROOT / "output_bf/s6_plot_frequency"

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False


def main():
    ap = argparse.ArgumentParser(description="Plot station map by median time interval + histogram")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC_ABS), help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None, help="Use pre-computed stats CSV instead of NC")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_PREFIX_ABS), help="步骤 s6 出图前缀（将生成 _map.png 与 _hist.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out).resolve()
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

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
    plt.tight_layout()
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

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
    plt.tight_layout()
    plt.savefig(out_hist, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_hist))


if __name__ == "__main__":
    main()
