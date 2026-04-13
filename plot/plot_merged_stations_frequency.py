#!/usr/bin/env python3
"""
步骤 s6 出图之一：站点主时间类型分布（全球地图 + 柱状图）。
输出：s6_plot_frequency_map.png、s6_plot_frequency_hist.png

这里的“frequency”不再按旧的中位间隔粗分类，而是直接使用最终 nc 中
写入的时间类型编码，和当前结果保持一致：

  daily / monthly / annual / climatology / other

用法：
  python plot/plot_merged_stations_frequency.py
  python plot/plot_merged_stations_frequency.py --nc /path/to/s6.nc --out output/s6_plot_frequency
"""

import argparse
import sys
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import (
    DEFAULT_NC,
    DEFAULT_OUT_DIR,
    RES_CODES,
    RES_COLORS,
    RES_ORDER,
    annotate_bars,
    load_station_stats,
)

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

_EDGE_COLOR = (0.35, 0.35, 0.35)


def _draw_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot dominant temporal type by station")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC), help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None, help="Use pre-computed stats CSV with primary_resolution")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_frequency"),
                    help="输出前缀（将生成 _map.png 与 _hist.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    data = load_station_stats(
        nc_path=args.nc if not args.from_csv else None,
        csv_path=args.from_csv,
        out_dir=out_prefix.parent if not args.from_csv else None,
    )
    lat = data["lat"]
    lon = data["lon"]
    primary_res = data["primary_resolution"]
    n_stat = data["n_stations"]

    valid = np.isin(primary_res, RES_ORDER)
    if not valid.any():
        print("No primary_resolution column found; re-run without --from-csv to rebuild stats from nc.")
        return

    counts = []
    for code in RES_ORDER:
        counts.append(int(np.sum(primary_res == code)))

    print("Primary temporal type counts (n_stations={:,}):".format(n_stat))
    for code, count in zip(RES_ORDER, counts):
        print("  {:12s}: {:,} ({:.1f}%)".format(
            RES_CODES[code], count, (count / float(max(n_stat, 1))) * 100.0
        ))

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    # 图1：主时间类型地图
    out_map = out_prefix.parent / (out_prefix.name + "_map.png")
    fig = plt.figure(figsize=(10, 5))
    if HAS_CARTOPY:
        ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
        _draw_basemap(ax)
        for code in RES_ORDER:
            mk = (primary_res == code)
            if mk.any():
                ax.scatter(
                    lon[mk], lat[mk],
                    color=RES_COLORS[code], s=4, alpha=0.85,
                    label="{} ({:,})".format(RES_CODES[code], int(mk.sum())),
                    transform=ccrs.PlateCarree(),
                )
    else:
        ax = fig.add_subplot(111)
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 90)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        for code in RES_ORDER:
            mk = (primary_res == code)
            if mk.any():
                ax.scatter(
                    lon[mk], lat[mk],
                    color=RES_COLORS[code], s=4, alpha=0.85,
                    label="{} ({:,})".format(RES_CODES[code], int(mk.sum())),
                )

    ax.set_title("Sediment stations — dominant temporal type  (n={:,})".format(n_stat), fontsize=11)
    ax.legend(loc="lower left", fontsize=8, framealpha=0.85, handletextpad=0.4, markerscale=2)
    plt.tight_layout()
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

    # 图2：主时间类型柱状图
    out_hist = out_prefix.parent / (out_prefix.name + "_hist.png")
    fig2, ax2 = plt.subplots(figsize=(8, 5))
    x = np.arange(len(RES_ORDER))
    bars = ax2.bar(
        x,
        counts,
        color=[RES_COLORS[code] for code in RES_ORDER],
        edgecolor="white",
        alpha=0.9,
    )
    annotate_bars(ax2, bars, fontsize=9, padding=3)
    ax2.set_xticks(x)
    ax2.set_xticklabels([RES_CODES[code] for code in RES_ORDER], rotation=20, ha="right")
    ax2.set_ylabel("Number of stations")
    ax2.set_title("Dominant temporal type by station  (n={:,})".format(n_stat))
    plt.tight_layout()
    plt.savefig(out_hist, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_hist))


if __name__ == "__main__":
    main()
