#!/usr/bin/env python3
"""
步骤 s6 出图之一：按数据来源绘图（全球地图 + 柱状图）。
输出：s6_plot_sources_map.png、s6_plot_sources_bar.png、s6_plot_sources.csv

当前版本直接使用最终 nc 站点层中的 `sources_used` 字段，
不再依赖旧的 s5 CSV 去反推来源。

用法：
  python plot/plot_merged_stations_sources.py
  python plot/plot_merged_stations_sources.py --nc /path/to/s6.nc
"""

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR, annotate_bars, load_station_sources

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

_SOURCE_COLORS = [
    "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7",
    "#4477AA", "#EE6677", "#228833", "#CCBB44", "#66CCEE", "#AA3377", "#BBBBBB",
    "#332288", "#88CCEE", "#44AA99", "#117733", "#999933", "#DDCC77",
    "#CC6677", "#882255", "#AA4499", "#333333",
]


def _get_cmap(n):
    from matplotlib.colors import ListedColormap
    if n <= len(_SOURCE_COLORS):
        return ListedColormap(_SOURCE_COLORS[:n])
    import matplotlib.colors as mc
    extra = n - len(_SOURCE_COLORS)
    base = [mc.to_rgba(c) for c in _SOURCE_COLORS]
    for h in np.linspace(0, 1, extra, endpoint=False):
        rgb = mc.hsv_to_rgb([h, 0.75, 0.95])
        base.append((rgb[0], rgb[1], rgb[2], 1.0))
    return ListedColormap(base[:n])


def _draw_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot stations by data source (map + bar)")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC), help="Merged NetCDF path")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_sources"),
                    help="输出前缀")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    data = load_station_sources(nc_path=args.nc)
    lat = data["lat"]
    lon = data["lon"]
    cluster_uid = data["cluster_uid"]
    primary_source = data["primary_source"]
    n_sources_arr = data["n_sources"]
    all_sources = data["all_sources"]
    n_stat = data["n_stations"]

    counts_map = Counter(primary_source.tolist())
    unique_srcs = [name for name, _ in counts_map.most_common()]
    counts = [counts_map[name] for name in unique_srcs]
    src2idx = dict((name, idx) for idx, name in enumerate(unique_srcs))

    print("Primary sources: {}".format(", ".join(
        "{} ({})".format(name, counts_map[name]) for name in unique_srcs
    )))

    out_csv = out_prefix.parent / (out_prefix.name + ".csv")
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "station_index", "cluster_uid", "lat", "lon",
            "primary_source", "n_sources", "all_sources"
        ])
        for i in range(n_stat):
            writer.writerow([
                i, cluster_uid[i], lat[i], lon[i],
                primary_source[i], int(n_sources_arr[i]), all_sources[i]
            ])
    print("Saved: {}".format(out_csv))

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    cmap = _get_cmap(len(unique_srcs))

    # 图1：全球地图
    out_map = out_prefix.parent / (out_prefix.name + "_map.png")
    fig = plt.figure(figsize=(13, 6))
    if HAS_CARTOPY:
        ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
        _draw_basemap(ax)
        for idx, src in enumerate(unique_srcs):
            mk = (primary_source == src)
            if mk.any():
                ax.scatter(
                    lon[mk], lat[mk],
                    color=cmap(idx), s=3, alpha=0.85,
                    label="{} ({:,})".format(src, int(mk.sum())),
                    transform=ccrs.PlateCarree(), zorder=idx + 2,
                )
    else:
        ax = fig.add_subplot(111)
        ax.set_xlim(-180, 180)
        ax.set_ylim(-60, 90)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        for idx, src in enumerate(unique_srcs):
            mk = (primary_source == src)
            if mk.any():
                ax.scatter(
                    lon[mk], lat[mk],
                    color=cmap(idx), s=3, alpha=0.85,
                    label="{} ({:,})".format(src, int(mk.sum())),
                    zorder=idx + 2,
                )

    ax.set_title("Sediment stations by primary source  (n={:,})".format(n_stat), fontsize=11)
    legend_cols = 3 if len(unique_srcs) > 18 else 2
    ax.legend(
        loc="upper left",
        fontsize=7,
        ncol=legend_cols,
        bbox_to_anchor=(1.02, 1.0),
        borderaxespad=0,
        frameon=True,
        handletextpad=0.4,
        markerscale=2,
    )
    plt.tight_layout(rect=[0, 0, 0.78, 1])
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

    # 图2：柱状图
    out_bar = out_prefix.parent / (out_prefix.name + "_bar.png")
    fig2, ax2 = plt.subplots(figsize=(max(8, len(unique_srcs) * 0.55), 5))
    x = np.arange(len(unique_srcs))
    bars = ax2.bar(
        x,
        counts,
        color=[cmap(src2idx[name]) for name in unique_srcs],
        edgecolor="white",
        alpha=0.9,
    )
    annotate_bars(ax2, bars, fontsize=8, padding=2)
    ax2.set_xticks(x)
    ax2.set_xticklabels(unique_srcs, rotation=45, ha="right", fontsize=9)
    ax2.set_ylabel("Number of stations")
    ax2.set_title("Stations per primary source  (total={:,})".format(n_stat))
    plt.tight_layout()
    plt.savefig(out_bar, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_bar))


if __name__ == "__main__":
    main()
