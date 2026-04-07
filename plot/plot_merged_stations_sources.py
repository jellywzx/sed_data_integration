#!/usr/bin/env python3
"""
步骤 s6 出图之一：按数据来源绘图（全球地图 + 柱状图）。
输出：s6_plot_sources_map.png、s6_plot_sources_bar.png、s6_plot_sources.csv

用法：
  python plot/plot_merged_stations_sources.py
  python plot/plot_merged_stations_sources.py --nc /path/to/s6.nc --s5-csv /path/to/s5.csv
"""

import argparse
import csv
import sys
from pathlib import Path
from collections import defaultdict

import numpy as np
import pandas as pd

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import DEFAULT_NC, DEFAULT_S5_CSV, DEFAULT_OUT_DIR, load_station_stats

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

# 色盲友好配色（Wong + Paul Tol，最多 24 类）
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
    base  = [mc.to_rgba(c) for c in _SOURCE_COLORS]
    for h in np.linspace(0, 1, extra, endpoint=False):
        rgb = mc.hsv_to_rgb([h, 0.75, 0.95])
        base.append((*rgb, 1.0))
    return ListedColormap(base[:n])


def _draw_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot stations by data source (map + bar)")
    ap.add_argument("--nc",       "-n", default=str(DEFAULT_NC),     help="Merged NetCDF path")
    ap.add_argument("--from-csv", "-c", default=None,                help="Use pre-computed stats CSV")
    ap.add_argument("--s5-csv",         default=str(DEFAULT_S5_CSV), help="s5_basin_clustered_stations.csv 路径")
    ap.add_argument("--out",      "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_sources"),
                    help="输出前缀")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    # 加载站点坐标
    data = load_station_stats(
        nc_path  = args.nc  if not args.from_csv else None,
        csv_path = args.from_csv,
        out_dir  = out_prefix.parent if not args.from_csv else None,
    )
    lat       = data["lat"]
    lon       = data["lon"]
    n_stat    = data["n_stations"]

    # 加载 s5 clustered stations → cluster_id/source 映射
    s5_path = Path(args.s5_csv)
    if not s5_path.is_file():
        print("s5 CSV not found: {}".format(s5_path))
        return
    st = pd.read_csv(s5_path)
    if "cluster_id" not in st.columns or "source" not in st.columns:
        print("s5 CSV must contain 'cluster_id' and 'source' columns.")
        return

    # cluster_id → 所有来源集合（一个 cluster 可能跨数据源）
    by_cid = defaultdict(set)
    for _, row in st.iterrows():
        cid = int(row["cluster_id"])
        by_cid[cid].add(str(row["source"]).strip())

    # 每个 station（NC 的 0-based 索引 = cluster_id 的 0-based 排序位置）
    # NC 的 station_index i 对应 cluster_id 排序后第 i 个；此处我们按 station_id 直接映射
    # 从 s5 CSV 中读取 cluster_id 列表，按 cluster_id 升序排列（s6 写 NC 时的顺序）
    all_cids = sorted(st["cluster_id"].unique())

    primary_source  = []
    all_sources_list = []
    for idx in range(n_stat):
        # NC station_index i 对应 all_cids[i]（s6 按 sorted cluster_ids 写入）
        cid  = all_cids[idx] if idx < len(all_cids) else idx
        srcs = sorted(by_cid.get(cid, set()))
        if len(srcs) == 0:
            primary_source.append("unknown")
            all_sources_list.append("")
        elif len(srcs) == 1:
            primary_source.append(srcs[0])
            all_sources_list.append(srcs[0])
        else:
            primary_source.append("mixed")
            all_sources_list.append(",".join(srcs))

    unique_srcs = sorted(set(primary_source))
    src2idx     = {s: i for i, s in enumerate(unique_srcs)}
    cidx        = np.array([src2idx[s] for s in primary_source], dtype=np.int32)
    n_cats      = len(unique_srcs)
    counts      = [primary_source.count(s) for s in unique_srcs]

    print("Sources: {}".format(", ".join(
        "{} ({})".format(s, c) for s, c in zip(unique_srcs, counts))))

    # 写 CSV
    out_csv = out_prefix.parent / (out_prefix.name + ".csv")
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_index", "lat", "lon", "primary_source", "n_sources", "all_sources"])
        for i in range(n_stat):
            n_src = len(by_cid.get(all_cids[i] if i < len(all_cids) else i, set()))
            w.writerow([i, lat[i], lon[i], primary_source[i], n_src, all_sources_list[i]])
    print("Saved: {}".format(out_csv))

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    cmap = _get_cmap(n_cats)

    # ── 图1：全球地图 ──────────────────────────────────────────────────────────
    out_map = out_prefix.parent / (out_prefix.name + "_map.png")
    fig = plt.figure(figsize=(13, 6))
    if HAS_CARTOPY:
        ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
        _draw_basemap(ax)
        # 按来源分批绘点（保证图例准确）
        for k, src in enumerate(unique_srcs):
            mk = np.array(primary_source) == src
            if mk.any():
                ax.scatter(lon[mk], lat[mk], color=cmap(k), s=3, alpha=0.85,
                           label="{} ({:,})".format(src, int(mk.sum())),
                           transform=ccrs.PlateCarree(), zorder=k + 2)
    else:
        ax = fig.add_subplot(111)
        ax.set_xlim(-180, 180); ax.set_ylim(-60, 90)
        ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")
        for k, src in enumerate(unique_srcs):
            mk = np.array(primary_source) == src
            if mk.any():
                ax.scatter(lon[mk], lat[mk], color=cmap(k), s=3, alpha=0.85,
                           label="{} ({:,})".format(src, int(mk.sum())), zorder=k + 2)

    ax.set_title("Sediment stations by data source  (n={:,})".format(n_stat), fontsize=11)
    ax.legend(loc="upper left", fontsize=7, ncol=2,
              bbox_to_anchor=(1.02, 1.0), borderaxespad=0,
              frameon=True, handletextpad=0.4, markerscale=2)
    plt.tight_layout(rect=[0, 0, 0.78, 1])
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

    # ── 图2：柱状图 ────────────────────────────────────────────────────────────
    out_bar = out_prefix.parent / (out_prefix.name + "_bar.png")
    order   = np.argsort(counts)[::-1]   # 降序
    fig2, ax2 = plt.subplots(figsize=(max(8, n_cats * 0.55), 5))
    bar_colors = [cmap(src2idx[unique_srcs[i]]) for i in order]
    bars = ax2.bar(range(n_cats), [counts[i] for i in order],
                   color=bar_colors, edgecolor="white", alpha=0.9)
    ax2.bar_label(bars, fontsize=8, padding=2)
    ax2.set_xticks(range(n_cats))
    ax2.set_xticklabels([unique_srcs[i] for i in order],
                        rotation=45, ha="right", fontsize=9)
    ax2.set_ylabel("Number of stations")
    ax2.set_title("Stations per data source  (total={:,})".format(n_stat))
    plt.tight_layout()
    plt.savefig(out_bar, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_bar))


if __name__ == "__main__":
    main()
