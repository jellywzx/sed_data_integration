#!/usr/bin/env python3
"""
步骤 s6 出图之一：按数据来源出图。需 s8_merged_all.nc（或 s6_plot_stats.csv）提供站点位置 + s4_clustered_stations.csv 提供 cluster_id/source。
输出：s6_plot_sources_map.png、s6_plot_sources_bar.png、s6_plot_sources.csv（默认在 output_bf/）。
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

from s6_plot_common import load_station_stats

# 直观绝对路径（按需修改根目录即可）
OUTPUT_R_ROOT = Path("/media/zhwei/data02/weizx/sediment_wzx_1111/Output_r")
DEFAULT_NC_SOURCES = OUTPUT_R_ROOT / "output_bf/03_merge/merged_all.nc"
DEFAULT_INPUT_DIR_ABS = OUTPUT_R_ROOT / "output_bf"
DEFAULT_S4_CLUSTERED_CSV = OUTPUT_R_ROOT / "output_bf/02_cluster/clustered_stations.csv"
DEFAULT_OUT_PREFIX_ABS = OUTPUT_R_ROOT / "output_bf/s6_plot_sources"


# Nature 期刊风格配色：Wong (Nature Methods) + Paul Tol Bright/Muted，色盲友好、区分度高
SOURCE_COLORS_24 = [
    "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7",  # Wong
    "#4477AA", "#EE6677", "#228833", "#CCBB44", "#66CCEE", "#AA3377", "#BBBBBB",  # Paul Tol Bright
    "#332288", "#88CCEE", "#44AA99", "#117733", "#999933", "#DDCC77", "#CC6677", "#882255", "#AA4499", "#333333",  # Paul Tol Muted + 深灰
]
# 若类别超过 24，用均匀色相补充
def _get_source_cmap(n_cats):
    from matplotlib.colors import ListedColormap
    if n_cats <= len(SOURCE_COLORS_24):
        return ListedColormap(SOURCE_COLORS_24[:n_cats])
    extra = n_cats - len(SOURCE_COLORS_24)
    import matplotlib.colors as mcolors
    base = np.array([mcolors.to_rgba(c) for c in SOURCE_COLORS_24])
    hues = np.linspace(0, 1, extra, endpoint=False)
    for h in hues:
        rgb = mcolors.hsv_to_rgb([h, 0.75, 0.95])
        base = np.vstack([base, [*rgb, 1.0]])
    return ListedColormap(base[:n_cats])


def main():
    ap = argparse.ArgumentParser(description="Plot stations by data source (map + bar)")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC_SOURCES), help="Merged NetCDF path (default: output_bf/03_merge/merged_all.nc)")
    ap.add_argument("--from-csv", "-c", default=None, help="Use pre-computed stats CSV instead of NC")
    ap.add_argument("--input-dir", "-I", default=str(DEFAULT_INPUT_DIR_ABS), help="含 clustered_stations.csv 的目录")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_PREFIX_ABS), help="步骤 s6 出图前缀（将生成 s6_plot_sources_map.png、s6_plot_sources_bar.png、s6_plot_sources.csv）")
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
    n_stations = data["n_stations"]

    indir = Path(args.input_dir).resolve()
    clustered_path = DEFAULT_S4_CLUSTERED_CSV if indir == DEFAULT_INPUT_DIR_ABS else (indir / "clustered_stations.csv").resolve()
    if not clustered_path.is_file():
        print("sclustered_stations.csv not found at {}; exit.".format(clustered_path))
        return

    st = pd.read_csv(clustered_path)
    if "cluster_id" not in st.columns or "source" not in st.columns:
        print("s4_clustered_stations.csv must have columns cluster_id and source; exit.")
        return

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

    if not HAS_MPL:
        print("matplotlib not found. Install with: pip install matplotlib")
    else:
        cmap = _get_source_cmap(n_cats)

        # 图1：站点地图（按来源着色）+ 大陆边界线，图例置于图外不重合
        fig_map = plt.figure(figsize=(12, 6))
        if HAS_CARTOPY:
            ax_map = fig_map.add_subplot(111, projection=ccrs.PlateCarree())
            ax_map.add_feature(cfeature.LAND, facecolor="none", edgecolor="none")
            # 大陆海岸线与国界：深灰色，不画南极
            _edgecolor = (0.35, 0.35, 0.35)
            ax_map.add_feature(cfeature.COASTLINE, linewidth=1.0, edgecolor=_edgecolor)
            ax_map.add_feature(cfeature.BORDERS, linewidth=0.3, linestyle="-", edgecolor=_edgecolor)
            ax_map.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())  # 南纬 60° 截断，不显示南极
            sc4 = ax_map.scatter(lon, lat, c=cidx, s=1.2, cmap=cmap, alpha=0.7, vmin=0, vmax=max(0, n_cats - 1), transform=ccrs.PlateCarree())
        else:
            ax_map = fig_map.add_subplot(111)
            ax_map.set_xlim(-180, 180)
            ax_map.set_ylim(-60, 90)
            sc4 = ax_map.scatter(lon, lat, c=cidx, s=1.2, cmap=cmap, alpha=0.7, vmin=0, vmax=max(0, n_cats - 1))
            ax_map.set_xlabel("Longitude")
            ax_map.set_ylabel("Latitude")
            ax_map.set_aspect("equal")
        ax_map.set_title("Stations by data source (primary)")
        # 图例放在图右侧外部，不与散点重合
        patches = [plt.matplotlib.patches.Patch(color=cmap(i), label=unique_sources[i]) for i in range(n_cats)]
        ax_map.legend(handles=patches, loc="upper left", fontsize=7, ncol=2, bbox_to_anchor=(1.02, 1.0), borderaxespad=0, frameon=True)
        plt.tight_layout(rect=[0, 0, 0.72, 1])
        out_map = (out_prefix.parent / (out_prefix.name + "_map.png")).resolve()
        plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
        plt.close()
        print("Saved: {}".format(out_map))

        # 图2：各来源站点数柱状图
        fig_bar = plt.figure(figsize=(max(8, n_cats * 0.4), 5))
        ax_bar = fig_bar.add_subplot(111)
        counts = [primary_source.count(s) for s in unique_sources]
        ax_bar.bar(range(n_cats), counts, color=[cmap(i) for i in range(n_cats)], edgecolor="white")
        ax_bar.set_xticks(range(n_cats))
        ax_bar.set_xticklabels(unique_sources, rotation=45, ha="right", fontsize=8)
        ax_bar.set_ylabel("Number of stations")
        ax_bar.set_title("Stations per source (primary)")
        plt.tight_layout()
        out_bar = (out_prefix.parent / (out_prefix.name + "_bar.png")).resolve()
        plt.savefig(out_bar, dpi=args.dpi, bbox_inches="tight")
        plt.close()
        print("Saved: {}".format(out_bar))

    src_csv = (out_prefix.parent / (out_prefix.name + ".csv")).resolve()
    with open(src_csv, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["station_index", "lat", "lon", "primary_source", "n_sources", "all_sources"])
        for i in range(n_stations):
            n_src = len(by_cid.get(i, set()))
            w.writerow([i, lat[i], lon[i], primary_source[i], n_src, all_sources_list[i]])
    print("Saved: {}".format(src_csv))


if __name__ == "__main__":
    main()
