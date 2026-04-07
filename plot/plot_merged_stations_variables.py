#!/usr/bin/env python3
"""
步骤 s6 出图之一：Q / SSC / SSL 变量可用性分布（全球地图 + 柱状图）。
输出：s6_plot_variables_map.png、s6_plot_variables_bar.png

组合类型（按论文关注度排序）：
  Q+SSC+SSL  全要素（径流+悬沙浓度+悬沙通量）
  Q+SSC      径流+悬沙浓度（最常见）
  Q+SSL      径流+悬沙通量
  Q          仅径流
  SSC+SSL    悬沙浓度+悬沙通量（无径流）
  SSC        仅悬沙浓度
  SSL        仅悬沙通量
  none       无有效数据

用法：
  python plot/plot_merged_stations_variables.py
  python plot/plot_merged_stations_variables.py --nc /path/to/s6.nc
"""

import argparse
import sys
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR, load_variable_availability, RES_CODES

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

# 组合顺序与颜色
_COMBO_ORDER  = ["Q+SSC+SSL", "Q+SSC", "Q+SSL", "Q", "SSC+SSL", "SSC", "SSL", "none"]
_COMBO_COLORS = {
    "Q+SSC+SSL": "#1565C0",
    "Q+SSC":     "#42A5F5",
    "Q+SSL":     "#00838F",
    "Q":         "#80DEEA",
    "SSC+SSL":   "#E65100",
    "SSC":       "#FFA726",
    "SSL":       "#FFE082",
    "none":      "#BDBDBD",
}


def _draw_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot Q/SSC/SSL availability map and bar chart")
    ap.add_argument("--nc",  "-n", default=str(DEFAULT_NC), help="Merged NetCDF path")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_variables"),
                    help="输出前缀（将生成 _map.png 与 _bar.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    data    = load_variable_availability(nc_path=args.nc)
    lat     = data["lat"]
    lon     = data["lon"]
    combo   = data["combo"]
    res     = data["primary_res"]
    n_stat  = data["n_stations"]

    # 统计各组合数量
    combo_counts = {c: int((combo == c).sum()) for c in _COMBO_ORDER}
    print("Variable availability (n_stations={:,}):".format(n_stat))
    for c in _COMBO_ORDER:
        if combo_counts[c]:
            print("  {:12s}: {:,} ({:.1f}%)".format(
                c, combo_counts[c], combo_counts[c] / n_stat * 100))

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    # ── 图1：全球地图 ──────────────────────────────────────────────────────────
    out_map = out_prefix.parent / (out_prefix.name + "_map.png")
    fig = plt.figure(figsize=(12, 5.5))
    if HAS_CARTOPY:
        ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
        _draw_basemap(ax)
    else:
        ax = fig.add_subplot(111)
        ax.set_xlim(-180, 180); ax.set_ylim(-60, 90)
        ax.set_xlabel("Longitude"); ax.set_ylabel("Latitude")

    # 按组合优先级从低到高绘（高优先级覆盖在上）
    for c in reversed(_COMBO_ORDER):
        mk = (combo == c)
        if not mk.any():
            continue
        color = _COMBO_COLORS[c]
        size  = 5 if c == "none" else 6
        kw    = dict(color=color, s=size, alpha=0.85 if c != "none" else 0.5,
                     label="{} ({:,})".format(c, combo_counts[c]),
                     linewidths=0)
        if HAS_CARTOPY:
            ax.scatter(lon[mk], lat[mk], transform=ccrs.PlateCarree(), **kw)
        else:
            ax.scatter(lon[mk], lat[mk], **kw)

    ax.set_title("Sediment stations — variable availability  (n={:,})".format(n_stat), fontsize=11)
    ax.legend(loc="lower left", fontsize=8, ncol=1,
              bbox_to_anchor=(1.01, 0.0), borderaxespad=0,
              frameon=True, handletextpad=0.4, markerscale=2)
    plt.tight_layout(rect=[0, 0, 0.85, 1])
    plt.savefig(out_map, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_map))

    # ── 图2：柱状图（左：组合数量；右：按分辨率分层）────────────────────────
    out_bar = out_prefix.parent / (out_prefix.name + "_bar.png")
    fig2, (ax_l, ax_r) = plt.subplots(1, 2, figsize=(14, 5))

    # 左图：各组合站点数
    combos_with_data = [c for c in _COMBO_ORDER if combo_counts[c] > 0]
    bar_vals   = [combo_counts[c] for c in combos_with_data]
    bar_colors = [_COMBO_COLORS[c] for c in combos_with_data]
    bars = ax_l.bar(combos_with_data, bar_vals, color=bar_colors,
                    edgecolor="white", alpha=0.9)
    ax_l.bar_label(bars, fontsize=9, padding=2)
    ax_l.set_xticklabels(combos_with_data, rotation=30, ha="right", fontsize=9)
    ax_l.set_ylabel("Number of stations")
    ax_l.set_title("Variable combination  (total={:,})".format(n_stat))

    # 右图：主要组合按分辨率分层柱状图
    # 只展示 Q、Q+SSC、Q+SSC+SSL 三组（最重要的三类）
    top_combos = ["Q", "Q+SSC", "Q+SSC+SSL"]
    top_combos = [c for c in top_combos if combo_counts.get(c, 0) > 0]
    res_labels = [RES_CODES.get(k, str(k)) for k in sorted(RES_CODES)]
    res_colors_list = ["#2196F3", "#4CAF50", "#FF9800", "#9E9E9E"]

    x     = np.arange(len(top_combos))
    width = 0.2
    for j, (res_code, res_label) in enumerate(sorted(RES_CODES.items())):
        sub_counts = []
        for c in top_combos:
            mk = (combo == c) & (res == res_code)
            sub_counts.append(int(mk.sum()))
        if any(sc > 0 for sc in sub_counts):
            ax_r.bar(x + j * width, sub_counts, width,
                     color=res_colors_list[j], edgecolor="white",
                     alpha=0.9, label=res_label)

    ax_r.set_xticks(x + width * 1.5)
    ax_r.set_xticklabels(top_combos, fontsize=10)
    ax_r.set_ylabel("Number of stations")
    ax_r.set_title("Variable availability by temporal resolution")
    ax_r.legend(title="Resolution", fontsize=9)

    plt.tight_layout()
    plt.savefig(out_bar, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_bar))


if __name__ == "__main__":
    main()
