#!/usr/bin/env python3
"""
步骤 s6 出图之一：流域特征分布（控制流域面积、Pfafstetter 大区、匹配质量）。
输出：
  s6_plot_basin_area.png    — 集水面积对数分布直方图 + 全球地图
  s6_plot_basin_region.png  — Pfafstetter 一级大区站点数柱状图
  s6_plot_basin_quality.png — 流域匹配质量分布

用法：
  python plot/plot_merged_stations_basin.py
  python plot/plot_merged_stations_basin.py --nc /path/to/s6.nc
"""

import argparse
import sys
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR, FILL_F, annotate_bars, load_basin_stats

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

# Pfafstetter 一级大区标签（首位数字 1–9）
_PFAF1_LABELS = {
    1: "S. America\n(Amazon/Orinoco)",
    2: "Africa\n(Atlantic)",
    3: "Africa\n(Indian Ocean)",
    4: "Europe",
    5: "N. Asia\n(Arctic)",
    6: "Central Asia",
    7: "S./SE. Asia",
    8: "Oceania",
    9: "N. America",
}
_PFAF1_COLORS = [
    "#1E88E5", "#43A047", "#FB8C00", "#8E24AA",
    "#00ACC1", "#F4511E", "#E53935", "#6D4C41", "#FDD835",
]

# 匹配质量标签（与当前 s6 basin_match_quality 保持一致）
_QUAL_DISPLAY = {
    -1: "unknown / N.A.",
     0: "distance only",
     1: "area matched",
     2: "failed",
}
_QUAL_COLORS = {
    -1: "#BDBDBD",
     0: "#43A047",
     1: "#1E88E5",
     2: "#E53935",
}


def _draw_basemap(ax):
    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=_EDGE_COLOR)
    ax.add_feature(cfeature.BORDERS,   linewidth=0.3, linestyle="-", edgecolor=_EDGE_COLOR)
    ax.set_extent([-180, 180, -60, 90], crs=ccrs.PlateCarree())


def main():
    ap = argparse.ArgumentParser(description="Plot basin area / Pfaf region / match quality")
    ap.add_argument("--nc",  "-n", default=str(DEFAULT_NC), help="Merged NetCDF path")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_basin"),
                    help="输出前缀（将生成 _area.png / _region.png / _quality.png）")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_prefix = Path(args.out)
    if out_prefix.suffix.lower() in (".png", ".jpg", ".pdf"):
        out_prefix = out_prefix.with_suffix("")
    out_prefix.parent.mkdir(parents=True, exist_ok=True)

    data       = load_basin_stats(nc_path=args.nc)
    lat        = data["lat"]
    lon        = data["lon"]
    area       = data["basin_area"]
    pfaf       = data["pfaf_code"]
    match_qual = data["match_quality"]
    qual_label_map = data.get("match_quality_labels", {})
    n_stat     = data["n_stations"]

    thr = FILL_F * 0.5
    valid_area = (area > thr) & np.isfinite(area)
    valid_pfaf = (pfaf > thr) & np.isfinite(pfaf)

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    # ── 图1：流域面积地图 + 直方图 ────────────────────────────────────────────
    out_area = out_prefix.parent / (out_prefix.name + "_area.png")
    if valid_area.any():
        area_v = area[valid_area]
        print("Basin area (km²): n={:,}  min={:.0f}  max={:.0f}  median={:.0f}".format(
            int(valid_area.sum()), area_v.min(), area_v.max(), np.median(area_v)))

        fig, (ax_map, ax_hist) = plt.subplots(1, 2, figsize=(15, 5),
                                               gridspec_kw={"width_ratios": [2, 1]})
        log_area = np.log10(np.clip(area, 1, None))

        # 地图
        if HAS_CARTOPY:
            from mpl_toolkits.axes_grid1.inset_locator import inset_axes
            ax_map_c = plt.subplot(121, projection=ccrs.PlateCarree())
            _draw_basemap(ax_map_c)
            sc = ax_map_c.scatter(
                lon[valid_area], lat[valid_area],
                c=log_area[valid_area], s=5, cmap="YlOrRd",
                alpha=0.85, transform=ccrs.PlateCarree(),
            )
            ax_map_c.set_title("Basin area  (n={:,} with data)".format(int(valid_area.sum())))
            cb = fig.colorbar(sc, ax=ax_map_c, label="log₁₀(Area / km²)",
                              shrink=0.7, aspect=25, pad=0.04, fraction=0.022)
            cb.ax.tick_params(labelsize=8)
            tks = [2, 3, 4, 5, 6, 7]
            cb.set_ticks(tks)
            cb.set_ticklabels(["100", "1k", "10k", "100k", "1M", "10M"])
            ax_map.remove()
        else:
            sc = ax_map.scatter(
                lon[valid_area], lat[valid_area],
                c=log_area[valid_area], s=5, cmap="YlOrRd", alpha=0.85,
            )
            ax_map.set_xlim(-180, 180); ax_map.set_ylim(-60, 90)
            ax_map.set_xlabel("Longitude"); ax_map.set_ylabel("Latitude")
            ax_map.set_title("Basin area  (n={:,} with data)".format(int(valid_area.sum())))
            fig.colorbar(sc, ax=ax_map, label="log₁₀(Area / km²)", shrink=0.8)

        # 直方图
        ax_hist.hist(np.log10(area_v), bins=50,
                     color="#FF7043", edgecolor="white", alpha=0.85)
        ax_hist.axvline(np.log10(np.median(area_v)), color="black", ls="--", lw=1.5,
                        label="median = {:.0f} km²".format(np.median(area_v)))
        ax_hist.set_xlabel("log₁₀(Basin area / km²)")
        ax_hist.set_ylabel("Number of stations")
        ax_hist.set_title("Basin area distribution")
        # 自定义 x 刻度
        x_tks = [2, 3, 4, 5, 6, 7]
        ax_hist.set_xticks(x_tks)
        ax_hist.set_xticklabels(["100", "1k", "10k", "100k", "1M", "10M"])
        ax_hist.legend(fontsize=9)

        plt.tight_layout()
        plt.savefig(out_area, dpi=args.dpi, bbox_inches="tight")
        plt.close()
        print("Saved: {}".format(out_area))
    else:
        print("No valid basin_area values found; skipping area plot.")

    # ── 图2：Pfafstetter 一级大区柱状图 ──────────────────────────────────────
    out_region = out_prefix.parent / (out_prefix.name + "_region.png")
    if valid_pfaf.any():
        pfaf_v   = pfaf[valid_pfaf]
        pfaf_int = pfaf_v.astype(np.int64)
        # 获取首位数字（Pfaf level-1）
        n_digits  = np.floor(np.log10(np.maximum(pfaf_int, 1))).astype(int)
        pfaf1     = (pfaf_int // (10 ** (n_digits - 1 + (n_digits == 0)))).astype(int)
        pfaf1     = np.clip(pfaf1, 1, 9)

        counts_by_region = [(d, int((pfaf1 == d).sum())) for d in range(1, 10)]
        counts_by_region = [(d, c) for d, c in counts_by_region if c > 0]

        fig3, ax3 = plt.subplots(figsize=(10, 5))
        xlabels = [_PFAF1_LABELS.get(d, "Region {}".format(d)) for d, _ in counts_by_region]
        vals    = [c for _, c in counts_by_region]
        colors3 = [_PFAF1_COLORS[(d - 1) % len(_PFAF1_COLORS)] for d, _ in counts_by_region]
        bars3   = ax3.bar(range(len(counts_by_region)), vals,
                          color=colors3, edgecolor="white", alpha=0.9)
        annotate_bars(ax3, bars3, fontsize=9, padding=2)
        ax3.set_xticks(range(len(counts_by_region)))
        ax3.set_xticklabels(xlabels, rotation=0, ha="center", fontsize=9)
        ax3.set_ylabel("Number of stations")
        ax3.set_title("Stations by Pfafstetter level-1 region  (n={:,} with pfaf code)".format(
            int(valid_pfaf.sum())))
        plt.tight_layout()
        plt.savefig(out_region, dpi=args.dpi, bbox_inches="tight")
        plt.close()
        print("Saved: {}".format(out_region))
    else:
        print("No valid pfaf_code values found; skipping region plot.")

    # ── 图3：流域匹配质量 ─────────────────────────────────────────────────────
    out_qual = out_prefix.parent / (out_prefix.name + "_quality.png")
    qual_int = match_qual.astype(int)
    qual_keys = sorted(set(list(_QUAL_DISPLAY.keys()) + qual_int.tolist()))
    qual_counts = [(q, int((qual_int == q).sum())) for q in qual_keys]
    qual_counts = [(q, c) for q, c in qual_counts if c > 0]

    if qual_counts:
        fig4, ax4 = plt.subplots(figsize=(8, 4))
        xlabels4 = []
        for q, _ in qual_counts:
            raw_label = qual_label_map.get(q, "")
            if raw_label:
                xlabels4.append(raw_label.replace("_", " "))
            else:
                xlabels4.append(_QUAL_DISPLAY.get(q, str(q)))
        vals4    = [c for _, c in qual_counts]
        colors4  = [_QUAL_COLORS.get(q, "#9E9E9E") for q, _ in qual_counts]
        bars4    = ax4.bar(range(len(qual_counts)), vals4,
                           color=colors4, edgecolor="white", alpha=0.9)
        annotate_bars(ax4, bars4, fontsize=9, padding=2)
        ax4.set_xticks(range(len(qual_counts)))
        ax4.set_xticklabels(xlabels4, rotation=15, ha="right", fontsize=9)
        ax4.set_ylabel("Number of stations")
        ax4.set_title("Basin match quality  (n={:,})".format(n_stat))
        plt.tight_layout()
        plt.savefig(out_qual, dpi=args.dpi, bbox_inches="tight")
        plt.close()
        print("Saved: {}".format(out_qual))
    else:
        print("No match quality data found; skipping quality plot.")


if __name__ == "__main__":
    main()
