#!/usr/bin/env python3
"""
步骤 s6 出图之一：逐年有效站点数（年度覆盖度）。
输出：s6_plot_yearly_coverage.png

展示内容：
  - 折线图：每一年中拥有至少一条有效记录（Q/SSC/SSL 任意一项非填充）的站点数
  - 按时间类型（daily / monthly / annual / climatology / other）分层堆叠面积图

用法：
  python plot/plot_merged_stations_yearly_coverage.py
  python plot/plot_merged_stations_yearly_coverage.py --nc /path/to/s6.nc
"""

import argparse
import sys
from pathlib import Path

import numpy as np

_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR, load_yearly_coverage, RES_CODES, RES_COLORS

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


def main():
    ap = argparse.ArgumentParser(description="Plot yearly station coverage by resolution")
    ap.add_argument("--nc",  "-n", default=str(DEFAULT_NC), help="Merged NetCDF path")
    ap.add_argument("--out", "-o", default=str(DEFAULT_OUT_DIR / "s6_plot_yearly_coverage.png"),
                    help="输出图像路径")
    ap.add_argument("--dpi", type=int, default=150, help="Figure DPI")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cov = load_yearly_coverage(nc_path=args.nc)
    years  = cov["years"]
    total  = cov["total"]
    by_res = cov["by_res"]
    y_min, y_max = cov["year_range"]

    if len(years) == 0:
        print("No valid records found; skipping yearly coverage plot.")
        return

    print("Year range: {} – {}   peak station count: {:,} (year {})".format(
        y_min, y_max, int(total.max()), int(years[total.argmax()])))

    if not HAS_MPL:
        print("matplotlib not found. pip install matplotlib")
        return

    # ── 堆叠面积图 + 总量折线 ──────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(12, 5))

    res_order  = list(sorted(RES_CODES.keys()))
    labels     = [RES_CODES[k] for k in res_order]
    colors     = [RES_COLORS[k] for k in res_order]
    stack_data = np.vstack([by_res.get(RES_CODES[k], np.zeros(len(years)))
                            for k in res_order])

    ax.stackplot(years, stack_data, labels=labels, colors=colors, alpha=0.75)
    ax.plot(years, total, color="black", lw=1.5, ls="--", label="total", zorder=10)

    # 标注峰值年
    peak_idx = int(total.argmax())
    ax.annotate(
        "peak {:,}\n({})".format(int(total[peak_idx]), int(years[peak_idx])),
        xy=(years[peak_idx], total[peak_idx]),
        xytext=(years[peak_idx] - max(5, (y_max - y_min) * 0.08), total[peak_idx] * 0.85),
        arrowprops=dict(arrowstyle="->", color="black", lw=1.0),
        fontsize=9, color="black",
    )

    ax.set_xlim(y_min, y_max)
    ax.set_ylim(0, total.max() * 1.15)
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of stations with valid records")
    ax.set_title("Annual station coverage — sediment reference dataset", fontsize=12)
    ax.legend(loc="upper left", fontsize=9, framealpha=0.85)

    # 添加次 x 轴刻度（每 10 年）
    decade_ticks = np.arange(int(np.ceil(y_min / 10)) * 10, y_max + 1, 10)
    ax.set_xticks(decade_ticks)
    ax.grid(axis="x", ls=":", lw=0.6, alpha=0.5)
    ax.grid(axis="y", ls=":", lw=0.6, alpha=0.5)

    plt.tight_layout()
    plt.savefig(out_path, dpi=args.dpi, bbox_inches="tight")
    plt.close()
    print("Saved: {}".format(out_path))


if __name__ == "__main__":
    main()
