#!/usr/bin/env python3
"""
步骤 s6 出图之一：从 s8_merged_all.nc 统计站点记录数，出图（地图 + 直方图）。
输出：s6_plot_records.png（默认在 output/）；若从 NC 首次加载会同时生成 s6_plot_stats.csv。
"""

import argparse
import sys
from pathlib import Path

import numpy as np

# s6_plot_common 已移至 scripts/，确保可从父目录导入
_scripts_dir = Path(__file__).resolve().parent.parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))
from s6_plot_common import OUTPUT_R_ROOT, DEFAULT_NC, DEFAULT_OUT_DIR, load_station_stats

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


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


if __name__ == "__main__":
    main()
