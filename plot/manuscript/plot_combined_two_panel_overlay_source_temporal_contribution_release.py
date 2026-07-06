#!/usr/bin/env python3
"""Combine the two overlay figures into a single two-panel figure.

Top panel:        main source spatial-temporal overlay (bars + temporal span)
Bottom panels:    other product source contributions (climatology + satellite)

This script reuses data-loading, normalization and annotation helpers from
plot_source_spatial_temporal_contribution_release.py.
"""

import argparse
import ctypes
import os
import sys
from pathlib import Path
from typing import Tuple

CONDA_LIB = "/share/home/dq134/.conda/envs/wzx/lib"
os.environ["LD_LIBRARY_PATH"] = CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
ctypes.CDLL(str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL)

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.gridspec import GridSpec

# ── Path setup & imports from the original module ────────────────────────
#   The original lives one directory up from this script.
MY_SCRIPT_DIR = Path(__file__).resolve().parent                 # plot/manuscript/
_PLOT_DIR = MY_SCRIPT_DIR.parent                                # plot/
if str(_PLOT_DIR) not in sys.path:
    sys.path.insert(0, str(_PLOT_DIR))

import plot_source_spatial_temporal_contribution_release as _orig

# Re-export constants and helpers from the original
SPATIAL_COLOR       = _orig.SPATIAL_COLOR
TEMPORAL_LINE_COLOR = _orig.TEMPORAL_LINE_COLOR
TEMPORAL_POINT_COLOR= _orig.TEMPORAL_POINT_COLOR
OUTPUT_STEM = "fig_combined_two_panel_overlay_source_temporal_contribution_release"

load_main_sources          = _orig.load_main_sources
load_other_product_sources = _orig.load_other_product_sources
plot_other_product_panel   = _orig.plot_other_product_panel
annotate_cluster_counts_on_twin = _orig.annotate_cluster_counts_on_twin
_set_year_limits           = _orig._set_year_limits

# Default output directory (mirrors the original script's logic)
PROJECT_DIR = MY_SCRIPT_DIR.parent.parent                   # scripts_basin_test/
DEFAULT_STATS_DIR = PROJECT_DIR / "output_other" / "stats_release"
OUTPUT_FIGURES_DIR = DEFAULT_STATS_DIR / "source_spatial_temporal_contribution" / "figures"
DPI = 300

FIGSIZE = (10.8, 15.5)   # wide enough for overlay, tall enough for both panels


# ── Adapted overlay panel (draws on a caller-supplied Axes) ────────────

def _plot_overlay_panel(ax_cluster, df):
    """Draw the source spatial-temporal overlay onto *ax_cluster*.

    Mirrors ``plot_overlay_source_contribution`` from the original module
    but does *not* create the figure, adjust subplots, or draw the legend —
    the caller handles those.
    """
    if df.empty:
        ax_cluster.set_title("No main source data available")
        ax_cluster.set_axis_off()
        return

    y = np.arange(len(df))
    ax_year = ax_cluster.twiny()
    ax_year.patch.set_alpha(0)
    ax_year.set_zorder(ax_cluster.get_zorder() + 1)

    # ── Spatial bars ──
    ax_cluster.barh(
        y, df["cluster_count"],
        color=SPATIAL_COLOR, alpha=0.45, height=0.62, zorder=1,
    )
    ax_cluster.set_yticks(y)
    ax_cluster.set_yticklabels(df["source_name"])
    ax_cluster.set_xlabel("Cluster count", color=SPATIAL_COLOR)
    ax_cluster.tick_params(axis="x", colors=SPATIAL_COLOR)
    ax_cluster.spines["bottom"].set_color(SPATIAL_COLOR)
    ax_cluster.grid(axis="x", linewidth=0.3, alpha=0.45, color=SPATIAL_COLOR)
    ax_cluster.set_axisbelow(True)

    max_cluster = pd.to_numeric(df["cluster_count"], errors="coerce").max()
    if pd.notna(max_cluster) and max_cluster > 0:
        ax_cluster.set_xlim(0, max_cluster * 1.24)

    # ── Temporal overlay ──
    time_df = df.dropna(subset=["first_year", "last_year"]).copy()
    if not time_df.empty:
        for idx, row in time_df.iterrows():
            ax_year.hlines(
                y[idx], row["first_year"], row["last_year"],
                color=TEMPORAL_LINE_COLOR, linewidth=1.7, alpha=0.9, zorder=3,
            )
        ax_year.scatter(
            time_df["last_year"], y[time_df.index],
            s=52, color=TEMPORAL_POINT_COLOR, alpha=0.82,
            edgecolor="white", linewidth=0.5, zorder=4,
        )
        _set_year_limits(ax_year, time_df)

    annotate_cluster_counts_on_twin(ax_cluster, ax_year, df, y)

    ax_year.set_xlabel("Year", color=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="x", colors=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="y", left=False, labelleft=False)
    ax_year.spines["top"].set_color(TEMPORAL_LINE_COLOR)

    ax_cluster.set_title("Source contributions to spatial coverage and temporal span",
                         fontsize=12)


# ── Combined figure ────────────────────────────────────────────────────

def plot_combined_two_panel(main_df, climatology_df, satellite_df) -> Tuple[Path, Path]:
    """Build a single figure with the overlay on top and other-products below."""
    fig = plt.figure(figsize=FIGSIZE)
    gs = GridSpec(
        3, 1, figure=fig,
        height_ratios=[7.0, 3.9, 2.9],
        hspace=0.35,
    )

    # ── Top: overlay panel (main sources) ──
    ax_top = fig.add_subplot(gs[0])
    _plot_overlay_panel(ax_top, main_df)

    # ── Middle: climatology ──
    ax_mid = fig.add_subplot(gs[1])
    plot_other_product_panel(
        ax_mid, climatology_df,
        "Climatology sources", "Station count", "#54a24b",
    )

    # ── Bottom: satellite ──
    ax_bot = fig.add_subplot(gs[2])
    plot_other_product_panel(
        ax_bot, satellite_df,
        "Satellite-validation sources", "Linked cluster count", "#9c755f",
    )

    # ── Single combined legend ──
    legend_handles = [
        Patch(facecolor=SPATIAL_COLOR, alpha=0.45, edgecolor="none",
              label="clusters"),
        Patch(facecolor=SPATIAL_COLOR, alpha=0.72, edgecolor="#2f4f6f",
              linewidth=0.5, label="clusters / records"),
        Patch(facecolor="#54a24b", alpha=0.48, edgecolor="none",
              label="climatology stations"),
        Patch(facecolor="#9c755f", alpha=0.48, edgecolor="none",
              label="satellite linked clusters"),
        Line2D([0], [0], color=TEMPORAL_LINE_COLOR, linewidth=1.7,
               label="temporal span"),
        Line2D([0], [0], marker="o", linestyle="none",
               markerfacecolor=TEMPORAL_POINT_COLOR, markeredgecolor="white",
               markersize=7, label="span end"),
    ]
    fig.legend(
        handles=legend_handles,
        loc="lower center", ncol=6, frameon=False,
        bbox_to_anchor=(0.5, 0.005),
    )

    fig.subplots_adjust(left=0.22, right=0.97, top=0.96, bottom=0.08)

    # ── Save ──
    OUTPUT_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    png_path = OUTPUT_FIGURES_DIR / "{}.png".format(OUTPUT_STEM)
    pdf_path = OUTPUT_FIGURES_DIR / "{}.pdf".format(OUTPUT_STEM)
    fig.savefig(png_path, dpi=DPI, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


# ── CLI ─────────────────────────────────────────────────────────────────

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot combined two-panel overlay figure: "
            "main-source overlay + other-product source contributions."
        ),
    )
    parser.add_argument(
        "--stats-dir",
        default=str(DEFAULT_STATS_DIR),
        help="Root stats_release output directory.  Default: {}".format(DEFAULT_STATS_DIR),
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Figure output directory.  Default: "
            "<stats-dir>/source_spatial_temporal_contribution/figures"
        ),
    )
    parser.add_argument(
        "--dpi", type=int, default=DPI,
        help="PNG output DPI.  Default: {}".format(DPI),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    global OUTPUT_FIGURES_DIR, DPI

    args = parse_args(argv)
    stats_dir = Path(args.stats_dir).expanduser().resolve()
    OUTPUT_FIGURES_DIR = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else stats_dir / "source_spatial_temporal_contribution" / "figures"
    )
    DPI = int(args.dpi)

    merged_df = load_main_sources(stats_dir)
    climatology_df, satellite_df = load_other_product_sources(stats_dir)

    png_path, pdf_path = plot_combined_two_panel(merged_df, climatology_df, satellite_df)
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(pdf_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
