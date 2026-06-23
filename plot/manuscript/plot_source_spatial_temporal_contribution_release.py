#!/usr/bin/env python3
"""Plot release source spatial contribution and temporal span.

This script reads release-only stats tables under output_other/stats_release
and writes manuscript-style source contribution figures. It mirrors
plot_source_spatial_temporal_contribution.py while using the stats_release
table layout.
"""

import argparse
from pathlib import Path
from typing import Tuple
import ctypes
import os

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


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_OTHER_DIR = PROJECT_DIR / "output_other"
DEFAULT_STATS_DIR = OUTPUT_OTHER_DIR / "stats_release"
OUTPUT_FIGURES_DIR = DEFAULT_STATS_DIR / "source_spatial_temporal_contribution" / "figures"
OUTPUT_STEM = "fig_source_spatial_temporal_contribution_release"
OVERLAY_OUTPUT_STEM = "fig_source_spatial_temporal_contribution_overlay_release"
OTHER_PRODUCTS_OUTPUT_STEM = "fig_other_products_source_contribution_overlay_release"

FIGSIZE = (11.5, 6.8)
OVERLAY_FIGSIZE = (10.8, 7.0)
DPI = 300

SPATIAL_COLOR = "#4c78a8"
TEMPORAL_LINE_COLOR = "#555555"
TEMPORAL_POINT_COLOR = "#d95f02"


def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            "Required input not found: {}\n"
            "Run stats_release.run_all_release_stats first, or pass --stats-dir "
            "to an existing release stats output directory.".format(path)
        )
    try:
        return pd.read_csv(path, keep_default_na=False)
    except pd.errors.EmptyDataError:
        raise ValueError(
            "Required input is empty: {}\n"
            "Run stats_release.run_all_release_stats again, or pass --stats-dir "
            "to a complete release stats output directory.".format(path)
        )


def read_csv_optional(path: Path) -> pd.DataFrame:
    """Read a CSV file, returning an empty DataFrame if missing or empty."""
    try:
        if not path.is_file():
            return pd.DataFrame()
        return pd.read_csv(path, keep_default_na=False)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return pd.DataFrame()


def _source_tables_dir(stats_dir: Path) -> Path:
    return Path(stats_dir) / "source_contribution" / "tables"


def _source_table(stats_dir: Path, name: str) -> Path:
    return _source_tables_dir(stats_dir) / name


def _has_nonempty_csv(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 1


def numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def format_count(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
        return "{:,.0f}".format(float(value))
    except Exception:
        return ""


def format_compact_count(value: object) -> str:
    try:
        if pd.isna(value):
            return ""
        value = float(value)
    except Exception:
        return ""
    abs_value = abs(value)
    if abs_value >= 1_000_000:
        return "{:.1f}M".format(value / 1_000_000).replace(".0M", "M")
    if abs_value >= 1_000:
        return "{:.1f}k".format(value / 1_000).replace(".0k", "k")
    return "{:,.0f}".format(value)


def _filter_by_source_type(df: pd.DataFrame, source_type: str, invert: bool = False) -> pd.DataFrame:
    if df.empty or "source_type" not in df.columns:
        return df.copy()
    values = df["source_type"].astype(str).str.strip().str.lower()
    mask = values.eq(source_type.lower())
    if invert:
        mask = ~mask
    return df[mask].copy()


def _normalize_contribution_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    required = {"source_name", "n_clusters", "n_records"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(table_name, ", ".join(missing)))

    df["source_name"] = df["source_name"].astype(str).str.strip()
    df["cluster_count"] = numeric(df, "n_clusters").fillna(0)
    df["spatial_record_count"] = numeric(df, "n_records").fillna(0)
    df = df[df["source_name"].ne("")]
    df = df.sort_values(["cluster_count", "source_name"], ascending=[False, True])
    return df[["source_name", "cluster_count", "spatial_record_count"]].reset_index(drop=True)


def _normalize_temporal_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    required = {"source_name", "first_year", "last_year", "n_records"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(table_name, ", ".join(missing)))

    df["source_name"] = df["source_name"].astype(str).str.strip()
    df["first_year"] = numeric(df, "first_year")
    df["last_year"] = numeric(df, "last_year")
    df["n_records"] = numeric(df, "n_records").fillna(0)
    df = df[df["source_name"].ne("")]
    df = df.dropna(subset=["first_year", "last_year"])

    if df.empty:
        return pd.DataFrame(columns=["source_name", "first_year", "last_year", "temporal_record_count"])

    grouped = (
        df.groupby("source_name", as_index=False)
        .agg(
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            temporal_record_count=("n_records", "sum"),
        )
        .sort_values("source_name")
    )
    return grouped


def _normalize_other_product_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    required = {"source_name", "n_clusters", "n_records", "first_year", "last_year"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(table_name, ", ".join(missing)))
    out = pd.DataFrame(
        {
            "source_name": df["source_name"].astype(str).str.strip(),
            "contribution_count": numeric(df, "n_clusters").fillna(0),
            "record_count": numeric(df, "n_records").fillna(0),
            "first_year": numeric(df, "first_year"),
            "last_year": numeric(df, "last_year"),
        }
    )
    out = out[out["source_name"].ne("")]
    return out.sort_values(["contribution_count", "source_name"], ascending=[True, False]).reset_index(drop=True)


def _read_main_source_tables(stats_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    dataset_path = _source_table(stats_dir, "table_main_source_dataset_contribution.csv")
    temporal_path = _source_table(stats_dir, "table_main_source_temporal_coverage.csv")
    if _has_nonempty_csv(dataset_path) and _has_nonempty_csv(temporal_path):
        return read_csv_required(dataset_path), read_csv_required(temporal_path)

    merged_dataset_path = _source_table(stats_dir, "table_source_dataset_contribution.csv")
    merged_temporal_path = _source_table(stats_dir, "table_source_temporal_coverage.csv")
    dataset = _filter_by_source_type(read_csv_required(merged_dataset_path), "satellite", invert=True)
    temporal = _filter_by_source_type(read_csv_required(merged_temporal_path), "satellite", invert=True)
    return dataset, temporal


def merge_source_contributions(spatial_df: pd.DataFrame, temporal_df: pd.DataFrame) -> pd.DataFrame:
    merged = spatial_df.merge(temporal_df, on="source_name", how="left")
    merged = merged.sort_values(["cluster_count", "source_name"], ascending=[True, False]).reset_index(drop=True)
    return merged


def load_main_sources(stats_dir: Path) -> pd.DataFrame:
    dataset, temporal = _read_main_source_tables(stats_dir)
    spatial_df = _normalize_contribution_table(dataset, "main source dataset contribution")
    temporal_df = _normalize_temporal_table(temporal, "main source temporal coverage")
    return merge_source_contributions(spatial_df, temporal_df)


def load_other_product_sources(stats_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    merged_dataset_path = _source_table(stats_dir, "table_source_dataset_contribution.csv")
    merged_temporal_path = _source_table(stats_dir, "table_source_temporal_coverage.csv")
    merged_dataset = read_csv_optional(merged_dataset_path)
    merged_temporal = read_csv_optional(merged_temporal_path)

    empty = pd.DataFrame(columns=["source_name", "contribution_count", "record_count", "first_year", "last_year"])
    climatology = empty.copy()
    if not merged_dataset.empty and not merged_temporal.empty:
        clim_dataset = _filter_by_source_type(merged_dataset, "climatology")
        clim_temporal = _filter_by_source_type(merged_temporal, "climatology")
        if not clim_dataset.empty and not clim_temporal.empty:
            climatology = _normalize_other_product_table(
                clim_dataset.merge(
                    clim_temporal[["source_name", "first_year", "last_year", "n_records"]],
                    on="source_name",
                    how="left",
                    suffixes=("", "_temporal"),
                ),
                "climatology source contribution",
            )

    sat_dataset_path = _source_table(stats_dir, "table_sat_source_dataset_contribution.csv")
    sat_temporal_path = _source_table(stats_dir, "table_sat_source_temporal_coverage.csv")
    if _has_nonempty_csv(sat_dataset_path) and _has_nonempty_csv(sat_temporal_path):
        sat_dataset = read_csv_required(sat_dataset_path)
        sat_temporal = read_csv_required(sat_temporal_path)
    elif not merged_dataset.empty and not merged_temporal.empty:
        sat_dataset = _filter_by_source_type(merged_dataset, "satellite")
        sat_temporal = _filter_by_source_type(merged_temporal, "satellite")
    else:
        sat_dataset = pd.DataFrame()
        sat_temporal = pd.DataFrame()

    satellite = empty.copy()
    if not sat_dataset.empty and not sat_temporal.empty:
        satellite = _normalize_other_product_table(
            sat_dataset.merge(
                sat_temporal[["source_name", "first_year", "last_year", "n_records"]],
                on="source_name",
                how="left",
                suffixes=("", "_temporal"),
            ),
            "satellite source contribution",
        )
    return climatology, satellite


def _temporal_point_sizes(record_counts: pd.Series) -> pd.Series:
    counts = pd.to_numeric(record_counts, errors="coerce").fillna(0).clip(lower=0)
    if counts.max() <= 0:
        return pd.Series([45.0] * len(counts), index=counts.index)
    scaled = np.log10(counts.clip(lower=1)) / max(1.0, np.log10(counts.clip(lower=1).max()))
    return 35 + 105 * scaled


def annotate_cluster_counts(ax, df: pd.DataFrame, y: np.ndarray, pad_fraction: float = 0.012) -> None:
    max_cluster = pd.to_numeric(df["cluster_count"], errors="coerce").max()
    if pd.isna(max_cluster) or max_cluster <= 0:
        return
    pad = max_cluster * pad_fraction
    has_temporal = "temporal_record_count" in df.columns
    for i, (ypos, value) in enumerate(zip(y, df["cluster_count"])):
        if pd.isna(value):
            continue
        label = format_count(value)
        if has_temporal:
            t_count = df.iloc[i].get("temporal_record_count")
            if pd.notna(t_count) and float(t_count) > 0:
                label += " / " + format_compact_count(t_count)
        ax.text(
            float(value) + pad,
            ypos,
            label,
            va="center",
            ha="left",
            fontsize=8.5,
            color="#2f4f6f",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.72, "pad": 0.4},
        )


def annotate_temporal_records(ax, time_df: pd.DataFrame, y: np.ndarray, year_pad: float = 0.8) -> None:
    for idx, row in time_df.iterrows():
        label = format_compact_count(row.get("temporal_record_count"))
        if not label:
            continue
        ax.text(
            float(row["last_year"]) + year_pad,
            y[idx],
            label,
            va="center",
            ha="left",
            fontsize=8.5,
            color=TEMPORAL_POINT_COLOR,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.72, "pad": 0.4},
        )


def annotate_cluster_counts_on_twin(
    source_ax,
    label_ax,
    df: pd.DataFrame,
    y: np.ndarray,
    pad_fraction: float = 0.012,
) -> None:
    max_cluster = pd.to_numeric(df["cluster_count"], errors="coerce").max()
    if pd.isna(max_cluster) or max_cluster <= 0:
        return
    pad = max_cluster * pad_fraction
    has_temporal = "temporal_record_count" in df.columns
    for i, (ypos, value) in enumerate(zip(y, df["cluster_count"])):
        if pd.isna(value):
            continue
        label = format_count(value)
        if has_temporal:
            t_count = df.iloc[i].get("temporal_record_count")
            if pd.notna(t_count) and float(t_count) > 0:
                label += " / " + format_compact_count(t_count)
        display_xy = source_ax.transData.transform((float(value) + pad, ypos))
        label_x, label_y = label_ax.transData.inverted().transform(display_xy)
        label_ax.text(
            label_x,
            label_y,
            label,
            va="center",
            ha="left",
            fontsize=8.5,
            color="#2f4f6f",
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.92, "pad": 0.4},
            zorder=6,
        )


def plot_other_product_panel(
    ax_count,
    df: pd.DataFrame,
    title: str,
    count_xlabel: str,
    bar_color: str,
) -> None:
    if df.empty:
        ax_count.set_title(title)
        ax_count.set_axis_off()
        return

    y = np.arange(len(df))
    ax_year = ax_count.twiny()
    ax_year.patch.set_alpha(0)
    ax_year.set_zorder(ax_count.get_zorder() + 1)

    ax_count.barh(
        y,
        df["contribution_count"],
        color=bar_color,
        alpha=0.48,
        height=0.62,
        zorder=1,
    )
    ax_count.set_yticks(y)
    ax_count.set_yticklabels(df["source_name"])
    ax_count.set_xlabel(count_xlabel, color=bar_color)
    ax_count.tick_params(axis="x", colors=bar_color)
    ax_count.spines["bottom"].set_color(bar_color)
    ax_count.grid(axis="x", linewidth=0.3, alpha=0.42, color=bar_color)
    ax_count.set_axisbelow(True)

    max_count = pd.to_numeric(df["contribution_count"], errors="coerce").max()
    if pd.notna(max_count) and max_count > 0:
        ax_count.set_xlim(0, max_count * 1.22)

    time_df = df.dropna(subset=["first_year", "last_year"]).copy()
    if not time_df.empty:
        for idx, row in time_df.iterrows():
            ax_year.hlines(
                y[idx],
                row["first_year"],
                row["last_year"],
                color=TEMPORAL_LINE_COLOR,
                linewidth=1.7,
                alpha=0.9,
                zorder=3,
            )
        ax_year.scatter(
            time_df["last_year"],
            y[time_df.index],
            s=52,
            color=TEMPORAL_POINT_COLOR,
            alpha=0.82,
            edgecolor="white",
            linewidth=0.5,
            zorder=4,
        )
        _set_year_limits(ax_year, time_df)

    ax_year.set_xlabel("Year", color=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="x", colors=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="y", left=False, labelleft=False)
    ax_year.spines["top"].set_color(TEMPORAL_LINE_COLOR)

    label_pad = max_count * 0.012 if pd.notna(max_count) and max_count > 0 else 0.0
    for i, (ypos, value) in enumerate(zip(y, df["contribution_count"])):
        if pd.isna(value):
            continue
        label = format_count(value)
        r_count = df.iloc[i].get("record_count")
        if pd.notna(r_count) and float(r_count) > 0:
            label += " / " + format_compact_count(r_count)
        if pd.notna(max_count) and max_count > 0 and float(value) >= max_count * 0.22:
            label_value = float(value) - label_pad
            ha = "right"
            color = "white"
            alpha = 0.08
        else:
            label_value = float(value) + label_pad
            ha = "left"
            color = "#2f4f6f"
            alpha = 0.92
        display_xy = ax_count.transData.transform((label_value, ypos))
        label_x, label_y = ax_year.transData.inverted().transform(display_xy)
        ax_year.text(
            label_x,
            label_y,
            label,
            va="center",
            ha=ha,
            fontsize=8.5,
            color=color,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": alpha, "pad": 0.4},
            zorder=6,
        )

    ax_count.set_title(title, loc="left", fontsize=11)


def plot_combined_source_contribution(df: pd.DataFrame) -> Tuple[Path, Path]:
    if df.empty:
        raise ValueError("No source rows available for plotting.")

    y = np.arange(len(df))
    fig, (ax_spatial, ax_time) = plt.subplots(
        1,
        2,
        figsize=FIGSIZE,
        sharey=True,
        gridspec_kw={"width_ratios": [1.0, 1.55], "wspace": 0.08},
    )

    ax_spatial.barh(y, df["cluster_count"], color=SPATIAL_COLOR, alpha=0.9)
    ax_spatial.set_yticks(y)
    ax_spatial.set_yticklabels(df["source_name"])
    ax_spatial.set_xlabel("Cluster count")
    ax_spatial.set_title("Spatial contribution")
    ax_spatial.grid(axis="x", linewidth=0.3, alpha=0.55)
    ax_spatial.set_axisbelow(True)

    max_cluster = pd.to_numeric(df["cluster_count"], errors="coerce").max()
    if pd.notna(max_cluster) and max_cluster > 0:
        ax_spatial.set_xlim(0, max_cluster * 1.22)
    annotate_cluster_counts(ax_spatial, df, y)

    time_df = df.dropna(subset=["first_year", "last_year"]).copy()
    if not time_df.empty:
        for idx, row in time_df.iterrows():
            ax_time.hlines(
                y[idx],
                row["first_year"],
                row["last_year"],
                color=TEMPORAL_LINE_COLOR,
                linewidth=1.5,
                alpha=0.88,
            )
        ax_time.scatter(
            time_df["last_year"],
            y[time_df.index],
            s=48,
            color=TEMPORAL_POINT_COLOR,
            alpha=0.78,
            edgecolor="white",
            linewidth=0.5,
            zorder=3,
        )
        year_min = int(np.floor(time_df["first_year"].min() / 10.0) * 10)
        year_max = int(np.ceil(time_df["last_year"].max() / 10.0) * 10)
        ax_time.set_xlim(year_min - 2, year_max + 9)

    ax_time.set_xlabel("Year")
    ax_time.set_title("Temporal span")
    ax_time.grid(axis="x", linewidth=0.3, alpha=0.55)
    ax_time.set_axisbelow(True)
    ax_time.tick_params(axis="y", left=False, labelleft=False)

    fig.suptitle("Source contributions to spatial coverage and temporal span", y=0.98)
    legend_handles = [
        Patch(facecolor=SPATIAL_COLOR, edgecolor="none", label="clusters"),
        Patch(facecolor=SPATIAL_COLOR, edgecolor="#2f4f6f", linewidth=0.5, label="clusters / records"),
        Line2D([0], [0], color=TEMPORAL_LINE_COLOR, linewidth=1.5, label="temporal span"),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="none",
            markerfacecolor=TEMPORAL_POINT_COLOR,
            markeredgecolor="white",
            markersize=7,
            label="span end",
        ),
    ]
    fig.legend(
        handles=legend_handles,
        loc="lower center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.56, 0.005),
    )
    fig.subplots_adjust(left=0.19, right=0.98, top=0.88, bottom=0.14)

    OUTPUT_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    png_path = OUTPUT_FIGURES_DIR / "{}.png".format(OUTPUT_STEM)
    pdf_path = OUTPUT_FIGURES_DIR / "{}.pdf".format(OUTPUT_STEM)
    fig.savefig(png_path, dpi=DPI, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def _set_year_limits(ax, time_df: pd.DataFrame) -> None:
    if time_df.empty:
        return
    year_min = int(np.floor(time_df["first_year"].min() / 10.0) * 10)
    year_max = int(np.ceil(time_df["last_year"].max() / 10.0) * 10)
    ax.set_xlim(year_min - 2, year_max + 9)


def plot_overlay_source_contribution(df: pd.DataFrame) -> Tuple[Path, Path]:
    if df.empty:
        raise ValueError("No source rows available for plotting.")

    y = np.arange(len(df))
    fig, ax_cluster = plt.subplots(figsize=OVERLAY_FIGSIZE)
    ax_year = ax_cluster.twiny()

    # Keep the year axis transparent so both layers share one visual panel.
    ax_year.patch.set_alpha(0)
    ax_year.set_zorder(ax_cluster.get_zorder() + 1)

    ax_cluster.barh(
        y,
        df["cluster_count"],
        color=SPATIAL_COLOR,
        alpha=0.45,
        height=0.62,
        zorder=1,
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

    time_df = df.dropna(subset=["first_year", "last_year"]).copy()
    if not time_df.empty:
        for idx, row in time_df.iterrows():
            ax_year.hlines(
                y[idx],
                row["first_year"],
                row["last_year"],
                color=TEMPORAL_LINE_COLOR,
                linewidth=1.7,
                alpha=0.9,
                zorder=3,
            )
        ax_year.scatter(
            time_df["last_year"],
            y[time_df.index],
            s=52,
            color=TEMPORAL_POINT_COLOR,
            alpha=0.82,
            edgecolor="white",
            linewidth=0.5,
            zorder=4,
        )
        _set_year_limits(ax_year, time_df)
    annotate_cluster_counts_on_twin(ax_cluster, ax_year, df, y)

    ax_year.set_xlabel("Year", color=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="x", colors=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="y", left=False, labelleft=False)
    ax_year.spines["top"].set_color(TEMPORAL_LINE_COLOR)

    ax_cluster.set_title("Source contributions to spatial coverage and temporal span")
    legend_handles = [
        Patch(facecolor=SPATIAL_COLOR, alpha=0.45, edgecolor="none", label="clusters"),
        Patch(facecolor=SPATIAL_COLOR, alpha=0.72, edgecolor="#2f4f6f", linewidth=0.5, label="clusters / records"),
        Line2D([0], [0], color=TEMPORAL_LINE_COLOR, linewidth=1.7, label="temporal span"),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="none",
            markerfacecolor=TEMPORAL_POINT_COLOR,
            markeredgecolor="white",
            markersize=7,
            label="span end",
        ),
    ]
    fig.legend(
        handles=legend_handles,
        loc="lower center",
        ncol=4,
        frameon=False,
        bbox_to_anchor=(0.58, 0.01),
    )
    fig.subplots_adjust(left=0.22, right=0.97, top=0.86, bottom=0.18)

    OUTPUT_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    png_path = OUTPUT_FIGURES_DIR / "{}.png".format(OVERLAY_OUTPUT_STEM)
    pdf_path = OUTPUT_FIGURES_DIR / "{}.pdf".format(OVERLAY_OUTPUT_STEM)
    fig.savefig(png_path, dpi=DPI, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def plot_other_products_source_contribution(climatology: pd.DataFrame, satellite: pd.DataFrame) -> Tuple[Path, Path]:
    fig, axes = plt.subplots(
        2,
        1,
        figsize=(10.8, 6.8),
        gridspec_kw={"height_ratios": [1.35, 1.0], "hspace": 0.55},
    )
    plot_other_product_panel(
        axes[0],
        climatology,
        "Climatology sources",
        "Station count",
        "#54a24b",
    )
    plot_other_product_panel(
        axes[1],
        satellite,
        "Satellite-validation sources",
        "Linked cluster count",
        "#9c755f",
    )

    fig.suptitle("Other product source contributions and temporal span", y=0.98)
    legend_handles = [
        Patch(facecolor="#54a24b", alpha=0.48, edgecolor="none", label="climatology stations"),
        Patch(facecolor="#9c755f", alpha=0.48, edgecolor="none", label="satellite linked clusters"),
        Patch(facecolor=SPATIAL_COLOR, alpha=0.48, edgecolor="#2f4f6f", linewidth=0.5, label="counts / records"),
        Line2D([0], [0], color=TEMPORAL_LINE_COLOR, linewidth=1.7, label="temporal span"),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="none",
            markerfacecolor=TEMPORAL_POINT_COLOR,
            markeredgecolor="white",
            markersize=7,
            label="span end",
        ),
    ]
    fig.legend(
        handles=legend_handles,
        loc="lower center",
        ncol=5,
        frameon=False,
        bbox_to_anchor=(0.56, 0.0),
    )
    fig.subplots_adjust(left=0.2, right=0.97, top=0.87, bottom=0.15)

    OUTPUT_FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    png_path = OUTPUT_FIGURES_DIR / "{}.png".format(OTHER_PRODUCTS_OUTPUT_STEM)
    pdf_path = OUTPUT_FIGURES_DIR / "{}.pdf".format(OTHER_PRODUCTS_OUTPUT_STEM)
    fig.savefig(png_path, dpi=DPI, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot source spatial-temporal contribution figures from stats_release outputs."
    )
    parser.add_argument(
        "--stats-dir",
        default=str(DEFAULT_STATS_DIR),
        help="Root stats_release output directory. Default: {}".format(DEFAULT_STATS_DIR),
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Figure output directory. Default: "
            "<stats-dir>/source_spatial_temporal_contribution/figures"
        ),
    )
    parser.add_argument("--dpi", type=int, default=DPI, help="PNG output DPI. Default: {}".format(DPI))
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

    merged = load_main_sources(stats_dir)
    climatology_df, satellite_df = load_other_product_sources(stats_dir)
    png_path, pdf_path = plot_combined_source_contribution(merged)
    overlay_png_path, overlay_pdf_path = plot_overlay_source_contribution(merged)
    other_png_path, other_pdf_path = plot_other_products_source_contribution(climatology_df, satellite_df)
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(pdf_path))
    print("Wrote {}".format(overlay_png_path))
    print("Wrote {}".format(overlay_pdf_path))
    print("Wrote {}".format(other_png_path))
    print("Wrote {}".format(other_pdf_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
