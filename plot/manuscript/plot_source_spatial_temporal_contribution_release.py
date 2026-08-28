#!/usr/bin/env python3
"""Plot release source spatial contribution and temporal span.

This script reads release catalog tables under output/sed_reference_release_minimal
and writes manuscript-style source contribution figures.
"""

import argparse
import datetime
from pathlib import Path
import shutil
import subprocess
from typing import Dict, List, Tuple
import ctypes
import os

CONDA_LIB = os.environ.get("SED_CONDA_LIB", "")
if CONDA_LIB and os.path.isdir(CONDA_LIB):
    os.environ["LD_LIBRARY_PATH"] = CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
    try:
        ctypes.CDLL(str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL)
    except Exception:
        pass

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent.parent
DEFAULT_RELEASE_DIR = PROJECT_DIR / "output" / "sed_reference_release_minimal"
OVERLAY_OUTPUT_STEM = "fig_source_spatial_temporal_contribution_overlay_release"
OTHER_PRODUCTS_OUTPUT_STEM = "fig_other_products_source_contribution_overlay_release"

DEFAULT_OUTPUT_DIR = PROJECT_DIR / "figures"

OVERLAY_FIGSIZE = (10.8, 7.0)
DPI = 300

SPATIAL_COLOR = "#0072B2"
TEMPORAL_LINE_COLOR = "#555555"
TEMPORAL_POINT_COLOR = "#E69F00"

OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
}


def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            "Required input not found: {}\n"
            "Expected the built-in sed_reference_release_minimal directory to be complete.".format(path)
        )
    try:
        return pd.read_csv(path, keep_default_na=False)
    except pd.errors.EmptyDataError:
        raise ValueError(
            "Required input is empty: {}\n"
            "Expected the built-in sed_reference_release_minimal directory to be complete.".format(path)
        )


def configure_matplotlib(plt) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "axes.unicode_minus": False,
        }
    )


def ensure_figure_dirs(figures_root: Path) -> Dict[str, Path]:
    root = Path(figures_root).resolve()
    dirs = {
        "root": root,
        "final": root / "final",
        "data": root / "data",
        "scripts": root / "scripts",
        "checklists": root / "checklists",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def run_text_command(cmd: List[str]) -> Tuple[bool, str]:
    try:
        result = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    except FileNotFoundError:
        return False, "{} unavailable".format(cmd[0])
    return result.returncode == 0, result.stdout.strip()


def pdf_page_size(pdfinfo_output: str) -> str:
    for line in pdfinfo_output.splitlines():
        if line.startswith("Page size:"):
            return line.split(":", 1)[1].strip()
    return "not found in pdfinfo output"


def font_embedding_status(pdffonts_output: str) -> str:
    lines = pdffonts_output.splitlines()
    if len(lines) < 3:
        return "no fonts reported by pdffonts"
    header = lines[0]
    if "emb" not in header or "sub" not in header:
        return "checked with pdffonts; review raw output"
    emb_start = header.index("emb")
    sub_start = header.index("sub")
    values = [line[emb_start:sub_start].strip().lower() for line in lines[2:] if line.strip()]
    if values and all(value == "yes" for value in values):
        return "all reported fonts embedded"
    if values:
        return "some reported fonts may not be embedded; review pdffonts output"
    return "no fonts reported by pdffonts"


def file_size_mb(path: Path) -> str:
    if not path.is_file():
        return "not found"
    return "{:.2f} MB".format(path.stat().st_size / (1024 * 1024))


def _write_csv(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def write_plotting_data(
    data_dir: Path,
    figure_id: str,
    merged_df: pd.DataFrame,
    climatology_df: pd.DataFrame,
    satellite_df: pd.DataFrame,
) -> List[Path]:
    outputs = []
    if not merged_df.empty:
        outputs.append(_write_csv(merged_df, data_dir / "{}_main_source_data.csv".format(figure_id)))
    if not climatology_df.empty:
        outputs.append(_write_csv(climatology_df, data_dir / "{}_climatology_data.csv".format(figure_id)))
    if not satellite_df.empty:
        outputs.append(_write_csv(satellite_df, data_dir / "{}_satellite_data.csv".format(figure_id)))
    return outputs


def write_figure_checklist(
    checklist_path: Path,
    figure_id: str,
    pdf_path: Path,
    png_path: Path,
    data_paths: List[Path],
    script_copy_path: Path,
    dpi: int,
    figsize: Tuple[float, float],
    is_multi_panel: bool,
    panel_labels: str,
) -> Path:
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(["pdffonts", str(pdf_path)])
    width_cm = figsize[0] * 2.54
    height_cm = figsize[1] * 2.54
    clines = [
        "# {} ESSD figure checklist".format(figure_id),
        "",
        "- Final PDF: `{}`".format(pdf_path.name),
        "- Final PNG: `{}`".format(png_path.name),
        "- Formats: PDF vector preferred; PNG bitmap companion",
        "- PNG dpi: {}".format(dpi),
        "- Intended size: {:.1f} x {:.1f} cm ({:.1f} x {:.1f} in)".format(width_cm, height_cm, figsize[0], figsize[1]),
        "- PDF page size: {}".format(pdf_page_size(pdfinfo_output) if pdfinfo_ok else "not checked ({})".format(pdfinfo_output)),
        "- PDF file size: {}".format(file_size_mb(pdf_path)),
        "- PNG file size: {}".format(file_size_mb(png_path)),
        "- Width >= 8 cm: yes",
        "- Font family: DejaVu Sans",
        "- Font consistency: one sans-serif family set in Matplotlib rcParams",
        "- Font embedding status: {}".format(font_embedding_status(pdffonts_output) if pdffonts_ok else "not checked ({})".format(pdffonts_output)),
        "- Colorblind-safe status: Okabe-Ito palette (blue {} + orange {})".format(OKABE_ITO["blue"], OKABE_ITO["orange"]),
        "- Coblis/equivalent review: requires manual Coblis/equivalent review after export",
        "- Legend completeness: colors, bar fills, line styles, and point markers explained",
        "- Panel labels: {}".format(panel_labels),
        "- Units and ranges: counts as comma-separated integers; years as four-digit integers",
        "- Dense point layers: N/A (bar chart / h-line figure)",
        "- Plotting script: `{}`".format(script_copy_path.name),
        "- Plotting-data availability: {} CSV files".format(len(data_paths)),
        "- Export date: {}".format(datetime.date.today().isoformat()),
    ]
    clines.extend("- Plotting data file: `{}`".format(p.name) for p in data_paths)
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text("\n".join(clines).rstrip() + "\n", encoding="utf-8")
    return checklist_path


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


def _require_columns(df: pd.DataFrame, table_name: str, required: set) -> None:
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(table_name, ", ".join(missing)))


def _read_minimal_catalog(release_dir: Path, name: str) -> pd.DataFrame:
    return read_csv_required(Path(release_dir) / name)


def _year_from_column(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_datetime(df[col], errors="coerce").dt.year


def _nunique_nonempty(values: pd.Series) -> int:
    clean = values.astype(str).str.strip()
    clean = clean[clean.ne("")]
    return int(clean.nunique())


def load_main_sources_from_minimal(release_dir: Path) -> pd.DataFrame:
    df = _read_minimal_catalog(release_dir, "source_station_catalog.csv")
    _require_columns(
        df,
        "source_station_catalog.csv",
        {"source_name", "cluster_uid", "n_records", "time_start", "time_end"},
    )

    df = df.copy()
    df["source_name"] = df["source_name"].astype(str).str.strip()
    df["n_records"] = numeric(df, "n_records").fillna(0)
    df["first_year"] = _year_from_column(df, "time_start")
    df["last_year"] = _year_from_column(df, "time_end")
    df = df[df["source_name"].ne("")]

    grouped = (
        df.groupby("source_name", as_index=False)
        .agg(
            cluster_count=("cluster_uid", _nunique_nonempty),
            spatial_record_count=("n_records", "sum"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            temporal_record_count=("n_records", "sum"),
        )
        .sort_values(["cluster_count", "source_name"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return grouped


def load_other_product_sources_from_minimal(release_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    empty = pd.DataFrame(columns=["source_name", "contribution_count", "record_count", "first_year", "last_year"])

    climatology_raw = _read_minimal_catalog(release_dir, "climatology_catalog.csv")
    _require_columns(
        climatology_raw,
        "climatology_catalog.csv",
        {"source_name", "station_uid", "source_station_time_coverage_start", "source_station_time_coverage_end"},
    )
    climatology_raw = climatology_raw.copy()
    climatology_raw["source_name"] = climatology_raw["source_name"].astype(str).str.strip()
    climatology_raw["first_year"] = _year_from_column(climatology_raw, "source_station_time_coverage_start")
    climatology_raw["last_year"] = _year_from_column(climatology_raw, "source_station_time_coverage_end")
    climatology_raw = climatology_raw[climatology_raw["source_name"].ne("")]
    if climatology_raw.empty:
        climatology = empty.copy()
    else:
        climatology = (
            climatology_raw.groupby("source_name", as_index=False)
            .agg(
                contribution_count=("station_uid", _nunique_nonempty),
                record_count=("station_uid", "size"),
                first_year=("first_year", "min"),
                last_year=("last_year", "max"),
            )
            .sort_values(["contribution_count", "source_name"], ascending=[True, False])
            .reset_index(drop=True)
        )

    satellite_raw = _read_minimal_catalog(release_dir, "satellite_catalog.csv")
    _require_columns(
        satellite_raw,
        "satellite_catalog.csv",
        {"source", "cluster_uid", "n_records", "time_start", "time_end"},
    )
    satellite_raw = satellite_raw.copy()
    satellite_raw["source_name"] = satellite_raw["source"].astype(str).str.strip()
    satellite_raw["n_records"] = numeric(satellite_raw, "n_records").fillna(0)
    satellite_raw["first_year"] = _year_from_column(satellite_raw, "time_start")
    satellite_raw["last_year"] = _year_from_column(satellite_raw, "time_end")
    satellite_raw = satellite_raw[satellite_raw["source_name"].ne("")]
    if satellite_raw.empty:
        satellite = empty.copy()
    else:
        satellite = (
            satellite_raw.groupby("source_name", as_index=False)
            .agg(
                contribution_count=("cluster_uid", _nunique_nonempty),
                record_count=("n_records", "sum"),
                first_year=("first_year", "min"),
                last_year=("last_year", "max"),
            )
            .sort_values(["contribution_count", "source_name"], ascending=[True, False])
            .reset_index(drop=True)
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


def _set_year_limits(ax, time_df: pd.DataFrame) -> None:
    if time_df.empty:
        return
    year_min = int(np.floor(time_df["first_year"].min() / 10.0) * 10)
    year_max = int(np.ceil(time_df["last_year"].max() / 10.0) * 10)
    ax.set_xlim(year_min - 2, year_max + 9)


def plot_overlay_source_contribution(
    df: pd.DataFrame,
    figure_id: str,
    figure_dirs: dict,
    dpi: int = 300,
) -> Tuple[Path, Path]:
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

    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def plot_other_products_source_contribution(
    climatology: pd.DataFrame,
    satellite: pd.DataFrame,
    figure_id: str,
    figure_dirs: dict,
    dpi: int = 300,
) -> Tuple[Path, Path]:
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
        OKABE_ITO["bluish_green"],
    )
    axes[0].text(
        0.01, 0.97, "(a)", transform=axes[0].transAxes,
        fontsize=13, fontweight="bold", va="top", ha="left",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85),
    )
    plot_other_product_panel(
        axes[1],
        satellite,
        "Satellite-validation sources",
        "Linked cluster count",
        OKABE_ITO["reddish_purple"],
    )
    axes[1].text(
        0.01, 0.97, "(b)", transform=axes[1].transAxes,
        fontsize=13, fontweight="bold", va="top", ha="left",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85),
    )

    fig.suptitle("Other product source contributions and temporal span", y=0.98)
    legend_handles = [
        Patch(facecolor=OKABE_ITO["bluish_green"], alpha=0.48, edgecolor="none", label="climatology stations"),
        Patch(facecolor=OKABE_ITO["reddish_purple"], alpha=0.48, edgecolor="none", label="satellite linked clusters"),
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

    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot source spatial-temporal contribution figures from minimal release catalogs."
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=(
            "Figure output directory. Figures are saved under final/, data/, scripts/, and checklists/ "
            "subdirectories per ESSD guidelines. Default: "
            "{}".format(DEFAULT_OUTPUT_DIR)
        ),
    )
    parser.add_argument(
        "--figure-id",
        default=None,
        help="Figure ID stem for both generated figures (overrides individual --figure-id-*).",
    )
    parser.add_argument(
        "--figure-id-b",
        default=OVERLAY_OUTPUT_STEM,
        help="Figure ID for the overlay figure. Default: {}".format(OVERLAY_OUTPUT_STEM),
    )
    parser.add_argument(
        "--figure-id-c",
        default=OTHER_PRODUCTS_OUTPUT_STEM,
        help="Figure ID for the other-products figure. Default: {}".format(OTHER_PRODUCTS_OUTPUT_STEM),
    )
    parser.add_argument("--dpi", type=int, default=DPI, help="PNG output DPI. Default: {}".format(DPI))
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    release_dir = DEFAULT_RELEASE_DIR.resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    dpi = int(args.dpi)

    # Resolve figure IDs
    figure_id_b = args.figure_id or args.figure_id_b
    figure_id_c = args.figure_id or args.figure_id_c

    # Configure matplotlib for ESSD compliance
    configure_matplotlib(plt)

    # Create ESSD output directory structure
    figure_dirs = ensure_figure_dirs(out_dir)

    # Load data (shared by all 3 figures)
    merged = load_main_sources_from_minimal(release_dir)
    climatology_df, satellite_df = load_other_product_sources_from_minimal(release_dir)

    # ---- Figure B: Overlay source contribution (single-panel with twin axes) ----
    png_b, pdf_b = plot_overlay_source_contribution(merged, figure_id_b, figure_dirs, dpi=dpi)
    data_paths_b = write_plotting_data(figure_dirs["data"], figure_id_b, merged, pd.DataFrame(), pd.DataFrame())
    script_copy_b = figure_dirs["scripts"] / "plot_{}.py".format(figure_id_b)
    shutil.copy2(__file__, script_copy_b)
    write_figure_checklist(
        figure_dirs["checklists"] / "{}_checklist.md".format(figure_id_b),
        figure_id_b, pdf_b, png_b, data_paths_b, script_copy_b, dpi, OVERLAY_FIGSIZE,
        is_multi_panel=False, panel_labels="N/A (single-panel figure with twin axes)",
    )

    # ---- Figure C: Other products source contribution (2-panel) ----
    png_c, pdf_c = plot_other_products_source_contribution(climatology_df, satellite_df, figure_id_c, figure_dirs, dpi=dpi)
    data_paths_c = write_plotting_data(figure_dirs["data"], figure_id_c, pd.DataFrame(), climatology_df, satellite_df)
    script_copy_c = figure_dirs["scripts"] / "plot_{}.py".format(figure_id_c)
    shutil.copy2(__file__, script_copy_c)
    write_figure_checklist(
        figure_dirs["checklists"] / "{}_checklist.md".format(figure_id_c),
        figure_id_c, pdf_c, png_c, data_paths_c, script_copy_c, dpi, (10.8, 6.8),
        is_multi_panel=True, panel_labels="`(a)` climatology sources, `(b)` satellite-validation sources",
    )

    # Print summary
    print("Wrote {}".format(pdf_b))
    print("Wrote {}".format(png_b))
    print("Wrote {}".format(pdf_c))
    print("Wrote {}".format(png_c))
    for path in data_paths_b + data_paths_c:
        print("Wrote {}".format(path))
    print("Wrote {}".format(script_copy_b))
    print("Wrote {}".format(script_copy_c))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
