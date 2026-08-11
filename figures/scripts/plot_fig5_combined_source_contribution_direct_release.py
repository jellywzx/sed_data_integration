#!/usr/bin/env python3
"""Draw the combined source-contribution release figure directly with Matplotlib.

This script does not paste pre-rendered PNG panels. It reloads the release CSV
tables, draws all panels in one Matplotlib figure, and exports vector PDF plus
PNG companion files.

All functionality previously imported from the companion module
``plot_fig_source_spatial_temporal_contribution_overlay_release.py`` has been
inlined so that this script is fully self-contained.
"""

import argparse
import ctypes
import datetime
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Dict, List, Tuple

# --- Ensure libstdc++ is loadable before importing numerical libraries ---
CONDA_LIB = "/share/home/dq134/.conda/envs/wzx/lib"
os.environ["LD_LIBRARY_PATH"] = CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
ctypes.CDLL(str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import AutoMinorLocator
from matplotlib.patches import Patch
import numpy as np
import pandas as pd


# ============================================================
# Paths & defaults
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent.parent
DEFAULT_RELEASE_DIR = PROJECT_DIR / "output" / "sed_reference_release_minimal"
DEFAULT_OUTPUT_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/figures"
)


def script_output_stem() -> str:
    stem = Path(__file__).resolve().stem
    return stem[5:] if stem.startswith("plot_") else stem


COMBINED_OUTPUT_STEM = script_output_stem()

# ============================================================
# Figure geometry constants
# ============================================================

WIDTH_CM = 35.0
HEIGHT_CM = 45.0
DPI = 300
CM_PER_INCH = 2.54

# ============================================================
# Font / style constants
# ============================================================

FONT_SIZE = 18
AXES_LABEL_SIZE = 18
AXES_TITLE_SIZE = 16
TICK_LABEL_SIZE = 16
LEGEND_FONT_SIZE = 16
PANEL_LABEL_SIZE = 20
MIN_VISIBLE_FONT_SIZE = 16

# ============================================================
# Colour palette (Okabe-Ito)
# ============================================================

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

SPATIAL_COLOR = "#0072B2"
TEMPORAL_LINE_COLOR = "#222222"
TEMPORAL_POINT_COLOR = "#E69F00"
TEMPORAL_LINE_WIDTH = 2
TEMPORAL_LINE_ALPHA = 0.6
TEMPORAL_POINT_SIZE = 100

# ============================================================
# Source-name mapping
# ============================================================

SOURCE_NAME_MAP = {
    "USGS": "USGS NWIS",
    "RiverSed": "RivSed",
    "Milliman & Farnsworth":"Milliman",
    "Vanmaercke et al.":"Vanmaercke",
    "High Mountain Asia (HMA)":"HMA",
    "Huanghe (Yellow River)":"Huanghe",
    "Ali & De Boer (Upper Indus)":'Ali_De_Boer',
}

# ============================================================
# Layout spacing
# ============================================================

HSPACE_PANEL = 0.25   # vertical gap between panel (a) and panel (b)
HSPACE_SUB = 0.7      # vertical gap between climatology / satellite sub-panels


# ============================================================
# Matplotlib configuration
# ============================================================

def configure_matplotlib() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
            "axes.unicode_minus": False,
            "font.size": FONT_SIZE,
            "axes.labelsize": AXES_LABEL_SIZE,
            "axes.titlesize": AXES_TITLE_SIZE,
            "xtick.labelsize": TICK_LABEL_SIZE,
            "ytick.labelsize": TICK_LABEL_SIZE,
            "legend.fontsize": LEGEND_FONT_SIZE,
        }
    )


# ============================================================
# Filesystem / subprocess utilities
# ============================================================

def ensure_figure_dirs(figures_root: Path) -> Dict[str, Path]:
    root = Path(figures_root).resolve()
    dirs = {
        "root": root,
        "final": root / "final",
        "data": root / "data",
        "scripts": root / "scripts",
        "checklists": root / "checklists",
    }
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)
    return dirs


def run_text_command(cmd: List[str]) -> Tuple[bool, str]:
    try:
        result = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
    except FileNotFoundError:
        return False, "{} unavailable".format(cmd[0])
    return result.returncode == 0, result.stdout.strip()


def file_size_mb(path: Path) -> str:
    if not path.is_file():
        return "not found"
    return "{:.2f} MB".format(path.stat().st_size / (1024 * 1024))


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
    values = [
        line[emb_start:sub_start].strip().lower() for line in lines[2:] if line.strip()
    ]
    if values and all(value == "yes" for value in values):
        return "all reported fonts embedded"
    if values:
        return "some reported fonts may not be embedded; review pdffonts output"
    return "no fonts reported by pdffonts"


# ============================================================
# Data-loading helpers
# ============================================================

def read_csv_required(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(
            "Required input not found: {}\n"
            "Expected the built-in sed_reference_release_minimal directory to be "
            "complete.".format(path)
        )
    try:
        return pd.read_csv(path, keep_default_na=False)
    except pd.errors.EmptyDataError:
        raise ValueError(
            "Required input is empty: {}\n"
            "Expected the built-in sed_reference_release_minimal directory to be "
            "complete.".format(path)
        )


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
        raise ValueError(
            "{} is missing columns: {}".format(table_name, ", ".join(missing))
        )


def _read_minimal_catalog(release_dir: Path, name: str) -> pd.DataFrame:
    return read_csv_required(Path(release_dir) / name)


def _year_from_column(df: pd.DataFrame, col: str) -> pd.Series:
    text = df[col].astype(str).str.strip()
    extracted = text.str.extract(r"^(\d{4})", expand=False)
    years = pd.to_numeric(extracted, errors="coerce")

    empty_like = text.eq("") | text.str.lower().isin({"nan", "nat", "none"})
    needs_fallback = years.isna() & ~empty_like
    if needs_fallback.any():
        fallback = df.loc[needs_fallback, col].apply(
            lambda value: pd.to_datetime(value, errors="coerce")
        )
        years.loc[needs_fallback] = fallback.apply(
            lambda value: value.year if pd.notna(value) else np.nan
        )
    return years


def _validate_temporal_coverage(
    df: pd.DataFrame, table_name: str, source_columns: Tuple[str, str]
) -> None:
    if df.empty:
        return
    bad = (
        numeric(df, "contribution_count").fillna(0).gt(0)
        & (df["first_year"].isna() | df["last_year"].isna())
    )
    if not bad.any():
        return
    sources = ", ".join(df.loc[bad, "source_name"].astype(str))
    raise ValueError(
        "{} produced missing temporal coverage for sources with "
        "contribution_count > 0 using columns {} and {}: {}".format(
            table_name, source_columns[0], source_columns[1], sources
        )
    )


def _nunique_nonempty(values: pd.Series) -> int:
    clean = values.astype(str).str.strip()
    clean = clean[clean.ne("")]
    return int(clean.nunique())


# ============================================================
# Load release-catalog tables
# ============================================================

def load_main_sources_from_minimal(release_dir: Path) -> pd.DataFrame:
    df = _read_minimal_catalog(release_dir, "source_station_catalog.csv")
    _require_columns(
        df,
        "source_station_catalog.csv",
        {"source_name", "station_uid", "n_records", "time_start", "time_end"},
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
            station_count=("station_uid", _nunique_nonempty),
            spatial_record_count=("n_records", "sum"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            temporal_record_count=("n_records", "sum"),
        )
        .sort_values(["station_count", "source_name"], ascending=[True, False])
        .reset_index(drop=True)
    )
    grouped["source_name"] = grouped["source_name"].replace(SOURCE_NAME_MAP)
    return grouped


def load_other_product_sources_from_minimal(
    release_dir: Path,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    empty = pd.DataFrame(
        columns=["source_name", "contribution_count", "record_count",
                 "first_year", "last_year"]
    )

    # -- climatology --
    climatology_raw = _read_minimal_catalog(release_dir, "climatology_catalog.csv")
    _require_columns(
        climatology_raw,
        "climatology_catalog.csv",
        {"source_name", "station_uid", "time"},
    )
    climatology_raw = climatology_raw.copy()
    climatology_raw["source_name"] = (
        climatology_raw["source_name"].astype(str).str.strip()
    )
    # Use per-station temporal coverage columns when present (time_start / time_end);
    # fall back to the single "time" midpoint column for older CSV exports.
    _has_time_range = (
        "time_start" in climatology_raw.columns
        and "time_end" in climatology_raw.columns
        and climatology_raw["time_start"].notna().any()
    )
    if _has_time_range:
        climatology_raw["first_year"] = _year_from_column(climatology_raw, "time_start")
        climatology_raw["last_year"] = _year_from_column(climatology_raw, "time_end")
    else:
        climatology_raw["first_year"] = _year_from_column(climatology_raw, "time")
        climatology_raw["last_year"] = _year_from_column(climatology_raw, "time")
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
            .sort_values(
                ["contribution_count", "source_name"], ascending=[True, False]
            )
            .reset_index(drop=True)
        )
    _validate_temporal_coverage(
        climatology, "climatology_catalog.csv", ("time", "time")
    )

    # -- satellite --
    satellite_raw = _read_minimal_catalog(release_dir, "satellite_catalog.csv")
    _require_columns(
        satellite_raw,
        "satellite_catalog.csv",
        {"source", "station_uid", "n_records", "time_start", "time_end"},
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
                contribution_count=("station_uid", _nunique_nonempty),
                record_count=("n_records", "sum"),
                first_year=("first_year", "min"),
                last_year=("last_year", "max"),
            )
            .sort_values(
                ["contribution_count", "source_name"], ascending=[True, False]
            )
            .reset_index(drop=True)
        )
    _validate_temporal_coverage(
        satellite, "satellite_catalog.csv", ("time_start", "time_end")
    )

    climatology["source_name"] = climatology["source_name"].replace(SOURCE_NAME_MAP)
    satellite["source_name"] = satellite["source_name"].replace(SOURCE_NAME_MAP)
    return climatology, satellite


# ============================================================
# Plotting-data CSV export
# ============================================================

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
    outputs: List[Path] = []
    if not merged_df.empty:
        outputs.append(
            _write_csv(merged_df, data_dir / "{}_main_source_data.csv".format(figure_id))
        )
    if not climatology_df.empty:
        outputs.append(
            _write_csv(
                climatology_df, data_dir / "{}_climatology_data.csv".format(figure_id)
            )
        )
    if not satellite_df.empty:
        outputs.append(
            _write_csv(
                satellite_df, data_dir / "{}_satellite_data.csv".format(figure_id)
            )
        )
    return outputs


# ============================================================
# Lower-level plotting helpers
# ============================================================

def _set_year_limits(ax, time_df: pd.DataFrame) -> None:
    if time_df.empty:
        return
    year_min = int(np.floor(time_df["first_year"].min() / 10.0) * 10)
    year_max = int(np.ceil(time_df["last_year"].max() / 10.0) * 10)
    ax.set_xlim(year_min - 2, year_max + 9)
    ax.xaxis.set_minor_locator(AutoMinorLocator(2))
    ax.tick_params(axis="x", which="minor", length=3, width=0.6, colors=TEMPORAL_LINE_COLOR)


def annotate_station_counts_on_twin(
    source_ax,
    label_ax,
    df: pd.DataFrame,
    y: np.ndarray,
    pad_fraction: float = 0.012,
) -> None:
    max_station = pd.to_numeric(df["station_count"], errors="coerce").max()
    if pd.isna(max_station) or max_station <= 0:
        return
    pad = max_station * pad_fraction
    has_temporal = "temporal_record_count" in df.columns
    for i, (ypos, value) in enumerate(zip(y, df["station_count"])):
        if pd.isna(value):
            continue
        label = format_count(value)
        if has_temporal:
            t_count = df.iloc[i].get("temporal_record_count")
            if pd.notna(t_count) and float(t_count) > 0:
                label += " / " + format_compact_count(t_count)
        source_ax.text(
            float(value) + pad,
            ypos,
            label,
            va="center",
            ha="left",
            fontsize=TICK_LABEL_SIZE,
            color="#2f4f6f",
            bbox={
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.82,
                "pad": 0.4,
            },
            zorder=5,
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

    df = df.reset_index(drop=True).copy()
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

    temporal_mask = df[["first_year", "last_year"]].notna().all(axis=1)
    time_df = df.loc[temporal_mask].copy()
    time_y = y[temporal_mask.to_numpy()]
    if not time_df.empty:
        for ypos, (_, row) in zip(time_y, time_df.iterrows()):
            ax_year.hlines(
                ypos,
                row["first_year"],
                row["last_year"],
                color=TEMPORAL_LINE_COLOR,
                linewidth=TEMPORAL_LINE_WIDTH,
                alpha=TEMPORAL_LINE_ALPHA,
                zorder=3,
            )
        ax_year.scatter(
            time_df["last_year"],
            time_y,
            s=TEMPORAL_POINT_SIZE,
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
        label_value = float(value) + label_pad
        ax_count.text(
            label_value,
            ypos,
            label,
            va="center",
            ha="left",
            fontsize=TICK_LABEL_SIZE,
            color="#2f4f6f",
            bbox={
                "facecolor": "white",
                "edgecolor": "none",
                "alpha": 0.82,
                "pad": 0.4,
            },
            zorder=5,
        )

    ax_count.set_title(title, loc="left", fontsize=AXES_TITLE_SIZE)


# ============================================================
# Panel drawing for the combined figure
# ============================================================

def draw_main_source_panel(ax_station, df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("No main source rows available for plotting.")

    df = df.reset_index(drop=True).copy()
    y = np.arange(len(df))
    ax_year = ax_station.twiny()
    ax_year.patch.set_alpha(0)
    ax_year.set_zorder(ax_station.get_zorder() + 1)

    ax_station.barh(
        y,
        df["station_count"],
        color=SPATIAL_COLOR,
        alpha=0.45,
        height=0.62,
        zorder=1,
    )
    ax_station.set_yticks(y)
    ax_station.set_yticklabels(df["source_name"])
    ax_station.set_xlabel("Reference station count", color=SPATIAL_COLOR)
    ax_station.tick_params(axis="x", colors=SPATIAL_COLOR)
    ax_station.spines["bottom"].set_color(SPATIAL_COLOR)
    ax_station.grid(axis="x", linewidth=0.3, alpha=0.45, color=SPATIAL_COLOR)
    ax_station.set_axisbelow(True)

    max_station = pd.to_numeric(df["station_count"], errors="coerce").max()
    if pd.notna(max_station) and max_station > 0:
        ax_station.set_xlim(0, max_station * 1.24)

    temporal_mask = df[["first_year", "last_year"]].notna().all(axis=1)
    time_df = df.loc[temporal_mask].copy()
    time_y = y[temporal_mask.to_numpy()]
    if not time_df.empty:
        for ypos, (_, row) in zip(time_y, time_df.iterrows()):
            ax_year.hlines(
                ypos,
                row["first_year"],
                row["last_year"],
                color=TEMPORAL_LINE_COLOR,
                linewidth=TEMPORAL_LINE_WIDTH,
                alpha=TEMPORAL_LINE_ALPHA,
                zorder=3,
            )
        ax_year.scatter(
            time_df["last_year"],
            time_y,
            s=TEMPORAL_POINT_SIZE,
            color=TEMPORAL_POINT_COLOR,
            alpha=0.82,
            edgecolor="white",
            linewidth=0.5,
            zorder=4,
        )
        _set_year_limits(ax_year, time_df)

    annotate_station_counts_on_twin(ax_station, ax_year, df, y)
    ax_year.set_xlabel("Year", color=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="x", colors=TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="y", left=False, labelleft=False)
    ax_year.spines["top"].set_color(TEMPORAL_LINE_COLOR)


def add_panel_label(
    ax, label: str, x: float = -0.12, y: float = 1.25
) -> None:
    ax.text(
        x,
        y,
        label,
        transform=ax.transAxes,
        fontsize=PANEL_LABEL_SIZE,
        fontweight="bold",
        va="top",
        ha="left",
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.9, "pad": 2.2},
        clip_on=False,
    )


def legend_handles() -> List[object]:
    return [
        Patch(
            facecolor=SPATIAL_COLOR,
            alpha=0.45,
            edgecolor="none",
            label="main reference stations",
        ),
        Patch(
            facecolor=SPATIAL_COLOR,
            alpha=0.72,
            edgecolor="#2f4f6f",
            linewidth=0.5,
            label="counts / records",
        ),
        Patch(
            facecolor=OKABE_ITO["bluish_green"],
            alpha=0.48,
            edgecolor="none",
            label="climatology stations",
        ),
        Line2D(
            [0],
            [0],
            color=TEMPORAL_LINE_COLOR,
            linewidth=TEMPORAL_LINE_WIDTH,
            label="temporal span",
        ),
        Patch(
            facecolor=OKABE_ITO["reddish_purple"],
            alpha=0.48,
            edgecolor="none",
            label="satellite stations",
        ),
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


def plot_combined_direct(
    merged: pd.DataFrame,
    climatology: pd.DataFrame,
    satellite: pd.DataFrame,
    figure_id: str,
    figure_dirs: Dict[str, Path],
    dpi: int = DPI,
    width_cm: float = WIDTH_CM,
    height_cm: float = HEIGHT_CM,
) -> Tuple[Path, Path, Tuple[float, float]]:
    figsize = (width_cm / CM_PER_INCH, height_cm / CM_PER_INCH)
    fig = plt.figure(figsize=figsize)

    # Outer GridSpec: panel (a) | panel (b)
    outer_gs = fig.add_gridspec(
        2,
        1,
        height_ratios=[2.6, 2],
        hspace=HSPACE_PANEL,
        left=0.22,
        right=0.97,
        top=0.9,
        bottom=0.16,
    )

    ax_main = fig.add_subplot(outer_gs[0, 0])

    # Inner GridSpec within panel (b): climatology | satellite
    inner_gs = outer_gs[1, 0].subgridspec(
        2,
        1,
        height_ratios=[1, 1],
        hspace=HSPACE_SUB,
    )
    ax_climatology = fig.add_subplot(inner_gs[0, 0])
    ax_satellite = fig.add_subplot(inner_gs[1, 0])

    draw_main_source_panel(ax_main, merged)
    plot_other_product_panel(
        ax_climatology,
        climatology,
        "",
        "Station count",
        OKABE_ITO["bluish_green"],
    )
    plot_other_product_panel(
        ax_satellite,
        satellite,
        "",
        "Station count",
        OKABE_ITO["reddish_purple"],
    )

    add_panel_label(ax_main, "(a) Main station-reference matrices", x=-0.15, y=1.1)
    add_panel_label(
        ax_climatology, "(b) Climatology auxiliary layer", x=-0.15, y=1.35
    )
    add_panel_label(
        ax_satellite, "(c) Satellite-derived auxiliary layer", x=-0.15, y=1.35
    )

    ax_satellite.legend(
        handles=legend_handles(),
        loc="upper center",
        ncol=3,
        frameon=False,
        bbox_to_anchor=(0.5, -0.30),
    )

    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path, figsize


# ============================================================
# ESSD checklist
# ============================================================

def write_combined_checklist(
    checklist_path: Path,
    figure_id: str,
    pdf_path: Path,
    png_path: Path,
    data_paths: List[Path],
    script_copy_path: Path,
    dpi: int,
    figsize: Tuple[float, float],
) -> Path:
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(["pdffonts", str(pdf_path)])
    width_cm = figsize[0] * CM_PER_INCH
    height_cm = figsize[1] * CM_PER_INCH
    font_status = (
        font_embedding_status(pdffonts_output)
        if pdffonts_ok
        else "not checked ({})".format(pdffonts_output)
    )
    text = """# {} ESSD figure checklist

## File information
- Final PDF: `{}`
- Final PNG: `{}`
- Formats: PDF vector preferred; PNG bitmap companion
- Plotting script: `{}`
- Checklist: `{}`

## Format and resolution
- Preferred vector format used: yes, PDF
- Bitmap dpi: {}
- PDF page size: {}
- PDF file size: {}
- PNG file size: {}

## Size and layout
- Figure size: {:.1f} x {:.1f} cm ({:.1f} x {:.1f} in)
- Width >= 8 cm: yes
- Multi-panel layout: 3 rows x 1 column
- Panel labels: (a), (b)

## Fonts
- Font family: DejaVu Sans
- Minimum visible font size: {} pt
- Single font family used: yes
- Font embedding setting: pdf.fonttype = 42
- Font embedding status (via pdffonts): {}

## Reproducibility
- Figure is drawn directly from release CSV tables; no PNG sub-figure compositing is used.
- Script is self-contained (all plotting logic inlined).
- Python executable: `{}`
- pandas version: {}
- matplotlib version: {}
- Plotting-data availability: {} CSV files
- Export date: {}
""".format(
        figure_id,
        pdf_path.name,
        png_path.name,
        script_copy_path.name,
        checklist_path.name,
        dpi,
        pdf_page_size(pdfinfo_output)
        if pdfinfo_ok
        else "not checked ({})".format(pdfinfo_output),
        file_size_mb(pdf_path),
        file_size_mb(png_path),
        width_cm,
        height_cm,
        figsize[0],
        figsize[1],
        MIN_VISIBLE_FONT_SIZE,
        font_status,
        sys.executable,
        pd.__version__,
        matplotlib.__version__,
        len(data_paths),
        datetime.date.today().isoformat(),
    )
    if data_paths:
        text += (
            "\n".join(
                "- Plotting data file: `{}`".format(p.name) for p in data_paths
            )
            + "\n"
        )
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text(text, encoding="utf-8")
    return checklist_path


def copy_script(scripts_dir: Path) -> Path:
    src = Path(__file__).resolve()
    dst = scripts_dir / src.name
    scripts_dir.mkdir(parents=True, exist_ok=True)
    if src != dst:
        shutil.copy2(src, dst)
    return dst


# ============================================================
# CLI
# ============================================================

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Draw the combined source-contribution release figure directly "
        "from release CSV tables."
    )
    parser.add_argument(
        "--release-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Release CSV directory. Default: {}".format(DEFAULT_RELEASE_DIR),
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Figure output directory. Default: {}".format(DEFAULT_OUTPUT_DIR),
    )
    parser.add_argument(
        "--figure-id",
        default=COMBINED_OUTPUT_STEM,
        help="Combined figure ID stem. Default: {}".format(COMBINED_OUTPUT_STEM),
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=DPI,
        help="PNG output DPI. Default: {}".format(DPI),
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_matplotlib()

    release_dir = Path(args.release_dir).expanduser().resolve()
    figure_dirs = ensure_figure_dirs(Path(args.out_dir).expanduser().resolve())
    dpi = int(args.dpi)

    merged = load_main_sources_from_minimal(release_dir)
    climatology_df, satellite_df = load_other_product_sources_from_minimal(release_dir)
    data_paths = write_plotting_data(
        figure_dirs["data"],
        args.figure_id,
        merged,
        climatology_df,
        satellite_df,
    )

    png_path, pdf_path, figsize = plot_combined_direct(
        merged=merged,
        climatology=climatology_df,
        satellite=satellite_df,
        figure_id=args.figure_id,
        figure_dirs=figure_dirs,
        dpi=dpi,
    )
    script_copy_path = copy_script(figure_dirs["scripts"])
    checklist_path = write_combined_checklist(
        checklist_path=figure_dirs["checklists"]
        / "{}_checklist.md".format(args.figure_id),
        figure_id=args.figure_id,
        pdf_path=pdf_path,
        png_path=png_path,
        data_paths=data_paths,
        script_copy_path=script_copy_path,
        dpi=dpi,
        figsize=figsize,
    )

    print("Wrote {}".format(pdf_path))
    print("Wrote {}".format(png_path))
    print(
        "Python {} | pandas {} | matplotlib {}".format(
            sys.executable, pd.__version__, matplotlib.__version__
        )
    )
    print("Copied script to {}".format(script_copy_path))
    print("Wrote {}".format(checklist_path))
    for data_path in data_paths:
        print("Wrote {}".format(data_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
