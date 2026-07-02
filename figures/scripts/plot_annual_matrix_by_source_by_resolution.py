#!/usr/bin/env python3
"""Plot annual matrix record counts by source dataset and temporal resolution.

This script reads the released station-by-time matrix NetCDF products and
counts non-empty station-time cells by year, source dataset, and resolution.

It produces a three-panel figure:
(a) Daily matrix records by source
(b) Monthly matrix records by source
(c) Annual matrix records by source

No command-line arguments are required. All settings are defined below.
"""

import argparse
import datetime
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd

try:
    from netCDF4 import Dataset, chartostring, num2date
except ImportError as exc:
    raise ImportError(
        "This script requires netCDF4. Please run it in the same environment "
        "used for the sediment release workflow."
    ) from exc


# ============================================================
# Built-in settings
# ============================================================

RELEASE_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
    "scripts_basin_test/output/sed_reference_release_minimal"
)

OUTPUT_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
    "scripts_basin_test/figures"
)

FIGURE_ID = "fig_annual_matrix_records_by_source_by_resolution"

RESOLUTIONS = ("daily", "monthly", "annual")

MATRIX_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}

PANEL_TITLES = {
    "daily": "(a) Daily",
    "monthly": "(b) Monthly",
    "annual": "(c) Annual",
}

# Top source groups are selected as the union of the largest contributors
# within each resolution. Remaining sources are grouped as Other.
TOP_N_PER_RESOLUTION = 5
OTHER_LABEL = "Other matrix sources"

# Okabe-Ito colorblind-safe palette (extended with Wong 2011 colors)
# Reference: docs/essd_figure_requirements.md
OKABE_ITO = {
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
    "black": "#000000",
}
OKABE_ITO_EXTENDED = [
    OKABE_ITO["orange"],
    OKABE_ITO["sky_blue"],
    OKABE_ITO["bluish_green"],
    OKABE_ITO["yellow"],
    OKABE_ITO["blue"],
    OKABE_ITO["vermillion"],
    OKABE_ITO["reddish_purple"],
    "#882E72",  # dark purple (replaces black)
    "#A6761D",  # amber/brown (Wong 2011)
    "#66A61E",  # lime green  (Wong 2011)
]
OTHER_COLOR = "#B0B0B0"


# Choose "bar" or "area".
# "bar" is more literal for annual counts; "area" gives a smoother overview.
PLOT_KIND = "bar"

CHUNK_TIME = 366
DPI = 300

WIDTH_CM = 30.0
HEIGHT_CM = 28.0
CM_PER_INCH = 2.54

# All font-size tuning is controlled here.
# These feed into rcParams and propagate to all text elements
# (axes labels, titles, tick labels, legends, annotations).
FONT_SIZE = 18
AXES_LABEL_SIZE = 18
AXES_TITLE_SIZE = 18
TICK_LABEL_SIZE = 17
LEGEND_FONT_SIZE = 16
MIN_VISIBLE_FONT_SIZE = 15


# ============================================================
# Utility functions
# ============================================================

def configure_matplotlib() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": FONT_SIZE,
            "axes.labelsize": AXES_LABEL_SIZE,
            "axes.titlesize": AXES_TITLE_SIZE,
            "xtick.labelsize": TICK_LABEL_SIZE,
            "ytick.labelsize": TICK_LABEL_SIZE,
            "legend.fontsize": LEGEND_FONT_SIZE,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "axes.linewidth": 0.8,
            "xtick.major.width": 0.8,
            "ytick.major.width": 0.8,
            "axes.unicode_minus": False,
            "savefig.dpi": DPI,
        }
    )


def ensure_figure_dirs(figures_root: Path) -> Dict[str, Path]:
    root = Path(figures_root).expanduser().resolve()
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


def compact_count(value: float, pos=None) -> str:
    try:
        value = float(value)
    except Exception:
        return ""

    abs_value = abs(value)
    if abs_value >= 1_000_000:
        return "{:.1f}M".format(value / 1_000_000).replace(".0M", "M")
    if abs_value >= 1_000:
        return "{:.0f}k".format(value / 1_000)
    return "{:.0f}".format(value)


def copy_script(scripts_dir: Path) -> Path:
    src = Path(__file__).resolve()
    dst = scripts_dir / src.name
    scripts_dir.mkdir(parents=True, exist_ok=True)
    if src != dst:
        shutil.copy2(src, dst)
    return dst


# ============================================================
# Data loading and decoding
# ============================================================

def read_source_lookup(release_dir: Path) -> Dict[str, str]:
    catalog_path = release_dir / "source_station_catalog.csv"
    if not catalog_path.is_file():
        raise FileNotFoundError("Missing source station catalog: {}".format(catalog_path))

    df = pd.read_csv(catalog_path, keep_default_na=False)

    required = {"source_station_uid", "source_name"}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(
            "{} is missing required columns: {}".format(
                catalog_path.name, ", ".join(missing)
            )
        )

    df["source_station_uid"] = df["source_station_uid"].astype(str).str.strip()
    df["source_name"] = df["source_name"].astype(str).str.strip()

    df = df[df["source_station_uid"].ne("")]
    df = df[df["source_name"].ne("")]
    df = df.drop_duplicates(subset=["source_station_uid"], keep="first")

    return dict(zip(df["source_station_uid"], df["source_name"]))


def decode_time_years(ds: Dataset) -> np.ndarray:
    if "time" not in ds.variables:
        raise ValueError("NetCDF file is missing required time coordinate.")

    time_var = ds.variables["time"]
    values = np.asarray(time_var[:])

    units = getattr(time_var, "units", "")
    calendar = getattr(time_var, "calendar", "standard")

    if units:
        dates = num2date(
            values,
            units=units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=False,
        )
        return np.asarray([int(date.year) for date in dates], dtype=np.int32)

    numeric_values = pd.to_numeric(pd.Series(values.ravel()), errors="coerce")
    if numeric_values.notna().all():
        min_value = numeric_values.min()
        max_value = numeric_values.max()
        if 1500 <= min_value <= 3000 and 1500 <= max_value <= 3000:
            return numeric_values.astype(int).to_numpy(dtype=np.int32)

    parsed = pd.to_datetime(values, errors="coerce")
    if pd.isna(parsed).any():
        raise ValueError("Could not decode NetCDF time coordinate.")

    return np.asarray([int(x.year) for x in parsed], dtype=np.int32)


def is_probable_char_array(var, values: np.ndarray) -> bool:
    if values.dtype.kind not in {"S", "U"}:
        return False
    if values.ndim < 3:
        return False
    if len(var.dimensions) != values.ndim:
        return False

    last_dim = str(var.dimensions[-1]).lower()
    return (
        "char" in last_dim
        or "strlen" in last_dim
        or "string" in last_dim
        or values.dtype.itemsize == 1
    )


def decode_uid_values(var, values: np.ndarray) -> Tuple[np.ndarray, List[str]]:
    arr = values

    if np.ma.isMaskedArray(arr):
        arr = arr.filled("")

    dims = list(var.dimensions)

    if is_probable_char_array(var, np.asarray(arr)):
        arr = chartostring(arr)
        dims = dims[:-1]

    arr = np.asarray(arr)

    if arr.dtype.kind == "S":
        arr = np.char.decode(arr, "utf-8", errors="ignore")
    else:
        arr = arr.astype(str)

    arr = np.char.strip(arr)

    return arr, dims


def valid_uid_mask(uid_array: np.ndarray) -> np.ndarray:
    arr = uid_array.astype(str)
    lower = np.char.lower(np.char.strip(arr))

    invalid = (
        (lower == "")
        | (lower == "nan")
        | (lower == "none")
        | (lower == "null")
        | (lower == "_")
        | (lower == "--")
        | (lower == "missing")
        | (lower == "fill")
    )

    return ~invalid


def slice_variable_by_time(var, time_dim_index: int, start: int, stop: int):
    slicer = [slice(None)] * len(var.dimensions)
    slicer[time_dim_index] = slice(start, stop)
    return var[tuple(slicer)]


# ============================================================
# Count matrix records by year, source, and resolution
# ============================================================

def count_resolution_records(
    nc_path: Path,
    resolution: str,
    source_lookup: Dict[str, str],
) -> pd.DataFrame:
    if not nc_path.is_file():
        raise FileNotFoundError("Missing matrix NetCDF: {}".format(nc_path))

    rows = []

    with Dataset(nc_path, "r") as ds:
        if "selected_source_station_uid" not in ds.variables:
            raise ValueError(
                "{} is missing selected_source_station_uid. "
                "This variable is required for source-level annual counts.".format(nc_path.name)
            )

        years = decode_time_years(ds)
        uid_var = ds.variables["selected_source_station_uid"]

        # Read flag variables for validity check: at least one of
        # Q, SSC, SSL must have non-missing data (flag != 9)
        q_flag_var = ds.variables["Q_flag"]
        ssc_flag_var = ds.variables["SSC_flag"]
        ssl_flag_var = ds.variables["SSL_flag"]

        if "time" not in uid_var.dimensions:
            raise ValueError(
                "{} variable selected_source_station_uid does not contain a time dimension.".format(
                    nc_path.name
                )
            )

        raw_time_axis = list(uid_var.dimensions).index("time")
        n_time = len(years)

        for start in range(0, n_time, CHUNK_TIME):
            stop = min(start + CHUNK_TIME, n_time)

            raw_values = slice_variable_by_time(uid_var, raw_time_axis, start, stop)
            uid_values, active_dims = decode_uid_values(uid_var, raw_values)

            if "time" not in active_dims:
                raise ValueError(
                    "Could not locate active time dimension after decoding "
                    "selected_source_station_uid."
                )

            time_axis = active_dims.index("time")
            uid_values = np.moveaxis(uid_values, time_axis, 0)

            years_chunk = years[start:stop]

            if uid_values.shape[0] != len(years_chunk):
                raise ValueError(
                    "Decoded selected_source_station_uid shape does not match time chunk "
                    "in {} for {}:{}.".format(nc_path.name, start, stop)
                )

            uid_flat = uid_values.reshape((uid_values.shape[0], -1))
            mask = valid_uid_mask(uid_flat)

            # Read flag variables for the same time slice
            qf_chunk = slice_variable_by_time(q_flag_var, raw_time_axis, start, stop)
            sf_chunk = slice_variable_by_time(ssc_flag_var, raw_time_axis, start, stop)
            lf_chunk = slice_variable_by_time(ssl_flag_var, raw_time_axis, start, stop)

            # Move time axis to position 0 (same transformation as uid_values)
            qf_moved = np.moveaxis(np.asarray(qf_chunk), raw_time_axis, 0)
            sf_moved = np.moveaxis(np.asarray(sf_chunk), raw_time_axis, 0)
            lf_moved = np.moveaxis(np.asarray(lf_chunk), raw_time_axis, 0)

            # Reshape to (n_time, n_clusters) to match uid_flat
            qf_flat = qf_moved.reshape((qf_moved.shape[0], -1))
            sf_flat = sf_moved.reshape((sf_moved.shape[0], -1))
            lf_flat = lf_moved.reshape((lf_moved.shape[0], -1))

            # At least one of Q, SSC, SSL must have non-missing data (flag != 9)
            has_data = (qf_flat != 9) | (sf_flat != 9) | (lf_flat != 9)

            # Combine: valid UID AND at least one non-missing flag
            effective_mask = mask & has_data

            if not effective_mask.any():
                continue

            year_flat = np.repeat(years_chunk, uid_flat.shape[1])[effective_mask.ravel()]
            selected_uids = uid_flat[effective_mask]

            source_names = [
                source_lookup.get(str(uid).strip(), "Unknown source")
                for uid in selected_uids
            ]

            chunk_df = pd.DataFrame(
                {
                    "resolution": resolution,
                    "year": year_flat.astype(np.int32),
                    "source_name": source_names,
                }
            )

            grouped = (
                chunk_df.groupby(["resolution", "year", "source_name"], as_index=False)
                .size()
                .rename(columns={"size": "n_matrix_records"})
            )
            rows.append(grouped)

    if not rows:
        return pd.DataFrame(
            columns=["resolution", "year", "source_name", "n_matrix_records"]
        )

    out = pd.concat(rows, ignore_index=True)
    out = (
        out.groupby(["resolution", "year", "source_name"], as_index=False)[
            "n_matrix_records"
        ]
        .sum()
        .sort_values(["resolution", "year", "source_name"])
        .reset_index(drop=True)
    )

    return out


def load_annual_record_counts(
    release_dir: Path,
    source_lookup: Dict[str, str],
) -> pd.DataFrame:
    pieces = []

    for resolution in RESOLUTIONS:
        if resolution not in MATRIX_FILES:
            raise ValueError("Unknown resolution: {}".format(resolution))

        nc_path = release_dir / MATRIX_FILES[resolution]
        print("Reading {}".format(nc_path))

        df = count_resolution_records(
            nc_path=nc_path,
            resolution=resolution,
            source_lookup=source_lookup,
        )
        pieces.append(df)

    counts = pd.concat(pieces, ignore_index=True)

    if counts.empty:
        raise ValueError("No selected_source_station_uid records were found.")

    counts = (
        counts.groupby(["resolution", "year", "source_name"], as_index=False)[
            "n_matrix_records"
        ]
        .sum()
        .sort_values(["resolution", "year", "source_name"])
        .reset_index(drop=True)
    )

    return counts


def build_source_groups(raw_counts: pd.DataFrame) -> List[str]:
    selected_sources = set()

    for resolution in RESOLUTIONS:
        sub = raw_counts[raw_counts["resolution"] == resolution]
        if sub.empty:
            continue

        top_sources = (
            sub.groupby("source_name")["n_matrix_records"]
            .sum()
            .sort_values(ascending=False)
            .head(TOP_N_PER_RESOLUTION)
            .index
        )
        selected_sources.update(top_sources)

    total_order = (
        raw_counts.groupby("source_name")["n_matrix_records"]
        .sum()
        .sort_values(ascending=False)
    )

    source_order = [src for src in total_order.index if src in selected_sources]

    if len(selected_sources) < raw_counts["source_name"].nunique():
        source_order.append(OTHER_LABEL)

    return source_order


def prepare_plot_counts(raw_counts: pd.DataFrame, source_order: Sequence[str]) -> pd.DataFrame:
    selected_sources = {src for src in source_order if src != OTHER_LABEL}

    df = raw_counts.copy()
    df["source_group"] = np.where(
        df["source_name"].isin(selected_sources),
        df["source_name"],
        OTHER_LABEL,
    )

    plot_df = (
        df.groupby(["resolution", "year", "source_group"], as_index=False)[
            "n_matrix_records"
        ]
        .sum()
        .sort_values(["resolution", "year", "source_group"])
        .reset_index(drop=True)
    )

    return plot_df


# ============================================================
# Plotting
# ============================================================

def build_color_map(source_order: Sequence[str]) -> Dict[str, str]:
    """Map each source group to a colorblind-safe palette color.
    
    The "Other matrix sources" group always gets grey. Named sources
    cycle through the extended Okabe-Ito palette without duplicates.
    """
    color_map = {}
    palette_idx = 0
    for source in source_order:
        if source == OTHER_LABEL:
            color_map[source] = OTHER_COLOR
        else:
            color_map[source] = OKABE_ITO_EXTENDED[
                palette_idx % len(OKABE_ITO_EXTENDED)
            ]
            palette_idx += 1
    return color_map


def draw_resolution_panel(
    ax,
    plot_counts: pd.DataFrame,
    resolution: str,
    source_order: Sequence[str],
    color_map: Dict[str, str],
    year_min: int,
    year_max: int,
) -> None:
    sub = plot_counts[plot_counts["resolution"] == resolution].copy()

    years = np.arange(year_min, year_max + 1)

    if sub.empty:
        ax.text(
            0.5,
            0.5,
            "No records",
            transform=ax.transAxes,
            ha="center",
            va="center",
        )
        ax.set_title(PANEL_TITLES[resolution], loc="left", weight="bold")
        return

    pivot = (
        sub.pivot_table(
            index="year",
            columns="source_group",
            values="n_matrix_records",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(index=years, fill_value=0)
        .reindex(columns=source_order, fill_value=0)
    )

    y_arrays = [pivot[col].to_numpy(dtype=float) for col in source_order]
    colors = [color_map[col] for col in source_order]

    if PLOT_KIND == "area":
        ax.stackplot(years, y_arrays, labels=source_order, colors=colors, alpha=0.92)
    elif PLOT_KIND == "bar":
        bottom = np.zeros(len(years), dtype=float)
        for label, values, color in zip(source_order, y_arrays, colors):
            ax.bar(
                years,
                values,
                bottom=bottom,
                width=0.9,
                label=label,
                color=color,
                linewidth=0,
            )
            bottom = bottom + values
    else:
        raise ValueError("Unsupported PLOT_KIND: {}".format(PLOT_KIND))

    # Overlay thin black line for total records per year (all sources)
    total_per_year = pivot.sum(axis=1)
    ax.plot(
        total_per_year.index,
        total_per_year.values,
        color="black",
        linewidth=0.6,
        linestyle="-",
        zorder=5,
    )

    ax.set_title(PANEL_TITLES[resolution], loc="left", weight="bold")
    ax.set_ylabel("Matrix records per year" if resolution == "monthly" else "")
    ax.yaxis.set_major_formatter(FuncFormatter(compact_count))
    ax.grid(axis="y", linewidth=0.35, alpha=0.5)
    ax.set_axisbelow(True)

    max_total = pivot.sum(axis=1).max()
    if pd.notna(max_total) and max_total > 0:
        ax.set_ylim(0, max_total * 1.12)


def draw_three_panel_plot(
    plot_counts: pd.DataFrame,
    source_order: Sequence[str],
    figure_dirs: Dict[str, Path],
) -> Tuple[Path, Path, Tuple[float, float]]:
    if plot_counts.empty:
        raise ValueError("No plotting data available.")

    year_min = int(np.floor(plot_counts["year"].min() / 10.0) * 10)
    year_max = int(np.ceil(plot_counts["year"].max() / 10.0) * 10)

    figsize = (WIDTH_CM / CM_PER_INCH, HEIGHT_CM / CM_PER_INCH)
    fig, axes = plt.subplots(
        3,
        1,
        figsize=figsize,
        sharex=True,
        gridspec_kw={"hspace": 0.22},
    )

    color_map = build_color_map(source_order)

    for ax, resolution in zip(axes, RESOLUTIONS):
        draw_resolution_panel(
            ax=ax,
            plot_counts=plot_counts,
            resolution=resolution,
            source_order=source_order,
            color_map=color_map,
            year_min=year_min,
            year_max=year_max,
        )

    axes[-1].set_xlabel("Year")
    axes[-1].set_xlim(year_min, year_max)

    legend_handles = [
        Patch(facecolor=color_map[source], edgecolor="none", label=source)
        for source in source_order
    ]
    legend_handles.append(
        Line2D([0], [0], color="black", linewidth=0.6, label="Total records")
    )

    fig.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.07),
        ncol=4,
        frameon=True,
    )

    # note = (
    #     "Counts represent non-empty station-time cells with selected_source_station_uid. "
    #     "Panels are counted separately from the daily, monthly, and annual released matrices. "
    #     "Record density indicates data availability and source contribution, not hydrological trends."
    # )
    # fig.text(
    #     0.01,
    #     0.005,
    #     note,
    #     ha="left",
    #     va="bottom",
    #     fontsize=MIN_VISIBLE_FONT_SIZE,
    # )

    fig.subplots_adjust(left=0.09, right=0.98, top=0.96, bottom=0.13)

    png_path = figure_dirs["final"] / "{}.png".format(FIGURE_ID)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(FIGURE_ID)

    fig.savefig(png_path, dpi=DPI, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight", metadata={"Creator": "Python Matplotlib"})
    plt.close(fig)

    return png_path, pdf_path, figsize


# ============================================================
# Checklist
# ============================================================

def write_checklist(
    checklist_path: Path,
    pdf_path: Path,
    png_path: Path,
    raw_counts_path: Path,
    plot_counts_path: Path,
    source_group_path: Path,
    script_copy_path: Path,
    figsize: Tuple[float, float],
) -> Path:
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(["pdffonts", str(pdf_path)])

    width_cm = figsize[0] * CM_PER_INCH
    height_cm = figsize[1] * CM_PER_INCH

    text = """# {} ESSD figure checklist

## File information
- Final PDF: `{}`
- Final PNG: `{}`
- Raw plotting data: `{}`
- Grouped plotting data: `{}`
- Source group table: `{}`
- Plotting script: `{}`
- Checklist: `{}`

## Data basis
- Release directory: `{}`
- Matrix products used: {}
- Count basis: non-empty matrix cells with valid `selected_source_station_uid`
  and at least one non-missing variable (Q_flag, SSC_flag, or SSL_flag != 9)
- Source lookup: `source_station_catalog.csv`
- Top source groups: top {} sources per resolution, unioned across daily/monthly/annual
- Plot kind: {}

## Format and resolution
- Preferred vector format used: yes, PDF
- Bitmap dpi: {}
- PDF page size: {}
- PDF file size: {}
- PNG file size: {}

## Size and layout
- Figure size: {:.1f} x {:.1f} cm ({:.1f} x {:.1f} in)
- Multi-panel layout: 3 rows x 1 column
- Panel labels: (a), (b), (c)

## Fonts
- Font family: DejaVu Sans
- Minimum visible font size: {} pt
- Font embedding setting: pdf.fonttype = 42
- Font embedding check: {}

## Color
- Colorblind-safe palette: Okabe-Ito (extended with Wong 2011)
- Coblis check: pending (recommend verification before submission)

## Legend
- All colors/symbols explained: yes
- Legend frame: enabled
- Legend placement: within figure, below panels

## Data availability
- All plotting data saved as CSV: yes
- Raw counts: `fig_annual_matrix_records_by_source_by_resolution_raw_counts.csv`
- Plot-ready grouped counts: `fig_annual_matrix_records_by_source_by_resolution_plot_counts.csv`
- Source group assignment: `fig_annual_matrix_records_by_source_by_resolution_source_groups.csv`

## Reproducibility
- Figure is drawn directly from released NetCDF matrix products and compact catalogues.
- Export date: {}
""".format(
        FIGURE_ID,
        pdf_path.name,
        png_path.name,
        raw_counts_path.name,
        plot_counts_path.name,
        source_group_path.name,
        script_copy_path.name,
        checklist_path.name,
        RELEASE_DIR,
        ", ".join(RESOLUTIONS),
        TOP_N_PER_RESOLUTION,
        PLOT_KIND,
        DPI,
        pdfinfo_output.splitlines()[0] if pdfinfo_ok and pdfinfo_output else "not checked",
        file_size_mb(pdf_path),
        file_size_mb(png_path),
        width_cm,
        height_cm,
        figsize[0],
        figsize[1],
        MIN_VISIBLE_FONT_SIZE,
        "checked" if pdffonts_ok else "not checked ({})".format(pdffonts_output),
        datetime.date.today().isoformat(),
    )

    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text(text, encoding="utf-8")
    return checklist_path


# ============================================================
# Main
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Plot annual matrix record counts by source and resolution."
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip NetCDF reading and CSV computation; re-plot from existing CSVs.",
    )
    args = parser.parse_args()

    configure_matplotlib()

    release_dir = RELEASE_DIR.expanduser().resolve()
    figure_dirs = ensure_figure_dirs(OUTPUT_DIR.expanduser().resolve())

    raw_counts_path = figure_dirs["data"] / "{}_raw_counts.csv".format(FIGURE_ID)
    plot_counts_path = figure_dirs["data"] / "{}_plot_counts.csv".format(FIGURE_ID)
    source_group_path = figure_dirs["data"] / "{}_source_groups.csv".format(FIGURE_ID)

    if args.plot_only:
        print("--plot-only mode: reading existing CSVs...")
        raw_counts = pd.read_csv(raw_counts_path)
        source_order_df = pd.read_csv(source_group_path)
        source_order = source_order_df["source_group"].tolist()
        plot_counts = pd.read_csv(plot_counts_path)
    else:
        source_lookup = read_source_lookup(release_dir)

        raw_counts = load_annual_record_counts(
            release_dir=release_dir,
            source_lookup=source_lookup,
        )

        source_order = build_source_groups(raw_counts)
        plot_counts = prepare_plot_counts(raw_counts, source_order)

        raw_counts.to_csv(raw_counts_path, index=False)
        plot_counts.to_csv(plot_counts_path, index=False)
        pd.DataFrame({"source_group": source_order}).to_csv(source_group_path, index=False)

    png_path, pdf_path, figsize = draw_three_panel_plot(
        plot_counts=plot_counts,
        source_order=source_order,
        figure_dirs=figure_dirs,
    )

    script_copy_path = copy_script(figure_dirs["scripts"])

    checklist_path = write_checklist(
        checklist_path=figure_dirs["checklists"] / "{}_checklist.md".format(FIGURE_ID),
        pdf_path=pdf_path,
        png_path=png_path,
        raw_counts_path=raw_counts_path,
        plot_counts_path=plot_counts_path,
        source_group_path=source_group_path,
        script_copy_path=script_copy_path,
        figsize=figsize,
    )

    print("Wrote {}".format(pdf_path))
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(raw_counts_path))
    print("Wrote {}".format(plot_counts_path))
    print("Wrote {}".format(source_group_path))
    print("Copied script to {}".format(script_copy_path))
    print("Wrote {}".format(checklist_path))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
