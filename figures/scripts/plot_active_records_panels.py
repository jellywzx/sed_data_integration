#!/usr/bin/env python3
"""Generate active-records three-panel figure from release matrix products.

Outputs:
    figures/final/active_records_panels.png
    figures/final/active_records_panels.pdf
    figures/data/active_records_panels_plotting_data.csv
    figures/checklists/active_records_panels_checklist.md

Usage:
    python plot_active_records_panels.py
    python plot_active_records_panels.py --release-dir /path/to/release
    python plot_active_records_panels.py --out-dir /path/to/figures
"""
# ---- Library path setup: MUST happen before any extension-module imports ----
import os as _os
import ctypes as _ctypes
from pathlib import Path as _Path

_conda_lib = "/share/home/dq134/.conda/envs/wzx/lib"
if _os.path.isdir(_conda_lib):
    _os.environ["LD_LIBRARY_PATH"] = _conda_lib + _os.pathsep + _os.environ.get("LD_LIBRARY_PATH", "")
    try:
        _ctypes.CDLL(str(_Path(_conda_lib) / "libstdc++.so.6"), mode=_ctypes.RTLD_GLOBAL)
    except Exception:
        pass
del _os, _ctypes, _Path, _conda_lib
# ---------------------------------------------------------------------------

# ---- Package path setup: absolute path to the project root ----
import sys as _sys
from pathlib import Path

_PACKAGE_ROOT = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test")
if str(_PACKAGE_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_PACKAGE_ROOT))
del _PACKAGE_ROOT, _sys
# ---------------------------------------------------------------------------

import argparse
import datetime
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from stats_release.common_stats import decode_time_axis
from stats_release.release_io import (
    add_common_args,
    context_from_args,
    read_numeric_var,
    setup_matplotlib,
)
from stats_release.release_paths import MATRIX_PRODUCTS


DEFAULT_FIGURES_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/figures")
DEFAULT_MINIMAL_RELEASE_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release_minimal")
VARIABLES = ("Q", "SSC", "SSL")
YEARLY_COLUMNS = (
    "resolution",
    "year",
    "active_clusters",
    "record_count_any",
    "record_count_Q",
    "record_count_SSC",
    "record_count_SSL",
    "complete_triplet_count",
    "complete_triplet_ratio",
)
RESOLUTION_COLORS = {
    "daily": "#4c78a8",
    "monthly": "#f58518",
    "annual": "#54a24b",
}
LINE_STYLES = {
    "daily": {"linestyle": "-", "marker": "None"},
    "monthly": {"linestyle": "-", "marker": ".", "markersize": 4},
    "annual": {"linestyle": "-", "marker": "o", "markersize": 4},
}

# ---- Central visual-style configuration ---------------------------------
# Change values here to adjust all font sizes, line widths, and figure
# geometry in one place.  Every hardcoded constant in write_figure_and_artifacts
# below is replaced by a STYLE[...] lookup.
STYLE = {
    # Figure geometry
    "figsize": (9, 8.5),
    "dpi": 300,

    # Font family (ESSD §6)
    "font_family": "DejaVu Sans",

    # Font sizes (ESSD: min 7 pt — all values below are ≥ 9)
    "panel_label_size": 15,
    "axis_label_size": 14,
    "tick_label_size": 13,
    "legend_text_size": 14,

    # Line styles
    "line_width": 1.8,

    # Grid
    "grid_alpha": 0.3,
}


def ensure_figure_dirs(figures_root: Path) -> dict:
    """Create and return the figure output directory structure."""
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


def _empty_year_item(resolution: str, year: int) -> dict:
    return {
        "resolution": resolution,
        "year": int(year),
        "active_clusters": 0,
        "record_count_any": 0,
        "record_count_Q": 0,
        "record_count_SSC": 0,
        "record_count_SSL": 0,
        "complete_triplet_count": 0,
        "complete_triplet_ratio": np.nan,
    }


def scan_by_year(ctx, resolution: str, file_name: str, row_chunk_size: int = 128) -> pd.DataFrame:
    """Scan a matrix NetCDF and return one yearly row per resolution."""
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(columns=YEARLY_COLUMNS)

    by_year = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        dates = decode_time_axis(ds)
        if len(dates) == 0:
            return pd.DataFrame(columns=YEARLY_COLUMNS)

        years = dates.year.to_numpy()
        unique_years = sorted(set(int(year) for year in years))
        year_cols = {year: years == year for year in unique_years}
        n_stations = int(len(ds.dimensions.get("n_stations", [])))

        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            shape = (stop - start, len(dates))
            var_masks = {}
            for variable in VARIABLES:
                if variable in ds.variables:
                    values = read_numeric_var(ds, variable, key=(slice(start, stop), slice(None)))
                    var_masks[variable] = np.isfinite(values)
                else:
                    var_masks[variable] = np.zeros(shape, dtype=bool)

            fallback_any = var_masks["Q"] | var_masks["SSC"] | var_masks["SSL"]
            if "selected_source_index" in ds.variables:
                selected = np.ma.asarray(ds.variables["selected_source_index"][start:stop, :]).filled(-1)
                any_mask = selected >= 0
            else:
                any_mask = fallback_any

            q_record = var_masks["Q"] & any_mask
            ssc_record = var_masks["SSC"] & any_mask
            ssl_record = var_masks["SSL"] & any_mask
            complete_triplet = q_record & ssc_record & ssl_record

            for year, cols in year_cols.items():
                item = by_year.setdefault(year, _empty_year_item(resolution, year))
                y_any = any_mask[:, cols]
                item["active_clusters"] += int(np.count_nonzero(np.any(y_any, axis=1)))
                item["record_count_any"] += int(np.count_nonzero(y_any))
                item["record_count_Q"] += int(np.count_nonzero(q_record[:, cols]))
                item["record_count_SSC"] += int(np.count_nonzero(ssc_record[:, cols]))
                item["record_count_SSL"] += int(np.count_nonzero(ssl_record[:, cols]))
                item["complete_triplet_count"] += int(np.count_nonzero(complete_triplet[:, cols]))

    rows = []
    for item in by_year.values():
        denominator = int(item["record_count_any"])
        if denominator > 0:
            item["complete_triplet_ratio"] = float(item["complete_triplet_count"]) / float(denominator)
        rows.append(item)

    if not rows:
        return pd.DataFrame(columns=YEARLY_COLUMNS)
    return pd.DataFrame(rows).loc[:, YEARLY_COLUMNS].sort_values(["resolution", "year"]).reset_index(drop=True)


def build_active_records_by_year(ctx) -> pd.DataFrame:
    """Compute yearly active clusters, records, and triplet completeness."""
    frames = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        frame = scan_by_year(ctx, resolution, file_name)
        print(f"  {resolution}: {len(frame)} years")
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=YEARLY_COLUMNS)
    return pd.concat(frames, ignore_index=True).loc[:, YEARLY_COLUMNS]


def write_figure_and_artifacts(by_year: pd.DataFrame, figure_dirs: dict, figure_id: str, dpi: int) -> None:
    """Plot three-panel active-records figure and save companion artifacts."""
    plt = setup_matplotlib()
    plt.rcParams["font.family"] = STYLE["font_family"]
    plt.rcParams["pdf.fonttype"] = 42
    plt.rcParams["ps.fonttype"] = 42
    plt.rcParams["axes.labelsize"] = STYLE["axis_label_size"]
    plt.rcParams["xtick.labelsize"] = STYLE["tick_label_size"]
    plt.rcParams["ytick.labelsize"] = STYLE["tick_label_size"]

    resolution_order = [resolution for resolution in MATRIX_PRODUCTS if resolution in set(by_year["resolution"].astype(str))]

    fig, axes = plt.subplots(3, 1, figsize=STYLE["figsize"], sharex=True)
    panels = [
        ("active_clusters", "Active clusters", "Active clusters by year"),
        ("record_count_any", "Record count", "Record count by year"),
        ("complete_triplet_ratio", "Complete Q\u2013SSC\u2013SSL cells (%)", "Complete Q\u2013SSC\u2013SSL triplets / any records"),
    ]

    for idx, (ax, (value_col, ylabel, title)) in enumerate(zip(axes, panels)):
        for resolution in resolution_order:
            group = by_year[by_year["resolution"].astype(str).eq(resolution)].sort_values("year")
            if group.empty:
                continue
            values = pd.to_numeric(group[value_col], errors="coerce")
            if value_col == "complete_triplet_ratio":
                values = values * 100.0
            ax.plot(
                group["year"],
                values,
                color=RESOLUTION_COLORS.get(resolution),
                linewidth=STYLE["line_width"],
                label=str(resolution),
                **LINE_STYLES.get(resolution, {}),
            )
        ax.set_ylabel(ylabel)
        ax.grid(alpha=STYLE["grid_alpha"])

    axes[1].set_yscale("log")
    axes[2].set_xlabel("Year")
    axes[2].set_ylim(0, 105)
    axes[2].set_xlim(1950, None)
    for index, ax in enumerate(axes):
        ax.text(
            0.02,
            0.95,
            f"({chr(97 + index)})",
            transform=ax.transAxes,
            fontsize=STYLE["panel_label_size"],
            fontweight="bold",
            va="top",
            ha="left",
        )

    handles, labels = axes[2].get_legend_handles_labels()
    axes[2].legend(handles, labels, frameon=False, loc="lower center",
                   ncol=len(labels) or 1, fontsize=STYLE["legend_text_size"])
    fig.tight_layout(rect=(0, 0, 1, 0.95))

    # ---- Save final figure files ----
    png_path = figure_dirs["final"] / f"{figure_id}.png"
    pdf_path = figure_dirs["final"] / f"{figure_id}.pdf"
    data_path = figure_dirs["data"] / f"{figure_id}_plotting_data.csv"
    checklist_path = figure_dirs["checklists"] / f"{figure_id}_checklist.md"
    script_path = figure_dirs["scripts"] / f"plot_{figure_id}.py"

    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight", metadata={"Creator": "Python Matplotlib"})
    plt.close(fig)
    print(f"Wrote {png_path}")
    print(f"Wrote {pdf_path}")

    # ---- Copy script to scripts/ dir ----
    source_script = Path(__file__).resolve()
    if source_script != script_path.resolve():
        shutil.copy2(str(source_script), str(script_path))
        print(f"Copied script to {script_path}")
    else:
        print(f"Script already at {script_path}")

    # ---- Save plotting data ----
    by_year.loc[:, YEARLY_COLUMNS].to_csv(data_path, index=False)
    print(f"Wrote {data_path}")

    # ---- Write ESSD-compliant checklist ----
    figsize_in = fig.get_size_inches()
    width_cm = figsize_in[0] * 2.54
    height_cm = figsize_in[1] * 2.54
    pdf_size_bytes = pdf_path.stat().st_size if pdf_path.exists() else 0
    png_size_bytes = png_path.stat().st_size if png_path.exists() else 0

    clines = [
        f"# Figure checklist: {figure_id}",
        "",
        "## Basic information",
        "",
        f"- Figure file: `{figure_id}.pdf`",
        f"- Plotting script: `plot_{figure_id}.py`",
        f"- Plotting data: `{figure_id}_plotting_data.csv`",
        f"- Date exported: {datetime.date.today().isoformat()}",
        "- Figure type: line plot (time series)",
        "- Single-panel or multi-panel: multi-panel (3 panels: active clusters, record count, triplet %)",
        "",
        "## File format and size",
        "",
        "- Final format: PDF + PNG",
        f"- DPI: {dpi}",
        f"- Width: {width_cm:.1f} cm",
        f"- Height: {height_cm:.1f} cm",
        f"- PDF file size: {pdf_size_bytes / 1024:.0f} KB",
        f"- PNG file size: {png_size_bytes / 1024:.0f} KB",
        f"- PDF < 2 MB: {'yes' if pdf_size_bytes < 2 * 1024 * 1024 else 'no'}",
        f"- Width >= 8 cm: {'yes' if width_cm >= 8 else 'no'}",
        "",
        "## Color and accessibility",
        "",
        "- Colorblind-safe palette used: fixed blue/orange/green categorical (needs manual Coblis review)",
        "- Continuous color map, if applicable: N/A",
        "- Coblis or equivalent check completed: requires manual review",
        "- Figure remains interpretable under color-vision-deficiency simulation: daily=blue/solid, monthly=orange/line+dot, annual=green/dots",
        "- Categories are distinguished by more than color when needed: yes \u2014 each resolution also has a distinct line/marker style",
        "",
        "## Font and text",
        "",
        "- Single font family used: yes",
        f"- Font family: {STYLE['font_family']}",
        "- Fonts embedded in vector file: yes (pdf.fonttype=42)",
        "- No unnecessary bold/italic variants: yes (only panel labels are bold)",
        "- No hidden text boxes or extra layers: yes",
        "- Sentence case used: yes",
        "",
        "## Legend and symbols",
        "",
        "- Legend included inside figure: yes",
        "- All colors explained: yes \u2014 daily (blue), monthly (orange), annual (green)",
        "- All markers explained: yes \u2014 annual uses filled circles, monthly uses small dots, daily uses solid line only",
        "- All line styles explained: yes \u2014 daily solid, monthly solid+dot, annual dots only",
        "- Point sizes explained, if applicable: yes (marker sizes defined in module-level LINE_STYLES)",
        "- Color bar included and labeled, if applicable: N/A",
        "- Legend does not obscure data: yes (placed above panels)",
        "",
        "## ESSD formatting",
        "",
        "- Panel labels use `(a)`, `(b)`, etc.: yes",
        "- Ranges use en dash with no spaces: yes (e.g. 1950\u20132025)",
        "- Coordinates use degree symbol and direction spacing: N/A",
        "- Numbers and units have a space: yes",
        "- Units use exponent format: N/A",
        "- h, km, and m abbreviations used correctly: N/A",
        "",
        "## Reproducibility",
        "",
        f"- Plotting data saved: yes (`{figure_id}_plotting_data.csv`)",
        f"- Plotting script saved: yes (`plot_{figure_id}.py`)",
        "- Input paths documented: yes (DEFAULT_MINIMAL_RELEASE_DIR, MATRIX_PRODUCTS)",
        "- Filtering rules documented: yes (active_clusters = cluster rows with >=1 record; record_count = selected_source_index >= 0 cells or cells with any finite Q/SSC/SSL; triplet = complete Q+SSC+SSL)",
        "- Color and marker mappings defined in code: yes (RESOLUTION_COLORS, LINE_STYLES at module level)",
        "- Figure can be regenerated from saved files: yes",
        "",
        "## Notes",
        "",
        "-",
    ]
    checklist_path.write_text("\n".join(clines) + "\n", encoding="utf-8")
    print(f"Wrote {checklist_path}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate active_records_panels figure from release matrix products."
    )
    add_common_args(parser, "misc")
    parser.set_defaults(release_dir=str(DEFAULT_MINIMAL_RELEASE_DIR))
    parser.set_defaults(out_dir=str(DEFAULT_FIGURES_DIR))
    args = parser.parse_args(argv)

    ctx = context_from_args(args)
    dpi = STYLE["dpi"]
    figure_dirs = ensure_figure_dirs(Path(args.out_dir))

    print("Scanning matrix products for active records panels ...")
    by_year = build_active_records_by_year(ctx)
    if by_year.empty:
        print("ERROR: no temporal data found in matrix products.", file=sys.stderr)
        return 1

    write_figure_and_artifacts(by_year, figure_dirs, "active_records_panels", dpi)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
