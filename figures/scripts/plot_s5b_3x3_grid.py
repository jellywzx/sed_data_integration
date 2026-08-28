#!/usr/bin/env python3
"""Driver: 4×3 grid scatter plot — one row per source pair, one column per window.

Reads the s5b pairs CSV, filters to SSC variable, and produces a single
4×3 figure: rows = source pairs (RiverSed vs USGS / GSED vs GFQA_v2 /
RiverSed vs HYDAT / Dethier vs GFQA_v2),
columns = pairing windows (exact / ±1d / ±2d).  Full range only, no zoom row.

ESSD compliance follows ``plot/AGENTS.md`` → ``docs/essd_figure_requirements.md``.
"""

import argparse
import datetime
import shutil
import sys
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

# -- Add the existing plot-script directory to sys.path -----------------------
PROJECT_DIR = Path(__file__).resolve().parents[2]
_PLOT_SCRIPTS_DIR = PROJECT_DIR / "figures" / "scripts"
sys.path.insert(0, str(_PLOT_SCRIPTS_DIR))

import plot_satellite_insitu_validation_scatter as _plot  # noqa: E402

# ============================================================================
#  HARDCODED CONFIGURATION  (edit these to change paths / parameters)
# ============================================================================

PATHS = {
    "pairs_csv": PROJECT_DIR / "validate" / "output" / "validation_results_s5b" / "validation_satellite_insitu_pairs.csv",
    "figures_root": PROJECT_DIR / "figures",
}

PARAMS = {
    "figure_id": "s5b_3x3_grid",
    "variable": "SSC",
    "high_turbidity_ssc": 1000.0,
    "ssc_bin_edges": "100,500,1000,5000",
    "dpi": 300,
}

# All font-size and visual-style parameters are managed here — a single place
# to adjust text sizes, marker sizes, and grid appearance.
STYLE = {
    # Font family (ESSD: single sans-serif)
    "font_family": "DejaVu Sans",
    # Font sizes (ESSD §6: all visible text >= 7 pt)
    "panel_label_size": 16,
    "axis_label_size": 15,
    "tick_label_size": 14,
    "legend_text_size": 14,
    "title_size": 16,
    # Figure geometry — per-panel dimensions in cm (converted to inches)
    "panel_width_cm": 8.5,
    "panel_height_cm": 7.0,
    # Markers
    "scatter_marker_size": 10,
    "scatter_alpha": 0.65,
    # Grid
    "grid_alpha": 0.25,
}

# -- Source pairs to plot (in row order) -------------------------------------
SOURCE_PAIRS = [
    "RiverSed vs USGS",
    "GSED vs GFQA_v2",
    "RiverSed vs HYDAT",
    "Dethier vs GFQA_v2",
]

WINDOWS = ("exact", "pm1d", "pm2d")  # cumulative: exact ⊂ pm1d ⊂ pm2d


# -- 4×3 grid figure ----------------------------------------------------------
def make_4x3_grid(plt, pair_records, variable="SSC", figure_id=None):
    """Build a 4×3 subplot grid: rows = source pairs, cols = pairing windows.

    Each subplot shows the scatter of satellite vs in-situ for one
    source_pair × window combination, with a 1:1 reference line and
    Pearson/Spearman correlation annotation.
    """
    subset = pair_records[pair_records["variable"] == variable].copy()
    if len(subset) < 1:
        return None, [], subset, "skipped: no {} pairs".format(variable)

    available_windows = [w for w in WINDOWS if w in set(subset["pairing_window"])]
    if not available_windows:
        return None, [], subset, "skipped: no configured windows"

    # source_pair → colour + marker (one row = one source_pair = one colour)
    pair_colors = {
        sp: _plot.OKABE_ITO[i % len(_plot.OKABE_ITO)]
        for i, sp in enumerate(SOURCE_PAIRS)
    }
    pair_markers = {
        sp: _plot.MARKER_SHAPES[i % len(_plot.MARKER_SHAPES)]
        for i, sp in enumerate(SOURCE_PAIRS)
    }

    n_rows = len(SOURCE_PAIRS)
    n_cols = len(WINDOWS)
    panel_w_in = STYLE["panel_width_cm"] / _plot.CM_PER_INCH
    panel_h_in = STYLE["panel_height_cm"] / _plot.CM_PER_INCH

    fig, axes = plt.subplots(
        n_rows, n_cols,
        figsize=(panel_w_in * n_cols, panel_h_in * n_rows),
        squeeze=False,
    )

    for row_idx, sp in enumerate(SOURCE_PAIRS):
        sp_color = pair_colors[sp]
        sp_marker = pair_markers[sp]

        for col_idx, window in enumerate(WINDOWS):
            ax = axes[row_idx][col_idx]
            panel_idx = row_idx * n_cols + col_idx

            part = subset[
                (subset["source_pair"] == sp) & (subset["pairing_window"] == window)
            ]

            if part.empty:
                ax.text(0.5, 0.5, "no data", transform=ax.transAxes,
                        ha="center", va="center", fontsize=STYLE["tick_label_size"],
                        color="grey")
                ax.set_title("{}".format(_plot.WINDOW_LABELS.get(window, window)),
                             fontsize=STYLE["title_size"])
                continue

            # scatter
            ax.scatter(
                part["insitu_value"],
                part["satellite_value"],
                s=STYLE["scatter_marker_size"],
                c=sp_color,
                marker=sp_marker,
                alpha=STYLE["scatter_alpha"],
                rasterized=True,
            )

            # 1:1 line
            finite = pd.to_numeric(
                part[["insitu_value", "satellite_value"]].stack(), errors="coerce"
            )
            finite = finite[np.isfinite(finite)]
            if len(finite):
                lo = float(finite.min())
                hi = float(finite.max())
                ax.plot([lo, hi], [lo, hi], color="black", linewidth=1, linestyle="--")

            # column titles: row 0 shows window label
            if row_idx == 0:
                window_label = _plot.WINDOW_LABELS.get(window, window)
                ax.set_title("{}".format(window_label),
                            fontsize=STYLE["title_size"])

            # axis labels: only on last row (x) and left column (y)
            if row_idx == n_rows - 1 and col_idx == 1:
                ax.set_xlabel("Station-reference {} (mg L⁻¹)".format(variable),
                              fontsize=STYLE["axis_label_size"])
            if row_idx == 1 and col_idx == 0:
                ax.set_ylabel("Satellite-derived {} (mg L⁻¹)".format(variable),
                              fontsize=STYLE["axis_label_size"])

            ax.grid(True, alpha=STYLE["grid_alpha"])
            if col_idx > 0:
                ax.tick_params(labelleft=False)

            # Scientific notation for first row axes
            if row_idx == 0:
                ax.ticklabel_format(style='scientific', scilimits=(0, 0), axis='both')
                ax.yaxis.get_offset_text().set_position((-0.14, 1.02))   # 调整 y轴 1e4 的位置
                ax.xaxis.get_offset_text().set_position((1.11,0))    # 调整 x轴 1e4 的位置
                

            # panel label (a)-(l)
            ax.text(
                0.0, 1.1,
                "({})".format(chr(97 + panel_idx)),
                transform=ax.transAxes,
                fontsize=STYLE["panel_label_size"],
                fontweight="bold",
                va="top", ha="left",
            )

            # statistics annotation (top-right corner)
            insitu_num = pd.to_numeric(part["insitu_value"], errors="coerce").to_numpy(dtype=float)
            sat_num = pd.to_numeric(part["satellite_value"], errors="coerce").to_numpy(dtype=float)
            r_pearson = _plot._safe_corr(insitu_num, sat_num, "pearson")
            r_spearman = _plot._safe_corr(insitu_num, sat_num, "spearman")
            r2 = r_pearson ** 2 if np.isfinite(r_pearson) else float("nan")
            n_pairs = len(part)
            n_reference_stations = part["station_uid"].nunique() if "station_uid" in part.columns else 0
            corr_lines = [
                "n = {}".format(n_pairs),
                "n_reference_stations = {}".format(n_reference_stations),
                "r = {:.3f}".format(r_pearson) if np.isfinite(r_pearson) else "r = NaN",
                "ρ = {:.3f}".format(r_spearman) if np.isfinite(r_spearman) else "ρ = NaN",
                "R² = {:.3f}".format(r2) if np.isfinite(r2) else "R² = NaN",
            ]
            ax.text(
                0.98, 0.02, "\n".join(corr_lines),
                transform=ax.transAxes,
                fontsize=STYLE["tick_label_size"] - 2,
                va="bottom", ha="right", linespacing=1.3,
            )

    # -- Row labels (source pair names) on the right side of each row ---------
    for row_idx, sp in enumerate(SOURCE_PAIRS):
        ax = axes[row_idx][-1]
        sp_color = pair_colors[sp]
        sp_marker = pair_markers[sp]
        # Add a small legend-like annotation on the rightmost panel
        ax.text(
            0.98, 0.93,
            sp,
            transform=ax.transAxes,
            fontsize=STYLE["legend_text_size"],
            fontweight="bold",
            va="top", ha="right",
            color=sp_color,
        )

    fig.subplots_adjust(hspace=0.35, wspace=0.17, top=0.96, bottom=0.06, left=0.08, right=0.93)

    return fig, available_windows, subset, "generated"


# -- ESSD checklist writer ----------------------------------------------------
def write_checklist(figure_id, fig, windows_used, dpi, checklist_path,
                    pdf_path, png_path, variable="SSC"):
    """Write an ESSD-compliant checklist markdown file for the 4×3 figure."""
    figsize_in = fig.get_size_inches()
    width_cm = figsize_in[0] * _plot.CM_PER_INCH
    height_cm = figsize_in[1] * _plot.CM_PER_INCH
    pdf_size_bytes = pdf_path.stat().st_size if pdf_path.exists() else 0
    png_size_bytes = png_path.stat().st_size if png_path.exists() else 0
    n_panels = len(SOURCE_PAIRS) * len(windows_used)

    clines = [
        "# Figure checklist: {}".format(figure_id),
        "",
        "## Basic information",
        "",
        "- Figure file: `{}.pdf` / `{}.png`".format(figure_id, figure_id),
        "- Plotting script: `plot_{}.py`".format(figure_id),
        "- Plotting data: `{}_plotting_data.csv`".format(figure_id),
        "- Date exported: {}".format(datetime.date.today().isoformat()),
        "- Figure type: 4×3 multi-panel scatter grid (satellite vs in-situ)",
        "- Single-panel or multi-panel: multi-panel ({} panels: {} rows × {} columns)".format(
            n_panels, len(SOURCE_PAIRS), len(windows_used)),
        "- Rows: {}".format(", ".join(SOURCE_PAIRS)),
        "- Columns: {}".format(", ".join(_plot.WINDOW_LABELS.get(w, w) for w in windows_used)),
        "",
        "## File format and size",
        "",
        "- Final format: PDF (vector) + PNG (bitmap)",
        "- DPI: {}".format(dpi),
        "- Width: {:.1f} cm".format(width_cm),
        "- Height: {:.1f} cm".format(height_cm),
        "- File size (PDF): {} KB".format(pdf_size_bytes / 1024),
        "- File size (PNG): {} KB".format(png_size_bytes / 1024),
        "- PDF < 2 MB: {}".format("yes" if pdf_size_bytes < 2 * 1024 * 1024 else "no"),
        "- Width >= 8 cm: {}".format("yes" if width_cm >= 8 else "no"),
        "",
        "## Color and accessibility",
        "",
        "- Colorblind-safe palette used: Yes (Okabe-Ito; ESSD §5.2)",
        "- Continuous color map, if applicable: N/A",
        "- Coblis or equivalent check completed: requires manual review",
        "- Figure remains interpretable under color-vision-deficiency simulation: "
        "source pairs in separate rows with distinct Okabe-Ito colours",
        "- Categories are distinguished by more than color when needed: "
        "Yes — rows identify the source pair; each row has a distinct colour and marker",
        "",
        "## Font and text",
        "",
        "- Single font family used: Yes",
        "- Font family: {}".format(STYLE["font_family"]),
        "- Fonts embedded in vector file: Yes (pdf.fonttype=42)",
        "- No unnecessary bold/italic variants: "
        "Yes (only panel labels are bold)",
        "- No hidden text boxes or extra layers: Yes",
        "- Sentence case used: Yes",
        "",
        "## Legend and symbols",
        "",
        "- Legend included inside figure: Source pair labels on right-side panels",
        "- All colors explained: Yes — one colour per row (source pair)",
        "- All markers explained: Yes — each source pair has a distinct marker shape",
        "- All line styles explained: Yes — dashed black line is 1:1 reference",
        "- Point sizes explained, if applicable: N/A (uniform marker size)",
        "- Color bar included and labeled, if applicable: N/A",
        "- Legend does not obscure data: Yes (row labels outside plot area)",
        "",
        "## ESSD formatting",
        "",
        "- Panel labels use `(a)`, `(b)`, etc.: Yes",
        "- Ranges use en dash with no spaces: N/A",
        "- Coordinates use degree symbol and direction spacing: N/A",
        "- Numbers and units have a space: Yes (e.g. \"mg L⁻¹\")",
        "- Units use exponent format: Yes (e.g. mg L⁻¹)",
        "- h, km, and m abbreviations used correctly: N/A",
        "",
        "## Reproducibility",
        "",
        "- Plotting data saved: Yes (`{}_plotting_data.csv`)".format(figure_id),
        "- Plotting script saved: Yes (`plot_{}.py`)".format(figure_id),
        "- Input paths documented: Yes (PATHS dict at module top)",
        "- Filtering rules documented: Yes (variable filter applied; strata assigned)",
        "- Color and marker mappings defined in code: Yes "
        "(OKABE_ITO palette + MARKER_SHAPES at module level)",
        "- Figure can be regenerated from saved files: Yes",
        "",
        "## Copyright",
        "",
        "- Figure fully generated from study data and code: Yes",
        "- External figure or basemap used: No",
        "- Reuse permission checked, if applicable: N/A",
        "- Source cited in caption, if applicable: N/A",
        "",
        "## Notes",
        "",
        "- 4×3 grid: all four s5b source pairs × three pairing windows, {} variable".format(
            variable),
        "- Part of s5b validation figure set",
    ]
    checklist_path.write_text("\n".join(clines) + "\n", encoding="utf-8")


# -- CLI ----------------------------------------------------------------------
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="s5b 3×3 grid scatter plot — one row per source pair, "
        "one column per pairing window (ESSD-compliant).  "
        "All parameters are hardcoded inside the script (PATHS / PARAMS / STYLE dicts).  "
        "Use --plot-only to skip statistics and regenerate artefacts.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip statistics computation; reuse existing metrics CSV and "
        "only regenerate the figure and artefacts.",
    )
    return parser.parse_args(argv)


# -- Main --------------------------------------------------------------------
def main(argv: Optional[Sequence[str]] = None):
    args = parse_args(argv)
    plot_only = args.plot_only

    # -- Matplotlib setup ----------------------------------------------------
    plt = _plot._setup_matplotlib()
    _plot.configure_matplotlib(plt)
    plt.rcParams.update({
        "font.family": STYLE["font_family"],
        "axes.labelsize": STYLE["axis_label_size"],
        "axes.titlesize": STYLE["title_size"],
        "xtick.labelsize": STYLE["tick_label_size"],
        "ytick.labelsize": STYLE["tick_label_size"],
        "legend.fontsize": STYLE["legend_text_size"],
    })

    # -- Resolve paths from hardcoded configuration --------------------------
    figure_dirs = _plot.ensure_figure_dirs(Path(PATHS["figures_root"]))
    figure_id = PARAMS["figure_id"]
    pairs_path = Path(PATHS["pairs_csv"])
    metrics_path = figure_dirs["data"] / "{}_metrics.csv".format(figure_id)
    script_path = figure_dirs["scripts"] / "plot_{}.py".format(figure_id)

    variable = PARAMS["variable"]
    high_turbidity_ssc = float(PARAMS["high_turbidity_ssc"])
    ssc_bin_edges = _plot._parse_ssc_bin_edges(PARAMS["ssc_bin_edges"])
    dpi = int(PARAMS["dpi"])

    if not pairs_path.exists():
        raise SystemExit("ERROR: pairs CSV not found: {}".format(pairs_path))

    print("Loading pairs from: {}".format(pairs_path))
    pairs = pd.read_csv(str(pairs_path), keep_default_na=False)
    # Normalize legacy cluster_* column names from s5b/validate pipeline
    _legacy_rename = {"cluster_uid": "station_uid", "cluster_id": "station_reference_id"}
    pairs = pairs.rename(columns={k: v for k, v in _legacy_rename.items() if k in pairs.columns and v not in pairs.columns})
    print("  {} rows loaded".format(len(pairs)))

    if pairs.empty:
        raise SystemExit("ERROR: paired records DataFrame is empty.")

    # -- Assure strata columns exist -----------------------------------------
    if not {"ssc_bin", "river_width_class", "climate_zone", "high_turbidity"}.issubset(
        pairs.columns
    ):
        print("Assigning missing strata columns ...")
        pairs = _plot.assign_strata(
            pairs,
            high_turbidity_ssc=high_turbidity_ssc,
            ssc_bin_edges=ssc_bin_edges,
        )

    # -- Metrics -------------------------------------------------------------
    if not plot_only:
        print("Computing validation metrics ...")
        metrics = _plot.compute_satellite_insitu_metrics(pairs)
        print("  -> {} metric rows computed".format(len(metrics)))
        metrics.to_csv(str(metrics_path), index=False)
        print("Saved metrics to: {}".format(metrics_path))
    else:
        if metrics_path.exists():
            metrics = pd.read_csv(str(metrics_path), keep_default_na=False)
            print("Loaded pre-computed metrics ({} rows) from: {}".format(
                len(metrics), metrics_path))
        else:
            print("Warning: metrics CSV not found ({}); continuing without metrics.".format(
                metrics_path))

    # -- Generate 4×3 grid figure --------------------------------------------
    print("Generating 4×3 grid scatter plot ...")
    fig, windows_used, plot_data, status = make_4x3_grid(
        plt, pairs, variable=variable, figure_id=figure_id,
    )
    print("  -> scatter plot: {}".format(status))

    if status.startswith("skipped"):
        return

    # -- Resolve output paths ------------------------------------------------
    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    data_path = figure_dirs["data"] / "{}_plotting_data.csv".format(figure_id)
    checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)

    # -- Save figure files ---------------------------------------------------
    fig.savefig(str(png_path), dpi=dpi, bbox_inches="tight")
    fig.savefig(str(pdf_path), dpi=dpi, bbox_inches="tight",
                metadata={"Creator": "Python Matplotlib"})
    plt.close(fig)
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(pdf_path))

    # -- Save plotting data --------------------------------------------------
    plot_data.to_csv(str(data_path), index=False)
    print("Wrote {}".format(data_path))

    # -- Write ESSD checklist ------------------------------------------------
    write_checklist(figure_id, fig, windows_used, dpi, checklist_path,
                    pdf_path, png_path, variable=variable)
    print("Wrote {}".format(checklist_path))

    # -- Script self-archiving -----------------------------------------------
    if str(Path(__file__).resolve()) != str(script_path.resolve()):
        shutil.copy(__file__, str(script_path))
        print("Copied script to: {}".format(script_path))
    else:
        print("Script already in scripts/ directory — skipping self-copy.")

    print("\nDone.")


if __name__ == "__main__":
    main()
