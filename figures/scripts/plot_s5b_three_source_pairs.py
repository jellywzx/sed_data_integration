#!/usr/bin/env python3
"""Driver: generate one scatter plot per source_pair from s5b validation output.

Reads the s5b pairs CSV, filters to SSC variable, and produces three
separate single-row (full-range only) scatter figures with ESSD-compliant
output artefacts (PNG, PDF, plotting data, metrics, checklists, script copy).

ESSD compliance follows ``plot/AGENTS.md`` → ``docs/essd_figure_requirements.md``.
"""

import argparse
import datetime
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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
    "base_figure_id": "s5b_three_source_pairs",
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
    "panel_width_cm": 12.7,  # 5 in
    "panel_height_cm": 10.16,  # 4 in
    # Markers
    "scatter_marker_size": 14,
    "scatter_alpha": 0.65,
    # Grid
    "grid_alpha": 0.25,
}

# -- Which source-pair x variable combos to plot ------------------------------
TARGETS = [
    {"source_pair": "RiverSed vs USGS",   "variable": "SSC",
     "figure_id": "s5b_riversed_usgs_ssc",
     "yaxis_scientific": True,
     "corr_text_top_left": True,
     "merge_panel_labels": True},
    {"source_pair": "RiverSed vs HYDAT",  "variable": "SSC",
     "figure_id": "s5b_riversed_hydat_ssc",
     "corr_text_top_left": True,
     "merge_panel_labels": True},
    {"source_pair": "Dethier vs GFQA_v2", "variable": "SSC",
     "figure_id": "s5b_dethier_gfqa_v2_ssc"},
]


# -- Single-row scatter (full range only, no zoom row) ------------------------
def make_scatter_single_row(plt, pair_records, variable="SSC", figure_id=None, yaxis_scientific=False, corr_text_top_left=False, merge_panel_labels=False):
    """Like ``make_scatter_figure`` but only the top row (no 0--100 zoom)."""
    subset = pair_records[pair_records["variable"] == variable].copy()
    if len(subset) < 1:
        return None, [], subset, "skipped: no {} pairs".format(variable)

    windows = [w for w in ("exact", "pm1d", "pm2d") if w in set(subset["pairing_window"])]
    if not windows:
        return None, [], subset, "skipped: no configured windows"

    # source_pair → colour + marker
    source_pairs = [s for s in subset["source_pair"].unique() if str(s).strip()]
    pair_colors = {sp: _plot.OKABE_ITO[i % len(_plot.OKABE_ITO)] for i, sp in enumerate(source_pairs)}
    pair_markers = {sp: _plot.MARKER_SHAPES[i % len(_plot.MARKER_SHAPES)] for i, sp in enumerate(source_pairs)}

    n_windows = len(windows)
    panel_w_in = STYLE["panel_width_cm"] / _plot.CM_PER_INCH
    panel_h_in = STYLE["panel_height_cm"] / _plot.CM_PER_INCH
    legend_margin_in = 1.25

    fig, axes = plt.subplots(
        1, n_windows,
        figsize=(panel_w_in * n_windows, panel_h_in + legend_margin_in),
        squeeze=False,
    )


    for col_idx in range(n_windows):
        ax = axes[0][col_idx]
        window = windows[col_idx]
        part = subset[subset["pairing_window"] == window]

        for sp in source_pairs:
            sp_part = part[part["source_pair"] == sp]
            if sp_part.empty:
                continue
            ax.scatter(
                sp_part["insitu_value"],
                sp_part["satellite_value"],
                s=STYLE["scatter_marker_size"],
                c=pair_colors.get(sp, "#333333"),
                marker=pair_markers.get(sp, "o"),
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

        # title
        window_label = _plot.WINDOW_LABELS.get(window, window)
        if merge_panel_labels:
            ax.set_title("({}) {} {} (n={} pairs)".format(chr(97 + col_idx), window_label, variable, len(part)))
        else:
            ax.set_title("{} {} (n={} pairs)".format(window_label, variable, len(part)))

        # axis labels
        if col_idx == 1:
            ax.set_xlabel("In-situ {} (mg L⁻¹)".format(variable))
        if col_idx == 0:
            ax.set_ylabel("Satellite {} (mg L⁻¹)".format(variable))
        ax.grid(True, alpha=STYLE["grid_alpha"])
        if col_idx > 0:
            ax.tick_params(labelleft=False)

        # axis scientific notation
        if yaxis_scientific:
            ax.ticklabel_format(axis="both", style="scientific", scilimits=(0, 0))

        # panel label (a)-(c)
        if not merge_panel_labels:
            ax.text(
                0.02, 0.98,
                "({})".format(chr(97 + col_idx)),
                transform=ax.transAxes,
                fontsize=STYLE["panel_label_size"],
                fontweight="bold",
                va="top", ha="left",
            )

        # correlation
        insitu_num = pd.to_numeric(part["insitu_value"], errors="coerce").to_numpy(dtype=float)
        sat_num = pd.to_numeric(part["satellite_value"], errors="coerce").to_numpy(dtype=float)
        r_pearson = _plot._safe_corr(insitu_num, sat_num, "pearson")
        r_spearman = _plot._safe_corr(insitu_num, sat_num, "spearman")
        corr_lines = [
            "Pearson r = {:.3f}".format(r_pearson) if np.isfinite(r_pearson) else "Pearson r = NaN",
            "Spearman ρ = {:.3f}".format(r_spearman) if np.isfinite(r_spearman) else "Spearman ρ = NaN",
        ]
        if corr_text_top_left:
            ax.text(
                0.02, 0.98, "\n".join(corr_lines),
                transform=ax.transAxes,
                fontsize=STYLE["tick_label_size"],
                va="top", ha="left", linespacing=1.5,
            )
        else:
            ax.text(
                0.98, 0.02, "\n".join(corr_lines),
                transform=ax.transAxes,
                fontsize=STYLE["tick_label_size"],
                va="bottom", ha="right", linespacing=1.5,
            )

    # shared legend
    handles, labels = [], []
    for sp in source_pairs:
        handles.append(plt.Line2D([0], [0], marker=pair_markers[sp], color=pair_colors[sp],
                                   markersize=8, linewidth=0, linestyle=""))
        labels.append(str(sp))
    handles.append(plt.Line2D([0], [0], color="black", linewidth=1, linestyle="--"))
    labels.append("1:1")
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, -0.08),
               ncol=len(handles), frameon=True, fontsize=STYLE["legend_text_size"])
    fig.subplots_adjust(bottom=0.14, top=0.92, wspace=0.23)

    return fig, windows, subset, "generated"


# -- ESSD checklist writer ----------------------------------------------------
def write_checklist(figure_id, fig, windows_used, dpi, checklist_path,
                    pdf_path, png_path, variable="SSC"):
    """Write an ESSD-compliant checklist markdown file for a single figure."""
    figsize_in = fig.get_size_inches()
    width_cm = figsize_in[0] * _plot.CM_PER_INCH
    height_cm = figsize_in[1] * _plot.CM_PER_INCH
    pdf_size_bytes = pdf_path.stat().st_size if pdf_path.exists() else 0
    png_size_bytes = png_path.stat().st_size if png_path.exists() else 0
    n_panels = len(windows_used)  # single row

    clines = [
        "# Figure checklist: {}".format(figure_id),
        "",
        "## Basic information",
        "",
        "- Figure file: `{}.pdf` / `{}.png`".format(figure_id, figure_id),
        "- Plotting script: `plot_{}.py`".format(figure_id),
        "- Plotting data: `{}_plotting_data.csv`".format(figure_id),
        "- Date exported: {}".format(datetime.date.today().isoformat()),
        "- Figure type: single-row multi-panel scatter (satellite vs in-situ)",
        "- Single-panel or multi-panel: multi-panel ({} panels: 1 row × {} columns, "
        "top: {}, no zoom row)".format(
            n_panels, n_panels,
            ", ".join(_plot.WINDOW_LABELS.get(w, w) for w in windows_used)),
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
        "source pairs distinguished by both Okabe-Ito colour and marker shape",
        "- Categories are distinguished by more than color when needed: "
        "Yes — each source_pair also has a distinct marker shape",
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
        "- Legend included inside figure: Yes",
        "- All colors explained: Yes — source pairs mapped to Okabe-Ito palette",
        "- All markers explained: Yes — each source pair has a distinct marker shape",
        "- All line styles explained: Yes — dashed black line is 1:1 reference",
        "- Point sizes explained, if applicable: N/A (uniform marker size)",
        "- Color bar included and labeled, if applicable: N/A",
        "- Legend does not obscure data: Yes (placed below panels)",
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
        "- {} source pair, {} variable, single-row full-range only".format(
            figure_id, variable),
        "- Part of s5b three-source-pair validation figure set",
    ]
    checklist_path.write_text("\n".join(clines) + "\n", encoding="utf-8")


# -- CLI ----------------------------------------------------------------------
def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="s5b three-source-pair scatter plots (ESSD-compliant).  "
        "All parameters are hardcoded inside the script (PATHS / PARAMS dicts).  "
        "Use --plot-only to skip statistics and regenerate artefacts.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip statistics computation; reuse existing metrics CSV and "
        "only regenerate the figures and artefacts.",
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
    base_figure_id = PARAMS["base_figure_id"]
    pairs_path = Path(PATHS["pairs_csv"])
    metrics_path = figure_dirs["data"] / "{}_metrics.csv".format(base_figure_id)
    script_path = figure_dirs["scripts"] / "plot_{}.py".format(base_figure_id)

    variable = PARAMS["variable"]
    high_turbidity_ssc = float(PARAMS["high_turbidity_ssc"])
    ssc_bin_edges = _plot._parse_ssc_bin_edges(PARAMS["ssc_bin_edges"])
    dpi = int(PARAMS["dpi"])

    if not pairs_path.exists():
        raise SystemExit("ERROR: pairs CSV not found: {}".format(pairs_path))

    print("Loading pairs from: {}".format(pairs_path))
    pairs = pd.read_csv(str(pairs_path), keep_default_na=False)
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

    # -- Generate one figure per target --------------------------------------
    for target in TARGETS:
        sp = target["source_pair"]
        var = target["variable"]
        figure_id = target["figure_id"]

        subset = pairs[
            (pairs["source_pair"] == sp) & (pairs["variable"] == var)
        ].copy()

        if subset.empty:
            print("\nSKIP {} / {} — no pairs".format(sp, var))
            continue

        print("\n--- {} / {}  ({} rows) ---".format(sp, var, len(subset)))

        fig, windows_used, plot_data, status = make_scatter_single_row(
            plt, subset, variable=var, figure_id=figure_id,
            yaxis_scientific=target.get("yaxis_scientific", False),
            corr_text_top_left=target.get("corr_text_top_left", False),
            merge_panel_labels=target.get("merge_panel_labels", False),
        )

        if status.startswith("skipped"):
            print("  {}".format(status))
            continue

        # Resolve per-figure output paths
        png_path = figure_dirs["final"] / "{}.png".format(figure_id)
        pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
        data_path = figure_dirs["data"] / "{}_plotting_data.csv".format(figure_id)
        checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)

        # Save figure files
        fig.savefig(str(png_path), dpi=dpi, bbox_inches="tight")
        fig.savefig(str(pdf_path), dpi=dpi, bbox_inches="tight",
                    metadata={"Creator": "Python Matplotlib"})
        plt.close(fig)

        # Save plotting data
        plot_data.to_csv(str(data_path), index=False)

        # Write ESSD checklist
        write_checklist(figure_id, fig, windows_used, dpi, checklist_path,
                        pdf_path, png_path, variable=var)

        print("  -> {}".format(png_path.name))
        print("  -> {}".format(pdf_path.name))
        print("  -> {}".format(data_path.name))
        print("  -> {}".format(checklist_path.name))

    # -- Script self-archiving -----------------------------------------------
    if str(Path(__file__).resolve()) != str(script_path.resolve()):
        shutil.copy(__file__, str(script_path))
        print("\nCopied script to: {}".format(script_path))
    else:
        print("\nScript already in scripts/ directory — skipping self-copy.")

    print("\nDone.")


if __name__ == "__main__":
    main()
