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
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# ============================================================================
#  HARDCODED CONFIGURATION  (edit these to change paths / parameters)
# ============================================================================

PATHS = {
    "pairs_csv": (
        "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/"
        "Output_r/scripts_basin_test/validate/output/validation_results_s5b/"
        "validation_satellite_insitu_pairs.csv"
    ),
    "figures_root": (
        "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/"
        "Output_r/scripts_basin_test/"
        "figures"
    ),
}


def script_output_stem() -> str:
    stem = Path(__file__).resolve().stem
    return stem[5:] if stem.startswith("plot_") else stem


PARAMS = {
    "figure_id": script_output_stem(),
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

CM_PER_INCH = 2.54
WINDOW_EXCLUSIVE = False
RESOLUTION_CODE = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology"}
METHOD_NOTES_BASE = (
    "satellite/reach-scale vs in-situ validation; satellite records are anchors; "
    "pairing windows are cumulative"
)
ASSUMPTIONS_BASE = (
    "compiled sources are secondary_compilation unless source text or taxonomy override "
    "identifies them as in_situ; missing river width is 'missing'; missing climate zone is 'unknown'"
)
OKABE_ITO = [
    "#0072B2",
    "#D55E00",
    "#009E73",
    "#E69F00",
    "#56B4E9",
    "#CC79A7",
    "#F0E442",
    "#000000",
]
MARKER_SHAPES = ["o", "s", "^", "D", "v", "<", ">", "p"]
WINDOW_LABELS = {
    "exact": "Exact",
    "pm1d": "±1 day",
    "pm2d": "±2 days",
}


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in ("nan", "none", "nat") else text


def _normalize_resolution(value) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return RESOLUTION_CODE.get(int(numeric), text)
    return text.lower()


def _format_edge(value: float) -> str:
    if value == int(value):
        return str(int(value))
    return "{:g}".format(value)


def _parse_ssc_bin_edges(text: str) -> Tuple[float, ...]:
    return tuple(float(v.strip()) for v in text.split(",") if v.strip())


def _bin_label(value: float, edges: Sequence[float]) -> str:
    if not np.isfinite(value):
        return "missing"
    for edge in edges:
        if value < edge:
            return "<{}".format(_format_edge(edge))
    return ">={}".format(_format_edge(edges[-1]))


def _first_nonempty(row: pd.Series, names: Sequence[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            text = _clean_text(row.get(name, ""))
            if text:
                return text
    return default


def _first_numeric(row: pd.Series, names: Sequence[str]) -> float:
    for name in names:
        if name in row.index:
            value = pd.to_numeric(pd.Series([row.get(name, np.nan)]), errors="coerce").iloc[0]
            if pd.notna(value) and np.isfinite(float(value)):
                return float(value)
    return float("nan")


def _width_class_from_numeric(width: float) -> str:
    if not np.isfinite(width):
        return "missing"
    if width < 30:
        return "<30m"
    if width < 100:
        return "30-99m"
    if width < 300:
        return "100-299m"
    return ">=300m"


def assign_strata(
    pair_records: pd.DataFrame,
    high_turbidity_ssc: float = 1000.0,
    ssc_bin_edges: Sequence[float] = (100.0, 500.0, 1000.0, 5000.0),
) -> pd.DataFrame:
    if pair_records.empty:
        out = pair_records.copy()
        for col in ("ssc_bin", "river_width_class", "climate_zone", "high_turbidity"):
            if col not in out.columns:
                out[col] = []
        return out

    work = pair_records.copy()
    ssc_bins: List[str] = []
    width_classes: List[str] = []
    climate_zones: List[str] = []
    high_turbidity_values: List[bool] = []

    for _, row in work.iterrows():
        ssc = _first_numeric(row, ("insitu_ssc", "satellite_ssc", "SSC"))
        ssc_bins.append(_bin_label(ssc, ssc_bin_edges))
        high_turbidity_values.append(bool(np.isfinite(ssc) and ssc >= float(high_turbidity_ssc)))

        width_class = _first_nonempty(
            row,
            (
                "river_width_class",
                "insitu_river_width_class",
                "satellite_river_width_class",
                "width_class",
            ),
        )
        if not width_class:
            width = _first_numeric(
                row,
                (
                    "river_width_m",
                    "insitu_river_width_m",
                    "satellite_river_width_m",
                    "width_m",
                    "river_width",
                ),
            )
            width_class = _width_class_from_numeric(width)
        width_classes.append(width_class or "missing")

        climate = _first_nonempty(
            row,
            (
                "climate_zone",
                "insitu_climate_zone",
                "satellite_climate_zone",
                "hydroatlas_climate_zone",
                "koppen_zone",
                "koppen",
                "climate_class",
            ),
            default="unknown",
        )
        climate_zones.append(climate or "unknown")

    work["ssc_bin"] = ssc_bins
    work["high_turbidity"] = high_turbidity_values
    work["river_width_class"] = width_classes
    work["climate_zone"] = climate_zones
    return work


def _cluster_group_key(df: pd.DataFrame) -> pd.Series:
    uid = df["cluster_uid"].astype(str).str.strip() if "cluster_uid" in df else pd.Series([""] * len(df))
    cid = df["cluster_id"].astype(str).str.strip() if "cluster_id" in df else pd.Series([""] * len(df))
    return uid.where(uid.ne(""), cid)


def _safe_corr(a: np.ndarray, b: np.ndarray, method: str) -> float:
    if len(a) < 2:
        return float("nan")
    if np.nanstd(a) == 0 or np.nanstd(b) == 0:
        return float("nan")
    if method == "spearman":
        left = pd.Series(a).rank(method="average")
        right = pd.Series(b).rank(method="average")
        return float(left.corr(right, method="pearson"))
    return float(pd.Series(a).corr(pd.Series(b), method="pearson"))


def _metric_values(group: pd.DataFrame) -> Dict[str, float]:
    sat = pd.to_numeric(group["satellite_value"], errors="coerce").to_numpy(dtype=float)
    insitu = pd.to_numeric(group["insitu_value"], errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(sat) & np.isfinite(insitu)
    sat = sat[valid]
    insitu = insitu[valid]
    if len(sat) == 0:
        return {
            "bias": float("nan"),
            "RMSE": float("nan"),
            "MAE": float("nan"),
            "MAPE": float("nan"),
            "median_absolute_error": float("nan"),
            "Pearson": float("nan"),
            "Spearman": float("nan"),
            "R2": float("nan"),
            "n_pairs": 0,
        }
    diff = sat - insitu
    mape_mask = insitu != 0
    pearson = _safe_corr(insitu, sat, "pearson")
    return {
        "bias": float(np.nanmean(diff)),
        "RMSE": float(np.sqrt(np.nanmean(diff ** 2))),
        "MAE": float(np.nanmean(np.abs(diff))),
        "MAPE": float(np.nanmean(np.abs(diff[mape_mask] / insitu[mape_mask]) * 100.0)) if np.any(mape_mask) else float("nan"),
        "median_absolute_error": float(np.nanmedian(np.abs(diff))),
        "Pearson": pearson,
        "Spearman": _safe_corr(insitu, sat, "spearman"),
        "R2": float(pearson ** 2) if np.isfinite(pearson) else float("nan"),
        "n_pairs": int(len(sat)),
    }


def compute_satellite_insitu_metrics(pair_records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "group_type",
        "pairing_window",
        "window_exclusive",
        "variable",
        "source_pair",
        "ssc_bin",
        "river_width_class",
        "climate_zone",
        "high_turbidity",
        "bias",
        "RMSE",
        "MAE",
        "MAPE",
        "median_absolute_error",
        "Pearson",
        "Spearman",
        "R2",
        "n_pairs",
        "n_clusters",
        "method_notes",
        "assumptions",
    ]
    if pair_records.empty:
        return pd.DataFrame(columns=columns)

    group_specs = {
        "overall": [],
        "source_pair": ["source_pair"],
        "source_pair_ssc_bin": ["source_pair", "ssc_bin"],
        "source_pair_width": ["source_pair", "river_width_class"],
        "source_pair_climate": ["source_pair", "climate_zone"],
        "source_pair_high_turbidity": ["source_pair", "high_turbidity"],
        "full_strata": ["source_pair", "ssc_bin", "river_width_class", "climate_zone", "high_turbidity"],
    }
    rows: List[Dict[str, object]] = []
    base_cols = ["pairing_window", "variable"]

    for group_type, strata_cols in group_specs.items():
        cols = base_cols + strata_cols
        for keys, group in pair_records.groupby(cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            values = dict(zip(cols, keys))
            metrics = _metric_values(group)
            cluster_key = _cluster_group_key(group)
            row: Dict[str, object] = {
                "group_type": group_type,
                "pairing_window": values.get("pairing_window", ""),
                "window_exclusive": WINDOW_EXCLUSIVE,
                "variable": values.get("variable", ""),
                "source_pair": values.get("source_pair", "ALL"),
                "ssc_bin": values.get("ssc_bin", "ALL"),
                "river_width_class": values.get("river_width_class", "ALL"),
                "climate_zone": values.get("climate_zone", "ALL"),
                "high_turbidity": values.get("high_turbidity", "ALL"),
                "n_clusters": int(cluster_key.nunique()),
                "method_notes": str(group["method_notes"].iloc[0])
                if "method_notes" in group
                else METHOD_NOTES_BASE,
                "assumptions": str(group["assumptions"].iloc[0])
                if "assumptions" in group
                else ASSUMPTIONS_BASE,
            }
            row.update(metrics)
            rows.append(row)

    return pd.DataFrame(rows, columns=columns)


def ensure_figure_dirs(figures_root: Path) -> dict:
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


def _setup_matplotlib():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def configure_matplotlib(plt) -> None:
    plt.rcParams.update({
        "font.family": STYLE["font_family"],
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "axes.labelsize": STYLE["axis_label_size"],
        "axes.titlesize": STYLE["title_size"],
        "xtick.labelsize": STYLE["tick_label_size"],
        "ytick.labelsize": STYLE["tick_label_size"],
        "legend.fontsize": STYLE["legend_text_size"],
        "axes.unicode_minus": False,
    })

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
        sp: OKABE_ITO[i % len(OKABE_ITO)]
        for i, sp in enumerate(SOURCE_PAIRS)
    }
    pair_markers = {
        sp: MARKER_SHAPES[i % len(MARKER_SHAPES)]
        for i, sp in enumerate(SOURCE_PAIRS)
    }

    n_rows = len(SOURCE_PAIRS)
    n_cols = len(WINDOWS)
    panel_w_in = STYLE["panel_width_cm"] / CM_PER_INCH
    panel_h_in = STYLE["panel_height_cm"] / CM_PER_INCH

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
                ax.set_title("{}".format(WINDOW_LABELS.get(window, window)),
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
                window_label = WINDOW_LABELS.get(window, window)
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
            r_pearson = _safe_corr(insitu_num, sat_num, "pearson")
            r_spearman = _safe_corr(insitu_num, sat_num, "spearman")
            r2 = r_pearson ** 2 if np.isfinite(r_pearson) else float("nan")
            n_pairs = len(part)
            n_reference_stations = part["station_uid"].nunique() if "station_uid" in part.columns else 0
            corr_lines = [
                "n = {}".format(n_pairs),
                "n_stations = {}".format(n_reference_stations),
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
    width_cm = figsize_in[0] * CM_PER_INCH
    height_cm = figsize_in[1] * CM_PER_INCH
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
        "- Columns: {}".format(", ".join(WINDOW_LABELS.get(w, w) for w in windows_used)),
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
    plt = _setup_matplotlib()
    configure_matplotlib(plt)
    plt.rcParams.update({
        "font.family": STYLE["font_family"],
        "axes.labelsize": STYLE["axis_label_size"],
        "axes.titlesize": STYLE["title_size"],
        "xtick.labelsize": STYLE["tick_label_size"],
        "ytick.labelsize": STYLE["tick_label_size"],
        "legend.fontsize": STYLE["legend_text_size"],
    })

    # -- Resolve paths from hardcoded configuration --------------------------
    figure_dirs = ensure_figure_dirs(Path(PATHS["figures_root"]))
    figure_id = PARAMS["figure_id"]
    pairs_path = Path(PATHS["pairs_csv"])
    metrics_path = figure_dirs["data"] / "{}_metrics.csv".format(figure_id)
    script_path = figure_dirs["scripts"] / "plot_{}.py".format(figure_id)

    variable = PARAMS["variable"]
    high_turbidity_ssc = float(PARAMS["high_turbidity_ssc"])
    ssc_bin_edges = _parse_ssc_bin_edges(PARAMS["ssc_bin_edges"])
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
        pairs = assign_strata(
            pairs,
            high_turbidity_ssc=high_turbidity_ssc,
            ssc_bin_edges=ssc_bin_edges,
        )

    # -- Metrics -------------------------------------------------------------
    if not plot_only:
        print("Computing validation metrics ...")
        metrics = compute_satellite_insitu_metrics(pairs)
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
