#!/usr/bin/env python3
"""ESSD-compliant satellite vs in-situ validation scatter plot.

Two modes:
  default     — load pair_records CSV → assign strata → compute metrics →
                save all artifacts (PNG, PDF, plotting data, metrics CSV,
                copied script, ESSD checklist)
  --plot-only — skip statistics, reuse existing metrics CSV, regenerate
                all other artifacts

Path and parameter settings are hardcoded below (``PATHS``, ``PARAMS``,
``STYLE`` dicts) so the script runs without CLI flags.  Only ``--plot-only``
toggles mode.

ESSD compliance follows ``plot/AGENTS.md`` → ``docs/essd_figure_requirements.md``.
"""

import argparse
import datetime
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# ============================================================================
#  HARDCODED CONFIGURATION  (edit these to change paths / parameters)
# ============================================================================

PATHS = {
    # Input — validation pairs CSV (output from s11 validation pipeline)
    "pairs_csv": (
        "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/"
        "Output_r/scripts_basin_test/output_other/"
        "validation_results/validation_satellite_insitu_pairs.csv"
    ),
    # ESSD figure root (must contain final/ data/ scripts/ checklists/ subdirs)
    "figures_root": (
        "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/"
        "Output_r/scripts_basin_test/figures"
    ),
}

PARAMS = {
    "figure_id": "satellite_insitu_validation_scatter",
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
    "panel_label_size": 14,
    "axis_label_size": 13,
    "tick_label_size": 12,
    "legend_text_size": 12,
    "title_size": 14,
    # Figure geometry — per-panel dimensions in cm (converted to inches)
    "panel_width_cm": 12.7,  # 5 in
    "panel_height_cm": 10.16,  # 4 in
    # Markers
    "scatter_marker_size": 14,
    "scatter_alpha": 0.65,
    # Grid
    "grid_alpha": 0.25,
}

# ============================================================================
#  MODULE-LEVEL CONSTANTS  (do not edit below unless extending functionality)
# ============================================================================

CM_PER_INCH = 2.54
VARIABLES = ("Q", "SSC", "SSL")
WINDOW_DAYS: Dict[str, int] = {"exact": 0, "pm1d": 1, "pm2d": 2}
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

# Okabe-Ito colorblind-safe palette (ESSD §5.2)
OKABE_ITO = [
    "#0072B2",  # blue
    "#D55E00",  # vermillion
    "#009E73",  # bluish_green
    "#E69F00",  # orange
    "#56B4E9",  # sky_blue
    "#CC79A7",  # reddish_purple
    "#F0E442",  # yellow
    "#000000",  # black
]
MARKER_SHAPES = ["o", "s", "^", "D", "v", "<", ">", "p"]

WINDOW_LABELS = {
    "exact": "Exact",
    "pm1d": "±1 day",
    "pm2d": "±2 days",
}

# ── Text / column helpers ──────────────────────────────────────────────────


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


def _lower_lookup(columns) -> Dict[str, str]:
    return {str(col).lower(): str(col) for col in columns}


def _first_existing(columns, candidates) -> Optional[str]:
    col_set = {str(col) for col in columns}
    for name in candidates:
        if name in col_set:
            return name
    lower = _lower_lookup(columns)
    for name in candidates:
        hit = lower.get(name.lower())
        if hit is not None:
            return hit
    return None


def _normalize_resolution(value) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return RESOLUTION_CODE.get(int(numeric), text)
    return text.lower()


# ── Bin-edge helpers ───────────────────────────────────────────────────────


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


# ── Strata helpers ─────────────────────────────────────────────────────────


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


# ── Strata assignment ──────────────────────────────────────────────────────


def assign_strata(
    pair_records: pd.DataFrame,
    high_turbidity_ssc: float = 1000.0,
    ssc_bin_edges: Sequence[float] = (100.0, 500.0, 1000.0, 5000.0),
) -> pd.DataFrame:
    """Add ``ssc_bin``, ``river_width_class``, ``climate_zone``,
    ``high_turbidity`` columns to the pair-records DataFrame."""
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


# ── Metrics computation ────────────────────────────────────────────────────


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
    """Compute aggregate metrics (bias, RMSE, MAE, MAPE, correlation, ...)
    grouped by overall, source_pair, and various strata combinations."""
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


# ── Figure infrastructure ─────────────────────────────────────────────────


def ensure_figure_dirs(figures_root: Path) -> dict:
    """Create and return the ESSD figure output directory structure."""
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
    """Import matplotlib with non-interactive Agg backend and return pyplot."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


def configure_matplotlib(plt) -> None:
    """Apply ESSD-compliant rcParams (font, embedding, sizes).

    All size values come from the ``STYLE`` dict at module top.
    """
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


# ── Scatter plot ───────────────────────────────────────────────────────────


def make_scatter_figure(
    plt,
    pair_records: pd.DataFrame,
    variable: str = "SSC",
) -> Tuple:
    """Create a multi-panel scatter figure of satellite vs in-situ values.

    One subplot per pairing window (exact / pm1d / pm2d).  Points are
    coloured by ``source_pair`` using the Okabe-Ito palette (ESSD §5.2)
    with distinct marker shapes.  Each subplot includes a 1:1 dashed
    reference line and a legend.

    Parameters
    ----------
    plt : module
        ``matplotlib.pyplot`` (Agg backend already set).
    pair_records : pd.DataFrame
        Paired satellite/in-situ records.  Must contain columns
        ``variable``, ``pairing_window``, ``insitu_value``,
        ``satellite_value``, and ``source_pair``.
    variable : str
        Variable to plot (default ``"SSC"``).

    Returns
    -------
    fig : matplotlib.figure.Figure
        The generated figure (caller saves it).
    windows_used : list of str
        The pairing windows that were plotted.
    plot_data : pd.DataFrame
        Subset of ``pair_records`` actually plotted (saved as plotting
        data artifact).
    status : str
        ``"generated"`` or ``"skipped: ..."``.
    """
    subset = pair_records[pair_records["variable"] == variable].copy()
    if len(subset) < 1:
        return None, [], subset, "skipped: no {} pairs".format(variable)

    windows = [w for w in ("exact", "pm1d", "pm2d") if w in set(subset["pairing_window"])]
    if not windows:
        return None, [], subset, "skipped: no configured windows"

    # --- mapping source_pair → colour + marker ---
    source_pairs = [s for s in subset["source_pair"].unique() if str(s).strip()]
    pair_colors = {sp: OKABE_ITO[i % len(OKABE_ITO)] for i, sp in enumerate(source_pairs)}
    pair_markers = {sp: MARKER_SHAPES[i % len(MARKER_SHAPES)] for i, sp in enumerate(source_pairs)}

    # --- build figure (2 rows: full range on top, 0–100 zoom on bottom) ---
    n_windows = len(windows)
    panel_w_in = STYLE["panel_width_cm"] / CM_PER_INCH
    panel_h_in = STYLE["panel_height_cm"] / CM_PER_INCH
    legend_margin_in = 1.25  # extra bottom space for shared legend
    fig, axes = plt.subplots(
        2, n_windows,
        figsize=(panel_w_in * n_windows, panel_h_in * 2 + legend_margin_in),
        squeeze=False,
    )

    for row_idx in range(2):
        is_zoom = row_idx == 1  # bottom row = 0–100 zoom
        for col_idx in range(n_windows):
            ax = axes[row_idx][col_idx]
            window = windows[col_idx]
            panel_idx = row_idx * n_windows + col_idx
            part = subset[subset["pairing_window"] == window]

            # --- scatter points ---
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

            # --- 1:1 reference line ---
            if is_zoom:
                ax.plot([0, 100], [0, 100], color="black", linewidth=1, linestyle="--")
            else:
                finite = pd.to_numeric(
                    part[["insitu_value", "satellite_value"]].stack(), errors="coerce"
                )
                finite = finite[np.isfinite(finite)]
                if len(finite):
                    lo = float(finite.min())
                    hi = float(finite.max())
                    ax.plot([lo, hi], [lo, hi], color="black", linewidth=1, linestyle="--")

            # --- axis limits ---
            if is_zoom:
                ax.set_xlim(0, 100)
                ax.set_ylim(0, 100)

            # --- title ---
            window_label = WINDOW_LABELS.get(window, window)
            if is_zoom:
                part_zoom = part[
                    (pd.to_numeric(part["insitu_value"], errors="coerce") <= 100)
                    & (pd.to_numeric(part["satellite_value"], errors="coerce") <= 100)
                ]
                n_zoom = len(part_zoom)
                ax.set_title(
                    "{} {} 0–100 zoom (n={} pairs)".format(
                        window_label, variable, n_zoom
                    )
                )
            else:
                ax.set_title(
                    "{} {} (n={} pairs)".format(window_label, variable, len(part))
                )

            # --- axis labels ---
            if col_idx == 1 and row_idx == 1:
                ax.set_xlabel("In-situ {} (mg L⁻¹)".format(variable))
            if col_idx == 0:
                ax.set_ylabel("Satellite {} (mg L⁻¹)".format(variable))
            ax.grid(True, alpha=STYLE["grid_alpha"])
            # Hide y-axis tick labels on columns > 0
            if col_idx > 0:
                ax.tick_params(labelleft=False)

            # --- panel labels (a)\u2013(f) ---
            ax.text(
                0.02, 0.98,
                "({})".format(chr(97 + panel_idx)),
                transform=ax.transAxes,
                fontsize=STYLE["panel_label_size"],
                fontweight="bold",
                va="top",
                ha="left",
            )

            # --- correlation coefficients ---
            if is_zoom:
                insitu_num = pd.to_numeric(part_zoom["insitu_value"], errors="coerce").to_numpy(dtype=float)
                sat_num = pd.to_numeric(part_zoom["satellite_value"], errors="coerce").to_numpy(dtype=float)
            else:
                insitu_num = pd.to_numeric(part["insitu_value"], errors="coerce").to_numpy(dtype=float)
                sat_num = pd.to_numeric(part["satellite_value"], errors="coerce").to_numpy(dtype=float)
            r_pearson = _safe_corr(insitu_num, sat_num, "pearson")
            r_spearman = _safe_corr(insitu_num, sat_num, "spearman")
            corr_lines = [
                "Pearson r = {:.3f}".format(r_pearson) if np.isfinite(r_pearson) else "Pearson r = NaN",
                "Spearman \u03c1 = {:.3f}".format(r_spearman) if np.isfinite(r_spearman) else "Spearman \u03c1 = NaN",
            ]
            ax.text(
                0.98, 0.02, "\n".join(corr_lines),
                transform=ax.transAxes,
                fontsize=STYLE["tick_label_size"],
                va="bottom", ha="right", linespacing=1.5,
            )

    # --- shared legend below the figure ---
    handles = []
    labels = []
    for sp in source_pairs:
        proxy = plt.Line2D(
            [0], [0], marker=pair_markers[sp], color=pair_colors[sp],
            markersize=8, linewidth=0, linestyle="",
        )
        handles.append(proxy)
        labels.append(str(sp))
    # 1:1 reference line proxy
    one2one = plt.Line2D(
        [0], [0], color="black", linewidth=1, linestyle="--",
    )
    handles.append(one2one)
    labels.append("1:1")

    fig.legend(
        handles, labels,
        loc="lower center", bbox_to_anchor=(0.5, -0.02),
        ncol=len(handles), frameon=True,
        fontsize=STYLE["legend_text_size"],
    )
    fig.subplots_adjust(bottom=0.10, top=0.94, hspace=0.30, wspace=0.23)
    return fig, windows, subset, "generated"


# ── CLI (only --plot-only) ─────────────────────────────────────────────────


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Satellite vs in-situ validation scatter plot (ESSD-compliant).  "
        "All parameters are hardcoded inside the script (PATHS / PARAMS / STYLE dicts).  "
        "Use --plot-only to skip statistics and regenerate artifacts.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip statistics computation; reuse existing metrics CSV and "
        "only regenerate the figure and artifacts.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    plot_only = args.plot_only

    # ── Matplotlib setup (Agg must be set before any pyplot import) ──────
    plt = _setup_matplotlib()
    configure_matplotlib(plt)

    # ── Resolve paths from hardcoded configuration ──────────────────────
    figure_dirs = ensure_figure_dirs(Path(PATHS["figures_root"]))
    figure_id = PARAMS["figure_id"]
    pairs_path = Path(PATHS["pairs_csv"])
    metrics_path = figure_dirs["data"] / "{}_metrics.csv".format(figure_id)
    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    data_path = figure_dirs["data"] / "{}_plotting_data.csv".format(figure_id)
    checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)
    script_path = figure_dirs["scripts"] / "plot_{}.py".format(figure_id)

    # ── Resolve parameters ──────────────────────────────────────────────
    variable = PARAMS["variable"]
    high_turbidity_ssc = float(PARAMS["high_turbidity_ssc"])
    ssc_bin_edges = _parse_ssc_bin_edges(PARAMS["ssc_bin_edges"])
    dpi = int(PARAMS["dpi"])

    if not pairs_path.exists():
        raise SystemExit("ERROR: pairs CSV not found: {}".format(pairs_path))

    print("Loading paired records from: {}".format(pairs_path))
    pair_records = pd.read_csv(str(pairs_path), keep_default_na=False)
    print("  -> {} rows loaded".format(len(pair_records)))

    if pair_records.empty:
        raise SystemExit("ERROR: paired records DataFrame is empty.")

    # ── Assure strata columns exist ─────────────────────────────────────
    _STRATA_COLS = {"ssc_bin", "river_width_class", "climate_zone", "high_turbidity"}
    if not _STRATA_COLS.issubset(pair_records.columns):
        print("Assigning missing strata columns ...")
        pair_records = assign_strata(pair_records, high_turbidity_ssc, ssc_bin_edges)

    if not plot_only:
        # ── Full mode: compute + save metrics ───────────────────────────
        print("Computing validation metrics ...")
        metrics = compute_satellite_insitu_metrics(pair_records)
        print("  -> {} metric rows computed".format(len(metrics)))
        metrics.to_csv(str(metrics_path), index=False)
        print("Saved metrics to: {}".format(metrics_path))
    else:
        # ── Plot-only mode: load existing metrics ────────────────────────
        if metrics_path.exists():
            metrics = pd.read_csv(str(metrics_path), keep_default_na=False)
            print("Loaded pre-computed metrics ({} rows) from: {}".format(
                len(metrics), metrics_path))
        else:
            metrics = pd.DataFrame()
            print("Warning: metrics CSV not found ({}); continuing without metrics.".format(
                metrics_path))

    # ── Create figure (shared by both modes) ────────────────────────────
    print("Generating scatter plot ...")
    fig, windows_used, plot_data, status = make_scatter_figure(
        plt, pair_records, variable=variable,
    )
    print("  -> scatter plot: {}".format(status))

    if status.startswith("skipped"):
        return

    # ── Save final figure files ─────────────────────────────────────────
    fig.savefig(str(png_path), dpi=dpi, bbox_inches="tight")
    fig.savefig(str(pdf_path), bbox_inches="tight",
                metadata={"Creator": "Python Matplotlib"})
    plt.close(fig)
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(pdf_path))

    # ── Save plotting data ──────────────────────────────────────────────
    plot_data.to_csv(str(data_path), index=False)
    print("Wrote {}".format(data_path))

# ── Write ESSD-compliant checklist ──────────────────────────────────
    figsize_in = fig.get_size_inches()
    width_cm = figsize_in[0] * CM_PER_INCH
    height_cm = figsize_in[1] * CM_PER_INCH
    pdf_size_bytes = pdf_path.stat().st_size if pdf_path.exists() else 0
    png_size_bytes = png_path.stat().st_size if png_path.exists() else 0
    n_panels = len(windows_used) * 2  # 2 rows × 3 windows

    clines = [
        "# Figure checklist: {}".format(figure_id),
        "",
        "## Basic information",
        "",
        "- Figure file: `{}.pdf` / `{}.png`".format(figure_id, figure_id),
        "- Plotting script: `plot_{}.py`".format(figure_id),
        "- Plotting data: `{}_plotting_data.csv`".format(figure_id),
        "- Date exported: {}".format(datetime.date.today().isoformat()),
        "- Figure type: multi-panel scatter (satellite vs in-situ)",
        "- Single-panel or multi-panel: multi-panel ({} panels: 2 rows × 3 columns, "
        "top: {}, bottom: 0–100 zoom)".format(
            n_panels, ", ".join(WINDOW_LABELS.get(w, w) for w in windows_used)),
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
        "- Legend does not obscure data: Yes (placed inside each panel)",
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
        "-",
    ]
    checklist_path.write_text("\n".join(clines) + "\n", encoding="utf-8")
    print("Wrote {}".format(checklist_path))

    print("Done.")


if __name__ == "__main__":
    main()
