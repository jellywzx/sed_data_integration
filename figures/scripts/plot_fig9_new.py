#!/usr/bin/env python3
"""Fig9 temporal-alignment validation grid.

Reads the s11 satellite/in-situ pairs CSV and plots a 2x3 scatter grid that
keeps daily nearest-day windows separate from period-aligned cross-resolution
pairs.
"""

import argparse
import datetime
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parents[2]

PATHS = {
    "pairs_csv": PROJECT_DIR / "validate" / "output" / "s11_satellite_insitu" / "validation_satellite_insitu_pairs.csv",
    "figures_root": PROJECT_DIR / "figures",
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

STYLE = {
    "font_family": "Times New Roman",
    "panel_label_size": 16,
    "axis_label_size": 15,
    "tick_label_size": 14,
    "legend_text_size": 13,
    "title_size": 14,
    "panel_width_cm": 8.5,
    "panel_height_cm": 7.0,
    "scatter_marker_size": 10,
    "scatter_alpha": 0.65,
    "grid_alpha": 0.25,
}

CM_PER_INCH = 2.54
WINDOW_EXCLUSIVE = False
METHOD_NOTES_BASE = (
    "satellite/reach-scale vs in-situ validation; same-resolution windows are "
    "nearest-day, period panels use temporal alignment intervals"
)
ASSUMPTIONS_BASE = (
    "period panels use s11 temporal alignment metadata; audit-only cross-resolution "
    "candidates are not included in the main figure"
)
SCIENTIFIC_EXPONENT_POSITION = {
    # Where to draw the scientific-notation exponents ("x10^n") on panel (a).
    # Positions are axes-fraction coordinates relative to that panel's axes:
    # (0, 0) = lower-left corner, (1, 1) = upper-right; values outside 0..1
    # place the text beyond the axes boundary. Each entry also carries the
    # horizontal/vertical alignment used to anchor the text at that spot.
    "xaxis": (1.0, -0.15, "right", "top"),
    "yaxis": (-0.35, 0.9, "left", "bottom"),
}

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
    "pm1d": "+/-1 day",
    "pm2d": "+/-2 days",
    "period": "Period",
}
TEMPORAL_LABELS = {
    "same_resolution_nearest_day": "Daily nearest-day",
    "monthly_to_daily_aggregate": "M->D aggregate",
    "monthly_to_monthly_period": "M->M period",
    "monthly_to_annual_aggregate": "M->Y aggregate",
}

PANEL_SPECS: List[List[Optional[Dict[str, str]]]] = [
    [
        {
            "source_pair": "RiverSed vs USGS",
            "pairing_window": "exact",
            "temporal_alignment_mode": "same_resolution_nearest_day",
            "title": "RiverSed vs USGS",
        },
        {
            "source_pair": "RiverSed vs HYDAT",
            "pairing_window": "exact",
            "temporal_alignment_mode": "same_resolution_nearest_day",
            "title": "RiverSed vs HYDAT",
        },
        {
            "source_pair": "GSED vs HYDAT",
            "pairing_window": "period",
            "temporal_alignment_mode": "monthly_to_daily_aggregate",
            "title": "GSED vs HYDAT",
        },
    ],
    [
        {
            "source_pair": "GSED vs USGS",
            "pairing_window": "period",
            "temporal_alignment_mode": "monthly_to_daily_aggregate",
            "title": "GSED vs USGS",
        },
        {
            "source_pair": "GSED vs Eurasian_River",
            "pairing_window": "period",
            "temporal_alignment_mode": "monthly_to_monthly_period",
            "title": "GSED vs Eurasian",
        },
        {
            "source_pair": "Dethier vs Mekong_Delta",
            "pairing_window": "period",
            "temporal_alignment_mode": "monthly_to_daily_aggregate",
            "title": "Dethier vs Mekong",
        },
    ],
]


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


def _format_edge(value: float) -> str:
    return str(int(value)) if value == int(value) else "{:g}".format(value)


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


def _boolish(value) -> bool:
    text = _clean_text(value).lower()
    if text in ("true", "t", "1", "yes", "y"):
        return True
    if text in ("false", "f", "0", "no", "n", ""):
        return False
    return bool(value)


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
            ("river_width_class", "insitu_river_width_class", "satellite_river_width_class", "width_class"),
        )
        if not width_class:
            width = _first_numeric(
                row,
                ("river_width_m", "insitu_river_width_m", "satellite_river_width_m", "width_m", "river_width"),
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
        "panel_id",
        "pairing_window",
        "window_exclusive",
        "variable",
        "source_pair",
        "ssc_bin",
        "river_width_class",
        "climate_zone",
        "high_turbidity",
        "temporal_alignment_mode",
        "satellite_time_support_class",
        "insitu_time_support_class",
        "is_cross_resolution",
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

    work = pair_records.copy()
    for name, default in (
        ("panel_id", "ALL"),
        ("temporal_alignment_mode", "same_resolution_nearest_day"),
        ("satellite_time_support_class", "unknown"),
        ("insitu_time_support_class", "unknown"),
        ("is_cross_resolution", False),
    ):
        if name not in work.columns:
            work[name] = default
    work["is_cross_resolution"] = work["is_cross_resolution"].map(_boolish)

    group_specs = {
        "overall": [],
        "panel": ["panel_id"],
        "source_pair": ["source_pair"],
        "temporal_alignment": [
            "temporal_alignment_mode",
            "satellite_time_support_class",
            "insitu_time_support_class",
            "is_cross_resolution",
        ],
        "source_pair_temporal": [
            "source_pair",
            "temporal_alignment_mode",
            "satellite_time_support_class",
            "insitu_time_support_class",
            "is_cross_resolution",
        ],
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
        for keys, group in work.groupby(cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            values = dict(zip(cols, keys))
            row: Dict[str, object] = {
                "group_type": group_type,
                "panel_id": values.get("panel_id", "ALL"),
                "pairing_window": values.get("pairing_window", ""),
                "window_exclusive": WINDOW_EXCLUSIVE,
                "variable": values.get("variable", ""),
                "source_pair": values.get("source_pair", "ALL"),
                "ssc_bin": values.get("ssc_bin", "ALL"),
                "river_width_class": values.get("river_width_class", "ALL"),
                "climate_zone": values.get("climate_zone", "ALL"),
                "high_turbidity": values.get("high_turbidity", "ALL"),
                "temporal_alignment_mode": values.get("temporal_alignment_mode", "ALL"),
                "satellite_time_support_class": values.get("satellite_time_support_class", "ALL"),
                "insitu_time_support_class": values.get("insitu_time_support_class", "ALL"),
                "is_cross_resolution": values.get("is_cross_resolution", "ALL"),
                "n_clusters": int(_cluster_group_key(group).nunique()),
                "method_notes": str(group["method_notes"].iloc[0]) if "method_notes" in group else METHOD_NOTES_BASE,
                "assumptions": str(group["assumptions"].iloc[0]) if "assumptions" in group else ASSUMPTIONS_BASE,
            }
            row.update(_metric_values(group))
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
    plt.rcParams.update(
        {
            "font.family": STYLE["font_family"],
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "axes.labelsize": STYLE["axis_label_size"],
            "axes.titlesize": STYLE["title_size"],
            "xtick.labelsize": STYLE["tick_label_size"],
            "ytick.labelsize": STYLE["tick_label_size"],
            "legend.fontsize": STYLE["legend_text_size"],
            "axes.unicode_minus": False,
        }
    )


def _panel_id(row_idx: int, col_idx: int) -> str:
    return "r{}c{}".format(row_idx + 1, col_idx + 1)


def _filter_panel(subset: pd.DataFrame, spec: Dict[str, str]) -> pd.DataFrame:
    part = subset[
        (subset["source_pair"].astype(str) == spec["source_pair"])
        & (subset["pairing_window"].astype(str) == spec["pairing_window"])
    ].copy()
    mode = _clean_text(spec.get("temporal_alignment_mode", ""))
    if mode and "temporal_alignment_mode" in part.columns:
        part = part[part["temporal_alignment_mode"].astype(str) == mode].copy()
    return part


def _apply_scientific_ticks(ax) -> None:
    from matplotlib.ticker import ScalarFormatter

    for axis in (ax.xaxis, ax.yaxis):
        formatter = ScalarFormatter(useMathText=False, useOffset=False)
        formatter.set_scientific(True)
        formatter.set_powerlimits((-6, 2))
        axis.set_major_formatter(formatter)

    # matplotlib re-places the auto offset text ("x10^n") on every draw, so it
    # cannot be positioned freely. Hide it and mirror its content onto custom
    # texts anchored to SCIENTIFIC_EXPONENT_POSITION instead. The draw_event
    # callback fires after the axes are drawn, i.e. once the formatter has
    # filled in the exponent text.
    auto_texts = {
        "xaxis": ax.xaxis.get_offset_text(),
        "yaxis": ax.yaxis.get_offset_text(),
    }
    for text in auto_texts.values():
        text.set_visible(False)

    custom_texts = {
        name: ax.text(
            0.0,
            0.0,
            "",
            transform=ax.transAxes,
            clip_on=False,
            fontsize=STYLE["tick_label_size"],
            ha=SCIENTIFIC_EXPONENT_POSITION[name][2],
            va=SCIENTIFIC_EXPONENT_POSITION[name][3],
        )
        for name in ("xaxis", "yaxis")
    }

    def _sync_exponent_text(event=None) -> None:
        for name, custom in custom_texts.items():
            auto = auto_texts[name]
            # tick_params(labelleft=True/labelbottom=True) re-enables the auto
            # offset text after we hid it, so keep forcing it hidden here.
            auto.set_visible(False)
            content = auto.get_text()
            custom.set_text(content)
            custom.set_visible(bool(content))
            custom.set_position(SCIENTIFIC_EXPONENT_POSITION[name][:2])

    ax.figure.canvas.mpl_connect("draw_event", _sync_exponent_text)


def _apply_plain_ticks(ax) -> None:
    from matplotlib.ticker import ScalarFormatter

    for axis in (ax.xaxis, ax.yaxis):
        formatter = ScalarFormatter(useOffset=False)
        formatter.set_scientific(False)
        axis.set_major_formatter(formatter)


def _annotate_no_data(ax, spec: Optional[Dict[str, str]]) -> None:
    if spec is None:
        text = "reserved"
    else:
        text = "no data\n{}\n{}".format(
            spec.get("source_pair", ""),
            TEMPORAL_LABELS.get(spec.get("temporal_alignment_mode", ""), spec.get("temporal_alignment_mode", "")),
        ).strip()
    ax.text(
        0.5,
        0.5,
        text,
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=STYLE["tick_label_size"],
        color="grey",
    )


def make_temporal_grid(plt, pair_records: pd.DataFrame, variable: str = "SSC"):
    subset = pair_records[pair_records["variable"].astype(str) == variable].copy()
    if len(subset) < 1:
        return None, pd.DataFrame(), "skipped: no {} pairs".format(variable)

    n_rows = len(PANEL_SPECS)
    n_cols = max(len(row) for row in PANEL_SPECS)
    panel_w_in = STYLE["panel_width_cm"] / CM_PER_INCH
    panel_h_in = STYLE["panel_height_cm"] / CM_PER_INCH
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(panel_w_in * n_cols, panel_h_in * n_rows),
        squeeze=False,
    )

    all_pairs = sorted({spec["source_pair"] for row in PANEL_SPECS for spec in row if spec})
    pair_colors = {sp: OKABE_ITO[i % len(OKABE_ITO)] for i, sp in enumerate(all_pairs)}
    pair_markers = {sp: MARKER_SHAPES[i % len(MARKER_SHAPES)] for i, sp in enumerate(all_pairs)}
    plotting_parts: List[pd.DataFrame] = []

    for row_idx in range(n_rows):
        row_specs = PANEL_SPECS[row_idx]
        for col_idx in range(n_cols):
            spec = row_specs[col_idx] if col_idx < len(row_specs) else None
            ax = axes[row_idx][col_idx]
            panel_idx = row_idx * n_cols + col_idx
            ax.text(
                -0.15,
                1.1,
                "({})".format(chr(97 + panel_idx)),
                transform=ax.transAxes,
                fontsize=STYLE["panel_label_size"],
                fontweight="bold",
                va="top",
                ha="left",
                clip_on=False,
            )
            if spec is None:
                _annotate_no_data(ax, spec)
                ax.set_xticks([])
                ax.set_yticks([])
                continue

            ax.set_title(spec.get("title", ""), fontsize=STYLE["title_size"])
            part = _filter_panel(subset, spec)
            if part.empty:
                _annotate_no_data(ax, spec)
                ax.grid(True, alpha=STYLE["grid_alpha"])
                continue

            part = part.copy()
            part["panel_id"] = _panel_id(row_idx, col_idx)
            part["panel_title"] = spec.get("title", "")
            plotting_parts.append(part)

            sp = spec["source_pair"]
            ax.scatter(
                pd.to_numeric(part["insitu_value"], errors="coerce"),
                pd.to_numeric(part["satellite_value"], errors="coerce"),
                s=STYLE["scatter_marker_size"],
                c=pair_colors[sp],
                marker=pair_markers[sp],
                alpha=STYLE["scatter_alpha"],
                rasterized=True,
            )
            finite = pd.to_numeric(part[["insitu_value", "satellite_value"]].stack(), errors="coerce")
            finite = finite[np.isfinite(finite)]
            if len(finite):
                lo = float(finite.min())
                hi = float(finite.max())
                ax.plot([lo, hi], [lo, hi], color="black", linewidth=1, linestyle="--")

            ax.grid(True, alpha=STYLE["grid_alpha"])
            if panel_idx == 0:
                _apply_scientific_ticks(ax)
            else:
                _apply_plain_ticks(ax)
            ax.tick_params(labelleft=True, labelbottom=True)

            insitu_num = pd.to_numeric(part["insitu_value"], errors="coerce").to_numpy(dtype=float)
            sat_num = pd.to_numeric(part["satellite_value"], errors="coerce").to_numpy(dtype=float)
            r_pearson = _safe_corr(insitu_num, sat_num, "pearson")
            r_spearman = _safe_corr(insitu_num, sat_num, "spearman")
            n_clusters = part["cluster_uid"].nunique() if "cluster_uid" in part.columns else 0
            diff = sat_num - insitu_num
            bias = float(np.nanmean(diff)) if np.isfinite(diff).any() else float("nan")
            corr_lines = [
                "n = {}".format(len(part)),
                "stations = {}".format(n_clusters),
                "r = {:.3f}".format(r_pearson) if np.isfinite(r_pearson) else "r = NaN",
                "ρ = {:.3f}".format(r_spearman) if np.isfinite(r_spearman) else "ρ = NaN",
                "bias = {:.2f}".format(bias) if np.isfinite(bias) else "bias = NaN",
            ]
            ax.text(
                0.98,
                0.02,
                "\n".join(corr_lines),
                transform=ax.transAxes,
                fontsize=STYLE["tick_label_size"] - 3,
                va="bottom",
                ha="right",
                linespacing=1.25,
            )

    fig.supxlabel(
        "Station-reference {} (mg L$^{{-1}}$)".format(variable),
        fontsize=STYLE["axis_label_size"],
        y=0.04,
    )
    fig.supylabel(
        "Satellite-derived {} (mg L$^{{-1}}$)".format(variable),
        fontsize=STYLE["axis_label_size"],
        x=0.025,
    )
    fig.subplots_adjust(hspace=0.48, wspace=0.24, top=0.94, bottom=0.15, left=0.10, right=0.93)
    # Pre-draw so the panel (a) exponent texts get populated before saving
    # (the draw_event callback runs after rendering completes).
    fig.canvas.draw()
    plotting_data = pd.concat(plotting_parts, ignore_index=True, sort=False) if plotting_parts else pd.DataFrame()
    return fig, plotting_data, "generated"


def write_checklist(
    figure_id: str,
    fig,
    plotting_data: pd.DataFrame,
    dpi: int,
    checklist_path: Path,
    pdf_path: Path,
    png_path: Path,
    variable: str = "SSC",
) -> None:
    figsize_in = fig.get_size_inches()
    width_cm = figsize_in[0] * CM_PER_INCH
    height_cm = figsize_in[1] * CM_PER_INCH
    pdf_size_bytes = pdf_path.stat().st_size if pdf_path.exists() else 0
    png_size_bytes = png_path.stat().st_size if png_path.exists() else 0
    n_rows = len(PANEL_SPECS)
    n_cols = max(len(row) for row in PANEL_SPECS)
    period_rows = int(plotting_data["pairing_window"].astype(str).eq("period").sum()) if not plotting_data.empty else 0

    panel_labels = []
    for row in PANEL_SPECS:
        for spec in row:
            if spec is None:
                panel_labels.append("reserved")
            else:
                panel_labels.append("{} [{}; {}]".format(
                    spec.get("source_pair", ""),
                    spec.get("pairing_window", ""),
                    spec.get("temporal_alignment_mode", ""),
                ))

    clines = [
        "# Figure checklist: {}".format(figure_id),
        "",
        "## Basic information",
        "",
        "- Figure file: `{}.pdf` / `{}.png`".format(figure_id, figure_id),
        "- Plotting script: `plot_{}.py`".format(figure_id),
        "- Plotting data: `{}_plotting_data.csv`".format(figure_id),
        "- Date exported: {}".format(datetime.date.today().isoformat()),
        "- Figure type: temporal-alignment validation scatter grid",
        "- Single-panel or multi-panel: multi-panel ({} panels: {} rows x {} columns)".format(
            n_rows * n_cols, n_rows, n_cols
        ),
        "- Panels: {}".format("; ".join(panel_labels)),
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
        "- Colorblind-safe palette used: Yes (Okabe-Ito)",
        "- Categories are distinguished by more than color when needed: Yes, by panel, color, and marker",
        "- Dashed black line marks the 1:1 reference",
        "",
        "## Font and text",
        "",
        "- Single font family used: Yes",
        "- Font family: {}".format(STYLE["font_family"]),
        "- Fonts embedded in vector file: Yes (pdf.fonttype=42)",
        "- Panel labels use `(a)`, `(b)`, etc.: Yes",
        "",
        "## Reproducibility",
        "",
        "- Plotting data saved: Yes (`{}_plotting_data.csv`)".format(figure_id),
        "- Metrics saved: Yes (`{}_metrics.csv`)".format(figure_id),
        "- Input paths documented: Yes (PATHS dict at module top)",
        "- Filtering rules documented: Yes (PANEL_SPECS + temporal alignment filters)",
        "- Audit-only cross-resolution candidates included: No",
        "- Period rows in plotting data: {}".format(period_rows),
        "",
        "## Notes",
        "",
        "- Variable: {}".format(variable),
        "- `period` means interval-aware temporal alignment, not a nearest-day window.",
    ]
    checklist_path.write_text("\n".join(clines) + "\n", encoding="utf-8")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fig9 temporal-alignment scatter grid. Parameters are hardcoded "
            "inside PATHS / PARAMS / STYLE."
        )
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip statistics computation; reuse existing metrics CSV and regenerate figure artefacts.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    plt = _setup_matplotlib()
    configure_matplotlib(plt)

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
    pairs = pd.read_csv(str(pairs_path), keep_default_na=False, low_memory=False)
    print("  {} rows loaded".format(len(pairs)))
    if pairs.empty:
        raise SystemExit("ERROR: paired records DataFrame is empty.")

    required = {"variable", "source_pair", "pairing_window", "satellite_value", "insitu_value"}
    missing = sorted(required - set(pairs.columns))
    if missing:
        raise SystemExit("ERROR: pairs CSV missing required columns: {}".format(", ".join(missing)))

    if "temporal_alignment_mode" not in pairs.columns:
        pairs["temporal_alignment_mode"] = "same_resolution_nearest_day"
    if "is_cross_resolution" not in pairs.columns:
        pairs["is_cross_resolution"] = False
    pairs["is_cross_resolution"] = pairs["is_cross_resolution"].map(_boolish)

    if not {"ssc_bin", "river_width_class", "climate_zone", "high_turbidity"}.issubset(pairs.columns):
        print("Assigning missing strata columns ...")
        pairs = assign_strata(
            pairs,
            high_turbidity_ssc=high_turbidity_ssc,
            ssc_bin_edges=ssc_bin_edges,
        )

    print("Generating temporal-alignment grid ...")
    fig, plot_data, status = make_temporal_grid(plt, pairs, variable=variable)
    print("  -> scatter plot: {}".format(status))
    if status.startswith("skipped"):
        return

    if not args.plot_only:
        print("Computing validation metrics from plotted data ...")
        metrics = compute_satellite_insitu_metrics(plot_data)
        print("  -> {} metric rows computed".format(len(metrics)))
        metrics.to_csv(str(metrics_path), index=False)
        print("Saved metrics to: {}".format(metrics_path))
    elif metrics_path.exists():
        metrics = pd.read_csv(str(metrics_path), keep_default_na=False)
        print("Loaded pre-computed metrics ({} rows) from: {}".format(len(metrics), metrics_path))
    else:
        print("Warning: metrics CSV not found ({}); continuing without metrics.".format(metrics_path))

    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    data_path = figure_dirs["data"] / "{}_plotting_data.csv".format(figure_id)
    checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)

    fig.savefig(str(png_path), dpi=dpi, bbox_inches="tight")
    fig.savefig(str(pdf_path), dpi=dpi, bbox_inches="tight", metadata={"Creator": "Python Matplotlib"})
    plt.close(fig)
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(pdf_path))

    plot_data.to_csv(str(data_path), index=False)
    print("Wrote {}".format(data_path))

    write_checklist(figure_id, fig, plot_data, dpi, checklist_path, pdf_path, png_path, variable=variable)
    print("Wrote {}".format(checklist_path))

    if str(Path(__file__).resolve()) != str(script_path.resolve()):
        shutil.copy(__file__, str(script_path))
        print("Copied script to: {}".format(script_path))
    else:
        print("Script already in scripts/ directory; skipping self-copy.")

    print("\nDone.")


if __name__ == "__main__":
    main()
