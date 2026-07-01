#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Combined QC2 + QC3 diagnostic plots for HYDAT station 01CE004.

This script produces a 2x2 figure combining:
  (a) Q time-series with QC2 log-IQR suspect highlights
  (b) SSC time-series with QC2 log-IQR suspect highlights
  (c) SSC-Q log-log scatter with QC2/QC3 classification
  (d) SSC-Q residual time-series with QC2/QC3 classification

All QC steps are recomputed locally for self-containment.
"""

from pathlib import Path
import warnings
from datetime import datetime
import shutil

import numpy as np
import pandas as pd
import netCDF4 as nc
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec


# ============================================================
# Absolute paths
# ============================================================

PROJECT_ROOT = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111").resolve()

HYDAT_QC_DIR = (
    PROJECT_ROOT
    / "Output_r"
    / "daily"
    / "HYDAT"
    / "qc"
).resolve()

FIGURE_ROOT = (
    PROJECT_ROOT
    / "Output_r"
    / "scripts_basin_test"
    / "figures"
).resolve()

FIGURE_FINAL_DIR = (FIGURE_ROOT / "final").resolve()
FIGURE_DATA_DIR = (FIGURE_ROOT / "data").resolve()
FIGURE_SCRIPTS_DIR = (FIGURE_ROOT / "scripts").resolve()
FIGURE_CHECKLIST_DIR = (FIGURE_ROOT / "checklists").resolve()


# ============================================================
# Hardcoded run parameters
# ============================================================

STATION_ID = "01CE004"
OUT_NAME = None  # Will become "QC2_QC3_{station_id}"

K = 1.5          # IQR multiplier for QC2 and QC3
MIN_SAMPLES = 5  # Minimum samples for QC2/QC3 fitting


# ============================================================
# ESSD-style Matplotlib settings (DejaVu Sans)
# ============================================================

mpl.rcParams["font.family"] = "DejaVu Sans"
mpl.rcParams["pdf.fonttype"] = 42
mpl.rcParams["ps.fonttype"] = 42
mpl.rcParams["svg.fonttype"] = "none"
mpl.rcParams["font.size"] = 15
mpl.rcParams["axes.labelsize"] = 16
mpl.rcParams["axes.titlesize"] = 17
mpl.rcParams["xtick.labelsize"] = 14
mpl.rcParams["ytick.labelsize"] = 14
mpl.rcParams["legend.fontsize"] = 14

# ============================================================
# Centralized font size configuration
# ============================================================

PANEL_LABEL_FONTSIZE = 18
LEGEND_FONTSIZE = 14


# ============================================================
# Constants
# ============================================================

FILL_VALUE_FLOAT = -9999.0
FLAG_GOOD = 0
FLAG_ESTIMATED = 1
FLAG_SUSPECT = 2
FLAG_BAD = 3
FLAG_NOT_CHECKED = 8
FLAG_MISSING = 9

OKABE_ITO = {
    "black": "#000000",
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
    "gray": "#999999",
    "light_gray": "#D9D9D9",
}


# ============================================================
# Utility functions
# ============================================================

def ensure_figure_dirs():
    """Create all required figure output directories."""
    for d in [
        FIGURE_FINAL_DIR,
        FIGURE_DATA_DIR,
        FIGURE_SCRIPTS_DIR,
        FIGURE_CHECKLIST_DIR,
    ]:
        d.mkdir(parents=True, exist_ok=True)


def as_float_array(values):
    """Convert NetCDF or masked array to float, replacing fill values with NaN."""
    if np.ma.isMaskedArray(values):
        arr = np.ma.filled(values, np.nan).astype(float)
    else:
        arr = np.asarray(values, dtype=float)
    arr[~np.isfinite(arr)] = np.nan
    for fv in [FILL_VALUE_FLOAT, -9999.0, -999.0]:
        arr[np.isclose(arr, fv, rtol=1e-5, atol=1e-5)] = np.nan
    return arr


def read_time_as_datetime(ds):
    """Read NetCDF time variable and convert to pandas DatetimeIndex."""
    time_var = ds.variables["time"]
    time_values = time_var[:]
    units = getattr(time_var, "units", "days since 1970-01-01 00:00:00")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        dates = nc.num2date(time_values, units=units, calendar=calendar)
        return pd.to_datetime([str(d) for d in dates])
    except Exception:
        warnings.warn(
            "Failed to decode NetCDF time. Falling back to days since 1970-01-01."
        )
        return pd.to_datetime(time_values, unit="D", origin="1970-01-01")


def finite_mask(values):
    """Return finite-value mask."""
    return np.isfinite(np.asarray(values, dtype=float))


# ============================================================
# QC recomputation functions (local, from plot_qc3_only.py)
# ============================================================

def apply_qc1_physical(values):
    """Recompute QC1 physical plausibility flags."""
    values = np.asarray(values, dtype=float)
    flag = np.full(values.shape, FLAG_GOOD, dtype=np.int8)
    flag[~np.isfinite(values)] = FLAG_MISSING
    flag[np.isfinite(values) & (values < 0)] = FLAG_BAD
    return flag


def compute_log_iqr_bounds(values, k=1.5, min_samples=5):
    """Compute log-IQR bounds in the original value space."""
    values = np.asarray(values, dtype=float)
    eval_mask = np.isfinite(values) & (values > 0)
    if eval_mask.sum() < min_samples:
        return None
    log_values = np.log10(values[eval_mask])
    q1, q3 = np.percentile(log_values, [25, 75])
    iqr = q3 - q1
    lower = 10 ** (q1 - k * iqr)
    upper = 10 ** (q3 + k * iqr)
    return lower, upper


def apply_qc2_log_iqr(values, qc1_flag, k=1.5, min_samples=5):
    """Recompute QC2 log-IQR screening."""
    values = np.asarray(values, dtype=float)
    qc1_flag = np.asarray(qc1_flag)
    step_flag = np.full(values.shape, FLAG_NOT_CHECKED, dtype=np.int8)
    final_flag = qc1_flag.copy()
    step_flag[qc1_flag == FLAG_MISSING] = FLAG_MISSING
    eval_mask = (
        (qc1_flag == FLAG_GOOD)
        & np.isfinite(values)
        & (values > 0)
    )
    if eval_mask.sum() < min_samples:
        return step_flag, final_flag, None, None
    bounds = compute_log_iqr_bounds(values[eval_mask], k=k, min_samples=min_samples)
    if bounds is None:
        return step_flag, final_flag, None, None
    lower, upper = bounds
    step_flag[eval_mask] = FLAG_GOOD
    suspect_mask = eval_mask & ((values < lower) | (values > upper))
    step_flag[suspect_mask] = FLAG_SUSPECT
    final_flag[suspect_mask] = FLAG_SUSPECT
    return step_flag, final_flag, lower, upper


def build_ssc_q_envelope_after_qc2(
    Q, SSC, Q_flag_after_qc2, SSC_flag_after_qc2, k=1.5, min_samples=5,
):
    """Build station-level SSC-Q envelope after QC2."""
    Q = np.asarray(Q, dtype=float)
    SSC = np.asarray(SSC, dtype=float)
    Q_flag_after_qc2 = np.asarray(Q_flag_after_qc2)
    SSC_flag_after_qc2 = np.asarray(SSC_flag_after_qc2)
    fit_mask = (
        np.isfinite(Q)
        & np.isfinite(SSC)
        & (Q > 0)
        & (SSC > 0)
        & (Q_flag_after_qc2 == FLAG_GOOD)
        & (SSC_flag_after_qc2 == FLAG_GOOD)
    )
    if fit_mask.sum() < min_samples:
        return None
    logQ = np.log10(Q[fit_mask])
    logSSC = np.log10(SSC[fit_mask])
    coef = np.polyfit(logQ, logSSC, 1)
    pred = np.polyval(coef, logQ)
    resid = logSSC - pred
    q1, q3 = np.percentile(resid, [25, 75])
    iqr = q3 - q1
    return {
        "coef": coef,
        "lower": q1 - k * iqr,
        "upper": q3 + k * iqr,
        "n_fit": int(fit_mask.sum()),
        "fit_mask": fit_mask,
    }


def apply_qc3_ssc_q(Q, SSC, Q_flag_after_qc2, SSC_flag_after_qc2, ssc_q_bounds):
    """Recompute QC3 SSC-Q consistency flags."""
    Q = np.asarray(Q, dtype=float)
    SSC = np.asarray(SSC, dtype=float)
    Q_flag_after_qc2 = np.asarray(Q_flag_after_qc2)
    SSC_flag_after_qc2 = np.asarray(SSC_flag_after_qc2)
    step_flag = np.full(Q.shape, FLAG_NOT_CHECKED, dtype=np.int8)
    residual = np.full(Q.shape, np.nan, dtype=float)
    missing_mask = ~np.isfinite(Q) | ~np.isfinite(SSC)
    step_flag[missing_mask] = FLAG_MISSING
    if ssc_q_bounds is None:
        return step_flag, residual
    eval_mask = (
        np.isfinite(Q)
        & np.isfinite(SSC)
        & (Q > 0)
        & (SSC > 0)
        & (Q_flag_after_qc2 == FLAG_GOOD)
        & (SSC_flag_after_qc2 == FLAG_GOOD)
    )
    step_flag[eval_mask] = FLAG_GOOD
    coef = ssc_q_bounds["coef"]
    logQ = np.log10(Q[eval_mask])
    logSSC = np.log10(SSC[eval_mask])
    logSSC_pred = coef[0] * logQ + coef[1]
    residual_eval = logSSC - logSSC_pred
    residual[eval_mask] = residual_eval
    suspect_eval = (
        (residual_eval < ssc_q_bounds["lower"])
        | (residual_eval > ssc_q_bounds["upper"])
    )
    eval_indices = np.where(eval_mask)[0]
    suspect_indices = eval_indices[suspect_eval]
    step_flag[suspect_indices] = FLAG_SUSPECT
    return step_flag, residual


def compute_residual_for_all_valid(Q, SSC, ssc_q_bounds):
    """Compute SSC-Q residuals for all positive finite Q-SSC pairs (plotting only)."""
    Q = np.asarray(Q, dtype=float)
    SSC = np.asarray(SSC, dtype=float)
    logQ_all = np.full(Q.shape, np.nan, dtype=float)
    logSSC_all = np.full(SSC.shape, np.nan, dtype=float)
    residual_all = np.full(SSC.shape, np.nan, dtype=float)
    valid = np.isfinite(Q) & np.isfinite(SSC) & (Q > 0) & (SSC > 0)
    if ssc_q_bounds is None:
        return logQ_all, logSSC_all, residual_all
    coef = ssc_q_bounds["coef"]
    logQ_all[valid] = np.log10(Q[valid])
    logSSC_all[valid] = np.log10(SSC[valid])
    residual_all[valid] = logSSC_all[valid] - (
        coef[0] * logQ_all[valid] + coef[1]
    )
    return logQ_all, logSSC_all, residual_all


def classify_points(valid, q_qc2_flag, ssc_qc2_flag, ssc_qc3_flag):
    """Classify points for plotting: QC2 suspect > QC3 suspect > not flagged."""
    valid = np.asarray(valid, dtype=bool)
    qc2_suspect = valid & (
        (q_qc2_flag == FLAG_SUSPECT) | (ssc_qc2_flag == FLAG_SUSPECT)
    )
    qc3_suspect = valid & (ssc_qc3_flag == FLAG_SUSPECT)
    not_flagged = valid & (~qc2_suspect) & (~qc3_suspect)
    other = valid & (~qc2_suspect) & (~qc3_suspect) & (~not_flagged)
    return {
        "qc2_suspect": qc2_suspect,
        "qc3_suspect": qc3_suspect,
        "not_flagged": not_flagged,
        "other": other,
    }


# ============================================================
# QC2 time-series panel (ported from plot_qc2_only.py)
# ============================================================

def plot_qc2_timeseries_panel(
    ax, dates, values, qc2_suspect_mask,
    variable_label, unit_label,
    lower_bound, upper_bound,
    show_legend=True,
):
    """Plot a time-series panel highlighting QC2 log-IQR suspect points."""
    values = np.asarray(values, dtype=float)
    valid = finite_mask(values)
    qc2_suspect_mask = np.asarray(qc2_suspect_mask, dtype=bool) & valid

    # Connecting line between all valid points
    ax.plot(
        dates[valid],
        values[valid],
        color=OKABE_ITO["light_gray"],
        linewidth=0.7,
        alpha=0.9,
        zorder=1,
    )

    # Scatter for all non-missing points
    ax.scatter(
        dates[valid],
        values[valid],
        s=10,
        color=OKABE_ITO["gray"],
        alpha=0.45,
        label="All non-missing points",
        zorder=2,
        rasterized=True,
    )

    # Highlight QC2 suspect points with orange triangles
    if np.any(qc2_suspect_mask):
        ax.scatter(
            dates[qc2_suspect_mask],
            values[qc2_suspect_mask],
            s=36,
            marker="^",
            facecolor=OKABE_ITO["orange"],
            edgecolor=OKABE_ITO["black"],
            linewidth=0.4,
            label=f"QC2 suspect points (n = {int(qc2_suspect_mask.sum())})",
            zorder=5,
        )

    # Log-IQR bounds as horizontal dashed lines
    bound_label_added = False
    if lower_bound is not None and np.isfinite(lower_bound):
        ax.axhline(
            lower_bound,
            color=OKABE_ITO["black"],
            linestyle="--",
            linewidth=1.0,
            alpha=0.9,
            label="Log-IQR bounds",
            zorder=3,
        )
        bound_label_added = True
    if upper_bound is not None and np.isfinite(upper_bound):
        ax.axhline(
            upper_bound,
            color=OKABE_ITO["black"],
            linestyle="--",
            linewidth=1.0,
            alpha=0.9,
            label=None if bound_label_added else "Log-IQR bounds",
            zorder=3,
        )

    ax.set_yscale("symlog", linthresh=1.0)
    ax.set_ylabel(f"{variable_label} ({unit_label})")
    # ax.set_title(variable_label, loc="left")  # removed per user request
    ax.grid(True, alpha=0.25, linewidth=0.5)
    if show_legend:
        ax.legend(frameon=False, loc="best")


# ============================================================
# Save combined plotting data and checklist
# ============================================================

def save_combined_plotting_data(
    out_csv,
    time, Q, SSC,
    Q_final_flag_from_nc, SSC_final_flag_from_nc,
    Q_qc1_flag, SSC_qc1_flag,
    Q_qc2_flag, SSC_qc2_flag,
    Q_qc2_lower, Q_qc2_upper,
    SSC_qc2_lower, SSC_qc2_upper,
    SSC_qc3_flag, valid, logQ, logSSC, residual, categories,
):
    """Save combined plotting data from QC2 and QC3 figures."""
    category = np.full(Q.shape, "not_plotted", dtype=object)
    category[categories["not_flagged"]] = "not_flagged_by_qc2_or_qc3"
    category[categories["qc2_suspect"]] = "qc2_log_iqr_suspect"
    category[categories["qc3_suspect"]] = "qc3_ssc_q_suspect"
    category[categories["other"]] = "other"

    df = pd.DataFrame(
        {
            "station_id": STATION_ID,
            "date": time.strftime("%Y-%m-%d"),
            "Q_m3_s": Q,
            "SSC_mg_L": SSC,
            "Q_flag_qc1_physical": Q_qc1_flag,
            "SSC_flag_qc1_physical": SSC_qc1_flag,
            "Q_flag_qc2_log_iqr": Q_qc2_flag,
            "SSC_flag_qc2_log_iqr": SSC_qc2_flag,
            "Q_qc2_suspect": Q_qc2_flag == FLAG_SUSPECT,
            "SSC_qc2_suspect": SSC_qc2_flag == FLAG_SUSPECT,
            "Q_final_flag": Q_final_flag_from_nc,
            "SSC_final_flag": SSC_final_flag_from_nc,
            "Q_qc2_lower_m3_s": Q_qc2_lower if Q_qc2_lower is not None else np.nan,
            "Q_qc2_upper_m3_s": Q_qc2_upper if Q_qc2_upper is not None else np.nan,
            "SSC_qc2_lower_mg_L": SSC_qc2_lower if SSC_qc2_lower is not None else np.nan,
            "SSC_qc2_upper_mg_L": SSC_qc2_upper if SSC_qc2_upper is not None else np.nan,
            "SSC_flag_qc3_ssc_q": SSC_qc3_flag,
            "plot_category": category,
            "log10_Q": logQ,
            "log10_SSC": logSSC,
            "ssc_q_residual_log10": residual,
        }
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)


def write_combined_checklist(
    checklist_path, pdf_path, png_path, plotting_data_path,
    script_path, station_id, width_cm, height_cm, dpi,
):
    """Write ESSD-style figure checklist for the combined figure."""
    try:
        pdf_size_mb = Path(pdf_path).stat().st_size / (1024 * 1024)
    except OSError:
        pdf_size_mb = np.nan
    try:
        png_size_mb = Path(png_path).stat().st_size / (1024 * 1024)
    except OSError:
        png_size_mb = np.nan

    text = f"""# Figure checklist: QC2_QC3_{station_id}

## File information

- Figure name: QC2_QC3_{station_id}
- Final vector figure: `{pdf_path}`
- Preview bitmap figure: `{png_path}`
- Plotting data: `{plotting_data_path}`
- Plotting script: `{script_path}`
- Checklist: `{checklist_path}`

## Format and resolution

- Preferred vector format used: yes, PDF
- Bitmap preview exported: yes, PNG
- Bitmap dpi: {dpi}
- PDF file size: {pdf_size_mb:.3f} MB
- PNG file size: {png_size_mb:.3f} MB

## Size and layout

- Figure size: {width_cm:.0f} mm x {height_cm:.0f} mm
- Figure width >= 8 cm: yes
- Multi-panel layout: 2 rows x 2 columns
- Panel labels: (a), (b), (c), (d)
- Full caption embedded in figure: no

## Panels

- Panel (a): Q time-series with QC2 log-IQR suspect points and bounds
- Panel (b): SSC time-series with QC2 log-IQR suspect points and bounds
- Panel (c): SSC-Q log-log scatter with QC2/QC3 classification and fitted envelope
- Panel (d): SSC-Q residual time-series with QC2/QC3 classification

## Fonts

- Font family: DejaVu Sans
- Minimum visible font size: 11 pt
- Single font family used: yes
- PDF font embedding setting: pdf.fonttype = 42

## Colors and symbols

- Colorblind-safe palette: yes, Okabe-Ito
- Red-green contrast avoided: yes
- QC2 suspect points: orange triangles with black outline
- QC3 suspect points: reddish-purple diamonds with black outline
- Not flagged points (panels c, d): blue circles
- All non-missing points (panels a, b): gray circles
- Log-IQR bounds (panels a, b): black dashed lines
- Fitted SSC-Q trend (panel c): solid black line
- IQR residual envelope (panels c, d): black dashed lines
- Legend explains colors, markers, and line types: yes

## Units and labels

- Q unit: m3 s-1 (panels a, c)
- SSC unit: mg L-1 (panels b, c)
- Panel (a, b) y-axis: symlog scale
- Panel (c) axes: log10 scale
- Sentence case labels used: yes
- Axis labels include units: yes

## Reproducibility

- Plotting data saved: yes
- Input file: HYDAT_{station_id}.nc (HYDAT/qc directory)
- Station ID: {station_id}
- QC parameters: k = {K}, min_samples = {MIN_SAMPLES}

## Notes

This figure combines QC2 log-IQR screening (panels a, b) with QC3 SSC-Q hydrological
consistency screening (panels c, d). QC2 flags are recomputed locally; QC3 uses a
linear fit in log-log space with an IQR residual envelope fitted only on records
that pass both QC1 and QC2. QC2 suspect points retain their label in the QC3 panels
and are not re-evaluated by QC3.
"""
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text(text, encoding="utf-8")


def copy_script_to_figure_scripts():
    """Copy this script to figures/scripts for reproducibility."""
    try:
        src = Path(__file__).resolve()
        dst = FIGURE_SCRIPTS_DIR / src.name
        FIGURE_SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
        if src != dst:
            shutil.copy2(src, dst)
        return dst
    except Exception as exc:
        warnings.warn(f"Failed to copy plotting script: {exc}")
        return Path(__file__).resolve()


def find_hydat_nc(station_id, qc_dir=HYDAT_QC_DIR):
    """Find HYDAT_<station_id>.nc under the HYDAT QC directory."""
    qc_dir = Path(qc_dir).expanduser().resolve()
    candidates = [
        qc_dir / f"HYDAT_{station_id}.nc",
        qc_dir / f"{station_id}.nc",
    ]
    for path in candidates:
        if path.exists():
            return path.resolve()
    matches = sorted(qc_dir.glob(f"*{station_id}*.nc"))
    if matches:
        return matches[0].resolve()
    raise FileNotFoundError(
        f"Cannot find NetCDF file for station_id={station_id} in {qc_dir}"
    )


# ============================================================
# Main
# ============================================================

def main():
    ensure_figure_dirs()

    # --- Find and read input ---
    nc_file = find_hydat_nc(STATION_ID, HYDAT_QC_DIR)
    print(f"Reading input file: {nc_file}")

    with nc.Dataset(nc_file, "r") as ds:
        time = read_time_as_datetime(ds)
        Q = as_float_array(ds.variables["Q"][:])
        SSC = as_float_array(ds.variables["SSC"][:])
        Q_final_flag_from_nc = np.asarray(ds.variables["Q_flag"][:])
        SSC_final_flag_from_nc = np.asarray(ds.variables["SSC_flag"][:])
        station_id = getattr(ds, "location_id", nc_file.stem.replace("HYDAT_", ""))
        station_name = getattr(ds, "station_name", station_id)

    out_name = OUT_NAME if OUT_NAME is not None else f"QC2_QC3_{station_id}"

    out_pdf = FIGURE_FINAL_DIR / f"{out_name}.pdf"
    out_png = FIGURE_FINAL_DIR / f"{out_name}.png"
    out_data = FIGURE_DATA_DIR / f"{out_name}_plotting_data.csv"
    out_checklist = FIGURE_CHECKLIST_DIR / f"{out_name}_checklist.md"
    out_script = copy_script_to_figure_scripts()

    # --- Recompute QC1 ---
    Q_qc1_flag = apply_qc1_physical(Q)
    SSC_qc1_flag = apply_qc1_physical(SSC)

    # --- Recompute QC2 log-IQR ---
    Q_qc2_flag, Q_flag_after_qc2, Q_qc2_lower, Q_qc2_upper = apply_qc2_log_iqr(
        values=Q,
        qc1_flag=Q_qc1_flag,
        k=K,
        min_samples=MIN_SAMPLES,
    )
    SSC_qc2_flag, SSC_flag_after_qc2, SSC_qc2_lower, SSC_qc2_upper = apply_qc2_log_iqr(
        values=SSC,
        qc1_flag=SSC_qc1_flag,
        k=K,
        min_samples=MIN_SAMPLES,
    )

    # --- Recompute QC3 SSC-Q envelope ---
    valid = np.isfinite(Q) & np.isfinite(SSC) & (Q > 0) & (SSC > 0)

    ssc_q_bounds = build_ssc_q_envelope_after_qc2(
        Q=Q,
        SSC=SSC,
        Q_flag_after_qc2=Q_flag_after_qc2,
        SSC_flag_after_qc2=SSC_flag_after_qc2,
        k=K,
        min_samples=MIN_SAMPLES,
    )

    if ssc_q_bounds is None:
        raise ValueError(
            "Cannot build SSC-Q envelope. "
            "Too few valid samples after QC2."
        )

    SSC_qc3_flag, _ = apply_qc3_ssc_q(
        Q=Q,
        SSC=SSC,
        Q_flag_after_qc2=Q_flag_after_qc2,
        SSC_flag_after_qc2=SSC_flag_after_qc2,
        ssc_q_bounds=ssc_q_bounds,
    )

    logQ_all, logSSC_all, residual_all = compute_residual_for_all_valid(
        Q=Q,
        SSC=SSC,
        ssc_q_bounds=ssc_q_bounds,
    )

    categories = classify_points(
        valid=valid,
        q_qc2_flag=Q_qc2_flag,
        ssc_qc2_flag=SSC_qc2_flag,
        ssc_qc3_flag=SSC_qc3_flag,
    )

    # QC2 suspect masks for time-series panels
    q_qc2_suspect = Q_qc2_flag == FLAG_SUSPECT
    ssc_qc2_suspect = SSC_qc2_flag == FLAG_SUSPECT

    # --- Save combined plotting data ---
    save_combined_plotting_data(
        out_csv=out_data,
        time=time, Q=Q, SSC=SSC,
        Q_final_flag_from_nc=Q_final_flag_from_nc,
        SSC_final_flag_from_nc=SSC_final_flag_from_nc,
        Q_qc1_flag=Q_qc1_flag, SSC_qc1_flag=SSC_qc1_flag,
        Q_qc2_flag=Q_qc2_flag, SSC_qc2_flag=SSC_qc2_flag,
        Q_qc2_lower=Q_qc2_lower, Q_qc2_upper=Q_qc2_upper,
        SSC_qc2_lower=SSC_qc2_lower, SSC_qc2_upper=SSC_qc2_upper,
        SSC_qc3_flag=SSC_qc3_flag,
        valid=valid, logQ=logQ_all, logSSC=logSSC_all,
        residual=residual_all, categories=categories,
    )

    # --- Create figure ---
    width_cm = 35.0
    height_cm = 25.0
    dpi = 300
    figsize = (width_cm / 2.54, height_cm / 2.54)


    fig = plt.figure(figsize=figsize)
    gs = GridSpec(4, 2, figure=fig, wspace=0.18, hspace=0.15)

    # Left column: QC2 time-series panels — 1:1 height ratio (2 rows each)
    axes_q = fig.add_subplot(gs[0:2, 0])      # Panel (a): Q
    axes_ssc = fig.add_subplot(gs[2:4, 0])    # Panel (b): SSC

    # Right column: QC3 diagnostic panels — 3:1 height ratio (3 rows + 1 row)
    axes_scatter = fig.add_subplot(gs[0:3, 1])   # Panel (c): SSC-Q scatter
    axes_residual = fig.add_subplot(gs[3, 1])    # Panel (d): residual

    axes = {'q': axes_q, 'ssc': axes_ssc, 'scatter': axes_scatter, 'residual': axes_residual}

    # ------------------------------------------------------------------
    # Panel (a): Q time-series with QC2 highlights (top-left)
    # ------------------------------------------------------------------
    plot_qc2_timeseries_panel(
        ax=axes['q'],
        dates=time,
        values=Q,
        qc2_suspect_mask=q_qc2_suspect,
        variable_label="Q",
        unit_label="m³ s⁻¹",
        lower_bound=Q_qc2_lower,
        upper_bound=Q_qc2_upper,
        show_legend=False,
    )
    axes['q'].tick_params(labelbottom=False)

    # ------------------------------------------------------------------
    # Panel (b): SSC time-series with QC2 highlights (bottom-left)
    # ------------------------------------------------------------------
    plot_qc2_timeseries_panel(
        ax=axes['ssc'],
        dates=time,
        values=SSC,
        qc2_suspect_mask=ssc_qc2_suspect,
        variable_label="SSC",
        unit_label="mg L⁻¹",
        lower_bound=SSC_qc2_lower,
        upper_bound=SSC_qc2_upper,
    )
    axes['ssc'].set_xlabel("Time")
    # Panel label (b)
    axes['ssc'].text(
        -0.05, 0.98, "(b)",
        transform=axes['ssc'].transAxes,
        ha="right", va="top", fontsize=PANEL_LABEL_FONTSIZE, fontweight="bold",
    )
    # Restrict SSC y-axis to data range +5% margin
    ssc_valid = SSC[np.isfinite(SSC)]
    if len(ssc_valid) > 0:
        axes['ssc'].set_ylim(bottom=0, top=np.nanmax(ssc_valid) * 1.05)

    # ------------------------------------------------------------------
    # Panel (c): SSC-Q log-log scatter (top-right)
    # ------------------------------------------------------------------
    ax_c = axes['scatter']

    if np.any(categories["not_flagged"]):
        m = categories["not_flagged"]
        ax_c.scatter(
            logQ_all[m], logSSC_all[m],
            s=20, c=OKABE_ITO["blue"], alpha=0.65, marker="o",
            label=f"Not flagged by QC2/QC3 (n = {m.sum()})",
        )

    if np.any(categories["qc2_suspect"]):
        m = categories["qc2_suspect"]
        ax_c.scatter(
            logQ_all[m], logSSC_all[m],
            s=28, facecolor=OKABE_ITO["orange"],
            edgecolor=OKABE_ITO["black"],
            linewidth=0.35, alpha=0.85, marker="^",
            label=f"QC2 log-IQR suspect (n = {m.sum()})",
        )

    if np.any(categories["qc3_suspect"]):
        m = categories["qc3_suspect"]
        ax_c.scatter(
            logQ_all[m], logSSC_all[m],
            s=34, facecolor=OKABE_ITO["reddish_purple"],
            edgecolor=OKABE_ITO["black"],
            linewidth=0.35, alpha=0.90, marker="D",
            label=f"QC3 SSC-Q suspect (n = {m.sum()})",
        )

    # Fitted trend and envelope lines
    x_line = np.linspace(
        np.nanmin(logQ_all[valid]),
        np.nanmax(logQ_all[valid]),
        200,
    )
    coef = ssc_q_bounds["coef"]
    y_mid = coef[0] * x_line + coef[1]
    y_low = y_mid + ssc_q_bounds["lower"]
    y_up = y_mid + ssc_q_bounds["upper"]

    ax_c.plot(
        x_line, y_mid,
        color=OKABE_ITO["black"], linestyle="-", linewidth=2,
        label="Fitted SSC-Q trend",
    )
    ax_c.plot(
        x_line, y_low,
        color=OKABE_ITO["black"], linestyle="--", linewidth=1,
        label="IQR residual envelope",
    )
    ax_c.plot(
        x_line, y_up,
        color=OKABE_ITO["black"], linestyle="--", linewidth=1,
    )

    ax_c.text(
        -0.10, 0.98, "(c)",
        transform=ax_c.transAxes, ha="right", va="top", fontsize=PANEL_LABEL_FONTSIZE, fontweight="bold",
    )
    ax_c.set_xlabel("log10(Q) [m³ s⁻¹]")
    ax_c.set_ylabel("log10(SSC) [mg L⁻¹]")
    # ax_c.set_title(f"SSC-Q diagnostic for {station_name} ({station_id})")
    ax_c.legend(frameon=True, fontsize=LEGEND_FONTSIZE, loc='upper left')

    # ------------------------------------------------------------------
    # Panel (d): Residual time-series (bottom-right)
    # ------------------------------------------------------------------
    ax_d = axes['residual']

    if np.any(categories["not_flagged"]):
        m = categories["not_flagged"]
        ax_d.scatter(
            time[m], residual_all[m],
            s=15, c=OKABE_ITO["blue"], alpha=0.65, marker="o",
        )

    if np.any(categories["qc2_suspect"]):
        m = categories["qc2_suspect"]
        ax_d.scatter(
            time[m], residual_all[m],
            s=22, facecolor=OKABE_ITO["orange"],
            edgecolor=OKABE_ITO["black"],
            linewidth=0.35, alpha=0.85, marker="^",
        )

    if np.any(categories["qc3_suspect"]):
        m = categories["qc3_suspect"]
        ax_d.scatter(
            time[m], residual_all[m],
            s=26, facecolor=OKABE_ITO["reddish_purple"],
            edgecolor=OKABE_ITO["black"],
            linewidth=0.35, alpha=0.90, marker="D",
        )

    ax_d.axhline(0, color=OKABE_ITO["black"], linewidth=1)
    ax_d.axhline(
        ssc_q_bounds["lower"],
        color=OKABE_ITO["black"], linestyle="--", linewidth=1,
    )
    ax_d.axhline(
        ssc_q_bounds["upper"],
        color=OKABE_ITO["black"], linestyle="--", linewidth=1,
    )

    ax_d.text(
        -0.10, 0.95, "(d)",
        transform=ax_d.transAxes, ha="right", va="top", fontsize=PANEL_LABEL_FONTSIZE, fontweight="bold"
    )
    ax_d.set_ylabel("Residual\n(log SSC)")
    ax_d.set_xlabel("Time")

    # ------------------------------------------------------------------
    # Station annotation on top-left panel
    # ------------------------------------------------------------------
    # Panel label (a)
    axes['q'].text(
        -0.05, 0.98, "(a)",
        transform=axes['q'].transAxes,
        ha="right", va="top", fontsize=PANEL_LABEL_FONTSIZE, fontweight="bold",
    )


    # ------------------------------------------------------------------
    # Save figure outputs
    # ------------------------------------------------------------------
    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    fig.subplots_adjust(left=0.16, right=0.97, bottom=0.07, top=0.96)
    fig.savefig(out_pdf, bbox_inches="tight")
    fig.savefig(out_png, dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    # ------------------------------------------------------------------
    # Write checklist
    # ------------------------------------------------------------------
    write_combined_checklist(
        checklist_path=out_checklist,
        pdf_path=out_pdf, png_path=out_png,
        plotting_data_path=out_data, script_path=out_script,
        station_id=station_id, width_cm=width_cm, height_cm=height_cm, dpi=dpi,
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"Saved PDF: {out_pdf}")
    print(f"Saved PNG: {out_png}")
    print(f"Saved plotting data: {out_data}")
    print(f"Saved checklist: {out_checklist}")
    print(f"Saved script copy: {out_script}")
    print()

    print("QC2 suspect per variable (time-series panels)")
    print(f"  Q QC2 suspect points:     {int(q_qc2_suspect.sum())}")
    print(f"  SSC QC2 suspect points:   {int(ssc_qc2_suspect.sum())}")
    if Q_qc2_lower is not None and Q_qc2_upper is not None:
        print(f"  Q QC2 bounds:             {Q_qc2_lower:.4f}, {Q_qc2_upper:.4f}")
    if SSC_qc2_lower is not None and SSC_qc2_upper is not None:
        print(f"  SSC QC2 bounds:           {SSC_qc2_lower:.4f}, {SSC_qc2_upper:.4f}")
    print()

    print("QC3 diagnostic summary (right-column panels)")
    print(f"  Valid Q-SSC pairs:        {int(valid.sum())}")
    print(f"  Fitting samples after QC2:{ssc_q_bounds['n_fit']}")
    print(f"  QC2 suspect points:       {int(categories['qc2_suspect'].sum())}")
    print(f"  QC3 suspect points:       {int(categories['qc3_suspect'].sum())}")
    print(f"  SSC-Q residual bounds:    {ssc_q_bounds['lower']:.4f}, {ssc_q_bounds['upper']:.4f}")


if __name__ == "__main__":
    main()
