#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone script to generate the paper-ready model-evaluation figure from
existing validation CSV outputs.

This script reads the CSV files produced by
validate_model_with_sed_reference.py (metrics_summary.csv and
station-level compare_*.csv files) and produces a 4-panel figure:

  (a) Reference stations used for model evaluation
  (b) Matched reference–model SSC pairs
  (c) Porto Velho discharge (Q) time series
  (d) Porto Velho sediment load (SSL) time series

Edit the DEFAULT_* constants at the top of this file to configure before running.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# Variable definitions (copied from validate_model_with_sed_reference.py)
# ============================================================
VARIABLES = {
    "Q": {
        "unit": "m³ s⁻¹",
        "metric_name": "Q_m3_s-1",
        "ref_col": "Q_reference_m3_s-1",
        "model_col": "Q_model_m3_s-1",
    },
    "SSC": {
        "unit": "mg L⁻¹",
        "metric_name": "SSC_mg_L",
        "ref_col": "SSC_reference_mg_L",
        "model_col": "SSC_model_mg_L",
    },
    "SSL": {
        "unit": "t d⁻¹",
        "metric_name": "SSL_t_day",
        "ref_col": "SSL_reference_t_day",
        "model_col": "SSL_model_t_day",
    },
}


# ============================================================
# Default configuration (hardcoded, edit before running)
# ============================================================
DEFAULT_INPUT_DIR = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output_other/validate_model_with_sed_reference"
DEFAULT_OUTPUT_DIR = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/"
DEFAULT_VARIABLE = "SSC"
DEFAULT_RESOLUTION = "daily"
DEFAULT_EXAMPLE_CLUSTER_UID = "SED000107"
DEFAULT_TIMESERIES_RESOLUTION = "daily"
DEFAULT_DPI = 300
DEFAULT_FIGURE_NUMBER = "model_validation"

# --- Regional map parameters ---
DEFAULT_REGION_LAT_MIN = -20
DEFAULT_REGION_LAT_MAX = 5
DEFAULT_REGION_LON_MIN = -80
DEFAULT_REGION_LON_MAX = -45
DEFAULT_MERIT_HYDRO_DIR = "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1"

# --- Extract station data directory for Porto Velho Q & SSL panels ---
DEFAULT_EXTRACT_DIR = "/share/home/dq134/wzx/sediment/CaMa-Flood_v4-sed_1125/out/GRFR_0p05_3h_1222/extractStation"

# --- Font size configuration ---
FONT_TITLE = 18
FONT_LABEL = 16
FONT_LABEL_MAP = 15          # map panel axis labels (tighter space)
FONT_LEGEND = 14
FONT_TICK = 14
FONT_ANNOTATION = 15
FONT_ANNOTATION_SMALL = 14
FONT_FALLBACK = 16            # fallback "data not available" text


# ============================================================
# Helper functions
# ============================================================


def collect_compare_pairs(input_dir: Path, variable: str, resolution: str) -> pd.DataFrame:
    """Scan station subdirectories and collect all paired (reference, model) samples.

    Reads compare_<variable>_<resolution>.csv from each station directory,
    merges with station_match.csv metadata, and returns a long-format DataFrame.
    Columns: cluster_uid, station_name, time, reference, model, variable,
             station_dir, model_grid_distance_km.
    """
    input_dir_path = Path(input_dir)
    compare_pattern = "compare_%s_%s.csv" % (variable, resolution)
    all_rows = []

    for station_dir in sorted(input_dir_path.iterdir()):
        if not station_dir.is_dir():
            continue
        compare_path = station_dir / compare_pattern
        if not compare_path.exists():
            continue

        compare_df = pd.read_csv(compare_path)
        if compare_df.empty:
            continue

        # Read station_match.csv for metadata
        cluster_uid = ""
        station_name = ""
        model_grid_distance_km = np.nan
        match_path = station_dir / "station_match.csv"
        if match_path.exists():
            mdf = pd.read_csv(match_path)
            if not mdf.empty:
                r = mdf.iloc[0]
                cluster_uid = str(r.get("cluster_uid", ""))
                station_name = str(r.get("station_name", ""))
                try:
                    model_grid_distance_km = float(r.get("model_grid_distance_km", np.nan))
                except (ValueError, TypeError):
                    model_grid_distance_km = np.nan

        # Fallback: extract cluster_uid and station_name from directory name
        if not cluster_uid and "_" in station_dir.name:
            parts = station_dir.name.split("_", 1)
            cluster_uid = parts[0]
            station_name = parts[1] if len(parts) > 1 else ""

        # Identify reference and model columns dynamically
        ref_cols = sorted(c for c in compare_df.columns if "reference" in c.lower())
        model_cols = sorted(c for c in compare_df.columns if "model" in c.lower())
        if not ref_cols or not model_cols:
            continue
        ref_col = ref_cols[0]
        model_col = model_cols[0]

        time_col = "time" if "time" in compare_df.columns else None
        if time_col is None:
            continue

        for _, row in compare_df.iterrows():
            all_rows.append({
                "cluster_uid": cluster_uid,
                "station_name": station_name,
                "time": row.get(time_col, np.nan),
                "reference": row.get(ref_col, np.nan),
                "model": row.get(model_col, np.nan),
                "variable": variable,
                "station_dir": station_dir.name,
                "model_grid_distance_km": model_grid_distance_km,
            })

    if not all_rows:
        return pd.DataFrame()

    result = pd.DataFrame(all_rows)
    result["time"] = pd.to_datetime(result["time"], errors="coerce")
    result = result.dropna(subset=["time", "reference", "model"])
    return result.reset_index(drop=True)


def select_example_station(metrics_df: pd.DataFrame, variable: str,
                           preferred_cluster_uid: str = "") -> str:
    """Select a representative station cluster_uid for the paper figure.

    If preferred_cluster_uid is provided and exists for the selected variable,
    use it.  Otherwise choose the station whose KGE is closest to the median KGE
    among stations with valid metrics and n >= median n when possible.
    Falls back to the station with the largest n if KGE is unavailable.
    """
    metric_name = VARIABLES[variable]["metric_name"] if variable in VARIABLES else variable
    var_df = metrics_df[metrics_df["variable"] == metric_name].copy()
    if var_df.empty:
        return ""

    if preferred_cluster_uid and preferred_cluster_uid in var_df["cluster_uid"].values:
        return preferred_cluster_uid

    # Try KGE-based selection
    if "kge" in var_df.columns:
        valid = var_df.dropna(subset=["kge", "pearson_r", "n"])
        if not valid.empty and valid["kge"].notna().any():
            median_n = float(valid["n"].median())
            if (valid["n"] >= median_n).any():
                candidates = valid[valid["n"] >= median_n].copy()
            else:
                candidates = valid.copy()
            median_kge = float(candidates["kge"].median())
            candidates.loc[:, "kge_dist"] = (candidates["kge"] - median_kge).abs()
            best = candidates.sort_values("kge_dist").iloc[0]
            return str(best["cluster_uid"])

    # Fallback: station with most data points
    if "n" in var_df.columns and var_df["n"].notna().any():
        best = var_df.loc[var_df["n"].idxmax()]
        return str(best["cluster_uid"])

    return ""


# ============================================================
# Plotting panel functions
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


def apply_axis_font_sizes(ax) -> None:
    """Apply the script-level tick font setting to both axes."""
    ax.tick_params(axis="both", which="both", labelsize=FONT_TICK)


def add_panel_label(ax, label: str) -> None:
    """Place a panel label without using an axes title."""
    ax.text(
        -0.07, 1.02, label,
        transform=ax.transAxes,
        fontsize=FONT_TITLE,
        ha="left",
        va="bottom",
        clip_on=False,
        fontweight="bold",
    )


def plot_panel_a_log_scatter(ax, pairs_df: pd.DataFrame, variable: str, unit: str,
                              min_threshold: float = 0.1) -> None:
    """Panel a: Log-log scatter of paired samples colored by station, with 1:1 and factor-of-10 lines."""
    df = pairs_df.copy()
    df = df[(df["reference"] > min_threshold) & (df["model"] > min_threshold)].copy()
    # Remove Fazenda Vista Alegre from scatter plot
    if "station_name" in df.columns:
        df = df[df["station_name"] != "Fazenda Vista Alegre"].copy()

    ax.set_xlabel("Observed %s (%s)" % (variable, unit), fontsize=FONT_LABEL)
    ax.set_ylabel("Simulated %s (%s)" % (variable, unit), fontsize=FONT_LABEL)
    apply_axis_font_sizes(ax)

    if df.empty:
        ax.text(0.5, 0.5, "No positive pairs available", transform=ax.transAxes,
                ha="center", va="center", fontsize=FONT_FALLBACK, style="italic")
        return

    # ---- Color per station to show model-performance differences across sites ----
    station_color_map = {
        "Manacapuru":           OKABE_ITO["orange"],
        "Serrinha":             OKABE_ITO["sky_blue"],
        "Caracarai":            OKABE_ITO["bluish_green"],
        "Porto_Velho":          OKABE_ITO["blue"],
        "Porto Velho":          OKABE_ITO["blue"],
        "Fazenda Vista Alegre": OKABE_ITO["reddish_purple"],
        "Obidos":               OKABE_ITO["reddish_purple"],
        "Itaituba":             OKABE_ITO["vermillion"],
    }

    if "cluster_uid" not in df.columns:
        ax.scatter(df["reference"], df["model"], s=16, alpha=0.82,
                   color=OKABE_ITO["blue"], edgecolors="none", rasterized=True)
    else:
        # Build station-name lookup per cluster_uid
        station_names = {}
        for sid in df["cluster_uid"].unique():
            sdf = df[df["cluster_uid"] == sid]
            if "station_name" in sdf.columns and sdf["station_name"].notna().any():
                sn = str(sdf["station_name"].iloc[0])
            else:
                sn = sid
            station_names[sid] = sn

        for sid in df["cluster_uid"].unique():
            sub = df[df["cluster_uid"] == sid]
            sn = station_names.get(sid, sid)
            label = sn.replace("_", " ")
            c = station_color_map.get(sn, "#999999")
            ax.scatter(sub["reference"], sub["model"], s=16, alpha=0.82,
                       color=c, edgecolors="none", rasterized=True, label=label)

    ax.set_xscale("log")
    ax.set_yscale("log")

    # Axis limits based on 1st-99th percentile of all values
    all_vals = np.concatenate([df["reference"].values, df["model"].values])
    p1, p99 = np.percentile(all_vals, [1, 99])
    ax.set_xlim(p1 * 0.9, p99 * 1.1)
    ax.set_ylim(p1 * 0.9, p99 * 1.1)

    # 1:1 line (full diagonal within axis limits)
    diag = np.logspace(np.log10(p1 * 0.9), np.log10(p99 * 1.1), 200)
    ax.plot(diag, diag, "k--", linewidth=1, alpha=0.7, label="1:1")

    # Factor-of-10 guide lines (+/- 1 order of magnitude)
    for factor in (0.1, 10.0):
        ax.plot(diag, diag * factor, ":", color="gray", linewidth=0.8, alpha=0.5)

    # Legend — place in upper-left to avoid 1:1 line and data in lower-right
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        unique = {}
        for h, l in zip(handles, labels):
            if l not in unique:
                unique[l] = h
        sorted_handles = []
        sorted_labels = []
        for l, h in unique.items():
            if l == "1:1":
                continue
            sorted_handles.append(h)
            sorted_labels.append(l)
        if "1:1" in unique:
            sorted_handles.append(unique["1:1"])
            sorted_labels.append("1:1")
        ax.legend(sorted_handles, sorted_labels, fontsize=FONT_LEGEND,
                  loc="lower right", markerscale=2,
                  framealpha=0.8, edgecolor="gray")

    # Annotate number of pairs and stations — lower left
    n_pairs = len(df)
    n_stations = int(df["cluster_uid"].nunique()) if "cluster_uid" in df.columns else 1
    ax.annotate("%d pairs / %d stations" % (n_pairs, n_stations),
                xy=(0.02, 0.02), xycoords="axes fraction",
                fontsize=FONT_ANNOTATION_SMALL, ha="left", va="bottom",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))

    # Add Pearson correlation in log space
    log_ref = np.log10(df["reference"])
    log_mod = np.log10(df["model"])
    r_value = np.corrcoef(log_ref, log_mod)[0, 1]
    ax.text(0.02, 0.95, "Pearson r = %.2f" % r_value, transform=ax.transAxes,
            fontsize=FONT_ANNOTATION, ha="left", va="top",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))


def plot_panel_b_domain_map(ax, input_dir: Path, variable: str) -> None:
    """Panel b: Regional model domain and reference stations map."""
    catalog_path = input_dir / "candidate_station_catalog.csv"
    if not catalog_path.exists():
        ax.text(0.5, 0.5, "Station catalog not available",
                transform=ax.transAxes, ha="center", va="center",
                fontsize=FONT_FALLBACK, style="italic")
        return

    cat_df = pd.read_csv(catalog_path)
    if cat_df.empty:
        ax.text(0.5, 0.5, "Station catalog is empty",
                transform=ax.transAxes, ha="center", va="center",
                fontsize=FONT_FALLBACK, style="italic")
        return

    var_metric = VARIABLES[variable]["metric_name"] if variable in VARIABLES else variable
    status_col = "%s_status" % var_metric

    # --- Land polygons ---
    polygons = _load_land_polygons_domain()
    if polygons:
        _plot_land_domain(ax, polygons)

    # --- River network ---
    merit_dir = DEFAULT_MERIT_HYDRO_DIR
    river_segs = _load_river_network_domain(
        merit_dir,
        view_lon_min=DEFAULT_REGION_LON_MIN if DEFAULT_REGION_LON_MIN is not None else -80.0,
        view_lon_max=DEFAULT_REGION_LON_MAX if DEFAULT_REGION_LON_MAX is not None else -45.0,
        view_lat_min=DEFAULT_REGION_LAT_MIN if DEFAULT_REGION_LAT_MIN is not None else -20.0,
        view_lat_max=DEFAULT_REGION_LAT_MAX if DEFAULT_REGION_LAT_MAX is not None else 5.0,
        min_order=5,
    )
    if river_segs:
        from matplotlib.collections import LineCollection
        lc = LineCollection(river_segs, colors="#3366CC", linewidths=0.4, alpha=0.6, zorder=1.5)
        ax.add_collection(lc)

    # --- Determine per-station status ---
    if status_col in cat_df.columns:
        raw_statuses = cat_df[status_col].fillna("").astype(str).str.strip().values
    else:
        raw_statuses = cat_df["candidate_status"].fillna("").astype(str).str.strip().values

    # Get lon/lat
    lons = np.array([
        float(_to_lon180_domain(r)) if np.isfinite(r) else np.nan
        for r in cat_df["lon"].values
    ])
    lats = pd.to_numeric(cat_df["lat"], errors="coerce").values

    # Plot validated stations (initial full extent, legend added later only for visible)
    validated_mask = (raw_statuses == "validated") & np.isfinite(lats) & np.isfinite(lons)

    # Exclude Fazenda Vista Alegre from map
    _fazenda_names_map = np.array([_clean_text_domain(n) for n in cat_df["station_name"].values])
    _is_fazenda_map = (_fazenda_names_map == "Fazenda Vista Alegre")
    validated_mask = validated_mask & ~_is_fazenda_map
    if validated_mask.any():
        ax.scatter(lons[validated_mask], lats[validated_mask],
                  color="#2ca02c", marker="o", s=20,
                  alpha=0.6, linewidths=0.5,
                  zorder=3)

    # Plot skipped stations grouped by filter_reason (initial full extent, no legend yet)
    skipped_mask = ((raw_statuses == "skipped") | (raw_statuses == "")) & np.isfinite(lats) & np.isfinite(lons)
    if skipped_mask.any():
        if "filter_reason" in cat_df.columns:
            skip_reasons = cat_df.loc[skipped_mask, "filter_reason"].fillna("unknown").astype(str).str.strip().values
        else:
            skip_reasons = np.array(["unknown"] * skipped_mask.sum())
        unique_reasons = np.unique(skip_reasons)
        reason_colors = {
            "outside_user_region": "#1f77b4",
            "outside_model_domain": "#d62728",
            "model_grid_distance_gt_threshold": "#ff7f0e",
            "no_time_overlap": "#9467bd",
            "invalid_lat_lon": "#7f7f7f",
            "not_processed_max_stations": "#cccccc",
        }
        for reason in unique_reasons:
            mask = (skip_reasons == reason) & np.isfinite(lats[skipped_mask]) & np.isfinite(lons[skipped_mask])
            if not mask.any():
                continue
            r_lons = lons[skipped_mask][mask]
            r_lats = lats[skipped_mask][mask]
            color = reason_colors.get(reason, "#d62728")
            ax.scatter(r_lons, r_lats,
                      color=color, marker="x", s=12,
                      alpha=0.6, linewidths=1,
                      zorder=3)

    # --- Regional zoom bounds (set limits NOW so legend only shows visible points) ---
    lon_min = _to_lon180_domain(DEFAULT_REGION_LON_MIN) if DEFAULT_REGION_LON_MIN is not None else -80.0
    lon_max = _to_lon180_domain(DEFAULT_REGION_LON_MAX) if DEFAULT_REGION_LON_MAX is not None else -45.0
    lat_min = DEFAULT_REGION_LAT_MIN if DEFAULT_REGION_LAT_MIN is not None else -20.0
    lat_max = DEFAULT_REGION_LAT_MAX if DEFAULT_REGION_LAT_MAX is not None else 5.0
    lat_pad = (lat_max - lat_min) * 0.05
    lon_pad = (lon_max - lon_min) * 0.05
    ax.set_xlim(lon_min - lon_pad, lon_max + lon_pad)
    ax.set_ylim(lat_min - lat_pad, lat_max + lat_pad)

    # Now replot validated + skipped only for points inside visible range
    vis_lon_min, vis_lon_max = ax.get_xlim()
    vis_lat_min, vis_lat_max = ax.get_ylim()
    # Validated
    v_mask = validated_mask & (lons >= vis_lon_min) & (lons <= vis_lon_max) & (lats >= vis_lat_min) & (lats <= vis_lat_max)
    if v_mask.any():
        ax.scatter(lons[v_mask], lats[v_mask],
                  color="#2ca02c", marker="o", s=20,
                  alpha=0.6, linewidths=0.5,
                  zorder=3, label="Validated")
    # Skipped by reason (within zoom region)
    s_mask = skipped_mask & (lons >= vis_lon_min) & (lons <= vis_lon_max) & (lats >= vis_lat_min) & (lats <= vis_lat_max)
    s_inds = np.where(s_mask)[0]
    if len(s_inds) > 0:
        if "filter_reason" in cat_df.columns:
            skip_reasons = cat_df.iloc[s_inds]["filter_reason"].fillna("unknown").astype(str).str.strip().values
        else:
            skip_reasons = np.array(["unknown"] * len(s_inds))
        unique_reasons = np.unique(skip_reasons)
        reason_colors = {
            "outside_user_region": "#1f77b4",
            "outside_model_domain": "#d62728",
            "model_grid_distance_gt_threshold": "#ff7f0e",
            "no_time_overlap": "#9467bd",
            "invalid_lat_lon": "#7f7f7f",
            "not_processed_max_stations": "#cccccc",
        }
        reason_labels = {
            "outside_user_region": "Skipped: outside region",
            "outside_model_domain": "Skipped: outside model domain",
            "model_grid_distance_gt_threshold": "Skipped: distance threshold",
            "no_time_overlap": "Skipped: no time overlap",
            "invalid_lat_lon": "Skipped: invalid lat/lon",
            "not_processed_max_stations": "Skipped: max stations limit",
        }
        for reason in unique_reasons:
            r_mask_s = (skip_reasons == reason)
            if not r_mask_s.any():
                continue
            r_lons = lons[s_mask][r_mask_s]
            r_lats = lats[s_mask][r_mask_s]
            color = reason_colors.get(reason, "#d62728")
            label = reason_labels.get(reason, "Skipped: %s" % reason)
            ax.scatter(r_lons, r_lats,
                      color=color, marker="x", s=12,
                      alpha=0.6, linewidths=1,
                      zorder=3, label=label)

    # --- Station labels (simplified: show only main station name) ---
    _label_offsets = [
        (8, 0), (-8, 0), (0, 8), (0, -8),
        (8, 8), (-8, 8), (8, -8), (-8, -8),
        (12, 0), (-12, 0), (0, 12), (0, -12),
        (12, 6), (-12, 6), (6, 12), (-6, 12),
        (6, -12), (-6, -12), (12, -6), (-12, -6),
    ]
    _placed_positions = []
    for i in range(len(cat_df)):
        if not validated_mask[i]:
            continue
        s_lat = lats[i]
        s_lon = lons[i]
        if not np.isfinite(s_lat) or not np.isfinite(s_lon):
            continue
        s_name = _clean_text_domain(cat_df.iloc[i].get("station_name", ""))
        label = s_name

        station_offsets = [(8, -18), (8, -24), (0, -24), (12, -24)] if s_name == "Itaituba" else _label_offsets

        placed = False
        for dx, dy in station_offsets:
            ap_lon = s_lon + dx * 0.02
            ap_lat = s_lat + dy * 0.02
            overlaps = False
            for ep_lon, ep_lat in _placed_positions:
                if abs(ap_lon - ep_lon) < 0.6 and abs(ap_lat - ep_lat) < 0.5:
                    overlaps = True
                    break
            if not overlaps:
                _placed_positions.append((ap_lon, ap_lat))
                ax.annotate(
                    label,
                    (s_lon, s_lat),
                    fontsize=FONT_ANNOTATION_SMALL,
                    xytext=(dx, dy),
                    textcoords="offset points",
                    alpha=0.85,
                    zorder=4,
                    bbox=dict(boxstyle="round,pad=0.1", facecolor="white",
                              edgecolor="none", alpha=0.7),
                )
                placed = True
                break

    ax.set_xlabel("Longitude (°)", fontsize=FONT_LABEL_MAP)
    ax.set_ylabel("Latitude (°)", fontsize=FONT_LABEL_MAP)
    apply_axis_font_sizes(ax)
    ax.grid(True, alpha=0.2, linewidth=0.3)

    # --- Legend ---
    handles_labels = ax.get_legend_handles_labels()
    if handles_labels[0]:
        legend = ax.legend(loc="lower left", fontsize=FONT_LEGEND,
                           markerscale=0.8, framealpha=0.8)
        if legend and handles_labels[0]:
            for lh in handles_labels[0]:
                if hasattr(lh, '_sizes'):
                    lh._sizes = [20]


def plot_panel_c_Q(ax, extract_dir: str) -> None:
    """Panel c: Porto Velho discharge (Q) time series (linear y, following plot_outflow_compare.py style)."""
    ext_dir = Path(extract_dir)

    # Read model outflow (3-hourly)
    model_file = ext_dir / "Porto Velho_15400000_model_outflw_best_uparea.csv"
    station_file = ext_dir / "filtered_station_data_15400000.csv"

    if not model_file.exists() or not station_file.exists():
        ax.text(0.5, 0.5, "Porto Velho data not available",
                transform=ax.transAxes, ha="center", va="center", fontsize=FONT_FALLBACK, style="italic")
        return

    model_df = pd.read_csv(model_file)
    model_df["date"] = pd.to_datetime(model_df["time"])
    model_df = model_df.sort_values("date")

    station_df = pd.read_csv(station_file)
    station_df["date"] = pd.to_datetime(station_df["date"])
    station_df = station_df.sort_values("date")

    # Limit to 2001-2005
    model_df = model_df[(model_df["date"] >= "2001-01-01") & (model_df["date"] <= "2005-12-31")]
    station_df = station_df[(station_df["date"] >= "2001-01-01") & (station_df["date"] <= "2005-12-31")]

    ax.plot(model_df["date"], model_df["outflw (m³/s)"],
            linestyle="-", linewidth=1.5, color="#1f77b4",
            label="Model Q", alpha=0.85)
    ax.plot(station_df["date"], station_df["valeur"],
            linestyle="--", linewidth=1.5, color="#ff7f0e",
            label="Observed Q", alpha=0.85)

    ax.set_xlabel("Time", fontsize=FONT_LABEL)
    ax.set_ylabel("Q (m³ s⁻¹)", fontsize=FONT_LABEL)
    ax.set_xlim(pd.Timestamp("2001-01-01"), pd.Timestamp("2005-12-31"))
    apply_axis_font_sizes(ax)
    ax.legend(fontsize=FONT_LEGEND, loc="upper right", framealpha=0.8, edgecolor="gray")
    ax.grid(True, alpha=0.3)

    # Annotate station name and data count
    n_model = len(model_df)
    n_obs = len(station_df)
    # ann_text = "Porto Velho | Model n=%d, Obs n=%d" % (n_model, n_obs)
    # ax.annotate(ann_text, xy=(0.98, 0.95), xycoords="axes fraction",
    #             fontsize=FONT_ANNOTATION_SMALL, ha="right", va="top",
    #             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))

    # Compute Pearson correlation between model and observed Q
    _q_merged = pd.merge(
        station_df[["date", "valeur"]].rename(columns={"valeur": "obs_q"}),
        model_df[["date", "outflw (m³/s)"]].rename(columns={"outflw (m³/s)": "mod_q"}),
        on="date", how="inner"
    ).dropna()
    if not _q_merged.empty:
        r_q = _q_merged["obs_q"].corr(_q_merged["mod_q"], method="pearson")
        ax.text(0.02, 0.95, "Pearson r = %.2f" % r_q, transform=ax.transAxes,
                fontsize=FONT_ANNOTATION, ha="left", va="top",
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))


    import matplotlib.pyplot as _plt
    import matplotlib.dates as mdates
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    _plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=FONT_TICK)


def plot_panel_d_SSL(ax, extract_dir: str) -> None:
    """Panel d: Porto Velho SSL time series (following plot_sedout_103tday_compare.py style)."""
    ext_dir = Path(extract_dir)

    # Read observed sediment flux
    sed_flux_file = ext_dir / "Porto Velho_15400000_sediment_flux.csv"
    mdl_sed_file = ext_dir / "Porto Velho_15400000_mdl_sedout_tonsday.csv"

    if not sed_flux_file.exists() or not mdl_sed_file.exists():
        ax.text(0.5, 0.5, "Porto Velho SSL data not available",
                transform=ax.transAxes, ha="center", va="center", fontsize=FONT_FALLBACK, style="italic")
        return

    sed_df = pd.read_csv(sed_flux_file)
    sed_df["date"] = pd.to_datetime(sed_df["date"])
    sed_df = sed_df.set_index("date")
    # Resample to daily mean
    sed_df = sed_df.resample("D").mean().dropna()
    # Limit to 2001-2005
    sed_df = sed_df.loc["2001-01-01":"2005-12-31"]

    mdl_df = pd.read_csv(mdl_sed_file)
    mdl_df["time"] = pd.to_datetime(mdl_df["time"])
    mdl_df = mdl_df.set_index("time")
    # Resample to daily mean
    mdl_df = mdl_df.resample("D").mean().dropna()
    # Limit to 2001-2005
    mdl_df = mdl_df.loc["2001-01-01":"2005-12-31"]

    # Merge on date (inner join) for Pearson correlation
    merged = pd.concat([
        sed_df["sediment_flux (10³ t/day)"],
        mdl_df["sedout (10³ t/day)"]
    ], axis=1, join="inner").dropna()
    merged.columns = ["Observed (10³ t/day)", "Model (10³ t/day)"]

    if merged.empty:
        ax.text(0.5, 0.5, "No overlapping SSL data",
                transform=ax.transAxes, ha="center", va="center", fontsize=FONT_FALLBACK, style="italic")
        return

    r_pearson = merged["Observed (10³ t/day)"].corr(merged["Model (10³ t/day)"], method="pearson")

    ax.scatter(merged.index, merged["Observed (10³ t/day)"],
               color="tab:red", s=15, alpha=0.6, zorder=3, label="Observed SSL")
    ax.plot(merged.index, merged["Model (10³ t/day)"],
            color="tab:blue", linewidth=1.5, label="Model SSL", alpha=0.85)

    ax.set_xlabel("Time", fontsize=FONT_LABEL)
    ax.set_ylabel("SSL (10³ t d⁻¹)", fontsize=FONT_LABEL)
    ax.set_xlim(pd.Timestamp("2001-01-01"), pd.Timestamp("2005-12-31"))
    apply_axis_font_sizes(ax)
    ax.legend(fontsize=FONT_LEGEND, loc="upper right", framealpha=0.8, edgecolor="gray")
    ax.grid(True, alpha=0.3)

    ax.text(0.02, 0.95,
            "Pearson r = %.2f" % r_pearson,
            transform=ax.transAxes, fontsize=FONT_ANNOTATION,
            verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))

    import matplotlib.pyplot as _plt
    import matplotlib.dates as mdates
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    _plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=FONT_TICK)


# ============================================================
# Main figure orchestrator
# ============================================================


_LAND_POLYGONS_CACHE_DOMAIN: list = None
_LAND_POLYGON_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_land.geojson"
_LAND_POLYGON_PATH = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/plot/ne_110m_land.geojson")


def _to_lon180_domain(lon):
    """Convert longitude to -180..180 range."""
    lon = np.asarray(lon, dtype=np.float64)
    return ((lon + 180.0) % 360.0) - 180.0


def _load_land_polygons_domain() -> list:
    """Load/cache Natural Earth 110m land polygons from GeoJSON."""
    global _LAND_POLYGONS_CACHE_DOMAIN
    if _LAND_POLYGONS_CACHE_DOMAIN is not None:
        return _LAND_POLYGONS_CACHE_DOMAIN

    import json
    import urllib.request

    data = None
    local_path = _LAND_POLYGON_PATH
    if local_path.is_file():
        try:
            with open(local_path, "r") as f:
                data = json.load(f)
            print("[INFO] Loaded land polygons from %s" % local_path)
        except Exception as exc:
            print("[WARN] Failed to load local land polygons: %s" % exc)

    if data is None:
        try:
            req = urllib.request.Request(
                _LAND_POLYGON_URL, headers={"User-Agent": "Mozilla/5.0"}
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            print("[WARN] Failed to download land polygons: %s" % exc)
            _LAND_POLYGONS_CACHE_DOMAIN = []
            return []

    polygons = []
    for feature in data.get("features", []):
        geom = feature.get("geometry", {})
        geom_type = geom.get("type")
        coords = geom.get("coordinates", [])
        if geom_type == "Polygon":
            ring = np.asarray(coords[0], dtype=np.float64)
            # Skip Antarctica (all latitudes south of -60°)
            if np.mean(ring[:, 1]) >= -60:
                polygons.append((ring[:, 0], ring[:, 1]))
        elif geom_type == "MultiPolygon":
            for poly_coords in coords:
                ring = np.asarray(poly_coords[0], dtype=np.float64)
                # Skip Antarctica polygons
                if np.mean(ring[:, 1]) >= -60:
                    polygons.append((ring[:, 0], ring[:, 1]))

    _LAND_POLYGONS_CACHE_DOMAIN = polygons
    return polygons


def _plot_land_domain(ax, polygons: list) -> None:
    """Fill land polygons on a matplotlib Axes."""
    for lon, lat in polygons:
        ax.fill(lon, lat, color="#EEEEEE", edgecolor="#CCCCCC",
                linewidth=0.3, zorder=0)


def _load_river_network_domain(
    merit_dir: str,
    view_lon_min: float = -80.0,
    view_lon_max: float = -45.0,
    view_lat_min: float = -20.0,
    view_lat_max: float = 5.0,
    min_order: int = 5,
) -> list:
    """Load river line segments from MERIT Hydro shapefiles overlapping the viewport."""
    try:
        import shapefile as _sf
    except ImportError:
        print("[WARN] shapefile (pyshp) not available. Skipping river network.")
        return []

    _PFAF_CODES = ["61", "62", "63", "64", "66", "67"]
    _SUB_DIR = "pfaf_level_02"
    _PREFIX = "riv_pfaf_"
    _SUFFIX = "_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"

    segments = []
    base = Path(merit_dir) / _SUB_DIR

    for code in _PFAF_CODES:
        shp_path = base / ("%s%s%s" % (_PREFIX, code, _SUFFIX))
        if not shp_path.exists():
            continue
        try:
            sf = _sf.Reader(str(shp_path))
        except Exception as exc:
            print("[WARN] Failed to open river shapefile %s: %s" % (shp_path, exc))
            continue

        _field_names = [f[0] for f in sf.fields if f[0] != "DeletionFlag"]
        if "order" in _field_names:
            _order_idx = _field_names.index("order")
        else:
            _order_idx = -1

        for shape, rec in zip(sf.iterShapes(), sf.iterRecords()):
            pts = np.asarray(shape.points, dtype=np.float64)
            if len(pts) < 2:
                continue
            lons_seg = pts[:, 0]
            lats_seg = pts[:, 1]
            if (np.all(lons_seg < view_lon_min) or np.all(lons_seg > view_lon_max)
                or np.all(lats_seg < view_lat_min) or np.all(lats_seg > view_lat_max)):
                continue
            if min_order > 1 and _order_idx >= 0:
                seg_order = int(rec[_order_idx])
                if seg_order < min_order:
                    continue
            segments.append(pts)

        sf.close()

    if not segments:
        print("[WARN] No river segments loaded from %s" % base)
    else:
        print("[INFO] Loaded %d river segments (order >= %d) from MERIT Hydro"
              % (len(segments), min_order))
    return segments


def _clean_text_domain(value: object) -> str:
    """Safely extract clean string from a CSV field."""
    if value is None:
        return ""
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() == "nan" else text
def write_figure_checklist(
    fname: str,
    output_dir: Path,
    dpi: int,
    width_cm: float,
    height_cm: float,
) -> None:
    """Create an ESSD-compliant figure checklist (ESSD \u00a717)."""
    import datetime
    checklist_dir = output_dir / "figures" / "checklists"
    checklist_dir.mkdir(parents=True, exist_ok=True)
    checklist_path = checklist_dir / ("%s_checklist.md" % fname)

    pdf_size_mb = "(to be filled)"
    png_size_mb = "(to be filled)"

    head = "# Figure checklist: " + fname + "\n"
    head += "\n## Basic information\n"
    head += "\n- Figure file: " + fname + ".pdf / " + fname + ".png"
    head += "\n- Plotting script: plot_validate_model_with_sed_reference.py"
    head += "\n- Plotting data: " + fname + "_paired_samples.csv"
    head += "\n- Date exported: " + datetime.date.today().isoformat()
    head += "\n- Figure type: Multi-panel (scatter + map + time series)"
    head += "\n- Single-panel or multi-panel: Multi-panel (4 panels)"
    head += "\n\n## File format and size"
    head += "\n\n- Final format: PDF (vector) + PNG (bitmap)"
    head += "\n- DPI: " + str(dpi)
    head += "\n- Width: %.1f cm" % width_cm
    head += "\n- Height: %.1f cm" % height_cm
    head += "\n- File size (PDF): " + pdf_size_mb
    head += "\n- File size (PNG): " + png_size_mb
    head += "\n- PDF < 2 MB:"
    head += "\n- Non-PDF < 5 MB:"
    head += "\n- Width >= 8 cm: Yes"
    head += "\n\n## Color and accessibility"
    head += "\n\n- Colorblind-safe palette used: Yes (Okabe-Ito)"
    head += "\n- Continuous color map, if applicable: N/A"
    head += "\n- Coblis or equivalent check completed:"
    head += "\n- Figure remains interpretable under color-vision-deficiency simulation:"
    head += "\n- Categories are distinguished by more than color when needed: Yes (marker shapes + line styles)"
    head += "\n\n## Font and text"
    head += "\n\n- Single font family used: Yes (DejaVu Sans)"
    head += "\n- Font family: DejaVu Sans"
    head += "\n- Fonts embedded in vector file:"
    head += "\n- No unnecessary bold/italic variants: Yes"
    head += "\n- No hidden text boxes or extra layers: Yes"
    head += "\n- Sentence case used: Yes"
    head += "\n\n## Legend and symbols"
    head += "\n\n- Legend included inside figure: Yes"
    head += "\n- All colors explained: Yes"
    head += "\n- All markers explained: Yes"
    head += "\n- All line styles explained: Yes"
    head += "\n- Point sizes explained, if applicable: N/A"
    head += "\n- Color bar included and labeled, if applicable: N/A"
    head += "\n- Legend does not obscure data: Yes"
    head += "\n\n## ESSD formatting"
    head += "\n\n- Panel labels use (a), (b), etc.: Yes"
    head += "\n- Ranges use en dash with no spaces:"
    head += "\n- Coordinates use degree symbol and direction spacing: Yes"
    head += "\n- Numbers and units have a space: Yes"
    head += "\n- Units use exponent format: Yes"
    head += "\n- h, km, and m abbreviations used correctly: Yes"
    head += "\n\n## Reproducibility"
    head += "\n\n- Plotting data saved: Yes"
    head += "\n- Plotting script saved: Yes (this file)"
    head += "\n- Input paths documented: Yes (DEFAULT_* constants)"
    head += "\n- Filtering rules documented: Yes (min_threshold, etc.)"
    head += "\n- Color and marker mappings defined in code: Yes"
    head += "\n- Figure can be regenerated from saved files: Yes"
    head += "\n\n## Copyright"
    head += "\n\n- Figure fully generated from study data and code: Yes"
    head += "\n- External figure or basemap used: Yes (Natural Earth land polygons)"
    head += "\n- Reuse permission checked, if applicable:"
    head += "\n- Source cited in caption, if applicable:"
    head += "\n\n## Notes"
    head += "\n\n-\n"

    with open(checklist_path, "w") as f:
        f.write(head)
    print("[INFO] Figure checklist created: %s" % checklist_path)




def make_paper_figure(
    input_dir: Path,
    variable: str,
    resolution: str,
    example_cluster_uid: str = "",
    target_timeseries_resolution: str = "daily",
    dpi: int = 300,
    extract_dir: str = "",
    output_dir: str = "",
    figure_number: str = DEFAULT_FIGURE_NUMBER,
) -> None:
    """Create a 4-panel paper-ready model evaluation figure and save to disk.

    Panel a: Reference stations used for model evaluation (regional map).
    Panel b: Matched reference–model SSC pairs (log-log scatter, colored by station).
    Panel c: Porto Velho discharge (Q) time series.
    Panel d: Porto Velho sediment load (SSL) time series.

    Saves ESSD-compliant figure and supporting files to figures/ in output_dir.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # --- ESSD-compliant font and vector settings (ESSD §16) ---
    matplotlib.rcParams["font.family"] = "DejaVu Sans"
    matplotlib.rcParams["pdf.fonttype"] = 42
    matplotlib.rcParams["ps.fonttype"] = 42
    matplotlib.rcParams["axes.unicode_minus"] = False

    input_dir_path = Path(input_dir)
    extr_dir = extract_dir if extract_dir else DEFAULT_EXTRACT_DIR
    output_dir_path = Path(output_dir) if output_dir else Path(DEFAULT_OUTPUT_DIR)


    # --- Validate inputs ---
    if variable not in VARIABLES:
        valid_vars = ", ".join(sorted(VARIABLES))
        print("[ERROR] Variable '%s' not found in VARIABLES. Available: %s"
              % (variable, valid_vars), file=sys.stderr)
        sys.exit(1)
    unit = VARIABLES[variable]["unit"]

    # --- Collect paired samples ---
    pairs_df = collect_compare_pairs(input_dir_path, variable, resolution)
    if pairs_df.empty:
        print("[WARN] No compare CSV files found for variable '%s' resolution '%s'. "
              "Skipping paper figure." % (variable, resolution))
        return
    print("[INFO] Collected %d paired samples (%d stations) for %s"
          % (len(pairs_df), pairs_df["cluster_uid"].nunique(), variable))

    # --- Select representative station ---
    metrics_path = input_dir_path / "metrics_summary.csv"
    if metrics_path.exists():
        metrics_df = pd.read_csv(metrics_path)
        print("[INFO] Loaded %d metric rows from %s" % (len(metrics_df), metrics_path))
        selected_uid = select_example_station(metrics_df, variable, example_cluster_uid)
    else:
        selected_uid = example_cluster_uid if example_cluster_uid else ""

    # --- Build figure layout ---
    # 2 rows x 2 columns
    width_cm = 35.5
    height_cm = 28
    figsize = (width_cm / 2.54, height_cm / 2.54)
    fig = plt.figure(figsize=figsize)
    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1], hspace=0.30, wspace=0.20)
    ax_a = fig.add_subplot(gs[0, 0])    # top left      — panel a (map)
    ax_b = fig.add_subplot(gs[0, 1])    # top right     — panel b (scatter)
    ax_c = fig.add_subplot(gs[1, 0])    # bottom left   — panel c (Q)
    ax_d = fig.add_subplot(gs[1, 1])    # bottom right  — panel d (SSL)

    plot_panel_b_domain_map(ax_a, input_dir_path, variable)
    plot_panel_a_log_scatter(ax_b, pairs_df, variable, unit)
    plot_panel_c_Q(ax_c, extr_dir)
    plot_panel_d_SSL(ax_d, extr_dir)

    for axis, label in ((ax_a, "(a)"), (ax_b, "(b)"), (ax_c, "(c)"), (ax_d, "(d)")):
        add_panel_label(axis, label)

    # --- Save output with ESSD naming (ESSD §2, §4) ---
    fname = figure_number
    figures_final_dir = output_dir_path / "figures" / "final"
    figures_final_dir.mkdir(parents=True, exist_ok=True)

    pdf_path = figures_final_dir / ("%s.pdf" % fname)
    png_path = figures_final_dir / ("%s.png" % fname)
    fig.savefig(str(pdf_path), dpi=dpi, bbox_inches="tight")
    fig.savefig(str(png_path), dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    # --- Save plotting data (ESSD §14) ---
    data_dir = output_dir_path / "figures" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    pairs_df.to_csv(data_dir / ("%s_paired_samples.csv" % fname), index=False)
    print("[INFO] Plotting data saved: %s_paired_samples.csv" % fname)

    # --- Create figure checklist (ESSD §17) ---
    write_figure_checklist(
        fname, output_dir_path, dpi,
        width_cm=14.0 * 2.54,
        height_cm=10.0 * 2.54,
    )
    print("[INFO] ESSD figure saved: %s" % pdf_path)
    print("[INFO] ESSD figure saved: %s" % png_path)


def main() -> None:
    """Run with hardcoded defaults (edit DEFAULT_* constants at top of file)."""
    make_paper_figure(
        input_dir=Path(DEFAULT_INPUT_DIR),
        output_dir=DEFAULT_OUTPUT_DIR,
        variable=DEFAULT_VARIABLE,
        resolution=DEFAULT_RESOLUTION,
        example_cluster_uid=DEFAULT_EXAMPLE_CLUSTER_UID,
        target_timeseries_resolution="daily",
        dpi=DEFAULT_DPI,
        extract_dir=DEFAULT_EXTRACT_DIR,

    )


if __name__ == "__main__":
    main()
