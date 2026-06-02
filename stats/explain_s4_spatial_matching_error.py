#!/usr/bin/env python3
"""Explain S4 station-level spatial basin matching diagnostics.

This script reads only S4 intermediate outputs:

  - s4_upstream_basins.csv
      Station-level basin tracing result.
  - s4_reported_area_check.csv
      Optional S4 reported-area consistency check.

No command-line arguments are required. Edit the USER CONFIGURATION block if
paths differ, then run:

    python3 tools/explain_s4_spatial_matching_error.py

Main questions answered:
  - At S4, how many station-level rows are resolved/unresolved?
  - What are the main unresolved reasons: no_match, large_offset,
    area_mismatch, geometry_inconsistent?
  - Among rows with positive source-reported drainage area, how successful is
    basin matching?
  - Are reported_area and area_error present at S4 before later release steps?
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover
    plt = None


# =============================================================================
# USER CONFIGURATION
# =============================================================================
OUTPUT_R_ROOT = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r"
)

S4_UPSTREAM_CSV = OUTPUT_R_ROOT / "scripts_basin_test/output/s4_upstream_basins.csv"
S4_REPORTED_AREA_CHECK_CSV = OUTPUT_R_ROOT / "scripts_basin_test/output/s4_reported_area_check.csv"

OUTPUT_DIR = OUTPUT_R_ROOT / "scripts_basin_test/output/s4_spatial_match_error"

MAKE_FIGURES = True
TOP_N_MANUAL_REVIEW = 100

# Exclude remote/reach-scale sources from main S4 denominator, while still
# writing them to s4_spatial_match_all_rows.csv.
EXCLUDE_REMOTE_REACH_SCALE_FROM_MAIN_STATS = True

REMOTE_REACH_SOURCE_PATTERNS = [
    "riversed",
    "river_sed",
    "gsed",
    "dethier",
    "deither",
    "source_remote_sensing_no_basin_match",
]

AREA_SUPPORTED_QUALITIES = {"area_matched", "area_approximate"}


# =============================================================================
# Small utilities
# =============================================================================
def _clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def _clean_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _bool_series(series: pd.Series) -> pd.Series:
    text = _clean_series(series).str.lower()
    return text.isin({"1", "true", "yes", "y", "t"})


def _to_numeric(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


def _first_existing(columns: Sequence[str], choices: Sequence[str]) -> Optional[str]:
    for col in choices:
        if col in columns:
            return col
    return None


def _coalesce_columns(df: pd.DataFrame, choices: Sequence[str], default=np.nan) -> pd.Series:
    out = pd.Series(default, index=df.index)
    for col in choices:
        if col not in df.columns:
            continue
        values = df[col]
        if pd.api.types.is_numeric_dtype(values):
            mask = out.isna() & values.notna()
        else:
            text = _clean_series(values)
            mask = out.isna() & text.ne("")
            values = text
        out.loc[mask] = values.loc[mask]
    return out


def _coalesce_text_columns(df: pd.DataFrame, choices: Sequence[str], default: str = "") -> pd.Series:
    out = pd.Series(default, index=df.index, dtype=object)
    for col in choices:
        if col not in df.columns:
            continue
        text = _clean_series(df[col])
        mask = out.astype(str).str.strip().eq("") & text.ne("")
        out.loc[mask] = text.loc[mask]
    return out


def _format_number(value: float, digits: int = 2) -> str:
    return "NA" if pd.isna(value) else f"{float(value):.{digits}f}"


def numeric_summary(series: pd.Series) -> Dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(s) == 0:
        return {"n": 0, "median": np.nan, "p75": np.nan, "p90": np.nan, "p95": np.nan, "max": np.nan}
    return {
        "n": int(len(s)),
        "median": float(s.quantile(0.50)),
        "p75": float(s.quantile(0.75)),
        "p90": float(s.quantile(0.90)),
        "p95": float(s.quantile(0.95)),
        "max": float(s.max()),
    }


def _format_numeric_summary(summary: Dict[str, float], unit: str = "", digits: int = 2) -> str:
    suffix = f" {unit}" if unit else ""
    return (
        f"n={int(summary['n'])}, "
        f"median={_format_number(summary['median'], digits)}{suffix}, "
        f"p75={_format_number(summary['p75'], digits)}{suffix}, "
        f"p90={_format_number(summary['p90'], digits)}{suffix}, "
        f"p95={_format_number(summary['p95'], digits)}{suffix}, "
        f"max={_format_number(summary['max'], digits)}{suffix}"
    )


def summarize_counts(df: pd.DataFrame, columns: Sequence[str], count_name: str = "row_count") -> pd.DataFrame:
    work = df.copy()
    for col in columns:
        if col not in work.columns:
            work[col] = "NA"
        work[col] = _clean_series(work[col]).replace("", "NA")
    table = work.groupby(list(columns), dropna=False).size().reset_index(name=count_name)
    total = float(table[count_name].sum())
    table["fraction"] = table[count_name] / total if total else 0.0
    table["percent"] = table["fraction"] * 100.0
    return table.sort_values(count_name, ascending=False, kind="stable")


# =============================================================================
# Source exclusion
# =============================================================================
def _source_text_for_filter(row: pd.Series) -> str:
    fields = ["source", "sources_used", "primary_source", "source_name", "data_source_name", "method"]
    parts = [_clean_text(row.get(f, "")) for f in fields]
    return " | ".join(p for p in parts if p).lower()


def _remote_source_label(text: str) -> Optional[str]:
    t = text.lower()
    if "riversed" in t or "river_sed" in t:
        return "RiverSed"
    if re.search(r"\bgsed\b", t) or "global suspended sediment dynamics" in t:
        return "GSED"
    if "dethier" in t or "deither" in t:
        return "Dethier"
    if "source_remote_sensing_no_basin_match" in t:
        return "source_remote_sensing_no_basin_match"
    return None


def _is_remote_reach_scale_row(row: pd.Series) -> bool:
    text = _source_text_for_filter(row)
    if not text:
        return False
    return any(pattern in text for pattern in REMOTE_REACH_SOURCE_PATTERNS)


def build_exclusion_summary(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    labels = []
    excluded = []
    for _, row in df.iterrows():
        text = _source_text_for_filter(row)
        label = _remote_source_label(text)
        is_excluded = bool(label is not None or _is_remote_reach_scale_row(row))
        labels.append(label if label is not None else ("remote_or_reach_scale" if is_excluded else "not_excluded"))
        excluded.append(is_excluded)

    mask = pd.Series(excluded, index=df.index, name="excluded_remote_reach_scale")
    label_series = pd.Series(labels, index=df.index, name="excluded_source_group")

    summary = (
        pd.DataFrame({"excluded_source_group": label_series, "excluded_remote_reach_scale": mask})
        .groupby(["excluded_source_group", "excluded_remote_reach_scale"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["excluded_remote_reach_scale", "row_count"], ascending=[False, False], kind="stable")
    )
    total = float(len(df))
    summary["percent_of_input"] = summary["row_count"] / total * 100.0 if total else 0.0
    return summary, mask


# =============================================================================
# S4 normalization and diagnostics
# =============================================================================
def normalize_s4_table(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    out = df.copy()
    out["_table_name"] = table_name
    out["_input_row_number"] = np.arange(len(out), dtype=int)

    if "station_id" not in out.columns:
        if "source_station_id" in out.columns:
            out["station_id"] = out["source_station_id"]
        elif "native_station_id" in out.columns:
            out["station_id"] = out["native_station_id"]
        elif "cluster_id" in out.columns:
            out["station_id"] = out["cluster_id"]
        else:
            out["station_id"] = out["_input_row_number"]

    if "lat" not in out.columns:
        lat_col = _first_existing(out.columns, ["latitude", "source_station_lat", "station_lat"])
        out["lat"] = out[lat_col] if lat_col else np.nan
    if "lon" not in out.columns:
        lon_col = _first_existing(out.columns, ["longitude", "source_station_lon", "station_lon"])
        out["lon"] = out[lon_col] if lon_col else np.nan

    out["match_quality"] = _coalesce_text_columns(out, ["match_quality", "basin_match_quality", "match_status"]).str.lower()
    out["basin_status"] = _coalesce_text_columns(out, ["basin_status", "status"]).str.lower()
    out["basin_flag"] = _coalesce_text_columns(out, ["basin_flag", "flag"]).str.lower()
    out["method"] = _coalesce_text_columns(out, ["method", "basin_method"])
    out["source"] = _coalesce_text_columns(out, ["source", "sources_used", "primary_source", "source_name", "data_source_name"])

    out["reported_area"] = _coalesce_columns(
        out,
        ["reported_area", "drainage_area_km2", "source_reported_area_km2", "reported_area_km2", "source_area_km2"],
    )
    out["distance_m"] = _coalesce_columns(out, ["distance_m", "basin_distance_m", "match_distance_m"])
    out["basin_id"] = _coalesce_columns(out, ["basin_id", "basin_comid", "COMID"])
    out["uparea_merit"] = _coalesce_columns(out, ["uparea_merit", "upstream_area_km2", "basin_area_km2", "basin_area"])
    out["area_error"] = _coalesce_columns(out, ["area_error", "basin_area_error"])

    for col in ["point_in_local", "point_in_basin"]:
        if col not in out.columns:
            alt = _first_existing(out.columns, [f"basin_{col}", f"{col}_catchment"])
            out[col] = out[alt] if alt else False
        out[col] = _bool_series(out[col])

    _to_numeric(out, ["lat", "lon", "reported_area", "distance_m", "basin_id", "uparea_merit", "area_error"])

    # Fill status / flag conservatively if one is missing.
    blank_status = out["basin_status"].eq("")
    if blank_status.any():
        out.loc[blank_status, "basin_status"] = np.where(
            out.loc[blank_status, "basin_flag"].eq("ok"), "resolved", "unresolved"
        )
    blank_flag = out["basin_flag"].eq("")
    if blank_flag.any():
        out.loc[blank_flag, "basin_flag"] = np.where(
            out.loc[blank_flag, "basin_status"].eq("resolved"), "ok", "no_match"
        )

    # If area_error is not present but both reported_area and MERIT area exist,
    # reconstruct log10(MERIT upstream area / source-reported area).
    reported = pd.to_numeric(out["reported_area"], errors="coerce")
    merit = pd.to_numeric(out["uparea_merit"], errors="coerce")
    area_error = pd.to_numeric(out["area_error"], errors="coerce")
    fill_area_error = area_error.isna() & reported.gt(0) & merit.gt(0)
    out.loc[fill_area_error, "area_error"] = np.log10(merit.loc[fill_area_error] / reported.loc[fill_area_error])

    out["has_reported_area"] = reported.notna() & np.isfinite(reported) & reported.gt(0)

    exclusion_summary, remote_mask = build_exclusion_summary(out)
    out["excluded_remote_reach_scale"] = remote_mask
    return out


def add_distance_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["distance_m"] = pd.to_numeric(out["distance_m"], errors="coerce")
    bins = [-np.inf, 100.0, 300.0, 1000.0, 5000.0, 120000.0, np.inf]
    labels = ["<=100 m", "100-300 m", "300-1000 m", "1-5 km", "5-120 km", ">120 km or invalid"]
    out["distance_bin"] = pd.cut(out["distance_m"], bins=bins, labels=labels)
    out["distance_bin"] = out["distance_bin"].astype("object").fillna("not_available")
    out["distance_km"] = out["distance_m"] / 1000.0
    return out


def add_area_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["area_error"] = pd.to_numeric(out["area_error"], errors="coerce")
    out["area_log10_error_abs"] = out["area_error"].abs()
    out["area_ratio_merit_to_reported"] = np.where(
        np.isfinite(out["area_error"]),
        np.power(10.0, out["area_error"]),
        np.nan,
    )
    ratio = out["area_ratio_merit_to_reported"]
    out["area_factor_difference"] = np.where(
        ratio.notna() & (ratio > 0),
        np.maximum(ratio, 1.0 / ratio),
        np.nan,
    )

    bins = [-np.inf, 0.1, 0.3, 0.5, 1.0, np.inf]
    labels = [
        "<=0.1 log10 units (~within 26%)",
        "0.1-0.3 log10 units (~1.26-2x)",
        "0.3-0.5 log10 units (~2-3.16x)",
        "0.5-1.0 log10 units (~3.16-10x)",
        ">1.0 log10 units (>10x)",
    ]
    out["area_error_bin"] = pd.cut(out["area_log10_error_abs"], bins=bins, labels=labels)
    out["area_error_bin"] = out["area_error_bin"].astype("object").fillna("not_available")
    return out


def classify_spatial_error(row: pd.Series) -> Tuple[str, str, int]:
    basin_status = _clean_text(row.get("basin_status", "")).lower()
    basin_flag = _clean_text(row.get("basin_flag", "")).lower()
    match_quality = _clean_text(row.get("match_quality", "")).lower()
    distance = row.get("distance_m", np.nan)
    point_in_local = bool(row.get("point_in_local", False))
    point_in_basin = bool(row.get("point_in_basin", False))

    if basin_status != "resolved":
        if basin_flag == "large_offset":
            return (
                "D_large_offset_unresolved",
                "Offset is beyond the acceptance threshold; the selected basin may be a neighboring river or wrong branch.",
                4,
            )
        if basin_flag == "area_mismatch":
            return (
                "D_area_mismatch_unresolved",
                "MERIT upstream area and source-reported drainage area strongly disagree.",
                4,
            )
        if basin_flag == "geometry_inconsistent":
            return (
                "D_geometry_inconsistent_unresolved",
                "Point-in-catchment or full-basin evidence is not strong enough under acceptance rules.",
                4,
            )
        return (
            "D_no_publishable_basin_match",
            "No safe basin assignment is available or matching failed.",
            4,
        )

    finite_distance = pd.notna(distance) and np.isfinite(distance)
    if finite_distance and distance <= 300.0:
        if match_quality in AREA_SUPPORTED_QUALITIES:
            return (
                "A_high_confidence_area_supported_close",
                "Resolved, close to the matched reach, and area evidence supports the match.",
                1,
            )
        return (
            "A_high_confidence_close_distance",
            "Resolved and within 300 m of the matched reach.",
            1,
        )

    if finite_distance and distance <= 1000.0 and match_quality in AREA_SUPPORTED_QUALITIES:
        return (
            "B_moderate_offset_area_supported",
            "Resolved with moderate offset and drainage-area evidence.",
            2,
        )

    if finite_distance and distance <= 1000.0 and point_in_local:
        return (
            "B_moderate_offset_local_geometry_supported",
            "Resolved with moderate offset and local catchment support.",
            2,
        )

    if finite_distance and distance <= 1000.0 and point_in_basin:
        return (
            "C_moderate_offset_full_basin_only",
            "Resolved with full-basin support but weaker local/area evidence.",
            3,
        )

    return (
        "C_resolved_but_weak_diagnostics",
        "Resolved, but available diagnostics are weak or incomplete.",
        3,
    )


def add_spatial_error_classes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    classified = out.apply(classify_spatial_error, axis=1, result_type="expand")
    classified.columns = ["spatial_error_class", "spatial_error_explanation", "spatial_error_severity"]
    return pd.concat([out, classified], axis=1)


# =============================================================================
# Plot helpers
# =============================================================================
def _save_bar(table: pd.DataFrame, label_col: str, value_col: str, title: str, out_png: Path) -> None:
    if plt is None or table.empty or label_col not in table.columns or value_col not in table.columns:
        return
    fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(table))))
    plot_df = table.sort_values(value_col, ascending=True)
    ax.barh(plot_df[label_col].astype(str), plot_df[value_col])
    ax.set_xlabel(value_col)
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def _save_hist(series: pd.Series, title: str, xlabel: str, out_png: Path, log_x: bool = False) -> None:
    if plt is None:
        return
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if log_x:
        s = s[s > 0]
    if len(s) == 0:
        return
    bins = np.logspace(np.log10(max(s.min(), 1e-6)), np.log10(s.max()), 50) if log_x else 50
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.hist(s, bins=bins)
    if log_x:
        ax.set_xscale("log")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("count")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def _save_scatter(df: pd.DataFrame, out_png: Path) -> None:
    if plt is None or df.empty:
        return
    if "distance_m" not in df.columns or "area_log10_error_abs" not in df.columns:
        return
    work = df[["distance_m", "area_log10_error_abs"]].replace([np.inf, -np.inf], np.nan).dropna()
    work = work[(work["distance_m"] > 0) & (work["area_log10_error_abs"] >= 0)]
    if work.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(work["distance_m"], work["area_log10_error_abs"], s=10, alpha=0.35)
    ax.set_xscale("log")
    ax.set_xlabel("point-to-reach distance (m, log scale)")
    ax.set_ylabel("abs(log10(MERIT area / reported area))")
    ax.set_title("S4 distance vs drainage-area mismatch")
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def _save_stacked_count_bar(df: pd.DataFrame, x_col: str, stack_col: str, title: str, out_png: Path) -> None:
    if plt is None or df.empty or x_col not in df.columns or stack_col not in df.columns:
        return
    pivot = pd.crosstab(_clean_series(df[x_col]).replace("", "NA"), _clean_series(df[stack_col]).replace("", "NA"))
    if pivot.empty:
        return
    fig, ax = plt.subplots(figsize=(max(8, 0.6 * len(pivot)), 5))
    pivot.plot(kind="bar", stacked=True, ax=ax)
    ax.set_xlabel(x_col)
    ax.set_ylabel("row count")
    ax.set_title(title)
    ax.legend(title=stack_col, bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


# =============================================================================
# Output writers
# =============================================================================
def _write_count_outputs(df: pd.DataFrame, out_dir: Path, prefix: str) -> None:
    summarize_counts(df, ["basin_status"]).to_csv(out_dir / f"{prefix}_status_counts.csv", index=False)
    summarize_counts(df, ["basin_flag"]).to_csv(out_dir / f"{prefix}_flag_counts.csv", index=False)
    summarize_counts(df, ["match_quality"]).to_csv(out_dir / f"{prefix}_match_quality_counts.csv", index=False)
    summarize_counts(df, ["spatial_error_class"]).to_csv(out_dir / f"{prefix}_spatial_error_class_counts.csv", index=False)
    summarize_counts(df, ["distance_bin", "basin_status"]).to_csv(out_dir / f"{prefix}_distance_bin_by_status.csv", index=False)
    summarize_counts(df, ["area_error_bin", "match_quality"]).to_csv(out_dir / f"{prefix}_area_error_bin_by_quality.csv", index=False)
    summarize_counts(df, ["has_reported_area", "basin_status"]).to_csv(out_dir / f"{prefix}_status_by_reported_area_presence.csv", index=False)
    summarize_counts(df, ["excluded_remote_reach_scale", "basin_status"]).to_csv(out_dir / f"{prefix}_status_by_remote_exclusion.csv", index=False)

    if "source" in df.columns:
        summarize_counts(df, ["source", "basin_status"]).to_csv(out_dir / f"{prefix}_status_by_source.csv", index=False)
    if "method" in df.columns:
        summarize_counts(df, ["method", "basin_status"]).to_csv(out_dir / f"{prefix}_status_by_method.csv", index=False)


def _write_reported_area_outputs(df: pd.DataFrame, out_dir: Path, prefix: str) -> pd.DataFrame:
    area_df = df[df["has_reported_area"]].copy()
    area_df.to_csv(out_dir / f"{prefix}_reported_area_rows.csv", index=False)
    summarize_counts(area_df, ["basin_status"]).to_csv(out_dir / f"{prefix}_reported_area_status_counts.csv", index=False)
    summarize_counts(area_df, ["basin_flag"]).to_csv(out_dir / f"{prefix}_reported_area_flag_counts.csv", index=False)
    summarize_counts(area_df, ["match_quality"]).to_csv(out_dir / f"{prefix}_reported_area_match_quality_counts.csv", index=False)
    summarize_counts(area_df, ["spatial_error_class"]).to_csv(out_dir / f"{prefix}_reported_area_spatial_error_class_counts.csv", index=False)
    summarize_counts(area_df, ["basin_status", "match_quality"]).to_csv(out_dir / f"{prefix}_reported_area_status_quality_counts.csv", index=False)
    summarize_counts(area_df, ["area_error_bin", "match_quality"]).to_csv(out_dir / f"{prefix}_reported_area_area_error_bin_quality_counts.csv", index=False)
    return area_df


def _write_manual_review_outputs(df: pd.DataFrame, out_dir: Path, prefix: str, top_n: int) -> None:
    review_cols = [
        "_table_name",
        "station_id",
        "cluster_id",
        "lat",
        "lon",
        "source",
        "method",
        "source_station_name",
        "source_river_name",
        "source_station_id",
        "basin_id",
        "distance_m",
        "reported_area",
        "uparea_merit",
        "area_error",
        "area_factor_difference",
        "match_quality",
        "point_in_local",
        "point_in_basin",
        "basin_status",
        "basin_flag",
        "has_reported_area",
        "spatial_error_class",
        "spatial_error_explanation",
        "spatial_error_severity",
    ]
    review_cols = [c for c in review_cols if c in df.columns]

    df.sort_values("distance_m", ascending=False).head(top_n)[review_cols].to_csv(
        out_dir / f"{prefix}_manual_review_top_large_offsets.csv", index=False
    )
    df[_clean_series(df["basin_flag"]).str.lower().eq("area_mismatch")].sort_values(
        "area_log10_error_abs", ascending=False
    ).head(top_n)[review_cols].to_csv(out_dir / f"{prefix}_manual_review_area_mismatch.csv", index=False)
    df[df["spatial_error_severity"] >= 4].sort_values(
        ["spatial_error_severity", "distance_m"], ascending=[False, False]
    ).head(top_n)[review_cols].to_csv(out_dir / f"{prefix}_manual_review_high_risk.csv", index=False)


def _write_figures(df: pd.DataFrame, area_df: pd.DataFrame, figs_dir: Path, prefix: str) -> None:
    if plt is None:
        return
    figs_dir.mkdir(parents=True, exist_ok=True)

    _save_bar(summarize_counts(df, ["basin_status"]), "basin_status", "row_count", f"{prefix}: basin status counts", figs_dir / f"{prefix}_basin_status_counts.png")
    _save_bar(summarize_counts(df, ["basin_flag"]), "basin_flag", "row_count", f"{prefix}: basin flag counts", figs_dir / f"{prefix}_basin_flag_counts.png")
    _save_bar(summarize_counts(df, ["match_quality"]), "match_quality", "row_count", f"{prefix}: match quality counts", figs_dir / f"{prefix}_match_quality_counts.png")
    _save_bar(summarize_counts(df, ["spatial_error_class"]), "spatial_error_class", "row_count", f"{prefix}: spatial error class counts", figs_dir / f"{prefix}_spatial_error_class_counts.png")
    _save_bar(summarize_counts(df, ["has_reported_area"]), "has_reported_area", "row_count", f"{prefix}: rows with/without reported_area", figs_dir / f"{prefix}_reported_area_presence_counts.png")
    _save_hist(df["distance_m"], f"{prefix}: point-to-reach distance distribution", "distance (m)", figs_dir / f"{prefix}_distance_hist_logx.png", log_x=True)
    _save_hist(df["area_log10_error_abs"], f"{prefix}: drainage-area mismatch distribution", "abs(log10 area ratio)", figs_dir / f"{prefix}_area_error_hist.png")
    _save_scatter(df, figs_dir / f"{prefix}_distance_vs_area_error.png")
    _save_stacked_count_bar(df, "has_reported_area", "basin_status", f"{prefix}: basin status by reported_area availability", figs_dir / f"{prefix}_status_by_reported_area_presence.png")
    _save_stacked_count_bar(df, "distance_bin", "basin_status", f"{prefix}: basin status by distance bin", figs_dir / f"{prefix}_status_by_distance_bin.png")

    if not area_df.empty:
        area_dir = figs_dir / "reported_area"
        area_dir.mkdir(exist_ok=True)
        _save_bar(summarize_counts(area_df, ["basin_status"]), "basin_status", "row_count", f"{prefix}: basin status among reported_area rows", area_dir / f"{prefix}_reported_area_basin_status_counts.png")
        _save_bar(summarize_counts(area_df, ["basin_flag"]), "basin_flag", "row_count", f"{prefix}: basin flag among reported_area rows", area_dir / f"{prefix}_reported_area_basin_flag_counts.png")
        _save_bar(summarize_counts(area_df, ["match_quality"]), "match_quality", "row_count", f"{prefix}: match quality among reported_area rows", area_dir / f"{prefix}_reported_area_match_quality_counts.png")
        _save_hist(area_df["distance_m"], f"{prefix}: distance among reported_area rows", "distance (m)", area_dir / f"{prefix}_reported_area_distance_hist_logx.png", log_x=True)
        _save_hist(area_df["area_log10_error_abs"], f"{prefix}: area mismatch among reported_area rows", "abs(log10 area ratio)", area_dir / f"{prefix}_reported_area_area_error_hist.png")
        _save_scatter(area_df, area_dir / f"{prefix}_reported_area_distance_vs_area_error.png")
        _save_stacked_count_bar(area_df, "area_error_bin", "basin_status", f"{prefix}: reported_area status by area error bin", area_dir / f"{prefix}_reported_area_status_by_area_error_bin.png")
        _save_stacked_count_bar(area_df, "match_quality", "basin_status", f"{prefix}: reported_area status by match quality", area_dir / f"{prefix}_reported_area_status_by_match_quality.png")


def write_summary_text(
    input_path: Path,
    df_all: pd.DataFrame,
    df_main: pd.DataFrame,
    exclusion_summary: pd.DataFrame,
    area_df: pd.DataFrame,
    out_path: Path,
    title: str,
) -> None:
    total_all = len(df_all)
    total_main = len(df_main)

    status_counts = summarize_counts(df_main, ["basin_status"])
    flag_counts = summarize_counts(df_main, ["basin_flag"])
    quality_counts = summarize_counts(df_main, ["match_quality"])
    class_counts = summarize_counts(df_main, ["spatial_error_class"])

    status = _clean_series(df_main["basin_status"]).str.lower()
    all_distance = numeric_summary(df_main["distance_m"])
    resolved_distance = numeric_summary(df_main[status.eq("resolved")]["distance_m"])
    unresolved_distance = numeric_summary(df_main[status.eq("unresolved")]["distance_m"])

    area_total = len(area_df)
    area_status = _clean_series(area_df["basin_status"]).str.lower() if area_total else pd.Series(dtype=str)
    area_flag = _clean_series(area_df["basin_flag"]).str.lower() if area_total else pd.Series(dtype=str)
    area_quality = _clean_series(area_df["match_quality"]).str.lower() if area_total else pd.Series(dtype=str)
    area_distance = numeric_summary(area_df["distance_m"]) if area_total else numeric_summary(pd.Series(dtype=float))
    area_abs = numeric_summary(area_df["area_log10_error_abs"]) if area_total else numeric_summary(pd.Series(dtype=float))

    def pct(n: int, denom: int) -> float:
        return 100.0 * float(n) / float(denom) if denom else 0.0

    lines: List[str] = []
    lines.append(title)
    lines.append("=" * len(title))
    lines.append("")
    lines.append(f"Input table: {input_path}")
    lines.append(f"All S4 rows: {total_all}")
    lines.append(f"Rows used in main statistics: {total_main}")
    lines.append(f"Remote/reach-scale rows excluded from main statistics: {int(df_all['excluded_remote_reach_scale'].sum())}")
    lines.append("")
    lines.append("Remote/reach-scale source groups")
    for row in exclusion_summary.itertuples(index=False):
        lines.append(
            f"  - {row.excluded_source_group} | excluded={row.excluded_remote_reach_scale}: "
            f"{int(row.row_count)} ({float(row.percent_of_input):.2f}% of all rows)"
        )
    lines.append("")
    lines.append("1. Basin status")
    for row in status_counts.itertuples(index=False):
        lines.append(f"  - {row.basin_status}: {int(row.row_count)} ({float(row.percent):.2f}%)")
    lines.append("")
    lines.append("2. Basin flag")
    for row in flag_counts.itertuples(index=False):
        lines.append(f"  - {row.basin_flag}: {int(row.row_count)} ({float(row.percent):.2f}%)")
    lines.append("")
    lines.append("3. Match quality")
    for row in quality_counts.itertuples(index=False):
        lines.append(f"  - {row.match_quality}: {int(row.row_count)} ({float(row.percent):.2f}%)")
    lines.append("")
    lines.append("4. Spatial error class")
    for row in class_counts.itertuples(index=False):
        lines.append(f"  - {row.spatial_error_class}: {int(row.row_count)} ({float(row.percent):.2f}%)")
    lines.append("")
    lines.append("5. Distance diagnostics")
    lines.append(f"  - all finite distances: {_format_numeric_summary(all_distance, 'm', 2)}")
    lines.append(f"  - resolved distances: {_format_numeric_summary(resolved_distance, 'm', 2)}")
    lines.append(f"  - unresolved distances: {_format_numeric_summary(unresolved_distance, 'm', 2)}")
    lines.append("")
    lines.append("6. Reported-area subset")
    if area_total == 0:
        lines.append("  - No rows have positive reported_area after main filtering.")
    else:
        area_resolved = int(area_status.eq("resolved").sum())
        area_supported = int(area_quality.isin(AREA_SUPPORTED_QUALITIES).sum())
        area_mismatch = int(area_flag.eq("area_mismatch").sum())
        lines.append(f"  - rows with positive reported_area: {area_total} ({pct(area_total, total_main):.2f}% of main rows)")
        lines.append(f"  - resolved / publishable: {area_resolved} ({pct(area_resolved, area_total):.2f}% of reported_area rows)")
        lines.append(f"  - area-supported match_quality ({', '.join(sorted(AREA_SUPPORTED_QUALITIES))}): {area_supported} ({pct(area_supported, area_total):.2f}% of reported_area rows)")
        lines.append(f"  - area_mismatch basin_flag: {area_mismatch} ({pct(area_mismatch, area_total):.2f}% of reported_area rows)")
        lines.append(f"  - distance among reported_area rows: {_format_numeric_summary(area_distance, 'm', 2)}")
        lines.append(f"  - abs(area_error) among reported_area rows: {_format_numeric_summary(area_abs, '', 3)}")
        lines.append("  - reported_area basin_status breakdown:")
        for row in summarize_counts(area_df, ["basin_status"]).itertuples(index=False):
            lines.append(f"    - {row.basin_status}: {int(row.row_count)} ({float(row.percent):.2f}%)")
        lines.append("  - reported_area match_quality breakdown:")
        for row in summarize_counts(area_df, ["match_quality"]).itertuples(index=False):
            lines.append(f"    - {row.match_quality}: {int(row.row_count)} ({float(row.percent):.2f}%)")
    lines.append("")
    lines.append("7. Interpretation")
    lines.append(
        "  S4 is the station-level basin tracing stage. Use these outputs to debug "
        "reported_area, area_error, point-to-reach distance, local/full basin geometry, "
        "and raw basin_status before later cluster/release transformations."
    )
    lines.append("")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def process_s4_table(input_path: Path, out_dir: Path, prefix: str, title: str, require_exists: bool) -> Optional[pd.DataFrame]:
    if not input_path.is_file():
        msg = f"[WARN] Missing input, skipped: {input_path}"
        if require_exists:
            raise FileNotFoundError(msg)
        print(msg)
        return None

    raw = pd.read_csv(input_path)
    df_all = normalize_s4_table(raw, prefix)
    df_all = add_distance_diagnostics(df_all)
    df_all = add_area_diagnostics(df_all)
    df_all = add_spatial_error_classes(df_all)

    exclusion_summary, exclude_mask = build_exclusion_summary(df_all)

    if EXCLUDE_REMOTE_REACH_SCALE_FROM_MAIN_STATS:
        df_main = df_all.loc[~exclude_mask].copy()
    else:
        df_main = df_all.copy()

    out_dir.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(out_dir / f"{prefix}_all_rows.csv", index=False)
    df_main.to_csv(out_dir / f"{prefix}_main_rows.csv", index=False)
    exclusion_summary.to_csv(out_dir / f"{prefix}_remote_reach_scale_exclusion_summary.csv", index=False)

    _write_count_outputs(df_main, out_dir, prefix)
    area_df = _write_reported_area_outputs(df_main, out_dir, prefix)
    _write_manual_review_outputs(df_main, out_dir, prefix, TOP_N_MANUAL_REVIEW)

    write_summary_text(
        input_path=input_path,
        df_all=df_all,
        df_main=df_main,
        exclusion_summary=exclusion_summary,
        area_df=area_df,
        out_path=out_dir / f"{prefix}_summary.txt",
        title=title,
    )

    if MAKE_FIGURES:
        _write_figures(df_main, area_df, out_dir / "figures", prefix)

    return df_main


def write_s4_overview(out_dir: Path, tables: Dict[str, pd.DataFrame]) -> None:
    rows = []
    for name, df in tables.items():
        if df is None:
            continue
        status = _clean_series(df["basin_status"]).str.lower()
        flag = _clean_series(df["basin_flag"]).str.lower()
        quality = _clean_series(df["match_quality"]).str.lower()
        area_df = df[df["has_reported_area"]].copy()
        area_status = _clean_series(area_df["basin_status"]).str.lower() if len(area_df) else pd.Series(dtype=str)
        area_quality = _clean_series(area_df["match_quality"]).str.lower() if len(area_df) else pd.Series(dtype=str)
        area_flag = _clean_series(area_df["basin_flag"]).str.lower() if len(area_df) else pd.Series(dtype=str)

        rows.append({
            "table": name,
            "main_rows": int(len(df)),
            "resolved_rows": int(status.eq("resolved").sum()),
            "resolved_percent": 100.0 * float(status.eq("resolved").sum()) / float(len(df)) if len(df) else 0.0,
            "unresolved_rows": int(status.eq("unresolved").sum()),
            "no_match_rows": int(flag.eq("no_match").sum()),
            "large_offset_rows": int(flag.eq("large_offset").sum()),
            "area_mismatch_rows": int(flag.eq("area_mismatch").sum()),
            "geometry_inconsistent_rows": int(flag.eq("geometry_inconsistent").sum()),
            "area_supported_rows": int(quality.isin(AREA_SUPPORTED_QUALITIES).sum()),
            "rows_with_reported_area": int(len(area_df)),
            "reported_area_resolved_rows": int(area_status.eq("resolved").sum()) if len(area_df) else 0,
            "reported_area_resolved_percent": 100.0 * float(area_status.eq("resolved").sum()) / float(len(area_df)) if len(area_df) else 0.0,
            "reported_area_area_supported_rows": int(area_quality.isin(AREA_SUPPORTED_QUALITIES).sum()) if len(area_df) else 0,
            "reported_area_area_mismatch_rows": int(area_flag.eq("area_mismatch").sum()) if len(area_df) else 0,
            "distance_m_median": numeric_summary(df["distance_m"])["median"],
            "distance_m_p90": numeric_summary(df["distance_m"])["p90"],
            "reported_area_abs_area_error_median": numeric_summary(area_df["area_log10_error_abs"])["median"] if len(area_df) else np.nan,
            "reported_area_abs_area_error_p90": numeric_summary(area_df["area_log10_error_abs"])["p90"] if len(area_df) else np.nan,
        })

    overview = pd.DataFrame(rows)
    overview.to_csv(out_dir / "s4_overview.csv", index=False)

    lines = ["S4 Spatial Matching Overview", "=" * 28, ""]
    for row in overview.itertuples(index=False):
        lines.append(f"{row.table}:")
        lines.append(f"  - main rows: {int(row.main_rows)}")
        lines.append(f"  - resolved: {int(row.resolved_rows)} ({float(row.resolved_percent):.2f}%)")
        lines.append(f"  - unresolved: {int(row.unresolved_rows)}")
        lines.append(f"  - no_match: {int(row.no_match_rows)}")
        lines.append(f"  - large_offset: {int(row.large_offset_rows)}")
        lines.append(f"  - area_mismatch: {int(row.area_mismatch_rows)}")
        lines.append(f"  - geometry_inconsistent: {int(row.geometry_inconsistent_rows)}")
        lines.append(f"  - rows with positive reported_area: {int(row.rows_with_reported_area)}")
        lines.append(
            f"  - reported_area resolved: {int(row.reported_area_resolved_rows)} "
            f"({float(row.reported_area_resolved_percent):.2f}%)"
        )
        lines.append("")
    (out_dir / "s4_overview.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    out_dir = Path(OUTPUT_DIR).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not MAKE_FIGURES:
        global plt
        plt = None

    s4_main = process_s4_table(
        input_path=Path(S4_UPSTREAM_CSV).expanduser(),
        out_dir=out_dir / "s4_upstream_basins",
        prefix="s4_upstream_basins",
        title="S4 Upstream Basins Spatial Matching Diagnostics",
        require_exists=True,
    )

    s4_area_check = process_s4_table(
        input_path=Path(S4_REPORTED_AREA_CHECK_CSV).expanduser(),
        out_dir=out_dir / "s4_reported_area_check",
        prefix="s4_reported_area_check",
        title="S4 Reported Area Check Spatial Matching Diagnostics",
        require_exists=False,
    )

    write_s4_overview(
        out_dir=out_dir,
        tables={
            "s4_upstream_basins": s4_main,
            "s4_reported_area_check": s4_area_check,
        },
    )

    print("")
    print(f"Wrote S4 spatial matching diagnostics -> {out_dir}")
    print("Key outputs:")
    print(f"  - {out_dir / 's4_overview.txt'}")
    print(f"  - {out_dir / 's4_overview.csv'}")
    print(f"  - {out_dir / 's4_upstream_basins' / 's4_upstream_basins_summary.txt'}")
    print(f"  - {out_dir / 's4_upstream_basins' / 's4_upstream_basins_main_rows.csv'}")
    print(f"  - {out_dir / 's4_upstream_basins' / 's4_upstream_basins_reported_area_rows.csv'}")
    print(f"  - {out_dir / 's4_reported_area_check' / 's4_reported_area_check_summary.txt'}")
    print(f"  - {out_dir / 's4_upstream_basins' / 'figures'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

