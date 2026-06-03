#!/usr/bin/env python3
"""Explain spatial matching uncertainty for the released sediment reference product.

This release-product diagnostic defaults to the publication package table:

    scripts_basin_test/output/sed_reference_release/station_catalog.csv

and writes a separate output directory:

    scripts_basin_test/output_other/explain_s8_basin_matching_error

The main statistics are computed after excluding satellite / reach-scale sources
such as RiverSed, GSED, and Dethier. Those products are valid observations, but
their coordinates represent image-derived reaches, centerlines, or ROIs rather
than precise gauge-outlet points. Therefore they should not be included in the
denominator when evaluating release-level basin matching errors.

The summary and figures also report a dedicated subset analysis for rows with a
source-reported drainage area (reported_area). This directly answers: among rows
where drainage-area evidence is available, how successful is basin matching?

Main outputs
------------
- spatial_match_error_table.csv
    Row-level release-product diagnostics after excluding satellite/reach-scale
    rows.
- spatial_match_error_summary.txt
    Human-readable release-level summary.
- remote_sensing_exclusion_summary.csv / .txt
    Counts of rows excluded before the main matching-error statistics.
- spatial_match_threshold_sensitivity.csv
    Release-schema-aware threshold sensitivity table.
- reported_area_*.csv
    Dedicated summaries for rows with positive reported_area.
- figures/*.png
    General release-level plots and reported_area-specific plots.

Usage
-----
Edit the USER CONFIGURATION block if your paths are non-standard, then run:

    python3 tools/explain_spatial_matching_error.py

No command-line arguments are required.
"""

# from __future__ import annotations  # removed for Python 3.6 compat

import os
import shutil
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - plotting is optional
    plt = None


# =============================================================================
# USER CONFIGURATION
# =============================================================================
INPUT_CSV_PATH = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release/station_catalog.csv"
OUTPUT_DIR = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output_other/explain_s8_basin_matching_error"

# Number of rows exported in each manual-review queue.
TOP_N_MANUAL_REVIEW = 100

# Set to False if you only want CSV/TXT outputs and do not want PNG figures.
MAKE_FIGURES = True

# Main statistics are computed after excluding these satellite/reach-scale
# sources. The list includes basin_policy.py aliases and spelling variants.
EXCLUDE_REMOTE_REACH_SCALE = True
REMOTE_REACH_SOURCE_PATTERNS = [
    "riversed",
    "river_sed",
    "gsed",
    "dethier",
    "deither",
    "source_remote_sensing_no_basin_match",
]
REMOTE_SOURCE_DISPLAY_ORDER = ["RiverSed", "GSED", "Dethier", "deither", "source_remote_sensing_no_basin_match"]

AREA_SUPPORTED_QUALITIES = {"area_matched", "area_approximate"}
UNRESOLVED_FLAGS = {"large_offset", "area_mismatch", "geometry_inconsistent", "no_match"}
DEFAULT_RELEASE_RELATIVE_PATH = Path("scripts_basin_test/output/sed_reference_release/station_catalog.csv")
DEFAULT_OUT_SUBDIR_NAME = "explain_s8_basin_matching_error"


# =============================================================================
# Path resolution
# =============================================================================
def _candidate_roots() -> List[Path]:
    """Return likely repository / Output_r roots for no-argument execution."""
    roots: List[Path] = []
    env_root = os.environ.get("OUTPUT_R_ROOT", "").strip()
    if env_root:
        roots.append(Path(env_root).expanduser())

    script_dir = Path(__file__).resolve().parent
    roots.extend([
        Path.cwd(),
        script_dir,
        script_dir.parent,
        script_dir.parent.parent,
    ])

    unique: List[Path] = []
    seen = set()
    for root in roots:
        try:
            resolved = root.resolve()
        except Exception:
            resolved = root
        key = str(resolved)
        if key not in seen:
            unique.append(resolved)
            seen.add(key)
    return unique


def _repo_default_input() -> Optional[Path]:
    """Auto-detect the release station catalogue without command-line args."""
    for root in _candidate_roots():
        candidate = root / DEFAULT_RELEASE_RELATIVE_PATH
        if candidate.is_file():
            return candidate
    return None


def _repo_default_out_dir(input_path: Optional[Path] = None) -> Path:
    """Resolve the default output directory without command-line args."""
    if input_path is not None:
        # For the standard path, input.parent.parent is scripts_basin_test/output.
        if input_path.parent.name == "sed_reference_release":
            return input_path.parent.parent / DEFAULT_OUT_SUBDIR_NAME
        return input_path.parent / DEFAULT_OUT_SUBDIR_NAME
    return Path("scripts_basin_test/output_other") / DEFAULT_OUT_SUBDIR_NAME


def _resolve_configured_input() -> Path:
    if INPUT_CSV_PATH:
        path = Path(INPUT_CSV_PATH).expanduser()
        return path.resolve() if path.exists() else path

    detected = _repo_default_input()
    if detected is None:
        searched = "\n".join(f"  - {root / DEFAULT_RELEASE_RELATIVE_PATH}" for root in _candidate_roots())
        raise FileNotFoundError(
            "Could not auto-detect sed_reference_release/station_catalog.csv.\n"
            "Please edit INPUT_CSV_PATH in the USER CONFIGURATION block.\n"
            "Searched:\n" + searched
        )
    return detected


def _resolve_configured_out_dir(input_path: Path) -> Path:
    if OUTPUT_DIR:
        return Path(OUTPUT_DIR).expanduser()
    return _repo_default_out_dir(input_path)


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


def _copy_if_exists(df: pd.DataFrame, src: str, dst: str) -> None:
    if dst not in df.columns and src in df.columns:
        df[dst] = df[src]


def _format_number(value: float, digits: int = 2) -> str:
    return "NA" if pd.isna(value) else f"{value:.{digits}f}"


def _format_numeric_summary(summary: Dict[str, float], unit: str = "", digits: int = 2) -> str:
    suffix = f" {unit}" if unit else ""
    return (
        f"n={int(summary['n'])}, "
        f"median={_format_number(summary['median'], digits)}{suffix}, "
        f"p90={_format_number(summary['p90'], digits)}{suffix}, "
        f"p95={_format_number(summary['p95'], digits)}{suffix}, "
        f"max={_format_number(summary['max'], digits)}{suffix}"
    )


# =============================================================================
# Release schema adapter
# =============================================================================
def normalize_release_station_catalog(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read release station_catalog.csv and adapt it to diagnostic names.

    The publication table uses release-facing names such as basin_match_quality
    and basin_distance_m. The downstream diagnostic functions expect normalized
    names such as match_quality and distance_m. This function preserves all
    original columns while adding normalized aliases.
    """
    raw = pd.read_csv(path)
    df = raw.copy()
    df["_input_row_number"] = np.arange(len(df), dtype=int)
    df["_input_table_type"] = "release_station_catalog"

    if "station_id" not in df.columns:
        if "cluster_uid" in df.columns:
            df["station_id"] = df["cluster_uid"]
        elif "cluster_id" in df.columns:
            df["station_id"] = df["cluster_id"]
        else:
            df["station_id"] = df["_input_row_number"]

    _copy_if_exists(df, "latitude", "lat")
    _copy_if_exists(df, "longitude", "lon")

    # Release schema -> diagnostic schema.
    _copy_if_exists(df, "basin_match_quality", "match_quality")
    _copy_if_exists(df, "match_status", "match_quality")
    _copy_if_exists(df, "basin_distance_m", "distance_m")
    _copy_if_exists(df, "basin_point_in_local", "point_in_local")
    _copy_if_exists(df, "basin_point_in_basin", "point_in_basin")
    _copy_if_exists(df, "point_in_local_catchment", "point_in_local")
    _copy_if_exists(df, "point_in_full_basin", "point_in_basin")
    _copy_if_exists(df, "upstream_area_km2", "uparea_merit")
    _copy_if_exists(df, "basin_area_km2", "uparea_merit")
    _copy_if_exists(df, "drainage_area_km2", "reported_area")
    _copy_if_exists(df, "source_reported_area_km2", "reported_area")

    # Manual-review friendly aliases.
    _copy_if_exists(df, "station_name", "source_station_name")
    _copy_if_exists(df, "river_name", "source_river_name")
    _copy_if_exists(df, "source_station_uid", "source_station_id")
    _copy_if_exists(df, "native_station_id", "source_station_id")

    if "source" not in df.columns:
        if "sources_used" in df.columns:
            df["source"] = df["sources_used"]
        elif "primary_source" in df.columns:
            df["source"] = df["primary_source"]
        elif "source_name" in df.columns:
            df["source"] = df["source_name"]
        else:
            df["source"] = ""

    for col in ["lon", "lat", "basin_id", "reported_area", "area_error", "uparea_merit", "distance_m", "n_upstream_reaches"]:
        if col not in df.columns:
            df[col] = np.nan

    for col in ["match_quality", "basin_status", "basin_flag", "source", "method"]:
        if col not in df.columns:
            df[col] = ""

    for col in ["point_in_local", "point_in_basin"]:
        if col not in df.columns:
            df[col] = False

    _to_numeric(
        df,
        ["lon", "lat", "basin_id", "reported_area", "area_error", "uparea_merit", "distance_m", "n_upstream_reaches", "cluster_id"],
    )

    for col in ["source", "method", "match_quality", "basin_status", "basin_flag"]:
        df[col] = _clean_series(df[col]).str.lower()

    for col in ["point_in_local", "point_in_basin"]:
        df[col] = _bool_series(df[col])

    blank_status = df["basin_status"].eq("")
    if blank_status.any():
        df.loc[blank_status, "basin_status"] = np.where(
            df.loc[blank_status, "basin_flag"].eq("ok"), "resolved", "unresolved"
        )
    blank_flag = df["basin_flag"].eq("")
    if blank_flag.any():
        df.loc[blank_flag, "basin_flag"] = np.where(
            df.loc[blank_flag, "basin_status"].eq("resolved"), "ok", "no_match"
        )

    return raw, df


def _source_text_for_filter(row: pd.Series) -> str:
    fields = ["sources_used", "source", "primary_source", "source_name", "data_source_name", "method"]
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


def build_exclusion_summary(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """Return exclusion counts and boolean exclusion mask."""
    labels = []
    exclude = []
    for _, row in df.iterrows():
        label = _remote_source_label(_source_text_for_filter(row))
        is_excluded = label is not None if EXCLUDE_REMOTE_REACH_SCALE else False
        labels.append(label if label is not None else "not_excluded")
        exclude.append(is_excluded)

    mask = pd.Series(exclude, index=df.index, name="excluded_remote_reach_scale")
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
# Diagnostics
# =============================================================================
def add_area_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    """Add area-ratio diagnostics from area_error if available.

    area_error is interpreted as log10(MERIT upstream area / source-reported
    area). Therefore 10 ** area_error is the multiplicative area ratio.
    """
    out = df.copy()
    if "area_error" not in out.columns:
        out["area_error"] = np.nan
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


def add_distance_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["distance_m"] = pd.to_numeric(out["distance_m"], errors="coerce")
    bins = [-np.inf, 100.0, 300.0, 1000.0, 5000.0, 120000.0, np.inf]
    labels = ["<=100 m", "100-300 m", "300-1000 m", "1-5 km", "5-120 km", ">120 km or invalid"]
    out["distance_bin"] = pd.cut(out["distance_m"], bins=bins, labels=labels)
    out["distance_bin"] = out["distance_bin"].astype("object").fillna("not_available")
    out["distance_km"] = out["distance_m"] / 1000.0
    return out


def has_reported_area(df: pd.DataFrame) -> pd.Series:
    """Rows with a usable source-reported drainage area."""
    if "reported_area" not in df.columns:
        return pd.Series(False, index=df.index)
    reported = pd.to_numeric(df["reported_area"], errors="coerce")
    return reported.notna() & np.isfinite(reported) & (reported > 0)


def classify_spatial_error(row: pd.Series) -> Tuple[str, str, int, str]:
    """Return error class, explanation, severity rank, and recommended use."""
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
                "The nearest or selected reach is farther than the release threshold; the basin assignment may represent a neighboring river or wrong branch.",
                4,
                "Keep the observation, but exclude from basin-scale attribution unless manually reviewed.",
            )
        if basin_flag == "area_mismatch":
            return (
                "D_area_mismatch_unresolved",
                "The matched MERIT upstream area and source-reported drainage area disagree strongly.",
                4,
                "Exclude from basin-scale attribution; review coordinates, source area, and nearby candidate reaches.",
            )
        if basin_flag == "geometry_inconsistent":
            return (
                "D_geometry_inconsistent_unresolved",
                "The point is not supported by the local/full basin geometry under the acceptance rules.",
                4,
                "Exclude from released basin polygons; inspect the local catchment and upstream basin geometry.",
            )
        return (
            "D_no_publishable_basin_match",
            "No safe basin assignment is available or the reach match failed.",
            4,
            "Use only as an observation with coordinates; do not use for basin-level analysis.",
        )

    finite_distance = pd.notna(distance) and np.isfinite(distance)
    if finite_distance and distance <= 300.0:
        if match_quality in AREA_SUPPORTED_QUALITIES:
            return (
                "A_high_confidence_area_supported_close",
                "The station is within 300 m of the matched reach and drainage-area evidence supports the match.",
                1,
                "Suitable for basin-scale analysis under the standard release policy.",
            )
        return (
            "A_high_confidence_close_distance",
            "The station is within 300 m of the matched reach; this is consistent with bank-vs-channel coordinate offsets.",
            1,
            "Suitable for basin-scale analysis under the standard release policy.",
        )

    if finite_distance and distance <= 1000.0 and match_quality in AREA_SUPPORTED_QUALITIES:
        return (
            "B_moderate_offset_area_supported",
            "The point-to-reach offset is moderate, but drainage-area evidence supports the selected reach.",
            2,
            "Suitable for basin-scale analysis, but keep distance and area diagnostics in uncertainty notes.",
        )

    if finite_distance and distance <= 1000.0 and point_in_local:
        return (
            "B_moderate_offset_local_geometry_supported",
            "The point-to-reach offset is moderate, but the original point falls inside the matched local catchment.",
            2,
            "Usable for basin-scale analysis with spatial uncertainty acknowledged.",
        )

    if finite_distance and distance <= 1000.0 and point_in_basin:
        return (
            "C_moderate_offset_full_basin_only",
            "The point is inside the full upstream basin but lacks stronger local or area support.",
            3,
            "Use cautiously; prefer manual review for basin-scale attribution.",
        )

    return (
        "C_resolved_but_weak_diagnostics",
        "The row is marked resolved, but the available diagnostics provide weak or incomplete evidence.",
        3,
        "Review before using in strict basin-scale analyses.",
    )


def add_spatial_error_classes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    classified = out.apply(classify_spatial_error, axis=1, result_type="expand")
    classified.columns = [
        "spatial_error_class",
        "spatial_error_explanation",
        "spatial_error_severity",
        "recommended_use",
    ]
    out = pd.concat([out, classified], axis=1)
    out["match_publishability"] = np.where(
        out["basin_status"].eq("resolved"),
        "publishable_basin_assignment",
        "observation_retained_basin_not_published",
    )
    out["has_reported_area"] = has_reported_area(out)
    return out


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


# =============================================================================
# Release-aware threshold sensitivity
# =============================================================================
def threshold_reclassify(
    df: pd.DataFrame,
    close_m: float,
    moderate_m: float,
    accept_area_approx: bool = True,
    accept_local: bool = True,
) -> pd.Series:
    """Reapply a simplified release policy under alternative thresholds."""
    quality = _clean_series(df["match_quality"]).str.lower()
    flag = _clean_series(df["basin_flag"]).str.lower()
    distance = pd.to_numeric(df["distance_m"], errors="coerce")
    point_in_local = df["point_in_local"].astype(bool) if "point_in_local" in df.columns else pd.Series(False, index=df.index)

    if "basin_id" in df.columns and pd.to_numeric(df["basin_id"], errors="coerce").notna().any():
        has_candidate = pd.to_numeric(df["basin_id"], errors="coerce").notna()
    else:
        has_candidate = quality.ne("failed") & flag.ne("no_match")

    area_supported = quality.eq("area_matched") | (accept_area_approx & quality.eq("area_approximate"))
    rejected = (
        (~has_candidate)
        | quality.eq("failed")
        | quality.eq("area_mismatch")
        | flag.eq("area_mismatch")
        | flag.eq("no_match")
        | distance.isna()
    )

    accepted = (
        (~rejected)
        & (
            (distance <= close_m)
            | ((distance <= moderate_m) & area_supported)
            | ((distance <= moderate_m) & accept_local & point_in_local)
        )
    )
    return pd.Series(np.where(accepted, "resolved", "unresolved"), index=df.index)


def build_threshold_sensitivity(df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    close_options = [100.0, 300.0, 500.0]
    moderate_options = [500.0, 1000.0, 2000.0]

    current_status = _clean_series(df["basin_status"]).str.lower()
    current_resolved = current_status.eq("resolved")
    total = len(df)

    for close_m in close_options:
        for moderate_m in moderate_options:
            if moderate_m < close_m:
                continue
            for accept_area_approx in [True, False]:
                new_status = threshold_reclassify(
                    df,
                    close_m=close_m,
                    moderate_m=moderate_m,
                    accept_area_approx=accept_area_approx,
                    accept_local=True,
                )
                new_resolved = new_status.eq("resolved")
                rows.append(
                    {
                        "close_distance_threshold_m": close_m,
                        "moderate_distance_threshold_m": moderate_m,
                        "accept_area_approximate": accept_area_approx,
                        "resolved_count": int(new_resolved.sum()),
                        "resolved_percent": 100.0 * float(new_resolved.sum()) / float(total) if total else 0.0,
                        "changed_from_current_count": int((new_status != current_status).sum()),
                        "newly_resolved_count": int((new_resolved & ~current_resolved).sum()),
                        "newly_unresolved_count": int((~new_resolved & current_resolved).sum()),
                    }
                )
    return pd.DataFrame(rows)


# =============================================================================
# Output writers
# =============================================================================
def write_exclusion_summary(
    raw_count: int,
    filtered_count: int,
    exclusion_summary: pd.DataFrame,
    out_dir: Path,
    input_path: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    exclusion_summary.to_csv(out_dir / "remote_sensing_exclusion_summary.csv", index=False)

    excluded_total = raw_count - filtered_count
    lines = []
    lines.append("Remote-sensing / reach-scale exclusion summary")
    lines.append("=" * 48)
    lines.append("")
    lines.append(f"Input release station_catalog: {input_path}")
    lines.append(f"Input release product rows: {raw_count}")
    lines.append(f"Excluded remote-sensing / reach-scale rows: {excluded_total}")
    lines.append(f"Rows retained for main release-level basin-matching statistics: {filtered_count}")
    lines.append("")
    lines.append("Excluded source groups:")
    excluded = exclusion_summary[exclusion_summary["excluded_remote_reach_scale"] == True]  # noqa: E712
    if excluded.empty:
        lines.append("  - none")
    else:
        for row in excluded.itertuples(index=False):
            lines.append(f"  - {row.excluded_source_group}: {int(row.row_count)}")
    lines.append("")
    lines.append("Interpretation:")
    lines.append(
        "  Satellite-derived and reach-scale products are retained in the release package, "
        "but they are excluded from the denominator of the main basin-matching error statistics "
        "because they are not gauge-outlet basin assignments."
    )
    (out_dir / "remote_sensing_exclusion_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_count_table(lines: List[str], table: pd.DataFrame, columns: Sequence[str], indent: str = "    ") -> None:
    if table.empty:
        lines.append(f"{indent}- none")
        return
    for row in table.itertuples(index=False):
        parts = [str(getattr(row, c)) for c in columns]
        label = " | ".join(parts)
        lines.append(f"{indent}- {label}: {int(row.row_count)} ({float(row.percent):.2f}%)")


def reported_area_summary_tables(area_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if area_df.empty:
        return {
            "status": pd.DataFrame(),
            "flag": pd.DataFrame(),
            "quality": pd.DataFrame(),
            "class": pd.DataFrame(),
            "status_quality": pd.DataFrame(),
            "area_error_bin_quality": pd.DataFrame(),
        }
    return {
        "status": summarize_counts(area_df, ["basin_status"]),
        "flag": summarize_counts(area_df, ["basin_flag"]),
        "quality": summarize_counts(area_df, ["match_quality"]),
        "class": summarize_counts(area_df, ["spatial_error_class"]),
        "status_quality": summarize_counts(area_df, ["basin_status", "match_quality"]),
        "area_error_bin_quality": summarize_counts(area_df, ["area_error_bin", "match_quality"]),
    }


def write_summary_text(
    df: pd.DataFrame,
    out_path: Path,
    input_path: Path,
    raw_count: int,
    excluded_count: int,
    exclusion_summary: pd.DataFrame,
) -> None:
    total = len(df)
    status_counts = summarize_counts(df, ["basin_status"])
    flag_counts = summarize_counts(df, ["basin_flag"])
    class_counts = summarize_counts(df, ["spatial_error_class"])

    status = _clean_series(df["basin_status"]).str.lower()
    flag = _clean_series(df["basin_flag"]).str.lower()
    quality = _clean_series(df["match_quality"]).str.lower()
    resolved = df[status.eq("resolved")]
    unresolved = df[status.eq("unresolved")]
    resolved_distance = numeric_summary(resolved["distance_m"])
    unresolved_distance = numeric_summary(unresolved["distance_m"])
    all_distance = numeric_summary(df["distance_m"])
    area_available = df[has_reported_area(df)].copy()
    area_abs = numeric_summary(area_available["area_log10_error_abs"] if len(area_available) else pd.Series(dtype=float))
    area_distance = numeric_summary(area_available["distance_m"] if len(area_available) else pd.Series(dtype=float))
    area_tables = reported_area_summary_tables(area_available)

    def pct(n: int, denom: Optional[int] = None) -> float:
        d = total if denom is None else denom
        return 100.0 * n / d if d else 0.0

    n_area_mismatch = int((flag == "area_mismatch").sum())
    n_large_offset = int((flag == "large_offset").sum())
    n_geom = int((flag == "geometry_inconsistent").sum())
    n_no_match = int((flag == "no_match").sum())

    area_total = len(area_available)
    area_status = _clean_series(area_available["basin_status"]).str.lower() if area_total else pd.Series(dtype=str)
    area_flag = _clean_series(area_available["basin_flag"]).str.lower() if area_total else pd.Series(dtype=str)
    area_quality = _clean_series(area_available["match_quality"]).str.lower() if area_total else pd.Series(dtype=str)
    area_resolved = int(area_status.eq("resolved").sum()) if area_total else 0
    area_area_supported = int(area_quality.isin(AREA_SUPPORTED_QUALITIES).sum()) if area_total else 0
    area_area_mismatch = int(area_flag.eq("area_mismatch").sum()) if area_total else 0

    lines: List[str] = []
    lines.append("Release-Level Spatial Matching Error Explanation")
    lines.append("=" * 49)
    lines.append("")
    lines.append(f"Input table: {input_path}")
    lines.append(f"Input release product rows: {raw_count}")
    lines.append(f"Excluded remote-sensing / reach-scale rows before main statistics: {excluded_count} ({pct(excluded_count, raw_count):.2f}% of input)")
    lines.append(f"Rows analyzed in main release-level statistics: {total}")
    lines.append("")
    lines.append("Excluded source groups")
    excluded = exclusion_summary[exclusion_summary["excluded_remote_reach_scale"] == True]  # noqa: E712
    if excluded.empty:
        lines.append("  - none")
    else:
        for row in excluded.itertuples(index=False):
            lines.append(f"  - {row.excluded_source_group}: {int(row.row_count)}")
    lines.append("")
    lines.append("1. Publication status after source filtering")
    _append_count_table(lines, status_counts, ["basin_status"], indent="  ")
    lines.append("")
    lines.append("2. Basin flag breakdown after source filtering")
    _append_count_table(lines, flag_counts, ["basin_flag"], indent="  ")
    lines.append("")
    lines.append("3. Spatial error classes after source filtering")
    _append_count_table(lines, class_counts, ["spatial_error_class"], indent="  ")
    lines.append("")
    lines.append("4. Distance diagnostics")
    lines.append(f"  - all finite distances: {_format_numeric_summary(all_distance, 'm', 2)}")
    lines.append(f"  - resolved distances: {_format_numeric_summary(resolved_distance, 'm', 2)}")
    lines.append(f"  - unresolved finite distances: {_format_numeric_summary(unresolved_distance, 'm', 2)}")
    lines.append("")
    lines.append("5. Drainage-area diagnostics")
    lines.append(f"  - rows with positive reported_area: {area_total} ({pct(area_total):.2f}% of analyzed rows)")
    lines.append(f"  - abs(area_error) among rows with reported_area: {_format_numeric_summary(area_abs, '', 3)}")
    lines.append(
        "  - interpretation: area_error = log10(MERIT upstream area / source-reported area); abs(area_error)=0.3 means about a factor-of-2 difference."
    )
    lines.append("")
    lines.append("6. Basin-matching results among rows with reported_area")
    if area_total == 0:
        lines.append("  - No rows have positive reported_area after source filtering.")
    else:
        lines.append(f"  - positive reported_area rows: {area_total} ({pct(area_total):.2f}% of analyzed rows)")
        lines.append(f"  - resolved / publishable basin assignments: {area_resolved} ({pct(area_resolved, area_total):.2f}% of reported_area rows)")
        lines.append(f"  - area-supported match_quality ({', '.join(sorted(AREA_SUPPORTED_QUALITIES))}): {area_area_supported} ({pct(area_area_supported, area_total):.2f}% of reported_area rows)")
        lines.append(f"  - area_mismatch basin_flag: {area_area_mismatch} ({pct(area_area_mismatch, area_total):.2f}% of reported_area rows)")
        lines.append(f"  - distance among reported_area rows: {_format_numeric_summary(area_distance, 'm', 2)}")
        lines.append(f"  - abs(area_error) among reported_area rows: {_format_numeric_summary(area_abs, '', 3)}")
        lines.append("  - basin_status breakdown:")
        _append_count_table(lines, area_tables["status"], ["basin_status"], indent="    ")
        lines.append("  - basin_flag breakdown:")
        _append_count_table(lines, area_tables["flag"], ["basin_flag"], indent="    ")
        lines.append("  - match_quality breakdown:")
        _append_count_table(lines, area_tables["quality"], ["match_quality"], indent="    ")
        lines.append("  - spatial_error_class breakdown:")
        _append_count_table(lines, area_tables["class"], ["spatial_error_class"], indent="    ")
    lines.append("")
    lines.append("7. Main uncertainty mechanisms among checked release-product rows")
    lines.append(f"  - no publishable match: {n_no_match} ({pct(n_no_match):.2f}%)")
    lines.append(f"  - large point-to-reach offset: {n_large_offset} ({pct(n_large_offset):.2f}%)")
    lines.append(f"  - source area vs MERIT area mismatch: {n_area_mismatch} ({pct(n_area_mismatch):.2f}%)")
    lines.append(f"  - geometry inconsistency: {n_geom} ({pct(n_geom):.2f}%)")
    lines.append("")
    lines.append("8. Suggested manuscript wording")
    lines.append(
        "  Release-level basin-matching uncertainty was evaluated from station_catalog.csv after excluding satellite-derived and reach-scale products from the matching-error denominator. Basin assignments were quantified using point-to-reach distance, drainage-area agreement, and point-in-polygon diagnostics. For rows with source-reported drainage area, we separately summarized the resolved rate, area-supported match qualities, and area-mismatch failures. Resolved rows are considered suitable for released basin polygons, whereas unresolved rows are retained as observations but excluded from formal basin-polygon publication."
    )
    lines.append("")
    lines.append("9. Suggested user filtering")
    lines.append("  - Standard basin-scale use: basin_status == 'resolved'.")
    lines.append("  - Conservative basin-scale use: basin_status == 'resolved' and spatial_error_severity <= 2.")
    lines.append("  - Reported-area-supported use: has_reported_area == True and match_quality in ['area_matched', 'area_approximate'].")
    lines.append("  - Observation-only use: all retained rows can be used as records, but unresolved rows should not be used to extract upstream basin attributes.")
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# =============================================================================
# Plot helpers
# =============================================================================
def _save_bar(table: pd.DataFrame, label_col: str, value_col: str, title: str, out_png: Path) -> None:
    if plt is None or table.empty:
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
    if len(s) == 0:
        return
    if log_x:
        s = s[s > 0]
        if len(s) == 0:
            return
        bins = np.logspace(np.log10(max(s.min(), 1e-6)), np.log10(s.max()), 50)
    else:
        bins = 50
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
    if plt is None or "area_log10_error_abs" not in df.columns:
        return
    work = df[["distance_m", "area_log10_error_abs"]].copy()
    work = work.replace([np.inf, -np.inf], np.nan).dropna()
    work = work[(work["distance_m"] > 0) & (work["area_log10_error_abs"] >= 0)]
    if work.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(work["distance_m"], work["area_log10_error_abs"], s=10, alpha=0.35)
    ax.set_xscale("log")
    ax.set_xlabel("point-to-reach distance (m, log scale)")
    ax.set_ylabel("abs(log10(MERIT area / reported area))")
    ax.set_title("Distance vs drainage-area mismatch")
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def _save_threshold_plot(table: pd.DataFrame, out_png: Path) -> None:
    if plt is None or table.empty:
        return
    work = table[table["accept_area_approximate"] == True].copy()  # noqa: E712
    if work.empty:
        return
    labels = work.apply(
        lambda r: f"{int(r['close_distance_threshold_m'])}/{int(r['moderate_distance_threshold_m'])}", axis=1
    )
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(np.arange(len(work)), work["resolved_percent"], marker="o")
    ax.set_xticks(np.arange(len(work)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("resolved rows (%)")
    ax.set_xlabel("close/moderate distance thresholds (m)")
    ax.set_title("Release-level basin-status sensitivity to distance thresholds")
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def _save_stacked_count_bar(
    df: pd.DataFrame,
    x_col: str,
    stack_col: str,
    title: str,
    ylabel: str,
    out_png: Path,
    normalize: bool = False,
) -> None:
    if plt is None or df.empty or x_col not in df.columns or stack_col not in df.columns:
        return
    pivot = pd.crosstab(df[x_col].fillna("NA"), df[stack_col].fillna("NA"))
    if pivot.empty:
        return
    if normalize:
        row_totals = pivot.sum(axis=1).replace(0, np.nan)
        plot_df = pivot.div(row_totals, axis=0) * 100.0
    else:
        plot_df = pivot
    fig, ax = plt.subplots(figsize=(max(8, 0.6 * len(plot_df)), 5))
    plot_df.plot(kind="bar", stacked=True, ax=ax)
    ax.set_xlabel(x_col)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title=stack_col, bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def _save_reported_area_resolution_pie(area_df: pd.DataFrame, out_png: Path) -> None:
    if plt is None or area_df.empty:
        return
    counts = _clean_series(area_df["basin_status"]).replace("", "NA").value_counts()
    if counts.empty:
        return
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(counts.values, labels=counts.index.astype(str), autopct="%1.1f%%", startangle=90)
    ax.set_title("Basin status among rows with reported_area")
    fig.tight_layout()
    fig.savefig(out_png, dpi=180)
    plt.close(fig)


def write_reported_area_csv_outputs(area_df: pd.DataFrame, out_dir: Path) -> None:
    tables = reported_area_summary_tables(area_df)
    tables["status"].to_csv(out_dir / "reported_area_match_status_counts.csv", index=False)
    tables["flag"].to_csv(out_dir / "reported_area_match_flag_counts.csv", index=False)
    tables["quality"].to_csv(out_dir / "reported_area_match_quality_counts.csv", index=False)
    tables["class"].to_csv(out_dir / "reported_area_spatial_error_class_counts.csv", index=False)
    tables["status_quality"].to_csv(out_dir / "reported_area_match_status_quality_counts.csv", index=False)
    tables["area_error_bin_quality"].to_csv(out_dir / "reported_area_area_error_bin_quality_counts.csv", index=False)
    area_df.to_csv(out_dir / "reported_area_spatial_match_rows.csv", index=False)


def write_reported_area_figures(area_df: pd.DataFrame, figs_dir: Path) -> None:
    if plt is None or area_df.empty:
        return
    reported_dir = figs_dir / "reported_area"
    reported_dir.mkdir(exist_ok=True)

    _save_bar(
        summarize_counts(area_df, ["basin_status"]),
        "basin_status",
        "row_count",
        "Basin status among rows with reported_area",
        reported_dir / "reported_area_basin_status_counts.png",
    )
    _save_bar(
        summarize_counts(area_df, ["basin_flag"]),
        "basin_flag",
        "row_count",
        "Basin flag among rows with reported_area",
        reported_dir / "reported_area_basin_flag_counts.png",
    )
    _save_bar(
        summarize_counts(area_df, ["match_quality"]),
        "match_quality",
        "row_count",
        "Match quality among rows with reported_area",
        reported_dir / "reported_area_match_quality_counts.png",
    )
    _save_reported_area_resolution_pie(area_df, reported_dir / "reported_area_basin_status_share.png")
    _save_hist(
        area_df["distance_m"],
        "Point-to-reach distance among rows with reported_area",
        "distance (m)",
        reported_dir / "reported_area_distance_hist_logx.png",
        log_x=True,
    )
    _save_hist(
        area_df["area_log10_error_abs"],
        "Drainage-area mismatch among rows with reported_area",
        "abs(log10 area ratio)",
        reported_dir / "reported_area_area_error_hist.png",
    )
    _save_scatter(area_df, reported_dir / "reported_area_distance_vs_area_error.png")
    _save_stacked_count_bar(
        area_df,
        x_col="area_error_bin",
        stack_col="basin_status",
        title="Basin status by drainage-area error bin",
        ylabel="row count",
        out_png=reported_dir / "reported_area_status_by_area_error_bin.png",
        normalize=False,
    )
    _save_stacked_count_bar(
        area_df,
        x_col="match_quality",
        stack_col="basin_status",
        title="Basin status by match quality for rows with reported_area",
        ylabel="row count",
        out_png=reported_dir / "reported_area_status_by_match_quality.png",
        normalize=False,
    )


def write_outputs(
    df: pd.DataFrame,
    input_path: Path,
    out_dir: Path,
    top_n: int,
    raw_count: int,
    excluded_count: int,
    exclusion_summary: pd.DataFrame,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    row_table = df.sort_values(["spatial_error_severity", "distance_m"], ascending=[False, False], kind="stable")
    row_table.to_csv(out_dir / "spatial_match_error_table.csv", index=False)

    summarize_counts(df, ["basin_status"]).to_csv(out_dir / "spatial_match_status_counts.csv", index=False)
    summarize_counts(df, ["basin_flag"]).to_csv(out_dir / "spatial_match_flag_counts.csv", index=False)
    summarize_counts(df, ["match_quality"]).to_csv(out_dir / "spatial_match_quality_counts.csv", index=False)
    summarize_counts(df, ["spatial_error_class"]).to_csv(out_dir / "spatial_match_error_class_counts.csv", index=False)
    summarize_counts(df, ["distance_bin", "basin_status"]).to_csv(out_dir / "spatial_match_distance_bins.csv", index=False)
    summarize_counts(df, ["area_error_bin", "match_quality"]).to_csv(out_dir / "spatial_match_area_error_bins.csv", index=False)
    summarize_counts(df, ["has_reported_area", "basin_status"]).to_csv(out_dir / "spatial_match_status_by_reported_area_presence.csv", index=False)

    area_df = df[df["has_reported_area"]].copy()
    write_reported_area_csv_outputs(area_df, out_dir)

    if "resolution" in df.columns:
        summarize_counts(df, ["resolution", "basin_status"]).to_csv(out_dir / "spatial_match_status_by_resolution.csv", index=False)
        summarize_counts(df, ["resolution", "basin_flag"]).to_csv(out_dir / "spatial_match_flag_by_resolution.csv", index=False)

    if "source" in df.columns:
        summarize_counts(df, ["source", "basin_status"]).to_csv(out_dir / "spatial_match_status_by_source.csv", index=False)
        summarize_counts(df, ["source", "basin_flag"]).to_csv(out_dir / "spatial_match_flag_by_source.csv", index=False)

    threshold_table = build_threshold_sensitivity(df)
    threshold_table.to_csv(out_dir / "spatial_match_threshold_sensitivity.csv", index=False)

    queue_cols = [
        "station_id",
        "cluster_uid",
        "cluster_id",
        "resolution",
        "lat",
        "lon",
        "source",
        "sources_used",
        "source_station_name",
        "source_river_name",
        "source_station_id",
        "station_name",
        "river_name",
        "basin_id",
        "distance_m",
        "reported_area",
        "uparea_merit",
        "upstream_area_km2",
        "area_error",
        "area_ratio_merit_to_reported",
        "match_quality",
        "point_in_local",
        "point_in_basin",
        "basin_status",
        "basin_flag",
        "has_reported_area",
        "spatial_error_class",
        "spatial_error_explanation",
        "spatial_error_severity",
        "recommended_use",
    ]
    queue_cols = [c for c in queue_cols if c in df.columns]

    df.sort_values("distance_m", ascending=False).head(top_n)[queue_cols].to_csv(out_dir / "manual_review_top_large_offsets.csv", index=False)
    df[_clean_series(df["basin_flag"]).str.lower().eq("area_mismatch")].sort_values(
        "area_log10_error_abs", ascending=False
    ).head(top_n)[queue_cols].to_csv(out_dir / "manual_review_area_mismatch.csv", index=False)
    df[_clean_series(df["basin_flag"]).str.lower().eq("geometry_inconsistent")].sort_values(
        "distance_m", ascending=False
    ).head(top_n)[queue_cols].to_csv(out_dir / "manual_review_geometry_inconsistent.csv", index=False)
    df[df["spatial_error_severity"] >= 4].sort_values(
        ["spatial_error_severity", "distance_m"], ascending=[False, False]
    ).head(top_n)[queue_cols].to_csv(out_dir / "manual_review_high_risk.csv", index=False)

    write_exclusion_summary(
        raw_count=raw_count,
        filtered_count=len(df),
        exclusion_summary=exclusion_summary,
        out_dir=out_dir,
        input_path=input_path,
    )
    write_summary_text(
        df,
        out_dir / "spatial_match_error_summary.txt",
        input_path,
        raw_count=raw_count,
        excluded_count=excluded_count,
        exclusion_summary=exclusion_summary,
    )

    figs_dir = out_dir / "figures"
    figs_dir.mkdir(exist_ok=True)
    _save_hist(df["distance_m"], "Release-level point-to-reach distance distribution", "distance (m)", figs_dir / "distance_hist_logx.png", log_x=True)
    _save_hist(df["area_log10_error_abs"], "Release-level drainage-area mismatch distribution", "abs(log10 area ratio)", figs_dir / "area_error_hist.png", log_x=False)
    _save_scatter(df, figs_dir / "distance_vs_area_error.png")
    _save_bar(summarize_counts(df, ["basin_flag"]), "basin_flag", "row_count", "Release-level basin flag counts", figs_dir / "basin_flag_counts.png")
    _save_bar(summarize_counts(df, ["spatial_error_class"]), "spatial_error_class", "row_count", "Release-level spatial error class counts", figs_dir / "spatial_error_class_counts.png")
    _save_bar(summarize_counts(df, ["has_reported_area"]), "has_reported_area", "row_count", "Rows with/without reported_area", figs_dir / "reported_area_presence_counts.png")
    _save_stacked_count_bar(
        df,
        x_col="has_reported_area",
        stack_col="basin_status",
        title="Basin status by reported_area availability",
        ylabel="row count",
        out_png=figs_dir / "basin_status_by_reported_area_presence.png",
    )
    _save_threshold_plot(threshold_table, figs_dir / "threshold_sensitivity.png")
    write_reported_area_figures(area_df, figs_dir)


def build_spatial_error_package(input_path: Path, out_dir: Path, top_n: int = 100) -> pd.DataFrame:
    raw_df, df = normalize_release_station_catalog(input_path)
    raw_count = len(raw_df)
    exclusion_summary, exclude_mask = build_exclusion_summary(df)

    if EXCLUDE_REMOTE_REACH_SCALE:
        df = df.loc[~exclude_mask].copy()
    df["excluded_remote_reach_scale"] = False

    df = add_distance_diagnostics(df)
    df = add_area_diagnostics(df)
    df = add_spatial_error_classes(df)

    write_outputs(
        df,
        input_path=input_path,
        out_dir=out_dir,
        top_n=top_n,
        raw_count=raw_count,
        excluded_count=int(exclude_mask.sum()),
        exclusion_summary=exclusion_summary,
    )
    return df


def main() -> int:
    input_path = _resolve_configured_input()
    if not input_path.is_file():
        raise FileNotFoundError(
            f"Input CSV not found: {input_path}\n"
            "Edit INPUT_CSV_PATH in the USER CONFIGURATION block."
        )

    out_dir = _resolve_configured_out_dir(input_path)

    if not MAKE_FIGURES:
        global plt
        plt = None

    df = build_spatial_error_package(
        input_path=input_path,
        out_dir=out_dir,
        top_n=TOP_N_MANUAL_REVIEW,
    )
    area_rows = int(df["has_reported_area"].sum()) if "has_reported_area" in df.columns else 0
    print(f"Input release station_catalog: {input_path}")
    print(f"Wrote release-level spatial matching error package -> {out_dir}")
    print(f"Rows processed after remote/reach-scale filtering: {len(df)}")
    print(f"Rows with positive reported_area: {area_rows}")
    print("Key outputs:")
    print(f"  - {out_dir / 'spatial_match_error_summary.txt'}")
    print(f"  - {out_dir / 'remote_sensing_exclusion_summary.txt'}")
    print(f"  - {out_dir / 'spatial_match_error_table.csv'}")
    print(f"  - {out_dir / 'reported_area_match_status_counts.csv'}")
    print(f"  - {out_dir / 'spatial_match_threshold_sensitivity.csv'}")
    print(f"  - {out_dir / 'figures'}")
    # =============================================================================
    # Copy markdown-format outputs to the docs/reports directory
    # =============================================================================
    reports_dir = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/docs/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    md_extensions = {".md", ".txt", ".csv"}
    md_files = [
        "spatial_match_error_summary.txt",
        "remote_sensing_exclusion_summary.txt",
        "spatial_match_error_table.csv",
        "spatial_match_status_counts.csv",
        "spatial_match_flag_counts.csv",
        "spatial_match_quality_counts.csv",
        "spatial_match_error_class_counts.csv",
        "spatial_match_distance_bins.csv",
        "spatial_match_area_error_bins.csv",
        "spatial_match_status_by_reported_area_presence.csv",
        "spatial_match_threshold_sensitivity.csv",
        "manual_review_top_large_offsets.csv",
        "manual_review_area_mismatch.csv",
        "manual_review_geometry_inconsistent.csv",
        "manual_review_high_risk.csv",
        "remote_sensing_exclusion_summary.csv",
        "reported_area_match_status_counts.csv",
        "reported_area_match_flag_counts.csv",
        "reported_area_match_quality_counts.csv",
        "reported_area_spatial_error_class_counts.csv",
        "reported_area_match_status_quality_counts.csv",
        "reported_area_area_error_bin_quality_counts.csv",
        "reported_area_spatial_match_rows.csv",
    ]
    for fname in md_files:
        src = out_dir / fname
        if src.is_file():
            shutil.copy2(str(src), str(reports_dir / fname))
    
    # Copy figures directory recursively
    figs_src = out_dir / "figures"
    if figs_src.is_dir():
        figs_dst = reports_dir / "figures"
        if figs_dst.is_dir():
            shutil.rmtree(str(figs_dst))
        shutil.copytree(str(figs_src), str(figs_dst))
    
    print(f"Copied md/txt/csv outputs and figures -> {reports_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
