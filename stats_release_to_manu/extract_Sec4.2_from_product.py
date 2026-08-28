#!/usr/bin/env python3
"""Extract manuscript Section 4.2 source-contribution values without reading NetCDF.

This script is intentionally a *post-processing* reader.  It consumes values that
have already been produced by either:

1. the plotting-data CSVs written by
   ``figures/scripts/plot_fig5_combined_source_contribution_direct_release.py`` (preferred for exact Figure 5 parity), or
2. ``stats_release`` CSV tables, or
3. the three release CSV catalogues read by the Figure 5 helper script
   (``source_station_catalog.csv``, ``climatology_catalog.csv``,
   ``satellite_catalog.csv``), or
4. committed Markdown reports under ``docs/reports/stats_release`` as a final
   fallback.

It never opens ``sed_reference_timeseries_*.nc`` and therefore does not require
``netCDF4``.

Important counting note
-----------------------
The Figure 5 main panel and stats_release/source_contribution use source/catalog
attribution.  These counts are not necessarily identical to final-selected
matrix record counts.  The output therefore keeps ``unique_station_count`` and
``source_summed_station_count`` separate and records the provenance used for
all values instead of silently mixing definitions.

Typical usage from the sed_data_integration repository root::

    python extract_section_4_2_stats_from_products.py \
      --repo-root . \
      --output-dir docs/reports/section_4_2_from_products

If Figure 5 was rendered to a non-repository directory, point directly to its
``data`` directory::

    python extract_section_4_2_stats_from_products.py \
      --repo-root . \
      --figure-data-dir /path/to/figures/data \
      --output-dir docs/reports/section_4_2_from_products

Run with the project environment:
python extract_Sec4.2_from_product.py

Outputs
-------
- ``section_4_2_stats_from_products.json``
- ``section_4_2_main_sources.csv``
- ``section_4_2_climatology_sources.csv``
- ``section_4_2_satellite_sources.csv``
- ``section_4_2_stats_from_products.md``
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


DEFAULT_FIGURE_ID = "fig5_combined_source_contribution_direct_release"
SOURCE_NAME_MAP = {
    "USGS": "USGS NWIS",
    "RiverSed": "RivSed",
}

MAIN_COLUMNS = [
    "source_name",
    "source_name_display",
    "station_count",
    "source_station_count",
    "record_count",
    "first_year",
    "last_year",
]
OTHER_COLUMNS = [
    "source_name",
    "source_name_display",
    "station_count",
    "record_count",
    "first_year",
    "last_year",
]


# -----------------------------------------------------------------------------
# Small helpers
# -----------------------------------------------------------------------------


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "nat", "none"} else text


def _num_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    return pd.to_numeric(
        df[col].astype(str).str.replace(",", "", regex=False).str.strip(),
        errors="coerce",
    )


def _as_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"nan", "nat", "none"}:
        return None
    try:
        return int(round(float(text)))
    except Exception:
        return None


def _year_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype="float64")
    text = df[col].astype(str).str.strip()
    extracted = text.str.extract(r"^\s*([12]\d{3})", expand=False)
    out = pd.to_numeric(extracted, errors="coerce")
    needs_fallback = out.isna() & text.ne("") & ~text.str.lower().isin({"nan", "nat", "none"})
    if needs_fallback.any():
        parsed = pd.to_datetime(text.loc[needs_fallback], errors="coerce")
        out.loc[needs_fallback] = parsed.dt.year
    return out


def _nunique_nonempty(values: pd.Series) -> int:
    clean = values.astype(str).str.strip()
    clean = clean[clean.ne("") & ~clean.str.lower().isin({"nan", "none", "nat"})]
    return int(clean.nunique())


def _first_existing(paths: Iterable[Path]) -> Optional[Path]:
    for path in paths:
        if path and Path(path).is_file():
            return Path(path)
    return None


def _read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, keep_default_na=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _display_name(name: object) -> str:
    raw = _clean_text(name)
    return SOURCE_NAME_MAP.get(raw, raw)


def _coerce_canonical(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = np.nan if col not in {"source_name", "source_name_display"} else ""
    out["source_name"] = out["source_name"].astype(str).str.strip()
    out = out[out["source_name"].ne("")].copy()
    out["source_name_display"] = out["source_name"].map(_display_name)
    for col in ("station_count", "source_station_count", "record_count", "first_year", "last_year"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out[list(columns)].reset_index(drop=True)


def _merge_source_frames(primary: pd.DataFrame, supplemental: pd.DataFrame) -> pd.DataFrame:
    """Fill missing cells in primary from supplemental, matched on source_name."""
    if primary.empty:
        return supplemental.copy()
    if supplemental.empty:
        return primary.copy()
    p = primary.set_index("source_name", drop=False).copy()
    s = supplemental.set_index("source_name", drop=False).copy()
    for source, row in s.iterrows():
        if source not in p.index:
            p.loc[source] = row
            continue
        for col in p.columns:
            if col == "source_name":
                continue
            value = p.at[source, col]
            missing = False
            try:
                missing = pd.isna(value)
            except Exception:
                missing = False
            if isinstance(value, str) and not value.strip():
                missing = True
            if missing and col in row.index:
                p.at[source, col] = row[col]
    return p.reset_index(drop=True)


def _safe_min(series: pd.Series) -> Optional[int]:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    return int(vals.min()) if not vals.empty else None


def _safe_max(series: pd.Series) -> Optional[int]:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    return int(vals.max()) if not vals.empty else None


def _safe_sum(series: pd.Series) -> int:
    return int(round(pd.to_numeric(series, errors="coerce").fillna(0).sum()))


def _fmt_int(value: object) -> str:
    number = _as_int(value)
    return "NA" if number is None else f"{number:,}"


# -----------------------------------------------------------------------------
# Figure 5 plotting data
# -----------------------------------------------------------------------------


def _figure_data_paths(data_dir: Path, figure_id: str) -> Dict[str, Path]:
    return {
        "main": data_dir / f"{figure_id}_main_source_data.csv",
        "climatology": data_dir / f"{figure_id}_climatology_data.csv",
        "satellite": data_dir / f"{figure_id}_satellite_data.csv",
    }


def load_main_from_figure_data(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return _coerce_canonical(pd.DataFrame(), MAIN_COLUMNS)
    required = {"source_name", "first_year", "last_year"}
    cluster_col = "cluster_count" if "cluster_count" in df.columns else "station_count"
    if cluster_col not in df.columns:
        raise ValueError(f"{path} is missing a station/cluster count column (cluster_count or station_count)")
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing Figure 5 columns: {sorted(missing)}")
    record_col = "temporal_record_count" if "temporal_record_count" in df.columns else "spatial_record_count"
    if record_col not in df.columns:
        raise ValueError(f"{path} has neither temporal_record_count nor spatial_record_count")
    out = pd.DataFrame(
        {
            "source_name": df["source_name"].astype(str).str.strip(),
            "station_count": _num_series(df, cluster_col),
            "source_station_count": np.nan,
            "record_count": _num_series(df, record_col),
            "first_year": _num_series(df, "first_year"),
            "last_year": _num_series(df, "last_year"),
        }
    )
    # Undo display-name mapping so JSON has stable raw keys.
    reverse = {v: k for k, v in SOURCE_NAME_MAP.items()}
    out["source_name"] = out["source_name"].replace(reverse)
    return _coerce_canonical(out, MAIN_COLUMNS)


def load_other_from_figure_data(path: Path) -> pd.DataFrame:
    df = _read_csv(path)
    if df.empty:
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS)
    required = {"source_name", "contribution_count", "first_year", "last_year"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing Figure 5 columns: {sorted(missing)}")
    out = pd.DataFrame(
        {
            "source_name": df["source_name"].astype(str).str.strip(),
            "station_count": _num_series(df, "contribution_count"),
            "record_count": _num_series(df, "record_count"),
            "first_year": _num_series(df, "first_year"),
            "last_year": _num_series(df, "last_year"),
        }
    )
    reverse = {v: k for k, v in SOURCE_NAME_MAP.items()}
    out["source_name"] = out["source_name"].replace(reverse)
    return _coerce_canonical(out, OTHER_COLUMNS)


# -----------------------------------------------------------------------------
# Raw CSV inputs used by the Figure 5 helper
# -----------------------------------------------------------------------------


def load_main_from_figure_inputs(release_dir: Path) -> Tuple[pd.DataFrame, Dict[str, Optional[int]]]:
    path = release_dir / "source_station_catalog.csv"
    if not path.is_file():
        return _coerce_canonical(pd.DataFrame(), MAIN_COLUMNS), {}
    df = _read_csv(path)
    # Resolve column name: prefer cluster_uid, fall back to station_uid
    station_uid_col = "cluster_uid" if "cluster_uid" in df.columns else ("station_uid" if "station_uid" in df.columns else None)
    if station_uid_col is None:
        raise ValueError(f"{path} is missing a station/cluster UID column (cluster_uid or station_uid)")
    source_station_col = "source_station_uid" if "source_station_uid" in df.columns else station_uid_col
    required = {"source_name", station_uid_col, "n_records", "time_start", "time_end"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing Figure 5 input columns: {sorted(missing)}")
    df = df.copy()
    df["source_name"] = df["source_name"].astype(str).str.strip()
    df = df[df["source_name"].ne("")]
    df["record_count"] = _num_series(df, "n_records").fillna(0)
    df["first_year"] = _year_series(df, "time_start")
    df["last_year"] = _year_series(df, "time_end")
    grouped = (
        df.groupby("source_name", as_index=False)
        .agg(
            station_count=(station_uid_col, _nunique_nonempty),
            source_station_count=(source_station_col, _nunique_nonempty),
            record_count=("record_count", "sum"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
        )
        .reset_index(drop=True)
    )
    unique_meta = {
        "unique_station_count": _nunique_nonempty(df[station_uid_col]),
        "unique_source_station_count": _nunique_nonempty(df[source_station_col]),
    }
    return _coerce_canonical(grouped, MAIN_COLUMNS), unique_meta


def load_climatology_from_figure_inputs(release_dir: Path) -> Tuple[pd.DataFrame, Dict[str, Optional[int]]]:
    path = release_dir / "climatology_catalog.csv"
    if not path.is_file():
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS), {}
    df = _read_csv(path)
    required = {"source_name", "station_uid", "time"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing Figure 5 input columns: {sorted(missing)}")
    df = df.copy()
    df["source_name"] = df["source_name"].astype(str).str.strip()
    df = df[df["source_name"].ne("")]
    df["first_year"] = _year_series(df, "time")
    df["last_year"] = _year_series(df, "time")
    grouped = (
        df.groupby("source_name", as_index=False)
        .agg(
            station_count=("station_uid", _nunique_nonempty),
            record_count=("station_uid", "size"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
        )
        .reset_index(drop=True)
    )
    meta = {"unique_station_count": _nunique_nonempty(df["station_uid"])}
    return _coerce_canonical(grouped, OTHER_COLUMNS), meta


def load_satellite_from_figure_inputs(release_dir: Path) -> Tuple[pd.DataFrame, Dict[str, Optional[int]]]:
    path = release_dir / "satellite_catalog.csv"
    if not path.is_file():
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS), {}
    df = _read_csv(path)
    source_col = "source" if "source" in df.columns else "source_name"
    station_col = "cluster_uid" if "cluster_uid" in df.columns else ("satellite_station_uid" if "satellite_station_uid" in df.columns else "station_uid")
    required = {source_col, station_col, "n_records", "time_start", "time_end"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing Figure 5 input columns: {sorted(missing)}")
    df = df.copy()
    df["source_name"] = df[source_col].astype(str).str.strip()
    df = df[df["source_name"].ne("")]
    df["record_count"] = _num_series(df, "n_records").fillna(0)
    df["first_year"] = _year_series(df, "time_start")
    df["last_year"] = _year_series(df, "time_end")
    grouped = (
        df.groupby("source_name", as_index=False)
        .agg(
            station_count=(station_col, _nunique_nonempty),
            record_count=("record_count", "sum"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
        )
        .reset_index(drop=True)
    )
    meta = {"unique_station_count": _nunique_nonempty(df[station_col])}
    return _coerce_canonical(grouped, OTHER_COLUMNS), meta


# -----------------------------------------------------------------------------
# stats_release machine-readable CSV tables
# -----------------------------------------------------------------------------


def _stats_table(stats_root: Path, module: str, filename: str) -> Optional[Path]:
    candidates = [
        stats_root / module / "tables" / filename,
        stats_root / "tables" / filename,
    ]
    return _first_existing(candidates)


def load_main_from_stats_tables(stats_root: Path) -> Tuple[pd.DataFrame, Dict[str, Optional[int]], List[Path]]:
    used: List[Path] = []
    dataset_path = _stats_table(stats_root, "source_contribution", "table_main_source_dataset_contribution.csv")
    metrics_path = _stats_table(stats_root, "source_contribution", "table_main_report_key_metrics.csv")
    if dataset_path is None:
        return _coerce_canonical(pd.DataFrame(), MAIN_COLUMNS), {}, used
    used.append(dataset_path)
    df = _read_csv(dataset_path)
    if df.empty or "source_name" not in df.columns:
        return _coerce_canonical(pd.DataFrame(), MAIN_COLUMNS), {}, used
    out = pd.DataFrame(
        {
            "source_name": df["source_name"].astype(str).str.strip(),
            "station_count": _num_series(df, "n_clusters"),
            "source_station_count": _num_series(df, "n_source_stations"),
            "record_count": _num_series(df, "n_records"),
            "first_year": _num_series(df, "first_year"),
            "last_year": _num_series(df, "last_year"),
        }
    )
    meta: Dict[str, Optional[int]] = {}
    if metrics_path is not None:
        used.append(metrics_path)
        metrics = _read_csv(metrics_path)
        if not metrics.empty and {"metric", "value"}.issubset(metrics.columns):
            mapping = dict(zip(metrics["metric"].astype(str), metrics["value"]))
            meta = {
                "source_dataset_count_metric": _as_int(mapping.get("total_source_datasets")),
                "source_station_count_metric": _as_int(mapping.get("total_source_stations")),
                "source_summed_station_count_metric": _as_int(
                    mapping.get("total_clusters_source_sum")
                ),
                "record_count_metric": _as_int(mapping.get("total_records")),
                "first_year_metric": _as_int(mapping.get("earliest_year")),
                "last_year_metric": _as_int(mapping.get("latest_year")),
            }
    return _coerce_canonical(out, MAIN_COLUMNS), meta, used


def load_climatology_from_stats_tables(stats_root: Path) -> Tuple[pd.DataFrame, Dict[str, Optional[int]], List[Path]]:
    used: List[Path] = []
    source_path = _stats_table(stats_root, "temporal", "table_climatology_by_source.csv")
    summary_path = _stats_table(stats_root, "temporal", "table_climatology_temporal_summary.csv")
    if source_path is None:
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS), {}, used
    used.append(source_path)
    df = _read_csv(source_path)
    if df.empty or "source_name" not in df.columns:
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS), {}, used

    station_col = next((c for c in ("active_units", "station_count", "n_stations", "cluster_count") if c in df.columns), None)
    record_col = next((c for c in ("record_count_any", "record_count", "n_records") if c in df.columns), None)
    out = pd.DataFrame(
        {
            "source_name": df["source_name"].astype(str).str.strip(),
            "station_count": _num_series(df, station_col) if station_col else np.nan,
            "record_count": _num_series(df, record_col) if record_col else np.nan,
            "first_year": _num_series(df, "first_year"),
            "last_year": _num_series(df, "last_year"),
        }
    )
    meta: Dict[str, Optional[int]] = {}
    if summary_path is not None:
        used.append(summary_path)
        summary = _read_csv(summary_path)
        if not summary.empty:
            row = summary.iloc[0]
            meta = {
                "unique_station_count": _as_int(row.get("active_units", row.get("station_count"))),
                "record_count_metric": _as_int(row.get("record_count_any", row.get("record_count"))),
                "first_year_metric": _as_int(row.get("first_year")),
                "last_year_metric": _as_int(row.get("last_year")),
            }
    return _coerce_canonical(out, OTHER_COLUMNS), meta, used


def load_satellite_from_stats_tables(stats_root: Path) -> Tuple[pd.DataFrame, Dict[str, Optional[int]], List[Path]]:
    used: List[Path] = []
    # Prefer the temporal table because it has active units + time span; the
    # source-contribution dual-track table is a fallback.
    source_path = _stats_table(stats_root, "temporal", "table_satellite_by_source.csv")
    summary_path = _stats_table(stats_root, "temporal", "table_satellite_temporal_summary.csv")
    if source_path is not None:
        used.append(source_path)
        df = _read_csv(source_path)
        if not df.empty and "source_name" in df.columns:
            station_col = next((c for c in ("active_units", "station_count", "n_source_stations") if c in df.columns), None)
            record_col = next((c for c in ("record_count_any", "record_count", "n_records") if c in df.columns), None)
            out = pd.DataFrame(
                {
                    "source_name": df["source_name"].astype(str).str.strip(),
                    "station_count": _num_series(df, station_col) if station_col else np.nan,
                    "record_count": _num_series(df, record_col) if record_col else np.nan,
                    "first_year": _num_series(df, "first_year"),
                    "last_year": _num_series(df, "last_year"),
                }
            )
            meta: Dict[str, Optional[int]] = {}
            if summary_path is not None:
                used.append(summary_path)
                summary = _read_csv(summary_path)
                if not summary.empty:
                    row = summary.iloc[0]
                    meta = {
                        "unique_station_count": _as_int(row.get("active_units", row.get("station_count"))),
                        "record_count_metric": _as_int(row.get("record_count_any", row.get("record_count"))),
                        "first_year_metric": _as_int(row.get("first_year")),
                        "last_year_metric": _as_int(row.get("last_year")),
                    }
            return _coerce_canonical(out, OTHER_COLUMNS), meta, used

    sat_path = _stats_table(stats_root, "source_contribution", "table_sat_source_dataset_contribution.csv")
    if sat_path is None:
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS), {}, used
    used.append(sat_path)
    df = _read_csv(sat_path)
    if df.empty or "source_name" not in df.columns:
        return _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS), {}, used
    out = pd.DataFrame(
        {
            "source_name": df["source_name"].astype(str).str.strip(),
            "station_count": _num_series(df, "n_source_stations"),
            "record_count": _num_series(df, "n_records"),
            "first_year": _num_series(df, "first_year"),
            "last_year": _num_series(df, "last_year"),
        }
    )
    return _coerce_canonical(out, OTHER_COLUMNS), {}, used


# -----------------------------------------------------------------------------
# Markdown fallback for committed stats_release reports
# -----------------------------------------------------------------------------


def _parse_markdown_table(lines: List[str], start_index: int) -> pd.DataFrame:
    i = start_index
    while i < len(lines) and not lines[i].lstrip().startswith("|"):
        if lines[i].startswith("#") and i > start_index:
            return pd.DataFrame()
        i += 1
    if i + 1 >= len(lines):
        return pd.DataFrame()
    header = [x.strip() for x in lines[i].strip().strip("|").split("|")]
    sep = lines[i + 1].strip()
    if not sep.startswith("|") or not re.search(r"---", sep):
        return pd.DataFrame()
    rows: List[List[str]] = []
    i += 2
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if not line.lstrip().startswith("|"):
            break
        cells = [x.strip().replace("\\|", "|") for x in line.strip().strip("|").split("|")]
        if len(cells) == len(header):
            rows.append(cells)
        i += 1
    return pd.DataFrame(rows, columns=header)


def _table_after_heading(text: str, heading_text: str) -> pd.DataFrame:
    lines = text.splitlines()
    target = heading_text.lower().strip()
    for i, line in enumerate(lines):
        if line.lstrip("# ").strip().lower() == target:
            return _parse_markdown_table(lines, i + 1)
    return pd.DataFrame()


def _find_report(repo_root: Path, relative_candidates: Sequence[str]) -> Optional[Path]:
    return _first_existing(repo_root / rel for rel in relative_candidates)


def load_from_docs_markdown(repo_root: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Dict[str, Optional[int]]], List[Path]]:
    used: List[Path] = []
    main = _coerce_canonical(pd.DataFrame(), MAIN_COLUMNS)
    climatology = _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS)
    satellite = _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS)
    meta: Dict[str, Dict[str, Optional[int]]] = {"main": {}, "climatology": {}, "satellite": {}}

    source_report = _find_report(
        repo_root,
        [
            "docs/reports/stats_release/source_contribution/reports/source_contribution_stats.md",
            "docs/reports/stats_release/source_contribution/reports/source_contribution_report.md",
            "docs/reports/source_contribution_stats.md",
        ],
    )
    if source_report is not None:
        used.append(source_report)
        text = source_report.read_text(encoding="utf-8")
        table = _table_after_heading(text, "Main Source Contribution (In-Situ / Reference / Climatology)")
        if not table.empty and "source name" in table.columns:
            main = _coerce_canonical(
                pd.DataFrame(
                    {
                        "source_name": table["source name"],
                        "station_count": table.get("clusters", table.get("reference stations", np.nan)),
                        "source_station_count": table.get("n source stations", np.nan),
                        "record_count": table.get("n records", np.nan),
                        "first_year": table.get("first year", np.nan),
                        "last_year": table.get("last year", np.nan),
                    }
                ),
                MAIN_COLUMNS,
            )
        key = _table_after_heading(text, "Key Metrics (Main Track — In-Situ / Reference / Climatology)")
        if not key.empty and {"metric", "value"}.issubset(key.columns):
            mapping = dict(zip(key["metric"].astype(str), key["value"]))
            meta["main"] = {
                "source_dataset_count_metric": _as_int(mapping.get("total_source_datasets")),
                "source_station_count_metric": _as_int(mapping.get("total_source_stations")),
                "source_summed_station_count_metric": _as_int(mapping.get("total_clusters_source_sum")),
                "record_count_metric": _as_int(mapping.get("total_records")),
                "first_year_metric": _as_int(mapping.get("earliest_year")),
                "last_year_metric": _as_int(mapping.get("latest_year")),
            }
        sat = _table_after_heading(text, "Satellite Source Datasets")
        if not sat.empty and "source name" in sat.columns:
            satellite = _coerce_canonical(
                pd.DataFrame(
                    {
                        "source_name": sat["source name"],
                        "station_count": sat.get("n source stations", np.nan),
                        "record_count": sat.get("n records", np.nan),
                        "first_year": sat.get("first year", np.nan),
                        "last_year": sat.get("last year", np.nan),
                    }
                ),
                OTHER_COLUMNS,
            )

    temporal_report = _find_report(
        repo_root,
        [
            "docs/reports/stats_release/temporal/article_temporal_coverage_report.md",
            "docs/reports/article_temporal_coverage_report.md",
        ],
    )
    if temporal_report is not None:
        used.append(temporal_report)
        text = temporal_report.read_text(encoding="utf-8")
        top = _table_after_heading(text, "Top Sources")
        if not top.empty and "source name" in top.columns:
            supplemental = _coerce_canonical(
                pd.DataFrame(
                    {
                        "source_name": top["source name"],
                        "station_count": top.get("active units", np.nan),
                        "source_station_count": np.nan,
                        "record_count": top.get("record count", np.nan),
                        "first_year": top.get("first year", np.nan),
                        "last_year": top.get("last year", np.nan),
                    }
                ),
                MAIN_COLUMNS,
            )
            main = _merge_source_frames(main, supplemental)
        clim = _table_after_heading(text, "Climatology by Source")
        if not clim.empty and "source name" in clim.columns:
            station_col = next((c for c in ("active units", "station count", "stations") if c in clim.columns), None)
            record_col = next((c for c in ("record count any", "record count", "records") if c in clim.columns), None)
            climatology = _coerce_canonical(
                pd.DataFrame(
                    {
                        "source_name": clim["source name"],
                        "station_count": clim[station_col] if station_col else np.nan,
                        "record_count": clim[record_col] if record_col else np.nan,
                        "first_year": clim.get("first year", np.nan),
                        "last_year": clim.get("last year", np.nan),
                    }
                ),
                OTHER_COLUMNS,
            )
        sat_src = _table_after_heading(text, "Satellite by Source")
        if not sat_src.empty and "source name" in sat_src.columns:
            supplemental_sat = _coerce_canonical(
                pd.DataFrame(
                    {
                        "source_name": sat_src["source name"],
                        "station_count": sat_src.get("active units", np.nan),
                        "record_count": sat_src.get("record count any", np.nan),
                        "first_year": sat_src.get("first year", np.nan),
                        "last_year": sat_src.get("last year", np.nan),
                    }
                ),
                OTHER_COLUMNS,
            )
            satellite = _merge_source_frames(satellite, supplemental_sat)
        sat_summary = _table_after_heading(text, "Satellite Validation Product")
        if not sat_summary.empty:
            row = sat_summary.iloc[0]
            meta["satellite"] = {
                "unique_station_count": _as_int(row.get("active units")),
                "record_count_metric": _as_int(row.get("record count any")),
                "first_year_metric": _as_int(row.get("first year")),
                "last_year_metric": _as_int(row.get("last year")),
            }
    return main, climatology, satellite, meta, used


# -----------------------------------------------------------------------------
# Selecting sources and building Section 4.2-ready values
# -----------------------------------------------------------------------------


def _track_summary(
    df: pd.DataFrame,
    meta: Dict[str, Optional[int]],
    unique_station_count: Optional[int] = None,
) -> Dict[str, Optional[int]]:
    result: Dict[str, Optional[int]] = {
        "source_dataset_count": int(df["source_name"].nunique()) if not df.empty else None,
        "unique_station_count": unique_station_count,
        "source_summed_station_count": _safe_sum(df["station_count"]) if not df.empty else None,
        "record_count": _safe_sum(df["record_count"]) if not df.empty else None,
        "first_year": _safe_min(df["first_year"]) if not df.empty else None,
        "last_year": _safe_max(df["last_year"]) if not df.empty else None,
    }
    # Prefer explicit stats_release summary metrics where they represent the
    # same quantity.  Do not convert a source-summed metric into a unique count.
    if meta.get("source_dataset_count_metric") is not None:
        result["source_dataset_count"] = meta["source_dataset_count_metric"]
    if meta.get("source_summed_station_count_metric") is not None:
        result["source_summed_station_count"] = meta["source_summed_station_count_metric"]
    if meta.get("record_count_metric") is not None:
        result["record_count"] = meta["record_count_metric"]
    if meta.get("first_year_metric") is not None:
        result["first_year"] = meta["first_year_metric"]
    if meta.get("last_year_metric") is not None:
        result["last_year"] = meta["last_year_metric"]
    if result["unique_station_count"] is None and meta.get("unique_station_count") is not None:
        result["unique_station_count"] = meta["unique_station_count"]
    return result


def _records(df: pd.DataFrame) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for row in df.sort_values(["record_count", "source_name"], ascending=[False, True], na_position="last").to_dict("records"):
        clean: Dict[str, object] = {}
        for key, value in row.items():
            if key in {"source_name", "source_name_display"}:
                clean[key] = _clean_text(value)
            else:
                clean[key] = _as_int(value)
        rows.append(clean)
    return rows


def _named_claims(main: pd.DataFrame, climatology: pd.DataFrame, satellite: pd.DataFrame) -> Dict[str, object]:
    def row_for(df: pd.DataFrame, raw_name: str) -> Dict[str, Optional[int]]:
        if df.empty:
            return {}
        hit = df[df["source_name"].astype(str).eq(raw_name)]
        if hit.empty:
            return {}
        row = hit.iloc[0]
        return {
            "station_count": _as_int(row.get("station_count")),
            "record_count": _as_int(row.get("record_count")),
            "first_year": _as_int(row.get("first_year")),
            "last_year": _as_int(row.get("last_year")),
        }

    return {
        "main_named_sources": {
            name: row_for(main, name)
            for name in [
                "GFQA_v2",
                "USGS",
                "HYDAT",
                "Bayern",
                "EUSEDcollab",
                "Chao_Phraya_River",
                "Eurasian_River",
                "GloRiSe",
                "HYBAM",
            ]
        },
        "climatology_named_sources": {
            name: row_for(climatology, name)
            for name in ["Milliman", "Vanmaercke", "HMA", "Huanghe", "Ali_and_De_Boer", "Ali and De Boer"]
        },
        "satellite_named_sources": {
            name: row_for(satellite, name) for name in ["RiverSed", "GSED", "Dethier"]
        },
    }


def _candidate_figure_data_dirs(repo_root: Path, explicit: Optional[Path]) -> List[Path]:
    dirs: List[Path] = []
    if explicit is not None:
        dirs.append(explicit)
    dirs.extend(
        [
            repo_root / "figures" / "data",
            repo_root / "output_other" / "figures" / "data",
            repo_root / "output" / "figures" / "data",
        ]
    )
    # De-duplicate while preserving order.
    seen = set()
    out = []
    for d in dirs:
        key = str(d.resolve()) if d.exists() else str(d)
        if key not in seen:
            seen.add(key)
            out.append(d)
    return out


def _candidate_stats_roots(repo_root: Path, explicit: Optional[Path]) -> List[Path]:
    roots: List[Path] = []
    if explicit is not None:
        roots.append(explicit)
    roots.extend(
        [
            repo_root / "output_other" / "stats_release",
            repo_root / "docs" / "reports" / "stats_release",
        ]
    )
    seen = set()
    out = []
    for r in roots:
        key = str(r.resolve()) if r.exists() else str(r)
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def collect(args: argparse.Namespace) -> Tuple[Dict[str, object], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    repo_root = Path(args.repo_root).expanduser().resolve()
    release_dir = Path(args.release_dir).expanduser().resolve() if args.release_dir else repo_root / "output" / "sed_reference_release_minimal"
    figure_data_dir = Path(args.figure_data_dir).expanduser().resolve() if args.figure_data_dir else None
    stats_root_explicit = Path(args.stats_root).expanduser().resolve() if args.stats_root else None

    warnings: List[str] = []
    provenance: Dict[str, object] = {
        "main": [],
        "climatology": [],
        "satellite": [],
        "notes": [],
        "unique_count_inputs": [],
    }

    empty_main = _coerce_canonical(pd.DataFrame(), MAIN_COLUMNS)
    empty_other = _coerce_canonical(pd.DataFrame(), OTHER_COLUMNS)
    main = empty_main
    climatology = empty_other
    satellite = empty_other
    main_meta: Dict[str, Optional[int]] = {}
    clim_meta: Dict[str, Optional[int]] = {}
    sat_meta: Dict[str, Optional[int]] = {}
    raw_main_meta: Dict[str, Optional[int]] = {}
    raw_clim_meta: Dict[str, Optional[int]] = {}
    raw_sat_meta: Dict[str, Optional[int]] = {}

    preference = args.source_preference

    # 1) Figure plotting-data CSVs.
    if preference in {"auto", "figure-data"}:
        for data_dir in _candidate_figure_data_dirs(repo_root, figure_data_dir):
            paths = _figure_data_paths(data_dir, args.figure_id)
            if main.empty and paths["main"].is_file():
                main = load_main_from_figure_data(paths["main"])
                provenance["main"].append(str(paths["main"]))
            if climatology.empty and paths["climatology"].is_file():
                climatology = load_other_from_figure_data(paths["climatology"])
                provenance["climatology"].append(str(paths["climatology"]))
            if satellite.empty and paths["satellite"].is_file():
                satellite = load_other_from_figure_data(paths["satellite"])
                provenance["satellite"].append(str(paths["satellite"]))
            if not main.empty and not climatology.empty and not satellite.empty:
                break

    # 2) stats_release machine-readable tables.
    if preference in {"auto", "stats"}:
        for stats_root in _candidate_stats_roots(repo_root, stats_root_explicit):
            if main.empty:
                frame, meta, used = load_main_from_stats_tables(stats_root)
                if not frame.empty:
                    main, main_meta = frame, meta
                    provenance["main"].extend(str(p) for p in used)
            if climatology.empty:
                frame, meta, used = load_climatology_from_stats_tables(stats_root)
                if not frame.empty:
                    climatology, clim_meta = frame, meta
                    provenance["climatology"].extend(str(p) for p in used)
            if satellite.empty:
                frame, meta, used = load_satellite_from_stats_tables(stats_root)
                if not frame.empty:
                    satellite, sat_meta = frame, meta
                    provenance["satellite"].extend(str(p) for p in used)

    # 3) Raw release CSVs read by the Figure 5 helper.  In auto mode these are
    # also used to recover *unique* station totals even when the source rows came
    # from a precomputed product.
    if preference in {"auto", "figure-inputs"} or args.enrich_unique_counts:
        raw_main, raw_main_meta = load_main_from_figure_inputs(release_dir)
        raw_clim, raw_clim_meta = load_climatology_from_figure_inputs(release_dir)
        raw_sat, raw_sat_meta = load_satellite_from_figure_inputs(release_dir)
        if preference in {"auto", "figure-inputs"}:
            if main.empty and not raw_main.empty:
                main = raw_main
                provenance["main"].append(str(release_dir / "source_station_catalog.csv"))
            if climatology.empty and not raw_clim.empty:
                climatology = raw_clim
                provenance["climatology"].append(str(release_dir / "climatology_catalog.csv"))
            if satellite.empty and not raw_sat.empty:
                satellite = raw_sat
                provenance["satellite"].append(str(release_dir / "satellite_catalog.csv"))

    # 4) Committed Markdown reports.  Useful on a checkout that only contains
    # copied reports and no output_other tables/release package.
    if preference in {"auto", "docs"} and (main.empty or climatology.empty or satellite.empty):
        d_main, d_clim, d_sat, d_meta, used = load_from_docs_markdown(repo_root)
        if main.empty and not d_main.empty:
            main, main_meta = d_main, d_meta.get("main", {})
            provenance["main"].extend(str(p) for p in used)
        if climatology.empty and not d_clim.empty:
            climatology, clim_meta = d_clim, d_meta.get("climatology", {})
            provenance["climatology"].extend(str(p) for p in used)
        if satellite.empty and not d_sat.empty:
            satellite, sat_meta = d_sat, d_meta.get("satellite", {})
            provenance["satellite"].extend(str(p) for p in used)

    # If a stats table supplied main rows, use temporal committed report only as
    # a supplemental source for missing years/sources, never to overwrite values.
    if preference == "auto" and not main.empty:
        d_main, _, _, _, used = load_from_docs_markdown(repo_root)
        if not d_main.empty:
            before = set(main["source_name"].astype(str))
            main = _merge_source_frames(main, d_main)
            after = set(main["source_name"].astype(str))
            if after != before:
                provenance["main"].extend(str(p) for p in used)
                provenance["notes"].append("Committed temporal report filled main-source rows absent from a truncated source-contribution report.")

    # Unique counts come only from a raw catalogue or an explicit summary metric
    # that is genuinely unique.  Source-summed counts are kept separate.
    main_unique = raw_main_meta.get("unique_station_count")
    clim_unique = raw_clim_meta.get("unique_station_count")
    sat_unique = raw_sat_meta.get("unique_station_count")
    if main_unique is not None:
        provenance["unique_count_inputs"].append(str(release_dir / "source_station_catalog.csv"))
    if clim_unique is not None:
        provenance["unique_count_inputs"].append(str(release_dir / "climatology_catalog.csv"))
    if sat_unique is not None:
        provenance["unique_count_inputs"].append(str(release_dir / "satellite_catalog.csv"))

    main_summary = _track_summary(main, main_meta, main_unique)
    clim_summary = _track_summary(climatology, clim_meta, clim_unique)
    sat_summary = _track_summary(satellite, sat_meta, sat_unique)

    if main.empty:
        warnings.append("No main-source product was found. Provide --stats-root, --figure-data-dir, or a release directory containing source_station_catalog.csv.")
    if climatology.empty:
        warnings.append("No climatology source product was found. The current committed temporal report may legitimately contain no climatology rows; provide Figure 5 plotting data or climatology_catalog.csv if needed.")
    if satellite.empty:
        warnings.append("No satellite source product was found. Provide stats_release tables, Figure 5 plotting data, or satellite_catalog.csv.")
    if main_summary.get("unique_station_count") is None:
        warnings.append(
            "Main unique station count cannot be reconstructed from aggregated Figure/stats tables alone. "
            "The reported source_summed_station_count may over-count stations shared by multiple sources."
        )

    provenance["notes"].append(
        "Figure 5 main record_count is catalogue/source-attribution based (source_station_catalog n_records), not a final-selected NetCDF matrix count."
    )
    provenance["notes"].append(
        "Figure 5 climatology first_year/last_year are derived from climatology_catalog.time in the current plotting helper; these are representative catalogue times, not guaranteed source-reported coverage spans."
    )

    result: Dict[str, object] = {
        "schema": "section_4_2_stats_from_products/v1",
        "repo_root": str(repo_root),
        "figure_id": args.figure_id,
        "source_preference": preference,
        "release_dir": str(release_dir),
        "provenance": provenance,
        "warnings": warnings,
        "main": {
            "summary": main_summary,
            "sources": _records(main),
        },
        "climatology": {
            "summary": clim_summary,
            "sources": _records(climatology),
        },
        "satellite": {
            "summary": sat_summary,
            "sources": _records(satellite),
        },
        "section_4_2_named_claims": _named_claims(main, climatology, satellite),
        "counting_policy": {
            "main_station_count": "unique_station_count is unique cluster_uid only when source_station_catalog.csv is available; source_summed_station_count is the sum of per-source station/cluster counts.",
            "main_record_count": "From the selected precomputed product. Figure 5 uses summed source_station_catalog.n_records; stats_release main source contribution uses attributed source-station records.",
            "climatology": "Figure 5 uses unique station_uid per source and climatology_catalog.time for first/last year.",
            "satellite": "Figure 5 uses unique cluster_uid (or satellite_station_uid fallback) per source and sums n_records.",
        },
    }
    return result, main, climatology, satellite


# -----------------------------------------------------------------------------
# Output
# -----------------------------------------------------------------------------


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _source_table_markdown(df: pd.DataFrame, max_rows: int = 50) -> str:
    if df.empty:
        return "_No rows._"
    cols = [c for c in ["source_name_display", "station_count", "source_station_count", "record_count", "first_year", "last_year"] if c in df.columns]
    view = df[cols].copy().sort_values(["record_count", "source_name_display"], ascending=[False, True], na_position="last").head(max_rows)
    headers = {
        "source_name_display": "source",
        "station_count": "stations",
        "source_station_count": "source stations",
        "record_count": "records",
        "first_year": "first year",
        "last_year": "last year",
    }
    lines = ["| " + " | ".join(headers[c] for c in cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, row in view.iterrows():
        cells = []
        for c in cols:
            if c == "source_name_display":
                cells.append(_clean_text(row[c]))
            else:
                cells.append(_fmt_int(row[c]))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _summary_lines(title: str, summary: Dict[str, Optional[int]]) -> List[str]:
    return [
        f"### {title}",
        "",
        f"- Source datasets: {_fmt_int(summary.get('source_dataset_count'))}",
        f"- Unique stations (when reconstructable): {_fmt_int(summary.get('unique_station_count'))}",
        f"- Source-summed stations: {_fmt_int(summary.get('source_summed_station_count'))}",
        f"- Records: {_fmt_int(summary.get('record_count'))}",
        f"- Temporal span: {_fmt_int(summary.get('first_year'))}–{_fmt_int(summary.get('last_year'))}",
        "",
    ]



def _source_row(df: pd.DataFrame, name: str) -> Dict[str, object]:
    if df.empty:
        return {}
    hit = df[df["source_name"].astype(str).eq(name)]
    return hit.iloc[0].to_dict() if not hit.empty else {}


def _fmt_sci(value: object, digits: int = 1) -> str:
    """Format a number in scientific-notation style: 1.7 × 10⁶."""
    n = _as_int(value)
    if n is None:
        return "NA"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.{digits}f} × 10⁶"
    if n >= 1_000:
        return f"{n / 1_000:.{digits}f} × 10³"
    return f"{n:,}"


def _generate_section_4_2_narrative(result: Dict[str, object]) -> str:
    """Produce the Section 4.2 narrative with values filled from extracted data."""
    m = result["main"]["summary"]
    c = result["climatology"]["summary"]
    s = result["satellite"]["summary"]
    main_df = pd.DataFrame(result["main"]["sources"])
    clim_df = pd.DataFrame(result["climatology"]["sources"])
    sat_df = pd.DataFrame(result["satellite"]["sources"])

    def row(df: pd.DataFrame, name: str) -> Dict[str, object]:
        hit = df[df["source_name"].astype(str).eq(name)]
        return hit.iloc[0].to_dict() if not hit.empty else {}

    def sci(df: pd.DataFrame, name: str, field: str = "record_count", digits: int = 1) -> str:
        r = row(df, name)
        return _fmt_sci(r.get(field), digits)

    def stations(df: pd.DataFrame, name: str) -> str:
        r = row(df, name)
        n = _as_int(r.get("station_count"))
        return f"{n:,}" if n is not None else "NA"

    def yr(df: pd.DataFrame, name: str, field: str) -> str:
        r = row(df, name)
        n = _as_int(r.get(field))
        return str(n) if n is not None else "NA"

    main_stations = _fmt_int(m.get("unique_station_count"))
    main_records = _fmt_int(m.get("record_count"))
    main_sources = _fmt_int(m.get("source_dataset_count"))
    main_span = f"{_as_int(m.get('first_year'))}–{_as_int(m.get('last_year'))}"

    clim_stations = _fmt_int(c.get("unique_station_count"))
    clim_sources = _fmt_int(c.get("source_dataset_count"))
    clim_span = f"{_as_int(c.get('first_year'))}–{_as_int(c.get('last_year'))}"

    sat_stations = _fmt_int(s.get("unique_station_count"))
    sat_records = _fmt_int(s.get("record_count"))
    sat_sources = _fmt_int(s.get("source_dataset_count"))
    sat_span = f"{_as_int(s.get('first_year'))}–{_as_int(s.get('last_year'))}"

    # Build narrative
    paras = []
    eused_clause = ""
    if row(main_df, "EUSEDcollab"):
        eused_clause = (
            f"EUSEDcollab provides a substantial contribution of "
            f"{stations(main_df, 'EUSEDcollab')} stations and {sci(main_df, 'EUSEDcollab')} records. "
        )
    paras.append(
        f"Source-level contributions and temporal coverage to the three released components are summarized in Fig. 5. "
        f"The main station-reference matrices integrate {main_stations} stations and {main_records} records "
        f"from {main_sources} source datasets spanning {main_span} and are dominated by a few sources, "
        f"whose station counts and record densities differ markedly. "
        f"GFQA_v2 contributes the largest number of stations ({stations(main_df, 'GFQA_v2')}), "
        f"but a comparatively modest number of records ({sci(main_df, 'GFQA_v2')}). "
        f"USGS NWIS and HYDAT contribute fewer stations ({stations(main_df, 'USGS')} and {stations(main_df, 'HYDAT')}, respectively) "
        f"but much larger numbers of time-resolved records (approximately {sci(main_df, 'USGS')} and {sci(main_df, 'HYDAT')}), "
        f"reflecting their dense daily monitoring archives. "
        f"Bayern similarly contributes only {stations(main_df, 'Bayern')} stations but approximately {sci(main_df, 'Bayern')} records. "
        f"{eused_clause}"
        f"Smaller regional and basin-specific datasets contribute fewer stations but broaden geographic and temporal coverage, "
        f"including early records from the Eurasian River and Chao Phraya River datasets."
    )

    paras.append(
        f"The temporal spans in Fig. 5a show that the main matrices combine a small number of historical or long-running sources "
        f"with a broader set of shorter and more recent datasets. "
        f"The Chao Phraya River dataset provides the earliest coverage, beginning in {yr(main_df, 'Chao_Phraya_River', 'first_year')}, "
        f"followed by the Eurasian River dataset in {yr(main_df, 'Eurasian_River', 'first_year')} "
        f"and HYDAT in {yr(main_df, 'HYDAT', 'first_year')}. "
        f"Bayern, GloRiSe, and USGS NWIS expand coverage from the 1950s onwards, "
        f"while Bayern, USGS NWIS, HYBAM, and several recent basin-specific datasets extend the observational record into the 2020s. "
        f"Overall, the early part of the record is supported by a limited number of historical and national sources, "
        f"whereas the period after approximately 1980 is represented by a broader mixture of agency monitoring networks, "
        f"regional datasets, and project-level observations."
    )

    milliman_stations = stations(clim_df, 'Milliman')
    vanmaercke_stations = stations(clim_df, 'Vanmaercke')
    hma_stations = stations(clim_df, 'HMA')
    huanghe_stations = stations(clim_df, 'Huanghe')
    ali_stations = stations(clim_df, 'Ali_De_Boer')
    vanmaercke_span = f"{yr(clim_df, 'Vanmaercke', 'first_year')}–{yr(clim_df, 'Vanmaercke', 'last_year')}"
    hma_last = yr(clim_df, 'HMA', 'last_year')

    paras.append(
        f"The climatology auxiliary layer contains {clim_stations} stations from {clim_sources} source datasets, "
        f"with reported temporal coverage spanning {clim_span}. "
        f"It is dominated by Milliman and Vanmaercke, which provide {milliman_stations} and {vanmaercke_stations} stations, respectively, "
        f"with smaller contributions from HMA, Huanghe, and Ali and De Boer. "
        f"Vanmaercke provides the longest temporal coverage ({vanmaercke_span}), "
        f"while HMA extends regional sediment records to {hma_last}. "
        f"Huanghe and Ali and De Boer complement these datasets by summarizing multi-decadal sediment observations from specific river basins. "
        f"However, Milliman does not report time coverage, so it should be interpreted only as background climatological information. "
        f"The satellite-derived auxiliary layer contains {sat_stations} stations and {sat_records} records "
        f"from {sat_sources} datasets spanning {sat_span}, "
        f"with RivSed contributing the largest share, followed by GSED and Dethier."
    )

    return "\n\n".join(paras)

def write_outputs(
    result: Dict[str, object],
    main: pd.DataFrame,
    climatology: pd.DataFrame,
    satellite: pd.DataFrame,
    output_dir: Path,
) -> Dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "section_4_2_stats_from_products.json"
    main_csv = output_dir / "section_4_2_main_sources.csv"
    clim_csv = output_dir / "section_4_2_climatology_sources.csv"
    sat_csv = output_dir / "section_4_2_satellite_sources.csv"
    md_path = output_dir / "section_4_2_stats_from_products.md"

    json_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    _write_csv(main, main_csv)
    _write_csv(climatology, clim_csv)
    _write_csv(satellite, sat_csv)

    lines: List[str] = [
        "# Section 4.2 statistics from precomputed products",
        "",
        "This report is generated without opening the release NetCDF matrices. It reads existing `stats_release` products, Figure 5 plotting data, or the release CSV catalogues used by the Figure 5 helper.",
        "",
    ]
    lines.extend(_summary_lines("Main station-reference source contribution", result["main"]["summary"]))
    lines.append(_source_table_markdown(main))
    lines.append("")
    lines.extend(_summary_lines("Climatology auxiliary layer", result["climatology"]["summary"]))
    lines.append(_source_table_markdown(climatology))
    lines.append("")
    lines.extend(_summary_lines("Satellite-derived auxiliary layer", result["satellite"]["summary"]))
    lines.append(_source_table_markdown(satellite))
    lines.extend(["", "## Provenance", ""])
    for track in ("main", "climatology", "satellite"):
        paths = result["provenance"].get(track, [])
        lines.append(f"- **{track}**: " + (", ".join(f"`{p}`" for p in paths) if paths else "no input found"))
    unique_inputs = result["provenance"].get("unique_count_inputs", [])
    if unique_inputs:
        lines.append("- **unique-count enrichment**: " + ", ".join(f"`{p}`" for p in unique_inputs))
    for note in result["provenance"].get("notes", []):
        lines.append(f"- {note}")
    if result.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in result["warnings"])
    lines.extend(["", "## Section 4.2 Narrative (auto-filled)", ""])
    lines.append(_generate_section_4_2_narrative(result))
    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    return {
        "json": json_path,
        "main_csv": main_csv,
        "climatology_csv": clim_csv,
        "satellite_csv": sat_csv,
        "markdown": md_path,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    default_repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Read Section 4.2 source-contribution values from stats_release/Figure 5 products without NetCDF."
    )
    parser.add_argument(
        "--repo-root",
        default=str(default_repo_root),
        help="sed_data_integration repository root. Default: current directory.",
    )
    parser.add_argument(
        "--release-dir",
        default=str(default_repo_root / "output" / "sed_reference_release_minimal"),
        help="Release CSV directory used by Figure 5. Default: <repo>/output/sed_reference_release_minimal.",
    )
    parser.add_argument(
        "--stats-root",
        default=str(default_repo_root / "output_other" / "stats_release"),
        help="stats_release output root.",
    )
    parser.add_argument(
        "--figure-data-dir",
        default=str(default_repo_root / "figures" / "data"),
        help="Directory containing Figure 5 plotting-data CSVs written by write_plotting_data().",
    )
    parser.add_argument(
        "--figure-id",
        default=DEFAULT_FIGURE_ID,
        help=f"Figure data filename stem. Default: {DEFAULT_FIGURE_ID}.",
    )
    parser.add_argument(
        "--source-preference",
        choices=["auto", "figure-data", "stats", "figure-inputs", "docs"],
        default="auto",
        help="Preferred input family. auto tries Figure plotting CSV -> stats CSV -> Figure raw CSV inputs -> docs Markdown.",
    )
    parser.add_argument(
        "--enrich-unique-counts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also read source_station_catalog.csv to recover unique cluster/station totals when available. Default: true.",
    )
    parser.add_argument(
        "--output-dir",
        default="docs/reports/section_4_2_from_products",
        help="Output directory. Relative paths are resolved from the current shell directory.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any of the main/climatology/satellite tracks cannot be found.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    result, main_df, clim_df, sat_df = collect(args)
    output_dir = Path(args.output_dir).expanduser().resolve()
    outputs = write_outputs(result, main_df, clim_df, sat_df, output_dir)

    print("Section 4.2 product-based extraction complete")
    print(f"  JSON: {outputs['json']}")
    print(f"  Markdown: {outputs['markdown']}")
    print(f"  Main sources: {len(main_df)}")
    print(f"  Climatology sources: {len(clim_df)}")
    print(f"  Satellite sources: {len(sat_df)}")
    for warning in result.get("warnings", []):
        print(f"WARNING: {warning}", file=sys.stderr)

    if args.strict and (main_df.empty or clim_df.empty or sat_df.empty):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
