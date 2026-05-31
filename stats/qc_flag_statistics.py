#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate QC flag summary tables and figures for the sediment reference dataset.

Purpose
-------
This script summarizes final and stage-specific quality-control flags for use in:
  - manuscript Section 3 Quality Control;
  - manuscript Section 5 Uncertainty Assessment;
  - supplementary information.

Default inputs
--------------
The script first tries to read the release master NetCDF:
  scripts_basin_test/output/sed_reference_release/sed_reference_master.nc

If that file is not present, it falls back to the s6 master NetCDF:
  scripts_basin_test/output/s6_basin_merged_all.nc

The optional s6 quality-order CSV is used to recover source_type/source_family:
  scripts_basin_test/output/s6_cluster_quality_order.csv

Outputs
-------
  tables/table_qc_flag_summary.csv
  tables/table_qc_flag_by_source.csv
  tables/table_qc_flag_by_resolution.csv
  tables/table_qc_flag_by_variable.csv
  tables/table_qc_flag_by_year.csv
  tables/table_qc_flag_by_cluster.csv
  tables/table_qc_flag_problem_clusters.csv
  tables/table_qc_health_kpis.csv
  tables/table_qc_stage_effectiveness.csv
  tables/table_qc_issue_hotspots.csv
  tables/table_qc_yearly_trends.csv
  figures/fig_qc_flag_distribution.png
  figures/fig_qc_flag_by_source_type.png
  figures/fig_qc_health_by_resolution.png
  figures/fig_qc_yearly_problem_trends.png
  figures/fig_qc_missing_trends.png
  figures/fig_qc_stage_summary.png
  figures/fig_qc_top_problem_sources.png
  figures/fig_qc_top_problem_clusters.png
  article_qc_flag_report.md

Run
---
  python qc_flag_statistics.py

Optional examples
-----------------
  python qc_flag_statistics.py \
      --input-master-nc scripts_basin_test/output/sed_reference_release/sed_reference_master.nc \
      --quality-order-csv scripts_basin_test/output/s6_cluster_quality_order.csv \
      --tables-dir tables \
      --figures-dir figures
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_SCRIPT_DIR = SCRIPT_DIR.parent  # scripts_basin_test/
if str(PROJECT_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_SCRIPT_DIR))
ROOT_DIR = PROJECT_SCRIPT_DIR.parent  # Output_r/
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline_paths import (
    RELEASE_MASTER_NC,
    S6_MERGED_NC,
    S6_QUALITY_ORDER_CSV,
)
from qc_contract import STANDARD_QC_STAGE_NAMES, STANDARD_QC_STAGE_NAME_TO_SPEC

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - checked in main()
    nc4 = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - figures are optional
    plt = None


PROJECT_ROOT = ROOT_DIR

DEFAULT_RELEASE_MASTER_NC = PROJECT_ROOT / RELEASE_MASTER_NC
DEFAULT_S6_MASTER_NC = PROJECT_ROOT / S6_MERGED_NC
DEFAULT_QUALITY_ORDER_CSV = PROJECT_ROOT / S6_QUALITY_ORDER_CSV
DEFAULT_TABLES_DIR = PROJECT_ROOT / "scripts_basin_test/output_other/qc_flag_statistics/tables"
DEFAULT_FIGURES_DIR = PROJECT_ROOT / "scripts_basin_test/output_other/qc_flag_statistics/figures"
DEFAULT_REPORT_PATH = PROJECT_ROOT / "scripts_basin_test/output_other/qc_flag_statistics/article_qc_flag_report.md"

FLAG_CODES = [0, 1, 2, 3, 8, 9]
FINAL_VARIABLE_ORDER = ["Q", "SSC", "SSL"]
RESOLUTION_ORDER = ["daily", "monthly", "annual", "climatology", "other"]
HOTSPOT_MIN_TOTAL = 100
COMMON_FLAG_MEANINGS = {
    0: "good",
    1: "derived/estimated",
    2: "suspect",
    3: "bad",
    8: "not checked",
    9: "missing",
}

RESOLUTION_CODE_TO_NAME = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}

FINAL_FLAG_SPECS = [
    {
        "qc_level": "final",
        "qc_stage": "final",
        "variable": "Q",
        "flag_variable": "Q_flag",
    },
    {
        "qc_level": "final",
        "qc_stage": "final",
        "variable": "SSC",
        "flag_variable": "SSC_flag",
    },
    {
        "qc_level": "final",
        "qc_stage": "final",
        "variable": "SSL",
        "flag_variable": "SSL_flag",
    },
]

SUMMARY_BASE_COLUMNS = [
    "qc_level",
    "qc_stage",
    "variable",
    "flag_variable",
    "flag",
    "meaning",
    "count",
    "percentage",
    "n_total",
]

STAGE_EFFECTIVENESS_COLUMNS = [
    "qc_stage",
    "variable",
    "flag_variable",
    "n_total",
    "flag0_count",
    "flag0_rate",
    "flag0_meaning",
    "flag1_count",
    "flag1_rate",
    "flag1_meaning",
    "flag2_count",
    "flag2_rate",
    "flag2_meaning",
    "flag3_count",
    "flag3_rate",
    "flag3_meaning",
    "flag8_count",
    "flag8_rate",
    "flag8_meaning",
    "flag9_count",
    "flag9_rate",
    "flag9_meaning",
    "pass_count",
    "pass_rate",
    "derived_or_propagated_count",
    "derived_or_propagated_rate",
    "suspect_count",
    "suspect_rate",
    "bad_count",
    "bad_rate",
    "not_checked_count",
    "not_checked_rate",
    "missing_count",
    "missing_rate",
]

HOTSPOT_COLUMNS = [
    "grouping_level",
    "source_dataset",
    "source_type",
    "temporal_resolution",
    "cluster_uid",
    "cluster_id",
    "variable",
    "flag_variable",
    "n_total",
    "usable_count",
    "problem_count",
    "missing_count",
    "not_checked_count",
    "issue_count",
    "usable_rate",
    "problem_rate",
    "missing_rate",
    "not_checked_rate",
    "issue_rate",
]


# -----------------------------------------------------------------------------
# General helpers
# -----------------------------------------------------------------------------

def _clean_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    try:
        if np.ma.is_masked(value):
            return default
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return default
    return text


def _resolve_path(path_text: str, base: Path = PROJECT_ROOT) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _default_master_nc() -> Path:
    if DEFAULT_RELEASE_MASTER_NC.is_file():
        return DEFAULT_RELEASE_MASTER_NC
    return DEFAULT_S6_MASTER_NC


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _mode_text(values: Iterable[object], default: str = "unknown") -> str:
    cleaned = [_clean_text(v) for v in values]
    cleaned = [v for v in cleaned if v]
    if not cleaned:
        return default
    counts = pd.Series(cleaned).value_counts(dropna=True)
    if counts.empty:
        return default
    return str(counts.index[0])


def _parse_flag_meanings(flag_values: object, flag_meanings: object) -> Dict[int, str]:
    """Parse CF-style flag_values + flag_meanings attributes into a mapping."""
    out: Dict[int, str] = {}
    if flag_values is None or flag_meanings is None:
        return out
    try:
        values = np.asarray(flag_values).reshape(-1)
    except Exception:
        return out
    words = str(flag_meanings).replace(",", " ").split()
    for value, word in zip(values, words):
        try:
            out[int(value)] = str(word).replace("_", " ")
        except Exception:
            continue
    return out


def _meaning_for(flag: int, meaning_map: Mapping[int, str]) -> str:
    if int(flag) in meaning_map:
        return meaning_map[int(flag)]
    if int(flag) in COMMON_FLAG_MEANINGS:
        return COMMON_FLAG_MEANINGS[int(flag)]
    return "unknown"


def _cross_join(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    left = left.copy()
    right = right.copy()
    left["_tmp_cross_key"] = 1
    right["_tmp_cross_key"] = 1
    out = left.merge(right, on="_tmp_cross_key", how="outer")
    return out.drop(columns=["_tmp_cross_key"])


# -----------------------------------------------------------------------------
# NetCDF readers
# -----------------------------------------------------------------------------

def _pad_or_trim(values: np.ndarray, size: int, fill_value: object) -> np.ndarray:
    arr = np.asarray(values).reshape(-1)
    if len(arr) >= size:
        return arr[:size]
    pad = np.full(size - len(arr), fill_value, dtype=arr.dtype if len(arr) else object)
    return np.concatenate([arr, pad])


def _read_int_var(ds, name: str, size: int, fill_value: int) -> np.ndarray:
    if name not in ds.variables:
        return np.full(size, fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    raw = raw.filled(fill_value) if np.ma.isMaskedArray(raw) else raw
    raw = _pad_or_trim(np.asarray(raw), size, fill_value)
    series = pd.to_numeric(pd.Series(raw), errors="coerce").fillna(fill_value)
    return series.astype(np.int64).to_numpy()


def _read_flag_var(ds, name: str, size: int, fill_value: int = 9) -> np.ndarray:
    if name not in ds.variables:
        return np.full(size, fill_value, dtype=np.int16)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    raw = raw.filled(fill_value) if np.ma.isMaskedArray(raw) else raw
    raw = _pad_or_trim(np.asarray(raw), size, fill_value)
    series = pd.to_numeric(pd.Series(raw), errors="coerce").fillna(fill_value)
    return series.astype(np.int16).to_numpy()


def _decode_text_array(values: object, size: int, default: str = "") -> List[str]:
    try:
        arr = np.ma.asarray(values)
        if np.ma.isMaskedArray(arr):
            arr = arr.filled(default)
        arr = np.asarray(arr, dtype=object).reshape(-1)
    except Exception:
        arr = np.asarray([], dtype=object)
    arr = _pad_or_trim(arr, size, default)
    return [_clean_text(item, default=default) for item in arr]


def _read_text_var(ds, name: str, size: int, default: str = "") -> List[str]:
    if name not in ds.variables:
        return [default] * size
    var = ds.variables[name]
    try:
        values = var[:]
    except Exception:
        return [default] * size

    # Handle fixed-width character arrays if they ever appear.
    try:
        values_arr = np.asarray(values)
        if values_arr.dtype.kind in {"S", "U"} and values_arr.ndim > 1:
            values = nc4.chartostring(values_arr)
    except Exception:
        pass

    return _decode_text_array(values, size=size, default=default)


def _read_year_array(ds, size: int) -> np.ndarray:
    if "time" not in ds.variables:
        return np.full(size, -9999, dtype=np.int32)

    t_var = ds.variables["time"]
    try:
        raw = np.ma.asarray(t_var[:]).reshape(-1)
    except Exception:
        return np.full(size, -9999, dtype=np.int32)

    raw = _pad_or_trim(raw, size, np.nan)
    mask = np.ma.getmaskarray(raw)
    raw = np.ma.asarray(raw).filled(np.nan).astype(float)
    valid = (~mask) & np.isfinite(raw)

    years = np.full(size, -9999, dtype=np.int32)
    if not np.any(valid):
        return years

    units = getattr(t_var, "units", "days since 1970-01-01")
    calendar = getattr(t_var, "calendar", "gregorian")
    valid_idx = np.where(valid)[0]
    valid_values = raw[valid_idx]

    try:
        try:
            dates = nc4.num2date(
                valid_values,
                units=units,
                calendar=calendar,
                only_use_cftime_datetimes=False,
            )
        except TypeError:
            dates = nc4.num2date(valid_values, units=units, calendar=calendar)
    except Exception:
        dates = pd.to_datetime(valid_values, unit="D", origin="1970-01-01", errors="coerce")

    for idx, date_value in zip(valid_idx, dates):
        try:
            if hasattr(date_value, "year"):
                years[idx] = int(date_value.year)
            else:
                ts = pd.Timestamp(date_value)
                if not pd.isna(ts):
                    years[idx] = int(ts.year)
        except Exception:
            continue
    return years


def _lookup_by_index(index_arr: np.ndarray, lookup: Sequence[object], default: object) -> List[object]:
    out: List[object] = []
    n = len(lookup)
    for idx in index_arr:
        try:
            i = int(idx)
        except Exception:
            out.append(default)
            continue
        if 0 <= i < n:
            out.append(lookup[i])
        else:
            out.append(default)
    return out


def _stage_label(flag_variable: str) -> str:
    name = str(flag_variable)
    if name.endswith("_qc1"):
        return "physical_plausibility"
    if name.endswith("_qc2"):
        return "log_iqr"
    if name.endswith("_qc3"):
        return "ssc_q_consistency"
    return "stage_specific"


def _stage_variable(flag_variable: str) -> str:
    return str(flag_variable).split("_", 1)[0]


def _stage_meaning_map_from_contract(flag_variable: str) -> Dict[int, str]:
    spec = STANDARD_QC_STAGE_NAME_TO_SPEC.get(flag_variable, {})
    return _parse_flag_meanings(spec.get("flag_values"), spec.get("flag_meanings"))


def read_master_records(
    master_nc: Path,
    include_stage_flags: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, np.ndarray], List[Dict[str, str]], Dict[str, Dict[int, str]]]:
    """Read record-level metadata and QC flags from the master NetCDF."""
    if nc4 is None:
        raise RuntimeError("netCDF4 is required. Install it with: pip install netCDF4")
    if not master_nc.is_file():
        raise FileNotFoundError("Master NetCDF not found: {}".format(master_nc))

    with nc4.Dataset(master_nc, "r") as ds:
        if "n_records" in ds.dimensions:
            n_records = len(ds.dimensions["n_records"])
        elif "station_index" in ds.variables:
            n_records = int(np.asarray(ds.variables["station_index"][:]).size)
        else:
            raise ValueError("Cannot infer n_records from {}".format(master_nc))

        station_index = _read_int_var(ds, "station_index", n_records, fill_value=-1)
        source_station_index = _read_int_var(ds, "source_station_index", n_records, fill_value=-1)
        resolution_code = _read_int_var(ds, "resolution", n_records, fill_value=4)
        source_dataset = _read_text_var(ds, "source", n_records, default="unknown")
        year = _read_year_array(ds, n_records)

        if "n_stations" in ds.dimensions:
            n_stations = len(ds.dimensions["n_stations"])
        else:
            n_stations = int(max(station_index.max() + 1, 0)) if len(station_index) else 0

        cluster_ids = _read_int_var(ds, "cluster_id", n_stations, fill_value=-1)
        if "cluster_uid" in ds.variables:
            cluster_uids = _read_text_var(ds, "cluster_uid", n_stations, default="")
        else:
            cluster_uids = ["SED{:06d}".format(int(cid)) if int(cid) >= 0 else "" for cid in cluster_ids]

        record_cluster_ids = _lookup_by_index(station_index, cluster_ids.tolist(), default=-1)
        record_cluster_uids = _lookup_by_index(station_index, cluster_uids, default="")

        records = pd.DataFrame(
            {
                "record_index": np.arange(n_records, dtype=np.int64),
                "station_index": station_index.astype(np.int64),
                "cluster_id": pd.to_numeric(pd.Series(record_cluster_ids), errors="coerce").fillna(-1).astype(np.int64),
                "cluster_uid": [_clean_text(x) for x in record_cluster_uids],
                "source_station_index": source_station_index.astype(np.int64),
                "source_dataset": [_clean_text(x, default="unknown") for x in source_dataset],
                "temporal_resolution": [RESOLUTION_CODE_TO_NAME.get(int(x), "other") for x in resolution_code],
                "resolution_code": resolution_code.astype(np.int16),
                "year": year.astype(np.int32),
            }
        )
        records["source_type"] = "unknown"

        flag_arrays: Dict[str, np.ndarray] = {}
        flag_specs: List[Dict[str, str]] = []
        meaning_maps: Dict[str, Dict[int, str]] = {}

        for spec in FINAL_FLAG_SPECS:
            flag_variable = spec["flag_variable"]
            if flag_variable not in ds.variables:
                raise ValueError(
                    "Required final QC flag variable '{}' is missing from {}".format(
                        flag_variable, master_nc
                    )
                )
            flag_arrays[flag_variable] = _read_flag_var(ds, flag_variable, n_records, fill_value=9)
            flag_specs.append(dict(spec))
            # Use the manuscript-facing final-flag vocabulary requested for this table.
            meaning_maps[flag_variable] = dict(COMMON_FLAG_MEANINGS)

        if include_stage_flags:
            for flag_variable in STANDARD_QC_STAGE_NAMES:
                if flag_variable not in ds.variables:
                    continue
                fill_value = int(
                    STANDARD_QC_STAGE_NAME_TO_SPEC.get(flag_variable, {}).get("fill_value", 9)
                )
                flag_arrays[flag_variable] = _read_flag_var(ds, flag_variable, n_records, fill_value=fill_value)
                flag_specs.append(
                    {
                        "qc_level": "stage",
                        "qc_stage": _stage_label(flag_variable),
                        "variable": _stage_variable(flag_variable),
                        "flag_variable": flag_variable,
                    }
                )
                var = ds.variables[flag_variable]
                meaning_map = _parse_flag_meanings(
                    getattr(var, "flag_values", None),
                    getattr(var, "flag_meanings", None),
                )
                if not meaning_map:
                    meaning_map = _stage_meaning_map_from_contract(flag_variable)
                meaning_maps[flag_variable] = meaning_map or dict(COMMON_FLAG_MEANINGS)

    return records, flag_arrays, flag_specs, meaning_maps


# -----------------------------------------------------------------------------
# Provenance enrichment
# -----------------------------------------------------------------------------

def attach_source_type(records: pd.DataFrame, quality_order_csv: Optional[Path]) -> pd.DataFrame:
    """Attach source_type/source_family using s6_cluster_quality_order.csv where possible."""
    records = records.copy()
    records["source_type"] = "unknown"

    if quality_order_csv is None or not quality_order_csv.is_file():
        print(
            "Warning: quality-order CSV not found; source_type will be 'unknown': {}".format(
                quality_order_csv
            ),
            file=sys.stderr,
        )
        return records

    quality = pd.read_csv(quality_order_csv)
    required = {"source", "source_station_index", "source_family"}
    if not required.issubset(set(quality.columns)):
        print(
            "Warning: quality-order CSV lacks {}; source_type will be 'unknown': {}".format(
                sorted(required - set(quality.columns)), quality_order_csv
            ),
            file=sys.stderr,
        )
        return records

    quality = quality.loc[:, ["source", "source_station_index", "source_family"]].copy()
    quality["source"] = quality["source"].map(lambda x: _clean_text(x, default="unknown"))
    quality["source_family"] = quality["source_family"].map(lambda x: _clean_text(x, default="unknown"))
    quality["source_station_index"] = pd.to_numeric(
        quality["source_station_index"], errors="coerce"
    ).fillna(-1).astype(np.int64)

    pair_lookup = (
        quality.groupby(["source_station_index", "source"], dropna=False)["source_family"]
        .agg(_mode_text)
        .reset_index()
        .rename(columns={"source_family": "source_type_pair"})
    )

    out = records.merge(
        pair_lookup,
        how="left",
        left_on=["source_station_index", "source_dataset"],
        right_on=["source_station_index", "source"],
    )
    if "source" in out.columns:
        out = out.drop(columns=["source"])

    source_lookup = (
        quality.groupby("source", dropna=False)["source_family"]
        .agg(_mode_text)
        .to_dict()
    )
    source_fallback = out["source_dataset"].map(source_lookup)

    out["source_type"] = out["source_type_pair"].fillna(source_fallback).fillna("unknown")
    out["source_type"] = out["source_type"].map(lambda x: _clean_text(x, default="unknown"))
    out = out.drop(columns=["source_type_pair"])
    return out


# -----------------------------------------------------------------------------
# Statistics
# -----------------------------------------------------------------------------

def _normal_flag_codes(flag_array: np.ndarray) -> List[int]:
    observed = pd.to_numeric(pd.Series(flag_array), errors="coerce").dropna().astype(int).unique().tolist()
    return sorted(set(FLAG_CODES).union(set(observed)))


def summarize_one_flag(
    records: pd.DataFrame,
    flag_array: np.ndarray,
    spec: Mapping[str, str],
    meaning_map: Mapping[int, str],
    group_cols: Sequence[str],
) -> pd.DataFrame:
    """Summarize one flag variable for the requested grouping columns."""
    missing_cols = [col for col in group_cols if col not in records.columns]
    if missing_cols:
        raise KeyError("Missing grouping columns: {}".format(missing_cols))

    group_cols = list(group_cols)
    flag_codes = _normal_flag_codes(flag_array)
    flag_df = pd.DataFrame({"flag": flag_codes})

    if group_cols:
        work = records.loc[:, group_cols].copy()
    else:
        work = pd.DataFrame(index=records.index)
    work["flag"] = pd.to_numeric(pd.Series(flag_array), errors="coerce").fillna(9).astype(np.int16).to_numpy()

    if group_cols:
        counts = (
            work.groupby(group_cols + ["flag"], dropna=False)
            .size()
            .reset_index(name="count")
        )
        groups = work.loc[:, group_cols].drop_duplicates()
        totals = (
            work.groupby(group_cols, dropna=False)
            .size()
            .reset_index(name="n_total")
        )
        grid = _cross_join(groups, flag_df)
        out = grid.merge(counts, how="left", on=group_cols + ["flag"])
        out = out.merge(totals, how="left", on=group_cols)
    else:
        counts = work.groupby("flag", dropna=False).size().reset_index(name="count")
        out = flag_df.merge(counts, how="left", on="flag")
        out["n_total"] = len(work)

    out["count"] = out["count"].fillna(0).astype(np.int64)
    out["n_total"] = out["n_total"].fillna(0).astype(np.int64)
    out["percentage"] = np.where(
        out["n_total"] > 0,
        out["count"].astype(float) / out["n_total"].astype(float) * 100.0,
        0.0,
    )
    out["percentage"] = out["percentage"].round(6)

    out["qc_level"] = spec["qc_level"]
    out["qc_stage"] = spec["qc_stage"]
    out["variable"] = spec["variable"]
    out["flag_variable"] = spec["flag_variable"]
    out["meaning"] = out["flag"].map(lambda x: _meaning_for(int(x), meaning_map))

    return out.loc[:, list(group_cols) + SUMMARY_BASE_COLUMNS]


def summarize_flags(
    records: pd.DataFrame,
    flag_arrays: Mapping[str, np.ndarray],
    flag_specs: Sequence[Mapping[str, str]],
    meaning_maps: Mapping[str, Mapping[int, str]],
    group_cols: Sequence[str],
) -> pd.DataFrame:
    parts = []
    for spec in flag_specs:
        flag_variable = spec["flag_variable"]
        if flag_variable not in flag_arrays:
            continue
        parts.append(
            summarize_one_flag(
                records=records,
                flag_array=flag_arrays[flag_variable],
                spec=spec,
                meaning_map=meaning_maps.get(flag_variable, COMMON_FLAG_MEANINGS),
                group_cols=group_cols,
            )
        )
    if not parts:
        return pd.DataFrame(columns=list(group_cols) + SUMMARY_BASE_COLUMNS)
    out = pd.concat(parts, ignore_index=True)
    sort_cols = list(group_cols) + ["qc_level", "qc_stage", "variable", "flag_variable", "flag"]
    return out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)


def _problem_cluster_table(by_cluster: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Rank clusters by final suspect+bad+missing percentage."""
    if by_cluster.empty:
        return pd.DataFrame()

    final = by_cluster.loc[by_cluster["qc_level"].eq("final")].copy()
    if final.empty:
        return pd.DataFrame()

    final["is_problem_flag"] = final["flag"].isin([2, 3, 9])
    agg = (
        final.groupby(["cluster_uid", "cluster_id", "variable", "flag_variable"], dropna=False)
        .apply(
            lambda g: pd.Series(
                {
                    "problem_count": int(g.loc[g["is_problem_flag"], "count"].sum()),
                    "n_total": int(g["n_total"].max()) if len(g) else 0,
                }
            )
        )
        .reset_index()
    )
    agg["problem_percentage"] = np.where(
        agg["n_total"] > 0,
        agg["problem_count"].astype(float) / agg["n_total"].astype(float) * 100.0,
        0.0,
    )
    agg["problem_percentage"] = agg["problem_percentage"].round(6)
    agg = agg.sort_values(
        ["problem_percentage", "problem_count", "n_total"],
        ascending=[False, False, False],
        kind="mergesort",
    )
    if top_n and top_n > 0:
        agg = agg.head(int(top_n))
    return agg.reset_index(drop=True)


def _final_only(table: pd.DataFrame) -> pd.DataFrame:
    if table.empty or "qc_level" not in table.columns:
        return pd.DataFrame()
    return table.loc[table["qc_level"].eq("final")].copy()


def _count_for_flag(group: pd.DataFrame, flag: int) -> int:
    rows = group.loc[group["flag"].eq(int(flag)), "count"]
    if rows.empty:
        return 0
    return int(rows.sum())


def _rate(count: object, total: object) -> float:
    try:
        total_value = float(total)
        if total_value <= 0:
            return 0.0
        return round(float(count) / total_value * 100.0, 6)
    except Exception:
        return 0.0


def _rate_row_from_group(group: pd.DataFrame, keys: Mapping[str, object]) -> Dict[str, object]:
    n_total = int(group["n_total"].max()) if len(group) else 0
    good = _count_for_flag(group, 0)
    derived = _count_for_flag(group, 1)
    suspect = _count_for_flag(group, 2)
    bad = _count_for_flag(group, 3)
    not_checked = _count_for_flag(group, 8)
    missing = _count_for_flag(group, 9)
    usable = good + derived
    problem = suspect + bad
    issue = problem + missing
    row: Dict[str, object] = dict(keys)
    row.update(
        {
            "n_total": n_total,
            "good_count": good,
            "derived_count": derived,
            "suspect_count": suspect,
            "bad_count": bad,
            "not_checked_count": not_checked,
            "missing_count": missing,
            "usable_count": usable,
            "problem_count": problem,
            "issue_count": issue,
            "good_rate": _rate(good, n_total),
            "derived_rate": _rate(derived, n_total),
            "suspect_rate": _rate(suspect, n_total),
            "bad_rate": _rate(bad, n_total),
            "not_checked_rate": _rate(not_checked, n_total),
            "missing_rate": _rate(missing, n_total),
            "usable_rate": _rate(usable, n_total),
            "problem_rate": _rate(problem, n_total),
            "issue_rate": _rate(issue, n_total),
        }
    )
    return row


def _rate_table(
    table: pd.DataFrame,
    group_cols: Sequence[str],
    min_total: int = 0,
) -> pd.DataFrame:
    final = _final_only(table)
    if final.empty:
        return pd.DataFrame()

    rows: List[Dict[str, object]] = []
    group_cols = list(group_cols)
    for keys, group in final.groupby(group_cols + ["variable", "flag_variable"], dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        key_map = dict(zip(group_cols + ["variable", "flag_variable"], keys))
        row = _rate_row_from_group(group, key_map)
        if int(row["n_total"]) >= int(min_total):
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    sort_cols = [col for col in group_cols + ["variable"] if col in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="mergesort")
    return out.reset_index(drop=True)


def build_qc_health_kpis(by_resolution: pd.DataFrame) -> pd.DataFrame:
    out = _rate_table(by_resolution, ["temporal_resolution"])
    if out.empty:
        return out
    out["temporal_resolution"] = pd.Categorical(
        out["temporal_resolution"],
        categories=RESOLUTION_ORDER,
        ordered=True,
    )
    out["variable"] = pd.Categorical(out["variable"], categories=FINAL_VARIABLE_ORDER, ordered=True)
    return out.sort_values(["temporal_resolution", "variable"], kind="mergesort").reset_index(drop=True)


def build_qc_yearly_trends(by_year: pd.DataFrame) -> pd.DataFrame:
    out = _rate_table(by_year, ["year", "temporal_resolution"])
    if out.empty:
        return out
    out["year"] = pd.to_numeric(out["year"], errors="coerce").fillna(-9999).astype(np.int32)
    out = out.loc[out["year"] > 0].copy()
    out["temporal_resolution"] = pd.Categorical(
        out["temporal_resolution"],
        categories=RESOLUTION_ORDER,
        ordered=True,
    )
    out["variable"] = pd.Categorical(out["variable"], categories=FINAL_VARIABLE_ORDER, ordered=True)
    return out.sort_values(["year", "temporal_resolution", "variable"], kind="mergesort").reset_index(drop=True)


def build_qc_stage_effectiveness(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame(columns=STAGE_EFFECTIVENESS_COLUMNS)
    stage = summary.loc[summary["qc_level"].eq("stage")].copy()
    if stage.empty:
        return pd.DataFrame(columns=STAGE_EFFECTIVENESS_COLUMNS)

    rows: List[Dict[str, object]] = []
    group_cols = ["qc_stage", "variable", "flag_variable"]
    for keys, group in stage.groupby(group_cols, dropna=False, sort=True):
        key_map = dict(zip(group_cols, keys))
        n_total = int(group["n_total"].max()) if len(group) else 0
        row: Dict[str, object] = dict(key_map)
        row["n_total"] = n_total
        for flag in FLAG_CODES:
            count = _count_for_flag(group, flag)
            row["flag{}_count".format(flag)] = count
            row["flag{}_rate".format(flag)] = _rate(count, n_total)
            meanings = group.loc[group["flag"].eq(flag), "meaning"]
            row["flag{}_meaning".format(flag)] = meanings.iloc[0] if len(meanings) else _meaning_for(flag, COMMON_FLAG_MEANINGS)

        row["pass_count"] = row["flag0_count"]
        row["pass_rate"] = row["flag0_rate"]
        row["derived_or_propagated_count"] = row["flag1_count"]
        row["derived_or_propagated_rate"] = row["flag1_rate"]
        row["suspect_count"] = row["flag2_count"]
        row["suspect_rate"] = row["flag2_rate"]
        row["bad_count"] = row["flag3_count"]
        row["bad_rate"] = row["flag3_rate"]
        row["not_checked_count"] = row["flag8_count"]
        row["not_checked_rate"] = row["flag8_rate"]
        row["missing_count"] = row["flag9_count"]
        row["missing_rate"] = row["flag9_rate"]
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=STAGE_EFFECTIVENESS_COLUMNS)
    return (
        out.reindex(columns=STAGE_EFFECTIVENESS_COLUMNS)
        .sort_values(["qc_stage", "variable", "flag_variable"], kind="mergesort")
        .reset_index(drop=True)
    )


def _hotspot_rows_from_rate_table(
    rate_table: pd.DataFrame,
    grouping_level: str,
    id_cols: Sequence[str],
) -> pd.DataFrame:
    if rate_table.empty:
        return pd.DataFrame()
    out = rate_table.copy()
    out.insert(0, "grouping_level", grouping_level)
    for col in ["source_dataset", "source_type", "temporal_resolution", "cluster_uid", "cluster_id"]:
        if col not in out.columns:
            out[col] = ""
    keep_cols = list(HOTSPOT_COLUMNS)
    for col in id_cols:
        if col not in keep_cols and col in out.columns:
            keep_cols.insert(1, col)
    return out.loc[:, keep_cols]


def build_qc_issue_hotspots(
    by_source: pd.DataFrame,
    by_resolution: pd.DataFrame,
    by_cluster: pd.DataFrame,
    min_total: int = HOTSPOT_MIN_TOTAL,
) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []

    source_rates = _rate_table(by_source, ["source_dataset", "source_type"], min_total=min_total)
    parts.append(_hotspot_rows_from_rate_table(source_rates, "source", ["source_dataset", "source_type"]))

    final_source = _final_only(by_source)
    if not final_source.empty:
        grouped = (
            final_source.groupby(["source_type", "variable", "flag_variable", "flag"], dropna=False, as_index=False)["count"]
            .sum()
        )
        totals = grouped.groupby(["source_type", "variable", "flag_variable"], dropna=False)["count"].sum().reset_index(name="n_total")
        source_type_table = grouped.merge(totals, how="left", on=["source_type", "variable", "flag_variable"])
        source_type_table["percentage"] = np.where(
            source_type_table["n_total"] > 0,
            source_type_table["count"].astype(float) / source_type_table["n_total"].astype(float) * 100.0,
            0.0,
        )
        source_type_rates = _rate_table(source_type_table.assign(qc_level="final"), ["source_type"], min_total=min_total)
        parts.append(_hotspot_rows_from_rate_table(source_type_rates, "source_type", ["source_type"]))

    resolution_rates = _rate_table(by_resolution, ["temporal_resolution"], min_total=min_total)
    parts.append(_hotspot_rows_from_rate_table(resolution_rates, "resolution", ["temporal_resolution"]))

    cluster_rates = _rate_table(by_cluster, ["cluster_uid", "cluster_id"], min_total=min_total)
    parts.append(_hotspot_rows_from_rate_table(cluster_rates, "cluster", ["cluster_uid", "cluster_id"]))

    parts = [part for part in parts if part is not None and not part.empty]
    if not parts:
        return pd.DataFrame(columns=HOTSPOT_COLUMNS)
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(
        ["issue_rate", "issue_count", "problem_rate", "n_total"],
        ascending=[False, False, False, False],
        kind="mergesort",
    ).reset_index(drop=True)


# -----------------------------------------------------------------------------
# Figures
# -----------------------------------------------------------------------------

def _plot_stacked_percent(
    table: pd.DataFrame,
    index_col: str,
    output_path: Path,
    title: str,
    xlabel: str,
    ylabel: str = "Percentage of records (%)",
    flag_order: Sequence[int] = FLAG_CODES,
    label_rotation: int = 0,
) -> None:
    if plt is None:
        print("Warning: matplotlib is not installed; skipping figure {}".format(output_path), file=sys.stderr)
        return
    if table.empty:
        print("Warning: empty table; skipping figure {}".format(output_path), file=sys.stderr)
        return

    pivot = (
        table.pivot_table(index=index_col, columns="flag", values="percentage", aggfunc="sum", observed=False)
        .fillna(0.0)
    )
    if pivot.empty:
        print("Warning: no plottable data; skipping figure {}".format(output_path), file=sys.stderr)
        return

    ordered_flags = [flag for flag in flag_order if flag in pivot.columns]
    extra_flags = [flag for flag in pivot.columns if flag not in ordered_flags]
    ordered_flags.extend(sorted(extra_flags))
    pivot = pivot.loc[:, ordered_flags]

    x = np.arange(len(pivot.index))
    fig_width = max(7.0, min(18.0, 0.6 * len(pivot.index) + 4.5))
    fig, ax = plt.subplots(figsize=(fig_width, 5.0))
    bottom = np.zeros(len(pivot.index), dtype=float)
    for flag in ordered_flags:
        values = pivot[flag].to_numpy(dtype=float)
        ax.bar(x, values, bottom=bottom, label="{}: {}".format(flag, COMMON_FLAG_MEANINGS.get(int(flag), "unknown")))
        bottom += values

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, max(100.0, float(np.nanmax(bottom)) if len(bottom) else 100.0))
    ax.set_xticks(x)
    ax.set_xticklabels([str(item) for item in pivot.index], rotation=label_rotation, ha="right" if label_rotation else "center")
    ax.legend(title="Flag", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0.0)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _save_figure(fig, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _plot_stacked_rate_columns(
    table: pd.DataFrame,
    label_col: str,
    rate_cols: Sequence[str],
    labels: Sequence[str],
    output_path: Path,
    title: str,
    xlabel: str = "",
    ylabel: str = "Percentage of records (%)",
    label_rotation: int = 45,
) -> None:
    if plt is None:
        print("Warning: matplotlib is not installed; skipping figure {}".format(output_path), file=sys.stderr)
        return
    if table.empty:
        print("Warning: empty table; skipping figure {}".format(output_path), file=sys.stderr)
        return

    work = table.copy()
    x = np.arange(len(work))
    fig_width = max(8.0, min(20.0, 0.6 * len(work) + 4.5))
    fig, ax = plt.subplots(figsize=(fig_width, 5.5))
    bottom = np.zeros(len(work), dtype=float)
    for col, label in zip(rate_cols, labels):
        if col not in work.columns:
            continue
        values = pd.to_numeric(work[col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
        ax.bar(x, values, bottom=bottom, label=label)
        bottom += values

    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, max(100.0, float(np.nanmax(bottom)) if len(bottom) else 100.0))
    ax.set_xticks(x)
    ax.set_xticklabels(
        [str(item) for item in work[label_col]],
        rotation=label_rotation,
        ha="right" if label_rotation else "center",
    )
    ax.legend(title="Metric", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0.0)
    _save_figure(fig, output_path)


def _plot_yearly_rate_trends(
    yearly: pd.DataFrame,
    rate_col: str,
    output_path: Path,
    title: str,
    ylabel: str,
) -> None:
    if plt is None:
        print("Warning: matplotlib is not installed; skipping figure {}".format(output_path), file=sys.stderr)
        return
    if yearly.empty or rate_col not in yearly.columns:
        print("Warning: empty yearly table; skipping figure {}".format(output_path), file=sys.stderr)
        return

    work = yearly.copy()
    work["year"] = pd.to_numeric(work["year"], errors="coerce")
    work = work.loc[work["year"].notna() & (work["year"] > 0)].copy()
    if work.empty:
        print("Warning: no valid years; skipping figure {}".format(output_path), file=sys.stderr)
        return
    work["series_label"] = work["temporal_resolution"].astype(str) + " " + work["variable"].astype(str)

    fig, ax = plt.subplots(figsize=(12.0, 6.0))
    for label, group in work.groupby("series_label", sort=True):
        group = group.sort_values("year")
        ax.plot(
            group["year"].to_numpy(dtype=float),
            pd.to_numeric(group[rate_col], errors="coerce").fillna(0.0).to_numpy(dtype=float),
            linewidth=1.7,
            marker="o",
            markersize=2.5,
            label=str(label),
        )
    ax.set_title(title)
    ax.set_xlabel("Year")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0.0)
    ax.grid(True, axis="y", alpha=0.25)
    ax.legend(title="Resolution / variable", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0.0)
    _save_figure(fig, output_path)


def _plot_stage_summary(stage_effectiveness: pd.DataFrame, output_path: Path) -> None:
    if stage_effectiveness.empty:
        print("Warning: empty stage-effectiveness table; skipping figure {}".format(output_path), file=sys.stderr)
        return
    work = stage_effectiveness.copy()
    work["stage_variable"] = work["qc_stage"].astype(str) + "\n" + work["variable"].astype(str)
    _plot_stacked_rate_columns(
        table=work,
        label_col="stage_variable",
        rate_cols=["flag0_rate", "flag1_rate", "flag2_rate", "flag3_rate", "flag8_rate", "flag9_rate"],
        labels=["flag 0", "flag 1", "flag 2", "flag 3", "flag 8", "flag 9"],
        output_path=output_path,
        title="Stage-specific QC flag distribution",
        xlabel="QC stage and variable",
    )


def _plot_top_hotspots(
    hotspots: pd.DataFrame,
    grouping_level: str,
    output_path: Path,
    top_n: int,
    title: str,
) -> None:
    if plt is None:
        print("Warning: matplotlib is not installed; skipping figure {}".format(output_path), file=sys.stderr)
        return
    if hotspots.empty:
        print("Warning: empty hotspot table; skipping figure {}".format(output_path), file=sys.stderr)
        return

    work = hotspots.loc[hotspots["grouping_level"].eq(grouping_level)].copy()
    if work.empty:
        print("Warning: no {} hotspots; skipping figure {}".format(grouping_level, output_path), file=sys.stderr)
        return
    work = work.sort_values(
        ["issue_rate", "issue_count", "problem_rate", "n_total"],
        ascending=[False, False, False, False],
        kind="mergesort",
    ).head(max(1, int(top_n))).copy()

    if grouping_level == "source":
        group_label = work["source_dataset"].astype(str) + "\n" + work["variable"].astype(str)
    elif grouping_level == "cluster":
        group_label = work["cluster_uid"].astype(str) + "\n" + work["variable"].astype(str)
    else:
        group_label = work[grouping_level].astype(str) + "\n" + work["variable"].astype(str)
    work["plot_label"] = group_label
    work = work.iloc[::-1].reset_index(drop=True)

    y = np.arange(len(work))
    missing = pd.to_numeric(work["missing_rate"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    problem = pd.to_numeric(work["problem_rate"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    fig_height = max(5.0, min(14.0, 0.45 * len(work) + 2.0))
    fig, ax = plt.subplots(figsize=(10.0, fig_height))
    ax.barh(y, missing, label="missing")
    ax.barh(y, problem, left=missing, label="suspect + bad")
    ax.set_yticks(y)
    ax.set_yticklabels([str(item) for item in work["plot_label"]])
    ax.set_xlabel("Issue rate (%)")
    ax.set_title(title)
    ax.set_xlim(0, max(100.0, float(np.nanmax(missing + problem)) if len(work) else 100.0))
    ax.grid(True, axis="x", alpha=0.25)
    ax.legend(title="Issue component")
    _save_figure(fig, output_path)


def write_figures(
    records: pd.DataFrame,
    flag_arrays: Mapping[str, np.ndarray],
    flag_specs: Sequence[Mapping[str, str]],
    meaning_maps: Mapping[str, Mapping[int, str]],
    summary: pd.DataFrame,
    by_source: pd.DataFrame,
    health_kpis: pd.DataFrame,
    yearly_trends: pd.DataFrame,
    stage_effectiveness: pd.DataFrame,
    issue_hotspots: pd.DataFrame,
    figures_dir: Path,
    top_n_sources: int,
    top_n_clusters: int,
) -> None:
    figures_dir = _ensure_dir(figures_dir)

    final_summary = summary.loc[summary["qc_level"].eq("final")].copy()
    final_summary["flag_variable"] = pd.Categorical(
        final_summary["flag_variable"],
        categories=["Q_flag", "SSC_flag", "SSL_flag"],
        ordered=True,
    )
    final_summary = final_summary.sort_values(["flag_variable", "flag"])

    _plot_stacked_percent(
        table=final_summary,
        index_col="flag_variable",
        output_path=figures_dir / "fig_qc_flag_distribution.png",
        title="Distribution of final QC flags",
        xlabel="Final QC flag variable",
        label_rotation=0,
    )

    final_by_source_type = by_source.loc[by_source["qc_level"].eq("final")].copy()
    if not final_by_source_type.empty:
        grouped = (
            final_by_source_type.groupby(["source_type", "variable", "flag"], dropna=False, as_index=False)["count"]
            .sum()
        )
        totals = (
            grouped.groupby(["source_type", "variable"], dropna=False)["count"]
            .sum()
            .reset_index(name="n_total")
        )
        grouped = grouped.merge(totals, how="left", on=["source_type", "variable"])
        grouped["percentage"] = np.where(
            grouped["n_total"] > 0,
            grouped["count"].astype(float) / grouped["n_total"].astype(float) * 100.0,
            0.0,
        )
        grouped["source_type_variable"] = grouped["source_type"].astype(str) + "\n" + grouped["variable"].astype(str)
        grouped = grouped.sort_values(["source_type", "variable", "flag"], kind="mergesort")
    else:
        grouped = pd.DataFrame(columns=["source_type_variable", "flag", "percentage"])

    _plot_stacked_percent(
        table=grouped,
        index_col="source_type_variable",
        output_path=figures_dir / "fig_qc_flag_by_source_type.png",
        title="Final QC flag distribution by source type",
        xlabel="Source type and variable",
        label_rotation=45,
    )

    if not health_kpis.empty:
        health = health_kpis.copy()
        health["resolution_variable"] = health["temporal_resolution"].astype(str) + "\n" + health["variable"].astype(str) + "\nn=" + health["n_total"].apply(lambda x: f"{int(x):,}")
        _plot_stacked_rate_columns(
            table=health,
            label_col="resolution_variable",
            rate_cols=["usable_rate", "problem_rate", "missing_rate", "not_checked_rate"],
            labels=["usable (0+1)", "problem (2+3)", "missing (9)", "not checked (8)"],
            output_path=figures_dir / "fig_qc_health_by_resolution.png",
            title="Final QC health by product resolution",
            xlabel="Resolution and variable",
        )

    _plot_yearly_rate_trends(
        yearly=yearly_trends,
        rate_col="problem_rate",
        output_path=figures_dir / "fig_qc_yearly_problem_trends.png",
        title="Yearly final QC problem-rate trends",
        ylabel="Problem rate: suspect + bad (%)",
    )
    _plot_yearly_rate_trends(
        yearly=yearly_trends,
        rate_col="missing_rate",
        output_path=figures_dir / "fig_qc_missing_trends.png",
        title="Yearly final QC missing-rate trends",
        ylabel="Missing rate (%)",
    )
    _plot_stage_summary(
        stage_effectiveness=stage_effectiveness,
        output_path=figures_dir / "fig_qc_stage_summary.png",
    )
    _plot_top_hotspots(
        hotspots=issue_hotspots,
        grouping_level="source",
        output_path=figures_dir / "fig_qc_top_problem_sources.png",
        top_n=top_n_sources,
        title="Top source-level QC issue hotspots",
    )
    _plot_top_hotspots(
        hotspots=issue_hotspots,
        grouping_level="cluster",
        output_path=figures_dir / "fig_qc_top_problem_clusters.png",
        top_n=top_n_clusters,
        title="Top cluster-level QC issue hotspots",
    )


# -----------------------------------------------------------------------------
# Output orchestration
# -----------------------------------------------------------------------------

def write_tables(
    records: pd.DataFrame,
    flag_arrays: Mapping[str, np.ndarray],
    flag_specs: Sequence[Mapping[str, str]],
    meaning_maps: Mapping[str, Mapping[int, str]],
    tables_dir: Path,
    problem_cluster_top_n: int,
    hotspot_min_total: int = HOTSPOT_MIN_TOTAL,
) -> Dict[str, pd.DataFrame]:
    tables_dir = _ensure_dir(tables_dir)

    tables: Dict[str, pd.DataFrame] = {}
    table_plan = [
        ("summary", "table_qc_flag_summary.csv", []),
        ("by_source", "table_qc_flag_by_source.csv", ["source_dataset", "source_type"]),
        ("by_resolution", "table_qc_flag_by_resolution.csv", ["temporal_resolution"]),
        ("by_variable", "table_qc_flag_by_variable.csv", []),
        ("by_year", "table_qc_flag_by_year.csv", ["year", "temporal_resolution"]),
        ("by_cluster", "table_qc_flag_by_cluster.csv", ["cluster_uid", "cluster_id"]),
    ]

    for key, filename, group_cols in table_plan:
        table = summarize_flags(
            records=records,
            flag_arrays=flag_arrays,
            flag_specs=flag_specs,
            meaning_maps=meaning_maps,
            group_cols=group_cols,
        )
        table.to_csv(tables_dir / filename, index=False)
        tables[key] = table
        print("Wrote {} ({:,} rows)".format(tables_dir / filename, len(table)))

    problem = _problem_cluster_table(tables["by_cluster"], top_n=problem_cluster_top_n)
    problem.to_csv(tables_dir / "table_qc_flag_problem_clusters.csv", index=False)
    tables["problem_clusters"] = problem
    print("Wrote {} ({:,} rows)".format(tables_dir / "table_qc_flag_problem_clusters.csv", len(problem)))

    health_kpis = build_qc_health_kpis(tables["by_resolution"])
    health_kpis.to_csv(tables_dir / "table_qc_health_kpis.csv", index=False)
    tables["health_kpis"] = health_kpis
    print("Wrote {} ({:,} rows)".format(tables_dir / "table_qc_health_kpis.csv", len(health_kpis)))

    stage_effectiveness = build_qc_stage_effectiveness(tables["summary"])
    stage_effectiveness.to_csv(tables_dir / "table_qc_stage_effectiveness.csv", index=False)
    tables["stage_effectiveness"] = stage_effectiveness
    print("Wrote {} ({:,} rows)".format(tables_dir / "table_qc_stage_effectiveness.csv", len(stage_effectiveness)))

    issue_hotspots = build_qc_issue_hotspots(
        by_source=tables["by_source"],
        by_resolution=tables["by_resolution"],
        by_cluster=tables["by_cluster"],
        min_total=int(hotspot_min_total),
    )
    issue_hotspots.to_csv(tables_dir / "table_qc_issue_hotspots.csv", index=False)
    tables["issue_hotspots"] = issue_hotspots
    print("Wrote {} ({:,} rows)".format(tables_dir / "table_qc_issue_hotspots.csv", len(issue_hotspots)))

    yearly_trends = build_qc_yearly_trends(tables["by_year"])
    yearly_trends.to_csv(tables_dir / "table_qc_yearly_trends.csv", index=False)
    tables["yearly_trends"] = yearly_trends
    print("Wrote {} ({:,} rows)".format(tables_dir / "table_qc_yearly_trends.csv", len(yearly_trends)))
    return tables


def _filter_flag_specs(
    flag_specs: Sequence[Mapping[str, str]],
    flag_arrays: MutableMapping[str, np.ndarray],
    meaning_maps: MutableMapping[str, Mapping[int, str]],
    skip_stage_flags: bool,
) -> List[Dict[str, str]]:
    if not skip_stage_flags:
        return [dict(spec) for spec in flag_specs]

    kept = [dict(spec) for spec in flag_specs if spec.get("qc_level") == "final"]
    kept_names = {spec["flag_variable"] for spec in kept}
    for name in list(flag_arrays.keys()):
        if name not in kept_names:
            flag_arrays.pop(name, None)
            meaning_maps.pop(name, None)
    return kept


# -----------------------------------------------------------------------------
# Markdown report
# -----------------------------------------------------------------------------

def _fmt_int(value: object) -> str:
    try:
        return "{:,}".format(int(float(value)))
    except Exception:
        return "NA"


def _fmt_pct(value: object, digits: int = 1) -> str:
    try:
        return "{:.{}f}%".format(float(value), int(digits))
    except Exception:
        return "NA"


def _markdown_table(rows: Sequence[Mapping[str, object]], columns: Sequence[str], headers: Sequence[str]) -> str:
    if not rows:
        return "_No rows._"
    out = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join(out)


def _link_text(path: Path, base: Path) -> str:
    try:
        text = str(path.resolve().relative_to(base.resolve()))
    except Exception:
        text = str(path)
    return text.replace("\\", "/")


def _existing_link(path: Path, base: Path) -> str:
    text = _link_text(path, base)
    if path.is_file():
        return "[{}]({})".format(text, text)
    return "`{}` (not generated)".format(text)


def _compact_final_rows(summary: pd.DataFrame) -> List[Dict[str, object]]:
    metrics = _rate_table(summary, [])
    if metrics.empty:
        return []
    metrics["variable"] = pd.Categorical(metrics["variable"], categories=FINAL_VARIABLE_ORDER, ordered=True)
    metrics = metrics.sort_values("variable", kind="mergesort")
    rows = []
    for _, row in metrics.iterrows():
        rows.append(
            {
                "variable": str(row["variable"]),
                "n_total": _fmt_int(row["n_total"]),
                "good_rate": _fmt_pct(row["good_rate"]),
                "derived_rate": _fmt_pct(row["derived_rate"]),
                "usable_rate": _fmt_pct(row["usable_rate"]),
                "problem_rate": _fmt_pct(row["problem_rate"]),
                "missing_rate": _fmt_pct(row["missing_rate"]),
                "not_checked_rate": _fmt_pct(row["not_checked_rate"]),
            }
        )
    return rows


def _compact_health_rows(health_kpis: pd.DataFrame) -> List[Dict[str, object]]:
    if health_kpis.empty:
        return []
    rows = []
    for _, row in health_kpis.iterrows():
        rows.append(
            {
                "resolution": str(row.get("temporal_resolution", "")),
                "variable": str(row.get("variable", "")),
                "n_total": _fmt_int(row.get("n_total")),
                "usable_rate": _fmt_pct(row.get("usable_rate")),
                "problem_rate": _fmt_pct(row.get("problem_rate")),
                "missing_rate": _fmt_pct(row.get("missing_rate")),
            }
        )
    return rows


def _compact_stage_rows(stage_effectiveness: pd.DataFrame) -> List[Dict[str, object]]:
    if stage_effectiveness.empty:
        return []
    rows = []
    for _, row in stage_effectiveness.iterrows():
        rows.append(
            {
                "qc_stage": str(row.get("qc_stage", "")),
                "variable": str(row.get("variable", "")),
                "flag_variable": str(row.get("flag_variable", "")),
                "pass_rate": _fmt_pct(row.get("pass_rate")),
                "suspect_rate": _fmt_pct(row.get("suspect_rate")),
                "bad_rate": _fmt_pct(row.get("bad_rate")),
                "not_checked_rate": _fmt_pct(row.get("not_checked_rate")),
                "missing_rate": _fmt_pct(row.get("missing_rate")),
            }
        )
    return rows


def _compact_hotspot_rows(hotspots: pd.DataFrame, grouping_level: str, top_n: int) -> List[Dict[str, object]]:
    if hotspots.empty:
        return []
    work = hotspots.loc[hotspots["grouping_level"].eq(grouping_level)].copy()
    if work.empty:
        return []
    work = work.sort_values(
        ["issue_rate", "issue_count", "problem_rate", "n_total"],
        ascending=[False, False, False, False],
        kind="mergesort",
    ).head(max(1, int(top_n)))
    rows = []
    for _, row in work.iterrows():
        if grouping_level == "source":
            name = str(row.get("source_dataset", ""))
        elif grouping_level == "cluster":
            name = str(row.get("cluster_uid", ""))
        elif grouping_level == "resolution":
            name = str(row.get("temporal_resolution", ""))
        else:
            name = str(row.get("source_type", ""))
        rows.append(
            {
                "name": name,
                "type": str(row.get("source_type", "")),
                "variable": str(row.get("variable", "")),
                "n_total": _fmt_int(row.get("n_total")),
                "issue_rate": _fmt_pct(row.get("issue_rate")),
                "problem_rate": _fmt_pct(row.get("problem_rate")),
                "missing_rate": _fmt_pct(row.get("missing_rate")),
            }
        )
    return rows


def _headline_findings(summary: pd.DataFrame, health_kpis: pd.DataFrame) -> List[str]:
    metrics = _rate_table(summary, [])
    findings: List[str] = []
    if not metrics.empty:
        q = metrics.loc[metrics["variable"].astype(str).eq("Q")]
        ssc = metrics.loc[metrics["variable"].astype(str).eq("SSC")]
        ssl = metrics.loc[metrics["variable"].astype(str).eq("SSL")]
        if not q.empty:
            row = q.iloc[0]
            findings.append(
                "Q 的 final flag 以 good 为主：good rate 为 {}，usable rate 为 {}。".format(
                    _fmt_pct(row["good_rate"]), _fmt_pct(row["usable_rate"])
                )
            )
        if not ssc.empty and not ssl.empty:
            findings.append(
                "SSC 和 SSL 的主要限制来自 missing：SSC missing rate 为 {}，SSL missing rate 为 {}。".format(
                    _fmt_pct(ssc.iloc[0]["missing_rate"]), _fmt_pct(ssl.iloc[0]["missing_rate"])
                )
            )
        not_checked_max = float(pd.to_numeric(metrics["not_checked_rate"], errors="coerce").fillna(0.0).max())
        findings.append("Final flag 8 的最高比例为 {}，说明最终发布层基本没有未检查记录残留。".format(_fmt_pct(not_checked_max)))

    if not health_kpis.empty:
        usable = health_kpis.sort_values(["usable_rate", "n_total"], ascending=[False, False], kind="mergesort").head(1)
        missing = health_kpis.sort_values(["missing_rate", "n_total"], ascending=[False, False], kind="mergesort").head(1)
        if not usable.empty:
            row = usable.iloc[0]
            findings.append(
                "{} {} 的 usable rate 最高，为 {}。".format(
                    row.get("temporal_resolution"), row.get("variable"), _fmt_pct(row.get("usable_rate"))
                )
            )
        if not missing.empty:
            row = missing.iloc[0]
            findings.append(
                "{} {} 的 missing rate 最高，为 {}，适合在结果讨论中作为覆盖限制说明。".format(
                    row.get("temporal_resolution"), row.get("variable"), _fmt_pct(row.get("missing_rate"))
                )
            )
    return findings


def write_report(
    report_path: Path,
    master_nc: Path,
    quality_order_csv: Optional[Path],
    tables_dir: Path,
    figures_dir: Path,
    tables: Mapping[str, pd.DataFrame],
    top_n_sources: int,
    top_n_clusters: int,
    skip_stage_flags: bool,
) -> Path:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    base = report_path.parent

    summary = tables.get("summary", pd.DataFrame())
    health = tables.get("health_kpis", pd.DataFrame())
    stage = tables.get("stage_effectiveness", pd.DataFrame())
    hotspots = tables.get("issue_hotspots", pd.DataFrame())

    lines: List[str] = [
        "# S8 QC Flag Statistics Report",
        "",
        "本报告由 `stats/qc_flag_statistics.py` 自动生成，用于总结 S8 发布级产品的 final QC flags 和阶段性 QC flags。",
        "",
        "## 数据与方法",
        "",
        "- 输入 master NetCDF：`{}`".format(master_nc),
        "- source type 辅助表：`{}`".format(quality_order_csv if quality_order_csv is not None else "not used"),
        "- 输出表目录：`{}`".format(tables_dir),
        "- 输出图目录：`{}`".format(figures_dir),
        "- 统计口径：`usable_rate = flag 0 + flag 1`；`problem_rate = flag 2 + flag 3`；`missing_rate = flag 9`；`not_checked_rate = flag 8`。",
        "- Final flag 含义：0=good，1=derived/estimated，2=suspect，3=bad，8=not checked，9=missing。",
        "",
        "## 核心结论",
        "",
    ]

    findings = _headline_findings(summary, health)
    if findings:
        lines.extend(["- " + item for item in findings])
    else:
        lines.append("- 未生成可用的核心统计结论，请检查输入表。")

    lines.extend(
        [
            "",
            "### Final QC by Variable",
            "",
            _markdown_table(
                _compact_final_rows(summary),
                ["variable", "n_total", "good_rate", "derived_rate", "usable_rate", "problem_rate", "missing_rate", "not_checked_rate"],
                ["Variable", "N", "Good", "Derived", "Usable", "Problem", "Missing", "Not checked"],
            ),
            "",
            "### Resolution Difference",
            "",
            _markdown_table(
                _compact_health_rows(health),
                ["resolution", "variable", "n_total", "usable_rate", "problem_rate", "missing_rate"],
                ["Resolution", "Variable", "N", "Usable", "Problem", "Missing"],
            ),
            "",
            "## QC 阶段解释",
            "",
        ]
    )
    if skip_stage_flags:
        lines.append("本次运行使用了 `--skip-stage-flags`，因此报告只包含 final QC flags。")
    else:
        lines.extend(
            [
                "阶段性 QC 用于解释 final flag 的来源：QC1 主要对应物理范围筛查，QC2 对应 log-IQR 异常筛查，QC3 对应 SSC-Q 一致性或 SSL 传播关系。",
                "",
                _markdown_table(
                    _compact_stage_rows(stage),
                    ["qc_stage", "variable", "flag_variable", "pass_rate", "suspect_rate", "bad_rate", "not_checked_rate", "missing_rate"],
                    ["Stage", "Variable", "Flag var", "Flag 0", "Suspect", "Bad", "Not checked", "Missing"],
                ),
            ]
        )

    lines.extend(
        [
            "",
            "## 热点诊断",
            "",
            "下面的热点表按 `issue_rate = problem_rate + missing_rate` 排序；其中 `problem_rate` 只包含 suspect+bad，missing 单独保留，便于区分质量异常和观测缺失。",
            "",
            "### Top Sources",
            "",
            _markdown_table(
                _compact_hotspot_rows(hotspots, "source", top_n_sources),
                ["name", "type", "variable", "n_total", "issue_rate", "problem_rate", "missing_rate"],
                ["Source", "Type", "Variable", "N", "Issue", "Problem", "Missing"],
            ),
            "",
            "### Top Clusters",
            "",
            _markdown_table(
                _compact_hotspot_rows(hotspots, "cluster", top_n_clusters),
                ["name", "type", "variable", "n_total", "issue_rate", "problem_rate", "missing_rate"],
                ["Cluster", "Type", "Variable", "N", "Issue", "Problem", "Missing"],
            ),
            "",
            "## 图表建议",
            "",
            "- 正文 QC 概览：{}".format(_existing_link(figures_dir / "fig_qc_flag_distribution.png", base)),
            "- 正文分辨率对比：{}".format(_existing_link(figures_dir / "fig_qc_health_by_resolution.png", base)),
            "- 时间变化讨论：{} 和 {}".format(
                _existing_link(figures_dir / "fig_qc_yearly_problem_trends.png", base),
                _existing_link(figures_dir / "fig_qc_missing_trends.png", base),
            ),
            "- 方法/补充材料：{}".format(_existing_link(figures_dir / "fig_qc_stage_summary.png", base)),
            "- 补充材料热点诊断：{} 和 {}".format(
                _existing_link(figures_dir / "fig_qc_top_problem_sources.png", base),
                _existing_link(figures_dir / "fig_qc_top_problem_clusters.png", base),
            ),
            "",
            "## 输出数据索引",
            "",
            "- {}".format(_existing_link(tables_dir / "table_qc_flag_summary.csv", base)),
            "- {}".format(_existing_link(tables_dir / "table_qc_health_kpis.csv", base)),
            "- {}".format(_existing_link(tables_dir / "table_qc_stage_effectiveness.csv", base)),
            "- {}".format(_existing_link(tables_dir / "table_qc_issue_hotspots.csv", base)),
            "- {}".format(_existing_link(tables_dir / "table_qc_yearly_trends.csv", base)),
            "- {}".format(_existing_link(tables_dir / "table_qc_flag_problem_clusters.csv", base)),
            "",
            "## 可支撑的论文表述",
            "",
            "- Q 数据整体质量较稳定，final good/usable 比例可作为发布级流量数据可靠性的核心证据。",
            "- SSC/SSL 的限制主要体现为可用观测覆盖不足，而不是大量 suspect/bad；因此讨论中应把质量异常和观测缺失分开表述。",
            "- source、cluster 热点表适合放入 supplement，用于说明少数数据源或站点簇对缺失/问题比例的贡献。",
        ]
    )

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("Wrote {}".format(report_path))
    return report_path


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate QC flag statistics tables and figures for the sediment reference dataset."
    )
    parser.add_argument(
        "--input-master-nc",
        default="",
        help=(
            "Input master NetCDF. Default: release master if present, otherwise s6 master. "
            "Relative paths are resolved against the Output_r root."
        ),
    )
    parser.add_argument(
        "--quality-order-csv",
        default=str(DEFAULT_QUALITY_ORDER_CSV),
        help="s6 quality-order CSV used to recover source_type/source_family.",
    )
    parser.add_argument(
        "--tables-dir",
        default=str(DEFAULT_TABLES_DIR),
        help="Output directory for CSV tables. Default: {}".format(DEFAULT_TABLES_DIR),
    )
    parser.add_argument(
        "--figures-dir",
        default=str(DEFAULT_FIGURES_DIR),
        help="Output directory for figures. Default: {}".format(DEFAULT_FIGURES_DIR),
    )
    parser.add_argument(
        "--report-path",
        default=str(DEFAULT_REPORT_PATH),
        help="Output Markdown QC report path. Default: {}".format(DEFAULT_REPORT_PATH),
    )
    parser.add_argument(
        "--skip-stage-flags",
        action="store_true",
        help="Only summarize final Q_flag/SSC_flag/SSL_flag and skip stage-specific QC flags.",
    )
    parser.add_argument(
        "--problem-cluster-top-n",
        type=int,
        default=100,
        help="Number of highest suspect+bad+missing clusters to write to table_qc_flag_problem_clusters.csv. Use 0 for all.",
    )
    parser.add_argument(
        "--top-n-sources",
        type=int,
        default=10,
        help="Number of source-level hotspots to show in figures and the Markdown report.",
    )
    parser.add_argument(
        "--top-n-clusters",
        type=int,
        default=None,
        help="Number of cluster-level hotspots to show in figures and the Markdown report. Default: --problem-cluster-top-n.",
    )
    parser.add_argument(
        "--hotspot-min-total",
        type=int,
        default=HOTSPOT_MIN_TOTAL,
        help="Minimum n_total required for rows in table_qc_issue_hotspots.csv.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    if nc4 is None:
        print("Error: netCDF4 is required. Install it with: pip install netCDF4", file=sys.stderr)
        return 1

    master_nc = _resolve_path(args.input_master_nc) if args.input_master_nc else _default_master_nc()
    quality_order_csv = _resolve_path(args.quality_order_csv) if args.quality_order_csv else None
    tables_dir = _resolve_path(args.tables_dir)
    figures_dir = _resolve_path(args.figures_dir)
    report_path = _resolve_path(args.report_path)
    top_n_clusters = int(args.top_n_clusters) if args.top_n_clusters is not None else int(args.problem_cluster_top_n)
    if top_n_clusters <= 0:
        top_n_clusters = 100

    print("Input master NetCDF: {}".format(master_nc))
    print("Quality-order CSV: {}".format(quality_order_csv))
    print("Tables directory: {}".format(tables_dir))
    print("Figures directory: {}".format(figures_dir))
    print("Report path: {}".format(report_path))

    records, flag_arrays, flag_specs, meaning_maps = read_master_records(
        master_nc=master_nc,
        include_stage_flags=not args.skip_stage_flags,
    )
    flag_specs = _filter_flag_specs(
        flag_specs=flag_specs,
        flag_arrays=flag_arrays,
        meaning_maps=meaning_maps,
        skip_stage_flags=bool(args.skip_stage_flags),
    )
    records = attach_source_type(records, quality_order_csv)

    print(
        "Loaded {:,} records, {} flag variables: {}".format(
            len(records),
            len(flag_specs),
            ", ".join(spec["flag_variable"] for spec in flag_specs),
        )
    )

    tables = write_tables(
        records=records,
        flag_arrays=flag_arrays,
        flag_specs=flag_specs,
        meaning_maps=meaning_maps,
        tables_dir=tables_dir,
        problem_cluster_top_n=int(args.problem_cluster_top_n),
        hotspot_min_total=int(args.hotspot_min_total),
    )
    write_figures(
        records=records,
        flag_arrays=flag_arrays,
        flag_specs=flag_specs,
        meaning_maps=meaning_maps,
        summary=tables["summary"],
        by_source=tables["by_source"],
        health_kpis=tables["health_kpis"],
        yearly_trends=tables["yearly_trends"],
        stage_effectiveness=tables["stage_effectiveness"],
        issue_hotspots=tables["issue_hotspots"],
        figures_dir=figures_dir,
        top_n_sources=int(args.top_n_sources),
        top_n_clusters=top_n_clusters,
    )
    write_report(
        report_path=report_path,
        master_nc=master_nc,
        quality_order_csv=quality_order_csv,
        tables_dir=tables_dir,
        figures_dir=figures_dir,
        tables=tables,
        top_n_sources=int(args.top_n_sources),
        top_n_clusters=top_n_clusters,
        skip_stage_flags=bool(args.skip_stage_flags),
    )

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
