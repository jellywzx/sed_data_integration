#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate QC flag summary tables and figures for the sediment reference dataset.

This reporting script summarizes final QC flags and, when present, stage-specific
QC flags for manuscript Section 3 Quality Control, Section 5 Uncertainty
Assessment, and supplementary information.

Default inputs
--------------
1. scripts_basin_test/output/sed_reference_release/sed_reference_master.nc
2. scripts_basin_test/output/s6_basin_merged_all.nc, used as fallback
3. scripts_basin_test/output/s6_cluster_quality_order.csv, optional, used to
   recover source_type/source_family

Outputs
-------
tables/table_qc_flag_summary.csv
tables/table_qc_flag_by_source.csv
tables/table_qc_flag_by_resolution.csv
tables/table_qc_flag_by_variable.csv
tables/table_qc_flag_by_year.csv
tables/table_qc_flag_by_cluster.csv
tables/table_qc_flag_problem_clusters.csv
figures/fig_qc_flag_distribution.png
figures/fig_qc_flag_by_source_type.png

Run
---
python stats/qc_flag_statistics.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

# The script lives in stats/. Add the repository root so sibling modules such as
# pipeline_paths.py and qc_contract.py remain importable.
REPO_DIR = Path(__file__).resolve().parents[1]
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

import numpy as np
import pandas as pd

from pipeline_paths import (
    RELEASE_MASTER_NC,
    S6_MERGED_NC,
    S6_QUALITY_ORDER_CSV,
    get_output_r_root,
)
from qc_contract import STANDARD_QC_STAGE_NAMES, STANDARD_QC_STAGE_NAME_TO_SPEC

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover
    nc4 = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None


PROJECT_ROOT = get_output_r_root(REPO_DIR)
DEFAULT_RELEASE_MASTER_NC = PROJECT_ROOT / RELEASE_MASTER_NC
DEFAULT_S6_MASTER_NC = PROJECT_ROOT / S6_MERGED_NC
DEFAULT_QUALITY_ORDER_CSV = PROJECT_ROOT / S6_QUALITY_ORDER_CSV
DEFAULT_TABLES_DIR = PROJECT_ROOT / "tables"
DEFAULT_FIGURES_DIR = PROJECT_ROOT / "figures"

FLAG_CODES = [0, 1, 2, 3, 8, 9]
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
    ("final", "final", "Q", "Q_flag"),
    ("final", "final", "SSC", "SSC_flag"),
    ("final", "final", "SSL", "SSL_flag"),
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


def clean_text(value: object, default: str = "") -> str:
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


def resolve_path(path_text: str, base: Path = PROJECT_ROOT) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path
    return (base / path).resolve()


def default_master_nc() -> Path:
    if DEFAULT_RELEASE_MASTER_NC.is_file():
        return DEFAULT_RELEASE_MASTER_NC
    return DEFAULT_S6_MASTER_NC


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def mode_text(values: Iterable[object], default: str = "unknown") -> str:
    cleaned = [clean_text(value) for value in values]
    cleaned = [value for value in cleaned if value]
    if not cleaned:
        return default
    counts = pd.Series(cleaned).value_counts(dropna=True)
    if counts.empty:
        return default
    return str(counts.index[0])


def parse_flag_meanings(flag_values: object, flag_meanings: object) -> Dict[int, str]:
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


def meaning_for(flag: int, meaning_map: Mapping[int, str]) -> str:
    flag = int(flag)
    return meaning_map.get(flag, COMMON_FLAG_MEANINGS.get(flag, "unknown"))


def cross_join(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    left = left.copy()
    right = right.copy()
    left["_tmp_cross_key"] = 1
    right["_tmp_cross_key"] = 1
    out = left.merge(right, on="_tmp_cross_key", how="outer")
    return out.drop(columns=["_tmp_cross_key"])


def pad_or_trim(values: np.ndarray, size: int, fill_value: object) -> np.ndarray:
    arr = np.asarray(values).reshape(-1)
    if len(arr) >= size:
        return arr[:size]
    pad = np.full(size - len(arr), fill_value, dtype=arr.dtype if len(arr) else object)
    return np.concatenate([arr, pad])


def read_int_var(ds, name: str, size: int, fill_value: int) -> np.ndarray:
    if name not in ds.variables:
        return np.full(size, fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    raw = raw.filled(fill_value) if np.ma.isMaskedArray(raw) else raw
    raw = pad_or_trim(np.asarray(raw), size, fill_value)
    return pd.to_numeric(pd.Series(raw), errors="coerce").fillna(fill_value).astype(np.int64).to_numpy()


def read_flag_var(ds, name: str, size: int, fill_value: int = 9) -> np.ndarray:
    if name not in ds.variables:
        return np.full(size, fill_value, dtype=np.int16)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    raw = raw.filled(fill_value) if np.ma.isMaskedArray(raw) else raw
    raw = pad_or_trim(np.asarray(raw), size, fill_value)
    return pd.to_numeric(pd.Series(raw), errors="coerce").fillna(fill_value).astype(np.int16).to_numpy()


def decode_text_array(values: object, size: int, default: str = "") -> List[str]:
    try:
        arr = np.ma.asarray(values)
        if np.ma.isMaskedArray(arr):
            arr = arr.filled(default)
        arr = np.asarray(arr, dtype=object).reshape(-1)
    except Exception:
        arr = np.asarray([], dtype=object)
    arr = pad_or_trim(arr, size, default)
    return [clean_text(item, default=default) for item in arr]


def read_text_var(ds, name: str, size: int, default: str = "") -> List[str]:
    if name not in ds.variables:
        return [default] * size
    try:
        values = ds.variables[name][:]
    except Exception:
        return [default] * size
    try:
        values_arr = np.asarray(values)
        if values_arr.dtype.kind in {"S", "U"} and values_arr.ndim > 1:
            values = nc4.chartostring(values_arr)
    except Exception:
        pass
    return decode_text_array(values, size=size, default=default)


def read_year_array(ds, size: int) -> np.ndarray:
    if "time" not in ds.variables:
        return np.full(size, -9999, dtype=np.int32)
    t_var = ds.variables["time"]
    try:
        raw = np.ma.asarray(t_var[:]).reshape(-1)
    except Exception:
        return np.full(size, -9999, dtype=np.int32)

    raw = pad_or_trim(raw, size, np.nan)
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
            dates = nc4.num2date(valid_values, units=units, calendar=calendar, only_use_cftime_datetimes=False)
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


def lookup_by_index(index_arr: np.ndarray, lookup: Sequence[object], default: object) -> List[object]:
    out: List[object] = []
    n = len(lookup)
    for idx in index_arr:
        try:
            i = int(idx)
        except Exception:
            out.append(default)
            continue
        out.append(lookup[i] if 0 <= i < n else default)
    return out


def stage_label(flag_variable: str) -> str:
    if flag_variable.endswith("_qc1"):
        return "physical_plausibility"
    if flag_variable.endswith("_qc2"):
        return "log_iqr"
    if flag_variable.endswith("_qc3"):
        return "ssc_q_consistency"
    return "stage_specific"


def stage_variable(flag_variable: str) -> str:
    return str(flag_variable).split("_", 1)[0]


def stage_meaning_map_from_contract(flag_variable: str) -> Dict[int, str]:
    spec = STANDARD_QC_STAGE_NAME_TO_SPEC.get(flag_variable, {})
    return parse_flag_meanings(spec.get("flag_values"), spec.get("flag_meanings"))


def read_master_records(master_nc: Path, include_stage_flags: bool = True):
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

        station_index = read_int_var(ds, "station_index", n_records, -1)
        source_station_index = read_int_var(ds, "source_station_index", n_records, -1)
        resolution_code = read_int_var(ds, "resolution", n_records, 4)
        source_dataset = read_text_var(ds, "source", n_records, default="unknown")
        year = read_year_array(ds, n_records)

        if "n_stations" in ds.dimensions:
            n_stations = len(ds.dimensions["n_stations"])
        else:
            n_stations = int(max(station_index.max() + 1, 0)) if len(station_index) else 0

        cluster_ids = read_int_var(ds, "cluster_id", n_stations, -1)
        if "cluster_uid" in ds.variables:
            cluster_uids = read_text_var(ds, "cluster_uid", n_stations, default="")
        else:
            cluster_uids = ["SED{:06d}".format(int(cid)) if int(cid) >= 0 else "" for cid in cluster_ids]

        records = pd.DataFrame(
            {
                "record_index": np.arange(n_records, dtype=np.int64),
                "station_index": station_index.astype(np.int64),
                "cluster_id": pd.to_numeric(
                    pd.Series(lookup_by_index(station_index, cluster_ids.tolist(), -1)), errors="coerce"
                ).fillna(-1).astype(np.int64),
                "cluster_uid": [clean_text(x) for x in lookup_by_index(station_index, cluster_uids, "")],
                "source_station_index": source_station_index.astype(np.int64),
                "source_dataset": [clean_text(x, default="unknown") for x in source_dataset],
                "temporal_resolution": [RESOLUTION_CODE_TO_NAME.get(int(x), "other") for x in resolution_code],
                "resolution_code": resolution_code.astype(np.int16),
                "year": year.astype(np.int32),
                "source_type": "unknown",
            }
        )

        flag_arrays: Dict[str, np.ndarray] = {}
        flag_specs: List[Dict[str, str]] = []
        meaning_maps: Dict[str, Dict[int, str]] = {}

        for qc_level, qc_stage, variable, flag_variable in FINAL_FLAG_SPECS:
            if flag_variable not in ds.variables:
                raise ValueError("Required final QC flag variable '{}' is missing from {}".format(flag_variable, master_nc))
            flag_arrays[flag_variable] = read_flag_var(ds, flag_variable, n_records, 9)
            flag_specs.append(
                {
                    "qc_level": qc_level,
                    "qc_stage": qc_stage,
                    "variable": variable,
                    "flag_variable": flag_variable,
                }
            )
            meaning_maps[flag_variable] = dict(COMMON_FLAG_MEANINGS)

        if include_stage_flags:
            for flag_variable in STANDARD_QC_STAGE_NAMES:
                if flag_variable not in ds.variables:
                    continue
                fill_value = int(STANDARD_QC_STAGE_NAME_TO_SPEC.get(flag_variable, {}).get("fill_value", 9))
                flag_arrays[flag_variable] = read_flag_var(ds, flag_variable, n_records, fill_value)
                flag_specs.append(
                    {
                        "qc_level": "stage",
                        "qc_stage": stage_label(flag_variable),
                        "variable": stage_variable(flag_variable),
                        "flag_variable": flag_variable,
                    }
                )
                var = ds.variables[flag_variable]
                meaning_map = parse_flag_meanings(getattr(var, "flag_values", None), getattr(var, "flag_meanings", None))
                if not meaning_map:
                    meaning_map = stage_meaning_map_from_contract(flag_variable)
                meaning_maps[flag_variable] = meaning_map or dict(COMMON_FLAG_MEANINGS)

    return records, flag_arrays, flag_specs, meaning_maps


def attach_source_type(records: pd.DataFrame, quality_order_csv: Optional[Path]) -> pd.DataFrame:
    records = records.copy()
    records["source_type"] = "unknown"
    if quality_order_csv is None or not quality_order_csv.is_file():
        print("Warning: quality-order CSV not found; source_type will be 'unknown': {}".format(quality_order_csv), file=sys.stderr)
        return records

    quality = pd.read_csv(quality_order_csv)
    required = {"source", "source_station_index", "source_family"}
    if not required.issubset(quality.columns):
        missing = sorted(required - set(quality.columns))
        print("Warning: quality-order CSV lacks {}; source_type will be 'unknown': {}".format(missing, quality_order_csv), file=sys.stderr)
        return records

    quality = quality.loc[:, ["source", "source_station_index", "source_family"]].copy()
    quality["source"] = quality["source"].map(lambda x: clean_text(x, default="unknown"))
    quality["source_family"] = quality["source_family"].map(lambda x: clean_text(x, default="unknown"))
    quality["source_station_index"] = pd.to_numeric(quality["source_station_index"], errors="coerce").fillna(-1).astype(np.int64)

    pair_lookup = (
        quality.groupby(["source_station_index", "source"], dropna=False)["source_family"]
        .agg(mode_text)
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

    source_lookup = quality.groupby("source", dropna=False)["source_family"].agg(mode_text).to_dict()
    fallback = out["source_dataset"].map(source_lookup)
    out["source_type"] = out["source_type_pair"].fillna(fallback).fillna("unknown")
    out["source_type"] = out["source_type"].map(lambda x: clean_text(x, default="unknown"))
    return out.drop(columns=["source_type_pair"])


def normal_flag_codes(flag_array: np.ndarray) -> List[int]:
    observed = pd.to_numeric(pd.Series(flag_array), errors="coerce").dropna().astype(int).unique().tolist()
    return sorted(set(FLAG_CODES).union(observed))


def summarize_one_flag(records: pd.DataFrame, flag_array: np.ndarray, spec: Mapping[str, str], meaning_map: Mapping[int, str], group_cols: Sequence[str]) -> pd.DataFrame:
    group_cols = list(group_cols)
    missing_cols = [col for col in group_cols if col not in records.columns]
    if missing_cols:
        raise KeyError("Missing grouping columns: {}".format(missing_cols))

    work = records.loc[:, group_cols].copy() if group_cols else pd.DataFrame(index=records.index)
    work["flag"] = pd.to_numeric(pd.Series(flag_array), errors="coerce").fillna(9).astype(np.int16).to_numpy()
    flag_df = pd.DataFrame({"flag": normal_flag_codes(flag_array)})

    if group_cols:
        counts = work.groupby(group_cols + ["flag"], dropna=False).size().reset_index(name="count")
        groups = work.loc[:, group_cols].drop_duplicates()
        totals = work.groupby(group_cols, dropna=False).size().reset_index(name="n_total")
        out = cross_join(groups, flag_df).merge(counts, how="left", on=group_cols + ["flag"]).merge(totals, how="left", on=group_cols)
    else:
        counts = work.groupby("flag", dropna=False).size().reset_index(name="count")
        out = flag_df.merge(counts, how="left", on="flag")
        out["n_total"] = len(work)

    out["count"] = out["count"].fillna(0).astype(np.int64)
    out["n_total"] = out["n_total"].fillna(0).astype(np.int64)
    out["percentage"] = np.where(out["n_total"] > 0, out["count"] / out["n_total"] * 100.0, 0.0).round(6)
    out["qc_level"] = spec["qc_level"]
    out["qc_stage"] = spec["qc_stage"]
    out["variable"] = spec["variable"]
    out["flag_variable"] = spec["flag_variable"]
    out["meaning"] = out["flag"].map(lambda x: meaning_for(int(x), meaning_map))
    return out.loc[:, group_cols + SUMMARY_BASE_COLUMNS]


def summarize_flags(records: pd.DataFrame, flag_arrays: Mapping[str, np.ndarray], flag_specs: Sequence[Mapping[str, str]], meaning_maps: Mapping[str, Mapping[int, str]], group_cols: Sequence[str]) -> pd.DataFrame:
    parts = []
    for spec in flag_specs:
        flag_variable = spec["flag_variable"]
        if flag_variable not in flag_arrays:
            continue
        parts.append(
            summarize_one_flag(
                records,
                flag_arrays[flag_variable],
                spec,
                meaning_maps.get(flag_variable, COMMON_FLAG_MEANINGS),
                group_cols,
            )
        )
    if not parts:
        return pd.DataFrame(columns=list(group_cols) + SUMMARY_BASE_COLUMNS)
    out = pd.concat(parts, ignore_index=True)
    sort_cols = list(group_cols) + ["qc_level", "qc_stage", "variable", "flag_variable", "flag"]
    return out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)


def problem_cluster_table(by_cluster: pd.DataFrame, top_n: int) -> pd.DataFrame:
    if by_cluster.empty:
        return pd.DataFrame()
    final = by_cluster.loc[by_cluster["qc_level"].eq("final")].copy()
    if final.empty:
        return pd.DataFrame()
    problem = final.loc[final["flag"].isin([2, 3, 9])]
    problem_counts = (
        problem.groupby(["cluster_uid", "cluster_id", "variable", "flag_variable"], dropna=False)["count"]
        .sum()
        .reset_index(name="problem_count")
    )
    totals = (
        final.groupby(["cluster_uid", "cluster_id", "variable", "flag_variable"], dropna=False)["n_total"]
        .max()
        .reset_index()
    )
    out = totals.merge(problem_counts, how="left", on=["cluster_uid", "cluster_id", "variable", "flag_variable"])
    out["problem_count"] = out["problem_count"].fillna(0).astype(np.int64)
    out["problem_percentage"] = np.where(out["n_total"] > 0, out["problem_count"] / out["n_total"] * 100.0, 0.0).round(6)
    out = out.sort_values(["problem_percentage", "problem_count", "n_total"], ascending=[False, False, False], kind="mergesort")
    if top_n and top_n > 0:
        out = out.head(int(top_n))
    return out.reset_index(drop=True)


def plot_stacked_percent(table: pd.DataFrame, index_col: str, output_path: Path, title: str, xlabel: str, label_rotation: int = 0) -> None:
    if plt is None:
        print("Warning: matplotlib is not installed; skipping figure {}".format(output_path), file=sys.stderr)
        return
    if table.empty:
        print("Warning: empty table; skipping figure {}".format(output_path), file=sys.stderr)
        return

    pivot = table.pivot_table(index=index_col, columns="flag", values="percentage", aggfunc="sum").fillna(0.0)
    if pivot.empty:
        return
    ordered_flags = [flag for flag in FLAG_CODES if flag in pivot.columns]
    ordered_flags += sorted([flag for flag in pivot.columns if flag not in ordered_flags])
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
    ax.set_ylabel("Percentage of records (%)")
    ax.set_ylim(0, max(100.0, float(np.nanmax(bottom)) if len(bottom) else 100.0))
    ax.set_xticks(x)
    ax.set_xticklabels([str(item) for item in pivot.index], rotation=label_rotation, ha="right" if label_rotation else "center")
    ax.legend(title="Flag", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0.0)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def write_figures(summary: pd.DataFrame, by_source: pd.DataFrame, figures_dir: Path) -> None:
    figures_dir = ensure_dir(figures_dir)
    final_summary = summary.loc[summary["qc_level"].eq("final")].copy()
    final_summary["flag_variable"] = pd.Categorical(final_summary["flag_variable"], categories=["Q_flag", "SSC_flag", "SSL_flag"], ordered=True)
    final_summary = final_summary.sort_values(["flag_variable", "flag"])
    plot_stacked_percent(final_summary, "flag_variable", figures_dir / "fig_qc_flag_distribution.png", "Distribution of final QC flags", "Final QC flag variable")

    final_by_source = by_source.loc[by_source["qc_level"].eq("final")].copy()
    if final_by_source.empty:
        source_type_table = pd.DataFrame(columns=["source_type_variable", "flag", "percentage"])
    else:
        source_type_table = final_by_source.groupby(["source_type", "variable", "flag"], dropna=False, as_index=False)["count"].sum()
        totals = source_type_table.groupby(["source_type", "variable"], dropna=False)["count"].sum().reset_index(name="n_total")
        source_type_table = source_type_table.merge(totals, how="left", on=["source_type", "variable"])
        source_type_table["percentage"] = np.where(source_type_table["n_total"] > 0, source_type_table["count"] / source_type_table["n_total"] * 100.0, 0.0)
        source_type_table["source_type_variable"] = source_type_table["source_type"].astype(str) + "\n" + source_type_table["variable"].astype(str)
        source_type_table = source_type_table.sort_values(["source_type", "variable", "flag"], kind="mergesort")
    plot_stacked_percent(source_type_table, "source_type_variable", figures_dir / "fig_qc_flag_by_source_type.png", "Final QC flag distribution by source type", "Source type and variable", label_rotation=45)


def write_tables(records: pd.DataFrame, flag_arrays: Mapping[str, np.ndarray], flag_specs: Sequence[Mapping[str, str]], meaning_maps: Mapping[str, Mapping[int, str]], tables_dir: Path, problem_cluster_top_n: int):
    tables_dir = ensure_dir(tables_dir)
    plan = [
        ("summary", "table_qc_flag_summary.csv", []),
        ("by_source", "table_qc_flag_by_source.csv", ["source_dataset", "source_type"]),
        ("by_resolution", "table_qc_flag_by_resolution.csv", ["temporal_resolution"]),
        ("by_variable", "table_qc_flag_by_variable.csv", []),
        ("by_year", "table_qc_flag_by_year.csv", ["year", "temporal_resolution"]),
        ("by_cluster", "table_qc_flag_by_cluster.csv", ["cluster_uid", "cluster_id"]),
    ]
    tables = {}
    for key, filename, group_cols in plan:
        table = summarize_flags(records, flag_arrays, flag_specs, meaning_maps, group_cols)
        table.to_csv(tables_dir / filename, index=False)
        tables[key] = table
        print("Wrote {} ({:,} rows)".format(tables_dir / filename, len(table)))

    problem = problem_cluster_table(tables["by_cluster"], top_n=problem_cluster_top_n)
    problem.to_csv(tables_dir / "table_qc_flag_problem_clusters.csv", index=False)
    tables["problem_clusters"] = problem
    print("Wrote {} ({:,} rows)".format(tables_dir / "table_qc_flag_problem_clusters.csv", len(problem)))
    return tables


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate QC flag statistics tables and figures.")
    parser.add_argument("--input-master-nc", default="", help="Input master NetCDF. Default: release master if present, otherwise s6 master.")
    parser.add_argument("--quality-order-csv", default=str(DEFAULT_QUALITY_ORDER_CSV), help="s6 quality-order CSV used to recover source_type/source_family.")
    parser.add_argument("--tables-dir", default=str(DEFAULT_TABLES_DIR), help="Output directory for CSV tables.")
    parser.add_argument("--figures-dir", default=str(DEFAULT_FIGURES_DIR), help="Output directory for figures.")
    parser.add_argument("--skip-stage-flags", action="store_true", help="Only summarize final Q_flag/SSC_flag/SSL_flag.")
    parser.add_argument("--problem-cluster-top-n", type=int, default=100, help="Number of highest suspect+bad+missing clusters to write. Use 0 for all.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if nc4 is None:
        print("Error: netCDF4 is required. Install it with: pip install netCDF4", file=sys.stderr)
        return 1

    master_nc = resolve_path(args.input_master_nc) if args.input_master_nc else default_master_nc()
    quality_order_csv = resolve_path(args.quality_order_csv) if args.quality_order_csv else None
    tables_dir = resolve_path(args.tables_dir)
    figures_dir = resolve_path(args.figures_dir)

    print("Input master NetCDF: {}".format(master_nc))
    print("Quality-order CSV: {}".format(quality_order_csv))
    print("Tables directory: {}".format(tables_dir))
    print("Figures directory: {}".format(figures_dir))

    records, flag_arrays, flag_specs, meaning_maps = read_master_records(master_nc, include_stage_flags=not args.skip_stage_flags)
    if args.skip_stage_flags:
        flag_specs = [spec for spec in flag_specs if spec.get("qc_level") == "final"]
        keep = {spec["flag_variable"] for spec in flag_specs}
        flag_arrays = {name: arr for name, arr in flag_arrays.items() if name in keep}
        meaning_maps = {name: value for name, value in meaning_maps.items() if name in keep}

    records = attach_source_type(records, quality_order_csv)
    print("Loaded {:,} records, {} flag variables: {}".format(len(records), len(flag_specs), ", ".join(spec["flag_variable"] for spec in flag_specs)))

    tables = write_tables(records, flag_arrays, flag_specs, meaning_maps, tables_dir, int(args.problem_cluster_top_n))
    write_figures(tables["summary"], tables["by_source"], figures_dir)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
