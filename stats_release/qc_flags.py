#!/usr/bin/env python3
"""QC flag statistics from release NetCDF products only."""
# ---- Library path setup: MUST happen before any extension-module imports ----
import os as _os
import ctypes as _ctypes
from pathlib import Path as _Path
_conda_lib = "/share/home/dq134/.conda/envs/wzx/lib"
if _os.path.isdir(_conda_lib):
    _os.environ["LD_LIBRARY_PATH"] = _conda_lib + _os.pathsep + _os.environ.get("LD_LIBRARY_PATH", "")
    try:
        _ctypes.CDLL(str(_Path(_conda_lib) / "libstdc++.so.6"), mode=_ctypes.RTLD_GLOBAL)
    except Exception:
        pass
del _os, _ctypes, _Path, _conda_lib
# ---------------------------------------------------------------------------





import argparse
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import add_common_args, context_from_args, copy_report_to_docs, setup_matplotlib, write_csv, write_markdown
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import pct, resolution_values, save_figure
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


DEFAULT_FLAG_MEANINGS = {0: "good", 1: "estimated", 2: "suspect", 3: "bad", 8: "not_checked", 9: "missing"}
FLAG_COLORS = {0: "#2ca02c", 1: "#1f77b4", 2: "#ff7f0e", 3: "#d62728", 8: "#9467bd", 9: "#7f7f7f"}
PRODUCT_GROUPS = {"master": "main", "climatology": "climatology", "satellite": "satellite"}
FINAL_TO_STAGE_FLAGS = {
    "Q": ("Q_qc1", "Q_qc2"),
    "SSC": ("SSC_qc1", "SSC_qc2", "SSC_qc3"),
    "SSL": ("SSL_qc1", "SSL_qc2", "SSL_qc3"),
}
YEARLY_TREND_PRODUCTS = (
    ("master_nc", "master"),
    ("climatology_nc", "climatology"),
    ("satellite_nc", "satellite"),
)
CLUSTER_PRODUCTS = (
    ("master_nc", "master", "station_index", "cluster_id"),
    ("satellite_nc", "satellite", "satellite_station_index", "cluster_id_station"),
)


def _flag_mapping(var) -> dict:
    values = getattr(var, "flag_values", None)
    meanings = str(getattr(var, "flag_meanings", "")).split()
    mapping = {}
    if values is not None and meanings:
        raw_values = np.asarray(values).reshape(-1)
        for value, meaning in zip(raw_values, meanings):
            try:
                mapping[int(value)] = str(meaning)
            except Exception:
                pass
    for value, meaning in DEFAULT_FLAG_MEANINGS.items():
        mapping.setdefault(value, meaning)
    return mapping


def _declared_flag_values(var, mapping: dict) -> list:
    values = getattr(var, "flag_values", None)
    if values is None:
        return sorted(mapping)
    declared = []
    for value in np.asarray(values).reshape(-1):
        try:
            declared.append(int(value))
        except Exception:
            pass
    return sorted(set(declared))


def _product_group(product: str) -> str:
    return PRODUCT_GROUPS.get(str(product), str(product))


def _count_flags_for_product(ctx, file_name: str, product: str, chunk_size: int) -> tuple:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(), pd.DataFrame()
    rows = []
    schema_rows = []
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = len(ds.dimensions.get("n_records", [])) or len(ds.dimensions.get("n_satellite_records", []))
        flag_vars = [name for name in ds.variables if name.endswith("_flag") or "_qc" in name]
        for flag_var in sorted(flag_vars):
            var = ds.variables[flag_var]
            dtype_kind = getattr(var.dtype, "kind", "")
            if dtype_kind not in {"i", "u", "f"}:
                continue
            meaning_map = _flag_mapping(var)
            for value in _declared_flag_values(var, meaning_map):
                schema_rows.append(
                    {
                        "product": product,
                        "flag_variable": flag_var,
                        "flag_value": int(value),
                        "flag_meaning": meaning_map.get(int(value), DEFAULT_FLAG_MEANINGS.get(int(value), "other")),
                        "long_name": getattr(var, "long_name", ""),
                    }
                )
            counts = {}
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                try:
                    arr = np.ma.asarray(var[start:stop]).filled(9).reshape(-1)
                except Exception:
                    continue
                numeric = pd.to_numeric(pd.Series(arr), errors="coerce").dropna().astype(int).to_numpy()
                if numeric.size == 0:
                    continue
                for value, cnt in zip(*np.unique(numeric, return_counts=True)):
                    counts[int(value)] = counts.get(int(value), 0) + int(cnt)
            total = sum(counts.values())
            for value, cnt in sorted(counts.items()):
                rows.append(
                    {
                        "product": product,
                        "flag_variable": flag_var,
                        "flag_value": int(value),
                        "flag_meaning": meaning_map.get(int(value), DEFAULT_FLAG_MEANINGS.get(int(value), "other")),
                        "count": int(cnt),
                        "percent": round(100.0 * cnt / total, 6) if total else 0.0,
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(schema_rows)


def _read_text_slice(ds, name: str, start: int, stop: int) -> np.ndarray:
    if name not in ds.variables:
        return np.asarray([""] * max(0, stop - start), dtype=object)
    arr = np.asarray(ds.variables[name][start:stop], dtype=object).reshape(-1)
    return np.asarray([str(item).strip() for item in arr], dtype=object)


def _read_text_all(ds, name: str) -> np.ndarray:
    if name not in ds.variables:
        return np.asarray([], dtype=object)
    arr = np.asarray(ds.variables[name][:], dtype=object).reshape(-1)
    return np.asarray([str(item).strip() for item in arr], dtype=object)


def _read_numeric_slice(ds, name: str, start: int, stop: int, fill_value: int = -1) -> np.ndarray:
    arr = np.ma.asarray(ds.variables[name][start:stop])
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(fill_value)
    return np.asarray(arr, dtype=np.int64).reshape(-1)


def _read_flag_slice(ds, name: str, start: int, stop: int, fill_value: int = 9) -> np.ndarray:
    arr = np.ma.asarray(ds.variables[name][start:stop]).filled(fill_value).reshape(-1)
    return np.asarray(arr, dtype=np.int16)


def _record_dimension(ds) -> tuple[str, int]:
    for name in ("n_records", "n_satellite_records"):
        if name in ds.dimensions:
            return name, len(ds.dimensions[name])
    return "", 0


def _read_resolution_slice(ds, start: int, stop: int) -> np.ndarray:
    if "resolution" not in ds.variables:
        return np.asarray(["unknown"] * max(0, stop - start), dtype=object)
    var = ds.variables["resolution"]
    if getattr(var.dtype, "kind", "") in {"i", "u", "f"}:
        return resolution_values(ds, slice(start, stop))
    values = _read_text_slice(ds, "resolution", start, stop)
    values[values == ""] = "unknown"
    return values


def _read_year_slice(ds, start: int, stop: int) -> np.ndarray:
    n = max(0, stop - start)
    years = np.full(n, np.nan, dtype="float64")
    if "time" not in ds.variables:
        return years
    time_var = ds.variables["time"]
    values = np.ma.asarray(time_var[start:stop]).astype("float64")
    if np.ma.isMaskedArray(values):
        values = values.filled(np.nan)
    values = np.asarray(values, dtype="float64").reshape(-1)
    valid = np.isfinite(values)
    if not np.any(valid):
        return years
    import netCDF4 as nc4

    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        dates = nc4.num2date(values[valid], units=units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        dates = nc4.num2date(values[valid], units=units, calendar=calendar)
    for idx, date in zip(np.flatnonzero(valid), dates):
        year = getattr(date, "year", None)
        if year is None:
            parsed = pd.to_datetime(str(date), errors="coerce")
            if pd.isna(parsed):
                continue
            year = parsed.year
        years[idx] = int(year)
    return years


def _flag_class_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    meanings = frame["flag_meaning"].astype(str)
    out = frame.copy()
    out = out.assign(
        is_good=meanings.isin(["good", "pass", "not_propagated"]) | out["flag_value"].eq(0),
        is_estimated=meanings.isin(["estimated", "derived"]) | out["flag_value"].eq(1),
        is_suspect=meanings.eq("suspect") | out["flag_value"].eq(2),
        is_bad=meanings.eq("bad") | out["flag_value"].eq(3),
        is_missing=meanings.eq("missing") | out["flag_value"].eq(9),
        is_not_checked=meanings.eq("not_checked") | out["flag_value"].eq(8),
    )
    out["is_usable"] = out["is_good"] | out["is_estimated"]
    out["is_problem"] = out["is_suspect"] | out["is_bad"]
    return out


def _health_from_counts(counts: pd.DataFrame, group_cols) -> pd.DataFrame:
    if counts.empty:
        return pd.DataFrame()
    classified = _flag_class_columns(counts)
    rows = []
    for keys, group in classified.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        total = int(group["count"].sum())
        good = int(group.loc[group["is_good"], "count"].sum())
        estimated = int(group.loc[group["is_estimated"], "count"].sum())
        usable = int(group.loc[group["is_usable"], "count"].sum())
        suspect = int(group.loc[group["is_suspect"], "count"].sum())
        bad = int(group.loc[group["is_bad"], "count"].sum())
        missing = int(group.loc[group["is_missing"], "count"].sum())
        not_checked = int(group.loc[group["is_not_checked"], "count"].sum())
        problem = int(group.loc[group["is_problem"], "count"].sum())
        row.update(
            {
                "n_total": total,
                "good_count": good,
                "derived_count": estimated,
                "analysis_ready_count": usable,
                "usable_count": usable,
                "suspect_count": suspect,
                "bad_count": bad,
                "not_checked_count": not_checked,
                "missing_count": missing,
                "problem_count": problem,
                "good_rate": pct(good, total),
                "derived_rate": pct(estimated, total),
                "analysis_ready_rate": pct(usable, total),
                "usable_rate": pct(usable, total),
                "suspect_rate": pct(suspect, total),
                "bad_rate": pct(bad, total),
                "not_checked_rate": pct(not_checked, total),
                "missing_rate": pct(missing, total),
                "problem_rate": pct(problem, total),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def _build_main_flag_counts_by_resolution(ctx, chunk_size: int) -> pd.DataFrame:
    path = ctx.require_input(ctx.release_file(PRODUCT_FILES["master_nc"]), required=False)
    if path is None:
        return pd.DataFrame()
    rows = []
    with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
        if "n_records" not in ds.dimensions:
            return pd.DataFrame()
        n_records = len(ds.dimensions["n_records"])
        flag_vars = [name for name in ds.variables if name.endswith("_flag") or "_qc" in name]
        for flag_var in sorted(flag_vars):
            var = ds.variables[flag_var]
            if getattr(var.dtype, "kind", "") not in {"i", "u", "f"}:
                continue
            meaning_map = _flag_mapping(var)
            counts = {}
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                resolution = resolution_values(ds, slice(start, stop))
                values = _read_flag_slice(ds, flag_var, start, stop)
                frame = pd.DataFrame({"temporal_resolution": resolution, "flag_value": values})
                grouped = frame.groupby(["temporal_resolution", "flag_value"], dropna=False).size().reset_index(name="count")
                for _, row in grouped.iterrows():
                    key = (str(row["temporal_resolution"]), int(row["flag_value"]))
                    counts[key] = counts.get(key, 0) + int(row["count"])
            variable = _variable_from_flag(flag_var)
            qc_level, qc_stage = _stage_from_flag(flag_var)
            for (resolution, value), count in sorted(counts.items()):
                rows.append(
                    {
                        "product_group": "main",
                        "temporal_resolution": resolution,
                        "qc_level": qc_level,
                        "qc_stage": qc_stage,
                        "variable": variable,
                        "flag_variable": flag_var,
                        "flag": int(value),
                        "flag_value": int(value),
                        "meaning": meaning_map.get(int(value), DEFAULT_FLAG_MEANINGS.get(int(value), "other")),
                        "flag_meaning": meaning_map.get(int(value), DEFAULT_FLAG_MEANINGS.get(int(value), "other")),
                        "count": int(count),
                    }
                )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    group_cols = ["product_group", "temporal_resolution", "flag_variable"]
    out["n_total"] = out.groupby(group_cols, dropna=False)["count"].transform("sum").astype(int)
    out["percentage"] = out.apply(lambda r: pct(r["count"], r["n_total"]), axis=1)
    order = {"daily": 0, "monthly": 1, "annual": 2}
    out["_resolution_order"] = out["temporal_resolution"].map(order).fillna(99)
    out = out.sort_values(["_resolution_order", "variable", "flag_variable", "flag"]).drop(columns=["_resolution_order"])
    return out.reset_index(drop=True)


def _build_main_resolution_health(flag_by_resolution: pd.DataFrame) -> pd.DataFrame:
    if flag_by_resolution.empty:
        return pd.DataFrame()
    final_counts = flag_by_resolution[flag_by_resolution["qc_level"].eq("final")].copy()
    if final_counts.empty:
        return pd.DataFrame()
    health = _health_from_counts(
        final_counts,
        ["product_group", "temporal_resolution", "variable", "flag_variable"],
    )
    order = {"daily": 0, "monthly": 1, "annual": 2}
    health["_resolution_order"] = health["temporal_resolution"].map(order).fillna(99)
    health = health.sort_values(["_resolution_order", "variable", "flag_variable"]).drop(columns=["_resolution_order"])
    return health.reset_index(drop=True)


def _build_yearly_final_flag_counts(ctx, chunk_size: int) -> pd.DataFrame:
    rows = []
    for product_key, release_component in YEARLY_TREND_PRODUCTS:
        path = ctx.require_input(ctx.release_file(PRODUCT_FILES[product_key]), required=False)
        if path is None:
            continue
        with ctx.open_dataset(PRODUCT_FILES[product_key], required=True) as ds:
            record_dim, n_records = _record_dimension(ds)
            if not record_dim or n_records <= 0:
                continue
            flag_vars = [
                name
                for name in sorted(ds.variables)
                if name.endswith("_flag")
                and getattr(ds.variables[name].dtype, "kind", "") in {"i", "u", "f"}
                and ds.variables[name].dimensions[:1] == (record_dim,)
            ]
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                years = _read_year_slice(ds, start, stop)
                valid_year = np.isfinite(years)
                if not np.any(valid_year):
                    continue
                resolution = _read_resolution_slice(ds, start, stop)
                for flag_var in flag_vars:
                    values = _read_flag_slice(ds, flag_var, start, stop)
                    frame = pd.DataFrame(
                        {
                            "year": years[valid_year].astype(int),
                            "temporal_resolution": resolution[valid_year],
                            "flag_value": values[valid_year],
                        }
                    )
                    grouped = (
                        frame.groupby(["year", "temporal_resolution", "flag_value"], dropna=False)
                        .size()
                        .reset_index(name="count")
                    )
                    meaning_map = _flag_mapping(ds.variables[flag_var])
                    variable = _variable_from_flag(flag_var)
                    for _, row in grouped.iterrows():
                        flag_value = int(row["flag_value"])
                        rows.append(
                            {
                                "product_group": _product_group(release_component),
                                "release_component": release_component,
                                "year": int(row["year"]),
                                "temporal_resolution": str(row["temporal_resolution"]),
                                "qc_level": "final",
                                "qc_stage": "final",
                                "variable": variable,
                                "flag_variable": flag_var,
                                "flag": flag_value,
                                "flag_value": flag_value,
                                "meaning": meaning_map.get(flag_value, DEFAULT_FLAG_MEANINGS.get(flag_value, "other")),
                                "flag_meaning": meaning_map.get(flag_value, DEFAULT_FLAG_MEANINGS.get(flag_value, "other")),
                                "count": int(row["count"]),
                            }
                        )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    group_cols = ["product_group", "release_component", "year", "temporal_resolution", "flag_variable"]
    out = (
        out.groupby(
            [
                "product_group",
                "release_component",
                "year",
                "temporal_resolution",
                "qc_level",
                "qc_stage",
                "variable",
                "flag_variable",
                "flag",
                "flag_value",
                "meaning",
                "flag_meaning",
            ],
            dropna=False,
        )
        .agg(count=("count", "sum"))
        .reset_index()
    )
    out["n_total"] = out.groupby(group_cols, dropna=False)["count"].transform("sum").astype(int)
    out["percentage"] = out.apply(lambda r: pct(r["count"], r["n_total"]), axis=1)
    order = {"daily": 0, "monthly": 1, "annual": 2, "climatology": 3, "other": 4, "unknown": 5}
    out["_product_order"] = out["release_component"].map({"master": 0, "climatology": 1, "satellite": 2}).fillna(99)
    out["_resolution_order"] = out["temporal_resolution"].map(order).fillna(99)
    out = out.sort_values(["_product_order", "year", "_resolution_order", "variable", "flag"]).drop(
        columns=["_product_order", "_resolution_order"]
    )
    return out.reset_index(drop=True)


def _build_yearly_final_flag_trends(flag_by_year: pd.DataFrame) -> pd.DataFrame:
    if flag_by_year.empty:
        return pd.DataFrame()
    yearly = _health_from_counts(
        flag_by_year,
        ["product_group", "release_component", "year", "temporal_resolution", "variable", "flag_variable"],
    )
    if yearly.empty:
        return yearly
    yearly["n_records"] = yearly["n_total"]
    yearly["suspect_bad_count"] = yearly["problem_count"]
    yearly["suspect_bad_rate"] = yearly["problem_rate"]
    columns = [
        "product_group",
        "release_component",
        "year",
        "temporal_resolution",
        "variable",
        "flag_variable",
        "n_records",
        "analysis_ready_count",
        "suspect_bad_count",
        "missing_count",
        "not_checked_count",
        "good_count",
        "derived_count",
        "analysis_ready_rate",
        "suspect_bad_rate",
        "missing_rate",
        "not_checked_rate",
        "good_rate",
        "derived_rate",
    ]
    yearly = yearly[[col for col in columns if col in yearly.columns]]
    order = {"daily": 0, "monthly": 1, "annual": 2, "climatology": 3, "other": 4, "unknown": 5}
    yearly["_product_order"] = yearly["release_component"].map({"master": 0, "climatology": 1, "satellite": 2}).fillna(99)
    yearly["_resolution_order"] = yearly["temporal_resolution"].map(order).fillna(99)
    yearly = yearly.sort_values(["_product_order", "year", "_resolution_order", "variable"]).drop(
        columns=["_product_order", "_resolution_order"]
    )
    return yearly.reset_index(drop=True)


def _build_problem_clusters(ctx, chunk_size: int) -> pd.DataFrame:
    rows = []
    for product_key, release_component, station_index_var, station_cluster_id_var in CLUSTER_PRODUCTS:
        path = ctx.require_input(ctx.release_file(PRODUCT_FILES[product_key]), required=False)
        if path is None:
            continue
        with ctx.open_dataset(PRODUCT_FILES[product_key], required=True) as ds:
            record_dim, n_records = _record_dimension(ds)
            if not record_dim or n_records <= 0:
                continue
            if station_index_var not in ds.variables or "cluster_uid" not in ds.variables:
                continue
            cluster_uids = _read_text_all(ds, "cluster_uid")
            if cluster_uids.size == 0:
                continue
            if station_cluster_id_var in ds.variables:
                station_cluster_ids = np.asarray(ds.variables[station_cluster_id_var][:], dtype=np.int64).reshape(-1)
            else:
                station_cluster_ids = np.arange(cluster_uids.size, dtype=np.int64)
            flag_vars = [
                name
                for name in sorted(ds.variables)
                if name.endswith("_flag")
                and getattr(ds.variables[name].dtype, "kind", "") in {"i", "u", "f"}
                and ds.variables[name].dimensions[:1] == (record_dim,)
            ]
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                station_index = _read_numeric_slice(ds, station_index_var, start, stop)
                valid_station = (station_index >= 0) & (station_index < cluster_uids.size)
                if not np.any(valid_station):
                    continue
                cluster_uid = np.full(stop - start, "", dtype=object)
                cluster_id = np.full(stop - start, -1, dtype=np.int64)
                cluster_uid[valid_station] = cluster_uids[station_index[valid_station]]
                valid_id = valid_station & (station_index < station_cluster_ids.size)
                cluster_id[valid_id] = station_cluster_ids[station_index[valid_id]]
                valid_cluster = valid_station & (cluster_uid != "")
                if not np.any(valid_cluster):
                    continue
                resolution = _read_resolution_slice(ds, start, stop)
                for flag_var in flag_vars:
                    values = _read_flag_slice(ds, flag_var, start, stop)
                    frame = pd.DataFrame(
                        {
                            "cluster_uid": cluster_uid[valid_cluster],
                            "cluster_id": cluster_id[valid_cluster],
                            "temporal_resolution": resolution[valid_cluster],
                            "n_records": 1,
                            "analysis_ready_count": np.isin(values[valid_cluster], [0, 1]).astype(int),
                            "suspect_bad_count": np.isin(values[valid_cluster], [2, 3]).astype(int),
                            "missing_count": (values[valid_cluster] == 9).astype(int),
                            "good_count": (values[valid_cluster] == 0).astype(int),
                            "derived_count": (values[valid_cluster] == 1).astype(int),
                        }
                    )
                    grouped = (
                        frame.groupby(["cluster_uid", "cluster_id", "temporal_resolution"], dropna=False)
                        .agg(
                            n_records=("n_records", "sum"),
                            analysis_ready_count=("analysis_ready_count", "sum"),
                            suspect_bad_count=("suspect_bad_count", "sum"),
                            missing_count=("missing_count", "sum"),
                            good_count=("good_count", "sum"),
                            derived_count=("derived_count", "sum"),
                        )
                        .reset_index()
                    )
                    variable = _variable_from_flag(flag_var)
                    for _, row in grouped.iterrows():
                        rows.append(
                            {
                                "product_group": _product_group(release_component),
                                "release_component": release_component,
                                "cluster_uid": str(row["cluster_uid"]),
                                "cluster_id": int(row["cluster_id"]),
                                "temporal_resolution": str(row["temporal_resolution"]),
                                "variable": variable,
                                "flag_variable": flag_var,
                                "n_records": int(row["n_records"]),
                                "analysis_ready_count": int(row["analysis_ready_count"]),
                                "suspect_bad_count": int(row["suspect_bad_count"]),
                                "missing_count": int(row["missing_count"]),
                                "good_count": int(row["good_count"]),
                                "derived_count": int(row["derived_count"]),
                            }
                        )
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    out = (
        out.groupby(
            [
                "product_group",
                "release_component",
                "cluster_uid",
                "cluster_id",
                "temporal_resolution",
                "variable",
                "flag_variable",
            ],
            dropna=False,
        )
        .agg(
            n_records=("n_records", "sum"),
            analysis_ready_count=("analysis_ready_count", "sum"),
            suspect_bad_count=("suspect_bad_count", "sum"),
            missing_count=("missing_count", "sum"),
            good_count=("good_count", "sum"),
            derived_count=("derived_count", "sum"),
        )
        .reset_index()
    )
    out["analysis_ready_rate"] = out.apply(lambda r: pct(r["analysis_ready_count"], r["n_records"]), axis=1)
    out["suspect_bad_rate"] = out.apply(lambda r: pct(r["suspect_bad_count"], r["n_records"]), axis=1)
    out["missing_rate"] = out.apply(lambda r: pct(r["missing_count"], r["n_records"]), axis=1)
    out["good_rate"] = out.apply(lambda r: pct(r["good_count"], r["n_records"]), axis=1)
    out["derived_rate"] = out.apply(lambda r: pct(r["derived_count"], r["n_records"]), axis=1)
    out["grouping_level"] = "cluster_variable_resolution"
    clusters = out[out["suspect_bad_count"].gt(0)].copy()
    columns = [
        "cluster_uid",
        "cluster_id",
        "grouping_level",
        "product_group",
        "release_component",
        "temporal_resolution",
        "variable",
        "flag_variable",
        "n_records",
        "analysis_ready_count",
        "suspect_bad_count",
        "missing_count",
        "analysis_ready_rate",
        "suspect_bad_rate",
        "missing_rate",
        "good_count",
        "derived_count",
        "good_rate",
        "derived_rate",
    ]
    clusters = clusters[[col for col in columns if col in clusters.columns]]
    return clusters.sort_values(["suspect_bad_count", "suspect_bad_rate"], ascending=[False, False]).reset_index(drop=True)


def _build_final_good_stage_missing_by_source_resolution(ctx, chunk_size: int) -> pd.DataFrame:
    path = ctx.require_input(ctx.release_file(PRODUCT_FILES["master_nc"]), required=False)
    if path is None:
        return pd.DataFrame()
    rows = []
    with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
        if "n_records" not in ds.dimensions:
            return pd.DataFrame()
        n_records = len(ds.dimensions["n_records"])
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            source = _read_text_slice(ds, "source", start, stop)
            source[source == ""] = "unknown"
            resolution = resolution_values(ds, slice(start, stop))
            for variable, stage_flags in FINAL_TO_STAGE_FLAGS.items():
                final_flag = "{}_flag".format(variable)
                if final_flag not in ds.variables:
                    continue
                final = _read_flag_slice(ds, final_flag, start, stop)
                final_good = final == 0
                if not np.any(final_good):
                    continue
                for stage_flag in stage_flags:
                    if stage_flag not in ds.variables:
                        continue
                    stage = _read_flag_slice(ds, stage_flag, start, stop)
                    frame = pd.DataFrame(
                        {
                            "source_dataset": source[final_good],
                            "temporal_resolution": resolution[final_good],
                            "stage_flag": stage[final_good],
                        }
                    )
                    grouped = (
                        frame.assign(
                            final_good_count=1,
                            stage_missing_count=frame["stage_flag"].eq(9).astype(int),
                            stage_not_checked_count=frame["stage_flag"].eq(8).astype(int),
                            stage_pass_count=frame["stage_flag"].eq(0).astype(int),
                        )
                        .groupby(["source_dataset", "temporal_resolution"], dropna=False)
                        .agg(
                            final_good_count=("final_good_count", "sum"),
                            stage_missing_count=("stage_missing_count", "sum"),
                            stage_not_checked_count=("stage_not_checked_count", "sum"),
                            stage_pass_count=("stage_pass_count", "sum"),
                        )
                        .reset_index()
                    )
                    grouped.insert(0, "product_group", "main")
                    grouped.insert(1, "release_component", "master")
                    grouped.insert(5, "variable", variable)
                    grouped.insert(6, "qc_stage", _stage_from_flag(stage_flag)[1])
                    grouped.insert(7, "stage_flag_variable", stage_flag)
                    rows.append(grouped)
    if not rows:
        return pd.DataFrame(
            columns=[
                "product_group",
                "release_component",
                "source_dataset",
                "temporal_resolution",
                "variable",
                "qc_stage",
                "stage_flag_variable",
                "final_good_count",
                "stage_missing_count",
                "stage_missing_rate",
                "stage_not_checked_count",
                "stage_not_checked_rate",
                "stage_pass_count",
                "stage_pass_rate",
            ]
        )
    out = (
        pd.concat(rows, ignore_index=True)
        .groupby(
            [
                "product_group",
                "release_component",
                "source_dataset",
                "temporal_resolution",
                "variable",
                "qc_stage",
                "stage_flag_variable",
            ],
            dropna=False,
        )
        .agg(
            final_good_count=("final_good_count", "sum"),
            stage_missing_count=("stage_missing_count", "sum"),
            stage_not_checked_count=("stage_not_checked_count", "sum"),
            stage_pass_count=("stage_pass_count", "sum"),
        )
        .reset_index()
    )
    out["stage_missing_rate"] = out.apply(lambda r: pct(r["stage_missing_count"], r["final_good_count"]), axis=1)
    out["stage_not_checked_rate"] = out.apply(lambda r: pct(r["stage_not_checked_count"], r["final_good_count"]), axis=1)
    out["stage_pass_rate"] = out.apply(lambda r: pct(r["stage_pass_count"], r["final_good_count"]), axis=1)
    out = out[
        [
            "product_group",
            "release_component",
            "source_dataset",
            "temporal_resolution",
            "variable",
            "qc_stage",
            "stage_flag_variable",
            "final_good_count",
            "stage_missing_count",
            "stage_missing_rate",
            "stage_not_checked_count",
            "stage_not_checked_rate",
            "stage_pass_count",
            "stage_pass_rate",
        ]
    ]
    return out.sort_values(["stage_missing_count", "final_good_count"], ascending=[False, False]).reset_index(drop=True)


def build_qc_stats(ctx, chunk_size: int) -> dict:
    pieces = []
    schemas = []
    for product_key, product in (("master_nc", "master"), ("climatology_nc", "climatology"), ("satellite_nc", "satellite")):
        counts_i, schema_i = _count_flags_for_product(ctx, PRODUCT_FILES[product_key], product, chunk_size)
        pieces.append(counts_i)
        schemas.append(schema_i)
    counts = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    schema = pd.concat(schemas, ignore_index=True).drop_duplicates() if schemas else pd.DataFrame()
    health = pd.DataFrame()
    if not counts.empty:
        meanings = counts["flag_meaning"].astype(str)
        counts = counts.assign(
            is_good=meanings.isin(["good", "pass", "not_propagated"]) | counts["flag_value"].eq(0),
            is_estimated=meanings.isin(["estimated", "derived"]) | counts["flag_value"].eq(1),
            is_suspect=meanings.eq("suspect") | counts["flag_value"].eq(2),
            is_bad=meanings.eq("bad") | counts["flag_value"].eq(3),
            is_missing=meanings.eq("missing") | counts["flag_value"].eq(9),
            is_not_checked=meanings.eq("not_checked") | counts["flag_value"].eq(8),
        )
        counts["is_usable"] = counts["is_good"] | counts["is_estimated"]
        health = (
            counts.assign(is_problem=counts["is_suspect"] | counts["is_bad"])
            .groupby(["product", "flag_variable"], dropna=False)
            .apply(
                lambda g: pd.Series(
                    {
                        "total_flags": int(g["count"].sum()),
                        "good_count": int(g.loc[g["is_good"], "count"].sum()),
                        "estimated_count": int(g.loc[g["is_estimated"], "count"].sum()),
                        "usable_count": int(g.loc[g["is_usable"], "count"].sum()),
                        "suspect_count": int(g.loc[g["is_suspect"], "count"].sum()),
                        "bad_count": int(g.loc[g["is_bad"], "count"].sum()),
                        "missing_count": int(g.loc[g["is_missing"], "count"].sum()),
                        "not_checked_count": int(g.loc[g["is_not_checked"], "count"].sum()),
                        "problem_count": int(g.loc[g["is_problem"], "count"].sum()),
                    }
                )
            )
            .reset_index()
        )
        health["good_percent"] = health.apply(
            lambda row: round(100.0 * row["good_count"] / row["total_flags"], 6) if row["total_flags"] else 0.0,
            axis=1,
        )
        health["usable_percent"] = health.apply(
            lambda row: round(100.0 * row["usable_count"] / row["total_flags"], 6) if row["total_flags"] else 0.0,
            axis=1,
        )
        health["problem_percent"] = health.apply(
            lambda row: round(100.0 * row["problem_count"] / row["total_flags"], 6) if row["total_flags"] else 0.0,
            axis=1,
        )
        counts = counts.drop(
            columns=[
                "is_good",
                "is_estimated",
                "is_suspect",
                "is_bad",
                "is_missing",
                "is_not_checked",
                "is_usable",
            ]
        )
    legacy = _build_legacy_tables(counts, health)
    final_good_stage_missing = _build_final_good_stage_missing_by_source_resolution(ctx, chunk_size)
    flag_by_resolution = _build_main_flag_counts_by_resolution(ctx, chunk_size)
    resolution_health_kpis = _build_main_resolution_health(flag_by_resolution)
    flag_by_year = _build_yearly_final_flag_counts(ctx, chunk_size)
    yearly_trends = _build_yearly_final_flag_trends(flag_by_year)
    problem_clusters = _build_problem_clusters(ctx, chunk_size)
    legacy["flag_by_resolution"] = flag_by_resolution
    legacy["flag_by_year"] = flag_by_year
    legacy["yearly_trends"] = yearly_trends
    legacy["flag_by_cluster"] = pd.DataFrame()
    legacy["flag_problem_clusters"] = problem_clusters

    # ---- final flags by resolution matrix ----
    flag_summary = flag_by_resolution
    matrix_final_flags_by_resolution = pd.DataFrame()
    if not flag_summary.empty:
        final_flags = flag_summary[flag_summary["qc_level"].eq("final")].copy()
        if not final_flags.empty:
            final_flags["resolution"] = final_flags["temporal_resolution"]
            matrix_final_flags_by_resolution = (
                final_flags.groupby(["resolution", "flag_variable", "flag"], dropna=False)
                .agg(count=("count", "sum"))
                .reset_index()
                .rename(columns={"flag": "flag_value"})
            )

    return {
        "flag_counts": counts,
        "health": health,
        "flag_schema": schema,
        "matrix_final_flags_by_resolution": matrix_final_flags_by_resolution,
        "final_good_stage_missing_by_source_resolution": final_good_stage_missing,
        "resolution_health_kpis": resolution_health_kpis,
        "cluster_problem_kpis": problem_clusters,
        **legacy,
    }


def _variable_from_flag(flag_variable: str) -> str:
    text = str(flag_variable)
    for suffix in ("_flag", "_qc1", "_qc2", "_qc3", "_qc4"):
        if text.endswith(suffix):
            return text[: -len(suffix)]
    return text.split("_")[0]


def _stage_from_flag(flag_variable: str) -> tuple:
    if flag_variable.endswith("_flag"):
        return "final", "final"
    if flag_variable.endswith("_qc1"):
        return "stage", "physical_plausibility"
    if flag_variable.endswith("_qc2"):
        return "stage", "log_iqr"
    if flag_variable.endswith("_qc3"):
        return "stage", "ssc_q_consistency"
    return "stage", "other"


def _build_legacy_tables(counts: pd.DataFrame, health: pd.DataFrame) -> dict:
    if counts.empty:
        empty = pd.DataFrame()
        return {
            "flag_summary": empty,
            "flag_by_source": empty,
            "flag_by_resolution": empty,
            "flag_by_variable": empty,
            "flag_by_year": empty,
            "flag_by_cluster": empty,
            "flag_problem_clusters": empty,
            "health_kpis": empty,
            "stage_effectiveness": empty,
            "issue_hotspots": empty,
            "yearly_trends": empty,
        }
    rows = []
    for _, row in counts.iterrows():
        variable = _variable_from_flag(row["flag_variable"])
        qc_level, qc_stage = _stage_from_flag(str(row["flag_variable"]))
        product = str(row["product"])
        rows.append(
            {
                "qc_level": qc_level,
                "qc_stage": qc_stage,
                "product_group": _product_group(product),
                "release_component": product,
                "temporal_resolution": product,
                "variable": variable,
                "flag_variable": row["flag_variable"],
                "flag": int(row["flag_value"]),
                "meaning": row["flag_meaning"],
                "count": int(row["count"]),
                "percentage": row["percent"],
                "n_total": int(counts[counts["flag_variable"].eq(row["flag_variable"]) & counts["product"].eq(row["product"])]["count"].sum()),
            }
        )
    summary = pd.DataFrame(rows)
    by_variable = (
        summary.groupby(["qc_level", "qc_stage", "variable", "flag_variable", "flag", "meaning"], dropna=False)
        .agg(count=("count", "sum"))
        .reset_index()
    )
    by_variable["n_total"] = by_variable.groupby(["qc_level", "qc_stage", "variable", "flag_variable"], dropna=False)[
        "count"
    ].transform("sum")
    by_variable["percentage"] = by_variable.apply(lambda r: pct(r["count"], r["n_total"]), axis=1)
    by_resolution = summary.copy()
    by_source = summary.copy()
    by_source.insert(0, "source_dataset", "all_release_sources")
    by_source.insert(1, "source_type", "all")
    by_year = pd.DataFrame()
    by_cluster = pd.DataFrame(
        columns=[
            "cluster_uid",
            "cluster_id",
            "temporal_resolution",
            "variable",
            "flag_variable",
            "flag",
            "meaning",
            "count",
            "percentage",
            "n_total",
        ]
    )
    health_rows = []
    for _, row in health.iterrows():
        variable = _variable_from_flag(row["flag_variable"])
        total = int(row["total_flags"])
        product = str(row["product"])
        health_rows.append(
            {
                "product_group": _product_group(product),
                "release_component": product,
                "temporal_resolution": product,
                "variable": variable,
                "flag_variable": row["flag_variable"],
                "n_total": total,
                "good_count": int(row["good_count"]),
                "derived_count": int(row["estimated_count"]),
                "suspect_count": int(row["suspect_count"]),
                "bad_count": int(row["bad_count"]),
                "not_checked_count": int(row["not_checked_count"]),
                "missing_count": int(row["missing_count"]),
                "usable_count": int(row["usable_count"]),
                "problem_count": int(row["problem_count"]),
                "good_rate": pct(row["good_count"], total),
                "derived_rate": pct(row["estimated_count"], total),
                "suspect_rate": pct(row["suspect_count"], total),
                "bad_rate": pct(row["bad_count"], total),
                "not_checked_rate": pct(row["not_checked_count"], total),
                "missing_rate": pct(row["missing_count"], total),
                "usable_rate": pct(row["usable_count"], total),
                "problem_rate": pct(row["problem_count"], total),
            }
        )
    health_kpis = pd.DataFrame(health_rows)
    stage = health_kpis[health_kpis["flag_variable"].astype(str).str.contains("_qc")].copy()
    if not stage.empty:
        stage["qc_stage"] = stage["flag_variable"].map(lambda v: _stage_from_flag(v)[1])
    hotspots = health_kpis.sort_values("problem_rate", ascending=False).head(100).copy()
    hotspots.insert(0, "grouping_level", "product_variable")
    hotspots.insert(1, "source_dataset", "all_release_sources")
    problem_clusters = pd.DataFrame()
    yearly = pd.DataFrame()
    return {
        "flag_summary": summary,
        "flag_by_source": by_source,
        "flag_by_resolution": by_resolution,
        "flag_by_variable": by_variable,
        "flag_by_year": by_year,
        "flag_by_cluster": by_cluster,
        "flag_problem_clusters": problem_clusters,
        "health_kpis": health_kpis,
        "stage_effectiveness": stage,
        "issue_hotspots": hotspots,
        "yearly_trends": yearly,
    }


def write_figures(stats: dict, figures_dir: Path, dpi: int) -> None:
    """Write QC flag figures."""
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)
    flag_counts = stats.get("flag_counts", pd.DataFrame())
    health = stats.get("health", pd.DataFrame())

    # Stacked bar by product
    if not flag_counts.empty:
        products = sorted(flag_counts["product"].unique())
        fig, ax = plt.subplots(figsize=(8.0, 4.5))
        for i, product in enumerate(products):
            sub = flag_counts[flag_counts["product"] == product]
            totals = sub.groupby("flag_value")["count"].sum()
            total = totals.sum()
            bottom = 0
            for fv in [0, 1, 2, 3, 8, 9]:
                cnt = totals.get(fv, 0)
                pct = cnt / total * 100 if total else 0
                ax.bar(i, pct, bottom=bottom, color=FLAG_COLORS.get(fv, "#cccccc"),
                       label="{}: {}".format(fv, DEFAULT_FLAG_MEANINGS.get(fv, "other")) if i == 0 else "")
                bottom += pct
        ax.set_xticks(range(len(products)))
        ax.set_xticklabels(products)
        ax.set_ylabel("Percentage (%)")
        ax.set_title("QC Flag Distribution by Product")
        ax.set_ylim(0, 105)
        ax.legend(frameon=False, fontsize=8)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_qc_flag_distribution.png", dpi=dpi, also_pdf=False)
        plt.close(fig)

    # Health bar chart
    if not health.empty:
        fig, ax = plt.subplots(figsize=(8.0, 4.5))
        x = np.arange(len(health))
        width = 0.35
        ax.bar(x - width / 2, health["good_percent"], width, label="Good %", color="#2ca02c")
        problem_pct = health.apply(
            lambda r: round(100.0 * r["problem_count"] / r["total_flags"], 6) if r["total_flags"] else 0.0, axis=1
        )
        ax.bar(x + width / 2, problem_pct, width, label="Problem %", color="#d62728")
        ax.set_xticks(x)
        ax.set_xticklabels(health["flag_variable"] + "\n" + health["product"], rotation=45, ha="right")
        ax.set_ylabel("Percentage (%)")
        ax.set_title("QC Health by Product and Flag Variable")
        ax.legend(frameon=False)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_qc_health.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_health_by_resolution.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_flag_by_source_type.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_yearly_problem_trends.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_missing_trends.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_stage_summary.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_top_problem_sources.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_top_problem_clusters.png", dpi=dpi, also_pdf=False)
        plt.close(fig)

    for product in ("climatology", "satellite"):
        sub_dir = figures_dir / product
        sub_dir.mkdir(parents=True, exist_ok=True)
        sub_counts = flag_counts[flag_counts["product"].eq(product)] if not flag_counts.empty and "product" in flag_counts.columns else pd.DataFrame()
        sub_health = health[health["product"].eq(product)] if not health.empty and "product" in health.columns else pd.DataFrame()
        sub_stats = {"flag_counts": sub_counts, "health": sub_health}
        if sub_counts.empty and sub_health.empty:
            fig, ax = plt.subplots(figsize=(6, 3.5))
            ax.text(0.5, 0.5, "No {} QC records".format(product), ha="center", va="center", transform=ax.transAxes)
            ax.axis("off")
            for name in (
                "fig_qc_flag_distribution.png",
                "fig_qc_flag_by_source_type.png",
                "fig_qc_health_by_resolution.png",
                "fig_qc_missing_trends.png",
                "fig_qc_stage_summary.png",
                "fig_qc_top_problem_clusters.png",
                "fig_qc_top_problem_sources.png",
                "fig_qc_yearly_problem_trends.png",
            ):
                save_figure(fig, sub_dir / name, dpi=dpi, also_pdf=False)
            plt.close(fig)
        else:
            # Reuse the already generated aggregate visual style by saving a compact product label figure.
            fig, ax = plt.subplots(figsize=(6, 3.5))
            if not sub_counts.empty:
                sub_counts.groupby("flag_value")["count"].sum().plot(kind="bar", ax=ax, color="#4c78a8")
                ax.set_ylabel("Flags")
            ax.set_title("{} QC flags".format(product.capitalize()))
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            for name in (
                "fig_qc_flag_distribution.png",
                "fig_qc_flag_by_source_type.png",
                "fig_qc_health_by_resolution.png",
                "fig_qc_missing_trends.png",
                "fig_qc_stage_summary.png",
                "fig_qc_top_problem_clusters.png",
                "fig_qc_top_problem_sources.png",
                "fig_qc_yearly_problem_trends.png",
            ):
                save_figure(fig, sub_dir / name, dpi=dpi, also_pdf=False)
            plt.close(fig)


def _product_filter(frame: pd.DataFrame, product: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    group = _product_group(product)
    if "product_group" in frame.columns:
        return frame[frame["product_group"].astype(str).eq(group)].copy()
    if "release_component" in frame.columns:
        return frame[frame["release_component"].astype(str).eq(product)].copy()
    if "temporal_resolution" in frame.columns:
        return frame[frame["temporal_resolution"].astype(str).eq(product)].copy()
    if "product" in frame.columns:
        return frame[frame["product"].astype(str).eq(product)].copy()
    return frame.iloc[0:0].copy()


def build_detailed_qc_report(
    ctx,
    stats: dict,
    tables_dir: Path,
    figures_dir: Path,
    report_dir: Path,
    *,
    product: str = "",
) -> list[str]:
    title_product = product.capitalize() if product else "Release"
    flag_summary = stats.get("flag_summary", pd.DataFrame())
    flag_counts = stats.get("flag_counts", pd.DataFrame())
    schema = stats.get("flag_schema", pd.DataFrame())
    health = stats.get("health_kpis", pd.DataFrame())
    hotspots = stats.get("issue_hotspots", pd.DataFrame())
    stage = stats.get("stage_effectiveness", pd.DataFrame())
    yearly = stats.get("yearly_trends", pd.DataFrame())
    by_source = stats.get("flag_by_source", pd.DataFrame())
    by_resolution = stats.get("flag_by_resolution", pd.DataFrame())
    by_variable = stats.get("flag_by_variable", pd.DataFrame())
    problem_clusters = stats.get("flag_problem_clusters", pd.DataFrame())
    final_good_stage_missing = stats.get("final_good_stage_missing_by_source_resolution", pd.DataFrame())
    resolution_health = stats.get("resolution_health_kpis", pd.DataFrame())

    if product:
        flag_summary = _product_filter(flag_summary, product)
        flag_counts = _product_filter(flag_counts, product)
        schema = _product_filter(schema, product)
        health = _product_filter(health, product)
        hotspots = _product_filter(hotspots, product)
        stage = _product_filter(stage, product)
        yearly = _product_filter(yearly, product)
        by_source = _product_filter(by_source, product)
        by_resolution = _product_filter(by_resolution, product)
        by_variable = _product_filter(by_variable, product)
        problem_clusters = _product_filter(problem_clusters, product)
        final_good_stage_missing = _product_filter(final_good_stage_missing, product)
        resolution_health = _product_filter(resolution_health, product)

    total_flags = pd.to_numeric(flag_counts.get("count", 0), errors="coerce").fillna(0).sum() if not flag_counts.empty else 0
    final_rows = flag_summary[flag_summary.get("qc_level", pd.Series(dtype=str)).astype(str).eq("final")] if not flag_summary.empty and "qc_level" in flag_summary.columns else pd.DataFrame()
    stage_rows = flag_summary[flag_summary.get("qc_level", pd.Series(dtype=str)).astype(str).eq("stage")] if not flag_summary.empty and "qc_level" in flag_summary.columns else pd.DataFrame()
    problem_total = pd.to_numeric(health.get("problem_count", 0), errors="coerce").fillna(0).sum() if not health.empty else 0
    missing_total = pd.to_numeric(health.get("missing_count", 0), errors="coerce").fillna(0).sum() if not health.empty else 0
    usable_total = pd.to_numeric(health.get("usable_count", 0), errors="coerce").fillna(0).sum() if not health.empty else 0

    lines = [
        "# {} QC Flag Report".format(title_product),
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.",
        "",
        "## Headline",
        "",
        "- Flag observations summarized: {}".format(fmt_int(total_flags)),
        "- Final flag rows: {}".format(fmt_int(len(final_rows))),
        "- Stage flag rows: {}".format(fmt_int(len(stage_rows))),
        "- Usable flag count from health KPIs: {}".format(fmt_int(usable_total)),
        "- Problem flag count from health KPIs: {}".format(fmt_int(problem_total)),
        "- Missing flag count from health KPIs: {}".format(fmt_int(missing_total)),
        "- Stage-effectiveness rows available: {}".format(fmt_int(len(stage))),
        "",
        "## Flag Schema",
        "",
        sorted_markdown_table(
            schema,
            columns=["product", "flag_variable", "flag_value", "flag_meaning", "long_name"],
            max_rows=24,
        ),
    ]
    append_table_section(
        lines,
        "Final Flag Summary",
        final_rows,
        columns=["product_group", "release_component", "variable", "flag_variable", "flag", "meaning", "count", "percentage", "n_total"],
        sort_by="count",
        max_rows=24,
    )
    append_table_section(
        lines,
        "Stage Flag Summary",
        stage_rows,
        columns=["product_group", "release_component", "variable", "qc_stage", "flag_variable", "flag", "meaning", "count", "percentage", "n_total"],
        sort_by="count",
        max_rows=24,
    )
    append_table_section(
        lines,
        "Health KPIs",
        health,
        columns=[
            "product_group",
            "release_component",
            "variable",
            "flag_variable",
            "n_total",
            "good_count",
            "derived_count",
            "usable_count",
            "problem_count",
            "missing_count",
            "good_rate",
            "usable_rate",
            "problem_rate",
            "missing_rate",
        ],
        sort_by="problem_count",
        max_rows=24,
        note="Usable combines good and estimated/derived values (flags 0-1). Problem counts suspect/bad (flags 2-3) only; missing (flag 9) is reported separately.",
    )
    append_table_section(
        lines,
        "Issue Hotspots",
        hotspots,
        columns=[
            "grouping_level",
            "source_dataset",
            "product_group",
            "release_component",
            "variable",
            "flag_variable",
            "n_total",
            "usable_count",
            "problem_count",
            "missing_count",
            "usable_rate",
            "problem_rate",
            "missing_rate",
        ],
        sort_by="problem_count",
        max_rows=20,
    )
    append_table_section(
        lines,
        "Stage Effectiveness",
        stage,
        columns=[
            "product_group",
            "release_component",
            "variable",
            "qc_stage",
            "flag_variable",
            "n_total",
            "good_count",
            "bad_count",
            "not_checked_count",
            "missing_count",
            "good_rate",
            "problem_rate",
            "missing_rate",
        ],
        sort_by="problem_count",
        max_rows=20,
    )
    append_table_section(
        lines,
        "Final Flag Health by True Temporal Resolution",
        resolution_health,
        columns=[
            "product_group",
            "temporal_resolution",
            "variable",
            "flag_variable",
            "n_total",
            "good_count",
            "derived_count",
            "analysis_ready_count",
            "problem_count",
            "missing_count",
            "good_rate",
            "derived_rate",
            "analysis_ready_rate",
            "problem_rate",
            "missing_rate",
        ],
        max_rows=18,
        note="Rows are main-product final flags split by record-level temporal resolution; `analysis_ready` combines flags 0 and 1.",
    )
    append_table_section(
        lines,
        "Final Good With Missing Stage QC",
        final_good_stage_missing,
        columns=[
            "product_group",
            "release_component",
            "source_dataset",
            "temporal_resolution",
            "variable",
            "qc_stage",
            "stage_flag_variable",
            "final_good_count",
            "stage_missing_count",
            "stage_missing_rate",
            "stage_not_checked_count",
            "stage_not_checked_rate",
            "stage_pass_count",
            "stage_pass_rate",
        ],
        sort_by="stage_missing_count",
        max_rows=24,
        note="Rows are restricted to final good records (`*_flag == 0`) in the master release product; rates use `final_good_count` as denominator.",
    )
    append_table_section(
        lines,
        "Flag Counts by Source",
        by_source,
        columns=[
            "source_dataset",
            "source_type",
            "qc_level",
            "qc_stage",
            "product_group",
            "release_component",
            "variable",
            "flag_variable",
            "flag",
            "meaning",
            "count",
            "percentage",
            "n_total",
        ],
        max_rows=16,
    )
    append_table_section(
        lines,
        "Flag Counts by True Temporal Resolution",
        by_resolution,
        columns=[
            "product_group",
            "temporal_resolution",
            "qc_level",
            "qc_stage",
            "variable",
            "flag_variable",
            "flag",
            "meaning",
            "count",
            "percentage",
            "n_total",
        ],
        max_rows=16,
        note="This table uses the main/master record-level `resolution` variable (`daily`, `monthly`, `annual`), not the release component name.",
    )
    append_table_section(
        lines,
        "Flag Counts by Variable",
        by_variable,
        max_rows=16,
    )
    append_table_section(
        lines,
        "Problem Clusters",
        problem_clusters,
        columns=[
            "cluster_uid",
            "cluster_id",
            "grouping_level",
            "product_group",
            "release_component",
            "temporal_resolution",
            "variable",
            "flag_variable",
            "n_records",
            "analysis_ready_count",
            "suspect_bad_count",
            "missing_count",
            "analysis_ready_rate",
            "suspect_bad_rate",
            "missing_rate",
        ],
        sort_by="suspect_bad_count",
        max_rows=16,
        note="Rows are true cluster-level final-flag summaries for release products that carry SED cluster identifiers; `suspect_bad` combines flags 2 and 3.",
    )
    append_table_section(
        lines,
        "Yearly Trends",
        yearly,
        columns=[
            "product_group",
            "release_component",
            "year",
            "temporal_resolution",
            "variable",
            "flag_variable",
            "n_records",
            "analysis_ready_count",
            "suspect_bad_count",
            "missing_count",
            "analysis_ready_rate",
            "suspect_bad_rate",
            "missing_rate",
        ],
        sort_by="year",
        ascending=False,
        max_rows=18,
        note="Rows use final flag variables only (`*_flag`), grouped by `year x temporal_resolution x variable`; stage QC flags are excluded from the denominator.",
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.",
            "- Flag values partition into analysis-ready (0-1: good + derived/estimated), suspect/bad (2-3), and missing (9); `problem_count` covers suspect/bad only.",
            "- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.",
            "- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.",
        ]
    )
    product_figures = figures_dir / product if product and (figures_dir / product).is_dir() else figures_dir
    append_figure_index(lines, product_figures, report_dir)
    return safe_lines(lines)


def build_article_qc_flag_report(ctx, stats: dict, tables_dir: Path, figures_dir: Path, report_dir: Path) -> list[str]:
    return build_detailed_qc_report(ctx, stats, tables_dir, figures_dir, report_dir)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only QC flag statistics.")
    add_common_args(parser, "qc_flags")
    parser.add_argument("--chunk-size", type=int, default=500000)
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    stats = build_qc_stats(ctx, max(1, int(args.chunk_size)))
    for name, frame in stats.items():
        write_csv(frame, tables_dir / "table_qc_{}.csv".format(name))
    legacy_names = (
        "flag_summary",
        "flag_by_source",
        "flag_by_resolution",
        "flag_by_variable",
        "flag_by_year",
        "flag_by_cluster",
        "flag_problem_clusters",
        "health_kpis",
        "issue_hotspots",
        "stage_effectiveness",
        "yearly_trends",
    )
    for legacy_name in legacy_names:
        write_csv(stats[legacy_name], tables_dir / "table_qc_{}.csv".format(legacy_name))
    for product in ("climatology", "satellite"):
        product_dir = tables_dir / product
        product_dir.mkdir(parents=True, exist_ok=True)
        for legacy_name in legacy_names:
            frame = stats[legacy_name]
            sub = _product_filter(frame, product)
            write_csv(sub, product_dir / "table_qc_{}.csv".format(legacy_name))
    if not args.skip_figures:
        try:
            write_figures(stats, ctx.figures_dir(), max(72, int(args.dpi)))
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    md_path = ctx.output_path("reports", "qc_flag_stats.md")
    report_lines = build_detailed_qc_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    article_path = ctx.output_path("article_qc_flag_report.md")
    write_markdown(build_article_qc_flag_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir), article_path)
    for product in ("climatology", "satellite"):
        product_report_dir = ctx.output_path("reports", product, "x").parent
        write_markdown(
            build_detailed_qc_report(ctx, stats, tables_dir / product, ctx.figures_dir(), product_report_dir, product=product),
            ctx.output_path("reports", product, "article_qc_flag_report.md"),
        )
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
        copy_report_to_docs(article_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote QC flag stats to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
