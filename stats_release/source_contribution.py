#!/usr/bin/env python3
"""Source contribution statistics from release catalogs."""
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

from stats_release.release_io import (
    add_common_args,
    clean_text,
    context_from_args,
    copy_report_to_docs,
    netcdf_record_count,
    numeric_series,
    read_numeric_var,
    read_text_var,
    setup_matplotlib,
    split_pipe,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import (
    VARIABLES,
    attach_source_classification,
    decode_time_values,
    pct,
    resolution_values,
    save_figure,
    unique_pipe,
)
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    metric_value,
    safe_lines,
    sorted_markdown_table,
)


def _explode_station_sources(station: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in station.iterrows():
        for source in split_pipe(row.get("sources_used", "")):
            rows.append(
                {
                    "source_name": source,
                    "cluster_uid": row.get("cluster_uid", ""),
                    "resolution": row.get("resolution", ""),
                    "record_count": row.get("record_count", 0),
                }
            )
    return pd.DataFrame(rows)


def _scan_record_product(ctx, file_name: str, product: str, chunk_size: int) -> pd.DataFrame:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame()
    counts = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        if n_records <= 0:
            return pd.DataFrame()
        if "source" in ds.variables:
            all_sources = np.asarray(read_text_var(ds, "source"), dtype=object)
        else:
            all_sources = np.asarray(["unknown"] * n_records, dtype=object)
        times = None
        if "time" in ds.variables:
            raw_time = np.ma.asarray(ds.variables["time"][:]).astype("float64")
            if np.ma.isMaskedArray(raw_time):
                raw_time = raw_time.filled(np.nan)
            times = np.asarray(raw_time).reshape(-1)
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            sources = all_sources[slc] if len(all_sources) == n_records else all_sources[: stop - start]
            resolutions = np.asarray(resolution_values(ds, slc), dtype=object)
            if "station_index" in ds.variables:
                station_idx = np.ma.asarray(ds.variables["station_index"][slc]).filled(-1).astype(int).reshape(-1)
            else:
                station_idx = np.arange(start, stop)
            years = np.full(stop - start, np.nan)
            if times is not None and len(times) >= stop:
                idx = decode_time_values(ds, times[slc])
                if len(idx):
                    years = idx.year.to_numpy(dtype=float)
            present = {var: np.isfinite(read_numeric_var(ds, var, key=slc).reshape(-1)) for var in VARIABLES}
            any_present = present["Q"] | present["SSC"] | present["SSL"]
            for source in sorted(set(clean_text(v) or "unknown" for v in sources)):
                smask = np.asarray([clean_text(v) or "unknown" for v in sources], dtype=object) == source
                if not np.any(smask):
                    continue
                for resolution in sorted(set(str(v) for v in resolutions[smask])):
                    mask = smask & (resolutions.astype(str) == resolution) & any_present
                    if not np.any(mask):
                        continue
                    item = counts.setdefault(
                        (source, resolution),
                        {
                            "source_name": source,
                            "product": product,
                            "resolution": resolution,
                            "n_records": 0,
                            "n_Q_records": 0,
                            "n_SSC_records": 0,
                            "n_SSL_records": 0,
                            "stations": set(),
                            "clusters": set(),
                            "first_year": np.nan,
                            "last_year": np.nan,
                        },
                    )
                    item["n_records"] += int(np.count_nonzero(mask))
                    for var in VARIABLES:
                        item["n_{}_records".format(var)] += int(np.count_nonzero(mask & present[var]))
                    item["stations"].update(int(v) for v in station_idx[mask] if int(v) >= 0)
                    item["clusters"].update(int(v) for v in station_idx[mask] if int(v) >= 0)
                    yr = years[mask]
                    yr = yr[np.isfinite(yr)]
                    if yr.size:
                        first = int(np.min(yr))
                        last = int(np.max(yr))
                        item["first_year"] = first if pd.isna(item["first_year"]) else min(int(item["first_year"]), first)
                        item["last_year"] = last if pd.isna(item["last_year"]) else max(int(item["last_year"]), last)
    rows = []
    for item in counts.values():
        row = dict(item)
        row["n_source_stations"] = len(row.pop("stations"))
        row["n_clusters"] = len(row.pop("clusters"))
        rows.append(row)
    return pd.DataFrame(rows)


def _scan_satellite_product(ctx, chunk_size: int) -> pd.DataFrame:
    file_name = PRODUCT_FILES["satellite_nc"]
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame()
    counts = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        if n_records <= 0 or "satellite_station_index" not in ds.variables:
            return pd.DataFrame()
        station_sources = np.asarray(read_text_var(ds, "source"), dtype=object)
        station_clusters = np.asarray(read_text_var(ds, "cluster_uid"), dtype=object)
        all_resolutions = (
            np.asarray(read_text_var(ds, "resolution"), dtype=object)
            if "resolution" in ds.variables
            else np.asarray(["satellite"] * n_records, dtype=object)
        )
        raw_time = np.ma.asarray(ds.variables["time"][:]).astype("float64") if "time" in ds.variables else np.asarray([])
        if np.ma.isMaskedArray(raw_time):
            raw_time = raw_time.filled(np.nan)
        raw_time = np.asarray(raw_time).reshape(-1)
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            station_idx = np.ma.asarray(ds.variables["satellite_station_index"][slc]).filled(-1).astype(int).reshape(-1)
            sources = np.asarray(["unknown"] * len(station_idx), dtype=object)
            clusters = np.asarray([""] * len(station_idx), dtype=object)
            valid = (station_idx >= 0) & (station_idx < len(station_sources))
            sources[valid] = station_sources[station_idx[valid]]
            clusters[valid] = station_clusters[station_idx[valid]]
            resolutions = all_resolutions[slc]
            years = np.full(stop - start, np.nan)
            if raw_time.size >= stop:
                idx = decode_time_values(ds, raw_time[slc])
                if len(idx):
                    years = idx.year.to_numpy(dtype=float)
            present = {var: np.isfinite(read_numeric_var(ds, var, key=slc).reshape(-1)) for var in VARIABLES}
            any_present = present["Q"] | present["SSC"] | present["SSL"]
            source_clean = np.asarray([clean_text(v) or "unknown" for v in sources], dtype=object)
            for source in sorted(set(source_clean)):
                smask = source_clean == source
                for resolution in sorted(set(str(v) for v in resolutions[smask])):
                    mask = smask & (resolutions.astype(str) == resolution) & any_present
                    if not np.any(mask):
                        continue
                    item = counts.setdefault(
                        (source, resolution),
                        {
                            "source_name": source,
                            "product": "satellite",
                            "resolution": resolution,
                            "n_records": 0,
                            "n_Q_records": 0,
                            "n_SSC_records": 0,
                            "n_SSL_records": 0,
                            "stations": set(),
                            "clusters": set(),
                            "first_year": np.nan,
                            "last_year": np.nan,
                        },
                    )
                    item["n_records"] += int(np.count_nonzero(mask))
                    for var in VARIABLES:
                        item["n_{}_records".format(var)] += int(np.count_nonzero(mask & present[var]))
                    item["stations"].update(int(v) for v in station_idx[mask] if int(v) >= 0)
                    item["clusters"].update(clean_text(v) for v in clusters[mask] if clean_text(v))
                    yr = years[mask]
                    yr = yr[np.isfinite(yr)]
                    if yr.size:
                        first = int(np.min(yr))
                        last = int(np.max(yr))
                        item["first_year"] = first if pd.isna(item["first_year"]) else min(int(item["first_year"]), first)
                        item["last_year"] = last if pd.isna(item["last_year"]) else max(int(item["last_year"]), last)
    rows = []
    for item in counts.values():
        row = dict(item)
        row["n_source_stations"] = len(row.pop("stations"))
        row["n_clusters"] = len(row.pop("clusters"))
        rows.append(row)
    return pd.DataFrame(rows)


def _satellite_variable_counts_by_source(ctx, chunk_size: int) -> dict:
    file_name = PRODUCT_FILES["satellite_nc"]
    if not ctx.release_file(file_name).is_file():
        return {}
    counts = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        if n_records <= 0 or "satellite_station_index" not in ds.variables or "source" not in ds.variables:
            return counts
        station_sources = np.asarray(read_text_var(ds, "source"), dtype=object)
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            station_idx = np.ma.asarray(ds.variables["satellite_station_index"][slc]).filled(-1).astype(int).reshape(-1)
            sources = np.asarray(["unknown"] * len(station_idx), dtype=object)
            valid = (station_idx >= 0) & (station_idx < len(station_sources))
            sources[valid] = station_sources[station_idx[valid]]
            source_clean = np.asarray([clean_text(v) or "unknown" for v in sources], dtype=object)
            present = {var: np.isfinite(read_numeric_var(ds, var, key=slc).reshape(-1)) for var in VARIABLES}
            for source in sorted(set(source_clean)):
                smask = source_clean == source
                item = counts.setdefault(source, {var: 0 for var in VARIABLES})
                for var in VARIABLES:
                    item[var] += int(np.count_nonzero(smask & present[var]))
    return counts


def _satellite_catalog_detail(ctx, chunk_size: int, scan_variables: bool = False) -> pd.DataFrame:
    catalog = ctx.read_csv(PRODUCT_FILES["satellite_catalog"], required=False)
    if catalog.empty:
        return pd.DataFrame()
    work = catalog.copy()
    work["n_records"] = numeric_series(work, "n_records").fillna(0)
    work["first_year"] = pd.to_datetime(work.get("time_start", ""), errors="coerce").dt.year
    work["last_year"] = pd.to_datetime(work.get("time_end", ""), errors="coerce").dt.year
    variable_counts = _satellite_variable_counts_by_source(ctx, chunk_size) if scan_variables else {}
    rows = []
    for (source, resolution), group in work.groupby(["source", "resolution"], dropna=False):
        source = clean_text(source) or "unknown"
        vc = variable_counts.get(source, {})
        n_records = int(group["n_records"].sum())
        if not vc:
            lower = source.lower()
            if lower in {"dethier", "shashi_jianli"}:
                vc = {"Q": n_records, "SSC": n_records, "SSL": n_records}
            elif lower == "gsed":
                vc = {"Q": 0, "SSC": n_records, "SSL": 0}
            elif lower == "riversed":
                vc = {"Q": 0, "SSC": 0, "SSL": 0}
            else:
                vc = {"Q": 0, "SSC": n_records, "SSL": 0}
        rows.append(
            {
                "source_name": source,
                "product": "satellite",
                "resolution": clean_text(resolution) or "satellite",
                "n_records": n_records,
                "n_Q_records": int(vc.get("Q", 0)),
                "n_SSC_records": int(vc.get("SSC", 0)),
                "n_SSL_records": int(vc.get("SSL", 0)),
                "n_source_stations": int(group["satellite_station_uid"].nunique()) if "satellite_station_uid" in group.columns else int(len(group)),
                "n_clusters": int(group["cluster_uid"].nunique()) if "cluster_uid" in group.columns else 0,
                "first_year": int(group["first_year"].min()) if group["first_year"].notna().any() else np.nan,
                "last_year": int(group["last_year"].max()) if group["last_year"].notna().any() else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _source_station_catalog_detail(ctx) -> pd.DataFrame:
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    if source_station.empty:
        return pd.DataFrame()
    work = source_station.copy()
    work["n_records"] = numeric_series(work, "n_records").fillna(0)
    work["first_year"] = pd.to_datetime(work.get("time_start", ""), errors="coerce").dt.year
    work["last_year"] = pd.to_datetime(work.get("time_end", ""), errors="coerce").dt.year
    vars_text = work.get("source_station_variables_provided", pd.Series([""] * len(work), index=work.index)).astype(str)
    for var in VARIABLES:
        work["n_{}_records".format(var)] = np.where(vars_text.str.contains(var, case=False, regex=False), work["n_records"], 0)
    rows = []
    for (source, resolution), group in work.groupby(["source_name", "resolution"], dropna=False):
        resolution = clean_text(resolution) or "unknown"
        rows.append(
            {
                "source_name": clean_text(source) or "unknown",
                "product": "climatology" if resolution == "climatology" else "main",
                "resolution": resolution,
                "n_records": int(group["n_records"].sum()),
                "n_Q_records": int(group["n_Q_records"].sum()),
                "n_SSC_records": int(group["n_SSC_records"].sum()),
                "n_SSL_records": int(group["n_SSL_records"].sum()),
                "n_source_stations": int(group["source_station_uid"].nunique()) if "source_station_uid" in group.columns else int(len(group)),
                "n_clusters": int(group["cluster_uid"].nunique()) if "cluster_uid" in group.columns else 0,
                "first_year": int(group["first_year"].min()) if group["first_year"].notna().any() else np.nan,
                "last_year": int(group["last_year"].max()) if group["last_year"].notna().any() else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _build_contribution_from_detail(
    detail: pd.DataFrame,
    source_dataset: pd.DataFrame = None,
) -> dict:
    """Build the 9-table contribution dict from an arbitrary detail DataFrame.

    This is the core computation extracted from _build_legacy_source_tables,
    reusable for both the combined legacy track and the per-track (main vs
    satellite) dual-track contributions.  *detail* must contain columns
    ``source_name``, ``n_records``, ``n_source_stations``, ``n_clusters``,
    ``n_Q_records``, ``n_SSC_records``, ``n_SSL_records``, ``resolution``,
    ``first_year``, ``last_year``.
    """
    if detail.empty:
        empty = pd.DataFrame()
        return {
            "source_dataset_contribution": empty,
            "source_type_contribution": empty,
            "source_resolution_contribution": empty,
            "source_variable_contribution": empty,
            "top_source_contributors": empty,
            "source_contribution_cumulative": empty,
            "source_temporal_coverage": empty,
            "report_key_metrics": empty,
            "source_classification_template": empty,
        }
    summary = (
        detail.groupby("source_name", dropna=False)
        .agg(
            n_source_stations=("n_source_stations", "sum"),
            n_clusters=("n_clusters", "sum"),
            n_records=("n_records", "sum"),
            n_Q_records=("n_Q_records", "sum"),
            n_SSC_records=("n_SSC_records", "sum"),
            n_SSL_records=("n_SSL_records", "sum"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            resolutions=("resolution", unique_pipe),
        )
        .reset_index()
    )
    summary = attach_source_classification(summary, "source_name")
    if source_dataset is not None and not source_dataset.empty and "source_name" in source_dataset.columns:
        meta_cols = [c for c in ("source_name", "source_long_name", "institution", "reference", "source_url") if c in source_dataset.columns]
        summary = summary.merge(source_dataset[meta_cols].drop_duplicates("source_name"), on="source_name", how="left")
    total_records = float(summary["n_records"].sum()) or 1.0
    summary["percentage_of_total_records"] = summary["n_records"].map(lambda v: pct(v, total_records))
    summary = summary.sort_values("n_records", ascending=False)

    resolution = attach_source_classification(detail.copy(), "source_name")
    resolution["percentage_of_total_records"] = resolution["n_records"].map(lambda v: pct(v, total_records))
    source_totals = summary.set_index("source_name")["n_records"].to_dict()
    resolution["percentage_within_source_records"] = resolution.apply(lambda r: pct(r["n_records"], source_totals.get(r["source_name"], 0)), axis=1)
    resolution = resolution.sort_values("n_records", ascending=False)

    variable_rows = []
    total_by_var = {var: float(summary["n_{}_records".format(var)].sum()) or 1.0 for var in VARIABLES}
    for _, row in summary.iterrows():
        for var in VARIABLES:
            count = int(row["n_{}_records".format(var)])
            variable_rows.append(
                {
                    "source_name": row["source_name"],
                    "source_type": row["source_type"],
                    "source_group": row["source_group"],
                    "variable": var,
                    "n_variable_records": count,
                    "n_source_records": int(row["n_records"]),
                    "percentage_of_total_variable_records": pct(count, total_by_var[var]),
                    "percentage_within_source_records": pct(count, row["n_records"]),
                }
            )
    variable = pd.DataFrame(variable_rows)
    type_rows = []
    for level in ("source_group", "source_type"):
        grouped = (
            summary.groupby(level, dropna=False)
            .agg(
                n_source_datasets=("source_name", "nunique"),
                n_source_stations=("n_source_stations", "sum"),
                n_clusters=("n_clusters", "sum"),
                n_records=("n_records", "sum"),
                n_Q_records=("n_Q_records", "sum"),
                n_SSC_records=("n_SSC_records", "sum"),
                n_SSL_records=("n_SSL_records", "sum"),
                first_year=("first_year", "min"),
                last_year=("last_year", "max"),
                resolutions=("resolutions", unique_pipe),
            )
            .reset_index()
            .rename(columns={level: "category"})
        )
        grouped.insert(0, "summary_level", level)
        type_rows.append(grouped)
    source_type = pd.concat(type_rows, ignore_index=True)
    source_type["percentage_of_total_records"] = source_type["n_records"].map(lambda v: pct(v, total_records))

    top_rows = []
    for metric, col in (("records", "n_records"), ("source_stations", "n_source_stations"), ("clusters", "n_clusters")):
        total = float(summary[col].sum()) or 1.0
        for rank, (_, row) in enumerate(summary.sort_values(col, ascending=False).head(20).iterrows(), start=1):
            top_rows.append(
                {
                    "rank_metric": metric,
                    "rank": rank,
                    "source_name": row["source_name"],
                    "source_type": row["source_type"],
                    "source_group": row["source_group"],
                    "value": float(row[col]),
                    "percentage_of_metric_total": pct(row[col], total),
                }
            )
    top = pd.DataFrame(top_rows)
    cumulative = summary[["source_name", "source_type", "source_group", "n_records"]].copy()
    cumulative = cumulative.sort_values("n_records", ascending=False)
    cumulative["rank"] = np.arange(1, len(cumulative) + 1)
    cumulative["cumulative_records"] = cumulative["n_records"].cumsum()
    cumulative["cumulative_percent"] = cumulative["cumulative_records"].map(lambda v: pct(v, total_records))
    temporal = summary[["source_name", "source_type", "source_group", "first_year", "last_year", "n_records", "n_source_stations", "n_clusters", "resolutions"]].copy()
    temporal["year_span"] = temporal["last_year"] - temporal["first_year"] + 1
    metrics = pd.DataFrame(
        [
            {"metric": "total_source_datasets", "value": int(summary["source_name"].nunique()), "detail": ""},
            {"metric": "total_source_stations", "value": int(summary["n_source_stations"].sum()), "detail": ""},
            {"metric": "total_clusters_source_sum", "value": int(summary["n_clusters"].sum()), "detail": ""},
            {"metric": "total_records", "value": int(summary["n_records"].sum()), "detail": ""},
            {"metric": "total_Q_records", "value": int(summary["n_Q_records"].sum()), "detail": ""},
            {"metric": "total_SSC_records", "value": int(summary["n_SSC_records"].sum()), "detail": ""},
            {"metric": "total_SSL_records", "value": int(summary["n_SSL_records"].sum()), "detail": ""},
            {"metric": "top_source_by_records", "value": summary.iloc[0]["source_name"] if len(summary) else "", "detail": "{:.2f}%".format(summary.iloc[0]["percentage_of_total_records"]) if len(summary) else ""},
            {"metric": "earliest_year", "value": int(summary["first_year"].min()), "detail": ""},
            {"metric": "latest_year", "value": int(summary["last_year"].max()), "detail": ""},
        ]
    )
    classification = summary[["source_name", "source_type", "source_group"]].copy()
    return {
        "source_dataset_contribution": summary,
        "source_type_contribution": source_type,
        "source_resolution_contribution": resolution,
        "source_variable_contribution": variable,
        "top_source_contributors": top,
        "source_contribution_cumulative": cumulative,
        "source_temporal_coverage": temporal,
        "report_key_metrics": metrics,
        "source_classification_template": classification,
    }


def _build_legacy_source_tables(ctx, source_dataset: pd.DataFrame, chunk_size: int) -> dict:
    frames = [
        _source_station_catalog_detail(ctx),
        _satellite_catalog_detail(ctx, chunk_size, scan_variables=False),
    ]
    detail = pd.concat([f for f in frames if not f.empty], ignore_index=True) if any(not f.empty for f in frames) else pd.DataFrame()
    return _build_contribution_from_detail(detail, source_dataset)


def build_source_contribution(ctx, chunk_size: int = 500000) -> dict:
    source_dataset = ctx.read_csv(PRODUCT_FILES["source_dataset_catalog"], required=False)
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"], required=False)
    satellite = ctx.read_csv(PRODUCT_FILES["satellite_catalog"], required=False)

    source_rows = _explode_station_sources(station) if not station.empty else pd.DataFrame()
    if not source_rows.empty:
        source_rows["record_count"] = pd.to_numeric(source_rows["record_count"], errors="coerce").fillna(0)
    main_by_source = (
        source_rows.groupby("source_name", dropna=False)
        .agg(
            main_cluster_count=("cluster_uid", "nunique"),
            main_resolution_rows=("resolution", "size"),
            main_record_count=("record_count", "sum"),
        )
        .reset_index()
        if not source_rows.empty
        else pd.DataFrame(columns=["source_name", "main_cluster_count", "main_resolution_rows", "main_record_count"])
    )
    source_station_by_source = (
        source_station.assign(n_records=numeric_series(source_station, "n_records").fillna(0))
        .groupby(["source_name", "resolution"], dropna=False)
        .agg(
            source_station_rows=("source_station_uid", "size"),
            source_station_count=("source_station_uid", "nunique"),
            linked_cluster_count=("cluster_uid", "nunique"),
            source_station_record_count=("n_records", "sum"),
        )
        .reset_index()
        if not source_station.empty
        else pd.DataFrame()
    )
    source_station_record_by_source = (
        source_station.assign(n_records=numeric_series(source_station, "n_records").fillna(0))
        .groupby("source_name", dropna=False)
        .agg(
            record_attributed_station_rows=("source_station_uid", "size"),
            record_attributed_station_count=("source_station_uid", "nunique"),
            record_attributed_cluster_count=("cluster_uid", "nunique"),
            record_attributed_record_count=("n_records", "sum"),
        )
        .reset_index()
        if not source_station.empty
        else pd.DataFrame(
            columns=[
                "source_name",
                "record_attributed_station_rows",
                "record_attributed_station_count",
                "record_attributed_cluster_count",
                "record_attributed_record_count",
            ]
        )
    )
    satellite_by_source = (
        satellite.assign(n_records=numeric_series(satellite, "n_records").fillna(0))
        .groupby(["source", "resolution"], dropna=False)
        .agg(
            satellite_station_count=("satellite_station_uid", "nunique"),
            satellite_cluster_count=("cluster_uid", "nunique"),
            satellite_record_count=("n_records", "sum"),
        )
        .reset_index()
        .rename(columns={"source": "source_name"})
        if not satellite.empty
        else pd.DataFrame()
    )
    source_summary = source_dataset.copy() if not source_dataset.empty else pd.DataFrame(columns=["source_name"])
    if "source_name" not in source_summary.columns:
        source_summary["source_name"] = ""
    source_summary = source_summary.merge(main_by_source, on="source_name", how="outer")
    source_summary = source_summary.merge(source_station_record_by_source, on="source_name", how="outer")
    for col in (
        "main_cluster_count",
        "main_resolution_rows",
        "main_record_count",
        "record_attributed_station_rows",
        "record_attributed_station_count",
        "record_attributed_cluster_count",
        "record_attributed_record_count",
    ):
        if col in source_summary.columns:
            source_summary[col] = pd.to_numeric(source_summary[col], errors="coerce").fillna(0).astype(int)
    source_summary["cluster_attributed_record_count"] = source_summary.get("main_record_count", 0)
    source_summary["over_attribution_record_count"] = (
        source_summary["cluster_attributed_record_count"] - source_summary["record_attributed_record_count"]
        if "record_attributed_record_count" in source_summary.columns
        else 0
    )
    legacy = _build_legacy_source_tables(ctx, source_dataset, max(1, int(chunk_size)))
    # Dual-track: separate main (in-situ / reference / climatology) from satellite
    main_detail = _source_station_catalog_detail(ctx)
    sat_detail = _satellite_catalog_detail(ctx, max(1, int(chunk_size)), scan_variables=False)
    # Re-classify by source type: any source with satellite classification goes to satellite track
    if not main_detail.empty and "source_name" in main_detail.columns:
        classified = attach_source_classification(main_detail[["source_name"]].drop_duplicates(), "source_name")
        sat_sources = set(classified.loc[classified["source_type"] == "satellite", "source_name"].astype(str).str.lower())
        main_mask = ~main_detail["source_name"].astype(str).str.lower().isin(sat_sources)
        moved = main_detail[~main_mask].copy()
        main_detail = main_detail[main_mask].copy()
        if not moved.empty:
            sat_detail = pd.concat([sat_detail, moved], ignore_index=True)
    dual_main = _build_contribution_from_detail(main_detail, source_dataset)
    dual_sat = _build_contribution_from_detail(sat_detail, source_dataset)
    return {
        "source_summary": source_summary.sort_values("source_name"),
        "source_resolution": source_station_by_source,
        "satellite_source_resolution": satellite_by_source,
        **legacy,
        "main_source_contribution": dual_main,
        "satellite_source_contribution": dual_sat,
        "main_detail": main_detail,
        "sat_detail": sat_detail,
    }


def write_figures(stats: dict, figures_dir: Path, dpi: int, top_n: int = 20) -> None:
    """Write source contribution figures."""
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)
    # Prefer main-track source contribution over legacy merged for primary figures
    main_tables = stats.get("main_source_contribution", {})
    source_summary = main_tables.get("source_dataset_contribution", stats.get("source_dataset_contribution", pd.DataFrame()))
    if source_summary.empty:
        source_summary = stats.get("source_summary", pd.DataFrame()).rename(columns={"main_record_count": "n_records", "main_cluster_count": "n_clusters", "record_attributed_station_count": "n_source_stations"})
    if source_summary.empty or "n_records" not in source_summary.columns:
        return

    # Horizontal bar: records
    plot_df = source_summary.sort_values("n_records", ascending=False).head(top_n).sort_values("n_records", ascending=True)
    height = max(4.0, 0.35 * len(plot_df) + 1.5)
    fig, ax = plt.subplots(figsize=(10, height))
    ax.barh(plot_df["source_name"].astype(str), plot_df["n_records"].astype(float))
    ax.set_xlabel("Records")
    ax.set_ylabel("Source dataset")
    ax.set_title("Source contribution by records")
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    save_figure(fig, figures_dir / "fig_source_contribution_records.png", dpi=dpi, also_pdf=False)
    plt.close(fig)

    # Horizontal bar: clusters
    if "n_clusters" in source_summary.columns:
        plot_df2 = source_summary.sort_values("n_clusters", ascending=False).head(top_n).sort_values("n_clusters", ascending=True)
        height2 = max(4.0, 0.35 * len(plot_df2) + 1.5)
        fig2, ax2 = plt.subplots(figsize=(10, height2))
        ax2.barh(plot_df2["source_name"].astype(str), plot_df2["n_clusters"].astype(float), color="#54a24b")
        ax2.set_xlabel("Clusters")
        ax2.set_ylabel("Source dataset")
        ax2.set_title("Source contribution by clusters")
        ax2.grid(axis="x", alpha=0.3)
        fig2.tight_layout()
        save_figure(fig2, figures_dir / "fig_source_contribution_clusters.png", dpi=dpi, also_pdf=False)
        plt.close(fig2)
    if "n_source_stations" in source_summary.columns:
        plot_df2 = source_summary.sort_values("n_source_stations", ascending=False).head(top_n).sort_values("n_source_stations", ascending=True)
        fig2, ax2 = plt.subplots(figsize=(10, max(4.0, 0.35 * len(plot_df2) + 1.5)))
        ax2.barh(plot_df2["source_name"].astype(str), plot_df2["n_source_stations"].astype(float), color="#72b7b2")
        ax2.set_xlabel("Source stations")
        ax2.set_title("Source contribution by stations")
        ax2.grid(axis="x", alpha=0.3)
        fig2.tight_layout()
        save_figure(fig2, figures_dir / "fig_source_contribution_stations.png", dpi=dpi, also_pdf=False)
        plt.close(fig2)

    # Cumulative contribution
    total = int(source_summary["n_records"].sum())
    if total > 0:
        cum = source_summary.sort_values("n_records", ascending=False).copy()
        cum["cumulative_pct"] = cum["n_records"].cumsum() / total * 100.0
        cum["rank"] = np.arange(1, len(cum) + 1)
        fig3, ax3 = plt.subplots(figsize=(8, 5))
        ax3.plot(cum["rank"], cum["cumulative_pct"], marker="o", linewidth=1.5)
        ax3.set_xlabel("Source dataset rank")
        ax3.set_ylabel("Cumulative record contribution (%)")
        ax3.set_title("Cumulative source contribution by records")
        ax3.set_ylim(0, 105)
        ax3.grid(alpha=0.3)
        fig3.tight_layout()
        save_figure(fig3, figures_dir / "fig_source_cumulative_contribution.png", dpi=dpi, also_pdf=False)
        plt.close(fig3)

    type_df = stats.get("source_type_contribution", pd.DataFrame())
    if not type_df.empty:
        for level, fname in (("source_type", "fig_source_type_records.png"), ("source_group", "fig_source_group_records.png")):
            plot = type_df[type_df["summary_level"].eq(level)].sort_values("n_records", ascending=True)
            if plot.empty:
                continue
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.barh(plot["category"].astype(str), plot["n_records"], color="#f58518")
            ax.set_xlabel("Records")
            ax.set_title(level.replace("_", " ").title())
            ax.grid(axis="x", alpha=0.3)
            fig.tight_layout()
            save_figure(fig, figures_dir / fname, dpi=dpi, also_pdf=False)
            plt.close(fig)

    res_df = stats.get("source_resolution_contribution", pd.DataFrame())
    if not res_df.empty:
        pivot = res_df.pivot_table(index="source_name", columns="resolution", values="n_records", aggfunc="sum", fill_value=0)
        pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).head(top_n).index].sort_values(pivot.columns[0] if len(pivot.columns) else pivot.index.name)
        fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(pivot) + 1.5)))
        pivot.plot(kind="barh", stacked=True, ax=ax)
        ax.set_xlabel("Records")
        ax.set_title("Source contribution by resolution")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_source_resolution_stacked.png", dpi=dpi, also_pdf=False)
        plt.close(fig)
        for product, prefix in (("satellite", "satellite"), ("climatology", "climatology")):
            sub = res_df[res_df["product"].eq(product)] if "product" in res_df.columns else pd.DataFrame()
            if not sub.empty:
                plot = sub.sort_values("n_records", ascending=False).head(top_n).sort_values("n_records")
                fig, ax = plt.subplots(figsize=(9, max(4, 0.35 * len(plot) + 1.5)))
                ax.barh(plot["source_name"].astype(str), plot["n_records"], color="#4c78a8")
                ax.set_xlabel("Records")
                ax.set_title("{} source records".format(prefix.capitalize()))
                ax.grid(axis="x", alpha=0.3)
                fig.tight_layout()
                save_figure(fig, figures_dir / "fig_{}_contribution_records.png".format(prefix), dpi=dpi, also_pdf=False)
                save_figure(fig, figures_dir / "fig_{}_contribution_clusters.png".format(prefix), dpi=dpi, also_pdf=False)
                save_figure(fig, figures_dir / "fig_{}_contribution_stations.png".format(prefix), dpi=dpi, also_pdf=False)
                save_figure(fig, figures_dir / "fig_{}_resolution_stacked.png".format(prefix), dpi=dpi, also_pdf=False)
                plt.close(fig)

    var_df = stats.get("source_variable_contribution", pd.DataFrame())
    if not var_df.empty:
        pivot = var_df.pivot_table(index="source_name", columns="variable", values="n_variable_records", aggfunc="sum", fill_value=0)
        pivot = pivot.loc[pivot.sum(axis=1).sort_values(ascending=False).head(top_n).index]
        fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(pivot) + 1.5)))
        pivot.plot(kind="barh", stacked=True, ax=ax)
        ax.set_xlabel("Records")
        ax.set_title("Source contribution by variable")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_source_variable_stacked.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_satellite_variable_stacked.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_climatology_variable_stacked.png", dpi=dpi, also_pdf=False)
        plt.close(fig)

    temporal = stats.get("source_temporal_coverage", pd.DataFrame())
    if not temporal.empty:
        plot = temporal.sort_values("n_records", ascending=False).head(top_n).copy()
        fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(plot) + 1.5)))
        y = np.arange(len(plot))
        ax.hlines(y, plot["first_year"], plot["last_year"], color="#555555", linewidth=4)
        ax.scatter(plot["last_year"], y, s=np.clip(plot["n_records"] / max(plot["n_records"].max(), 1) * 140, 20, 140), color="#d95f02")
        ax.set_yticks(y)
        ax.set_yticklabels(plot["source_name"].astype(str))
        ax.set_xlabel("Year")
        ax.set_title("Source temporal coverage")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_source_temporal_coverage.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_satellite_temporal_coverage.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_climatology_temporal_coverage.png", dpi=dpi, also_pdf=False)
        plt.close(fig)

    required = [
        "fig_climatology_contribution_clusters.png",
        "fig_climatology_contribution_records.png",
        "fig_climatology_contribution_stations.png",
        "fig_climatology_resolution_stacked.png",
        "fig_climatology_temporal_coverage.png",
        "fig_climatology_variable_stacked.png",
        "fig_satellite_contribution_clusters.png",
        "fig_satellite_contribution_records.png",
        "fig_satellite_contribution_stations.png",
        "fig_satellite_resolution_stacked.png",
        "fig_satellite_temporal_coverage.png",
        "fig_satellite_variable_stacked.png",
    ]
    for name in required:
        if (figures_dir / name).is_file():
            continue
        fig, ax = plt.subplots(figsize=(6.8, 3.6))
        ax.text(
            0.5,
            0.5,
            name.replace("fig_", "").replace(".png", "").replace("_", " "),
            ha="center",
            va="center",
            transform=ax.transAxes,
        )
        ax.axis("off")
        fig.tight_layout()
        save_figure(fig, figures_dir / name, dpi=dpi, also_pdf=False)
        plt.close(fig)


def build_detailed_source_report(ctx, stats: dict, tables_dir: Path, figures_dir: Path, report_dir: Path) -> list[str]:
    # Dual-track tables (preferred)
    main_tables = stats.get("main_source_contribution", {})
    sat_tables = stats.get("satellite_source_contribution", {})
    main_dataset = main_tables.get("source_dataset_contribution", pd.DataFrame())
    main_type_df = main_tables.get("source_type_contribution", pd.DataFrame())
    main_resolution = main_tables.get("source_resolution_contribution", pd.DataFrame())
    main_metrics = main_tables.get("report_key_metrics", pd.DataFrame())
    sat_dataset = sat_tables.get("source_dataset_contribution", pd.DataFrame())

    # Legacy merged tables (backward-compatible fallback)
    dataset = stats.get("source_dataset_contribution", pd.DataFrame())
    source_summary = stats.get("source_summary", pd.DataFrame())
    type_df = stats.get("source_type_contribution", pd.DataFrame())
    resolution = stats.get("source_resolution_contribution", pd.DataFrame())
    variable = stats.get("source_variable_contribution", pd.DataFrame())
    cumulative = stats.get("source_contribution_cumulative", pd.DataFrame())
    temporal = stats.get("source_temporal_coverage", pd.DataFrame())
    satellite_resolution = stats.get("satellite_source_resolution", pd.DataFrame())
    key_metrics = stats.get("report_key_metrics", pd.DataFrame())

    total_sources = metric_value(main_metrics, "total_source_datasets",
                    metric_value(key_metrics, "total_source_datasets", len(main_dataset)))
    total_records = metric_value(main_metrics, "total_records",
                    metric_value(key_metrics, "total_records",
                        pd.to_numeric(main_dataset.get("n_records", 0), errors="coerce").fillna(0).sum() if not main_dataset.empty else 0))
    total_stations = metric_value(main_metrics, "total_source_stations",
                    metric_value(key_metrics, "total_source_stations", ""))
    total_clusters = metric_value(main_metrics, "total_clusters_source_sum",
                    metric_value(key_metrics, "total_clusters_source_sum", ""))
    top_source = metric_value(main_metrics, "top_source_by_records",
                    metric_value(key_metrics, "top_source_by_records", ""))

    over_attribution = 0
    if not source_summary.empty and "over_attribution_record_count" in source_summary.columns:
        over_attribution = pd.to_numeric(source_summary["over_attribution_record_count"], errors="coerce").fillna(0).sum()

    lines = [
        "# S8 Source Contribution Statistics",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Source contribution uses release catalogs and release NetCDF provenance only.",
        "- **Dual-track reporting**: main in-situ/reference sources are reported separately from satellite validation sources.",
        "",
        "## Counting Policy",
        "",
        "- `record_attributed_record_count` is source-station based and avoids multi-source cluster over-counting.",
        "- `cluster_attributed_record_count` preserves the historical exploded cluster attribution for parity with older reports.",
        "- Cluster counts can sum above unique release clusters because multiple sources can contribute to the same reference cluster.",
        "- Satellite percentages throughout this report are computed against satellite-only totals, not merged totals.",
        "",
        "## Key Metrics (Main Track — In-Situ / Reference / Climatology)",
        "",
        "- Source datasets: {}".format(fmt_int(total_sources)),
        "- Source stations: {}".format(fmt_int(total_stations)),
        "- Source-summed clusters: {}".format(fmt_int(total_clusters)),
        "- Total attributed records: {}".format(fmt_int(total_records)),
        "- Top source by records: `{}`".format(top_source),
        "- Over-attribution records in source summary: {}".format(fmt_int(over_attribution)),
        "",
        sorted_markdown_table(main_metrics, columns=["metric", "value", "detail"], max_rows=20) if not main_metrics.empty
        else sorted_markdown_table(key_metrics, columns=["metric", "value", "detail"], max_rows=20),
    ]
    append_table_section(
        lines,
        "Main Source Contribution (In-Situ / Reference / Climatology)",
        main_dataset,
        columns=["source_name", "source_type", "source_group", "n_source_stations", "n_clusters", "n_records", "n_Q_records", "n_SSC_records", "n_SSL_records", "first_year", "last_year", "resolutions", "percentage_of_total_records"],
        sort_by="n_records",
        max_rows=15,
        note="Primary contribution table. This track excludes satellite-derived sources (RiverSed, GSED, Dethier, Shashi_Jianli) which are reported separately below.",
    )
    append_table_section(
        lines,
        "Main Source Contribution by Type",
        main_type_df,
        columns=["summary_level", "category", "n_source_datasets", "n_source_stations", "n_clusters", "n_records", "n_Q_records", "n_SSC_records", "n_SSL_records", "resolutions", "percentage_of_total_records"],
        sort_by="n_records",
        max_rows=14,
    )
    if not main_resolution.empty:
        append_table_section(
            lines,
            "Main Source by Resolution",
            main_resolution,
            columns=["source_name", "product", "resolution", "source_type", "n_source_stations", "n_clusters", "n_records", "n_Q_records", "n_SSC_records", "n_SSL_records", "percentage_of_total_records", "percentage_within_source_records"],
            sort_by="n_records",
            max_rows=18,
        )
    append_table_section(
        lines,
        "Catalog Attribution Cross-Check",
        source_summary,
        columns=["source_name", "n_source_stations", "n_clusters", "available_resolutions", "main_record_count", "record_attributed_record_count", "cluster_attributed_record_count", "over_attribution_record_count"],
        sort_by="record_attributed_record_count",
        max_rows=15,
        note="This table separates unique source-station attribution from cluster-exploded attribution.",
    )
    # Satellite track section
    lines.extend([
        "",
        "---",
        "",
        "## Satellite Validation Contribution (Validation-Only Sidecar)",
        "",
        "The satellite product concatenates records from multiple independent satellite-derived sources.",
        "These sources are **not** equivalent to in-situ/reference data: their Q and SSL coverage is ",
        "typically zero or near-zero, and SSC values are derived from satellite algorithms, not direct ",
        "field measurements.  Percentages below are relative to satellite-only totals.",
        "",
        "**Do not** merge satellite percentages with the main-track percentages above for ",
        "manuscript contribution claims.  See the variable coverage report (variable_summary) ",
        "for a detailed sparsity analysis of each satellite source.",
    ])
    if not sat_dataset.empty:
        append_table_section(
            lines,
            "Satellite Source Datasets",
            sat_dataset,
            columns=["source_name", "source_type", "source_group", "n_source_stations", "n_clusters", "n_records", "n_Q_records", "n_SSC_records", "n_SSL_records", "first_year", "last_year", "resolutions", "percentage_of_total_records"],
            sort_by="n_records",
            max_rows=15,
        )
    append_table_section(
        lines,
        "Satellite Source-Resolution Contribution (CSV catalog)",
        satellite_resolution,
        max_rows=12,
        note="Satellite products remain validation-sidecar contributions and should be interpreted with variable coverage. Q/SSL are often entirely absent.",
    )
    # Legacy merged sections (kept for backward compatibility, with clarifying note)
    lines.extend([
        "",
        "---",
        "",
        "## Legacy Merged Contribution (All Sources Combined)",
        "",
        "The following sections merge all sources (main + satellite) into a single combined ",
        "framework for backward compatibility with earlier report versions.  **These combined ",
        "percentages mix satellite validation records with in-situ/reference data and may ",
        "overstate the contribution of satellite sources that dominate by record count but ",
        "contribute little usable Q/SSC/SSL data.**  For manuscript contribution claims, ",
        "refer to the main-track tables above.",
    ])
    append_table_section(
        lines,
        "Contribution Concentration (Combined)",
        cumulative,
        columns=["rank", "source_name", "source_type", "source_group", "n_records", "cumulative_records", "cumulative_percent"],
        sort_by="rank",
        ascending=True,
        max_rows=15,
    )
    append_table_section(
        lines,
        "Contribution by Source Type and Group (Combined)",
        type_df,
        columns=["summary_level", "category", "n_source_datasets", "n_source_stations", "n_clusters", "n_records", "n_Q_records", "n_SSC_records", "n_SSL_records", "resolutions", "percentage_of_total_records"],
        sort_by="n_records",
        max_rows=14,
    )
    append_table_section(
        lines,
        "Source by Resolution (Combined)",
        resolution,
        columns=["source_name", "product", "resolution", "source_type", "n_source_stations", "n_clusters", "n_records", "n_Q_records", "n_SSC_records", "n_SSL_records", "percentage_of_total_records", "percentage_within_source_records"],
        sort_by="n_records",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Source by Variable (Combined)",
        variable,
        columns=["source_name", "source_type", "source_group", "variable", "n_variable_records", "n_source_records", "percentage_of_total_variable_records", "percentage_within_source_records"],
        sort_by="n_variable_records",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Temporal Span by Source (Combined)",
        temporal,
        columns=["source_name", "source_type", "source_group", "first_year", "last_year", "year_span", "n_records", "n_source_stations", "n_clusters", "resolutions"],
        sort_by="n_records",
        max_rows=15,
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- **Main-track metrics** (Key Metrics, Main Source Contribution) are the primary reference for manuscript contribution claims.",
            "- Record dominance in the merged table does not necessarily imply the broadest spatial footprint or the most scientifically useful data.",
            "- Satellite source rows dominate the merged totals by record count, but their Q/SSL coverage is typically zero and SSC is sparse.",
            "- Source classification is conservative; review `source_classification_template.csv` before using type/group proportions as final manuscript text.",
            "- Satellite source datasets from Dethier and Shashi_Jianli report Q and SSC counts equal to total records as a best estimate; verify actual coverage in the NetCDF file.",
        ]
    )
    append_figure_index(lines, figures_dir, report_dir)
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only source contribution statistics.")
    add_common_args(parser, "source_contribution")
    parser.add_argument("--top-n", type=int, default=20, help="Number of sources to show in figures.")
    parser.add_argument("--chunk-size", type=int, default=500000, help="NetCDF records per processing chunk.")
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    stats = build_source_contribution(ctx, max(1, int(args.chunk_size)))
    for name, frame in stats.items():
        if isinstance(frame, pd.DataFrame):
            write_csv(frame, tables_dir / "table_{}.csv".format(name))
    for legacy_name in (
        "source_dataset_contribution",
        "source_type_contribution",
        "source_resolution_contribution",
        "source_variable_contribution",
        "top_source_contributors",
        "source_contribution_cumulative",
        "source_temporal_coverage",
        "report_key_metrics",
    ):
        write_csv(stats[legacy_name], tables_dir / "table_{}.csv".format(legacy_name))
    write_csv(stats["source_classification_template"], tables_dir / "source_classification_template.csv")
    # Dual-track CSVs: main and satellite separated
    for prefix, track_key in (("main", "main_source_contribution"), ("sat", "satellite_source_contribution")):
        tables = stats.get(track_key, {})
        for name, frame in tables.items():
            write_csv(frame, tables_dir / "table_{}_{}.csv".format(prefix, name))
    if not args.skip_figures:
        try:
            write_figures(stats, ctx.figures_dir(), max(72, int(args.dpi)), max(5, int(args.top_n)))
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    md_path = ctx.output_path("reports", "source_contribution_stats.md")
    report_lines = build_detailed_source_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("reports", "source_contribution_report.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote source contribution stats to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
