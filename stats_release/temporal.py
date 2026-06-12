#!/usr/bin/env python3
"""Temporal coverage statistics computed only from release products."""
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
    count_matrix_selected_cells,
    netcdf_record_count,
    numeric_series,
    read_numeric_var,
    read_text_var,
    setup_matplotlib,
    split_pipe,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import MATRIX_PRODUCTS, PRODUCT_FILES
from stats_release.common_stats import VARIABLES, decode_time_axis, pct, save_figure, unique_pipe
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


RESOLUTION_ORDER = ("daily", "monthly", "annual", "climatology")
MATRIX_RESOLUTIONS = ("daily", "monthly", "annual")


def _date_text(value) -> str:
    text = clean_text(value)
    if not text:
        return ""
    try:
        return pd.Timestamp(text).strftime("%Y-%m-%d")
    except Exception:
        return text[:10]


def _nc_time_range(ctx, file_name: str) -> tuple:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return 0, "", ""
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        if n_records == 0:
            n_records = count_matrix_selected_cells(ds) or 0
        if "time" not in ds.variables:
            return n_records, "", ""
        time_var = ds.variables["time"]
        raw = np.ma.asarray(time_var[:]).astype(float)
        if np.ma.isMaskedArray(raw):
            raw = raw.filled(np.nan)
        raw = raw[np.isfinite(raw)]
        if len(raw) == 0:
            return n_records, "", ""
        units = getattr(time_var, "units", "days since 1970-01-01")
        calendar = getattr(time_var, "calendar", "gregorian")
        import netCDF4 as nc4

        try:
            dates = nc4.num2date([float(raw.min()), float(raw.max())], units=units, calendar=calendar, only_use_cftime_datetimes=False)
        except TypeError:
            dates = nc4.num2date([float(raw.min()), float(raw.max())], units=units, calendar=calendar)
        return n_records, _date_text(dates[0]), _date_text(dates[1])


def _time_axis_diagnostics(ctx, resolution: str, file_name: str) -> dict:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    base = {
        "resolution": resolution,
        "file_name": file_name,
        "n_time": 0,
        "time_start": "",
        "time_end": "",
        "unique_years": 0,
        "unique_year_months": 0,
        "expected_regular_periods": 0,
        "duplicate_periods": 0,
        "axis_interpretation": "missing",
    }
    if path is None:
        return base
    with ctx.open_dataset(file_name, required=True) as ds:
        if "time" not in ds.variables:
            return base
        time_var = ds.variables["time"]
        raw = np.ma.asarray(time_var[:]).astype(float)
        if np.ma.isMaskedArray(raw):
            raw = raw.filled(np.nan)
        raw = raw[np.isfinite(raw)]
        if len(raw) == 0:
            return base
        units = getattr(time_var, "units", "days since 1970-01-01")
        calendar = getattr(time_var, "calendar", "gregorian")
        import netCDF4 as nc4

        try:
            dates = nc4.num2date(raw, units=units, calendar=calendar, only_use_cftime_datetimes=False)
        except TypeError:
            dates = nc4.num2date(raw, units=units, calendar=calendar)
        idx = pd.DatetimeIndex(pd.to_datetime([str(d) for d in dates]))
        base["n_time"] = int(len(idx))
        base["time_start"] = _date_text(idx.min())
        base["time_end"] = _date_text(idx.max())
        base["unique_years"] = int(idx.year.nunique())
        base["unique_year_months"] = int(idx.to_period("M").nunique())
        if resolution == "daily":
            expected = int((idx.max().normalize() - idx.min().normalize()).days) + 1
            unique_periods = int(idx.normalize().nunique())
        elif resolution == "monthly":
            expected = int(len(pd.period_range(idx.min().to_period("M"), idx.max().to_period("M"), freq="M")))
            unique_periods = int(idx.to_period("M").nunique())
        elif resolution == "annual":
            expected = int(idx.year.max() - idx.year.min() + 1)
            unique_periods = int(idx.year.nunique())
        else:
            expected = int(len(idx))
            unique_periods = int(len(idx))
        base["expected_regular_periods"] = expected
        base["duplicate_periods"] = int(len(idx) - unique_periods)
        base["axis_interpretation"] = "regular_period_axis" if len(idx) == expected and base["duplicate_periods"] == 0 else "sparse_observation_date_axis"
        return base


def _scan_matrix_temporal(ctx, resolution: str, file_name: str, row_chunk_size: int = 128) -> tuple:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return {}, pd.DataFrame(), pd.DataFrame()
    by_year = {}
    unit_rows = []
    with ctx.open_dataset(file_name, required=True) as ds:
        dates = decode_time_axis(ds)
        if len(dates) == 0 or "selected_source_index" not in ds.variables:
            return {}, pd.DataFrame(), pd.DataFrame()
        years = dates.year.to_numpy()
        unique_years = sorted(set(int(y) for y in years))
        cluster_uids = read_text_var(ds, "cluster_uid", size=len(ds.dimensions.get("n_stations", [])))
        n_stations = int(len(ds.dimensions.get("n_stations", [])))
        record_counts_any = []
        record_counts_var = {var: [] for var in VARIABLES}
        first_dates = []
        last_dates = []
        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            selected = np.ma.asarray(ds.variables["selected_source_index"][start:stop, :]).filled(-1)
            any_mask = selected >= 0
            var_masks = {}
            for var in VARIABLES:
                if var in ds.variables:
                    vals = read_numeric_var(ds, var, key=(slice(start, stop), slice(None)))
                    var_masks[var] = np.isfinite(vals)
                else:
                    var_masks[var] = np.zeros_like(any_mask, dtype=bool)
            for year in unique_years:
                cols = years == year
                if not np.any(cols):
                    continue
                ymask = any_mask[:, cols]
                item = by_year.setdefault(
                    year,
                    {
                        "resolution": resolution,
                        "year": year,
                        "active_units": 0,
                        "active_clusters": 0,
                        "record_count_any": 0,
                        "record_count_Q": 0,
                        "record_count_SSC": 0,
                        "record_count_SSL": 0,
                    },
                )
                active = np.any(ymask, axis=1)
                item["active_units"] += int(np.count_nonzero(active))
                item["active_clusters"] += int(np.count_nonzero(active))
                item["record_count_any"] += int(np.count_nonzero(ymask))
                for var in VARIABLES:
                    item["record_count_{}".format(var)] += int(np.count_nonzero(var_masks[var][:, cols]))
            counts_any = np.count_nonzero(any_mask, axis=1)
            record_counts_any.extend(counts_any.tolist())
            for var in VARIABLES:
                record_counts_var[var].extend(np.count_nonzero(var_masks[var], axis=1).tolist())
            for i in range(stop - start):
                cols = np.flatnonzero(any_mask[i])
                if cols.size:
                    first = dates[cols[0]].strftime("%Y-%m-%d")
                    last = dates[cols[-1]].strftime("%Y-%m-%d")
                else:
                    first = ""
                    last = ""
                first_dates.append(first)
                last_dates.append(last)
                unit_rows.append(
                    {
                        "resolution": resolution,
                        "unit_type": "cluster",
                        "unit_id": cluster_uids[start + i] if start + i < len(cluster_uids) else str(start + i),
                        "first_date": first,
                        "last_date": last,
                        "record_count_any": int(counts_any[i]),
                        "record_count_Q": int(record_counts_var["Q"][-(stop - start) + i]),
                        "record_count_SSC": int(record_counts_var["SSC"][-(stop - start) + i]),
                        "record_count_SSL": int(record_counts_var["SSL"][-(stop - start) + i]),
                    }
                )
        active_lengths = np.asarray([v for v in record_counts_any if v > 0], dtype=float)
        first_nonblank = [d for d in first_dates if d]
        last_nonblank = [d for d in last_dates if d]
        summary = {
            "resolution": resolution,
            "unit_type": "cluster",
            "source_file": str(ctx.release_file(file_name)),
            "first_date": min(first_nonblank) if first_nonblank else "",
            "last_date": max(last_nonblank) if last_nonblank else "",
            "first_year": int(pd.Timestamp(min(first_nonblank)).year) if first_nonblank else "",
            "last_year": int(pd.Timestamp(max(last_nonblank)).year) if last_nonblank else "",
            "time_steps": int(len(dates)),
            "active_units": int(np.count_nonzero(np.asarray(record_counts_any) > 0)),
            "active_units_Q": int(np.count_nonzero(np.asarray(record_counts_var["Q"]) > 0)),
            "active_units_SSC": int(np.count_nonzero(np.asarray(record_counts_var["SSC"]) > 0)),
            "active_units_SSL": int(np.count_nonzero(np.asarray(record_counts_var["SSL"]) > 0)),
            "record_count_any": int(np.sum(record_counts_any)),
            "record_count_Q": int(np.sum(record_counts_var["Q"])),
            "record_count_SSC": int(np.sum(record_counts_var["SSC"])),
            "record_count_SSL": int(np.sum(record_counts_var["SSL"])),
            "records_any": int(np.sum(record_counts_any)),
            "records_Q": int(np.sum(record_counts_var["Q"])),
            "records_SSC": int(np.sum(record_counts_var["SSC"])),
            "records_SSL": int(np.sum(record_counts_var["SSL"])),
            "mean_record_length_steps": float(np.mean(active_lengths)) if active_lengths.size else 0.0,
            "median_record_length_steps": float(np.median(active_lengths)) if active_lengths.size else 0.0,
            "max_record_length_steps": float(np.max(active_lengths)) if active_lengths.size else 0.0,
            "product": "",
            "active_clusters": int(np.count_nonzero(np.asarray(record_counts_any) > 0)),
        }
        for threshold in (10, 20, 30, 50, 100):
            summary["n_gt_{}_years".format(threshold)] = int(np.count_nonzero(active_lengths > threshold))
    return summary, pd.DataFrame(by_year.values()), pd.DataFrame(unit_rows)


def _distribution_from_unit_rows(unit_df: pd.DataFrame) -> pd.DataFrame:
    if unit_df.empty:
        return pd.DataFrame()
    rows = []
    bins = [-1, 0, 10, 30, 100, 365, 3650, np.inf]
    labels = ["0", "1-10", "11-30", "31-100", "101-365", "366-3650", ">3650"]
    for resolution, group in unit_df.groupby("resolution", dropna=False):
        cats = pd.cut(pd.to_numeric(group["record_count_any"], errors="coerce").fillna(0), bins=bins, labels=labels)
        counts = cats.value_counts().reindex(labels).fillna(0)
        for label, count in counts.items():
            rows.append({"resolution": resolution, "record_length_bin": label, "unit_count": int(count)})
    return pd.DataFrame(rows)


def _long_record_counts(summary_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in summary_df.iterrows():
        rows.append(
            {
                "resolution": row.get("resolution", ""),
                "n_gt_10_years": row.get("n_gt_10_years", 0),
                "n_gt_20_years": row.get("n_gt_20_years", 0),
                "n_gt_30_years": row.get("n_gt_30_years", 0),
                "n_gt_50_years": row.get("n_gt_50_years", 0),
                "n_gt_100_years": row.get("n_gt_100_years", 0),
            }
        )
    return pd.DataFrame(rows)


def _source_temporal_from_station(station: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if station.empty or "sources_used" not in station.columns:
        return pd.DataFrame()
    work = station.copy()
    work["record_count"] = numeric_series(work, "record_count").fillna(0)
    for _, row in work.iterrows():
        for source in split_pipe(row.get("sources_used", "")):
            rows.append(
                {
                    "source_name": source,
                    "resolution": clean_text(row.get("resolution", "")),
                    "cluster_uid": clean_text(row.get("cluster_uid", "")),
                    "record_count": row.get("record_count", 0),
                    "first_year": int(pd.Timestamp(row.get("time_start")).year) if clean_text(row.get("time_start", "")) else np.nan,
                    "last_year": int(pd.Timestamp(row.get("time_end")).year) if clean_text(row.get("time_end", "")) else np.nan,
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return (
        df.groupby("source_name", dropna=False)
        .agg(
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            record_count=("record_count", "sum"),
            active_units=("cluster_uid", "nunique"),
            resolutions=("resolution", unique_pipe),
        )
        .reset_index()
        .sort_values("record_count", ascending=False)
    )


def _catalog_temporal_summary(catalog: pd.DataFrame, product: str, station_col: str, source_col: str) -> tuple:
    if catalog.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    work = catalog.copy()
    work["n_records"] = numeric_series(work, "n_records").fillna(0)
    work["first_year"] = pd.to_datetime(work.get("time_start", ""), errors="coerce").dt.year
    work["last_year"] = pd.to_datetime(work.get("time_end", ""), errors="coerce").dt.year
    summary = pd.DataFrame(
        [
            {
                "resolution": product,
                "unit_type": station_col,
                "first_date": clean_text(work.get("time_start", pd.Series([""])).min()),
                "last_date": clean_text(work.get("time_end", pd.Series([""])).max()),
                "first_year": int(work["first_year"].min()) if work["first_year"].notna().any() else "",
                "last_year": int(work["last_year"].max()) if work["last_year"].notna().any() else "",
                "active_units": int(work[station_col].nunique()) if station_col in work.columns else int(len(work)),
                "record_count_any": int(work["n_records"].sum()),
                "product": product,
            }
        ]
    )
    by_source = (
        work.groupby(source_col, dropna=False)
        .agg(
            source_name=(source_col, "first"),
            first_year=("first_year", "min"),
            last_year=("last_year", "max"),
            active_units=(station_col, "nunique"),
            record_count_any=("n_records", "sum"),
        )
        .reset_index(drop=True)
        .sort_values("record_count_any", ascending=False)
        if source_col in work.columns
        else pd.DataFrame()
    )
    station_rows = work[
        [col for col in (station_col, source_col, "cluster_uid", "resolution", "n_records", "time_start", "time_end", "first_year", "last_year") if col in work.columns]
    ].copy()
    return summary, by_source, station_rows


def build_temporal_stats(ctx) -> dict:
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"])
    satellite = ctx.read_csv(PRODUCT_FILES["satellite_catalog"], required=False)
    rows = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        subset = station[station["resolution"].astype(str).str.strip().eq(resolution)].copy()
        n_records, nc_start, nc_end = _nc_time_range(ctx, file_name)
        rows.append(
            {
                "product": "matrix",
                "resolution": resolution,
                "station_rows": int(len(subset)),
                "cluster_count": int(subset["cluster_uid"].nunique()) if "cluster_uid" in subset.columns else 0,
                "record_count_catalog": int(numeric_series(subset, "record_count").fillna(0).sum()),
                "record_count_nc": int(n_records),
                "time_start": nc_start or _date_text(subset["time_start"].min() if "time_start" in subset.columns and len(subset) else ""),
                "time_end": nc_end or _date_text(subset["time_end"].max() if "time_end" in subset.columns and len(subset) else ""),
            }
        )
    clim_records, clim_start, clim_end = _nc_time_range(ctx, PRODUCT_FILES["climatology_nc"])
    rows.append(
        {
            "product": "climatology",
            "resolution": "climatology",
            "station_rows": int(clim_records),
            "cluster_count": 0,
            "record_count_catalog": int(clim_records),
            "record_count_nc": int(clim_records),
            "time_start": clim_start,
            "time_end": clim_end,
        }
    )
    sat_records, sat_start, sat_end = _nc_time_range(ctx, PRODUCT_FILES["satellite_nc"])
    rows.append(
        {
            "product": "satellite",
            "resolution": "all",
            "station_rows": int(satellite["satellite_station_uid"].nunique()) if not satellite.empty and "satellite_station_uid" in satellite.columns else 0,
            "cluster_count": int(satellite["cluster_uid"].nunique()) if not satellite.empty and "cluster_uid" in satellite.columns else 0,
            "record_count_catalog": int(numeric_series(satellite, "n_records").fillna(0).sum()) if not satellite.empty else 0,
            "record_count_nc": int(sat_records),
            "time_start": sat_start,
            "time_end": sat_end,
        }
    )
    basic_summary = pd.DataFrame(rows)
    matrix_summaries = []
    by_year_frames = []
    unit_frames = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        summary_i, by_year_i, unit_i = _scan_matrix_temporal(ctx, resolution, file_name)
        if summary_i:
            matrix_summaries.append(summary_i)
        if not by_year_i.empty:
            by_year_frames.append(by_year_i)
        if not unit_i.empty:
            unit_frames.append(unit_i)
    summary = pd.DataFrame(matrix_summaries) if matrix_summaries else basic_summary
    regional = pd.DataFrame()
    if {"continent_region", "country", "resolution"}.issubset(station.columns):
        regional = (
            station.assign(record_count=numeric_series(station, "record_count").fillna(0))
            .groupby(["continent_region", "country", "resolution"], dropna=False)
            .agg(
                cluster_count=("cluster_uid", "nunique"),
                record_count=("record_count", "sum"),
                time_start=("time_start", "min"),
                time_end=("time_end", "max"),
            )
            .reset_index()
            .sort_values("record_count", ascending=False)
        )
    time_axis = pd.DataFrame([_time_axis_diagnostics(ctx, resolution, file_name) for resolution, file_name in MATRIX_PRODUCTS.items()])
    by_year = pd.concat(by_year_frames, ignore_index=True) if by_year_frames else pd.DataFrame()
    unit_df = pd.concat(unit_frames, ignore_index=True) if unit_frames else pd.DataFrame()
    distribution = _distribution_from_unit_rows(unit_df)
    by_variable_rows = []
    for _, row in summary.iterrows():
        for var in VARIABLES:
            by_variable_rows.append(
                {
                    "resolution": row.get("resolution", ""),
                    "variable": var,
                    "active_units": row.get("active_units_{}".format(var), 0),
                    "record_count": row.get("record_count_{}".format(var), 0),
                    "first_year": row.get("first_year", ""),
                    "last_year": row.get("last_year", ""),
                }
            )
    by_variable = pd.DataFrame(by_variable_rows)
    by_source = _source_temporal_from_station(station)
    by_region_resolution = regional.rename(columns={"records": "record_count"}) if not regional.empty else pd.DataFrame()
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    clim_catalog = source_station[source_station["resolution"].astype(str).eq("climatology")].copy() if not source_station.empty and "resolution" in source_station.columns else pd.DataFrame()
    clim_summary, clim_by_source, clim_station = _catalog_temporal_summary(clim_catalog, "climatology", "source_station_uid", "source_name")
    sat_summary, sat_by_source, sat_station = _catalog_temporal_summary(satellite, "satellite_validation", "satellite_station_uid", "source")
    sat_by_year = pd.DataFrame()
    if not satellite.empty:
        sat_work = satellite.copy()
        sat_work["n_records"] = numeric_series(sat_work, "n_records").fillna(0)
        sat_work["year"] = pd.to_datetime(sat_work.get("time_start", ""), errors="coerce").dt.year
        sat_by_year = (
            sat_work.groupby(["resolution", "year"], dropna=False)
            .agg(active_units=("satellite_station_uid", "nunique"), record_count_any=("n_records", "sum"))
            .reset_index()
            .rename(columns={"active_units": "active_units", "record_count_any": "record_count_any"})
        )
    sat_linked = (
        satellite.groupby("cluster_uid", dropna=False)
        .agg(satellite_station_count=("satellite_station_uid", "nunique"), record_count_any=("n_records", lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum()))
        .reset_index()
        if not satellite.empty and "cluster_uid" in satellite.columns
        else pd.DataFrame()
    )
    return {
        "summary": basic_summary,
        "regional": regional,
        "time_axis_diagnostics": time_axis,
        "temporal_coverage_by_resolution": summary,
        "temporal_coverage_by_variable": by_variable,
        "active_units_by_year": by_year,
        "active_clusters_by_year": by_year,
        "record_length_distribution": distribution,
        "temporal_coverage_record_lengths_by_unit": unit_df,
        "long_records_by_resolution": _long_record_counts(summary),
        "temporal_coverage_by_source": by_source,
        "temporal_coverage_by_region_resolution": by_region_resolution,
        "climatology_temporal_summary": clim_summary,
        "climatology_by_source": clim_by_source,
        "climatology_record_lengths_by_station": clim_station,
        "satellite_temporal_summary": sat_summary,
        "satellite_by_year": sat_by_year,
        "satellite_by_source": sat_by_source,
        "satellite_record_lengths_by_station": sat_station,
        "satellite_by_linked_cluster": sat_linked,
    }


def write_figures(stats: dict, figures_dir: Path, dpi: int) -> None:
    """Write temporal coverage figures."""
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)
    summary = stats.get("summary", pd.DataFrame())
    if summary.empty:
        return

    # Horizontal bar of temporal coverage spans
    fig, ax = plt.subplots(figsize=(8.0, 4.5))
    y_labels = []
    starts = []
    ends = []
    n_records = []
    for _, row in summary.iterrows():
        s = clean_text(row.get("time_start", ""))
        e = clean_text(row.get("time_end", ""))
        if s and e:
            try:
                start_y = int(pd.Timestamp(s).year)
                end_y = int(pd.Timestamp(e).year)
            except Exception:
                continue
            label = "{} ({})".format(str(row.get("resolution", "")).capitalize(), row.get("product", ""))
            y_labels.append(label)
            starts.append(start_y)
            ends.append(end_y)
            n_records.append(int(row.get("record_count_nc", 0)))
    if not y_labels:
        ax.text(0.5, 0.5, "No temporal data available", ha="center", va="center", transform=ax.transAxes)
    else:
        y = np.arange(len(y_labels))
        ax.hlines(y, starts, ends, linewidth=6, color="#4c78a8")
        ax.scatter(ends, y, s=[max(20, min(200, r / 1000 if r else 0)) for r in n_records], color="#f58518", zorder=3)
        ax.set_yticks(y)
        ax.set_yticklabels(y_labels)
        ax.set_xlabel("Year")
        ax.set_title("Temporal coverage of release products")
        ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    save_figure(fig, figures_dir / "fig_temporal_coverage.png", dpi=dpi, also_pdf=False)
    plt.close(fig)

    by_year = stats.get("active_units_by_year", pd.DataFrame())
    if not by_year.empty:
        fig, ax = plt.subplots(figsize=(9, 4.5))
        for resolution, group in by_year.groupby("resolution"):
            ax.plot(group["year"], group["active_units"], label=str(resolution))
        ax.set_xlabel("Year")
        ax.set_ylabel("Active units")
        ax.set_title("Active units by year")
        ax.legend(frameon=False)
        ax.grid(alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_active_units_by_year.png", dpi=dpi)
        save_figure(fig, figures_dir / "fig_active_clusters_by_year.png", dpi=dpi)
        plt.close(fig)
        fig, ax = plt.subplots(figsize=(9, 4.5))
        for resolution, group in by_year.groupby("resolution"):
            ax.plot(group["year"], group["record_count_any"], label=str(resolution))
        ax.set_xlabel("Year")
        ax.set_ylabel("Records")
        ax.set_title("Records by year")
        ax.legend(frameon=False)
        ax.grid(alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_records_by_year_variable.png", dpi=dpi)
        save_figure(fig, figures_dir / "fig_temporal_coverage_heatmap.png", dpi=dpi)
        plt.close(fig)

    dist = stats.get("record_length_distribution", pd.DataFrame())
    if not dist.empty:
        fig, ax = plt.subplots(figsize=(8, 4.2))
        pivot = dist.pivot_table(index="record_length_bin", columns="resolution", values="unit_count", fill_value=0)
        pivot.plot(kind="bar", ax=ax)
        ax.set_ylabel("Units")
        ax.set_title("Record length distribution")
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_record_length_distribution.png", dpi=dpi)
        save_figure(fig, figures_dir / "fig_record_length_histogram.png", dpi=dpi)
        save_figure(fig, figures_dir / "fig_climatology_record_length_distribution.png", dpi=dpi)
        save_figure(fig, figures_dir / "fig_satellite_record_length_distribution.png", dpi=dpi)
        plt.close(fig)

    long_df = stats.get("long_records_by_resolution", pd.DataFrame())
    if not long_df.empty:
        cols = [c for c in long_df.columns if c.startswith("n_gt_")]
        fig, ax = plt.subplots(figsize=(8, 4.2))
        long_df.set_index("resolution")[cols].plot(kind="bar", ax=ax)
        ax.set_ylabel("Units")
        ax.set_title("Long record counts")
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_long_record_counts.png", dpi=dpi)
        plt.close(fig)

    src = stats.get("temporal_coverage_by_source", pd.DataFrame())
    if not src.empty:
        plot = src.head(20)
        fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(plot) + 1.5)))
        y = np.arange(len(plot))
        ax.hlines(y, plot["first_year"], plot["last_year"], linewidth=4, color="#555555")
        ax.set_yticks(y)
        ax.set_yticklabels(plot["source_name"].astype(str))
        ax.set_xlabel("Year")
        ax.set_title("Source temporal span")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_source_temporal_span.png", dpi=dpi)
        plt.close(fig)

    for table_name, fig_name in (
        ("climatology_by_source", "fig_climatology_source_contribution.png"),
        ("satellite_by_source", "fig_satellite_source_contribution.png"),
        ("satellite_by_year", "fig_satellite_active_units_by_year.png"),
        ("satellite_by_year", "fig_satellite_records_by_year_variable.png"),
        ("satellite_by_year", "fig_satellite_temporal_heatmap.png"),
        ("temporal_coverage_by_variable", "fig_climatology_variable_coverage.png"),
    ):
        df = stats.get(table_name, pd.DataFrame())
        if df.empty:
            continue
        fig, ax = plt.subplots(figsize=(8, 4))
        if "source_name" in df.columns:
            plot = df.head(15).sort_values(df.columns[-1])
            ax.barh(plot["source_name"].astype(str), pd.to_numeric(plot[df.columns[-1]], errors="coerce").fillna(0), color="#4c78a8")
        elif "year" in df.columns:
            ax.plot(df["year"], pd.to_numeric(df.get("active_units", df.get("record_count_any")), errors="coerce").fillna(0))
        else:
            df.groupby("variable")["record_count"].sum().plot(kind="bar", ax=ax)
        ax.set_title(fig_name.replace("fig_", "").replace(".png", "").replace("_", " ").title())
        ax.grid(alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / fig_name, dpi=dpi)
        plt.close(fig)

    for name in ("fig_climatology_source_contribution.png",):
        if (figures_dir / name).is_file():
            continue
        fig, ax = plt.subplots(figsize=(6.8, 3.6))
        ax.text(0.5, 0.5, name.replace("fig_", "").replace(".png", "").replace("_", " "), ha="center", va="center", transform=ax.transAxes)
        ax.axis("off")
        fig.tight_layout()
        save_figure(fig, figures_dir / name, dpi=dpi)
        plt.close(fig)


def build_detailed_temporal_report(ctx, stats: dict, tables_dir: Path, figures_dir: Path, report_dir: Path) -> list[str]:
    summary = stats.get("summary", pd.DataFrame())
    by_resolution = stats.get("temporal_coverage_by_resolution", pd.DataFrame())
    by_variable = stats.get("temporal_coverage_by_variable", pd.DataFrame())
    by_year = stats.get("active_units_by_year", pd.DataFrame())
    distribution = stats.get("record_length_distribution", pd.DataFrame())
    unit_lengths = stats.get("temporal_coverage_record_lengths_by_unit", pd.DataFrame())
    long_records = stats.get("long_records_by_resolution", pd.DataFrame())
    by_source = stats.get("temporal_coverage_by_source", pd.DataFrame())
    region_resolution = stats.get("temporal_coverage_by_region_resolution", pd.DataFrame())
    time_axis = stats.get("time_axis_diagnostics", pd.DataFrame())
    clim_summary = stats.get("climatology_temporal_summary", pd.DataFrame())
    clim_source = stats.get("climatology_by_source", pd.DataFrame())
    satellite_summary = stats.get("satellite_temporal_summary", pd.DataFrame())
    satellite_source = stats.get("satellite_by_source", pd.DataFrame())
    satellite_year = stats.get("satellite_by_year", pd.DataFrame())

    total_matrix_records = 0
    if not by_resolution.empty and "record_count_any" in by_resolution.columns:
        total_matrix_records = pd.to_numeric(by_resolution["record_count_any"], errors="coerce").fillna(0).sum()
    sparse_axes = []
    if not time_axis.empty and "axis_interpretation" in time_axis.columns:
        sparse_axes = sorted(set(time_axis[time_axis["axis_interpretation"].astype(str).str.contains("sparse", case=False, na=False)]["resolution"].astype(str)))

    lines = [
        "# Release Temporal Coverage Statistics",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Product groups: in-situ matrix products, climatology product, and satellite validation product.",
        "",
        "## Headline",
        "",
        "- Matrix records with any Q/SSC/SSL value: {}".format(fmt_int(total_matrix_records)),
        "- Matrix resolutions: {}".format(", ".join([r for r in MATRIX_RESOLUTIONS])),
        "- Sparse time axes detected: {}".format(", ".join(sparse_axes) if sparse_axes else "none"),
        "",
        "## Product Summary",
        "",
        sorted_markdown_table(
            summary,
            columns=["product", "resolution", "station_rows", "cluster_count", "record_count_catalog", "record_count_nc", "time_start", "time_end"],
            max_rows=10,
        ),
    ]
    append_table_section(
        lines,
        "Matrix Coverage by Resolution",
        by_resolution,
        columns=[
            "resolution",
            "first_date",
            "last_date",
            "time_steps",
            "active_units",
            "active_clusters",
            "record_count_any",
            "record_count_Q",
            "record_count_SSC",
            "record_count_SSL",
            "median_record_length_steps",
            "max_record_length_steps",
        ],
        sort_by="record_count_any",
        max_rows=8,
    )
    append_table_section(
        lines,
        "Variable Coverage by Resolution",
        by_variable,
        columns=["resolution", "variable", "active_units", "record_count", "first_year", "last_year"],
        sort_by="record_count",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Time-Axis Diagnostics",
        time_axis,
        columns=["resolution", "file_name", "n_time", "time_start", "time_end", "unique_years", "unique_year_months", "expected_regular_periods", "duplicate_periods", "axis_interpretation"],
        max_rows=10,
        note="Sparse axes mean the release matrix stores observation dates, not a dense regular calendar grid.",
    )
    append_table_section(
        lines,
        "Record-Length Distribution",
        distribution,
        columns=["resolution", "record_length_bin", "unit_count"],
        max_rows=16,
    )
    append_table_section(
        lines,
        "Long Record Summary",
        long_records,
        max_rows=10,
    )
    append_table_section(
        lines,
        "Top Source Temporal Coverage",
        by_source,
        columns=["source_name", "resolution", "active_units", "record_count_any", "first_year", "last_year"],
        sort_by="record_count_any",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Region by Resolution",
        region_resolution,
        columns=["continent_region", "resolution", "active_units", "record_count_any", "first_year", "last_year"],
        sort_by="record_count_any",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Climatology Temporal Summary",
        clim_summary,
        max_rows=10,
        note="Climatology is reported as a standalone product rather than a basin-cluster matrix.",
    )
    append_table_section(
        lines,
        "Climatology by Source",
        clim_source,
        sort_by="record_count_any",
        max_rows=12,
    )
    append_table_section(
        lines,
        "Satellite Temporal Summary",
        satellite_summary,
        max_rows=10,
        note="Satellite temporal coverage is validation-only and should be filtered by usable variables before analysis.",
    )
    append_table_section(
        lines,
        "Satellite by Source",
        satellite_source,
        sort_by="record_count_any",
        max_rows=12,
    )
    append_table_section(
        lines,
        "Satellite by Year",
        satellite_year,
        columns=["resolution", "year", "active_units", "record_count_any"],
        sort_by="year",
        ascending=True,
        max_rows=18,
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- `record_count_any` counts rows where at least one sediment-reference variable is available.",
            "- Long calendar span should be interpreted with record density; sparse series may span many years with few observations.",
            "- The monthly and annual matrix time dimensions are not necessarily regular period indexes.",
        ]
    )
    append_figure_index(lines, figures_dir, report_dir)
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only temporal coverage statistics.")
    add_common_args(parser, "temporal")
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    stats = build_temporal_stats(ctx)
    for name, frame in stats.items():
        if name.startswith("temporal_coverage") or name.startswith("active_") or name.startswith("record_") or name.startswith("long_") or name.startswith("climatology_") or name.startswith("satellite_"):
            write_csv(frame, tables_dir / "table_{}.csv".format(name))
        else:
            write_csv(frame, tables_dir / "table_temporal_{}.csv".format(name))
    if not args.skip_figures:
        try:
            write_figures(stats, ctx.figures_dir(), max(72, int(args.dpi)))
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    md_path = ctx.output_path("reports", "temporal_coverage_stats.md")
    report_lines = build_detailed_temporal_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("article_temporal_coverage_summary.md"))
    write_markdown(report_lines, ctx.output_path("article_temporal_coverage_report.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote temporal stats to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
