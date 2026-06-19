#!/usr/bin/env python3
"""Run manuscript-facing statistics for sed_reference_release_minimal.

This module reads only the minimal release package.  It does not require
sed_reference_master.nc, climatology, satellite products, GPKG sidecars, overlap
candidate tables, or selected_source_index.  It is intended to produce a stable
set of numbers that can be copied into the manuscript after rebuilding the S8
minimal package.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - handled at runtime
    nc4 = None

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import clean_text, ensure_parent, write_csv, write_json, write_markdown
from stats_release.reporting import fmt_int, safe_lines, sorted_markdown_table


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MINIMAL_RELEASE_DIR = PROJECT_ROOT / "scripts_basin_test" / "output" / "sed_reference_release_minimal"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output_other" / "stats_release_minimal"

MATRIX_PRODUCTS = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
CATALOG_FILES = (
    "station_catalog.csv",
    "source_station_catalog.csv",
    "source_dataset_catalog.csv",
)
AUXILIARY_FILES = (
    "README.md",
    "release_inventory.csv",
    "minimal_release_validation_report.csv",
)
VARIABLES = ("Q", "SSC", "SSL")
FLAG_VALUES = (0, 1, 2, 3, 8, 9)
FLAG_MEANINGS = {
    0: "good",
    1: "estimated",
    2: "suspect",
    3: "bad",
    8: "not_checked",
    9: "missing",
}


class MinimalStatsError(RuntimeError):
    """Raised when required minimal release inputs are missing or unreadable."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def release_manifest(release_dir: Path) -> pd.DataFrame:
    rows = []
    for path in sorted(p for p in release_dir.rglob("*") if p.is_file()):
        stat = path.stat()
        rows.append(
            {
                "relative_path": path.relative_to(release_dir).as_posix(),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "sha256": file_sha256(path),
            }
        )
    return pd.DataFrame(rows, columns=["relative_path", "size_bytes", "mtime_ns", "sha256"])


def require_minimal_inputs(release_dir: Path) -> None:
    required = [release_dir / name for name in list(MATRIX_PRODUCTS.values()) + list(CATALOG_FILES)]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise MinimalStatsError("Missing required minimal release file(s):\n  - " + "\n  - ".join(missing))
    if nc4 is None:
        raise MinimalStatsError("netCDF4 is required to inspect minimal matrix NetCDF files")


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([np.nan] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series([""] * len(frame), index=frame.index, dtype=object)
    return frame[column].map(clean_text)


def finite_numeric(values) -> np.ndarray:
    arr = np.ma.asarray(values).astype("float64")
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype="float64")
    arr[arr == -9999.0] = np.nan
    arr[arr == 1.0e20] = np.nan
    return arr


def decode_strings(values) -> List[str]:
    arr = np.asarray(values)
    if arr.size == 0:
        return []
    try:
        if arr.dtype.kind in {"S", "U"} and arr.ndim >= 2 and arr.shape[-1] > 1:
            arr = nc4.chartostring(arr)
    except Exception:
        pass
    arr = np.asarray(arr, dtype=object).reshape(-1)
    out = []
    for item in arr:
        if isinstance(item, bytes):
            item = item.decode("utf-8", errors="ignore")
        out.append(clean_text(item))
    return out


def read_text_var(ds, name: str, size: int | None = None) -> List[str]:
    if name not in ds.variables:
        return [""] * int(size or 0)
    return decode_strings(ds.variables[name][:])


def decode_time_range(ds) -> Tuple[int, str, str]:
    if "time" not in ds.variables:
        return 0, "", ""
    time_var = ds.variables["time"]
    raw = finite_numeric(time_var[:]).reshape(-1)
    raw = raw[np.isfinite(raw)]
    if raw.size == 0:
        return 0, "", ""
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "standard")
    try:
        dates = nc4.num2date([float(raw.min()), float(raw.max())], units=units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        dates = nc4.num2date([float(raw.min()), float(raw.max())], units=units, calendar=calendar)
    except Exception:
        return int(raw.size), "", ""
    return int(raw.size), pd.Timestamp(str(dates[0])).strftime("%Y-%m-%d"), pd.Timestamp(str(dates[1])).strftime("%Y-%m-%d")


def matrix_scan(path: Path, resolution: str, row_chunk_size: int = 128) -> Tuple[dict, pd.DataFrame, pd.DataFrame]:
    summary: Dict[str, object] = {
        "resolution": resolution,
        "file_name": path.name,
        "file_size_mb": round(path.stat().st_size / (1024 * 1024), 3),
        "n_stations": 0,
        "n_time": 0,
        "time_start": "",
        "time_end": "",
        "record_count_any": 0,
        "selected_source_station_uid_present": 0,
        "selected_source_station_uid_unique": 0,
    }
    for var in VARIABLES:
        summary[f"record_count_{var}"] = 0
        summary[f"active_stations_{var}"] = 0
    summary["active_stations_any"] = 0

    flag_counts: Dict[Tuple[str, int], int] = {}
    selected_uid_values = set()
    selected_uid_present = 0

    with nc4.Dataset(str(path), "r") as ds:
        n_stations = int(len(ds.dimensions.get("n_stations", [])))
        if n_stations == 0:
            for dim_name in ds.dimensions:
                if "station" in dim_name.lower():
                    n_stations = int(len(ds.dimensions[dim_name]))
                    break
        n_time, time_start, time_end = decode_time_range(ds)
        summary.update({"n_stations": n_stations, "n_time": n_time, "time_start": time_start, "time_end": time_end})

        lat_values = finite_numeric(ds.variables["lat"][:]) if "lat" in ds.variables else np.asarray([])
        lon_values = finite_numeric(ds.variables["lon"][:]) if "lon" in ds.variables else np.asarray([])
        lat_values = lat_values[np.isfinite(lat_values)]
        lon_values = lon_values[np.isfinite(lon_values)]
        summary["lat_min"] = float(np.min(lat_values)) if lat_values.size else np.nan
        summary["lat_max"] = float(np.max(lat_values)) if lat_values.size else np.nan
        summary["lon_min"] = float(np.min(lon_values)) if lon_values.size else np.nan
        summary["lon_max"] = float(np.max(lon_values)) if lon_values.size else np.nan

        active_any = np.zeros(n_stations, dtype=bool)
        active_by_var = {var: np.zeros(n_stations, dtype=bool) for var in VARIABLES}
        row_chunk_size = max(1, int(row_chunk_size))
        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            row_mask_any = np.zeros((stop - start, n_time), dtype=bool)
            for var in VARIABLES:
                if var not in ds.variables:
                    values = np.zeros((stop - start, n_time), dtype=bool)
                    present = values
                else:
                    values = finite_numeric(ds.variables[var][start:stop, :])
                    present = np.isfinite(values)
                row_mask_any |= present
                summary[f"record_count_{var}"] += int(np.count_nonzero(present))
                active_by_var[var][start:stop] = np.any(present, axis=1)

                flag_name = f"{var}_flag"
                if flag_name in ds.variables:
                    flags = finite_numeric(ds.variables[flag_name][start:stop, :]).reshape(-1)
                    valid_flags = flags[np.isfinite(flags)].astype(int)
                    for flag in FLAG_VALUES:
                        flag_counts[(var, flag)] = flag_counts.get((var, flag), 0) + int(np.count_nonzero(valid_flags == flag))
                    other = int(np.count_nonzero(~np.isin(valid_flags, FLAG_VALUES)))
                    if other:
                        flag_counts[(var, -1)] = flag_counts.get((var, -1), 0) + other

            active_any[start:stop] = np.any(row_mask_any, axis=1)
            summary["record_count_any"] += int(np.count_nonzero(row_mask_any))

            if "selected_source_station_uid" in ds.variables:
                values = decode_strings(ds.variables["selected_source_station_uid"][start:stop, :])
                for value in values:
                    if value:
                        selected_uid_present += 1
                        selected_uid_values.add(value)

        summary["active_stations_any"] = int(np.count_nonzero(active_any))
        for var in VARIABLES:
            summary[f"active_stations_{var}"] = int(np.count_nonzero(active_by_var[var]))
        summary["selected_source_station_uid_present"] = int(selected_uid_present)
        summary["selected_source_station_uid_unique"] = int(len(selected_uid_values))

    flag_rows = []
    for (var, flag), count in sorted(flag_counts.items()):
        flag_rows.append(
            {
                "resolution": resolution,
                "variable": var,
                "flag": flag,
                "flag_meaning": "other" if flag == -1 else FLAG_MEANINGS.get(flag, "unknown"),
                "count": int(count),
            }
        )
    uid_rows = [{"resolution": resolution, "source_station_uid": value} for value in sorted(selected_uid_values)]
    return summary, pd.DataFrame(flag_rows), pd.DataFrame(uid_rows)


def build_catalog_stats(release_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    station = read_csv_if_exists(release_dir / "station_catalog.csv")
    source_station = read_csv_if_exists(release_dir / "source_station_catalog.csv")
    source_dataset = read_csv_if_exists(release_dir / "source_dataset_catalog.csv")

    station_rows = []
    if not station.empty:
        for resolution, group in station.groupby(text(station, "resolution"), dropna=False):
            resolution = clean_text(resolution) or "unknown"
            station_rows.append(
                {
                    "resolution": resolution,
                    "station_catalog_rows": int(len(group)),
                    "unique_clusters": int(text(group, "cluster_uid").replace("", np.nan).dropna().nunique()),
                    "record_count_sum": int(numeric(group, "record_count").fillna(0).sum()),
                    "n_valid_time_steps_sum": int(numeric(group, "n_valid_time_steps").fillna(0).sum()),
                    "time_start": text(group, "time_start").replace("", np.nan).dropna().min() if "time_start" in group.columns else "",
                    "time_end": text(group, "time_end").replace("", np.nan).dropna().max() if "time_end" in group.columns else "",
                    "lat_min": numeric(group, "lat").min(),
                    "lat_max": numeric(group, "lat").max(),
                    "lon_min": numeric(group, "lon").min(),
                    "lon_max": numeric(group, "lon").max(),
                }
            )
    station_summary = pd.DataFrame(station_rows)

    source_station_rows = []
    if not source_station.empty:
        for resolution, group in source_station.groupby(text(source_station, "resolution"), dropna=False):
            resolution = clean_text(resolution) or "unknown"
            source_station_rows.append(
                {
                    "resolution": resolution,
                    "source_station_catalog_rows": int(len(group)),
                    "unique_source_stations": int(text(group, "source_station_uid").replace("", np.nan).dropna().nunique()),
                    "unique_sources": int(text(group, "source_name").replace("", np.nan).dropna().nunique()),
                    "unique_clusters": int(text(group, "cluster_uid").replace("", np.nan).dropna().nunique()),
                    "n_records_sum": int(numeric(group, "n_records").fillna(0).sum()),
                    "time_start": text(group, "time_start").replace("", np.nan).dropna().min() if "time_start" in group.columns else "",
                    "time_end": text(group, "time_end").replace("", np.nan).dropna().max() if "time_end" in group.columns else "",
                }
            )
    source_station_summary = pd.DataFrame(source_station_rows)

    dataset_rows = []
    if not source_dataset.empty:
        doi_cols = [col for col in ("data_doi", "article_doi") if col in source_dataset.columns]
        citation_cols = [col for col in ("preferred_citation", "reference") if col in source_dataset.columns]
        for _, row in source_dataset.iterrows():
            source_name = clean_text(row.get("source_name", ""))
            doi_present = any(clean_text(row.get(col, "")) for col in doi_cols)
            citation_present = any(clean_text(row.get(col, "")) for col in citation_cols)
            dataset_rows.append(
                {
                    "source_name": source_name,
                    "n_records": pd.to_numeric(row.get("n_records", 0), errors="coerce"),
                    "n_source_stations": pd.to_numeric(row.get("n_source_stations", 0), errors="coerce"),
                    "n_clusters": pd.to_numeric(row.get("n_clusters", 0), errors="coerce"),
                    "has_doi": int(bool(doi_present)),
                    "has_license_or_terms": int(bool(clean_text(row.get("license_or_terms", "")))),
                    "has_access_date": int(bool(clean_text(row.get("access_date", "")))),
                    "has_citation": int(bool(citation_present)),
                    "variables_used": clean_text(row.get("variables_used", "")),
                    "temporal_span_used": clean_text(row.get("temporal_span_used", "")),
                }
            )
    source_dataset_summary = pd.DataFrame(dataset_rows)

    metadata_rows = []
    metadata_fields = [
        ("source_dataset_catalog.csv", source_dataset, "data_doi"),
        ("source_dataset_catalog.csv", source_dataset, "article_doi"),
        ("source_dataset_catalog.csv", source_dataset, "license_or_terms"),
        ("source_dataset_catalog.csv", source_dataset, "access_date"),
        ("source_dataset_catalog.csv", source_dataset, "preferred_citation"),
        ("source_dataset_catalog.csv", source_dataset, "reference"),
        ("source_dataset_catalog.csv", source_dataset, "source_url"),
    ]
    for catalog_name, frame, column in metadata_fields:
        total = int(len(frame))
        present = int(text(frame, column).ne("").sum()) if total and column in frame.columns else 0
        metadata_rows.append(
            {
                "catalog": catalog_name,
                "field": column,
                "rows": total,
                "present": present,
                "missing": total - present,
                "present_percent": round(100.0 * present / total, 6) if total else 0.0,
            }
        )
    metadata_completeness = pd.DataFrame(metadata_rows)
    return station_summary, source_station_summary, source_dataset_summary, metadata_completeness


def build_cross_checks(matrix_summary: pd.DataFrame, station_summary: pd.DataFrame, source_station_summary: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, mrow in matrix_summary.iterrows():
        resolution = clean_text(mrow.get("resolution", ""))
        srow = station_summary[station_summary.get("resolution", pd.Series(dtype=object)).astype(str).eq(resolution)] if not station_summary.empty else pd.DataFrame()
        ssrow = source_station_summary[source_station_summary.get("resolution", pd.Series(dtype=object)).astype(str).eq(resolution)] if not source_station_summary.empty else pd.DataFrame()
        station_clusters = int(srow.iloc[0].get("unique_clusters", 0)) if not srow.empty else 0
        station_records = int(srow.iloc[0].get("record_count_sum", 0)) if not srow.empty else 0
        source_station_records = int(ssrow.iloc[0].get("n_records_sum", 0)) if not ssrow.empty else 0
        rows.append(
            {
                "resolution": resolution,
                "check": "matrix_station_count_vs_catalog_unique_clusters",
                "status": "pass" if int(mrow.get("n_stations", 0)) == station_clusters else "warning",
                "matrix_value": int(mrow.get("n_stations", 0)),
                "catalog_value": station_clusters,
                "details": "n_stations should normally match station_catalog unique cluster_uid for the same resolution",
            }
        )
        rows.append(
            {
                "resolution": resolution,
                "check": "matrix_any_record_count_vs_station_catalog_sum",
                "status": "pass" if int(mrow.get("record_count_any", 0)) == station_records else "warning",
                "matrix_value": int(mrow.get("record_count_any", 0)),
                "catalog_value": station_records,
                "details": "record_count_any is computed from any finite Q/SSC/SSL matrix cell",
            }
        )
        rows.append(
            {
                "resolution": resolution,
                "check": "matrix_any_record_count_vs_source_station_catalog_sum",
                "status": "pass" if int(mrow.get("record_count_any", 0)) == source_station_records else "warning",
                "matrix_value": int(mrow.get("record_count_any", 0)),
                "catalog_value": source_station_records,
                "details": "source_station_catalog may count source-station rows and can legitimately differ from selected matrix cells when provenance rows aggregate differently",
            }
        )
    return pd.DataFrame(rows)


def add_number(rows: list, field: str, value, unit: str, source: str, method: str, resolution: str = "all", notes: str = "") -> None:
    rows.append(
        {
            "manuscript_field": field,
            "resolution": resolution,
            "value": value,
            "unit": unit,
            "source_file": source,
            "source_column_or_method": method,
            "notes": notes,
        }
    )


def build_manuscript_numbers(
    matrix_summary: pd.DataFrame,
    station_summary: pd.DataFrame,
    source_station_summary: pd.DataFrame,
    source_dataset_summary: pd.DataFrame,
    metadata_completeness: pd.DataFrame,
    validation: pd.DataFrame,
) -> pd.DataFrame:
    rows = []
    total_clusters = int(station_summary["unique_clusters"].sum()) if not station_summary.empty and "unique_clusters" in station_summary.columns else 0
    unique_source_stations = int(source_station_summary["unique_source_stations"].sum()) if not source_station_summary.empty and "unique_source_stations" in source_station_summary.columns else 0
    n_sources = int(source_dataset_summary["source_name"].replace("", np.nan).dropna().nunique()) if not source_dataset_summary.empty else 0
    time_start = matrix_summary["time_start"].replace("", np.nan).dropna().min() if not matrix_summary.empty else ""
    time_end = matrix_summary["time_end"].replace("", np.nan).dropna().max() if not matrix_summary.empty else ""

    add_number(rows, "minimal_release_cluster_resolution_rows", int(station_summary["station_catalog_rows"].sum()) if not station_summary.empty else 0, "rows", "station_catalog.csv", "sum(station_catalog rows)")
    add_number(rows, "minimal_release_clusters_sum_by_resolution", total_clusters, "cluster-resolution unique cluster_uid count", "station_catalog.csv", "sum unique cluster_uid by resolution", notes="Use per-resolution rows for exact reporting; summed value counts the same cluster once per resolution.")
    add_number(rows, "minimal_release_source_station_resolution_rows", int(source_station_summary["source_station_catalog_rows"].sum()) if not source_station_summary.empty else 0, "rows", "source_station_catalog.csv", "sum(source_station_catalog rows)")
    add_number(rows, "minimal_release_source_stations_sum_by_resolution", unique_source_stations, "source-station-resolution unique source_station_uid count", "source_station_catalog.csv", "sum unique source_station_uid by resolution")
    add_number(rows, "minimal_release_source_datasets", n_sources, "datasets", "source_dataset_catalog.csv", "unique source_name")
    add_number(rows, "minimal_release_time_start", time_start, "date", "matrix NetCDF files", "minimum decoded time coordinate")
    add_number(rows, "minimal_release_time_end", time_end, "date", "matrix NetCDF files", "maximum decoded time coordinate")

    for _, row in matrix_summary.iterrows():
        resolution = clean_text(row.get("resolution", ""))
        source = clean_text(row.get("file_name", ""))
        add_number(rows, "matrix_stations", int(row.get("n_stations", 0)), "stations/clusters", source, "n_stations dimension", resolution)
        add_number(rows, "matrix_time_steps", int(row.get("n_time", 0)), "time steps", source, "time dimension", resolution)
        add_number(rows, "matrix_record_count_any", int(row.get("record_count_any", 0)), "station-time cells", source, "count finite Q or SSC or SSL", resolution)
        for var in VARIABLES:
            add_number(rows, f"matrix_{var}_nonmissing_records", int(row.get(f"record_count_{var}", 0)), "station-time cells", source, f"count finite {var}", resolution)
            add_number(rows, f"matrix_{var}_active_stations", int(row.get(f"active_stations_{var}", 0)), "stations/clusters", source, f"stations with any finite {var}", resolution)
        add_number(rows, "matrix_active_stations_any", int(row.get("active_stations_any", 0)), "stations/clusters", source, "stations with any finite Q/SSC/SSL", resolution)
        add_number(rows, "matrix_selected_source_station_uid_nonempty_cells", int(row.get("selected_source_station_uid_present", 0)), "station-time cells", source, "non-empty selected_source_station_uid", resolution)
        add_number(rows, "matrix_selected_source_station_uid_unique", int(row.get("selected_source_station_uid_unique", 0)), "source stations", source, "unique selected_source_station_uid", resolution)
        add_number(rows, "matrix_time_start", row.get("time_start", ""), "date", source, "decoded minimum time", resolution)
        add_number(rows, "matrix_time_end", row.get("time_end", ""), "date", source, "decoded maximum time", resolution)

    if not metadata_completeness.empty:
        for _, row in metadata_completeness.iterrows():
            add_number(rows, f"metadata_present_{row['field']}", int(row.get("present", 0)), "source datasets", row.get("catalog", ""), row.get("field", ""), notes="Metadata completeness count for source_dataset_catalog.csv")
            add_number(rows, f"metadata_missing_{row['field']}", int(row.get("missing", 0)), "source datasets", row.get("catalog", ""), row.get("field", ""), notes="Metadata completeness count for source_dataset_catalog.csv")

    if not validation.empty and "status" in validation.columns:
        status_counts = validation["status"].astype(str).str.lower().value_counts().to_dict()
        for status, count in sorted(status_counts.items()):
            add_number(rows, f"minimal_validation_{status}_checks", int(count), "checks", "minimal_release_validation_report.csv", "status value_counts")

    return pd.DataFrame(rows)


def write_report(
    out_dir: Path,
    release_dir: Path,
    matrix_summary: pd.DataFrame,
    station_summary: pd.DataFrame,
    source_station_summary: pd.DataFrame,
    source_dataset_summary: pd.DataFrame,
    metadata_completeness: pd.DataFrame,
    cross_checks: pd.DataFrame,
    manuscript_numbers: pd.DataFrame,
    run_started: str,
    run_finished: str,
) -> Path:
    validation_warnings = int(cross_checks["status"].astype(str).eq("warning").sum()) if not cross_checks.empty else 0
    total_records = int(matrix_summary["record_count_any"].sum()) if not matrix_summary.empty else 0
    total_sources = int(source_dataset_summary["source_name"].replace("", np.nan).dropna().nunique()) if not source_dataset_summary.empty else 0
    lines = [
        "# Minimal Release Statistics Report",
        "",
        "## Scope",
        "",
        "- Release directory: `{}`".format(release_dir),
        "- Output directory: `{}`".format(out_dir),
        "- Run started UTC: `{}`".format(run_started),
        "- Run finished UTC: `{}`".format(run_finished),
        "- Products inspected: daily, monthly, and annual minimal matrix NetCDF files plus compact catalogs.",
        "- Excluded by design: master, climatology, satellite, GPKG, parquet, and overlap-candidate products.",
        "",
        "## Headline numbers",
        "",
        "- Matrix station-time cells with any Q/SSC/SSL value: {}".format(fmt_int(total_records)),
        "- Source datasets in minimal catalog: {}".format(fmt_int(total_sources)),
        "- Cross-check warnings: {}".format(fmt_int(validation_warnings)),
        "",
        "## Matrix summary by resolution",
        "",
        sorted_markdown_table(
            matrix_summary,
            columns=[
                "resolution",
                "n_stations",
                "n_time",
                "time_start",
                "time_end",
                "record_count_any",
                "record_count_Q",
                "record_count_SSC",
                "record_count_SSL",
                "active_stations_any",
            ],
            max_rows=10,
        ),
        "",
        "## Station catalog summary",
        "",
        sorted_markdown_table(
            station_summary,
            columns=["resolution", "station_catalog_rows", "unique_clusters", "record_count_sum", "time_start", "time_end"],
            max_rows=10,
        ),
        "",
        "## Source-station catalog summary",
        "",
        sorted_markdown_table(
            source_station_summary,
            columns=["resolution", "source_station_catalog_rows", "unique_source_stations", "unique_sources", "n_records_sum", "time_start", "time_end"],
            max_rows=10,
        ),
        "",
        "## Source-dataset metadata completeness",
        "",
        sorted_markdown_table(
            metadata_completeness,
            columns=["field", "rows", "present", "missing", "present_percent"],
            max_rows=20,
        ),
        "",
        "## Cross-checks",
        "",
        sorted_markdown_table(
            cross_checks,
            columns=["resolution", "check", "status", "matrix_value", "catalog_value", "details"],
            max_rows=30,
        ),
        "",
        "## Manuscript number table",
        "",
        "Use `manuscript_numbers.csv` as the authoritative table for updating text and tables.",
    ]
    report_path = out_dir / "minimal_release_stats_report.md"
    write_markdown(safe_lines(lines), report_path)
    return report_path


def copy_reports_to_docs(report_path: Path, enabled: bool) -> None:
    if not enabled:
        return
    docs_dir = PROJECT_ROOT / "docs" / "reports"
    docs_dir.mkdir(parents=True, exist_ok=True)
    target = docs_dir / report_path.name
    target.write_text(report_path.read_text(encoding="utf-8"), encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run manuscript-facing statistics for sed_reference_release_minimal.")
    parser.add_argument("--release-dir", default=str(DEFAULT_MINIMAL_RELEASE_DIR), help="Path to sed_reference_release_minimal.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory for minimal release statistics.")
    parser.add_argument("--row-chunk-size", type=int, default=128, help="Station-row chunk size for matrix scans.")
    parser.add_argument("--copy-reports", action="store_true", help="Also copy Markdown report to docs/reports/.")
    args = parser.parse_args(argv)

    release_dir = Path(args.release_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    tables_dir = out_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    run_started = utc_now()
    require_minimal_inputs(release_dir)

    manifest = release_manifest(release_dir)
    write_csv(manifest, out_dir / "run_manifest.csv")

    matrix_rows = []
    flag_frames = []
    uid_frames = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        summary, flag_df, uid_df = matrix_scan(release_dir / file_name, resolution, row_chunk_size=args.row_chunk_size)
        matrix_rows.append(summary)
        if not flag_df.empty:
            flag_frames.append(flag_df)
        if not uid_df.empty:
            uid_frames.append(uid_df)
    matrix_summary = pd.DataFrame(matrix_rows)
    flag_counts = pd.concat(flag_frames, ignore_index=True) if flag_frames else pd.DataFrame(columns=["resolution", "variable", "flag", "flag_meaning", "count"])
    selected_source_uids = pd.concat(uid_frames, ignore_index=True) if uid_frames else pd.DataFrame(columns=["resolution", "source_station_uid"])

    station_summary, source_station_summary, source_dataset_summary, metadata_completeness = build_catalog_stats(release_dir)
    validation = read_csv_if_exists(release_dir / "minimal_release_validation_report.csv")
    cross_checks = build_cross_checks(matrix_summary, station_summary, source_station_summary)
    manuscript_numbers = build_manuscript_numbers(matrix_summary, station_summary, source_station_summary, source_dataset_summary, metadata_completeness, validation)

    write_csv(matrix_summary, tables_dir / "table_matrix_summary_by_resolution.csv")
    write_csv(flag_counts, tables_dir / "table_matrix_flag_counts.csv")
    write_csv(selected_source_uids, tables_dir / "table_selected_source_station_uids.csv")
    write_csv(station_summary, tables_dir / "table_station_catalog_summary_by_resolution.csv")
    write_csv(source_station_summary, tables_dir / "table_source_station_catalog_summary_by_resolution.csv")
    write_csv(source_dataset_summary, tables_dir / "table_source_dataset_summary.csv")
    write_csv(metadata_completeness, tables_dir / "table_source_dataset_metadata_completeness.csv")
    write_csv(cross_checks, tables_dir / "table_minimal_cross_checks.csv")
    write_csv(manuscript_numbers, out_dir / "manuscript_numbers.csv")

    run_finished = utc_now()
    report_path = write_report(
        out_dir,
        release_dir,
        matrix_summary,
        station_summary,
        source_station_summary,
        source_dataset_summary,
        metadata_completeness,
        cross_checks,
        manuscript_numbers,
        run_started,
        run_finished,
    )
    copy_reports_to_docs(report_path, bool(args.copy_reports))

    write_json(
        {
            "run_started_utc": run_started,
            "run_finished_utc": run_finished,
            "release_dir": str(release_dir),
            "out_dir": str(out_dir),
            "matrix_products": MATRIX_PRODUCTS,
            "catalog_files": list(CATALOG_FILES),
            "outputs": [
                "run_manifest.csv",
                "minimal_release_stats_report.md",
                "manuscript_numbers.csv",
                "tables/table_matrix_summary_by_resolution.csv",
                "tables/table_matrix_flag_counts.csv",
                "tables/table_selected_source_station_uids.csv",
                "tables/table_station_catalog_summary_by_resolution.csv",
                "tables/table_source_station_catalog_summary_by_resolution.csv",
                "tables/table_source_dataset_summary.csv",
                "tables/table_source_dataset_metadata_completeness.csv",
                "tables/table_minimal_cross_checks.csv",
            ],
        },
        out_dir / "run_manifest.json",
    )

    warnings = int(cross_checks["status"].astype(str).eq("warning").sum()) if not cross_checks.empty else 0
    print("[write] {}".format(out_dir / "manuscript_numbers.csv"))
    print("[write] {}".format(report_path))
    if warnings:
        print("[warn] {} cross-check warning(s); inspect table_minimal_cross_checks.csv".format(warnings))
    print("[done] minimal release statistics completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
