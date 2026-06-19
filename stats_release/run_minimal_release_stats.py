#!/usr/bin/env python3
"""Compute manuscript-facing statistics for sed_reference_release_minimal.

This runner is intentionally separate from run_all_release_stats.py.  The
standard release statistics suite expects the full S8 release package, including
sed_reference_master.nc, climatology, satellite products, GPKG sidecars, and
selected_source_index.  The minimal release intentionally excludes those files
and variables, so this module reads only the six minimal release products:

- sed_reference_timeseries_daily.nc
- sed_reference_timeseries_monthly.nc
- sed_reference_timeseries_annual.nc
- station_catalog.csv
- source_station_catalog.csv
- source_dataset_catalog.csv
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - reported at runtime
    nc4 = None


MINIMAL_MATRIX_PRODUCTS = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
MINIMAL_CATALOGS = (
    "station_catalog.csv",
    "source_station_catalog.csv",
    "source_dataset_catalog.csv",
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
MISSING_NUMERIC_SENTINELS = (-9999.0, 1.0e20)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_csv(frame: pd.DataFrame, path: Path) -> Path:
    ensure_parent(path)
    frame.to_csv(path, index=False)
    return path


def write_json(payload: dict, path: Path) -> Path:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def write_markdown(lines: Iterable[str], path: Path) -> Path:
    ensure_parent(path)
    path.write_text("\n".join(str(line) for line in lines).rstrip() + "\n", encoding="utf-8")
    return path


def read_csv(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def file_manifest(root: Path) -> list[dict]:
    root = Path(root).resolve()
    rows = []
    if not root.exists():
        return rows
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        stat = path.stat()
        rows.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )
    return rows


def metadata_fingerprint(root: Path) -> str:
    payload = json.dumps(file_manifest(root), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def script_fingerprint() -> str:
    root = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(root.glob("*.py")):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def valid_mask(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype="float64")
    mask = np.isfinite(arr)
    for fill in MISSING_NUMERIC_SENTINELS:
        mask &= arr != fill
    return mask


def read_numeric_var(ds, name: str, key=slice(None)) -> np.ndarray:
    if name not in ds.variables:
        return np.asarray([], dtype="float64")
    arr = np.ma.asarray(ds.variables[name][key]).astype("float64")
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    arr = np.asarray(arr, dtype="float64")
    for fill in MISSING_NUMERIC_SENTINELS:
        arr[arr == fill] = np.nan
    return arr


def read_flag_var(ds, name: str, key, fallback_shape: tuple[int, ...]) -> np.ndarray:
    if name not in ds.variables:
        return np.full(fallback_shape, 9, dtype="int16")
    arr = np.ma.asarray(ds.variables[name][key])
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(9)
    return np.asarray(arr).astype("int16", copy=False)


def decode_text_array(raw) -> list[str]:
    if np.ma.isMaskedArray(raw):
        raw = raw.filled("")
    arr = np.asarray(raw)
    if arr.dtype.kind == "S":
        try:
            arr = np.char.decode(arr, "utf-8", errors="ignore")
        except TypeError:
            arr = arr.astype(str)
    if arr.dtype.kind in {"U", "O", "S"}:
        return [clean_text(item) for item in arr.reshape(-1)]
    return [clean_text(item) for item in arr.astype(object).reshape(-1)]


def read_text_var(ds, name: str, key=slice(None), size: int | None = None) -> list[str]:
    if name not in ds.variables:
        return [""] * int(size or 0)
    values = ds.variables[name][key]
    return decode_text_array(values)


def decode_time_axis(ds) -> pd.DatetimeIndex:
    if "time" not in ds.variables or nc4 is None:
        return pd.DatetimeIndex([])
    time_var = ds.variables["time"]
    raw = np.ma.asarray(time_var[:]).astype("float64")
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(np.nan)
    raw = np.asarray(raw, dtype="float64")
    raw = raw[np.isfinite(raw)]
    if raw.size == 0:
        return pd.DatetimeIndex([])
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        dates = nc4.num2date(raw, units=units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        dates = nc4.num2date(raw, units=units, calendar=calendar)
    return pd.DatetimeIndex(pd.to_datetime([str(item) for item in dates]))


def fmt_int(value: object) -> str:
    try:
        return "{:,}".format(int(float(value)))
    except Exception:
        return str(value)


def pct(numerator: int | float, denominator: int | float) -> float:
    denominator = float(denominator or 0)
    if denominator == 0:
        return 0.0
    return round(100.0 * float(numerator) / denominator, 6)


def first_existing(root: Path, names: Iterable[str]) -> Path | None:
    for name in names:
        path = root / name
        if path.is_file():
            return path
    return None


def build_catalog_summaries(release_dir: Path) -> dict[str, pd.DataFrame]:
    station = read_csv(release_dir / "station_catalog.csv")
    source_station = read_csv(release_dir / "source_station_catalog.csv")
    source_dataset = read_csv(release_dir / "source_dataset_catalog.csv")

    catalog_rows = []
    if not station.empty:
        catalog_rows.append(
            {
                "metric": "station_catalog_rows",
                "value": int(len(station)),
                "unit": "rows",
                "source_file": "station_catalog.csv",
            }
        )
        if "cluster_uid" in station.columns:
            catalog_rows.append(
                {
                    "metric": "unique_clusters",
                    "value": int(station["cluster_uid"].map(clean_text).replace("", np.nan).dropna().nunique()),
                    "unit": "clusters",
                    "source_file": "station_catalog.csv",
                }
            )
        if "country" in station.columns:
            catalog_rows.append(
                {
                    "metric": "countries_or_territories",
                    "value": int(station["country"].map(clean_text).replace("", np.nan).dropna().nunique()),
                    "unit": "unique values",
                    "source_file": "station_catalog.csv",
                }
            )
    if not source_station.empty:
        source_station_uids = source_station["source_station_uid"].map(clean_text) if "source_station_uid" in source_station.columns else pd.Series(dtype=object)
        catalog_rows.append(
            {
                "metric": "source_station_catalog_rows",
                "value": int(len(source_station)),
                "unit": "rows",
                "source_file": "source_station_catalog.csv",
            }
        )
        catalog_rows.append(
            {
                "metric": "unique_source_stations",
                "value": int(source_station_uids.replace("", np.nan).dropna().nunique()),
                "unit": "source stations",
                "source_file": "source_station_catalog.csv",
            }
        )
    if not source_dataset.empty:
        catalog_rows.append(
            {
                "metric": "source_dataset_catalog_rows",
                "value": int(len(source_dataset)),
                "unit": "source datasets",
                "source_file": "source_dataset_catalog.csv",
            }
        )

    resolution_rows = []
    if not station.empty and "resolution" in station.columns:
        work = station.copy()
        work["resolution"] = work["resolution"].map(lambda x: clean_text(x).lower())
        for resolution, group in work.groupby("resolution", dropna=False):
            row = {
                "resolution": resolution,
                "catalog_rows": int(len(group)),
                "unique_clusters": int(group["cluster_uid"].map(clean_text).replace("", np.nan).dropna().nunique()) if "cluster_uid" in group.columns else 0,
                "time_start": "",
                "time_end": "",
                "record_count_sum": 0,
                "n_valid_time_steps_sum": 0,
                "lat_min": "",
                "lat_max": "",
                "lon_min": "",
                "lon_max": "",
            }
            if "time_start" in group.columns:
                starts = pd.to_datetime(group["time_start"].replace("", np.nan), errors="coerce").dropna()
                row["time_start"] = starts.min().strftime("%Y-%m-%d") if not starts.empty else ""
            if "time_end" in group.columns:
                ends = pd.to_datetime(group["time_end"].replace("", np.nan), errors="coerce").dropna()
                row["time_end"] = ends.max().strftime("%Y-%m-%d") if not ends.empty else ""
            if "record_count" in group.columns:
                row["record_count_sum"] = int(pd.to_numeric(group["record_count"], errors="coerce").fillna(0).sum())
            if "n_valid_time_steps" in group.columns:
                row["n_valid_time_steps_sum"] = int(pd.to_numeric(group["n_valid_time_steps"], errors="coerce").fillna(0).sum())
            for col in ("lat", "lon"):
                if col in group.columns:
                    vals = pd.to_numeric(group[col], errors="coerce").dropna()
                    if not vals.empty:
                        row[f"{col}_min"] = float(vals.min())
                        row[f"{col}_max"] = float(vals.max())
            resolution_rows.append(row)

    metadata_columns = [
        "reference",
        "data_doi",
        "article_doi",
        "source_url",
        "license_or_terms",
        "access_date",
        "preferred_citation",
        "metadata_status",
    ]
    metadata_rows = []
    if not source_dataset.empty:
        for column in metadata_columns:
            if column in source_dataset.columns:
                values = source_dataset[column].map(clean_text)
                missing = int(values.eq("").sum())
                total = int(len(values))
                metadata_rows.append(
                    {
                        "field": column,
                        "total_source_datasets": total,
                        "nonmissing": total - missing,
                        "missing": missing,
                        "nonmissing_percent": pct(total - missing, total),
                        "missing_source_names": ";".join(
                            source_dataset.loc[values.eq(""), "source_name"].map(clean_text).tolist()
                            if "source_name" in source_dataset.columns
                            else []
                        ),
                    }
                )
            else:
                metadata_rows.append(
                    {
                        "field": column,
                        "total_source_datasets": int(len(source_dataset)),
                        "nonmissing": 0,
                        "missing": int(len(source_dataset)),
                        "nonmissing_percent": 0.0,
                        "missing_source_names": "__column_absent__",
                    }
                )

    source_dataset_rows = []
    if not source_dataset.empty:
        numeric_cols = [
            "n_source_stations",
            "n_clusters",
            "n_source_station_resolution_rows",
            "n_cluster_resolution_rows",
            "n_records",
        ]
        keep_cols = [
            col
            for col in [
                "source_name",
                "source_category",
                "release_role",
                "included_in_minimal_release",
                "available_resolutions",
                "time_start",
                "time_end",
                *numeric_cols,
                "variables_used",
                "variable_treatment",
                "license_or_terms",
                "access_date",
                "preferred_citation",
                "metadata_status",
            ]
            if col in source_dataset.columns
        ]
        source_dataset_rows = source_dataset.loc[:, keep_cols].copy()
        for col in numeric_cols:
            if col in source_dataset_rows.columns:
                source_dataset_rows[col] = pd.to_numeric(source_dataset_rows[col], errors="coerce").fillna(0).astype(int)

    return {
        "catalog_headline": pd.DataFrame(catalog_rows),
        "catalog_by_resolution": pd.DataFrame(resolution_rows),
        "source_dataset_metadata_completeness": pd.DataFrame(metadata_rows),
        "source_dataset_summary": pd.DataFrame(source_dataset_rows),
        "station_catalog": station,
        "source_station_catalog": source_station,
        "source_dataset_catalog": source_dataset,
    }


def source_lookup_from_catalog(source_station: pd.DataFrame) -> dict[tuple[str, str], str]:
    lookup: dict[tuple[str, str], str] = {}
    if source_station.empty or "source_station_uid" not in source_station.columns:
        return lookup
    for _, row in source_station.iterrows():
        uid = clean_text(row.get("source_station_uid", ""))
        if not uid:
            continue
        resolution = clean_text(row.get("resolution", "")).lower()
        source = clean_text(row.get("source_name", ""))
        lookup[(uid, resolution)] = source
        lookup.setdefault((uid, ""), source)
    return lookup


def scan_matrix_product(
    release_dir: Path,
    resolution: str,
    file_name: str,
    source_lookup: dict[tuple[str, str], str],
    chunk_size: int,
) -> dict[str, pd.DataFrame]:
    path = release_dir / file_name
    if not path.is_file():
        base = pd.DataFrame(
            [
                {
                    "resolution": resolution,
                    "file_name": file_name,
                    "file_exists": 0,
                    "n_stations": 0,
                    "n_time": 0,
                    "matrix_cells": 0,
                    "time_start": "",
                    "time_end": "",
                    "record_count_any": 0,
                    "active_clusters_any": 0,
                    "lat_min": "",
                    "lat_max": "",
                    "lon_min": "",
                    "lon_max": "",
                }
            ]
        )
        return {
            "matrix_summary": base,
            "variable_counts": pd.DataFrame(),
            "qc_flag_counts": pd.DataFrame(),
            "source_contribution": pd.DataFrame(),
            "station_record_counts": pd.DataFrame(),
            "time_axis_diagnostics": pd.DataFrame(),
        }
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to scan minimal matrix NetCDF products")

    variable_counts = {
        var: {"n_present": 0, "active_clusters": set(), "n_good": 0, "n_estimated": 0, "n_suspect": 0, "n_bad": 0}
        for var in VARIABLES
    }
    flag_counts = {(var, flag): 0 for var in VARIABLES for flag in FLAG_VALUES}
    source_counts: dict[tuple[str, str, str], dict] = {}
    station_rows = []

    with nc4.Dataset(str(path), "r") as ds:
        n_stations = int(len(ds.dimensions.get("n_stations", [])))
        n_time = int(len(ds.dimensions.get("time", [])))
        matrix_cells = int(n_stations * n_time)
        dates = decode_time_axis(ds)
        time_start = dates.min().strftime("%Y-%m-%d") if len(dates) else ""
        time_end = dates.max().strftime("%Y-%m-%d") if len(dates) else ""
        cluster_uids = read_text_var(ds, "cluster_uid", size=n_stations)
        lat = read_numeric_var(ds, "lat") if "lat" in ds.variables else np.asarray([])
        lon = read_numeric_var(ds, "lon") if "lon" in ds.variables else np.asarray([])

        record_count_any = 0
        active_any_clusters: set[str] = set()

        for start in range(0, n_stations, chunk_size):
            stop = min(start + chunk_size, n_stations)
            key = (slice(start, stop), slice(None))
            row_count = stop - start
            var_masks = {}
            flags_by_var = {}
            for var in VARIABLES:
                values = read_numeric_var(ds, var, key=key)
                if values.size == 0:
                    mask = np.zeros((row_count, n_time), dtype=bool)
                else:
                    mask = valid_mask(values)
                var_masks[var] = mask
                flags = read_flag_var(ds, f"{var}_flag", key, mask.shape)
                flags_by_var[var] = flags
                variable_counts[var]["n_present"] += int(np.count_nonzero(mask))
                for flag in FLAG_VALUES:
                    flag_count = int(np.count_nonzero(mask & (flags == flag)))
                    flag_counts[(var, flag)] += flag_count
                variable_counts[var]["n_good"] += int(np.count_nonzero(mask & (flags == 0)))
                variable_counts[var]["n_estimated"] += int(np.count_nonzero(mask & (flags == 1)))
                variable_counts[var]["n_suspect"] += int(np.count_nonzero(mask & (flags == 2)))
                variable_counts[var]["n_bad"] += int(np.count_nonzero(mask & (flags == 3)))

            any_mask = np.zeros((row_count, n_time), dtype=bool)
            for mask in var_masks.values():
                any_mask |= mask
            record_count_any += int(np.count_nonzero(any_mask))

            source_uids = []
            if "selected_source_station_uid" in ds.variables:
                source_uids = read_text_var(ds, "selected_source_station_uid", key=key, size=row_count * n_time)
            if len(source_uids) != row_count * n_time:
                source_uids = [""] * (row_count * n_time)
            source_uid_arr = np.asarray(source_uids, dtype=object).reshape(row_count, n_time)

            for local_row in range(row_count):
                cluster_uid = cluster_uids[start + local_row] if start + local_row < len(cluster_uids) else str(start + local_row)
                row_any = any_mask[local_row]
                row_any_count = int(np.count_nonzero(row_any))
                if row_any_count:
                    active_any_clusters.add(cluster_uid)
                station_row = {
                    "resolution": resolution,
                    "cluster_uid": cluster_uid,
                    "record_count_any": row_any_count,
                }
                for var in VARIABLES:
                    count = int(np.count_nonzero(var_masks[var][local_row]))
                    station_row[f"record_count_{var}"] = count
                    if count:
                        variable_counts[var]["active_clusters"].add(cluster_uid)
                station_rows.append(station_row)

            # Source contribution is counted over non-empty matrix cells.  If the
            # source uid is unavailable for a cell, the row is kept as unknown so
            # the manuscript-facing totals remain auditable.
            flat_any = any_mask.reshape(-1)
            flat_uids = source_uid_arr.reshape(-1)
            for uid in flat_uids[flat_any]:
                uid = clean_text(uid)
                source = source_lookup.get((uid, resolution), source_lookup.get((uid, ""), "unknown_source" if uid else "missing_source_station_uid"))
                key2 = (resolution, source, uid)
                item = source_counts.setdefault(key2, {"record_count_any": 0, "Q_records": 0, "SSC_records": 0, "SSL_records": 0})
                item["record_count_any"] += 1
            # Add variable-specific source counts without iterating every cell
            # more than necessary.
            for var in VARIABLES:
                flat_var = var_masks[var].reshape(-1)
                for uid in flat_uids[flat_var]:
                    uid = clean_text(uid)
                    source = source_lookup.get((uid, resolution), source_lookup.get((uid, ""), "unknown_source" if uid else "missing_source_station_uid"))
                    key2 = (resolution, source, uid)
                    item = source_counts.setdefault(key2, {"record_count_any": 0, "Q_records": 0, "SSC_records": 0, "SSL_records": 0})
                    item[f"{var}_records"] += 1

        summary = {
            "resolution": resolution,
            "file_name": file_name,
            "file_exists": 1,
            "n_stations": n_stations,
            "n_time": n_time,
            "matrix_cells": matrix_cells,
            "time_start": time_start,
            "time_end": time_end,
            "record_count_any": int(record_count_any),
            "active_clusters_any": int(len(active_any_clusters)),
            "lat_min": float(np.nanmin(lat)) if lat.size and np.isfinite(lat).any() else "",
            "lat_max": float(np.nanmax(lat)) if lat.size and np.isfinite(lat).any() else "",
            "lon_min": float(np.nanmin(lon)) if lon.size and np.isfinite(lon).any() else "",
            "lon_max": float(np.nanmax(lon)) if lon.size and np.isfinite(lon).any() else "",
        }

        if len(dates):
            if resolution == "daily":
                expected = int((dates.max().normalize() - dates.min().normalize()).days) + 1
                unique_periods = int(dates.normalize().nunique())
            elif resolution == "monthly":
                expected = int(len(pd.period_range(dates.min().to_period("M"), dates.max().to_period("M"), freq="M")))
                unique_periods = int(dates.to_period("M").nunique())
            elif resolution == "annual":
                expected = int(dates.year.max() - dates.year.min() + 1)
                unique_periods = int(dates.year.nunique())
            else:
                expected = int(len(dates))
                unique_periods = int(len(dates))
            time_diag = {
                "resolution": resolution,
                "file_name": file_name,
                "n_time": int(len(dates)),
                "time_start": time_start,
                "time_end": time_end,
                "unique_years": int(dates.year.nunique()),
                "unique_year_months": int(dates.to_period("M").nunique()),
                "expected_regular_periods": expected,
                "duplicate_periods": int(len(dates) - unique_periods),
                "axis_interpretation": "regular_period_axis" if len(dates) == expected and len(dates) == unique_periods else "sparse_observation_date_axis",
            }
        else:
            time_diag = {
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

    variable_rows = []
    for var, item in variable_counts.items():
        n_present = int(item["n_present"])
        row = {
            "resolution": resolution,
            "file_name": file_name,
            "variable": var,
            "matrix_cells": matrix_cells,
            "n_present": n_present,
            "n_missing_grid_cells": int(matrix_cells - n_present),
            "active_clusters": int(len(item["active_clusters"])),
            "n_good": int(item["n_good"]),
            "n_estimated": int(item["n_estimated"]),
            "n_suspect": int(item["n_suspect"]),
            "n_bad": int(item["n_bad"]),
            "present_percent_of_grid": pct(n_present, matrix_cells),
            "good_percent_of_present": pct(item["n_good"], n_present),
            "usable_0_1_percent_of_present": pct(item["n_good"] + item["n_estimated"], n_present),
        }
        variable_rows.append(row)

    flag_rows = []
    for (var, flag), count in sorted(flag_counts.items()):
        present = variable_counts[var]["n_present"]
        flag_rows.append(
            {
                "resolution": resolution,
                "file_name": file_name,
                "variable": var,
                "flag": int(flag),
                "flag_meaning": FLAG_MEANINGS.get(flag, ""),
                "count_among_present_values": int(count),
                "percent_among_present_values": pct(count, present),
            }
        )

    source_rows = []
    for (res, source, uid), item in sorted(source_counts.items()):
        source_rows.append(
            {
                "resolution": res,
                "source_name": source,
                "source_station_uid": uid,
                **item,
            }
        )

    return {
        "matrix_summary": pd.DataFrame([summary]),
        "variable_counts": pd.DataFrame(variable_rows),
        "qc_flag_counts": pd.DataFrame(flag_rows),
        "source_contribution": pd.DataFrame(source_rows),
        "station_record_counts": pd.DataFrame(station_rows),
        "time_axis_diagnostics": pd.DataFrame([time_diag]),
    }


def aggregate_source_contribution(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    agg = (
        rows.groupby(["resolution", "source_name"], dropna=False)
        .agg(
            source_station_count=("source_station_uid", lambda s: int(pd.Series(s).map(clean_text).replace("", np.nan).dropna().nunique())),
            record_count_any=("record_count_any", "sum"),
            Q_records=("Q_records", "sum"),
            SSC_records=("SSC_records", "sum"),
            SSL_records=("SSL_records", "sum"),
        )
        .reset_index()
        .sort_values(["resolution", "record_count_any", "source_name"], ascending=[True, False, True], kind="mergesort")
        .reset_index(drop=True)
    )
    return agg


def build_manuscript_numbers(
    catalog_stats: dict[str, pd.DataFrame],
    matrix_summary: pd.DataFrame,
    variable_counts: pd.DataFrame,
    source_contribution: pd.DataFrame,
    validation_summary: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    def add(field: str, value: object, unit: str, source_file: str, method: str, resolution: str = "all", notes: str = ""):
        rows.append(
            {
                "manuscript_field": field,
                "resolution": resolution,
                "value": value,
                "unit": unit,
                "source_file": source_file,
                "source_column_or_method": method,
                "notes": notes,
            }
        )

    headline = catalog_stats.get("catalog_headline", pd.DataFrame())
    if not headline.empty:
        for _, r in headline.iterrows():
            add(str(r["metric"]), r["value"], str(r["unit"]), str(r["source_file"]), "catalog summary")

    by_res = catalog_stats.get("catalog_by_resolution", pd.DataFrame())
    if not by_res.empty:
        for _, r in by_res.iterrows():
            res = clean_text(r["resolution"])
            add("clusters_by_resolution", int(r.get("unique_clusters", 0)), "clusters", "station_catalog.csv", "unique cluster_uid", res)
            add("catalog_record_count_by_resolution", int(r.get("record_count_sum", 0)), "records", "station_catalog.csv", "sum(record_count)", res)
            if clean_text(r.get("time_start", "")):
                add("time_start_by_resolution", r.get("time_start", ""), "date", "station_catalog.csv", "min(time_start)", res)
            if clean_text(r.get("time_end", "")):
                add("time_end_by_resolution", r.get("time_end", ""), "date", "station_catalog.csv", "max(time_end)", res)

    if not matrix_summary.empty:
        for _, r in matrix_summary.iterrows():
            res = clean_text(r["resolution"])
            add("matrix_stations_by_resolution", int(r.get("n_stations", 0)), "stations/clusters", str(r.get("file_name", "")), "n_stations dimension", res)
            add("matrix_time_steps_by_resolution", int(r.get("n_time", 0)), "time steps", str(r.get("file_name", "")), "time dimension", res)
            add("matrix_nonempty_cells_by_resolution", int(r.get("record_count_any", 0)), "station-time cells", str(r.get("file_name", "")), "union of non-missing Q/SSC/SSL cells", res)
            if clean_text(r.get("time_start", "")):
                add("matrix_time_start_by_resolution", r.get("time_start", ""), "date", str(r.get("file_name", "")), "decoded time axis min", res)
            if clean_text(r.get("time_end", "")):
                add("matrix_time_end_by_resolution", r.get("time_end", ""), "date", str(r.get("file_name", "")), "decoded time axis max", res)

    if not variable_counts.empty:
        total_by_var = variable_counts.groupby("variable", dropna=False)["n_present"].sum().reset_index()
        for _, r in total_by_var.iterrows():
            add(f"{r['variable']}_nonmissing_cells_total", int(r["n_present"]), "station-time cells", "daily/monthly/annual NetCDF", "sum non-missing values across minimal matrices")
        for _, r in variable_counts.iterrows():
            add(f"{r['variable']}_nonmissing_cells_by_resolution", int(r["n_present"]), "station-time cells", str(r["file_name"]), "non-missing matrix values", clean_text(r["resolution"]))
            add(f"{r['variable']}_good_values_by_resolution", int(r["n_good"]), "station-time cells", str(r["file_name"]), "non-missing values with flag 0", clean_text(r["resolution"]))
            add(f"{r['variable']}_estimated_values_by_resolution", int(r["n_estimated"]), "station-time cells", str(r["file_name"]), "non-missing values with flag 1", clean_text(r["resolution"]))

    if not source_contribution.empty:
        total_sources = int(source_contribution["source_name"].map(clean_text).replace("", np.nan).dropna().nunique())
        total_source_stations = int(source_contribution["source_station_count"].sum()) if "source_station_count" in source_contribution.columns else 0
        add("source_datasets_used_in_minimal_cells", total_sources, "source datasets", "selected_source_station_uid + source_station_catalog.csv", "unique source_name with selected cells")
        add("source_station_resolution_links_used_in_minimal_cells", total_source_stations, "source-station resolution links", "selected_source_station_uid + source_station_catalog.csv", "sum unique selected source_station_uid by source/resolution")

    if not validation_summary.empty:
        fail_count = int(pd.to_numeric(validation_summary.get("fail", 0), errors="coerce").fillna(0).sum()) if "fail" in validation_summary.columns else 0
        add("minimal_validation_fail_count", fail_count, "checks", "minimal_release_validation_report.csv", "status count")

    return pd.DataFrame(rows)


def build_validation_summary(release_dir: Path) -> pd.DataFrame:
    path = release_dir / "minimal_release_validation_report.csv"
    if not path.is_file():
        return pd.DataFrame([{"status": "missing_report", "count": 1}])
    frame = pd.read_csv(path, keep_default_na=False)
    if "status" not in frame.columns:
        return pd.DataFrame([{"status": "malformed_report", "count": len(frame)}])
    counts = frame.groupby("status", dropna=False).size().reset_index(name="count")
    return counts.sort_values("status").reset_index(drop=True)


def markdown_table(frame: pd.DataFrame, columns: list[str], max_rows: int = 20) -> str:
    if frame.empty:
        return "_No rows._"
    work = frame.loc[:, [col for col in columns if col in frame.columns]].head(max_rows).copy()
    if work.empty:
        return "_No displayable columns._"
    lines = []
    header = "| " + " | ".join(work.columns) + " |"
    sep = "| " + " | ".join(["---"] * len(work.columns)) + " |"
    lines.extend([header, sep])
    for _, row in work.iterrows():
        lines.append("| " + " | ".join(clean_text(row.get(col, "")) for col in work.columns) + " |")
    return "\n".join(lines)


def build_report(
    release_dir: Path,
    out_dir: Path,
    run_started: str,
    run_finished: str,
    release_fp: str,
    script_fp: str,
    catalog_stats: dict[str, pd.DataFrame],
    matrix_summary: pd.DataFrame,
    variable_counts: pd.DataFrame,
    source_contribution: pd.DataFrame,
    metadata_completeness: pd.DataFrame,
    validation_summary: pd.DataFrame,
) -> list[str]:
    lines = [
        "# Minimal Release Statistics Report",
        "",
        "## Run identity",
        "",
        f"- Minimal release directory: `{release_dir}`",
        f"- Output directory: `{out_dir}`",
        f"- Run started UTC: `{run_started}`",
        f"- Run finished UTC: `{run_finished}`",
        f"- Release fingerprint: `{release_fp}`",
        f"- Stats script fingerprint: `{script_fp}`",
        "",
        "## Scope",
        "",
        "This report reads only the minimal release package. It does not read the full S8 master product, climatology product, satellite product, GPKG sidecars, overlap candidates, or upstream pipeline intermediates.",
        "",
        "## Catalog headline",
        "",
        markdown_table(catalog_stats.get("catalog_headline", pd.DataFrame()), ["metric", "value", "unit", "source_file"], max_rows=20),
        "",
        "## Resolution summary",
        "",
        markdown_table(
            catalog_stats.get("catalog_by_resolution", pd.DataFrame()),
            ["resolution", "unique_clusters", "record_count_sum", "n_valid_time_steps_sum", "time_start", "time_end", "lat_min", "lat_max", "lon_min", "lon_max"],
            max_rows=10,
        ),
        "",
        "## Matrix summary",
        "",
        markdown_table(
            matrix_summary,
            ["resolution", "n_stations", "n_time", "record_count_any", "active_clusters_any", "time_start", "time_end", "lat_min", "lat_max", "lon_min", "lon_max"],
            max_rows=10,
        ),
        "",
        "## Variable counts",
        "",
        markdown_table(
            variable_counts,
            ["resolution", "variable", "n_present", "active_clusters", "n_good", "n_estimated", "n_suspect", "n_bad", "present_percent_of_grid", "good_percent_of_present", "usable_0_1_percent_of_present"],
            max_rows=30,
        ),
        "",
        "## Source contribution",
        "",
        markdown_table(
            source_contribution,
            ["resolution", "source_name", "source_station_count", "record_count_any", "Q_records", "SSC_records", "SSL_records"],
            max_rows=30,
        ),
        "",
        "## Source metadata completeness",
        "",
        markdown_table(
            metadata_completeness,
            ["field", "total_source_datasets", "nonmissing", "missing", "nonmissing_percent"],
            max_rows=20,
        ),
        "",
        "## Minimal validation status",
        "",
        markdown_table(validation_summary, ["status", "count"], max_rows=10),
        "",
        "## Manuscript update guidance",
        "",
        "- Use `manuscript_numbers.csv` as the primary table for replacing manuscript placeholders.",
        "- Use `tables/table_minimal_matrix_summary.csv` for station/time/cell counts by resolution.",
        "- Use `tables/table_minimal_variable_counts.csv` and `tables/table_minimal_qc_flag_counts.csv` for Q/SSC/SSL and quality-flag numbers.",
        "- Use `tables/table_minimal_source_dataset_metadata_completeness.csv` to check DOI, license, access date, and citation completeness before submission.",
    ]
    return lines


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run manuscript-facing statistics for sed_reference_release_minimal.")
    parser.add_argument(
        "--release-dir",
        default="scripts_basin_test/output/sed_reference_release_minimal",
        help="Path to sed_reference_release_minimal.",
    )
    parser.add_argument(
        "--out-dir",
        default="output_other/stats_release_minimal",
        help="Output directory for minimal-release statistics.",
    )
    parser.add_argument("--chunk-size", type=int, default=128, help="Station-row chunk size for matrix scans.")
    parser.add_argument("--copy-reports", action="store_true", help="Copy Markdown report to docs/reports/.")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if nc4 is None:
        print("[fail] netCDF4 is required to run minimal release statistics", file=sys.stderr)
        return 1

    release_dir = Path(args.release_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    tables_dir = out_dir / "tables"
    out_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    run_started = utc_now()
    print(f"[config] minimal release dir: {release_dir}")
    print(f"[config] output dir:          {out_dir}")
    print(f"[config] chunk size:          {args.chunk_size}")

    missing_required = [name for name in (*MINIMAL_MATRIX_PRODUCTS.values(), *MINIMAL_CATALOGS) if not (release_dir / name).is_file()]
    if missing_required:
        for name in missing_required:
            print(f"[fail] missing required minimal release file: {release_dir / name}", file=sys.stderr)
        return 1

    release_fp = metadata_fingerprint(release_dir)
    script_fp = script_fingerprint()

    catalog_stats = build_catalog_summaries(release_dir)
    source_lookup = source_lookup_from_catalog(catalog_stats["source_station_catalog"])

    matrix_results = []
    for resolution, file_name in MINIMAL_MATRIX_PRODUCTS.items():
        print(f"[scan] {resolution}: {file_name}", flush=True)
        matrix_results.append(scan_matrix_product(release_dir, resolution, file_name, source_lookup, max(1, int(args.chunk_size))))

    matrix_summary = pd.concat([item["matrix_summary"] for item in matrix_results], ignore_index=True)
    variable_counts = pd.concat([item["variable_counts"] for item in matrix_results], ignore_index=True)
    qc_flag_counts = pd.concat([item["qc_flag_counts"] for item in matrix_results], ignore_index=True)
    station_record_counts = pd.concat([item["station_record_counts"] for item in matrix_results], ignore_index=True)
    source_station_contribution = pd.concat([item["source_contribution"] for item in matrix_results], ignore_index=True)
    source_contribution = aggregate_source_contribution(source_station_contribution)
    time_axis_diagnostics = pd.concat([item["time_axis_diagnostics"] for item in matrix_results], ignore_index=True)
    validation_summary = build_validation_summary(release_dir)

    manuscript_numbers = build_manuscript_numbers(
        catalog_stats,
        matrix_summary,
        variable_counts,
        source_contribution,
        validation_summary,
    )

    outputs = {}
    outputs["catalog_headline"] = write_csv(catalog_stats["catalog_headline"], tables_dir / "table_minimal_catalog_headline.csv")
    outputs["catalog_by_resolution"] = write_csv(catalog_stats["catalog_by_resolution"], tables_dir / "table_minimal_catalog_by_resolution.csv")
    outputs["source_dataset_summary"] = write_csv(catalog_stats["source_dataset_summary"], tables_dir / "table_minimal_source_dataset_summary.csv")
    outputs["metadata_completeness"] = write_csv(
        catalog_stats["source_dataset_metadata_completeness"],
        tables_dir / "table_minimal_source_dataset_metadata_completeness.csv",
    )
    outputs["matrix_summary"] = write_csv(matrix_summary, tables_dir / "table_minimal_matrix_summary.csv")
    outputs["variable_counts"] = write_csv(variable_counts, tables_dir / "table_minimal_variable_counts.csv")
    outputs["qc_flag_counts"] = write_csv(qc_flag_counts, tables_dir / "table_minimal_qc_flag_counts.csv")
    outputs["station_record_counts"] = write_csv(station_record_counts, tables_dir / "table_minimal_station_record_counts.csv")
    outputs["source_station_contribution"] = write_csv(source_station_contribution, tables_dir / "table_minimal_source_station_contribution.csv")
    outputs["source_contribution"] = write_csv(source_contribution, tables_dir / "table_minimal_source_contribution.csv")
    outputs["time_axis_diagnostics"] = write_csv(time_axis_diagnostics, tables_dir / "table_minimal_time_axis_diagnostics.csv")
    outputs["validation_summary"] = write_csv(validation_summary, tables_dir / "table_minimal_validation_summary.csv")
    outputs["manuscript_numbers"] = write_csv(manuscript_numbers, out_dir / "manuscript_numbers.csv")

    run_finished = utc_now()
    manifest_df = pd.DataFrame(
        [
            {
                "name": key,
                "relative_path": path.relative_to(out_dir).as_posix(),
                "size_bytes": int(path.stat().st_size),
                "release_fingerprint": release_fp,
                "stats_script_fingerprint": script_fp,
                "run_started_utc": run_started,
                "run_finished_utc": run_finished,
            }
            for key, path in sorted(outputs.items())
        ]
    )
    write_csv(manifest_df, out_dir / "run_manifest.csv")
    write_json(
        {
            "run_started_utc": run_started,
            "run_finished_utc": run_finished,
            "minimal_release_dir": str(release_dir),
            "out_dir": str(out_dir),
            "release_fingerprint": release_fp,
            "stats_script_fingerprint": script_fp,
            "outputs": manifest_df.to_dict(orient="records"),
        },
        out_dir / "run_manifest.json",
    )

    report_path = write_markdown(
        build_report(
            release_dir,
            out_dir,
            run_started,
            run_finished,
            release_fp,
            script_fp,
            catalog_stats,
            matrix_summary,
            variable_counts,
            source_contribution,
            catalog_stats["source_dataset_metadata_completeness"],
            validation_summary,
        ),
        out_dir / "minimal_release_stats_report.md",
    )
    if args.copy_reports:
        docs_dir = Path(__file__).resolve().parents[1] / "docs" / "reports"
        docs_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(report_path), str(docs_dir / report_path.name))

    failures = []
    if not validation_summary.empty and "status" in validation_summary.columns and "count" in validation_summary.columns:
        fail_rows = validation_summary[validation_summary["status"].astype(str).str.lower().eq("fail")]
        fail_count = int(pd.to_numeric(fail_rows["count"], errors="coerce").fillna(0).sum()) if not fail_rows.empty else 0
        if fail_count:
            failures.append(f"minimal validation has {fail_count} failing checks")

    if failures:
        for item in failures:
            print(f"[warn] {item}")
    print(f"[done] wrote minimal release statistics to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
