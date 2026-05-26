#!/usr/bin/env python3
"""
Build temporal-coverage tables and figures for the sediment reference dataset.

Purpose
-------
This script summarizes the final daily / monthly / annual matrix products and
standalone climatology product for manuscript Section 4.2. It reports temporal
ranges, active clusters/stations by year, per-cluster/station record lengths,
coverage ratios, and long-record counts.

Default inputs
--------------
The script first looks for release products under
``scripts_basin_test/output/sed_reference_release`` and falls back to s6 pipeline
outputs when release files are not present:

  - daily:       sed_reference_timeseries_daily.nc or s6_basin_matrix_daily.nc
  - monthly:     sed_reference_timeseries_monthly.nc or s6_basin_matrix_monthly.nc
  - annual:      sed_reference_timeseries_annual.nc or s6_basin_matrix_annual.nc
  - climatology: sed_reference_climatology.nc or s6_climatology_only.nc

Default outputs
---------------
Relative output paths are resolved under the repository/script root:

  - tables/table_temporal_coverage_by_resolution.csv
  - tables/table_active_clusters_by_year.csv
  - tables/table_record_length_distribution.csv
  - tables/table_temporal_coverage_record_lengths_by_unit.csv
  - figures/fig_active_clusters_by_year.png
  - figures/fig_record_length_histogram.png

Notes on units
--------------
For daily / monthly / annual products, the statistical unit is a basin cluster.
For climatology, the statistical unit is a standalone climatology station, since
climatology is intentionally exported outside the basin-cluster mainline.
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
    from netCDF4 import num2date

    HAS_NC4 = True
except ImportError:  # pragma: no cover - handled at runtime
    nc4 = None
    num2date = None
    HAS_NC4 = False


SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_ROOT = SCRIPT_DIR.parent

# Support both layouts:
#   1) repo_root/stats/temporal_coverage_stats.py
#   2) Output_r/scripts_basin_test/stats/temporal_coverage_stats.py
# In layout (2), shared modules such as qc_contract.py may live under Output_r.
for import_root in (SCRIPT_ROOT, SCRIPT_ROOT.parent):
    path_text = str(import_root)
    if path_text not in sys.path:
        sys.path.insert(0, path_text)

from pipeline_paths import (  # noqa: E402
    RELEASE_CLIMATOLOGY_NC,
    RELEASE_MATRIX_ANNUAL_NC,
    RELEASE_MATRIX_DAILY_NC,
    RELEASE_MATRIX_MONTHLY_NC,
    RELEASE_DATASET_DIR,
    S6_CLIMATOLOGY_NC,
    S6_MATRIX_DIR,
    get_output_r_root,
)
from qc_contract import Q_VAR_NAMES, SSC_VAR_NAMES, SSL_VAR_NAMES, TIME_VAR_NAMES  # noqa: E402


PROJECT_ROOT = get_output_r_root(SCRIPT_ROOT)

RESOLUTION_ORDER = ("daily", "monthly", "annual", "climatology")
MATRIX_RESOLUTIONS = ("daily", "monthly", "annual")
CORE_VARS = ("Q", "SSC", "SSL")
LONG_YEAR_THRESHOLDS = (10, 20, 30, 50, 100)
FILL_FLOAT_VALUES = (-9999.0, -9999)
DAYS_PER_YEAR = 365.25
MONTHS_PER_YEAR = 12.0

RELEASE_PATHS = {
    "daily": PROJECT_ROOT / RELEASE_MATRIX_DAILY_NC,
    "monthly": PROJECT_ROOT / RELEASE_MATRIX_MONTHLY_NC,
    "annual": PROJECT_ROOT / RELEASE_MATRIX_ANNUAL_NC,
    "climatology": PROJECT_ROOT / RELEASE_CLIMATOLOGY_NC,
}
S6_PATHS = {
    "daily": PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_daily.nc",
    "monthly": PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_monthly.nc",
    "annual": PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_annual.nc",
    "climatology": PROJECT_ROOT / S6_CLIMATOLOGY_NC,
}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _resolve_under_script_root(path_value: str) -> Path:
    path = Path(path_value).expanduser()
    if path.is_absolute():
        return path
    return (SCRIPT_ROOT / path).resolve()


def _date_text(value) -> str:
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return ""
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d")


def _year_value(value):
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return ""
    if pd.isna(ts):
        return ""
    return int(ts.year)


def _format_float(value, digits: int = 6):
    if value is None:
        return np.nan
    try:
        if pd.isna(value):
            return np.nan
        return round(float(value), digits)
    except Exception:
        return np.nan


def _first_existing_var(nc, names: Sequence[str]) -> Optional[str]:
    for name in names:
        if name in nc.variables:
            return name
    return None


def _var_fill_values(var) -> Tuple[float, ...]:
    values: List[float] = list(FILL_FLOAT_VALUES)
    for attr_name in ("_FillValue", "missing_value"):
        try:
            raw = getattr(var, attr_name)
        except Exception:
            continue
        try:
            arr = np.asarray(raw).reshape(-1)
        except Exception:
            arr = np.asarray([raw])
        for item in arr:
            try:
                val = float(item)
            except Exception:
                continue
            if math.isfinite(val) and val not in values:
                values.append(val)
    return tuple(values)


def _valid_float_mask(values, fill_values: Iterable[float]) -> np.ndarray:
    """Return True where a numeric data chunk is a real, non-fill observation."""
    masked = np.ma.asarray(values)
    if np.ma.isMaskedArray(masked):
        valid = ~np.ma.getmaskarray(masked)
        data = masked.filled(np.nan)
    else:
        data = np.asarray(values)
        valid = np.ones(data.shape, dtype=bool)

    try:
        data = np.asarray(data, dtype=np.float64)
    except Exception:
        return np.zeros(np.asarray(values).shape, dtype=bool)

    valid &= np.isfinite(data)
    for fill in fill_values:
        try:
            valid &= data != float(fill)
        except Exception:
            pass
    return valid


def _read_text_1d(nc, names: Sequence[str], size: int) -> List[str]:
    name = _first_existing_var(nc, names)
    if name is None:
        return [""] * int(size)
    try:
        values = nc.variables[name][:]
        arr = np.asarray(values, dtype=object).reshape(-1)
    except Exception:
        return [""] * int(size)

    out = []
    for item in arr[:size]:
        if isinstance(item, bytes):
            text = item.decode("utf-8", errors="ignore")
        else:
            text = str(item)
        if text.lower() in ("nan", "none"):
            text = ""
        out.append(text.strip())
    if len(out) < size:
        out.extend([""] * (size - len(out)))
    return out


def _read_numeric_1d(nc, names: Sequence[str], size: int, default=np.nan) -> np.ndarray:
    name = _first_existing_var(nc, names)
    if name is None:
        return np.full(int(size), default)
    try:
        values = np.ma.asarray(nc.variables[name][:]).filled(default)
        arr = np.asarray(values).reshape(-1)
    except Exception:
        return np.full(int(size), default)
    if len(arr) >= size:
        return arr[:size]
    return np.concatenate([arr, np.full(size - len(arr), default)])


def _decode_time(nc, preferred_names: Sequence[str] = TIME_VAR_NAMES) -> pd.DatetimeIndex:
    time_name = _first_existing_var(nc, preferred_names)
    if time_name is None:
        raise ValueError("No time variable found; checked {}".format(", ".join(preferred_names)))

    var = nc.variables[time_name]
    raw_values = np.asarray(var[:]).reshape(-1)
    units = getattr(var, "units", "days since 1970-01-01")
    calendar = getattr(var, "calendar", "gregorian")

    decoded = None
    if num2date is not None:
        try:
            decoded = num2date(
                raw_values,
                units=units,
                calendar=calendar,
                only_use_cftime_datetimes=False,
            )
        except TypeError:
            try:
                decoded = num2date(raw_values, units=units, calendar=calendar)
            except Exception:
                decoded = None
        except Exception:
            decoded = None

    if decoded is None:
        try:
            return pd.DatetimeIndex(pd.to_datetime(raw_values, unit="D", origin="1970-01-01", errors="coerce"))
        except Exception:
            return pd.DatetimeIndex(pd.to_datetime(raw_values, errors="coerce"))

    try:
        return pd.DatetimeIndex(pd.to_datetime(decoded, errors="coerce"))
    except Exception:
        converted = []
        for item in decoded:
            if hasattr(item, "isoformat"):
                converted.append(item.isoformat())
            elif hasattr(item, "year") and hasattr(item, "month") and hasattr(item, "day"):
                converted.append("{:04d}-{:02d}-{:02d}".format(item.year, item.month, item.day))
            else:
                converted.append(str(item))
        return pd.DatetimeIndex(pd.to_datetime(converted, errors="coerce"))


def _month_span_inclusive(first: pd.Timestamp, last: pd.Timestamp) -> int:
    return int((last.year - first.year) * 12 + (last.month - first.month) + 1)


def _span_lengths(first, last, resolution: str) -> Dict[str, float]:
    try:
        start = pd.Timestamp(first)
        end = pd.Timestamp(last)
    except Exception:
        return {
            "record_length_steps": 0,
            "record_length_days": np.nan,
            "record_length_months": np.nan,
            "record_length_years": np.nan,
        }
    if pd.isna(start) or pd.isna(end) or end < start:
        return {
            "record_length_steps": 0,
            "record_length_days": np.nan,
            "record_length_months": np.nan,
            "record_length_years": np.nan,
        }

    days = int((end.normalize() - start.normalize()).days) + 1
    months = max(1, _month_span_inclusive(start, end))
    years = max(1, int(end.year - start.year + 1))

    if resolution == "daily":
        return {
            "record_length_steps": int(days),
            "record_length_days": float(days),
            "record_length_months": float(days / (DAYS_PER_YEAR / MONTHS_PER_YEAR)),
            "record_length_years": float(days / DAYS_PER_YEAR),
        }
    if resolution == "monthly":
        return {
            "record_length_steps": int(months),
            "record_length_days": float((months / MONTHS_PER_YEAR) * DAYS_PER_YEAR),
            "record_length_months": float(months),
            "record_length_years": float(months / MONTHS_PER_YEAR),
        }
    if resolution == "annual":
        return {
            "record_length_steps": int(years),
            "record_length_days": float(years * DAYS_PER_YEAR),
            "record_length_months": float(years * MONTHS_PER_YEAR),
            "record_length_years": float(years),
        }

    # Climatology is not necessarily a regular time series. We report the
    # calendar span implied by its station metadata or record dates.
    return {
        "record_length_steps": int(days),
        "record_length_days": float(days),
        "record_length_months": float(days / (DAYS_PER_YEAR / MONTHS_PER_YEAR)),
        "record_length_years": float(days / DAYS_PER_YEAR),
    }


def _unit_row(
    resolution: str,
    unit_type: str,
    unit_id: str,
    cluster_id,
    first_date,
    last_date,
    valid_any: int,
    valid_q: int,
    valid_ssc: int,
    valid_ssl: int,
) -> Dict[str, object]:
    lengths = _span_lengths(first_date, last_date, resolution)
    denominator = float(lengths.get("record_length_steps", 0) or 0)
    coverage_ratio = (float(valid_any) / denominator) if denominator > 0 else np.nan
    return {
        "resolution": resolution,
        "unit_type": unit_type,
        "unit_id": str(unit_id),
        "cluster_id": "" if cluster_id is None or pd.isna(cluster_id) else cluster_id,
        "first_date": _date_text(first_date),
        "last_date": _date_text(last_date),
        "first_year": _year_value(first_date),
        "last_year": _year_value(last_date),
        "valid_observations_any": int(valid_any),
        "valid_Q": int(valid_q),
        "valid_SSC": int(valid_ssc),
        "valid_SSL": int(valid_ssl),
        "record_length_steps": int(lengths["record_length_steps"]),
        "record_length_days": _format_float(lengths["record_length_days"]),
        "record_length_months": _format_float(lengths["record_length_months"]),
        "record_length_years": _format_float(lengths["record_length_years"]),
        "temporal_coverage_ratio": _format_float(coverage_ratio),
    }


# ---------------------------------------------------------------------------
# Input path resolution
# ---------------------------------------------------------------------------


def _candidate_paths_from_input_dir(input_dir: Path, resolution: str) -> List[Path]:
    if resolution == "climatology":
        return [
            input_dir / "sed_reference_climatology.nc",
            input_dir / "s6_climatology_only.nc",
        ]
    return [
        input_dir / "sed_reference_timeseries_{}.nc".format(resolution),
        input_dir / "s6_basin_matrix_{}.nc".format(resolution),
    ]


def resolve_input_paths(args) -> Dict[str, Path]:
    paths: Dict[str, Path] = {}
    requested = [res for res in args.resolutions if res in RESOLUTION_ORDER]

    if args.input_dir:
        input_dir = Path(args.input_dir).expanduser().resolve()
        for resolution in requested:
            for candidate in _candidate_paths_from_input_dir(input_dir, resolution):
                if candidate.is_file():
                    paths[resolution] = candidate
                    break
        return paths

    fallback = not args.no_fallback_to_s6
    for resolution in requested:
        release_path = RELEASE_PATHS[resolution]
        s6_path = S6_PATHS[resolution]
        if release_path.is_file():
            paths[resolution] = release_path
        elif fallback and s6_path.is_file():
            paths[resolution] = s6_path
    return paths


# ---------------------------------------------------------------------------
# Matrix resolution reader: daily / monthly / annual
# ---------------------------------------------------------------------------


def _matrix_unit_ids(nc, n_units: int) -> Tuple[List[str], np.ndarray]:
    cluster_uids = _read_text_1d(nc, ["cluster_uid"], n_units)
    cluster_ids = _read_numeric_1d(nc, ["cluster_id"], n_units, default=np.nan)
    unit_ids: List[str] = []
    for idx in range(n_units):
        uid = cluster_uids[idx].strip() if idx < len(cluster_uids) else ""
        if uid:
            unit_ids.append(uid)
            continue
        cid = cluster_ids[idx] if idx < len(cluster_ids) else np.nan
        if pd.notna(cid):
            try:
                unit_ids.append("SED{:06d}".format(int(cid)))
            except Exception:
                unit_ids.append(str(cid))
        else:
            unit_ids.append("unit_{:06d}".format(idx))
    return unit_ids, cluster_ids


def read_matrix_resolution(
    path: Path,
    resolution: str,
    chunk_rows: int = 64,
) -> Tuple[Dict[str, object], pd.DataFrame, pd.DataFrame]:
    with nc4.Dataset(path, "r") as nc:
        dates = _decode_time(nc)
        if len(dates) == 0:
            raise ValueError("{} has an empty time axis".format(path))
        n_time = int(len(dates))
        if "n_stations" in nc.dimensions:
            n_units = int(len(nc.dimensions["n_stations"]))
        else:
            # Fall back to the first core variable shape if needed.
            q_name = _first_existing_var(nc, Q_VAR_NAMES)
            if q_name is None:
                raise ValueError("{} has no n_stations dimension or Q variable".format(path))
            n_units = int(nc.variables[q_name].shape[0])

        unit_ids, cluster_ids = _matrix_unit_ids(nc, n_units)
        var_names = {
            "Q": _first_existing_var(nc, Q_VAR_NAMES),
            "SSC": _first_existing_var(nc, SSC_VAR_NAMES),
            "SSL": _first_existing_var(nc, SSL_VAR_NAMES),
        }
        fill_values = {
            key: _var_fill_values(nc.variables[name]) if name else tuple(FILL_FLOAT_VALUES)
            for key, name in var_names.items()
        }

        q_counts = np.zeros(n_units, dtype=np.int64)
        ssc_counts = np.zeros(n_units, dtype=np.int64)
        ssl_counts = np.zeros(n_units, dtype=np.int64)
        any_counts = np.zeros(n_units, dtype=np.int64)
        first_idx = np.full(n_units, -1, dtype=np.int64)
        last_idx = np.full(n_units, -1, dtype=np.int64)

        years = pd.Series(dates).dt.year.to_numpy()
        valid_years = sorted(int(y) for y in pd.Series(years).dropna().unique())
        year_col_idx = {year: np.where(years == year)[0] for year in valid_years}
        by_year = {
            year: {
                "year": int(year),
                "resolution": resolution,
                "unit_type": "cluster",
                "active_units": 0,
                "records_any": 0,
                "records_Q": 0,
                "records_SSC": 0,
                "records_SSL": 0,
            }
            for year in valid_years
        }

        records_q = 0
        records_ssc = 0
        records_ssl = 0
        records_any = 0

        for start in range(0, n_units, max(1, int(chunk_rows))):
            stop = min(start + max(1, int(chunk_rows)), n_units)
            shape = (stop - start, n_time)
            valid_masks: Dict[str, np.ndarray] = {}
            for key, name in var_names.items():
                if name is None:
                    valid_masks[key] = np.zeros(shape, dtype=bool)
                    continue
                values = nc.variables[name][start:stop, :]
                valid_masks[key] = _valid_float_mask(values, fill_values[key])

            q_mask = valid_masks["Q"]
            ssc_mask = valid_masks["SSC"]
            ssl_mask = valid_masks["SSL"]
            any_mask = q_mask | ssc_mask | ssl_mask

            local_q = q_mask.sum(axis=1).astype(np.int64)
            local_ssc = ssc_mask.sum(axis=1).astype(np.int64)
            local_ssl = ssl_mask.sum(axis=1).astype(np.int64)
            local_any = any_mask.sum(axis=1).astype(np.int64)

            q_counts[start:stop] = local_q
            ssc_counts[start:stop] = local_ssc
            ssl_counts[start:stop] = local_ssl
            any_counts[start:stop] = local_any

            records_q += int(local_q.sum())
            records_ssc += int(local_ssc.sum())
            records_ssl += int(local_ssl.sum())
            records_any += int(local_any.sum())

            active_local = local_any > 0
            if np.any(active_local):
                local_first = np.argmax(any_mask, axis=1)
                local_last = n_time - 1 - np.argmax(any_mask[:, ::-1], axis=1)
                idx = np.where(active_local)[0]
                first_idx[start + idx] = local_first[idx]
                last_idx[start + idx] = local_last[idx]

            for year, cols in year_col_idx.items():
                if cols.size == 0:
                    continue
                y_q = q_mask[:, cols]
                y_ssc = ssc_mask[:, cols]
                y_ssl = ssl_mask[:, cols]
                y_any = y_q | y_ssc | y_ssl
                row = by_year[year]
                row["active_units"] += int(np.count_nonzero(y_any.any(axis=1)))
                row["records_any"] += int(np.count_nonzero(y_any))
                row["records_Q"] += int(np.count_nonzero(y_q))
                row["records_SSC"] += int(np.count_nonzero(y_ssc))
                row["records_SSL"] += int(np.count_nonzero(y_ssl))

    active = any_counts > 0
    unit_rows = []
    for idx in np.where(active)[0]:
        unit_rows.append(
            _unit_row(
                resolution=resolution,
                unit_type="cluster",
                unit_id=unit_ids[idx],
                cluster_id=cluster_ids[idx] if idx < len(cluster_ids) else "",
                first_date=dates[first_idx[idx]],
                last_date=dates[last_idx[idx]],
                valid_any=int(any_counts[idx]),
                valid_q=int(q_counts[idx]),
                valid_ssc=int(ssc_counts[idx]),
                valid_ssl=int(ssl_counts[idx]),
            )
        )

    unit_df = pd.DataFrame(unit_rows)
    if len(unit_df) > 0:
        first_date = pd.to_datetime(unit_df["first_date"], errors="coerce").min()
        last_date = pd.to_datetime(unit_df["last_date"], errors="coerce").max()
    else:
        first_date = pd.NaT
        last_date = pd.NaT

    summary = {
        "resolution": resolution,
        "unit_type": "cluster",
        "source_file": str(path),
        "first_date": _date_text(first_date),
        "last_date": _date_text(last_date),
        "first_year": _year_value(first_date),
        "last_year": _year_value(last_date),
        "time_steps": int(n_time),
        "active_units": int(np.count_nonzero(active)),
        "records_any": int(records_any),
        "records_Q": int(records_q),
        "records_SSC": int(records_ssc),
        "records_SSL": int(records_ssl),
    }
    by_year_df = pd.DataFrame([by_year[year] for year in valid_years])
    return summary, by_year_df, unit_df


# ---------------------------------------------------------------------------
# Climatology reader
# ---------------------------------------------------------------------------


def _climatology_station_ids(nc, n_stations: int) -> List[str]:
    station_uids = _read_text_1d(nc, ["station_uid"], n_stations)
    out = []
    for idx, uid in enumerate(station_uids):
        uid = str(uid or "").strip()
        out.append(uid if uid else "CLM{:06d}".format(idx))
    return out


def _read_1d_valid_var(nc, names: Sequence[str], n_records: int) -> np.ndarray:
    name = _first_existing_var(nc, names)
    if name is None:
        return np.zeros(int(n_records), dtype=bool)
    var = nc.variables[name]
    values = var[:]
    mask = _valid_float_mask(values, _var_fill_values(var)).reshape(-1)
    if len(mask) >= n_records:
        return mask[:n_records]
    return np.concatenate([mask, np.zeros(n_records - len(mask), dtype=bool)])


def _parse_optional_date(text: str):
    try:
        ts = pd.Timestamp(str(text).strip())
    except Exception:
        return pd.NaT
    if pd.isna(ts):
        return pd.NaT
    return ts


def read_climatology(path: Path) -> Tuple[Dict[str, object], pd.DataFrame, pd.DataFrame]:
    with nc4.Dataset(path, "r") as nc:
        if "n_records" in nc.dimensions:
            n_records = int(len(nc.dimensions["n_records"]))
        elif "time" in nc.dimensions:
            n_records = int(len(nc.dimensions["time"]))
        else:
            raise ValueError("{} has no n_records/time dimension".format(path))

        if "n_stations" in nc.dimensions:
            n_stations = int(len(nc.dimensions["n_stations"]))
        else:
            station_index_probe = _read_numeric_1d(nc, ["station_index"], n_records, default=-1)
            n_stations = int(np.nanmax(station_index_probe)) + 1 if len(station_index_probe) else 0

        dates = _decode_time(nc)
        if len(dates) >= n_records:
            dates = dates[:n_records]
        elif len(dates) < n_records:
            pad = pd.DatetimeIndex([pd.NaT] * (n_records - len(dates)))
            dates = dates.append(pad)

        station_index = _read_numeric_1d(nc, ["station_index"], n_records, default=-1).astype(np.int64, copy=False)
        station_ids = _climatology_station_ids(nc, n_stations)
        declared_start_text = _read_text_1d(nc, ["source_station_time_coverage_start", "time_coverage_start"], n_stations)
        declared_end_text = _read_text_1d(nc, ["source_station_time_coverage_end", "time_coverage_end"], n_stations)

        q_mask = _read_1d_valid_var(nc, Q_VAR_NAMES, n_records)
        ssc_mask = _read_1d_valid_var(nc, SSC_VAR_NAMES, n_records)
        ssl_mask = _read_1d_valid_var(nc, SSL_VAR_NAMES, n_records)
        any_mask = q_mask | ssc_mask | ssl_mask

    records_df = pd.DataFrame(
        {
            "station_index": station_index,
            "time": pd.DatetimeIndex(dates),
            "year": pd.Series(dates).dt.year.to_numpy(),
            "valid_Q": q_mask,
            "valid_SSC": ssc_mask,
            "valid_SSL": ssl_mask,
            "valid_any": any_mask,
        }
    )
    records_df = records_df[(records_df["station_index"] >= 0) & (records_df["station_index"] < n_stations)].copy()

    valid_records = records_df[records_df["valid_any"]].copy()
    if len(valid_records) > 0:
        first_valid = valid_records.groupby("station_index")["time"].min()
        last_valid = valid_records.groupby("station_index")["time"].max()
        any_counts = valid_records.groupby("station_index").size()
    else:
        first_valid = pd.Series(dtype="datetime64[ns]")
        last_valid = pd.Series(dtype="datetime64[ns]")
        any_counts = pd.Series(dtype="int64")

    q_counts = records_df[records_df["valid_Q"]].groupby("station_index").size()
    ssc_counts = records_df[records_df["valid_SSC"]].groupby("station_index").size()
    ssl_counts = records_df[records_df["valid_SSL"]].groupby("station_index").size()

    unit_rows = []
    for idx in sorted(any_counts.index.tolist()):
        declared_start = _parse_optional_date(declared_start_text[int(idx)] if int(idx) < len(declared_start_text) else "")
        declared_end = _parse_optional_date(declared_end_text[int(idx)] if int(idx) < len(declared_end_text) else "")
        start = declared_start if pd.notna(declared_start) else first_valid.get(idx, pd.NaT)
        end = declared_end if pd.notna(declared_end) else last_valid.get(idx, pd.NaT)
        # If one side of declared coverage is malformed, fall back to the observed bound.
        if pd.isna(start):
            start = first_valid.get(idx, pd.NaT)
        if pd.isna(end):
            end = last_valid.get(idx, pd.NaT)
        unit_rows.append(
            _unit_row(
                resolution="climatology",
                unit_type="climatology_station",
                unit_id=station_ids[int(idx)] if int(idx) < len(station_ids) else "CLM{:06d}".format(int(idx)),
                cluster_id="",
                first_date=start,
                last_date=end,
                valid_any=int(any_counts.get(idx, 0)),
                valid_q=int(q_counts.get(idx, 0)),
                valid_ssc=int(ssc_counts.get(idx, 0)),
                valid_ssl=int(ssl_counts.get(idx, 0)),
            )
        )

    unit_df = pd.DataFrame(unit_rows)

    year_rows = []
    if len(records_df) > 0:
        valid_years = sorted(int(y) for y in records_df["year"].dropna().unique())
        for year in valid_years:
            sub = records_df[records_df["year"] == year]
            sub_any = sub[sub["valid_any"]]
            year_rows.append(
                {
                    "year": int(year),
                    "resolution": "climatology",
                    "unit_type": "climatology_station",
                    "active_units": int(sub_any["station_index"].nunique()),
                    "records_any": int(sub["valid_any"].sum()),
                    "records_Q": int(sub["valid_Q"].sum()),
                    "records_SSC": int(sub["valid_SSC"].sum()),
                    "records_SSL": int(sub["valid_SSL"].sum()),
                }
            )
    by_year_df = pd.DataFrame(year_rows)

    if len(unit_df) > 0:
        first_date = pd.to_datetime(unit_df["first_date"], errors="coerce").min()
        last_date = pd.to_datetime(unit_df["last_date"], errors="coerce").max()
    else:
        first_date = pd.NaT
        last_date = pd.NaT

    summary = {
        "resolution": "climatology",
        "unit_type": "climatology_station",
        "source_file": str(path),
        "first_date": _date_text(first_date),
        "last_date": _date_text(last_date),
        "first_year": _year_value(first_date),
        "last_year": _year_value(last_date),
        "time_steps": int(records_df["time"].dropna().nunique()) if len(records_df) else 0,
        "active_units": int(len(unit_df)),
        "records_any": int(records_df["valid_any"].sum()) if len(records_df) else 0,
        "records_Q": int(records_df["valid_Q"].sum()) if len(records_df) else 0,
        "records_SSC": int(records_df["valid_SSC"].sum()) if len(records_df) else 0,
        "records_SSL": int(records_df["valid_SSL"].sum()) if len(records_df) else 0,
    }
    return summary, by_year_df, unit_df


# ---------------------------------------------------------------------------
# Summaries and figures
# ---------------------------------------------------------------------------


def _safe_quantile(values: pd.Series, q: float):
    values = pd.to_numeric(values, errors="coerce").dropna()
    if len(values) == 0:
        return np.nan
    return float(values.quantile(q))


def add_length_summary_columns(summary_rows: List[Dict[str, object]], unit_df: pd.DataFrame) -> List[Dict[str, object]]:
    if len(unit_df) == 0:
        return summary_rows

    unit_by_resolution = {res: sub.copy() for res, sub in unit_df.groupby("resolution", sort=False)}
    out_rows = []
    for row in summary_rows:
        resolution = row["resolution"]
        sub = unit_by_resolution.get(resolution, pd.DataFrame())
        if len(sub) == 0:
            metrics = {
                "mean_record_length_steps": np.nan,
                "median_record_length_steps": np.nan,
                "max_record_length_steps": np.nan,
                "mean_record_length_years": np.nan,
                "median_record_length_years": np.nan,
                "max_record_length_years": np.nan,
                "mean_temporal_coverage_ratio": np.nan,
                "median_temporal_coverage_ratio": np.nan,
            }
            long_counts = {"n_gt_{}_years".format(th): 0 for th in LONG_YEAR_THRESHOLDS}
        else:
            steps = pd.to_numeric(sub["record_length_steps"], errors="coerce")
            years = pd.to_numeric(sub["record_length_years"], errors="coerce")
            cov = pd.to_numeric(sub["temporal_coverage_ratio"], errors="coerce")
            metrics = {
                "mean_record_length_steps": _format_float(steps.mean()),
                "median_record_length_steps": _format_float(steps.median()),
                "max_record_length_steps": _format_float(steps.max()),
                "mean_record_length_years": _format_float(years.mean()),
                "median_record_length_years": _format_float(years.median()),
                "max_record_length_years": _format_float(years.max()),
                "mean_temporal_coverage_ratio": _format_float(cov.mean()),
                "median_temporal_coverage_ratio": _format_float(cov.median()),
            }
            long_counts = {
                "n_gt_{}_years".format(th): int(np.count_nonzero(years > th))
                for th in LONG_YEAR_THRESHOLDS
            }
        new_row = dict(row)
        new_row.update(metrics)
        new_row.update(long_counts)
        out_rows.append(new_row)
    return out_rows


def build_record_length_distribution(unit_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if len(unit_df) == 0:
        return pd.DataFrame(rows)

    for resolution in RESOLUTION_ORDER:
        sub = unit_df[unit_df["resolution"] == resolution].copy()
        if len(sub) == 0:
            continue
        unit_type = str(sub["unit_type"].iloc[0])
        steps = pd.to_numeric(sub["record_length_steps"], errors="coerce")
        years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        row = {
            "resolution": resolution,
            "unit_type": unit_type,
            "n_units": int(len(sub)),
            "min_steps": _format_float(steps.min()),
            "p05_steps": _format_float(_safe_quantile(steps, 0.05)),
            "p25_steps": _format_float(_safe_quantile(steps, 0.25)),
            "median_steps": _format_float(steps.median()),
            "mean_steps": _format_float(steps.mean()),
            "p75_steps": _format_float(_safe_quantile(steps, 0.75)),
            "p95_steps": _format_float(_safe_quantile(steps, 0.95)),
            "max_steps": _format_float(steps.max()),
            "min_years_equiv": _format_float(years.min()),
            "p05_years_equiv": _format_float(_safe_quantile(years, 0.05)),
            "p25_years_equiv": _format_float(_safe_quantile(years, 0.25)),
            "median_years_equiv": _format_float(years.median()),
            "mean_years_equiv": _format_float(years.mean()),
            "p75_years_equiv": _format_float(_safe_quantile(years, 0.75)),
            "p95_years_equiv": _format_float(_safe_quantile(years, 0.95)),
            "max_years_equiv": _format_float(years.max()),
        }
        for threshold in LONG_YEAR_THRESHOLDS:
            row["n_gt_{}_years".format(threshold)] = int(np.count_nonzero(years > threshold))
        rows.append(row)
    return pd.DataFrame(rows)


def _ordered_concat(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    frames = [df for df in frames if df is not None and len(df) > 0]
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "resolution" in out.columns:
        order = {name: idx for idx, name in enumerate(RESOLUTION_ORDER)}
        out["_resolution_order"] = out["resolution"].map(order).fillna(999).astype(int)
        sort_cols = ["_resolution_order"]
        if "year" in out.columns:
            sort_cols = ["year", "_resolution_order"]
        elif "unit_id" in out.columns:
            sort_cols = ["_resolution_order", "unit_id"]
        out = out.sort_values(sort_cols).drop(columns=["_resolution_order"]).reset_index(drop=True)
    return out


def write_tables(
    tables_dir: Path,
    summary_rows: List[Dict[str, object]],
    by_year_df: pd.DataFrame,
    unit_df: pd.DataFrame,
) -> Dict[str, Path]:
    tables_dir.mkdir(parents=True, exist_ok=True)
    summary_with_lengths = add_length_summary_columns(summary_rows, unit_df)
    summary_df = pd.DataFrame(summary_with_lengths)
    distribution_df = build_record_length_distribution(unit_df)

    outputs = {
        "temporal_coverage_by_resolution": tables_dir / "table_temporal_coverage_by_resolution.csv",
        "active_clusters_by_year": tables_dir / "table_active_clusters_by_year.csv",
        "record_length_distribution": tables_dir / "table_record_length_distribution.csv",
        "record_lengths_by_unit": tables_dir / "table_temporal_coverage_record_lengths_by_unit.csv",
    }
    summary_df.to_csv(outputs["temporal_coverage_by_resolution"], index=False)
    by_year_df.to_csv(outputs["active_clusters_by_year"], index=False)
    distribution_df.to_csv(outputs["record_length_distribution"], index=False)
    unit_df.to_csv(outputs["record_lengths_by_unit"], index=False)
    return outputs


def plot_active_clusters_by_year(by_year_df: pd.DataFrame, out_path: Path, include_climatology: bool = False) -> None:
    if len(by_year_df) == 0:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_df = by_year_df.copy()
    if not include_climatology:
        plot_df = plot_df[plot_df["resolution"] != "climatology"].copy()
    if len(plot_df) == 0:
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7.2, 4.2))
    for resolution in RESOLUTION_ORDER:
        sub = plot_df[plot_df["resolution"] == resolution].sort_values("year")
        if len(sub) == 0:
            continue
        plt.plot(sub["year"], sub["active_units"], linewidth=1.8, label=resolution)
    plt.xlabel("Year")
    plt.ylabel("Active clusters/stations")
    plt.title("Active temporal coverage by year")
    plt.legend(frameon=False)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()


def plot_record_length_histogram(unit_df: pd.DataFrame, out_path: Path) -> None:
    if len(unit_df) == 0:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    values_all = pd.to_numeric(unit_df["record_length_years"], errors="coerce").dropna()
    values_all = values_all[values_all >= 0]
    if len(values_all) == 0:
        return

    max_years = float(values_all.max())
    if max_years <= 0:
        bins = np.linspace(0, 1, 11)
    else:
        bins = np.linspace(0, max_years, min(50, max(12, int(math.ceil(max_years)) + 1)))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7.2, 4.2))
    for resolution in RESOLUTION_ORDER:
        sub = unit_df[unit_df["resolution"] == resolution]
        vals = pd.to_numeric(sub["record_length_years"], errors="coerce").dropna()
        vals = vals[vals >= 0]
        if len(vals) == 0:
            continue
        plt.hist(vals, bins=bins, histtype="step", linewidth=1.8, label=resolution)
    plt.xlabel("Record length (years equivalent)")
    plt.ylabel("Number of clusters/stations")
    plt.title("Record length distribution")
    plt.legend(frameon=False)
    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close()


def write_figures(
    figures_dir: Path,
    by_year_df: pd.DataFrame,
    unit_df: pd.DataFrame,
    include_climatology_in_yearly_plot: bool,
) -> Dict[str, Path]:
    figures_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "active_clusters_by_year": figures_dir / "fig_active_clusters_by_year.png",
        "record_length_histogram": figures_dir / "fig_record_length_histogram.png",
    }
    plot_active_clusters_by_year(
        by_year_df,
        outputs["active_clusters_by_year"],
        include_climatology=include_climatology_in_yearly_plot,
    )
    plot_record_length_histogram(unit_df, outputs["record_length_histogram"])
    return outputs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None):
    ap = argparse.ArgumentParser(
        description="Summarize temporal coverage and record lengths for Section 4.2 tables/figures."
    )
    ap.add_argument(
        "--input-dir",
        default="",
        help=(
            "Optional directory containing release or s6 NetCDF files. If omitted, the script "
            "uses pipeline_paths release paths and then s6 fallback paths."
        ),
    )
    ap.add_argument(
        "--resolutions",
        nargs="+",
        default=list(RESOLUTION_ORDER),
        choices=list(RESOLUTION_ORDER),
        help="Resolutions to summarize; default: daily monthly annual climatology.",
    )
    ap.add_argument(
        "--tables-dir",
        default="output_other/temporal_coverage_stats/tables",
        help="Output table directory. Relative paths are resolved under the script/repository root.",
    )
    ap.add_argument(
        "--figures-dir",
        default="output_other/temporal_coverage_stats/figures",
        help="Output figure directory. Relative paths are resolved under the script/repository root.",
    )
    ap.add_argument(
        "--chunk-rows",
        type=int,
        default=64,
        help="Number of station/cluster rows read per chunk for matrix NetCDF files.",
    )
    ap.add_argument(
        "--no-fallback-to-s6",
        action="store_true",
        help="Only use release products when --input-dir is not supplied; do not fall back to s6 outputs.",
    )
    ap.add_argument(
        "--include-climatology-in-yearly-plot",
        action="store_true",
        help="Include standalone climatology stations in fig_active_clusters_by_year.png.",
    )
    ap.add_argument(
        "--skip-figures",
        action="store_true",
        help="Write CSV tables only and skip PNG figure creation.",
    )
    ap.add_argument(
        "--strict-inputs",
        action="store_true",
        help="Fail if any requested resolution file is missing. By default missing resolutions are skipped.",
    )
    return ap.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if not HAS_NC4:
        print("Error: netCDF4 is required. Install it with `pip install netCDF4`.", file=sys.stderr)
        return 1

    requested = [res for res in args.resolutions if res in RESOLUTION_ORDER]
    input_paths = resolve_input_paths(args)
    missing = [res for res in requested if res not in input_paths]
    if missing:
        message = "Missing requested resolution files: {}".format(", ".join(missing))
        if args.strict_inputs:
            print("Error: " + message, file=sys.stderr)
            return 1
        print("Warning: {}; those resolutions will be skipped.".format(message), file=sys.stderr)

    if not input_paths:
        print("Error: no input NetCDF files found.", file=sys.stderr)
        print("Checked release directory: {}".format(PROJECT_ROOT / RELEASE_DATASET_DIR), file=sys.stderr)
        print("Checked s6 matrix directory: {}".format(PROJECT_ROOT / S6_MATRIX_DIR), file=sys.stderr)
        return 1

    print("Temporal coverage inputs:")
    for resolution in RESOLUTION_ORDER:
        if resolution in input_paths:
            print("  {}: {}".format(resolution, input_paths[resolution]))

    summary_rows: List[Dict[str, object]] = []
    by_year_frames: List[pd.DataFrame] = []
    unit_frames: List[pd.DataFrame] = []

    for resolution in RESOLUTION_ORDER:
        path = input_paths.get(resolution)
        if path is None:
            continue
        print("Reading {}: {}".format(resolution, path))
        if resolution in MATRIX_RESOLUTIONS:
            summary, by_year, unit_df = read_matrix_resolution(path, resolution, chunk_rows=args.chunk_rows)
        else:
            summary, by_year, unit_df = read_climatology(path)
        summary_rows.append(summary)
        by_year_frames.append(by_year)
        unit_frames.append(unit_df)
        print(
            "  active_units={active_units} records_any={records_any} first={first_date} last={last_date}".format(
                **summary
            )
        )

    by_year_df = _ordered_concat(by_year_frames)
    unit_df = _ordered_concat(unit_frames)

    tables_dir = _resolve_under_script_root(args.tables_dir)
    table_outputs = write_tables(tables_dir, summary_rows, by_year_df, unit_df)

    print("Wrote tables:")
    for path in table_outputs.values():
        print("  {}".format(path))

    if not args.skip_figures:
        figures_dir = _resolve_under_script_root(args.figures_dir)
        try:
            figure_outputs = write_figures(
                figures_dir,
                by_year_df,
                unit_df,
                include_climatology_in_yearly_plot=args.include_climatology_in_yearly_plot,
            )
            print("Wrote figures:")
            for path in figure_outputs.values():
                print("  {}".format(path))
        except ImportError as exc:
            print("Error: matplotlib is required to write figures: {}".format(exc), file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
