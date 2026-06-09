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
import shutil
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
    RELEASE_SATELLITE_CATALOG_CSV,
    RELEASE_SATELLITE_NC,
    S6_CLIMATOLOGY_NC,
    S6_MATRIX_DIR,
    get_output_r_root,
)
from qc_contract import Q_VAR_NAMES, SSC_VAR_NAMES, SSL_VAR_NAMES, TIME_VAR_NAMES  # noqa: E402


PROJECT_ROOT = get_output_r_root(SCRIPT_ROOT)

RESOLUTION_ORDER = ("daily", "monthly", "annual", "climatology")
MATRIX_RESOLUTIONS = ("daily", "monthly", "annual")
SATELLITE_PRODUCT = "satellite_validation"
SATELLITE_OUTPUT_RESOLUTION = "satellite_daily"
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
RELEASE_SATELLITE_PATH = PROJECT_ROOT / RELEASE_SATELLITE_NC
RELEASE_SATELLITE_CATALOG_PATH = PROJECT_ROOT / RELEASE_SATELLITE_CATALOG_CSV
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


def _period_key_values(dates: pd.DatetimeIndex) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    series = pd.Series(pd.DatetimeIndex(dates))
    day_keys = series.dt.strftime("%Y-%m-%d").fillna("").to_numpy()
    month_keys = series.dt.strftime("%Y-%m").fillna("").to_numpy()
    year_keys = series.dt.year.fillna(-9999).astype(int).to_numpy()
    return day_keys, month_keys, year_keys


def _unique_observed_periods(mask: np.ndarray, day_keys, month_keys, year_keys) -> Tuple[int, int, int]:
    mask = np.asarray(mask, dtype=bool)
    if mask.size == 0 or not np.any(mask):
        return 0, 0, 0
    days = np.asarray(day_keys)[mask]
    months = np.asarray(month_keys)[mask]
    years = np.asarray(year_keys)[mask]
    return (
        int(len(np.unique(days[days != ""]))),
        int(len(np.unique(months[months != ""]))),
        int(len(np.unique(years[years >= 0]))),
    )


def _calendar_span_periods(lengths: Dict[str, float], resolution: str) -> float:
    if resolution == "daily":
        return float(lengths.get("record_length_days", np.nan))
    if resolution == "monthly":
        return float(lengths.get("record_length_months", np.nan))
    if resolution == "annual":
        return float(lengths.get("record_length_years", np.nan))
    return float(lengths.get("record_length_days", np.nan))


def _observed_period_count(unique_days: int, unique_months: int, unique_years: int, resolution: str) -> int:
    if resolution == "daily":
        return int(unique_days)
    if resolution == "monthly":
        return int(unique_months)
    if resolution == "annual":
        return int(unique_years)
    return int(unique_days)


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
    unique_days: int,
    unique_months: int,
    unique_years: int,
) -> Dict[str, object]:
    lengths = _span_lengths(first_date, last_date, resolution)
    calendar_periods = _calendar_span_periods(lengths, resolution)
    observed_periods = _observed_period_count(unique_days, unique_months, unique_years, resolution)
    nominal_coverage_ratio = (
        (float(valid_any) / float(lengths.get("record_length_steps", 0)))
        if resolution == "daily" and float(lengths.get("record_length_steps", 0) or 0) > 0
        else np.nan
    )
    observed_period_density = (
        min(1.0, float(observed_periods) / float(calendar_periods))
        if calendar_periods and calendar_periods > 0
        else np.nan
    )
    years = float(lengths.get("record_length_years", np.nan))
    record_frequency_per_year = (float(valid_any) / years) if years and years > 0 else np.nan
    return {
        "resolution": resolution,
        "unit_type": unit_type,
        "unit_id": str(unit_id),
        "cluster_id": "" if cluster_id is None or pd.isna(cluster_id) else cluster_id,
        "first_date": _date_text(first_date),
        "last_date": _date_text(last_date),
        "first_year": _year_value(first_date),
        "last_year": _year_value(last_date),
        "record_count_any": int(valid_any),
        "record_count_Q": int(valid_q),
        "record_count_SSC": int(valid_ssc),
        "record_count_SSL": int(valid_ssl),
        "unique_observed_days": int(unique_days),
        "unique_observed_months": int(unique_months),
        "unique_observed_years": int(unique_years),
        "record_length_steps": int(lengths["record_length_steps"]),
        "calendar_span_days": _format_float(lengths["record_length_days"]),
        "calendar_span_months": _format_float(lengths["record_length_months"]),
        "calendar_span_years": _format_float(lengths["record_length_years"]),
        "record_length_days": _format_float(lengths["record_length_days"]),
        "record_length_months": _format_float(lengths["record_length_months"]),
        "record_length_years": _format_float(lengths["record_length_years"]),
        "nominal_coverage_ratio": _format_float(nominal_coverage_ratio),
        "observed_period_density": _format_float(observed_period_density),
        "temporal_coverage_ratio": _format_float(
            nominal_coverage_ratio if resolution == "daily" else observed_period_density
        ),
        "record_frequency_per_year": _format_float(record_frequency_per_year),
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
        unique_day_counts = np.zeros(n_units, dtype=np.int64)
        unique_month_counts = np.zeros(n_units, dtype=np.int64)
        unique_year_counts = np.zeros(n_units, dtype=np.int64)
        first_idx = np.full(n_units, -1, dtype=np.int64)
        last_idx = np.full(n_units, -1, dtype=np.int64)

        years = pd.Series(dates).dt.year.to_numpy()
        day_keys, month_keys, year_keys = _period_key_values(dates)
        valid_years = sorted(int(y) for y in pd.Series(years).dropna().unique())
        year_col_idx = {year: np.where(years == year)[0] for year in valid_years}
        by_year = {
            year: {
                "year": int(year),
                "resolution": resolution,
                "unit_type": "cluster",
                "active_units": 0,
                "active_units_Q": 0,
                "active_units_SSC": 0,
                "active_units_SSL": 0,
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
                for local_idx in idx:
                    unique_days, unique_months, unique_years = _unique_observed_periods(
                        any_mask[local_idx, :],
                        day_keys,
                        month_keys,
                        year_keys,
                    )
                    global_idx = start + local_idx
                    unique_day_counts[global_idx] = unique_days
                    unique_month_counts[global_idx] = unique_months
                    unique_year_counts[global_idx] = unique_years

            for year, cols in year_col_idx.items():
                if cols.size == 0:
                    continue
                y_q = q_mask[:, cols]
                y_ssc = ssc_mask[:, cols]
                y_ssl = ssl_mask[:, cols]
                y_any = y_q | y_ssc | y_ssl
                row = by_year[year]
                row["active_units"] += int(np.count_nonzero(y_any.any(axis=1)))
                row["active_units_Q"] += int(np.count_nonzero(y_q.any(axis=1)))
                row["active_units_SSC"] += int(np.count_nonzero(y_ssc.any(axis=1)))
                row["active_units_SSL"] += int(np.count_nonzero(y_ssl.any(axis=1)))
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
                unique_days=int(unique_day_counts[idx]),
                unique_months=int(unique_month_counts[idx]),
                unique_years=int(unique_year_counts[idx]),
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
        "active_units_Q": int(np.count_nonzero(q_counts > 0)),
        "active_units_SSC": int(np.count_nonzero(ssc_counts > 0)),
        "active_units_SSL": int(np.count_nonzero(ssl_counts > 0)),
        "record_count_any": int(records_any),
        "record_count_Q": int(records_q),
        "record_count_SSC": int(records_ssc),
        "record_count_SSL": int(records_ssl),
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
        station_valid = valid_records[valid_records["station_index"] == idx]
        unique_days = unique_months = unique_years = 0
        if len(station_valid) > 0:
            day_keys, month_keys, year_keys = _period_key_values(pd.DatetimeIndex(station_valid["time"]))
            unique_days, unique_months, unique_years = _unique_observed_periods(
                np.ones(len(station_valid), dtype=bool),
                day_keys,
                month_keys,
                year_keys,
            )
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
                unique_days=int(unique_days),
                unique_months=int(unique_months),
                unique_years=int(unique_years),
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
                    "active_units_Q": int(sub[sub["valid_Q"]]["station_index"].nunique()),
                    "active_units_SSC": int(sub[sub["valid_SSC"]]["station_index"].nunique()),
                    "active_units_SSL": int(sub[sub["valid_SSL"]]["station_index"].nunique()),
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
        "active_units_Q": int(records_df[records_df["valid_Q"]]["station_index"].nunique()) if len(records_df) else 0,
        "active_units_SSC": int(records_df[records_df["valid_SSC"]]["station_index"].nunique()) if len(records_df) else 0,
        "active_units_SSL": int(records_df[records_df["valid_SSL"]]["station_index"].nunique()) if len(records_df) else 0,
        "record_count_any": int(records_df["valid_any"].sum()) if len(records_df) else 0,
        "record_count_Q": int(records_df["valid_Q"].sum()) if len(records_df) else 0,
        "record_count_SSC": int(records_df["valid_SSC"].sum()) if len(records_df) else 0,
        "record_count_SSL": int(records_df["valid_SSL"].sum()) if len(records_df) else 0,
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
                "mean_nominal_coverage_ratio": np.nan,
                "median_nominal_coverage_ratio": np.nan,
                "mean_observed_period_density": np.nan,
                "median_observed_period_density": np.nan,
                "mean_record_frequency_per_year": np.nan,
                "median_record_frequency_per_year": np.nan,
                "mean_temporal_coverage_ratio": np.nan,
                "median_temporal_coverage_ratio": np.nan,
            }
            long_counts = {"n_gt_{}_years".format(th): 0 for th in LONG_YEAR_THRESHOLDS}
        else:
            steps = pd.to_numeric(sub["record_length_steps"], errors="coerce")
            years = pd.to_numeric(sub["record_length_years"], errors="coerce")
            cov = pd.to_numeric(sub["temporal_coverage_ratio"], errors="coerce")
            density = pd.to_numeric(sub.get("observed_period_density", pd.Series(dtype="float64")), errors="coerce")
            nominal = pd.to_numeric(sub.get("nominal_coverage_ratio", pd.Series(dtype="float64")), errors="coerce")
            frequency = pd.to_numeric(sub.get("record_frequency_per_year", pd.Series(dtype="float64")), errors="coerce")
            metrics = {
                "mean_record_length_steps": _format_float(steps.mean()),
                "median_record_length_steps": _format_float(steps.median()),
                "max_record_length_steps": _format_float(steps.max()),
                "mean_record_length_years": _format_float(years.mean()),
                "median_record_length_years": _format_float(years.median()),
                "max_record_length_years": _format_float(years.max()),
                "mean_nominal_coverage_ratio": _format_float(nominal.mean()),
                "median_nominal_coverage_ratio": _format_float(nominal.median()),
                "mean_observed_period_density": _format_float(density.mean()),
                "median_observed_period_density": _format_float(density.median()),
                "mean_record_frequency_per_year": _format_float(frequency.mean()),
                "median_record_frequency_per_year": _format_float(frequency.median()),
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

    for resolution in _resolution_sequence(unit_df["resolution"].unique()):
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
        order = {name: idx for idx, name in enumerate(RESOLUTION_ORDER + (SATELLITE_OUTPUT_RESOLUTION,))}
        out["_resolution_order"] = out["resolution"].map(order).fillna(999).astype(int)
        sort_cols = ["_resolution_order"]
        if "year" in out.columns:
            sort_cols = ["year", "_resolution_order"]
        elif "unit_id" in out.columns:
            sort_cols = ["_resolution_order", "unit_id"]
        out = out.sort_values(sort_cols).drop(columns=["_resolution_order"]).reset_index(drop=True)
    return out


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in ("nan", "none", "null", "<na>") else text


def _read_release_csv(file_name: str) -> pd.DataFrame:
    path = PROJECT_ROOT / RELEASE_DATASET_DIR / file_name
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def _resolution_sequence(values: Iterable[str]) -> List[str]:
    order = list(RESOLUTION_ORDER) + [SATELLITE_OUTPUT_RESOLUTION]
    present = [_clean_text(v) for v in values if _clean_text(v)]
    out = [res for res in order if res in present]
    for res in sorted(set(present)):
        if res not in out:
            out.append(res)
    return out


def _record_span_years(start_value, end_value, resolution: str) -> float:
    lengths = _span_lengths(pd.to_datetime(start_value, errors="coerce"), pd.to_datetime(end_value, errors="coerce"), resolution)
    return _format_float(lengths.get("record_length_years"))


def build_temporal_coverage_by_variable(by_year_df: pd.DataFrame, summary_rows: List[Dict[str, object]]) -> pd.DataFrame:
    rows = []
    if len(by_year_df) == 0:
        return pd.DataFrame(rows)

    summary_by_resolution = dict((row["resolution"], row) for row in summary_rows)
    for resolution in RESOLUTION_ORDER:
        sub = by_year_df[by_year_df["resolution"] == resolution].copy()
        if len(sub) == 0:
            continue
        summary = summary_by_resolution.get(resolution, {})
        for variable in CORE_VARS:
            record_col = "records_{}".format(variable)
            active_col = "active_units_{}".format(variable)
            if record_col not in sub.columns:
                continue
            active_series = pd.to_numeric(
                sub[active_col] if active_col in sub.columns else sub[record_col] > 0,
                errors="coerce",
            ).fillna(0)
            record_series = pd.to_numeric(sub[record_col], errors="coerce").fillna(0)
            observed = sub[record_series > 0].copy()
            if len(observed) > 0:
                peak_idx = active_series.idxmax()
                first_year = int(observed["year"].min())
                last_year = int(observed["year"].max())
                peak_year = int(sub.loc[peak_idx, "year"])
                peak_active = int(active_series.loc[peak_idx])
            else:
                first_year = ""
                last_year = ""
                peak_year = ""
                peak_active = 0
            rows.append(
                {
                    "resolution": resolution,
                    "unit_type": summary.get("unit_type", ""),
                    "variable": variable,
                    "first_year": first_year,
                    "last_year": last_year,
                    "active_units": _safe_int_value(summary.get(active_col, 0)),
                    "record_count": _safe_int_value(summary.get("record_count_{}".format(variable), summary.get(record_col, 0))),
                    "peak_active_units": peak_active,
                    "peak_active_year": peak_year,
                    "peak_records": int(record_series.max()) if len(record_series) else 0,
                    "peak_record_year": int(sub.loc[record_series.idxmax(), "year"]) if len(record_series) else "",
                }
            )
    return pd.DataFrame(rows)


def build_long_records_by_resolution(unit_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if len(unit_df) == 0:
        return pd.DataFrame(rows)
    for resolution in _resolution_sequence(unit_df["resolution"].unique()):
        sub = unit_df[unit_df["resolution"] == resolution].copy()
        if len(sub) == 0:
            continue
        years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        row = {
            "resolution": resolution,
            "unit_type": str(sub["unit_type"].iloc[0]),
            "n_units": int(len(sub)),
            "median_record_length_years": _format_float(years.median()),
            "max_record_length_years": _format_float(years.max()),
        }
        for threshold in LONG_YEAR_THRESHOLDS:
            gt = sub[years > threshold]
            row["n_gt_{}_years".format(threshold)] = int(len(gt))
            row["pct_gt_{}_years".format(threshold)] = _format_float((100.0 * len(gt) / len(sub)) if len(sub) else np.nan, 3)
        rows.append(row)
    return pd.DataFrame(rows)


def build_temporal_coverage_by_source() -> pd.DataFrame:
    source_station = _read_release_csv("source_station_catalog.csv")
    if len(source_station) == 0:
        return pd.DataFrame()
    dataset = _read_release_csv("source_dataset_catalog.csv")

    work = source_station.copy()
    for col in ("source_name", "source_long_name", "resolution", "source_station_uid", "cluster_uid"):
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].map(_clean_text)
    if "n_records" not in work.columns:
        work["n_records"] = 0
    work["n_records"] = pd.to_numeric(work["n_records"], errors="coerce").fillna(0).astype(int)
    work["time_start_dt"] = pd.to_datetime(work.get("time_start", ""), errors="coerce")
    work["time_end_dt"] = pd.to_datetime(work.get("time_end", ""), errors="coerce")
    work["record_length_years"] = [
        _record_span_years(start, end, resolution)
        for start, end, resolution in zip(work["time_start_dt"], work["time_end_dt"], work["resolution"])
    ]

    rows = []
    group_cols = ["source_name", "resolution"]
    for (source_name, resolution), sub in work.groupby(group_cols, sort=True):
        first_date = sub["time_start_dt"].min()
        last_date = sub["time_end_dt"].max()
        years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        rows.append(
            {
                "source_name": source_name,
                "resolution": resolution,
                "source_long_name": _clean_text(sub["source_long_name"].iloc[0]) if "source_long_name" in sub.columns else "",
                "first_date": _date_text(first_date),
                "last_date": _date_text(last_date),
                "first_year": _year_value(first_date),
                "last_year": _year_value(last_date),
                "source_station_rows": int(len(sub)),
                "source_stations": int(sub["source_station_uid"].replace("", np.nan).dropna().nunique()),
                "clusters": int(sub["cluster_uid"].replace("", np.nan).dropna().nunique()),
                "record_count": int(sub["n_records"].sum()),
                "median_record_length_years": _format_float(years.median()),
                "max_record_length_years": _format_float(years.max()),
                "n_gt_10_years": int(np.count_nonzero(years > 10)),
                "n_gt_20_years": int(np.count_nonzero(years > 20)),
                "n_gt_30_years": int(np.count_nonzero(years > 30)),
                "n_gt_50_years": int(np.count_nonzero(years > 50)),
                "n_gt_100_years": int(np.count_nonzero(years > 100)),
            }
        )
    out = pd.DataFrame(rows)
    if len(dataset) and "source_name" in dataset.columns:
        keep_cols = [
            col
            for col in ("source_name", "institution", "available_resolutions", "geographic_coverage")
            if col in dataset.columns
        ]
        out = out.merge(dataset[keep_cols].drop_duplicates("source_name"), on="source_name", how="left")
    return out.sort_values(["resolution", "record_count", "source_name"], ascending=[True, False, True]).reset_index(drop=True)


def build_temporal_coverage_by_region_resolution() -> pd.DataFrame:
    station = _read_release_csv("station_catalog.csv")
    if len(station) == 0:
        return pd.DataFrame()

    work = station.copy()
    for col in ("continent_region", "country", "resolution", "cluster_uid"):
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].map(_clean_text)
    work["continent_region"] = work["continent_region"].replace("", "Unknown")
    work["country"] = work["country"].replace("", "Unknown")
    work["record_count"] = pd.to_numeric(work.get("record_count", 0), errors="coerce").fillna(0).astype(int)
    work["time_start_dt"] = pd.to_datetime(work.get("time_start", ""), errors="coerce")
    work["time_end_dt"] = pd.to_datetime(work.get("time_end", ""), errors="coerce")
    work["record_length_years"] = [
        _record_span_years(start, end, resolution)
        for start, end, resolution in zip(work["time_start_dt"], work["time_end_dt"], work["resolution"])
    ]

    rows = []
    for (region, country, resolution), sub in work.groupby(["continent_region", "country", "resolution"], sort=True):
        first_date = sub["time_start_dt"].min()
        last_date = sub["time_end_dt"].max()
        years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        rows.append(
            {
                "continent_region": region,
                "country": country,
                "resolution": resolution,
                "first_date": _date_text(first_date),
                "last_date": _date_text(last_date),
                "first_year": _year_value(first_date),
                "last_year": _year_value(last_date),
                "clusters": int(sub["cluster_uid"].replace("", np.nan).dropna().nunique()),
                "cluster_resolution_rows": int(len(sub)),
                "record_count": int(sub["record_count"].sum()),
                "median_record_length_years": _format_float(years.median()),
                "max_record_length_years": _format_float(years.max()),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["continent_region", "country", "resolution"]
    ).reset_index(drop=True)


def build_climatology_station_tables(path: Optional[Path] = None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = Path(path or RELEASE_PATHS["climatology"])
    if not path.is_file() or not HAS_NC4:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    with nc4.Dataset(path, "r") as nc:
        n_stations = int(len(nc.dimensions["n_stations"])) if "n_stations" in nc.dimensions else 0
        n_records = int(len(nc.dimensions["n_records"])) if "n_records" in nc.dimensions else 0
        station_ids = _climatology_station_ids(nc, n_stations)
        source_indices = _read_numeric_1d(nc, ["source_index"], n_stations, default=-1).astype(int, copy=False)
        source_names = _read_text_1d(nc, ["source_name"], int(len(nc.dimensions["n_sources"])) if "n_sources" in nc.dimensions else 0)
        declared_start = _read_text_1d(nc, ["source_station_time_coverage_start", "time_coverage_start"], n_stations)
        declared_end = _read_text_1d(nc, ["source_station_time_coverage_end", "time_coverage_end"], n_stations)
        station_index = _read_numeric_1d(nc, ["station_index"], n_records, default=-1).astype(int, copy=False)
        dates = _decode_time(nc)
        if len(dates) >= n_records:
            dates = dates[:n_records]
        elif len(dates) < n_records:
            dates = dates.append(pd.DatetimeIndex([pd.NaT] * (n_records - len(dates))))
        q_mask = _read_1d_valid_var(nc, Q_VAR_NAMES, n_records)
        ssc_mask = _read_1d_valid_var(nc, SSC_VAR_NAMES, n_records)
        ssl_mask = _read_1d_valid_var(nc, SSL_VAR_NAMES, n_records)

    records = pd.DataFrame(
        {
            "station_index": station_index,
            "time": pd.DatetimeIndex(dates),
            "valid_Q": q_mask,
            "valid_SSC": ssc_mask,
            "valid_SSL": ssl_mask,
        }
    )
    records = records[(records["station_index"] >= 0) & (records["station_index"] < n_stations)].copy()
    records["valid_any"] = records["valid_Q"] | records["valid_SSC"] | records["valid_SSL"]

    rows = []
    for idx in range(n_stations):
        sub = records[records["station_index"] == idx]
        valid = sub[sub["valid_any"]]
        start = _parse_optional_date(declared_start[idx] if idx < len(declared_start) else "")
        end = _parse_optional_date(declared_end[idx] if idx < len(declared_end) else "")
        if pd.isna(start) and len(valid):
            start = valid["time"].min()
        if pd.isna(end) and len(valid):
            end = valid["time"].max()
        years = _record_span_years(start, end, "climatology")
        source_idx = source_indices[idx] if idx < len(source_indices) else -1
        source = source_names[source_idx] if 0 <= source_idx < len(source_names) else ""
        rows.append(
            {
                "station_uid": station_ids[idx],
                "source": source,
                "first_date": _date_text(start),
                "last_date": _date_text(end),
                "first_year": _year_value(start),
                "last_year": _year_value(end),
                "record_count_any": int(sub["valid_any"].sum()) if len(sub) else 0,
                "record_count_Q": int(sub["valid_Q"].sum()) if len(sub) else 0,
                "record_count_SSC": int(sub["valid_SSC"].sum()) if len(sub) else 0,
                "record_count_SSL": int(sub["valid_SSL"].sum()) if len(sub) else 0,
                "has_Q": bool(sub["valid_Q"].any()) if len(sub) else False,
                "has_SSC": bool(sub["valid_SSC"].any()) if len(sub) else False,
                "has_SSL": bool(sub["valid_SSL"].any()) if len(sub) else False,
                "record_length_years": years,
            }
        )
    station_df = pd.DataFrame(rows)
    if len(station_df) == 0:
        return station_df, pd.DataFrame(), pd.DataFrame()

    years = pd.to_numeric(station_df["record_length_years"], errors="coerce")
    summary = {
        "product": "climatology",
        "unit_type": "climatology_station",
        "stations": int(len(station_df)),
        "sources": int(station_df["source"].replace("", np.nan).dropna().nunique()),
        "first_date": _date_text(pd.to_datetime(station_df["first_date"], errors="coerce").min()),
        "last_date": _date_text(pd.to_datetime(station_df["last_date"], errors="coerce").max()),
        "first_year": int(pd.to_numeric(station_df["first_year"], errors="coerce").min()),
        "last_year": int(pd.to_numeric(station_df["last_year"], errors="coerce").max()),
        "record_count_any": int(station_df["record_count_any"].sum()),
        "stations_Q": int(station_df["has_Q"].sum()),
        "stations_SSC": int(station_df["has_SSC"].sum()),
        "stations_SSL": int(station_df["has_SSL"].sum()),
        "median_record_length_years": _format_float(years.median()),
        "max_record_length_years": _format_float(years.max()),
    }
    for threshold in LONG_YEAR_THRESHOLDS:
        summary["n_gt_{}_years".format(threshold)] = int(np.count_nonzero(years > threshold))
    summary_df = pd.DataFrame([summary])

    source_rows = []
    for source, sub in station_df.groupby("source", sort=True):
        source_years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        first = pd.to_datetime(sub["first_date"], errors="coerce").min()
        last = pd.to_datetime(sub["last_date"], errors="coerce").max()
        source_rows.append(
            {
                "source": source,
                "stations": int(len(sub)),
                "record_count_any": int(sub["record_count_any"].sum()),
                "first_date": _date_text(first),
                "last_date": _date_text(last),
                "first_year": _year_value(first),
                "last_year": _year_value(last),
                "stations_Q": int(sub["has_Q"].sum()),
                "stations_SSC": int(sub["has_SSC"].sum()),
                "stations_SSL": int(sub["has_SSL"].sum()),
                "median_record_length_years": _format_float(source_years.median()),
                "max_record_length_years": _format_float(source_years.max()),
            }
        )
    source_df = pd.DataFrame(source_rows).sort_values(["record_count_any", "source"], ascending=[False, True])
    return summary_df, source_df, station_df


def resolve_satellite_paths(args) -> Tuple[Optional[Path], Optional[Path]]:
    if args.input_dir:
        input_dir = Path(args.input_dir).expanduser().resolve()
        nc_path = input_dir / "sed_reference_satellite.nc"
        catalog_path = input_dir / "satellite_catalog.csv"
    else:
        nc_path = RELEASE_SATELLITE_PATH
        catalog_path = RELEASE_SATELLITE_CATALOG_PATH
    return (nc_path if nc_path.is_file() else None, catalog_path if catalog_path.is_file() else None)


def _satellite_output_resolution(value: object) -> str:
    text = _clean_text(value).lower()
    return "satellite_{}".format(text or "daily")


def _satellite_catalog(catalog_path: Optional[Path]) -> pd.DataFrame:
    if catalog_path is None or not Path(catalog_path).is_file():
        return pd.DataFrame()
    df = pd.read_csv(catalog_path, keep_default_na=False)
    for col in ("satellite_station_uid", "cluster_uid", "source", "source_family", "resolution", "merge_policy"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].map(_clean_text)
    for col in ("cluster_id", "n_records", "lat", "lon"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["resolution"] = df["resolution"].str.lower().replace("", "daily")
    df["output_resolution"] = df["resolution"].map(_satellite_output_resolution)
    df["time_start_dt"] = pd.to_datetime(df.get("time_start", ""), errors="coerce")
    df["time_end_dt"] = pd.to_datetime(df.get("time_end", ""), errors="coerce")
    df["n_records"] = pd.to_numeric(df.get("n_records", 0), errors="coerce").fillna(0).astype(int)
    df["record_length_years"] = [
        _record_span_years(start, end, resolution)
        for start, end, resolution in zip(df["time_start_dt"], df["time_end_dt"], df["resolution"])
    ]
    return df


def build_satellite_station_table(catalog: pd.DataFrame) -> pd.DataFrame:
    if len(catalog) == 0:
        return pd.DataFrame()
    rows = []
    for row in catalog.itertuples(index=False):
        resolution = getattr(row, "output_resolution")
        native_resolution = getattr(row, "resolution", "daily")
        start = getattr(row, "time_start_dt")
        end = getattr(row, "time_end_dt")
        lengths = _span_lengths(start, end, native_resolution if native_resolution in MATRIX_RESOLUTIONS else "daily")
        rows.append(
            {
                "resolution": resolution,
                "unit_type": "satellite_station",
                "unit_id": getattr(row, "satellite_station_uid"),
                "cluster_id": getattr(row, "cluster_id") if hasattr(row, "cluster_id") else "",
                "cluster_uid": getattr(row, "cluster_uid") if hasattr(row, "cluster_uid") else "",
                "source": getattr(row, "source") if hasattr(row, "source") else "",
                "source_family": getattr(row, "source_family") if hasattr(row, "source_family") else "",
                "first_date": _date_text(start),
                "last_date": _date_text(end),
                "first_year": _year_value(start),
                "last_year": _year_value(end),
                "record_count_any": int(getattr(row, "n_records")),
                "record_count_Q": np.nan,
                "record_count_SSC": np.nan,
                "record_count_SSL": np.nan,
                "unique_observed_days": np.nan,
                "unique_observed_months": np.nan,
                "unique_observed_years": np.nan,
                "record_length_steps": int(lengths["record_length_steps"]),
                "calendar_span_days": _format_float(lengths["record_length_days"]),
                "calendar_span_months": _format_float(lengths["record_length_months"]),
                "calendar_span_years": _format_float(lengths["record_length_years"]),
                "record_length_days": _format_float(lengths["record_length_days"]),
                "record_length_months": _format_float(lengths["record_length_months"]),
                "record_length_years": _format_float(lengths["record_length_years"]),
                "nominal_coverage_ratio": np.nan,
                "observed_period_density": np.nan,
                "temporal_coverage_ratio": np.nan,
                "record_frequency_per_year": _format_float(
                    float(getattr(row, "n_records")) / float(lengths["record_length_years"])
                    if lengths["record_length_years"] and lengths["record_length_years"] > 0
                    else np.nan
                ),
            }
        )
    return pd.DataFrame(rows)


def build_satellite_catalog_tables(catalog: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    station_df = build_satellite_station_table(catalog)
    if len(catalog) == 0:
        return pd.DataFrame(), pd.DataFrame(), station_df

    source_rows = []
    for (source, resolution), sub in catalog.groupby(["source", "resolution"], sort=True):
        years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        first = sub["time_start_dt"].min()
        last = sub["time_end_dt"].max()
        source_rows.append(
            {
                "source": source,
                "resolution": resolution,
                "output_resolution": _satellite_output_resolution(resolution),
                "source_family": "|".join(sorted(set(x for x in sub["source_family"].map(_clean_text) if x))),
                "satellite_stations": int(sub["satellite_station_uid"].nunique()),
                "linked_clusters": int(sub["cluster_uid"].replace("", np.nan).dropna().nunique()),
                "record_count_any": int(sub["n_records"].sum()),
                "first_date": _date_text(first),
                "last_date": _date_text(last),
                "first_year": _year_value(first),
                "last_year": _year_value(last),
                "median_record_length_years": _format_float(years.median()),
                "max_record_length_years": _format_float(years.max()),
            }
        )
    by_source = pd.DataFrame(source_rows).sort_values(["record_count_any", "source"], ascending=[False, True])

    cluster_rows = []
    for cluster_uid, sub in catalog.groupby("cluster_uid", sort=True):
        if _clean_text(cluster_uid) == "":
            continue
        first = sub["time_start_dt"].min()
        last = sub["time_end_dt"].max()
        cluster_rows.append(
            {
                "cluster_uid": cluster_uid,
                "cluster_id": _clean_text(sub["cluster_id"].dropna().iloc[0]) if "cluster_id" in sub.columns and len(sub["cluster_id"].dropna()) else "",
                "satellite_stations": int(sub["satellite_station_uid"].nunique()),
                "sources": int(sub["source"].replace("", np.nan).dropna().nunique()),
                "source_names": "|".join(sorted(set(x for x in sub["source"].map(_clean_text) if x))),
                "record_count_any": int(sub["n_records"].sum()),
                "first_date": _date_text(first),
                "last_date": _date_text(last),
                "first_year": _year_value(first),
                "last_year": _year_value(last),
            }
        )
    by_cluster = pd.DataFrame(cluster_rows).sort_values(["record_count_any", "cluster_uid"], ascending=[False, True])
    return by_source, by_cluster, station_df


def build_satellite_by_year_from_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    if len(catalog) == 0:
        return pd.DataFrame()
    rows = []
    valid = catalog.dropna(subset=["time_start_dt", "time_end_dt"]).copy()
    if len(valid) == 0:
        return pd.DataFrame()
    for resolution, res_df in valid.groupby("output_resolution", sort=True):
        first_year = int(res_df["time_start_dt"].dt.year.min())
        last_year = int(res_df["time_end_dt"].dt.year.max())
        for year in range(first_year, last_year + 1):
            active = res_df[(res_df["time_start_dt"].dt.year <= year) & (res_df["time_end_dt"].dt.year >= year)]
            if len(active) == 0:
                continue
            rows.append(
                {
                    "year": int(year),
                    "resolution": resolution,
                    "unit_type": "satellite_station",
                    "active_units": int(active["satellite_station_uid"].nunique()),
                    "active_clusters": int(active["cluster_uid"].replace("", np.nan).dropna().nunique()),
                    "active_units_Q": np.nan,
                    "active_units_SSC": np.nan,
                    "active_units_SSL": np.nan,
                    "records_any": np.nan,
                    "records_Q": np.nan,
                    "records_SSC": np.nan,
                    "records_SSL": np.nan,
                    "source": "catalog_span",
                }
            )
    return pd.DataFrame(rows)


def _decode_time_chunk(time_var, raw_values) -> pd.DatetimeIndex:
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        if "days since 1970-01-01" in units:
            return pd.DatetimeIndex(pd.to_datetime(np.asarray(raw_values), unit="D", origin="1970-01-01", errors="coerce"))
    except Exception:
        pass
    try:
        return pd.DatetimeIndex(pd.to_datetime(num2date(raw_values, units=units, calendar=calendar), errors="coerce"))
    except Exception:
        return pd.DatetimeIndex(pd.to_datetime(raw_values, errors="coerce"))


def scan_satellite_nc(
    nc_path: Optional[Path],
    catalog: pd.DataFrame,
    chunk_records: int = 1000000,
) -> Tuple[List[Dict[str, object]], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if nc_path is None or not Path(nc_path).is_file() or not HAS_NC4:
        return [], pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    with nc4.Dataset(nc_path, "r") as nc:
        n_records = int(len(nc.dimensions["n_satellite_records"]))
        n_stations = int(len(nc.dimensions["n_satellite_stations"]))
        if len(catalog) >= n_stations and "output_resolution" in catalog.columns:
            station_res = catalog["output_resolution"].astype(str).to_numpy()[:n_stations]
        else:
            station_res = np.asarray([SATELLITE_OUTPUT_RESOLUTION] * n_stations, dtype=object)
        station_var = nc.variables["satellite_station_index"]
        cluster_var = nc.variables["cluster_id"]
        time_var = nc.variables["time"]
        var_names = {
            "Q": _first_existing_var(nc, Q_VAR_NAMES),
            "SSC": _first_existing_var(nc, SSC_VAR_NAMES),
            "SSL": _first_existing_var(nc, SSL_VAR_NAMES),
        }
        fill_values = {
            key: _var_fill_values(nc.variables[name]) if name else tuple(FILL_FLOAT_VALUES)
            for key, name in var_names.items()
        }

        q_counts = np.zeros(n_stations, dtype=np.int64)
        ssc_counts = np.zeros(n_stations, dtype=np.int64)
        ssl_counts = np.zeros(n_stations, dtype=np.int64)
        any_counts = np.zeros(n_stations, dtype=np.int64)
        first_by_resolution: Dict[str, pd.Timestamp] = {}
        last_by_resolution: Dict[str, pd.Timestamp] = {}
        year_rows: Dict[Tuple[int, str], Dict[str, object]] = {}
        year_station_sets: Dict[Tuple[int, str], set] = {}
        year_cluster_sets: Dict[Tuple[int, str], set] = {}
        year_var_station_sets: Dict[Tuple[int, str, str], set] = {}

        for start in range(0, n_records, max(1, int(chunk_records))):
            stop = min(start + max(1, int(chunk_records)), n_records)
            station_idx = np.asarray(station_var[start:stop], dtype=np.int64)
            cluster_ids = np.asarray(cluster_var[start:stop])
            dates = _decode_time_chunk(time_var, time_var[start:stop])
            years = pd.Series(dates).dt.year.to_numpy()
            valid_year_mask = pd.notna(pd.Series(years)).to_numpy()

            masks = {}
            for key, name in var_names.items():
                if name is None:
                    masks[key] = np.zeros(stop - start, dtype=bool)
                else:
                    masks[key] = _valid_float_mask(nc.variables[name][start:stop], fill_values[key]).reshape(-1)
            any_mask = masks["Q"] | masks["SSC"] | masks["SSL"]
            valid_station = (station_idx >= 0) & (station_idx < n_stations)
            any_mask &= valid_station
            for key in CORE_VARS:
                masks[key] &= valid_station

            if np.any(any_mask):
                observed_stations = station_idx[any_mask]
                observed_resolutions = np.unique(station_res[observed_stations])
                for resolution in observed_resolutions:
                    rmask = any_mask & (station_res[station_idx] == resolution)
                    observed_dates = dates[rmask]
                    if len(observed_dates) == 0:
                        continue
                    chunk_first = observed_dates.min()
                    chunk_last = observed_dates.max()
                    prev_first = first_by_resolution.get(str(resolution), pd.NaT)
                    prev_last = last_by_resolution.get(str(resolution), pd.NaT)
                    first_by_resolution[str(resolution)] = chunk_first if pd.isna(prev_first) or chunk_first < prev_first else prev_first
                    last_by_resolution[str(resolution)] = chunk_last if pd.isna(prev_last) or chunk_last > prev_last else prev_last

            for key, counts in (("Q", q_counts), ("SSC", ssc_counts), ("SSL", ssl_counts)):
                idx, vals = np.unique(station_idx[masks[key]], return_counts=True)
                good = (idx >= 0) & (idx < n_stations)
                counts[idx[good]] += vals[good]
            idx, vals = np.unique(station_idx[any_mask], return_counts=True)
            good = (idx >= 0) & (idx < n_stations)
            any_counts[idx[good]] += vals[good]

            for year in sorted(set(int(y) for y in years[valid_year_mask] if not pd.isna(y))):
                ymask = years == year
                candidate_stations = station_idx[ymask & valid_station]
                for resolution in np.unique(station_res[candidate_stations]) if candidate_stations.size else []:
                    rmask = station_res[station_idx] == resolution
                    row_key = (year, str(resolution))
                    row = year_rows.setdefault(
                        row_key,
                        {
                            "year": int(year),
                            "resolution": str(resolution),
                            "unit_type": "satellite_station",
                            "records_any": 0,
                            "records_Q": 0,
                            "records_SSC": 0,
                            "records_SSL": 0,
                        },
                    )
                    y_any = ymask & rmask & any_mask
                    row["records_any"] += int(np.count_nonzero(y_any))
                    for key in CORE_VARS:
                        y_var = ymask & rmask & masks[key]
                        row["records_{}".format(key)] += int(np.count_nonzero(y_var))
                        year_var_station_sets.setdefault((year, str(resolution), key), set()).update(
                            int(v) for v in np.unique(station_idx[y_var]) if 0 <= int(v) < n_stations
                        )
                    year_station_sets.setdefault(row_key, set()).update(
                        int(v) for v in np.unique(station_idx[y_any]) if 0 <= int(v) < n_stations
                    )
                    year_cluster_sets.setdefault(row_key, set()).update(
                        int(v) for v in np.unique(cluster_ids[y_any]) if pd.notna(v)
                    )

    year_output = []
    for row_key in sorted(year_rows):
        year, resolution = row_key
        row = dict(year_rows[row_key])
        row["active_units"] = int(len(year_station_sets.get(row_key, set())))
        row["active_clusters"] = int(len(year_cluster_sets.get(row_key, set())))
        for key in CORE_VARS:
            row["active_units_{}".format(key)] = int(len(year_var_station_sets.get((year, resolution, key), set())))
        row["source"] = "satellite_nc"
        year_output.append(row)
    by_year_df = pd.DataFrame(year_output)
    if len(by_year_df) > 0:
        ordered_cols = [
            "year",
            "resolution",
            "unit_type",
            "active_units",
            "active_clusters",
            "active_units_Q",
            "active_units_SSC",
            "active_units_SSL",
            "records_any",
            "records_Q",
            "records_SSC",
            "records_SSL",
            "source",
        ]
        by_year_df = by_year_df[ordered_cols]

    source_file = str(nc_path)
    summaries = []
    for resolution in _resolution_sequence(station_res):
        station_mask = station_res == resolution
        active = (any_counts > 0) & station_mask
        catalog_subset = catalog[catalog["output_resolution"] == resolution] if len(catalog) else pd.DataFrame()
        first_date = first_by_resolution.get(resolution, pd.NaT)
        last_date = last_by_resolution.get(resolution, pd.NaT)
        summaries.append(
            {
                "resolution": resolution,
                "product": SATELLITE_PRODUCT,
                "unit_type": "satellite_station",
                "source_file": source_file,
                "first_date": _date_text(first_date),
                "last_date": _date_text(last_date),
                "first_year": _year_value(first_date),
                "last_year": _year_value(last_date),
                "time_steps": int(any_counts[station_mask].sum()),
                "active_units": int(np.count_nonzero(active)),
                "active_clusters": int(catalog_subset["cluster_uid"].replace("", np.nan).dropna().nunique()) if len(catalog_subset) else np.nan,
                "active_units_Q": int(np.count_nonzero((q_counts > 0) & station_mask)),
                "active_units_SSC": int(np.count_nonzero((ssc_counts > 0) & station_mask)),
                "active_units_SSL": int(np.count_nonzero((ssl_counts > 0) & station_mask)),
                "record_count_any": int(any_counts[station_mask].sum()),
                "record_count_Q": int(q_counts[station_mask].sum()),
                "record_count_SSC": int(ssc_counts[station_mask].sum()),
                "record_count_SSL": int(ssl_counts[station_mask].sum()),
                "records_any": int(any_counts[station_mask].sum()),
                "records_Q": int(q_counts[station_mask].sum()),
                "records_SSC": int(ssc_counts[station_mask].sum()),
                "records_SSL": int(ssl_counts[station_mask].sum()),
            }
        )

    variable_rows = []
    for resolution in _resolution_sequence(station_res):
        station_mask = station_res == resolution
        res_year = by_year_df[by_year_df["resolution"] == resolution] if len(by_year_df) else pd.DataFrame()
        active_clusters = next((s.get("active_clusters", np.nan) for s in summaries if s["resolution"] == resolution), np.nan)
        for key, counts in (("Q", q_counts), ("SSC", ssc_counts), ("SSL", ssl_counts)):
            record_col = "records_{}".format(key)
            active_col = "active_units_{}".format(key)
            observed = res_year[pd.to_numeric(res_year[record_col], errors="coerce").fillna(0) > 0] if len(res_year) else pd.DataFrame()
            if len(observed):
                peak_idx = pd.to_numeric(res_year[active_col], errors="coerce").idxmax()
                record_peak_idx = pd.to_numeric(res_year[record_col], errors="coerce").idxmax()
                first_year_val = int(observed["year"].min())
                last_year_val = int(observed["year"].max())
                peak_active = int(res_year.loc[peak_idx, active_col])
                peak_year = int(res_year.loc[peak_idx, "year"])
                peak_records = int(res_year.loc[record_peak_idx, record_col])
                peak_record_year = int(res_year.loc[record_peak_idx, "year"])
            else:
                first_year_val = last_year_val = peak_year = peak_record_year = ""
                peak_active = peak_records = 0
            variable_rows.append(
                {
                    "resolution": resolution,
                    "unit_type": "satellite_station",
                    "variable": key,
                    "first_year": first_year_val,
                    "last_year": last_year_val,
                    "active_units": int(np.count_nonzero((counts > 0) & station_mask)),
                    "linked_clusters": active_clusters,
                    "record_count": int(counts[station_mask].sum()),
                    "peak_active_units": peak_active,
                    "peak_active_year": peak_year,
                    "peak_records": peak_records,
                    "peak_record_year": peak_record_year,
                }
            )
    variable_df = pd.DataFrame(variable_rows)

    station_counts = pd.DataFrame(
        {
            "satellite_station_index": np.arange(len(any_counts), dtype=int),
            "record_count_any_nc": any_counts,
            "record_count_Q_nc": q_counts,
            "record_count_SSC_nc": ssc_counts,
            "record_count_SSL_nc": ssl_counts,
        }
    )
    return summaries, by_year_df, variable_df, station_counts


def build_satellite_summaries_from_catalog(catalog: pd.DataFrame, station_df: pd.DataFrame) -> List[Dict[str, object]]:
    if len(catalog) == 0:
        return []
    summaries = []
    for resolution, sub in catalog.groupby("output_resolution", sort=True):
        first = sub["time_start_dt"].min()
        last = sub["time_end_dt"].max()
        station_sub = station_df[station_df["resolution"] == resolution] if len(station_df) else pd.DataFrame()
        summary = {
            "resolution": resolution,
            "product": SATELLITE_PRODUCT,
            "unit_type": "satellite_station",
            "source_file": str(RELEASE_SATELLITE_CATALOG_PATH),
            "first_date": _date_text(first),
            "last_date": _date_text(last),
            "first_year": _year_value(first),
            "last_year": _year_value(last),
            "time_steps": int(sub["n_records"].sum()),
            "active_units": int(sub["satellite_station_uid"].nunique()),
            "active_clusters": int(sub["cluster_uid"].replace("", np.nan).dropna().nunique()),
            "active_units_Q": np.nan,
            "active_units_SSC": np.nan,
            "active_units_SSL": np.nan,
            "record_count_any": int(sub["n_records"].sum()),
            "record_count_Q": np.nan,
            "record_count_SSC": np.nan,
            "record_count_SSL": np.nan,
            "records_any": int(sub["n_records"].sum()),
            "records_Q": np.nan,
            "records_SSC": np.nan,
            "records_SSL": np.nan,
        }
        years = pd.to_numeric(station_sub["record_length_years"], errors="coerce") if len(station_sub) else pd.Series(dtype="float64")
        for threshold in LONG_YEAR_THRESHOLDS:
            summary["n_gt_{}_years".format(threshold)] = int(np.count_nonzero(years > threshold))
        summaries.append(summary)
    return summaries


def write_tables(
    tables_dir: Path,
    summary_rows: List[Dict[str, object]],
    by_year_df: pd.DataFrame,
    unit_df: pd.DataFrame,
    extra_tables: Optional[Dict[str, pd.DataFrame]] = None,
) -> Dict[str, Path]:
    tables_dir.mkdir(parents=True, exist_ok=True)
    summary_with_lengths = add_length_summary_columns(summary_rows, unit_df)
    summary_df = pd.DataFrame(summary_with_lengths)
    distribution_df = build_record_length_distribution(unit_df)
    by_variable_df = build_temporal_coverage_by_variable(by_year_df, summary_rows)
    if extra_tables and "satellite_by_variable" in extra_tables and len(extra_tables["satellite_by_variable"]):
        by_variable_df = pd.concat([by_variable_df, extra_tables["satellite_by_variable"]], ignore_index=True)
    long_records_df = build_long_records_by_resolution(unit_df)
    by_source_df = build_temporal_coverage_by_source()
    by_region_resolution_df = build_temporal_coverage_by_region_resolution()

    outputs = {
        "temporal_coverage_by_resolution": tables_dir / "table_temporal_coverage_by_resolution.csv",
        "temporal_coverage_by_variable": tables_dir / "table_temporal_coverage_by_variable.csv",
        "active_units_by_year": tables_dir / "table_active_units_by_year.csv",
        "active_clusters_by_year": tables_dir / "table_active_clusters_by_year.csv",
        "record_length_distribution": tables_dir / "table_record_length_distribution.csv",
        "record_lengths_by_unit": tables_dir / "table_temporal_coverage_record_lengths_by_unit.csv",
        "long_records_by_resolution": tables_dir / "table_long_records_by_resolution.csv",
        "temporal_coverage_by_source": tables_dir / "table_temporal_coverage_by_source.csv",
        "temporal_coverage_by_region_resolution": tables_dir / "table_temporal_coverage_by_region_resolution.csv",
        "climatology_temporal_summary": tables_dir / "table_climatology_temporal_summary.csv",
        "climatology_by_source": tables_dir / "table_climatology_by_source.csv",
        "climatology_record_lengths_by_station": tables_dir / "table_climatology_record_lengths_by_station.csv",
        "satellite_temporal_summary": tables_dir / "table_satellite_temporal_summary.csv",
        "satellite_by_year": tables_dir / "table_satellite_by_year.csv",
        "satellite_by_source": tables_dir / "table_satellite_by_source.csv",
        "satellite_record_lengths_by_station": tables_dir / "table_satellite_record_lengths_by_station.csv",
        "satellite_by_linked_cluster": tables_dir / "table_satellite_by_linked_cluster.csv",
    }
    summary_df.to_csv(outputs["temporal_coverage_by_resolution"], index=False)
    by_variable_df.to_csv(outputs["temporal_coverage_by_variable"], index=False)
    by_year_df.to_csv(outputs["active_units_by_year"], index=False)
    by_year_df.to_csv(outputs["active_clusters_by_year"], index=False)
    distribution_df.to_csv(outputs["record_length_distribution"], index=False)
    unit_df.to_csv(outputs["record_lengths_by_unit"], index=False)
    long_records_df.to_csv(outputs["long_records_by_resolution"], index=False)
    by_source_df.to_csv(outputs["temporal_coverage_by_source"], index=False)
    by_region_resolution_df.to_csv(outputs["temporal_coverage_by_region_resolution"], index=False)
    extra_tables = extra_tables or {}
    for key in (
        "climatology_temporal_summary",
        "climatology_by_source",
        "climatology_record_lengths_by_station",
        "satellite_temporal_summary",
        "satellite_by_year",
        "satellite_by_source",
        "satellite_record_lengths_by_station",
        "satellite_by_linked_cluster",
    ):
        df = extra_tables.get(key)
        if df is not None and len(df):
            df.to_csv(outputs[key], index=False)
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
    _save_current_figure(out_path)
    plt.close()


def _save_current_figure(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    png_path = out_path.with_suffix(".png")
    pdf_path = out_path.with_suffix(".pdf")
    import matplotlib.pyplot as plt

    plt.savefig(png_path, dpi=300)
    plt.savefig(pdf_path)


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

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7.2, 4.2))
    for resolution in RESOLUTION_ORDER:
        sub = unit_df[unit_df["resolution"] == resolution]
        vals = pd.to_numeric(sub["record_length_years"], errors="coerce").dropna()
        vals = vals[vals >= 0]
        if len(vals) == 0:
            continue
        vals = np.sort(vals.to_numpy(dtype=float))
        y = np.arange(1, len(vals) + 1, dtype=float) / float(len(vals))
        plt.step(vals, y, where="post", linewidth=1.8, label=resolution)
    plt.xlabel("Record length (years equivalent)")
    plt.ylabel("Cumulative fraction of clusters/stations")
    plt.title("Record length distribution (ECDF)")
    plt.legend(frameon=False)
    plt.tight_layout()
    _save_current_figure(out_path)
    plt.close()


def plot_records_by_year_variable(by_year_df: pd.DataFrame, out_path: Path) -> None:
    if len(by_year_df) == 0:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_df = by_year_df[by_year_df["resolution"].isin(MATRIX_RESOLUTIONS)].copy()
    if len(plot_df) == 0:
        return
    fig, axes = plt.subplots(len(MATRIX_RESOLUTIONS), 1, figsize=(7.2, 6.0), sharex=True)
    colors = {"Q": "#1f77b4", "SSC": "#d62728", "SSL": "#2ca02c"}
    for ax, resolution in zip(axes, MATRIX_RESOLUTIONS):
        sub = plot_df[plot_df["resolution"] == resolution].sort_values("year")
        for variable in CORE_VARS:
            col = "records_{}".format(variable)
            if col in sub.columns:
                ax.plot(sub["year"], sub[col], linewidth=1.3, color=colors[variable], label=variable)
        ax.set_ylabel(resolution)
        ax.grid(True, axis="y", alpha=0.25)
    axes[-1].set_xlabel("Year")
    axes[0].set_title("Annual record counts by variable")
    axes[0].legend(frameon=False, ncol=3)
    fig.text(0.02, 0.5, "Valid cluster-time observations", rotation=90, va="center")
    plt.tight_layout(rect=(0.04, 0, 1, 1))
    _save_current_figure(out_path)
    plt.close()


def plot_long_record_counts(unit_df: pd.DataFrame, out_path: Path) -> None:
    if len(unit_df) == 0:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = []
    for resolution in RESOLUTION_ORDER:
        sub = unit_df[unit_df["resolution"] == resolution]
        years = pd.to_numeric(sub["record_length_years"], errors="coerce")
        for threshold in LONG_YEAR_THRESHOLDS:
            rows.append(
                {
                    "resolution": resolution,
                    "threshold": threshold,
                    "count": int(np.count_nonzero(years > threshold)),
                }
            )
    df = pd.DataFrame(rows)
    if len(df) == 0:
        return
    x = np.arange(len(LONG_YEAR_THRESHOLDS), dtype=float)
    width = 0.18
    plt.figure(figsize=(7.2, 4.2))
    for idx, resolution in enumerate(RESOLUTION_ORDER):
        sub = df[df["resolution"] == resolution]
        if len(sub) == 0:
            continue
        offset = (idx - 1.5) * width
        plt.bar(x + offset, sub["count"], width=width, label=resolution)
    plt.xticks(x, [">{:d}".format(th) for th in LONG_YEAR_THRESHOLDS])
    plt.xlabel("Record length threshold (years)")
    plt.ylabel("Number of clusters/stations")
    plt.title("Long-record counts by product")
    plt.legend(frameon=False)
    plt.tight_layout()
    _save_current_figure(out_path)
    plt.close()


def plot_temporal_coverage_heatmap(by_year_df: pd.DataFrame, out_path: Path) -> None:
    if len(by_year_df) == 0:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_df = by_year_df[by_year_df["resolution"].isin(MATRIX_RESOLUTIONS)].copy()
    if len(plot_df) == 0:
        return
    plot_df["decade"] = (pd.to_numeric(plot_df["year"], errors="coerce") // 10 * 10).astype(int)
    pivot = plot_df.pivot_table(
        index="resolution",
        columns="decade",
        values="active_units",
        aggfunc="mean",
        fill_value=0,
    )
    pivot = pivot.reindex(list(MATRIX_RESOLUTIONS)).fillna(0)
    if pivot.shape[1] == 0:
        return
    plt.figure(figsize=(8.0, 2.8))
    image = plt.imshow(pivot.to_numpy(dtype=float), aspect="auto", cmap="viridis")
    plt.yticks(np.arange(len(pivot.index)), pivot.index.tolist())
    plt.xticks(np.arange(len(pivot.columns)), [str(int(c)) for c in pivot.columns], rotation=45, ha="right")
    plt.xlabel("Decade")
    plt.title("Mean active clusters by decade")
    cbar = plt.colorbar(image)
    cbar.set_label("Mean active clusters")
    plt.tight_layout()
    _save_current_figure(out_path)
    plt.close()


def plot_source_temporal_span(out_path: Path, max_sources: int = 20) -> None:
    source_df = build_temporal_coverage_by_source()
    if len(source_df) == 0:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    work = source_df.copy()
    work["first_year"] = pd.to_numeric(work["first_year"], errors="coerce")
    work["last_year"] = pd.to_numeric(work["last_year"], errors="coerce")
    work["record_count"] = pd.to_numeric(work["record_count"], errors="coerce").fillna(0)
    work = work.dropna(subset=["first_year", "last_year"])
    if len(work) == 0:
        return
    work["label"] = work["source_name"] + " (" + work["resolution"] + ")"
    work = work.sort_values("record_count", ascending=False).head(max_sources)
    work = work.sort_values(["first_year", "last_year", "label"]).reset_index(drop=True)
    y = np.arange(len(work))
    sizes = 20 + 80 * (
        np.log10(work["record_count"].clip(lower=1)) / max(1.0, np.log10(work["record_count"].clip(lower=1).max()))
    )
    plt.figure(figsize=(7.2, max(4.2, 0.25 * len(work) + 1.2)))
    for idx, row in work.iterrows():
        plt.hlines(y[idx], row["first_year"], row["last_year"], color="#555555", linewidth=1.4)
    plt.scatter(work["last_year"], y, s=sizes, color="#1f77b4", alpha=0.75)
    plt.yticks(y, work["label"])
    plt.xlabel("Year")
    plt.title("Temporal span of major source contributions")
    plt.tight_layout()
    _save_current_figure(out_path)
    plt.close()


def _plot_ecdf_by_group(df: pd.DataFrame, value_col: str, group_col: str, out_path: Path, title: str, xlabel: str) -> None:
    if len(df) == 0 or value_col not in df.columns:
        return
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.figure(figsize=(7.2, 4.2))
    plotted = False
    for label, sub in df.groupby(group_col, sort=True):
        vals = pd.to_numeric(sub[value_col], errors="coerce").dropna()
        vals = vals[vals >= 0]
        if len(vals) == 0:
            continue
        vals = np.sort(vals.to_numpy(dtype=float))
        y = np.arange(1, len(vals) + 1, dtype=float) / float(len(vals))
        plt.step(vals, y, where="post", linewidth=1.8, label=str(label) or "Unknown")
        plotted = True
    if not plotted:
        plt.close()
        return
    plt.xlabel(xlabel)
    plt.ylabel("Cumulative fraction")
    plt.title(title)
    plt.legend(frameon=False)
    plt.tight_layout()
    _save_current_figure(out_path)
    plt.close()


def plot_climatology_figures(figures_dir: Path, tables: Dict[str, pd.DataFrame]) -> Dict[str, Path]:
    outputs = {
        "climatology_record_length_distribution": figures_dir / "fig_climatology_record_length_distribution.png",
        "climatology_variable_coverage": figures_dir / "fig_climatology_variable_coverage.png",
        "climatology_source_contribution": figures_dir / "fig_climatology_source_contribution.png",
    }
    station_df = tables.get("climatology_record_lengths_by_station", pd.DataFrame())
    source_df = tables.get("climatology_by_source", pd.DataFrame())
    _plot_ecdf_by_group(
        station_df.assign(product="climatology") if len(station_df) else station_df,
        "record_length_years",
        "product",
        outputs["climatology_record_length_distribution"],
        "Climatology station temporal-span distribution",
        "Temporal span (years)",
    )

    if len(station_df):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        counts = [int(station_df.get("has_{}".format(var), pd.Series(dtype=bool)).sum()) for var in CORE_VARS]
        plt.figure(figsize=(5.4, 3.6))
        plt.bar(list(CORE_VARS), counts, color=["#1f77b4", "#d62728", "#2ca02c"])
        plt.ylabel("Stations")
        plt.title("Climatology variable coverage")
        plt.tight_layout()
        _save_current_figure(outputs["climatology_variable_coverage"])
        plt.close()

    if len(source_df):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        work = source_df.sort_values("record_count_any", ascending=False).head(12).sort_values("record_count_any")
        plt.figure(figsize=(7.2, max(3.8, 0.3 * len(work) + 1.2)))
        plt.barh(work["source"].replace("", "Unknown"), work["record_count_any"], color="#4c78a8")
        plt.xlabel("Records")
        plt.title("Climatology source contribution")
        plt.tight_layout()
        _save_current_figure(outputs["climatology_source_contribution"])
        plt.close()
    return outputs


def plot_satellite_figures(figures_dir: Path, tables: Dict[str, pd.DataFrame]) -> Dict[str, Path]:
    outputs = {
        "satellite_active_units_by_year": figures_dir / "fig_satellite_active_units_by_year.png",
        "satellite_records_by_year_variable": figures_dir / "fig_satellite_records_by_year_variable.png",
        "satellite_record_length_distribution": figures_dir / "fig_satellite_record_length_distribution.png",
        "satellite_source_contribution": figures_dir / "fig_satellite_source_contribution.png",
        "satellite_temporal_heatmap": figures_dir / "fig_satellite_temporal_heatmap.png",
    }
    by_year = tables.get("satellite_by_year", pd.DataFrame())
    station_df = tables.get("satellite_record_lengths_by_station", pd.DataFrame())
    source_df = tables.get("satellite_by_source", pd.DataFrame())

    if len(by_year):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        plot_df = by_year.sort_values("year")
        plt.figure(figsize=(7.2, 4.2))
        for resolution, sub in plot_df.groupby("resolution", sort=True):
            plt.plot(sub["year"], sub["active_units"], linewidth=1.8, label="{} stations".format(resolution))
            if "active_clusters" in sub.columns:
                plt.plot(sub["year"], sub["active_clusters"], linewidth=1.4, linestyle="--", label="{} clusters".format(resolution))
        plt.xlabel("Year")
        plt.ylabel("Active units")
        plt.title("Satellite validation temporal coverage")
        plt.legend(frameon=False)
        plt.tight_layout()
        _save_current_figure(outputs["satellite_active_units_by_year"])
        plt.close()

        if all("records_{}".format(var) in plot_df.columns for var in CORE_VARS):
            plt.figure(figsize=(7.2, 4.2))
            for resolution, sub in plot_df.groupby("resolution", sort=True):
                for var, color in zip(CORE_VARS, ["#1f77b4", "#d62728", "#2ca02c"]):
                    plt.plot(
                        sub["year"],
                        pd.to_numeric(sub["records_{}".format(var)], errors="coerce"),
                        linewidth=1.3,
                        label="{} {}".format(resolution, var),
                        color=color,
                        linestyle="-" if resolution.endswith("daily") else "--",
                    )
            plt.xlabel("Year")
            plt.ylabel("Records")
            plt.title("Satellite validation records by variable")
            plt.legend(frameon=False)
            plt.tight_layout()
            _save_current_figure(outputs["satellite_records_by_year_variable"])
            plt.close()

        heat = plot_df.copy()
        heat["decade"] = (pd.to_numeric(heat["year"], errors="coerce") // 10 * 10).astype(int)
        heat_rows = []
        for var in CORE_VARS:
            col = "records_{}".format(var)
            if col in heat.columns:
                tmp = heat.groupby("decade")[col].sum().reset_index()
                tmp["variable"] = var
                heat_rows.append(tmp.rename(columns={col: "records"}))
        if heat_rows:
            heat_df = pd.concat(heat_rows, ignore_index=True)
            pivot = heat_df.pivot_table(index="variable", columns="decade", values="records", fill_value=0)
            plt.figure(figsize=(7.2, 2.8))
            image = plt.imshow(pivot.to_numpy(dtype=float), aspect="auto", cmap="magma")
            plt.yticks(np.arange(len(pivot.index)), pivot.index.tolist())
            plt.xticks(np.arange(len(pivot.columns)), [str(int(c)) for c in pivot.columns], rotation=45, ha="right")
            plt.xlabel("Decade")
            plt.title("Satellite records by decade")
            cbar = plt.colorbar(image)
            cbar.set_label("Records")
            plt.tight_layout()
            _save_current_figure(outputs["satellite_temporal_heatmap"])
            plt.close()

    _plot_ecdf_by_group(
        station_df,
        "record_length_years",
        "source" if "source" in station_df.columns else "resolution",
        outputs["satellite_record_length_distribution"],
        "Satellite station record-length distribution",
        "Temporal span (years)",
    )

    if len(source_df):
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        work = source_df.sort_values("record_count_any", ascending=False).head(12).sort_values("record_count_any")
        labels = work["source"] + " (" + work["resolution"].astype(str) + ")"
        plt.figure(figsize=(7.2, max(3.8, 0.3 * len(work) + 1.2)))
        plt.barh(labels, work["record_count_any"], color="#f58518")
        plt.xlabel("Records")
        plt.title("Satellite validation source contribution")
        plt.tight_layout()
        _save_current_figure(outputs["satellite_source_contribution"])
        plt.close()
    return outputs


def write_figures(
    figures_dir: Path,
    by_year_df: pd.DataFrame,
    unit_df: pd.DataFrame,
    include_climatology_in_yearly_plot: bool,
    extra_tables: Optional[Dict[str, pd.DataFrame]] = None,
) -> Dict[str, Path]:
    figures_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "active_units_by_year": figures_dir / "fig_active_units_by_year.png",
        "active_clusters_by_year": figures_dir / "fig_active_clusters_by_year.png",
        "records_by_year_variable": figures_dir / "fig_records_by_year_variable.png",
        "record_length_distribution": figures_dir / "fig_record_length_distribution.png",
        "record_length_histogram": figures_dir / "fig_record_length_histogram.png",
        "long_record_counts": figures_dir / "fig_long_record_counts.png",
        "temporal_coverage_heatmap": figures_dir / "fig_temporal_coverage_heatmap.png",
        "source_temporal_span": figures_dir / "fig_source_temporal_span.png",
    }
    plot_active_clusters_by_year(
        by_year_df,
        outputs["active_units_by_year"],
        include_climatology=include_climatology_in_yearly_plot,
    )
    # Backward-compatible filename for existing manuscript drafts.
    plot_active_clusters_by_year(
        by_year_df,
        outputs["active_clusters_by_year"],
        include_climatology=include_climatology_in_yearly_plot,
    )
    plot_records_by_year_variable(by_year_df, outputs["records_by_year_variable"])
    plot_record_length_histogram(unit_df, outputs["record_length_distribution"])
    plot_record_length_histogram(unit_df, outputs["record_length_histogram"])
    plot_long_record_counts(unit_df, outputs["long_record_counts"])
    plot_temporal_coverage_heatmap(by_year_df, outputs["temporal_coverage_heatmap"])
    plot_source_temporal_span(outputs["source_temporal_span"])
    extra_tables = extra_tables or {}
    outputs.update(plot_climatology_figures(figures_dir, extra_tables))
    outputs.update(plot_satellite_figures(figures_dir, extra_tables))
    return outputs


def _format_int(value) -> str:
    try:
        if pd.isna(value):
            return "0"
        return "{:,}".format(int(value))
    except Exception:
        return str(value)


def _safe_int_value(value, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _summary_row(summary_df: pd.DataFrame, resolution: str) -> Dict[str, object]:
    sub = summary_df[summary_df["resolution"] == resolution]
    if len(sub) == 0:
        return {}
    return sub.iloc[0].to_dict()


def _peak_active_text(by_year_df: pd.DataFrame, resolution: str) -> str:
    sub = by_year_df[by_year_df["resolution"] == resolution].copy()
    if len(sub) == 0:
        return "not available"
    idx = pd.to_numeric(sub["active_units"], errors="coerce").idxmax()
    row = sub.loc[idx]
    return "{:,} active units in {:d}".format(int(row["active_units"]), int(row["year"]))


def _format_report_float(value, digits: int = 1) -> str:
    try:
        if pd.isna(value):
            return "NA"
        return "{:.{}f}".format(float(value), int(digits))
    except Exception:
        return "NA"


def _format_report_pct(value, digits: int = 1) -> str:
    return "{}%".format(_format_report_float(value, digits=digits))


def _safe_markdown(value) -> str:
    text = _clean_text(value)
    return text.replace("|", "\\|")


def _markdown_table(rows: Sequence[Dict[str, object]], columns: Sequence[str], headers: Sequence[str]) -> str:
    if not rows:
        return "No rows available for this run."
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_safe_markdown(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines)


def _compact_temporal_summary_rows(summary_df: pd.DataFrame) -> List[Dict[str, object]]:
    if summary_df.empty:
        return []
    rows: List[Dict[str, object]] = []
    for _, row in summary_df.iterrows():
        rows.append(
            {
                "resolution": row.get("resolution", ""),
                "product": row.get("product", ""),
                "unit_type": row.get("unit_type", ""),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "units": _format_int(row.get("active_units", 0)),
                "clusters": _format_int(row.get("active_clusters", row.get("active_units", 0))),
                "records": _format_int(row.get("record_count_any", row.get("records_any", 0))),
                "median_years": _format_report_float(row.get("median_record_length_years")),
                "max_years": _format_report_float(row.get("max_record_length_years")),
                "gt50": _format_int(row.get("n_gt_50_years", 0)),
                "gt100": _format_int(row.get("n_gt_100_years", 0)),
            }
        )
    return rows


def _compact_temporal_variable_rows(by_variable_df: pd.DataFrame) -> List[Dict[str, object]]:
    if by_variable_df.empty:
        return []
    work = by_variable_df.copy()
    work["resolution"] = pd.Categorical(work["resolution"], categories=_resolution_sequence(work["resolution"].unique()), ordered=True)
    work = work.sort_values(["resolution", "variable"], kind="mergesort")
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "resolution": row.get("resolution", ""),
                "variable": row.get("variable", ""),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "active_units": _format_int(row.get("active_units", 0)),
                "records": _format_int(row.get("record_count", 0)),
                "peak_units": "{} in {}".format(_format_int(row.get("peak_active_units", 0)), row.get("peak_active_year", "")),
                "peak_records": "{} in {}".format(_format_int(row.get("peak_records", 0)), row.get("peak_record_year", "")),
            }
        )
    return rows


def _compact_year_peak_rows(by_year_df: pd.DataFrame) -> List[Dict[str, object]]:
    if by_year_df.empty:
        return []
    rows: List[Dict[str, object]] = []
    for resolution in _resolution_sequence(by_year_df["resolution"].unique()):
        sub = by_year_df[by_year_df["resolution"].eq(resolution)].copy()
        if sub.empty:
            continue
        sub["active_units"] = pd.to_numeric(sub["active_units"], errors="coerce").fillna(0)
        sub["records_any"] = pd.to_numeric(sub["records_any"], errors="coerce").fillna(0)
        peak_units = sub.sort_values(["active_units", "year"], ascending=[False, True], kind="mergesort").iloc[0]
        peak_records = sub.sort_values(["records_any", "year"], ascending=[False, True], kind="mergesort").iloc[0]
        rows.append(
            {
                "resolution": resolution,
                "years": "{}-{}".format(int(pd.to_numeric(sub["year"], errors="coerce").min()), int(pd.to_numeric(sub["year"], errors="coerce").max())),
                "peak_units": "{} in {}".format(_format_int(peak_units.get("active_units", 0)), int(peak_units.get("year", 0))),
                "peak_records": "{} in {}".format(_format_int(peak_records.get("records_any", 0)), int(peak_records.get("year", 0))),
                "total_records": _format_int(sub["records_any"].sum()),
            }
        )
    return rows


def _compact_long_record_rows(unit_df: pd.DataFrame) -> List[Dict[str, object]]:
    long_df = build_long_records_by_resolution(unit_df)
    if long_df.empty:
        return []
    rows: List[Dict[str, object]] = []
    for _, row in long_df.iterrows():
        rows.append(
            {
                "resolution": row.get("resolution", ""),
                "units": _format_int(row.get("n_units", 0)),
                "median": _format_report_float(row.get("median_record_length_years")),
                "max": _format_report_float(row.get("max_record_length_years")),
                "gt10": "{} ({})".format(_format_int(row.get("n_gt_10_years", 0)), _format_report_pct(row.get("pct_gt_10_years", 0))),
                "gt30": "{} ({})".format(_format_int(row.get("n_gt_30_years", 0)), _format_report_pct(row.get("pct_gt_30_years", 0))),
                "gt50": "{} ({})".format(_format_int(row.get("n_gt_50_years", 0)), _format_report_pct(row.get("pct_gt_50_years", 0))),
                "gt100": "{} ({})".format(_format_int(row.get("n_gt_100_years", 0)), _format_report_pct(row.get("pct_gt_100_years", 0))),
            }
        )
    return rows


def _compact_temporal_source_rows(source_df: pd.DataFrame, top_n: int = 15) -> List[Dict[str, object]]:
    if source_df.empty:
        return []
    work = source_df.copy()
    if "record_count" not in work.columns:
        work["record_count"] = work.get("record_count_any", 0)
    if "clusters" not in work.columns:
        work["clusters"] = work.get("linked_clusters", "")
    work["record_count"] = pd.to_numeric(work.get("record_count", 0), errors="coerce").fillna(0)
    work["_clusters_num"] = pd.to_numeric(work.get("clusters", 0), errors="coerce").fillna(0)
    work = work.sort_values(["record_count", "_clusters_num"], ascending=[False, False], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "source": row.get("source_name", row.get("source", "")),
                "resolution": row.get("resolution", ""),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "stations": _format_int(row.get("source_stations", row.get("satellite_stations", row.get("stations", 0)))),
                "clusters": _format_int(row.get("clusters", row.get("linked_clusters", ""))),
                "records": _format_int(row.get("record_count", row.get("record_count_any", 0))),
                "median_years": _format_report_float(row.get("median_record_length_years")),
                "max_years": _format_report_float(row.get("max_record_length_years")),
            }
        )
    return rows


def _compact_region_temporal_rows(region_df: pd.DataFrame, top_n: int = 15) -> List[Dict[str, object]]:
    if region_df.empty:
        return []
    work = region_df.copy()
    work["record_count"] = pd.to_numeric(work.get("record_count", 0), errors="coerce").fillna(0)
    work = work.sort_values(["record_count", "clusters"], ascending=[False, False], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "region": row.get("continent_region", ""),
                "country": row.get("country", ""),
                "resolution": row.get("resolution", ""),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "clusters": _format_int(row.get("clusters", 0)),
                "records": _format_int(row.get("record_count", 0)),
                "median_years": _format_report_float(row.get("median_record_length_years")),
                "max_years": _format_report_float(row.get("max_record_length_years")),
            }
        )
    return rows


def write_article_summary(
    out_path: Path,
    summary_rows: List[Dict[str, object]],
    by_year_df: pd.DataFrame,
    unit_df: pd.DataFrame,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df = pd.DataFrame(add_length_summary_columns(summary_rows, unit_df))

    if len(unit_df) > 0:
        all_first = pd.to_datetime(unit_df["first_date"], errors="coerce").min()
        all_last = pd.to_datetime(unit_df["last_date"], errors="coerce").max()
        all_first_year = _year_value(all_first)
        all_last_year = _year_value(all_last)
    else:
        all_first_year = ""
        all_last_year = ""

    daily = _summary_row(summary_df, "daily")
    monthly = _summary_row(summary_df, "monthly")
    annual = _summary_row(summary_df, "annual")
    climatology = _summary_row(summary_df, "climatology")

    lines = [
        "# S8 temporal coverage statistics for ESSD",
        "",
        "## Manuscript-ready summary",
        "",
        (
            "The release provides daily, monthly, annual, and climatological sediment-reference "
            "products with temporal coverage spanning from {} to {} across all products."
        ).format(all_first_year, all_last_year),
        "",
    ]

    if daily and monthly and annual:
        lines.append(
            "The main time-series products contain {} daily clusters, {} monthly clusters, and {} annual clusters.".format(
                _format_int(daily.get("active_units", 0)),
                _format_int(monthly.get("active_units", 0)),
                _format_int(annual.get("active_units", 0)),
            )
        )
        lines.append("")

    for resolution, row, noun in (
        ("daily", daily, "daily"),
        ("monthly", monthly, "monthly"),
        ("annual", annual, "annual"),
    ):
        if not row:
            continue
        extra = ""
        if resolution == "annual":
            extra = (
                " Annual coverage is described by observed records and calendar span, "
                "rather than by a regular-grid coverage ratio, because the annual time axis is irregular."
            )
        lines.append(
            "{} records span {}-{}, with {} valid cluster-time observations across {} clusters and a median record length of {:.1f} years.{}".format(
                noun.capitalize(),
                row.get("first_year", ""),
                row.get("last_year", ""),
                _format_int(row.get("record_count_any", row.get("records_any", 0))),
                _format_int(row.get("active_units", 0)),
                float(row.get("median_record_length_years", np.nan)),
                extra,
            )
        )
        lines.append("")

    if daily:
        lines.append(
            "Long daily records are a major strength of the release: {} daily clusters are longer than 50 years and {} daily clusters are longer than 100 years.".format(
                _format_int(daily.get("n_gt_50_years", 0)),
                _format_int(daily.get("n_gt_100_years", 0)),
            )
        )
        lines.append("")

    peaks = []
    for resolution in MATRIX_RESOLUTIONS:
        peaks.append("{}: {}".format(resolution, _peak_active_text(by_year_df, resolution)))
    lines.append("Peak temporal coverage differs by product: {}.".format("; ".join(peaks)))
    lines.append("")

    if climatology:
        lines.append(
            "The climatology product is reported separately as {} standalone climatology stations, because it is not a basin-cluster time-series matrix.".format(
                _format_int(climatology.get("active_units", 0))
            )
        )
        lines.append("")

    lines.extend(
        [
            "## Output tables",
            "",
            "- `tables/table_temporal_coverage_by_resolution.csv`",
            "- `tables/table_temporal_coverage_by_variable.csv`",
            "- `tables/table_active_units_by_year.csv`",
            "- `tables/table_record_length_distribution.csv`",
            "- `tables/table_temporal_coverage_record_lengths_by_unit.csv`",
            "- `tables/table_long_records_by_resolution.csv`",
            "- `tables/table_temporal_coverage_by_source.csv`",
            "- `tables/table_temporal_coverage_by_region_resolution.csv`",
            "",
            "## Output figures",
            "",
            "- `figures/fig_active_units_by_year.png` and `.pdf`",
            "- `figures/fig_records_by_year_variable.png` and `.pdf`",
            "- `figures/fig_record_length_distribution.png` and `.pdf`",
            "- `figures/fig_long_record_counts.png` and `.pdf`",
            "- `figures/fig_temporal_coverage_heatmap.png` and `.pdf`",
            "- `figures/fig_source_temporal_span.png` and `.pdf`",
        ]
    )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def write_article_report(
    out_path: Path,
    summary_rows: List[Dict[str, object]],
    by_year_df: pd.DataFrame,
    unit_df: pd.DataFrame,
    extra_tables: Optional[Dict[str, pd.DataFrame]] = None,
) -> Path:
    extra_tables = extra_tables or {}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary_df = pd.DataFrame(add_length_summary_columns(summary_rows, unit_df))
    daily = _summary_row(summary_df, "daily")
    monthly = _summary_row(summary_df, "monthly")
    annual = _summary_row(summary_df, "annual")
    climatology = _summary_row(summary_df, "climatology")
    satellite_rows = summary_df[summary_df["product"].fillna("") == SATELLITE_PRODUCT].copy() if "product" in summary_df.columns else pd.DataFrame()

    clim_summary = extra_tables.get("climatology_temporal_summary", pd.DataFrame())
    sat_summary = extra_tables.get("satellite_temporal_summary", pd.DataFrame())
    sat_source = extra_tables.get("satellite_by_source", pd.DataFrame())
    by_variable = build_temporal_coverage_by_variable(by_year_df, summary_rows)
    by_source = build_temporal_coverage_by_source()
    by_region = build_temporal_coverage_by_region_resolution()

    lines = [
        "# Temporal coverage results for the S8 ESSD release",
        "",
        "## Overview",
        "",
        (
            "The temporal coverage statistics are reported for three product groups: "
            "the basin-cluster time-series matrices (daily, monthly, and annual), "
            "the standalone climatology stations, and the satellite-validation product. "
            "These groups use different statistical units and should therefore be described separately."
        ),
        "",
        "## Main Time-Series Products",
        "",
        "| Product | Unit | First year | Last year | Units | Records | Median length (yr) | Max length (yr) | >50 yr | >100 yr |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in (daily, monthly, annual):
        if not row:
            continue
        lines.append(
            "| {resolution} | {unit_type} | {first_year} | {last_year} | {active_units} | {records} | {median:.1f} | {maxv:.1f} | {gt50} | {gt100} |".format(
                resolution=row.get("resolution", ""),
                unit_type=row.get("unit_type", ""),
                first_year=row.get("first_year", ""),
                last_year=row.get("last_year", ""),
                active_units=_format_int(row.get("active_units", 0)),
                records=_format_int(row.get("record_count_any", row.get("records_any", 0))),
                median=float(row.get("median_record_length_years", np.nan)),
                maxv=float(row.get("max_record_length_years", np.nan)),
                gt50=_format_int(row.get("n_gt_50_years", 0)),
                gt100=_format_int(row.get("n_gt_100_years", 0)),
            )
        )
    lines.extend(
        [
            "",
            "### Product-Level Coverage Detail",
            "",
            _markdown_table(
                _compact_temporal_summary_rows(summary_df),
                ["resolution", "product", "unit_type", "span", "units", "clusters", "records", "median_years", "max_years", "gt50", "gt100"],
                ["Resolution", "Product", "Unit", "Span", "Units", "Clusters", "Records", "Median yr", "Max yr", ">50 yr", ">100 yr"],
            ),
            "",
            "### Variable Coverage",
            "",
            _markdown_table(
                _compact_temporal_variable_rows(by_variable),
                ["resolution", "variable", "span", "active_units", "records", "peak_units", "peak_records"],
                ["Resolution", "Variable", "Span", "Active units", "Records", "Peak active units", "Peak records"],
            ),
            "",
            "### Yearly Peaks",
            "",
            _markdown_table(
                _compact_year_peak_rows(by_year_df),
                ["resolution", "years", "peak_units", "peak_records", "total_records"],
                ["Resolution", "Years", "Peak active units", "Peak records", "Total records"],
            ),
            "",
            "### Long-Record Diagnostics",
            "",
            _markdown_table(
                _compact_long_record_rows(unit_df),
                ["resolution", "units", "median", "max", "gt10", "gt30", "gt50", "gt100"],
                ["Resolution", "Units", "Median yr", "Max yr", ">10 yr", ">30 yr", ">50 yr", ">100 yr"],
            ),
            "",
            "Daily coverage is the strongest long-record component of the release, with "
            "{} clusters longer than 50 years and {} clusters longer than 100 years. Monthly coverage has many clusters but shorter median spans, while annual coverage contains fewer clusters but includes several very long records.".format(
                _format_int(daily.get("n_gt_50_years", 0) if daily else 0),
                _format_int(daily.get("n_gt_100_years", 0) if daily else 0),
            ),
            "",
            "Peak active coverage occurs at {} for daily, {} for monthly, and {} for annual products.".format(
                _peak_active_text(by_year_df, "daily"),
                _peak_active_text(by_year_df, "monthly"),
                _peak_active_text(by_year_df, "annual"),
            ),
            "",
            "## Source and Regional Temporal Coverage",
            "",
            "### Top Source-Resolution Contributions",
            "",
            _markdown_table(
                _compact_temporal_source_rows(by_source, top_n=15),
                ["source", "resolution", "span", "stations", "clusters", "records", "median_years", "max_years"],
                ["Source", "Resolution", "Span", "Stations", "Clusters", "Records", "Median yr", "Max yr"],
            ),
            "",
            "### Top Region-Resolution Contributions",
            "",
            _markdown_table(
                _compact_region_temporal_rows(by_region, top_n=15),
                ["region", "country", "resolution", "span", "clusters", "records", "median_years", "max_years"],
                ["Region", "Country", "Resolution", "Span", "Clusters", "Records", "Median yr", "Max yr"],
            ),
            "",
            "These source and region tables separate record volume from span length. A source can dominate total records through dense daily sampling even when its spatial footprint is narrower than a source with many short station records.",
            "",
            "## Climatology Product",
            "",
        ]
    )
    if climatology:
        lines.append(
            "The climatology product contains {} standalone stations spanning {}-{}. It is not a basin-cluster time-series matrix, so it is summarized separately from the daily/monthly/annual products.".format(
                _format_int(climatology.get("active_units", 0)),
                climatology.get("first_year", ""),
                climatology.get("last_year", ""),
            )
        )
        lines.append("")
        if len(clim_summary):
            row = clim_summary.iloc[0]
            lines.append(
                "Variable coverage in the climatology product includes {} Q stations, {} SSC stations, and {} SSL stations across {} sources.".format(
                    _format_int(row.get("stations_Q", 0)),
                    _format_int(row.get("stations_SSC", 0)),
                    _format_int(row.get("stations_SSL", 0)),
                    _format_int(row.get("sources", 0)),
                )
            )
            lines.append("")

    lines.extend(["## Satellite Validation Product", ""])
    if len(satellite_rows):
        sat_units = int(pd.to_numeric(satellite_rows["active_units"], errors="coerce").fillna(0).sum())
        sat_clusters = int(pd.to_numeric(satellite_rows.get("active_clusters", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        sat_records = int(pd.to_numeric(satellite_rows["record_count_any"], errors="coerce").fillna(0).sum())
        sat_first = int(pd.to_numeric(satellite_rows["first_year"], errors="coerce").min())
        sat_last = int(pd.to_numeric(satellite_rows["last_year"], errors="coerce").max())
        sat_resolution_text = ", ".join(
            "{} ({})".format(row["resolution"], _format_int(row["active_units"]))
            for _, row in satellite_rows.sort_values("resolution").iterrows()
        )
        lines.append(
            "The satellite-validation product is summarized with satellite-prefixed resolution labels to distinguish it from the main matrices. It contains {} satellite station-resolution rows linked to approximately {} basin clusters, with {} records spanning {}-{}. Resolution-specific rows are: {}.".format(
                _format_int(sat_units),
                _format_int(sat_clusters),
                _format_int(sat_records),
                sat_first,
                sat_last,
                sat_resolution_text,
            )
        )
        lines.append("")
    if len(sat_summary):
        row = sat_summary.iloc[0]
        lines.append(
            "The satellite summary reports {} sources/source families and uses station-level catalog spans; when NetCDF scanning is enabled, Q/SSC/SSL record counts and annual active units are computed directly from `sed_reference_satellite.nc`.".format(
                _format_int(row.get("sources", sat_source["source"].nunique() if len(sat_source) else 0))
            )
        )
        lines.append("")

    lines.extend(
        [
            "### Climatology Source Detail",
            "",
            _markdown_table(
                _compact_temporal_source_rows(extra_tables.get("climatology_by_source", pd.DataFrame()), top_n=12),
                ["source", "resolution", "span", "stations", "clusters", "records", "median_years", "max_years"],
                ["Source", "Resolution", "Span", "Stations", "Clusters", "Records", "Median yr", "Max yr"],
            ),
            "",
            "### Satellite Source Detail",
            "",
            _markdown_table(
                _compact_temporal_source_rows(sat_source, top_n=12),
                ["source", "resolution", "span", "stations", "clusters", "records", "median_years", "max_years"],
                ["Source", "Resolution", "Span", "Stations", "Clusters", "Records", "Median yr", "Max yr"],
            ),
            "",
            "## Interpretation Notes",
            "",
            "- Daily, monthly, annual, climatology, and satellite products use different units; compare trends within product groups before comparing across product groups.",
            "- `active_units` measures whether a unit has at least one valid record in a year, while `records` measures sampling density.",
            "- Long-record counts are useful evidence for model evaluation, but sparse annual records should be interpreted by record count and calendar span together.",
            "",
        ]
    )

    lines.extend(
        [
            "## Recommended ESSD Use",
            "",
            "- Main text: use `fig_active_units_by_year`, `fig_record_length_distribution`, and `fig_temporal_coverage_heatmap` for daily/monthly/annual coverage.",
            "- Climatology: use `fig_climatology_variable_coverage` and `fig_climatology_record_length_distribution` in a separate climatology paragraph or supplement.",
            "- Satellite validation: use `fig_satellite_active_units_by_year`, `fig_satellite_records_by_year_variable`, and `fig_satellite_source_contribution` in the validation/supplement section.",
            "- Tables: use `table_temporal_coverage_by_resolution.csv` as the compact master table; use the climatology and satellite dedicated tables for supplementary material.",
            "",
            "## Output Files",
            "",
            "- `tables/table_climatology_temporal_summary.csv`",
            "- `tables/table_climatology_by_source.csv`",
            "- `tables/table_climatology_record_lengths_by_station.csv`",
            "- `tables/table_satellite_temporal_summary.csv`",
            "- `tables/table_satellite_by_year.csv`",
            "- `tables/table_satellite_by_source.csv`",
            "- `tables/table_satellite_record_lengths_by_station.csv`",
            "- `tables/table_satellite_by_linked_cluster.csv`",
        ]
    )
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


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
        "--summary-path",
        default="output_other/temporal_coverage_stats/article_temporal_coverage_summary.md",
        help="Output manuscript-ready Markdown summary. Relative paths are resolved under the script/repository root.",
    )
    ap.add_argument(
        "--report-path",
        default="output_other/temporal_coverage_stats/article_temporal_coverage_report.md",
        help="Output detailed ESSD temporal coverage report. Relative paths are resolved under the script/repository root.",
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
        "--exclude-satellite",
        action="store_true",
        help="Exclude satellite-validation temporal statistics and figures (included by default).",
    )
    ap.add_argument(
        "--skip-satellite-nc",
        action="store_true",
        help="Use satellite_catalog.csv only; do not scan the large satellite NetCDF record table.",
    )
    ap.add_argument(
        "--satellite-chunk-records",
        type=int,
        default=1000000,
        help="Number of satellite flat records read per chunk when scanning sed_reference_satellite.nc.",
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
    extra_tables: Dict[str, pd.DataFrame] = {}

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

    climatology_path = input_paths.get("climatology", RELEASE_PATHS["climatology"])
    clim_summary, clim_by_source, clim_station = build_climatology_station_tables(climatology_path)
    extra_tables["climatology_temporal_summary"] = clim_summary
    extra_tables["climatology_by_source"] = clim_by_source
    extra_tables["climatology_record_lengths_by_station"] = clim_station

    if not args.exclude_satellite:
        satellite_nc_path, satellite_catalog_path = resolve_satellite_paths(args)
        if satellite_catalog_path is None:
            print("Warning: satellite_catalog.csv not found; satellite statistics will be skipped.", file=sys.stderr)
        else:
            print("Reading satellite catalog: {}".format(satellite_catalog_path))
            satellite_catalog = _satellite_catalog(satellite_catalog_path)
            sat_by_source, sat_by_cluster, sat_station_df = build_satellite_catalog_tables(satellite_catalog)
            sat_by_year = build_satellite_by_year_from_catalog(satellite_catalog)
            sat_variable_df = pd.DataFrame()
            sat_summaries = build_satellite_summaries_from_catalog(satellite_catalog, sat_station_df)

            if not args.skip_satellite_nc:
                if satellite_nc_path is None:
                    print("Warning: sed_reference_satellite.nc not found; using satellite catalog only.", file=sys.stderr)
                else:
                    print("Reading satellite NetCDF by chunks: {}".format(satellite_nc_path))
                    nc_summary, nc_by_year, nc_variable_df, station_counts = scan_satellite_nc(
                        satellite_nc_path,
                        satellite_catalog,
                        chunk_records=args.satellite_chunk_records,
                    )
                    if nc_summary:
                        sat_summaries = nc_summary
                    if len(nc_by_year):
                        sat_by_year = nc_by_year
                    if len(nc_variable_df):
                        sat_variable_df = nc_variable_df
                    if len(station_counts) and len(sat_station_df):
                        sat_station_df = sat_station_df.reset_index(drop=True).copy()
                        sat_station_df["satellite_station_index"] = np.arange(len(sat_station_df), dtype=int)
                        sat_station_df = sat_station_df.merge(station_counts, on="satellite_station_index", how="left")
                        for var in ("any", "Q", "SSC", "SSL"):
                            nc_col = "record_count_{}_nc".format(var)
                            out_col = "record_count_{}".format(var)
                            if nc_col in sat_station_df.columns:
                                sat_station_df[out_col] = sat_station_df[nc_col]

            for sat_summary in sat_summaries:
                sat_summary["source_file"] = str(satellite_nc_path or satellite_catalog_path)
                summary_rows.append(sat_summary)
                print(
                    "  {} active_units={} records_any={} first={} last={}".format(
                        sat_summary.get("resolution", "satellite"),
                        sat_summary.get("active_units", ""),
                        sat_summary.get("record_count_any", sat_summary.get("records_any", "")),
                        sat_summary.get("first_date", ""),
                        sat_summary.get("last_date", ""),
                    )
                )
            if len(sat_by_year):
                by_year_frames.append(sat_by_year)
            if len(sat_station_df):
                unit_frames.append(sat_station_df)

            sat_summary_table = pd.DataFrame(sat_summaries)
            if len(sat_summary_table) and len(satellite_catalog):
                sources = []
                families = []
                catalog_stations = []
                catalog_records = []
                for resolution in sat_summary_table["resolution"]:
                    sub = satellite_catalog[satellite_catalog["output_resolution"] == resolution]
                    sources.append(int(sub["source"].replace("", np.nan).dropna().nunique()))
                    families.append(int(sub["source_family"].replace("", np.nan).dropna().nunique()))
                    catalog_stations.append(int(len(sub)))
                    catalog_records.append(int(sub["n_records"].sum()))
                sat_summary_table["sources"] = sources
                sat_summary_table["source_families"] = families
                sat_summary_table["catalog_stations"] = catalog_stations
                sat_summary_table["catalog_record_count"] = catalog_records
            extra_tables["satellite_temporal_summary"] = sat_summary_table
            extra_tables["satellite_by_year"] = sat_by_year
            extra_tables["satellite_by_source"] = sat_by_source
            extra_tables["satellite_record_lengths_by_station"] = sat_station_df
            extra_tables["satellite_by_linked_cluster"] = sat_by_cluster
            extra_tables["satellite_by_variable"] = sat_variable_df
    else:
        extra_tables["satellite_temporal_summary"] = pd.DataFrame()
        extra_tables["satellite_by_year"] = pd.DataFrame()
        extra_tables["satellite_by_source"] = pd.DataFrame()
        extra_tables["satellite_record_lengths_by_station"] = pd.DataFrame()
        extra_tables["satellite_by_linked_cluster"] = pd.DataFrame()
        extra_tables["satellite_by_variable"] = pd.DataFrame()

    by_year_df = _ordered_concat(by_year_frames)
    unit_df = _ordered_concat(unit_frames)

    tables_dir = _resolve_under_script_root(args.tables_dir)
    table_outputs = write_tables(tables_dir, summary_rows, by_year_df, unit_df, extra_tables=extra_tables)

    print("Wrote tables:")
    for path in table_outputs.values():
        print("  {}".format(path))

    summary_path = _resolve_under_script_root(args.summary_path)
    summary_output = write_article_summary(summary_path, summary_rows, by_year_df, unit_df)
    print("Wrote manuscript summary:")
    print("  {}".format(summary_output))

    report_path = _resolve_under_script_root(args.report_path)
    report_output = write_article_report(report_path, summary_rows, by_year_df, unit_df, extra_tables=extra_tables)
    print("Wrote detailed report:")
    print("  {}".format(report_output))

    # ---------------------------------------------------------------------------
    # Copy markdown outputs to doc/reports
    # ---------------------------------------------------------------------------
    docs_reports_dir = SCRIPT_ROOT / "docs" / "reports"
    for md_path in (summary_output, report_output):
        try:
            shutil.copy2(md_path, docs_reports_dir)
            print("Copied {} -> {}".format(md_path, docs_reports_dir))
        except Exception as exc:
            print("Warning: could not copy {} to {}: {}".format(md_path, docs_reports_dir, exc), file=sys.stderr)

    if not args.skip_figures:
        figures_dir = _resolve_under_script_root(args.figures_dir)
        try:
            figure_outputs = write_figures(
                figures_dir,
                by_year_df,
                unit_df,
                include_climatology_in_yearly_plot=args.include_climatology_in_yearly_plot,
                extra_tables=extra_tables,
            )
            print("Wrote figures:")
            for path in figure_outputs.values():
                print("  {}".format(path.with_suffix(".png")))
                print("  {}".format(path.with_suffix(".pdf")))
        except ImportError as exc:
            print("Error: matplotlib is required to write figures: {}".format(exc), file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
