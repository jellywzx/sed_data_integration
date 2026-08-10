#!/usr/bin/env python3
"""Audit calendar-period observation counts in the final minimal S8 release.

This diagnostic reads the final user-facing products under
``scripts_basin_test/output/sed_reference_release_minimal`` and checks, for
EVERY released matrix cluster (and every standalone climatology station), how
many populated observations fall in each calendar day, month, and year.

The main purpose is to answer two temporal-resolution integrity questions:

1. Does a released ``monthly`` cluster contain more than one observation in the
   same calendar month?
2. Does a released ``annual`` cluster contain more than one observation in the
   same calendar year?

Those two cases are written both as cluster-period detail tables and as
source-level summaries.  Source attribution for the matrix products is taken
from ``selected_source_station_uid`` and joined to
``source_station_catalog.csv``.  Thus the audit diagnoses the FINAL selected
records, not merely the source-station inventory.

By default, a populated output record is defined exactly as the sediment-
oriented release logic: at least one non-missing SSC or SSL value is present.
Use ``--record-mode any`` to count cells with any non-missing Q, SSC, or SSL.

Default inputs
--------------
  scripts_basin_test/output/sed_reference_release_minimal/
    sed_reference_timeseries_daily.nc
    sed_reference_timeseries_monthly.nc
    sed_reference_timeseries_annual.nc
    sed_reference_climatology.nc
    source_station_catalog.csv

Default outputs
---------------
  scripts_basin_test/output/temporal_resolution_integrity_audit/
    temporal_period_counts_by_unit.csv
    product_integrity_summary.csv
    daily_gt1_record_per_day.csv
    monthly_gt1_record_per_month.csv
    monthly_gt1_record_per_month_by_source.csv
    monthly_gt1_by_source.csv
    annual_gt1_record_per_year.csv
    annual_gt1_record_per_year_by_source.csv
    annual_gt1_by_source.csv
    temporal_resolution_integrity_report.md

Examples
--------
  python3 tools/audit_temporal_resolution_integrity.py

  python3 tools/audit_temporal_resolution_integrity.py \
    --release-dir scripts_basin_test/output/sed_reference_release_minimal \
    --out-dir scripts_basin_test/output/temporal_resolution_integrity_audit

  python3 tools/audit_temporal_resolution_integrity.py --fail-on-violations
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError as exc:  # pragma: no cover - runtime dependency
    raise SystemExit("netCDF4 is required for this audit: {}".format(exc))


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import get_output_r_root  # noqa: E402


OUTPUT_R_ROOT = get_output_r_root(REPO_ROOT)
DEFAULT_RELEASE_DIR = OUTPUT_R_ROOT / "scripts_basin_test/output/sed_reference_release_minimal"
DEFAULT_OUT_DIR = OUTPUT_R_ROOT / "scripts_basin_test/output/temporal_resolution_integrity_audit"

MATRIX_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
CLIMATOLOGY_FILE = "sed_reference_climatology.nc"
SOURCE_STATION_CATALOG = "source_station_catalog.csv"
CORE_VARS = ("Q", "SSC", "SSL")
SEDIMENT_VARS = ("SSC", "SSL")
FILL_FLOAT_VALUES = (-9999.0, -9999)

UNIT_COLUMNS = [
    "product",
    "resolution",
    "unit_type",
    "unit_id",
    "station_name",
    "river_name",
    "lat",
    "lon",
    "source_names",
    "first_observation_date",
    "last_observation_date",
    "record_count",
    "declared_n_valid_time_steps",
    "record_count_delta_vs_declared",
    "unique_observed_days",
    "unique_observed_months",
    "unique_observed_years",
    "max_records_per_day",
    "max_records_per_month",
    "max_records_per_year",
    "n_days_gt1",
    "n_months_gt1",
    "n_years_gt1",
    "native_period",
    "max_records_per_native_period",
    "n_native_periods_gt1",
    "native_resolution_violation",
]

PERIOD_DETAIL_COLUMNS = [
    "resolution",
    "unit_type",
    "unit_id",
    "station_name",
    "river_name",
    "lat",
    "lon",
    "calendar_period",
    "period_record_count",
    "extra_records_beyond_one",
    "n_unique_dates_in_period",
    "record_dates",
    "source_names",
    "source_record_counts_json",
    "selected_source_station_uids",
]

PERIOD_SOURCE_COLUMNS = [
    "resolution",
    "unit_type",
    "unit_id",
    "station_name",
    "river_name",
    "lat",
    "lon",
    "calendar_period",
    "period_record_count",
    "extra_records_beyond_one",
    "source_name",
    "source_record_count",
    "source_station_uids",
    "record_dates",
]

SOURCE_SUMMARY_COLUMNS = [
    "resolution",
    "source_name",
    "offending_clusters",
    "offending_periods_involving_source",
    "source_records_in_offending_periods",
    "periods_where_source_itself_has_gt1_record",
    "max_total_records_in_offending_period",
    "max_source_records_in_offending_period",
]

PRODUCT_SUMMARY_COLUMNS = [
    "product",
    "resolution",
    "unit_type",
    "units_with_records",
    "record_count",
    "max_records_per_day",
    "max_records_per_month",
    "max_records_per_year",
    "native_period",
    "units_with_native_period_gt1",
    "offending_native_periods",
    "max_records_per_native_period",
]


def banner(text: str) -> None:
    print("\n" + "=" * 78)
    print("  {}".format(text))
    print("=" * 78)


def log(text: str) -> None:
    print("  {}".format(text))


def _resolve_path(value: str, base: Path = OUTPUT_R_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if path.exists():
        return path.resolve()
    return (base / path).resolve()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Directory containing the final minimal release products.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for audit CSV/Markdown outputs.",
    )
    parser.add_argument(
        "--record-mode",
        choices=("sediment", "any"),
        default="sediment",
        help=(
            "Record definition. 'sediment' = SSC or SSL non-missing (default, "
            "matching the release eligibility rule); 'any' = Q or SSC or SSL non-missing."
        ),
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=32,
        help="Number of matrix clusters read per chunk (default: 32).",
    )
    parser.add_argument(
        "--fail-on-violations",
        action="store_true",
        help="Exit with status 2 when monthly >1/month or annual >1/year is found.",
    )
    args = parser.parse_args(argv)
    args.release_dir = _resolve_path(args.release_dir)
    args.out_dir = _resolve_path(args.out_dir)
    if args.chunk_rows < 1:
        parser.error("--chunk-rows must be >= 1")
    return args


def _clean_text(value) -> str:
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, (bytes, np.bytes_)):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).replace("\x00", "").strip()
    return "" if text.lower() in {"", "nan", "none", "nat", "null", "<na>"} else text


def _decode_text_values(values, expected_len: Optional[int] = None) -> List[str]:
    """Decode NetCDF vlen strings or fixed-width char arrays."""
    arr = np.ma.asarray(values)
    if np.ma.isMaskedArray(arr):
        if arr.dtype.kind in {"S", "U", "O"}:
            fill = b"" if arr.dtype.kind == "S" else ""
            arr = arr.filled(fill)
        else:
            arr = arr.filled("")
    arr = np.asarray(arr)

    decoded = None
    if arr.dtype.kind in {"S", "U"} and arr.ndim >= 2:
        try:
            decoded = np.asarray(nc4.chartostring(arr)).reshape(-1)
        except Exception:
            decoded = None
    if decoded is None:
        decoded = arr.reshape(-1)

    out = [_clean_text(value) for value in decoded]
    if expected_len is not None:
        if len(out) < expected_len:
            out.extend([""] * (expected_len - len(out)))
        elif len(out) > expected_len:
            out = out[:expected_len]
    return out


def _read_text_1d(ds, name: str, size: int) -> List[str]:
    if name not in ds.variables:
        return [""] * size
    try:
        return _decode_text_values(ds.variables[name][:], expected_len=size)
    except Exception:
        return [""] * size


def _read_numeric_1d(ds, name: str, size: int, default=np.nan) -> np.ndarray:
    if name not in ds.variables:
        return np.full(size, default)
    try:
        arr = np.ma.asarray(ds.variables[name][:]).filled(default)
        arr = np.asarray(arr).reshape(-1)
    except Exception:
        return np.full(size, default)
    if len(arr) < size:
        arr = np.concatenate([arr, np.full(size - len(arr), default)])
    return arr[:size]


def _var_fill_values(var) -> Tuple[float, ...]:
    fills = list(FILL_FLOAT_VALUES)
    for attr in ("_FillValue", "missing_value"):
        try:
            raw = getattr(var, attr)
        except Exception:
            continue
        for item in np.asarray(raw).reshape(-1):
            try:
                value = float(item)
            except Exception:
                continue
            if math.isfinite(value) and value not in fills:
                fills.append(value)
    return tuple(fills)


def _valid_numeric_mask(var, values) -> np.ndarray:
    masked = np.ma.asarray(values)
    valid = ~np.ma.getmaskarray(masked) if np.ma.isMaskedArray(masked) else np.ones(masked.shape, dtype=bool)
    try:
        data = np.asarray(masked.filled(np.nan) if np.ma.isMaskedArray(masked) else masked, dtype=np.float64)
    except Exception:
        return np.zeros(masked.shape, dtype=bool)
    valid &= np.isfinite(data)
    for fill in _var_fill_values(var):
        valid &= data != fill
    return valid


def _decode_time(ds) -> pd.DatetimeIndex:
    if "time" not in ds.variables:
        raise ValueError("NetCDF product has no 'time' variable")
    var = ds.variables["time"]
    raw = np.asarray(var[:]).reshape(-1)
    units = getattr(var, "units", "days since 1970-01-01")
    calendar = getattr(var, "calendar", "gregorian")
    try:
        decoded = nc4.num2date(
            raw,
            units=units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
        )
    except TypeError:
        decoded = nc4.num2date(raw, units=units, calendar=calendar)

    values = []
    for item in decoded:
        try:
            values.append(pd.Timestamp(item))
        except Exception:
            try:
                values.append(pd.Timestamp(item.isoformat()))
            except Exception:
                values.append(pd.NaT)
    return pd.DatetimeIndex(values)


def _calendar_keys(dates: pd.DatetimeIndex) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    day = np.asarray([ts.strftime("%Y-%m-%d") if pd.notna(ts) else "" for ts in dates], dtype=object)
    month = np.asarray([ts.strftime("%Y-%m") if pd.notna(ts) else "" for ts in dates], dtype=object)
    year = np.asarray([ts.strftime("%Y") if pd.notna(ts) else "" for ts in dates], dtype=object)
    return day, month, year, day.copy()


def _period_counts(keys: np.ndarray, cols: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    if len(cols) == 0:
        return np.asarray([], dtype=object), np.asarray([], dtype=np.int64)
    selected = np.asarray(keys, dtype=object)[cols]
    selected = selected[selected != ""]
    if selected.size == 0:
        return np.asarray([], dtype=object), np.asarray([], dtype=np.int64)
    unique, counts = np.unique(selected, return_counts=True)
    return unique.astype(object), counts.astype(np.int64)


def _count_stats(keys: np.ndarray, cols: np.ndarray) -> Tuple[int, int, int]:
    unique, counts = _period_counts(keys, cols)
    if counts.size == 0:
        return 0, 0, 0
    return int(len(unique)), int(counts.max()), int(np.count_nonzero(counts > 1))


def _native_period(resolution: str) -> str:
    return {"daily": "day", "monthly": "month", "annual": "year"}.get(resolution, "")


def _native_fields(resolution: str, day_stats, month_stats, year_stats) -> Tuple[int, int, bool]:
    stats = {"daily": day_stats, "monthly": month_stats, "annual": year_stats}.get(resolution)
    if stats is None:
        return 0, 0, False
    _, max_count, n_gt1 = stats
    return int(max_count), int(n_gt1), bool(n_gt1 > 0)


def _matrix_n_units(ds) -> int:
    if "n_stations" in ds.dimensions:
        return int(len(ds.dimensions["n_stations"]))
    for name in ("SSC", "SSL", "Q"):
        if name in ds.variables and ds.variables[name].ndim >= 2:
            return int(ds.variables[name].shape[0])
    raise ValueError("Cannot determine matrix station/cluster dimension")


def _matrix_valid_chunk(ds, start: int, stop: int, n_time: int, record_mode: str) -> np.ndarray:
    var_names = SEDIMENT_VARS if record_mode == "sediment" else CORE_VARS
    valid = np.zeros((stop - start, n_time), dtype=bool)
    found = False
    for name in var_names:
        if name not in ds.variables:
            continue
        var = ds.variables[name]
        values = var[start:stop, :]
        mask = _valid_numeric_mask(var, values)
        if mask.shape != valid.shape:
            raise ValueError("Unexpected {} shape {} in matrix; expected {}".format(name, mask.shape, valid.shape))
        valid |= mask
        found = True
    if not found:
        raise ValueError("None of record variables {} found".format(", ".join(var_names)))
    return valid


def _selected_uids_for_columns(var, row_index: int, columns: Sequence[int]) -> Dict[int, str]:
    cols = np.asarray(sorted(set(int(v) for v in columns)), dtype=np.int64)
    if cols.size == 0:
        return {}
    try:
        if var.ndim == 2:
            raw = var[row_index, cols]
        elif var.ndim >= 3:
            raw = var[row_index, cols, ...]
        else:
            return {}
        decoded = _decode_text_values(raw, expected_len=len(cols))
        return {int(col): decoded[idx] for idx, col in enumerate(cols)}
    except Exception:
        # Conservative fallback: read one whole row only for a cluster that is
        # already known to contain an offending period.
        try:
            if var.ndim == 2:
                raw = var[row_index, :]
            else:
                raw = var[row_index, :, ...]
            row_values = _decode_text_values(raw)
            return {int(col): row_values[int(col)] if int(col) < len(row_values) else "" for col in cols}
        except Exception:
            return {}


def _load_source_lookup(release_dir: Path):
    path = release_dir / SOURCE_STATION_CATALOG
    if not path.is_file():
        log("WARNING: {} missing; source attribution will be UNKNOWN".format(path))
        return {}, {}
    df = pd.read_csv(path, keep_default_na=False, low_memory=False)
    required = {"source_station_uid", "source_name"}
    if not required.issubset(df.columns):
        log("WARNING: source_station_catalog.csv lacks {}; source attribution will be UNKNOWN".format(sorted(required - set(df.columns))))
        return {}, {}

    exact = {}
    uid_sources = defaultdict(set)
    for _, row in df.iterrows():
        uid = _clean_text(row.get("source_station_uid", ""))
        source = _clean_text(row.get("source_name", "")) or "UNKNOWN"
        resolution = _clean_text(row.get("resolution", "")).lower()
        if not uid:
            continue
        uid_sources[uid].add(source)
        if resolution:
            exact[(uid, resolution)] = source
    fallback = {uid: next(iter(values)) for uid, values in uid_sources.items() if len(values) == 1}
    return exact, fallback


def _source_for_uid(uid: str, resolution: str, exact: Mapping, fallback: Mapping) -> str:
    uid = _clean_text(uid)
    if not uid:
        return "UNKNOWN"
    return exact.get((uid, resolution), fallback.get(uid, "UNKNOWN"))


def _period_detail_for_matrix(
    resolution: str,
    row_idx: int,
    unit_id: str,
    metadata: Mapping[str, object],
    cols: np.ndarray,
    period_keys: np.ndarray,
    day_keys: np.ndarray,
    selected_uid_var,
    exact_source: Mapping,
    fallback_source: Mapping,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    unique, counts = _period_counts(period_keys, cols)
    offending_periods = unique[counts > 1]
    if len(offending_periods) == 0:
        return [], []

    offending_cols = []
    for period in offending_periods:
        offending_cols.extend(int(col) for col in cols[np.asarray(period_keys[cols], dtype=object) == period])
    uid_by_col = (
        _selected_uids_for_columns(selected_uid_var, row_idx, offending_cols)
        if selected_uid_var is not None
        else {}
    )

    detail_rows = []
    source_rows = []
    for period in offending_periods:
        period_cols = cols[np.asarray(period_keys[cols], dtype=object) == period]
        period_cols = np.asarray(period_cols, dtype=np.int64)
        record_dates = [str(day_keys[col]) for col in period_cols if str(day_keys[col])]
        source_station_uids = [_clean_text(uid_by_col.get(int(col), "")) for col in period_cols]
        source_names = [
            _source_for_uid(uid, resolution, exact_source, fallback_source)
            for uid in source_station_uids
        ]
        source_counts = Counter(source_names)
        n_period = int(len(period_cols))

        detail_rows.append(
            {
                "resolution": resolution,
                "unit_type": "cluster",
                "unit_id": unit_id,
                "station_name": metadata.get("station_name", ""),
                "river_name": metadata.get("river_name", ""),
                "lat": metadata.get("lat", np.nan),
                "lon": metadata.get("lon", np.nan),
                "calendar_period": str(period),
                "period_record_count": n_period,
                "extra_records_beyond_one": n_period - 1,
                "n_unique_dates_in_period": len(set(record_dates)),
                "record_dates": "|".join(record_dates),
                "source_names": "|".join(sorted(source_counts)),
                "source_record_counts_json": json.dumps(dict(sorted(source_counts.items())), ensure_ascii=False, sort_keys=True),
                "selected_source_station_uids": "|".join(sorted(set(uid for uid in source_station_uids if uid))),
            }
        )

        by_source_uid = defaultdict(set)
        by_source_dates = defaultdict(list)
        for col, uid, source in zip(period_cols, source_station_uids, source_names):
            if uid:
                by_source_uid[source].add(uid)
            if str(day_keys[int(col)]):
                by_source_dates[source].append(str(day_keys[int(col)]))
        for source, source_count in sorted(source_counts.items()):
            source_rows.append(
                {
                    "resolution": resolution,
                    "unit_type": "cluster",
                    "unit_id": unit_id,
                    "station_name": metadata.get("station_name", ""),
                    "river_name": metadata.get("river_name", ""),
                    "lat": metadata.get("lat", np.nan),
                    "lon": metadata.get("lon", np.nan),
                    "calendar_period": str(period),
                    "period_record_count": n_period,
                    "extra_records_beyond_one": n_period - 1,
                    "source_name": source,
                    "source_record_count": int(source_count),
                    "source_station_uids": "|".join(sorted(by_source_uid[source])),
                    "record_dates": "|".join(by_source_dates[source]),
                }
            )
    return detail_rows, source_rows


def audit_matrix_product(
    path: Path,
    resolution: str,
    record_mode: str,
    chunk_rows: int,
    exact_source: Mapping,
    fallback_source: Mapping,
):
    banner("Auditing {} matrix".format(resolution))
    log(str(path))
    if not path.is_file():
        raise FileNotFoundError(path)

    unit_rows: List[Dict[str, object]] = []
    native_detail_rows: List[Dict[str, object]] = []
    native_source_rows: List[Dict[str, object]] = []

    with nc4.Dataset(path, "r") as ds:
        dates = _decode_time(ds)
        n_time = len(dates)
        n_units = _matrix_n_units(ds)
        day_keys, month_keys, year_keys, date_strings = _calendar_keys(dates)

        cluster_uids = _read_text_1d(ds, "cluster_uid", n_units)
        station_names = _read_text_1d(ds, "station_name", n_units)
        river_names = _read_text_1d(ds, "river_name", n_units)
        lat = _read_numeric_1d(ds, "lat", n_units)
        lon = _read_numeric_1d(ds, "lon", n_units)
        declared = _read_numeric_1d(ds, "n_valid_time_steps", n_units)
        selected_uid_var = ds.variables.get("selected_source_station_uid")
        native_keys = {"daily": day_keys, "monthly": month_keys, "annual": year_keys}[resolution]

        for start in range(0, n_units, chunk_rows):
            stop = min(start + chunk_rows, n_units)
            valid_chunk = _matrix_valid_chunk(ds, start, stop, n_time, record_mode)
            for local_idx in range(stop - start):
                row_idx = start + local_idx
                cols = np.flatnonzero(valid_chunk[local_idx])
                if cols.size == 0:
                    continue

                day_stats = _count_stats(day_keys, cols)
                month_stats = _count_stats(month_keys, cols)
                year_stats = _count_stats(year_keys, cols)
                native_max, native_gt1, native_violation = _native_fields(
                    resolution, day_stats, month_stats, year_stats
                )
                first_date = str(date_strings[cols[0]]) if len(cols) else ""
                last_date = str(date_strings[cols[-1]]) if len(cols) else ""
                uid = cluster_uids[row_idx] or "unit_{:06d}".format(row_idx)
                declared_value = declared[row_idx]
                declared_int = int(declared_value) if np.isfinite(declared_value) else np.nan
                delta = int(len(cols) - declared_int) if record_mode == "sediment" and np.isfinite(declared_value) else np.nan

                metadata = {
                    "station_name": station_names[row_idx],
                    "river_name": river_names[row_idx],
                    "lat": lat[row_idx],
                    "lon": lon[row_idx],
                }
                unit_rows.append(
                    {
                        "product": path.name,
                        "resolution": resolution,
                        "unit_type": "cluster",
                        "unit_id": uid,
                        "station_name": station_names[row_idx],
                        "river_name": river_names[row_idx],
                        "lat": lat[row_idx],
                        "lon": lon[row_idx],
                        "source_names": "",
                        "first_observation_date": first_date,
                        "last_observation_date": last_date,
                        "record_count": int(len(cols)),
                        "declared_n_valid_time_steps": declared_int,
                        "record_count_delta_vs_declared": delta,
                        "unique_observed_days": day_stats[0],
                        "unique_observed_months": month_stats[0],
                        "unique_observed_years": year_stats[0],
                        "max_records_per_day": day_stats[1],
                        "max_records_per_month": month_stats[1],
                        "max_records_per_year": year_stats[1],
                        "n_days_gt1": day_stats[2],
                        "n_months_gt1": month_stats[2],
                        "n_years_gt1": year_stats[2],
                        "native_period": _native_period(resolution),
                        "max_records_per_native_period": native_max,
                        "n_native_periods_gt1": native_gt1,
                        "native_resolution_violation": native_violation,
                    }
                )

                if native_violation:
                    detail, by_source = _period_detail_for_matrix(
                        resolution=resolution,
                        row_idx=row_idx,
                        unit_id=uid,
                        metadata=metadata,
                        cols=cols,
                        period_keys=native_keys,
                        day_keys=day_keys,
                        selected_uid_var=selected_uid_var,
                        exact_source=exact_source,
                        fallback_source=fallback_source,
                    )
                    native_detail_rows.extend(detail)
                    native_source_rows.extend(by_source)

            log("processed clusters {:,}-{:,.0f} / {:,}".format(start + 1, stop, n_units))

    return (
        pd.DataFrame(unit_rows, columns=UNIT_COLUMNS),
        pd.DataFrame(native_detail_rows, columns=PERIOD_DETAIL_COLUMNS),
        pd.DataFrame(native_source_rows, columns=PERIOD_SOURCE_COLUMNS),
    )


def _climatology_record_sources(ds, station_index: np.ndarray, n_stations: int, n_records: int) -> np.ndarray:
    if "source" not in ds.variables:
        return np.asarray(["UNKNOWN"] * n_records, dtype=object)
    values = _decode_text_values(ds.variables["source"][:])
    if len(values) == n_records:
        return np.asarray([value or "UNKNOWN" for value in values], dtype=object)
    if len(values) == n_stations:
        station_sources = np.asarray([value or "UNKNOWN" for value in values], dtype=object)
        out = np.asarray(["UNKNOWN"] * n_records, dtype=object)
        ok = (station_index >= 0) & (station_index < n_stations)
        out[ok] = station_sources[station_index[ok]]
        return out
    return np.asarray(["UNKNOWN"] * n_records, dtype=object)


def audit_climatology(path: Path, record_mode: str):
    banner("Auditing climatology product")
    log(str(path))
    if not path.is_file():
        log("WARNING: climatology product not found; skipping")
        return pd.DataFrame(columns=UNIT_COLUMNS)

    with nc4.Dataset(path, "r") as ds:
        if "n_records" in ds.dimensions:
            n_records = int(len(ds.dimensions["n_records"]))
        elif "time" in ds.dimensions:
            n_records = int(len(ds.dimensions["time"]))
        else:
            raise ValueError("Climatology product has no n_records/time dimension")

        if "n_stations" in ds.dimensions:
            n_stations = int(len(ds.dimensions["n_stations"]))
        else:
            probe = _read_numeric_1d(ds, "station_index", n_records, default=-1)
            n_stations = int(np.max(probe)) + 1 if np.any(probe >= 0) else 0

        dates = _decode_time(ds)
        if len(dates) < n_records:
            dates = dates.append(pd.DatetimeIndex([pd.NaT] * (n_records - len(dates))))
        dates = dates[:n_records]
        day_keys, month_keys, year_keys, date_strings = _calendar_keys(dates)

        station_index = _read_numeric_1d(ds, "station_index", n_records, default=-1).astype(np.int64, copy=False)
        station_uid = _read_text_1d(ds, "station_uid", n_stations)
        station_name = _read_text_1d(ds, "station_name", n_stations)
        river_name = _read_text_1d(ds, "river_name", n_stations)
        lat = _read_numeric_1d(ds, "lat", n_stations)
        lon = _read_numeric_1d(ds, "lon", n_stations)
        record_sources = _climatology_record_sources(ds, station_index, n_stations, n_records)

        var_names = SEDIMENT_VARS if record_mode == "sediment" else CORE_VARS
        valid = np.zeros(n_records, dtype=bool)
        found = False
        for name in var_names:
            if name not in ds.variables:
                continue
            mask = _valid_numeric_mask(ds.variables[name], ds.variables[name][:]).reshape(-1)
            if len(mask) < n_records:
                mask = np.concatenate([mask, np.zeros(n_records - len(mask), dtype=bool)])
            valid |= mask[:n_records]
            found = True
        if not found:
            raise ValueError("No record variables found in climatology product")

    rows = []
    for idx in range(n_stations):
        cols = np.flatnonzero(valid & (station_index == idx))
        if cols.size == 0:
            continue
        day_stats = _count_stats(day_keys, cols)
        month_stats = _count_stats(month_keys, cols)
        year_stats = _count_stats(year_keys, cols)
        sources = sorted(set(_clean_text(record_sources[col]) or "UNKNOWN" for col in cols))
        rows.append(
            {
                "product": path.name,
                "resolution": "climatology",
                "unit_type": "climatology_station",
                "unit_id": station_uid[idx] or "CLM{:06d}".format(idx),
                "station_name": station_name[idx],
                "river_name": river_name[idx],
                "lat": lat[idx],
                "lon": lon[idx],
                "source_names": "|".join(sources),
                "first_observation_date": str(date_strings[cols[0]]),
                "last_observation_date": str(date_strings[cols[-1]]),
                "record_count": int(len(cols)),
                "declared_n_valid_time_steps": np.nan,
                "record_count_delta_vs_declared": np.nan,
                "unique_observed_days": day_stats[0],
                "unique_observed_months": month_stats[0],
                "unique_observed_years": year_stats[0],
                "max_records_per_day": day_stats[1],
                "max_records_per_month": month_stats[1],
                "max_records_per_year": year_stats[1],
                "n_days_gt1": day_stats[2],
                "n_months_gt1": month_stats[2],
                "n_years_gt1": year_stats[2],
                "native_period": "",
                "max_records_per_native_period": np.nan,
                "n_native_periods_gt1": np.nan,
                "native_resolution_violation": False,
            }
        )
    return pd.DataFrame(rows, columns=UNIT_COLUMNS)


def build_source_summary(source_rows: pd.DataFrame) -> pd.DataFrame:
    if source_rows.empty:
        return pd.DataFrame(columns=SOURCE_SUMMARY_COLUMNS)
    rows = []
    for (resolution, source), group in source_rows.groupby(["resolution", "source_name"], dropna=False):
        rows.append(
            {
                "resolution": resolution,
                "source_name": _clean_text(source) or "UNKNOWN",
                "offending_clusters": int(group["unit_id"].nunique()),
                "offending_periods_involving_source": int(len(group)),
                "source_records_in_offending_periods": int(pd.to_numeric(group["source_record_count"], errors="coerce").fillna(0).sum()),
                "periods_where_source_itself_has_gt1_record": int((pd.to_numeric(group["source_record_count"], errors="coerce").fillna(0) > 1).sum()),
                "max_total_records_in_offending_period": int(pd.to_numeric(group["period_record_count"], errors="coerce").fillna(0).max()),
                "max_source_records_in_offending_period": int(pd.to_numeric(group["source_record_count"], errors="coerce").fillna(0).max()),
            }
        )
    return pd.DataFrame(rows, columns=SOURCE_SUMMARY_COLUMNS).sort_values(
        ["offending_clusters", "offending_periods_involving_source", "source_name"],
        ascending=[False, False, True],
    ).reset_index(drop=True)


def build_product_summary(unit_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if unit_df.empty:
        return pd.DataFrame(columns=PRODUCT_SUMMARY_COLUMNS)
    for (product, resolution, unit_type), group in unit_df.groupby(["product", "resolution", "unit_type"], sort=False):
        native_period = _native_period(str(resolution))
        rows.append(
            {
                "product": product,
                "resolution": resolution,
                "unit_type": unit_type,
                "units_with_records": int(len(group)),
                "record_count": int(pd.to_numeric(group["record_count"], errors="coerce").fillna(0).sum()),
                "max_records_per_day": int(pd.to_numeric(group["max_records_per_day"], errors="coerce").fillna(0).max()),
                "max_records_per_month": int(pd.to_numeric(group["max_records_per_month"], errors="coerce").fillna(0).max()),
                "max_records_per_year": int(pd.to_numeric(group["max_records_per_year"], errors="coerce").fillna(0).max()),
                "native_period": native_period,
                "units_with_native_period_gt1": int(group["native_resolution_violation"].astype(bool).sum()) if native_period else 0,
                "offending_native_periods": int(pd.to_numeric(group["n_native_periods_gt1"], errors="coerce").fillna(0).sum()) if native_period else 0,
                "max_records_per_native_period": int(pd.to_numeric(group["max_records_per_native_period"], errors="coerce").fillna(0).max()) if native_period else 0,
            }
        )
    return pd.DataFrame(rows, columns=PRODUCT_SUMMARY_COLUMNS)


def _write_csv(df: pd.DataFrame, path: Path, columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if df is None or df.empty:
        pd.DataFrame(columns=list(columns)).to_csv(path, index=False)
    else:
        df.reindex(columns=list(columns)).to_csv(path, index=False)
    log("wrote {}".format(path))


def _md_table(df: pd.DataFrame, max_rows: int = 30) -> str:
    if df is None or df.empty:
        return "*No rows.*"
    work = df.head(max_rows).copy()
    try:
        return work.to_markdown(index=False)
    except Exception:
        return "```text\n{}\n```".format(work.to_string(index=False))


def write_report(
    out_path: Path,
    release_dir: Path,
    record_mode: str,
    product_summary: pd.DataFrame,
    monthly_detail: pd.DataFrame,
    monthly_source_summary: pd.DataFrame,
    annual_detail: pd.DataFrame,
    annual_source_summary: pd.DataFrame,
    daily_detail: pd.DataFrame,
) -> None:
    monthly_clusters = int(monthly_detail["unit_id"].nunique()) if not monthly_detail.empty else 0
    annual_clusters = int(annual_detail["unit_id"].nunique()) if not annual_detail.empty else 0
    daily_clusters = int(daily_detail["unit_id"].nunique()) if not daily_detail.empty else 0

    lines = [
        "# Temporal-resolution integrity audit",
        "",
        "## Scope",
        "",
        "- Release directory: `{}`".format(release_dir),
        "- Record mode: `{}`".format(record_mode),
        "- A populated record is {}.".format(
            "a station-time cell with at least one non-missing SSC or SSL value"
            if record_mode == "sediment"
            else "a station-time cell with at least one non-missing Q, SSC, or SSL value"
        ),
        "- Matrix products are checked cluster by cluster. The climatology product is checked by standalone `station_uid` because it is not a basin-cluster matrix.",
        "",
        "## Product summary",
        "",
        _md_table(product_summary),
        "",
        "## Native-resolution violations",
        "",
        "- Daily clusters with >1 released record on the same calendar day: **{}**".format(daily_clusters),
        "- Monthly clusters with >1 released record in the same calendar month: **{}** ({} offending cluster-months).".format(
            monthly_clusters, len(monthly_detail)
        ),
        "- Annual clusters with >1 released record in the same calendar year: **{}** ({} offending cluster-years).".format(
            annual_clusters, len(annual_detail)
        ),
        "",
        "## Monthly >1 record/month by selected source",
        "",
        _md_table(monthly_source_summary),
        "",
        "## Annual >1 record/year by selected source",
        "",
        _md_table(annual_source_summary),
        "",
        "## Interpretation note",
        "",
        "A monthly violation means that the final monthly matrix contains multiple populated observation dates within at least one calendar month for that cluster. An annual violation means that the final annual matrix contains multiple populated observation dates within at least one calendar year. These diagnostics identify temporal-support cases that should be inspected before describing the products as one-value-per-month or one-value-per-year series.",
        "",
        "The detailed CSV files preserve the offending calendar period, exact released dates, selected source-station UID(s), and source contribution counts so that the originating source can be traced directly.",
        "",
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    log("wrote {}".format(out_path))


def main(argv=None) -> int:
    args = parse_args(argv)
    release_dir = args.release_dir
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    banner("Temporal-resolution integrity audit")
    log("release dir: {}".format(release_dir))
    log("output dir:  {}".format(out_dir))
    log("record mode: {}".format(args.record_mode))

    exact_source, fallback_source = _load_source_lookup(release_dir)

    all_units = []
    detail_by_resolution = {}
    source_by_resolution = {}
    for resolution, file_name in MATRIX_FILES.items():
        unit_df, detail_df, source_df = audit_matrix_product(
            release_dir / file_name,
            resolution=resolution,
            record_mode=args.record_mode,
            chunk_rows=args.chunk_rows,
            exact_source=exact_source,
            fallback_source=fallback_source,
        )
        all_units.append(unit_df)
        detail_by_resolution[resolution] = detail_df
        source_by_resolution[resolution] = source_df

    climatology_df = audit_climatology(release_dir / CLIMATOLOGY_FILE, args.record_mode)
    if not climatology_df.empty:
        all_units.append(climatology_df)

    unit_df = pd.concat(all_units, ignore_index=True) if all_units else pd.DataFrame(columns=UNIT_COLUMNS)
    product_summary = build_product_summary(unit_df)

    daily_detail = detail_by_resolution.get("daily", pd.DataFrame(columns=PERIOD_DETAIL_COLUMNS))
    monthly_detail = detail_by_resolution.get("monthly", pd.DataFrame(columns=PERIOD_DETAIL_COLUMNS))
    annual_detail = detail_by_resolution.get("annual", pd.DataFrame(columns=PERIOD_DETAIL_COLUMNS))
    monthly_source = source_by_resolution.get("monthly", pd.DataFrame(columns=PERIOD_SOURCE_COLUMNS))
    annual_source = source_by_resolution.get("annual", pd.DataFrame(columns=PERIOD_SOURCE_COLUMNS))
    monthly_source_summary = build_source_summary(monthly_source)
    annual_source_summary = build_source_summary(annual_source)

    _write_csv(unit_df, out_dir / "temporal_period_counts_by_unit.csv", UNIT_COLUMNS)
    _write_csv(product_summary, out_dir / "product_integrity_summary.csv", PRODUCT_SUMMARY_COLUMNS)
    _write_csv(daily_detail, out_dir / "daily_gt1_record_per_day.csv", PERIOD_DETAIL_COLUMNS)
    _write_csv(monthly_detail, out_dir / "monthly_gt1_record_per_month.csv", PERIOD_DETAIL_COLUMNS)
    _write_csv(monthly_source, out_dir / "monthly_gt1_record_per_month_by_source.csv", PERIOD_SOURCE_COLUMNS)
    _write_csv(monthly_source_summary, out_dir / "monthly_gt1_by_source.csv", SOURCE_SUMMARY_COLUMNS)
    _write_csv(annual_detail, out_dir / "annual_gt1_record_per_year.csv", PERIOD_DETAIL_COLUMNS)
    _write_csv(annual_source, out_dir / "annual_gt1_record_per_year_by_source.csv", PERIOD_SOURCE_COLUMNS)
    _write_csv(annual_source_summary, out_dir / "annual_gt1_by_source.csv", SOURCE_SUMMARY_COLUMNS)

    write_report(
        out_dir / "temporal_resolution_integrity_report.md",
        release_dir=release_dir,
        record_mode=args.record_mode,
        product_summary=product_summary,
        monthly_detail=monthly_detail,
        monthly_source_summary=monthly_source_summary,
        annual_detail=annual_detail,
        annual_source_summary=annual_source_summary,
        daily_detail=daily_detail,
    )

    banner("Audit result")
    log("daily clusters >1 record/day: {}".format(daily_detail["unit_id"].nunique() if not daily_detail.empty else 0))
    log("monthly clusters >1 record/month: {}".format(monthly_detail["unit_id"].nunique() if not monthly_detail.empty else 0))
    log("monthly offending cluster-months: {}".format(len(monthly_detail)))
    log("annual clusters >1 record/year: {}".format(annual_detail["unit_id"].nunique() if not annual_detail.empty else 0))
    log("annual offending cluster-years: {}".format(len(annual_detail)))
    log("report: {}".format(out_dir / "temporal_resolution_integrity_report.md"))

    has_key_violations = (not monthly_detail.empty) or (not annual_detail.empty)
    if args.fail_on_violations and has_key_violations:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
