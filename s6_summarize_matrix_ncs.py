#!/usr/bin/env python3
"""
Summarize station x time matrix NetCDF files with xarray-style previews and
basic statistics tables.

Input:
  - scripts_basin_test/output/s6_matrix_by_resolution/*.nc

Default outputs:
  - scripts_basin_test/output/s6_matrix_by_resolution/summary/00_matrix_file_overview.csv
  - scripts_basin_test/output/s6_matrix_by_resolution/summary/<stem>_dataset_repr.txt
  - scripts_basin_test/output/s6_matrix_by_resolution/summary/<stem>_basic_stats.csv

Design:
  - open each matrix NC with xarray;
  - write a readable text summary using xarray.Dataset repr;
  - write a metric/value CSV for each file;
  - write one combined overview table across all files.
"""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline_paths import (
    S6_MATRIX_DIR,
    S6_MATRIX_SUMMARY_DIR,
    get_output_r_root,
)

try:
    import xarray as xr
    HAS_XR = True
except ImportError:
    xr = None
    HAS_XR = False

try:
    import h5netcdf
    HAS_H5NC = True
except ImportError:
    h5netcdf = None
    HAS_H5NC = False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_INPUT_DIR = PROJECT_ROOT / S6_MATRIX_DIR
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / S6_MATRIX_SUMMARY_DIR

CORE_VARS = ("Q", "SSC", "SSL")
FLAG_VARS = ("Q_flag", "SSC_flag", "SSL_flag")


def _open_dataset(path):
    last_error = None
    for engine in (None, "netcdf4", "h5netcdf"):
        for decode_cf in (True, False):
            try:
                kwargs = dict(decode_cf=decode_cf, mask_and_scale=True)
                if engine is None:
                    return xr.open_dataset(path, **kwargs)
                return xr.open_dataset(path, engine=engine, **kwargs)
            except Exception as exc:
                last_error = exc
    raise last_error


def _safe_item(value, default=np.nan):
    try:
        if hasattr(value, "item"):
            return value.item()
        return value
    except Exception:
        return default


def _format_value(value):
    if value is None:
        return ""
    if isinstance(value, (np.floating, float)):
        if np.isnan(value):
            return ""
        return "{:.6g}".format(float(value))
    if isinstance(value, (np.integer, int)):
        return int(value)
    return str(value)


def _time_bounds(ds):
    if "time" not in ds.variables or int(ds.sizes.get("time", 0)) == 0:
        return "", ""
    values = np.asarray(ds["time"].values)
    if values.size == 0:
        return "", ""
    try:
        times = pd.to_datetime(values)
        times = pd.Series(times).dropna()
        if len(times) == 0:
            return "", ""
        return str(times.min()), str(times.max())
    except Exception:
        flat = np.asarray(values).astype(str)
        return str(flat.min()) if flat.size else "", str(flat.max()) if flat.size else ""


def _count_notnull(arr):
    return int(_safe_item(arr.count(), 0))


def _count_negative(arr):
    try:
        return int(_safe_item(((arr < 0) & arr.notnull()).sum(), 0))
    except Exception:
        return 0


def _safe_stat(arr, stat_name):
    if _count_notnull(arr) == 0:
        return np.nan
    try:
        if stat_name == "min":
            return float(_safe_item(arr.min(skipna=True)))
        if stat_name == "max":
            return float(_safe_item(arr.max(skipna=True)))
        if stat_name == "mean":
            return float(_safe_item(arr.mean(skipna=True)))
    except Exception:
        return np.nan
    return np.nan


def _masked_to_array(values):
    arr = np.ma.asarray(values)
    if np.ma.isMaskedArray(arr):
        return arr.filled(np.nan if np.issubdtype(arr.dtype, np.floating) else 0)
    return np.asarray(arr)


def _valid_mask(arr, fill_value=None):
    arr = np.asarray(arr)
    if np.issubdtype(arr.dtype, np.floating):
        mask = np.isfinite(arr)
    else:
        mask = np.ones(arr.shape, dtype=bool)
    if fill_value is not None:
        try:
            mask &= arr != fill_value
        except Exception:
            pass
    return mask


def _count_notnull_1d(values, fill_value=None):
    arr = _masked_to_array(values)
    return int(_valid_mask(arr, fill_value=fill_value).sum())


def _iter_row_slices(n_rows, chunk_rows=64):
    for start in range(0, n_rows, chunk_rows):
        yield start, min(start + chunk_rows, n_rows)


def _scan_float_var_h5(var, n_rows):
    valid_count = 0
    neg_count = 0
    valid_sum = 0.0
    valid_min = np.inf
    valid_max = -np.inf
    fill_value = var.attrs.get("_FillValue", None)

    for start, stop in _iter_row_slices(n_rows):
        data = _masked_to_array(var[start:stop, :])
        valid_mask = _valid_mask(data, fill_value=fill_value)
        chunk_count = int(valid_mask.sum())
        if chunk_count == 0:
            continue
        values = data[valid_mask]
        valid_count += chunk_count
        neg_count += int((values < 0).sum())
        valid_sum += float(values.astype(np.float64).sum())
        chunk_min = float(values.min())
        chunk_max = float(values.max())
        if chunk_min < valid_min:
            valid_min = chunk_min
        if chunk_max > valid_max:
            valid_max = chunk_max

    return {
        "non_missing": valid_count,
        "negative_count": neg_count,
        "min": valid_min if valid_count else np.nan,
        "mean": (valid_sum / valid_count) if valid_count else np.nan,
        "max": valid_max if valid_count else np.nan,
    }


def _scan_flag_var_h5(var, n_rows):
    counts = {0: 0, 1: 0, 2: 0, 3: 0, 9: 0}
    for start, stop in _iter_row_slices(n_rows):
        data = _masked_to_array(var[start:stop, :])
        for code in counts:
            counts[code] += int(np.count_nonzero(data == code))
    return counts


def _scan_binary_count_h5(var, n_rows, true_value=1):
    total = 0
    for start, stop in _iter_row_slices(n_rows):
        data = _masked_to_array(var[start:stop, :])
        total += int(np.count_nonzero(data == true_value))
    return total


def _scan_nonnegative_count_h5(var, n_rows):
    total = 0
    for start, stop in _iter_row_slices(n_rows):
        data = _masked_to_array(var[start:stop, :])
        total += int(np.count_nonzero(data >= 0))
    return total


def _collect_stats_h5(path, time_start="", time_end="", time_type="", created=""):
    if not HAS_H5NC:
        return None

    with h5netcdf.File(path, "r") as ds:
        n_stations = int(ds.dimensions["n_stations"].size) if "n_stations" in ds.dimensions else 0
        n_time = int(ds.dimensions["time"].size) if "time" in ds.dimensions else 0
        n_sources = int(ds.dimensions["n_sources"].size) if "n_sources" in ds.dimensions else 0
        total_cells = int(n_stations * n_time)
        stats = {
            "file_name": path.name,
            "file_path": str(path),
            "file_size_mb": path.stat().st_size / (1024 * 1024),
            "time_type": str(time_type or ds.attrs.get("time_type", "")),
            "created": str(created or ds.attrs.get("history", "")),
            "n_stations": n_stations,
            "n_time": n_time,
            "n_sources": n_sources,
            "total_matrix_cells": total_cells,
        }

        stats["time_start"] = time_start
        stats["time_end"] = time_end

        for name in ("lat", "lon", "basin_area"):
            if name in ds.variables:
                fill_value = ds.variables[name].attrs.get("_FillValue", None)
                stats["{}_non_missing".format(name)] = _count_notnull_1d(
                    ds.variables[name][:], fill_value=fill_value
                )

        if "n_valid_time_steps" in ds.variables:
            fill_value = ds.variables["n_valid_time_steps"].attrs.get("_FillValue", None)
            values = _masked_to_array(ds.variables["n_valid_time_steps"][:])
            finite = values[_valid_mask(values, fill_value=fill_value)]
            stats["stations_with_any_data"] = int(np.count_nonzero(finite > 0))
            stats["mean_valid_time_steps_per_station"] = float(finite.mean()) if finite.size else np.nan
            stats["max_valid_time_steps_per_station"] = float(finite.max()) if finite.size else np.nan

        if "n_sources_in_resolution" in ds.variables:
            fill_value = ds.variables["n_sources_in_resolution"].attrs.get("_FillValue", None)
            values = _masked_to_array(ds.variables["n_sources_in_resolution"][:])
            finite = values[_valid_mask(values, fill_value=fill_value)]
            stats["mean_sources_per_station"] = float(finite.mean()) if finite.size else np.nan
            stats["max_sources_per_station"] = float(finite.max()) if finite.size else np.nan

        if "is_overlap" in ds.variables:
            overlap_cells = _scan_binary_count_h5(ds.variables["is_overlap"], n_stations, true_value=1)
            stats["overlap_cell_count"] = overlap_cells
            stats["overlap_cell_fraction"] = overlap_cells / total_cells if total_cells else np.nan

        if "selected_source_index" in ds.variables:
            selected_cells = _scan_nonnegative_count_h5(ds.variables["selected_source_index"], n_stations)
            stats["selected_source_cell_count"] = selected_cells
            stats["selected_source_cell_fraction"] = selected_cells / total_cells if total_cells else np.nan

        for var in CORE_VARS:
            if var in ds.variables:
                result = _scan_float_var_h5(ds.variables[var], n_stations)
                for key, value in result.items():
                    stats["{}_{}".format(var, key)] = value
                stats["{}_non_missing_fraction".format(var)] = (
                    result["non_missing"] / total_cells if total_cells else np.nan
                )

        for var in FLAG_VARS:
            if var in ds.variables:
                counts = _scan_flag_var_h5(ds.variables[var], n_stations)
                stats["{}_good".format(var)] = counts[0]
                stats["{}_estimated".format(var)] = counts[1]
                stats["{}_suspect".format(var)] = counts[2]
                stats["{}_bad".format(var)] = counts[3]
                stats["{}_missing".format(var)] = counts[9]

        return stats


def _collect_metric_rows(stats):
    n_stations = int(stats.get("n_stations", 0))
    n_time = int(stats.get("n_time", 0))
    n_sources = int(stats.get("n_sources", 0))
    total_cells = int(stats.get("total_matrix_cells", n_stations * n_time))

    def _metric(metric, value, note=""):
        return {"metric": metric, "value": _format_value(value), "note": note}

    rows = [
        _metric("file_name", stats.get("file_name", "")),
        _metric("file_path", stats.get("file_path", "")),
        _metric("file_size_mb", stats.get("file_size_mb", np.nan)),
        _metric("time_type", stats.get("time_type", "")),
        _metric("created", stats.get("created", "")),
        _metric("n_stations", n_stations),
        _metric("n_time", n_time),
        _metric("n_sources", n_sources),
        _metric("total_matrix_cells", total_cells),
        _metric("time_start", stats.get("time_start", "")),
        _metric("time_end", stats.get("time_end", "")),
    ]

    for name in (
        "lat_non_missing",
        "lon_non_missing",
        "basin_area_non_missing",
        "stations_with_any_data",
        "mean_valid_time_steps_per_station",
        "max_valid_time_steps_per_station",
        "mean_sources_per_station",
        "max_sources_per_station",
        "overlap_cell_count",
        "overlap_cell_fraction",
        "selected_source_cell_count",
        "selected_source_cell_fraction",
    ):
        if name in stats:
            rows.append(_metric(name, stats[name]))

    for var in CORE_VARS:
        if "{}_non_missing".format(var) in stats:
            rows.extend(
                [
                    _metric("{}_non_missing".format(var), stats.get("{}_non_missing".format(var), np.nan)),
                    _metric("{}_non_missing_fraction".format(var), stats.get("{}_non_missing_fraction".format(var), np.nan)),
                    _metric("{}_negative_count".format(var), stats.get("{}_negative_count".format(var), np.nan)),
                    _metric("{}_min".format(var), stats.get("{}_min".format(var), np.nan)),
                    _metric("{}_mean".format(var), stats.get("{}_mean".format(var), np.nan)),
                    _metric("{}_max".format(var), stats.get("{}_max".format(var), np.nan)),
                ]
            )

    for var in FLAG_VARS:
        if "{}_good".format(var) in stats:
            rows.extend(
                [
                    _metric("{}_good".format(var), stats.get("{}_good".format(var), np.nan)),
                    _metric("{}_estimated".format(var), stats.get("{}_estimated".format(var), np.nan)),
                    _metric("{}_suspect".format(var), stats.get("{}_suspect".format(var), np.nan)),
                    _metric("{}_bad".format(var), stats.get("{}_bad".format(var), np.nan)),
                    _metric("{}_missing".format(var), stats.get("{}_missing".format(var), np.nan)),
                ]
            )

    return rows


def _collect_overview_row(stats):
    overview = {}
    for key in (
        "file_name",
        "file_path",
        "file_size_mb",
        "time_type",
        "n_stations",
        "n_time",
        "n_sources",
        "total_matrix_cells",
        "time_start",
        "time_end",
        "stations_with_any_data",
        "mean_valid_time_steps_per_station",
        "overlap_cell_count",
        "overlap_cell_fraction",
        "Q_non_missing",
        "Q_non_missing_fraction",
        "SSC_non_missing",
        "SSC_non_missing_fraction",
        "SSL_non_missing",
        "SSL_non_missing_fraction",
    ):
        overview[key] = stats.get(key, np.nan)
    if "file_size_mb" in overview and overview["file_size_mb"] is not np.nan:
        try:
            overview["file_size_mb"] = round(float(overview["file_size_mb"]), 3)
        except Exception:
            pass
    return overview


def _write_dataset_repr(ds, path, out_path):
    lines = [
        "# xarray.Dataset summary",
        "generated_at: {}".format(datetime.now().isoformat(timespec="seconds")),
        "file_name: {}".format(path.name),
        "file_path: {}".format(path),
        "",
        repr(ds),
        "",
        "# global_attrs",
    ]
    if ds.attrs:
        for key in sorted(ds.attrs):
            lines.append("{}: {}".format(key, ds.attrs[key]))
    else:
        lines.append("(none)")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description="Summarize matrix NetCDF files with xarray-style text and stats CSVs")
    ap.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="directory containing matrix nc files")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUTPUT_DIR), help="output summary directory")
    args = ap.parse_args()

    if not HAS_XR:
        print("Error: xarray is required. pip install xarray netCDF4 or xarray h5netcdf")
        return 1
    if not HAS_H5NC:
        print("Error: h5netcdf is required for chunked statistics scanning.")
        return 1

    input_dir = Path(args.input_dir).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not input_dir.is_dir():
        print("Error: input dir not found: {}".format(input_dir))
        return 1

    nc_paths = sorted(input_dir.glob("*.nc"))
    if not nc_paths:
        print("Error: no .nc files found under {}".format(input_dir))
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    overview_rows = []

    for path in nc_paths:
        print("Summarizing {}".format(path.name))
        with _open_dataset(path) as ds:
            repr_path = out_dir / "{}_dataset_repr.txt".format(path.stem)
            stats_path = out_dir / "{}_basic_stats.csv".format(path.stem)
            time_start, time_end = _time_bounds(ds)
            time_type = ds.attrs.get("time_type", "")
            created = ds.attrs.get("history", "")

            _write_dataset_repr(ds, path, repr_path)
        stats = _collect_stats_h5(
            path,
            time_start=time_start,
            time_end=time_end,
            time_type=time_type,
            created=created,
        )
        metric_rows = _collect_metric_rows(stats)
        pd.DataFrame(metric_rows).to_csv(stats_path, index=False)

        overview_rows.append(_collect_overview_row(stats))
        print("  wrote {}".format(repr_path))
        print("  wrote {}".format(stats_path))

    overview_df = pd.DataFrame(overview_rows)
    overview_df = overview_df.sort_values(["time_type", "file_name"], na_position="last")
    overview_path = out_dir / "00_matrix_file_overview.csv"
    overview_df.to_csv(overview_path, index=False)
    print("Wrote {}".format(overview_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
