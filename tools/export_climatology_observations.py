#!/usr/bin/env python3
"""
Export sed_reference_climatology.nc to a flat CSV table for direct querying.

Default input:
  scripts_basin_test/output/sed_reference_release_minimal/sed_reference_climatology.nc

Default output:
  scripts_basin_test/output/sed_reference_release_minimal/climatology_observations.csv
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import get_output_r_root

try:
    import netCDF4 as nc4

    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False

try:
    import h5netcdf

    HAS_H5NETCDF = True
except ImportError:
    h5netcdf = None
    HAS_H5NETCDF = False

PROJECT_ROOT = get_output_r_root(REPO_ROOT)
DEFAULT_MINIMAL_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_minimal"
DEFAULT_INPUT_NC = DEFAULT_MINIMAL_DIR / "sed_reference_climatology.nc"
DEFAULT_OUTPUT_CSV = DEFAULT_MINIMAL_DIR / "climatology_observations.csv"

STATION_FIELDS = (
    "station_uid",
    "lat",
    "lon",
    "station_name",
    "river_name",
    "source_station_id",
    "temporal_span",
    "source_station_time_coverage_start",
    "source_station_time_coverage_end",
    "source_station_summary",
    "source_station_comment",
    "source_station_variables_provided",
    "source_station_data_limitations",
    "source_station_declared_temporal_resolution",
    "source_station_path",
)
SOURCE_FIELDS = (
    "source_name",
    "source_long_name",
    "institution",
    "reference",
    "source_url",
)
RECORD_FIELDS = (
    "time",
    "resolution",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
    "source",
)


def resolve_path(value, base=PROJECT_ROOT):
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if path.exists():
        return path.resolve()
    return (base / path).resolve()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-nc", default=str(DEFAULT_INPUT_NC), help="Input sed_reference_climatology.nc")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV), help="Output query-friendly CSV")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output CSV if it already exists")
    args = parser.parse_args(argv)
    args.input_nc = resolve_path(args.input_nc)
    args.output_csv = resolve_path(args.output_csv)
    return args


def _clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat", "na", "n/a", "null", "_", "--"}:
        return ""
    return text


def _decode_nc_text(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip("\x00").strip()
    if isinstance(value, np.bytes_):
        return value.decode("utf-8", errors="ignore").strip("\x00").strip()
    if isinstance(value, np.ndarray):
        if value.shape == ():
            return _decode_nc_text(value.item())
        if value.dtype.kind in {"S", "U"}:
            return "".join(_decode_nc_text(item) for item in value.reshape(-1)).strip("\x00").strip()
    return _clean_text(value)


def _var_values(ds, name):
    if name not in ds.variables:
        return []
    values = np.asarray(ds.variables[name][:])
    if np.ma.isMaskedArray(values):
        if values.dtype.kind in {"S", "U", "O"}:
            values = values.filled(b"")
        else:
            values = values.filled(np.nan)
    if values.shape == ():
        return [_decode_nc_text(values.item()) if values.dtype.kind in {"S", "U", "O"} else values.item()]
    if values.dtype.kind in {"S", "U", "O"}:
        if values.ndim >= 2 and values.dtype.kind in {"S", "U"}:
            rows = values.reshape((-1, values.shape[-1]))
            return [_decode_nc_text(row) for row in rows]
        return [_decode_nc_text(item) for item in values.reshape(-1)]
    return values.reshape(-1).tolist()


def _var_attr(var, name, default=""):
    if hasattr(var, "getncattr"):
        if name in var.ncattrs():
            return var.getncattr(name)
        return default
    return var.attrs.get(name, default)


def _format_time_value(value, units, calendar):
    if value is None or pd.isna(value):
        return ""
    units = _clean_text(units)
    calendar = _clean_text(calendar) or "standard"
    if HAS_NC and nc4 is not None and units:
        try:
            dt = nc4.num2date(
                float(value),
                units=units,
                calendar=calendar,
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=False,
            )
            if getattr(dt, "hour", 0) == 0 and getattr(dt, "minute", 0) == 0 and getattr(dt, "second", 0) == 0:
                return dt.strftime("%Y-%m-%d")
            return dt.isoformat()
        except Exception:
            pass
    return value


def _at(values, index, default=""):
    try:
        if index is None or index < 0 or index >= len(values):
            return default
        value = values[index]
    except Exception:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    return value


def _index(value):
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except Exception:
        return None


def _open_climatology(path):
    if HAS_NC:
        return nc4.Dataset(path, "r")
    if HAS_H5NETCDF:
        return h5netcdf.File(path, "r")
    raise RuntimeError("netCDF4 or h5netcdf is required to read climatology NetCDF files")


def export_climatology_observations(input_nc, output_csv):
    if not input_nc.is_file():
        raise FileNotFoundError("Input climatology NetCDF not found: {}".format(input_nc))

    with _open_climatology(input_nc) as ds:
        station_index_values = _var_values(ds, "station_index")
        station_source_index_values = _var_values(ds, "source_index")
        record_source_values = _var_values(ds, "source")

        station_values = {name: _var_values(ds, name) for name in STATION_FIELDS}
        source_values = {name: _var_values(ds, name) for name in SOURCE_FIELDS}
        record_values = {name: _var_values(ds, name) for name in RECORD_FIELDS}

        time_values = record_values.get("time", [])
        time_units = _var_attr(ds.variables["time"], "units", "") if "time" in ds.variables else ""
        time_calendar = _var_attr(ds.variables["time"], "calendar", "standard") if "time" in ds.variables else "standard"
        decoded_time_values = [_format_time_value(value, time_units, time_calendar) for value in time_values]

    n_records = max(
        [len(station_index_values), len(decoded_time_values)]
        + [len(record_values.get(name, [])) for name in RECORD_FIELDS]
    )

    rows = []
    for record_idx in range(n_records):
        station_idx = _index(_at(station_index_values, record_idx))
        source_idx = _index(_at(station_source_index_values, station_idx)) if station_idx is not None else None
        record_source = _at(record_source_values, record_idx)

        row = {
            "record_index": record_idx,
            "station_index": "" if station_idx is None else station_idx,
            "time": _at(decoded_time_values, record_idx),
            "time_raw": _at(time_values, record_idx),
            "resolution": _at(record_values.get("resolution", []), record_idx),
            "Q": _at(record_values.get("Q", []), record_idx),
            "SSC": _at(record_values.get("SSC", []), record_idx),
            "SSL": _at(record_values.get("SSL", []), record_idx),
            "Q_flag": _at(record_values.get("Q_flag", []), record_idx),
            "SSC_flag": _at(record_values.get("SSC_flag", []), record_idx),
            "SSL_flag": _at(record_values.get("SSL_flag", []), record_idx),
            "record_source": record_source,
        }

        for name in STATION_FIELDS:
            row[name] = _at(station_values.get(name, []), station_idx)

        for name in SOURCE_FIELDS:
            row[name] = _at(source_values.get(name, []), source_idx)

        if not row.get("source_name") and record_source:
            row["source_name"] = record_source

        rows.append(row)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_csv, index=False)
    print("[write] {}".format(output_csv))
    print("[done] exported {} climatology record(s)".format(len(rows)))


def main(argv=None):
    args = parse_args(argv)
    print("[config] input climatology nc: {}".format(args.input_nc))
    print("[config] output csv:           {}".format(args.output_csv))
    if args.output_csv.exists() and not args.overwrite:
        raise FileExistsError("Output CSV already exists: {} (use --overwrite to replace it)".format(args.output_csv))
    export_climatology_observations(args.input_nc, args.output_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
