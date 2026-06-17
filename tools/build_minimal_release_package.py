#!/usr/bin/env python3
"""
Build post-release slim package directories from the full S8 release package.

This tool validates the full release input, prepares the target output
directories, builds a minimal matrix NetCDF package, and writes optional
standalone climatology and satellite-validation extension packages.

Default input:
  scripts_basin_test/output/sed_reference_release/

Default outputs:
  scripts_basin_test/output/sed_reference_release_minimal/
  scripts_basin_test/output/sed_reference_release_climatology/
  scripts_basin_test/output/sed_reference_release_satellite/
"""

import argparse
import re
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from pipeline_paths import RELEASE_DATASET_DIR, get_output_r_root

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


PROJECT_ROOT = get_output_r_root(SCRIPTS_DIR)
DEFAULT_RELEASE_DIR = PROJECT_ROOT / RELEASE_DATASET_DIR
DEFAULT_MINIMAL_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_minimal"
DEFAULT_CLIMATOLOGY_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_climatology"
DEFAULT_SATELLITE_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_satellite"
DEFAULT_SCHEMA_PATH = SCRIPTS_DIR / "config/release_minimal_schema.yml"

MINIMAL_PACKAGE_FILES = ()
MINIMAL_MATRIX_FILES = ()
MINIMAL_KEEP_VARS = ()
MINIMAL_REQUIRED_VARS = ()
COMPRESSED_MATRIX_VARS = set()
GLOBAL_ATTRS_TO_KEEP = ()
CLIMATOLOGY_PACKAGE_FILES = (
    "sed_reference_climatology.nc",
)
SATELLITE_PACKAGE_FILES = (
    "sed_reference_satellite.nc",
    "satellite_catalog.csv",
)
MINIMAL_FORBIDDEN_FILES = ()
MINIMAL_FORBIDDEN_VARS = ()
MINIMAL_RESOLUTIONS = {"daily", "monthly", "annual"}
MINIMAL_CATALOG_COLUMNS = {}
MINIMAL_STATION_CATALOG_COLUMNS = ()
MINIMAL_SOURCE_STATION_CATALOG_COLUMNS = ()
MINIMAL_SOURCE_DATASET_CATALOG_COLUMNS = ()

BUILD_FAILURES = []
BUILD_WARNINGS = []

PACKAGING_SCRIPT = Path(__file__).resolve()


class MinimalSchemaError(ValueError):
    pass


def _schema_list(schema, key):
    if key not in schema:
        raise MinimalSchemaError("Schema missing required field: {}".format(key))
    value = schema[key]
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise MinimalSchemaError("Schema field {} must be a list of non-empty strings".format(key))
    return tuple(value)


def _schema_catalog_columns(schema):
    key = "minimal_catalog_columns"
    if key not in schema:
        raise MinimalSchemaError("Schema missing required field: {}".format(key))
    value = schema[key]
    if not isinstance(value, dict):
        raise MinimalSchemaError("Schema field {} must be a mapping of catalog file names to columns".format(key))

    required_catalogs = (
        "station_catalog.csv",
        "source_station_catalog.csv",
        "source_dataset_catalog.csv",
    )
    result = {}
    for catalog_name in required_catalogs:
        if catalog_name not in value:
            raise MinimalSchemaError(
                "Schema field {} missing required catalog: {}".format(key, catalog_name)
            )
        columns = value[catalog_name]
        if not isinstance(columns, list) or any(
            not isinstance(item, str) or not item for item in columns
        ):
            raise MinimalSchemaError(
                "Schema field {}.{} must be a list of non-empty strings".format(key, catalog_name)
            )
        result[catalog_name] = tuple(columns)
    return result


def load_minimal_schema(path):
    if not path.is_file():
        raise MinimalSchemaError("Minimal release schema file not found: {}".format(path))
    try:
        with path.open("r", encoding="utf-8") as stream:
            schema = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise MinimalSchemaError("Minimal release schema is not valid YAML: {} ({})".format(path, exc))
    if not isinstance(schema, dict):
        raise MinimalSchemaError("Minimal release schema must be a YAML mapping: {}".format(path))

    catalog_columns = _schema_catalog_columns(schema)
    return {
        "minimal_matrix_files": _schema_list(schema, "minimal_matrix_files"),
        "keep_variables": _schema_list(schema, "keep_variables"),
        "required_variables": _schema_list(schema, "required_variables"),
        "compressed_variables": _schema_list(schema, "compressed_variables"),
        "global_attributes_to_keep": _schema_list(schema, "global_attributes_to_keep"),
        "forbidden_files": _schema_list(schema, "forbidden_files"),
        "forbidden_variables": _schema_list(schema, "forbidden_variables"),
        "minimal_catalog_columns": catalog_columns,
    }


def apply_minimal_schema(schema):
    global MINIMAL_PACKAGE_FILES
    global MINIMAL_MATRIX_FILES
    global MINIMAL_KEEP_VARS
    global MINIMAL_REQUIRED_VARS
    global COMPRESSED_MATRIX_VARS
    global GLOBAL_ATTRS_TO_KEEP
    global MINIMAL_FORBIDDEN_FILES
    global MINIMAL_FORBIDDEN_VARS
    global MINIMAL_CATALOG_COLUMNS
    global MINIMAL_STATION_CATALOG_COLUMNS
    global MINIMAL_SOURCE_STATION_CATALOG_COLUMNS
    global MINIMAL_SOURCE_DATASET_CATALOG_COLUMNS

    MINIMAL_MATRIX_FILES = schema["minimal_matrix_files"]
    MINIMAL_KEEP_VARS = schema["keep_variables"]
    MINIMAL_REQUIRED_VARS = schema["required_variables"]
    COMPRESSED_MATRIX_VARS = set(schema["compressed_variables"])
    GLOBAL_ATTRS_TO_KEEP = schema["global_attributes_to_keep"]
    MINIMAL_FORBIDDEN_FILES = schema["forbidden_files"]
    MINIMAL_FORBIDDEN_VARS = schema["forbidden_variables"]
    MINIMAL_CATALOG_COLUMNS = schema["minimal_catalog_columns"]
    MINIMAL_STATION_CATALOG_COLUMNS = MINIMAL_CATALOG_COLUMNS["station_catalog.csv"]
    MINIMAL_SOURCE_STATION_CATALOG_COLUMNS = MINIMAL_CATALOG_COLUMNS["source_station_catalog.csv"]
    MINIMAL_SOURCE_DATASET_CATALOG_COLUMNS = MINIMAL_CATALOG_COLUMNS["source_dataset_catalog.csv"]
    MINIMAL_PACKAGE_FILES = tuple(MINIMAL_MATRIX_FILES) + tuple(MINIMAL_CATALOG_COLUMNS)


try:
    apply_minimal_schema(load_minimal_schema(DEFAULT_SCHEMA_PATH))
except MinimalSchemaError:
    pass


def resolve_path(value, base=PROJECT_ROOT):
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if path.exists():
        return path.resolve()
    return (base / path).resolve()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR))
    parser.add_argument("--minimal-dir", default=str(DEFAULT_MINIMAL_DIR))
    parser.add_argument("--climatology-dir", default=str(DEFAULT_CLIMATOLOGY_DIR))
    parser.add_argument("--satellite-dir", default=str(DEFAULT_SATELLITE_DIR))
    parser.add_argument("--schema", default=str(DEFAULT_SCHEMA_PATH), help="Minimal package schema YAML")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output directories")
    parser.add_argument("--dry-run", action="store_true", help="Print planned actions without writing files")
    parser.add_argument("--skip-climatology", action="store_true", help="Skip climatology-only package")
    parser.add_argument("--skip-satellite", action="store_true", help="Skip satellite-validation package")
    parser.add_argument(
        "--compression-level",
        type=int,
        default=4,
        help="NetCDF compression level for future slimming implementation (default: 4)",
    )
    parser.add_argument(
        "--matrix-workers",
        type=int,
        default=3,
        help="Parallel workers for daily/monthly/annual minimal matrix NetCDF copies (default: 3)",
    )
    args = parser.parse_args(argv)

    args.release_dir = resolve_path(args.release_dir)
    args.minimal_dir = resolve_path(args.minimal_dir)
    args.climatology_dir = resolve_path(args.climatology_dir)
    args.satellite_dir = resolve_path(args.satellite_dir)
    args.schema = resolve_path(args.schema, base=SCRIPTS_DIR)

    try:
        apply_minimal_schema(load_minimal_schema(args.schema))
    except MinimalSchemaError as exc:
        parser.error(str(exc))

    if args.compression_level < 0 or args.compression_level > 9:
        parser.error("--compression-level must be between 0 and 9")
    if args.matrix_workers < 1:
        parser.error("--matrix-workers must be >= 1")

    return args


def validate_inputs(release_dir):
    print("[check] release dir: {}".format(release_dir))
    if not release_dir.is_dir():
        raise FileNotFoundError("Full S8 release directory not found: {}".format(release_dir))

    missing = []
    required_paths = []
    for name in MINIMAL_PACKAGE_FILES:
        path = release_dir / name
        required_paths.append(path)
        if not path.is_file():
            missing.append(path)

    if missing:
        print("[check] missing required release files:")
        for path in missing:
            print("  - {}".format(path))
        raise FileNotFoundError("Missing {} required release file(s)".format(len(missing)))

    print("[check] required release files: ok ({})".format(len(required_paths)))
    return required_paths


def prepare_output_dir(path, force=False, dry_run=False):
    print("[prepare] output dir: {}".format(path))
    if dry_run:
        if path.exists() and force:
            print("[dry-run] would remove and recreate {}".format(path))
        elif path.exists():
            print("[dry-run] would reuse existing empty directory or fail if non-empty: {}".format(path))
        else:
            print("[dry-run] would create {}".format(path))
        return

    if path.exists():
        if not path.is_dir():
            raise NotADirectoryError("Output path exists but is not a directory: {}".format(path))
        if any(path.iterdir()):
            if not force:
                raise FileExistsError(
                    "Output directory is not empty: {} (use --force to replace it)".format(path)
                )
            shutil.rmtree(path)

    path.mkdir(parents=True, exist_ok=True)


def _copy_global_attrs(src, dst):
    attrs = _minimal_global_attrs(src, src.variables.keys())
    for name in GLOBAL_ATTRS_TO_KEEP:
        dst.setncattr(name, attrs.get(name, ""))


def _copy_h5_global_attrs(src, dst):
    attrs = _minimal_global_attrs(src, src.variables.keys())
    for name in GLOBAL_ATTRS_TO_KEEP:
        dst.attrs[name] = attrs.get(name, "")


def _source_attr(src, name, default=""):
    if hasattr(src, "getncattr"):
        if name in src.ncattrs():
            return src.getncattr(name)
        return default
    return src.attrs.get(name, default)


def _source_var_attr(var, name, default=""):
    if hasattr(var, "getncattr"):
        if name in var.ncattrs():
            return var.getncattr(name)
        return default
    return var.attrs.get(name, default)


def _utc_iso8601_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_attr_value(value):
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _first_nonempty_attr(src, *names):
    for name in names:
        value = _clean_attr_value(_source_attr(src, name, ""))
        if value:
            return value
    return ""


def _release_provenance_nc_candidates(release_dir):
    names = (
        "sed_reference_master.nc",
        "sed_reference_timeseries_daily.nc",
        "sed_reference_timeseries_monthly.nc",
        "sed_reference_timeseries_annual.nc",
        "sed_reference_climatology.nc",
        "sed_reference_satellite.nc",
    )
    for name in names:
        path = release_dir / name
        if path.is_file():
            yield path


def _read_release_nc_attrs(path):
    if HAS_NC:
        with nc4.Dataset(path, "r") as ds:
            return {
                "source_release_version": _clean_attr_value(_source_attr(ds, "release_version", "")),
                "source_release_date_created": _clean_attr_value(_source_attr(ds, "date_created", "")),
                "source_release_date_modified": _clean_attr_value(_source_attr(ds, "date_modified", "")),
            }
    if HAS_H5NETCDF:
        with h5netcdf.File(path, "r") as ds:
            return {
                "source_release_version": _clean_attr_value(_source_attr(ds, "release_version", "")),
                "source_release_date_created": _clean_attr_value(_source_attr(ds, "date_created", "")),
                "source_release_date_modified": _clean_attr_value(_source_attr(ds, "date_modified", "")),
            }
    return {
        "source_release_version": "",
        "source_release_date_created": "",
        "source_release_date_modified": "",
    }


def read_release_provenance(release_dir, schema_path, package_created_at):
    provenance = {
        "source_release_directory": str(release_dir),
        "source_release_version": "",
        "source_release_date_created": "",
        "source_release_date_modified": "",
        "packaging_script": str(PACKAGING_SCRIPT),
        "schema_path": str(schema_path),
        "package_created_at": package_created_at,
    }

    found_nc = False
    for path in _release_provenance_nc_candidates(release_dir):
        found_nc = True
        try:
            attrs = _read_release_nc_attrs(path)
        except Exception as exc:
            _warn(BUILD_WARNINGS, "could not read release provenance from {}: {}".format(path, exc))
            continue
        for key, value in attrs.items():
            if value and not provenance[key]:
                provenance[key] = value
        if (
            provenance["source_release_version"]
            and provenance["source_release_date_created"]
            and provenance["source_release_date_modified"]
        ):
            return provenance

    if not found_nc:
        _warn(BUILD_WARNINGS, "no full release NetCDF found for provenance attributes in {}".format(release_dir))
    return provenance


def _history_created_time(src):
    history = _clean_attr_value(_source_attr(src, "history", ""))
    match = re.search(r"Created\s+([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+)", history)
    return match.group(1) if match else ""


def _valid_numeric_values(var):
    data = np.asarray(var[:])
    if np.ma.isMaskedArray(data):
        data = data.compressed()
    else:
        data = data.reshape(-1)
    if data.size == 0:
        return data
    data = data.astype(float, copy=False)
    fill_value = _source_var_attr(var, "_FillValue", None)
    mask = np.isfinite(data)
    if fill_value is not None:
        try:
            mask &= data != float(fill_value)
        except (TypeError, ValueError):
            pass
    return data[mask]


def _format_float(value):
    return "{:.8g}".format(float(value))


def _format_time_value(value, units, calendar):
    units = _clean_attr_value(units)
    calendar = _clean_attr_value(calendar) or "standard"
    if HAS_NC and nc4 is not None:
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

    match = re.match(r"^\s*(days|hours|minutes|seconds)\s+since\s+([0-9]{4}-[0-9]{2}-[0-9]{2})", units)
    if not match:
        return _format_float(value)
    unit_name, origin = match.groups()
    unit_map = {"days": "D", "hours": "h", "minutes": "m", "seconds": "s"}
    try:
        ts = pd.to_datetime(origin) + pd.to_timedelta(float(value), unit=unit_map[unit_name])
    except Exception:
        return _format_float(value)
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
        return ts.strftime("%Y-%m-%d")
    return ts.isoformat()


def _time_coverage_attrs(src):
    if "time" not in src.variables:
        return "", ""
    values = _valid_numeric_values(src.variables["time"])
    if values.size == 0:
        return "", ""
    time_var = src.variables["time"]
    units = _source_var_attr(time_var, "units", "")
    calendar = _source_var_attr(time_var, "calendar", "standard")
    return (
        _format_time_value(np.nanmin(values), units, calendar),
        _format_time_value(np.nanmax(values), units, calendar),
    )


def _geospatial_attrs(src):
    result = {}
    for var_name, min_key, max_key in (
        ("lat", "geospatial_lat_min", "geospatial_lat_max"),
        ("lon", "geospatial_lon_min", "geospatial_lon_max"),
    ):
        if var_name not in src.variables:
            result[min_key] = ""
            result[max_key] = ""
            continue
        values = _valid_numeric_values(src.variables[var_name])
        result[min_key] = _format_float(np.nanmin(values)) if values.size else ""
        result[max_key] = _format_float(np.nanmax(values)) if values.size else ""
    return result


def _qc_flag_meanings(src):
    mappings = []
    for name in ("Q_flag", "SSC_flag", "SSL_flag"):
        if name not in src.variables:
            continue
        flag_meanings = _clean_attr_value(_source_var_attr(src.variables[name], "flag_meanings", ""))
        if not flag_meanings:
            continue
        meanings = flag_meanings.split()
        flag_values = _source_var_attr(src.variables[name], "flag_values", None)
        if flag_values is not None:
            values = np.asarray(flag_values).reshape(-1).tolist()
        else:
            values = []
        if len(values) == len(meanings):
            value = "; ".join("{}={}".format(_format_flag_value(v), m) for v, m in zip(values, meanings))
        else:
            value = flag_meanings
        if value and value not in mappings:
            mappings.append(value)
    return " | ".join(mappings)


def _format_flag_value(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if numeric.is_integer():
        return str(int(numeric))
    return _format_float(numeric)


def _variables_provided(variable_names):
    primary = [name for name in ("Q", "SSC", "SSL") if name in variable_names]
    flags = [name for name in ("Q_flag", "SSC_flag", "SSL_flag") if name in variable_names]
    parts = []
    if primary:
        parts.append(", ".join(primary))
    if flags:
        parts.append("quality flags: {}".format(", ".join(flags)))
    return "; ".join(parts)


def _minimal_global_attrs(src, variable_names):
    attrs = {name: _clean_attr_value(_source_attr(src, name, "")) for name in GLOBAL_ATTRS_TO_KEEP}
    variable_names = set(variable_names)
    time_start, time_end = _time_coverage_attrs(src)
    geo = _geospatial_attrs(src)

    attrs["title"] = attrs["title"] or _first_nonempty_attr(src, "title")
    attrs["product_role"] = attrs["product_role"] or "minimal {} station-time matrix".format(
        _first_nonempty_attr(src, "time_type") or "resolution-specific"
    )
    attrs["release_version"] = attrs["release_version"] or _first_nonempty_attr(
        src, "release_version", "dataset_version"
    )
    attrs["date_created"] = attrs["date_created"] or _history_created_time(src)
    attrs["date_modified"] = attrs["date_modified"] or datetime.now().isoformat(timespec="seconds")
    attrs["Conventions"] = attrs["Conventions"] or _first_nonempty_attr(src, "Conventions", "conventions")
    attrs["summary"] = attrs["summary"] or (
        "Minimal station-by-time matrix product for river discharge, suspended sediment concentration, "
        "and suspended sediment load."
    )
    attrs["variables_provided"] = attrs["variables_provided"] or _variables_provided(variable_names)
    attrs["qc_flag_meanings"] = attrs["qc_flag_meanings"] or _qc_flag_meanings(src)
    attrs["time_coverage_start"] = attrs["time_coverage_start"] or time_start
    attrs["time_coverage_end"] = attrs["time_coverage_end"] or time_end
    for key, value in geo.items():
        attrs[key] = attrs[key] or value

    return attrs


def _copy_variable_attrs(src_var, dst_var):
    for name in src_var.ncattrs():
        if name == "_FillValue":
            continue
        dst_var.setncattr(name, src_var.getncattr(name))


def _create_output_variable(dst, name, src_var, compression_level):
    kwargs = {}
    if "_FillValue" in src_var.ncattrs():
        kwargs["fill_value"] = src_var.getncattr("_FillValue")

    if name in COMPRESSED_MATRIX_VARS:
        kwargs["zlib"] = True
        kwargs["complevel"] = compression_level

    return dst.createVariable(name, src_var.dtype, src_var.dimensions, **kwargs)


def _copy_variable_data(name, src_var, dst_var, station_chunk_size=128):
    if "n_stations" in src_var.dimensions and len(src_var.dimensions) >= 2:
        if src_var.dtype is str or src_var.dtype == str:
            station_chunk_size = 8
        station_axis = src_var.dimensions.index("n_stations")
        n_stations = src_var.shape[station_axis]
        print(
            "[copy] variable {} in station chunks of {}".format(name, station_chunk_size),
            flush=True,
        )
        for start in range(0, n_stations, station_chunk_size):
            stop = min(start + station_chunk_size, n_stations)
            slices = [slice(None)] * len(src_var.dimensions)
            slices[station_axis] = slice(start, stop)
            slices = tuple(slices)
            dst_var[slices] = src_var[slices]
    else:
        print("[copy] variable {}".format(name), flush=True)
        dst_var[:] = src_var[:]


def _copy_h5_variable_data(name, src_var, dst_var, station_chunk_size=128):
    if "n_stations" in src_var.dimensions and len(src_var.dimensions) >= 2:
        if src_var.dtype is str or src_var.dtype == str or src_var.dtype == object:
            station_chunk_size = 8
        station_axis = src_var.dimensions.index("n_stations")
        n_stations = src_var.shape[station_axis]
        print(
            "[copy] variable {} in station chunks of {}".format(name, station_chunk_size),
            flush=True,
        )
        for start in range(0, n_stations, station_chunk_size):
            stop = min(start + station_chunk_size, n_stations)
            slices = [slice(None)] * len(src_var.dimensions)
            slices[station_axis] = slice(start, stop)
            slices = tuple(slices)
            dst_var[slices] = src_var[slices]
    elif src_var.shape == ():
        print("[copy] variable {}".format(name), flush=True)
        dst_var[...] = src_var[()]
    else:
        print("[copy] variable {}".format(name), flush=True)
        dst_var[:] = src_var[:]


def _copy_minimal_matrix_nc_netCDF4(src_path, dst_path, keep_vars, required_vars, compression_level=4):
    if not HAS_NC:
        print("[fail] netCDF4 is not available")
        return False

    if not src_path.is_file():
        print("[fail] source NetCDF not found: {}".format(src_path))
        return False

    tmp_path = dst_path.with_name(dst_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    with nc4.Dataset(src_path, "r") as src:
        missing_required = [name for name in required_vars if name not in src.variables]
        if missing_required:
            print("[fail] {} missing required variables: {}".format(src_path.name, ", ".join(missing_required)))
            return False

        vars_to_copy = []
        for name in keep_vars:
            if name in src.variables:
                vars_to_copy.append(name)
            else:
                print("[warn] {} missing optional variable: {}".format(src_path.name, name))

        required_dims = []
        for name in vars_to_copy:
            for dim_name in src.variables[name].dimensions:
                if dim_name not in required_dims:
                    required_dims.append(dim_name)

        with nc4.Dataset(tmp_path, "w", format=src.data_model) as dst:
            _copy_global_attrs(src, dst)
            for dim_name in required_dims:
                dim = src.dimensions[dim_name]
                dim_size = None if dim.isunlimited() else len(dim)
                dst.createDimension(dim_name, dim_size)

            for name in vars_to_copy:
                src_var = src.variables[name]
                dst_var = _create_output_variable(dst, name, src_var, compression_level)
                _copy_variable_attrs(src_var, dst_var)
                _copy_variable_data(name, src_var, dst_var)

    if dst_path.exists():
        dst_path.unlink()
    tmp_path.rename(dst_path)
    print("[write] {}".format(dst_path))
    return True


def _copy_minimal_matrix_nc_h5netcdf(src_path, dst_path, keep_vars, required_vars, compression_level=4):
    if not HAS_H5NETCDF:
        print("[fail] h5netcdf is not available")
        return False

    if not src_path.is_file():
        print("[fail] source NetCDF not found: {}".format(src_path))
        return False

    tmp_path = dst_path.with_name(dst_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    with h5netcdf.File(src_path, "r") as src:
        missing_required = [name for name in required_vars if name not in src.variables]
        if missing_required:
            print("[fail] {} missing required variables: {}".format(src_path.name, ", ".join(missing_required)))
            return False

        vars_to_copy = []
        for name in keep_vars:
            if name in src.variables:
                vars_to_copy.append(name)
            else:
                print("[warn] {} missing optional variable: {}".format(src_path.name, name))

        required_dims = []
        for name in vars_to_copy:
            for dim_name in src.variables[name].dimensions:
                if dim_name not in required_dims:
                    required_dims.append(dim_name)

        with h5netcdf.File(tmp_path, "w") as dst:
            _copy_h5_global_attrs(src, dst)

            for dim_name in required_dims:
                dst.dimensions[dim_name] = len(src.dimensions[dim_name])

            for name in vars_to_copy:
                src_var = src.variables[name]
                fill_value = src_var.attrs.get("_FillValue", None)
                dtype = src_var._h5ds.dtype
                kwargs = {}
                if name in COMPRESSED_MATRIX_VARS:
                    kwargs["compression"] = "gzip"
                    kwargs["compression_opts"] = compression_level
                dst_var = dst.create_variable(
                    name,
                    dimensions=src_var.dimensions,
                    dtype=dtype,
                    fillvalue=fill_value,
                    **kwargs,
                )
                for attr_name, attr_value in src_var.attrs.items():
                    if attr_name == "_FillValue":
                        continue
                    dst_var.attrs[attr_name] = attr_value
                _copy_h5_variable_data(name, src_var, dst_var)

    if dst_path.exists():
        dst_path.unlink()
    tmp_path.rename(dst_path)
    print("[write] {}".format(dst_path))
    return True


def copy_minimal_matrix_nc(src_path, dst_path, keep_vars, required_vars, compression_level=4):
    print("[copy] {} -> {}".format(src_path, dst_path), flush=True)
    if HAS_NC:
        return _copy_minimal_matrix_nc_netCDF4(
            src_path,
            dst_path,
            keep_vars,
            required_vars,
            compression_level=compression_level,
        )
    if HAS_H5NETCDF:
        return _copy_minimal_matrix_nc_h5netcdf(
            src_path,
            dst_path,
            keep_vars,
            required_vars,
            compression_level=compression_level,
        )
    print("[fail] netCDF4 or h5netcdf is required to build minimal NetCDF files")
    return False


def _warn(warnings, message):
    warnings.append(message)
    print("[warn] {}".format(message))


def _read_catalog_csv(path):
    return pd.read_csv(path, keep_default_na=False)


def _filter_minimal_resolutions(df):
    if "resolution" not in df.columns:
        return df.iloc[0:0].copy()
    resolution = df["resolution"].astype(str).str.strip().str.lower()
    return df[resolution.isin(MINIMAL_RESOLUTIONS)].copy()


def _ensure_columns(df, columns, warnings, catalog_name):
    for column in columns:
        if column not in df.columns:
            df[column] = ""
            _warn(warnings, "{} missing optional column {}; filled empty values".format(catalog_name, column))
    return df


def slim_station_catalog(src, dst, warnings):
    print("[catalog] slimming station_catalog.csv")
    df = _read_catalog_csv(src)
    df = _filter_minimal_resolutions(df)

    if "n_valid_time_steps" not in df.columns and "record_count" in df.columns:
        df["n_valid_time_steps"] = df["record_count"]
        _warn(warnings, "station_catalog.csv missing n_valid_time_steps; copied from record_count")

    if "country" not in df.columns:
        df["country"] = ""
        _warn(warnings, "station_catalog.csv missing country; filled empty values")

    df = _ensure_columns(
        df,
        MINIMAL_STATION_CATALOG_COLUMNS,
        warnings,
        "station_catalog.csv",
    )
    df = df.loc[:, MINIMAL_STATION_CATALOG_COLUMNS]
    df = df.sort_values(["resolution", "cluster_uid"], kind="mergesort").reset_index(drop=True)
    df.to_csv(dst, index=False)
    print("[write] {}".format(dst))


def slim_source_station_catalog(src, dst, warnings):
    print("[catalog] slimming source_station_catalog.csv")
    df = _read_catalog_csv(src)
    df = _filter_minimal_resolutions(df)
    df = _ensure_columns(
        df,
        MINIMAL_SOURCE_STATION_CATALOG_COLUMNS,
        warnings,
        "source_station_catalog.csv",
    )
    df = df.loc[:, MINIMAL_SOURCE_STATION_CATALOG_COLUMNS]
    df = df.sort_values(
        ["resolution", "cluster_uid", "source_name", "source_station_uid"],
        kind="mergesort",
    ).reset_index(drop=True)
    df.to_csv(dst, index=False)
    print("[write] {}".format(dst))


def slim_source_dataset_catalog(src, dst, warnings):
    print("[catalog] slimming source_dataset_catalog.csv")
    df = _read_catalog_csv(src)
    df = _ensure_columns(
        df,
        MINIMAL_SOURCE_DATASET_CATALOG_COLUMNS,
        warnings,
        "source_dataset_catalog.csv",
    )
    df = df.loc[:, MINIMAL_SOURCE_DATASET_CATALOG_COLUMNS]
    df = df.sort_values(["source_name"], kind="mergesort").reset_index(drop=True)
    df.to_csv(dst, index=False)
    print("[write] {}".format(dst))


def build_minimal_catalogs(args, warnings):
    catalog_jobs = (
        ("station_catalog.csv", slim_station_catalog),
        ("source_station_catalog.csv", slim_source_station_catalog),
        ("source_dataset_catalog.csv", slim_source_dataset_catalog),
    )
    if args.dry_run:
        for name, _ in catalog_jobs:
            print("[dry-run] would build minimal catalog CSV: {}".format(args.minimal_dir / name))
        return

    for name, func in catalog_jobs:
        func(args.release_dir / name, args.minimal_dir / name, warnings)


def write_inventory(
    package_dir,
    package_name,
    release_dir,
    source_files,
    provenance,
    dry_run=False,
    inventory_name="release_inventory.csv",
    status="copied",
):
    inventory_path = package_dir / inventory_name
    rows = []
    for name in source_files:
        source_path = release_dir / name
        if package_name == "sed_reference_release_minimal":
            row_status = "minimal_nc" if name in MINIMAL_MATRIX_FILES else "minimal_catalog"
        else:
            row_status = status if source_path.is_file() else "missing_source"
        rows.append(
            {
                "package": package_name,
                "file": name,
                "source_path": str(source_path),
                "source_exists": bool(source_path.is_file()),
                "status": row_status,
                "source_release_version": provenance["source_release_version"],
                "source_release_date_created": provenance["source_release_date_created"],
                "source_release_date_modified": provenance["source_release_date_modified"],
                "packaging_script": provenance["packaging_script"],
                "schema_path": provenance["schema_path"],
                "package_created_at": provenance["package_created_at"],
            }
        )

    if dry_run:
        print("[dry-run] would write inventory: {}".format(inventory_path))
        return

    pd.DataFrame(rows).to_csv(inventory_path, index=False)
    print("[write] {}".format(inventory_path))


def _readme_provenance_block(provenance, package_role):
    lines = [
        "- Source release directory: `{}`".format(provenance["source_release_directory"]),
        "- Source release version: `{}`".format(provenance["source_release_version"]),
    ]
    if provenance["source_release_date_created"]:
        lines.append("- Source release date_created: `{}`".format(provenance["source_release_date_created"]))
    if provenance["source_release_date_modified"]:
        lines.append("- Source release date_modified: `{}`".format(provenance["source_release_date_modified"]))
    lines.extend(
        [
            "- Packaging script path: `{}`".format(provenance["packaging_script"]),
            "- Schema path: `{}`".format(provenance["schema_path"]),
            "- Package role: {}".format(package_role),
            "",
        ]
    )
    return "\n".join(lines)


def write_readme(package_dir, package_name, release_dir, provenance, compression_level=None, dry_run=False):
    readme_path = package_dir / "README.md"
    if package_name == "sed_reference_release_minimal":
        package_role = "minimal station-reference matrix package for daily/monthly/annual use."
        text = """# sed_reference_release_minimal

Generated by `tools/build_minimal_release_package.py`.

{provenance_block}
- Matrix files keep selected user-facing fields and omit master, climatology, satellite,
  overlap-candidate, parquet, and GPKG products.
- Requested NetCDF compression level: `{compression_level}`

""".format(
            provenance_block=_readme_provenance_block(provenance, package_role),
            compression_level=compression_level,
        )
    elif package_name == "sed_reference_release_climatology":
        package_role = "standalone climatology package."
        text = """# sed_reference_release_climatology

Generated by `tools/build_minimal_release_package.py`.

{provenance_block}
- Use this package separately from the daily/monthly/annual matrix minimal package.
- NetCDF file is copied from the full release without slimming.

""".format(
            provenance_block=_readme_provenance_block(provenance, package_role),
        )
    elif package_name == "sed_reference_release_satellite":
        package_role = "satellite validation-only package."
        text = """# sed_reference_release_satellite

Generated by `tools/build_minimal_release_package.py`.

{provenance_block}
- Satellite data are retained for validation and do not enter the main station-reference merge.
- NetCDF and catalog files are copied from the full release without slimming.

""".format(
            provenance_block=_readme_provenance_block(provenance, package_role),
        )
    else:
        package_role = package_name
        text = """# {package_name}

Generated by `tools/build_minimal_release_package.py`.

{provenance_block}

""".format(
            package_name=package_name,
            provenance_block=_readme_provenance_block(provenance, package_role),
        )

    if dry_run:
        print("[dry-run] would write README: {}".format(readme_path))
        return

    readme_path.write_text(text, encoding="utf-8")
    print("[write] {}".format(readme_path))


def copy_release_file(src, dst, dry_run=False):
    if dry_run:
        print("[dry-run] would copy {} -> {}".format(src, dst))
        return
    shutil.copy2(src, dst)
    print("[copy] {} -> {}".format(src, dst))


def _build_copy_package(package_name, package_dir, release_dir, source_files, inventory_name, args):
    print("[build] {} package".format(package_name))
    missing = [name for name in source_files if not (release_dir / name).is_file()]
    if missing:
        _warn(
            BUILD_WARNINGS,
            "{} missing source file(s): {}".format(package_name, ", ".join(missing)),
        )

    prepare_output_dir(package_dir, force=args.force, dry_run=args.dry_run)

    for name in source_files:
        src = release_dir / name
        if src.is_file():
            copy_release_file(src, package_dir / name, dry_run=args.dry_run)
        else:
            print("[warn] skip missing optional package source: {}".format(src))

    write_inventory(
        package_dir,
        package_name,
        release_dir,
        source_files,
        args.release_provenance,
        dry_run=args.dry_run,
        inventory_name=inventory_name,
        status="copied_from_full_release",
    )
    write_readme(
        package_dir,
        package_name,
        release_dir,
        args.release_provenance,
        dry_run=args.dry_run,
    )


def _copy_minimal_matrix_worker(payload):
    (
        name,
        release_dir,
        minimal_dir,
        keep_vars,
        required_vars,
        compressed_vars,
        global_attrs_to_keep,
        compression_level,
    ) = payload
    global COMPRESSED_MATRIX_VARS
    global GLOBAL_ATTRS_TO_KEEP
    COMPRESSED_MATRIX_VARS = set(compressed_vars)
    GLOBAL_ATTRS_TO_KEEP = tuple(global_attrs_to_keep)
    ok = copy_minimal_matrix_nc(
        release_dir / name,
        minimal_dir / name,
        keep_vars,
        required_vars,
        compression_level=compression_level,
    )
    return name, ok


def _matrix_variables(path):
    if HAS_NC:
        with nc4.Dataset(path, "r") as ds:
            return list(ds.variables)
    if HAS_H5NETCDF:
        with h5netcdf.File(path, "r") as ds:
            return list(ds.variables)
    raise RuntimeError("netCDF4 or h5netcdf is required to inspect NetCDF files")


def _matrix_global_attr_names(path):
    if HAS_NC:
        with nc4.Dataset(path, "r") as ds:
            return list(ds.ncattrs())
    if HAS_H5NETCDF:
        with h5netcdf.File(path, "r") as ds:
            return list(ds.attrs.keys())
    raise RuntimeError("netCDF4 or h5netcdf is required to inspect NetCDF files")


def validate_minimal_package(args):
    report_path = args.minimal_dir / "minimal_release_validation_report.csv"
    if args.dry_run:
        print("[dry-run] would write validation report: {}".format(report_path))
        return

    rows = []

    def add(check, status, message, evidence=""):
        rows.append(
            {
                "check": check,
                "status": status,
                "message": message,
                "evidence": evidence,
            }
        )

    for name in MINIMAL_PACKAGE_FILES:
        path = args.minimal_dir / name
        add(
            "required_file:{}".format(name),
            "pass" if path.is_file() else "fail",
            "required minimal file present" if path.is_file() else "required minimal file missing",
            str(path),
        )

    for name in MINIMAL_FORBIDDEN_FILES:
        path = args.minimal_dir / name
        add(
            "forbidden_file:{}".format(name),
            "fail" if path.exists() else "pass",
            "forbidden file absent" if not path.exists() else "forbidden file present",
            str(path),
        )

    gpkg_files = sorted(path.name for path in args.minimal_dir.glob("*.gpkg"))
    add(
        "forbidden_file_type:gpkg",
        "fail" if gpkg_files else "pass",
        "no GPKG files in minimal package" if not gpkg_files else "GPKG files found",
        ";".join(gpkg_files),
    )

    overlap_candidate_files = sorted(
        path.name
        for path in args.minimal_dir.iterdir()
        if "overlap" in path.name.lower() and "candidate" in path.name.lower()
    )
    add(
        "forbidden_file_type:overlap_candidates",
        "fail" if overlap_candidate_files else "pass",
        "no overlap candidate files in minimal package"
        if not overlap_candidate_files
        else "overlap candidate files found",
        ";".join(overlap_candidate_files),
    )

    parquet_files = sorted(path.name for path in args.minimal_dir.glob("*.parquet"))
    add(
        "forbidden_file_type:parquet",
        "fail" if parquet_files else "pass",
        "no parquet files in minimal package" if not parquet_files else "parquet files found",
        ";".join(parquet_files),
    )

    for name in MINIMAL_MATRIX_FILES:
        matrix_path = args.minimal_dir / name
        if not matrix_path.is_file():
            add(
                "matrix_variables:{}".format(name),
                "fail",
                "matrix file missing; cannot inspect variables",
                str(matrix_path),
            )
            continue
        try:
            variables = _matrix_variables(matrix_path)
        except Exception as exc:
            add(
                "matrix_variables:{}".format(name),
                "fail",
                "cannot inspect matrix variables",
                str(exc),
            )
            continue
        forbidden_present = [name for name in MINIMAL_FORBIDDEN_VARS if name in variables]
        add(
            "forbidden_matrix_vars:{}".format(name),
            "fail" if forbidden_present else "pass",
            "forbidden matrix variables absent"
            if not forbidden_present
            else "forbidden matrix variables present",
            ";".join(forbidden_present),
        )
        try:
            attr_names = _matrix_global_attr_names(matrix_path)
        except Exception as exc:
            add(
                "matrix_global_attrs:{}".format(name),
                "fail",
                "cannot inspect matrix global attributes",
                str(exc),
            )
            continue
        attr_name_set = set(attr_names)
        missing_attrs = [attr_name for attr_name in GLOBAL_ATTRS_TO_KEEP if attr_name not in attr_name_set]
        add(
            "matrix_global_attrs:{}".format(name),
            "fail" if missing_attrs else "pass",
            "required matrix global attributes present"
            if not missing_attrs
            else "required matrix global attributes missing",
            ";".join(missing_attrs),
        )
        expected_order = list(GLOBAL_ATTRS_TO_KEEP)
        add(
            "matrix_global_attr_order:{}".format(name),
            "pass" if attr_names == expected_order else "fail",
            "matrix global attributes follow required order"
            if attr_names == expected_order
            else "matrix global attributes are out of order",
            "expected={}; actual={}".format(
                "|".join(expected_order),
                "|".join(attr_names),
            ),
        )

    df = pd.DataFrame(rows)
    df.to_csv(report_path, index=False)
    print("[write] {}".format(report_path))

    status_counts = df["status"].value_counts().to_dict()
    if status_counts.get("fail", 0):
        BUILD_FAILURES.append(
            "minimal validation failed: {} failing check(s)".format(status_counts.get("fail", 0))
        )
    if status_counts.get("warning", 0):
        BUILD_WARNINGS.append(
            "minimal validation warning: {} warning check(s)".format(status_counts.get("warning", 0))
        )


def build_minimal_package(args):
    package_name = "sed_reference_release_minimal"
    print("[build] {} package".format(package_name))
    prepare_output_dir(args.minimal_dir, force=args.force, dry_run=args.dry_run)

    if args.dry_run:
        for name in MINIMAL_MATRIX_FILES:
            print("[dry-run] would build minimal matrix NetCDF: {}".format(args.minimal_dir / name))
        print("[dry-run] matrix workers: {}".format(min(args.matrix_workers, len(MINIMAL_MATRIX_FILES))))
    else:
        worker_count = min(args.matrix_workers, len(MINIMAL_MATRIX_FILES))
        print("[build] matrix workers: {}".format(worker_count))
        payloads = [
            (
                name,
                args.release_dir,
                args.minimal_dir,
                MINIMAL_KEEP_VARS,
                MINIMAL_REQUIRED_VARS,
                tuple(COMPRESSED_MATRIX_VARS),
                GLOBAL_ATTRS_TO_KEEP,
                args.compression_level,
            )
            for name in MINIMAL_MATRIX_FILES
        ]
        if worker_count == 1:
            for payload in payloads:
                name, ok = _copy_minimal_matrix_worker(payload)
                if not ok:
                    BUILD_FAILURES.append("minimal matrix failed: {}".format(name))
        else:
            with ProcessPoolExecutor(max_workers=worker_count) as executor:
                future_to_name = {
                    executor.submit(_copy_minimal_matrix_worker, payload): payload[0]
                    for payload in payloads
                }
                for future in as_completed(future_to_name):
                    name = future_to_name[future]
                    try:
                        _, ok = future.result()
                    except Exception as exc:
                        ok = False
                        print("[fail] minimal matrix {} raised: {}".format(name, exc))
                    if ok:
                        print("[done] minimal matrix: {}".format(name))
                    else:
                        BUILD_FAILURES.append("minimal matrix failed: {}".format(name))

    build_minimal_catalogs(args, BUILD_WARNINGS)

    write_inventory(
        args.minimal_dir,
        package_name,
        args.release_dir,
        MINIMAL_PACKAGE_FILES,
        args.release_provenance,
        dry_run=args.dry_run,
    )
    write_readme(
        args.minimal_dir,
        package_name,
        args.release_dir,
        args.release_provenance,
        compression_level=args.compression_level,
        dry_run=args.dry_run,
    )
    validate_minimal_package(args)


def build_climatology_package(args):
    _build_copy_package(
        "sed_reference_release_climatology",
        args.climatology_dir,
        args.release_dir,
        CLIMATOLOGY_PACKAGE_FILES,
        "climatology_release_inventory.csv",
        args,
    )


def build_satellite_package(args):
    _build_copy_package(
        "sed_reference_release_satellite",
        args.satellite_dir,
        args.release_dir,
        SATELLITE_PACKAGE_FILES,
        "satellite_release_inventory.csv",
        args,
    )


def main(argv=None):
    args = parse_args(argv)

    print("[config] full release dir:       {}".format(args.release_dir))
    print("[config] minimal output dir:     {}".format(args.minimal_dir))
    print("[config] climatology output dir: {}".format(args.climatology_dir))
    print("[config] satellite output dir:   {}".format(args.satellite_dir))
    print("[config] minimal schema:         {}".format(args.schema))
    print("[config] compression level:      {}".format(args.compression_level))
    print("[config] matrix workers:         {}".format(args.matrix_workers))
    print("[config] dry run:                {}".format(args.dry_run))
    print("[config] force:                  {}".format(args.force))
    print("[config] netCDF4 available:      {}".format(HAS_NC))
    print("[config] h5netcdf available:    {}".format(HAS_H5NETCDF))

    args.package_created_at = _utc_iso8601_now()
    print("[config] package created at:     {}".format(args.package_created_at))

    validate_inputs(args.release_dir)
    args.release_provenance = read_release_provenance(
        args.release_dir,
        args.schema,
        args.package_created_at,
    )
    build_minimal_package(args)

    if args.skip_climatology:
        print("[skip] climatology package")
    else:
        build_climatology_package(args)

    if args.skip_satellite:
        print("[skip] satellite package")
    else:
        build_satellite_package(args)

    if BUILD_WARNINGS:
        print("[warn] {} build warning(s):".format(len(BUILD_WARNINGS)))
        for item in BUILD_WARNINGS:
            print("  - {}".format(item))

    if BUILD_FAILURES:
        print("[fail] {} build failure(s):".format(len(BUILD_FAILURES)))
        for item in BUILD_FAILURES:
            print("  - {}".format(item))
        return 1

    print("[done] post-release packages completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
