#!/usr/bin/env python3
"""Shared CF-1.8 and ACDD-1.3 metadata normalization for release NetCDF products."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - exercised by callers in lean envs
    nc4 = None



from release_netcdf_schema import (
    ACDD_CONVENTION,
    ACDD_PARITY_GLOBAL_ATTRS,
    ACDD_PUBLICATION_DEPENDENT_ATTRS,
    CF_CONVENTION,
    CF_PARITY_ATTRS,
    COORD_ATTRS,
    FLAG_MEANINGS,
    FLAG_VALUES,
    PRODUCT_ALIASES,
    PRODUCT_DESCRIPTIONS,
    RELEASE_ACDD_CONFIG,
    SCIENCE_LONG_NAMES,
    SCIENCE_UNITS,
    SUPPORTED_PRODUCT_KINDS,
    VARIABLE_PARITY_ATTRS,
)

# Backward-compatible private aliases for internal use
_SCIENCE_UNITS = SCIENCE_UNITS
_SCIENCE_LONG_NAMES = SCIENCE_LONG_NAMES
_COORD_ATTRS = COORD_ATTRS
_PRODUCT_ALIASES = PRODUCT_ALIASES
_SUPPORTED_PRODUCT_KINDS = SUPPORTED_PRODUCT_KINDS

def apply_cf18_metadata(nc_path, product_kind):
    """Normalize CF-1.8 metadata in-place without changing NetCDF data arrays."""
    _require_nc4()
    path = Path(nc_path)
    kind = _normalize_product_kind(product_kind)
    with nc4.Dataset(path, "r+") as ds:
        _merge_conventions(ds, (CF_CONVENTION,))
        if kind in {"daily_matrix", "monthly_matrix", "annual_matrix"}:
            _apply_matrix_metadata(ds)
        elif kind == "climatology":
            _apply_indexed_timeseries_metadata(
                ds,
                feature_id_var="station_uid",
                index_var="station_index",
                instance_dim="n_stations",
                index_dim="n_records",
            )
        elif kind == "satellite":
            _apply_indexed_timeseries_metadata(
                ds,
                feature_id_var="satellite_station_uid",
                index_var="satellite_station_index",
                instance_dim="n_satellite_stations",
                index_dim="n_satellite_records",
                ensure_science_long_names=True,
            )
        elif kind == "master":
            _apply_master_metadata(ds)
        _append_history_once(ds, kind)
        ds.sync()
    return path


def apply_acdd13_metadata(nc_path, product_kind, release_context=None):
    """Normalize ACDD-1.3 discovery metadata in-place for a release NetCDF."""
    _require_nc4()
    path = Path(nc_path)
    kind = _normalize_product_kind(product_kind)
    release_context = dict(release_context or {})
    with nc4.Dataset(path, "r+") as ds:
        _merge_conventions(ds, (ACDD_CONVENTION,))
        _apply_acdd_global_attrs(ds, kind, release_context)
        _apply_acdd_variable_attrs(ds)
        _append_history_once(ds, kind, convention_label="ACDD-1.3")
        ds.sync()
    return path


def apply_release_conventions(
    nc_path,
    product_kind,
    enable_cf18=True,
    enable_acdd13=True,
    release_context=None,
):
    """Apply all release metadata conventions through one idempotent entrypoint."""
    if enable_cf18:
        apply_cf18_metadata(nc_path, product_kind)
    if enable_acdd13:
        apply_acdd13_metadata(nc_path, product_kind, release_context=release_context)
    return Path(nc_path)


def audit_cf18_metadata(nc_path, product_kind):
    """Return structured CF-1.8 metadata checks for one release NetCDF product."""
    _require_nc4()
    path = Path(nc_path)
    kind = _normalize_product_kind(product_kind)
    rows = []
    with nc4.Dataset(path, "r") as ds:
        _audit_global_conventions(ds, rows)
        if kind in {"daily_matrix", "monthly_matrix", "annual_matrix"}:
            _audit_expected_attrs(
                ds,
                rows,
                _matrix_expected_attrs(ds),
                product_kind=kind,
            )
        elif kind == "climatology":
            _audit_expected_attrs(
                ds,
                rows,
                _indexed_timeseries_expected_attrs(
                    "station_uid",
                    "station_index",
                    "n_stations",
                ),
                product_kind=kind,
            )
            _audit_no_fabricated_time_bounds(ds, rows, kind)
        elif kind == "satellite":
            _audit_expected_attrs(
                ds,
                rows,
                _indexed_timeseries_expected_attrs(
                    "satellite_station_uid",
                    "satellite_station_index",
                    "n_satellite_stations",
                    require_science_long_names=True,
                ),
                product_kind=kind,
            )
        elif kind == "master":
            _audit_expected_attrs(
                ds,
                rows,
                _master_generic_expected_attrs(),
                product_kind=kind,
            )
            _audit_master_dsg_status(ds, rows)
    return rows


def audit_acdd13_metadata(nc_path, product_kind):
    """Return structured ACDD-1.3 discovery metadata checks for one product."""
    _require_nc4()
    path = Path(nc_path)
    kind = _normalize_product_kind(product_kind)
    rows = []
    with nc4.Dataset(path, "r") as ds:
        _audit_acdd_required(ds, rows)
        _audit_acdd_computed(ds, rows)
        _audit_acdd_supported(ds, rows)
        _audit_acdd_publication_dependent(ds, rows)
        _audit_acdd_variable_attrs(ds, rows)
        _audit_acdd_product_shape(ds, kind, rows)
    return rows


def audit_release_conventions(nc_path, product_kind):
    """Audit CF and ACDD release metadata for one product."""
    rows = []
    rows.extend(audit_cf18_metadata(nc_path, product_kind))
    rows.extend(audit_acdd13_metadata(nc_path, product_kind))
    return rows


def audit_cf_attribute_parity(full_nc_path, minimal_nc_path, attrs=CF_PARITY_ATTRS):
    """Check selected variable attributes match for variables present in both files."""
    _require_nc4()
    rows = []
    full_nc_path = Path(full_nc_path)
    minimal_nc_path = Path(minimal_nc_path)
    with nc4.Dataset(full_nc_path, "r") as full, nc4.Dataset(minimal_nc_path, "r") as minimal:
        common_variables = sorted(set(full.variables) & set(minimal.variables))
        mismatches = []
        for var_name in common_variables:
            src_var = full.variables[var_name]
            dst_var = minimal.variables[var_name]
            for attr_name in attrs:
                left = _variable_attr(src_var, attr_name)
                right = _variable_attr(dst_var, attr_name)
                if not _values_equal(left, right):
                    mismatches.append(
                        "{}.{} full={} minimal={}".format(
                            var_name,
                            attr_name,
                            _format_attr_value(left),
                            _format_attr_value(right),
                        )
                    )
        rows.append(
            {
                "check": "cf_variable_attribute_parity",
                "status": "pass" if not mismatches else "fail",
                "details": "common_variables={} mismatches={}".format(
                    len(common_variables),
                    len(mismatches),
                )
                if not mismatches
                else "; ".join(mismatches[:12]),
            }
        )
    return rows


def audit_release_attribute_parity(
    full_nc_path,
    minimal_nc_path,
    global_attrs=ACDD_PARITY_GLOBAL_ATTRS,
    variable_attrs=VARIABLE_PARITY_ATTRS,
):
    """Check full/minimal parity for inherited release metadata."""
    _require_nc4()
    rows = []
    with nc4.Dataset(full_nc_path, "r") as full, nc4.Dataset(minimal_nc_path, "r") as minimal:
        mismatches = []
        for attr_name in global_attrs:
            left = _object_attr(full, attr_name)
            right = _object_attr(minimal, attr_name)
            if not _values_equal(left, right):
                mismatches.append(
                    "{} full={} minimal={}".format(
                        attr_name,
                        _format_attr_value(left),
                        _format_attr_value(right),
                    )
                )
        rows.append(
            {
                "check": "release_global_attribute_parity",
                "status": "pass" if not mismatches else "fail",
                "details": "checked={} mismatches={}".format(len(global_attrs), len(mismatches))
                if not mismatches
                else "; ".join(mismatches[:12]),
            }
        )
    rows.extend(audit_cf_attribute_parity(full_nc_path, minimal_nc_path, attrs=variable_attrs))
    return rows


def _require_nc4():
    if nc4 is None:
        raise RuntimeError("netCDF4 is required for release NetCDF CF metadata normalization")


def _normalize_product_kind(product_kind):
    text = str(product_kind or "").strip().lower().replace("-", "_")
    kind = _PRODUCT_ALIASES.get(text, text)
    if kind not in _SUPPORTED_PRODUCT_KINDS:
        raise ValueError("Unsupported NetCDF release product kind: {}".format(product_kind))
    return kind


def _utc_now_text():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _merge_conventions(ds, required):
    existing = str(getattr(ds, "Conventions", "") or "")
    raw_tokens = existing.replace(",", " ").split()
    tokens = []
    seen = set()
    for token in raw_tokens:
        normalized = _canonical_convention_token(token)
        key = normalized.lower()
        if key not in seen:
            tokens.append(normalized)
            seen.add(key)
    for token in required:
        normalized = _canonical_convention_token(token)
        key = normalized.lower()
        if key not in seen:
            tokens.append(normalized)
            seen.add(key)
    _set_attr(ds, "Conventions", ", ".join(tokens))


def _canonical_convention_token(token):
    text = str(token or "").strip()
    if text.lower() == CF_CONVENTION.lower():
        return CF_CONVENTION
    if text.lower() == ACDD_CONVENTION.lower():
        return ACDD_CONVENTION
    return text


def _append_history_once(ds, product_kind, convention_label="CF-1.8"):
    marker = "{} metadata normalized by release_netcdf_conventions.py for {}".format(
        convention_label,
        product_kind,
    )
    history = str(getattr(ds, "history", "") or "").strip()
    if marker in history:
        return
    entry = "{}: {}".format(_utc_now_text(), marker)
    _set_attr(ds, "history", history + "\n" + entry if history else entry)


def _set_attr(obj, name, value, only_if_missing=False):
    if only_if_missing:
        current = _object_attr(obj, name)
        if current not in (None, ""):
            return
    if not _values_equal(_object_attr(obj, name), value):
        setattr(obj, name, value)


def _set_attr_if_empty(obj, name, value):
    if value is None:
        value = ""
    if _clean_text(_object_attr(obj, name)):
        return
    _set_attr(obj, name, value)


def _object_attr(obj, name):
    try:
        if hasattr(obj, "ncattrs") and name not in obj.ncattrs():
            return None
        return getattr(obj, name)
    except AttributeError:
        return None


def _variable_attr(var, name):
    if name not in var.ncattrs():
        return None
    return getattr(var, name)


def _values_equal(left, right):
    left_norm = _canonical_attr_value(left)
    right_norm = _canonical_attr_value(right)
    return left_norm == right_norm


def _canonical_attr_value(value):
    if value is None:
        return None
    if isinstance(value, np.ndarray):
        return tuple(_canonical_attr_value(item) for item in value.reshape(-1).tolist())
    if isinstance(value, (list, tuple)):
        return tuple(_canonical_attr_value(item) for item in value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _format_attr_value(value):
    value = _canonical_attr_value(value)
    if value is None:
        return "(missing)"
    if isinstance(value, tuple):
        return "[" + ",".join(str(item) for item in value) + "]"
    return str(value)


def _set_variable_attrs(ds, var_name, attrs, only_if_missing=()):
    if var_name not in ds.variables:
        return
    var = ds.variables[var_name]
    only_if_missing = set(only_if_missing or ())
    for attr_name, value in attrs.items():
        _set_attr(var, attr_name, value, only_if_missing=attr_name in only_if_missing)


def _set_coordinate_attrs(ds):
    for var_name, attrs in _COORD_ATTRS.items():
        _set_variable_attrs(ds, var_name, attrs)
    if "time" in ds.variables:
        time_var = ds.variables["time"]
        if not str(getattr(time_var, "units", "") or "").strip():
            _set_attr(time_var, "units", "days since 1970-01-01")
        if not str(getattr(time_var, "calendar", "") or "").strip():
            _set_attr(time_var, "calendar", "gregorian")


def _set_science_attrs(ds, ensure_long_names=False):
    for var_name, units in _SCIENCE_UNITS.items():
        attrs = {
            "units": units,
            "coordinates": "time lat lon",
            "ancillary_variables": "{}_flag".format(var_name),
        }
        if ensure_long_names:
            attrs["long_name"] = _SCIENCE_LONG_NAMES[var_name]
        _set_variable_attrs(
            ds,
            var_name,
            attrs,
            only_if_missing=("long_name",) if ensure_long_names else (),
        )
    for var_name in ("Q_flag", "SSC_flag", "SSL_flag"):
        _set_variable_attrs(
            ds,
            var_name,
            {
                "flag_values": FLAG_VALUES,
                "flag_meanings": FLAG_MEANINGS,
            },
        )


def _apply_acdd_global_attrs(ds, kind, release_context):
    now = _utc_now_text()
    description = PRODUCT_DESCRIPTIONS[kind]
    release_version = _release_version(ds, release_context)

    _merge_conventions(ds, (CF_CONVENTION, ACDD_CONVENTION))
    _set_attr(ds, "id", "org.sysu.sed_reference.{}.{}".format(kind, release_version))
    _set_attr(ds, "naming_authority", RELEASE_ACDD_CONFIG["naming_authority"])
    _set_attr(ds, "date_modified", now)
    _set_attr(ds, "date_metadata_modified", now)

    _set_attr_if_empty(ds, "release_version", release_version if release_version != "unversioned" else "")
    _set_attr_if_empty(ds, "title", description["title"])
    _set_attr_if_empty(ds, "summary", description["summary"])
    _set_attr_if_empty(ds, "keywords", RELEASE_ACDD_CONFIG["keywords"])
    _set_attr_if_empty(ds, "creator_name", _context_or_config(release_context, "creator_name"))
    _set_attr_if_empty(ds, "creator_email", _context_or_config(release_context, "creator_email"))
    _set_attr_if_empty(ds, "creator_institution", _context_or_config(release_context, "creator_institution"))
    _set_attr_if_empty(ds, "institution", _context_or_config(release_context, "institution"))
    _set_attr_if_empty(ds, "processing_level", RELEASE_ACDD_CONFIG["processing_level"])
    _set_attr_if_empty(ds, "standard_name_vocabulary", RELEASE_ACDD_CONFIG["standard_name_vocabulary"])
    _set_attr_if_empty(ds, "acknowledgement", _context_or_config(release_context, "acknowledgement"))
    _set_attr_if_empty(ds, "source", _product_source_text(kind))
    _set_attr_if_empty(ds, "comment", description["comment"])
    _set_attr_if_empty(ds, "date_created", _history_created_time(ds) or now)

    for attr_name in ACDD_PUBLICATION_DEPENDENT_ATTRS:
        value = _clean_text(release_context.get(attr_name, "") or RELEASE_ACDD_CONFIG.get(attr_name, ""))
        if value:
            _set_attr_if_empty(ds, attr_name, value)
        else:
            _set_attr_if_empty(ds, attr_name, "")

    resolution = description["time_coverage_resolution"]
    if resolution:
        _set_attr(ds, "time_coverage_resolution", resolution)
    else:
        _set_attr(ds, "time_coverage_resolution", "")

    geo = _geospatial_metadata(ds)
    geo.pop("geospatial_bounds", None)
    for attr_name, value in geo.items():
        _set_attr(ds, attr_name, value)
    temporal = _time_coverage_metadata(ds)
    temporal.pop("time_coverage_duration", None)
    for attr_name, value in temporal.items():
        _set_attr(ds, attr_name, value)


def _apply_acdd_variable_attrs(ds):
    for var_name in ds.variables:
        var = ds.variables[var_name]
        ctype = _coverage_content_type(var_name, var)
        if ctype:
            _set_attr(var, "coverage_content_type", ctype)
    for var_name, units in _SCIENCE_UNITS.items():
        if var_name in ds.variables:
            var = ds.variables[var_name]
            _set_attr_if_empty(var, "long_name", _SCIENCE_LONG_NAMES[var_name])
            _set_attr_if_empty(var, "units", units)


def _context_or_config(context, name):
    return _clean_text(context.get(name, "")) or RELEASE_ACDD_CONFIG.get(name, "")


def _release_version(ds, context):
    for value in (
        context.get("release_version", ""),
        _object_attr(ds, "release_version"),
        _object_attr(ds, "dataset_version"),
        _object_attr(ds, "product_version"),
    ):
        text = _clean_text(value)
        if text:
            return text
    return "unversioned"


def _history_created_time(ds):
    import re

    history = _clean_text(_object_attr(ds, "history"))
    match = re.search(r"Created\s+([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+)", history)
    return match.group(1) if match else ""


def _product_source_text(kind):
    return "sed_reference_release {}".format(kind)


def _cdm_data_type(ds, kind):
    if kind == "master":
        feature_id_var = _matrix_feature_id_var(ds)
        return "TimeSeries" if _indexed_timeseries_eligible(ds, feature_id_var, "station_index", "n_stations", "n_records") else "Other"
    return PRODUCT_DESCRIPTIONS[kind]["cdm_data_type"]


def _coverage_content_type(var_name, var):
    if var_name in {"lat", "lon", "time"}:
        return "coordinate"
    if var_name in {"Q", "SSC", "SSL"}:
        return "physicalMeasurement"
    if var_name in {"Q_flag", "SSC_flag", "SSL_flag"} or var_name.endswith("_flag"):
        return "qualityInformation"
    return "auxiliaryInformation"


def _clean_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat", "null"} else text


def _valid_numeric_values(var):
    data = np.ma.asarray(var[:])
    if data.size == 0:
        return np.asarray([], dtype=np.float64)
    fill_value = _variable_attr(var, "_FillValue")
    values = np.asarray(data.filled(np.nan), dtype=np.float64).reshape(-1)
    mask = np.isfinite(values)
    if fill_value is not None:
        try:
            mask &= values != float(fill_value)
        except (TypeError, ValueError):
            pass
    return values[mask]


def _format_number(value):
    return "{:.10g}".format(float(value))


def _geospatial_metadata(ds):
    result = {
        "geospatial_lat_min": "",
        "geospatial_lat_max": "",
        "geospatial_lon_min": "",
        "geospatial_lon_max": "",
        "geospatial_bounds": "",
    }
    if "lat" not in ds.variables or "lon" not in ds.variables:
        return result
    lat_values = _valid_numeric_values(ds.variables["lat"])
    lon_values = _valid_numeric_values(ds.variables["lon"])
    if lat_values.size == 0 or lon_values.size == 0:
        return result
    lat_min = float(np.nanmin(lat_values))
    lat_max = float(np.nanmax(lat_values))
    lon_min = float(np.nanmin(lon_values))
    lon_max = float(np.nanmax(lon_values))
    result.update(
        {
            "geospatial_lat_min": _format_number(lat_min),
            "geospatial_lat_max": _format_number(lat_max),
            "geospatial_lon_min": _format_number(lon_min),
            "geospatial_lon_max": _format_number(lon_max),
            "geospatial_bounds": (
                "POLYGON(({lon_min} {lat_min}, {lon_max} {lat_min}, "
                "{lon_max} {lat_max}, {lon_min} {lat_max}, {lon_min} {lat_min}))"
            ).format(
                lon_min=_format_number(lon_min),
                lon_max=_format_number(lon_max),
                lat_min=_format_number(lat_min),
                lat_max=_format_number(lat_max),
            ),
        }
    )
    return result


def _time_coverage_metadata(ds):
    result = {
        "time_coverage_start": "",
        "time_coverage_end": "",
        "time_coverage_duration": "",
    }
    if "time" not in ds.variables:
        return result
    values = _valid_numeric_values(ds.variables["time"])
    if values.size == 0:
        return result
    time_var = ds.variables["time"]
    units = _clean_text(_variable_attr(time_var, "units")) or "days since 1970-01-01"
    calendar = _clean_text(_variable_attr(time_var, "calendar")) or "gregorian"
    start_num = float(np.nanmin(values))
    end_num = float(np.nanmax(values))
    start_text = _format_time_value(start_num, units, calendar)
    end_text = _format_time_value(end_num, units, calendar)
    result["time_coverage_start"] = start_text
    result["time_coverage_end"] = end_text
    result["time_coverage_duration"] = _iso_duration_days(max(0.0, end_num - start_num))
    return result


def _format_time_value(value, units, calendar):
    try:
        dt = nc4.num2date(
            value,
            units=units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=False,
        )
        hour = getattr(dt, "hour", 0)
        minute = getattr(dt, "minute", 0)
        second = getattr(dt, "second", 0)
        if hour == 0 and minute == 0 and second == 0:
            return dt.strftime("%Y-%m-%d")
        return dt.isoformat()
    except Exception:
        return _format_number(value)


def _iso_duration_days(days):
    days_int = int(round(float(days)))
    return "P{}D".format(days_int)


def _apply_matrix_metadata(ds):
    _set_attr(ds, "featureType", "timeSeries")
    _set_variable_attrs(ds, _matrix_feature_id_var(ds), {"cf_role": "timeseries_id"})
    _set_coordinate_attrs(ds)
    _set_science_attrs(ds)


def _apply_indexed_timeseries_metadata(
    ds,
    feature_id_var,
    index_var,
    instance_dim,
    index_dim,
    ensure_science_long_names=False,
):
    _set_attr(ds, "featureType", "timeSeries")
    _set_variable_attrs(ds, feature_id_var, {"cf_role": "timeseries_id"})
    _set_variable_attrs(ds, index_var, {"instance_dimension": instance_dim})
    _set_coordinate_attrs(ds)
    _set_science_attrs(ds, ensure_long_names=ensure_science_long_names)


def _apply_master_metadata(ds):
    _set_coordinate_attrs(ds)
    _set_science_attrs(ds)
    feature_id_var = _matrix_feature_id_var(ds)
    if _indexed_timeseries_eligible(ds, feature_id_var, "station_index", "n_stations", "n_records"):
        _set_attr(ds, "featureType", "timeSeries")
        _set_variable_attrs(ds, feature_id_var, {"cf_role": "timeseries_id"})
        _set_variable_attrs(ds, "station_index", {"instance_dimension": "n_stations"})


def _indexed_timeseries_eligible(ds, feature_id_var, index_var, instance_dim, index_dim):
    if instance_dim not in ds.dimensions or index_dim not in ds.dimensions:
        return False
    if feature_id_var not in ds.variables or index_var not in ds.variables:
        return False
    if tuple(ds.variables[feature_id_var].dimensions) != (instance_dim,):
        return False
    if tuple(ds.variables[index_var].dimensions) != (index_dim,):
        return False
    if "time" not in ds.variables or tuple(ds.variables["time"].dimensions) != (index_dim,):
        return False
    for var_name in ("Q", "SSC", "SSL"):
        if var_name in ds.variables and tuple(ds.variables[var_name].dimensions) != (index_dim,):
            return False
    return True


def _matrix_feature_id_var(ds):
    return "station_uid" if "station_uid" in ds.variables else "cluster_uid"


def _matrix_expected_attrs(ds):
    feature_id_var = _matrix_feature_id_var(ds)
    expected = [
        ("global", None, "featureType", "timeSeries"),
        ("variable", feature_id_var, "cf_role", "timeseries_id"),
    ]
    expected.extend(_coordinate_expected_attrs())
    expected.extend(_science_expected_attrs())
    expected.extend(_flag_expected_attrs())
    return expected


def _indexed_timeseries_expected_attrs(
    feature_id_var,
    index_var,
    instance_dim,
    require_science_long_names=False,
):
    expected = [
        ("global", None, "featureType", "timeSeries"),
        ("variable", feature_id_var, "cf_role", "timeseries_id"),
        ("variable", index_var, "instance_dimension", instance_dim),
    ]
    expected.extend(_coordinate_expected_attrs())
    expected.extend(_science_expected_attrs(require_long_names=require_science_long_names))
    expected.extend(_flag_expected_attrs())
    return expected


def _master_generic_expected_attrs():
    expected = []
    expected.extend(_coordinate_expected_attrs())
    expected.extend(_science_expected_attrs())
    expected.extend(_flag_expected_attrs())
    return expected


def _coordinate_expected_attrs():
    expected = []
    for var_name, attrs in _COORD_ATTRS.items():
        for attr_name, value in attrs.items():
            expected.append(("variable", var_name, attr_name, value))
    return expected


def _science_expected_attrs(require_long_names=False):
    expected = []
    for var_name, units in _SCIENCE_UNITS.items():
        expected.append(("variable", var_name, "units", units))
        expected.append(("variable", var_name, "coordinates", "time lat lon"))
        expected.append(("variable", var_name, "ancillary_variables", "{}_flag".format(var_name)))
        if require_long_names:
            expected.append(("variable", var_name, "long_name", _SCIENCE_LONG_NAMES[var_name]))
    return expected


def _flag_expected_attrs():
    expected = []
    for var_name in ("Q_flag", "SSC_flag", "SSL_flag"):
        expected.append(("variable", var_name, "flag_values", FLAG_VALUES))
        expected.append(("variable", var_name, "flag_meanings", FLAG_MEANINGS))
    return expected


def _audit_global_conventions(ds, rows):
    conventions = str(getattr(ds, "Conventions", "") or "")
    tokens = [token for token in conventions.replace(",", " ").split() if token]
    ok = any(token == CF_CONVENTION for token in tokens)
    rows.append(
        {
            "check": "cf_conventions",
            "status": "pass" if ok else "fail",
            "details": "Conventions={!r}".format(conventions),
        }
    )


def _audit_expected_attrs(ds, rows, expected, product_kind):
    missing_or_bad = []
    checked = 0
    for scope, var_name, attr_name, expected_value in expected:
        if scope == "global":
            actual = _object_attr(ds, attr_name)
            label = attr_name
        else:
            if var_name not in ds.variables:
                missing_or_bad.append("{}.{} variable missing".format(var_name, attr_name))
                continue
            actual = _variable_attr(ds.variables[var_name], attr_name)
            label = "{}.{}".format(var_name, attr_name)
        checked += 1
        if not _values_equal(actual, expected_value):
            missing_or_bad.append(
                "{} expected {} got {}".format(
                    label,
                    _format_attr_value(expected_value),
                    _format_attr_value(actual),
                )
            )
    rows.append(
        {
            "check": "cf18_metadata_{}".format(product_kind),
            "status": "pass" if not missing_or_bad else "fail",
            "details": "checked={} mismatches={}".format(checked, len(missing_or_bad))
            if not missing_or_bad
            else "; ".join(missing_or_bad[:12]),
        }
    )


def _audit_master_dsg_status(ds, rows):
    feature_id_var = _matrix_feature_id_var(ds)
    eligible = _indexed_timeseries_eligible(ds, feature_id_var, "station_index", "n_stations", "n_records")
    if not eligible:
        rows.append(
            {
                "check": "master_dsg_featureType_declarable",
                "status": "info",
                "details": "master lacks a complete indexed-ragged timeSeries structure; featureType not required",
            }
        )
        return
    declared = (
        str(getattr(ds, "featureType", "") or "") == "timeSeries"
        and _variable_attr(ds.variables[feature_id_var], "cf_role") == "timeseries_id"
        and _variable_attr(ds.variables["station_index"], "instance_dimension") == "n_stations"
    )
    rows.append(
        {
            "check": "master_dsg_featureType_declarable",
            "status": "pass" if declared else "fail",
            "details": "indexed-ragged timeSeries structure present; declared={}".format(declared),
        }
    )


def _audit_no_fabricated_time_bounds(ds, rows, product_kind):
    bounds_attr = _variable_attr(ds.variables["time"], "bounds") if "time" in ds.variables else None
    rows.append(
        {
            "check": "climatology_time_bounds_policy",
            "status": "info",
            "details": "time.bounds={!r}; no climatological bounds are required unless source observation periods are explicit per record".format(
                bounds_attr or ""
            ),
        }
    )


def _audit_acdd_required(ds, rows):
    required = ("title", "summary", "keywords", "Conventions")
    missing = [name for name in required if not _clean_text(_object_attr(ds, name))]
    conventions = _clean_text(_object_attr(ds, "Conventions"))
    convention_tokens = {token for token in conventions.replace(",", " ").split() if token}
    for token in (CF_CONVENTION, ACDD_CONVENTION):
        if token not in convention_tokens and token not in missing:
            missing.append("Conventions:{}".format(token))
    rows.append(
        {
            "check": "acdd_required_discovery_metadata",
            "status": "pass" if not missing else "fail",
            "details": "required ACDD discovery fields present" if not missing else "missing: " + ", ".join(missing),
        }
    )


def _audit_acdd_computed(ds, rows):
    expected_geo = _geospatial_metadata(ds)
    expected_time = _time_coverage_metadata(ds)
    expected_geo.pop("geospatial_bounds", None)
    expected_time.pop("time_coverage_duration", None)
    mismatches = []
    for attr_name, expected in list(expected_geo.items()) + list(expected_time.items()):
        actual = _clean_text(_object_attr(ds, attr_name))
        if expected and actual != expected:
            mismatches.append("{} expected {} got {}".format(attr_name, expected, actual or "(empty)"))
        elif not expected and not actual:
            mismatches.append("{} unavailable".format(attr_name))
    rows.append(
        {
            "check": "acdd_computed_spatiotemporal_metadata",
            "status": "pass" if not mismatches else "fail",
            "details": "computed geospatial/time fields match coordinates" if not mismatches else "; ".join(mismatches[:12]),
        }
    )


def _audit_acdd_supported(ds, rows):
    required = ("creator_name", "creator_email", "creator_institution", "institution")
    missing = [name for name in required if not _clean_text(_object_attr(ds, name))]
    rows.append(
        {
            "check": "acdd_manuscript_supported_metadata",
            "status": "pass" if not missing else "fail",
            "details": "creator/institution fields present" if not missing else "missing: " + ", ".join(missing),
        }
    )


def _audit_acdd_publication_dependent(ds, rows):
    missing = [name for name in ACDD_PUBLICATION_DEPENDENT_ATTRS if not _clean_text(_object_attr(ds, name))]
    rows.append(
        {
            "check": "acdd_publication_dependent_metadata",
            "status": "pass" if not missing else "warning",
            "details": "publication-dependent fields present" if not missing else "publication-dependent fields not filled: " + ", ".join(missing),
        }
    )


def _audit_acdd_variable_attrs(ds, rows):
    missing = []
    for var_name, var in ds.variables.items():
        expected = _coverage_content_type(var_name, var)
        actual = _clean_text(_variable_attr(var, "coverage_content_type"))
        if expected and actual != expected:
            missing.append("{}.coverage_content_type expected {} got {}".format(var_name, expected, actual or "(empty)"))
        if var_name in _SCIENCE_UNITS:
            for attr_name in ("long_name", "units"):
                if not _clean_text(_variable_attr(var, attr_name)):
                    missing.append("{}.{} empty".format(var_name, attr_name))
    rows.append(
        {
            "check": "acdd_variable_metadata",
            "status": "pass" if not missing else "fail",
            "details": "coverage_content_type and science variable names/units present" if not missing else "; ".join(missing[:12]),
        }
    )


def _audit_acdd_product_shape(ds, kind, rows):
    expected = PRODUCT_DESCRIPTIONS[kind]
    failures = []
    title = _clean_text(_object_attr(ds, "title"))
    summary = _clean_text(_object_attr(ds, "summary"))
    if not title:
        failures.append("title empty")
    if not summary:
        failures.append("summary empty")
    expected_resolution = expected["time_coverage_resolution"]
    actual_resolution = _clean_text(_object_attr(ds, "time_coverage_resolution"))
    if expected_resolution and actual_resolution != expected_resolution:
        failures.append("time_coverage_resolution expected {} got {}".format(expected_resolution, actual_resolution or "(empty)"))
    if kind in {"master", "climatology", "satellite"} and actual_resolution:
        failures.append("time_coverage_resolution should be empty for {}".format(kind))
    rows.append(
        {
            "check": "acdd_product_kind_metadata_{}".format(kind),
            "status": "pass" if not failures else "fail",
            "details": "product-kind title/summary/resolution/cdm metadata valid" if not failures else "; ".join(failures[:12]),
        }
    )
