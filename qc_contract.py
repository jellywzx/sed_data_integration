#!/usr/bin/env python3
"""Shared stage-2 contract helpers for QC NetCDF files."""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd


TIME_VAR_NAMES = ["time", "Time", "t", "datetime", "date", "sample"]
LAT_VAR_NAMES = ["lat", "latitude", "Latitude", "LAT"]
LON_VAR_NAMES = ["lon", "longitude", "Longitude", "LON"]

Q_VAR_NAMES = ["Q", "discharge", "Discharge_m3_s", "Discharge"]
SSC_VAR_NAMES = ["SSC", "ssc", "TSS_mg_L", "TSS"]
SSL_VAR_NAMES = ["SSL", "sediment_load", "Sediment_load"]

FINAL_Q_FLAG_NAMES = ["Q_flag", "discharge_flag", "q_flag"]
FINAL_SSC_FLAG_NAMES = ["SSC_flag", "ssc_flag", "TSS_flag", "tss_flag"]
FINAL_SSL_FLAG_NAMES = ["SSL_flag", "ssl_flag", "sediment_load_flag"]

STATION_NAME_ATTR_KEYS = ["station_name", "Station_Name", "stationName", "name"]
RIVER_NAME_ATTR_KEYS = ["river_name", "River_Name", "riverName", "river"]
STATION_ID_ATTR_KEYS = [
    "station_id",
    "Source_ID",
    "Station_ID",
    "source_id",
    "stationID",
    "ID",
    "location_id",
]
TEMPORAL_RESOLUTION_ATTR_KEYS = [
    "temporal_resolution",
    "Temporal_Resolution",
    "time_resolution",
    "resolution",
]
TEMPORAL_SPAN_ATTR_KEYS = ["temporal_span", "Temporal_Span", "measurement_period"]
TIME_COVERAGE_START_ATTR_KEYS = ["time_coverage_start", "data_period_start", "start_date"]
TIME_COVERAGE_END_ATTR_KEYS = ["time_coverage_end", "data_period_end", "end_date"]
SUMMARY_ATTR_KEYS = ["summary"]
COMMENT_ATTR_KEYS = ["comment"]
VARIABLES_PROVIDED_ATTR_KEYS = ["variables_provided", "Variables_Provided"]
DATA_LIMITATIONS_ATTR_KEYS = ["data_limitations"]
SOURCE_NAME_ATTR_KEYS = ["data_source_name", "Data_Source_Name", "dataset_name"]
CREATOR_INSTITUTION_ATTR_KEYS = [
    "creator_institution",
    "contributor_institution",
    "institution",
    "insitiution",
]
SOURCE_URL_ATTR_KEYS = [
    "source_data_link",
    "source_url",
    "sediment_data_source",
    "discharge_data_source",
]

REFERENCE_ATTR_PREFIXES = ("reference", "references")
FLAG_FILL_BYTE = -127
FILL_FLOAT_VALUES = (-9999.0, -9999)

STANDARD_QC_STAGE_SPECS = (
    {
        "name": "Q_qc1",
        "aliases": ["Q_qc1", "Q_flag_qc1_physical"],
        "fill_value": 9,
        "flag_values": np.array([0, 3, 9], dtype=np.int8),
        "flag_meanings": "pass bad missing",
        "long_name": "qc stage 1 physical screen for river discharge",
    },
    {
        "name": "SSC_qc1",
        "aliases": ["SSC_qc1", "SSC_flag_qc1_physical"],
        "fill_value": 9,
        "flag_values": np.array([0, 3, 9], dtype=np.int8),
        "flag_meanings": "pass bad missing",
        "long_name": "qc stage 1 physical screen for suspended sediment concentration",
    },
    {
        "name": "SSL_qc1",
        "aliases": ["SSL_qc1", "SSL_flag_qc1_physical"],
        "fill_value": 9,
        "flag_values": np.array([0, 3, 9], dtype=np.int8),
        "flag_meanings": "pass bad missing",
        "long_name": "qc stage 1 physical screen for suspended sediment load",
    },
    {
        "name": "Q_qc2",
        "aliases": ["Q_qc2", "Q_flag_qc2_log_iqr"],
        "fill_value": 9,
        "flag_values": np.array([0, 2, 8, 9], dtype=np.int8),
        "flag_meanings": "pass suspect not_checked missing",
        "long_name": "qc stage 2 log-iqr screen for river discharge",
    },
    {
        "name": "SSC_qc2",
        "aliases": ["SSC_qc2", "SSC_flag_qc2_log_iqr"],
        "fill_value": 9,
        "flag_values": np.array([0, 2, 8, 9], dtype=np.int8),
        "flag_meanings": "pass suspect not_checked missing",
        "long_name": "qc stage 2 log-iqr screen for suspended sediment concentration",
    },
    {
        "name": "SSL_qc2",
        "aliases": ["SSL_qc2", "SSL_flag_qc2_log_iqr"],
        "fill_value": 9,
        "flag_values": np.array([0, 2, 8, 9], dtype=np.int8),
        "flag_meanings": "pass suspect not_checked missing",
        "long_name": "qc stage 2 log-iqr screen for suspended sediment load",
    },
    {
        "name": "SSC_qc3",
        "aliases": ["SSC_qc3", "SSC_flag_qc3_ssc_q"],
        "fill_value": 9,
        "flag_values": np.array([0, 2, 8, 9], dtype=np.int8),
        "flag_meanings": "pass suspect not_checked missing",
        "long_name": "qc stage 3 cross-variable screen for suspended sediment concentration",
    },
    {
        "name": "SSL_qc3",
        "aliases": ["SSL_qc3", "SSL_qc3_prop", "SSL_flag_qc3_from_ssc_q"],
        "fill_value": 9,
        "flag_values": np.array([0, 1, 8, 9], dtype=np.int8),
        "flag_meanings": "not_propagated propagated not_checked missing",
        "long_name": "qc stage 3 propagation flag for suspended sediment load",
    },
)

STANDARD_QC_STAGE_NAMES = tuple(spec["name"] for spec in STANDARD_QC_STAGE_SPECS)
STANDARD_QC_STAGE_NAME_TO_SPEC = dict((spec["name"], spec) for spec in STANDARD_QC_STAGE_SPECS)

SOURCE_STATION_TEXT_FIELDS = (
    "source_station_temporal_span",
    "source_station_time_coverage_start",
    "source_station_time_coverage_end",
    "source_station_summary",
    "source_station_comment",
    "source_station_variables_provided",
    "source_station_data_limitations",
    "source_station_declared_temporal_resolution",
)

_STAGE1_PARITY_KEYS = {
    "station_id": STATION_ID_ATTR_KEYS,
    "temporal_resolution": TEMPORAL_RESOLUTION_ATTR_KEYS,
    "temporal_span": TEMPORAL_SPAN_ATTR_KEYS,
    "time_coverage_start": TIME_COVERAGE_START_ATTR_KEYS,
    "time_coverage_end": TIME_COVERAGE_END_ATTR_KEYS,
    "data_source_name": SOURCE_NAME_ATTR_KEYS,
    "source_data_link": SOURCE_URL_ATTR_KEYS,
    "creator_institution": CREATOR_INSTITUTION_ATTR_KEYS,
    "summary": SUMMARY_ATTR_KEYS,
    "comment": COMMENT_ATTR_KEYS,
    "variables_provided": VARIABLES_PROVIDED_ATTR_KEYS,
    "data_limitations": DATA_LIMITATIONS_ATTR_KEYS,
}


def _clean_text(value, limit=None):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.lower() in ("", "nan", "none"):
        return ""
    if limit is not None:
        return text[: int(limit)]
    return text


def _get_attr_names(obj):
    if isinstance(obj, dict):
        return list(obj.keys())
    try:
        return list(obj.ncattrs())
    except Exception:
        return []


def get_attr_value(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    try:
        return getattr(obj, key, None)
    except Exception:
        return None


def get_first_attr_value(obj, keys, limit=None):
    attr_names = _get_attr_names(obj)
    attr_map = dict((str(name).lower(), name) for name in attr_names)
    for key in keys:
        raw = None
        if isinstance(obj, dict):
            raw = obj.get(key)
            if raw is None:
                matched = attr_map.get(str(key).lower())
                if matched is not None:
                    raw = obj.get(matched)
        else:
            matched = attr_map.get(str(key).lower(), key)
            raw = get_attr_value(obj, matched)
        text = _clean_text(raw, limit=limit)
        if text:
            return text
    return ""


def collect_reference_text(obj, limit=1024):
    parts = []
    for attr_name in _get_attr_names(obj):
        if str(attr_name).lower().startswith(REFERENCE_ATTR_PREFIXES):
            text = _clean_text(get_attr_value(obj, attr_name))
            if text and text not in parts:
                parts.append(text)
    merged = " | ".join(parts)
    if limit is not None:
        return merged[: int(limit)]
    return merged


def get_first_var_name(ds, names):
    variables = getattr(ds, "variables", {})
    for name in names:
        if name in variables:
            return name
    return None


def _read_var_values(var):
    try:
        return var[:]
    except Exception:
        try:
            return var.values
        except Exception:
            return None


def _coerce_scalar(value):
    if value is None:
        return None
    if np.ma.isMaskedArray(value):
        flat = value.flatten()
        if flat.size == 0:
            return None
        value = flat.flat[0]
        if np.ma.is_masked(value):
            return None
    else:
        arr = np.asarray(value).reshape(-1)
        if arr.size == 0:
            return None
        value = arr.flat[0]
    try:
        value = float(value)
    except Exception:
        return None
    if np.isnan(value) or value in FILL_FLOAT_VALUES:
        return None
    return value


def read_scalar_variable(ds, names):
    var_name = get_first_var_name(ds, names)
    if var_name is None:
        return None
    return _coerce_scalar(_read_var_values(ds.variables[var_name]))


def read_station_metadata(ds):
    return {
        "station_name": get_first_attr_value(ds, STATION_NAME_ATTR_KEYS, limit=256),
        "river_name": get_first_attr_value(ds, RIVER_NAME_ATTR_KEYS, limit=256),
        "source_station_id": get_first_attr_value(ds, STATION_ID_ATTR_KEYS, limit=256),
    }


def read_source_metadata(ds):
    return {
        "source_long_name": get_first_attr_value(ds, SOURCE_NAME_ATTR_KEYS, limit=512),
        "institution": get_first_attr_value(ds, CREATOR_INSTITUTION_ATTR_KEYS, limit=512),
        "reference": collect_reference_text(ds, limit=1024),
        "source_url": get_first_attr_value(ds, SOURCE_URL_ATTR_KEYS, limit=512),
    }


def _format_timestamp_text(value):
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return ""
    if pd.isna(ts):
        return ""
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0 and ts.microsecond == 0:
        return ts.strftime("%Y-%m-%d")
    return ts.isoformat()


def build_time_coverage_from_dates(dates):
    try:
        times = pd.to_datetime(list(dates), errors="coerce")
    except Exception:
        return ("", "", "")
    series = pd.Series(times).dropna()
    if len(series) == 0:
        return ("", "", "")
    start_text = _format_timestamp_text(series.min())
    end_text = _format_timestamp_text(series.max())
    if start_text and end_text and start_text != end_text:
        span_text = "{} to {}".format(start_text, end_text)
    else:
        span_text = start_text or end_text
    return (start_text, end_text, span_text)


def read_explanatory_metadata(ds, dates=None):
    start_text = get_first_attr_value(ds, TIME_COVERAGE_START_ATTR_KEYS, limit=128)
    end_text = get_first_attr_value(ds, TIME_COVERAGE_END_ATTR_KEYS, limit=128)
    span_text = get_first_attr_value(ds, TEMPORAL_SPAN_ATTR_KEYS, limit=256)
    if dates is not None and (not start_text or not end_text or not span_text):
        derived_start, derived_end, derived_span = build_time_coverage_from_dates(dates)
        if not start_text:
            start_text = derived_start
        if not end_text:
            end_text = derived_end
        if not span_text:
            span_text = derived_span
    return {
        "temporal_span": span_text,
        "time_coverage_start": start_text,
        "time_coverage_end": end_text,
        "summary": get_first_attr_value(ds, SUMMARY_ATTR_KEYS, limit=2048),
        "comment": get_first_attr_value(ds, COMMENT_ATTR_KEYS, limit=2048),
        "variables_provided": get_first_attr_value(ds, VARIABLES_PROVIDED_ATTR_KEYS, limit=1024),
        "data_limitations": get_first_attr_value(ds, DATA_LIMITATIONS_ATTR_KEYS, limit=2048),
        "declared_temporal_resolution": get_first_attr_value(ds, TEMPORAL_RESOLUTION_ATTR_KEYS, limit=128),
    }


def read_contract_metadata(ds, dates=None):
    metadata = {}
    metadata.update(read_station_metadata(ds))
    metadata.update(read_source_metadata(ds))
    metadata.update(read_explanatory_metadata(ds, dates=dates))
    metadata["lat"] = read_scalar_variable(ds, LAT_VAR_NAMES)
    metadata["lon"] = read_scalar_variable(ds, LON_VAR_NAMES)
    return metadata


def _pad_array(values, size, fill_value):
    arr = np.asarray(values).reshape(-1)
    if len(arr) >= size:
        return arr[:size]
    pad = np.full(size - len(arr), fill_value, dtype=arr.dtype if len(arr) else type(fill_value))
    return np.concatenate([arr, pad])


def read_flag_array(ds, aliases, size, fill_value=9):
    var_name = get_first_var_name(ds, aliases)
    if var_name is None:
        return np.full(size, fill_value, dtype=np.int8)
    raw = np.asarray(_read_var_values(ds.variables[var_name])).reshape(-1)
    if len(raw) >= size:
        raw = raw[:size]
    else:
        raw = np.concatenate([raw, np.full(size - len(raw), FLAG_FILL_BYTE, dtype=np.int16)])
    result = raw.astype(np.int16, copy=False)
    result[result == FLAG_FILL_BYTE] = fill_value
    result[~np.isfinite(result)] = fill_value
    return result.astype(np.int8, copy=False)


def read_standardized_qc_stage_arrays(ds, size):
    arrays = {}
    for spec in STANDARD_QC_STAGE_SPECS:
        arrays[spec["name"]] = read_flag_array(
            ds,
            spec["aliases"],
            size=size,
            fill_value=int(spec["fill_value"]),
        )
    return arrays


def append_stage_qc_variables(nc, dimensions, chunksizes=None, zlib=True, complevel=4):
    variables = {}
    for spec in STANDARD_QC_STAGE_SPECS:
        kwargs = {
            "fill_value": np.int8(spec["fill_value"]),
            "zlib": zlib,
            "complevel": complevel,
        }
        if chunksizes is not None:
            kwargs["chunksizes"] = chunksizes
        var = nc.createVariable(spec["name"], "i1", dimensions, **kwargs)
        var.long_name = spec["long_name"]
        var.flag_values = spec["flag_values"]
        var.flag_meanings = spec["flag_meanings"]
        variables[spec["name"]] = var
    return variables


def load_stage1_attr_priority_map():
    project_root = Path(__file__).resolve().parents[2]
    script_root = project_root / "Script"
    if not script_root.is_dir():
        raise RuntimeError("Stage-1 Script directory not found: {}".format(script_root))
    if str(script_root) not in sys.path:
        sys.path.insert(0, str(script_root))
    from code.global_attrs import ATTR_PRIORITY_MAP

    return ATTR_PRIORITY_MAP


def ensure_stage1_alias_parity():
    stage1_map = load_stage1_attr_priority_map()
    mismatches = []
    for key, stage2_aliases in _STAGE1_PARITY_KEYS.items():
        missing = sorted(set(stage1_map.get(key, [])) - set(stage2_aliases))
        if missing:
            mismatches.append("{} missing {}".format(key, ", ".join(missing)))
    if mismatches:
        raise RuntimeError(
            "Stage-2 alias contract drifts from Script/code/global_attrs.py: {}".format(
                " | ".join(mismatches)
            )
        )
    return True


def normalize_declared_temporal_resolution(value):
    text = _clean_text(value).lower()
    if not text:
        return ""
    if "climatology" in text or "climatological" in text:
        return "climatology"
    if "quarter" in text:
        return "monthly"
    if "month" in text:
        return "monthly"
    if "annual" in text or "year" in text:
        return "annual"
    if "daily" in text or re.match(r"^1\s*day$", text):
        return "daily"
    if "hour" in text:
        return "daily"
    if text == "single_point":
        return "single_point"
    return text
