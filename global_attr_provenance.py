#!/usr/bin/env python3
"""Utilities for carrying upstream NetCDF global attributes into release NCs.

The release products are multi-station datasets, so upstream file-level
attributes are stored as station/source-station variables rather than copied
to product-level global attributes.
"""

import json
from collections import OrderedDict
from pathlib import Path

import numpy as np

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - runtime dependency in pipeline env
    nc4 = None


GLOBAL_ATTR_SCHEMA_VERSION = "2"

PROMOTED_ATTR_ALIASES = OrderedDict(
    [
        ("country", ("country", "Country")),
        ("continent_region", ("continent_region", "continent", "region", "Continent_Region")),
        ("geographic_coverage", ("geographic_coverage", "Geographic_Coverage")),
        ("iso_a3", ("iso_a3", "ISO_A3", "adm0_a3", "ADM0_A3")),
        ("geo_attribute_source", ("geo_attribute_source", "Geo_Attribute_Source")),
        ("geo_attribute_confidence", ("geo_attribute_confidence", "Geo_Attribute_Confidence")),
        ("geo_attribute_method", ("geo_attribute_method", "Geo_Attribute_Method")),
        ("geo_boundary_dataset", ("geo_boundary_dataset", "Geo_Boundary_Dataset")),
        ("geo_boundary_version", ("geo_boundary_version", "Geo_Boundary_Version")),
        ("station_id", ("station_id", "Source_ID", "Station_ID", "source_id", "stationID", "ID", "location_id")),
        ("dataset_name", ("dataset_name", "Dataset_Name")),
        ("data_source_name", ("data_source_name", "Data_Source_Name")),
        ("observation_type", ("observation_type", "Observation_Type")),
        ("temporal_resolution", ("temporal_resolution", "Temporal_Resolution", "time_resolution", "resolution")),
        ("time_coverage_start", ("time_coverage_start", "data_period_start", "start_date")),
        ("time_coverage_end", ("time_coverage_end", "data_period_end", "end_date")),
        ("creator_name", ("creator_name", "Creator_Name")),
        ("creator_email", ("creator_email", "Creator_Email")),
        ("creator_institution", ("creator_institution", "contributor_institution", "institution", "insitiution")),
        ("source_data_link", ("source_data_link", "source_url", "sediment_data_source", "discharge_data_source")),
        ("processing_level", ("processing_level", "Processing_Level")),
        ("featureType", ("featureType", "feature_type")),
        ("date_created", ("date_created", "Date_Created")),
        ("date_modified", ("date_modified", "Date_Modified")),
    ]
)

PROMOTED_GEO_FIELDS = {
    "country",
    "continent_region",
    "geographic_coverage",
    "iso_a3",
    "geo_attribute_source",
    "geo_attribute_confidence",
    "geo_attribute_method",
    "geo_boundary_dataset",
    "geo_boundary_version",
}


def clean_text(value, limit=None):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    if limit is not None:
        return text[: int(limit)]
    return text


def split_path_text(value):
    paths = []
    for part in clean_text(value).split("|"):
        part = clean_text(part)
        if part and part not in paths:
            paths.append(part)
    return paths


def _json_value(value):
    if value is None or np.ma.is_masked(value):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    if isinstance(value, np.generic):
        return _json_value(value.item())
    if isinstance(value, np.ndarray):
        return [_json_value(item) for item in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _dedupe_key(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def read_global_attrs(path):
    """Return all global attributes from a NetCDF file as JSON-safe values."""
    if nc4 is None:
        return {}
    path = Path(clean_text(path))
    if not path.is_file():
        return {}
    try:
        with nc4.Dataset(str(path), "r") as ds:
            return OrderedDict((name, _json_value(getattr(ds, name))) for name in sorted(ds.ncattrs()))
    except Exception:
        return {}


def _append_attr_value(values, value):
    key = _dedupe_key(value)
    if key not in values:
        values[key] = value


def merge_global_attrs_for_paths(paths):
    """Merge attrs from one or more source NC paths into a station payload."""
    merged = OrderedDict()
    for path in sorted(set(clean_text(p) for p in paths if clean_text(p))):
        attrs = read_global_attrs(path)
        for name, value in attrs.items():
            merged.setdefault(name, OrderedDict())
            _append_attr_value(merged[name], value)

    compact = OrderedDict()
    for name in sorted(merged):
        values = list(merged[name].values())
        compact[name] = values[0] if len(values) == 1 else values

    promoted = promote_global_attrs(compact)
    return {
        "json": json.dumps(compact, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        "names": "|".join(sorted(compact.keys())),
        "count": int(len(compact)),
        "promoted": promoted,
    }


def global_attr_payloads_for_path_groups(path_groups):
    return [merge_global_attrs_for_paths(paths) for paths in path_groups]


def promote_global_attrs(attrs):
    lowered = {str(name).lower(): name for name in attrs.keys()}
    out = OrderedDict()
    for field, aliases in PROMOTED_ATTR_ALIASES.items():
        values = []
        seen = set()
        for alias in aliases:
            actual = attrs.get(alias)
            if actual is None and str(alias).lower() in lowered:
                actual = attrs.get(lowered[str(alias).lower()])
            if actual is None:
                continue
            parts = actual if isinstance(actual, list) else [actual]
            for value in parts:
                text = clean_text(value)
                if text and text not in seen:
                    values.append(text)
                    seen.add(text)
        out[field] = "|".join(values)
    return out


def empty_global_attr_payload():
    return {"json": "{}", "names": "", "count": 0, "promoted": promote_global_attrs({})}


def _payload_text(payloads, key):
    return np.asarray([clean_text(payload.get(key, "")) for payload in payloads], dtype=object)


def _payload_count(payloads):
    return np.asarray([int(payload.get("count", 0) or 0) for payload in payloads], dtype=np.int32)


def write_global_attr_payload_variables(nc, dimension, prefix, payloads, subject):
    """Write JSON/name/count variables for station-level upstream attrs."""
    json_name = "{}_global_attrs_json".format(prefix)
    names_name = "{}_global_attr_names".format(prefix)
    count_name = "{}_global_attr_count".format(prefix)

    json_v = nc.createVariable(json_name, str, (dimension,))
    json_v.long_name = "upstream NetCDF global attributes for {}".format(subject)
    json_v.comment = "JSON object stored in the release NC so upstream files are not needed for metadata queries"
    json_v.schema_version = GLOBAL_ATTR_SCHEMA_VERSION
    json_v[:] = _payload_text(payloads, "json")

    names_v = nc.createVariable(names_name, str, (dimension,))
    names_v.long_name = "pipe-separated upstream global attribute names for {}".format(subject)
    names_v[:] = _payload_text(payloads, "names")

    count_v = nc.createVariable(count_name, "i4", (dimension,))
    count_v.long_name = "number of upstream global attribute keys stored for {}".format(subject)
    count_v[:] = _payload_count(payloads)


def write_promoted_global_attr_variables(nc, dimension, payloads, var_prefix="", subject="station"):
    """Write common upstream attrs as plain string variables for direct query."""
    for field in PROMOTED_ATTR_ALIASES:
        name = "{}{}".format(var_prefix, field)
        if name in nc.variables:
            continue
        var = nc.createVariable(name, str, (dimension,))
        if field in PROMOTED_GEO_FIELDS:
            var.long_name = "promoted release geographic metadata '{}' for {}".format(field, subject)
            var.comment = (
                "Upstream NetCDF global attributes are used first; configured admin0 boundary "
                "point-in-polygon enrichment may fill missing geographic values. The raw JSON "
                "global-attribute payload remains upstream-only."
            )
        else:
            var.long_name = "promoted upstream global attribute '{}' for {}".format(field, subject)
            var.comment = "Derived from the JSON global-attribute payload stored in this release NC"
        var[:] = np.asarray(
            [clean_text(payload.get("promoted", {}).get(field, "")) for payload in payloads],
            dtype=object,
        )


def set_global_attr_policy(nc):
    nc.upstream_global_attrs_policy = (
        "Upstream file-level global attributes are stored as station/source-station variables, "
        "not as product-level global attributes."
    )
    nc.upstream_global_attrs_storage = (
        "JSON payload variables plus promoted station-level string variables; promoted geographic "
        "variables may include boundary-derived fills for missing upstream attributes"
    )
    nc.upstream_global_attrs_schema_version = GLOBAL_ATTR_SCHEMA_VERSION
