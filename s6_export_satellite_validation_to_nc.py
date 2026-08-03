#!/usr/bin/env python3
"""
Export satellite validation-only observations from s5b linkage rows.

Runtime policy is built-in so users can run:
  python3 s6_export_satellite_validation_to_nc.py
without passing CLI arguments.
"""

import os
import hashlib
import argparse
import socket
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import sys
from tqdm import tqdm

import numpy as np
import pandas as pd

from global_attr_provenance import (
    merge_global_attrs_for_paths,
    set_global_attr_policy,
    write_global_attr_payload_variables,
    write_promoted_global_attr_variables,
)
from geo_boundary_enrichment import (
    boundary_options_from_argv,
    enrich_global_attr_payloads,
    geo_values_from_payload,
)
from pipeline_paths import (
    S2_ORGANIZED_DIR,
    S5_BASIN_CLUSTERED_CSV,
    S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV,
    S6_SATELLITE_VALIDATION_CATALOG_CSV,
    S6_SATELLITE_VALIDATION_NC,
    get_output_r_root,
)
from release_netcdf_conventions import apply_release_conventions
from release_netcdf_schema import FLAG_MEANINGS, FLAG_VALUES, SCIENCE_LONG_NAMES, SCIENCE_UNITS
from s6_basin_merge_to_nc import (
    FILL,
    HAS_NC,
    _read_source_meta_from_nc,
    _read_station_meta_from_nc,
    load_nc_series,
)
from source_family import classify_source_family, classify_source_family_from_observation_type

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
ORGANIZED_ROOT = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()

DEFAULT_INPUT = PROJECT_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV
DEFAULT_S5_CSV = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUTPUT = PROJECT_ROOT / S6_SATELLITE_VALIDATION_NC
DEFAULT_CATALOG = PROJECT_ROOT / S6_SATELLITE_VALIDATION_CATALOG_CSV
DEFAULT_RESOLUTIONS = ("daily", "monthly", "annual")
DEFAULT_WORKERS = min(8, max(1, os.cpu_count() or 1))

# ---- built-in runtime parameters (edit here; no CLI input required) ----
BUILTIN_INPUT = DEFAULT_INPUT
BUILTIN_S5_CSV = DEFAULT_S5_CSV
BUILTIN_OUTPUT = DEFAULT_OUTPUT
BUILTIN_CATALOG = DEFAULT_CATALOG
BUILTIN_RESOLUTIONS = DEFAULT_RESOLUTIONS
BUILTIN_WORKERS_BY_HOST = {
    "node113": 24,
}

LINKAGE_TEXT_FIELDS = (
    "satellite_location_uid",
    "linked_cluster_uid",
    "linked_resolution",
    "link_resolution_relation",
    "link_attempted_resolutions",
    "link_status",
    "link_reason",
    "link_method",
    "linkage_mode",
    "link_quality",
    "unlinked_reason",
)
LINKAGE_NUMERIC_FIELDS = (
    "linked_cluster_id",
    "satellite_reach_id",
    "main_reach_id",
    "same_reach",
    "reach_hops",
    "point_distance_m",
    "network_distance_m",
    "area_log10_diff",
    "candidate_count",
    "eligible_candidate_count",
    "link_distance_m",
    "link_uparea_log10_error",
    "link_candidate_count",
)

REQUIRED_V2_LINKAGE_COLUMNS = {
    "satellite_key",
    "satellite_station_id",
    "satellite_source",
    "satellite_resolution",
    "satellite_comid",
    "link_status",
    "link_method",
    "link_confidence",
    "linked_cluster_id",
    "linked_cluster_uid",
    "linked_comid",
    "same_reach",
    "topology_hops",
    "area_rel_error",
    "representative_point_distance_m",
    "n_valid_candidates",
    "rejection_reason",
}


def _default_workers_for_host():
    host = str(socket.gethostname() or "").split(".")[0].strip().lower()
    configured = BUILTIN_WORKERS_BY_HOST.get(host, DEFAULT_WORKERS)
    configured = int(configured) if configured is not None else DEFAULT_WORKERS
    if configured <= 0:
        configured = max(1, os.cpu_count() or 1)
    return max(1, configured)


def is_satellite_observation(source):
    """Check if *source* name maps to satellite family (backward compat wrapper)."""
    return classify_source_family(source) == "satellite"


def _normalize_resolution(value):
    text = str(value or "").strip().lower()
    if text == "quarterly":
        return "monthly"
    if text == "single_point":
        return "daily"
    if text == "annually_climatology":
        return "climatology"
    return text


def _resolve_station_path(path_text):
    text = "" if path_text is None else str(path_text)
    path = Path(text)
    if not path.is_absolute():
        return str((ORGANIZED_ROOT / path).resolve())
    if path.is_file():
        return str(path)
    try:
        parts = path.resolve().parts
    except Exception:
        parts = path.parts
    marker = "output_resolution_organized"
    for i, part in enumerate(parts):
        if part == marker and i + 1 < len(parts):
            candidate = (ORGANIZED_ROOT / Path(*parts[i + 1 :])).resolve()
            if candidate.is_file():
                return str(candidate)
    return text


def _safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value, default=-1):
    try:
        if pd.isna(value):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _safe_bool_int(value, default=-1):
    try:
        if pd.isna(value):
            return int(default)
    except Exception:
        pass
    if isinstance(value, bool):
        return 1 if value else 0
    text = _safe_text(value).lower()
    if text in {"true", "1", "yes", "y", "t"}:
        return 1
    if text in {"false", "0", "no", "n", "f"}:
        return 0
    return _safe_int(value, default=default)


def _safe_text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _satellite_key_from_row(row):
    station_id = _safe_text(row.get("station_id", ""))
    source = _safe_text(row.get("source", "")).lower()
    native = _safe_text(row.get("source_station_id", ""))
    resolution = _normalize_resolution(row.get("resolution", ""))
    payload = "\x1f".join([station_id, source, native, resolution])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return "SATV2{}".format(digest)


def _is_satellite_s5_row(row):
    observation_type = _safe_text(row.get("observation_type", "")).lower()
    if observation_type:
        return "satellite" in observation_type
    source = _safe_text(row.get("source", "")).lower()
    return any(token in source for token in ("riversed", "river_sed", "gsed", "dethier", "aquasat"))


def _cluster_uid_from_id(value):
    cluster_id = _safe_int(value, default=-1)
    return "SED{:06d}".format(cluster_id) if cluster_id >= 0 else ""


def _numeric_series(frame, column, default=np.nan):
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series([default] * len(frame), index=frame.index)


def _text_series(frame, column, default=""):
    if column in frame.columns:
        return frame[column].map(_safe_text)
    return pd.Series([default] * len(frame), index=frame.index)


def _load_s5_satellite_lookup(s5_csv):
    s5_csv = Path(s5_csv)
    if not s5_csv.is_file():
        raise ValueError("s5b v2 links require S5 CSV to recover source paths: {}".format(s5_csv))
    s5 = pd.read_csv(s5_csv, low_memory=False)
    needed = {
        "station_id",
        "source",
        "source_station_id",
        "path",
        "resolution",
        "observation_type",
        "cluster_id",
        "lat",
        "lon",
    }
    missing = sorted(needed - set(s5.columns))
    if missing:
        raise ValueError("S5 CSV missing columns needed for s5b v2 links: {}".format(", ".join(missing)))

    sat = s5.loc[s5.apply(_is_satellite_s5_row, axis=1)].copy()
    sat["satellite_key"] = sat.apply(_satellite_key_from_row, axis=1)
    sat["resolution"] = sat["resolution"].map(_normalize_resolution)
    sat["satellite_location_uid"] = sat["satellite_key"]
    sat["cluster_uid"] = sat["cluster_id"].map(_cluster_uid_from_id)

    keep = [
        "satellite_key",
        "satellite_location_uid",
        "station_id",
        "cluster_id",
        "cluster_uid",
        "source",
        "source_station_id",
        "path",
        "resolution",
        "observation_type",
        "lat",
        "lon",
    ]
    sat = sat[keep].copy()
    duplicates = sat.duplicated("satellite_key", keep=False)
    if duplicates.any():
        examples = sat.loc[
            duplicates,
            ["satellite_key", "station_id", "source", "source_station_id", "resolution"],
        ].head(10)
        raise ValueError("duplicate satellite_key values in S5 CSV: {}".format(examples.to_dict("records")))
    return sat


def _normalize_v2_linkage_table(linkage, s5_csv):
    missing = sorted(REQUIRED_V2_LINKAGE_COLUMNS - set(linkage.columns))
    if missing:
        raise ValueError("s5b v2 links CSV missing columns: {}".format(", ".join(missing)))

    s5_sat = _load_s5_satellite_lookup(s5_csv)
    work = linkage.copy()
    work["satellite_key"] = work["satellite_key"].map(_safe_text)
    merged = work.merge(
        s5_sat,
        how="left",
        on="satellite_key",
        suffixes=("", "_s5"),
        validate="many_to_one",
    )

    missing_path = merged["path"].fillna("").astype(str).str.strip().eq("")
    if missing_path.any():
        examples = merged.loc[
            missing_path,
            ["satellite_key", "satellite_station_id", "satellite_source", "satellite_resolution"],
        ].head(10)
        raise ValueError(
            "could not recover source path for {} s5b v2 row(s): {}".format(
                int(missing_path.sum()),
                examples.to_dict("records"),
            )
        )

    sat_resolution = merged["satellite_resolution"].map(_normalize_resolution)
    reason = _text_series(merged, "rejection_reason")
    normalized = pd.DataFrame(index=merged.index)
    normalized["satellite_location_uid"] = merged["satellite_key"].map(_safe_text)
    normalized["source"] = merged["source"].map(_safe_text)
    normalized["path"] = merged["path"].map(_safe_text)
    normalized["cluster_id"] = merged["cluster_id"]
    normalized["cluster_uid"] = merged["cluster_uid"].map(_safe_text)
    normalized["resolution"] = merged["resolution"].map(_normalize_resolution)
    normalized["observation_type"] = merged["observation_type"].map(_safe_text)
    normalized["lat"] = _numeric_series(merged, "satellite_lat").fillna(_numeric_series(merged, "lat"))
    normalized["lon"] = _numeric_series(merged, "satellite_lon").fillna(_numeric_series(merged, "lon"))

    normalized["linked_cluster_uid"] = merged["linked_cluster_uid"].map(_safe_text)
    normalized["linked_resolution"] = sat_resolution
    normalized["link_resolution_relation"] = "same_resolution"
    normalized["link_attempted_resolutions"] = sat_resolution
    normalized["link_status"] = merged["link_status"].map(_safe_text).str.lower()
    normalized["link_reason"] = reason
    normalized.loc[normalized["link_reason"].eq(""), "link_reason"] = normalized["link_status"]
    normalized["link_method"] = merged["link_method"].map(_safe_text)
    normalized["linkage_mode"] = "s5b_v2_topology"
    normalized["link_quality"] = merged["link_confidence"].map(_safe_text)
    normalized["unlinked_reason"] = reason

    normalized["linked_cluster_id"] = _numeric_series(merged, "linked_cluster_id")
    normalized["satellite_reach_id"] = _numeric_series(merged, "satellite_comid")
    normalized["main_reach_id"] = _numeric_series(merged, "linked_comid")
    normalized["same_reach"] = merged["same_reach"] if "same_reach" in merged.columns else -1
    normalized["reach_hops"] = _numeric_series(merged, "topology_hops")
    normalized["point_distance_m"] = _numeric_series(merged, "representative_point_distance_m")
    normalized["network_distance_m"] = np.nan
    normalized["area_log10_diff"] = _numeric_series(merged, "area_rel_error")
    normalized["candidate_count"] = _numeric_series(merged, "n_valid_candidates")
    normalized["eligible_candidate_count"] = _numeric_series(merged, "n_valid_candidates")
    normalized["link_distance_m"] = _numeric_series(merged, "representative_point_distance_m")
    normalized["link_uparea_log10_error"] = _numeric_series(merged, "area_rel_error")
    normalized["link_candidate_count"] = _numeric_series(merged, "n_valid_candidates")
    normalized["s5b_schema"] = "v2"
    return normalized


def _load_linkage_input(input_path, s5_csv):
    linkage = pd.read_csv(input_path, low_memory=False)
    if "satellite_key" not in linkage.columns:
        raise ValueError("expected s5b v2 links CSV with satellite_key column: {}".format(input_path))
    return _normalize_v2_linkage_table(linkage, s5_csv)


def _linkage_payload(row):
    return {
        "satellite_location_uid": _safe_text(row.get("satellite_location_uid", "")),
        "linked_cluster_id": _safe_int(row.get("linked_cluster_id", -1), default=-1),
        "linked_cluster_uid": _safe_text(row.get("linked_cluster_uid", "")),
        "linked_resolution": _safe_text(row.get("linked_resolution", "")),
        "link_resolution_relation": _safe_text(row.get("link_resolution_relation", "")),
        "link_attempted_resolutions": _safe_text(row.get("link_attempted_resolutions", "")),
        "link_status": _safe_text(row.get("link_status", "")),
        "link_reason": _safe_text(row.get("link_reason", row.get("unlinked_reason", ""))),
        "link_method": _safe_text(row.get("link_method", "")),
        "linkage_mode": _safe_text(row.get("linkage_mode", "")),
        "link_quality": _safe_text(row.get("link_quality", "")),
        "satellite_reach_id": _safe_int(row.get("satellite_reach_id", -1), default=-1),
        "main_reach_id": _safe_int(row.get("main_reach_id", -1), default=-1),
        "same_reach": _safe_bool_int(row.get("same_reach", -1), default=-1),
        "reach_hops": _safe_int(row.get("reach_hops", -1), default=-1),
        "point_distance_m": _safe_float(row.get("point_distance_m", row.get("link_distance_m", np.nan))),
        "network_distance_m": _safe_float(row.get("network_distance_m", np.nan)),
        "area_log10_diff": _safe_float(
            row.get("area_log10_diff", row.get("link_uparea_log10_error", np.nan))
        ),
        "candidate_count": _safe_int(row.get("candidate_count", row.get("link_candidate_count", 0)), default=0),
        "eligible_candidate_count": _safe_int(
            row.get("eligible_candidate_count", row.get("link_candidate_count", 0)),
            default=0,
        ),
        "link_distance_m": _safe_float(row.get("link_distance_m", np.nan)),
        "link_uparea_log10_error": _safe_float(
            row.get("link_uparea_log10_error", np.nan)
        ),
        "link_candidate_count": _safe_int(row.get("link_candidate_count", 0), default=0),
        "unlinked_reason": _safe_text(row.get("unlinked_reason", "")),
    }


def _time_bounds(dates):
    if not dates:
        return "", ""
    times = pd.to_datetime(list(dates), errors="coerce")
    times = pd.Series(times).dropna()
    if len(times) == 0:
        return "", ""
    return times.min().strftime("%Y-%m-%d"), times.max().strftime("%Y-%m-%d")


def _worker_load_satellite_candidate(payload):
    resolved_path = Path(payload["resolved_candidate_path"])
    if not resolved_path.is_file():
        return {"status": "unreadable", "reason": "missing file"}

    source = _safe_text(payload.get("source", ""))
    observation_type = _safe_text(payload.get("observation_type", ""))
    source_family = classify_source_family(source)
    if source_family != "satellite":
        return {"status": "skip_non_satellite"}

    series, _unit_issues = load_nc_series(str(resolved_path))
    if series is None or len(series) == 0:
        return {"status": "unreadable", "reason": "empty or unreadable timeseries"}

    station_name, river_name, source_station_native_id = _read_station_meta_from_nc(str(resolved_path))
    source_long_name, institution, reference, source_url = _read_source_meta_from_nc(str(resolved_path))
    global_attr_payload = merge_global_attrs_for_paths([str(resolved_path)])

    resolution = _normalize_resolution(payload.get("resolution", ""))
    cluster_id = _safe_int(payload.get("cluster_id", -1), default=-1)
    cluster_uid = _safe_text(payload.get("cluster_uid", ""))
    linkage = _linkage_payload(payload)
    station_key = (
        linkage["satellite_location_uid"],
        cluster_id,
        source,
        observation_type,
        resolution,
        _safe_text(payload.get("candidate_path", "")),
        _safe_text(payload.get("resolved_candidate_path", "")),
        _safe_text(station_name),
        _safe_text(river_name),
        _safe_text(source_station_native_id),
    )

    records = []
    for rec in series.itertuples(index=False):
        date_ts = pd.Timestamp(getattr(rec, "date"))
        time_val = (date_ts - pd.Timestamp("1970-01-01")).total_seconds() / 86400.0
        records.append(
            {
                "cluster_id": cluster_id,
                "time": float(time_val),
                "date": date_ts.strftime("%Y-%m-%d"),
                "resolution": resolution,
                "Q": float(getattr(rec, "Q")) if pd.notna(getattr(rec, "Q")) else np.nan,
                "SSC": float(getattr(rec, "SSC")) if pd.notna(getattr(rec, "SSC")) else np.nan,
                "SSL": float(getattr(rec, "SSL")) if pd.notna(getattr(rec, "SSL")) else np.nan,
                "Q_flag": int(getattr(rec, "Q_flag")) if pd.notna(getattr(rec, "Q_flag")) else 9,
                "SSC_flag": int(getattr(rec, "SSC_flag")) if pd.notna(getattr(rec, "SSC_flag")) else 9,
                "SSL_flag": int(getattr(rec, "SSL_flag")) if pd.notna(getattr(rec, "SSL_flag")) else 9,
            }
        )

    return {
        "status": "ok",
        "station_key": station_key,
        "station_payload": {
            "cluster_id": cluster_id,
            "cluster_uid": cluster_uid
            or ("SED{:06d}".format(cluster_id) if cluster_id >= 0 else ""),
            "source": source,
            "source_family": source_family,
            "observation_type": observation_type,
            "source_station_native_id": _safe_text(source_station_native_id),
            "station_name": _safe_text(station_name),
            "river_name": _safe_text(river_name),
            "lat": _safe_float(payload.get("lat", np.nan)),
            "lon": _safe_float(payload.get("lon", np.nan)),
            "resolution": resolution,
            "candidate_path": _safe_text(payload.get("candidate_path", "")),
            "resolved_candidate_path": str(resolved_path),
            "validation_only": 1,
            "merge_policy": "validation_only",
            "global_attr_payload": global_attr_payload,
            **linkage,
        },
        "source_meta": {
            "source": source,
            "source_long_name": source_long_name,
            "institution": institution,
            "reference": reference,
            "source_url": source_url,
        },
        "records": records,
    }


def _write_satellite_validation_nc(
    out_path,
    station_rows,
    record_rows,
    source_meta_rows,
):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    n_stations = len(station_rows)
    n_records = len(record_rows)
    sources = sorted(source_meta_rows.keys())
    source_to_idx = {name: i for i, name in enumerate(sources)}

    with nc4.Dataset(out_path, "w", format="NETCDF4") as nc:
        nc.createDimension("n_satellite_stations", n_stations)
        nc.createDimension("n_satellite_records", n_records)
        nc.createDimension("n_sources", len(sources))

        def _str_station_var(name, long_name):
            v = nc.createVariable(name, str, ("n_satellite_stations",))
            v.long_name = long_name
            return v

        sat_uid_v = _str_station_var("satellite_station_uid", "stable satellite validation station uid")
        sat_location_uid_v = _str_station_var(
            "satellite_location_uid", "stable satellite location identifier independent of resolution"
        )
        cluster_uid_v = _str_station_var("cluster_uid", "stable cluster uid")
        linked_cluster_uid_v = _str_station_var(
            "linked_cluster_uid",
            "main reference cluster lookup key; does not merge satellite data into main matrices",
        )
        unlinked_reason_v = _str_station_var(
            "unlinked_reason", "standard reason when no main reference cluster is linked"
        )
        source_v = _str_station_var("source", "source dataset short name")
        station_name_v = _str_station_var("station_name", "source station name")
        river_name_v = _str_station_var("river_name", "source river name")
        resolution_v = _str_station_var("station_resolution", "time resolution for this source station")

        station_global_attr_payloads = [
            row.get("global_attr_payload") or merge_global_attrs_for_paths([row.get("resolved_candidate_path", "")])
            for row in station_rows
        ]

        link_distance_v = nc.createVariable(
            "link_distance_m", "f4", ("n_satellite_stations",), fill_value=FILL
        )
        link_distance_v.long_name = "distance from satellite location to linked main cluster representative"
        link_distance_v.units = "m"
        link_area_error_v = nc.createVariable(
            "link_uparea_log10_error", "f4", ("n_satellite_stations",), fill_value=FILL
        )
        link_area_error_v.long_name = "relative upstream-area error used for s5b v2 linkage ranking"
        source_station_index_v = nc.createVariable("source_station_index", "i4", ("n_satellite_stations",))
        source_station_index_v.long_name = "0-based source-station index in satellite validation table"
        lat_v = nc.createVariable("lat", "f4", ("n_satellite_stations",), fill_value=FILL)
        lat_v.long_name = "station latitude"
        lat_v.units = "degrees_north"
        lon_v = nc.createVariable("lon", "f4", ("n_satellite_stations",), fill_value=FILL)
        lon_v.long_name = "station longitude"
        lon_v.units = "degrees_east"
        station_source_index_v = nc.createVariable("source_index", "i4", ("n_satellite_stations",), fill_value=-1)
        station_source_index_v.long_name = "0-based index into n_sources"

        rec_station_idx_v = nc.createVariable("satellite_station_index", "i4", ("n_satellite_records",))
        rec_station_idx_v.long_name = "0-based index into n_satellite_stations"
        rec_cluster_id_v = nc.createVariable("cluster_id", "i4", ("n_satellite_records",))
        rec_cluster_id_v.long_name = "cluster id for each record"
        rec_time_v = nc.createVariable("time", "f8", ("n_satellite_records",))
        rec_time_v.long_name = "time"
        rec_time_v.units = "days since 1970-01-01"
        rec_time_v.calendar = "gregorian"
        rec_date_v = nc.createVariable("date", str, ("n_satellite_records",))
        rec_date_v.long_name = "ISO date text"
        rec_resolution_v = nc.createVariable("resolution", str, ("n_satellite_records",))
        rec_resolution_v.long_name = "time resolution"

        q_v = nc.createVariable("Q", "f4", ("n_satellite_records",), fill_value=FILL)
        q_v.units = "m3 s-1"
        ssc_v = nc.createVariable("SSC", "f4", ("n_satellite_records",), fill_value=FILL)
        ssc_v.units = "mg L-1"
        ssl_v = nc.createVariable("SSL", "f4", ("n_satellite_records",), fill_value=FILL)
        ssl_v.units = SCIENCE_UNITS["SSL"]

        qf_v = nc.createVariable("Q_flag", "i1", ("n_satellite_records",), fill_value=np.int8(9))
        sscf_v = nc.createVariable("SSC_flag", "i1", ("n_satellite_records",), fill_value=np.int8(9))
        sslf_v = nc.createVariable("SSL_flag", "i1", ("n_satellite_records",), fill_value=np.int8(9))
        for var in (qf_v, sscf_v, sslf_v):
            var.flag_values = FLAG_VALUES
            var.flag_meanings = FLAG_MEANINGS

        source_name_v = nc.createVariable("source_name", str, ("n_sources",))
        source_long_name_v = nc.createVariable("source_long_name", str, ("n_sources",))
        reference_v = nc.createVariable("reference", str, ("n_sources",))
        source_url_v = nc.createVariable("source_url", str, ("n_sources",))

        cluster_id_station_v = nc.createVariable("cluster_id_station", "i4", ("n_satellite_stations",), fill_value=-1)
        cluster_id_station_v.long_name = "cluster id for each satellite validation station"
        source_family_v = nc.createVariable("source_family", str, ("n_satellite_stations",))
        source_family_v.long_name = "source family classification"
        validation_only_v = nc.createVariable("validation_only", "i1", ("n_satellite_stations",), fill_value=np.int8(0))
        validation_only_v.long_name = "1 when this station is validation-only and excluded from the main merge"
        merge_policy_v = nc.createVariable("merge_policy", str, ("n_satellite_stations",))
        merge_policy_v.long_name = "merge policy for this station in the context of the main reference merge"

        sat_uid_v[:] = np.asarray([row["satellite_station_uid"] for row in station_rows], dtype=object)
        sat_location_uid_v[:] = np.asarray(
            [row["satellite_location_uid"] for row in station_rows], dtype=object
        )
        cluster_uid_v[:] = np.asarray([row["cluster_uid"] for row in station_rows], dtype=object)
        linked_cluster_uid_v[:] = np.asarray(
            [row["linked_cluster_uid"] for row in station_rows], dtype=object
        )
        unlinked_reason_v[:] = np.asarray(
            [row["unlinked_reason"] for row in station_rows], dtype=object
        )
        source_v[:] = np.asarray([row["source"] for row in station_rows], dtype=object)
        station_name_v[:] = np.asarray([row["station_name"] for row in station_rows], dtype=object)
        river_name_v[:] = np.asarray([row["river_name"] for row in station_rows], dtype=object)
        resolution_v[:] = np.asarray([row["resolution"] for row in station_rows], dtype=object)
        source_station_index_v[:] = np.asarray([row["source_station_index"] for row in station_rows], dtype=np.int32)
        station_source_index_v[:] = np.asarray([source_to_idx.get(row["source"], -1) for row in station_rows], dtype=np.int32)

        cluster_id_station_v[:] = np.asarray([_safe_int(row.get("cluster_id", -1), default=-1) for row in station_rows], dtype=np.int32)
        source_family_v[:] = np.asarray([classify_source_family(row.get("source", "")) for row in station_rows], dtype=object)
        validation_only_v[:] = np.asarray([np.int8(1) for _ in station_rows], dtype=np.int8)
        merge_policy_v[:] = np.asarray(["validation_only" for _ in station_rows], dtype=object)

        link_distance_vals = np.asarray(
            [
                row["link_distance_m"] if row["link_distance_m"] is not None else np.nan
                for row in station_rows
            ],
            dtype=np.float32,
        )
        link_area_error_vals = np.asarray(
            [
                row["link_uparea_log10_error"]
                if row["link_uparea_log10_error"] is not None
                else np.nan
                for row in station_rows
            ],
            dtype=np.float32,
        )
        link_distance_vals[np.isnan(link_distance_vals)] = FILL
        link_area_error_vals[np.isnan(link_area_error_vals)] = FILL
        link_distance_v[:] = link_distance_vals
        link_area_error_v[:] = link_area_error_vals

        write_global_attr_payload_variables(
            nc,
            "n_satellite_stations",
            "satellite_station",
            station_global_attr_payloads,
            "satellite validation station",
            include_names=True, include_count=True,
        )
        write_promoted_global_attr_variables(
            nc,
            "n_satellite_stations",
            station_global_attr_payloads,
            subject="satellite validation station",
            omit_fields=("station_id", "dataset_name", "data_source_name", "observation_type", "temporal_resolution", "creator_name", "creator_email", "creator_institution", "source_data_link", "processing_level", "featureType", "date_created", "date_modified"),
        )

        lat_vals = np.asarray(
            [row["lat"] if row["lat"] is not None else np.nan for row in station_rows], dtype=np.float32
        )
        lon_vals = np.asarray(
            [row["lon"] if row["lon"] is not None else np.nan for row in station_rows], dtype=np.float32
        )
        lat_vals[np.isnan(lat_vals)] = FILL
        lon_vals[np.isnan(lon_vals)] = FILL
        lat_v[:] = lat_vals
        lon_v[:] = lon_vals

        rec_station_idx_v[:] = np.asarray([row["satellite_station_index"] for row in record_rows], dtype=np.int32)
        rec_cluster_id_v[:] = np.asarray([row["cluster_id"] for row in record_rows], dtype=np.int32)
        rec_time_v[:] = np.asarray([row["time"] for row in record_rows], dtype=np.float64)
        rec_date_v[:] = np.asarray([row["date"] for row in record_rows], dtype=object)
        rec_resolution_v[:] = np.asarray([row["resolution"] for row in record_rows], dtype=object)

        q_vals = np.asarray([row["Q"] for row in record_rows], dtype=np.float32)
        ssc_vals = np.asarray([row["SSC"] for row in record_rows], dtype=np.float32)
        ssl_vals = np.asarray([row["SSL"] for row in record_rows], dtype=np.float32)
        q_vals[np.isnan(q_vals)] = FILL
        ssc_vals[np.isnan(ssc_vals)] = FILL
        ssl_vals[np.isnan(ssl_vals)] = FILL
        q_v[:] = q_vals
        ssc_v[:] = ssc_vals
        ssl_v[:] = ssl_vals
        qf_v[:] = np.asarray([row["Q_flag"] for row in record_rows], dtype=np.int8)
        sscf_v[:] = np.asarray([row["SSC_flag"] for row in record_rows], dtype=np.int8)
        sslf_v[:] = np.asarray([row["SSL_flag"] for row in record_rows], dtype=np.int8)

        source_name_v[:] = np.asarray(sources, dtype=object)
        source_long_name_v[:] = np.asarray(
            [_safe_text(source_meta_rows[name].get("source_long_name", "")) for name in sources],
            dtype=object,
        )
        reference_v[:] = np.asarray(
            [_safe_text(source_meta_rows[name].get("reference", "")) for name in sources],
            dtype=object,
        )
        source_url_v[:] = np.asarray(
            [_safe_text(source_meta_rows[name].get("source_url", "")) for name in sources],
            dtype=object,
        )

        nc.title = "Satellite validation-only sediment observations"
        nc.role = "validation_only"
        nc.merge_policy = "excluded from main station reference merge"
        nc.intended_use = (
            "satellite-vs-station validation and diagnostic comparison; not used for station-reference merging"
        )
        nc.source = "Exported from s5b v2 satellite-main-cluster links and source NetCDF files"
        nc.linked_cluster_semantics = (
            "linked_cluster_uid and linked_cluster_id connect validation-only satellite locations "
            "to main reference records; they do not indicate that satellite observations were "
            "merged into the main station matrices"
        )
        # Conventions managed by apply_release_conventions
        nc.qc_stage_schema_version = "1"
        set_global_attr_policy(nc)
        nc.history = "Created {} by s6_export_satellite_validation_to_nc.py".format(
            datetime.now().isoformat(timespec="seconds")
        )
        nc.n_satellite_stations = str(n_stations)
        nc.n_satellite_records = str(n_records)
        nc.validation_only_source_families = "satellite"
        nc.created = datetime.now().isoformat(timespec="seconds")
        nc.sync()

    apply_release_conventions(out_path, "satellite")


def _build_satellite_catalog(station_rows, station_record_map):
    catalog_rows = []
    for station_index, station_row in enumerate(station_rows):
        recs = station_record_map.get(station_index, [])
        time_start, time_end = _time_bounds([item["date"] for item in recs])
        row = {
            "satellite_station_uid": station_row["satellite_station_uid"],
            "station_name": station_row["station_name"],
            "river_name": station_row["river_name"],
            "source": station_row["source"],
            "resolution": station_row["resolution"],
            "time_start": time_start,
            "time_end": time_end,
            "n_records": int(len(recs)),
            "lat": station_row["lat"] if station_row["lat"] is not None else np.nan,
            "lon": station_row["lon"] if station_row["lon"] is not None else np.nan,
            "geographic_coverage": station_row.get("global_attr_payload", {}).get("promoted", {}).get("geographic_coverage", ""),
            "cluster_uid": station_row["cluster_uid"],
            "linked_cluster_uid": station_row["linked_cluster_uid"],
            "unlinked_reason": station_row["unlinked_reason"],
            "link_distance_m": station_row["link_distance_m"],
            "link_uparea_log10_error": station_row["link_uparea_log10_error"],
            "satellite_location_uid": station_row["satellite_location_uid"],
            "cluster_id": station_row["cluster_id"],
"source_family": classify_source_family(station_row["source"]),            "validation_only": 1,            "merge_policy": "validation_only",
            "source_station_index": station_row["source_station_index"],
        }
        catalog_rows.append(row)

    return pd.DataFrame(catalog_rows).sort_values(
        ["resolution", "cluster_uid", "source", "satellite_station_uid"],
        kind="mergesort",
    ).reset_index(drop=True)

def _parse_runtime_args(argv):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--input", default=str(BUILTIN_INPUT))
    parser.add_argument("--s5-csv", default=str(BUILTIN_S5_CSV))
    parser.add_argument("--output", default=str(BUILTIN_OUTPUT))
    parser.add_argument("--catalog", default=str(BUILTIN_CATALOG))
    parser.add_argument("--progress-log", default="")
    args, _unknown = parser.parse_known_args(argv)
    return args


def main():
    runtime_args = _parse_runtime_args(sys.argv[1:])
    _geo_options = boundary_options_from_argv(sys.argv[1:])
    _progress_log = runtime_args.progress_log

    if not HAS_NC or nc4 is None:
        print("Error: netCDF4 is required.")
        return 1

    input_path = Path(runtime_args.input).resolve()
    s5_csv = Path(runtime_args.s5_csv).resolve()
    output_path = Path(runtime_args.output).resolve()
    catalog_path = Path(runtime_args.catalog).resolve()
    allowed_resolutions = set(_normalize_resolution(item) for item in BUILTIN_RESOLUTIONS)

    if not input_path.is_file():
        print("Error: input not found: {}".format(input_path))
        return 1
    if not s5_csv.is_file():
        print("Error: S5 CSV not found: {}".format(s5_csv))
        return 1

    try:
        stations = _load_linkage_input(input_path, s5_csv)
    except Exception as exc:
        print("Error: {}".format(exc))
        return 1
    required_columns = {
        "satellite_location_uid",
        "source",
        "path",
        "cluster_id",
        "cluster_uid",
        "resolution",
        "observation_type",
        *LINKAGE_TEXT_FIELDS,
        *LINKAGE_NUMERIC_FIELDS,
    }
    missing = sorted(required_columns - set(stations.columns))
    if missing:
        print("Error: input missing columns: {}".format(", ".join(missing)))
        return 1

    stations["resolution_norm"] = stations["resolution"].map(_normalize_resolution)
    blank_observation_type = stations["observation_type"].fillna("").astype(str).str.strip().eq("")
    if blank_observation_type.any():
        print(
            "Error: input has {} rows with blank observation_type; cannot classify satellite validation candidates.".format(
                int(blank_observation_type.sum())
            )
        )
        return 1

    stations["source_family"] = stations.apply(
        lambda r: classify_source_family(
            r.get("source", ""),
            resolution=r.get("resolution"),
            observation_type=r.get("observation_type", ""),
        ),
        axis=1,
    )
    stations = stations[stations["resolution_norm"].isin(allowed_resolutions)].copy()
    stations = stations[stations["source_family"].eq("satellite")].copy()
    if len(stations) == 0:
        print("No satellite validation candidates found.")
        return 1

    stations["_candidate_path"] = stations["path"].astype(str)
    stations["_resolved_candidate_path"] = stations["_candidate_path"].map(_resolve_station_path)

    source_meta_rows = {}
    station_key_to_idx = {}
    station_rows = []
    station_record_map = {}
    record_rows = []
    unreadable = 0

    payloads = []
    for _, row in stations.iterrows():
        payloads.append(
            {
                "source": _safe_text(row.get("source", "")),
                "observation_type": _safe_text(row.get("observation_type", "")),
                "cluster_id": _safe_int(row.get("cluster_id", -1), default=-1),
                "cluster_uid": _safe_text(row.get("cluster_uid", "")),
                "resolution": _normalize_resolution(row.get("resolution_norm", row.get("resolution", ""))),
                "lat": row.get("lat", np.nan),
                "lon": row.get("lon", np.nan),
                "candidate_path": _safe_text(row.get("_candidate_path", "")),
                "resolved_candidate_path": _safe_text(row.get("_resolved_candidate_path", "")),
                **_linkage_payload(row),
            }
        )

    n_workers = int(_default_workers_for_host() or 0)
    if n_workers <= 0:
        n_workers = max(1, os.cpu_count() or 1)
    n_workers = min(n_workers, max(1, len(payloads)))
    print(
        "Satellite candidate rows: {} | workers={} | resolutions={}".format(
            len(payloads),
            n_workers,
            ",".join(sorted(allowed_resolutions)),
        )
    )

    if n_workers <= 1:
        worker_results = map(_worker_load_satellite_candidate, payloads)
    else:
        chunksize = max(8, min(64, len(payloads) // max(1, n_workers * 4)))
        executor = ProcessPoolExecutor(max_workers=n_workers)
        worker_results = executor.map(_worker_load_satellite_candidate, payloads, chunksize=chunksize)

    progress_fo = None
    if _progress_log:
        try:
            progress_fo = open(_progress_log, "w", buffering=1)
        except Exception:
            pass  # fail silently — progress logging is optional

    try:
        _iterator = tqdm(
            worker_results,
            total=len(payloads),
            desc="Satellite",
            unit="candidate",
            file=progress_fo or sys.stderr,
            disable=progress_fo is None and not sys.stderr.isatty(),
            mininterval=1.0,
        )
        for res in _iterator:
            status = res.get("status", "")
            if status == "ok":
                source_meta = res.get("source_meta", {})
                source_name = _safe_text(source_meta.get("source", ""))
                if source_name and source_name not in source_meta_rows:
                    source_meta_rows[source_name] = {
                        "source_long_name": source_meta.get("source_long_name", ""),
                        "institution": source_meta.get("institution", ""),
                        "reference": source_meta.get("reference", ""),
                        "source_url": source_meta.get("source_url", ""),
                    }

                station_key = res["station_key"]
                station_payload = dict(res["station_payload"])
                station_index = station_key_to_idx.get(station_key)
                if station_index is None:
                    station_index = len(station_rows)
                    station_key_to_idx[station_key] = station_index
                    station_payload["satellite_station_uid"] = "SAT{:06d}".format(station_index)
                    station_payload["source_station_index"] = station_index
                    station_rows.append(station_payload)
                    station_record_map[station_index] = []

                for rec_row in res.get("records", []):
                    out_row = dict(rec_row)
                    out_row["satellite_station_index"] = station_index
                    record_rows.append(out_row)
                    station_record_map[station_index].append(out_row)
            elif status == "unreadable":
                unreadable += 1
    finally:
        if progress_fo:
            progress_fo.close()
        if n_workers > 1:
            executor.shutdown(wait=True)

    if len(station_rows) == 0 or len(record_rows) == 0:
        print("No satellite validation candidates found.")
        if unreadable > 0:
            print("Unreadable candidate files: {}".format(unreadable))
        return 1

    record_rows.sort(key=lambda item: (item["satellite_station_index"], item["time"]))
    station_global_attr_payloads = [
        row.get("global_attr_payload") or merge_global_attrs_for_paths([row.get("resolved_candidate_path", "")])
        for row in station_rows
    ]
    enrich_global_attr_payloads(
        station_global_attr_payloads,
        [row.get("lat", np.nan) for row in station_rows],
        [row.get("lon", np.nan) for row in station_rows],
        subject="s6 satellite validation stations",
        **_geo_options,
    )
    for station_row, payload in zip(station_rows, station_global_attr_payloads):
        station_row["global_attr_payload"] = payload

    _write_satellite_validation_nc(
        output_path,
        station_rows=station_rows,
        record_rows=record_rows,
        source_meta_rows=source_meta_rows,
    )

    catalog_df = _build_satellite_catalog(station_rows, station_record_map)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_df.to_csv(catalog_path, index=False)

    print("Wrote satellite validation NC: {}".format(output_path))
    print("Wrote satellite validation catalog: {} ({} rows)".format(catalog_path, len(catalog_df)))
    if unreadable > 0:
        print("Warning: skipped unreadable candidate files: {}".format(unreadable))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
