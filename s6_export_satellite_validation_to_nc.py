#!/usr/bin/env python3
"""
Export satellite validation-only sediment observations from s5 candidates.

Runtime policy is built-in so users can run:
  python3 s6_export_satellite_validation_to_nc.py
without passing CLI arguments.
"""

import os
import socket
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import sys
from tqdm import tqdm

import numpy as np
import pandas as pd

from pipeline_paths import (
    S2_ORGANIZED_DIR,
    S5_BASIN_CLUSTERED_CSV,
    S6_SATELLITE_VALIDATION_CATALOG_CSV,
    S6_SATELLITE_VALIDATION_NC,
    get_output_r_root,
)
from s6_basin_merge_to_nc import (
    FILL,
    HAS_NC,
    _read_source_meta_from_nc,
    _read_station_meta_from_nc,
    classify_source_family_from_observation_type,
    load_nc_series,
)

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
ORGANIZED_ROOT = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()

DEFAULT_INPUT = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUTPUT = PROJECT_ROOT / S6_SATELLITE_VALIDATION_NC
DEFAULT_CATALOG = PROJECT_ROOT / S6_SATELLITE_VALIDATION_CATALOG_CSV
DEFAULT_RESOLUTIONS = ("daily", "monthly", "annual")
DEFAULT_WORKERS = min(8, max(1, os.cpu_count() or 1))

# ---- built-in runtime parameters (edit here; no CLI input required) ----
BUILTIN_INPUT = DEFAULT_INPUT
BUILTIN_OUTPUT = DEFAULT_OUTPUT
BUILTIN_CATALOG = DEFAULT_CATALOG
BUILTIN_RESOLUTIONS = DEFAULT_RESOLUTIONS
BUILTIN_WORKERS_BY_HOST = {
    "node113": 24,
}


def _default_workers_for_host():
    host = str(socket.gethostname() or "").split(".")[0].strip().lower()
    configured = BUILTIN_WORKERS_BY_HOST.get(host, DEFAULT_WORKERS)
    configured = int(configured) if configured is not None else DEFAULT_WORKERS
    if configured <= 0:
        configured = max(1, os.cpu_count() or 1)
    return max(1, configured)


def is_satellite_observation(observation_type):
    return classify_source_family_from_observation_type(observation_type) == "satellite"


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


def _safe_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


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
    source_family = classify_source_family_from_observation_type(observation_type)
    if source_family != "satellite":
        return {"status": "skip_non_satellite"}

    series, _unit_issues = load_nc_series(str(resolved_path))
    if series is None or len(series) == 0:
        return {"status": "unreadable", "reason": "empty or unreadable timeseries"}

    station_name, river_name, source_station_native_id = _read_station_meta_from_nc(str(resolved_path))
    source_long_name, institution, reference, source_url = _read_source_meta_from_nc(str(resolved_path))

    resolution = _normalize_resolution(payload.get("resolution", ""))
    cluster_id = _safe_int(payload.get("cluster_id", -1), default=-1)
    station_key = (
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
            "cluster_uid": "SED{:06d}".format(cluster_id) if cluster_id >= 0 else "",
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
        cluster_uid_v = _str_station_var("cluster_uid", "stable cluster uid")
        source_v = _str_station_var("source", "source dataset short name")
        family_v = _str_station_var("source_family", "source family")
        native_id_v = _str_station_var("source_station_native_id", "native source station id")
        station_name_v = _str_station_var("station_name", "source station name")
        river_name_v = _str_station_var("river_name", "source river name")
        resolution_v = _str_station_var("station_resolution", "time resolution for this source station")
        candidate_path_v = _str_station_var("candidate_path", "candidate path text from s5")
        resolved_path_v = _str_station_var("resolved_candidate_path", "resolved absolute candidate path")
        merge_policy_v = _str_station_var("merge_policy", "merge policy for this station")

        cluster_id_v = nc.createVariable("cluster_id_station", "i4", ("n_satellite_stations",))
        cluster_id_v.long_name = "cluster id from s5 for this source station"
        source_station_index_v = nc.createVariable("source_station_index", "i4", ("n_satellite_stations",))
        source_station_index_v.long_name = "0-based source-station index in satellite validation table"
        lat_v = nc.createVariable("lat", "f4", ("n_satellite_stations",), fill_value=FILL)
        lat_v.long_name = "station latitude"
        lat_v.units = "degrees_north"
        lon_v = nc.createVariable("lon", "f4", ("n_satellite_stations",), fill_value=FILL)
        lon_v.long_name = "station longitude"
        lon_v.units = "degrees_east"
        validation_only_v = nc.createVariable("validation_only", "i1", ("n_satellite_stations",), fill_value=np.int8(1))
        validation_only_v.long_name = "validation-only flag"
        validation_only_v.flag_values = np.array([0, 1], dtype=np.int8)
        validation_only_v.flag_meanings = "false true"
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
        ssl_v.units = "ton day-1"

        qf_v = nc.createVariable("Q_flag", "i1", ("n_satellite_records",), fill_value=np.int8(9))
        sscf_v = nc.createVariable("SSC_flag", "i1", ("n_satellite_records",), fill_value=np.int8(9))
        sslf_v = nc.createVariable("SSL_flag", "i1", ("n_satellite_records",), fill_value=np.int8(9))
        for var in (qf_v, sscf_v, sslf_v):
            var.flag_values = np.array([0, 1, 2, 3, 9], dtype=np.int8)
            var.flag_meanings = "good estimated suspect bad missing"

        source_name_v = nc.createVariable("source_name", str, ("n_sources",))
        source_long_name_v = nc.createVariable("source_long_name", str, ("n_sources",))
        institution_v = nc.createVariable("institution", str, ("n_sources",))
        reference_v = nc.createVariable("reference", str, ("n_sources",))
        source_url_v = nc.createVariable("source_url", str, ("n_sources",))

        sat_uid_v[:] = np.asarray([row["satellite_station_uid"] for row in station_rows], dtype=object)
        cluster_uid_v[:] = np.asarray([row["cluster_uid"] for row in station_rows], dtype=object)
        source_v[:] = np.asarray([row["source"] for row in station_rows], dtype=object)
        family_v[:] = np.asarray([row["source_family"] for row in station_rows], dtype=object)
        native_id_v[:] = np.asarray([row["source_station_native_id"] for row in station_rows], dtype=object)
        station_name_v[:] = np.asarray([row["station_name"] for row in station_rows], dtype=object)
        river_name_v[:] = np.asarray([row["river_name"] for row in station_rows], dtype=object)
        resolution_v[:] = np.asarray([row["resolution"] for row in station_rows], dtype=object)
        candidate_path_v[:] = np.asarray([row["candidate_path"] for row in station_rows], dtype=object)
        resolved_path_v[:] = np.asarray([row["resolved_candidate_path"] for row in station_rows], dtype=object)
        merge_policy_v[:] = np.asarray([row["merge_policy"] for row in station_rows], dtype=object)
        cluster_id_v[:] = np.asarray([row["cluster_id"] for row in station_rows], dtype=np.int32)
        source_station_index_v[:] = np.asarray([row["source_station_index"] for row in station_rows], dtype=np.int32)
        station_source_index_v[:] = np.asarray([source_to_idx.get(row["source"], -1) for row in station_rows], dtype=np.int32)
        validation_only_v[:] = np.ones(n_stations, dtype=np.int8)

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
        institution_v[:] = np.asarray(
            [_safe_text(source_meta_rows[name].get("institution", "")) for name in sources],
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
        nc.source = "Exported from s5_basin_clustered_stations.csv satellite candidates and source NetCDF files"
        nc.Conventions = "CF-1.8"
        nc.qc_stage_schema_version = "1"
        nc.history = "Created {} by s6_export_satellite_validation_to_nc.py".format(
            datetime.now().isoformat(timespec="seconds")
        )
        nc.n_satellite_stations = str(n_stations)
        nc.n_satellite_records = str(n_records)
        nc.validation_only_source_families = "satellite"
        nc.created = datetime.now().isoformat(timespec="seconds")
        nc.sync()


def main():
    # parse optional --progress-log (no other CLI args)
    _progress_log = ""
    _skip_next = False
    for _i, _a in enumerate(sys.argv[1:]):
        if _skip_next:
            _skip_next = False
            continue
        if _a == "--progress-log" and _i + 2 < len(sys.argv):
            _progress_log = sys.argv[_i + 2]
            _skip_next = True
        elif _a.startswith("--progress-log="):
            _progress_log = _a.split("=", 1)[1]

    if not HAS_NC or nc4 is None:
        print("Error: netCDF4 is required.")
        return 1

    input_path = Path(BUILTIN_INPUT).resolve()
    output_path = Path(BUILTIN_OUTPUT).resolve()
    catalog_path = Path(BUILTIN_CATALOG).resolve()
    allowed_resolutions = set(_normalize_resolution(item) for item in BUILTIN_RESOLUTIONS)

    if not input_path.is_file():
        print("Error: input not found: {}".format(input_path))
        return 1

    stations = pd.read_csv(input_path)
    required_columns = {"source", "path", "cluster_id", "resolution", "observation_type"}
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

    stations["source_family"] = stations["observation_type"].map(classify_source_family_from_observation_type)
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
                "resolution": _normalize_resolution(row.get("resolution_norm", row.get("resolution", ""))),
                "lat": row.get("lat", np.nan),
                "lon": row.get("lon", np.nan),
                "candidate_path": _safe_text(row.get("_candidate_path", "")),
                "resolved_candidate_path": _safe_text(row.get("_resolved_candidate_path", "")),
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
    _write_satellite_validation_nc(
        output_path,
        station_rows=station_rows,
        record_rows=record_rows,
        source_meta_rows=source_meta_rows,
    )

    catalog_rows = []
    for station_index, station_row in enumerate(station_rows):
        recs = station_record_map.get(station_index, [])
        time_start, time_end = _time_bounds([item["date"] for item in recs])
        catalog_rows.append(
            {
                "satellite_station_uid": station_row["satellite_station_uid"],
                "cluster_uid": station_row["cluster_uid"],
                "cluster_id": station_row["cluster_id"],
                "source": station_row["source"],
                "source_family": station_row["source_family"],
                "observation_type": station_row["observation_type"],
                "resolution": station_row["resolution"],
                "lat": station_row["lat"] if station_row["lat"] is not None else np.nan,
                "lon": station_row["lon"] if station_row["lon"] is not None else np.nan,
                "station_name": station_row["station_name"],
                "river_name": station_row["river_name"],
                "source_station_native_id": station_row["source_station_native_id"],
                "candidate_path": station_row["candidate_path"],
                "resolved_candidate_path": station_row["resolved_candidate_path"],
                "n_records": int(len(recs)),
                "time_start": time_start,
                "time_end": time_end,
                "validation_only": 1,
                "merge_policy": "validation_only",
            }
        )

    catalog_df = pd.DataFrame(catalog_rows)
    catalog_df = catalog_df.sort_values(
        ["resolution", "cluster_uid", "source", "satellite_station_uid"], kind="mergesort"
    ).reset_index(drop=True)
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_df.to_csv(catalog_path, index=False)

    print("Wrote satellite validation NC: {}".format(output_path))
    print("Wrote satellite validation catalog: {} ({} rows)".format(catalog_path, len(catalog_df)))
    if unreadable > 0:
        print("Warning: skipped unreadable candidate files: {}".format(unreadable))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
