#!/usr/bin/env python3
"""
Shared cluster/source/basin catalog builders and multi-layer GPKG helpers.

The basin mainline only covers daily / monthly / annual products.
Climatology remains a separate product and is intentionally excluded here.
"""

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from basin_policy import MATCH_QUALITY_CODE_TO_NAME

try:
    import netCDF4 as nc4

    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False

try:
    import geopandas as gpd
    from shapely.geometry import Point

    HAS_GPD = True
except ImportError:
    gpd = None
    Point = None
    HAS_GPD = False


CLUSTER_RESOLUTIONS = ("daily", "monthly", "annual")
RESOLUTION_CODE_TO_NAME = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}
RESOLUTION_NAME_TO_CODE = {name: code for code, name in RESOLUTION_CODE_TO_NAME.items()}

STATION_CATALOG_COLUMNS = [
    "master_station_index",
    "cluster_uid",
    "cluster_id",
    "lat",
    "lon",
    "basin_area",
    "pfaf_code",
    "n_upstream_reaches",
    "station_name",
    "river_name",
    "source_station_id",
    "sources_used",
    "n_source_stations_in_cluster",
    "basin_match_quality_code",
    "basin_match_quality",
    "basin_status",
    "basin_flag",
    "basin_distance_m",
    "point_in_local",
    "point_in_basin",
    "daily_record_count",
    "daily_time_start",
    "daily_time_end",
    "monthly_record_count",
    "monthly_time_start",
    "monthly_time_end",
    "annual_record_count",
    "annual_time_start",
    "annual_time_end",
    "available_resolutions",
    "n_available_resolutions",
]

RESOLUTION_CATALOG_COLUMNS = [
    "master_station_index",
    "cluster_uid",
    "cluster_id",
    "resolution",
    "record_count",
    "time_start",
    "time_end",
    "station_name",
    "river_name",
    "source_station_id",
    "sources_used",
    "lat",
    "lon",
    "basin_area",
    "pfaf_code",
    "n_upstream_reaches",
    "basin_match_quality_code",
    "basin_match_quality",
    "basin_status",
    "basin_flag",
    "basin_distance_m",
    "point_in_local",
    "point_in_basin",
    "n_source_stations_in_cluster",
]

SOURCE_RESOLUTION_CATALOG_COLUMNS = [
    "source_station_index",
    "source_station_uid",
    "cluster_uid",
    "cluster_id",
    "resolution",
    "n_records",
    "time_start",
    "time_end",
    "source_name",
    "source_long_name",
    "institution",
    "reference",
    "source_url",
    "source_station_native_id",
    "source_station_name",
    "source_station_river_name",
    "source_station_lat",
    "source_station_lon",
    "source_station_paths",
    "source_station_cluster_index",
    "source_station_source_index",
]

BASIN_RESOLUTION_CATALOG_COLUMNS = [
    "cluster_uid",
    "cluster_id",
    "resolution",
    "record_count",
    "time_start",
    "time_end",
    "station_id",
    "basin_id",
    "basin_area",
    "pfaf_code",
    "area_error",
    "uparea_merit",
    "method",
    "n_upstream_reaches",
    "n_station_rows",
    "n_sources",
    "n_source_stations_in_cluster",
    "basin_match_quality",
    "basin_status",
    "basin_flag",
    "basin_distance_m",
    "point_in_local",
    "point_in_basin",
    "station_name",
    "river_name",
    "source_station_id",
]

SUMMARY_LAYER_COLUMNS = [
    "cluster_uid",
    "cluster_id",
    "station_name",
    "river_name",
    "source_station_id",
    "lat",
    "lon",
    "basin_area",
    "pfaf_code",
    "n_upstream_reaches",
    "basin_match_quality",
    "basin_status",
    "basin_flag",
    "basin_distance_m",
    "point_in_local",
    "point_in_basin",
    "n_source_stations_in_cluster",
    "available_resolutions",
    "n_available_resolutions",
    "daily_record_count",
    "daily_time_start",
    "daily_time_end",
    "monthly_record_count",
    "monthly_time_start",
    "monthly_time_end",
    "annual_record_count",
    "annual_time_start",
    "annual_time_end",
    "sources_used",
]

RESOLUTION_LAYER_COLUMNS = [
    "cluster_uid",
    "cluster_id",
    "resolution",
    "record_count",
    "time_start",
    "time_end",
    "station_name",
    "river_name",
    "sources_used",
    "lat",
    "lon",
    "basin_area",
    "pfaf_code",
    "n_upstream_reaches",
    "basin_match_quality",
    "basin_status",
    "basin_flag",
    "basin_distance_m",
    "point_in_local",
    "point_in_basin",
    "n_source_stations_in_cluster",
]

SOURCE_LAYER_COLUMNS = [
    "source_station_uid",
    "cluster_uid",
    "cluster_id",
    "resolution",
    "n_records",
    "time_start",
    "time_end",
    "source_name",
    "station_name",
    "river_name",
    "source_station_native_id",
    "source_station_paths",
    "lat",
    "lon",
]

BASIN_LAYER_COLUMNS = [
    "cluster_uid",
    "cluster_id",
    "resolution",
    "record_count",
    "time_start",
    "time_end",
    "station_id",
    "basin_id",
    "basin_area",
    "pfaf_code",
    "area_error",
    "uparea_merit",
    "method",
    "n_upstream_reaches",
    "n_station_rows",
    "n_sources",
    "n_source_stations_in_cluster",
    "basin_match_quality",
    "basin_status",
    "basin_flag",
    "basin_distance_m",
    "point_in_local",
    "point_in_basin",
    "station_name",
    "river_name",
    "source_station_id",
]


def _clean_text(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _read_text_var(ds, name, size=None):
    if name not in ds.variables:
        return [""] * int(size or 0)
    values = ds.variables[name][:]
    arr = np.asarray(values, dtype=object).reshape(-1)
    return [_clean_text(item) for item in arr]


def _read_float_array(ds, name, fill_values=None, size=None):
    if name not in ds.variables:
        return np.full(int(size or 0), np.nan, dtype=np.float64)
    arr = np.ma.asarray(ds.variables[name][:]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype=np.float64)
    if fill_values:
        for fill in fill_values:
            arr[arr == fill] = np.nan
    return arr


def _read_int_array(ds, name, fill_value=-1, size=None):
    if name not in ds.variables:
        return np.full(int(size or 0), fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        arr = raw.filled(fill_value)
    else:
        arr = np.asarray(raw)
    result = np.full(arr.shape, fill_value, dtype=np.int64)
    valid = np.isfinite(arr.astype(np.float64, copy=False))
    result[valid] = arr[valid].astype(np.int64)
    return result


def _decode_time_numbers(values, units, calendar):
    values = np.asarray(values, dtype=np.float64).reshape(-1)
    out = [""] * len(values)
    valid_mask = np.isfinite(values)
    if not valid_mask.any():
        return out
    try:
        decoded = nc4.num2date(
            values[valid_mask],
            units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
        )
    except TypeError:
        decoded = nc4.num2date(values[valid_mask], units, calendar=calendar)
    valid_values = []
    for item in decoded:
        text = _clean_text(item)
        if text:
            try:
                valid_values.append(pd.Timestamp(text).strftime("%Y-%m-%d %H:%M:%S"))
            except Exception:
                valid_values.append(text)
        else:
            valid_values.append("")
    for idx, text in zip(np.flatnonzero(valid_mask), valid_values):
        out[int(idx)] = text
    return out


def _collect_matrix_station_stats(matrix_nc, resolution, chunk_rows=16):
    with nc4.Dataset(matrix_nc, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        n_time = len(ds.dimensions["time"])
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        valid_counts = _read_int_array(ds, "n_valid_time_steps", fill_value=0, size=n_stations)
        time_var = ds.variables["time"]
        time_values = _read_float_array(ds, "time", size=n_time)
        time_units = getattr(time_var, "units", "days since 1970-01-01")
        time_calendar = getattr(time_var, "calendar", "gregorian")

        first_time_num = np.full(n_stations, np.nan, dtype=np.float64)
        last_time_num = np.full(n_stations, np.nan, dtype=np.float64)

        for start in range(0, n_stations, chunk_rows):
            stop = min(start + chunk_rows, n_stations)
            q = np.ma.asarray(ds.variables["Q"][start:stop, :]).filled(np.nan)
            ssc = np.ma.asarray(ds.variables["SSC"][start:stop, :]).filled(np.nan)
            ssl = np.ma.asarray(ds.variables["SSL"][start:stop, :]).filled(np.nan)
            mask = np.isfinite(q) | np.isfinite(ssc) | np.isfinite(ssl)
            row_has = mask.any(axis=1)
            if not np.any(row_has):
                continue

            first_idx = np.where(row_has, mask.argmax(axis=1), -1)
            last_idx = np.where(row_has, n_time - 1 - mask[:, ::-1].argmax(axis=1), -1)
            global_rows = np.arange(start, stop, dtype=np.int64)
            valid_rows = global_rows[row_has]
            first_time_num[valid_rows] = time_values[first_idx[row_has]]
            last_time_num[valid_rows] = time_values[last_idx[row_has]]

    return pd.DataFrame(
        {
            "cluster_uid": cluster_uids,
            "{}_record_count".format(resolution): valid_counts.astype(np.int64),
            "{}_time_start".format(resolution): _decode_time_numbers(
                first_time_num,
                time_units,
                time_calendar,
            ),
            "{}_time_end".format(resolution): _decode_time_numbers(
                last_time_num,
                time_units,
                time_calendar,
            ),
        }
    )


def _normalize_columns(frame, columns, int_fill_map, float_cols, text_cols):
    work = frame.copy()
    for col in columns:
        if col not in work.columns:
            work[col] = ""

    for col, fill_value in int_fill_map.items():
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(fill_value).astype(np.int64)
    for col in float_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    for col in text_cols:
        work[col] = work[col].fillna("").astype(str).str.strip()

    return work.reindex(columns=[col for col in columns if col in work.columns] + [col for col in work.columns if col not in columns])


def normalize_cluster_station_catalog(station_catalog):
    work = station_catalog.copy()
    for col in STATION_CATALOG_COLUMNS:
        if col not in work.columns:
            work[col] = ""

    int_fill_map = {
        "master_station_index": -1,
        "cluster_id": -1,
        "n_upstream_reaches": -9999,
        "n_source_stations_in_cluster": 0,
        "basin_match_quality_code": -1,
        "point_in_local": 0,
        "point_in_basin": 0,
        "daily_record_count": 0,
        "monthly_record_count": 0,
        "annual_record_count": 0,
        "n_available_resolutions": 0,
    }
    float_cols = ("lat", "lon", "basin_area", "pfaf_code", "basin_distance_m")
    text_cols = (
        "cluster_uid",
        "station_name",
        "river_name",
        "source_station_id",
        "sources_used",
        "basin_match_quality",
        "basin_status",
        "basin_flag",
        "daily_time_start",
        "daily_time_end",
        "monthly_time_start",
        "monthly_time_end",
        "annual_time_start",
        "annual_time_end",
        "available_resolutions",
    )

    for col, fill_value in int_fill_map.items():
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(fill_value).astype(np.int64)
    for col in float_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    for col in text_cols:
        work[col] = work[col].fillna("").astype(str).str.strip()

    work = work.reindex(columns=STATION_CATALOG_COLUMNS)
    return work


def normalize_cluster_resolution_catalog(resolution_catalog):
    work = resolution_catalog.copy()
    for col in RESOLUTION_CATALOG_COLUMNS:
        if col not in work.columns:
            work[col] = ""

    int_fill_map = {
        "master_station_index": -1,
        "cluster_id": -1,
        "record_count": 0,
        "n_upstream_reaches": -9999,
        "basin_match_quality_code": -1,
        "point_in_local": 0,
        "point_in_basin": 0,
        "n_source_stations_in_cluster": 0,
    }
    float_cols = ("lat", "lon", "basin_area", "pfaf_code", "basin_distance_m")
    text_cols = (
        "cluster_uid",
        "resolution",
        "time_start",
        "time_end",
        "station_name",
        "river_name",
        "source_station_id",
        "sources_used",
        "basin_match_quality",
        "basin_status",
        "basin_flag",
    )

    for col, fill_value in int_fill_map.items():
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(fill_value).astype(np.int64)
    for col in float_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    for col in text_cols:
        work[col] = work[col].fillna("").astype(str).str.strip()

    work = work.reindex(columns=RESOLUTION_CATALOG_COLUMNS)
    return work


def normalize_source_station_resolution_catalog(source_catalog):
    work = source_catalog.copy()
    for col in SOURCE_RESOLUTION_CATALOG_COLUMNS:
        if col not in work.columns:
            work[col] = ""

    int_fill_map = {
        "source_station_index": -1,
        "cluster_id": -1,
        "n_records": 0,
        "source_station_cluster_index": -1,
        "source_station_source_index": -1,
    }
    float_cols = ("source_station_lat", "source_station_lon")
    text_cols = tuple(
        col
        for col in SOURCE_RESOLUTION_CATALOG_COLUMNS
        if col not in int_fill_map and col not in float_cols
    )

    for col, fill_value in int_fill_map.items():
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(fill_value).astype(np.int64)
    for col in float_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    for col in text_cols:
        work[col] = work[col].fillna("").astype(str).str.strip()

    work = work.reindex(columns=SOURCE_RESOLUTION_CATALOG_COLUMNS)
    return work


def normalize_cluster_basin_resolution_catalog(basin_catalog):
    geometry = basin_catalog["geometry"] if "geometry" in basin_catalog.columns else None
    work = basin_catalog.copy()
    for col in BASIN_RESOLUTION_CATALOG_COLUMNS:
        if col not in work.columns:
            work[col] = ""

    int_fill_map = {
        "cluster_id": -1,
        "record_count": 0,
        "station_id": -1,
        "n_upstream_reaches": -9999,
        "n_station_rows": 0,
        "n_sources": 0,
        "n_source_stations_in_cluster": 0,
        "point_in_local": 0,
        "point_in_basin": 0,
    }
    float_cols = ("basin_area", "pfaf_code", "area_error", "uparea_merit", "basin_id", "basin_distance_m")
    text_cols = (
        "cluster_uid",
        "resolution",
        "time_start",
        "time_end",
        "method",
        "basin_match_quality",
        "basin_status",
        "basin_flag",
        "station_name",
        "river_name",
        "source_station_id",
    )

    for col, fill_value in int_fill_map.items():
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(fill_value).astype(np.int64)
    for col in float_cols:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    for col in text_cols:
        work[col] = work[col].fillna("").astype(str).str.strip()

    work = work.reindex(columns=BASIN_RESOLUTION_CATALOG_COLUMNS + (["geometry"] if geometry is not None else []))
    if geometry is not None and HAS_GPD:
        return gpd.GeoDataFrame(work, geometry="geometry", crs=getattr(basin_catalog, "crs", None))
    return work


def build_cluster_station_catalog(master_nc, matrix_paths):
    if not HAS_NC:
        raise RuntimeError("netCDF4 is required to build cluster catalogs")

    with nc4.Dataset(master_nc, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        cluster_ids = _read_int_array(ds, "cluster_id", fill_value=-1, size=n_stations)
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        if not any(cluster_uids):
            cluster_uids = [
                "SED{:06d}".format(int(cid)) if cid >= 0 else ""
                for cid in cluster_ids
            ]

        station_df = pd.DataFrame(
            {
                "master_station_index": np.arange(n_stations, dtype=np.int32),
                "cluster_uid": cluster_uids,
                "cluster_id": cluster_ids,
                "lat": _read_float_array(ds, "lat", fill_values=(-9999.0,), size=n_stations),
                "lon": _read_float_array(ds, "lon", fill_values=(-9999.0,), size=n_stations),
                "basin_area": _read_float_array(ds, "basin_area", fill_values=(-9999.0,), size=n_stations),
                "pfaf_code": _read_float_array(ds, "pfaf_code", fill_values=(-9999.0,), size=n_stations),
                "n_upstream_reaches": _read_int_array(
                    ds,
                    "n_upstream_reaches",
                    fill_value=-9999,
                    size=n_stations,
                ),
                "station_name": _read_text_var(ds, "station_name", size=n_stations),
                "river_name": _read_text_var(ds, "river_name", size=n_stations),
                "source_station_id": _read_text_var(ds, "source_station_id", size=n_stations),
                "sources_used": _read_text_var(ds, "sources_used", size=n_stations),
                "n_source_stations_in_cluster": _read_int_array(
                    ds,
                    "n_source_stations_in_cluster",
                    fill_value=0,
                    size=n_stations,
                ),
                "basin_status": _read_text_var(ds, "basin_status", size=n_stations),
                "basin_flag": _read_text_var(ds, "basin_flag", size=n_stations),
                "basin_distance_m": _read_float_array(
                    ds,
                    "basin_distance_m",
                    fill_values=(-9999.0,),
                    size=n_stations,
                ),
                "point_in_local": _read_int_array(
                    ds,
                    "point_in_local",
                    fill_value=0,
                    size=n_stations,
                ),
                "point_in_basin": _read_int_array(
                    ds,
                    "point_in_basin",
                    fill_value=0,
                    size=n_stations,
                ),
            }
        )

        match_codes = _read_int_array(ds, "basin_match_quality", fill_value=-1, size=n_stations)
        station_df["basin_match_quality_code"] = match_codes
        station_df["basin_match_quality"] = [
            MATCH_QUALITY_CODE_TO_NAME.get(int(code), "unknown")
            for code in match_codes
        ]

    for resolution in CLUSTER_RESOLUTIONS:
        matrix_path = Path(matrix_paths.get(resolution, ""))
        if matrix_path.is_file():
            stats_df = _collect_matrix_station_stats(matrix_path, resolution)
            station_df = station_df.merge(stats_df, on="cluster_uid", how="left")

    for resolution in CLUSTER_RESOLUTIONS:
        count_col = "{}_record_count".format(resolution)
        start_col = "{}_time_start".format(resolution)
        end_col = "{}_time_end".format(resolution)
        if count_col not in station_df.columns:
            station_df[count_col] = 0
        if start_col not in station_df.columns:
            station_df[start_col] = ""
        if end_col not in station_df.columns:
            station_df[end_col] = ""

    station_df = normalize_cluster_station_catalog(station_df)
    station_df["available_resolutions"] = station_df.apply(
        lambda row: "|".join(
            resolution
            for resolution in CLUSTER_RESOLUTIONS
            if int(row.get("{}_record_count".format(resolution), 0)) > 0
        ),
        axis=1,
    )
    station_df["n_available_resolutions"] = station_df["available_resolutions"].map(
        lambda text: len([part for part in text.split("|") if part])
    ).astype(np.int64)
    return normalize_cluster_station_catalog(station_df)


def build_cluster_resolution_catalog(station_catalog):
    station_df = normalize_cluster_station_catalog(station_catalog)
    rows = []

    for resolution in CLUSTER_RESOLUTIONS:
        count_col = "{}_record_count".format(resolution)
        start_col = "{}_time_start".format(resolution)
        end_col = "{}_time_end".format(resolution)
        subset = station_df[station_df[count_col] > 0].copy()
        if len(subset) == 0:
            continue
        subset["resolution"] = resolution
        subset["record_count"] = subset[count_col].astype(np.int64)
        subset["time_start"] = subset[start_col].fillna("").astype(str).str.strip()
        subset["time_end"] = subset[end_col].fillna("").astype(str).str.strip()
        rows.append(subset[RESOLUTION_CATALOG_COLUMNS])

    if rows:
        out = pd.concat(rows, ignore_index=True)
        order_map = {name: idx for idx, name in enumerate(CLUSTER_RESOLUTIONS)}
        out["_resolution_order"] = out["resolution"].map(order_map)
        out = out.sort_values(
            ["cluster_id", "_resolution_order", "cluster_uid"],
            kind="stable",
        ).drop(columns="_resolution_order")
    else:
        out = pd.DataFrame(columns=RESOLUTION_CATALOG_COLUMNS)

    return normalize_cluster_resolution_catalog(out)


def build_source_station_resolution_catalog(master_nc, chunk_size=500000):
    if not HAS_NC:
        raise RuntimeError("netCDF4 is required to build source-station catalogs")

    with nc4.Dataset(master_nc, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        n_source_stations = len(ds.dimensions["n_source_stations"])
        n_sources = len(ds.dimensions["n_sources"])
        n_records = len(ds.dimensions["n_records"])

        cluster_ids = _read_int_array(ds, "cluster_id", fill_value=-1, size=n_stations)
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        if not any(cluster_uids):
            cluster_uids = [
                "SED{:06d}".format(int(cid)) if cid >= 0 else ""
                for cid in cluster_ids
            ]

        source_names = _read_text_var(ds, "source_name", size=n_sources)
        source_long_names = _read_text_var(ds, "source_long_name", size=n_sources)
        institutions = _read_text_var(ds, "institution", size=n_sources)
        references = _read_text_var(ds, "reference", size=n_sources)
        source_urls = _read_text_var(ds, "source_url", size=n_sources)

        cluster_indices = _read_int_array(
            ds,
            "source_station_cluster_index",
            fill_value=-1,
            size=n_source_stations,
        )
        source_indices = _read_int_array(
            ds,
            "source_station_source_index",
            fill_value=-1,
            size=n_source_stations,
        )

        source_station_df = pd.DataFrame(
            {
                "source_station_index": np.arange(n_source_stations, dtype=np.int32),
                "source_station_uid": _read_text_var(ds, "source_station_uid", size=n_source_stations),
                "source_station_cluster_index": cluster_indices,
                "source_station_source_index": source_indices,
                "source_station_native_id": _read_text_var(
                    ds,
                    "source_station_native_id",
                    size=n_source_stations,
                ),
                "source_station_name": _read_text_var(ds, "source_station_name", size=n_source_stations),
                "source_station_river_name": _read_text_var(
                    ds,
                    "source_station_river_name",
                    size=n_source_stations,
                ),
                "source_station_lat": _read_float_array(
                    ds,
                    "source_station_lat",
                    fill_values=(-9999.0,),
                    size=n_source_stations,
                ),
                "source_station_lon": _read_float_array(
                    ds,
                    "source_station_lon",
                    fill_values=(-9999.0,),
                    size=n_source_stations,
                ),
                "source_station_paths": _read_text_var(ds, "source_station_paths", size=n_source_stations),
            }
        )

        source_station_df["cluster_uid"] = source_station_df["source_station_cluster_index"].map(
            lambda idx: cluster_uids[int(idx)] if 0 <= int(idx) < len(cluster_uids) else ""
        )
        source_station_df["cluster_id"] = source_station_df["source_station_cluster_index"].map(
            lambda idx: int(cluster_ids[int(idx)]) if 0 <= int(idx) < len(cluster_ids) else -1
        )
        source_station_df["source_name"] = source_station_df["source_station_source_index"].map(
            lambda idx: source_names[int(idx)] if 0 <= int(idx) < len(source_names) else ""
        )
        source_station_df["source_long_name"] = source_station_df["source_station_source_index"].map(
            lambda idx: source_long_names[int(idx)] if 0 <= int(idx) < len(source_long_names) else ""
        )
        source_station_df["institution"] = source_station_df["source_station_source_index"].map(
            lambda idx: institutions[int(idx)] if 0 <= int(idx) < len(institutions) else ""
        )
        source_station_df["reference"] = source_station_df["source_station_source_index"].map(
            lambda idx: references[int(idx)] if 0 <= int(idx) < len(references) else ""
        )
        source_station_df["source_url"] = source_station_df["source_station_source_index"].map(
            lambda idx: source_urls[int(idx)] if 0 <= int(idx) < len(source_urls) else ""
        )

        time_var = ds.variables["time"]
        time_units = getattr(time_var, "units", "days since 1970-01-01")
        time_calendar = getattr(time_var, "calendar", "gregorian")

        aggregated = {}
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            source_station_index = np.ma.asarray(ds.variables["source_station_index"][start:stop]).filled(-1)
            resolution_codes = np.ma.asarray(ds.variables["resolution"][start:stop]).filled(-1)
            time_values = np.ma.asarray(ds.variables["time"][start:stop]).filled(np.nan)

            frame = pd.DataFrame(
                {
                    "source_station_index": np.asarray(source_station_index, dtype=np.int64).reshape(-1),
                    "resolution_code": np.asarray(resolution_codes, dtype=np.int64).reshape(-1),
                    "time_num": np.asarray(time_values, dtype=np.float64).reshape(-1),
                }
            )
            frame = frame[frame["source_station_index"] >= 0].copy()
            if len(frame) == 0:
                continue
            frame["resolution"] = frame["resolution_code"].map(RESOLUTION_CODE_TO_NAME).fillna("")
            frame = frame[frame["resolution"].isin(CLUSTER_RESOLUTIONS) & np.isfinite(frame["time_num"])].copy()
            if len(frame) == 0:
                continue
            summary = (
                frame.groupby(["source_station_index", "resolution"], as_index=False)
                .agg(
                    n_records=("time_num", "size"),
                    time_min=("time_num", "min"),
                    time_max=("time_num", "max"),
                )
            )
            for row in summary.itertuples(index=False):
                key = (int(row.source_station_index), str(row.resolution))
                if key not in aggregated:
                    aggregated[key] = {
                        "n_records": 0,
                        "time_min": np.nan,
                        "time_max": np.nan,
                    }
                aggregated[key]["n_records"] += int(row.n_records)
                aggregated[key]["time_min"] = (
                    float(row.time_min)
                    if not np.isfinite(aggregated[key]["time_min"])
                    else min(float(aggregated[key]["time_min"]), float(row.time_min))
                )
                aggregated[key]["time_max"] = (
                    float(row.time_max)
                    if not np.isfinite(aggregated[key]["time_max"])
                    else max(float(aggregated[key]["time_max"]), float(row.time_max))
                )

    rows = []
    for (source_station_index, resolution), stats in aggregated.items():
        rows.append(
            {
                "source_station_index": int(source_station_index),
                "resolution": resolution,
                "n_records": int(stats["n_records"]),
                "time_min": float(stats["time_min"]),
                "time_max": float(stats["time_max"]),
            }
        )
    if not rows:
        return normalize_source_station_resolution_catalog(pd.DataFrame(columns=SOURCE_RESOLUTION_CATALOG_COLUMNS))

    stats_df = pd.DataFrame(rows)
    stats_df["time_start"] = _decode_time_numbers(
        stats_df["time_min"].values,
        time_units,
        time_calendar,
    )
    stats_df["time_end"] = _decode_time_numbers(
        stats_df["time_max"].values,
        time_units,
        time_calendar,
    )
    stats_df = stats_df.drop(columns=["time_min", "time_max"])

    out = stats_df.merge(source_station_df, on="source_station_index", how="left")
    order_map = {name: idx for idx, name in enumerate(CLUSTER_RESOLUTIONS)}
    out["_resolution_order"] = out["resolution"].map(order_map).fillna(999).astype(np.int64)
    out = out.sort_values(
        ["cluster_id", "_resolution_order", "source_station_uid", "source_station_index"],
        kind="stable",
    ).drop(columns="_resolution_order")
    return normalize_source_station_resolution_catalog(out)


def build_source_dataset_catalog(source_station_resolution_catalog):
    source_catalog = normalize_source_station_resolution_catalog(source_station_resolution_catalog)
    keep_cols = ["source_name", "source_long_name", "institution", "reference", "source_url"]
    source_df = source_catalog[keep_cols].drop_duplicates(subset=["source_name"]).copy()
    source_df = source_df.sort_values("source_name").reset_index(drop=True)
    counts = (
        source_catalog.groupby("source_name", observed=True)
        .agg(
            n_source_station_resolution_rows=("source_station_uid", "size"),
            n_source_stations=("source_station_uid", "nunique"),
            n_clusters=("cluster_uid", "nunique"),
            n_cluster_resolution_rows=("cluster_uid", "size"),
            available_resolutions=("resolution", lambda x: "|".join(sorted(set(str(v) for v in x if str(v).strip())))),
        )
        .reset_index()
    )
    return source_df.merge(counts, on="source_name", how="left")


def list_gpkg_layers(path):
    path = Path(path)
    if not path.is_file():
        return []
    with sqlite3.connect(str(path)) as conn:
        tables = pd.read_sql_query(
            "SELECT table_name FROM gpkg_contents WHERE data_type = 'features' ORDER BY table_name",
            conn,
        )
    return tables["table_name"].astype(str).tolist()


def read_gpkg_layer(path, layer):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG operations")
    return gpd.read_file(Path(path), layer=layer)


def _make_geometry(lat_values, lon_values):
    geometry = []
    for lat, lon in zip(lat_values, lon_values):
        if pd.notna(lat) and pd.notna(lon):
            geometry.append(Point(float(lon), float(lat)))
        else:
            geometry.append(None)
    return geometry


def _to_geodataframe(frame, keep_cols):
    data = frame.reindex(columns=keep_cols).copy()
    geometry = _make_geometry(data["lat"], data["lon"])
    return gpd.GeoDataFrame(data, geometry=geometry, crs="EPSG:4326")


def _write_multilayer_gpkg(layers, out_path):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG export")

    path = Path(out_path)
    if path.exists():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)

    for layer_name, gdf in layers.items():
        gdf.to_file(
            driver="GPKG",
            filename=path,
            layer=layer_name,
            encoding="UTF-8",
        )

    return path


def build_cluster_gpkg_layers(station_catalog, resolution_catalog):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for cluster GPKG export")

    station_df = normalize_cluster_station_catalog(station_catalog)
    resolution_df = normalize_cluster_resolution_catalog(resolution_catalog)

    layers = {
        "cluster_summary": _to_geodataframe(station_df, SUMMARY_LAYER_COLUMNS),
    }

    for resolution in CLUSTER_RESOLUTIONS:
        subset = resolution_df[resolution_df["resolution"] == resolution].copy()
        layers["cluster_{}".format(resolution)] = _to_geodataframe(
            subset,
            RESOLUTION_LAYER_COLUMNS,
        )

    return layers


def write_cluster_points_gpkg(station_catalog, resolution_catalog, out_path):
    layers = build_cluster_gpkg_layers(station_catalog, resolution_catalog)
    return _write_multilayer_gpkg(layers, out_path)


def build_source_station_gpkg_layers(source_station_resolution_catalog):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for source-station GPKG export")

    source_df = normalize_source_station_resolution_catalog(source_station_resolution_catalog).copy()
    source_df["station_name"] = source_df["source_station_name"]
    source_df["river_name"] = source_df["source_station_river_name"]
    source_df["lat"] = source_df["source_station_lat"]
    source_df["lon"] = source_df["source_station_lon"]

    layers = {}
    for resolution in CLUSTER_RESOLUTIONS:
        subset = source_df[source_df["resolution"] == resolution].copy()
        subset = subset[np.isfinite(subset["lat"]) & np.isfinite(subset["lon"])].copy()
        layers["source_{}".format(resolution)] = _to_geodataframe(subset, SOURCE_LAYER_COLUMNS)
    return layers


def write_source_stations_gpkg(source_station_resolution_catalog, out_path):
    layers = build_source_station_gpkg_layers(source_station_resolution_catalog)
    return _write_multilayer_gpkg(layers, out_path)


def build_cluster_basin_resolution_catalog(cluster_resolution_catalog, representatives, basin_gdf):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for basin GPKG export")

    resolution_df = normalize_cluster_resolution_catalog(cluster_resolution_catalog)
    reps = representatives.copy()
    if "cluster_id" in reps.columns:
        reps["cluster_id"] = pd.to_numeric(reps["cluster_id"], errors="coerce").astype("Int64")
        reps = reps.dropna(subset=["cluster_id"]).copy()
        reps["cluster_id"] = reps["cluster_id"].astype(int)
    if "station_id" in reps.columns:
        reps["station_id"] = pd.to_numeric(reps["station_id"], errors="coerce").astype("Int64")
        reps = reps.dropna(subset=["station_id"]).copy()
        reps["station_id"] = reps["station_id"].astype(int)

    basin_work = basin_gdf.copy()
    basin_work["station_id"] = pd.to_numeric(basin_work["station_id"], errors="coerce").astype("Int64")
    basin_work = basin_work.dropna(subset=["station_id"]).copy()
    basin_work["station_id"] = basin_work["station_id"].astype(int)

    reps = reps.rename(
        columns={
            "match_quality": "basin_match_quality",
            "n_rows": "n_station_rows",
            "n_src": "n_sources",
            "area_err": "area_error",
            "uparea": "uparea_merit",
        }
    )
    keep_rep_cols = [
        "cluster_id",
        "station_id",
        "basin_id",
        "area_error",
        "uparea_merit",
        "method",
        "n_station_rows",
        "n_sources",
        "basin_status",
        "basin_flag",
        "distance_m",
        "point_in_local",
        "point_in_basin",
        "station_name",
        "river_name",
        "source_station_id",
    ]
    for col in keep_rep_cols:
        if col not in reps.columns:
            reps[col] = ""

    merged = resolution_df.merge(reps[keep_rep_cols], on="cluster_id", how="left")
    if "basin_status_x" in merged.columns and "basin_status_y" in merged.columns:
        merged["basin_status"] = merged["basin_status_x"].where(
            merged["basin_status_x"].astype(str).str.strip() != "",
            merged["basin_status_y"],
        )
        merged = merged.drop(columns=["basin_status_x", "basin_status_y"])
    if "basin_flag_x" in merged.columns and "basin_flag_y" in merged.columns:
        merged["basin_flag"] = merged["basin_flag_x"].where(
            merged["basin_flag_x"].astype(str).str.strip() != "",
            merged["basin_flag_y"],
        )
        merged = merged.drop(columns=["basin_flag_x", "basin_flag_y"])
    if "station_name_x" in merged.columns and "station_name_y" in merged.columns:
        merged["station_name"] = merged["station_name_x"].where(
            merged["station_name_x"].astype(str).str.strip() != "",
            merged["station_name_y"],
        )
        merged = merged.drop(columns=["station_name_x", "station_name_y"])
    if "river_name_x" in merged.columns and "river_name_y" in merged.columns:
        merged["river_name"] = merged["river_name_x"].where(
            merged["river_name_x"].astype(str).str.strip() != "",
            merged["river_name_y"],
        )
        merged = merged.drop(columns=["river_name_x", "river_name_y"])
    if "source_station_id_x" in merged.columns and "source_station_id_y" in merged.columns:
        merged["source_station_id"] = merged["source_station_id_x"].where(
            merged["source_station_id_x"].astype(str).str.strip() != "",
            merged["source_station_id_y"],
        )
        merged = merged.drop(columns=["source_station_id_x", "source_station_id_y"])
    if "point_in_local_x" in merged.columns and "point_in_local_y" in merged.columns:
        merged["point_in_local"] = pd.to_numeric(
            merged["point_in_local_x"].where(
                pd.notna(merged["point_in_local_x"]),
                merged["point_in_local_y"],
            ),
            errors="coerce",
        )
        merged = merged.drop(columns=["point_in_local_x", "point_in_local_y"])
    if "point_in_basin_x" in merged.columns and "point_in_basin_y" in merged.columns:
        merged["point_in_basin"] = pd.to_numeric(
            merged["point_in_basin_x"].where(
                pd.notna(merged["point_in_basin_x"]),
                merged["point_in_basin_y"],
            ),
            errors="coerce",
        )
        merged = merged.drop(columns=["point_in_basin_x", "point_in_basin_y"])
    if "basin_distance_m" not in merged.columns and "distance_m" in merged.columns:
        merged["basin_distance_m"] = pd.to_numeric(merged["distance_m"], errors="coerce")
    elif "distance_m" in merged.columns:
        merged["basin_distance_m"] = pd.to_numeric(
            merged["basin_distance_m"].where(pd.notna(merged["basin_distance_m"]), merged["distance_m"]),
            errors="coerce",
        )
    if "point_in_local" in merged.columns:
        merged["point_in_local"] = pd.to_numeric(merged["point_in_local"], errors="coerce").fillna(0).astype(np.int64)
    if "point_in_basin" in merged.columns:
        merged["point_in_basin"] = pd.to_numeric(merged["point_in_basin"], errors="coerce").fillna(0).astype(np.int64)
    if "basin_status" in merged.columns:
        merged = merged[
            merged["basin_status"].fillna("").astype(str).str.strip().str.lower().eq("resolved")
        ].copy()
    merged = merged.merge(
        basin_work[["station_id", "geometry"]],
        on="station_id",
        how="left",
    )
    merged = merged[merged["geometry"].notna()].copy()

    if "n_source_stations_in_cluster" not in merged.columns:
        merged["n_source_stations_in_cluster"] = pd.to_numeric(
            merged.get("n_sources", 0),
            errors="coerce",
        ).fillna(0).astype(np.int64)

    out = gpd.GeoDataFrame(
        merged[BASIN_RESOLUTION_CATALOG_COLUMNS + ["geometry"]].copy(),
        geometry="geometry",
        crs=basin_work.crs or "EPSG:4326",
    )
    order_map = {name: idx for idx, name in enumerate(CLUSTER_RESOLUTIONS)}
    out["_resolution_order"] = out["resolution"].map(order_map).fillna(999).astype(np.int64)
    out = out.sort_values(
        ["cluster_id", "_resolution_order", "cluster_uid"],
        kind="stable",
    ).drop(columns="_resolution_order")
    return normalize_cluster_basin_resolution_catalog(out)


def build_cluster_basin_gpkg_layers(cluster_basin_resolution_catalog, layer_prefix="basin"):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for basin GPKG export")

    basin_gdf = normalize_cluster_basin_resolution_catalog(cluster_basin_resolution_catalog)
    layers = {}
    for resolution in CLUSTER_RESOLUTIONS:
        subset = basin_gdf[basin_gdf["resolution"] == resolution].copy()
        layers["{}_{}".format(layer_prefix, resolution)] = subset[BASIN_LAYER_COLUMNS + ["geometry"]].copy()
    return layers


def write_cluster_basins_gpkg(cluster_basin_resolution_catalog, out_path, layer_prefix="basin"):
    layers = build_cluster_basin_gpkg_layers(cluster_basin_resolution_catalog, layer_prefix=layer_prefix)
    return _write_multilayer_gpkg(layers, out_path)
