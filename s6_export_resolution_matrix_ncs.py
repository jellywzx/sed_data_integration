#!/usr/bin/env python3
"""
Export basin-merged time series as one NetCDF per resolution, each using a
station x time matrix layout.

Input:
  - scripts_basin_test/output/s5_basin_clustered_stations.csv

Default outputs:
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_daily.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_monthly.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_annual.nc

Design:
  - reuse the current s6 quality-ranking merge rule inside each
    (cluster_id, resolution);
  - write one file per resolution;
  - store Q / SSC / SSL and flags as 2D (n_stations, n_time) matrices;
  - preserve cluster-level basin metadata and a compact source lookup table;
  - expose `selected_source_station_uid` so each matrix cell can be traced back
    to one source station without opening master.nc.
"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

from basin_policy import (
    MATCH_QUALITY_CODES,
    MATCH_QUALITY_MEANINGS,
)
from pipeline_paths import (
    S2_ORGANIZED_DIR,
    S5_BASIN_CLUSTERED_CSV,
    S6_MATRIX_DIR,
    get_output_r_root,
)
from s6_basin_merge_to_nc import (
    FILL,
    HAS_NC,
    RESOLUTION_CODES,
    STANDARD_QC_STAGE_NAMES,
    _build_source_station_key,
    _summarize_unit_issues,
    _read_source_meta_from_nc,
    _read_station_meta_from_nc,
    append_stage_qc_variables,
    build_cluster_series,
)

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_INPUT = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUT_DIR = PROJECT_ROOT / S6_MATRIX_DIR
DEFAULT_RESOLUTIONS = ("daily", "monthly", "annual")


def _normalize_resolution(value):
    text = str(value or "").strip().lower()
    if text == "quarterly":
        return "monthly"
    if text == "single_point":
        return "daily"
    if text == "annually_climatology":
        return "climatology"
    return text


def _resolve_station_path(path_value, organized_root):
    path = Path(path_value)
    if not path.is_absolute():
        return str(organized_root / path)
    if path.is_file():
        return str(path)
    try:
        parts = path.resolve().parts
        marker = "output_resolution_organized"
        for i, part in enumerate(parts):
            if part == marker and i + 1 < len(parts):
                rel = Path(*parts[i + 1 :])
                candidate = organized_root / rel
                if candidate.is_file():
                    return str(candidate)
    except Exception:
        pass
    return str(path)


def _clean_bool(value):
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "t"}


def _build_cluster_rep_lookup(stations):
    rep = (
        stations[stations["station_id"] == stations["cluster_id"]]
        .drop_duplicates(subset=["cluster_id"])
        .set_index("cluster_id")
    )
    return rep


def _worker_build_cluster(args):
    cid, resolution, recs = args
    return cid, resolution, build_cluster_series(cid, resolution, recs)


def _build_source_lookup(resolution_df):
    unique_sources = sorted(resolution_df["source"].astype(str).unique().tolist())
    source_to_idx = {name: i for i, name in enumerate(unique_sources)}

    long_names = [""] * len(unique_sources)
    institutions = [""] * len(unique_sources)
    references = [""] * len(unique_sources)
    urls = [""] * len(unique_sources)

    for source_name, sidx in source_to_idx.items():
        rep_path = str(resolution_df.loc[resolution_df["source"] == source_name, "path"].iloc[0])
        meta = _read_source_meta_from_nc(rep_path)
        long_names[sidx] = meta[0] or source_name
        institutions[sidx] = meta[1]
        references[sidx] = meta[2]
        urls[sidx] = meta[3]

    return {
        "names": unique_sources,
        "to_idx": source_to_idx,
        "long_names": long_names,
        "institutions": institutions,
        "references": references,
        "urls": urls,
    }


def _build_station_metadata(cluster_ids, cluster_rep_lookup, resolution_df):
    n_stations = len(cluster_ids)
    cluster_to_idx = {cid: i for i, cid in enumerate(cluster_ids)}

    lat_arr = np.full(n_stations, FILL, dtype=np.float32)
    lon_arr = np.full(n_stations, FILL, dtype=np.float32)
    cluster_uid_arr = [""] * n_stations
    station_name_arr = [""] * n_stations
    river_name_arr = [""] * n_stations
    source_station_id_arr = [""] * n_stations
    basin_area_arr = np.full(n_stations, FILL, dtype=np.float32)
    pfaf_code_arr = np.full(n_stations, FILL, dtype=np.float32)
    n_reaches_arr = np.full(n_stations, -9999, dtype=np.int32)
    match_quality_arr = np.full(n_stations, -1, dtype=np.int8)
    basin_distance_arr = np.full(n_stations, FILL, dtype=np.float32)
    point_in_local_arr = np.zeros(n_stations, dtype=np.int8)
    point_in_basin_arr = np.zeros(n_stations, dtype=np.int8)
    basin_status_arr = ["unknown"] * n_stations
    basin_flag_arr = ["unknown"] * n_stations
    sources_used_arr = [""] * n_stations
    n_sources_arr = np.zeros(n_stations, dtype=np.int32)

    station_meta_cache = {}

    for cid, idx in cluster_to_idx.items():
        group = resolution_df[resolution_df["cluster_id"] == cid]
        if cid in cluster_rep_lookup.index:
            rep_row = cluster_rep_lookup.loc[cid]
        else:
            rep_row = group.iloc[0]

        rep_path = str(rep_row["path"])
        if rep_path not in station_meta_cache:
            station_meta_cache[rep_path] = _read_station_meta_from_nc(rep_path)
        station_name, river_name, native_id = station_meta_cache[rep_path]

        cluster_uid_arr[idx] = "SED{:06d}".format(int(cid))
        station_name_arr[idx] = station_name
        river_name_arr[idx] = river_name
        source_station_id_arr[idx] = native_id

        lat_val = rep_row.get("lat", np.nan)
        lon_val = rep_row.get("lon", np.nan)
        if pd.notna(lat_val):
            lat_arr[idx] = float(lat_val)
        else:
            mean_lat = group["lat"].dropna()
            if len(mean_lat):
                lat_arr[idx] = float(mean_lat.mean())
        if pd.notna(lon_val):
            lon_arr[idx] = float(lon_val)
        else:
            mean_lon = group["lon"].dropna()
            if len(mean_lon):
                lon_arr[idx] = float(mean_lon.mean())

        basin_area = rep_row.get("basin_area", np.nan)
        pfaf_code = rep_row.get("pfaf_code", np.nan)
        n_reaches = rep_row.get("n_upstream_reaches", np.nan)
        match_quality = str(rep_row.get("match_quality", "unknown") or "unknown").strip()
        basin_distance = rep_row.get("distance_m", np.nan)
        basin_status = str(rep_row.get("basin_status", "unknown") or "unknown").strip()
        basin_flag = str(rep_row.get("basin_flag", "unknown") or "unknown").strip()

        if pd.notna(basin_area):
            basin_area_arr[idx] = float(basin_area)
        if pd.notna(pfaf_code):
            pfaf_code_arr[idx] = float(pfaf_code)
        if pd.notna(n_reaches):
            n_reaches_arr[idx] = int(n_reaches)
        match_quality_arr[idx] = np.int8(MATCH_QUALITY_CODES.get(match_quality, -1))
        if pd.notna(basin_distance):
            basin_distance_arr[idx] = float(basin_distance)
        point_in_local_arr[idx] = np.int8(1 if _clean_bool(rep_row.get("point_in_local", False)) else 0)
        point_in_basin_arr[idx] = np.int8(1 if _clean_bool(rep_row.get("point_in_basin", False)) else 0)
        basin_status_arr[idx] = basin_status
        basin_flag_arr[idx] = basin_flag

        sources = sorted(group["source"].astype(str).unique().tolist())
        sources_used_arr[idx] = "|".join(sources)
        n_sources_arr[idx] = len(sources)

    return {
        "cluster_to_idx": cluster_to_idx,
        "lat": lat_arr,
        "lon": lon_arr,
        "cluster_uid": cluster_uid_arr,
        "station_name": station_name_arr,
        "river_name": river_name_arr,
        "source_station_id": source_station_id_arr,
        "basin_area": basin_area_arr,
        "pfaf_code": pfaf_code_arr,
        "n_upstream_reaches": n_reaches_arr,
        "basin_match_quality": match_quality_arr,
        "basin_distance_m": basin_distance_arr,
        "point_in_local": point_in_local_arr,
        "point_in_basin": point_in_basin_arr,
        "basin_status": basin_status_arr,
        "basin_flag": basin_flag_arr,
        "sources_used": sources_used_arr,
        "n_sources_in_resolution": n_sources_arr,
    }


def _build_source_station_uid_lookup(stations_all):
    work = stations_all.copy()
    for col in ("station_name", "river_name", "source_station_id"):
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].fillna("").astype(str)

    work = work[work["resolution_norm"].astype(str).str.strip().ne("climatology")].copy()
    key_to_index = {}
    row_to_source_station_index = {}

    for row in work.itertuples(index=False):
        row_dict = {
            "path": getattr(row, "path", ""),
            "source": getattr(row, "source", ""),
            "cluster_id": getattr(row, "cluster_id", -1),
            "lat": getattr(row, "lat", np.nan),
            "lon": getattr(row, "lon", np.nan),
            "resolution": getattr(row, "resolution", "other"),
            "station_name": getattr(row, "station_name", ""),
            "river_name": getattr(row, "river_name", ""),
            "source_station_id": getattr(row, "source_station_id", ""),
        }
        key = _build_source_station_key(row_dict)
        source_station_index = key_to_index.get(key)
        if source_station_index is None:
            source_station_index = len(key_to_index)
            key_to_index[key] = source_station_index
        row_to_source_station_index[int(getattr(row, "_input_row_index"))] = int(source_station_index)

    index_to_uid = {
        int(source_station_index): "SRC{:06d}".format(int(source_station_index))
        for source_station_index in range(len(key_to_index))
    }
    return row_to_source_station_index, index_to_uid


def _write_matrix_nc(out_path, resolution, cluster_ids, metadata, source_lookup, series_results, source_station_uid_lookup):
    all_dates = sorted(
        {
            pd.Timestamp(date).date()
            for result in series_results.values()
            for date in result[0]
        }
    )
    if not all_dates:
        raise ValueError("No dates available for resolution {}".format(resolution))

    n_stations = len(cluster_ids)
    n_time = len(all_dates)
    ref = pd.Timestamp("1970-01-01")
    time_values = (
        (pd.to_datetime(all_dates) - ref).total_seconds().values / 86400.0
    ).astype(np.float64)
    date_to_idx = {date: i for i, date in enumerate(all_dates)}
    chunks = (1, min(512, n_time))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with nc4.Dataset(out_path, "w", format="NETCDF4") as nc:
        nc.createDimension("n_stations", n_stations)
        nc.createDimension("time", n_time)
        nc.createDimension("n_sources", len(source_lookup["names"]))

        lat_v = nc.createVariable("lat", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        lat_v.long_name = "latitude of basin cluster representative"
        lat_v.units = "degrees_north"
        lat_v[:] = metadata["lat"]

        lon_v = nc.createVariable("lon", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        lon_v.long_name = "longitude of basin cluster representative"
        lon_v.units = "degrees_east"
        lon_v[:] = metadata["lon"]

        cid_v = nc.createVariable("cluster_id", "i4", ("n_stations",))
        cid_v.long_name = "original cluster_id from s5_basin_clustered_stations"
        cid_v[:] = np.asarray(cluster_ids, dtype=np.int32)

        uid_v = nc.createVariable("cluster_uid", str, ("n_stations",))
        uid_v.long_name = "stable cluster identifier"
        uid_v[:] = np.asarray(metadata["cluster_uid"], dtype=object)

        stn_v = nc.createVariable("station_name", str, ("n_stations",))
        stn_v.long_name = "station name from representative source file"
        stn_v[:] = np.asarray(metadata["station_name"], dtype=object)

        riv_v = nc.createVariable("river_name", str, ("n_stations",))
        riv_v.long_name = "river name from representative source file"
        riv_v[:] = np.asarray(metadata["river_name"], dtype=object)

        sid_v = nc.createVariable("source_station_id", str, ("n_stations",))
        sid_v.long_name = "native station identifier from representative source file"
        sid_v[:] = np.asarray(metadata["source_station_id"], dtype=object)

        srcs_v = nc.createVariable("sources_used", str, ("n_stations",))
        srcs_v.long_name = "pipe-separated list of sources contributing to this resolution for the cluster"
        srcs_v[:] = np.asarray(metadata["sources_used"], dtype=object)

        nsrc_v = nc.createVariable("n_sources_in_resolution", "i4", ("n_stations",))
        nsrc_v.long_name = "number of source datasets contributing to this resolution for the cluster"
        nsrc_v[:] = metadata["n_sources_in_resolution"]

        ba_v = nc.createVariable("basin_area", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        ba_v.long_name = "basin drainage area"
        ba_v.units = "km2"
        ba_v[:] = metadata["basin_area"]

        pf_v = nc.createVariable("pfaf_code", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        pf_v.long_name = "Pfafstetter basin code"
        pf_v[:] = metadata["pfaf_code"]

        nr_v = nc.createVariable("n_upstream_reaches", "i4", ("n_stations",), fill_value=-9999, zlib=True, complevel=4)
        nr_v.long_name = "number of upstream river reaches"
        nr_v[:] = metadata["n_upstream_reaches"]

        mq_v = nc.createVariable("basin_match_quality", "i1", ("n_stations",), fill_value=np.int8(-1), zlib=True, complevel=4)
        mq_v.long_name = "basin matching quality from basin tracer"
        mq_v.flag_values = np.array([0, 1, 2, 3, 4, -1], dtype=np.int8)
        mq_v.flag_meanings = MATCH_QUALITY_MEANINGS
        mq_v[:] = metadata["basin_match_quality"]

        bd_v = nc.createVariable("basin_distance_m", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        bd_v.long_name = "distance from original station point to matched reach"
        bd_v.units = "m"
        bd_v[:] = metadata["basin_distance_m"]

        pil_v = nc.createVariable("point_in_local", "i1", ("n_stations",), fill_value=np.int8(0), zlib=True, complevel=4)
        pil_v.long_name = "whether the original station point is covered by the matched local catchment"
        pil_v.flag_values = np.array([0, 1], dtype=np.int8)
        pil_v.flag_meanings = "false true"
        pil_v[:] = metadata["point_in_local"]

        pib_v = nc.createVariable("point_in_basin", "i1", ("n_stations",), fill_value=np.int8(0), zlib=True, complevel=4)
        pib_v.long_name = "whether the original station point is covered by the traced upstream basin"
        pib_v.flag_values = np.array([0, 1], dtype=np.int8)
        pib_v.flag_meanings = "false true"
        pib_v[:] = metadata["point_in_basin"]

        bs_v = nc.createVariable("basin_status", str, ("n_stations",))
        bs_v.long_name = "release-facing basin assignment status"
        bs_v[:] = np.asarray(metadata["basin_status"], dtype=object)

        bf_v = nc.createVariable("basin_flag", str, ("n_stations",))
        bf_v.long_name = "release-facing basin status reason flag"
        bf_v[:] = np.asarray(metadata["basin_flag"], dtype=object)

        time_v = nc.createVariable("time", "f8", ("time",), zlib=True, complevel=4)
        time_v.long_name = "time"
        time_v.units = "days since 1970-01-01"
        time_v.calendar = "gregorian"
        time_v[:] = time_values

        sname_v = nc.createVariable("source_name", str, ("n_sources",))
        sname_v.long_name = "short dataset identifier"
        sname_v[:] = np.asarray(source_lookup["names"], dtype=object)

        slong_v = nc.createVariable("source_long_name", str, ("n_sources",))
        slong_v.long_name = "full dataset name"
        slong_v[:] = np.asarray(source_lookup["long_names"], dtype=object)

        inst_v = nc.createVariable("institution", str, ("n_sources",))
        inst_v.long_name = "data provider institution"
        inst_v[:] = np.asarray(source_lookup["institutions"], dtype=object)

        ref_v = nc.createVariable("reference", str, ("n_sources",))
        ref_v.long_name = "reference string(s) for source dataset"
        ref_v[:] = np.asarray(source_lookup["references"], dtype=object)

        url_v = nc.createVariable("source_url", str, ("n_sources",))
        url_v.long_name = "data access URL or DOI"
        url_v[:] = np.asarray(source_lookup["urls"], dtype=object)

        q_v = nc.createVariable("Q", "f4", ("n_stations", "time"), fill_value=FILL, zlib=True, complevel=4, chunksizes=chunks)
        q_v.long_name = "river discharge"
        q_v.units = "m3 s-1"

        ssc_v = nc.createVariable("SSC", "f4", ("n_stations", "time"), fill_value=FILL, zlib=True, complevel=4, chunksizes=chunks)
        ssc_v.long_name = "suspended sediment concentration"
        ssc_v.units = "mg L-1"

        ssl_v = nc.createVariable("SSL", "f4", ("n_stations", "time"), fill_value=FILL, zlib=True, complevel=4, chunksizes=chunks)
        ssl_v.long_name = "suspended sediment load"
        ssl_v.units = "ton day-1"

        flag_kw = dict(flag_values=np.array([0, 1, 2, 3, 9], dtype=np.int8), flag_meanings="good estimated suspect bad missing")

        qf_v = nc.createVariable("Q_flag", "i1", ("n_stations", "time"), fill_value=np.int8(9), zlib=True, complevel=4, chunksizes=chunks)
        qf_v.long_name = "quality flag for river discharge"
        for key, value in flag_kw.items():
            setattr(qf_v, key, value)

        sscf_v = nc.createVariable("SSC_flag", "i1", ("n_stations", "time"), fill_value=np.int8(9), zlib=True, complevel=4, chunksizes=chunks)
        sscf_v.long_name = "quality flag for suspended sediment concentration"
        for key, value in flag_kw.items():
            setattr(sscf_v, key, value)

        sslf_v = nc.createVariable("SSL_flag", "i1", ("n_stations", "time"), fill_value=np.int8(9), zlib=True, complevel=4, chunksizes=chunks)
        sslf_v.long_name = "quality flag for suspended sediment load"
        for key, value in flag_kw.items():
            setattr(sslf_v, key, value)

        stage_qc_vars = append_stage_qc_variables(
            nc,
            ("n_stations", "time"),
            chunksizes=chunks,
            zlib=True,
            complevel=4,
        )
        ov_v = nc.createVariable("is_overlap", "i1", ("n_stations", "time"), fill_value=np.int8(0), zlib=True, complevel=4, chunksizes=chunks)
        ov_v.long_name = "whether multiple source files competed for this station-time cell"
        ov_v.flag_values = np.array([0, 1], dtype=np.int8)
        ov_v.flag_meanings = "single_source quality_score_selection_applied"

        src_v = nc.createVariable("selected_source_index", "i4", ("n_stations", "time"), fill_value=-1, zlib=True, complevel=4, chunksizes=chunks)
        src_v.long_name = "0-based index into n_sources dimension for the chosen source"

        ssuid_v = nc.createVariable("selected_source_station_uid", str, ("n_stations", "time"))
        ssuid_v.long_name = "stable source-station identifier for the chosen record"
        ssuid_v.comment = (
            "format SRC######; cell-level provenance key for the selected source station. "
            "Allows matrix-only users to trace each exported value back to one source station "
            "without consulting master.nc."
        )

        count_v = nc.createVariable("n_valid_time_steps", "i4", ("n_stations",), zlib=True, complevel=4)
        count_v.long_name = "number of time steps with at least one non-missing value"
        valid_counts = np.zeros(n_stations, dtype=np.int32)

        for station_idx, cid in enumerate(cluster_ids):
            result = series_results[cid]
            (
                dates_arr,
                q_arr,
                ssc_arr,
                ssl_arr,
                q_flag_arr,
                ssc_flag_arr,
                ssl_flag_arr,
                stage_qc_arrs,
                is_overlap_arr,
                source_arr,
                source_station_idx_arr,
            ) = result

            q_row = np.full(n_time, FILL, dtype=np.float32)
            ssc_row = np.full(n_time, FILL, dtype=np.float32)
            ssl_row = np.full(n_time, FILL, dtype=np.float32)
            qf_row = np.full(n_time, 9, dtype=np.int8)
            sscf_row = np.full(n_time, 9, dtype=np.int8)
            sslf_row = np.full(n_time, 9, dtype=np.int8)
            stage_qc_rows = dict(
                (field_name, np.full(n_time, 9, dtype=np.int8))
                for field_name in STANDARD_QC_STAGE_NAMES
            )
            ov_row = np.zeros(n_time, dtype=np.int8)
            src_row = np.full(n_time, -1, dtype=np.int32)
            ssuid_row = np.full(n_time, "", dtype=object)

            for i, date in enumerate(pd.to_datetime(dates_arr).date):
                col = date_to_idx[date]
                q_row[col] = q_arr[i]
                ssc_row[col] = ssc_arr[i]
                ssl_row[col] = ssl_arr[i]
                qf_row[col] = q_flag_arr[i]
                sscf_row[col] = ssc_flag_arr[i]
                sslf_row[col] = ssl_flag_arr[i]
                for field_name in STANDARD_QC_STAGE_NAMES:
                    stage_qc_rows[field_name][col] = stage_qc_arrs[field_name][i]
                ov_row[col] = is_overlap_arr[i]
                source_name = str(source_arr[i] or "")
                src_row[col] = source_lookup["to_idx"].get(source_name, -1)
                source_station_index = int(source_station_idx_arr[i]) if np.isfinite(source_station_idx_arr[i]) else -1
                if source_station_index >= 0:
                    ssuid_row[col] = source_station_uid_lookup.get(source_station_index, "")

            valid_counts[station_idx] = int(
                np.count_nonzero((q_row != FILL) | (ssc_row != FILL) | (ssl_row != FILL))
            )
            q_v[station_idx, :] = q_row
            ssc_v[station_idx, :] = ssc_row
            ssl_v[station_idx, :] = ssl_row
            qf_v[station_idx, :] = qf_row
            sscf_v[station_idx, :] = sscf_row
            sslf_v[station_idx, :] = sslf_row
            for field_name in STANDARD_QC_STAGE_NAMES:
                stage_qc_vars[field_name][station_idx, :] = stage_qc_rows[field_name]
            ov_v[station_idx, :] = ov_row
            src_v[station_idx, :] = src_row
            ssuid_v[station_idx, :] = np.asarray(ssuid_row, dtype=object)

        count_v[:] = valid_counts

        nc.title = "Global river suspended sediment dataset ({}) station-time matrix".format(resolution)
        nc.Conventions = "CF-1.8"
        nc.source = (
            "Exported from s5_basin_clustered_stations.csv using the same per-cluster "
            "quality-ranking merge rule as s6_basin_merge_to_nc.py, but written as a "
            "station x time matrix"
        )
        nc.history = "Created {} by s6_export_resolution_matrix_ncs.py".format(
            datetime.now().isoformat(timespec="seconds")
        )
        nc.time_type = resolution
        nc.time_type_code = str(RESOLUTION_CODES.get(resolution, RESOLUTION_CODES["other"]))
        nc.matrix_layout = "rows are stations (clusters), columns are time steps shared within one resolution"
        nc.provenance_key = "selected_source_station_uid"
        nc.classification_policy = "manual_review_on_conflict"
        nc.qc_stage_schema_version = "1"
        nc.n_clusters = str(n_stations)
        nc.n_time_steps = str(n_time)

        nc.sync()


def _collect_resolution_series(resolution, resolution_df, workers):
    tasks = []
    for cid, group in resolution_df.groupby("cluster_id", sort=True):
        recs = list(zip(group["source"], group["path"], group["source_station_global_index"]))
        tasks.append((int(cid), resolution, recs))

    series_results = {}
    quality_messages = 0
    unit_issue_rows = []
    use_parallel = workers > 1 and len(tasks) > 1

    if use_parallel:
        with ProcessPoolExecutor(max_workers=min(workers, len(tasks))) as ex:
            futures = {ex.submit(_worker_build_cluster, task): task for task in tasks}
            with tqdm(total=len(tasks), desc="{} series".format(resolution), unit="cluster") as pbar:
                for future in as_completed(futures):
                    cid, _, result = future.result()
                    if result is not None:
                        if len(result) == 2 and result[0] is None:
                            unit_issue_rows.extend(result[1])
                            pbar.update(1)
                            continue
                        (
                            dates_arr,
                            q_arr,
                            ssc_arr,
                            ssl_arr,
                            q_flag_arr,
                            ssc_flag_arr,
                            ssl_flag_arr,
                            stage_qc_arrs,
                            is_overlap_arr,
                            source_arr,
                            source_station_idx_arr,
                            quality_rows,
                            quality_log,
                            unit_issues,
                        ) = result
                        series_results[cid] = (
                            dates_arr,
                            q_arr,
                            ssc_arr,
                            ssl_arr,
                            q_flag_arr,
                            ssc_flag_arr,
                            ssl_flag_arr,
                            stage_qc_arrs,
                            is_overlap_arr,
                            source_arr,
                            source_station_idx_arr,
                        )
                        if unit_issues:
                            unit_issue_rows.extend(unit_issues)
                        if quality_log:
                            quality_messages += 1
                            tqdm.write(quality_log)
                    pbar.update(1)
    else:
        with tqdm(total=len(tasks), desc="{} series".format(resolution), unit="cluster") as pbar:
            for task in tasks:
                cid, _, result = _worker_build_cluster(task)
                if result is not None:
                    if len(result) == 2 and result[0] is None:
                        unit_issue_rows.extend(result[1])
                        pbar.update(1)
                        continue
                    (
                        dates_arr,
                        q_arr,
                        ssc_arr,
                        ssl_arr,
                        q_flag_arr,
                        ssc_flag_arr,
                        ssl_flag_arr,
                        stage_qc_arrs,
                        is_overlap_arr,
                        source_arr,
                        source_station_idx_arr,
                        quality_rows,
                        quality_log,
                        unit_issues,
                    ) = result
                    series_results[cid] = (
                        dates_arr,
                        q_arr,
                        ssc_arr,
                        ssl_arr,
                        q_flag_arr,
                        ssc_flag_arr,
                        ssl_flag_arr,
                        stage_qc_arrs,
                        is_overlap_arr,
                        source_arr,
                        source_station_idx_arr,
                    )
                    if unit_issues:
                        unit_issue_rows.extend(unit_issues)
                    if quality_log:
                        quality_messages += 1
                        tqdm.write(quality_log)
                pbar.update(1)

    return series_results, quality_messages, unit_issue_rows


def main():
    ap = argparse.ArgumentParser(description="Export one station x time NC per basin resolution")
    ap.add_argument("--input", "-i", default=str(DEFAULT_INPUT), help="input s5 clustered csv")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="output directory for matrix nc files")
    ap.add_argument(
        "--resolutions",
        nargs="+",
        default=list(DEFAULT_RESOLUTIONS),
        help="resolution names to export separately; default: daily monthly annual",
    )
    ap.add_argument(
        "--workers",
        "-w",
        type=int,
        default=8,
        help="parallel workers for per-cluster series merge; default: 8",
    )
    args = ap.parse_args()

    if not HAS_NC or nc4 is None:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1

    inp_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    resolutions = [_normalize_resolution(res) for res in args.resolutions]
    resolutions = [res for res in resolutions if res]
    resolutions = list(dict.fromkeys(resolutions))

    if not inp_path.is_file():
        print("Error: input not found: {}".format(inp_path))
        return 1

    organized_root = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()
    stations_all = pd.read_csv(inp_path)
    for col in ["path", "source", "lat", "lon", "cluster_id", "station_id", "resolution"]:
        if col not in stations_all.columns:
            print("Error: s5 CSV missing required column '{}'".format(col))
            return 1

    stations_all = stations_all.copy()
    stations_all["_input_row_index"] = np.arange(len(stations_all), dtype=np.int32)
    stations_all["resolution_norm"] = stations_all["resolution"].map(_normalize_resolution)
    stations_all["path"] = stations_all["path"].apply(lambda p: _resolve_station_path(p, organized_root))
    row_to_source_station_index, source_station_uid_lookup = _build_source_station_uid_lookup(stations_all)

    stations = stations_all.copy()
    stations["resolution_norm"] = stations["resolution"].map(_normalize_resolution)
    stations = stations[stations["resolution_norm"].isin(resolutions)].copy()
    stations = stations[stations["path"].apply(lambda p: Path(p).is_file())].copy()

    if len(stations) == 0:
        print("Error: no valid rows remain after resolution/path filtering.")
        return 1

    stations["source_station_global_index"] = stations["_input_row_index"].map(
        lambda idx: row_to_source_station_index.get(int(idx), -1)
    ).astype(np.int32)
    cluster_rep_lookup = _build_cluster_rep_lookup(stations_all)

    print("Input rows after filtering: {}".format(len(stations)))
    print("Requested resolutions: {}".format(", ".join(resolutions)))
    print("Output dir: {}".format(out_dir))

    for resolution in resolutions:
        resolution_df = stations[stations["resolution_norm"] == resolution].copy()
        if len(resolution_df) == 0:
            print("Skip {}: no rows found".format(resolution))
            continue

        print("\n[{}] rows={} clusters={}".format(
            resolution,
            len(resolution_df),
            resolution_df["cluster_id"].nunique(),
        ))
        source_lookup = _build_source_lookup(resolution_df)
        series_results, quality_messages, unit_issue_rows = _collect_resolution_series(
            resolution, resolution_df, max(1, int(args.workers))
        )
        _summarize_unit_issues(
            unit_issue_rows,
            strict_mode=False,
            label="{} matrix input unit validation".format(resolution),
        )
        if not series_results:
            print("Skip {}: no non-empty merged series".format(resolution))
            continue

        cluster_ids = sorted(series_results.keys())
        metadata = _build_station_metadata(cluster_ids, cluster_rep_lookup, resolution_df)
        out_path = out_dir / "s6_basin_matrix_{}.nc".format(resolution)
        _write_matrix_nc(
            out_path,
            resolution,
            cluster_ids,
            metadata,
            source_lookup,
            series_results,
            source_station_uid_lookup,
        )
        total_points = sum(len(result[0]) for result in series_results.values())
        print(
            "Wrote {} | stations={} | time_steps={} | merged_points={} | quality_logs={}".format(
                out_path,
                len(cluster_ids),
                len(
                    {
                        pd.Timestamp(date).date()
                        for result in series_results.values()
                        for date in result[0]
                    }
                ),
                total_points,
                quality_messages,
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
