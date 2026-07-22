#!/usr/bin/env python3
"""Link satellite locations to hydrologically constrained main clusters.

The linkage is computed once per satellite location and resolution. Satellite
records are never scanned here, and satellite singleton cluster identifiers are
preserved in the output.
"""

import argparse
import hashlib
import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from basin_policy import (
    SOURCE_SPATIAL_SUPPORT_POINT_ANCHORED_REACH,
    classify_source_spatial_support,
)
from pipeline_paths import (
    S5_BASIN_CLUSTERED_CSV,
    S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV,
    S6_MATRIX_DIR,
    get_output_r_root,
)
from s6_basin_merge_to_nc import classify_source_family_from_observation_type

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - exercised by the CLI environment check
    nc4 = None

try:
    import pyogrio
except ImportError:  # pragma: no cover - optional until Dethier linkage uses MERIT files
    pyogrio = None

try:
    from shapely.geometry import Point
except ImportError:  # pragma: no cover - optional until Dethier linkage uses MERIT geometry
    Point = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_S5_CSV = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUTPUT = PROJECT_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV
DEFAULT_MATRIX_DIR = PROJECT_ROOT / S6_MATRIX_DIR
DEFAULT_MERIT_DIR = PROJECT_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
SUPPORTED_RESOLUTIONS = ("daily", "monthly", "annual")
DEFAULT_MAX_DISTANCE_M = 5000.0
DEFAULT_POINT_ANCHORED_MAX_POINT_DISTANCE_M = 10000.0
DEFAULT_POINT_ANCHORED_MAX_NETWORK_DISTANCE_M = 20000.0
DEFAULT_POINT_ANCHORED_MAX_REACH_HOPS = 5
DEFAULT_POINT_ANCHORED_MAX_AREA_LOG10_DIFF = 0.3
DETHIER_HIGH_CONFIDENCE_MAX_POINT_DISTANCE_M = 5000.0
DETHIER_HIGH_CONFIDENCE_MAX_NETWORK_DISTANCE_M = 5000.0
DETHIER_HIGH_CONFIDENCE_MAX_REACH_HOPS = 1
DETHIER_HIGH_CONFIDENCE_MAX_AREA_LOG10_DIFF = 0.30
DETHIER_MODERATE_CONFIDENCE_MAX_POINT_DISTANCE_M = 10000.0
DETHIER_MODERATE_CONFIDENCE_MAX_NETWORK_DISTANCE_M = 20000.0
DETHIER_MODERATE_CONFIDENCE_MAX_REACH_HOPS = 5
DETHIER_MODERATE_CONFIDENCE_MAX_AREA_LOG10_DIFF = 0.15
DETHIER_CONNECTED_REACH_MAX_PATH_LENGTH_M = 20000.0
LINK_METHOD = "same_merit_reach_resolution_matrix_5km"
GSED_DAILY_FALLBACK_LINK_METHOD = "gsed_monthly_to_daily_same_merit_reach_matrix_5km"
POINT_ANCHORED_LINK_METHOD = "point_anchored_merit_reach_network_5hop_20km"
GSED_NETWORK_LINK_METHOD = "gsed_merit_reach_network_1hop"
GSED_DAILY_FALLBACK_NETWORK_LINK_METHOD = "gsed_monthly_to_daily_merit_reach_network_1hop"
GSED_SOURCE_NAMES = frozenset(("gsed",))
GSED_RESOLVED_SAME_REACH_MAX_DISTANCE_M = 10000.0
GSED_RESOLVED_ADJACENT_MAX_POINT_DISTANCE_M = 15000.0
GSED_RESOLVED_ADJACENT_MAX_NETWORK_DISTANCE_M = 10000.0
GSED_RESOLVED_ADJACENT_MAX_AREA_LOG10_DIFF = 0.15
GSED_PROVISIONAL_MAX_POINT_DISTANCE_M = 5000.0
GSED_PROVISIONAL_MAX_NETWORK_DISTANCE_M = 5000.0
GSED_PROVISIONAL_MAX_AREA_LOG10_DIFF = 0.10
GSED_PROVISIONAL_BASIN_FLAGS = frozenset(("large_offset", "geometry_inconsistent"))

OUTPUT_COLUMNS = [
    "satellite_location_uid",
    "station_id",
    "cluster_id",
    "cluster_uid",
    "source",
    "source_station_id",
    "path",
    "observation_type",
    "lat",
    "lon",
    "resolution",
    "basin_id",
    "uparea_merit",
    "basin_status",
    "basin_flag",
    "linked_cluster_id",
    "linked_cluster_uid",
    "linked_resolution",
    "link_resolution_relation",
    "link_attempted_resolutions",
    "link_status",
    "link_reason",
    "link_method",
    "linkage_mode",
    "satellite_reach_id",
    "main_reach_id",
    "same_reach",
    "reach_hops",
    "point_distance_m",
    "network_distance_m",
    "area_log10_diff",
    "candidate_count",
    "eligible_candidate_count",
    "link_quality",
    "link_distance_m",
    "link_uparea_log10_error",
    "link_candidate_count",
    "nearest_candidate_cluster_uid",
    "nearest_candidate_point_distance_m",
    "nearest_candidate_network_distance_m",
    "nearest_candidate_reach_hops",
    "nearest_candidate_area_log10_diff",
    "candidate_rejection_reason",
    "unlinked_reason",
]


def normalize_resolution(value):
    text = "" if value is None else str(value).strip().lower()
    return {
        "quarterly": "monthly",
        "single_point": "daily",
        "annually_climatology": "climatology",
    }.get(text, text)


def _clean_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _finite_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def satellite_location_uid(source, source_station_id, lat, lon):
    """Return a deterministic location identifier independent of resolution."""
    lat_value = _finite_float(lat)
    lon_value = _finite_float(lon)
    lat_text = "NA" if not math.isfinite(lat_value) else "{:.8f}".format(lat_value)
    lon_text = "NA" if not math.isfinite(lon_value) else "{:.8f}".format(lon_value)
    payload = "\x1f".join(
        [
            _clean_text(source).lower(),
            _clean_text(source_station_id),
            lat_text,
            lon_text,
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20].upper()
    return "SATLOC{}".format(digest)


def haversine_distance_m(lat1, lon1, lat2, lon2):
    lat1 = math.radians(float(lat1))
    lon1 = math.radians(float(lon1))
    lat2 = math.radians(float(lat2))
    lon2 = math.radians(float(lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
    return 6371008.8 * 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def upstream_area_log10_error(satellite_area, main_area):
    satellite_area = _finite_float(satellite_area)
    main_area = _finite_float(main_area)
    if satellite_area <= 0 or main_area <= 0:
        return math.nan
    return abs(math.log10(satellite_area / main_area))


def _finite_int(value):
    number = _finite_float(value)
    if not math.isfinite(number):
        return None
    return int(number)


def _count_value(value):
    number = _finite_float(value)
    return int(number) if math.isfinite(number) else 0


def _source_key(value):
    return _clean_text(value).strip().lower()


def allowed_link_resolutions(source, satellite_resolution):
    """Return ordered main-resolution targets allowed for one satellite row."""
    resolution = normalize_resolution(satellite_resolution)
    if _source_key(source) in GSED_SOURCE_NAMES and resolution == "monthly":
        return ("monthly", "daily")
    return (resolution,)


def _link_resolution_relation(satellite_resolution, linked_resolution):
    satellite_resolution = normalize_resolution(satellite_resolution)
    linked_resolution = normalize_resolution(linked_resolution)
    if not linked_resolution:
        return ""
    if satellite_resolution == linked_resolution:
        return "same_resolution"
    if satellite_resolution == "monthly" and linked_resolution == "daily":
        return "gsed_monthly_to_daily_fallback"
    return "cross_resolution"


def _reach_file_code(reach_id):
    reach_id = _finite_int(reach_id)
    if reach_id is None:
        return ""
    return str(abs(reach_id))[:2]


def _geometry_length_m(geometry):
    if geometry is None or getattr(geometry, "is_empty", True):
        return math.nan
    coords = list(getattr(geometry, "coords", []))
    if len(coords) < 2:
        return math.nan
    total = 0.0
    for first, second in zip(coords[:-1], coords[1:]):
        total += haversine_distance_m(first[1], first[0], second[1], second[0])
    return total


def _normalize_reach_row(reach_id, row):
    reach_id = _finite_int(row.get("COMID", reach_id))
    if reach_id is None:
        return None
    upstream_ids = row.get("upstream_ids")
    if upstream_ids is None:
        upstream_ids = [row.get(name, 0) for name in ("up1", "up2", "up3", "up4")]
    upstream_ids = tuple(
        int(value)
        for value in (_finite_int(item) for item in upstream_ids)
        if value is not None and value > 0
    )
    downstream_id = _finite_int(row.get("NextDownID", row.get("downstream_id", 0)))
    if downstream_id is not None and downstream_id <= 0:
        downstream_id = None
    length_m = _finite_float(row.get("length_m", math.nan))
    if not math.isfinite(length_m):
        length_km = _finite_float(row.get("lengthkm", math.nan))
        if math.isfinite(length_km):
            length_m = length_km * 1000.0
    geometry = row.get("geometry")
    if not math.isfinite(length_m):
        length_m = _geometry_length_m(geometry)
    return {
        "reach_id": reach_id,
        "downstream_id": downstream_id,
        "upstream_ids": upstream_ids,
        "length_m": length_m,
        "uparea": _finite_float(row.get("uparea", math.nan)),
        "geometry": geometry,
    }


class MeritReachNetwork:
    """Small COMID-indexed MERIT reach reader for Dethier 1-hop linkage."""

    def __init__(self, merit_dir=None, reach_rows=None):
        self.merit_dir = Path(merit_dir) if merit_dir else None
        self._cache = {}
        if reach_rows:
            for reach_id, row in reach_rows.items():
                normalized = _normalize_reach_row(reach_id, row)
                if normalized is not None:
                    self._cache[int(normalized["reach_id"])] = normalized

    def get(self, reach_id):
        reach_id = _finite_int(reach_id)
        if reach_id is None:
            return None
        if reach_id in self._cache:
            return self._cache[reach_id]
        loaded = self._load_from_merit(reach_id)
        self._cache[reach_id] = loaded
        return loaded

    def prefetch(self, reach_ids):
        if pyogrio is None or self.merit_dir is None:
            return
        pending = sorted(
            {
                int(reach_id)
                for reach_id in (_finite_int(value) for value in reach_ids)
                if reach_id is not None and int(reach_id) not in self._cache
            }
        )
        by_pfaf = {}
        for reach_id in pending:
            pfaf_code = _reach_file_code(reach_id)
            if pfaf_code:
                by_pfaf.setdefault(pfaf_code, []).append(reach_id)
        for pfaf_code, ids in by_pfaf.items():
            path = (
                self.merit_dir
                / "pfaf_level_02"
                / "riv_pfaf_{}_MERIT_Hydro_v07_Basins_v01_bugfix1.shp".format(pfaf_code)
            )
            if not path.is_file():
                for reach_id in ids:
                    self._cache.setdefault(reach_id, None)
                continue
            loaded_ids = set()
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                where = "COMID IN ({})".format(",".join(str(item) for item in chunk))
                try:
                    frame = pyogrio.read_dataframe(
                        str(path),
                        where=where,
                        columns=[
                            "COMID",
                            "NextDownID",
                            "up1",
                            "up2",
                            "up3",
                            "up4",
                            "lengthkm",
                            "uparea",
                        ],
                    )
                except Exception:
                    continue
                for _, row in frame.iterrows():
                    normalized = _normalize_reach_row(None, row)
                    if normalized is None:
                        continue
                    reach_id = int(normalized["reach_id"])
                    self._cache[reach_id] = normalized
                    loaded_ids.add(reach_id)
            for reach_id in ids:
                if reach_id not in loaded_ids:
                    self._cache.setdefault(reach_id, None)

    def _load_from_merit(self, reach_id):
        if pyogrio is None or self.merit_dir is None:
            return None
        pfaf_code = _reach_file_code(reach_id)
        if not pfaf_code:
            return None
        path = (
            self.merit_dir
            / "pfaf_level_02"
            / "riv_pfaf_{}_MERIT_Hydro_v07_Basins_v01_bugfix1.shp".format(pfaf_code)
        )
        if not path.is_file():
            return None
        try:
            frame = pyogrio.read_dataframe(
                str(path),
                where="COMID = {}".format(int(reach_id)),
                columns=[
                    "COMID",
                    "NextDownID",
                    "up1",
                    "up2",
                    "up3",
                    "up4",
                    "lengthkm",
                    "uparea",
                ],
            )
        except Exception:
            return None
        if frame.empty:
            return None
        return _normalize_reach_row(reach_id, frame.iloc[0])

    def adjacent_reaches(self, reach_id, max_hops=1):
        if int(max_hops) < 1:
            return {}
        reach = self.get(reach_id)
        if reach is None:
            return {}
        adjacent = {}
        for upstream_id in reach["upstream_ids"]:
            adjacent[int(upstream_id)] = "upstream"
        if reach["downstream_id"] is not None:
            adjacent[int(reach["downstream_id"])] = "downstream"
        return adjacent

    def connected_reaches(
        self,
        reach_id,
        max_hops=5,
        max_path_length_m=DETHIER_CONNECTED_REACH_MAX_PATH_LENGTH_M,
    ):
        start_id = _finite_int(reach_id)
        if start_id is None or int(max_hops) < 1:
            return []
        start = self.get(start_id)
        if start is None:
            return []

        def reach_length_m(item):
            reach = self.get(item)
            if reach is None:
                return 0.0
            length = _finite_float(reach.get("length_m", math.nan))
            return length if math.isfinite(length) and length > 0 else 0.0

        connected = []

        def add_direction(seed_ids, relation):
            frontier = []
            for next_id in seed_ids:
                next_id = _finite_int(next_id)
                if next_id is None or next_id == start_id:
                    continue
                path_length = reach_length_m(next_id)
                if path_length > float(max_path_length_m):
                    continue
                path = (start_id, int(next_id))
                frontier.append((int(next_id), path, 1, path_length))

            while frontier:
                current_id, path, hops, path_length = frontier.pop(0)
                connected.append(
                    {
                        "reach_id": int(current_id),
                        "relation": relation,
                        "reach_hops": int(hops),
                        "reach_path": path,
                    }
                )
                if hops >= int(max_hops):
                    continue
                current = self.get(current_id)
                if current is None:
                    continue
                if relation == "upstream":
                    next_ids = current["upstream_ids"]
                else:
                    next_ids = (current["downstream_id"],)
                for next_id in next_ids:
                    next_id = _finite_int(next_id)
                    if next_id is None or next_id in path:
                        continue
                    next_path_length = path_length + reach_length_m(next_id)
                    if next_path_length > float(max_path_length_m):
                        continue
                    frontier.append(
                        (
                            int(next_id),
                            path + (int(next_id),),
                            hops + 1,
                            next_path_length,
                        )
                    )

        add_direction(start["upstream_ids"], "upstream")
        if start["downstream_id"] is not None:
            add_direction((start["downstream_id"],), "downstream")
        connected.sort(key=lambda item: (item["reach_hops"], item["relation"], item["reach_id"]))
        return connected


def _reach_measure_m(reach, lat, lon):
    if Point is None or reach is None:
        return math.nan
    geometry = reach.get("geometry")
    length_m = _finite_float(reach.get("length_m"))
    if geometry is None or getattr(geometry, "is_empty", True) or not math.isfinite(length_m):
        return math.nan
    native_length = _finite_float(getattr(geometry, "length", math.nan))
    if native_length <= 0:
        return math.nan
    try:
        projected = geometry.project(Point(float(lon), float(lat)))
    except Exception:
        return math.nan
    fraction = min(1.0, max(0.0, float(projected) / native_length))
    return fraction * length_m


def _endpoint_pair_measures_m(first_reach, second_reach):
    first_geom = first_reach.get("geometry") if first_reach else None
    second_geom = second_reach.get("geometry") if second_reach else None
    first_len = _finite_float(first_reach.get("length_m") if first_reach else math.nan)
    second_len = _finite_float(second_reach.get("length_m") if second_reach else math.nan)
    if (
        first_geom is None
        or second_geom is None
        or getattr(first_geom, "is_empty", True)
        or getattr(second_geom, "is_empty", True)
        or not math.isfinite(first_len)
        or not math.isfinite(second_len)
    ):
        return math.nan, math.nan
    first_coords = list(first_geom.coords)
    second_coords = list(second_geom.coords)
    if len(first_coords) < 2 or len(second_coords) < 2:
        return math.nan, math.nan
    endpoints = (
        (first_coords[0], 0.0, second_coords[0], 0.0),
        (first_coords[0], 0.0, second_coords[-1], second_len),
        (first_coords[-1], first_len, second_coords[0], 0.0),
        (first_coords[-1], first_len, second_coords[-1], second_len),
    )
    best = min(
        endpoints,
        key=lambda item: (item[0][0] - item[2][0]) ** 2 + (item[0][1] - item[2][1]) ** 2,
    )
    return best[1], best[3]


def _network_distance_m(
    network,
    satellite_reach_id,
    main_reach_id,
    satellite_lat,
    satellite_lon,
    main_lat,
    main_lon,
    reach_path=None,
):
    satellite_reach = network.get(satellite_reach_id)
    main_reach = network.get(main_reach_id)
    if satellite_reach is None or main_reach is None:
        return math.nan
    satellite_measure = _reach_measure_m(satellite_reach, satellite_lat, satellite_lon)
    main_measure = _reach_measure_m(main_reach, main_lat, main_lon)
    if not math.isfinite(satellite_measure) or not math.isfinite(main_measure):
        return math.nan
    if int(satellite_reach_id) == int(main_reach_id):
        return abs(satellite_measure - main_measure)

    if reach_path is None:
        reach_path = (int(satellite_reach_id), int(main_reach_id))
    reach_path = tuple(int(item) for item in reach_path)
    if len(reach_path) < 2 or reach_path[0] != int(satellite_reach_id) or reach_path[-1] != int(main_reach_id):
        return math.nan

    connection_measures = {}
    for first_id, second_id in zip(reach_path[:-1], reach_path[1:]):
        first_reach = network.get(first_id)
        second_reach = network.get(second_id)
        first_endpoint, second_endpoint = _endpoint_pair_measures_m(first_reach, second_reach)
        if not math.isfinite(first_endpoint) or not math.isfinite(second_endpoint):
            return math.nan
        connection_measures.setdefault(int(first_id), []).append(first_endpoint)
        connection_measures.setdefault(int(second_id), []).append(second_endpoint)

    total = abs(satellite_measure - connection_measures[int(satellite_reach_id)][0])
    for intermediate_id in reach_path[1:-1]:
        measures = connection_measures[int(intermediate_id)]
        if len(measures) < 2:
            return math.nan
        total += abs(measures[0] - measures[1])
    total += abs(main_measure - connection_measures[int(main_reach_id)][-1])
    return total


def _read_text_variable(dataset, name, size):
    if name not in dataset.variables:
        return [""] * size
    values = dataset.variables[name][:]
    return [_clean_text(value) for value in np.asarray(values).tolist()]


def load_matrix_station_table(path, resolution):
    """Read only station-level linkage metadata from one main matrix."""
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to read main matrix files")
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError("main matrix not found: {}".format(path))

    with nc4.Dataset(path, "r") as dataset:
        required = {"cluster_id", "cluster_uid", "lat", "lon", "basin_area", "basin_status"}
        missing = sorted(required - set(dataset.variables))
        if missing:
            raise ValueError("{} missing variables: {}".format(path, ", ".join(missing)))
        size = len(dataset.dimensions["n_stations"])
        return pd.DataFrame(
            {
                "cluster_id": np.asarray(dataset.variables["cluster_id"][:]).astype(int),
                "cluster_uid": _read_text_variable(dataset, "cluster_uid", size),
                "lat": np.ma.filled(dataset.variables["lat"][:], np.nan),
                "lon": np.ma.filled(dataset.variables["lon"][:], np.nan),
                "basin_area": np.ma.filled(dataset.variables["basin_area"][:], np.nan),
                "basin_status": _read_text_variable(dataset, "basin_status", size),
                "resolution": [normalize_resolution(resolution)] * size,
            }
        )


def _validate_station_input(stations):
    required = {
        "station_id",
        "cluster_id",
        "source",
        "source_station_id",
        "path",
        "observation_type",
        "lat",
        "lon",
        "resolution",
        "basin_id",
        "uparea_merit",
        "basin_status",
    }
    missing = sorted(required - set(stations.columns))
    if missing:
        raise ValueError("s5 station input missing columns: {}".format(", ".join(missing)))


def _build_main_reach_index(stations, matrix_tables):
    source_family = stations["observation_type"].map(classify_source_family_from_observation_type)
    main_rows = stations.loc[~source_family.eq("satellite")].copy()
    main_rows["resolution_norm"] = main_rows["resolution"].map(normalize_resolution)
    main_rows["cluster_id"] = pd.to_numeric(main_rows["cluster_id"], errors="coerce")
    main_rows["basin_id"] = pd.to_numeric(main_rows["basin_id"], errors="coerce")

    hydrology = {}
    for (resolution, cluster_id), group in main_rows.groupby(
        ["resolution_norm", "cluster_id"], sort=True, dropna=True
    ):
        resolved = group["basin_status"].fillna("").astype(str).str.strip().str.lower().eq("resolved")
        basin_ids = sorted(set(group.loc[resolved, "basin_id"].dropna().astype(int).tolist()))
        if len(basin_ids) == 1:
            hydrology[(normalize_resolution(resolution), int(cluster_id))] = basin_ids[0]

    reach_index = {}
    matrix_resolutions = set()
    for resolution, matrix in sorted(matrix_tables.items()):
        resolution = normalize_resolution(resolution)
        matrix_resolutions.add(resolution)
        if matrix.empty:
            continue
        ordered = matrix.sort_values(["cluster_uid", "cluster_id"], kind="mergesort")
        for row in ordered.itertuples(index=False):
            cluster_id = int(row.cluster_id)
            basin_id = hydrology.get((resolution, cluster_id))
            if basin_id is None or _clean_text(row.basin_status).lower() != "resolved":
                continue
            candidate = {
                "cluster_id": cluster_id,
                "cluster_uid": _clean_text(row.cluster_uid),
                "resolution": resolution,
                "basin_id": int(basin_id),
                "lat": _finite_float(row.lat),
                "lon": _finite_float(row.lon),
                "uparea": _finite_float(row.basin_area),
            }
            reach_index.setdefault((resolution, int(basin_id)), []).append(candidate)
    return reach_index, matrix_resolutions


def _unlinked_row(base, reason):
    base.update(
        {
            "linked_cluster_id": pd.NA,
            "linked_cluster_uid": "",
            "linked_resolution": "",
            "link_resolution_relation": "",
            "link_attempted_resolutions": _clean_text(base.get("link_attempted_resolutions", "")),
            "link_status": "unlinked",
            "link_reason": reason,
            "link_method": "",
            "satellite_reach_id": _finite_int(base.get("basin_id")),
            "main_reach_id": pd.NA,
            "same_reach": pd.NA,
            "reach_hops": pd.NA,
            "point_distance_m": math.nan,
            "network_distance_m": math.nan,
            "area_log10_diff": math.nan,
            "candidate_count": _count_value(base.get("candidate_count", 0)),
            "eligible_candidate_count": _count_value(base.get("eligible_candidate_count", 0)),
            "link_quality": "",
            "link_distance_m": math.nan,
            "link_uparea_log10_error": math.nan,
            "link_candidate_count": _count_value(base.get("eligible_candidate_count", 0)),
            "nearest_candidate_cluster_uid": _clean_text(base.get("nearest_candidate_cluster_uid", "")),
            "nearest_candidate_point_distance_m": _finite_float(
                base.get("nearest_candidate_point_distance_m", math.nan)
            ),
            "nearest_candidate_network_distance_m": _finite_float(
                base.get("nearest_candidate_network_distance_m", math.nan)
            ),
            "nearest_candidate_reach_hops": base.get("nearest_candidate_reach_hops", pd.NA),
            "nearest_candidate_area_log10_diff": _finite_float(
                base.get("nearest_candidate_area_log10_diff", math.nan)
            ),
            "candidate_rejection_reason": _clean_text(base.get("candidate_rejection_reason", "")),
            "unlinked_reason": reason,
        }
    )
    return base


def _linkage_base_defaults(base, linkage_mode):
    base.update(
        {
            "linkage_mode": linkage_mode,
            "link_reason": "",
            "link_resolution_relation": "",
            "link_attempted_resolutions": "",
            "satellite_reach_id": _finite_int(base.get("basin_id")),
            "main_reach_id": pd.NA,
            "same_reach": pd.NA,
            "reach_hops": pd.NA,
            "point_distance_m": math.nan,
            "network_distance_m": math.nan,
            "area_log10_diff": math.nan,
            "candidate_count": 0,
            "eligible_candidate_count": 0,
            "nearest_candidate_cluster_uid": "",
            "nearest_candidate_point_distance_m": math.nan,
            "nearest_candidate_network_distance_m": math.nan,
            "nearest_candidate_reach_hops": pd.NA,
            "nearest_candidate_area_log10_diff": math.nan,
            "candidate_rejection_reason": "",
        }
    )
    return base


def _link_legacy_reach_scale_for_resolution(
    base,
    satellite_resolution,
    target_resolution,
    reach_index,
    max_distance_m,
):
    satellite_reach_id = int(base["basin_id"])
    target_resolution = normalize_resolution(target_resolution)
    reach_candidates = reach_index.get((target_resolution, satellite_reach_id), [])
    base["candidate_count"] = len(reach_candidates)
    if not reach_candidates:
        return None, "no_main_cluster_on_same_reach"

    qualified = []
    for candidate in reach_candidates:
        if not math.isfinite(candidate["lat"]) or not math.isfinite(candidate["lon"]):
            continue
        distance = haversine_distance_m(
            base["lat"], base["lon"], candidate["lat"], candidate["lon"]
        )
        if distance > float(max_distance_m):
            continue
        area_error = upstream_area_log10_error(base["uparea_merit"], candidate["uparea"])
        qualified.append((candidate, distance, area_error))

    base["eligible_candidate_count"] = len(qualified)
    if not qualified:
        return None, "no_candidate_within_5km"

    qualified.sort(
        key=lambda item: (
            item[1],
            not math.isfinite(item[2]),
            item[2] if math.isfinite(item[2]) else math.inf,
            item[0]["cluster_uid"],
            item[0]["cluster_id"],
        )
    )
    candidate, distance, area_error = qualified[0]
    base.update(
        {
            "linked_cluster_id": candidate["cluster_id"],
            "linked_cluster_uid": candidate["cluster_uid"],
            "linked_resolution": target_resolution,
            "link_resolution_relation": _link_resolution_relation(
                satellite_resolution, target_resolution
            ),
            "link_status": "linked",
            "link_reason": "same_reach_within_point_distance",
            "link_method": LINK_METHOD
            if normalize_resolution(satellite_resolution) == target_resolution
            else GSED_DAILY_FALLBACK_LINK_METHOD,
            "main_reach_id": int(candidate["basin_id"]),
            "same_reach": True,
            "reach_hops": 0,
            "point_distance_m": distance,
            "network_distance_m": math.nan,
            "area_log10_diff": area_error,
            "candidate_count": len(reach_candidates),
            "eligible_candidate_count": len(qualified),
            "link_quality": "distance_and_area_ranked"
            if math.isfinite(area_error)
            else "distance_ranked_area_unavailable",
            "link_distance_m": distance,
            "link_uparea_log10_error": area_error,
            "link_candidate_count": len(qualified),
            "unlinked_reason": "",
        }
    )
    return base, ""


def _link_legacy_reach_scale(
    base,
    satellite_resolution,
    target_resolutions,
    reach_index,
    max_distance_m,
):
    attempted = tuple(normalize_resolution(item) for item in target_resolutions if normalize_resolution(item))
    attempted_text = _clean_text(base.get("link_attempted_resolutions", "")) or "|".join(attempted)
    base["link_attempted_resolutions"] = attempted_text
    last_reason = "no_allowed_resolution"
    total_candidates = 0
    total_eligible = 0
    for target_resolution in attempted:
        working = dict(base)
        linked, reason = _link_legacy_reach_scale_for_resolution(
            working,
            satellite_resolution,
            target_resolution,
            reach_index,
            max_distance_m=max_distance_m,
        )
        total_candidates += _count_value(working.get("candidate_count", 0))
        total_eligible += _count_value(working.get("eligible_candidate_count", 0))
        last_reason = reason or last_reason
        if linked is not None:
            return linked
    base["candidate_count"] = total_candidates
    base["eligible_candidate_count"] = total_eligible
    return _unlinked_row(base, last_reason)


def _candidate_with_gsed_metrics(base, candidate, relation, reach_hops):
    satellite_reach_id = int(base["basin_id"])
    main_reach_id = int(candidate["basin_id"])
    same_reach = satellite_reach_id == main_reach_id
    point_distance = haversine_distance_m(
        base["lat"], base["lon"], candidate["lat"], candidate["lon"]
    )
    area_error = upstream_area_log10_error(base["uparea_merit"], candidate["uparea"])
    return {
        "candidate": candidate,
        "relation": relation,
        "same_reach": same_reach,
        "reach_hops": int(reach_hops),
        "point_distance_m": point_distance,
        "network_distance_m": math.nan,
        "area_log10_diff": area_error,
    }


def _ensure_gsed_network_distance(base, metrics, reach_network):
    if math.isfinite(metrics["network_distance_m"]):
        return metrics["network_distance_m"]
    candidate = metrics["candidate"]
    metrics["network_distance_m"] = _network_distance_m(
        reach_network,
        int(base["basin_id"]),
        int(candidate["basin_id"]),
        base["lat"],
        base["lon"],
        candidate["lat"],
        candidate["lon"],
    )
    return metrics["network_distance_m"]


def _gsed_resolved_failure_reason(base, metrics, reach_network):
    if metrics["same_reach"]:
        point_distance = metrics["point_distance_m"]
        if (
            not math.isfinite(point_distance)
            or point_distance > GSED_RESOLVED_SAME_REACH_MAX_DISTANCE_M
        ):
            return "candidate_point_distance_exceeded"
        network_distance = _ensure_gsed_network_distance(base, metrics, reach_network)
        if math.isfinite(network_distance):
            if network_distance > GSED_RESOLVED_SAME_REACH_MAX_DISTANCE_M:
                return "candidate_network_distance_exceeded"
            return ""
        return ""

    point_distance = metrics["point_distance_m"]
    if (
        not math.isfinite(point_distance)
        or point_distance > GSED_RESOLVED_ADJACENT_MAX_POINT_DISTANCE_M
    ):
        return "candidate_point_distance_exceeded"
    area_error = metrics["area_log10_diff"]
    if (
        not math.isfinite(area_error)
        or area_error > GSED_RESOLVED_ADJACENT_MAX_AREA_LOG10_DIFF
    ):
        return "candidate_area_inconsistent"
    network_distance = _ensure_gsed_network_distance(base, metrics, reach_network)
    if (
        not math.isfinite(network_distance)
        or network_distance > GSED_RESOLVED_ADJACENT_MAX_NETWORK_DISTANCE_M
    ):
        return "candidate_network_distance_exceeded"
    return ""


def _gsed_provisional_failure_reason(base, metrics, reach_network):
    if int(metrics["reach_hops"]) > 1:
        return "no_main_cluster_within_1hop"
    point_distance = metrics["point_distance_m"]
    if (
        not math.isfinite(point_distance)
        or point_distance > GSED_PROVISIONAL_MAX_POINT_DISTANCE_M
    ):
        return "candidate_point_distance_exceeded"
    area_error = metrics["area_log10_diff"]
    if (
        not math.isfinite(area_error)
        or area_error > GSED_PROVISIONAL_MAX_AREA_LOG10_DIFF
    ):
        return "candidate_area_inconsistent"
    network_distance = _ensure_gsed_network_distance(base, metrics, reach_network)
    if (
        not math.isfinite(network_distance)
        or network_distance > GSED_PROVISIONAL_MAX_NETWORK_DISTANCE_M
    ):
        return "candidate_network_distance_exceeded"
    return ""


def _sort_gsed_candidates(candidates):
    candidates.sort(
        key=lambda item: (
            item["target_resolution_order"],
            0 if item["basin_link_class"] == "resolved" else 1,
            0 if item["same_reach"] else 1,
            item["reach_hops"],
            item["area_log10_diff"] if math.isfinite(item["area_log10_diff"]) else math.inf,
            item["network_distance_m"] if math.isfinite(item["network_distance_m"]) else math.inf,
            item["point_distance_m"] if math.isfinite(item["point_distance_m"]) else math.inf,
            item["candidate"]["cluster_uid"],
            item["candidate"]["cluster_id"],
        )
    )


def _link_gsed_reach_scale_network(base, satellite_resolution, target_resolutions, reach_index, reach_network):
    attempted = tuple(normalize_resolution(item) for item in target_resolutions if normalize_resolution(item))
    base["link_attempted_resolutions"] = _clean_text(
        base.get("link_attempted_resolutions", "")
    ) or "|".join(attempted)

    basin_status = base["basin_status"].lower()
    basin_flag = base["basin_flag"].lower()
    if basin_status == "unresolved":
        if basin_flag == "no_match":
            return _unlinked_row(base, "satellite_basin_no_match")
        if basin_flag == "area_mismatch":
            return _unlinked_row(base, "satellite_basin_area_mismatch")
        if basin_flag not in GSED_PROVISIONAL_BASIN_FLAGS or not math.isfinite(base["basin_id"]):
            return _unlinked_row(base, "unresolved_without_strong_link_evidence")
        basin_link_class = "provisional"
    elif basin_status == "resolved":
        if not math.isfinite(base["basin_id"]):
            return _unlinked_row(base, "missing_satellite_reach")
        basin_link_class = "resolved"
    else:
        return _unlinked_row(base, "satellite_basin_unresolved")

    satellite_reach_id = int(base["basin_id"])
    total_candidates = 0
    total_eligible = 0
    failure_reasons = []
    selected = None

    for resolution_order, target_resolution in enumerate(attempted):
        same_reach_candidates = [
            candidate
            for candidate in reach_index.get((target_resolution, satellite_reach_id), [])
            if math.isfinite(candidate["lat"]) and math.isfinite(candidate["lon"])
        ]
        same_metrics = [
            _candidate_with_gsed_metrics(base, candidate, "same", 0)
            for candidate in same_reach_candidates
        ]
        total_candidates += len(same_metrics)
        eligible = []
        for metrics in same_metrics:
            reason = (
                _gsed_provisional_failure_reason(base, metrics, reach_network)
                if basin_link_class == "provisional"
                else _gsed_resolved_failure_reason(base, metrics, reach_network)
            )
            if reason:
                failure_reasons.append(reason)
                continue
            metrics["target_resolution"] = target_resolution
            metrics["target_resolution_order"] = resolution_order
            metrics["basin_link_class"] = basin_link_class
            eligible.append(metrics)

        if not eligible:
            adjacent_by_reach = reach_network.adjacent_reaches(satellite_reach_id, max_hops=1)
            adjacent_metrics = []
            for adjacent_reach_id, relation in sorted(adjacent_by_reach.items()):
                for candidate in reach_index.get((target_resolution, int(adjacent_reach_id)), []):
                    if not math.isfinite(candidate["lat"]) or not math.isfinite(candidate["lon"]):
                        continue
                    adjacent_metrics.append(
                        _candidate_with_gsed_metrics(base, candidate, relation, 1)
                    )
            total_candidates += len(adjacent_metrics)
            for metrics in adjacent_metrics:
                reason = (
                    _gsed_provisional_failure_reason(base, metrics, reach_network)
                    if basin_link_class == "provisional"
                    else _gsed_resolved_failure_reason(base, metrics, reach_network)
                )
                if reason:
                    failure_reasons.append(reason)
                    continue
                metrics["target_resolution"] = target_resolution
                metrics["target_resolution_order"] = resolution_order
                metrics["basin_link_class"] = basin_link_class
                eligible.append(metrics)

        total_eligible += len(eligible)
        if eligible:
            _sort_gsed_candidates(eligible)
            selected = eligible[0]
            break

    base["candidate_count"] = total_candidates
    base["eligible_candidate_count"] = total_eligible
    if selected is None:
        if basin_link_class == "provisional":
            return _unlinked_row(base, "unresolved_without_strong_link_evidence")
        reason = failure_reasons[0] if failure_reasons else "no_main_cluster_within_1hop"
        return _unlinked_row(base, reason)

    candidate = selected["candidate"]
    target_resolution = selected["target_resolution"]
    is_provisional = selected["basin_link_class"] == "provisional"
    link_reason = (
        "provisional_basin_unresolved_strong_network_evidence"
        if is_provisional
        else "same_reach_network_constrained"
        if selected["same_reach"]
        else "adjacent_reach_network_constrained"
    )
    link_quality = (
        "provisional_satellite_basin_unresolved"
        if is_provisional
        else "network_distance_and_area_ranked"
        if math.isfinite(selected["area_log10_diff"])
        else "network_distance_ranked_area_unavailable"
    )
    base.update(
        {
            "linked_cluster_id": candidate["cluster_id"],
            "linked_cluster_uid": candidate["cluster_uid"],
            "linked_resolution": target_resolution,
            "link_resolution_relation": _link_resolution_relation(
                satellite_resolution, target_resolution
            ),
            "link_status": "linked",
            "link_reason": link_reason,
            "link_method": GSED_NETWORK_LINK_METHOD
            if normalize_resolution(satellite_resolution) == target_resolution
            else GSED_DAILY_FALLBACK_NETWORK_LINK_METHOD,
            "main_reach_id": int(candidate["basin_id"]),
            "same_reach": bool(selected["same_reach"]),
            "reach_hops": int(selected["reach_hops"]),
            "point_distance_m": selected["point_distance_m"],
            "network_distance_m": selected["network_distance_m"],
            "area_log10_diff": selected["area_log10_diff"],
            "candidate_count": total_candidates,
            "eligible_candidate_count": total_eligible,
            "link_quality": link_quality,
            "link_distance_m": selected["point_distance_m"],
            "link_uparea_log10_error": selected["area_log10_diff"],
            "link_candidate_count": total_eligible,
            "unlinked_reason": "",
        }
    )
    return base


def _candidate_with_dethier_metrics(base, candidate, network, relation, reach_hops, reach_path):
    satellite_reach_id = int(base["basin_id"])
    main_reach_id = int(candidate["basin_id"])
    same_reach = satellite_reach_id == main_reach_id
    point_distance = haversine_distance_m(
        base["lat"], base["lon"], candidate["lat"], candidate["lon"]
    )
    network_distance = _network_distance_m(
        network,
        satellite_reach_id,
        main_reach_id,
        base["lat"],
        base["lon"],
        candidate["lat"],
        candidate["lon"],
        reach_path=reach_path,
    )
    area_error = upstream_area_log10_error(base["uparea_merit"], candidate["uparea"])
    return {
        "candidate": candidate,
        "relation": relation,
        "same_reach": same_reach,
        "reach_hops": int(reach_hops),
        "reach_path": tuple(reach_path),
        "point_distance_m": point_distance,
        "network_distance_m": network_distance,
        "area_log10_diff": area_error,
    }


def _dethier_candidate_confidence_and_reason(
    metrics,
    max_point_distance_m,
    max_network_distance_m,
    max_reach_hops,
    max_area_log10_diff,
):
    max_point_distance_m = min(
        float(max_point_distance_m),
        DETHIER_MODERATE_CONFIDENCE_MAX_POINT_DISTANCE_M,
    )
    max_network_distance_m = min(
        float(max_network_distance_m),
        DETHIER_MODERATE_CONFIDENCE_MAX_NETWORK_DISTANCE_M,
    )
    max_reach_hops = min(
        int(max_reach_hops),
        DETHIER_MODERATE_CONFIDENCE_MAX_REACH_HOPS,
    )
    high_area_limit = min(
        float(max_area_log10_diff),
        DETHIER_HIGH_CONFIDENCE_MAX_AREA_LOG10_DIFF,
    )
    moderate_area_limit = min(
        float(max_area_log10_diff),
        DETHIER_MODERATE_CONFIDENCE_MAX_AREA_LOG10_DIFF,
    )

    if not math.isfinite(metrics["point_distance_m"]):
        return "", "candidate_point_distance_unavailable"
    if metrics["point_distance_m"] > max_point_distance_m:
        return "", "candidate_point_distance_exceeded"
    if not math.isfinite(metrics["network_distance_m"]):
        return "", "candidate_network_distance_unavailable"
    if metrics["network_distance_m"] > max_network_distance_m:
        return "", "candidate_network_distance_exceeded"
    if int(metrics["reach_hops"]) > max_reach_hops:
        return "", "candidate_reach_hops_exceeded"

    area_error = metrics["area_log10_diff"]
    if (
        metrics["point_distance_m"] <= DETHIER_HIGH_CONFIDENCE_MAX_POINT_DISTANCE_M
        and metrics["network_distance_m"] <= DETHIER_HIGH_CONFIDENCE_MAX_NETWORK_DISTANCE_M
        and int(metrics["reach_hops"]) <= DETHIER_HIGH_CONFIDENCE_MAX_REACH_HOPS
        and (not math.isfinite(area_error) or area_error <= high_area_limit)
    ):
        return "high_confidence", ""

    if not math.isfinite(area_error):
        return "", "candidate_area_missing_for_moderate_confidence"
    if area_error > moderate_area_limit:
        return "", "candidate_area_inconsistent"
    return "moderate_confidence", ""


def _set_nearest_dethier_candidate_diagnostics(base, candidates):
    if not candidates:
        return
    nearest = sorted(
        candidates,
        key=lambda item: (
            item["point_distance_m"] if math.isfinite(item["point_distance_m"]) else math.inf,
            item["network_distance_m"] if math.isfinite(item["network_distance_m"]) else math.inf,
            item["reach_hops"],
            item["area_log10_diff"] if math.isfinite(item["area_log10_diff"]) else math.inf,
            item["candidate"]["cluster_uid"],
            item["candidate"]["cluster_id"],
        ),
    )[0]
    base.update(
        {
            "nearest_candidate_cluster_uid": nearest["candidate"]["cluster_uid"],
            "nearest_candidate_point_distance_m": nearest["point_distance_m"],
            "nearest_candidate_network_distance_m": nearest["network_distance_m"],
            "nearest_candidate_reach_hops": int(nearest["reach_hops"]),
            "nearest_candidate_area_log10_diff": nearest["area_log10_diff"],
            "candidate_rejection_reason": _clean_text(
                nearest.get("candidate_rejection_reason", "")
            ),
        }
    )


def _link_dethier_point_anchored(
    base,
    resolution,
    reach_index,
    reach_network,
    max_point_distance_m,
    max_network_distance_m,
    max_reach_hops,
    max_area_log10_diff,
):
    satellite_reach_id = int(base["basin_id"])
    satellite_reach = reach_network.get(satellite_reach_id)
    if satellite_reach is None:
        return _unlinked_row(base, "satellite_reach_network_missing")

    same_reach_candidates = reach_index.get((resolution, satellite_reach_id), [])
    raw_same = [
        _candidate_with_dethier_metrics(
            base,
            candidate,
            reach_network,
            "same",
            0,
            (satellite_reach_id,),
        )
        for candidate in same_reach_candidates
        if math.isfinite(candidate["lat"]) and math.isfinite(candidate["lon"])
    ]

    connected_metrics = []
    if int(max_reach_hops) >= 1:
        connected_reaches = reach_network.connected_reaches(
            satellite_reach_id,
            max_hops=max_reach_hops,
            max_path_length_m=DETHIER_CONNECTED_REACH_MAX_PATH_LENGTH_M,
        )
        for connected in connected_reaches:
            connected_reach_id = int(connected["reach_id"])
            for candidate in reach_index.get((resolution, connected_reach_id), []):
                if not math.isfinite(candidate["lat"]) or not math.isfinite(candidate["lon"]):
                    continue
                connected_metrics.append(
                    _candidate_with_dethier_metrics(
                        base,
                        candidate,
                        reach_network,
                        connected["relation"],
                        connected["reach_hops"],
                        connected["reach_path"],
                    )
                )

    candidates = raw_same + connected_metrics
    eligible = []
    for item in candidates:
        confidence, reason = _dethier_candidate_confidence_and_reason(
            item,
            max_point_distance_m,
            max_network_distance_m,
            max_reach_hops,
            max_area_log10_diff,
        )
        item["confidence"] = confidence
        item["candidate_rejection_reason"] = reason
        if confidence:
            eligible.append(item)

    candidate_count = len(candidates)
    base["candidate_count"] = candidate_count
    base["eligible_candidate_count"] = len(eligible)
    if not eligible:
        if candidate_count == 0:
            return _unlinked_row(base, "no_main_cluster_on_same_or_connected_reach")
        _set_nearest_dethier_candidate_diagnostics(base, candidates)
        return _unlinked_row(base, "no_eligible_point_anchored_candidate")

    eligible.sort(
        key=lambda item: (
            0 if item["confidence"] == "high_confidence" else 1,
            item["network_distance_m"],
            item["area_log10_diff"] if math.isfinite(item["area_log10_diff"]) else math.inf,
            item["point_distance_m"],
            item["reach_hops"],
            item["candidate"]["cluster_uid"],
            item["candidate"]["cluster_id"],
        )
    )
    best = eligible[0]
    candidate = best["candidate"]
    link_reason = "same_reach_point_anchored" if best["same_reach"] else "connected_reach_point_anchored"
    base.update(
        {
            "linked_cluster_id": candidate["cluster_id"],
            "linked_cluster_uid": candidate["cluster_uid"],
            "linked_resolution": resolution,
            "link_resolution_relation": _link_resolution_relation(resolution, resolution),
            "link_attempted_resolutions": _clean_text(base.get("link_attempted_resolutions", resolution)) or resolution,
            "link_status": "linked",
            "link_reason": link_reason,
            "link_method": POINT_ANCHORED_LINK_METHOD,
            "main_reach_id": int(candidate["basin_id"]),
            "same_reach": bool(best["same_reach"]),
            "reach_hops": int(best["reach_hops"]),
            "point_distance_m": best["point_distance_m"],
            "network_distance_m": best["network_distance_m"],
            "area_log10_diff": best["area_log10_diff"],
            "candidate_count": candidate_count,
            "eligible_candidate_count": len(eligible),
            "link_quality": best["confidence"],
            "link_distance_m": best["point_distance_m"],
            "link_uparea_log10_error": best["area_log10_diff"],
            "link_candidate_count": len(eligible),
            "unlinked_reason": "",
        }
    )
    return base


def link_satellite_to_main_clusters(
    stations,
    matrix_tables,
    max_distance_m=DEFAULT_MAX_DISTANCE_M,
    merit_dir=None,
    reach_network=None,
    max_point_distance_m=DEFAULT_POINT_ANCHORED_MAX_POINT_DISTANCE_M,
    max_network_distance_m=DEFAULT_POINT_ANCHORED_MAX_NETWORK_DISTANCE_M,
    max_reach_hops=DEFAULT_POINT_ANCHORED_MAX_REACH_HOPS,
    max_area_log10_diff=DEFAULT_POINT_ANCHORED_MAX_AREA_LOG10_DIFF,
):
    """Build one deterministic linkage row per satellite location/resolution."""
    _validate_station_input(stations)
    work = stations.copy()
    work["resolution_norm"] = work["resolution"].map(normalize_resolution)
    source_family = work["observation_type"].map(classify_source_family_from_observation_type)
    satellites = work.loc[source_family.eq("satellite")].copy()
    satellites["satellite_location_uid"] = satellites.apply(
        lambda row: satellite_location_uid(
            row.get("source"), row.get("source_station_id"), row.get("lat"), row.get("lon")
        ),
        axis=1,
    )
    satellites["cluster_id"] = pd.to_numeric(satellites["cluster_id"], errors="raise").astype(int)
    satellites["cluster_uid"] = satellites["cluster_id"].map(lambda value: "SED{:06d}".format(value))

    key_columns = ["satellite_location_uid", "resolution_norm"]
    duplicate_mask = satellites.duplicated(key_columns, keep=False)
    if duplicate_mask.any():
        duplicate_keys = satellites.loc[duplicate_mask, key_columns].drop_duplicates().head(10)
        raise ValueError(
            "satellite location/resolution linkage keys are not unique: {}".format(
                duplicate_keys.to_dict("records")
            )
        )

    reach_index, matrix_resolutions = _build_main_reach_index(work, matrix_tables)
    if reach_network is None:
        configured_merit_dir = merit_dir or os.environ.get("MERIT_DIR") or DEFAULT_MERIT_DIR
        reach_network = MeritReachNetwork(configured_merit_dir)
    elif isinstance(reach_network, dict):
        reach_network = MeritReachNetwork(reach_rows=reach_network)
    gsed_satellites = satellites.loc[
        satellites["source"].map(_source_key).isin(GSED_SOURCE_NAMES)
    ]
    if not gsed_satellites.empty and hasattr(reach_network, "prefetch"):
        gsed_reach_ids = pd.to_numeric(gsed_satellites["basin_id"], errors="coerce").dropna()
        main_reach_ids = [
            candidate["basin_id"]
            for candidates in reach_index.values()
            for candidate in candidates
        ]
        reach_network.prefetch(list(gsed_reach_ids) + main_reach_ids)
    satellites = satellites.sort_values(key_columns + ["cluster_id"], kind="mergesort")
    output_rows = []

    for row in satellites.itertuples(index=False):
        resolution = normalize_resolution(row.resolution_norm)
        base = {
            "satellite_location_uid": row.satellite_location_uid,
            "station_id": int(row.station_id),
            "cluster_id": int(row.cluster_id),
            "cluster_uid": row.cluster_uid,
            "source": _clean_text(row.source),
            "source_station_id": _clean_text(row.source_station_id),
            "path": _clean_text(row.path),
            "observation_type": _clean_text(row.observation_type),
            "lat": _finite_float(row.lat),
            "lon": _finite_float(row.lon),
            "resolution": resolution,
            "basin_id": _finite_float(row.basin_id),
            "uparea_merit": _finite_float(row.uparea_merit),
            "basin_status": _clean_text(row.basin_status),
            "basin_flag": _clean_text(getattr(row, "basin_flag", "")),
        }
        linkage_mode = classify_source_spatial_support(base["source"])
        _linkage_base_defaults(base, linkage_mode)

        if resolution not in SUPPORTED_RESOLUTIONS:
            output_rows.append(_unlinked_row(base, "invalid_resolution"))
            continue
        if not math.isfinite(base["lat"]) or not math.isfinite(base["lon"]):
            output_rows.append(_unlinked_row(base, "missing_satellite_coordinates"))
            continue
        if _source_key(base["source"]) in GSED_SOURCE_NAMES:
            target_resolutions = tuple(
                item for item in allowed_link_resolutions(base["source"], resolution)
                if item in matrix_resolutions
            )
            base["link_attempted_resolutions"] = "|".join(
                allowed_link_resolutions(base["source"], resolution)
            )
            if not target_resolutions:
                output_rows.append(_unlinked_row(base, "matrix_missing"))
                continue
            output_rows.append(
                _link_gsed_reach_scale_network(
                    base,
                    resolution,
                    target_resolutions,
                    reach_index,
                    reach_network,
                )
            )
            continue
        if base["basin_status"].lower() != "resolved":
            output_rows.append(_unlinked_row(base, "satellite_basin_unresolved"))
            continue
        if not math.isfinite(base["basin_id"]):
            output_rows.append(_unlinked_row(base, "missing_satellite_reach"))
            continue

        if linkage_mode == SOURCE_SPATIAL_SUPPORT_POINT_ANCHORED_REACH:
            base["link_attempted_resolutions"] = resolution
            if resolution not in matrix_resolutions:
                output_rows.append(_unlinked_row(base, "matrix_missing"))
                continue
            output_rows.append(
                _link_dethier_point_anchored(
                    base,
                    resolution,
                    reach_index,
                    reach_network,
                    max_point_distance_m=max_point_distance_m,
                    max_network_distance_m=max_network_distance_m,
                    max_reach_hops=max_reach_hops,
                    max_area_log10_diff=max_area_log10_diff,
                )
            )
        else:
            target_resolutions = tuple(
                item for item in allowed_link_resolutions(base["source"], resolution)
                if item in matrix_resolutions
            )
            base["link_attempted_resolutions"] = "|".join(
                allowed_link_resolutions(base["source"], resolution)
            )
            if not target_resolutions:
                output_rows.append(_unlinked_row(base, "matrix_missing"))
                continue
            output_rows.append(
                _link_legacy_reach_scale(
                    base,
                    resolution,
                    target_resolutions,
                    reach_index,
                    max_distance_m=max_distance_m,
                )
            )

    result = pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)
    if result.duplicated(["satellite_location_uid", "resolution"]).any():
        raise AssertionError("linkage output contains duplicate location/resolution keys")
    return result.sort_values(
        ["satellite_location_uid", "resolution", "cluster_id"], kind="mergesort"
    ).reset_index(drop=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--s5-csv", default=str(DEFAULT_S5_CSV))
    parser.add_argument("--matrix-dir", default=str(DEFAULT_MATRIX_DIR))
    parser.add_argument("--out", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--max-distance-m", type=float, default=DEFAULT_MAX_DISTANCE_M)
    parser.add_argument("--merit-dir", default=str(os.environ.get("MERIT_DIR") or DEFAULT_MERIT_DIR))
    parser.add_argument(
        "--point-anchored-max-point-distance-m",
        type=float,
        default=DEFAULT_POINT_ANCHORED_MAX_POINT_DISTANCE_M,
    )
    parser.add_argument(
        "--point-anchored-max-network-distance-m",
        type=float,
        default=DEFAULT_POINT_ANCHORED_MAX_NETWORK_DISTANCE_M,
    )
    parser.add_argument(
        "--point-anchored-max-reach-hops",
        type=int,
        default=DEFAULT_POINT_ANCHORED_MAX_REACH_HOPS,
    )
    parser.add_argument(
        "--point-anchored-max-area-log10-diff",
        type=float,
        default=DEFAULT_POINT_ANCHORED_MAX_AREA_LOG10_DIFF,
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    s5_csv = Path(args.s5_csv)
    if not s5_csv.is_file():
        print("Error: s5 input not found: {}".format(s5_csv), file=sys.stderr)
        return 1
    try:
        stations = pd.read_csv(s5_csv, low_memory=False)
        matrix_dir = Path(args.matrix_dir)
        matrix_tables = {
            resolution: load_matrix_station_table(
                matrix_dir / "s6_basin_matrix_{}.nc".format(resolution), resolution
            )
            for resolution in SUPPORTED_RESOLUTIONS
        }
        result = link_satellite_to_main_clusters(
            stations,
            matrix_tables,
            max_distance_m=args.max_distance_m,
            merit_dir=args.merit_dir,
            max_point_distance_m=args.point_anchored_max_point_distance_m,
            max_network_distance_m=args.point_anchored_max_network_distance_m,
            max_reach_hops=args.point_anchored_max_reach_hops,
            max_area_log10_diff=args.point_anchored_max_area_log10_diff,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print("Error: {}".format(exc), file=sys.stderr)
        return 1
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False)
    print(
        "Wrote {} rows to {} (linked={}, unlinked={})".format(
            len(result),
            output,
            int(result["link_status"].eq("linked").sum()),
            int(result["link_status"].eq("unlinked").sum()),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
