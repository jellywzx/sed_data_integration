#!/usr/bin/env python3
"""Link satellite locations to hydrologically constrained main clusters.

The linkage is computed once per satellite location and resolution. Satellite
records are never scanned here, and satellite singleton cluster identifiers are
preserved in the output.
"""

import argparse
import hashlib
import math
from pathlib import Path

import numpy as np
import pandas as pd

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


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_S5_CSV = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUTPUT = PROJECT_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV
DEFAULT_MATRIX_DIR = PROJECT_ROOT / S6_MATRIX_DIR
SUPPORTED_RESOLUTIONS = ("daily", "monthly", "annual")
DEFAULT_MAX_DISTANCE_M = 5000.0
LINK_METHOD = "same_merit_reach_resolution_matrix_5km"

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
    "link_status",
    "link_method",
    "link_quality",
    "link_distance_m",
    "link_uparea_log10_error",
    "link_candidate_count",
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
            "link_status": "unlinked",
            "link_method": "",
            "link_quality": "",
            "link_distance_m": math.nan,
            "link_uparea_log10_error": math.nan,
            "link_candidate_count": 0,
            "unlinked_reason": reason,
        }
    )
    return base


def link_satellite_to_main_clusters(stations, matrix_tables, max_distance_m=DEFAULT_MAX_DISTANCE_M):
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

        if resolution not in SUPPORTED_RESOLUTIONS:
            output_rows.append(_unlinked_row(base, "invalid_resolution"))
            continue
        if not math.isfinite(base["lat"]) or not math.isfinite(base["lon"]):
            output_rows.append(_unlinked_row(base, "missing_satellite_coordinates"))
            continue
        if base["basin_status"].lower() != "resolved":
            output_rows.append(_unlinked_row(base, "satellite_basin_unresolved"))
            continue
        if not math.isfinite(base["basin_id"]):
            output_rows.append(_unlinked_row(base, "missing_satellite_reach"))
            continue
        if resolution not in matrix_resolutions:
            output_rows.append(_unlinked_row(base, "matrix_missing"))
            continue

        reach_candidates = reach_index.get((resolution, int(base["basin_id"])), [])
        if not reach_candidates:
            output_rows.append(_unlinked_row(base, "no_main_cluster_on_same_reach"))
            continue

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

        if not qualified:
            output_rows.append(_unlinked_row(base, "no_candidate_within_5km"))
            continue

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
                "linked_resolution": resolution,
                "link_status": "linked",
                "link_method": LINK_METHOD,
                "link_quality": "distance_and_area_ranked"
                if math.isfinite(area_error)
                else "distance_ranked_area_unavailable",
                "link_distance_m": distance,
                "link_uparea_log10_error": area_error,
                "link_candidate_count": len(qualified),
                "unlinked_reason": "",
            }
        )
        output_rows.append(base)

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
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    stations = pd.read_csv(Path(args.s5_csv), low_memory=False)
    matrix_dir = Path(args.matrix_dir)
    matrix_tables = {
        resolution: load_matrix_station_table(
            matrix_dir / "s6_basin_matrix_{}.nc".format(resolution), resolution
        )
        for resolution in SUPPORTED_RESOLUTIONS
    }
    result = link_satellite_to_main_clusters(
        stations, matrix_tables, max_distance_m=args.max_distance_m
    )
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
