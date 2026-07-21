#!/usr/bin/env python3
"""Fast satellite-to-main-cluster linkage using ``station_catalog.csv``.

This is an alternative producer for ``s5b_satellite_main_cluster_linkage.csv``.
It preserves the conservative rules in ``s5b_link_satellite_to_main_clusters.py``
but replaces three main-matrix NetCDF reads with one lightweight catalog read.

The catalog identifies clusters that actually occur in the released daily,
monthly, or annual products. The s5 table remains the source of satellite rows
and of the MERIT reach (``basin_id``) assigned to each main cluster.
"""

import argparse
import math
import os
import sys
from pathlib import Path

import pandas as pd

from basin_policy import (
    SOURCE_SPATIAL_SUPPORT_POINT_ANCHORED_REACH,
    classify_source_spatial_support,
)
from pipeline_paths import (
    RELEASE_STATION_CATALOG_CSV,
    S5_BASIN_CLUSTERED_CSV,
    S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV,
    get_output_r_root,
)
from s5b_link_satellite_to_main_clusters import (
    DEFAULT_MAX_DISTANCE_M,
    DEFAULT_MERIT_DIR,
    DEFAULT_POINT_ANCHORED_MAX_AREA_LOG10_DIFF,
    DEFAULT_POINT_ANCHORED_MAX_NETWORK_DISTANCE_M,
    DEFAULT_POINT_ANCHORED_MAX_POINT_DISTANCE_M,
    DEFAULT_POINT_ANCHORED_MAX_REACH_HOPS,
    LINK_METHOD,
    GSED_DAILY_FALLBACK_LINK_METHOD,
    OUTPUT_COLUMNS,
    SUPPORTED_RESOLUTIONS,
    MeritReachNetwork,
    _clean_text,
    _finite_float,
    _link_dethier_point_anchored,
    _link_legacy_reach_scale,
    _linkage_base_defaults,
    _unlinked_row,
    _validate_station_input,
    allowed_link_resolutions,
    normalize_resolution,
    satellite_location_uid,
)
from s6_basin_merge_to_nc import classify_source_family_from_observation_type


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_S5_CSV = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_STATION_CATALOG = PROJECT_ROOT / RELEASE_STATION_CATALOG_CSV
DEFAULT_OUTPUT = PROJECT_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKAGE_CSV


def _truthy_record_count(value):
    """Return True when a resolution-specific catalog record count is positive."""
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def _split_available_resolutions(value):
    text = _clean_text(value).lower()
    if not text:
        return []
    for separator in (",", ";"):
        text = text.replace(separator, "|")
    return [normalize_resolution(part) for part in text.split("|") if _clean_text(part)]


def load_station_catalog_table(path):
    """Expand one-row-per-cluster station catalog into cluster-resolution rows."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError("station catalog not found: {}".format(path))

    catalog = pd.read_csv(path, low_memory=False)
    required = {"cluster_id", "cluster_uid", "lat", "lon", "basin_area", "basin_status"}
    missing = sorted(required - set(catalog.columns))
    if missing:
        raise ValueError("station catalog missing columns: {}".format(", ".join(missing)))

    has_resolution_rows = "resolution" in catalog.columns
    has_available_resolutions = "available_resolutions" in catalog.columns
    has_count_columns = any(
        "{}_record_count".format(resolution) in catalog.columns
        for resolution in SUPPORTED_RESOLUTIONS
    )
    if not (has_resolution_rows or has_available_resolutions or has_count_columns):
        raise ValueError(
            "station catalog must contain resolution, available_resolutions, or "
            "daily/monthly/annual_record_count columns"
        )

    rows = []
    for row in catalog.itertuples(index=False):
        payload = row._asdict()
        resolutions = []
        if has_resolution_rows:
            resolution = normalize_resolution(payload.get("resolution", ""))
            if resolution:
                resolutions.append(resolution)
        if has_available_resolutions:
            resolutions.extend(_split_available_resolutions(payload.get("available_resolutions", "")))
        if has_count_columns:
            for resolution in SUPPORTED_RESOLUTIONS:
                if _truthy_record_count(payload.get("{}_record_count".format(resolution))):
                    resolutions.append(resolution)

        for resolution in sorted(set(resolutions)):
            if resolution not in SUPPORTED_RESOLUTIONS:
                continue
            rows.append(
                {
                    "cluster_id": payload.get("cluster_id"),
                    "cluster_uid": payload.get("cluster_uid"),
                    "lat": payload.get("lat"),
                    "lon": payload.get("lon"),
                    "basin_area": payload.get("basin_area"),
                    "basin_status": payload.get("basin_status"),
                    "resolution": resolution,
                }
            )

    result = pd.DataFrame(rows)
    if result.empty:
        return pd.DataFrame(
            columns=[
                "cluster_id",
                "cluster_uid",
                "lat",
                "lon",
                "basin_area",
                "basin_status",
                "resolution",
            ]
        )

    result["cluster_id"] = pd.to_numeric(result["cluster_id"], errors="raise").astype(int)
    duplicate = result.duplicated(["cluster_id", "resolution"], keep=False)
    if duplicate.any():
        sample = result.loc[duplicate, ["cluster_id", "resolution"]].head(10)
        raise ValueError(
            "station catalog contains duplicate cluster-resolution rows: {}".format(
                sample.to_dict("records")
            )
        )
    return result.sort_values(["resolution", "cluster_uid", "cluster_id"], kind="mergesort")


def _build_main_reach_index_from_catalog(stations, catalog):
    """Join catalog clusters to s5 hydrology and index by resolution + reach."""
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
    catalog_resolutions = set()
    for row in catalog.itertuples(index=False):
        resolution = normalize_resolution(row.resolution)
        catalog_resolutions.add(resolution)
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
    return reach_index, catalog_resolutions


def link_satellite_to_main_clusters_from_catalog(
    stations,
    station_catalog,
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
    satellites["cluster_uid"] = satellites["cluster_id"].map(
        lambda value: "SED{:06d}".format(value)
    )

    key_columns = ["satellite_location_uid", "resolution_norm"]
    duplicate_mask = satellites.duplicated(key_columns, keep=False)
    if duplicate_mask.any():
        sample = satellites.loc[duplicate_mask, key_columns].drop_duplicates().head(10)
        raise ValueError(
            "satellite location/resolution linkage keys are not unique: {}".format(
                sample.to_dict("records")
            )
        )

    reach_index, catalog_resolutions = _build_main_reach_index_from_catalog(
        work, station_catalog
    )
    if reach_network is None:
        configured_merit_dir = merit_dir or os.environ.get("MERIT_DIR") or DEFAULT_MERIT_DIR
        reach_network = MeritReachNetwork(configured_merit_dir)
    elif isinstance(reach_network, dict):
        reach_network = MeritReachNetwork(reach_rows=reach_network)
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
        if base["basin_status"].lower() != "resolved":
            output_rows.append(_unlinked_row(base, "satellite_basin_unresolved"))
            continue
        if not math.isfinite(base["basin_id"]):
            output_rows.append(_unlinked_row(base, "missing_satellite_reach"))
            continue

        if linkage_mode == SOURCE_SPATIAL_SUPPORT_POINT_ANCHORED_REACH:
            base["link_attempted_resolutions"] = resolution
            if resolution not in catalog_resolutions:
                output_rows.append(_unlinked_row(base, "catalog_resolution_missing"))
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
                if item in catalog_resolutions
            )
            base["link_attempted_resolutions"] = "|".join(
                allowed_link_resolutions(base["source"], resolution)
            )
            if not target_resolutions:
                output_rows.append(_unlinked_row(base, "catalog_resolution_missing"))
                continue
            linked = _link_legacy_reach_scale(
                base,
                resolution,
                target_resolutions,
                reach_index,
                max_distance_m=max_distance_m,
            )
            if _clean_text(linked.get("link_method")) == LINK_METHOD:
                linked["link_method"] = LINK_METHOD.replace("matrix", "station_catalog")
            if _clean_text(linked.get("link_method")) == GSED_DAILY_FALLBACK_LINK_METHOD:
                linked["link_method"] = GSED_DAILY_FALLBACK_LINK_METHOD.replace(
                    "matrix", "station_catalog"
                )
            output_rows.append(linked)

    result = pd.DataFrame(output_rows, columns=OUTPUT_COLUMNS)
    if result.duplicated(["satellite_location_uid", "resolution"]).any():
        raise AssertionError("linkage output contains duplicate location/resolution keys")
    return result.sort_values(
        ["satellite_location_uid", "resolution", "cluster_id"], kind="mergesort"
    ).reset_index(drop=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--s5-csv", default=str(DEFAULT_S5_CSV))
    parser.add_argument("--station-catalog", default=str(DEFAULT_STATION_CATALOG))
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
        station_catalog = load_station_catalog_table(args.station_catalog)
        result = link_satellite_to_main_clusters_from_catalog(
            stations,
            station_catalog,
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
