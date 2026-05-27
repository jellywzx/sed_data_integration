#!/usr/bin/env python3
"""Smoke test for filtering satellite rows out of S4 GPKG exports only."""

import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

import s4_basin_trace_watch as s4


def _row(station_id, include_in_gpkg):
    return {
        "station_id": station_id,
        "lon": 100.0 + station_id,
        "lat": 10.0 + station_id,
        "source_station_name": "station-{}".format(station_id),
        "source_river_name": "river-{}".format(station_id),
        "source_station_id": "src-{}".format(station_id),
        "basin_id": 1000 + station_id,
        "basin_area": 123.0,
        "match_quality": "distance_only",
        "reported_area": np.nan,
        "area_error": np.nan,
        "uparea_merit": 123.0,
        "pfaf_code": "1",
        "method": "test",
        "distance_m": 1.0,
        "point_in_local": True,
        "point_in_basin": True,
        "reach_hint_used": False,
        "reach_anchor_source": "",
        "reach_anchor_lat": np.nan,
        "reach_anchor_lon": np.nan,
        "reach_endpoint_match_count": 0,
        "reach_hint_method": "",
        "basin_status": "resolved",
        "basin_flag": "ok",
        "n_upstream_reaches": 1,
        "geometry_wkt": "POINT ({} {})".format(100.0 + station_id, 10.0 + station_id),
        "geometry_local_wkt": "POINT ({} {})".format(100.0 + station_id, 10.0 + station_id),
        "_s4_gpkg_include": include_in_gpkg,
    }


def _result_df():
    columns = s4.CSV_COLUMNS + s4.GEOMETRY_EXPORT_COLUMNS + s4.GPKG_EXPORT_CONTROL_COLUMNS
    return pd.DataFrame(
        [
            _row(0, True),
            _row(1, False),
        ],
        columns=columns,
    )


def _run_merge(tmp, exclude_satellite):
    calls = []

    def fake_write_gpkg_from_wkt(result_df, wkt_column, out_path, label, logger):
        if s4.GPKG_EXPORT_CONTROL_COLUMNS[0] in result_df.columns:
            raise AssertionError("internal GPKG control column leaked into export frame")
        calls.append(
            {
                "label": label,
                "wkt_column": wkt_column,
                "station_ids": result_df["station_id"].astype(int).tolist(),
            }
        )

    original_paths = (
        s4.OUT_CSV,
        s4.OUT_REPORTED_AREA_CSV,
        s4.OUT_GPKG,
        s4.OUT_LOCAL_GPKG,
    )
    original_write = s4._write_gpkg_from_wkt
    original_save_gpkg = s4.SAVE_GPKG
    original_exclude = s4.GPKG_EXCLUDE_SATELLITE

    try:
        s4.OUT_CSV = tmp / "s4_upstream_basins.csv"
        s4.OUT_REPORTED_AREA_CSV = tmp / "s4_reported_area_check.csv"
        s4.OUT_GPKG = tmp / "s4_upstream_basins.gpkg"
        s4.OUT_LOCAL_GPKG = tmp / "s4_local_catchments.gpkg"
        s4._write_gpkg_from_wkt = fake_write_gpkg_from_wkt
        s4.SAVE_GPKG = True
        s4.GPKG_EXCLUDE_SATELLITE = exclude_satellite

        s4._merge_and_write_outputs(_result_df(), logging.getLogger("test_s4_gpkg"))
    finally:
        (
            s4.OUT_CSV,
            s4.OUT_REPORTED_AREA_CSV,
            s4.OUT_GPKG,
            s4.OUT_LOCAL_GPKG,
        ) = original_paths
        s4._write_gpkg_from_wkt = original_write
        s4.SAVE_GPKG = original_save_gpkg
        s4.GPKG_EXCLUDE_SATELLITE = original_exclude

    out = pd.read_csv(tmp / "s4_upstream_basins.csv")
    if len(out) != 2:
        raise AssertionError("S4 CSV should retain all rows, got {}".format(len(out)))
    for column in s4.GEOMETRY_EXPORT_COLUMNS + s4.GPKG_EXPORT_CONTROL_COLUMNS:
        if column in out.columns:
            raise AssertionError("{} leaked into final S4 CSV".format(column))
    return calls


def main():
    logging.basicConfig(level=logging.INFO)
    original_exclude = s4.GPKG_EXCLUDE_SATELLITE
    try:
        s4.GPKG_EXCLUDE_SATELLITE = True
        for observation_type in (
            "satellite",
            "remote_sensing",
            "Remote Sensing Observation",
            "satellite-observation",
        ):
            if s4._gpkg_include_for_observation_type(observation_type):
                raise AssertionError("{} should be excluded from GPKG".format(observation_type))
        if not s4._gpkg_include_for_observation_type("In-situ station data"):
            raise AssertionError("in-situ observation type should stay in GPKG")
    finally:
        s4.GPKG_EXCLUDE_SATELLITE = original_exclude

    with tempfile.TemporaryDirectory(prefix="s4_gpkg_filter_") as tmp_name:
        tmp = Path(tmp_name)

        calls = _run_merge(tmp, exclude_satellite=True)
        if len(calls) != 2:
            raise AssertionError("expected two GPKG write calls, got {}".format(len(calls)))
        for call in calls:
            if call["station_ids"] != [0]:
                raise AssertionError(
                    "satellite rows should be excluded from {}, got station_ids={}".format(
                        call["label"],
                        call["station_ids"],
                    )
                )

        calls = _run_merge(tmp, exclude_satellite=False)
        for call in calls:
            if call["station_ids"] != [0, 1]:
                raise AssertionError(
                    "all rows should be exported when filter is disabled for {}, got {}".format(
                        call["label"],
                        call["station_ids"],
                    )
                )

    print("s4 GPKG satellite exclusion smoke test passed.")


if __name__ == "__main__":
    main()
