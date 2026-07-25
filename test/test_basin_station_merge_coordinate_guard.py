#!/usr/bin/env python3
"""Tests for basin_station_merge source-coordinate guards."""

import tempfile
from pathlib import Path

import pandas as pd

from basin_station_merge import load_station_to_basin_cluster_map


def _station_df():
    return pd.DataFrame(
        [
            {
                "station_key": "S3_a",
                "station_id": 0,
                "lat": 0.0,
                "lon": 0.0,
                "observation_type": "In-situ station data",
            },
            {
                "station_key": "S3_b",
                "station_id": 1,
                "lat": 0.0,
                "lon": 0.004,
                "observation_type": "In-situ station data",
            },
        ]
    )


def _basin_df(lat_b=0.0, lon_b=0.004, reach_anchor_far=True):
    reach_lat = 50.0 if reach_anchor_far else 0.0
    reach_lon = 50.0 if reach_anchor_far else 0.004
    return pd.DataFrame(
        [
            {
                "station_key": "S3_a",
                "station_id": 0,
                "basin_id": 1,
                "basin_status": "resolved",
                "uparea_merit": 100.0,
                "lat": 0.0,
                "lon": 0.0,
                "reach_anchor_lat": -50.0,
                "reach_anchor_lon": -50.0,
            },
            {
                "station_key": "S3_b",
                "station_id": 1,
                "basin_id": 1,
                "basin_status": "resolved",
                "uparea_merit": 101.0,
                "lat": lat_b,
                "lon": lon_b,
                "reach_anchor_lat": reach_lat,
                "reach_anchor_lon": reach_lon,
            },
        ]
    )


def _write_and_cluster(basin_df, station_df=None, max_distance=1000.0):
    with tempfile.TemporaryDirectory(prefix="basin_station_guard_") as tmp_name:
        basin_csv = Path(tmp_name) / "basin.csv"
        basin_df.to_csv(basin_csv, index=False)
        return load_station_to_basin_cluster_map(
            basin_csv,
            station_df=station_df,
            max_station_distance_m=max_distance,
            max_upstream_rel_error=0.10,
            upstream_area_col="uparea_merit",
        )


def test_s3_s4_ordinary_coordinate_mismatch_fails():
    basin_df = _basin_df(lat_b=2.0, lon_b=2.0)
    try:
        _write_and_cluster(basin_df, station_df=_station_df())
    except ValueError as exc:
        assert "ordinary lat disagrees with s3" in str(exc) or "ordinary lon disagrees with s3" in str(exc)
        return
    raise AssertionError("s3/s4 ordinary coordinate mismatch should fail")


def test_clustering_uses_s3_coordinates():
    mapping, stats = _write_and_cluster(_basin_df(), station_df=_station_df())
    assert mapping[0] == 0
    assert mapping[1] == 0
    assert stats["n_changed"] == 1


def test_reach_anchor_coordinates_do_not_drive_station_distance():
    mapping, _ = _write_and_cluster(_basin_df(reach_anchor_far=True), station_df=_station_df())
    assert mapping[0] == 0
    assert mapping[1] == 0


if __name__ == "__main__":
    test_s3_s4_ordinary_coordinate_mismatch_fails()
    test_clustering_uses_s3_coordinates()
    test_reach_anchor_coordinates_do_not_drive_station_distance()
    print("basin_station_merge coordinate guard tests passed")
