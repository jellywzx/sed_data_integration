#!/usr/bin/env python3
"""Tests for conservative satellite-to-main-cluster linkage."""

import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from s5b_link_satellite_to_main_clusters import (  # noqa: E402
    link_satellite_to_main_clusters,
)


def _satellite(
    station_id=100,
    resolution="daily",
    lat=0.0,
    lon=0.0,
    basin_id=9001,
    basin_status="resolved",
    uparea=100.0,
):
    return {
        "station_id": station_id,
        "cluster_id": station_id,
        "source": "RiverSed",
        "source_station_id": "SAT-{}".format(station_id),
        "path": "{}/RiverSed/SAT-{}.nc".format(resolution, station_id),
        "observation_type": "Satellite",
        "lat": lat,
        "lon": lon,
        "resolution": resolution,
        "basin_id": basin_id,
        "uparea_merit": uparea,
        "basin_status": basin_status,
        "basin_flag": "ok" if basin_status == "resolved" else "large_offset",
    }


def _main(cluster_id, resolution="daily", basin_id=9001, uparea=100.0):
    return {
        "station_id": cluster_id,
        "cluster_id": cluster_id,
        "source": "USGS",
        "source_station_id": "MAIN-{}".format(cluster_id),
        "path": "{}/USGS/MAIN-{}.nc".format(resolution, cluster_id),
        "observation_type": "in_situ",
        "lat": 0.0,
        "lon": 0.0,
        "resolution": resolution,
        "basin_id": basin_id,
        "uparea_merit": uparea,
        "basin_status": "resolved",
        "basin_flag": "ok",
    }


def _matrix(cluster_id, lon, resolution="daily", uparea=100.0, uid=None, lat=0.0):
    return {
        "cluster_id": cluster_id,
        "cluster_uid": uid or "SED{:06d}".format(cluster_id),
        "lat": lat,
        "lon": lon,
        "basin_area": uparea,
        "basin_status": "resolved",
        "resolution": resolution,
    }


def _run(satellites, mains, matrices):
    stations = pd.DataFrame(list(satellites) + list(mains))
    matrix_tables = {
        resolution: pd.DataFrame(rows)
        for resolution, rows in matrices.items()
    }
    return link_satellite_to_main_clusters(stations, matrix_tables)


def test_unique_candidate_links():
    result = _run(
        [_satellite()],
        [_main(1)],
        {"daily": [_matrix(1, lon=0.01)]},
    )
    row = result.iloc[0]
    assert row["link_status"] == "linked"
    assert row["linked_cluster_id"] == 1
    assert row["linked_resolution"] == "daily"
    assert row["link_candidate_count"] == 1


def test_multiple_candidates_choose_nearest():
    result = _run(
        [_satellite()],
        [_main(1), _main(2)],
        {"daily": [_matrix(1, lon=0.02), _matrix(2, lon=0.005)]},
    )
    row = result.iloc[0]
    assert row["linked_cluster_id"] == 2
    assert row["link_candidate_count"] == 2


def test_tie_break_uses_area_then_cluster_uid_then_cluster_id():
    result = _run(
        [_satellite(uparea=100.0)],
        [_main(20), _main(10), _main(30)],
        {
            "daily": [
                _matrix(30, lon=0.01, uparea=200.0),
                _matrix(20, lon=0.01, uparea=100.0, uid="SED000010"),
                _matrix(10, lon=0.01, uparea=100.0, uid="SED000010"),
            ]
        },
    )
    row = result.iloc[0]
    assert row["linked_cluster_id"] == 10
    assert row["link_uparea_log10_error"] == 0.0


def test_cross_resolution_does_not_link():
    result = _run(
        [_satellite(resolution="daily")],
        [_main(1, resolution="annual")],
        {"daily": [], "annual": [_matrix(1, lon=0.0, resolution="annual")]},
    )
    row = result.iloc[0]
    assert row["link_status"] == "unlinked"
    assert row["unlinked_reason"] == "no_main_cluster_on_same_reach"


def test_candidate_over_5km_does_not_link():
    result = _run(
        [_satellite()],
        [_main(1)],
        {"daily": [_matrix(1, lon=0.06)]},
    )
    row = result.iloc[0]
    assert row["link_status"] == "unlinked"
    assert row["unlinked_reason"] == "no_candidate_within_5km"


def test_unresolved_satellite_does_not_link():
    result = _run(
        [_satellite(basin_status="unresolved")],
        [_main(1)],
        {"daily": [_matrix(1, lon=0.0)]},
    )
    row = result.iloc[0]
    assert row["link_status"] == "unlinked"
    assert row["unlinked_reason"] == "satellite_basin_unresolved"


def test_unresolved_main_cluster_does_not_link():
    matrix = _matrix(1, lon=0.0)
    matrix["basin_status"] = "unresolved"
    result = _run([_satellite()], [_main(1)], {"daily": [matrix]})
    row = result.iloc[0]
    assert row["link_status"] == "unlinked"
    assert row["unlinked_reason"] == "no_main_cluster_on_same_reach"


def test_input_order_does_not_change_result():
    satellites = [_satellite(station_id=101), _satellite(station_id=102, lon=1.0, basin_id=9002)]
    mains = [_main(1), _main(2, basin_id=9002)]
    matrices = {
        "daily": [
            _matrix(1, lon=0.01),
            _matrix(2, lon=1.01),
        ]
    }
    first = _run(satellites, mains, matrices)
    second = _run(
        list(reversed(satellites)),
        list(reversed(mains)),
        {"daily": list(reversed(matrices["daily"]))},
    )
    pd.testing.assert_frame_equal(first, second)


def test_linkage_key_is_unique_and_location_uid_is_resolution_independent():
    satellites = [
        _satellite(station_id=101, resolution="daily"),
        _satellite(station_id=101, resolution="annual"),
    ]
    satellites[1]["source_station_id"] = satellites[0]["source_station_id"]
    mains = [_main(1, resolution="daily"), _main(1, resolution="annual")]
    matrices = {
        "daily": [_matrix(1, lon=0.01, resolution="daily")],
        "annual": [_matrix(1, lon=0.01, resolution="annual")],
    }
    result = _run(satellites, mains, matrices)
    assert result["satellite_location_uid"].nunique() == 1
    assert not result.duplicated(["satellite_location_uid", "resolution"]).any()
    assert set(result["resolution"]) == {"daily", "annual"}


def main():
    test_unique_candidate_links()
    test_multiple_candidates_choose_nearest()
    test_tie_break_uses_area_then_cluster_uid_then_cluster_id()
    test_cross_resolution_does_not_link()
    test_candidate_over_5km_does_not_link()
    test_unresolved_satellite_does_not_link()
    test_unresolved_main_cluster_does_not_link()
    test_input_order_does_not_change_result()
    test_linkage_key_is_unique_and_location_uid_is_resolution_independent()
    print("s5b satellite linkage tests passed")


if __name__ == "__main__":
    main()
