#!/usr/bin/env python3
"""Tests for stable s3 station_key generation and station_id ordering."""

import pandas as pd

from s3_collect_qc_stations import build_station_key, prepare_s3_station_table


def _rows(order):
    base = {
        "a": {
            "path": "daily/USGS/USGS_daily_a.nc",
            "source": " USGS ",
            "resolution": " Daily ",
            "lat": 1.0,
            "lon": 2.0,
        },
        "b": {
            "path": "monthly/HYBAM/HYBAM_monthly_b.nc",
            "source": "HYBAM",
            "resolution": "monthly",
            "lat": 3.0,
            "lon": 4.0,
        },
        "c": {
            "path": "daily/HYBAM/HYBAM_daily_c.nc",
            "source": "HYBAM",
            "resolution": "daily",
            "lat": 5.0,
            "lon": 6.0,
        },
    }
    return pd.DataFrame([base[k] for k in order])


def test_station_key_is_stable_when_scan_order_changes():
    first = prepare_s3_station_table(_rows(["a", "b", "c"]))
    second = prepare_s3_station_table(_rows(["c", "a", "b"]))
    by_path_first = dict(zip(first["path"], first["station_key"]))
    by_path_second = dict(zip(second["path"], second["station_key"]))
    assert by_path_first == by_path_second
    assert first.loc[first["path"].eq("daily/USGS/USGS_daily_a.nc"), "station_key"].iloc[0] == build_station_key(
        "usgs",
        "daily",
        "daily/USGS/USGS_daily_a.nc",
    )


def test_station_id_is_reproducible_after_stable_sort():
    first = prepare_s3_station_table(_rows(["a", "b", "c"]))
    second = prepare_s3_station_table(_rows(["c", "a", "b"]))
    assert first[["station_key", "station_id", "path"]].to_dict("records") == second[
        ["station_key", "station_id", "path"]
    ].to_dict("records")


if __name__ == "__main__":
    test_station_key_is_stable_when_scan_order_changes()
    test_station_id_is_reproducible_after_stable_sort()
    print("s3 station_key stability tests passed")
