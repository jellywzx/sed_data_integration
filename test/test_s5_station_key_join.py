#!/usr/bin/env python3
"""Tests for s5 station_key-based basin metadata joins."""

import sys
import tempfile
from pathlib import Path

import pandas as pd

import s5_basin_merge
from s5_basin_merge import validate_station_key_join


def _s3_df():
    return pd.DataFrame(
        [
            {
                "station_key": "S3_b",
                "station_id": 1,
                "path": "daily/src/b.nc",
                "source": "src",
                "resolution": "daily",
                "lat": 0.0,
                "lon": 0.004,
                "observation_type": "In-situ station data",
            },
            {
                "station_key": "S3_a",
                "station_id": 0,
                "path": "daily/src/a.nc",
                "source": "src",
                "resolution": "daily",
                "lat": 0.0,
                "lon": 0.0,
                "observation_type": "In-situ station data",
            },
        ]
    )


def _s4_df():
    return pd.DataFrame(
        [
            {
                "station_key": "S3_a",
                "station_id": 0,
                "lat": 0.0,
                "lon": 0.0,
                "basin_id": 10,
                "basin_status": "resolved",
                "uparea_merit": 100.0,
                "basin_area": 100.0,
                "match_quality": "ok",
                "area_error": 0.0,
                "pfaf_code": "1",
                "method": "test",
                "n_upstream_reaches": 1,
            },
            {
                "station_key": "S3_b",
                "station_id": 1,
                "lat": 0.0,
                "lon": 0.004,
                "basin_id": 10,
                "basin_status": "resolved",
                "uparea_merit": 104.0,
                "basin_area": 104.0,
                "match_quality": "ok",
                "area_error": 0.0,
                "pfaf_code": "1",
                "method": "test",
                "n_upstream_reaches": 1,
            },
        ]
    )


def test_shuffled_s3_rows_merge_basin_metadata_by_station_key():
    with tempfile.TemporaryDirectory(prefix="s5_key_join_") as tmp_name:
        tmp = Path(tmp_name)
        s3_csv = tmp / "s3.csv"
        s4_csv = tmp / "s4.csv"
        out_csv = tmp / "s5.csv"
        report_csv = tmp / "report.csv"
        _s3_df().to_csv(s3_csv, index=False)
        _s4_df().to_csv(s4_csv, index=False)

        old_argv = sys.argv[:]
        try:
            sys.argv = [
                "s5_basin_merge.py",
                "--s3-csv",
                str(s3_csv),
                "--basin-csv",
                str(s4_csv),
                "--out",
                str(out_csv),
                "--report",
                str(report_csv),
            ]
            assert s5_basin_merge.main() == 0
        finally:
            sys.argv = old_argv

        out = pd.read_csv(out_csv)
        by_key = out.set_index("station_key")
        assert int(by_key.loc["S3_a", "station_id"]) == 0
        assert int(by_key.loc["S3_a", "basin_id"]) == 10
        assert int(by_key.loc["S3_b", "station_id"]) == 1
        assert int(by_key.loc["S3_b", "basin_id"]) == 10
        assert set(out["cluster_id"].astype(int)) == {0}


def test_missing_extra_and_duplicate_station_key_fail():
    s3 = _s3_df()
    s4 = _s4_df()

    try:
        validate_station_key_join(s3.copy(), s4[s4["station_key"].ne("S3_b")].copy())
    except ValueError as exc:
        assert "missing" in str(exc)
    else:
        raise AssertionError("missing s4 key should fail")

    extra = pd.concat(
        [
            s4,
            pd.DataFrame(
                [
                    {
                        "station_key": "S3_extra",
                        "station_id": 99,
                        "lat": 9.0,
                        "lon": 9.0,
                        "basin_id": 99,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    try:
        validate_station_key_join(s3.copy(), extra)
    except ValueError as exc:
        assert "extra" in str(exc)
    else:
        raise AssertionError("extra s4 key should fail")

    dup = pd.concat([s4, s4.iloc[[0]]], ignore_index=True)
    try:
        validate_station_key_join(s3.copy(), dup)
    except ValueError as exc:
        assert "duplicate station_key" in str(exc)
    else:
        raise AssertionError("duplicate s4 key should fail")


if __name__ == "__main__":
    test_shuffled_s3_rows_merge_basin_metadata_by_station_key()
    test_missing_extra_and_duplicate_station_key_fail()
    print("s5 station_key join tests passed")
