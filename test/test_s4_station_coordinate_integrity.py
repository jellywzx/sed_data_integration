#!/usr/bin/env python3
"""Tests for s4 finalize s3/s4 key and coordinate integrity checks."""

import json
import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

import s4_basin_trace_watch as s4


def _s3_rows():
    return pd.DataFrame(
        [
            {
                "station_key": "S3_a",
                "station_id": 0,
                "path": "daily/src/a.nc",
                "source": "src",
                "resolution": "daily",
                "lat": 10.0,
                "lon": 100.0,
            },
            {
                "station_key": "S3_b",
                "station_id": 1,
                "path": "daily/src/b.nc",
                "source": "src",
                "resolution": "daily",
                "lat": 11.0,
                "lon": 101.0,
            },
        ]
    )


def _s4_rows(anchor_offset=0.0):
    return pd.DataFrame(
        [
            {
                "station_key": "S3_a",
                "station_id": 0,
                "lon": 100.0,
                "lat": 10.0,
                "source_station_name": "a",
                "source_river_name": "",
                "source_station_id": "a",
                "basin_id": 1,
                "basin_area": 100.0,
                "match_quality": "ok",
                "reported_area": np.nan,
                "area_error": np.nan,
                "uparea_merit": 100.0,
                "pfaf_code": "1",
                "method": "test",
                "distance_m": 0.0,
                "point_in_local": True,
                "point_in_basin": True,
                "reach_hint_used": True,
                "reach_anchor_source": "endpoint",
                "reach_anchor_lat": 99.0 + anchor_offset,
                "reach_anchor_lon": 199.0 + anchor_offset,
                "reach_endpoint_match_count": 1,
                "reach_hint_method": "test",
                "basin_status": "resolved",
                "basin_flag": "ok",
                "n_upstream_reaches": 1,
            },
            {
                "station_key": "S3_b",
                "station_id": 1,
                "lon": 101.0,
                "lat": 11.0,
                "source_station_name": "b",
                "source_river_name": "",
                "source_station_id": "b",
                "basin_id": 2,
                "basin_area": 200.0,
                "match_quality": "ok",
                "reported_area": np.nan,
                "area_error": np.nan,
                "uparea_merit": 200.0,
                "pfaf_code": "2",
                "method": "test",
                "distance_m": 0.0,
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
            },
        ]
    )


def test_modified_s4_ordinary_coordinate_fails_finalize_integrity():
    s4_df = _s4_rows()
    s4_df.loc[s4_df["station_key"].eq("S3_a"), "lat"] = 10.01
    try:
        s4._validate_s3_s4_integrity(_s3_rows(), s4_df, logging.getLogger("test_s4_coord"))
    except ValueError as exc:
        assert "ordinary lat mismatch" in str(exc)
        return
    raise AssertionError("ordinary coordinate mismatch should fail")


def test_reach_anchor_coordinate_difference_is_not_ordinary_coordinate_error():
    s4._validate_s3_s4_integrity(
        _s3_rows(),
        _s4_rows(anchor_offset=1000.0),
        logging.getLogger("test_s4_anchor"),
    )


def _with_finalize_globals(tmp, func):
    old = (
        s4.S3_CSV,
        s4.OUT_CSV,
        s4.OUT_REPORTED_AREA_CSV,
        s4.OUT_GPKG,
        s4.OUT_LOCAL_GPKG,
        s4.SHARD_DIR,
        s4.MERIT_DIR,
        s4.SHARD_COUNT,
        s4.SAVE_GPKG,
    )
    try:
        s4.S3_CSV = tmp / "s3.csv"
        s4.OUT_CSV = tmp / "s4_upstream_basins.csv"
        s4.OUT_REPORTED_AREA_CSV = tmp / "s4_reported_area_check.csv"
        s4.OUT_GPKG = tmp / "s4_upstream_basins.gpkg"
        s4.OUT_LOCAL_GPKG = tmp / "s4_local_catchments.gpkg"
        s4.SHARD_DIR = tmp / "s4_shards"
        s4.MERIT_DIR = tmp / "MERIT"
        s4.SHARD_COUNT = 2
        s4.SAVE_GPKG = False
        s4.SHARD_DIR.mkdir(parents=True, exist_ok=True)
        s4.MERIT_DIR.mkdir(parents=True, exist_ok=True)
        return func()
    finally:
        (
            s4.S3_CSV,
            s4.OUT_CSV,
            s4.OUT_REPORTED_AREA_CSV,
            s4.OUT_GPKG,
            s4.OUT_LOCAL_GPKG,
            s4.SHARD_DIR,
            s4.MERIT_DIR,
            s4.SHARD_COUNT,
            s4.SAVE_GPKG,
        ) = old


def _write_two_shards():
    _s3_rows().to_csv(s4.S3_CSV, index=False)
    rows = _s4_rows()
    for idx in range(2):
        old_index = s4.SHARD_INDEX
        try:
            s4.SHARD_INDEX = idx
            s4._ensure_shard_manifest_for_resume(
                idx,
                existing_files=False,
                logger=logging.getLogger("test_s4_multishard_manifest"),
            )
        finally:
            s4.SHARD_INDEX = old_index
        rows[rows["station_id"].mod(2).eq(idx)].to_csv(s4._shard_csv_path(idx), index=False)


def test_multishard_finalize_accepts_matching_manifests():
    with tempfile.TemporaryDirectory(prefix="s4_multishard_ok_") as tmp_name:
        tmp = Path(tmp_name)

        def run():
            _write_two_shards()
            assert s4._finalize_from_shards(logging.getLogger("test_s4_multishard_ok")) == 0
            out = pd.read_csv(s4.OUT_CSV)
            assert set(out["station_key"]) == {"S3_a", "S3_b"}

        _with_finalize_globals(tmp, run)


def test_multishard_finalize_rejects_manifest_fingerprint_mismatch():
    with tempfile.TemporaryDirectory(prefix="s4_multishard_bad_") as tmp_name:
        tmp = Path(tmp_name)

        def run():
            _write_two_shards()
            manifest_path = s4._shard_manifest_path(1)
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            data["s3_sha256"] = "bad"
            manifest_path.write_text(json.dumps(data), encoding="utf-8")
            assert s4._finalize_from_shards(logging.getLogger("test_s4_multishard_bad")) == 1
            assert not s4.OUT_CSV.exists()

        _with_finalize_globals(tmp, run)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_modified_s4_ordinary_coordinate_fails_finalize_integrity()
    test_reach_anchor_coordinate_difference_is_not_ordinary_coordinate_error()
    test_multishard_finalize_accepts_matching_manifests()
    test_multishard_finalize_rejects_manifest_fingerprint_mismatch()
    print("s4 station coordinate integrity tests passed")
