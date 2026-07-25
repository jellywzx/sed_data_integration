#!/usr/bin/env python3
"""Unit tests for s4 shard input fingerprint manifests."""

import json
import logging
import tempfile
from pathlib import Path

import pandas as pd

import s4_basin_trace_watch as s4


def _write_s3(path, suffix=""):
    pd.DataFrame(
        [
            {
                "station_key": "S3_a{}".format(suffix),
                "station_id": 0,
                "path": "daily/src/a{}.nc".format(suffix),
                "source": "src",
                "resolution": "daily",
                "lat": 1.0,
                "lon": 2.0,
            }
        ]
    ).to_csv(path, index=False)


def _with_s4_globals(tmp, func):
    old = (
        s4.S3_CSV,
        s4.SHARD_DIR,
        s4.MERIT_DIR,
        s4.SHARD_COUNT,
        s4.SHARD_INDEX,
        s4.BATCH_SIZE,
        s4.SAVE_GPKG,
        s4.GPKG_EXCLUDE_SATELLITE,
    )
    try:
        s4.S3_CSV = tmp / "s3.csv"
        s4.SHARD_DIR = tmp / "s4_shards"
        s4.MERIT_DIR = tmp / "MERIT"
        s4.SHARD_COUNT = 1
        s4.SHARD_INDEX = 0
        s4.BATCH_SIZE = 3
        s4.SAVE_GPKG = False
        s4.GPKG_EXCLUDE_SATELLITE = True
        s4.SHARD_DIR.mkdir(parents=True, exist_ok=True)
        s4.MERIT_DIR.mkdir(parents=True, exist_ok=True)
        return func()
    finally:
        (
            s4.S3_CSV,
            s4.SHARD_DIR,
            s4.MERIT_DIR,
            s4.SHARD_COUNT,
            s4.SHARD_INDEX,
            s4.BATCH_SIZE,
            s4.SAVE_GPKG,
            s4.GPKG_EXCLUDE_SATELLITE,
        ) = old


def test_s3_content_change_refuses_resume():
    with tempfile.TemporaryDirectory(prefix="s4_manifest_change_") as tmp_name:
        tmp = Path(tmp_name)

        def run():
            logger = logging.getLogger("test_s4_manifest_change")
            _write_s3(s4.S3_CSV, "")
            s4._shard_work_csv_path(0).write_text("station_key\nS3_a\n", encoding="utf-8")
            s4._ensure_shard_manifest_for_resume(0, existing_files=False, logger=logger)
            _write_s3(s4.S3_CSV, "_changed")
            try:
                s4._ensure_shard_manifest_for_resume(0, existing_files=True, logger=logger)
            except ValueError as exc:
                assert "Manifest mismatch" in str(exc)
                assert "s3_sha256" in str(exc)
                return
            raise AssertionError("changed s3 should refuse resume")

        _with_s4_globals(tmp, run)


def test_legacy_shard_without_manifest_refuses_resume():
    with tempfile.TemporaryDirectory(prefix="s4_manifest_missing_") as tmp_name:
        tmp = Path(tmp_name)

        def run():
            logger = logging.getLogger("test_s4_manifest_missing")
            _write_s3(s4.S3_CSV)
            s4._shard_work_csv_path(0).write_text("station_key\nS3_a\n", encoding="utf-8")
            try:
                s4._ensure_shard_manifest_for_resume(0, existing_files=True, logger=logger)
            except ValueError as exc:
                assert "Existing shard predates input fingerprint protection" in str(exc)
                return
            raise AssertionError("legacy shard without manifest should refuse resume")

        _with_s4_globals(tmp, run)


def test_same_input_and_config_allows_resume():
    with tempfile.TemporaryDirectory(prefix="s4_manifest_same_") as tmp_name:
        tmp = Path(tmp_name)

        def run():
            logger = logging.getLogger("test_s4_manifest_same")
            _write_s3(s4.S3_CSV)
            s4._shard_work_csv_path(0).write_text("station_key\nS3_a\n", encoding="utf-8")
            manifest_path = s4._ensure_shard_manifest_for_resume(0, existing_files=False, logger=logger)
            assert manifest_path.is_file()
            s4._ensure_shard_manifest_for_resume(0, existing_files=True, logger=logger)
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            assert data["manifest_version"] == 1
            assert data["config"]["batch_size"] == 3

        _with_s4_globals(tmp, run)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_s3_content_change_refuses_resume()
    test_legacy_shard_without_manifest_refuses_resume()
    test_same_input_and_config_allows_resume()
    print("s4 resume input fingerprint tests passed")
