#!/usr/bin/env python3
"""Smoke test that remote-sensing sources bypass s4 MERIT basin tracing."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
S3_REL = Path("scripts_basin_test/output/s3_collected_stations.csv")
S4_REL = Path("scripts_basin_test/output/s4_upstream_basins.csv")
SKIP_METHOD = "source_remote_sensing_no_basin_match"


def _write_s3(root):
    out_path = root / S3_REL
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "path": "monthly/RiverSed_monthly_a.nc",
            "source": "RiverSed",
            "lat": 10.0,
            "lon": 100.0,
            "resolution": "monthly",
            "station_name": "RiverSed sample",
            "river_name": "A",
            "source_station_id": "rs-1",
            "reported_area": 1234.0,
        },
        {
            "path": "monthly/GSED_monthly_b.nc",
            "source": "GSED",
            "lat": 11.0,
            "lon": 101.0,
            "resolution": "monthly",
            "station_name": "GSED sample",
            "river_name": "B",
            "source_station_id": "gs-1",
            "reported_area": 2345.0,
        },
        {
            "path": "monthly/Dethier_monthly_c.nc",
            "source": "Dethier",
            "lat": 12.0,
            "lon": 102.0,
            "resolution": "monthly",
            "station_name": "Dethier sample",
            "river_name": "C",
            "source_station_id": "dt-1",
            "reported_area": 3456.0,
        },
        {
            "path": "monthly/Deither_monthly_d.nc",
            "source": "Deither",
            "lat": 13.0,
            "lon": 103.0,
            "resolution": "monthly",
            "station_name": "Deither alias sample",
            "river_name": "D",
            "source_station_id": "dt-2",
            "reported_area": 4567.0,
        },
    ]
    pd.DataFrame(rows).to_csv(out_path, index=False)
    return out_path


def _assert_all_missing(df, columns):
    for col in columns:
        if col not in df.columns:
            raise AssertionError("missing output column: {}".format(col))
        if not df[col].isna().all():
            raise AssertionError("{} should be entirely missing:\n{}".format(col, df[col]))


def main():
    with tempfile.TemporaryDirectory(prefix="s4_skip_sources_") as tmp:
        root = Path(tmp)
        _write_s3(root)

        env = os.environ.copy()
        env.update(
            {
                "OUTPUT_R_ROOT": str(root),
                "MERIT_DIR": str(root / "missing_merit"),
                "S4_SAVE_GPKG": "0",
                "S4_RESUME": "0",
                "S4_N_WORKERS": "2",
                "S4_BATCH_SIZE": "2",
                "PYTHONPATH": str(SCRIPT_DIR)
                + os.pathsep
                + env.get("PYTHONPATH", ""),
            }
        )

        proc = subprocess.run(
            [sys.executable, str(SCRIPT_DIR / "s4_basin_trace_watch.py")],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        combined_output = proc.stdout + "\n" + proc.stderr
        if proc.returncode != 0:
            raise AssertionError(
                "s4 failed with code {}\n{}".format(proc.returncode, combined_output)
            )
        if "MERIT dir not found" in combined_output:
            raise AssertionError("s4 should not require MERIT_DIR for skip-only input")
        if "GSED shapefile" in combined_output:
            raise AssertionError("s4 should not load or warn about GSED reach hints")

        out_path = root / S4_REL
        if not out_path.is_file():
            raise AssertionError("missing s4 output: {}".format(out_path))

        out = pd.read_csv(out_path)
        if len(out) != 4:
            raise AssertionError("expected 4 output rows, got {}".format(len(out)))

        _assert_all_missing(
            out,
            [
                "reported_area",
                "basin_id",
                "basin_area",
                "area_error",
                "uparea_merit",
                "pfaf_code",
                "distance_m",
                "n_upstream_reaches",
            ],
        )

        expected_text = {
            "match_quality": "failed",
            "method": SKIP_METHOD,
            "basin_status": "unresolved",
            "basin_flag": "no_match",
        }
        for col, expected in expected_text.items():
            values = set(out[col].fillna("").astype(str))
            if values != {expected}:
                raise AssertionError("{} expected {}, got {}".format(col, expected, values))

        for col in ("point_in_local", "point_in_basin"):
            values = set(out[col].fillna(False).astype(bool))
            if values != {False}:
                raise AssertionError("{} expected False, got {}".format(col, values))

    print("s4 skip-source smoke test passed.")


if __name__ == "__main__":
    main()
