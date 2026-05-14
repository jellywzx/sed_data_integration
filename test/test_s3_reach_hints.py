#!/usr/bin/env python3
"""Unit test for s3 reach-hint pass-through from NetCDF."""

import math
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - optional local dependency
    nc4 = None

from s3_collect_qc_stations import _collect_one_nc  # noqa: E402


def _write_sample_nc(path):
    with nc4.Dataset(path, "w") as ds:
        for name, value in {
            "lat": 11.0,
            "lon": 101.0,
            "upstream_area": 1234.0,
            "reach_midpoint_lat": 11.0,
            "reach_midpoint_lon": 101.0,
            "reach_endpoint_1_lat": 10.5,
            "reach_endpoint_1_lon": 100.5,
            "reach_endpoint_2_lat": 11.5,
            "reach_endpoint_2_lon": 101.5,
        }.items():
            var = ds.createVariable(name, "f4")
            var.assignValue(value)
        text_values = {
            "reach_endpoint_candidates_json": '[{"latitude":10.5,"longitude":100.5}]',
            "reach_coordinate_method": "reach_midpoint",
            "reach_geometry_source": "fixture.shp",
        }
        for name, value in text_values.items():
            var = ds.createVariable(name, str)
            var.assignValue(value)
        ds.station_id = "fixture"


def main():
    if nc4 is None:
        print("s3 reach-hint test skipped: netCDF4 not installed")
        return
    with tempfile.TemporaryDirectory(prefix="s3_reach_hints_") as tmp:
        root = Path(tmp)
        nc_path = root / "daily" / "GSED_daily_fixture.nc"
        nc_path.parent.mkdir(parents=True, exist_ok=True)
        _write_sample_nc(nc_path)
        row = _collect_one_nc(str(nc_path), str(root))
        if row is None:
            raise AssertionError("_collect_one_nc returned None")
        assert row["source"] == "GSED"
        assert math.isnan(row["reported_area"])
        assert row["reach_midpoint_lat"] == 11.0
        assert row["reach_endpoint_2_lon"] == 101.5
        assert row["reach_coordinate_method"] == "reach_midpoint"
        assert row["reach_geometry_source"] == "fixture.shp"
    print("s3 reach-hint pass-through test passed.")


if __name__ == "__main__":
    main()
