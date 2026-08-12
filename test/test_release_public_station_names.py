#!/usr/bin/env python3
"""Tests for public station-name conversion in S9."""

import builtins
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - depends on environment
    nc4 = None


SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import release_public_station_names as public_names  # noqa: E402


def _require_nc4():
    if nc4 is not None:
        return
    raise unittest.SkipTest("netCDF4 is required for NetCDF public-name tests")


def _write_netcdf_fixture(path):
    _require_nc4()
    with nc4.Dataset(path, "w", format="NETCDF4") as ds:
        ds.createDimension("n_stations", 2)
        ds.createDimension("time", 3)
        ds.n_clusters = "2"
        ds.summary = "Matrix keyed by cluster_uid; clusters are station-reference rows."

        uid = ds.createVariable("cluster_uid", str, ("n_stations",))
        uid.cf_role = "timeseries_id"
        uid.long_name = "stable cluster identifier"
        uid[:] = np.asarray(["SED000001", "SED000002"], dtype=object)

        cid = ds.createVariable("cluster_id", "i4", ("n_stations",))
        cid.long_name = "cluster id"
        cid[:] = np.asarray([1, 2], dtype=np.int32)

        time = ds.createVariable("time", "f8", ("time",))
        time.units = "days since 1970-01-01"
        time[:] = np.asarray([0.0, 1.0, 2.0], dtype=np.float64)

        q = ds.createVariable("Q", "f4", ("n_stations", "time"))
        q[:] = np.arange(6, dtype=np.float32).reshape(2, 3)


class PublicStationNamesTest(unittest.TestCase):
    def test_csv_columns_and_schema_values_are_converted(self):
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            path = release_dir / "station_catalog.csv"
            pd.DataFrame(
                [
                    {
                        "cluster_uid": "SED000001",
                        "cluster_id": "1",
                        "station_name": "Cluster Creek",
                        "note": "Use cluster_uid and n_clusters for joins.",
                        "n_source_stations_in_cluster": "3",
                    }
                ]
            ).to_csv(path, index=False)

            rows = public_names.convert_release_dir(release_dir, audit=True)

            result = pd.read_csv(path, keep_default_na=False, dtype=str)
            self.assertIn("station_uid", result.columns)
            self.assertIn("station_reference_id", result.columns)
            self.assertIn("n_source_stations_in_reference_station", result.columns)
            self.assertNotIn("cluster_uid", result.columns)
            self.assertEqual(result.loc[0, "station_name"], "Cluster Creek")
            self.assertEqual(result.loc[0, "note"], "Use station_uid and n_reference_stations for joins.")
            self.assertFalse(public_names.has_failures(rows))

    def test_netcdf_variables_and_attrs_are_converted_without_data_loss(self):
        _require_nc4()
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            path = release_dir / "sed_reference_timeseries_daily.nc"
            _write_netcdf_fixture(path)

            rows = public_names.convert_release_dir(release_dir, audit=True)

            with nc4.Dataset(path, "r") as ds:
                self.assertIn("station_uid", ds.variables)
                self.assertIn("station_reference_id", ds.variables)
                self.assertNotIn("cluster_uid", ds.variables)
                self.assertNotIn("cluster_id", ds.variables)
                self.assertEqual(ds.n_reference_stations, "2")
                self.assertNotIn("cluster_uid", ds.summary)
                self.assertEqual(ds.variables["station_uid"].cf_role, "timeseries_id")
                self.assertEqual(ds.variables["station_uid"].long_name, "stable reference station identifier")
                np.testing.assert_array_equal(ds.variables["station_reference_id"][:], np.asarray([1, 2], dtype=np.int32))
                np.testing.assert_array_equal(ds.variables["Q"][:], np.arange(6, dtype=np.float32).reshape(2, 3))
            self.assertFalse(public_names.has_failures(rows))

    def test_residual_old_schema_names_are_reported_as_failures(self):
        _require_nc4()
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            path = release_dir / "collision.nc"
            with nc4.Dataset(path, "w", format="NETCDF4") as ds:
                ds.createDimension("n_stations", 1)
                ds.createVariable("station_uid", str, ("n_stations",))[:] = np.asarray(["SED000001"], dtype=object)
                ds.createVariable("cluster_uid", str, ("n_stations",))[:] = np.asarray(["SED000001"], dtype=object)

            rows = public_names.convert_release_dir(release_dir, audit=True)

            self.assertTrue(public_names.has_failures(rows))
            self.assertTrue(any(row.action == "audit_residual" and row.status == "fail" for row in rows))

    def test_gpkg_without_optional_dependencies_is_skipped_cleanly(self):
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name in {"fiona", "geopandas"}:
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            path = release_dir / "sed_reference_cluster_points.gpkg"
            path.write_bytes(b"not a real gpkg")
            rows = []

            with mock.patch("builtins.__import__", side_effect=fake_import):
                public_names.process_gpkg(path, release_dir, rows, dry_run=False)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].status, "skip")
            self.assertIn("GPKG conversion", rows[0].details)


if __name__ == "__main__":
    unittest.main()
