#!/usr/bin/env python3
"""Tests for the S9 Zenodo publication audit contract."""

import importlib.util
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
MODULE_PATH = SCRIPT_DIR / "tools" / "audit_release_for_zenodo.py"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

spec = importlib.util.spec_from_file_location("audit_release_for_zenodo", MODULE_PATH)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def _require_nc4():
    if nc4 is not None:
        return
    raise unittest.SkipTest("netCDF4 is required for these tests")


def _write_matrix(path, uid_var="station_uid"):
    _require_nc4()
    with nc4.Dataset(path, "w", format="NETCDF4") as ds:
        ds.createDimension("n_stations", 1)
        ds.createDimension("time", 2)
        uid = ds.createVariable(uid_var, str, ("n_stations",))
        uid[:] = np.asarray(["SED000001"], dtype=object)
        time = ds.createVariable("time", "f8", ("time",))
        time.units = "days since 1970-01-01"
        time.calendar = "gregorian"
        time[:] = np.asarray([0.0, 1.0], dtype=np.float64)
        for name in ("SSC", "SSL"):
            var = ds.createVariable(name, "f4", ("n_stations", "time"))
            var[:] = np.asarray([[1.0, 2.0]], dtype=np.float32)
        selected = ds.createVariable("selected_source_station_uid", str, ("n_stations", "time"))
        selected[:] = np.asarray([["SRC000001", "SRC000001"]], dtype=object)
        counts = ds.createVariable("n_valid_time_steps", "i4", ("n_stations",))
        counts[:] = np.asarray([2], dtype=np.int32)


class AuditReleaseForZenodoS9Test(unittest.TestCase):
    def test_defaults_point_to_s9_output_tree(self):
        with mock.patch.object(sys, "argv", ["audit_release_for_zenodo.py"]):
            args = audit.parse_args()
        self.assertEqual(args.release_dir, str(SCRIPT_DIR / "output" / "sed_reference_release_minimal"))
        self.assertEqual(args.output_dir, str(SCRIPT_DIR / "output" / "s9_zenodo_publication_audit"))

    def test_required_files_do_not_include_full_s8_only_products(self):
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            rows = []
            audit.audit_required_files(release_dir, rows)
        targets = {row["target"] for row in rows}
        self.assertNotIn("sed_reference_master.nc", targets)
        self.assertNotIn("sed_reference_overlap_candidates.csv.gz", targets)
        self.assertIn("public_station_names_report.csv", targets)

    def test_public_station_names_report_missing_and_fail_rows_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            rows = []
            audit.audit_public_station_names_report(release_dir, rows)
            self.assertEqual(rows[-1]["status"], "fail")

            pd.DataFrame([{"file": "x", "status": "fail"}]).to_csv(
                release_dir / "public_station_names_report.csv",
                index=False,
            )
            rows = []
            audit.audit_public_station_names_report(release_dir, rows)
            self.assertEqual(rows[-1]["status"], "fail")

    def test_matrix_requires_station_uid_and_joins_catalogs(self):
        _require_nc4()
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            _write_matrix(release_dir / "sed_reference_timeseries_daily.nc", uid_var="station_uid")
            source_catalog = pd.DataFrame(
                [{"source_station_uid": "SRC000001", "source_name": "Test", "resolution": "daily"}]
            )
            station_catalog = pd.DataFrame(
                [{"station_uid": "SED000001", "resolution": "daily", "record_count": 2}]
            )
            rows = []
            audit.audit_matrix(
                release_dir,
                "daily",
                source_catalog,
                station_catalog,
                rows,
                station_chunk_size=1,
            )
            by_check = {row["check"]: row for row in rows}
            self.assertEqual(by_check["daily_station_uid_unique"]["status"], "pass")
            self.assertEqual(by_check["daily_selected_uid_catalog_join"]["status"], "pass")
            self.assertEqual(by_check["daily_station_catalog_records"]["status"], "pass")

    def test_matrix_with_only_cluster_uid_fails_public_contract(self):
        _require_nc4()
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            _write_matrix(release_dir / "sed_reference_timeseries_daily.nc", uid_var="cluster_uid")
            rows = []
            audit.audit_matrix(
                release_dir,
                "daily",
                pd.DataFrame(),
                pd.DataFrame(),
                rows,
                station_chunk_size=1,
            )
            failures = [row for row in rows if row["status"] == "fail"]
            self.assertTrue(any(row["check"] == "daily_station_uid_variable" for row in failures))

    def test_public_schema_residuals_fail_for_old_tokens(self):
        _require_nc4()
        with tempfile.TemporaryDirectory() as tmp:
            release_dir = Path(tmp)
            (release_dir / "README.md").write_text("Use cluster_uid here.\n", encoding="utf-8")
            pd.DataFrame([{"cluster_id": "1", "note": "n_clusters"}]).to_csv(
                release_dir / "station_catalog.csv",
                index=False,
            )
            _write_matrix(release_dir / "sed_reference_timeseries_daily.nc", uid_var="cluster_uid")

            rows = []
            audit.audit_public_schema_residuals(release_dir, rows)
            failed = [row for row in rows if row["status"] == "fail"]
            self.assertTrue(any(row["check"].endswith("_text") for row in failed))
            self.assertTrue(any(row["check"].endswith("_csv") for row in failed))
            self.assertTrue(any(row["check"].endswith("_netcdf") for row in failed))


if __name__ == "__main__":
    unittest.main()
