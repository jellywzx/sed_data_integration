#!/usr/bin/env python3
"""Tests for record-level satellite query-catalogue export."""

import tempfile
import unittest
from pathlib import Path

import netCDF4 as nc4
import numpy as np
import pandas as pd

from release_satellite_query_catalog import QUERY_COLUMNS, export_satellite_query_catalog


class SatelliteQueryCatalogTest(unittest.TestCase):
    def test_export_flattens_station_metadata_to_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            nc_path = root / "sed_reference_satellite.nc"
            out_path = root / "satellite_query_catalog.csv.gz"

            with nc4.Dataset(nc_path, "w", format="NETCDF4") as ds:
                ds.createDimension("n_satellite_stations", 2)
                ds.createDimension("n_satellite_records", 3)

                station_text = {
                    "satellite_station_uid": ["SAT000001", "SAT000002"],
                    "source": ["RivSed", "GSED"],
                    "station_name": ["A", "B"],
                    "river_name": ["River A", "River B"],
                    "station_resolution": ["daily", "monthly"],
                    "station_uid": ["SATINT001", "SATINT002"],
                    "linked_station_uid": ["SED000001", ""],
                    "unlinked_reason": ["", "no_match"],
                }
                for name, values in station_text.items():
                    var = ds.createVariable(name, str, ("n_satellite_stations",))
                    var[:] = np.asarray(values, dtype=object)

                for name, values in {
                    "lat": [10.0, 20.0],
                    "lon": [100.0, 110.0],
                    "link_distance_m": [25.0, np.nan],
                }.items():
                    var = ds.createVariable(name, "f8", ("n_satellite_stations",), fill_value=-9999.0)
                    arr = np.asarray(values, dtype=float)
                    var[:] = np.ma.masked_invalid(arr)

                station_index = ds.createVariable(
                    "satellite_station_index", "i4", ("n_satellite_records",)
                )
                station_index[:] = np.asarray([0, 1, 0], dtype=np.int32)

                time = ds.createVariable("time", "f8", ("n_satellite_records",))
                time[:] = np.asarray([1.0, 2.0, 3.0])

                for name, values in {
                    "Q": [np.nan, 5.0, np.nan],
                    "SSC": [10.0, 20.0, 30.0],
                    "SSL": [np.nan, 7.0, np.nan],
                }.items():
                    var = ds.createVariable(name, "f4", ("n_satellite_records",), fill_value=-9999.0)
                    var[:] = np.ma.masked_invalid(np.asarray(values, dtype=np.float32))

                for name, values in {
                    "Q_flag": [9, 0, 9],
                    "SSC_flag": [0, 0, 0],
                    "SSL_flag": [9, 0, 9],
                }.items():
                    var = ds.createVariable(name, "i1", ("n_satellite_records",))
                    var[:] = np.asarray(values, dtype=np.int8)

            summary = export_satellite_query_catalog(
                nc_path,
                out_path,
                chunk_size=2,
            )

            self.assertEqual(summary["records"], 3)
            self.assertEqual(summary["stations"], 2)

            result = pd.read_csv(out_path, keep_default_na=False)
            self.assertEqual(list(result.columns), list(QUERY_COLUMNS))
            self.assertEqual(len(result), 3)
            self.assertEqual(result.loc[0, "source"], "RivSed")
            self.assertEqual(result.loc[1, "source"], "GSED")
            self.assertEqual(result.loc[2, "linked_station_uid"], "SED000001")
            self.assertEqual(int(result.loc[1, "satellite_station_index"]), 1)


if __name__ == "__main__":
    unittest.main()
