#!/usr/bin/env python3
"""Regression tests for auxiliary release source-field naming."""

import unittest
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = REPO_ROOT / "release_minimal_schema.yml"


class AuxiliarySourceFieldSchemaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with SCHEMA_PATH.open("r", encoding="utf-8") as fp:
            cls.schema = yaml.safe_load(fp)

    def test_satellite_netcdf_uses_source_not_source_name(self):
        self.assertIn("source", self.schema["satellite_keep_variables"])
        self.assertIn("source", self.schema["satellite_required_variables"])
        self.assertNotIn("source_name", self.schema["satellite_keep_variables"])
        self.assertNotIn("source_name", self.schema["satellite_required_variables"])
        self.assertIn("source_name", self.schema["satellite_forbidden_variables"])
        self.assertNotIn("source", self.schema["satellite_forbidden_variables"])

    def test_climatology_netcdf_uses_source_not_source_name(self):
        self.assertIn("source", self.schema["climatology_keep_variables"])
        self.assertIn("source", self.schema["climatology_required_variables"])
        self.assertNotIn("source_name", self.schema["climatology_keep_variables"])
        self.assertNotIn("source_name", self.schema["climatology_required_variables"])
        self.assertIn("source_name", self.schema["climatology_forbidden_variables"])
        self.assertNotIn("source", self.schema["climatology_forbidden_variables"])

    def test_climatology_csv_matches_netcdf_public_fields(self):
        expected = [
            "lat",
            "lon",
            "station_uid",
            "station_name",
            "river_name",
            "geographic_coverage",
            "station_index",
            "time",
            "time_coverage_start",
            "time_coverage_end",
            "resolution",
            "Q",
            "Q_flag",
            "SSC",
            "SSC_flag",
            "SSL",
            "SSL_flag",
            "source",
        ]
        self.assertEqual(self.schema["climatology_keep_variables"], expected)
        self.assertEqual(self.schema["climatology_required_variables"], expected)
        self.assertEqual(self.schema["climatology_query_columns"], expected)

    def test_catalog_source_name_contract_is_unchanged(self):
        catalogs = self.schema["minimal_catalog_columns"]
        self.assertIn("source_name", catalogs["source_station_catalog.csv"])
        self.assertIn("source_name", catalogs["source_dataset_catalog.csv"])
        self.assertIn("source_name", catalogs["satellite_catalog.csv"])


if __name__ == "__main__":
    unittest.main()
