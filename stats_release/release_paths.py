#!/usr/bin/env python3
"""Path defaults and release product names for release-only statistics."""

from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RELEASE_DIR = PROJECT_ROOT / "output" / "sed_reference_release"
DEFAULT_STATS_ROOT = PROJECT_ROOT / "output_other" / "stats_release"

PRODUCT_FILES = {
    "master_nc": "sed_reference_master.nc",
    "daily_nc": "sed_reference_timeseries_daily.nc",
    "monthly_nc": "sed_reference_timeseries_monthly.nc",
    "annual_nc": "sed_reference_timeseries_annual.nc",
    "climatology_nc": "sed_reference_climatology.nc",
    "satellite_nc": "sed_reference_satellite.nc",
    "station_catalog": "station_catalog.csv",
    "source_station_catalog": "source_station_catalog.csv",
    "source_dataset_catalog": "source_dataset_catalog.csv",
    "satellite_catalog": "satellite_catalog.csv",
    "satellite_validation_catalog": "satellite_validation_catalog.csv",
    "cluster_points_gpkg": "sed_reference_cluster_points.gpkg",
    "cluster_basins_gpkg": "sed_reference_cluster_basins.gpkg",
    "source_stations_gpkg": "sed_reference_source_stations.gpkg",
    "overlap_candidates_csv_gz": "sed_reference_overlap_candidates.csv.gz",
    "overlap_candidates_parquet": "sed_reference_overlap_candidates.parquet",
    "satellite_candidates_csv_gz": "sed_reference_satellite_candidates.csv.gz",
    "satellite_candidates_parquet": "sed_reference_satellite_candidates.parquet",
    "inventory_csv": "release_inventory.csv",
    "validation_csv": "release_validation_report.csv",
    "readme": "README.md",
    "application_readme": "application_sed_reference_release.md",
    "example_workflow": "example_reference_workflow.py",
}

MATRIX_PRODUCTS = {
    "daily": PRODUCT_FILES["daily_nc"],
    "monthly": PRODUCT_FILES["monthly_nc"],
    "annual": PRODUCT_FILES["annual_nc"],
}

CORE_PRODUCTS = (
    "master_nc",
    "daily_nc",
    "monthly_nc",
    "annual_nc",
    "climatology_nc",
    "station_catalog",
    "source_station_catalog",
    "source_dataset_catalog",
)


def default_out_dir(module_name: str) -> Path:
    return DEFAULT_STATS_ROOT / module_name
