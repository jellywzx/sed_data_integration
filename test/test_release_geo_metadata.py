#!/usr/bin/env python3
"""Tests for release geographic metadata propagation."""

import sys
import tempfile
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from cluster_spatial_catalog import (  # noqa: E402
    GEO_METADATA_COLUMNS,
    attach_geo_metadata_from_source_catalog,
    build_cluster_gpkg_layers,
    build_source_dataset_catalog,
    build_source_station_gpkg_layers,
    enrich_source_station_geography,
    HAS_GPD,
)


def _write_nc(path, attrs):
    import netCDF4 as nc4

    with nc4.Dataset(path, "w") as ds:
        ds.createDimension("time", 1)
        for key, value in attrs.items():
            setattr(ds, key, value)


def _source_catalog(path):
    return pd.DataFrame(
        [
            {
                "source_station_index": 0,
                "source_station_uid": "SRC000001",
                "cluster_uid": "SED000001",
                "cluster_id": 1,
                "resolution": "daily",
                "n_records": 1,
                "time_start": "2020-01-01",
                "time_end": "2020-01-01",
                "source_name": "ExampleSource",
                "source_long_name": "Example Source",
                "source_station_lat": 10.0,
                "source_station_lon": 20.0,
                "source_station_paths": str(path),
            }
        ]
    )


def test_source_station_geo_metadata_from_nc_global_attrs():
    with tempfile.TemporaryDirectory() as tmp:
        nc_path = Path(tmp) / "station.nc"
        _write_nc(
            nc_path,
            {
                "country": "Thailand",
                "continent_region": "Asia",
                "Geographic_Coverage": "Phetchaburi River Basin, Thailand",
                "ISO_A3": "THA",
            },
        )
        enriched = enrich_source_station_geography(_source_catalog(nc_path))
        row = enriched.iloc[0]

    assert row["country"] == "Thailand"
    assert row["continent_region"] == "Asia"
    assert row["geographic_coverage"] == "Phetchaburi River Basin, Thailand"
    assert row["iso_a3"] == "THA"
    assert row["geo_attribute_source"] == "source_nc_global_attrs"
    assert row["geo_attribute_confidence"] == "high"


def test_geo_metadata_aggregates_to_cluster_and_source_dataset_catalogs():
    with tempfile.TemporaryDirectory() as tmp:
        nc_path = Path(tmp) / "station.nc"
        _write_nc(nc_path, {"country": "Canada", "geographic_coverage": "Fraser River"})
        source = enrich_source_station_geography(_source_catalog(nc_path))

    station = pd.DataFrame(
        [
            {
                "master_station_index": 0,
                "cluster_uid": "SED000001",
                "cluster_id": 1,
                "resolution": "daily",
                "record_count": 1,
                "lat": 10.0,
                "lon": 20.0,
            }
        ]
    )
    out = attach_geo_metadata_from_source_catalog(station, source, ("cluster_uid", "resolution"))
    row = out.iloc[0]
    assert row["country"] == "Canada"
    assert row["geographic_coverage"] == "Fraser River"
    assert row["geo_attribute_source"] == "source_nc_global_attrs_partial"
    assert row["geo_attribute_confidence"] == "medium"

    dataset = build_source_dataset_catalog(source)
    assert set(GEO_METADATA_COLUMNS).issubset(dataset.columns)
    assert dataset.iloc[0]["country"] == "Canada"


def test_gpkg_layer_schemas_include_geo_metadata_when_geopandas_available():
    if not HAS_GPD:
        return
    source = pd.DataFrame(
        [
            {
                "source_station_index": 0,
                "source_station_uid": "SRC000001",
                "cluster_uid": "SED000001",
                "cluster_id": 1,
                "resolution": "daily",
                "n_records": 1,
                "source_name": "ExampleSource",
                "source_station_lat": 10.0,
                "source_station_lon": 20.0,
                "country": "Thailand",
                "continent_region": "Asia",
                "geographic_coverage": "Basin",
                "iso_a3": "THA",
                "geo_attribute_source": "existing_catalog",
                "geo_attribute_confidence": "high",
            }
        ]
    )
    station = pd.DataFrame(
        [
            {
                "master_station_index": 0,
                "cluster_uid": "SED000001",
                "cluster_id": 1,
                "lat": 10.0,
                "lon": 20.0,
                "country": "Thailand",
                "continent_region": "Asia",
                "geographic_coverage": "Basin",
                "iso_a3": "THA",
                "geo_attribute_source": "existing_catalog",
                "geo_attribute_confidence": "high",
                "daily_record_count": 1,
                "available_resolutions": "daily",
                "n_available_resolutions": 1,
            }
        ]
    )
    resolution = station.copy()
    resolution["resolution"] = "daily"
    resolution["record_count"] = 1

    cluster_layers = build_cluster_gpkg_layers(station, resolution)
    source_layers = build_source_station_gpkg_layers(source)
    assert set(GEO_METADATA_COLUMNS).issubset(cluster_layers["cluster_summary"].columns)
    assert set(GEO_METADATA_COLUMNS).issubset(cluster_layers["cluster_daily"].columns)
    assert set(GEO_METADATA_COLUMNS).issubset(source_layers["source_daily"].columns)
