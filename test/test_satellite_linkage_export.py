#!/usr/bin/env python3
"""Tests for station-level linkage in the satellite validation products."""

import tempfile
import sys
from pathlib import Path

import netCDF4 as nc4
import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from global_attr_provenance import empty_global_attr_payload
from s6_export_satellite_validation_to_nc import (
    DEFAULT_INPUT,
    _build_satellite_catalog,
    _write_satellite_validation_nc,
)


def _station(index, linked):
    cluster_id = 101 + index
    row = {
        "satellite_station_uid": "SAT{:06d}".format(index),
        "satellite_location_uid": "SATLOC{:04d}".format(index),
        "cluster_id": cluster_id,
        "cluster_uid": "SED{:06d}".format(cluster_id),
        "source": "RiverSed",
        "source_family": "satellite",
        "observation_type": "Satellite",
        "source_station_native_id": "native-{}".format(index),
        "station_name": "Satellite {}".format(index),
        "river_name": "River {}".format(index),
        "lat": float(index),
        "lon": float(index),
        "resolution": "daily",
        "candidate_path": "candidate-{}.nc".format(index),
        "resolved_candidate_path": "/missing/candidate-{}.nc".format(index),
        "validation_only": 1,
        "merge_policy": "validation_only",
        "source_station_index": index,
        "global_attr_payload": empty_global_attr_payload(),
    }
    if linked:
        row.update(
            {
                "linked_cluster_id": 7,
                "linked_cluster_uid": "SED000007",
                "linked_resolution": "daily",
                "link_status": "linked",
                "link_method": "same_merit_reach_resolution_matrix_5km",
                "link_quality": "distance_and_area_ranked",
                "link_distance_m": 123.5,
                "link_uparea_log10_error": 0.02,
                "link_candidate_count": 2,
                "unlinked_reason": "",
            }
        )
    else:
        row.update(
            {
                "linked_cluster_id": -1,
                "linked_cluster_uid": "",
                "linked_resolution": "",
                "link_status": "unlinked",
                "link_method": "",
                "link_quality": "",
                "link_distance_m": None,
                "link_uparea_log10_error": None,
                "link_candidate_count": 0,
                "unlinked_reason": "no_main_cluster_on_same_reach",
            }
        )
    return row


def _record(station_index, day):
    return {
        "satellite_station_index": station_index,
        "cluster_id": 101 + station_index,
        "time": float(day),
        "date": "2000-01-{:02d}".format(day + 1),
        "resolution": "daily",
        "Q": 1.0,
        "SSC": 2.0,
        "SSL": 3.0,
        "Q_flag": 0,
        "SSC_flag": 0,
        "SSL_flag": 0,
    }


def _texts(variable):
    return [str(value) for value in np.asarray(variable[:]).tolist()]


def test_linked_and_unlinked_export_preserves_singletons_and_record_lookup():
    stations = [_station(0, linked=True), _station(1, linked=False)]
    records = [_record(0, 0), _record(1, 0), _record(0, 1)]
    record_map = {0: [records[0], records[2]], 1: [records[1]]}

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        main_products = [tmp_path / "main_daily.nc", tmp_path / "main_monthly.nc"]
        for index, path in enumerate(main_products):
            path.write_bytes("main-product-{}".format(index).encode("ascii"))
        before = {path: path.read_bytes() for path in main_products}

        output = tmp_path / "satellite.nc"
        _write_satellite_validation_nc(
            output,
            station_rows=stations,
            record_rows=records,
            source_meta_rows={"RiverSed": {}},
        )

        assert {path: path.read_bytes() for path in main_products} == before
        with nc4.Dataset(output, "r") as dataset:
            dataset.set_auto_mask(False)
            assert dataset.variables["cluster_id_station"][:].tolist() == [101, 102]
            assert _texts(dataset.variables["cluster_uid"]) == ["SED000101", "SED000102"]
            assert dataset.variables["linked_cluster_id"][:].tolist() == [7, -1]
            assert _texts(dataset.variables["linked_cluster_uid"]) == ["SED000007", ""]
            assert _texts(dataset.variables["link_status"]) == ["linked", "unlinked"]
            assert _texts(dataset.variables["unlinked_reason"]) == [
                "",
                "no_main_cluster_on_same_reach",
            ]
            assert dataset.variables["validation_only"][:].tolist() == [1, 1]
            assert _texts(dataset.variables["merge_policy"]) == [
                "validation_only",
                "validation_only",
            ]

            station_index = dataset.variables["satellite_station_index"][:]
            station_links = np.asarray(dataset.variables["linked_cluster_uid"][:])
            record_links = station_links[station_index]
            assert record_links.tolist() == ["SED000007", "", "SED000007"]
            assert "do not indicate" in dataset.linked_cluster_semantics

        catalog = _build_satellite_catalog(stations, record_map)
        assert catalog["link_status"].tolist() == ["linked", "unlinked"]
        assert catalog["cluster_uid"].tolist() == ["SED000101", "SED000102"]
        assert catalog["linked_cluster_uid"].tolist() == ["SED000007", ""]
        assert catalog.loc[0, "linked_cluster_id"] == 7
        assert np.isnan(catalog.loc[1, "linked_cluster_id"])
        assert catalog["validation_only"].tolist() == [1, 1]
        assert catalog["merge_policy"].tolist() == ["validation_only", "validation_only"]


def test_exporter_default_input_is_s5b_linkage():
    assert DEFAULT_INPUT.name == "s5b_satellite_main_cluster_linkage.csv"


def main():
    test_linked_and_unlinked_export_preserves_singletons_and_record_lookup()
    test_exporter_default_input_is_s5b_linkage()
    print("satellite linkage export tests passed")


if __name__ == "__main__":
    main()
