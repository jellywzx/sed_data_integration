#!/usr/bin/env python3
"""Unit tests for s11 satellite / in-situ validation helpers.

These tests use synthetic in-memory inputs only. They do not read source
datasets and do not run any pipeline step.
"""

import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from s11_satellite_insitu_validation import (  # noqa: E402
    assign_strata,
    classify_source_family,
    compute_satellite_insitu_metrics,
    load_relevant_satellite_validation_records,
    load_source_taxonomy,
    normalize_observation_table,
    pair_satellite_insitu_records,
)


def _assert_close(label, actual, expected, tol=1e-9):
    if abs(actual - expected) > tol:
        raise AssertionError("{}: expected {}, got {}".format(label, expected, actual))


def test_source_taxonomy_defaults_and_override():
    cases = {
        "RiverSed reach product": "satellite",
        "GSED product": "satellite",
        "Dethier": "satellite",
        "AquaSat": "satellite",
        "reach-scale remote sensing": "satellite",
        "USGS-0123": "in_situ",
        "HYDAT station": "in_situ",
        "GRDC": "in_situ",
        "HYBAM": "in_situ",
        "Bayern": "in_situ",
        "GFQA_v2": "in_situ",
        "compiled in situ archive": "in_situ",
        "compiled secondary product": "secondary_compilation",
    }
    for source, expected in cases.items():
        actual = classify_source_family(source)
        if actual != expected:
            raise AssertionError("{}: expected {}, got {}".format(source, expected, actual))

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "taxonomy.csv"
        pd.DataFrame([{"source": "CompiledSource", "family": "in_situ"}]).to_csv(path, index=False)
        overrides = load_source_taxonomy(path)
        actual = classify_source_family("CompiledSource", overrides)
        if actual != "in_situ":
            raise AssertionError("taxonomy override should win, got {}".format(actual))

    actual = classify_source_family("Bayern", raw_family="other", observation_type="In-situ station data")
    if actual != "in_situ":
        raise AssertionError("observation_type should override stale raw source_family, got {}".format(actual))

    actual = classify_source_family("AnySource", raw_family="other", observation_type="Satellite")
    if actual != "satellite":
        raise AssertionError("Satellite observation_type should map to satellite, got {}".format(actual))


def test_pairing_windows_are_cumulative_and_tie_breaks_by_flag():
    raw = pd.DataFrame(
        [
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "date": "2020-01-02",
                "source": "RiverSed",
                "source_station_uid": "sat",
                "SSC": 1000.0,
                "SSC_flag": 0,
            },
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "date": "2020-01-02",
                "source": "USGS",
                "source_station_uid": "insitu_bad_flag",
                "SSC": 900.0,
                "SSC_flag": 3,
            },
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "date": "2020-01-02",
                "source": "HYDAT",
                "source_station_uid": "insitu_good_flag",
                "SSC": 950.0,
                "SSC_flag": 1,
            },
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "date": "2020-01-03",
                "source": "GRDC",
                "source_station_uid": "insitu_next_day",
                "SSC": 800.0,
                "SSC_flag": 0,
            },
        ]
    )
    observations = normalize_observation_table(raw, input_mode="candidate_sidecar")
    pairs = pair_satellite_insitu_records(observations, windows=("exact", "pm1d", "pm2d"), input_mode="candidate_sidecar")
    ssc_pairs = pairs[pairs["variable"] == "SSC"].sort_values("pairing_window")
    if len(ssc_pairs) != 3:
        raise AssertionError("expected cumulative exact/pm1d/pm2d SSC pairs, got {}".format(len(ssc_pairs)))
    if set(ssc_pairs["time_delta_days"]) != {0}:
        raise AssertionError("pm1d/pm2d should include the exact best match")
    if set(ssc_pairs["insitu_source"]) != {"HYDAT"}:
        raise AssertionError("tie-break should choose lower/better flag HYDAT")


def test_metrics_skip_zero_mape_and_r2_is_pearson_squared():
    pairs = pd.DataFrame(
        [
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "variable": "SSC",
                "pairing_window": "exact",
                "source_pair": "RiverSed vs USGS",
                "satellite_value": 0.0,
                "insitu_value": 0.0,
                "ssc_bin": "missing",
                "river_width_class": "missing",
                "climate_zone": "unknown",
                "high_turbidity": False,
                "method_notes": "test",
                "assumptions": "test",
            },
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "variable": "SSC",
                "pairing_window": "exact",
                "source_pair": "RiverSed vs USGS",
                "satellite_value": 4.0,
                "insitu_value": 2.0,
                "ssc_bin": "missing",
                "river_width_class": "missing",
                "climate_zone": "unknown",
                "high_turbidity": False,
                "method_notes": "test",
                "assumptions": "test",
            },
            {
                "cluster_uid": "C2",
                "cluster_id": 2,
                "resolution": "daily",
                "variable": "SSC",
                "pairing_window": "exact",
                "source_pair": "RiverSed vs USGS",
                "satellite_value": 8.0,
                "insitu_value": 4.0,
                "ssc_bin": "missing",
                "river_width_class": "missing",
                "climate_zone": "unknown",
                "high_turbidity": False,
                "method_notes": "test",
                "assumptions": "test",
            },
        ]
    )
    metrics = compute_satellite_insitu_metrics(pairs)
    overall = metrics[metrics["group_type"] == "overall"].iloc[0]
    _assert_close("MAPE", float(overall["MAPE"]), 100.0)
    _assert_close("Pearson", float(overall["Pearson"]), 1.0)
    _assert_close("R2", float(overall["R2"]), float(overall["Pearson"]) ** 2)
    if int(overall["n_clusters"]) != 2:
        raise AssertionError("n_clusters should count unique clusters")


def test_assign_strata_defaults_missing_attrs_and_high_turbidity():
    pairs = pd.DataFrame(
        [
            {
                "cluster_uid": "C1",
                "cluster_id": 1,
                "resolution": "daily",
                "variable": "SSC",
                "pairing_window": "exact",
                "source_pair": "RiverSed vs USGS",
                "satellite_value": 1300.0,
                "insitu_value": 1200.0,
                "satellite_ssc": 1300.0,
                "insitu_ssc": 1200.0,
                "method_notes": "test",
                "assumptions": "test",
            }
        ]
    )
    out = assign_strata(pairs)
    row = out.iloc[0]
    if row["ssc_bin"] != "1000-4999":
        raise AssertionError("unexpected SSC bin: {}".format(row["ssc_bin"]))
    if not bool(row["high_turbidity"]):
        raise AssertionError("SSC >= 1000 should be high turbidity")
    if row["river_width_class"] != "missing":
        raise AssertionError("missing width should be labeled missing")
    if row["climate_zone"] != "unknown":
        raise AssertionError("missing climate should be labeled unknown")


def test_satellite_validation_nc_sparse_extract_and_parallel_match():
    try:
        import xarray as xr
    except Exception as exc:
        raise AssertionError("xarray is required for this test: {}".format(exc))

    with tempfile.TemporaryDirectory() as tmp:
        release_dir = Path(tmp)
        nc_path = release_dir / "sed_reference_satellite_validation.nc"
        origin = pd.Timestamp("1970-01-01")

        def day(text):
            return float((pd.Timestamp(text) - origin) / pd.Timedelta(days=1))

        ds = xr.Dataset(
            {
                "satellite_station_uid": ("n_satellite_stations", np.asarray(["SAT1", "SAT2"], dtype=object)),
                "cluster_uid": ("n_satellite_stations", np.asarray(["C1", "C2"], dtype=object)),
                "source": ("n_satellite_stations", np.asarray(["RiverSed", "RiverSed"], dtype=object)),
                "source_family": ("n_satellite_stations", np.asarray(["satellite", "satellite"], dtype=object)),
                "station_resolution": ("n_satellite_stations", np.asarray(["daily", "daily"], dtype=object)),
                "candidate_path": ("n_satellite_stations", np.asarray(["sat1.nc", "sat2.nc"], dtype=object)),
                "resolved_candidate_path": ("n_satellite_stations", np.asarray(["sat1.nc", "sat2.nc"], dtype=object)),
                "cluster_id_station": ("n_satellite_stations", np.asarray([1, 2], dtype=np.int32)),
                "source_station_index": ("n_satellite_stations", np.asarray([0, 1], dtype=np.int32)),
                "satellite_station_index": ("n_satellite_records", np.asarray([0, 0, 0, 1], dtype=np.int32)),
                "cluster_id": ("n_satellite_records", np.asarray([1, 1, 1, 2], dtype=np.int32)),
                "time": ("n_satellite_records", np.asarray([day("2020-01-08"), day("2020-01-10"), day("2020-01-20"), day("2020-01-10")])),
                "date": ("n_satellite_records", np.asarray(["2020-01-08", "2020-01-10", "2020-01-20", "2020-01-10"], dtype=object)),
                "resolution": ("n_satellite_records", np.asarray(["daily", "daily", "daily", "daily"], dtype=object)),
                "Q": ("n_satellite_records", np.asarray([1.0, 2.0, 3.0, 4.0], dtype=np.float32)),
                "SSC": ("n_satellite_records", np.asarray([10.0, 20.0, 30.0, 40.0], dtype=np.float32)),
                "SSL": ("n_satellite_records", np.asarray([100.0, 200.0, 300.0, 400.0], dtype=np.float32)),
                "Q_flag": ("n_satellite_records", np.asarray([0.0, 0.0, 0.0, 0.0], dtype=np.float32)),
                "SSC_flag": ("n_satellite_records", np.asarray([0.0, 0.0, 0.0, 0.0], dtype=np.float32)),
                "SSL_flag": ("n_satellite_records", np.asarray([0.0, 0.0, 0.0, 0.0], dtype=np.float32)),
            }
        )
        ds["time"].attrs["units"] = "days since 1970-01-01"
        ds.to_netcdf(str(nc_path), engine="h5netcdf")
        ds.close()

        candidate_rows = pd.DataFrame(
            [
                {
                    "cluster_uid": "C1",
                    "cluster_id": 1,
                    "resolution": "daily",
                    "date": "2020-01-10",
                    "source": "USGS",
                    "source_family": "in_situ",
                    "source_station_uid": "INS1",
                    "Q": 1.5,
                    "SSC": 15.0,
                    "SSL": 150.0,
                }
            ]
        )
        serial, serial_stats = load_relevant_satellite_validation_records(
            release_dir,
            candidate_rows,
            windows=("exact", "pm1d", "pm2d"),
            workers=1,
            chunk_size=2,
            progress=None,
        )
        parallel, parallel_stats = load_relevant_satellite_validation_records(
            release_dir,
            candidate_rows,
            windows=("exact", "pm1d", "pm2d"),
            workers=2,
            chunk_size=2,
            progress=None,
        )

        if len(serial) != 2:
            raise AssertionError("expected two satellite rows inside pm2d window, got {}".format(len(serial)))
        if serial_stats["matching_station_count"] != 1:
            raise AssertionError("expected one matching satellite station")
        if serial_stats["time_hits"] != 2 or serial_stats["value_hits"] != 2:
            raise AssertionError("unexpected satellite extraction stats: {}".format(serial_stats))
        if not serial.equals(parallel):
            raise AssertionError("serial and parallel satellite extraction should match")
        if serial_stats["satellite_rows"] != parallel_stats["satellite_rows"]:
            raise AssertionError("serial and parallel stats should agree on row count")

        combined = pd.concat([candidate_rows, serial], ignore_index=True, sort=False)
        observations = normalize_observation_table(combined, input_mode="candidate_sidecar")
        pairs_serial = pair_satellite_insitu_records(observations, workers=1, progress=None)
        pairs_parallel = pair_satellite_insitu_records(observations, workers=2, progress=None)
        if len(pairs_serial) != 12:
            raise AssertionError("expected cumulative-window pairs for two satellite records, got {}".format(len(pairs_serial)))
        if not pairs_serial.equals(pairs_parallel):
            raise AssertionError("serial and parallel pairing should match")


if __name__ == "__main__":
    for name, func in sorted(globals().items()):
        if name.startswith("test_") and callable(func):
            func()
