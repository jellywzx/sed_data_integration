#!/usr/bin/env python3
"""Unit tests for s10 validation helper functions.

These tests use synthetic in-memory inputs only.  They do not read real release
data and do not run any pipeline step.
"""

import importlib.util
import math
import sys
import tempfile
from pathlib import Path

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

S10_PATH = SCRIPT_DIR / "validate" / "s10_final_validation_results.py"
S10_SPEC = importlib.util.spec_from_file_location("s10_final_validation_results", S10_PATH)
s10_validation = importlib.util.module_from_spec(S10_SPEC)
S10_SPEC.loader.exec_module(s10_validation)

TRUE_PAIR_SKIP_REASON = s10_validation.TRUE_PAIR_SKIP_REASON
_summarize_overlap_candidates = s10_validation._summarize_overlap_candidates
aggregate_pair_metrics = s10_validation.aggregate_pair_metrics
build_legacy_validation_tables = s10_validation.build_legacy_validation_tables
build_overlap_pair_records = s10_validation.build_overlap_pair_records
build_overlap_availability_diagnostic = s10_validation.build_overlap_availability_diagnostic
canonical_family_pair = s10_validation.canonical_family_pair
canonical_source_pair = s10_validation.canonical_source_pair
classify_source_family = s10_validation.classify_source_family
compute_pair_metrics = s10_validation.compute_pair_metrics
import s8_publish_reference_dataset as s8_release  # noqa: E402


def _assert_close(label, actual, expected, tol=1e-9):
    if abs(actual - expected) > tol:
        raise AssertionError("{}: expected {}, got {}".format(label, expected, actual))


def _assert_nan(label, value):
    if not math.isnan(value):
        raise AssertionError("{}: expected NaN, got {}".format(label, value))


def test_compute_pair_metrics_bias_rmse_mae():
    metrics = compute_pair_metrics([1.0, 2.0, 3.0], [2.0, 4.0, 6.0])
    _assert_close("bias", metrics["bias"], 2.0)
    _assert_close("RMSE", metrics["RMSE"], math.sqrt((1.0 + 4.0 + 9.0) / 3.0))
    _assert_close("MAE", metrics["MAE"], 2.0)
    if metrics["n_pairs"] != 3:
        raise AssertionError("n_pairs should be 3")


def test_compute_pair_metrics_mape_skips_zero_ref():
    metrics = compute_pair_metrics([0.0, 2.0, 4.0], [10.0, 3.0, 2.0])
    expected_mape = ((abs(1.0 / 2.0) + abs(-2.0 / 4.0)) / 2.0) * 100.0
    _assert_close("MAPE", metrics["MAPE"], expected_mape)
    if metrics["n_valid_mape"] != 2:
        raise AssertionError("n_valid_mape should skip ref == 0")


def test_compute_pair_metrics_correlations_need_two_samples():
    metrics = compute_pair_metrics([1.0], [2.0])
    _assert_nan("Pearson correlation", metrics["Pearson correlation"])
    _assert_nan("Spearman rank correlation", metrics["Spearman rank correlation"])


def test_classify_source_family():
    cases = {
        "USGS-0123": "USGS",
        "HYDAT station": "HYDAT",
        "RiverSed reach product": "satellite",
        "GSED product": "satellite",
        "Dethier compiled station": "satellite",
        "AquaSat": "satellite",
        "GRDC": "in_situ",
        "HYBAM": "in_situ",
        "field in situ notes": "in_situ",
        "insitu archive": "in_situ",
        "compiled secondary product": "secondary_compilation",
        "unknown": "other",
    }
    for source, expected in cases.items():
        actual = classify_source_family(source)
        if actual != expected:
            raise AssertionError("{}: expected {}, got {}".format(source, expected, actual))


def test_canonical_source_pair_stable_order():
    a, b, pair = canonical_source_pair("z_source", "A_source")
    if (a, b, pair) != ("A_source", "z_source", "A_source vs z_source"):
        raise AssertionError("unexpected canonical pair: {}".format((a, b, pair)))
    a2, b2, pair2 = canonical_source_pair("A_source", "z_source")
    if (a2, b2, pair2) != (a, b, pair):
        raise AssertionError("canonical pair should be stable across input order")


def test_canonical_family_pair_stable_order():
    a, b, pair = canonical_family_pair("satellite", "HYDAT")
    if (a, b, pair) != ("HYDAT", "satellite", "HYDAT vs satellite"):
        raise AssertionError("unexpected canonical family pair: {}".format((a, b, pair)))


def test_build_overlap_pair_records_from_mock_candidates():
    candidates = pd.DataFrame(
        [
            {
                "cluster_uid": "SED000001",
                "cluster_id": 1,
                "resolution": "daily",
                "time": 10957.0,
                "date": "2000-01-01",
                "source": "USGS",
                "source_family": "USGS",
                "source_station_uid": "SRC000001",
                "Q": 10.0,
                "SSC": 20.0,
                "SSL": 30.0,
            },
            {
                "cluster_uid": "SED000001",
                "cluster_id": 1,
                "resolution": "daily",
                "time": 10957.0,
                "date": "2000-01-01",
                "source": "HYDAT",
                "source_family": "HYDAT",
                "source_station_uid": "SRC000002",
                "Q": 12.0,
                "SSC": 18.0,
                "SSL": 33.0,
            },
        ]
    )
    pair_records = build_overlap_pair_records(candidates)
    if len(pair_records) != 3:
        raise AssertionError("expected one pair row per variable")
    metrics = aggregate_pair_metrics(pair_records)
    q_metrics = metrics[metrics["variable"] == "Q"].iloc[0]
    if int(q_metrics["n_pairs"]) != 1:
        raise AssertionError("expected one Q pair")


def test_summarize_overlap_candidates_mock():
    candidates = pd.DataFrame(
        [
            {
                "cluster_uid": "SED000001",
                "resolution": "daily",
                "time": 10957.0,
                "date": "2000-01-01",
                "source": "USGS",
                "source_family": "USGS",
                "selected_flag": 1,
                "is_overlap": 1,
                "candidate_group_key": "SED000001|daily|2000-01-01",
            },
            {
                "cluster_uid": "SED000001",
                "resolution": "daily",
                "time": 10957.0,
                "date": "2000-01-01",
                "source": "HYDAT",
                "source_family": "HYDAT",
                "selected_flag": 0,
                "is_overlap": 1,
                "candidate_group_key": "SED000001|daily|2000-01-01",
            },
        ]
    )
    summary = _summarize_overlap_candidates(candidates)
    if int(summary["n_candidate_rows"].sum()) != 2:
        raise AssertionError("candidate summary should count two rows")
    if int(summary["n_overlap_groups"].max()) != 1:
        raise AssertionError("candidate summary should count one overlap group")


def test_missing_candidate_schema_diagnostic_skips_pair_metrics():
    schema = pd.DataFrame(
        [
            {
                "file_name": "sed_reference_master.nc",
                "object_type": "netcdf_variable",
                "dimension_or_column_or_variable": "is_overlap",
                "dtype": "int8",
                "shape_or_count": "n_records",
                "has_missing_values": False,
                "notes": "",
                "method_notes": "test",
                "assumptions": "test",
            }
        ]
    )
    diagnostic = build_overlap_availability_diagnostic(
        schema,
        supports_candidate_values=False,
        supports_source_pair_metrics=False,
    )
    if diagnostic.empty:
        raise AssertionError("diagnostic should contain at least one row")
    if bool(diagnostic["supports_source_pair_metrics"].iloc[0]):
        raise AssertionError("supports_source_pair_metrics should be false")
    if TRUE_PAIR_SKIP_REASON not in str(diagnostic["reason"].iloc[0]):
        raise AssertionError("diagnostic should include the required skipped reason")


def test_build_legacy_validation_tables_mock():
    records = pd.DataFrame(
        [
            {
                "resolution": "daily",
                "source": "USGS",
                "source_family": "USGS",
                "cluster_uid": "C1",
                "source_station_uid": "S1",
                "is_overlap": 1,
            },
            {
                "resolution": "daily",
                "source": "HYDAT",
                "source_family": "HYDAT",
                "cluster_uid": "C1",
                "source_station_uid": "S2",
                "is_overlap": 0,
            },
            {
                "resolution": "monthly",
                "source": "GFQA_v2",
                "source_family": "other",
                "cluster_uid": "C2",
                "source_station_uid": "S3",
                "is_overlap": 1,
            },
            {
                "resolution": "annual",
                "source": "HYBAM",
                "source_family": "in_situ",
                "cluster_uid": "C2",
                "source_station_uid": "S3",
                "is_overlap": 0,
            },
        ]
    )
    candidates = pd.DataFrame(
        [
            {"candidate_group_key": "G1", "selected_flag": 1, "is_overlap": 1},
            {"candidate_group_key": "G1", "selected_flag": 0, "is_overlap": 1},
            {"candidate_group_key": "G2", "selected_flag": 1, "is_overlap": 0},
        ]
    )
    tables, summary = build_legacy_validation_tables(records, candidates)

    res_family = tables["validation_resolution_source_family.csv"]
    if list(res_family.columns) != ["res", "HYDAT", "USGS", "in_situ", "other"]:
        raise AssertionError("unexpected resolution/source_family columns: {}".format(list(res_family.columns)))
    daily = res_family[res_family["res"] == "daily"].iloc[0]
    if int(daily["HYDAT"]) != 1 or int(daily["USGS"]) != 1:
        raise AssertionError("daily family counts should include HYDAT and USGS")

    by_source = tables["validation_source_by_resolution.csv"]
    if list(by_source.columns) != ["src", "annual", "daily", "monthly"]:
        raise AssertionError("unexpected source/resolution columns: {}".format(list(by_source.columns)))
    usgs = by_source[by_source["src"] == "USGS"].iloc[0]
    if int(usgs["daily"]) != 1:
        raise AssertionError("USGS should have one daily record")

    overlap_source = tables["validation_overlap_by_source.csv"]
    gfqa = overlap_source[overlap_source["src"] == "GFQA_v2"].iloc[0]
    if int(gfqa["count"]) != 1 or int(gfqa["sum"]) != 1:
        raise AssertionError("GFQA_v2 overlap count/sum mismatch")

    overlap_res = tables["validation_overlap_by_resolution.csv"]
    if list(overlap_res.columns) != ["res", "0", "1"]:
        raise AssertionError("unexpected overlap/resolution columns: {}".format(list(overlap_res.columns)))
    daily_overlap = overlap_res[overlap_res["res"] == "daily"].iloc[0]
    if int(daily_overlap["0"]) != 1 or int(daily_overlap["1"]) != 1:
        raise AssertionError("daily overlap flags should have one non-overlap and one overlap")

    if summary["n_master"] != 4:
        raise AssertionError("n_master should be 4")
    if summary["n_clusters"] != 2:
        raise AssertionError("n_clusters should be 2")
    if summary["n_ss"] != 3:
        raise AssertionError("n_ss should be 3")
    if summary["n_candidates"] != 3 or summary["n_sel"] != 2:
        raise AssertionError("candidate summary counts mismatch")
    if summary["n_overlap_pairs"] != 1:
        raise AssertionError("overlap group count should count only overlap groups")
    if summary["ov_dist"] != {"non_overlap": 2, "overlap": 2}:
        raise AssertionError("unexpected overlap distribution: {}".format(summary["ov_dist"]))


def test_build_legacy_validation_tables_empty_inputs():
    tables, summary = build_legacy_validation_tables(pd.DataFrame(), pd.DataFrame())
    expected_columns = {
        "validation_resolution_source_family.csv": ["res"],
        "validation_source_by_resolution.csv": ["src"],
        "validation_overlap_by_source.csv": ["src", "count", "sum"],
        "validation_overlap_by_resolution.csv": ["res", "0", "1"],
    }
    for name, columns in expected_columns.items():
        if list(tables[name].columns) != columns:
            raise AssertionError("{} columns mismatch: {}".format(name, list(tables[name].columns)))
    if summary["n_master"] != 0 or summary["n_candidates"] != 0:
        raise AssertionError("empty summary counts should be zero")
    if summary["fam_dist"] != {}:
        raise AssertionError("empty fam_dist should be empty")


def test_build_overlap_candidates_sidecar_mocked_series():
    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        first = tmpdir / "first.nc"
        second = tmpdir / "second.nc"
        first.write_text("", encoding="utf-8")
        second.write_text("", encoding="utf-8")
        quality = pd.DataFrame(
            [
                {
                    "cluster_id": 1,
                    "cluster_uid": "SED000001",
                    "resolution": "daily",
                    "quality_rank": 1,
                    "n_candidates": 2,
                    "is_top_ranked": 1,
                    "source": "USGS",
                    "observation_type": "in_situ",
                    "source_station_index": 0,
                    "source_station_uid": "SRC000001",
                    "path": str(first),
                    "quality_score": 1.0,
                },
                {
                    "cluster_id": 1,
                    "cluster_uid": "SED000001",
                    "resolution": "daily",
                    "quality_rank": 2,
                    "n_candidates": 2,
                    "is_top_ranked": 0,
                    "source": "HYDAT",
                    "observation_type": "in_situ",
                    "source_station_index": 1,
                    "source_station_uid": "SRC000002",
                    "path": str(second),
                    "quality_score": 0.5,
                },
            ]
        )
        quality_path = tmpdir / "quality.csv"
        out_path = tmpdir / "sidecar.csv.gz"
        quality.to_csv(quality_path, index=False)

        def fake_read_candidate_series(path):
            if Path(path).name == "first.nc":
                frame = pd.DataFrame(
                    [
                        {"date": "2000-01-01", "time": 10957.0, "Q": 1.0, "SSC": 2.0, "SSL": 3.0, "Q_flag": 0, "SSC_flag": 0, "SSL_flag": 0},
                        {"date": "2000-01-02", "time": 10958.0, "Q": 4.0, "SSC": 5.0, "SSL": 6.0, "Q_flag": 0, "SSC_flag": 0, "SSL_flag": 0},
                    ]
                )
            else:
                frame = pd.DataFrame(
                    [
                        {"date": "2000-01-01", "time": 10957.0, "Q": 1.5, "SSC": 2.5, "SSL": 3.5, "Q_flag": 0, "SSC_flag": 0, "SSL_flag": 0},
                    ]
                )
            return frame, {"Q": "m3 s-1", "SSC": "mg L-1", "SSL": "ton day-1"}, ""

        old_reader = s8_release._read_candidate_series
        old_nc4 = s8_release.nc4
        s8_release._read_candidate_series = fake_read_candidate_series
        s8_release.nc4 = object()
        try:
            built_path, row_count, detail = s8_release.build_overlap_candidates_sidecar(
                quality_order_csv=quality_path,
                source_station_catalog=pd.DataFrame(),
                out_path=out_path,
                mode="overlap-only",
            )
        finally:
            s8_release._read_candidate_series = old_reader
            s8_release.nc4 = old_nc4
        if Path(built_path) != out_path:
            raise AssertionError("sidecar path was not returned")
        if row_count != 2:
            raise AssertionError("overlap-only sidecar should contain two candidate rows")
        sidecar = pd.read_csv(out_path)
        if int(sidecar["selected_flag"].sum()) != 1:
            raise AssertionError("exactly one candidate should be selected for the overlap date")
        if set(sidecar["is_overlap"]) != {1}:
            raise AssertionError("overlap-only sidecar should contain only overlap rows")
        if "rows=2" not in detail:
            raise AssertionError("build detail should include row count")


def main():
    test_compute_pair_metrics_bias_rmse_mae()
    test_compute_pair_metrics_mape_skips_zero_ref()
    test_compute_pair_metrics_correlations_need_two_samples()
    test_classify_source_family()
    test_canonical_source_pair_stable_order()
    test_canonical_family_pair_stable_order()
    test_build_overlap_pair_records_from_mock_candidates()
    test_summarize_overlap_candidates_mock()
    test_missing_candidate_schema_diagnostic_skips_pair_metrics()
    test_build_legacy_validation_tables_mock()
    test_build_legacy_validation_tables_empty_inputs()
    test_build_overlap_candidates_sidecar_mocked_series()
    print("s10 validation helper unit tests passed.")


if __name__ == "__main__":
    main()
