#!/usr/bin/env python3
"""Tests for s5b ordering in local and LSF pipeline paths."""

import contextlib
import io
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import run_s1_s8_basin_pipeline as pipeline  # noqa: E402
import submit_s6_fast  # noqa: E402
from s5b_link_satellite_to_main_clusters import main as s5b_main  # noqa: E402


def test_local_pipeline_orders_s5b_before_satellite_export():
    original_argv = sys.argv
    try:
        sys.argv = ["run_s1_s8_basin_pipeline.py", "--local-s6", "--yes"]
        args = pipeline.parse_args()
    finally:
        sys.argv = original_argv
    specs = pipeline.build_stage_specs(args, sys.executable)
    names = [command["name"] for command in specs["s6"]["commands"]]
    assert names.index("s6_export_annual_matrix_nc") < names.index(
        "s5b_link_satellite_to_main_clusters"
    )
    assert names.index("s5b_link_satellite_to_main_clusters") < names.index(
        "s6_export_satellite_validation_to_nc"
    )
    assert pipeline._parse_steps_arg("s5b") == ["s5b"]
    assert specs["s5b"]["commands"][0]["name"] == "s5b_link_satellite_to_main_clusters"


def test_lsf_pipeline_dependencies_enforce_matrix_s5b_satellite_order():
    args = submit_s6_fast.parse_args(["--skip-climatology-export"])
    jobs = submit_s6_fast.build_jobs(args, sys.executable)
    by_step = {job["step"]: job for job in jobs}
    assert by_step["s5b"]["depends_on_steps"] == ["daily", "monthly", "annual"]
    assert by_step["satellite"]["depends_on_steps"] == ["s5b"]
    steps = [job["step"] for job in jobs]
    assert steps.index("annual") < steps.index("s5b") < steps.index("satellite")


def test_s5b_missing_input_fails_clearly():
    with tempfile.TemporaryDirectory() as tmp:
        missing = Path(tmp) / "missing-s5.csv"
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            status = s5b_main(["--s5-csv", str(missing)])
    assert status == 1
    assert "s5 input not found" in stderr.getvalue()

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        s5_csv = tmp_path / "s5.csv"
        s5_csv.write_text("station_id\n", encoding="utf-8")
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            status = s5b_main(
                ["--s5-csv", str(s5_csv), "--matrix-dir", str(tmp_path / "missing")]
            )
    assert status == 1
    assert "main matrix not found" in stderr.getvalue()


def main():
    test_local_pipeline_orders_s5b_before_satellite_export()
    test_lsf_pipeline_dependencies_enforce_matrix_s5b_satellite_order()
    test_s5b_missing_input_fails_clearly()
    print("s5b pipeline wiring tests passed")


if __name__ == "__main__":
    main()
