#!/usr/bin/env python3
"""Unit tests for Python LSF submitter helpers.

These tests do not call bsub or bjobs.
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import submit_s4_lsf  # noqa: E402
import submit_s6_fast  # noqa: E402


def test_parse_lsf_job_id():
    assert submit_s4_lsf.parse_job_id("Job <12345> is submitted to queue <normal>.") == "12345"
    assert submit_s6_fast.parse_job_id("Job <67890> is submitted.") == "67890"


def test_s4_trace_script_preserves_lsf_job_index_expansion():
    script = submit_s4_lsf.build_trace_script("/usr/bin/python3", 2, "1")
    assert 'LIVE_LOG="output/logs/s4_lsf/s4_trace.${JOB_INDEX}.live.log"' in script
    assert "export S4_SHARD_COUNT=2" in script
    assert '"${PYTHON_BIN}" s4_basin_trace_watch.py' in script


def test_s6_run_only_annual_selects_one_job():
    args = submit_s6_fast.parse_args(
        [
            "--dry-run",
            "--run-only",
            "annual",
            "--python",
            sys.executable,
        ]
    )
    jobs = submit_s6_fast.build_jobs(args, sys.executable)
    assert [job["step"] for job in jobs] == ["annual"]
    assert jobs[0]["outputs"][0].name == "s6_basin_matrix_annual.nc"


def test_s6_summary_dependency_expression_in_bsub_command():
    args = submit_s6_fast.parse_args(["--dry-run", "--python", sys.executable])
    cmd = submit_s6_fast.build_bsub_cmd(
        "s6fast_summary",
        1,
        4000,
        "echo summary",
        args,
        dep="ended(100) && ended(101)",
    )
    assert "-w" in cmd
    assert cmd[cmd.index("-w") + 1] == "ended(100) && ended(101)"


def test_wait_for_job_stops_on_done_status():
    old_status = submit_s4_lsf.bjobs_status
    calls = []

    def fake_status(job_id):
        calls.append(job_id)
        return "DONE"

    submit_s4_lsf.bjobs_status = fake_status
    try:
        assert submit_s4_lsf.wait_for_job("123", 1, "unit") == "DONE"
    finally:
        submit_s4_lsf.bjobs_status = old_status
    assert calls == ["123"]


def main():
    test_parse_lsf_job_id()
    test_s4_trace_script_preserves_lsf_job_index_expansion()
    test_s6_run_only_annual_selects_one_job()
    test_s6_summary_dependency_expression_in_bsub_command()
    test_wait_for_job_stops_on_done_status()
    print("lsf submitter tests passed")


if __name__ == "__main__":
    main()
