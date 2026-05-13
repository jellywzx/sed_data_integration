#!/usr/bin/env python3
"""
Run the scripts_basin_test basin mainline in order.

Default stage layout:
  s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7

Stage details:
  s6 = master NC + matrix NC exports + climatology NC export
  s7 = cluster GPKG + source-station GPKG + cluster-basin GPKG
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from pipeline_paths import OUTPUT_LOG_DIR


SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_R_ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = OUTPUT_R_ROOT / "scripts_basin_test" / "output"
ORGANIZED_DIR = (OUTPUT_R_ROOT / "../output_resolution_organized").resolve()
DEFAULT_MERIT_DIR = OUTPUT_R_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"
DEFAULT_LOG_FILE = OUTPUT_R_ROOT / OUTPUT_LOG_DIR / "run_s1_to_s7_basin_pipeline.log"
STAGES = ("s1", "s2", "s3", "s4", "s5", "s6", "s7")


def _quote(parts):
    return " ".join(shlex.quote(str(part)) for part in parts)


def _now_text():
    return datetime.now().isoformat(timespec="seconds")


def _write_log(log_fp, message=""):
    text = str(message)
    if not text.endswith("\n"):
        text += "\n"
    log_fp.write(text)
    log_fp.flush()


def _print_and_log(log_fp, message=""):
    print(message)
    _write_log(log_fp, message)


def _stream_command(cmd, cwd, env, log_fp):
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
    )

    try:
        if proc.stdout is not None:
            for line in proc.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log_fp.write(line)
                log_fp.flush()
    finally:
        if proc.stdout is not None:
            proc.stdout.close()

    return proc.wait()


def _python_major(executable):
    if not executable:
        return None
    try:
        proc = subprocess.run(
            [str(executable), "-c", "import sys; print(sys.version_info[0])"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    try:
        return int(proc.stdout.strip())
    except ValueError:
        return None


def resolve_python(python_arg):
    candidates = []

    def add(value):
        if value and value not in candidates:
            candidates.append(value)

    add(python_arg)
    add(os.environ.get("PYTHON_BIN", "").strip())
    add(sys.executable)
    add(shutil.which("python3"))
    add(shutil.which("python"))

    for candidate in candidates:
        if _python_major(candidate) == 3:
            return str(candidate)

    raise SystemExit(
        "Could not find a usable Python 3 interpreter. "
        "Please pass --python /path/to/python3."
    )


def _bool_env(value):
    return "1" if value else "0"


def _validate_stage_range(start_at, end_at):
    start_index = STAGES.index(start_at)
    end_index = STAGES.index(end_at)
    if start_index > end_index:
        raise SystemExit("--start-at must not be after --end-at.")
    return STAGES[start_index : end_index + 1]


def stage_outputs():
    return {
        "s1": [
            OUTPUT_DIR / "s1_verify_time_resolution_results.csv",
            OUTPUT_DIR / "s1_resolution_review_queue.csv",
            OUTPUT_DIR / "s1_resolution_review_overrides.csv",
        ],
        "s2": [ORGANIZED_DIR],
        "s3": [OUTPUT_DIR / "s3_collected_stations.csv"],
        "s4": [
            OUTPUT_DIR / "s4_upstream_basins.csv",
            OUTPUT_DIR / "s4_reported_area_check.csv",
        ],
        "s5": [
            OUTPUT_DIR / "s5_basin_clustered_stations.csv",
            OUTPUT_DIR / "s5_basin_cluster_report.csv",
        ],
        "s6": [
            OUTPUT_DIR / "s6_basin_merged_all.nc",
            OUTPUT_DIR / "s6_cluster_quality_order.csv",
            OUTPUT_DIR / "s6_matrix_by_resolution" / "s6_basin_matrix_daily.nc",
            OUTPUT_DIR / "s6_matrix_by_resolution" / "s6_basin_matrix_monthly.nc",
            OUTPUT_DIR / "s6_matrix_by_resolution" / "s6_basin_matrix_annual.nc",
        ],
        "s7": [
            OUTPUT_DIR / "s7_cluster_points.gpkg",
            OUTPUT_DIR / "s7_cluster_station_catalog.csv",
            OUTPUT_DIR / "s7_cluster_resolution_catalog.csv",
            OUTPUT_DIR / "s7_source_stations.gpkg",
            OUTPUT_DIR / "s7_source_station_resolution_catalog.csv",
            OUTPUT_DIR / "s7_cluster_basins.gpkg",
        ],
    }


def build_stage_specs(args, python_bin):
    s5_csv = OUTPUT_DIR / "s5_basin_clustered_stations.csv"
    master_nc = OUTPUT_DIR / "s6_basin_merged_all.nc"
    matrix_dir = OUTPUT_DIR / "s6_matrix_by_resolution"
    cluster_catalog = OUTPUT_DIR / "s7_cluster_resolution_catalog.csv"

    s4_env = {
        "S4_N_WORKERS": str(args.s4_workers),
        "S4_BATCH_SIZE": str(args.s4_batch_size),
        "S4_RESUME": _bool_env(not args.s4_no_resume),
        "S4_SAVE_GPKG": _bool_env(not args.s4_no_gpkg),
        "S4_MAXTASKSPERCHILD": str(args.s4_maxtasksperchild),
        "MERIT_DIR": str(Path(args.merit_dir).expanduser().resolve()),
    }

    s6_commands = [
        {
            "name": "s6_basin_merge_to_nc",
            "cmd": [
                python_bin,
                str(SCRIPT_DIR / "s6_basin_merge_to_nc.py"),
                "-i",
                str(s5_csv),
                "-o",
                str(OUTPUT_DIR / "s6_basin_merged_all.nc"),
                "--quality-order-csv",
                str(OUTPUT_DIR / "s6_cluster_quality_order.csv"),
                "-w",
                str(args.s6_workers),
            ]
            + (["--include-climatology"] if args.s6_include_climatology else []),
        },
        {
            "name": "s6_export_resolution_matrix_ncs",
            "cmd": [
                python_bin,
                str(SCRIPT_DIR / "s6_export_resolution_matrix_ncs.py"),
                "-i",
                str(s5_csv),
                "--out-dir",
                str(matrix_dir),
                "-w",
                str(args.matrix_workers),
            ],
        },
    ]
    if not args.skip_climatology_export:
        s6_commands.append(
            {
                "name": "s6_export_climatology_to_nc",
                "cmd": [
                    python_bin,
                    str(SCRIPT_DIR / "s6_export_climatology_to_nc.py"),
                    "--input-dir",
                    str(ORGANIZED_DIR / "climatology"),
                    "--output",
                    str(OUTPUT_DIR / "s6_climatology_only.nc"),
                    "--output-shp",
                    str(OUTPUT_DIR / "s6_climatology_stations.shp"),
                ],
            }
        )

    cluster_basin_cmd = [
        python_bin,
        str(SCRIPT_DIR / "s7_export_cluster_basin_shp.py"),
        "--stations",
        str(s5_csv),
        "--cluster-resolution-catalog",
        str(cluster_catalog),
        "--basin-gpkg",
        str(OUTPUT_DIR / "s4_upstream_basins.gpkg"),
        "--local-basin-gpkg",
        str(OUTPUT_DIR / "s4_local_catchments.gpkg"),
        "--out",
        str(OUTPUT_DIR / "s7_cluster_basins.gpkg"),
        "--local-out",
        str(OUTPUT_DIR / "s7_cluster_basins_local.gpkg"),
    ]
    if args.include_local_basins:
        cluster_basin_cmd.append("--include-local-basins")

    s7_commands = [
        {
            "name": "s7_export_cluster_shp",
            "cmd": [
                python_bin,
                str(SCRIPT_DIR / "s7_export_cluster_shp.py"),
                "--nc",
                str(master_nc),
                "--daily-nc",
                str(matrix_dir / "s6_basin_matrix_daily.nc"),
                "--monthly-nc",
                str(matrix_dir / "s6_basin_matrix_monthly.nc"),
                "--annual-nc",
                str(matrix_dir / "s6_basin_matrix_annual.nc"),
                "--out-gpkg",
                str(OUTPUT_DIR / "s7_cluster_points.gpkg"),
                "--out-station-catalog",
                str(OUTPUT_DIR / "s7_cluster_station_catalog.csv"),
                "--out-resolution-catalog",
                str(cluster_catalog),
            ],
        },
        {
            "name": "s7_export_source_station_shp",
            "cmd": [
                python_bin,
                str(SCRIPT_DIR / "s7_export_source_station_shp.py"),
                "--nc",
                str(master_nc),
                "--out",
                str(OUTPUT_DIR / "s7_source_stations.gpkg"),
                "--out-catalog",
                str(OUTPUT_DIR / "s7_source_station_resolution_catalog.csv"),
            ],
        },
        {
            "name": "s7_export_cluster_basin_shp",
            "cmd": cluster_basin_cmd,
        },
    ]

    return {
        "s1": {
            "label": "verify time resolution",
            "commands": [
                {
                    "name": "s1_verify_time_resolution",
                    "cmd": [python_bin, str(SCRIPT_DIR / "s1_verify_time_resolution.py")],
                    "allow_nonzero_if_outputs_exist": True,
                }
            ],
        },
        "s2": {
            "label": "reorganize qc by resolution",
            "commands": [
                {
                    "name": "s2_reorganize_qc_by_resolution",
                    "cmd": [
                        python_bin,
                        str(SCRIPT_DIR / "s2_reorganize_qc_by_resolution.py"),
                        "-j",
                        str(args.s2_workers),
                    ]
                    + (["--clear-all"] if args.s2_clear else []),
                }
            ],
        },
        "s3": {
            "label": "collect basin-mainline stations",
            "commands": [
                {
                    "name": "s3_collect_qc_stations",
                    "cmd": [
                        python_bin,
                        str(SCRIPT_DIR / "s3_collect_qc_stations.py"),
                        "--root",
                        str(OUTPUT_R_ROOT),
                        "--out",
                        "scripts_basin_test/output/s3_collected_stations.csv",
                        "-j",
                        str(args.s3_workers),
                        "--exclude-resolutions",
                        args.s3_exclude_resolutions,
                    ],
                }
            ],
        },
        "s4": {
            "label": "trace upstream basins",
            "commands": [
                {
                    "name": "s4_basin_trace_watch",
                    "cmd": [python_bin, str(SCRIPT_DIR / "s4_basin_trace_watch.py")],
                    "env": s4_env,
                }
            ],
        },
        "s5": {
            "label": "merge stations by basin cluster",
            "commands": [
                {
                    "name": "s5_basin_merge",
                    "cmd": [
                        python_bin,
                        str(SCRIPT_DIR / "s5_basin_merge.py"),
                        "--s3-csv",
                        str(OUTPUT_DIR / "s3_collected_stations.csv"),
                        "--basin-csv",
                        str(OUTPUT_DIR / "s4_upstream_basins.csv"),
                        "--out",
                        str(OUTPUT_DIR / "s5_basin_clustered_stations.csv"),
                        "--report",
                        str(OUTPUT_DIR / "s5_basin_cluster_report.csv"),
                    ],
                }
            ],
        },
        "s6": {
            "label": "build basin NetCDF outputs",
            "commands": s6_commands,
        },
        "s7": {
            "label": "export GIS sidecars",
            "commands": s7_commands,
        },
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the scripts_basin_test s1-s7 basin mainline in order."
    )
    parser.add_argument("--python", help="Python 3 interpreter used to launch each step.")
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_FILE),
        help="Combined pipeline log file path. Default: scripts_basin_test/output/logs/run_s1_to_s7_basin_pipeline.log",
    )
    parser.add_argument("--start-at", choices=STAGES, default="s1", help="First logical stage to run.")
    parser.add_argument("--end-at", choices=STAGES, default="s7", help="Last logical stage to run.")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    parser.add_argument(
        "--strict-s1",
        action="store_true",
        help="Treat a non-zero s1 exit code as fatal even if the s1 CSV was produced.",
    )

    parser.add_argument("--s2-workers", type=int, default=8, help="Worker count for s2.")
    parser.add_argument("--s2-clear", action="store_true", help="Pass --clear-all to s2.")
    parser.add_argument("--s3-workers", type=int, default=32, help="Worker count for s3.")
    parser.add_argument(
        "--s3-exclude-resolutions",
        default="climatology",
        help="Comma-separated resolutions excluded from s3. Default keeps climatology out of the basin mainline.",
    )
    parser.add_argument("--s4-workers", type=int, default=24, help="Worker count for s4 via S4_N_WORKERS.")
    parser.add_argument("--s4-batch-size", type=int, default=50, help="Batch size for s4 via S4_BATCH_SIZE.")
    parser.add_argument(
        "--s4-maxtasksperchild",
        type=int,
        default=10,
        help="maxtasksperchild for s4 worker pool.",
    )
    parser.add_argument("--s4-no-resume", action="store_true", help="Disable s4 resume mode.")
    parser.add_argument("--s4-no-gpkg", action="store_true", help="Disable s4 GPKG export.")
    parser.add_argument(
        "--merit-dir",
        default=str(DEFAULT_MERIT_DIR),
        help="MERIT Hydro directory passed to s4 via MERIT_DIR.",
    )
    parser.add_argument("--s6-workers", type=int, default=24, help="Worker count for s6_basin_merge_to_nc.py.")
    parser.add_argument("--matrix-workers", type=int, default=8, help="Worker count for s6_export_resolution_matrix_ncs.py.")
    parser.add_argument(
        "--s6-include-climatology",
        action="store_true",
        help="Pass --include-climatology to s6_basin_merge_to_nc.py.",
    )
    parser.add_argument(
        "--skip-climatology-export",
        action="store_true",
        help="Skip s6_export_climatology_to_nc.py.",
    )
    parser.add_argument(
        "--include-local-basins",
        action="store_true",
        help="Generate optional s7_cluster_basins_local.gpkg. Default: skip local basins.",
    )
    return parser.parse_args()


def _all_exist(paths):
    return all(path.exists() for path in paths)


def _review_queue_count(path):
    path = Path(path)
    if not path.is_file():
        return 0
    try:
        import pandas as pd

        df = pd.read_csv(path, keep_default_na=False)
    except Exception:
        return 0
    return int(len(df))


def main():
    args = parse_args()
    python_bin = resolve_python(args.python)
    stages = _validate_stage_range(args.start_at, args.end_at)
    outputs = stage_outputs()
    specs = build_stage_specs(args, python_bin)
    log_path = Path(args.log_file).expanduser().resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    merit_dir = Path(args.merit_dir).expanduser().resolve()
    with open(log_path, "w", encoding="utf-8") as log_fp:
        _write_log(log_fp, "===== Run started {} =====".format(_now_text()))
        _write_log(log_fp, "Command line: {}".format(_quote(sys.argv)))
        _print_and_log(log_fp, "scripts_basin_test root: {}".format(SCRIPT_DIR))
        _print_and_log(log_fp, "Output_r root:           {}".format(OUTPUT_R_ROOT))
        _print_and_log(log_fp, "Python:                  {}".format(python_bin))
        _print_and_log(log_fp, "MERIT dir:               {}".format(merit_dir))
        _print_and_log(log_fp, "Stages:                  {}".format(" -> ".join(stages)))
        _print_and_log(log_fp, "Combined log:            {}".format(log_path))
        _print_and_log(log_fp, "")

        pipeline_start = time.time()
        summary = []

        for stage in stages:
            spec = specs[stage]
            stage_header = "=== {}: {} ===".format(stage, spec["label"])
            _print_and_log(log_fp, stage_header)
            _write_log(log_fp, "[{}] stage_start {}".format(stage, _now_text()))

            if args.dry_run:
                for command in spec["commands"]:
                    _print_and_log(log_fp, _quote(command["cmd"]))
                summary.append((stage, "dry-run", 0.0))
                _print_and_log(log_fp, "")
                continue

            stage_start = time.time()
            for command in spec["commands"]:
                command_text = _quote(command["cmd"])
                _print_and_log(log_fp, command_text)
                cmd_env = os.environ.copy()
                cmd_env["OUTPUT_R_ROOT"] = str(OUTPUT_R_ROOT)
                cmd_env["PYTHONUNBUFFERED"] = "1"
                for key, value in command.get("env", {}).items():
                    cmd_env[key] = str(value)

                _write_log(
                    log_fp,
                    "[{}] command_start {} | {}".format(stage, _now_text(), command["name"]),
                )
                try:
                    returncode = _stream_command(
                        command["cmd"],
                        cwd=OUTPUT_R_ROOT,
                        env=cmd_env,
                        log_fp=log_fp,
                    )
                except OSError as exc:
                    message = "Failed to launch {}: {}".format(command["name"], exc)
                    print(message, file=sys.stderr)
                    _write_log(log_fp, message)
                    return 1

                _write_log(
                    log_fp,
                    "[{}] command_end {} | {} | returncode={}".format(
                        stage, _now_text(), command["name"], returncode
                    ),
                )

                if returncode == 0:
                    continue

                if (
                    stage == "s1"
                    and not args.strict_s1
                    and command.get("allow_nonzero_if_outputs_exist")
                    and _all_exist(outputs["s1"])
                ):
                    _print_and_log(
                        log_fp,
                        "s1 returned {} but produced {}.".format(returncode, outputs["s1"][0]),
                    )
                    _print_and_log(
                        log_fp,
                        "Continuing because s1 commonly uses a non-zero exit to report mismatches.",
                    )
                    continue

                message = "Command failed in stage {} with exit code {}: {}".format(
                    stage, returncode, command["name"]
                )
                print(message, file=sys.stderr)
                _write_log(log_fp, message)
                return 1

            if not _all_exist(outputs[stage]):
                _write_log(log_fp, "[{}] missing expected outputs after stage".format(stage))
                print("Stage {} finished but expected outputs are missing:".format(stage), file=sys.stderr)
                for path in outputs[stage]:
                    if not path.exists():
                        missing_line = "  - {}".format(path)
                        print(missing_line, file=sys.stderr)
                        _write_log(log_fp, missing_line)
                return 1

            if stage == "s1":
                review_queue_path = outputs["s1"][1]
                unresolved_count = _review_queue_count(review_queue_path)
                if unresolved_count > 0:
                    message = (
                        "Stage s1 produced {} unresolved review rows in {}. "
                        "Resolve them via s1_resolution_review_overrides.csv before running s2."
                    ).format(unresolved_count, review_queue_path)
                    print(message, file=sys.stderr)
                    _write_log(log_fp, message)
                    return 1

            stage_elapsed = time.time() - stage_start
            summary.append((stage, "ok", stage_elapsed))
            _print_and_log(log_fp, "Stage {} finished in {:.1f}s".format(stage, stage_elapsed))
            _write_log(log_fp, "[{}] stage_end {} | elapsed_s={:.1f}".format(stage, _now_text(), stage_elapsed))
            _print_and_log(log_fp, "")

        total_elapsed = time.time() - pipeline_start
        _print_and_log(log_fp, "Pipeline summary:")
        for stage, status, elapsed in summary:
            _print_and_log(log_fp, "  {:>2}  {:<8} {:.1f}s".format(stage, status, elapsed))
        _print_and_log(log_fp, "Total elapsed: {:.1f}s".format(total_elapsed))
        _print_and_log(log_fp, "Finished through {}.".format(stages[-1]))
        _write_log(log_fp, "===== Run finished {} =====".format(_now_text()))
        return 0


if __name__ == "__main__":
    sys.exit(main())
