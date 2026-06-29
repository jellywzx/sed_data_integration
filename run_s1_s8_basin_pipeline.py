#!/usr/bin/env python3
"""
Run the scripts_basin_test basin mainline in order.

Default stage layout:
  s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7 -> s8

Stage details:
  s6 = master NC + matrix NC exports + climatology NC export + satellite validation NC export
  s7 = cluster GPKG + source-station GPKG + cluster-basin GPKG
  s8 = release package + catalogs + validation report

Typical usage:
  # Show all available command-line options
  python run_s1_s8_basin_pipeline.py --help

  # Run the whole pipeline with the built-in defaults
  python run_s1_s8_basin_pipeline.py

  # Run with a YAML configuration file (see pipeline_config.yaml for all options)
  python run_s1_s8_basin_pipeline.py --config-file pipeline_config.yaml
  python run_s1_s8_basin_pipeline.py -c pipeline_config.yaml

  # Run with YAML config, preview only (dry-run), skip confirmation
  python run_s1_s8_basin_pipeline.py -c pipeline_config.yaml --dry-run --yes

  # CLI flags override the YAML config file values
  python run_s1_s8_basin_pipeline.py -c pipeline_config.yaml --steps s1,s2,s3

  # Run a continuous stage range
  python run_s1_s8_basin_pipeline.py --start-at s3 --end-at s6

  # Run explicit stages only; useful for reruns or skipping finished stages
  python run_s1_s8_basin_pipeline.py --steps s4,s5,s8

  # Preview the commands without executing them
  python run_s1_s8_basin_pipeline.py --steps s6,s7 --dry-run

Notes:
  1. If --steps is provided, it takes priority and --start-at/--end-at are ignored.
  2. The BUILTIN_* constants below define the default runtime behavior for routine runs.
  3. Command-line options override the BUILTIN_* defaults for the current invocation only.
"""

import argparse
import os
import shlex
import shutil
import socket
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
DEFAULT_LOG_FILE = OUTPUT_R_ROOT / OUTPUT_LOG_DIR / "run_s1_to_s8_basin_pipeline.log"
STAGES = ("s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8")

# ---- Built-in runtime defaults -------------------------------------------------
# These constants are the script's everyday defaults.
# For stable long-term preferences, edit them here.
# For one-off runs, prefer passing CLI arguments so the change only affects
# the current invocation.
BUILTIN_START_AT = "s1"
BUILTIN_END_AT = "s8"
BUILTIN_STRICT_S1 = False

BUILTIN_S2_WORKERS = 8
BUILTIN_S2_CLEAR = False
BUILTIN_S2_DATASET = ""
BUILTIN_S3_WORKERS = 32
BUILTIN_S3_EXCLUDE_RESOLUTIONS = "climatology"

BUILTIN_S4_WORKERS = 24
BUILTIN_S4_BATCH_SIZE = 50
BUILTIN_S4_MAXTASKSPERCHILD = 10
BUILTIN_S4_RESUME = True
BUILTIN_S4_SAVE_GPKG = True
BUILTIN_S4_ARRAY_SIZE = 16
BUILTIN_MERIT_DIR = DEFAULT_MERIT_DIR

BUILTIN_S6_WORKERS = 24
BUILTIN_MATRIX_WORKERS = None
BUILTIN_MATRIX_RESOLUTION_WORKERS = None
BUILTIN_S6_INCLUDE_CLIMATOLOGY = False
BUILTIN_SKIP_CLIMATOLOGY_EXPORT = False

BUILTIN_INCLUDE_LOCAL_BASINS = False

BUILTIN_S8_LINK_MODE = "hardlink"
BUILTIN_S8_SKIP_GPKG = False
BUILTIN_S8_INCLUDE_BASIN_POLYGONS = True
BUILTIN_S8_SKIP_VALIDATION = False
BUILTIN_S8_FORCE = True
BUILTIN_S8_MINIMAL_MATRIX_WORKERS = 3
BUILTIN_S8_MINIMAL_COMPRESSION = 4
BUILTIN_S8_SKIP_MINIMAL_CLIMATOLOGY = False
BUILTIN_S8_SKIP_MINIMAL_SATELLITE = False
BUILTIN_CLUSTER_POLL_SECONDS = 60


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


def _parse_steps_arg(steps_text):
    selected = []
    for raw in str(steps_text).split(","):
        step = raw.strip().lower()
        if not step:
            continue
        if step not in STAGES:
            raise SystemExit(
                "Invalid step '{}' in --steps. Allowed: {}".format(
                    step, ", ".join(STAGES)
                )
            )
        if step not in selected:
            selected.append(step)
    if not selected:
        raise SystemExit("--steps was provided but no valid step was parsed.")
    return selected


def _split_multi_value(value):
    """Normalize list-like config/CLI values into a flat argument list."""
    if value is None:
        return []
    raw_values = value if isinstance(value, (list, tuple, set)) else [value]
    result = []
    for raw in raw_values:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        for token in shlex.split(text):
            for piece in token.split(","):
                piece = piece.strip()
                if piece:
                    result.append(piece)
    return result


def _optional_multi_arg(flag, value):
    values = _split_multi_value(value)
    return [flag] + values if values else []


def _resolve_selected_stages(args):
    if args.steps:
        return _parse_steps_arg(args.steps)
    return _validate_stage_range(args.start_at, args.end_at)


def stage_outputs():
    release_dir = OUTPUT_DIR / "sed_reference_release"
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
            OUTPUT_DIR / "s6_satellite_validation_only.nc",
            OUTPUT_DIR / "s6_satellite_validation_catalog.csv",
        ],
        "s7": [
            OUTPUT_DIR / "s7_cluster_points.gpkg",
            OUTPUT_DIR / "s7_cluster_station_catalog.csv",
            OUTPUT_DIR / "s7_cluster_resolution_catalog.csv",
            OUTPUT_DIR / "s7_source_stations.gpkg",
            OUTPUT_DIR / "s7_source_station_resolution_catalog.csv",
            OUTPUT_DIR / "s7_cluster_basins.gpkg",
        ],
        "s8": [
            release_dir / "sed_reference_master.nc",
            release_dir / "sed_reference_timeseries_daily.nc",
            release_dir / "sed_reference_timeseries_monthly.nc",
            release_dir / "sed_reference_timeseries_annual.nc",
            release_dir / "sed_reference_climatology.nc",
            release_dir / "sed_reference_satellite.nc",
            release_dir / "station_catalog.csv",
            release_dir / "source_station_catalog.csv",
            release_dir / "source_dataset_catalog.csv",
            release_dir / "satellite_catalog.csv",
            release_dir / "release_validation_report.csv",
            release_dir / "release_inventory.csv",
            release_dir / "README.md",
            OUTPUT_DIR / "sed_reference_release_minimal" / "minimal_release_validation_report.csv",
            OUTPUT_DIR / "sed_reference_release_minimal" / "release_inventory.csv",
            OUTPUT_DIR / "sed_reference_release_minimal" / "README.md",
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
    s4_cluster_command = {
        "name": "submit_s4_lsf",
        "cmd": [
            python_bin,
            str(SCRIPT_DIR / "submit_s4_lsf.py"),
            "--array-size",
            str(args.s4_array_size),
            "--python",
            python_bin,
            "--wait",
            "--poll-seconds",
            str(args.cluster_poll_seconds),
        ],
        "env": s4_env,
    }
    s4_local_command = {
        "name": "s4_basin_trace_watch",
        "cmd": [python_bin, str(SCRIPT_DIR / "s4_basin_trace_watch.py")],
        "env": s4_env,
    }

    def build_matrix_cmd(script_name):
        cmd = [
            python_bin,
            str(SCRIPT_DIR / script_name),
            "-i",
            str(s5_csv),
            "--out-dir",
            str(matrix_dir),
        ]
        if matrix_workers is not None:
            cmd += ["--workers", str(matrix_workers)]
        if matrix_resolution_workers is not None:
            cmd += ["--resolution-workers", str(matrix_resolution_workers)]
        return cmd

    host = str(socket.gethostname() or "").strip().split(".")[0].lower()
    matrix_workers = args.matrix_workers
    matrix_resolution_workers = args.matrix_resolution_workers
    if host == "node113":
        if matrix_workers is None:
            matrix_workers = 32
        if matrix_resolution_workers is None:
            matrix_resolution_workers = 1

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
            "name": "s6_export_daily_matrix_nc",
            "cmd": build_matrix_cmd("s6_export_daily_matrix_nc.py"),
        },
        {
            "name": "s6_export_monthly_matrix_nc",
            "cmd": build_matrix_cmd("s6_export_monthly_matrix_nc.py"),
        },
        {
            "name": "s6_export_annual_matrix_nc",
            "cmd": build_matrix_cmd("s6_export_annual_matrix_nc.py"),
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
    s6_commands.append(
        {
            "name": "s6_export_satellite_validation_to_nc",
            "cmd": [
                python_bin,
                str(SCRIPT_DIR / "s6_export_satellite_validation_to_nc.py"),
            ],
        }
    )
    s6_cluster_cmd = [
        python_bin,
        str(SCRIPT_DIR / "submit_s6_fast.py"),
        "--python",
        python_bin,
        "--wait",
        "--poll-seconds",
        str(args.cluster_poll_seconds),
        "--s6-workers",
        str(args.s6_workers),
    ]
    if matrix_workers is not None:
        s6_cluster_cmd += ["--matrix-workers", str(matrix_workers)]
    if matrix_resolution_workers is not None:
        s6_cluster_cmd += ["--matrix-resolution-workers", str(matrix_resolution_workers)]
    if args.s6_include_climatology:
        s6_cluster_cmd.append("--s6-include-climatology")
    if args.skip_climatology_export:
        s6_cluster_cmd.append("--skip-climatology-export")
    s6_cluster_commands = [
        {
            "name": "submit_s6_fast",
            "cmd": s6_cluster_cmd,
            "env": {"RUN_ONLY": ""},
        }
    ]

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

    s8_cmd = [
        python_bin,
        str(SCRIPT_DIR / "s8_publish_reference_dataset.py"),
        "--link-mode",
        str(args.s8_link_mode),
    ]
    if args.s8_skip_gpkg:
        s8_cmd.append("--skip-gpkg")
    if args.s8_include_basin_polygons:
        s8_cmd.append("--include-basin-polygons")
    if args.s8_skip_validation:
        s8_cmd.append("--skip-validation")
    if args.s8_force:
        s8_cmd.append("--force")

    s8_commands = [
        {
            "name": "s8_publish_reference_dataset",
            "cmd": s8_cmd,
        },
        {
            "name": "s8_publish_minimal_release_package",
            "cmd": [
                python_bin,
                str(SCRIPT_DIR / "s8_publish_minimal_release_package.py"),
                "--release-dir",
                str(OUTPUT_DIR / "sed_reference_release"),
                "--matrix-workers",
                str(args.s8_minimal_matrix_workers),
                "--compression-level",
                str(args.s8_minimal_compression),
            ]
            + (["--skip-climatology"] if args.s8_skip_minimal_climatology else [])
            + (["--skip-satellite"] if args.s8_skip_minimal_satellite else [])
            + (["--force"] if args.s8_force else [])
            + (["--dry-run"] if args.dry_run else []),
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
                    + _optional_multi_arg("--dataset", args.s2_dataset)
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
            "commands": [s4_local_command] if args.local_s4 else [s4_cluster_command],
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
            "commands": s6_commands if args.local_s6 else s6_cluster_commands,
        },
        "s7": {
            "label": "export GIS sidecars",
            "commands": s7_commands,
        },
        "s8": {
            "label": "publish reference release package",
            "commands": s8_commands,
        },
    }


def parse_args(defaults=None):
    parser = argparse.ArgumentParser(
        description="Run the scripts_basin_test s1-s8 basin mainline in order."
    )
    parser.add_argument("--python", help="Python 3 interpreter used to launch each step.")
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_FILE),
        help="Combined pipeline log file path. Default: scripts_basin_test/output/logs/run_s1_to_s8_basin_pipeline.log",
    )
    parser.add_argument("--start-at", choices=STAGES, default=BUILTIN_START_AT, help="First logical stage to run.")
    parser.add_argument("--end-at", choices=STAGES, default=BUILTIN_END_AT, help="Last logical stage to run.")
    parser.add_argument(
        "--steps",
        help=(
            "Comma-separated explicit stage list, e.g. s1,s2,s5 or s4,s6,s8. "
            "When set, --start-at/--end-at are ignored."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    parser.add_argument(
        "--config-file", "-c",
        default="",
        help="JSON config file path. Sets both CLI arguments and environment variables (see --dump-config-template).",
    )
    parser.add_argument(
        "--dump-config-template",
        action="store_true",
        default=False,
        help="Print a template JSON config file and exit.",
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        default=False,
        help="Skip the interactive configuration confirmation prompt.",
    )
    parser.add_argument(
        "--cluster-poll-seconds",
        type=int,
        default=BUILTIN_CLUSTER_POLL_SECONDS,
        help="Polling interval for Python LSF submitters used by s4/s6.",
    )
    parser.add_argument(
        "--local-s4",
        action="store_true",
        help="Run s4_basin_trace_watch.py locally instead of submitting S4 to LSF.",
    )
    parser.add_argument(
        "--local-s6",
        action="store_true",
        help="Run S6 component scripts locally instead of submitting S6 to LSF.",
    )
    parser.add_argument(
        "--strict-s1",
        action="store_true",
        default=BUILTIN_STRICT_S1,
        help="Treat a non-zero s1 exit code as fatal even if the s1 CSV was produced.",
    )

    parser.add_argument("--s2-workers", type=int, default=BUILTIN_S2_WORKERS, help="Worker count for s2.")
    parser.add_argument(
        "--s2-clear",
        action="store_true",
        default=BUILTIN_S2_CLEAR,
        help="Pass --clear-all to s2.",
    )
    parser.add_argument(
        "--s2-dataset",
        nargs="+",
        default=BUILTIN_S2_DATASET,
        help=(
            "Limit s2 to selected dataset(s). Supports multiple values and comma-separated values, "
            "for example: --s2-dataset Huanghe GloRiSe/SS."
        ),
    )
    parser.add_argument("--s3-workers", type=int, default=BUILTIN_S3_WORKERS, help="Worker count for s3.")
    parser.add_argument(
        "--s3-exclude-resolutions",
        default=BUILTIN_S3_EXCLUDE_RESOLUTIONS,
        help="Comma-separated resolutions excluded from s3. Default keeps climatology out of the basin mainline.",
    )
    parser.add_argument("--s4-workers", type=int, default=BUILTIN_S4_WORKERS, help="Worker count for s4 via S4_N_WORKERS.")
    parser.add_argument(
        "--s4-array-size",
        type=int,
        default=BUILTIN_S4_ARRAY_SIZE,
        help="LSF array size for submit_s4_lsf.py when S4 runs in cluster mode.",
    )
    parser.add_argument("--s4-batch-size", type=int, default=BUILTIN_S4_BATCH_SIZE, help="Batch size for s4 via S4_BATCH_SIZE.")
    parser.add_argument(
        "--s4-maxtasksperchild",
        type=int,
        default=BUILTIN_S4_MAXTASKSPERCHILD,
        help="maxtasksperchild for s4 worker pool.",
    )
    parser.add_argument(
        "--s4-no-resume",
        action="store_true",
        default=not BUILTIN_S4_RESUME,
        help="Disable s4 resume mode.",
    )
    parser.add_argument(
        "--s4-no-gpkg",
        action="store_true",
        default=not BUILTIN_S4_SAVE_GPKG,
        help="Disable s4 GPKG export.",
    )
    parser.add_argument(
        "--merit-dir",
        default=str(BUILTIN_MERIT_DIR),
        help="MERIT Hydro directory passed to s4 via MERIT_DIR.",
    )
    parser.add_argument("--s6-workers", type=int, default=BUILTIN_S6_WORKERS, help="Worker count for s6_basin_merge_to_nc.py.")
    parser.add_argument(
        "--matrix-workers",
        type=int,
        default=BUILTIN_MATRIX_WORKERS,
        help="Optional override of total worker budget for the per-resolution s6 matrix export scripts.",
    )
    parser.add_argument(
        "--matrix-resolution-workers",
        type=int,
        default=BUILTIN_MATRIX_RESOLUTION_WORKERS,
        help="Optional override passed through to the per-resolution s6 matrix export scripts.",
    )
    parser.add_argument(
        "--s6-include-climatology",
        action="store_true",
        default=BUILTIN_S6_INCLUDE_CLIMATOLOGY,
        help="Pass --include-climatology to s6_basin_merge_to_nc.py.",
    )
    parser.add_argument(
        "--skip-climatology-export",
        action="store_true",
        default=BUILTIN_SKIP_CLIMATOLOGY_EXPORT,
        help="Skip s6_export_climatology_to_nc.py.",
    )
    parser.add_argument(
        "--include-local-basins",
        action="store_true",
        default=BUILTIN_INCLUDE_LOCAL_BASINS,
        help="Generate optional s7_cluster_basins_local.gpkg. Default: skip local basins.",
    )
    parser.add_argument(
        "--s8-link-mode",
        choices=("hardlink", "symlink", "copy"),
        default=BUILTIN_S8_LINK_MODE,
        help="How s8 materializes canonical files in sed_reference_release.",
    )
    parser.add_argument(
        "--s8-skip-gpkg",
        action="store_true",
        default=BUILTIN_S8_SKIP_GPKG,
        help="Pass --skip-gpkg to s8_publish_reference_dataset.py.",
    )
    parser.add_argument(
        "--s8-no-basin-polygons",
        action="store_false",
        dest="s8_include_basin_polygons",
        default=BUILTIN_S8_INCLUDE_BASIN_POLYGONS,
        help="Do not pass --include-basin-polygons to s8_publish_reference_dataset.py.",
    )
    parser.add_argument(
        "--s8-skip-validation",
        action="store_true",
        default=BUILTIN_S8_SKIP_VALIDATION,
        help="Pass --skip-validation to s8_publish_reference_dataset.py.",
    )
    parser.add_argument(
        "--s8-no-force",
        action="store_false",
        dest="s8_force",
        default=BUILTIN_S8_FORCE,
        help="Do not pass --force to s8_publish_reference_dataset.py.",
    )
    parser.add_argument(
        "--s8-minimal-matrix-workers",
        type=int,
        default=BUILTIN_S8_MINIMAL_MATRIX_WORKERS,
        help="Parallel workers for minimal matrix NetCDF copies (default: 3).",
    )
    parser.add_argument(
        "--s8-minimal-compression",
        type=int,
        default=BUILTIN_S8_MINIMAL_COMPRESSION,
        help="NetCDF compression level for minimal matrix files (0-9, default: 4).",
    )
    parser.add_argument(
        "--s8-skip-minimal-climatology",
        action="store_true",
        default=BUILTIN_S8_SKIP_MINIMAL_CLIMATOLOGY,
        help="Skip building the climatology extension package in s8 minimal release.",
    )
    parser.add_argument(
        "--s8-skip-minimal-satellite",
        action="store_true",
        default=BUILTIN_S8_SKIP_MINIMAL_SATELLITE,
        help="Skip building the satellite extension package in s8 minimal release.",
    )
    if defaults:
        parser.set_defaults(**defaults)
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


def _confirm_config(args, stages, python_bin):
    """Print the resolved configuration and ask the user to confirm before proceeding."""
    s4_env = {
        "S4_N_WORKERS": str(args.s4_workers),
        "S4_BATCH_SIZE": str(args.s4_batch_size),
        "S4_RESUME": "true" if not args.s4_no_resume else "false",
        "S4_SAVE_GPKG": "true" if not args.s4_no_gpkg else "false",
        "S4_MAXTASKSPERCHILD": str(args.s4_maxtasksperchild),
        "MERIT_DIR": str(Path(args.merit_dir).expanduser().resolve()),
    }
    if args.s4_array_size != 16:
        s4_env["S4_ARRAY_SIZE"] = str(args.s4_array_size)

    # ── 读取仅能通过 export 设置的环境变量 ──
    def _env(name, default=""):
        return os.environ.get(name, default)

    # s6 任务筛选
    s6_run_only = _env("RUN_ONLY")
    # LSF 队列设置
    s4_queue = _env("S4_QUEUE", "normal")
    s4_ncores = _env("S4_NCORES", "24")
    s4_mem = _env("S4_MEM", "120G")
    s4_ptile = _env("S4_PTILE", "24")
    s4_gpkg_exclude_sat = _env("S4_GPKG_EXCLUDE_SATELLITE", "1")
    lsf_queue = _env("LSF_QUEUE")
    lsf_project = _env("LSF_PROJECT")
    lsf_extra = _env("LSF_EXTRA")
    job_tag = _env("JOB_TAG", "s6fast")
    # s6 各子步骤 worker 数
    merge_workers = _env("MERGE_WORKERS", "40")
    merge_metadata_workers = _env("MERGE_METADATA_WORKERS", "32")
    da_workers = _env("DAILY_WORKERS", "40")
    mo_workers = _env("MONTHLY_WORKERS", "20")
    an_workers = _env("ANNUAL_WORKERS", "4")
    # s6 各子步骤 LSF 资源
    merge_n = _env("MERGE_N", "48")
    merge_mem = _env("MERGE_MEM_MB", "240000")
    clim_n = _env("CLIM_N", "4")
    clim_mem = _env("CLIM_MEM_MB", "16000")
    satval_n = _env("SATVAL_N", "24")
    satval_mem = _env("SATVAL_MEM_MB", "64000")

    # ── CLI 来源的配置项 ──
    env_lines = [
        ("OUTPUT_R_ROOT", str(OUTPUT_R_ROOT)),
        ("PYTHON_BIN", python_bin),
        ("Stages", " -> ".join(stages)),
        ("MERIT_DIR", s4_env["MERIT_DIR"]),
        ("S4_N_WORKERS", s4_env["S4_N_WORKERS"]),
        ("S4_BATCH_SIZE", s4_env["S4_BATCH_SIZE"]),
        ("S4_RESUME", s4_env["S4_RESUME"]),
        ("S4_SAVE_GPKG", s4_env["S4_SAVE_GPKG"]),
        ("S4_MAXTASKSPERCHILD", s4_env["S4_MAXTASKSPERCHILD"]),
        ("S4_ARRAY_SIZE", s4_env.get("S4_ARRAY_SIZE", "16 (default)")),
        ("S4 mode", "local" if args.local_s4 else "cluster (LSF array)"),
        ("S6 mode", "local" if args.local_s6 else "cluster (LSF)"),
        ("Include climatology in s6 master", str(args.s6_include_climatology)),
        ("Skip climatology export", str(args.skip_climatology_export)),
        ("s3 exclude resolutions", args.s3_exclude_resolutions),
        ("s8 force overwrite", str(args.s8_force)),
        ("s8 skip validation", str(args.s8_skip_validation)),
        ("s8 include basin polygons", str(args.s8_include_basin_polygons)),
        ("s2 workers", str(args.s2_workers)),
        ("s2 clear", str(args.s2_clear)),
        ("s2 dataset filter", ", ".join(_split_multi_value(args.s2_dataset)) or "all"),
        ("s3 workers", str(args.s3_workers)),
        ("s6 workers", str(args.s6_workers)),
        ("matrix workers", str(args.matrix_workers)),
        ("matrix resolution workers", str(args.matrix_resolution_workers)),
        ("s8 link mode", str(args.s8_link_mode)),
        ("s8 minimal matrix workers", str(args.s8_minimal_matrix_workers)),
        ("s8 minimal compression", str(args.s8_minimal_compression)),
        ("s8 skip minimal climatology", str(args.s8_skip_minimal_climatology)),
        ("s8 skip minimal satellite", str(args.s8_skip_minimal_satellite)),
        ("s8 skip gpkg", str(args.s8_skip_gpkg)),
        ("include local basins", str(args.include_local_basins)),
        ("strict s1", str(args.strict_s1)),
        ("cluster poll seconds", str(args.cluster_poll_seconds)),
        ("dry run", str(args.dry_run)),
        ("Log file", str(args.log_file)),
    ]

    # ── 仅能通过 export 设置的环境变量 ──
    env_only_lines = [
        ("S6: RUN_ONLY", s6_run_only if s6_run_only else "all (no filter)"),
        ("S6: JOB_TAG", job_tag),
        ("S6: MERGE_WORKERS", merge_workers),
        ("S6: MERGE_METADATA_WORKERS", merge_metadata_workers),
        ("S6: DAILY_WORKERS", da_workers),
        ("S6: MONTHLY_WORKERS", mo_workers),
        ("S6: ANNUAL_WORKERS", an_workers),
        ("S6: MERGE_N / MERGE_MEM_MB", "{} / {}".format(merge_n, merge_mem)),
        ("S6: CLIM_N / CLIM_MEM_MB", "{} / {}".format(clim_n, clim_mem)),
        ("S6: SATVAL_N / SATVAL_MEM_MB", "{} / {}".format(satval_n, satval_mem)),
        ("S4: QUEUE / NCORES / MEM / PTILE", "{} / {} / {} / {}".format(s4_queue, s4_ncores, s4_mem, s4_ptile)),
        ("S4: GPKG_EXCLUDE_SATELLITE", s4_gpkg_exclude_sat),
        ("LSF: QUEUE / PROJECT / EXTRA", "{} / {} / {}".format(lsf_queue if lsf_queue else "(default)", lsf_project if lsf_project else "(empty)", lsf_extra if lsf_extra else "(empty)")),
    ]

    hints = [
        ("Stages", "--steps s1,s2,s3,s5,s6  or  --start-at s4 --end-at s8"),
        ("MERIT_DIR", "--merit-dir /path/to/MERIT_Hydro"),
        ("S4_N_WORKERS", "--s4-workers N"),
        ("S4_BATCH_SIZE", "--s4-batch-size N"),
        ("S4_RESUME", "--s4-no-resume (to disable resume)"),
        ("S4_SAVE_GPKG", "--s4-no-gpkg (to disable GPKG export)"),
        ("S4 mode", "--local-s4 (run locally instead of LSF)"),
        ("S6 mode", "--local-s6 (run locally instead of LSF)"),
        ("Include climatology in s6 master", "--s6-include-climatology"),
        ("Skip climatology export", "--skip-climatology-export"),
        ("s8 skip validation", "--s8-skip-validation"),
        ("[env] S6: RUN_ONLY", "export RUN_ONLY=merge,matrix_daily,..."),
        ("[env] S6: JOB_TAG", "export JOB_TAG=my_tag"),
        ("[env] S6: DAILY_WORKERS / ...", "export DAILY_WORKERS=40"),
        ("[env] S4: S4_QUEUE / S4_NCORES / ...", "export S4_QUEUE=normal"),
        ("[env] LSF: LSF_QUEUE / LSF_PROJECT", "export LSF_QUEUE=normal"),
        ("s2 workers", "--s2-workers N"),
        ("s2 clear", "--s2-clear (clear old output before reorganizing)"),
        ("s2 dataset filter", "--s2-dataset Huanghe GloRiSe/SS"),
        ("s3 workers", "--s3-workers N"),
        ("s3 exclude resolutions", "--s3-exclude-resolutions"),
        ("s4 array size", "--s4-array-size N"),
        ("s4 maxtasksperchild", "--s4-maxtasksperchild N"),
        ("s6 workers", "--s6-workers N"),
        ("matrix workers", "--matrix-workers N"),
        ("matrix resolution workers", "--matrix-resolution-workers N"),
        ("s8 link mode", "--s8-link-mode hardlink|symlink|copy"),
        ("s8 skip gpkg", "--s8-skip-gpkg"),
        ("s8 minimal matrix workers", "--s8-minimal-matrix-workers N"),
        ("s8 minimal compression", "--s8-minimal-compression N"),
        ("s8 skip minimal climatology", "--s8-skip-minimal-climatology"),
        ("s8 skip minimal satellite", "--s8-skip-minimal-satellite"),
        ("include local basins", "--include-local-basins"),
        ("strict s1", "--strict-s1"),
        ("cluster poll seconds", "--cluster-poll-seconds N"),
        ("dry run", "--dry-run"),
        ("s8 force overwrite", "--s8-no-force (to disable force)"),
        ("s8 include basin polygons", "--s8-no-basin-polygons (to exclude basin polygons)"),
        ("Python", "--python /path/to/python3"),
        ("Log file", "--log-file PATH"),
    ]

    separator = "=" * 64
    print(separator)
    print("Pipeline Configuration Summary".center(64))
    print(separator)
    print("  >>> CLI-configured settings <<<")
    print(separator)
    for key, value in env_lines:
        print(f"  {key:<30s} {value}")
    print(separator)
    print("  >>> Env-only settings (export VAR=...) <<<")
    for key, value in env_only_lines:
        print(f"  {key:<30s} {value}")
    print(separator)
    print("How to modify each setting:")
    for key, hint in hints:
        print(f"  {key:<30s} {hint}")
    print(separator)

    if args.yes:
        return True

    try:
        response = input("Proceed with this configuration? [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        response = "n"
    if response and response not in ("y", "yes", ""):
        print("Aborted by user.", file=sys.stderr)
        return False
    return True


_CONFIG_FIELDS = [
    # (dest_name, argparse_flag)
    ("steps", "--steps"),
    ("start_at", "--start-at"),
    ("end_at", "--end-at"),
    ("dry_run", "--dry-run"),
    ("yes", "--yes"),
    ("config_file", "--config-file"),
    ("local_s4", "--local-s4"),
    ("local_s6", "--local-s6"),
    ("strict_s1", "--strict-s1"),
    ("s2_workers", "--s2-workers"),
    ("s2_clear", "--s2-clear"),
    ("s2_dataset", "--s2-dataset"),
    ("s3_workers", "--s3-workers"),
    ("s3_exclude_resolutions", "--s3-exclude-resolutions"),
    ("s4_workers", "--s4-workers"),
    ("s4_array_size", "--s4-array-size"),
    ("s4_batch_size", "--s4-batch-size"),
    ("s4_maxtasksperchild", "--s4-maxtasksperchild"),
    ("s4_no_resume", "--s4-no-resume"),
    ("s4_no_gpkg", "--s4-no-gpkg"),
    ("merit_dir", "--merit-dir"),
    ("s6_workers", "--s6-workers"),
    ("s6_include_climatology", "--s6-include-climatology"),
    ("skip_climatology_export", "--skip-climatology-export"),
    ("matrix_workers", "--matrix-workers"),
    ("matrix_resolution_workers", "--matrix-resolution-workers"),
    ("cluster_poll_seconds", "--cluster-poll-seconds"),
    ("include_local_basins", "--include-local-basins"),
    ("s8_link_mode", "--s8-link-mode"),
    ("s8_skip_gpkg", "--s8-skip-gpkg"),
    ("s8_skip_validation", "--s8-skip-validation"),
    ("s8_include_basin_polygons", "--s8-include-basin-polygons"),
    ("s8_force", "--s8-force"),
    ("s8_minimal_matrix_workers", "--s8-minimal-matrix-workers"),
    ("s8_minimal_compression", "--s8-minimal-compression"),
    ("s8_skip_minimal_climatology", "--s8-skip-minimal-climatology"),
    ("s8_skip_minimal_satellite", "--s8-skip-minimal-satellite"),
    ("python", "--python"),
    ("log_file", "--log-file"),
]


def _write_config_template():
    """Print a YAML config template with all supported keys."""
    import sys
    yaml_text = r"""# ============================================================
# Pipeline configuration file for run_s1_s8_basin_pipeline.py
# ============================================================
#
# Usage:
#   cd Output_r/scripts_basin_test
#   python3 run_s1_s8_basin_pipeline.py --config-file pipeline_config.yaml
#   python3 run_s1_s8_basin_pipeline.py -c pipeline_config.yaml --dry-run --yes
#
# CLI flags override config file values:
#   python3 run_s1_s8_basin_pipeline.py -c pipeline_config.yaml --steps s1,s2,s3
#
# Generate this template:
#   python3 run_s1_s8_basin_pipeline.py --dump-config-template
# ============================================================

cli:
  # 要运行的阶段列表（逗号分隔），留空则使用 start_at/end_at
  # 可选值: s1, s2, s3, s4, s5, s6, s7, s8
  steps: ""

  # 起始 / 结束阶段（steps 为空时生效）
  start_at: s1
  end_at: s8

  # 试跑模式：只打印命令，不实际执行
  dry_run: false
  # 跳过交互确认提示（自动继续）
  "yes": false

  # 运行解释器和总日志
  python: ""   # 留空=自动选择可用的 Python 3
  log_file: "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/logs/run_s1_to_s8_basin_pipeline.log"

  # s4 / s6 在本地运行（不提交 LSF）
  local_s4: false
  local_s6: false

  # s1 返回非 0 时视为致命错误
  strict_s1: false

  # s2: 按分辨率整理
  s2_workers: 8            # 并行 worker 数，推荐 4-16
  s2_clear: false          # 清空旧输出目录重新组织
  s2_dataset: ""           # 只处理指定数据集；留空=全部。示例: Huanghe 或 [Huanghe, GloRiSe/SS]

  # s3: 收集 basin 主线测站
  s3_workers: 32           # 并行 worker 数，推荐 8-32
  s3_exclude_resolutions: climatology   # 排除的分辨率，可选 daily,monthly,annual,climatology

  # s4: 流域追踪
  s4_workers: 24           # S4_N_WORKERS，推荐 8-48
  s4_array_size: 16        # LSF 阵列大小（集群模式），推荐 8-32
  s4_batch_size: 50        # 每批站点数，推荐 20-100
  s4_maxtasksperchild: 10  # worker 最大子任务数，推荐 8-20
  s4_no_resume: false      # 关闭 resume（重新跑所有 shard）
  s4_no_gpkg: false        # 关闭 GPKG 几何输出

  # MERIT Hydro 数据集路径
  merit_dir: "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1"

  # s6: NetCDF 导出
  s6_workers: 24               # merge worker 数，推荐 8-40
  matrix_workers:              # 每个 resolution matrix 导出的总 worker；留空=脚本默认
  matrix_resolution_workers:   # 每个 resolution 内部 worker；留空=脚本默认
  s6_include_climatology: false   # 将 climatology 合并到 master NC
  skip_climatology_export: false   # 跳过独立气候 NC 导出

  # LSF 轮询 / s7 局部流域
  cluster_poll_seconds: 60   # LSF 轮询间隔秒，推荐 30-120
  include_local_basins: false   # 生成局部流域 GPKG

  # s8: 发布
  s8_link_mode: hardlink        # 可选 hardlink, copy, symlink
  s8_skip_gpkg: false           # 跳过 GPKG 发布
  s8_skip_validation: false     # 跳过发布校验
  s8_include_basin_polygons: true  # 包含流域多边形
  s8_force: true                # 强制覆盖已存在的发布文件
  s8_minimal_matrix_workers: 3        # Parallel workers for minimal matrix NetCDF copies
  s8_minimal_compression: 4           # NetCDF compression level (0-9)
  s8_skip_minimal_climatology: false  # Skip climatology extension package
  s8_skip_minimal_satellite: false    # Skip satellite extension package

# ============================================================
# 环境变量（仅通过 export 设置，非 CLI 参数）
# 修改这里会自动注入到 os.environ
# ============================================================
env:
  # s6 仅运行指定的子步骤，逗号分隔。留空=全部运行
  # 可选: merge, matrix_daily, matrix_monthly, matrix_annual, climatology, satellite
  RUN_ONLY: ""

  JOB_TAG: s6fast              # LSF 作业名前缀
  MERGE_WORKERS: "40"          # merge worker 数，推荐 8-48
  MERGE_METADATA_WORKERS: "32" # 元数据 worker 数，推荐 8-32
  DAILY_WORKERS: "40"          # 日矩阵 worker 数，推荐 8-40
  MONTHLY_WORKERS: "20"        # 月矩阵 worker 数，推荐 4-20
  ANNUAL_WORKERS: "4"          # 年矩阵 worker 数，推荐 1-8

  MERGE_N: "48"                # merge 步骤 LSF 核数，推荐 24-64
  MERGE_MEM_MB: "240000"       # merge 步骤 LSF 内存 MB，推荐 120000-480000
  CLIM_N: "4"                  # 气候导出 LSF 核数，推荐 2-8
  CLIM_MEM_MB: "16000"         # 气候导出 LSF 内存 MB，推荐 8000-32000
  SATVAL_N: "24"               # 卫星验证 LSF 核数，推荐 8-32
  SATVAL_MEM_MB: "64000"       # 卫星验证 LSF 内存 MB，推荐 32000-128000

  # s4 LSF 配置
  S4_QUEUE: normal              # LSF 队列名
  S4_NCORES: "24"               # 每 job 核数，推荐 8-48
  S4_MEM: 120G                  # 每 job 内存
  S4_PTILE: "24"                # 每节点核数
  S4_GPKG_EXCLUDE_SATELLITE: "1"  # GPKG 排除卫星站点，1=排除

  # s6 LSF 配置
  LSF_QUEUE: ""                # LSF 队列（留空用默认）
  LSF_PROJECT: ""              # LSF project 名称
  LSF_EXTRA: ""                # 附加 bsub 参数
"""
    print(yaml_text)
    sys.exit(0)
def _load_config_file(config_path):
    """Load YAML/JSON config, set os.environ from 'env' section, return the 'cli' dict."""
    config_path = Path(config_path)
    if not config_path.is_file():
        raise SystemExit("Config file not found: {}".format(config_path))
    cfg = None
    # Try YAML first, fallback to JSON
    try:
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except (ImportError, yaml.YAMLError):
        try:
            import json
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception as exc:
            raise SystemExit("Failed to parse config file {}: {}".format(config_path, exc))

    if cfg is None:
        raise SystemExit("Config file {} is empty or invalid.".format(config_path))

    # ── Apply env vars ──
    env_section = cfg.get("env", {})
    if env_section:
        for key, value in env_section.items():
            if value is not None and str(value).strip():
                os.environ[key] = str(value)

    # ── Return CLI section (coerce keys to strings, values to types argparse expects) ──
    cli_section = cfg.get("cli", {}) or {}
    # YAML parses "yes" as boolean True; coerce all keys back to strings
    cli_section = {str(k): v for k, v in cli_section.items()}
    # Argparse set_defaults expects non-string values for store_true/store_false
    # actions, but strings for everything else.  Keep native types as-is and let
    # argparse handle coercion.
    print("Loaded config file: {}".format(config_path))
    return cli_section


def main():
    # ── Pre-parse: check for --dump-config-template or --config-file ──
    pre_args = [a for a in sys.argv[1:] if not a.startswith("--config-file=") and a not in ("--config-file", "-c")]
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--dump-config-template", action="store_true", default=False)
    pre_parser.add_argument("--config-file", "-c", default="")
    pre_parsed, _ = pre_parser.parse_known_args()

    if pre_parsed.dump_config_template:
        _write_config_template()

    if pre_parsed.config_file:
        cli_overrides = _load_config_file(pre_parsed.config_file)
        args = parse_args(defaults=cli_overrides)
        print("Applied {} CLI defaults from config file (CLI flags take precedence).".format(
            sum(1 for d in cli_overrides if hasattr(args, d))
        ))
    else:
        args = parse_args()
    python_bin = resolve_python(args.python)
    stages = _resolve_selected_stages(args)
    outputs = stage_outputs()
    specs = build_stage_specs(args, python_bin)

    if not _confirm_config(args, stages, python_bin):
        return 1

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
        def _env(name, default=""):
            return os.environ.get(name, default)
        s6_run_only = _env("RUN_ONLY")
        _print_and_log(log_fp, "S4 mode:                 {}".format("local" if args.local_s4 else "cluster (LSF)"))
        _print_and_log(log_fp, "S6 mode:                 {}".format("local" if args.local_s6 else "cluster (LSF)"))
        _print_and_log(log_fp, "S6 RUN_ONLY:             {}".format(s6_run_only if s6_run_only else "all (no filter)"))
        _print_and_log(log_fp, "Include climatology in s6 master: {}".format(args.s6_include_climatology))
        _print_and_log(log_fp, "Skip climatology export: {}".format(args.skip_climatology_export))
        _print_and_log(log_fp, "MERIT dir (resolved):    {}".format(merit_dir))
        _print_and_log(log_fp, "S4_RESUME:               {}".format(not args.s4_no_resume))
        _print_and_log(log_fp, "S4_N_WORKERS:            {}".format(args.s4_workers))
        _print_and_log(log_fp, "S4_BATCH_SIZE:           {}".format(args.s4_batch_size))
        _print_and_log(log_fp, "S4_SAVE_GPKG:            {}".format(not args.s4_no_gpkg))
        _print_and_log(log_fp, "S4_MAXTASKSPERCHILD:     {}".format(args.s4_maxtasksperchild))
        _print_and_log(log_fp, "S4_ARRAY_SIZE:           {}".format(args.s4_array_size))
        _print_and_log(log_fp, "s3 exclude resolutions:  {}".format(args.s3_exclude_resolutions))
        _print_and_log(log_fp, "s8 force overwrite:      {}".format(args.s8_force))
        _print_and_log(log_fp, "s8 skip validation:      {}".format(args.s8_skip_validation))
        _print_and_log(log_fp, "s8 include basin poly:   {}".format(args.s8_include_basin_polygons))
        _print_and_log(log_fp, "local s4:                {}".format(args.local_s4))
        _print_and_log(log_fp, "local s6:                {}".format(args.local_s6))
        _print_and_log(log_fp, "s2 workers:              {}".format(args.s2_workers))
        _print_and_log(log_fp, "s2 clear:                {}".format(args.s2_clear))
        _print_and_log(log_fp, "s2 dataset filter:       {}".format(", ".join(_split_multi_value(args.s2_dataset)) or "all"))
        _print_and_log(log_fp, "s3 workers:              {}".format(args.s3_workers))
        _print_and_log(log_fp, "s6 workers:              {}".format(args.s6_workers))
        _print_and_log(log_fp, "matrix workers:          {}".format(args.matrix_workers))
        _print_and_log(log_fp, "matrix resolution workers: {}".format(args.matrix_resolution_workers))
        _print_and_log(log_fp, "s8 link mode:            {}".format(args.s8_link_mode))
        _print_and_log(log_fp, "s8 skip gpkg:            {}".format(args.s8_skip_gpkg))
        _print_and_log(log_fp, "s8 minimal matrix workers:     {}".format(args.s8_minimal_matrix_workers))
        _print_and_log(log_fp, "s8 minimal compression:        {}".format(args.s8_minimal_compression))
        _print_and_log(log_fp, "s8 skip minimal climatology:  {}".format(args.s8_skip_minimal_climatology))
        _print_and_log(log_fp, "s8 skip minimal satellite:    {}".format(args.s8_skip_minimal_satellite))
        _print_and_log(log_fp, "include local basins:    {}".format(args.include_local_basins))
        _print_and_log(log_fp, "strict s1:               {}".format(args.strict_s1))
        _print_and_log(log_fp, "cluster poll seconds:    {}".format(args.cluster_poll_seconds))
        _print_and_log(log_fp, "dry run:                 {}".format(args.dry_run))
        # ── 仅能通过 export 设置的环境变量 ──
        _print_and_log(log_fp, "--- env-only settings ---")
        _print_and_log(log_fp, "RUN_ONLY:                {}".format(s6_run_only if s6_run_only else "all (no filter)"))
        _print_and_log(log_fp, "JOB_TAG:                 {}".format(_env("JOB_TAG", "s6fast")))
        _print_and_log(log_fp, "MERGE_WORKERS:           {}".format(_env("MERGE_WORKERS", "40")))
        _print_and_log(log_fp, "MERGE_METADATA_WORKERS:  {}".format(_env("MERGE_METADATA_WORKERS", "32")))
        _print_and_log(log_fp, "DAILY_WORKERS:           {}".format(_env("DAILY_WORKERS", "40")))
        _print_and_log(log_fp, "MONTHLY_WORKERS:         {}".format(_env("MONTHLY_WORKERS", "20")))
        _print_and_log(log_fp, "ANNUAL_WORKERS:          {}".format(_env("ANNUAL_WORKERS", "4")))
        _print_and_log(log_fp, "MERGE_N:                 {}".format(_env("MERGE_N", "48")))
        _print_and_log(log_fp, "MERGE_MEM_MB:            {}".format(_env("MERGE_MEM_MB", "240000")))
        _print_and_log(log_fp, "CLIM_N:                  {}".format(_env("CLIM_N", "4")))
        _print_and_log(log_fp, "CLIM_MEM_MB:             {}".format(_env("CLIM_MEM_MB", "16000")))
        _print_and_log(log_fp, "SATVAL_N:                {}".format(_env("SATVAL_N", "24")))
        _print_and_log(log_fp, "SATVAL_MEM_MB:           {}".format(_env("SATVAL_MEM_MB", "64000")))
        _print_and_log(log_fp, "S4_QUEUE:                {}".format(_env("S4_QUEUE", "normal")))
        _print_and_log(log_fp, "S4_NCORES:               {}".format(_env("S4_NCORES", "24")))
        _print_and_log(log_fp, "S4_MEM:                  {}".format(_env("S4_MEM", "120G")))
        _print_and_log(log_fp, "S4_PTILE:                {}".format(_env("S4_PTILE", "24")))
        _print_and_log(log_fp, "S4_GPKG_EXCLUDE_SATELLITE: {}".format(_env("S4_GPKG_EXCLUDE_SATELLITE", "1")))
        _print_and_log(log_fp, "LSF_QUEUE:               {}".format(_env("LSF_QUEUE") or "(empty)"))
        _print_and_log(log_fp, "LSF_PROJECT:             {}".format(_env("LSF_PROJECT") or "(empty)"))
        _print_and_log(log_fp, "LSF_EXTRA:               {}".format(_env("LSF_EXTRA") or "(empty)"))
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
