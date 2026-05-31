#!/usr/bin/env python3
"""Submit S4 basin tracing jobs to LSF.

This is the Python equivalent of submit_s4_lsf.sh.  It submits the trace array,
the finalize job, and a small summary job.  With --wait, it blocks until the
summary job leaves LSF and then validates the summary/output files.
"""

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "output"
LOG_DIR = OUT_DIR / "logs" / "s4_lsf"
SHARD_DIR = OUT_DIR / "s4_shards"


def shell_join(parts):
    import shlex

    return " ".join(shlex.quote(str(part)) for part in parts)


def env_value(name, default):
    value = os.environ.get(name, "")
    return value if value != "" else default


def positive_int(text, name):
    try:
        value = int(text)
    except Exception:
        raise SystemExit("{} must be a positive integer, got {!r}".format(name, text))
    if value <= 0:
        raise SystemExit("{} must be a positive integer, got {!r}".format(name, text))
    return value


def resolve_python(value):
    candidates = [
        value,
        os.environ.get("PYTHON_BIN", "").strip(),
        sys.executable,
        shutil.which("python3"),
        shutil.which("python"),
    ]
    for candidate in candidates:
        if candidate:
            return str(candidate)
    raise SystemExit("python3 not found. Pass --python /path/to/python3.")


def parse_job_id(output):
    match = re.search(r"<(\d+)>", output or "")
    if not match:
        raise RuntimeError("failed to parse LSF job id from output: {}".format(output))
    return match.group(1)


def run_checked(cmd, env=None):
    proc = subprocess.run(
        cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        check=False,
    )
    return proc.returncode, proc.stdout


def submit_job(name, bsub_cmd, dry_run=False, submit_log=None, env=None):
    rendered = shell_join(bsub_cmd)
    if dry_run:
        line = "[DRY_RUN] {}".format(rendered)
        print(line)
        if submit_log:
            submit_log.parent.mkdir(parents=True, exist_ok=True)
            with submit_log.open("a", encoding="utf-8") as fp:
                fp.write(line + "\n")
        return "DRYRUN_{}".format(name)

    if shutil.which("bsub") is None:
        raise RuntimeError("bsub not found in PATH")

    rc, output = run_checked(bsub_cmd, env=env)
    if submit_log:
        submit_log.parent.mkdir(parents=True, exist_ok=True)
        with submit_log.open("a", encoding="utf-8") as fp:
            fp.write("[{}] {}\n".format(datetime.now().strftime("%F %T"), name))
            fp.write(rendered + "\n")
            fp.write(output + "\n\n")
    print(output, end="" if output.endswith("\n") else "\n")
    if rc != 0:
        raise RuntimeError("bsub failed for {} with exit code {}".format(name, rc))
    return parse_job_id(output)


def bjobs_status(job_id):
    commands = [
        ["bjobs", "-noheader", "-a", str(job_id)],
        ["bjobs", "-noheader", str(job_id)],
    ]
    for cmd in commands:
        if shutil.which(cmd[0]) is None:
            return None
        rc, output = run_checked(cmd)
        if rc == 0 and output.strip():
            parts = output.split()
            if len(parts) >= 3:
                return parts[2]
            return "UNKNOWN"
    return None


def wait_for_job(job_id, poll_seconds, label, summary_log=None):
    print("Waiting for {} job {} ...".format(label, job_id), flush=True)
    while True:
        status = bjobs_status(job_id)
        if status in {"DONE", "EXIT"}:
            print("{} job {} ended with LSF status {}".format(label, job_id, status))
            return status
        if status is None and summary_log and Path(summary_log).is_file():
            print("{} job {} no longer listed; summary log exists".format(label, job_id))
            return "ENDED"
        if status is None:
            print("{} job {} is no longer listed by bjobs".format(label, job_id))
            return "ENDED"
        print("{} job {} status: {}".format(label, job_id, status), flush=True)
        time.sleep(max(1, int(poll_seconds)))


def clean(value):
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null"} else text


def size_text(path):
    path = Path(path)
    if not path.is_file():
        return "MISSING"
    size = path.stat().st_size
    units = ["B", "K", "M", "G", "T"]
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    return "{:.1f}{}".format(value, unit) if unit != "B" else "{}B".format(size)


def lsf_log_status(path):
    path = Path(path)
    if not path.is_file():
        return "NO_LSF_LOG"
    text = path.read_text(errors="replace")
    if "Successfully completed." in text:
        return "DONE"
    if "Exited with exit code" in text:
        return "EXIT"
    return "ENDED_UNKNOWN"


def counter_for(rows, name, limit=12):
    if not rows or name not in rows[0]:
        return []
    return Counter(clean(row.get(name)) or "(blank)" for row in rows).most_common(limit)


def count_true(rows, name):
    if not rows or name not in rows[0]:
        return None
    return sum(1 for row in rows if clean(row.get(name)).lower() in {"1", "true", "t", "yes", "y"})


def write_summary(summary_log, array_job_id, finalize_job_id, array_size):
    summary_log = Path(summary_log)
    rows = []
    csv_path = OUT_DIR / "s4_upstream_basins.csv"
    partial_csv_path = OUT_DIR / "s4_upstream_basins.partial.csv"
    reported_area_csv = OUT_DIR / "s4_reported_area_check.csv"
    gpkg_path = OUT_DIR / "s4_upstream_basins.gpkg"
    local_gpkg_path = OUT_DIR / "s4_local_catchments.gpkg"
    finalize_out = LOG_DIR / "s4_finalize.out"
    finalize_err = LOG_DIR / "s4_finalize.err"
    finalize_live = LOG_DIR / "s4_finalize.live.log"

    if csv_path.is_file():
        with csv_path.open(newline="", encoding="utf-8") as fp:
            rows = list(csv.DictReader(fp))

    matched_rows = sum(1 for row in rows if clean(row.get("basin_id")))
    skipped_rows = 0
    if rows and "method" in rows[0]:
        skipped_rows = sum(
            1
            for row in rows
            if clean(row.get("method")) == "source_remote_sensing_no_basin_match"
        )

    shard_files = [
        SHARD_DIR / "s4_upstream_basins.shard_{:04d}.csv".format(i)
        for i in range(int(array_size))
    ]
    present_shards = [path for path in shard_files if path.is_file()]
    missing_shards = [path.name for path in shard_files if not path.is_file()]

    output_files = [
        csv_path,
        partial_csv_path,
        reported_area_csv,
        gpkg_path,
        local_gpkg_path,
        finalize_out,
        finalize_err,
        finalize_live,
    ]

    lines = []
    lines.append("S4 basin matching summary")
    lines.append("Generated: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    lines.append("Array job id: {}".format(array_job_id))
    lines.append("Finalize job id: {}".format(finalize_job_id))
    lines.append("Finalize LSF status: {}".format(lsf_log_status(finalize_out)))
    lines.append("S4_GPKG_EXCLUDE_SATELLITE: {}".format(os.environ.get("S4_GPKG_EXCLUDE_SATELLITE", "")))
    lines.append("")
    lines.append("Shard completion:")
    lines.append("  expected shards: {}".format(array_size))
    lines.append("  present shards : {}".format(len(present_shards)))
    lines.append("  missing shards : {}".format(", ".join(missing_shards[:40]) if missing_shards else "none"))
    if len(missing_shards) > 40:
        lines.append("  missing shards : ... {} more".format(len(missing_shards) - 40))
    lines.append("")
    lines.append("Basin matching counts from s4_upstream_basins.csv:")
    lines.append("  output rows          : {}".format(len(rows) if csv_path.is_file() else "CSV_MISSING"))
    lines.append("  rows with basin_id   : {}".format(matched_rows if csv_path.is_file() else "CSV_MISSING"))
    lines.append("  no-basin skip rows   : {}".format(skipped_rows if csv_path.is_file() else "CSV_MISSING"))
    for col in ("point_in_local", "point_in_basin"):
        value = count_true(rows, col)
        if value is not None:
            lines.append("  {} true : {}".format(col, value))
    lines.append("")

    for col in ("basin_status", "basin_flag", "match_quality", "method", "source"):
        counts = counter_for(rows, col)
        if counts:
            lines.append("{} counts:".format(col))
            for key, value in counts:
                lines.append("  {:<42} {}".format(key, value))
            lines.append("")

    lines.append("Output files:")
    for path in output_files:
        lines.append("  {:<36} {}".format(path.relative_to(SCRIPT_DIR).as_posix(), size_text(path)))
    lines.append("")
    lines.append("GPKG status:")
    lines.append("  upstream basins gpkg : {}".format("YES" if gpkg_path.is_file() else "NO"))
    lines.append("  local catchments gpkg: {}".format("YES" if local_gpkg_path.is_file() else "NO"))
    lines.append("")

    all_shards_present = not missing_shards
    required_outputs_present = csv_path.is_file()
    if all_shards_present and required_outputs_present and lsf_log_status(finalize_out) == "DONE":
        result = "S4_COMPLETED"
    elif required_outputs_present:
        result = "S4_OUTPUT_CSV_PRESENT_BUT_REVIEW_LOGS"
    else:
        result = "S4_INCOMPLETE_OR_FAILED"
    lines.append("RESULT: {}".format(result))

    summary_log.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(lines) + "\n"
    summary_log.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if result == "S4_COMPLETED" else 1


def validate_summary(summary_log):
    summary_log = Path(summary_log)
    if not summary_log.is_file():
        print("Error: missing S4 summary log: {}".format(summary_log), file=sys.stderr)
        return 1
    text = summary_log.read_text(errors="replace")
    if "RESULT: S4_COMPLETED" not in text:
        print("Error: S4 summary did not report completion: {}".format(summary_log), file=sys.stderr)
        return 1
    required = [OUT_DIR / "s4_upstream_basins.csv", OUT_DIR / "s4_reported_area_check.csv"]
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        print("Error: S4 outputs missing after cluster run:", file=sys.stderr)
        for path in missing:
            print("  - {}".format(path), file=sys.stderr)
        return 1
    return 0


def build_trace_script(python_bin, array_size, exclude_satellite):
    live_log = "output/logs/s4_lsf/s4_trace.${JOB_INDEX}.live.log"
    lines = [
        "set -euo pipefail",
        "cd {}".format(shell_join([SCRIPT_DIR])),
        "mkdir -p output/logs/s4_lsf output/s4_shards",
        "PYTHON_BIN={}".format(shell_join([python_bin])),
        'JOB_INDEX="${LSB_JOBINDEX:-1}"',
        'LIVE_LOG="{}"'.format(live_log),
        'exec >> "${LIVE_LOG}" 2>&1',
        "export S4_SHARD_COUNT={}".format(int(array_size)),
        'export S4_SHARD_INDEX="$((JOB_INDEX - 1))"',
        "export S4_FINALIZE_ONLY=0",
        "export S4_GPKG_EXCLUDE_SATELLITE={}".format(shell_join([exclude_satellite])),
        'echo "[$(date \'+%F %T\')] live log: ${LIVE_LOG}"',
        'echo "[$(date \'+%F %T\')] start shard job: index=${S4_SHARD_INDEX}/${S4_SHARD_COUNT}"',
        'echo "[$(date \'+%F %T\')] host: $(hostname)"',
        'echo "[$(date \'+%F %T\')] python: ${PYTHON_BIN}"',
        'echo "[$(date \'+%F %T\')] S4_GPKG_EXCLUDE_SATELLITE=${S4_GPKG_EXCLUDE_SATELLITE}"',
        '"${PYTHON_BIN}" s4_basin_trace_watch.py',
        'echo "[$(date \'+%F %T\')] shard done: index=${S4_SHARD_INDEX}/${S4_SHARD_COUNT}"',
    ]
    return "\n".join(lines)


def build_finalize_script(python_bin, array_size, exclude_satellite):
    lines = [
        "set -euo pipefail",
        "cd {}".format(shell_join([SCRIPT_DIR])),
        "mkdir -p output/logs/s4_lsf output/s4_shards",
        "PYTHON_BIN={}".format(shell_join([python_bin])),
        "LIVE_LOG=output/logs/s4_lsf/s4_finalize.live.log",
        'exec >> "${LIVE_LOG}" 2>&1',
        "export S4_SHARD_COUNT={}".format(int(array_size)),
        "export S4_SHARD_INDEX=0",
        "export S4_FINALIZE_ONLY=1",
        "export S4_GPKG_EXCLUDE_SATELLITE={}".format(shell_join([exclude_satellite])),
        'echo "[$(date \'+%F %T\')] live log: ${LIVE_LOG}"',
        'echo "[$(date \'+%F %T\')] start finalize: shard_count=${S4_SHARD_COUNT}"',
        'echo "[$(date \'+%F %T\')] host: $(hostname)"',
        'echo "[$(date \'+%F %T\')] python: ${PYTHON_BIN}"',
        'echo "[$(date \'+%F %T\')] S4_GPKG_EXCLUDE_SATELLITE=${S4_GPKG_EXCLUDE_SATELLITE}"',
        '"${PYTHON_BIN}" s4_basin_trace_watch.py',
        'echo "[$(date \'+%F %T\')] finalize done"',
    ]
    return "\n".join(lines)


def build_summary_script(python_bin, summary_log, array_job_id, finalize_job_id, array_size, exclude_satellite):
    cmd = [
        python_bin,
        str(SCRIPT_DIR / "submit_s4_lsf.py"),
        "_summary",
        "--summary-log",
        str(summary_log),
        "--array-job-id",
        str(array_job_id),
        "--finalize-job-id",
        str(finalize_job_id),
        "--array-size",
        str(array_size),
    ]
    lines = [
        "set -euo pipefail",
        "cd {}".format(shell_join([SCRIPT_DIR])),
        "export S4_GPKG_EXCLUDE_SATELLITE={}".format(shell_join([exclude_satellite])),
        shell_join(cmd),
    ]
    return "\n".join(lines)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Submit S4 basin tracing jobs to LSF")
    parser.add_argument("array_size_pos", nargs="?", help="Compatibility positional array size")
    parser.add_argument("--array-size", type=int, default=None, help="S4 LSF array size")
    parser.add_argument("--python", dest="python_bin", help="Python interpreter for LSF jobs")
    parser.add_argument("--queue", default=env_value("S4_QUEUE", "normal"))
    parser.add_argument("--cores", default=env_value("S4_NCORES", "24"))
    parser.add_argument("--mem", default=env_value("S4_MEM", "120G"))
    parser.add_argument("--ptile", default=env_value("S4_PTILE", "24"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--summary-log")
    return parser.parse_args(argv)


def parse_summary_args(argv):
    parser = argparse.ArgumentParser(description="Write S4 LSF summary")
    parser.add_argument("--summary-log", required=True)
    parser.add_argument("--array-job-id", required=True)
    parser.add_argument("--finalize-job-id", required=True)
    parser.add_argument("--array-size", required=True, type=int)
    return parser.parse_args(argv)


def submit_s4(args):
    array_size = args.array_size
    if array_size is None and args.array_size_pos:
        array_size = positive_int(args.array_size_pos, "ARRAY_SIZE")
    if array_size is None:
        array_size = 16
    array_size = positive_int(array_size, "ARRAY_SIZE")

    python_bin = resolve_python(args.python_bin)
    cores = positive_int(args.cores, "S4_NCORES")
    ptile = positive_int(args.ptile, "S4_PTILE")
    exclude_satellite = env_value("S4_GPKG_EXCLUDE_SATELLITE", "1")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SHARD_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    submit_log = LOG_DIR / "submit_s4_lsf.{}.log".format(stamp)
    summary_log = Path(args.summary_log) if args.summary_log else LOG_DIR / "s4_summary.{}.log".format(stamp)

    job_env = os.environ.copy()
    job_env.update(
        {
            "S4_SHARD_COUNT": str(array_size),
            "PYTHON_BIN": python_bin,
            "S4_GPKG_EXCLUDE_SATELLITE": str(exclude_satellite),
        }
    )

    print("SCRIPT_DIR={}".format(SCRIPT_DIR))
    print("PYTHON_BIN={}".format(python_bin))
    print("LOG_DIR={}".format(LOG_DIR))
    print("SUBMIT_LOG={}".format(submit_log))
    print("SUMMARY_LOG={}".format(summary_log))
    print("ARRAY_SIZE={}".format(array_size))
    print("DRY_RUN={}".format(int(args.dry_run)))

    trace_cmd = [
        "bsub",
        "-q",
        str(args.queue),
        "-J",
        "s4_trace[1-{}]".format(array_size),
        "-o",
        "output/logs/s4_lsf/s4_trace.%I.out",
        "-e",
        "output/logs/s4_lsf/s4_trace.%I.err",
        "-n",
        str(cores),
        "-R",
        "rusage[mem={}]".format(args.mem),
        "-R",
        "span[ptile={}] span[hosts=1]".format(ptile),
        "bash",
        "-lc",
        build_trace_script(python_bin, array_size, exclude_satellite),
    ]
    array_job_id = submit_job("s4_trace", trace_cmd, args.dry_run, submit_log, job_env)

    finalize_cmd = [
        "bsub",
        "-q",
        str(args.queue),
        "-J",
        "s4_finalize",
        "-w",
        "ended({})".format(array_job_id),
        "-o",
        "output/logs/s4_lsf/s4_finalize.out",
        "-e",
        "output/logs/s4_lsf/s4_finalize.err",
        "-n",
        "1",
        "-R",
        "rusage[mem=24G]",
        "-R",
        "span[hosts=1]",
        "bash",
        "-lc",
        build_finalize_script(python_bin, array_size, exclude_satellite),
    ]
    finalize_job_id = submit_job("s4_finalize", finalize_cmd, args.dry_run, submit_log, job_env)

    summary_cmd = [
        "bsub",
        "-q",
        str(args.queue),
        "-J",
        "s4_summary",
        "-w",
        "ended({})".format(finalize_job_id),
        "-o",
        "output/logs/s4_lsf/s4_summary.out",
        "-e",
        "output/logs/s4_lsf/s4_summary.err",
        "-n",
        "1",
        "-R",
        "rusage[mem=2G]",
        "-R",
        "span[hosts=1]",
        "bash",
        "-lc",
        build_summary_script(python_bin, summary_log, array_job_id, finalize_job_id, array_size, exclude_satellite),
    ]
    summary_job_id = submit_job("s4_summary", summary_cmd, args.dry_run, submit_log, job_env)

    print("")
    print("Submitted.")
    print("Array job id: {}".format(array_job_id))
    print("Finalize job id: {}".format(finalize_job_id))
    print("Summary job id: {}".format(summary_job_id))
    print("S4_GPKG_EXCLUDE_SATELLITE: {}".format(exclude_satellite))
    print("Track with: bjobs -w {} {} {}".format(array_job_id, finalize_job_id, summary_job_id))
    print("Submit log: {}".format(submit_log))
    print("Summary log: {}".format(summary_log))

    if args.dry_run:
        return 0
    if args.wait:
        wait_for_job(summary_job_id, args.poll_seconds, "S4 summary", summary_log)
        return validate_summary(summary_log)
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "_summary":
        args = parse_summary_args(argv[1:])
        return write_summary(
            args.summary_log,
            args.array_job_id,
            args.finalize_job_id,
            args.array_size,
        )
    args = parse_args(argv)
    try:
        return submit_s4(args)
    except Exception as exc:
        print("Error: {}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
