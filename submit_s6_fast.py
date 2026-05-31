#!/usr/bin/env python3
"""Submit S6 NetCDF export jobs to LSF.

This is the Python equivalent of submit_s6_fast.sh.  It submits S6 component
jobs in parallel, then submits a summary job depending on the submitted jobs.
With --wait, it blocks until the summary job ends and validates the outputs.
"""

import argparse
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR / "output"
LOG_DIR = OUT_DIR / "logs" / "s6_lsf_parallel"
MATRIX_DIR = OUT_DIR / "s6_matrix_by_resolution"
CLIM_INPUT_DIR = (
    SCRIPT_DIR.parent.parent / "output_resolution_organized" / "climatology"
).resolve()


def shell_join(parts):
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
        "/share/home/dq134/.conda/envs/wzx/bin/python3",
        sys.executable,
        shutil.which("python3"),
        shutil.which("python"),
    ]
    for candidate in candidates:
        if candidate and Path(str(candidate)).exists():
            return str(candidate)
        if candidate and shutil.which(str(candidate)):
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


def submit_job(name, bsub_cmd, dry_run=False, submit_log=None):
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

    rc, output = run_checked(bsub_cmd)
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


def write_summary(summary_log, specs):
    summary_log = Path(summary_log)
    lines = []
    all_jobs_done = True
    all_outputs_present = True
    lines.append("S6 fast summary")
    lines.append("Generated: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    lines.append("")
    lines.append("{:<12} {:<12} {:<16} {}".format("STEP", "JOBID", "LSF_STATUS", "OUTPUT_FILES"))
    lines.append("{:<12} {:<12} {:<16} {}".format("----", "-----", "----------", "------------"))

    for spec in specs:
        step, jid, lsf_out, files_csv = spec.split("|", 3)
        status = lsf_log_status(lsf_out)
        if status != "DONE":
            all_jobs_done = False
        file_parts = []
        for raw_path in files_csv.split(","):
            path = Path(raw_path)
            if path.is_file():
                file_parts.append("OK:{}:{}".format(path.name, size_text(path)))
            else:
                file_parts.append("MISSING:{}".format(path.name))
                all_outputs_present = False
        lines.append(
            "{:<12} {:<12} {:<16} {}".format(
                step,
                jid,
                status,
                "; ".join(file_parts),
            )
        )

    lines.append("")
    if all_jobs_done and all_outputs_present:
        result = "ALL_STEPS_COMPLETED_SUCCESSFULLY"
    elif all_jobs_done:
        result = "ALL_STEPS_ENDED_BUT_OUTPUTS_MISSING"
    else:
        result = "ONE_OR_MORE_STEPS_FAILED_OR_UNKNOWN"
    lines.append("RESULT: {}".format(result))

    summary_log.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(lines) + "\n"
    summary_log.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if result == "ALL_STEPS_COMPLETED_SUCCESSFULLY" else 1


def validate_summary(summary_log, expected_outputs):
    summary_log = Path(summary_log)
    if not summary_log.is_file():
        print("Error: missing S6 summary log: {}".format(summary_log), file=sys.stderr)
        return 1
    text = summary_log.read_text(errors="replace")
    if "RESULT: ALL_STEPS_COMPLETED_SUCCESSFULLY" not in text:
        print("Error: S6 summary did not report completion: {}".format(summary_log), file=sys.stderr)
        return 1
    missing = [str(path) for path in expected_outputs if not Path(path).is_file()]
    if missing:
        print("Error: S6 outputs missing after cluster run:", file=sys.stderr)
        for path in missing:
            print("  - {}".format(path), file=sys.stderr)
        return 1
    return 0


def build_bsub_cmd(name, ncores, mem_mb, bash_script, args, dep=None):
    cmd = [
        "bsub",
        "-n",
        str(ncores),
        "-R",
        "span[hosts=1] rusage[mem=4096]",
        "-M",
        str(mem_mb),
        "-J",
        name,
        "-oo",
        str(LOG_DIR / "{}.%J.out".format(name)),
        "-eo",
        str(LOG_DIR / "{}.%J.err".format(name)),
    ]
    if args.queue:
        cmd += ["-q", str(args.queue)]
    if args.project:
        cmd += ["-P", str(args.project)]
    if args.extra:
        cmd.extend(shlex.split(args.extra))
    if dep:
        cmd += ["-w", dep]
    cmd += ["bash", "-lc", bash_script]
    return cmd


def cd_script(command):
    return "set -euo pipefail\ncd {}\n{}".format(shell_join([SCRIPT_DIR]), command)


def python_cmd(python_bin, script_name, *args):
    return shell_join([python_bin, script_name] + [str(item) for item in args])


def build_jobs(args, python_bin):
    s5_csv = OUT_DIR / "s5_basin_clustered_stations.csv"
    jobs = []
    merge_workers = positive_int(args.s6_workers, "MERGE_WORKERS")
    merge_metadata_workers = positive_int(env_value("MERGE_METADATA_WORKERS", "32"), "MERGE_METADATA_WORKERS")
    if args.matrix_workers is not None:
        daily_workers = positive_int(args.matrix_workers, "DAILY_WORKERS")
        monthly_workers = positive_int(args.matrix_workers, "MONTHLY_WORKERS")
        annual_workers = positive_int(args.matrix_workers, "ANNUAL_WORKERS")
    else:
        daily_workers = positive_int(env_value("DAILY_WORKERS", "40"), "DAILY_WORKERS")
        monthly_workers = positive_int(env_value("MONTHLY_WORKERS", "20"), "MONTHLY_WORKERS")
        annual_workers = positive_int(env_value("ANNUAL_WORKERS", "4"), "ANNUAL_WORKERS")
    resolution_workers = str(args.matrix_resolution_workers or 1)

    merge_cmd = [
        python_bin,
        "s6_basin_merge_to_nc.py",
        "-i",
        str(s5_csv),
        "-o",
        str(OUT_DIR / "s6_basin_merged_all.nc"),
        "--quality-order-csv",
        str(OUT_DIR / "s6_cluster_quality_order.csv"),
        "-w",
        str(merge_workers),
        "--metadata-workers",
        str(merge_metadata_workers),
    ]
    if args.s6_include_climatology:
        merge_cmd.append("--include-climatology")

    jobs.append(
        {
            "step": "merge",
            "job_name": "{}_merge".format(args.job_tag),
            "ncores": positive_int(env_value("MERGE_N", "48"), "MERGE_N"),
            "mem_mb": positive_int(env_value("MERGE_MEM_MB", "240000"), "MERGE_MEM_MB"),
            "script": cd_script(shell_join(merge_cmd)),
            "outputs": [OUT_DIR / "s6_basin_merged_all.nc", OUT_DIR / "s6_cluster_quality_order.csv"],
        }
    )

    matrix_specs = [
        ("daily", "s6_export_daily_matrix_nc.py", daily_workers, "DAILY_N", "48", "DAILY_MEM_MB", "240000"),
        ("monthly", "s6_export_monthly_matrix_nc.py", monthly_workers, "MONTHLY_N", "24", "MONTHLY_MEM_MB", "120000"),
        ("annual", "s6_export_annual_matrix_nc.py", annual_workers, "ANNUAL_N", "4", "ANNUAL_MEM_MB", "16000"),
    ]
    for step, script_name, workers, n_env, n_default, mem_env, mem_default in matrix_specs:
        cmd = [
            python_bin,
            script_name,
            "-i",
            str(s5_csv),
            "--out-dir",
            str(MATRIX_DIR),
            "--workers",
            str(workers),
            "--resolution-workers",
            str(resolution_workers),
        ]
        jobs.append(
            {
                "step": step,
                "job_name": "{}_{}".format(args.job_tag, step),
                "ncores": positive_int(env_value(n_env, n_default), n_env),
                "mem_mb": positive_int(env_value(mem_env, mem_default), mem_env),
                "script": cd_script(shell_join(cmd)),
                "outputs": [MATRIX_DIR / "s6_basin_matrix_{}.nc".format(step)],
            }
        )

    if not args.skip_climatology_export:
        clim_cmd = [
            python_bin,
            "s6_export_climatology_to_nc.py",
            "--input-dir",
            str(CLIM_INPUT_DIR),
            "--output",
            str(OUT_DIR / "s6_climatology_only.nc"),
            "--output-shp",
            str(OUT_DIR / "s6_climatology_stations.shp"),
        ]
        jobs.append(
            {
                "step": "clim",
                "job_name": "{}_clim".format(args.job_tag),
                "ncores": positive_int(env_value("CLIM_N", "4"), "CLIM_N"),
                "mem_mb": positive_int(env_value("CLIM_MEM_MB", "16000"), "CLIM_MEM_MB"),
                "script": cd_script(shell_join(clim_cmd)),
                "outputs": [OUT_DIR / "s6_climatology_only.nc"],
            }
        )

    sat_script = "\n".join(
        [
            "set -euo pipefail",
            "cd {}".format(shell_join([SCRIPT_DIR])),
            'progress_log="{}"'.format(LOG_DIR / "s6_satellite_progress_${LSB_JOBID}.log"),
            shell_join([python_bin, "-u", "s6_export_satellite_validation_to_nc.py"])
            + ' --progress-log "${progress_log}"',
        ]
    )
    jobs.append(
        {
            "step": "satellite",
            "job_name": "{}_satellite".format(args.job_tag),
            "ncores": positive_int(env_value("SATVAL_N", "24"), "SATVAL_N"),
            "mem_mb": positive_int(env_value("SATVAL_MEM_MB", "64000"), "SATVAL_MEM_MB"),
            "script": sat_script,
            "outputs": [
                OUT_DIR / "s6_satellite_validation_only.nc",
                OUT_DIR / "s6_satellite_validation_catalog.csv",
            ],
        }
    )

    run_only = args.run_only or env_value("RUN_ONLY", "")
    if run_only:
        wanted = set(item.strip() for item in run_only.split(",") if item.strip())
        jobs = [job for job in jobs if job["step"] in wanted]
    return jobs


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Submit S6 export jobs to LSF")
    parser.add_argument("--python", dest="python_bin", help="Python interpreter for LSF jobs")
    parser.add_argument("--queue", default=env_value("LSF_QUEUE", ""))
    parser.add_argument("--project", default=env_value("LSF_PROJECT", ""))
    parser.add_argument("--extra", default=env_value("LSF_EXTRA", ""))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--run-only", default=env_value("RUN_ONLY", ""))
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--job-tag", default=env_value("JOB_TAG", "s6fast"))
    parser.add_argument("--s6-workers", default=env_value("MERGE_WORKERS", "40"))
    parser.add_argument("--matrix-workers", default=None)
    parser.add_argument("--matrix-resolution-workers", default=None)
    parser.add_argument("--s6-include-climatology", action="store_true")
    parser.add_argument("--skip-climatology-export", action="store_true")
    parser.add_argument("--summary-log")
    return parser.parse_args(argv)


def parse_summary_args(argv):
    parser = argparse.ArgumentParser(description="Write S6 LSF summary")
    parser.add_argument("--summary-log", required=True)
    parser.add_argument("specs", nargs="+")
    return parser.parse_args(argv)


def submit_s6(args):
    python_bin = resolve_python(args.python_bin)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    MATRIX_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    submit_log = LOG_DIR / "submit_s6_fast.{}.log".format(stamp)
    summary_log = Path(args.summary_log) if args.summary_log else LOG_DIR / "s6fast_summary.{}.log".format(stamp)
    s5_csv = OUT_DIR / "s5_basin_clustered_stations.csv"

    if not args.dry_run:
        if not s5_csv.is_file():
            raise RuntimeError("missing input {}".format(s5_csv))
        if not CLIM_INPUT_DIR.is_dir() and not args.skip_climatology_export:
            raise RuntimeError("missing climatology dir {}".format(CLIM_INPUT_DIR))

    print("ROOT_DIR={}".format(SCRIPT_DIR))
    print("PYTHON_BIN={}".format(python_bin))
    print("LOG_DIR={}".format(LOG_DIR))
    print("SUBMIT_LOG={}".format(submit_log))
    print("SUMMARY_LOG={}".format(summary_log))
    print("DRY_RUN={}".format(int(args.dry_run)))
    print("RUN_ONLY={}".format(args.run_only or env_value("RUN_ONLY", "all")))

    jobs = build_jobs(args, python_bin)
    if not jobs:
        raise RuntimeError("no S6 jobs selected; check --run-only/RUN_ONLY")

    submitted = []
    for job in jobs:
        jid = submit_job(
            job["step"],
            build_bsub_cmd(
                job["job_name"],
                job["ncores"],
                job["mem_mb"],
                job["script"],
                args,
            ),
            args.dry_run,
            submit_log,
        )
        job["job_id"] = jid
        submitted.append(job)

    dep = " && ".join("ended({})".format(job["job_id"]) for job in submitted)
    summary_specs = []
    for job in submitted:
        out_log = LOG_DIR / "{}.{}.out".format(job["job_name"], job["job_id"])
        summary_specs.append(
            "{}|{}|{}|{}".format(
                job["step"],
                job["job_id"],
                out_log,
                ",".join(str(path) for path in job["outputs"]),
            )
        )

    summary_cmd = [
        python_bin,
        str(SCRIPT_DIR / "submit_s6_fast.py"),
        "_summary",
        "--summary-log",
        str(summary_log),
    ] + summary_specs
    summary_script = cd_script(shell_join(summary_cmd))
    summary_jid = submit_job(
        "summary",
        build_bsub_cmd(
            "{}_summary".format(args.job_tag),
            1,
            4000,
            summary_script,
            args,
            dep=dep,
        ),
        args.dry_run,
        submit_log,
    )

    print("Submitted jobs:")
    for job in submitted:
        print("  {:<9}: {}".format(job["step"], job["job_id"]))
    print("  summary  : {}".format(summary_jid))
    print("Track with: bjobs -w {}".format(" ".join([job["job_id"] for job in submitted] + [summary_jid])))
    print("Submit log: {}".format(submit_log))
    print("Summary log: {}".format(summary_log))

    if args.dry_run:
        return 0
    if args.wait:
        wait_for_job(summary_jid, args.poll_seconds, "S6 summary", summary_log)
        expected = []
        for job in submitted:
            expected.extend(job["outputs"])
        return validate_summary(summary_log, expected)
    return 0


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "_summary":
        args = parse_summary_args(argv[1:])
        return write_summary(args.summary_log, args.specs)
    args = parse_args(argv)
    try:
        return submit_s6(args)
    except Exception as exc:
        print("Error: {}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
