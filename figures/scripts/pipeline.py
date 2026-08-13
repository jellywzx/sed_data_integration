#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline to run all plot_fig*.py scripts in this directory in parallel.

Usage:
    python3 pipeline.py               # use all CPU cores
    python3 pipeline.py -j 4           # use 4 parallel workers
    python3 pipeline.py --dry-run      # list scripts without executing
"""

import subprocess
import sys
import time
from pathlib import Path
from multiprocessing import Pool


def run_one(script_path):
    """Run a single plot script. Returns (name, ok, elapsed, stderr_tail)."""
    name = script_path.name
    t0 = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(script_path.parent),
    )

    elapsed = time.time() - t0
    if result.returncode == 0:
        return (name, True, elapsed, None)
    else:
        stderr_tail = result.stderr.decode("utf-8", errors="replace").strip().splitlines()[-10:]
        return (name, False, elapsed, stderr_tail)


def main():
    # Parse arguments
    dry_run = "--dry-run" in sys.argv
    n_jobs = 10
    for i, arg in enumerate(sys.argv):
        if arg == "-j" and i + 1 < len(sys.argv):
            n_jobs = int(sys.argv[i + 1])
        elif arg.startswith("-j"):
            n_jobs = int(arg[2:])

    scripts_dir = Path(__file__).resolve().parent
    scripts = sorted(scripts_dir.glob("plot_fig*.py"))

    if not scripts:
        print("No plot_fig*.py scripts found.")
        return

    print(f"Found {len(scripts)} script(s), running with {n_jobs} workers:")
    for s in scripts:
        print(f"  {s.name}")
    print()

    if dry_run:
        print("Dry-run mode — no scripts executed.")
        return

    # Run in parallel
    t_total = time.time()
    results = []
    with Pool(processes=n_jobs) as pool:
        # imap_unordered: print as each completes
        for result in pool.imap_unordered(run_one, scripts):
            results.append(result)
            name, ok, elapsed, _ = result
            status = "OK" if ok else "FAILED"
            print(f"  [{status}] {name} ({elapsed:.1f}s)")
            if not ok:
                stderr_tail = result[3]
                print("    stderr tail:")
                for line in stderr_tail:
                    print(f"      {line}")

    t_total = time.time() - t_total

    # Summary
    failed = [(name, tail) for name, ok, _, tail in results if not ok]
    n_ok = len(results) - len(failed)
    print()
    print("=" * 60)
    print(f"Total time: {t_total:.1f}s  |  {n_ok}/{len(results)} succeeded")
    if failed:
        print("Failed:")
        for name, tail in failed:
            print(f"  FAIL  {name}")
        sys.exit(1)
    else:
        print("All scripts succeeded.")
        sys.exit(0)


if __name__ == "__main__":
    main()
