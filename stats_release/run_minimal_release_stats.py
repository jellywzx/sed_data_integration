#!/usr/bin/env python3
"""
Minimal release statistics runner for sed_reference_release_minimal.

This script is a lightweight wrapper around stats_release.run_all_release_stats,
configured to be compatible with the minimal release package where certain
full-release components (e.g., master, climatology, satellite products)
may not exist.

It enables robust execution using --continue-on-error and writes outputs
into a dedicated minimal stats directory for manuscript reporting.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def build_command(args):
    cmd = [
        sys.executable,
        "-m",
        "stats_release.run_all_release_stats",
        "--release-dir",
        str(args.release_dir),
        "--out-dir",
        str(args.out_dir),
        "--continue-on-error",
        "--copy-reports",
    ]

    if args.modules:
        cmd += ["--modules", ";".join(args.modules)]

    return cmd


def main():
    parser = argparse.ArgumentParser(
        description="Run statistics for sed_reference_release_minimal"
    )

    parser.add_argument(
        "--release-dir",
        required=True,
        help="Path to sed_reference_release_minimal directory",
    )

    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for minimal stats",
    )

    parser.add_argument(
        "--modules",
        nargs="*",
        default=None,
        help="Optional subset of stats modules to run",
    )

    args = parser.parse_args()

    Path(args.out_dir).mkdir(parents=True, exist_ok=True)

    cmd = build_command(args)

    print("[run_minimal_release_stats] Executing:")
    print(" ".join(cmd))

    result = subprocess.run(cmd)

    if result.returncode != 0:
        print("[run_minimal_release_stats] WARNING: run_all_release_stats exited with errors")
        print("This is expected in minimal mode if optional datasets are missing.")

    print("[run_minimal_release_stats] Done.")


if __name__ == "__main__":
    main()
