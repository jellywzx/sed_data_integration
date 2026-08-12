#!/usr/bin/env python3
"""S9: convert public minimal release products to station-facing names."""

import argparse
import shutil
import sys
from pathlib import Path

from pipeline_paths import get_output_r_root
from release_public_station_names import convert_release_dir, has_failures, write_report


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_RELEASE_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_minimal"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_minimal_final"
DEFAULT_EXAMPLE_SCRIPT = SCRIPT_DIR / "tools" / "example_reference_workflow_minimal.py"


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Rename public minimal release cluster schema names to station-facing names."
    )
    ap.add_argument(
        "--release-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Path to input release directory (default: scripts_basin_test/output/sed_reference_release_minimal).",
    )
    ap.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for converted files (default: scripts_basin_test/output/sed_reference_release_minimal_final).",
    )
    ap.add_argument(
        "--report",
        default="",
        help="Output CSV report path (default: <output-dir>/public_station_names_report.csv).",
    )
    ap.add_argument(
        "--example-script",
        default=str(DEFAULT_EXAMPLE_SCRIPT),
        help="Minimal example workflow script copied into the public package.",
    )
    ap.add_argument("--dry-run", action="store_true", help="Report planned changes without rewriting files.")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero when residual old public cluster schema names remain.")
    ap.add_argument(
        "--no-example",
        action="store_true",
        help="Do not copy the minimal example workflow script into the release directory.",
    )
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    release_dir = Path(args.release_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    report_path = Path(args.report).expanduser().resolve() if args.report else output_dir / "public_station_names_report.csv"
    example_script = None if args.no_example else Path(args.example_script).expanduser().resolve()

    print("[config] release dir: {}".format(release_dir))
    print("[config] output dir:  {}".format(output_dir))
    print("[config] report:      {}".format(report_path))
    print("[config] dry run:     {}".format(args.dry_run))
    print("[config] strict:      {}".format(args.strict))
    if example_script is not None:
        print("[config] example:     {}".format(example_script))

    # Copy input to output if they differ
    if release_dir != output_dir:
        if not args.dry_run:
            if output_dir.exists():
                shutil.rmtree(output_dir)
            shutil.copytree(release_dir, output_dir)
            print("[copy] {} -> {}".format(release_dir, output_dir))
        else:
            print("[dry-run] would copy {} -> {}".format(release_dir, output_dir))

    rows = convert_release_dir(
        output_dir,
        example_script=example_script,
        dry_run=args.dry_run,
        audit=True,
    )
    write_report(rows, report_path, dry_run=args.dry_run)
    if args.dry_run:
        print("[dry-run] would write report: {}".format(report_path))
    else:
        print("[write] {}".format(report_path))

    status_counts = {}
    for row in rows:
        status_counts[row.status] = status_counts.get(row.status, 0) + 1
    print("[summary] {}".format(status_counts))

    if args.strict and has_failures(rows):
        print("[fail] residual old public cluster schema naming remains", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
