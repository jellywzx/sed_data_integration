#!/usr/bin/env python3
"""S9: convert public minimal release products to station-facing names."""

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

from pipeline_paths import get_output_r_root
from release_public_station_names import convert_release_dir, has_failures, write_report
from release_satellite_query_catalog import QUERY_CATALOG_NAME, export_satellite_query_catalog
from s8_publish_minimal_release_package import build_minimal_key_contract_rows


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_RELEASE_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_minimal"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "scripts_basin_test/output/sed_reference_release_minimal_final"
DEFAULT_EXAMPLE_SCRIPT = SCRIPT_DIR / "tools" / "example_reference_workflow_minimal.py"
DEFAULT_SATELLITE_QUERY_CHUNK_SIZE = 250000


def append_generated_query_inventory(release_dir, summary):
    """Register the S9-generated satellite query catalogue in release_inventory.csv."""
    inventory_path = release_dir / "release_inventory.csv"
    if not inventory_path.is_file():
        print("[warn] release inventory missing; query catalogue was not registered: {}".format(inventory_path))
        return
    frame = pd.read_csv(inventory_path, keep_default_na=False)
    if "file" not in frame.columns:
        print("[warn] release inventory has no file column; query catalogue was not registered")
        return
    frame = frame[frame["file"].astype(str) != QUERY_CATALOG_NAME].copy()
    row = {column: "" for column in frame.columns}
    if len(frame):
        template = frame.iloc[0].to_dict()
        for column in frame.columns:
            row[column] = template.get(column, "")
    if "package" in row:
        row["package"] = "sed_reference_release"
    row["file"] = QUERY_CATALOG_NAME
    if "source_path" in row:
        row["source_path"] = str(release_dir / "sed_reference_satellite.nc")
    if "source_exists" in row:
        row["source_exists"] = True
    if "status" in row:
        row["status"] = "generated_query_table"
    frame = pd.concat([frame, pd.DataFrame([row])], ignore_index=True)
    frame.to_csv(inventory_path, index=False)
    print("[write] {} (registered {} records)".format(inventory_path, summary["records"]))


def append_key_contract_validation(release_dir, dry_run=False):
    validation_path = release_dir / "release_validation_report.csv"
    rows = build_minimal_key_contract_rows(release_dir)
    if dry_run:
        print("[dry-run] would append {} key-contract validation rows to {}".format(len(rows), validation_path))
        return rows
    if validation_path.is_file():
        current = pd.read_csv(validation_path, keep_default_na=False)
    else:
        current = pd.DataFrame(columns=["check", "status", "message", "evidence"])
    current = current[
        ~current["check"].astype(str).str.startswith("key_contract:")
    ].copy()
    updated = pd.concat([current, pd.DataFrame(rows)], ignore_index=True)
    updated.to_csv(validation_path, index=False)
    print("[write] {}".format(validation_path))
    return rows


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
    ap.add_argument(
        "--satellite-query-chunk-size",
        type=int,
        default=DEFAULT_SATELLITE_QUERY_CHUNK_SIZE,
        help="Record chunk size used when exporting satellite_query_catalog.csv.gz.",
    )
    ap.add_argument(
        "--skip-satellite-query-catalog",
        action="store_true",
        help="Do not generate the record-level satellite query catalogue.",
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
    print("[config] satellite query chunk size: {}".format(args.satellite_query_chunk_size))
    print("[config] skip satellite query catalogue: {}".format(args.skip_satellite_query_catalog))
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

    if not args.skip_satellite_query_catalog:
        query_nc = output_dir / "sed_reference_satellite.nc"
        query_path = output_dir / QUERY_CATALOG_NAME
        if args.dry_run:
            print("[dry-run] would export {} from {}".format(query_path, query_nc))
        else:
            try:
                query_summary = export_satellite_query_catalog(
                    query_nc,
                    query_path,
                    chunk_size=args.satellite_query_chunk_size,
                )
            except Exception as exc:
                print("[fail] satellite query catalogue export failed: {}".format(exc), file=sys.stderr)
                return 1
            append_generated_query_inventory(output_dir, query_summary)
            print(
                "[query] wrote {} rows for {} satellite stations -> {}".format(
                    query_summary["records"],
                    query_summary["stations"],
                    query_path,
                )
            )

    write_report(rows, report_path, dry_run=args.dry_run)
    if args.dry_run:
        print("[dry-run] would write report: {}".format(report_path))
    else:
        print("[write] {}".format(report_path))

    key_rows = append_key_contract_validation(output_dir, dry_run=args.dry_run)
    key_status_counts = {}
    for row in key_rows:
        key_status_counts[row["status"]] = key_status_counts.get(row["status"], 0) + 1
    print("[validation] key-contract {}".format(key_status_counts))
    key_failures = key_status_counts.get("fail", 0)

    status_counts = {}
    for row in rows:
        status_counts[row.status] = status_counts.get(row.status, 0) + 1
    print("[summary] {}".format(status_counts))

    if key_failures:
        print("[fail] key-contract validation failed: {} failing check(s)".format(key_failures), file=sys.stderr)
        return 1
    if args.strict and has_failures(rows):
        print("[fail] residual old public cluster schema naming remains", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
