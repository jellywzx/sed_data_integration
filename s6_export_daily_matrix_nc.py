#!/usr/bin/env python3
"""Export only the daily basin matrix NetCDF.

This wrapper pre-filters satellite validation-only rows before calling the
shared resolution-matrix exporter.

Why:
  s6_export_resolution_matrix_ncs.py excludes satellite from final main values
  through build_cluster_series(..., include_satellite_in_main_merge=False), but
  if satellite rows are passed into it, their NetCDF files may still be opened,
  read, and scored before exclusion. Because satellite data is large, this
  wrapper filters satellite rows from the S5 CSV first.

Default behavior:
  - remove source_family == "satellite" rows from the S5 CSV;
  - write a temporary non-satellite S5 CSV under <out-dir>/_tmp/;
  - call the shared exporter with --resolutions daily;
  - DO NOT compress NetCDF output by default.

Use --keep-satellite to restore old behavior for diagnostics.
Use --compress --compression-level 4 when you later want compressed output.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from s6_basin_merge_to_nc import classify_source_family
from s6_export_resolution_matrix_ncs import (
    DEFAULT_INPUT,
    DEFAULT_OUT_DIR,
    main as export_main,
)


def _has_resolution_arg(argv):
    """daily wrapper owns --resolutions; users should not override it."""
    for arg in argv:
        text = str(arg)
        if text == "--resolutions" or text.startswith("--resolutions="):
            return True
    return False


def _build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Export daily basin matrix NetCDF after pre-filtering satellite rows."
        )
    )
    parser.add_argument(
        "--input",
        "-i",
        default=str(DEFAULT_INPUT),
        help="input s5_basin_clustered_stations.csv",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="output directory for matrix NetCDF files",
    )
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=None,
        help="worker budget passed to s6_export_resolution_matrix_ncs.py",
    )
    parser.add_argument(
        "--resolution-workers",
        type=int,
        default=None,
        help="resolution worker count passed to s6_export_resolution_matrix_ncs.py",
    )
    parser.add_argument(
        "--filtered-input",
        default="",
        help=(
            "optional path for the temporary non-satellite S5 CSV. "
            "Default: <out-dir>/_tmp/<input-stem>.daily_no_satellite.csv"
        ),
    )
    parser.add_argument(
        "--keep-satellite",
        action="store_true",
        help=(
            "do not pre-filter satellite rows; restore old behavior where "
            "satellite candidates are passed to the shared exporter"
        ),
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help=(
            "enable zlib compression in the underlying matrix exporter. "
            "Default is no compression for faster temporary daily matrix writes."
        ),
    )
    parser.add_argument(
        "--compression-level",
        type=int,
        default=4,
        help=(
            "zlib compression level passed to the underlying matrix exporter "
            "when --compress is set. Default: 4"
        ),
    )
    return parser


def _write_non_satellite_input(input_path, out_dir, filtered_input_path):
    """Create a temporary S5 CSV with satellite rows removed."""
    input_path = Path(input_path).resolve()
    out_dir = Path(out_dir).resolve()

    if not input_path.is_file():
        raise SystemExit("Error: input not found: {}".format(input_path))

    stations = pd.read_csv(input_path)

    required_columns = {
        "path",
        "source",
        "lat",
        "lon",
        "cluster_id",
        "station_id",
        "resolution",
    }
    missing = sorted(required_columns - set(stations.columns))
    if missing:
        raise SystemExit(
            "Error: S5 CSV missing required columns: {}".format(
                ", ".join(missing)
            )
        )

    source_family = stations["source"].map(classify_source_family)
    satellite_mask = source_family.eq("satellite")

    n_total = int(len(stations))
    n_satellite = int(satellite_mask.sum())
    n_remaining = n_total - n_satellite

    if n_remaining <= 0:
        raise SystemExit(
            "Error: all {} S5 rows are satellite; daily main matrix would be empty. "
            "Use --keep-satellite only if you intentionally want the old behavior.".format(
                n_total
            )
        )

    if filtered_input_path:
        filtered_path = Path(filtered_input_path).resolve()
    else:
        filtered_path = (
            out_dir
            / "_tmp"
            / "{}.daily_no_satellite.csv".format(input_path.stem)
        )

    filtered_path.parent.mkdir(parents=True, exist_ok=True)

    satellite_sources = (
        stations.loc[satellite_mask, "source"]
        .fillna("")
        .astype(str)
        .value_counts()
        .head(20)
    )

    filtered = stations.loc[~satellite_mask].copy()
    filtered.to_csv(filtered_path, index=False)

    print(
        "Pre-filtered satellite rows before daily matrix export: "
        "removed {} / {} rows; remaining {} rows. filtered_input={}".format(
            n_satellite,
            n_total,
            n_remaining,
            filtered_path,
        )
    )

    if len(satellite_sources) > 0:
        print(
            "Top satellite sources removed: {}".format(
                ", ".join(
                    "{}={}".format(source, count)
                    for source, count in satellite_sources.items()
                )
            )
        )

    return filtered_path


def _build_export_argv(argv):
    if _has_resolution_arg(argv):
        raise SystemExit(
            "s6_export_daily_matrix_nc.py fixes --resolutions daily; "
            "please omit --resolutions."
        )

    parser = _build_parser()
    args = parser.parse_args(argv)

    input_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()

    if args.keep_satellite:
        export_input = input_path
        print(
            "Satellite pre-filter disabled by --keep-satellite; "
            "daily matrix will use original input CSV: {}".format(export_input)
        )
    else:
        export_input = _write_non_satellite_input(
            input_path=input_path,
            out_dir=out_dir,
            filtered_input_path=args.filtered_input,
        )

    export_argv = [
        "--resolutions",
        "daily",
        "--input",
        str(export_input),
        "--out-dir",
        str(out_dir),
    ]

    if args.workers is not None:
        export_argv.extend(["--workers", str(args.workers)])
    if args.resolution_workers is not None:
        export_argv.extend(["--resolution-workers", str(args.resolution_workers)])

    # Default: do not pass --compress, so the underlying exporter writes
    # uncompressed NetCDF if it has been updated to default compress=False.
    if args.compress:
        compression_level = int(args.compression_level)
        if compression_level < 1:
            compression_level = 1
        if compression_level > 9:
            compression_level = 9
        export_argv.append("--compress")
        export_argv.extend(["--compression-level", str(compression_level)])
        print("NetCDF compression requested: zlib level {}".format(compression_level))
    else:
        print("NetCDF compression requested: none")

    return export_argv


if __name__ == "__main__":
    raise SystemExit(export_main(_build_export_argv(sys.argv[1:])))