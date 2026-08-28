#!/usr/bin/env python3
"""
Patch observation_type column into an existing s6_cluster_quality_order.csv.

Reads the observation_type global attribute from each referenced NetCDF file.
Falls back to s5_basin_clustered_stations.csv if a file is unreadable.

Usage:
    cd scripts_basin_test
    python3 tools/patch_quality_order_observation_type.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_QUALITY_ORDER_CSV = PROJECT_ROOT / "output" / "s6_cluster_quality_order.csv"
DEFAULT_S5_CSV = PROJECT_ROOT / "output" / "s5_basin_clustered_stations.csv"

# Columns in correct output order (matching what s8 expects)
COLUMNS = [
    "cluster_id",
    "cluster_uid",
    "cluster_index",
    "resolution",
    "observation_type",  # inserted after resolution
    "quality_rank",
    "n_candidates",
    "is_top_ranked",
    "source",
    "source_station_index",
    "source_station_uid",
    "path",
    "quality_score",
    "good_flag_count",
    "valid_flag_count",
    "n_time_rows",
    "n_nonempty_rows",
    "n_publish_rows",
    "source_family",
    "merge_eligible",
    "validation_only",
    "merge_exclusion_reason",
    "merge_policy",
]

# Ensure all original columns are covered
REQUIRED_ORIGINAL_COLUMNS = {
    "cluster_id", "cluster_uid", "cluster_index", "resolution",
    "quality_rank", "n_candidates", "is_top_ranked", "source",
    "source_station_index", "source_station_uid", "path", "quality_score",
    "good_flag_count", "valid_flag_count", "n_time_rows", "n_nonempty_rows",
    "n_publish_rows", "source_family", "merge_eligible", "validation_only",
    "merge_exclusion_reason", "merge_policy",
}


def _read_observation_type_from_nc(path_str: str) -> str:
    """Read observation_type global attribute from a NetCDF file."""
    if nc4 is None:
        return ""
    try:
        with nc4.Dataset(path_str, "r") as ds:
            return str(getattr(ds, "observation_type", "")).strip()
    except Exception:
        return ""


def _build_s5_fallback(s5_path: Path) -> dict[str, str]:
    """Build a dict mapping relative path -> observation_type from s5 CSV."""
    if not s5_path.is_file():
        print("Warning: s5 CSV not found at {}; no fallback available.".format(s5_path), file=sys.stderr)
        return {}
    s5 = pd.read_csv(s5_path, keep_default_na=False, low_memory=False)
    if "observation_type" not in s5.columns or "path" not in s5.columns:
        print("Warning: s5 CSV missing 'path' or 'observation_type' columns.", file=sys.stderr)
        return {}
    lookup = {}
    for _, row in s5.iterrows():
        rel = str(row.get("path", "")).strip()
        ot = str(row.get("observation_type", "")).strip()
        if rel and ot:
            lookup[rel] = ot
    return lookup


def _extract_relative_path(abs_path: str) -> str:
    """Extract path relative to output_resolution_organized/ directory."""
    marker = "output_resolution_organized/"
    idx = abs_path.find(marker)
    if idx >= 0:
        return abs_path[idx + len(marker):]
    return abs_path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Add observation_type column to s6_cluster_quality_order.csv."
    )
    parser.add_argument(
        "--quality-order-csv",
        default=str(DEFAULT_QUALITY_ORDER_CSV),
        help="Path to s6_cluster_quality_order.csv",
    )
    parser.add_argument(
        "--s5-csv",
        default=str(DEFAULT_S5_CSV),
        help="Path to s5_basin_clustered_stations.csv (fallback)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print summary without writing.",
    )
    args = parser.parse_args(argv)

    quality_path = Path(args.quality_order_csv)
    s5_path = Path(args.s5_csv)

    if not quality_path.is_file():
        print("Error: quality order CSV not found: {}".format(quality_path), file=sys.stderr)
        return 1

    # 1. Load quality order CSV
    qo = pd.read_csv(quality_path, keep_default_na=False)
    missing_cols = REQUIRED_ORIGINAL_COLUMNS - set(qo.columns)
    if missing_cols:
        print("Error: quality order CSV missing expected columns: {}".format(
            ", ".join(sorted(missing_cols))), file=sys.stderr)
        return 1

    print("Loaded quality order CSV: {} rows, {} columns".format(len(qo), len(qo.columns)))

    # 2. Check if observation_type already exists
    if "observation_type" in qo.columns:
        blank = qo["observation_type"].fillna("").astype(str).str.strip().eq("")
        if blank.sum() == 0:
            print("observation_type column already present with no blanks. Nothing to do.")
            return 0
        print("observation_type column exists but {} rows are blank. Replacing...".format(blank.sum()))
        qo = qo.drop(columns=["observation_type"])

    # 3. Build s5 fallback lookup
    s5_lookup = _build_s5_fallback(s5_path)
    print("s5 fallback lookup: {} paths indexed".format(len(s5_lookup)))

    # 4. Fill observation_type for each row
    obs_types: list[str] = []
    nc_ok = 0
    nc_fail = 0
    s5_ok = 0
    s5_fail = 0

    paths = qo["path"].tolist()
    for i, path_str in enumerate(paths):
        ot = _read_observation_type_from_nc(path_str)
        if ot:
            obs_types.append(ot)
            nc_ok += 1
        else:
            # Fallback: try s5
            rel = _extract_relative_path(path_str)
            ot = s5_lookup.get(rel, "")
            if ot:
                obs_types.append(ot)
                s5_ok += 1
            else:
                obs_types.append("")
                s5_fail += 1

        if (i + 1) % 500 == 0:
            print("  Progress: {}/{} rows processed".format(i + 1, len(paths)))

    # 5. Summary
    print()
    print("Patch summary:")
    print("  Total rows:           {}".format(len(qo)))
    print("  From NetCDF:          {}".format(nc_ok))
    print("  From s5 fallback:     {}".format(s5_ok))
    print("  Failed (blank):       {}".format(s5_fail))
    if s5_fail > 0:
        print("  WARNING: {} rows will have blank observation_type!".format(s5_fail), file=sys.stderr)

    # 6. Add column and reorder
    qo["observation_type"] = obs_types

    # Place observation_type after resolution
    existing_cols = [c for c in COLUMNS if c in qo.columns]
    extra_cols = [c for c in qo.columns if c not in COLUMNS]
    qo = qo[existing_cols + extra_cols]

    if args.dry_run:
        print("Dry-run mode: no changes written.")
        print("New columns:", list(qo.columns))
        print("Observation type value counts:")
        print(qo["observation_type"].value_counts().to_string())
    else:
        qo.to_csv(quality_path, index=False)
        print("Updated {} ({} rows)".format(quality_path, len(qo)))

    return 0 if s5_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
