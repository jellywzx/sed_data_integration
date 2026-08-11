#!/usr/bin/env python3
"""Summarize the s6 SSC/SSL publish filter by source dataset.

The s6 merge already records, for every candidate source file, the counts used
around ``filter_publishable_sediment_records`` in
``s6_cluster_quality_order.csv``:

- ``n_time_rows``: rows before the SSC/SSL publish filter;
- ``n_nonempty_rows``: rows with at least one non-missing Q/SSC/SSL value;
- ``n_publish_rows``: rows retained because SSC or SSL is non-missing.

This script aggregates those exact counters by source dataset and optionally by
source + temporal resolution. Therefore it does not reread the NetCDF files and
does not alter the merge result.

Important: ``load_nc_series`` masks values with final flags 3 (bad) and 9
(missing) to NaN before these counters are computed. The summary therefore
reports the publish-filter decision after that masking step.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import S6_QUALITY_ORDER_CSV, get_output_r_root  # noqa: E402


DEFAULT_INPUT = get_output_r_root(REPO_ROOT) / S6_QUALITY_ORDER_CSV
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT / "output_other" / "summarize_sediment_publish_filter"
)
DEFAULT_SOURCE_OUTPUT = DEFAULT_OUTPUT_DIR / "s6_sediment_filter_summary_by_source.csv"
DEFAULT_SOURCE_RES_OUTPUT = (
    DEFAULT_OUTPUT_DIR / "s6_sediment_filter_summary_by_source_resolution.csv"
)

REQUIRED_COLUMNS = {
    "source",
    "resolution",
    "path",
    "n_time_rows",
    "n_nonempty_rows",
    "n_publish_rows",
}
COUNT_COLUMNS = ["n_time_rows", "n_nonempty_rows", "n_publish_rows"]


def _prepare_rows(df: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing:
        raise ValueError(
            "quality-order CSV is missing required columns: {}".format(
                ", ".join(missing)
            )
        )

    work = df.copy()
    work["source"] = work["source"].fillna("").astype(str).str.strip()
    work["resolution"] = work["resolution"].fillna("").astype(str).str.strip()
    work["path"] = work["path"].fillna("").astype(str).str.strip()

    for col in COUNT_COLUMNS:
        work[col] = pd.to_numeric(work[col], errors="coerce")
        if work[col].isna().any():
            n_bad = int(work[col].isna().sum())
            raise ValueError(
                "column '{}' contains {} non-numeric/missing values".format(col, n_bad)
            )
        work[col] = work[col].astype(np.int64)

    invalid = (
        (work["n_time_rows"] < 0)
        | (work["n_nonempty_rows"] < 0)
        | (work["n_publish_rows"] < 0)
        | (work["n_nonempty_rows"] > work["n_time_rows"])
        | (work["n_publish_rows"] > work["n_nonempty_rows"])
    )
    if invalid.any():
        cols = [
            c
            for c in [
                "source",
                "resolution",
                "cluster_id",
                "path",
                "n_time_rows",
                "n_nonempty_rows",
                "n_publish_rows",
            ]
            if c in work.columns
        ]
        sample = work.loc[invalid, cols].head(20).to_string(index=False)
        raise ValueError(
            "inconsistent s6 filter counters detected in {} rows; sample:\n{}".format(
                int(invalid.sum()), sample
            )
        )

    work["records_before_filter"] = work["n_time_rows"]
    work["retained_records"] = work["n_publish_rows"]
    work["deleted_no_ssc_or_ssl"] = (
        work["n_time_rows"] - work["n_publish_rows"]
    )
    work["deleted_q_only"] = (
        work["n_nonempty_rows"] - work["n_publish_rows"]
    )
    work["deleted_all_missing"] = (
        work["n_time_rows"] - work["n_nonempty_rows"]
    )
    return work


def _aggregate(work: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    count_cols = [
        "records_before_filter",
        "retained_records",
        "deleted_no_ssc_or_ssl",
        "deleted_q_only",
        "deleted_all_missing",
    ]
    grouped = (
        work.groupby(group_cols, dropna=False, sort=True)
        .agg(
            candidate_rows=("path", "size"),
            unique_files=("path", "nunique"),
            **{col: (col, "sum") for col in count_cols},
        )
        .reset_index()
    )

    before = grouped["records_before_filter"].to_numpy(dtype=float)
    retained = grouped["retained_records"].to_numpy(dtype=float)
    deleted = grouped["deleted_no_ssc_or_ssl"].to_numpy(dtype=float)
    grouped["retention_rate_pct"] = np.where(
        before > 0, retained / before * 100.0, np.nan
    )
    grouped["deletion_rate_pct"] = np.where(
        before > 0, deleted / before * 100.0, np.nan
    )

    order = list(group_cols) + [
        "candidate_rows",
        "unique_files",
        "records_before_filter",
        "retained_records",
        "deleted_no_ssc_or_ssl",
        "deleted_q_only",
        "deleted_all_missing",
        "retention_rate_pct",
        "deletion_rate_pct",
    ]
    return grouped[order]


def build_summaries(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = _prepare_rows(df)
    by_source = _aggregate(work, ["source"])
    by_source_resolution = _aggregate(work, ["source", "resolution"])
    return by_source, by_source_resolution


def _add_all_sources_row(by_source: pd.DataFrame) -> pd.DataFrame:
    if by_source.empty:
        return by_source

    sum_cols = [
        "candidate_rows",
        "unique_files",
        "records_before_filter",
        "retained_records",
        "deleted_no_ssc_or_ssl",
        "deleted_q_only",
        "deleted_all_missing",
    ]
    total = {col: int(by_source[col].sum()) for col in sum_cols}
    total["source"] = "__ALL_SOURCES__"
    before = float(total["records_before_filter"])
    total["retention_rate_pct"] = (
        100.0 * float(total["retained_records"]) / before if before > 0 else np.nan
    )
    total["deletion_rate_pct"] = (
        100.0 * float(total["deleted_no_ssc_or_ssl"]) / before
        if before > 0
        else np.nan
    )
    return pd.concat([by_source, pd.DataFrame([total])], ignore_index=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate the s6 SSC/SSL publish-filter counters by source dataset."
        )
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="s6_cluster_quality_order.csv path (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_SOURCE_OUTPUT),
        help="source-level summary CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--resolution-output",
        default=str(DEFAULT_SOURCE_RES_OUTPUT),
        help="source + resolution summary CSV (default: %(default)s)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    resolution_output_path = Path(args.resolution_output)

    if not input_path.is_file():
        print("Error: quality-order CSV not found: {}".format(input_path))
        return 1

    df = pd.read_csv(input_path)
    by_source, by_source_resolution = build_summaries(df)
    by_source_with_total = _add_all_sources_row(by_source)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    resolution_output_path.parent.mkdir(parents=True, exist_ok=True)
    by_source_with_total.to_csv(output_path, index=False, float_format="%.4f")
    by_source_resolution.to_csv(
        resolution_output_path, index=False, float_format="%.4f"
    )

    print("Sediment publish-filter summary (SSC present OR SSL present):")
    if by_source.empty:
        print("  no candidate rows found")
    else:
        display_cols = [
            "source",
            "records_before_filter",
            "retained_records",
            "deleted_no_ssc_or_ssl",
            "retention_rate_pct",
        ]
        print(by_source[display_cols].to_string(index=False, float_format="{:.2f}".format))

    print("Wrote source summary: {}".format(output_path))
    print("Wrote source-resolution summary: {}".format(resolution_output_path))
    print(
        "Note: deleted_no_ssc_or_ssl = deleted_q_only + deleted_all_missing; "
        "counts are evaluated after final flags 3/9 have been masked to NaN by s6."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
