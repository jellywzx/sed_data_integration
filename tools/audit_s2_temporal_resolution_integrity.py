#!/usr/bin/env python3
"""Audit S2 output temporal-resolution integrity.

This script checks files in output_resolution_organized. For daily/monthly/annual
outputs, a file is flagged when one native day/month/year contains more than one
populated sediment observation.
"""

from __future__ import annotations

import argparse
import os
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from pandas.errors import EmptyDataError


DEFAULT_REPO = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test"
)
DEFAULT_ORGANIZED = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/output_resolution_organized"
)
DEFAULT_OUTDIR = DEFAULT_REPO / "output" / "s2_temporal_resolution_integrity_audit"
DEFAULT_CLASSIFICATION = DEFAULT_REPO / "output" / "s2_resolution_classification_details.csv"

RESOLUTIONS = ["daily", "monthly", "annual", "climatology", "other"]
TIME_CANDIDATES = ["time", "Time", "t", "datetime", "date", "sample"]
SEDIMENT_CANDIDATES = [
    "SSC",
    "ssc",
    "TSS_mg_L",
    "TSS",
    "SSL",
    "sediment_load",
    "Sediment_load",
]


def decode_times(values):
    arr = np.asarray(values)
    if arr.size == 0:
        return pd.DatetimeIndex([])
    return pd.to_datetime(arr.ravel(), errors="coerce")


def valid_mask_for_sediment(ds, time_name, n_time):
    masks = []
    for name in SEDIMENT_CANDIDATES:
        if name not in ds.variables:
            continue
        var = ds[name]
        try:
            values = np.asarray(var.values)
        except Exception:
            continue
        if time_name in var.dims:
            axis = var.dims.index(time_name)
            moved = np.moveaxis(values, axis, 0)
            flat = moved.reshape((moved.shape[0], -1))
        elif values.shape and values.shape[0] == n_time:
            flat = values.reshape((values.shape[0], -1))
        else:
            continue
        if flat.shape[0] != n_time:
            continue
        if np.issubdtype(flat.dtype, np.number):
            masks.append(np.any(np.isfinite(flat), axis=1))
        else:
            masks.append(np.any(pd.notna(flat), axis=1))
    if not masks:
        return np.ones(n_time, dtype=bool)
    mask = np.zeros(n_time, dtype=bool)
    for item in masks:
        mask |= item
    return mask


def period_index(times, resolution):
    if resolution == "daily":
        return times.to_period("D").astype(str)
    if resolution == "monthly":
        return times.to_period("M").astype(str)
    if resolution == "annual":
        return times.to_period("Y").astype(str)
    return None


def audit_one(args):
    resolution, path_str, organized_str, source_lookup = args
    organized = Path(organized_str)
    path = Path(path_str)
    try:
        rel = str(path.relative_to(organized))
    except Exception:
        rel = str(path)
    s2_dest = str(path)
    source = source_lookup.get(s2_dest) or path.name.split("_", 1)[0]
    try:
        with xr.open_dataset(path, decode_times=True, mask_and_scale=True) as ds:
            time_name = next((c for c in TIME_CANDIDATES if c in ds.variables), None)
            if time_name is None:
                return [], [], {
                    "file": s2_dest,
                    "resolution_dir": resolution,
                    "source": source,
                    "error": "no_time_variable",
                }
            times = decode_times(ds[time_name].values)
            if len(times) == 0:
                return [], [], {
                    "file": s2_dest,
                    "resolution_dir": resolution,
                    "source": source,
                    "error": "empty_time",
                }
            mask = valid_mask_for_sediment(ds, time_name, len(times))
            observed = pd.DatetimeIndex(times[mask]).dropna()
            if len(observed) == 0:
                return [], [], None

            rows = []
            violations = []
            if resolution in ("daily", "monthly", "annual"):
                periods = period_index(observed, resolution)
                dates = observed.strftime("%Y-%m-%d")
                frame = pd.DataFrame({"native_period": periods, "record_date": dates})
                grouped = (
                    frame.groupby("native_period")["record_date"]
                    .agg(
                        record_count="size",
                        n_unique_dates=lambda s: s.nunique(),
                        first_date="min",
                        last_date="max",
                        sample_dates=lambda s: ";".join(list(pd.unique(s))[:12]),
                    )
                    .reset_index()
                )
                for row in grouped.itertuples(index=False):
                    rec = {
                        "resolution_dir": resolution,
                        "source": source,
                        "file": s2_dest,
                        "relative_file": rel,
                        "native_period": row.native_period,
                        "record_count": int(row.record_count),
                        "n_unique_dates": int(row.n_unique_dates),
                        "first_date": row.first_date,
                        "last_date": row.last_date,
                        "sample_dates": row.sample_dates,
                    }
                    rows.append(rec)
                    if int(row.record_count) > 1:
                        violations.append(rec)
            else:
                rows.append(
                    {
                        "resolution_dir": resolution,
                        "source": source,
                        "file": s2_dest,
                        "relative_file": rel,
                        "native_period": "",
                        "record_count": int(len(observed)),
                        "n_unique_dates": int(
                            pd.Index(observed.strftime("%Y-%m-%d")).nunique()
                        ),
                        "first_date": observed.min().strftime("%Y-%m-%d"),
                        "last_date": observed.max().strftime("%Y-%m-%d"),
                        "sample_dates": ";".join(
                            list(pd.unique(observed.strftime("%Y-%m-%d")))[:12]
                        ),
                    }
                )
            return rows, violations, None
    except Exception as exc:
        return [], [], {
            "file": s2_dest,
            "resolution_dir": resolution,
            "source": source,
            "error": repr(exc),
            "traceback": traceback.format_exc(limit=3),
        }


def load_source_lookup(classification_path):
    if not classification_path.exists() or classification_path.stat().st_size == 0:
        return {}
    try:
        df = pd.read_csv(classification_path, dtype=str)
    except EmptyDataError:
        return {}
    lookup = {}
    if "s2_dest_path" in df.columns and "source" in df.columns:
        for row in df[["s2_dest_path", "source"]].dropna().itertuples(index=False):
            lookup[str(row.s2_dest_path)] = str(row.source)
    return lookup


def md_table(df, max_rows=None):
    if df is None or df.empty:
        return ""
    if max_rows is not None:
        df = df.head(max_rows)
    cols = list(df.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in df.iterrows():
        vals = []
        for col in cols:
            val = "" if pd.isna(row[col]) else str(row[col])
            vals.append(val.replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--organized-dir", type=Path, default=DEFAULT_ORGANIZED)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTDIR)
    parser.add_argument("--classification-csv", type=Path, default=DEFAULT_CLASSIFICATION)
    parser.add_argument("--workers", type=int, default=0)
    return parser.parse_args()


def main():
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    source_lookup = load_source_lookup(args.classification_csv)
    tasks = []
    for resolution in RESOLUTIONS:
        base = args.organized_dir / resolution
        if not base.exists():
            continue
        for path in sorted(base.rglob("*.nc")):
            tasks.append((resolution, str(path), str(args.organized_dir), source_lookup))

    workers = args.workers or min(32, max(1, (os.cpu_count() or 8) // 2))
    print(
        "Auditing {} s2 NetCDF files with {} workers on {}".format(
            len(tasks), workers, os.uname().nodename
        ),
        flush=True,
    )

    rows = []
    violations = []
    errors = []
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(audit_one, task) for task in tasks]
        for fut in as_completed(futures):
            r, v, e = fut.result()
            rows.extend(r)
            violations.extend(v)
            if e:
                errors.append(e)
            done += 1
            if done % 2000 == 0 or done == len(tasks):
                print("  done {}/{} files".format(done, len(tasks)), flush=True)

    period_df = pd.DataFrame(rows)
    violation_df = pd.DataFrame(violations)
    error_df = pd.DataFrame(errors)

    period_path = args.out_dir / "s2_temporal_period_counts_by_file.csv"
    violation_path = args.out_dir / "s2_native_resolution_violations.csv"
    summary_path = args.out_dir / "s2_product_integrity_summary.csv"
    source_path = args.out_dir / "s2_native_resolution_violations_by_source.csv"
    error_path = args.out_dir / "s2_audit_errors.csv"
    report_path = args.out_dir / "s2_temporal_resolution_integrity_report.md"

    period_df.to_csv(period_path, index=False)
    violation_df.to_csv(violation_path, index=False)
    error_df.to_csv(error_path, index=False)

    file_counts = pd.DataFrame(
        [
            {
                "resolution_dir": resolution,
                "files_scanned": sum(1 for task in tasks if task[0] == resolution),
            }
            for resolution in RESOLUTIONS
        ]
    )
    if violation_df.empty:
        v_summary = pd.DataFrame(
            columns=[
                "resolution_dir",
                "files_with_native_period_gt1",
                "offending_native_periods",
                "max_records_in_native_period",
            ]
        )
        v_source = pd.DataFrame(
            columns=[
                "resolution_dir",
                "source",
                "files_with_native_period_gt1",
                "offending_native_periods",
                "max_records_in_native_period",
            ]
        )
    else:
        v_summary = (
            violation_df.groupby("resolution_dir")
            .agg(
                files_with_native_period_gt1=("file", "nunique"),
                offending_native_periods=("native_period", "count"),
                max_records_in_native_period=("record_count", "max"),
            )
            .reset_index()
        )
        v_source = (
            violation_df.groupby(["resolution_dir", "source"])
            .agg(
                files_with_native_period_gt1=("file", "nunique"),
                offending_native_periods=("native_period", "count"),
                max_records_in_native_period=("record_count", "max"),
            )
            .reset_index()
            .sort_values(
                ["resolution_dir", "offending_native_periods"],
                ascending=[True, False],
            )
        )

    summary = file_counts.merge(v_summary, on="resolution_dir", how="left").fillna(0)
    for col in [
        "files_scanned",
        "files_with_native_period_gt1",
        "offending_native_periods",
        "max_records_in_native_period",
    ]:
        if col in summary.columns:
            summary[col] = summary[col].astype(int)
    summary.to_csv(summary_path, index=False)
    v_source.to_csv(source_path, index=False)

    lines = [
        "# S2 Temporal Resolution Integrity Audit",
        "",
        "- Host: `{}`".format(os.uname().nodename),
        "- Input: `{}`".format(args.organized_dir),
        "- Files scanned: **{}**".format(len(tasks)),
        "- Rule: daily/monthly/annual outputs are flagged when a native day/month/year contains more than one populated sediment observation.",
        "",
        "## Summary",
        "",
        md_table(summary) if not summary.empty else "No summary found.",
        "",
        "## Violations By Source",
        "",
        md_table(v_source, 50)
        if not v_source.empty
        else "No native-period multiplicity violations found.",
        "",
        "## Outputs",
        "",
    ]
    for path in [period_path, violation_path, summary_path, source_path, error_path]:
        lines.append("- `{}`".format(path))
    if not error_df.empty:
        lines.extend(["", "## Read Errors", "", md_table(error_df, 30)])
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("\nSummary:")
    print(summary.to_string(index=False))
    print("\nViolations by source top 30:")
    print("none" if v_source.empty else v_source.head(30).to_string(index=False))
    print("\nErrors:", len(error_df))
    print("Report:", report_path)


if __name__ == "__main__":
    main()
