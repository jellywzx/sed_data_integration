#!/usr/bin/env python3
"""Build the manuscript Table 7 QC-flag summary from release matrix products.

This module intentionally pools the daily, monthly, and annual station-reference
matrices, matching the manuscript definition of "all records pooled from the
daily, monthly, and annual matrices". It does not use sed_reference_master.nc,
because that file can contain a different record axis from the three published
matrix products.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import add_common_args, context_from_args, write_csv, write_markdown
from stats_release.release_paths import MATRIX_PRODUCTS
from stats_release.reporting import display_path, safe_lines


VARIABLES = ("Q", "SSC", "SSL")
FINAL_FLAGS = (0, 1, 2, 3, 9)


def _percent(count: int, total: int) -> float:
    return 100.0 * count / total if total else 0.0


def _cell(count: int, total: int) -> str:
    return "{:,} ({:.1f} %)".format(int(count), _percent(int(count), int(total)))


def _count_matrix_final_flags(ctx, file_name: str, resolution: str, chunk_size: int) -> pd.DataFrame:
    """Count final flags for a matrix product without assuming an n_records axis."""
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame()

    rows = []
    with ctx.open_dataset(file_name, required=True) as ds:
        for variable in VARIABLES:
            flag_name = "{}_flag".format(variable)
            if flag_name not in ds.variables:
                continue
            var = ds.variables[flag_name]
            shape = tuple(int(v) for v in var.shape)
            if not shape:
                continue

            trailing = int(np.prod(shape[1:], dtype=np.int64)) if len(shape) > 1 else 1
            axis_chunk = max(1, int(chunk_size) // max(1, trailing))
            counts = {flag: 0 for flag in FINAL_FLAGS}
            extra_counts = {}

            for start in range(0, shape[0], axis_chunk):
                stop = min(start + axis_chunk, shape[0])
                arr = np.ma.asarray(var[start:stop]).filled(9).reshape(-1)
                numeric = pd.to_numeric(pd.Series(arr), errors="coerce").dropna().astype(int).to_numpy()
                if numeric.size == 0:
                    continue
                values, value_counts = np.unique(numeric, return_counts=True)
                for flag, count in zip(values, value_counts):
                    flag = int(flag)
                    count = int(count)
                    if flag in counts:
                        counts[flag] += count
                    else:
                        extra_counts[flag] = extra_counts.get(flag, 0) + count

            total = int(sum(counts.values()) + sum(extra_counts.values()))
            for flag in FINAL_FLAGS:
                count = int(counts[flag])
                rows.append(
                    {
                        "product": resolution,
                        "flag_variable": flag_name,
                        "flag_value": flag,
                        "count": count,
                        "percent": round(_percent(count, total), 6),
                        "n_total": total,
                    }
                )
            for flag, count in sorted(extra_counts.items()):
                rows.append(
                    {
                        "product": resolution,
                        "flag_variable": flag_name,
                        "flag_value": int(flag),
                        "count": int(count),
                        "percent": round(_percent(int(count), total), 6),
                        "n_total": total,
                    }
                )

    return pd.DataFrame(rows)


def build_table7(ctx, chunk_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return manuscript Table 7 and the pooled resolution-level flag counts."""
    pieces = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        counts = _count_matrix_final_flags(ctx, file_name, resolution, chunk_size)
        if not counts.empty:
            pieces.append(counts)

    pooled = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    if pooled.empty:
        return pd.DataFrame(), pooled

    rows = []
    for variable in VARIABLES:
        flag_var = "{}_flag".format(variable)
        sub = pooled[pooled["flag_variable"].eq(flag_var)].copy()
        by_flag = sub.groupby("flag_value", dropna=False)["count"].sum().to_dict()
        total = int(sum(int(v) for v in by_flag.values()))

        good = int(by_flag.get(0, 0))
        derived = int(by_flag.get(1, 0))
        suspect = int(by_flag.get(2, 0))
        bad = int(by_flag.get(3, 0))
        missing = int(by_flag.get(9, 0))
        analysis_ready = good + derived
        suspect_bad = suspect + bad

        rows.append(
            {
                "Variable": variable,
                "Good, n (%)": _cell(good, total),
                "Derived, n (%)": _cell(derived, total),
                "Analysis-ready, n (%)": _cell(analysis_ready, total),
                "Suspect/bad, n (%)": _cell(suspect_bad, total),
                "Missing, n (%)": _cell(missing, total),
                "total_records": total,
                "good_count": good,
                "derived_count": derived,
                "analysis_ready_count": analysis_ready,
                "suspect_count": suspect,
                "bad_count": bad,
                "suspect_bad_count": suspect_bad,
                "missing_count": missing,
                "good_percent": round(_percent(good, total), 6),
                "derived_percent": round(_percent(derived, total), 6),
                "analysis_ready_percent": round(_percent(analysis_ready, total), 6),
                "suspect_bad_percent": round(_percent(suspect_bad, total), 6),
                "missing_percent": round(_percent(missing, total), 6),
            }
        )

    return pd.DataFrame(rows), pooled


def build_report(ctx, table7: pd.DataFrame, pooled: pd.DataFrame, table_path: Path) -> list[str]:
    lines = [
        "# Manuscript Table 7 — Final quality-flag distribution",
        "",
        "This table is generated directly from the final `Q_flag`, `SSC_flag`, and `SSL_flag` variables in the daily, monthly, and annual station-reference matrices. Counts are pooled across the three temporal-resolution products, matching the manuscript Table 7 definition. Re-running this module updates the table automatically from the current release files.",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- CSV output: `{}`".format(display_path(table_path)),
        "",
        "**Table 7. Final quality-flag distribution in the main station-reference matrices.** Counts and percentages were calculated across all records pooled from the daily, monthly, and annual matrices. Analysis-ready values are defined as flags 0–1. Suspect/bad values are defined as flags 2–3. Missing values are defined as flag 9.",
        "",
    ]

    if table7.empty:
        lines.append("No matrix QC-flag records were available.")
        return safe_lines(lines)

    display_cols = [
        "Variable",
        "Good, n (%)",
        "Derived, n (%)",
        "Analysis-ready, n (%)",
        "Suspect/bad, n (%)",
        "Missing, n (%)",
    ]
    lines.append(table7[display_cols].to_markdown(index=False))
    lines.extend(["", "## Record-count check", ""])

    resolution_totals = (
        pooled.groupby(["product", "flag_variable"], dropna=False)["count"]
        .sum()
        .reset_index()
        .pivot(index="product", columns="flag_variable", values="count")
        .fillna(0)
        .astype(int)
        .reset_index()
        .rename(columns={"product": "resolution"})
    )
    for col in ("Q_flag", "SSC_flag", "SSL_flag"):
        if col not in resolution_totals.columns:
            resolution_totals[col] = 0
    resolution_totals = resolution_totals[["resolution", "Q_flag", "SSC_flag", "SSL_flag"]]
    lines.append(resolution_totals.to_markdown(index=False))
    lines.extend(
        [
            "",
            "The pooled denominator for each variable should equal the total number of records across the daily, monthly, and annual matrices. If these totals diverge across variables, inspect the release flag arrays before using the manuscript table.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build manuscript Table 7 from matrix QC flags.")
    add_common_args(parser, "manuscript_table7_qc")
    parser.add_argument("--chunk-size", type=int, default=500000)
    args = parser.parse_args(argv)
    ctx = context_from_args(args)

    table7, pooled = build_table7(ctx, max(1, int(args.chunk_size)))
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    table_path = tables_dir / "table_manuscript_table7_qc_flags.csv"
    pooled_path = tables_dir / "table_manuscript_table7_qc_flags_by_resolution.csv"
    report_path = reports_dir / "manuscript_table7_qc_report.md"

    write_csv(table7, table_path)
    write_csv(pooled, pooled_path)
    write_markdown(build_report(ctx, table7, pooled, table_path), report_path)

    print("Wrote manuscript Table 7 to {}".format(table_path))
    print("Wrote manuscript Table 7 report to {}".format(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
