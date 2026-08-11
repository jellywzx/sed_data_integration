#!/usr/bin/env python3
"""Build manuscript Table 7 from stats_release/qc_flags statistical output.

This module does not read release NetCDF files. Its direct upstream input is the
matrix final-flag statistics CSV written into the ``stats_release/qc_flags``
output tree. This keeps the manuscript table's data lineage explicit:

release matrices -> stats_release/qc_flags statistics -> manuscript Table 7.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import add_common_args, context_from_args, write_csv, write_markdown
from stats_release.release_paths import DEFAULT_STATS_ROOT
from stats_release.reporting import display_path, safe_lines


VARIABLES = ("Q", "SSC", "SSL")
FINAL_FLAG_VARIABLES = tuple("{}_flag".format(v) for v in VARIABLES)
EXPECTED_RESOLUTIONS = ("daily", "monthly", "annual")
DEFAULT_QC_STATS_CSV = (
    DEFAULT_STATS_ROOT / "qc_flags" / "tables" / "table_qc_matrix_final_flags_by_resolution.csv"
)


def _percent(count: int, total: int) -> float:
    return 100.0 * count / total if total else 0.0


def _cell(count: int, total: int) -> str:
    return "{:,} ({:.1f} %)".format(int(count), _percent(int(count), int(total)))


def read_qc_stats(path: Path) -> pd.DataFrame:
    """Read and validate the direct stats_release/qc_flags input artifact."""
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(
            "QC statistics input not found: {}. Run `python3 -m stats_release.qc_flags_matrix "
            "--out-dir output_other/stats_release/qc_flags` first.".format(path)
        )

    frame = pd.read_csv(path)
    required = {"resolution", "flag_variable", "flag_value", "count"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError("QC statistics input is missing required columns: {}".format(", ".join(missing)))

    frame = frame.copy()
    frame["resolution"] = frame["resolution"].astype(str)
    frame["flag_variable"] = frame["flag_variable"].astype(str)
    frame["flag_value"] = pd.to_numeric(frame["flag_value"], errors="raise").astype(int)
    frame["count"] = pd.to_numeric(frame["count"], errors="raise").astype(int)
    frame = frame[
        frame["resolution"].isin(EXPECTED_RESOLUTIONS)
        & frame["flag_variable"].isin(FINAL_FLAG_VARIABLES)
    ].copy()

    found_resolutions = set(frame["resolution"].unique())
    missing_resolutions = [r for r in EXPECTED_RESOLUTIONS if r not in found_resolutions]
    if missing_resolutions:
        raise ValueError(
            "QC statistics input does not contain all manuscript matrix resolutions; missing: {}".format(
                ", ".join(missing_resolutions)
            )
        )
    return frame


def build_table7(qc_stats: pd.DataFrame) -> pd.DataFrame:
    """Aggregate qc_flags statistics into the manuscript Table 7 layout."""
    rows = []
    for variable in VARIABLES:
        flag_var = "{}_flag".format(variable)
        sub = qc_stats[qc_stats["flag_variable"].eq(flag_var)].copy()
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
    return pd.DataFrame(rows)


def build_report(table7: pd.DataFrame, qc_stats: pd.DataFrame, qc_stats_path: Path, table_path: Path) -> list[str]:
    lines = [
        "# Manuscript Table 7 — Final quality-flag distribution",
        "",
        "This report does **not** read release NetCDF files directly. Its sole data input is the `stats_release/qc_flags` matrix final-flag statistics CSV. The lineage is therefore: release matrices → `stats_release/qc_flags` statistics → Manuscript Table 7.",
        "",
        "- Direct QC statistics input: `{}`".format(display_path(qc_stats_path)),
        "- Table 7 CSV output: `{}`".format(display_path(table_path)),
        "",
        "**Table 7. Final quality-flag distribution in the main station-reference matrices.** Counts and percentages were calculated across all records pooled from the daily, monthly, and annual matrices. Analysis-ready values are defined as flags 0–1. Suspect/bad values are defined as flags 2–3. Missing values are defined as flag 9.",
        "",
    ]

    display_cols = [
        "Variable",
        "Good, n (%)",
        "Derived, n (%)",
        "Analysis-ready, n (%)",
        "Suspect/bad, n (%)",
        "Missing, n (%)",
    ]
    lines.append(table7[display_cols].to_markdown(index=False))
    lines.extend(["", "## Upstream QC-statistics denominator check", ""])

    resolution_totals = (
        qc_stats.groupby(["resolution", "flag_variable"], dropna=False)["count"]
        .sum()
        .reset_index()
        .pivot(index="resolution", columns="flag_variable", values="count")
        .fillna(0)
        .astype(int)
        .reset_index()
    )
    for col in FINAL_FLAG_VARIABLES:
        if col not in resolution_totals.columns:
            resolution_totals[col] = 0
    resolution_totals = resolution_totals[["resolution", *FINAL_FLAG_VARIABLES]]
    lines.append(resolution_totals.to_markdown(index=False))
    lines.extend(
        [
            "",
            "These denominator checks come from the QC statistics artifact itself; Table 7 performs no second scan of the release products.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build manuscript Table 7 from stats_release/qc_flags output.")
    add_common_args(parser, "manuscript_table7_qc")
    parser.add_argument(
        "--qc-stats-csv",
        type=Path,
        default=DEFAULT_QC_STATS_CSV,
        help="Direct input CSV generated under stats_release/qc_flags/tables.",
    )
    args = parser.parse_args(argv)
    ctx = context_from_args(args)

    qc_stats_path = Path(args.qc_stats_csv).resolve()
    qc_stats = read_qc_stats(qc_stats_path)
    table7 = build_table7(qc_stats)

    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    table_path = tables_dir / "table_manuscript_table7_qc_flags.csv"
    upstream_copy_path = tables_dir / "table_manuscript_table7_qc_flags_by_resolution.csv"
    report_path = reports_dir / "manuscript_table7_qc_report.md"

    write_csv(table7, table_path)
    write_csv(qc_stats, upstream_copy_path)
    write_markdown(build_report(table7, qc_stats, qc_stats_path, table_path), report_path)

    print("Read QC statistics from {}".format(qc_stats_path))
    print("Wrote manuscript Table 7 to {}".format(table_path))
    print("Wrote manuscript Table 7 report to {}".format(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
