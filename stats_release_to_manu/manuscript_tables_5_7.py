#!/usr/bin/env python3
"""Build Manuscript Tables 5, 6, and 7 from stats_release CSV artifacts only.

No release NetCDF file is opened by this module. Direct data lineage:

- Table 5 <- stats_release/spatial + stats_release/temporal
- Table 6 <- stats_release/variable_summary
- Table 7 <- stats_release/qc_flags

The purpose is to keep manuscript-facing numbers reproducibly downstream of the
release statistics suite rather than independently rescanning release products.
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


RESOLUTIONS = ("daily", "monthly", "annual")
VARIABLES = ("Q", "SSC", "SSL")
FINAL_FLAG_VARIABLES = tuple("{}_flag".format(v) for v in VARIABLES)

DEFAULT_SPATIAL_BY_RESOLUTION = (
    DEFAULT_STATS_ROOT / "spatial" / "tables" / "table_spatial_coverage_by_resolution.csv"
)
DEFAULT_SPATIAL_SUMMARY = (
    DEFAULT_STATS_ROOT / "spatial" / "tables" / "table_spatial_coverage_summary.csv"
)
DEFAULT_TEMPORAL_BY_RESOLUTION = (
    DEFAULT_STATS_ROOT / "temporal" / "tables" / "table_temporal_coverage_by_resolution.csv"
)
DEFAULT_VARIABLE_COVERAGE = (
    DEFAULT_STATS_ROOT / "variable_summary" / "tables" / "table_variable_coverage_by_resolution.csv"
)
DEFAULT_FLAG01_SUMMARY = (
    DEFAULT_STATS_ROOT / "variable_summary" / "tables" / "table_flag01_summary_statistics.csv"
)
DEFAULT_QC_MATRIX_FLAGS = (
    DEFAULT_STATS_ROOT / "qc_flags" / "tables" / "table_qc_matrix_final_flags_by_resolution.csv"
)


TABLE5_RECOMMENDED_USE = {
    "daily": "Daily model evaluation, event-scale sediment dynamics, and station-level time-series extraction.",
    "monthly": "Monthly model evaluation, seasonal analysis, and comparison where daily records are sparse or highly variable.",
    "annual": "Annual sediment-load evaluation, long-term flux comparison, and basin-scale suspended-sediment flux assessment.",
    "all": "The station total is deduplicated across temporal resolutions, because some stations occur in more than one temporal-resolution product. Select one resolution or document any cross-resolution aggregation or comparison.",
}


def _read_csv(path: Path, required_columns: set[str], label: str) -> pd.DataFrame:
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError("{} statistics input not found: {}".format(label, path))
    frame = pd.read_csv(path)
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError("{} is missing required columns: {}".format(label, ", ".join(missing)))
    return frame


def _int(value) -> int:
    return int(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0).iloc[0])


def _float(value) -> float:
    return float(pd.to_numeric(pd.Series([value]), errors="coerce").fillna(float("nan")).iloc[0])


def _fmt_number(value) -> str:
    value = _float(value)
    if pd.isna(value):
        return "NA"
    if value == 0:
        return "0"
    av = abs(value)
    if av >= 1000:
        return "{:,.0f}".format(value)
    if av >= 100:
        return "{:.0f}".format(value)
    if av >= 10:
        return "{:.2f}".format(value).rstrip("0").rstrip(".")
    return "{:.2f}".format(value)


def _fmt_count_pct(count: int, total: int) -> str:
    pct = 100.0 * count / total if total else 0.0
    return "{:,} ({:.1f} %)".format(int(count), pct)


def _summary_metric(summary: pd.DataFrame, metric: str) -> int:
    if summary.empty or not {"metric", "value"}.issubset(summary.columns):
        return 0
    hit = summary[summary["metric"].astype(str).eq(metric)]
    return _int(hit.iloc[0]["value"]) if not hit.empty else 0


def build_table5(spatial_by_resolution: pd.DataFrame, spatial_summary: pd.DataFrame, temporal: pd.DataFrame) -> pd.DataFrame:
    """Build manuscript Table 5 from spatial and temporal stats artifacts."""
    s = spatial_by_resolution.copy()
    t = temporal.copy()
    s["resolution"] = s["resolution"].astype(str)
    t["resolution"] = t["resolution"].astype(str)

    rows = []
    for resolution in RESOLUTIONS:
        srow = s[s["resolution"].eq(resolution)]
        trow = t[t["resolution"].eq(resolution)]
        if srow.empty or trow.empty:
            raise ValueError("Missing {} row in spatial or temporal statistics for Table 5".format(resolution))
        srow = srow.iloc[0]
        trow = trow.iloc[0]
        station_count = _int(srow["reference_station_count"])
        record_count = _int(srow["record_count"])
        first_year = _int(trow["first_year"])
        last_year = _int(trow["last_year"])
        rows.append(
            {
                "Matrix product": "{} matrix".format(resolution.capitalize()),
                "Spatial unit": "Station",
                "Station count": station_count,
                "Record count": record_count,
                "Temporal span": "{}–{}".format(first_year, last_year),
                "Recommended use": TABLE5_RECOMMENDED_USE[resolution],
            }
        )

    unique_stations = _summary_metric(spatial_summary, "final_reference_station_count")
    if not unique_stations:
        # Fallback to any generic cluster_count metric used by older stats output.
        unique_stations = _summary_metric(spatial_summary, "reference_station_count")
    total_records = sum(int(row["Record count"]) for row in rows)
    first_year = min(int(str(row["Temporal span"]).split("–")[0]) for row in rows)
    last_year = max(int(str(row["Temporal span"]).split("–")[1]) for row in rows)
    rows.append(
        {
            "Matrix product": "All main matrices",
            "Spatial unit": "Unique station",
            "Station count": unique_stations,
            "Record count": total_records,
            "Temporal span": "{}–{}".format(first_year, last_year),
            "Recommended use": TABLE5_RECOMMENDED_USE["all"],
        }
    )
    return pd.DataFrame(rows)


def build_table6(variable_coverage: pd.DataFrame, flag01_summary: pd.DataFrame) -> pd.DataFrame:
    """Build manuscript Table 6 from variable_summary statistics artifacts."""
    coverage = variable_coverage.copy()
    stats = flag01_summary.copy()
    coverage["resolution"] = coverage["resolution"].astype(str)
    stats["resolution"] = stats["resolution"].astype(str)
    stats["variable"] = stats["variable"].astype(str)

    rows = []
    for resolution in RESOLUTIONS:
        crow = coverage[coverage["resolution"].eq(resolution)]
        if crow.empty:
            raise ValueError("Missing {} variable-coverage row for Table 6".format(resolution))
        crow = crow.iloc[0]
        total = _int(crow["n_records_total"])
        for variable in VARIABLES:
            count_col = "{}_records".format(variable)
            if count_col not in crow.index:
                raise ValueError("Variable coverage statistics missing column {}".format(count_col))
            nonmissing = _int(crow[count_col])
            srow = stats[stats["resolution"].eq(resolution) & stats["variable"].eq(variable)]
            if srow.empty:
                raise ValueError("Missing flag 0–1 summary row for {} {}".format(resolution, variable))
            srow = srow.iloc[0]
            unit = str(srow.get("unit", "")).strip()
            label = "{} ({})".format(variable, unit) if unit else variable
            rows.append(
                {
                    "Resolution": resolution.capitalize(),
                    "Variable": label,
                    "Non-missing values, n (%)": _fmt_count_pct(nonmissing, total),
                    "Mean": _fmt_number(srow["mean"]),
                    "Median": _fmt_number(srow["median"]),
                    "P05–P95": "{}–{}".format(_fmt_number(srow["p05"]), _fmt_number(srow["p95"])),
                    "P99": _fmt_number(srow["p99"]),
                    "total_records": total,
                    "nonmissing_count": nonmissing,
                    "nonmissing_percent": round(100.0 * nonmissing / total, 6) if total else 0.0,
                    "analysis_ready_count": _int(srow.get("n_flag01_records", 0)),
                }
            )
    return pd.DataFrame(rows)


def build_table7(qc_stats: pd.DataFrame) -> pd.DataFrame:
    """Build manuscript Table 7 from qc_flags statistics artifact."""
    frame = qc_stats.copy()
    frame["resolution"] = frame["resolution"].astype(str)
    frame["flag_variable"] = frame["flag_variable"].astype(str)
    frame["flag_value"] = pd.to_numeric(frame["flag_value"], errors="raise").astype(int)
    frame["count"] = pd.to_numeric(frame["count"], errors="raise").astype(int)
    frame = frame[
        frame["resolution"].isin(RESOLUTIONS) & frame["flag_variable"].isin(FINAL_FLAG_VARIABLES)
    ].copy()

    missing_res = [r for r in RESOLUTIONS if r not in set(frame["resolution"])]
    if missing_res:
        raise ValueError("QC statistics missing matrix resolutions: {}".format(", ".join(missing_res)))

    rows = []
    for variable in VARIABLES:
        flag_var = "{}_flag".format(variable)
        sub = frame[frame["flag_variable"].eq(flag_var)]
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
                "Good, n (%)": _fmt_count_pct(good, total),
                "Derived, n (%)": _fmt_count_pct(derived, total),
                "Analysis-ready, n (%)": _fmt_count_pct(analysis_ready, total),
                "Suspect/bad, n (%)": _fmt_count_pct(suspect_bad, total),
                "Missing, n (%)": _fmt_count_pct(missing, total),
                "total_records": total,
                "good_count": good,
                "derived_count": derived,
                "analysis_ready_count": analysis_ready,
                "suspect_bad_count": suspect_bad,
                "missing_count": missing,
            }
        )
    return pd.DataFrame(rows)


def build_report(
    table5: pd.DataFrame,
    table6: pd.DataFrame,
    table7: pd.DataFrame,
    paths: dict[str, Path],
) -> list[str]:
    lines = [
        "# Manuscript Tables 5–7 — stats_release-derived report",
        "",
        "This report is generated entirely from `stats_release/` statistical artifacts. The manuscript table builder does not open release NetCDF files directly.",
        "",
        "## Data lineage",
        "",
        "- **Table 5:** `stats_release/spatial` + `stats_release/temporal`",
        "- **Table 6:** `stats_release/variable_summary`",
        "- **Table 7:** `stats_release/qc_flags`",
        "",
        "### Direct upstream files",
        "",
    ]
    for label, path in paths.items():
        lines.append("- {}: `{}`".format(label, display_path(path)))

    lines.extend(
        [
            "",
            "## Table 5",
            "",
            "**Table 5. Summary statistics and recommended applications of the main station-reference matrix products.**",
            "",
            table5.to_markdown(index=False),
            "",
            "## Table 6",
            "",
            "**Table 6. Variable availability and distribution statistics for the main station-reference matrices.** For each temporal resolution and variable, ‘Non-missing values, n (%)’ is derived from the variable-coverage statistics relative to the total number of records in that matrix. Median and percentile statistics come from the flag 0–1 analysis-ready summary generated by `stats_release/variable_summary`.",
            "",
            table6[["Resolution", "Variable", "Non-missing values, n (%)", "Mean", "Median", "P05–P95", "P99"]].to_markdown(index=False),
            "",
            "## Table 7",
            "",
            "**Table 7. Final quality-flag distribution in the main station-reference matrices.** Counts and percentages are pooled across daily, monthly, and annual matrix QC statistics. Analysis-ready values are flags 0–1, suspect/bad values are flags 2–3, and missing values are flag 9.",
            "",
            table7[["Variable", "Good, n (%)", "Derived, n (%)", "Analysis-ready, n (%)", "Suspect/bad, n (%)", "Missing, n (%)"]].to_markdown(index=False),
            "",
            "## Reproducibility",
            "",
            "Re-run the corresponding `stats_release` modules first, then re-run this manuscript table builder. Any changes in the upstream release statistics will propagate automatically into Tables 5–7.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build Manuscript Tables 5–7 from stats_release CSV outputs only.")
    add_common_args(parser, "manuscript_tables_5_7")
    parser.add_argument("--spatial-by-resolution", type=Path, default=DEFAULT_SPATIAL_BY_RESOLUTION)
    parser.add_argument("--spatial-summary", type=Path, default=DEFAULT_SPATIAL_SUMMARY)
    parser.add_argument("--temporal-by-resolution", type=Path, default=DEFAULT_TEMPORAL_BY_RESOLUTION)
    parser.add_argument("--variable-coverage", type=Path, default=DEFAULT_VARIABLE_COVERAGE)
    parser.add_argument("--flag01-summary", type=Path, default=DEFAULT_FLAG01_SUMMARY)
    parser.add_argument("--qc-matrix-flags", type=Path, default=DEFAULT_QC_MATRIX_FLAGS)
    args = parser.parse_args(argv)
    ctx = context_from_args(args)

    paths = {
        "Spatial coverage by resolution": Path(args.spatial_by_resolution).resolve(),
        "Spatial summary": Path(args.spatial_summary).resolve(),
        "Temporal coverage by resolution": Path(args.temporal_by_resolution).resolve(),
        "Variable coverage by resolution": Path(args.variable_coverage).resolve(),
        "Flag 0–1 summary statistics": Path(args.flag01_summary).resolve(),
        "Matrix final QC flags": Path(args.qc_matrix_flags).resolve(),
    }

    spatial_by_resolution = _read_csv(
        paths["Spatial coverage by resolution"],
        {"resolution", "reference_station_count", "record_count"},
        "spatial coverage by resolution",
    )
    spatial_summary = _read_csv(
        paths["Spatial summary"], {"metric", "value"}, "spatial summary"
    )
    temporal = _read_csv(
        paths["Temporal coverage by resolution"],
        {"resolution", "first_year", "last_year"},
        "temporal coverage by resolution",
    )
    variable_coverage = _read_csv(
        paths["Variable coverage by resolution"],
        {"resolution", "n_records_total", "Q_records", "SSC_records", "SSL_records"},
        "variable coverage by resolution",
    )
    flag01_summary = _read_csv(
        paths["Flag 0–1 summary statistics"],
        {"resolution", "variable", "n_flag01_records", "mean", "median", "p05", "p95", "p99", "unit"},
        "flag 0–1 summary statistics",
    )
    qc_stats = _read_csv(
        paths["Matrix final QC flags"],
        {"resolution", "flag_variable", "flag_value", "count"},
        "matrix final QC flags",
    )

    table5 = build_table5(spatial_by_resolution, spatial_summary, temporal)
    table6 = build_table6(variable_coverage, flag01_summary)
    table7 = build_table7(qc_stats)

    docs_dir = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/stats_release_to_manu/docs")
    tables_dir = docs_dir / "tables"
    reports_dir = docs_dir / "reports"
    table5_path = tables_dir / "table_manuscript_table5.csv"
    table6_path = tables_dir / "table_manuscript_table6.csv"
    table7_path = tables_dir / "table_manuscript_table7.csv"
    report_path = reports_dir / "manuscript_tables_5_7_report.md"

    write_csv(table5, table5_path)
    write_csv(table6, table6_path)
    write_csv(table7, table7_path)
    write_markdown(build_report(table5, table6, table7, paths), report_path)

    print("Wrote Manuscript Table 5 to {}".format(table5_path))
    print("Wrote Manuscript Table 6 to {}".format(table6_path))
    print("Wrote Manuscript Table 7 to {}".format(table7_path))
    print("Wrote combined manuscript report to {}".format(report_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
