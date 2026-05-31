#!/usr/bin/env python3
"""Write an ESSD-ready temporal coverage results report from S8 stats tables."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_ROOT = SCRIPT_DIR.parent
DEFAULT_STATS_DIR = SCRIPT_ROOT / "output_other" / "temporal_coverage_stats"
DEFAULT_TABLES_DIR = DEFAULT_STATS_DIR / "tables"
DEFAULT_FIGURES_DIR = DEFAULT_STATS_DIR / "figures"
DEFAULT_OUT = DEFAULT_STATS_DIR / "article_temporal_coverage_report.md"

MATRIX_RESOLUTIONS = ("daily", "monthly", "annual")
RESOLUTION_ORDER = ("daily", "monthly", "annual", "climatology")
CORE_VARS = ("Q", "SSC", "SSL")


def read_table(tables_dir: Path, name: str) -> pd.DataFrame:
    path = tables_dir / name
    if not path.is_file():
        raise FileNotFoundError("Required table not found: {}".format(path))
    return pd.read_csv(path)


def as_int(value) -> int:
    try:
        if pd.isna(value):
            return 0
        return int(value)
    except Exception:
        return 0


def fmt_int(value) -> str:
    return "{:,}".format(as_int(value))


def fmt_float(value, digits: int = 1) -> str:
    try:
        if pd.isna(value):
            return "n/a"
        return ("{:,." + str(digits) + "f}").format(float(value))
    except Exception:
        return "n/a"


def pct(value, digits: int = 1) -> str:
    try:
        if pd.isna(value):
            return "n/a"
        return ("{:,." + str(digits) + "f}%").format(float(value))
    except Exception:
        return "n/a"


def row_by_resolution(df: pd.DataFrame, resolution: str) -> Dict[str, object]:
    sub = df[df["resolution"] == resolution]
    if len(sub) == 0:
        return {}
    return sub.iloc[0].to_dict()


def markdown_table(rows: List[Dict[str, object]], columns: Iterable[str], headers: Iterable[str]) -> str:
    columns = list(columns)
    headers = list(headers)
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        values = [str(row.get(col, "")) for col in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def dataframe_markdown(df: pd.DataFrame) -> str:
    if len(df) == 0:
        return "_No rows available._"
    work = df.copy()
    work.insert(0, "resolution", work.index.astype(str))
    columns = [str(col) for col in work.columns]
    lines = []
    lines.append("| " + " | ".join(columns) + " |")
    lines.append("| " + " | ".join(["---"] * len(columns)) + " |")
    for row in work.itertuples(index=False):
        values = []
        for value in row:
            if isinstance(value, (float, np.floating)):
                values.append(fmt_float(value))
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def peak_rows(by_year: pd.DataFrame) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for resolution in MATRIX_RESOLUTIONS:
        sub = by_year[by_year["resolution"] == resolution].copy()
        if len(sub) == 0:
            continue
        idx = pd.to_numeric(sub["active_units"], errors="coerce").idxmax()
        peak = sub.loc[idx]
        rows.append(
            {
                "resolution": resolution,
                "year_range": "{}-{}".format(as_int(sub["year"].min()), as_int(sub["year"].max())),
                "peak_active_units": fmt_int(peak["active_units"]),
                "peak_year": as_int(peak["year"]),
                "records": fmt_int(sub["records_any"].sum()),
            }
        )
    return rows


def decade_table(by_year: pd.DataFrame) -> pd.DataFrame:
    work = by_year[by_year["resolution"].isin(MATRIX_RESOLUTIONS)].copy()
    if len(work) == 0:
        return pd.DataFrame()
    work["decade"] = (pd.to_numeric(work["year"], errors="coerce") // 10 * 10).astype(int)
    pivot = work.pivot_table(
        index="resolution",
        columns="decade",
        values="active_units",
        aggfunc="mean",
        fill_value=0,
    )
    return pivot.reindex(list(MATRIX_RESOLUTIONS)).fillna(0)


def product_summary_rows(summary: pd.DataFrame, long_records: pd.DataFrame) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    long_by_res = {row["resolution"]: row for row in long_records.to_dict("records")}
    for resolution in RESOLUTION_ORDER:
        row = row_by_resolution(summary, resolution)
        if not row:
            continue
        long_row = long_by_res.get(resolution, {})
        rows.append(
            {
                "resolution": resolution,
                "unit": row.get("unit_type", ""),
                "years": "{}-{}".format(as_int(row.get("first_year")), as_int(row.get("last_year"))),
                "units": fmt_int(row.get("active_units")),
                "records": fmt_int(row.get("record_count_any", row.get("records_any", 0))),
                "median_years": fmt_float(row.get("median_record_length_years")),
                "max_years": fmt_float(row.get("max_record_length_years")),
                "gt50": fmt_int(long_row.get("n_gt_50_years", 0)),
                "gt100": fmt_int(long_row.get("n_gt_100_years", 0)),
            }
        )
    return rows


def variable_summary_rows(variable: pd.DataFrame) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for resolution in RESOLUTION_ORDER:
        for var_name in CORE_VARS:
            sub = variable[(variable["resolution"] == resolution) & (variable["variable"] == var_name)]
            if len(sub) == 0:
                continue
            row = sub.iloc[0]
            rows.append(
                {
                    "resolution": resolution,
                    "variable": var_name,
                    "years": "{}-{}".format(as_int(row["first_year"]), as_int(row["last_year"])),
                    "units": fmt_int(row["active_units"]),
                    "records": fmt_int(row["record_count"]),
                    "peak": "{} in {}".format(fmt_int(row["peak_active_units"]), as_int(row["peak_active_year"])),
                }
            )
    return rows


def source_summary_rows(source: pd.DataFrame, top_n: int = 8) -> List[Dict[str, object]]:
    if len(source) == 0:
        return []
    work = source.copy()
    work["record_count"] = pd.to_numeric(work["record_count"], errors="coerce").fillna(0)
    work = work.sort_values("record_count", ascending=False).head(top_n)
    rows: List[Dict[str, object]] = []
    for row in work.to_dict("records"):
        rows.append(
            {
                "source": row.get("source_name", ""),
                "resolution": row.get("resolution", ""),
                "years": "{}-{}".format(as_int(row.get("first_year")), as_int(row.get("last_year"))),
                "clusters": fmt_int(row.get("clusters")),
                "records": fmt_int(row.get("record_count")),
                "median_years": fmt_float(row.get("median_record_length_years")),
                "max_years": fmt_float(row.get("max_record_length_years")),
            }
        )
    return rows


def figure_exists(figures_dir: Path, stem: str) -> str:
    png = figures_dir / "{}.png".format(stem)
    pdf = figures_dir / "{}.pdf".format(stem)
    if png.is_file() and pdf.is_file():
        return "`figures/{}.png` and `.pdf`".format(stem)
    if png.is_file():
        return "`figures/{}.png`".format(stem)
    if pdf.is_file():
        return "`figures/{}.pdf`".format(stem)
    return "`figures/{}` (not found)".format(stem)


def write_report(tables_dir: Path, figures_dir: Path, out_path: Path) -> Path:
    summary = read_table(tables_dir, "table_temporal_coverage_by_resolution.csv")
    variable = read_table(tables_dir, "table_temporal_coverage_by_variable.csv")
    long_records = read_table(tables_dir, "table_long_records_by_resolution.csv")
    by_year = read_table(tables_dir, "table_active_units_by_year.csv")
    source = read_table(tables_dir, "table_temporal_coverage_by_source.csv")
    distribution = read_table(tables_dir, "table_record_length_distribution.csv")
    region = read_table(tables_dir, "table_temporal_coverage_by_region_resolution.csv")

    daily = row_by_resolution(summary, "daily")
    monthly = row_by_resolution(summary, "monthly")
    annual = row_by_resolution(summary, "annual")
    climatology = row_by_resolution(summary, "climatology")
    all_first = as_int(summary["first_year"].min())
    all_last = as_int(summary["last_year"].max())

    product_rows = product_summary_rows(summary, long_records)
    variable_rows = variable_summary_rows(variable)
    source_rows = source_summary_rows(source)
    peaks = peak_rows(by_year)
    decades = decade_table(by_year)

    lines: List[str] = [
        "# Temporal Coverage Results for the S8 Sediment Reference Release",
        "",
        "## Purpose and Scope",
        "",
        (
            "This report summarizes the temporal coverage of the S8 release products for use in "
            "an ESSD manuscript. The main time-series products are the daily, monthly, and annual "
            "basin-cluster matrices. The climatology product is reported separately because its "
            "statistical unit is a standalone climatology station rather than a basin-cluster "
            "time-series unit."
        ),
        "",
        (
            "The satellite-validation product is not mixed into the main temporal coverage metrics. "
            "It has a different validation-oriented purpose and different statistical unit, so it "
            "should be summarized in a separate validation or supplementary section."
        ),
        "",
        "## Key Results",
        "",
        (
            "Across the released daily, monthly, annual, and climatological products, temporal "
            "coverage spans {}-{}. The main time-series products contain {} daily clusters, "
            "{} monthly clusters, and {} annual clusters."
        ).format(
            all_first,
            all_last,
            fmt_int(daily.get("active_units", 0)),
            fmt_int(monthly.get("active_units", 0)),
            fmt_int(annual.get("active_units", 0)),
        ),
        "",
        (
            "The daily product provides the broadest and longest high-frequency coverage, spanning "
            "{}-{} with {} valid cluster-time observations and a median record length of {} years. "
            "It contains {} clusters longer than 50 years and {} clusters longer than 100 years."
        ).format(
            as_int(daily.get("first_year")),
            as_int(daily.get("last_year")),
            fmt_int(daily.get("record_count_any", daily.get("records_any", 0))),
            fmt_float(daily.get("median_record_length_years")),
            fmt_int(daily.get("n_gt_50_years", 0)),
            fmt_int(daily.get("n_gt_100_years", 0)),
        ),
        "",
        (
            "The monthly product spans {}-{} with {} valid observations across {} clusters. "
            "Its median record length is {} years, and most monthly records are shorter than "
            "10 years, although the longest monthly cluster spans {} years."
        ).format(
            as_int(monthly.get("first_year")),
            as_int(monthly.get("last_year")),
            fmt_int(monthly.get("record_count_any", monthly.get("records_any", 0))),
            fmt_int(monthly.get("active_units", 0)),
            fmt_float(monthly.get("median_record_length_years")),
            fmt_float(monthly.get("max_record_length_years")),
        ),
        "",
        (
            "The annual product spans {}-{} with {} valid observations across {} clusters. "
            "Annual coverage is best described using observed records, calendar span, and record "
            "length because its time axis is not a regular annual grid."
        ).format(
            as_int(annual.get("first_year")),
            as_int(annual.get("last_year")),
            fmt_int(annual.get("record_count_any", annual.get("records_any", 0))),
            fmt_int(annual.get("active_units", 0)),
        ),
        "",
        (
            "The climatology product contains {} standalone climatology stations spanning "
            "{}-{}. It should be interpreted as a companion product rather than merged with the "
            "basin-cluster time-series coverage."
        ).format(
            fmt_int(climatology.get("active_units", 0)),
            as_int(climatology.get("first_year")),
            as_int(climatology.get("last_year")),
        ),
        "",
        "## Coverage by Product Resolution",
        "",
        markdown_table(
            product_rows,
            ["resolution", "unit", "years", "units", "records", "median_years", "max_years", "gt50", "gt100"],
            [
                "Product",
                "Statistical unit",
                "Years",
                "Active units",
                "Valid records",
                "Median length (yr)",
                "Max length (yr)",
                ">50 yr",
                ">100 yr",
            ],
        ),
        "",
        "Interpretation: the daily product dominates the total record count and contains the strongest long-record signal. The monthly product has many clusters but shorter median spans, while the annual product has relatively few clusters but includes several long annual records.",
        "",
        "## Variable-Specific Temporal Coverage",
        "",
        markdown_table(
            variable_rows,
            ["resolution", "variable", "years", "units", "records", "peak"],
            ["Product", "Variable", "Years", "Active units", "Records", "Peak active units"],
        ),
        "",
        (
            "Variable coverage differs most strongly in the daily product: Q is available for "
            "{} daily clusters, while SSC and SSL are available for {} and {} daily clusters, "
            "respectively. In the monthly product, all three variables are available for {} "
            "clusters. In the annual product, SSC has the widest active-unit coverage."
        ).format(
            fmt_int(variable[(variable["resolution"] == "daily") & (variable["variable"] == "Q")]["active_units"].iloc[0]),
            fmt_int(variable[(variable["resolution"] == "daily") & (variable["variable"] == "SSC")]["active_units"].iloc[0]),
            fmt_int(variable[(variable["resolution"] == "daily") & (variable["variable"] == "SSL")]["active_units"].iloc[0]),
            fmt_int(variable[(variable["resolution"] == "monthly") & (variable["variable"] == "Q")]["active_units"].iloc[0]),
        ),
        "",
        "## Record-Length Distribution",
        "",
        markdown_table(
            [
                {
                    "resolution": row["resolution"],
                    "n_units": fmt_int(row["n_units"]),
                    "p25": fmt_float(row["p25_years_equiv"]),
                    "median": fmt_float(row["median_years_equiv"]),
                    "mean": fmt_float(row["mean_years_equiv"]),
                    "p75": fmt_float(row["p75_years_equiv"]),
                    "p95": fmt_float(row["p95_years_equiv"]),
                    "max": fmt_float(row["max_years_equiv"]),
                }
                for row in distribution.to_dict("records")
            ],
            ["resolution", "n_units", "p25", "median", "mean", "p75", "p95", "max"],
            ["Product", "Units", "P25 (yr)", "Median (yr)", "Mean (yr)", "P75 (yr)", "P95 (yr)", "Max (yr)"],
        ),
        "",
        "The daily product has a highly skewed record-length distribution: the median length is 12.7 years, but the 95th percentile exceeds 100 years. This long upper tail is important for trend analysis and long-term sediment regime studies.",
        "",
        "## Temporal Evolution of Active Coverage",
        "",
        markdown_table(
            peaks,
            ["resolution", "year_range", "peak_active_units", "peak_year", "records"],
            ["Product", "Year range", "Peak active units", "Peak year", "Total records"],
        ),
        "",
        "Mean active clusters by decade:",
        "",
        dataframe_markdown(decades.round(1)),
        "",
        "The daily product expands rapidly through the mid-20th century and peaks in 1980. Monthly coverage increases most strongly after 2000 and peaks in 2013, reflecting the contribution of modern monthly source products. Annual coverage is sparse before 2000 and reaches its highest active-cluster count in 2018.",
        "",
        "## Source Contribution to Temporal Coverage",
        "",
        markdown_table(
            source_rows,
            ["source", "resolution", "years", "clusters", "records", "median_years", "max_years"],
            ["Source", "Product", "Years", "Clusters", "Records", "Median length (yr)", "Max length (yr)"],
        ),
        "",
        (
            "HYDAT is the dominant daily long-record contributor, providing more than 12 million "
            "daily records and most of the >50-year daily records. USGS contributes a large number "
            "of daily records with generally shorter spans, while Bayern contributes fewer daily "
            "clusters but includes multi-decadal records. EUSEDcollab and GFQA_v2 are the main "
            "monthly contributors by record count."
        ),
        "",
        "## Geographic and Regional Notes",
        "",
        (
            "The regional temporal table currently contains {} region-resolution rows. Region and "
            "country fields are complete for some major daily sources, such as HYDAT and Bayern, "
            "but many monthly and annual rows remain labelled as Unknown because the release "
            "catalog does not yet provide harmonized country or continent metadata for all source "
            "products."
        ).format(fmt_int(len(region))),
        "",
        "## Suggested ESSD Usage",
        "",
        "- Main-text table: use the `Coverage by Product Resolution` table above or `tables/table_temporal_coverage_by_resolution.csv`.",
        "- Main-text figures: use {}, {}, and {}.".format(
            figure_exists(figures_dir, "fig_active_units_by_year"),
            figure_exists(figures_dir, "fig_record_length_distribution"),
            figure_exists(figures_dir, "fig_temporal_coverage_heatmap"),
        ),
        "- Supplementary figures: use {}, {}, and {}.".format(
            figure_exists(figures_dir, "fig_records_by_year_variable"),
            figure_exists(figures_dir, "fig_long_record_counts"),
            figure_exists(figures_dir, "fig_source_temporal_span"),
        ),
        "- Supplementary tables: use the variable-level, source-level, region-level, and unit-level CSV tables in `tables/`.",
        "",
        "## Manuscript-Ready Text",
        "",
        (
            "The S8 release provides daily, monthly, annual, and climatological sediment-reference "
            "products with temporal coverage extending from {} to {}. The main basin-cluster "
            "time-series products include {} daily clusters, {} monthly clusters, and {} annual "
            "clusters. Daily observations span {}-{} and comprise {} valid cluster-time "
            "observations, with a median cluster record length of {} years and a maximum length "
            "of {} years. The daily product contains {} clusters longer than 50 years and {} "
            "clusters longer than 100 years, providing substantial support for long-term sediment "
            "analyses."
        ).format(
            all_first,
            all_last,
            fmt_int(daily.get("active_units", 0)),
            fmt_int(monthly.get("active_units", 0)),
            fmt_int(annual.get("active_units", 0)),
            as_int(daily.get("first_year")),
            as_int(daily.get("last_year")),
            fmt_int(daily.get("record_count_any", daily.get("records_any", 0))),
            fmt_float(daily.get("median_record_length_years")),
            fmt_float(daily.get("max_record_length_years")),
            fmt_int(daily.get("n_gt_50_years", 0)),
            fmt_int(daily.get("n_gt_100_years", 0)),
        ),
        "",
        (
            "Monthly records span {}-{} and include {} valid observations across {} clusters, "
            "with a median record length of {} years. Annual records span {}-{} and include {} "
            "valid observations across {} clusters. Because the annual time axis is not a regular "
            "annual grid, annual temporal coverage is reported using observed records and calendar "
            "span rather than a regular-grid coverage ratio. Peak active coverage occurs in 1980 "
            "for the daily product, 2013 for the monthly product, and 2018 for the annual product."
        ).format(
            as_int(monthly.get("first_year")),
            as_int(monthly.get("last_year")),
            fmt_int(monthly.get("record_count_any", monthly.get("records_any", 0))),
            fmt_int(monthly.get("active_units", 0)),
            fmt_float(monthly.get("median_record_length_years")),
            as_int(annual.get("first_year")),
            as_int(annual.get("last_year")),
            fmt_int(annual.get("record_count_any", annual.get("records_any", 0))),
            fmt_int(annual.get("active_units", 0)),
        ),
        "",
        (
            "The climatology product contains {} standalone climatology stations and spans "
            "{}-{}. It is reported separately from the basin-cluster time-series matrices because "
            "its statistical unit and intended use differ from the daily, monthly, and annual "
            "products."
        ).format(
            fmt_int(climatology.get("active_units", 0)),
            as_int(climatology.get("first_year")),
            as_int(climatology.get("last_year")),
        ),
        "",
    ]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write an ESSD temporal coverage results report.")
    parser.add_argument("--tables-dir", default=str(DEFAULT_TABLES_DIR), help="Directory containing temporal coverage CSV tables.")
    parser.add_argument("--figures-dir", default=str(DEFAULT_FIGURES_DIR), help="Directory containing temporal coverage figures.")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Output Markdown report path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out = write_report(Path(args.tables_dir), Path(args.figures_dir), Path(args.out))
    print("Wrote temporal coverage ESSD report: {}".format(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
