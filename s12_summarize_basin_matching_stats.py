#!/usr/bin/env python3
"""Summarize basin-status and basin-flag distributions from s7 cluster catalogs.

This script is intentionally simple to rerun during manual checking:
edit the file-level defaults below if the input/output paths ever change, then run

    python3 s12_summarize_basin_matching_stats.py

Outputs:
  - a human-readable TXT summary
  - several CSV tables for quick filtering or plotting
"""

from pathlib import Path

import pandas as pd

from basin_policy import NO_BASIN_MATCH_SOURCES, should_skip_basin_matching
from pipeline_paths import (
    S7_CLUSTER_RESOLUTION_CATALOG_CSV,
    S7_CLUSTER_STATION_CATALOG_CSV,
    get_output_r_root,
)


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

# User-editable defaults. Change these if you want to point the summary at a
# different export batch, then run the script directly without CLI arguments.
STATION_CATALOG_PATH = ROOT / S7_CLUSTER_STATION_CATALOG_CSV
RESOLUTION_CATALOG_PATH = ROOT / S7_CLUSTER_RESOLUTION_CATALOG_CSV
SUMMARY_TXT_PATH = ROOT / "scripts_basin_test/output/s12_basin_matching_summary.txt"
SUMMARY_CSV_PREFIX = ROOT / "scripts_basin_test/output/s12_basin_matching"


def _ensure_required_columns(df, required, label):
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(
            "{} is missing required columns: {}".format(label, ", ".join(missing))
        )


def _clean_text(series):
    return series.fillna("").astype(str).str.strip()


def _split_sources_used(value):
    text = "" if value is None else str(value).strip()
    if not text or text.lower() == "nan":
        return []
    return [part.strip() for part in text.split("|") if part.strip()]


def _mark_no_basin_match_sources(df):
    work = df.copy()
    if "sources_used" not in work.columns:
        work["has_no_basin_match_source"] = False
        work["no_basin_match_sources_used"] = ""
        return work

    parsed = work["sources_used"].map(_split_sources_used)
    work["no_basin_match_sources_used"] = parsed.map(
        lambda items: "|".join(sorted(src for src in items if should_skip_basin_matching(src)))
    )
    work["has_no_basin_match_source"] = work["no_basin_match_sources_used"].astype(str).str.strip() != ""
    return work


def _count_table(df, column, count_name):
    work = df.copy()
    work[column] = _clean_text(work[column]).replace("", "NA")
    counts = (
        work[column]
        .value_counts(dropna=False)
        .rename_axis(column)
        .reset_index(name=count_name)
    )
    total = int(counts[count_name].sum())
    counts["fraction"] = counts[count_name] / float(total) if total else 0.0
    counts["percent"] = counts["fraction"] * 100.0
    return counts


def _resolution_status_table(df):
    work = df.copy()
    work["resolution"] = _clean_text(work["resolution"]).replace("", "NA")
    work["basin_status"] = _clean_text(work["basin_status"]).replace("", "NA")
    table = work.groupby(["resolution", "basin_status"]).size().reset_index()
    table = table.rename(columns={0: "row_count"})
    table = table.sort_values(["resolution", "basin_status"], kind="stable")
    totals = table.groupby("resolution")["row_count"].transform("sum")
    table["fraction_within_resolution"] = table["row_count"] / totals
    table["percent_within_resolution"] = table["fraction_within_resolution"] * 100.0
    return table


def _resolution_flag_table(df):
    work = df.copy()
    work["resolution"] = _clean_text(work["resolution"]).replace("", "NA")
    work["basin_flag"] = _clean_text(work["basin_flag"]).replace("", "NA")
    table = work.groupby(["resolution", "basin_flag"]).size().reset_index()
    table = table.rename(columns={0: "row_count"})
    table = table.sort_values(["resolution", "basin_flag"], kind="stable")
    totals = table.groupby("resolution")["row_count"].transform("sum")
    table["fraction_within_resolution"] = table["row_count"] / totals
    table["percent_within_resolution"] = table["fraction_within_resolution"] * 100.0
    return table


def _format_count_lines(df, label_col, count_col):
    lines = []
    for row in df.itertuples(index=False):
        label = getattr(row, label_col)
        count = int(getattr(row, count_col))
        percent = float(getattr(row, "percent"))
        lines.append("  - {}: {} ({:.2f}%)".format(label, count, percent))
    return lines


def build_summary_tables(station_catalog, resolution_catalog):
    station_catalog = _mark_no_basin_match_sources(station_catalog)
    resolution_catalog = _mark_no_basin_match_sources(resolution_catalog)
    _ensure_required_columns(
        station_catalog,
        ["cluster_uid", "cluster_id", "basin_status", "basin_flag", "has_no_basin_match_source"],
        "station catalog",
    )
    _ensure_required_columns(
        resolution_catalog,
        ["cluster_uid", "cluster_id", "resolution", "basin_status", "basin_flag", "has_no_basin_match_source"],
        "resolution catalog",
    )

    station_status = _count_table(station_catalog, "basin_status", "cluster_count")
    station_flag = _count_table(station_catalog, "basin_flag", "cluster_count")
    unresolved_station_flag = _count_table(
        station_catalog[_clean_text(station_catalog["basin_status"]).str.lower() == "unresolved"],
        "basin_flag",
        "cluster_count",
    )

    resolution_status = _count_table(resolution_catalog, "basin_status", "row_count")
    resolution_flag = _count_table(resolution_catalog, "basin_flag", "row_count")
    unresolved_resolution_flag = _count_table(
        resolution_catalog[_clean_text(resolution_catalog["basin_status"]).str.lower() == "unresolved"],
        "basin_flag",
        "row_count",
    )

    resolution_by_status = _resolution_status_table(resolution_catalog)
    resolution_by_flag = _resolution_flag_table(resolution_catalog)
    no_basin_match_station_catalog = station_catalog[station_catalog["has_no_basin_match_source"]].copy()
    no_basin_match_resolution_catalog = resolution_catalog[resolution_catalog["has_no_basin_match_source"]].copy()
    no_basin_match_station_status = _count_table(no_basin_match_station_catalog, "basin_status", "cluster_count")
    no_basin_match_station_flag = _count_table(no_basin_match_station_catalog, "basin_flag", "cluster_count")
    no_basin_match_resolution_status = _count_table(no_basin_match_resolution_catalog, "basin_status", "row_count")
    no_basin_match_resolution_flag = _count_table(no_basin_match_resolution_catalog, "basin_flag", "row_count")

    return {
        "station_catalog": station_catalog,
        "resolution_catalog": resolution_catalog,
        "station_status": station_status,
        "station_flag": station_flag,
        "unresolved_station_flag": unresolved_station_flag,
        "resolution_status": resolution_status,
        "resolution_flag": resolution_flag,
        "unresolved_resolution_flag": unresolved_resolution_flag,
        "resolution_by_status": resolution_by_status,
        "resolution_by_flag": resolution_by_flag,
        "no_basin_match_station_catalog": no_basin_match_station_catalog,
        "no_basin_match_resolution_catalog": no_basin_match_resolution_catalog,
        "no_basin_match_station_status": no_basin_match_station_status,
        "no_basin_match_station_flag": no_basin_match_station_flag,
        "no_basin_match_resolution_status": no_basin_match_resolution_status,
        "no_basin_match_resolution_flag": no_basin_match_resolution_flag,
    }


def write_summary_text(out_path, station_catalog, resolution_catalog, tables):
    lines = []
    lines.append("Basin Matching Summary")
    lines.append("")
    lines.append("Inputs")
    lines.append("  - station catalog: {}".format(STATION_CATALOG_PATH))
    lines.append("  - resolution catalog: {}".format(RESOLUTION_CATALOG_PATH))
    lines.append("")
    lines.append("Station-Level Cluster Summary")
    lines.append("  - total clusters: {}".format(len(station_catalog)))
    lines.extend(_format_count_lines(tables["station_status"], "basin_status", "cluster_count"))
    lines.append("")
    lines.append("Station-Level Basin Flag Summary")
    lines.extend(_format_count_lines(tables["station_flag"], "basin_flag", "cluster_count"))
    lines.append("")
    lines.append("Unresolved Cluster Internal Breakdown")
    lines.extend(_format_count_lines(tables["unresolved_station_flag"], "basin_flag", "cluster_count"))
    lines.append("")
    lines.append("No-Basin-Match Source Cluster Summary ({})".format(", ".join(NO_BASIN_MATCH_SOURCES)))
    lines.append("  - policy: observations are kept, MERIT basin assignment is skipped")
    lines.append("  - expected labels: basin_status=unresolved, basin_flag=no_match")
    lines.append("  - total no-basin-match clusters: {}".format(len(tables["no_basin_match_station_catalog"])))
    lines.extend(_format_count_lines(tables["no_basin_match_station_status"], "basin_status", "cluster_count"))
    lines.append("")
    lines.append("No-Basin-Match Source Cluster Flags")
    lines.extend(_format_count_lines(tables["no_basin_match_station_flag"], "basin_flag", "cluster_count"))
    lines.append("")
    lines.append("Resolution-Level Summary")
    lines.append("  - total cluster-resolution rows: {}".format(len(resolution_catalog)))
    lines.extend(_format_count_lines(tables["resolution_status"], "basin_status", "row_count"))
    lines.append("")
    lines.append("Resolution-Level Basin Flag Summary")
    lines.extend(_format_count_lines(tables["resolution_flag"], "basin_flag", "row_count"))
    lines.append("")
    lines.append("Unresolved Resolution-Row Internal Breakdown")
    lines.extend(_format_count_lines(tables["unresolved_resolution_flag"], "basin_flag", "row_count"))
    lines.append("")
    lines.append("No-Basin-Match Source Resolution Summary ({})".format(", ".join(NO_BASIN_MATCH_SOURCES)))
    lines.append("  - policy: observations are kept, MERIT basin assignment is skipped")
    lines.append("  - expected labels: basin_status=unresolved, basin_flag=no_match")
    lines.append("  - total no-basin-match cluster-resolution rows: {}".format(len(tables["no_basin_match_resolution_catalog"])))
    lines.extend(_format_count_lines(tables["no_basin_match_resolution_status"], "basin_status", "row_count"))
    lines.append("")
    lines.append("No-Basin-Match Source Resolution Flags")
    lines.extend(_format_count_lines(tables["no_basin_match_resolution_flag"], "basin_flag", "row_count"))
    lines.append("")
    lines.append("Resolution x Basin Status")
    for row in tables["resolution_by_status"].itertuples(index=False):
        lines.append(
            "  - {} / {}: {} ({:.2f}% within {})".format(
                row.resolution,
                row.basin_status,
                int(row.row_count),
                float(row.percent_within_resolution),
                row.resolution,
            )
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_summary_csvs(prefix, tables):
    prefix.parent.mkdir(parents=True, exist_ok=True)
    outputs = {
        "station_status": prefix.with_name(prefix.name + "_station_status.csv"),
        "station_flag": prefix.with_name(prefix.name + "_station_flag.csv"),
        "station_unresolved_flag": prefix.with_name(prefix.name + "_station_unresolved_flag.csv"),
        "resolution_status": prefix.with_name(prefix.name + "_resolution_status.csv"),
        "resolution_flag": prefix.with_name(prefix.name + "_resolution_flag.csv"),
        "resolution_unresolved_flag": prefix.with_name(prefix.name + "_resolution_unresolved_flag.csv"),
        "resolution_by_status": prefix.with_name(prefix.name + "_resolution_by_status.csv"),
        "resolution_by_flag": prefix.with_name(prefix.name + "_resolution_by_flag.csv"),
        "no_basin_match_station_status": prefix.with_name(prefix.name + "_no_basin_match_station_status.csv"),
        "no_basin_match_station_flag": prefix.with_name(prefix.name + "_no_basin_match_station_flag.csv"),
        "no_basin_match_resolution_status": prefix.with_name(prefix.name + "_no_basin_match_resolution_status.csv"),
        "no_basin_match_resolution_flag": prefix.with_name(prefix.name + "_no_basin_match_resolution_flag.csv"),
    }
    tables["station_status"].to_csv(outputs["station_status"], index=False)
    tables["station_flag"].to_csv(outputs["station_flag"], index=False)
    tables["unresolved_station_flag"].to_csv(outputs["station_unresolved_flag"], index=False)
    tables["resolution_status"].to_csv(outputs["resolution_status"], index=False)
    tables["resolution_flag"].to_csv(outputs["resolution_flag"], index=False)
    tables["unresolved_resolution_flag"].to_csv(outputs["resolution_unresolved_flag"], index=False)
    tables["resolution_by_status"].to_csv(outputs["resolution_by_status"], index=False)
    tables["resolution_by_flag"].to_csv(outputs["resolution_by_flag"], index=False)
    tables["no_basin_match_station_status"].to_csv(outputs["no_basin_match_station_status"], index=False)
    tables["no_basin_match_station_flag"].to_csv(outputs["no_basin_match_station_flag"], index=False)
    tables["no_basin_match_resolution_status"].to_csv(outputs["no_basin_match_resolution_status"], index=False)
    tables["no_basin_match_resolution_flag"].to_csv(outputs["no_basin_match_resolution_flag"], index=False)
    return outputs


def main():
    if not STATION_CATALOG_PATH.is_file():
        raise FileNotFoundError("station catalog not found: {}".format(STATION_CATALOG_PATH))
    if not RESOLUTION_CATALOG_PATH.is_file():
        raise FileNotFoundError("resolution catalog not found: {}".format(RESOLUTION_CATALOG_PATH))

    station_catalog = pd.read_csv(STATION_CATALOG_PATH)
    resolution_catalog = pd.read_csv(RESOLUTION_CATALOG_PATH)

    tables = build_summary_tables(station_catalog, resolution_catalog)
    write_summary_text(SUMMARY_TXT_PATH, station_catalog, resolution_catalog, tables)
    csv_outputs = write_summary_csvs(SUMMARY_CSV_PREFIX, tables)

    print("Wrote basin matching summary text: {}".format(SUMMARY_TXT_PATH))
    for name, path in csv_outputs.items():
        print("Wrote {}: {}".format(name, path))
    print("Total clusters = {}".format(len(station_catalog)))
    print("Total cluster-resolution rows = {}".format(len(resolution_catalog)))


if __name__ == "__main__":
    main()
