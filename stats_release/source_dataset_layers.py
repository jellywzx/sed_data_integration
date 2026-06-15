#!/usr/bin/env python3
"""Release-only source dataset layer membership statistics."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import (
    add_common_args,
    context_from_args,
    copy_report_to_docs,
    numeric_series,
    split_pipe,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import PRODUCT_FILES
from stats_release.reporting import (
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


def _main_membership(station: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in station.iterrows():
        for source in split_pipe(row.get("sources_used", "")):
            rows.append(
                {
                    "source_name": source,
                    "layer": "main_station_catalog",
                    "resolution": row.get("resolution", ""),
                    "cluster_uid": row.get("cluster_uid", ""),
                    "row_count": 1,
                    "record_count": row.get("record_count", 0),
                }
            )
    return pd.DataFrame(rows)


def build_layer_stats(ctx) -> dict:
    source_dataset = ctx.read_csv(PRODUCT_FILES["source_dataset_catalog"], required=False)
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"], required=False)
    satellite = ctx.read_csv(PRODUCT_FILES["satellite_catalog"], required=False)

    frames = []
    if not station.empty:
        frames.append(_main_membership(station))
    if not source_station.empty:
        frames.append(
            pd.DataFrame(
                {
                    "source_name": source_station.get("source_name", ""),
                    "layer": "source_station_catalog",
                    "resolution": source_station.get("resolution", ""),
                    "cluster_uid": source_station.get("cluster_uid", ""),
                    "row_count": 1,
                    "record_count": numeric_series(source_station, "n_records").fillna(0),
                }
            )
        )
    if not satellite.empty:
        frames.append(
            pd.DataFrame(
                {
                    "source_name": satellite.get("source", ""),
                    "layer": "satellite_catalog",
                    "resolution": satellite.get("resolution", ""),
                    "cluster_uid": satellite.get("cluster_uid", ""),
                    "row_count": 1,
                    "record_count": numeric_series(satellite, "n_records").fillna(0),
                }
            )
        )
    for product_key, layer_name in (
        ("overlap_candidates_csv_gz", "overlap_candidates_sidecar"),
        ("overlap_candidates_parquet", "overlap_candidates_sidecar"),
        ("satellite_candidates_csv_gz", "satellite_candidates_sidecar"),
        ("satellite_candidates_parquet", "satellite_candidates_sidecar"),
        ("satellite_validation_catalog", "satellite_validation_catalog"),
    ):
        path = ctx.release_file(PRODUCT_FILES[product_key])
        if not path.is_file():
            continue
        frames.append(
            pd.DataFrame(
                [
                    {
                        "source_name": product_key,
                        "layer": layer_name,
                        "resolution": "",
                        "cluster_uid": "",
                        "row_count": 1,
                        "record_count": 0,
                    }
                ]
            )
        )
    membership = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not membership.empty:
        membership["record_count"] = pd.to_numeric(membership["record_count"], errors="coerce").fillna(0)
    summary = (
        membership.groupby(["source_name", "layer"], dropna=False)
        .agg(
            rows=("row_count", "sum"),
            clusters=("cluster_uid", "nunique"),
            records=("record_count", "sum"),
            resolutions=("resolution", lambda s: "|".join(sorted(set(str(v) for v in s if str(v).strip())))),
        )
        .reset_index()
        if not membership.empty
        else pd.DataFrame(columns=["source_name", "layer", "rows", "clusters", "records", "resolutions"])
    )
    source_rollup = (
        summary.groupby("source_name", dropna=False)
        .agg(
            layers=("layer", lambda s: "|".join(sorted(set(str(v) for v in s if str(v).strip())))),
            total_rows=("rows", "sum"),
            total_clusters=("clusters", "sum"),
            total_records=("records", "sum"),
        )
        .reset_index()
        if not summary.empty
        else pd.DataFrame(columns=["source_name", "layers", "total_rows", "total_clusters", "total_records"])
    )
    if not source_dataset.empty and "source_name" in source_dataset.columns:
        source_rollup = source_dataset[["source_name"]].drop_duplicates().merge(source_rollup, on="source_name", how="outer")
    unsupported = pd.DataFrame(
        [
            {
                "layer": name,
                "release_only_status": "unsupported_release_only",
                "reason": "requires pipeline intermediate file outside release package",
            }
            for name in (
                "mainline_s3_collected_stations",
                "mainline_s5_clustered_stations",
                "mainline_s6_quality_order_candidates",
                "mainline_s7_source_station_catalog",
            )
        ]
    )
    return {"membership": membership, "summary": summary, "source_rollup": source_rollup, "unsupported_pipeline_layers": unsupported}


def build_detailed_layer_report(ctx, stats: dict, tables_dir: Path) -> list[str]:
    membership = stats.get("membership", pd.DataFrame())
    summary = stats.get("summary", pd.DataFrame())
    source_rollup = stats.get("source_rollup", pd.DataFrame())
    unsupported = stats.get("unsupported_pipeline_layers", pd.DataFrame())
    unique_sources = membership["source_name"].nunique() if not membership.empty and "source_name" in membership.columns else 0
    total_rows = pd.to_numeric(membership.get("row_count", 0), errors="coerce").fillna(0).sum() if not membership.empty else 0
    total_records = pd.to_numeric(membership.get("record_count", 0), errors="coerce").fillna(0).sum() if not membership.empty else 0
    lines = [
        "# Source Dataset Layer Report",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- The report uses release catalogs and candidate sidecars only.",
        "",
        "## Headline",
        "",
        "- Release-visible source datasets: {}".format(fmt_int(unique_sources)),
        "- Release-visible membership rows: {}".format(fmt_int(total_rows)),
        "- Release-visible attributed records: {}".format(fmt_int(total_records)),
        "- Pipeline-only layers marked unsupported: {}".format(fmt_int(len(unsupported))),
        "",
        "## Release Layer Summary",
        "",
        sorted_markdown_table(
            summary,
            columns=["source_name", "layer", "rows", "clusters", "records", "resolutions"],
            max_rows=18,
        ),
    ]
    append_table_section(
        lines,
        "Source Rollup",
        source_rollup,
        columns=["source_name", "layers", "total_rows", "total_clusters", "total_records"],
        sort_by="total_records",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Membership Sample",
        membership,
        columns=["source_name", "layer", "resolution", "cluster_uid", "row_count", "record_count"],
        sort_by="record_count",
        max_rows=18,
        note="Membership rows are catalog-derived. Multiple source layers can refer to the same cluster, so totals are diagnostic rather than unique release totals.",
    )
    append_table_section(
        lines,
        "Unsupported Pipeline Layers",
        unsupported,
        columns=["layer", "release_only_status", "reason"],
        max_rows=10,
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- Release-only layers are suitable for published package QA and manuscript provenance summaries.",
            "- S3/S5/S6/S7 pipeline-layer counts are not inferred from release files because that would require non-release intermediate outputs.",
            "- Use `parity_manifest.csv` to see the same unsupported status in the legacy-output parity audit.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only source dataset layer statistics.")
    add_common_args(parser, "source_dataset_layers")
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    stats = build_layer_stats(ctx)
    for name, frame in stats.items():
        write_csv(frame, tables_dir / "table_source_layer_{}.csv".format(name))
    write_csv(stats["membership"], ctx.output_path("source_dataset_layer_membership.csv"))
    write_csv(stats["summary"], ctx.output_path("source_dataset_layer_summary.csv"))
    md_path = ctx.output_path("reports", "source_dataset_layers.md")
    report_lines = build_detailed_layer_report(ctx, stats, tables_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("source_dataset_layer_report.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote source dataset layer stats to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
