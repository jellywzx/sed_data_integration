#!/usr/bin/env python3
"""Release-level basin assignment diagnostics from station_catalog.csv."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import (
    add_common_args,
    clean_text,
    context_from_args,
    copy_report_to_docs,
    numeric_series,
    setup_matplotlib,
    split_pipe,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import pct, save_figure
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


def _spatial_error_class(row) -> str:
    status = clean_text(row.get("basin_status", "")).lower()
    flag = clean_text(row.get("basin_flag", "")).lower()
    distance = row.get("basin_distance_m", np.nan)
    try:
        distance = float(distance)
    except Exception:
        distance = np.nan
    if status == "unresolved":
        return flag or "unresolved"
    if status not in {"resolved", "matched"}:
        return "unknown_status"
    if flag in {"ok", "exact", "distance_only", ""} and (not np.isfinite(distance) or distance <= 1000):
        return "high_confidence"
    if np.isfinite(distance) and distance > 50000:
        return "large_offset"
    if "area" in flag:
        return "area_mismatch"
    if "geometry" in flag:
        return "geometry_inconsistent"
    return flag or "resolved_review"


def _match_quality(row) -> str:
    status = clean_text(row.get("basin_status", "")).lower()
    klass = clean_text(row.get("spatial_error_class", "")).lower()
    if status == "unresolved":
        return "excluded"
    if klass in {"high_confidence", "ok"}:
        return "high"
    if klass in {"large_offset", "area_mismatch", "geometry_inconsistent"}:
        return "manual_review"
    return "moderate"


def _summarize_counts(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=cols + ["rows", "clusters", "records", "percent_rows"])
    work = df.copy()
    for col in cols:
        if col not in work.columns:
            work[col] = ""
    grouped = (
        work.groupby(cols, dropna=False)
        .agg(rows=("cluster_uid", "size"), clusters=("cluster_uid", "nunique"), records=("record_count", "sum"))
        .reset_index()
        .sort_values(["rows", "records"], ascending=False)
    )
    grouped["percent_rows"] = grouped["rows"].map(lambda v: pct(v, len(work)))
    return grouped


def _prepare_spatial_match_table(station: pd.DataFrame) -> pd.DataFrame:
    work = station.copy()
    work["record_count"] = numeric_series(work, "record_count").fillna(0)
    work["basin_distance_m"] = numeric_series(work, "basin_distance_m")
    work["basin_area"] = numeric_series(work, "basin_area")
    work["has_reported_area"] = work["basin_area"].gt(0)
    work["spatial_error_class"] = work.apply(_spatial_error_class, axis=1)
    work["match_quality"] = work.apply(_match_quality, axis=1)
    work["distance_bin"] = pd.cut(
        work["basin_distance_m"],
        bins=[-1, 0, 100, 1000, 5000, 10000, 50000, float("inf")],
        labels=["0", "0-100", "100-1000", "1000-5000", "5000-10000", "10000-50000", ">50000"],
    ).astype(str)
    work["area_error_bin"] = np.where(work["basin_area"].gt(0), "reported_area_available", "no_reported_area")
    keep = [
        "cluster_uid",
        "cluster_id",
        "resolution",
        "record_count",
        "time_start",
        "time_end",
        "station_name",
        "river_name",
        "sources_used",
        "country",
        "continent_region",
        "iso_a3",
        "lat",
        "lon",
        "basin_area",
        "basin_status",
        "basin_flag",
        "basin_match_quality",
        "basin_distance_m",
        "point_in_local",
        "point_in_basin",
        "has_reported_area",
        "distance_bin",
        "area_error_bin",
        "spatial_error_class",
        "match_quality",
    ]
    return work[[col for col in keep if col in work.columns]].copy()


def build_basin_diagnostics(ctx) -> dict:
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"])
    spatial_table = _prepare_spatial_match_table(station)
    station["record_count"] = numeric_series(station, "record_count").fillna(0)
    station["basin_distance_m"] = numeric_series(station, "basin_distance_m")
    station["has_basin_area"] = numeric_series(station, "basin_area").gt(0)
    station["is_unresolved"] = station["basin_status"].astype(str).str.strip().eq("unresolved")
    station["distance_bin"] = pd.cut(
        station["basin_distance_m"],
        bins=[-1, 0, 100, 1000, 5000, 10000, 50000, float("inf")],
        labels=["0", "0-100", "100-1000", "1000-5000", "5000-10000", "10000-50000", ">50000"],
    ).astype(str)
    status_counts = (
        station.groupby(["basin_status", "basin_flag"], dropna=False)
        .agg(rows=("cluster_uid", "size"), clusters=("cluster_uid", "nunique"), records=("record_count", "sum"))
        .reset_index()
        .sort_values(["clusters", "records"], ascending=False)
    )
    by_resolution = (
        station.groupby(["resolution", "basin_status"], dropna=False)
        .agg(rows=("cluster_uid", "size"), clusters=("cluster_uid", "nunique"), records=("record_count", "sum"))
        .reset_index()
    )
    by_distance = (
        station.groupby(["distance_bin", "basin_status"], dropna=False)
        .agg(rows=("cluster_uid", "size"), clusters=("cluster_uid", "nunique"))
        .reset_index()
    )
    by_area_presence = (
        station.groupby(["has_basin_area", "basin_status"], dropna=False)
        .agg(rows=("cluster_uid", "size"), clusters=("cluster_uid", "nunique"))
        .reset_index()
    )
    point_flags = (
        station.groupby(["point_in_local", "point_in_basin", "basin_status"], dropna=False)
        .agg(rows=("cluster_uid", "size"), clusters=("cluster_uid", "nunique"))
        .reset_index()
        if {"point_in_local", "point_in_basin"}.issubset(station.columns)
        else pd.DataFrame()
    )
    source_rows = []
    if "sources_used" in station.columns:
        for _, row in station.iterrows():
            for source in split_pipe(row.get("sources_used", "")):
                source_rows.append(
                    {
                        "source_name": source,
                        "cluster_uid": row.get("cluster_uid", ""),
                        "record_count": row.get("record_count", 0),
                        "is_unresolved": bool(row.get("is_unresolved", False)),
                    }
                )
    source_summary = pd.DataFrame(source_rows)
    if not source_summary.empty:
        source_summary["record_count"] = pd.to_numeric(source_summary["record_count"], errors="coerce").fillna(0)
        source_summary = (
            source_summary.groupby("source_name", dropna=False)
            .agg(
                rows=("cluster_uid", "size"),
                unresolved_rows=("is_unresolved", "sum"),
                records=("record_count", "sum"),
                unresolved_records=("record_count", lambda s: s[source_summary.loc[s.index, "is_unresolved"]].sum()),
            )
            .reset_index()
        )
        source_summary["unresolved_row_percent"] = source_summary.apply(
            lambda row: round(100.0 * row["unresolved_rows"] / row["rows"], 6) if row["rows"] else 0.0,
            axis=1,
        )
        source_summary["unresolved_record_percent"] = source_summary.apply(
            lambda row: round(100.0 * row["unresolved_records"] / row["records"], 6) if row["records"] else 0.0,
            axis=1,
        )
        source_summary = source_summary.sort_values(["unresolved_records", "unresolved_row_percent"], ascending=False)
    country_key = ["iso_a3", "country"] if {"iso_a3", "country"}.issubset(station.columns) else ["country"]
    country_summary = (
        station.groupby(country_key, dropna=False)
        .agg(
            rows=("cluster_uid", "size"),
            unresolved_rows=("is_unresolved", "sum"),
            records=("record_count", "sum"),
            unresolved_records=("record_count", lambda s: s[station.loc[s.index, "is_unresolved"]].sum()),
        )
        .reset_index()
        if "country" in station.columns
        else pd.DataFrame()
    )
    if not country_summary.empty:
        country_summary["unresolved_record_percent"] = country_summary.apply(
            lambda row: round(100.0 * row["unresolved_records"] / row["records"], 6) if row["records"] else 0.0,
            axis=1,
        )
        country_summary = country_summary.sort_values("unresolved_records", ascending=False)
    resolved_point_anomalies = pd.DataFrame()
    if {"point_in_local", "point_in_basin"}.issubset(station.columns):
        resolved_point_anomalies = station[
            station["basin_status"].astype(str).str.strip().eq("resolved")
            & (~(station["point_in_local"].astype(int).eq(1) & station["point_in_basin"].astype(int).eq(1)))
        ].copy()
        keep_cols = [
            col
            for col in (
                "cluster_uid",
                "resolution",
                "record_count",
                "sources_used",
                "country",
                "iso_a3",
                "river_name",
                "lat",
                "lon",
                "basin_match_quality",
                "basin_distance_m",
                "point_in_local",
                "point_in_basin",
            )
            if col in resolved_point_anomalies.columns
        ]
        resolved_point_anomalies = resolved_point_anomalies.sort_values("record_count", ascending=False)[keep_cols]
    reported_area = spatial_table[spatial_table["has_reported_area"].astype(bool)].copy() if "has_reported_area" in spatial_table.columns else pd.DataFrame()
    threshold_rows = []
    distance = pd.to_numeric(spatial_table.get("basin_distance_m", pd.Series([], dtype=float)), errors="coerce")
    for threshold in [0, 100, 1000, 5000, 10000, 50000]:
        accepted = spatial_table[spatial_table["basin_status"].astype(str).str.lower().eq("resolved") & (distance.fillna(0) <= threshold)]
        threshold_rows.append(
            {
                "distance_threshold_m": threshold,
                "accepted_rows": int(len(accepted)),
                "accepted_clusters": int(accepted["cluster_uid"].nunique()) if "cluster_uid" in accepted.columns else 0,
                "accepted_percent_rows": pct(len(accepted), len(spatial_table)),
            }
        )
    threshold_table = pd.DataFrame(threshold_rows)
    queue_cols = [
        col
        for col in (
            "cluster_uid",
            "resolution",
            "record_count",
            "sources_used",
            "country",
            "river_name",
            "lat",
            "lon",
            "basin_status",
            "basin_flag",
            "basin_distance_m",
            "basin_area",
            "point_in_local",
            "point_in_basin",
            "spatial_error_class",
            "match_quality",
        )
        if col in spatial_table.columns
    ]
    remote_summary = pd.DataFrame(
        [
            {
                "subset": "release_station_catalog",
                "rows": int(len(spatial_table)),
                "remote_sensing_rows_excluded": 0,
                "note": "Release station_catalog excludes satellite validation-only records; see satellite_catalog.csv for validation products.",
            }
        ]
    )
    unknown = spatial_table[
        spatial_table["basin_status"].astype(str).str.strip().isin(["", "unknown"])
        | spatial_table["lat"].isna()
        | spatial_table["lon"].isna()
    ].copy()
    return {
        "status_counts": status_counts,
        "status_by_resolution": by_resolution,
        "status_by_distance": by_distance,
        "status_by_area_presence": by_area_presence,
        "point_flag_counts": point_flags,
        "unresolved_by_source": source_summary,
        "unresolved_by_country": country_summary,
        "resolved_point_anomalies": resolved_point_anomalies,
        "spatial_match_error_table": spatial_table,
        "spatial_match_status_counts": _summarize_counts(spatial_table, ["basin_status"]),
        "spatial_match_flag_counts": _summarize_counts(spatial_table, ["basin_flag"]),
        "spatial_match_quality_counts": _summarize_counts(spatial_table, ["match_quality"]),
        "spatial_match_error_class_counts": _summarize_counts(spatial_table, ["spatial_error_class"]),
        "spatial_match_distance_bins": _summarize_counts(spatial_table, ["distance_bin", "basin_status"]),
        "spatial_match_area_error_bins": _summarize_counts(spatial_table, ["area_error_bin", "match_quality"]),
        "spatial_match_status_by_reported_area_presence": _summarize_counts(spatial_table, ["has_reported_area", "basin_status"]),
        "spatial_match_status_by_resolution": _summarize_counts(spatial_table, ["resolution", "basin_status"]),
        "spatial_match_flag_by_resolution": _summarize_counts(spatial_table, ["resolution", "basin_flag"]),
        "spatial_match_status_by_source": source_summary.rename(columns={"source_name": "source", "unresolved_rows": "unresolved_rows_release"}),
        "spatial_match_flag_by_source": source_summary.rename(columns={"source_name": "source", "unresolved_rows": "unresolved_rows_release"}),
        "spatial_match_threshold_sensitivity": threshold_table,
        "reported_area_spatial_match_rows": reported_area,
        "reported_area_match_status_counts": _summarize_counts(reported_area, ["basin_status"]),
        "reported_area_match_flag_counts": _summarize_counts(reported_area, ["basin_flag"]),
        "reported_area_match_quality_counts": _summarize_counts(reported_area, ["match_quality"]),
        "reported_area_spatial_error_class_counts": _summarize_counts(reported_area, ["spatial_error_class"]),
        "reported_area_match_status_quality_counts": _summarize_counts(reported_area, ["basin_status", "match_quality"]),
        "reported_area_area_error_bin_quality_counts": _summarize_counts(reported_area, ["area_error_bin", "match_quality"]),
        "manual_review_top_large_offsets": spatial_table.sort_values("basin_distance_m", ascending=False).head(200)[queue_cols],
        "manual_review_area_mismatch": spatial_table[spatial_table["spatial_error_class"].astype(str).str.contains("area", case=False, na=False)].head(200)[queue_cols],
        "manual_review_geometry_inconsistent": spatial_table[spatial_table["spatial_error_class"].astype(str).str.contains("geometry", case=False, na=False)].head(200)[queue_cols],
        "manual_review_high_risk": spatial_table[spatial_table["match_quality"].astype(str).eq("manual_review") | spatial_table["basin_status"].astype(str).eq("unresolved")].head(200)[queue_cols],
        "remote_sensing_exclusion_summary": remote_summary,
        "unknown_stations": unknown,
    }


def write_legacy_text_outputs(ctx, tables: dict) -> None:
    summary = tables.get("spatial_match_status_counts", pd.DataFrame())
    total = int(summary["rows"].sum()) if not summary.empty and "rows" in summary.columns else 0
    unresolved = 0
    if not summary.empty and "basin_status" in summary.columns:
        unresolved = int(summary.loc[summary["basin_status"].astype(str).eq("unresolved"), "rows"].sum())
    lines = [
        "Release spatial matching error summary",
        "======================================",
        "",
        "Input: station_catalog.csv",
        "Rows: {:,}".format(total),
        "Unresolved rows: {:,} ({:.2f}%)".format(unresolved, pct(unresolved, total)),
    ]
    for name in ("spatial_match_error_summary.txt",):
        ctx.output_path(name).write_text("\n".join(lines) + "\n", encoding="utf-8")
    md = ["# Spatial Match Error Summary", "", "- Rows: {:,}".format(total), "- Unresolved rows: {:,} ({:.2f}%)".format(unresolved, pct(unresolved, total))]
    for name in ("spatial_match_error_summary_essd.md", "spatial_match_error_detailed_report.md"):
        write_markdown(md, ctx.output_path(name))
    remote = tables.get("remote_sensing_exclusion_summary", pd.DataFrame())
    note = remote.to_string(index=False) if not remote.empty else "No remote-sensing release rows were excluded."
    ctx.output_path("remote_sensing_exclusion_summary.txt").write_text(note + "\n", encoding="utf-8")


def write_figures(tables: dict, figures_dir: Path, dpi: int) -> None:
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)

    def bar_from_table(table_name: str, x_col: str, y_col: str, title: str, filename: str) -> None:
        df = tables.get(table_name, pd.DataFrame())
        if df.empty or x_col not in df.columns or y_col not in df.columns:
            return
        plot = df.head(20).copy()
        fig, ax = plt.subplots(figsize=(8, max(3.5, 0.28 * len(plot) + 1.5)))
        ax.barh(plot[x_col].astype(str), pd.to_numeric(plot[y_col], errors="coerce").fillna(0), color="#4c78a8")
        ax.set_xlabel(y_col.replace("_", " ").title())
        ax.set_title(title)
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / filename, dpi=dpi, also_pdf=False)
        plt.close(fig)

    bar_from_table("spatial_match_flag_counts", "basin_flag", "rows", "Basin flag counts", "basin_flag_counts.png")
    bar_from_table("spatial_match_error_class_counts", "spatial_error_class", "rows", "Spatial error classes", "spatial_error_class_counts.png")
    bar_from_table("spatial_match_status_by_reported_area_presence", "basin_status", "rows", "Status by reported-area presence", "basin_status_by_reported_area_presence.png")
    ra = tables.get("spatial_match_error_table", pd.DataFrame())
    if not ra.empty:
        distance = pd.to_numeric(ra.get("basin_distance_m"), errors="coerce")
        fig, ax = plt.subplots(figsize=(7.5, 4.0))
        vals = distance[np.isfinite(distance) & (distance >= 0)]
        if len(vals):
            ax.hist(np.log10(vals + 1), bins=50, color="#f58518")
        ax.set_xlabel("log10(distance_m + 1)")
        ax.set_ylabel("Rows")
        ax.set_title("Basin match distance")
        ax.grid(alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "distance_hist_logx.png", dpi=dpi, also_pdf=False)
        plt.close(fig)
        if {"lon", "lat"}.issubset(ra.columns):
            unresolved = ra[ra["basin_status"].astype(str).eq("unresolved")].copy()
            fig, ax = plt.subplots(figsize=(9, 4.5))
            ax.scatter(pd.to_numeric(unresolved.get("lon"), errors="coerce"), pd.to_numeric(unresolved.get("lat"), errors="coerce"), s=8, alpha=0.5)
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            ax.set_title("Unresolved/unknown basin points")
            ax.grid(alpha=0.3)
            fig.tight_layout()
            save_figure(fig, figures_dir / "unknown_points_map.png", dpi=dpi, also_pdf=False)
            plt.close(fig)
    th = tables.get("spatial_match_threshold_sensitivity", pd.DataFrame())
    if not th.empty:
        fig, ax = plt.subplots(figsize=(7.5, 4.0))
        ax.plot(th["distance_threshold_m"], th["accepted_percent_rows"], marker="o")
        ax.set_xlabel("Distance threshold (m)")
        ax.set_ylabel("Accepted rows (%)")
        ax.set_title("Threshold sensitivity")
        ax.grid(alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "threshold_sensitivity.png", dpi=dpi, also_pdf=False)
        plt.close(fig)
    presence = tables.get("spatial_match_status_by_reported_area_presence", pd.DataFrame())
    if not presence.empty and "has_reported_area" in presence.columns:
        fig, ax = plt.subplots(figsize=(6.5, 3.8))
        presence.groupby("has_reported_area")["rows"].sum().plot(kind="bar", ax=ax, color="#54a24b")
        ax.set_ylabel("Rows")
        ax.set_title("Reported-area presence")
        fig.tight_layout()
        save_figure(fig, figures_dir / "reported_area_presence_counts.png", dpi=dpi, also_pdf=False)
        plt.close(fig)


def build_detailed_basin_report(ctx, tables: dict, tables_dir: Path, figures_dir: Path, report_dir: Path) -> list[str]:
    status = tables.get("spatial_match_status_counts", pd.DataFrame())
    flag = tables.get("spatial_match_flag_counts", pd.DataFrame())
    quality = tables.get("spatial_match_quality_counts", pd.DataFrame())
    by_source = tables.get("unresolved_by_source", pd.DataFrame())
    by_country = tables.get("unresolved_by_country", pd.DataFrame())
    anomalies = tables.get("resolved_point_anomalies", pd.DataFrame())
    threshold = tables.get("spatial_match_threshold_sensitivity", pd.DataFrame())
    reported_status = tables.get("reported_area_match_status_counts", pd.DataFrame())
    reported_quality = tables.get("reported_area_match_quality_counts", pd.DataFrame())
    distance_bins = tables.get("spatial_match_distance_bins", pd.DataFrame())
    area_bins = tables.get("spatial_match_area_error_bins", pd.DataFrame())
    review_offsets = tables.get("manual_review_top_large_offsets", pd.DataFrame())
    review_area = tables.get("manual_review_area_mismatch", pd.DataFrame())
    review_geometry = tables.get("manual_review_geometry_inconsistent", pd.DataFrame())
    review_high_risk = tables.get("manual_review_high_risk", pd.DataFrame())
    remote = tables.get("remote_sensing_exclusion_summary", pd.DataFrame())

    total = pd.to_numeric(status.get("rows", 0), errors="coerce").fillna(0).sum() if not status.empty else 0
    unresolved = 0
    if not status.empty and {"basin_status", "rows"}.issubset(status.columns):
        unresolved = pd.to_numeric(status.loc[status["basin_status"].astype(str).eq("unresolved"), "rows"], errors="coerce").fillna(0).sum()
    unresolved_records = 0
    if not status.empty and {"basin_status", "records"}.issubset(status.columns):
        unresolved_records = pd.to_numeric(status.loc[status["basin_status"].astype(str).eq("unresolved"), "records"], errors="coerce").fillna(0).sum()

    lines = [
        "# Spatial Match Error Detailed Report",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Diagnostics are computed from `station_catalog.csv` and release-side basin fields only.",
        "",
        "## Headline",
        "",
        "- Station catalog rows: {}".format(fmt_int(total)),
        "- Unresolved rows: {} ({:.2f}%)".format(fmt_int(unresolved), pct(unresolved, total)),
        "- Records affected by unresolved rows: {}".format(fmt_int(unresolved_records)),
        "- Resolved stations with point flags requiring review: {}".format(fmt_int(len(anomalies))),
        "- High-risk manual review rows emitted: {}".format(fmt_int(len(review_high_risk))),
        "",
        "## Status Summary",
        "",
        sorted_markdown_table(status, columns=["basin_status", "rows", "clusters", "records", "percent_rows"], max_rows=10),
    ]
    append_table_section(
        lines,
        "Flag and Match-Quality Summary",
        flag,
        columns=["basin_flag", "rows", "clusters", "records", "percent_rows"],
        sort_by="rows",
        max_rows=12,
    )
    append_table_section(
        lines,
        "Match Quality",
        quality,
        columns=["match_quality", "rows", "clusters", "records", "percent_rows"],
        sort_by="rows",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Unresolved Priority by Source",
        by_source,
        columns=["source_name", "rows", "unresolved_rows", "records", "unresolved_records", "unresolved_row_percent", "unresolved_record_percent"],
        sort_by="unresolved_records",
        max_rows=15,
        note="Prioritize sources with both high unresolved rows and high affected record counts.",
    )
    append_table_section(
        lines,
        "Unresolved Priority by Country",
        by_country,
        columns=["country", "iso_a3", "rows", "unresolved_rows", "records", "unresolved_records", "unresolved_row_percent", "unresolved_record_percent"],
        sort_by="unresolved_records",
        max_rows=15,
    )
    append_table_section(
        lines,
        "Resolved Point-Flag Anomalies",
        anomalies,
        columns=["cluster_uid", "resolution", "record_count", "sources_used", "country", "iso_a3", "river_name", "basin_match_quality", "basin_distance_m", "point_in_local", "point_in_basin"],
        sort_by="record_count",
        max_rows=16,
        note="These rows are resolved but have local/basin point flags that are not fully passing.",
    )
    append_table_section(
        lines,
        "Distance Threshold Sensitivity",
        threshold,
        columns=["distance_threshold_m", "accepted_rows", "accepted_clusters", "accepted_percent_rows"],
        max_rows=12,
    )
    append_table_section(
        lines,
        "Distance Bins",
        distance_bins,
        max_rows=12,
    )
    append_table_section(
        lines,
        "Reported-Area Status",
        reported_status,
        columns=["basin_status", "rows", "clusters", "records", "percent_rows"],
        sort_by="rows",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Reported-Area Quality",
        reported_quality,
        columns=["match_quality", "rows", "clusters", "records", "percent_rows"],
        sort_by="rows",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Area Error Bins",
        area_bins,
        max_rows=12,
    )
    append_table_section(
        lines,
        "Manual Review Queue: Large Offsets",
        review_offsets,
        max_rows=12,
    )
    append_table_section(
        lines,
        "Manual Review Queue: Area Mismatch",
        review_area,
        max_rows=12,
    )
    append_table_section(
        lines,
        "Manual Review Queue: Geometry Inconsistent",
        review_geometry,
        max_rows=12,
    )
    append_table_section(
        lines,
        "Remote-Sensing Exclusion Summary",
        remote,
        max_rows=10,
    )
    lines.extend(
        [
            "",
            "## Recommended Follow-Up",
            "",
            "- Do not auto-resolve unresolved rows solely from this report; repair high-impact sources first and preserve status/quality fields.",
            "- Review `large_offset`, `area_mismatch`, and geometry-inconsistent queues before publishing basin-sensitive analyses.",
            "- Treat resolved point-flag anomalies as lower-confidence or manually reviewed basin assignments.",
        ]
    )
    append_figure_index(lines, figures_dir, report_dir)
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only basin assignment diagnostics.")
    add_common_args(parser, "basin_diagnostics")
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    stats = build_basin_diagnostics(ctx)
    for name, frame in stats.items():
        write_csv(frame, tables_dir / "table_basin_{}.csv".format(name))
        legacy_names = {
            "spatial_match_error_table",
            "spatial_match_status_counts",
            "spatial_match_flag_counts",
            "spatial_match_quality_counts",
            "spatial_match_error_class_counts",
            "spatial_match_distance_bins",
            "spatial_match_area_error_bins",
            "spatial_match_status_by_reported_area_presence",
            "spatial_match_status_by_resolution",
            "spatial_match_flag_by_resolution",
            "spatial_match_status_by_source",
            "spatial_match_flag_by_source",
            "spatial_match_threshold_sensitivity",
            "reported_area_spatial_match_rows",
            "reported_area_match_status_counts",
            "reported_area_match_flag_counts",
            "reported_area_match_quality_counts",
            "reported_area_spatial_error_class_counts",
            "reported_area_match_status_quality_counts",
            "reported_area_area_error_bin_quality_counts",
            "manual_review_top_large_offsets",
            "manual_review_area_mismatch",
            "manual_review_geometry_inconsistent",
            "manual_review_high_risk",
            "remote_sensing_exclusion_summary",
            "unknown_stations",
        }
        if name in legacy_names:
            write_csv(frame, ctx.output_path("{}.csv".format(name)))
    if not args.skip_figures:
        try:
            write_figures(stats, ctx.figures_dir(), max(72, int(args.dpi)))
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    write_legacy_text_outputs(ctx, stats)
    md_path = ctx.output_path("reports", "basin_diagnostics.md")
    report_lines = build_detailed_basin_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("spatial_match_error_summary_essd.md"))
    write_markdown(report_lines, ctx.output_path("spatial_match_error_detailed_report.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote basin diagnostics to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
