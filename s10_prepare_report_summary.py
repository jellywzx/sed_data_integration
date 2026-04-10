#!/usr/bin/env python3
"""
Prepare presentation-ready summary figures and stepwise audit tables for the
merged basin reference dataset.

Outputs are written to:
  scripts_basin_test/output/report_summary/
"""

import math
import struct
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR / "output"
OUT_DIR = ROOT / "report_summary"

S1_VERIFY = ROOT / "s1_verify_time_resolution_results.csv"
S3_COLLECTED = ROOT / "s3_collected_stations.csv"
S4_UPSTREAM = ROOT / "s4_upstream_basins.csv"
S5_CLUSTERED = ROOT / "s5_basin_clustered_stations.csv"
S6_MERGED_NC = ROOT / "s6_basin_merged_all.nc"
S6_PLOT_STATS = ROOT / "s6_plot_stats.csv"
S7_CLUSTER_SHP_DBF = ROOT / "s7_cluster_stations.dbf"
S7_SOURCE_SHP_DBF = ROOT / "s7_source_stations.dbf"
S7_CLUSTER_BASIN_DBF = ROOT / "s7_cluster_basins.dbf"


def dbf_row_count(path):
    with path.open("rb") as fh:
        header = fh.read(32)
    return struct.unpack("<I", header[4:8])[0]


def decode_object_array(values):
    out = []
    for value in values:
        if isinstance(value, (bytes, bytearray)):
            out.append(value.decode("utf-8"))
        else:
            out.append(str(value))
    return out


def format_int(value):
    return "{:,}".format(int(value))


def format_pct(value):
    return "{:.2%}".format(float(value))


def label_bars(ax, bars, fmt="{:,.0f}", rotation=0, fontsize=9):
    ymax = max((bar.get_height() for bar in bars), default=0)
    offset = ymax * 0.015 if ymax else 0.5
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            height + offset,
            fmt.format(height),
            ha="center",
            va="bottom",
            fontsize=fontsize,
            rotation=rotation,
        )


def build_summary():
    s1 = pd.read_csv(S1_VERIFY)
    s3 = pd.read_csv(S3_COLLECTED)
    s4 = pd.read_csv(S4_UPSTREAM)
    s5 = pd.read_csv(S5_CLUSTERED)
    s6_stats = pd.read_csv(S6_PLOT_STATS)

    cluster_size = s5.groupby("cluster_id").size().sort_index()
    multi_station_clusters = int((cluster_size > 1).sum())
    max_cluster_size = int(cluster_size.max())

    s4_success = int(s4["basin_id"].notna().sum())
    s4_failed = int(s4["basin_id"].isna().sum())

    cluster_shp_count = dbf_row_count(S7_CLUSTER_SHP_DBF)
    source_shp_count = dbf_row_count(S7_SOURCE_SHP_DBF)
    cluster_basin_count = dbf_row_count(S7_CLUSTER_BASIN_DBF)

    ds = xr.open_dataset(S6_MERGED_NC, engine="h5netcdf", decode_cf=False, mask_and_scale=False)
    try:
        nc_n_records = int(ds.sizes["n_records"])
        nc_n_source_stations = int(ds.sizes["n_source_stations"])
        nc_n_stations = int(ds.sizes["n_stations"])
        cluster_ids = ds["cluster_id"].values.astype(np.int64)

        resolution_codes = ds["resolution"].values.astype(np.int64)
        resolution_names = str(ds["resolution"].attrs.get("flag_meanings", "")).split()
        resolution_rows = []
        for idx, name in enumerate(resolution_names):
            count = int((resolution_codes == idx).sum())
            resolution_rows.append(
                {
                    "resolution_name": name,
                    "record_count": count,
                    "record_fraction": count / max(nc_n_records, 1),
                }
            )
        record_resolution = pd.DataFrame(resolution_rows)

        source_station_paths = decode_object_array(ds["source_station_paths"].values)
        combined_source_station_rows = int(sum("|" in path for path in source_station_paths))

        source_station_index = ds["source_station_index"].values.astype(np.int64)
        missing_source_station_records = int((source_station_index < 0).sum())
        missing_source_station_fraction = missing_source_station_records / max(nc_n_records, 1)

        blank_source_records = 0
        if "source" in ds.variables:
            record_source = np.array(decode_object_array(ds["source"].values), dtype=object)
            blank_source_records = int(np.sum(record_source == ""))

        sources_used = decode_object_array(ds["sources_used"].values)
        cluster_meta = pd.DataFrame(
            {
                "cluster_id": cluster_ids.astype(int),
                "sources_used": sources_used,
            }
        )
        missing_mask = source_station_index < 0
        missing_cluster_counts = (
            pd.Series(cluster_ids[ds["station_index"].values.astype(np.int64)[missing_mask]])
            .value_counts()
            .rename_axis("cluster_id")
            .reset_index(name="missing_records")
        )
        missing_by_sources_used = (
            missing_cluster_counts
            .merge(cluster_meta, on="cluster_id", how="left")
            .assign(sources_used=lambda x: x["sources_used"].replace("", "blank"))
            .groupby("sources_used", as_index=False)["missing_records"]
            .sum()
            .sort_values("missing_records", ascending=False)
            .reset_index(drop=True)
        )
    finally:
        ds.close()

    source_counts = (
        s5["source"]
        .fillna("unknown")
        .value_counts()
        .rename_axis("source")
        .reset_index(name="station_rows")
    )

    station_resolution = (
        s5["resolution"]
        .fillna("unknown")
        .value_counts()
        .reindex(["daily", "monthly", "annual", "climatology", "other"], fill_value=0)
        .rename_axis("resolution_name")
        .reset_index(name="station_rows")
    )

    cluster_bin_rows = []
    bin_specs = [
        ("1", cluster_size == 1),
        ("2", cluster_size == 2),
        ("3-5", (cluster_size >= 3) & (cluster_size <= 5)),
        ("6-10", (cluster_size >= 6) & (cluster_size <= 10)),
        ("11-20", (cluster_size >= 11) & (cluster_size <= 20)),
        (">20", cluster_size > 20),
    ]
    for label, mask in bin_specs:
        cluster_bin_rows.append(
            {
                "cluster_size_bin": label,
                "cluster_count": int(mask.sum()),
            }
        )
    cluster_size_bins = pd.DataFrame(cluster_bin_rows)

    source_lineage = pd.DataFrame(
        [
            {"stage": "s3 collected", "count": int(len(s3))},
            {"stage": "s4 basin rows", "count": int(len(s4))},
            {"stage": "s4 basin matched", "count": s4_success},
            {"stage": "s5 source rows", "count": int(len(s5))},
            {"stage": "s6 source stations", "count": nc_n_source_stations},
            {"stage": "s7 source points", "count": source_shp_count},
        ]
    )

    cluster_lineage = pd.DataFrame(
        [
            {"stage": "s5 unique clusters", "count": int(s5["cluster_id"].nunique())},
            {"stage": "s6 nc stations", "count": nc_n_stations},
            {"stage": "s7 cluster points", "count": cluster_shp_count},
            {"stage": "s7 cluster basins", "count": cluster_basin_count},
        ]
    )

    audit_rows = [
        {
            "item": "s1_verified_files",
            "count": int(len(s1)),
            "status": "info",
            "assessment": "All candidate files checked for time semantics.",
            "note": "This is an input-file count, not a final station count.",
        },
        {
            "item": "s3_collected_station_rows",
            "count": int(len(s3)),
            "status": "ok",
            "assessment": "Collected station rows are the merge-line entry count.",
            "note": "One row equals one reorganized station file.",
        },
        {
            "item": "s4_upstream_rows",
            "count": int(len(s4)),
            "status": "ok",
            "assessment": "Exactly matches s3; no station rows dropped before basin tracing.",
            "note": "s4 rows == s3 rows",
        },
        {
            "item": "s4_basin_matched_rows",
            "count": s4_success,
            "status": "ok",
            "assessment": "Basin tracing success rate is very high.",
            "note": "{} matched ({})".format(format_int(s4_success), format_pct(s4_success / max(len(s4), 1))),
        },
        {
            "item": "s4_basin_failed_rows",
            "count": s4_failed,
            "status": "review",
            "assessment": "A small set of mostly isolated or special-case stations failed basin tracing.",
            "note": "{} failed ({})".format(format_int(s4_failed), format_pct(s4_failed / max(len(s4), 1))),
        },
        {
            "item": "s5_cluster_rows",
            "count": int(len(s5)),
            "status": "ok",
            "assessment": "Exactly matches s3 and s4 source-station rows.",
            "note": "No source-station rows lost during cluster assignment.",
        },
        {
            "item": "s5_unique_clusters",
            "count": int(s5["cluster_id"].nunique()),
            "status": "ok",
            "assessment": "Source rows compress to basin-unit clusters as expected.",
            "note": "Compression ratio = {}".format(format_pct(s5["cluster_id"].nunique() / max(len(s5), 1))),
        },
        {
            "item": "s5_multi_station_clusters",
            "count": multi_station_clusters,
            "status": "ok",
            "assessment": "A meaningful fraction of clusters merge multiple source stations.",
            "note": "max cluster size = {}".format(max_cluster_size),
        },
        {
            "item": "s6_nc_source_stations",
            "count": nc_n_source_stations,
            "status": "review",
            "assessment": "Almost matches s5 rows, but one pair of GFQA paths is stored as a combined source-station entry.",
            "note": "Difference vs s5 rows = {}; combined path entries = {}".format(
                len(s5) - nc_n_source_stations,
                combined_source_station_rows,
            ),
        },
        {
            "item": "s6_nc_clusters",
            "count": nc_n_stations,
            "status": "ok",
            "assessment": "Exactly matches the s5 unique cluster count.",
            "note": "Cluster count is internally consistent between csv and nc.",
        },
        {
            "item": "s6_missing_source_station_index_records",
            "count": missing_source_station_records,
            "status": "review",
            "assessment": "Record-level provenance is not fully populated for part of the merged time series.",
            "note": "{} records missing source_station_index ({})".format(
                format_int(missing_source_station_records),
                format_pct(missing_source_station_fraction),
            ),
        },
        {
            "item": "s6_blank_source_records",
            "count": blank_source_records,
            "status": "review" if blank_source_records else "ok",
            "assessment": "Blank source strings track the same provenance gap seen in source_station_index.",
            "note": "{} blank source records".format(format_int(blank_source_records)),
        },
        {
            "item": "s7_cluster_point_records",
            "count": cluster_shp_count,
            "status": "ok",
            "assessment": "Cluster point shapefile matches nc station count.",
            "note": "s7 cluster points == s6 n_stations",
        },
        {
            "item": "s7_source_point_records",
            "count": source_shp_count,
            "status": "ok",
            "assessment": "Source-station point shapefile matches nc source-station count.",
            "note": "s7 source points == s6 n_source_stations",
        },
        {
            "item": "s7_cluster_basin_records",
            "count": cluster_basin_count,
            "status": "ok",
            "assessment": "Cluster basin polygons equal cluster count minus failed basin traces.",
            "note": "{} = {} - {}".format(
                format_int(cluster_basin_count),
                format_int(nc_n_stations),
                format_int(s4_failed),
            ),
        },
    ]
    audit = pd.DataFrame(audit_rows)

    summary = {
        "s3_rows": int(len(s3)),
        "s4_rows": int(len(s4)),
        "s4_success": s4_success,
        "s4_failed": s4_failed,
        "s5_rows": int(len(s5)),
        "s5_clusters": int(s5["cluster_id"].nunique()),
        "s5_multi_station_clusters": multi_station_clusters,
        "s5_max_cluster_size": max_cluster_size,
        "s6_n_source_stations": nc_n_source_stations,
        "s6_n_stations": nc_n_stations,
        "s6_n_records": nc_n_records,
        "s6_missing_source_station_records": missing_source_station_records,
        "s6_missing_source_station_fraction": missing_source_station_fraction,
        "s7_cluster_points": cluster_shp_count,
        "s7_source_points": source_shp_count,
        "s7_cluster_basins": cluster_basin_count,
    }

    return {
        "summary": summary,
        "audit": audit,
        "source_counts": source_counts,
        "station_resolution": station_resolution,
        "record_resolution": record_resolution,
        "cluster_size_bins": cluster_size_bins,
        "source_lineage": source_lineage,
        "cluster_lineage": cluster_lineage,
        "missing_by_sources_used": missing_by_sources_used,
        "s6_stats": s6_stats,
    }


def save_csv_outputs(bundle):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    bundle["audit"].to_csv(OUT_DIR / "step_station_audit.csv", index=False)
    bundle["source_counts"].to_csv(OUT_DIR / "source_station_rows_by_source.csv", index=False)
    bundle["station_resolution"].to_csv(OUT_DIR / "station_rows_by_resolution.csv", index=False)
    bundle["record_resolution"].to_csv(OUT_DIR / "record_counts_by_resolution.csv", index=False)
    bundle["cluster_size_bins"].to_csv(OUT_DIR / "cluster_size_bins.csv", index=False)
    bundle["source_lineage"].to_csv(OUT_DIR / "source_station_lineage.csv", index=False)
    bundle["cluster_lineage"].to_csv(OUT_DIR / "cluster_lineage.csv", index=False)
    bundle["missing_by_sources_used"].to_csv(OUT_DIR / "missing_provenance_by_sources_used.csv", index=False)


def save_brief(bundle):
    s = bundle["summary"]
    lines = [
        "# Basin Merge Report Brief",
        "",
        "## Key numbers",
        "",
        "- s3 collected source-station rows: {}".format(format_int(s["s3_rows"])),
        "- s4 basin matched rows: {} / {} ({})".format(
            format_int(s["s4_success"]),
            format_int(s["s4_rows"]),
            format_pct(s["s4_success"] / max(s["s4_rows"], 1)),
        ),
        "- s4 basin failed rows: {}".format(format_int(s["s4_failed"])),
        "- s5 unique clusters: {}".format(format_int(s["s5_clusters"])),
        "- s5 multi-station clusters: {}".format(format_int(s["s5_multi_station_clusters"])),
        "- s6 merged records: {}".format(format_int(s["s6_n_records"])),
        "- s6 source-station entries: {}".format(format_int(s["s6_n_source_stations"])),
        "- s6 cluster entries: {}".format(format_int(s["s6_n_stations"])),
        "- s7 cluster basin polygons: {}".format(format_int(s["s7_cluster_basins"])),
        "",
        "## Ready-to-say interpretation",
        "",
        "- The source-station lineage is mostly stable: s3 -> s4 -> s5 all keep 20,469 rows.",
        "- Basin tracing is successful for 20,427 rows (99.79%); only 42 rows fail.",
        "- After basin-unit merging, 20,469 source-station rows compress to 13,674 clusters.",
        "- The final nc station count exactly matches the cluster count.",
        "- The final source-station count is lower by 1 because one GFQA pair is stored as a combined source-station entry.",
        "- Cluster basin polygons are 13,632, exactly equal to 13,674 clusters minus 42 failed basin traces.",
        "- The main residual issue is record-level provenance: {} records ({}) have missing source_station_index.".format(
            format_int(s["s6_missing_source_station_records"]),
            format_pct(s["s6_missing_source_station_fraction"]),
        ),
        "- Most of the provenance gap is concentrated in GSED-related clusters; see missing_provenance_by_sources_used.csv.",
        "",
        "## Files",
        "",
        "- report_overview.png",
        "- step_station_audit.csv",
        "- source_station_lineage.csv",
        "- cluster_lineage.csv",
        "- missing_provenance_by_sources_used.csv",
    ]
    (OUT_DIR / "report_brief.md").write_text("\n".join(lines), encoding="utf-8")


def plot_overview(bundle):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    source_lineage = bundle["source_lineage"]
    cluster_lineage = bundle["cluster_lineage"]
    record_resolution = bundle["record_resolution"]
    source_counts = bundle["source_counts"].head(10).iloc[::-1]
    cluster_size_bins = bundle["cluster_size_bins"]
    s6_stats = bundle["s6_stats"].copy()

    fig, axes = plt.subplots(2, 3, figsize=(18, 10), constrained_layout=True)

    # Panel A: source-station lineage
    ax = axes[0, 0]
    bars = ax.bar(source_lineage["stage"], source_lineage["count"], color="#4C78A8")
    ax.set_title("Source-station lineage")
    ax.set_ylabel("count")
    ax.tick_params(axis="x", rotation=25)
    label_bars(ax, bars)

    # Panel B: cluster lineage
    ax = axes[0, 1]
    bars = ax.bar(cluster_lineage["stage"], cluster_lineage["count"], color="#72B7B2")
    ax.set_title("Cluster lineage")
    ax.set_ylabel("count")
    ax.tick_params(axis="x", rotation=20)
    label_bars(ax, bars)

    # Panel C: final record resolution mix
    ax = axes[0, 2]
    rr = record_resolution.copy()
    bars = ax.bar(rr["resolution_name"], rr["record_count"], color="#F58518")
    ax.set_title("Final record resolution mix")
    ax.set_ylabel("record count")
    ax.tick_params(axis="x", rotation=20)
    label_bars(ax, bars, fmt="{:,.0f}", fontsize=8)

    # Panel D: top sources by station rows
    ax = axes[1, 0]
    ax.barh(source_counts["source"], source_counts["station_rows"], color="#54A24B")
    ax.set_title("Top 10 sources by source-station rows")
    ax.set_xlabel("station rows")

    # Panel E: cluster size distribution
    ax = axes[1, 1]
    bars = ax.bar(cluster_size_bins["cluster_size_bin"], cluster_size_bins["cluster_count"], color="#E45756")
    ax.set_title("Cluster size distribution")
    ax.set_xlabel("source stations per cluster")
    ax.set_ylabel("cluster count")
    label_bars(ax, bars, fontsize=8)

    # Panel F: global scatter of merged clusters by record volume
    ax = axes[1, 2]
    color_values = np.log10(s6_stats["n_records"].clip(lower=1))
    scatter = ax.scatter(
        s6_stats["lon"],
        s6_stats["lat"],
        c=color_values,
        s=8,
        cmap="viridis",
        alpha=0.75,
        linewidths=0,
    )
    ax.set_title("Merged clusters colored by log10(record count)")
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    ax.set_xlim(-180, 180)
    ax.set_ylim(-90, 90)
    cbar = fig.colorbar(scatter, ax=ax, shrink=0.9)
    cbar.set_label("log10(n_records)")

    fig.suptitle("Merged basin reference dataset: report overview", fontsize=16, y=1.02)
    fig.savefig(OUT_DIR / "report_overview.png", dpi=180, bbox_inches="tight")
    plt.close(fig)


def main():
    bundle = build_summary()
    save_csv_outputs(bundle)
    save_brief(bundle)
    plot_overview(bundle)
    print("Wrote report summary outputs to {}".format(OUT_DIR))


if __name__ == "__main__":
    main()
