#!/usr/bin/env python3
"""Spatial coverage statistics computed only from release products."""
# ---- Library path setup: MUST happen before any extension-module imports ----
import os as _os
import ctypes as _ctypes
from pathlib import Path as _Path
_conda_lib = "/share/home/dq134/.conda/envs/wzx/lib"
if _os.path.isdir(_conda_lib):
    _os.environ["LD_LIBRARY_PATH"] = _conda_lib + _os.pathsep + _os.environ.get("LD_LIBRARY_PATH", "")
    try:
        _ctypes.CDLL(str(_Path(_conda_lib) / "libstdc++.so.6"), mode=_ctypes.RTLD_GLOBAL)
    except Exception:
        pass
del _os, _ctypes, _Path, _conda_lib
# ---------------------------------------------------------------------------





import argparse
import shutil
import sqlite3
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
    text_series,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import attach_source_classification, pct, save_figure, unique_pipe, write_geojson_points
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


def _valid_latlon(frame: pd.DataFrame, lat_col="lat", lon_col="lon") -> pd.Series:
    lat = numeric_series(frame, lat_col)
    lon = numeric_series(frame, lon_col)
    return lat.between(-90, 90) & lon.between(-180, 180)


def _mode_text(series: pd.Series, default: str = "Unknown") -> str:
    cleaned = series.map(clean_text)
    cleaned = cleaned[cleaned.ne("")]
    if cleaned.empty:
        return default
    return str(cleaned.value_counts().index[0])


def _gpkg_layer_counts(ctx, file_name: str) -> pd.DataFrame:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(columns=["file_name", "layer", "feature_count"])
    rows = []
    with sqlite3.connect(str(path)) as conn:
        layers = pd.read_sql_query(
            "SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name",
            conn,
        )["table_name"].astype(str).tolist()
        for layer in layers:
            try:
                count = pd.read_sql_query(
                    "SELECT COUNT(*) AS n FROM {}".format('"' + layer.replace('"', '""') + '"'),
                    conn,
                )["n"].iloc[0]
            except Exception:
                count = -1
            rows.append({"file_name": file_name, "layer": layer, "feature_count": int(count)})
    return pd.DataFrame(rows)


def _cluster_table(station: pd.DataFrame) -> pd.DataFrame:
    work = station.copy()
    for col in ("cluster_uid", "resolution", "country", "continent_region", "iso_a3", "basin_status", "basin_flag"):
        work[col] = text_series(work, col)
    work["record_count"] = numeric_series(work, "record_count").fillna(0)
    work["basin_area"] = numeric_series(work, "basin_area")
    work["n_upstream_reaches"] = numeric_series(work, "n_upstream_reaches")
    work["lat"] = numeric_series(work, "lat")
    work["lon"] = numeric_series(work, "lon")
    work["valid_latlon"] = _valid_latlon(work)
    grouped = []
    for uid, group in work.groupby("cluster_uid", dropna=False, sort=False):
        grouped.append(
            {
                "cluster_uid": uid,
                "n_resolutions": int(group["resolution"].nunique()),
                "available_resolutions": "|".join(sorted(v for v in group["resolution"].unique() if v)),
                "record_count": int(group["record_count"].sum()),
                "country": clean_text(group["country"].replace("", np.nan).dropna().iloc[0]) if group["country"].ne("").any() else "Unknown",
                "continent_region": clean_text(group["continent_region"].replace("", np.nan).dropna().iloc[0]) if group["continent_region"].ne("").any() else "Unknown",
                "iso_a3": clean_text(group["iso_a3"].replace("", np.nan).dropna().iloc[0]) if group["iso_a3"].ne("").any() else "",
                "basin_status": clean_text(group["basin_status"].replace("", np.nan).dropna().iloc[0]) if group["basin_status"].ne("").any() else "unknown",
                "basin_flag": clean_text(group["basin_flag"].replace("", np.nan).dropna().iloc[0]) if group["basin_flag"].ne("").any() else "",
                "basin_area": float(group["basin_area"].dropna().iloc[0]) if group["basin_area"].notna().any() else np.nan,
                "n_upstream_reaches": float(group["n_upstream_reaches"].dropna().iloc[0]) if group["n_upstream_reaches"].notna().any() else np.nan,
                "lat": float(group["lat"].dropna().iloc[0]) if group["lat"].notna().any() else np.nan,
                "lon": float(group["lon"].dropna().iloc[0]) if group["lon"].notna().any() else np.nan,
                "valid_latlon": int(group["valid_latlon"].any()),
            }
        )
    return pd.DataFrame(grouped)


def _source_membership(station: pd.DataFrame, source_station: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if not source_station.empty and "source_name" in source_station.columns:
        work = source_station.copy()
        work["n_records"] = numeric_series(work, "n_records").fillna(0)
        work["source_station_lat"] = numeric_series(work, "source_station_lat")
        work["source_station_lon"] = numeric_series(work, "source_station_lon")
        for _, row in work.iterrows():
            rows.append(
                {
                    "source_name": clean_text(row.get("source_name", "")),
                    "cluster_uid": clean_text(row.get("cluster_uid", "")),
                    "resolution": clean_text(row.get("resolution", "")),
                    "record_count": float(row.get("n_records", 0) or 0),
                    "lat": row.get("source_station_lat", np.nan),
                    "lon": row.get("source_station_lon", np.nan),
                    "country": clean_text(row.get("country", "")),
                    "continent_region": clean_text(row.get("continent_region", "")),
                }
            )
    elif not station.empty and "sources_used" in station.columns:
        work = station.copy()
        work["record_count"] = numeric_series(work, "record_count").fillna(0)
        for _, row in work.iterrows():
            for source in unique_pipe([row.get("sources_used", "")]).split("|"):
                if source:
                    rows.append(
                        {
                            "source_name": source,
                            "cluster_uid": clean_text(row.get("cluster_uid", "")),
                            "resolution": clean_text(row.get("resolution", "")),
                            "record_count": float(row.get("record_count", 0) or 0),
                            "lat": row.get("lat", np.nan),
                            "lon": row.get("lon", np.nan),
                            "country": clean_text(row.get("country", "")),
                            "continent_region": clean_text(row.get("continent_region", "")),
                        }
                    )
    return pd.DataFrame(rows)


def _area_distribution(clusters: pd.DataFrame) -> pd.DataFrame:
    area = pd.to_numeric(clusters.get("basin_area", pd.Series([], dtype=float)), errors="coerce")
    valid = area[np.isfinite(area) & (area > 0)]
    rows = [
        {"section": "summary", "label": "valid_cluster_count", "value_km2": "", "cluster_count": int(valid.size), "fraction_of_valid_area_clusters": ""},
        {
            "section": "summary",
            "label": "missing_or_invalid_cluster_count",
            "value_km2": "",
            "cluster_count": int(len(area) - valid.size),
            "fraction_of_valid_area_clusters": "",
        },
    ]
    for label, value in (
        ("min", valid.min() if len(valid) else np.nan),
        ("p05", valid.quantile(0.05) if len(valid) else np.nan),
        ("p25", valid.quantile(0.25) if len(valid) else np.nan),
        ("mean", valid.mean() if len(valid) else np.nan),
        ("median", valid.median() if len(valid) else np.nan),
        ("p75", valid.quantile(0.75) if len(valid) else np.nan),
        ("p95", valid.quantile(0.95) if len(valid) else np.nan),
        ("max", valid.max() if len(valid) else np.nan),
    ):
        rows.append({"section": "summary", "label": label, "value_km2": value, "cluster_count": "", "fraction_of_valid_area_clusters": ""})
    bins = [0, 10, 100, 1000, 10000, 100000, np.inf]
    labels = ["<10 km2", "10-100 km2", "100-1,000 km2", "1,000-10,000 km2", "10,000-100,000 km2", ">100,000 km2"]
    cats = pd.cut(valid, bins=bins, labels=labels, include_lowest=False)
    counts = cats.value_counts().reindex(labels).fillna(0)
    for label, count in counts.items():
        rows.append(
            {
                "section": "bin",
                "label": label,
                "value_km2": "",
                "cluster_count": int(count),
                "fraction_of_valid_area_clusters": float(count / valid.size) if valid.size else 0.0,
            }
        )
    return pd.DataFrame(rows)


def _spatial_summary(clusters: pd.DataFrame, station: pd.DataFrame, satellite: pd.DataFrame, gpkg_layers: pd.DataFrame) -> pd.DataFrame:
    total = int(clusters["cluster_uid"].nunique()) if "cluster_uid" in clusters.columns else 0
    resolved = int(clusters["basin_status"].eq("resolved").sum()) if "basin_status" in clusters.columns else 0
    unresolved = int(clusters["basin_status"].eq("unresolved").sum()) if "basin_status" in clusters.columns else 0
    unknown_status = max(0, total - resolved - unresolved)
    area = pd.to_numeric(clusters.get("basin_area", pd.Series([], dtype=float)), errors="coerce")
    valid_area = area[np.isfinite(area) & (area > 0)]
    lat = pd.to_numeric(clusters.get("lat", pd.Series([], dtype=float)), errors="coerce")
    lon = pd.to_numeric(clusters.get("lon", pd.Series([], dtype=float)), errors="coerce")
    basin_polygons = 0
    if not gpkg_layers.empty and "layer" in gpkg_layers.columns:
        hits = gpkg_layers[gpkg_layers["layer"].astype(str).str.contains("basin", case=False, na=False)]
        basin_polygons = int(pd.to_numeric(hits.get("feature_count", pd.Series([], dtype=float)), errors="coerce").clip(lower=0).sum())
    rows = [
        ("main_release", "station_catalog_rows", len(station), "rows", "station_catalog.csv", ""),
        ("main_release", "final_cluster_count", total, "clusters", "station_catalog.csv", ""),
        ("main_release", "daily_cluster_count", int(station[station["resolution"].astype(str).eq("daily")]["cluster_uid"].nunique()) if "resolution" in station.columns else 0, "clusters", "station_catalog.csv", ""),
        ("main_release", "monthly_cluster_count", int(station[station["resolution"].astype(str).eq("monthly")]["cluster_uid"].nunique()) if "resolution" in station.columns else 0, "clusters", "station_catalog.csv", ""),
        ("main_release", "annual_cluster_count", int(station[station["resolution"].astype(str).eq("annual")]["cluster_uid"].nunique()) if "resolution" in station.columns else 0, "clusters", "station_catalog.csv", ""),
        ("basin_assignment", "resolved_cluster_count", resolved, "clusters", "station_catalog.csv", ""),
        ("basin_assignment", "resolved_cluster_percent", pct(resolved, total), "percent", "station_catalog.csv", ""),
        ("basin_assignment", "unresolved_cluster_count", unresolved, "clusters", "station_catalog.csv", ""),
        ("basin_assignment", "unresolved_cluster_percent", pct(unresolved, total), "percent", "station_catalog.csv", ""),
        ("basin_assignment", "unknown_status_cluster_count", unknown_status, "clusters", "station_catalog.csv", ""),
        ("basin_assignment", "unknown_status_cluster_percent", pct(unknown_status, total), "percent", "station_catalog.csv", ""),
        ("basin_polygons", "basin_polygon_cluster_count", basin_polygons, "clusters", PRODUCT_FILES["cluster_basins_gpkg"], ""),
        ("basin_polygons", "basin_polygon_cluster_percent", pct(basin_polygons, total), "percent", PRODUCT_FILES["cluster_basins_gpkg"], ""),
        ("coordinates", "clusters_with_valid_lat_lon", int(clusters["valid_latlon"].sum()) if "valid_latlon" in clusters.columns else 0, "clusters", "station_catalog.csv", ""),
        ("coordinates", "latitude_min", float(lat.min()) if lat.notna().any() else np.nan, "degrees_north", "station_catalog.csv", ""),
        ("coordinates", "latitude_max", float(lat.max()) if lat.notna().any() else np.nan, "degrees_north", "station_catalog.csv", ""),
        ("coordinates", "longitude_min", float(lon.min()) if lon.notna().any() else np.nan, "degrees_east", "station_catalog.csv", ""),
        ("coordinates", "longitude_max", float(lon.max()) if lon.notna().any() else np.nan, "degrees_east", "station_catalog.csv", ""),
        ("area", "upstream_area_valid_cluster_count", int(valid_area.size), "clusters", "station_catalog.csv", ""),
        ("area", "upstream_area_missing_or_invalid_cluster_count", int(total - valid_area.size), "clusters", "station_catalog.csv", ""),
        ("geography", "unknown_country_cluster_count", int(clusters["country_canonical"].eq("Unknown").sum()) if "country_canonical" in clusters.columns else 0, "clusters", "station_catalog.csv", ""),
        ("satellite_validation", "satellite_station_rows", len(satellite), "rows", "satellite_catalog.csv", ""),
        ("satellite_validation", "satellite_linked_cluster_count", int(satellite["cluster_uid"].nunique()) if not satellite.empty and "cluster_uid" in satellite.columns else 0, "clusters", "satellite_catalog.csv", ""),
        ("satellite_validation", "satellite_record_count", int(numeric_series(satellite, "n_records").fillna(0).sum()) if not satellite.empty else 0, "records", "satellite_catalog.csv", ""),
    ]
    for label in ("min", "p05", "p25", "mean", "median", "p75", "p95", "max"):
        if len(valid_area):
            value = getattr(valid_area, label)() if label in {"min", "max", "mean", "median"} else valid_area.quantile({"p05": 0.05, "p25": 0.25, "p75": 0.75, "p95": 0.95}[label])
        else:
            value = np.nan
        rows.append(("area", "upstream_area_{}".format(label), value, "km2", "station_catalog.csv", ""))
    return pd.DataFrame(rows, columns=["section", "metric", "value", "unit", "source_file", "notes"])


def _canonicalize_geo(clusters: pd.DataFrame) -> tuple:
    rows = []
    key = clusters["iso_a3"].map(clean_text)
    key = key.where(key.ne(""), clusters["country"].map(clean_text))
    work = clusters.assign(_geo_key=key)
    for geo_key, group in work.groupby("_geo_key", dropna=False, sort=False):
        countries = sorted(set(v for v in group["country"].map(clean_text) if v))
        regions = sorted(set(v for v in group["continent_region"].map(clean_text) if v))
        rows.append(
            {
                "_geo_key": geo_key,
                "iso_a3": _mode_text(group["iso_a3"], ""),
                "country_canonical": _mode_text(group["country"]),
                "continent_region_canonical": _mode_text(group["continent_region"]),
                "country_aliases": "|".join(countries),
                "continent_region_aliases": "|".join(regions),
                "cluster_count": int(group["cluster_uid"].nunique()),
                "has_alias_conflict": int(len(countries) > 1 or len(regions) > 1),
            }
        )
    aliases = pd.DataFrame(rows)
    out = work.merge(aliases[["_geo_key", "country_canonical", "continent_region_canonical"]], on="_geo_key", how="left")
    return out.drop(columns=["_geo_key"]), aliases.drop(columns=["_geo_key"])


def build_spatial_stats(ctx) -> dict:
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"])
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    satellite = ctx.read_csv(PRODUCT_FILES["satellite_catalog"], required=False)

    clusters = _cluster_table(station)
    clusters, country_aliases = _canonicalize_geo(clusters)
    by_resolution = (
        station.assign(record_count=numeric_series(station, "record_count").fillna(0), iso_a3_clean=text_series(station, "iso_a3"))
        .groupby("resolution", dropna=False)
        .agg(
            station_rows=("cluster_uid", "size"),
            cluster_count=("cluster_uid", "nunique"),
            record_count=("record_count", "sum"),
            country_count=("iso_a3_clean", lambda s: s.fillna("").astype(str).str.strip().replace("", np.nan).nunique()),
        )
        .reset_index()
    )
    by_country = (
        clusters.groupby(["continent_region_canonical", "country_canonical", "iso_a3"], dropna=False)
        .agg(
            cluster_count=("cluster_uid", "nunique"),
            record_count=("record_count", "sum"),
            valid_latlon_count=("valid_latlon", "sum"),
        )
        .reset_index()
        .rename(columns={"continent_region_canonical": "continent_region", "country_canonical": "country"})
        .sort_values(["cluster_count", "record_count"], ascending=False)
    )
    basin_status = (
        clusters.groupby(["basin_status", "basin_flag"], dropna=False)
        .agg(cluster_count=("cluster_uid", "nunique"), record_count=("record_count", "sum"))
        .reset_index()
        .sort_values("cluster_count", ascending=False)
    )
    gpkg_layers = pd.concat(
        [
            _gpkg_layer_counts(ctx, PRODUCT_FILES["cluster_points_gpkg"]),
            _gpkg_layer_counts(ctx, PRODUCT_FILES["cluster_basins_gpkg"]),
            _gpkg_layer_counts(ctx, PRODUCT_FILES["source_stations_gpkg"]),
        ],
        ignore_index=True,
    )
    satellite_summary = pd.DataFrame()
    if not satellite.empty:
        satellite_summary = (
            satellite.assign(n_records=numeric_series(satellite, "n_records").fillna(0))
            .groupby(["source", "resolution"], dropna=False)
            .agg(
                satellite_station_count=("satellite_station_uid", "nunique"),
                linked_cluster_count=("cluster_uid", "nunique"),
                record_count=("n_records", "sum"),
            )
            .reset_index()
            .sort_values("record_count", ascending=False)
        )
    source_geo = pd.DataFrame()
    if not source_station.empty:
        source_geo = (
            source_station.groupby(["continent_region", "country"], dropna=False)
            .agg(source_station_rows=("source_station_uid", "size"), sources=("source_name", "nunique"))
            .reset_index()
            .sort_values("source_station_rows", ascending=False)
        )
    headline = pd.DataFrame(
        [
            {"metric": "cluster_count", "value": int(clusters["cluster_uid"].nunique()), "unit": "clusters"},
            {"metric": "station_catalog_rows", "value": int(len(station)), "unit": "rows"},
            {"metric": "valid_latlon_clusters", "value": int(clusters["valid_latlon"].sum()), "unit": "clusters"},
            {"metric": "unknown_country_clusters", "value": int(clusters["country_canonical"].eq("Unknown").sum()), "unit": "clusters"},
            {"metric": "source_station_rows", "value": int(len(source_station)), "unit": "rows"},
            {"metric": "satellite_catalog_rows", "value": int(len(satellite)), "unit": "rows"},
        ]
    )
    source_membership = _source_membership(station, source_station)
    if not source_membership.empty:
        source_by = (
            source_membership.groupby("source_name", dropna=False)
            .agg(
                cluster_count=("cluster_uid", "nunique"),
                source_station_count=("cluster_uid", "size"),
                source_station_resolution_rows=("resolution", "size"),
                record_count=("record_count", "sum"),
                available_resolutions=("resolution", unique_pipe),
                min_lat=("lat", "min"),
                max_lat=("lat", "max"),
                min_lon=("lon", "min"),
                max_lon=("lon", "max"),
            )
            .reset_index()
        )
        total_records = float(source_by["record_count"].sum()) or 1.0
        total_clusters_sum = float(source_by["cluster_count"].sum()) or 1.0
        source_by["record_percent_of_source_catalog"] = source_by["record_count"].map(lambda v: pct(v, total_records))
        source_by["cluster_percent_of_source_rows"] = source_by["cluster_count"].map(lambda v: pct(v, total_clusters_sum))
        source_by = source_by.sort_values(["cluster_count", "record_count"], ascending=False)
        region_source = (
            source_membership.groupby(["continent_region", "source_name"], dropna=False)
            .agg(cluster_count=("cluster_uid", "nunique"), record_count=("record_count", "sum"))
            .reset_index()
            .sort_values(["continent_region", "record_count"], ascending=[True, False])
        )
        source_type = attach_source_classification(source_by, "source_name")
        source_type = (
            source_type.groupby(["source_type", "source_group"], dropna=False)
            .agg(source_count=("source_name", "nunique"), cluster_count=("cluster_count", "sum"), record_count=("record_count", "sum"))
            .reset_index()
            .sort_values("record_count", ascending=False)
        )
    else:
        source_by = pd.DataFrame()
        region_source = pd.DataFrame()
        source_type = pd.DataFrame()
    by_region = (
        clusters.groupby("continent_region_canonical", dropna=False)
        .agg(cluster_count=("cluster_uid", "nunique"), record_count=("record_count", "sum"), country_count=("country_canonical", "nunique"))
        .reset_index()
        .rename(columns={"continent_region_canonical": "continent_region"})
        .sort_values("cluster_count", ascending=False)
    )
    by_region_resolution = (
        station.assign(record_count=numeric_series(station, "record_count").fillna(0))
        .groupby(["continent_region", "resolution"], dropna=False)
        .agg(cluster_count=("cluster_uid", "nunique"), record_count=("record_count", "sum"))
        .reset_index()
        .sort_values(["continent_region", "record_count"], ascending=[True, False])
        if {"continent_region", "resolution"}.issubset(station.columns)
        else pd.DataFrame()
    )
    upstream = _area_distribution(clusters)
    unknown_geo = clusters[
        clusters["country_canonical"].eq("Unknown") | clusters["continent_region_canonical"].eq("Unknown")
    ].copy()
    cluster_attrs = clusters[
        [col for col in ("cluster_uid", "country_canonical", "continent_region_canonical", "iso_a3", "lat", "lon", "basin_area", "basin_status", "basin_flag", "record_count", "available_resolutions") if col in clusters.columns]
    ].rename(columns={"country_canonical": "country", "continent_region_canonical": "continent_region"})
    spatial_summary = _spatial_summary(clusters, station, satellite, gpkg_layers)
    satellite_upstream = pd.DataFrame(columns=["section", "label", "value_km2", "cluster_count", "fraction_of_valid_area_clusters"])
    return {
        "headline": headline,
        "by_resolution": by_resolution,
        "by_country": by_country,
        "country_aliases": country_aliases.sort_values(["has_alias_conflict", "cluster_count"], ascending=False),
        "basin_status": basin_status,
        "gpkg_layers": gpkg_layers,
        "satellite_summary": satellite_summary,
        "source_geo": source_geo,
        "table_spatial_coverage_summary": spatial_summary,
        "article_spatial_coverage_metrics": spatial_summary,
        "table_spatial_coverage_by_resolution": by_resolution.rename(columns={"station_rows": "source_station_resolution_rows"}),
        "table_spatial_coverage_by_region": by_region,
        "table_spatial_coverage_by_country": by_country,
        "table_upstream_area_distribution": upstream,
        "table_spatial_coverage_by_source": source_by,
        "table_spatial_coverage_by_region_source": region_source,
        "table_spatial_coverage_by_region_resolution": by_region_resolution,
        "table_spatial_coverage_by_source_type": source_type,
        "table_satellite_validation_spatial_coverage": satellite_summary,
        "table_satellite_upstream_area_distribution_s4": satellite_upstream,
        "table_unknown_country_region_clusters": unknown_geo,
        "table_basin_polygon_layers": gpkg_layers.rename(columns={"layer": "layer_name"}),
        "table_cluster_spatial_attributes": cluster_attrs,
    }


def write_figures(stats: dict, figures_dir: Path, dpi: int, top_n: int = 15) -> None:
    """Write spatial coverage figures."""
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)

    # Bar chart: clusters by resolution
    by_res = stats.get("by_resolution", pd.DataFrame())
    if not by_res.empty:
        fig, ax = plt.subplots(figsize=(7.2, 4.0))
        res_order = ["daily", "monthly", "annual", "climatology", "other"]
        plot_data = by_res.set_index("resolution").reindex(res_order).dropna(subset=["cluster_count"])
        colors = ["#4c78a8", "#f58518", "#54a24b", "#e45756", "#72b7b2"]
        ax.bar(plot_data.index, plot_data["cluster_count"], color=colors[:len(plot_data)])
        ax.set_xlabel("Resolution")
        ax.set_ylabel("Cluster count")
        ax.set_title("Clusters by temporal resolution")
        for i, (_, row) in enumerate(plot_data.iterrows()):
            ax.text(i, row["cluster_count"], "{:,.0f}".format(row["cluster_count"]),
                    ha="center", va="bottom", fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_clusters_by_resolution.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_spatial_coverage_by_resolution.png", dpi=dpi)
        plt.close(fig)

    # Horizontal bar: top countries
    by_country = stats.get("by_country", pd.DataFrame())
    if not by_country.empty:
        plot_data = by_country.head(top_n).sort_values("cluster_count", ascending=True)
        fig, ax = plt.subplots(figsize=(7.2, max(4.0, 0.3 * len(plot_data) + 1.5)))
        labels = plot_data["country"].astype(str).str.replace("_", " ").str.title()
        ax.barh(labels, plot_data["cluster_count"], color="#4c78a8")
        ax.set_xlabel("Cluster count")
        ax.set_title("Top {} countries by cluster count".format(top_n))
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_top_countries.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_spatial_coverage_by_region_country.png", dpi=dpi)
        plt.close(fig)

    clusters = stats.get("table_cluster_spatial_attributes", pd.DataFrame())
    if not clusters.empty:
        valid = clusters[
            pd.to_numeric(clusters.get("lat"), errors="coerce").between(-90, 90)
            & pd.to_numeric(clusters.get("lon"), errors="coerce").between(-180, 180)
        ].copy()
        write_geojson_points(valid, figures_dir / "global_cluster_distribution_points.geojson")
        if not valid.empty:
            fig, ax = plt.subplots(figsize=(10, 4.8))
            area = pd.to_numeric(valid.get("basin_area"), errors="coerce").fillna(0)
            sizes = np.clip(np.sqrt(area.clip(lower=0)) / 10.0, 4, 90)
            ax.scatter(pd.to_numeric(valid["lon"], errors="coerce"), pd.to_numeric(valid["lat"], errors="coerce"), s=sizes, alpha=0.45, color="#4c78a8", edgecolors="none")
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            ax.set_title("Global cluster distribution")
            ax.grid(alpha=0.25)
            fig.tight_layout()
            save_figure(fig, figures_dir / "fig_global_cluster_distribution.png", dpi=dpi)
            save_figure(fig, figures_dir / "fig_global_bubble_map.png", dpi=dpi, also_pdf=False)
            save_figure(fig, figures_dir / "fig_timeseries_spatial_coverage.png", dpi=dpi)
            plt.close(fig)

    by_region = stats.get("table_spatial_coverage_by_region", pd.DataFrame())
    if not by_region.empty:
        plot = by_region.head(top_n).sort_values("cluster_count")
        fig, ax = plt.subplots(figsize=(8, max(3.5, 0.35 * len(plot) + 1.3)))
        ax.barh(plot["continent_region"].astype(str), plot["cluster_count"], color="#72b7b2")
        ax.set_xlabel("Clusters")
        ax.set_title("Spatial coverage by region")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_spatial_coverage_by_region.png", dpi=dpi)
        plt.close(fig)

    upstream = stats.get("table_upstream_area_distribution", pd.DataFrame())
    if not upstream.empty:
        bins = upstream[upstream["section"].eq("bin")]
        if not bins.empty:
            fig, ax = plt.subplots(figsize=(8, 4.2))
            ax.bar(bins["label"].astype(str), pd.to_numeric(bins["cluster_count"], errors="coerce").fillna(0), color="#f58518")
            ax.set_ylabel("Clusters")
            ax.set_title("Upstream area distribution")
            ax.tick_params(axis="x", rotation=35)
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            save_figure(fig, figures_dir / "fig_upstream_area_distribution.png", dpi=dpi)
            save_figure(fig, figures_dir / "fig_upstream_area_histogram.png", dpi=dpi, also_pdf=False)
            save_figure(fig, figures_dir / "fig_satellite_upstream_area_distribution.png", dpi=dpi)
            plt.close(fig)

    source_by = stats.get("table_spatial_coverage_by_source", pd.DataFrame())
    if not source_by.empty:
        plot = source_by.head(top_n).sort_values("cluster_count")
        fig, ax = plt.subplots(figsize=(9, max(4, 0.32 * len(plot) + 1.5)))
        ax.barh(plot["source_name"].astype(str), plot["cluster_count"], color="#54a24b")
        ax.set_xlabel("Clusters")
        ax.set_title("Source spatial contribution")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_source_spatial_contribution.png", dpi=dpi)
        save_figure(fig, figures_dir / "fig_spatial_coverage_by_region_source_clusters.png", dpi=dpi)
        plt.close(fig)

        plot = source_by.head(top_n).sort_values("record_count")
        fig, ax = plt.subplots(figsize=(9, max(4, 0.32 * len(plot) + 1.5)))
        ax.barh(plot["source_name"].astype(str), plot["record_count"], color="#e45756")
        ax.set_xlabel("Records")
        ax.set_title("Source spatial contribution by records")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_spatial_coverage_by_region_source_records.png", dpi=dpi)
        plt.close(fig)

    region_res = stats.get("table_spatial_coverage_by_region_resolution", pd.DataFrame())
    if not region_res.empty:
        pivot = region_res.pivot_table(index="continent_region", columns="resolution", values="cluster_count", aggfunc="sum", fill_value=0)
        fig, ax = plt.subplots(figsize=(9, max(4, 0.35 * len(pivot) + 1.5)))
        pivot.plot(kind="barh", stacked=True, ax=ax)
        ax.set_xlabel("Clusters")
        ax.set_title("Region by resolution")
        ax.grid(axis="x", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_spatial_coverage_by_region_resolution.png", dpi=dpi)
        plt.close(fig)

    satellite = stats.get("table_satellite_validation_spatial_coverage", pd.DataFrame())
    if not satellite.empty:
        plot = satellite[satellite["source"].astype(str).ne("")].copy() if "source" in satellite.columns else satellite.copy()
        if not plot.empty:
            fig, ax = plt.subplots(figsize=(7.5, 4.0))
            ax.bar(plot["source"].astype(str), pd.to_numeric(plot["record_count"], errors="coerce").fillna(0), color="#b279a2")
            ax.set_ylabel("Records")
            ax.set_title("Satellite validation spatial coverage")
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            save_figure(fig, figures_dir / "fig_satellite_validation_spatial_distribution.png", dpi=dpi)
            save_figure(fig, figures_dir / "fig_main_vs_satellite_spatial_coverage.png", dpi=dpi)
            save_figure(fig, figures_dir / "fig_climatology_spatial_coverage.png", dpi=dpi)
            save_figure(fig, figures_dir / "fig_climatology_vs_timeseries_coverage.png", dpi=dpi)
            save_figure(fig, figures_dir / "fig_composite_spatial_coverage.png", dpi=dpi)
            plt.close(fig)

    basin = stats.get("basin_status", pd.DataFrame())
    if not basin.empty:
        fig, ax = plt.subplots(figsize=(7.2, 4.0))
        basin.groupby("basin_status")["cluster_count"].sum().plot(kind="bar", ax=ax, color="#4c78a8")
        ax.set_ylabel("Clusters")
        ax.set_title("Cluster status and basins")
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_global_cluster_status_and_basins.png", dpi=dpi)
        plt.close(fig)


def build_detailed_spatial_report(ctx, stats: dict, tables_dir: Path, figures_dir: Path, report_dir: Path) -> list[str]:
    headline = stats.get("headline", pd.DataFrame())
    summary = stats.get("table_spatial_coverage_summary", pd.DataFrame())
    by_resolution = stats.get("table_spatial_coverage_by_resolution", pd.DataFrame())
    by_region = stats.get("table_spatial_coverage_by_region", pd.DataFrame())
    by_country = stats.get("table_spatial_coverage_by_country", pd.DataFrame())
    by_source = stats.get("table_spatial_coverage_by_source", pd.DataFrame())
    region_resolution = stats.get("table_spatial_coverage_by_region_resolution", pd.DataFrame())
    source_type = stats.get("table_spatial_coverage_by_source_type", pd.DataFrame())
    upstream = stats.get("table_upstream_area_distribution", pd.DataFrame())
    basin_status = stats.get("basin_status", pd.DataFrame())
    satellite = stats.get("table_satellite_validation_spatial_coverage", pd.DataFrame())
    aliases = stats.get("country_aliases", pd.DataFrame())
    unknown = stats.get("table_unknown_country_region_clusters", pd.DataFrame())
    gpkg = stats.get("table_basin_polygon_layers", pd.DataFrame())

    cluster_total = ""
    record_total = ""
    if not headline.empty and {"metric", "value"}.issubset(headline.columns):
        hit = headline[headline["metric"].astype(str).eq("cluster_count")]
        cluster_total = hit.iloc[0]["value"] if not hit.empty else ""
        hit = headline[headline["metric"].astype(str).eq("record_count")]
        record_total = hit.iloc[0]["value"] if not hit.empty else ""
    unknown_count = len(unknown) if not unknown.empty else 0
    alias_conflicts = int(pd.to_numeric(aliases.get("has_alias_conflict", 0), errors="coerce").fillna(0).gt(0).sum()) if not aliases.empty else 0

    lines = [
        "# Release Spatial Coverage Statistics",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Spatial statistics use release catalogs, GeoPackages, and satellite validation catalogs only.",
        "",
        "## Headline",
        "",
        "- Reference clusters: {}".format(fmt_int(cluster_total)),
        "- Release records represented by station catalog: {}".format(fmt_int(record_total)),
        "- Country/region rows needing canonicalization review: {}".format(fmt_int(alias_conflicts)),
        "- Clusters with unknown country or region: {}".format(fmt_int(unknown_count)),
        "",
        "## Article-Ready Metrics",
        "",
        sorted_markdown_table(summary, columns=["section", "metric", "value", "unit", "source_file", "notes"], max_rows=18),
    ]
    append_table_section(
        lines,
        "Coverage by Temporal Resolution",
        by_resolution,
        columns=["resolution", "source_station_resolution_rows", "cluster_count", "record_count", "country_count"],
        sort_by="record_count",
        max_rows=8,
    )
    append_table_section(
        lines,
        "Coverage by Region",
        by_region,
        columns=["continent_region", "cluster_count", "record_count", "country_count"],
        sort_by="cluster_count",
        max_rows=12,
    )
    append_table_section(
        lines,
        "Top Countries",
        by_country,
        columns=["country", "iso_a3", "continent_region", "cluster_count", "record_count"],
        sort_by="cluster_count",
        max_rows=15,
        note="Country statistics prefer canonical country names and ISO3 codes where available.",
    )
    append_table_section(
        lines,
        "Top Source Spatial Contributions",
        by_source,
        columns=["source_name", "source_type", "cluster_count", "record_count", "country_count", "continent_region_count"],
        sort_by="record_count",
        max_rows=15,
    )
    append_table_section(
        lines,
        "Region by Resolution",
        region_resolution,
        columns=["continent_region", "resolution", "cluster_count", "record_count"],
        sort_by="record_count",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Source Type Footprint",
        source_type,
        columns=["source_type", "source_group", "source_count", "cluster_count", "record_count", "country_count"],
        sort_by="record_count",
        max_rows=12,
    )
    append_table_section(
        lines,
        "Upstream Area Distribution",
        upstream,
        columns=["section", "label", "value_km2", "cluster_count", "fraction_of_valid_area_clusters"],
        max_rows=16,
        note="Area metrics describe release basin polygons or catalog basin attributes, not new basin matching.",
    )
    append_table_section(
        lines,
        "Basin Assignment Status",
        basin_status,
        columns=["basin_status", "cluster_count", "record_count", "percent_clusters"],
        sort_by="cluster_count",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Satellite Validation Spatial Coverage",
        satellite,
        columns=["source", "satellite_station_count", "linked_cluster_count", "record_count", "country_count"],
        sort_by="record_count",
        max_rows=12,
        note="Satellite rows are validation-sidecar coverage; variable completeness is reported in the variable and QC modules.",
    )
    append_table_section(
        lines,
        "Country Alias Review",
        aliases,
        columns=["country_raw", "country_canonical", "iso_a3", "continent_region", "cluster_count", "has_alias_conflict"],
        sort_by="cluster_count",
        max_rows=15,
    )
    append_table_section(
        lines,
        "GeoPackage Layer Counts",
        gpkg,
        columns=["file_name", "layer_name", "feature_count"],
        sort_by="feature_count",
        max_rows=10,
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- Region and country statements should be made from canonical country/ISO3 tables rather than raw country text.",
            "- Basin status here is descriptive; unresolved or lower-confidence matches are analyzed in `basin_diagnostics`.",
            "- Satellite validation coverage is kept separate from the main in-situ matrix products.",
        ]
    )
    append_figure_index(lines, figures_dir, report_dir)
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only spatial coverage statistics.")
    add_common_args(parser, "spatial")
    parser.add_argument("--top-n", type=int, default=15, help="Number of top countries to show in figure.")
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    tables_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    stats = build_spatial_stats(ctx)
    for name, frame in stats.items():
        if name.startswith("table_") or name.startswith("article_"):
            write_csv(frame, tables_dir / "{}.csv".format(name))
        else:
            write_csv(frame, tables_dir / "table_{}.csv".format(name))
    if not args.skip_figures:
        try:
            write_figures(stats, ctx.figures_dir(), max(72, int(args.dpi)), max(5, int(args.top_n)))
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    md_path = ctx.output_path("reports", "spatial_coverage_stats.md")
    report_lines = build_detailed_spatial_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("article_spatial_coverage_summary.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote spatial stats to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
