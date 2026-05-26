#!/usr/bin/env python3
"""Generate spatial coverage tables and figures for Section 4.1.

Inputs default to the s7 basin-mainline products. The script keeps cluster-level
statistics at the s7_cluster_station_catalog.csv grain and only uses the basin
GPKG to count which clusters actually have exported polygons.
"""

import argparse
import json
import math
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_SCRIPT_DIR = SCRIPT_DIR.parent
if str(PROJECT_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_SCRIPT_DIR))

import numpy as np
import pandas as pd

from pipeline_paths import (
    S7_CLUSTER_BASINS_GPKG,
    S7_CLUSTER_STATION_CATALOG_CSV,
    S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV,
    get_output_r_root,
)

try:
    import geopandas as gpd
    from shapely.geometry import Point
    HAS_GPD = True
except ImportError:
    gpd = None
    Point = None
    HAS_GPD = False

try:
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    plt = None
    HAS_MPL = False

ROOT = get_output_r_root(PROJECT_SCRIPT_DIR)
DEFAULT_CLUSTER_CATALOG = ROOT / S7_CLUSTER_STATION_CATALOG_CSV
DEFAULT_SOURCE_CATALOG = ROOT / S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV
DEFAULT_BASIN_GPKG = ROOT / S7_CLUSTER_BASINS_GPKG
DEFAULT_TABLES_DIR = ROOT / "scripts_basin_test/output_other/tables"
DEFAULT_FIGURES_DIR = ROOT / "scripts_basin_test/output_other/figures"

AREA_BINS = [0, 10, 100, 1000, 10000, 100000, np.inf]
AREA_LABELS = [
    "<10 km²",
    "10–100 km²",
    "100–1,000 km²",
    "1,000–10,000 km²",
    "10,000–100,000 km²",
    ">100,000 km²",
]
COUNTRY_COLS = ["country", "country_name", "admin", "adm0_name", "NAME", "name", "NAME_EN"]
ISO_COLS = ["iso_a3", "ISO_A3", "adm0_a3", "ADM0_A3", "sov_a3", "SOV_A3"]
CONTINENT_COLS = ["continent", "CONTINENT"]
REGION_COLS = ["region", "REGION", "subregion", "SUBREGION", "region_un", "REGION_UN"]


def clean_text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def first_col(columns, candidates):
    lower = {str(c).lower(): c for c in columns}
    for col in candidates:
        if col in columns:
            return col
        if col.lower() in lower:
            return lower[col.lower()]
    return None


def read_csv(path, required=False):
    path = Path(path)
    if not path.is_file():
        if required:
            raise FileNotFoundError("Required input not found: {}".format(path))
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def add_cluster_key(df):
    out = df.copy()
    if "cluster_uid" in out.columns:
        uid = out["cluster_uid"].map(clean_text)
    else:
        uid = pd.Series([""] * len(out), index=out.index)
    if "cluster_id" in out.columns:
        cid = pd.to_numeric(out["cluster_id"], errors="coerce")
        cid_key = cid.map(lambda x: "cluster_id:{}".format(int(x)) if pd.notna(x) else "")
    else:
        cid_key = pd.Series([""] * len(out), index=out.index)
    fallback = pd.Series(["row_index:{}".format(i) for i in range(len(out))], index=out.index)
    key = uid.where(uid != "", cid_key).where(lambda s: s != "", fallback)
    out["cluster_key"] = key.astype(str)
    return out


def numeric(df, col):
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def valid_latlon(df, lat_col="lat", lon_col="lon"):
    lat = numeric(df, lat_col)
    lon = numeric(df, lon_col)
    return lat.between(-90, 90) & lon.between(-180, 180)


def normalise_clusters(df, area_col):
    out = add_cluster_key(df)
    for col in ["lat", "lon", "basin_area", "uparea_merit", "upstream_area", "area_km2"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["cluster_uid", "basin_status", "basin_flag", "available_resolutions", "sources_used"]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(clean_text)
    if area_col not in out.columns:
        for fallback in ["basin_area", "uparea_merit", "upstream_area", "area_km2"]:
            if fallback in out.columns:
                print("Warning: --area-column '{}' not found; using '{}'".format(area_col, fallback))
                area_col = fallback
                break
    if area_col not in out.columns:
        raise ValueError("No upstream-area column found; requested '{}'".format(area_col))
    out["area_km2"] = pd.to_numeric(out[area_col], errors="coerce")
    out["valid_latlon"] = valid_latlon(out)
    status = out["basin_status"].str.lower().str.strip()
    out["is_resolved"] = status.eq("resolved")
    out["is_unresolved"] = status.eq("unresolved")
    return out, area_col


def quote_ident(name):
    return '"' + str(name).replace('"', '""') + '"'


def basin_polygon_keys(gpkg_path, prefix="basin"):
    gpkg_path = Path(gpkg_path)
    keys = set()
    if not gpkg_path.is_file():
        print("Warning: basin GPKG not found; polygon count will be zero: {}".format(gpkg_path))
        return keys
    try:
        with sqlite3.connect(str(gpkg_path)) as conn:
            layers = pd.read_sql_query(
                "SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name",
                conn,
            )["table_name"].astype(str).tolist()
            layers = [x for x in layers if not prefix or x.startswith(prefix)]
            for layer in layers:
                cols = pd.read_sql_query("PRAGMA table_info({})".format(quote_ident(layer)), conn)["name"].astype(str).tolist()
                keep = [c for c in ["cluster_uid", "cluster_id"] if c in cols]
                if not keep:
                    continue
                sql = "SELECT {} FROM {}".format(", ".join(quote_ident(c) for c in keep), quote_ident(layer))
                rows = add_cluster_key(pd.read_sql_query(sql, conn))
                keys.update(rows["cluster_key"].dropna().astype(str).tolist())
    except Exception as exc:
        print("Warning: failed to read basin polygon keys from {}: {}".format(gpkg_path, exc))
    return keys


def attach_geography(cluster_df, world_boundaries=None):
    out = cluster_df.copy()
    country_col = first_col(out.columns, COUNTRY_COLS)
    iso_col = first_col(out.columns, ISO_COLS)
    continent_col = first_col(out.columns, CONTINENT_COLS)
    region_col = first_col(out.columns, REGION_COLS)
    out["country"] = out[country_col].map(clean_text) if country_col else ""
    out["iso_a3"] = out[iso_col].map(clean_text) if iso_col else ""
    out["continent"] = out[continent_col].map(clean_text) if continent_col else ""
    out["region"] = out[region_col].map(clean_text) if region_col else ""

    needs_join = (out["country"].eq("").all() or out["continent"].eq("").all()) and bool(world_boundaries)
    if needs_join and not HAS_GPD:
        print("Warning: geopandas unavailable; cannot join --world-boundaries")
    elif needs_join:
        boundary_path = Path(world_boundaries)
        if not boundary_path.is_file():
            print("Warning: world boundaries not found: {}".format(boundary_path))
        else:
            try:
                valid = out["valid_latlon"]
                pts = gpd.GeoDataFrame(
                    out.loc[valid].copy(),
                    geometry=[Point(float(x), float(y)) for x, y in zip(out.loc[valid, "lon"], out.loc[valid, "lat"])],
                    crs="EPSG:4326",
                )
                world = gpd.read_file(boundary_path)
                world = world.set_crs("EPSG:4326") if world.crs is None else world.to_crs("EPSG:4326")
                w_country = first_col(world.columns, COUNTRY_COLS)
                w_iso = first_col(world.columns, ISO_COLS)
                w_continent = first_col(world.columns, CONTINENT_COLS)
                w_region = first_col(world.columns, REGION_COLS)
                keep = [c for c in [w_country, w_iso, w_continent, w_region, "geometry"] if c is not None and c in world.columns]
                joined = gpd.sjoin(pts, world[keep], how="left", predicate="within")
                if w_country and out["country"].eq("").all():
                    out.loc[joined.index, "country"] = joined[w_country].map(clean_text)
                if w_iso and out["iso_a3"].eq("").all():
                    out.loc[joined.index, "iso_a3"] = joined[w_iso].map(clean_text)
                if w_continent and out["continent"].eq("").all():
                    out.loc[joined.index, "continent"] = joined[w_continent].map(clean_text)
                if w_region and out["region"].eq("").all():
                    out.loc[joined.index, "region"] = joined[w_region].map(clean_text)
            except Exception as exc:
                print("Warning: failed to join world boundaries: {}".format(exc))
    elif not (country_col or continent_col or region_col):
        print("Warning: no country/region columns and no --world-boundaries; using Unknown groups")

    out["country"] = out["country"].map(clean_text).replace("", "Unknown")
    out["iso_a3"] = out["iso_a3"].map(clean_text).replace("", "UNK")
    out["continent"] = out["continent"].map(clean_text).replace("", "Unknown")
    out["region"] = out["region"].map(clean_text)
    out.loc[out["region"].eq(""), "region"] = out.loc[out["region"].eq(""), "continent"]
    out["region"] = out["region"].replace("", "Unknown")
    return out


def split_values(series):
    values = set()
    for item in series:
        for part in clean_text(item).replace(",", "|").split("|"):
            part = clean_text(part)
            if part:
                values.add(part)
    return values


def attach_sources(cluster_df, source_df):
    out = cluster_df.copy()
    out["source_names"] = out["sources_used"].map(clean_text) if "sources_used" in out.columns else ""
    if source_df.empty or "source_name" not in source_df.columns:
        out["n_sources"] = out["source_names"].map(lambda x: len(split_values(pd.Series([x]))))
        return out
    src = add_cluster_key(source_df)
    src["source_name"] = src["source_name"].map(clean_text)
    grouped = src[src["source_name"] != ""].groupby("cluster_key")["source_name"].agg(lambda x: "|".join(sorted(set(x)))).reset_index()
    grouped = grouped.rename(columns={"source_name": "source_names_from_catalog"})
    out = out.merge(grouped, on="cluster_key", how="left")
    out["source_names"] = out["source_names_from_catalog"].where(out["source_names_from_catalog"].fillna("").ne(""), out["source_names"])
    out = out.drop(columns=["source_names_from_catalog"])
    out["n_sources"] = out["source_names"].map(lambda x: len(split_values(pd.Series([x]))))
    return out


def group_summary(group):
    area = group["area_km2"]
    valid_area = area[np.isfinite(area) & (area > 0)]
    lat = numeric(group, "lat")
    lon = numeric(group, "lon")
    sources = split_values(group["source_names"]) if "source_names" in group.columns else set()
    return pd.Series({
        "cluster_count": int(group["cluster_key"].nunique()),
        "resolved_cluster_count": int(group.loc[group["is_resolved"], "cluster_key"].nunique()),
        "unresolved_cluster_count": int(group.loc[group["is_unresolved"], "cluster_key"].nunique()),
        "basin_polygon_cluster_count": int(group.loc[group["has_basin_polygon"], "cluster_key"].nunique()),
        "clusters_with_valid_upstream_area": int(len(valid_area)),
        "mean_upstream_area_km2": float(valid_area.mean()) if len(valid_area) else np.nan,
        "median_upstream_area_km2": float(valid_area.median()) if len(valid_area) else np.nan,
        "min_lat": float(lat.min()) if lat.notna().any() else np.nan,
        "max_lat": float(lat.max()) if lat.notna().any() else np.nan,
        "min_lon": float(lon.min()) if lon.notna().any() else np.nan,
        "max_lon": float(lon.max()) if lon.notna().any() else np.nan,
        "n_sources": int(len(sources)),
        "source_names": "|".join(sorted(sources)),
    })


def summarise_global(df, area_col):
    area = df["area_km2"]
    valid = area[np.isfinite(area) & (area > 0)]
    lat = numeric(df, "lat")
    lon = numeric(df, "lon")
    rows = [
        ("final_cluster_count", df["cluster_key"].nunique(), "clusters"),
        ("resolved_basin_assignment_cluster_count", df.loc[df["is_resolved"], "cluster_key"].nunique(), "clusters"),
        ("unresolved_cluster_count", df.loc[df["is_unresolved"], "cluster_key"].nunique(), "clusters"),
        ("basin_status_unknown_or_other_cluster_count", df.loc[~df["is_resolved"] & ~df["is_unresolved"], "cluster_key"].nunique(), "clusters"),
        ("basin_polygon_cluster_count", df.loc[df["has_basin_polygon"], "cluster_key"].nunique(), "clusters"),
        ("clusters_with_valid_lat_lon", df.loc[df["valid_latlon"], "cluster_key"].nunique(), "clusters"),
        ("latitude_min", lat.min() if lat.notna().any() else np.nan, "degrees_north"),
        ("latitude_max", lat.max() if lat.notna().any() else np.nan, "degrees_north"),
        ("longitude_min", lon.min() if lon.notna().any() else np.nan, "degrees_east"),
        ("longitude_max", lon.max() if lon.notna().any() else np.nan, "degrees_east"),
        ("upstream_area_valid_cluster_count", len(valid), "clusters"),
        ("upstream_area_missing_or_invalid_cluster_count", df["cluster_key"].nunique() - len(valid), "clusters"),
    ]
    for name, func in [("min", np.min), ("mean", np.mean), ("median", np.median), ("max", np.max)]:
        rows.append(("upstream_area_{}".format(name), func(valid) if len(valid) else np.nan, "km2"))
    for q in [0.05, 0.25, 0.75, 0.95]:
        rows.append(("upstream_area_p{:02d}".format(int(q * 100)), valid.quantile(q) if len(valid) else np.nan, "km2"))
    out = pd.DataFrame(rows, columns=["metric", "value", "units"])
    out["area_column"] = area_col
    return out


def upstream_distribution(df, area_col):
    valid = df["area_km2"][np.isfinite(df["area_km2"]) & (df["area_km2"] > 0)]
    rows = []
    stats = {
        "valid_cluster_count": len(valid),
        "missing_or_invalid_cluster_count": df["cluster_key"].nunique() - len(valid),
        "min": valid.min() if len(valid) else np.nan,
        "p05": valid.quantile(0.05) if len(valid) else np.nan,
        "p25": valid.quantile(0.25) if len(valid) else np.nan,
        "mean": valid.mean() if len(valid) else np.nan,
        "median": valid.median() if len(valid) else np.nan,
        "p75": valid.quantile(0.75) if len(valid) else np.nan,
        "p95": valid.quantile(0.95) if len(valid) else np.nan,
        "max": valid.max() if len(valid) else np.nan,
    }
    count_stats = {"valid_cluster_count", "missing_or_invalid_cluster_count"}
    for label, value in stats.items():
        rows.append({
            "section": "summary",
            "label": label,
            "value_km2": np.nan if label in count_stats else value,
            "cluster_count": value if label in count_stats else np.nan,
            "fraction_of_valid_area_clusters": np.nan,
            "area_column": area_col,
        })
    cuts = pd.cut(valid, AREA_BINS, labels=AREA_LABELS, right=False, include_lowest=True) if len(valid) else pd.Series(dtype="category")
    counts = cuts.value_counts(sort=False) if len(valid) else {}
    for label in AREA_LABELS:
        count = int(counts.get(label, 0)) if len(valid) else 0
        rows.append({
            "section": "bin",
            "label": label,
            "value_km2": np.nan,
            "cluster_count": count,
            "fraction_of_valid_area_clusters": count / len(valid) if len(valid) else np.nan,
            "area_column": area_col,
        })
    return pd.DataFrame(rows)


def source_type_summary(source_df):
    if source_df.empty:
        return pd.DataFrame()
    src = add_cluster_key(source_df)
    source_col = first_col(src.columns, ["source_type", "source_family", "source_name", "source_long_name", "institution"])
    if source_col is None:
        src["source_type"] = "Unknown"
        source_col = "source_type"
    src["source_type"] = src[source_col].map(clean_text).replace("", "Unknown")
    if "source_station_uid" not in src.columns:
        src["source_station_uid"] = ""
    if "resolution" not in src.columns:
        src["resolution"] = ""
    if "n_records" not in src.columns:
        src["n_records"] = 0
    src["n_records"] = pd.to_numeric(src["n_records"], errors="coerce").fillna(0)
    lat_col = "source_station_lat" if "source_station_lat" in src.columns else "lat"
    lon_col = "source_station_lon" if "source_station_lon" in src.columns else "lon"
    src["lat_num"] = numeric(src, lat_col)
    src["lon_num"] = numeric(src, lon_col)

    def one(group):
        return pd.Series({
            "source_type_source_column": source_col,
            "cluster_count": int(group["cluster_key"].nunique()),
            "source_station_count": int(group["source_station_uid"].nunique()),
            "source_station_resolution_rows": int(len(group)),
            "record_count": int(group["n_records"].sum()),
            "available_resolutions": "|".join(sorted(set(clean_text(x) for x in group["resolution"] if clean_text(x)))),
            "min_lat": group["lat_num"].min() if group["lat_num"].notna().any() else np.nan,
            "max_lat": group["lat_num"].max() if group["lat_num"].notna().any() else np.nan,
            "min_lon": group["lon_num"].min() if group["lon_num"].notna().any() else np.nan,
            "max_lon": group["lon_num"].max() if group["lon_num"].notna().any() else np.nan,
        })
    return src.groupby("source_type", dropna=False).apply(one).reset_index().sort_values(["cluster_count", "source_type"], ascending=[False, True])


def json_safe(value):
    if value is None:
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return str(value)


def write_geojson(df, path):
    props = ["cluster_uid", "cluster_id", "country", "iso_a3", "continent", "region", "basin_status", "basin_flag", "area_km2", "has_basin_polygon", "available_resolutions", "n_sources", "source_names"]
    features = []
    for _, row in df[df["valid_latlon"]].iterrows():
        properties = {"cluster_key": json_safe(row.get("cluster_key"))}
        properties.update({col: json_safe(row.get(col)) for col in props if col in df.columns})
        features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [float(row["lon"]), float(row["lat"])]}, "properties": properties})
    with Path(path).open("w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False)


def plot_area_histogram(df, path):
    if not HAS_MPL:
        print("Warning: matplotlib unavailable; skipping histogram")
        return
    valid = df["area_km2"][np.isfinite(df["area_km2"]) & (df["area_km2"] > 0)]
    if len(valid) == 0:
        print("Warning: no valid upstream area values; skipping histogram")
        return
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(np.log10(valid), bins=40)
    ticks = [1, 10, 100, 1000, 10000, 100000, 1000000]
    ax.set_xticks([math.log10(x) for x in ticks])
    ax.set_xticklabels(["{:g}".format(x) for x in ticks])
    for boundary in [10, 100, 1000, 10000, 100000]:
        ax.axvline(math.log10(boundary), linestyle="--", linewidth=0.8)
    ax.set_xlabel("Upstream area (km², log scale)")
    ax.set_ylabel("Cluster count")
    ax.set_title("Distribution of upstream basin area")
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def plot_global_map(df, path, world_boundaries=None):
    if not HAS_MPL:
        print("Warning: matplotlib unavailable; skipping global map")
        return
    valid = df[df["valid_latlon"]]
    if len(valid) == 0:
        print("Warning: no valid cluster coordinates; skipping global map")
        return
    fig, ax = plt.subplots(figsize=(11, 5.5))
    if world_boundaries and HAS_GPD and Path(world_boundaries).is_file():
        try:
            world = gpd.read_file(world_boundaries)
            world = world.set_crs("EPSG:4326") if world.crs is None else world.to_crs("EPSG:4326")
            world.boundary.plot(ax=ax, linewidth=0.3)
        except Exception as exc:
            print("Warning: failed to draw world boundaries: {}".format(exc))
    unresolved = valid[~valid["is_resolved"]]
    resolved = valid[valid["is_resolved"]]
    if len(unresolved):
        ax.scatter(unresolved["lon"], unresolved["lat"], s=6, alpha=0.45, label="unresolved/other")
    if len(resolved):
        ax.scatter(resolved["lon"], resolved["lat"], s=6, alpha=0.55, label="resolved")
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Global distribution of final clusters")
    ax.grid(True, linewidth=0.3, alpha=0.35)
    ax.legend(loc="lower left", markerscale=2)
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate spatial coverage statistics and figures.")
    parser.add_argument("--cluster-catalog", default=str(DEFAULT_CLUSTER_CATALOG))
    parser.add_argument("--source-catalog", default=str(DEFAULT_SOURCE_CATALOG))
    parser.add_argument("--basin-gpkg", default=str(DEFAULT_BASIN_GPKG))
    parser.add_argument("--tables-dir", default=str(DEFAULT_TABLES_DIR))
    parser.add_argument("--figures-dir", default=str(DEFAULT_FIGURES_DIR))
    parser.add_argument("--area-column", default="basin_area")
    parser.add_argument("--world-boundaries", default="", help="Optional country/continent polygon file for spatial joins")
    parser.add_argument("--basin-layer-prefix", default="basin")
    parser.add_argument("--skip-figures", action="store_true")
    parser.add_argument("--skip-map", action="store_true")
    parser.add_argument("--no-geojson", action="store_true")
    args = parser.parse_args()

    tables_dir = Path(args.tables_dir).resolve()
    figures_dir = Path(args.figures_dir).resolve()
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    source_df = read_csv(args.source_catalog, required=False)
    clusters, area_col = normalise_clusters(read_csv(args.cluster_catalog, required=True), args.area_column)
    polygon_keys = basin_polygon_keys(args.basin_gpkg, args.basin_layer_prefix)
    clusters["has_basin_polygon"] = clusters["cluster_key"].isin(polygon_keys)
    clusters = attach_geography(clusters, args.world_boundaries.strip() or None)
    clusters = attach_sources(clusters, source_df)

    summary = summarise_global(clusters, area_col)
    by_region = clusters.groupby(["continent", "region"], dropna=False).apply(group_summary).reset_index().sort_values("cluster_count", ascending=False)
    by_country = clusters.groupby(["continent", "region", "country", "iso_a3"], dropna=False).apply(group_summary).reset_index().sort_values("cluster_count", ascending=False)
    area_dist = upstream_distribution(clusters, area_col)
    by_source = source_type_summary(source_df)

    summary.to_csv(tables_dir / "table_spatial_coverage_summary.csv", index=False)
    by_region.to_csv(tables_dir / "table_spatial_coverage_by_region.csv", index=False)
    by_country.to_csv(tables_dir / "table_spatial_coverage_by_country.csv", index=False)
    area_dist.to_csv(tables_dir / "table_upstream_area_distribution.csv", index=False)
    by_source.to_csv(tables_dir / "table_spatial_coverage_by_source_type.csv", index=False)

    keep = ["cluster_key", "cluster_uid", "cluster_id", "lat", "lon", "country", "iso_a3", "continent", "region", "basin_status", "basin_flag", "area_km2", "has_basin_polygon", "available_resolutions", "n_sources", "source_names"]
    clusters[[c for c in keep if c in clusters.columns]].to_csv(tables_dir / "table_cluster_spatial_attributes.csv", index=False)
    if not args.no_geojson:
        write_geojson(clusters, figures_dir / "global_cluster_distribution_points.geojson")
    if not args.skip_figures:
        plot_area_histogram(clusters, figures_dir / "fig_upstream_area_histogram.png")
        if not args.skip_map:
            plot_global_map(clusters, figures_dir / "fig_global_cluster_distribution.png", args.world_boundaries.strip() or None)

    print("Wrote spatial coverage outputs to {} and {}".format(tables_dir, figures_dir))
    print("Area column used: {}".format(area_col))
    print("Final cluster count: {}".format(int(clusters["cluster_key"].nunique())))
    print("Resolved basin assignment clusters: {}".format(int(clusters.loc[clusters["is_resolved"], "cluster_key"].nunique())))
    print("Unresolved clusters: {}".format(int(clusters.loc[clusters["is_unresolved"], "cluster_key"].nunique())))
    print("Clusters with basin polygons: {}".format(int(clusters.loc[clusters["has_basin_polygon"], "cluster_key"].nunique())))
    return 0


if __name__ == "__main__":
    sys.exit(main())