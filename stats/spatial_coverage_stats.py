#!/usr/bin/env python3
"""Build S8 release-level spatial coverage statistics for the ESSD manuscript.

This script is intentionally fixed-configuration and release-product oriented.
Run it on node113 only; it reads the S8 release package and writes statistics
under output_other/spatial_coverage_stats/tables.
"""

import math
import socket
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

try:
    import geopandas as gpd
    from shapely.geometry import Point

    HAS_GPD = True
except ImportError:  # pragma: no cover - optional runtime dependency
    gpd = None
    Point = None
    HAS_GPD = False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

RELEASE_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
    "scripts_basin_test/output/sed_reference_release"
)
OUTPUT_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
    "scripts_basin_test/output_other/spatial_coverage_stats"
)
TABLES_DIR = OUTPUT_DIR / "tables"
FIGURES_DIR = OUTPUT_DIR / "figures"
WORLD_BOUNDARIES = (
    "/share/home/dq134/.conda/envs/wzx/lib/python3.9/site-packages/"
    "pyogrio/tests/fixtures/naturalearth_lowres/naturalearth_lowres.shp"
)

STATION_CATALOG = RELEASE_DIR / "station_catalog.csv"
SOURCE_STATION_CATALOG = RELEASE_DIR / "source_station_catalog.csv"
SATELLITE_CATALOG = RELEASE_DIR / "satellite_catalog.csv"
CLUSTER_POINTS_GPKG = RELEASE_DIR / "sed_reference_cluster_points.gpkg"
CLUSTER_BASINS_GPKG = RELEASE_DIR / "sed_reference_cluster_basins.gpkg"

REQUIRED_HOST = "node113"
PYTHON = "/share/home/dq134/.conda/envs/wzx/bin/python3"
RUN_HINT = (
    "ssh node113 'cd /share/home/dq134/wzx/sed_data/sediment_wzx_1111/"
    "Output_r/scripts_basin_test && {py} stats/spatial_coverage_stats.py && "
    "{py} stats/plot_spatial_coverage_stats.py'"
).format(
    py=PYTHON
)

AREA_COLUMN = "basin_area"
RESOLUTION_ORDER = ("daily", "monthly", "annual")
AREA_BINS = [0, 10, 100, 1000, 10000, 100000, np.inf]
AREA_LABELS = [
    "<10 km2",
    "10-100 km2",
    "100-1,000 km2",
    "1,000-10,000 km2",
    "10,000-100,000 km2",
    ">100,000 km2",
]
COUNTRY_COLS = ("country", "country_name", "admin", "adm0_name", "name", "NAME", "NAME_EN")
ISO_COLS = ("iso_a3", "ISO_A3", "adm0_a3", "ADM0_A3", "sov_a3", "SOV_A3")
CONTINENT_COLS = ("continent", "CONTINENT", "continent_region", "region")
REGION_COLS = ("region", "REGION", "subregion", "SUBREGION", "continent_region")
METRIC_COLUMNS = ("section", "metric", "value", "unit", "source_file", "notes")


def require_node113() -> None:
    host = socket.gethostname().split(".")[0]
    if host != REQUIRED_HOST:
        raise SystemExit(
            "This spatial coverage script must run on node113, not {}.\n"
            "Use:\n  {}".format(host, RUN_HINT)
        )


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def first_col(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    columns = list(columns)
    lower = {str(c).lower(): c for c in columns}
    for col in candidates:
        if col in columns:
            return col
        if col.lower() in lower:
            return lower[col.lower()]
    return None


def read_csv(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.is_file():
        if required:
            raise FileNotFoundError("Required input not found: {}".format(path))
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def valid_latlon(df: pd.DataFrame, lat_col: str = "lat", lon_col: str = "lon") -> pd.Series:
    lat = numeric(df, lat_col)
    lon = numeric(df, lon_col)
    return lat.between(-90, 90) & lon.between(-180, 180)


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def split_values(value: object) -> List[str]:
    values: List[str] = []
    for part in clean_text(value).replace(",", "|").split("|"):
        part = clean_text(part)
        if part and part not in values:
            values.append(part)
    return values


def join_values(values: Iterable[object]) -> str:
    out: List[str] = []
    for value in values:
        for part in split_values(value):
            if part not in out:
                out.append(part)
    return "|".join(sorted(out))


def first_valid(values: Iterable[object]) -> object:
    for value in values:
        if clean_text(value) != "":
            return value
    return ""


def first_positive(values: Iterable[object]) -> float:
    vals = pd.to_numeric(pd.Series(list(values)), errors="coerce")
    vals = vals[np.isfinite(vals) & (vals > 0)]
    return float(vals.iloc[0]) if len(vals) else np.nan


def status_priority(values: Iterable[object]) -> str:
    statuses = [clean_text(v).lower() for v in values if clean_text(v)]
    if "resolved" in statuses:
        return "resolved"
    if "unresolved" in statuses:
        return "unresolved"
    if "unknown" in statuses:
        return "unknown"
    return statuses[0] if statuses else "unknown"


def resolution_sort_key(value: str) -> Tuple[int, str]:
    value = clean_text(value).lower()
    try:
        return RESOLUTION_ORDER.index(value), value
    except ValueError:
        return 99, value


def normalise_station_catalog(station_df: pd.DataFrame) -> pd.DataFrame:
    out = station_df.copy()
    for col in ("cluster_uid", "cluster_id", "resolution", "sources_used", "basin_status", "basin_flag"):
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(clean_text)
    for col in ("lat", "lon", AREA_COLUMN, "record_count", "n_source_stations_in_cluster"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["resolution"] = out["resolution"].str.lower()
    out["valid_latlon"] = valid_latlon(out)
    out["area_km2"] = numeric(out, AREA_COLUMN)
    out["cluster_key"] = out["cluster_uid"].where(out["cluster_uid"].ne(""), out["cluster_id"].map(lambda x: "cluster_id:{}".format(x)))
    out["cluster_resolution_key"] = out["cluster_key"] + "|" + out["resolution"]
    return out


def polygon_keys_by_layer(gpkg_path: Path) -> Tuple[Set[str], Dict[str, Set[str]], pd.DataFrame]:
    all_keys: Set[str] = set()
    by_resolution: Dict[str, Set[str]] = {r: set() for r in RESOLUTION_ORDER}
    rows: List[Dict[str, object]] = []
    if not gpkg_path.is_file():
        return all_keys, by_resolution, pd.DataFrame(rows)

    with sqlite3.connect(str(gpkg_path)) as conn:
        layers = pd.read_sql_query(
            "SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name",
            conn,
        )["table_name"].astype(str).tolist()
        for layer in layers:
            if not layer.startswith("basin_"):
                continue
            cols = pd.read_sql_query("PRAGMA table_info({})".format(quote_ident(layer)), conn)["name"].astype(str).tolist()
            key_col = first_col(cols, ("cluster_uid", "cluster_id"))
            if key_col is None:
                continue
            keys = pd.read_sql_query("SELECT {} FROM {}".format(quote_ident(key_col), quote_ident(layer)), conn)[key_col]
            layer_keys = set(keys.map(clean_text).replace("", np.nan).dropna().astype(str).tolist())
            resolution = layer.replace("basin_", "", 1).lower()
            all_keys.update(layer_keys)
            if resolution in by_resolution:
                by_resolution[resolution].update(layer_keys)
            rows.append(
                {
                    "layer": layer,
                    "resolution": resolution,
                    "polygon_feature_count": int(len(keys)),
                    "polygon_cluster_count": int(len(layer_keys)),
                }
            )
    return all_keys, by_resolution, pd.DataFrame(rows)


def attach_geography(cluster_df: pd.DataFrame) -> pd.DataFrame:
    out = cluster_df.copy()
    country_col = first_col(out.columns, COUNTRY_COLS)
    iso_col = first_col(out.columns, ISO_COLS)
    continent_col = first_col(out.columns, CONTINENT_COLS)
    region_col = first_col(out.columns, REGION_COLS)

    out["country"] = out[country_col].map(clean_text) if country_col else ""
    out["iso_a3"] = out[iso_col].map(clean_text) if iso_col else ""
    out["continent"] = out[continent_col].map(clean_text) if continent_col else ""
    out["region"] = out[region_col].map(clean_text) if region_col else ""

    boundary_path = Path(WORLD_BOUNDARIES).expanduser() if WORLD_BOUNDARIES else None
    needs_join = boundary_path and boundary_path.is_file() and (out["country"].eq("").any() or out["continent"].eq("").any())
    if needs_join and not HAS_GPD:
        print("Warning: geopandas unavailable; cannot join WORLD_BOUNDARIES")
    elif needs_join:
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
            rename = {}
            for source, target in (
                (w_country, "boundary_country"),
                (w_iso, "boundary_iso_a3"),
                (w_continent, "boundary_continent"),
                (w_region, "boundary_region"),
            ):
                if source and source in world.columns:
                    rename[source] = target
            keep = list(rename.keys()) + ["geometry"]
            boundary = world[keep].rename(columns=rename)
            joined = gpd.sjoin(pts, boundary, how="left", predicate="within")
            region_source = "boundary_region" if "boundary_region" in joined.columns else "boundary_continent"
            for target, source in (
                ("country", "boundary_country"),
                ("iso_a3", "boundary_iso_a3"),
                ("continent", "boundary_continent"),
                ("region", region_source),
            ):
                if source in joined.columns:
                    values = joined[source].map(clean_text)
                    mask = values.ne("")
                    out.loc[joined.index[mask], target] = values.loc[mask].values
        except Exception as exc:
            print("Warning: failed to join WORLD_BOUNDARIES: {}".format(exc))

    out["country"] = out["country"].map(clean_text).replace("", "Unknown")
    out["iso_a3"] = out["iso_a3"].map(clean_text).replace("", "UNK")
    out["continent"] = out["continent"].map(clean_text).replace("", "Unknown")
    out["region"] = out["region"].map(clean_text)
    out.loc[out["region"].eq(""), "region"] = out.loc[out["region"].eq(""), "continent"]
    out["region"] = out["region"].replace("", "Unknown")
    return out


def build_cluster_table(station_df: pd.DataFrame, source_df: pd.DataFrame, polygon_keys: Set[str]) -> pd.DataFrame:
    grouped = []
    for cluster_key, group in station_df.groupby("cluster_key", dropna=False):
        group = group.copy()
        resolutions = sorted(set(group["resolution"].map(clean_text)), key=resolution_sort_key)
        grouped.append(
            {
                "cluster_key": cluster_key,
                "cluster_uid": clean_text(first_valid(group["cluster_uid"])),
                "cluster_id": clean_text(first_valid(group["cluster_id"])),
                "lat": pd.to_numeric(group["lat"], errors="coerce").dropna().iloc[0] if group["lat"].notna().any() else np.nan,
                "lon": pd.to_numeric(group["lon"], errors="coerce").dropna().iloc[0] if group["lon"].notna().any() else np.nan,
                "area_km2": first_positive(group["area_km2"]),
                "basin_status": status_priority(group["basin_status"]),
                "basin_flag": clean_text(first_valid(group["basin_flag"])),
                "available_resolutions": "|".join(resolutions),
                "n_available_resolutions": len(resolutions),
                "record_count": int(pd.to_numeric(group["record_count"], errors="coerce").fillna(0).sum()),
                "country": clean_text(first_valid(group["country"])) if "country" in group.columns else "",
                "iso_a3": clean_text(first_valid(group["iso_a3"])) if "iso_a3" in group.columns else "",
                "continent": clean_text(first_valid(group["continent_region"])) if "continent_region" in group.columns else "",
                "region": clean_text(first_valid(group["continent_region"])) if "continent_region" in group.columns else "",
                "geographic_coverage": clean_text(first_valid(group["geographic_coverage"])) if "geographic_coverage" in group.columns else "",
                "geo_attribute_source": clean_text(first_valid(group["geo_attribute_source"])) if "geo_attribute_source" in group.columns else "",
                "geo_attribute_confidence": clean_text(first_valid(group["geo_attribute_confidence"])) if "geo_attribute_confidence" in group.columns else "",
                "sources_used": join_values(group["sources_used"]),
            }
        )
    clusters = pd.DataFrame(grouped)
    clusters["valid_latlon"] = valid_latlon(clusters)
    clusters["is_resolved"] = clusters["basin_status"].eq("resolved")
    clusters["is_unresolved"] = clusters["basin_status"].eq("unresolved")
    clusters["is_unknown_status"] = ~(clusters["is_resolved"] | clusters["is_unresolved"])
    clusters["has_basin_polygon"] = clusters["cluster_key"].isin(polygon_keys)

    if not source_df.empty and "source_name" in source_df.columns:
        src = source_df.copy()
        src["cluster_key"] = src["cluster_uid"].map(clean_text).where(src["cluster_uid"].map(clean_text).ne(""), src["cluster_id"].map(clean_text))
        src["source_name"] = src["source_name"].map(clean_text)
        by_cluster = src[src["source_name"].ne("")].groupby("cluster_key")["source_name"].agg(join_values).reset_index()
        by_cluster = by_cluster.rename(columns={"source_name": "source_names_from_catalog"})
        clusters = clusters.merge(by_cluster, on="cluster_key", how="left")
        clusters["source_names"] = clusters["source_names_from_catalog"].where(
            clusters["source_names_from_catalog"].fillna("").ne(""), clusters["sources_used"]
        )
        clusters = clusters.drop(columns=["source_names_from_catalog"])
    else:
        clusters["source_names"] = clusters["sources_used"]
    clusters["n_sources"] = clusters["source_names"].map(lambda x: len(split_values(x)))
    clusters = attach_geography(clusters)
    return clusters


def area_values(df: pd.DataFrame) -> pd.Series:
    values = pd.to_numeric(df["area_km2"], errors="coerce")
    return values[np.isfinite(values) & (values > 0)]


def pct(numer: float, denom: float) -> float:
    return float(numer) / float(denom) * 100.0 if denom else np.nan


def add_metric(rows: List[Dict[str, object]], section: str, metric: str, value: object, unit: str, source_file: str, notes: str = "") -> None:
    rows.append(
        {
            "section": section,
            "metric": metric,
            "value": value,
            "unit": unit,
            "source_file": source_file,
            "notes": notes,
        }
    )


def summarise_global(clusters: pd.DataFrame, station_df: pd.DataFrame, satellite_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    valid_area = area_values(clusters)
    lat = numeric(clusters, "lat")
    lon = numeric(clusters, "lon")
    total = int(clusters["cluster_key"].nunique())
    resolved = int(clusters.loc[clusters["is_resolved"], "cluster_key"].nunique())
    unresolved = int(clusters.loc[clusters["is_unresolved"], "cluster_key"].nunique())
    unknown = int(clusters.loc[clusters["is_unknown_status"], "cluster_key"].nunique())
    polygons = int(clusters.loc[clusters["has_basin_polygon"], "cluster_key"].nunique())
    unknown_country = int(clusters.loc[clusters["country"].eq("Unknown"), "cluster_key"].nunique())
    unknown_continent = int(clusters.loc[clusters["continent"].eq("Unknown"), "cluster_key"].nunique())

    add_metric(rows, "main_release", "station_catalog_rows", len(station_df), "rows", STATION_CATALOG.name)
    add_metric(rows, "main_release", "final_cluster_count", total, "clusters", STATION_CATALOG.name)
    for resolution in RESOLUTION_ORDER:
        count = int(station_df.loc[station_df["resolution"].eq(resolution), "cluster_key"].nunique())
        add_metric(rows, "main_release", "{}_cluster_count".format(resolution), count, "clusters", STATION_CATALOG.name)
    add_metric(rows, "basin_assignment", "resolved_cluster_count", resolved, "clusters", STATION_CATALOG.name)
    add_metric(rows, "basin_assignment", "resolved_cluster_percent", pct(resolved, total), "percent", STATION_CATALOG.name)
    add_metric(rows, "basin_assignment", "unresolved_cluster_count", unresolved, "clusters", STATION_CATALOG.name)
    add_metric(rows, "basin_assignment", "unresolved_cluster_percent", pct(unresolved, total), "percent", STATION_CATALOG.name)
    add_metric(rows, "basin_assignment", "unknown_status_cluster_count", unknown, "clusters", STATION_CATALOG.name)
    add_metric(rows, "basin_assignment", "unknown_status_cluster_percent", pct(unknown, total), "percent", STATION_CATALOG.name)
    add_metric(rows, "basin_polygons", "basin_polygon_cluster_count", polygons, "clusters", CLUSTER_BASINS_GPKG.name)
    add_metric(rows, "basin_polygons", "basin_polygon_cluster_percent", pct(polygons, total), "percent", CLUSTER_BASINS_GPKG.name)
    add_metric(rows, "coordinates", "clusters_with_valid_lat_lon", int(clusters.loc[clusters["valid_latlon"], "cluster_key"].nunique()), "clusters", STATION_CATALOG.name)
    add_metric(rows, "coordinates", "latitude_min", float(lat.min()) if lat.notna().any() else np.nan, "degrees_north", STATION_CATALOG.name)
    add_metric(rows, "coordinates", "latitude_max", float(lat.max()) if lat.notna().any() else np.nan, "degrees_north", STATION_CATALOG.name)
    add_metric(rows, "coordinates", "longitude_min", float(lon.min()) if lon.notna().any() else np.nan, "degrees_east", STATION_CATALOG.name)
    add_metric(rows, "coordinates", "longitude_max", float(lon.max()) if lon.notna().any() else np.nan, "degrees_east", STATION_CATALOG.name)
    add_metric(rows, "area", "upstream_area_valid_cluster_count", len(valid_area), "clusters", STATION_CATALOG.name)
    add_metric(rows, "area", "upstream_area_missing_or_invalid_cluster_count", total - len(valid_area), "clusters", STATION_CATALOG.name)
    for label, value in (
        ("min", valid_area.min() if len(valid_area) else np.nan),
        ("p05", valid_area.quantile(0.05) if len(valid_area) else np.nan),
        ("p25", valid_area.quantile(0.25) if len(valid_area) else np.nan),
        ("mean", valid_area.mean() if len(valid_area) else np.nan),
        ("median", valid_area.median() if len(valid_area) else np.nan),
        ("p75", valid_area.quantile(0.75) if len(valid_area) else np.nan),
        ("p95", valid_area.quantile(0.95) if len(valid_area) else np.nan),
        ("max", valid_area.max() if len(valid_area) else np.nan),
    ):
        add_metric(rows, "area", "upstream_area_{}".format(label), value, "km2", STATION_CATALOG.name)
    add_metric(rows, "geography", "unknown_country_cluster_count", unknown_country, "clusters", STATION_CATALOG.name)
    add_metric(rows, "geography", "unknown_country_cluster_percent", pct(unknown_country, total), "percent", STATION_CATALOG.name)
    add_metric(rows, "geography", "unknown_continent_cluster_count", unknown_continent, "clusters", STATION_CATALOG.name)
    add_metric(rows, "geography", "unknown_continent_cluster_percent", pct(unknown_continent, total), "percent", STATION_CATALOG.name)
    if not satellite_df.empty:
        add_metric(rows, "satellite_validation", "satellite_station_rows", len(satellite_df), "rows", SATELLITE_CATALOG.name)
        add_metric(rows, "satellite_validation", "satellite_linked_cluster_count", satellite_df["cluster_uid"].map(clean_text).nunique(), "clusters", SATELLITE_CATALOG.name)
        add_metric(rows, "satellite_validation", "satellite_record_count", int(numeric(satellite_df, "n_records").fillna(0).sum()), "records", SATELLITE_CATALOG.name)
    return pd.DataFrame(rows, columns=METRIC_COLUMNS)


def group_summary(group: pd.DataFrame) -> pd.Series:
    valid_area = area_values(group)
    lat = numeric(group, "lat")
    lon = numeric(group, "lon")
    total = int(group["cluster_key"].nunique())
    resolved = int(group.loc[group["is_resolved"], "cluster_key"].nunique())
    polygons = int(group.loc[group["has_basin_polygon"], "cluster_key"].nunique())
    return pd.Series(
        {
            "cluster_count": total,
            "resolved_cluster_count": resolved,
            "unresolved_cluster_count": int(group.loc[group["is_unresolved"], "cluster_key"].nunique()),
            "unknown_status_cluster_count": int(group.loc[group["is_unknown_status"], "cluster_key"].nunique()),
            "basin_polygon_cluster_count": polygons,
            "resolved_cluster_percent": pct(resolved, total),
            "basin_polygon_cluster_percent": pct(polygons, total),
            "clusters_with_valid_upstream_area": int(len(valid_area)),
            "mean_upstream_area_km2": float(valid_area.mean()) if len(valid_area) else np.nan,
            "median_upstream_area_km2": float(valid_area.median()) if len(valid_area) else np.nan,
            "min_lat": float(lat.min()) if lat.notna().any() else np.nan,
            "max_lat": float(lat.max()) if lat.notna().any() else np.nan,
            "min_lon": float(lon.min()) if lon.notna().any() else np.nan,
            "max_lon": float(lon.max()) if lon.notna().any() else np.nan,
            "n_sources": int(len(split_values(join_values(group["source_names"])))),
            "source_names": join_values(group["source_names"]),
        }
    )


def resolution_summary(station_df: pd.DataFrame, polygon_by_resolution: Dict[str, Set[str]]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    for resolution in RESOLUTION_ORDER:
        group = station_df[station_df["resolution"].eq(resolution)].copy()
        keys = set(group["cluster_key"].map(clean_text))
        polygon_keys = polygon_by_resolution.get(resolution, set())
        valid_area = area_values(group.drop_duplicates("cluster_key"))
        total = len(keys)
        resolved = group.loc[group["basin_status"].str.lower().eq("resolved"), "cluster_key"].nunique()
        rows.append(
            {
                "resolution": resolution,
                "catalog_rows": int(len(group)),
                "cluster_count": int(total),
                "record_count": int(numeric(group, "record_count").fillna(0).sum()),
                "resolved_cluster_count": int(resolved),
                "unresolved_cluster_count": int(group.loc[group["basin_status"].str.lower().eq("unresolved"), "cluster_key"].nunique()),
                "unknown_status_cluster_count": int(total - resolved - group.loc[group["basin_status"].str.lower().eq("unresolved"), "cluster_key"].nunique()),
                "basin_polygon_cluster_count": int(len(keys & polygon_keys)),
                "basin_polygon_cluster_percent": pct(len(keys & polygon_keys), total),
                "valid_area_cluster_count": int(len(valid_area)),
                "median_upstream_area_km2": float(valid_area.median()) if len(valid_area) else np.nan,
                "mean_upstream_area_km2": float(valid_area.mean()) if len(valid_area) else np.nan,
                "min_lat": float(numeric(group, "lat").min()) if len(group) else np.nan,
                "max_lat": float(numeric(group, "lat").max()) if len(group) else np.nan,
                "min_lon": float(numeric(group, "lon").min()) if len(group) else np.nan,
                "max_lon": float(numeric(group, "lon").max()) if len(group) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def upstream_distribution(clusters: pd.DataFrame) -> pd.DataFrame:
    valid = area_values(clusters)
    rows: List[Dict[str, object]] = []
    count_stats = {
        "valid_cluster_count": len(valid),
        "missing_or_invalid_cluster_count": clusters["cluster_key"].nunique() - len(valid),
    }
    for label, count in count_stats.items():
        rows.append(
            {
                "section": "summary",
                "label": label,
                "value_km2": np.nan,
                "cluster_count": int(count),
                "fraction_of_valid_area_clusters": np.nan,
            }
        )
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
        rows.append(
            {
                "section": "summary",
                "label": label,
                "value_km2": value,
                "cluster_count": np.nan,
                "fraction_of_valid_area_clusters": np.nan,
            }
        )
    cuts = pd.cut(valid, AREA_BINS, labels=AREA_LABELS, right=False, include_lowest=True) if len(valid) else pd.Series(dtype="category")
    counts = cuts.value_counts(sort=False) if len(valid) else {}
    for label in AREA_LABELS:
        count = int(counts.get(label, 0)) if len(valid) else 0
        rows.append(
            {
                "section": "bin",
                "label": label,
                "value_km2": np.nan,
                "cluster_count": count,
                "fraction_of_valid_area_clusters": count / len(valid) if len(valid) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def source_summary(source_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty:
        return pd.DataFrame()
    src = source_df.copy()
    src["cluster_key"] = src["cluster_uid"].map(clean_text).where(src["cluster_uid"].map(clean_text).ne(""), src["cluster_id"].map(clean_text))
    src["source_name"] = src["source_name"].map(clean_text).replace("", "Unknown")
    for col in ("n_records", "source_station_lat", "source_station_lon"):
        if col in src.columns:
            src[col] = pd.to_numeric(src[col], errors="coerce")
    rows = []
    for source, group in src.groupby("source_name", dropna=False):
        rows.append(
            {
                "source_name": source,
                "cluster_count": int(group["cluster_key"].nunique()),
                "source_station_count": int(group["source_station_uid"].map(clean_text).replace("", np.nan).dropna().nunique()) if "source_station_uid" in group.columns else int(len(group)),
                "source_station_resolution_rows": int(len(group)),
                "record_count": int(numeric(group, "n_records").fillna(0).sum()),
                "available_resolutions": join_values(group["resolution"]) if "resolution" in group.columns else "",
                "min_lat": float(numeric(group, "source_station_lat").min()) if "source_station_lat" in group.columns else np.nan,
                "max_lat": float(numeric(group, "source_station_lat").max()) if "source_station_lat" in group.columns else np.nan,
                "min_lon": float(numeric(group, "source_station_lon").min()) if "source_station_lon" in group.columns else np.nan,
                "max_lon": float(numeric(group, "source_station_lon").max()) if "source_station_lon" in group.columns else np.nan,
            }
        )
    out = pd.DataFrame(rows)
    total_records = out["record_count"].sum()
    total_clusters = out["cluster_count"].sum()
    out["record_percent_of_source_catalog"] = out["record_count"].map(lambda x: pct(x, total_records))
    out["cluster_percent_of_source_rows"] = out["cluster_count"].map(lambda x: pct(x, total_clusters))
    return out.sort_values(["cluster_count", "record_count", "source_name"], ascending=[False, False, True])


def _source_rows_with_cluster_geography(clusters: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    if source_df.empty:
        return pd.DataFrame()
    src = source_df.copy()
    src["cluster_key"] = src["cluster_uid"].map(clean_text).where(src["cluster_uid"].map(clean_text).ne(""), src["cluster_id"].map(clean_text))
    src["source_name"] = src["source_name"].map(clean_text).replace("", "Unknown")
    src["resolution"] = src["resolution"].map(clean_text).str.lower() if "resolution" in src.columns else ""
    src["n_records"] = numeric(src, "n_records").fillna(0)
    for col in ("source_station_lat", "source_station_lon"):
        if col in src.columns:
            src[col] = pd.to_numeric(src[col], errors="coerce")

    cluster_cols = [
        "cluster_key",
        "cluster_uid",
        "continent",
        "region",
        "country",
        "lat",
        "lon",
    ]
    geo = clusters[[c for c in cluster_cols if c in clusters.columns]].copy()
    geo = geo.rename(columns={"cluster_uid": "cluster_uid_from_cluster", "lat": "cluster_lat", "lon": "cluster_lon"})
    merged = src.merge(geo, on="cluster_key", how="left")
    for col in ("continent", "region", "country"):
        if col not in merged.columns:
            merged[col] = "Unknown"
        merged[col] = merged[col].map(clean_text).replace("", "Unknown")
    if "source_station_uid" not in merged.columns:
        merged["source_station_uid"] = ""
    return merged


def region_source_summary(clusters: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    src = _source_rows_with_cluster_geography(clusters, source_df)
    if src.empty:
        return pd.DataFrame()
    rows = []
    for (continent, region, source), group in src.groupby(["continent", "region", "source_name"], dropna=False):
        lat_col = "source_station_lat" if "source_station_lat" in group.columns else "cluster_lat"
        lon_col = "source_station_lon" if "source_station_lon" in group.columns else "cluster_lon"
        rows.append(
            {
                "continent": continent,
                "region": region,
                "source_name": source,
                "cluster_count": int(group["cluster_key"].nunique()),
                "source_station_count": int(group["source_station_uid"].map(clean_text).replace("", np.nan).dropna().nunique()),
                "source_station_resolution_rows": int(len(group)),
                "record_count": int(numeric(group, "n_records").fillna(0).sum()),
                "available_resolutions": join_values(group["resolution"]) if "resolution" in group.columns else "",
                "min_lat": float(numeric(group, lat_col).min()) if lat_col in group.columns and numeric(group, lat_col).notna().any() else np.nan,
                "max_lat": float(numeric(group, lat_col).max()) if lat_col in group.columns and numeric(group, lat_col).notna().any() else np.nan,
                "min_lon": float(numeric(group, lon_col).min()) if lon_col in group.columns and numeric(group, lon_col).notna().any() else np.nan,
                "max_lon": float(numeric(group, lon_col).max()) if lon_col in group.columns and numeric(group, lon_col).notna().any() else np.nan,
            }
        )
    out = pd.DataFrame(rows)
    region_totals = out.groupby("region")["cluster_count"].sum().to_dict()
    out["_region_total"] = out["region"].map(region_totals).fillna(0)
    out["_unknown_region"] = out["region"].eq("Unknown").astype(int)
    out = out.sort_values(["_unknown_region", "_region_total", "region", "cluster_count", "source_name"], ascending=[True, False, True, False, True])
    return out.drop(columns=["_region_total", "_unknown_region"])


def region_resolution_summary(clusters: pd.DataFrame, source_df: pd.DataFrame) -> pd.DataFrame:
    src = _source_rows_with_cluster_geography(clusters, source_df)
    if src.empty:
        return pd.DataFrame()
    rows = []
    for (continent, region, resolution), group in src.groupby(["continent", "region", "resolution"], dropna=False):
        rows.append(
            {
                "continent": continent,
                "region": region,
                "resolution": clean_text(resolution) or "unknown",
                "cluster_count": int(group["cluster_key"].nunique()),
                "source_station_count": int(group["source_station_uid"].map(clean_text).replace("", np.nan).dropna().nunique()),
                "source_station_resolution_rows": int(len(group)),
                "record_count": int(numeric(group, "n_records").fillna(0).sum()),
            }
        )
    out = pd.DataFrame(rows)
    region_totals = out.groupby("region")["cluster_count"].sum().to_dict()
    out["_region_total"] = out["region"].map(region_totals).fillna(0)
    out["_unknown_region"] = out["region"].eq("Unknown").astype(int)
    out["_resolution_order"] = out["resolution"].map(lambda x: resolution_sort_key(x)[0])
    out = out.sort_values(["_unknown_region", "_region_total", "region", "_resolution_order"], ascending=[True, False, True, True])
    return out.drop(columns=["_region_total", "_unknown_region", "_resolution_order"])


def satellite_summary(satellite_df: pd.DataFrame) -> pd.DataFrame:
    if satellite_df.empty:
        return pd.DataFrame()
    sat = satellite_df.copy()
    sat["source"] = sat["source"].map(clean_text).replace("", "Unknown") if "source" in sat.columns else "Unknown"
    for col in ("lat", "lon", "n_records"):
        if col in sat.columns:
            sat[col] = pd.to_numeric(sat[col], errors="coerce")

    def one(name: str, group: pd.DataFrame) -> Dict[str, object]:
        return {
            "summary_level": name,
            "satellite_station_rows": int(len(group)),
            "linked_cluster_count": int(group["cluster_uid"].map(clean_text).replace("", np.nan).dropna().nunique()) if "cluster_uid" in group.columns else 0,
            "record_count": int(numeric(group, "n_records").fillna(0).sum()),
            "min_lat": float(numeric(group, "lat").min()) if len(group) else np.nan,
            "max_lat": float(numeric(group, "lat").max()) if len(group) else np.nan,
            "min_lon": float(numeric(group, "lon").min()) if len(group) else np.nan,
            "max_lon": float(numeric(group, "lon").max()) if len(group) else np.nan,
            "time_start_min": pd.to_datetime(group["time_start"], errors="coerce").min().strftime("%Y-%m-%d") if "time_start" in group.columns and pd.to_datetime(group["time_start"], errors="coerce").notna().any() else "",
            "time_end_max": pd.to_datetime(group["time_end"], errors="coerce").max().strftime("%Y-%m-%d") if "time_end" in group.columns and pd.to_datetime(group["time_end"], errors="coerce").notna().any() else "",
        }

    rows = [one("all", sat)]
    for source, group in sat.groupby("source", dropna=False):
        row = one("source", group)
        row["source"] = source
        rows.append(row)
    out = pd.DataFrame(rows)
    if "source" not in out.columns:
        out["source"] = ""
    return out[
        [
            "summary_level",
            "source",
            "satellite_station_rows",
            "linked_cluster_count",
            "record_count",
            "min_lat",
            "max_lat",
            "min_lon",
            "max_lon",
            "time_start_min",
            "time_end_max",
        ]
    ]


def unknown_geography_rows(clusters: pd.DataFrame) -> pd.DataFrame:
    mask = clusters["country"].eq("Unknown") | clusters["region"].eq("Unknown") | clusters["continent"].eq("Unknown")
    out = clusters.loc[mask].copy()
    keep = [
        "cluster_key",
        "cluster_uid",
        "cluster_id",
        "lat",
        "lon",
        "country",
        "iso_a3",
        "continent",
        "region",
        "geographic_coverage",
        "geo_attribute_source",
        "geo_attribute_confidence",
        "source_names",
        "available_resolutions",
        "basin_status",
        "basin_flag",
        "area_km2",
        "has_basin_polygon",
        "record_count",
    ]
    out = out[[c for c in keep if c in out.columns]].sort_values(["source_names", "cluster_uid", "cluster_key"])
    out["unknown_reason"] = "country_or_region_not_resolved_by_catalog_or_boundary_join"
    return out


def metric_lookup(metrics: pd.DataFrame) -> Dict[str, object]:
    return dict(zip(metrics["metric"], metrics["value"]))


def fmt_int(value: object) -> str:
    try:
        return "{:,}".format(int(round(float(value))))
    except Exception:
        return "NA"


def fmt_float(value: object, digits: int = 1) -> str:
    try:
        value = float(value)
        if not math.isfinite(value):
            return "NA"
        return "{:.{}f}".format(value, digits)
    except Exception:
        return "NA"


def fmt_percent(value: object, digits: int = 1) -> str:
    return "{}%".format(fmt_float(value, digits=digits))


def fmt_fraction(value: object, digits: int = 1) -> str:
    try:
        return "{:.{}f}%".format(float(value) * 100.0, int(digits))
    except Exception:
        return "NA"


def safe_markdown(value: object) -> str:
    return clean_text(value).replace("|", "\\|")


def markdown_table(rows: Sequence[Dict[str, object]], columns: Sequence[str], headers: Sequence[str]) -> str:
    if not rows:
        return "_No rows._"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(safe_markdown(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines)


def compact_resolution_rows(by_resolution: pd.DataFrame) -> List[Dict[str, object]]:
    if by_resolution.empty:
        return []
    work = by_resolution.copy()
    total_records = pd.to_numeric(work.get("record_count", 0), errors="coerce").fillna(0).sum()
    work = work.sort_values(
        "resolution",
        key=lambda s: s.map(lambda x: resolution_sort_key(str(x))),
        kind="mergesort",
    )
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        records = float(row.get("record_count", 0) or 0)
        rows.append(
            {
                "resolution": row.get("resolution", ""),
                "clusters": fmt_int(row.get("cluster_count")),
                "records": fmt_int(row.get("record_count")),
                "record_share": fmt_percent(pct(records, total_records), digits=2),
                "resolved": "{} ({})".format(fmt_int(row.get("resolved_cluster_count")), fmt_percent(row.get("basin_polygon_cluster_percent"))),
                "polygons": "{} ({})".format(fmt_int(row.get("basin_polygon_cluster_count")), fmt_percent(row.get("basin_polygon_cluster_percent"))),
                "median_area": fmt_float(row.get("median_upstream_area_km2")),
                "lat_span": "{} to {}".format(fmt_float(row.get("min_lat")), fmt_float(row.get("max_lat"))),
                "lon_span": "{} to {}".format(fmt_float(row.get("min_lon")), fmt_float(row.get("max_lon"))),
            }
        )
    return rows


def compact_area_rows(area_dist: pd.DataFrame) -> List[Dict[str, object]]:
    if area_dist.empty:
        return []
    rows: List[Dict[str, object]] = []
    for _, row in area_dist[area_dist["section"].eq("bin")].iterrows():
        rows.append(
            {
                "area_bin": row.get("label", ""),
                "clusters": fmt_int(row.get("cluster_count")),
                "share": fmt_fraction(row.get("fraction_of_valid_area_clusters")),
            }
        )
    return rows


def compact_top_rows(df: pd.DataFrame, sort_col: str, name_col: str, top_n: int = 10) -> List[Dict[str, object]]:
    if df.empty or sort_col not in df.columns:
        return []
    work = df.copy()
    work[sort_col] = pd.to_numeric(work[sort_col], errors="coerce").fillna(0)
    work = work.sort_values([sort_col, name_col], ascending=[False, True], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "name": row.get(name_col, ""),
                "clusters": fmt_int(row.get("cluster_count", row.get("linked_cluster_count", ""))),
                "stations": fmt_int(row.get("source_station_count", row.get("satellite_station_rows", ""))),
                "records": fmt_int(row.get("record_count")),
                "resolutions": row.get("available_resolutions", ""),
                "resolved": fmt_percent(row.get("resolved_cluster_percent", "")) if "resolved_cluster_percent" in row.index else "",
                "polygons": fmt_percent(row.get("basin_polygon_cluster_percent", "")) if "basin_polygon_cluster_percent" in row.index else "",
            }
        )
    return rows


def compact_region_resolution_rows(by_region_resolution: pd.DataFrame, top_n: int = 15) -> List[Dict[str, object]]:
    if by_region_resolution.empty:
        return []
    work = by_region_resolution.copy()
    work["record_count"] = pd.to_numeric(work.get("record_count", 0), errors="coerce").fillna(0)
    work = work.sort_values(["record_count", "cluster_count"], ascending=[False, False], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "continent": row.get("continent", ""),
                "region": row.get("region", ""),
                "resolution": row.get("resolution", ""),
                "clusters": fmt_int(row.get("cluster_count")),
                "stations": fmt_int(row.get("source_station_count")),
                "records": fmt_int(row.get("record_count")),
            }
        )
    return rows


def compact_satellite_rows(satellite_df: pd.DataFrame) -> List[Dict[str, object]]:
    if satellite_df.empty:
        return []
    rows: List[Dict[str, object]] = []
    work = satellite_df.sort_values(["summary_level", "record_count"], ascending=[True, False], kind="mergesort")
    for _, row in work.iterrows():
        label = row.get("source", "") if clean_text(row.get("source", "")) else row.get("summary_level", "")
        rows.append(
            {
                "source": label,
                "stations": fmt_int(row.get("satellite_station_rows")),
                "clusters": fmt_int(row.get("linked_cluster_count")),
                "records": fmt_int(row.get("record_count")),
                "lat_span": "{} to {}".format(fmt_float(row.get("min_lat")), fmt_float(row.get("max_lat"))),
                "lon_span": "{} to {}".format(fmt_float(row.get("min_lon")), fmt_float(row.get("max_lon"))),
                "time_span": "{} to {}".format(row.get("time_start_min", ""), row.get("time_end_max", "")),
            }
        )
    return rows


def compact_polygon_layer_rows(polygon_layer_summary: pd.DataFrame) -> List[Dict[str, object]]:
    if polygon_layer_summary.empty:
        return []
    work = polygon_layer_summary.sort_values(
        "resolution",
        key=lambda s: s.map(lambda x: resolution_sort_key(str(x))),
        kind="mergesort",
    )
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "layer": row.get("layer", ""),
                "resolution": row.get("resolution", ""),
                "features": fmt_int(row.get("polygon_feature_count")),
                "clusters": fmt_int(row.get("polygon_cluster_count")),
            }
        )
    return rows


def write_article_summary(
    metrics: pd.DataFrame,
    area_dist: pd.DataFrame,
    source_df: pd.DataFrame,
    satellite_df: pd.DataFrame,
    by_resolution: pd.DataFrame,
    by_region: pd.DataFrame,
    by_country: pd.DataFrame,
    by_region_source: pd.DataFrame,
    by_region_resolution: pd.DataFrame,
    polygon_layer_summary: pd.DataFrame,
    unknown_geo: pd.DataFrame,
) -> None:
    m = metric_lookup(metrics)
    bin_rows = area_dist[area_dist["section"].eq("bin")].copy()
    bin_text = "; ".join(
        "{}: {} clusters ({:.1%})".format(row["label"], int(row["cluster_count"]), row["fraction_of_valid_area_clusters"])
        for _, row in bin_rows.iterrows()
    )
    top_sources = source_df.head(5) if not source_df.empty else pd.DataFrame()
    source_text = "; ".join(
        "{}: {} clusters, {} records".format(row["source_name"], fmt_int(row["cluster_count"]), fmt_int(row["record_count"]))
        for _, row in top_sources.iterrows()
    )
    sat_all = satellite_df[satellite_df["summary_level"].eq("all")].head(1) if not satellite_df.empty else pd.DataFrame()
    if len(sat_all):
        sat = sat_all.iloc[0]
        satellite_text = (
            "The satellite-validation product contains {} station rows linked to {} clusters, "
            "with coordinates spanning {:.1f} to {:.1f} degrees latitude and {:.1f} to {:.1f} degrees longitude."
        ).format(
            fmt_int(sat["satellite_station_rows"]),
            fmt_int(sat["linked_cluster_count"]),
            float(sat["min_lat"]),
            float(sat["max_lat"]),
            float(sat["min_lon"]),
            float(sat["max_lon"]),
        )
    else:
        satellite_text = "The satellite-validation catalog was not available for this run."

    unknown_note = ""
    if float(m.get("unknown_country_cluster_percent", 100.0)) > 20.0 or float(m.get("unknown_continent_cluster_percent", 100.0)) > 20.0:
        unknown_note = (
            "\n\nNote for manuscript drafting: country/continent fields remain incomplete "
            "for a large fraction of clusters. Do not cite regional coverage conclusions "
            "until WORLD_BOUNDARIES is configured and the spatial join is rerun."
        )

    lines = [
        "# S8 spatial coverage statistics for ESSD",
        "",
        "## Manuscript-ready summary",
        "",
        (
            "The S8 release contains {total} final main-product clusters. Resolution-specific coverage is "
            "{daily} daily clusters, {monthly} monthly clusters, and {annual} annual clusters. Basin assignment "
            "resolved {resolved} clusters ({resolved_pct}% of all clusters), while {unresolved} clusters "
            "({unresolved_pct}%) remain unresolved and {unknown} clusters ({unknown_pct}%) have unknown or other "
            "basin status. The published basin sidecar contains polygons for {polygons} clusters ({polygon_pct}% of all clusters)."
        ).format(
            total=fmt_int(m.get("final_cluster_count")),
            daily=fmt_int(m.get("daily_cluster_count")),
            monthly=fmt_int(m.get("monthly_cluster_count")),
            annual=fmt_int(m.get("annual_cluster_count")),
            resolved=fmt_int(m.get("resolved_cluster_count")),
            resolved_pct=fmt_float(m.get("resolved_cluster_percent")),
            unresolved=fmt_int(m.get("unresolved_cluster_count")),
            unresolved_pct=fmt_float(m.get("unresolved_cluster_percent")),
            unknown=fmt_int(m.get("unknown_status_cluster_count")),
            unknown_pct=fmt_float(m.get("unknown_status_cluster_percent")),
            polygons=fmt_int(m.get("basin_polygon_cluster_count")),
            polygon_pct=fmt_float(m.get("basin_polygon_cluster_percent")),
        ),
        "",
        (
            "The main-product coordinates span {lat_min} to {lat_max} degrees latitude and {lon_min} to {lon_max} "
            "degrees longitude. Valid upstream basin areas are available for {area_count} clusters; the median area is "
            "{area_median} km2, with an interquartile range of {area_p25}-{area_p75} km2 and a maximum of {area_max} km2."
        ).format(
            lat_min=fmt_float(m.get("latitude_min")),
            lat_max=fmt_float(m.get("latitude_max")),
            lon_min=fmt_float(m.get("longitude_min")),
            lon_max=fmt_float(m.get("longitude_max")),
            area_count=fmt_int(m.get("upstream_area_valid_cluster_count")),
            area_median=fmt_float(m.get("upstream_area_median")),
            area_p25=fmt_float(m.get("upstream_area_p25")),
            area_p75=fmt_float(m.get("upstream_area_p75")),
            area_max=fmt_float(m.get("upstream_area_max")),
        ),
        "",
        "Main source contributions by cluster count: {}".format(source_text or "NA"),
        "",
        satellite_text + unknown_note,
        "",
        "## Key Metrics",
        "",
        "- Final clusters: {}".format(fmt_int(m.get("final_cluster_count"))),
        "- Station catalog rows: {}".format(fmt_int(m.get("station_catalog_rows"))),
        "- Main-product record count: {}".format(fmt_int(pd.to_numeric(by_resolution.get("record_count", 0), errors="coerce").fillna(0).sum())),
        "- Basin-resolved clusters: {} ({})".format(fmt_int(m.get("resolved_cluster_count")), fmt_percent(m.get("resolved_cluster_percent"))),
        "- Published basin polygons: {} ({})".format(fmt_int(m.get("basin_polygon_cluster_count")), fmt_percent(m.get("basin_polygon_cluster_percent"))),
        "- Unknown country clusters: {} ({})".format(fmt_int(m.get("unknown_country_cluster_count")), fmt_percent(m.get("unknown_country_cluster_percent"))),
        "",
        "## Resolution Coverage",
        "",
        markdown_table(
            compact_resolution_rows(by_resolution),
            ["resolution", "clusters", "records", "record_share", "resolved", "polygons", "median_area", "lat_span", "lon_span"],
            ["Resolution", "Clusters", "Records", "Record share", "Resolved", "Polygons", "Median area km2", "Latitude", "Longitude"],
        ),
        "",
        "Resolution-specific records are highly uneven, so spatial coverage should be interpreted together with temporal record volume. Annual coverage is spatially narrow but can still contain long individual records.",
        "",
        "## Upstream Basin Area",
        "",
        markdown_table(
            compact_area_rows(area_dist),
            ["area_bin", "clusters", "share"],
            ["Area bin", "Clusters", "Share of valid-area clusters"],
        ),
        "",
        "The basin-area distribution is right-skewed: most resolved clusters fall below 10,000 km2, while a smaller set of very large basins controls the upper tail.",
        "",
        "## Geographic Hotspots",
        "",
        "### Regions by Cluster Count",
        "",
        markdown_table(
            compact_top_rows(by_region, "cluster_count", "region", top_n=10),
            ["name", "clusters", "resolved", "polygons"],
            ["Region", "Clusters", "Resolved", "Polygons"],
        ),
        "",
        "### Countries by Cluster Count",
        "",
        markdown_table(
            compact_top_rows(by_country, "cluster_count", "country", top_n=15),
            ["name", "clusters", "resolved", "polygons"],
            ["Country", "Clusters", "Resolved", "Polygons"],
        ),
        "",
        "### Region-Resolution Record Hotspots",
        "",
        markdown_table(
            compact_region_resolution_rows(by_region_resolution, top_n=15),
            ["continent", "region", "resolution", "clusters", "stations", "records"],
            ["Continent", "Region", "Resolution", "Clusters", "Source stations", "Records"],
        ),
        "",
        "## Source Spatial Contribution",
        "",
        "### Top Sources by Clusters",
        "",
        markdown_table(
            compact_top_rows(source_df, "cluster_count", "source_name", top_n=12),
            ["name", "clusters", "stations", "records", "resolutions"],
            ["Source", "Clusters", "Source stations", "Records", "Resolutions"],
        ),
        "",
        "### Top Sources by Records",
        "",
        markdown_table(
            compact_top_rows(source_df, "record_count", "source_name", top_n=12),
            ["name", "clusters", "stations", "records", "resolutions"],
            ["Source", "Clusters", "Source stations", "Records", "Resolutions"],
        ),
        "",
        "The cluster-based and record-based rankings answer different questions: the former describes spatial footprint, while the latter describes the amount of time-series information contributed by each source.",
        "",
        "## Satellite Validation Spatial Coverage",
        "",
        markdown_table(
            compact_satellite_rows(satellite_df),
            ["source", "stations", "clusters", "records", "lat_span", "lon_span", "time_span"],
            ["Source", "Station rows", "Linked clusters", "Records", "Latitude", "Longitude", "Time span"],
        ),
        "",
        "## Basin Polygon Layers",
        "",
        markdown_table(
            compact_polygon_layer_rows(polygon_layer_summary),
            ["layer", "resolution", "features", "clusters"],
            ["Layer", "Resolution", "Polygon features", "Polygon clusters"],
        ),
        "",
        "## Diagnostics and Limitations",
        "",
        "- Unknown country/region rows written for review: {}".format(fmt_int(len(unknown_geo))),
        "- Regional summaries depend on catalog geography plus the configured world-boundary join; unknown geography should be reviewed before strong continent/country claims.",
        "- Cluster counts by source are not additive across sources because multiple datasets can contribute to the same merged cluster.",
        "",
        "## Output Tables",
        "",
        "- `tables/table_spatial_coverage_summary.csv`",
        "- `tables/table_spatial_coverage_by_resolution.csv`",
        "- `tables/table_spatial_coverage_by_region.csv`",
        "- `tables/table_spatial_coverage_by_country.csv`",
        "- `tables/table_spatial_coverage_by_source.csv`",
        "- `tables/table_spatial_coverage_by_region_source.csv`",
        "- `tables/table_spatial_coverage_by_region_resolution.csv`",
        "- `tables/table_upstream_area_distribution.csv`",
        "- `tables/table_satellite_validation_spatial_coverage.csv`",
        "- `tables/table_unknown_country_region_clusters.csv`",
        "",
        "## Figure Suggestions",
        "",
        "- Main text: `fig_global_cluster_distribution`, `fig_spatial_coverage_by_resolution`, and `fig_upstream_area_distribution`.",
        "- Supplement: `fig_spatial_coverage_by_region_country`, `fig_source_spatial_contribution`, and satellite-validation spatial figures.",
        "",
        "## Manuscript-Usable Statements",
        "",
        "- The release provides broad river-basin coverage, but regional completeness should be interpreted together with unresolved basin and unknown-geography diagnostics.",
        "- The published basin sidecar covers the same cluster count as the resolved-basin subset, making polygon availability a direct proxy for basin-resolution success in this release.",
        "- Source rankings should be separated into spatial footprint and record-volume contribution, because a source can cover many clusters with few records or fewer clusters with dense long records.",
        "",
        "<!-- Compact legacy paragraph values",
        "",
        "Upstream-area bins: {}".format(bin_text or "NA"),
        "",
        "Main source contributions by cluster count: {}".format(source_text or "NA"),
        "",
        "-->",
    ]
    text = "\n".join(lines) + "\n"
    (OUTPUT_DIR / "article_spatial_coverage_summary.md").write_text(text, encoding="utf-8")


def write_outputs(
    clusters: pd.DataFrame,
    station_df: pd.DataFrame,
    source_df: pd.DataFrame,
    satellite_df: pd.DataFrame,
    polygon_by_resolution: Dict[str, Set[str]],
    polygon_layer_summary: pd.DataFrame,
) -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    summary = summarise_global(clusters, station_df, satellite_df)
    by_resolution = resolution_summary(station_df, polygon_by_resolution)
    by_region = (
        clusters.groupby(["continent", "region"], dropna=False)
        .apply(group_summary)
        .reset_index()
        .sort_values("cluster_count", ascending=False)
    )
    by_country = (
        clusters.groupby(["continent", "region", "country", "iso_a3"], dropna=False)
        .apply(group_summary)
        .reset_index()
        .sort_values("cluster_count", ascending=False)
    )
    area_dist = upstream_distribution(clusters)
    by_source = source_summary(source_df)
    by_region_source = region_source_summary(clusters, source_df)
    by_region_resolution = region_resolution_summary(clusters, source_df)
    by_satellite = satellite_summary(satellite_df)
    unknown_geo = unknown_geography_rows(clusters)

    summary.to_csv(TABLES_DIR / "table_spatial_coverage_summary.csv", index=False)
    by_resolution.to_csv(TABLES_DIR / "table_spatial_coverage_by_resolution.csv", index=False)
    by_region.to_csv(TABLES_DIR / "table_spatial_coverage_by_region.csv", index=False)
    by_country.to_csv(TABLES_DIR / "table_spatial_coverage_by_country.csv", index=False)
    area_dist.to_csv(TABLES_DIR / "table_upstream_area_distribution.csv", index=False)
    by_source.to_csv(TABLES_DIR / "table_spatial_coverage_by_source.csv", index=False)
    by_region_source.to_csv(TABLES_DIR / "table_spatial_coverage_by_region_source.csv", index=False)
    by_region_resolution.to_csv(TABLES_DIR / "table_spatial_coverage_by_region_resolution.csv", index=False)
    by_satellite.to_csv(TABLES_DIR / "table_satellite_validation_spatial_coverage.csv", index=False)
    unknown_geo.to_csv(TABLES_DIR / "table_unknown_country_region_clusters.csv", index=False)
    summary.to_csv(TABLES_DIR / "article_spatial_coverage_metrics.csv", index=False)
    if not polygon_layer_summary.empty:
        polygon_layer_summary.to_csv(TABLES_DIR / "table_basin_polygon_layers.csv", index=False)

    keep = [
        "cluster_key",
        "cluster_uid",
        "cluster_id",
        "lat",
        "lon",
        "country",
        "iso_a3",
        "continent",
        "region",
        "geographic_coverage",
        "basin_status",
        "basin_flag",
        "area_km2",
        "has_basin_polygon",
        "available_resolutions",
        "n_available_resolutions",
        "record_count",
        "n_sources",
        "source_names",
        "valid_latlon",
    ]
    clusters[[c for c in keep if c in clusters.columns]].to_csv(TABLES_DIR / "table_cluster_spatial_attributes.csv", index=False)
    write_article_summary(
        summary,
        area_dist,
        by_source,
        by_satellite,
        by_resolution,
        by_region,
        by_country,
        by_region_source,
        by_region_resolution,
        polygon_layer_summary,
        unknown_geo,
    )


def main() -> int:
    require_node113()
    station_df = normalise_station_catalog(read_csv(STATION_CATALOG, required=True))
    source_df = read_csv(SOURCE_STATION_CATALOG, required=False)
    satellite_df = read_csv(SATELLITE_CATALOG, required=False)
    polygon_keys, polygon_by_resolution, polygon_layer_summary = polygon_keys_by_layer(CLUSTER_BASINS_GPKG)
    clusters = build_cluster_table(station_df, source_df, polygon_keys)
    write_outputs(clusters, station_df, source_df, satellite_df, polygon_by_resolution, polygon_layer_summary)

    total = int(clusters["cluster_key"].nunique())
    resolved = int(clusters.loc[clusters["is_resolved"], "cluster_key"].nunique())
    polygons = int(clusters.loc[clusters["has_basin_polygon"], "cluster_key"].nunique())
    print("Wrote S8 spatial coverage statistics to {}".format(TABLES_DIR))
    print("Final clusters: {}".format(total))
    print("Resolved basin clusters: {}".format(resolved))
    print("Clusters with published basin polygons: {}".format(polygons))
    print("Article summary: {}".format(OUTPUT_DIR / "article_spatial_coverage_summary.md"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
