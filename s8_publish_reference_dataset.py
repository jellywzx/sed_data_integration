#!/usr/bin/env python3
"""
Publish the sediment reference dataset as a user-facing release package.

This script does not rebuild the upstream basin pipeline. Instead, it packages
existing outputs into a release-oriented layout with:

1. canonical NetCDF file names for end users;
2. station/source catalogs for fast lookup and provenance tracing;
3. GPKG spatial sidecars for GIS users;
4. a release README and a small validation report.

Default inputs:
  - scripts_basin_test/output/s6_basin_merged_all.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_daily.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_monthly.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_annual.nc
  - scripts_basin_test/output/s6_climatology_only.nc
  - scripts_basin_test/output/s7_cluster_basins.shp   (optional sidecar source)

Default output directory:
  - scripts_basin_test/output/sed_reference_release/
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline_paths import (
    RELEASE_CLUSTER_BASINS_GPKG,
    RELEASE_CLUSTER_POINTS_GPKG,
    RELEASE_CLIMATOLOGY_NC,
    RELEASE_DATASET_DIR,
    RELEASE_INVENTORY_CSV,
    RELEASE_MASTER_NC,
    RELEASE_MATRIX_ANNUAL_NC,
    RELEASE_MATRIX_DAILY_NC,
    RELEASE_MATRIX_MONTHLY_NC,
    RELEASE_README_MD,
    RELEASE_SOURCE_DATASET_CATALOG_CSV,
    RELEASE_SOURCE_STATION_CATALOG_CSV,
    RELEASE_SOURCE_STATIONS_GPKG,
    RELEASE_STATION_CATALOG_CSV,
    RELEASE_VALIDATION_CSV,
    S6_CLIMATOLOGY_NC,
    S6_MATRIX_DIR,
    S6_MERGED_NC,
    S7_CLUSTER_BASIN_SHP,
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
    import netCDF4 as nc4

    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_MASTER_NC = PROJECT_ROOT / S6_MERGED_NC
DEFAULT_MATRIX_DAILY = PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_daily.nc"
DEFAULT_MATRIX_MONTHLY = PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_monthly.nc"
DEFAULT_MATRIX_ANNUAL = PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_annual.nc"
DEFAULT_CLIM_NC = PROJECT_ROOT / S6_CLIMATOLOGY_NC
DEFAULT_CLUSTER_BASIN_VECTOR = PROJECT_ROOT / S7_CLUSTER_BASIN_SHP
DEFAULT_RELEASE_DIR = PROJECT_ROOT / RELEASE_DATASET_DIR
DEFAULT_RELEASE_README = PROJECT_ROOT / RELEASE_README_MD
DEFAULT_STATION_CATALOG = PROJECT_ROOT / RELEASE_STATION_CATALOG_CSV
DEFAULT_SOURCE_STATION_CATALOG = PROJECT_ROOT / RELEASE_SOURCE_STATION_CATALOG_CSV
DEFAULT_SOURCE_DATASET_CATALOG = PROJECT_ROOT / RELEASE_SOURCE_DATASET_CATALOG_CSV
DEFAULT_CLUSTER_POINTS_GPKG = PROJECT_ROOT / RELEASE_CLUSTER_POINTS_GPKG
DEFAULT_SOURCE_STATIONS_GPKG = PROJECT_ROOT / RELEASE_SOURCE_STATIONS_GPKG
DEFAULT_CLUSTER_BASINS_GPKG = PROJECT_ROOT / RELEASE_CLUSTER_BASINS_GPKG
DEFAULT_VALIDATION_CSV = PROJECT_ROOT / RELEASE_VALIDATION_CSV
DEFAULT_INVENTORY_CSV = PROJECT_ROOT / RELEASE_INVENTORY_CSV
DEFAULT_EXAMPLE_SCRIPT = SCRIPT_DIR / "example_reference_workflow.py"

RESOLUTION_CODE_TO_NAME = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}
RESOLUTION_NAME_TO_CODE = {name: code for code, name in RESOLUTION_CODE_TO_NAME.items()}
RESOLUTION_ORDER = {name: idx for idx, name in enumerate(["daily", "monthly", "annual", "climatology", "other"])}
MATCH_QUALITY_CODE_TO_NAME = {
    -1: "unknown",
    0: "distance_only",
    1: "area_matched",
    2: "failed",
}
CORE_FILE_SPECS = (
    ("master", RELEASE_MASTER_NC, "Authoritative record-level reference dataset"),
    ("daily", RELEASE_MATRIX_DAILY_NC, "Daily station x time matrix for validation"),
    ("monthly", RELEASE_MATRIX_MONTHLY_NC, "Monthly station x time matrix for validation"),
    ("annual", RELEASE_MATRIX_ANNUAL_NC, "Annual station x time matrix for validation"),
    ("climatology", RELEASE_CLIMATOLOGY_NC, "Standalone climatology reference dataset"),
)


def _clean_text(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def _read_text_var(ds, name, size=None):
    if name not in ds.variables:
        return [""] * int(size or 0)
    values = ds.variables[name][:]
    arr = np.asarray(values, dtype=object).reshape(-1)
    return [_clean_text(item) for item in arr]


def _read_float_array(ds, name, fill_values=None, size=None):
    if name not in ds.variables:
        return np.full(int(size or 0), np.nan, dtype=np.float64)
    arr = np.ma.asarray(ds.variables[name][:]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype=np.float64)
    if fill_values:
        for fill in fill_values:
            arr[arr == fill] = np.nan
    return arr


def _read_int_array(ds, name, fill_value=-1, size=None):
    if name not in ds.variables:
        return np.full(int(size or 0), fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        arr = raw.filled(fill_value)
    else:
        arr = np.asarray(raw)
    result = np.full(arr.shape, fill_value, dtype=np.int64)
    valid = np.isfinite(arr.astype(np.float64, copy=False))
    result[valid] = arr[valid].astype(np.int64)
    return result


def _decode_time_numbers(values, units, calendar):
    values = np.asarray(values, dtype=np.float64).reshape(-1)
    out = [""] * len(values)
    valid_mask = np.isfinite(values)
    if not valid_mask.any():
        return out
    try:
        decoded = nc4.num2date(
            values[valid_mask],
            units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
        )
    except TypeError:
        decoded = nc4.num2date(values[valid_mask], units, calendar=calendar)
    valid_values = []
    for item in decoded:
        text = _clean_text(item)
        if text:
            try:
                valid_values.append(pd.Timestamp(text).strftime("%Y-%m-%d %H:%M:%S"))
            except Exception:
                valid_values.append(text)
        else:
            valid_values.append("")
    for idx, text in zip(np.flatnonzero(valid_mask), valid_values):
        out[int(idx)] = text
    return out


def _ensure_removed(path):
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _prepare_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def _link_or_copy_file(src, dst, mode="hardlink", force=False):
    src = Path(src).resolve()
    dst = Path(dst)
    if not src.is_file():
        raise FileNotFoundError("Missing source file: {}".format(src))
    if dst.exists() or dst.is_symlink():
        if not force and dst.resolve() == src:
            return dst
        _ensure_removed(dst)
    _prepare_parent(dst)
    if mode == "hardlink":
        try:
            os.link(str(src), str(dst))
            return dst
        except OSError:
            mode = "copy"
    if mode == "symlink":
        dst.symlink_to(src)
        return dst
    shutil.copy2(str(src), str(dst))
    return dst


def _canonical_core_sources(master_nc, daily_nc, monthly_nc, annual_nc, climatology_nc):
    return {
        "master": Path(master_nc).resolve(),
        "daily": Path(daily_nc).resolve(),
        "monthly": Path(monthly_nc).resolve(),
        "annual": Path(annual_nc).resolve(),
        "climatology": Path(climatology_nc).resolve(),
    }


def _collect_matrix_station_stats(matrix_nc, resolution, chunk_rows=16):
    with nc4.Dataset(matrix_nc, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        n_time = len(ds.dimensions["time"])
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        valid_counts = _read_int_array(ds, "n_valid_time_steps", fill_value=0, size=n_stations)
        time_var = ds.variables["time"]
        time_values = _read_float_array(ds, "time", size=n_time)
        time_units = getattr(time_var, "units", "days since 1970-01-01")
        time_calendar = getattr(time_var, "calendar", "gregorian")

        first_time_num = np.full(n_stations, np.nan, dtype=np.float64)
        last_time_num = np.full(n_stations, np.nan, dtype=np.float64)

        for start in range(0, n_stations, chunk_rows):
            stop = min(start + chunk_rows, n_stations)
            q = np.ma.asarray(ds.variables["Q"][start:stop, :]).filled(np.nan)
            ssc = np.ma.asarray(ds.variables["SSC"][start:stop, :]).filled(np.nan)
            ssl = np.ma.asarray(ds.variables["SSL"][start:stop, :]).filled(np.nan)
            mask = np.isfinite(q) | np.isfinite(ssc) | np.isfinite(ssl)
            row_has = mask.any(axis=1)
            if not np.any(row_has):
                continue

            first_idx = np.where(row_has, mask.argmax(axis=1), -1)
            last_idx = np.where(row_has, n_time - 1 - mask[:, ::-1].argmax(axis=1), -1)
            global_rows = np.arange(start, stop, dtype=np.int64)
            valid_rows = global_rows[row_has]
            first_time_num[valid_rows] = time_values[first_idx[row_has]]
            last_time_num[valid_rows] = time_values[last_idx[row_has]]

    return pd.DataFrame(
        {
            "cluster_uid": cluster_uids,
            "{}_record_count".format(resolution): valid_counts.astype(np.int64),
            "{}_time_start".format(resolution): _decode_time_numbers(
                first_time_num,
                time_units,
                time_calendar,
            ),
            "{}_time_end".format(resolution): _decode_time_numbers(
                last_time_num,
                time_units,
                time_calendar,
            ),
        }
    )


def build_station_catalog(master_nc, matrix_paths):
    with nc4.Dataset(master_nc, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        cluster_ids = _read_int_array(ds, "cluster_id", fill_value=-1, size=n_stations)
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        if not any(cluster_uids):
            cluster_uids = ["SED{:06d}".format(int(cid)) if cid >= 0 else "" for cid in cluster_ids]

        station_df = pd.DataFrame(
            {
                "master_station_index": np.arange(n_stations, dtype=np.int32),
                "cluster_uid": cluster_uids,
                "cluster_id": cluster_ids,
                "lat": _read_float_array(ds, "lat", fill_values=(-9999.0,), size=n_stations),
                "lon": _read_float_array(ds, "lon", fill_values=(-9999.0,), size=n_stations),
                "basin_area": _read_float_array(ds, "basin_area", fill_values=(-9999.0,), size=n_stations),
                "pfaf_code": _read_float_array(ds, "pfaf_code", fill_values=(-9999.0,), size=n_stations),
                "n_upstream_reaches": _read_int_array(ds, "n_upstream_reaches", fill_value=-9999, size=n_stations),
                "station_name": _read_text_var(ds, "station_name", size=n_stations),
                "river_name": _read_text_var(ds, "river_name", size=n_stations),
                "source_station_id": _read_text_var(ds, "source_station_id", size=n_stations),
                "sources_used": _read_text_var(ds, "sources_used", size=n_stations),
                "n_source_stations_in_cluster": _read_int_array(
                    ds,
                    "n_source_stations_in_cluster",
                    fill_value=0,
                    size=n_stations,
                ),
            }
        )

        match_codes = _read_int_array(ds, "basin_match_quality", fill_value=-1, size=n_stations)
        station_df["basin_match_quality_code"] = match_codes
        station_df["basin_match_quality"] = [
            MATCH_QUALITY_CODE_TO_NAME.get(int(code), "unknown") for code in match_codes
        ]

    for resolution in ("daily", "monthly", "annual"):
        matrix_path = Path(matrix_paths.get(resolution, ""))
        if matrix_path.is_file():
            stats_df = _collect_matrix_station_stats(matrix_path, resolution)
            station_df = station_df.merge(stats_df, on="cluster_uid", how="left")

    for resolution in ("daily", "monthly", "annual", "climatology", "other"):
        count_col = "{}_record_count".format(resolution)
        start_col = "{}_time_start".format(resolution)
        end_col = "{}_time_end".format(resolution)
        if count_col not in station_df.columns:
            station_df[count_col] = 0
        if start_col not in station_df.columns:
            station_df[start_col] = ""
        if end_col not in station_df.columns:
            station_df[end_col] = ""

    count_cols = [col for col in station_df.columns if col.endswith("_record_count")]
    for col in count_cols:
        station_df[col] = station_df[col].fillna(0).astype(np.int64)
    time_cols = [col for col in station_df.columns if col.endswith("_time_start") or col.endswith("_time_end")]
    for col in time_cols:
        station_df[col] = station_df[col].fillna("")

    station_df["available_resolutions"] = station_df.apply(
        lambda row: "|".join(
            resolution
            for resolution in ("daily", "monthly", "annual", "climatology", "other")
            if row.get("{}_record_count".format(resolution), 0) > 0
        ),
        axis=1,
    )
    station_df["n_available_resolutions"] = station_df["available_resolutions"].map(
        lambda text: len([part for part in text.split("|") if part])
    )

    for col in ("cluster_id", "n_upstream_reaches", "n_source_stations_in_cluster", "basin_match_quality_code"):
        station_df[col] = station_df[col].astype(np.int64)

    return station_df


def build_source_station_catalog(master_nc, station_catalog):
    cluster_uid_lookup = station_catalog.set_index("master_station_index")["cluster_uid"].to_dict()
    cluster_id_lookup = station_catalog.set_index("master_station_index")["cluster_id"].to_dict()

    with nc4.Dataset(master_nc, "r") as ds:
        n_source_stations = len(ds.dimensions["n_source_stations"])
        n_sources = len(ds.dimensions["n_sources"])

        source_names = _read_text_var(ds, "source_name", size=n_sources)
        source_long_names = _read_text_var(ds, "source_long_name", size=n_sources)
        institutions = _read_text_var(ds, "institution", size=n_sources)
        references = _read_text_var(ds, "reference", size=n_sources)
        source_urls = _read_text_var(ds, "source_url", size=n_sources)

        cluster_indices = _read_int_array(
            ds,
            "source_station_cluster_index",
            fill_value=-1,
            size=n_source_stations,
        )
        source_indices = _read_int_array(
            ds,
            "source_station_source_index",
            fill_value=-1,
            size=n_source_stations,
        )

        source_station_df = pd.DataFrame(
            {
                "source_station_index": np.arange(n_source_stations, dtype=np.int32),
                "source_station_uid": _read_text_var(ds, "source_station_uid", size=n_source_stations),
                "source_station_cluster_index": cluster_indices,
                "source_station_source_index": source_indices,
                "source_station_native_id": _read_text_var(
                    ds,
                    "source_station_native_id",
                    size=n_source_stations,
                ),
                "source_station_name": _read_text_var(ds, "source_station_name", size=n_source_stations),
                "source_station_river_name": _read_text_var(
                    ds,
                    "source_station_river_name",
                    size=n_source_stations,
                ),
                "source_station_lat": _read_float_array(
                    ds,
                    "source_station_lat",
                    fill_values=(-9999.0,),
                    size=n_source_stations,
                ),
                "source_station_lon": _read_float_array(
                    ds,
                    "source_station_lon",
                    fill_values=(-9999.0,),
                    size=n_source_stations,
                ),
                "source_station_paths": _read_text_var(ds, "source_station_paths", size=n_source_stations),
                "source_station_resolutions": _read_text_var(
                    ds,
                    "source_station_resolutions",
                    size=n_source_stations,
                ),
            }
        )

    source_station_df["cluster_uid"] = source_station_df["source_station_cluster_index"].map(
        lambda idx: cluster_uid_lookup.get(int(idx), "") if int(idx) >= 0 else ""
    )
    source_station_df["cluster_id"] = source_station_df["source_station_cluster_index"].map(
        lambda idx: cluster_id_lookup.get(int(idx), -1) if int(idx) >= 0 else -1
    )
    source_station_df["source_name"] = source_station_df["source_station_source_index"].map(
        lambda idx: source_names[int(idx)] if 0 <= int(idx) < len(source_names) else ""
    )
    source_station_df["source_long_name"] = source_station_df["source_station_source_index"].map(
        lambda idx: source_long_names[int(idx)] if 0 <= int(idx) < len(source_long_names) else ""
    )
    source_station_df["institution"] = source_station_df["source_station_source_index"].map(
        lambda idx: institutions[int(idx)] if 0 <= int(idx) < len(institutions) else ""
    )
    source_station_df["reference"] = source_station_df["source_station_source_index"].map(
        lambda idx: references[int(idx)] if 0 <= int(idx) < len(references) else ""
    )
    source_station_df["source_url"] = source_station_df["source_station_source_index"].map(
        lambda idx: source_urls[int(idx)] if 0 <= int(idx) < len(source_urls) else ""
    )

    source_station_df["n_paths"] = source_station_df["source_station_paths"].map(
        lambda text: len([part for part in text.split("|") if part])
    )
    source_station_df["n_resolutions"] = source_station_df["source_station_resolutions"].map(
        lambda text: len([part for part in text.split("|") if part])
    )
    return source_station_df


def build_source_dataset_catalog(source_station_catalog):
    keep_cols = ["source_name", "source_long_name", "institution", "reference", "source_url"]
    source_df = source_station_catalog[keep_cols].drop_duplicates(subset=["source_name"]).copy()
    source_df = source_df.sort_values("source_name").reset_index(drop=True)
    counts = (
        source_station_catalog.groupby("source_name", observed=True)
        .agg(
            n_source_stations=("source_station_uid", "size"),
            n_clusters=("cluster_uid", "nunique"),
        )
        .reset_index()
    )
    source_df = source_df.merge(counts, on="source_name", how="left")
    return source_df


def _write_csv(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def _write_gpkg(gdf, path):
    path = Path(path)
    if path.exists():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GPKG", encoding="UTF-8")
    return path


def write_cluster_points_gpkg(station_catalog, out_path):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG export")
    work = station_catalog.copy()
    work = work[np.isfinite(work["lat"]) & np.isfinite(work["lon"])].copy()
    keep_cols = [
        "cluster_uid",
        "cluster_id",
        "station_name",
        "river_name",
        "source_station_id",
        "lat",
        "lon",
        "basin_area",
        "pfaf_code",
        "n_upstream_reaches",
        "basin_match_quality",
        "n_source_stations_in_cluster",
        "available_resolutions",
        "sources_used",
    ]
    geometry = [Point(float(lon), float(lat)) for lon, lat in zip(work["lon"], work["lat"])]
    gdf = gpd.GeoDataFrame(work[keep_cols].copy(), geometry=geometry, crs="EPSG:4326")
    return _write_gpkg(gdf, out_path)


def write_source_stations_gpkg(source_station_catalog, out_path):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG export")
    work = source_station_catalog.copy()
    work = work[np.isfinite(work["source_station_lat"]) & np.isfinite(work["source_station_lon"])].copy()
    keep_cols = [
        "source_station_uid",
        "cluster_uid",
        "cluster_id",
        "source_name",
        "source_long_name",
        "source_station_native_id",
        "source_station_name",
        "source_station_river_name",
        "source_station_lat",
        "source_station_lon",
        "source_station_resolutions",
    ]
    geometry = [
        Point(float(lon), float(lat))
        for lon, lat in zip(work["source_station_lon"], work["source_station_lat"])
    ]
    gdf = gpd.GeoDataFrame(work[keep_cols].copy(), geometry=geometry, crs="EPSG:4326")
    return _write_gpkg(gdf, out_path)


def write_cluster_basins_gpkg(input_vector, out_path):
    input_vector = Path(input_vector)
    if not input_vector.exists():
        return None
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG export")
    gdf = gpd.read_file(input_vector)
    if "cluster_ui" in gdf.columns and "cluster_uid" not in gdf.columns:
        gdf = gdf.rename(columns={"cluster_ui": "cluster_uid"})
    return _write_gpkg(gdf, out_path)


def write_release_readme(out_path):
    content = """# Sediment Reference Dataset Release

This directory is the user-facing release layer of the sediment reference dataset.

## Core NetCDF products

- `sed_reference_master.nc`: authoritative long-table archive with full provenance.
- `sed_reference_timeseries_daily.nc`: daily `station x time` matrix for validation.
- `sed_reference_timeseries_monthly.nc`: monthly `station x time` matrix for validation.
- `sed_reference_timeseries_annual.nc`: annual `station x time` matrix for validation.
- `sed_reference_climatology.nc`: standalone climatology dataset.

## Catalogs

- `station_catalog.csv`: one row per basin cluster (`cluster_uid`) with coordinates, basin attributes, available resolutions, and time coverage.
- `source_station_catalog.csv`: one row per original source station (`source_station_uid`) with links back to cluster, source dataset, and original file path.
- `source_dataset_catalog.csv`: one row per source dataset with metadata and counts.

## GIS sidecars

- `sed_reference_cluster_points.gpkg`: cluster point layer keyed by `cluster_uid`.
- `sed_reference_source_stations.gpkg`: original source-station point layer keyed by `source_station_uid`.
- `sed_reference_cluster_basins.gpkg`: optional cluster basin polygons keyed by `cluster_uid`.

## Recommended workflow

1. Open the matrix file that matches your model output resolution.
2. Use `lat/lon` in that matrix file or `station_catalog.csv` to find the nearest `cluster_uid`.
3. Extract the observed time series and compare it with the model time series.
4. If you need provenance, query `sed_reference_master.nc` with `cluster_uid + time + resolution`.
5. Use `source_station_catalog.csv` to resolve `source_station_uid`, original station metadata, and original file path.
6. Keep climatology analyses separate and use `sed_reference_climatology.nc` directly.

## Quick example

The helper script `example_reference_workflow.py` shows:

- nearest-station matching;
- matrix time-series extraction;
- optional model/reference alignment for a gridded model NetCDF;
- provenance lookup back to `source_station_uid`.

Example:

```bash
python3 example_reference_workflow.py \\
  --release-dir . \\
  --resolution monthly \\
  --lat 30.5 \\
  --lon 114.3 \\
  --variable SSC
```

## Notes

- `cluster_uid` is the stable station key for the basin mainline products.
- `source_station_uid` is the stable key for tracing back to original stations.
- `station_uid` is the stable key inside the climatology product only.
- The release does not automatically aggregate daily model output to monthly or annual resolution.
"""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def _relative_to_release(path, release_dir):
    try:
        return str(Path(path).resolve().relative_to(Path(release_dir).resolve()))
    except Exception:
        return str(Path(path))


def write_inventory(file_records, out_path, release_dir):
    rows = []
    for kind, path, description in file_records:
        path = Path(path)
        if not path.exists():
            continue
        rows.append(
            {
                "kind": kind,
                "file_name": path.name,
                "relative_path": _relative_to_release(path, release_dir),
                "description": description,
                "file_size_mb": round(path.stat().st_size / (1024 * 1024), 3),
            }
        )
    df = pd.DataFrame(rows).sort_values(["kind", "file_name"]).reset_index(drop=True)
    return _write_csv(df, out_path)


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1 = np.deg2rad(np.asarray(lat1, dtype=np.float64))
    lon1 = np.deg2rad(np.asarray(lon1, dtype=np.float64))
    lat2 = np.deg2rad(np.asarray(lat2, dtype=np.float64))
    lon2 = np.deg2rad(np.asarray(lon2, dtype=np.float64))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _first_overlap_sample(ds):
    if "is_overlap" not in ds.variables or "selected_source_index" not in ds.variables:
        return None
    n_stations = len(ds.dimensions["n_stations"])
    n_time = len(ds.dimensions["time"])
    overlap_var = ds.variables["is_overlap"]
    source_var = ds.variables["selected_source_index"]
    for start in range(0, n_stations, 32):
        stop = min(start + 32, n_stations)
        overlap = np.ma.asarray(overlap_var[start:stop, :]).filled(0)
        selected = np.ma.asarray(source_var[start:stop, :]).filled(-1)
        mask = (overlap == 1) & (selected >= 0)
        hits = np.argwhere(mask)
        if hits.size == 0:
            continue
        local_row, col = hits[0]
        station_idx = start + int(local_row)
        if station_idx < n_stations and int(col) < n_time:
            return station_idx, int(col), int(selected[local_row, col])
    return None


def _find_master_record_index(master_ds, station_index, resolution_code, target_time_num, chunk_size=500000):
    n_records = len(master_ds.dimensions["n_records"])
    station_var = master_ds.variables["station_index"]
    resolution_var = master_ds.variables["resolution"]
    time_var = master_ds.variables["time"]

    for start in range(0, n_records, chunk_size):
        stop = min(start + chunk_size, n_records)
        station_chunk = np.asarray(station_var[start:stop], dtype=np.int32).reshape(-1)
        resolution_chunk = np.asarray(resolution_var[start:stop], dtype=np.int16).reshape(-1)
        time_chunk = np.asarray(time_var[start:stop], dtype=np.float64).reshape(-1)
        mask = (
            (station_chunk == int(station_index))
            & (resolution_chunk == int(resolution_code))
            & np.isclose(time_chunk, float(target_time_num))
        )
        hit = np.flatnonzero(mask)
        if len(hit) > 0:
            return start + int(hit[0])
    return None


def validate_release(
    master_nc,
    matrix_paths,
    climatology_nc,
    station_catalog,
    source_station_catalog,
    out_csv,
):
    rows = []
    cluster_uid_lookup = station_catalog.set_index("cluster_uid")["master_station_index"].to_dict()
    source_station_catalog = source_station_catalog.copy()
    source_station_uid_lookup = source_station_catalog.set_index("source_station_index")[
        "source_station_uid"
    ].to_dict()

    with nc4.Dataset(master_nc, "r") as master_ds:
        master_cluster_uids = _read_text_var(master_ds, "cluster_uid", size=len(master_ds.dimensions["n_stations"]))
        master_time_var = master_ds.variables["time"]
        master_time_units = getattr(master_time_var, "units", "days since 1970-01-01")
        master_time_calendar = getattr(master_time_var, "calendar", "gregorian")
        master_source_station_uids = _read_text_var(
            master_ds,
            "source_station_uid",
            size=len(master_ds.dimensions["n_source_stations"]),
        )
        master_cluster_uid_set = set(master_cluster_uids)

        for resolution, path in matrix_paths.items():
            path = Path(path)
            status = "pass"
            detail = ""
            if not path.is_file():
                rows.append(
                    {
                        "check": "matrix_exists_{}".format(resolution),
                        "status": "fail",
                        "details": "Missing {}".format(path),
                    }
                )
                continue

            with nc4.Dataset(path, "r") as ds:
                required = {"lat", "lon", "cluster_uid", "time", "SSC", "n_valid_time_steps"}
                missing = sorted(required - set(ds.variables))
                if missing:
                    rows.append(
                        {
                            "check": "matrix_structure_{}".format(resolution),
                            "status": "fail",
                            "details": "Missing variables: {}".format(", ".join(missing)),
                        }
                    )
                    continue

                lats = _read_float_array(ds, "lat", fill_values=(-9999.0,), size=len(ds.dimensions["n_stations"]))
                lons = _read_float_array(ds, "lon", fill_values=(-9999.0,), size=len(ds.dimensions["n_stations"]))
                cluster_uids = _read_text_var(ds, "cluster_uid", size=len(ds.dimensions["n_stations"]))
                valid_steps = _read_int_array(
                    ds,
                    "n_valid_time_steps",
                    fill_value=0,
                    size=len(ds.dimensions["n_stations"]),
                )

                non_empty_idx = np.flatnonzero(valid_steps > 0)
                if len(non_empty_idx) == 0:
                    rows.append(
                        {
                            "check": "matrix_nonempty_{}".format(resolution),
                            "status": "fail",
                            "details": "No stations with data in {}".format(path.name),
                        }
                    )
                    continue

                sample_idx = int(non_empty_idx[0])
                sample_lat = float(lats[sample_idx])
                sample_lon = float(lons[sample_idx])
                distances = _haversine_km(sample_lat, sample_lon, lats, lons)
                nearest_idx = int(np.nanargmin(distances))
                rows.append(
                    {
                        "check": "nearest_station_lookup_{}".format(resolution),
                        "status": "pass" if nearest_idx == sample_idx else "fail",
                        "details": "sample={} nearest={}".format(sample_idx, nearest_idx),
                    }
                )

                ssc_row = np.ma.asarray(ds.variables["SSC"][sample_idx, :]).filled(np.nan)
                non_missing = int(np.count_nonzero(np.isfinite(ssc_row)))
                rows.append(
                    {
                        "check": "matrix_series_extract_{}".format(resolution),
                        "status": "pass" if non_missing > 0 else "fail",
                        "details": "cluster_uid={} non_missing_SSC={}".format(
                            cluster_uids[sample_idx],
                            non_missing,
                        ),
                    }
                )

                cluster_uid = cluster_uids[sample_idx]
                rows.append(
                    {
                        "check": "master_lookup_{}".format(resolution),
                        "status": "pass" if cluster_uid in master_cluster_uid_set else "fail",
                        "details": cluster_uid,
                    }
                )

                overlap_sample = _first_overlap_sample(ds)
                if overlap_sample is None:
                    rows.append(
                        {
                            "check": "overlap_consistency_{}".format(resolution),
                            "status": "skip",
                            "details": "No overlap cell found in {}".format(path.name),
                        }
                    )
                else:
                    station_row, time_col, selected_source_idx = overlap_sample
                    sample_cluster_uid = cluster_uids[station_row]
                    master_idx = cluster_uid_lookup.get(sample_cluster_uid, None)
                    if master_idx is None:
                        rows.append(
                            {
                                "check": "overlap_consistency_{}".format(resolution),
                                "status": "fail",
                                "details": "cluster_uid missing from station catalog: {}".format(sample_cluster_uid),
                            }
                        )
                        continue
                    time_var = ds.variables["time"]
                    matrix_time_val = float(np.asarray(time_var[time_col]).reshape(-1)[0])
                    try:
                        decoded = nc4.num2date(
                            matrix_time_val,
                            getattr(time_var, "units", master_time_units),
                            calendar=getattr(time_var, "calendar", master_time_calendar),
                            only_use_cftime_datetimes=False,
                        )
                    except TypeError:
                        decoded = nc4.num2date(
                            matrix_time_val,
                            getattr(time_var, "units", master_time_units),
                            calendar=getattr(time_var, "calendar", master_time_calendar),
                        )
                    target_time_num = nc4.date2num(
                        decoded,
                        master_time_units,
                        calendar=master_time_calendar,
                    )
                    record_idx = _find_master_record_index(
                        master_ds=master_ds,
                        station_index=int(master_idx),
                        resolution_code=RESOLUTION_NAME_TO_CODE[resolution],
                        target_time_num=target_time_num,
                    )
                    if record_idx is None:
                        rows.append(
                            {
                                "check": "overlap_consistency_{}".format(resolution),
                                "status": "fail",
                                "details": "No matching master record found",
                            }
                        )
                    else:
                        source_name = _clean_text(ds.variables["source_name"][selected_source_idx])
                        master_overlap = int(np.ma.asarray(master_ds.variables["is_overlap"][record_idx]).filled(0))
                        master_source = _clean_text(master_ds.variables["source"][record_idx])
                        pass_flag = (
                            master_overlap == 1
                            and master_source == source_name
                        )
                        rows.append(
                            {
                                "check": "overlap_consistency_{}".format(resolution),
                                "status": "pass" if pass_flag else "fail",
                                "details": "matrix_source={} master_source={} record_idx={}".format(
                                    source_name,
                                    master_source,
                                    record_idx,
                                ),
                            }
                        )

        sampled_indices = [
            int(idx)
            for idx in np.ma.asarray(master_ds.variables["source_station_index"][:1000]).filled(-1)
            if int(idx) >= 0
        ]
        source_station_ok = True
        for idx in sampled_indices:
            idx = int(idx)
            uid_master = master_source_station_uids[idx] if idx < len(master_source_station_uids) else ""
            uid_catalog = source_station_uid_lookup.get(idx, "")
            if uid_master != uid_catalog:
                source_station_ok = False
                detail = "Mismatch at source_station_index={}".format(idx)
                break
        else:
            detail = "sampled_rows={}".format(len(sampled_indices))
        rows.append(
            {
                "check": "source_station_catalog_lookup",
                "status": "pass" if source_station_ok else "fail",
                "details": detail,
            }
        )

    with nc4.Dataset(climatology_nc, "r") as clim_ds:
        station_uids = _read_text_var(clim_ds, "station_uid", size=len(clim_ds.dimensions["n_stations"]))
        unique_ok = len(station_uids) == len(set(station_uids))
        rows.append(
            {
                "check": "climatology_station_uid_unique",
                "status": "pass" if unique_ok else "fail",
                "details": "n_stations={}".format(len(station_uids)),
            }
        )
        paths = _read_text_var(clim_ds, "source_station_path", size=len(clim_ds.dimensions["n_stations"]))
        existing = sum(1 for path in paths if path and Path(path).is_file())
        rows.append(
            {
                "check": "climatology_path_exists",
                "status": "pass" if existing == len(paths) else "fail",
                "details": "{}/{} source files found".format(existing, len(paths)),
            }
        )

    report_df = pd.DataFrame(rows)
    _write_csv(report_df, out_csv)
    failed = report_df["status"].eq("fail").any()
    return not failed, report_df


def main():
    ap = argparse.ArgumentParser(description="Publish the sediment reference dataset release package")
    ap.add_argument("--master-nc", default=str(DEFAULT_MASTER_NC))
    ap.add_argument("--daily-nc", default=str(DEFAULT_MATRIX_DAILY))
    ap.add_argument("--monthly-nc", default=str(DEFAULT_MATRIX_MONTHLY))
    ap.add_argument("--annual-nc", default=str(DEFAULT_MATRIX_ANNUAL))
    ap.add_argument("--climatology-nc", default=str(DEFAULT_CLIM_NC))
    ap.add_argument("--cluster-basin-vector", default=str(DEFAULT_CLUSTER_BASIN_VECTOR))
    ap.add_argument("--out-dir", default=str(DEFAULT_RELEASE_DIR))
    ap.add_argument(
        "--link-mode",
        choices=("hardlink", "symlink", "copy"),
        default="hardlink",
        help="How to materialize canonical NetCDF/example files in the release dir",
    )
    ap.add_argument("--skip-gpkg", action="store_true", help="Skip GPKG spatial sidecars")
    ap.add_argument(
        "--include-basin-polygons",
        action="store_true",
        help="Also convert cluster basin polygons to GPKG; disabled by default because it can be heavy",
    )
    ap.add_argument("--skip-validation", action="store_true", help="Skip release validation checks")
    ap.add_argument("--force", action="store_true", help="Overwrite existing release files")
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1

    master_nc = Path(args.master_nc).resolve()
    daily_nc = Path(args.daily_nc).resolve()
    monthly_nc = Path(args.monthly_nc).resolve()
    annual_nc = Path(args.annual_nc).resolve()
    climatology_nc = Path(args.climatology_nc).resolve()
    basin_vector = Path(args.cluster_basin_vector).resolve()
    out_dir = Path(args.out_dir).resolve()

    required_inputs = [master_nc, daily_nc, monthly_nc, annual_nc, climatology_nc]
    missing = [str(path) for path in required_inputs if not path.is_file()]
    if missing:
        print("Error: required inputs missing:")
        for item in missing:
            print("  - {}".format(item))
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    print("Release dir: {}".format(out_dir))

    core_sources = _canonical_core_sources(master_nc, daily_nc, monthly_nc, annual_nc, climatology_nc)
    core_destinations = {
        "master": out_dir / Path(RELEASE_MASTER_NC).name,
        "daily": out_dir / Path(RELEASE_MATRIX_DAILY_NC).name,
        "monthly": out_dir / Path(RELEASE_MATRIX_MONTHLY_NC).name,
        "annual": out_dir / Path(RELEASE_MATRIX_ANNUAL_NC).name,
        "climatology": out_dir / Path(RELEASE_CLIMATOLOGY_NC).name,
    }

    file_records = []
    for kind, _, description in CORE_FILE_SPECS:
        dst = _link_or_copy_file(
            core_sources[kind],
            core_destinations[kind],
            mode=args.link_mode,
            force=args.force,
        )
        file_records.append(("core_netcdf", dst, description))
        print("Prepared {} -> {}".format(kind, dst.name))

    station_catalog = build_station_catalog(
        master_nc,
        matrix_paths={
            "daily": daily_nc,
            "monthly": monthly_nc,
            "annual": annual_nc,
        },
    )
    source_station_catalog = build_source_station_catalog(master_nc, station_catalog)
    source_dataset_catalog = build_source_dataset_catalog(source_station_catalog)

    station_catalog_path = _write_csv(station_catalog, out_dir / Path(RELEASE_STATION_CATALOG_CSV).name)
    source_station_catalog_path = _write_csv(
        source_station_catalog,
        out_dir / Path(RELEASE_SOURCE_STATION_CATALOG_CSV).name,
    )
    source_dataset_catalog_path = _write_csv(
        source_dataset_catalog,
        out_dir / Path(RELEASE_SOURCE_DATASET_CATALOG_CSV).name,
    )
    file_records.extend(
        [
            ("catalog", station_catalog_path, "Cluster-level lookup catalog"),
            ("catalog", source_station_catalog_path, "Source-station provenance catalog"),
            ("catalog", source_dataset_catalog_path, "Source-dataset metadata catalog"),
        ]
    )
    print("Wrote catalogs: station={}, source_station={}, source_dataset={}".format(
        len(station_catalog),
        len(source_station_catalog),
        len(source_dataset_catalog),
    ))

    if args.skip_gpkg:
        print("Skip GPKG sidecars by request.")
    else:
        if not HAS_GPD:
            print("Warning: geopandas is unavailable, skip GPKG sidecars.")
        else:
            cluster_points_path = write_cluster_points_gpkg(
                station_catalog,
                out_dir / Path(RELEASE_CLUSTER_POINTS_GPKG).name,
            )
            source_stations_path = write_source_stations_gpkg(
                source_station_catalog,
                out_dir / Path(RELEASE_SOURCE_STATIONS_GPKG).name,
            )
            file_records.extend(
                [
                    ("spatial", cluster_points_path, "Cluster point sidecar keyed by cluster_uid"),
                    ("spatial", source_stations_path, "Source-station point sidecar keyed by source_station_uid"),
                ]
            )
            print("Wrote GPKG sidecars: {}, {}".format(cluster_points_path.name, source_stations_path.name))

            basin_out = None
            if args.include_basin_polygons and basin_vector.exists():
                basin_out = write_cluster_basins_gpkg(
                    basin_vector,
                    out_dir / Path(RELEASE_CLUSTER_BASINS_GPKG).name,
                )
            if basin_out is not None:
                file_records.append(
                    ("spatial", basin_out, "Cluster basin polygon sidecar keyed by cluster_uid")
                )
                print("Wrote basin polygon GPKG: {}".format(basin_out.name))
            elif args.include_basin_polygons:
                print("Cluster basin vector not found, skip polygon sidecar: {}".format(basin_vector))
            else:
                print("Skip basin polygon GPKG by default; use --include-basin-polygons to enable it.")

    if DEFAULT_EXAMPLE_SCRIPT.is_file():
        example_dst = _link_or_copy_file(
            DEFAULT_EXAMPLE_SCRIPT,
            out_dir / DEFAULT_EXAMPLE_SCRIPT.name,
            mode=args.link_mode,
            force=args.force,
        )
        file_records.append(("support", example_dst, "Example workflow script"))
        print("Prepared example script: {}".format(example_dst.name))
    else:
        print("Warning: example script not found: {}".format(DEFAULT_EXAMPLE_SCRIPT))

    readme_path = write_release_readme(out_dir / Path(RELEASE_README_MD).name)
    file_records.append(("support", readme_path, "Release usage guide"))

    validation_path = out_dir / Path(RELEASE_VALIDATION_CSV).name
    if args.skip_validation:
        print("Skip validation by request.")
    else:
        ok, report_df = validate_release(
            master_nc=master_nc,
            matrix_paths={
                "daily": daily_nc,
                "monthly": monthly_nc,
                "annual": annual_nc,
            },
            climatology_nc=climatology_nc,
            station_catalog=station_catalog,
            source_station_catalog=source_station_catalog,
            out_csv=validation_path,
        )
        file_records.append(("report", validation_path, "Release validation report"))
        print("Validation checks: {} rows".format(len(report_df)))
        if not ok:
            print("Error: release validation reported failures. See {}".format(validation_path))
            return 1

    inventory_path = write_inventory(file_records, out_dir / Path(RELEASE_INVENTORY_CSV).name, out_dir)
    print("Wrote inventory: {}".format(inventory_path))
    print("Release package is ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
