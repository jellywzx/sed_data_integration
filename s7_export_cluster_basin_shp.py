#!/usr/bin/env python3
"""
s7: export resolution-aware cluster basin polygons.

The polygon selection rule stays the same as before:
1. pick one representative station per cluster_id from s5;
2. read that representative station's polygon from the s4 GPKG;
3. expand the representative geometry by the resolutions that are actually
   present in s7_cluster_resolution_catalog.csv.

Outputs:
  - a multi-layer GPKG with basin_daily / basin_monthly / basin_annual;
  - optionally a local-catchment GPKG with basin_local_* layers.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from cluster_spatial_catalog import (
    CLUSTER_RESOLUTIONS,
    HAS_GPD,
    build_cluster_basin_resolution_catalog,
    normalize_cluster_resolution_catalog,
    write_cluster_basins_gpkg,
)
from pipeline_paths import (
    S4_LOCAL_GPKG,
    S4_UPSTREAM_GPKG,
    S5_BASIN_CLUSTERED_CSV,
    S7_CLUSTER_BASINS_GPKG,
    S7_CLUSTER_RESOLUTION_CATALOG_CSV,
    S7_LOCAL_BASINS_GPKG,
    get_output_r_root,
)

try:
    import geopandas as gpd
except ImportError:
    gpd = None


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_STATIONS = ROOT / S5_BASIN_CLUSTERED_CSV
_DEFAULT_CLUSTER_RESOLUTION_CATALOG = ROOT / S7_CLUSTER_RESOLUTION_CATALOG_CSV
_DEFAULT_BASIN_GPKG = ROOT / S4_UPSTREAM_GPKG
_DEFAULT_LOCAL_GPKG = ROOT / S4_LOCAL_GPKG
_DEFAULT_OUT = ROOT / S7_CLUSTER_BASINS_GPKG
_DEFAULT_LOCAL_OUT = ROOT / S7_LOCAL_BASINS_GPKG


def _safe_text(value, maxlen=120):
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()[:maxlen]


def _series_or_default(df: pd.DataFrame, col: str, default_value):
    if col in df.columns:
        return df[col]
    return pd.Series([default_value] * len(df), index=df.index)


def _pick_representative_rows(stations: pd.DataFrame) -> pd.DataFrame:
    work = stations.copy()
    work["station_id"] = pd.to_numeric(work["station_id"], errors="coerce").astype("Int64")
    work["cluster_id"] = pd.to_numeric(work["cluster_id"], errors="coerce").astype("Int64")
    work = work.dropna(subset=["station_id", "cluster_id"]).copy()
    work["station_id"] = work["station_id"].astype(int)
    work["cluster_id"] = work["cluster_id"].astype(int)

    work["_is_rep"] = (work["station_id"] == work["cluster_id"]).astype(int)
    work = work.sort_values(["cluster_id", "_is_rep", "station_id"], ascending=[True, False, True])
    reps = work.drop_duplicates(subset=["cluster_id"], keep="first").copy()

    summary = (
        work.groupby("cluster_id")
        .agg(
            n_rows=("station_id", "size"),
            n_src=("source", "nunique"),
        )
        .reset_index()
    )
    reps = reps.merge(summary, on="cluster_id", how="left")

    reps["station_name"] = _series_or_default(reps, "station_name", "").map(lambda x: _safe_text(x, 160))
    reps["river_name"] = _series_or_default(reps, "river_name", "").map(lambda x: _safe_text(x, 160))
    reps["source_station_id"] = _series_or_default(reps, "source_station_id", "").map(lambda x: _safe_text(x, 160))
    reps["match_quality"] = _series_or_default(reps, "match_quality", "").map(lambda x: _safe_text(x, 60))
    reps["method"] = _series_or_default(reps, "method", "").map(lambda x: _safe_text(x, 60))
    reps["basin_id"] = pd.to_numeric(_series_or_default(reps, "basin_id", pd.NA), errors="coerce")
    reps["basin_area"] = pd.to_numeric(_series_or_default(reps, "basin_area", pd.NA), errors="coerce")
    reps["pfaf_code"] = pd.to_numeric(_series_or_default(reps, "pfaf_code", pd.NA), errors="coerce")
    reps["area_error"] = pd.to_numeric(_series_or_default(reps, "area_error", pd.NA), errors="coerce")
    reps["uparea_merit"] = pd.to_numeric(_series_or_default(reps, "uparea_merit", pd.NA), errors="coerce")
    reps["n_upstream_reaches"] = pd.to_numeric(
        _series_or_default(reps, "n_upstream_reaches", -9999),
        errors="coerce",
    ).fillna(-9999).astype(int)
    return reps.drop(columns=["_is_rep"])


def _load_polygon_gdf(path):
    gdf = gpd.read_file(path)
    if "station_id" not in gdf.columns:
        raise ValueError("'station_id' column not found in {}".format(path))
    return gdf


def _write_resolution_basin_gpkg(cluster_resolution_catalog, representatives, basin_gpkg, out_path, layer_prefix):
    basin_gdf = _load_polygon_gdf(basin_gpkg)
    basin_catalog = build_cluster_basin_resolution_catalog(
        cluster_resolution_catalog=cluster_resolution_catalog,
        representatives=representatives,
        basin_gdf=basin_gdf,
    )
    gpkg_path = write_cluster_basins_gpkg(basin_catalog, out_path, layer_prefix=layer_prefix)
    return gpkg_path, basin_catalog


def main():
    ap = argparse.ArgumentParser(
        description="s7: export resolution-aware cluster basin polygons as multi-layer GPKG"
    )
    ap.add_argument("--stations", default=str(_DEFAULT_STATIONS), help="s5 cluster CSV path")
    ap.add_argument(
        "--cluster-resolution-catalog",
        default=str(_DEFAULT_CLUSTER_RESOLUTION_CATALOG),
        help="s7 cluster resolution catalog CSV path",
    )
    ap.add_argument("--basin-gpkg", default=str(_DEFAULT_BASIN_GPKG), help="s4 upstream basin GPKG path")
    ap.add_argument("--local-basin-gpkg", default=str(_DEFAULT_LOCAL_GPKG))
    ap.add_argument(
        "--out",
        default=str(_DEFAULT_OUT),
        help="output multi-layer cluster basin GPKG path",
    )
    ap.add_argument(
        "--local-out",
        default=str(_DEFAULT_LOCAL_OUT),
        help="optional output local-catchment GPKG path",
    )
    args = ap.parse_args()

    if not HAS_GPD:
        print("Error: geopandas is required. Please install geopandas in the runtime environment.")
        return 1

    station_path = Path(args.stations).resolve()
    cluster_resolution_catalog_path = Path(args.cluster_resolution_catalog).resolve()
    basin_gpkg = Path(args.basin_gpkg).resolve()
    local_basin_gpkg = Path(args.local_basin_gpkg).resolve()
    out_path = Path(args.out).resolve()
    local_out_path = Path(args.local_out).resolve()

    required_inputs = [station_path, cluster_resolution_catalog_path, basin_gpkg]
    missing = [str(path) for path in required_inputs if not path.is_file()]
    if missing:
        print("Error: required inputs missing:")
        for item in missing:
            print("  - {}".format(item))
        return 1
    if out_path.suffix.lower() != ".gpkg":
        print("Error: --out must be a .gpkg path")
        return 1
    if local_out_path.suffix.lower() != ".gpkg":
        print("Error: --local-out must be a .gpkg path")
        return 1

    stations = pd.read_csv(station_path)
    required_cols = {"station_id", "cluster_id"}
    missing_cols = sorted(required_cols - set(stations.columns))
    if missing_cols:
        print("Error: missing required columns in {}: {}".format(station_path, ", ".join(missing_cols)))
        return 1

    representatives = _pick_representative_rows(stations)
    cluster_resolution_catalog = normalize_cluster_resolution_catalog(
        pd.read_csv(cluster_resolution_catalog_path, keep_default_na=False)
    )

    gpkg_path, basin_catalog = _write_resolution_basin_gpkg(
        cluster_resolution_catalog=cluster_resolution_catalog,
        representatives=representatives,
        basin_gpkg=basin_gpkg,
        out_path=out_path,
        layer_prefix="basin",
    )
    print("Wrote cluster basin GPKG: {}".format(gpkg_path))
    for resolution in CLUSTER_RESOLUTIONS:
        count = int((basin_catalog["resolution"] == resolution).sum())
        print("basin_{} rows = {}".format(resolution, count))

    if local_basin_gpkg.is_file():
        local_gpkg_path, local_catalog = _write_resolution_basin_gpkg(
            cluster_resolution_catalog=cluster_resolution_catalog,
            representatives=representatives,
            basin_gpkg=local_basin_gpkg,
            out_path=local_out_path,
            layer_prefix="basin_local",
        )
        print("Wrote local basin GPKG: {}".format(local_gpkg_path))
        for resolution in CLUSTER_RESOLUTIONS:
            count = int((local_catalog["resolution"] == resolution).sum())
            print("basin_local_{} rows = {}".format(resolution, count))
    else:
        print("Warning: local catchment GPKG not found ({}), skipping".format(local_basin_gpkg))

    print("Join key: cluster_uid + resolution")
    return 0


if __name__ == "__main__":
    sys.exit(main())
