#!/usr/bin/env python3
"""
s7：导出最终 cluster 级流域单元 Shapefile。

思路：
1. s5_basin_clustered_stations.csv 中每个 cluster_id 取一个代表站点
   （优先 station_id == cluster_id 的行）；
2. 用该代表站点的 station_id 去 s4_upstream_basins.gpkg 中读取流域面几何；
3. 输出一个 polygon shapefile，每个 cluster 一条记录。

输入：
  - scripts_basin_test/output/s5_basin_clustered_stations.csv
  - scripts_basin_test/output/s4_upstream_basins.gpkg

输出：
  - scripts_basin_test/output/s7_cluster_basins.shp

说明：
  - 该文件是最终 cluster 级流域单元面文件；
  - shapefile 字段名最多 10 个字符，因此使用 cluster_ui 作为字段名；
    它对应 NC 中的 cluster_uid。
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from pipeline_paths import (
    S4_UPSTREAM_GPKG,
    S4_LOCAL_GPKG, 
    S5_BASIN_CLUSTERED_CSV,
    S7_CLUSTER_BASIN_SHP,
    S7_LOCAL_BASIN_SHP,
    get_output_r_root,
)

try:
    import geopandas as gpd
    HAS_GPD = True
except ImportError:
    HAS_GPD = False

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_STATIONS = ROOT / S5_BASIN_CLUSTERED_CSV
_DEFAULT_BASIN_GPKG = ROOT / S4_UPSTREAM_GPKG
_DEFAULT_LOCAL_GPKG  = ROOT / S4_LOCAL_GPKG
_DEFAULT_OUT = ROOT / S7_CLUSTER_BASIN_SHP
_DEFAULT_LOCAL_OUT   = ROOT / S7_LOCAL_BASIN_SHP


def _safe_text(value, maxlen=120):
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()[:maxlen]


def _build_cluster_uid(cluster_id):
    return "SED{:06d}".format(int(cluster_id))


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

    # 优先选择 station_id == cluster_id 的代表站；若不存在，则退回 cluster 内最小 station_id。
    work["_is_rep"] = (work["station_id"] == work["cluster_id"]).astype(int)
    work = work.sort_values(["cluster_id", "_is_rep", "station_id"], ascending=[True, False, True])
    reps = work.drop_duplicates(subset=["cluster_id"], keep="first").copy()

    # 额外统计 cluster 内包含多少条站点记录、多少个数据源。
    summary = (
        work.groupby("cluster_id")
        .agg(
            n_rows=("station_id", "size"),
            n_src=("source", "nunique"),
        )
        .reset_index()
    )
    reps = reps.merge(summary, on="cluster_id", how="left")
    return reps.drop(columns=["_is_rep"])


def main():
    ap = argparse.ArgumentParser(
        description="s7：从 s4 流域面和 s5 cluster 表导出最终 cluster 级 basin shapefile"
    )
    ap.add_argument("--stations", default=str(_DEFAULT_STATIONS), help="s5 cluster CSV 路径")
    ap.add_argument("--basin-gpkg", default=str(_DEFAULT_BASIN_GPKG), help="s4 流域面 GPKG 路径")
    ap.add_argument("--local-basin-gpkg", default=str(_DEFAULT_LOCAL_GPKG))
    ap.add_argument("--local-out",        default=str(_DEFAULT_LOCAL_OUT))
    ap.add_argument("--out", default=str(_DEFAULT_OUT), help="输出 shapefile 路径")
    args = ap.parse_args()

    if not HAS_GPD:
        print("Error: geopandas is required. Please install geopandas in the runtime environment.")
        return 1

    station_path = Path(args.stations)
    basin_gpkg = Path(args.basin_gpkg)
    out_path = Path(args.out)
    local_basin_gpkg = Path(args.local_basin_gpkg)
    local_out_path   = Path(args.local_out)

    if not station_path.is_file():
        print("Error: station CSV not found: {}".format(station_path))
        return 1
    if not basin_gpkg.is_file():
        print("Error: basin GPKG not found: {}".format(basin_gpkg))
        print("Hint: rerun s4 with GPKG output enabled, or point --basin-gpkg to an existing polygon file.")
        return 1

    stations = pd.read_csv(station_path)
    required_cols = {"station_id", "cluster_id"}
    missing = sorted(required_cols - set(stations.columns))
    if missing:
        print("Error: missing required columns in {}: {}".format(station_path, ", ".join(missing)))
        return 1

    reps = _pick_representative_rows(stations)
    print("Loaded {} cluster representatives from {}".format(len(reps), station_path))

    basin_gdf = gpd.read_file(basin_gpkg)
    if "station_id" not in basin_gdf.columns:
        print("Error: 'station_id' column not found in {}".format(basin_gpkg))
        return 1

    basin_gdf = basin_gdf.copy()
    basin_gdf["station_id"] = pd.to_numeric(basin_gdf["station_id"], errors="coerce").astype("Int64")
    basin_gdf = basin_gdf.dropna(subset=["station_id"]).copy()
    basin_gdf["station_id"] = basin_gdf["station_id"].astype(int)

    merged = reps.merge(
        basin_gdf[["station_id", "geometry"]],
        on="station_id",
        how="left",
    )

    n_total = len(merged)
    merged = merged[merged["geometry"].notna()].copy()
    n_kept = len(merged)
    n_dropped = n_total - n_kept

    merged["cluster_ui"] = merged["cluster_id"].map(_build_cluster_uid)
    merged["cluster_id"] = merged["cluster_id"].astype(int)
    merged["basin_id"] = pd.to_numeric(_series_or_default(merged, "basin_id", pd.NA), errors="coerce")
    merged["basin_area"] = pd.to_numeric(_series_or_default(merged, "basin_area", pd.NA), errors="coerce")
    merged["pfaf_code"] = pd.to_numeric(_series_or_default(merged, "pfaf_code", pd.NA), errors="coerce")
    merged["area_err"] = pd.to_numeric(_series_or_default(merged, "area_error", pd.NA), errors="coerce")
    merged["uparea"] = pd.to_numeric(_series_or_default(merged, "uparea_merit", pd.NA), errors="coerce")
    merged["n_up_reach"] = pd.to_numeric(_series_or_default(merged, "n_upstream_reaches", -9999), errors="coerce").fillna(-9999).astype(int)
    merged["n_rows"] = pd.to_numeric(_series_or_default(merged, "n_rows", 0), errors="coerce").fillna(0).astype(int)
    merged["n_src"] = pd.to_numeric(_series_or_default(merged, "n_src", 0), errors="coerce").fillna(0).astype(int)
    merged["match_qual"] = _series_or_default(merged, "match_quality", "").map(lambda x: _safe_text(x, 40))
    merged["method"] = _series_or_default(merged, "method", "").map(lambda x: _safe_text(x, 40))
    merged["stn_name"] = _series_or_default(merged, "station_name", "").map(lambda x: _safe_text(x, 80))
    merged["river_name"] = _series_or_default(merged, "river_name", "").map(lambda x: _safe_text(x, 80))
    merged["src_stn_id"] = _series_or_default(merged, "source_station_id", "").map(lambda x: _safe_text(x, 80))

    out_cols = [
        "cluster_ui",
        "cluster_id",
        "basin_id",
        "basin_area",
        "pfaf_code",
        "match_qual",
        "area_err",
        "uparea",
        "method",
        "n_up_reach",
        "n_rows",
        "n_src",
        "stn_name",
        "river_name",
        "src_stn_id",
        "geometry",
    ]
    out_gdf = gpd.GeoDataFrame(merged[out_cols], geometry="geometry", crs=basin_gdf.crs or "EPSG:4326")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    driver = "ESRI Shapefile" if out_path.suffix.lower() == ".shp" else "GPKG"
    out_gdf.to_file(out_path, driver=driver, encoding="UTF-8")

    print("Wrote {} cluster basin polygons -> {}".format(n_kept, out_path))
    if n_dropped > 0:
        print("Skipped {} clusters without basin geometry".format(n_dropped))

        # ── 最小单元集水区 SHP ───────────────────────────────────────────────────
    if local_basin_gpkg.is_file():
        local_gdf = gpd.read_file(local_basin_gpkg)
        if "station_id" not in local_gdf.columns:
            print("Warning: 'station_id' not found in local GPKG, skipping local SHP output")
        else:
            local_gdf = local_gdf.copy()
            local_gdf["station_id"] = pd.to_numeric(local_gdf["station_id"], errors="coerce").astype("Int64")
            local_gdf = local_gdf.dropna(subset=["station_id"]).copy()
            local_gdf["station_id"] = local_gdf["station_id"].astype(int)

            merged_local = reps.merge(
                local_gdf[["station_id", "geometry"]],
                on="station_id",
                how="left",
            )
            n_local_total = len(merged_local)
            merged_local = merged_local[merged_local["geometry"].notna()].copy()
            n_local_kept = len(merged_local)

            merged_local["cluster_ui"] = merged_local["cluster_id"].map(_build_cluster_uid)
            merged_local["cluster_id"] = merged_local["cluster_id"].astype(int)
            merged_local["basin_id"]   = pd.to_numeric(_series_or_default(merged_local, "basin_id", pd.NA), errors="coerce")
            merged_local["basin_area"] = pd.to_numeric(_series_or_default(merged_local, "basin_area", pd.NA), errors="coerce")
            merged_local["pfaf_code"]  = pd.to_numeric(_series_or_default(merged_local, "pfaf_code", pd.NA), errors="coerce")
            merged_local["area_err"]   = pd.to_numeric(_series_or_default(merged_local, "area_error", pd.NA), errors="coerce")
            merged_local["uparea"]     = pd.to_numeric(_series_or_default(merged_local, "uparea_merit", pd.NA), errors="coerce")
            merged_local["n_up_reach"] = pd.to_numeric(_series_or_default(merged_local, "n_upstream_reaches", -9999), errors="coerce").fillna(-9999).astype(int)
            merged_local["n_rows"]     = pd.to_numeric(_series_or_default(merged_local, "n_rows", 0), errors="coerce").fillna(0).astype(int)
            merged_local["n_src"]      = pd.to_numeric(_series_or_default(merged_local, "n_src", 0), errors="coerce").fillna(0).astype(int)
            merged_local["match_qual"] = _series_or_default(merged_local, "match_quality", "").map(lambda x: _safe_text(x, 40))
            merged_local["method"]     = _series_or_default(merged_local, "method", "").map(lambda x: _safe_text(x, 40))
            merged_local["stn_name"]   = _series_or_default(merged_local, "station_name", "").map(lambda x: _safe_text(x, 80))
            merged_local["river_name"] = _series_or_default(merged_local, "river_name", "").map(lambda x: _safe_text(x, 80))
            merged_local["src_stn_id"] = _series_or_default(merged_local, "source_station_id", "").map(lambda x: _safe_text(x, 80))

            local_out_gdf = gpd.GeoDataFrame(merged_local[out_cols], geometry="geometry", crs=local_gdf.crs or "EPSG:4326")
            local_out_path.parent.mkdir(parents=True, exist_ok=True)
            local_driver = "ESRI Shapefile" if local_out_path.suffix.lower() == ".shp" else "GPKG"
            local_out_gdf.to_file(local_out_path, driver=local_driver, encoding="UTF-8")
            print("Wrote {} cluster local catchment polygons -> {}".format(n_local_kept, local_out_path))
            if n_local_total - n_local_kept > 0:
                print("Skipped {} clusters without local catchment geometry".format(n_local_total - n_local_kept))
    else:
        print("Warning: local catchment GPKG not found ({}), skipping".format(local_basin_gpkg))
        print("Hint: rerun s4 to generate s4_local_catchments.gpkg")

    print("Join key in shapefile: cluster_ui  (matches NC 'cluster_uid')")
    return 0


if __name__ == "__main__":
    sys.exit(main())
