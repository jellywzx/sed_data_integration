#!/usr/bin/env python3
"""
s11: run checklist-oriented audit for the basin reference dataset.

Outputs:
  scripts_basin_test/output/manual_review/08_checklist_auto_results.csv
  scripts_basin_test/output/manual_review/09_manual_check_guide.csv
  scripts_basin_test/output/manual_review/10_resolution_record_counts.csv
  scripts_basin_test/output/manual_review/11_overlap_provenance_summary.csv
  scripts_basin_test/output/manual_review/12_variable_completeness.csv
  scripts_basin_test/output/manual_review/13_flag_distribution.csv
  scripts_basin_test/output/manual_review/14_source_coverage_summary.csv
  scripts_basin_test/output/manual_review/15_cluster_join_summary.csv
  scripts_basin_test/output/manual_review/16_cluster_point_polygon_check.csv
  scripts_basin_test/output/manual_review/17_basin_geometry_quality.csv
  scripts_basin_test/output/manual_review/18_provenance_path_check.csv

This version reads the new multi-layer GPKG outputs and evaluates joins by the
composite key `cluster_uid + resolution`.
"""

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from cluster_spatial_catalog import (
    CLUSTER_RESOLUTIONS,
    HAS_GPD,
    list_gpkg_layers,
    read_gpkg_layer,
)
from pipeline_paths import (
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    S6_CLIMATOLOGY_NC,
    S6_MERGED_NC,
    S7_CLUSTER_BASINS_GPKG,
    S7_CLUSTER_POINTS_GPKG,
    S7_SOURCE_STATIONS_GPKG,
    get_output_r_root,
)

if HAS_GPD:
    import geopandas as gpd
else:
    gpd = None


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_CHECKLIST = SCRIPT_DIR / "manual_review_checklist.csv"
DEFAULT_S4 = ROOT / S4_UPSTREAM_CSV
DEFAULT_S5 = ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_S6 = ROOT / S6_MERGED_NC
DEFAULT_CLIM_NC = ROOT / S6_CLIMATOLOGY_NC
DEFAULT_CLUSTER_GPKG = ROOT / S7_CLUSTER_POINTS_GPKG
DEFAULT_SOURCE_GPKG = ROOT / S7_SOURCE_STATIONS_GPKG
DEFAULT_CLUSTER_BASIN_GPKG = ROOT / S7_CLUSTER_BASINS_GPKG
DEFAULT_OUT_DIR = ROOT / "scripts_basin_test/output/manual_review"

CORE_OUTPUT_LABELS = [
    "s6_nc",
    "cluster_points_gpkg",
    "source_stations_gpkg",
    "cluster_basins_gpkg",
]


def open_netcdf_dataset(path: Path):
    kwargs = dict(decode_cf=False, mask_and_scale=False)
    last_exc = None
    for engine in (None, "netcdf4", "h5netcdf"):
        try:
            if engine is None:
                return xr.open_dataset(path, **kwargs)
            return xr.open_dataset(path, engine=engine, **kwargs)
        except Exception as exc:
            last_exc = exc
    raise last_exc


def decode_object_array(values):
    out = []
    for value in values:
        if isinstance(value, (bytes, bytearray)):
            out.append(value.decode("utf-8"))
        else:
            out.append(str(value))
    return out


def safe_series_to_str(series: pd.Series):
    return series.fillna("").astype(str).str.strip()


def file_info_rows(file_map):
    rows = []
    for label, path in file_map.items():
        row = {
            "dataset": label,
            "path": str(path),
            "exists": path.exists(),
            "is_file": path.is_file(),
            "size_mb": np.nan,
            "mtime": "",
        }
        if path.exists():
            stat = path.stat()
            row["size_mb"] = round(stat.st_size / 1024 / 1024, 3)
            row["mtime"] = pd.Timestamp(stat.st_mtime, unit="s").isoformat()
        rows.append(row)
    return pd.DataFrame(rows)


def _empty_gdf(columns=None):
    columns = columns or ["geometry"]
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG audit")
    return gpd.GeoDataFrame({col: pd.Series(dtype=object) for col in columns}, geometry="geometry", crs="EPSG:4326")


def _concat_gdfs(frames, empty_columns=None):
    if len(frames) == 0:
        return _empty_gdf(empty_columns or ["geometry"])
    crs = frames[0].crs
    combined = pd.concat(frames, ignore_index=True, sort=False)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=crs)


def _load_multilayer_vector(path: Path, prefix: str, include_summary=False):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for GPKG audit")
    layers = set(list_gpkg_layers(path))
    summary = _empty_gdf()
    if include_summary and "{}_summary".format(prefix) in layers:
        summary = read_gpkg_layer(path, "{}_summary".format(prefix))
    frames = []
    for resolution in CLUSTER_RESOLUTIONS:
        layer_name = "{}_{}".format(prefix, resolution)
        if layer_name not in layers:
            continue
        gdf = read_gpkg_layer(path, layer_name)
        if "resolution" not in gdf.columns:
            gdf = gdf.copy()
            gdf["resolution"] = resolution
        frames.append(gdf)
    if prefix == "source":
        empty_columns = ["cluster_uid", "resolution", "source_station_uid", "geometry"]
    else:
        empty_columns = ["cluster_uid", "resolution", "geometry"]
    combined = _concat_gdfs(frames, empty_columns=empty_columns)
    return summary, combined, sorted(layers)


def load_s5(path: Path):
    usecols = [
        "station_id",
        "path",
        "source",
        "resolution",
        "station_name",
        "river_name",
        "source_station_id",
        "cluster_id",
        "basin_id",
        "basin_area",
        "match_quality",
        "pfaf_code",
        "method",
    ]
    df = pd.read_csv(path, usecols=usecols)
    df["cluster_id"] = pd.to_numeric(df["cluster_id"], errors="coerce").astype("Int64")
    return df


def load_s4(path: Path):
    df = pd.read_csv(path, usecols=["station_id", "basin_id"])
    df["basin_id"] = pd.to_numeric(df["basin_id"], errors="coerce")
    return df


def _resolution_names_from_ds(ds):
    if "resolution" in ds.variables and "flag_meanings" in ds["resolution"].attrs:
        names = str(ds["resolution"].attrs["flag_meanings"]).split()
        if names:
            return names
    return ["daily", "monthly", "annual", "climatology", "other"]


def load_nc_bundle(path: Path):
    ds = open_netcdf_dataset(path)
    try:
        dims = {k: int(v) for k, v in ds.sizes.items()}

        cluster_id = ds["cluster_id"].values.astype(np.int64)
        if "cluster_uid" in ds.variables:
            cluster_uid = np.array(decode_object_array(ds["cluster_uid"].values), dtype=object)
        else:
            cluster_uid = np.array(["SED{:06d}".format(int(v)) for v in cluster_id], dtype=object)

        resolution_codes = ds["resolution"].values.astype(np.int16)
        resolution_names = _resolution_names_from_ds(ds)
        code_to_name = {i: name for i, name in enumerate(resolution_names)}
        resolution_labels = np.array([code_to_name.get(int(x), "unknown") for x in resolution_codes], dtype=object)

        station_index = ds["station_index"].values.astype(np.int64)
        source_station_index = ds["source_station_index"].values.astype(np.int64)
        is_overlap = ds["is_overlap"].values.astype(np.int8) if "is_overlap" in ds.variables else np.zeros(len(station_index), dtype=np.int8)
        source_station_cluster_index = (
            ds["source_station_cluster_index"].values.astype(np.int64)
            if "source_station_cluster_index" in ds.variables
            else np.array([], dtype=np.int64)
        )

        source_station_uid = np.array(decode_object_array(ds["source_station_uid"].values), dtype=object)
        source_station_paths = np.array(decode_object_array(ds["source_station_paths"].values), dtype=object)
        source_station_resolutions = np.array(decode_object_array(ds["source_station_resolutions"].values), dtype=object)

        q = ds["Q"].values
        ssc = ds["SSC"].values
        ssl = ds["SSL"].values
        q_flag = ds["Q_flag"].values.astype(np.int16)
        ssc_flag = ds["SSC_flag"].values.astype(np.int16)
        ssl_flag = ds["SSL_flag"].values.astype(np.int16)

        source_names = np.array(decode_object_array(ds["source_name"].values), dtype=object) if "source_name" in ds.variables else np.array([], dtype=object)
        source_records = np.array(decode_object_array(ds["source"].values), dtype=object) if "source" in ds.variables else np.array([], dtype=object)

        record_cluster_uid = cluster_uid[station_index]
        main_mask = np.isin(resolution_labels, np.array(CLUSTER_RESOLUTIONS, dtype=object))

        cluster_resolution_df = (
            pd.DataFrame(
                {
                    "cluster_uid": record_cluster_uid[main_mask],
                    "resolution": resolution_labels[main_mask],
                }
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        source_mask = main_mask & (source_station_index >= 0)
        source_station_uid_for_records = np.array(
            [
                source_station_uid[idx] if 0 <= idx < len(source_station_uid) else ""
                for idx in source_station_index[source_mask].tolist()
            ],
            dtype=object,
        )
        source_resolution_df = (
            pd.DataFrame(
                {
                    "source_station_index": source_station_index[source_mask].astype(np.int64),
                    "source_station_uid": source_station_uid_for_records,
                    "cluster_uid": record_cluster_uid[source_mask],
                    "resolution": resolution_labels[source_mask],
                }
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )

        return {
            "dims": dims,
            "cluster_id": cluster_id,
            "cluster_uid": cluster_uid,
            "resolution_codes": resolution_codes,
            "resolution_labels": resolution_labels,
            "resolution_names": resolution_names,
            "station_index": station_index,
            "source_station_index": source_station_index,
            "is_overlap": is_overlap,
            "source_station_cluster_index": source_station_cluster_index,
            "source_station_uid": source_station_uid,
            "source_station_paths": source_station_paths,
            "source_station_resolutions": source_station_resolutions,
            "q": q,
            "ssc": ssc,
            "ssl": ssl,
            "q_flag": q_flag,
            "ssc_flag": ssc_flag,
            "ssl_flag": ssl_flag,
            "source_names": source_names,
            "source_records": source_records,
            "cluster_resolution_df": cluster_resolution_df,
            "source_resolution_df": source_resolution_df,
        }
    finally:
        ds.close()


def load_simple_nc_stats(path: Path):
    if not path.is_file():
        return {"exists": False, "dims": {}, "n_records": 0, "n_stations": 0}
    ds = open_netcdf_dataset(path)
    try:
        dims = {k: int(v) for k, v in ds.sizes.items()}
        return {
            "exists": True,
            "dims": dims,
            "n_records": int(dims.get("n_records", 0)),
            "n_stations": int(dims.get("n_stations", 0)),
        }
    finally:
        ds.close()


def build_resolution_summary(nc):
    unique_codes, counts = np.unique(nc["resolution_codes"], return_counts=True)
    rows = []
    for code, count in zip(unique_codes.tolist(), counts.tolist()):
        rows.append(
            {
                "resolution_code": int(code),
                "resolution_name": nc["resolution_names"][int(code)] if int(code) < len(nc["resolution_names"]) else "unknown",
                "record_count": int(count),
                "record_fraction": float(count) / max(int(counts.sum()), 1),
            }
        )
    return pd.DataFrame(rows).sort_values("resolution_code").reset_index(drop=True)


def build_variable_completeness(nc):
    rows = []
    for name, values, fill in [
        ("Q", nc["q"], -9999.0),
        ("SSC", nc["ssc"], -9999.0),
        ("SSL", nc["ssl"], -9999.0),
    ]:
        values = np.asarray(values)
        missing = int(np.sum(~np.isfinite(values) | (values == fill)))
        total = int(values.size)
        rows.append(
            {
                "variable": name,
                "total_records": total,
                "non_missing_records": total - missing,
                "missing_records": missing,
                "non_missing_fraction": float(total - missing) / max(total, 1),
            }
        )
    return pd.DataFrame(rows)


def build_flag_distribution(nc):
    rows = []
    flag_names = {0: "good", 1: "estimated", 2: "suspect", 3: "bad", 9: "missing"}
    for var_name, flags in [
        ("Q_flag", nc["q_flag"]),
        ("SSC_flag", nc["ssc_flag"]),
        ("SSL_flag", nc["ssl_flag"]),
    ]:
        unique_vals, counts = np.unique(flags, return_counts=True)
        total = int(counts.sum())
        for value, count in zip(unique_vals.tolist(), counts.tolist()):
            rows.append(
                {
                    "flag_variable": var_name,
                    "flag_value": int(value),
                    "flag_name": flag_names.get(int(value), "unknown"),
                    "record_count": int(count),
                    "record_fraction": float(count) / max(total, 1),
                }
            )
    return pd.DataFrame(rows)


def build_source_coverage(s5_df, nc):
    s5_counts = (
        safe_series_to_str(s5_df["source"])
        .replace("", "blank")
        .value_counts()
        .rename_axis("source_name")
        .reset_index(name="s5_station_rows")
    )
    nc_source_names = pd.DataFrame({"source_name": pd.Series(nc["source_names"], dtype=object)})
    if len(nc["source_records"]):
        nc_record_counts = (
            pd.Series(nc["source_records"], dtype=object)
            .fillna("")
            .astype(str)
            .str.strip()
            .replace("", "blank")
            .value_counts()
            .rename_axis("source_name")
            .reset_index(name="s6_record_count")
        )
    else:
        nc_record_counts = pd.DataFrame(columns=["source_name", "s6_record_count"])

    out = (
        pd.DataFrame({"source_name": sorted(set(s5_counts["source_name"]).union(set(nc_source_names["source_name"])).union(set(nc_record_counts["source_name"])))})
        .merge(s5_counts, on="source_name", how="left")
        .merge(nc_record_counts, on="source_name", how="left")
    )
    out["in_s5"] = out["s5_station_rows"].fillna(0).gt(0)
    out["in_s6_source_table"] = out["source_name"].isin(set(nc_source_names["source_name"]))
    out["in_s6_records"] = out["s6_record_count"].fillna(0).gt(0)
    return out.sort_values(["in_s5", "source_name"], ascending=[False, True]).reset_index(drop=True)


def _cluster_key_set(df):
    if len(df) == 0:
        return set()
    return set(zip(safe_series_to_str(df["cluster_uid"]), safe_series_to_str(df["resolution"])))


def build_join_summary(nc, cluster_resolution_df, source_resolution_df, basin_resolution_df):
    nc_cluster_set = _cluster_key_set(nc["cluster_resolution_df"])
    cluster_set = _cluster_key_set(cluster_resolution_df)
    source_cluster_set = _cluster_key_set(source_resolution_df[["cluster_uid", "resolution"]].drop_duplicates()) if len(source_resolution_df) else set()
    basin_set = _cluster_key_set(basin_resolution_df)

    rows = [
        {
            "check_group": "nc_vs_cluster_points",
            "left_only_count": len(nc_cluster_set - cluster_set),
            "right_only_count": len(cluster_set - nc_cluster_set),
            "intersection_count": len(nc_cluster_set & cluster_set),
        },
        {
            "check_group": "cluster_points_vs_cluster_basins",
            "left_only_count": len(cluster_set - basin_set),
            "right_only_count": len(basin_set - cluster_set),
            "intersection_count": len(cluster_set & basin_set),
        },
        {
            "check_group": "source_points_vs_cluster_points",
            "left_only_count": len(source_cluster_set - cluster_set),
            "right_only_count": len(cluster_set - source_cluster_set),
            "intersection_count": len(source_cluster_set & cluster_set),
        },
    ]
    return pd.DataFrame(rows)


def build_overlap_summary(nc):
    overlap_mask = nc["is_overlap"] == 1
    overlap_total = int(overlap_mask.sum())
    missing_source_idx = int(np.sum(overlap_mask & (nc["source_station_index"] < 0)))
    blank_source = 0
    if len(nc["source_records"]):
        blank_source = int(np.sum(overlap_mask & (pd.Series(nc["source_records"]).fillna("").astype(str).str.strip().values == "")))

    rows = [
        {"metric": "overlap_records", "value": overlap_total},
        {"metric": "overlap_missing_source_station_index", "value": missing_source_idx},
        {"metric": "overlap_missing_source_station_fraction", "value": float(missing_source_idx) / max(overlap_total, 1)},
        {"metric": "overlap_blank_source_records", "value": blank_source},
    ]

    flag_rows = []
    for label, flags in [("Q_flag", nc["q_flag"]), ("SSC_flag", nc["ssc_flag"]), ("SSL_flag", nc["ssl_flag"])]:
        overlap_flags = flags[overlap_mask]
        unique_vals, counts = np.unique(overlap_flags, return_counts=True)
        for value, count in zip(unique_vals.tolist(), counts.tolist()):
            flag_rows.append(
                {
                    "metric": label,
                    "flag_value": int(value),
                    "record_count": int(count),
                    "record_fraction": float(count) / max(int(counts.sum()), 1),
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(flag_rows)


def build_point_polygon_check(cluster_resolution_gdf, basin_resolution_gdf, source_resolution_gdf=None):
    cluster_left = cluster_resolution_gdf.rename(columns={"geometry": "point_geom"}).copy()
    basin_right = basin_resolution_gdf.rename(columns={"geometry": "poly_geom"}).copy()
    merged = cluster_left.merge(
        basin_right[["cluster_uid", "resolution", "poly_geom"]],
        on=["cluster_uid", "resolution"],
        how="left",
    )

    results = []
    for row in merged.itertuples(index=False):
        rec = {
            "cluster_uid": row.cluster_uid,
            "resolution": row.resolution,
        }
        if hasattr(row, "station_name"):
            rec["station_name"] = getattr(row, "station_name", "")
        if hasattr(row, "river_name"):
            rec["river_name"] = getattr(row, "river_name", "")
        if hasattr(row, "record_count"):
            rec["record_count"] = getattr(row, "record_count", np.nan)
        if row.poly_geom is None:
            rec.update({"relation": "missing_polygon", "distance_deg": np.nan})
        else:
            covers = row.poly_geom.covers(row.point_geom)
            if covers:
                rec.update({"relation": "covers", "distance_deg": 0.0})
            else:
                rec.update({"relation": "outside", "distance_deg": float(row.point_geom.distance(row.poly_geom))})
        results.append(rec)

    detail = pd.DataFrame(results)
    if len(detail) == 0:
        detail = pd.DataFrame(columns=["cluster_uid", "resolution", "relation", "distance_deg"])
    if source_resolution_gdf is not None and len(source_resolution_gdf):
        src_df = pd.DataFrame(source_resolution_gdf.drop(columns="geometry", errors="ignore")).copy()
        agg = (
            src_df.groupby(["cluster_uid", "resolution"], as_index=False)
            .agg(
                source_station_uids=("source_station_uid", lambda x: "|".join(sorted(set(str(v) for v in x if str(v).strip())))),
                source_names=("source_name", lambda x: "|".join(sorted(set(str(v) for v in x if str(v).strip())))),
                source_station_names=("station_name", lambda x: "|".join(sorted(set(str(v) for v in x if str(v).strip())))),
            )
        )
        detail = detail.merge(agg, on=["cluster_uid", "resolution"], how="left")

    summary = (
        detail.groupby(["resolution", "relation"], as_index=False)
        .size()
        .rename(columns={"size": "cluster_count"})
        .sort_values(["resolution", "relation"])
        .reset_index(drop=True)
    )
    return summary, detail


def build_basin_geometry_quality(basin_resolution_gdf):
    rows = []
    all_resolutions = ["all"] + list(CLUSTER_RESOLUTIONS)
    for resolution in all_resolutions:
        subset = basin_resolution_gdf if resolution == "all" else basin_resolution_gdf[basin_resolution_gdf["resolution"] == resolution]
        rows.extend(
            [
                {"resolution": resolution, "metric": "polygon_count", "value": int(len(subset))},
                {"resolution": resolution, "metric": "empty_geometry_count", "value": int(subset.geometry.is_empty.sum()) if len(subset) else 0},
                {"resolution": resolution, "metric": "invalid_geometry_count", "value": int((~subset.geometry.is_valid).sum()) if len(subset) else 0},
                {"resolution": resolution, "metric": "null_geometry_count", "value": int(subset.geometry.isna().sum()) if len(subset) else 0},
            ]
        )
    return pd.DataFrame(rows)


def build_path_check(nc):
    rows = []
    missing_parts = 0
    total_parts = 0
    for idx, path_text in enumerate(nc["source_station_paths"].tolist()):
        parts = [p.strip() for p in str(path_text).split("|") if str(p).strip()]
        if not parts:
            rows.append({"source_station_uid": nc["source_station_uid"][idx], "path_count": 0, "missing_path_count": 0, "all_paths_exist": False})
            continue
        miss = 0
        for part in parts:
            total_parts += 1
            if not Path(part).exists():
                miss += 1
                missing_parts += 1
        rows.append(
            {
                "source_station_uid": nc["source_station_uid"][idx],
                "path_count": len(parts),
                "missing_path_count": miss,
                "all_paths_exist": miss == 0,
            }
        )
    df = pd.DataFrame(rows)
    summary = pd.DataFrame(
        [
            {"metric": "source_station_rows", "value": int(len(df))},
            {"metric": "source_station_rows_all_paths_exist", "value": int(df["all_paths_exist"].sum())},
            {"metric": "source_station_rows_missing_any_path", "value": int((~df["all_paths_exist"]).sum())},
            {"metric": "total_path_entries", "value": int(total_parts)},
            {"metric": "missing_path_entries", "value": int(missing_parts)},
        ]
    )
    return summary, df


def make_result(check_id, auto_status, auto_summary, evidence="", output_file="", manual_required=False, manual_hint=""):
    return {
        "check_id": check_id,
        "auto_status": auto_status,
        "auto_summary": auto_summary,
        "evidence": evidence,
        "output_file": output_file,
        "manual_required": "yes" if manual_required else "no",
        "manual_hint": manual_hint,
    }


def build_manual_guide():
    rows = [
        {
            "check_id": "A12",
            "suggested_file": "16_cluster_point_polygon_check.csv",
            "how_to_check": "先看 relation 列是否有 outside 或 missing_polygon；再在 GIS 里打开 s7_cluster_points.gpkg 和 s7_cluster_basins.gpkg，对同一个 cluster_uid + resolution 联查这些异常 key。",
            "pass_condition": "点被对应分辨率的流域面 covers，或仅极少量贴边界。",
        },
        {
            "check_id": "A14",
            "suggested_file": "06_overlap_cluster_queue.csv; 11_overlap_provenance_summary.csv",
            "how_to_check": "优先查看 overlap 记录多的 cluster，检查所选来源是否来自明显低质量源；结合 Q_flag/SSC_flag/SSL_flag 和 source_station_index 抽样核对。",
            "pass_condition": "overlap 记录以 good/estimated 为主，没有成批 bad 记录胜出。",
        },
        {
            "check_id": "B05",
            "suggested_file": "17_basin_geometry_quality.csv; s7_cluster_basins.gpkg",
            "how_to_check": "先看不同分辨率图层的几何质量统计，再在 GIS 中抽样核对 basin_area、pfaf_code、match_quality 与几何大小是否一致。",
            "pass_condition": "属性无系统性缺失，几何形态与 basin_area 大体相符。",
        },
        {
            "check_id": "C04",
            "suggested_file": "s7_cluster_points.gpkg; s7_cluster_basins.gpkg",
            "how_to_check": "在 GIS 中按 daily/monthly/annual 三个图层浏览，观察是否有大块空白或异常密集区域。",
            "pass_condition": "空间分布符合预期，不存在明显整区漏失或异常聚集。",
        },
        {
            "check_id": "C05",
            "suggested_file": "18_provenance_path_check.csv",
            "how_to_check": "抽样最终记录，从 source_station_index 找到 source_station_uid，再看 source_station_paths 是否能定位到原始文件。",
            "pass_condition": "抽样链路能从最终记录回到原始文件。",
        },
        {
            "check_id": "C06",
            "suggested_file": "18_provenance_path_check.csv; s5_basin_clustered_stations.csv",
            "how_to_check": "反向抽样原始文件路径或 source_station_id，检查它最终进入了哪个 cluster。",
            "pass_condition": "原始文件能稳定回连到 cluster。",
        },
    ]
    return pd.DataFrame(rows)


def _metric_value(df, metric, resolution="all"):
    subset = df[(df["metric"] == metric) & (df["resolution"] == resolution)]
    if len(subset) == 0:
        return 0
    return int(subset["value"].iloc[0])


def main():
    ap = argparse.ArgumentParser(description="Run checklist-oriented automated audit")
    ap.add_argument("--checklist", default=str(DEFAULT_CHECKLIST), help="manual review checklist csv")
    ap.add_argument("--s4", default=str(DEFAULT_S4), help="s4 upstream basin csv")
    ap.add_argument("--s5", default=str(DEFAULT_S5), help="s5 clustered csv")
    ap.add_argument("--s6", default=str(DEFAULT_S6), help="s6 merged nc")
    ap.add_argument("--climatology-nc", default=str(DEFAULT_CLIM_NC), help="standalone climatology nc")
    ap.add_argument("--cluster-shp", "--cluster-gpkg", dest="cluster_vector", default=str(DEFAULT_CLUSTER_GPKG), help="cluster point GPKG path")
    ap.add_argument("--source-shp", "--source-gpkg", dest="source_vector", default=str(DEFAULT_SOURCE_GPKG), help="source station GPKG path")
    ap.add_argument("--cluster-basin-shp", "--cluster-basin-gpkg", dest="cluster_basin_vector", default=str(DEFAULT_CLUSTER_BASIN_GPKG), help="cluster basin GPKG path")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="output directory")
    args = ap.parse_args()

    if not HAS_GPD:
        raise RuntimeError("geopandas is required for the resolution-aware checklist audit")

    checklist_path = Path(args.checklist)
    s4_path = Path(args.s4)
    s5_path = Path(args.s5)
    s6_path = Path(args.s6)
    climatology_nc_path = Path(args.climatology_nc)
    cluster_vector_path = Path(args.cluster_vector)
    source_vector_path = Path(args.source_vector)
    cluster_basin_vector_path = Path(args.cluster_basin_vector)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    checklist = pd.read_csv(checklist_path)

    file_map = {
        "s6_nc": s6_path,
        "climatology_nc": climatology_nc_path,
        "cluster_points_gpkg": cluster_vector_path,
        "source_stations_gpkg": source_vector_path,
        "cluster_basins_gpkg": cluster_basin_vector_path,
        "s4_csv": s4_path,
        "s5_csv": s5_path,
    }
    dataset_summary = file_info_rows(file_map)
    dataset_summary.to_csv(out_dir / "00_dataset_summary.csv", index=False)

    s4_df = load_s4(s4_path)
    s5_df = load_s5(s5_path)
    nc = load_nc_bundle(s6_path)
    clim_nc = load_simple_nc_stats(climatology_nc_path)

    cluster_summary_gdf, cluster_resolution_gdf, cluster_layers = _load_multilayer_vector(cluster_vector_path, "cluster", include_summary=True)
    _, source_resolution_gdf, source_layers = _load_multilayer_vector(source_vector_path, "source", include_summary=False)
    _, basin_resolution_gdf, basin_layers = _load_multilayer_vector(cluster_basin_vector_path, "basin", include_summary=False)

    cluster_resolution_df = pd.DataFrame(cluster_resolution_gdf.drop(columns="geometry", errors="ignore"))
    source_resolution_df = pd.DataFrame(source_resolution_gdf.drop(columns="geometry", errors="ignore"))
    basin_resolution_df = pd.DataFrame(basin_resolution_gdf.drop(columns="geometry", errors="ignore"))

    resolution_summary = build_resolution_summary(nc)
    resolution_summary.to_csv(out_dir / "10_resolution_record_counts.csv", index=False)

    var_summary = build_variable_completeness(nc)
    var_summary.to_csv(out_dir / "12_variable_completeness.csv", index=False)

    flag_summary = build_flag_distribution(nc)
    flag_summary.to_csv(out_dir / "13_flag_distribution.csv", index=False)

    source_summary = build_source_coverage(s5_df, nc)
    source_summary.to_csv(out_dir / "14_source_coverage_summary.csv", index=False)

    join_summary = build_join_summary(nc, cluster_resolution_df, source_resolution_df, basin_resolution_df)
    join_summary.to_csv(out_dir / "15_cluster_join_summary.csv", index=False)

    overlap_summary, overlap_flag_summary = build_overlap_summary(nc)
    overlap_combined = overlap_summary.copy()
    if len(overlap_flag_summary):
        overlap_flag_summary = overlap_flag_summary.copy()
        overlap_flag_summary["value"] = overlap_flag_summary["record_count"]
        overlap_combined = pd.concat([overlap_combined, overlap_flag_summary], ignore_index=True, sort=False)
    overlap_combined.to_csv(out_dir / "11_overlap_provenance_summary.csv", index=False)

    point_polygon_summary, point_polygon_detail = build_point_polygon_check(
        cluster_resolution_gdf,
        basin_resolution_gdf,
        source_resolution_gdf=source_resolution_gdf,
    )
    point_polygon_summary.to_csv(out_dir / "16_cluster_point_polygon_check.csv", index=False)
    point_polygon_detail.to_csv(out_dir / "16_cluster_point_polygon_detail.csv", index=False)

    basin_geometry_quality = build_basin_geometry_quality(basin_resolution_gdf)
    basin_geometry_quality.to_csv(out_dir / "17_basin_geometry_quality.csv", index=False)

    path_summary, path_detail = build_path_check(nc)
    path_summary.to_csv(out_dir / "18_provenance_path_summary.csv", index=False)
    path_detail.to_csv(out_dir / "18_provenance_path_check.csv", index=False)

    cluster_count_nc = int(nc["dims"].get("n_stations", 0))
    source_station_count_nc = int(nc["dims"].get("n_source_stations", 0))
    record_count_nc = int(nc["dims"].get("n_records", 0))
    cluster_resolution_count_nc = int(len(nc["cluster_resolution_df"]))
    source_resolution_count_nc = int(len(nc["source_resolution_df"]))
    cluster_resolution_count_gpkg = int(len(cluster_resolution_df))
    source_resolution_count_gpkg = int(len(source_resolution_df))
    basin_resolution_count_gpkg = int(len(basin_resolution_df))
    s4_failed = int(s4_df["basin_id"].isna().sum())

    observed_resolutions = set(resolution_summary.loc[resolution_summary["record_count"] > 0, "resolution_name"].tolist())
    single_point_present = bool((safe_series_to_str(s5_df["resolution"]) == "single_point").any()) or bool(pd.Series(nc["source_station_resolutions"]).str.contains(r"(^|\\|)single_point($|\\|)", regex=True).any())
    quarterly_present = bool((safe_series_to_str(s5_df["resolution"]) == "quarterly").any()) or bool(pd.Series(nc["source_station_resolutions"]).str.contains(r"(^|\\|)quarterly($|\\|)", regex=True).any())

    resolution_counts = dict(zip(resolution_summary["resolution_name"], resolution_summary["record_count"]))
    annual_count = int(resolution_counts.get("annual", 0))
    climatology_count = int(resolution_counts.get("climatology", 0))
    climatology_total_records = climatology_count + int(clim_nc["n_records"])
    clusters_with_both = 0
    if annual_count > 0 and climatology_count > 0:
        annual_clusters = set(nc["station_index"][nc["resolution_labels"] == "annual"].tolist())
        clim_clusters = set(nc["station_index"][nc["resolution_labels"] == "climatology"].tolist())
        clusters_with_both = len(annual_clusters & clim_clusters)

    source_station_cluster_index = nc["source_station_cluster_index"]
    mapped_clusters = set(source_station_cluster_index[source_station_cluster_index >= 0].tolist())
    missing_cluster_source_map = cluster_count_nc - len(mapped_clusters)

    missing_source_station_records = int(np.sum(nc["source_station_index"] < 0))
    overlap_records = int(np.sum(nc["is_overlap"] == 1))
    overlap_missing_source_idx = int(np.sum((nc["is_overlap"] == 1) & (nc["source_station_index"] < 0)))

    cluster_key_set = _cluster_key_set(cluster_resolution_df)
    basin_key_set = _cluster_key_set(basin_resolution_df)
    source_cluster_key_set = _cluster_key_set(source_resolution_df[["cluster_uid", "resolution"]].drop_duplicates()) if len(source_resolution_df) else set()
    nc_cluster_key_set = _cluster_key_set(nc["cluster_resolution_df"])
    source_uid_resolution_count = int(source_resolution_df[["source_station_uid", "resolution"]].drop_duplicates().shape[0]) if len(source_resolution_df) else 0

    file_mtimes = dataset_summary[dataset_summary["dataset"].isin(CORE_OUTPUT_LABELS) & dataset_summary["exists"]]["mtime"]
    if len(file_mtimes):
        mtimes = pd.to_datetime(file_mtimes)
        time_span_hours = (mtimes.max() - mtimes.min()).total_seconds() / 3600.0
    else:
        time_span_hours = np.nan

    invalid_geometry_total = _metric_value(basin_geometry_quality, "invalid_geometry_count", "all")
    empty_geometry_total = _metric_value(basin_geometry_quality, "empty_geometry_count", "all")

    missing_basin_keys = len(cluster_key_set - basin_key_set)
    missing_cluster_keys_from_nc = len(nc_cluster_key_set - cluster_key_set)
    source_keys_not_in_cluster = len(source_cluster_key_set - cluster_key_set)
    outside_or_missing = int(
        point_polygon_detail["relation"].isin(["outside", "missing_polygon"]).sum()
    ) if "relation" in point_polygon_detail.columns else 0

    results = [
        make_result(
            "A01",
            "pass" if dataset_summary.loc[dataset_summary["dataset"].isin(CORE_OUTPUT_LABELS), "exists"].all() else "fail",
            "all core outputs exist and are non-empty" if dataset_summary.loc[dataset_summary["dataset"].isin(CORE_OUTPUT_LABELS), "exists"].all() else "some core outputs are missing",
            evidence="; ".join("{}={}MB".format(r.dataset, r.size_mb) for r in dataset_summary.itertuples(index=False) if r.dataset in CORE_OUTPUT_LABELS),
            output_file="00_dataset_summary.csv",
        ),
        make_result(
            "A02",
            "pass" if pd.notna(time_span_hours) and time_span_hours <= 24 else "warn",
            "core output timestamps are tightly grouped" if pd.notna(time_span_hours) and time_span_hours <= 24 else "core output timestamps span more than 24 hours",
            evidence="time_span_hours={:.2f}".format(float(time_span_hours)) if pd.notna(time_span_hours) else "timestamp missing",
            output_file="00_dataset_summary.csv",
        ),
        make_result(
            "A03",
            "pass" if cluster_resolution_count_nc == cluster_resolution_count_gpkg and basin_resolution_count_gpkg <= cluster_resolution_count_gpkg else "warn",
            "cluster resolution counts are structurally consistent" if cluster_resolution_count_nc == cluster_resolution_count_gpkg and basin_resolution_count_gpkg <= cluster_resolution_count_gpkg else "cluster/basin resolution counts have gaps",
            evidence="nc_cluster_resolution_rows={}, cluster_points_rows={}, cluster_basins_rows={}, s4_failed={}".format(cluster_resolution_count_nc, cluster_resolution_count_gpkg, basin_resolution_count_gpkg, s4_failed),
            output_file="15_cluster_join_summary.csv",
        ),
        make_result(
            "A04",
            "pass" if source_resolution_count_nc == source_resolution_count_gpkg else "fail",
            "nc source resolution rows match source station GPKG" if source_resolution_count_nc == source_resolution_count_gpkg else "nc/source station resolution counts mismatch",
            evidence="nc_source_resolution_rows={}, source_gpkg_rows={}, source_uid_resolution_nunique={}".format(source_resolution_count_nc, source_resolution_count_gpkg, source_uid_resolution_count),
            output_file="15_cluster_join_summary.csv",
        ),
        make_result(
            "A05",
            "pass" if observed_resolutions.issubset({"daily", "monthly", "annual", "climatology"}) or (
                clim_nc["exists"] and observed_resolutions.issubset({"daily", "monthly", "annual", "other"})
            ) else "warn",
            "main nc uses expected time classes; climatology may be stored separately" if observed_resolutions.issubset({"daily", "monthly", "annual", "climatology"}) or (
                clim_nc["exists"] and observed_resolutions.issubset({"daily", "monthly", "annual", "other"})
            ) else "final records still contain unexpected time classes",
            evidence="main_observed={} ; separate_climatology_nc_records={}".format("|".join(sorted(observed_resolutions)), int(clim_nc["n_records"])),
            output_file="10_resolution_record_counts.csv",
        ),
        make_result(
            "A06",
            "pass" if not single_point_present else "fail",
            "single_point has been merged into daily" if not single_point_present else "single_point still appears in final classified outputs",
            evidence="single_point_present={}".format(single_point_present),
            output_file="10_resolution_record_counts.csv",
        ),
        make_result(
            "A07",
            "pass" if not quarterly_present else "fail",
            "quarterly has been merged into monthly" if not quarterly_present else "quarterly still appears in final classified outputs",
            evidence="quarterly_present={}".format(quarterly_present),
            output_file="10_resolution_record_counts.csv",
        ),
        make_result(
            "A08",
            "pass" if annual_count > 0 and climatology_total_records > 0 else "fail",
            "annual and climatology are both retained" if annual_count > 0 and climatology_total_records > 0 else "annual or climatology is missing in final outputs",
            evidence="annual_records={}, climatology_records_main_nc={}, climatology_records_separate_nc={}, clusters_with_both_main_nc={}".format(
                annual_count, climatology_count, int(clim_nc["n_records"]), clusters_with_both
            ),
            output_file="10_resolution_record_counts.csv",
        ),
        make_result(
            "A09",
            "pass" if missing_cluster_source_map == 0 else "fail",
            "every cluster maps to at least one source station" if missing_cluster_source_map == 0 else "some clusters have no source-station mapping",
            evidence="clusters_without_source_station_map={}".format(missing_cluster_source_map),
            output_file="15_cluster_join_summary.csv",
        ),
        make_result(
            "A10",
            "pass" if missing_source_station_records == 0 else "fail",
            "every final record maps back to a source station" if missing_source_station_records == 0 else "some final records are missing source_station_index",
            evidence="missing_source_station_index_records={} of {}".format(missing_source_station_records, record_count_nc),
            output_file="11_overlap_provenance_summary.csv",
        ),
        make_result(
            "A11",
            "pass" if cluster_key_set.issubset(basin_key_set) else "warn",
            "every cluster resolution row has a basin polygon" if cluster_key_set.issubset(basin_key_set) else "some cluster resolution rows have no basin polygon",
            evidence="missing_cluster_resolution_basins={}".format(missing_basin_keys),
            output_file="15_cluster_join_summary.csv",
        ),
        make_result(
            "A12",
            "pass" if outside_or_missing == 0 else "warn",
            "cluster points are covered by their basin polygons" if outside_or_missing == 0 else "some cluster points fall outside or lack basin polygons",
            evidence="; ".join("{}|{}={}".format(r.resolution, r.relation, r.cluster_count) for r in point_polygon_summary.itertuples(index=False)) if len(point_polygon_summary) else "no polygon check rows",
            output_file="16_cluster_point_polygon_check.csv",
            manual_required=True,
            manual_hint="在 GIS 中优先复核 outside 和 missing_polygon 的 cluster_uid + resolution。",
        ),
        make_result(
            "A13",
            "pass" if overlap_missing_source_idx == 0 else "fail",
            "all overlap records keep record-level provenance" if overlap_missing_source_idx == 0 else "some overlap records cannot trace back to a source station",
            evidence="overlap_records={}, overlap_missing_source_station_index={}".format(overlap_records, overlap_missing_source_idx),
            output_file="11_overlap_provenance_summary.csv",
        ),
        make_result(
            "A14",
            "manual",
            "needs manual judgement on whether the chosen overlap source is reasonable",
            evidence="use overlap flag summary and overlap queue",
            output_file="06_overlap_cluster_queue.csv; 11_overlap_provenance_summary.csv",
            manual_required=True,
            manual_hint="优先抽查 overlap 记录最多、且 flag 不是全 good 的 cluster。",
        ),
        make_result(
            "B05",
            "pass" if invalid_geometry_total == 0 and empty_geometry_total == 0 else "warn",
            "cluster basin geometry has no invalid or empty polygons" if invalid_geometry_total == 0 and empty_geometry_total == 0 else "some basin polygons are invalid or empty",
            evidence="invalid_geometry_count={}, empty_geometry_count={}".format(invalid_geometry_total, empty_geometry_total),
            output_file="17_basin_geometry_quality.csv",
            manual_required=True,
            manual_hint="脚本只检查几何有效性，属性是否合理还需在 GIS 里看。",
        ),
        make_result(
            "B06",
            "pass" if var_summary["non_missing_records"].gt(0).all() else "fail",
            "Q/SSC/SSL all have non-missing records" if var_summary["non_missing_records"].gt(0).all() else "at least one core variable has zero non-missing records",
            evidence="; ".join("{}={:.2%}".format(r.variable, r.non_missing_fraction) for r in var_summary.itertuples(index=False)),
            output_file="12_variable_completeness.csv",
        ),
        make_result(
            "B07",
            "warn" if flag_summary[flag_summary["flag_name"].isin(["bad", "missing"])]["record_fraction"].max() > 0.2 else "pass",
            "flag distribution is mostly in acceptable categories" if flag_summary[flag_summary["flag_name"].isin(["bad", "missing"])]["record_fraction"].max() <= 0.2 else "bad/missing flags are substantial for at least one variable",
            evidence="worst_bad_or_missing_fraction={:.2%}".format(float(flag_summary[flag_summary["flag_name"].isin(["bad", "missing"])]["record_fraction"].max())),
            output_file="13_flag_distribution.csv",
        ),
        make_result(
            "B08",
            "pass" if source_summary["in_s5"].equals(source_summary["in_s6_source_table"]) else "warn",
            "source tables are broadly consistent between s5 and s6" if source_summary["in_s5"].equals(source_summary["in_s6_source_table"]) else "some sources appear in s5 but not clearly in s6 source tables",
            evidence="sources_in_s5_not_in_s6={}".format(int(((source_summary["in_s5"]) & (~source_summary["in_s6_source_table"])).sum())),
            output_file="14_source_coverage_summary.csv",
        ),
        make_result(
            "B09",
            "pass" if missing_cluster_keys_from_nc == 0 and missing_basin_keys == 0 else "warn",
            "cluster resolution joins are stable across nc, points and basins" if missing_cluster_keys_from_nc == 0 and missing_basin_keys == 0 else "cluster resolution join has gaps in at least one layer",
            evidence="nc_minus_cluster_points={}, cluster_points_minus_basins={}".format(missing_cluster_keys_from_nc, missing_basin_keys),
            output_file="15_cluster_join_summary.csv",
        ),
        make_result(
            "B10",
            "pass" if source_keys_not_in_cluster == 0 else "fail",
            "every source-station GPKG row links back to a cluster point layer" if source_keys_not_in_cluster == 0 else "some source station rows cannot link back to cluster points",
            evidence="source_cluster_resolution_keys_not_in_cluster_points={}".format(source_keys_not_in_cluster),
            output_file="15_cluster_join_summary.csv",
        ),
        make_result(
            "C03",
            "pass" if invalid_geometry_total == 0 else "warn",
            "basin polygon geometries are valid" if invalid_geometry_total == 0 else "some basin polygon geometries are invalid",
            evidence="invalid_geometry_count={}".format(invalid_geometry_total),
            output_file="17_basin_geometry_quality.csv",
        ),
        make_result(
            "C05",
            "pass" if missing_source_station_records == 0 and int(path_summary.loc[path_summary["metric"] == "missing_path_entries", "value"].iloc[0]) == 0 else "warn",
            "record-to-file provenance is structurally complete" if missing_source_station_records == 0 and int(path_summary.loc[path_summary["metric"] == "missing_path_entries", "value"].iloc[0]) == 0 else "record-to-file provenance has gaps",
            evidence="missing_source_station_index_records={}, missing_path_entries={}".format(missing_source_station_records, int(path_summary.loc[path_summary["metric"] == "missing_path_entries", "value"].iloc[0])),
            output_file="18_provenance_path_summary.csv",
            manual_required=True,
            manual_hint="脚本只验证链路是否存在，是否真的语义正确仍需抽样看。",
        ),
        make_result(
            "C06",
            "pass" if int(path_summary.loc[path_summary["metric"] == "missing_path_entries", "value"].iloc[0]) == 0 else "warn",
            "source-station paths are available for reverse tracing" if int(path_summary.loc[path_summary["metric"] == "missing_path_entries", "value"].iloc[0]) == 0 else "some source-station paths are missing",
            evidence="source_station_rows_missing_any_path={}".format(int(path_summary.loc[path_summary["metric"] == "source_station_rows_missing_any_path", "value"].iloc[0])),
            output_file="18_provenance_path_check.csv",
            manual_required=True,
            manual_hint="再从路径反查 cluster 是否正确，需要人工抽样。",
        ),
    ]

    auto_df = pd.DataFrame(results)
    merged = checklist.merge(auto_df, on="check_id", how="left")
    merged["auto_status"] = merged["auto_status"].fillna("manual")
    merged["manual_required"] = merged["manual_required"].fillna("yes")
    merged["output_file"] = merged["output_file"].fillna("")
    merged["auto_summary"] = merged["auto_summary"].fillna("requires manual review")
    merged["evidence"] = merged["evidence"].fillna("")
    merged["manual_hint"] = merged["manual_hint"].fillna("")
    merged.to_csv(out_dir / "08_checklist_auto_results.csv", index=False)

    manual_guide = build_manual_guide()
    manual_guide = checklist[["check_id", "priority", "category", "check_item"]].merge(manual_guide, on="check_id", how="left")
    manual_guide = manual_guide[manual_guide["how_to_check"].notna()].copy()
    manual_guide.to_csv(out_dir / "09_manual_check_guide.csv", index=False)

    print("Checklist audit written to {}".format(out_dir))
    print("Loaded GPKG layers:")
    print("  cluster: {}".format(", ".join(cluster_layers)))
    print("  source: {}".format(", ".join(source_layers)))
    print("  basin: {}".format(", ".join(basin_layers)))
    print("Main outputs:")
    for name in [
        "08_checklist_auto_results.csv",
        "09_manual_check_guide.csv",
        "10_resolution_record_counts.csv",
        "11_overlap_provenance_summary.csv",
        "12_variable_completeness.csv",
        "13_flag_distribution.csv",
        "14_source_coverage_summary.csv",
        "15_cluster_join_summary.csv",
        "16_cluster_point_polygon_check.csv",
        "17_basin_geometry_quality.csv",
        "18_provenance_path_summary.csv",
    ]:
        print("  - {}".format(out_dir / name))


if __name__ == "__main__":
    main()
