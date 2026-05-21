#!/usr/bin/env python3
"""
s9：生成适合人工抽查的 CSV 表格。

输出目录默认：
  scripts_basin_test/output/manual_review/

主要输出：
  00_dataset_summary.csv
  01_linkage_summary.csv
  02_resolution_summary.csv
  03_priority_cluster_queue.csv
  04_random_cluster_queue.csv
  05_multi_resolution_clusters.csv
  06_overlap_cluster_queue.csv
  07_missing_basin_queue.csv
  19_basin_distance_review_queue.csv
  20_unresolved_basin_queue.csv

目标：
1. 先把最值得人工检查的 cluster 自动挑出来；
2. 所有输出都用 CSV，方便在 VSCode 中直接筛选和编辑。
"""

import argparse
import math
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from pipeline_paths import (
    S4_UPSTREAM_GPKG,
    S5_BASIN_CLUSTERED_CSV,
    S6_MERGED_NC,
    S7_CLUSTER_BASINS_GPKG,
    S7_CLUSTER_POINTS_GPKG,
    S7_SOURCE_STATIONS_GPKG,
    get_output_r_root,
)

try:
    import shapefile
    HAS_PYSHP = True
except ImportError:
    HAS_PYSHP = False


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_S5 = ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_S6 = ROOT / S6_MERGED_NC
DEFAULT_S4_GPKG = ROOT / S4_UPSTREAM_GPKG
DEFAULT_CLUSTER_GPKG = ROOT / S7_CLUSTER_POINTS_GPKG
DEFAULT_SOURCE_SHP = ROOT / S7_SOURCE_STATIONS_GPKG
DEFAULT_CLUSTER_BASIN_SHP = ROOT / S7_CLUSTER_BASINS_GPKG
DEFAULT_OUT_DIR = ROOT / "scripts_basin_test/output/manual_review"


def open_netcdf_dataset(path: Path):
    """
    Open a NetCDF file with a tolerant engine fallback.

    On node114 the wzx env has netCDF4 but not h5netcdf, while some other
    environments may be the opposite. This helper keeps the script portable.
    """
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


def _safe_text(value, maxlen=160):
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()[:maxlen]


def _join_unique(values, max_items=6, maxlen=200):
    vals = []
    seen = set()
    for value in values:
        s = _safe_text(value, maxlen=120)
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        vals.append(s)
        if len(vals) >= max_items:
            break
    return "|".join(vals)[:maxlen]


def _nonempty_nunique(series):
    s = series.fillna("").astype(str).str.strip()
    s = s[s != ""]
    return int(s.nunique())


def _maybe_count_shapefile(path: Path):
    if not path.is_file():
        return np.nan, "missing"
    if not HAS_PYSHP:
        return np.nan, "pyshp_not_installed"
    try:
        reader = shapefile.Reader(str(path))
        return len(reader), "ok"
    except Exception as exc:
        return np.nan, "error: {}".format(exc)


def _count_gpkg_features(path: Path):
    if not path.is_file():
        return np.nan, "missing"
    try:
        with sqlite3.connect(str(path)) as conn:
            tables = pd.read_sql_query(
                "SELECT table_name FROM gpkg_contents WHERE data_type = 'features'",
                conn,
            )
            if len(tables) == 0:
                return 0, "ok_empty"
            total = 0
            for table_name in tables["table_name"].tolist():
                sql = 'SELECT COUNT(*) AS n FROM "{}"'.format(table_name.replace('"', '""'))
                total += int(pd.read_sql_query(sql, conn)["n"].iloc[0])
            return total, "ok"
    except Exception as exc:
        return np.nan, "error: {}".format(exc)


def load_s5_cluster_stats(path: Path):
    usecols = [
        "station_id",
        "path",
        "source",
        "lat",
        "lon",
        "resolution",
        "station_name",
        "river_name",
        "source_station_id",
        "cluster_id",
        "basin_id",
        "basin_area",
        "match_quality",
        "area_error",
        "uparea_merit",
        "pfaf_code",
        "method",
        "distance_m",
        "point_in_local",
        "point_in_basin",
        "basin_status",
        "basin_flag",
        "n_upstream_reaches",
    ]
    df = pd.read_csv(path, usecols=usecols)
    df["station_id"] = pd.to_numeric(df["station_id"], errors="coerce").astype("Int64")
    df["cluster_id"] = pd.to_numeric(df["cluster_id"], errors="coerce").astype("Int64")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    if "distance_m" in df.columns:
        df["distance_m"] = pd.to_numeric(df["distance_m"], errors="coerce")
    df = df.dropna(subset=["cluster_id"]).copy()

    rep = df.copy()
    rep["_is_rep"] = (
        pd.to_numeric(rep["station_id"], errors="coerce") == pd.to_numeric(rep["cluster_id"], errors="coerce")
    ).astype(int)
    rep = rep.sort_values(["cluster_id", "_is_rep", "station_id"], ascending=[True, False, True])
    rep = rep.drop_duplicates(subset=["cluster_id"], keep="first").copy()

    grp = df.groupby("cluster_id")

    cluster_stats = pd.DataFrame({
        "cluster_id": grp.size().index.astype(int),
        "n_station_rows": grp.size().values.astype(int),
        "n_sources": grp["source"].nunique().values.astype(int),
        "n_resolutions": grp["resolution"].nunique().values.astype(int),
        "n_station_names": grp["station_name"].apply(_nonempty_nunique).values.astype(int),
        "n_river_names": grp["river_name"].apply(_nonempty_nunique).values.astype(int),
        "n_source_station_ids": grp["source_station_id"].apply(_nonempty_nunique).values.astype(int),
        "n_basin_ids": grp["basin_id"].nunique(dropna=True).values.astype(int),
        "lat_min": grp["lat"].min().values,
        "lat_max": grp["lat"].max().values,
        "lon_min": grp["lon"].min().values,
        "lon_max": grp["lon"].max().values,
        "basin_area_min": grp["basin_area"].min().values,
        "basin_area_max": grp["basin_area"].max().values,
    })
    cluster_stats["lat_span"] = cluster_stats["lat_max"] - cluster_stats["lat_min"]
    cluster_stats["lon_span"] = cluster_stats["lon_max"] - cluster_stats["lon_min"]
    cluster_stats["cluster_uid"] = cluster_stats["cluster_id"].map(lambda x: "SED{:06d}".format(int(x)))

    extra = grp.apply(
        lambda g: pd.Series({
            "sources": _join_unique(g["source"]),
            "resolutions": _join_unique(g["resolution"]),
            "station_names": _join_unique(g["station_name"]),
            "river_names": _join_unique(g["river_name"]),
            "basin_ids": _join_unique(g["basin_id"]),
            "match_qualities": _join_unique(g["match_quality"]),
            "methods": _join_unique(g["method"]),
            "example_paths": _join_unique(g["path"], max_items=3, maxlen=260),
            "example_station_ids": _join_unique(g["station_id"], max_items=6),
            "example_source_station_ids": _join_unique(g["source_station_id"], max_items=6),
            "basin_statuses": _join_unique(g["basin_status"]),
            "basin_flags": _join_unique(g["basin_flag"]),
        })
    ).reset_index()

    cluster_stats = cluster_stats.merge(extra, on="cluster_id", how="left")
    rep_cols = [
        "cluster_id",
        "distance_m",
        "point_in_local",
        "point_in_basin",
        "basin_status",
        "basin_flag",
    ]
    for col in rep_cols:
        if col not in rep.columns:
            rep[col] = np.nan if col == "distance_m" else ""
    rep_subset = rep[rep_cols].copy()
    cluster_stats = cluster_stats.merge(rep_subset, on="cluster_id", how="left")
    return df, cluster_stats.sort_values("cluster_id").reset_index(drop=True)


def load_s6_stats(path: Path):
    ds = open_netcdf_dataset(path)
    try:
        dims = {k: int(v) for k, v in ds.sizes.items()}

        cluster_id = ds["cluster_id"].values.astype(np.int64)
        cluster_uid_raw = ds["cluster_uid"].values if "cluster_uid" in ds.variables else np.array([], dtype=object)
        cluster_uid = np.array([
            x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)
            for x in cluster_uid_raw
        ], dtype=object) if len(cluster_uid_raw) else np.array(
            ["SED{:06d}".format(int(v)) for v in cluster_id], dtype=object
        )

        station_index = ds["station_index"].values.astype(np.int64)
        resolution = ds["resolution"].values.astype(np.int64) if "resolution" in ds.variables else np.array([], dtype=np.int64)
        is_overlap = ds["is_overlap"].values.astype(np.int64) if "is_overlap" in ds.variables else np.zeros_like(station_index)
        src_cluster_idx = (
            ds["source_station_cluster_index"].values.astype(np.int64)
            if "source_station_cluster_index" in ds.variables
            else np.array([], dtype=np.int64)
        )

        n_stations = int(dims.get("n_stations", len(cluster_id)))
        total_records = np.bincount(station_index, minlength=n_stations)
        overlap_records = np.bincount(station_index[is_overlap == 1], minlength=n_stations)
        source_station_counts = (
            np.bincount(src_cluster_idx[src_cluster_idx >= 0], minlength=n_stations)
            if len(src_cluster_idx) else np.zeros(n_stations, dtype=np.int64)
        )

        nc_station_stats = pd.DataFrame({
            "cluster_id": cluster_id.astype(int),
            "cluster_uid": cluster_uid,
            "nc_total_records": total_records.astype(int),
            "nc_overlap_records": overlap_records.astype(int),
            "nc_source_station_count": source_station_counts.astype(int),
        })
        nc_station_stats["nc_overlap_fraction"] = np.where(
            nc_station_stats["nc_total_records"] > 0,
            nc_station_stats["nc_overlap_records"] / nc_station_stats["nc_total_records"],
            0.0,
        )

        res_flag_meanings = ""
        if "resolution" in ds.variables:
            res_flag_meanings = ds["resolution"].attrs.get("flag_meanings", "")
        code_map = {}
        if res_flag_meanings:
            meanings = str(res_flag_meanings).split()
            for i, meaning in enumerate(meanings):
                code_map[i] = meaning
        else:
            code_map = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"}

        if len(resolution):
            unique_codes, counts = np.unique(resolution, return_counts=True)
            res_summary = pd.DataFrame({
                "resolution_code": unique_codes.astype(int),
                "resolution_name": [code_map.get(int(x), "unknown") for x in unique_codes],
                "record_count": counts.astype(int),
            })
            res_summary["record_fraction"] = res_summary["record_count"] / max(int(counts.sum()), 1)
        else:
            res_summary = pd.DataFrame(columns=["resolution_code", "resolution_name", "record_count", "record_fraction"])

        return dims, nc_station_stats, res_summary
    finally:
        ds.close()


def build_priority_queue(cluster_stats: pd.DataFrame):
    rows = []
    for row in cluster_stats.itertuples(index=False):
        reasons = []
        score = 0

        if row.n_station_rows >= 5:
            reasons.append("many_station_rows")
            score += 3
        if row.n_sources >= 3:
            reasons.append("many_sources")
            score += 3
        if row.n_resolutions >= 3:
            reasons.append("many_resolutions")
            score += 2
        if row.n_river_names >= 2:
            reasons.append("multiple_river_names")
            score += 4
        if row.n_station_names >= 3:
            reasons.append("multiple_station_names")
            score += 2
        if row.n_basin_ids >= 2:
            reasons.append("multiple_basin_ids")
            score += 5
        if str(getattr(row, "basin_status", "")).strip().lower() != "resolved":
            reasons.append("unresolved_basin")
            score += 5
        if "large_offset" in str(getattr(row, "basin_flags", "")).lower():
            reasons.append("large_offset")
            score += 4
        if "area_mismatch" in str(getattr(row, "basin_flags", "")).lower():
            reasons.append("area_mismatch")
            score += 4
        if pd.isna(row.basin_area_min) or pd.isna(row.basin_area_max):
            reasons.append("missing_basin_area")
            score += 4
        if "failed" in str(row.match_qualities).lower() or "unknown" in str(row.match_qualities).lower():
            reasons.append("weak_match_quality")
            score += 3
        if getattr(row, "nc_overlap_records", 0) > 0:
            reasons.append("has_overlap")
            score += 1 + min(4, int(math.ceil(getattr(row, "nc_overlap_records", 0) / 1000.0)))
        if getattr(row, "nc_overlap_fraction", 0.0) >= 0.10:
            reasons.append("high_overlap_fraction")
            score += 2
        if row.lat_span > 1.0 or row.lon_span > 1.0:
            reasons.append("large_spatial_span")
            score += 2

        if reasons:
            record = row._asdict()
            record["review_score"] = score
            record["review_reasons"] = "|".join(reasons)
            rows.append(record)

    if not rows:
        return pd.DataFrame(columns=list(cluster_stats.columns) + ["review_score", "review_reasons"])

    out = pd.DataFrame(rows)
    return out.sort_values(
        ["review_score", "n_station_rows", "n_sources", "nc_overlap_records"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def build_distance_review_queue(cluster_stats: pd.DataFrame, per_bin=50, seed=42):
    work = cluster_stats.copy()
    work["distance_m"] = pd.to_numeric(work.get("distance_m", np.nan), errors="coerce")
    work["distance_bin"] = pd.cut(
        work["distance_m"],
        bins=[-np.inf, 300.0, 1000.0, np.inf],
        labels=["<=300m", "300-1000m", ">1000m"],
    )
    work = work.dropna(subset=["distance_bin"]).copy()
    if len(work) == 0:
        return work

    samples = []
    for label in ["<=300m", "300-1000m", ">1000m"]:
        subset = work[work["distance_bin"] == label].copy()
        if len(subset) == 0:
            continue
        n_take = min(per_bin, len(subset))
        subset = subset.sample(n_take, random_state=seed).sort_values("cluster_id")
        subset["sample_bin"] = label
        samples.append(subset)
    if not samples:
        return work.iloc[0:0].copy()
    return pd.concat(samples, ignore_index=True, sort=False)


def write_dataset_summary(out_dir: Path, files_info):
    rows = []
    for label, path in files_info:
        rows.append({
            "dataset": label,
            "path": str(path),
            "exists": path.is_file(),
            "size_mb": round(path.stat().st_size / 1024 / 1024, 3) if path.is_file() else np.nan,
        })
    pd.DataFrame(rows).to_csv(out_dir / "00_dataset_summary.csv", index=False)


def write_linkage_summary(out_dir: Path, s5_df: pd.DataFrame, cluster_stats: pd.DataFrame, nc_dims, nc_stats, extra_counts):
    rows = [
        {"item": "s5_station_rows", "value": int(len(s5_df)), "note": "rows in s5_basin_clustered_stations.csv"},
        {"item": "s5_cluster_count", "value": int(cluster_stats["cluster_id"].nunique()), "note": "unique cluster_id in s5"},
        {"item": "nc_n_stations", "value": int(nc_dims.get("n_stations", np.nan)), "note": "n_stations dimension in nc"},
        {"item": "nc_n_source_stations", "value": int(nc_dims.get("n_source_stations", np.nan)), "note": "n_source_stations dimension in nc"},
        {"item": "nc_n_records", "value": int(nc_dims.get("n_records", np.nan)), "note": "n_records dimension in nc"},
        {"item": "cluster_uid_in_nc", "value": int(nc_stats["cluster_uid"].nunique()) if len(nc_stats) else np.nan, "note": "unique cluster_uid in nc station layer"},
    ]
    for label, (count, status) in extra_counts.items():
        rows.append({"item": label, "value": count, "note": status})
    pd.DataFrame(rows).to_csv(out_dir / "01_linkage_summary.csv", index=False)


def main():
    ap = argparse.ArgumentParser(description="生成适合 VSCode 编辑和人工抽查的 CSV 表格")
    ap.add_argument("--s5", default=str(DEFAULT_S5), help="s5 cluster CSV 路径")
    ap.add_argument("--s6", default=str(DEFAULT_S6), help="s6 merged nc 路径")
    ap.add_argument("--s4-gpkg", default=str(DEFAULT_S4_GPKG), help="s4 upstream gpkg 路径")
    ap.add_argument("--cluster-shp", default=str(DEFAULT_CLUSTER_GPKG), help="cluster 点 gpkg 路径")
    ap.add_argument("--source-shp", default=str(DEFAULT_SOURCE_SHP), help="source station shp/gpkg 路径")
    ap.add_argument("--cluster-basin-shp", default=str(DEFAULT_CLUSTER_BASIN_SHP), help="cluster basin shp/gpkg 路径")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="输出目录")
    ap.add_argument("--top-n", type=int, default=200, help="重点队列保留前 N 行")
    ap.add_argument("--random-n", type=int, default=100, help="随机抽样 cluster 数量")
    ap.add_argument("--seed", type=int, default=42, help="随机种子")
    args = ap.parse_args()

    s5_path = Path(args.s5)
    s6_path = Path(args.s6)
    s4_gpkg_path = Path(args.s4_gpkg)
    cluster_shp_path = Path(args.cluster_shp)
    source_shp_path = Path(args.source_shp)
    cluster_basin_shp_path = Path(args.cluster_basin_shp)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not s5_path.is_file():
        raise FileNotFoundError("s5 CSV not found: {}".format(s5_path))
    if not s6_path.is_file():
        raise FileNotFoundError("s6 NC not found: {}".format(s6_path))

    write_dataset_summary(
        out_dir,
        [
            ("s5_cluster_csv", s5_path),
            ("s6_nc", s6_path),
            ("s4_upstream_gpkg", s4_gpkg_path),
            ("cluster_points_gpkg", cluster_shp_path),
            ("source_station_gpkg", source_shp_path),
            ("cluster_basin_gpkg", cluster_basin_shp_path),
        ],
    )

    s5_df, cluster_stats = load_s5_cluster_stats(s5_path)
    nc_dims, nc_stats, res_summary = load_s6_stats(s6_path)
    cluster_stats = cluster_stats.merge(nc_stats, on=["cluster_id", "cluster_uid"], how="left")

    extra_counts = {
        "cluster_vector_records": _count_gpkg_features(cluster_shp_path) if cluster_shp_path.suffix.lower() == ".gpkg" else _maybe_count_shapefile(cluster_shp_path),
        "source_vector_records": _count_gpkg_features(source_shp_path) if source_shp_path.suffix.lower() == ".gpkg" else _maybe_count_shapefile(source_shp_path),
        "cluster_basin_vector_records": _count_gpkg_features(cluster_basin_shp_path) if cluster_basin_shp_path.suffix.lower() == ".gpkg" else _maybe_count_shapefile(cluster_basin_shp_path),
        "s4_upstream_gpkg_features": _count_gpkg_features(s4_gpkg_path),
    }
    write_linkage_summary(out_dir, s5_df, cluster_stats, nc_dims, nc_stats, extra_counts)

    res_summary.to_csv(out_dir / "02_resolution_summary.csv", index=False)

    priority_queue = build_priority_queue(cluster_stats)
    priority_queue.head(args.top_n).to_csv(out_dir / "03_priority_cluster_queue.csv", index=False)

    rng = np.random.default_rng(args.seed)
    sample_n = min(args.random_n, len(cluster_stats))
    if sample_n > 0:
        random_queue = cluster_stats.sample(sample_n, random_state=args.seed).sort_values("cluster_id").reset_index(drop=True)
    else:
        random_queue = cluster_stats.iloc[0:0].copy()
    random_queue.to_csv(out_dir / "04_random_cluster_queue.csv", index=False)

    multi_res = cluster_stats[cluster_stats["n_resolutions"] > 1].copy()
    multi_res = multi_res.sort_values(["n_resolutions", "n_station_rows", "n_sources"], ascending=[False, False, False])
    multi_res.to_csv(out_dir / "05_multi_resolution_clusters.csv", index=False)

    overlap_queue = cluster_stats[cluster_stats["nc_overlap_records"].fillna(0) > 0].copy()
    overlap_queue = overlap_queue.sort_values(["nc_overlap_records", "nc_overlap_fraction"], ascending=[False, False])
    overlap_queue.head(args.top_n).to_csv(out_dir / "06_overlap_cluster_queue.csv", index=False)

    missing_basin = cluster_stats[
        cluster_stats["basin_ids"].fillna("").astype(str).str.strip().eq("")
        | cluster_stats["basin_area_min"].isna()
        | cluster_stats["basin_area_max"].isna()
    ].copy()
    missing_basin = missing_basin.sort_values(["n_station_rows", "n_sources"], ascending=[False, False])
    missing_basin.to_csv(out_dir / "07_missing_basin_queue.csv", index=False)

    distance_review = build_distance_review_queue(cluster_stats, per_bin=50, seed=args.seed)
    distance_review.to_csv(out_dir / "19_basin_distance_review_queue.csv", index=False)

    unresolved = cluster_stats[
        cluster_stats["basin_status"].fillna("").astype(str).str.strip().str.lower() != "resolved"
    ].copy()
    unresolved = unresolved.sort_values(["n_station_rows", "n_sources", "cluster_id"], ascending=[False, False, True])
    unresolved.to_csv(out_dir / "20_unresolved_basin_queue.csv", index=False)

    print("Manual review tables written to: {}".format(out_dir))
    print("Files:")
    for name in [
        "00_dataset_summary.csv",
        "01_linkage_summary.csv",
        "02_resolution_summary.csv",
        "03_priority_cluster_queue.csv",
        "04_random_cluster_queue.csv",
        "05_multi_resolution_clusters.csv",
        "06_overlap_cluster_queue.csv",
        "07_missing_basin_queue.csv",
        "19_basin_distance_review_queue.csv",
        "20_unresolved_basin_queue.csv",
    ]:
        print("  - {}".format(out_dir / name))


if __name__ == "__main__":
    main()
