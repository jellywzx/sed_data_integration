#!/usr/bin/env python3
"""
流域合并站点工具：
读取 basin_tracer 结果（含 station_id, basin_id），构建
station_id -> basin-merged cluster_id 映射。

合并规则：
1) 仅 basin_status=resolved 且 basin_id 有效的站点可参与合并；
2) 同一 basin 内仅当 cluster 间所有跨组 pair 都满足：
   - 距离 <= max_station_distance_m
   - upstream area 对称相对误差 <= max_upstream_rel_error
   才允许合并（complete-linkage 风格）；
3) 不满足条件的站点保留 singleton（cluster_id=station_id）。
"""

import math
from pathlib import Path

import pandas as pd


def _haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in meters."""
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _symmetric_rel_error(a: float, b: float) -> float:
    """abs(a - b) / max(abs(a), abs(b))."""
    denom = max(abs(a), abs(b))
    if denom == 0.0:
        return 0.0 if a == b else float("inf")
    return abs(a - b) / denom


def _can_merge_clusters(
    left_cluster,
    right_cluster,
    station_meta,
    max_station_distance_m: float,
    max_upstream_rel_error: float,
) -> bool:
    """Complete-linkage check: all cross-cluster station pairs must pass."""
    for sid_l in left_cluster:
        lat_l, lon_l, area_l = station_meta[sid_l]
        for sid_r in right_cluster:
            lat_r, lon_r, area_r = station_meta[sid_r]
            dist_m = _haversine_distance_m(lat_l, lon_l, lat_r, lon_r)
            if dist_m > max_station_distance_m:
                return False
            rel_error = _symmetric_rel_error(area_l, area_r)
            if rel_error > max_upstream_rel_error:
                return False
    return True


def load_station_to_basin_cluster_map(
    basin_csv_path: Path,
    station_df=None,
    max_station_distance_m=5000.0,
    max_upstream_rel_error=0.10,
    upstream_area_col="uparea_merit",
):
    """
    读取 basin_tracer 输出（s6_upstream_basins.csv），生成：
      station_id(cluster_id) -> basin-merged cluster_id 映射。

    返回：
      mapping: dict[int, int]
      stats: {
        "n_station": int,   # 输入中唯一 station 数
        "n_success": int,   # resolved 且有 basin_id 的 station 数
        "n_basins": int,    # 唯一 basin 数
        "n_clusters_from_basins": int,  # basin 侧最终聚类数量
        "n_changed": int,   # station_id 被重映射数量
        "max_station_distance_m": float,
        "max_upstream_rel_error": float,
        "upstream_area_col": str,
      }
    """
    basin_csv_path = Path(basin_csv_path)
    if not basin_csv_path.is_file():
        raise FileNotFoundError("Basin CSV not found: {}".format(basin_csv_path))

    df = pd.read_csv(basin_csv_path)
    if "station_id" not in df.columns or "basin_id" not in df.columns:
        raise ValueError("Basin CSV must contain columns: station_id, basin_id")

    df = df.dropna(subset=["station_id"]).copy()
    if len(df) == 0:
        return {}, {
            "n_station": 0,
            "n_success": 0,
            "n_basins": 0,
            "n_clusters_from_basins": 0,
            "n_changed": 0,
            "max_station_distance_m": float(max_station_distance_m),
            "max_upstream_rel_error": float(max_upstream_rel_error),
            "upstream_area_col": str(upstream_area_col),
        }

    df["station_id"] = df["station_id"].astype(int)
    df = df.drop_duplicates(subset=["station_id"], keep="first").copy()

    if station_df is not None:
        required_cols = {"station_id", "lat", "lon"}
        missing = required_cols.difference(station_df.columns)
        if missing:
            raise ValueError(
                "station_df must contain columns: station_id, lat, lon; missing={}".format(
                    sorted(missing)
                )
            )
        station_loc = station_df[["station_id", "lat", "lon"]].dropna(subset=["station_id"]).copy()
        station_loc["station_id"] = station_loc["station_id"].astype(int)
        station_loc = station_loc.drop_duplicates(subset=["station_id"], keep="first")
        df = df.merge(station_loc, on="station_id", how="left", suffixes=("", "_station"))
        for c in ["lat", "lon"]:
            station_c = "{}_station".format(c)
            if station_c in df.columns:
                if c in df.columns:
                    df[c] = df[c].where(df[c].notna(), df[station_c])
                else:
                    df[c] = df[station_c]
                df = df.drop(columns=[station_c])

    resolved_mask = (
        df["basin_status"].fillna("").astype(str).str.strip().str.lower().eq("resolved")
        if "basin_status" in df.columns
        else pd.Series(False, index=df.index)
    )
    ok = df[resolved_mask].copy()
    ok["basin_id"] = pd.to_numeric(ok["basin_id"], errors="coerce")
    ok = ok.dropna(subset=["basin_id"]).copy()
    ok["basin_id"] = ok["basin_id"].astype("int64")

    mapping = {sid: sid for sid in df["station_id"].unique().tolist()}
    n_clusters_from_basins = 0
    if len(ok) > 0 and upstream_area_col in ok.columns:
        ok["lat"] = pd.to_numeric(ok.get("lat"), errors="coerce")
        ok["lon"] = pd.to_numeric(ok.get("lon"), errors="coerce")
        ok[upstream_area_col] = pd.to_numeric(ok[upstream_area_col], errors="coerce")
        candidates = ok.dropna(subset=["lat", "lon", upstream_area_col]).copy()

        for _, grp in candidates.groupby("basin_id"):
            rows = grp[["station_id", "lat", "lon", upstream_area_col]].drop_duplicates(
                subset=["station_id"]
            )
            if len(rows) == 0:
                continue

            station_meta = {
                int(r["station_id"]): (
                    float(r["lat"]),
                    float(r["lon"]),
                    float(r[upstream_area_col]),
                )
                for _, r in rows.iterrows()
            }
            clusters = [{sid} for sid in sorted(station_meta)]

            merged = True
            while merged:
                merged = False
                for i in range(len(clusters)):
                    for j in range(i + 1, len(clusters)):
                        if _can_merge_clusters(
                            clusters[i],
                            clusters[j],
                            station_meta,
                            float(max_station_distance_m),
                            float(max_upstream_rel_error),
                        ):
                            clusters[i] = clusters[i] | clusters[j]
                            del clusters[j]
                            merged = True
                            break
                    if merged:
                        break

            n_clusters_from_basins += len(clusters)
            for cluster in clusters:
                rep = min(cluster)
                for sid in cluster:
                    mapping[sid] = rep

    n_changed = int(sum(1 for sid, rep in mapping.items() if sid != rep))
    stats = {
        "n_station": int(df["station_id"].nunique()),
        "n_success": int(ok["station_id"].nunique()),
        "n_basins": int(ok["basin_id"].nunique()),
        "n_clusters_from_basins": int(n_clusters_from_basins),
        "n_changed": n_changed,
        "max_station_distance_m": float(max_station_distance_m),
        "max_upstream_rel_error": float(max_upstream_rel_error),
        "upstream_area_col": str(upstream_area_col),
    }
    return mapping, stats
