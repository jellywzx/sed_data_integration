#!/usr/bin/env python3
"""
流域合并站点工具：
读取 basin_tracer 结果（含 station_key, station_id, basin_id），构建
station_id -> basin-merged cluster_id 映射。

合并规则：
1) 仅 basin_status=resolved 且 basin_id 有效的站点可参与合并；
2) observation_type=Satellite 的站点保留为 singleton，不参与合并候选；
3) 同一 basin 内仅当 cluster 间所有跨组 pair 都满足：
   - 距离 <= max_station_distance_m
   - upstream area 对称相对误差 <= max_upstream_rel_error
   才允许合并（complete-linkage 风格）；
4) 不满足条件的站点保留 singleton（cluster_id=station_id）。

station_key 是 s3-s5 的稳定内部关联键。普通站点 lat/lon 必须来自 s3；
s4 的普通 lat/lon 只用于一致性审计，reach_anchor_lat/lon 不参与普通聚类距离。
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd


SATELLITE_OBSERVATION_TYPES = frozenset(
    [
        "satellite",
        "remote_sensing",
        "remote_sensing_observation",
        "satellite_observation",
    ]
)


def _normalize_observation_type(value) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return text.replace("-", "_").replace(" ", "_")


def _is_satellite_observation_type(value) -> bool:
    return _normalize_observation_type(value) in SATELLITE_OBSERVATION_TYPES


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


def _normalize_key_series(series):
    return series.fillna("").astype(str).map(lambda x: x.strip())


def _sample_rows(df, columns, limit=20):
    cols = [c for c in columns if c in df.columns]
    if not cols:
        return "(no sample columns available)"
    return df.loc[:, cols].head(limit).to_string(index=False)


def _require_unique_station_key(df, label):
    if "station_key" not in df.columns:
        raise ValueError(
            "{} must contain station_key; rerun s3 and rerun s4 with S4_RESUME=0.".format(
                label
            )
        )
    df["station_key"] = _normalize_key_series(df["station_key"])
    missing = df["station_key"].eq("")
    if missing.any():
        raise ValueError(
            "{} has {} missing station_key values. First rows:\n{}".format(
                label,
                int(missing.sum()),
                _sample_rows(df.loc[missing], ["station_key", "station_id", "path"]),
            )
        )
    dup = df["station_key"].duplicated(keep=False)
    if dup.any():
        raise ValueError(
            "{} has {} duplicate station_key values. First rows:\n{}".format(
                label,
                int(dup.sum()),
                _sample_rows(df.loc[dup], ["station_key", "station_id", "path"]),
            )
        )


def _coerce_station_id(df, label):
    if "station_id" not in df.columns:
        raise ValueError("{} must contain station_id".format(label))
    numeric = pd.to_numeric(df["station_id"], errors="coerce")
    invalid = numeric.isna() | (numeric % 1 != 0)
    if invalid.any():
        raise ValueError(
            "{} has {} invalid station_id values. First rows:\n{}".format(
                label,
                int(invalid.sum()),
                _sample_rows(df.loc[invalid], ["station_key", "station_id", "path"]),
            )
        )
    df["station_id"] = numeric.astype("int64")


def _merge_basin_with_station_coordinates(df, station_df):
    required_cols = {"station_key", "station_id", "lat", "lon"}
    missing = required_cols.difference(station_df.columns)
    if missing:
        raise ValueError("station_df must contain {}; missing={}".format(sorted(required_cols), sorted(missing)))

    basin = df.copy()
    stations = station_df.copy()
    _require_unique_station_key(basin, "Basin CSV")
    _require_unique_station_key(stations, "station_df")
    _coerce_station_id(basin, "Basin CSV")
    _coerce_station_id(stations, "station_df")

    basin_keys = set(basin["station_key"].tolist())
    station_keys = set(stations["station_key"].tolist())
    missing_in_basin = sorted(station_keys.difference(basin_keys))
    extra_in_basin = sorted(basin_keys.difference(station_keys))
    if missing_in_basin:
        raise ValueError(
            "Basin CSV is missing {} station_df station_key values. First rows:\n{}".format(
                len(missing_in_basin),
                _sample_rows(stations[stations["station_key"].isin(missing_in_basin[:20])], ["station_key", "station_id", "path"]),
            )
        )
    if extra_in_basin:
        raise ValueError(
            "Basin CSV has {} extra station_key values. First rows:\n{}".format(
                len(extra_in_basin),
                _sample_rows(basin[basin["station_key"].isin(extra_in_basin[:20])], ["station_key", "station_id", "lat", "lon"]),
            )
        )

    station_cols = ["station_key", "station_id", "lat", "lon"]
    if "observation_type" in stations.columns:
        station_cols.append("observation_type")
    station_loc = stations[station_cols].copy()
    station_loc = station_loc.rename(
        columns={
            "station_id": "station_id_s3",
            "lat": "lat_s3",
            "lon": "lon_s3",
            "observation_type": "observation_type_s3",
        }
    )
    merged = basin.merge(station_loc, on="station_key", how="inner", validate="one_to_one")
    id_mismatch = merged["station_id"].astype("int64") != merged["station_id_s3"].astype("int64")
    if id_mismatch.any():
        raise ValueError(
            "Basin CSV station_id disagrees with s3 for {} station_key values. First rows:\n{}".format(
                int(id_mismatch.sum()),
                merged.loc[id_mismatch, ["station_key", "station_id", "station_id_s3"]].head(20).to_string(index=False),
            )
        )

    for col in ["lat", "lon"]:
        s4_col = col
        s3_col = "{}_s3".format(col)
        if s4_col in merged.columns:
            left = pd.to_numeric(merged[s4_col], errors="coerce")
            right = pd.to_numeric(merged[s3_col], errors="coerce")
            mismatch = ~np.isclose(left, right, rtol=0.0, atol=1e-10, equal_nan=True)
            if mismatch.any():
                raise ValueError(
                    "Basin CSV ordinary {} disagrees with s3 for {} station_key values. First rows:\n{}".format(
                        col,
                        int(mismatch.sum()),
                        merged.loc[
                            mismatch,
                            ["station_key", "station_id", s4_col, s3_col],
                        ].head(20).to_string(index=False),
                    )
                )

    merged["station_id"] = merged["station_id_s3"].astype("int64")
    merged["lat"] = pd.to_numeric(merged["lat_s3"], errors="coerce")
    merged["lon"] = pd.to_numeric(merged["lon_s3"], errors="coerce")
    if "observation_type_s3" in merged.columns:
        if "observation_type" in merged.columns:
            existing = merged["observation_type"].fillna("").astype(str).str.strip()
            merged["observation_type"] = merged["observation_type"].where(
                existing.ne(""),
                merged["observation_type_s3"],
            )
        else:
            merged["observation_type"] = merged["observation_type_s3"]
    return merged.drop(columns=["station_id_s3", "lat_s3", "lon_s3", "observation_type_s3"], errors="ignore")


def load_station_to_basin_cluster_map(
    basin_csv_path: Path,
    station_df=None,
    max_station_distance_m=1000.0,
    max_upstream_rel_error=0.10,
    upstream_area_col="uparea_merit",
):
    """
    读取 basin_tracer 输出（s4_upstream_basins.csv），生成：
      station_id(cluster_id) -> basin-merged cluster_id 映射。

    返回：
      mapping: dict[int, int]
      stats: {
        "n_station": int,   # 输入中唯一 station 数
        "n_success": int,   # resolved 且有 basin_id 的 station 数
        "n_satellite_excluded_from_merge": int,
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
    if "station_key" not in df.columns or "station_id" not in df.columns or "basin_id" not in df.columns:
        raise ValueError("Basin CSV must contain columns: station_key, station_id, basin_id")

    df = df.dropna(subset=["station_id"]).copy()
    if len(df) == 0:
        return {}, {
            "n_station": 0,
            "n_success": 0,
            "n_satellite_excluded_from_merge": 0,
            "n_basins": 0,
            "n_clusters_from_basins": 0,
            "n_changed": 0,
            "max_station_distance_m": float(max_station_distance_m),
            "max_upstream_rel_error": float(max_upstream_rel_error),
            "upstream_area_col": str(upstream_area_col),
        }

    _require_unique_station_key(df, "Basin CSV")
    _coerce_station_id(df, "Basin CSV")

    if station_df is not None:
        df = _merge_basin_with_station_coordinates(df, station_df)

    resolved_mask = (
        df["basin_status"].fillna("").astype(str).str.strip().str.lower().eq("resolved")
        if "basin_status" in df.columns
        else pd.Series(False, index=df.index)
    )
    ok = df[resolved_mask].copy()
    ok["basin_id"] = pd.to_numeric(ok["basin_id"], errors="coerce")
    ok = ok.dropna(subset=["basin_id"]).copy()
    ok["basin_id"] = ok["basin_id"].astype("int64")
    if "observation_type" in ok.columns:
        satellite_candidate_mask = ok["observation_type"].map(_is_satellite_observation_type)
        n_satellite_excluded_from_merge = int(satellite_candidate_mask.sum())
        merge_ok = ok.loc[~satellite_candidate_mask].copy()
    else:
        n_satellite_excluded_from_merge = 0
        merge_ok = ok

    mapping = {sid: sid for sid in df["station_id"].unique().tolist()}
    n_clusters_from_basins = 0
    if len(merge_ok) > 0 and upstream_area_col in merge_ok.columns:
        merge_ok["lat"] = pd.to_numeric(merge_ok.get("lat"), errors="coerce")
        merge_ok["lon"] = pd.to_numeric(merge_ok.get("lon"), errors="coerce")
        merge_ok[upstream_area_col] = pd.to_numeric(merge_ok[upstream_area_col], errors="coerce")
        candidates = merge_ok.dropna(subset=["lat", "lon", upstream_area_col]).copy()

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
        "n_satellite_excluded_from_merge": int(n_satellite_excluded_from_merge),
        "n_basins": int(ok["basin_id"].nunique()),
        "n_clusters_from_basins": int(n_clusters_from_basins),
        "n_changed": n_changed,
        "max_station_distance_m": float(max_station_distance_m),
        "max_upstream_rel_error": float(max_upstream_rel_error),
        "upstream_area_col": str(upstream_area_col),
    }
    return mapping, stats
