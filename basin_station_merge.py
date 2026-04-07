#!/usr/bin/env python3
"""
流域合并站点工具：
读取 basin_tracer 结果（含 station_id, basin_id），构建
station_id -> basin-merged cluster_id 映射。

合并规则：
1) basin_id 相同视为同一流域；
2) 同流域多个 station_id 统一映射为该流域最小 station_id；
3) basin_id 缺失站点保留原 station_id。
"""

from pathlib import Path

import pandas as pd


def load_station_to_basin_cluster_map(basin_csv_path: Path):
    """
    读取 basin_tracer 输出（s6_upstream_basins.csv），生成：
      station_id(cluster_id) -> basin-merged cluster_id 映射。

    返回：
      mapping: dict[int, int]
      stats: {
        "n_station": int,   # 输入中唯一 station 数
        "n_success": int,   # 有 basin_id 的 station 数
        "n_basins": int,    # 唯一 basin 数
        "n_changed": int,   # station_id 被重映射数量
      }
    """
    basin_csv_path = Path(basin_csv_path)
    if not basin_csv_path.is_file():
        raise FileNotFoundError("Basin CSV not found: {}".format(basin_csv_path))

    df = pd.read_csv(basin_csv_path, usecols=["station_id", "basin_id"])
    if "station_id" not in df.columns or "basin_id" not in df.columns:
        raise ValueError("Basin CSV must contain columns: station_id, basin_id")

    df = df.dropna(subset=["station_id"]).copy()
    if len(df) == 0:
        return {}, {"n_station": 0, "n_success": 0, "n_basins": 0, "n_changed": 0}

    df["station_id"] = df["station_id"].astype(int)
    ok = df.dropna(subset=["basin_id"]).copy()
    ok["basin_id"] = ok["basin_id"].astype("int64")

    basin_to_rep = ok.groupby("basin_id")["station_id"].min().to_dict()
    mapping = {sid: sid for sid in df["station_id"].tolist()}
    for _, r in ok.iterrows():
        sid = int(r["station_id"])
        bid = int(r["basin_id"])
        mapping[sid] = int(basin_to_rep[bid])

    n_changed = int(sum(1 for sid, rep in mapping.items() if sid != rep))
    stats = {
        "n_station": int(df["station_id"].nunique()),
        "n_success": int(ok["station_id"].nunique()),
        "n_basins": int(ok["basin_id"].nunique()),
        "n_changed": n_changed,
    }
    return mapping, stats

