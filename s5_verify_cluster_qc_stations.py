#!/usr/bin/env python3
"""
步骤 s5：验证 s4_cluster_qc_stations 的聚类结果是否正确（仅控制台输出，无文件输出）。

检查项：
  1) 同一 cluster 内：多站时在“距离<阈值”的边下应连通（图连通性）。
  2) 不同 cluster 之间：抽样跨簇站对，不应出现距离 < 阈值的对（否则应被合并）。
  3) 输出简要统计与违规列表（若有）。

用法（在 Output_r 根目录下运行）：
  python scripts/s5_verify_cluster_qc_stations.py --clustered scripts/output/s4_clustered_stations.csv [--threshold 0.01]
  python scripts/s5_verify_cluster_qc_stations.py -c scripts/output/s4_clustered_stations.csv -t 0.01 --sample-cross 5000

输入（默认）：
  - scripts/output/s4_clustered_stations.csv（步骤 s4 输出，来自 pipeline_paths.S4_CLUSTERED_CSV）
  - threshold 默认来自 pipeline_paths.S4_S5_THRESHOLD_DEG（与 s4 保持一致）
输出（默认）：
  - 无文件输出（仅终端/日志输出验证结果）
"""

import argparse
from pathlib import Path
from math import radians, sin, cos, sqrt, atan2
from collections import defaultdict

import numpy as np
import pandas as pd
from pipeline_paths import (
    S4_S5_THRESHOLD_DEG,
    S4_S5_SOURCE_THRESHOLD_OVERRIDE_DEG,
    get_output_r_root,
)


def haversine_deg(lat1, lon1, lat2, lon2):
    """与 2_cluster_qc_stations 一致：近似距离（度），约 0.01°≈1.1km。"""
    R = 6371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlam = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlam / 2) ** 2
    d_km = 2 * R * atan2(sqrt(a), sqrt(1 - a))
    return d_km / 111.0


def haversine_km(lat1, lon1, lat2, lon2):
    """返回公里。"""
    R = 6371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlam = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlam / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


def _source_threshold(source, default_threshold, overrides):
    s = str(source).strip().lower()
    for key, val in overrides.items():
        if s.startswith(str(key).strip().lower()):
            return float(val)
    return float(default_threshold)


_LOG_TEE_ENABLED = False


def _enable_script_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    import atexit
    import sys
    from datetime import datetime

    log_path = Path(__file__).resolve().with_name("{}_log.txt".format(Path(__file__).stem))
    if log_path.exists():
        try:
            log_path.unlink()
        except Exception:
            pass
    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("\n===== Run started {} =====\n".format(datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()

    class _TeeStream:
        def __init__(self, stream, log_file):
            self._stream = stream
            self._log_file = log_file

        def write(self, data):
            self._stream.write(data)
            self._log_file.write(data)
            self._log_file.flush()

        def flush(self):
            self._stream.flush()
            self._log_file.flush()

    sys.stdout = _TeeStream(sys.stdout, log_fp)
    sys.stderr = _TeeStream(sys.stderr, log_fp)
    atexit.register(log_fp.close)
    _LOG_TEE_ENABLED = True


def main():
    _enable_script_logging()
    _root = get_output_r_root(Path(__file__).resolve().parent)
    default_clustered = _root / "scripts" / "output" / "s4_clustered_stations.csv"
    ap = argparse.ArgumentParser(description="步骤 s5：验证聚类结果（读 s4_clustered_stations.csv）")
    ap.add_argument("--clustered", "-c", default=str(default_clustered),
                   help="步骤 s4 输出 s4_clustered_stations.csv 路径（默认: Output_r/scripts/output/s4_clustered_stations.csv）")
    ap.add_argument("--threshold", "-t", type=float, default=S4_S5_THRESHOLD_DEG,
                   help="Same threshold (degrees) used when clustering; shared default from pipeline_paths.S4_S5_THRESHOLD_DEG")
    ap.add_argument("--sample-cross", type=int, default=0,
                   help="Max random cross-cluster pairs to check (0=auto, ~100k)")
    ap.add_argument("--quick", action="store_true",
                   help="Skip connectivity check for clusters with >200 stations; use 5k cross sample")
    args = ap.parse_args()

    path = Path(args.clustered)
    if not path.is_absolute():
        path = _root / path
    print("Using clustered CSV: {}".format(path))
    if not path.is_file():
        print("Error: not found: {}".format(path))
        return

    df = pd.read_csv(path)
    for col in ["lat", "lon", "cluster_id"]:
        if col not in df.columns:
            print("Error: CSV must have columns: lat, lon, cluster_id")
            return

    threshold = args.threshold
    n = len(df)
    cids = df["cluster_id"].values
    lats = df["lat"].values.astype(float)
    lons = df["lon"].values.astype(float)
    source_overrides = S4_S5_SOURCE_THRESHOLD_OVERRIDE_DEG
    if "source" in df.columns:
        station_thresholds = np.asarray(
            [_source_threshold(src, threshold, source_overrides) for src in df["source"].values],
            dtype=np.float64,
        )
    else:
        station_thresholds = np.full(n, float(threshold), dtype=np.float64)
    print("Source threshold overrides: {}".format(source_overrides))

    # 1) 同一 cluster 内：图连通性（边 = 距离 < 阈值的对）
    print("Check 1: Within-cluster connectivity (each cluster = one connected component under threshold)")
    violations_connect = []
    clusters = defaultdict(list)
    for i in range(n):
        clusters[cids[i]].append(i)
    n_multi = 0
    max_connect_size = 200 if args.quick else 999999
    for cid, inds in clusters.items():
        if len(inds) <= 1:
            continue
        if len(inds) > max_connect_size:
            continue  # --quick: skip very large clusters
        n_multi += 1
        # 建图：节点 inds，边 (i,j) 若 d(i,j) < threshold
        parent = {idx: idx for idx in inds}

        def find(x):
            stack = []
            while parent[x] != x:
                stack.append(x)
                x = parent[x]
            for i in stack:
                parent[i] = x
            return x

        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for ii in range(len(inds)):
            for jj in range(ii + 1, len(inds)):
                i, j = inds[ii], inds[jj]
                d = haversine_deg(lats[i], lons[i], lats[j], lons[j])
                pair_threshold = min(station_thresholds[i], station_thresholds[j])
                if d < pair_threshold:
                    union(i, j)
        roots = set(find(i) for i in inds)
        if len(roots) > 1:
            violations_connect.append((cid, len(inds), len(roots)))

    if violations_connect:
        print("  FAIL: {} cluster(s) are not connected under threshold:".format(len(violations_connect)))
        for cid, size, n_comp in violations_connect[:20]:
            print("    cluster_id={} n_stations={} n_components={}".format(cid, size, n_comp))
        if len(violations_connect) > 20:
            print("    ... and {} more".format(len(violations_connect) - 20))
    else:
        print("  OK: All multi-station clusters are connected under threshold.")

    # 2) 不同 cluster 之间：抽样检查跨簇站对，不应存在距离 < 阈值的对
    print("\nCheck 2: No pair from different clusters with distance < threshold (sampled)")
    idx_by_cid = clusters  # cid -> list of row indices
    # 随机抽若干跨簇站对做检查（全量 O(n^2) 太慢）
    if args.quick:
        sample_size = 5000
    else:
        sample_size = args.sample_cross if args.sample_cross > 0 else min(100000, n * (n - 1) // 2)
    violations_cross = []
    np.random.seed(42)
    checked = 0
    for _ in range(sample_size):
        i, j = np.random.randint(0, n, 2)
        if i >= j or cids[i] == cids[j]:
            continue
        checked += 1
        d = haversine_deg(lats[i], lons[i], lats[j], lons[j])
        pair_threshold = min(station_thresholds[i], station_thresholds[j])
        if d < pair_threshold:
            violations_cross.append((cids[i], cids[j], i, j, d, df["path"].iloc[i], df["path"].iloc[j]))
    print("  Sampled {} cross-cluster pairs.".format(checked))

    if violations_cross:
        print("  FAIL: {} pair(s) from different clusters with distance < threshold:".format(len(violations_cross)))
        for c1, c2, i, j, d, p1, p2 in violations_cross[:10]:
            print("    cluster {} vs {} d_deg={:.5f} ({} km)".format(c1, c2, d, d * 111))
            print("      {}".format(Path(p1).name))
            print("      {}".format(Path(p2).name))
        if len(violations_cross) > 10:
            print("    ... and {} more".format(len(violations_cross) - 10))
    else:
        print("  OK: No cross-cluster pairs within threshold (sampled or full).")

    # 3) 统计摘要
    print("\nSummary:")
    print("  Total stations: {}".format(n))
    print("  Total clusters: {}".format(len(clusters)))
    sizes = [len(inds) for inds in clusters.values()]
    print("  Clusters with 1 station: {}".format(sum(1 for s in sizes if s == 1)))
    print("  Clusters with 2+ stations: {}".format(sum(1 for s in sizes if s >= 2)))
    if sizes:
        print("  Max cluster size: {}".format(max(sizes)))
    print("  Threshold: {} deg (~{:.2f} km)".format(threshold, threshold * 111))

    # 多站 cluster 内最大成对距离（公里）
    max_diam_km = 0
    for cid, inds in clusters.items():
        if len(inds) < 2:
            continue
        for ii in range(len(inds)):
            for jj in range(ii + 1, len(inds)):
                i, j = inds[ii], inds[jj]
                d_km = haversine_km(lats[i], lons[i], lats[j], lons[j])
                max_diam_km = max(max_diam_km, d_km)
    print("  Max pairwise distance within any cluster: {:.2f} km".format(max_diam_km))

    if violations_connect or violations_cross:
        print("\nResult: VERIFICATION FAILED")
    else:
        print("\nResult: VERIFICATION PASSED (distance merging logic is consistent)")


if __name__ == "__main__":
    main()
