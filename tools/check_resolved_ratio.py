#!/usr/bin/env python3
"""
统计 s5 basin merge 输出中：
- resolved / unresolved 分别包含哪些数据集
- 每个 basin_status + source 的站点数量
- 每个数据集内 resolved / unresolved 的占比

不需要命令行参数，直接在下面 CONFIG 区域修改路径即可。

运行：
  python summarize_basin_status_datasets.py
"""

from pathlib import Path

import pandas as pd


# =========================
# CONFIG：在这里改路径和列名
# =========================

INPUT_CSV = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/s5_basin_clustered_stations.csv")

OUT_LONG_CSV = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/s5_basin_status_dataset_summary_long.csv")
OUT_PIVOT_CSV = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/s5_basin_status_dataset_summary_pivot.csv")

DATASET_COL = "source"
STATUS_COL = "basin_status"

TARGET_STATUSES = ["resolved", "unresolved"]


def normalize_status(x):
    """统一 basin_status 写法。"""
    if pd.isna(x):
        return "missing"
    x = str(x).strip().lower()
    return x if x else "missing"


def normalize_dataset(x):
    """统一数据集名称写法。"""
    if pd.isna(x):
        return "missing"
    x = str(x).strip()
    return x if x else "missing"


def main():
    if not INPUT_CSV.is_file():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)

    required_cols = {DATASET_COL, STATUS_COL}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}. "
            f"Available columns: {list(df.columns)}"
        )

    work = df.copy()
    work[STATUS_COL] = work[STATUS_COL].map(normalize_status)
    work[DATASET_COL] = work[DATASET_COL].map(normalize_dataset)

    # 只统计 resolved / unresolved
    target = work[work[STATUS_COL].isin(TARGET_STATUSES)].copy()

    if len(target) == 0:
        print("No resolved or unresolved rows found.")
        return 0

    # 长表：每个 status + dataset 的数量
    summary = (
        target
        .groupby([STATUS_COL, DATASET_COL], dropna=False)
        .size()
        .reset_index(name="station_count")
    )

    # 每个 dataset 的总数
    # 注意：这里的分母只包含 resolved + unresolved，不包含 missing 或其他 status
    dataset_total = (
        target
        .groupby(DATASET_COL, dropna=False)
        .size()
        .reset_index(name="dataset_total")
    )

    summary = summary.merge(dataset_total, on=DATASET_COL, how="left")
    summary["ratio_in_dataset"] = summary["station_count"] / summary["dataset_total"]
    summary["percent_in_dataset"] = summary["ratio_in_dataset"] * 100.0

    summary = summary.sort_values(
        [DATASET_COL, STATUS_COL],
        ascending=[True, True],
    ).reset_index(drop=True)

    # 打印 resolved / unresolved 分别包含哪些数据集
    print("\n=== Dataset list by basin_status ===")
    for status in TARGET_STATUSES:
        datasets = (
            summary.loc[summary[STATUS_COL] == status, DATASET_COL]
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        print(f"\n{status}: {len(datasets)} datasets")
        for ds in datasets:
            print(f"  - {ds}")

    # 打印长表统计
    print("\n=== Counts and ratios by basin_status and dataset ===")
    display_summary = summary.copy()
    display_summary["ratio_in_dataset"] = display_summary["ratio_in_dataset"].round(4)
    display_summary["percent_in_dataset"] = display_summary["percent_in_dataset"].round(2)
    print(display_summary.to_string(index=False))

    # 宽表：每个 dataset 一行
    count_pivot = (
        summary
        .pivot_table(
            index=DATASET_COL,
            columns=STATUS_COL,
            values="station_count",
            fill_value=0,
            aggfunc="sum",
        )
        .reset_index()
    )

    # 确保 resolved / unresolved 两列都存在
    for status in TARGET_STATUSES:
        if status not in count_pivot.columns:
            count_pivot[status] = 0

    count_pivot["dataset_total"] = (
        count_pivot["resolved"] + count_pivot["unresolved"]
    )

    count_pivot["resolved_ratio"] = (
        count_pivot["resolved"] / count_pivot["dataset_total"]
    )
    count_pivot["unresolved_ratio"] = (
        count_pivot["unresolved"] / count_pivot["dataset_total"]
    )

    count_pivot["resolved_percent"] = count_pivot["resolved_ratio"] * 100.0
    count_pivot["unresolved_percent"] = count_pivot["unresolved_ratio"] * 100.0

    count_pivot = count_pivot[
        [
            DATASET_COL,
            "dataset_total",
            "resolved",
            "unresolved",
            "resolved_ratio",
            "unresolved_ratio",
            "resolved_percent",
            "unresolved_percent",
        ]
    ].sort_values(DATASET_COL).reset_index(drop=True)

    print("\n=== Pivot summary by dataset ===")
    display_pivot = count_pivot.copy()
    display_pivot["resolved_ratio"] = display_pivot["resolved_ratio"].round(4)
    display_pivot["unresolved_ratio"] = display_pivot["unresolved_ratio"].round(4)
    display_pivot["resolved_percent"] = display_pivot["resolved_percent"].round(2)
    display_pivot["unresolved_percent"] = display_pivot["unresolved_percent"].round(2)
    print(display_pivot.to_string(index=False))

    # 输出 CSV
    OUT_LONG_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUT_PIVOT_CSV.parent.mkdir(parents=True, exist_ok=True)

    summary.to_csv(OUT_LONG_CSV, index=False)
    count_pivot.to_csv(OUT_PIVOT_CSV, index=False)

    print(f"\nWrote long summary CSV: {OUT_LONG_CSV}")
    print(f"Wrote pivot summary CSV: {OUT_PIVOT_CSV}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
