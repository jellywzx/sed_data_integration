#!/usr/bin/env python3
"""
在保留原有 Output_r 目录结构的前提下，根据时间分辨率校验结果，
将「所有 qc 文件夹下的 .nc」按检测到的时间分辨率复制到新目录。

新目录结构：
  {out_dir}/
    daily/                    # 检测为 daily 或 hourly
    monthly/                  # 检测为 monthly 或 quarterly
    annual/                   # 年尺度观测
    climatology/              # 多年平均/气候态
    other/                    # 其余：irregular、no_time_var、error 等

每个分辨率目录下不按数据集分子文件夹，文件名为全库唯一，体现「数据源」和「时间分辨率」：
  {数据源}_{分辨率}_{原文件名无后缀}.nc
  若重名则追加 _2, _3, ...

依赖：需先运行 s1_verify_time_resolution.py，生成步骤 1 输出 s1_verify_time_resolution_results.csv。

用法（在 Output_r 根目录下运行）：
  python scripts/s2_reorganize_qc_by_resolution.py
  python scripts/s2_reorganize_qc_by_resolution.py --out-dir my_reorganized
  python scripts/s2_reorganize_qc_by_resolution.py -j 16   # 16 线程并行复制

输入（默认）：
  - scripts/output/s1_verify_time_resolution_results.csv（步骤 s1 输出，来自 pipeline_paths.S1_VERIFY_CSV）
  - Output_r 下原始 qc 目录中的 .nc
输出（默认）：
  - ../output_resolution_organized/（步骤 s2 输出目录，来自 pipeline_paths.S2_ORGANIZED_DIR，供 s3 默认扫描）
  - scripts/output/s2_other_resolution_summary.csv（other 分类汇总，来自 pipeline_paths.S2_OTHER_SUMMARY_CSV）
  - scripts/output/s2_other_resolution_details.csv（other 分类明细，来自 pipeline_paths.S2_OTHER_DETAILS_CSV）

说明：
  - 步骤 s2 无单个 CSV 输出，结果是按分辨率整理后的目录。
"""

import re
import shutil
import sys
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import xarray as xr
from pipeline_paths import (
    S1_VERIFY_CSV,
    S2_ORGANIZED_DIR,
    S2_OTHER_SUMMARY_CSV,
    S2_OTHER_DETAILS_CSV,
    RESOLUTION_DIRS,
    get_output_r_root,
)

# 项目根 Output_r
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = get_output_r_root(SCRIPT_DIR)  # Output_r（支持 OUTPUT_R_ROOT 覆盖）
# 校验结果 CSV（相对 ROOT_DIR）；步骤 s1 输出
VERIFY_CSV = S1_VERIFY_CSV
# 新目录名（相对 ROOT_DIR），仅包含 qc 下 nc 按分辨率整理后的副本
OUT_DIR = S2_ORGANIZED_DIR
# 并行复制时的默认线程数（I/O 为主，线程池即可）
DEFAULT_WORKERS = 8
LEGACY_RESOLUTION_DIRS = ("annually_climatology", "quarterly", "single_point")

def get_source_from_path(path: str, root_dir: Path) -> str:
    """从相对路径解析数据源，如 daily/GloRiSe/SS/qc/xxx.nc -> GloRiSe_SS。"""
    try:
        p = Path(path).resolve()
        root = root_dir.resolve()
        rel = p.relative_to(root)
        parts = rel.parts
        if "qc" in parts:
            idx = parts.index("qc")
            before = parts[:idx]
            if len(before) >= 2:
                source = "_".join(before[1:])
            else:
                source = before[0] if before else "unknown"
        else:
            source = parts[0] if parts else "unknown"
        return re.sub(r"[^\w\-]", "_", source).strip("_") or "unknown"
    except Exception:
        return "unknown"


def safe_fname_part(s: str) -> str:
    """文件名安全：只保留字母数字下划线横线。"""
    return re.sub(r"[^\w\-]", "_", str(s)).strip("_") or "unknown"


def resolution_from_semantics(temporal_semantics: str) -> str:
    """将 s1 输出的 temporal_semantics 映射到 s2 目录名。"""
    if not temporal_semantics or not isinstance(temporal_semantics, str):
        return "other"
    d = temporal_semantics.strip().lower()
    if d == "hourly":
        return "daily"
    if d == "single_point":
        return "daily"
    if d == "quarterly":
        return "monthly"
    if d in RESOLUTION_DIRS:
        return d
    return "other"


def should_treat_irregular_as_daily(nc_path: Path) -> bool:
    """
    对 detected_frequency=irregular 的文件做二次判定：
    若时间轴呈“离散日值记录”特征，则归入 daily。

    规则：
      1) 有可识别时间变量，且时间点 >= 1；
      2) 所有时间都在 00:00:00（无小时/分钟/秒）；
      3) 即使全部在每月 1 号，也按 daily 处理（按当前业务规则放宽）。
    """
    try:
        with xr.open_dataset(nc_path) as ds:
            time_candidates = ["time", "Time", "t", "datetime", "date"]
            time_var = next((c for c in time_candidates if c in ds.variables), None)
            if time_var is None:
                return False

            times = pd.to_datetime(ds[time_var].values, errors="coerce")
            if getattr(times, "size", 0) < 1:
                return False

            # 去掉无法解析时间，至少保留 1 个有效点
            ts = pd.Series(times).dropna()
            if len(ts) < 1:
                return False

            # 要求都落在整天（00:00:00）
            if not ((ts.dt.hour == 0) & (ts.dt.minute == 0) & (ts.dt.second == 0)).all():
                return False

            return True
    except Exception:
        return False


def _copy_one(item):
    """单次复制，供线程池调用。返回 (res_dir_name, None) 成功，(res_dir_name, (src, err)) 失败。"""
    src_path, dest_path, res_dir_name = item
    try:
        shutil.copy2(src_path, dest_path)
        return (res_dir_name, None)
    except Exception as e:
        return (res_dir_name, (str(src_path), str(e)))


def export_other_resolution_reports(other_df: pd.DataFrame, root_dir: Path, summary_out: str, details_out: str):
    """导出 other 分类的汇总与明细 CSV。"""
    summary_path = root_dir / summary_out
    details_path = root_dir / details_out
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    details_path.parent.mkdir(parents=True, exist_ok=True)

    if len(other_df) == 0:
        pd.DataFrame(columns=["metric", "value"]).to_csv(summary_path, index=False)
        pd.DataFrame(columns=["path", "source", "detected_frequency", "temporal_semantics", "single_point_interpretation"]).to_csv(
            details_path, index=False
        )
        print("other 目录无数据，已写出空报告：")
        print(f"  summary: {summary_path}")
        print(f"  details: {details_path}")
        return

    work = other_df.copy()
    if "single_point_interpretation" not in work.columns:
        work["single_point_interpretation"] = ""
    work["single_point_interpretation"] = work["single_point_interpretation"].fillna("").astype(str)

    details_cols = [
        "path",
        "source",
        "detected_frequency",
        "temporal_semantics",
        "single_point_interpretation",
        "rel_path",
        "path_resolution",
        "consistent",
    ]
    details_cols = [c for c in details_cols if c in work.columns]
    details_df = work[details_cols].sort_values(["detected_frequency", "source"]).reset_index(drop=True)

    summary_rows = [
        {"metric": "other_total_files", "value": int(len(work))},
        {"metric": "other_total_sources", "value": int(work["source"].nunique())},
    ]

    freq_counts = work["detected_frequency"].value_counts()
    for freq, cnt in freq_counts.items():
        summary_rows.append({"metric": f"detected_frequency::{freq}", "value": int(cnt)})

    single_df = work[work["detected_frequency"] == "single_point"]
    if len(single_df) > 0:
        interp_counts = single_df["single_point_interpretation"].replace("", "(empty)").value_counts()
        for interp, cnt in interp_counts.items():
            summary_rows.append({"metric": f"single_point_interpretation::{interp}", "value": int(cnt)})

    source_counts = work["source"].value_counts()
    for src, cnt in source_counts.items():
        summary_rows.append({"metric": f"source::{src}", "value": int(cnt)})

    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    details_df.to_csv(details_path, index=False)
    print("已导出 other 分类报告：")
    print(f"  summary: {summary_path}")
    print(f"  details: {details_path}")


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
    ap = argparse.ArgumentParser(description="按时间分辨率校验结果将 qc 下 nc 复制到新目录（数据源_分辨率_原名）")
    ap.add_argument("--out-dir", "-o", default=OUT_DIR, help=f"新目录名（相对 Output_r），默认 {OUT_DIR}")
    ap.add_argument("--verify-csv", default=VERIFY_CSV, help=f"校验结果 CSV 路径，默认 {VERIFY_CSV}")
    ap.add_argument("--clear", action="store_true", help="复制前清空输出目录下各时间语义目录，避免残留旧分类文件")
    ap.add_argument("--workers", "-j", type=int, default=DEFAULT_WORKERS, metavar="N", help=f"并行复制线程数，默认 {DEFAULT_WORKERS}，设为 1 则串行")
    ap.add_argument("--other-summary-out", default=S2_OTHER_SUMMARY_CSV, help="other 分类汇总输出 CSV")
    ap.add_argument("--other-details-out", default=S2_OTHER_DETAILS_CSV, help="other 分类明细输出 CSV")
    args = ap.parse_args()

    root_dir = Path(ROOT_DIR).resolve()
    if not root_dir.is_dir():
        print(f"错误：根目录不存在: {root_dir}", file=sys.stderr)
        sys.exit(1)

    verify_path = root_dir / args.verify_csv
    if not verify_path.is_file():
        print(f"错误：未找到校验结果 {verify_path}，请先运行 s1_verify_time_resolution.py", file=sys.stderr)
        sys.exit(1)

    out_base = root_dir / args.out_dir
    for sub in RESOLUTION_DIRS:
        (out_base / sub).mkdir(parents=True, exist_ok=True)
    for legacy_sub in LEGACY_RESOLUTION_DIRS:
        legacy_dir = out_base / legacy_sub
        if legacy_dir.exists():
            legacy_dir.mkdir(parents=True, exist_ok=True)

    if args.clear:
        for sub in RESOLUTION_DIRS + LEGACY_RESOLUTION_DIRS:
            d = out_base / sub
            if not d.exists():
                continue
            for f in d.iterdir():
                if f.is_file():
                    f.unlink()
            print(f"已清空: {d}")

    df = pd.read_csv(verify_path)
    for col in ("path", "detected_frequency"):
        if col not in df.columns:
            print(f"错误：CSV 缺少列 {col}", file=sys.stderr)
            sys.exit(1)

    # 只处理路径中包含 qc 的 nc（qc 文件夹下的数据）
    def is_qc_path(path_str):
        if pd.isna(path_str):
            return False
        parts = Path(path_str).parts
        return "qc" in parts

    df = df[df["path"].apply(is_qc_path)].copy()
    if "temporal_semantics" in df.columns:
        df["resolution_dir"] = df["temporal_semantics"].apply(resolution_from_semantics)
    else:
        df["resolution_dir"] = df["detected_frequency"].apply(resolution_from_semantics)

    # irregular 的二次判定：若时间轴表现为离散日值记录，则改归 daily
    irregular_mask = df["resolution_dir"].astype(str).str.strip().str.lower() == "irregular"
    irregular_idx = df[irregular_mask].index.tolist()
    n_irregular_to_daily = 0
    for idx in irregular_idx:
        p = Path(df.at[idx, "path"])
        if p.is_file() and should_treat_irregular_as_daily(p):
            df.at[idx, "resolution_dir"] = "daily"
            n_irregular_to_daily += 1

    if n_irregular_to_daily > 0:
        print(f"irregular 二次判定改归 daily: {n_irregular_to_daily} 个文件")

    df["source"] = df["path"].apply(lambda p: get_source_from_path(p, root_dir))
    df["stem"] = df["path"].apply(lambda p: Path(p).stem)
    df["safe_source"] = df["source"].apply(safe_fname_part)
    df["safe_stem"] = df["stem"].apply(safe_fname_part)

    # 已使用的文件名（不含 .nc），按 resolution 目录记录，用于生成唯一名
    used = {}
    for r in RESOLUTION_DIRS:
        used[r] = set()

    copied = {r: 0 for r in RESOLUTION_DIRS}
    skipped = 0
    tasks = []  # (src_path, dest_path, res_dir_name)

    for _, row in df.iterrows():
        src_path = Path(row["path"])
        if not src_path.is_file():
            skipped += 1
            continue
        res_dir_name = row["resolution_dir"]
        res_dir = out_base / res_dir_name
        base = f"{row['safe_source']}_{res_dir_name}_{row['safe_stem']}"
        base_candidate = base
        idx = 2
        while base_candidate in used[res_dir_name]:
            base_candidate = f"{base}_{idx}"
            idx += 1
        used[res_dir_name].add(base_candidate)
        dest_path = res_dir / (base_candidate + ".nc")
        tasks.append((src_path, dest_path, res_dir_name))

    workers = max(1, int(args.workers))
    errors = []
    if workers == 1:
        for item in tasks:
            res_dir_name, err = _copy_one(item)
            if err:
                errors.append(err)
            else:
                copied[res_dir_name] += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_copy_one, item): item for item in tasks}
            for fut in as_completed(futures):
                res_dir_name, err = fut.result()
                if err:
                    errors.append(err)
                else:
                    copied[res_dir_name] += 1

    print(f"新目录: {out_base}")
    print(f"已处理 qc 下 nc 数量: {len(df)}（跳过不存在: {skipped}）")
    for r in RESOLUTION_DIRS:
        print(f"  {r}: {copied[r]} 个文件")
    if errors:
        print(f"复制失败 {len(errors)} 个:")
        for p, e in errors[:10]:
            print(f"  {p} -> {e}")
        if len(errors) > 10:
            print(f"  ... 共 {len(errors)} 个")
    else:
        print("全部复制完成。")

    # other 目录的数据集构成说明
    other_df = df[df["resolution_dir"] == "other"]
    if len(other_df) > 0:
        print("\n--- other 目录构成（未归入标准时间语义目录的文件）---")
        print("按 detected_frequency 统计:")
        for freq, cnt in other_df["detected_frequency"].value_counts().items():
            print(f"  {freq}: {cnt} 个")
        single_in_other = other_df[other_df["detected_frequency"] == "single_point"]
        if len(single_in_other) > 0 and "single_point_interpretation" in other_df.columns:
            print("\nsingle_point 中留在 other 的 single_point_interpretation 统计（前 15 类）:")
            interp = single_in_other["single_point_interpretation"].fillna("").astype(str)
            for val, c in interp.value_counts().head(15).items():
                print(f"  {val or '(空)'}: {c} 个")
        print("---")
    export_other_resolution_reports(other_df, root_dir, args.other_summary_out, args.other_details_out)

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
