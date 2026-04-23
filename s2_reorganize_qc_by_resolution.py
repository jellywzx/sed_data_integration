#!/usr/bin/env python3
"""
在保留原有源库不变的前提下，根据时间分辨率校验结果，
将默认输入根目录（优先 Output_r_attr_fixed，其次 Output_r）下「所有 qc 文件夹中的 .nc」
按检测到的时间分辨率复制到新目录。

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
  python scripts/s2_reorganize_qc_by_resolution.py --dataset Huanghe
  python scripts/s2_reorganize_qc_by_resolution.py --dataset GloRiSe GloRiSe/SS
  python scripts/s2_reorganize_qc_by_resolution.py --dataset Huanghe --clear-all

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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
from pipeline_paths import (
    S1_REVIEW_OVERRIDES_CSV,
    S1_REVIEW_QUEUE_CSV,
    S1_VERIFY_CSV,
    S2_ORGANIZED_DIR,
    S2_OTHER_SUMMARY_CSV,
    S2_OTHER_DETAILS_CSV,
    RESOLUTION_DIRS,
)
from qc_contract import ensure_stage1_alias_parity
from time_resolution import (
    get_preferred_output_root,
    should_treat_irregular_as_daily,
    sync_temporal_resolution_attrs,
)

# 项目根默认优先使用 Output_r_attr_fixed；可通过 OUTPUT_R_ROOT 覆盖
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = get_preferred_output_root(SCRIPT_DIR)
# 校验结果 CSV（相对 ROOT_DIR）；步骤 s1 输出
VERIFY_CSV = S1_VERIFY_CSV
REVIEW_QUEUE_CSV = S1_REVIEW_QUEUE_CSV
REVIEW_OVERRIDES_CSV = S1_REVIEW_OVERRIDES_CSV
# 新目录名（相对 ROOT_DIR），仅包含 qc 下 nc 按分辨率整理后的副本
OUT_DIR = S2_ORGANIZED_DIR
# 并行执行数：
#   - 阶段 1：ThreadPoolExecutor 并行复制
#   - 阶段 2：ProcessPoolExecutor 并行标准化属性
DEFAULT_WORKERS = 16
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


def normalize_dataset_selector(value: str) -> str:
    """标准化 --dataset 传入值，兼容大小写、逗号和简单空白差异。"""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("\\", "/")
    text = re.sub(r"\s*/\s*", "/", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip(" _/").lower()


def split_dataset_selectors(values) -> set:
    """解析 --dataset 参数，支持空格分隔或逗号分隔多个数据集。"""
    keep = set()
    for raw in values or []:
        for piece in str(raw).split(","):
            norm = normalize_dataset_selector(piece)
            if norm:
                keep.add(norm)
    return keep


def get_dataset_parts_from_path(path: str, root_dir: Path):
    """提取相对 ROOT_DIR 的数据集层级（跳过最前面的分辨率目录）。"""
    try:
        p = Path(path).resolve()
        root = root_dir.resolve()
        rel = p.relative_to(root)
        parts = rel.parts
        if "qc" in parts:
            before = parts[: parts.index("qc")]
        else:
            before = parts[:-1]
        if len(before) >= 2:
            return tuple(str(x) for x in before[1:])
        if len(before) == 1:
            return (str(before[0]),)
    except Exception:
        pass
    return tuple()


def get_dataset_filter_aliases(path: str, root_dir: Path) -> set:
    """生成可用于 --dataset 匹配的别名。"""
    aliases = set()
    source = get_source_from_path(path, root_dir)
    if source:
        aliases.add(normalize_dataset_selector(source))

    dataset_parts = get_dataset_parts_from_path(path, root_dir)
    if dataset_parts:
        aliases.add(normalize_dataset_selector(dataset_parts[0]))
        aliases.add(normalize_dataset_selector("/".join(dataset_parts)))
        aliases.add(normalize_dataset_selector("_".join(dataset_parts)))

    return {x for x in aliases if x}


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


def _read_review_queue(root_dir: Path):
    review_path = root_dir / REVIEW_QUEUE_CSV
    if not review_path.is_file():
        return pd.DataFrame(), review_path
    try:
        df = pd.read_csv(review_path, keep_default_na=False)
    except Exception as exc:
        raise SystemExit("错误：无法读取人工审核队列 {}: {}".format(review_path, exc))
    if len(df) == 0:
        return df, review_path
    if "review_required" in df.columns:
        mask = df["review_required"].astype(str).str.strip().str.lower().isin(("1", "true", "yes"))
        df = df[mask].copy()
    return df, review_path


def _copy_one(item):
    """单次复制，供线程池调用。返回 (res_dir_name, dest_path, err)。"""
    src_path, dest_path, res_dir_name = item[:3]
    try:
        shutil.copy2(src_path, dest_path)
        return (res_dir_name, str(dest_path), None)
    except Exception as e:
        return (res_dir_name, None, (str(src_path), str(e)))


def _normalize_one(item):
    """单次同步时间分辨率属性并标准化全局属性，供进程池调用。"""
    dest_path_str, sync_target_resolution, sync_reason = item
    try:
        from attr_normalizer import normalize_nc_attrs

        if sync_target_resolution:
            sync_temporal_resolution_attrs(
                dest_path_str,
                target_resolution=sync_target_resolution,
                stage="s2",
                reason=sync_reason,
            )
        normalize_nc_attrs(dest_path_str)
        return (dest_path_str, None)
    except Exception as exc:
        return (dest_path_str, str(exc))


def _get_s2_copy_resolution(row):
    """返回 s2 副本应回写的时间分辨率。

    仅对标准目录 daily/monthly/annual/climatology 回写；
    other 只是收纳目录，不回写为业务分辨率。
    """
    resolution_dir = str(row.get("resolution_dir", "") or "").strip().lower()
    if resolution_dir in ("daily", "monthly", "annual", "climatology"):
        return resolution_dir
    return ""


def _get_s2_copy_reason(row):
    raw_freq = str(row.get("raw_detected_frequency", row.get("detected_frequency", "")) or "").strip().lower()
    detected_freq = str(row.get("detected_frequency", "") or "").strip().lower()
    resolution_dir = str(row.get("resolution_dir", "") or "").strip().lower()

    if raw_freq == "irregular" and resolution_dir == "daily":
        return "s2 irregular secondary check"
    if raw_freq == "hourly" and resolution_dir == "daily":
        return "s2 mapped hourly to daily"
    if raw_freq == "quarterly" and resolution_dir == "monthly":
        return "s2 mapped quarterly to monthly"
    if raw_freq == "single_point" and resolution_dir == "daily":
        return "s2 mapped single_point to daily"
    if detected_freq == "annual" and resolution_dir == "climatology":
        return "s2 aligned annual metadata to climatology"
    if resolution_dir:
        return "s2 aligned organized copy to final resolution"
    return ""


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


def clear_output_resolution_dirs(out_base: Path):
    """清空 s2 输出目录下的已有内容，避免残留旧文件影响重跑。"""
    cleared = []
    for sub in RESOLUTION_DIRS + LEGACY_RESOLUTION_DIRS:
        d = out_base / sub
        if not d.exists():
            continue
        for child in d.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except FileNotFoundError:
                continue
        cleared.append(d)
    return cleared


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
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr

    class _TeeStream:
        def __init__(self, stream, log_file):
            self._stream = stream
            self._log_file = log_file

        def write(self, data):
            self._stream.write(data)
            try:
                self._log_file.write(data)
                self._log_file.flush()
            except (ValueError, OSError):
                pass

        def flush(self):
            self._stream.flush()
            try:
                self._log_file.flush()
            except (ValueError, OSError):
                pass

    def _close_log_file():
        if sys.stdout is not orig_stdout:
            sys.stdout = orig_stdout
        if sys.stderr is not orig_stderr:
            sys.stderr = orig_stderr
        try:
            log_fp.close()
        except (ValueError, OSError):
            pass

    sys.stdout = _TeeStream(sys.stdout, log_fp)
    sys.stderr = _TeeStream(sys.stderr, log_fp)
    atexit.register(_close_log_file)
    _LOG_TEE_ENABLED = True


def main():
    _enable_script_logging()
    ap = argparse.ArgumentParser(description="按时间分辨率校验结果将 qc 下 nc 复制到新目录（数据源_分辨率_原名）")
    ap.add_argument("--out-dir", "-o", default=OUT_DIR, help=f"新目录名（相对 Output_r），默认 {OUT_DIR}")
    ap.add_argument("--verify-csv", default=VERIFY_CSV, help=f"校验结果 CSV 路径，默认 {VERIFY_CSV}")
    ap.add_argument(
        "--dataset",
        nargs="+",
        help="只处理指定数据集，可传多个；支持 source、顶层目录名、GloRiSe/SS，也支持逗号分隔",
    )
    ap.set_defaults(clear_mode="auto")
    ap.add_argument(
        "--clear-all",
        "--clear",
        dest="clear_mode",
        action="store_const",
        const="all",
        help="复制前清空整个输出目录下各时间语义目录。危险操作；在 --dataset 模式下也会生效。",
    )
    ap.add_argument(
        "--no-clear",
        dest="clear_mode",
        action="store_const",
        const="none",
        help="跳过预清空，保留输出目录中已有文件。",
    )
    ap.add_argument(
        "--workers",
        "-j",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help=f"并行执行数，阶段1用于复制线程数，阶段2用于属性标准化进程数，默认 {DEFAULT_WORKERS}",
    )
    ap.add_argument("--other-summary-out", default=S2_OTHER_SUMMARY_CSV, help="other 分类汇总输出 CSV")
    ap.add_argument("--other-details-out", default=S2_OTHER_DETAILS_CSV, help="other 分类明细输出 CSV")
    args = ap.parse_args()

    root_dir = Path(ROOT_DIR).resolve()
    if not root_dir.is_dir():
        print(f"错误：根目录不存在: {root_dir}", file=sys.stderr)
        sys.exit(1)

    ensure_stage1_alias_parity()

    review_queue, review_path = _read_review_queue(root_dir)
    if len(review_queue) > 0:
        overrides_path = root_dir / REVIEW_OVERRIDES_CSV
        print("错误：存在尚未处理的时间语义冲突，s2 已阻断。", file=sys.stderr)
        print("请先处理人工审核队列：{}".format(review_path), file=sys.stderr)
        print("处理后将结论写入 override 文件：{}".format(overrides_path), file=sys.stderr)
        sys.exit(1)

    verify_path = root_dir / args.verify_csv
    if not verify_path.is_file():
        print(f"错误：未找到校验结果 {verify_path}，请先运行 s1_verify_time_resolution.py", file=sys.stderr)
        sys.exit(1)

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
    df["source"] = df["path"].apply(lambda p: get_source_from_path(p, root_dir))

    if args.dataset:
        keep = split_dataset_selectors(args.dataset)
        if not keep:
            print("错误：--dataset 未解析出有效的数据集名称。", file=sys.stderr)
            sys.exit(1)

        dataset_aliases = df["path"].apply(lambda p: get_dataset_filter_aliases(p, root_dir))
        mask = dataset_aliases.apply(lambda aliases: bool(aliases & keep))
        df = df[mask].copy()

        print("按数据集筛选：{}".format(", ".join(sorted(keep))))
        print(f"命中的 qc 文件数：{len(df)}")

        if len(df) == 0:
            available_aliases = sorted(alias for alias in dataset_aliases.explode().dropna().astype(str).unique())
            preview = ", ".join(available_aliases[:20]) if available_aliases else "(无可用数据集)"
            print(
                "错误：--dataset 未匹配到任何 qc 文件。可尝试传入顶层目录名、source，或类似 GloRiSe/SS 的写法。",
                file=sys.stderr,
            )
            print(f"可用筛选名示例：{preview}", file=sys.stderr)
            sys.exit(1)

        matched_sources = sorted(df["source"].dropna().astype(str).unique())
        preview = ", ".join(matched_sources[:20])
        print(f"命中的 source：{preview}")
        if len(matched_sources) > 20:
            print(f"  ... 共 {len(matched_sources)} 个 source")

    if "final_semantics" in df.columns:
        df["resolution_dir"] = df["final_semantics"].apply(resolution_from_semantics)
    elif "temporal_semantics" in df.columns:
        df["resolution_dir"] = df["temporal_semantics"].apply(resolution_from_semantics)
    else:
        df["resolution_dir"] = df["detected_frequency"].apply(resolution_from_semantics)

    # irregular 的二次判定：若时间轴表现为离散日值记录，则改归 daily
    # irregular_mask = df["resolution_dir"].astype(str).str.strip().str.lower() == "irregular"

    if "final_semantics" in df.columns:
        irregular_mask = df["final_semantics"].astype(str).str.strip().str.lower() == "irregular"
    elif "temporal_semantics" in df.columns:
        irregular_mask = df["temporal_semantics"].astype(str).str.strip().str.lower() == "irregular"
    else:
        irregular_mask = df["detected_frequency"].astype(str).str.strip().str.lower() == "irregular"

    irregular_idx = df[irregular_mask].index.tolist()
    n_irregular_to_daily = 0
    for idx in tqdm(irregular_idx, desc="判定 irregular -> daily", unit="file"):
        p = Path(df.at[idx, "path"])
        if p.is_file() and should_treat_irregular_as_daily(p):
            df.at[idx, "resolution_dir"] = "daily"
            n_irregular_to_daily += 1

    if n_irregular_to_daily > 0:
        print(f"irregular 二次判定改归 daily: {n_irregular_to_daily} 个文件")

    out_base = root_dir / args.out_dir
    for sub in RESOLUTION_DIRS:
        (out_base / sub).mkdir(parents=True, exist_ok=True)
    for legacy_sub in LEGACY_RESOLUTION_DIRS:
        legacy_dir = out_base / legacy_sub
        if legacy_dir.exists():
            legacy_dir.mkdir(parents=True, exist_ok=True)

    dataset_mode = bool(args.dataset)
    if args.clear_mode == "all":
        should_clear = True
    elif args.clear_mode == "none":
        should_clear = False
    else:
        should_clear = not dataset_mode

    if dataset_mode and args.clear_mode == "auto":
        print("检测到 --dataset：默认跳过预清空输出目录；如确实需要全量清空，请显式传入 --clear-all。")

    if should_clear:
        cleared_dirs = clear_output_resolution_dirs(out_base)
        if cleared_dirs:
            print("运行前已清空输出目录：")
            for d in cleared_dirs:
                print(f"  {d}")
        else:
            print("运行前清空输出目录：未发现可清理内容")
    else:
        if args.clear_mode == "none":
            print("已跳过预清空输出目录（--no-clear）")
        else:
            print("已跳过预清空输出目录（auto 模式）")

    df["stem"] = df["path"].apply(lambda p: Path(p).stem)
    df["safe_source"] = df["source"].apply(safe_fname_part)
    df["safe_stem"] = df["stem"].apply(safe_fname_part)

    # 已使用的文件名（不含 .nc），按 resolution 目录记录，用于生成唯一名
    used = {}
    for r in RESOLUTION_DIRS:
        used[r] = set()

    copied = {r: 0 for r in RESOLUTION_DIRS}
    skipped = 0
    tasks = []  # (src_path, dest_path, res_dir_name, sync_target_resolution, sync_reason)

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
        tasks.append(
            (
                src_path,
                dest_path,
                res_dir_name,
                _get_s2_copy_resolution(row),
                _get_s2_copy_reason(row),
            )
        )

    workers = max(1, int(args.workers))
    copy_errors = []
    attr_errors = []
    normalize_tasks = []

    print("\n阶段 1：并行复制")

    if workers == 1:
        for item in tqdm(tasks, desc="复制文件", unit="file"):
            res_dir_name, dest_path_str, err = _copy_one(item)
            if err:
                copy_errors.append(err)
            else:
                copied[res_dir_name] += 1
                normalize_tasks.append(
                    (
                        dest_path_str,
                        item[3],
                        item[4],
                    )
                )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_item = {}
            for item in tasks:
                fut = executor.submit(_copy_one, item)
                future_to_item[fut] = item
            for fut in tqdm(as_completed(future_to_item), total=len(future_to_item), desc="并行复制", unit="file"):
                res_dir_name, dest_path_str, err = fut.result()
                if err:
                    copy_errors.append(err)
                else:
                    copied[res_dir_name] += 1
                    item = future_to_item[fut]
                    normalize_tasks.append(
                        (
                            dest_path_str,
                            item[3],
                            item[4],
                        )
                    )

    print(f"新目录: {out_base}")
    print(f"已处理 qc 下 nc 数量: {len(df)}（跳过不存在: {skipped}）")
    for r in RESOLUTION_DIRS:
        print(f"  {r}: {copied[r]} 个文件")
    if copy_errors:
        print(f"复制失败 {len(copy_errors)} 个:")
        for p, e in copy_errors[:10]:
            print(f"  {p} -> {e}")
        if len(copy_errors) > 10:
            print(f"  ... 共 {len(copy_errors)} 个")
    else:
        print("全部复制完成。")

    if normalize_tasks:
        print("\n阶段 2：并行回写副本时间分辨率属性并标准化全局属性")
        if workers == 1:
            for item in tqdm(normalize_tasks, desc="标准化属性", unit="file"):
                _, err = _normalize_one(item)
                if err:
                    attr_errors.append((item[0], err))
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(_normalize_one, item) for item in normalize_tasks]
                for fut in tqdm(as_completed(futures), total=len(futures), desc="并行标准化属性", unit="file"):
                    dest_path_str, err = fut.result()
                    if err:
                        attr_errors.append((dest_path_str, err))

        if attr_errors:
            print(f"[s2] WARNING: 属性标准化失败 {len(attr_errors)} 个:")
            for p, e in attr_errors[:10]:
                print(f"  {p} -> {e}")
            if len(attr_errors) > 10:
                print(f"  ... 共 {len(attr_errors)} 个")
        else:
            print("全部文件属性标准化完成。")
    else:
        print("\n阶段 2：无已复制文件，跳过属性标准化。")

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

    sys.exit(1 if copy_errors else 0)


if __name__ == "__main__":
    main()
