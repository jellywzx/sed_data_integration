#!/usr/bin/env python3
"""
s0：审计所有 QC NC 文件的全局属性结构。

扫描指定根目录下所有 qc/ 子目录中的 .nc 文件，
提取每个文件的 NetCDF 全局属性，汇总输出为：
  1. 详细 CSV  — 每行一个文件，列为所有出现过的属性键
  2. 可读汇总文本 — 按属性结构分组，每种结构打印一个代表文件的完整属性

直接运行：
  python s0_audit_qc_attributes.py
"""

import csv
import sys
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path

# =============================================================================
# ★ 用户配置区：按需修改以下变量
# =============================================================================

# QC 数据根目录（包含各数据集子文件夹，其下含 qc/*.nc）
ROOT_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r")

# 并行进程数（None = 自动使用全部 CPU 核数）
WORKERS = 32

# 输出文件路径
OUT_CSV     = ROOT_DIR / "s0_audit_qc_attributes.csv"
OUT_SUMMARY = ROOT_DIR / "s0_audit_qc_summary.txt"

# 属性值截断长度（避免超长字符串撑爆 CSV）
ATTR_MAX_LEN = 300

# =============================================================================


def _process_one(nc_path_str: str) -> dict:
    """读取单个 NC 文件的全局属性（在子进程中执行）。"""
    try:
        import netCDF4 as nc4
        with nc4.Dataset(nc_path_str, "r") as ds:
            attrs = {
                k: str(getattr(ds, k, ""))[:ATTR_MAX_LEN]
                for k in ds.ncattrs()
            }
        return {"path": nc_path_str, "error": "", "attrs": attrs}
    except Exception as exc:
        return {"path": nc_path_str, "error": str(exc)[:300], "attrs": {}}


def collect_nc_files(root: Path) -> list:
    """递归收集 root 下所有 qc/ 子目录中的 .nc 文件（绝对路径字符串列表）。"""
    return [str(p) for p in sorted(root.rglob("*.nc")) if "qc" in p.parts]


def run_audit():
    root    = ROOT_DIR.resolve()
    workers = WORKERS if WORKERS and WORKERS > 0 else cpu_count()
    out_csv     = Path(str(OUT_CSV).replace("/path/to/your/Output_r", str(root)))
    out_summary = Path(str(OUT_SUMMARY).replace("/path/to/your/Output_r", str(root)))

    nc_files = collect_nc_files(root)
    if not nc_files:
        print(f"[s0] 在 {root} 下未找到任何 qc/*.nc 文件，请检查 ROOT_DIR。")
        sys.exit(1)

    total = len(nc_files)
    print(f"[s0] 根目录      : {root}")
    print(f"[s0] QC NC 文件数 : {total}")
    print(f"[s0] 并行进程数   : {workers}")
    print(f"[s0] 开始读取属性 ...")

    # ── 并行处理 ──────────────────────────────────────────────────────────────
    results = []
    try:
        from tqdm import tqdm
        use_tqdm = True
    except ImportError:
        use_tqdm = False

    with Pool(processes=workers) as pool:
        it = pool.imap_unordered(_process_one, nc_files, chunksize=20)
        if use_tqdm:
            it = tqdm(it, total=total, unit="file", ncols=80)
        else:
            # 无 tqdm 时每处理 200 个打印一次进度
            _orig_it = it
            def _it_with_print():
                for i, r in enumerate(_orig_it, 1):
                    if i % 200 == 0 or i == total:
                        print(f"  [{i}/{total}]")
                    yield r
            it = _it_with_print()
        for res in it:
            results.append(res)

    # ── 统计所有属性键，按出现频次降序排列 ────────────────────────────────────
    key_count: dict = {}
    for r in results:
        for k in r["attrs"]:
            key_count[k] = key_count.get(k, 0) + 1
    all_keys = sorted(key_count, key=lambda k: (-key_count[k], k))

    # ── 写详细 CSV ─────────────────────────────────────────────────────────────
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["file_path", "error", "attr_signature"] + all_keys)
        for r in sorted(results, key=lambda x: x["path"]):
            sig = "|".join(sorted(r["attrs"].keys()))
            writer.writerow(
                [r["path"], r["error"], sig]
                + [r["attrs"].get(k, "") for k in all_keys]
            )
    print(f"[s0] 详细 CSV      : {out_csv}")

    # ── 按属性结构分组 ─────────────────────────────────────────────────────────
    sig_groups: dict = {}
    error_files = []
    for r in results:
        if r["error"]:
            error_files.append(r)
            continue
        sig = tuple(sorted(r["attrs"].keys()))
        sig_groups.setdefault(sig, []).append(r)

    sig_groups_sorted = sorted(
        sig_groups.items(), key=lambda x: (-len(x[1]), x[1][0]["path"])
    )

    # ── 写汇总报告 ─────────────────────────────────────────────────────────────
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    with open(out_summary, "w", encoding="utf-8") as f:
        ts = datetime.now().isoformat(timespec="seconds")
        f.write("QC NC 文件全局属性审计报告\n")
        f.write(f"生成时间     : {ts}\n")
        f.write(f"根目录       : {root}\n")
        f.write(f"总文件数     : {total}\n")
        f.write(f"读取失败     : {len(error_files)}\n")
        f.write(f"属性结构种数 : {len(sig_groups)}\n")
        f.write("\n")

        # 属性键出现频次总览
        f.write("── 属性键出现频次 ──\n")
        for k in all_keys:
            pct = key_count[k] / total * 100
            f.write(f"  {k:<45s}  {key_count[k]:>6d} 文件  ({pct:5.1f}%)\n")
        f.write("\n")

        # 各结构组详情
        for idx, (sig, files) in enumerate(sig_groups_sorted, 1):
            rep = files[0]
            f.write(f"\n{'='*70}\n")
            f.write(f"结构 {idx}/{len(sig_groups)}  共 {len(files)} 个文件\n")
            f.write(f"代表文件: {rep['path']}\n")
            f.write(f"{'='*70}\n")
            for k in sorted(rep["attrs"].keys()):
                f.write(f"  {k}: {rep['attrs'][k]}\n")
            f.write(f"\n  文件列表（共 {len(files)} 个）:\n")
            for r in files[:10]:
                f.write(f"    {r['path']}\n")
            if len(files) > 10:
                f.write(f"    ... 还有 {len(files) - 10} 个文件\n")

        # 读取失败文件
        if error_files:
            f.write(f"\n\n{'='*70}\n")
            f.write(f"读取失败文件（共 {len(error_files)} 个）\n")
            f.write(f"{'='*70}\n")
            for r in error_files:
                f.write(f"  {r['path']}\n")
                f.write(f"    错误: {r['error']}\n")

    print(f"[s0] 汇总报告      : {out_summary}")
    print(f"\n[s0] 完成！共 {total} 个文件，{len(sig_groups)} 种属性结构，{len(error_files)} 个读取失败。")


if __name__ == "__main__":
    run_audit()

