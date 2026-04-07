#!/usr/bin/env python3
"""
步骤 s6（出图主入口）：一次性调用所有 plot_merged_stations_*.py 子脚本，
生成完整的泥沙参考数据集可视化图集。

生成图像（默认输出至 scripts_basin_test/output/）：
  s6_plot_records_map.png         — 全球站点分布（按记录数着色）
  s6_plot_records_hist.png        — 记录数直方图
  s6_plot_span_map.png            — 全球站点分布（按时间跨度着色）
  s6_plot_span_hist.png           — 时间跨度直方图
  s6_plot_frequency_map.png       — 全球站点分布（按采样频率分类着色）
  s6_plot_frequency_hist.png      — 采样频率直方图（log 轴）
  s6_plot_sources_map.png         — 全球站点分布（按数据来源着色）
  s6_plot_sources_bar.png         — 各来源站点数柱状图
  s6_plot_sources.csv             — 每站点来源明细
  s6_plot_variables_map.png       — 全球站点分布（按 Q/SSC/SSL 组合着色）
  s6_plot_variables_bar.png       — 变量组合柱状图 + 按分辨率分层柱状图
  s6_plot_yearly_coverage.png     — 逐年有效站点数堆叠面积图
  s6_plot_basin_area.png          — 集水面积分布（地图 + 直方图）
  s6_plot_basin_region.png        — Pfafstetter 一级大区站点数柱状图
  s6_plot_basin_quality.png       — 流域匹配质量分布柱状图

依赖：
  pip install matplotlib cartopy netCDF4 numpy pandas

用法（在 scripts_basin_test/ 下运行）：
  python plot/s6_plot_merged_nc_stations.py
  python plot/s6_plot_merged_nc_stations.py --nc /path/to/s6.nc --dpi 200
  python plot/s6_plot_merged_nc_stations.py --only records span   # 只运行指定子图
"""

import argparse
import atexit
import sys
from datetime import datetime
from pathlib import Path

# 确保 plot/ 和 scripts_basin_test/ 均在 sys.path
_PLOT_DIR    = Path(__file__).resolve().parent
_SCRIPTS_DIR = _PLOT_DIR.parent
for _p in (_PLOT_DIR, _SCRIPTS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from s6_plot_common import DEFAULT_NC, DEFAULT_S5_CSV, DEFAULT_OUT_DIR

# ─────────────────────────────────────────────────────────────────────────────
# 日志 tee
# ─────────────────────────────────────────────────────────────────────────────
_LOG_TEE_ENABLED = False


def _enable_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    log_path = Path(__file__).resolve().with_name("{}_log.txt".format(Path(__file__).stem))
    try:
        log_path.unlink(missing_ok=True)
    except Exception:
        pass
    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("===== Run started {} =====\n".format(
        datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()

    class _Tee:
        def __init__(self, s, f):
            self._s, self._f = s, f
        def write(self, d):
            self._s.write(d); self._f.write(d); self._f.flush()
        def flush(self):
            self._s.flush(); self._f.flush()

    sys.stdout = _Tee(sys.stdout, log_fp)
    sys.stderr = _Tee(sys.stderr, log_fp)
    atexit.register(log_fp.close)
    _LOG_TEE_ENABLED = True


# ─────────────────────────────────────────────────────────────────────────────
# 子图任务注册表
# ─────────────────────────────────────────────────────────────────────────────

def _run_records(nc, out_dir, dpi):
    from plot_merged_stations_records import main as _main
    sys.argv = ["plot_merged_stations_records.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_records"),
                "--dpi", str(dpi)]
    _main()


def _run_span(nc, out_dir, dpi):
    from plot_merged_stations_span import main as _main
    sys.argv = ["plot_merged_stations_span.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_span"),
                "--dpi", str(dpi)]
    _main()


def _run_frequency(nc, out_dir, dpi):
    from plot_merged_stations_frequency import main as _main
    sys.argv = ["plot_merged_stations_frequency.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_frequency"),
                "--dpi", str(dpi)]
    _main()


def _run_sources(nc, s5_csv, out_dir, dpi):
    from plot_merged_stations_sources import main as _main
    sys.argv = ["plot_merged_stations_sources.py",
                "--nc", nc, "--s5-csv", str(s5_csv),
                "--out", str(out_dir / "s6_plot_sources"),
                "--dpi", str(dpi)]
    _main()


def _run_variables(nc, out_dir, dpi):
    from plot_merged_stations_variables import main as _main
    sys.argv = ["plot_merged_stations_variables.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_variables"),
                "--dpi", str(dpi)]
    _main()


def _run_yearly(nc, out_dir, dpi):
    from plot_merged_stations_yearly_coverage import main as _main
    sys.argv = ["plot_merged_stations_yearly_coverage.py",
                "--nc", nc,
                "--out", str(out_dir / "s6_plot_yearly_coverage.png"),
                "--dpi", str(dpi)]
    _main()


def _run_basin(nc, out_dir, dpi):
    from plot_merged_stations_basin import main as _main
    sys.argv = ["plot_merged_stations_basin.py",
                "--nc", nc, "--out", str(out_dir / "s6_plot_basin"),
                "--dpi", str(dpi)]
    _main()


# 所有子任务：(key, 描述, 调用函数)
_ALL_TASKS = [
    ("records",   "记录数分布（地图 + 直方图）"),
    ("span",      "时间跨度分布（地图 + 直方图）"),
    ("frequency", "采样频率分布（地图 + 直方图）"),
    ("sources",   "数据来源分布（地图 + 柱状图）"),
    ("variables", "Q/SSC/SSL 变量可用性（地图 + 柱状图）"),
    ("yearly",    "逐年覆盖度（堆叠面积图）"),
    ("basin",     "流域特征（面积地图 + Pfaf 大区 + 匹配质量）"),
]


def main():
    _enable_logging()

    ap = argparse.ArgumentParser(
        description="步骤 s6 出图主入口：一次生成全部可视化图集",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--nc",     "-n", default=str(DEFAULT_NC),    help="s6 NetCDF 路径")
    ap.add_argument("--s5-csv",       default=str(DEFAULT_S5_CSV),help="s5 clustered CSV 路径（来源出图用）")
    ap.add_argument("--out-dir", "-o",default=str(DEFAULT_OUT_DIR),help="图像输出目录")
    ap.add_argument("--dpi",          type=int, default=150,       help="图像 DPI")
    ap.add_argument("--only", nargs="+",
                    choices=[t[0] for t in _ALL_TASKS],
                    help="只运行指定子图（空格分隔），如 --only records span")
    args = ap.parse_args()

    nc      = args.nc
    s5_csv  = Path(args.s5_csv)
    out_dir = Path(args.out_dir)
    dpi     = args.dpi
    out_dir.mkdir(parents=True, exist_ok=True)

    # 检查 NC
    if not Path(nc).is_file():
        print("ERROR: NC file not found: {}".format(nc))
        print("  请先运行 s6_basin_merge_to_nc.py 生成该文件。")
        sys.exit(1)

    run_set = set(args.only) if args.only else {t[0] for t in _ALL_TASKS}

    print("=" * 68)
    print("s6 出图主入口  NC={}".format(nc))
    print("输出目录: {}".format(out_dir))
    print("=" * 68)

    results = {}

    def _run(key, fn, *fn_args):
        if key not in run_set:
            return
        print("\n── {} ──".format(dict(_ALL_TASKS)[key]))
        try:
            fn(*fn_args)
            results[key] = "OK"
        except Exception as e:
            import traceback
            print("  [WARN] {} 出图失败: {}".format(key, e))
            traceback.print_exc()
            results[key] = "FAILED: {}".format(e)

    _run("records",   _run_records,   nc, out_dir, dpi)
    _run("span",      _run_span,      nc, out_dir, dpi)
    _run("frequency", _run_frequency, nc, out_dir, dpi)
    _run("sources",   _run_sources,   nc, s5_csv, out_dir, dpi)
    _run("variables", _run_variables, nc, out_dir, dpi)
    _run("yearly",    _run_yearly,    nc, out_dir, dpi)
    _run("basin",     _run_basin,     nc, out_dir, dpi)

    print("\n" + "=" * 68)
    print("完成汇总：")
    for key, desc in _ALL_TASKS:
        if key in results:
            status = "✓" if results[key] == "OK" else "✗"
            print("  {} {:12s} {}".format(status, key, results[key]))
    print("输出目录: {}".format(out_dir))
    print("=" * 68)


if __name__ == "__main__":
    main()
