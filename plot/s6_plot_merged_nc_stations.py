#!/usr/bin/env python3
"""
步骤 s6（出图主入口）：一次性调用所有 plot_merged_stations_*.py 子脚本，
生成完整的泥沙参考数据集可视化图集。

生成图像（默认输出至 scripts_basin_test/output/）：
  s6_plot_records_map.png
  s6_plot_records_hist.png
  s6_plot_span_map.png
  s6_plot_span_hist.png
  s6_plot_frequency_map.png
  s6_plot_frequency_hist.png
  s6_plot_sources_map.png
  s6_plot_sources_bar.png
  s6_plot_sources.csv
  s6_plot_variables_map.png
  s6_plot_variables_bar.png
  s6_plot_yearly_coverage.png
  s6_plot_basin_area.png
  s6_plot_basin_region.png
  s6_plot_basin_quality.png
"""

import argparse
import atexit
import sys
from datetime import datetime
from pathlib import Path

_PLOT_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _PLOT_DIR.parent
for _p in (_PLOT_DIR, _SCRIPTS_DIR):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from s6_plot_common import DEFAULT_NC, DEFAULT_OUT_DIR
from pipeline_paths import get_log_path

_LOG_TEE_ENABLED = False


def _enable_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return

    log_path = get_log_path(_SCRIPTS_DIR, "{}_log.txt".format(Path(__file__).stem))
    try:
        log_path.unlink(missing_ok=True)
    except Exception:
        pass

    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("===== Run started {} =====\n".format(datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()

    class _Tee:
        def __init__(self, stream, file_handle):
            self._stream = stream
            self._file_handle = file_handle

        def write(self, data):
            self._stream.write(data)
            try:
                self._file_handle.write(data)
                self._file_handle.flush()
            except Exception:
                pass

        def flush(self):
            self._stream.flush()
            try:
                self._file_handle.flush()
            except Exception:
                pass

    sys.stdout = _Tee(sys.stdout, log_fp)
    sys.stderr = _Tee(sys.stderr, log_fp)
    atexit.register(log_fp.close)
    _LOG_TEE_ENABLED = True


def _run_records(nc, out_dir, dpi):
    from plot_merged_stations_records import main as _main
    sys.argv = [
        "plot_merged_stations_records.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_records"),
        "--dpi", str(dpi),
    ]
    _main()


def _run_span(nc, out_dir, dpi):
    from plot_merged_stations_span import main as _main
    sys.argv = [
        "plot_merged_stations_span.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_span"),
        "--dpi", str(dpi),
    ]
    _main()


def _run_frequency(nc, out_dir, dpi):
    from plot_merged_stations_frequency import main as _main
    sys.argv = [
        "plot_merged_stations_frequency.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_frequency"),
        "--dpi", str(dpi),
    ]
    _main()


def _run_sources(nc, out_dir, dpi):
    from plot_merged_stations_sources import main as _main
    sys.argv = [
        "plot_merged_stations_sources.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_sources"),
        "--dpi", str(dpi),
    ]
    _main()


def _run_variables(nc, out_dir, dpi):
    from plot_merged_stations_variables import main as _main
    sys.argv = [
        "plot_merged_stations_variables.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_variables"),
        "--dpi", str(dpi),
    ]
    _main()


def _run_yearly(nc, out_dir, dpi):
    from plot_merged_stations_yearly_coverage import main as _main
    sys.argv = [
        "plot_merged_stations_yearly_coverage.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_yearly_coverage.png"),
        "--dpi", str(dpi),
    ]
    _main()


def _run_basin(nc, out_dir, dpi):
    from plot_merged_stations_basin import main as _main
    sys.argv = [
        "plot_merged_stations_basin.py",
        "--nc", nc,
        "--out", str(out_dir / "s6_plot_basin"),
        "--dpi", str(dpi),
    ]
    _main()


_ALL_TASKS = [
    ("records", "记录数分布（地图 + 直方图）", _run_records),
    ("span", "时间跨度分布（地图 + 直方图）", _run_span),
    ("frequency", "主时间类型分布（地图 + 柱状图）", _run_frequency),
    ("sources", "数据来源分布（地图 + 柱状图）", _run_sources),
    ("variables", "Q/SSC/SSL 变量可用性（地图 + 柱状图）", _run_variables),
    ("yearly", "逐年覆盖度（堆叠面积图）", _run_yearly),
    ("basin", "流域特征（面积地图 + Pfaf 大区 + 匹配质量）", _run_basin),
]


def main():
    _enable_logging()

    ap = argparse.ArgumentParser(description="步骤 s6 出图主入口：一次生成全部可视化图集")
    ap.add_argument("--nc", "-n", default=str(DEFAULT_NC), help="s6 NetCDF 路径")
    ap.add_argument("--out-dir", "-o", default=str(DEFAULT_OUT_DIR), help="图像输出目录")
    ap.add_argument("--dpi", type=int, default=150, help="图像 DPI")
    ap.add_argument(
        "--only",
        nargs="+",
        choices=[item[0] for item in _ALL_TASKS],
        help="只运行指定子图（空格分隔），如 --only records span",
    )
    args = ap.parse_args()

    nc = args.nc
    out_dir = Path(args.out_dir)
    dpi = args.dpi
    out_dir.mkdir(parents=True, exist_ok=True)

    if not Path(nc).is_file():
        print("ERROR: NC file not found: {}".format(nc))
        print("  请先运行 s6_basin_merge_to_nc.py 生成该文件。")
        sys.exit(1)

    run_set = set(args.only) if args.only else set(item[0] for item in _ALL_TASKS)

    print("=" * 68)
    print("s6 出图主入口  NC={}".format(nc))
    print("输出目录: {}".format(out_dir))
    print("=" * 68)

    results = {}

    for key, desc, runner in _ALL_TASKS:
        if key not in run_set:
            continue
        print("\n── {} ──".format(desc))
        try:
            runner(nc, out_dir, dpi)
            results[key] = "OK"
        except Exception as exc:
            import traceback
            print("  [WARN] {} 出图失败: {}".format(key, exc))
            traceback.print_exc()
            results[key] = "FAILED: {}".format(exc)

    print("\n" + "=" * 68)
    print("完成汇总：")
    for key, desc, _runner in _ALL_TASKS:
        if key in results:
            status = "✓" if results[key] == "OK" else "✗"
            print("  {} {:12s} {}".format(status, key, results[key]))
    print("输出目录: {}".format(out_dir))
    print("=" * 68)


if __name__ == "__main__":
    main()
