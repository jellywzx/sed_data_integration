#!/usr/bin/env python3
"""Standalone script: generate fig_active_units_by_year.png from release products.

Usage:
    python plot_active_units_by_year.py
    python plot_active_units_by_year.py --release-dir /path/to/release --out-dir /path/to/output
    python plot_active_units_by_year.py -o my_figure.png
"""
# ---- Library path setup: MUST happen before any extension-module imports ----
import os as _os
import ctypes as _ctypes
from pathlib import Path as _Path
_conda_lib = "/share/home/dq134/.conda/envs/wzx/lib"
if _os.path.isdir(_conda_lib):
    _os.environ["LD_LIBRARY_PATH"] = _conda_lib + _os.pathsep + _os.environ.get("LD_LIBRARY_PATH", "")
    try:
        _ctypes.CDLL(str(_Path(_conda_lib) / "libstdc++.so.6"), mode=_ctypes.RTLD_GLOBAL)
    except Exception:
        pass
del _os, _ctypes, _Path, _conda_lib
# ---------------------------------------------------------------------------

# ---- Package path setup: make stats_release importable from any cwd -------
import sys as _sys
_script_dir = Path(__file__).resolve().parent
_package_root = _script_dir.parent.parent
if str(_package_root) not in _sys.path:
    _sys.path.insert(0, str(_package_root))
del _script_dir, _package_root, _sys
# ---------------------------------------------------------------------------

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from stats_release.release_io import (
    add_common_args,
    clean_text,
    context_from_args,
    setup_matplotlib,
)
from stats_release.release_paths import MATRIX_PRODUCTS
from stats_release.common_stats import VARIABLES, decode_time_axis, save_figure
from stats_release.release_io import read_numeric_var, read_text_var




# ---- Absolute default paths for minimal release ----
_DEFAULT_RELEASE_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release_minimal")
_DEFAULT_OUT_DIR     = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output_other/stats_release_minimal")
# ----------------------------------------------------------------

def scan_by_year(ctx, resolution: str, file_name: str, row_chunk_size: int = 128) -> pd.DataFrame:
    """Scan a matrix NetCDF and return a DataFrame of (year, resolution, active_units, record_count_any).

    This is a focused subset of ``temporal._scan_matrix_temporal``, extracting only
    the per-year statistics needed for the active-units-by-year figure.
    """
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame()

    by_year = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        dates = decode_time_axis(ds)
        if len(dates) == 0 or "selected_source_index" not in ds.variables:
            return pd.DataFrame()
        years = dates.year.to_numpy()
        unique_years = sorted(set(int(y) for y in years))
        n_stations = int(len(ds.dimensions.get("n_stations", [])))

        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            selected = np.ma.asarray(ds.variables["selected_source_index"][start:stop, :]).filled(-1)
            any_mask = selected >= 0

            for year in unique_years:
                cols = years == year
                if not np.any(cols):
                    continue
                ymask = any_mask[:, cols]
                item = by_year.setdefault(year, {
                    "resolution": resolution,
                    "year": year,
                    "active_units": 0,
                    "record_count_any": 0,
                })
                active = np.any(ymask, axis=1)
                item["active_units"] += int(np.count_nonzero(active))
                item["record_count_any"] += int(np.count_nonzero(ymask))

    return pd.DataFrame(list(by_year.values()))


def build_active_units_by_year(ctx) -> pd.DataFrame:
    """Compute active_units_by_year from all matrix products (daily, monthly, annual)."""
    frames = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        df = scan_by_year(ctx, resolution, file_name)
        print(f"  {resolution}: {len(df)} years")
        if not df.empty:
            frames.append(df)
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return result


def write_figure(by_year: pd.DataFrame, output_path: Path, dpi: int) -> None:
    """Plot and save fig_active_units_by_year.png."""
    plt = setup_matplotlib()
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for resolution, group in by_year.groupby("resolution"):
        group = group.sort_values("year")
        ax.plot(group["year"], group["active_units"], label=str(resolution))
    ax.set_xlabel("Year")
    ax.set_ylabel("Active units")
    ax.set_title("Active units by year")
    ax.legend(frameon=False)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    save_figure(fig, output_path, dpi=dpi, also_pdf=False)
    plt.close(fig)
    print(f"Wrote {output_path}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate fig_active_units_by_year.png from release products."
    )
    add_common_args(parser, "temporal")
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output PNG path (default: <out-dir>/figures/fig_active_units_by_year.png)"
    )
    args = parser.parse_args(argv)
    # Override defaults with absolute minimal-release paths
    if args.release_dir == str(Path.cwd() / 'output' / 'sed_reference_release'):
        args.release_dir = str(_DEFAULT_RELEASE_DIR)
    if args.out_dir == str(Path.cwd() / 'output_other' / 'stats_release' / 'temporal'):
        args.out_dir = str(_DEFAULT_OUT_DIR)
    ctx = context_from_args(args)
    dpi = max(72, int(args.dpi))

    print("Scanning matrix products for yearly active unit counts ...")
    by_year = build_active_units_by_year(ctx)
    if by_year.empty:
        print("ERROR: no temporal data found in matrix products.", file=sys.stderr)
        return 1

    output_path = Path(args.output) if args.output else ctx.figures_dir() / "fig_active_units_by_year.png"
    write_figure(by_year, output_path, dpi)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
