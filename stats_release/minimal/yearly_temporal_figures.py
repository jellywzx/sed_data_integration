#!/usr/bin/env python3
"""Build yearly temporal tables and figures for sed_reference_release_minimal."""

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

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from stats_release.common_stats import VARIABLES, decode_time_axis, save_figure
from stats_release.release_io import ReleaseContext, read_numeric_var, setup_matplotlib, write_csv
from stats_release.release_paths import MATRIX_PRODUCTS


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MINIMAL_RELEASE_DIR = PROJECT_ROOT / "output" / "sed_reference_release_minimal"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output_other" / "stats_release_minimal" / "temporal"
YEARLY_COLUMNS = (
    "resolution",
    "year",
    "active_units",
    "active_clusters",
    "active_units_Q",
    "active_units_SSC",
    "active_units_SSL",
    "record_count_any",
    "record_count_Q",
    "record_count_SSC",
    "record_count_SSL",
)
VARIABLE_COLORS = {
    "Q": "#4c78a8",
    "SSC": "#f58518",
    "SSL": "#54a24b",
}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Build active-units-by-year and records-by-year figures for sed_reference_release_minimal."
    )
    parser.add_argument(
        "--release-dir",
        default=str(DEFAULT_MINIMAL_RELEASE_DIR),
        help="Path to output/sed_reference_release_minimal.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Output directory for yearly temporal tables and figures.",
    )
    parser.add_argument(
        "--row-chunk-size",
        type=int,
        default=128,
        help="Station-row chunk size for matrix NetCDF scans.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Figure DPI.")
    parser.add_argument("--skip-figures", action="store_true", help="Only write the yearly CSV table.")
    args = parser.parse_args(argv)
    args.release_dir = Path(args.release_dir).expanduser().resolve()
    args.out_dir = Path(args.out_dir).expanduser().resolve()
    args.row_chunk_size = max(1, int(args.row_chunk_size))
    args.dpi = max(72, int(args.dpi))
    return args


def scan_matrix_by_year(ctx: ReleaseContext, resolution: str, file_name: str, row_chunk_size: int) -> pd.DataFrame:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(columns=YEARLY_COLUMNS)

    by_year = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        dates = decode_time_axis(ds)
        if len(dates) == 0:
            return pd.DataFrame(columns=YEARLY_COLUMNS)

        years = dates.year.to_numpy()
        unique_years = sorted(set(int(year) for year in years))
        n_stations = int(len(ds.dimensions.get("n_stations", [])))

        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            any_mask = np.zeros((stop - start, len(dates)), dtype=bool)
            var_masks = {}
            for variable in VARIABLES:
                if variable in ds.variables:
                    var_masks[variable] = np.isfinite(read_numeric_var(ds, variable, key=(slice(start, stop), slice(None))))
                else:
                    var_masks[variable] = np.zeros_like(any_mask, dtype=bool)
                any_mask |= var_masks[variable]
            if "selected_source_index" in ds.variables:
                selected = np.ma.asarray(ds.variables["selected_source_index"][start:stop, :]).filled(-1)
                any_mask = selected >= 0

            for year in unique_years:
                cols = years == year
                if not np.any(cols):
                    continue
                ymask = any_mask[:, cols]
                item = by_year.setdefault(
                    year,
                    {
                        "resolution": resolution,
                        "year": year,
                        "active_units": 0,
                        "active_clusters": 0,
                        "active_units_Q": 0,
                        "active_units_SSC": 0,
                        "active_units_SSL": 0,
                        "record_count_any": 0,
                        "record_count_Q": 0,
                        "record_count_SSC": 0,
                        "record_count_SSL": 0,
                    },
                )
                active = np.any(ymask, axis=1)
                item["active_units"] += int(np.count_nonzero(active))
                item["active_clusters"] += int(np.count_nonzero(active))
                item["record_count_any"] += int(np.count_nonzero(ymask))
                for variable in VARIABLES:
                    item[f"active_units_{variable}"] += int(np.count_nonzero(np.any(var_masks[variable][:, cols], axis=1)))
                    item[f"record_count_{variable}"] += int(np.count_nonzero(var_masks[variable][:, cols]))

    frame = pd.DataFrame(by_year.values(), columns=YEARLY_COLUMNS)
    return frame.sort_values(["resolution", "year"]).reset_index(drop=True)


def build_active_units_by_year(ctx: ReleaseContext, row_chunk_size: int) -> pd.DataFrame:
    frames = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        frame = scan_matrix_by_year(ctx, resolution, file_name, row_chunk_size)
        print(f"{resolution}: {len(frame)} years")
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=YEARLY_COLUMNS)
    return pd.concat(frames, ignore_index=True).loc[:, YEARLY_COLUMNS]


def plot_yearly_line(by_year: pd.DataFrame, value_col: str, ylabel: str, title: str, output_path: Path, dpi: int) -> None:
    plt = setup_matplotlib()
    fig, ax = plt.subplots(figsize=(9, 4.5))
    for resolution, group in by_year.groupby("resolution"):
        ax.plot(group["year"], group[value_col], label=str(resolution))
    ax.set_xlabel("Year")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(frameon=False)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    save_figure(fig, output_path, dpi=dpi)
    plt.close(fig)


def plot_variable_records_units_by_resolution(by_year: pd.DataFrame, output_path: Path, dpi: int) -> None:
    plt = setup_matplotlib()
    resolutions = [resolution for resolution in MATRIX_PRODUCTS if resolution in set(by_year["resolution"].astype(str))]
    if not resolutions:
        return

    fig, axes = plt.subplots(len(resolutions), 1, figsize=(10, 3.1 * len(resolutions)), sharex=True)
    if len(resolutions) == 1:
        axes = [axes]

    legend_handles = []
    legend_labels = []
    for index, (ax, resolution) in enumerate(zip(axes, resolutions)):
        group = by_year[by_year["resolution"].astype(str).eq(resolution)].sort_values("year")
        ax2 = ax.twinx()
        for variable in VARIABLES:
            color = VARIABLE_COLORS.get(variable, None)
            records_line = ax.plot(
                group["year"],
                pd.to_numeric(group[f"record_count_{variable}"], errors="coerce").fillna(0),
                color=color,
                linewidth=1.8,
                label=f"{variable} records",
            )[0]
            units_line = ax2.plot(
                group["year"],
                pd.to_numeric(group[f"active_units_{variable}"], errors="coerce").fillna(0),
                color=color,
                linewidth=1.6,
                linestyle="--",
                label=f"{variable} units",
            )[0]
            if index == 0:
                legend_handles.extend([records_line, units_line])
                legend_labels.extend([f"{variable} records", f"{variable} units"])

        ax.set_ylabel("Records")
        ax2.set_ylabel("Active units")
        ax.set_title(str(resolution).capitalize())
        ax.grid(alpha=0.3)
        ax2.grid(False)

    axes[-1].set_xlabel("Year")
    fig.suptitle("Variable records and active units by year and resolution", y=0.995)
    if legend_handles:
        fig.legend(legend_handles, legend_labels, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=(0, 0, 1, 0.91))
    save_figure(fig, output_path, dpi=dpi)
    plt.close(fig)


def write_figures(by_year: pd.DataFrame, figures_dir: Path, dpi: int) -> None:
    figures_dir.mkdir(parents=True, exist_ok=True)
    plot_yearly_line(
        by_year,
        "active_units",
        "Active units",
        "Active units by year",
        figures_dir / "fig_active_units_by_year.png",
        dpi,
    )
    plot_yearly_line(
        by_year,
        "record_count_any",
        "Records",
        "Records by year",
        figures_dir / "fig_records_by_year_variable.png",
        dpi,
    )
    plot_variable_records_units_by_resolution(
        by_year,
        figures_dir / "fig_variable_records_units_by_year_resolution.png",
        dpi,
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    ctx = ReleaseContext(release_dir=args.release_dir, out_dir=args.out_dir, strict_release_only=True)
    tables_dir = ctx.output_path("tables", "x").parent

    by_year = build_active_units_by_year(ctx, args.row_chunk_size)
    if by_year.empty:
        print("ERROR: no yearly matrix temporal data found.", file=sys.stderr)
        return 1

    csv_path = write_csv(by_year, tables_dir / "table_active_units_by_year.csv")
    print(f"Wrote {csv_path}")

    if not args.skip_figures:
        write_figures(by_year, ctx.figures_dir(), args.dpi)
        print(f"Wrote figures to {ctx.figures_dir()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
