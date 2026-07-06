#!/usr/bin/env python3
"""Standalone: generate fig_records_by_year_variable.png from a temporal by-year CSV.

Usage:
    python plot_records_by_year.py [input_csv] [output_png] [--dpi DPI]

The input CSV should contain at least the columns:
    resolution, year, record_count_any

The default input is ``table_active_units_by_year.csv`` from the
sed_reference_release_minimal temporal-stats output.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd


# ---- Default paths for sed_reference_release_minimal (absolute) ----------
_SCRIPTS_BASIN = Path('/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test')
DEFAULT_RELEASE_DIR = _SCRIPTS_BASIN / "output" / "sed_reference_release_minimal"
DEFAULT_STATS_DIR   = _SCRIPTS_BASIN / "output_other" / "stats_release_minimal"
DEFAULT_INPUT_CSV   = DEFAULT_STATS_DIR / "tables" / "table_active_units_by_year.csv"
# -------------------------------------------------------------------------


def setup_matplotlib():
    """Import matplotlib with non-interactive Agg backend."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate fig_records_by_year_variable.png from a by-year CSV."
    )
    parser.add_argument(
        "input_csv",
        type=str,
        nargs="?",
        default=str(DEFAULT_INPUT_CSV),
        help="Path to the CSV (resolution, year, record_count_any, …) "
             f"(default: {DEFAULT_INPUT_CSV})",
    )
    parser.add_argument(
        "output_png",
        type=str,
        nargs="?",
        default="fig_records_by_year_variable.png",
        help="Output PNG path (default: fig_records_by_year_variable.png)",
    )
    parser.add_argument(
        "--dpi",
        type=int,
        default=300,
        help="Figure DPI (default: 300)",
    )
    args = parser.parse_args()

    # ---- Read & validate input ----
    df = pd.read_csv(args.input_csv)
    required = {"resolution", "year", "record_count_any"}
    missing = required - set(df.columns)
    if missing:
        print(
            f"Error: input CSV is missing required columns: {missing}",
            file=sys.stderr,
        )
        print(f"  Available columns: {list(df.columns)}", file=sys.stderr)
        return 1

    # ---- Plot ----
    plt = setup_matplotlib()

    fig, ax = plt.subplots(figsize=(9, 4.5))
    for resolution, group in df.groupby("resolution"):
        ax.plot(
            group["year"],
            group["record_count_any"],
            label=str(resolution),
        )
    ax.set_xlabel("Year")
    ax.set_ylabel("Records")
    ax.set_title("Records by year")
    ax.legend(frameon=False)
    ax.grid(alpha=0.3)
    fig.tight_layout()

    # ---- Save ----
    out_path = Path(args.output_png)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(out_path), dpi=args.dpi)
    print(f"Saved figure to {out_path}")
    plt.close(fig)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
