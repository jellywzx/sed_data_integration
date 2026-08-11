#!/usr/bin/env python3
"""Write daily/monthly/annual final-QC counts into the qc_flags stats output.

This is a small companion to :mod:`stats_release.qc_flags`.  It deliberately
reuses ``_count_flags_for_product`` so the counting and flag-meaning logic stay
identical to the main QC statistics module, while adding the temporal-resolution
breakdown needed by manuscript Table 7.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.qc_flags import _count_flags_for_product
from stats_release.release_io import add_common_args, context_from_args, write_csv
from stats_release.release_paths import MATRIX_PRODUCTS


FINAL_FLAG_VARIABLES = ("Q_flag", "SSC_flag", "SSL_flag")


def build_matrix_final_flag_stats(ctx, chunk_size: int) -> pd.DataFrame:
    """Count final Q/SSC/SSL flags separately for daily, monthly, annual matrices."""
    pieces = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        counts, _ = _count_flags_for_product(ctx, file_name, resolution, chunk_size)
        if counts.empty:
            continue
        counts = counts[counts["flag_variable"].isin(FINAL_FLAG_VARIABLES)].copy()
        counts.insert(0, "resolution", counts.pop("product"))
        pieces.append(counts)

    if not pieces:
        return pd.DataFrame(
            columns=["resolution", "flag_variable", "flag_value", "flag_meaning", "count", "percent"]
        )
    return pd.concat(pieces, ignore_index=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Build daily/monthly/annual final QC-flag statistics using stats_release.qc_flags counting logic."
    )
    add_common_args(parser, "qc_flags")
    parser.add_argument("--chunk-size", type=int, default=500000)
    args = parser.parse_args(argv)
    ctx = context_from_args(args)

    frame = build_matrix_final_flag_stats(ctx, max(1, int(args.chunk_size)))
    tables_dir = ctx.output_path("tables", "x").parent
    out_path = tables_dir / "table_qc_matrix_final_flags_by_resolution.csv"
    write_csv(frame, out_path)
    print("Wrote matrix final QC flag statistics to {}".format(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
