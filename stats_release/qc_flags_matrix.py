#!/usr/bin/env python3
"""Write daily/monthly/annual final-QC counts into the qc_flags stats output.

This companion statistic belongs to the ``stats_release/qc_flags`` output
family. It reads the final Q/SSC/SSL flag arrays from the three published matrix
products and writes one normalized CSV that downstream manuscript reporting can
consume without reopening the NetCDF files.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import add_common_args, context_from_args, write_csv
from stats_release.release_paths import MATRIX_PRODUCTS


VARIABLES = ("Q", "SSC", "SSL")
FINAL_FLAGS = (0, 1, 2, 3, 9)
FLAG_MEANINGS = {0: "good", 1: "derived", 2: "suspect", 3: "bad", 9: "missing"}


def _percent(count: int, total: int) -> float:
    return 100.0 * int(count) / int(total) if total else 0.0


def _count_matrix(ctx, file_name: str, resolution: str, chunk_size: int) -> pd.DataFrame:
    """Count final flag values for one station-by-time matrix product."""
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame()

    rows = []
    with ctx.open_dataset(file_name, required=True) as ds:
        for variable in VARIABLES:
            flag_name = "{}_flag".format(variable)
            if flag_name not in ds.variables:
                continue

            var = ds.variables[flag_name]
            shape = tuple(int(v) for v in var.shape)
            if not shape:
                continue

            # Chunk along the leading dimension while keeping approximately
            # chunk_size scalar flag values per read. This works for both
            # 1-D record arrays and 2-D station-by-time matrices.
            trailing = int(np.prod(shape[1:], dtype=np.int64)) if len(shape) > 1 else 1
            leading_chunk = max(1, int(chunk_size) // max(1, trailing))
            counts = {}

            for start in range(0, shape[0], leading_chunk):
                stop = min(start + leading_chunk, shape[0])
                arr = np.ma.asarray(var[start:stop]).filled(9).reshape(-1)
                numeric = pd.to_numeric(pd.Series(arr), errors="coerce").dropna().astype(int).to_numpy()
                if numeric.size == 0:
                    continue
                values, value_counts = np.unique(numeric, return_counts=True)
                for flag, count in zip(values, value_counts):
                    counts[int(flag)] = counts.get(int(flag), 0) + int(count)

            total = int(sum(counts.values()))
            all_flags = sorted(set(FINAL_FLAGS).union(counts))
            for flag in all_flags:
                count = int(counts.get(flag, 0))
                rows.append(
                    {
                        "resolution": resolution,
                        "flag_variable": flag_name,
                        "flag_value": int(flag),
                        "flag_meaning": FLAG_MEANINGS.get(int(flag), "other"),
                        "count": count,
                        "percent": round(_percent(count, total), 6),
                        "n_total": total,
                    }
                )

    return pd.DataFrame(rows)


def build_matrix_final_flag_stats(ctx, chunk_size: int) -> pd.DataFrame:
    """Count final Q/SSC/SSL flags for daily, monthly, and annual matrices."""
    pieces = []
    for resolution, file_name in MATRIX_PRODUCTS.items():
        frame = _count_matrix(ctx, file_name, resolution, chunk_size)
        if not frame.empty:
            pieces.append(frame)

    if not pieces:
        return pd.DataFrame(
            columns=[
                "resolution",
                "flag_variable",
                "flag_value",
                "flag_meaning",
                "count",
                "percent",
                "n_total",
            ]
        )
    return pd.concat(pieces, ignore_index=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Build matrix final QC-flag statistics for the stats_release/qc_flags output family."
    )
    # Deliberately use the qc_flags output namespace so this CSV is a formal
    # upstream artifact of manuscript Table 7.
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
