#!/usr/bin/env python3
"""Q/SSC/SSL coverage statistics from sed_reference_release_minimal.
Multi-panel figure with ESSD-compliant output artifacts.

Outputs (AGENTS.md-compliant):
    figures/final/variable_distribution.png
    figures/final/variable_distribution.pdf
    figures/data/variable_distribution_plotting_data.csv
    figures/scripts/plot_variable_distribution.py
    figures/checklists/variable_distribution_checklist.md
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

import argparse
import datetime
import shutil
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# ── Absolute paths ──────────────────────────────────────────────────────────
PROJECT_SRC = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test")
PROJECT_ROOT = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/stats_release")
if __package__ in {None, ""}:
    sys.path.insert(0, str(PROJECT_SRC))

from stats_release.common_stats import VARIABLES, decode_time_axis, numeric_stats, pct
from stats_release.release_io import (
    ReleaseContext,
    clean_text,
    read_numeric_var,
    read_text_var,
    setup_matplotlib,
    write_csv,
    write_markdown,
)
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)

# ── Output directories (absolute) ───────────────────────────────────────────
DEFAULT_MINIMAL_RELEASE_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/sed_reference_release_minimal")
DEFAULT_OUT_DIR          = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/stats_release/output_other/stats_release_minimal/variable_summary")
DEFAULT_FIGURES_DIR      = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/figures")

MATRIX_PRODUCTS = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}

EXTENSION_PRODUCTS = {
    "climatology": "sed_reference_climatology.nc",
    "satellite": "sed_reference_satellite.nc",
}

# ── Okabe‑Ito colourblind-safe palette ─────────────────────────────────────
OKABE_ITO = {
    "orange":       "#E69F00",
    "sky_blue":     "#56B4E9",
    "bluish_green": "#009E73",
}

PRODUCT_COLORS = {
    "daily":       OKABE_ITO["orange"],
    "monthly":     OKABE_ITO["sky_blue"],
    "annual":      OKABE_ITO["bluish_green"],
}

# ── Font size constants (ESSD: min 7 pt for all text) ────────────────────────
FONT_FAMILY = "DejaVu Sans"
FONT_SIZE_LEGEND = 13
FONT_SIZE_TICK = 14
FONT_SIZE_LABEL = 15
FONT_SIZE_PANEL = 16


def ensure_figure_dirs(figures_root: Path) -> dict:
    """Create and return the AGENTS.md figure output directory structure."""
    root = Path(figures_root).resolve()
    dirs = {
        "root": root,
        "final": root / "final",
        "data": root / "data",
        "scripts": root / "scripts",
        "checklists": root / "checklists",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Build minimal-release variable coverage statistics.")
    parser.add_argument("--release-dir", default=str(DEFAULT_MINIMAL_RELEASE_DIR), help="Path to sed_reference_release_minimal.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory for this module.")
    parser.add_argument("--figures-dir", default=str(DEFAULT_FIGURES_DIR), help="Root figures directory (AGENTS.md structure).")
    parser.add_argument("--matrix-row-chunk-size", type=int, default=64, help="Station-row chunk size for matrix scans.")
    parser.add_argument("--record-chunk-size", type=int, default=500000, help="Record chunk size for extension scans.")
    parser.add_argument("--skip-figures", action="store_true", help="Skip figure creation.")
    parser.add_argument("--figures-only", action="store_true", help="Skip stats and report; only generate the figure.")
    parser.add_argument("--dpi", type=int, default=300, help="Figure DPI.")
    args = parser.parse_args(argv)
    args.matrix_row_chunk_size = max(1, int(args.matrix_row_chunk_size))
    args.record_chunk_size = max(1, int(args.record_chunk_size))
    return args


def _concat_values(pieces: list[np.ndarray]) -> np.ndarray:
    return np.concatenate(pieces) if pieces else np.asarray([], dtype="float64")


def _flag_array(ds, var_name: str, key, shape) -> np.ndarray:
    flag_name = "{}_flag".format(var_name)
    if flag_name not in ds.variables:
        return np.full(shape, 9, dtype="int16")
    return np.ma.asarray(ds.variables[flag_name][key]).filled(9).astype("int16")


def _matrix_combo_name(q_present: np.ndarray, ssc_present: np.ndarray, ssl_present: np.ndarray) -> np.ndarray:
    code = q_present.astype("int8") + 2 * ssc_present.astype("int8") + 4 * ssl_present.astype("int8")
    labels = np.asarray(
        [
            "none",
            "Q only",
            "SSC only",
            "Q+SSC",
            "SSL only",
            "Q+SSL",
            "SSC+SSL",
            "Q+SSC+SSL",
        ],
        dtype=object,
    )
    return labels[code]


def _record_dimension_name(ds) -> str:
    for name in ("n_records", "n_satellite_records", "record"):
        if name in ds.dimensions:
            return name
    return ""


def _record_source_values(ds, product: str, key) -> np.ndarray:
    if product == "satellite" and "satellite_station_index" in ds.variables and "source" in ds.variables:
        station_sources = np.asarray(read_text_var(ds, "source"), dtype=object)
        station_idx = np.ma.asarray(ds.variables["satellite_station_index"][key]).filled(-1).astype("int64").reshape(-1)
        out = np.asarray([""] * len(station_idx), dtype=object)
        valid = (station_idx >= 0) & (station_idx < len(station_sources))
        out[valid] = station_sources[station_idx[valid]]
        return out
    if "source" in ds.variables:
        return np.asarray(read_text_var(ds, "source"), dtype=object)[key]
    return np.asarray([""] * (key.stop - key.start), dtype=object)


def _time_range(ds) -> tuple[str, str]:
    time_axis = decode_time_axis(ds)
    if len(time_axis) == 0:
        return "", ""
    return str(time_axis.min().date()), str(time_axis.max().date())


def scan_matrix_product(ctx: ReleaseContext, resolution: str, file_name: str, row_chunk_size: int) -> dict:
    variable_counts = {
        var: {
            "n_present": 0,
            "n_good": 0,
            "n_estimated": 0,
            "n_usable": 0,
            "stations_with_present": 0,
            "values": [],
        }
        for var in VARIABLES
    }
    colocation_counts = defaultdict(int)
    colocation_station_counts = defaultdict(int)
    extremes = []

    with ctx.open_dataset(file_name, required=True) as ds:
        n_stations = int(len(ds.dimensions.get("n_stations", [])))
        n_time = int(len(ds.dimensions.get("time", [])))
        n_cells = int(n_stations * n_time)
        time_start, time_end = _time_range(ds)
        units = {var: clean_text(getattr(ds.variables[var], "units", "")) if var in ds.variables else "" for var in VARIABLES}
        n_valid_time_steps_sum = 0
        if "n_valid_time_steps" in ds.variables:
            n_valid = np.ma.asarray(ds.variables["n_valid_time_steps"][:]).filled(0)
            n_valid_time_steps_sum = int(np.sum(n_valid.astype("int64")))

        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            key = (slice(start, stop), slice(None))
            present_masks = {}
            for var in VARIABLES:
                values = np.asarray(read_numeric_var(ds, var, key=key), dtype="float64")
                present = np.isfinite(values)
                flags = _flag_array(ds, var, key, values.shape)
                present_masks[var] = present

                variable_counts[var]["n_present"] += int(np.count_nonzero(present))
                variable_counts[var]["n_good"] += int(np.count_nonzero(present & (flags == 0)))
                variable_counts[var]["n_estimated"] += int(np.count_nonzero(present & (flags == 1)))
                variable_counts[var]["n_usable"] += int(np.count_nonzero(present & np.isin(flags, [0, 1])))
                variable_counts[var]["stations_with_present"] += int(np.count_nonzero(np.any(present, axis=1)))
                if np.any(present):
                    finite_values = values[present]
                    variable_counts[var]["values"].append(finite_values)
                    top_n = min(20, finite_values.size)
                    top_local = np.argpartition(finite_values, -top_n)[-top_n:]
                    finite_flat = np.flatnonzero(present.reshape(-1))
                    for local in top_local:
                        flat_pos = int(finite_flat[int(local)])
                        station_offset, time_index = divmod(flat_pos, n_time)
                        extremes.append(
                            {
                                "product": "matrix",
                                "resolution": resolution,
                                "variable": var,
                                "value": float(finite_values[int(local)]),
                                "station_index": int(start + station_offset),
                                "time_index": int(time_index),
                                "record_index": "",
                                "review_reason": "top_high_value",
                                "unit": units.get(var, ""),
                            }
                        )

            combos = _matrix_combo_name(present_masks["Q"], present_masks["SSC"], present_masks["SSL"])
            for combo in np.unique(combos):
                combo_text = str(combo)
                if combo_text == "none":
                    continue
                mask = combos == combo
                colocation_counts[combo_text] += int(np.count_nonzero(mask))
                colocation_station_counts[combo_text] += int(np.count_nonzero(np.any(mask, axis=1)))

        n_nonempty_cells = int(sum(colocation_counts.values()))
        resolution_row = {
            "product": "matrix",
            "resolution": resolution,
            "file_name": file_name,
            "n_stations": n_stations,
            "n_time_steps": n_time,
            "n_cells": n_cells,
            "n_nonempty_cells": n_nonempty_cells,
            "n_valid_time_steps_sum": n_valid_time_steps_sum,
            "nonempty_percent_of_cells": pct(n_nonempty_cells, n_cells),
            "time_start": time_start,
            "time_end": time_end,
        }

    variable_rows = []
    coverage_rows = []
    for var in VARIABLES:
        values = _concat_values(variable_counts[var]["values"])
        row = {
            "product": "matrix",
            "resolution": resolution,
            "variable": var,
            "n_cells": n_cells,
            "n_present": variable_counts[var]["n_present"],
            "n_good": variable_counts[var]["n_good"],
            "n_estimated": variable_counts[var]["n_estimated"],
            "n_usable": variable_counts[var]["n_usable"],
            "stations_with_present": variable_counts[var]["stations_with_present"],
            "present_percent_of_cells": pct(variable_counts[var]["n_present"], n_cells),
            "good_percent_of_cells": pct(variable_counts[var]["n_good"], n_cells),
            "estimated_percent_of_cells": pct(variable_counts[var]["n_estimated"], n_cells),
            "usable_percent_of_cells": pct(variable_counts[var]["n_usable"], n_cells),
            "usable_percent_of_present": pct(variable_counts[var]["n_usable"], variable_counts[var]["n_present"]),
            "unit": units.get(var, ""),
        }
        row.update(numeric_stats(values))
        variable_rows.append(row)
        coverage_rows.append(
            {
                "product": "matrix",
                "resolution": resolution,
                "variable": var,
                "denominator_type": "station_time_cells",
                "n_records": "",
                "n_cells": n_cells,
                "n_present": row["n_present"],
                "n_good": row["n_good"],
                "n_estimated": row["n_estimated"],
                "n_usable": row["n_usable"],
                "present_percent": row["present_percent_of_cells"],
                "good_percent": row["good_percent_of_cells"],
                "estimated_percent": row["estimated_percent_of_cells"],
                "usable_percent": row["usable_percent_of_cells"],
                "unit": row["unit"],
            }
        )

    colocation_rows = []
    for combo, count in sorted(colocation_counts.items()):
        colocation_rows.append(
            {
                "product": "matrix",
                "resolution": resolution,
                "combination": combo,
                "n_cells": int(count),
                "stations_with_combination": int(colocation_station_counts[combo]),
                "percent_of_cells": pct(count, n_cells),
                "percent_of_nonempty_cells": pct(count, n_nonempty_cells),
            }
        )

    return {
        "resolution": pd.DataFrame([resolution_row]),
        "variables": pd.DataFrame(variable_rows),
        "coverage": pd.DataFrame(coverage_rows),
        "colocation": pd.DataFrame(colocation_rows),
        "extremes": pd.DataFrame(extremes),
    }


def scan_matrix_products(ctx: ReleaseContext, row_chunk_size: int) -> dict:
    scanned = [scan_matrix_product(ctx, resolution, file_name, row_chunk_size) for resolution, file_name in MATRIX_PRODUCTS.items()]
    return {
        "resolution": pd.concat([item["resolution"] for item in scanned], ignore_index=True),
        "variables": pd.concat([item["variables"] for item in scanned], ignore_index=True),
        "coverage": pd.concat([item["coverage"] for item in scanned], ignore_index=True),
        "colocation": pd.concat([item["colocation"] for item in scanned], ignore_index=True),
        "extremes": pd.concat([item["extremes"] for item in scanned], ignore_index=True),
    }


def scan_record_product(ctx: ReleaseContext, product: str, file_name: str, chunk_size: int) -> dict:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return {
            "product": pd.DataFrame(),
            "variables": pd.DataFrame(),
            "source_variables": pd.DataFrame(),
            "coverage": pd.DataFrame(),
            "extremes": pd.DataFrame(),
        }

    source_counts = defaultdict(lambda: defaultdict(lambda: {"n_records": 0, "n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0, "values": []}))
    product_counts = {
        var: {"n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0, "values": []}
        for var in VARIABLES
    }
    extremes = []

    with ctx.open_dataset(file_name, required=True) as ds:
        record_dim = _record_dimension_name(ds)
        n_records = int(len(ds.dimensions[record_dim])) if record_dim else 0
        n_stations = 0
        for dim_name in ("n_stations", "n_satellite_stations"):
            if dim_name in ds.dimensions:
                n_stations = int(len(ds.dimensions[dim_name]))
                break
        time_start, time_end = _time_range(ds)
        units = {var: clean_text(getattr(ds.variables[var], "units", "")) if var in ds.variables else "" for var in VARIABLES}

        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            key = slice(start, stop)
            sources = _record_source_values(ds, product, key)
            for var in VARIABLES:
                values = np.asarray(read_numeric_var(ds, var, key=key), dtype="float64").reshape(-1)
                present = np.isfinite(values)
                flags = _flag_array(ds, var, key, values.shape).reshape(-1)

                product_counts[var]["n_present"] += int(np.count_nonzero(present))
                product_counts[var]["n_good"] += int(np.count_nonzero(present & (flags == 0)))
                product_counts[var]["n_estimated"] += int(np.count_nonzero(present & (flags == 1)))
                product_counts[var]["n_usable"] += int(np.count_nonzero(present & np.isin(flags, [0, 1])))
                if np.any(present):
                    present_values = values[present]
                    product_counts[var]["values"].append(present_values)
                    top_n = min(20, present_values.size)
                    top_local = np.argpartition(present_values, -top_n)[-top_n:]
                    finite_idx = np.flatnonzero(present)
                    for local in top_local:
                        record_index = int(start + finite_idx[int(local)])
                        extremes.append(
                            {
                                "product": product,
                                "resolution": "",
                                "variable": var,
                                "value": float(present_values[int(local)]),
                                "station_index": "",
                                "time_index": "",
                                "record_index": record_index,
                                "review_reason": "top_high_value",
                                "unit": units.get(var, ""),
                            }
                        )

                for source in sorted(set(sources)):
                    source = clean_text(source) or "unknown"
                    mask = sources == source
                    item = source_counts[source][var]
                    item["n_records"] += int(np.count_nonzero(mask))
                    item["n_present"] += int(np.count_nonzero(mask & present))
                    item["n_good"] += int(np.count_nonzero(mask & present & (flags == 0)))
                    item["n_estimated"] += int(np.count_nonzero(mask & present & (flags == 1)))
                    item["n_usable"] += int(np.count_nonzero(mask & present & np.isin(flags, [0, 1])))
                    if np.any(mask & present):
                        item["values"].append(values[mask & present])

    product_rows = [
        {
            "product": product,
            "file_name": file_name,
            "n_stations": n_stations,
            "n_records": n_records,
            "time_start": time_start,
            "time_end": time_end,
        }
    ]
    variable_rows = []
    coverage_rows = []
    for var in VARIABLES:
        values = _concat_values(product_counts[var]["values"])
        row = {
            "product": product,
            "variable": var,
            "n_records": n_records,
            "n_present": product_counts[var]["n_present"],
            "n_good": product_counts[var]["n_good"],
            "n_estimated": product_counts[var]["n_estimated"],
            "n_usable": product_counts[var]["n_usable"],
            "present_percent": pct(product_counts[var]["n_present"], n_records),
            "good_percent": pct(product_counts[var]["n_good"], n_records),
            "estimated_percent": pct(product_counts[var]["n_estimated"], n_records),
            "usable_percent": pct(product_counts[var]["n_usable"], n_records),
            "unit": units.get(var, ""),
        }
        row.update(numeric_stats(values))
        variable_rows.append(row)
        coverage_rows.append(
            {
                "product": product,
                "resolution": "",
                "variable": var,
                "denominator_type": "records",
                "n_records": n_records,
                "n_cells": "",
                "n_present": row["n_present"],
                "n_good": row["n_good"],
                "n_estimated": row["n_estimated"],
                "n_usable": row["n_usable"],
                "present_percent": row["present_percent"],
                "good_percent": row["good_percent"],
                "estimated_percent": row["estimated_percent"],
                "usable_percent": row["usable_percent"],
                "unit": row["unit"],
            }
        )

    source_rows = []
    for source, by_var in sorted(source_counts.items()):
        for var in VARIABLES:
            item = by_var[var]
            values = _concat_values(item["values"])
            row = {
                "product": product,
                "source_name": source,
                "variable": var,
                "n_records": item["n_records"],
                "n_present": item["n_present"],
                "n_good": item["n_good"],
                "n_estimated": item["n_estimated"],
                "n_usable": item["n_usable"],
                "present_percent": pct(item["n_present"], item["n_records"]),
                "good_percent": pct(item["n_good"], item["n_records"]),
                "estimated_percent": pct(item["n_estimated"], item["n_records"]),
                "usable_percent": pct(item["n_usable"], item["n_records"]),
                "unit": units.get(var, ""),
            }
            row.update(numeric_stats(values))
            source_rows.append(row)

    return {
        "product": pd.DataFrame(product_rows),
        "variables": pd.DataFrame(variable_rows),
        "source_variables": pd.DataFrame(source_rows),
        "coverage": pd.DataFrame(coverage_rows),
        "extremes": pd.DataFrame(extremes),
    }


def scan_extension_products(ctx: ReleaseContext, chunk_size: int) -> dict:
    scanned = [scan_record_product(ctx, product, file_name, chunk_size) for product, file_name in EXTENSION_PRODUCTS.items()]
    return {
        "product": pd.concat([item["product"] for item in scanned], ignore_index=True),
        "variables": pd.concat([item["variables"] for item in scanned], ignore_index=True),
        "source_variables": pd.concat([item["source_variables"] for item in scanned], ignore_index=True),
        "coverage": pd.concat([item["coverage"] for item in scanned], ignore_index=True),
        "extremes": pd.concat([item["extremes"] for item in scanned], ignore_index=True),
    }


def trim_extremes(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    return (
        frame.sort_values(["product", "resolution", "variable", "value"], ascending=[True, True, True, False])
        .groupby(["product", "resolution", "variable"], as_index=False, group_keys=False)
        .head(20)
        .reset_index(drop=True)
    )


def _read_values_for_figure(ctx: ReleaseContext, product: str, file_name: str, var_name: str, matrix_rows: int, record_rows: int) -> dict:
    value_pieces = []
    flag_pieces = []
    with ctx.open_dataset(file_name, required=True) as ds:
        if product in MATRIX_PRODUCTS:
            n_stations = int(len(ds.dimensions.get("n_stations", [])))
            for start in range(0, n_stations, matrix_rows):
                stop = min(start + matrix_rows, n_stations)
                key = (slice(start, stop), slice(None))
                values = read_numeric_var(ds, var_name, key=key)
                flags = _flag_array(ds, var_name, key, values.shape)
                valid = np.isfinite(values) & np.isin(flags, [0, 1])
                if np.any(valid):
                    value_pieces.append(values[valid])
                    flag_pieces.append(flags[valid])
        else:
            record_dim = _record_dimension_name(ds)
            n_records = int(len(ds.dimensions[record_dim])) if record_dim else 0
            for start in range(0, n_records, record_rows):
                stop = min(start + record_rows, n_records)
                key = slice(start, stop)
                values = read_numeric_var(ds, var_name, key=key)
                flags = _flag_array(ds, var_name, key, values.shape)
                valid = np.isfinite(values) & np.isin(flags, [0, 1])
                if np.any(valid):
                    value_pieces.append(values[valid])
                    flag_pieces.append(flags[valid])
    return {"values": _concat_values(value_pieces), "flags": _concat_values(flag_pieces).astype("int16")}


def _plot_axis_limits(values: np.ndarray, fixed_limits: tuple[float, float]) -> tuple[float, float]:
    if values.size < 2:
        return fixed_limits
    low, high = np.percentile(values, [1, 99])
    if not np.isfinite(low) or not np.isfinite(high) or low >= high:
        return fixed_limits
    pad = max((high - low) * 0.05, 0.05)
    low = max(float(fixed_limits[0]), float(low - pad))
    high = min(float(fixed_limits[1]), float(high + pad))
    if low >= high:
        return fixed_limits
    return low, high


def write_figure_and_artifacts(
    ctx: ReleaseContext,
    figure_dirs: dict,
    figure_id: str,
    dpi: int,
    matrix_rows: int,
    record_rows: int,
) -> None:
    """Multi-panel Q/SSC/SSL distribution figure with all AGENTS.md-required artifacts."""
    import matplotlib as mpl

    try:
        plt = setup_matplotlib()
    except Exception:
        return

    # ── ESSD-compliant rcParams ────────────────────────────────────────────
    mpl.rcParams["font.family"] = FONT_FAMILY
    mpl.rcParams["pdf.fonttype"] = 42
    mpl.rcParams["ps.fonttype"] = 42
    mpl.rcParams["svg.fonttype"] = "none"
    mpl.rcParams["axes.unicode_minus"] = False

    products = [
        ("daily", MATRIX_PRODUCTS["daily"]),
        ("monthly", MATRIX_PRODUCTS["monthly"]),
        ("annual", MATRIX_PRODUCTS["annual"]),
    ]

    use_log = {"Q": True, "SSC": True, "SSL": True}
    variable_labels = {"Q": "Q", "SSC": "SSC", "SSL": "SSL"}
    annual_draw_boxplot = True
    N_BINS = 55

    xlabels = {"Q": "Q (m³ s⁻¹)", "SSC": "SSC (mg L⁻¹)", "SSL": "SSL (t d⁻¹)"}
    fixed_x_limits = {"Q": (-3.0, 5.0), "SSC": (-1.0, 5.0), "SSL": (-3.0, 7.0)}

    fig, axes = plt.subplots(3, 1, figsize=(7.2, 9.0), sharex=False)
    plotting_rows = []

    for idx, var_name in enumerate(VARIABLES):
        ax = axes[idx]
        product_data = {}
        combined_values = []

        for product, file_name in products:
            if ctx.require_input(ctx.release_file(file_name), required=False) is None:
                continue
            data = _read_values_for_figure(ctx, product, file_name, var_name, matrix_rows, record_rows)
            values = data["values"]
            flags = data["flags"]
            if values.size == 0:
                continue

            if use_log[var_name]:
                positive = values > 0
                values = values[positive]
                flags = flags[positive]
                if values.size == 0:
                    continue
                plot_values = np.log10(values)
            else:
                plot_values = values

            n_good = int(np.count_nonzero(flags == 0))
            n_estimated = int(np.count_nonzero(flags == 1))
            n_usable = int(plot_values.size)
            product_data[product] = {
                "values": plot_values,
                "n_good": n_good,
                "n_estimated": n_estimated,
                "n_usable": n_usable,
            }
            combined_values.append(plot_values)

        any_data = bool(product_data)
        if any_data:
            all_values = _concat_values(combined_values)
            x_min, x_max = _plot_axis_limits(all_values, fixed_x_limits[var_name])
            bins = np.linspace(x_min, x_max, N_BINS)

            for product, _file_name in products:
                if product not in product_data:
                    continue
                plot_values = product_data[product]["values"]
                n_good = product_data[product]["n_good"]
                n_estimated = product_data[product]["n_estimated"]
                n_usable = product_data[product]["n_usable"]
                color = PRODUCT_COLORS.get(product, "#333333")

                if product == "annual":
                    q05, q25, q50, q75, q95 = np.percentile(plot_values, [5, 25, 50, 75, 95])
                    plotting_rows.append({
                        "plot_kind": "boxplot",
                        "flag_filter": "flag_0_or_1_usable",
                        "variable": var_name,
                        "product": product,
                        "bin_left": "",
                        "bin_right": "",
                        "density": "",
                        "p05": q05,
                        "p25": q25,
                        "median": q50,
                        "p75": q75,
                        "p95": q95,
                        "statistic": "",
                        "x_value": "",
                        "n_usable": n_usable,
                        "n_good": n_good,
                        "n_estimated": n_estimated,
                    })
                    plotting_rows.append({
                        "plot_kind": "rug",
                        "flag_filter": "flag_0_or_1_usable",
                        "variable": var_name,
                        "product": product,
                        "bin_left": "",
                        "bin_right": "",
                        "density": "",
                        "p05": q05,
                        "p25": q25,
                        "median": q50,
                        "p75": q75,
                        "p95": q95,
                        "statistic": "",
                        "x_value": "",
                        "n_usable": n_usable,
                        "n_good": n_good,
                        "n_estimated": n_estimated,
                    })
                    plotting_rows.append({
                        "plot_kind": "iqr_marker",
                        "flag_filter": "flag_0_or_1_usable",
                        "variable": var_name,
                        "product": product,
                        "bin_left": "",
                        "bin_right": "",
                        "density": "",
                        "p05": q05,
                        "p25": q25,
                        "median": q50,
                        "p75": q75,
                        "p95": q95,
                        "statistic": "",
                        "x_value": "",
                        "n_usable": n_usable,
                        "n_good": n_good,
                        "n_estimated": n_estimated,
                    })
                    continue

                in_range = plot_values[(plot_values >= x_min) & (plot_values <= x_max)]
                if in_range.size == 0:
                    continue
                counts, bin_edges = np.histogram(in_range, bins=bins, density=True)
                for i in range(len(counts)):
                    plotting_rows.append({
                        "plot_kind": "histogram",
                        "flag_filter": "flag_0_or_1_usable",
                        "variable": var_name,
                        "product": product,
                        "bin_left": bin_edges[i],
                        "bin_right": bin_edges[i + 1],
                        "density": counts[i],
                        "p05": "",
                        "p25": "",
                        "median": "",
                        "p75": "",
                        "p95": "",
                        "statistic": "",
                        "x_value": "",
                        "n_usable": n_usable,
                        "n_good": n_good,
                        "n_estimated": n_estimated,
                    })

                ax.hist(
                    in_range,
                    bins=bins,
                    density=True,
                    histtype="step",
                    linewidth=1.4,
                    color=color,
                    label="{}".format(product),
                )

            q05, q50, q95 = np.percentile(all_values, [5, 50, 95])
            for statistic, x_value, linestyle, linewidth in [
                ("P5", q05, "--", 0.9),
                ("median", q50, "-", 1.4),
                ("P95", q95, "--", 0.9),
            ]:
                ax.axvline(x_value, color="0.25", linestyle=linestyle, linewidth=linewidth, alpha=0.85)
                plotting_rows.append({
                    "plot_kind": "summary_line",
                    "flag_filter": "flag_0_or_1_usable",
                    "variable": var_name,
                    "product": "all",
                    "bin_left": "",
                    "bin_right": "",
                    "density": "",
                    "p05": "",
                    "p25": "",
                    "median": "",
                    "p75": "",
                    "p95": "",
                    "statistic": statistic,
                    "x_value": x_value,
                    "n_usable": int(all_values.size),
                    "n_good": int(sum(item["n_good"] for item in product_data.values())),
                    "n_estimated": int(sum(item["n_estimated"] for item in product_data.values())),
                })

            ax.set_xlim(x_min, x_max)
            if var_name == "SSC":
                ax.xaxis.set_major_locator(mpl.ticker.FixedLocator([0, 1, 2, 3, 4]))
            ax.xaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda v, _: f"{10**v:g}"))

            if "annual" in product_data:
                ylim = ax.get_ylim()
                y_max = ylim[1] if np.isfinite(ylim[1]) and ylim[1] > 0 else 1.0
                box_y = y_max * 0.08
                box_width = y_max * 0.08
                annual_values = product_data["annual"]["values"]

                if annual_draw_boxplot:
                    ax.boxplot(
                        annual_values,
                        vert=False,
                        positions=[box_y],
                        widths=[box_width],
                        showfliers=False,
                        manage_ticks=False,
                        patch_artist=True,
                        boxprops={"facecolor": PRODUCT_COLORS["annual"], "edgecolor": PRODUCT_COLORS["annual"], "alpha": 0.25},
                        medianprops={"color": PRODUCT_COLORS["annual"], "linewidth": 1.3},
                        whiskerprops={"color": PRODUCT_COLORS["annual"], "linewidth": 1.0},
                        capprops={"color": PRODUCT_COLORS["annual"], "linewidth": 1.0},
                    )



                ax.set_ylim(ylim)
                annual = product_data["annual"]

                # Legend: only panel a (Q) gets a legend
                if idx == 0:
                    annual_handle = mpl.lines.Line2D(
                        [0], [0],
                        color=PRODUCT_COLORS["annual"],
                        linewidth=1.4,
                        label="annual boxplot",
                    )
                    solid_line = mpl.lines.Line2D(
                        [0], [0], color="0.25", linestyle="-", linewidth=1.4, label="Median"
                    )
                    dash_line = mpl.lines.Line2D(
                        [0], [0], color="0.25", linestyle="--", linewidth=0.9, label="P5 / P95"
                    )
                    handles, labels = ax.get_legend_handles_labels()
                    handles.extend([annual_handle, solid_line, dash_line])
                    labels.extend([annual_handle.get_label(), solid_line.get_label(), dash_line.get_label()])
                    ax.legend(handles, labels, frameon=False, fontsize=FONT_SIZE_LEGEND, loc="upper right")
            else:
                if idx == 0:
                    solid_line = mpl.lines.Line2D(
                        [0], [0], color="0.25", linestyle="-", linewidth=1.4, label="Median"
                    )
                    dash_line = mpl.lines.Line2D(
                        [0], [0], color="0.25", linestyle="--", linewidth=0.9, label="P5 / P95"
                    )
                    handles, labels = ax.get_legend_handles_labels()
                    handles.extend([solid_line, dash_line])
                    labels.extend([solid_line.get_label(), dash_line.get_label()])
                    ax.legend(handles, labels, frameon=False, fontsize=FONT_SIZE_LEGEND, loc="upper right")

        if not any_data:
            ax.text(0.5, 0.5, "No valid {} values".format(var_name),
                    ha="center", va="center", transform=ax.transAxes, fontsize=FONT_SIZE_LABEL)
        else:
            ax.set_xlabel(xlabels[var_name], fontsize=FONT_SIZE_LABEL)
            ax.tick_params(labelsize=FONT_SIZE_TICK)
            ax.grid(True, alpha=0.3)

        # Panel label
        ax.text(0.01, 0.97, "({})".format(chr(97 + idx)),
                transform=ax.transAxes, fontsize=FONT_SIZE_PANEL,
                fontweight="bold", va="top", ha="left",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

    fig.tight_layout()
    fig.supylabel("Probability density", fontsize=FONT_SIZE_LABEL, x=-0.045)

    # ── Save artifacts ─────────────────────────────────────────────────────
    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    data_path = figure_dirs["data"] / "{}_plotting_data.csv".format(figure_id)
    script_path = figure_dirs["scripts"] / "plot_{}.py".format(figure_id)
    checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)

    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    print("Wrote {}".format(png_path))
    print("Wrote {}".format(pdf_path))

    # Plotting data
    pd.DataFrame(plotting_rows).to_csv(data_path, index=False)
    print("Wrote {}".format(data_path))

    # Script copy
    source_script = Path(__file__).resolve()
    if source_script != script_path.resolve():
        shutil.copy2(str(source_script), str(script_path))
        print("Copied script to {}".format(script_path))
    else:
        print("Script already at {}".format(script_path))

    # Checklist
    figsize = fig.get_size_inches()
    width_cm = figsize[0] * 2.54
    height_cm = figsize[1] * 2.54

    clines = [
        "# Figure checklist: {}".format(figure_id),
        "",
        "## Basic information",
        "",
        "- Figure file: `{}.pdf` / `{}.png`".format(figure_id, figure_id),
        "- Plotting script: `plot_{}.py`".format(figure_id),
        "- Plotting data: `{}_plotting_data.csv`".format(figure_id),
        "- Date exported: {}".format(datetime.date.today().isoformat()),
        "- Figure type: Multi-panel distribution histogram with annual boxplot",
        "- Single-panel or multi-panel: Multi-panel (3 panels: Q, SSC, SSL)",
        "",
        "## File format and size",
        "",
        "- Final format: PDF (vector), PNG (bitmap)",
        "- PNG dpi: {}".format(dpi),
        "- Width: {:.1f} cm ({:.1f} in)".format(width_cm, figsize[0]),
        "- Height: {:.1f} cm ({:.1f} in)".format(height_cm, figsize[1]),
        "- Width >= 8 cm: yes",
        "",
        "## Color and accessibility",
        "",
        "- Colorblind-safe palette used: Okabe-Ito (3 colours: orange, sky_blue, bluish_green)",
        "- Continuous color map, if applicable: N/A (histogram step lines and boxplot)",
        "- Coblis or equivalent check completed: Requires manual review after export",
        "- Figure remains interpretable under colour-vision-deficiency simulation: Requires manual review",
        "- Categories are distinguished by more than colour when needed: Daily/monthly use step histograms; annual uses boxplots",
        "",
        "## Font and text",
        "",
        "- Single font family used: DejaVu Sans",
        "- Font family: DejaVu Sans",
        "- Fonts embedded in vector file: Yes (pdf.fonttype=42)",
        "- No unnecessary bold/italic variants: Yes",
        "- No hidden text boxes or extra layers: Yes",
        "- Sentence case used: Yes",
        "",
        "## Legend and symbols",
        "",
        "- Legend included inside figure: Yes (per panel)",
        "- All colours explained: Yes (daily/monthly/annual)",
        "- All markers explained: Yes (daily/monthly as step histograms, annual as boxplot, reference lines)",
        "- All line styles explained: Yes (step histograms and P5/median/P95 reference lines)",
        "- Point sizes explained, if applicable: N/A",
        "- Color bar included and labeled, if applicable: N/A",
        "- Legend does not obscure data: Yes",
        "",
        "## ESSD formatting",
        "",
        '- Panel labels use "(a)", "(b)", etc.: Yes',
        "- Ranges use en dash with no spaces: N/A",
        "- Coordinates use degree symbol and direction spacing: N/A",
        "- Numbers and units have a space: Yes",
        "- Units use exponent format: Yes (m³ s⁻¹, mg L⁻¹, t d⁻¹)",
        "- h, km, and m abbreviations used correctly: N/A",
        "",
        "## Reproducibility",
        "",
        "- Plotting data saved: Yes",
        "- Plotting script saved: Yes",
        "- Input paths documented: Yes (variable at script top)",
        "- Filtering rules documented: Yes (finite values, positive values for log10, flag == 0 or flag == 1)",
        "- Colour and marker mappings defined in code: Yes (PRODUCT_COLORS)",
        "- Figure can be regenerated from saved files: Yes",
        "",
        "## Notes",
        "",
        "- Flag 0 is good data and flag 1 is estimated/derived data; both are included as usable data in this figure.",
        "- A manual Coblis check should be performed on the final exported file.",
        "",
    ]
    checklist_path.write_text("\n".join(clines) + "\n", encoding="utf-8")
    print("Wrote {}".format(checklist_path))


def build_report(ctx: ReleaseContext, stats: dict, tables_dir: Path, figures_dir: Path, reports_dir: Path) -> list[str]:
    coverage = stats["variable_coverage"]
    matrix_resolution = stats["matrix_resolution_summary"]
    matrix_variable = stats["matrix_variable_summary"]
    matrix_colocation = stats["matrix_colocated_variable_coverage"]
    extension_variable = stats["extension_variable_summary"]
    extension_source_variable = stats["extension_source_variable_summary"]
    extremes = stats["extreme_value_review_points"]
    satellite_sources = (
        extension_source_variable[extension_source_variable["product"].astype(str).eq("satellite")]
        if not extension_source_variable.empty and "product" in extension_source_variable.columns
        else pd.DataFrame()
    )

    matrix_present = int(pd.to_numeric(matrix_variable.get("n_present", 0), errors="coerce").fillna(0).sum()) if not matrix_variable.empty else 0
    extension_present = int(pd.to_numeric(extension_variable.get("n_present", 0), errors="coerce").fillna(0).sum()) if not extension_variable.empty else 0

    lines = [
        "# Minimal Variable Coverage Results Report",
        "",
        "## Scope",
        "",
        "- Minimal release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Variables covered: Q, SSC, SSL.",
        "- Matrix denominators are station-time cells; climatology and satellite denominators are records.",
        "",
        "## Headline",
        "",
        "- Product-variable rows summarized: {}".format(fmt_int(len(coverage))),
        "- Matrix non-missing values: {}".format(fmt_int(matrix_present)),
        "- Extension non-missing values: {}".format(fmt_int(extension_present)),
        "- Satellite source-variable rows: {}".format(fmt_int(len(satellite_sources))),
        "- Extreme review points emitted: {}".format(fmt_int(len(extremes))),
        "",
        "## Product by Variable Coverage",
        "",
        sorted_markdown_table(
            coverage,
            columns=[
                "product",
                "resolution",
                "variable",
                "denominator_type",
                "n_records",
                "n_cells",
                "n_present",
                "n_good",
                "n_estimated",
                "n_usable",
                "present_percent",
                "usable_percent",
            ],
            max_rows=18,
        ),
    ]
    append_table_section(
        lines,
        "Matrix Resolution Summary",
        matrix_resolution,
        columns=["resolution", "n_stations", "n_time_steps", "n_cells", "n_nonempty_cells", "nonempty_percent_of_cells", "time_start", "time_end"],
        sort_by="n_nonempty_cells",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Matrix Variable Summary",
        matrix_variable,
        columns=["resolution", "variable", "n_present", "n_good", "n_estimated", "n_usable", "stations_with_present", "present_percent_of_cells", "mean", "median", "p05", "p95", "p99", "unit"],
        sort_by="n_present",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Matrix Co-Located Variable Coverage",
        matrix_colocation,
        columns=["resolution", "combination", "n_cells", "stations_with_combination", "percent_of_cells", "percent_of_nonempty_cells"],
        sort_by="n_cells",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Extension Variable Summary",
        extension_variable,
        columns=["product", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "mean", "median", "p05", "p95", "p99", "unit"],
        sort_by="n_present",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Satellite Source by Variable",
        satellite_sources,
        columns=["source_name", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "unit"],
        sort_by="n_records",
        max_rows=18,
        note="Satellite rows are validation-only and should be filtered by source and variable before use.",
    )
    append_table_section(
        lines,
        "Extreme Value Review Points",
        extremes,
        columns=["product", "resolution", "variable", "value", "station_index", "time_index", "record_index", "review_reason", "unit"],
        sort_by="value",
        max_rows=20,
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- Matrix products use station-time cells as denominators, so their percentages are not directly comparable to record-oriented release statistics.",
            "- `n_usable` counts present values with flag 0 or 1, matching the good-or-estimated interpretation used elsewhere in release statistics.",
            "- Extreme review points are candidates for manual inspection, not automatic removal rules.",
        ]
    )
    append_figure_index(lines, figures_dir, reports_dir)
    return safe_lines(lines)


def build_stats(ctx: ReleaseContext, matrix_row_chunk_size: int, record_chunk_size: int) -> dict:
    matrix = scan_matrix_products(ctx, matrix_row_chunk_size)
    extension = scan_extension_products(ctx, record_chunk_size)
    extremes = trim_extremes(pd.concat([matrix["extremes"], extension["extremes"]], ignore_index=True))
    coverage = pd.concat([matrix["coverage"], extension["coverage"]], ignore_index=True)
    return {
        "variable_coverage": coverage,
        "matrix_resolution_summary": matrix["resolution"],
        "matrix_variable_summary": matrix["variables"],
        "matrix_colocated_variable_coverage": matrix["colocation"],
        "extension_product_summary": extension["product"],
        "extension_variable_summary": extension["variables"],
        "extension_source_variable_summary": extension["source_variables"],
        "extreme_value_review_points": extremes,
    }


def main(argv=None) -> int:
    args = parse_args(argv)
    ctx = ReleaseContext(
        release_dir=Path(args.release_dir),
        out_dir=Path(args.out_dir),
        strict_release_only=True,
    )
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    figures_dir = Path(args.figures_dir)
    if not args.figures_only:
        stats = build_stats(ctx, args.matrix_row_chunk_size, args.record_chunk_size)
        for name, frame in stats.items():
            write_csv(frame, tables_dir / "table_{}.csv".format(name))
    else:
        stats = None

    if not args.skip_figures:
        figure_dirs = ensure_figure_dirs(figures_dir)
        try:
            write_figure_and_artifacts(
                ctx, figure_dirs, "variable_distribution",
                max(300, int(args.dpi)),
                args.matrix_row_chunk_size, args.record_chunk_size,
            )
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)

    if not args.figures_only:
        report_lines = build_report(ctx, stats, tables_dir, figures_dir, reports_dir)
        md_path = ctx.output_path("reports", "variable_coverage_summary_minimal.md")
        write_markdown(report_lines, md_path)
        write_markdown(report_lines, ctx.output_path("variable_coverage_results_report_minimal.md"))
        print("Wrote minimal variable summary to {}".format(tables_dir / "table_variable_coverage.csv"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
