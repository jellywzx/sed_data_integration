#!/usr/bin/env python3
"""Build manuscript statistics for sed_reference_release_minimal.

This runner is intentionally independent from run_all_release_stats because the
minimal package is not a full release package: it has no master NetCDF and its
main products are station-time matrices.  The outputs are compact CSV tables
and one Markdown report suitable for manuscript number checks.
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.common_stats import (
    FLAG_MEANINGS,
    FLAG_VALUES,
    VARIABLES,
    decode_time_axis,
    pct,
)
from stats_release.release_io import (
    ReleaseContext,
    clean_text,
    file_manifest,
    metadata_fingerprint,
    read_numeric_var,
    read_text_var,
    script_fingerprint,
    write_csv,
    write_json,
    write_markdown,
)
from stats_release.reporting import (
    display_path,
    fmt_float,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MINIMAL_RELEASE_DIR = PROJECT_ROOT / "output" / "sed_reference_release_minimal"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output_other" / "stats_release_minimal"
DOCS_MINIMAL_REPORT_DIR = PROJECT_ROOT / "docs" / "reports" / "stats_release" / "minimal"

MATRIX_PRODUCTS = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}

CATALOG_FILES = {
    "station_catalog": "station_catalog.csv",
    "source_station_catalog": "source_station_catalog.csv",
    "source_dataset_catalog": "source_dataset_catalog.csv",
    "satellite_catalog": "satellite_catalog.csv",
}

EXTENSION_PRODUCTS = {
    "climatology": "sed_reference_climatology.nc",
    "satellite": "sed_reference_satellite.nc",
}

MANAGED_OUTPUTS = (
    "file_inventory.csv",
    "catalog_summary.csv",
    "source_dataset_summary.csv",
    "matrix_resolution_summary.csv",
    "matrix_variable_summary.csv",
    "matrix_colocation_summary.csv",
    "qc_flag_counts.csv",
    "extension_product_summary.csv",
    "extension_variable_summary.csv",
    "extension_source_variable_summary.csv",
    "minimal_release_overview.csv",
    "run_manifest.csv",
    "run_manifest.json",
    "manuscript_minimal_stats.md",
)


class RunningStats:
    """Streaming numeric summary for values that fit a manuscript table."""

    def __init__(self) -> None:
        self.n = 0
        self.total = 0.0
        self.total_sq = 0.0
        self.min_value = math.nan
        self.max_value = math.nan

    def update(self, values: np.ndarray) -> None:
        vals = np.asarray(values, dtype="float64").reshape(-1)
        vals = vals[np.isfinite(vals)]
        if vals.size == 0:
            return
        self.n += int(vals.size)
        self.total += float(np.sum(vals))
        self.total_sq += float(np.sum(vals * vals))
        vmin = float(np.min(vals))
        vmax = float(np.max(vals))
        self.min_value = vmin if not math.isfinite(self.min_value) else min(self.min_value, vmin)
        self.max_value = vmax if not math.isfinite(self.max_value) else max(self.max_value, vmax)

    def as_dict(self) -> dict:
        if self.n == 0:
            return {
                "mean": np.nan,
                "standard_deviation": np.nan,
                "min": np.nan,
                "max": np.nan,
            }
        mean = self.total / self.n
        variance = max(0.0, (self.total_sq / self.n) - (mean * mean))
        return {
            "mean": mean,
            "standard_deviation": math.sqrt(variance),
            "min": self.min_value,
            "max": self.max_value,
        }


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--release-dir",
        default=str(DEFAULT_MINIMAL_RELEASE_DIR),
        help="Path to output/sed_reference_release_minimal.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for minimal-release manuscript statistics.",
    )
    parser.add_argument(
        "--matrix-row-chunk-size",
        type=int,
        default=64,
        help="Station-row chunk size for daily/monthly/annual matrix scans.",
    )
    parser.add_argument(
        "--record-chunk-size",
        type=int,
        default=500000,
        help="Record chunk size for climatology and satellite extension scans.",
    )
    parser.add_argument(
        "--no-clean-output",
        action="store_false",
        dest="clean_output",
        default=True,
        help="Do not remove previously managed minimal stats outputs first.",
    )
    parser.add_argument(
        "--copy-reports",
        action="store_true",
        help="Deprecated compatibility flag; manuscript_minimal_stats.md is always copied to docs/reports/stats_release/minimal/.",
    )
    args = parser.parse_args(argv)
    args.release_dir = Path(args.release_dir).expanduser().resolve()
    args.out_dir = Path(args.out_dir).expanduser().resolve()
    args.matrix_row_chunk_size = max(1, int(args.matrix_row_chunk_size))
    args.record_chunk_size = max(1, int(args.record_chunk_size))
    return args


def clean_managed_outputs(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in MANAGED_OUTPUTS:
        path = out_dir / name
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(str(path))


def copy_minimal_report_to_docs(report_path: Path) -> Path:
    target = DOCS_MINIMAL_REPORT_DIR / Path(report_path).name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(report_path), str(target))
    return target


def file_inventory(ctx: ReleaseContext) -> pd.DataFrame:
    rows = []
    for item in file_manifest(ctx.release_dir):
        path = ctx.release_dir / item["relative_path"]
        rows.append(
            {
                "file_name": item["relative_path"],
                "file_type": "".join(path.suffixes).lstrip(".") or "unknown",
                "size_bytes": item["size_bytes"],
                "size_mb": round(item["size_bytes"] / 1024.0 / 1024.0, 6),
            }
        )
    return pd.DataFrame(rows)


def _date_range_from_text(values: Iterable[object]) -> tuple[str, str]:
    series = pd.to_datetime(pd.Series(list(values), dtype=object), errors="coerce")
    series = series.dropna()
    if series.empty:
        return "", ""
    return str(series.min().date()), str(series.max().date())


def _catalog_date_range(frame: pd.DataFrame) -> tuple[str, str]:
    starts = frame["time_start"] if "time_start" in frame.columns else pd.Series(dtype=object)
    ends = frame["time_end"] if "time_end" in frame.columns else pd.Series(dtype=object)
    start, _ = _date_range_from_text(starts)
    _, end = _date_range_from_text(ends)
    return start, end


def catalog_summary(ctx: ReleaseContext) -> tuple[pd.DataFrame, pd.DataFrame]:
    station = ctx.read_csv(CATALOG_FILES["station_catalog"], required=False)
    source_station = ctx.read_csv(CATALOG_FILES["source_station_catalog"], required=False)
    source_dataset = ctx.read_csv(CATALOG_FILES["source_dataset_catalog"], required=False)
    satellite = ctx.read_csv(CATALOG_FILES["satellite_catalog"], required=False)

    rows = []
    if not station.empty:
        for resolution, sub in station.groupby("resolution", dropna=False):
            start, end = _catalog_date_range(sub)
            rows.append(
                {
                    "table": "station_catalog",
                    "resolution": clean_text(resolution),
                    "rows": int(len(sub)),
                    "unique_cluster_uid": int(sub["cluster_uid"].nunique()) if "cluster_uid" in sub.columns else 0,
                    "record_count_sum": int(pd.to_numeric(sub.get("record_count", 0), errors="coerce").fillna(0).sum()),
                    "n_valid_time_steps_sum": int(
                        pd.to_numeric(sub.get("n_valid_time_steps", 0), errors="coerce").fillna(0).sum()
                    ),
                    "time_start": start,
                    "time_end": end,
                }
            )
    if not source_station.empty:
        for resolution, sub in source_station.groupby("resolution", dropna=False):
            start, end = _catalog_date_range(sub)
            rows.append(
                {
                    "table": "source_station_catalog",
                    "resolution": clean_text(resolution),
                    "rows": int(len(sub)),
                    "unique_cluster_uid": int(sub["cluster_uid"].nunique()) if "cluster_uid" in sub.columns else 0,
                    "record_count_sum": int(pd.to_numeric(sub.get("n_records", 0), errors="coerce").fillna(0).sum()),
                    "n_valid_time_steps_sum": "",
                    "time_start": start,
                    "time_end": end,
                }
            )
    if not satellite.empty:
        start, end = _catalog_date_range(satellite)
        rows.append(
            {
                "table": "satellite_catalog",
                "resolution": "daily",
                "rows": int(len(satellite)),
                "unique_cluster_uid": int(satellite["cluster_uid"].nunique()) if "cluster_uid" in satellite.columns else 0,
                "record_count_sum": int(pd.to_numeric(satellite.get("n_records", 0), errors="coerce").fillna(0).sum()),
                "n_valid_time_steps_sum": "",
                "time_start": start,
                "time_end": end,
            }
        )

    source_dataset_out = pd.DataFrame()
    if not source_dataset.empty:
        source_dataset_out = source_dataset.copy()
        for col in ("n_source_stations", "n_records"):
            if col in source_dataset_out.columns:
                source_dataset_out[col] = pd.to_numeric(source_dataset_out[col], errors="coerce").fillna(0).astype(int)
        if "reference" in source_dataset_out.columns:
            source_dataset_out["reference_short"] = source_dataset_out["reference"].map(lambda v: clean_text(v)[:220])
        keep = [col for col in ["source_name", "n_source_stations", "n_records", "source_url", "reference_short"] if col in source_dataset_out.columns]
        source_dataset_out = source_dataset_out.loc[:, keep].sort_values("n_records", ascending=False)

    return pd.DataFrame(rows), source_dataset_out


def _station_count_for_mask(mask: np.ndarray) -> int:
    if mask.size == 0:
        return 0
    return int(np.count_nonzero(np.any(mask, axis=1)))


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


def scan_matrix_product(ctx: ReleaseContext, resolution: str, file_name: str, row_chunk_size: int) -> dict:
    variable_counts = {
        var: {
            "n_present": 0,
            "n_good": 0,
            "n_estimated": 0,
            "n_usable": 0,
            "stations_with_present": 0,
            "stats": RunningStats(),
        }
        for var in VARIABLES
    }
    flag_counts = defaultdict(int)
    colocation_counts = defaultdict(int)
    colocation_station_counts = defaultdict(int)
    nonempty_cells = 0
    n_valid_time_steps_sum = 0

    with ctx.open_dataset(file_name, required=True) as ds:
        n_stations = int(len(ds.dimensions.get("n_stations", [])))
        n_time = int(len(ds.dimensions.get("time", [])))
        total_cells = int(n_stations * n_time)
        time_axis = decode_time_axis(ds)
        time_start = str(time_axis.min().date()) if len(time_axis) else ""
        time_end = str(time_axis.max().date()) if len(time_axis) else ""
        units = {var: clean_text(getattr(ds.variables[var], "units", "")) if var in ds.variables else "" for var in VARIABLES}
        if "n_valid_time_steps" in ds.variables:
            n_valid = np.ma.asarray(ds.variables["n_valid_time_steps"][:]).filled(0)
            n_valid_time_steps_sum = int(np.sum(n_valid.astype("int64")))

        for start in range(0, n_stations, row_chunk_size):
            stop = min(start + row_chunk_size, n_stations)
            slc = (slice(start, stop), slice(None))
            present_masks = {}
            for var in VARIABLES:
                values = read_numeric_var(ds, var, key=slc)
                present = np.isfinite(values)
                present_masks[var] = present
                variable_counts[var]["n_present"] += int(np.count_nonzero(present))
                variable_counts[var]["stations_with_present"] += _station_count_for_mask(present)
                variable_counts[var]["stats"].update(values[present])

                flag_name = "{}_flag".format(var)
                if flag_name in ds.variables:
                    flags = np.ma.asarray(ds.variables[flag_name][slc]).filled(9).astype("int16")
                    variable_counts[var]["n_good"] += int(np.count_nonzero(present & (flags == 0)))
                    variable_counts[var]["n_estimated"] += int(np.count_nonzero(present & (flags == 1)))
                    variable_counts[var]["n_usable"] += int(np.count_nonzero(present & np.isin(flags, [0, 1])))
                    for flag_value in FLAG_VALUES:
                        flag_counts[(var, int(flag_value))] += int(np.count_nonzero(flags == flag_value))

            any_present = present_masks["Q"] | present_masks["SSC"] | present_masks["SSL"]
            nonempty_cells += int(np.count_nonzero(any_present))
            combos = _matrix_combo_name(present_masks["Q"], present_masks["SSC"], present_masks["SSL"])
            for combo in np.unique(combos):
                combo_text = str(combo)
                mask = combos == combo
                colocation_counts[combo_text] += int(np.count_nonzero(mask))
                colocation_station_counts[combo_text] += _station_count_for_mask(mask)

    variable_rows = []
    for var in VARIABLES:
        row = {
            "product": "matrix",
            "resolution": resolution,
            "variable": var,
            "n_cells": total_cells,
            "n_present": variable_counts[var]["n_present"],
            "n_good": variable_counts[var]["n_good"],
            "n_estimated": variable_counts[var]["n_estimated"],
            "n_usable": variable_counts[var]["n_usable"],
            "stations_with_present": variable_counts[var]["stations_with_present"],
            "present_percent_of_cells": pct(variable_counts[var]["n_present"], total_cells),
            "usable_percent_of_present": pct(variable_counts[var]["n_usable"], variable_counts[var]["n_present"]),
            "unit": units.get(var, ""),
        }
        row.update(variable_counts[var]["stats"].as_dict())
        variable_rows.append(row)

    flag_rows = []
    for (var, flag_value), count in sorted(flag_counts.items()):
        flag_rows.append(
            {
                "product": "matrix",
                "resolution": resolution,
                "variable": var,
                "flag_value": int(flag_value),
                "flag_meaning": FLAG_MEANINGS.get(int(flag_value), ""),
                "count": int(count),
                "percent_of_cells": pct(count, total_cells),
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
                "percent_of_cells": pct(count, total_cells),
                "percent_of_nonempty_cells": pct(count, nonempty_cells) if combo != "none" else 0.0,
            }
        )

    resolution_row = {
        "product": "matrix",
        "resolution": resolution,
        "file_name": file_name,
        "n_stations": n_stations,
        "n_time_steps": n_time,
        "n_cells": total_cells,
        "n_nonempty_cells": int(nonempty_cells),
        "n_valid_time_steps_sum": int(n_valid_time_steps_sum),
        "nonempty_percent_of_cells": pct(nonempty_cells, total_cells),
        "time_start": time_start,
        "time_end": time_end,
    }
    for var in VARIABLES:
        resolution_row["{}_present".format(var)] = variable_counts[var]["n_present"]
        resolution_row["{}_stations_with_present".format(var)] = variable_counts[var]["stations_with_present"]

    return {
        "resolution": resolution_row,
        "variables": variable_rows,
        "flags": flag_rows,
        "colocation": colocation_rows,
    }


def scan_matrix_products(ctx: ReleaseContext, row_chunk_size: int) -> dict:
    scanned = [
        scan_matrix_product(ctx, resolution, file_name, row_chunk_size)
        for resolution, file_name in MATRIX_PRODUCTS.items()
    ]
    return {
        "resolution": pd.DataFrame([item["resolution"] for item in scanned]),
        "variables": pd.DataFrame([row for item in scanned for row in item["variables"]]),
        "flags": pd.DataFrame([row for item in scanned for row in item["flags"]]),
        "colocation": pd.DataFrame([row for item in scanned for row in item["colocation"]]),
    }


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


def scan_record_product(ctx: ReleaseContext, product: str, file_name: str, chunk_size: int) -> dict:
    product_rows = []
    source_counts = defaultdict(lambda: defaultdict(lambda: {"n_records": 0, "n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0, "stats": RunningStats()}))

    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return {"product": pd.DataFrame(), "variables": pd.DataFrame(), "source_variables": pd.DataFrame()}

    with ctx.open_dataset(file_name, required=True) as ds:
        record_dim = _record_dimension_name(ds)
        n_records = int(len(ds.dimensions[record_dim])) if record_dim else 0
        time_axis = decode_time_axis(ds)
        time_start = str(time_axis.min().date()) if len(time_axis) else ""
        time_end = str(time_axis.max().date()) if len(time_axis) else ""
        units = {var: clean_text(getattr(ds.variables[var], "units", "")) if var in ds.variables else "" for var in VARIABLES}
        product_counts = {
            var: {"n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0, "stats": RunningStats()}
            for var in VARIABLES
        }

        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            key = slice(start, stop)
            sources = _record_source_values(ds, product, key)
            for var in VARIABLES:
                values = read_numeric_var(ds, var, key=key)
                present = np.isfinite(values)
                flags = (
                    np.ma.asarray(ds.variables["{}_flag".format(var)][key]).filled(9).astype("int16")
                    if "{}_flag".format(var) in ds.variables
                    else np.full(values.shape, 9, dtype="int16")
                )
                product_counts[var]["n_present"] += int(np.count_nonzero(present))
                product_counts[var]["n_good"] += int(np.count_nonzero(present & (flags == 0)))
                product_counts[var]["n_estimated"] += int(np.count_nonzero(present & (flags == 1)))
                product_counts[var]["n_usable"] += int(np.count_nonzero(present & np.isin(flags, [0, 1])))
                product_counts[var]["stats"].update(values[present])

                for source in sorted(set(sources)):
                    source = clean_text(source) or "unknown"
                    mask = sources == source
                    item = source_counts[source][var]
                    item["n_records"] += int(np.count_nonzero(mask))
                    item["n_present"] += int(np.count_nonzero(mask & present))
                    item["n_good"] += int(np.count_nonzero(mask & present & (flags == 0)))
                    item["n_estimated"] += int(np.count_nonzero(mask & present & (flags == 1)))
                    item["n_usable"] += int(np.count_nonzero(mask & present & np.isin(flags, [0, 1])))
                    item["stats"].update(values[mask & present])

        n_stations = 0
        for dim_name in ("n_stations", "n_satellite_stations"):
            if dim_name in ds.dimensions:
                n_stations = int(len(ds.dimensions[dim_name]))
                break
        product_rows.append(
            {
                "product": product,
                "file_name": file_name,
                "n_stations": n_stations,
                "n_records": n_records,
                "time_start": time_start,
                "time_end": time_end,
            }
        )

    variable_rows = []
    for var in VARIABLES:
        row = {
            "product": product,
            "variable": var,
            "n_records": n_records,
            "n_present": product_counts[var]["n_present"],
            "n_good": product_counts[var]["n_good"],
            "n_estimated": product_counts[var]["n_estimated"],
            "n_usable": product_counts[var]["n_usable"],
            "present_percent": pct(product_counts[var]["n_present"], n_records),
            "usable_percent_of_present": pct(product_counts[var]["n_usable"], product_counts[var]["n_present"]),
            "unit": units.get(var, ""),
        }
        row.update(product_counts[var]["stats"].as_dict())
        variable_rows.append(row)

    source_variable_rows = []
    for source, by_var in sorted(source_counts.items()):
        for var in VARIABLES:
            item = by_var[var]
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
                "usable_percent_of_present": pct(item["n_usable"], item["n_present"]),
                "unit": units.get(var, ""),
            }
            row.update(item["stats"].as_dict())
            source_variable_rows.append(row)

    return {
        "product": pd.DataFrame(product_rows),
        "variables": pd.DataFrame(variable_rows),
        "source_variables": pd.DataFrame(source_variable_rows),
    }


def scan_extension_products(ctx: ReleaseContext, chunk_size: int) -> dict:
    scanned = [scan_record_product(ctx, product, file_name, chunk_size) for product, file_name in EXTENSION_PRODUCTS.items()]
    return {
        "product": pd.concat([item["product"] for item in scanned], ignore_index=True),
        "variables": pd.concat([item["variables"] for item in scanned], ignore_index=True),
        "source_variables": pd.concat([item["source_variables"] for item in scanned], ignore_index=True),
    }


def build_overview(
    inventory: pd.DataFrame,
    catalog: pd.DataFrame,
    source_dataset: pd.DataFrame,
    matrix_resolution: pd.DataFrame,
    matrix_variable: pd.DataFrame,
    extension_product: pd.DataFrame,
) -> pd.DataFrame:
    metrics = [
        ("minimal_files", len(inventory)),
        ("minimal_size_mb", round(pd.to_numeric(inventory.get("size_mb", 0), errors="coerce").fillna(0).sum(), 3)),
        ("matrix_resolutions", matrix_resolution["resolution"].nunique() if "resolution" in matrix_resolution.columns else 0),
        ("matrix_stations_sum", int(pd.to_numeric(matrix_resolution.get("n_stations", 0), errors="coerce").fillna(0).sum())),
        ("matrix_nonempty_cells_sum", int(pd.to_numeric(matrix_resolution.get("n_nonempty_cells", 0), errors="coerce").fillna(0).sum())),
        ("source_datasets", len(source_dataset)),
        ("source_dataset_records_sum", int(pd.to_numeric(source_dataset.get("n_records", 0), errors="coerce").fillna(0).sum())),
        ("catalog_rows_total", int(pd.to_numeric(catalog.get("rows", 0), errors="coerce").fillna(0).sum())),
        ("extension_products", len(extension_product)),
        ("extension_records_sum", int(pd.to_numeric(extension_product.get("n_records", 0), errors="coerce").fillna(0).sum())),
    ]
    for var in VARIABLES:
        sub = matrix_variable[matrix_variable["variable"].astype(str).eq(var)] if "variable" in matrix_variable.columns else pd.DataFrame()
        metrics.append(("matrix_{}_present_sum".format(var), int(pd.to_numeric(sub.get("n_present", 0), errors="coerce").fillna(0).sum())))
    return pd.DataFrame(metrics, columns=["metric", "value"])


def build_report(
    ctx: ReleaseContext,
    out_dir: Path,
    run_started: str,
    run_finished: str,
    release_fp: str,
    script_fp: str,
    overview: pd.DataFrame,
    inventory: pd.DataFrame,
    catalog: pd.DataFrame,
    source_dataset: pd.DataFrame,
    matrix_resolution: pd.DataFrame,
    matrix_variable: pd.DataFrame,
    matrix_colocation: pd.DataFrame,
    extension_product: pd.DataFrame,
    extension_variable: pd.DataFrame,
    extension_source_variable: pd.DataFrame,
) -> list[str]:
    total_size = pd.to_numeric(inventory.get("size_mb", 0), errors="coerce").fillna(0).sum() if not inventory.empty else 0
    lines = [
        "# Minimal Release Manuscript Statistics",
        "",
        "## Run Identity",
        "",
        "- Minimal release package: `{}`".format(display_path(ctx.release_dir)),
        "- Stats output: `{}`".format(display_path(out_dir)),
        "- Run started UTC: `{}`".format(run_started),
        "- Run finished UTC: `{}`".format(run_finished),
        "- Release fingerprint: `{}`".format(release_fp),
        "- Stats script fingerprint: `{}`".format(script_fp),
        "",
        "## Headline",
        "",
        "- Files in minimal package: {}".format(fmt_int(len(inventory))),
        "- Minimal package size: {} MB".format(fmt_float(total_size, digits=2)),
        "- Matrix resolutions: {}".format(fmt_int(matrix_resolution["resolution"].nunique() if "resolution" in matrix_resolution.columns else 0)),
        "- Matrix station rows across resolutions: {}".format(fmt_int(pd.to_numeric(matrix_resolution.get("n_stations", 0), errors="coerce").fillna(0).sum())),
        "- Matrix nonempty station-time cells: {}".format(fmt_int(pd.to_numeric(matrix_resolution.get("n_nonempty_cells", 0), errors="coerce").fillna(0).sum())),
        "- Source datasets listed: {}".format(fmt_int(len(source_dataset))),
        "- Extension records: {}".format(fmt_int(pd.to_numeric(extension_product.get("n_records", 0), errors="coerce").fillna(0).sum())),
        "",
        "## Overview Metrics",
        "",
        sorted_markdown_table(overview, columns=["metric", "value"], max_rows=30),
        "",
        "## Matrix Resolution Summary",
        "",
        sorted_markdown_table(
            matrix_resolution,
            columns=[
                "resolution",
                "n_stations",
                "n_time_steps",
                "n_cells",
                "n_nonempty_cells",
                "nonempty_percent_of_cells",
                "time_start",
                "time_end",
            ],
            sort_by="n_nonempty_cells",
            max_rows=10,
        ),
        "",
        "## Matrix Variable Summary",
        "",
        sorted_markdown_table(
            matrix_variable,
            columns=[
                "resolution",
                "variable",
                "n_present",
                "n_good",
                "n_estimated",
                "n_usable",
                "stations_with_present",
                "present_percent_of_cells",
                "mean",
                "min",
                "max",
                "unit",
            ],
            sort_by="n_present",
            max_rows=18,
        ),
        "",
        "## Matrix Co-Location Summary",
        "",
        sorted_markdown_table(
            matrix_colocation[matrix_colocation["combination"].astype(str).ne("none")]
            if not matrix_colocation.empty and "combination" in matrix_colocation.columns
            else matrix_colocation,
            columns=["resolution", "combination", "n_cells", "stations_with_combination", "percent_of_nonempty_cells"],
            sort_by="n_cells",
            max_rows=18,
        ),
        "",
        "## Catalog Summary",
        "",
        sorted_markdown_table(
            catalog,
            columns=["table", "resolution", "rows", "unique_cluster_uid", "record_count_sum", "time_start", "time_end"],
            sort_by="record_count_sum",
            max_rows=18,
        ),
        "",
        "## Top Source Datasets",
        "",
        sorted_markdown_table(
            source_dataset,
            columns=["source_name", "n_source_stations", "n_records", "source_url"],
            sort_by="n_records",
            max_rows=20,
        ),
        "",
        "## Extension Products",
        "",
        sorted_markdown_table(
            extension_product,
            columns=["product", "n_stations", "n_records", "time_start", "time_end"],
            sort_by="n_records",
            max_rows=10,
        ),
        "",
        "## Extension Variable Summary",
        "",
        sorted_markdown_table(
            extension_variable,
            columns=["product", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "unit"],
            sort_by="n_present",
            max_rows=18,
        ),
        "",
        "## Satellite Source By Variable",
        "",
        sorted_markdown_table(
            extension_source_variable[extension_source_variable["product"].astype(str).eq("satellite")]
            if not extension_source_variable.empty and "product" in extension_source_variable.columns
            else extension_source_variable,
            columns=["source_name", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "unit"],
            sort_by="n_records",
            max_rows=18,
        ),
        "",
        "## Output Tables",
        "",
    ]
    for name in MANAGED_OUTPUTS:
        if name.endswith(".csv"):
            lines.append("- `{}`".format(name))
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- Matrix denominators are station-time cells, not source-record rows.",
            "- `n_nonempty_cells` counts cells where at least one of Q, SSC, or SSL is present.",
            "- `n_usable` counts present values with flag 0 or 1, matching the good-or-estimated interpretation used elsewhere in release statistics.",
            "- Satellite rows are validation-only and should be filtered by source and variable before analysis.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    args = parse_args(argv)
    if not args.release_dir.is_dir():
        raise FileNotFoundError("Minimal release directory not found: {}".format(args.release_dir))

    ctx = ReleaseContext(release_dir=args.release_dir, out_dir=args.out_dir, strict_release_only=True)
    if args.clean_output:
        clean_managed_outputs(args.out_dir)
    else:
        args.out_dir.mkdir(parents=True, exist_ok=True)

    run_started = utc_now()
    release_fp = metadata_fingerprint(ctx.release_dir)
    script_fp = script_fingerprint()

    inventory = file_inventory(ctx)
    catalog, source_dataset = catalog_summary(ctx)
    matrix = scan_matrix_products(ctx, args.matrix_row_chunk_size)
    extension = scan_extension_products(ctx, args.record_chunk_size)
    overview = build_overview(
        inventory,
        catalog,
        source_dataset,
        matrix["resolution"],
        matrix["variables"],
        extension["product"],
    )

    outputs = {
        "file_inventory.csv": inventory,
        "catalog_summary.csv": catalog,
        "source_dataset_summary.csv": source_dataset,
        "matrix_resolution_summary.csv": matrix["resolution"],
        "matrix_variable_summary.csv": matrix["variables"],
        "matrix_colocation_summary.csv": matrix["colocation"],
        "qc_flag_counts.csv": matrix["flags"],
        "extension_product_summary.csv": extension["product"],
        "extension_variable_summary.csv": extension["variables"],
        "extension_source_variable_summary.csv": extension["source_variables"],
        "minimal_release_overview.csv": overview,
    }
    for file_name, frame in outputs.items():
        write_csv(frame, ctx.output_path(file_name))

    run_finished = utc_now()
    manifest = pd.DataFrame(file_manifest(args.out_dir))
    manifest["release_fingerprint"] = release_fp
    manifest["stats_script_fingerprint"] = script_fp
    manifest["run_started_utc"] = run_started
    manifest["run_finished_utc"] = run_finished
    write_csv(manifest, ctx.output_path("run_manifest.csv"))
    write_json(
        {
            "release_dir": str(ctx.release_dir),
            "out_dir": str(args.out_dir),
            "run_started_utc": run_started,
            "run_finished_utc": run_finished,
            "release_fingerprint": release_fp,
            "stats_script_fingerprint": script_fp,
            "matrix_row_chunk_size": int(args.matrix_row_chunk_size),
            "record_chunk_size": int(args.record_chunk_size),
            "outputs": manifest.to_dict(orient="records"),
        },
        ctx.output_path("run_manifest.json"),
    )

    report = build_report(
        ctx,
        args.out_dir,
        run_started,
        run_finished,
        release_fp,
        script_fp,
        overview,
        inventory,
        catalog,
        source_dataset,
        matrix["resolution"],
        matrix["variables"],
        matrix["colocation"],
        extension["product"],
        extension["variables"],
        extension["source_variables"],
    )
    report_path = ctx.output_path("manuscript_minimal_stats.md")
    write_markdown(report, report_path)
    copied_report = copy_minimal_report_to_docs(report_path)

    print("Wrote minimal release manuscript stats to {}".format(args.out_dir))
    print("Report: {}".format(report_path))
    if copied_report is not None:
        print("Copied report: {}".format(copied_report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
