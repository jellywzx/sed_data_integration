#!/usr/bin/env python3
"""
Script 01: release inventory and scale statistics for sediment reference release.

Purpose
-------
Generate final release-product inventory, NetCDF schema summaries, data-volume
statistics, coverage rates, and GeoPackage feature counts for abstract/results
text and final QA.

Default target release directory:
    ../output/sed_reference_release/ when this script is placed under stats/,
    with fallback auto-detection for scripts_basin_test/output/sed_reference_release/.

Default output directory:
    ../output_other/ when this script is placed under stats/.

Typical use
-----------
    python3 stats/stats_release_inventory.py
    python3 stats/stats_release_inventory.py --release-dir /path/to/sed_reference_release
    python3 stats/stats_release_inventory.py --release-dir ... --out-dir ...

Outputs
-------
    release_inventory_stats_summary.csv
        Long-form metric table: one row per statistic.

    release_inventory_stats_summary_wide.csv
        One-row table with metric keys as columns, convenient for abstract text.

    release_inventory_stats_files.csv
        All files included in the release directory, with size and type.

    release_inventory_stats_netcdf_schema.csv
        Dimensions, variables, and global attributes for each release NetCDF.

    release_inventory_stats_gpkg_layers.csv
        GeoPackage feature/layer counts, plus distinct UID counts where possible.

    release_inventory_stats_article_metrics.csv
        Article-ready key numbers selected from the full summary outputs.

    release_inventory_stats_summary.md
        Human-readable Markdown summary with article-ready key numbers.

Notes
-----
- NetCDF numeric missing values are counted using masks, _FillValue,
  missing_value, and finite checks for floating-point arrays.
- Matrix non-missing cell count means station-time cells with at least one of
  Q, SSC, or SSL present. If n_valid_time_steps is available, its sum is used
  because it is written by the matrix export script with exactly this meaning.
- GeoPackage counts are read via SQLite, so geopandas/fiona are not required.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - runtime dependency check
    nc4 = None


# Standard release contract names used by s8_publish_reference_dataset.py.
NETCDF_PRODUCTS = OrderedDict(
    [
        ("master", "sed_reference_master.nc"),
        ("daily_matrix", "sed_reference_timeseries_daily.nc"),
        ("monthly_matrix", "sed_reference_timeseries_monthly.nc"),
        ("annual_matrix", "sed_reference_timeseries_annual.nc"),
        ("climatology", "sed_reference_climatology.nc"),
        ("satellite_validation", "sed_reference_satellite_validation.nc"),
    ]
)

MATRIX_PRODUCTS = OrderedDict(
    [
        ("daily", "sed_reference_timeseries_daily.nc"),
        ("monthly", "sed_reference_timeseries_monthly.nc"),
        ("annual", "sed_reference_timeseries_annual.nc"),
    ]
)

GPKG_PRODUCTS = OrderedDict(
    [
        ("cluster_points", "sed_reference_cluster_points.gpkg"),
        ("source_stations", "sed_reference_source_stations.gpkg"),
        ("cluster_basins", "sed_reference_cluster_basins.gpkg"),
    ]
)

CATALOG_PRODUCTS = OrderedDict(
    [
        ("station_catalog", "station_catalog.csv"),
        ("source_station_catalog", "source_station_catalog.csv"),
        ("source_dataset_catalog", "source_dataset_catalog.csv"),
        ("overlap_candidates", "sed_reference_overlap_candidates.csv.gz"),
        ("overlap_candidates_parquet", "sed_reference_overlap_candidates.parquet"),
        ("satellite_candidates", "sed_reference_satellite_candidates.csv.gz"),
        ("satellite_candidates_parquet", "sed_reference_satellite_candidates.parquet"),
        ("satellite_validation_catalog", "satellite_validation_catalog.csv"),
        ("validation_report", "release_validation_report.csv"),
        ("release_inventory", "release_inventory.csv"),
        ("readme", "README.md"),
    ]
)

CATALOG_COUNT_PRODUCTS = OrderedDict(
    [
        ("satellite_validation_catalog", "satellite_validation_catalog.csv"),
        ("satellite_candidates", "sed_reference_satellite_candidates.csv.gz"),
        ("overlap_candidates", "sed_reference_overlap_candidates.csv.gz"),
    ]
)

RESOLUTION_CODE_TO_NAME = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}

MEASURE_VARS = ("Q", "SSC", "SSL")
FALLBACK_FILL_VALUES = (-9999, -9999.0, 1.0e20)


def clean_scalar(value: Any, max_len: int = 2000) -> str:
    """Return a compact, CSV-safe string representation."""
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    elif isinstance(value, np.ndarray):
        try:
            text = json.dumps(value.tolist(), ensure_ascii=False)
        except Exception:
            text = str(value)
    elif isinstance(value, (list, tuple)):
        try:
            text = json.dumps(list(value), ensure_ascii=False)
        except Exception:
            text = str(value)
    else:
        text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        return text[: max_len - 3] + "..."
    return text


def as_number(value: Any) -> Any:
    """Convert numpy scalars to plain Python scalars for JSON/CSV outputs."""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if math.isnan(float(value)):
            return ""
        return float(value)
    return value


def metric_row(
    metric: str,
    value: Any,
    *,
    group: str,
    product: str = "",
    file_name: str = "",
    unit: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    return {
        "group": group,
        "product": product,
        "metric": metric,
        "value": as_number(value),
        "unit": unit,
        "file_name": file_name,
        "notes": notes,
    }


def metric_key(row: Dict[str, Any]) -> str:
    bits = [row.get("group", ""), row.get("product", ""), row.get("metric", "")]
    text = "_".join(bit for bit in bits if bit)
    text = re.sub(r"[^0-9A-Za-z]+", "_", text).strip("_").lower()
    return text


def infer_file_kind(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".nc"):
        return "netcdf"
    if name.endswith(".gpkg"):
        return "geopackage"
    if name.endswith(".csv") or name.endswith(".csv.gz"):
        return "csv"
    if name.endswith(".md"):
        return "markdown"
    if name.endswith(".py"):
        return "python"
    return path.suffix.lower().lstrip(".") or "file"


def file_inventory(release_dir: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if not release_dir.exists():
        return pd.DataFrame(columns=["relative_path", "file_name", "kind", "size_bytes", "size_mb"])
    for path in sorted(p for p in release_dir.rglob("*") if p.is_file()):
        rel = path.relative_to(release_dir)
        rows.append(
            {
                "relative_path": str(rel),
                "file_name": path.name,
                "kind": infer_file_kind(path),
                "size_bytes": int(path.stat().st_size),
                "size_mb": round(path.stat().st_size / (1024 * 1024), 6),
            }
        )
    return pd.DataFrame(rows)


def dim_size(ds: Any, name: str) -> int:
    dim = ds.dimensions.get(name)
    return int(len(dim)) if dim is not None else 0


def var_shape(var: Any) -> Tuple[int, ...]:
    return tuple(int(x) for x in getattr(var, "shape", ()))


def variable_fill_values(var: Any) -> List[Any]:
    fills: List[Any] = []
    for attr in ("_FillValue", "missing_value"):
        if hasattr(var, attr):
            raw = getattr(var, attr)
            arr = np.asarray(raw).reshape(-1)
            fills.extend(arr.tolist())
    fills.extend(FALLBACK_FILL_VALUES)

    # Deduplicate while preserving comparable scalar values.
    result: List[Any] = []
    seen = set()
    for item in fills:
        try:
            key = float(item)
            if not np.isfinite(key):
                continue
            key = ("float", key)
        except Exception:
            key = ("text", str(item))
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def valid_mask_from_array(data: Any, var: Any) -> np.ndarray:
    """Return True where values are not missing for a NetCDF variable chunk."""
    masked = np.ma.asarray(data)
    if np.ma.isMaskedArray(masked):
        valid = ~np.ma.getmaskarray(masked)
        arr = masked.filled(np.nan)
    else:
        arr = np.asarray(masked)
        valid = np.ones(arr.shape, dtype=bool)

    if np.issubdtype(arr.dtype, np.floating):
        valid &= np.isfinite(arr)
    elif np.issubdtype(arr.dtype, np.integer):
        # Integers are finite by construction.
        pass
    else:
        text = arr.astype(str)
        valid &= text != ""
        valid &= pd.Series(text.reshape(-1)).str.lower().ne("nan").to_numpy().reshape(text.shape)
        return valid

    for fill in variable_fill_values(var):
        try:
            valid &= arr != np.asarray(fill, dtype=arr.dtype)
        except Exception:
            continue
    return valid


def iter_variable_slices(shape: Sequence[int], chunk_target: int = 2_000_000) -> Iterable[Tuple[slice, ...]]:
    """Yield slices that keep each loaded chunk reasonably small."""
    if not shape:
        yield tuple()
        return
    shape = tuple(int(x) for x in shape)
    if len(shape) == 1:
        step = max(1, min(shape[0], chunk_target))
        for start in range(0, shape[0], step):
            yield (slice(start, min(start + step, shape[0])),)
        return

    # Chunk along the first dimension and keep all trailing dimensions.
    trailing = int(np.prod(shape[1:])) if len(shape) > 1 else 1
    first_step = max(1, min(shape[0], chunk_target // max(1, trailing)))
    for start in range(0, shape[0], first_step):
        yield (slice(start, min(start + first_step, shape[0])),) + tuple(slice(None) for _ in shape[1:])


def count_nonmissing_var(var: Any, chunk_target: int = 2_000_000) -> int:
    """Count non-missing cells in a numeric/string NetCDF variable."""
    shape = var_shape(var)
    if not shape:
        try:
            return int(valid_mask_from_array(var[...], var).sum())
        except Exception:
            return 0
    count = 0
    for slc in iter_variable_slices(shape, chunk_target=chunk_target):
        data = var[slc]
        count += int(valid_mask_from_array(data, var).sum())
    return count


def count_any_nonmissing(ds: Any, var_names: Sequence[str], chunk_target: int = 2_000_000) -> Optional[int]:
    """Count cells where any of the given variables are present."""
    vars_existing = [ds.variables[name] for name in var_names if name in ds.variables]
    if not vars_existing:
        return None
    shape = var_shape(vars_existing[0])
    if not shape:
        return int(any(count_nonmissing_var(var) > 0 for var in vars_existing))

    total = 0
    for slc in iter_variable_slices(shape, chunk_target=chunk_target):
        any_valid = np.zeros(np.asarray(vars_existing[0][slc]).shape, dtype=bool)
        for var in vars_existing:
            if var_shape(var) != shape:
                continue
            any_valid |= valid_mask_from_array(var[slc], var)
        total += int(any_valid.sum())
    return total


def read_int_values_chunked(var: Any, chunk_target: int = 4_000_000) -> np.ndarray:
    """Read a 1D integer-like variable into memory for small cardinality summaries."""
    pieces: List[np.ndarray] = []
    for slc in iter_variable_slices(var_shape(var), chunk_target=chunk_target):
        raw = np.ma.asarray(var[slc])
        if np.ma.isMaskedArray(raw):
            arr = raw.filled(-9999)
        else:
            arr = np.asarray(raw)
        pieces.append(arr.reshape(-1).astype(np.int64, copy=False))
    if not pieces:
        return np.array([], dtype=np.int64)
    return np.concatenate(pieces)


def netcdf_schema_rows(path: Path, product: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if nc4 is None or not path.is_file():
        return rows
    with nc4.Dataset(path, "r") as ds:
        for name, dim in ds.dimensions.items():
            rows.append(
                {
                    "product": product,
                    "file_name": path.name,
                    "object_type": "dimension",
                    "name": name,
                    "dimensions": "",
                    "shape": int(len(dim)),
                    "dtype": "",
                    "attribute": "",
                    "value": "",
                    "attrs_json": "",
                }
            )
        for name, var in ds.variables.items():
            attrs = {attr: clean_scalar(getattr(var, attr)) for attr in var.ncattrs()}
            rows.append(
                {
                    "product": product,
                    "file_name": path.name,
                    "object_type": "variable",
                    "name": name,
                    "dimensions": "|".join(getattr(var, "dimensions", ())),
                    "shape": "|".join(str(x) for x in var_shape(var)),
                    "dtype": str(getattr(var, "dtype", "")),
                    "attribute": "",
                    "value": "",
                    "attrs_json": json.dumps(attrs, ensure_ascii=False, sort_keys=True),
                }
            )
        for attr in ds.ncattrs():
            rows.append(
                {
                    "product": product,
                    "file_name": path.name,
                    "object_type": "global_attribute",
                    "name": "",
                    "dimensions": "",
                    "shape": "",
                    "dtype": "",
                    "attribute": attr,
                    "value": clean_scalar(getattr(ds, attr)),
                    "attrs_json": "",
                }
            )
    return rows


def add_netcdf_file_schema_metrics(path: Path, product: str, summary: List[Dict[str, Any]]) -> None:
    if nc4 is None or not path.is_file():
        return
    with nc4.Dataset(path, "r") as ds:
        summary.append(metric_row("dimension_count", len(ds.dimensions), group="netcdf_schema", product=product, file_name=path.name))
        summary.append(metric_row("variable_count", len(ds.variables), group="netcdf_schema", product=product, file_name=path.name))
        summary.append(metric_row("global_attribute_count", len(ds.ncattrs()), group="netcdf_schema", product=product, file_name=path.name))


def summarize_master(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if nc4 is None or not path.is_file():
        rows.append(metric_row("status", "missing", group="master", product="master", file_name=path.name))
        return rows
    with nc4.Dataset(path, "r") as ds:
        n_stations = dim_size(ds, "n_stations")
        n_source_stations = dim_size(ds, "n_source_stations")
        n_records = dim_size(ds, "n_records")
        rows.extend(
            [
                metric_row("cluster_count", n_stations, group="master", product="master", file_name=path.name, unit="clusters"),
                metric_row("source_station_count", n_source_stations, group="master", product="master", file_name=path.name, unit="source stations"),
                metric_row("observation_record_count", n_records, group="master", product="master", file_name=path.name, unit="records"),
            ]
        )

        if "resolution" in ds.variables:
            vals = read_int_values_chunked(ds.variables["resolution"])
            unique_codes = sorted(int(v) for v in np.unique(vals) if int(v) in RESOLUTION_CODE_TO_NAME)
            names = [RESOLUTION_CODE_TO_NAME[code] for code in unique_codes]
            rows.append(
                metric_row(
                    "temporal_resolution_types",
                    "|".join(names),
                    group="master",
                    product="master",
                    file_name=path.name,
                    notes="decoded from resolution flag values",
                )
            )
            for code in unique_codes:
                name = RESOLUTION_CODE_TO_NAME[code]
                rows.append(
                    metric_row(
                        f"resolution_{name}_record_count",
                        int(np.count_nonzero(vals == code)),
                        group="master",
                        product="master",
                        file_name=path.name,
                        unit="records",
                    )
                )

        for var_name in MEASURE_VARS:
            if var_name in ds.variables:
                rows.append(
                    metric_row(
                        f"{var_name}_nonmissing_record_count",
                        count_nonmissing_var(ds.variables[var_name]),
                        group="master",
                        product="master",
                        file_name=path.name,
                        unit="records",
                    )
                )
            else:
                rows.append(
                    metric_row(
                        f"{var_name}_nonmissing_record_count",
                        "",
                        group="master",
                        product="master",
                        file_name=path.name,
                        unit="records",
                        notes="variable missing",
                    )
                )
    return rows


def summarize_matrix(path: Path, resolution: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    product = f"{resolution}_matrix"
    if nc4 is None or not path.is_file():
        rows.append(metric_row("status", "missing", group="matrix", product=product, file_name=path.name))
        return rows
    with nc4.Dataset(path, "r") as ds:
        n_stations = dim_size(ds, "n_stations")
        n_time = dim_size(ds, "time")
        total_cells = int(n_stations * n_time)
        rows.extend(
            [
                metric_row("station_cluster_count", n_stations, group="matrix", product=product, file_name=path.name, unit="stations/clusters"),
                metric_row("time_step_count", n_time, group="matrix", product=product, file_name=path.name, unit="time steps"),
                metric_row("total_station_time_cells", total_cells, group="matrix", product=product, file_name=path.name, unit="cells"),
            ]
        )

        # Matrix exporter writes n_valid_time_steps as cells where any Q/SSC/SSL exists.
        if "n_valid_time_steps" in ds.variables:
            # count_nonmissing_var counts valid station rows, not the sum. We need the actual sum.
            raw = np.ma.asarray(ds.variables["n_valid_time_steps"][:])
            if np.ma.isMaskedArray(raw):
                arr = raw.filled(0)
            else:
                arr = np.asarray(raw)
            any_nonmissing = int(np.nansum(arr.astype(np.float64)))
        else:
            any_nonmissing = count_any_nonmissing(ds, MEASURE_VARS) or 0

        rows.append(
            metric_row(
                "nonmissing_cell_count_any_Q_SSC_SSL",
                any_nonmissing,
                group="matrix",
                product=product,
                file_name=path.name,
                unit="cells",
                notes="station-time cells with at least one of Q, SSC, SSL present",
            )
        )
        rows.append(
            metric_row(
                "coverage_any_Q_SSC_SSL",
                round(any_nonmissing / total_cells, 8) if total_cells else "",
                group="matrix",
                product=product,
                file_name=path.name,
                unit="fraction",
            )
        )

        for var_name in MEASURE_VARS:
            if var_name not in ds.variables:
                rows.append(
                    metric_row(
                        f"{var_name}_coverage",
                        "",
                        group="matrix",
                        product=product,
                        file_name=path.name,
                        unit="fraction",
                        notes="variable missing",
                    )
                )
                continue
            nonmissing = count_nonmissing_var(ds.variables[var_name])
            rows.append(
                metric_row(
                    f"{var_name}_nonmissing_cell_count",
                    nonmissing,
                    group="matrix",
                    product=product,
                    file_name=path.name,
                    unit="cells",
                )
            )
            rows.append(
                metric_row(
                    f"{var_name}_coverage",
                    round(nonmissing / total_cells, 8) if total_cells else "",
                    group="matrix",
                    product=product,
                    file_name=path.name,
                    unit="fraction",
                )
            )
            rows.append(
                metric_row(
                    f"{var_name}_coverage_pct",
                    round(100.0 * nonmissing / total_cells, 4) if total_cells else "",
                    group="matrix",
                    product=product,
                    file_name=path.name,
                    unit="percent",
                )
            )
    return rows


def summarize_climatology(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    product = "climatology"
    if nc4 is None or not path.is_file():
        rows.append(metric_row("status", "missing", group="climatology", product=product, file_name=path.name))
        return rows
    with nc4.Dataset(path, "r") as ds:
        n_stations = dim_size(ds, "n_stations")
        n_sources = dim_size(ds, "n_sources")
        n_records = dim_size(ds, "n_records")
        rows.extend(
            [
                metric_row("climatology_station_count", n_stations, group="climatology", product=product, file_name=path.name, unit="stations"),
                metric_row("source_count", n_sources, group="climatology", product=product, file_name=path.name, unit="sources"),
                metric_row("record_count", n_records, group="climatology", product=product, file_name=path.name, unit="records"),
            ]
        )
        for var_name in MEASURE_VARS:
            if var_name in ds.variables:
                rows.append(
                    metric_row(
                        f"{var_name}_nonmissing_count",
                        count_nonmissing_var(ds.variables[var_name]),
                        group="climatology",
                        product=product,
                        file_name=path.name,
                        unit="records",
                    )
                )
            else:
                rows.append(
                    metric_row(
                        f"{var_name}_nonmissing_count",
                        "",
                        group="climatology",
                        product=product,
                        file_name=path.name,
                        unit="records",
                        notes="variable missing",
                    )
                )
    return rows



def summarize_satellite_validation(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    product = "satellite_validation"
    if nc4 is None or not path.is_file():
        rows.append(metric_row("status", "missing", group="satellite_validation", product=product, file_name=path.name))
        return rows
    with nc4.Dataset(path, "r") as ds:
        dims = {
            "satellite_station_count": dim_size(ds, "n_satellite_stations"),
            "satellite_record_count": dim_size(ds, "n_satellite_records"),
            "source_count": dim_size(ds, "n_sources"),
        }
        for metric, value in dims.items():
            unit = "records" if metric == "satellite_record_count" else "stations" if metric == "satellite_station_count" else "sources"
            rows.append(metric_row(metric, value, group="satellite_validation", product=product, file_name=path.name, unit=unit))
    return rows


def count_csv_rows_chunked(path: Path, chunksize: int = 500_000) -> Any:
    if not path.is_file():
        return ""
    total = 0
    for chunk in pd.read_csv(path, usecols=[0], chunksize=chunksize):
        total += len(chunk)
    return int(total)


def summarize_catalog_counts(release_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product, file_name in CATALOG_COUNT_PRODUCTS.items():
        path = release_dir / file_name
        metric = f"{product}_row_count"
        if not path.is_file():
            rows.append(metric_row(metric, "", group="catalog_counts", product=product, file_name=file_name, unit="rows", notes="file missing"))
            continue
        rows.append(
            metric_row(
                metric,
                count_csv_rows_chunked(path),
                group="catalog_counts",
                product=product,
                file_name=file_name,
                unit="rows",
                notes="counted with pandas chunksize to avoid loading the full file at once",
            )
        )
    return rows

def sqlite_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def preferred_uid_columns(product: str) -> Tuple[str, ...]:
    if product == "source_stations":
        return ("source_station_uid", "cluster_uid", "station_uid")
    if product in {"cluster_points", "cluster_basins"}:
        return ("cluster_uid", "source_station_uid", "station_uid")
    return ("source_station_uid", "cluster_uid", "station_uid")


def gpkg_layer_rows(path: Path, product: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.is_file():
        return rows
    try:
        with sqlite3.connect(str(path)) as con:
            layer_info = pd.read_sql_query(
                "SELECT table_name, data_type, identifier FROM gpkg_contents WHERE data_type='features' ORDER BY table_name",
                con,
            )
            for _, layer in layer_info.iterrows():
                table = str(layer["table_name"])
                quoted = sqlite_identifier(table)
                count = int(con.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
                col_info = con.execute(f"PRAGMA table_info({quoted})").fetchall()
                cols = [str(item[1]) for item in col_info]
                uid_col = ""
                for candidate in preferred_uid_columns(product):
                    if candidate in cols:
                        uid_col = candidate
                        break
                distinct_uid_count = ""
                if uid_col:
                    distinct_uid_count = int(
                        con.execute(
                            f"SELECT COUNT(DISTINCT {sqlite_identifier(uid_col)}) FROM {quoted} "
                            f"WHERE {sqlite_identifier(uid_col)} IS NOT NULL AND CAST({sqlite_identifier(uid_col)} AS TEXT) <> ''"
                        ).fetchone()[0]
                    )
                rows.append(
                    {
                        "product": product,
                        "file_name": path.name,
                        "layer_name": table,
                        "feature_count": count,
                        "uid_column": uid_col,
                        "distinct_uid_count": distinct_uid_count,
                        "columns": "|".join(cols),
                    }
                )
    except Exception as exc:
        rows.append(
            {
                "product": product,
                "file_name": path.name,
                "layer_name": "",
                "feature_count": "",
                "uid_column": "",
                "distinct_uid_count": "",
                "columns": "",
                "error": str(exc),
            }
        )
    return rows


def summarize_gpkgs(release_dir: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    summary: List[Dict[str, Any]] = []
    layer_rows: List[Dict[str, Any]] = []

    for product, file_name in GPKG_PRODUCTS.items():
        path = release_dir / file_name
        layers = gpkg_layer_rows(path, product)
        layer_rows.extend(layers)
        if not path.is_file():
            summary.append(metric_row("status", "missing", group="geopackage", product=product, file_name=file_name))
            continue
        valid_layers = [row for row in layers if row.get("feature_count", "") != ""]
        summary.append(metric_row("layer_count", len(valid_layers), group="geopackage", product=product, file_name=file_name, unit="layers"))
        summary.append(
            metric_row(
                "feature_count_total_all_layers",
                int(sum(int(row["feature_count"]) for row in valid_layers)),
                group="geopackage",
                product=product,
                file_name=file_name,
                unit="features",
            )
        )
        for row in valid_layers:
            layer = row["layer_name"]
            summary.append(
                metric_row(
                    f"layer_{layer}_feature_count",
                    int(row["feature_count"]),
                    group="geopackage",
                    product=product,
                    file_name=file_name,
                    unit="features",
                )
            )
            if row.get("distinct_uid_count", "") != "":
                summary.append(
                    metric_row(
                        f"layer_{layer}_distinct_{row['uid_column']}_count",
                        int(row["distinct_uid_count"]),
                        group="geopackage",
                        product=product,
                        file_name=file_name,
                        unit="unique ids",
                    )
                )

        # Abstract-friendly headline counts.
        if product == "cluster_points":
            headline = next((row for row in valid_layers if row["layer_name"] == "cluster_summary"), None)
            if headline is None and valid_layers:
                # Fallback: use the maximum distinct cluster_uid count across layers.
                uid_counts = [int(row["distinct_uid_count"]) for row in valid_layers if row.get("uid_column") == "cluster_uid" and row.get("distinct_uid_count", "") != ""]
                value = max(uid_counts) if uid_counts else int(sum(int(row["feature_count"]) for row in valid_layers))
            else:
                value = int(headline["feature_count"]) if headline else 0
            summary.append(metric_row("cluster_point_count", value, group="geopackage_headline", product=product, file_name=file_name, unit="points"))
        elif product == "source_stations":
            uid_counts = [int(row["distinct_uid_count"]) for row in valid_layers if row.get("uid_column") == "source_station_uid" and row.get("distinct_uid_count", "") != ""]
            value = max(uid_counts) if uid_counts else int(sum(int(row["feature_count"]) for row in valid_layers))
            summary.append(metric_row("source_station_point_count", value, group="geopackage_headline", product=product, file_name=file_name, unit="points"))
        elif product == "cluster_basins":
            value = int(sum(int(row["feature_count"]) for row in valid_layers))
            summary.append(metric_row("basin_polygon_count", value, group="geopackage_headline", product=product, file_name=file_name, unit="polygons"))

    return summary, layer_rows



def summary_value(summary_df: pd.DataFrame, group: str, product: str, metric: str) -> Any:
    hit = summary_df[
        (summary_df["group"] == group)
        & (summary_df["product"] == product)
        & (summary_df["metric"] == metric)
    ]
    if hit.empty:
        return ""
    return hit.iloc[0]["value"]


def summary_unit(summary_df: pd.DataFrame, group: str, product: str, metric: str) -> str:
    hit = summary_df[
        (summary_df["group"] == group)
        & (summary_df["product"] == product)
        & (summary_df["metric"] == metric)
    ]
    if hit.empty:
        return ""
    return str(hit.iloc[0].get("unit", ""))


def summary_file(summary_df: pd.DataFrame, group: str, product: str, metric: str) -> str:
    hit = summary_df[
        (summary_df["group"] == group)
        & (summary_df["product"] == product)
        & (summary_df["metric"] == metric)
    ]
    if hit.empty:
        return ""
    return str(hit.iloc[0].get("file_name", ""))


def article_metric_row(section: str, metric: str, value: Any, *, unit: str = "", source_file: str = "", notes: str = "") -> Dict[str, Any]:
    return {
        "section": section,
        "metric": metric,
        "value": as_number(value),
        "unit": unit,
        "source_file": source_file,
        "notes": notes,
    }


def pct_from_fraction(value: Any) -> Any:
    try:
        if value == "" or pd.isna(value):
            return ""
        return round(100.0 * float(value), 4)
    except Exception:
        return ""


def build_article_metrics(summary_df: pd.DataFrame, files_df: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    def add(section: str, group: str, product: str, source_metric: str, *, metric: Optional[str] = None, notes: str = "") -> None:
        value = summary_value(summary_df, group, product, source_metric)
        rows.append(
            article_metric_row(
                section,
                metric or source_metric,
                value,
                unit=summary_unit(summary_df, group, product, source_metric),
                source_file=summary_file(summary_df, group, product, source_metric),
                notes=notes,
            )
        )

    add("release", "release", "", "file_count", metric="release_file_count")
    add("release", "release", "", "total_size_mb", metric="release_total_size_mb")

    add("master", "master", "master", "cluster_count")
    add("master", "master", "master", "source_station_count")
    add("master", "master", "master", "observation_record_count")
    add("master", "master", "master", "temporal_resolution_types")
    for resolution in ("daily", "monthly", "annual"):
        add("master", "master", "master", f"resolution_{resolution}_record_count", metric=f"{resolution}_record_count")
    for var_name in MEASURE_VARS:
        add("master", "master", "master", f"{var_name}_nonmissing_record_count")

    for resolution in ("daily", "monthly", "annual"):
        product = f"{resolution}_matrix"
        section = f"matrix_{resolution}"
        add(section, "matrix", product, "station_cluster_count")
        add(section, "matrix", product, "time_step_count")
        add(section, "matrix", product, "total_station_time_cells")
        add(section, "matrix", product, "nonmissing_cell_count_any_Q_SSC_SSL")
        coverage = summary_value(summary_df, "matrix", product, "coverage_any_Q_SSC_SSL")
        rows.append(
            article_metric_row(
                section,
                "coverage_any_Q_SSC_SSL_pct",
                pct_from_fraction(coverage),
                unit="percent",
                source_file=summary_file(summary_df, "matrix", product, "coverage_any_Q_SSC_SSL"),
                notes="100 * coverage_any_Q_SSC_SSL fraction",
            )
        )
        for var_name in MEASURE_VARS:
            add(section, "matrix", product, f"{var_name}_nonmissing_cell_count")
            add(section, "matrix", product, f"{var_name}_coverage_pct")

    add("climatology", "climatology", "climatology", "climatology_station_count")
    add("climatology", "climatology", "climatology", "source_count")
    add("climatology", "climatology", "climatology", "record_count")
    for var_name in MEASURE_VARS:
        add("climatology", "climatology", "climatology", f"{var_name}_nonmissing_count")

    add("spatial", "geopackage_headline", "cluster_points", "cluster_point_count")
    add("spatial", "geopackage_headline", "source_stations", "source_station_point_count", notes="maximum distinct source_station_uid count across source station resolution layers")
    add("spatial", "geopackage", "source_stations", "feature_count_total_all_layers", metric="source_station_feature_count_total_all_layers")
    add("spatial", "geopackage_headline", "cluster_basins", "basin_polygon_count")

    add("satellite_validation", "satellite_validation", "satellite_validation", "satellite_station_count")
    add("satellite_validation", "satellite_validation", "satellite_validation", "satellite_record_count")
    add("satellite_validation", "satellite_validation", "satellite_validation", "source_count")
    add("satellite_validation", "catalog_counts", "satellite_validation_catalog", "satellite_validation_catalog_row_count")

    add("candidate_records", "catalog_counts", "satellite_candidates", "satellite_candidates_row_count")
    add("candidate_records", "catalog_counts", "overlap_candidates", "overlap_candidates_row_count")

    return pd.DataFrame(rows, columns=["section", "metric", "value", "unit", "source_file", "notes"])


def build_summary_markdown(summary_df: pd.DataFrame, files_df: pd.DataFrame, article_df: pd.DataFrame, out_path: Path) -> None:
    def md_cell(value: Any) -> str:
        return str(value).replace("|", r"\|")

    def value_for(group: str, product: str, metric: str) -> str:
        return str(summary_value(summary_df, group, product, metric))

    def article_value(section: str, metric: str) -> str:
        hit = article_df[(article_df["section"] == section) & (article_df["metric"] == metric)]
        if hit.empty:
            return ""
        return str(hit.iloc[0]["value"])

    def add_article_table(lines: List[str], rows: Sequence[Tuple[str, str]]) -> None:
        lines.append("| Metric | Value |")
        lines.append("|---|---:|")
        for section, metric in rows:
            label = metric.replace("_", " ")
            lines.append(f"| {md_cell(label)} | {md_cell(article_value(section, metric))} |")
        lines.append("")

    lines: List[str] = []
    lines.append("# Release inventory statistics")
    lines.append("")
    lines.append("## Release files")
    lines.append("")
    lines.append(f"- File count: {len(files_df)}")
    if not files_df.empty:
        lines.append(f"- Total size: {files_df['size_mb'].sum():.3f} MB")
    lines.append("")

    lines.append("## Article-ready key numbers")
    lines.append("")
    add_article_table(
        lines,
        [
            ("master", "cluster_count"),
            ("master", "source_station_count"),
            ("master", "observation_record_count"),
            ("master", "temporal_resolution_types"),
            ("master", "Q_nonmissing_record_count"),
            ("master", "SSC_nonmissing_record_count"),
            ("master", "SSL_nonmissing_record_count"),
            ("climatology", "climatology_station_count"),
            ("climatology", "record_count"),
            ("spatial", "cluster_point_count"),
            ("spatial", "source_station_point_count"),
            ("spatial", "basin_polygon_count"),
            ("satellite_validation", "satellite_station_count"),
            ("satellite_validation", "satellite_record_count"),
            ("candidate_records", "satellite_candidates_row_count"),
            ("candidate_records", "overlap_candidates_row_count"),
        ],
    )

    lines.append("## Abstract-ready headline metrics")
    lines.append("")
    headline_rows = [
        ("Master clusters", value_for("master", "master", "cluster_count")),
        ("Master source stations", value_for("master", "master", "source_station_count")),
        ("Master observation records", value_for("master", "master", "observation_record_count")),
        ("Master temporal resolutions", value_for("master", "master", "temporal_resolution_types")),
        ("Climatology stations", value_for("climatology", "climatology", "climatology_station_count")),
        ("Climatology sources", value_for("climatology", "climatology", "source_count")),
        ("Cluster points", value_for("geopackage_headline", "cluster_points", "cluster_point_count")),
        ("Source station points", value_for("geopackage_headline", "source_stations", "source_station_point_count")),
        ("Basin polygons", value_for("geopackage_headline", "cluster_basins", "basin_polygon_count")),
        ("Satellite validation stations", value_for("satellite_validation", "satellite_validation", "satellite_station_count")),
        ("Satellite validation records", value_for("satellite_validation", "satellite_validation", "satellite_record_count")),
    ]
    lines.append("| Metric | Value |")
    lines.append("|---|---:|")
    for key, val in headline_rows:
        lines.append(f"| {md_cell(key)} | {md_cell(val)} |")
    lines.append("")

    lines.append("## Matrix coverage")
    lines.append("")
    lines.append("| Resolution | Stations/clusters | Time steps | Non-missing cells | Any coverage % | Q coverage % | SSC coverage % | SSL coverage % |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for resolution in ("daily", "monthly", "annual"):
        product = f"{resolution}_matrix"
        lines.append(
            "| {res} | {stations} | {steps} | {cells} | {any_cov} | {q} | {ssc} | {ssl} |".format(
                res=resolution,
                stations=md_cell(value_for("matrix", product, "station_cluster_count")),
                steps=md_cell(value_for("matrix", product, "time_step_count")),
                cells=md_cell(value_for("matrix", product, "nonmissing_cell_count_any_Q_SSC_SSL")),
                any_cov=md_cell(article_value(f"matrix_{resolution}", "coverage_any_Q_SSC_SSL_pct")),
                q=md_cell(value_for("matrix", product, "Q_coverage_pct")),
                ssc=md_cell(value_for("matrix", product, "SSC_coverage_pct")),
                ssl=md_cell(value_for("matrix", product, "SSL_coverage_pct")),
            )
        )
    lines.append("")

    lines.append("## Satellite validation")
    lines.append("")
    add_article_table(
        lines,
        [
            ("satellite_validation", "satellite_station_count"),
            ("satellite_validation", "satellite_record_count"),
            ("satellite_validation", "source_count"),
            ("satellite_validation", "satellite_validation_catalog_row_count"),
        ],
    )

    lines.append("## Candidate and overlap records")
    lines.append("")
    add_article_table(
        lines,
        [
            ("candidate_records", "satellite_candidates_row_count"),
            ("candidate_records", "overlap_candidates_row_count"),
        ],
    )

    out_path.write_text("\n".join(lines), encoding="utf-8")

def project_root_from_script(script_path: Path) -> Path:
    """Infer the scripts_basin_test root when this script is stored in stats/."""
    script_dir = script_path.resolve().parent
    if script_dir.name == "stats":
        return script_dir.parent
    return script_dir


def choose_default_release_dir(script_path: Path) -> Path:
    """Infer release dir from script location first, then current working directory."""
    project_root = project_root_from_script(script_path)
    candidates = [
        project_root / "output" / "sed_reference_release",
        Path.cwd() / "output" / "sed_reference_release",
        Path.cwd() / "scripts_basin_test" / "output" / "sed_reference_release",
        script_path.resolve().parent / "output" / "sed_reference_release",
        Path.cwd(),
    ]
    for candidate in candidates:
        if (candidate / "sed_reference_master.nc").is_file():
            return candidate.resolve()
    return candidates[0].resolve()


def choose_default_out_dir(script_path: Path) -> Path:
    """Default all stats outputs to output_other next to output/."""
    return (project_root_from_script(script_path) / "output_other"/"release_inventory_stats").resolve()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate release inventory, NetCDF schema, product scale, coverage, and GPKG statistics."
    )
    parser.add_argument(
        "--release-dir",
        default=None,
        help="Path to sed_reference_release directory. Default: auto-detect output/sed_reference_release relative to stats/ or scripts_basin_test/.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Directory for output CSV/Markdown files. Default: output_other next to output/.",
    )
    parser.add_argument(
        "--prefix",
        default="release_inventory_stats",
        help="Output file prefix. Default: release_inventory_stats",
    )
    args = parser.parse_args(argv)

    script_path = Path(__file__)
    release_dir = Path(args.release_dir).expanduser().resolve() if args.release_dir else choose_default_release_dir(script_path)
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else choose_default_out_dir(script_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    if nc4 is None:
        print("Error: netCDF4 is required. Install with `pip install netCDF4`.", file=sys.stderr)
        return 2

    summary_rows: List[Dict[str, Any]] = []
    schema_rows: List[Dict[str, Any]] = []

    files_df = file_inventory(release_dir)
    files_out = out_dir / f"{args.prefix}_files.csv"
    files_df.to_csv(files_out, index=False)

    total_size_mb = float(files_df["size_mb"].sum()) if not files_df.empty else 0.0
    summary_rows.extend(
        [
            metric_row("release_dir", str(release_dir), group="release"),
            metric_row("file_count", len(files_df), group="release", unit="files"),
            metric_row("total_size_mb", round(total_size_mb, 6), group="release", unit="MB"),
            metric_row("file_names", "|".join(files_df["relative_path"].tolist()) if not files_df.empty else "", group="release"),
        ]
    )

    # Presence/status for standard release contract files.
    standard_products = OrderedDict()
    standard_products.update(NETCDF_PRODUCTS)
    standard_products.update(GPKG_PRODUCTS)
    standard_products.update(CATALOG_PRODUCTS)
    for product, file_name in standard_products.items():
        path = release_dir / file_name
        summary_rows.append(
            metric_row(
                "present",
                int(path.is_file()),
                group="release_file_presence",
                product=product,
                file_name=file_name,
                notes="standard or optional release product",
            )
        )
        if path.is_file():
            summary_rows.append(
                metric_row(
                    "size_mb",
                    round(path.stat().st_size / (1024 * 1024), 6),
                    group="release_file_presence",
                    product=product,
                    file_name=file_name,
                    unit="MB",
                )
            )

    # NetCDF schema and generic schema counts.
    for product, file_name in NETCDF_PRODUCTS.items():
        path = release_dir / file_name
        if not path.is_file():
            continue
        schema_rows.extend(netcdf_schema_rows(path, product))
        add_netcdf_file_schema_metrics(path, product, summary_rows)

    # Product-specific scale statistics.
    summary_rows.extend(summarize_master(release_dir / NETCDF_PRODUCTS["master"]))
    for resolution, file_name in MATRIX_PRODUCTS.items():
        summary_rows.extend(summarize_matrix(release_dir / file_name, resolution))
    summary_rows.extend(summarize_climatology(release_dir / NETCDF_PRODUCTS["climatology"]))
    summary_rows.extend(summarize_satellite_validation(release_dir / NETCDF_PRODUCTS["satellite_validation"]))
    summary_rows.extend(summarize_catalog_counts(release_dir))

    gpkg_summary, gpkg_layers = summarize_gpkgs(release_dir)
    summary_rows.extend(gpkg_summary)

    summary_df = pd.DataFrame(summary_rows)
    schema_df = pd.DataFrame(schema_rows)
    gpkg_layers_df = pd.DataFrame(gpkg_layers)
    article_df = build_article_metrics(summary_df, files_df)

    summary_out = out_dir / f"{args.prefix}_summary.csv"
    schema_out = out_dir / f"{args.prefix}_netcdf_schema.csv"
    gpkg_out = out_dir / f"{args.prefix}_gpkg_layers.csv"
    article_out = out_dir / f"{args.prefix}_article_metrics.csv"
    wide_out = out_dir / f"{args.prefix}_summary_wide.csv"
    markdown_out = out_dir / f"{args.prefix}_summary.md"

    summary_df.to_csv(summary_out, index=False)
    schema_df.to_csv(schema_out, index=False)
    gpkg_layers_df.to_csv(gpkg_out, index=False)
    article_df.to_csv(article_out, index=False)

    wide_values: Dict[str, Any] = {}
    for _, row in summary_df.iterrows():
        key = metric_key(row.to_dict())
        # Keep first occurrence; duplicate metric names should be rare because product is included.
        wide_values.setdefault(key, row["value"])
    pd.DataFrame([wide_values]).to_csv(wide_out, index=False)

    build_summary_markdown(summary_df, files_df, article_df, markdown_out)

    print("Release dir: {}".format(release_dir))
    print("Output dir: {}".format(out_dir))
    print("Wrote: {}".format(summary_out))
    print("Wrote: {}".format(wide_out))
    print("Wrote: {}".format(files_out))
    print("Wrote: {}".format(schema_out))
    print("Wrote: {}".format(gpkg_out))
    print("Wrote: {}".format(article_out))
    print("Wrote: {}".format(markdown_out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
