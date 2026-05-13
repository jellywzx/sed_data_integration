#!/usr/bin/env python3
"""s8 release-only validation diagnostics for sediment reference products.

This script is intentionally independent from the s6/s7/s8 build workflow.  It
reads only files inside ``--release-dir`` and writes validation diagnostics into
``--out-dir``.
"""

from __future__ import annotations

import argparse
import itertools
import math
import os
import sqlite3
import time as time_module
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


VARIABLES = ("Q", "SSC", "SSL")
DEFAULT_UNITS = {
    "Q": "m3 s-1",
    "SSC": "mg L-1",
    "SSL": "t day-1",
}
TIMESERIES_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
MASTER_FILE = "sed_reference_master.nc"
OVERLAP_CANDIDATES_FILE = "sed_reference_overlap_candidates.csv.gz"
CATALOG_FILES = (
    "station_catalog.csv",
    "source_station_catalog.csv",
    "source_dataset_catalog.csv",
    OVERLAP_CANDIDATES_FILE,
)
GPKG_FILES = (
    "sed_reference_cluster_points.gpkg",
    "sed_reference_source_stations.gpkg",
    "sed_reference_cluster_basins.gpkg",
)
TRUE_PAIR_SKIP_REASON = (
    "True source-pair overlap consistency cannot be computed from s8 release "
    "products alone because sed_reference_overlap_candidates.csv.gz is absent "
    "or does not contain multi-source candidate values."
)
RELEASE_ONLY_NOTES = (
    "release-only validation; no s6/s7 intermediate files, source station NC, "
    "or raw source datasets were read"
)
RELEASE_ONLY_ASSUMPTIONS = (
    "s8 release products are treated as the only allowed inputs"
)
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RELEASE_DIR = SCRIPT_DIR / "output" / "sed_reference_release"
DEFAULT_OUT_DIR = SCRIPT_DIR / "output" / "validation_results"
# Default runtime configuration.  Edit these constants if a different default
# release/output path or worker count is needed; no CLI arguments are required
# for the standard workflow.
DEFAULT_WORKERS = max(1, min(24, os.cpu_count() or 1))
OVERLAP_CANDIDATE_REQUIRED_COLUMNS = {
    "cluster_uid",
    "cluster_id",
    "resolution",
    "time",
    "date",
    "source",
    "source_family",
    "source_station_uid",
    "source_station_index",
    "candidate_path",
    "candidate_rank",
    "candidate_quality_score",
    "selected_flag",
    "is_overlap",
    "n_candidates_at_time",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
    "method_notes",
    "assumptions",
}


def log_progress(message: str) -> None:
    """Print progress immediately so long release scans are not silent."""
    stamp = time_module.strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] {}".format(stamp, message), flush=True)


def classify_source_family(source: str) -> str:
    """Classify a source name into the lightweight validation taxonomy."""
    text = "" if source is None else str(source)
    low = text.lower()
    if "usgs" in low:
        return "USGS"
    if "hydat" in low:
        return "HYDAT"
    if any(token in low for token in ("riversed", "gsed", "dethier", "aquasat")):
        return "satellite"
    if any(token in low for token in ("grdc", "hybam", "in situ", "insitu")):
        return "in_situ"
    if any(token in low for token in ("compiled", "compilation", "secondary")):
        return "secondary_compilation"
    return "other"


def _clean_pair_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def canonical_source_pair(source_a, source_b) -> Tuple[str, str, str]:
    """Return a stable ordered source pair as ``(a, b, "a vs b")``."""
    first, second = sorted(
        (_clean_pair_value(source_a), _clean_pair_value(source_b)),
        key=lambda value: (value.lower(), value),
    )
    return first, second, "{} vs {}".format(first, second)


def canonical_family_pair(family_a, family_b) -> Tuple[str, str, str]:
    """Return a stable ordered source-family pair as ``(a, b, "a vs b")``."""
    first, second = sorted(
        (_clean_pair_value(family_a), _clean_pair_value(family_b)),
        key=lambda value: (value.lower(), value),
    )
    return first, second, "{} vs {}".format(first, second)


def _to_float_array(values) -> np.ndarray:
    series = pd.Series(values)
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)


def _safe_corr(a: np.ndarray, b: np.ndarray, method: str) -> float:
    if len(a) < 2:
        return float("nan")
    if np.nanstd(a) == 0 or np.nanstd(b) == 0:
        return float("nan")
    if method == "spearman":
        left = pd.Series(a).rank(method="average")
        right = pd.Series(b).rank(method="average")
        return float(left.corr(right, method="pearson"))
    return float(pd.Series(a).corr(pd.Series(b), method="pearson"))


def compute_pair_metrics(ref, cmp) -> Dict[str, float]:
    """Compute pairwise consistency metrics for two numeric arrays.

    ``bias`` is defined as ``cmp - ref``.  Missing values in either array are
    skipped.  MAPE skips records where ``ref`` is zero.
    """
    ref_arr = _to_float_array(ref)
    cmp_arr = _to_float_array(cmp)
    if len(ref_arr) != len(cmp_arr):
        raise ValueError("ref and cmp must have the same length")

    valid = np.isfinite(ref_arr) & np.isfinite(cmp_arr)
    ref_valid = ref_arr[valid]
    cmp_valid = cmp_arr[valid]
    n_pairs = int(len(ref_valid))
    if n_pairs == 0:
        return {
            "bias": float("nan"),
            "RMSE": float("nan"),
            "MAE": float("nan"),
            "MAPE": float("nan"),
            "n_valid_mape": 0,
            "Pearson correlation": float("nan"),
            "Spearman rank correlation": float("nan"),
            "n_pairs": 0,
        }

    diff = cmp_valid - ref_valid
    mape_mask = ref_valid != 0
    n_valid_mape = int(np.count_nonzero(mape_mask))
    mape = (
        float(np.nanmean(np.abs(diff[mape_mask] / ref_valid[mape_mask]) * 100.0))
        if n_valid_mape
        else float("nan")
    )
    return {
        "bias": float(np.nanmean(diff)),
        "RMSE": float(np.sqrt(np.nanmean(diff ** 2))),
        "MAE": float(np.nanmean(np.abs(diff))),
        "MAPE": mape,
        "n_valid_mape": n_valid_mape,
        "Pearson correlation": _safe_corr(ref_valid, cmp_valid, "pearson"),
        "Spearman rank correlation": _safe_corr(ref_valid, cmp_valid, "spearman"),
        "n_pairs": n_pairs,
    }


def build_overlap_availability_diagnostic(
    schema_inventory: pd.DataFrame,
    supports_candidate_values: bool,
    supports_source_pair_metrics: bool,
    reason: Optional[str] = None,
) -> pd.DataFrame:
    """Build release-product availability diagnostics for overlap validation."""
    if reason is None:
        reason = (
            "candidate-level source values are present in s8 release products"
            if supports_source_pair_metrics
            else TRUE_PAIR_SKIP_REASON
        )
    rows = []
    key_names = {
        "cluster_uid",
        "cluster_id",
        "resolution",
        "time",
        "date",
        "source",
        "source_family",
        "source_station_uid",
        "candidate_rank",
        "candidate_quality_score",
        "selected_flag",
        "is_overlap",
        "n_candidates_at_time",
        "Q",
        "SSC",
        "SSL",
    }
    if schema_inventory is not None and not schema_inventory.empty:
        work = schema_inventory.copy()
        names = work.get("dimension_or_column_or_variable", pd.Series(dtype=str)).astype(str)
        mask = names.isin(key_names) | names.str.replace("global_attr:", "", regex=False).isin(key_names)
        selected = work[mask].copy()
        for _, row in selected.iterrows():
            rows.append(
                {
                    "file_name": row.get("file_name", ""),
                    "variable_or_column": row.get("dimension_or_column_or_variable", ""),
                    "supports_candidate_values": bool(supports_candidate_values),
                    "supports_source_pair_metrics": bool(supports_source_pair_metrics),
                    "reason": reason,
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )
    if not rows:
        rows.append(
            {
                "file_name": "",
                "variable_or_column": "",
                "supports_candidate_values": bool(supports_candidate_values),
                "supports_source_pair_metrics": bool(supports_source_pair_metrics),
                "reason": reason,
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return pd.DataFrame(rows)


def _standard_csv_columns(df: pd.DataFrame, required: Sequence[str]) -> pd.DataFrame:
    work = df.copy()
    for col in required:
        if col not in work.columns:
            work[col] = np.nan
    if "method_notes" not in work.columns:
        work["method_notes"] = RELEASE_ONLY_NOTES
    if "assumptions" not in work.columns:
        work["assumptions"] = RELEASE_ONLY_ASSUMPTIONS
    return work[list(required)]


def _import_xarray():
    try:
        import xarray as xr  # type: ignore

        return xr
    except Exception:
        return None


def _decode_value_array(values) -> List[str]:
    arr = np.asarray(values)
    if arr.ndim == 0:
        arr = arr.reshape(1)
    if arr.dtype.kind == "S":
        return [item.decode("utf-8", errors="ignore").strip() for item in arr.reshape(-1)]
    if arr.dtype.kind == "U":
        return [str(item).strip() for item in arr.reshape(-1)]
    if arr.dtype.kind in ("O",):
        out = []
        for item in arr.reshape(-1):
            if isinstance(item, bytes):
                out.append(item.decode("utf-8", errors="ignore").strip())
            else:
                out.append("" if pd.isna(item) else str(item).strip())
        return out
    return ["" if pd.isna(item) else str(item).strip() for item in arr.reshape(-1)]


def _series_from_data_array(da) -> pd.Series:
    values = np.asarray(da.values)
    if values.dtype.kind in ("S", "U", "O"):
        if values.ndim == 2:
            joined = []
            for row in values:
                joined.append("".join(_decode_value_array(row)).strip())
            return pd.Series(joined)
        return pd.Series(_decode_value_array(values))
    return pd.Series(values.reshape(-1))


def _format_time_values(values, units: str = "") -> pd.Series:
    raw = pd.Series(values)
    if np.issubdtype(raw.dtype, np.datetime64):
        return pd.to_datetime(raw, errors="coerce").dt.strftime("%Y-%m-%d").fillna(raw.astype(str))
    numeric = pd.to_numeric(raw, errors="coerce")
    if numeric.notna().any() and " since " in str(units):
        unit_name, origin = str(units).split(" since ", 1)
        unit_name = unit_name.strip().lower()
        origin = origin.strip().split(" ")[0]
        unit_map = {
            "day": "D",
            "days": "D",
            "hour": "h",
            "hours": "h",
            "second": "s",
            "seconds": "s",
        }
        pandas_unit = unit_map.get(unit_name)
        if pandas_unit:
            try:
                base = pd.to_datetime(origin, errors="coerce")
                formatted = base + pd.to_timedelta(numeric, unit=pandas_unit)
                return formatted.dt.strftime("%Y-%m-%d").fillna(raw.astype(str))
            except Exception:
                pass
    return raw.astype(str)


def _has_missing_values(values) -> bool:
    series = pd.Series(values)
    if series.dtype.kind in ("O", "S", "U"):
        text = series.astype(str).str.strip()
        return bool(series.isna().any() or text.eq("").any() or text.str.lower().eq("nan").any())
    return bool(pd.isna(series).any())


def _nc_has_missing(da):
    # Keep schema inspection metadata-only.  Reading large NetCDF variables just
    # to discover missingness dominates runtime for release products.
    if any(key in da.attrs for key in ("_FillValue", "missing_value")):
        return "not_scanned_fill_value_metadata_present"
    return "not_scanned_fast_schema_inspection"


def inspect_netcdf(path: Path) -> Tuple[List[Dict[str, object]], Dict[str, str]]:
    rows: List[Dict[str, object]] = []
    units: Dict[str, str] = {}
    xr = _import_xarray()
    if xr is None:
        rows.append(
            {
                "file_name": path.name,
                "object_type": "netcdf_file",
                "dimension_or_column_or_variable": "",
                "dtype": "",
                "shape_or_count": "",
                "has_missing_values": "",
                "notes": "skipped: xarray not available",
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
        return rows, units

    try:
        ds = xr.open_dataset(path, decode_times=False, mask_and_scale=True)
    except Exception as exc:
        rows.append(
            {
                "file_name": path.name,
                "object_type": "netcdf_file",
                "dimension_or_column_or_variable": "",
                "dtype": "",
                "shape_or_count": "",
                "has_missing_values": "",
                "notes": "skipped: cannot open NetCDF: {}".format(exc),
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
        return rows, units

    try:
        for dim_name, dim_len in ds.sizes.items():
            rows.append(
                {
                    "file_name": path.name,
                    "object_type": "netcdf_dimension",
                    "dimension_or_column_or_variable": dim_name,
                    "dtype": "dimension",
                    "shape_or_count": int(dim_len),
                    "has_missing_values": "",
                    "notes": "",
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )
        for attr_name, attr_value in ds.attrs.items():
            rows.append(
                {
                    "file_name": path.name,
                    "object_type": "netcdf_global_attribute",
                    "dimension_or_column_or_variable": "global_attr:{}".format(attr_name),
                    "dtype": type(attr_value).__name__,
                    "shape_or_count": "",
                    "has_missing_values": "",
                    "notes": str(attr_value)[:200],
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )
        for var_name, da in ds.variables.items():
            if var_name in VARIABLES:
                unit = str(da.attrs.get("units", "")).strip()
                if unit:
                    units[var_name] = unit
            rows.append(
                {
                    "file_name": path.name,
                    "object_type": "netcdf_variable",
                    "dimension_or_column_or_variable": var_name,
                    "dtype": str(da.dtype),
                    "shape_or_count": "x".join(str(int(ds.sizes[d])) for d in da.dims),
                    "has_missing_values": _nc_has_missing(da),
                    "notes": "dims={}; attrs={}".format(
                        ",".join(da.dims),
                        ",".join(sorted(str(key) for key in da.attrs.keys())),
                    ),
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )
    finally:
        ds.close()
    return rows, units


def inspect_csv(path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    try:
        df = pd.read_csv(path, keep_default_na=False, nrows=0)
    except Exception as exc:
        return [
            {
                "file_name": path.name,
                "object_type": "csv_file",
                "dimension_or_column_or_variable": "",
                "dtype": "",
                "shape_or_count": "",
                "has_missing_values": "",
                "notes": "skipped: cannot read CSV: {}".format(exc),
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        ]
    for col in df.columns:
        rows.append(
            {
                "file_name": path.name,
                "object_type": "csv_column",
                "dimension_or_column_or_variable": col,
                "dtype": str(df[col].dtype),
                "shape_or_count": "not_counted_fast_schema_inspection",
                "has_missing_values": "not_scanned_fast_schema_inspection",
                "notes": "columns read with nrows=0 for fast schema inspection",
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return rows


def inspect_gpkg(path: Path) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    try:
        conn = sqlite3.connect("file:{}?mode=ro".format(path), uri=True)
    except Exception as exc:
        return [
            {
                "file_name": path.name,
                "object_type": "gpkg_sidecar",
                "dimension_or_column_or_variable": "",
                "dtype": "",
                "shape_or_count": "",
                "has_missing_values": "",
                "notes": "skipped: cannot open GPKG as SQLite metadata: {}".format(exc),
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        ]

    try:
        layer_rows = conn.execute(
            "SELECT table_name, data_type FROM gpkg_contents ORDER BY table_name"
        ).fetchall()
    except Exception as exc:
        conn.close()
        return [
            {
                "file_name": path.name,
                "object_type": "gpkg_sidecar",
                "dimension_or_column_or_variable": "",
                "dtype": "",
                "shape_or_count": "",
                "has_missing_values": "",
                "notes": "skipped: cannot read gpkg_contents metadata: {}".format(exc),
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        ]

    try:
        geometry_lookup = {}
        try:
            geometry_lookup = {
                str(table_name): str(geometry_type_name)
                for table_name, geometry_type_name in conn.execute(
                    "SELECT table_name, geometry_type_name FROM gpkg_geometry_columns"
                ).fetchall()
            }
        except Exception:
            geometry_lookup = {}

        for layer, data_type in layer_rows:
            layer = str(layer)
            rows.append(
                {
                    "file_name": path.name,
                    "object_type": "gpkg_layer",
                    "dimension_or_column_or_variable": layer,
                    "dtype": geometry_lookup.get(layer, str(data_type)),
                    "shape_or_count": "not_counted_fast_sqlite_schema_inspection",
                    "has_missing_values": "",
                    "notes": "metadata read from gpkg_contents; geometry rows were not scanned",
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )
            try:
                field_rows = conn.execute('PRAGMA table_info("{}")'.format(layer.replace('"', '""'))).fetchall()
            except Exception as exc:
                rows.append(
                    {
                        "file_name": path.name,
                        "object_type": "gpkg_field",
                        "dimension_or_column_or_variable": layer,
                        "dtype": "",
                        "shape_or_count": "",
                        "has_missing_values": "",
                        "notes": "skipped: cannot inspect fields from SQLite metadata: {}".format(exc),
                        "method_notes": RELEASE_ONLY_NOTES,
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                )
                continue
            for _, field_name, field_type, _, _, _ in field_rows:
                rows.append(
                    {
                        "file_name": path.name,
                        "object_type": "gpkg_field",
                        "dimension_or_column_or_variable": str(field_name),
                        "dtype": str(field_type),
                        "shape_or_count": "not_counted_fast_sqlite_schema_inspection",
                        "has_missing_values": "not_checked_gpkg_field",
                        "notes": "layer={}; metadata read via PRAGMA table_info".format(layer),
                        "method_notes": RELEASE_ONLY_NOTES,
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                )
    finally:
        conn.close()
    return rows


def _inspect_release_file_task(kind: str, path_text: str) -> Tuple[List[Dict[str, object]], Dict[str, str]]:
    path = Path(path_text)
    if kind == "netcdf":
        return inspect_netcdf(path)
    if kind == "csv":
        return inspect_csv(path), {}
    if kind == "gpkg":
        return inspect_gpkg(path), {}
    return [
        {
            "file_name": path.name,
            "object_type": "release_file",
            "dimension_or_column_or_variable": "",
            "dtype": "",
            "shape_or_count": "",
            "has_missing_values": "",
            "notes": "skipped: unknown inspection kind {}".format(kind),
            "method_notes": RELEASE_ONLY_NOTES,
            "assumptions": RELEASE_ONLY_ASSUMPTIONS,
        }
    ], {}


def _run_schema_inspection_tasks(
    tasks: Sequence[Tuple[str, Path]],
    workers: int,
    progress: Optional[Callable[[str], None]] = None,
) -> Tuple[List[Dict[str, object]], Dict[str, str]]:
    rows: List[Dict[str, object]] = []
    units: Dict[str, str] = {}
    if not tasks:
        return rows, units
    if workers <= 1 or len(tasks) == 1:
        for idx, (kind, path) in enumerate(tasks, start=1):
            if progress:
                progress("Inspecting schema file {}/{}: {}".format(idx, len(tasks), path.name))
            task_rows, task_units = _inspect_release_file_task(kind, str(path))
            rows.extend(task_rows)
            units.update(task_units)
        return rows, units

    max_workers = max(1, min(int(workers), len(tasks)))
    if progress:
        progress("Inspecting {} release schema files with {} workers".format(len(tasks), max_workers))
        progress("Schema files queued: {}".format(", ".join(path.name for _, path in tasks)))
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_inspect_release_file_task, kind, str(path)): (kind, path)
            for kind, path in tasks
        }
        pending_names = {path.name for _, path in tasks}
        for idx, future in enumerate(as_completed(future_map), start=1):
            kind, path = future_map[future]
            try:
                task_rows, task_units = future.result()
            except Exception as exc:
                task_rows, task_units = [
                    {
                        "file_name": path.name,
                        "object_type": "{}_file".format(kind),
                        "dimension_or_column_or_variable": "",
                        "dtype": "",
                        "shape_or_count": "",
                        "has_missing_values": "",
                        "notes": "skipped: parallel inspection failed: {}".format(exc),
                        "method_notes": RELEASE_ONLY_NOTES,
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                ], {}
            rows.extend(task_rows)
            units.update(task_units)
            pending_names.discard(path.name)
            if progress:
                remaining = ", ".join(sorted(pending_names)) if pending_names else "none"
                progress(
                    "Finished schema file {}/{}: {}; remaining: {}".format(
                        idx,
                        len(tasks),
                        path.name,
                        remaining,
                    )
                )
    return rows, units


def inspect_release_schema(
    release_dir: Path,
    workers: int = DEFAULT_WORKERS,
    progress: Optional[Callable[[str], None]] = None,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    rows: List[Dict[str, object]] = []
    units: Dict[str, str] = {}
    tasks: List[Tuple[str, Path]] = []

    for name in (MASTER_FILE,) + tuple(TIMESERIES_FILES.values()):
        path = release_dir / name
        if path.exists():
            tasks.append(("netcdf", path))
        else:
            rows.append(
                {
                    "file_name": name,
                    "object_type": "expected_release_file",
                    "dimension_or_column_or_variable": "",
                    "dtype": "",
                    "shape_or_count": "",
                    "has_missing_values": "",
                    "notes": "missing optional/expected release file",
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )

    for name in CATALOG_FILES:
        path = release_dir / name
        if path.exists():
            tasks.append(("csv", path))
        else:
            rows.append(
                {
                    "file_name": name,
                    "object_type": "expected_release_file",
                    "dimension_or_column_or_variable": "",
                    "dtype": "",
                    "shape_or_count": "",
                    "has_missing_values": "",
                    "notes": "missing optional/expected release file",
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )

    inspected_gpkg = set()
    for name in GPKG_FILES:
        path = release_dir / name
        if path.exists():
            tasks.append(("gpkg", path))
            inspected_gpkg.add(name)
        else:
            rows.append(
                {
                    "file_name": name,
                    "object_type": "expected_release_file",
                    "dimension_or_column_or_variable": "",
                    "dtype": "",
                    "shape_or_count": "",
                    "has_missing_values": "",
                    "notes": "missing optional/expected GPKG sidecar",
                    "method_notes": RELEASE_ONLY_NOTES,
                    "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                }
            )
    for path in sorted(release_dir.glob("*.gpkg")):
        if path.name not in inspected_gpkg:
            tasks.append(("gpkg", path))

    task_rows, task_units = _run_schema_inspection_tasks(tasks, workers=workers, progress=progress)
    rows.extend(task_rows)
    units.update(task_units)

    inventory = pd.DataFrame(rows)
    inventory = _standard_csv_columns(
        inventory,
        [
            "file_name",
            "object_type",
            "dimension_or_column_or_variable",
            "dtype",
            "shape_or_count",
            "has_missing_values",
            "notes",
            "method_notes",
            "assumptions",
        ],
    )
    if not inventory.empty:
        inventory = inventory.sort_values(
            ["file_name", "object_type", "dimension_or_column_or_variable"],
            kind="mergesort",
        ).reset_index(drop=True)
    return inventory, units


def _first_existing(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    column_set = {str(col) for col in columns}
    for name in candidates:
        if name in column_set:
            return name
    lower = {str(col).lower(): str(col) for col in columns}
    for name in candidates:
        if name.lower() in lower:
            return lower[name.lower()]
    return None


def _load_source_station_catalog(release_dir: Path) -> pd.DataFrame:
    path = release_dir / "source_station_catalog.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        header = pd.read_csv(path, keep_default_na=False, nrows=0)
        wanted = {
            "source_station_uid",
            "station_uid",
            "source",
            "source_name",
            "source_dataset",
            "dataset",
            "dataset_name",
            "source_family",
            "source_type",
            "source_category",
        }
        usecols = [col for col in header.columns if col in wanted]
        if not usecols:
            return pd.DataFrame()
        return pd.read_csv(path, keep_default_na=False, usecols=usecols)
    except Exception:
        return pd.DataFrame()


def _find_record_dim(ds) -> Optional[str]:
    if "n_records" in ds.sizes:
        return "n_records"
    for var_name in VARIABLES + ("time", "resolution", "is_overlap"):
        if var_name in ds.variables:
            dims = tuple(ds[var_name].dims)
            if len(dims) == 1:
                return dims[0]
    one_dim_vars = [tuple(da.dims)[0] for da in ds.variables.values() if len(tuple(da.dims)) == 1]
    if one_dim_vars:
        counts = pd.Series(one_dim_vars).value_counts()
        return str(counts.index[0])
    return None


def _record_series(ds, name: str, record_dim: str) -> Optional[pd.Series]:
    if name not in ds.variables:
        return None
    da = ds[name]
    if record_dim not in da.dims:
        return None
    if len(da.dims) == 1:
        series = _series_from_data_array(da)
        return series.reset_index(drop=True)
    if len(da.dims) == 2 and da.dims[0] == record_dim:
        values = np.asarray(da.values)
        if values.dtype.kind in ("S", "U"):
            joined = []
            for row in values:
                chars = _decode_value_array(row)
                joined.append("".join(chars).strip())
            return pd.Series(joined)
    return None


def _indexed_lookup_series(ds, value_name: str, index_series: pd.Series) -> Optional[pd.Series]:
    if value_name not in ds.variables:
        return None
    values = _series_from_data_array(ds[value_name]).reset_index(drop=True)
    idx = pd.to_numeric(index_series, errors="coerce")
    out = []
    for value in idx:
        if pd.isna(value):
            out.append("")
            continue
        integer = int(value)
        if 0 <= integer < len(values):
            out.append(values.iloc[integer])
        else:
            out.append("")
    return pd.Series(out)


def _load_master_records(
    release_dir: Path,
    include_values: bool = False,
    progress: Optional[Callable[[str], None]] = None,
) -> Tuple[pd.DataFrame, str]:
    path = release_dir / MASTER_FILE
    if not path.exists():
        return pd.DataFrame(), "sed_reference_master.nc not found in release-dir"
    xr = _import_xarray()
    if xr is None:
        return pd.DataFrame(), "xarray not available for master NetCDF reading"
    try:
        ds = xr.open_dataset(path, decode_times=False, mask_and_scale=True)
    except Exception as exc:
        return pd.DataFrame(), "cannot open master NetCDF: {}".format(exc)

    try:
        record_dim = _find_record_dim(ds)
        if record_dim is None:
            return pd.DataFrame(), "record dimension could not be inferred from master NetCDF"
        n_records = int(ds.sizes[record_dim])
        if progress:
            value_mode = "including Q/SSC/SSL values" if include_values else "metadata/provenance only"
            progress("Reading master records: {} rows ({})".format(n_records, value_mode))
        records = pd.DataFrame({"record_index": np.arange(n_records)})

        if include_values:
            for var in VARIABLES:
                if progress:
                    progress("Reading master variable {}".format(var))
                series = _record_series(ds, var, record_dim)
                if series is not None and len(series) == n_records:
                    records[var] = pd.to_numeric(series, errors="coerce")

        for name in ("resolution", "cluster_uid", "cluster_id", "source", "source_family", "source_station_uid", "is_overlap"):
            if progress:
                progress("Reading master provenance field {}".format(name))
            series = _record_series(ds, name, record_dim)
            if series is not None and len(series) == n_records:
                records[name] = series

        if progress:
            progress("Reading master time field without per-record date formatting")
        time_series = _record_series(ds, "time", record_dim)
        if time_series is not None and len(time_series) == n_records:
            records["time"] = time_series
        if progress:
            progress("Reading master date field")
        date_series = _record_series(ds, "date", record_dim)
        if date_series is not None and len(date_series) == n_records:
            records["date"] = date_series.astype(str)

        station_index = None
        for index_name in ("station_index", "master_station_index", "cluster_index"):
            if progress:
                progress("Checking master cluster index field {}".format(index_name))
            candidate = _record_series(ds, index_name, record_dim)
            if candidate is not None and len(candidate) == n_records:
                station_index = candidate
                records[index_name] = candidate
                break
        if "cluster_uid" not in records.columns and station_index is not None:
            if progress:
                progress("Resolving cluster_uid from indexed lookup")
            cluster_lookup = _indexed_lookup_series(ds, "cluster_uid", station_index)
            if cluster_lookup is not None:
                records["cluster_uid"] = cluster_lookup
        if "cluster_id" not in records.columns and station_index is not None:
            if progress:
                progress("Resolving cluster_id from indexed lookup")
            cluster_lookup = _indexed_lookup_series(ds, "cluster_id", station_index)
            if cluster_lookup is not None:
                records["cluster_id"] = cluster_lookup

        source_index = None
        for index_name in ("source_station_index", "selected_source_index", "source_index"):
            if progress:
                progress("Checking master source index field {}".format(index_name))
            candidate = _record_series(ds, index_name, record_dim)
            if candidate is not None and len(candidate) == n_records:
                source_index = candidate
                records[index_name] = candidate
                break
        if "source_station_uid" not in records.columns and source_index is not None:
            if progress:
                progress("Resolving source_station_uid from indexed lookup")
            source_lookup = _indexed_lookup_series(ds, "source_station_uid", source_index)
            if source_lookup is not None:
                records["source_station_uid"] = source_lookup
    finally:
        ds.close()

    catalog = _load_source_station_catalog(release_dir)
    if not catalog.empty:
        if progress:
            progress("Joining source_station_catalog provenance fields")
        uid_col = _first_existing(catalog.columns, ("source_station_uid", "station_uid"))
        source_col = _first_existing(
            catalog.columns,
            ("source", "source_name", "source_dataset", "dataset", "dataset_name"),
        )
        family_col = _first_existing(catalog.columns, ("source_family", "source_type", "source_category"))
        if uid_col and "source_station_uid" in records.columns:
            merge_cols = [uid_col]
            extra_cols = []
            if source_col:
                extra_cols.append(source_col)
            if family_col:
                extra_cols.append(family_col)
            if extra_cols:
                lookup = catalog[merge_cols + extra_cols].drop_duplicates(uid_col)
                records = records.merge(
                    lookup,
                    how="left",
                    left_on="source_station_uid",
                    right_on=uid_col,
                    suffixes=("", "_catalog"),
                )
                if source_col and "source" not in records.columns:
                    records["source"] = records[source_col]
                if family_col and "source_family" not in records.columns:
                    records["source_family"] = records[family_col]
        if "source" not in records.columns and source_col and source_col in catalog.columns:
            # Last-resort catalog summary fallback cannot assign records, so leave
            # selected-source summaries empty rather than guessing.
            pass

    if "source" not in records.columns and "source_station_uid" in records.columns:
        records["source"] = records["source_station_uid"].astype(str)
    if "source_family" not in records.columns:
        source_for_family = records["source"] if "source" in records.columns else pd.Series([""] * len(records))
        records["source_family"] = source_for_family.map(classify_source_family)
    else:
        source_for_family = records["source"] if "source" in records.columns else pd.Series([""] * len(records))
        family = records["source_family"].astype(str)
        missing_family = family.str.strip().eq("") | family.str.lower().eq("nan")
        records.loc[missing_family, "source_family"] = source_for_family[missing_family].map(classify_source_family)
    if "date" not in records.columns and "time" in records.columns:
        records["date"] = records["time"]

    value_note = "with Q/SSC/SSL values" if include_values else "without Q/SSC/SSL values"
    return records, "master release records loaded {}".format(value_note)


def _load_overlap_candidates(release_dir: Path, progress: Optional[Callable[[str], None]] = None) -> Tuple[pd.DataFrame, str]:
    path = release_dir / OVERLAP_CANDIDATES_FILE
    if not path.exists():
        return pd.DataFrame(), "{} not found in release-dir".format(OVERLAP_CANDIDATES_FILE)
    try:
        header = pd.read_csv(path, keep_default_na=False, nrows=0)
    except Exception as exc:
        return pd.DataFrame(), "cannot read {} header: {}".format(OVERLAP_CANDIDATES_FILE, exc)
    missing = sorted(OVERLAP_CANDIDATE_REQUIRED_COLUMNS - set(header.columns))
    if missing:
        return pd.DataFrame(), "{} missing required columns: {}".format(
            OVERLAP_CANDIDATES_FILE,
            ", ".join(missing),
        )
    if progress:
        progress("Reading candidate-level overlap sidecar {}".format(path.name))
    try:
        candidates = pd.read_csv(path, keep_default_na=False)
    except Exception as exc:
        return pd.DataFrame(), "cannot read {}: {}".format(OVERLAP_CANDIDATES_FILE, exc)
    for var in VARIABLES:
        candidates[var] = pd.to_numeric(candidates[var], errors="coerce")
    for col in ("time", "candidate_quality_score", "selected_flag", "is_overlap", "n_candidates_at_time"):
        if col in candidates.columns:
            candidates[col] = pd.to_numeric(candidates[col], errors="coerce")
    for col in ("source", "source_family", "source_station_uid", "resolution", "date"):
        if col not in candidates.columns:
            candidates[col] = ""
        candidates[col] = candidates[col].astype(str)
    missing_family = candidates["source_family"].str.strip().eq("") | candidates["source_family"].str.lower().eq("nan")
    candidates.loc[missing_family, "source_family"] = candidates.loc[missing_family, "source"].map(classify_source_family)
    if "date" not in candidates.columns and "time" in candidates.columns:
        candidates["date"] = candidates["time"].astype(str)
    return candidates, "{} rows loaded from {}".format(len(candidates), OVERLAP_CANDIDATES_FILE)


def _cluster_key(records: pd.DataFrame) -> Optional[str]:
    for key in ("cluster_uid", "cluster_id"):
        if key in records.columns and records[key].astype(str).str.strip().ne("").any():
            return key
    return None


def _has_multi_source_cluster_time(records: pd.DataFrame) -> bool:
    if records.empty:
        return False
    cluster_col = _cluster_key(records)
    required = [cluster_col, "resolution", "source", "source_family"] if cluster_col else []
    time_col = "time" if "time" in records.columns else ("date" if "date" in records.columns else None)
    if not cluster_col or time_col is None:
        return False
    for col in required + [time_col]:
        if col not in records.columns:
            return False

    work = records[[cluster_col, "resolution", time_col, "source", "source_family"]].copy()
    work["_source_key"] = (
        work["source_family"].astype(str).str.strip()
        + "\t"
        + work["source"].astype(str).str.strip()
    )
    source_counts = work.groupby([cluster_col, "resolution", time_col], dropna=False)["_source_key"].nunique()
    return bool((source_counts >= 2).any())


def release_supports_candidate_pairs(records: pd.DataFrame) -> Tuple[bool, str]:
    if not _has_multi_source_cluster_time(records):
        return False, TRUE_PAIR_SKIP_REASON
    cluster_col = _cluster_key(records)
    time_col = "time" if "time" in records.columns else ("date" if "date" in records.columns else None)
    value_cols = [var for var in VARIABLES if var in records.columns]
    if not value_cols:
        return False, TRUE_PAIR_SKIP_REASON

    work = records.copy()
    work["_source_key"] = (
        work["source_family"].astype(str).str.strip()
        + "\t"
        + work["source"].astype(str).str.strip()
    )
    for _, group in work.groupby([cluster_col, "resolution", time_col], dropna=False):
        if group["_source_key"].nunique(dropna=True) < 2:
            continue
        for var in value_cols:
            valid_value = pd.to_numeric(group[var], errors="coerce").notna()
            if group.loc[valid_value, "_source_key"].nunique(dropna=True) >= 2:
                return True, "candidate-level multi-source values are present in s8 release products"
    return False, TRUE_PAIR_SKIP_REASON


def build_overlap_pair_records(records: pd.DataFrame) -> pd.DataFrame:
    cluster_col = _cluster_key(records)
    time_col = "time" if "time" in records.columns else ("date" if "date" in records.columns else None)
    if cluster_col is None or time_col is None:
        return pd.DataFrame()
    value_cols = [var for var in VARIABLES if var in records.columns]
    if not value_cols:
        return pd.DataFrame()

    rows: List[Dict[str, object]] = []
    group_cols = [cluster_col, "resolution", time_col]
    work = records.copy()
    for _, group in work.groupby(group_cols, dropna=False):
        group = group.copy()
        if "source_station_uid" not in group.columns:
            group["source_station_uid"] = ""
        group["_sort_key"] = list(
            zip(
                group["source_family"].astype(str),
                group["source"].astype(str),
                group["source_station_uid"].astype(str),
            )
        )
        group = group.sort_values("_sort_key").reset_index(drop=True)
        for i, j in itertools.combinations(range(len(group)), 2):
            left = group.iloc[i]
            right = group.iloc[j]
            left_key = (
                str(left.get("source_family", "")),
                str(left.get("source", "")),
            )
            right_key = (
                str(right.get("source_family", "")),
                str(right.get("source", "")),
            )
            if left_key == right_key:
                continue
            a = left
            b = right
            source_pair = "{} vs {}".format(str(a.get("source", "")).strip(), str(b.get("source", "")).strip())
            family_pair = "{} vs {}".format(
                str(a.get("source_family", "")).strip(),
                str(b.get("source_family", "")).strip(),
            )
            for var in value_cols:
                value_a = pd.to_numeric(pd.Series([a.get(var, np.nan)]), errors="coerce").iloc[0]
                value_b = pd.to_numeric(pd.Series([b.get(var, np.nan)]), errors="coerce").iloc[0]
                if pd.isna(value_a) or pd.isna(value_b):
                    continue
                diff = float(value_b) - float(value_a)
                pct_error = float(abs(diff / float(value_a)) * 100.0) if float(value_a) != 0 else float("nan")
                rows.append(
                    {
                        "cluster_uid": a.get("cluster_uid", b.get("cluster_uid", "")),
                        "cluster_id": a.get("cluster_id", b.get("cluster_id", "")),
                        "resolution": a.get("resolution", b.get("resolution", "")),
                        "time": a.get("time", b.get("time", "")),
                        "date": a.get("date", b.get("date", "")),
                        "variable": var,
                        "source_a": a.get("source", ""),
                        "source_b": b.get("source", ""),
                        "source_family_a": a.get("source_family", ""),
                        "source_family_b": b.get("source_family", ""),
                        "source_pair": source_pair,
                        "source_family_pair": family_pair,
                        "source_station_uid_a": a.get("source_station_uid", ""),
                        "source_station_uid_b": b.get("source_station_uid", ""),
                        "value_a": float(value_a),
                        "value_b": float(value_b),
                        "diff_b_minus_a": diff,
                        "abs_diff": abs(diff),
                        "pct_error": pct_error,
                        "method_notes": "bias is defined as source_b - source_a; {}".format(RELEASE_ONLY_NOTES),
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                )
    columns = [
        "cluster_uid",
        "cluster_id",
        "resolution",
        "time",
        "date",
        "variable",
        "source_a",
        "source_b",
        "source_family_a",
        "source_family_b",
        "source_pair",
        "source_family_pair",
        "source_station_uid_a",
        "source_station_uid_b",
        "value_a",
        "value_b",
        "diff_b_minus_a",
        "abs_diff",
        "pct_error",
        "method_notes",
        "assumptions",
    ]
    return _standard_csv_columns(pd.DataFrame(rows), columns)


def aggregate_pair_metrics(pair_records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "source_pair",
        "source_family_pair",
        "resolution",
        "variable",
        "bias",
        "RMSE",
        "MAE",
        "MAPE",
        "n_valid_mape",
        "Pearson correlation",
        "Spearman rank correlation",
        "n_pairs",
        "n_clusters",
        "time_start",
        "time_end",
        "method_notes",
        "assumptions",
    ]
    if pair_records.empty:
        return _standard_csv_columns(pd.DataFrame(), columns)
    rows = []
    for keys, group in pair_records.groupby(["source_pair", "source_family_pair", "resolution", "variable"], dropna=False):
        metrics = compute_pair_metrics(group["value_a"], group["value_b"])
        cluster_ids = group["cluster_uid"].astype(str)
        fallback = group["cluster_id"].astype(str)
        cluster_key = cluster_ids.where(cluster_ids.str.strip().ne(""), fallback)
        time_values = group["time"].astype(str)
        rows.append(
            {
                "source_pair": keys[0],
                "source_family_pair": keys[1],
                "resolution": keys[2],
                "variable": keys[3],
                "bias": metrics["bias"],
                "RMSE": metrics["RMSE"],
                "MAE": metrics["MAE"],
                "MAPE": metrics["MAPE"],
                "n_valid_mape": metrics["n_valid_mape"],
                "Pearson correlation": metrics["Pearson correlation"],
                "Spearman rank correlation": metrics["Spearman rank correlation"],
                "n_pairs": metrics["n_pairs"],
                "n_clusters": int(cluster_key.nunique()),
                "time_start": time_values.min() if len(time_values) else "",
                "time_end": time_values.max() if len(time_values) else "",
                "method_notes": "bias is defined as source_b - source_a; {}".format(RELEASE_ONLY_NOTES),
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return _standard_csv_columns(pd.DataFrame(rows), columns)


def aggregate_pair_metrics_pooled(pair_records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "source_pair",
        "source_family_pair",
        "resolution",
        "variable",
        "bias",
        "RMSE",
        "MAE",
        "MAPE",
        "n_valid_mape",
        "Pearson correlation",
        "Spearman rank correlation",
        "n_pairs",
        "n_clusters",
        "time_start",
        "time_end",
        "method_notes",
        "assumptions",
    ]
    if pair_records.empty:
        return _standard_csv_columns(pd.DataFrame(), columns)
    rows = []
    for keys, group in pair_records.groupby(["source_pair", "source_family_pair", "variable"], dropna=False):
        metrics = compute_pair_metrics(group["value_a"], group["value_b"])
        cluster_ids = group["cluster_uid"].astype(str)
        fallback = group["cluster_id"].astype(str)
        cluster_key = cluster_ids.where(cluster_ids.str.strip().ne(""), fallback)
        time_values = group["time"].astype(str)
        rows.append(
            {
                "source_pair": keys[0],
                "source_family_pair": keys[1],
                "resolution": "all",
                "variable": keys[2],
                "bias": metrics["bias"],
                "RMSE": metrics["RMSE"],
                "MAE": metrics["MAE"],
                "MAPE": metrics["MAPE"],
                "n_valid_mape": metrics["n_valid_mape"],
                "Pearson correlation": metrics["Pearson correlation"],
                "Spearman rank correlation": metrics["Spearman rank correlation"],
                "n_pairs": metrics["n_pairs"],
                "n_clusters": int(cluster_key.nunique()),
                "time_start": time_values.min() if len(time_values) else "",
                "time_end": time_values.max() if len(time_values) else "",
                "method_notes": "bias is defined as source_b - source_a; {}".format(RELEASE_ONLY_NOTES),
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return _standard_csv_columns(pd.DataFrame(rows), columns)


def _summarize_selected_sources(records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "resolution",
        "source",
        "source_family",
        "n_records",
        "n_clusters",
        "time_start",
        "time_end",
        "fraction_of_records",
        "fraction_overlap_flagged",
        "method_notes",
        "assumptions",
    ]
    if records.empty or not {"resolution", "source", "source_family"}.issubset(records.columns):
        return _standard_csv_columns(
            pd.DataFrame(
                [
                    {
                        "resolution": "",
                        "source": "",
                        "source_family": "",
                        "n_records": 0,
                        "n_clusters": 0,
                        "time_start": "",
                        "time_end": "",
                        "fraction_of_records": np.nan,
                        "fraction_overlap_flagged": np.nan,
                        "method_notes": "selected source records not available in inspected s8 release products",
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                ]
            ),
            columns,
        )
    total = float(len(records)) if len(records) else float("nan")
    cluster_col = _cluster_key(records)
    rows = []
    for keys, group in records.groupby(["resolution", "source", "source_family"], dropna=False):
        time_values = group["time"].astype(str) if "time" in group.columns else pd.Series(dtype=str)
        if "is_overlap" in group.columns:
            overlap = pd.to_numeric(group["is_overlap"], errors="coerce")
            fraction_overlap = float((overlap == 1).sum() / len(group)) if len(group) else float("nan")
            overlap_note = RELEASE_ONLY_NOTES
        else:
            fraction_overlap = float("nan")
            overlap_note = "is_overlap field not available in s8 release product; {}".format(RELEASE_ONLY_NOTES)
        rows.append(
            {
                "resolution": keys[0],
                "source": keys[1],
                "source_family": keys[2],
                "n_records": int(len(group)),
                "n_clusters": int(group[cluster_col].nunique()) if cluster_col else np.nan,
                "time_start": time_values.min() if len(time_values) else "",
                "time_end": time_values.max() if len(time_values) else "",
                "fraction_of_records": float(len(group) / total) if total else np.nan,
                "fraction_overlap_flagged": fraction_overlap,
                "method_notes": overlap_note,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return _standard_csv_columns(pd.DataFrame(rows), columns)


def _summarize_overlap_flags(records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "resolution",
        "source",
        "source_family",
        "n_records",
        "n_overlap_flagged",
        "overlap_fraction",
        "n_clusters",
        "method_notes",
        "assumptions",
    ]
    if records.empty or not {"resolution", "source", "source_family"}.issubset(records.columns):
        return _standard_csv_columns(
            pd.DataFrame(
                [
                    {
                        "resolution": "",
                        "source": "",
                        "source_family": "",
                        "n_records": 0,
                        "n_overlap_flagged": np.nan,
                        "overlap_fraction": np.nan,
                        "n_clusters": 0,
                        "method_notes": "selected source records not available in inspected s8 release products",
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                ]
            ),
            columns,
        )
    cluster_col = _cluster_key(records)
    rows = []
    for keys, group in records.groupby(["resolution", "source", "source_family"], dropna=False):
        if "is_overlap" in group.columns:
            overlap = pd.to_numeric(group["is_overlap"], errors="coerce")
            n_overlap = int((overlap == 1).sum())
            overlap_fraction = float(n_overlap / len(group)) if len(group) else np.nan
            notes = RELEASE_ONLY_NOTES
        else:
            n_overlap = np.nan
            overlap_fraction = np.nan
            notes = "is_overlap field not available in s8 release product; {}".format(RELEASE_ONLY_NOTES)
        rows.append(
            {
                "resolution": keys[0],
                "source": keys[1],
                "source_family": keys[2],
                "n_records": int(len(group)),
                "n_overlap_flagged": n_overlap,
                "overlap_fraction": overlap_fraction,
                "n_clusters": int(group[cluster_col].nunique()) if cluster_col else np.nan,
                "method_notes": notes,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return _standard_csv_columns(pd.DataFrame(rows), columns)


def _summarize_overlap_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "resolution",
        "source",
        "source_family",
        "n_candidate_rows",
        "n_selected_rows",
        "n_overlap_groups",
        "n_clusters",
        "time_start",
        "time_end",
        "method_notes",
        "assumptions",
    ]
    if candidates.empty or not {"resolution", "source", "source_family"}.issubset(candidates.columns):
        return _standard_csv_columns(
            pd.DataFrame(
                [
                    {
                        "resolution": "",
                        "source": "",
                        "source_family": "",
                        "n_candidate_rows": 0,
                        "n_selected_rows": 0,
                        "n_overlap_groups": 0,
                        "n_clusters": 0,
                        "time_start": "",
                        "time_end": "",
                        "method_notes": "{} not available or not readable".format(OVERLAP_CANDIDATES_FILE),
                        "assumptions": RELEASE_ONLY_ASSUMPTIONS,
                    }
                ]
            ),
            columns,
        )
    cluster_col = _cluster_key(candidates)
    time_col = "time" if "time" in candidates.columns else ("date" if "date" in candidates.columns else None)
    rows = []
    for keys, group in candidates.groupby(["resolution", "source", "source_family"], dropna=False):
        selected = pd.to_numeric(group.get("selected_flag", pd.Series(dtype=float)), errors="coerce")
        if "candidate_group_key" in group.columns:
            overlap_group_key = group["candidate_group_key"].astype(str)
        elif cluster_col and time_col:
            overlap_group_key = (
                group[cluster_col].astype(str)
                + "|"
                + group["resolution"].astype(str)
                + "|"
                + group[time_col].astype(str)
            )
        else:
            overlap_group_key = pd.Series([""] * len(group), index=group.index)
        if "is_overlap" in group.columns:
            overlap_mask = pd.to_numeric(group["is_overlap"], errors="coerce") == 1
            n_overlap_groups = int(overlap_group_key[overlap_mask].nunique())
        else:
            n_overlap_groups = int(overlap_group_key.nunique())
        time_values = group[time_col].astype(str) if time_col else pd.Series(dtype=str)
        rows.append(
            {
                "resolution": keys[0],
                "source": keys[1],
                "source_family": keys[2],
                "n_candidate_rows": int(len(group)),
                "n_selected_rows": int((selected == 1).sum()),
                "n_overlap_groups": n_overlap_groups,
                "n_clusters": int(group[cluster_col].nunique()) if cluster_col else np.nan,
                "time_start": time_values.min() if len(time_values) else "",
                "time_end": time_values.max() if len(time_values) else "",
                "method_notes": RELEASE_ONLY_NOTES,
                "assumptions": RELEASE_ONLY_ASSUMPTIONS,
            }
        )
    return _standard_csv_columns(pd.DataFrame(rows), columns)


def _collect_key_fields(inventory: pd.DataFrame) -> str:
    key_names = {
        "cluster_uid",
        "cluster_id",
        "resolution",
        "time",
        "date",
        "source",
        "source_family",
        "source_station_uid",
        "candidate_rank",
        "candidate_quality_score",
        "selected_flag",
        "is_overlap",
        "n_candidates_at_time",
        "Q",
        "SSC",
        "SSL",
    }
    if inventory.empty:
        return "No schema inventory rows were created."
    found = sorted(
        set(inventory["dimension_or_column_or_variable"].astype(str)).intersection(key_names)
    )
    return ", ".join(found) if found else "No required validation key fields were found by exact name."


def _write_plot_files(pair_records: pd.DataFrame, units: Dict[str, str], figures_dir: Path) -> List[Tuple[str, str]]:
    generated: List[Tuple[str, str]] = []
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        return [("figures", "skipped: matplotlib not available: {}".format(exc))]

    figures_dir.mkdir(parents=True, exist_ok=True)
    for var in VARIABLES:
        subset = pair_records[pair_records["variable"].astype(str) == var].copy()
        unit = units.get(var, DEFAULT_UNITS[var])
        n_pairs = int(len(subset))
        if n_pairs < 2:
            generated.append(("figures/overlap_pair_scatter_{}.png".format(var), "skipped: fewer than 2 samples"))
            generated.append(("figures/overlap_pair_bias_box_{}.png".format(var), "skipped: fewer than 2 samples"))
            continue
        scatter_path = figures_dir / "overlap_pair_scatter_{}.png".format(var)
        fig, ax = plt.subplots(figsize=(6, 5))
        ax.scatter(subset["value_a"], subset["value_b"], s=12, alpha=0.7)
        ax.set_title("{} overlap source-pair scatter ({}; n_pairs={})".format(var, unit, n_pairs))
        ax.set_xlabel("source_a {} ({})".format(var, unit))
        ax.set_ylabel("source_b {} ({})".format(var, unit))
        finite = pd.concat([subset["value_a"], subset["value_b"]], ignore_index=True)
        finite = pd.to_numeric(finite, errors="coerce").dropna()
        if len(finite):
            lo = float(finite.min())
            hi = float(finite.max())
            ax.plot([lo, hi], [lo, hi], color="black", linewidth=1, linestyle="--")
        fig.tight_layout()
        fig.savefig(scatter_path, dpi=180)
        plt.close(fig)
        generated.append(("figures/{}".format(scatter_path.name), "generated"))

        box_path = figures_dir / "overlap_pair_bias_box_{}.png".format(var)
        fig, ax = plt.subplots(figsize=(max(6, min(14, subset["source_pair"].nunique() * 1.2)), 5))
        grouped = [
            pd.to_numeric(group["diff_b_minus_a"], errors="coerce").dropna().to_numpy()
            for _, group in subset.groupby("source_pair")
        ]
        labels = [str(label) for label, _ in subset.groupby("source_pair")]
        grouped = [values for values in grouped if len(values)]
        if grouped:
            ax.boxplot(grouped, labels=labels, showfliers=False)
            ax.set_title("{} overlap pair bias box ({}; n_pairs={})".format(var, unit, n_pairs))
            ax.set_ylabel("source_b - source_a {} ({})".format(var, unit))
            ax.tick_params(axis="x", rotation=45)
            fig.tight_layout()
            fig.savefig(box_path, dpi=180)
            generated.append(("figures/{}".format(box_path.name), "generated"))
        else:
            generated.append(("figures/{}".format(box_path.name), "skipped: no finite bias values"))
        plt.close(fig)
    return generated


def _write_summary(
    out_path: Path,
    release_files: Sequence[str],
    inventory: pd.DataFrame,
    supports_pairs: bool,
    selected_summary: pd.DataFrame,
    overlap_summary: pd.DataFrame,
    candidate_summary: pd.DataFrame,
    metrics: pd.DataFrame,
    generated_outputs: Sequence[Tuple[str, str]],
    load_note: str,
) -> None:
    key_fields = _collect_key_fields(inventory)
    lines: List[str] = []
    lines.append("# Validation Results Summary")
    lines.append("")
    lines.append("## 1. Input files")
    if release_files:
        lines.extend("- `{}`".format(name) for name in release_files)
    else:
        lines.append("- No files were found in release-dir.")
    lines.append("")
    lines.append("## 2. Product schema inspection")
    lines.append("- Key validation fields found by exact name: {}.".format(key_fields))
    lines.append("- Inspected NetCDF dimensions, variables, global attributes, catalog CSV columns, and GPKG sidecar layers/fields when readable.")
    lines.append("- Master-record loading note: {}.".format(load_note))
    lines.append("")
    lines.append("## 3. Method")
    lines.append("- This script reads only s8 release products in `--release-dir`; it does not read s6/s7 intermediate files, source station NC, or raw source datasets.")
    lines.append("- Source-pair metrics are computed from `sed_reference_overlap_candidates.csv.gz` when it contains candidate-level values for at least two distinct sources at the same cluster, resolution, and time/date key.")
    lines.append("- Source taxonomy uses lightweight substring rules: USGS, HYDAT, satellite, in_situ, secondary_compilation, and other.")
    lines.append("- Source pairs and family pairs are ordered stably by source family/source/source station uid; bias is `source_b - source_a`.")
    lines.append("")
    lines.append("## 4. Key numeric results")
    if supports_pairs and not metrics.empty:
        preview = metrics.sort_values(["n_pairs", "source_pair", "variable"], ascending=[False, True, True]).head(12)
        for _, row in preview.iterrows():
            lines.append(
                "- `{}` / `{}` / `{}` / `{}`: n_pairs={}, bias={}, RMSE={}, MAPE={}, Spearman={}.".format(
                    row.get("source_pair", ""),
                    row.get("source_family_pair", ""),
                    row.get("resolution", ""),
                    row.get("variable", ""),
                    row.get("n_pairs", ""),
                    row.get("bias", ""),
                    row.get("RMSE", ""),
                    row.get("MAPE", ""),
                    row.get("Spearman rank correlation", ""),
                )
            )
    else:
        lines.append("- {}.".format(TRUE_PAIR_SKIP_REASON))
        if not selected_summary.empty:
            total_records = pd.to_numeric(selected_summary["n_records"], errors="coerce").sum()
            lines.append("- Selected-source diagnostic rows: {}; selected records summarized: {}.".format(len(selected_summary), int(total_records)))
        if not overlap_summary.empty:
            lines.append("- Overlap-flag diagnostic rows: {}.".format(len(overlap_summary)))
    if not candidate_summary.empty:
        candidate_rows = pd.to_numeric(candidate_summary.get("n_candidate_rows", pd.Series(dtype=float)), errors="coerce").sum()
        lines.append("- Candidate sidecar summary rows: {}; candidate rows summarized: {}.".format(len(candidate_summary), int(candidate_rows)))
    lines.append("")
    lines.append("## 5. Limitations")
    lines.append("- If s8 release products keep only selected records, non-selected candidate values cannot be validated.")
    lines.append("- `is_overlap=1` indicates overlap or competition, but it does not mean all candidate source values were preserved.")
    lines.append("- True same cluster-time multi-source candidate consistency requires a candidate-level provenance sidecar, or a separate upstream candidate-validation script.")
    lines.append("- This validation is constrained to s8 release products only.")
    lines.append("")
    lines.append("## 6. Generated tables and figures")
    for name, status in generated_outputs:
        lines.append("- `{}`: {}".format(name, status))
    lines.append("")
    lines.append("## 7. Skipped validations")
    lines.append("- satellite vs in-situ multi-window validation: skipped by scope")
    lines.append("- long-term benchmark comparison: skipped by scope")
    lines.append("- coverage bias and representativeness validation: skipped by scope")
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def run_validation(
    release_dir: Path,
    out_dir: Path,
    workers: int = DEFAULT_WORKERS,
    progress: Optional[Callable[[str], None]] = log_progress,
) -> None:
    release_dir = release_dir.resolve()
    out_dir = out_dir.resolve()
    if not release_dir.exists() or not release_dir.is_dir():
        raise SystemExit("release-dir does not exist or is not a directory: {}".format(release_dir))

    if progress:
        progress("Starting s10 validation")
        progress("Release dir: {}".format(release_dir))
        progress("Output dir: {}".format(out_dir))
    out_dir.mkdir(parents=True, exist_ok=True)
    figures_dir = out_dir / "figures"
    release_files = sorted(path.name for path in release_dir.iterdir() if path.is_file())

    workers = max(1, int(workers))
    if progress:
        progress("Worker count for independent schema inspection: {}".format(workers))
    inventory, units = inspect_release_schema(release_dir, workers=workers, progress=progress)
    inventory_path = out_dir / "validation_product_schema_inventory.csv"
    inventory.to_csv(inventory_path, index=False)
    if progress:
        progress("Wrote {}".format(inventory_path))

    records, load_note = _load_master_records(release_dir, include_values=False, progress=progress)
    candidates, candidate_load_note = _load_overlap_candidates(release_dir, progress=progress)
    load_note = "{}; {}".format(load_note, candidate_load_note)
    if progress:
        progress("Checking candidate-level sidecar support for source-pair metrics")
    if not candidates.empty:
        supports_pairs, support_reason = release_supports_candidate_pairs(candidates)
        if not supports_pairs and progress:
            progress("Candidate sidecar is present but does not contain usable multi-source value pairs")
    else:
        supports_pairs, support_reason = False, TRUE_PAIR_SKIP_REASON
        if progress:
            progress("Candidate sidecar not available; using Path B diagnostics")
    diagnostic = build_overlap_availability_diagnostic(
        inventory,
        supports_candidate_values=supports_pairs,
        supports_source_pair_metrics=supports_pairs,
        reason=support_reason,
    )
    diagnostic_path = out_dir / "validation_overlap_availability_diagnostic.csv"
    diagnostic.to_csv(diagnostic_path, index=False)
    if progress:
        progress("Wrote {}".format(diagnostic_path))

    if progress:
        progress("Summarizing selected sources")
    selected_summary = _summarize_selected_sources(records)
    selected_path = out_dir / "validation_selected_source_summary.csv"
    selected_summary.to_csv(selected_path, index=False)
    if progress:
        progress("Wrote {}".format(selected_path))

    if progress:
        progress("Summarizing overlap flags")
    overlap_summary = _summarize_overlap_flags(records)
    overlap_path = out_dir / "validation_overlap_flag_summary.csv"
    overlap_summary.to_csv(overlap_path, index=False)
    if progress:
        progress("Wrote {}".format(overlap_path))

    if progress:
        progress("Summarizing overlap candidate sidecar")
    candidate_summary = _summarize_overlap_candidates(candidates)
    candidate_summary_path = out_dir / "validation_overlap_candidate_summary.csv"
    candidate_summary.to_csv(candidate_summary_path, index=False)
    if progress:
        progress("Wrote {}".format(candidate_summary_path))

    generated_outputs: List[Tuple[str, str]] = [
        (inventory_path.name, "generated"),
        (diagnostic_path.name, "generated"),
        (selected_path.name, "generated"),
        (overlap_path.name, "generated"),
        (candidate_summary_path.name, "generated"),
    ]
    metrics = pd.DataFrame()
    metrics_by_variable = pd.DataFrame()
    if supports_pairs:
        if progress:
            progress("Path A enabled from candidate sidecar; building overlap source-pair records")
        pair_records = build_overlap_pair_records(candidates)
        pair_records_path = out_dir / "validation_overlap_pair_records.csv"
        pair_records.to_csv(pair_records_path, index=False)
        generated_outputs.append((pair_records_path.name, "generated"))
        if progress:
            progress("Wrote {}".format(pair_records_path))

        if progress:
            progress("Aggregating source-pair metrics")
        metrics_by_variable = aggregate_pair_metrics(pair_records)
        metrics = aggregate_pair_metrics_pooled(pair_records)
        metrics_path = out_dir / "validation_overlap_source_pairs.csv"
        by_variable_path = out_dir / "validation_overlap_source_pairs_by_variable.csv"
        metrics.to_csv(metrics_path, index=False)
        metrics_by_variable.to_csv(by_variable_path, index=False)
        generated_outputs.append((metrics_path.name, "generated"))
        generated_outputs.append((by_variable_path.name, "generated"))
        if progress:
            progress("Wrote {}".format(metrics_path))
            progress("Wrote {}".format(by_variable_path))
            progress("Generating figures")
        generated_outputs.extend(_write_plot_files(pair_records, units, figures_dir))
    else:
        if progress:
            progress("Path B enabled; source-pair metrics and figures are skipped")
        generated_outputs.append(("validation_overlap_pair_records.csv", "skipped: {}".format(TRUE_PAIR_SKIP_REASON)))
        generated_outputs.append(("validation_overlap_source_pairs.csv", "skipped: {}".format(TRUE_PAIR_SKIP_REASON)))
        generated_outputs.append(("validation_overlap_source_pairs_by_variable.csv", "skipped: {}".format(TRUE_PAIR_SKIP_REASON)))
        for var in VARIABLES:
            generated_outputs.append(("figures/overlap_pair_scatter_{}.png".format(var), "skipped: {}".format(TRUE_PAIR_SKIP_REASON)))
            generated_outputs.append(("figures/overlap_pair_bias_box_{}.png".format(var), "skipped: {}".format(TRUE_PAIR_SKIP_REASON)))

    summary_path = out_dir / "validation_results_summary.md"
    generated_outputs.append((summary_path.name, "generated"))
    if progress:
        progress("Writing summary markdown")
    _write_summary(
        summary_path,
        release_files,
        inventory,
        supports_pairs,
        selected_summary,
        overlap_summary,
        candidate_summary,
        metrics,
        generated_outputs,
        load_note,
    )
    if progress:
        progress("Wrote {}".format(summary_path))
        progress("s10 validation complete")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate s8 sediment reference release overlap consistency and product verifiability."
    )
    parser.add_argument(
        "--release-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Path to scripts_basin_test/output/sed_reference_release.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Output directory for validation result tables, summary, and figures.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Number of worker processes for independent release schema inspection. Use 1 for serial mode.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run_validation(Path(args.release_dir), Path(args.out_dir), workers=args.workers)


if __name__ == "__main__":
    main()
