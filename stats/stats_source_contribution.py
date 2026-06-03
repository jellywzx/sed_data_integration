#!/usr/bin/env python3
"""
Summarize source-dataset contributions for the sediment reference release.

This script is intended to run after s8_publish_reference_dataset.py.  It reads
release-level provenance from sed_reference_master.nc and, when available,
source_station_catalog.csv / source_dataset_catalog.csv.  It writes manuscript-
ready contribution tables and simple bar figures.

Default outputs are written under:

  scripts_basin_test/output_other/source_contribution/

  tables/table_source_dataset_contribution.csv
  tables/table_source_type_contribution.csv
  tables/table_source_resolution_contribution.csv
  tables/table_source_variable_contribution.csv
  tables/table_top_source_contributors.csv
  tables/table_source_contribution_cumulative.csv
  tables/table_source_temporal_coverage.csv
  tables/table_report_key_metrics.csv
  tables/source_classification_template.csv
  figures/fig_source_contribution_records.png
  figures/fig_source_contribution_stations.png
  figures/fig_source_contribution_clusters.png
  figures/fig_source_type_records.png
  figures/fig_source_group_records.png
  figures/fig_source_resolution_stacked.png
  figures/fig_source_variable_stacked.png
  figures/fig_source_cumulative_contribution.png
  figures/fig_source_temporal_coverage.png
  figures/fig_satellite_contribution_records.png
  figures/fig_satellite_contribution_stations.png
  figures/fig_satellite_contribution_clusters.png
  figures/fig_satellite_resolution_stacked.png
  figures/fig_satellite_variable_stacked.png
  figures/fig_satellite_temporal_coverage.png
  figures/fig_climatology_contribution_records.png
  figures/fig_climatology_contribution_stations.png
  figures/fig_climatology_contribution_clusters.png
  figures/fig_climatology_resolution_stacked.png
  figures/fig_climatology_variable_stacked.png
  figures/fig_climatology_temporal_coverage.png
  reports/source_contribution_report.md

Notes on classification
-----------------------
The current s6/s7/s8 provenance stores source names and metadata, but not a
fully authoritative source_type/source_group taxonomy.  This script therefore
uses a small, conservative rule-based classifier, with an optional override CSV:

  --source-classification-csv source_classification.csv

Expected override columns:
  source_name, source_type, source_group

Supported source_type values are not hard-coded, but the intended manuscript
classes are:
  in-situ, satellite, climatology, literature

Recommended source_group values are:
  national agencies, global compilations, regional datasets, satellite products
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from collections import defaultdict
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - runtime dependency check
    nc4 = None

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    HAS_MATPLOTLIB = True
    MATPLOTLIB_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - runtime environment dependent
    matplotlib = None
    plt = None
    HAS_MATPLOTLIB = False
    MATPLOTLIB_IMPORT_ERROR = str(exc)

SCRIPT_DIR = Path(__file__).resolve().parent


def _find_repo_root(start_dir: Path) -> Path:
    """Return the nearest parent directory containing pipeline_paths.py.

    The stats script may be called from the repository root, from stats/, or from
    another working directory.  Do not rely on cwd; locate the project root from
    this file's path.
    """
    for candidate in (start_dir, *start_dir.parents):
        if (candidate / "pipeline_paths.py").is_file():
            return candidate
    raise RuntimeError(
        "Cannot locate pipeline_paths.py. Please put this script under the "
        "sed_data_integration repository, for example: stats/stats_source_contribution.py"
    )


REPO_SCRIPT_DIR = _find_repo_root(SCRIPT_DIR)
if str(REPO_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_SCRIPT_DIR))

from pipeline_paths import (
    RELEASE_CLIMATOLOGY_NC,
    RELEASE_DATASET_DIR,
    RELEASE_MASTER_NC,
    RELEASE_SATELLITE_CATALOG_CSV,
    RELEASE_SATELLITE_NC,
    RELEASE_SOURCE_DATASET_CATALOG_CSV,
    RELEASE_SOURCE_STATION_CATALOG_CSV,
    get_output_r_root,
)

PROJECT_ROOT = get_output_r_root(REPO_SCRIPT_DIR)

DEFAULT_RELEASE_DIR = PROJECT_ROOT / RELEASE_DATASET_DIR
DEFAULT_SOURCE_CONTRIBUTION_DIR = PROJECT_ROOT / "scripts_basin_test/output_other/source_contribution"
DEFAULT_MASTER_NC = PROJECT_ROOT / RELEASE_MASTER_NC
DEFAULT_CLIMATOLOGY_NC = PROJECT_ROOT / RELEASE_CLIMATOLOGY_NC
DEFAULT_SATELLITE_NC = PROJECT_ROOT / RELEASE_SATELLITE_NC
DEFAULT_SATELLITE_CATALOG = PROJECT_ROOT / RELEASE_SATELLITE_CATALOG_CSV
DEFAULT_SOURCE_STATION_CATALOG = PROJECT_ROOT / RELEASE_SOURCE_STATION_CATALOG_CSV
DEFAULT_SOURCE_DATASET_CATALOG = PROJECT_ROOT / RELEASE_SOURCE_DATASET_CATALOG_CSV

FILL_VALUES = {-9999.0, -9999, -999999.0, 9.969209968386869e36}
RESOLUTION_CODE_TO_NAME = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}
DATASET_COLUMNS = [
    "source_name",
    "source_type",
    "source_group",
    "n_source_stations",
    "n_clusters",
    "n_records",
    "n_Q_records",
    "n_SSC_records",
    "n_SSL_records",
    "first_year",
    "last_year",
    "resolutions",
    "percentage_of_total_records",
    "source_long_name",
    "institution",
    "reference",
    "source_url",
]
TYPE_COLUMNS = [
    "summary_level",
    "category",
    "n_source_datasets",
    "n_source_stations",
    "n_clusters",
    "n_records",
    "n_Q_records",
    "n_SSC_records",
    "n_SSL_records",
    "first_year",
    "last_year",
    "resolutions",
    "percentage_of_total_records",
]
RESOLUTION_COLUMNS = [
    "source_name",
    "source_type",
    "source_group",
    "resolution",
    "n_source_stations",
    "n_clusters",
    "n_records",
    "n_Q_records",
    "n_SSC_records",
    "n_SSL_records",
    "first_year",
    "last_year",
    "percentage_of_total_records",
    "percentage_within_source_records",
]
VARIABLE_COLUMNS = [
    "source_name",
    "source_type",
    "source_group",
    "variable",
    "n_variable_records",
    "n_source_records",
    "percentage_of_total_variable_records",
    "percentage_within_source_records",
]
TOP_COLUMNS = [
    "rank_metric",
    "rank",
    "source_name",
    "source_type",
    "source_group",
    "value",
    "percentage_of_metric_total",
]
CUMULATIVE_COLUMNS = [
    "rank",
    "source_name",
    "source_type",
    "source_group",
    "n_records",
    "percentage_of_total_records",
    "cumulative_records",
    "cumulative_percentage_of_total_records",
]
TEMPORAL_COLUMNS = [
    "source_name",
    "source_type",
    "source_group",
    "first_year",
    "last_year",
    "year_span",
    "n_records",
    "n_source_stations",
    "n_clusters",
    "resolutions",
]
KEY_METRIC_COLUMNS = ["metric", "value", "detail"]
CLASSIFICATION_TEMPLATE_COLUMNS = [
    "source_name",
    "suggested_source_type",
    "suggested_source_group",
    "source_long_name",
    "institution",
    "reference",
    "source_url",
]
VARIABLE_RECORD_COLUMNS = {
    "Q": "n_Q_records",
    "SSC": "n_SSC_records",
    "SSL": "n_SSL_records",
}


def _clean_text(value) -> str:
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def _read_text_var(ds, name: str, size: Optional[int] = None) -> List[str]:
    """Read a NetCDF text variable as a Python string list."""
    if name not in ds.variables:
        return [""] * int(size or 0)

    var = ds.variables[name]
    values = var[:]

    try:
        # netCDF char arrays often have shape (n, strlen).
        if getattr(values, "dtype", None) is not None and values.dtype.kind in {"S", "U"} and values.ndim > 1:
            arr = nc4.chartostring(values)
        else:
            arr = np.asarray(values, dtype=object)
    except Exception:
        arr = np.asarray(values, dtype=object)

    arr = np.asarray(arr, dtype=object).reshape(-1)
    result = [_clean_text(item) for item in arr]
    if size is not None and len(result) < int(size):
        result.extend([""] * (int(size) - len(result)))
    if size is not None:
        result = result[: int(size)]
    return result


def _read_int_var(ds, name: str, size: Optional[int] = None, fill_value: int = -1) -> np.ndarray:
    if name not in ds.variables:
        return np.full(int(size or 0), fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(fill_value)
    arr = np.asarray(raw)
    out = np.full(arr.shape, fill_value, dtype=np.int64)
    try:
        valid = np.isfinite(arr.astype(np.float64, copy=False))
        out[valid] = arr[valid].astype(np.int64)
    except Exception:
        for i, item in enumerate(arr):
            try:
                out[i] = int(item)
            except Exception:
                out[i] = fill_value
    if size is not None and len(out) < int(size):
        out = np.concatenate([out, np.full(int(size) - len(out), fill_value, dtype=np.int64)])
    if size is not None:
        out = out[: int(size)]
    return out


def _read_float_slice(ds, name: str, start: int, stop: int) -> Optional[np.ndarray]:
    if name not in ds.variables:
        return None
    raw = np.ma.asarray(ds.variables[name][start:stop]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(raw):
        arr = raw.filled(np.nan)
    else:
        arr = np.asarray(raw, dtype=np.float64)
    for fill in FILL_VALUES:
        arr[arr == fill] = np.nan
    return arr


def _has_data(arr: Optional[np.ndarray], size: int) -> np.ndarray:
    if arr is None:
        return np.zeros(int(size), dtype=bool)
    result = np.isfinite(arr)
    if len(result) < int(size):
        result = np.concatenate([result, np.zeros(int(size) - len(result), dtype=bool)])
    return result[: int(size)]


def _read_int_slice(ds, name: str, start: int, stop: int, fill_value: int = -1) -> np.ndarray:
    size = int(stop) - int(start)
    if name not in ds.variables:
        return np.full(size, fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][start:stop]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(fill_value)
    arr = np.asarray(raw)
    out = np.full(arr.shape, fill_value, dtype=np.int64)
    try:
        valid = np.isfinite(arr.astype(np.float64, copy=False))
        out[valid] = arr[valid].astype(np.int64)
    except Exception:
        for i, item in enumerate(arr):
            try:
                out[i] = int(item)
            except Exception:
                out[i] = fill_value
    if len(out) < size:
        out = np.concatenate([out, np.full(size - len(out), fill_value, dtype=np.int64)])
    return out[:size]


def _read_text_slice(ds, name: str, start: int, stop: int) -> List[str]:
    size = int(stop) - int(start)
    if name not in ds.variables:
        return [""] * size
    values = ds.variables[name][start:stop]
    try:
        if getattr(values, "dtype", None) is not None and values.dtype.kind in {"S", "U"} and values.ndim > 1:
            arr = nc4.chartostring(values)
        else:
            arr = np.asarray(values, dtype=object)
    except Exception:
        arr = np.asarray(values, dtype=object)
    result = [_clean_text(item) for item in np.asarray(arr, dtype=object).reshape(-1)]
    if len(result) < size:
        result.extend([""] * (size - len(result)))
    return result[:size]


def _read_resolution_slice(ds, name: str, start: int, stop: int) -> List[str]:
    size = int(stop) - int(start)
    if name not in ds.variables:
        return ["other"] * size
    dtype = getattr(ds.variables[name], "dtype", None)
    if dtype is not None and np.dtype(dtype).kind in {"i", "u", "f"}:
        codes = _read_int_slice(ds, name, start, stop, fill_value=-1)
        return [RESOLUTION_CODE_TO_NAME.get(int(code), "other") for code in codes]
    values = _read_text_slice(ds, name, start, stop)
    return [_clean_text(value) or "other" for value in values]


def _format_resolution(values: Iterable[str]) -> str:
    order = {"daily": 0, "monthly": 1, "annual": 2, "climatology": 3, "other": 4}
    cleaned = sorted(
        {_clean_text(v) for v in values if _clean_text(v)},
        key=lambda item: (order.get(item, 999), item),
    )
    return "|".join(cleaned)


def _year_from_time_num(value: float, units: str, calendar: str) -> Optional[int]:
    if value is None or not np.isfinite(value):
        return None
    if nc4 is not None:
        try:
            dt = nc4.num2date(
                [float(value)],
                units,
                calendar=calendar,
                only_use_cftime_datetimes=False,
            )[0]
            if hasattr(dt, "year"):
                return int(dt.year)
            return int(pd.Timestamp(str(dt)).year)
        except TypeError:
            try:
                dt = nc4.num2date([float(value)], units, calendar=calendar)[0]
                return int(dt.year)
            except Exception:
                pass
        except Exception:
            pass
    try:
        return int(pd.to_datetime(float(value), unit="D", origin="1970-01-01").year)
    except Exception:
        return None


def _empty_stats() -> Dict:
    return {
        "source_name": "",
        "source_long_name": "",
        "institution": "",
        "reference": "",
        "source_url": "",
        "products": set(),
        "source_stations": set(),
        "clusters": set(),
        "resolutions": set(),
        "n_records": 0,
        "n_Q_records": 0,
        "n_SSC_records": 0,
        "n_SSL_records": 0,
        "time_min": math.inf,
        "time_max": -math.inf,
    }


def _update_min_text(stats: Dict, field: str, value: str) -> None:
    value = _clean_text(value)
    if value and not stats.get(field):
        stats[field] = value


def _update_stats_from_frame(stats: Dict, sub: pd.DataFrame) -> None:
    if sub.empty:
        return
    if "source_station_uid" in sub.columns and "source_station_index" in sub.columns:
        stats["source_stations"].update(
            _clean_text(v) or str(i) for v, i in zip(sub["source_station_uid"], sub["source_station_index"])
        )
    elif "source_station_uid" in sub.columns:
        stats["source_stations"].update(_clean_text(v) for v in sub["source_station_uid"] if _clean_text(v))
    if "cluster_uid" in sub.columns:
        stats["clusters"].update(_clean_text(v) for v in sub["cluster_uid"] if _clean_text(v))
    if "resolution" in sub.columns:
        stats["resolutions"].update(_clean_text(v) for v in sub["resolution"] if _clean_text(v))
    stats["n_records"] += int(len(sub))
    stats["n_Q_records"] += int(sub["has_q"].sum()) if "has_q" in sub.columns else 0
    stats["n_SSC_records"] += int(sub["has_ssc"].sum()) if "has_ssc" in sub.columns else 0
    stats["n_SSL_records"] += int(sub["has_ssl"].sum()) if "has_ssl" in sub.columns else 0
    if "time_num" in sub.columns:
        valid_time = pd.to_numeric(sub["time_num"], errors="coerce")
        valid_time = valid_time[np.isfinite(valid_time)]
        if len(valid_time):
            stats["time_min"] = min(stats["time_min"], float(valid_time.min()))
            stats["time_max"] = max(stats["time_max"], float(valid_time.max()))


def _get_dim_size(ds, candidates: Iterable[str]) -> int:
    for name in candidates:
        if name in ds.dimensions:
            return len(ds.dimensions[name])
    return 0


def summarize_master_nc(
    master_nc: Path,
    chunk_size: int = 1_000_000,
) -> Tuple[Dict[str, Dict], Dict[Tuple[str, str], Dict], str, str]:
    if nc4 is None:
        raise RuntimeError("netCDF4 is required. Please install netCDF4 before running this script.")
    master_nc = Path(master_nc)
    if not master_nc.is_file():
        raise FileNotFoundError("Missing master NetCDF: {}".format(master_nc))

    stats_by_source: Dict[str, Dict] = defaultdict(_empty_stats)
    stats_by_source_resolution: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)

    with nc4.Dataset(master_nc, "r") as ds:
        n_sources = _get_dim_size(ds, ["n_sources", "source"])
        n_source_stations = _get_dim_size(ds, ["n_source_stations", "source_station"])
        n_stations = _get_dim_size(ds, ["n_stations", "station"])
        n_records = _get_dim_size(ds, ["n_records", "record"])
        if n_records <= 0:
            raise RuntimeError("master NetCDF has no n_records dimension: {}".format(master_nc))

        source_names = _read_text_var(ds, "source_name", size=n_sources)
        source_long_names = _read_text_var(ds, "source_long_name", size=n_sources)
        institutions = _read_text_var(ds, "institution", size=n_sources)
        references = _read_text_var(ds, "reference", size=n_sources)
        source_urls = _read_text_var(ds, "source_url", size=n_sources)

        source_station_uids = _read_text_var(ds, "source_station_uid", size=n_source_stations)
        source_station_source_index = _read_int_var(
            ds,
            "source_station_source_index",
            size=n_source_stations,
            fill_value=-1,
        )
        source_station_cluster_index = _read_int_var(
            ds,
            "source_station_cluster_index",
            size=n_source_stations,
            fill_value=-1,
        )

        cluster_ids = _read_int_var(ds, "cluster_id", size=n_stations, fill_value=-1)
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        if not any(cluster_uids):
            cluster_uids = ["SED{:06d}".format(int(cid)) if cid >= 0 else "" for cid in cluster_ids]

        station_to_source_name = []
        station_to_source_meta = []
        station_to_cluster_uid = []
        for source_idx, cluster_idx in zip(source_station_source_index, source_station_cluster_index):
            if 0 <= int(source_idx) < len(source_names):
                source_name = source_names[int(source_idx)]
                meta = {
                    "source_long_name": source_long_names[int(source_idx)] if int(source_idx) < len(source_long_names) else "",
                    "institution": institutions[int(source_idx)] if int(source_idx) < len(institutions) else "",
                    "reference": references[int(source_idx)] if int(source_idx) < len(references) else "",
                    "source_url": source_urls[int(source_idx)] if int(source_idx) < len(source_urls) else "",
                }
            else:
                source_name = "unknown"
                meta = {"source_long_name": "", "institution": "", "reference": "", "source_url": ""}
            station_to_source_name.append(source_name or "unknown")
            station_to_source_meta.append(meta)
            if 0 <= int(cluster_idx) < len(cluster_uids):
                station_to_cluster_uid.append(cluster_uids[int(cluster_idx)] or str(cluster_ids[int(cluster_idx)]))
            else:
                station_to_cluster_uid.append("")

        time_var = ds.variables.get("time")
        time_units = getattr(time_var, "units", "days since 1970-01-01") if time_var is not None else "days since 1970-01-01"
        time_calendar = getattr(time_var, "calendar", "gregorian") if time_var is not None else "gregorian"

        for start in range(0, n_records, int(chunk_size)):
            stop = min(start + int(chunk_size), n_records)
            source_station_index = _read_int_slice(ds, "source_station_index", start, stop, fill_value=-1)
            resolution_codes = _read_int_slice(ds, "resolution", start, stop, fill_value=-1)
            time_values = _read_float_slice(ds, "time", start, stop)
            if time_values is None:
                time_values = np.full(stop - start, np.nan, dtype=np.float64)

            q = _read_float_slice(ds, "Q", start, stop)
            ssc = _read_float_slice(ds, "SSC", start, stop)
            ssl = _read_float_slice(ds, "SSL", start, stop)
            chunk_len = stop - start
            has_q = _has_data(q, chunk_len)
            has_ssc = _has_data(ssc, chunk_len)
            has_ssl = _has_data(ssl, chunk_len)
            any_value = has_q | has_ssc | has_ssl

            frame = pd.DataFrame(
                {
                    "source_station_index": source_station_index,
                    "resolution": [RESOLUTION_CODE_TO_NAME.get(int(code), "other") for code in resolution_codes],
                    "time_num": time_values,
                    "has_q": has_q,
                    "has_ssc": has_ssc,
                    "has_ssl": has_ssl,
                    "any_value": any_value,
                }
            )
            frame = frame[frame["source_station_index"].between(0, len(source_station_uids) - 1)]
            if frame.empty:
                continue

            frame["source_name"] = frame["source_station_index"].map(lambda idx: station_to_source_name[int(idx)])
            frame["source_station_uid"] = frame["source_station_index"].map(lambda idx: source_station_uids[int(idx)])
            frame["cluster_uid"] = frame["source_station_index"].map(lambda idx: station_to_cluster_uid[int(idx)])

            # Final records in s6 are intended to have at least one Q/SSC/SSL value.
            # Keep all rows to match n_records, but non-empty record logic remains visible
            # through n_Q/n_SSC/n_SSL fields.
            grouped = frame.groupby("source_name", observed=True)
            for source_name, sub in grouped:
                stats = stats_by_source[source_name]
                stats["source_name"] = source_name
                stats["products"].add("master")
                first_idx = int(sub["source_station_index"].iloc[0])
                if 0 <= first_idx < len(station_to_source_meta):
                    meta = station_to_source_meta[first_idx]
                    for field in ("source_long_name", "institution", "reference", "source_url"):
                        _update_min_text(stats, field, meta.get(field, ""))
                _update_stats_from_frame(stats, sub)

            grouped_resolution = frame.groupby(["source_name", "resolution"], observed=True)
            for (source_name, resolution), sub in grouped_resolution:
                key = (source_name, resolution)
                stats = stats_by_source_resolution[key]
                stats["source_name"] = source_name
                stats["resolution"] = resolution
                stats["products"].add("master")
                first_idx = int(sub["source_station_index"].iloc[0])
                if 0 <= first_idx < len(station_to_source_meta):
                    meta = station_to_source_meta[first_idx]
                    for field in ("source_long_name", "institution", "reference", "source_url"):
                        _update_min_text(stats, field, meta.get(field, ""))
                _update_stats_from_frame(stats, sub)

    return stats_by_source, stats_by_source_resolution, time_units, time_calendar


def _pick_existing_var(ds, candidates: Iterable[str]) -> Optional[str]:
    for name in candidates:
        if name in ds.variables:
            return name
    return None


def _derive_source_from_path(path_text: str) -> str:
    text = _clean_text(path_text)
    if not text:
        return "unknown_climatology"
    parts = Path(text).parts
    for part in parts:
        low = part.lower()
        if low in {"daily", "monthly", "annual", "climatology", "output_resolution_organized"}:
            continue
        if part:
            return part
    return Path(text).stem or "unknown_climatology"


def summarize_climatology_nc(climatology_nc: Path) -> Tuple[Dict[str, Dict], Dict[Tuple[str, str], Dict], str, str]:
    """Best-effort climatology summary.

    The release climatology file is intentionally separate from the basin
    cluster mainline, so n_clusters is usually zero here unless cluster_uid is
    explicitly present in the climatology NetCDF.
    """
    empty: Dict[str, Dict] = defaultdict(_empty_stats)
    empty_resolution: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)
    if nc4 is None:
        return empty, empty_resolution, "days since 1970-01-01", "gregorian"
    climatology_nc = Path(climatology_nc)
    if not climatology_nc.is_file():
        return empty, empty_resolution, "days since 1970-01-01", "gregorian"

    stats_by_source: Dict[str, Dict] = defaultdict(_empty_stats)
    stats_by_source_resolution: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)
    with nc4.Dataset(climatology_nc, "r") as ds:
        n_sources = _get_dim_size(ds, ["n_sources", "source"])
        n_stations = _get_dim_size(ds, ["n_stations", "station", "stations"])
        if n_stations <= 0:
            return empty, empty_resolution, "days since 1970-01-01", "gregorian"

        uid_name = _pick_existing_var(ds, ["station_uid", "source_station_uid", "source_station_id"])
        source_name_var = _pick_existing_var(ds, ["source_name", "source", "dataset", "source_dataset"])
        source_index_var = _pick_existing_var(ds, ["source_index", "station_source_index"])
        path_var = _pick_existing_var(ds, ["source_station_path", "source_station_paths", "path"])
        cluster_var = _pick_existing_var(ds, ["cluster_uid", "cluster_id"])

        station_uids = _read_text_var(ds, uid_name, size=n_stations) if uid_name else ["CLIM{:06d}".format(i) for i in range(n_stations)]
        paths = _read_text_var(ds, path_var, size=n_stations) if path_var else [""] * n_stations
        source_names_by_index = []
        source_meta_by_index = []
        if source_name_var and n_sources > 0 and ds.variables[source_name_var].shape[0] == n_sources:
            source_names_by_index = _read_text_var(ds, source_name_var, size=n_sources)
            source_long_names = _read_text_var(ds, "source_long_name", size=n_sources)
            institutions = _read_text_var(ds, "institution", size=n_sources)
            references = _read_text_var(ds, "reference", size=n_sources)
            source_urls = _read_text_var(ds, "source_url", size=n_sources)
            for source_idx in range(n_sources):
                source_meta_by_index.append(
                    {
                        "source_long_name": source_long_names[source_idx] if source_idx < len(source_long_names) else "",
                        "institution": institutions[source_idx] if source_idx < len(institutions) else "",
                        "reference": references[source_idx] if source_idx < len(references) else "",
                        "source_url": source_urls[source_idx] if source_idx < len(source_urls) else "",
                    }
                )

        if source_names_by_index and source_index_var:
            source_indices = _read_int_var(ds, source_index_var, size=n_stations, fill_value=-1)
            source_names = [
                source_names_by_index[int(source_idx)]
                if 0 <= int(source_idx) < len(source_names_by_index)
                else _derive_source_from_path(paths[idx])
                for idx, source_idx in enumerate(source_indices)
            ]
            source_meta = [
                source_meta_by_index[int(source_idx)]
                if 0 <= int(source_idx) < len(source_meta_by_index)
                else {}
                for source_idx in source_indices
            ]
        elif source_name_var:
            source_names = _read_text_var(ds, source_name_var, size=n_stations)
            source_meta = [{} for _ in range(n_stations)]
        else:
            source_names = [_derive_source_from_path(path) for path in paths]
            source_meta = [{} for _ in range(n_stations)]
        clusters = _read_text_var(ds, cluster_var, size=n_stations) if cluster_var else [""] * n_stations

        q_name = _pick_existing_var(ds, ["Q", "discharge", "streamflow"])
        ssc_name = _pick_existing_var(ds, ["SSC", "ssc", "sediment_concentration"])
        ssl_name = _pick_existing_var(ds, ["SSL", "ssl", "sediment_load"])

        def per_station_counts(var_name: Optional[str]) -> np.ndarray:
            if not var_name:
                return np.zeros(n_stations, dtype=np.int64)
            raw = np.ma.asarray(ds.variables[var_name][:]).astype(np.float64)
            arr = raw.filled(np.nan) if np.ma.isMaskedArray(raw) else np.asarray(raw, dtype=np.float64)
            for fill in FILL_VALUES:
                arr[arr == fill] = np.nan
            if arr.ndim == 0:
                return np.zeros(n_stations, dtype=np.int64)
            if arr.shape[0] == n_stations:
                return np.isfinite(arr.reshape(n_stations, -1)).sum(axis=1).astype(np.int64)
            if arr.ndim > 1 and arr.shape[-1] == n_stations:
                return np.isfinite(np.moveaxis(arr, -1, 0).reshape(n_stations, -1)).sum(axis=1).astype(np.int64)
            if len(arr.reshape(-1)) == n_stations:
                return np.isfinite(arr.reshape(n_stations)).astype(np.int64)
            return np.zeros(n_stations, dtype=np.int64)

        q_counts = per_station_counts(q_name)
        ssc_counts = per_station_counts(ssc_name)
        ssl_counts = per_station_counts(ssl_name)
        record_counts = np.maximum.reduce([q_counts, ssc_counts, ssl_counts])
        record_counts[record_counts == 0] = 1

        time_var = ds.variables.get("time")
        time_units = getattr(time_var, "units", "days since 1970-01-01") if time_var is not None else "days since 1970-01-01"
        time_calendar = getattr(time_var, "calendar", "gregorian") if time_var is not None else "gregorian"
        time_min = math.inf
        time_max = -math.inf
        if time_var is not None:
            time_values = np.ma.asarray(time_var[:]).astype(np.float64)
            time_values = time_values.filled(np.nan) if np.ma.isMaskedArray(time_values) else np.asarray(time_values, dtype=np.float64)
            time_values = time_values[np.isfinite(time_values)]
            if len(time_values):
                time_min = float(time_values.min())
                time_max = float(time_values.max())

        for idx in range(n_stations):
            source_name = _clean_text(source_names[idx]) or "unknown_climatology"
            stats = stats_by_source[source_name]
            stats["source_name"] = source_name
            stats["products"].add("climatology")
            for field in ("source_long_name", "institution", "reference", "source_url"):
                _update_min_text(stats, field, source_meta[idx].get(field, "") if idx < len(source_meta) else "")
            stats["source_stations"].add(_clean_text(station_uids[idx]) or "CLIM{:06d}".format(idx))
            cluster_text = _clean_text(clusters[idx])
            if cluster_text:
                stats["clusters"].add(cluster_text)
            stats["resolutions"].add("climatology")
            stats["n_records"] += int(record_counts[idx])
            stats["n_Q_records"] += int(q_counts[idx])
            stats["n_SSC_records"] += int(ssc_counts[idx])
            stats["n_SSL_records"] += int(ssl_counts[idx])
            if np.isfinite(time_min):
                stats["time_min"] = min(stats["time_min"], time_min)
            if np.isfinite(time_max):
                stats["time_max"] = max(stats["time_max"], time_max)

            res_stats = stats_by_source_resolution[(source_name, "climatology")]
            res_stats["source_name"] = source_name
            res_stats["resolution"] = "climatology"
            res_stats["products"].add("climatology")
            for field in ("source_long_name", "institution", "reference", "source_url"):
                _update_min_text(res_stats, field, source_meta[idx].get(field, "") if idx < len(source_meta) else "")
            res_stats["source_stations"].add(_clean_text(station_uids[idx]) or "CLIM{:06d}".format(idx))
            if cluster_text:
                res_stats["clusters"].add(cluster_text)
            res_stats["resolutions"].add("climatology")
            res_stats["n_records"] += int(record_counts[idx])
            res_stats["n_Q_records"] += int(q_counts[idx])
            res_stats["n_SSC_records"] += int(ssc_counts[idx])
            res_stats["n_SSL_records"] += int(ssl_counts[idx])
            if np.isfinite(time_min):
                res_stats["time_min"] = min(res_stats["time_min"], time_min)
            if np.isfinite(time_max):
                res_stats["time_max"] = max(res_stats["time_max"], time_max)
    return stats_by_source, stats_by_source_resolution, time_units, time_calendar


def summarize_satellite_nc(
    satellite_nc: Path,
    chunk_size: int = 1_000_000,
) -> Tuple[Dict[str, Dict], Dict[Tuple[str, str], Dict], str, str]:
    """Summarize the validation-only satellite sidecar release product."""
    empty: Dict[str, Dict] = defaultdict(_empty_stats)
    empty_resolution: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)
    if nc4 is None:
        return empty, empty_resolution, "days since 1970-01-01", "gregorian"
    satellite_nc = Path(satellite_nc)
    if not satellite_nc.is_file():
        return empty, empty_resolution, "days since 1970-01-01", "gregorian"

    stats_by_source: Dict[str, Dict] = defaultdict(_empty_stats)
    stats_by_source_resolution: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)

    with nc4.Dataset(satellite_nc, "r") as ds:
        n_sources = _get_dim_size(ds, ["n_sources", "source"])
        n_stations = _get_dim_size(ds, ["n_satellite_stations", "n_stations", "station"])
        n_records = _get_dim_size(ds, ["n_satellite_records", "n_records", "record"])
        if n_stations <= 0 or n_records <= 0:
            return empty, empty_resolution, "days since 1970-01-01", "gregorian"

        source_names_by_index = _read_text_var(ds, "source_name", size=n_sources)
        source_long_names = _read_text_var(ds, "source_long_name", size=n_sources)
        institutions = _read_text_var(ds, "institution", size=n_sources)
        references = _read_text_var(ds, "reference", size=n_sources)
        source_urls = _read_text_var(ds, "source_url", size=n_sources)
        source_meta_by_index = []
        for source_idx in range(n_sources):
            source_meta_by_index.append(
                {
                    "source_long_name": source_long_names[source_idx] if source_idx < len(source_long_names) else "",
                    "institution": institutions[source_idx] if source_idx < len(institutions) else "",
                    "reference": references[source_idx] if source_idx < len(references) else "",
                    "source_url": source_urls[source_idx] if source_idx < len(source_urls) else "",
                }
            )

        station_uids = _read_text_var(ds, "satellite_station_uid", size=n_stations)
        station_sources = _read_text_var(ds, "source", size=n_stations)
        station_source_index = _read_int_var(ds, "source_index", size=n_stations, fill_value=-1)
        cluster_uids = _read_text_var(ds, "cluster_uid", size=n_stations)
        cluster_ids = _read_int_var(ds, "cluster_id_station", size=n_stations, fill_value=-1)
        if not any(cluster_uids):
            cluster_uids = ["SED{:06d}".format(int(cid)) if cid >= 0 else "" for cid in cluster_ids]

        station_to_source_name = []
        station_to_source_meta = []
        for source_idx, station_source in zip(station_source_index, station_sources):
            if 0 <= int(source_idx) < len(source_names_by_index):
                source_name = source_names_by_index[int(source_idx)]
                meta = source_meta_by_index[int(source_idx)]
            else:
                source_name = station_source or "unknown_satellite"
                meta = {}
            station_to_source_name.append(source_name or "unknown_satellite")
            station_to_source_meta.append(meta)

        time_var = ds.variables.get("time")
        time_units = getattr(time_var, "units", "days since 1970-01-01") if time_var is not None else "days since 1970-01-01"
        time_calendar = getattr(time_var, "calendar", "gregorian") if time_var is not None else "gregorian"

        for start in range(0, n_records, int(chunk_size)):
            stop = min(start + int(chunk_size), n_records)
            station_index = _read_int_slice(ds, "satellite_station_index", start, stop, fill_value=-1)
            resolutions = _read_resolution_slice(ds, "resolution", start, stop)
            time_values = _read_float_slice(ds, "time", start, stop)
            if time_values is None:
                time_values = np.full(stop - start, np.nan, dtype=np.float64)

            q = _read_float_slice(ds, "Q", start, stop)
            ssc = _read_float_slice(ds, "SSC", start, stop)
            ssl = _read_float_slice(ds, "SSL", start, stop)
            chunk_len = stop - start
            frame = pd.DataFrame(
                {
                    "source_station_index": station_index,
                    "resolution": resolutions,
                    "time_num": time_values,
                    "has_q": _has_data(q, chunk_len),
                    "has_ssc": _has_data(ssc, chunk_len),
                    "has_ssl": _has_data(ssl, chunk_len),
                }
            )
            frame = frame[frame["source_station_index"].between(0, len(station_uids) - 1)]
            if frame.empty:
                continue

            frame["source_name"] = frame["source_station_index"].map(lambda idx: station_to_source_name[int(idx)])
            frame["source_station_uid"] = frame["source_station_index"].map(lambda idx: station_uids[int(idx)])
            frame["cluster_uid"] = frame["source_station_index"].map(lambda idx: cluster_uids[int(idx)])

            for source_name, sub in frame.groupby("source_name", observed=True):
                stats = stats_by_source[source_name]
                stats["source_name"] = source_name
                stats["products"].add("satellite")
                first_idx = int(sub["source_station_index"].iloc[0])
                if 0 <= first_idx < len(station_to_source_meta):
                    meta = station_to_source_meta[first_idx]
                    for field in ("source_long_name", "institution", "reference", "source_url"):
                        _update_min_text(stats, field, meta.get(field, ""))
                _update_stats_from_frame(stats, sub)

            for (source_name, resolution), sub in frame.groupby(["source_name", "resolution"], observed=True):
                stats = stats_by_source_resolution[(source_name, resolution)]
                stats["source_name"] = source_name
                stats["resolution"] = resolution
                stats["products"].add("satellite")
                first_idx = int(sub["source_station_index"].iloc[0])
                if 0 <= first_idx < len(station_to_source_meta):
                    meta = station_to_source_meta[first_idx]
                    for field in ("source_long_name", "institution", "reference", "source_url"):
                        _update_min_text(stats, field, meta.get(field, ""))
                _update_stats_from_frame(stats, sub)

    return stats_by_source, stats_by_source_resolution, time_units, time_calendar


def _read_optional_csv(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_csv(path)


def _source_text(row: pd.Series) -> str:
    fields = [
        row.get("source_name", ""),
        row.get("source_long_name", ""),
        row.get("institution", ""),
        row.get("reference", ""),
        row.get("source_url", ""),
        row.get("source_products", ""),
    ]
    return " ".join(_clean_text(v) for v in fields).lower()


def classify_source(row: pd.Series) -> Tuple[str, str]:
    """Conservative fallback classifier for manuscript grouping."""
    text = _source_text(row)
    resolutions = _clean_text(row.get("resolutions", "")).lower()
    products = set(part for part in _clean_text(row.get("source_products", "")).lower().split("|") if part)

    if "satellite" in products and not (products - {"satellite"}):
        return "satellite", "satellite products"

    if (
        ("climatology" in products and not (products - {"climatology"}))
        or ("climatology" in resolutions and not any(res in resolutions for res in ["daily", "monthly", "annual"]))
    ):
        return "climatology", "global compilations"

    satellite_tokens = [
        "satellite",
        "remote sensing",
        "remote_sensing",
        "gsed",
        "riversed",
        "river sediment flux",
        "landsat",
        "sentinel",
        "modis",
    ]
    if any(token in text for token in satellite_tokens):
        return "satellite", "satellite products"

    literature_tokens = [
        "literature",
        "journal",
        "paper",
        "doi",
        "supplement",
        "compilation",
        "compiled",
        "global sediment",
    ]
    if any(token in text for token in literature_tokens):
        return "literature", "global compilations"

    national_tokens = [
        "usgs",
        "hydat",
        "water survey of canada",
        "environment canada",
        "national",
        "agency",
        "ministry",
        "geological survey",
        "bureau",
    ]
    if any(token in text for token in national_tokens):
        return "in-situ", "national agencies"

    global_tokens = ["grdc", "gsim", "global", "world", "international"]
    if any(token in text for token in global_tokens):
        return "in-situ", "global compilations"

    return "in-situ", "regional datasets"


def apply_source_classification(dataset_df: pd.DataFrame, classification_csv: Optional[Path]) -> pd.DataFrame:
    out = dataset_df.copy()
    if out.empty:
        out["source_type"] = []
        out["source_group"] = []
        return out
    inferred = out.apply(classify_source, axis=1, result_type="expand")
    out["source_type"] = inferred[0]
    out["source_group"] = inferred[1]

    if classification_csv:
        mapping = _read_optional_csv(classification_csv)
        if not mapping.empty:
            required = {"source_name", "source_type", "source_group"}
            missing = sorted(required - set(mapping.columns))
            if missing:
                raise ValueError(
                    "source classification CSV is missing columns: {}".format(", ".join(missing))
                )
            mapping = mapping[list(required)].copy()
            mapping["source_name"] = mapping["source_name"].astype(str).str.strip()
            out = out.merge(mapping, on="source_name", how="left", suffixes=("", "_override"))
            for col in ["source_type", "source_group"]:
                override = out["{}_override".format(col)].fillna("").astype(str).str.strip()
                out[col] = np.where(override.ne(""), override, out[col])
                out = out.drop(columns=["{}_override".format(col)])
    return out


def stats_to_dataset_frame(
    stats_by_source: Dict[str, Dict],
    time_units: str,
    time_calendar: str,
    extra_metadata_paths: Iterable[Path],
) -> pd.DataFrame:
    rows = []
    for source_name, stats in stats_by_source.items():
        first_year = _year_from_time_num(stats["time_min"], time_units, time_calendar)
        last_year = _year_from_time_num(stats["time_max"], time_units, time_calendar)
        rows.append(
            {
                "source_name": source_name,
                "source_long_name": stats.get("source_long_name", ""),
                "institution": stats.get("institution", ""),
                "reference": stats.get("reference", ""),
                "source_url": stats.get("source_url", ""),
                "n_source_stations": len(stats["source_stations"]),
                "n_clusters": len(stats["clusters"]),
                "n_records": int(stats["n_records"]),
                "n_Q_records": int(stats["n_Q_records"]),
                "n_SSC_records": int(stats["n_SSC_records"]),
                "n_SSL_records": int(stats["n_SSL_records"]),
                "first_year": first_year if first_year is not None else "",
                "last_year": last_year if last_year is not None else "",
                "resolutions": _format_resolution(stats["resolutions"]),
                "source_products": _format_resolution(stats.get("products", set())),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        out = pd.DataFrame(columns=[col for col in DATASET_COLUMNS if col not in {"source_type", "source_group", "percentage_of_total_records"}])

    # Enrich with existing release catalogs when they contain fuller metadata.
    for metadata_path in extra_metadata_paths:
        meta = _read_optional_csv(metadata_path)
        if meta.empty or "source_name" not in meta.columns:
            continue
        keep_cols = [
            col
            for col in ["source_name", "source_long_name", "institution", "reference", "source_url"]
            if col in meta.columns
        ]
        if len(keep_cols) <= 1:
            continue
        meta = meta[keep_cols].drop_duplicates(subset=["source_name"]).copy()
        out = out.merge(meta, on="source_name", how="left", suffixes=("", "_catalog"))
        for col in keep_cols:
            if col == "source_name":
                continue
            cat_col = "{}_catalog".format(col)
            if cat_col in out.columns:
                out[col] = np.where(
                    out[col].fillna("").astype(str).str.strip().eq(""),
                    out[cat_col].fillna(""),
                    out[col],
                )
                out = out.drop(columns=[cat_col])

    total_records = int(out["n_records"].sum()) if "n_records" in out.columns else 0
    out["percentage_of_total_records"] = (
        out["n_records"] / total_records * 100.0 if total_records > 0 else 0.0
    )
    return out


def merge_stats_dicts(base: Dict[str, Dict], extra: Dict[str, Dict]) -> Dict[str, Dict]:
    out = defaultdict(_empty_stats)
    for mapping in [base, extra]:
        for key, stats in mapping.items():
            target = out[key]
            if isinstance(key, tuple):
                target["source_name"] = key[0]
                if len(key) > 1:
                    target["resolution"] = key[1]
            else:
                target["source_name"] = key
            for field in ["source_long_name", "institution", "reference", "source_url"]:
                _update_min_text(target, field, stats.get(field, ""))
            target["source_stations"].update(stats.get("source_stations", set()))
            target["clusters"].update(stats.get("clusters", set()))
            target["resolutions"].update(stats.get("resolutions", set()))
            target["products"].update(stats.get("products", set()))
            for field in ["n_records", "n_Q_records", "n_SSC_records", "n_SSL_records"]:
                target[field] += int(stats.get(field, 0) or 0)
            if np.isfinite(stats.get("time_min", math.inf)):
                target["time_min"] = min(target["time_min"], float(stats["time_min"]))
            if np.isfinite(stats.get("time_max", -math.inf)):
                target["time_max"] = max(target["time_max"], float(stats["time_max"]))
    return out


def build_type_frame(dataset_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total_records = int(dataset_df["n_records"].sum()) if len(dataset_df) else 0

    for level, col in [("source_type", "source_type"), ("source_group", "source_group")]:
        if col not in dataset_df.columns:
            continue
        for category, sub in dataset_df.groupby(col, dropna=False, observed=True):
            category = _clean_text(category) or "unknown"
            first_years = pd.to_numeric(sub["first_year"], errors="coerce")
            last_years = pd.to_numeric(sub["last_year"], errors="coerce")
            resolutions = []
            for text in sub["resolutions"].fillna(""):
                resolutions.extend([part for part in str(text).split("|") if part])
            records = int(sub["n_records"].sum())
            rows.append(
                {
                    "summary_level": level,
                    "category": category,
                    "n_source_datasets": int(sub["source_name"].nunique()),
                    "n_source_stations": int(sub["n_source_stations"].sum()),
                    "n_clusters": int(sub["n_clusters"].sum()),
                    "n_records": records,
                    "n_Q_records": int(sub["n_Q_records"].sum()),
                    "n_SSC_records": int(sub["n_SSC_records"].sum()),
                    "n_SSL_records": int(sub["n_SSL_records"].sum()),
                    "first_year": int(first_years.min()) if first_years.notna().any() else "",
                    "last_year": int(last_years.max()) if last_years.notna().any() else "",
                    "resolutions": _format_resolution(resolutions),
                    "percentage_of_total_records": (records / total_records * 100.0) if total_records > 0 else 0.0,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=TYPE_COLUMNS)
    return out.reindex(columns=TYPE_COLUMNS).sort_values(
        ["summary_level", "n_records", "category"],
        ascending=[True, False, True],
    )


def build_resolution_frame(
    stats_by_source_resolution: Dict[Tuple[str, str], Dict],
    dataset_df: pd.DataFrame,
    time_units: str,
    time_calendar: str,
) -> pd.DataFrame:
    rows = []
    if dataset_df.empty:
        return pd.DataFrame(columns=RESOLUTION_COLUMNS)

    source_meta = dataset_df.set_index("source_name")[["source_type", "source_group", "n_records"]].to_dict("index")
    total_records = int(dataset_df["n_records"].sum())
    for key, stats in stats_by_source_resolution.items():
        if isinstance(key, tuple):
            source_name, resolution = key[0], key[1]
        else:
            source_name = stats.get("source_name", key)
            resolution = stats.get("resolution", _format_resolution(stats.get("resolutions", [])))
        meta = source_meta.get(source_name, {})
        source_records = int(meta.get("n_records", 0) or 0)
        records = int(stats.get("n_records", 0) or 0)
        first_year = _year_from_time_num(stats.get("time_min", math.inf), time_units, time_calendar)
        last_year = _year_from_time_num(stats.get("time_max", -math.inf), time_units, time_calendar)
        rows.append(
            {
                "source_name": source_name,
                "source_type": meta.get("source_type", ""),
                "source_group": meta.get("source_group", ""),
                "resolution": resolution,
                "n_source_stations": len(stats.get("source_stations", set())),
                "n_clusters": len(stats.get("clusters", set())),
                "n_records": records,
                "n_Q_records": int(stats.get("n_Q_records", 0) or 0),
                "n_SSC_records": int(stats.get("n_SSC_records", 0) or 0),
                "n_SSL_records": int(stats.get("n_SSL_records", 0) or 0),
                "first_year": first_year if first_year is not None else "",
                "last_year": last_year if last_year is not None else "",
                "percentage_of_total_records": (records / total_records * 100.0) if total_records > 0 else 0.0,
                "percentage_within_source_records": (records / source_records * 100.0) if source_records > 0 else 0.0,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=RESOLUTION_COLUMNS)
    return out.reindex(columns=RESOLUTION_COLUMNS).sort_values(
        ["n_records", "source_name", "resolution"],
        ascending=[False, True, True],
    )


def build_variable_frame(dataset_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if dataset_df.empty:
        return pd.DataFrame(columns=VARIABLE_COLUMNS)
    totals = {
        var_name: int(dataset_df[col].sum()) if col in dataset_df.columns else 0
        for var_name, col in VARIABLE_RECORD_COLUMNS.items()
    }
    for _, row in dataset_df.iterrows():
        source_records = int(row.get("n_records", 0) or 0)
        for var_name, col in VARIABLE_RECORD_COLUMNS.items():
            count = int(row.get(col, 0) or 0)
            total_var = totals.get(var_name, 0)
            rows.append(
                {
                    "source_name": row.get("source_name", ""),
                    "source_type": row.get("source_type", ""),
                    "source_group": row.get("source_group", ""),
                    "variable": var_name,
                    "n_variable_records": count,
                    "n_source_records": source_records,
                    "percentage_of_total_variable_records": (count / total_var * 100.0) if total_var > 0 else 0.0,
                    "percentage_within_source_records": (count / source_records * 100.0) if source_records > 0 else 0.0,
                }
            )
    out = pd.DataFrame(rows)
    return out.reindex(columns=VARIABLE_COLUMNS).sort_values(
        ["variable", "n_variable_records", "source_name"],
        ascending=[True, False, True],
    )


def build_top_contributors_frame(dataset_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    rows = []
    metrics = [
        ("records", "n_records"),
        ("source_stations", "n_source_stations"),
        ("clusters", "n_clusters"),
    ]
    for metric_name, col in metrics:
        if dataset_df.empty or col not in dataset_df.columns:
            continue
        total = float(dataset_df[col].sum())
        sub = dataset_df.sort_values([col, "source_name"], ascending=[False, True]).head(int(top_n))
        for rank, (_, row) in enumerate(sub.iterrows(), start=1):
            value = float(row.get(col, 0) or 0)
            rows.append(
                {
                    "rank_metric": metric_name,
                    "rank": rank,
                    "source_name": row.get("source_name", ""),
                    "source_type": row.get("source_type", ""),
                    "source_group": row.get("source_group", ""),
                    "value": value,
                    "percentage_of_metric_total": (value / total * 100.0) if total > 0 else 0.0,
                }
            )
    if not rows:
        return pd.DataFrame(columns=TOP_COLUMNS)
    return pd.DataFrame(rows).reindex(columns=TOP_COLUMNS)


def build_cumulative_frame(dataset_df: pd.DataFrame) -> pd.DataFrame:
    if dataset_df.empty:
        return pd.DataFrame(columns=CUMULATIVE_COLUMNS)
    out = dataset_df.sort_values(["n_records", "source_name"], ascending=[False, True]).copy()
    total = int(out["n_records"].sum())
    out["rank"] = np.arange(1, len(out) + 1)
    out["cumulative_records"] = out["n_records"].cumsum()
    out["cumulative_percentage_of_total_records"] = (
        out["cumulative_records"] / total * 100.0 if total > 0 else 0.0
    )
    keep = out[
        [
            "rank",
            "source_name",
            "source_type",
            "source_group",
            "n_records",
            "percentage_of_total_records",
            "cumulative_records",
            "cumulative_percentage_of_total_records",
        ]
    ].copy()
    return keep.reindex(columns=CUMULATIVE_COLUMNS)


def build_temporal_frame(dataset_df: pd.DataFrame) -> pd.DataFrame:
    if dataset_df.empty:
        return pd.DataFrame(columns=TEMPORAL_COLUMNS)
    out = dataset_df[
        [
            "source_name",
            "source_type",
            "source_group",
            "first_year",
            "last_year",
            "n_records",
            "n_source_stations",
            "n_clusters",
            "resolutions",
        ]
    ].copy()
    first = pd.to_numeric(out["first_year"], errors="coerce")
    last = pd.to_numeric(out["last_year"], errors="coerce")
    out["year_span"] = np.where(first.notna() & last.notna(), (last - first + 1).astype("Int64"), pd.NA)
    return out.reindex(columns=TEMPORAL_COLUMNS).sort_values(
        ["n_records", "source_name"],
        ascending=[False, True],
    )


def build_key_metrics_frame(dataset_df: pd.DataFrame, cumulative_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    def add(metric: str, value, detail: str = "") -> None:
        rows.append({"metric": metric, "value": value, "detail": detail})

    total_records = int(dataset_df["n_records"].sum()) if not dataset_df.empty else 0
    add("total_source_datasets", int(dataset_df["source_name"].nunique()) if not dataset_df.empty else 0)
    add("total_source_stations", int(dataset_df["n_source_stations"].sum()) if not dataset_df.empty else 0)
    add("total_clusters_source_sum", int(dataset_df["n_clusters"].sum()) if not dataset_df.empty else 0)
    add("total_records", total_records)
    for var_name, col in VARIABLE_RECORD_COLUMNS.items():
        add("total_{}_records".format(var_name), int(dataset_df[col].sum()) if col in dataset_df.columns else 0)

    if not dataset_df.empty:
        top = dataset_df.sort_values(["n_records", "source_name"], ascending=[False, True]).iloc[0]
        add("top_source_by_records", top["source_name"], "{:.2f}%".format(float(top["percentage_of_total_records"])))
        first_years = pd.to_numeric(dataset_df["first_year"], errors="coerce")
        last_years = pd.to_numeric(dataset_df["last_year"], errors="coerce")
        add("earliest_year", int(first_years.min()) if first_years.notna().any() else "")
        add("latest_year", int(last_years.max()) if last_years.notna().any() else "")

    for n in [1, 5, 10]:
        if not cumulative_df.empty:
            idx = min(n, len(cumulative_df)) - 1
            value = float(cumulative_df.iloc[idx]["cumulative_percentage_of_total_records"])
            add("top_{}_sources_record_share_percent".format(n), value)
        else:
            add("top_{}_sources_record_share_percent".format(n), 0.0)

    if not rows:
        return pd.DataFrame(columns=KEY_METRIC_COLUMNS)
    return pd.DataFrame(rows).reindex(columns=KEY_METRIC_COLUMNS)


def build_classification_template(dataset_df: pd.DataFrame) -> pd.DataFrame:
    if dataset_df.empty:
        return pd.DataFrame(columns=CLASSIFICATION_TEMPLATE_COLUMNS)
    out = dataset_df[
        [
            "source_name",
            "source_type",
            "source_group",
            "source_long_name",
            "institution",
            "reference",
            "source_url",
        ]
    ].copy()
    out = out.rename(
        columns={
            "source_type": "suggested_source_type",
            "source_group": "suggested_source_group",
        }
    )
    return out.reindex(columns=CLASSIFICATION_TEMPLATE_COLUMNS).sort_values("source_name")


def write_bar_figure(df: pd.DataFrame, value_col: str, out_path: Path, title: str, top_n: int = 20) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plot_df = df.sort_values(value_col, ascending=False).head(int(top_n)).copy()
    if plot_df.empty:
        plot_df = pd.DataFrame({"source_name": ["no data"], value_col: [0]})
    plot_df = plot_df.sort_values(value_col, ascending=True)

    height = max(4.0, 0.35 * len(plot_df) + 1.5)
    fig, ax = plt.subplots(figsize=(10, height))
    ax.barh(plot_df["source_name"].astype(str), plot_df[value_col].astype(float))
    ax.set_xlabel(value_col)
    ax.set_ylabel("source dataset")
    ax.set_title(title)
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def write_category_bar_figure(
    type_df: pd.DataFrame,
    summary_level: str,
    out_path: Path,
    title: str,
    value_col: str = "n_records",
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plot_df = type_df[type_df["summary_level"] == summary_level].copy() if not type_df.empty else pd.DataFrame()
    if plot_df.empty:
        plot_df = pd.DataFrame({"category": ["no data"], value_col: [0]})
    plot_df = plot_df.sort_values(value_col, ascending=True)

    height = max(3.5, 0.45 * len(plot_df) + 1.5)
    fig, ax = plt.subplots(figsize=(9, height))
    ax.barh(plot_df["category"].astype(str), plot_df[value_col].astype(float))
    ax.set_xlabel(value_col)
    ax.set_ylabel(summary_level)
    ax.set_title(title)
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def write_stacked_source_figure(
    df: pd.DataFrame,
    category_col: str,
    value_col: str,
    out_path: Path,
    title: str,
    top_n: int = 20,
    category_order: Optional[List[str]] = None,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        pivot = pd.DataFrame({"no data": [0]}, index=["no data"])
    else:
        totals = df.groupby("source_name", observed=True)[value_col].sum().sort_values(ascending=False)
        top_sources = list(totals.head(int(top_n)).index)
        plot_df = df[df["source_name"].isin(top_sources)].copy()
        pivot = plot_df.pivot_table(
            index="source_name",
            columns=category_col,
            values=value_col,
            aggfunc="sum",
            fill_value=0,
        )
        pivot["__total__"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("__total__", ascending=True).drop(columns=["__total__"])
        if category_order:
            ordered = [col for col in category_order if col in pivot.columns]
            ordered.extend([col for col in pivot.columns if col not in ordered])
            pivot = pivot[ordered]
    height = max(4.0, 0.35 * len(pivot) + 1.5)
    fig, ax = plt.subplots(figsize=(10, height))
    left = np.zeros(len(pivot), dtype=float)
    y = np.arange(len(pivot))
    for col in pivot.columns:
        values = pivot[col].astype(float).to_numpy()
        ax.barh(y, values, left=left, label=str(col))
        left += values
    ax.set_yticks(y)
    ax.set_yticklabels(pivot.index.astype(str))
    ax.set_xlabel(value_col)
    ax.set_ylabel("source dataset")
    ax.set_title(title)
    ax.grid(axis="x", alpha=0.3)
    if len(pivot.columns) > 1:
        ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def write_cumulative_figure(cumulative_df: pd.DataFrame, out_path: Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 5))
    if cumulative_df.empty:
        ax.plot([0], [0], marker="o")
    else:
        ax.plot(
            cumulative_df["rank"].astype(float),
            cumulative_df["cumulative_percentage_of_total_records"].astype(float),
            marker="o",
            linewidth=1.5,
        )
    ax.set_xlabel("source dataset rank")
    ax.set_ylabel("cumulative record contribution (%)")
    ax.set_title("Cumulative source contribution by records")
    ax.set_ylim(0, 105)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def write_temporal_coverage_figure(temporal_df: pd.DataFrame, out_path: Path, top_n: int = 20) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plot_df = temporal_df.copy()
    if not plot_df.empty:
        plot_df["first_year_num"] = pd.to_numeric(plot_df["first_year"], errors="coerce")
        plot_df["last_year_num"] = pd.to_numeric(plot_df["last_year"], errors="coerce")
        plot_df = plot_df[plot_df["first_year_num"].notna() & plot_df["last_year_num"].notna()]
        plot_df = plot_df.sort_values(["n_records", "source_name"], ascending=[False, True]).head(int(top_n))
        plot_df = plot_df.sort_values("first_year_num", ascending=True)
    if plot_df.empty:
        plot_df = pd.DataFrame(
            {"source_name": ["no data"], "first_year_num": [0], "last_year_num": [0], "n_records": [0]}
        )

    height = max(4.0, 0.35 * len(plot_df) + 1.5)
    fig, ax = plt.subplots(figsize=(10, height))
    y = np.arange(len(plot_df))
    ax.hlines(
        y,
        plot_df["first_year_num"].astype(float),
        plot_df["last_year_num"].astype(float),
        linewidth=3,
    )
    ax.scatter(plot_df["first_year_num"].astype(float), y, s=20)
    ax.scatter(plot_df["last_year_num"].astype(float), y, s=20)
    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["source_name"].astype(str))
    ax.set_xlabel("year")
    ax.set_ylabel("source dataset")
    ax.set_title("Temporal coverage of top source datasets")
    ax.grid(axis="x", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def force_product_classification(dataset_df: pd.DataFrame, source_type: str, source_group: str) -> pd.DataFrame:
    out = dataset_df.copy()
    if out.empty:
        return out
    out["source_type"] = source_type
    out["source_group"] = source_group
    return out


def prepare_product_frames(
    stats_by_source: Dict[str, Dict],
    stats_by_source_resolution: Dict[Tuple[str, str], Dict],
    time_units: str,
    time_calendar: str,
    metadata_paths: Iterable[Path],
    classification_csv: Optional[Path],
    forced_source_type: str,
    forced_source_group: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dataset_df = stats_to_dataset_frame(stats_by_source, time_units, time_calendar, metadata_paths)
    dataset_df = apply_source_classification(dataset_df, classification_csv)
    dataset_df = force_product_classification(dataset_df, forced_source_type, forced_source_group)
    dataset_df = dataset_df.reindex(columns=DATASET_COLUMNS).sort_values(
        ["n_records", "source_name"],
        ascending=[False, True],
    )
    resolution_df = build_resolution_frame(stats_by_source_resolution, dataset_df, time_units, time_calendar)
    variable_df = build_variable_frame(dataset_df)
    temporal_df = build_temporal_frame(dataset_df)
    return dataset_df, resolution_df, variable_df, temporal_df


def write_product_figure_set(
    product_label: str,
    filename_prefix: str,
    figure_dir: Path,
    dataset_df: pd.DataFrame,
    resolution_df: pd.DataFrame,
    variable_df: pd.DataFrame,
    temporal_df: pd.DataFrame,
    top_n: int = 20,
) -> Dict[str, Path]:
    title_label = product_label.capitalize()
    paths = {
        "{} contribution by records".format(title_label): figure_dir / "fig_{}_contribution_records.png".format(filename_prefix),
        "{} contribution by source stations".format(title_label): figure_dir / "fig_{}_contribution_stations.png".format(filename_prefix),
        "{} contribution by clusters".format(title_label): figure_dir / "fig_{}_contribution_clusters.png".format(filename_prefix),
        "{} resolution contribution".format(title_label): figure_dir / "fig_{}_resolution_stacked.png".format(filename_prefix),
        "{} variable coverage".format(title_label): figure_dir / "fig_{}_variable_stacked.png".format(filename_prefix),
        "{} temporal coverage".format(title_label): figure_dir / "fig_{}_temporal_coverage.png".format(filename_prefix),
    }
    write_bar_figure(dataset_df, "n_records", paths["{} contribution by records".format(title_label)], "{} source contribution by records".format(title_label), top_n=top_n)
    write_bar_figure(dataset_df, "n_source_stations", paths["{} contribution by source stations".format(title_label)], "{} source contribution by source stations".format(title_label), top_n=top_n)
    write_bar_figure(dataset_df, "n_clusters", paths["{} contribution by clusters".format(title_label)], "{} source contribution by clusters".format(title_label), top_n=top_n)
    write_stacked_source_figure(
        resolution_df,
        "resolution",
        "n_records",
        paths["{} resolution contribution".format(title_label)],
        "{} source contribution by resolution".format(title_label),
        top_n=top_n,
        category_order=["daily", "monthly", "annual", "climatology", "other"],
    )
    write_stacked_source_figure(
        variable_df,
        "variable",
        "n_variable_records",
        paths["{} variable coverage".format(title_label)],
        "{} variable coverage by valid records".format(title_label),
        top_n=top_n,
        category_order=["Q", "SSC", "SSL"],
    )
    write_temporal_coverage_figure(temporal_df, paths["{} temporal coverage".format(title_label)], top_n=top_n)
    return paths


def _percent(value) -> str:
    try:
        return "{:.2f}%".format(float(value))
    except Exception:
        return "NA"


def _int_text(value) -> str:
    try:
        if pd.isna(value):
            return "0"
        return "{:,}".format(int(float(value)))
    except Exception:
        return "NA"


def _safe_md(value) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"nan", "none", "null", "<na>"}:
        text = ""
    return text.replace("|", "\\|")


def _markdown_table(rows: List[Dict[str, object]], columns: List[str], headers: List[str]) -> str:
    if not rows:
        return "_No rows._"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_safe_md(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines)


def _compact_dataset_rows(dataset_df: pd.DataFrame, sort_col: str, top_n: int = 12) -> List[Dict[str, object]]:
    if dataset_df.empty or sort_col not in dataset_df.columns:
        return []
    work = dataset_df.copy()
    work[sort_col] = pd.to_numeric(work[sort_col], errors="coerce").fillna(0)
    work = work.sort_values([sort_col, "source_name"], ascending=[False, True], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "source": row.get("source_name", ""),
                "type": row.get("source_type", ""),
                "group": row.get("source_group", ""),
                "stations": _int_text(row.get("n_source_stations", 0)),
                "clusters": _int_text(row.get("n_clusters", 0)),
                "records": _int_text(row.get("n_records", 0)),
                "record_share": _percent(row.get("percentage_of_total_records", 0)),
                "q": _int_text(row.get("n_Q_records", 0)),
                "ssc": _int_text(row.get("n_SSC_records", 0)),
                "ssl": _int_text(row.get("n_SSL_records", 0)),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "resolutions": row.get("resolutions", ""),
            }
        )
    return rows


def _compact_type_rows(type_df: pd.DataFrame, summary_level: str) -> List[Dict[str, object]]:
    if type_df.empty:
        return []
    work = type_df[type_df["summary_level"].eq(summary_level)].copy()
    if work.empty:
        return []
    work = work.sort_values(["n_records", "category"], ascending=[False, True], kind="mergesort")
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "category": row.get("category", ""),
                "sources": _int_text(row.get("n_source_datasets", 0)),
                "stations": _int_text(row.get("n_source_stations", 0)),
                "clusters": _int_text(row.get("n_clusters", 0)),
                "records": _int_text(row.get("n_records", 0)),
                "record_share": _percent(row.get("percentage_of_total_records", 0)),
                "q": _int_text(row.get("n_Q_records", 0)),
                "ssc": _int_text(row.get("n_SSC_records", 0)),
                "ssl": _int_text(row.get("n_SSL_records", 0)),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "resolutions": row.get("resolutions", ""),
            }
        )
    return rows


def _compact_resolution_rows(resolution_df: pd.DataFrame, top_n: int = 20) -> List[Dict[str, object]]:
    if resolution_df.empty:
        return []
    work = resolution_df.copy()
    work["n_records"] = pd.to_numeric(work["n_records"], errors="coerce").fillna(0)
    work = work.sort_values(["n_records", "source_name"], ascending=[False, True], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "source": row.get("source_name", ""),
                "resolution": row.get("resolution", ""),
                "type": row.get("source_type", ""),
                "stations": _int_text(row.get("n_source_stations", 0)),
                "clusters": _int_text(row.get("n_clusters", 0)),
                "records": _int_text(row.get("n_records", 0)),
                "global_share": _percent(row.get("percentage_of_total_records", 0)),
                "within_source": _percent(row.get("percentage_within_source_records", 0)),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
            }
        )
    return rows


def _compact_variable_rows(variable_df: pd.DataFrame, top_n: int = 20) -> List[Dict[str, object]]:
    if variable_df.empty:
        return []
    work = variable_df.copy()
    work["n_variable_records"] = pd.to_numeric(work["n_variable_records"], errors="coerce").fillna(0)
    work = work.sort_values(["n_variable_records", "source_name"], ascending=[False, True], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "source": row.get("source_name", ""),
                "variable": row.get("variable", ""),
                "type": row.get("source_type", ""),
                "variable_records": _int_text(row.get("n_variable_records", 0)),
                "source_records": _int_text(row.get("n_source_records", 0)),
                "variable_share": _percent(row.get("percentage_of_total_variable_records", 0)),
                "within_source": _percent(row.get("percentage_within_source_records", 0)),
            }
        )
    return rows


def _compact_cumulative_rows(cumulative_df: pd.DataFrame) -> List[Dict[str, object]]:
    if cumulative_df.empty:
        return []
    rows: List[Dict[str, object]] = []
    work = cumulative_df.copy()
    work["cumulative_percentage_of_total_records"] = pd.to_numeric(work["cumulative_percentage_of_total_records"], errors="coerce")
    for threshold in [50, 75, 90, 95, 99]:
        sub = work[work["cumulative_percentage_of_total_records"] >= threshold]
        if sub.empty:
            continue
        row = sub.iloc[0]
        rows.append(
            {
                "threshold": "{}%".format(threshold),
                "rank": _int_text(row.get("rank", 0)),
                "source": row.get("source_name", ""),
                "cumulative_records": _int_text(row.get("cumulative_records", 0)),
                "cumulative_share": _percent(row.get("cumulative_percentage_of_total_records", 0)),
            }
        )
    return rows


def _compact_temporal_rows(temporal_df: pd.DataFrame, top_n: int = 12) -> List[Dict[str, object]]:
    if temporal_df.empty:
        return []
    work = temporal_df.copy()
    work["n_records"] = pd.to_numeric(work["n_records"], errors="coerce").fillna(0)
    work = work.sort_values(["n_records", "source_name"], ascending=[False, True], kind="mergesort").head(top_n)
    rows: List[Dict[str, object]] = []
    for _, row in work.iterrows():
        rows.append(
            {
                "source": row.get("source_name", ""),
                "type": row.get("source_type", ""),
                "span": "{}-{}".format(row.get("first_year", ""), row.get("last_year", "")),
                "years": _int_text(row.get("year_span", 0)),
                "records": _int_text(row.get("n_records", 0)),
                "stations": _int_text(row.get("n_source_stations", 0)),
                "clusters": _int_text(row.get("n_clusters", 0)),
                "resolutions": row.get("resolutions", ""),
            }
        )
    return rows


def _rel_path(path: Path, base_dir: Path) -> str:
    return os.path.relpath(str(Path(path)), str(Path(base_dir))).replace(os.sep, "/")


def _metric_value(metrics_df: pd.DataFrame, metric: str, default=""):
    if metrics_df.empty:
        return default
    sub = metrics_df[metrics_df["metric"] == metric]
    if sub.empty:
        return default
    return sub.iloc[0]["value"]


def build_report_insights(
    dataset_df: pd.DataFrame,
    type_df: pd.DataFrame,
    resolution_df: pd.DataFrame,
    variable_df: pd.DataFrame,
    cumulative_df: pd.DataFrame,
) -> List[str]:
    insights = []
    if dataset_df.empty:
        return ["No source contribution records were available for this run."]

    top = dataset_df.sort_values(["n_records", "source_name"], ascending=[False, True]).iloc[0]
    insights.append(
        "Top source by records is `{}` with {:,} records ({} of all records).".format(
            top["source_name"],
            int(top["n_records"]),
            _percent(top["percentage_of_total_records"]),
        )
    )
    if len(cumulative_df) >= 5:
        insights.append(
            "The top 5 sources contribute {} of all records.".format(
                _percent(cumulative_df.iloc[4]["cumulative_percentage_of_total_records"])
            )
        )
    elif not cumulative_df.empty:
        insights.append(
            "All listed sources contribute {} of all records.".format(
                _percent(cumulative_df.iloc[-1]["cumulative_percentage_of_total_records"])
            )
        )

    source_type = type_df[type_df["summary_level"] == "source_type"] if not type_df.empty else pd.DataFrame()
    if not source_type.empty:
        top_type = source_type.sort_values(["n_records", "category"], ascending=[False, True]).iloc[0]
        insights.append(
            "Dominant source type is `{}` with {:,} records ({}).".format(
                top_type["category"],
                int(top_type["n_records"]),
                _percent(top_type["percentage_of_total_records"]),
            )
        )

    if not resolution_df.empty:
        res = resolution_df.groupby("resolution", observed=True)["n_records"].sum().sort_values(ascending=False)
        if len(res):
            insights.append(
                "Dominant resolution is `{}` with {:,} records.".format(res.index[0], int(res.iloc[0]))
            )

    if not variable_df.empty:
        var = variable_df.groupby("variable", observed=True)["n_variable_records"].sum().sort_values(ascending=False)
        if len(var):
            insights.append(
                "Best-covered variable is `{}` with {:,} valid records.".format(var.index[0], int(var.iloc[0]))
            )
            if len(var) > 1:
                insights.append(
                    "Most limited variable is `{}` with {:,} valid records.".format(var.index[-1], int(var.iloc[-1]))
                )

    first_years = pd.to_numeric(dataset_df["first_year"], errors="coerce")
    last_years = pd.to_numeric(dataset_df["last_year"], errors="coerce")
    if first_years.notna().any() and last_years.notna().any():
        insights.append(
            "Overall temporal coverage spans {} to {} across sources with parseable dates.".format(
                int(first_years.min()),
                int(last_years.max()),
            )
        )
    return insights


def write_markdown_report(
    report_path: Path,
    dataset_df: pd.DataFrame,
    type_df: pd.DataFrame,
    resolution_df: pd.DataFrame,
    variable_df: pd.DataFrame,
    cumulative_df: pd.DataFrame,
    temporal_df: pd.DataFrame,
    key_metrics_df: pd.DataFrame,
    table_paths: Dict[str, Path],
    figure_paths: Dict[str, Path],
    input_paths: Dict[str, Path],
) -> None:
    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_dir = report_path.parent

    insights = build_report_insights(dataset_df, type_df, resolution_df, variable_df, cumulative_df)
    total_records = _metric_value(key_metrics_df, "total_records", 0)
    total_sources = _metric_value(key_metrics_df, "total_source_datasets", 0)
    total_stations = _metric_value(key_metrics_df, "total_source_stations", 0)
    total_clusters = _metric_value(key_metrics_df, "total_clusters_source_sum", 0)

    lines = [
        "# S8 Source Contribution Statistics",
        "",
        "Generated: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "",
        "## Data Sources",
        "",
    ]
    for label, path in input_paths.items():
        lines.append("- `{}`: `{}`".format(label, path))
    lines.extend(
        [
            "",
            "## Statistical Scope",
            "",
            "- `n_records` counts release-level records with provenance in the master/satellite/climatology products.",
            "- `n_Q_records`, `n_SSC_records`, and `n_SSL_records` count valid values for each variable.",
            "- `n_source_stations` counts source station identifiers before reference clustering.",
            "- `n_clusters` counts reference clusters touched by each source where cluster identifiers are available.",
            "- `source_type` and `source_group` are rule-inferred unless overridden by `--source-classification-csv`.",
            "",
            "## Key Metrics",
            "",
            "- Source datasets: {:,}".format(int(float(total_sources or 0))),
            "- Source stations summed across datasets: {:,}".format(int(float(total_stations or 0))),
            "- Cluster counts summed across datasets: {:,}".format(int(float(total_clusters or 0))),
            "- Total records: {:,}".format(int(float(total_records or 0))),
            "- Top 1 record share: {}".format(
                _percent(_metric_value(key_metrics_df, "top_1_sources_record_share_percent", 0))
            ),
            "- Top 5 record share: {}".format(
                _percent(_metric_value(key_metrics_df, "top_5_sources_record_share_percent", 0))
            ),
            "- Top 10 record share: {}".format(
                _percent(_metric_value(key_metrics_df, "top_10_sources_record_share_percent", 0))
            ),
            "",
            "## Main Insights",
            "",
        ]
    )
    for item in insights:
        lines.append("- {}".format(item))

    lines.extend(
        [
            "",
            "## Contribution Concentration",
            "",
            _markdown_table(
                _compact_cumulative_rows(cumulative_df),
                ["threshold", "rank", "source", "cumulative_records", "cumulative_share"],
                ["Cumulative threshold", "Rank reached", "Source at threshold", "Cumulative records", "Cumulative share"],
            ),
            "",
            "This concentration table is useful for explaining how strongly the release is dominated by the largest source datasets. It should be read together with the cluster and station tables, because record dominance does not necessarily imply the broadest spatial footprint.",
            "",
            "## Dataset Rankings",
            "",
            "### Top Sources by Records",
            "",
            _markdown_table(
                _compact_dataset_rows(dataset_df, "n_records", top_n=12),
                ["source", "type", "group", "stations", "clusters", "records", "record_share", "q", "ssc", "ssl", "span", "resolutions"],
                ["Source", "Type", "Group", "Stations", "Clusters", "Records", "Share", "Q", "SSC", "SSL", "Span", "Resolutions"],
            ),
            "",
            "### Top Sources by Source Stations",
            "",
            _markdown_table(
                _compact_dataset_rows(dataset_df, "n_source_stations", top_n=12),
                ["source", "type", "group", "stations", "clusters", "records", "record_share", "q", "ssc", "ssl", "span", "resolutions"],
                ["Source", "Type", "Group", "Stations", "Clusters", "Records", "Share", "Q", "SSC", "SSL", "Span", "Resolutions"],
            ),
            "",
            "### Top Sources by Clusters",
            "",
            _markdown_table(
                _compact_dataset_rows(dataset_df, "n_clusters", top_n=12),
                ["source", "type", "group", "stations", "clusters", "records", "record_share", "q", "ssc", "ssl", "span", "resolutions"],
                ["Source", "Type", "Group", "Stations", "Clusters", "Records", "Share", "Q", "SSC", "SSL", "Span", "Resolutions"],
            ),
            "",
            "## Source Classes",
            "",
            "### Source Type Contribution",
            "",
            _markdown_table(
                _compact_type_rows(type_df, "source_type"),
                ["category", "sources", "stations", "clusters", "records", "record_share", "q", "ssc", "ssl", "span", "resolutions"],
                ["Type", "Sources", "Stations", "Clusters", "Records", "Share", "Q", "SSC", "SSL", "Span", "Resolutions"],
            ),
            "",
            "### Source Group Contribution",
            "",
            _markdown_table(
                _compact_type_rows(type_df, "source_group"),
                ["category", "sources", "stations", "clusters", "records", "record_share", "q", "ssc", "ssl", "span", "resolutions"],
                ["Group", "Sources", "Stations", "Clusters", "Records", "Share", "Q", "SSC", "SSL", "Span", "Resolutions"],
            ),
            "",
            "Classification is intentionally conservative. The generated `source_classification_template.csv` should be reviewed before using the type/group proportions as final manuscript statements.",
            "",
            "## Resolution and Variable Structure",
            "",
            "### Top Source-Resolution Rows",
            "",
            _markdown_table(
                _compact_resolution_rows(resolution_df, top_n=20),
                ["source", "resolution", "type", "stations", "clusters", "records", "global_share", "within_source", "span"],
                ["Source", "Resolution", "Type", "Stations", "Clusters", "Records", "Global share", "Within source", "Span"],
            ),
            "",
            "### Top Source-Variable Rows",
            "",
            _markdown_table(
                _compact_variable_rows(variable_df, top_n=20),
                ["source", "variable", "type", "variable_records", "source_records", "variable_share", "within_source"],
                ["Source", "Variable", "Type", "Variable records", "Source records", "Variable share", "Within source"],
            ),
            "",
            "The variable table helps distinguish sources that contribute dense discharge records from those that also carry SSC or SSL observations. This distinction is important because Q coverage can dominate total records even when sediment-variable coverage is much smaller.",
            "",
            "## Temporal Span by Source",
            "",
            _markdown_table(
                _compact_temporal_rows(temporal_df, top_n=15),
                ["source", "type", "span", "years", "records", "stations", "clusters", "resolutions"],
                ["Source", "Type", "Span", "Years", "Records", "Stations", "Clusters", "Resolutions"],
            ),
            "",
            "Temporal span is source-level and should be interpreted with record density. Long calendar span with few records is not equivalent to dense continuous sampling.",
        ]
    )

    lines.extend(["", "## Figures", ""])
    for label, path in figure_paths.items():
        lines.append("### {}".format(label))
        lines.append("")
        lines.append("![{}]({})".format(label, _rel_path(path, report_dir)))
        lines.append("")

    lines.extend(["## Output Tables", ""])
    for label, path in table_paths.items():
        lines.append("- `{}`: `{}`".format(label, _rel_path(path, report_dir)))

    lines.extend(
        [
            "",
            "## Notes and Limitations",
            "",
            "- Source classification is conservative and should be reviewed with `source_classification_template.csv` before manuscript use.",
            "- Climatology is summarized as a standalone release product and assigned the `climatology` resolution.",
            "- Satellite observations are summarized from the validation-only release sidecar and kept available as separate satellite figures.",
            "- Cluster counts are source-level counts and can sum to more than the unique release cluster count because several sources can contribute to the same cluster.",
            "- The report describes release-product statistics only and does not infer scientific causality.",
            "",
        ]
    )
    report_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize source-dataset contribution tables and figures for the sediment reference release."
    )
    parser.add_argument("--master-nc", default=str(DEFAULT_MASTER_NC), help="Release master NetCDF path.")
    parser.add_argument(
        "--source-station-catalog",
        default=str(DEFAULT_SOURCE_STATION_CATALOG),
        help="Release source_station_catalog.csv path used to enrich metadata.",
    )
    parser.add_argument(
        "--source-dataset-catalog",
        default=str(DEFAULT_SOURCE_DATASET_CATALOG),
        help="Existing release source_dataset_catalog.csv path used to enrich metadata.",
    )
    parser.add_argument(
        "--climatology-nc",
        default=str(DEFAULT_CLIMATOLOGY_NC),
        help="Optional release climatology NetCDF path.",
    )
    parser.add_argument(
        "--satellite-nc",
        default=str(DEFAULT_SATELLITE_NC),
        help="Optional release satellite validation-only NetCDF path.",
    )
    parser.add_argument(
        "--satellite-catalog",
        default=str(DEFAULT_SATELLITE_CATALOG),
        help="Optional release satellite_catalog.csv path used to enrich satellite metadata.",
    )
    parser.add_argument(
        "--include-climatology",
        action="store_true",
        default=True,
        help="Include standalone climatology NetCDF when present. Default: true.",
    )
    parser.add_argument(
        "--exclude-climatology",
        action="store_false",
        dest="include_climatology",
        help="Exclude standalone climatology NetCDF from contribution summaries.",
    )
    parser.add_argument(
        "--include-satellite",
        action="store_true",
        default=True,
        help="Include validation-only satellite NetCDF when present. Default: true.",
    )
    parser.add_argument(
        "--exclude-satellite",
        action="store_false",
        dest="include_satellite",
        help="Exclude validation-only satellite NetCDF from contribution summaries.",
    )
    parser.add_argument(
        "--source-classification-csv",
        default="",
        help="Optional CSV with source_name, source_type, source_group override columns.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_SOURCE_CONTRIBUTION_DIR),
        help=(
            "Output root. Default: scripts_basin_test/output_other/source_contribution. "
            "Tables go to out-dir/tables, figures to out-dir/figures, reports to out-dir/reports."
        ),
    )
    parser.add_argument(
        "--report-md",
        default="",
        help="Optional Markdown report path. Default: out-dir/reports/source_contribution_report.md.",
    )
    parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip Markdown report generation.",
    )
    parser.add_argument(
        "--write-classification-template",
        action="store_true",
        default=True,
        help="Write tables/source_classification_template.csv. Default: true.",
    )
    parser.add_argument(
        "--no-classification-template",
        action="store_false",
        dest="write_classification_template",
        help="Skip source classification template output.",
    )
    parser.add_argument("--chunk-size", type=int, default=1_000_000, help="Number of master records per processing chunk.")
    parser.add_argument("--top-n", type=int, default=20, help="Number of source datasets shown in each figure.")
    args = parser.parse_args()

    master_stats, master_resolution_stats, time_units, time_calendar = summarize_master_nc(
        Path(args.master_nc),
        chunk_size=args.chunk_size,
    )

    satellite_stats: Dict[str, Dict] = defaultdict(_empty_stats)
    satellite_resolution_stats: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)
    satellite_units = time_units
    satellite_calendar = time_calendar
    if args.include_satellite:
        satellite_stats, satellite_resolution_stats, satellite_units, satellite_calendar = summarize_satellite_nc(
            Path(args.satellite_nc),
            chunk_size=args.chunk_size,
        )

    clim_stats: Dict[str, Dict] = defaultdict(_empty_stats)
    clim_resolution_stats: Dict[Tuple[str, str], Dict] = defaultdict(_empty_stats)
    clim_units = time_units
    clim_calendar = time_calendar
    if args.include_climatology:
        clim_stats, clim_resolution_stats, clim_units, clim_calendar = summarize_climatology_nc(Path(args.climatology_nc))

    stats = merge_stats_dicts(master_stats, satellite_stats)
    stats = merge_stats_dicts(stats, clim_stats)
    resolution_stats = merge_stats_dicts(master_resolution_stats, satellite_resolution_stats)
    resolution_stats = merge_stats_dicts(resolution_stats, clim_resolution_stats)
    # If sidecars have their own time axis, keep their numeric values but use
    # master time metadata only when sidecars did not provide anything parseable.
    if satellite_stats and (time_units == "days since 1970-01-01"):
        time_units = satellite_units or time_units
        time_calendar = satellite_calendar or time_calendar
    if clim_stats and (time_units == "days since 1970-01-01"):
        time_units = clim_units or time_units
        time_calendar = clim_calendar or time_calendar

    classification_csv = Path(args.source_classification_csv) if args.source_classification_csv else None
    metadata_paths = [Path(args.source_station_catalog), Path(args.source_dataset_catalog), Path(args.satellite_catalog)]
    dataset_df = stats_to_dataset_frame(stats, time_units, time_calendar, metadata_paths)
    dataset_df = apply_source_classification(dataset_df, classification_csv)
    dataset_df = dataset_df.reindex(columns=DATASET_COLUMNS).sort_values(
        ["n_records", "source_name"],
        ascending=[False, True],
    )

    type_df = build_type_frame(dataset_df)
    resolution_df = build_resolution_frame(resolution_stats, dataset_df, time_units, time_calendar)
    variable_df = build_variable_frame(dataset_df)
    top_df = build_top_contributors_frame(dataset_df, top_n=args.top_n)
    cumulative_df = build_cumulative_frame(dataset_df)
    temporal_df = build_temporal_frame(dataset_df)
    key_metrics_df = build_key_metrics_frame(dataset_df, cumulative_df)
    classification_template_df = build_classification_template(dataset_df)

    satellite_dataset_df = pd.DataFrame(columns=DATASET_COLUMNS)
    satellite_resolution_df = pd.DataFrame(columns=RESOLUTION_COLUMNS)
    satellite_variable_df = pd.DataFrame(columns=VARIABLE_COLUMNS)
    satellite_temporal_df = pd.DataFrame(columns=TEMPORAL_COLUMNS)
    if args.include_satellite:
        (
            satellite_dataset_df,
            satellite_resolution_df,
            satellite_variable_df,
            satellite_temporal_df,
        ) = prepare_product_frames(
            satellite_stats,
            satellite_resolution_stats,
            satellite_units,
            satellite_calendar,
            [Path(args.satellite_catalog)],
            classification_csv,
            "satellite",
            "satellite products",
        )

    climatology_dataset_df = pd.DataFrame(columns=DATASET_COLUMNS)
    climatology_resolution_df = pd.DataFrame(columns=RESOLUTION_COLUMNS)
    climatology_variable_df = pd.DataFrame(columns=VARIABLE_COLUMNS)
    climatology_temporal_df = pd.DataFrame(columns=TEMPORAL_COLUMNS)
    if args.include_climatology:
        (
            climatology_dataset_df,
            climatology_resolution_df,
            climatology_variable_df,
            climatology_temporal_df,
        ) = prepare_product_frames(
            clim_stats,
            clim_resolution_stats,
            clim_units,
            clim_calendar,
            metadata_paths,
            classification_csv,
            "climatology",
            "global compilations",
        )

    out_dir = Path(args.out_dir)
    table_dir = out_dir / "tables"
    figure_dir = out_dir / "figures"
    report_dir = out_dir / "reports"
    table_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    dataset_out = table_dir / "table_source_dataset_contribution.csv"
    type_out = table_dir / "table_source_type_contribution.csv"
    resolution_out = table_dir / "table_source_resolution_contribution.csv"
    variable_out = table_dir / "table_source_variable_contribution.csv"
    top_out = table_dir / "table_top_source_contributors.csv"
    cumulative_out = table_dir / "table_source_contribution_cumulative.csv"
    temporal_out = table_dir / "table_source_temporal_coverage.csv"
    key_metrics_out = table_dir / "table_report_key_metrics.csv"
    classification_template_out = table_dir / "source_classification_template.csv"
    dataset_df.to_csv(dataset_out, index=False)
    type_df.to_csv(type_out, index=False)
    resolution_df.to_csv(resolution_out, index=False)
    variable_df.to_csv(variable_out, index=False)
    top_df.to_csv(top_out, index=False)
    cumulative_df.to_csv(cumulative_out, index=False)
    temporal_df.to_csv(temporal_out, index=False)
    key_metrics_df.to_csv(key_metrics_out, index=False)
    if args.write_classification_template:
        classification_template_df.to_csv(classification_template_out, index=False)

    figure_paths = {
        "Source contribution by records": figure_dir / "fig_source_contribution_records.png",
        "Source contribution by source stations": figure_dir / "fig_source_contribution_stations.png",
        "Source contribution by clusters": figure_dir / "fig_source_contribution_clusters.png",
        "Source type contribution by records": figure_dir / "fig_source_type_records.png",
        "Source group contribution by records": figure_dir / "fig_source_group_records.png",
        "Source resolution contribution": figure_dir / "fig_source_resolution_stacked.png",
        "Source variable coverage": figure_dir / "fig_source_variable_stacked.png",
        "Cumulative source contribution": figure_dir / "fig_source_cumulative_contribution.png",
        "Temporal coverage": figure_dir / "fig_source_temporal_coverage.png",
    }
    if HAS_MATPLOTLIB:
        write_bar_figure(dataset_df, "n_records", figure_paths["Source contribution by records"], "Source dataset contribution by records", top_n=args.top_n)
        write_bar_figure(dataset_df, "n_source_stations", figure_paths["Source contribution by source stations"], "Source dataset contribution by source stations", top_n=args.top_n)
        write_bar_figure(dataset_df, "n_clusters", figure_paths["Source contribution by clusters"], "Source dataset contribution by clusters", top_n=args.top_n)
        write_category_bar_figure(type_df, "source_type", figure_paths["Source type contribution by records"], "Source type contribution by records")
        write_category_bar_figure(type_df, "source_group", figure_paths["Source group contribution by records"], "Source group contribution by records")
        write_stacked_source_figure(
            resolution_df,
            "resolution",
            "n_records",
            figure_paths["Source resolution contribution"],
            "Source contribution by resolution",
            top_n=args.top_n,
            category_order=["daily", "monthly", "annual", "climatology", "other"],
        )
        write_stacked_source_figure(
            variable_df,
            "variable",
            "n_variable_records",
            figure_paths["Source variable coverage"],
            "Source variable coverage by valid records",
            top_n=args.top_n,
            category_order=["Q", "SSC", "SSL"],
        )
        write_cumulative_figure(cumulative_df, figure_paths["Cumulative source contribution"])
        write_temporal_coverage_figure(temporal_df, figure_paths["Temporal coverage"], top_n=args.top_n)
    else:
        print("Warning: matplotlib is unavailable; skipping source-contribution figures: {}".format(MATPLOTLIB_IMPORT_ERROR), file=sys.stderr)
    if HAS_MATPLOTLIB and args.include_satellite:
        figure_paths.update(
            write_product_figure_set(
                "satellite",
                "satellite",
                figure_dir,
                satellite_dataset_df,
                satellite_resolution_df,
                satellite_variable_df,
                satellite_temporal_df,
                top_n=args.top_n,
            )
        )
    if HAS_MATPLOTLIB and args.include_climatology:
        figure_paths.update(
            write_product_figure_set(
                "climatology",
                "climatology",
                figure_dir,
                climatology_dataset_df,
                climatology_resolution_df,
                climatology_variable_df,
                climatology_temporal_df,
                top_n=args.top_n,
            )
        )

    table_paths = {
        "table_source_dataset_contribution.csv": dataset_out,
        "table_source_type_contribution.csv": type_out,
        "table_source_resolution_contribution.csv": resolution_out,
        "table_source_variable_contribution.csv": variable_out,
        "table_top_source_contributors.csv": top_out,
        "table_source_contribution_cumulative.csv": cumulative_out,
        "table_source_temporal_coverage.csv": temporal_out,
        "table_report_key_metrics.csv": key_metrics_out,
    }
    if args.write_classification_template:
        table_paths["source_classification_template.csv"] = classification_template_out

    report_path = Path(args.report_md) if args.report_md else (report_dir / "source_contribution_report.md")
    if not args.no_report:
        write_markdown_report(
            report_path,
            dataset_df,
            type_df,
            resolution_df,
            variable_df,
            cumulative_df,
            temporal_df,
            key_metrics_df,
            table_paths,
            figure_paths,
            {
                "master_nc": Path(args.master_nc),
                "satellite_nc": Path(args.satellite_nc),
                "satellite_catalog": Path(args.satellite_catalog),
                "climatology_nc": Path(args.climatology_nc),
                "source_station_catalog": Path(args.source_station_catalog),
                "source_dataset_catalog": Path(args.source_dataset_catalog),
            },
        )

    for path in table_paths.values():
        print("Wrote {}".format(path))
    if HAS_MATPLOTLIB:
        for path in figure_paths.values():
            print("Wrote {}".format(path))
    else:
        print("Skipped figure writing because matplotlib is unavailable.")
    if not args.no_report:
        print("Wrote {}".format(report_path))
    # =============================================================================
    # Copy outputs to the docs/reports directory
    # =============================================================================
    reports_dir = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/docs/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Copy CSV tables
    for table_path in table_paths.values():
        if table_path.is_file():
            shutil.copy2(str(table_path), str(reports_dir / table_path.name))

    # Copy markdown report
    if not args.no_report and report_path.is_file():
        shutil.copy2(str(report_path), str(reports_dir / report_path.name))

    # Copy figures if directory exists
    if figure_dir.is_dir():
        figs_dst = reports_dir / "figures"
        if figs_dst.is_dir():
            shutil.rmtree(str(figs_dst))
        shutil.copytree(str(figure_dir), str(figs_dst))

    print(f"Copied all outputs (tables, report, figures) -> {reports_dir}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
