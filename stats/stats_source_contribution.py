#!/usr/bin/env python3
"""
Summarize source-dataset contributions for the sediment reference release.

This script is intended to run after s8_publish_reference_dataset.py.  It reads
release-level provenance from sed_reference_master.nc and, when available,
source_station_catalog.csv / source_dataset_catalog.csv.  It writes manuscript-
ready contribution tables and simple bar figures.

Default outputs are written under the release directory:

  tables/table_source_dataset_contribution.csv
  tables/table_source_type_contribution.csv
  figures/fig_source_contribution_records.png
  figures/fig_source_contribution_stations.png

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
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - runtime dependency check
    nc4 = None

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

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
    RELEASE_SOURCE_DATASET_CATALOG_CSV,
    RELEASE_SOURCE_STATION_CATALOG_CSV,
    get_output_r_root,
)

PROJECT_ROOT = get_output_r_root(REPO_SCRIPT_DIR)

DEFAULT_RELEASE_DIR = PROJECT_ROOT / RELEASE_DATASET_DIR
DEFAULT_MASTER_NC = PROJECT_ROOT / RELEASE_MASTER_NC
DEFAULT_CLIMATOLOGY_NC = PROJECT_ROOT / RELEASE_CLIMATOLOGY_NC
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


def _get_dim_size(ds, candidates: Iterable[str]) -> int:
    for name in candidates:
        if name in ds.dimensions:
            return len(ds.dimensions[name])
    return 0


def summarize_master_nc(master_nc: Path, chunk_size: int = 1_000_000) -> Tuple[Dict[str, Dict], str, str]:
    if nc4 is None:
        raise RuntimeError("netCDF4 is required. Please install netCDF4 before running this script.")
    master_nc = Path(master_nc)
    if not master_nc.is_file():
        raise FileNotFoundError("Missing master NetCDF: {}".format(master_nc))

    stats_by_source: Dict[str, Dict] = defaultdict(_empty_stats)

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
                first_idx = int(sub["source_station_index"].iloc[0])
                if 0 <= first_idx < len(station_to_source_meta):
                    meta = station_to_source_meta[first_idx]
                    for field in ("source_long_name", "institution", "reference", "source_url"):
                        _update_min_text(stats, field, meta.get(field, ""))
                stats["source_stations"].update(_clean_text(v) or str(i) for v, i in zip(sub["source_station_uid"], sub["source_station_index"]))
                stats["clusters"].update(_clean_text(v) for v in sub["cluster_uid"] if _clean_text(v))
                stats["resolutions"].update(_clean_text(v) for v in sub["resolution"] if _clean_text(v))
                stats["n_records"] += int(len(sub))
                stats["n_Q_records"] += int(sub["has_q"].sum())
                stats["n_SSC_records"] += int(sub["has_ssc"].sum())
                stats["n_SSL_records"] += int(sub["has_ssl"].sum())
                valid_time = pd.to_numeric(sub["time_num"], errors="coerce")
                valid_time = valid_time[np.isfinite(valid_time)]
                if len(valid_time):
                    stats["time_min"] = min(stats["time_min"], float(valid_time.min()))
                    stats["time_max"] = max(stats["time_max"], float(valid_time.max()))

    return stats_by_source, time_units, time_calendar


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


def summarize_climatology_nc(climatology_nc: Path) -> Tuple[Dict[str, Dict], str, str]:
    """Best-effort climatology summary.

    The release climatology file is intentionally separate from the basin
    cluster mainline, so n_clusters is usually zero here unless cluster_uid is
    explicitly present in the climatology NetCDF.
    """
    empty: Dict[str, Dict] = defaultdict(_empty_stats)
    if nc4 is None:
        return empty, "days since 1970-01-01", "gregorian"
    climatology_nc = Path(climatology_nc)
    if not climatology_nc.is_file():
        return empty, "days since 1970-01-01", "gregorian"

    stats_by_source: Dict[str, Dict] = defaultdict(_empty_stats)
    with nc4.Dataset(climatology_nc, "r") as ds:
        n_stations = _get_dim_size(ds, ["n_stations", "station", "stations"])
        if n_stations <= 0:
            return empty, "days since 1970-01-01", "gregorian"

        uid_name = _pick_existing_var(ds, ["station_uid", "source_station_uid", "source_station_id"])
        source_name_var = _pick_existing_var(ds, ["source_name", "source", "dataset", "source_dataset"])
        path_var = _pick_existing_var(ds, ["source_station_path", "source_station_paths", "path"])
        cluster_var = _pick_existing_var(ds, ["cluster_uid", "cluster_id"])

        station_uids = _read_text_var(ds, uid_name, size=n_stations) if uid_name else ["CLIM{:06d}".format(i) for i in range(n_stations)]
        paths = _read_text_var(ds, path_var, size=n_stations) if path_var else [""] * n_stations
        if source_name_var:
            source_names = _read_text_var(ds, source_name_var, size=n_stations)
        else:
            source_names = [_derive_source_from_path(path) for path in paths]
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
    return stats_by_source, time_units, time_calendar


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
    ]
    return " ".join(_clean_text(v) for v in fields).lower()


def classify_source(row: pd.Series) -> Tuple[str, str]:
    """Conservative fallback classifier for manuscript grouping."""
    text = _source_text(row)
    resolutions = _clean_text(row.get("resolutions", "")).lower()

    if "climatology" in resolutions and not any(res in resolutions for res in ["daily", "monthly", "annual"]):
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
        for source_name, stats in mapping.items():
            target = out[source_name]
            target["source_name"] = source_name
            for field in ["source_long_name", "institution", "reference", "source_url"]:
                _update_min_text(target, field, stats.get(field, ""))
            target["source_stations"].update(stats.get("source_stations", set()))
            target["clusters"].update(stats.get("clusters", set()))
            target["resolutions"].update(stats.get("resolutions", set()))
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
        "--source-classification-csv",
        default="",
        help="Optional CSV with source_name, source_type, source_group override columns.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Output root. Tables go to out-dir/tables and figures to out-dir/figures.",
    )
    parser.add_argument("--chunk-size", type=int, default=1_000_000, help="Number of master records per processing chunk.")
    parser.add_argument("--top-n", type=int, default=20, help="Number of source datasets shown in each figure.")
    args = parser.parse_args()

    master_stats, time_units, time_calendar = summarize_master_nc(Path(args.master_nc), chunk_size=args.chunk_size)

    if args.include_climatology:
        clim_stats, clim_units, clim_calendar = summarize_climatology_nc(Path(args.climatology_nc))
        # If climatology has its own time axis, keep its numeric values but use master time
        # metadata only when climatology did not provide anything parseable. In normal use,
        # the master and climatology products both use days since 1970-01-01.
        stats = merge_stats_dicts(master_stats, clim_stats)
        if clim_stats and (time_units == "days since 1970-01-01"):
            time_units = clim_units or time_units
            time_calendar = clim_calendar or time_calendar
    else:
        stats = master_stats

    metadata_paths = [Path(args.source_station_catalog), Path(args.source_dataset_catalog)]
    dataset_df = stats_to_dataset_frame(stats, time_units, time_calendar, metadata_paths)
    classification_csv = Path(args.source_classification_csv) if args.source_classification_csv else None
    dataset_df = apply_source_classification(dataset_df, classification_csv)
    dataset_df = dataset_df.reindex(columns=DATASET_COLUMNS).sort_values(
        ["n_records", "source_name"],
        ascending=[False, True],
    )

    type_df = build_type_frame(dataset_df)

    out_dir = Path(args.out_dir)
    table_dir = out_dir / "tables"
    figure_dir = out_dir / "figures"
    table_dir.mkdir(parents=True, exist_ok=True)
    figure_dir.mkdir(parents=True, exist_ok=True)

    dataset_out = table_dir / "table_source_dataset_contribution.csv"
    type_out = table_dir / "table_source_type_contribution.csv"
    dataset_df.to_csv(dataset_out, index=False)
    type_df.to_csv(type_out, index=False)

    write_bar_figure(
        dataset_df,
        "n_records",
        figure_dir / "fig_source_contribution_records.png",
        "Source dataset contribution by records",
        top_n=args.top_n,
    )
    write_bar_figure(
        dataset_df,
        "n_source_stations",
        figure_dir / "fig_source_contribution_stations.png",
        "Source dataset contribution by source stations",
        top_n=args.top_n,
    )

    print("Wrote {}".format(dataset_out))
    print("Wrote {}".format(type_out))
    print("Wrote {}".format(figure_dir / "fig_source_contribution_records.png"))
    print("Wrote {}".format(figure_dir / "fig_source_contribution_stations.png"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
