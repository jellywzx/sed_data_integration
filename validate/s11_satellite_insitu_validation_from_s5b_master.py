#!/usr/bin/env python3
"""Validate satellite source records against main station-reference records using s5b linkage CSV.

This script is standalone; it inlines the release-validation helpers it needs.

Key difference
--------------
The satellite side is not read from ``sed_reference_satellite.nc``. Instead, this
script treats the s5b satellite-to-main-cluster linkage CSV as the authoritative
linkage catalogue and reads satellite observations directly from their source
NetCDF files.

The accelerated s5b v2 links identify satellite rows by ``satellite_key`` and
do not carry source paths, so this script recovers ``path`` and satellite
source metadata from ``s5_basin_clustered_stations.csv`` via ``--s5-csv``.

The in-situ side follows the original s11 logic and uses selected records from
``sed_reference_master.nc``. Normalization, temporal pairing, stratification,
metrics, figures, and summary helpers are defined locally below.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import sys
import time as time_module
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import xarray as xr
except ImportError as exc:  # pragma: no cover
    raise SystemExit("xarray is required: {}".format(exc))


# -----------------------------------------------------------------------------
# Inlined release-validation helpers (cluster-schema version)
# -----------------------------------------------------------------------------

VARIABLES = ("Q", "SSC", "SSL")
WINDOW_DAYS = {"exact": 0, "pm1d": 1, "pm2d": 2}
WINDOW_EXCLUSIVE = False
MASTER_FILE = "sed_reference_master.nc"
DEFAULT_WORKERS = max(1, min(32, os.cpu_count() or 1))
DEFAULT_HIGH_TURBIDITY_SSC = 1000.0
DEFAULT_SSC_BIN_EDGES = (100.0, 500.0, 1000.0, 5000.0)
METHOD_NOTES_BASE = (
    "satellite/reach-scale vs in-situ validation; satellite records are anchors; "
    "pairing windows are cumulative"
)
ASSUMPTIONS_BASE = (
    "source_family is derived from source NetCDF observation_type when available, "
    "then taxonomy/source metadata fallbacks; missing river width is 'missing'; "
    "missing climate zone is 'unknown'"
)
RESOLUTION_CODE = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology"}
LINKED_RESOLUTIONS = frozenset(RESOLUTION_CODE.values())
OBSERVATION_TYPE_ATTRS = ("observation_type", "Type", "type")
SOURCE_PATH_COLUMNS = (
    "source_station_paths",
    "source_path",
    "candidate_path",
    "path",
    "nc_path",
    "file_path",
)
DAILY_MEAN_SOURCE_KEYS = frozenset((
    "usgs", "usgs_nwis", "hydat", "bayern", "shashi_jianli", "shashi", "jianli",
))
DAILY_DISCRETE_SOURCE_KEYS = frozenset((
    "gfqa", "gfqa_v2", "global_flow_and_water_quality_archive_v2",
    "glorise", "glorise_v1_1", "hybam", "rhine", "nerc", "myanmar",
))
DAILY_FROM_HIGHFREQ_SOURCE_KEYS = frozenset((
    "robotham", "fukushima", "mekong", "mekong_delta",
))
MONTHLY_PRODUCT_SOURCE_KEYS = frozenset((
    "eurasian_river", "eusedcollab", "eusedcollab_dataset", "gsed", "dethier",
))
MONTHLY_DAILY_MIN_COVERAGE = 0.70
MONTHLY_DAILY_MIN_VALID_DAYS = 20
ANNUAL_MONTHLY_MIN_COVERAGE = 0.75
ANNUAL_MONTHLY_MIN_VALID_MONTHS = 9
TEMPORAL_AUDIT_COLUMNS = [
    "cluster_uid", "cluster_id", "satellite_source", "satellite_source_station_uid",
    "satellite_record_id", "satellite_source_resolution", "linked_resolution",
    "satellite_time_support_class", "variable", "satellite_period_start",
    "satellite_period_end", "audit_reason", "candidate_sources",
    "n_candidate_records", "n_values_aggregated", "coverage_fraction",
    "temporal_alignment_mode",
]


def log_progress(message: str) -> None:
    stamp = time_module.strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] {}".format(stamp, message), flush=True)


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in ("nan", "none", "nat") else text


def _lower_lookup(columns: Iterable[str]) -> Dict[str, str]:
    return {str(col).lower(): str(col) for col in columns}


def _first_existing(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    column_set = {str(col) for col in columns}
    for name in candidates:
        if name in column_set:
            return name
    lower = _lower_lookup(columns)
    for name in candidates:
        hit = lower.get(name.lower())
        if hit is not None:
            return hit
    return None


def _decode_value_array(values) -> List[str]:
    return [_clean_text(value) for value in np.asarray(values).reshape(-1)]


def _series_from_data_array(da) -> pd.Series:
    values = da.values
    if values.dtype.kind in ("S", "U"):
        if values.ndim == 1:
            return pd.Series(_decode_value_array(values))
        if values.ndim == 2:
            return pd.Series(["".join(_decode_value_array(row)).strip() for row in values])
    try:
        return pd.Series(np.ma.asarray(values).filled(np.nan).reshape(-1))
    except Exception:
        return pd.Series(np.asarray(values).reshape(-1))


def _find_record_dim(ds) -> Optional[str]:
    if "n_records" in ds.sizes:
        return "n_records"
    for var_name in VARIABLES + ("time", "date", "resolution", "is_overlap"):
        if var_name in ds.variables:
            dims = tuple(ds[var_name].dims)
            if len(dims) == 1:
                return dims[0]
    one_dim_vars = [
        tuple(da.dims)[0]
        for da in ds.variables.values()
        if len(tuple(da.dims)) == 1
    ]
    if one_dim_vars:
        return str(pd.Series(one_dim_vars).value_counts().index[0])
    return None


def _open_dataset_compat(path: Path):
    errors = []
    for engine in (None, "h5netcdf"):
        try:
            kwargs = {"decode_times": False, "mask_and_scale": True}
            if engine is not None:
                kwargs["engine"] = engine
            return xr.open_dataset(path, **kwargs)
        except Exception as exc:
            label = "default" if engine is None else engine
            errors.append("{}: {}".format(label, exc))
    raise RuntimeError(
        "cannot open dataset {}; tried {}".format(path, "; ".join(errors))
    )


def parse_ssc_bin_edges(text: str) -> Tuple[float, ...]:
    values = []
    for token in str(text).split(","):
        token = token.strip()
        if token:
            values.append(float(token))
    if not values:
        return DEFAULT_SSC_BIN_EDGES
    return tuple(sorted(set(values)))


def _format_edge(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return "{:g}".format(value)


def _bin_label(value: float, edges: Sequence[float]) -> str:
    if not np.isfinite(value):
        return "missing"
    sorted_edges = list(edges)
    if value < sorted_edges[0]:
        return "<{}".format(_format_edge(sorted_edges[0]))
    for left, right in zip(sorted_edges[:-1], sorted_edges[1:]):
        if left <= value < right:
            upper = (
                _format_edge(right - 1)
                if float(right).is_integer()
                else "<{}".format(_format_edge(right))
            )
            return "{}-{}".format(_format_edge(left), upper)
    return ">={}".format(_format_edge(sorted_edges[-1]))


def _family_key(value) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _source_key_matches(value, keys: Iterable[str]) -> bool:
    key = _family_key(value)
    if not key:
        return False
    compact = key.replace("_", "")
    for candidate in keys:
        candidate_key = _family_key(candidate)
        if not candidate_key:
            continue
        candidate_compact = candidate_key.replace("_", "")
        if (
            key == candidate_key
            or candidate_key in key
            or key in candidate_key
            or compact == candidate_compact
            or candidate_compact in compact
            or compact in candidate_compact
        ):
            return True
    return False


def classify_time_support_class(
    source: str,
    resolution: str,
    source_family: str = "",
    variable: str = "",
) -> str:
    resolution = _normalize_resolution(resolution)
    source_text = _clean_text(source)
    key = _family_key(source_text)
    variable = _clean_text(variable).upper()
    if resolution == "monthly":
        if _source_key_matches(key, MONTHLY_PRODUCT_SOURCE_KEYS):
            return "monthly_product"
        return "monthly_unknown"
    if resolution == "annual":
        return "annual_product"
    if resolution != "daily":
        return "unknown"
    if "bayern" in key and ("raw" in key or "sensor" in key):
        return "daily_from_highfreq"
    if _source_key_matches(key, DAILY_FROM_HIGHFREQ_SOURCE_KEYS):
        return "daily_from_highfreq"
    if _source_key_matches(key, DAILY_DISCRETE_SOURCE_KEYS):
        return "daily_discrete"
    if _source_key_matches(key, DAILY_MEAN_SOURCE_KEYS):
        return "daily_mean"
    normalized_family = _normalize_family_label(source_family)
    if normalized_family == "satellite":
        return "daily_satellite"
    return "daily_unknown"


def _period_bounds_for_resolution(value, resolution: str) -> Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    timestamp = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(timestamp):
        return pd.NaT, pd.NaT, pd.NaT
    timestamp = pd.Timestamp(timestamp).floor("D")
    resolution = _normalize_resolution(resolution)
    if resolution == "monthly":
        start = pd.Timestamp(year=timestamp.year, month=timestamp.month, day=1)
        end = start + pd.offsets.MonthEnd(0)
    elif resolution == "annual":
        start = pd.Timestamp(year=timestamp.year, month=1, day=1)
        end = pd.Timestamp(year=timestamp.year, month=12, day=31)
    else:
        start = timestamp
        end = timestamp
    center = start + (end - start) / 2
    return start, end, center


def _add_temporal_metadata(out: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    source_resolution_col = _first_existing(
        raw.columns,
        ("source_resolution", "satellite_resolution", "native_resolution", "original_resolution"),
    )
    linked_resolution_col = _first_existing(
        raw.columns,
        ("linked_resolution", "s5b_linked_resolution", "target_resolution"),
    )
    if source_resolution_col is not None:
        out["source_resolution"] = raw[source_resolution_col].map(_normalize_resolution)
    else:
        out["source_resolution"] = out["resolution"].map(_normalize_resolution)
    if linked_resolution_col is not None:
        linked = raw[linked_resolution_col].map(_normalize_resolution)
        out["linked_resolution"] = linked.where(linked.astype(str).str.strip().ne(""), out["resolution"])
    else:
        out["linked_resolution"] = out["resolution"].map(_normalize_resolution)
    out["source_resolution"] = out["source_resolution"].where(
        out["source_resolution"].astype(str).str.strip().ne(""),
        out["resolution"],
    )
    out["linked_resolution"] = out["linked_resolution"].where(
        out["linked_resolution"].astype(str).str.strip().ne(""),
        out["resolution"],
    )
    out["is_cross_resolution"] = (
        out["source_resolution"].map(_normalize_resolution)
        != out["linked_resolution"].map(_normalize_resolution)
    )
    support_cache: Dict[Tuple[str, str, str], str] = {}
    support_values = []
    for source, resolution, family in zip(
        out["source"], out["source_resolution"], out["source_family"]
    ):
        key = (_clean_text(source), _normalize_resolution(resolution), _clean_text(family))
        support = support_cache.get(key)
        if support is None:
            support = classify_time_support_class(key[0], key[1], key[2])
            support_cache[key] = support
        support_values.append(support)
    out["time_support_class"] = support_values
    time_values = pd.to_datetime(out["time"], errors="coerce").dt.floor("D")
    source_resolution = out["source_resolution"].map(_normalize_resolution)
    out["period_start"] = time_values
    out["period_end"] = time_values
    monthly = source_resolution.eq("monthly") & time_values.notna()
    if monthly.any():
        month_start = time_values.loc[monthly].dt.to_period("M").dt.to_timestamp()
        out.loc[monthly, "period_start"] = month_start
        out.loc[monthly, "period_end"] = month_start + pd.offsets.MonthEnd(0)
    annual = source_resolution.eq("annual") & time_values.notna()
    if annual.any():
        years = time_values.loc[annual].dt.year
        out.loc[annual, "period_start"] = pd.to_datetime(years.astype(str) + "-01-01")
        out.loc[annual, "period_end"] = pd.to_datetime(years.astype(str) + "-12-31")
    out["period_center"] = out["period_start"] + (out["period_end"] - out["period_start"]) / 2
    return out


def _normalize_family_label(value) -> str:
    text = _clean_text(value)
    key = _family_key(text)
    if not key:
        return ""
    if key in (
        "satellite", "satellite_data", "satellite_station", "satellite_derived",
        "satellite_derived_data", "remote_sensing", "remote", "reach_scale",
        "reachscale", "reach_scale_station",
    ):
        return "satellite"
    if key in (
        "climatology", "clim", "climatological", "climate",
        "global_compilation", "compilation", "literature_compilation",
    ):
        return "climatology"
    if key in (
        "in_situ", "insitu", "in_situ_data", "in_situ_station",
        "in_situ_station_data", "station", "station_data", "field",
        "field_observation", "field_observations", "gauge", "gauge_station",
        "observational", "monitoring_station", "monitoring_network",
    ):
        return "in_situ"
    if key in ("usgs", "hydat", "grdc", "hybam", "gfqa", "gfqa_v2"):
        return "in_situ"
    if key in ("secondary", "secondary_compilation", "compiled"):
        return "secondary_compilation"
    if key in ("model", "modeled", "modelled"):
        return "model"
    if key in ("other", "unknown"):
        return "other"
    low = text.lower()
    if any(token in low for token in (
        "satellite", "remote sensing", "remote-sensing", "landsat", "reach-scale"
    )):
        return "satellite"
    if any(token in low for token in (
        "in-situ", "in situ", "insitu", "station data", "gauge", "monitoring network"
    )):
        return "in_situ"
    if any(token in low for token in ("compiled", "compilation", "secondary")):
        return "secondary_compilation"
    return ""


SOURCE_FAMILY_BY_DATASET_KEY = {
    "riversed": "satellite",
    "river_sed": "satellite",
    "river_sed_aquasat": "satellite",
    "aquasat": "satellite",
    "gsed": "satellite",
    "gsed_dataset": "satellite",
    "dethier": "satellite",
    "dethier_glacier_fed_rivers_dataset": "satellite",
    "gfqa": "in_situ",
    "gfqa_v2": "in_situ",
    "global_flow_and_water_quality_archive_v2": "in_situ",
    "usgs": "in_situ",
    "usgs_nwis": "in_situ",
    "hydat": "in_situ",
    "hydat_dataset": "in_situ",
    "milliman": "climatology",
    "milliman_farnsworth_global_river_sediment_database": "climatology",
    "vanmaercke": "climatology",
    "vanmaercke_et_al_2014_african_sediment_yield_database": "climatology",
    "eusedcollab": "in_situ",
    "eusedcollab_dataset": "in_situ",
    "ali_de_boer": "climatology",
    "ali_de_boer_dataset": "climatology",
    "hma": "climatology",
    "hma_dataset": "climatology",
    "robotham": "in_situ",
    "robotham_dataset": "in_situ",
    "myanmar": "in_situ",
    "myanmar_irrawaddy_and_salween_rivers": "in_situ",
    "shashi_jianli": "in_situ",
    "shashi_jianli_dataset": "in_situ",
    "bayern": "in_situ",
    "bayern_state_environmental_agency_lfu_river_monitoring_network": "in_situ",
    "huanghe": "in_situ",
    "yellow_river": "in_situ",
    "yajiang": "in_situ",
    "fukushima": "in_situ",
    "glorise": "in_situ",
    "grdc": "in_situ",
    "hybam": "in_situ",
}


def _family_from_dataset_text(value) -> str:
    key = _family_key(value)
    if not key:
        return ""
    family = SOURCE_FAMILY_BY_DATASET_KEY.get(key)
    if family:
        return family
    for dataset_key, candidate_family in SOURCE_FAMILY_BY_DATASET_KEY.items():
        if dataset_key and (dataset_key in key or key in dataset_key):
            return candidate_family
    low = _clean_text(value).lower()
    compact = key.replace("_", "")
    if any(token in low for token in ("riversed", "river sed", "gsed", "dethier", "aquasat")):
        return "satellite"
    if any(token in low for token in ("satellite", "remote sensing", "remote-sensing", "landsat", "reach-scale")):
        return "satellite"
    if "reachscale" in compact:
        return "satellite"
    if any(token in low for token in ("usgs", "hydat", "grdc", "hybam", "gfqa", "milliman", "vanmaercke", "eusedcollab")):
        return "in_situ"
    if any(token in low for token in ("robotham", "myanmar", "shashi", "jianli", "bayern", "hma", "ali", "de boer", "yajiang", "huanghe", "fukushima", "glorise")):
        return "in_situ"
    if any(token in low for token in ("in situ", "in-situ", "insitu", "gauge", "field observation", "monitoring network")):
        return "in_situ"
    if any(token in low for token in ("compiled", "compilation", "secondary")):
        return "secondary_compilation"
    return ""


def load_source_taxonomy(path: Optional[Path] = None) -> Dict[str, str]:
    overrides: Dict[str, str] = {}
    if path is None:
        return overrides
    if not path.exists():
        raise FileNotFoundError("source taxonomy CSV not found: {}".format(path))
    table = pd.read_csv(path, keep_default_na=False)
    source_col = _first_existing(
        table.columns, ("source", "source_name", "dataset", "source_dataset")
    )
    family_col = _first_existing(
        table.columns, ("family", "source_family", "class", "source_class")
    )
    if source_col is None or family_col is None:
        raise ValueError("taxonomy CSV must contain source and family/source_family columns")
    for _, row in table.iterrows():
        source = _clean_text(row.get(source_col, ""))
        family = _normalize_family_label(row.get(family_col, ""))
        if source and family:
            overrides[source.lower()] = family
    return overrides


def classify_source_family(
    source: str,
    overrides: Optional[Dict[str, str]] = None,
    raw_family: str = "",
    observation_type: str = "",
) -> str:
    if overrides is None:
        overrides = {}
    source_text = _clean_text(source)
    override = overrides.get(source_text.lower())
    if override:
        return override
    normalized_observation = _normalize_family_label(observation_type)
    if normalized_observation and normalized_observation != "other":
        return normalized_observation
    normalized_raw = _normalize_family_label(raw_family)
    if normalized_raw and normalized_raw != "other":
        return normalized_raw
    for value in (source_text, raw_family):
        family = _family_from_dataset_text(value)
        if family:
            return family
    return normalized_observation or normalized_raw or "other"


def _attr_value_to_text(value) -> str:
    try:
        arr = np.asarray(value)
        if arr.shape:
            parts = [_clean_text(item) for item in arr.reshape(-1)]
            return " ".join(part for part in parts if part)
    except Exception:
        pass
    return _clean_text(value)


def _read_nc_global_attrs(path: Path) -> Dict[str, str]:
    try:
        import h5py  # type: ignore

        with h5py.File(str(path), "r") as handle:
            return {
                str(key): _attr_value_to_text(value)
                for key, value in handle.attrs.items()
            }
    except Exception:
        pass
    ds = None
    try:
        ds = _open_dataset_compat(path)
        return {
            str(key): _attr_value_to_text(value)
            for key, value in getattr(ds, "attrs", {}).items()
        }
    except Exception:
        return {}
    finally:
        if ds is not None:
            ds.close()


def _first_attr(attrs: Dict[str, str], names: Sequence[str]) -> str:
    if not attrs:
        return ""
    lower = {str(key).lower(): str(key) for key in attrs}
    for name in names:
        key = lower.get(str(name).lower())
        if key is not None:
            value = _clean_text(attrs.get(key, ""))
            if value:
                return value
    return ""


def _split_path_list(value) -> List[str]:
    text = _clean_text(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[|;,]", text) if part.strip()]


def _source_root_candidates(release_dir: Path) -> List[Path]:
    roots = [release_dir, release_dir.parent, PROJECT_DIR, PROJECT_DIR.parent]
    for root_base in (
        PROJECT_DIR.parent.parent,
        release_dir.parent,
        release_dir.parent.parent,
        release_dir.parent.parent.parent,
    ):
        roots.append(root_base / "output_resolution_organized")
        roots.append(root_base / "Output_r" / "output_resolution_organized")
    unique = []
    seen = set()
    for root in roots:
        key = str(root)
        if key not in seen:
            seen.add(key)
            unique.append(root)
    return unique


def _resolve_source_nc_path(value, release_dir: Path) -> Optional[Path]:
    for token in _split_path_list(value):
        candidate = Path(token).expanduser()
        if candidate.is_absolute() and candidate.exists():
            return candidate
        for root in _source_root_candidates(release_dir):
            path = (root / candidate).resolve()
            if path.exists():
                return path
    return None


def _row_source_nc_path(row: pd.Series, release_dir: Path) -> Optional[Path]:
    for col in SOURCE_PATH_COLUMNS:
        if col in row.index:
            path = _resolve_source_nc_path(row.get(col, ""), release_dir)
            if path is not None:
                return path
    return None


def _read_observation_type_worker(item: Tuple[str, str]) -> Tuple[str, str]:
    key, path_text = item
    attrs = _read_nc_global_attrs(Path(path_text))
    return key, _first_attr(attrs, OBSERVATION_TYPE_ATTRS)


def add_observation_type_from_source_attrs(
    raw: pd.DataFrame,
    release_dir: Path,
    workers: int = 1,
    progress=log_progress,
) -> pd.DataFrame:
    if raw.empty:
        return raw
    path_cols = [col for col in SOURCE_PATH_COLUMNS if col in raw.columns]
    if not path_cols:
        return raw

    out = raw.copy()
    resolved_paths = []
    path_lookup: Dict[str, Path] = {}
    for _, row in out[path_cols].iterrows():
        path = _row_source_nc_path(row, release_dir)
        key = str(path) if path is not None else ""
        resolved_paths.append(key)
        if path is not None:
            path_lookup[key] = path
    if not path_lookup:
        return out

    observation_by_path: Dict[str, str] = {}
    tasks = [(key, str(path)) for key, path in sorted(path_lookup.items())]
    workers = max(1, int(workers or 1))
    if progress:
        progress(
            "Reading observation_type attrs from {} source NetCDF files with {} worker(s)".format(
                len(tasks), workers
            )
        )
    if workers == 1 or len(tasks) <= 1:
        for task in tasks:
            key, observation_type = _read_observation_type_worker(task)
            observation_by_path[key] = observation_type
    else:
        chunksize = max(1, min(200, int(math.ceil(len(tasks) / float(workers * 8)))))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for key, observation_type in executor.map(
                _read_observation_type_worker, tasks, chunksize=chunksize
            ):
                observation_by_path[key] = observation_type

    attr_observation = (
        pd.Series(resolved_paths, index=out.index)
        .map(observation_by_path)
        .fillna("")
        .map(_clean_text)
    )
    existing_col = _first_existing(out.columns, OBSERVATION_TYPE_ATTRS)
    if "observation_type" not in out.columns:
        out["observation_type"] = (
            out[existing_col].map(_clean_text) if existing_col else ""
        )
    else:
        out["observation_type"] = out["observation_type"].map(_clean_text)
    missing = out["observation_type"].astype(str).str.strip().eq("")
    out.loc[missing, "observation_type"] = attr_observation[missing]
    if progress:
        populated = int(attr_observation.astype(str).str.strip().ne("").sum())
        progress(
            "Loaded observation_type from {} source NetCDF files; populated {} observation rows".format(
                len(path_lookup), populated
            )
        )
    return out


def _coerce_datetime_from_columns(df: pd.DataFrame) -> pd.Series:
    date_col = _first_existing(
        df.columns, ("date", "datetime", "timestamp", "obs_date", "observation_date")
    )
    if date_col is not None:
        parsed = pd.to_datetime(df[date_col], errors="coerce")
        if parsed.notna().any():
            return parsed.dt.floor("D")
    time_col = _first_existing(df.columns, ("time", "obs_time", "observation_time"))
    if time_col is None:
        return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    raw = df[time_col]
    numeric = pd.to_numeric(raw, errors="coerce")
    if numeric.notna().any() and pd.api.types.is_numeric_dtype(raw):
        return pd.to_datetime(
            numeric, unit="D", origin="unix", errors="coerce"
        ).dt.floor("D")
    parsed = pd.to_datetime(raw, errors="coerce")
    if parsed.notna().any():
        return parsed.dt.floor("D")
    return pd.to_datetime(
        numeric, unit="D", origin="unix", errors="coerce"
    ).dt.floor("D")


def _parse_cf_days_since(values: pd.Series, units: str) -> pd.Series:
    match = re.search(
        r"days\s+since\s+([0-9]{4}-[0-9]{2}-[0-9]{2})",
        str(units),
        flags=re.I,
    )
    origin = match.group(1) if match else "1970-01-01"
    numeric = pd.to_numeric(values, errors="coerce")
    return pd.to_datetime(
        numeric, unit="D", origin=pd.Timestamp(origin), errors="coerce"
    )


def _extract_column(
    df: pd.DataFrame, candidates: Sequence[str], default=""
) -> pd.Series:
    col = _first_existing(df.columns, candidates)
    if col is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[col]


def _flag_series(df: pd.DataFrame, variable: str) -> pd.Series:
    col = _first_existing(
        df.columns,
        (
            "{}_flag".format(variable),
            "{}_qc_flag".format(variable),
            "{}_quality_flag".format(variable),
            "{}_flag".format(variable.lower()),
            "flag", "qc_flag", "quality_flag",
        ),
    )
    if col is None:
        return pd.Series([np.nan] * len(df), index=df.index)
    return df[col]


def normalize_observation_table(
    raw: pd.DataFrame,
    taxonomy_overrides: Optional[Dict[str, str]] = None,
    input_mode: str = "",
) -> pd.DataFrame:
    if taxonomy_overrides is None:
        taxonomy_overrides = {}
    if raw.empty:
        return pd.DataFrame()

    out = pd.DataFrame(index=raw.index)
    record_id_col = _first_existing(
        raw.columns, ("record_id", "record_index", "candidate_id", "row_id")
    )
    out["record_id"] = (
        raw[record_id_col].astype(str)
        if record_id_col
        else np.arange(len(raw)).astype(str)
    )
    out["cluster_uid"] = _extract_column(
        raw, ("cluster_uid", "station_uid", "cluster_uuid"), ""
    )
    out["cluster_id"] = _extract_column(
        raw, ("cluster_id", "station_id", "master_station_index", "station_index"), ""
    )
    out["resolution"] = _extract_column(
        raw, ("resolution", "time_resolution", "temporal_resolution"), ""
    ).map(_normalize_resolution)
    out["time"] = _coerce_datetime_from_columns(raw)
    if "date" not in raw.columns and "_time_units" in raw.columns and "time" in raw.columns:
        out["time"] = _parse_cf_days_since(
            raw["time"], _clean_text(raw["_time_units"].iloc[0])
        ).dt.floor("D")

    out["source_station_uid"] = _extract_column(
        raw,
        ("source_station_uid", "station_uid", "source_station_id", "source_station_native_id"),
        "",
    ).map(_clean_text)
    source_col = _first_existing(
        raw.columns, ("source", "source_name", "source_dataset", "dataset", "dataset_name")
    )
    family_col = _first_existing(
        raw.columns, ("source_family", "source_type", "source_category", "family")
    )
    observation_col = _first_existing(raw.columns, OBSERVATION_TYPE_ATTRS)
    out["source"] = raw[source_col].map(_clean_text) if source_col else ""
    missing_source = out["source"].astype(str).str.strip().eq("")
    out.loc[missing_source, "source"] = out.loc[missing_source, "source_station_uid"]
    raw_family = (
        raw[family_col].map(_clean_text)
        if family_col
        else pd.Series([""] * len(raw), index=raw.index)
    )
    raw_observation = (
        raw[observation_col].map(_clean_text)
        if observation_col
        else pd.Series([""] * len(raw), index=raw.index)
    )
    out["observation_type"] = raw_observation
    out["source_family"] = [
        classify_source_family(source, taxonomy_overrides, family, observation)
        for source, family, observation in zip(
            out["source"], raw_family, raw_observation
        )
    ]

    for variable in VARIABLES:
        col = _first_existing(raw.columns, (variable, variable.lower()))
        out[variable] = (
            pd.to_numeric(raw[col], errors="coerce") if col is not None else np.nan
        )
        out["{}_flag".format(variable)] = _flag_series(raw, variable)

    for canonical, candidates in (
        ("river_width_class", ("river_width_class", "width_class", "river_width_category")),
        ("river_width_m", ("river_width_m", "width_m", "river_width", "bankfull_width_m")),
        ("climate_zone", ("climate_zone", "hydroatlas_climate_zone", "koppen_zone", "koppen", "climate_class")),
    ):
        col = _first_existing(raw.columns, candidates)
        if col is not None:
            out[canonical] = raw[col]

    out = _add_temporal_metadata(out, raw)
    out["input_mode"] = input_mode
    has_cluster = (
        out["cluster_uid"].astype(str).str.strip().ne("")
        | out["cluster_id"].astype(str).str.strip().ne("")
    )
    has_core = (
        has_cluster
        & out["resolution"].astype(str).str.strip().ne("")
        & out["time"].notna()
    )
    has_source = out["source"].astype(str).str.strip().ne("")
    return out[has_core & has_source].reset_index(drop=True)


def _flag_rank(value) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        return float(numeric)
    text = _clean_text(value).lower()
    if not text:
        return 9.0
    if text in ("good", "valid", "pass", "passed", "ok", "a"):
        return 0.0
    if text in ("suspect", "questionable", "estimated", "estimate", "b"):
        return 2.0
    if text in ("bad", "fail", "failed", "invalid", "reject", "rejected"):
        return 9.0
    return 5.0


def _groupby_compat(df: pd.DataFrame, by_cols: Sequence[str]):
    try:
        return df.groupby(list(by_cols), dropna=False)
    except TypeError:
        return df.groupby(list(by_cols))


def _cluster_group_key(df: pd.DataFrame) -> pd.Series:
    uid = df["cluster_uid"].astype(str).str.strip()
    cid = df["cluster_id"].astype(str).str.strip()
    return uid.where(uid.ne(""), cid)


def _method_notes(input_mode: str) -> str:
    return "{}; input_mode={}; window_exclusive={}".format(
        METHOD_NOTES_BASE, input_mode, str(WINDOW_EXCLUSIVE).lower()
    )


def _pair_output_columns() -> List[str]:
    return [
        "cluster_uid", "cluster_id", "resolution", "variable", "pairing_window",
        "window_exclusive", "satellite_time", "insitu_time", "time_delta_days",
        "satellite_source", "insitu_source", "satellite_source_family",
        "insitu_source_family", "satellite_source_station_uid",
        "insitu_source_station_uid", "satellite_record_id", "insitu_record_id",
        "satellite_value", "insitu_value", "diff_satellite_minus_insitu",
        "pct_error_vs_insitu", "satellite_flag", "insitu_flag", "source_pair",
        "satellite_ssc", "insitu_ssc", "satellite_river_width_class",
        "insitu_river_width_class", "satellite_river_width_m",
        "insitu_river_width_m", "satellite_climate_zone", "insitu_climate_zone",
        "method_notes", "assumptions", "temporal_alignment_mode",
        "satellite_time_support_class", "insitu_time_support_class",
        "satellite_period_start", "satellite_period_end",
        "insitu_period_start", "insitu_period_end",
        "n_insitu_values_aggregated", "insitu_coverage_fraction",
        "is_cross_resolution",
    ]


def _row_time_support_class(row: pd.Series, variable: str = "") -> str:
    return classify_time_support_class(
        row.get("source", ""),
        row.get("source_resolution", row.get("resolution", "")),
        row.get("source_family", ""),
        variable=variable,
    )


def _timestamp_or_nat(value) -> pd.Timestamp:
    parsed = pd.to_datetime(pd.Series([value]), errors="coerce").iloc[0]
    return pd.NaT if pd.isna(parsed) else pd.Timestamp(parsed)


def _format_sources(values: Iterable[object]) -> str:
    return "|".join(sorted({clean for clean in (_clean_text(value) for value in values) if clean}))


def _worst_flag_value(values: pd.Series):
    if values.empty:
        return np.nan
    ranks = values.map(_flag_rank)
    if ranks.empty:
        return np.nan
    return values.iloc[int(np.argmax(ranks.to_numpy(dtype=float)))]


def _base_pair_record(
    sat: pd.Series,
    insitu_source: str,
    insitu_source_family: str,
    insitu_source_station_uid: str,
    insitu_record_id: str,
    variable: str,
    pairing_window: str,
    satellite_time,
    insitu_time,
    time_delta_days: int,
    satellite_value: float,
    insitu_value: float,
    satellite_flag,
    insitu_flag,
    insitu_ssc,
    insitu_river_width_class,
    insitu_river_width_m,
    insitu_climate_zone,
    temporal_alignment_mode: str,
    satellite_time_support_class: str,
    insitu_time_support_class: str,
    satellite_period_start,
    satellite_period_end,
    insitu_period_start,
    insitu_period_end,
    n_insitu_values_aggregated: int,
    insitu_coverage_fraction: float,
    is_cross_resolution: bool,
    method_notes: str,
) -> Dict[str, object]:
    diff = satellite_value - insitu_value
    pct = diff / insitu_value * 100.0 if insitu_value != 0 else float("nan")
    return {
        "cluster_uid": sat.get("cluster_uid", ""),
        "cluster_id": sat.get("cluster_id", ""),
        "resolution": sat.get("resolution", ""),
        "variable": variable,
        "pairing_window": pairing_window,
        "window_exclusive": WINDOW_EXCLUSIVE,
        "satellite_time": satellite_time,
        "insitu_time": insitu_time,
        "time_delta_days": int(time_delta_days),
        "satellite_source": sat.get("source", ""),
        "insitu_source": insitu_source,
        "satellite_source_family": sat.get("source_family", ""),
        "insitu_source_family": insitu_source_family,
        "satellite_source_station_uid": sat.get("source_station_uid", ""),
        "insitu_source_station_uid": insitu_source_station_uid,
        "satellite_record_id": sat.get("record_id", ""),
        "insitu_record_id": insitu_record_id,
        "satellite_value": satellite_value,
        "insitu_value": insitu_value,
        "diff_satellite_minus_insitu": diff,
        "pct_error_vs_insitu": pct,
        "satellite_flag": satellite_flag,
        "insitu_flag": insitu_flag,
        "source_pair": "{} vs {}".format(sat.get("source", ""), insitu_source),
        "satellite_ssc": sat.get("SSC", np.nan),
        "insitu_ssc": insitu_ssc,
        "satellite_river_width_class": sat.get("river_width_class", ""),
        "insitu_river_width_class": insitu_river_width_class,
        "satellite_river_width_m": sat.get("river_width_m", np.nan),
        "insitu_river_width_m": insitu_river_width_m,
        "satellite_climate_zone": sat.get("climate_zone", ""),
        "insitu_climate_zone": insitu_climate_zone,
        "method_notes": method_notes,
        "assumptions": ASSUMPTIONS_BASE,
        "temporal_alignment_mode": temporal_alignment_mode,
        "satellite_time_support_class": satellite_time_support_class,
        "insitu_time_support_class": insitu_time_support_class,
        "satellite_period_start": satellite_period_start,
        "satellite_period_end": satellite_period_end,
        "insitu_period_start": insitu_period_start,
        "insitu_period_end": insitu_period_end,
        "n_insitu_values_aggregated": int(n_insitu_values_aggregated),
        "insitu_coverage_fraction": insitu_coverage_fraction,
        "is_cross_resolution": bool(is_cross_resolution),
    }


def _temporal_audit_record(
    sat: pd.Series,
    variable: str,
    reason: str,
    candidate_sources: str = "",
    n_candidate_records: int = 0,
    n_values_aggregated: int = 0,
    coverage_fraction: float = math.nan,
    temporal_alignment_mode: str = "",
) -> Dict[str, object]:
    return {
        "cluster_uid": sat.get("cluster_uid", ""),
        "cluster_id": sat.get("cluster_id", ""),
        "satellite_source": sat.get("source", ""),
        "satellite_source_station_uid": sat.get("source_station_uid", ""),
        "satellite_record_id": sat.get("record_id", ""),
        "satellite_source_resolution": sat.get("source_resolution", sat.get("resolution", "")),
        "linked_resolution": sat.get("linked_resolution", sat.get("resolution", "")),
        "satellite_time_support_class": _row_time_support_class(sat, variable),
        "variable": variable,
        "satellite_period_start": sat.get("period_start", pd.NaT),
        "satellite_period_end": sat.get("period_end", pd.NaT),
        "audit_reason": reason,
        "candidate_sources": candidate_sources,
        "n_candidate_records": int(n_candidate_records),
        "n_values_aggregated": int(n_values_aggregated),
        "coverage_fraction": coverage_fraction,
        "temporal_alignment_mode": temporal_alignment_mode,
    }


def _aggregate_daily_candidates_for_period(
    candidates: pd.DataFrame,
    variable: str,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
) -> List[Dict[str, object]]:
    if candidates.empty:
        return []
    flag_col = "{}_flag".format(variable)
    in_period = candidates[
        (candidates["_time_day"] >= period_start)
        & (candidates["_time_day"] <= period_end)
        & pd.to_numeric(candidates[variable], errors="coerce").notna()
    ].copy()
    if in_period.empty:
        return []
    period_days = max(1, int((period_end - period_start).days) + 1)
    aggregates: List[Dict[str, object]] = []
    group_cols = ["source", "source_station_uid"]
    for (_, _), group in _groupby_compat(in_period, group_cols):
        values = pd.to_numeric(group[variable], errors="coerce").dropna()
        if values.empty:
            continue
        n_days = int(group.loc[values.index, "_time_day"].nunique())
        coverage = float(n_days) / float(period_days)
        passes_coverage = (
            coverage >= MONTHLY_DAILY_MIN_COVERAGE
            or n_days >= MONTHLY_DAILY_MIN_VALID_DAYS
        )
        aggregates.append(
            {
                "source": group["source"].iloc[0],
                "source_family": group["source_family"].iloc[0],
                "source_station_uid": group["source_station_uid"].iloc[0],
                "record_id": "|".join(group.loc[values.index, "record_id"].astype(str).head(5)),
                "value": float(values.mean()),
                "flag": _worst_flag_value(group.loc[values.index, flag_col]) if flag_col in group else np.nan,
                "flag_rank": max(_flag_rank(value) for value in group.loc[values.index, flag_col]) if flag_col in group else 9.0,
                "ssc": (
                    float(pd.to_numeric(group.loc[values.index, "SSC"], errors="coerce").mean())
                    if "SSC" in group and pd.to_numeric(group.loc[values.index, "SSC"], errors="coerce").notna().any()
                    else np.nan
                ),
                "river_width_class": group.get("river_width_class", pd.Series([""])).iloc[0],
                "river_width_m": group.get("river_width_m", pd.Series([np.nan])).iloc[0],
                "climate_zone": group.get("climate_zone", pd.Series([""])).iloc[0],
                "n_days": n_days,
                "coverage": coverage,
                "passes_coverage": passes_coverage,
                "time_support_class": _row_time_support_class(group.iloc[0], variable),
            }
        )
    return aggregates


def _aggregate_monthly_satellite_for_annual_period(
    candidates: pd.DataFrame,
    variable: str,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
) -> Optional[Dict[str, object]]:
    if candidates.empty:
        return None
    flag_col = "{}_flag".format(variable)
    in_period = candidates[
        (pd.to_datetime(candidates["period_start"], errors="coerce") >= period_start)
        & (pd.to_datetime(candidates["period_start"], errors="coerce") <= period_end)
        & pd.to_numeric(candidates[variable], errors="coerce").notna()
    ].copy()
    if in_period.empty:
        return None
    values = pd.to_numeric(in_period[variable], errors="coerce").dropna()
    if values.empty:
        return None
    n_months = int(pd.to_datetime(in_period.loc[values.index, "period_start"], errors="coerce").dt.to_period("M").nunique())
    coverage = float(n_months) / 12.0
    passes_coverage = (
        coverage >= ANNUAL_MONTHLY_MIN_COVERAGE
        or n_months >= ANNUAL_MONTHLY_MIN_VALID_MONTHS
    )
    return {
        "record_id": "|".join(in_period.loc[values.index, "record_id"].astype(str).head(5)),
        "value": float(values.mean()),
        "flag": _worst_flag_value(in_period.loc[values.index, flag_col]) if flag_col in in_period else np.nan,
        "ssc": (
            float(pd.to_numeric(in_period.loc[values.index, "SSC"], errors="coerce").mean())
            if "SSC" in in_period and pd.to_numeric(in_period.loc[values.index, "SSC"], errors="coerce").notna().any()
            else np.nan
        ),
        "n_months": n_months,
        "coverage": coverage,
        "passes_coverage": passes_coverage,
    }


def _pair_monthly_satellite_year_to_annual_variable(
    sat_year_group: pd.DataFrame,
    insitu: pd.DataFrame,
    variable: str,
    method_notes: str,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    rows: List[Dict[str, object]] = []
    audits: List[Dict[str, object]] = []
    if sat_year_group.empty:
        return rows, audits
    sat = sat_year_group.sort_values(["period_start", "record_id"], kind="mergesort").iloc[0].copy()
    first_period = _timestamp_or_nat(sat.get("period_start", pd.NaT))
    if pd.isna(first_period):
        audits.append(_temporal_audit_record(sat, variable, "missing_satellite_period"))
        return rows, audits
    year = int(first_period.year)
    annual_start = pd.Timestamp(year=year, month=1, day=1)
    annual_end = pd.Timestamp(year=year, month=12, day=31)
    annual_center = annual_start + (annual_end - annual_start) / 2
    sat_aggregate = _aggregate_monthly_satellite_for_annual_period(
        sat_year_group, variable, annual_start, annual_end
    )
    if sat_aggregate is None:
        audits.append(_temporal_audit_record(sat, variable, "no_satellite_values", temporal_alignment_mode="monthly_to_annual_aggregate"))
        return rows, audits
    if not sat_aggregate["passes_coverage"]:
        audits.append(
            _temporal_audit_record(
                sat,
                variable,
                "insufficient_temporal_coverage",
                candidate_sources=_format_sources(sat_year_group.get("source", pd.Series(dtype=object))),
                n_candidate_records=len(sat_year_group),
                n_values_aggregated=sat_aggregate["n_months"],
                coverage_fraction=sat_aggregate["coverage"],
                temporal_alignment_mode="monthly_to_annual_aggregate",
            )
        )
        return rows, audits

    valid = insitu.loc[pd.to_numeric(insitu[variable], errors="coerce").notna()].copy()
    candidate_sources = _format_sources(valid.get("source", pd.Series(dtype=object)))
    if valid.empty:
        audits.append(_temporal_audit_record(sat, variable, "no_insitu_values", n_candidate_records=0))
        return rows, audits
    valid["variable_time_support_class"] = [
        _row_time_support_class(row, variable) for _, row in valid.iterrows()
    ]
    compatible = valid[valid["variable_time_support_class"].eq("annual_product")].copy()
    if compatible.empty:
        audits.append(
            _temporal_audit_record(
                sat,
                variable,
                "unsupported_time_support_class",
                candidate_sources=candidate_sources,
                n_candidate_records=len(valid),
                temporal_alignment_mode="monthly_to_annual_aggregate",
            )
        )
        return rows, audits
    same_period = compatible[
        pd.to_datetime(compatible["period_start"], errors="coerce").eq(annual_start)
    ].copy()
    if same_period.empty:
        audits.append(
            _temporal_audit_record(
                sat,
                variable,
                "no_matching_annual_period",
                candidate_sources=_format_sources(compatible["source"]),
                n_candidate_records=len(compatible),
                temporal_alignment_mode="monthly_to_annual_aggregate",
            )
        )
        return rows, audits
    same_period["_flag_rank"] = same_period["{}_flag".format(variable)].map(_flag_rank)
    best = same_period.sort_values(
        ["_flag_rank", "source", "source_station_uid", "record_id"],
        kind="mergesort",
    ).iloc[0]
    sat_pair = sat.copy()
    sat_pair[variable] = sat_aggregate["value"]
    sat_pair["{}_flag".format(variable)] = sat_aggregate["flag"]
    sat_pair["record_id"] = sat_aggregate["record_id"]
    sat_pair["period_start"] = annual_start
    sat_pair["period_end"] = annual_end
    sat_pair["period_center"] = annual_center
    if variable == "SSC" or pd.notna(sat_aggregate["ssc"]):
        sat_pair["SSC"] = sat_aggregate["ssc"]
    rows.append(
        _base_pair_record(
            sat_pair,
            best.get("source", ""),
            best.get("source_family", ""),
            best.get("source_station_uid", ""),
            best.get("record_id", ""),
            variable,
            "period",
            annual_center,
            best.get("period_center", annual_center),
            0,
            float(sat_aggregate["value"]),
            float(best.get(variable, np.nan)),
            sat_aggregate["flag"],
            best.get("{}_flag".format(variable), np.nan),
            best.get("SSC", np.nan),
            best.get("river_width_class", ""),
            best.get("river_width_m", np.nan),
            best.get("climate_zone", ""),
            "monthly_to_annual_aggregate",
            _row_time_support_class(sat, variable),
            _row_time_support_class(best, variable),
            annual_start,
            annual_end,
            best.get("period_start", pd.NaT),
            best.get("period_end", pd.NaT),
            1,
            1.0,
            True,
            method_notes,
        )
    )
    return rows, audits


def _pair_cross_resolution_satellite_variable(
    sat: pd.Series,
    insitu: pd.DataFrame,
    variable: str,
    method_notes: str,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    rows: List[Dict[str, object]] = []
    audits: List[Dict[str, object]] = []
    sat_value = pd.to_numeric(pd.Series([sat.get(variable, np.nan)]), errors="coerce").iloc[0]
    if pd.isna(sat_value) or not np.isfinite(float(sat_value)):
        return rows, audits

    sat_source_resolution = _normalize_resolution(sat.get("source_resolution", sat.get("resolution", "")))
    linked_resolution = _normalize_resolution(sat.get("linked_resolution", sat.get("resolution", "")))
    is_cross_resolution = bool(sat_source_resolution and linked_resolution and sat_source_resolution != linked_resolution)
    sat_support = _row_time_support_class(sat, variable)
    sat_start = _timestamp_or_nat(sat.get("period_start", pd.NaT))
    sat_end = _timestamp_or_nat(sat.get("period_end", pd.NaT))
    sat_center = _timestamp_or_nat(sat.get("period_center", sat.get("_time_day", pd.NaT)))
    if pd.isna(sat_start) or pd.isna(sat_end):
        audits.append(_temporal_audit_record(sat, variable, "missing_satellite_period"))
        return rows, audits

    valid = insitu.loc[pd.to_numeric(insitu[variable], errors="coerce").notna()].copy()
    candidate_sources = _format_sources(valid.get("source", pd.Series(dtype=object)))
    if valid.empty:
        audits.append(_temporal_audit_record(sat, variable, "no_insitu_values", n_candidate_records=0))
        return rows, audits

    if sat_source_resolution == "monthly" and linked_resolution == "daily":
        valid["variable_time_support_class"] = [
            _row_time_support_class(row, variable) for _, row in valid.iterrows()
        ]
        compatible = valid[
            valid["variable_time_support_class"].isin(["daily_mean", "daily_from_highfreq"])
        ].copy()
        if compatible.empty:
            reason = (
                "discrete_sampling_not_comparable"
                if valid["variable_time_support_class"].eq("daily_discrete").any()
                else "unsupported_time_support_class"
            )
            audits.append(
                _temporal_audit_record(
                    sat,
                    variable,
                    reason,
                    candidate_sources=candidate_sources,
                    n_candidate_records=len(valid),
                    temporal_alignment_mode="monthly_to_daily_aggregate",
                )
            )
            return rows, audits
        aggregates = _aggregate_daily_candidates_for_period(
            compatible, variable, sat_start, sat_end
        )
        passed = [item for item in aggregates if item["passes_coverage"]]
        if not passed:
            best_coverage = max([item["coverage"] for item in aggregates], default=math.nan)
            best_n = max([item["n_days"] for item in aggregates], default=0)
            audits.append(
                _temporal_audit_record(
                    sat,
                    variable,
                    "insufficient_temporal_coverage",
                    candidate_sources=_format_sources(compatible["source"]),
                    n_candidate_records=len(compatible),
                    n_values_aggregated=best_n,
                    coverage_fraction=best_coverage,
                    temporal_alignment_mode="monthly_to_daily_aggregate",
                )
            )
            return rows, audits
        best = sorted(
            passed,
            key=lambda item: (
                -int(item["n_days"]),
                -float(item["coverage"]),
                float(item["flag_rank"]),
                str(item["source"]).lower(),
                str(item["source_station_uid"]).lower(),
            ),
        )[0]
        rows.append(
            _base_pair_record(
                sat,
                best["source"],
                best["source_family"],
                best["source_station_uid"],
                best["record_id"],
                variable,
                "period",
                sat_center,
                sat_center,
                0,
                float(sat_value),
                float(best["value"]),
                sat.get("{}_flag".format(variable), np.nan),
                best["flag"],
                best["ssc"],
                best["river_width_class"],
                best["river_width_m"],
                best["climate_zone"],
                "monthly_to_daily_aggregate",
                sat_support,
                best["time_support_class"],
                sat_start,
                sat_end,
                sat_start,
                sat_end,
                best["n_days"],
                best["coverage"],
                is_cross_resolution,
                method_notes,
            )
        )
        return rows, audits

    if sat_source_resolution == "monthly" and linked_resolution == "monthly":
        valid["variable_time_support_class"] = [
            _row_time_support_class(row, variable) for _, row in valid.iterrows()
        ]
        compatible = valid[valid["variable_time_support_class"].eq("monthly_product")].copy()
        if compatible.empty:
            audits.append(
                _temporal_audit_record(
                    sat,
                    variable,
                    "unsupported_time_support_class",
                    candidate_sources=candidate_sources,
                    n_candidate_records=len(valid),
                    temporal_alignment_mode="monthly_to_monthly_period",
                )
            )
            return rows, audits
        same_period = compatible[
            pd.to_datetime(compatible["period_start"], errors="coerce").eq(sat_start)
        ].copy()
        if same_period.empty:
            audits.append(
                _temporal_audit_record(
                    sat,
                    variable,
                    "no_matching_monthly_period",
                    candidate_sources=_format_sources(compatible["source"]),
                    n_candidate_records=len(compatible),
                    temporal_alignment_mode="monthly_to_monthly_period",
                )
            )
            return rows, audits
        same_period["_flag_rank"] = same_period["{}_flag".format(variable)].map(_flag_rank)
        best = same_period.sort_values(
            ["_flag_rank", "source", "source_station_uid", "record_id"],
            kind="mergesort",
        ).iloc[0]
        rows.append(
            _base_pair_record(
                sat,
                best.get("source", ""),
                best.get("source_family", ""),
                best.get("source_station_uid", ""),
                best.get("record_id", ""),
                variable,
                "period",
                sat_center,
                best.get("period_center", sat_center),
                0,
                float(sat_value),
                float(best.get(variable, np.nan)),
                sat.get("{}_flag".format(variable), np.nan),
                best.get("{}_flag".format(variable), np.nan),
                best.get("SSC", np.nan),
                best.get("river_width_class", ""),
                best.get("river_width_m", np.nan),
                best.get("climate_zone", ""),
                "monthly_to_monthly_period",
                sat_support,
                _row_time_support_class(best, variable),
                sat_start,
                sat_end,
                best.get("period_start", pd.NaT),
                best.get("period_end", pd.NaT),
                1,
                1.0,
                is_cross_resolution,
                method_notes,
            )
        )
        return rows, audits

    audits.append(
        _temporal_audit_record(
            sat,
            variable,
            "unsupported_resolution_transition",
            candidate_sources=candidate_sources,
            n_candidate_records=len(valid),
            temporal_alignment_mode="unsupported",
        )
    )
    return rows, audits


def _pair_group_worker(
    item: Tuple[int, pd.DataFrame, Tuple[str, ...], str]
) -> Tuple[int, List[Dict[str, object]], List[Dict[str, object]]]:
    ordinal, group, windows, input_mode = item
    rows: List[Dict[str, object]] = []
    audits: List[Dict[str, object]] = []
    satellites = group[group["source_family"] == "satellite"]
    insitu = group[group["source_family"] == "in_situ"]
    if satellites.empty or insitu.empty:
        return ordinal, rows, audits

    insitu_per_var = {}
    for variable in VARIABLES:
        flag_col = "{}_flag".format(variable)
        valid = insitu.loc[pd.to_numeric(insitu[variable], errors="coerce").notna()]
        if valid.empty:
            continue
        insitu_per_var[variable] = valid.assign(
            _flag_rank=valid[flag_col].map(_flag_rank),
            _source_sort=valid["source"].astype(str).str.lower(),
            _uid_sort=valid["source_station_uid"].astype(str).str.lower(),
            _record_sort=valid["record_id"].astype(str),
        )

    method_notes = _method_notes(input_mode)
    satellite_source_resolution = satellites["source_resolution"].map(_normalize_resolution)
    satellite_linked_resolution = satellites["linked_resolution"].map(_normalize_resolution)
    monthly_to_annual_mask = satellite_source_resolution.eq("monthly") & satellite_linked_resolution.eq("annual")
    if monthly_to_annual_mask.any():
        annual_satellites = satellites.loc[monthly_to_annual_mask].copy()
        annual_satellites["_period_year"] = pd.to_datetime(
            annual_satellites["period_start"], errors="coerce"
        ).dt.year
        for variable in VARIABLES:
            valid_satellites = annual_satellites.loc[
                pd.to_numeric(annual_satellites[variable], errors="coerce").notna()
                & annual_satellites["_period_year"].notna()
            ].copy()
            if valid_satellites.empty:
                continue
            for _, sat_year_group in _groupby_compat(
                valid_satellites,
                ["source", "source_station_uid", "_period_year"],
            ):
                annual_rows, annual_audits = _pair_monthly_satellite_year_to_annual_variable(
                    sat_year_group, insitu, variable, method_notes
                )
                rows.extend(annual_rows)
                audits.extend(annual_audits)
    satellites_for_row_loop = satellites.loc[~monthly_to_annual_mask].copy()

    for _, sat in satellites_for_row_loop.iterrows():
        sat_cluster_uid = sat.get("cluster_uid", "")
        sat_cluster_id = sat.get("cluster_id", "")
        sat_resolution = sat.get("resolution", "")
        sat_time = sat["_time_day"]
        sat_source = sat.get("source", "")
        sat_source_family = sat.get("source_family", "")
        sat_source_station_uid = sat.get("source_station_uid", "")
        sat_record_id = sat.get("record_id", "")
        sat_ssc = sat.get("SSC", np.nan)
        sat_river_width_class = sat.get("river_width_class", "")
        sat_river_width_m = sat.get("river_width_m", np.nan)
        sat_climate_zone = sat.get("climate_zone", "")
        sat_source_resolution = _normalize_resolution(sat.get("source_resolution", sat_resolution))
        sat_linked_resolution = _normalize_resolution(sat.get("linked_resolution", sat_resolution))
        is_cross_resolution = bool(sat_source_resolution and sat_linked_resolution and sat_source_resolution != sat_linked_resolution)

        if is_cross_resolution or (sat_source_resolution == "monthly" and sat_linked_resolution == "monthly"):
            for variable in VARIABLES:
                cross_rows, cross_audits = _pair_cross_resolution_satellite_variable(
                    sat, insitu, variable, method_notes
                )
                rows.extend(cross_rows)
                audits.extend(cross_audits)
            continue

        for variable in VARIABLES:
            valid = insitu_per_var.get(variable)
            if valid is None:
                continue
            try:
                sat_value = float(sat.get(variable, np.nan))
            except (ValueError, TypeError):
                sat_value = np.nan
            if not np.isfinite(sat_value):
                continue
            deltas = (valid["_time_day"] - sat_time).dt.days
            candidates = valid.assign(
                _time_delta_days=deltas,
                _abs_delta=deltas.abs(),
            ).sort_values(
                ["_abs_delta", "_flag_rank", "_source_sort", "_uid_sort", "_time_day", "_record_sort"],
                kind="mergesort",
            )
            for window in windows:
                max_days = WINDOW_DAYS[window]
                match = (
                    candidates[candidates["_abs_delta"] == 0]
                    if window == "exact"
                    else candidates[candidates["_abs_delta"] <= max_days]
                )
                if match.empty:
                    continue
                best = match.iloc[0]
                try:
                    insitu_value = float(best.get(variable, np.nan))
                except (ValueError, TypeError):
                    insitu_value = np.nan
                rows.append(
                    _base_pair_record(
                        sat,
                        best.get("source", ""),
                        best.get("source_family", ""),
                        best.get("source_station_uid", ""),
                        best.get("record_id", ""),
                        variable,
                        window,
                        sat_time,
                        best["_time_day"],
                        int(best["_time_delta_days"]),
                        sat_value,
                        insitu_value,
                        sat.get("{}_flag".format(variable), np.nan),
                        best.get("{}_flag".format(variable), np.nan),
                        best.get("SSC", np.nan),
                        best.get("river_width_class", ""),
                        best.get("river_width_m", np.nan),
                        best.get("climate_zone", ""),
                        "same_resolution_nearest_day",
                        _row_time_support_class(sat, variable),
                        _row_time_support_class(best, variable),
                        sat.get("period_start", sat_time),
                        sat.get("period_end", sat_time),
                        best.get("period_start", best["_time_day"]),
                        best.get("period_end", best["_time_day"]),
                        1,
                        1.0,
                        False,
                        method_notes,
                    )
                )
    return ordinal, rows, audits


def pair_satellite_insitu_records(
    observations: pd.DataFrame,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    input_mode: str = "",
    workers: int = 1,
    progress=log_progress,
    return_audit: bool = False,
):
    columns = _pair_output_columns()
    if observations.empty:
        empty_pairs = pd.DataFrame(columns=columns)
        empty_audit = pd.DataFrame(columns=TEMPORAL_AUDIT_COLUMNS)
        return (empty_pairs, empty_audit) if return_audit else empty_pairs
    work = observations.copy()
    work["_cluster_key"] = _cluster_group_key(work)
    work["_time_day"] = pd.to_datetime(work["time"], errors="coerce").dt.floor("D")
    tasks = []
    for ordinal, (_, group) in enumerate(
        _groupby_compat(work, ["_cluster_key", "resolution"])
    ):
        families = set(group["source_family"].astype(str))
        if "satellite" in families and "in_situ" in families:
            tasks.append((ordinal, group.copy(), tuple(windows), input_mode))
    workers = max(1, int(workers or 1))
    if progress:
        progress(
            "Pairing {} eligible cluster/resolution groups with {} worker(s)".format(
                len(tasks), workers
            )
        )
    if workers == 1 or len(tasks) <= 1:
        results = [_pair_group_worker(task) for task in tasks]
    else:
        chunksize = max(1, min(50, int(math.ceil(len(tasks) / float(workers * 8)))))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_pair_group_worker, tasks, chunksize=chunksize))
    rows: List[Dict[str, object]] = []
    audit_rows: List[Dict[str, object]] = []
    for _, group_rows, group_audits in sorted(results, key=lambda item: item[0]):
        rows.extend(group_rows)
        audit_rows.extend(group_audits)
    pairs = pd.DataFrame(rows, columns=columns)
    audit = pd.DataFrame(audit_rows, columns=TEMPORAL_AUDIT_COLUMNS)
    return (pairs, audit) if return_audit else pairs


def _load_external_attributes(path: Optional[Path]) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    if not path.exists():
        raise FileNotFoundError("external attributes CSV not found: {}".format(path))
    return pd.read_csv(path, keep_default_na=False)


def _merge_external_attributes(pairs: pd.DataFrame, attrs: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty or attrs.empty:
        return pairs
    work = pairs.copy()
    attr = attrs.copy()
    attr_cols = list(attr.columns)
    cluster_uid_col = _first_existing(attr_cols, ("cluster_uid", "station_uid"))
    cluster_id_col = _first_existing(attr_cols, ("cluster_id", "station_id"))
    resolution_col = _first_existing(
        attr_cols, ("resolution", "time_resolution", "temporal_resolution")
    )
    if cluster_uid_col is not None:
        left_keys = ["cluster_uid"]
        right_keys = [cluster_uid_col]
    elif cluster_id_col is not None:
        left_keys = ["cluster_id"]
        right_keys = [cluster_id_col]
    else:
        return work
    if resolution_col is not None:
        left_keys.append("resolution")
        right_keys.append(resolution_col)
    rename = {
        col: "external_{}".format(col) if col in work.columns else col
        for col in attr.columns
        if col not in right_keys
    }
    attr = attr.rename(columns=rename)
    right_keys = [rename.get(col, col) for col in right_keys]
    attr = attr.drop_duplicates(right_keys)
    return work.merge(
        attr,
        how="left",
        left_on=left_keys,
        right_on=right_keys,
        suffixes=("", "_external"),
    )


def _first_nonempty(row: pd.Series, names: Sequence[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            text = _clean_text(row.get(name, ""))
            if text:
                return text
    return default


def _first_numeric(row: pd.Series, names: Sequence[str]) -> float:
    for name in names:
        if name in row.index:
            value = pd.to_numeric(
                pd.Series([row.get(name, np.nan)]), errors="coerce"
            ).iloc[0]
            if pd.notna(value) and np.isfinite(float(value)):
                return float(value)
    return float("nan")


def _width_class_from_numeric(width: float) -> str:
    if not np.isfinite(width):
        return "missing"
    if width < 30:
        return "<30m"
    if width < 100:
        return "30-99m"
    if width < 300:
        return "100-299m"
    return ">=300m"


def assign_strata(
    pair_records: pd.DataFrame,
    external_attributes: Optional[pd.DataFrame] = None,
    high_turbidity_ssc: float = DEFAULT_HIGH_TURBIDITY_SSC,
    ssc_bin_edges: Sequence[float] = DEFAULT_SSC_BIN_EDGES,
) -> pd.DataFrame:
    if pair_records.empty:
        out = pair_records.copy()
        for col in ("ssc_bin", "river_width_class", "climate_zone", "high_turbidity"):
            if col not in out.columns:
                out[col] = []
        return out
    work = _merge_external_attributes(
        pair_records,
        external_attributes if external_attributes is not None else pd.DataFrame(),
    )
    ssc_values = []
    ssc_bins = []
    width_classes = []
    climate_zones = []
    high_turbidity = []
    for _, row in work.iterrows():
        ssc = _first_numeric(row, ("insitu_ssc", "satellite_ssc", "SSC", "external_SSC"))
        ssc_values.append(ssc)
        ssc_bins.append(_bin_label(ssc, ssc_bin_edges))
        high_turbidity.append(
            bool(np.isfinite(ssc) and ssc >= float(high_turbidity_ssc))
        )
        width_class = _first_nonempty(
            row,
            (
                "river_width_class", "external_river_width_class",
                "insitu_river_width_class", "satellite_river_width_class",
                "width_class", "external_width_class",
            ),
        )
        if not width_class:
            width = _first_numeric(
                row,
                (
                    "river_width_m", "external_river_width_m",
                    "insitu_river_width_m", "satellite_river_width_m",
                    "width_m", "external_width_m", "river_width",
                    "external_river_width",
                ),
            )
            width_class = _width_class_from_numeric(width)
        width_classes.append(width_class or "missing")
        climate = _first_nonempty(
            row,
            (
                "climate_zone", "external_climate_zone", "insitu_climate_zone",
                "satellite_climate_zone", "hydroatlas_climate_zone",
                "external_hydroatlas_climate_zone", "koppen_zone",
                "external_koppen_zone", "koppen", "external_koppen",
                "climate_class", "external_climate_class",
            ),
            default="unknown",
        )
        climate_zones.append(climate or "unknown")
    work["ssc_reference_value"] = ssc_values
    work["ssc_bin"] = ssc_bins
    work["river_width_class"] = width_classes
    work["climate_zone"] = climate_zones
    work["high_turbidity"] = high_turbidity
    return work


def _safe_corr(a: np.ndarray, b: np.ndarray, method: str) -> float:
    if len(a) < 2 or np.nanstd(a) == 0 or np.nanstd(b) == 0:
        return float("nan")
    if method == "spearman":
        left = pd.Series(a).rank(method="average")
        right = pd.Series(b).rank(method="average")
        return float(left.corr(right, method="pearson"))
    return float(pd.Series(a).corr(pd.Series(b), method="pearson"))


def _metric_values(group: pd.DataFrame) -> Dict[str, float]:
    sat = pd.to_numeric(group["satellite_value"], errors="coerce").to_numpy(dtype=float)
    insitu = pd.to_numeric(group["insitu_value"], errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(sat) & np.isfinite(insitu)
    sat = sat[valid]
    insitu = insitu[valid]
    if len(sat) == 0:
        return {
            "bias": float("nan"), "RMSE": float("nan"), "MAE": float("nan"),
            "MAPE": float("nan"), "median_absolute_error": float("nan"),
            "Pearson": float("nan"), "Spearman": float("nan"),
            "R2": float("nan"), "n_pairs": 0,
        }
    diff = sat - insitu
    mape_mask = insitu != 0
    pearson = _safe_corr(insitu, sat, "pearson")
    return {
        "bias": float(np.nanmean(diff)),
        "RMSE": float(np.sqrt(np.nanmean(diff ** 2))),
        "MAE": float(np.nanmean(np.abs(diff))),
        "MAPE": (
            float(np.nanmean(np.abs(diff[mape_mask] / insitu[mape_mask]) * 100.0))
            if np.any(mape_mask)
            else float("nan")
        ),
        "median_absolute_error": float(np.nanmedian(np.abs(diff))),
        "Pearson": pearson,
        "Spearman": _safe_corr(insitu, sat, "spearman"),
        "R2": float(pearson ** 2) if np.isfinite(pearson) else float("nan"),
        "n_pairs": int(len(sat)),
    }


def compute_satellite_insitu_metrics(pair_records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "group_type", "pairing_window", "window_exclusive", "variable",
        "source_pair", "ssc_bin", "river_width_class", "climate_zone",
        "high_turbidity", "temporal_alignment_mode",
        "satellite_time_support_class", "insitu_time_support_class",
        "is_cross_resolution", "bias", "RMSE", "MAE", "MAPE",
        "median_absolute_error", "Pearson", "Spearman", "R2", "n_pairs",
        "n_clusters", "method_notes", "assumptions",
    ]
    if pair_records.empty:
        return pd.DataFrame(columns=columns)
    pair_records = pair_records.copy()
    for name, default in (
        ("temporal_alignment_mode", "same_resolution_nearest_day"),
        ("satellite_time_support_class", "unknown"),
        ("insitu_time_support_class", "unknown"),
        ("is_cross_resolution", False),
    ):
        if name not in pair_records.columns:
            pair_records[name] = default
    group_specs = {
        "overall": [],
        "source_pair": ["source_pair"],
        "temporal_alignment": [
            "temporal_alignment_mode",
            "satellite_time_support_class",
            "insitu_time_support_class",
            "is_cross_resolution",
        ],
        "source_pair_temporal": [
            "source_pair",
            "temporal_alignment_mode",
            "satellite_time_support_class",
            "insitu_time_support_class",
            "is_cross_resolution",
        ],
        "source_pair_ssc_bin": ["source_pair", "ssc_bin"],
        "source_pair_width": ["source_pair", "river_width_class"],
        "source_pair_climate": ["source_pair", "climate_zone"],
        "source_pair_high_turbidity": ["source_pair", "high_turbidity"],
        "full_strata": ["source_pair", "ssc_bin", "river_width_class", "climate_zone", "high_turbidity"],
    }
    rows: List[Dict[str, object]] = []
    for group_type, strata_cols in group_specs.items():
        cols = ["pairing_window", "variable"] + strata_cols
        for keys, group in _groupby_compat(pair_records, cols):
            if not isinstance(keys, tuple):
                keys = (keys,)
            values = dict(zip(cols, keys))
            row = {
                "group_type": group_type,
                "pairing_window": values.get("pairing_window", ""),
                "window_exclusive": WINDOW_EXCLUSIVE,
                "variable": values.get("variable", ""),
                "source_pair": values.get("source_pair", "ALL"),
                "ssc_bin": values.get("ssc_bin", "ALL"),
                "river_width_class": values.get("river_width_class", "ALL"),
                "climate_zone": values.get("climate_zone", "ALL"),
                "high_turbidity": values.get("high_turbidity", "ALL"),
                "temporal_alignment_mode": values.get("temporal_alignment_mode", "ALL"),
                "satellite_time_support_class": values.get("satellite_time_support_class", "ALL"),
                "insitu_time_support_class": values.get("insitu_time_support_class", "ALL"),
                "is_cross_resolution": values.get("is_cross_resolution", "ALL"),
                "n_clusters": int(_cluster_group_key(group).nunique()),
                "method_notes": (
                    str(group["method_notes"].iloc[0])
                    if "method_notes" in group
                    else METHOD_NOTES_BASE
                ),
                "assumptions": (
                    str(group["assumptions"].iloc[0])
                    if "assumptions" in group
                    else ASSUMPTIONS_BASE
                ),
            }
            row.update(_metric_values(group))
            rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def _write_scatter_by_window(
    pair_records: pd.DataFrame, figures_dir: Path
) -> Tuple[str, str]:
    subset = pair_records[pair_records["variable"] == "SSC"].copy()
    rel = "figures/satellite_insitu_scatter_by_window_SSC.png"
    if len(subset) < 1:
        return rel, "skipped: no SSC pairs"
    import matplotlib.pyplot as plt  # type: ignore

    windows = [
        window
        for window in ("exact", "pm1d", "pm2d")
        if window in set(subset["pairing_window"])
    ]
    if not windows:
        return rel, "skipped: no configured windows"
    fig, axes = plt.subplots(1, len(windows), figsize=(5 * len(windows), 4), squeeze=False)
    for ax, window in zip(axes[0], windows):
        part = subset[subset["pairing_window"] == window]
        ax.scatter(part["insitu_value"], part["satellite_value"], s=14, alpha=0.65)
        finite = pd.to_numeric(
            part[["insitu_value", "satellite_value"]].stack(), errors="coerce"
        )
        finite = finite[np.isfinite(finite)]
        if len(finite):
            lo = float(finite.min())
            hi = float(finite.max())
            ax.plot([lo, hi], [lo, hi], color="black", linewidth=1, linestyle="--")
        ax.set_title("{} SSC (n={})".format(window, len(part)))
        ax.set_xlabel("in-situ SSC")
        ax.set_ylabel("satellite SSC")
        ax.grid(True, alpha=0.25)
    fig.tight_layout()
    path = figures_dir / "satellite_insitu_scatter_by_window_SSC.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return rel, "generated"


def _write_residual_by_ssc_bin(
    pair_records: pd.DataFrame, figures_dir: Path
) -> Tuple[str, str]:
    subset = pair_records[pair_records["variable"] == "SSC"].copy()
    rel = "figures/satellite_insitu_residual_by_ssc_bin.png"
    if len(subset) < 1:
        return rel, "skipped: no SSC pairs"
    import matplotlib.pyplot as plt  # type: ignore

    grouped = [
        (label, group["diff_satellite_minus_insitu"].dropna().astype(float).values)
        for label, group in subset.groupby("ssc_bin")
    ]
    grouped = [(label, values) for label, values in grouped if len(values)]
    if not grouped:
        return rel, "skipped: no finite SSC residuals"
    labels = [label for label, _ in grouped]
    data = [values for _, values in grouped]
    fig, ax = plt.subplots(figsize=(max(7, len(labels) * 1.2), 4.5))
    ax.boxplot(data, tick_labels=labels, showfliers=False)
    ax.axhline(0, color="black", linewidth=1, linestyle="--")
    ax.set_title("SSC residual by SSC bin")
    ax.set_xlabel("SSC bin")
    ax.set_ylabel("satellite - in-situ")
    ax.grid(True, axis="y", alpha=0.25)
    fig.autofmt_xdate(rotation=30, ha="right")
    fig.tight_layout()
    path = figures_dir / "satellite_insitu_residual_by_ssc_bin.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return rel, "generated"


def _write_metric_heatmap(
    metrics: pd.DataFrame, figures_dir: Path
) -> Tuple[str, str]:
    rel = "figures/satellite_insitu_metric_heatmap.png"
    subset = metrics[
        (metrics["group_type"] == "source_pair")
        & (metrics["variable"] == "SSC")
        & pd.to_numeric(metrics["RMSE"], errors="coerce").notna()
    ].copy()
    if subset.empty:
        return rel, "skipped: no SSC source-pair RMSE metrics"
    import matplotlib.pyplot as plt  # type: ignore

    pivot = subset.pivot_table(
        index="source_pair", columns="pairing_window", values="RMSE", aggfunc="first"
    )
    for window in ("exact", "pm1d", "pm2d"):
        if window not in pivot.columns:
            pivot[window] = np.nan
    pivot = pivot[["exact", "pm1d", "pm2d"]]
    fig, ax = plt.subplots(figsize=(7, max(4, len(pivot) * 0.35)))
    image = ax.imshow(pivot.values.astype(float), aspect="auto", cmap="viridis")
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_title("SSC RMSE by source pair and window")
    cbar = fig.colorbar(image, ax=ax)
    cbar.set_label("RMSE")
    fig.tight_layout()
    path = figures_dir / "satellite_insitu_metric_heatmap.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return rel, "generated"


def write_figures(
    pair_records: pd.DataFrame,
    metrics: pd.DataFrame,
    out_dir: Path,
    figure_variables: Sequence[str] = ("SSC",),
) -> List[Tuple[str, str]]:
    if "SSC" not in set(figure_variables):
        return [
            ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: SSC not requested in --figure-variables"),
            ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: SSC not requested in --figure-variables"),
            ("figures/satellite_insitu_metric_heatmap.png", "skipped: SSC not requested in --figure-variables"),
        ]
    try:
        import matplotlib  # type: ignore

        matplotlib.use("Agg")
    except Exception as exc:
        return [
            ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: matplotlib unavailable: {}".format(exc)),
            ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: matplotlib unavailable: {}".format(exc)),
            ("figures/satellite_insitu_metric_heatmap.png", "skipped: matplotlib unavailable: {}".format(exc)),
        ]
    figures_dir = out_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    return [
        _write_scatter_by_window(pair_records, figures_dir),
        _write_residual_by_ssc_bin(pair_records, figures_dir),
        _write_metric_heatmap(metrics, figures_dir),
    ]


def write_summary(
    out_path: Path,
    input_path: Optional[Path],
    input_mode: str,
    load_note: str,
    observations: pd.DataFrame,
    pair_records: pd.DataFrame,
    metrics: pd.DataFrame,
    generated_outputs: Sequence[Tuple[str, str]],
) -> None:
    lines: List[str] = []
    lines.append("# Satellite / In-Situ Validation Summary")
    lines.append("")
    lines.append("## 1. Inputs")
    lines.append("- Input mode: `{}`.".format(input_mode))
    lines.append("- Input file: `{}`.".format(input_path if input_path is not None else MASTER_FILE))
    lines.append("- Load note: {}.".format(load_note))
    lines.append("- Observation rows after normalization: {}.".format(len(observations)))
    lines.append("")
    lines.append("## 2. Method")
    lines.append("- Satellite/reach-scale records are anchors; in-situ records are selected within the same cluster and resolution.")
    lines.append("- Windows are cumulative: `exact` is included in `pm1d`, and `pm1d` is included in `pm2d`; `window_exclusive=false`.")
    lines.append("- Bias and residuals are `satellite - in-situ`; MAPE skips pairs where the in-situ denominator is zero.")
    lines.append("- R2 is `Pearson^2` when Pearson is finite.")
    lines.append("")
    lines.append("## 3. Key Results")
    lines.append("- Pair rows: {}.".format(len(pair_records)))
    if not metrics.empty:
        preview = metrics[metrics["group_type"] == "source_pair"].copy()
        if not preview.empty:
            preview = preview.sort_values(
                ["n_pairs", "pairing_window", "variable"],
                ascending=[False, True, True],
            ).head(12)
            for _, row in preview.iterrows():
                lines.append(
                    "- `{}` / `{}` / `{}`: n_pairs={}, n_clusters={}, bias={}, RMSE={}, Spearman={}.".format(
                        row.get("pairing_window", ""), row.get("variable", ""),
                        row.get("source_pair", ""), row.get("n_pairs", ""),
                        row.get("n_clusters", ""), row.get("bias", ""),
                        row.get("RMSE", ""), row.get("Spearman", ""),
                    )
                )
    else:
        lines.append("- No metric rows were generated.")
    lines.append("")
    lines.append("## 4. Limitations")
    if input_mode == "selected_master":
        lines.append("- This fallback uses only selected release records; non-selected candidate source values are not represented.")
        lines.append("- Same-day cross-source exact pairs may be undercounted because the release typically keeps one selected record per cluster, resolution, and time.")
    else:
        lines.append("- Candidate-sidecar results depend on what the sidecar preserved; if it only contains overlap candidates, wider windows may be incomplete.")
    lines.append("- Missing river width is reported as `missing`; missing climate zone is reported as `unknown`.")
    lines.append("")
    lines.append("## 5. Generated Outputs")
    for name, status in generated_outputs:
        lines.append("- `{}`: {}".format(name, status))
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


# Keep the existing base.* call sites below, but point them at this module rather
# than at validate/satellite_insitu_validation.py. This preserves behavior while
# removing the external helper dependency.
base = sys.modules[__name__]


# -----------------------------------------------------------------------------
# s11b s5b-linkage workflow
# -----------------------------------------------------------------------------

try:
    from pipeline_paths import (
        S5_BASIN_CLUSTERED_CSV,
        S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV,
    )
except ImportError:  # allows module-style execution from the validate directory
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from pipeline_paths import (  # type: ignore
        S5_BASIN_CLUSTERED_CSV,
        S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV,
    )


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_R_ROOT = Path(
    os.environ.get("OUTPUT_R_ROOT", str(PROJECT_DIR.parent))
).expanduser().resolve()

DEFAULT_LINKAGE_CSV = OUTPUT_R_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV
DEFAULT_S5_CSV = OUTPUT_R_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_SOURCE_ROOT = (OUTPUT_R_ROOT / "../output_resolution_organized").resolve()
DEFAULT_RELEASE_DIR = OUTPUT_R_ROOT / "scripts_basin_test/output/sed_reference_release"
DEFAULT_OUT_DIR = OUTPUT_R_ROOT / "scripts_basin_test/validate/output/s11_satellite_insitu"

REQUIRED_LINKAGE_COLUMNS = {
    "satellite_location_uid", "cluster_id", "cluster_uid", "source", "path",
    "resolution", "linked_cluster_id", "linked_cluster_uid",
    "linked_resolution", "link_status",
}

REQUIRED_V2_LINKAGE_COLUMNS = {
    "satellite_key", "satellite_station_id", "satellite_source",
    "satellite_resolution", "link_status", "link_method", "link_confidence",
    "linked_cluster_id", "linked_cluster_uid", "representative_point_distance_m",
    "n_valid_candidates",
}

LINK_META_COLUMNS = [
    "satellite_location_uid", "satellite_key", "s5b_schema", "source",
    "source_station_id", "path", "linked_cluster_id", "linked_cluster_uid",
    "linked_resolution", "link_method", "link_quality", "link_distance_m",
    "link_uparea_log10_error", "link_area_rel_error", "link_candidate_count",
]


def log_progress(message: str) -> None:
    stamp = time_module.strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] {}".format(stamp, message), flush=True)


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _normalize_resolution(value) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return RESOLUTION_CODE.get(int(numeric), text)
    text = text.lower()
    return {
        "quarterly": "monthly",
        "single_point": "daily",
        "annually_climatology": "climatology",
    }.get(text, text)


def _satellite_key_from_row(row) -> str:
    station_id = _clean_text(row.get("station_id", ""))
    source = _clean_text(row.get("source", "")).lower()
    native = _clean_text(row.get("source_station_id", ""))
    resolution = _normalize_resolution(row.get("resolution", ""))
    payload = "\x1f".join([station_id, source, native, resolution])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return "SATV2{}".format(digest)


def _is_satellite_s5_row(row) -> bool:
    observation_type = _clean_text(row.get("observation_type", "")).lower()
    if observation_type:
        return "satellite" in observation_type
    source = _clean_text(row.get("source", "")).lower()
    return any(
        token in source
        for token in ("riversed", "river_sed", "gsed", "dethier", "aquasat")
    )


def _cluster_uid_from_id(value) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
        number = int(float(value))
    except Exception:
        return ""
    return "SED{:06d}".format(number) if number >= 0 else ""


def _first_existing(names: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    exact = {str(name): str(name) for name in names}
    lower = {str(name).lower(): str(name) for name in names}
    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]
        hit = lower.get(candidate.lower())
        if hit is not None:
            return hit
    return None


def _open_dataset(path: Path):
    errors: List[str] = []
    for engine in (None, "h5netcdf"):
        try:
            kwargs = {"decode_times": False, "mask_and_scale": True}
            if engine is not None:
                kwargs["engine"] = engine
            return xr.open_dataset(path, **kwargs)
        except Exception as exc:
            errors.append("{}: {}".format(engine or "default", exc))
    raise RuntimeError("cannot open {}; tried {}".format(path, "; ".join(errors)))


def _decode_numeric_cf_time(
    values: np.ndarray,
    units: str,
    calendar: str,
) -> pd.DatetimeIndex:
    numeric = pd.to_numeric(
        pd.Series(np.asarray(values).reshape(-1)), errors="coerce"
    )
    try:
        import netCDF4  # type: ignore

        decoded = netCDF4.num2date(
            numeric.to_numpy(dtype=float),
            units=units,
            calendar=calendar or "standard",
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=False,
        )
        texts = []
        for value in np.asarray(decoded).reshape(-1):
            if value is None:
                texts.append("")
                continue
            try:
                texts.append(
                    "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
                        int(value.year), int(value.month), int(value.day),
                        int(getattr(value, "hour", 0)), int(getattr(value, "minute", 0)),
                        int(getattr(value, "second", 0)),
                    )
                )
            except Exception:
                texts.append(str(value))
        return pd.DatetimeIndex(pd.to_datetime(texts, errors="coerce"))
    except Exception:
        pass

    match = re.search(
        r"(days?|hours?|minutes?|seconds?)\s+since\s+"
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}(?:[ T][^ ]+)?)",
        str(units),
        flags=re.I,
    )
    if match:
        unit_text = match.group(1).lower()
        origin = pd.Timestamp(match.group(2))
        unit = {
            "day": "D", "days": "D", "hour": "h", "hours": "h",
            "minute": "m", "minutes": "m", "second": "s", "seconds": "s",
        }[unit_text]
        return pd.DatetimeIndex(
            pd.to_datetime(numeric, unit=unit, origin=origin, errors="coerce")
        )
    return pd.DatetimeIndex(
        pd.to_datetime(numeric, unit="D", origin="1970-01-01", errors="coerce")
    )


def _read_time_axis(ds) -> Tuple[pd.DatetimeIndex, str]:
    time_name = _first_existing(
        ds.variables, ("time", "date", "datetime", "timestamp", "obs_time")
    )
    if time_name is None:
        raise ValueError("no time/date variable")
    da = ds[time_name]
    if len(da.dims) != 1:
        raise ValueError(
            "time variable {} is not one-dimensional: {}".format(time_name, da.dims)
        )
    time_dim = da.dims[0]
    raw = np.asarray(da.values).reshape(-1)
    if np.issubdtype(raw.dtype, np.datetime64):
        parsed = pd.to_datetime(raw, errors="coerce")
    elif raw.dtype.kind in {"S", "U", "O"}:
        texts = [
            value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
            for value in raw
        ]
        parsed = pd.to_datetime(texts, errors="coerce")
    else:
        units = _clean_text(
            da.attrs.get("units", da.encoding.get("units", "days since 1970-01-01"))
        )
        calendar = _clean_text(
            da.attrs.get("calendar", da.encoding.get("calendar", "standard"))
        )
        parsed = _decode_numeric_cf_time(raw, units, calendar)
    return pd.DatetimeIndex(parsed).floor("D"), str(time_dim)


def _extract_numeric_time_series(
    ds,
    candidates: Sequence[str],
    time_dim: str,
    n_time: int,
) -> np.ndarray:
    name = _first_existing(ds.variables, candidates)
    if name is None:
        return np.full(n_time, np.nan, dtype=float)
    da = ds[name]
    if time_dim not in da.dims:
        return np.full(n_time, np.nan, dtype=float)
    ordered_dims = [time_dim] + [dim for dim in da.dims if dim != time_dim]
    values = np.ma.filled(
        np.ma.asarray(da.transpose(*ordered_dims).values), np.nan
    )
    if values.shape[0] != n_time:
        raise ValueError(
            "{} time length {} does not match {}".format(name, values.shape[0], n_time)
        )
    if values.ndim > 1:
        extra_size = int(np.prod(values.shape[1:]))
        if extra_size != 1:
            raise ValueError(
                "{} has unsupported non-singleton dimensions {}".format(name, da.dims)
            )
        values = values.reshape(n_time)
    return pd.to_numeric(
        pd.Series(np.asarray(values).reshape(-1)), errors="coerce"
    ).to_numpy(dtype=float)


def _resolve_source_path(
    raw_path: str,
    source_root: Path,
    linkage_csv_parent: Path,
) -> Tuple[Optional[Path], List[str]]:
    text = _clean_text(raw_path)
    if not text:
        return None, []
    supplied = Path(text).expanduser()
    candidates: List[Path] = []
    if supplied.is_absolute():
        candidates.append(supplied)
    else:
        candidates.extend([source_root / supplied, linkage_csv_parent / supplied, supplied])
    deduplicated: List[Path] = []
    seen = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        key = str(resolved)
        if key not in seen:
            seen.add(key)
            deduplicated.append(resolved)
    for candidate in deduplicated:
        if candidate.is_file():
            return candidate, [str(item) for item in deduplicated]
    return None, [str(item) for item in deduplicated]


def _read_one_satellite_source(
    task: Tuple[int, Dict[str, object], str, str, str, str]
) -> Tuple[int, List[Dict[str, object]], Dict[str, object]]:
    ordinal, row, source_root_text, linkage_parent_text, window_start_text, window_end_text = task
    source_root = Path(source_root_text)
    linkage_parent = Path(linkage_parent_text)
    resolved_path, attempted = _resolve_source_path(
        _clean_text(row.get("path", "")), source_root, linkage_parent
    )
    report: Dict[str, object] = {
        "satellite_location_uid": _clean_text(row.get("satellite_location_uid", "")),
        "source": _clean_text(row.get("source", "")),
        "source_station_id": _clean_text(row.get("source_station_id", "")),
        "linked_cluster_uid": _clean_text(row.get("linked_cluster_uid", "")),
        "linked_resolution": _normalize_resolution(row.get("linked_resolution", "")),
        "path_from_s5b": _clean_text(row.get("path", "")),
        "resolved_path": str(resolved_path) if resolved_path else "",
        "status": "", "message": "", "n_time_steps": 0, "n_time_overlap": 0,
        "n_value_rows": 0, "time_start_loaded": "", "time_end_loaded": "",
    }
    if resolved_path is None:
        report["status"] = "missing_file"
        report["message"] = "attempted: {}".format(" | ".join(attempted))
        return ordinal, [], report

    window_start = pd.Timestamp(window_start_text)
    window_end = pd.Timestamp(window_end_text)
    ds = None
    try:
        ds = _open_dataset(resolved_path)
        times, time_dim = _read_time_axis(ds)
        n_time = len(times)
        report["n_time_steps"] = int(n_time)
        in_window = (
            pd.Series(times).notna().to_numpy()
            & (times >= window_start)
            & (times <= window_end)
        )
        report["n_time_overlap"] = int(in_window.sum())
        if not in_window.any():
            report["status"] = "no_time_overlap"
            return ordinal, [], report
        q = _extract_numeric_time_series(ds, ("Q", "q", "discharge"), time_dim, n_time)
        ssc = _extract_numeric_time_series(ds, ("SSC", "ssc"), time_dim, n_time)
        ssl = _extract_numeric_time_series(ds, ("SSL", "ssl"), time_dim, n_time)
        q_flag = _extract_numeric_time_series(
            ds, ("Q_flag", "q_flag", "Q_qc_flag", "q_qc_flag"), time_dim, n_time
        )
        ssc_flag = _extract_numeric_time_series(
            ds, ("SSC_flag", "ssc_flag", "SSC_qc_flag", "ssc_qc_flag"), time_dim, n_time
        )
        ssl_flag = _extract_numeric_time_series(
            ds, ("SSL_flag", "ssl_flag", "SSL_qc_flag", "ssl_qc_flag"), time_dim, n_time
        )
        keep = in_window & (np.isfinite(q) | np.isfinite(ssc) | np.isfinite(ssl))
        if not keep.any():
            report["status"] = "no_variable_values"
            return ordinal, [], report
        rows: List[Dict[str, object]] = []
        for position in np.where(keep)[0]:
            timestamp = pd.Timestamp(times[position])
            rows.append(
                {
                    "record_id": "{}:{}".format(
                        _clean_text(row.get("satellite_location_uid", "")), int(position)
                    ),
                    "cluster_id": row.get("cluster_id", ""),
                    "cluster_uid": _clean_text(row.get("cluster_uid", "")),
                    "resolution": _normalize_resolution(row.get("resolution", "")),
                    "source_resolution": _normalize_resolution(row.get("resolution", "")),
                    "linked_cluster_id": row.get("linked_cluster_id", ""),
                    "linked_cluster_uid": _clean_text(row.get("linked_cluster_uid", "")),
                    "linked_resolution": _normalize_resolution(row.get("linked_resolution", "")),
                    "link_status": "linked",
                    "source": _clean_text(row.get("source", "")),
                    "source_family": "satellite",
                    "observation_type": "Satellite",
                    "source_station_uid": _clean_text(row.get("satellite_location_uid", "")),
                    "source_station_native_id": _clean_text(row.get("source_station_id", "")),
                    "source_station_paths": str(resolved_path),
                    "candidate_path": _clean_text(row.get("path", "")),
                    "date": timestamp.strftime("%Y-%m-%d"),
                    "Q": q[position], "SSC": ssc[position], "SSL": ssl[position],
                    "Q_flag": q_flag[position], "SSC_flag": ssc_flag[position],
                    "SSL_flag": ssl_flag[position],
                    "validation_only": 1,
                    "merge_policy": "validation_only_from_s5b_source_nc",
                }
            )
        report["status"] = "loaded"
        report["n_value_rows"] = int(len(rows))
        report["time_start_loaded"] = min(pd.Timestamp(item["date"]) for item in rows).strftime("%Y-%m-%d")
        report["time_end_loaded"] = max(pd.Timestamp(item["date"]) for item in rows).strftime("%Y-%m-%d")
        return ordinal, rows, report
    except Exception as exc:
        report["status"] = "read_error"
        report["message"] = "{}: {}".format(type(exc).__name__, exc)
        return ordinal, [], report
    finally:
        if ds is not None:
            ds.close()


def _load_s5_satellite_lookup(s5_csv: Path) -> pd.DataFrame:
    if not s5_csv.is_file():
        raise ValueError(
            "v2 s5b linkage requires --s5-csv to recover source paths; missing: {}".format(s5_csv)
        )
    s5 = pd.read_csv(s5_csv, low_memory=False)
    needed = {"station_id", "source", "source_station_id", "path", "resolution", "cluster_id"}
    missing = sorted(needed - set(s5.columns))
    if missing:
        raise ValueError("s5 CSV missing columns needed for v2 linkage: {}".format(", ".join(missing)))
    sat = s5.loc[s5.apply(_is_satellite_s5_row, axis=1)].copy()
    sat["satellite_key"] = sat.apply(_satellite_key_from_row, axis=1)
    sat["resolution"] = sat["resolution"].map(_normalize_resolution)
    sat["satellite_location_uid"] = sat["satellite_key"]
    sat["cluster_uid"] = sat["cluster_id"].map(_cluster_uid_from_id)
    keep = [
        "satellite_key", "satellite_location_uid", "station_id", "cluster_id",
        "cluster_uid", "source", "source_station_id", "path", "resolution",
    ]
    sat = sat[keep].copy()
    duplicates = sat.duplicated("satellite_key", keep=False)
    if duplicates.any():
        examples = sat.loc[
            duplicates,
            ["satellite_key", "station_id", "source", "source_station_id", "resolution"],
        ].head(10)
        raise ValueError(
            "duplicate satellite_key values in s5 CSV; examples: {}".format(
                examples.to_dict("records")
            )
        )
    return sat


def _normalize_v2_linkage_table(linkage: pd.DataFrame, s5_csv: Path) -> pd.DataFrame:
    missing = sorted(REQUIRED_V2_LINKAGE_COLUMNS - set(linkage.columns))
    if missing:
        raise ValueError("s5b v2 linkage CSV missing columns: {}".format(", ".join(missing)))
    s5_sat = _load_s5_satellite_lookup(s5_csv)
    work = linkage.copy()
    work["satellite_key"] = work["satellite_key"].map(_clean_text)
    merged = work.merge(
        s5_sat, how="left", on="satellite_key", suffixes=("", "_s5"),
        validate="many_to_one",
    )
    missing_path = merged["path"].fillna("").astype(str).str.strip().eq("")
    if missing_path.any():
        examples = merged.loc[
            missing_path,
            ["satellite_key", "satellite_station_id", "satellite_source", "satellite_resolution"],
        ].head(10)
        raise ValueError(
            "could not recover source path for {} v2 linkage row(s); examples: {}".format(
                int(missing_path.sum()), examples.to_dict("records")
            )
        )
    normalized = pd.DataFrame(index=merged.index)
    normalized["satellite_location_uid"] = merged["satellite_key"].map(_clean_text)
    normalized["cluster_id"] = merged["cluster_id"]
    normalized["cluster_uid"] = merged["cluster_uid"].map(_clean_text)
    normalized["source"] = merged["source"].map(_clean_text)
    normalized["source_station_id"] = merged["source_station_id"].map(_clean_text)
    normalized["path"] = merged["path"].map(_clean_text)
    normalized["resolution"] = merged["resolution"].map(_normalize_resolution)
    normalized["linked_cluster_id"] = merged["linked_cluster_id"]
    normalized["linked_cluster_uid"] = merged["linked_cluster_uid"].map(_clean_text)
    if "linked_resolution" in merged.columns:
        linked_resolution = merged["linked_resolution"].map(_clean_text)
        fallback_resolution = merged["satellite_resolution"].map(_clean_text)
        normalized["linked_resolution"] = linked_resolution.where(
            linked_resolution.ne(""), fallback_resolution
        ).map(_normalize_resolution)
    else:
        normalized["linked_resolution"] = merged["satellite_resolution"].map(_normalize_resolution)
    normalized["link_status"] = merged["link_status"].map(_clean_text).str.lower()
    normalized["link_method"] = merged["link_method"].map(_clean_text)
    normalized["link_quality"] = merged["link_confidence"].map(_clean_text)
    normalized["link_distance_m"] = pd.to_numeric(
        merged.get("representative_point_distance_m", np.nan), errors="coerce"
    )
    normalized["link_uparea_log10_error"] = pd.to_numeric(
        merged.get("area_rel_error", np.nan), errors="coerce"
    )
    normalized["link_area_rel_error"] = normalized["link_uparea_log10_error"]
    normalized["link_candidate_count"] = pd.to_numeric(
        merged.get("n_valid_candidates", np.nan), errors="coerce"
    )
    normalized["unlinked_reason"] = merged.get(
        "rejection_reason", pd.Series([""] * len(merged), index=merged.index)
    ).map(_clean_text)
    normalized["s5b_schema"] = "v2"
    normalized["satellite_key"] = merged["satellite_key"].map(_clean_text)
    return normalized


def _validate_linkage_table(
    linkage: pd.DataFrame, s5_csv: Optional[Path] = None
) -> pd.DataFrame:
    if "satellite_key" not in linkage.columns or "satellite_source" not in linkage.columns:
        raise ValueError("expected s5b v2 linkage CSV with satellite_key and satellite_source columns")
    if s5_csv is None:
        raise ValueError("v2 s5b linkage requires --s5-csv")
    work = _normalize_v2_linkage_table(linkage, s5_csv)
    missing = sorted(REQUIRED_LINKAGE_COLUMNS - set(work.columns))
    if missing:
        raise ValueError("normalized s5b v2 linkage CSV missing columns: {}".format(", ".join(missing)))
    work["link_status"] = work["link_status"].map(_clean_text).str.lower()
    work["resolution"] = work["resolution"].map(_normalize_resolution)
    work["linked_resolution"] = work["linked_resolution"].map(_normalize_resolution)
    work["linked_cluster_uid"] = work["linked_cluster_uid"].map(_clean_text)
    work["satellite_location_uid"] = work["satellite_location_uid"].map(_clean_text)
    for col in ("source", "source_station_id", "path"):
        if col in work.columns:
            work[col] = work[col].map(_clean_text)
    duplicates = work.duplicated(["satellite_location_uid", "resolution"], keep=False)
    if duplicates.any():
        examples = (
            work.loc[duplicates, ["satellite_location_uid", "resolution"]]
            .drop_duplicates().head(10).to_dict("records")
        )
        raise ValueError("duplicate satellite location/resolution keys: {}".format(examples))
    return work


def _load_insitu_observations(
    release_dir: Path,
    taxonomy: Dict[str, str],
    workers: int,
    target_pairs: set = None,
) -> Tuple[pd.DataFrame, str]:
    """Load in-situ observations from sed_reference_master.nc.

    When target_pairs is provided, only matching (cluster_uid, resolution)
    records are read from the master product.
    """
    path = release_dir / base.MASTER_FILE
    if not path.is_file():
        raise SystemExit("sed_reference_master.nc not found: {}".format(path))
    log_progress("Reading master NC (pre-filtered): {}".format(path))
    ds = None
    try:
        ds = base._open_dataset_compat(path)
        record_dim = base._find_record_dim(ds)
        if record_dim is None:
            raise SystemExit("record dimension could not be inferred from master NC")
        n_records = int(ds.sizes[record_dim])
        if target_pairs:
            uid_arr = _read_station_var_to_record_space(
                ds, "cluster_uid", record_dim, n_records
            )
            res_arr = _read_var_array(ds, "resolution", record_dim)
            clean_uid = np_clean_text_array(uid_arr)
            mask = np.zeros(n_records, dtype=bool)
            for uid, res in target_pairs:
                res_code = _resolution_text_to_code(res)
                if res_code is not None:
                    mask |= (clean_uid == uid) & (res_arr == res_code)
            n_matched = int(mask.sum())
            log_progress(
                "Master NC pre-filter: {}/{} records match {} target pairs".format(
                    n_matched, n_records, len(target_pairs)
                )
            )
            if n_matched == 0:
                ds.close()
                ds = None
                return pd.DataFrame(), "no master NC records match target pairs"
        else:
            mask = np.ones(n_records, dtype=bool)
        indices = np.where(mask)[0]
        records = pd.DataFrame({"record_index": indices})
        for name in base.VARIABLES:
            series = _read_masked_series(ds, name, record_dim, indices, n_records)
            if series is not None:
                records[name] = pd.to_numeric(series, errors="coerce")
            flag = _read_masked_series(
                ds, "{}_flag".format(name), record_dim, indices, n_records
            )
            if flag is not None:
                records["{}_flag".format(name)] = flag
        provenance_fields = (
            "resolution", "cluster_uid", "cluster_id", "source", "source_family",
            "observation_type", "source_station_uid", "source_station_paths",
            "candidate_path", "is_overlap", "river_width_class", "river_width_m",
            "climate_zone",
        )
        for name in provenance_fields:
            series = _read_masked_series(ds, name, record_dim, indices, n_records)
            if series is not None:
                records[name] = series
        time_series = _read_masked_series(ds, "time", record_dim, indices, n_records)
        if time_series is not None:
            records["time"] = time_series
            records["_time_units"] = getattr(
                ds["time"], "units", "days since 1970-01-01"
            )
        date_series = _read_masked_series(ds, "date", record_dim, indices, n_records)
        if date_series is not None:
            records["date"] = pd.Series(date_series).astype(str).values
    finally:
        if ds is not None:
            ds.close()
    if records.empty:
        return pd.DataFrame(), "no master NC records match target pairs"
    load_note = "selected master records; pre-filtered to {} target pairs".format(
        len(target_pairs) if target_pairs else len(records)
    )
    raw = base.add_observation_type_from_source_attrs(
        records, release_dir, workers=workers, progress=log_progress
    )
    normalized = base.normalize_observation_table(
        raw, taxonomy, input_mode="selected_master"
    )
    insitu = normalized[normalized["source_family"].eq("in_situ")].copy()
    return insitu, load_note


def _read_var_array(ds, name: str, record_dim: str) -> np.ndarray:
    if name not in ds.variables:
        raise KeyError("variable {} not found".format(name))
    da = ds[name]
    if record_dim not in da.dims:
        raise ValueError(
            "record dimension {} not in {} dims {}".format(record_dim, name, da.dims)
        )
    if da.dims == (record_dim,):
        return np.asarray(da.values).reshape(-1)
    values = np.asarray(da.values)
    if values.dtype.kind in ("S", "U") and values.ndim == 2:
        return np.array([
            b"".join(values[i].reshape(-1)).decode("utf-8", errors="ignore").strip()
            for i in range(values.shape[0])
        ])
    return values.reshape(values.shape[0], -1)[:, 0]


def _read_indexed_var_to_record_space(
    ds, name: str, index_name: str, n_records: int
) -> np.ndarray:
    if name not in ds.variables:
        raise KeyError("variable {} not found".format(name))
    if index_name not in ds.variables:
        raise KeyError("index variable {} not found".format(index_name))
    index = pd.to_numeric(
        pd.Series(np.asarray(ds[index_name].values).reshape(-1)), errors="coerce"
    ).to_numpy(dtype=float)
    values = _series_from_data_array(ds[name]).reset_index(drop=True)
    result = np.full(n_records, "", dtype=object)
    valid = ~np.isnan(index) & (index >= 0) & (index < len(values))
    result[valid] = values.iloc[index[valid].astype(int)].to_numpy()
    return result


def _read_station_var_to_record_space(
    ds, name: str, record_dim: str, n_records: int
) -> np.ndarray:
    """Expand station-dimension metadata to record space via station_index."""
    return _read_indexed_var_to_record_space(
        ds, name, "station_index", n_records
    )


def _read_masked_series(
    ds, name: str, record_dim: str, indices: np.ndarray, n_records: int
):
    if name not in ds.variables:
        return None
    da = ds[name]
    dims = tuple(da.dims)
    if record_dim not in dims:
        if "n_source_stations" in dims and "source_station_index" in ds.variables:
            full = _read_indexed_var_to_record_space(
                ds, name, "source_station_index", n_records
            )
            return pd.Series(full[indices])
        if "n_stations" in dims and "station_index" in ds.variables:
            full = _read_station_var_to_record_space(ds, name, record_dim, n_records)
            return pd.Series(full[indices])
        return None
    if dims == (record_dim,):
        result = np.asarray(da.values)[indices]
        if result.dtype.kind in ("S", "U"):
            return pd.Series([
                v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
                for v in result.reshape(-1)
            ])
        try:
            result = np.ma.asarray(result).filled(np.nan)
        except Exception:
            pass
        return pd.Series(np.asarray(result).reshape(-1))
    if len(dims) == 2 and dims[0] == record_dim:
        arr = np.asarray(da.values)
        if arr.dtype.kind in ("S", "U"):
            return pd.Series([
                b"".join(arr[i].reshape(-1)).decode("utf-8", errors="ignore").strip()
                for i in indices
            ])
        sub = arr[indices, :].reshape(len(indices), -1)
        try:
            values = np.ma.asarray(sub[:, 0]).filled(np.nan)
        except Exception:
            values = sub[:, 0]
        return pd.Series(values)
    return None


def np_clean_text_array(arr: np.ndarray) -> np.ndarray:
    if arr.dtype.kind in ("S", "U"):
        result = np.array([
            v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
            for v in arr.reshape(-1)
        ])
    else:
        result = np.array([str(v) for v in arr.reshape(-1)])
    result = np.char.strip(result)
    for missing in ("nan", "none", "nat"):
        result[np.char.lower(result) == missing] = ""
    return result


def _resolution_text_to_code(res: str):
    return {"daily": 0, "monthly": 1, "annual": 2, "climatology": 3}.get(res)


def _load_station_catalog_set(release_dir: Path) -> set:
    path = release_dir / "station_catalog.csv"
    if not path.is_file():
        raise SystemExit("station_catalog.csv not found in release directory: {}".format(path))
    cat = pd.read_csv(path, low_memory=False)
    required = {"cluster_uid", "resolution"}
    missing = sorted(required - set(cat.columns))
    if missing:
        raise ValueError(
            "station_catalog.csv missing required columns: {}".format(", ".join(missing))
        )
    pairs = set()
    for _, row in cat.iterrows():
        uid = _clean_text(row.get("cluster_uid", ""))
        res = _normalize_resolution(row.get("resolution", ""))
        if uid and res:
            pairs.add((uid, res))
    log_progress(
        "Station catalog loaded: {} unique (cluster_uid, resolution) pairs".format(len(pairs))
    )
    return pairs


def _filter_insitu_by_targets(insitu: pd.DataFrame, target_pairs: set) -> pd.DataFrame:
    if insitu.empty:
        return insitu
    insitu_uid = insitu["cluster_uid"].map(_clean_text)
    insitu_res = insitu["resolution"].map(_normalize_resolution)
    mask = pd.Series(list(zip(insitu_uid, insitu_res)), index=insitu.index).isin(target_pairs)
    result = insitu.loc[mask].copy()
    log_progress(
        "Insitu filter: kept {}/{} rows ({} unique cluster/resolution pairs)".format(
            len(result), len(insitu),
            result.drop_duplicates(["cluster_uid", "resolution"]).shape[0],
        )
    )
    return result


def _build_insitu_windows(
    insitu: pd.DataFrame,
    windows: Sequence[str],
) -> Dict[Tuple[str, str], Tuple[pd.Timestamp, pd.Timestamp]]:
    if insitu.empty:
        return {}
    max_days = max(base.WINDOW_DAYS[window] for window in windows) if windows else 0
    work = insitu.copy()
    work["cluster_uid"] = work["cluster_uid"].map(_clean_text)
    work["resolution"] = work["resolution"].map(_normalize_resolution)
    work["time"] = pd.to_datetime(work["time"], errors="coerce").dt.floor("D")
    work = work[
        work["cluster_uid"].ne("")
        & work["resolution"].isin(base.LINKED_RESOLUTIONS)
        & work["time"].notna()
    ]
    ranges = {}
    for (cluster_uid, resolution), group in work.groupby(
        ["cluster_uid", "resolution"], sort=True
    ):
        ranges[(cluster_uid, resolution)] = (
            pd.Timestamp(group["time"].min()) - pd.Timedelta(days=max_days),
            pd.Timestamp(group["time"].max()) + pd.Timedelta(days=max_days),
        )
    return ranges


def _load_satellite_observations_from_s5b(
    linkage: pd.DataFrame,
    linkage_csv: Path,
    source_root: Path,
    insitu_windows: Dict[Tuple[str, str], Tuple[pd.Timestamp, pd.Timestamp]],
    taxonomy: Dict[str, str],
    workers: int,
    sources: Optional[Sequence[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    linked = linkage[
        linkage["link_status"].eq("linked")
        & linkage["linked_cluster_uid"].ne("")
        & linkage["linked_resolution"].isin(base.LINKED_RESOLUTIONS)
    ].copy()
    if sources:
        allowed = {item.strip().lower() for item in sources if item.strip()}
        linked = linked[linked["source"].map(_clean_text).str.lower().isin(allowed)].copy()
    linkage_summary = (
        linkage.assign(
            source=linkage["source"].map(_clean_text),
            unlinked_reason=linkage.get(
                "unlinked_reason", pd.Series([""] * len(linkage))
            ).map(_clean_text),
        )
        .groupby(["source", "link_status", "unlinked_reason"], dropna=False)
        .size().reset_index(name="n_locations")
        .sort_values(["source", "link_status", "n_locations"], ascending=[True, True, False])
    )
    tasks = []
    pre_reports: List[Dict[str, object]] = []
    for ordinal, row in enumerate(
        linked.sort_values(
            ["linked_cluster_uid", "linked_resolution", "satellite_location_uid"],
            kind="mergesort",
        ).to_dict("records")
    ):
        key = (
            _clean_text(row.get("linked_cluster_uid", "")),
            _normalize_resolution(row.get("linked_resolution", "")),
        )
        time_range = insitu_windows.get(key)
        if time_range is None:
            pre_reports.append(
                {
                    "satellite_location_uid": _clean_text(row.get("satellite_location_uid", "")),
                    "source": _clean_text(row.get("source", "")),
                    "source_station_id": _clean_text(row.get("source_station_id", "")),
                    "linked_cluster_uid": key[0], "linked_resolution": key[1],
                    "path_from_s5b": _clean_text(row.get("path", "")),
                    "resolved_path": "", "status": "no_insitu_cluster_resolution",
                    "message": "", "n_time_steps": 0, "n_time_overlap": 0,
                    "n_value_rows": 0, "time_start_loaded": "", "time_end_loaded": "",
                }
            )
            continue
        tasks.append(
            (
                ordinal, row, str(source_root), str(linkage_csv.parent),
                time_range[0].isoformat(), time_range[1].isoformat(),
            )
        )
    log_progress(
        "Reading {} linked satellite source files with {} worker(s)".format(
            len(tasks), max(1, int(workers or 1))
        )
    )
    workers = max(1, int(workers or 1))
    if workers == 1 or len(tasks) <= 1:
        results = [_read_one_satellite_source(task) for task in tasks]
    else:
        chunksize = max(1, min(20, int(math.ceil(len(tasks) / float(workers * 8)))))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_read_one_satellite_source, tasks, chunksize=chunksize))
    raw_rows: List[Dict[str, object]] = []
    reports = list(pre_reports)
    for _, rows, report in sorted(results, key=lambda item: item[0]):
        raw_rows.extend(rows)
        reports.append(report)
    raw = pd.DataFrame(raw_rows)
    report_df = pd.DataFrame(reports)
    if not raw.empty:
        for col, linked_col in (
            ("cluster_uid", "linked_cluster_uid"),
            ("resolution", "linked_resolution"),
        ):
            if linked_col in raw.columns:
                mask = raw[linked_col].astype(str).str.strip().ne("")
                raw.loc[mask, col] = raw.loc[mask, linked_col]
    satellite = base.normalize_observation_table(
        raw, taxonomy, input_mode="s5b_linkage_csv"
    )
    if satellite.empty:
        return satellite, report_df, linkage_summary
    return (
        satellite[satellite["source_family"].eq("satellite")].copy(),
        report_df,
        linkage_summary,
    )


def _attach_linkage_metadata(pairs: pd.DataFrame, linkage: pd.DataFrame) -> pd.DataFrame:
    if pairs.empty:
        return pairs
    available = [column for column in LINK_META_COLUMNS if column in linkage.columns]
    metadata = linkage[
        linkage["link_status"].map(_clean_text).str.lower().eq("linked")
    ][available].copy()
    metadata = metadata.drop_duplicates("satellite_location_uid")
    metadata = metadata.rename(
        columns={
            "satellite_key": "s5b_satellite_key",
            "s5b_schema": "s5b_schema",
            "source": "s5b_satellite_source",
            "source_station_id": "s5b_source_station_id",
            "path": "s5b_source_path",
            "linked_cluster_id": "s5b_linked_cluster_id",
            "linked_cluster_uid": "s5b_linked_cluster_uid",
            "linked_resolution": "s5b_linked_resolution",
        }
    )
    return pairs.merge(
        metadata,
        how="left",
        left_on="satellite_source_station_uid",
        right_on="satellite_location_uid",
    )


def run_validation(
    release_dir: Path,
    linkage_csv: Path,
    source_root: Path,
    out_dir: Path,
    s5_csv: Path = DEFAULT_S5_CSV,
    source_taxonomy_csv: Optional[Path] = None,
    external_attributes_csv: Optional[Path] = None,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    high_turbidity_ssc: float = base.DEFAULT_HIGH_TURBIDITY_SSC,
    ssc_bin_edges: Sequence[float] = base.DEFAULT_SSC_BIN_EDGES,
    figure_variables: Sequence[str] = ("SSC",),
    write_plots: bool = True,
    workers: int = base.DEFAULT_WORKERS,
    sources: Optional[Sequence[str]] = None,
    strict_source_files: bool = False,
) -> None:
    release_dir = release_dir.expanduser().resolve()
    linkage_csv = linkage_csv.expanduser().resolve()
    s5_csv = s5_csv.expanduser().resolve()
    source_root = source_root.expanduser().resolve()
    out_dir = out_dir.expanduser().resolve()
    if not release_dir.is_dir():
        raise SystemExit("release-dir does not exist: {}".format(release_dir))
    if not linkage_csv.is_file():
        raise SystemExit("s5b linkage CSV does not exist: {}".format(linkage_csv))
    unknown_windows = [window for window in windows if window not in base.WINDOW_DAYS]
    if unknown_windows:
        raise SystemExit("unknown pairing windows: {}".format(", ".join(unknown_windows)))
    out_dir.mkdir(parents=True, exist_ok=True)
    log_progress("Starting s11b validation from s5b linkage CSV")
    log_progress("Release dir: {}".format(release_dir))
    log_progress("Linkage CSV: {}".format(linkage_csv))
    log_progress("S5 CSV: {}".format(s5_csv))
    log_progress("Satellite source root: {}".format(source_root))
    log_progress("Output dir: {}".format(out_dir))

    taxonomy = base.load_source_taxonomy(source_taxonomy_csv)
    external_attrs = base._load_external_attributes(external_attributes_csv)
    linkage = _validate_linkage_table(
        pd.read_csv(linkage_csv, low_memory=False), s5_csv=s5_csv
    )
    log_progress(
        "s5b rows={}, linked={}, unlinked={}".format(
            len(linkage), int(linkage["link_status"].eq("linked").sum()),
            int(linkage["link_status"].eq("unlinked").sum()),
        )
    )
    catalog_pairs = _load_station_catalog_set(release_dir)
    linked_before = int(linkage["link_status"].eq("linked").sum())
    linkage_uid = linkage["linked_cluster_uid"].map(_clean_text)
    linkage_res = linkage["linked_resolution"].map(_normalize_resolution)
    linkage_pairs = pd.Series(list(zip(linkage_uid, linkage_res)), index=linkage.index)
    in_catalog = linkage_pairs.isin(catalog_pairs)
    keep_mask = linkage["link_status"].ne("linked") | in_catalog
    linkage = linkage.loc[keep_mask].copy()
    linked_after = int(linkage["link_status"].eq("linked").sum())
    log_progress(
        "Catalog filter: linked {} -> {} ({} rows excluded)".format(
            linked_before, linked_after, linked_before - linked_after
        )
    )
    target_pairs = set()
    for _, row in linkage[linkage["link_status"].eq("linked")].iterrows():
        uid = _clean_text(row.get("linked_cluster_uid", ""))
        res = _normalize_resolution(row.get("linked_resolution", ""))
        if uid and res:
            target_pairs.add((uid, res))
    log_progress("Valid linkage target pairs (in catalog): {}".format(len(target_pairs)))

    input_path = release_dir / base.MASTER_FILE
    insitu, load_note = _load_insitu_observations(
        release_dir=release_dir,
        taxonomy=taxonomy,
        workers=workers,
        target_pairs=target_pairs,
    )
    log_progress("Master NC insitu observations (all): {}".format(len(insitu)))
    insitu = _filter_insitu_by_targets(insitu, target_pairs)
    log_progress("Filtered insitu observations (catalog targets only): {}".format(len(insitu)))
    insitu_windows = _build_insitu_windows(insitu, windows)
    log_progress("In-situ cluster/resolution windows: {}".format(len(insitu_windows)))

    satellite, load_report, linkage_summary = _load_satellite_observations_from_s5b(
        linkage=linkage,
        linkage_csv=linkage_csv,
        source_root=source_root,
        insitu_windows=insitu_windows,
        taxonomy=taxonomy,
        workers=workers,
        sources=sources,
    )
    log_progress("Normalized satellite observations: {}".format(len(satellite)))
    if strict_source_files and not load_report.empty:
        failed = load_report[load_report["status"].isin(["missing_file", "read_error"])]
        if not failed.empty:
            raise SystemExit(
                "{} linked satellite files failed to load; see validation_satellite_source_load_report.csv".format(len(failed))
            )

    observations = pd.concat([insitu, satellite], ignore_index=True, sort=False)
    pair_mode = "s5b_linkage_csv+selected_master_catalog_filtered"
    pairs, temporal_audit = base.pair_satellite_insitu_records(
        observations,
        windows=windows,
        input_mode=pair_mode,
        workers=workers,
        progress=log_progress,
        return_audit=True,
    )
    pairs = _attach_linkage_metadata(pairs, linkage)
    pairs = base.assign_strata(
        pairs,
        external_attributes=external_attrs,
        high_turbidity_ssc=high_turbidity_ssc,
        ssc_bin_edges=ssc_bin_edges,
    )
    metrics = base.compute_satellite_insitu_metrics(pairs)

    pair_path = out_dir / "validation_satellite_insitu_pairs.csv"
    metric_path = out_dir / "validation_satellite_insitu_metrics.csv"
    load_report_path = out_dir / "validation_satellite_source_load_report.csv"
    linkage_summary_path = out_dir / "validation_s5b_linkage_summary.csv"
    temporal_audit_path = out_dir / "validation_temporal_alignment_audit.csv"
    summary_path = out_dir / "validation_satellite_insitu_summary.md"
    pairs.to_csv(pair_path, index=False)
    metrics.to_csv(metric_path, index=False)
    load_report.to_csv(load_report_path, index=False)
    linkage_summary.to_csv(linkage_summary_path, index=False)
    temporal_audit.to_csv(temporal_audit_path, index=False)

    generated_outputs: List[Tuple[str, str]] = [
        (pair_path.name, "generated"),
        (metric_path.name, "generated"),
        (load_report_path.name, "generated"),
        (linkage_summary_path.name, "generated"),
        (temporal_audit_path.name, "generated"),
    ]
    if write_plots:
        generated_outputs.extend(
            base.write_figures(
                pairs, metrics, out_dir, figure_variables=figure_variables
            )
        )
    else:
        generated_outputs.extend(
            [
                ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: --no-figures"),
                ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: --no-figures"),
                ("figures/satellite_insitu_metric_heatmap.png", "skipped: --no-figures"),
            ]
        )
    generated_outputs.append((summary_path.name, "generated"))
    base.write_summary(
        summary_path,
        input_path,
        pair_mode,
        "{}; satellite records loaded directly from s5b source paths; insitu filtered to station_catalog pairs".format(load_note),
        observations,
        pairs,
        metrics,
        generated_outputs,
    )

    status_counts = (
        load_report["status"].value_counts(dropna=False).to_dict()
        if not load_report.empty
        else {}
    )
    with summary_path.open("a", encoding="utf-8") as handle:
        handle.write("\n## 6. s5b linkage input\n")
        handle.write("- Linkage CSV: `{}`.\n".format(linkage_csv))
        handle.write("- Station catalog pairs loaded: `{}`.\n".format(len(catalog_pairs)))
        handle.write(
            "- Linkage linked rows before/after catalog filter: `{}` / `{}`.\n".format(
                linked_before, linked_after
            )
        )
        handle.write("- S5 CSV used for v2 path recovery: `{}`.\n".format(s5_csv))
        handle.write(
            "- s5b linkage schema counts: `{}`.\n".format(
                linkage.get("s5b_schema", pd.Series(dtype=object)).value_counts(dropna=False).to_dict()
            )
        )
        handle.write("- Satellite source root: `{}`.\n".format(source_root))
        handle.write(
            "- Total s5b rows: {}; linked rows: {}.\n".format(
                len(linkage), int(linkage["link_status"].eq("linked").sum())
            )
        )
        handle.write("- Satellite source-load status counts: `{}`.\n".format(status_counts))
        handle.write("- Satellite observation rows loaded: {}.\n".format(len(satellite)))
        handle.write("- Temporal alignment audit rows: {}.\n".format(len(temporal_audit)))
        handle.write("- Final pair rows: {}; metric rows: {}.\n".format(len(pairs), len(metrics)))
    log_progress(
        "Complete: pairs={}, metric_rows={}, satellite_rows={}".format(
            len(pairs), len(metrics), len(satellite)
        )
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--release-dir", default=str(DEFAULT_RELEASE_DIR),
        help="Directory containing sed_reference_master.nc and catalogues."
    )
    parser.add_argument(
        "--s5b-linkage-csv", default=str(DEFAULT_LINKAGE_CSV),
        help="s5b satellite-to-main-cluster linkage CSV."
    )
    parser.add_argument(
        "--s5-csv", default=str(DEFAULT_S5_CSV),
        help="s5 basin-clustered stations CSV. Required for v2 path recovery."
    )
    parser.add_argument(
        "--source-root", default=str(DEFAULT_SOURCE_ROOT),
        help="Root corresponding to relative paths in the s5b path column."
    )
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Validation output directory.")
    parser.add_argument("--source-taxonomy-csv", help="Optional source-family taxonomy override CSV.")
    parser.add_argument("--external-attributes-csv", help="Optional cluster width/climate attributes CSV.")
    parser.add_argument(
        "--windows", nargs="+", default=["exact", "pm1d", "pm2d"],
        choices=sorted(base.WINDOW_DAYS),
    )
    parser.add_argument("--high-turbidity-ssc", type=float, default=base.DEFAULT_HIGH_TURBIDITY_SSC)
    parser.add_argument(
        "--ssc-bin-edges",
        default=",".join(base._format_edge(value) for value in base.DEFAULT_SSC_BIN_EDGES),
    )
    parser.add_argument("--figure-variables", nargs="+", default=["SSC"], choices=list(base.VARIABLES))
    parser.add_argument("--sources", nargs="+", help="Optional satellite source filter, for example RiverSed Dethier.")
    parser.add_argument("--workers", type=int, default=base.DEFAULT_WORKERS)
    parser.add_argument("--no-figures", action="store_true")
    parser.add_argument(
        "--strict-source-files", action="store_true",
        help="Fail if any linked source NetCDF is missing or unreadable."
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run_validation(
        release_dir=Path(args.release_dir),
        linkage_csv=Path(args.s5b_linkage_csv),
        s5_csv=Path(args.s5_csv),
        source_root=Path(args.source_root),
        out_dir=Path(args.out_dir),
        source_taxonomy_csv=(
            Path(args.source_taxonomy_csv).expanduser().resolve()
            if args.source_taxonomy_csv else None
        ),
        external_attributes_csv=(
            Path(args.external_attributes_csv).expanduser().resolve()
            if args.external_attributes_csv else None
        ),
        windows=args.windows,
        high_turbidity_ssc=float(args.high_turbidity_ssc),
        ssc_bin_edges=base.parse_ssc_bin_edges(args.ssc_bin_edges),
        figure_variables=args.figure_variables,
        write_plots=not args.no_figures,
        workers=max(1, int(args.workers)),
        sources=args.sources,
        strict_source_files=args.strict_source_files,
    )


if __name__ == "__main__":
    main()
