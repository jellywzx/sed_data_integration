#!/usr/bin/env python3
"""Satellite / reach-scale vs in-situ validation for the sediment release.

This script is intentionally independent from ``s10_validation_results.py``.
It prefers a candidate-level release sidecar when available, and falls back to
selected records in ``sed_reference_master.nc`` when candidate values were not
published.
"""

import argparse
import math
import os
import re
import time as time_module
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


VARIABLES = ("Q", "SSC", "SSL")
WINDOW_DAYS = {"exact": 0, "pm1d": 1, "pm2d": 2}
WINDOW_EXCLUSIVE = False
MASTER_FILE = "sed_reference_master.nc"
SATELLITE_VALIDATION_FILE = "sed_reference_satellite_validation.nc"
CANDIDATE_SIDECAR_FILES = (
    "sed_reference_overlap_candidates.parquet",
    "sed_reference_overlap_candidates.csv.gz",
)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DEFAULT_RELEASE_DIR = PROJECT_DIR / "output" / "sed_reference_release"
DEFAULT_OUT_DIR = PROJECT_DIR / "output" / "validation_results"
DEFAULT_WORKERS = max(1, min(32, os.cpu_count() or 1))
DEFAULT_SATELLITE_CHUNK_SIZE = 1000000
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
OBSERVATION_TYPE_ATTRS = ("observation_type", "Type", "type")
SOURCE_PATH_COLUMNS = (
    "source_station_paths",
    "source_path",
    "candidate_path",
    "path",
    "nc_path",
    "file_path",
)


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


def _coerce_numeric(series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _decode_value_array(values) -> List[str]:
    out = []
    for value in np.asarray(values).reshape(-1):
        out.append(_clean_text(value))
    return out


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
    one_dim_vars = [tuple(da.dims)[0] for da in ds.variables.values() if len(tuple(da.dims)) == 1]
    if one_dim_vars:
        return str(pd.Series(one_dim_vars).value_counts().index[0])
    return None


def _record_series(ds, name: str, record_dim: str) -> Optional[pd.Series]:
    if name not in ds.variables:
        return None
    da = ds[name]
    if record_dim not in da.dims:
        return None
    if len(da.dims) == 1:
        return _series_from_data_array(da).reset_index(drop=True)
    if len(da.dims) == 2 and da.dims[0] == record_dim:
        return _series_from_data_array(da).reset_index(drop=True)
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
        out.append(values.iloc[integer] if 0 <= integer < len(values) else "")
    return pd.Series(out)


def _parse_cf_days_since(values: pd.Series, units: str) -> pd.Series:
    match = re.search(r"days\s+since\s+([0-9]{4}-[0-9]{2}-[0-9]{2})", str(units), flags=re.I)
    origin = match.group(1) if match else "1970-01-01"
    numeric = pd.to_numeric(values, errors="coerce")
    return pd.to_datetime(numeric, unit="D", origin=pd.Timestamp(origin), errors="coerce")


def _datetime_to_cf_days(values: pd.Series, units: str) -> pd.Series:
    match = re.search(r"days\s+since\s+([0-9]{4}-[0-9]{2}-[0-9]{2})", str(units), flags=re.I)
    origin = pd.Timestamp(match.group(1) if match else "1970-01-01")
    parsed = pd.to_datetime(values, errors="coerce")
    return (parsed - origin) / pd.Timedelta(days=1)


def _coerce_datetime_from_columns(df: pd.DataFrame) -> pd.Series:
    date_col = _first_existing(df.columns, ("date", "datetime", "timestamp", "obs_date", "observation_date"))
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
        return pd.to_datetime(numeric, unit="D", origin="unix", errors="coerce").dt.floor("D")
    parsed = pd.to_datetime(raw, errors="coerce")
    if parsed.notna().any():
        return parsed.dt.floor("D")
    return pd.to_datetime(numeric, unit="D", origin="unix", errors="coerce").dt.floor("D")


def _read_table(path: Path) -> pd.DataFrame:
    suffixes = "".join(path.suffixes).lower()
    if suffixes.endswith(".parquet"):
        return pd.read_parquet(path)
    if suffixes.endswith(".csv") or suffixes.endswith(".csv.gz") or suffixes.endswith(".gz"):
        return pd.read_csv(path, keep_default_na=False, low_memory=False)
    raise ValueError("Unsupported candidate sidecar extension: {}".format(path))


def _groupby_compat(df: pd.DataFrame, by_cols: Sequence[str]):
    try:
        return df.groupby(list(by_cols), dropna=False)
    except TypeError:
        return df.groupby(list(by_cols))


def _open_dataset_compat(path: Path):
    import xarray as xr  # type: ignore

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
    raise RuntimeError("cannot open dataset {}; tried {}".format(path, "; ".join(errors)))


def parse_ssc_bin_edges(text: str) -> Tuple[float, ...]:
    values = []
    for token in str(text).split(","):
        token = token.strip()
        if token:
            values.append(float(token))
    if not values:
        return DEFAULT_SSC_BIN_EDGES
    values = sorted(set(values))
    return tuple(values)


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
            if float(right).is_integer():
                upper = _format_edge(right - 1)
            else:
                upper = "<{}".format(_format_edge(right))
            return "{}-{}".format(_format_edge(left), upper)
    return ">={}".format(_format_edge(sorted_edges[-1]))


def _family_key(value) -> str:
    text = _clean_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def load_source_taxonomy(path: Optional[Path] = None) -> Dict[str, str]:
    """Load exact source-name overrides from CSV.

    Accepted columns are ``source`` plus one of ``family``, ``source_family``,
    ``class`` or ``source_class``.
    """
    overrides: Dict[str, str] = {}
    if path is None:
        return overrides
    if not path.exists():
        raise FileNotFoundError("source taxonomy CSV not found: {}".format(path))
    table = pd.read_csv(path, keep_default_na=False)
    source_col = _first_existing(table.columns, ("source", "source_name", "dataset", "source_dataset"))
    family_col = _first_existing(table.columns, ("family", "source_family", "class", "source_class"))
    if source_col is None or family_col is None:
        raise ValueError("taxonomy CSV must contain source and family/source_family columns")
    for _, row in table.iterrows():
        source = _clean_text(row.get(source_col, ""))
        family = _normalize_family_label(row.get(family_col, ""))
        if source and family:
            overrides[source.lower()] = family
    return overrides


def _normalize_family_label(value) -> str:
    text = _clean_text(value)
    key = _family_key(text)
    if not key:
        return ""
    if key in (
        "satellite",
        "satellite_data",
        "satellite_station",
        "satellite_derived",
        "satellite_derived_data",
        "remote_sensing",
        "remote",
        "reach_scale",
        "reachscale",
        "reach_scale_station",
    ):
        return "satellite"
    if key in (
        "in_situ",
        "insitu",
        "in_situ_data",
        "in_situ_station",
        "in_situ_station_data",
        "station",
        "station_data",
        "field",
        "field_observation",
        "field_observations",
        "gauge",
        "gauge_station",
        "observational",
        "monitoring_station",
        "monitoring_network",
    ):
        return "in_situ"
    if key in ("usgs", "hydat", "grdc", "hybam", "gfqa", "gfqa_v2"):
        return "in_situ"
    if key in ("secondary", "secondary_compilation", "compiled", "compilation"):
        return "secondary_compilation"
    if key in ("model", "modeled", "modelled"):
        return "model"
    if key in ("other", "unknown"):
        return "other"

    low = text.lower()
    if any(token in low for token in ("satellite", "remote sensing", "remote-sensing", "landsat", "reach-scale")):
        return "satellite"
    if any(token in low for token in ("in-situ", "in situ", "insitu", "station data", "gauge", "monitoring network")):
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
    "milliman": "in_situ",
    "milliman_farnsworth_global_river_sediment_database": "in_situ",
    "vanmaercke": "in_situ",
    "vanmaercke_et_al_2014_african_sediment_yield_database": "in_situ",
    "eusedcollab": "in_situ",
    "eusedcollab_dataset": "in_situ",
    "ali_de_boer": "in_situ",
    "ali_de_boer_dataset": "in_situ",
    "hma": "in_situ",
    "hma_dataset": "in_situ",
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
    if normalized_observation:
        return normalized_observation
    if normalized_raw:
        return normalized_raw
    return "other"


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
            return {str(key): _attr_value_to_text(value) for key, value in handle.attrs.items()}
    except Exception:
        pass

    ds = None
    try:
        ds = _open_dataset_compat(path)
        return {str(key): _attr_value_to_text(value) for key, value in getattr(ds, "attrs", {}).items()}
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
    for base in (PROJECT_DIR.parent.parent, release_dir.parent, release_dir.parent.parent, release_dir.parent.parent.parent):
        roots.append(base / "output_resolution_organized")
        roots.append(base / "Output_r" / "output_resolution_organized")
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
    """Attach canonical observation_type from source NetCDF global attributes."""
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
        progress("Reading observation_type attrs from {} source NetCDF files with {} worker(s)".format(len(tasks), workers))
    if workers == 1 or len(tasks) <= 1:
        for task in tasks:
            key, observation_type = _read_observation_type_worker(task)
            observation_by_path[key] = observation_type
    else:
        chunksize = max(1, min(200, int(math.ceil(len(tasks) / float(workers * 8)))))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for key, observation_type in executor.map(_read_observation_type_worker, tasks, chunksize=chunksize):
                observation_by_path[key] = observation_type

    attr_observation = pd.Series(resolved_paths, index=out.index).map(observation_by_path).fillna("").map(_clean_text)
    existing_col = _first_existing(out.columns, OBSERVATION_TYPE_ATTRS)
    if "observation_type" not in out.columns:
        out["observation_type"] = out[existing_col].map(_clean_text) if existing_col else ""
    else:
        out["observation_type"] = out["observation_type"].map(_clean_text)

    missing = out["observation_type"].astype(str).str.strip().eq("")
    out.loc[missing, "observation_type"] = attr_observation[missing]

    if progress:
        populated = int(attr_observation.astype(str).str.strip().ne("").sum())
        progress(
            "Loaded observation_type from {} source NetCDF files; populated {} observation rows".format(
                len(path_lookup),
                populated,
            )
        )
    return out


def _load_source_station_catalog(release_dir: Path) -> pd.DataFrame:
    path = release_dir / "source_station_catalog.csv"
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, keep_default_na=False)
    except Exception:
        return pd.DataFrame()


def load_observations_from_candidate_sidecar(
    release_dir: Path,
    candidate_sidecar: Optional[Path] = None,
    progress=log_progress,
) -> Tuple[pd.DataFrame, Optional[Path], str]:
    if candidate_sidecar is not None:
        paths = [candidate_sidecar]
    else:
        paths = [release_dir / name for name in CANDIDATE_SIDECAR_FILES]
    for path in paths:
        if path.exists():
            if progress:
                progress("Reading candidate sidecar: {}".format(path))
            return _read_table(path), path, "candidate_sidecar"
    return pd.DataFrame(), None, "candidate sidecar not found"


def load_observations_from_master_nc(release_dir: Path, progress=log_progress) -> Tuple[pd.DataFrame, str]:
    path = release_dir / MASTER_FILE
    if not path.exists():
        return pd.DataFrame(), "sed_reference_master.nc not found"
    try:
        import xarray as xr  # type: ignore
    except Exception as exc:
        return pd.DataFrame(), "xarray unavailable for master NetCDF reading: {}".format(exc)

    if progress:
        progress("Reading selected master records: {}".format(path))
    try:
        ds = _open_dataset_compat(path)
    except Exception as exc:
        return pd.DataFrame(), "cannot open master NetCDF: {}".format(exc)

    try:
        record_dim = _find_record_dim(ds)
        if record_dim is None:
            return pd.DataFrame(), "record dimension could not be inferred"
        n_records = int(ds.sizes[record_dim])
        records = pd.DataFrame({"record_index": np.arange(n_records)})

        for name in VARIABLES:
            series = _record_series(ds, name, record_dim)
            if series is not None and len(series) == n_records:
                records[name] = pd.to_numeric(series, errors="coerce")
            flag = _record_series(ds, "{}_flag".format(name), record_dim)
            if flag is not None and len(flag) == n_records:
                records["{}_flag".format(name)] = flag

        provenance_fields = (
            "resolution",
            "cluster_uid",
            "cluster_id",
            "source",
            "source_family",
            "observation_type",
            "source_station_uid",
            "source_station_paths",
            "candidate_path",
            "is_overlap",
            "river_width_class",
            "river_width_m",
            "climate_zone",
        )
        for name in provenance_fields:
            series = _record_series(ds, name, record_dim)
            if series is not None and len(series) == n_records:
                records[name] = series

        time_series = _record_series(ds, "time", record_dim)
        if time_series is not None and len(time_series) == n_records:
            records["time"] = time_series
            time_units = getattr(ds["time"], "units", "days since 1970-01-01")
            records["_time_units"] = time_units
        date_series = _record_series(ds, "date", record_dim)
        if date_series is not None and len(date_series) == n_records:
            records["date"] = date_series.astype(str)

        station_index = None
        for index_name in ("station_index", "master_station_index", "cluster_index"):
            candidate = _record_series(ds, index_name, record_dim)
            if candidate is not None and len(candidate) == n_records:
                station_index = candidate
                records[index_name] = candidate
                break
        if "cluster_uid" not in records.columns and station_index is not None:
            lookup = _indexed_lookup_series(ds, "cluster_uid", station_index)
            if lookup is not None:
                records["cluster_uid"] = lookup
        if "cluster_id" not in records.columns and station_index is not None:
            lookup = _indexed_lookup_series(ds, "cluster_id", station_index)
            if lookup is not None:
                records["cluster_id"] = lookup

        source_index = None
        for index_name in ("source_station_index", "selected_source_index", "source_index"):
            candidate = _record_series(ds, index_name, record_dim)
            if candidate is not None and len(candidate) == n_records:
                source_index = candidate
                records[index_name] = candidate
                break
        if "source_station_uid" not in records.columns and source_index is not None:
            lookup = _indexed_lookup_series(ds, "source_station_uid", source_index)
            if lookup is not None:
                records["source_station_uid"] = lookup
    finally:
        ds.close()

    catalog = _load_source_station_catalog(release_dir)
    if not catalog.empty and "source_station_uid" in records.columns:
        uid_col = _first_existing(catalog.columns, ("source_station_uid", "station_uid"))
        source_col = _first_existing(catalog.columns, ("source", "source_name", "source_dataset", "dataset", "dataset_name"))
        family_col = _first_existing(catalog.columns, ("source_family", "source_type", "source_category"))
        observation_col = _first_existing(catalog.columns, OBSERVATION_TYPE_ATTRS)
        path_col = _first_existing(catalog.columns, SOURCE_PATH_COLUMNS)
        extra_cols = []
        for col in (source_col, family_col, observation_col, path_col):
            if col is not None and col not in extra_cols:
                extra_cols.append(col)
        if uid_col is not None and extra_cols:
            lookup = catalog[[uid_col] + extra_cols].drop_duplicates(uid_col)
            records = records.merge(
                lookup,
                how="left",
                left_on="source_station_uid",
                right_on=uid_col,
                suffixes=("", "_catalog"),
            )
            merged_source_col = None
            if source_col:
                if source_col in records.columns and source_col != "source":
                    merged_source_col = source_col
                elif "{}_catalog".format(source_col) in records.columns:
                    merged_source_col = "{}_catalog".format(source_col)
                elif source_col in records.columns:
                    merged_source_col = source_col
            merged_family_col = None
            if family_col:
                if family_col in records.columns and family_col != "source_family":
                    merged_family_col = family_col
                elif "{}_catalog".format(family_col) in records.columns:
                    merged_family_col = "{}_catalog".format(family_col)
                elif family_col in records.columns:
                    merged_family_col = family_col
            merged_observation_col = None
            if observation_col:
                if observation_col in records.columns and observation_col != "observation_type":
                    merged_observation_col = observation_col
                elif "{}_catalog".format(observation_col) in records.columns:
                    merged_observation_col = "{}_catalog".format(observation_col)
                elif observation_col in records.columns:
                    merged_observation_col = observation_col
            merged_path_col = None
            if path_col:
                if path_col in records.columns and path_col != "source_station_paths":
                    merged_path_col = path_col
                elif "{}_catalog".format(path_col) in records.columns:
                    merged_path_col = "{}_catalog".format(path_col)
                elif path_col in records.columns:
                    merged_path_col = path_col
            if merged_source_col and "source" not in records.columns:
                records["source"] = records[merged_source_col]
            if merged_family_col and "source_family" not in records.columns:
                records["source_family"] = records[merged_family_col]
            if merged_observation_col and "observation_type" not in records.columns:
                records["observation_type"] = records[merged_observation_col]
            if merged_path_col and "source_station_paths" not in records.columns:
                records["source_station_paths"] = records[merged_path_col]
            if source_col and "source" in records.columns:
                missing = records["source"].astype(str).str.strip().eq("")
                if merged_source_col:
                    records.loc[missing, "source"] = records.loc[missing, merged_source_col]
            if family_col and "source_family" in records.columns:
                missing = records["source_family"].astype(str).str.strip().eq("")
                if merged_family_col:
                    records.loc[missing, "source_family"] = records.loc[missing, merged_family_col]
            if observation_col and "observation_type" in records.columns:
                missing = records["observation_type"].astype(str).str.strip().eq("")
                if merged_observation_col:
                    records.loc[missing, "observation_type"] = records.loc[missing, merged_observation_col]
            if path_col and "source_station_paths" in records.columns:
                missing = records["source_station_paths"].astype(str).str.strip().eq("")
                if merged_path_col:
                    records.loc[missing, "source_station_paths"] = records.loc[missing, merged_path_col]

    return records, "selected master records loaded"


def load_observations_from_satellite_validation_nc(release_dir: Path, progress=log_progress) -> Tuple[pd.DataFrame, str]:
    path = release_dir / SATELLITE_VALIDATION_FILE
    if not path.exists():
        return pd.DataFrame(), "satellite validation NetCDF not found"
    try:
        import xarray as xr  # type: ignore
    except Exception as exc:
        return pd.DataFrame(), "xarray unavailable for satellite NetCDF reading: {}".format(exc)

    if progress:
        progress("Reading satellite validation records: {}".format(path))
    try:
        ds = _open_dataset_compat(path)
    except Exception as exc:
        return pd.DataFrame(), "cannot open satellite validation NetCDF: {}".format(exc)

    try:
        record_dim = "n_satellite_records" if "n_satellite_records" in ds.sizes else _find_record_dim(ds)
        if record_dim is None:
            return pd.DataFrame(), "satellite record dimension could not be inferred"
        n_records = int(ds.sizes[record_dim])
        records = pd.DataFrame({"record_index": np.arange(n_records)})

        for name in (
            "cluster_uid",
            "cluster_id",
            "resolution",
            "source",
            "source_family",
            "observation_type",
            "source_station_index",
            "source_station_uid",
            "source_station_paths",
            "satellite_station_uid",
            "validation_only",
            "merge_policy",
        ):
            series = _record_series(ds, name, record_dim)
            if series is not None and len(series) == n_records:
                records[name] = series

        if "source_station_uid" not in records.columns:
            if "satellite_station_uid" in records.columns:
                records["source_station_uid"] = records["satellite_station_uid"]
            elif "source_station_index" in records.columns:
                lookup = _indexed_lookup_series(ds, "satellite_station_uid", records["source_station_index"])
                if lookup is not None:
                    records["source_station_uid"] = lookup

        for name in VARIABLES:
            series = _record_series(ds, name, record_dim)
            if series is not None and len(series) == n_records:
                records[name] = pd.to_numeric(series, errors="coerce")
            flag = _record_series(ds, "{}_flag".format(name), record_dim)
            if flag is not None and len(flag) == n_records:
                records["{}_flag".format(name)] = flag

        time_series = _record_series(ds, "time", record_dim)
        if time_series is not None and len(time_series) == n_records:
            records["time"] = time_series
            time_units = getattr(ds["time"], "units", "days since 1970-01-01")
            records["_time_units"] = time_units
        date_series = _record_series(ds, "date", record_dim)
        if date_series is not None and len(date_series) == n_records:
            records["date"] = date_series.astype(str)
    finally:
        ds.close()

    return records, "satellite validation records loaded"


def _build_satellite_candidate_windows(
    raw: pd.DataFrame,
    windows: Sequence[str],
    satellite_time_units: str,
) -> Tuple[Dict[Tuple[int, str], Dict[str, object]], int]:
    if raw.empty or "cluster_id" not in raw.columns:
        return {}, 0

    work = pd.DataFrame(index=raw.index)
    work["cluster_id"] = pd.to_numeric(raw["cluster_id"], errors="coerce")
    work["cluster_uid"] = _extract_column(raw, ("cluster_uid", "station_uid", "cluster_uuid"), "").map(_clean_text)
    work["resolution"] = _extract_column(raw, ("resolution", "time_resolution", "temporal_resolution"), "").map(_normalize_resolution)
    work["time"] = _coerce_datetime_from_columns(raw)
    valid = work["cluster_id"].notna() & work["resolution"].astype(str).str.strip().ne("") & work["time"].notna()
    work = work[valid].copy()
    if work.empty:
        return {}, 0

    max_window = max(WINDOW_DAYS[window] for window in windows) if windows else 0
    ranges: Dict[Tuple[int, str], Dict[str, object]] = {}
    for (cluster_id, resolution), group in _groupby_compat(work, ["cluster_id", "resolution"]):
        if pd.isna(cluster_id):
            continue
        start = group["time"].min() - pd.Timedelta(days=max_window)
        end = group["time"].max() + pd.Timedelta(days=max_window)
        start_day = _datetime_to_cf_days(pd.Series([start]), satellite_time_units).iloc[0]
        end_day = _datetime_to_cf_days(pd.Series([end]), satellite_time_units).iloc[0]
        if pd.isna(start_day) or pd.isna(end_day):
            continue
        cluster_uid = ""
        nonempty_uid = group["cluster_uid"][group["cluster_uid"].astype(str).str.strip().ne("")]
        if len(nonempty_uid):
            cluster_uid = _clean_text(nonempty_uid.iloc[0])
        key = (int(cluster_id), _normalize_resolution(resolution))
        ranges[key] = {
            "cluster_uid": cluster_uid,
            "start_day": float(start_day),
            "end_day": float(end_day),
        }
    return ranges, int(work[["cluster_id", "resolution"]].drop_duplicates().shape[0])


def _station_series(ds, name: str, station_dim: str, default="") -> pd.Series:
    if name not in ds.variables or station_dim not in ds[name].dims:
        return pd.Series([default] * int(ds.sizes[station_dim]))
    series = _series_from_data_array(ds[name]).reset_index(drop=True)
    if len(series) != int(ds.sizes[station_dim]):
        return pd.Series([default] * int(ds.sizes[station_dim]))
    return series


def _load_matching_satellite_station_metadata(
    ds,
    candidate_windows: Dict[Tuple[int, str], Dict[str, object]],
) -> Tuple[Dict[int, Dict[str, object]], int]:
    station_dim = "n_satellite_stations" if "n_satellite_stations" in ds.sizes else None
    if station_dim is None:
        return {}, 0
    n_stations = int(ds.sizes[station_dim])
    cluster_name = "cluster_id_station" if "cluster_id_station" in ds.variables else "cluster_id"
    if cluster_name not in ds.variables:
        return {}, 0

    cluster_ids = pd.to_numeric(_station_series(ds, cluster_name, station_dim, np.nan), errors="coerce")
    resolutions = _station_series(ds, "station_resolution", station_dim, "").map(_normalize_resolution)
    cluster_uids = _station_series(ds, "cluster_uid", station_dim, "").map(_clean_text)
    sources = _station_series(ds, "source", station_dim, "").map(_clean_text)
    families = _station_series(ds, "source_family", station_dim, "satellite").map(_clean_text)
    station_uids = _station_series(ds, "satellite_station_uid", station_dim, "").map(_clean_text)
    native_ids = _station_series(ds, "source_station_native_id", station_dim, "").map(_clean_text)
    candidate_paths = _station_series(ds, "candidate_path", station_dim, "").map(_clean_text)
    resolved_paths = _station_series(ds, "resolved_candidate_path", station_dim, "").map(_clean_text)

    meta_by_station: Dict[int, Dict[str, object]] = {}
    for idx in range(n_stations):
        cluster_id = cluster_ids.iloc[idx]
        if pd.isna(cluster_id):
            continue
        resolution = resolutions.iloc[idx]
        key = (int(cluster_id), resolution)
        if key not in candidate_windows:
            continue
        family = classify_source_family(sources.iloc[idx], raw_family=families.iloc[idx], observation_type="Satellite")
        if family != "satellite":
            continue
        cluster_uid = cluster_uids.iloc[idx] or _clean_text(candidate_windows[key].get("cluster_uid", ""))
        meta_by_station[idx] = {
            "cluster_id": int(cluster_id),
            "cluster_uid": cluster_uid,
            "resolution": resolution,
            "source": sources.iloc[idx],
            "source_family": "satellite",
            "observation_type": "Satellite",
            "source_station_uid": station_uids.iloc[idx],
            "source_station_native_id": native_ids.iloc[idx],
            "candidate_path": candidate_paths.iloc[idx],
            "source_station_paths": resolved_paths.iloc[idx] or candidate_paths.iloc[idx],
        }
    return meta_by_station, len(set((int(cid), res) for cid, res in zip(cluster_ids.dropna().astype(int), resolutions[cluster_ids.notna()])))


def _satellite_chunk_worker(
    item: Tuple[int, str, int, int, Dict[int, Dict[str, object]], Dict[Tuple[int, str], Dict[str, object]], str]
) -> Tuple[int, pd.DataFrame, Dict[str, int]]:
    ordinal, path_text, start, stop, meta_by_station, candidate_windows, time_units = item
    stats = {
        "chunks": 1,
        "records_scanned": max(0, int(stop) - int(start)),
        "station_hits": 0,
        "time_hits": 0,
        "value_hits": 0,
    }
    rows: List[Dict[str, object]] = []
    ds = _open_dataset_compat(Path(path_text))
    try:
        record_slice = slice(start, stop)
        station_idx = np.asarray(ds["satellite_station_index"].values[record_slice])
        times = np.asarray(ds["time"].values[record_slice], dtype=float)
        station_mask = np.asarray([int(idx) in meta_by_station for idx in station_idx], dtype=bool)
        stats["station_hits"] = int(station_mask.sum())
        if not stats["station_hits"]:
            return ordinal, pd.DataFrame(), stats

        keep_positions = []
        for local_pos in np.where(station_mask)[0]:
            station_integer = int(station_idx[local_pos])
            meta = meta_by_station[station_integer]
            key = (int(meta["cluster_id"]), _normalize_resolution(meta["resolution"]))
            window = candidate_windows.get(key)
            if not window:
                continue
            time_value = times[local_pos]
            if np.isfinite(time_value) and float(window["start_day"]) <= float(time_value) <= float(window["end_day"]):
                keep_positions.append(int(local_pos))
        stats["time_hits"] = len(keep_positions)
        if not keep_positions:
            return ordinal, pd.DataFrame(), stats

        keep_positions_arr = np.asarray(keep_positions, dtype=int)
        values: Dict[str, np.ndarray] = {}
        flags: Dict[str, np.ndarray] = {}
        for variable in VARIABLES:
            if variable in ds.variables:
                values[variable] = np.asarray(ds[variable].isel(n_satellite_records=keep_positions_arr + start).values)
            else:
                values[variable] = np.full(len(keep_positions_arr), np.nan)
            flag_name = "{}_flag".format(variable)
            if flag_name in ds.variables:
                flags[variable] = np.asarray(ds[flag_name].isel(n_satellite_records=keep_positions_arr + start).values)
            else:
                flags[variable] = np.full(len(keep_positions_arr), np.nan)

        finite_any = np.zeros(len(keep_positions_arr), dtype=bool)
        for variable in VARIABLES:
            finite_any |= np.isfinite(pd.to_numeric(pd.Series(values[variable]), errors="coerce").to_numpy(dtype=float))
        stats["value_hits"] = int(finite_any.sum())
        if not stats["value_hits"]:
            return ordinal, pd.DataFrame(), stats

        for out_pos, local_pos in enumerate(keep_positions_arr):
            if not finite_any[out_pos]:
                continue
            global_index = int(start) + int(local_pos)
            station_integer = int(station_idx[local_pos])
            meta = meta_by_station[station_integer]
            row: Dict[str, object] = {
                "record_index": "satellite:{}".format(global_index),
                "cluster_uid": meta.get("cluster_uid", ""),
                "cluster_id": meta.get("cluster_id", ""),
                "resolution": meta.get("resolution", ""),
                "source": meta.get("source", ""),
                "source_family": "satellite",
                "observation_type": "Satellite",
                "source_station_uid": meta.get("source_station_uid", ""),
                "source_station_native_id": meta.get("source_station_native_id", ""),
                "source_station_paths": meta.get("source_station_paths", ""),
                "candidate_path": meta.get("candidate_path", ""),
                "time": float(times[local_pos]) if np.isfinite(times[local_pos]) else np.nan,
                "_time_units": time_units,
                "validation_only": 1,
                "merge_policy": "validation_only",
            }
            if np.isfinite(times[local_pos]):
                parsed_date = _parse_cf_days_since(pd.Series([float(times[local_pos])]), time_units).iloc[0]
                row["date"] = parsed_date.strftime("%Y-%m-%d") if pd.notna(parsed_date) else ""
            else:
                row["date"] = ""
            for variable in VARIABLES:
                row[variable] = values[variable][out_pos]
                row["{}_flag".format(variable)] = flags[variable][out_pos]
            rows.append(row)
    finally:
        ds.close()
    return ordinal, pd.DataFrame(rows), stats


def load_relevant_satellite_validation_records(
    release_dir: Path,
    candidate_rows: pd.DataFrame,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    workers: int = 1,
    chunk_size: int = DEFAULT_SATELLITE_CHUNK_SIZE,
    progress=log_progress,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    stats = {
        "candidate_keys": 0,
        "satellite_catalog_keys": 0,
        "matching_station_count": 0,
        "chunks": 0,
        "records_scanned": 0,
        "station_hits": 0,
        "time_hits": 0,
        "value_hits": 0,
        "satellite_rows": 0,
        "missing_file": 0,
        "no_common_key": 0,
        "no_matching_station": 0,
        "no_time_overlap": 0,
        "no_variable_values": 0,
    }
    path = release_dir / SATELLITE_VALIDATION_FILE
    if not path.exists():
        stats["missing_file"] = 1
        return pd.DataFrame(), stats

    ds = _open_dataset_compat(path)
    try:
        time_units = getattr(ds["time"], "units", "days since 1970-01-01") if "time" in ds.variables else "days since 1970-01-01"
        candidate_windows, candidate_key_count = _build_satellite_candidate_windows(candidate_rows, windows, time_units)
        stats["candidate_keys"] = int(candidate_key_count)
        if not candidate_windows:
            stats["no_common_key"] = 1
            return pd.DataFrame(), stats

        meta_by_station, satellite_catalog_keys = _load_matching_satellite_station_metadata(ds, candidate_windows)
        stats["satellite_catalog_keys"] = int(satellite_catalog_keys)
        stats["matching_station_count"] = int(len(meta_by_station))
        if not meta_by_station:
            stats["no_matching_station"] = 1
            return pd.DataFrame(), stats

        n_records = int(ds.sizes.get("n_satellite_records", 0))
    finally:
        ds.close()

    chunk_size = max(1, int(chunk_size or DEFAULT_SATELLITE_CHUNK_SIZE))
    workers = max(1, int(workers or 1))
    chunks = [(start, min(start + chunk_size, n_records)) for start in range(0, n_records, chunk_size)]
    stats["chunks"] = len(chunks)
    if progress:
        progress(
            "Satellite validation NC: candidate_keys={}, matching_stations={}, chunks={}, workers={}".format(
                stats["candidate_keys"],
                stats["matching_station_count"],
                stats["chunks"],
                workers,
            )
        )

    tasks = [
        (ordinal, str(path), start, stop, meta_by_station, candidate_windows, time_units)
        for ordinal, (start, stop) in enumerate(chunks)
    ]
    frames = []
    results = []
    if workers == 1 or len(tasks) <= 1:
        results = [_satellite_chunk_worker(task) for task in tasks]
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_satellite_chunk_worker, tasks, chunksize=1))

    for _, frame, chunk_stats in sorted(results, key=lambda item: item[0]):
        for key in ("records_scanned", "station_hits", "time_hits", "value_hits"):
            stats[key] += int(chunk_stats.get(key, 0))
        if not frame.empty:
            frames.append(frame)

    if not frames:
        if stats["station_hits"] == 0:
            stats["no_matching_station"] = 1
        elif stats["time_hits"] == 0:
            stats["no_time_overlap"] = 1
        elif stats["value_hits"] == 0:
            stats["no_variable_values"] = 1
        return pd.DataFrame(), stats

    out = pd.concat(frames, ignore_index=True)
    stats["satellite_rows"] = int(len(out))
    return out, stats


def _normalize_resolution(value) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return RESOLUTION_CODE.get(int(numeric), text)
    return text.lower()


def _extract_column(df: pd.DataFrame, candidates: Sequence[str], default="") -> pd.Series:
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
            "flag",
            "qc_flag",
            "quality_flag",
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
    record_id_col = _first_existing(raw.columns, ("record_id", "record_index", "candidate_id", "row_id"))
    out["record_id"] = raw[record_id_col].astype(str) if record_id_col else np.arange(len(raw)).astype(str)
    out["cluster_uid"] = _extract_column(raw, ("cluster_uid", "station_uid", "cluster_uuid"), "")
    out["cluster_id"] = _extract_column(raw, ("cluster_id", "station_id", "master_station_index", "station_index"), "")
    out["resolution"] = _extract_column(raw, ("resolution", "time_resolution", "temporal_resolution"), "").map(_normalize_resolution)
    out["time"] = _coerce_datetime_from_columns(raw)
    if "date" not in raw.columns and "_time_units" in raw.columns and "time" in raw.columns:
        out["time"] = _parse_cf_days_since(raw["time"], _clean_text(raw["_time_units"].iloc[0])).dt.floor("D")

    out["source_station_uid"] = _extract_column(
        raw,
        ("source_station_uid", "station_uid", "source_station_id", "source_station_native_id"),
        "",
    ).map(_clean_text)
    source_col = _first_existing(raw.columns, ("source", "source_name", "source_dataset", "dataset", "dataset_name"))
    family_col = _first_existing(raw.columns, ("source_family", "source_type", "source_category", "family"))
    observation_col = _first_existing(raw.columns, OBSERVATION_TYPE_ATTRS)
    out["source"] = raw[source_col].map(_clean_text) if source_col else ""
    missing_source = out["source"].astype(str).str.strip().eq("")
    out.loc[missing_source, "source"] = out.loc[missing_source, "source_station_uid"]
    raw_family = raw[family_col].map(_clean_text) if family_col else pd.Series([""] * len(raw), index=raw.index)
    raw_observation = (
        raw[observation_col].map(_clean_text)
        if observation_col
        else pd.Series([""] * len(raw), index=raw.index)
    )
    out["observation_type"] = raw_observation
    out["source_family"] = [
        classify_source_family(source, taxonomy_overrides, family, observation)
        for source, family, observation in zip(out["source"], raw_family, raw_observation)
    ]

    for variable in VARIABLES:
        col = _first_existing(raw.columns, (variable, variable.lower()))
        out[variable] = pd.to_numeric(raw[col], errors="coerce") if col is not None else np.nan
        out["{}_flag".format(variable)] = _flag_series(raw, variable)

    for canonical, candidates in (
        ("river_width_class", ("river_width_class", "width_class", "river_width_category")),
        ("river_width_m", ("river_width_m", "width_m", "river_width", "bankfull_width_m")),
        ("climate_zone", ("climate_zone", "hydroatlas_climate_zone", "koppen_zone", "koppen", "climate_class")),
    ):
        col = _first_existing(raw.columns, candidates)
        if col is not None:
            out[canonical] = raw[col]

    out["input_mode"] = input_mode
    has_cluster = out["cluster_uid"].astype(str).str.strip().ne("") | out["cluster_id"].astype(str).str.strip().ne("")
    has_core = has_cluster & out["resolution"].astype(str).str.strip().ne("") & out["time"].notna()
    has_source = out["source"].astype(str).str.strip().ne("")
    return out[has_core & has_source].reset_index(drop=True)


def _flag_rank(value) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        return float(numeric)
    text = _clean_text(value).lower()
    if not text:
        return 9.0
    good = ("good", "valid", "pass", "passed", "ok", "a")
    suspect = ("suspect", "questionable", "estimated", "estimate", "b")
    bad = ("bad", "fail", "failed", "invalid", "reject", "rejected")
    if text in good:
        return 0.0
    if text in suspect:
        return 2.0
    if text in bad:
        return 9.0
    return 5.0


def _cluster_group_key(df: pd.DataFrame) -> pd.Series:
    uid = df["cluster_uid"].astype(str).str.strip()
    cid = df["cluster_id"].astype(str).str.strip()
    return uid.where(uid.ne(""), cid)


def _method_notes(input_mode: str) -> str:
    return "{}; input_mode={}; window_exclusive={}".format(METHOD_NOTES_BASE, input_mode, str(WINDOW_EXCLUSIVE).lower())


def _pair_group_worker(item: Tuple[int, pd.DataFrame, Tuple[str, ...], str]) -> Tuple[int, List[Dict[str, object]]]:
    ordinal, group, windows, input_mode = item
    rows: List[Dict[str, object]] = []
    satellites = group[group["source_family"] == "satellite"].copy()
    insitu = group[group["source_family"] == "in_situ"].copy()
    if satellites.empty or insitu.empty:
        return ordinal, rows

    for _, sat in satellites.iterrows():
        for variable in VARIABLES:
            sat_value = pd.to_numeric(pd.Series([sat.get(variable, np.nan)]), errors="coerce").iloc[0]
            if pd.isna(sat_value):
                continue
            insitu_valid = insitu[pd.to_numeric(insitu[variable], errors="coerce").notna()].copy()
            if insitu_valid.empty:
                continue
            deltas = (insitu_valid["_time_day"] - sat["_time_day"]).dt.days
            insitu_valid = insitu_valid.assign(
                _time_delta_days=deltas,
                _abs_delta=deltas.abs(),
                _flag_rank=insitu_valid["{}_flag".format(variable)].map(_flag_rank),
                _source_sort=insitu_valid["source"].astype(str).str.lower(),
                _uid_sort=insitu_valid["source_station_uid"].astype(str).str.lower(),
                _record_sort=insitu_valid["record_id"].astype(str),
            )
            for window in windows:
                max_days = WINDOW_DAYS[window]
                if window == "exact":
                    candidates = insitu_valid[insitu_valid["_abs_delta"] == 0].copy()
                else:
                    candidates = insitu_valid[insitu_valid["_abs_delta"] <= max_days].copy()
                if candidates.empty:
                    continue
                best = candidates.sort_values(
                    ["_abs_delta", "_flag_rank", "_source_sort", "_uid_sort", "_time_day", "_record_sort"],
                    kind="mergesort",
                ).iloc[0]
                insitu_value = pd.to_numeric(pd.Series([best.get(variable, np.nan)]), errors="coerce").iloc[0]
                diff = float(sat_value) - float(insitu_value)
                pct = float(diff / float(insitu_value) * 100.0) if float(insitu_value) != 0 else float("nan")
                rows.append(
                    {
                        "cluster_uid": sat.get("cluster_uid", ""),
                        "cluster_id": sat.get("cluster_id", ""),
                        "resolution": sat.get("resolution", ""),
                        "variable": variable,
                        "pairing_window": window,
                        "window_exclusive": WINDOW_EXCLUSIVE,
                        "satellite_time": sat["_time_day"],
                        "insitu_time": best["_time_day"],
                        "time_delta_days": int(best["_time_delta_days"]),
                        "satellite_source": sat.get("source", ""),
                        "insitu_source": best.get("source", ""),
                        "satellite_source_family": sat.get("source_family", ""),
                        "insitu_source_family": best.get("source_family", ""),
                        "satellite_source_station_uid": sat.get("source_station_uid", ""),
                        "insitu_source_station_uid": best.get("source_station_uid", ""),
                        "satellite_record_id": sat.get("record_id", ""),
                        "insitu_record_id": best.get("record_id", ""),
                        "satellite_value": float(sat_value),
                        "insitu_value": float(insitu_value),
                        "diff_satellite_minus_insitu": diff,
                        "pct_error_vs_insitu": pct,
                        "satellite_flag": sat.get("{}_flag".format(variable), np.nan),
                        "insitu_flag": best.get("{}_flag".format(variable), np.nan),
                        "source_pair": "{} vs {}".format(sat.get("source", ""), best.get("source", "")),
                        "satellite_ssc": sat.get("SSC", np.nan),
                        "insitu_ssc": best.get("SSC", np.nan),
                        "satellite_river_width_class": sat.get("river_width_class", ""),
                        "insitu_river_width_class": best.get("river_width_class", ""),
                        "satellite_river_width_m": sat.get("river_width_m", np.nan),
                        "insitu_river_width_m": best.get("river_width_m", np.nan),
                        "satellite_climate_zone": sat.get("climate_zone", ""),
                        "insitu_climate_zone": best.get("climate_zone", ""),
                        "method_notes": _method_notes(input_mode),
                        "assumptions": ASSUMPTIONS_BASE,
                    }
                )
    return ordinal, rows


def pair_satellite_insitu_records(
    observations: pd.DataFrame,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    input_mode: str = "",
    workers: int = 1,
    progress=log_progress,
) -> pd.DataFrame:
    columns = [
        "cluster_uid",
        "cluster_id",
        "resolution",
        "variable",
        "pairing_window",
        "window_exclusive",
        "satellite_time",
        "insitu_time",
        "time_delta_days",
        "satellite_source",
        "insitu_source",
        "satellite_source_family",
        "insitu_source_family",
        "satellite_source_station_uid",
        "insitu_source_station_uid",
        "satellite_record_id",
        "insitu_record_id",
        "satellite_value",
        "insitu_value",
        "diff_satellite_minus_insitu",
        "pct_error_vs_insitu",
        "satellite_flag",
        "insitu_flag",
        "source_pair",
        "satellite_ssc",
        "insitu_ssc",
        "satellite_river_width_class",
        "insitu_river_width_class",
        "satellite_river_width_m",
        "insitu_river_width_m",
        "satellite_climate_zone",
        "insitu_climate_zone",
        "method_notes",
        "assumptions",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)

    work = observations.copy()
    work["_cluster_key"] = _cluster_group_key(work)
    work["_time_day"] = pd.to_datetime(work["time"], errors="coerce").dt.floor("D")
    rows: List[Dict[str, object]] = []

    group_cols = ["_cluster_key", "resolution"]
    tasks = []
    for ordinal, (_, group) in enumerate(_groupby_compat(work, group_cols)):
        families = set(group["source_family"].astype(str))
        if "satellite" in families and "in_situ" in families:
            tasks.append((ordinal, group.copy(), tuple(windows), input_mode))

    workers = max(1, int(workers or 1))
    if progress:
        progress("Pairing {} eligible cluster/resolution groups with {} worker(s)".format(len(tasks), workers))

    if workers == 1 or len(tasks) <= 1:
        results = [_pair_group_worker(task) for task in tasks]
    else:
        chunksize = max(1, min(50, int(math.ceil(len(tasks) / float(workers * 8)))))
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_pair_group_worker, tasks, chunksize=chunksize))

    for _, group_rows in sorted(results, key=lambda item: item[0]):
        rows.extend(group_rows)
    return pd.DataFrame(rows, columns=columns)


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
    resolution_col = _first_existing(attr_cols, ("resolution", "time_resolution", "temporal_resolution"))
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

    rename = {}
    for col in attr.columns:
        if col in right_keys:
            continue
        rename[col] = "external_{}".format(col) if col in work.columns else col
    attr = attr.rename(columns=rename)
    right_keys = [rename.get(col, col) for col in right_keys]
    attr = attr.drop_duplicates(right_keys)
    return work.merge(attr, how="left", left_on=left_keys, right_on=right_keys, suffixes=("", "_external"))


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
            value = pd.to_numeric(pd.Series([row.get(name, np.nan)]), errors="coerce").iloc[0]
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

    work = _merge_external_attributes(pair_records, external_attributes if external_attributes is not None else pd.DataFrame())
    ssc_values = []
    ssc_bins = []
    width_classes = []
    climate_zones = []
    high_turbidity = []
    for _, row in work.iterrows():
        ssc = _first_numeric(row, ("insitu_ssc", "satellite_ssc", "SSC", "external_SSC"))
        ssc_values.append(ssc)
        ssc_bins.append(_bin_label(ssc, ssc_bin_edges))
        high_turbidity.append(bool(np.isfinite(ssc) and ssc >= float(high_turbidity_ssc)))

        width_class = _first_nonempty(
            row,
            (
                "river_width_class",
                "external_river_width_class",
                "insitu_river_width_class",
                "satellite_river_width_class",
                "width_class",
                "external_width_class",
            ),
        )
        if not width_class:
            width = _first_numeric(
                row,
                (
                    "river_width_m",
                    "external_river_width_m",
                    "insitu_river_width_m",
                    "satellite_river_width_m",
                    "width_m",
                    "external_width_m",
                    "river_width",
                    "external_river_width",
                ),
            )
            width_class = _width_class_from_numeric(width)
        width_classes.append(width_class or "missing")

        climate = _first_nonempty(
            row,
            (
                "climate_zone",
                "external_climate_zone",
                "insitu_climate_zone",
                "satellite_climate_zone",
                "hydroatlas_climate_zone",
                "external_hydroatlas_climate_zone",
                "koppen_zone",
                "external_koppen_zone",
                "koppen",
                "external_koppen",
                "climate_class",
                "external_climate_class",
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
    if len(a) < 2:
        return float("nan")
    if np.nanstd(a) == 0 or np.nanstd(b) == 0:
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
            "bias": float("nan"),
            "RMSE": float("nan"),
            "MAE": float("nan"),
            "MAPE": float("nan"),
            "median_absolute_error": float("nan"),
            "Pearson": float("nan"),
            "Spearman": float("nan"),
            "R2": float("nan"),
            "n_pairs": 0,
        }
    diff = sat - insitu
    mape_mask = insitu != 0
    pearson = _safe_corr(insitu, sat, "pearson")
    return {
        "bias": float(np.nanmean(diff)),
        "RMSE": float(np.sqrt(np.nanmean(diff ** 2))),
        "MAE": float(np.nanmean(np.abs(diff))),
        "MAPE": float(np.nanmean(np.abs(diff[mape_mask] / insitu[mape_mask]) * 100.0)) if np.any(mape_mask) else float("nan"),
        "median_absolute_error": float(np.nanmedian(np.abs(diff))),
        "Pearson": pearson,
        "Spearman": _safe_corr(insitu, sat, "spearman"),
        "R2": float(pearson ** 2) if np.isfinite(pearson) else float("nan"),
        "n_pairs": int(len(sat)),
    }


def compute_satellite_insitu_metrics(pair_records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "group_type",
        "pairing_window",
        "window_exclusive",
        "variable",
        "source_pair",
        "ssc_bin",
        "river_width_class",
        "climate_zone",
        "high_turbidity",
        "bias",
        "RMSE",
        "MAE",
        "MAPE",
        "median_absolute_error",
        "Pearson",
        "Spearman",
        "R2",
        "n_pairs",
        "n_clusters",
        "method_notes",
        "assumptions",
    ]
    if pair_records.empty:
        return pd.DataFrame(columns=columns)

    group_specs = {
        "overall": [],
        "source_pair": ["source_pair"],
        "source_pair_ssc_bin": ["source_pair", "ssc_bin"],
        "source_pair_width": ["source_pair", "river_width_class"],
        "source_pair_climate": ["source_pair", "climate_zone"],
        "source_pair_high_turbidity": ["source_pair", "high_turbidity"],
        "full_strata": ["source_pair", "ssc_bin", "river_width_class", "climate_zone", "high_turbidity"],
    }
    rows: List[Dict[str, object]] = []
    base_cols = ["pairing_window", "variable"]
    for group_type, strata_cols in group_specs.items():
        cols = base_cols + strata_cols
        for keys, group in _groupby_compat(pair_records, cols):
            if not isinstance(keys, tuple):
                keys = (keys,)
            values = dict(zip(cols, keys))
            metrics = _metric_values(group)
            cluster_key = _cluster_group_key(group)
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
                "n_clusters": int(cluster_key.nunique()),
                "method_notes": str(group["method_notes"].iloc[0]) if "method_notes" in group else METHOD_NOTES_BASE,
                "assumptions": str(group["assumptions"].iloc[0]) if "assumptions" in group else ASSUMPTIONS_BASE,
            }
            row.update(metrics)
            rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def _write_scatter_by_window(pair_records: pd.DataFrame, figures_dir: Path) -> Tuple[str, str]:
    subset = pair_records[pair_records["variable"] == "SSC"].copy()
    rel = "figures/satellite_insitu_scatter_by_window_SSC.png"
    if len(subset) < 1:
        return rel, "skipped: no SSC pairs"
    import matplotlib.pyplot as plt  # type: ignore

    windows = [window for window in ("exact", "pm1d", "pm2d") if window in set(subset["pairing_window"])]
    if not windows:
        return rel, "skipped: no configured windows"
    fig, axes = plt.subplots(1, len(windows), figsize=(5 * len(windows), 4), squeeze=False)
    for ax, window in zip(axes[0], windows):
        part = subset[subset["pairing_window"] == window]
        ax.scatter(part["insitu_value"], part["satellite_value"], s=14, alpha=0.65)
        finite = pd.to_numeric(part[["insitu_value", "satellite_value"]].stack(), errors="coerce")
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


def _write_residual_by_ssc_bin(pair_records: pd.DataFrame, figures_dir: Path) -> Tuple[str, str]:
    subset = pair_records[pair_records["variable"] == "SSC"].copy()
    rel = "figures/satellite_insitu_residual_by_ssc_bin.png"
    if len(subset) < 1:
        return rel, "skipped: no SSC pairs"
    import matplotlib.pyplot as plt  # type: ignore

    grouped = [(label, group["diff_satellite_minus_insitu"].dropna().astype(float).values) for label, group in subset.groupby("ssc_bin")]
    grouped = [(label, values) for label, values in grouped if len(values)]
    if not grouped:
        return rel, "skipped: no finite SSC residuals"
    labels = [label for label, _ in grouped]
    data = [values for _, values in grouped]
    fig, ax = plt.subplots(figsize=(max(7, len(labels) * 1.2), 4.5))
    ax.boxplot(data, labels=labels, showfliers=False)
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


def _write_metric_heatmap(metrics: pd.DataFrame, figures_dir: Path) -> Tuple[str, str]:
    rel = "figures/satellite_insitu_metric_heatmap.png"
    subset = metrics[
        (metrics["group_type"] == "source_pair")
        & (metrics["variable"] == "SSC")
        & pd.to_numeric(metrics["RMSE"], errors="coerce").notna()
    ].copy()
    if subset.empty:
        return rel, "skipped: no SSC source-pair RMSE metrics"
    import matplotlib.pyplot as plt  # type: ignore

    pivot = subset.pivot_table(index="source_pair", columns="pairing_window", values="RMSE", aggfunc="first")
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
    generated = []
    generated.append(_write_scatter_by_window(pair_records, figures_dir))
    generated.append(_write_residual_by_ssc_bin(pair_records, figures_dir))
    generated.append(_write_metric_heatmap(metrics, figures_dir))
    return generated


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
            preview = preview.sort_values(["n_pairs", "pairing_window", "variable"], ascending=[False, True, True]).head(12)
            for _, row in preview.iterrows():
                lines.append(
                    "- `{}` / `{}` / `{}`: n_pairs={}, n_clusters={}, bias={}, RMSE={}, Spearman={}.".format(
                        row.get("pairing_window", ""),
                        row.get("variable", ""),
                        row.get("source_pair", ""),
                        row.get("n_pairs", ""),
                        row.get("n_clusters", ""),
                        row.get("bias", ""),
                        row.get("RMSE", ""),
                        row.get("Spearman", ""),
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


def _find_candidate_sidecar(release_dir: Path, explicit: Optional[Path]) -> Optional[Path]:
    if explicit is not None:
        return explicit if explicit.exists() else None
    for name in CANDIDATE_SIDECAR_FILES:
        path = release_dir / name
        if path.exists():
            return path
    return None


def run_validation(
    release_dir: Path,
    out_dir: Path,
    candidate_sidecar: Optional[Path] = None,
    source_taxonomy_csv: Optional[Path] = None,
    external_attributes_csv: Optional[Path] = None,
    allow_master_fallback: bool = True,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    high_turbidity_ssc: float = DEFAULT_HIGH_TURBIDITY_SSC,
    ssc_bin_edges: Sequence[float] = DEFAULT_SSC_BIN_EDGES,
    figure_variables: Sequence[str] = ("SSC",),
    write_plots: bool = True,
    workers: int = DEFAULT_WORKERS,
    include_satellite_validation_nc: bool = True,
    satellite_chunk_size: int = DEFAULT_SATELLITE_CHUNK_SIZE,
    progress=log_progress,
) -> None:
    release_dir = release_dir.resolve()
    out_dir = out_dir.resolve()
    if not release_dir.exists() or not release_dir.is_dir():
        raise SystemExit("release-dir does not exist or is not a directory: {}".format(release_dir))
    unknown_windows = [window for window in windows if window not in WINDOW_DAYS]
    if unknown_windows:
        raise SystemExit("unknown pairing windows: {}".format(", ".join(unknown_windows)))

    if progress:
        progress("Starting s11 satellite / in-situ validation")
        progress("Release dir: {}".format(release_dir))
        progress("Output dir: {}".format(out_dir))
        progress("Workers: {}".format(max(1, int(workers or 1))))
    out_dir.mkdir(parents=True, exist_ok=True)

    taxonomy = load_source_taxonomy(source_taxonomy_csv)
    external_attrs = _load_external_attributes(external_attributes_csv)

    input_path = _find_candidate_sidecar(release_dir, candidate_sidecar)
    raw = pd.DataFrame()
    load_note = ""
    input_mode = ""
    if input_path is not None:
        raw, input_path, input_mode = load_observations_from_candidate_sidecar(release_dir, input_path, progress=progress)
        load_note = "candidate sidecar loaded"
    if raw.empty:
        if candidate_sidecar is not None and not allow_master_fallback:
            raise SystemExit("candidate sidecar not found or empty: {}".format(candidate_sidecar))
        if not allow_master_fallback:
            raise SystemExit("candidate sidecar not found and master fallback is disabled")
        raw, load_note = load_observations_from_master_nc(release_dir, progress=progress)
        input_path = release_dir / MASTER_FILE
        input_mode = "selected_master"

    if include_satellite_validation_nc and input_mode == "candidate_sidecar" and not raw.empty:
        satellite_rows, satellite_stats = load_relevant_satellite_validation_records(
            release_dir,
            raw,
            windows=windows,
            workers=workers,
            chunk_size=satellite_chunk_size,
            progress=progress,
        )
        if progress:
            progress(
                "Satellite validation extraction: rows={satellite_rows}, station_hits={station_hits}, "
                "time_hits={time_hits}, value_hits={value_hits}, no_common_key={no_common_key}, "
                "no_matching_station={no_matching_station}, no_time_overlap={no_time_overlap}, "
                "no_variable_values={no_variable_values}".format(**satellite_stats)
            )
        if not satellite_rows.empty:
            raw = pd.concat([raw, satellite_rows], ignore_index=True, sort=False)
            load_note = "{}; appended {} satellite validation rows".format(load_note, len(satellite_rows))
    elif progress and input_mode == "candidate_sidecar":
        progress("Satellite validation NC merge skipped by --no-satellite-validation-nc")

    raw = add_observation_type_from_source_attrs(raw, release_dir, workers=workers, progress=progress)
    observations = normalize_observation_table(raw, taxonomy, input_mode=input_mode)
    if progress:
        progress("Normalized observations: {}".format(len(observations)))
    pair_records = pair_satellite_insitu_records(
        observations,
        windows=windows,
        input_mode=input_mode,
        workers=workers,
        progress=progress,
    )
    pair_records = assign_strata(
        pair_records,
        external_attributes=external_attrs,
        high_turbidity_ssc=high_turbidity_ssc,
        ssc_bin_edges=ssc_bin_edges,
    )
    if progress:
        progress("Built pair records: {}".format(len(pair_records)))
    metrics = compute_satellite_insitu_metrics(pair_records)
    if progress:
        progress("Aggregated metric rows: {}".format(len(metrics)))

    pair_path = out_dir / "validation_satellite_insitu_pairs.csv"
    metric_path = out_dir / "validation_satellite_insitu_metrics.csv"
    summary_path = out_dir / "validation_satellite_insitu_summary.md"
    pair_records.to_csv(pair_path, index=False)
    metrics.to_csv(metric_path, index=False)
    generated_outputs: List[Tuple[str, str]] = [
        (pair_path.name, "generated"),
        (metric_path.name, "generated"),
    ]
    if write_plots:
        generated_outputs.extend(write_figures(pair_records, metrics, out_dir, figure_variables=figure_variables))
    else:
        generated_outputs.extend(
            [
                ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: --no-figures"),
                ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: --no-figures"),
                ("figures/satellite_insitu_metric_heatmap.png", "skipped: --no-figures"),
            ]
        )
    generated_outputs.append((summary_path.name, "generated"))
    write_summary(
        summary_path,
        input_path,
        input_mode,
        load_note,
        observations,
        pair_records,
        metrics,
        generated_outputs,
    )
    if progress:
        progress("s11 validation complete")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate satellite/reach-scale records against in-situ records.")
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR), help="Path to sed_reference_release.")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory for validation tables and figures.")
    parser.add_argument("--candidate-sidecar", help="Optional candidate sidecar path. Defaults to known release names.")
    parser.add_argument("--source-taxonomy-csv", help="Optional source taxonomy override CSV.")
    parser.add_argument("--external-attributes-csv", help="Optional cluster external attributes CSV for width/climate strata.")
    parser.add_argument("--no-master-fallback", action="store_true", help="Fail if no candidate sidecar is available.")
    parser.add_argument("--windows", nargs="+", default=["exact", "pm1d", "pm2d"], choices=sorted(WINDOW_DAYS))
    parser.add_argument("--high-turbidity-ssc", type=float, default=DEFAULT_HIGH_TURBIDITY_SSC)
    parser.add_argument("--ssc-bin-edges", default=",".join(_format_edge(v) for v in DEFAULT_SSC_BIN_EDGES))
    parser.add_argument("--figure-variables", nargs="+", default=["SSC"], choices=list(VARIABLES))
    parser.add_argument("--no-figures", action="store_true")
    parser.add_argument(
        "--no-satellite-validation-nc",
        action="store_true",
        help="Do not append matching records from sed_reference_satellite_validation.nc.",
    )
    parser.add_argument(
        "--satellite-chunk-size",
        type=int,
        default=DEFAULT_SATELLITE_CHUNK_SIZE,
        help="Record chunk size when scanning sed_reference_satellite_validation.nc.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Parallel worker count for source-attribute reading and pairing; use 1 for serial mode.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run_validation(
        release_dir=Path(args.release_dir),
        out_dir=Path(args.out_dir),
        candidate_sidecar=Path(args.candidate_sidecar).resolve() if args.candidate_sidecar else None,
        source_taxonomy_csv=Path(args.source_taxonomy_csv).resolve() if args.source_taxonomy_csv else None,
        external_attributes_csv=Path(args.external_attributes_csv).resolve() if args.external_attributes_csv else None,
        allow_master_fallback=not args.no_master_fallback,
        windows=args.windows,
        high_turbidity_ssc=float(args.high_turbidity_ssc),
        ssc_bin_edges=parse_ssc_bin_edges(args.ssc_bin_edges),
        figure_variables=args.figure_variables,
        write_plots=not args.no_figures,
        workers=args.workers,
        include_satellite_validation_nc=not args.no_satellite_validation_nc,
        satellite_chunk_size=args.satellite_chunk_size,
    )


if __name__ == "__main__":
    main()
