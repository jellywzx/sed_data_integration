#!/usr/bin/env python3
"""Satellite / reach-scale vs in-situ validation.

This version is configured by editing the USER CONFIG block below, so it can be
run without command-line arguments:

    python s11_satellite_insitu_validation.py

Supported input modes
---------------------
1. s6-candidates
   Reads scripts_basin_test/output/s6_cluster_quality_order.csv and opens the
   source NetCDF files listed in its path column. This is the recommended mode
   when you have run through s6 but have not regenerated s8 release outputs.
   It preserves candidate source observations for satellite vs in-situ pairing.

2. s6-selected
   Reads scripts_basin_test/output/s6_basin_merged_all.nc directly. This is
   useful only as a selected-record sanity check; it usually cannot build true
   satellite vs in-situ pairs because non-selected candidate values are absent.

3. s5-candidates
   Reads scripts_basin_test/output/s5_basin_clustered_stations.csv and opens the
   source NetCDF files listed in its path column. This can run after s5, before
   s6/s8, and preserves candidate source observations for validation.

4. release
   Reads s8 release candidate sidecar when available, otherwise falls back to
   sed_reference_master.nc if RUN_ALLOW_MASTER_FALLBACK=True.

5. auto
   Prefer current s6 candidate files, then s6 selected, then s5. Explicit
   RUN_INPUT_MODE="release" is required to read sed_reference_release.

Outputs
-------
validation_satellite_insitu_pairs.csv
validation_satellite_insitu_metrics.csv
validation_satellite_insitu_summary.md
figures/*.png, unless RUN_NO_FIGURES=True
"""

from __future__ import annotations

import math
import os
import re
import time as time_module
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


VARIABLES = ("Q", "SSC", "SSL")
WINDOW_DAYS = {"exact": 0, "pm1d": 1, "pm2d": 2}
WINDOW_EXCLUSIVE = False

MASTER_FILE = "sed_reference_master.nc"
CANDIDATE_SIDECAR_FILES = (
    "sed_reference_overlap_candidates.parquet",
    "sed_reference_overlap_candidates.csv.gz",
)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_HIGH_TURBIDITY_SSC = 1000.0
DEFAULT_SSC_BIN_EDGES = (100.0, 500.0, 1000.0, 5000.0)
METHOD_NOTES_BASE = (
    "satellite/reach-scale vs in-situ validation; satellite records are anchors; "
    "pairing windows are cumulative"
)
ASSUMPTIONS_BASE = (
    "compiled sources are secondary_compilation unless source text or taxonomy override "
    "identifies them as in_situ; missing river width is 'missing'; missing climate zone is 'unknown'"
)
RESOLUTION_CODE = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"}


try:
    from pipeline_paths import (
        S2_ORGANIZED_DIR,
        S5_BASIN_CLUSTERED_CSV,
        S6_MERGED_NC,
        S6_QUALITY_ORDER_CSV,
        get_output_r_root,
    )
except Exception:
    S2_ORGANIZED_DIR = "../output_resolution_organized"
    S5_BASIN_CLUSTERED_CSV = "scripts_basin_test/output/s5_basin_clustered_stations.csv"
    S6_MERGED_NC = "scripts_basin_test/output/s6_basin_merged_all.nc"
    S6_QUALITY_ORDER_CSV = "scripts_basin_test/output/s6_cluster_quality_order.csv"

    def get_output_r_root(script_dir: Path) -> Path:
        return script_dir.parent.resolve()


OUTPUT_R_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_RELEASE_DIR = OUTPUT_R_ROOT / "scripts_basin_test/output/sed_reference_release"
DEFAULT_S6_NC = OUTPUT_R_ROOT / S6_MERGED_NC
DEFAULT_S6_QUALITY_ORDER_CSV = OUTPUT_R_ROOT / S6_QUALITY_ORDER_CSV
DEFAULT_S5_CSV = OUTPUT_R_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_ORGANIZED_ROOT = (OUTPUT_R_ROOT / S2_ORGANIZED_DIR).resolve()


# ─────────────────────────────────────────────────────────────────────────────
# USER CONFIG
# Edit this block, then run:
#   python s11_satellite_insitu_validation.py
# ─────────────────────────────────────────────────────────────────────────────

# Choose one:
#   "s6-candidates"  after s6 has finished; reads s6_cluster_quality_order.csv
#   "s6-selected"    after s6 has finished; reads s6_basin_merged_all.nc
#   "s5-candidates"  after s5 has finished; reads s5 paths and source NC files
#   "release"        after s8 has finished; reads sed_reference_release
#   "auto"           prefer current s6 candidates, then s6-selected, then s5
RUN_INPUT_MODE = "s6-candidates"

RUN_RELEASE_DIR = DEFAULT_RELEASE_DIR
RUN_S6_NC = DEFAULT_S6_NC
RUN_S6_QUALITY_ORDER_CSV = DEFAULT_S6_QUALITY_ORDER_CSV
RUN_S5_CSV = DEFAULT_S5_CSV
RUN_ORGANIZED_ROOT = DEFAULT_ORGANIZED_ROOT

RUN_OUT_DIR_BY_MODE = {
    "release": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_release",
    "candidate_sidecar": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_release",
    "selected_master": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_release",
    "s6-candidates": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_s6_candidates",
    "s6-selected": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_s6_selected",
    "s5-candidates": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_s5_candidates",
    "auto": OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_auto",
}

# s6-candidates can be slow because it opens source NetCDF files listed by s6.
# For a quick smoke test, set 100 or 200. For full validation, set None.
RUN_MAX_S6_CANDIDATE_FILES = None

# True skips cluster_id + resolution groups with only one candidate source,
# because they cannot produce satellite-vs-in-situ candidate pairs.
RUN_S6_CANDIDATES_OVERLAP_GROUPS_ONLY = True

# False is safer because source-family detection may require opening NetCDF
# global attributes. Set True only if the quality-order source/path text already
# reliably distinguishes satellite and in_situ sources in your run.
RUN_S6_CANDIDATES_REQUIRE_MIXED_FAMILIES = False

# s5-candidates can be slow because it opens many source NetCDF files.
# For a quick smoke test, set 100 or 200. For full validation, set None.
RUN_MAX_S5_FILES = None

# Optional taxonomy CSV for source classification.
# Columns accepted: source/source_name/source_dataset and family/source_family/class/source_class
# Example:
# source,source_family
# RiverSed,satellite
# USGS,in_situ
RUN_SOURCE_TAXONOMY_CSV = None

# Optional external attributes for strata such as river_width_m/climate_zone.
RUN_EXTERNAL_ATTRIBUTES_CSV = None

RUN_ALLOW_MASTER_FALLBACK = False
RUN_WINDOWS = ["exact", "pm1d", "pm2d"]
RUN_HIGH_TURBIDITY_SSC = DEFAULT_HIGH_TURBIDITY_SSC
RUN_SSC_BIN_EDGES = DEFAULT_SSC_BIN_EDGES
RUN_FIGURE_VARIABLES = ["SSC"]

# Start with True if you only want tables. Set False for final report figures.
RUN_NO_FIGURES = False

# Fast-read options for large s6 NetCDF files.
# These only affect RUN_INPUT_MODE = "s6-selected".
#
# True means only validate records where s6 marked is_overlap == 1. This is
# usually much faster and often the most relevant subset for satellite vs in-situ
# comparison, because it focuses on multi-source dates.
RUN_S6_OVERLAP_ONLY = True

# Optional cap for a deterministic smoke test. Use 100_000 or 500_000 while
# debugging. Set None for full selected-record validation.
RUN_S6_MAX_RECORDS = 500_000

# Random seed used only when RUN_S6_MAX_RECORDS samples from a larger set.
RUN_S6_RANDOM_SEED = 2026

# Optional resolution filter. Examples:
#   None
#   ("daily",)
#   ("daily", "monthly")
RUN_S6_RESOLUTION_ALLOWLIST = None

# Parallel workers for heavy candidate-file I/O stages (s6-candidates/s5-candidates).
# Set to 1 to disable parallel loading. None means auto (= min(8, CPU count)).
RUN_CANDIDATE_IO_WORKERS = 12

# Parallel workers for satellite-vs-in_situ pairing stage.
# Set to 1 to run sequentially. None means auto (= min(8, CPU count)).
RUN_PAIRING_WORKERS = 16


def log_progress(message: str) -> None:
    stamp = time_module.strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] {}".format(stamp, message), flush=True)


def _auto_worker_count(configured: Optional[int], cap: int = 8) -> int:
    if configured is not None:
        try:
            parsed = int(configured)
        except Exception:
            parsed = 1
        return max(1, parsed)
    cpu = int(os.cpu_count() or 1)
    return max(1, min(cap, cpu))


def _effective_worker_count(configured: Optional[int], n_jobs: int, cap: int = 8) -> int:
    if n_jobs <= 1:
        return 1
    return max(1, min(_auto_worker_count(configured, cap=cap), int(n_jobs)))


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
    return "" if text.lower() in ("nan", "none", "nat", "<na>") else text


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
    if values.dtype.kind in ("S", "U", "O"):
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

    if "_time_units" in df.columns and numeric.notna().any():
        units = _clean_text(df["_time_units"].iloc[0])
        return _parse_cf_days_since(raw, units).dt.floor("D")

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
        return pd.read_csv(path, keep_default_na=False)
    raise ValueError("Unsupported candidate sidecar extension: {}".format(path))


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
    """Normalize a free-text source/type label for matching."""
    text = _clean_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _normalize_family_label(value) -> str:
    text = _clean_text(value)
    key = _family_key(text)
    if not key:
        return ""

    if key in (
        "satellite",
        "satellite_station",
        "satellite_derived",
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

    if key in ("usgs", "hydat", "grdc", "hybam"):
        return "in_situ"
    if key in ("secondary", "secondary_compilation", "compiled", "compilation"):
        return "secondary_compilation"
    if key in ("model", "modeled", "modelled"):
        return "model"
    if key in ("other", "unknown"):
        return "other"

    # Some global attributes contain full phrases rather than short enum labels.
    low = text.lower()
    if any(token in low for token in ("satellite", "remote sensing", "remote-sensing", "landsat", "reach-scale")):
        return "satellite"
    if any(token in low for token in ("in-situ", "in situ", "insitu", "station data", "gauge", "monitoring network")):
        return "in_situ"
    if any(token in low for token in ("compiled", "compilation", "secondary")):
        return "secondary_compilation"
    return ""


# Dataset-level defaults mirrored from Sed_data/code/dataset_attr_profiles.py
# plus converter-level global-attribute names used by source NetCDF files.
# Values are intentionally canonicalized to the two families needed by this
# validation script. Unknown/other sources are left as "other" instead of being
# forced into either side of the satellite-vs-in-situ comparison.
SOURCE_FAMILY_BY_DATASET_KEY = {
    # Satellite / reach-scale products.
    "riversed": "satellite",
    "river_sed": "satellite",
    "river_sed_aquasat": "satellite",
    "aquasat": "satellite",
    "gsed": "satellite",
    "gsed_dataset": "satellite",
    "dethier": "satellite",
    "dethier_glacier_fed_rivers_dataset": "satellite",

    # In-situ products from canonical profiles and converter global attrs.
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

    # Exact aliases first.
    family = SOURCE_FAMILY_BY_DATASET_KEY.get(key)
    if family:
        return family

    # Substring matching for verbose data_source_name/source strings.
    for dataset_key, candidate_family in SOURCE_FAMILY_BY_DATASET_KEY.items():
        if dataset_key and (dataset_key in key or key in dataset_key):
            return candidate_family

    low = _clean_text(value).lower()
    compact = key.replace("_", "")

    # Satellite checks must come before "station" checks because several
    # satellite products use the phrase "Satellite station".
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


def _dataset_name_from_path(path: Path) -> str:
    """Infer dataset folder from paths like <resolution>/<dataset>/qc/*.nc."""
    try:
        parts = list(Path(path).parts)
    except Exception:
        return ""

    lowered = [str(part).lower() for part in parts]
    if "qc" in lowered:
        idx = lowered.index("qc")
        if idx >= 1:
            return str(parts[idx - 1])
    for marker in ("output_resolution_organized", "output_r"):
        if marker in lowered:
            idx = lowered.index(marker)
            if idx + 2 < len(parts):
                # Usually output_resolution_organized/<resolution>/<dataset>/qc/file.nc
                return str(parts[idx + 2])
    if len(parts) >= 2:
        return str(Path(path).parent.parent.name if Path(path).parent.name.lower() == "qc" else Path(path).parent.name)
    return ""


def _read_dataset_attrs_as_text(ds) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    try:
        for key, value in getattr(ds, "attrs", {}).items():
            attrs[str(key)] = _clean_text(value)
    except Exception:
        pass
    return attrs


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


def load_source_taxonomy(path: Optional[Path] = None) -> Dict[str, str]:
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


def classify_source_family(source: str, overrides: Optional[Dict[str, str]] = None, raw_family: str = "") -> str:
    """Classify source into satellite/in_situ/secondary/model/other.

    Precedence:
      1. explicit taxonomy CSV override;
      2. explicit source_family/source_type/source_category/observation_type/Type;
      3. canonical Sed_data dataset profile names and converter global attrs;
      4. conservative text heuristics.
    """
    if overrides is None:
        overrides = {}

    source_text = _clean_text(source)
    lookup_keys = []
    for value in (source_text, raw_family):
        cleaned = _clean_text(value)
        if cleaned:
            lookup_keys.extend([cleaned.lower(), _family_key(cleaned)])
    for key in lookup_keys:
        override = overrides.get(key)
        if override:
            return override

    normalized_raw = _normalize_family_label(raw_family)
    if normalized_raw:
        return normalized_raw

    for value in (source_text, raw_family):
        family = _family_from_dataset_text(value)
        if family:
            return family

    return "other"


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
        ds = xr.open_dataset(path, decode_times=False, mask_and_scale=True)
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
            "source_station_uid",
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
            records["_time_units"] = getattr(ds["time"], "units", "days since 1970-01-01")
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
        extra_cols = [col for col in (source_col, family_col) if col is not None]
        if uid_col is not None and extra_cols:
            lookup = catalog[[uid_col] + extra_cols].drop_duplicates(uid_col)
            records = records.merge(
                lookup,
                how="left",
                left_on="source_station_uid",
                right_on=uid_col,
                suffixes=("", "_catalog"),
            )
            if source_col and "source" in records.columns:
                merged_col = source_col if source_col in records.columns else "{}_catalog".format(source_col)
                if merged_col in records.columns:
                    missing = records["source"].astype(str).str.strip().eq("")
                    records.loc[missing, "source"] = records.loc[missing, merged_col]
            elif source_col and source_col in records.columns:
                records["source"] = records[source_col]

            if family_col and "source_family" in records.columns:
                merged_col = family_col if family_col in records.columns else "{}_catalog".format(family_col)
                if merged_col in records.columns:
                    missing = records["source_family"].astype(str).str.strip().eq("")
                    records.loc[missing, "source_family"] = records.loc[missing, merged_col]
            elif family_col and family_col in records.columns:
                records["source_family"] = records[family_col]

    return records, "selected master records loaded"


def _format_cluster_uid(value) -> str:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return "SED{:06d}".format(int(numeric))
    return _clean_text(value)


def _selected_record_series(ds, name: str, record_dim: str, indices: np.ndarray) -> Optional[pd.Series]:
    """Return one record-dimension variable for selected record indices only."""
    if name not in ds.variables:
        return None

    da = ds[name]
    if record_dim not in da.dims:
        return None

    try:
        if len(da.dims) == 1:
            sub = da.isel({record_dim: indices})
            return _series_from_data_array(sub).reset_index(drop=True)
        if len(da.dims) == 2 and da.dims[0] == record_dim:
            sub = da.isel({record_dim: indices})
            return _series_from_data_array(sub).reset_index(drop=True)
    except Exception:
        return None

    return None


def _resolution_labels_from_codes(values: np.ndarray) -> np.ndarray:
    """Convert numeric/string s6 resolution values to labels."""
    out = []
    for value in values:
        numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.notna(numeric) and float(numeric).is_integer():
            out.append(RESOLUTION_CODE.get(int(numeric), str(value)))
        else:
            out.append(_normalize_resolution(value))
    return np.asarray(out, dtype=object)


def _read_record_filter_array(ds, name: str, record_dim: str, expected_len: int) -> Optional[np.ndarray]:
    """Read a cheap one-dimensional record-level variable for filtering."""
    if name not in ds.variables:
        return None
    da = ds[name]
    if record_dim not in da.dims or len(da.dims) != 1:
        return None
    try:
        values = np.asarray(da.values).reshape(-1)
    except Exception:
        return None
    if len(values) != expected_len:
        return None
    return values


def load_observations_from_s6_nc(
    s6_nc: Path,
    progress=log_progress,
) -> Tuple[pd.DataFrame, str]:
    """Load selected record-level observations directly from s6_basin_merged_all.nc.

    This fast version avoids converting the whole NetCDF into pandas. It first
    reads cheap record-level filter variables, builds a selected record index,
    and then loads only selected rows for the variables needed by the validation.

    Controlled by these USER CONFIG values:
      RUN_S6_OVERLAP_ONLY
      RUN_S6_MAX_RECORDS
      RUN_S6_RANDOM_SEED
      RUN_S6_RESOLUTION_ALLOWLIST
    """
    if not s6_nc.exists():
        return pd.DataFrame(), "s6 NetCDF not found: {}".format(s6_nc)

    try:
        import xarray as xr  # type: ignore
    except Exception as exc:
        return pd.DataFrame(), "xarray unavailable for s6 NetCDF reading: {}".format(exc)

    if progress:
        progress("Reading s6 selected records with fast filters: {}".format(s6_nc))

    try:
        ds = xr.open_dataset(s6_nc, decode_times=False, mask_and_scale=True)
    except Exception as exc:
        return pd.DataFrame(), "cannot open s6 NetCDF: {}".format(exc)

    try:
        record_dim = _find_record_dim(ds)
        if record_dim is None:
            return pd.DataFrame(), "record dimension could not be inferred from s6 NetCDF"

        n_records = int(ds.sizes[record_dim])
        mask = np.ones(n_records, dtype=bool)

        # 1. Optional overlap-only filter. This is usually the biggest speedup.
        overlap_only = bool(globals().get("RUN_S6_OVERLAP_ONLY", False))
        if overlap_only:
            overlap = _read_record_filter_array(ds, "is_overlap", record_dim, n_records)
            if overlap is not None:
                mask &= pd.to_numeric(pd.Series(overlap), errors="coerce").fillna(0).to_numpy() == 1
            elif progress:
                progress("RUN_S6_OVERLAP_ONLY=True, but is_overlap was not available; skipping overlap filter")

        # 2. Optional resolution allowlist, for example ("daily",).
        allowlist = globals().get("RUN_S6_RESOLUTION_ALLOWLIST", None)
        if allowlist:
            res_values = _read_record_filter_array(ds, "resolution", record_dim, n_records)
            if res_values is not None:
                labels = _resolution_labels_from_codes(res_values)
                mask &= np.isin(labels, list(allowlist))
            elif progress:
                progress("RUN_S6_RESOLUTION_ALLOWLIST is set, but resolution was not available; skipping resolution filter")

        indices = np.flatnonzero(mask)

        # 3. Optional deterministic cap/sample.
        max_records = globals().get("RUN_S6_MAX_RECORDS", None)
        if max_records is not None and int(max_records) > 0 and len(indices) > int(max_records):
            rng = np.random.default_rng(int(globals().get("RUN_S6_RANDOM_SEED", 2026)))
            indices = np.sort(rng.choice(indices, size=int(max_records), replace=False))

        if progress:
            progress("s6 record filter selected {:,} of {:,} records".format(len(indices), n_records))

        if len(indices) == 0:
            return pd.DataFrame(), "no s6 records matched fast filters"

        records = pd.DataFrame({"record_index": indices.astype(np.int64)})

        # 4. Load only selected record-level data variables.
        for name in VARIABLES:
            series = _selected_record_series(ds, name, record_dim, indices)
            if series is not None and len(series) == len(indices):
                records[name] = pd.to_numeric(series, errors="coerce")

            flag = _selected_record_series(ds, "{}_flag".format(name), record_dim, indices)
            if flag is not None and len(flag) == len(indices):
                records["{}_flag".format(name)] = flag

        for name in (
            "resolution",
            "time",
            "source",
            "source_family",
            "is_overlap",
            "river_width_class",
            "river_width_m",
            "climate_zone",
            "station_index",
            "source_station_index",
        ):
            series = _selected_record_series(ds, name, record_dim, indices)
            if series is not None and len(series) == len(indices):
                records[name] = series

        if "time" in records.columns and "time" in ds.variables:
            records["_time_units"] = getattr(ds["time"], "units", "days since 1970-01-01")

        # 5. Recover station lookup fields from n_stations using selected station_index.
        station_index = records.get("station_index")
        if station_index is not None:
            if "cluster_uid" not in records.columns:
                lookup = _indexed_lookup_series(ds, "cluster_uid", station_index)
                if lookup is not None:
                    records["cluster_uid"] = lookup
            if "cluster_id" not in records.columns:
                lookup = _indexed_lookup_series(ds, "cluster_id", station_index)
                if lookup is not None:
                    records["cluster_id"] = lookup

        # 6. Recover source-station UID from n_source_stations using selected source_station_index.
        source_station_index = records.get("source_station_index")
        if source_station_index is not None:
            if "source_station_uid" not in records.columns:
                lookup = _indexed_lookup_series(ds, "source_station_uid", source_station_index)
                if lookup is not None:
                    records["source_station_uid"] = lookup

        # 7. Older/fallback s6 files may not have record-level source strings.
        if "source" not in records.columns and source_station_index is not None:
            if "source_station_source_index" in ds.variables and "source_name" in ds.variables:
                ss_source_idx = _series_from_data_array(ds["source_station_source_index"]).reset_index(drop=True)
                source_names = _series_from_data_array(ds["source_name"]).reset_index(drop=True)
                out = []

                for raw_idx in pd.to_numeric(source_station_index, errors="coerce"):
                    if pd.isna(raw_idx):
                        out.append("")
                        continue
                    ss_idx = int(raw_idx)
                    if 0 <= ss_idx < len(ss_source_idx):
                        src_idx = pd.to_numeric(pd.Series([ss_source_idx.iloc[ss_idx]]), errors="coerce").iloc[0]
                        if pd.notna(src_idx) and 0 <= int(src_idx) < len(source_names):
                            out.append(_clean_text(source_names.iloc[int(src_idx)]))
                        else:
                            out.append("")
                    else:
                        out.append("")

                records["source"] = out

        return records, "s6 selected records loaded with fast filters"
    finally:
        ds.close()


def _first_nonempty(row: pd.Series, names: Sequence[str], default: str = "") -> str:
    for name in names:
        if name in row.index:
            text = _clean_text(row.get(name, ""))
            if text:
                return text
    return default


def _resolve_s5_station_path(path_text: str, s5_csv: Path, organized_root: Path) -> Path:
    text = _clean_text(path_text)
    if not text:
        return Path("")
    path = Path(text).expanduser()
    if path.is_absolute():
        return path

    candidates = [
        organized_root / path,
        s5_csv.parent / path,
        s5_csv.parent.parent / path,
        SCRIPT_DIR / path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return (organized_root / path).resolve()


def _read_source_nc_as_observations(
    nc_path: Path,
    row: pd.Series,
    row_number: int,
    record_prefix: str = "s5row",
) -> pd.DataFrame:
    try:
        import xarray as xr  # type: ignore
    except Exception:
        return pd.DataFrame()

    if not nc_path.exists() or not nc_path.is_file():
        return pd.DataFrame()

    try:
        ds = xr.open_dataset(nc_path, decode_times=False, mask_and_scale=True)
    except Exception:
        return pd.DataFrame()

    try:
        attrs = _read_dataset_attrs_as_text(ds)
        attr_data_source_name = _first_attr(
            attrs,
            ("data_source_name", "Data_Source_Name", "dataset_name", "Data Source Name"),
        )
        attr_source = _first_attr(attrs, ("source",))
        attr_observation_type = _first_attr(attrs, ("observation_type", "Type", "type"))
        attr_source_family = _first_attr(
            attrs,
            ("source_family", "source_type", "source_category", "family", "observation_type", "Type", "type"),
        )
        path_dataset = _dataset_name_from_path(nc_path)

        record_dim = _find_record_dim(ds)
        if record_dim is None:
            return pd.DataFrame()

        n_records = int(ds.sizes[record_dim])
        raw = pd.DataFrame({"record_index": ["{}{}:{}".format(record_prefix, row_number, i) for i in range(n_records)]})

        time_series = _record_series(ds, "time", record_dim)
        if time_series is not None and len(time_series) == n_records:
            raw["time"] = time_series
            raw["_time_units"] = getattr(ds["time"], "units", "days since 1970-01-01")

        date_series = _record_series(ds, "date", record_dim)
        if date_series is not None and len(date_series) == n_records:
            raw["date"] = date_series.astype(str)

        for name in VARIABLES:
            series = None
            for candidate in (name, name.lower()):
                series = _record_series(ds, candidate, record_dim)
                if series is not None:
                    break
            raw[name] = pd.to_numeric(series, errors="coerce") if series is not None and len(series) == n_records else np.nan

            flag = None
            for candidate in (
                "{}_flag".format(name),
                "{}_qc_flag".format(name),
                "{}_quality_flag".format(name),
                "{}_flag".format(name.lower()),
            ):
                flag = _record_series(ds, candidate, record_dim)
                if flag is not None:
                    break
            raw["{}_flag".format(name)] = flag if flag is not None and len(flag) == n_records else np.nan

        cluster_id = row.get("cluster_id", "")
        cluster_uid = row.get("cluster_uid", "")
        if not _clean_text(cluster_uid):
            cluster_uid = _format_cluster_uid(cluster_id)

        row_source = _first_nonempty(
            row,
            ("source", "source_name", "source_dataset", "dataset", "dataset_name"),
            default="",
        )
        row_family = _first_nonempty(
            row,
            ("source_family", "source_type", "source_category", "family", "observation_type", "Type", "type"),
            default="",
        )

        raw["cluster_uid"] = cluster_uid
        raw["cluster_id"] = cluster_id
        raw["resolution"] = row.get("resolution", "")
        raw["source"] = row_source or attr_data_source_name or path_dataset or attr_source
        raw["source_family"] = row_family or attr_source_family
        raw["data_source_name"] = attr_data_source_name
        raw["global_source"] = attr_source
        raw["observation_type"] = attr_observation_type
        raw["dataset_from_path"] = path_dataset
        raw["source_station_uid"] = _first_nonempty(
            row,
            ("source_station_uid", "source_station_id", "station_id"),
            default="{}{}".format(record_prefix, row_number),
        )
        raw["source_path"] = str(nc_path)

        return raw
    finally:
        ds.close()


def _load_s5_candidate_worker(
    job: Tuple[int, Dict[str, object], str, str],
) -> Tuple[int, str, pd.DataFrame]:
    ordinal, row_dict, s5_csv_text, organized_root_text = job
    row = pd.Series(row_dict)
    nc_path = _resolve_s5_station_path(
        row.get("path", ""),
        s5_csv=Path(s5_csv_text),
        organized_root=Path(organized_root_text),
    )
    if not nc_path.exists() or not nc_path.is_file():
        return ordinal, "missing", pd.DataFrame()
    frame = _read_source_nc_as_observations(nc_path, row, ordinal)
    if frame.empty:
        return ordinal, "empty", pd.DataFrame()
    return ordinal, "ok", frame


def load_observations_from_s5_candidates(
    s5_csv: Path,
    organized_root: Path,
    max_files: Optional[int] = None,
    io_workers: Optional[int] = None,
    progress=log_progress,
) -> Tuple[pd.DataFrame, str]:
    """Build candidate-level observations from s5 station rows and source NC paths."""
    if not s5_csv.exists():
        return pd.DataFrame(), "s5 CSV not found: {}".format(s5_csv)

    if progress:
        progress("Reading s5 candidate source list: {}".format(s5_csv))

    s5 = pd.read_csv(s5_csv, keep_default_na=False)
    if "path" not in s5.columns:
        return pd.DataFrame(), "s5 CSV has no path column: {}".format(s5_csv)

    frames: List[Tuple[int, pd.DataFrame]] = []
    rows = list(s5.iterrows())
    if max_files is not None and max_files > 0:
        rows = rows[: int(max_files)]
    n_rows = len(rows)
    n_missing = 0
    n_empty = 0

    workers = _effective_worker_count(io_workers, n_rows, cap=8)
    if progress:
        progress("s5-candidates I/O workers: {}".format(workers))

    if workers <= 1:
        for ordinal, (_, row) in enumerate(rows, start=1):
            nc_path = _resolve_s5_station_path(row.get("path", ""), s5_csv=s5_csv, organized_root=organized_root)
            if not nc_path.exists() or not nc_path.is_file():
                n_missing += 1
            else:
                frame = _read_source_nc_as_observations(nc_path, row, ordinal)
                if frame.empty:
                    n_empty += 1
                else:
                    frames.append((ordinal, frame))
            if progress and ordinal % 100 == 0:
                progress("Loaded s5 candidate files: {}/{}".format(ordinal, n_rows))
    else:
        jobs = [
            (ordinal, row.to_dict(), str(s5_csv), str(organized_root))
            for ordinal, (_, row) in enumerate(rows, start=1)
        ]
        done = 0
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_load_s5_candidate_worker, job) for job in jobs]
            for future in as_completed(futures):
                done += 1
                ordinal, status, frame = future.result()
                if status == "ok" and not frame.empty:
                    frames.append((ordinal, frame))
                elif status == "missing":
                    n_missing += 1
                else:
                    n_empty += 1
                if progress and done % 100 == 0:
                    progress("Loaded s5 candidate files: {}/{}".format(done, n_rows))

    if not frames:
        return pd.DataFrame(), (
            "no source observations could be loaded from s5 paths "
            "(missing_files={}, empty_or_unreadable_files={})".format(n_missing, n_empty)
        )

    frames.sort(key=lambda item: item[0])
    out = pd.concat([frame for _, frame in frames], ignore_index=True, sort=False)
    return out, (
        "s5 candidate source observations loaded from {} files; "
        "missing_files={}; empty_or_unreadable_files={}; io_workers={}"
    ).format(len(frames), n_missing, n_empty, workers)


def _resolve_s6_candidate_path(path_text: str, quality_order_csv: Path, organized_root: Path) -> Path:
    """Resolve a path stored in s6_cluster_quality_order.csv.

    s6 usually writes paths relative to output_resolution_organized, but older
    runs may contain absolute paths from a different machine/run.  If the path
    contains the output_resolution_organized marker, rebuild it under the current
    RUN_ORGANIZED_ROOT to avoid accidentally following stale absolute paths.
    """
    text = _clean_text(path_text)
    if not text:
        return Path("")

    raw_path = Path(text).expanduser()

    marker = "output_resolution_organized"
    try:
        parts = raw_path.parts
        for idx, part in enumerate(parts):
            if part == marker and idx + 1 < len(parts):
                candidate = (organized_root / Path(*parts[idx + 1:])).resolve()
                if candidate.exists():
                    return candidate
    except Exception:
        pass

    if raw_path.is_absolute():
        return raw_path

    candidates = [
        organized_root / raw_path,
        quality_order_csv.parent / raw_path,
        quality_order_csv.parent.parent / raw_path,
        SCRIPT_DIR / raw_path,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return (organized_root / raw_path).resolve()


def _quality_row_family_hint(row: pd.Series) -> str:
    """Best-effort source-family hint before opening the source NetCDF."""
    hints = []
    for name in (
        "source",
        "source_name",
        "source_dataset",
        "dataset",
        "dataset_name",
        "source_family",
        "source_type",
        "source_category",
        "observation_type",
        "Type",
        "type",
        "path",
    ):
        if name in row.index:
            hints.append(_clean_text(row.get(name, "")))
    if "path" in row.index:
        hints.append(_dataset_name_from_path(row.get("path", "")))
    context = " | ".join([hint for hint in hints if hint])
    raw_family = _first_nonempty(
        row,
        ("source_family", "source_type", "source_category", "family", "observation_type", "Type", "type"),
        default="",
    )
    return classify_source_family(context, raw_family=raw_family)


def _load_s6_candidate_worker(
    job: Tuple[int, Dict[str, object], str, str],
) -> Tuple[int, str, pd.DataFrame]:
    ordinal, row_dict, quality_order_csv_text, organized_root_text = job
    row = pd.Series(row_dict)
    nc_path = _resolve_s6_candidate_path(
        row.get("path", ""),
        quality_order_csv=Path(quality_order_csv_text),
        organized_root=Path(organized_root_text),
    )
    if not nc_path.exists() or not nc_path.is_file():
        return ordinal, "missing", pd.DataFrame()

    frame = _read_source_nc_as_observations(nc_path, row, ordinal, record_prefix="s6qrow")
    if frame.empty:
        return ordinal, "empty", pd.DataFrame()

    frame["candidate_rank"] = row.get("quality_rank", "")
    frame["candidate_quality_score"] = row.get("quality_score", "")
    frame["candidate_source_station_index"] = row.get("source_station_index", "")
    frame["candidate_path"] = row.get("path", "")
    return ordinal, "ok", frame


def load_observations_from_s6_candidates(
    quality_order_csv: Path,
    organized_root: Path,
    max_files: Optional[int] = None,
    overlap_groups_only: bool = True,
    require_mixed_families: bool = False,
    io_workers: Optional[int] = None,
    progress=log_progress,
) -> Tuple[pd.DataFrame, str]:
    """Build candidate-level observations from s6 quality-order rows.

    This mirrors the candidate-value source used by s8 overlap sidecar, but it
    can be run immediately after s6.  It does not read sed_reference_release, so
    it avoids mixing the current s6 run with stale s8 outputs.
    """
    quality_order_csv = Path(quality_order_csv).expanduser().resolve()
    organized_root = Path(organized_root).expanduser().resolve()

    if not quality_order_csv.exists():
        return pd.DataFrame(), "s6 quality-order CSV not found: {}".format(quality_order_csv)

    if progress:
        progress("Reading s6 candidate source list: {}".format(quality_order_csv))

    quality = pd.read_csv(quality_order_csv, keep_default_na=False)
    required = {"cluster_id", "resolution", "path"}
    missing = sorted(required - set(quality.columns))
    if missing:
        return pd.DataFrame(), "s6 quality-order CSV missing columns: {}".format(", ".join(missing))

    if quality.empty:
        return pd.DataFrame(), "s6 quality-order CSV is empty: {}".format(quality_order_csv)

    if "source" not in quality.columns:
        quality["source"] = ""
    if "cluster_uid" not in quality.columns:
        quality["cluster_uid"] = ""
    if "quality_rank" not in quality.columns:
        quality["quality_rank"] = np.arange(len(quality)) + 1
    if "source_station_index" not in quality.columns:
        quality["source_station_index"] = -1
    if "source_station_uid" not in quality.columns:
        quality["source_station_uid"] = ""
    if "quality_score" not in quality.columns:
        quality["quality_score"] = np.nan

    quality["cluster_id"] = pd.to_numeric(quality["cluster_id"], errors="coerce").fillna(-1).astype(int)
    quality["quality_rank"] = pd.to_numeric(quality["quality_rank"], errors="coerce").fillna(999999).astype(int)
    quality["source_station_index"] = pd.to_numeric(quality["source_station_index"], errors="coerce").fillna(-1).astype(int)
    quality["quality_score"] = pd.to_numeric(quality["quality_score"], errors="coerce")
    quality["resolution"] = quality["resolution"].map(_normalize_resolution)
    quality["cluster_uid"] = quality["cluster_uid"].map(_clean_text)
    missing_uid = quality["cluster_uid"].astype(str).str.strip().eq("")
    quality.loc[missing_uid, "cluster_uid"] = quality.loc[missing_uid, "cluster_id"].map(_format_cluster_uid)

    group_cols = ["cluster_id", "resolution"]
    if overlap_groups_only:
        group_sizes = quality.groupby(group_cols)["path"].transform("size")
        before = len(quality)
        quality = quality[group_sizes >= 2].copy()
        if progress:
            progress("s6-candidates kept {:,} of {:,} quality rows after single-candidate group filter".format(len(quality), before))

    if require_mixed_families and not quality.empty:
        keep_indices = []
        for _, group in quality.groupby(group_cols, sort=False):
            families = {_quality_row_family_hint(row) for _, row in group.iterrows()}
            if "satellite" in families and "in_situ" in families:
                keep_indices.extend(group.index.tolist())
        before = len(quality)
        quality = quality.loc[keep_indices].copy() if keep_indices else quality.iloc[0:0].copy()
        if progress:
            progress("s6-candidates kept {:,} of {:,} quality rows after mixed-family text prefilter".format(len(quality), before))

    if quality.empty:
        return pd.DataFrame(), "no s6 candidate rows remained after filters"

    quality = quality.sort_values(
        ["cluster_id", "resolution", "quality_rank", "source_station_index", "path"],
        kind="mergesort",
    ).reset_index(drop=True)

    if max_files is not None and int(max_files) > 0 and len(quality) > int(max_files):
        quality = quality.head(int(max_files)).copy()

    frames: List[Tuple[int, pd.DataFrame]] = []
    n_missing = 0
    n_empty = 0
    rows = list(quality.iterrows())
    n_rows = len(rows)
    workers = _effective_worker_count(io_workers, n_rows, cap=8)
    if progress:
        progress("s6-candidates I/O workers: {}".format(workers))

    if workers <= 1:
        for ordinal, (_, row) in enumerate(rows, start=1):
            nc_path = _resolve_s6_candidate_path(row.get("path", ""), quality_order_csv=quality_order_csv, organized_root=organized_root)
            if not nc_path.exists() or not nc_path.is_file():
                n_missing += 1
                if progress and n_missing <= 5:
                    progress("Missing s6 candidate file: {}".format(row.get("path", "")))
                continue

            frame = _read_source_nc_as_observations(nc_path, row, ordinal, record_prefix="s6qrow")
            if frame.empty:
                n_empty += 1
            else:
                frame["candidate_rank"] = row.get("quality_rank", "")
                frame["candidate_quality_score"] = row.get("quality_score", "")
                frame["candidate_source_station_index"] = row.get("source_station_index", "")
                frame["candidate_path"] = row.get("path", "")
                frames.append((ordinal, frame))

            if progress and ordinal % 100 == 0:
                progress("Loaded s6 candidate files: {}/{}".format(ordinal, n_rows))
    else:
        jobs = [
            (ordinal, row.to_dict(), str(quality_order_csv), str(organized_root))
            for ordinal, (_, row) in enumerate(rows, start=1)
        ]
        done = 0
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_load_s6_candidate_worker, job) for job in jobs]
            for future in as_completed(futures):
                done += 1
                ordinal, status, frame = future.result()
                if status == "ok" and not frame.empty:
                    frames.append((ordinal, frame))
                elif status == "missing":
                    n_missing += 1
                    if progress and n_missing <= 5:
                        progress("Missing s6 candidate file (worker): {}".format(jobs[ordinal - 1][1].get("path", "")))
                else:
                    n_empty += 1
                if progress and done % 100 == 0:
                    progress("Loaded s6 candidate files: {}/{}".format(done, n_rows))

    if not frames:
        return pd.DataFrame(), (
            "no source observations could be loaded from s6 quality-order paths "
            "(missing_files={}, empty_or_unreadable_files={})".format(n_missing, n_empty)
        )

    frames.sort(key=lambda item: item[0])
    out = pd.concat([frame for _, frame in frames], ignore_index=True, sort=False)
    note = (
        "s6 candidate source observations loaded from {} files; "
        "missing_files={}; empty_or_unreadable_files={}; overlap_groups_only={}; require_mixed_families={}; io_workers={}"
    ).format(len(frames), n_missing, n_empty, bool(overlap_groups_only), bool(require_mixed_families), workers)
    return out, note


def _normalize_resolution(value) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    numeric = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.notna(numeric) and float(numeric).is_integer():
        return RESOLUTION_CODE.get(int(numeric), text)
    low = text.lower()
    if low == "single_point":
        return "daily"
    if low == "quarterly":
        return "monthly"
    if low == "annually_climatology":
        return "climatology"
    return low


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
    out["cluster_uid"] = _extract_column(raw, ("cluster_uid", "station_uid", "cluster_uuid"), "").map(_clean_text)
    out["cluster_id"] = _extract_column(raw, ("cluster_id", "station_id", "master_station_index", "station_index"), "").map(_clean_text)
    out["resolution"] = _extract_column(raw, ("resolution", "time_resolution", "temporal_resolution"), "").map(_normalize_resolution)
    out["time"] = _coerce_datetime_from_columns(raw)

    out["source_station_uid"] = _extract_column(
        raw,
        ("source_station_uid", "station_uid", "source_station_id", "source_station_native_id"),
        "",
    ).map(_clean_text)

    source_col = _first_existing(
        raw.columns,
        (
            "source",
            "source_name",
            "source_dataset",
            "dataset",
            "dataset_name",
            "data_source_name",
            "Data_Source_Name",
            "dataset_from_path",
            "global_source",
        ),
    )
    family_col = _first_existing(
        raw.columns,
        (
            "source_family",
            "source_type",
            "source_category",
            "observation_type",
            "Type",
            "type",
            "family",
            "global_source",
        ),
    )
    out["source"] = raw[source_col].map(_clean_text) if source_col else ""
    missing_source = out["source"].astype(str).str.strip().eq("")
    for fallback_col in ("data_source_name", "Data_Source_Name", "dataset_from_path", "global_source", "source_station_uid"):
        if fallback_col in raw.columns and missing_source.any():
            fallback_values = raw[fallback_col].map(_clean_text)
            use_fallback = missing_source & fallback_values.astype(str).str.strip().ne("")
            out.loc[use_fallback, "source"] = fallback_values.loc[use_fallback]
            missing_source = out["source"].astype(str).str.strip().eq("")

    raw_family = raw[family_col].map(_clean_text) if family_col else pd.Series([""] * len(raw), index=raw.index)

    source_contexts = []
    context_cols = [
        source_col,
        "data_source_name",
        "Data_Source_Name",
        "dataset_from_path",
        "global_source",
        "observation_type",
        "Type",
        "type",
        "source_path",
    ]
    for idx, row in raw.iterrows():
        hints = [out.loc[idx, "source"] if idx in out.index else ""]
        for col in context_cols:
            if col and col in raw.columns:
                hints.append(_clean_text(row.get(col, "")))
        unique_hints = []
        seen_hints = set()
        for hint in hints:
            if hint and hint.lower() not in seen_hints:
                unique_hints.append(hint)
                seen_hints.add(hint.lower())
        source_contexts.append(" | ".join(unique_hints))

    out["source_family"] = [
        classify_source_family(source_context, taxonomy_overrides, family)
        for source_context, family in zip(source_contexts, raw_family)
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


def _pair_group_rows(group: pd.DataFrame, windows: Sequence[str], input_mode: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    satellites = group[group["source_family"] == "satellite"].copy()
    insitu = group[group["source_family"] == "in_situ"].copy()
    if satellites.empty or insitu.empty:
        return rows

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
    return rows


def pair_satellite_insitu_records(
    observations: pd.DataFrame,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    input_mode: str = "",
    pair_workers: Optional[int] = None,
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
    group_cols = ["_cluster_key", "resolution"]
    grouped = [group.copy() for _, group in work.groupby(group_cols, dropna=False, sort=False)]
    workers = _effective_worker_count(pair_workers, len(grouped), cap=8)
    rows: List[Dict[str, object]] = []

    if progress:
        progress("Pairing workers: {} across {} groups".format(workers, len(grouped)))

    if workers <= 1:
        for group in grouped:
            rows.extend(_pair_group_rows(group, windows=windows, input_mode=input_mode))
    else:
        done = 0
        group_rows: List[Tuple[int, List[Dict[str, object]]]] = []
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_pair_group_rows, group, tuple(windows), input_mode): idx
                for idx, group in enumerate(grouped, start=1)
            }
            for future in as_completed(futures):
                done += 1
                group_rows.append((futures[future], future.result()))
                if progress and done % 200 == 0:
                    progress("Pairing progress: {}/{} groups".format(done, len(grouped)))
        group_rows.sort(key=lambda item: item[0])
        for _, result_rows in group_rows:
            rows.extend(result_rows)

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
        for keys, group in pair_records.groupby(cols, dropna=False):
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
            ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: SSC not requested in RUN_FIGURE_VARIABLES"),
            ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: SSC not requested in RUN_FIGURE_VARIABLES"),
            ("figures/satellite_insitu_metric_heatmap.png", "skipped: SSC not requested in RUN_FIGURE_VARIABLES"),
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
    lines.append("- Input file: `{}`.".format(input_path if input_path is not None else ""))
    lines.append("- Load note: {}.".format(load_note))
    lines.append("- Observation rows after normalization: {}.".format(len(observations)))

    if not observations.empty:
        family_counts = observations["source_family"].value_counts(dropna=False).to_dict()
        lines.append("- Source family counts: `{}`.".format(family_counts))
        source_counts = observations["source"].value_counts(dropna=False).head(20).to_dict()
        lines.append("- Top source counts: `{}`.".format(source_counts))

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
    elif input_mode == "s6-candidates":
        lines.append("- This mode reads candidate source files from s6_cluster_quality_order.csv, after s6 and before s8.")
        lines.append("- It avoids stale s8 release outputs and preserves non-selected candidate source values for pairwise validation.")
    elif input_mode == "s6-selected":
        lines.append("- This mode uses only s6-selected records. Candidate sources that lost the s6 quality-ranking selection are not represented.")
        lines.append("- It is best for smoke testing and selected-record validation after s6, before s8 release packaging; true source-pair validation usually needs s6-candidates or release sidecar.")
    elif input_mode == "s5-candidates":
        lines.append("- This mode reads candidate source files from s5 paths, before s6 selection. It is useful for candidate-level validation.")
        lines.append("- Path resolution depends on RUN_ORGANIZED_ROOT and the s5 path column.")
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


def _auto_input_mode(
    release_dir: Path,
    candidate_sidecar: Optional[Path],
    s6_nc: Optional[Path],
    s5_csv: Optional[Path],
    s6_quality_order_csv: Optional[Path] = None,
) -> str:
    # For this pre-s8 validation script, prefer the current s6 candidate list so
    # auto mode does not accidentally read stale sed_reference_release outputs.
    if s6_quality_order_csv is not None and s6_quality_order_csv.exists():
        return "s6-candidates"
    if s6_nc is not None and s6_nc.exists():
        return "s6-selected"
    if s5_csv is not None and s5_csv.exists():
        return "s5-candidates"
    if candidate_sidecar is not None and candidate_sidecar.exists():
        return "release"
    if release_dir.exists() and (_find_candidate_sidecar(release_dir, None) is not None or (release_dir / MASTER_FILE).exists()):
        return "release"
    return "release"


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
    progress=log_progress,
    input_mode: str = "auto",
    s6_nc: Optional[Path] = None,
    s6_quality_order_csv: Optional[Path] = None,
    s5_csv: Optional[Path] = None,
    organized_root: Optional[Path] = None,
    max_s5_files: Optional[int] = None,
    max_s6_candidate_files: Optional[int] = None,
    s6_candidates_overlap_groups_only: bool = True,
    s6_candidates_require_mixed_families: bool = False,
    candidate_io_workers: Optional[int] = None,
    pairing_workers: Optional[int] = None,
) -> None:
    release_dir = Path(release_dir).expanduser().resolve()
    out_dir = Path(out_dir).expanduser().resolve()
    s6_nc = Path(s6_nc).expanduser().resolve() if s6_nc is not None else DEFAULT_S6_NC
    s6_quality_order_csv = (
        Path(s6_quality_order_csv).expanduser().resolve()
        if s6_quality_order_csv is not None
        else DEFAULT_S6_QUALITY_ORDER_CSV
    )
    s5_csv = Path(s5_csv).expanduser().resolve() if s5_csv is not None else DEFAULT_S5_CSV
    organized_root = Path(organized_root).expanduser().resolve() if organized_root is not None else DEFAULT_ORGANIZED_ROOT

    unknown_windows = [window for window in windows if window not in WINDOW_DAYS]
    if unknown_windows:
        raise SystemExit("unknown pairing windows: {}".format(", ".join(unknown_windows)))

    if input_mode == "auto":
        input_mode = _auto_input_mode(release_dir, candidate_sidecar, s6_nc, s5_csv, s6_quality_order_csv)

    if progress:
        progress("Starting s11 satellite / in-situ validation")
        progress("Input mode: {}".format(input_mode))
        progress("Output dir: {}".format(out_dir))

    out_dir.mkdir(parents=True, exist_ok=True)

    taxonomy = load_source_taxonomy(source_taxonomy_csv)
    external_attrs = _load_external_attributes(external_attributes_csv)

    input_path: Optional[Path] = None
    raw = pd.DataFrame()
    load_note = ""

    if input_mode == "release":
        if not release_dir.exists() or not release_dir.is_dir():
            raise SystemExit("release-dir does not exist or is not a directory: {}".format(release_dir))

        input_path = _find_candidate_sidecar(release_dir, candidate_sidecar)
        if input_path is not None:
            raw, input_path, loaded_mode = load_observations_from_candidate_sidecar(
                release_dir,
                input_path,
                progress=progress,
            )
            input_mode = loaded_mode
            load_note = "candidate sidecar loaded"

        if raw.empty:
            if candidate_sidecar is not None and not allow_master_fallback:
                raise SystemExit("candidate sidecar not found or empty: {}".format(candidate_sidecar))
            if not allow_master_fallback:
                raise SystemExit("candidate sidecar not found and master fallback is disabled")
            raw, load_note = load_observations_from_master_nc(release_dir, progress=progress)
            input_path = release_dir / MASTER_FILE
            input_mode = "selected_master"

    elif input_mode == "s6-candidates":
        raw, load_note = load_observations_from_s6_candidates(
            quality_order_csv=s6_quality_order_csv,
            organized_root=organized_root,
            max_files=max_s6_candidate_files,
            overlap_groups_only=s6_candidates_overlap_groups_only,
            require_mixed_families=s6_candidates_require_mixed_families,
            io_workers=candidate_io_workers,
            progress=progress,
        )
        input_path = s6_quality_order_csv
        if raw.empty:
            raise SystemExit(load_note)

    elif input_mode == "s6-selected":
        raw, load_note = load_observations_from_s6_nc(s6_nc, progress=progress)
        input_path = s6_nc
        if raw.empty:
            raise SystemExit(load_note)

    elif input_mode == "s5-candidates":
        raw, load_note = load_observations_from_s5_candidates(
            s5_csv=s5_csv,
            organized_root=organized_root,
            max_files=max_s5_files,
            io_workers=candidate_io_workers,
            progress=progress,
        )
        input_path = s5_csv
        if raw.empty:
            raise SystemExit(load_note)

    else:
        raise SystemExit("unknown input mode: {}".format(input_mode))

    observations = normalize_observation_table(raw, taxonomy, input_mode=input_mode)
    if progress:
        progress("Normalized observations: {}".format(len(observations)))
        if not observations.empty:
            progress("Source families: {}".format(observations["source_family"].value_counts(dropna=False).to_dict()))

    pair_records = pair_satellite_insitu_records(
        observations,
        windows=windows,
        input_mode=input_mode,
        pair_workers=pairing_workers,
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
                ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: RUN_NO_FIGURES=True"),
                ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: RUN_NO_FIGURES=True"),
                ("figures/satellite_insitu_metric_heatmap.png", "skipped: RUN_NO_FIGURES=True"),
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
        progress("Summary: {}".format(summary_path))


def _resolve_config_path(value) -> Optional[Path]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Path(text).expanduser().resolve()


def run_from_builtin_config() -> None:
    input_mode = str(RUN_INPUT_MODE).strip()

    release_dir = Path(RUN_RELEASE_DIR).expanduser().resolve()
    s6_nc = Path(RUN_S6_NC).expanduser().resolve()
    s6_quality_order_csv = Path(RUN_S6_QUALITY_ORDER_CSV).expanduser().resolve()
    s5_csv = Path(RUN_S5_CSV).expanduser().resolve()
    organized_root = Path(RUN_ORGANIZED_ROOT).expanduser().resolve()

    out_dir = RUN_OUT_DIR_BY_MODE.get(
        input_mode,
        OUTPUT_R_ROOT / "scripts_basin_test/output/validation_results_builtin",
    )
    out_dir = Path(out_dir).expanduser().resolve()

    source_taxonomy_csv = _resolve_config_path(RUN_SOURCE_TAXONOMY_CSV)
    external_attributes_csv = _resolve_config_path(RUN_EXTERNAL_ATTRIBUTES_CSV)

    run_validation(
        release_dir=release_dir,
        out_dir=out_dir,
        candidate_sidecar=None,
        source_taxonomy_csv=source_taxonomy_csv,
        external_attributes_csv=external_attributes_csv,
        allow_master_fallback=RUN_ALLOW_MASTER_FALLBACK,
        windows=RUN_WINDOWS,
        high_turbidity_ssc=float(RUN_HIGH_TURBIDITY_SSC),
        ssc_bin_edges=RUN_SSC_BIN_EDGES,
        figure_variables=RUN_FIGURE_VARIABLES,
        write_plots=not RUN_NO_FIGURES,
        input_mode=input_mode,
        s6_nc=s6_nc,
        s6_quality_order_csv=s6_quality_order_csv,
        s5_csv=s5_csv,
        organized_root=organized_root,
        max_s5_files=RUN_MAX_S5_FILES,
        max_s6_candidate_files=RUN_MAX_S6_CANDIDATE_FILES,
        s6_candidates_overlap_groups_only=RUN_S6_CANDIDATES_OVERLAP_GROUPS_ONLY,
        s6_candidates_require_mixed_families=RUN_S6_CANDIDATES_REQUIRE_MIXED_FAMILIES,
        candidate_io_workers=RUN_CANDIDATE_IO_WORKERS,
        pairing_workers=RUN_PAIRING_WORKERS,
    )


def main() -> None:
    run_from_builtin_config()


if __name__ == "__main__":
    main()
