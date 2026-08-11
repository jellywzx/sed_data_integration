#!/usr/bin/env python3
"""Cross-resolution consistency assessment for the main sediment matrices.

This read-only validation script compares the released daily, monthly, and
annual station-reference matrices after aggregating the finer-resolution
product to the calendar support of the coarser-resolution product:

* daily -> monthly
* daily -> annual
* monthly -> annual

The script has two complementary purposes:

1. Product-processing consistency: determine whether values in a finer matrix,
   after transparent temporal aggregation, agree with values in a coarser
   matrix at the same ``station_uid`` and calendar period.
2. Provenance-aware cross-source diagnostics: distinguish comparisons that use
   the same source station from comparisons supported by different source
   datasets. Same-source comparisons are processing checks, not independent
   observational validation.

The script never modifies release products. It writes detailed pair tables,
structural diagnostics, summary statistics, publication-style figures, and a
Markdown report under ``output/validation_results/cross_resolution`` by
default.

Expected release inputs
-----------------------
* sed_reference_timeseries_daily.nc
* sed_reference_timeseries_monthly.nc
* sed_reference_timeseries_annual.nc
* source_station_catalog.csv

Core conventions
----------------
* Q is compared as an arithmetic mean in m3 s-1.
* SSC is compared as an arithmetic mean in mg L-1.
* SSL is compared as mean daily load in t d-1, not as a monthly/annual sum.
* ``good`` mode retains flag 0 only.
* ``analysis_ready`` mode retains flags 0 and 1.
* Zero values are retained for linear-space errors but excluded from log-ratio
  statistics and log-log figures.
* All available pairs are retained. Observation support is reported and
  classified rather than hidden behind a single hard-coded coverage cutoff.

Example
-------
python validate/s14_validate_cross_resolution_consistency.py \
  --release-dir output/sed_reference_release \
  --out-dir output/validation_results/cross_resolution \
  --flag-modes good,analysis_ready \
  --monthly-weighting both \
  --overwrite
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time as time_module
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - checked explicitly in main
    nc4 = None

try:
    from scipy import stats as scipy_stats
except ImportError:  # pragma: no cover - statistics have fallbacks
    scipy_stats = None


VARIABLES: Tuple[str, ...] = ("Q", "SSC", "SSL")
FLAG_VARIABLES: Mapping[str, str] = {
    "Q": "Q_flag",
    "SSC": "SSC_flag",
    "SSL": "SSL_flag",
}
FLAG_MODES: Mapping[str, Tuple[int, ...]] = {
    "good": (0,),
    "analysis_ready": (0, 1),
}
COMPARISON_SPECS: Tuple[Tuple[str, str, str], ...] = (
    ("daily_monthly", "daily", "monthly"),
    ("daily_annual", "daily", "annual"),
    ("monthly_annual", "monthly", "annual"),
)
MATRIX_FILENAMES: Mapping[str, str] = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
FILL_VALUES: Tuple[float, ...] = (-9999.0, 9.969209968386869e36)

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_RELEASE_DIR = REPO_ROOT / "output" / "sed_reference_release"
DEFAULT_OUT_DIR = SCRIPT_DIR / "output" / "validate_cross_resolution"
DEFAULT_SOURCE_CATALOG = DEFAULT_RELEASE_DIR / "source_station_catalog.csv"


@dataclass(frozen=True)
class MatrixStationMetadata:
    station_uid: str
    row_index: int
    lat: float
    lon: float
    basin_area: float
    station_name: str
    river_name: str
    basin_status: str


@dataclass(frozen=True)
class SourceInfo:
    source_station_uid: str
    resolution: str
    source_name: str
    native_id: str
    station_name: str
    river_name: str
    lat: float
    lon: float
    canonical_key: str


class ValidationError(RuntimeError):
    """Raised for actionable input or release-structure problems."""


def log(message: str) -> None:
    stamp = time_module.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, (bytes, np.bytes_)):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).replace("\x00", "").strip()
    if text.lower() in {"", "nan", "none", "nat", "null", "na", "n/a"}:
        return ""
    return text


def normalize_token(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def decode_text_vector(values) -> List[str]:
    """Decode NetCDF VLEN strings or fixed-width character arrays."""
    arr = np.ma.asarray(values)
    if np.ma.isMaskedArray(arr):
        if arr.dtype.kind in {"S", "U", "O"}:
            arr = arr.filled("")
        else:
            arr = arr.filled(np.nan)
    arr = np.asarray(arr)
    if arr.ndim == 0:
        return [clean_text(arr.item())]

    if arr.dtype.kind in {"S", "U"} and arr.ndim >= 2 and arr.dtype.itemsize <= 4:
        rows = arr.reshape((-1, arr.shape[-1]))
        decoded = []
        for row in rows:
            decoded.append("".join(clean_text(item) for item in row).strip())
        return decoded

    return [clean_text(item) for item in arr.reshape(-1)]


def read_numeric(values) -> np.ndarray:
    arr = np.ma.asarray(values).astype(np.float64)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    arr = np.asarray(arr, dtype=np.float64)
    for fill in FILL_VALUES:
        arr[arr == fill] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def read_flags(values) -> np.ndarray:
    arr = np.ma.asarray(values)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(9)
    arr = np.asarray(arr)
    out = np.full(arr.shape, 9, dtype=np.int16)
    try:
        numeric = arr.astype(np.float64)
        valid = np.isfinite(numeric)
        out[valid] = numeric[valid].astype(np.int16)
    except (TypeError, ValueError):
        pass
    out[~np.isin(out, np.array([0, 1, 2, 3, 9], dtype=np.int16))] = 9
    return out


def read_indices(values, fill: int = -1) -> np.ndarray:
    arr = np.ma.asarray(values)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(fill)
    arr = np.asarray(arr)
    out = np.full(arr.shape, fill, dtype=np.int64)
    try:
        numeric = arr.astype(np.float64)
        valid = np.isfinite(numeric)
        out[valid] = numeric[valid].astype(np.int64)
    except (TypeError, ValueError):
        pass
    return out


def decode_time_variable(var) -> pd.DatetimeIndex:
    values = np.ma.asarray(var[:])
    if np.ma.isMaskedArray(values):
        values = values.filled(np.nan)
    units = clean_text(getattr(var, "units", "days since 1970-01-01"))
    calendar = clean_text(getattr(var, "calendar", "gregorian")) or "gregorian"
    try:
        decoded = nc4.num2date(
            values,
            units=units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=False,
        )
        strings = []
        for item in np.asarray(decoded).reshape(-1):
            if item is None:
                strings.append("")
            else:
                try:
                    strings.append(item.isoformat())
                except Exception:
                    strings.append(str(item))
        result = pd.to_datetime(strings, errors="coerce")
    except Exception:
        numeric = pd.to_numeric(np.asarray(values).reshape(-1), errors="coerce")
        result = pd.to_datetime(numeric, unit="D", origin="1970-01-01", errors="coerce")
    return pd.DatetimeIndex(result).tz_localize(None)


def _read_optional_station_numeric(ds, name: str, size: int) -> np.ndarray:
    if name not in ds.variables:
        return np.full(size, np.nan, dtype=np.float64)
    values = read_numeric(ds.variables[name][:]).reshape(-1)
    if len(values) != size:
        raise ValidationError(
            f"Variable {name!r} has {len(values)} values; expected {size} station values"
        )
    return values


def _read_optional_station_text(ds, name: str, size: int) -> List[str]:
    if name not in ds.variables:
        return [""] * size
    values = decode_text_vector(ds.variables[name][:])
    if len(values) != size:
        raise ValidationError(
            f"Variable {name!r} has {len(values)} values; expected {size} station values"
        )
    return values


class MatrixReader:
    """Memory-conscious row reader for one released station-by-time matrix."""

    def __init__(self, path: Path, resolution: str):
        if nc4 is None:
            raise ValidationError("netCDF4 is required; install it with `pip install netCDF4`")
        self.path = Path(path)
        self.resolution = resolution
        if not self.path.is_file():
            raise FileNotFoundError(f"Matrix file not found: {self.path}")
        self.ds = nc4.Dataset(self.path, "r")

        required = {"station_uid", "time"}.union(VARIABLES).union(FLAG_VARIABLES.values())
        missing = sorted(name for name in required if name not in self.ds.variables)
        if missing:
            self.close()
            raise ValidationError(f"{self.path.name} is missing required variables: {missing}")

        self.station_uids = decode_text_vector(self.ds.variables["station_uid"][:])
        if not self.station_uids:
            self.close()
            raise ValidationError(f"No station_uid values found in {self.path}")
        if len(set(self.station_uids)) != len(self.station_uids):
            duplicates = pd.Series(self.station_uids).value_counts()
            duplicate_ids = duplicates[duplicates > 1].index.tolist()[:10]
            self.close()
            raise ValidationError(
                f"Duplicate station_uid values in {self.path.name}: {duplicate_ids}"
            )
        self.index_by_uid = {uid: idx for idx, uid in enumerate(self.station_uids)}
        self.time = decode_time_variable(self.ds.variables["time"])
        if self.time.isna().all():
            self.close()
            raise ValidationError(f"Could not decode time coordinate in {self.path}")

        n_stations = len(self.station_uids)
        self.lat = _read_optional_station_numeric(self.ds, "lat", n_stations)
        self.lon = _read_optional_station_numeric(self.ds, "lon", n_stations)
        self.basin_area = _read_optional_station_numeric(self.ds, "basin_area", n_stations)
        self.station_name = _read_optional_station_text(self.ds, "station_name", n_stations)
        self.river_name = _read_optional_station_text(self.ds, "river_name", n_stations)
        self.basin_status = _read_optional_station_text(self.ds, "basin_status", n_stations)
        self.source_names = (
            decode_text_vector(self.ds.variables["source_name"][:])
            if "source_name" in self.ds.variables
            else []
        )

    def close(self) -> None:
        try:
            self.ds.close()
        except Exception:
            pass

    def __enter__(self) -> "MatrixReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def metadata(self, station_uid: str) -> MatrixStationMetadata:
        idx = self.index_by_uid[station_uid]
        return MatrixStationMetadata(
            station_uid=station_uid,
            row_index=idx,
            lat=float(self.lat[idx]) if np.isfinite(self.lat[idx]) else np.nan,
            lon=float(self.lon[idx]) if np.isfinite(self.lon[idx]) else np.nan,
            basin_area=(
                float(self.basin_area[idx]) if np.isfinite(self.basin_area[idx]) else np.nan
            ),
            station_name=self.station_name[idx],
            river_name=self.river_name[idx],
            basin_status=self.basin_status[idx],
        )

    def _read_row(self, variable_name: str, row_index: int):
        var = self.ds.variables[variable_name]
        dims = tuple(getattr(var, "dimensions", ()))
        if not dims:
            return var[:]
        station_dim = None
        for candidate in ("n_stations", "station", "stations"):
            if candidate in dims:
                station_dim = candidate
                break
        if station_dim is None:
            # The released matrices use n_stations as the first dimension.
            station_axis = 0
        else:
            station_axis = dims.index(station_dim)
        slices = [slice(None)] * len(dims)
        slices[station_axis] = row_index
        return var[tuple(slices)]

    def read_cluster_rows(self, station_uid: str) -> Dict[str, object]:
        idx = self.index_by_uid[station_uid]
        values = {name: read_numeric(self._read_row(name, idx)).reshape(-1) for name in VARIABLES}
        flags = {
            name: read_flags(self._read_row(FLAG_VARIABLES[name], idx)).reshape(-1)
            for name in VARIABLES
        }

        n_time = len(self.time)
        for name in VARIABLES:
            if len(values[name]) != n_time or len(flags[name]) != n_time:
                raise ValidationError(
                    f"{self.path.name}: row length mismatch for {station_uid} {name}; "
                    f"values={len(values[name])}, flags={len(flags[name])}, time={n_time}"
                )

        if "selected_source_station_uid" in self.ds.variables:
            source_uids = decode_text_vector(
                self._read_row("selected_source_station_uid", idx)
            )
        else:
            source_uids = [""] * n_time
        if len(source_uids) != n_time:
            source_uids = (source_uids + [""] * n_time)[:n_time]

        if "selected_source_index" in self.ds.variables:
            source_indices = read_indices(
                self._read_row("selected_source_index", idx), fill=-1
            ).reshape(-1)
        else:
            source_indices = np.full(n_time, -1, dtype=np.int64)
        if len(source_indices) != n_time:
            padded = np.full(n_time, -1, dtype=np.int64)
            limit = min(n_time, len(source_indices))
            padded[:limit] = source_indices[:limit]
            source_indices = padded

        source_names = []
        for source_index in source_indices:
            integer = int(source_index)
            if 0 <= integer < len(self.source_names):
                source_names.append(self.source_names[integer])
            else:
                source_names.append("")

        return {
            "time": self.time,
            "values": values,
            "flags": flags,
            "source_station_uid": np.asarray(source_uids, dtype=object),
            "source_name": np.asarray(source_names, dtype=object),
        }


def choose_column(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    exact = {str(col): str(col) for col in columns}
    lower = {str(col).lower(): str(col) for col in columns}
    for candidate in candidates:
        if candidate in exact:
            return exact[candidate]
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def numeric_or_nan(value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return np.nan
    return number if np.isfinite(number) else np.nan


def canonical_source_key(
    source_name: str,
    native_id: str,
    station_name: str,
    river_name: str,
    lat: float,
    lon: float,
    fallback_uid: str,
) -> str:
    source_key = normalize_token(source_name) or "unknown_source"
    native_key = normalize_token(native_id)
    if native_key:
        return f"{source_key}|id:{native_key}"

    station_key = normalize_token(station_name)
    river_key = normalize_token(river_name)
    if np.isfinite(lat) and np.isfinite(lon):
        coordinate_key = f"{lat:.4f},{lon:.4f}"
        if station_key or river_key:
            return f"{source_key}|name:{station_key}|river:{river_key}|coord:{coordinate_key}"
        return f"{source_key}|coord:{coordinate_key}"

    if station_key or river_key:
        return f"{source_key}|name:{station_key}|river:{river_key}"
    return f"{source_key}|uid:{normalize_token(fallback_uid) or 'unknown'}"


class SourceCatalogResolver:
    """Resolve matrix provenance UIDs to cross-resolution source identities."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.by_uid_resolution: Dict[Tuple[str, str], SourceInfo] = {}
        self.unique_by_uid: Dict[str, SourceInfo] = {}
        self._cache: Dict[Tuple[str, str, str], SourceInfo] = {}
        if not self.path.is_file():
            log(f"Source-station catalogue not found; provenance identity will use matrix fields only: {self.path}")
            return

        table = pd.read_csv(self.path, keep_default_na=False, low_memory=False)
        uid_col = choose_column(table.columns, ("source_station_uid", "station_uid"))
        resolution_col = choose_column(table.columns, ("resolution", "time_resolution"))
        source_col = choose_column(table.columns, ("source_name", "source", "dataset"))
        native_col = choose_column(
            table.columns,
            ("source_station_native_id", "source_station_id", "native_id"),
        )
        station_col = choose_column(
            table.columns,
            ("source_station_name", "station_name"),
        )
        river_col = choose_column(
            table.columns,
            ("source_station_river_name", "river_name"),
        )
        lat_col = choose_column(
            table.columns,
            ("source_station_lat", "lat", "latitude"),
        )
        lon_col = choose_column(
            table.columns,
            ("source_station_lon", "lon", "longitude"),
        )
        if uid_col is None:
            log(f"Catalogue {self.path.name} lacks source_station_uid; using matrix provenance only")
            return

        seen_by_uid: MutableMapping[str, List[SourceInfo]] = {}
        for row in table.to_dict(orient="records"):
            uid = clean_text(row.get(uid_col, ""))
            if not uid:
                continue
            resolution = clean_text(row.get(resolution_col, "")).lower() if resolution_col else ""
            source_name = clean_text(row.get(source_col, "")) if source_col else ""
            native_id = clean_text(row.get(native_col, "")) if native_col else ""
            station_name = clean_text(row.get(station_col, "")) if station_col else ""
            river_name = clean_text(row.get(river_col, "")) if river_col else ""
            lat = numeric_or_nan(row.get(lat_col, np.nan)) if lat_col else np.nan
            lon = numeric_or_nan(row.get(lon_col, np.nan)) if lon_col else np.nan
            key = canonical_source_key(
                source_name,
                native_id,
                station_name,
                river_name,
                lat,
                lon,
                uid,
            )
            info = SourceInfo(
                source_station_uid=uid,
                resolution=resolution,
                source_name=source_name,
                native_id=native_id,
                station_name=station_name,
                river_name=river_name,
                lat=lat,
                lon=lon,
                canonical_key=key,
            )
            self.by_uid_resolution[(uid, resolution)] = info
            seen_by_uid.setdefault(uid, []).append(info)

        for uid, infos in seen_by_uid.items():
            canonical_keys = {info.canonical_key for info in infos}
            if len(canonical_keys) == 1:
                self.unique_by_uid[uid] = infos[0]

        log(
            f"Loaded source catalogue: rows={len(table):,}, "
            f"uid-resolution keys={len(self.by_uid_resolution):,}"
        )

    def resolve(self, uid: str, resolution: str, matrix_source_name: str = "") -> SourceInfo:
        uid = clean_text(uid)
        resolution = clean_text(resolution).lower()
        matrix_source_name = clean_text(matrix_source_name)
        cache_key = (uid, resolution, matrix_source_name)
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        info = self.by_uid_resolution.get((uid, resolution))
        if info is None:
            info = self.unique_by_uid.get(uid)
        if info is None:
            source_name = matrix_source_name
            key = canonical_source_key(
                source_name=source_name,
                native_id="",
                station_name="",
                river_name="",
                lat=np.nan,
                lon=np.nan,
                fallback_uid=uid,
            )
            info = SourceInfo(
                source_station_uid=uid,
                resolution=resolution,
                source_name=source_name,
                native_id="",
                station_name="",
                river_name="",
                lat=np.nan,
                lon=np.nan,
                canonical_key=key,
            )
        elif not info.source_name and matrix_source_name:
            key = canonical_source_key(
                source_name=matrix_source_name,
                native_id=info.native_id,
                station_name=info.station_name,
                river_name=info.river_name,
                lat=info.lat,
                lon=info.lon,
                fallback_uid=uid,
            )
            info = SourceInfo(
                source_station_uid=info.source_station_uid,
                resolution=info.resolution,
                source_name=matrix_source_name,
                native_id=info.native_id,
                station_name=info.station_name,
                river_name=info.river_name,
                lat=info.lat,
                lon=info.lon,
                canonical_key=key,
            )

        self._cache[cache_key] = info
        return info


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if not all(np.isfinite(value) for value in (lat1, lon1, lat2, lon2)):
        return np.nan
    radius = 6_371_008.8
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return 2.0 * radius * math.asin(min(1.0, math.sqrt(a)))


def symmetric_relative_difference(a: float, b: float) -> float:
    if not np.isfinite(a) or not np.isfinite(b):
        return np.nan
    denominator = max(abs(a), abs(b))
    if denominator == 0:
        return 0.0
    return abs(a - b) / denominator


def structural_consistency_rows(
    comparison: str,
    lower: MatrixReader,
    higher: MatrixReader,
    common_uids: Sequence[str],
    coordinate_tolerance_m: float,
    basin_area_tolerance: float,
) -> pd.DataFrame:
    rows = []
    for uid in common_uids:
        a = lower.metadata(uid)
        b = higher.metadata(uid)
        distance = haversine_m(a.lat, a.lon, b.lat, b.lon)
        area_diff = symmetric_relative_difference(a.basin_area, b.basin_area)
        coordinate_pass = bool(np.isfinite(distance) and distance <= coordinate_tolerance_m)
        if not np.isfinite(distance):
            coordinate_pass = not (
                np.isfinite(a.lat)
                or np.isfinite(a.lon)
                or np.isfinite(b.lat)
                or np.isfinite(b.lon)
            )
        area_pass = bool(np.isfinite(area_diff) and area_diff <= basin_area_tolerance)
        if not np.isfinite(area_diff):
            area_pass = not (np.isfinite(a.basin_area) or np.isfinite(b.basin_area))
        rows.append(
            {
                "comparison": comparison,
                "station_uid": uid,
                "lower_resolution": lower.resolution,
                "higher_resolution": higher.resolution,
                "coordinate_difference_m": distance,
                "coordinate_pass": coordinate_pass,
                "lower_lat": a.lat,
                "lower_lon": a.lon,
                "higher_lat": b.lat,
                "higher_lon": b.lon,
                "lower_basin_area_km2": a.basin_area,
                "higher_basin_area_km2": b.basin_area,
                "basin_area_relative_difference": area_diff,
                "basin_area_pass": area_pass,
                "lower_basin_status": a.basin_status,
                "higher_basin_status": b.basin_status,
                "basin_status_changed": clean_text(a.basin_status) != clean_text(b.basin_status),
                "lower_station_name": a.station_name,
                "higher_station_name": b.station_name,
                "station_name_changed": clean_text(a.station_name) != clean_text(b.station_name),
                "lower_river_name": a.river_name,
                "higher_river_name": b.river_name,
                "river_name_changed": clean_text(a.river_name) != clean_text(b.river_name),
                "structural_pass": coordinate_pass and area_pass,
            }
        )
    return pd.DataFrame(rows)


def period_labels(times: pd.DatetimeIndex, target_resolution: str) -> np.ndarray:
    if target_resolution == "monthly":
        return times.to_period("M").astype(str).to_numpy()
    if target_resolution == "annual":
        return times.year.astype("Int64").astype(str).to_numpy()
    raise ValueError(f"Unsupported target resolution: {target_resolution}")


def source_aggregation_methods(
    source_resolution: str,
    target_resolution: str,
    monthly_weighting: str,
) -> Tuple[str, ...]:
    if source_resolution == "monthly" and target_resolution == "annual":
        if monthly_weighting == "unweighted":
            return ("unweighted_monthly_mean",)
        if monthly_weighting == "days":
            return ("days_weighted_monthly_mean",)
        return ("unweighted_monthly_mean", "days_weighted_monthly_mean")
    return ("arithmetic_mean",)


def support_class(value: float, moderate_threshold: float, high_threshold: float) -> str:
    if not np.isfinite(value):
        return "unknown"
    if value >= high_threshold:
        return "high"
    if value >= moderate_threshold:
        return "moderate"
    return "low"


def derivation_class(flags: Sequence[int]) -> str:
    values = sorted(set(int(value) for value in flags))
    if values == [0]:
        return "reported"
    if values == [1]:
        return "derived"
    if values == [0, 1]:
        return "mixed_reported_derived"
    if not values:
        return "unknown"
    return "other"


def join_unique(values: Iterable[str]) -> str:
    cleaned = sorted({clean_text(value) for value in values if clean_text(value)})
    return "||".join(cleaned)


def provenance_payload(
    uids: Sequence[str],
    matrix_source_names: Sequence[str],
    resolution: str,
    resolver: SourceCatalogResolver,
) -> Dict[str, str]:
    infos = []
    for uid, matrix_source_name in zip(uids, matrix_source_names):
        uid_clean = clean_text(uid)
        source_clean = clean_text(matrix_source_name)
        if not uid_clean and not source_clean:
            continue
        infos.append(resolver.resolve(uid_clean, resolution, source_clean))
    return {
        "source_station_uids": join_unique(info.source_station_uid for info in infos),
        "source_names": join_unique(info.source_name for info in infos),
        "canonical_source_keys": join_unique(info.canonical_key for info in infos),
        "source_native_ids": join_unique(info.native_id for info in infos),
    }


def aggregate_group_value(values: np.ndarray, weights: np.ndarray, method: str) -> float:
    if len(values) == 0:
        return np.nan
    if method == "days_weighted_monthly_mean":
        valid = np.isfinite(values) & np.isfinite(weights) & (weights > 0)
        if not np.any(valid):
            return np.nan
        return float(np.average(values[valid], weights=weights[valid]))
    return float(np.nanmean(values))


def aggregate_matrix_row(
    row_data: Mapping[str, object],
    matrix_resolution: str,
    target_resolution: str,
    variable: str,
    allowed_flags: Sequence[int],
    aggregation_method: str,
    resolver: SourceCatalogResolver,
    moderate_support_fraction: float,
    high_support_fraction: float,
) -> pd.DataFrame:
    times = pd.DatetimeIndex(row_data["time"])
    values = np.asarray(row_data["values"][variable], dtype=np.float64)
    flags = np.asarray(row_data["flags"][variable], dtype=np.int16)
    source_uids = np.asarray(row_data["source_station_uid"], dtype=object)
    source_names = np.asarray(row_data["source_name"], dtype=object)
    periods = period_labels(times, target_resolution)

    valid = (
        times.notna()
        & np.isfinite(values)
        & np.isin(flags, np.asarray(tuple(allowed_flags), dtype=np.int16))
    )
    if not np.any(valid):
        return pd.DataFrame()

    frame = pd.DataFrame(
        {
            "time": times[valid],
            "period": periods[valid],
            "value": values[valid],
            "flag": flags[valid],
            "source_station_uid": source_uids[valid],
            "matrix_source_name": source_names[valid],
        }
    )
    frame["days_in_month"] = frame["time"].dt.days_in_month.astype(float)

    rows = []
    for period, group in frame.groupby("period", sort=True):
        group_values = group["value"].to_numpy(dtype=np.float64)
        weights = group["days_in_month"].to_numpy(dtype=np.float64)
        if aggregation_method == "days_weighted_monthly_mean":
            monthly = (
                group.assign(_month=group["time"].dt.to_period("M"))
                .groupby("_month", sort=True)
                .agg(value=("value", "mean"), days_in_month=("days_in_month", "first"))
            )
            aggregated = aggregate_group_value(
                monthly["value"].to_numpy(dtype=np.float64),
                monthly["days_in_month"].to_numpy(dtype=np.float64),
                aggregation_method,
            )
        else:
            aggregated = aggregate_group_value(group_values, weights, aggregation_method)
        if not np.isfinite(aggregated):
            continue

        unique_days = int(group["time"].dt.normalize().nunique())
        unique_months = int(group["time"].dt.to_period("M").nunique())
        if target_resolution == "monthly":
            timestamp = pd.Period(period, freq="M").start_time
            denominator = float(timestamp.days_in_month)
            support = unique_days / denominator if denominator > 0 else np.nan
        else:
            year = int(period)
            if matrix_resolution == "daily":
                denominator = 366.0 if pd.Timestamp(year=year, month=12, day=31).is_leap_year else 365.0
                support = unique_days / denominator
            elif matrix_resolution == "monthly":
                support = unique_months / 12.0
            else:
                support = np.nan

        provenance = provenance_payload(
            group["source_station_uid"].astype(str).tolist(),
            group["matrix_source_name"].astype(str).tolist(),
            matrix_resolution,
            resolver,
        )
        rows.append(
            {
                "period": str(period),
                "value": aggregated,
                "n_records": int(len(group)),
                "n_unique_days": unique_days,
                "n_unique_months": unique_months,
                "support_fraction": float(support) if np.isfinite(support) else np.nan,
                "support_class": support_class(
                    support,
                    moderate_threshold=moderate_support_fraction,
                    high_threshold=high_support_fraction,
                ),
                "flags_used": ",".join(str(value) for value in sorted(group["flag"].unique())),
                "derivation_class": derivation_class(group["flag"].tolist()),
                **provenance,
            }
        )
    return pd.DataFrame(rows)


def split_serialized(value: str) -> set:
    return {token for token in clean_text(value).split("||") if token}


def classify_provenance(lower_row: Mapping[str, object], higher_row: Mapping[str, object]) -> str:
    lower_keys = split_serialized(lower_row.get("canonical_source_keys", ""))
    higher_keys = split_serialized(higher_row.get("canonical_source_keys", ""))
    lower_sources = split_serialized(lower_row.get("source_names", ""))
    higher_sources = split_serialized(higher_row.get("source_names", ""))

    if len(lower_keys) > 1 and len(higher_keys) > 1:
        return "mixed_both_sources"
    if len(lower_keys) > 1:
        return "mixed_lower_sources"
    if len(higher_keys) > 1:
        return "mixed_higher_sources"
    if len(lower_keys) == 1 and len(higher_keys) == 1:
        if lower_keys == higher_keys:
            return "same_source_station"
        if lower_sources and higher_sources and lower_sources.intersection(higher_sources):
            return "same_source_dataset_different_station"
        if lower_sources and higher_sources and lower_sources.isdisjoint(higher_sources):
            return "different_source_dataset"
        return "different_or_unresolved_source_station"
    if lower_sources and higher_sources:
        if lower_sources.intersection(higher_sources):
            return "same_source_dataset_unresolved_station"
        return "different_source_dataset"
    return "unknown_provenance"


def pair_aggregates(
    comparison: str,
    station_uid: str,
    lower_resolution: str,
    higher_resolution: str,
    variable: str,
    flag_mode: str,
    aggregation_method: str,
    lower: pd.DataFrame,
    higher: pd.DataFrame,
) -> pd.DataFrame:
    if lower.empty or higher.empty:
        return pd.DataFrame()
    merged = lower.merge(higher, on="period", how="inner", suffixes=("_lower", "_higher"))
    if merged.empty:
        return pd.DataFrame()

    rows = []
    for row in merged.to_dict(orient="records"):
        lower_value = numeric_or_nan(row.get("value_lower"))
        higher_value = numeric_or_nan(row.get("value_higher"))
        if not np.isfinite(lower_value) or not np.isfinite(higher_value):
            continue
        provenance_class = classify_provenance(
            {
                "canonical_source_keys": row.get("canonical_source_keys_lower", ""),
                "source_names": row.get("source_names_lower", ""),
            },
            {
                "canonical_source_keys": row.get("canonical_source_keys_higher", ""),
                "source_names": row.get("source_names_higher", ""),
            },
        )
        difference = lower_value - higher_value
        ratio = np.nan
        log_ratio = np.nan
        if lower_value > 0 and higher_value > 0:
            ratio = lower_value / higher_value
            log_ratio = math.log10(ratio)
        rows.append(
            {
                "comparison": comparison,
                "station_uid": station_uid,
                "lower_resolution": lower_resolution,
                "higher_resolution": higher_resolution,
                "period": row["period"],
                "variable": variable,
                "flag_mode": flag_mode,
                "aggregation_method": aggregation_method,
                "lower_aggregated_value": lower_value,
                "higher_matrix_value": higher_value,
                "difference_lower_minus_higher": difference,
                "absolute_difference": abs(difference),
                "ratio_lower_over_higher": ratio,
                "log10_ratio_lower_over_higher": log_ratio,
                "n_lower_records": int(row.get("n_records_lower", 0)),
                "n_lower_unique_days": int(row.get("n_unique_days_lower", 0)),
                "n_lower_unique_months": int(row.get("n_unique_months_lower", 0)),
                "lower_support_fraction": numeric_or_nan(row.get("support_fraction_lower")),
                "lower_support_class": clean_text(row.get("support_class_lower", "unknown")),
                "n_higher_records": int(row.get("n_records_higher", 0)),
                "lower_flags_used": clean_text(row.get("flags_used_lower", "")),
                "higher_flags_used": clean_text(row.get("flags_used_higher", "")),
                "lower_derivation_class": clean_text(row.get("derivation_class_lower", "")),
                "higher_derivation_class": clean_text(row.get("derivation_class_higher", "")),
                "lower_source_station_uids": clean_text(row.get("source_station_uids_lower", "")),
                "higher_source_station_uids": clean_text(row.get("source_station_uids_higher", "")),
                "lower_source_names": clean_text(row.get("source_names_lower", "")),
                "higher_source_names": clean_text(row.get("source_names_higher", "")),
                "lower_source_native_ids": clean_text(row.get("source_native_ids_lower", "")),
                "higher_source_native_ids": clean_text(row.get("source_native_ids_higher", "")),
                "lower_canonical_source_keys": clean_text(
                    row.get("canonical_source_keys_lower", "")
                ),
                "higher_canonical_source_keys": clean_text(
                    row.get("canonical_source_keys_higher", "")
                ),
                "provenance_class": provenance_class,
                "derivation_pair_class": (
                    f"{clean_text(row.get('derivation_class_lower', 'unknown'))}--"
                    f"{clean_text(row.get('derivation_class_higher', 'unknown'))}"
                ),
            }
        )
    return pd.DataFrame(rows)


def safe_pearson(x: np.ndarray, y: np.ndarray) -> float:
    valid = np.isfinite(x) & np.isfinite(y)
    x, y = x[valid], y[valid]
    if len(x) < 2 or np.nanstd(x) == 0 or np.nanstd(y) == 0:
        return np.nan
    if scipy_stats is not None:
        try:
            return float(scipy_stats.pearsonr(x, y).statistic)
        except Exception:
            pass
    return float(np.corrcoef(x, y)[0, 1])


def safe_spearman(x: np.ndarray, y: np.ndarray) -> float:
    valid = np.isfinite(x) & np.isfinite(y)
    x, y = x[valid], y[valid]
    if len(x) < 2 or np.nanstd(x) == 0 or np.nanstd(y) == 0:
        return np.nan
    if scipy_stats is not None:
        try:
            return float(scipy_stats.spearmanr(x, y).statistic)
        except Exception:
            pass
    return float(pd.Series(x).corr(pd.Series(y), method="spearman"))


def metric_values(frame: pd.DataFrame) -> Dict[str, float]:
    if frame.empty:
        return {
            "n_pairs": 0,
            "n_clusters": 0,
            "pearson_r": np.nan,
            "spearman_rho": np.nan,
            "bias": np.nan,
            "mae": np.nan,
            "rmse": np.nan,
            "median_ratio": np.nan,
            "median_log10_ratio": np.nan,
            "factor_2_fraction": np.nan,
            "factor_10_fraction": np.nan,
            "n_positive_pairs": 0,
        }
    x = pd.to_numeric(frame["higher_matrix_value"], errors="coerce").to_numpy(dtype=float)
    y = pd.to_numeric(frame["lower_aggregated_value"], errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(x) & np.isfinite(y)
    x, y = x[valid], y[valid]
    if len(x) == 0:
        return metric_values(pd.DataFrame())
    diff = y - x
    positive = (x > 0) & (y > 0)
    ratios = y[positive] / x[positive]
    log_ratios = np.log10(ratios) if len(ratios) else np.array([], dtype=float)
    return {
        "n_pairs": int(len(x)),
        "n_clusters": int(frame.loc[valid, "station_uid"].nunique()),
        "pearson_r": safe_pearson(x, y),
        "spearman_rho": safe_spearman(x, y),
        "bias": float(np.mean(diff)),
        "mae": float(np.mean(np.abs(diff))),
        "rmse": float(np.sqrt(np.mean(diff**2))),
        "median_ratio": float(np.median(ratios)) if len(ratios) else np.nan,
        "median_log10_ratio": float(np.median(log_ratios)) if len(log_ratios) else np.nan,
        "factor_2_fraction": (
            float(np.mean((ratios >= 0.5) & (ratios <= 2.0))) if len(ratios) else np.nan
        ),
        "factor_10_fraction": (
            float(np.mean((ratios >= 0.1) & (ratios <= 10.0))) if len(ratios) else np.nan
        ),
        "n_positive_pairs": int(len(ratios)),
    }


def summary_group_frames(pair_values: pd.DataFrame):
    base_cols = ["comparison", "variable", "flag_mode", "aggregation_method"]
    for key, group in pair_values.groupby(base_cols, dropna=False, sort=True):
        key_dict = dict(zip(base_cols, key if isinstance(key, tuple) else (key,)))
        yield "overall", key_dict, group
        for provenance, sub in group.groupby("provenance_class", dropna=False, sort=True):
            yield "provenance", {**key_dict, "provenance_class": provenance}, sub
        for support, sub in group.groupby("lower_support_class", dropna=False, sort=True):
            yield "support", {**key_dict, "lower_support_class": support}, sub
        for (provenance, support), sub in group.groupby(
            ["provenance_class", "lower_support_class"], dropna=False, sort=True
        ):
            yield (
                "provenance_support",
                {
                    **key_dict,
                    "provenance_class": provenance,
                    "lower_support_class": support,
                },
                sub,
            )


def build_summary_metrics(pair_values: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if pair_values.empty:
        return pd.DataFrame()
    for scope, keys, group in summary_group_frames(pair_values):
        rows.append(
            {
                "scope": scope,
                **keys,
                "provenance_class": keys.get("provenance_class", "all"),
                "lower_support_class": keys.get("lower_support_class", "all"),
                **metric_values(group),
            }
        )
    return pd.DataFrame(rows)


def cluster_balanced_frame(group: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for station_uid, sub in group.groupby("station_uid", sort=True):
        lower = pd.to_numeric(sub["lower_aggregated_value"], errors="coerce")
        higher = pd.to_numeric(sub["higher_matrix_value"], errors="coerce")
        valid = lower.notna() & higher.notna()
        if not valid.any():
            continue
        rows.append(
            {
                "station_uid": station_uid,
                "lower_aggregated_value": float(lower[valid].median()),
                "higher_matrix_value": float(higher[valid].median()),
                "n_period_pairs": int(valid.sum()),
                "lower_support_fraction": float(
                    pd.to_numeric(sub.loc[valid, "lower_support_fraction"], errors="coerce").median()
                ),
            }
        )
    return pd.DataFrame(rows)


def bootstrap_cluster_metrics(
    cluster_frame: pd.DataFrame,
    reps: int,
    rng: np.random.Generator,
) -> Dict[str, float]:
    metric_names = ("bias", "mae", "rmse", "spearman_rho", "median_log10_ratio")
    output = {f"{name}_ci_low": np.nan for name in metric_names}
    output.update({f"{name}_ci_high": np.nan for name in metric_names})
    if reps <= 0 or len(cluster_frame) < 2:
        return output
    samples: Dict[str, List[float]] = {name: [] for name in metric_names}
    n = len(cluster_frame)
    for _ in range(reps):
        indices = rng.integers(0, n, size=n)
        sampled = cluster_frame.iloc[indices].copy()
        sampled["station_uid"] = [f"boot_{i}" for i in range(len(sampled))]
        metrics = metric_values(sampled)
        for name in metric_names:
            value = metrics.get(name, np.nan)
            if np.isfinite(value):
                samples[name].append(float(value))
    for name, values in samples.items():
        if values:
            output[f"{name}_ci_low"] = float(np.quantile(values, 0.025))
            output[f"{name}_ci_high"] = float(np.quantile(values, 0.975))
    return output


def build_cluster_balanced_metrics(
    pair_values: pd.DataFrame,
    bootstrap_reps: int,
    random_seed: int,
) -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(random_seed)
    if pair_values.empty:
        return pd.DataFrame()
    for scope, keys, group in summary_group_frames(pair_values):
        cluster_frame = cluster_balanced_frame(group)
        metrics = metric_values(cluster_frame) if not cluster_frame.empty else metric_values(pd.DataFrame())
        rows.append(
            {
                "scope": scope,
                **keys,
                "provenance_class": keys.get("provenance_class", "all"),
                "lower_support_class": keys.get("lower_support_class", "all"),
                "n_original_period_pairs": int(len(group)),
                **metrics,
                **bootstrap_cluster_metrics(cluster_frame, bootstrap_reps, rng),
            }
        )
    return pd.DataFrame(rows)


def build_source_pair_summary(pair_values: pd.DataFrame) -> pd.DataFrame:
    if pair_values.empty:
        return pd.DataFrame()
    group_cols = [
        "comparison",
        "variable",
        "flag_mode",
        "aggregation_method",
        "lower_source_names",
        "higher_source_names",
        "provenance_class",
    ]
    rows = []
    for keys, group in pair_values.groupby(group_cols, dropna=False, sort=True):
        key_dict = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,)))
        rows.append({**key_dict, **metric_values(group)})
    return pd.DataFrame(rows)


def primary_method(comparison: str) -> str:
    if comparison == "monthly_annual":
        return "unweighted_monthly_mean"
    return "arithmetic_mean"


def finite_text(value: float, digits: int = 3) -> str:
    return f"{value:.{digits}f}" if np.isfinite(value) else "NA"


def downsample(frame: pd.DataFrame, max_points: int, seed: int) -> pd.DataFrame:
    if len(frame) <= max_points:
        return frame
    return frame.sample(n=max_points, random_state=seed)


def plot_cross_resolution_scatter(
    pair_values: pd.DataFrame,
    out_png: Path,
    out_pdf: Path,
    flag_mode: str,
    max_points: int,
    random_seed: int,
) -> None:
    comparisons = [spec[0] for spec in COMPARISON_SPECS]
    labels = {
        "daily_monthly": "Daily → monthly",
        "daily_annual": "Daily → annual",
        "monthly_annual": "Monthly → annual",
    }
    units = {"Q": "m³ s⁻¹", "SSC": "mg L⁻¹", "SSL": "t d⁻¹"}
    fig, axes = plt.subplots(3, 3, figsize=(13.2, 12.2), constrained_layout=True)
    legend_handles = None

    for row_idx, variable in enumerate(VARIABLES):
        for col_idx, comparison in enumerate(comparisons):
            ax = axes[row_idx, col_idx]
            subset = pair_values[
                (pair_values["comparison"] == comparison)
                & (pair_values["variable"] == variable)
                & (pair_values["flag_mode"] == flag_mode)
                & (pair_values["aggregation_method"] == primary_method(comparison))
            ].copy()
            subset = subset[
                (pd.to_numeric(subset["higher_matrix_value"], errors="coerce") > 0)
                & (pd.to_numeric(subset["lower_aggregated_value"], errors="coerce") > 0)
            ]
            if subset.empty:
                ax.text(0.5, 0.5, "No positive paired values", ha="center", va="center")
                ax.set_axis_off()
                continue

            plotted = downsample(subset, max_points=max_points, seed=random_seed + row_idx * 10 + col_idx)
            independent = plotted["provenance_class"].eq("different_source_dataset")
            h1 = ax.scatter(
                plotted.loc[~independent, "higher_matrix_value"],
                plotted.loc[~independent, "lower_aggregated_value"],
                s=9,
                alpha=0.24,
                linewidths=0,
                label="Same/mixed source",
            )
            h2 = ax.scatter(
                plotted.loc[independent, "higher_matrix_value"],
                plotted.loc[independent, "lower_aggregated_value"],
                s=16,
                alpha=0.72,
                linewidths=0,
                label="Different source datasets",
            )
            legend_handles = (h1, h2)

            x = pd.to_numeric(subset["higher_matrix_value"], errors="coerce").to_numpy(float)
            y = pd.to_numeric(subset["lower_aggregated_value"], errors="coerce").to_numpy(float)
            combined = np.concatenate([x[np.isfinite(x)], y[np.isfinite(y)]])
            low = 10 ** math.floor(math.log10(np.nanmin(combined)))
            high = 10 ** math.ceil(math.log10(np.nanmax(combined)))
            if low == high:
                low /= 10.0
                high *= 10.0
            line = np.geomspace(low, high, 200)
            ax.plot(line, line, linestyle="--", linewidth=1.0, label="1:1")
            ax.plot(line, line * 2.0, linestyle=":", linewidth=0.8)
            ax.plot(line, line / 2.0, linestyle=":", linewidth=0.8)
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.set_xlim(low, high)
            ax.set_ylim(low, high)
            ax.grid(True, which="both", linewidth=0.35, alpha=0.35)

            metrics = metric_values(subset)
            n_independent = int(
                subset["provenance_class"].eq("different_source_dataset").sum()
            )
            annotation = (
                f"n={metrics['n_pairs']:,}; clusters={metrics['n_clusters']:,}\n"
                f"different-source n={n_independent:,}\n"
                f"ρ={finite_text(metrics['spearman_rho'])}; "
                f"median ratio={finite_text(metrics['median_ratio'], 2)}"
            )
            ax.text(
                0.03,
                0.97,
                annotation,
                transform=ax.transAxes,
                va="top",
                ha="left",
                fontsize=8.5,
                bbox={"boxstyle": "round,pad=0.25", "facecolor": "white", "alpha": 0.78, "edgecolor": "none"},
            )
            if row_idx == 0:
                ax.set_title(labels[comparison])
            if row_idx == 2:
                ax.set_xlabel(f"Coarser matrix {variable} ({units[variable]})")
            if col_idx == 0:
                ax.set_ylabel(f"Aggregated finer matrix {variable} ({units[variable]})")
            ax.text(
                0.01,
                1.02,
                f"({chr(97 + row_idx * 3 + col_idx)})",
                transform=ax.transAxes,
                fontweight="bold",
            )

    if legend_handles is not None:
        fig.legend(
            legend_handles,
            ["Same/mixed source", "Different source datasets"],
            loc="upper center",
            ncol=2,
            frameon=False,
        )
    fig.suptitle(
        f"Cross-resolution consistency of main station-reference matrices ({flag_mode})",
        y=1.015,
        fontsize=14,
    )
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    fig.savefig(out_pdf, bbox_inches="tight")
    plt.close(fig)


def plot_support_sensitivity(
    pair_values: pd.DataFrame,
    out_png: Path,
    flag_mode: str,
    max_points: int,
    random_seed: int,
) -> None:
    comparisons = [spec[0] for spec in COMPARISON_SPECS]
    labels = {
        "daily_monthly": "Daily → monthly",
        "daily_annual": "Daily → annual",
        "monthly_annual": "Monthly → annual",
    }
    fig, axes = plt.subplots(3, 3, figsize=(13.2, 11.5), constrained_layout=True)
    for row_idx, variable in enumerate(VARIABLES):
        for col_idx, comparison in enumerate(comparisons):
            ax = axes[row_idx, col_idx]
            subset = pair_values[
                (pair_values["comparison"] == comparison)
                & (pair_values["variable"] == variable)
                & (pair_values["flag_mode"] == flag_mode)
                & (pair_values["aggregation_method"] == primary_method(comparison))
            ].copy()
            support = pd.to_numeric(subset["lower_support_fraction"], errors="coerce")
            log_ratio = pd.to_numeric(
                subset["log10_ratio_lower_over_higher"], errors="coerce"
            ).abs()
            subset = subset[support.notna() & log_ratio.notna()].copy()
            if subset.empty:
                ax.text(0.5, 0.5, "No log-ratio support pairs", ha="center", va="center")
                ax.set_axis_off()
                continue
            subset["_support"] = pd.to_numeric(
                subset["lower_support_fraction"], errors="coerce"
            )
            subset["_abs_log_ratio"] = pd.to_numeric(
                subset["log10_ratio_lower_over_higher"], errors="coerce"
            ).abs()
            plotted = downsample(subset, max_points, random_seed + 100 + row_idx * 10 + col_idx)
            ax.scatter(plotted["_support"], plotted["_abs_log_ratio"], s=9, alpha=0.25, linewidths=0)

            bins = np.linspace(0.0, 1.0, 11)
            subset["_bin"] = pd.cut(subset["_support"], bins=bins, include_lowest=True)
            medians = subset.groupby("_bin", observed=True)["_abs_log_ratio"].median()
            centers = [interval.mid for interval in medians.index]
            ax.plot(centers, medians.values, marker="o", linewidth=1.2, label="Bin median")
            ax.axhline(math.log10(2.0), linestyle=":", linewidth=0.9)
            ax.set_xlim(-0.02, 1.02)
            ax.grid(True, linewidth=0.35, alpha=0.35)
            if row_idx == 0:
                ax.set_title(labels[comparison])
            if row_idx == 2:
                ax.set_xlabel("Finer-product observation support fraction")
            if col_idx == 0:
                ax.set_ylabel(f"|log₁₀ ratio| ({variable})")
            ax.text(
                0.01,
                1.02,
                f"({chr(97 + row_idx * 3 + col_idx)})",
                transform=ax.transAxes,
                fontweight="bold",
            )
    fig.suptitle(
        f"Cross-resolution disagreement versus observation support ({flag_mode})",
        y=1.015,
        fontsize=14,
    )
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close(fig)


def markdown_table(frame: pd.DataFrame, max_rows: int = 30) -> str:
    if frame is None or frame.empty:
        return "_No rows._"
    display = frame.head(max_rows).copy()
    for column in display.columns:
        if pd.api.types.is_float_dtype(display[column]):
            display[column] = display[column].map(
                lambda value: "" if pd.isna(value) else f"{value:.4g}"
            )
    headers = [str(column) for column in display.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in display.iterrows():
        values = [clean_text(row[column]).replace("|", "\\|") for column in display.columns]
        lines.append("| " + " | ".join(values) + " |")
    if len(frame) > max_rows:
        lines.append(f"\n_Showing {max_rows} of {len(frame)} rows._")
    return "\n".join(lines)


def write_report(
    path: Path,
    args,
    overlap_summary: pd.DataFrame,
    structural: pd.DataFrame,
    funnel: pd.DataFrame,
    summary: pd.DataFrame,
    source_pairs: pd.DataFrame,
) -> None:
    overall = summary[
        (summary.get("scope", "") == "overall")
        & (summary.get("flag_mode", "") == "analysis_ready")
    ].copy() if not summary.empty else pd.DataFrame()
    if not overall.empty:
        keep = [
            "comparison",
            "variable",
            "aggregation_method",
            "n_pairs",
            "n_clusters",
            "spearman_rho",
            "bias",
            "rmse",
            "median_ratio",
            "factor_2_fraction",
        ]
        overall = overall[[column for column in keep if column in overall.columns]]

    structural_summary = pd.DataFrame()
    if not structural.empty:
        structural_summary = (
            structural.groupby("comparison", as_index=False)
            .agg(
                common_clusters=("station_uid", "nunique"),
                structural_pass=("structural_pass", "sum"),
                structural_fail=("structural_pass", lambda values: int((~values.astype(bool)).sum())),
                max_coordinate_difference_m=("coordinate_difference_m", "max"),
                max_basin_area_relative_difference=("basin_area_relative_difference", "max"),
            )
        )

    independent = source_pairs[
        source_pairs.get("provenance_class", "").eq("different_source_dataset")
    ].copy() if not source_pairs.empty else pd.DataFrame()
    if not independent.empty:
        independent = independent.sort_values(["n_pairs", "n_clusters"], ascending=False)
        keep = [
            "comparison",
            "variable",
            "flag_mode",
            "aggregation_method",
            "lower_source_names",
            "higher_source_names",
            "n_pairs",
            "n_clusters",
            "spearman_rho",
            "median_ratio",
        ]
        independent = independent[[column for column in keep if column in independent.columns]]

    lines = [
        "# Cross-resolution consistency assessment",
        "",
        "## Interpretation",
        "",
        "This assessment compares finer-resolution matrix values, aggregated to calendar months or years, with values in the coarser main station-reference matrices at common `station_uid` values and periods. Comparisons involving the same source station assess processing, temporal classification, aggregation, and export consistency; they are not independent observational validation. Comparisons between different source datasets provide stronger cross-source evidence but remain conditional on common cluster assignment and temporal support.",
        "",
        "## Configuration",
        "",
        f"- Release directory: `{args.release_dir}`",
        f"- Flag modes: `{','.join(args.flag_modes)}`",
        f"- Monthly-to-annual weighting: `{args.monthly_weighting}`",
        f"- Support classes: low < {args.moderate_support_fraction:g}; moderate < {args.high_support_fraction:g}; high ≥ {args.high_support_fraction:g}",
        f"- Coordinate tolerance: {args.coordinate_tolerance_m:g} m",
        f"- Basin-area relative tolerance: {args.basin_area_tolerance:g}",
        f"- Cluster bootstrap repetitions: {args.bootstrap_reps}",
        "",
        "## Cross-resolution cluster inventory",
        "",
        markdown_table(overlap_summary),
        "",
        "## Structural consistency",
        "",
        markdown_table(structural_summary),
        "",
        "## Match funnel",
        "",
        markdown_table(funnel, max_rows=60),
        "",
        "## Overall analysis-ready metrics",
        "",
        markdown_table(overall, max_rows=30),
        "",
        "## Different-source dataset pairings",
        "",
        markdown_table(independent, max_rows=30),
        "",
        "## Output interpretation notes",
        "",
        "- Q, SSC, and SSL are compared as arithmetic means unless the monthly-to-annual days-weighted sensitivity is explicitly selected.",
        "- SSL remains in t d-1 and is therefore averaged rather than summed.",
        "- Linear-space bias, MAE, and RMSE include zero values. Ratio, log-ratio, factor-of-2, and log-log plots use positive pairs only.",
        "- Sparse periods are retained and identified by `lower_support_fraction` and `lower_support_class`; users can apply their own support threshold from the detailed pair tables.",
        "- `different_source_dataset` is the most relevant provenance class for cross-source support. `same_source_station` should be interpreted as a processing consistency check.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def ensure_writable_outputs(paths: Sequence[Path], overwrite: bool) -> None:
    existing = [path for path in paths if path.exists()]
    if existing and not overwrite:
        sample = "\n".join(str(path) for path in existing[:10])
        raise FileExistsError(
            "Output files already exist. Use --overwrite to replace them:\n" + sample
        )


def parse_flag_modes(text: str) -> Tuple[str, ...]:
    modes = tuple(clean_text(token).lower() for token in str(text).split(",") if clean_text(token))
    unknown = sorted(set(modes) - set(FLAG_MODES))
    if unknown:
        raise argparse.ArgumentTypeError(
            f"Unknown flag mode(s): {unknown}; choose from {sorted(FLAG_MODES)}"
        )
    if not modes:
        raise argparse.ArgumentTypeError("At least one flag mode is required")
    return modes


def resolve_path(value: str, base: Path = REPO_ROOT) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path.resolve()
    if path.exists():
        return path.resolve()
    return (base / path).resolve()


def parse_args(argv: Optional[Sequence[str]] = None):
    """Return hardcoded configuration. Command-line arguments are ignored."""
    import argparse
    from types import SimpleNamespace

    # --- Hardcoded configuration ---
    release_dir = str(DEFAULT_RELEASE_DIR)
    out_dir = str(DEFAULT_OUT_DIR)
    flag_modes = ('good', 'analysis_ready')
    monthly_weighting = 'both'
    moderate_support_fraction = 0.50
    high_support_fraction = 0.80
    coordinate_tolerance_m = 10.0
    basin_area_tolerance = 0.001
    resolved_only = False
    bootstrap_reps = 500
    random_seed = 42
    max_plot_points = 30000
    skip_plots = False
    overwrite = True
    # --- End hardcoded configuration ---

    args = SimpleNamespace(
        release_dir=resolve_path(release_dir),
        out_dir=resolve_path(out_dir),
        flag_modes=flag_modes,
        monthly_weighting=monthly_weighting,
        moderate_support_fraction=moderate_support_fraction,
        high_support_fraction=high_support_fraction,
        coordinate_tolerance_m=coordinate_tolerance_m,
        basin_area_tolerance=basin_area_tolerance,
        resolved_only=resolved_only,
        bootstrap_reps=bootstrap_reps,
        random_seed=random_seed,
        max_plot_points=max_plot_points,
        skip_plots=skip_plots,
        overwrite=overwrite,
    )

    for attr in ('daily_file', 'monthly_file', 'annual_file'):
        value = Path(MATRIX_FILENAMES[attr.replace('_file', '')]).expanduser()
        setattr(args, attr, value.resolve() if value.is_absolute() else (args.release_dir / value).resolve())

    catalog = Path('source_station_catalog.csv').expanduser()
    args.source_station_catalog = (
        catalog.resolve() if catalog.is_absolute() else (args.release_dir / catalog).resolve()
    )

    return args

def resolved_cluster_filter(lower: MatrixReader, higher: MatrixReader, uid: str) -> bool:
    lower_status = clean_text(lower.metadata(uid).basin_status).lower()
    higher_status = clean_text(higher.metadata(uid).basin_status).lower()
    if not lower_status and not higher_status:
        return True
    return lower_status == "resolved" and higher_status == "resolved"


def matrix_inventory(readers: Mapping[str, MatrixReader]) -> pd.DataFrame:
    sets = {resolution: set(reader.station_uids) for resolution, reader in readers.items()}
    rows = []
    for resolution in ("daily", "monthly", "annual"):
        rows.append(
            {
                "inventory_type": "matrix",
                "comparison": resolution,
                "cluster_count": len(sets[resolution]),
                "time_start": str(readers[resolution].time.min().date()),
                "time_end": str(readers[resolution].time.max().date()),
                "time_steps": len(readers[resolution].time),
            }
        )
    for comparison, lower_resolution, higher_resolution in COMPARISON_SPECS:
        common = sets[lower_resolution].intersection(sets[higher_resolution])
        rows.append(
            {
                "inventory_type": "pairwise_overlap",
                "comparison": comparison,
                "cluster_count": len(common),
                "time_start": "",
                "time_end": "",
                "time_steps": np.nan,
            }
        )
    triple = sets["daily"].intersection(sets["monthly"]).intersection(sets["annual"])
    rows.append(
        {
            "inventory_type": "three_way_overlap",
            "comparison": "daily_monthly_annual",
            "cluster_count": len(triple),
            "time_start": "",
            "time_end": "",
            "time_steps": np.nan,
        }
    )
    return pd.DataFrame(rows)


def run_comparison(
    comparison: str,
    lower: MatrixReader,
    higher: MatrixReader,
    resolver: SourceCatalogResolver,
    args,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict[str, object]]]:
    common_uids = sorted(set(lower.station_uids).intersection(higher.station_uids))
    if args.resolved_only:
        common_uids = [uid for uid in common_uids if resolved_cluster_filter(lower, higher, uid)]
    log(
        f"{comparison}: common clusters={len(common_uids):,} "
        f"({lower.resolution}={len(lower.station_uids):,}, {higher.resolution}={len(higher.station_uids):,})"
    )

    structural = structural_consistency_rows(
        comparison=comparison,
        lower=lower,
        higher=higher,
        common_uids=common_uids,
        coordinate_tolerance_m=args.coordinate_tolerance_m,
        basin_area_tolerance=args.basin_area_tolerance,
    )

    pair_parts = []
    funnel_rows: List[Dict[str, object]] = [
        {
            "stage": "common_clusters",
            "comparison": comparison,
            "variable": "all",
            "flag_mode": "all",
            "count": len(common_uids),
            "notes": "station_uid present in both matrices after optional resolved-only filter",
        }
    ]
    methods = source_aggregation_methods(
        lower.resolution,
        higher.resolution,
        args.monthly_weighting,
    )

    for position, uid in enumerate(common_uids, start=1):
        if position == 1 or position % 50 == 0 or position == len(common_uids):
            log(f"{comparison}: processing cluster {position:,}/{len(common_uids):,}")
        lower_row = lower.read_cluster_rows(uid)
        higher_row = higher.read_cluster_rows(uid)
        for flag_mode in args.flag_modes:
            allowed = FLAG_MODES[flag_mode]
            for variable in VARIABLES:
                higher_aggregate = aggregate_matrix_row(
                    row_data=higher_row,
                    matrix_resolution=higher.resolution,
                    target_resolution=higher.resolution,
                    variable=variable,
                    allowed_flags=allowed,
                    aggregation_method="arithmetic_mean",
                    resolver=resolver,
                    moderate_support_fraction=args.moderate_support_fraction,
                    high_support_fraction=args.high_support_fraction,
                )
                if higher_aggregate.empty:
                    continue
                for method in methods:
                    lower_aggregate = aggregate_matrix_row(
                        row_data=lower_row,
                        matrix_resolution=lower.resolution,
                        target_resolution=higher.resolution,
                        variable=variable,
                        allowed_flags=allowed,
                        aggregation_method=method,
                        resolver=resolver,
                        moderate_support_fraction=args.moderate_support_fraction,
                        high_support_fraction=args.high_support_fraction,
                    )
                    paired = pair_aggregates(
                        comparison=comparison,
                        station_uid=uid,
                        lower_resolution=lower.resolution,
                        higher_resolution=higher.resolution,
                        variable=variable,
                        flag_mode=flag_mode,
                        aggregation_method=method,
                        lower=lower_aggregate,
                        higher=higher_aggregate,
                    )
                    if not paired.empty:
                        pair_parts.append(paired)

    pairs = pd.concat(pair_parts, ignore_index=True) if pair_parts else pd.DataFrame()
    if not pairs.empty:
        for (variable, flag_mode, method), group in pairs.groupby(
            ["variable", "flag_mode", "aggregation_method"], sort=True
        ):
            funnel_rows.extend(
                [
                    {
                        "stage": "paired_periods",
                        "comparison": comparison,
                        "variable": variable,
                        "flag_mode": flag_mode,
                        "count": len(group),
                        "notes": method,
                    },
                    {
                        "stage": "paired_clusters",
                        "comparison": comparison,
                        "variable": variable,
                        "flag_mode": flag_mode,
                        "count": group["station_uid"].nunique(),
                        "notes": method,
                    },
                    {
                        "stage": "different_source_period_pairs",
                        "comparison": comparison,
                        "variable": variable,
                        "flag_mode": flag_mode,
                        "count": int(
                            group["provenance_class"].eq("different_source_dataset").sum()
                        ),
                        "notes": method,
                    },
                    {
                        "stage": "different_source_clusters",
                        "comparison": comparison,
                        "variable": variable,
                        "flag_mode": flag_mode,
                        "count": group.loc[
                            group["provenance_class"].eq("different_source_dataset"),
                            "station_uid",
                        ].nunique(),
                        "notes": method,
                    },
                ]
            )
    return pairs, structural, funnel_rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if nc4 is None:
        raise SystemExit(
            "Error: netCDF4 is required for this validation script. "
            "Install it with `pip install netCDF4`."
        )

    output_paths = [
        args.out_dir / "s14_cross_resolution_funnel.csv",
        args.out_dir / "s14_cluster_overlap_summary.csv",
        args.out_dir / "s14_structural_consistency.csv",
        args.out_dir / "s14_pair_values_daily_monthly.csv",
        args.out_dir / "s14_pair_values_daily_annual.csv",
        args.out_dir / "s14_pair_values_monthly_annual.csv",
        args.out_dir / "s14_all_pair_values.csv",
        args.out_dir / "s14_summary_metrics.csv",
        args.out_dir / "s14_cluster_balanced_metrics.csv",
        args.out_dir / "s14_source_pair_summary.csv",
        args.out_dir / "s14_cross_resolution_scatter.png",
        args.out_dir / "s14_cross_resolution_scatter.pdf",
        args.out_dir / "s14_support_sensitivity.png",
        args.out_dir / "s14_cross_resolution_report.md",
        args.out_dir / "s14_run_config.json",
    ]
    ensure_writable_outputs(output_paths, args.overwrite)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    log(f"Release directory: {args.release_dir}")
    log(f"Output directory:  {args.out_dir}")
    log(f"Flag modes:        {','.join(args.flag_modes)}")
    log(f"Monthly weighting: {args.monthly_weighting}")

    resolver = SourceCatalogResolver(args.source_station_catalog)
    readers: Dict[str, MatrixReader] = {}
    pair_tables: Dict[str, pd.DataFrame] = {}
    structural_parts = []
    funnel_rows = []

    try:
        readers["daily"] = MatrixReader(args.daily_file, "daily")
        readers["monthly"] = MatrixReader(args.monthly_file, "monthly")
        readers["annual"] = MatrixReader(args.annual_file, "annual")
        overlap_summary = matrix_inventory(readers)
        overlap_summary.to_csv(
            args.out_dir / "s14_cluster_overlap_summary.csv", index=False
        )

        for comparison, lower_resolution, higher_resolution in COMPARISON_SPECS:
            pairs, structural, comparison_funnel = run_comparison(
                comparison=comparison,
                lower=readers[lower_resolution],
                higher=readers[higher_resolution],
                resolver=resolver,
                args=args,
            )
            pair_tables[comparison] = pairs
            structural_parts.append(structural)
            funnel_rows.extend(comparison_funnel)
            pair_path = args.out_dir / f"s14_pair_values_{comparison}.csv"
            pairs.to_csv(pair_path, index=False)
            log(f"Wrote {pair_path.name}: {len(pairs):,} rows")

        structural_all = (
            pd.concat(structural_parts, ignore_index=True)
            if structural_parts
            else pd.DataFrame()
        )
        structural_all.to_csv(
            args.out_dir / "s14_structural_consistency.csv", index=False
        )
        funnel = pd.DataFrame(funnel_rows)
        funnel.to_csv(args.out_dir / "s14_cross_resolution_funnel.csv", index=False)

        all_pairs = (
            pd.concat(
                [frame for frame in pair_tables.values() if not frame.empty],
                ignore_index=True,
            )
            if any(not frame.empty for frame in pair_tables.values())
            else pd.DataFrame()
        )
        all_pairs.to_csv(args.out_dir / "s14_all_pair_values.csv", index=False)

        summary = build_summary_metrics(all_pairs)
        summary.to_csv(args.out_dir / "s14_summary_metrics.csv", index=False)
        cluster_summary = build_cluster_balanced_metrics(
            all_pairs,
            bootstrap_reps=args.bootstrap_reps,
            random_seed=args.random_seed,
        )
        cluster_summary.to_csv(
            args.out_dir / "s14_cluster_balanced_metrics.csv", index=False
        )
        source_pairs = build_source_pair_summary(all_pairs)
        source_pairs.to_csv(
            args.out_dir / "s14_source_pair_summary.csv", index=False
        )

        if not args.skip_plots and not all_pairs.empty:
            plot_mode = "analysis_ready" if "analysis_ready" in args.flag_modes else args.flag_modes[0]
            plot_cross_resolution_scatter(
                all_pairs,
                out_png=args.out_dir / "s14_cross_resolution_scatter.png",
                out_pdf=args.out_dir / "s14_cross_resolution_scatter.pdf",
                flag_mode=plot_mode,
                max_points=args.max_plot_points,
                random_seed=args.random_seed,
            )
            plot_support_sensitivity(
                all_pairs,
                out_png=args.out_dir / "s14_support_sensitivity.png",
                flag_mode=plot_mode,
                max_points=args.max_plot_points,
                random_seed=args.random_seed,
            )

        write_report(
            args.out_dir / "s14_cross_resolution_report.md",
            args=args,
            overlap_summary=overlap_summary,
            structural=structural_all,
            funnel=funnel,
            summary=summary,
            source_pairs=source_pairs,
        )

        config = {
            "release_dir": str(args.release_dir),
            "daily_file": str(args.daily_file),
            "monthly_file": str(args.monthly_file),
            "annual_file": str(args.annual_file),
            "source_station_catalog": str(args.source_station_catalog),
            "out_dir": str(args.out_dir),
            "flag_modes": list(args.flag_modes),
            "monthly_weighting": args.monthly_weighting,
            "moderate_support_fraction": args.moderate_support_fraction,
            "high_support_fraction": args.high_support_fraction,
            "coordinate_tolerance_m": args.coordinate_tolerance_m,
            "basin_area_tolerance": args.basin_area_tolerance,
            "resolved_only": args.resolved_only,
            "bootstrap_reps": args.bootstrap_reps,
            "random_seed": args.random_seed,
            "matrix_cluster_counts": {
                resolution: len(reader.station_uids)
                for resolution, reader in readers.items()
            },
            "pair_row_counts": {
                comparison: len(frame) for comparison, frame in pair_tables.items()
            },
        }
        (args.out_dir / "s14_run_config.json").write_text(
            json.dumps(config, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        log("Cross-resolution validation completed")
        log(f"All pair rows: {len(all_pairs):,}")
        log(f"Report: {args.out_dir / 's14_cross_resolution_report.md'}")
        return 0
    finally:
        for reader in readers.values():
            reader.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValidationError, FileNotFoundError, FileExistsError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(2)
