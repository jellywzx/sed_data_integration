#!/usr/bin/env python3
"""Cross-source comparison between main matrices and climatology observations.

The script spatially links climatology stations to released main-matrix clusters,
excludes records selected from the same source dataset, aggregates daily/monthly/
annual main-matrix observations to period-matched long-term means, and compares
SSC and SSL in scatter plots and summary tables.

Scientific interpretation
-------------------------
Climatology timestamps are treated as representative only. Period matching uses
station-level source coverage fields (start/end year) when available. Main-matrix
values are first averaged within each calendar year and then averaged across the
valid overlapping years, so years with dense sampling do not dominate the
long-term mean.

Default inputs under --release-dir:
  sed_reference_timeseries_daily.nc
  sed_reference_timeseries_monthly.nc
  sed_reference_timeseries_annual.nc
  sed_reference_climatology.nc

Default outputs under --out-dir:
  s13_match_funnel.csv
  s13_spatial_candidates.csv
  s13_selected_station_matches.csv
  s13_source_specific_pair_values.csv
  s13_resolution_pair_values.csv
  s13_primary_pair_values.csv
  s13_summary_metrics.csv
  s13_scatter_main_climatology.png
  s13_scatter_main_climatology.pdf
  s13_main_climatology_report.md
"""


import argparse
import math
import re
import sys
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError as exc:  # pragma: no cover - handled at runtime
    try:
        from h5netcdf import legacyapi as nc4
    except ImportError:
        nc4 = None
        _NETCDF_IMPORT_ERROR = exc
    else:
        _NETCDF_IMPORT_ERROR = None
else:
    _NETCDF_IMPORT_ERROR = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DEFAULT_RELEASE_DIR = PROJECT_DIR / "output" / "sed_reference_release"
DEFAULT_OUT_DIR = PROJECT_DIR / "validate" / "output" / "main_climatology"

MATRIX_FILENAMES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
CLIMATOLOGY_FILENAME = "sed_reference_climatology.nc"
VARIABLES = ("SSC", "SSL")
FLAG_VARIABLES = {"SSC": "SSC_flag", "SSL": "SSL_flag"}
RESOLUTION_PRIORITY = {"daily": 1, "monthly": 2, "annual": 3}
EARTH_RADIUS_M = 6_371_008.8
FILL_VALUES = (-9999.0, -9999, 9.96921e36)


# Source aliases are deliberately conservative. Canonicalization is used only
# to identify obvious same-source comparisons that must be excluded.
SOURCE_ALIASES = {
    "usgs": "usgs_nwis",
    "usgsnwis": "usgs_nwis",
    "usgs_nwis": "usgs_nwis",
    "nwis": "usgs_nwis",
    "hydatdataset": "hydat",
    "hydat_dataset": "hydat",
    "gfqa": "gfqa_v2",
    "gfqav2": "gfqa_v2",
    "gfqa_v2": "gfqa_v2",
    "globalflowandwaterqualityarchivev2": "gfqa_v2",
    "global_flow_and_water_quality_archive_v2": "gfqa_v2",
    "eusedcollabdataset": "eusedcollab",
    "eusedcollab_dataset": "eusedcollab",
    "millimanfarnsworth": "milliman",
    "milliman_farnsworth": "milliman",
    "alianddeboer": "ali_de_boer",
    "ali_de_boer_dataset": "ali_de_boer",
    "hmadataset": "hma",
    "hma_dataset": "hma",
    "vanmaerckeetal2014africansedimentyielddatabase": "vanmaercke",
    "vanmaercke_et_al_2014_african_sediment_yield_database": "vanmaercke",
}

GENERIC_NAME_TOKENS = {
    "river",
    "riv",
    "station",
    "gauge",
    "gauging",
    "site",
    "at",
    "near",
    "the",
    "of",
}


class ValidationError(RuntimeError):
    """Raised for a release structure that cannot support the comparison."""


def log(message: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
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
    return "" if text.lower() in {"", "nan", "none", "nat", "null", "n/a", "na"} else text


def normalize_name(value, remove_generic: bool = False) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    if remove_generic:
        tokens = [token for token in tokens if token not in GENERIC_NAME_TOKENS]
    return " ".join(tokens)


def canonical_source(value) -> str:
    key = normalize_name(value).replace(" ", "_")
    compact = key.replace("_", "")
    return SOURCE_ALIASES.get(key, SOURCE_ALIASES.get(compact, key))


def name_similarity(left, right, remove_generic: bool = False) -> float:
    a = normalize_name(left, remove_generic=remove_generic)
    b = normalize_name(right, remove_generic=remove_generic)
    if not a or not b:
        return np.nan
    if a == b:
        return 1.0
    return float(SequenceMatcher(None, a, b).ratio())


def parse_flag_list(text: str) -> Tuple[int, ...]:
    values: List[int] = []
    for token in str(text).split(","):
        token = token.strip()
        if token:
            values.append(int(token))
    if not values:
        raise argparse.ArgumentTypeError("At least one quality flag is required")
    return tuple(sorted(set(values)))


def resolve_path(value: Optional[str], default: Path) -> Path:
    path = Path(value).expanduser() if value else default
    return path.resolve()


def require_netcdf() -> None:
    if nc4 is None:
        raise RuntimeError(
            "netCDF4 is required for this script. Install it with 'pip install netCDF4'. "
            f"Original import error: {_NETCDF_IMPORT_ERROR}"
        )


def _fill_candidates(var) -> List[float]:
    fills: List[float] = list(FILL_VALUES)
    for attr in ("_FillValue", "missing_value"):
        try:
            raw = getattr(var, attr)
        except Exception:
            continue
        try:
            fills.extend(np.asarray(raw).astype(float).reshape(-1).tolist())
        except Exception:
            pass
    return fills


def read_numeric(var, key=slice(None)) -> np.ndarray:
    raw = np.ma.asarray(var[key])
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(np.nan)
    arr = np.asarray(raw, dtype=np.float64)
    for fill in _fill_candidates(var):
        if np.isfinite(fill):
            arr[arr == fill] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    return arr


def read_int(var, key=slice(None), fill_value: int = -1) -> np.ndarray:
    raw = np.ma.asarray(var[key])
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(fill_value)
    arr = np.asarray(raw)
    out = np.full(arr.shape, fill_value, dtype=np.int64)
    try:
        numeric = arr.astype(np.float64)
        valid = np.isfinite(numeric)
        out[valid] = numeric[valid].astype(np.int64)
    except Exception:
        pass
    return out


def read_text(var, key=slice(None)) -> List[str]:
    raw = np.ma.asarray(var[key])
    if np.ma.isMaskedArray(raw):
        if raw.dtype.kind in {"S", "U", "O"}:
            raw = raw.filled("")
        else:
            raw = raw.filled(np.nan)
    arr = np.asarray(raw)
    if arr.ndim == 0:
        return [clean_text(arr.item())]
    if arr.dtype.kind in {"S", "U"} and arr.ndim >= 2:
        rows = arr.reshape((-1, arr.shape[-1]))
        return [clean_text("".join(clean_text(item) for item in row)) for row in rows]
    return [clean_text(item) for item in arr.reshape(-1)]


def variable_or_none(ds, names: Sequence[str]):
    for name in names:
        if name in ds.variables:
            return ds.variables[name]
    return None


def dimension_size(ds, preferred: Sequence[str], fallback_var: Optional[str] = None) -> int:
    for name in preferred:
        if name in ds.dimensions:
            return len(ds.dimensions[name])
    if fallback_var and fallback_var in ds.variables:
        return int(np.asarray(ds.variables[fallback_var][:]).shape[0])
    raise ValidationError(f"Cannot determine dimension size from {preferred}")


def pad_list(values: Sequence, size: int, fill="") -> List:
    values = list(values)
    if len(values) >= size:
        return values[:size]
    return values + [fill] * (size - len(values))


def pad_array(values: np.ndarray, size: int, fill=np.nan, dtype=np.float64) -> np.ndarray:
    arr = np.asarray(values, dtype=dtype).reshape(-1)
    if len(arr) >= size:
        return arr[:size]
    return np.concatenate([arr, np.full(size - len(arr), fill, dtype=dtype)])


def decode_time_values(ds) -> Tuple[np.ndarray, List[str]]:
    if "time" not in ds.variables:
        raise ValidationError("NetCDF file does not contain a time variable")
    var = ds.variables["time"]
    values = read_numeric(var).reshape(-1)
    units = clean_text(getattr(var, "units", "days since 1970-01-01")) or "days since 1970-01-01"
    calendar = clean_text(getattr(var, "calendar", "standard")) or "standard"
    years = np.full(len(values), -1, dtype=np.int32)
    labels = [""] * len(values)

    valid_idx = np.where(np.isfinite(values))[0]
    if len(valid_idx) == 0:
        return years, labels

    try:
        dates = nc4.num2date(
            values[valid_idx],
            units=units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=False,
        )
        for idx, date in zip(valid_idx, np.asarray(dates).reshape(-1)):
            year = int(getattr(date, "year"))
            years[idx] = year
            try:
                labels[idx] = date.strftime("%Y-%m-%d")
            except Exception:
                labels[idx] = str(date)
        return years, labels
    except Exception:
        pass

    match = re.search(r"days\s+since\s+(\d{4}-\d{2}-\d{2})", units, flags=re.I)
    origin = pd.Timestamp(match.group(1) if match else "1970-01-01")
    parsed = origin + pd.to_timedelta(values[valid_idx], unit="D")
    years[valid_idx] = parsed.year.to_numpy(dtype=np.int32)
    for idx, date in zip(valid_idx, parsed):
        labels[idx] = date.strftime("%Y-%m-%d")
    return years, labels


def extract_years(value) -> List[int]:
    text = clean_text(value)
    years = []
    for token in re.findall(r"(?<!\d)(18\d{2}|19\d{2}|20\d{2}|21\d{2})(?!\d)", text):
        year = int(token)
        if 1800 <= year <= 2199:
            years.append(year)
    return years


def parse_coverage_years(start, end, temporal_span="") -> Tuple[Optional[int], Optional[int], str]:
    start_years = extract_years(start)
    end_years = extract_years(end)
    span_years = extract_years(temporal_span)

    start_year = start_years[0] if start_years else (min(span_years) if span_years else None)
    end_year = end_years[-1] if end_years else (max(span_years) if span_years else None)

    if start_year is not None and end_year is not None:
        if end_year < start_year:
            start_year, end_year = end_year, start_year
        return start_year, end_year, "known"
    if start_year is not None:
        return start_year, start_year, "partial_single_year"
    if end_year is not None:
        return end_year, end_year, "partial_single_year"
    return None, None, "unknown"


def load_main_station_catalog(matrix_paths: Mapping[str, Path]) -> pd.DataFrame:
    catalog: Dict[str, Dict] = {}

    for resolution, path in matrix_paths.items():
        if not path.is_file():
            log(f"Matrix not found, skipping {resolution}: {path}")
            continue
        log(f"Reading {resolution} station metadata: {path}")
        with nc4.Dataset(path, "r") as ds:
            n_stations = dimension_size(ds, ("n_stations", "station"), fallback_var="lat")
            uid_var = variable_or_none(ds, ("station_uid",))
            if uid_var is not None:
                uids = pad_list(read_text(uid_var), n_stations)
            elif "station_reference_id" in ds.variables:
                ids = read_int(ds.variables["station_reference_id"], fill_value=-1).reshape(-1)
                uids = [f"SED{int(value):06d}" if value >= 0 else "" for value in ids]
            else:
                raise ValidationError(f"{path} contains neither station_uid nor station_reference_id")

            lat = pad_array(read_numeric(ds.variables["lat"]), n_stations)
            lon = pad_array(read_numeric(ds.variables["lon"]), n_stations)
            station_name = pad_list(read_text(ds.variables["station_name"]), n_stations) if "station_name" in ds.variables else [""] * n_stations
            river_name = pad_list(read_text(ds.variables["river_name"]), n_stations) if "river_name" in ds.variables else [""] * n_stations
            basin_status = pad_list(read_text(ds.variables["basin_status"]), n_stations, "unknown") if "basin_status" in ds.variables else ["unknown"] * n_stations

            for idx in range(n_stations):
                uid = clean_text(uids[idx])
                if not uid:
                    uid = f"{resolution.upper()}_ROW_{idx:06d}"
                row = catalog.setdefault(
                    uid,
                    {
                        "main_station_uid": uid,
                        "main_lat": np.nan,
                        "main_lon": np.nan,
                        "main_station_name": "",
                        "main_river_name": "",
                        "main_basin_status": "unknown",
                        "available_resolutions": [],
                        "daily_index": np.nan,
                        "monthly_index": np.nan,
                        "annual_index": np.nan,
                    },
                )
                if not np.isfinite(row["main_lat"]) and np.isfinite(lat[idx]):
                    row["main_lat"] = float(lat[idx])
                if not np.isfinite(row["main_lon"]) and np.isfinite(lon[idx]):
                    row["main_lon"] = float(lon[idx])
                if not row["main_station_name"] and station_name[idx]:
                    row["main_station_name"] = station_name[idx]
                if not row["main_river_name"] and river_name[idx]:
                    row["main_river_name"] = river_name[idx]
                status = clean_text(basin_status[idx]) or "unknown"
                if row["main_basin_status"] == "unknown" or status == "resolved":
                    row["main_basin_status"] = status
                row["available_resolutions"].append(resolution)
                row[f"{resolution}_index"] = int(idx)

    if not catalog:
        raise ValidationError("No main matrix station metadata could be loaded")

    rows = list(catalog.values())
    for row in rows:
        row["available_resolutions"] = "|".join(sorted(set(row["available_resolutions"])))
    result = pd.DataFrame(rows)
    result["main_station_name_norm"] = result["main_station_name"].map(lambda x: normalize_name(x, True))
    result["main_river_name_norm"] = result["main_river_name"].map(lambda x: normalize_name(x, True))
    return result


def _station_field(ds, name: str, n_stations: int, default="") -> List:
    if name not in ds.variables:
        return [default] * n_stations
    return pad_list(read_text(ds.variables[name]), n_stations, default)


def load_climatology_records(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not path.is_file():
        raise FileNotFoundError(f"Climatology NetCDF not found: {path}")
    log(f"Reading climatology observations: {path}")

    with nc4.Dataset(path, "r") as ds:
        n_stations = dimension_size(ds, ("n_stations", "station"), fallback_var="lat")
        n_records = dimension_size(ds, ("n_records", "record"), fallback_var="time")

        station_index = read_int(ds.variables["station_index"], fill_value=-1).reshape(-1) if "station_index" in ds.variables else np.arange(n_records, dtype=np.int64)
        station_index = pad_array(station_index, n_records, fill=-1, dtype=np.int64)

        source_index = read_int(ds.variables["source_index"], fill_value=-1).reshape(-1) if "source_index" in ds.variables else np.full(n_stations, -1, dtype=np.int64)
        source_index = pad_array(source_index, n_stations, fill=-1, dtype=np.int64)

        source_names = read_text(ds.variables["source_name"]) if "source_name" in ds.variables else []
        record_sources = read_text(ds.variables["source"]) if "source" in ds.variables else []
        record_sources = pad_list(record_sources, n_records)

        station_uid = _station_field(ds, "station_uid", n_stations)
        station_name = _station_field(ds, "station_name", n_stations)
        river_name = _station_field(ds, "river_name", n_stations)
        source_station_id = _station_field(ds, "source_station_id", n_stations)
        coverage_start = _station_field(ds, "source_station_time_coverage_start", n_stations)
        coverage_end = _station_field(ds, "source_station_time_coverage_end", n_stations)
        temporal_span = _station_field(ds, "temporal_span", n_stations)
        lat = pad_array(read_numeric(ds.variables["lat"]), n_stations)
        lon = pad_array(read_numeric(ds.variables["lon"]), n_stations)

        years, time_labels = decode_time_values(ds)
        years = pad_array(years, n_records, fill=-1, dtype=np.int32)
        time_labels = pad_list(time_labels, n_records)

        values: Dict[str, np.ndarray] = {}
        flags: Dict[str, np.ndarray] = {}
        for variable in VARIABLES:
            values[variable] = pad_array(read_numeric(ds.variables[variable]), n_records) if variable in ds.variables else np.full(n_records, np.nan)
            flag_name = FLAG_VARIABLES[variable]
            flags[variable] = pad_array(
                read_int(ds.variables[flag_name], fill_value=9),
                n_records,
                fill=9,
                dtype=np.int64,
            ) if flag_name in ds.variables else np.full(n_records, 9, dtype=np.int64)

    station_rows: List[Dict] = []
    for idx in range(n_stations):
        src_idx = int(source_index[idx]) if idx < len(source_index) else -1
        source = source_names[src_idx] if 0 <= src_idx < len(source_names) else ""
        uid = clean_text(station_uid[idx]) or f"CLM{idx:06d}"
        start_year, end_year, coverage_status = parse_coverage_years(
            coverage_start[idx], coverage_end[idx], temporal_span[idx]
        )
        station_rows.append(
            {
                "climatology_station_index": idx,
                "climatology_station_uid": uid,
                "climatology_source": clean_text(source),
                "climatology_source_canonical": canonical_source(source),
                "climatology_station_name": station_name[idx],
                "climatology_river_name": river_name[idx],
                "climatology_source_station_id": source_station_id[idx],
                "climatology_lat": float(lat[idx]) if np.isfinite(lat[idx]) else np.nan,
                "climatology_lon": float(lon[idx]) if np.isfinite(lon[idx]) else np.nan,
                "climatology_coverage_start_text": coverage_start[idx],
                "climatology_coverage_end_text": coverage_end[idx],
                "climatology_temporal_span": temporal_span[idx],
                "coverage_start_year": start_year,
                "coverage_end_year": end_year,
                "coverage_status": coverage_status,
            }
        )
    stations = pd.DataFrame(station_rows)

    record_rows: List[Dict] = []
    for record_idx in range(n_records):
        station_idx = int(station_index[record_idx])
        if station_idx < 0 or station_idx >= n_stations:
            continue
        station = station_rows[station_idx].copy()
        if not station["climatology_source"] and record_sources[record_idx]:
            station["climatology_source"] = record_sources[record_idx]
            station["climatology_source_canonical"] = canonical_source(record_sources[record_idx])
        if not any(np.isfinite(values[var][record_idx]) for var in VARIABLES):
            continue
        station.update(
            {
                "climatology_record_index": record_idx,
                "climatology_record_uid": f"{station['climatology_station_uid']}::R{record_idx:06d}",
                "climatology_representative_year": int(years[record_idx]) if years[record_idx] >= 0 else np.nan,
                "climatology_representative_time": time_labels[record_idx],
                "SSC": values["SSC"][record_idx],
                "SSC_flag": int(flags["SSC"][record_idx]),
                "SSL": values["SSL"][record_idx],
                "SSL_flag": int(flags["SSL"][record_idx]),
            }
        )
        record_rows.append(station)

    records = pd.DataFrame(record_rows)
    if records.empty:
        raise ValidationError("Climatology file contains no SSC or SSL records")
    return stations, records


def haversine_distances_m(lat: float, lon: float, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    lat1 = math.radians(float(lat))
    lon1 = math.radians(float(lon))
    lat2 = np.radians(lats.astype(np.float64))
    lon2 = np.radians(lons.astype(np.float64))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)
    return 2.0 * EARTH_RADIUS_M * np.arcsin(np.sqrt(a))


def build_spatial_candidates(
    climatology_stations: pd.DataFrame,
    main_stations: pd.DataFrame,
    exact_distance_m: float,
    max_distance_m: float,
    sensitivity_distance_m: float,
    name_similarity_threshold: float,
) -> pd.DataFrame:
    main_valid = main_stations[
        np.isfinite(main_stations["main_lat"]) & np.isfinite(main_stations["main_lon"])
    ].copy()
    if main_valid.empty:
        raise ValidationError("No main-matrix clusters have valid coordinates")

    main_lats = main_valid["main_lat"].to_numpy(dtype=np.float64)
    main_lons = main_valid["main_lon"].to_numpy(dtype=np.float64)
    rows: List[Dict] = []

    valid_clim = climatology_stations[
        np.isfinite(climatology_stations["climatology_lat"])
        & np.isfinite(climatology_stations["climatology_lon"])
    ]
    for counter, (_, station) in enumerate(valid_clim.iterrows(), start=1):
        if counter % 250 == 0:
            log(f"Spatial matching progress: {counter}/{len(valid_clim)} climatology stations")
        distances = haversine_distances_m(
            station["climatology_lat"],
            station["climatology_lon"],
            main_lats,
            main_lons,
        )
        candidate_positions = np.where(distances <= sensitivity_distance_m)[0]
        for pos in candidate_positions:
            main = main_valid.iloc[int(pos)]
            distance = float(distances[pos])
            station_sim = name_similarity(
                station["climatology_station_name"], main["main_station_name"], remove_generic=True
            )
            river_sim = name_similarity(
                station["climatology_river_name"], main["main_river_name"], remove_generic=True
            )
            station_support = bool(np.isfinite(station_sim) and station_sim >= name_similarity_threshold)
            river_support = bool(np.isfinite(river_sim) and river_sim >= name_similarity_threshold)
            if distance <= exact_distance_m:
                match_class = "coordinate_exact"
            elif distance <= max_distance_m:
                match_class = "coordinate_near"
            else:
                match_class = "sensitivity_near"
            rows.append(
                {
                    "climatology_station_uid": station["climatology_station_uid"],
                    "climatology_source": station["climatology_source"],
                    "climatology_station_name": station["climatology_station_name"],
                    "climatology_river_name": station["climatology_river_name"],
                    "climatology_lat": station["climatology_lat"],
                    "climatology_lon": station["climatology_lon"],
                    "main_station_uid": main["main_station_uid"],
                    "main_station_name": main["main_station_name"],
                    "main_river_name": main["main_river_name"],
                    "main_lat": main["main_lat"],
                    "main_lon": main["main_lon"],
                    "main_basin_status": main["main_basin_status"],
                    "available_resolutions": main["available_resolutions"],
                    "daily_index": main["daily_index"],
                    "monthly_index": main["monthly_index"],
                    "annual_index": main["annual_index"],
                    "distance_m": distance,
                    "spatial_match_class": match_class,
                    "station_name_similarity": station_sim,
                    "river_name_similarity": river_sim,
                    "station_name_support": station_support,
                    "river_name_support": river_support,
                    "name_support_count": int(station_support) + int(river_support),
                }
            )

    return pd.DataFrame(rows)


def select_spatial_matches(
    climatology_stations: pd.DataFrame,
    candidates: pd.DataFrame,
    exact_distance_m: float,
    max_distance_m: float,
    ambiguity_distance_gap_m: float,
    ambiguity_distance_ratio: float,
    require_name_support_for_near: bool,
    resolved_main_only: bool,
) -> pd.DataFrame:
    candidate_groups = {
        uid: group.copy()
        for uid, group in candidates.groupby("climatology_station_uid", sort=False)
    } if not candidates.empty else {}
    rows: List[Dict] = []

    for _, station in climatology_stations.iterrows():
        uid = station["climatology_station_uid"]
        group = candidate_groups.get(uid, pd.DataFrame()).copy()
        if not group.empty:
            group = group[group["distance_m"] <= max_distance_m].copy()
            if resolved_main_only:
                group = group[group["main_basin_status"].astype(str).str.lower().eq("resolved")]

        base = station.to_dict()
        base.update(
            {
                "match_status": "no_primary_candidate",
                "n_primary_candidates": 0,
                "selected_main_station_uid": "",
                "selected_distance_m": np.nan,
                "selected_spatial_match_class": "",
                "selected_main_station_name": "",
                "selected_main_river_name": "",
                "selected_main_basin_status": "",
                "selected_available_resolutions": "",
                "selected_daily_index": np.nan,
                "selected_monthly_index": np.nan,
                "selected_annual_index": np.nan,
                "selected_station_name_similarity": np.nan,
                "selected_river_name_similarity": np.nan,
                "selection_reason": "no candidate within primary distance threshold",
            }
        )
        if group.empty:
            rows.append(base)
            continue

        group["_exact_rank"] = (group["distance_m"] <= exact_distance_m).astype(int)
        group = group.sort_values(
            ["_exact_rank", "name_support_count", "distance_m", "main_station_uid"],
            ascending=[False, False, True, True],
        ).reset_index(drop=True)
        base["n_primary_candidates"] = int(len(group))
        top = group.iloc[0]

        selected = False
        reason = ""
        if len(group) == 1:
            selected = True
            reason = "only primary-distance candidate"
        else:
            second = group.iloc[1]
            top_exact = float(top["distance_m"]) <= exact_distance_m
            second_exact = float(second["distance_m"]) <= exact_distance_m
            if top_exact and not second_exact:
                selected = True
                reason = "only exact-distance candidate"
            elif int(top["name_support_count"]) > int(second["name_support_count"]):
                selected = True
                reason = "stronger station/river name support"
            else:
                gap = float(second["distance_m"]) - float(top["distance_m"])
                ratio = float(second["distance_m"]) / max(float(top["distance_m"]), 1.0)
                if gap >= ambiguity_distance_gap_m or ratio >= ambiguity_distance_ratio:
                    selected = True
                    reason = "nearest candidate clearly separated from second candidate"
                else:
                    reason = "top candidates are not clearly distinguishable"

        if selected and require_name_support_for_near:
            if float(top["distance_m"]) > exact_distance_m and int(top["name_support_count"]) == 0:
                selected = False
                reason = "near match lacks station or river name support"
                base["match_status"] = "near_without_name_support"

        if selected:
            base.update(
                {
                    "match_status": "selected",
                    "selected_main_station_uid": top["main_station_uid"],
                    "selected_distance_m": float(top["distance_m"]),
                    "selected_spatial_match_class": top["spatial_match_class"],
                    "selected_main_station_name": top["main_station_name"],
                    "selected_main_river_name": top["main_river_name"],
                    "selected_main_basin_status": top["main_basin_status"],
                    "selected_available_resolutions": top["available_resolutions"],
                    "selected_daily_index": top["daily_index"],
                    "selected_monthly_index": top["monthly_index"],
                    "selected_annual_index": top["annual_index"],
                    "selected_station_name_similarity": top["station_name_similarity"],
                    "selected_river_name_similarity": top["river_name_similarity"],
                    "selection_reason": reason,
                }
            )
        elif base["match_status"] == "no_primary_candidate":
            base["match_status"] = "ambiguous"
            base["selection_reason"] = reason
        else:
            base["selection_reason"] = reason
        rows.append(base)

    return pd.DataFrame(rows)


def _source_names_from_matrix(ds) -> List[str]:
    if "source_name" not in ds.variables:
        raise ValidationError("Matrix does not contain source_name lookup")
    return read_text(ds.variables["source_name"])


def _matrix_row_index(anchor: pd.Series, resolution: str) -> Optional[int]:
    value = anchor.get(f"selected_{resolution}_index", np.nan)
    try:
        if pd.isna(value):
            return None
        return int(value)
    except Exception:
        return None


def _coverage_mask(
    years: np.ndarray,
    start_year: Optional[int],
    end_year: Optional[int],
    allow_unknown_coverage: bool,
) -> Tuple[np.ndarray, str, Optional[int], Optional[int]]:
    valid_years = years >= 0
    if start_year is not None and end_year is not None:
        return valid_years & (years >= int(start_year)) & (years <= int(end_year)), "known", int(start_year), int(end_year)
    if allow_unknown_coverage:
        observed = years[valid_years]
        if len(observed) == 0:
            return np.zeros(len(years), dtype=bool), "unknown_no_main_years", None, None
        return valid_years, "unknown_used_all_main_years", int(observed.min()), int(observed.max())
    return np.zeros(len(years), dtype=bool), "unknown_excluded", None, None


def aggregate_one_anchor_variable(
    anchor: pd.Series,
    resolution: str,
    years: np.ndarray,
    main_values: np.ndarray,
    main_flags: np.ndarray,
    selected_source_index: np.ndarray,
    source_names: Sequence[str],
    variable: str,
    allowed_flags: Sequence[int],
    climatology_allowed_flags: Sequence[int],
    min_overlap_years: int,
    min_records_per_year: int,
    allow_unknown_coverage: bool,
) -> List[Dict]:
    climate_value = anchor.get(variable, np.nan)
    climate_flag = anchor.get(FLAG_VARIABLES[variable], 9)
    if not np.isfinite(climate_value):
        return []
    try:
        climate_flag = int(climate_flag)
    except Exception:
        climate_flag = 9
    if climate_flag not in climatology_allowed_flags:
        return []

    start_year_raw = anchor.get("coverage_start_year")
    end_year_raw = anchor.get("coverage_end_year")
    start_year = int(start_year_raw) if pd.notna(start_year_raw) else None
    end_year = int(end_year_raw) if pd.notna(end_year_raw) else None
    coverage, period_status, effective_start, effective_end = _coverage_mask(
        years, start_year, end_year, allow_unknown_coverage
    )

    base_valid = (
        coverage
        & np.isfinite(main_values)
        & np.isin(main_flags, np.asarray(allowed_flags, dtype=np.int64))
        & (selected_source_index >= 0)
    )
    if not np.any(base_valid):
        return []

    climate_source_canonical = canonical_source(anchor.get("climatology_source", ""))
    valid_positions = np.where(base_valid)[0]
    rows = []
    for pos in valid_positions:
        source_idx = int(selected_source_index[pos])
        source = source_names[source_idx] if 0 <= source_idx < len(source_names) else ""
        if not source:
            continue
        source_canonical = canonical_source(source)
        if source_canonical and source_canonical == climate_source_canonical:
            continue
        rows.append(
            {
                "year": int(years[pos]),
                "value": float(main_values[pos]),
                "flag": int(main_flags[pos]),
                "main_source": source,
                "main_source_canonical": source_canonical,
            }
        )
    if not rows:
        return []

    raw = pd.DataFrame(rows)
    outputs: List[Dict] = []
    for (source, source_canonical), source_df in raw.groupby(
        ["main_source", "main_source_canonical"], dropna=False, sort=True
    ):
        annual = (
            source_df.groupby("year", as_index=False)
            .agg(
                annual_mean=("value", "mean"),
                records_in_year=("value", "size"),
                derived_records_in_year=("flag", lambda x: int(np.count_nonzero(np.asarray(x) == 1))),
            )
        )
        annual = annual[annual["records_in_year"] >= int(min_records_per_year)].copy()
        if len(annual) < int(min_overlap_years):
            continue

        n_records = int(source_df[source_df["year"].isin(annual["year"])].shape[0])
        if effective_start is not None and effective_end is not None and effective_end >= effective_start:
            denominator = effective_end - effective_start + 1
            overlap_fraction = float(len(annual) / denominator) if denominator > 0 else np.nan
        else:
            overlap_fraction = np.nan

        source_flags = source_df[source_df["year"].isin(annual["year"])]["flag"].to_numpy(dtype=np.int64)
        main_flag_class = "includes_derived" if np.any(source_flags == 1) else "reported_only"
        climatology_flag_class = "derived" if climate_flag == 1 else "reported"
        derivation_class = f"{climatology_flag_class}-{main_flag_class}"

        output = anchor.to_dict()
        output.update(
            {
                "resolution": resolution,
                "variable": variable,
                "climatology_value": float(climate_value),
                "climatology_flag": climate_flag,
                "main_source": clean_text(source),
                "main_source_canonical": clean_text(source_canonical),
                "main_aggregated_value": float(annual["annual_mean"].mean()),
                "main_annual_sd": float(annual["annual_mean"].std(ddof=1)) if len(annual) > 1 else np.nan,
                "main_annual_min": float(annual["annual_mean"].min()),
                "main_annual_max": float(annual["annual_mean"].max()),
                "n_overlap_years": int(len(annual)),
                "first_overlap_year": int(annual["year"].min()),
                "last_overlap_year": int(annual["year"].max()),
                "n_main_records": n_records,
                "median_records_per_year": float(annual["records_in_year"].median()),
                "overlap_fraction": overlap_fraction,
                "period_match_status": period_status,
                "effective_period_start_year": effective_start,
                "effective_period_end_year": effective_end,
                "main_flag_class": main_flag_class,
                "comparison_derivation_class": derivation_class,
                "same_source_records_excluded": True,
            }
        )
        outputs.append(output)
    return outputs


def build_source_specific_pairs(
    anchors: pd.DataFrame,
    matrix_paths: Mapping[str, Path],
    allowed_flags: Sequence[int],
    climatology_allowed_flags: Sequence[int],
    min_overlap_years: int,
    min_records_per_year: int,
    allow_unknown_coverage: bool,
) -> pd.DataFrame:
    outputs: List[Dict] = []

    for resolution, path in matrix_paths.items():
        if not path.is_file():
            continue
        usable = anchors[anchors[f"selected_{resolution}_index"].notna()].copy()
        if usable.empty:
            log(f"No matched climatology records have a {resolution} matrix row")
            continue
        log(f"Building {resolution} period-matched comparisons for {len(usable)} climatology records")

        with nc4.Dataset(path, "r") as ds:
            years, _ = decode_time_values(ds)
            source_names = _source_names_from_matrix(ds)
            if "selected_source_index" not in ds.variables:
                raise ValidationError(f"{path} does not contain selected_source_index")

            grouped = usable.groupby("selected_main_station_uid", sort=False)
            for station_counter, (station_uid, station_anchors) in enumerate(grouped, start=1):
                if station_counter % 100 == 0:
                    log(f"{resolution}: processed {station_counter}/{grouped.ngroups} matched stations")
                row_idx = _matrix_row_index(station_anchors.iloc[0], resolution)
                if row_idx is None:
                    continue
                selected_source_index = read_int(
                    ds.variables["selected_source_index"], key=(row_idx, slice(None)), fill_value=-1
                ).reshape(-1)
                if len(selected_source_index) != len(years):
                    raise ValidationError(
                        f"{resolution} row length mismatch for {station_uid}: "
                        f"source_index={len(selected_source_index)}, time={len(years)}"
                    )

                row_values: Dict[str, np.ndarray] = {}
                row_flags: Dict[str, np.ndarray] = {}
                for variable in VARIABLES:
                    if variable not in ds.variables:
                        row_values[variable] = np.full(len(years), np.nan)
                    else:
                        row_values[variable] = read_numeric(
                            ds.variables[variable], key=(row_idx, slice(None))
                        ).reshape(-1)
                    flag_name = FLAG_VARIABLES[variable]
                    if flag_name not in ds.variables:
                        row_flags[variable] = np.full(len(years), 9, dtype=np.int64)
                    else:
                        row_flags[variable] = read_int(
                            ds.variables[flag_name], key=(row_idx, slice(None)), fill_value=9
                        ).reshape(-1)

                for _, anchor in station_anchors.iterrows():
                    for variable in VARIABLES:
                        outputs.extend(
                            aggregate_one_anchor_variable(
                                anchor=anchor,
                                resolution=resolution,
                                years=years,
                                main_values=row_values[variable],
                                main_flags=row_flags[variable],
                                selected_source_index=selected_source_index,
                                source_names=source_names,
                                variable=variable,
                                allowed_flags=allowed_flags,
                                climatology_allowed_flags=climatology_allowed_flags,
                                min_overlap_years=min_overlap_years,
                                min_records_per_year=min_records_per_year,
                                allow_unknown_coverage=allow_unknown_coverage,
                            )
                        )

    return pd.DataFrame(outputs)


def select_resolution_pairs(source_specific: pd.DataFrame) -> pd.DataFrame:
    if source_specific.empty:
        return source_specific.copy()
    work = source_specific.copy()
    work["_source_rank"] = work["main_source"].astype(str)
    sort_cols = [
        "climatology_record_uid",
        "selected_main_station_uid",
        "variable",
        "resolution",
        "n_overlap_years",
        "n_main_records",
        "_source_rank",
    ]
    work = work.sort_values(
        sort_cols,
        ascending=[True, True, True, True, False, False, True],
    )
    selected = work.drop_duplicates(
        subset=["climatology_record_uid", "selected_main_station_uid", "variable", "resolution"],
        keep="first",
    ).drop(columns=["_source_rank"])
    selected["main_source_selection_reason"] = "most valid overlap years, then most main records"
    return selected.reset_index(drop=True)


def select_primary_pairs(resolution_pairs: pd.DataFrame) -> pd.DataFrame:
    if resolution_pairs.empty:
        return resolution_pairs.copy()
    work = resolution_pairs.copy()
    work["resolution_priority"] = work["resolution"].map(RESOLUTION_PRIORITY).fillna(0).astype(int)
    work["_overlap_fraction_sort"] = work["overlap_fraction"].fillna(-1.0)
    work = work.sort_values(
        [
            "climatology_record_uid",
            "selected_main_station_uid",
            "variable",
            "n_overlap_years",
            "_overlap_fraction_sort",
            "resolution_priority",
            "n_main_records",
        ],
        ascending=[True, True, True, False, False, False, False],
    )
    primary = work.drop_duplicates(
        subset=["climatology_record_uid", "selected_main_station_uid", "variable"],
        keep="first",
    ).drop(columns=["_overlap_fraction_sort"])
    primary["primary_resolution_selection_reason"] = (
        "most overlap years, then greatest overlap fraction, then annual/monthly/daily priority"
    )
    return primary.reset_index(drop=True)


def safe_corr(left: np.ndarray, right: np.ndarray) -> float:
    if len(left) < 3:
        return np.nan
    if np.nanstd(left) == 0 or np.nanstd(right) == 0:
        return np.nan
    return float(np.corrcoef(left, right)[0, 1])


def metric_row(group: pd.DataFrame, label_values: Mapping[str, str]) -> Dict:
    x = pd.to_numeric(group["climatology_value"], errors="coerce").to_numpy(dtype=np.float64)
    y = pd.to_numeric(group["main_aggregated_value"], errors="coerce").to_numpy(dtype=np.float64)
    finite = np.isfinite(x) & np.isfinite(y)
    x = x[finite]
    y = y[finite]
    positive = (x > 0) & (y > 0)
    diff = y - x

    row = dict(label_values)
    row.update(
        {
            "n_pairs": int(len(x)),
            "n_climatology_stations": int(group.loc[finite, "climatology_station_uid"].nunique()) if len(group) else 0,
            "n_main_stations": int(group.loc[finite, "selected_main_station_uid"].nunique()) if len(group) else 0,
            "bias_main_minus_climatology": float(np.mean(diff)) if len(diff) else np.nan,
            "mae": float(np.mean(np.abs(diff))) if len(diff) else np.nan,
            "rmse": float(np.sqrt(np.mean(diff ** 2))) if len(diff) else np.nan,
            "median_distance_m": float(pd.to_numeric(group["selected_distance_m"], errors="coerce").median()),
            "median_overlap_years": float(pd.to_numeric(group["n_overlap_years"], errors="coerce").median()),
            "n_positive_pairs": int(np.count_nonzero(positive)),
        }
    )

    if np.count_nonzero(positive) >= 3:
        log_x = np.log10(x[positive])
        log_y = np.log10(y[positive])
        rank_x = pd.Series(x[positive]).rank(method="average").to_numpy(dtype=np.float64)
        rank_y = pd.Series(y[positive]).rank(method="average").to_numpy(dtype=np.float64)
        ratio = y[positive] / x[positive]
        row.update(
            {
                "spearman_rho": safe_corr(rank_x, rank_y),
                "pearson_r_log10": safe_corr(log_x, log_y),
                "median_log10_ratio": float(np.median(np.log10(ratio))),
                "median_ratio_main_to_climatology": float(np.median(ratio)),
                "within_factor_2_percent": float(np.mean((ratio >= 0.5) & (ratio <= 2.0)) * 100.0),
                "within_factor_10_percent": float(np.mean((ratio >= 0.1) & (ratio <= 10.0)) * 100.0),
            }
        )
    else:
        row.update(
            {
                "spearman_rho": np.nan,
                "pearson_r_log10": np.nan,
                "median_log10_ratio": np.nan,
                "median_ratio_main_to_climatology": np.nan,
                "within_factor_2_percent": np.nan,
                "within_factor_10_percent": np.nan,
            }
        )
    return row


def compute_summary_metrics(primary_pairs: pd.DataFrame) -> pd.DataFrame:
    if primary_pairs.empty:
        return pd.DataFrame()
    rows: List[Dict] = []

    for variable, group in primary_pairs.groupby("variable", sort=True):
        rows.append(metric_row(group, {"summary_level": "overall", "variable": variable, "resolution": "all", "climatology_source": "all", "main_source": "all"}))

    for (variable, resolution), group in primary_pairs.groupby(["variable", "resolution"], sort=True):
        rows.append(metric_row(group, {"summary_level": "by_resolution", "variable": variable, "resolution": resolution, "climatology_source": "all", "main_source": "all"}))

    for (variable, climate_source, main_source), group in primary_pairs.groupby(
        ["variable", "climatology_source", "main_source"], sort=True, dropna=False
    ):
        rows.append(
            metric_row(
                group,
                {
                    "summary_level": "by_source_pair",
                    "variable": variable,
                    "resolution": "all",
                    "climatology_source": clean_text(climate_source),
                    "main_source": clean_text(main_source),
                },
            )
        )

    return pd.DataFrame(rows)


def build_funnel(
    climatology_stations: pd.DataFrame,
    climatology_records: pd.DataFrame,
    candidates: pd.DataFrame,
    selected_matches: pd.DataFrame,
    anchors: pd.DataFrame,
    source_specific_pairs: pd.DataFrame,
    primary_pairs: pd.DataFrame,
    exact_distance_m: float,
    max_distance_m: float,
) -> pd.DataFrame:
    valid_coords = np.isfinite(climatology_stations["climatology_lat"]) & np.isfinite(climatology_stations["climatology_lon"])
    primary_candidate_uids = set(
        candidates.loc[candidates["distance_m"] <= max_distance_m, "climatology_station_uid"].astype(str)
    ) if not candidates.empty else set()
    exact_candidate_uids = set(
        candidates.loc[candidates["distance_m"] <= exact_distance_m, "climatology_station_uid"].astype(str)
    ) if not candidates.empty else set()

    rows = [
        ("climatology_stations_total", len(climatology_stations), "unique climatology stations"),
        ("climatology_records_with_ssc_or_ssl", len(climatology_records), "record-level climatology observations"),
        ("climatology_stations_valid_coordinates", int(valid_coords.sum()), "finite WGS84 coordinates"),
        ("stations_with_exact_candidate", len(exact_candidate_uids), f"at least one main cluster within {exact_distance_m:g} m"),
        ("stations_with_primary_candidate", len(primary_candidate_uids), f"at least one main cluster within {max_distance_m:g} m"),
        ("stations_selected_unique_match", int((selected_matches["match_status"] == "selected").sum()), "selected after ambiguity screening"),
        ("stations_ambiguous", int((selected_matches["match_status"] == "ambiguous").sum()), "not uniquely distinguishable"),
        ("selected_records_joined_to_main_cluster", len(anchors), "climatology records at selected stations"),
        ("selected_records_known_coverage", int((anchors["coverage_status"] == "known").sum()) if len(anchors) else 0, "both coverage start and end resolved"),
        ("source_specific_valid_pairs", len(source_specific_pairs), "variable-resolution-source pairs meeting overlap criteria"),
        ("primary_valid_pairs", len(primary_pairs), "one primary resolution per climatology record, cluster and variable"),
        ("primary_ssc_pairs", int((primary_pairs.get("variable", pd.Series(dtype=str)) == "SSC").sum()), "primary SSC pairs"),
        ("primary_ssl_pairs", int((primary_pairs.get("variable", pd.Series(dtype=str)) == "SSL").sum()), "primary SSL pairs"),
    ]
    return pd.DataFrame(rows, columns=["stage", "count", "definition"])


def format_number(value, digits: int = 3) -> str:
    try:
        if pd.isna(value):
            return "NA"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "NA"


def write_markdown_report(
    path: Path,
    args,
    input_paths: Mapping[str, Path],
    funnel: pd.DataFrame,
    metrics: pd.DataFrame,
) -> None:
    lines = [
        "# Main matrix - climatology cross-source comparison",
        "",
        "## Purpose",
        "",
        "This diagnostic links spatially coincident or nearby climatology stations to main-matrix clusters, excludes main-matrix records selected from the same source dataset, and compares period-matched long-term SSC and SSL estimates.",
        "",
        "Climatology time coordinates are not used as exact observation dates. The script uses source coverage years when available, first averages main-matrix observations within each calendar year, and then averages the resulting annual means across overlapping years.",
        "",
        "## Inputs",
        "",
    ]
    for key, value in input_paths.items():
        lines.append(f"- **{key}**: `{value}`")

    lines.extend(
        [
            "",
            "## Configuration",
            "",
            f"- Exact-coordinate threshold: {args.exact_distance_m:g} m",
            f"- Primary nearby threshold: {args.max_distance_m:g} m",
            f"- Sensitivity candidate threshold: {args.sensitivity_distance_m:g} m",
            f"- Main allowed flags: {','.join(map(str, args.allowed_flags))}",
            f"- Climatology allowed flags: {','.join(map(str, args.climatology_allowed_flags))}",
            f"- Minimum valid overlap years: {args.min_overlap_years}",
            f"- Minimum records per year: {args.min_records_per_year}",
            f"- Unknown coverage allowed: {args.allow_unknown_coverage}",
            f"- Resolved main clusters only: {args.resolved_main_only}",
            f"- Name support required for non-exact matches: {args.require_name_support_for_near}",
            "",
            "## Match funnel",
            "",
            "| Stage | Count | Definition |",
            "|---|---:|---|",
        ]
    )
    for _, row in funnel.iterrows():
        lines.append(f"| {row['stage']} | {int(row['count'])} | {row['definition']} |")

    lines.extend(["", "## Overall metrics", ""])
    overall = metrics[metrics["summary_level"] == "overall"] if not metrics.empty else pd.DataFrame()
    if overall.empty:
        lines.append("No primary SSC or SSL pairs met the configured spatial, source-independence, quality, and temporal-overlap criteria.")
    else:
        lines.extend(
            [
                "| Variable | n | Spearman rho | Pearson r (log10) | Median main/climatology ratio | Within factor 2 (%) | RMSE |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for _, row in overall.iterrows():
            lines.append(
                "| {variable} | {n} | {rho} | {pearson} | {ratio} | {factor2} | {rmse} |".format(
                    variable=row["variable"],
                    n=int(row["n_pairs"]),
                    rho=format_number(row["spearman_rho"]),
                    pearson=format_number(row["pearson_r_log10"]),
                    ratio=format_number(row["median_ratio_main_to_climatology"]),
                    factor2=format_number(row["within_factor_2_percent"], 1),
                    rmse=format_number(row["rmse"]),
                )
            )

    lines.extend(
        [
            "",
            "## Interpretation notes",
            "",
            "- A selected pair indicates spatial coincidence or proximity, not proof that both products represent the identical physical gauge or cross-section.",
            "- Each primary point uses one independent main-matrix source. Records whose selected source canonicalizes to the climatology source are excluded before aggregation.",
            "- Daily, monthly, and annual matrices are processed separately. The primary table selects one resolution after source-specific aggregation; all retained alternatives remain in the resolution-level table.",
            "- SSL remains in the release unit of t d-1. Daily SSL is averaged within year rather than summed, preserving comparability with standardized climatology SSL.",
            "- Scatter plots and ratio metrics use positive finite values only. Zero values remain in the pair tables and raw-unit metrics.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def plot_pairs(primary_pairs: pd.DataFrame, png_path: Path, pdf_path: Path) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        log(f"Skipping plots because matplotlib is unavailable: {exc}")
        return

    if primary_pairs.empty:
        log("No primary pairs to plot; skipping figure generation")
        return

    variables_present = [var for var in VARIABLES if var in set(primary_pairs.get("variable", []))]
    if not variables_present:
        variables_present = list(VARIABLES)
    fig, axes = plt.subplots(1, len(variables_present), figsize=(6.4 * len(variables_present), 5.4), squeeze=False)
    marker_by_resolution = {"daily": "o", "monthly": "s", "annual": "^"}

    for axis, variable in zip(axes[0], variables_present):
        subset = primary_pairs[primary_pairs.get("variable", pd.Series(dtype=str)) == variable].copy()
        subset = subset[
            np.isfinite(pd.to_numeric(subset.get("climatology_value"), errors="coerce"))
            & np.isfinite(pd.to_numeric(subset.get("main_aggregated_value"), errors="coerce"))
            & (pd.to_numeric(subset.get("climatology_value"), errors="coerce") > 0)
            & (pd.to_numeric(subset.get("main_aggregated_value"), errors="coerce") > 0)
        ]
        if subset.empty:
            axis.text(0.5, 0.5, f"No positive {variable} pairs", ha="center", va="center", transform=axis.transAxes)
            axis.set_axis_off()
            continue

        for resolution, group in subset.groupby("resolution", sort=True):
            axis.scatter(
                group["climatology_value"],
                group["main_aggregated_value"],
                marker=marker_by_resolution.get(resolution, "o"),
                alpha=0.65,
                s=34,
                label=f"{resolution} (n={len(group)})",
            )

        values = np.concatenate(
            [
                subset["climatology_value"].to_numpy(dtype=np.float64),
                subset["main_aggregated_value"].to_numpy(dtype=np.float64),
            ]
        )
        lower = 10 ** math.floor(math.log10(values.min()))
        upper = 10 ** math.ceil(math.log10(values.max()))
        if lower == upper:
            lower /= 10.0
            upper *= 10.0
        line = np.logspace(math.log10(lower), math.log10(upper), 200)
        axis.plot(line, line, linestyle="--", linewidth=1.2, label="1:1")
        axis.plot(line, 2.0 * line, linestyle=":", linewidth=0.9)
        axis.plot(line, 0.5 * line, linestyle=":", linewidth=0.9)
        axis.plot(line, 10.0 * line, linestyle="-.", linewidth=0.7)
        axis.plot(line, 0.1 * line, linestyle="-.", linewidth=0.7)
        axis.set_xscale("log")
        axis.set_yscale("log")
        axis.set_xlim(lower, upper)
        axis.set_ylim(lower, upper)
        units = "mg L-1" if variable == "SSC" else "t d-1"
        axis.set_xlabel(f"Climatology {variable} ({units})")
        axis.set_ylabel(f"Main-matrix long-term mean {variable} ({units})")
        axis.set_title(variable)
        axis.grid(True, which="both", linewidth=0.4, alpha=0.35)

        rank_x = subset["climatology_value"].rank().to_numpy(dtype=np.float64)
        rank_y = subset["main_aggregated_value"].rank().to_numpy(dtype=np.float64)
        rho = safe_corr(rank_x, rank_y)
        ratio = np.median(
            subset["main_aggregated_value"].to_numpy(dtype=np.float64)
            / subset["climatology_value"].to_numpy(dtype=np.float64)
        )
        axis.text(
            0.03,
            0.97,
            f"n = {len(subset)}\nSpearman rho = {format_number(rho)}\nMedian ratio = {format_number(ratio)}",
            ha="left",
            va="top",
            transform=axis.transAxes,
        )
        axis.legend(loc="lower right", fontsize=8)

    fig.tight_layout()
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR), help="Directory containing released NetCDF files")
    parser.add_argument("--climatology-file", default=None, help="Override climatology NetCDF path")
    parser.add_argument("--daily-file", default=None, help="Override daily matrix path")
    parser.add_argument("--monthly-file", default=None, help="Override monthly matrix path")
    parser.add_argument("--annual-file", default=None, help="Override annual matrix path")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory")
    parser.add_argument("--exact-distance-m", type=float, default=100.0, help="Coordinate-exact distance threshold")
    parser.add_argument("--max-distance-m", type=float, default=1000.0, help="Primary nearby distance threshold")
    parser.add_argument("--sensitivity-distance-m", type=float, default=5000.0, help="Candidate audit distance threshold")
    parser.add_argument("--name-similarity-threshold", type=float, default=0.85, help="Station/river name support threshold")
    parser.add_argument("--ambiguity-distance-gap-m", type=float, default=100.0, help="Minimum top-two distance gap for unique nearest selection")
    parser.add_argument("--ambiguity-distance-ratio", type=float, default=1.5, help="Minimum second/first distance ratio for unique nearest selection")
    parser.add_argument("--allowed-flags", type=parse_flag_list, default=(0, 1), help="Allowed main-matrix flags, comma separated")
    parser.add_argument("--climatology-allowed-flags", type=parse_flag_list, default=(0, 1), help="Allowed climatology flags, comma separated")
    parser.add_argument("--min-overlap-years", type=int, default=3, help="Minimum valid annual means for a source-specific pair")
    parser.add_argument("--min-records-per-year", type=int, default=1, help="Minimum valid records required to retain a year")
    parser.add_argument("--allow-unknown-coverage", action="store_true", help="Use all main-matrix years when climatology coverage is unknown")
    parser.add_argument("--resolved-main-only", action="store_true", help="Restrict candidate main clusters to resolved basin assignments")
    parser.add_argument("--require-name-support-for-near", action="store_true", help="Require station or river name support for matches beyond exact threshold")
    parser.add_argument("--no-plots", action="store_true", help="Do not generate PNG/PDF scatter plots")
    parser.add_argument("--overwrite", action="store_true", help="Allow replacement of existing outputs")
    args = parser.parse_args(argv)

    args.release_dir = Path(args.release_dir).expanduser().resolve()
    args.out_dir = Path(args.out_dir).expanduser().resolve()
    args.climatology_file = resolve_path(args.climatology_file, args.release_dir / CLIMATOLOGY_FILENAME)
    args.matrix_paths = {
        "daily": resolve_path(args.daily_file, args.release_dir / MATRIX_FILENAMES["daily"]),
        "monthly": resolve_path(args.monthly_file, args.release_dir / MATRIX_FILENAMES["monthly"]),
        "annual": resolve_path(args.annual_file, args.release_dir / MATRIX_FILENAMES["annual"]),
    }

    if args.exact_distance_m < 0 or args.max_distance_m <= 0:
        parser.error("Distance thresholds must be positive")
    if args.exact_distance_m > args.max_distance_m:
        parser.error("--exact-distance-m cannot exceed --max-distance-m")
    if args.sensitivity_distance_m < args.max_distance_m:
        parser.error("--sensitivity-distance-m cannot be smaller than --max-distance-m")
    if args.min_overlap_years < 1 or args.min_records_per_year < 1:
        parser.error("Overlap and per-year record requirements must be at least 1")
    return args


def output_paths(out_dir: Path) -> Dict[str, Path]:
    return {
        "funnel": out_dir / "s13_match_funnel.csv",
        "candidates": out_dir / "s13_spatial_candidates.csv",
        "matches": out_dir / "s13_selected_station_matches.csv",
        "source_pairs": out_dir / "s13_source_specific_pair_values.csv",
        "resolution_pairs": out_dir / "s13_resolution_pair_values.csv",
        "primary_pairs": out_dir / "s13_primary_pair_values.csv",
        "metrics": out_dir / "s13_summary_metrics.csv",
        "plot_png": out_dir / "s13_scatter_main_climatology.png",
        "plot_pdf": out_dir / "s13_scatter_main_climatology.pdf",
        "report": out_dir / "s13_main_climatology_report.md",
    }


def guard_outputs(paths: Mapping[str, Path], overwrite: bool, no_plots: bool) -> None:
    relevant = [path for key, path in paths.items() if not (no_plots and key.startswith("plot_"))]
    existing = [path for path in relevant if path.exists()]
    if existing and not overwrite:
        sample = "\n".join(f"  - {path}" for path in existing[:10])
        raise FileExistsError(f"Output files already exist; use --overwrite to replace them:\n{sample}")


def write_dataframe(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"Wrote {path} ({len(df)} rows)")


def main(argv=None) -> int:
    args = parse_args(argv)
    require_netcdf()
    paths = output_paths(args.out_dir)
    guard_outputs(paths, args.overwrite, args.no_plots)
    args.out_dir.mkdir(parents=True, exist_ok=True)

    existing_matrices = {key: value for key, value in args.matrix_paths.items() if value.is_file()}
    if not existing_matrices:
        raise FileNotFoundError(
            "None of the daily, monthly, or annual matrix files exist under the configured paths"
        )

    input_paths = {"climatology": args.climatology_file, **existing_matrices}
    for label, path in input_paths.items():
        log(f"Input {label}: {path}")

    main_stations = load_main_station_catalog(existing_matrices)
    climatology_stations, climatology_records = load_climatology_records(args.climatology_file)

    candidates = build_spatial_candidates(
        climatology_stations=climatology_stations,
        main_stations=main_stations,
        exact_distance_m=args.exact_distance_m,
        max_distance_m=args.max_distance_m,
        sensitivity_distance_m=args.sensitivity_distance_m,
        name_similarity_threshold=args.name_similarity_threshold,
    )
    selected_matches = select_spatial_matches(
        climatology_stations=climatology_stations,
        candidates=candidates,
        exact_distance_m=args.exact_distance_m,
        max_distance_m=args.max_distance_m,
        ambiguity_distance_gap_m=args.ambiguity_distance_gap_m,
        ambiguity_distance_ratio=args.ambiguity_distance_ratio,
        require_name_support_for_near=args.require_name_support_for_near,
        resolved_main_only=args.resolved_main_only,
    )

    selected_only = selected_matches[selected_matches["match_status"] == "selected"].copy()
    anchor_columns = [
        "climatology_station_uid",
        "match_status",
        "selected_main_station_uid",
        "selected_distance_m",
        "selected_spatial_match_class",
        "selected_main_station_name",
        "selected_main_river_name",
        "selected_main_basin_status",
        "selected_available_resolutions",
        "selected_daily_index",
        "selected_monthly_index",
        "selected_annual_index",
        "selected_station_name_similarity",
        "selected_river_name_similarity",
        "selection_reason",
    ]
    anchors = climatology_records.merge(
        selected_only[anchor_columns],
        on="climatology_station_uid",
        how="inner",
        validate="many_to_one",
    )

    source_specific_pairs = build_source_specific_pairs(
        anchors=anchors,
        matrix_paths=existing_matrices,
        allowed_flags=args.allowed_flags,
        climatology_allowed_flags=args.climatology_allowed_flags,
        min_overlap_years=args.min_overlap_years,
        min_records_per_year=args.min_records_per_year,
        allow_unknown_coverage=args.allow_unknown_coverage,
    )
    resolution_pairs = select_resolution_pairs(source_specific_pairs)
    primary_pairs = select_primary_pairs(resolution_pairs)
    metrics = compute_summary_metrics(primary_pairs)
    funnel = build_funnel(
        climatology_stations=climatology_stations,
        climatology_records=climatology_records,
        candidates=candidates,
        selected_matches=selected_matches,
        anchors=anchors,
        source_specific_pairs=source_specific_pairs,
        primary_pairs=primary_pairs,
        exact_distance_m=args.exact_distance_m,
        max_distance_m=args.max_distance_m,
    )

    write_dataframe(funnel, paths["funnel"])
    write_dataframe(candidates, paths["candidates"])
    write_dataframe(selected_matches, paths["matches"])
    write_dataframe(source_specific_pairs, paths["source_pairs"])
    write_dataframe(resolution_pairs, paths["resolution_pairs"])
    write_dataframe(primary_pairs, paths["primary_pairs"])
    write_dataframe(metrics, paths["metrics"])

    if not args.no_plots:
        plot_pairs(primary_pairs, paths["plot_png"], paths["plot_pdf"])
        if paths["plot_png"].exists():
            log(f"Wrote {paths['plot_png']}")
        if paths["plot_pdf"].exists():
            log(f"Wrote {paths['plot_pdf']}")

    write_markdown_report(paths["report"], args, input_paths, funnel, metrics)
    log(f"Wrote {paths['report']}")
    log("Main matrix - climatology validation completed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
