#!/usr/bin/env python3
"""Validate satellite source records against main station-reference records using s5b linkage CSV.

This script is a companion to ``s11_satellite_insitu_validation.py``.

Key difference
--------------
The satellite side is not read from ``sed_reference_satellite.nc``. Instead, this
script treats the s5b satellite-to-main-cluster linkage CSV as the authoritative
linkage catalogue and reads satellite observations directly from their source
NetCDF files.

The accelerated s5b v2 links identify satellite rows by ``satellite_key`` and
do not carry source paths, so this script recovers ``path`` and satellite
source metadata from ``s5_basin_clustered_stations.csv`` via ``--s5-csv``.

The in-situ side follows the original s11 logic:
1. prefer a candidate sidecar when available;
2. otherwise use selected records from ``sed_reference_master.nc``.

The existing s11 functions are reused for normalization, temporal pairing,
stratification, metrics, figures, and the standard summary.
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

try:
    import s11_satellite_insitu_validation as base
except ImportError:  # allows module-style execution from the repository root
    from validate import s11_satellite_insitu_validation as base  # type: ignore

try:
    from pipeline_paths import (
        S5_BASIN_CLUSTERED_CSV,
        S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV,
        S6_MERGED_NC,
        S7_CLUSTER_RESOLUTION_CATALOG_CSV,
    )
except ImportError:  # allows module-style execution from the validate directory
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from pipeline_paths import (  # type: ignore
        S5_BASIN_CLUSTERED_CSV,
        S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV,
        S6_MERGED_NC,
        S7_CLUSTER_RESOLUTION_CATALOG_CSV,
    )


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_R_ROOT = Path(
    os.environ.get("OUTPUT_R_ROOT", str(PROJECT_DIR.parent))
).expanduser().resolve()

DEFAULT_LINKAGE_CSV = (
    OUTPUT_R_ROOT
    / S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV
)
DEFAULT_S5_CSV = (
    OUTPUT_R_ROOT
    / S5_BASIN_CLUSTERED_CSV
)
DEFAULT_SOURCE_ROOT = (OUTPUT_R_ROOT / "../output_resolution_organized").resolve()
DEFAULT_MASTER_NC = OUTPUT_R_ROOT / S6_MERGED_NC
DEFAULT_STATION_CATALOG_CSV = OUTPUT_R_ROOT / S7_CLUSTER_RESOLUTION_CATALOG_CSV
DEFAULT_OUT_DIR = (
    OUTPUT_R_ROOT
    / "scripts_basin_test/validate/output/validation_results_s5b"
)

REQUIRED_LINKAGE_COLUMNS = {
    "satellite_location_uid",
    "cluster_id",
    "cluster_uid",
    "source",
    "path",
    "resolution",
    "linked_cluster_id",
    "linked_cluster_uid",
    "linked_resolution",
    "link_status",
}

REQUIRED_V2_LINKAGE_COLUMNS = {
    "satellite_key",
    "satellite_station_id",
    "satellite_source",
    "satellite_resolution",
    "link_status",
    "link_method",
    "link_confidence",
    "linked_cluster_id",
    "linked_cluster_uid",
    "representative_point_distance_m",
    "n_valid_candidates",
}

LINK_META_COLUMNS = [
    "satellite_location_uid",
    "satellite_key",
    "s5b_schema",
    "source",
    "source_station_id",
    "path",
    "linked_cluster_id",
    "linked_cluster_uid",
    "linked_resolution",
    "link_method",
    "link_quality",
    "link_distance_m",
    "link_uparea_log10_error",
    "link_area_rel_error",
    "link_candidate_count",
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
    text = _clean_text(value).lower()
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
    return any(token in source for token in ("riversed", "river_sed", "gsed", "dethier", "aquasat"))


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
            kwargs = {
                "decode_times": False,
                "mask_and_scale": True,
            }
            if engine is not None:
                kwargs["engine"] = engine
            return xr.open_dataset(path, **kwargs)
        except Exception as exc:
            errors.append("{}: {}".format(engine or "default", exc))
    raise RuntimeError(
        "cannot open {}; tried {}".format(path, "; ".join(errors))
    )


def _decode_numeric_cf_time(
    values: np.ndarray,
    units: str,
    calendar: str,
) -> pd.DatetimeIndex:
    numeric = pd.to_numeric(
        pd.Series(np.asarray(values).reshape(-1)),
        errors="coerce",
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
                        int(value.year),
                        int(value.month),
                        int(value.day),
                        int(getattr(value, "hour", 0)),
                        int(getattr(value, "minute", 0)),
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
            "day": "D",
            "days": "D",
            "hour": "h",
            "hours": "h",
            "minute": "m",
            "minutes": "m",
            "second": "s",
            "seconds": "s",
        }[unit_text]
        return pd.DatetimeIndex(
            pd.to_datetime(
                numeric,
                unit=unit,
                origin=origin,
                errors="coerce",
            )
        )

    # Standardized source files in this project normally use days since 1970-01-01.
    return pd.DatetimeIndex(
        pd.to_datetime(
            numeric,
            unit="D",
            origin="1970-01-01",
            errors="coerce",
        )
    )


def _read_time_axis(ds) -> Tuple[pd.DatetimeIndex, str]:
    time_name = _first_existing(
        ds.variables,
        ("time", "date", "datetime", "timestamp", "obs_time"),
    )
    if time_name is None:
        raise ValueError("no time/date variable")

    da = ds[time_name]
    if len(da.dims) != 1:
        raise ValueError(
            "time variable {} is not one-dimensional: {}".format(
                time_name, da.dims
            )
        )
    time_dim = da.dims[0]
    raw = np.asarray(da.values).reshape(-1)

    if np.issubdtype(raw.dtype, np.datetime64):
        parsed = pd.to_datetime(raw, errors="coerce")
    elif raw.dtype.kind in {"S", "U", "O"}:
        texts = [
            value.decode("utf-8", errors="ignore")
            if isinstance(value, bytes)
            else str(value)
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
    values = np.ma.asarray(da.transpose(*ordered_dims).values)
    values = np.ma.filled(values, np.nan)

    if values.shape[0] != n_time:
        raise ValueError(
            "{} time length {} does not match {}".format(
                name, values.shape[0], n_time
            )
        )

    if values.ndim > 1:
        extra_size = int(np.prod(values.shape[1:]))
        if extra_size != 1:
            raise ValueError(
                "{} has unsupported non-singleton dimensions {}".format(
                    name, da.dims
                )
            )
        values = values.reshape(n_time)

    return pd.to_numeric(
        pd.Series(np.asarray(values).reshape(-1)),
        errors="coerce",
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
        candidates.extend(
            [
                source_root / supplied,
                linkage_csv_parent / supplied,
                supplied,
            ]
        )

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
    (
        ordinal,
        row,
        source_root_text,
        linkage_parent_text,
        window_start_text,
        window_end_text,
    ) = task

    source_root = Path(source_root_text)
    linkage_parent = Path(linkage_parent_text)
    resolved_path, attempted = _resolve_source_path(
        _clean_text(row.get("path", "")),
        source_root,
        linkage_parent,
    )

    report: Dict[str, object] = {
        "satellite_location_uid": _clean_text(
            row.get("satellite_location_uid", "")
        ),
        "source": _clean_text(row.get("source", "")),
        "source_station_id": _clean_text(row.get("source_station_id", "")),
        "linked_cluster_uid": _clean_text(
            row.get("linked_cluster_uid", "")
        ),
        "linked_resolution": _normalize_resolution(
            row.get("linked_resolution", "")
        ),
        "path_from_s5b": _clean_text(row.get("path", "")),
        "resolved_path": str(resolved_path) if resolved_path else "",
        "status": "",
        "message": "",
        "n_time_steps": 0,
        "n_time_overlap": 0,
        "n_value_rows": 0,
        "time_start_loaded": "",
        "time_end_loaded": "",
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

        q = _extract_numeric_time_series(
            ds, ("Q", "q", "discharge"), time_dim, n_time
        )
        ssc = _extract_numeric_time_series(
            ds, ("SSC", "ssc"), time_dim, n_time
        )
        ssl = _extract_numeric_time_series(
            ds, ("SSL", "ssl"), time_dim, n_time
        )
        q_flag = _extract_numeric_time_series(
            ds,
            ("Q_flag", "q_flag", "Q_qc_flag", "q_qc_flag"),
            time_dim,
            n_time,
        )
        ssc_flag = _extract_numeric_time_series(
            ds,
            ("SSC_flag", "ssc_flag", "SSC_qc_flag", "ssc_qc_flag"),
            time_dim,
            n_time,
        )
        ssl_flag = _extract_numeric_time_series(
            ds,
            ("SSL_flag", "ssl_flag", "SSL_qc_flag", "ssl_qc_flag"),
            time_dim,
            n_time,
        )

        finite_any = np.isfinite(q) | np.isfinite(ssc) | np.isfinite(ssl)
        keep = in_window & finite_any
        if not keep.any():
            report["status"] = "no_variable_values"
            return ordinal, [], report

        rows: List[Dict[str, object]] = []
        kept_positions = np.where(keep)[0]
        for position in kept_positions:
            timestamp = pd.Timestamp(times[position])
            rows.append(
                {
                    "record_id": "{}:{}".format(
                        _clean_text(row.get("satellite_location_uid", "")),
                        int(position),
                    ),
                    # Keep the satellite singleton identifiers for provenance.
                    # base.normalize_observation_table will replace the pairing
                    # key with linked_cluster_uid + linked_resolution.
                    "cluster_id": row.get("cluster_id", ""),
                    "cluster_uid": _clean_text(row.get("cluster_uid", "")),
                    "resolution": _normalize_resolution(
                        row.get("resolution", "")
                    ),
                    "linked_cluster_id": row.get("linked_cluster_id", ""),
                    "linked_cluster_uid": _clean_text(
                        row.get("linked_cluster_uid", "")
                    ),
                    "linked_resolution": _normalize_resolution(
                        row.get("linked_resolution", "")
                    ),
                    "link_status": "linked",
                    "source": _clean_text(row.get("source", "")),
                    "source_family": "satellite",
                    "observation_type": "Satellite",
                    # Use the stable s5b location UID as the satellite record key.
                    "source_station_uid": _clean_text(
                        row.get("satellite_location_uid", "")
                    ),
                    "source_station_native_id": _clean_text(
                        row.get("source_station_id", "")
                    ),
                    "source_station_paths": str(resolved_path),
                    "candidate_path": _clean_text(row.get("path", "")),
                    "date": timestamp.strftime("%Y-%m-%d"),
                    "Q": q[position],
                    "SSC": ssc[position],
                    "SSL": ssl[position],
                    "Q_flag": q_flag[position],
                    "SSC_flag": ssc_flag[position],
                    "SSL_flag": ssl_flag[position],
                    "validation_only": 1,
                    "merge_policy": "validation_only_from_s5b_source_nc",
                }
            )

        report["status"] = "loaded"
        report["n_value_rows"] = int(len(rows))
        report["time_start_loaded"] = min(
            pd.Timestamp(item["date"]) for item in rows
        ).strftime("%Y-%m-%d")
        report["time_end_loaded"] = max(
            pd.Timestamp(item["date"]) for item in rows
        ).strftime("%Y-%m-%d")
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

    mask = s5.apply(_is_satellite_s5_row, axis=1)
    sat = s5.loc[mask].copy()
    sat["satellite_key"] = sat.apply(_satellite_key_from_row, axis=1)
    sat["resolution"] = sat["resolution"].map(_normalize_resolution)
    sat["satellite_location_uid"] = sat["satellite_key"]
    sat["cluster_uid"] = sat["cluster_id"].map(_cluster_uid_from_id)
    keep = [
        "satellite_key",
        "satellite_location_uid",
        "station_id",
        "cluster_id",
        "cluster_uid",
        "source",
        "source_station_id",
        "path",
        "resolution",
    ]
    sat = sat[keep].copy()
    duplicates = sat.duplicated("satellite_key", keep=False)
    if duplicates.any():
        examples = sat.loc[duplicates, ["satellite_key", "station_id", "source", "source_station_id", "resolution"]].head(10)
        raise ValueError(
            "duplicate satellite_key values in s5 CSV; examples: {}".format(
                examples.to_dict("records")
            )
        )
    return sat


def _normalize_v2_linkage_table(linkage: pd.DataFrame, s5_csv: Path) -> pd.DataFrame:
    missing = sorted(REQUIRED_V2_LINKAGE_COLUMNS - set(linkage.columns))
    if missing:
        raise ValueError(
            "s5b v2 linkage CSV missing columns: {}".format(", ".join(missing))
        )

    s5_sat = _load_s5_satellite_lookup(s5_csv)
    work = linkage.copy()
    work["satellite_key"] = work["satellite_key"].map(_clean_text)
    merged = work.merge(
        s5_sat,
        how="left",
        on="satellite_key",
        suffixes=("", "_s5"),
        validate="many_to_one",
    )
    missing_path = merged["path"].fillna("").astype(str).str.strip().eq("")
    if missing_path.any():
        examples = merged.loc[missing_path, ["satellite_key", "satellite_station_id", "satellite_source", "satellite_resolution"]].head(10)
        raise ValueError(
            "could not recover source path for {} v2 linkage row(s); examples: {}".format(
                int(missing_path.sum()),
                examples.to_dict("records"),
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
    normalized["linked_resolution"] = merged["satellite_resolution"].map(_normalize_resolution)
    normalized["link_status"] = merged["link_status"].map(_clean_text).str.lower()
    normalized["link_method"] = merged["link_method"].map(_clean_text)
    normalized["link_quality"] = merged["link_confidence"].map(_clean_text)
    normalized["link_distance_m"] = pd.to_numeric(
        merged.get("representative_point_distance_m", np.nan),
        errors="coerce",
    )
    normalized["link_uparea_log10_error"] = pd.to_numeric(
        merged.get("area_rel_error", np.nan),
        errors="coerce",
    )
    normalized["link_area_rel_error"] = normalized["link_uparea_log10_error"]
    normalized["link_candidate_count"] = pd.to_numeric(
        merged.get("n_valid_candidates", np.nan),
        errors="coerce",
    )
    normalized["unlinked_reason"] = merged.get(
        "rejection_reason",
        pd.Series([""] * len(merged), index=merged.index),
    ).map(_clean_text)
    normalized["s5b_schema"] = "v2"
    normalized["satellite_key"] = merged["satellite_key"].map(_clean_text)
    return normalized


def _validate_linkage_table(linkage: pd.DataFrame, s5_csv: Optional[Path] = None) -> pd.DataFrame:
    if "satellite_key" not in linkage.columns or "satellite_source" not in linkage.columns:
        raise ValueError("expected s5b v2 linkage CSV with satellite_key and satellite_source columns")
    if s5_csv is None:
        raise ValueError("v2 s5b linkage requires --s5-csv")
    linkage = _normalize_v2_linkage_table(linkage, s5_csv)

    missing = sorted(REQUIRED_LINKAGE_COLUMNS - set(linkage.columns))
    if missing:
        raise ValueError(
            "normalized s5b v2 linkage CSV missing columns: {}".format(", ".join(missing))
        )

    work = linkage.copy()
    work["link_status"] = work["link_status"].map(_clean_text).str.lower()
    work["resolution"] = work["resolution"].map(_normalize_resolution)
    work["linked_resolution"] = work["linked_resolution"].map(
        _normalize_resolution
    )
    work["linked_cluster_uid"] = work["linked_cluster_uid"].map(_clean_text)
    work["satellite_location_uid"] = work[
        "satellite_location_uid"
    ].map(_clean_text)
    if "source" in work.columns:
        work["source"] = work["source"].map(_clean_text)
    if "source_station_id" in work.columns:
        work["source_station_id"] = work["source_station_id"].map(_clean_text)
    if "path" in work.columns:
        work["path"] = work["path"].map(_clean_text)
    duplicates = work.duplicated(
        ["satellite_location_uid", "resolution"],
        keep=False,
    )
    if duplicates.any():
        examples = (
            work.loc[
                duplicates,
                ["satellite_location_uid", "resolution"],
            ]
            .drop_duplicates()
            .head(10)
            .to_dict("records")
        )
        raise ValueError(
            "duplicate satellite location/resolution keys: {}".format(
                examples
            )
        )
    return work


def _load_insitu_observations(
    master_nc_path: Path,
    taxonomy: Dict[str, str],
    workers: int,
    target_pairs: set = None,
) -> Tuple[pd.DataFrame, str]:
    """Load in-situ observations from the master NetCDF (s6 merged output).

    When *target_pairs* is provided (a set of (cluster_uid, resolution)
    tuples), the function pre-filters the master NC by reading only the
    ``cluster_uid`` and ``resolution`` arrays first, building a boolean
    mask, and then loading only the matching records.  This avoids reading
    millions of unused rows.
    """
    path = master_nc_path.expanduser().resolve()
    if not path.is_file():
        raise SystemExit(
            "master NC not found: {}".format(path)
        )

    log_progress("Reading master NC (pre-filtered): {}".format(path))

    try:
        import xarray as xr
    except ImportError:
        raise SystemExit("xarray is required to read master NC")

    ds = None
    try:
        ds = base._open_dataset_compat(path)
        record_dim = base._find_record_dim(ds)
        if record_dim is None:
            raise SystemExit("record dimension could not be inferred from master NC")
        n_records = int(ds.sizes[record_dim])

        # --- Pre-filter: read only cluster_uid + resolution, build mask ---
        if target_pairs:
            # cluster_uid is on n_stations dim; map via station_index
            uid_arr = _read_station_var_to_record_space(ds, "cluster_uid", record_dim, n_records)
            res_arr = _read_var_array(ds, "resolution", record_dim)
            mask = np.zeros(n_records, dtype=bool)
            for uid, res in target_pairs:
                # resolution in master NC is stored as int8 codes
                res_code = _resolution_text_to_code(res)
                if res_code is not None:
                    mask |= (
                        (np_clean_text_array(uid_arr) == uid)
                        & (res_arr == res_code)
                    )
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
            n_matched = n_records

        indices = np.where(mask)[0]
        records = pd.DataFrame({"record_index": indices})

        # Read variables only for matching indices
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
            "resolution", "cluster_uid", "cluster_id", "source",
            "source_family", "observation_type", "source_station_uid",
            "source_station_paths", "candidate_path", "is_overlap",
            "river_width_class", "river_width_m", "climate_zone",
        )
        for name in provenance_fields:
            series = _read_masked_series(ds, name, record_dim, indices, n_records)
            if series is not None:
                records[name] = series

        time_series = _read_masked_series(ds, "time", record_dim, indices, n_records)
        if time_series is not None:
            records["time"] = time_series
            time_units = getattr(ds["time"], "units", "days since 1970-01-01")
            records["_time_units"] = time_units

        date_series = _read_masked_series(ds, "date", record_dim, indices, n_records)
        if date_series is not None:
            records["date"] = pd.Series(date_series).astype(str).values

    finally:
        if ds is not None:
            ds.close()

    if records.empty:
        return pd.DataFrame(), "no master NC records match target pairs"

    load_note = "selected master records; pre-filtered to {} target pairs".format(
        len(target_pairs) if target_pairs else n_records
    )

    raw = base.add_observation_type_from_source_attrs(
        records,
        master_nc_path.parent,
        workers=workers,
        progress=log_progress,
    )
    normalized = base.normalize_observation_table(
        raw,
        taxonomy,
        input_mode="selected_master",
    )
    insitu = normalized[
        normalized["source_family"].eq("in_situ")
    ].copy()
    return insitu, load_note


def _read_var_array(ds, name: str, record_dim: str) -> np.ndarray:
    """Read a 1-D variable along *record_dim* as a plain numpy array."""
    if name not in ds.variables:
        raise KeyError("variable {} not found".format(name))
    da = ds[name]
    if record_dim not in da.dims:
        raise ValueError(
            "record dimension {} not in {} dims {}".format(record_dim, name, da.dims)
        )
    if da.dims == (record_dim,):
        return np.asarray(da.values).reshape(-1)
    # 2-D along record_dim: use the base module's decoding
    values = np.asarray(da.values)
    if values.dtype.kind in ("S", "U") and values.ndim == 2:
        return np.array([
            b"".join(values[i].reshape(-1)).decode("utf-8", errors="ignore").strip()
            for i in range(values.shape[0])
        ])
    return values.reshape(values.shape[0], -1)[:, 0]



def _read_station_var_to_record_space(
    ds, name: str, record_dim: str, n_records: int
) -> np.ndarray:
    """Read a station-dimension variable and expand to record space via station_index.

    The master NC stores station-level metadata (cluster_uid, cluster_id, etc.)
    on an ``n_stations`` dimension while observation records are on ``n_records``.
    ``station_index`` maps each record to its station.  This helper follows that
    indirection and returns a record-length array.
    """
    if name not in ds.variables:
        raise KeyError("variable {} not found".format(name))

    # station_index: float64 array of length n_records, 0-based into n_stations
    station_idx = np.asarray(ds["station_index"].values).reshape(-1).astype(float)

    # station-level values
    raw_vals = np.asarray(ds[name].values)

    # Decode bytes/string arrays to plain str (object dtype)
    if raw_vals.dtype.kind in ("S", "U"):
        clean = np.array(
            [
                (
                    v.decode("utf-8", errors="ignore").strip()
                    if isinstance(v, bytes)
                    else str(v).strip()
                )
                for v in raw_vals.reshape(-1)
            ],
            dtype=object,
        )
    else:
        clean = np.asarray(raw_vals).reshape(-1)

    # Map to record space: records with invalid index get an empty string
    result = np.full(n_records, "", dtype=object)
    valid = (
        ~np.isnan(station_idx)
        & (station_idx >= 0)
        & (station_idx < len(clean))
    )
    result[valid] = clean[station_idx[valid].astype(int)]
    return result



def _read_masked_series(
    ds, name: str, record_dim: str, indices: np.ndarray, n_records: int
):
    """Read variable *name* at *indices* along *record_dim*.

    Handles both 1-D (record_dim,) and 2-D (record_dim, max_strlen)
    variables, including NetCDF character arrays.
    """
    if name not in ds.variables:
        return None
    da = ds[name]
    if record_dim not in da.dims:
        # Try station-dimension lookup via station_index
        if "station_index" in ds.variables:
            full = _read_station_var_to_record_space(ds, name, record_dim, n_records)
            return pd.Series(full[indices])
        return None

    dims = tuple(da.dims)

    if dims == (record_dim,):
        # 1-D along record dimension
        arr = np.asarray(da.values)
        result = arr[indices]
        if result.dtype.kind in ("S", "U"):
            return pd.Series([
                v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
                for v in result.reshape(-1)
            ])
        # Use masked array to handle fill values
        try:
            filled = np.ma.asarray(result).filled(np.nan)
        except Exception:
            filled = result
        return pd.Series(filled.reshape(-1))

    if len(dims) == 2 and dims[0] == record_dim:
        # 2-D: record_dim x something (e.g. char array for strings)
        arr = np.asarray(da.values)
        if arr.dtype.kind in ("S", "U"):
            result = [
                b"".join(arr[i].reshape(-1)).decode("utf-8", errors="ignore").strip()
                for i in indices
            ]
            return pd.Series(result)
        # Numeric 2-D: take first column only
        sub = arr[indices, :].reshape(len(indices), -1)
        try:
            filled = np.ma.asarray(sub[:, 0]).filled(np.nan)
        except Exception:
            filled = sub[:, 0]
        return pd.Series(filled)

    return None


def np_clean_text_array(arr: np.ndarray) -> np.ndarray:
    """Vectorized text cleaning for a numpy string/object array."""
    if arr.dtype.kind in ("S", "U"):
        result = np.array([
            v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
            for v in arr.reshape(-1)
        ])
    else:
        result = np.array([str(v) for v in arr.reshape(-1)])
    result = np.char.strip(result)
    mask = np.char.lower(result) == "nan"
    result[mask] = ""
    mask2 = np.char.lower(result) == "none"
    result[mask2] = ""
    mask3 = np.char.lower(result) == "nat"
    result[mask3] = ""
    return result


def _resolution_text_to_code(res: str):
    """Map a normalized resolution string to the int8 code used in master NC."""
    mapping = {"daily": 0, "monthly": 1, "annual": 2, "climatology": 3}
    return mapping.get(res)


def _load_station_catalog_set(catalog_csv: Path) -> set:
    """Load (cluster_uid, resolution) pairs from the station catalog CSV.

    Returns a set of (cluster_uid, resolution) tuples identifying every
    cluster/resolution pair that belongs to the formal release matrix.
    """
    path = catalog_csv.expanduser().resolve()
    if not path.is_file():
        raise SystemExit(
            "station catalog CSV not found: {}".format(path)
        )
    cat = pd.read_csv(path, low_memory=False)
    required = {"cluster_uid", "resolution"}
    missing = sorted(required - set(cat.columns))
    if missing:
        raise ValueError(
            "station_catalog.csv missing required columns: {}".format(
                ", ".join(missing)
            )
        )
    pairs = set()
    for _, row in cat.iterrows():
        uid = _clean_text(row.get("cluster_uid", ""))
        res = _normalize_resolution(row.get("resolution", ""))
        if uid and res:
            pairs.add((uid, res))
    log_progress(
        "Station catalog loaded: {} unique (cluster_uid, resolution) pairs".format(
            len(pairs)
        )
    )
    return pairs


def _filter_insitu_by_targets(
    insitu: pd.DataFrame,
    target_pairs: set,
) -> pd.DataFrame:
    """Keep only insitu rows whose (cluster_uid, resolution) is in *target_pairs*."""
    if insitu.empty:
        return insitu
    insitu_uid = insitu["cluster_uid"].map(_clean_text)
    insitu_res = insitu["resolution"].map(_normalize_resolution)
    insitu_pairs = pd.Series(
        list(zip(insitu_uid, insitu_res)),
        index=insitu.index,
    )
    mask = insitu_pairs.isin(target_pairs)
    n_before = len(insitu)
    result = insitu.loc[mask].copy()
    log_progress(
        "Insitu filter: kept {}/{} rows ({} unique cluster/resolution pairs)".format(
            len(result),
            n_before,
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

    max_days = max(
        base.WINDOW_DAYS[window] for window in windows
    ) if windows else 0

    work = insitu.copy()
    work["cluster_uid"] = work["cluster_uid"].map(_clean_text)
    work["resolution"] = work["resolution"].map(_normalize_resolution)
    work["time"] = pd.to_datetime(work["time"], errors="coerce").dt.floor("D")
    work = work[
        work["cluster_uid"].ne("")
        & work["resolution"].isin(base.LINKED_RESOLUTIONS)
        & work["time"].notna()
    ]

    ranges: Dict[
        Tuple[str, str],
        Tuple[pd.Timestamp, pd.Timestamp],
    ] = {}
    for (cluster_uid, resolution), group in work.groupby(
        ["cluster_uid", "resolution"],
        sort=True,
    ):
        ranges[(cluster_uid, resolution)] = (
            pd.Timestamp(group["time"].min())
            - pd.Timedelta(days=max_days),
            pd.Timestamp(group["time"].max())
            + pd.Timedelta(days=max_days),
        )
    return ranges


def _load_satellite_observations_from_s5b(
    linkage: pd.DataFrame,
    linkage_csv: Path,
    source_root: Path,
    insitu_windows: Dict[
        Tuple[str, str],
        Tuple[pd.Timestamp, pd.Timestamp],
    ],
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
        linked = linked[
            linked["source"].map(_clean_text).str.lower().isin(allowed)
        ].copy()

    linkage_summary = (
        linkage.assign(
            source=linkage["source"].map(_clean_text),
            unlinked_reason=linkage.get(
                "unlinked_reason",
                pd.Series([""] * len(linkage)),
            ).map(_clean_text),
        )
        .groupby(
            ["source", "link_status", "unlinked_reason"],
            dropna=False,
        )
        .size()
        .reset_index(name="n_locations")
        .sort_values(
            ["source", "link_status", "n_locations"],
            ascending=[True, True, False],
        )
    )

    tasks = []
    pre_reports: List[Dict[str, object]] = []
    for ordinal, row in enumerate(
        linked.sort_values(
            [
                "linked_cluster_uid",
                "linked_resolution",
                "satellite_location_uid",
            ],
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
                    "satellite_location_uid": _clean_text(
                        row.get("satellite_location_uid", "")
                    ),
                    "source": _clean_text(row.get("source", "")),
                    "source_station_id": _clean_text(
                        row.get("source_station_id", "")
                    ),
                    "linked_cluster_uid": key[0],
                    "linked_resolution": key[1],
                    "path_from_s5b": _clean_text(row.get("path", "")),
                    "resolved_path": "",
                    "status": "no_insitu_cluster_resolution",
                    "message": "",
                    "n_time_steps": 0,
                    "n_time_overlap": 0,
                    "n_value_rows": 0,
                    "time_start_loaded": "",
                    "time_end_loaded": "",
                }
            )
            continue

        tasks.append(
            (
                ordinal,
                row,
                str(source_root),
                str(linkage_csv.parent),
                time_range[0].isoformat(),
                time_range[1].isoformat(),
            )
        )

    log_progress(
        "Reading {} linked satellite source files with {} worker(s)".format(
            len(tasks),
            max(1, int(workers or 1)),
        )
    )

    workers = max(1, int(workers or 1))
    if workers == 1 or len(tasks) <= 1:
        results = [_read_one_satellite_source(task) for task in tasks]
    else:
        chunksize = max(
            1,
            min(
                20,
                int(math.ceil(len(tasks) / float(workers * 8))),
            ),
        )
        with ProcessPoolExecutor(max_workers=workers) as executor:
            results = list(
                executor.map(
                    _read_one_satellite_source,
                    tasks,
                    chunksize=chunksize,
                )
            )

    raw_rows: List[Dict[str, object]] = []
    reports = list(pre_reports)
    for _, rows, report in sorted(results, key=lambda item: item[0]):
        raw_rows.extend(rows)
        reports.append(report)

    raw = pd.DataFrame(raw_rows)
    report_df = pd.DataFrame(reports)

    satellite = base.normalize_observation_table(
        raw,
        taxonomy,
        input_mode="s5b_linkage_csv",
    )
    if satellite.empty:
        return satellite, report_df, linkage_summary

    satellite = satellite[
        satellite["source_family"].eq("satellite")
    ].copy()

    return satellite, report_df, linkage_summary


def _attach_linkage_metadata(
    pairs: pd.DataFrame,
    linkage: pd.DataFrame,
) -> pd.DataFrame:
    if pairs.empty:
        return pairs

    available = [
        column for column in LINK_META_COLUMNS if column in linkage.columns
    ]
    metadata = linkage[
        linkage["link_status"].map(_clean_text).str.lower().eq("linked")
    ][available].copy()
    metadata = metadata.drop_duplicates("satellite_location_uid")

    rename = {
        "satellite_key": "s5b_satellite_key",
        "s5b_schema": "s5b_schema",
        "source": "s5b_satellite_source",
        "source_station_id": "s5b_source_station_id",
        "path": "s5b_source_path",
        "linked_cluster_id": "s5b_linked_cluster_id",
        "linked_cluster_uid": "s5b_linked_cluster_uid",
        "linked_resolution": "s5b_linked_resolution",
    }
    metadata = metadata.rename(columns=rename)

    return pairs.merge(
        metadata,
        how="left",
        left_on="satellite_source_station_uid",
        right_on="satellite_location_uid",
    )


def run_validation(
    master_nc_path: Path,
    station_catalog_csv: Path,
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
    master_nc_path = master_nc_path.expanduser().resolve()
    station_catalog_csv = station_catalog_csv.expanduser().resolve()
    linkage_csv = linkage_csv.expanduser().resolve()
    s5_csv = s5_csv.expanduser().resolve()
    source_root = source_root.expanduser().resolve()
    out_dir = out_dir.expanduser().resolve()

    if not master_nc_path.is_file():
        raise SystemExit(
            "master NC does not exist: {}".format(master_nc_path)
        )
    if not station_catalog_csv.is_file():
        raise SystemExit(
            "station catalog CSV does not exist: {}".format(station_catalog_csv)
        )
    if not linkage_csv.is_file():
        raise SystemExit(
            "s5b linkage CSV does not exist: {}".format(linkage_csv)
        )

    unknown_windows = [
        window for window in windows
        if window not in base.WINDOW_DAYS
    ]
    if unknown_windows:
        raise SystemExit(
            "unknown pairing windows: {}".format(
                ", ".join(unknown_windows)
            )
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    log_progress("Starting s11b validation from s5b linkage CSV")
    log_progress("Master NC: {}".format(master_nc_path))
    log_progress("Station catalog CSV: {}".format(station_catalog_csv))
    log_progress("Linkage CSV: {}".format(linkage_csv))
    log_progress("S5 CSV: {}".format(s5_csv))
    log_progress("Satellite source root: {}".format(source_root))
    log_progress("Output dir: {}".format(out_dir))

    taxonomy = base.load_source_taxonomy(source_taxonomy_csv)
    external_attrs = base._load_external_attributes(
        external_attributes_csv
    )

    linkage = _validate_linkage_table(
        pd.read_csv(linkage_csv, low_memory=False),
        s5_csv=s5_csv,
    )
    log_progress(
        "s5b rows={}, linked={}, unlinked={}".format(
            len(linkage),
            int(linkage["link_status"].eq("linked").sum()),
            int(linkage["link_status"].eq("unlinked").sum()),
        )
    )

    # Load station catalog and filter linkage to formal matrix members only.
    catalog_pairs = _load_station_catalog_set(station_catalog_csv)
    linked_before = int(linkage["link_status"].eq("linked").sum())

    # Build a mask for linked rows whose target is in the catalog.
    linkage_uid = linkage["linked_cluster_uid"].map(_clean_text)
    linkage_res = linkage["linked_resolution"].map(_normalize_resolution)
    linkage_pairs = pd.Series(
        list(zip(linkage_uid, linkage_res)),
        index=linkage.index,
    )
    in_catalog = linkage_pairs.isin(catalog_pairs)
    # Keep: unlinked rows OR linked rows whose target is in the catalog.
    keep_mask = linkage["link_status"].ne("linked") | in_catalog
    n_excluded = int((~keep_mask).sum())
    linkage = linkage.loc[keep_mask].copy()

    linked_after = int(linkage["link_status"].eq("linked").sum())
    log_progress(
        "Catalog filter: linked {} -> {} ({} rows excluded)".format(
            linked_before, linked_after, linked_before - linked_after,
        )
    )

    # Build valid pairs set from the filtered linked rows only.
    target_pairs = set()
    linked_rows = linkage[linkage["link_status"].eq("linked")]
    for _, row in linked_rows.iterrows():
        uid = _clean_text(row.get("linked_cluster_uid", ""))
        res = _normalize_resolution(row.get("linked_resolution", ""))
        if uid and res:
            target_pairs.add((uid, res))
    log_progress(
        "Valid linkage target pairs (in catalog): {}".format(len(target_pairs))
    )

    # Always read insitu from master NC (s6 merged output).
    input_path = master_nc_path
    input_mode = "selected_master"
    insitu, load_note = _load_insitu_observations(
        master_nc_path=master_nc_path,
        taxonomy=taxonomy,
        workers=workers,
        target_pairs=target_pairs,
    )
    log_progress(
        "Master NC insitu observations (all): {}".format(len(insitu))
    )

    # Filter insitu to only target pairs from the catalog-matched linkage.
    insitu = _filter_insitu_by_targets(insitu, target_pairs)
    log_progress(
        "Filtered insitu observations (catalog targets only): {}".format(len(insitu))
    )

    insitu_windows = _build_insitu_windows(insitu, windows)
    log_progress(
        "In-situ cluster/resolution windows: {}".format(
            len(insitu_windows)
        )
    )

    satellite, load_report, linkage_summary = (
        _load_satellite_observations_from_s5b(
            linkage=linkage,
            linkage_csv=linkage_csv,
            source_root=source_root,
            insitu_windows=insitu_windows,
            taxonomy=taxonomy,
            workers=workers,
            sources=sources,
        )
    )
    log_progress(
        "Normalized satellite observations: {}".format(len(satellite))
    )

    if strict_source_files and not load_report.empty:
        failed = load_report[
            load_report["status"].isin(
                ["missing_file", "read_error"]
            )
        ]
        if not failed.empty:
            raise SystemExit(
                "{} linked satellite files failed to load; "
                "see validation_satellite_source_load_report.csv".format(
                    len(failed)
                )
            )

    observations = pd.concat(
        [insitu, satellite],
        ignore_index=True,
        sort=False,
    )

    pair_mode = "s5b_linkage_csv+selected_master_catalog_filtered"
    pairs = base.pair_satellite_insitu_records(
        observations,
        windows=windows,
        input_mode=pair_mode,
        workers=workers,
        progress=log_progress,
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
    load_report_path = (
        out_dir / "validation_satellite_source_load_report.csv"
    )
    linkage_summary_path = (
        out_dir / "validation_s5b_linkage_summary.csv"
    )
    summary_path = (
        out_dir / "validation_satellite_insitu_summary.md"
    )

    pairs.to_csv(pair_path, index=False)
    metrics.to_csv(metric_path, index=False)
    load_report.to_csv(load_report_path, index=False)
    linkage_summary.to_csv(linkage_summary_path, index=False)

    generated_outputs: List[Tuple[str, str]] = [
        (pair_path.name, "generated"),
        (metric_path.name, "generated"),
        (load_report_path.name, "generated"),
        (linkage_summary_path.name, "generated"),
    ]

    if write_plots:
        generated_outputs.extend(
            base.write_figures(
                pairs,
                metrics,
                out_dir,
                figure_variables=figure_variables,
            )
        )
    else:
        generated_outputs.extend(
            [
                (
                    "figures/satellite_insitu_scatter_by_window_SSC.png",
                    "skipped: --no-figures",
                ),
                (
                    "figures/satellite_insitu_residual_by_ssc_bin.png",
                    "skipped: --no-figures",
                ),
                (
                    "figures/satellite_insitu_metric_heatmap.png",
                    "skipped: --no-figures",
                ),
            ]
        )

    generated_outputs.append((summary_path.name, "generated"))
    base.write_summary(
        summary_path,
        input_path,
        pair_mode,
        "{}; satellite records loaded directly from s5b source paths; insitu filtered to station_catalog pairs".format(
            load_note
        ),
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
        handle.write(
            "- Master NC: `{}`.\n".format(master_nc_path)
        )
        handle.write(
            "- Station catalog CSV: `{}`.\n".format(station_catalog_csv)
        )
        handle.write(
            "- Linkage CSV: `{}`.\n".format(linkage_csv)
        )
        handle.write(
            "- Station catalog pairs loaded: `{}`.\n".format(len(catalog_pairs))
        )
        handle.write(
            "- Linkage linked rows before/after catalog filter: `{}` / `{}`.\n".format(
                linked_before, linked_after,
            )
        )
        handle.write(
            "- S5 CSV used for v2 path recovery: `{}`.\n".format(s5_csv)
        )
        handle.write(
            "- s5b linkage schema counts: `{}`.\n".format(
                linkage.get("s5b_schema", pd.Series(dtype=object)).value_counts(dropna=False).to_dict()
            )
        )
        handle.write(
            "- Satellite source root: `{}`.\n".format(source_root)
        )
        handle.write(
            "- Total s5b rows: {}; linked rows: {}.\n".format(
                len(linkage),
                int(linkage["link_status"].eq("linked").sum()),
            )
        )
        handle.write(
            "- Satellite source-load status counts: `{}`.\n".format(
                status_counts
            )
        )
        handle.write(
            "- Satellite observation rows loaded: {}.\n".format(
                len(satellite)
            )
        )
        handle.write(
            "- Final pair rows: {}; metric rows: {}.\n".format(
                len(pairs),
                len(metrics),
            )
        )

    log_progress(
        "Complete: pairs={}, metric_rows={}, satellite_rows={}".format(
            len(pairs),
            len(metrics),
            len(satellite),
        )
    )


def parse_args(
    argv: Optional[Sequence[str]] = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--master-nc",
        default=str(DEFAULT_MASTER_NC),
        help="Path to the master NetCDF (s6_basin_merged_all.nc).",
    )
    parser.add_argument(
        "--station-catalog-csv",
        default=str(DEFAULT_STATION_CATALOG_CSV),
        help="Path to the station catalog CSV (s7_cluster_resolution_catalog.csv).",
    )
    parser.add_argument(
        "--s5b-linkage-csv",
        default=str(DEFAULT_LINKAGE_CSV),
        help="s5b satellite-to-main-cluster linkage CSV.",
    )
    parser.add_argument(
        "--s5-csv",
        default=str(DEFAULT_S5_CSV),
        help=(
            "s5 basin-clustered stations CSV. Required when --s5b-linkage-csv "
            "points to v2 links because v2 stores source paths in the s5 table."
        ),
    )
    parser.add_argument(
        "--source-root",
        default=str(DEFAULT_SOURCE_ROOT),
        help=(
            "Root corresponding to relative paths in the s5b path column; "
            "normally output_resolution_organized."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Validation output directory.",
    )
    parser.add_argument(
        "--source-taxonomy-csv",
        help="Optional source-family taxonomy override CSV.",
    )
    parser.add_argument(
        "--external-attributes-csv",
        help="Optional cluster width/climate attributes CSV.",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        default=["exact", "pm1d", "pm2d"],
        choices=sorted(base.WINDOW_DAYS),
    )
    parser.add_argument(
        "--high-turbidity-ssc",
        type=float,
        default=base.DEFAULT_HIGH_TURBIDITY_SSC,
    )
    parser.add_argument(
        "--ssc-bin-edges",
        default=",".join(
            base._format_edge(value)
            for value in base.DEFAULT_SSC_BIN_EDGES
        ),
    )
    parser.add_argument(
        "--figure-variables",
        nargs="+",
        default=["SSC"],
        choices=list(base.VARIABLES),
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        help="Optional satellite source filter, for example RiverSed Dethier.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=base.DEFAULT_WORKERS,
    )
    parser.add_argument(
        "--no-figures",
        action="store_true",
    )
    parser.add_argument(
        "--strict-source-files",
        action="store_true",
        help="Fail if any linked source NetCDF is missing or unreadable.",
    )
    return parser.parse_args(argv)


def main(
    argv: Optional[Sequence[str]] = None,
) -> None:
    args = parse_args(argv)
    run_validation(
        master_nc_path=Path(args.master_nc),
        station_catalog_csv=Path(args.station_catalog_csv),
        linkage_csv=Path(args.s5b_linkage_csv),
        s5_csv=Path(args.s5_csv),
        source_root=Path(args.source_root),
        out_dir=Path(args.out_dir),
        source_taxonomy_csv=(
            Path(args.source_taxonomy_csv).expanduser().resolve()
            if args.source_taxonomy_csv
            else None
        ),
        external_attributes_csv=(
            Path(args.external_attributes_csv).expanduser().resolve()
            if args.external_attributes_csv
            else None
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
    run_validation(
        master_nc_path=DEFAULT_MASTER_NC,
        station_catalog_csv=DEFAULT_STATION_CATALOG_CSV,
        linkage_csv=DEFAULT_LINKAGE_CSV,
        source_root=DEFAULT_SOURCE_ROOT,
        out_dir=DEFAULT_OUT_DIR,
        s5_csv=DEFAULT_S5_CSV,
    )
