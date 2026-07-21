#!/usr/bin/env python3
"""Validate satellite source records against main station-reference records using s5b linkage CSV.

This script is a companion to ``s11_satellite_insitu_validation.py``.

Key difference
--------------
The satellite side is not read from ``sed_reference_satellite.nc``. Instead, this
script treats ``s5b_satellite_main_cluster_linkage.csv`` as the authoritative
linkage catalogue and reads satellite observations directly from the source
NetCDF files referenced by its ``path`` column.

The in-situ side follows the original s11 logic:
1. prefer a candidate sidecar when available;
2. otherwise use selected records from ``sed_reference_master.nc``.

The existing s11 functions are reused for normalization, temporal pairing,
stratification, metrics, figures, and the standard summary.
"""

from __future__ import annotations

import argparse
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


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_R_ROOT = Path(
    os.environ.get("OUTPUT_R_ROOT", str(PROJECT_DIR.parent))
).expanduser().resolve()

DEFAULT_LINKAGE_CSV = (
    OUTPUT_R_ROOT
    / "scripts_basin_test/output/s5b_satellite_main_cluster_linkage.csv"
)
DEFAULT_SOURCE_ROOT = (OUTPUT_R_ROOT / "../output_resolution_organized").resolve()
DEFAULT_RELEASE_DIR = (
    OUTPUT_R_ROOT
    / "scripts_basin_test/output/sed_reference_release"
)
DEFAULT_OUT_DIR = (
    OUTPUT_R_ROOT
    / "scripts_basin_test/output/validation_results_s5b"
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

LINK_META_COLUMNS = [
    "satellite_location_uid",
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


def _validate_linkage_table(linkage: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(REQUIRED_LINKAGE_COLUMNS - set(linkage.columns))
    if missing:
        raise ValueError(
            "s5b linkage CSV missing columns: {}".format(", ".join(missing))
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
    release_dir: Path,
    candidate_sidecar: Optional[Path],
    allow_master_fallback: bool,
    taxonomy: Dict[str, str],
    workers: int,
) -> Tuple[pd.DataFrame, Optional[Path], str, str]:
    raw = pd.DataFrame()
    input_path: Optional[Path] = None
    input_mode = ""
    load_note = ""

    sidecar = base._find_candidate_sidecar(release_dir, candidate_sidecar)
    if sidecar is not None:
        raw, input_path, input_mode = base.load_observations_from_candidate_sidecar(
            release_dir,
            sidecar,
            progress=log_progress,
        )
        load_note = "candidate sidecar loaded"

    if raw.empty:
        if not allow_master_fallback:
            raise RuntimeError(
                "candidate sidecar is unavailable or empty and master fallback is disabled"
            )
        raw, load_note = base.load_observations_from_master_nc(
            release_dir,
            progress=log_progress,
        )
        input_path = release_dir / base.MASTER_FILE
        input_mode = "selected_master"

    raw = base.add_observation_type_from_source_attrs(
        raw,
        release_dir,
        workers=workers,
        progress=log_progress,
    )
    normalized = base.normalize_observation_table(
        raw,
        taxonomy,
        input_mode=input_mode,
    )
    insitu = normalized[
        normalized["source_family"].eq("in_situ")
    ].copy()
    return insitu, input_path, input_mode, load_note


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
    release_dir: Path,
    linkage_csv: Path,
    source_root: Path,
    out_dir: Path,
    candidate_sidecar: Optional[Path] = None,
    source_taxonomy_csv: Optional[Path] = None,
    external_attributes_csv: Optional[Path] = None,
    allow_master_fallback: bool = True,
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
    source_root = source_root.expanduser().resolve()
    out_dir = out_dir.expanduser().resolve()

    if not release_dir.is_dir():
        raise SystemExit(
            "release-dir does not exist: {}".format(release_dir)
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
    log_progress("Release dir: {}".format(release_dir))
    log_progress("Linkage CSV: {}".format(linkage_csv))
    log_progress("Satellite source root: {}".format(source_root))
    log_progress("Output dir: {}".format(out_dir))

    taxonomy = base.load_source_taxonomy(source_taxonomy_csv)
    external_attrs = base._load_external_attributes(
        external_attributes_csv
    )

    linkage = _validate_linkage_table(
        pd.read_csv(linkage_csv, low_memory=False)
    )
    log_progress(
        "s5b rows={}, linked={}, unlinked={}".format(
            len(linkage),
            int(linkage["link_status"].eq("linked").sum()),
            int(linkage["link_status"].eq("unlinked").sum()),
        )
    )

    insitu, input_path, input_mode, load_note = _load_insitu_observations(
        release_dir=release_dir,
        candidate_sidecar=candidate_sidecar,
        allow_master_fallback=allow_master_fallback,
        taxonomy=taxonomy,
        workers=workers,
    )
    log_progress(
        "Normalized in-situ observations: {}".format(len(insitu))
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

    pair_mode = "s5b_linkage_csv+{}".format(input_mode)
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
        "{}; satellite records loaded directly from s5b source paths".format(
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
            "- Linkage CSV: `{}`.\n".format(linkage_csv)
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
        "--release-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Directory containing sed_reference_master.nc and catalogues.",
    )
    parser.add_argument(
        "--s5b-linkage-csv",
        default=str(DEFAULT_LINKAGE_CSV),
        help="s5b satellite-to-main-cluster linkage CSV.",
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
        "--candidate-sidecar",
        help=(
            "Optional in-situ candidate sidecar. When omitted, known release "
            "sidecar names are tried before master fallback."
        ),
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
        "--no-master-fallback",
        action="store_true",
        help="Fail when no candidate sidecar is available.",
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
        release_dir=Path(args.release_dir),
        linkage_csv=Path(args.s5b_linkage_csv),
        source_root=Path(args.source_root),
        out_dir=Path(args.out_dir),
        candidate_sidecar=(
            Path(args.candidate_sidecar).expanduser().resolve()
            if args.candidate_sidecar
            else None
        ),
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
        allow_master_fallback=not args.no_master_fallback,
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

