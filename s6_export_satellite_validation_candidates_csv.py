#!/usr/bin/env python3
"""Export a flat satellite validation candidate CSV sidecar.

This script is intentionally separate from the NetCDF exporter so it can be run
once after ``s6_export_satellite_validation_to_nc.py`` and reused by downstream
validation.  The output mirrors the candidate-level columns used by
``validate/s11_satellite_insitu_validation.py`` so s11 can read a CSV sidecar
instead of scanning the large satellite validation NetCDF on every run.
"""

import argparse
import gzip
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from pipeline_paths import (
    RELEASE_DATASET_DIR,
    S6_SATELLITE_VALIDATION_NC,
    get_output_r_root,
)

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - handled at runtime
    nc4 = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
DEFAULT_INPUT_NC = PROJECT_ROOT / S6_SATELLITE_VALIDATION_NC
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "scripts_basin_test/output/s6_satellite_validation_candidates.csv.gz"
DEFAULT_RELEASE_CSV = PROJECT_ROOT / RELEASE_DATASET_DIR / "sed_reference_satellite_candidates.csv.gz"
DEFAULT_CHUNK_SIZE = 1000000
VARIABLES = ("Q", "SSC", "SSL")

SATELLITE_CANDIDATE_COLUMNS = [
    "record_index",
    "cluster_uid",
    "cluster_id",
    "resolution",
    "time",
    "date",
    "source",
    "source_family",
    "observation_type",
    "source_station_uid",
    "satellite_station_uid",
    "source_station_index",
    "source_station_native_id",
    "source_station_name",
    "source_station_river_name",
    "source_station_paths",
    "source_station_lat",
    "source_station_lon",
    "candidate_path",
    "validation_only",
    "merge_policy",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
    "Q_units",
    "SSC_units",
    "SSL_units",
    "has_Q",
    "has_SSC",
    "has_SSL",
    "method_notes",
    "assumptions",
]

METHOD_NOTES = (
    "satellite validation candidate-level values flattened from "
    "s6_satellite_validation_only.nc for fast downstream s11 validation"
)
ASSUMPTIONS = (
    "satellite rows remain validation_only and excluded from main merged value "
    "selection; source_station_uid is the stable satellite_station_uid"
)


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


def _read_text_var(ds, name: str, size: int) -> List[str]:
    if name not in ds.variables:
        return [""] * int(size)
    raw = np.asarray(ds.variables[name][:], dtype=object).reshape(-1)
    if len(raw) < size:
        raw = np.concatenate([raw, np.asarray([""] * (size - len(raw)), dtype=object)])
    return [_clean_text(item) for item in raw[:size]]


def _read_numeric_var(ds, name: str, start: int, stop: int, fill=np.nan) -> np.ndarray:
    if name not in ds.variables:
        return np.full(stop - start, fill)
    arr = np.ma.asarray(ds.variables[name][start:stop]).reshape(-1)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(fill)
    arr = np.asarray(arr)
    if arr.dtype.kind in ("f", "i", "u"):
        arr = arr.astype(float, copy=False)
        arr[arr == -9999.0] = np.nan
        arr[arr >= 1.0e19] = np.nan
    return arr


def _station_metadata(ds) -> List[Dict[str, object]]:
    if "n_satellite_stations" not in ds.dimensions:
        return []
    n_stations = len(ds.dimensions["n_satellite_stations"])
    text = lambda name: _read_text_var(ds, name, n_stations)

    station_uids = text("satellite_station_uid")
    cluster_uids = text("cluster_uid")
    sources = text("source")
    families = text("source_family")
    native_ids = text("source_station_native_id")
    station_names = text("station_name")
    river_names = text("river_name")
    resolutions = text("station_resolution")
    candidate_paths = text("candidate_path")
    resolved_paths = text("resolved_candidate_path")
    merge_policies = text("merge_policy")

    def station_num(name: str, default=np.nan) -> np.ndarray:
        if name not in ds.variables:
            return np.full(n_stations, default)
        arr = np.ma.asarray(ds.variables[name][:]).reshape(-1)
        if np.ma.isMaskedArray(arr):
            arr = arr.filled(default)
        arr = np.asarray(arr)
        if len(arr) < n_stations:
            arr = np.concatenate([arr, np.full(n_stations - len(arr), default)])
        return arr[:n_stations]

    cluster_ids = station_num("cluster_id_station", -1).astype(int, copy=False)
    source_station_indices = station_num("source_station_index", -1).astype(int, copy=False)
    lats = station_num("lat", np.nan).astype(float, copy=False)
    lons = station_num("lon", np.nan).astype(float, copy=False)
    lats[lats == -9999.0] = np.nan
    lons[lons == -9999.0] = np.nan

    metadata = []
    for idx in range(n_stations):
        satellite_uid = station_uids[idx] or "SAT{:06d}".format(idx)
        source_family = families[idx] or "satellite"
        metadata.append(
            {
                "cluster_uid": cluster_uids[idx],
                "cluster_id": int(cluster_ids[idx]),
                "resolution": resolutions[idx],
                "source": sources[idx],
                "source_family": source_family,
                "observation_type": "Satellite",
                "source_station_uid": satellite_uid,
                "satellite_station_uid": satellite_uid,
                "source_station_index": int(source_station_indices[idx]),
                "source_station_native_id": native_ids[idx],
                "source_station_name": station_names[idx],
                "source_station_river_name": river_names[idx],
                "source_station_paths": resolved_paths[idx] or candidate_paths[idx],
                "source_station_lat": float(lats[idx]) if np.isfinite(lats[idx]) else np.nan,
                "source_station_lon": float(lons[idx]) if np.isfinite(lons[idx]) else np.nan,
                "candidate_path": candidate_paths[idx],
                "validation_only": 1,
                "merge_policy": merge_policies[idx] or "validation_only",
            }
        )
    return metadata


def _chunk_to_frame(ds, station_meta: Sequence[Dict[str, object]], start: int, stop: int) -> pd.DataFrame:
    n = stop - start
    if n <= 0:
        return pd.DataFrame(columns=SATELLITE_CANDIDATE_COLUMNS)

    station_idx = _read_numeric_var(ds, "satellite_station_index", start, stop, fill=-1).astype(int, copy=False)
    valid_station = (station_idx >= 0) & (station_idx < len(station_meta))
    if not np.any(valid_station):
        return pd.DataFrame(columns=SATELLITE_CANDIDATE_COLUMNS)

    q = _read_numeric_var(ds, "Q", start, stop)
    ssc = _read_numeric_var(ds, "SSC", start, stop)
    ssl = _read_numeric_var(ds, "SSL", start, stop)
    finite_any = np.isfinite(q) | np.isfinite(ssc) | np.isfinite(ssl)
    keep = valid_station & finite_any
    if not np.any(keep):
        return pd.DataFrame(columns=SATELLITE_CANDIDATE_COLUMNS)

    positions = np.flatnonzero(keep)
    kept_station_idx = station_idx[positions]
    metas = [station_meta[int(idx)] for idx in kept_station_idx]

    def from_meta(name: str):
        return [meta.get(name, "") for meta in metas]

    time_values = _read_numeric_var(ds, "time", start, stop)[positions]
    dates = _read_text_var(ds, "date", len(ds.dimensions["n_satellite_records"]))[start:stop]
    dates = [dates[int(pos)] if int(pos) < len(dates) else "" for pos in positions]
    record_resolution = _read_text_var(ds, "resolution", len(ds.dimensions["n_satellite_records"]))[start:stop]
    record_resolution = [record_resolution[int(pos)] if int(pos) < len(record_resolution) else "" for pos in positions]

    q_flag = _read_numeric_var(ds, "Q_flag", start, stop, fill=9)[positions]
    ssc_flag = _read_numeric_var(ds, "SSC_flag", start, stop, fill=9)[positions]
    ssl_flag = _read_numeric_var(ds, "SSL_flag", start, stop, fill=9)[positions]

    q_units = _clean_text(getattr(ds.variables.get("Q"), "units", "")) if "Q" in ds.variables else ""
    ssc_units = _clean_text(getattr(ds.variables.get("SSC"), "units", "")) if "SSC" in ds.variables else ""
    ssl_units = _clean_text(getattr(ds.variables.get("SSL"), "units", "")) if "SSL" in ds.variables else ""

    frame = pd.DataFrame(
        {
            "record_index": ["satellite:{}".format(start + int(pos)) for pos in positions],
            "cluster_uid": from_meta("cluster_uid"),
            "cluster_id": from_meta("cluster_id"),
            "resolution": [res or meta.get("resolution", "") for res, meta in zip(record_resolution, metas)],
            "time": time_values,
            "date": dates,
            "source": from_meta("source"),
            "source_family": "satellite",
            "observation_type": "Satellite",
            "source_station_uid": from_meta("source_station_uid"),
            "satellite_station_uid": from_meta("satellite_station_uid"),
            "source_station_index": from_meta("source_station_index"),
            "source_station_native_id": from_meta("source_station_native_id"),
            "source_station_name": from_meta("source_station_name"),
            "source_station_river_name": from_meta("source_station_river_name"),
            "source_station_paths": from_meta("source_station_paths"),
            "source_station_lat": from_meta("source_station_lat"),
            "source_station_lon": from_meta("source_station_lon"),
            "candidate_path": from_meta("candidate_path"),
            "validation_only": 1,
            "merge_policy": from_meta("merge_policy"),
            "Q": q[positions],
            "SSC": ssc[positions],
            "SSL": ssl[positions],
            "Q_flag": q_flag.astype(int, copy=False),
            "SSC_flag": ssc_flag.astype(int, copy=False),
            "SSL_flag": ssl_flag.astype(int, copy=False),
            "Q_units": q_units,
            "SSC_units": ssc_units,
            "SSL_units": ssl_units,
            "has_Q": np.isfinite(q[positions]).astype(int),
            "has_SSC": np.isfinite(ssc[positions]).astype(int),
            "has_SSL": np.isfinite(ssl[positions]).astype(int),
            "method_notes": METHOD_NOTES,
            "assumptions": ASSUMPTIONS,
        }
    )
    return frame.reindex(columns=SATELLITE_CANDIDATE_COLUMNS)


def export_satellite_candidates_csv(
    input_nc: Path,
    output_csv: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    if nc4 is None:
        raise RuntimeError("netCDF4 is required")
    input_nc = Path(input_nc).resolve()
    output_csv = Path(output_csv).resolve()
    if not input_nc.is_file():
        raise FileNotFoundError("satellite validation NetCDF not found: {}".format(input_nc))

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    with nc4.Dataset(str(input_nc), "r") as ds:
        if "n_satellite_records" not in ds.dimensions:
            raise ValueError("input NetCDF missing n_satellite_records dimension")
        station_meta = _station_metadata(ds)
        n_records = len(ds.dimensions["n_satellite_records"])
        chunk_size = max(1, int(chunk_size or DEFAULT_CHUNK_SIZE))
        with gzip.open(str(output_csv), "wt", encoding="utf-8", newline="") as handle:
            wrote_header = False
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                frame = _chunk_to_frame(ds, station_meta, start, stop)
                if frame.empty:
                    continue
                frame.to_csv(handle, index=False, header=not wrote_header)
                wrote_header = True
                rows_written += int(len(frame))
            if not wrote_header:
                pd.DataFrame(columns=SATELLITE_CANDIDATE_COLUMNS).to_csv(handle, index=False)
    return rows_written


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Export satellite validation records to a CSV sidecar.")
    parser.add_argument("--input-nc", default=str(DEFAULT_INPUT_NC))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    parser.add_argument(
        "--release-csv",
        default=str(DEFAULT_RELEASE_CSV),
        help="Optional release-layer copy. Pass an empty string to skip.",
    )
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    args = parser.parse_args(argv)

    output_rows = export_satellite_candidates_csv(
        input_nc=Path(args.input_nc),
        output_csv=Path(args.output_csv),
        chunk_size=int(args.chunk_size),
    )
    print("Wrote satellite candidate CSV: {} ({} rows)".format(args.output_csv, output_rows))

    release_csv = str(args.release_csv or "").strip()
    if release_csv:
        release_rows = export_satellite_candidates_csv(
            input_nc=Path(args.input_nc),
            output_csv=Path(release_csv),
            chunk_size=int(args.chunk_size),
        )
        print("Wrote release satellite candidate CSV: {} ({} rows)".format(release_csv, release_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
