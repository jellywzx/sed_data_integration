#!/usr/bin/env python3
"""
Validate that sed_reference release artifacts come from one consistent pipeline run.

This is intended as a stricter, standalone release gate for mixed-run releases.
It checks consistency across:
  - sed_reference_master.nc
  - sed_reference_timeseries_{daily,monthly,annual}.nc
  - station_catalog.csv
  - source_station_catalog.csv
  - sed_reference_cluster_points.gpkg
  - optional sed_reference_source_stations.gpkg
  - optional sed_reference_cluster_basins.gpkg

Exit code:
  0 = no ERROR rows
  1 = at least one ERROR row
  2 = script/runtime failure

Dependencies:
  required: pandas, numpy, netCDF4
  optional for GPKG checks: geopandas
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError as exc:  # pragma: no cover
    raise SystemExit("ERROR: netCDF4 is required. Install with: pip install netCDF4") from exc


RESOLUTIONS: Tuple[str, ...] = ("daily", "monthly", "annual")
RESOLUTION_CODE_TO_NAME = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"}
RESOLUTION_NAME_TO_CODE = {v: k for k, v in RESOLUTION_CODE_TO_NAME.items()}
FILL_VALUES = {-9999.0, -9999, 1.0e20, -1.0e20}

MASTER_REQUIRED_DIMS = {"n_stations", "n_source_stations", "n_records", "n_sources"}
MASTER_REQUIRED_VARS = {
    "cluster_uid",
    "cluster_id",
    "lat",
    "lon",
    "basin_status",
    "basin_flag",
    "point_in_local",
    "point_in_basin",
    "source_station_uid",
    "source_station_cluster_index",
    "source_station_index",
    "station_index",
    "resolution",
    "time",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
    "is_overlap",
    "source",
    "source_name",
}

MATRIX_REQUIRED_DIMS = {"n_stations", "time", "n_sources"}
MATRIX_REQUIRED_VARS = {
    "cluster_uid",
    "cluster_id",
    "lat",
    "lon",
    "basin_status",
    "basin_flag",
    "point_in_local",
    "point_in_basin",
    "time",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
    "is_overlap",
    "selected_source_index",
    "selected_source_station_uid",
    "n_valid_time_steps",
    "source_name",
}

STATION_REQUIRED_COLUMNS = {
    "cluster_uid",
    "cluster_id",
    "resolution",
    "record_count",
    "time_start",
    "time_end",
    "lat",
    "lon",
    "basin_status",
    "basin_flag",
}

SOURCE_REQUIRED_COLUMNS = {
    "source_station_uid",
    "cluster_uid",
    "cluster_id",
    "resolution",
    "n_records",
    "time_start",
    "time_end",
}

VERSION_ATTR_CANDIDATES = (
    "release_version",
    "dataset_version",
    "version",
    "pipeline_version",
    "code_version",
    "git_commit",
    "pipeline_git_sha",
    "source_commit",
)

DATE_ATTR_CANDIDATES = (
    "pipeline_run_id",
    "pipeline_run_date",
    "run_date",
    "release_date",
    "date_created",
    "created",
    "history",
)


@dataclass
class CheckResult:
    severity: str  # ERROR, WARN, PASS
    check: str
    artifact: str
    details: str

    def as_row(self) -> Dict[str, str]:
        return asdict(self)


def clean_text(value) -> str:
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
    return "" if text.lower() in {"nan", "none", "<null>"} else text


def read_text_var(ds: nc4.Dataset, name: str, size: Optional[int] = None) -> List[str]:
    if name not in ds.variables:
        return [""] * int(size or 0)
    raw = np.asarray(ds.variables[name][:], dtype=object).reshape(-1)
    out = [clean_text(v) for v in raw]
    if size is not None and len(out) < size:
        out.extend([""] * (int(size) - len(out)))
    return out[: int(size)] if size is not None else out


def read_int_var(ds: nc4.Dataset, name: str, fill_value: int = -1, size: Optional[int] = None) -> np.ndarray:
    if name not in ds.variables:
        return np.full(int(size or 0), fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(fill_value)
    arr = pd.to_numeric(pd.Series(np.asarray(raw).reshape(-1)), errors="coerce").fillna(fill_value)
    out = arr.astype(np.int64).to_numpy()
    if size is not None and len(out) < int(size):
        out = np.concatenate([out, np.full(int(size) - len(out), fill_value, dtype=np.int64)])
    return out[: int(size)] if size is not None else out


def read_float_var(ds: nc4.Dataset, name: str, size: Optional[int] = None) -> np.ndarray:
    if name not in ds.variables:
        return np.full(int(size or 0), np.nan, dtype=np.float64)
    raw = np.ma.asarray(ds.variables[name][:]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(raw):
        arr = raw.filled(np.nan)
    else:
        arr = np.asarray(raw, dtype=np.float64)
    for fill in FILL_VALUES:
        arr[arr == fill] = np.nan
    if size is not None and len(arr) < int(size):
        arr = np.concatenate([arr, np.full(int(size) - len(arr), np.nan, dtype=np.float64)])
    return arr[: int(size)] if size is not None else arr


def masked_to_nan(values) -> np.ndarray:
    arr = np.ma.asarray(values).astype(np.float64)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    arr = np.asarray(arr, dtype=np.float64)
    for fill in FILL_VALUES:
        arr[arr == fill] = np.nan
    return arr


def decode_num_times(values: Sequence[float], time_var) -> pd.Series:
    values = np.asarray(values, dtype=np.float64).reshape(-1)
    out = pd.Series([pd.NaT] * len(values), dtype="datetime64[ns]")
    finite = np.isfinite(values)
    if not finite.any():
        return out
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        decoded = nc4.num2date(values[finite], units=units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        decoded = nc4.num2date(values[finite], units=units, calendar=calendar)
    except Exception:
        decoded = pd.to_datetime(values[finite], unit="D", origin="1970-01-01", errors="coerce")
    converted = []
    for item in list(decoded):
        try:
            converted.append(pd.Timestamp(item).tz_localize(None))
        except Exception:
            try:
                converted.append(pd.Timestamp(str(item)).tz_localize(None))
            except Exception:
                converted.append(pd.NaT)
    out.iloc[np.flatnonzero(finite)] = converted
    return out


def norm_time(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    try:
        ts = ts.tz_localize(None)
    except Exception:
        pass
    return pd.Timestamp(ts).normalize()


def time_text(value) -> str:
    ts = norm_time(value)
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d")


def finite_any_rows(q: np.ndarray, ssc: np.ndarray, ssl: np.ndarray) -> np.ndarray:
    return np.isfinite(q) | np.isfinite(ssc) | np.isfinite(ssl)


def add(results: List[CheckResult], severity: str, check: str, artifact: str, details: str) -> None:
    results.append(CheckResult(severity=severity, check=check, artifact=str(artifact), details=str(details)))


def check_path(results: List[CheckResult], path: Path, label: str, required: bool = True) -> bool:
    if path.is_file():
        add(results, "PASS", "file_exists", label, str(path))
        return True
    add(results, "ERROR" if required else "WARN", "file_exists", label, f"Missing file: {path}")
    return False


def require_columns(results: List[CheckResult], frame: pd.DataFrame, required: Set[str], artifact: str) -> bool:
    missing = sorted(required - set(frame.columns))
    if missing:
        add(results, "ERROR", "required_columns", artifact, "Missing columns: " + ", ".join(missing))
        return False
    add(results, "PASS", "required_columns", artifact, "All required columns present")
    return True


def require_nc_structure(results: List[CheckResult], path: Path, required_dims: Set[str], required_vars: Set[str], artifact: str) -> bool:
    ok = True
    with nc4.Dataset(path, "r") as ds:
        missing_dims = sorted(required_dims - set(ds.dimensions))
        missing_vars = sorted(required_vars - set(ds.variables))
        if missing_dims:
            add(results, "ERROR", "required_dimensions", artifact, "Missing dimensions: " + ", ".join(missing_dims))
            ok = False
        else:
            add(results, "PASS", "required_dimensions", artifact, "All required dimensions present")
        if missing_vars:
            add(results, "ERROR", "required_variables", artifact, "Missing variables: " + ", ".join(missing_vars))
            ok = False
        else:
            add(results, "PASS", "required_variables", artifact, "All required variables present")
    return ok


def normalize_catalogs(results: List[CheckResult], station_catalog: pd.DataFrame, source_catalog: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    st = station_catalog.copy()
    src = source_catalog.copy()

    for col in ("cluster_uid", "resolution", "basin_status", "basin_flag"):
        if col in st.columns:
            st[col] = st[col].fillna("").astype(str).str.strip()
    for col in ("cluster_uid", "source_station_uid", "resolution"):
        if col in src.columns:
            src[col] = src[col].fillna("").astype(str).str.strip()

    if "record_count" in st.columns:
        st["record_count"] = pd.to_numeric(st["record_count"], errors="coerce").fillna(0).astype(np.int64)
    if "n_records" in src.columns:
        src["n_records"] = pd.to_numeric(src["n_records"], errors="coerce").fillna(0).astype(np.int64)
    for col in ("lat", "lon"):
        if col in st.columns:
            st[col] = pd.to_numeric(st[col], errors="coerce")
    for col in ("source_station_lat", "source_station_lon"):
        if col in src.columns:
            src[col] = pd.to_numeric(src[col], errors="coerce")

    if {"cluster_uid", "resolution"} <= set(st.columns):
        dup = st.duplicated(["cluster_uid", "resolution"], keep=False)
        if dup.any():
            sample = st.loc[dup, ["cluster_uid", "resolution"]].head(10).to_dict("records")
            add(results, "ERROR", "station_catalog_unique_key", "station_catalog.csv", f"Duplicate cluster_uid+resolution rows, sample={sample}")
        else:
            add(results, "PASS", "station_catalog_unique_key", "station_catalog.csv", f"{len(st)} unique cluster_uid+resolution rows")

        invalid = sorted(set(st["resolution"]) - set(RESOLUTIONS))
        if invalid:
            add(results, "ERROR", "station_catalog_resolution_domain", "station_catalog.csv", f"Unexpected resolutions in basin mainline release: {invalid}")
        else:
            add(results, "PASS", "station_catalog_resolution_domain", "station_catalog.csv", "Only daily/monthly/annual")

    if {"source_station_uid", "resolution"} <= set(src.columns):
        dup = src.duplicated(["source_station_uid", "resolution"], keep=False)
        if dup.any():
            sample = src.loc[dup, ["source_station_uid", "resolution"]].head(10).to_dict("records")
            add(results, "ERROR", "source_catalog_unique_key", "source_station_catalog.csv", f"Duplicate source_station_uid+resolution rows, sample={sample}")
        else:
            add(results, "PASS", "source_catalog_unique_key", "source_station_catalog.csv", f"{len(src)} unique source_station_uid+resolution rows")

        invalid = sorted(set(src["resolution"]) - set(RESOLUTIONS))
        if invalid:
            add(results, "ERROR", "source_catalog_resolution_domain", "source_station_catalog.csv", f"Unexpected resolutions in basin mainline release: {invalid}")
        else:
            add(results, "PASS", "source_catalog_resolution_domain", "source_station_catalog.csv", "Only daily/monthly/annual")

    for name, frame, count_col in (("station_catalog.csv", st, "record_count"), ("source_station_catalog.csv", src, "n_records")):
        if {"time_start", "time_end", count_col} <= set(frame.columns):
            active = frame[pd.to_numeric(frame[count_col], errors="coerce").fillna(0) > 0].copy()
            bad_time = active[
                active.apply(lambda r: pd.notna(norm_time(r["time_start"])) and pd.notna(norm_time(r["time_end"])) and norm_time(r["time_start"]) > norm_time(r["time_end"]), axis=1)
            ]
            if len(bad_time):
                add(results, "ERROR", "catalog_time_order", name, f"{len(bad_time)} rows have time_start > time_end")
            else:
                add(results, "PASS", "catalog_time_order", name, "All active rows have time_start <= time_end")

    return st, src


def station_key_set(station_catalog: pd.DataFrame) -> Set[Tuple[str, str]]:
    return set(zip(station_catalog["cluster_uid"].astype(str), station_catalog["resolution"].astype(str)))


def source_key_set(source_catalog: pd.DataFrame) -> Set[Tuple[str, str]]:
    return set(zip(source_catalog["source_station_uid"].astype(str), source_catalog["resolution"].astype(str)))


def summarize_master(path: Path) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Set[str]]]:
    with nc4.Dataset(path, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        n_source_stations = len(ds.dimensions["n_source_stations"])
        n_records = len(ds.dimensions["n_records"])

        cluster_uids = read_text_var(ds, "cluster_uid", n_stations)
        source_uids = read_text_var(ds, "source_station_uid", n_source_stations)
        station_idx = read_int_var(ds, "station_index", -1, n_records)
        source_station_idx = read_int_var(ds, "source_station_index", -1, n_records)
        resolution_codes = read_int_var(ds, "resolution", -1, n_records)
        times = read_float_var(ds, "time", n_records)
        decoded = decode_num_times(times, ds.variables["time"])

        rows = []
        source_rows = []
        bad_refs = {
            "bad_station_index": set(),
            "bad_source_station_index": set(),
            "bad_resolution_code": set(),
        }

        frame = pd.DataFrame(
            {
                "station_index": station_idx,
                "source_station_index": source_station_idx,
                "resolution_code": resolution_codes,
                "time": decoded,
            }
        )
        frame["resolution"] = frame["resolution_code"].map(RESOLUTION_CODE_TO_NAME).fillna("")
        frame["cluster_uid"] = frame["station_index"].map(lambda i: cluster_uids[int(i)] if 0 <= int(i) < len(cluster_uids) else "")
        frame["source_station_uid"] = frame["source_station_index"].map(lambda i: source_uids[int(i)] if 0 <= int(i) < len(source_uids) else "")

        bad_refs["bad_station_index"] = set(frame.loc[~frame["station_index"].between(0, n_stations - 1), "station_index"].astype(str))
        bad_refs["bad_source_station_index"] = set(frame.loc[(frame["source_station_index"] >= 0) & (~frame["source_station_index"].between(0, n_source_stations - 1)), "source_station_index"].astype(str))
        bad_refs["bad_resolution_code"] = set(frame.loc[~frame["resolution"].isin(RESOLUTIONS + ("climatology", "other")), "resolution_code"].astype(str))

        core = frame[frame["resolution"].isin(RESOLUTIONS) & frame["time"].notna() & (frame["cluster_uid"] != "")].copy()
        if not core.empty:
            summary = (
                core.groupby(["cluster_uid", "resolution"], as_index=False)
                .agg(record_count=("time", "size"), time_start=("time", "min"), time_end=("time", "max"))
            )
            rows = summary.to_dict("records")

        source_core = core[core["source_station_uid"] != ""].copy()
        if not source_core.empty:
            source_summary = (
                source_core.groupby(["source_station_uid", "resolution"], as_index=False)
                .agg(n_records=("time", "size"), time_start=("time", "min"), time_end=("time", "max"))
            )
            source_rows = source_summary.to_dict("records")

    return pd.DataFrame(rows), pd.DataFrame(source_rows), bad_refs


def summarize_matrix(path: Path, resolution: str, chunk_rows: int = 64) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    with nc4.Dataset(path, "r") as ds:
        n_stations = len(ds.dimensions["n_stations"])
        n_time = len(ds.dimensions["time"])
        cluster_uids = read_text_var(ds, "cluster_uid", n_stations)
        declared_counts = read_int_var(ds, "n_valid_time_steps", 0, n_stations)
        times = read_float_var(ds, "time", n_time)
        decoded_times = decode_num_times(times, ds.variables["time"])

        stats = []
        selected_source_pairs: Set[Tuple[str, str]] = set()
        selected_cluster_source_pairs: Set[Tuple[str, str, str]] = set()
        count_mismatch_samples = []
        shape_errors = []

        for var_name in ("Q", "SSC", "SSL", "selected_source_station_uid"):
            var = ds.variables[var_name]
            if var.shape != (n_stations, n_time):
                shape_errors.append(f"{var_name}.shape={var.shape}, expected={(n_stations, n_time)}")

        for start in range(0, n_stations, chunk_rows):
            stop = min(start + chunk_rows, n_stations)
            q = masked_to_nan(ds.variables["Q"][start:stop, :])
            ssc = masked_to_nan(ds.variables["SSC"][start:stop, :])
            ssl = masked_to_nan(ds.variables["SSL"][start:stop, :])
            valid = finite_any_rows(q, ssc, ssl)

            uid_chunk = np.asarray(ds.variables["selected_source_station_uid"][start:stop, :], dtype=object)
            for local_row in range(stop - start):
                global_row = start + local_row
                cluster_uid = cluster_uids[global_row]
                valid_cols = np.flatnonzero(valid[local_row])
                actual_count = int(len(valid_cols))
                declared = int(declared_counts[global_row])
                if actual_count != declared and len(count_mismatch_samples) < 10:
                    count_mismatch_samples.append((cluster_uid, declared, actual_count))
                if actual_count == 0:
                    continue

                first_col = int(valid_cols[0])
                last_col = int(valid_cols[-1])
                stats.append(
                    {
                        "cluster_uid": cluster_uid,
                        "resolution": resolution,
                        "record_count": actual_count,
                        "time_start": decoded_times.iloc[first_col],
                        "time_end": decoded_times.iloc[last_col],
                    }
                )

                # Only provenance cells that actually have a selected value for a non-empty matrix cell matter.
                selected_vals = uid_chunk[local_row, valid_cols].reshape(-1)
                for value in selected_vals:
                    uid = clean_text(value)
                    if uid:
                        selected_source_pairs.add((uid, resolution))
                        selected_cluster_source_pairs.add((cluster_uid, uid, resolution))

        summary = pd.DataFrame(stats)
        provenance = pd.DataFrame(
            [{"cluster_uid": c, "source_station_uid": u, "resolution": r} for c, u, r in sorted(selected_cluster_source_pairs)]
        )
        aux = {
            "selected_source_pairs": selected_source_pairs,
            "count_mismatch_samples": count_mismatch_samples,
            "shape_errors": shape_errors,
            "n_stations": n_stations,
            "n_time": n_time,
        }
        return summary, provenance, aux


def compare_summary_to_catalog(
    results: List[CheckResult],
    label: str,
    artifact: str,
    observed: pd.DataFrame,
    catalog: pd.DataFrame,
    count_col_observed: str,
    count_col_catalog: str,
    key_cols: Sequence[str],
) -> None:
    if observed.empty:
        add(results, "ERROR", f"{label}_nonempty", artifact, "Observed summary is empty")
        return

    obs = observed.copy()
    cat = catalog.copy()

    required_obs_cols = set(key_cols) | {count_col_observed, "time_start", "time_end"}
    required_cat_cols = set(key_cols) | {count_col_catalog, "time_start", "time_end"}
    missing_obs = sorted(required_obs_cols - set(obs.columns))
    missing_cat = sorted(required_cat_cols - set(cat.columns))
    if missing_obs or missing_cat:
        details = []
        if missing_obs:
            details.append(f"observed missing columns: {', '.join(missing_obs)}")
        if missing_cat:
            details.append(f"catalog missing columns: {', '.join(missing_cat)}")
        add(results, "ERROR", f"{label}_required_columns", artifact, "; ".join(details))
        return

    for col in key_cols:
        obs[col] = obs[col].astype(str)
        cat[col] = cat[col].astype(str)

    obs[count_col_observed] = pd.to_numeric(obs[count_col_observed], errors="coerce").fillna(0).astype(np.int64)
    cat[count_col_catalog] = pd.to_numeric(cat[count_col_catalog], errors="coerce").fillna(0).astype(np.int64)

    merged = obs.merge(cat, on=list(key_cols), how="outer", suffixes=("_observed", "_catalog"), indicator=True)
    observed_count_col = f"{count_col_observed}_observed" if f"{count_col_observed}_observed" in merged.columns else count_col_observed
    catalog_count_col = f"{count_col_catalog}_catalog" if f"{count_col_catalog}_catalog" in merged.columns else count_col_catalog
    missing_merged_counts = [col for col in (observed_count_col, catalog_count_col) if col not in merged.columns]
    if missing_merged_counts:
        add(results, "ERROR", f"{label}_count_columns", artifact, f"Missing merged count columns: {missing_merged_counts}; available={list(merged.columns)}")
        return

    missing_catalog = merged[merged["_merge"].eq("left_only")]
    extra_catalog = merged[merged["_merge"].eq("right_only") & (merged[catalog_count_col].fillna(0) > 0)]
    if len(missing_catalog):
        add(results, "ERROR", f"{label}_keys_in_catalog", artifact, f"{len(missing_catalog)} observed keys missing from catalog; sample={missing_catalog[list(key_cols)].head(10).to_dict('records')}")
    else:
        add(results, "PASS", f"{label}_keys_in_catalog", artifact, "All observed keys exist in catalog")

    if len(extra_catalog):
        add(results, "ERROR", f"{label}_catalog_no_extra_active_keys", artifact, f"{len(extra_catalog)} active catalog keys not found in observed data; sample={extra_catalog[list(key_cols)].head(10).to_dict('records')}")
    else:
        add(results, "PASS", f"{label}_catalog_no_extra_active_keys", artifact, "No active catalog-only keys")

    both = merged[merged["_merge"].eq("both")].copy()
    if both.empty:
        return

    count_bad = both[both[observed_count_col] != both[catalog_count_col]]
    if len(count_bad):
        sample_cols = list(key_cols) + [observed_count_col, catalog_count_col]
        add(results, "ERROR", f"{label}_record_count", artifact, f"{len(count_bad)} count mismatches; sample={count_bad[sample_cols].head(10).to_dict('records')}")
    else:
        add(results, "PASS", f"{label}_record_count", artifact, f"{len(both)} key counts match")

    both["time_start_observed_norm"] = both["time_start_observed"].map(time_text)
    both["time_end_observed_norm"] = both["time_end_observed"].map(time_text)
    both["time_start_catalog_norm"] = both["time_start_catalog"].map(time_text)
    both["time_end_catalog_norm"] = both["time_end_catalog"].map(time_text)
    time_bad = both[
        (both["time_start_observed_norm"] != both["time_start_catalog_norm"])
        | (both["time_end_observed_norm"] != both["time_end_catalog_norm"])
    ]
    if len(time_bad):
        sample_cols = list(key_cols) + [
            "time_start_observed_norm",
            "time_start_catalog_norm",
            "time_end_observed_norm",
            "time_end_catalog_norm",
        ]
        add(results, "ERROR", f"{label}_time_range", artifact, f"{len(time_bad)} time-range mismatches; sample={time_bad[sample_cols].head(10).to_dict('records')}")
    else:
        add(results, "PASS", f"{label}_time_range", artifact, f"{len(both)} key time ranges match")


def compare_master_matrix(results: List[CheckResult], master_summary: pd.DataFrame, matrix_summary: pd.DataFrame) -> None:
    if master_summary.empty or matrix_summary.empty:
        add(results, "ERROR", "master_matrix_nonempty", "master/matrix", f"master_rows={len(master_summary)} matrix_rows={len(matrix_summary)}")
        return
    compare_summary_to_catalog(
        results=results,
        label="master_vs_matrix",
        artifact="master/matrix",
        observed=master_summary,
        catalog=matrix_summary,
        count_col_observed="record_count",
        count_col_catalog="record_count",
        key_cols=("cluster_uid", "resolution"),
    )


def check_source_catalog_links(results: List[CheckResult], source_catalog: pd.DataFrame, station_catalog: pd.DataFrame) -> None:
    st_keys = station_key_set(station_catalog)
    bad = []
    for row in source_catalog[["cluster_uid", "source_station_uid", "resolution"]].itertuples(index=False):
        key = (str(row.cluster_uid), str(row.resolution))
        if key not in st_keys:
            bad.append({"source_station_uid": row.source_station_uid, "cluster_uid": row.cluster_uid, "resolution": row.resolution})
            if len(bad) >= 10:
                break
    if bad:
        add(results, "ERROR", "source_catalog_cluster_keys", "source_station_catalog.csv", f"source catalog rows point to missing station_catalog cluster_uid+resolution; sample={bad}")
    else:
        add(results, "PASS", "source_catalog_cluster_keys", "source_station_catalog.csv", "All source catalog cluster_uid+resolution keys exist in station_catalog")


def check_provenance_sources(
    results: List[CheckResult],
    matrix_provenance: pd.DataFrame,
    master_source_summary: pd.DataFrame,
    source_catalog: pd.DataFrame,
) -> None:
    src_keys = source_key_set(source_catalog)

    missing_matrix = []
    if not matrix_provenance.empty:
        for uid, res in sorted(set(zip(matrix_provenance["source_station_uid"], matrix_provenance["resolution"]))):
            if (str(uid), str(res)) not in src_keys:
                missing_matrix.append({"source_station_uid": uid, "resolution": res})
                if len(missing_matrix) >= 10:
                    break
    if missing_matrix:
        add(results, "ERROR", "matrix_source_station_uid_in_catalog", "matrix NetCDFs", f"selected_source_station_uid missing from source_station_catalog; sample={missing_matrix}")
    else:
        add(results, "PASS", "matrix_source_station_uid_in_catalog", "matrix NetCDFs", "All selected_source_station_uid values resolve in source_station_catalog")

    missing_master = []
    if not master_source_summary.empty:
        for uid, res in sorted(set(zip(master_source_summary["source_station_uid"], master_source_summary["resolution"]))):
            if (str(uid), str(res)) not in src_keys:
                missing_master.append({"source_station_uid": uid, "resolution": res})
                if len(missing_master) >= 10:
                    break
    if missing_master:
        add(results, "ERROR", "master_source_station_uid_in_catalog", "master NetCDF", f"source_station_uid missing from source_station_catalog; sample={missing_master}")
    else:
        add(results, "PASS", "master_source_station_uid_in_catalog", "master NetCDF", "All master source_station_uid values resolve in source_station_catalog")

    if not matrix_provenance.empty and {"source_station_uid", "cluster_uid", "resolution"} <= set(source_catalog.columns):
        lookup = source_catalog.set_index(["source_station_uid", "resolution"])["cluster_uid"].to_dict()
        bad = []
        for row in matrix_provenance.itertuples(index=False):
            cat_cluster = lookup.get((str(row.source_station_uid), str(row.resolution)), "")
            if cat_cluster and str(cat_cluster) != str(row.cluster_uid):
                bad.append({"matrix_cluster_uid": row.cluster_uid, "source_station_uid": row.source_station_uid, "catalog_cluster_uid": cat_cluster, "resolution": row.resolution})
                if len(bad) >= 10:
                    break
        if bad:
            add(results, "ERROR", "matrix_source_station_cluster_link", "matrix/source_station_catalog.csv", f"selected source_station_uid resolves to a different cluster; sample={bad}")
        else:
            add(results, "PASS", "matrix_source_station_cluster_link", "matrix/source_station_catalog.csv", "Matrix selected_source_station_uid cluster links match source_station_catalog")


def collect_global_run_attrs(path: Path) -> Dict[str, str]:
    with nc4.Dataset(path, "r") as ds:
        attrs = {name: clean_text(getattr(ds, name, "")) for name in ds.ncattrs()}
    version = ""
    version_key = ""
    for key in VERSION_ATTR_CANDIDATES:
        if attrs.get(key):
            version = attrs[key]
            version_key = key
            break
    date = ""
    date_key = ""
    for key in DATE_ATTR_CANDIDATES:
        if attrs.get(key):
            date = attrs[key]
            date_key = key
            break
    return {"version_key": version_key, "version": version, "date_key": date_key, "date": date}


def parse_date_from_attr(text: str) -> Optional[pd.Timestamp]:
    if not text:
        return None
    # Match common ISO snippets, e.g. "Created 2026-05-23T10:30:01 by script"
    match = re.search(r"\d{4}-\d{2}-\d{2}(?:[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.\d+)?)?", text)
    if not match:
        return None
    ts = pd.to_datetime(match.group(0), errors="coerce")
    if pd.isna(ts):
        return None
    try:
        ts = ts.tz_localize(None)
    except Exception:
        pass
    return pd.Timestamp(ts)


def check_global_attrs(results: List[CheckResult], nc_paths: Dict[str, Path], require: bool, tolerance_hours: float) -> None:
    attr_rows = {}
    for label, path in nc_paths.items():
        attr_rows[label] = collect_global_run_attrs(path)

    versions = {label: row["version"] for label, row in attr_rows.items() if row["version"]}
    if not versions:
        add(results, "ERROR" if require else "WARN", "netcdf_global_version_attr", "NetCDF global attrs", "No version-like global attributes found in any core NetCDF")
    elif len(set(versions.values())) > 1:
        add(results, "ERROR", "netcdf_global_version_attr", "NetCDF global attrs", f"Version attrs differ: {versions}")
    elif len(versions) != len(nc_paths):
        missing = sorted(set(nc_paths) - set(versions))
        add(results, "ERROR" if require else "WARN", "netcdf_global_version_attr", "NetCDF global attrs", f"Version attr present but missing in {missing}; values={versions}")
    else:
        add(results, "PASS", "netcdf_global_version_attr", "NetCDF global attrs", f"All version attrs match: {next(iter(versions.values()))}")

    parsed_dates = {}
    missing_date_labels = []
    for label, row in attr_rows.items():
        ts = parse_date_from_attr(row["date"])
        if ts is None:
            missing_date_labels.append(label)
        else:
            parsed_dates[label] = ts

    if not parsed_dates:
        add(results, "ERROR" if require else "WARN", "netcdf_global_date_attr", "NetCDF global attrs", "No parseable date-like global attributes found in core NetCDFs")
    elif missing_date_labels:
        add(results, "ERROR" if require else "WARN", "netcdf_global_date_attr", "NetCDF global attrs", f"Date attr missing/unparseable in {missing_date_labels}; parsed={parsed_dates}")
    else:
        min_ts = min(parsed_dates.values())
        max_ts = max(parsed_dates.values())
        delta_hours = (max_ts - min_ts).total_seconds() / 3600.0
        if delta_hours > tolerance_hours:
            add(results, "ERROR", "netcdf_global_date_attr", "NetCDF global attrs", f"Core NetCDF date attrs differ by {delta_hours:.2f}h > tolerance {tolerance_hours:.2f}h; parsed={parsed_dates}")
        else:
            add(results, "PASS", "netcdf_global_date_attr", "NetCDF global attrs", f"Core NetCDF date attrs within {delta_hours:.2f}h; parsed={parsed_dates}")


def gpkg_layers(path: Path) -> List[str]:
    import sqlite3

    with sqlite3.connect(str(path)) as conn:
        rows = conn.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name").fetchall()
    return [str(r[0]) for r in rows]


def check_gpkg(results: List[CheckResult], cluster_points_gpkg: Path, source_stations_gpkg: Optional[Path], cluster_basins_gpkg: Optional[Path], station_catalog: pd.DataFrame, source_catalog: pd.DataFrame) -> None:
    try:
        import geopandas as gpd
    except ImportError:
        add(results, "ERROR", "gpkg_dependency", "GeoPackage", "geopandas is required for GPKG checks. Install with: pip install geopandas")
        return

    if cluster_points_gpkg and cluster_points_gpkg.is_file():
        layers = set(gpkg_layers(cluster_points_gpkg))
        expected = {"cluster_summary"} | {f"cluster_{r}" for r in RESOLUTIONS}
        missing = sorted(expected - layers)
        if missing:
            add(results, "ERROR", "cluster_points_layers", cluster_points_gpkg.name, f"Missing layers: {missing}")
        else:
            add(results, "PASS", "cluster_points_layers", cluster_points_gpkg.name, f"All expected layers present: {sorted(expected)}")

        # Summary layer: one point per unique cluster_uid in station catalog.
        if "cluster_summary" in layers:
            gdf = gpd.read_file(cluster_points_gpkg, layer="cluster_summary")
            expected_count = int(station_catalog["cluster_uid"].nunique())
            actual_count = int(len(gdf))
            if actual_count != expected_count:
                add(results, "ERROR", "cluster_summary_point_count", cluster_points_gpkg.name, f"actual={actual_count}, expected_unique_station_catalog_clusters={expected_count}")
            else:
                add(results, "PASS", "cluster_summary_point_count", cluster_points_gpkg.name, f"{actual_count} points match unique station catalog clusters")

        for res in RESOLUTIONS:
            layer = f"cluster_{res}"
            if layer not in layers:
                continue
            gdf = gpd.read_file(cluster_points_gpkg, layer=layer)
            expected_count = int((station_catalog["resolution"] == res).sum())
            actual_count = int(len(gdf))
            if actual_count != expected_count:
                add(results, "ERROR", f"cluster_{res}_point_count", cluster_points_gpkg.name, f"actual={actual_count}, expected_station_catalog_rows={expected_count}")
            else:
                add(results, "PASS", f"cluster_{res}_point_count", cluster_points_gpkg.name, f"{actual_count} points match station catalog rows")

            if {"cluster_uid", "resolution"} <= set(gdf.columns):
                layer_keys = set(zip(gdf["cluster_uid"].astype(str), gdf["resolution"].astype(str)))
                expected_keys = set(zip(station_catalog.loc[station_catalog["resolution"] == res, "cluster_uid"].astype(str), station_catalog.loc[station_catalog["resolution"] == res, "resolution"].astype(str)))
                extra = sorted(layer_keys - expected_keys)[:10]
                missing = sorted(expected_keys - layer_keys)[:10]
                if extra or missing:
                    add(results, "ERROR", f"cluster_{res}_keys", cluster_points_gpkg.name, f"extra_sample={extra}; missing_sample={missing}")
                else:
                    add(results, "PASS", f"cluster_{res}_keys", cluster_points_gpkg.name, "Layer keys match station_catalog")
    else:
        add(results, "WARN", "cluster_points_gpkg", "GeoPackage", f"Cluster points GPKG not found: {cluster_points_gpkg}")

    if source_stations_gpkg and source_stations_gpkg.is_file():
        layers = set(gpkg_layers(source_stations_gpkg))
        expected = {f"source_{r}" for r in RESOLUTIONS}
        missing = sorted(expected - layers)
        if missing:
            add(results, "ERROR", "source_stations_layers", source_stations_gpkg.name, f"Missing layers: {missing}")
        else:
            add(results, "PASS", "source_stations_layers", source_stations_gpkg.name, f"All expected layers present: {sorted(expected)}")

        if {"source_station_lat", "source_station_lon"} <= set(source_catalog.columns):
            coords_ok = source_catalog["source_station_lat"].notna() & source_catalog["source_station_lon"].notna()
        else:
            coords_ok = pd.Series([True] * len(source_catalog), index=source_catalog.index)

        for res in RESOLUTIONS:
            layer = f"source_{res}"
            if layer not in layers:
                continue
            gdf = gpd.read_file(source_stations_gpkg, layer=layer)
            expected_count = int(((source_catalog["resolution"] == res) & coords_ok).sum())
            actual_count = int(len(gdf))
            if actual_count != expected_count:
                add(results, "ERROR", f"source_{res}_point_count", source_stations_gpkg.name, f"actual={actual_count}, expected_source_catalog_rows_with_coords={expected_count}")
            else:
                add(results, "PASS", f"source_{res}_point_count", source_stations_gpkg.name, f"{actual_count} points match source catalog rows with coordinates")
    elif source_stations_gpkg:
        add(results, "WARN", "source_stations_gpkg", "GeoPackage", f"Source stations GPKG not found: {source_stations_gpkg}")

    if cluster_basins_gpkg and cluster_basins_gpkg.is_file():
        layers = set(gpkg_layers(cluster_basins_gpkg))
        expected = {f"basin_{r}" for r in RESOLUTIONS}
        missing = sorted(expected - layers)
        if missing:
            add(results, "ERROR", "cluster_basins_layers", cluster_basins_gpkg.name, f"Missing layers: {missing}")
        else:
            add(results, "PASS", "cluster_basins_layers", cluster_basins_gpkg.name, f"All expected layers present: {sorted(expected)}")

        resolved_keys = set(
            zip(
                station_catalog.loc[station_catalog["basin_status"].str.lower().eq("resolved"), "cluster_uid"].astype(str),
                station_catalog.loc[station_catalog["basin_status"].str.lower().eq("resolved"), "resolution"].astype(str),
            )
        )
        all_station_keys = station_key_set(station_catalog)

        for res in RESOLUTIONS:
            layer = f"basin_{res}"
            if layer not in layers:
                continue
            gdf = gpd.read_file(cluster_basins_gpkg, layer=layer)
            if len(gdf) == 0:
                add(results, "WARN", f"basin_{res}_nonempty", cluster_basins_gpkg.name, "Layer is empty")
                continue
            if "basin_status" in gdf.columns:
                bad_status = gdf[~gdf["basin_status"].fillna("").astype(str).str.lower().eq("resolved")]
                if len(bad_status):
                    add(results, "ERROR", f"basin_{res}_only_resolved_status", cluster_basins_gpkg.name, f"{len(bad_status)} polygon rows are not basin_status=resolved")
                else:
                    add(results, "PASS", f"basin_{res}_only_resolved_status", cluster_basins_gpkg.name, "All polygon rows have basin_status=resolved")
            else:
                add(results, "ERROR", f"basin_{res}_basin_status_column", cluster_basins_gpkg.name, "Missing basin_status column")

            if {"cluster_uid", "resolution"} <= set(gdf.columns):
                keys = set(zip(gdf["cluster_uid"].astype(str), gdf["resolution"].astype(str)))
                non_catalog = sorted(keys - all_station_keys)[:10]
                non_resolved = sorted(keys - resolved_keys)[:10]
                if non_catalog:
                    add(results, "ERROR", f"basin_{res}_keys_in_station_catalog", cluster_basins_gpkg.name, f"Polygon keys not in station_catalog; sample={non_catalog}")
                else:
                    add(results, "PASS", f"basin_{res}_keys_in_station_catalog", cluster_basins_gpkg.name, "All polygon keys exist in station_catalog")
                if non_resolved:
                    add(results, "ERROR", f"basin_{res}_only_resolved_keys", cluster_basins_gpkg.name, f"Polygon keys not resolved in station_catalog; sample={non_resolved}")
                else:
                    add(results, "PASS", f"basin_{res}_only_resolved_keys", cluster_basins_gpkg.name, "All polygon keys correspond to resolved station_catalog rows")
    elif cluster_basins_gpkg:
        add(results, "WARN", "cluster_basins_gpkg", "GeoPackage", f"Basin polygon GPKG not found: {cluster_basins_gpkg}")


def default_paths(release_dir: Path) -> Dict[str, Path]:
    return {
        "master": release_dir / "sed_reference_master.nc",
        "daily": release_dir / "sed_reference_timeseries_daily.nc",
        "monthly": release_dir / "sed_reference_timeseries_monthly.nc",
        "annual": release_dir / "sed_reference_timeseries_annual.nc",
        "station_catalog": release_dir / "station_catalog.csv",
        "source_station_catalog": release_dir / "source_station_catalog.csv",
        "cluster_points_gpkg": release_dir / "sed_reference_cluster_points.gpkg",
        "source_stations_gpkg": release_dir / "sed_reference_source_stations.gpkg",
        "cluster_basins_gpkg": release_dir / "sed_reference_cluster_basins.gpkg",
    }


def find_scripts_basin_test_root() -> Path:
    """Find the real scripts_basin_test root.

    Search upward for a parent named scripts_basin_test before considering any
    child directory named scripts_basin_test. This prevents an accidentally
    created validate/scripts_basin_test/ directory from being mistaken for the
    project root.
    """
    starts = [Path(__file__).resolve().parent, Path.cwd().resolve()]

    # 1) If this script is inside scripts_basin_test/, return that ancestor.
    for base in starts:
        for parent in (base, *base.parents):
            if parent.name == "scripts_basin_test":
                return parent

    # 2) If running from Output_r/ or another parent, look for a child directory.
    for base in starts:
        for parent in (base, *base.parents):
            candidate = parent / "scripts_basin_test"
            if candidate.is_dir():
                return candidate.resolve()

    # 3) Fallback: preserve old convention but avoid double nesting when possible.
    cwd = Path.cwd().resolve()
    if cwd.name == "scripts_basin_test":
        return cwd
    return (cwd / "scripts_basin_test").resolve()


def resolve_release_dir(value: str) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    return (find_scripts_basin_test_root() / "output" / "sed_reference_release").resolve()


def default_report_dir() -> Path:
    return (find_scripts_basin_test_root() / "output_other" / "validate_mixed_run").resolve()


def resolve_default_report_csv(value: str) -> str:
    if value:
        return value
    return str((default_report_dir() / "mixed_run_validation_report.csv").resolve())


def resolve_default_report_md(value: str) -> str:
    if value:
        return value
    return str((default_report_dir() / "mixed_run_validation_report.md").resolve())


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Validate sed_reference release artifacts for mixed-run consistency")
    p.add_argument("--release-dir", default="", help="Release directory. Default: auto-detect scripts_basin_test/output/sed_reference_release")
    p.add_argument("--master-nc", default="")
    p.add_argument("--daily-nc", default="")
    p.add_argument("--monthly-nc", default="")
    p.add_argument("--annual-nc", default="")
    p.add_argument("--station-catalog", default="")
    p.add_argument("--source-station-catalog", default="")
    p.add_argument("--cluster-points-gpkg", default="")
    p.add_argument("--source-stations-gpkg", default="")
    p.add_argument("--cluster-basins-gpkg", default="")
    p.add_argument("--skip-gpkg", action="store_true")
    p.add_argument("--require-run-attrs", action="store_true", help="Fail if version/date-like global attrs are missing or unparseable")
    p.add_argument("--attr-time-tolerance-hours", type=float, default=24.0, help="Allowed spread among NetCDF date-like attrs")
    p.add_argument("--chunk-rows", type=int, default=64, help="Matrix row chunk size")
    p.add_argument("--report-csv", default="", help="Output report CSV path. Default: scripts_basin_test/output_other/validate_mixed_run/mixed_run_validation_report.csv")
    p.add_argument("--report-md", default="", help="Output Markdown report path. Default: scripts_basin_test/output_other/validate_mixed_run/mixed_run_validation_report.md")
    p.add_argument("--report-json", default="", help="Optional output report JSON path")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    release_dir = resolve_release_dir(args.release_dir)
    args.report_csv = resolve_default_report_csv(args.report_csv)
    args.report_md = resolve_default_report_md(args.report_md)
    args.release_dir_resolved = str(release_dir)
    args.scripts_basin_test_root = str(find_scripts_basin_test_root())
    paths = default_paths(release_dir)

    print(f"Resolved scripts_basin_test root: {args.scripts_basin_test_root}")
    print(f"Resolved release dir: {release_dir}")
    print(f"Validation report CSV: {args.report_csv}")
    print(f"Validation report MD: {args.report_md}")

    overrides = {
        "master": args.master_nc,
        "daily": args.daily_nc,
        "monthly": args.monthly_nc,
        "annual": args.annual_nc,
        "station_catalog": args.station_catalog,
        "source_station_catalog": args.source_station_catalog,
        "cluster_points_gpkg": args.cluster_points_gpkg,
        "source_stations_gpkg": args.source_stations_gpkg,
        "cluster_basins_gpkg": args.cluster_basins_gpkg,
    }
    for key, value in overrides.items():
        if value:
            paths[key] = Path(value).expanduser().resolve()

    results: List[CheckResult] = []

    required_files = ["master", "daily", "monthly", "annual", "station_catalog", "source_station_catalog"]
    ok = True
    for key in required_files:
        ok = check_path(results, paths[key], key, required=True) and ok
    if not ok:
        write_reports(args, results)
        print_report(results)
        return 1

    master_ok = require_nc_structure(results, paths["master"], MASTER_REQUIRED_DIMS, MASTER_REQUIRED_VARS, "master NetCDF")
    matrix_ok = True
    for res in RESOLUTIONS:
        matrix_ok = require_nc_structure(results, paths[res], MATRIX_REQUIRED_DIMS, MATRIX_REQUIRED_VARS, f"{res} matrix NetCDF") and matrix_ok
    if not (master_ok and matrix_ok):
        write_reports(args, results)
        print_report(results)
        return 1

    station_raw = pd.read_csv(paths["station_catalog"], keep_default_na=False)
    source_raw = pd.read_csv(paths["source_station_catalog"], keep_default_na=False)
    station_cols_ok = require_columns(results, station_raw, STATION_REQUIRED_COLUMNS, "station_catalog.csv")
    source_cols_ok = require_columns(results, source_raw, SOURCE_REQUIRED_COLUMNS, "source_station_catalog.csv")
    if not (station_cols_ok and source_cols_ok):
        write_reports(args, results)
        print_report(results)
        return 1

    station_catalog, source_catalog = normalize_catalogs(results, station_raw, source_raw)
    check_source_catalog_links(results, source_catalog, station_catalog)

    master_summary, master_source_summary, bad_refs = summarize_master(paths["master"])
    for name, values in bad_refs.items():
        if values:
            add(results, "ERROR", name, "master NetCDF", f"Invalid references: {sorted(values)[:10]}")
        else:
            add(results, "PASS", name, "master NetCDF", "No invalid references found")

    matrix_summaries = []
    matrix_provenance_rows = []
    for res in RESOLUTIONS:
        summary, prov, aux = summarize_matrix(paths[res], res, chunk_rows=max(1, int(args.chunk_rows)))
        matrix_summaries.append(summary)
        if not prov.empty:
            matrix_provenance_rows.append(prov)

        if aux["shape_errors"]:
            add(results, "ERROR", f"matrix_shape_{res}", paths[res].name, "; ".join(aux["shape_errors"]))
        else:
            add(results, "PASS", f"matrix_shape_{res}", paths[res].name, f"n_stations={aux['n_stations']} n_time={aux['n_time']}")

        if aux["count_mismatch_samples"]:
            add(results, "ERROR", f"matrix_declared_valid_count_{res}", paths[res].name, f"n_valid_time_steps does not match actual non-empty Q/SSC/SSL cells; sample={aux['count_mismatch_samples']}")
        else:
            add(results, "PASS", f"matrix_declared_valid_count_{res}", paths[res].name, "n_valid_time_steps matches actual non-empty Q/SSC/SSL cells")

    matrix_summary = pd.concat(matrix_summaries, ignore_index=True) if matrix_summaries else pd.DataFrame()
    matrix_provenance = pd.concat(matrix_provenance_rows, ignore_index=True).drop_duplicates() if matrix_provenance_rows else pd.DataFrame()

    compare_summary_to_catalog(
        results=results,
        label="matrix_vs_station_catalog",
        artifact="matrix/station_catalog.csv",
        observed=matrix_summary,
        catalog=station_catalog,
        count_col_observed="record_count",
        count_col_catalog="record_count",
        key_cols=("cluster_uid", "resolution"),
    )

    compare_summary_to_catalog(
        results=results,
        label="master_vs_station_catalog",
        artifact="master/station_catalog.csv",
        observed=master_summary,
        catalog=station_catalog,
        count_col_observed="record_count",
        count_col_catalog="record_count",
        key_cols=("cluster_uid", "resolution"),
    )

    compare_master_matrix(results, master_summary, matrix_summary)

    compare_summary_to_catalog(
        results=results,
        label="master_source_vs_source_catalog",
        artifact="master/source_station_catalog.csv",
        observed=master_source_summary,
        catalog=source_catalog,
        count_col_observed="n_records",
        count_col_catalog="n_records",
        key_cols=("source_station_uid", "resolution"),
    )

    check_provenance_sources(results, matrix_provenance, master_source_summary, source_catalog)

    check_global_attrs(
        results,
        {
            "master": paths["master"],
            "daily": paths["daily"],
            "monthly": paths["monthly"],
            "annual": paths["annual"],
        },
        require=bool(args.require_run_attrs),
        tolerance_hours=float(args.attr_time_tolerance_hours),
    )

    if not args.skip_gpkg:
        check_gpkg(
            results,
            cluster_points_gpkg=paths["cluster_points_gpkg"],
            source_stations_gpkg=paths["source_stations_gpkg"],
            cluster_basins_gpkg=paths["cluster_basins_gpkg"],
            station_catalog=station_catalog,
            source_catalog=source_catalog,
        )

    write_reports(args, results)
    print_report(results)
    return 1 if any(r.severity == "ERROR" for r in results) else 0


def markdown_cell(value) -> str:
    text = str(value).replace("\n", " ").replace("\r", " ")
    return text.replace("|", "\\|")


def build_markdown_report(args, results: List[CheckResult]) -> str:
    n_error = sum(r.severity == "ERROR" for r in results)
    n_warn = sum(r.severity == "WARN" for r in results)
    n_pass = sum(r.severity == "PASS" for r in results)
    status = "FAILED" if n_error else "OK"

    lines = [
        "# sed_reference mixed-run validation report",
        "",
        f"Status: **{status}**",
        "",
        "## Summary",
        "",
        "| PASS | WARN | ERROR |",
        "| ---: | ---: | ---: |",
        f"| {n_pass} | {n_warn} | {n_error} |",
        "",
        "## Paths",
        "",
        f"- scripts_basin_test root: `{getattr(args, 'scripts_basin_test_root', '')}`",
        f"- release dir: `{getattr(args, 'release_dir_resolved', '')}`",
        f"- CSV report: `{getattr(args, 'report_csv', '')}`",
        f"- Markdown report: `{getattr(args, 'report_md', '')}`",
    ]
    if getattr(args, "report_json", ""):
        lines.append(f"- JSON report: `{args.report_json}`")

    for severity in ("ERROR", "WARN", "PASS"):
        subset = [r for r in results if r.severity == severity]
        if not subset:
            continue
        lines.extend([
            "",
            f"## {severity} Checks",
            "",
            "| Check | Artifact | Details |",
            "| --- | --- | --- |",
        ])
        for r in subset:
            lines.append(f"| {markdown_cell(r.check)} | {markdown_cell(r.artifact)} | {markdown_cell(r.details)} |")

    lines.append("")
    return "\n".join(lines)


def write_reports(args, results: List[CheckResult]) -> None:
    rows = [r.as_row() for r in results]
    if args.report_csv:
        out = Path(args.report_csv).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["severity", "check", "artifact", "details"])
            writer.writeheader()
            writer.writerows(rows)
    if getattr(args, "report_md", ""):
        out = Path(args.report_md).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(build_markdown_report(args, results), encoding="utf-8")
    if args.report_json:
        out = Path(args.report_json).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


def print_report(results: List[CheckResult]) -> None:
    n_error = sum(r.severity == "ERROR" for r in results)
    n_warn = sum(r.severity == "WARN" for r in results)
    n_pass = sum(r.severity == "PASS" for r in results)

    print("\n=== sed_reference mixed-run validation ===")
    print(f"PASS={n_pass} WARN={n_warn} ERROR={n_error}")

    for severity in ("ERROR", "WARN"):
        subset = [r for r in results if r.severity == severity]
        if not subset:
            continue
        print(f"\n{severity}:")
        for r in subset:
            print(f"  - [{r.check}] {r.artifact}: {r.details}")

    if n_error == 0:
        print("\nOK: no ERROR checks found.")
    else:
        print("\nFAILED: mixed-run release gate found ERROR checks.")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(2)
