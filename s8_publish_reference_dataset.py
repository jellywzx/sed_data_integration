#!/usr/bin/env python3
"""
Publish the sediment reference dataset as a user-facing release package.

This script does not rebuild the upstream basin pipeline. Instead, it packages
existing s6 / s7 outputs into a release-oriented layout with:

1. canonical NetCDF file names for end users;
2. resolution-aware station/source catalogs for fast lookup and provenance;
3. multi-layer GPKG spatial sidecars for GIS users;
4. a release README and a small validation report.

Default inputs:
  - scripts_basin_test/output/s6_basin_merged_all.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_daily.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_monthly.nc
  - scripts_basin_test/output/s6_matrix_by_resolution/s6_basin_matrix_annual.nc
  - scripts_basin_test/output/s6_climatology_only.nc
  - scripts_basin_test/output/s7_cluster_station_catalog.csv
  - scripts_basin_test/output/s7_cluster_resolution_catalog.csv
  - scripts_basin_test/output/s7_source_station_resolution_catalog.csv
  - scripts_basin_test/output/s7_cluster_basins.gpkg   (optional sidecar source)

Default output directory:
  - scripts_basin_test/output/sed_reference_release/
"""

import argparse
import os
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from cluster_spatial_catalog import (
    CLUSTER_RESOLUTIONS,
    HAS_GPD,
    HAS_NC,
    RESOLUTION_CODE_TO_NAME,
    RESOLUTION_NAME_TO_CODE,
    build_source_dataset_catalog,
    normalize_cluster_resolution_catalog,
    normalize_cluster_station_catalog,
    normalize_source_station_resolution_catalog,
    write_cluster_points_gpkg,
    write_source_stations_gpkg,
)
from pipeline_paths import (
    RELEASE_CLUSTER_BASINS_GPKG,
    RELEASE_CLUSTER_POINTS_GPKG,
    RELEASE_CLIMATOLOGY_NC,
    RELEASE_DATASET_DIR,
    RELEASE_INVENTORY_CSV,
    RELEASE_MASTER_NC,
    RELEASE_MATRIX_ANNUAL_NC,
    RELEASE_MATRIX_DAILY_NC,
    RELEASE_MATRIX_MONTHLY_NC,
    RELEASE_OVERLAP_CANDIDATES_CSV,
    RELEASE_README_MD,
    RELEASE_SOURCE_DATASET_CATALOG_CSV,
    RELEASE_SOURCE_STATION_CATALOG_CSV,
    RELEASE_SOURCE_STATIONS_GPKG,
    RELEASE_STATION_CATALOG_CSV,
    RELEASE_VALIDATION_CSV,
    S2_ORGANIZED_DIR,
    S6_CLIMATOLOGY_NC,
    S6_QUALITY_ORDER_CSV,
    S6_MATRIX_DIR,
    S6_MERGED_NC,
    S7_CLUSTER_BASINS_GPKG,
    S7_CLUSTER_RESOLUTION_CATALOG_CSV,
    S7_CLUSTER_STATION_CATALOG_CSV,
    S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV,
    get_output_r_root,
)
from qc_contract import (
    FINAL_Q_FLAG_NAMES,
    FINAL_SSC_FLAG_NAMES,
    FINAL_SSL_FLAG_NAMES,
    Q_VAR_NAMES,
    SSC_VAR_NAMES,
    SSL_VAR_NAMES,
    TIME_VAR_NAMES,
)

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_MASTER_NC = PROJECT_ROOT / S6_MERGED_NC
DEFAULT_MATRIX_DAILY = PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_daily.nc"
DEFAULT_MATRIX_MONTHLY = PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_monthly.nc"
DEFAULT_MATRIX_ANNUAL = PROJECT_ROOT / S6_MATRIX_DIR / "s6_basin_matrix_annual.nc"
DEFAULT_CLIM_NC = PROJECT_ROOT / S6_CLIMATOLOGY_NC
DEFAULT_CLUSTER_STATION_CATALOG_INPUT = PROJECT_ROOT / S7_CLUSTER_STATION_CATALOG_CSV
DEFAULT_CLUSTER_RESOLUTION_CATALOG_INPUT = PROJECT_ROOT / S7_CLUSTER_RESOLUTION_CATALOG_CSV
DEFAULT_SOURCE_STATION_RESOLUTION_CATALOG_INPUT = PROJECT_ROOT / S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV
DEFAULT_CLUSTER_BASIN_VECTOR = PROJECT_ROOT / S7_CLUSTER_BASINS_GPKG
DEFAULT_RELEASE_DIR = PROJECT_ROOT / RELEASE_DATASET_DIR
DEFAULT_RELEASE_README = PROJECT_ROOT / RELEASE_README_MD
DEFAULT_STATION_CATALOG = PROJECT_ROOT / RELEASE_STATION_CATALOG_CSV
DEFAULT_SOURCE_STATION_CATALOG = PROJECT_ROOT / RELEASE_SOURCE_STATION_CATALOG_CSV
DEFAULT_SOURCE_DATASET_CATALOG = PROJECT_ROOT / RELEASE_SOURCE_DATASET_CATALOG_CSV
DEFAULT_OVERLAP_CANDIDATES_CSV = PROJECT_ROOT / RELEASE_OVERLAP_CANDIDATES_CSV
DEFAULT_CLUSTER_POINTS_GPKG = PROJECT_ROOT / RELEASE_CLUSTER_POINTS_GPKG
DEFAULT_SOURCE_STATIONS_GPKG = PROJECT_ROOT / RELEASE_SOURCE_STATIONS_GPKG
DEFAULT_CLUSTER_BASINS_GPKG = PROJECT_ROOT / RELEASE_CLUSTER_BASINS_GPKG
DEFAULT_VALIDATION_CSV = PROJECT_ROOT / RELEASE_VALIDATION_CSV
DEFAULT_INVENTORY_CSV = PROJECT_ROOT / RELEASE_INVENTORY_CSV
DEFAULT_EXAMPLE_SCRIPT = SCRIPT_DIR / "example_reference_workflow.py"
DEFAULT_QUALITY_ORDER_CSV = PROJECT_ROOT / S6_QUALITY_ORDER_CSV
ORGANIZED_ROOT = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()

CORE_FILE_SPECS = (
    ("master", RELEASE_MASTER_NC, "Authoritative record-level reference dataset"),
    ("daily", RELEASE_MATRIX_DAILY_NC, "Daily station x time matrix for validation"),
    ("monthly", RELEASE_MATRIX_MONTHLY_NC, "Monthly station x time matrix for validation"),
    ("annual", RELEASE_MATRIX_ANNUAL_NC, "Annual station x time matrix for validation"),
    ("climatology", RELEASE_CLIMATOLOGY_NC, "Standalone climatology reference dataset"),
)
OVERLAP_CANDIDATES_FILE_NAME = Path(RELEASE_OVERLAP_CANDIDATES_CSV).name
OVERLAP_CANDIDATE_COLUMNS = [
    "cluster_uid",
    "cluster_id",
    "resolution",
    "time",
    "date",
    "source",
    "source_family",
    "source_station_uid",
    "source_station_index",
    "source_station_native_id",
    "source_station_name",
    "source_station_river_name",
    "source_station_paths",
    "source_station_lat",
    "source_station_lon",
    "candidate_path",
    "candidate_rank",
    "candidate_quality_score",
    "selected_flag",
    "is_overlap",
    "n_candidates_at_time",
    "n_ranked_candidates_for_cluster_resolution",
    "candidate_group_key",
    "selection_reason",
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
OVERLAP_CANDIDATE_REQUIRED_COLUMNS = set(OVERLAP_CANDIDATE_COLUMNS)
OVERLAP_CANDIDATE_METHOD_NOTES = (
    "candidate-level values rebuilt during s8 from s6_cluster_quality_order.csv "
    "and source station NetCDF files; selected_flag follows s6 quality-rank fallback per date"
)
OVERLAP_CANDIDATE_ASSUMPTIONS = (
    "dates are normalized to YYYY-MM-DD; overlap means at least two candidate source-station rows "
    "with any Q/SSC/SSL value for the same cluster/resolution/date"
)
FULL_CHAIN_RERUN_HINT = (
    "Likely mixed-run outputs from different pipeline executions. "
    "Please rerun the full chain: s1 -> s2 -> s3 -> s4 -> s5 -> s6 -> s7 -> s8 before publishing."
)


def _clean_text(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _read_text_var(ds, name, size=None):
    if name not in ds.variables:
        return [""] * int(size or 0)
    values = ds.variables[name][:]
    arr = np.asarray(values, dtype=object).reshape(-1)
    return [_clean_text(item) for item in arr]


def _read_float_array(ds, name, fill_values=None, size=None):
    if name not in ds.variables:
        return np.full(int(size or 0), np.nan, dtype=np.float64)
    arr = np.ma.asarray(ds.variables[name][:]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype=np.float64)
    if fill_values:
        for fill in fill_values:
            arr[arr == fill] = np.nan
    return arr


def _read_int_array(ds, name, fill_value=-1, size=None):
    if name not in ds.variables:
        return np.full(int(size or 0), fill_value, dtype=np.int64)
    raw = np.ma.asarray(ds.variables[name][:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        arr = raw.filled(fill_value)
    else:
        arr = np.asarray(raw)
    result = np.full(arr.shape, fill_value, dtype=np.int64)
    valid = np.isfinite(arr.astype(np.float64, copy=False))
    result[valid] = arr[valid].astype(np.int64)
    return result


def _ensure_removed(path):
    if not path.exists():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    else:
        path.unlink()


def _prepare_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def _link_or_copy_file(src, dst, mode="hardlink", force=False):
    src = Path(src).resolve()
    dst = Path(dst)
    if not src.is_file():
        raise FileNotFoundError("Missing source file: {}".format(src))
    if dst.exists() or dst.is_symlink():
        if not force and dst.resolve() == src:
            return dst
        _ensure_removed(dst)
    _prepare_parent(dst)
    if mode == "hardlink":
        try:
            os.link(str(src), str(dst))
            return dst
        except OSError:
            mode = "copy"
    if mode == "symlink":
        dst.symlink_to(src)
        return dst
    shutil.copy2(str(src), str(dst))
    return dst


def _write_csv(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def classify_source_family(source):
    text = "" if source is None else str(source)
    low = text.lower()
    if "usgs" in low:
        return "USGS"
    if "hydat" in low:
        return "HYDAT"
    if any(token in low for token in ("riversed", "gsed", "dethier", "aquasat")):
        return "satellite"
    if any(token in low for token in ("grdc", "hybam", "in situ", "insitu")):
        return "in_situ"
    if any(token in low for token in ("compiled", "compilation", "secondary")):
        return "secondary_compilation"
    return "other"


def _first_existing(columns, candidates):
    column_set = set(columns)
    for name in candidates:
        if name in column_set:
            return name
    return None


def _empty_overlap_candidates_frame():
    return pd.DataFrame(columns=OVERLAP_CANDIDATE_COLUMNS)


def _portable_candidate_path(path_text):
    text = _clean_text(path_text)
    if not text:
        return ""
    path = Path(text)
    try:
        resolved = path.resolve()
        try:
            return str(resolved.relative_to(ORGANIZED_ROOT))
        except ValueError:
            pass
        marker = "output_resolution_organized"
        parts = resolved.parts
        for i, part in enumerate(parts):
            if part == marker and i + 1 < len(parts):
                return str(Path(*parts[i + 1:]))
    except Exception:
        pass
    return text


def _resolve_candidate_path(path_text):
    text = _clean_text(path_text)
    if not text:
        return Path(text)
    path = Path(text)
    if not path.is_absolute():
        return (ORGANIZED_ROOT / path).resolve()
    if path.is_file():
        return path
    try:
        marker = "output_resolution_organized"
        parts = path.parts
        for i, part in enumerate(parts):
            if part == marker and i + 1 < len(parts):
                candidate = (ORGANIZED_ROOT / Path(*parts[i + 1:])).resolve()
                if candidate.is_file():
                    return candidate
    except Exception:
        pass
    return path


def _first_nc_variable(ds, names):
    for name in names:
        if name in ds.variables:
            return name, ds.variables[name]
    return None, None


def _pad_array(values, size, fill_value):
    arr = np.asarray(values).reshape(-1)
    if len(arr) >= size:
        return arr[:size]
    return np.concatenate([arr, np.full(size - len(arr), fill_value)])


def _read_candidate_numeric_var(ds, names, size):
    name, var = _first_nc_variable(ds, names)
    if var is None:
        return np.full(size, np.nan, dtype=np.float64), ""
    arr = np.ma.asarray(var[:]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype=np.float64)
    arr = _pad_array(arr, size, np.nan).astype(np.float64)
    for fill_value in (-9999.0, -9999, 1.0e20):
        arr[arr == fill_value] = np.nan
    units = _clean_text(getattr(var, "units", ""))
    return arr, units


def _read_candidate_flag_var(ds, names, size):
    name, var = _first_nc_variable(ds, names)
    if var is None:
        return np.full(size, 9, dtype=np.int16)
    raw = np.ma.asarray(var[:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(9)
    raw = _pad_array(raw, size, 9)
    numeric = pd.to_numeric(pd.Series(raw), errors="coerce").fillna(9)
    return numeric.astype(np.int16).to_numpy()


def _decode_candidate_dates(time_var):
    raw = np.asarray(time_var[:]).reshape(-1)
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    try:
        decoded = nc4.num2date(raw, units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        try:
            decoded = nc4.num2date(raw, units, calendar=calendar)
        except Exception:
            decoded = pd.to_datetime(raw, unit="D", origin="1970-01-01")
    except Exception:
        decoded = pd.to_datetime(raw, unit="D", origin="1970-01-01")

    dates = []
    for item in list(decoded):
        try:
            ts = pd.Timestamp(item)
        except Exception:
            if hasattr(item, "isoformat"):
                ts = pd.Timestamp(item.isoformat())
            elif hasattr(item, "year"):
                ts = pd.Timestamp(item.year, item.month, item.day)
            else:
                ts = pd.Timestamp(str(item))
        dates.append(ts.normalize())
    return dates


def _read_candidate_series(path):
    if nc4 is None:
        return None, {}, "netCDF4 is unavailable"
    try:
        with nc4.Dataset(path, "r") as ds:
            time_name, time_var = _first_nc_variable(ds, TIME_VAR_NAMES)
            if time_var is None:
                return None, {}, "time variable not found"
            dates = _decode_candidate_dates(time_var)
            n = len(dates)
            if n == 0:
                return None, {}, "empty time variable"

            q, q_units = _read_candidate_numeric_var(ds, Q_VAR_NAMES, n)
            ssc, ssc_units = _read_candidate_numeric_var(ds, SSC_VAR_NAMES, n)
            ssl, ssl_units = _read_candidate_numeric_var(ds, SSL_VAR_NAMES, n)
            df = pd.DataFrame(
                {
                    "date": [ts.strftime("%Y-%m-%d") for ts in dates],
                    "time": [float((ts - pd.Timestamp("1970-01-01")).days) for ts in dates],
                    "Q": q,
                    "SSC": ssc,
                    "SSL": ssl,
                    "Q_flag": _read_candidate_flag_var(ds, FINAL_Q_FLAG_NAMES, n),
                    "SSC_flag": _read_candidate_flag_var(ds, FINAL_SSC_FLAG_NAMES, n),
                    "SSL_flag": _read_candidate_flag_var(ds, FINAL_SSL_FLAG_NAMES, n),
                }
            )
            df = df[df[["Q", "SSC", "SSL"]].notna().any(axis=1)].copy()
            if df.empty:
                return df, {"Q": q_units, "SSC": ssc_units, "SSL": ssl_units}, "no non-empty Q/SSC/SSL rows"
            df = df.drop_duplicates("date", keep="first").reset_index(drop=True)
            return df, {"Q": q_units, "SSC": ssc_units, "SSL": ssl_units}, ""
    except Exception as exc:
        return None, {}, "cannot read candidate NetCDF: {}".format(exc)


def _source_catalog_lookup(source_station_catalog):
    lookup = {}
    if source_station_catalog is None or source_station_catalog.empty:
        return lookup
    work = source_station_catalog.copy()
    uid_col = _first_existing(work.columns, ("source_station_uid", "station_uid"))
    idx_col = _first_existing(work.columns, ("source_station_index", "selected_source_index"))
    res_col = _first_existing(work.columns, ("resolution", "time_resolution"))
    if uid_col:
        for _, row in work.iterrows():
            uid = _clean_text(row.get(uid_col, ""))
            resolution = _clean_text(row.get(res_col, "")) if res_col else ""
            if uid:
                lookup[("uid", uid, resolution)] = row
                lookup.setdefault(("uid", uid, ""), row)
    if idx_col:
        for _, row in work.iterrows():
            try:
                idx = int(row.get(idx_col, -1))
            except Exception:
                continue
            resolution = _clean_text(row.get(res_col, "")) if res_col else ""
            lookup[("idx", idx, resolution)] = row
            lookup.setdefault(("idx", idx, ""), row)
    return lookup


def _catalog_value(row, names, default=""):
    if row is None:
        return default
    name = _first_existing(row.index, names)
    if not name:
        return default
    return _clean_text(row.get(name, default))


def _catalog_float(row, names):
    value = _catalog_value(row, names, "")
    try:
        return float(value)
    except Exception:
        return np.nan


def validate_overlap_candidates_sidecar(path):
    path = Path(path)
    if not path.is_file():
        return False, "overlap candidates sidecar not found"
    try:
        header = pd.read_csv(path, nrows=0)
    except Exception as exc:
        return False, "cannot read overlap candidates sidecar header: {}".format(exc)
    missing = sorted(OVERLAP_CANDIDATE_REQUIRED_COLUMNS - set(header.columns))
    if missing:
        return False, "missing columns: {}".format(", ".join(missing))
    return True, "schema ok"


def build_overlap_candidates_sidecar(
    quality_order_csv,
    source_station_catalog,
    out_path,
    mode="overlap-only",
):
    out_path = Path(out_path)
    quality_order_csv = Path(quality_order_csv)
    if mode not in {"overlap-only", "all-candidates"}:
        return None, 0, "unsupported overlap candidate mode: {}".format(mode)
    if nc4 is None:
        return None, 0, "netCDF4 is unavailable"
    if not quality_order_csv.is_file():
        return None, 0, "missing upstream quality order CSV: {}".format(quality_order_csv)

    required = {
        "cluster_id",
        "resolution",
        "quality_rank",
        "source",
        "source_station_index",
        "source_station_uid",
        "path",
        "quality_score",
    }
    quality = pd.read_csv(quality_order_csv, keep_default_na=False)
    missing = sorted(required - set(quality.columns))
    if missing:
        return None, 0, "quality order CSV missing columns: {}".format(", ".join(missing))
    if quality.empty:
        frame = _empty_overlap_candidates_frame()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(out_path, index=False, compression="gzip")
        return out_path, 0, "quality order CSV is empty; wrote header-only sidecar"

    for col in ("cluster_id", "quality_rank", "source_station_index"):
        quality[col] = pd.to_numeric(quality[col], errors="coerce").fillna(-1).astype(int)
    quality["quality_score"] = pd.to_numeric(quality["quality_score"], errors="coerce")
    if "cluster_uid" not in quality.columns:
        quality["cluster_uid"] = ""
    quality["cluster_uid"] = quality["cluster_uid"].astype(str)
    quality.loc[quality["cluster_uid"].str.strip().eq(""), "cluster_uid"] = quality.loc[
        quality["cluster_uid"].str.strip().eq(""),
        "cluster_id",
    ].map(lambda cid: "SED{:06d}".format(int(cid)) if int(cid) >= 0 else "")

    catalog_lookup = _source_catalog_lookup(source_station_catalog)
    rows = []
    warnings = []
    n_unreadable = 0
    n_groups = 0
    n_groups_with_rows = 0

    group_cols = ["cluster_id", "resolution"]
    for (cluster_id, resolution), group in quality.groupby(group_cols, sort=True):
        n_groups += 1
        group = group.sort_values(["quality_rank", "source_station_index", "path"]).reset_index(drop=True)
        candidates = []
        for _, qrow in group.iterrows():
            raw_path = _clean_text(qrow.get("path", ""))
            resolved_path = _resolve_candidate_path(raw_path)
            if not resolved_path.is_file():
                n_unreadable += 1
                if len(warnings) < 8:
                    warnings.append("missing candidate file: {}".format(raw_path))
                continue
            series, units, read_note = _read_candidate_series(resolved_path)
            if series is None:
                n_unreadable += 1
                if len(warnings) < 8:
                    warnings.append("{}: {}".format(raw_path, read_note))
                continue
            if series.empty:
                continue
            uid = _clean_text(qrow.get("source_station_uid", ""))
            try:
                source_station_index = int(qrow.get("source_station_index", -1))
            except Exception:
                source_station_index = -1
            catalog_row = catalog_lookup.get(("uid", uid, str(resolution)))
            if catalog_row is None:
                catalog_row = catalog_lookup.get(("uid", uid, ""))
            if catalog_row is None and source_station_index >= 0:
                catalog_row = catalog_lookup.get(("idx", source_station_index, str(resolution)))
                if catalog_row is None:
                    catalog_row = catalog_lookup.get(("idx", source_station_index, ""))
            candidates.append(
                {
                    "quality": qrow,
                    "series": series.set_index("date"),
                    "units": units,
                    "catalog": catalog_row,
                    "portable_path": _portable_candidate_path(raw_path),
                    "source_station_index": source_station_index,
                }
            )

        if not candidates:
            continue
        all_dates = sorted(set().union(*(set(item["series"].index) for item in candidates)))
        group_rows_before = len(rows)
        for date_text in all_dates:
            present = [item for item in candidates if date_text in item["series"].index]
            if not present:
                continue
            is_overlap = int(len(present) > 1)
            if mode == "overlap-only" and not is_overlap:
                continue
            selected_item = present[0]
            for item in present:
                qrow = item["quality"]
                data_row = item["series"].loc[date_text]
                if isinstance(data_row, pd.DataFrame):
                    data_row = data_row.iloc[0]
                source = _clean_text(qrow.get("source", ""))
                source_family = _catalog_value(
                    item["catalog"],
                    ("source_family", "source_type", "source_category"),
                    "",
                ) or classify_source_family(source)
                source_station_uid = _clean_text(qrow.get("source_station_uid", ""))
                cluster_uid = _clean_text(qrow.get("cluster_uid", ""))
                candidate_group_key = "{}|{}|{}".format(cluster_uid, resolution, date_text)
                rows.append(
                    {
                        "cluster_uid": cluster_uid,
                        "cluster_id": int(cluster_id),
                        "resolution": str(resolution),
                        "time": float(data_row.get("time", np.nan)),
                        "date": date_text,
                        "source": source,
                        "source_family": source_family,
                        "source_station_uid": source_station_uid,
                        "source_station_index": int(item["source_station_index"]),
                        "source_station_native_id": _catalog_value(
                            item["catalog"],
                            ("source_station_native_id", "native_id", "source_station_id"),
                        ),
                        "source_station_name": _catalog_value(
                            item["catalog"],
                            ("source_station_name", "station_name"),
                        ),
                        "source_station_river_name": _catalog_value(
                            item["catalog"],
                            ("source_station_river_name", "river_name"),
                        ),
                        "source_station_paths": _catalog_value(
                            item["catalog"],
                            ("source_station_paths", "paths", "path"),
                        ),
                        "source_station_lat": _catalog_float(
                            item["catalog"],
                            ("source_station_lat", "lat"),
                        ),
                        "source_station_lon": _catalog_float(
                            item["catalog"],
                            ("source_station_lon", "lon"),
                        ),
                        "candidate_path": item["portable_path"],
                        "candidate_rank": int(qrow.get("quality_rank", -1)),
                        "candidate_quality_score": float(qrow.get("quality_score", np.nan)),
                        "selected_flag": int(item is selected_item),
                        "is_overlap": is_overlap,
                        "n_candidates_at_time": int(len(present)),
                        "n_ranked_candidates_for_cluster_resolution": int(len(group)),
                        "candidate_group_key": candidate_group_key,
                        "selection_reason": "first non-empty candidate by s6 quality_rank for this date",
                        "Q": data_row.get("Q", np.nan),
                        "SSC": data_row.get("SSC", np.nan),
                        "SSL": data_row.get("SSL", np.nan),
                        "Q_flag": int(data_row.get("Q_flag", 9)),
                        "SSC_flag": int(data_row.get("SSC_flag", 9)),
                        "SSL_flag": int(data_row.get("SSL_flag", 9)),
                        "Q_units": item["units"].get("Q", ""),
                        "SSC_units": item["units"].get("SSC", ""),
                        "SSL_units": item["units"].get("SSL", ""),
                        "has_Q": int(pd.notna(data_row.get("Q", np.nan))),
                        "has_SSC": int(pd.notna(data_row.get("SSC", np.nan))),
                        "has_SSL": int(pd.notna(data_row.get("SSL", np.nan))),
                        "method_notes": OVERLAP_CANDIDATE_METHOD_NOTES,
                        "assumptions": OVERLAP_CANDIDATE_ASSUMPTIONS,
                    }
                )
        if len(rows) > group_rows_before:
            n_groups_with_rows += 1

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = _empty_overlap_candidates_frame()
    else:
        frame = frame.reindex(columns=OVERLAP_CANDIDATE_COLUMNS)
        frame = frame.sort_values(
            ["resolution", "cluster_uid", "time", "date", "candidate_rank", "source_station_uid"],
            kind="mergesort",
        ).reset_index(drop=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out_path, index=False, compression="gzip")
    ok, detail = validate_overlap_candidates_sidecar(out_path)
    if not ok:
        return out_path, len(frame), "generated sidecar failed schema validation: {}".format(detail)

    detail = "mode={} rows={} groups_with_rows={}/{}".format(mode, len(frame), n_groups_with_rows, n_groups)
    if n_unreadable:
        detail += "; unreadable_candidates={}".format(n_unreadable)
    if warnings:
        detail += "; warnings={}".format(" | ".join(warnings))
    return out_path, len(frame), detail


def _canonical_core_sources(master_nc, daily_nc, monthly_nc, annual_nc, climatology_nc):
    return {
        "master": Path(master_nc).resolve(),
        "daily": Path(daily_nc).resolve(),
        "monthly": Path(monthly_nc).resolve(),
        "annual": Path(annual_nc).resolve(),
        "climatology": Path(climatology_nc).resolve(),
    }


def _relative_to_release(path, release_dir):
    try:
        return str(Path(path).resolve().relative_to(Path(release_dir).resolve()))
    except Exception:
        return str(Path(path))


def write_inventory(file_records, out_path, release_dir):
    rows = []
    for kind, path, description in file_records:
        path = Path(path)
        if not path.exists():
            continue
        rows.append(
            {
                "kind": kind,
                "file_name": path.name,
                "relative_path": _relative_to_release(path, release_dir),
                "description": description,
                "file_size_mb": round(path.stat().st_size / (1024 * 1024), 3),
            }
        )
    df = pd.DataFrame(rows)
    if len(df):
        df = df.sort_values(["kind", "file_name"]).reset_index(drop=True)
    return _write_csv(df, out_path)


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1 = np.deg2rad(np.asarray(lat1, dtype=np.float64))
    lon1 = np.deg2rad(np.asarray(lon1, dtype=np.float64))
    lat2 = np.deg2rad(np.asarray(lat2, dtype=np.float64))
    lon2 = np.deg2rad(np.asarray(lon2, dtype=np.float64))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _first_overlap_sample(ds):
    if "is_overlap" not in ds.variables or "selected_source_index" not in ds.variables:
        return None
    n_stations = len(ds.dimensions["n_stations"])
    n_time = len(ds.dimensions["time"])
    overlap_var = ds.variables["is_overlap"]
    source_var = ds.variables["selected_source_index"]
    for start in range(0, n_stations, 32):
        stop = min(start + 32, n_stations)
        overlap = np.ma.asarray(overlap_var[start:stop, :]).filled(0)
        selected = np.ma.asarray(source_var[start:stop, :]).filled(-1)
        mask = (overlap == 1) & (selected >= 0)
        hits = np.argwhere(mask)
        if hits.size == 0:
            continue
        local_row, col = hits[0]
        station_idx = start + int(local_row)
        if station_idx < n_stations and int(col) < n_time:
            return station_idx, int(col), int(selected[local_row, col])
    return None


def _find_master_record_index(master_ds, station_index, resolution_code, target_time_num, chunk_size=500000):
    n_records = len(master_ds.dimensions["n_records"])
    station_var = master_ds.variables["station_index"]
    resolution_var = master_ds.variables["resolution"]
    time_var = master_ds.variables["time"]

    for start in range(0, n_records, chunk_size):
        stop = min(start + chunk_size, n_records)
        station_chunk = np.asarray(station_var[start:stop], dtype=np.int32).reshape(-1)
        resolution_chunk = np.asarray(resolution_var[start:stop], dtype=np.int16).reshape(-1)
        time_chunk = np.asarray(time_var[start:stop], dtype=np.float64).reshape(-1)
        mask = (
            (station_chunk == int(station_index))
            & (resolution_chunk == int(resolution_code))
            & np.isclose(time_chunk, float(target_time_num))
        )
        hit = np.flatnonzero(mask)
        if len(hit) > 0:
            return start + int(hit[0])
    return None


def _empty_core_stats():
    return {"record_count": 0, "time_start": "", "time_end": ""}


def _format_timestamp_text(value):
    text = _clean_text(value)
    if not text:
        return ""
    try:
        return pd.Timestamp(text).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text


def _format_num_time(value, units, calendar):
    try:
        if value is None or not np.isfinite(float(value)):
            return ""
    except Exception:
        return ""
    try:
        decoded = nc4.num2date(
            float(value),
            units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
        )
    except TypeError:
        decoded = nc4.num2date(
            float(value),
            units,
            calendar=calendar,
        )
    return _format_timestamp_text(decoded)


def _format_stats(stats):
    return "records={record_count}, time_start={time_start}, time_end={time_end}".format(
        record_count=int(stats.get("record_count", 0) or 0),
        time_start=stats.get("time_start", "") or "(empty)",
        time_end=stats.get("time_end", "") or "(empty)",
    )


def _format_coverage(summary):
    coverage = [
        resolution
        for resolution in CLUSTER_RESOLUTIONS
        if int(summary.get(resolution, {}).get("record_count", 0) or 0) > 0
    ]
    return "|".join(coverage) if coverage else "(none)"


def _summarize_master_core(master_ds):
    summary = {resolution: _empty_core_stats() for resolution in CLUSTER_RESOLUTIONS}
    n_records = len(master_ds.dimensions.get("n_records", []))
    if n_records <= 0 or "resolution" not in master_ds.variables or "time" not in master_ds.variables:
        return summary

    resolution_codes = _read_int_array(master_ds, "resolution", fill_value=-1, size=n_records)
    time_nums = _read_float_array(master_ds, "time", size=n_records)
    time_var = master_ds.variables["time"]
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")

    for resolution in CLUSTER_RESOLUTIONS:
        resolution_code = RESOLUTION_NAME_TO_CODE[resolution]
        mask = resolution_codes == int(resolution_code)
        if not mask.any():
            continue
        valid_times = time_nums[mask]
        valid_times = valid_times[np.isfinite(valid_times)]
        summary[resolution] = {
            "record_count": int(mask.sum()),
            "time_start": _format_num_time(valid_times.min(), units, calendar) if len(valid_times) else "",
            "time_end": _format_num_time(valid_times.max(), units, calendar) if len(valid_times) else "",
        }
    return summary


def _summarize_matrix_core(matrix_paths):
    summary = {resolution: _empty_core_stats() for resolution in CLUSTER_RESOLUTIONS}
    for resolution in CLUSTER_RESOLUTIONS:
        path = Path(matrix_paths.get(resolution, ""))
        if not path.is_file():
            continue
        with nc4.Dataset(path, "r") as ds:
            n_stations = len(ds.dimensions.get("n_stations", []))
            valid_counts = _read_int_array(
                ds,
                "n_valid_time_steps",
                fill_value=0,
                size=n_stations,
            )
            time_var = ds.variables.get("time")
            time_values = np.asarray(time_var[:], dtype=np.float64).reshape(-1) if time_var is not None else np.array([], dtype=np.float64)
            summary[resolution] = {
                "record_count": int(valid_counts.sum()),
                "time_start": _format_num_time(
                    time_values[0],
                    getattr(time_var, "units", "days since 1970-01-01"),
                    getattr(time_var, "calendar", "gregorian"),
                ) if len(time_values) else "",
                "time_end": _format_num_time(
                    time_values[-1],
                    getattr(time_var, "units", "days since 1970-01-01"),
                    getattr(time_var, "calendar", "gregorian"),
                ) if len(time_values) else "",
            }
    return summary


def _summarize_cluster_station_catalog_core(cluster_station_catalog):
    summary = {resolution: _empty_core_stats() for resolution in CLUSTER_RESOLUTIONS}
    work = normalize_cluster_station_catalog(cluster_station_catalog)
    for resolution in CLUSTER_RESOLUTIONS:
        count_col = "{}_record_count".format(resolution)
        start_col = "{}_time_start".format(resolution)
        end_col = "{}_time_end".format(resolution)
        count_values = pd.to_numeric(work[count_col], errors="coerce").fillna(0).astype(np.int64)
        subset = work[count_values > 0].copy()
        if len(subset) == 0:
            continue
        start_times = pd.to_datetime(subset[start_col], errors="coerce")
        end_times = pd.to_datetime(subset[end_col], errors="coerce")
        summary[resolution] = {
            "record_count": int(count_values[count_values > 0].sum()),
            "time_start": _format_timestamp_text(start_times.min()) if start_times.notna().any() else "",
            "time_end": _format_timestamp_text(end_times.max()) if end_times.notna().any() else "",
        }
    return summary


def _summarize_cluster_resolution_catalog_core(cluster_resolution_catalog):
    summary = {resolution: _empty_core_stats() for resolution in CLUSTER_RESOLUTIONS}
    work = normalize_cluster_resolution_catalog(cluster_resolution_catalog)
    for resolution in CLUSTER_RESOLUTIONS:
        subset = work[work["resolution"].astype(str).str.strip().eq(resolution)].copy()
        if len(subset) == 0:
            continue
        start_times = pd.to_datetime(subset["time_start"], errors="coerce")
        end_times = pd.to_datetime(subset["time_end"], errors="coerce")
        summary[resolution] = {
            "record_count": int(pd.to_numeric(subset["record_count"], errors="coerce").fillna(0).sum()),
            "time_start": _format_timestamp_text(start_times.min()) if start_times.notna().any() else "",
            "time_end": _format_timestamp_text(end_times.max()) if end_times.notna().any() else "",
        }
    return summary


def _summarize_source_station_catalog_core(source_station_catalog):
    summary = {resolution: _empty_core_stats() for resolution in CLUSTER_RESOLUTIONS}
    work = normalize_source_station_resolution_catalog(source_station_catalog)
    for resolution in CLUSTER_RESOLUTIONS:
        subset = work[work["resolution"].astype(str).str.strip().eq(resolution)].copy()
        if len(subset) == 0:
            continue
        start_times = pd.to_datetime(subset["time_start"], errors="coerce")
        end_times = pd.to_datetime(subset["time_end"], errors="coerce")
        summary[resolution] = {
            "record_count": int(pd.to_numeric(subset["n_records"], errors="coerce").fillna(0).sum()),
            "time_start": _format_timestamp_text(start_times.min()) if start_times.notna().any() else "",
            "time_end": _format_timestamp_text(end_times.max()) if end_times.notna().any() else "",
        }
    return summary


def _summarize_climatology_core(climatology_nc):
    path = Path(climatology_nc)
    summary = {"record_count": 0, "time_start": "", "time_end": "", "resolution_codes": []}
    if not path.is_file():
        return summary
    with nc4.Dataset(path, "r") as ds:
        n_records = len(ds.dimensions.get("n_records", []))
        summary["record_count"] = int(n_records)
        if "resolution" in ds.variables and n_records > 0:
            resolution_codes = np.ma.asarray(ds.variables["resolution"][:]).filled(-1)
            summary["resolution_codes"] = sorted(set(int(value) for value in np.asarray(resolution_codes).reshape(-1).tolist()))
        if "time" in ds.variables and n_records > 0:
            time_var = ds.variables["time"]
            time_values = np.asarray(time_var[:], dtype=np.float64).reshape(-1)
            finite = time_values[np.isfinite(time_values)]
            if len(finite):
                summary["time_start"] = _format_num_time(
                    finite.min(),
                    getattr(time_var, "units", "days since 1970-01-01"),
                    getattr(time_var, "calendar", "gregorian"),
                )
                summary["time_end"] = _format_num_time(
                    finite.max(),
                    getattr(time_var, "units", "days since 1970-01-01"),
                    getattr(time_var, "calendar", "gregorian"),
                )
    return summary


def write_release_readme(out_path):
    content = """# Sediment Reference Dataset Release

This directory is the user-facing release layer of the sediment reference dataset.

## Core NetCDF products

- `sed_reference_master.nc`: authoritative long-table archive with full provenance.
- `sed_reference_timeseries_daily.nc`: daily `station x time` matrix for validation, now with cell-level `selected_source_station_uid`.
- `sed_reference_timeseries_monthly.nc`: monthly `station x time` matrix for validation, now with cell-level `selected_source_station_uid`.
- `sed_reference_timeseries_annual.nc`: annual `station x time` matrix for validation, now with cell-level `selected_source_station_uid`.
- `sed_reference_climatology.nc`: standalone climatology dataset.

## Catalogs

- `station_catalog.csv`: one row per `cluster_uid + resolution` with coordinates, basin attributes, record count, and time coverage.
- `source_station_catalog.csv`: one row per `source_station_uid + resolution` with links back to cluster, source dataset, and original file path.
- `source_dataset_catalog.csv`: one row per source dataset with metadata and aggregate counts.
- `sed_reference_overlap_candidates.csv.gz`: optional candidate-level provenance sidecar for multi-source overlap validation. It preserves selected and non-selected candidate values for overlap keys when the upstream candidate files are available at publish time.

## GIS sidecars

- `sed_reference_cluster_points.gpkg`: multi-layer cluster point sidecar with `cluster_summary`, `cluster_daily`, `cluster_monthly`, and `cluster_annual`.
- `sed_reference_source_stations.gpkg`: multi-layer source-station sidecar with `source_daily`, `source_monthly`, and `source_annual`.
- `sed_reference_cluster_basins.gpkg`: optional multi-layer basin sidecar with `basin_daily`, `basin_monthly`, and `basin_annual`.

## Recommended workflow

1. Open the matrix file that matches your model output resolution.
2. Filter `station_catalog.csv` to that resolution, then use its `lat/lon` or the matching cluster layer in `sed_reference_cluster_points.gpkg` to find the nearest `cluster_uid`.
3. Extract the observed time series and compare it with the model time series.
4. If you need quick cell-level provenance, read `selected_source_station_uid` directly from the matrix file.
5. If you need full record-level provenance, query `sed_reference_master.nc` with `cluster_uid + time + resolution`.
6. Use `source_station_catalog.csv` to resolve `source_station_uid`, original station metadata, and original file path.
7. For true source-pair overlap consistency metrics, use `sed_reference_overlap_candidates.csv.gz` if it is present.
8. Keep climatology analyses separate and use `sed_reference_climatology.nc` directly.

## Quick example

The helper script `example_reference_workflow.py` shows:

- nearest-station matching within a chosen resolution;
- matrix time-series extraction;
- optional model/reference alignment for a gridded model NetCDF;
- provenance lookup back to `source_station_uid`.

Example:

```bash
python3 example_reference_workflow.py \\
  --release-dir . \\
  --resolution monthly \\
  --lat 30.5 \\
  --lon 114.3 \\
  --variable SSC
```

## Notes

- `cluster_uid + resolution` is the standard GIS join key for cluster points and basins.
- `source_station_uid + resolution` is the standard GIS join key for source points.
- `selected_source_station_uid` is the matrix-native provenance key for each station-time cell.
- `sed_reference_master.nc` and matrix NetCDF files keep the selected / winning record only; they do not store non-selected candidate values.
- `is_overlap=1` marks that multiple sources competed for a cluster-resolution-time key, but it is not itself a candidate-value table.
- Source-pair validation should use `sed_reference_overlap_candidates.csv.gz`. If that sidecar is absent, true source-pair metrics cannot be computed from the release package alone.
- `station_uid` is the stable key inside the climatology product only.
- The release does not automatically aggregate daily model output to monthly or annual resolution.
- Release validation blocks mixed-run outputs whose master / matrix / climatology / catalog time coverage or resolution coverage are inconsistent.
"""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    return out_path


def validate_release(
    master_nc,
    matrix_paths,
    climatology_nc,
    cluster_station_catalog,
    station_catalog,
    source_station_catalog,
    out_csv,
    overlap_candidates_csv=None,
):
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to validate release consistency")
    rows = []
    cluster_station_catalog = normalize_cluster_station_catalog(cluster_station_catalog)
    cluster_resolution_catalog = normalize_cluster_resolution_catalog(station_catalog)
    source_station_catalog = normalize_source_station_resolution_catalog(source_station_catalog)

    cluster_uid_lookup = cluster_station_catalog.set_index("cluster_uid")["master_station_index"].to_dict()
    source_key_lookup = source_station_catalog.set_index(
        ["source_station_index", "resolution"]
    )["source_station_uid"].to_dict()
    matrix_core_summary = _summarize_matrix_core(matrix_paths)
    cluster_station_core_summary = _summarize_cluster_station_catalog_core(cluster_station_catalog)
    cluster_resolution_core_summary = _summarize_cluster_resolution_catalog_core(cluster_resolution_catalog)
    source_station_core_summary = _summarize_source_station_catalog_core(source_station_catalog)
    climatology_summary = _summarize_climatology_core(climatology_nc)

    with nc4.Dataset(master_nc, "r") as master_ds:
        master_core_summary = _summarize_master_core(master_ds)
        core_summaries = {
            "master": master_core_summary,
            "matrix": matrix_core_summary,
            "cluster_station_catalog": cluster_station_core_summary,
            "cluster_resolution_catalog": cluster_resolution_core_summary,
            "source_station_catalog": source_station_core_summary,
        }
        for label, summary in core_summaries.items():
            rows.append(
                {
                    "check": "core_resolution_coverage_{}".format(label),
                    "status": "pass",
                    "details": _format_coverage(summary),
                }
            )

        coverage_map = dict(
            (label, tuple(res for res in CLUSTER_RESOLUTIONS if int(summary.get(res, {}).get("record_count", 0) or 0) > 0))
            for label, summary in core_summaries.items()
        )
        unique_coverages = set(coverage_map.values())
        if len(unique_coverages) > 1:
            rows.append(
                {
                    "check": "core_resolution_coverage_consistency",
                    "status": "fail",
                    "details": "{} coverage mismatch: {}".format(
                        FULL_CHAIN_RERUN_HINT,
                        "; ".join(
                            "{}={}".format(label, "|".join(values) if values else "(none)")
                            for label, values in coverage_map.items()
                        ),
                    ),
                }
            )
        else:
            rows.append(
                {
                    "check": "core_resolution_coverage_consistency",
                    "status": "pass",
                    "details": "all core products cover {}".format(
                        "|".join(next(iter(unique_coverages))) if unique_coverages and next(iter(unique_coverages)) else "(none)"
                    ),
                }
            )

        for resolution in CLUSTER_RESOLUTIONS:
            count_map = dict(
                (label, int(summary.get(resolution, {}).get("record_count", 0) or 0))
                for label, summary in core_summaries.items()
            )
            if len(set(count_map.values())) > 1:
                rows.append(
                    {
                        "check": "core_record_count_{}".format(resolution),
                        "status": "fail",
                        "details": "{} count mismatch for {}: {}".format(
                            FULL_CHAIN_RERUN_HINT,
                            resolution,
                            "; ".join("{}={}".format(label, value) for label, value in count_map.items()),
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "check": "core_record_count_{}".format(resolution),
                        "status": "pass",
                        "details": "{}={}".format(resolution, next(iter(set(count_map.values()))) if count_map else 0),
                    }
                )

            active_stats = dict(
                (label, summary.get(resolution, _empty_core_stats()))
                for label, summary in core_summaries.items()
                if int(summary.get(resolution, {}).get("record_count", 0) or 0) > 0
            )
            start_map = dict((label, stats.get("time_start", "")) for label, stats in active_stats.items())
            end_map = dict((label, stats.get("time_end", "")) for label, stats in active_stats.items())
            if active_stats and (len(set(start_map.values())) > 1 or len(set(end_map.values())) > 1):
                rows.append(
                    {
                        "check": "core_time_range_{}".format(resolution),
                        "status": "fail",
                        "details": "{} time-range mismatch for {}: {}".format(
                            FULL_CHAIN_RERUN_HINT,
                            resolution,
                            "; ".join(
                                "{}=({})".format(label, _format_stats(stats))
                                for label, stats in active_stats.items()
                            ),
                        ),
                    }
                )
            else:
                rows.append(
                    {
                        "check": "core_time_range_{}".format(resolution),
                        "status": "pass",
                        "details": "; ".join(
                            "{}=({})".format(label, _format_stats(stats))
                            for label, stats in active_stats.items()
                        ) if active_stats else "no records",
                    }
                )

        master_cluster_uids = _read_text_var(master_ds, "cluster_uid", size=len(master_ds.dimensions["n_stations"]))
        master_cluster_uid_set = set(master_cluster_uids)
        master_source_station_uids = _read_text_var(
            master_ds,
            "source_station_uid",
            size=len(master_ds.dimensions["n_source_stations"]),
        )
        master_time_var = master_ds.variables["time"]
        master_time_units = getattr(master_time_var, "units", "days since 1970-01-01")
        master_time_calendar = getattr(master_time_var, "calendar", "gregorian")

        for resolution, path in matrix_paths.items():
            path = Path(path)
            if not path.is_file():
                rows.append(
                    {
                        "check": "matrix_exists_{}".format(resolution),
                        "status": "fail",
                        "details": "Missing {}".format(path),
                    }
                )
                continue

            with nc4.Dataset(path, "r") as ds:
                required = {
                    "lat",
                    "lon",
                    "cluster_uid",
                    "time",
                    "SSC",
                    "n_valid_time_steps",
                    "selected_source_index",
                    "selected_source_station_uid",
                }
                missing = sorted(required - set(ds.variables))
                if missing:
                    rows.append(
                        {
                            "check": "matrix_structure_{}".format(resolution),
                            "status": "fail",
                            "details": "Missing variables: {}".format(", ".join(missing)),
                        }
                    )
                    continue

                lats = _read_float_array(ds, "lat", fill_values=(-9999.0,), size=len(ds.dimensions["n_stations"]))
                lons = _read_float_array(ds, "lon", fill_values=(-9999.0,), size=len(ds.dimensions["n_stations"]))
                cluster_uids = _read_text_var(ds, "cluster_uid", size=len(ds.dimensions["n_stations"]))
                valid_steps = _read_int_array(
                    ds,
                    "n_valid_time_steps",
                    fill_value=0,
                    size=len(ds.dimensions["n_stations"]),
                )
                non_empty_idx = np.flatnonzero(valid_steps > 0)
                if len(non_empty_idx) == 0:
                    rows.append(
                        {
                            "check": "matrix_nonempty_{}".format(resolution),
                            "status": "fail",
                            "details": "No stations with data in {}".format(path.name),
                        }
                    )
                    continue

                release_subset = cluster_resolution_catalog[cluster_resolution_catalog["resolution"] == resolution].copy()
                release_subset = release_subset[np.isfinite(release_subset["lat"]) & np.isfinite(release_subset["lon"])].copy()
                if len(release_subset) == 0:
                    rows.append(
                        {
                            "check": "station_catalog_resolution_{}".format(resolution),
                            "status": "fail",
                            "details": "station_catalog.csv has no rows for {}".format(resolution),
                        }
                    )
                else:
                    sample_row = release_subset.iloc[0]
                    distances = _haversine_km(
                        float(sample_row["lat"]),
                        float(sample_row["lon"]),
                        release_subset["lat"].values,
                        release_subset["lon"].values,
                    )
                    nearest = release_subset.iloc[int(np.nanargmin(distances))]
                    same_key = (
                        str(nearest["cluster_uid"]) == str(sample_row["cluster_uid"])
                        and str(nearest["resolution"]) == str(sample_row["resolution"])
                    )
                    rows.append(
                        {
                            "check": "nearest_station_lookup_{}".format(resolution),
                            "status": "pass" if same_key else "fail",
                            "details": "sample_key={}|{} nearest_key={}|{}".format(
                                sample_row["cluster_uid"],
                                sample_row["resolution"],
                                nearest["cluster_uid"],
                                nearest["resolution"],
                            ),
                        }
                    )

                sample_idx = int(non_empty_idx[0])
                ssc_row = np.ma.asarray(ds.variables["SSC"][sample_idx, :]).filled(np.nan)
                non_missing = int(np.count_nonzero(np.isfinite(ssc_row)))
                rows.append(
                    {
                        "check": "matrix_series_extract_{}".format(resolution),
                        "status": "pass" if non_missing > 0 else "fail",
                        "details": "cluster_uid={} non_missing_SSC={}".format(
                            cluster_uids[sample_idx],
                            non_missing,
                        ),
                    }
                )

                sample_cluster_uid = cluster_uids[sample_idx]
                rows.append(
                    {
                        "check": "master_lookup_{}".format(resolution),
                        "status": "pass" if sample_cluster_uid in master_cluster_uid_set else "fail",
                        "details": sample_cluster_uid,
                    }
                )

                overlap_sample = _first_overlap_sample(ds)
                if overlap_sample is None:
                    rows.append(
                        {
                            "check": "overlap_consistency_{}".format(resolution),
                            "status": "skip",
                            "details": "No overlap cell found in {}".format(path.name),
                        }
                    )
                    continue

                station_row, time_col, selected_source_idx = overlap_sample
                overlap_cluster_uid = cluster_uids[station_row]
                master_idx = cluster_uid_lookup.get(overlap_cluster_uid, None)
                if master_idx is None:
                    rows.append(
                        {
                            "check": "overlap_consistency_{}".format(resolution),
                            "status": "fail",
                            "details": "cluster_uid missing from cluster station catalog: {}".format(overlap_cluster_uid),
                        }
                    )
                    continue

                matrix_time_var = ds.variables["time"]
                matrix_time_val = float(np.asarray(matrix_time_var[time_col]).reshape(-1)[0])
                try:
                    decoded = nc4.num2date(
                        matrix_time_val,
                        getattr(matrix_time_var, "units", master_time_units),
                        calendar=getattr(matrix_time_var, "calendar", master_time_calendar),
                        only_use_cftime_datetimes=False,
                    )
                except TypeError:
                    decoded = nc4.num2date(
                        matrix_time_val,
                        getattr(matrix_time_var, "units", master_time_units),
                        calendar=getattr(matrix_time_var, "calendar", master_time_calendar),
                    )
                target_time_num = nc4.date2num(
                    decoded,
                    master_time_units,
                    calendar=master_time_calendar,
                )
                record_idx = _find_master_record_index(
                    master_ds=master_ds,
                    station_index=int(master_idx),
                    resolution_code=RESOLUTION_NAME_TO_CODE[resolution],
                    target_time_num=target_time_num,
                )
                if record_idx is None:
                    rows.append(
                        {
                            "check": "overlap_consistency_{}".format(resolution),
                            "status": "fail",
                            "details": "No matching master record found",
                        }
                    )
                    continue

                source_name = _clean_text(ds.variables["source_name"][selected_source_idx])
                master_overlap = int(np.ma.asarray(master_ds.variables["is_overlap"][record_idx]).filled(0))
                master_source = _clean_text(master_ds.variables["source"][record_idx])
                pass_flag = master_overlap == 1 and master_source == source_name
                rows.append(
                    {
                        "check": "overlap_consistency_{}".format(resolution),
                        "status": "pass" if pass_flag else "fail",
                        "details": "matrix_source={} master_source={} record_idx={}".format(
                            source_name,
                            master_source,
                            record_idx,
                        ),
                    }
                )

        source_station_index_arr = np.ma.asarray(master_ds.variables["source_station_index"][:]).filled(-1)
        resolution_codes_arr = np.ma.asarray(master_ds.variables["resolution"][:]).filled(-1)
        sampled_record_indices = []
        for idx, (src_idx, res_code) in enumerate(zip(source_station_index_arr.tolist(), resolution_codes_arr.tolist())):
            if int(src_idx) < 0:
                continue
            res_name = RESOLUTION_CODE_TO_NAME.get(int(res_code), "")
            if res_name not in CLUSTER_RESOLUTIONS:
                continue
            sampled_record_indices.append((idx, int(src_idx), res_name))
            if len(sampled_record_indices) >= 1000:
                break

        source_station_ok = True
        detail = "sampled_rows={}".format(len(sampled_record_indices))
        for record_idx, src_idx, res_name in sampled_record_indices:
            uid_master = master_source_station_uids[src_idx] if src_idx < len(master_source_station_uids) else ""
            uid_catalog = source_key_lookup.get((src_idx, res_name), "")
            if uid_master != uid_catalog:
                source_station_ok = False
                detail = "Mismatch at source_station_index={} resolution={} record_idx={}".format(
                    src_idx,
                    res_name,
                    record_idx,
                )
                break
        rows.append(
            {
                "check": "source_station_catalog_lookup",
                "status": "pass" if source_station_ok else "fail",
                "details": detail,
            }
        )

    rows.append(
        {
            "check": "climatology_record_count",
            "status": "pass" if int(climatology_summary.get("record_count", 0) or 0) > 0 else "fail",
            "details": _format_stats(climatology_summary),
        }
    )
    resolution_codes = climatology_summary.get("resolution_codes", [])
    if resolution_codes and set(resolution_codes) != {RESOLUTION_NAME_TO_CODE["climatology"]}:
        rows.append(
            {
                "check": "climatology_resolution_codes",
                "status": "fail",
                "details": "{} climatology file contains unexpected resolution codes: {}".format(
                    FULL_CHAIN_RERUN_HINT,
                    ",".join(str(code) for code in resolution_codes),
                ),
            }
        )
    else:
        rows.append(
            {
                "check": "climatology_resolution_codes",
                "status": "pass",
                "details": ",".join(str(code) for code in resolution_codes) if resolution_codes else "(empty)",
            }
        )

    with nc4.Dataset(climatology_nc, "r") as clim_ds:
        station_uids = _read_text_var(clim_ds, "station_uid", size=len(clim_ds.dimensions["n_stations"]))
        unique_ok = len(station_uids) == len(set(station_uids))
        rows.append(
            {
                "check": "climatology_station_uid_unique",
                "status": "pass" if unique_ok else "fail",
                "details": "n_stations={}".format(len(station_uids)),
            }
        )
        paths = _read_text_var(clim_ds, "source_station_path", size=len(clim_ds.dimensions["n_stations"]))
        existing = sum(1 for path in paths if path and Path(path).is_file())
        rows.append(
            {
                "check": "climatology_path_exists",
                "status": "pass" if existing == len(paths) else "fail",
                "details": "{}/{} source files found".format(existing, len(paths)),
            }
        )

    if overlap_candidates_csv is None or not Path(overlap_candidates_csv).is_file():
        rows.append(
            {
                "check": "overlap_candidates_sidecar",
                "status": "skip",
                "details": "sed_reference_overlap_candidates.csv.gz not generated; true source-pair metrics require this sidecar",
            }
        )
    else:
        ok, detail = validate_overlap_candidates_sidecar(overlap_candidates_csv)
        rows.append(
            {
                "check": "overlap_candidates_sidecar",
                "status": "pass" if ok else "fail",
                "details": detail,
            }
        )

    report_df = pd.DataFrame(rows)
    _write_csv(report_df, out_csv)
    failed = report_df["status"].eq("fail").any()
    return not failed, report_df


def main():
    ap = argparse.ArgumentParser(description="Publish the sediment reference dataset release package")
    ap.add_argument("--master-nc", default=str(DEFAULT_MASTER_NC))
    ap.add_argument("--daily-nc", default=str(DEFAULT_MATRIX_DAILY))
    ap.add_argument("--monthly-nc", default=str(DEFAULT_MATRIX_MONTHLY))
    ap.add_argument("--annual-nc", default=str(DEFAULT_MATRIX_ANNUAL))
    ap.add_argument("--climatology-nc", default=str(DEFAULT_CLIM_NC))
    ap.add_argument("--cluster-station-catalog", default=str(DEFAULT_CLUSTER_STATION_CATALOG_INPUT))
    ap.add_argument("--cluster-resolution-catalog", default=str(DEFAULT_CLUSTER_RESOLUTION_CATALOG_INPUT))
    ap.add_argument(
        "--source-station-resolution-catalog",
        default=str(DEFAULT_SOURCE_STATION_RESOLUTION_CATALOG_INPUT),
    )
    ap.add_argument("--cluster-basin-vector", default=str(DEFAULT_CLUSTER_BASIN_VECTOR))
    ap.add_argument("--out-dir", default=str(DEFAULT_RELEASE_DIR))
    ap.add_argument(
        "--link-mode",
        choices=("hardlink", "symlink", "copy"),
        default="hardlink",
        help="How to materialize canonical NetCDF/example files in the release dir",
    )
    ap.add_argument("--skip-gpkg", action="store_true", help="Skip GPKG spatial sidecars")
    ap.add_argument(
        "--skip-overlap-candidates",
        action="store_true",
        help="Skip the candidate-level overlap provenance sidecar",
    )
    ap.add_argument(
        "--overlap-candidates-mode",
        choices=("overlap-only", "all-candidates"),
        default="overlap-only",
        help="Rows to publish in sed_reference_overlap_candidates.csv.gz",
    )
    ap.add_argument(
        "--include-basin-polygons",
        action="store_true",
        help="Also publish the resolution-aware cluster basin GPKG",
    )
    ap.add_argument("--skip-validation", action="store_true", help="Skip release validation checks")
    ap.add_argument("--force", action="store_true", help="Overwrite existing release files")
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1

    master_nc = Path(args.master_nc).resolve()
    daily_nc = Path(args.daily_nc).resolve()
    monthly_nc = Path(args.monthly_nc).resolve()
    annual_nc = Path(args.annual_nc).resolve()
    climatology_nc = Path(args.climatology_nc).resolve()
    cluster_station_catalog_in = Path(args.cluster_station_catalog).resolve()
    cluster_resolution_catalog_in = Path(args.cluster_resolution_catalog).resolve()
    source_station_resolution_catalog_in = Path(args.source_station_resolution_catalog).resolve()
    basin_vector = Path(args.cluster_basin_vector).resolve()
    out_dir = Path(args.out_dir).resolve()

    required_inputs = [
        master_nc,
        daily_nc,
        monthly_nc,
        annual_nc,
        climatology_nc,
        cluster_station_catalog_in,
        cluster_resolution_catalog_in,
        source_station_resolution_catalog_in,
    ]
    missing = [str(path) for path in required_inputs if not path.is_file()]
    if missing:
        print("Error: required inputs missing:")
        for item in missing:
            print("  - {}".format(item))
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    print("Release dir: {}".format(out_dir))

    core_sources = _canonical_core_sources(master_nc, daily_nc, monthly_nc, annual_nc, climatology_nc)
    core_destinations = {
        "master": out_dir / Path(RELEASE_MASTER_NC).name,
        "daily": out_dir / Path(RELEASE_MATRIX_DAILY_NC).name,
        "monthly": out_dir / Path(RELEASE_MATRIX_MONTHLY_NC).name,
        "annual": out_dir / Path(RELEASE_MATRIX_ANNUAL_NC).name,
        "climatology": out_dir / Path(RELEASE_CLIMATOLOGY_NC).name,
    }

    file_records = []
    for kind, _, description in CORE_FILE_SPECS:
        dst = _link_or_copy_file(
            core_sources[kind],
            core_destinations[kind],
            mode=args.link_mode,
            force=args.force,
        )
        file_records.append(("core_netcdf", dst, description))
        print("Prepared {} -> {}".format(kind, dst.name))

    cluster_station_catalog = normalize_cluster_station_catalog(
        pd.read_csv(cluster_station_catalog_in, keep_default_na=False)
    )
    station_catalog = normalize_cluster_resolution_catalog(
        pd.read_csv(cluster_resolution_catalog_in, keep_default_na=False)
    )
    source_station_catalog = normalize_source_station_resolution_catalog(
        pd.read_csv(source_station_resolution_catalog_in, keep_default_na=False)
    )
    source_dataset_catalog = build_source_dataset_catalog(source_station_catalog)

    station_catalog_path = _write_csv(station_catalog, out_dir / Path(RELEASE_STATION_CATALOG_CSV).name)
    source_station_catalog_path = _write_csv(
        source_station_catalog,
        out_dir / Path(RELEASE_SOURCE_STATION_CATALOG_CSV).name,
    )
    source_dataset_catalog_path = _write_csv(
        source_dataset_catalog,
        out_dir / Path(RELEASE_SOURCE_DATASET_CATALOG_CSV).name,
    )
    file_records.extend(
        [
            ("catalog", station_catalog_path, "Resolution-aware cluster lookup catalog"),
            ("catalog", source_station_catalog_path, "Resolution-aware source-station provenance catalog"),
            ("catalog", source_dataset_catalog_path, "Source-dataset metadata catalog"),
        ]
    )
    print(
        "Wrote catalogs: station={}, source_station={}, source_dataset={}".format(
            len(station_catalog),
            len(source_station_catalog),
            len(source_dataset_catalog),
        )
    )

    overlap_candidates_path = out_dir / OVERLAP_CANDIDATES_FILE_NAME
    overlap_candidates_validation_path = None
    if args.skip_overlap_candidates:
        print("Skip overlap candidate sidecar by request.")
    else:
        built_path, row_count, detail = build_overlap_candidates_sidecar(
            quality_order_csv=DEFAULT_QUALITY_ORDER_CSV,
            source_station_catalog=source_station_catalog,
            out_path=overlap_candidates_path,
            mode=args.overlap_candidates_mode,
        )
        if built_path is None:
            print("Warning: skip overlap candidate sidecar: {}".format(detail))
        else:
            if "failed schema validation" in detail:
                print("Error: overlap candidate sidecar invalid: {}".format(detail))
                return 1
            file_records.append(
                (
                    "provenance_sidecar",
                    built_path,
                    "Candidate-level selected and non-selected values for multi-source overlap validation",
                )
            )
            overlap_candidates_validation_path = built_path
            print("Wrote overlap candidates: {} rows ({})".format(row_count, detail))

    if args.skip_gpkg:
        print("Skip GPKG sidecars by request.")
    else:
        if not HAS_GPD:
            print("Warning: geopandas is unavailable, skip GPKG sidecars.")
        else:
            cluster_points_path = write_cluster_points_gpkg(
                cluster_station_catalog,
                station_catalog,
                out_dir / Path(RELEASE_CLUSTER_POINTS_GPKG).name,
            )
            source_stations_path = write_source_stations_gpkg(
                source_station_catalog,
                out_dir / Path(RELEASE_SOURCE_STATIONS_GPKG).name,
            )
            file_records.extend(
                [
                    ("spatial", cluster_points_path, "Cluster point sidecar keyed by cluster_uid + resolution"),
                    ("spatial", source_stations_path, "Source-station sidecar keyed by source_station_uid + resolution"),
                ]
            )
            print("Wrote GPKG sidecars: {}, {}".format(cluster_points_path.name, source_stations_path.name))

            if args.include_basin_polygons:
                if basin_vector.is_file() and basin_vector.suffix.lower() == ".gpkg":
                    basin_out = _link_or_copy_file(
                        basin_vector,
                        out_dir / Path(RELEASE_CLUSTER_BASINS_GPKG).name,
                        mode=args.link_mode,
                        force=args.force,
                    )
                    file_records.append(
                        ("spatial", basin_out, "Cluster basin polygon sidecar keyed by cluster_uid + resolution")
                    )
                    print("Prepared basin polygon GPKG: {}".format(basin_out.name))
                elif basin_vector.is_file():
                    print("Warning: cluster basin vector is not a GPKG, skip: {}".format(basin_vector))
                else:
                    print("Warning: cluster basin vector not found, skip: {}".format(basin_vector))
            else:
                print("Skip basin polygon GPKG by default; use --include-basin-polygons to enable it.")

    if DEFAULT_EXAMPLE_SCRIPT.is_file():
        example_dst = _link_or_copy_file(
            DEFAULT_EXAMPLE_SCRIPT,
            out_dir / DEFAULT_EXAMPLE_SCRIPT.name,
            mode=args.link_mode,
            force=args.force,
        )
        file_records.append(("support", example_dst, "Example workflow script"))
        print("Prepared example script: {}".format(example_dst.name))
    else:
        print("Warning: example script not found: {}".format(DEFAULT_EXAMPLE_SCRIPT))

    readme_path = write_release_readme(out_dir / Path(RELEASE_README_MD).name)
    file_records.append(("support", readme_path, "Release usage guide"))

    validation_path = out_dir / Path(RELEASE_VALIDATION_CSV).name
    if args.skip_validation:
        print("Skip validation by request.")
    else:
        ok, report_df = validate_release(
            master_nc=master_nc,
            matrix_paths={
                "daily": daily_nc,
                "monthly": monthly_nc,
                "annual": annual_nc,
            },
            climatology_nc=climatology_nc,
            cluster_station_catalog=cluster_station_catalog,
            station_catalog=station_catalog,
            source_station_catalog=source_station_catalog,
            out_csv=validation_path,
            overlap_candidates_csv=overlap_candidates_validation_path,
        )
        file_records.append(("report", validation_path, "Release validation report"))
        print("Validation checks: {} rows".format(len(report_df)))
        if not ok:
            print("Error: release validation reported failures. See {}".format(validation_path))
            return 1

    inventory_path = write_inventory(file_records, out_dir / Path(RELEASE_INVENTORY_CSV).name, out_dir)
    print("Wrote inventory: {}".format(inventory_path))
    print("Release package is ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
