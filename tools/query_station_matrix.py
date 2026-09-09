#!/usr/bin/env python3
"""Query matrix NetCDF files by dataset/source name plus station_id."""

# ══════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════

from pathlib import Path

DATASET_NAME = "RiverSed"
STATION_ID   = "RiverSed_20035"

OUTPUT_ROOT = None
# OUTPUT_ROOT = "/data/Output_r"

RESOLUTION = "all"

VARIABLE = "all"

ONLY_VALID_ROWS = True

OUT_CSV = None
#   query_<dataset>_<station_id>_s6_matrix.csv

PREVIEW_ROWS = 20

# ══════════════════════════════════════════════════════════════════════

import sys
import re

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(SCRIPT_DIR.parent))

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

try:
    from pipeline_paths import (
        S5_BASIN_CLUSTERED_CSV,
        S6_MATRIX_DIR,
        get_output_r_root,
    )
except Exception:
    S5_BASIN_CLUSTERED_CSV = "scripts_basin_test/output/s5_basin_clustered_stations.csv"
    S6_MATRIX_DIR = "scripts_basin_test/output/s6_matrix_by_resolution"

    def get_output_r_root(_base):
        return Path(_base).resolve()


FILL_VALUES = {-9999, -9999.0}
CORE_VARS = ("Q", "SSC", "SSL")
CORE_FLAGS = ("Q_flag", "SSC_flag", "SSL_flag")


def _txt(value):
    """Convert NetCDF and pandas values to clean strings."""
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
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def _txt_arr(arr):
    return [_txt(v) for v in np.asarray(arr, dtype=object).reshape(-1)]


def _norm_id(value):
    """Normalize station_id values for loose matching."""
    text = _txt(value)
    if re.fullmatch(r"-?\d+\.0", text):
        text = text[:-2]
    return text.strip().upper()


def _norm_name(value):
    """Normalize dataset or folder names for loose matching."""
    return _txt(value).strip().lower()


def _as_float_arr(var, row=None):
    """Read a NetCDF variable and convert fill or masked values to NaN."""
    if row is None:
        arr = np.ma.asarray(var[:]).astype(float)
    else:
        arr = np.ma.asarray(var[row, :]).astype(float)
    out = arr.filled(np.nan)
    for fv in FILL_VALUES:
        out[out == fv] = np.nan
    return out


def _as_int_arr(var, row=None, fill=9):
    if row is None:
        arr = np.ma.asarray(var[:])
    else:
        arr = np.ma.asarray(var[row, :])
    return arr.filled(fill).astype(np.int32)


def _paths():
    root = Path(OUTPUT_ROOT).expanduser().resolve() if OUTPUT_ROOT else get_output_r_root(SCRIPT_DIR)
    s5_csv = root / S5_BASIN_CLUSTERED_CSV
    matrix_dir = root / S6_MATRIX_DIR
    return root, s5_csv, matrix_dir


def _path_parts_lower(path_value):
    try:
        return [p.lower() for p in Path(str(path_value)).parts]
    except Exception:
        return []


def _find_station_rows(s5_csv, dataset_name, station_id):
    if not s5_csv.is_file():
        raise FileNotFoundError(f"s5 CSV not found: {s5_csv}")

    df = pd.read_csv(s5_csv)
    df = df.copy()

    required = {"cluster_id", "path"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"s5 CSV is missing required columns: {missing}")

    dataset_q = _norm_name(dataset_name)
    station_q = _norm_id(station_id)

    dataset_mask = pd.Series(False, index=df.index)

    if "source" in df.columns:
        dataset_mask |= df["source"].map(_norm_name).eq(dataset_q)

    dataset_mask |= df["path"].astype(str).map(
        lambda p: dataset_q in _path_parts_lower(p)
    )

    id_cols = [
        "station_id",
        "source_station_id",
        "native_id",
        "source_station_native_id",
        "Source_ID",
        "Station_ID",
        "source_id",
        "stationID",
        "ID",
        "location_id",
    ]
    existing_id_cols = [c for c in id_cols if c in df.columns]

    if not existing_id_cols:
        raise ValueError(
            "s5 CSV has no columns available for station_id matching."
            f"Tried these column names: {id_cols}"
        )

    station_mask = pd.Series(False, index=df.index)
    for col in existing_id_cols:
        station_mask |= df[col].map(_norm_id).eq(station_q)

    hit = df[dataset_mask & station_mask].copy()

    if len(hit) == 0:
        msg = [
            "No matching rows found in s5_basin_clustered_stations.csv.",
            f"  DATASET_NAME = {dataset_name}",
            f"  STATION_ID   = {station_id}",
            "",
            f"Checked station_id columns: {existing_id_cols}",
        ]
        if "source" in df.columns:
            near_sources = sorted(df["source"].dropna().astype(str).unique().tolist())[:20]
            msg.append("")
            msg.append("First 20 source examples:")
            msg.append("  " + ", ".join(near_sources))
        raise ValueError("\n".join(msg))

    hit["cluster_id"] = hit["cluster_id"].astype(int)
    hit["cluster_uid"] = hit["cluster_id"].map(lambda x: f"SED{x:06d}")

    return hit, existing_id_cols


def _matrix_files(matrix_dir, resolution):
    if not matrix_dir.is_dir():
        raise FileNotFoundError(f"matrix directory not found: {matrix_dir}")

    files = sorted(matrix_dir.glob("*.nc"))
    if resolution != "all":
        r = resolution.lower().strip()
        files = [p for p in files if r in p.stem.lower()]

    return files


def _decode_times(ds):
    time_var = ds.variables["time"]
    units = getattr(time_var, "units", "days since 1970-01-01")
    cal = getattr(time_var, "calendar", "gregorian")
    try:
        times = nc4.num2date(
            time_var[:],
            units,
            calendar=cal,
            only_use_cftime_datetimes=False,
        )
    except TypeError:
        times = nc4.num2date(time_var[:], units, calendar=cal)

    return pd.to_datetime(list(times))


def _resolution_from_nc(path, ds):
    attr = getattr(ds, "time_type", "")
    if attr:
        return str(attr)
    stem = path.stem.lower()
    for key in ("daily", "monthly", "annual", "climatology"):
        if key in stem:
            return key
    return stem


def _source_names(ds):
    if "source_name" not in ds.variables:
        return []
    return _txt_arr(ds.variables["source_name"][:])


def _read_matrix_for_uid(nc_path, cluster_uid, variables):
    rows = []

    with nc4.Dataset(str(nc_path), "r") as ds:
        if "cluster_uid" not in ds.variables:
            return rows

        uids = _txt_arr(ds.variables["cluster_uid"][:])
        if cluster_uid not in uids:
            return rows

        row_idx = uids.index(cluster_uid)
        times = _decode_times(ds)
        resolution = _resolution_from_nc(nc_path, ds)

        station_meta = {
            "matrix_file": nc_path.name,
            "resolution": resolution,
            "cluster_uid": cluster_uid,
            "matrix_row": row_idx,
        }

        for name in [
            "cluster_id",
            "lat",
            "lon",
            "station_name",
            "river_name",
            "source_station_id",
            "sources_used",
            "n_sources_in_resolution",
            "basin_area",
            "pfaf_code",
            "n_upstream_reaches",
            "basin_match_quality",
            "basin_distance_m",
            "point_in_local",
            "point_in_basin",
            "basin_status",
            "basin_flag",
            "n_valid_time_steps",
        ]:
            if name not in ds.variables:
                continue
            v = ds.variables[name]
            try:
                val = v[row_idx]
                if getattr(v, "dtype", None) is str or isinstance(val, str):
                    station_meta[name] = _txt(val)
                else:
                    arr = np.ma.asarray(val)
                    if np.ma.is_masked(arr):
                        station_meta[name] = ""
                    else:
                        item = arr.item() if hasattr(arr, "item") else arr
                        station_meta[name] = item
            except Exception:
                station_meta[name] = ""

        data = {"time": times}

        # Q / SSC / SSL
        for var in variables:
            if var in ds.variables:
                data[var] = _as_float_arr(ds.variables[var], row_idx)

            flag = f"{var}_flag"
            if flag in ds.variables:
                data[flag] = _as_int_arr(ds.variables[flag], row_idx, fill=9)

        # provenance / overlap
        if "is_overlap" in ds.variables:
            data["is_overlap"] = _as_int_arr(ds.variables["is_overlap"], row_idx, fill=0)

        src_names = _source_names(ds)
        if "selected_source_index" in ds.variables:
            src_idx = _as_int_arr(ds.variables["selected_source_index"], row_idx, fill=-1)
            data["selected_source_index"] = src_idx
            data["selected_source"] = [
                src_names[i] if 0 <= int(i) < len(src_names) else ""
                for i in src_idx
            ]

        if "selected_source_station_uid" in ds.variables:
            data["selected_source_station_uid"] = _txt_arr(
                ds.variables["selected_source_station_uid"][row_idx, :]
            )

        for name in ds.variables:
            if name in data:
                continue
            if not re.match(r"^(Q|SSC|SSL)_qc\d+$", name):
                continue
            var = ds.variables[name]
            if getattr(var, "dimensions", ()) == ("n_stations", "time"):
                data[name] = _as_int_arr(var, row_idx, fill=9)

        df = pd.DataFrame(data)

        if ONLY_VALID_ROWS:
            present_vars = [v for v in variables if v in df.columns]
            if present_vars:
                keep = df[present_vars].notna().any(axis=1)
                df = df[keep].copy()

        if len(df) == 0:
            return rows

        for key, value in station_meta.items():
            df.insert(0, key, value)

        rows.append(df)

    return rows


def _query_all_matrices(hit_rows, matrix_dir, resolution, variables):
    files = _matrix_files(matrix_dir, resolution)
    if not files:
        raise FileNotFoundError(f"No matrix NetCDF files found: {matrix_dir}")

    frames = []
    cluster_uids = sorted(hit_rows["cluster_uid"].unique().tolist())

    for nc_path in files:
        for uid in cluster_uids:
            frames.extend(_read_matrix_for_uid(nc_path, uid, variables))

    if not frames:
        return pd.DataFrame(), files

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["resolution", "cluster_uid", "time"]).reset_index(drop=True)
    return out, files


def _print_match_summary(hit, id_cols):
    print("\nMatched s5 rows:")
    print(f"  Rows: {len(hit):,}")
    print(f"  Cluster count: {hit['cluster_uid'].nunique():,}")
    print(f"  cluster_uid: {', '.join(sorted(hit['cluster_uid'].unique().tolist()))}")
    print(f"  station_id columns checked: {', '.join(id_cols)}")

    cols = [
        c for c in [
            "source",
            "station_id",
            "source_station_id",
            "cluster_id",
            "cluster_uid",
            "resolution",
            "lat",
            "lon",
            "path",
        ]
        if c in hit.columns
    ]
    print()
    print(hit[cols].drop_duplicates().head(20).to_string(index=False))


def _print_result_summary(df, files):
    print("\nScanned matrix files:")
    for p in files:
        print(f"  - {p.name}")

    if df.empty:
        print("\nNo valid time series for the matched point were read from matrix NetCDF files.")
        return

    print("\nQuery Result Summary:")
    print(f"  Output rows: {len(df):,}")
    print(f"  Resolution: {', '.join(sorted(df['resolution'].astype(str).unique()))}")
    print(f"  cluster_uid: {', '.join(sorted(df['cluster_uid'].astype(str).unique()))}")

    var_cols = [v for v in CORE_VARS if v in df.columns]
    if var_cols:
        print("\nValid value counts by variable:")
        for v in var_cols:
            print(f"  {v}: {int(df[v].notna().sum()):,}")

    if PREVIEW_ROWS and PREVIEW_ROWS > 0:
        show_cols = [
            c for c in [
                "resolution",
                "time",
                "cluster_uid",
                "lat",
                "lon",
                "Q",
                "Q_flag",
                "SSC",
                "SSC_flag",
                "SSL",
                "SSL_flag",
                "selected_source",
                "selected_source_station_uid",
                "is_overlap",
            ]
            if c in df.columns
        ]
        print(f"\nPreview of first {PREVIEW_ROWS} rows:")
        print(df[show_cols].head(PREVIEW_ROWS).to_string(index=False))


def main():
    if nc4 is None:
        print("Error: netCDF4 is required. Run: pip install netCDF4")
        return 1

    resolution = str(RESOLUTION).strip().lower()
    if resolution not in {"all", "daily", "monthly", "annual", "climatology"}:
        print(f"Error: unsupported RESOLUTION: {RESOLUTION}")
        return 1

    variable = str(VARIABLE).strip().upper()
    variables = list(CORE_VARS) if variable == "ALL" else [variable]
    bad_vars = [v for v in variables if v not in CORE_VARS]
    if bad_vars:
        print(f"Error: unsupported VARIABLE: {bad_vars}; must be all / Q / SSC / SSL")
        return 1

    root, s5_csv, matrix_dir = _paths()

    print("Query configuration:")
    print(f"  OUTPUT_ROOT : {root}")
    print(f"  s5 CSV      : {s5_csv}")
    print(f"  matrix dir  : {matrix_dir}")
    print(f"  dataset     : {DATASET_NAME}")
    print(f"  station_id  : {STATION_ID}")
    print(f"  resolution  : {RESOLUTION}")
    print(f"  variable    : {VARIABLE}")

    try:
        hit, id_cols = _find_station_rows(s5_csv, DATASET_NAME, STATION_ID)
        _print_match_summary(hit, id_cols)

        df, files = _query_all_matrices(hit, matrix_dir, resolution, variables)
        _print_result_summary(df, files)

        if df.empty:
            return 2

        out_csv = OUT_CSV
        if out_csv is None:
            safe_dataset = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(DATASET_NAME)).strip("_")
            safe_station = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(STATION_ID)).strip("_")
            out_csv = SCRIPT_DIR / f"query_{safe_dataset}_{safe_station}_s6_matrix.csv"
        else:
            out_csv = Path(out_csv).expanduser().resolve()

        out_csv = Path(out_csv)
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_csv, index=False)
        print(f"\nExported CSV: {out_csv}")

    except Exception as exc:
        print(f"\nError: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
