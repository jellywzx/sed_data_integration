#!/usr/bin/env python3
"""
Example workflow for the minimal sediment reference package.

The minimal package contains daily/monthly/annual station-reference matrices
and compact catalog CSVs. It intentionally omits master provenance, overlap
candidate products, satellite validation data, and climatology products.

Typical usage:
  python3 tools/example_reference_workflow_minimal.py \
    --release-dir output/sed_reference_release_minimal \
    --resolution monthly \
    --lat 30.5 \
    --lon 114.3 \
    --variable SSC \
    --out-csv /tmp/minimal_reference_series.csv
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4

    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False

try:
    import h5netcdf

    HAS_H5NETCDF = True
except ImportError:
    h5netcdf = None
    HAS_H5NETCDF = False


MATRIX_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}
VARIABLES = {"Q", "SSC", "SSL"}


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1 = np.deg2rad(np.asarray(lat1, dtype=np.float64))
    lon1 = np.deg2rad(np.asarray(lon1, dtype=np.float64))
    lat2 = np.deg2rad(np.asarray(lat2, dtype=np.float64))
    lon2 = np.deg2rad(np.asarray(lon2, dtype=np.float64))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 6371.0 * 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def _clean_text(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _decode_strings(values):
    return [_clean_text(value) for value in np.asarray(values, dtype=object).reshape(-1)]


def load_station_catalog(release_dir):
    path = Path(release_dir) / "station_catalog.csv"
    if not path.is_file():
        raise FileNotFoundError("station_catalog.csv not found in {}".format(release_dir))
    return pd.read_csv(path, keep_default_na=False)


def find_nearest_station(station_catalog, lat, lon, resolution):
    work = station_catalog.copy()
    work = work[work["resolution"].astype(str).str.strip() == str(resolution)].copy()
    work = work[np.isfinite(work["lat"]) & np.isfinite(work["lon"])].copy()
    if len(work) == 0:
        raise ValueError("No station_catalog rows found for resolution '{}'".format(resolution))
    distances = _haversine_km(lat, lon, work["lat"].values, work["lon"].values)
    idx = int(np.argmin(distances))
    row = work.iloc[idx].copy()
    row["distance_km"] = float(distances[idx])
    return row


def _decode_time_values(time_values, units):
    units = str(units or "days since 1970-01-01")
    if " since " in units:
        unit_name, origin = units.split(" since ", 1)
    else:
        unit_name, origin = "days", "1970-01-01"
    unit_name = unit_name.strip().lower()
    origin = origin.strip().split(" ")[0]
    if unit_name.startswith("day"):
        return pd.Timestamp(origin) + pd.to_timedelta(time_values, unit="D")
    if unit_name.startswith("hour"):
        return pd.Timestamp(origin) + pd.to_timedelta(time_values, unit="h")
    if unit_name.startswith("second"):
        return pd.Timestamp(origin) + pd.to_timedelta(time_values, unit="s")
    raise ValueError("Unsupported time units: {}".format(units))


def _series_from_arrays(times, values, flags, source_station_uids):
    df = pd.DataFrame(
        {
            "time": pd.to_datetime(times),
            "reference_value": np.ma.asarray(values).filled(np.nan).astype(np.float64),
            "reference_flag": np.ma.asarray(flags).filled(9).astype(np.int16),
            "selected_source_station_uid": source_station_uids,
        }
    )
    return df[np.isfinite(df["reference_value"])].reset_index(drop=True)


def _extract_with_netCDF4(matrix_path, cluster_uid, variable):
    with nc4.Dataset(matrix_path, "r") as ds:
        cluster_uids = _decode_strings(ds.variables["cluster_uid"][:])
        row_idx = cluster_uids.index(cluster_uid)
        time_var = ds.variables["time"]
        try:
            times = nc4.num2date(
                time_var[:],
                getattr(time_var, "units", "days since 1970-01-01"),
                calendar=getattr(time_var, "calendar", "gregorian"),
                only_use_cftime_datetimes=False,
            )
        except TypeError:
            times = nc4.num2date(
                time_var[:],
                getattr(time_var, "units", "days since 1970-01-01"),
                calendar=getattr(time_var, "calendar", "gregorian"),
            )
        source_station_uids = _decode_strings(ds.variables["selected_source_station_uid"][row_idx, :])
        return _series_from_arrays(
            times,
            ds.variables[variable][row_idx, :],
            ds.variables["{}_flag".format(variable)][row_idx, :],
            source_station_uids,
        )


def _extract_with_h5netcdf(matrix_path, cluster_uid, variable):
    with h5netcdf.File(matrix_path, "r") as ds:
        cluster_uids = _decode_strings(ds.variables["cluster_uid"][:])
        row_idx = cluster_uids.index(cluster_uid)
        time_var = ds.variables["time"]
        times = _decode_time_values(time_var[:], time_var.attrs.get("units", "days since 1970-01-01"))
        source_station_uids = _decode_strings(ds.variables["selected_source_station_uid"][row_idx, :])
        return _series_from_arrays(
            times,
            ds.variables[variable][row_idx, :],
            ds.variables["{}_flag".format(variable)][row_idx, :],
            source_station_uids,
        )


def extract_reference_series(release_dir, resolution, cluster_uid, variable):
    matrix_path = Path(release_dir) / MATRIX_FILES[resolution]
    if not matrix_path.is_file():
        raise FileNotFoundError("Matrix file not found: {}".format(matrix_path))
    if HAS_NC:
        return _extract_with_netCDF4(matrix_path, cluster_uid, variable)
    if HAS_H5NETCDF:
        return _extract_with_h5netcdf(matrix_path, cluster_uid, variable)
    raise RuntimeError("netCDF4 or h5netcdf is required to read reference NetCDF files")


def main():
    ap = argparse.ArgumentParser(description="Example workflow for sed_reference_release_minimal")
    ap.add_argument("--release-dir", required=True, help="Path to sed_reference_release_minimal/")
    ap.add_argument("--resolution", choices=sorted(MATRIX_FILES), required=True)
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--variable", choices=sorted(VARIABLES), default="SSC")
    ap.add_argument("--out-csv", help="Optional output CSV for the extracted series")
    args = ap.parse_args()

    release_dir = Path(args.release_dir).resolve()
    station_catalog = load_station_catalog(release_dir)
    nearest = find_nearest_station(station_catalog, args.lat, args.lon, args.resolution)

    print("Nearest cluster_uid: {}".format(nearest["cluster_uid"]))
    print("Resolution: {}".format(nearest["resolution"]))
    print("Distance (km): {:.3f}".format(float(nearest["distance_km"])))
    print("Station name: {}".format(nearest.get("station_name", "")))
    print("River name: {}".format(nearest.get("river_name", "")))
    print("Time span: {} -> {}".format(nearest.get("time_start", ""), nearest.get("time_end", "")))

    ref_df = extract_reference_series(
        release_dir=release_dir,
        resolution=args.resolution,
        cluster_uid=str(nearest["cluster_uid"]),
        variable=args.variable,
    )
    if len(ref_df) == 0:
        raise RuntimeError("No non-missing {} series found for {}".format(args.variable, nearest["cluster_uid"]))

    print("Reference points: {}".format(len(ref_df)))
    print("Reference time span: {} -> {}".format(ref_df["time"].min(), ref_df["time"].max()))

    if args.out_csv:
        out_path = Path(args.out_csv).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        ref_df.to_csv(out_path, index=False)
        print("Wrote series CSV: {}".format(out_path))


if __name__ == "__main__":
    main()
