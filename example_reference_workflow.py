#!/usr/bin/env python3
"""
Example workflow for the sediment reference dataset release.

What this example demonstrates:
1. load the station catalog and find the nearest reference station;
2. extract the observed time series from a resolution-specific matrix NetCDF;
3. look up record-level provenance in the master NetCDF;
4. optionally align the reference series with a gridded model NetCDF.

Typical usage:
  python3 example_reference_workflow.py \
    --release-dir /path/to/sed_reference_release \
    --resolution monthly \
    --lat 30.5 \
    --lon 114.3 \
    --variable SSC

Optional model comparison:
  python3 example_reference_workflow.py \
    --release-dir /path/to/sed_reference_release \
    --resolution monthly \
    --lat 30.5 \
    --lon 114.3 \
    --variable SSC \
    --model-nc /path/to/model.nc \
    --model-var sediment \
    --out-csv /tmp/aligned_timeseries.csv
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt

    HAS_MPL = True
except ImportError:
    plt = None
    HAS_MPL = False

try:
    import netCDF4 as nc4

    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False

try:
    import xarray as xr

    HAS_XR = True
except ImportError:
    xr = None
    HAS_XR = False


RESOLUTION_CODE = {"daily": 0, "monthly": 1, "annual": 2}
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


def load_station_catalog(release_dir):
    path = Path(release_dir) / "station_catalog.csv"
    if not path.is_file():
        raise FileNotFoundError("station_catalog.csv not found in {}".format(release_dir))
    return pd.read_csv(path, keep_default_na=False)


def find_nearest_station(station_catalog, lat, lon):
    work = station_catalog.copy()
    work = work[np.isfinite(work["lat"]) & np.isfinite(work["lon"])].copy()
    distances = _haversine_km(lat, lon, work["lat"].values, work["lon"].values)
    idx = int(np.argmin(distances))
    row = work.iloc[idx].copy()
    row["distance_km"] = float(distances[idx])
    return row


def extract_reference_series(release_dir, resolution, cluster_uid, variable):
    matrix_path = Path(release_dir) / MATRIX_FILES[resolution]
    if not matrix_path.is_file():
        raise FileNotFoundError("Matrix file not found: {}".format(matrix_path))
    if not HAS_NC:
        raise RuntimeError("netCDF4 is required to read reference NetCDF files")

    with nc4.Dataset(matrix_path, "r") as ds:
        cluster_uids = np.asarray(ds.variables["cluster_uid"][:], dtype=object).reshape(-1)
        cluster_uids = [_clean_text(item) for item in cluster_uids]
        try:
            row_idx = cluster_uids.index(cluster_uid)
        except ValueError:
            raise KeyError("cluster_uid {} not found in {}".format(cluster_uid, matrix_path.name))

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

        data = np.ma.asarray(ds.variables[variable][row_idx, :]).filled(np.nan)
        flag = np.ma.asarray(ds.variables["{}_flag".format(variable)][row_idx, :]).filled(9)
        overlap = np.ma.asarray(ds.variables["is_overlap"][row_idx, :]).filled(0)
        selected_source = np.ma.asarray(ds.variables["selected_source_index"][row_idx, :]).filled(-1)
        source_names = np.asarray(ds.variables["source_name"][:], dtype=object).reshape(-1)
        source_names = [_clean_text(item) for item in source_names]

    df = pd.DataFrame(
        {
            "time": pd.to_datetime(times),
            "reference_value": data.astype(np.float64),
            "reference_flag": flag.astype(np.int16),
            "is_overlap": overlap.astype(np.int16),
            "selected_source_index": selected_source.astype(np.int32),
        }
    )
    df["selected_source_name"] = df["selected_source_index"].map(
        lambda idx: source_names[int(idx)] if 0 <= int(idx) < len(source_names) else ""
    )
    df = df[np.isfinite(df["reference_value"])].reset_index(drop=True)
    return df


def _find_master_record_index(ds, station_idx, resolution_code, target_time_num, chunk_size=500000):
    n_records = len(ds.dimensions["n_records"])
    station_var = ds.variables["station_index"]
    resolution_var = ds.variables["resolution"]
    time_var = ds.variables["time"]
    for start in range(0, n_records, chunk_size):
        stop = min(start + chunk_size, n_records)
        station_chunk = np.asarray(station_var[start:stop], dtype=np.int32).reshape(-1)
        resolution_chunk = np.asarray(resolution_var[start:stop], dtype=np.int16).reshape(-1)
        time_chunk = np.asarray(time_var[start:stop], dtype=np.float64).reshape(-1)
        mask = (
            (station_chunk == int(station_idx))
            & (resolution_chunk == int(resolution_code))
            & np.isclose(time_chunk, float(target_time_num))
        )
        hits = np.flatnonzero(mask)
        if len(hits) > 0:
            return start + int(hits[0])
    return None


def lookup_provenance(release_dir, cluster_uid, resolution, time_value):
    master_path = Path(release_dir) / "sed_reference_master.nc"
    source_station_catalog_path = Path(release_dir) / "source_station_catalog.csv"
    source_station_catalog = pd.read_csv(source_station_catalog_path, keep_default_na=False)

    if not HAS_NC:
        raise RuntimeError("netCDF4 is required to read master NetCDF")

    with nc4.Dataset(master_path, "r") as ds:
        cluster_uids = np.asarray(ds.variables["cluster_uid"][:], dtype=object).reshape(-1)
        cluster_uids = [_clean_text(item) for item in cluster_uids]
        try:
            station_idx = cluster_uids.index(cluster_uid)
        except ValueError:
            return pd.DataFrame()

        time_var = ds.variables["time"]
        target_time_num = nc4.date2num(
            pd.Timestamp(time_value).to_pydatetime(),
            getattr(time_var, "units", "days since 1970-01-01"),
            calendar=getattr(time_var, "calendar", "gregorian"),
        )
        record_idx = _find_master_record_index(
            ds,
            station_idx=int(station_idx),
            resolution_code=RESOLUTION_CODE[resolution],
            target_time_num=target_time_num,
        )
        if record_idx is None:
            return pd.DataFrame()
        source_station_lookup = source_station_catalog.set_index("source_station_index").to_dict("index")
        src_idx = int(np.ma.asarray(ds.variables["source_station_index"][record_idx]).filled(-1))
        source_station_row = source_station_lookup.get(src_idx, {})
        q_val = np.ma.asarray(ds.variables["Q"][record_idx]).filled(np.nan)
        ssc_val = np.ma.asarray(ds.variables["SSC"][record_idx]).filled(np.nan)
        ssl_val = np.ma.asarray(ds.variables["SSL"][record_idx]).filled(np.nan)
        overlap_val = int(np.ma.asarray(ds.variables["is_overlap"][record_idx]).filled(0))
        rows = [
            {
                "cluster_uid": cluster_uid,
                "time": str(pd.Timestamp(time_value)),
                "resolution": resolution,
                "source_name": _clean_text(ds.variables["source"][record_idx]),
                "source_station_index": src_idx,
                "source_station_uid": source_station_row.get("source_station_uid", ""),
                "source_station_native_id": source_station_row.get("source_station_native_id", ""),
                "source_station_name": source_station_row.get("source_station_name", ""),
                "source_station_paths": source_station_row.get("source_station_paths", ""),
                "is_overlap": overlap_val,
                "Q": float(q_val) if np.isfinite(q_val) else np.nan,
                "SSC": float(ssc_val) if np.isfinite(ssc_val) else np.nan,
                "SSL": float(ssl_val) if np.isfinite(ssl_val) else np.nan,
            }
        ]
    return pd.DataFrame(rows)


def _find_nearest_model_cell(ds, lat_name, lon_name, target_lat, target_lon):
    lat = ds[lat_name]
    lon = ds[lon_name]
    if lat.ndim == 1 and lon.ndim == 1:
        lat_idx = int(np.abs(lat.values - target_lat).argmin())
        lon_idx = int(np.abs(lon.values - target_lon).argmin())
        return {lat.dims[0]: lat_idx, lon.dims[0]: lon_idx}

    if lat.shape != lon.shape:
        raise ValueError("2D model lat/lon must have the same shape")

    distances = _haversine_km(target_lat, target_lon, lat.values, lon.values)
    flat_idx = int(np.nanargmin(distances))
    index = np.unravel_index(flat_idx, lat.shape)
    return {dim: int(i) for dim, i in zip(lat.dims, index)}


def extract_model_series(model_nc, model_var, lat, lon, lat_name, lon_name, time_name):
    if not HAS_XR:
        raise RuntimeError("xarray is required for model comparison")
    ds = xr.open_dataset(model_nc)
    if model_var not in ds:
        raise KeyError("model variable '{}' not found".format(model_var))
    if lat_name not in ds or lon_name not in ds or time_name not in ds:
        raise KeyError("model lat/lon/time variables are missing")

    indexers = _find_nearest_model_cell(ds, lat_name, lon_name, lat, lon)
    arr = ds[model_var].isel(indexers)
    if time_name not in arr.dims:
        ds.close()
        raise ValueError("model variable '{}' has no '{}' dimension".format(model_var, time_name))

    model_df = (
        arr.to_dataframe(name="model_value")
        .reset_index()
        [[time_name, "model_value"]]
        .rename(columns={time_name: "time"})
    )
    model_df["time"] = pd.to_datetime(model_df["time"])
    ds.close()
    return model_df


def summarize_alignment(aligned):
    if len(aligned) == 0:
        return {
            "n_pairs": 0,
            "bias": np.nan,
            "rmse": np.nan,
            "correlation": np.nan,
        }
    diff = aligned["model_value"] - aligned["reference_value"]
    corr = np.nan
    if len(aligned) >= 2:
        corr = aligned["model_value"].corr(aligned["reference_value"])
    return {
        "n_pairs": int(len(aligned)),
        "bias": float(diff.mean()),
        "rmse": float(np.sqrt(np.mean(diff ** 2))),
        "correlation": float(corr) if pd.notna(corr) else np.nan,
    }


def maybe_plot(aligned, out_path, variable, cluster_uid):
    if out_path is None:
        return
    if not HAS_MPL:
        raise RuntimeError("matplotlib is required for plotting")
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(aligned["time"], aligned["reference_value"], label="reference {}".format(variable), lw=1.8)
    if "model_value" in aligned.columns:
        ax.plot(aligned["time"], aligned["model_value"], label="model", lw=1.2)
    ax.set_title("cluster_uid = {}".format(cluster_uid))
    ax.set_xlabel("time")
    ax.set_ylabel(variable)
    ax.legend()
    ax.grid(True, alpha=0.3)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description="Example workflow for the sediment reference dataset release")
    ap.add_argument("--release-dir", required=True, help="Path to sed_reference_release/")
    ap.add_argument("--resolution", choices=sorted(MATRIX_FILES), required=True)
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--variable", choices=sorted(VARIABLES), default="SSC")
    ap.add_argument("--model-nc", help="Optional model NetCDF path")
    ap.add_argument("--model-var", help="Model variable name")
    ap.add_argument("--model-lat-name", default="lat")
    ap.add_argument("--model-lon-name", default="lon")
    ap.add_argument("--model-time-name", default="time")
    ap.add_argument("--out-csv", help="Optional output CSV for the extracted/aligned series")
    ap.add_argument("--out-plot", help="Optional output PNG for a quick comparison plot")
    args = ap.parse_args()

    release_dir = Path(args.release_dir).resolve()
    station_catalog = load_station_catalog(release_dir)
    nearest = find_nearest_station(station_catalog, args.lat, args.lon)

    print("Nearest cluster_uid: {}".format(nearest["cluster_uid"]))
    print("Distance (km): {:.3f}".format(float(nearest["distance_km"])))
    print("Station name: {}".format(nearest.get("station_name", "")))
    print("River name: {}".format(nearest.get("river_name", "")))
    print("Available resolutions: {}".format(nearest.get("available_resolutions", "")))

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

    aligned = ref_df.copy()

    if args.model_nc:
        if not args.model_var:
            raise ValueError("--model-var is required when --model-nc is provided")
        model_df = extract_model_series(
            model_nc=Path(args.model_nc).resolve(),
            model_var=args.model_var,
            lat=float(nearest["lat"]),
            lon=float(nearest["lon"]),
            lat_name=args.model_lat_name,
            lon_name=args.model_lon_name,
            time_name=args.model_time_name,
        )
        aligned = ref_df.merge(model_df, on="time", how="inner")
        stats = summarize_alignment(aligned)
        print("Aligned pairs: {}".format(stats["n_pairs"]))
        print("Bias: {}".format(stats["bias"]))
        print("RMSE: {}".format(stats["rmse"]))
        print("Correlation: {}".format(stats["correlation"]))

    provenance = lookup_provenance(
        release_dir=release_dir,
        cluster_uid=str(nearest["cluster_uid"]),
        resolution=args.resolution,
        time_value=aligned["time"].iloc[0],
    )
    if len(provenance):
        print("First provenance hit:")
        first = provenance.iloc[0]
        print("  source_name = {}".format(first["source_name"]))
        print("  source_station_uid = {}".format(first["source_station_uid"]))
        print("  source_station_name = {}".format(first["source_station_name"]))
        print("  source_station_paths = {}".format(first["source_station_paths"]))

    if args.out_csv:
        out_path = Path(args.out_csv).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        aligned.to_csv(out_path, index=False)
        print("Wrote series CSV: {}".format(out_path))

    if args.out_plot:
        maybe_plot(aligned, Path(args.out_plot).resolve(), args.variable, str(nearest["cluster_uid"]))
        print("Wrote plot: {}".format(Path(args.out_plot).resolve()))


if __name__ == "__main__":
    main()
