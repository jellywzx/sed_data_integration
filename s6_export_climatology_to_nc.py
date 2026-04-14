#!/usr/bin/env python3
"""
Export climatology files to a standalone NetCDF without basin tracing or basin
cluster merge.

Input:
  - output_resolution_organized/climatology/*.nc

Output:
  - scripts_basin_test/output/s6_climatology_only.nc

Design:
  - each climatology file is treated as one source station;
  - no basin screening, no cluster_id, no basin polygon linkage;
  - provenance is preserved through station_uid, source name, native station id
    and source file path.
"""

import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline_paths import (
    S2_ORGANIZED_DIR,
    S6_CLIMATOLOGY_NC,
    S6_CLIMATOLOGY_SHP,
    get_output_r_root,
)

try:
    import netCDF4 as nc4
    from netCDF4 import num2date
    HAS_NC = True
except ImportError:
    HAS_NC = False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_INPUT_DIR = (PROJECT_ROOT / S2_ORGANIZED_DIR / "climatology").resolve()
DEFAULT_OUTPUT = PROJECT_ROOT / S6_CLIMATOLOGY_NC
DEFAULT_OUTPUT_SHP = PROJECT_ROOT / S6_CLIMATOLOGY_SHP

FILL = -9999.0
TIME_NAMES = ["time", "Time", "t", "sample"]
Q_NAMES = ["Q", "discharge", "Discharge_m3_s", "Discharge"]
SSC_NAMES = ["SSC", "ssc", "TSS_mg_L", "TSS"]
SSL_NAMES = ["SSL", "sediment_load", "Sediment_load"]
Q_FLAG_NAMES = ["Q_flag", "discharge_flag", "q_flag"]
SSC_FLAG_NAMES = ["SSC_flag", "ssc_flag", "TSS_flag", "tss_flag"]
SSL_FLAG_NAMES = ["SSL_flag", "ssl_flag", "sediment_load_flag"]
LAT_NAMES = ["lat", "latitude", "Latitude"]
LON_NAMES = ["lon", "longitude", "Longitude"]
FLAG_FILL_BYTE = -127

_STATION_NAME_KEYS = ["station_name", "Station_Name", "stationName", "name"]
_RIVER_NAME_KEYS = ["river_name", "River_Name", "riverName", "river"]
_STATION_ID_KEYS = ["station_id", "Station_ID", "stationID", "ID"]
_SOURCE_NAME_KEYS = ["data_source_name", "Data_Source_Name"]
_INST_KEYS = ["creator_institution", "contributor_institution", "institution"]
_URL_KEYS = ["source_data_link", "sediment_data_source", "source_url"]


def _safe_scalar(var):
    if var is None:
        return None
    if np.ma.isMaskedArray(var):
        values = var.flatten()
        if values.size == 0:
            return None
        value = values.flat[0]
        if np.ma.is_masked(value):
            return None
        value = float(value)
    else:
        arr = np.asarray(var).flatten()
        if arr.size == 0:
            return None
        value = float(arr.flat[0])
    if np.isnan(value) or value in (FILL, -9999.0):
        return None
    return value


def _decode_text(value, limit=512):
    if value is None:
        return ""
    text = str(value).strip()
    return text[:limit]


def _get_var(nc, names, default=np.nan):
    for name in names:
        if name in nc.variables:
            return np.asarray(nc.variables[name][:]).flatten()
    return np.full(1, default)


def _read_flag_var(nc, names, size):
    for name in names:
        if name in nc.variables:
            raw = np.asarray(nc.variables[name][:]).flatten()
            raw = raw[:size] if len(raw) >= size else np.concatenate(
                [raw, np.full(size - len(raw), FLAG_FILL_BYTE, dtype=np.int8)]
            )
            result = raw.astype(np.int16)
            result[result == FLAG_FILL_BYTE] = 9
            return result.astype(np.int8)
    return np.full(size, 9, dtype=np.int8)


def get_source_from_organized_path(path, root_dir):
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if not parts:
            return "unknown"
        resolution = parts[0].strip().lower()
        stem = Path(parts[-1]).stem
        stem_parts = stem.split("_")
        for i, seg in enumerate(stem_parts):
            if seg == resolution:
                return "_".join(stem_parts[:i]) if i > 0 else (stem_parts[0] if stem_parts else "unknown")
        return stem_parts[0] if stem_parts else "unknown"
    except Exception:
        return "unknown"


def read_station_meta(path):
    try:
        with nc4.Dataset(path, "r") as ds:
            def _get_attr(keys):
                for key in keys:
                    value = getattr(ds, key, None)
                    if value is not None and str(value).strip():
                        return _decode_text(value, 256)
                return ""
            lat_name = next((x for x in LAT_NAMES if x in ds.variables), None)
            lon_name = next((x for x in LON_NAMES if x in ds.variables), None)
            lat = _safe_scalar(ds.variables[lat_name][:]) if lat_name else None
            lon = _safe_scalar(ds.variables[lon_name][:]) if lon_name else None
            return {
                "lat": lat,
                "lon": lon,
                "station_name": _get_attr(_STATION_NAME_KEYS),
                "river_name": _get_attr(_RIVER_NAME_KEYS),
                "source_station_id": _get_attr(_STATION_ID_KEYS),
            }
    except Exception:
        return {
            "lat": None,
            "lon": None,
            "station_name": "",
            "river_name": "",
            "source_station_id": "",
        }


def read_source_meta(path):
    try:
        with nc4.Dataset(path, "r") as ds:
            attr_lower = {k.lower(): k for k in ds.ncattrs()}

            def _get(keys):
                for key in keys:
                    original = attr_lower.get(key.lower())
                    if original:
                        value = _decode_text(getattr(ds, original, ""), 512)
                        if value:
                            return value
                return ""

            refs = []
            for key in ds.ncattrs():
                if key.lower().startswith("reference"):
                    value = _decode_text(getattr(ds, key, ""), 512)
                    if value and value not in refs:
                        refs.append(value)

            return {
                "source_long_name": _get(_SOURCE_NAME_KEYS),
                "institution": _get(_INST_KEYS),
                "reference": " | ".join(refs)[:1024],
                "source_url": _get(_URL_KEYS),
            }
    except Exception:
        return {
            "source_long_name": "",
            "institution": "",
            "reference": "",
            "source_url": "",
        }


def load_nc_series(path):
    try:
        with nc4.Dataset(path, "r") as nc:
            time_name = next((x for x in TIME_NAMES if x in nc.variables), None)
            if time_name is None:
                return None
            t = nc.variables[time_name]
            t_vals = np.asarray(t[:]).flatten()
            units = getattr(t, "units", "days since 1970-01-01")
            calendar = getattr(t, "calendar", "gregorian")
            try:
                times = num2date(t_vals, units, calendar=calendar)
            except TypeError:
                try:
                    times = num2date(t_vals, units, calendar=calendar, only_use_cftime_datetimes=False)
                except Exception:
                    times = pd.to_datetime(t_vals, unit="D", origin="1970-01-01")
            except Exception:
                times = pd.to_datetime(t_vals, unit="D", origin="1970-01-01")

            try:
                times = pd.to_datetime(times)
                if hasattr(times, "date"):
                    dates = [pd.Timestamp(tt).date() for tt in times]
                else:
                    dates = [pd.Timestamp(tt).date() for tt in times.tolist()]
            except Exception:
                dates = [pd.Timestamp(str(tt)).date() for tt in times]

            n = len(dates)
            if n == 0:
                return None

            def pad(arr, size, fill=np.nan):
                arr = np.asarray(arr).flatten()
                if len(arr) >= size:
                    return arr[:size]
                return np.concatenate([arr, np.full(size - len(arr), fill)])

            q = pad(_get_var(nc, Q_NAMES), n)
            ssc = pad(_get_var(nc, SSC_NAMES), n)
            ssl = pad(_get_var(nc, SSL_NAMES), n)
            q_flag = _read_flag_var(nc, Q_FLAG_NAMES, n)
            ssc_flag = _read_flag_var(nc, SSC_FLAG_NAMES, n)
            ssl_flag = _read_flag_var(nc, SSL_FLAG_NAMES, n)

            df = pd.DataFrame(
                {
                    "date": pd.to_datetime(dates).date,
                    "Q": q,
                    "SSC": ssc,
                    "SSL": ssl,
                    "Q_flag": q_flag,
                    "SSC_flag": ssc_flag,
                    "SSL_flag": ssl_flag,
                }
            )
            for col in ["Q", "SSC", "SSL"]:
                df.loc[df[col] == FILL, col] = np.nan
                df.loc[df[col] == -9999.0, col] = np.nan
            return df
    except Exception:
        return None


def collect_climatology_files(input_dir):
    paths = sorted(str(p) for p in Path(input_dir).rglob("*.nc"))
    rows = []
    source_meta_lookup = {}
    for i, path_str in enumerate(paths):
        path = Path(path_str)
        source = get_source_from_organized_path(path, input_dir.parent)
        station_meta = read_station_meta(path)
        series = load_nc_series(path)
        if series is None or len(series) == 0:
            continue
        rows.append(
            {
                "station_uid": "CLM{:06d}".format(i),
                "path": str(path),
                "source": source,
                "lat": station_meta["lat"],
                "lon": station_meta["lon"],
                "station_name": station_meta["station_name"],
                "river_name": station_meta["river_name"],
                "source_station_id": station_meta["source_station_id"],
                "series": series,
            }
        )
        if source not in source_meta_lookup:
            source_meta_lookup[source] = read_source_meta(path)
    return rows, source_meta_lookup


def main():
    ap = argparse.ArgumentParser(description="Export climatology files to a standalone NC")
    ap.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="organized climatology directory")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT), help="output climatology nc")
    ap.add_argument("--output-shp", default=str(DEFAULT_OUTPUT_SHP), help="output climatology point shapefile")
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. pip install netCDF4")
        return 1

    input_dir = Path(args.input_dir).resolve()
    output_path = Path(args.output).resolve()
    shp_path    = Path(args.output_shp).resolve() 
    if not input_dir.is_dir():
        print("Error: climatology input dir not found: {}".format(input_dir))
        return 1

    station_rows, source_meta_lookup = collect_climatology_files(input_dir)
    if not station_rows:
        print("Error: no valid climatology stations found under {}".format(input_dir))
        return 1

    unique_sources = sorted(source_meta_lookup.keys())
    source_to_idx = {name: i for i, name in enumerate(unique_sources)}

    n_stations = len(station_rows)
    station_index_parts = []
    time_parts = []
    q_parts = []
    ssc_parts = []
    ssl_parts = []
    q_flag_parts = []
    ssc_flag_parts = []
    ssl_flag_parts = []
    source_parts = []
    ref = pd.Timestamp("1970-01-01")

    lat_arr = np.full(n_stations, FILL, dtype=np.float32)
    lon_arr = np.full(n_stations, FILL, dtype=np.float32)
    station_uid_arr = [""] * n_stations
    station_name_arr = [""] * n_stations
    river_name_arr = [""] * n_stations
    native_id_arr = [""] * n_stations
    path_arr = [""] * n_stations
    source_index_arr = np.full(n_stations, -1, dtype=np.int32)

    for idx, row in enumerate(station_rows):
        station_uid_arr[idx] = row["station_uid"]
        station_name_arr[idx] = _decode_text(row["station_name"], 256)
        river_name_arr[idx] = _decode_text(row["river_name"], 256)
        native_id_arr[idx] = _decode_text(row["source_station_id"], 256)
        path_arr[idx] = row["path"]
        source_index_arr[idx] = source_to_idx[row["source"]]
        if row["lat"] is not None:
            lat_arr[idx] = float(row["lat"])
        if row["lon"] is not None:
            lon_arr[idx] = float(row["lon"])

        series = row["series"]
        dates = pd.to_datetime(series["date"])
        time_parts.append(((dates - ref).dt.total_seconds().values / 86400.0).astype(np.float64))
        station_index_parts.append(np.full(len(series), idx, dtype=np.int32))
        q_parts.append(series["Q"].fillna(FILL).values.astype(np.float32))
        ssc_parts.append(series["SSC"].fillna(FILL).values.astype(np.float32))
        ssl_parts.append(series["SSL"].fillna(FILL).values.astype(np.float32))
        q_flag_parts.append(series["Q_flag"].values.astype(np.int8))
        ssc_flag_parts.append(series["SSC_flag"].values.astype(np.int8))
        ssl_flag_parts.append(series["SSL_flag"].values.astype(np.int8))
        source_parts.append(np.full(len(series), row["source"], dtype=object))

    station_index = np.concatenate(station_index_parts)
    time_arr = np.concatenate(time_parts)
    q_arr = np.concatenate(q_parts)
    ssc_arr = np.concatenate(ssc_parts)
    ssl_arr = np.concatenate(ssl_parts)
    q_flag_arr = np.concatenate(q_flag_parts)
    ssc_flag_arr = np.concatenate(ssc_flag_parts)
    ssl_flag_arr = np.concatenate(ssl_flag_parts)
    source_arr = np.concatenate(source_parts)
    resolution_arr = np.full(len(time_arr), 3, dtype=np.int8)

    source_long_names = [""] * len(unique_sources)
    institutions = [""] * len(unique_sources)
    references = [""] * len(unique_sources)
    source_urls = [""] * len(unique_sources)
    for source_name, sidx in source_to_idx.items():
        meta = source_meta_lookup[source_name]
        source_long_names[sidx] = meta["source_long_name"] or source_name
        institutions[sidx] = meta["institution"]
        references[sidx] = meta["reference"]
        source_urls[sidx] = meta["source_url"]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with nc4.Dataset(output_path, "w", format="NETCDF4") as nc:
        nc.createDimension("n_stations", n_stations)
        nc.createDimension("n_sources", len(unique_sources))
        nc.createDimension("n_records", len(time_arr))

        lat_v = nc.createVariable("lat", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        lat_v.long_name = "latitude of climatology station"
        lat_v.units = "degrees_north"
        lat_v[:] = lat_arr

        lon_v = nc.createVariable("lon", "f4", ("n_stations",), fill_value=FILL, zlib=True, complevel=4)
        lon_v.long_name = "longitude of climatology station"
        lon_v.units = "degrees_east"
        lon_v[:] = lon_arr

        uid_v = nc.createVariable("station_uid", str, ("n_stations",))
        uid_v.long_name = "unique climatology station identifier"
        uid_v[:] = np.array(station_uid_arr, dtype=object)

        src_idx_v = nc.createVariable("source_index", "i4", ("n_stations",))
        src_idx_v.long_name = "0-based index into n_sources dimension"
        src_idx_v[:] = source_index_arr

        stn_v = nc.createVariable("station_name", str, ("n_stations",))
        stn_v[:] = np.array(station_name_arr, dtype=object)

        riv_v = nc.createVariable("river_name", str, ("n_stations",))
        riv_v[:] = np.array(river_name_arr, dtype=object)

        sid_v = nc.createVariable("source_station_id", str, ("n_stations",))
        sid_v.long_name = "native station id from source file"
        sid_v[:] = np.array(native_id_arr, dtype=object)

        path_v = nc.createVariable("source_station_path", str, ("n_stations",))
        path_v.long_name = "absolute path of the organized climatology nc"
        path_v[:] = np.array(path_arr, dtype=object)

        sname_v = nc.createVariable("source_name", str, ("n_sources",))
        sname_v[:] = np.array(unique_sources, dtype=object)

        slong_v = nc.createVariable("source_long_name", str, ("n_sources",))
        slong_v[:] = np.array(source_long_names, dtype=object)

        inst_v = nc.createVariable("institution", str, ("n_sources",))
        inst_v[:] = np.array(institutions, dtype=object)

        ref_v = nc.createVariable("reference", str, ("n_sources",))
        ref_v[:] = np.array(references, dtype=object)

        surl_v = nc.createVariable("source_url", str, ("n_sources",))
        surl_v[:] = np.array(source_urls, dtype=object)

        station_idx_v = nc.createVariable("station_index", "i4", ("n_records",), zlib=True, complevel=4)
        station_idx_v.long_name = "0-based index into n_stations dimension"
        station_idx_v[:] = station_index

        time_v = nc.createVariable("time", "f8", ("n_records",), zlib=True, complevel=4)
        time_v.long_name = "time"
        time_v.units = "days since 1970-01-01"
        time_v.calendar = "gregorian"
        time_v[:] = time_arr

        res_v = nc.createVariable("resolution", "i1", ("n_records",), zlib=True, complevel=4)
        res_v.long_name = "time type code for this record"
        res_v.flag_values = np.array([0, 1, 2, 3, 4], dtype=np.int8)
        res_v.flag_meanings = "daily monthly annual climatology other"
        res_v[:] = resolution_arr

        q_v = nc.createVariable("Q", "f4", ("n_records",), fill_value=FILL, zlib=True, complevel=4)
        q_v.long_name = "river discharge"
        q_v.units = "m3 s-1"
        q_v[:] = q_arr

        ssc_v = nc.createVariable("SSC", "f4", ("n_records",), fill_value=FILL, zlib=True, complevel=4)
        ssc_v.long_name = "suspended sediment concentration"
        ssc_v.units = "mg L-1"
        ssc_v[:] = ssc_arr

        ssl_v = nc.createVariable("SSL", "f4", ("n_records",), fill_value=FILL, zlib=True, complevel=4)
        ssl_v.long_name = "suspended sediment load"
        ssl_v.units = "ton day-1"
        ssl_v[:] = ssl_arr

        flag_kw = dict(flag_values=np.array([0, 1, 2, 3, 9], dtype=np.int8), flag_meanings="good estimated suspect bad missing")

        qf_v = nc.createVariable("Q_flag", "i1", ("n_records",), fill_value=np.int8(9), zlib=True, complevel=4)
        qf_v.long_name = "quality flag for river discharge"
        for key, value in flag_kw.items():
            setattr(qf_v, key, value)
        qf_v[:] = q_flag_arr

        sscf_v = nc.createVariable("SSC_flag", "i1", ("n_records",), fill_value=np.int8(9), zlib=True, complevel=4)
        sscf_v.long_name = "quality flag for suspended sediment concentration"
        for key, value in flag_kw.items():
            setattr(sscf_v, key, value)
        sscf_v[:] = ssc_flag_arr

        sslf_v = nc.createVariable("SSL_flag", "i1", ("n_records",), fill_value=np.int8(9), zlib=True, complevel=4)
        sslf_v.long_name = "quality flag for suspended sediment load"
        for key, value in flag_kw.items():
            setattr(sslf_v, key, value)
        sslf_v[:] = ssl_flag_arr

        rec_src_v = nc.createVariable("source", str, ("n_records",))
        rec_src_v.long_name = "source dataset name for this record"
        rec_src_v[:] = source_arr

        nc.title = "Global river suspended sediment climatology dataset (unclustered)"
        nc.Conventions = "CF-1.8"
        nc.source = "Exported directly from organized climatology files without basin tracing or basin merge"
        nc.history = "Created {} by s6_export_climatology_to_nc.py".format(datetime.now().isoformat(timespec="seconds"))
        nc.provenance_policy = "Each climatology station is one organized climatology file; source_station_path preserves file-level provenance"
        nc.time_type_policy = "All records in this file are climatology and therefore stored separately from the basin mainline"
        nc.n_input_files = str(n_stations)

        nc.sync()

    print("Wrote climatology NC: {}".format(output_path))
    print("Stations: {}  Records: {}".format(n_stations, len(time_arr)))

    # ── 输出点 SHP ───────────────────────────────────────────────────────────
    try:
        import geopandas as gpd
        from shapely.geometry import Point

        records = []
        for idx in range(n_stations):
            lat_val = float(lat_arr[idx])
            lon_val = float(lon_arr[idx])
            if lat_val == FILL or lon_val == FILL or np.isnan(lat_val) or np.isnan(lon_val):
                continue
            src_name = unique_sources[source_index_arr[idx]] if source_index_arr[idx] >= 0 else ""
            records.append({
                "station_ui": station_uid_arr[idx],
                "lat":        round(lat_val, 6),
                "lon":        round(lon_val, 6),
                "stn_name":   station_name_arr[idx][:80],
                "river_nm":   river_name_arr[idx][:80],
                "native_id":  native_id_arr[idx][:80],
                "source":     src_name[:40],
                "geometry":   Point(lon_val, lat_val),
            })

        if records:
            point_gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
            shp_path.parent.mkdir(parents=True, exist_ok=True)
            driver = "ESRI Shapefile" if shp_path.suffix.lower() == ".shp" else "GPKG"
            point_gdf.to_file(shp_path, driver=driver, encoding="UTF-8")
            print("Wrote climatology point SHP: {} ({} stations)".format(shp_path, len(records)))
        else:
            print("Warning: no valid coordinates found, skipping SHP output")
    except ImportError:
        print("Warning: geopandas not available, skipping SHP output")
    except Exception as e:
        print("Warning: SHP output failed: {}".format(e))

    return 0

    print("Wrote climatology NC: {}".format(output_path))
    print("Stations: {}  Records: {}".format(n_stations, len(time_arr)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
