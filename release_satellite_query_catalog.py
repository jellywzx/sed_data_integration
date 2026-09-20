#!/usr/bin/env python3
"""Export the final public satellite NetCDF as a record-level query catalogue.

The public query catalogue is generated only after S9 has converted internal
cluster-facing identifiers to the manuscript/SP station-facing names. One CSV
row is written for every n_satellite_records entry in sed_reference_satellite.nc.
"""

import argparse
import gzip
from pathlib import Path

import netCDF4 as nc4
import numpy as np
import pandas as pd


QUERY_CATALOG_NAME = "satellite_query_catalog.csv.gz"

QUERY_COLUMNS = (
    "lat",
    "lon",
    "time",
    "satellite_station_uid",
    "source",
    "station_name",
    "river_name",
    "station_resolution",
    "station_uid",
    "linked_station_uid",
    "link_distance_m",
    "unlinked_reason",
    "satellite_station_index",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
)

STATION_COLUMNS = (
    "lat",
    "lon",
    "satellite_station_uid",
    "source",
    "station_name",
    "river_name",
    "station_resolution",
    "station_uid",
    "linked_station_uid",
    "link_distance_m",
    "unlinked_reason",
)

TEXT_STATION_COLUMNS = {
    "satellite_station_uid",
    "source",
    "station_name",
    "river_name",
    "station_resolution",
    "station_uid",
    "linked_station_uid",
    "unlinked_reason",
}

RECORD_COLUMNS = (
    "time",
    "satellite_station_index",
    "Q",
    "SSC",
    "SSL",
    "Q_flag",
    "SSC_flag",
    "SSL_flag",
)

FLAG_COLUMNS = {"Q_flag", "SSC_flag", "SSL_flag"}


def _decode_text(value):
    if isinstance(value, (bytes, np.bytes_)):
        return value.decode("utf-8", errors="replace").strip("\x00").strip()
    if value is None:
        return ""
    return str(value).strip()


def _read_text_array(var):
    data = var[:]
    if np.ma.isMaskedArray(data):
        data = data.filled("")
    arr = np.asarray(data)
    if arr.dtype.kind in {"S", "U"} and arr.ndim >= 2 and arr.dtype.itemsize == 1:
        arr = nc4.chartostring(arr)
    return np.asarray([_decode_text(item) for item in np.asarray(arr).reshape(-1)], dtype=object)


def _read_numeric_array(var):
    data = np.ma.asarray(var[:])
    if np.ma.isMaskedArray(data):
        data = data.filled(np.nan)
    return np.asarray(data).reshape(-1)


def _read_record_slice(var, start, stop, name):
    data = np.ma.asarray(var[start:stop])
    if np.ma.isMaskedArray(data):
        if name in FLAG_COLUMNS:
            data = data.filled(9)
        elif name == "satellite_station_index":
            data = data.filled(-1)
        else:
            data = data.filled(np.nan)
    return np.asarray(data).reshape(-1)


def export_satellite_query_catalog(nc_path, output_path, chunk_size=250000):
    """Write a gzip-compressed record-level query catalogue from public NetCDF."""
    nc_path = Path(nc_path).expanduser().resolve()
    output_path = Path(output_path).expanduser().resolve()
    chunk_size = max(1, int(chunk_size))

    if not nc_path.is_file():
        raise FileNotFoundError("satellite NetCDF not found: {}".format(nc_path))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    with nc4.Dataset(nc_path, "r") as ds:
        required = set(QUERY_COLUMNS)
        missing = sorted(required.difference(ds.variables))
        if missing:
            raise KeyError(
                "public satellite NetCDF is missing query-catalogue variables: {}".format(
                    ", ".join(missing)
                )
            )
        if "n_satellite_records" not in ds.dimensions or "n_satellite_stations" not in ds.dimensions:
            raise KeyError("satellite NetCDF is missing n_satellite_records/n_satellite_stations dimensions")

        n_records = len(ds.dimensions["n_satellite_records"])
        n_stations = len(ds.dimensions["n_satellite_stations"])

        station_data = {}
        for name in STATION_COLUMNS:
            var = ds.variables[name]
            values = _read_text_array(var) if name in TEXT_STATION_COLUMNS else _read_numeric_array(var)
            if len(values) != n_stations:
                raise ValueError(
                    "{} has {} values; expected {} station values".format(
                        name, len(values), n_stations
                    )
                )
            station_data[name] = values

        written = 0
        with gzip.open(tmp_path, "wt", encoding="utf-8", newline="", compresslevel=6) as handle:
            first = True
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                record_data = {
                    name: _read_record_slice(ds.variables[name], start, stop, name)
                    for name in RECORD_COLUMNS
                }
                station_index = record_data["satellite_station_index"].astype(np.int64, copy=False)
                if np.any(station_index < 0) or np.any(station_index >= n_stations):
                    bad = station_index[(station_index < 0) | (station_index >= n_stations)]
                    raise ValueError(
                        "invalid satellite_station_index in records {}:{}; sample={}".format(
                            start,
                            stop,
                            bad[:10].tolist(),
                        )
                    )

                payload = {
                    "lat": station_data["lat"][station_index],
                    "lon": station_data["lon"][station_index],
                    "time": record_data["time"],
                    "satellite_station_uid": station_data["satellite_station_uid"][station_index],
                    "source": station_data["source"][station_index],
                    "station_name": station_data["station_name"][station_index],
                    "river_name": station_data["river_name"][station_index],
                    "station_resolution": station_data["station_resolution"][station_index],
                    "station_uid": station_data["station_uid"][station_index],
                    "linked_station_uid": station_data["linked_station_uid"][station_index],
                    "link_distance_m": station_data["link_distance_m"][station_index],
                    "unlinked_reason": station_data["unlinked_reason"][station_index],
                    "satellite_station_index": station_index,
                    "Q": record_data["Q"],
                    "SSC": record_data["SSC"],
                    "SSL": record_data["SSL"],
                    "Q_flag": record_data["Q_flag"],
                    "SSC_flag": record_data["SSC_flag"],
                    "SSL_flag": record_data["SSL_flag"],
                }
                frame = pd.DataFrame(payload, columns=QUERY_COLUMNS)
                frame.to_csv(handle, index=False, header=first)
                first = False
                written += len(frame)

    if written != n_records:
        if tmp_path.exists():
            tmp_path.unlink()
        raise RuntimeError(
            "satellite query catalogue row count mismatch: wrote {}, expected {}".format(
                written, n_records
            )
        )

    tmp_path.replace(output_path)

    header = list(pd.read_csv(output_path, nrows=0).columns)
    if header != list(QUERY_COLUMNS):
        raise RuntimeError(
            "satellite query catalogue header mismatch: {}".format("|".join(header))
        )

    return {
        "path": str(output_path),
        "records": int(written),
        "stations": int(n_stations),
        "columns": list(QUERY_COLUMNS),
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="Final public sed_reference_satellite.nc")
    parser.add_argument(
        "--output",
        default=QUERY_CATALOG_NAME,
        help="Output gzip CSV path (default: satellite_query_catalog.csv.gz).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=250000,
        help="Number of observation records exported per chunk.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    summary = export_satellite_query_catalog(
        args.input,
        args.output,
        chunk_size=args.chunk_size,
    )
    print(
        "Wrote {records} records for {stations} stations -> {path}".format(**summary)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
