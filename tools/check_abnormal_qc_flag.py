from pathlib import Path
import csv
import numpy as np
import netCDF4 as nc


ALLOWED_FLAGS = {0, 1, 2, 3, 9}
FLAG_VARS = ("Q_flag", "SSC_flag", "SSL_flag")
RESOLUTION_MAP = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}


def _read_text_array(var):
    arr = np.asarray(var[:], dtype=object).reshape(-1)
    out = []
    for x in arr:
        if x is None:
            out.append("")
        elif isinstance(x, bytes):
            out.append(x.decode("utf-8", errors="ignore").strip())
        else:
            out.append(str(x).strip())
    return out


def _read_numeric_array(var, dtype):
    data = np.ma.asarray(var[:]).reshape(-1)
    if np.ma.isMaskedArray(data):
        return data.astype(dtype).filled(-9999)
    return np.asarray(data, dtype=dtype).reshape(-1)


def find_abnormal_flags(nc_path, out_csv="abnormal_flag_trace.csv", allowed_flags=None):
    nc_path = Path(nc_path)
    if not nc_path.is_file():
        raise FileNotFoundError(f"File not found: {nc_path}")

    allowed = set(ALLOWED_FLAGS if allowed_flags is None else allowed_flags)
    rows = []

    with nc.Dataset(nc_path, "r") as ds:
        # record-level arrays
        station_index = _read_numeric_array(ds.variables["station_index"], np.int64)
        source_station_index = _read_numeric_array(ds.variables["source_station_index"], np.int64)
        resolution = _read_numeric_array(ds.variables["resolution"], np.int16)
        time_num = _read_numeric_array(ds.variables["time"], np.float64)
        source = _read_text_array(ds.variables["source"])

        time_var = ds.variables["time"]
        time_units = getattr(time_var, "units", "days since 1970-01-01")
        time_calendar = getattr(time_var, "calendar", "gregorian")

        # station-level arrays
        cluster_uid = _read_text_array(ds.variables["cluster_uid"])
        cluster_id = _read_numeric_array(ds.variables["cluster_id"], np.int64)

        # source-station-level arrays
        source_station_uid = _read_text_array(ds.variables["source_station_uid"])
        source_station_native_id = _read_text_array(ds.variables["source_station_native_id"])
        source_station_paths = _read_text_array(ds.variables["source_station_paths"])

        for flag_var in FLAG_VARS:
            if flag_var not in ds.variables:
                print(f"[WARN] Missing variable: {flag_var}")
                continue

            flag_arr = _read_numeric_array(ds.variables[flag_var], np.int16)

            unique_vals = sorted(set(flag_arr.tolist()))
            abnormal_vals = [v for v in unique_vals if v not in allowed and v != -9999]

            print(f"[{flag_var}] unique={unique_vals}")
            print(f"[{flag_var}] abnormal={abnormal_vals}")

            if not abnormal_vals:
                continue

            abnormal_mask = np.array([(v not in allowed and v != -9999) for v in flag_arr], dtype=bool)
            abnormal_idx = np.flatnonzero(abnormal_mask)

            for rec_idx in abnormal_idx:
                rec_idx = int(rec_idx)
                flag_value = int(flag_arr[rec_idx])

                st_idx = int(station_index[rec_idx]) if rec_idx < len(station_index) else -1
                ss_idx = int(source_station_index[rec_idx]) if rec_idx < len(source_station_index) else -1
                res_code = int(resolution[rec_idx]) if rec_idx < len(resolution) else -9999

                clu_uid = cluster_uid[st_idx] if 0 <= st_idx < len(cluster_uid) else ""
                clu_id = int(cluster_id[st_idx]) if 0 <= st_idx < len(cluster_id) else -1

                ss_uid = source_station_uid[ss_idx] if 0 <= ss_idx < len(source_station_uid) else ""
                ss_native = source_station_native_id[ss_idx] if 0 <= ss_idx < len(source_station_native_id) else ""
                ss_paths = source_station_paths[ss_idx] if 0 <= ss_idx < len(source_station_paths) else ""

                try:
                    dt = nc.num2date(
                        float(time_num[rec_idx]),
                        units=time_units,
                        calendar=time_calendar,
                        only_use_cftime_datetimes=False,
                    )
                except TypeError:
                    dt = nc.num2date(
                        float(time_num[rec_idx]),
                        units=time_units,
                        calendar=time_calendar,
                    )

                rows.append(
                    {
                        "record_index": rec_idx,
                        "flag_var": flag_var,
                        "flag_value": flag_value,
                        "cluster_uid": clu_uid,
                        "cluster_id": clu_id,
                        "station_index": st_idx,
                        "source": source[rec_idx] if rec_idx < len(source) else "",
                        "source_station_index": ss_idx,
                        "source_station_uid": ss_uid,
                        "source_station_native_id": ss_native,
                        "resolution_code": res_code,
                        "resolution_name": RESOLUTION_MAP.get(res_code, f"unknown_{res_code}"),
                        "time": str(dt),
                        "source_station_paths": ss_paths,
                    }
                )

    # 写 CSV
    out_csv = Path(out_csv)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "record_index",
                "flag_var",
                "flag_value",
                "cluster_uid",
                "cluster_id",
                "station_index",
                "source",
                "source_station_index",
                "source_station_uid",
                "source_station_native_id",
                "resolution_code",
                "resolution_name",
                "time",
                "source_station_paths",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print()
    print(f"Abnormal records: {len(rows)}")
    print(f"CSV written to: {out_csv.resolve()}")

    # 终端预览前几条
    preview_n = min(20, len(rows))
    if preview_n:
        print()
        print(f"Preview first {preview_n} abnormal records:")
        for row in rows[:preview_n]:
            print(
                f"- rec={row['record_index']} "
                f"{row['flag_var']}={row['flag_value']} "
                f"cluster={row['cluster_uid']} "
                f"source={row['source']} "
                f"time={row['time']} "
                f"path={row['source_station_paths']}"
            )

    return rows


if __name__ == "__main__":
    find_abnormal_flags(
        nc_path="s6_basin_merged_all.nc",
        out_csv="abnormal_flag_trace.csv",
        allowed_flags={0, 1, 2, 3, 9},
    )
