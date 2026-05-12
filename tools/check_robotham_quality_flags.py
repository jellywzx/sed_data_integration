from __future__ import annotations

from pathlib import Path
import argparse
import csv
from collections import Counter, defaultdict

import numpy as np
import netCDF4 as nc


DEFAULT_ALLOWED_FLAGS = {0, 1, 2, 3, 9}
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



def _safe_read_text(ds, var_name, expected_len=None):
    if var_name not in ds.variables:
        if expected_len is None:
            return []
        return [""] * expected_len
    arr = _read_text_array(ds.variables[var_name])
    if expected_len is not None and len(arr) < expected_len:
        arr = arr + [""] * (expected_len - len(arr))
    return arr



def _safe_read_numeric(ds, var_name, dtype, expected_len=None, fill_value=-9999):
    if var_name not in ds.variables:
        if expected_len is None:
            return np.array([], dtype=dtype)
        return np.full(expected_len, fill_value, dtype=dtype)
    arr = _read_numeric_array(ds.variables[var_name], dtype)
    if expected_len is not None and len(arr) < expected_len:
        pad = np.full(expected_len - len(arr), fill_value, dtype=dtype)
        arr = np.concatenate([arr, pad])
    return arr



def iter_robotham_files(root_dir: Path):
    patterns = [
        "Robotham*.nc",
        "*Robotham*.nc",
    ]
    seen = set()
    for pattern in patterns:
        for p in root_dir.rglob(pattern):
            if p.is_file() and p not in seen:
                seen.add(p)
                yield p



def inspect_one_file(nc_path: Path, allowed_flags: set[int]):
    rows = []
    summary = {}

    with nc.Dataset(nc_path, "r") as ds:
        nrec = len(ds.dimensions["record"]) if "record" in ds.dimensions else None
        if nrec is None:
            for candidate in FLAG_VARS:
                if candidate in ds.variables:
                    nrec = len(np.asarray(ds.variables[candidate][:]).reshape(-1))
                    break
        if nrec is None:
            raise ValueError(f"Cannot determine record length for {nc_path}")

        station_index = _safe_read_numeric(ds, "station_index", np.int64, nrec)
        source_station_index = _safe_read_numeric(ds, "source_station_index", np.int64, nrec)
        resolution = _safe_read_numeric(ds, "resolution", np.int16, nrec)
        time_num = _safe_read_numeric(ds, "time", np.float64, nrec)
        source = _safe_read_text(ds, "source", nrec)

        time_var = ds.variables["time"] if "time" in ds.variables else None
        time_units = getattr(time_var, "units", "days since 1970-01-01")
        time_calendar = getattr(time_var, "calendar", "gregorian")

        cluster_uid = _safe_read_text(ds, "cluster_uid")
        cluster_id = _safe_read_numeric(ds, "cluster_id", np.int64)
        source_station_uid = _safe_read_text(ds, "source_station_uid")
        source_station_native_id = _safe_read_text(ds, "source_station_native_id")
        source_station_paths = _safe_read_text(ds, "source_station_paths")

        for flag_var in FLAG_VARS:
            if flag_var not in ds.variables:
                summary[flag_var] = {
                    "unique_values": [],
                    "abnormal_values": [],
                    "abnormal_count": 0,
                }
                continue

            flag_arr = _read_numeric_array(ds.variables[flag_var], np.int16)
            unique_vals = sorted(set(flag_arr.tolist()))
            abnormal_vals = [v for v in unique_vals if v not in allowed_flags and v != -9999]
            abnormal_mask = np.array([(v not in allowed_flags and v != -9999) for v in flag_arr], dtype=bool)
            abnormal_idx = np.flatnonzero(abnormal_mask)

            summary[flag_var] = {
                "unique_values": unique_vals,
                "abnormal_values": abnormal_vals,
                "abnormal_count": int(abnormal_mask.sum()),
            }

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
                ss_paths = source_station_paths[ss_idx] if 0 <= ss_idx < len(source_station_paths) else str(nc_path)

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
                except Exception:
                    dt = ""

                rows.append(
                    {
                        "file_path": str(nc_path),
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

    return rows, summary



def inspect_directory(root_dir: Path, out_csv: Path, summary_csv: Path, allowed_flags: set[int]):
    all_rows = []
    file_counter = 0
    abnormal_file_counter = 0
    summary_rows = []
    abnormal_value_counter = Counter()
    abnormal_by_flag = Counter()
    abnormal_by_file = defaultdict(int)

    for nc_path in sorted(iter_robotham_files(root_dir)):
        file_counter += 1
        rows, file_summary = inspect_one_file(nc_path, allowed_flags)
        all_rows.extend(rows)

        file_has_abnormal = any(v["abnormal_count"] > 0 for v in file_summary.values())
        if file_has_abnormal:
            abnormal_file_counter += 1

        for flag_var, info in file_summary.items():
            summary_rows.append(
                {
                    "file_path": str(nc_path),
                    "flag_var": flag_var,
                    "unique_values": " ".join(map(str, info["unique_values"])),
                    "abnormal_values": " ".join(map(str, info["abnormal_values"])),
                    "abnormal_count": info["abnormal_count"],
                }
            )
            abnormal_by_flag[flag_var] += info["abnormal_count"]
            for v in info["abnormal_values"]:
                abnormal_value_counter[(flag_var, v)] += sum(1 for r in rows if r["flag_var"] == flag_var and r["flag_value"] == v)

        if rows:
            abnormal_by_file[str(nc_path)] += len(rows)

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "file_path",
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
        writer.writerows(all_rows)

    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["file_path", "flag_var", "unique_values", "abnormal_values", "abnormal_count"],
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Scanned Robotham files: {file_counter}")
    print(f"Files with abnormal flags: {abnormal_file_counter}")
    print(f"Abnormal records: {len(all_rows)}")
    print(f"Allowed flags: {sorted(allowed_flags)}")
    print(f"Detailed CSV: {out_csv.resolve()}")
    print(f"Summary CSV:  {summary_csv.resolve()}")
    print()

    if abnormal_by_flag:
        print("Abnormal counts by flag variable:")
        for flag_var in FLAG_VARS:
            print(f"- {flag_var}: {abnormal_by_flag.get(flag_var, 0)}")
        print()

    if abnormal_value_counter:
        print("Abnormal value counts:")
        for (flag_var, flag_value), n in sorted(abnormal_value_counter.items()):
            print(f"- {flag_var}={flag_value}: {n}")
        print()

    preview_n = min(20, len(all_rows))
    if preview_n:
        print(f"Preview first {preview_n} abnormal records:")
        for row in all_rows[:preview_n]:
            print(
                f"- rec={row['record_index']} "
                f"{row['flag_var']}={row['flag_value']} "
                f"cluster={row['cluster_uid']} "
                f"source={row['source']} "
                f"time={row['time']} "
                f"path={row['source_station_paths']}"
            )



def parse_args():
    parser = argparse.ArgumentParser(
        description="Check abnormal quality flags in output_resolution_organized/daily Robotham NetCDF files."
    )
    parser.add_argument(
        "root_dir",
        help="Root directory to scan, e.g. /share/home/dq134/wzx/sed_data/sediment_wzx_1111/output_resolution_organized/daily",
    )
    parser.add_argument(
        "--out-csv",
        default="robotham_abnormal_flag_trace.csv",
        help="CSV path for record-level abnormal rows.",
    )
    parser.add_argument(
        "--summary-csv",
        default="robotham_flag_summary.csv",
        help="CSV path for file-level summary.",
    )
    parser.add_argument(
        "--allow",
        nargs="+",
        type=int,
        default=sorted(DEFAULT_ALLOWED_FLAGS),
        help="Allowed final flag values. Default: 0 1 2 3 9",
    )
    return parser.parse_args()



def main():
    args = parse_args()
    root_dir = Path(args.root_dir)
    if not root_dir.exists():
        raise FileNotFoundError(f"Directory not found: {root_dir}")

    inspect_directory(
        root_dir=root_dir,
        out_csv=Path(args.out_csv),
        summary_csv=Path(args.summary_csv),
        allowed_flags=set(args.allow),
    )


if __name__ == "__main__":
    main()

