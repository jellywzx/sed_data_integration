from pathlib import Path
import numpy as np
import netCDF4 as nc


def check_flag_value_8(nc_path, flag_vars=("Q_flag", "SSC_flag", "SSL_flag"), max_examples=10):
    nc_path = Path(nc_path)
    if not nc_path.is_file():
        raise FileNotFoundError(f"File not found: {nc_path}")

    results = {}

    with nc.Dataset(nc_path, "r") as ds:
        print(f"Checking file: {nc_path}")
        print()

        for var_name in flag_vars:
            if var_name not in ds.variables:
                results[var_name] = {
                    "exists": False,
                    "has_8": False,
                    "count_8": 0,
                    "unique_values": [],
                    "example_indices": [],
                }
                print(f"[{var_name}] MISSING")
                continue

            var = ds.variables[var_name]
            data = np.ma.asarray(var[:])

            # 先提升类型，避免 int8 的 filled 溢出问题
            if np.ma.isMaskedArray(data):
                arr = data.astype(np.int16).filled(-9999)
            else:
                arr = np.asarray(data, dtype=np.int16)

            unique_values = np.unique(arr)
            has_8 = np.any(arr == 8)
            count_8 = int(np.count_nonzero(arr == 8))

            example_indices = []
            if has_8:
                idx = np.argwhere(arr == 8)
                example_indices = [tuple(map(int, x)) for x in idx[:max_examples]]

            results[var_name] = {
                "exists": True,
                "has_8": bool(has_8),
                "count_8": count_8,
                "unique_values": unique_values.tolist(),
                "example_indices": example_indices,
            }

            print(f"[{var_name}]")
            print("  exists: True")
            print(f"  dtype_in_nc: {var.dtype}")
            print(f"  has_8: {has_8}")
            print(f"  count_8: {count_8}")
            print(f"  unique_values: {unique_values.tolist()}")
            if has_8:
                print(f"  first_{max_examples}_indices_with_8: {example_indices}")
            print()

    return results


if __name__ == "__main__":
    nc_file = "s6_basin_merged_all.nc"
    results = check_flag_value_8(nc_file)

    any_8 = any(v["has_8"] for v in results.values() if v["exists"])
    print("=" * 60)
    if any_8:
        print("Found value 8 in at least one final flag variable.")
    else:
        print("No value 8 found in final flag variables.")
