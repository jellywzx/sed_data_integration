#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt


def find_nearest_station(ds, target_lat, target_lon):
    """
    在 nc 文件中寻找离目标经纬度最近的站点
    """
    lats = ds["lat"].values
    lons = ds["lon"].values

    valid = np.isfinite(lats) & np.isfinite(lons)
    if not np.any(valid):
        raise ValueError("文件中没有有效的站点经纬度信息")

    valid_idx = np.where(valid)[0]
    dist2 = (lats[valid] - target_lat) ** 2 + (lons[valid] - target_lon) ** 2
    nearest_local = np.argmin(dist2)
    nearest_idx = valid_idx[nearest_local]

    return int(nearest_idx), float(lats[nearest_idx]), float(lons[nearest_idx]), float(np.sqrt(dist2[nearest_local]))


def get_string_value(ds, var_name, idx, default=""):
    """
    兼容字符串变量读取
    """
    if var_name not in ds:
        return default
    value = ds[var_name].values[idx]
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def read_station_series(nc_path, target_lat, target_lon):
    """
    从单个 nc 文件读取最近站点的 Q/SSC/SSL 时间序列
    """
    nc_path = Path(nc_path)
    if not nc_path.exists():
        raise FileNotFoundError(f"文件不存在: {nc_path}")

    ds = xr.open_dataset(nc_path, decode_times=True)

    station_idx, station_lat, station_lon, dist = find_nearest_station(ds, target_lat, target_lon)

    q = ds["Q"].isel(n_stations=station_idx).values.astype(float)
    ssc = ds["SSC"].isel(n_stations=station_idx).values.astype(float)
    ssl = ds["SSL"].isel(n_stations=station_idx).values.astype(float)
    time_vals = pd.to_datetime(ds["time"].values)

    # 处理 FillValue
    q_fill = ds["Q"].attrs.get("_FillValue", None)
    ssc_fill = ds["SSC"].attrs.get("_FillValue", None)
    ssl_fill = ds["SSL"].attrs.get("_FillValue", None)

    if q_fill is not None:
        q = np.where(q == q_fill, np.nan, q)
    if ssc_fill is not None:
        ssc = np.where(ssc == ssc_fill, np.nan, ssc)
    if ssl_fill is not None:
        ssl = np.where(ssl == ssl_fill, np.nan, ssl)

    df = pd.DataFrame({
        "time": time_vals,
        "Q": q,
        "SSC": ssc,
        "SSL": ssl,
    })

    # 去掉三项都为空的行
    df = df.dropna(subset=["Q", "SSC", "SSL"], how="all").reset_index(drop=True)

    station_info = {
        "station_idx": station_idx,
        "cluster_id": int(ds["cluster_id"].values[station_idx]) if "cluster_id" in ds else None,
        "cluster_uid": get_string_value(ds, "cluster_uid", station_idx),
        "station_name": get_string_value(ds, "station_name", station_idx),
        "river_name": get_string_value(ds, "river_name", station_idx),
        "source_station_id": get_string_value(ds, "source_station_id", station_idx),
        "lat": station_lat,
        "lon": station_lon,
        "distance_in_degree": dist,
        "time_type": ds.attrs.get("time_type", "")
    }

    ds.close()
    return df, station_info


def plot_single_variable(df, var_name, ylabel, title, out_png):
    """
    单独输出一个变量的时间序列图
    """
    plt.figure(figsize=(12, 4.8))
    plt.plot(df["time"], df[var_name], linewidth=1.0)
    plt.xlabel("Time")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close()


def plot_three_variables(df, title, out_png):
    """
    输出 Q / SSC / SSL 三联图
    """
    fig, axes = plt.subplots(3, 1, figsize=(12, 9), sharex=True)

    axes[0].plot(df["time"], df["Q"], linewidth=1.0)
    axes[0].set_ylabel("Q (m3/s)")
    axes[0].set_title(title)

    axes[1].plot(df["time"], df["SSC"], linewidth=1.0)
    axes[1].set_ylabel("SSC (mg/L)")

    axes[2].plot(df["time"], df["SSL"], linewidth=1.0)
    axes[2].set_ylabel("SSL (ton/day)")
    axes[2].set_xlabel("Time")

    plt.tight_layout()
    plt.savefig(out_png, dpi=300, bbox_inches="tight")
    plt.close()


def process_one_nc(nc_path, target_lat, target_lon, out_dir):
    """
    处理一个 nc 文件，输出 csv 和 png
    """
    nc_path = Path(nc_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df, info = read_station_series(nc_path, target_lat, target_lon)

    res_name = info["time_type"] if info["time_type"] else nc_path.stem

    station_label = (
        f"cluster_id={info['cluster_id']}, "
        f"station={info['station_name']}, "
        f"river={info['river_name']}, "
        f"lat={info['lat']:.4f}, lon={info['lon']:.4f}"
    )

    # 保存 csv
    csv_path = out_dir / f"{res_name}_timeseries.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    # 保存三联图
    combined_png = out_dir / f"{res_name}_Q_SSC_SSL_timeseries.png"
    plot_three_variables(
        df,
        title=f"{res_name} | {station_label}",
        out_png=combined_png
    )

    # 分别保存单图
    q_png = out_dir / f"{res_name}_Q.png"
    ssc_png = out_dir / f"{res_name}_SSC.png"
    ssl_png = out_dir / f"{res_name}_SSL.png"

    plot_single_variable(
        df, "Q", "Q (m3/s)",
        f"{res_name} - Q | {station_label}",
        q_png
    )
    plot_single_variable(
        df, "SSC", "SSC (mg/L)",
        f"{res_name} - SSC | {station_label}",
        ssc_png
    )
    plot_single_variable(
        df, "SSL", "SSL (ton/day)",
        f"{res_name} - SSL | {station_label}",
        ssl_png
    )

    print("\n==============================")
    print(f"处理完成: {nc_path.name}")
    print(f"最近站点 index: {info['station_idx']}")
    print(f"cluster_id: {info['cluster_id']}")
    print(f"station_name: {info['station_name']}")
    print(f"river_name: {info['river_name']}")
    print(f"站点坐标: ({info['lat']}, {info['lon']})")
    print(f"目标点距离(度): {info['distance_in_degree']}")
    print(f"CSV: {csv_path}")
    print(f"三联图: {combined_png}")
    print(f"Q图: {q_png}")
    print(f"SSC图: {ssc_png}")
    print(f"SSL图: {ssl_png}")

    return df, info


def process_all_resolutions(daily_nc, monthly_nc, annual_nc, target_lat, target_lon, out_root):
    """
    处理 daily / monthly / annual 三个分辨率
    """
    files = {
        "daily": daily_nc,
        "monthly": monthly_nc,
        "annual": annual_nc,
    }

    results = {}
    out_root = Path(out_root)
    out_root.mkdir(parents=True, exist_ok=True)

    for key, file_path in files.items():
        if file_path is None:
            continue
        if not Path(file_path).exists():
            print(f"跳过 {key}，文件不存在: {file_path}")
            continue

        subdir = out_root / key
        df, info = process_one_nc(
            nc_path=file_path,
            target_lat=target_lat,
            target_lon=target_lon,
            out_dir=subdir
        )
        results[key] = {"data": df, "info": info}

    return results


if __name__ == "__main__":
    # =========================
    # 这里修改为你的文件路径和目标经纬度
    # =========================
    daily_nc = r"./output/s6_matrix_by_resolution/s6_basin_matrix_daily.nc"
    monthly_nc = r"./output/s6_matrix_by_resolution/s6_basin_matrix_monthly.nc"
    annual_nc = r"./output/s6_matrix_by_resolution/s6_basin_matrix_annual.nc"

    target_lat = 1.8214
    target_lon = -61.1236
    out_root = r"./plots"

    process_all_resolutions(
        daily_nc=daily_nc,
        monthly_nc=monthly_nc,
        annual_nc=annual_nc,
        target_lat=target_lat,
        target_lon=target_lon,
        out_root=out_root
    )
