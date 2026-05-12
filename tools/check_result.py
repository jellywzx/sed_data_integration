import numpy as np
import netCDF4 as nc4
import pandas as pd

path = "output/s6_basin_merged_all.nc"

with nc4.Dataset(path, "r") as ds:
    n_sta = ds.dimensions["n_stations"].size
    n_rec = ds.dimensions["n_records"].size
    n_src = ds.dimensions["n_sources"].size

    lat  = ds["lat"][:]
    lon  = ds["lon"][:]
    cid  = ds["cluster_id"][:]
    time = ds["time"][:]
    res  = ds["resolution"][:]
    q    = ds["Q"][:]
    ssc  = ds["SSC"][:]
    ssl  = ds["SSL"][:]
    qf   = ds["Q_flag"][:]
    si   = ds["station_index"][:]

    print(f"=== 维度 ===")
    print(f"  n_stations : {n_sta:,}")
    print(f"  n_records  : {n_rec:,}")
    print(f"  n_sources  : {n_src}")

    # ── 经纬度范围
    print(f"\n=== 经纬度 ===")
    print(f"  lat: [{lat.min():.2f}, {lat.max():.2f}]  NaN: {np.sum(np.isnan(lat))}")
    print(f"  lon: [{lon.min():.2f}, {lon.max():.2f}]  NaN: {np.sum(np.isnan(lon))}")

    # ── 时间范围
    dates = pd.to_datetime(time, unit="D", origin="1970-01-01")
    print(f"\n=== 时间范围 ===")
    print(f"  {dates.min().date()} ~ {dates.max().date()}")

    # ── 分辨率分布
    print(f"\n=== 分辨率分布 ===")
    for code, name in [(0,"daily"),(1,"monthly"),(2,"annually_clim"),(3,"other")]:
        print(f"  {name:20s}: {(res==code).sum():>10,} records")

    # ── 数据填充率（非 fill 比例）
    FILL = -9999.0
    q_valid   = np.sum((q != FILL) & ~np.ma.is_masked(q))
    ssc_valid = np.sum((ssc != FILL) & ~np.ma.is_masked(ssc))
    ssl_valid = np.sum((ssl != FILL) & ~np.ma.is_masked(ssl))
    print(f"\n=== 有效记录数（非 fill）===")
    print(f"  Q  : {q_valid:>10,} / {n_rec:,}  ({q_valid/n_rec:.1%})")
    print(f"  SSC: {ssc_valid:>10,} / {n_rec:,}  ({ssc_valid/n_rec:.1%})")
    print(f"  SSL: {ssl_valid:>10,} / {n_rec:,}  ({ssl_valid/n_rec:.1%})")

    # ── 异常值检查（Q/SSC/SSL 不应为负）
    q_neg   = np.sum((q   != FILL) & (q   < 0) & ~np.ma.is_masked(q))
    ssc_neg = np.sum((ssc != FILL) & (ssc < 0) & ~np.ma.is_masked(ssc))
    ssl_neg = np.sum((ssl != FILL) & (ssl < 0) & ~np.ma.is_masked(ssl))
    print(f"\n=== 负值异常 ===")
    print(f"  Q<0: {q_neg}   SSC<0: {ssc_neg}   SSL<0: {ssl_neg}")

    # ── 每站点覆盖记录数分布
    counts = np.bincount(si, minlength=n_sta)
    print(f"\n=== 每站点记录数 ===")
    print(f"  min={counts.min()}  median={np.median(counts):.0f}  "
        f"max={counts.max()}  空站: {(counts==0).sum()}")

    # ── quality flag 分布
    print(f"\n=== Q_flag 分布 ===")
    for v, name in [(0,"good"),(1,"estimated"),(2,"suspect"),(3,"bad"),(9,"missing")]:
        print(f"  {v} {name:12s}: {(qf==v).sum():>10,}")

    # ── source 数据集列表
    src_names = [str(s) for s in ds["source_name"][:]]
    print(f"\n=== 数据源 ({n_src}) ===")
    for s in src_names:
        print(f"  {s}")

# 3. 与输入 CSV 对比（交叉验证）

import pandas as pd
import netCDF4 as nc4

s5 = pd.read_csv("output/s5_basin_clustered_stations.csv")

with nc4.Dataset("output/s6_basin_merged_all.nc") as ds:
    nc_clusters = set(ds["cluster_id"][:].tolist())

csv_clusters = set(s5["cluster_id"].unique().tolist())

print(f"CSV clusters : {len(csv_clusters):,}")
print(f"NC  clusters : {len(nc_clusters):,}")
print(f"在CSV中但不在NC: {csv_clusters - nc_clusters}")   # 应为空
print(f"在NC中但不在CSV: {nc_clusters - csv_clusters}")   # 应为空
