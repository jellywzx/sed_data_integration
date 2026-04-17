import pandas as pd
from basin_tracer import UpstreamBasinTracer

# ========= 配置 =========
MERIT_DIR = "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1"

# 几个测试点（建议选你熟悉的河流附近）
test_points = [
    # (lon, lat, optional_reported_area)
    (116.4, 39.9, None),      # 北京附近
    (121.5, 31.2, None),      # 上海附近
    (114.3, 30.6, None),      # 武汉附近（长江）
    (120.0, 30.0, 50000),     # 带 reported_area
]

# ========= 初始化 =========
tracer = UpstreamBasinTracer(MERIT_DIR)

results = []

for i, (lon, lat, area) in enumerate(test_points):
    print(f"\n===== 测试点 {i} =====")
    print(f"lon={lon}, lat={lat}, reported_area={area}")

    result = tracer.get_upstream_basin(lon, lat, reported_area=area)

    print("basin_id:", result["basin_id"])
    print("match_quality:", result["match_quality"])
    print("distance_m:", result["distance"])
    print("basin_area:", result["basin_area"])
    print("n_upstream_reaches:", result["n_upstream_reaches"])
    print("method:", result["method"])

    # ===== 关键检查 =====
    if result["distance"] < 1:
        print("⚠️ 警告：distance 太小，可能仍然是度")
    if result["distance"] > 200000:
        print("⚠️ 警告：distance 太大，可能匹配错河")

    results.append({
        "lon": lon,
        "lat": lat,
        "distance_m": result["distance"],
        "basin_id": result["basin_id"],
        "match_quality": result["match_quality"],
        "basin_area": result["basin_area"],
        "n_reaches": result["n_upstream_reaches"],
    })

# ========= 汇总输出 =========
df = pd.DataFrame(results)
print("\n=== 汇总结果 ===")
print(df)

# 可选：保存
df.to_csv("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/test_distance_results.csv", index=False)
print("\n已保存 test_distance_results.csv")
