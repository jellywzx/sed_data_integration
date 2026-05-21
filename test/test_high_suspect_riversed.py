import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 80)

path = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_qc_flags.csv"
df = pd.read_csv(path)

sus = df[df["qc_level"] == "high_suspect"].copy()

cols = [
    "cluster_id",
    "date",
    "USGS_SSC",
    "RiverSed_SSC",
    "diff_b_minus_a",
    "abs_diff",
    "pct_error",
    "ratio_RiverSed_to_USGS",
    "robust_log_ratio_z",
    "source_station_uid_a",
    "source_station_uid_b",
]

sus = sus[cols].sort_values("pct_error", ascending=False)

print(sus.to_string(index=False))

out = "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/early_validation_results/usgs_riversed_daily_ssc_high_suspect_only.csv"
sus.to_csv(out, index=False)
print("\nWrote:", out)