#!/usr/bin/env python3
"""
Basin pipeline 路径配置（s1-s7 统一输出到 scripts_basin_test/output/）。

所有路径均为相对于 Output_r 根目录的相对路径字符串，
由各脚本通过 get_output_r_root() 解析为绝对路径。

Optional environment override:
  - OUTPUT_R_ROOT: 指定 Output_r 根目录（跨平台迁移时使用）。
"""

import os
from pathlib import Path

# ── 统一输出目录（相对 Output_r 根目录） ──────────────────────────────────────
PIPELINE_OUTPUT_DIR     = "scripts_basin_test/output"

# ── 标准时间类型目录（s2 输出目录名）───────────────────────────────────────────
# 说明：
#   - 仅保留 daily / monthly / annual / climatology 四类主分辨率；
#   - single_point 并入 daily，quarterly 并入 monthly；
#   - other 仅用于仍无法明确分类的情况。
RESOLUTION_DIRS = (
    "daily",
    "monthly",
    "annual",
    "climatology",
    "other",
)

# ── s1：时间分辨率验证 ────────────────────────────────────────────────────────
S1_VERIFY_CSV           = "scripts_basin_test/output/s1_verify_time_resolution_results.csv"
S1_REVIEW_QUEUE_CSV     = "scripts_basin_test/output/s1_resolution_review_queue.csv"
S1_REVIEW_OVERRIDES_CSV = "scripts_basin_test/output/s1_resolution_review_overrides.csv"

# ── s2：按分辨率重组 ──────────────────────────────────────────────────────────
# 重组目录相对 Output_r 根，位于其上一级（与 Output_r 并列）
S2_ORGANIZED_DIR        = "../output_resolution_organized"
S2_CLASSIFICATION_DETAILS_CSV = "scripts_basin_test/output/s2_resolution_classification_details.csv"
S2_OTHER_SUMMARY_CSV    = "scripts_basin_test/output/s2_other_resolution_summary.csv"
S2_OTHER_DETAILS_CSV    = "scripts_basin_test/output/s2_other_resolution_details.csv"

# ── s3：收集站点元数据 ────────────────────────────────────────────────────────
# path 列存储相对于 output_resolution_organized/ 目录的相对路径（跨平台可移植）
S3_COLLECTED_CSV        = "scripts_basin_test/output/s3_collected_stations.csv"

# ── s4：流域追踪（basin tracer） ──────────────────────────────────────────────
S4_UPSTREAM_CSV         = "scripts_basin_test/output/s4_upstream_basins.csv"
S4_UPSTREAM_GPKG        = "scripts_basin_test/output/s4_upstream_basins.gpkg"
S4_LOCAL_GPKG           = "scripts_basin_test/output/s4_local_catchments.gpkg"  
S4_REPORTED_AREA_CHECK_CSV = "scripts_basin_test/output/s4_reported_area_check.csv"

# ── s5：流域聚类合并 ──────────────────────────────────────────────────────────
S5_BASIN_CLUSTERED_CSV  = "scripts_basin_test/output/s5_basin_clustered_stations.csv"
S5_BASIN_REPORT_CSV     = "scripts_basin_test/output/s5_basin_cluster_report.csv"

# ── s6：时间序列合并输出 ──────────────────────────────────────────────────────
S6_MERGED_NC            = "scripts_basin_test/output/s6_basin_merged_all.nc"
S6_QUALITY_ORDER_CSV    = "scripts_basin_test/output/s6_cluster_quality_order.csv"
S6_MATRIX_DIR           = "scripts_basin_test/output/s6_matrix_by_resolution"
S6_MATRIX_SUMMARY_DIR   = "scripts_basin_test/output/s6_matrix_by_resolution/summary"
S6_CLIMATOLOGY_NC       = "scripts_basin_test/output/s6_climatology_only.nc"
S6_CLIMATOLOGY_SHP      = "scripts_basin_test/output/s6_climatology_stations.shp" 

# ── s7：空间文件导出 ──────────────────────────────────────────────────────────
# Deprecated compatibility path. The cluster summary SHP is no longer generated
# by the s7 mainline; use S7_CLUSTER_POINTS_GPKG instead.
S7_CLUSTER_SHP          = "scripts_basin_test/output/s7_cluster_stations.shp"
S7_CLUSTER_POINTS_GPKG  = "scripts_basin_test/output/s7_cluster_points.gpkg"
S7_CLUSTER_STATION_CATALOG_CSV = "scripts_basin_test/output/s7_cluster_station_catalog.csv"
S7_CLUSTER_RESOLUTION_CATALOG_CSV = "scripts_basin_test/output/s7_cluster_resolution_catalog.csv"
S7_SOURCE_STATIONS_GPKG = "scripts_basin_test/output/s7_source_stations.gpkg"
S7_SOURCE_STATION_RESOLUTION_CATALOG_CSV = "scripts_basin_test/output/s7_source_station_resolution_catalog.csv"
S7_CLUSTER_BASINS_GPKG  = "scripts_basin_test/output/s7_cluster_basins.gpkg"
S7_LOCAL_BASINS_GPKG    = "scripts_basin_test/output/s7_cluster_basins_local.gpkg"

# Deprecated aliases kept for older imports. New standard outputs are GPKG.
S7_SOURCE_STATION_SHP   = S7_SOURCE_STATIONS_GPKG
S7_CLUSTER_BASIN_SHP    = S7_CLUSTER_BASINS_GPKG
S7_LOCAL_BASIN_SHP      = S7_LOCAL_BASINS_GPKG

# ── 发布层：用户交付版参考数据集 ──────────────────────────────────────────────
RELEASE_DATASET_DIR                 = "scripts_basin_test/output/sed_reference_release"
RELEASE_MASTER_NC                   = "scripts_basin_test/output/sed_reference_release/sed_reference_master.nc"
RELEASE_MATRIX_DAILY_NC             = "scripts_basin_test/output/sed_reference_release/sed_reference_timeseries_daily.nc"
RELEASE_MATRIX_MONTHLY_NC           = "scripts_basin_test/output/sed_reference_release/sed_reference_timeseries_monthly.nc"
RELEASE_MATRIX_ANNUAL_NC            = "scripts_basin_test/output/sed_reference_release/sed_reference_timeseries_annual.nc"
RELEASE_CLIMATOLOGY_NC              = "scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc"
RELEASE_STATION_CATALOG_CSV         = "scripts_basin_test/output/sed_reference_release/station_catalog.csv"
RELEASE_SOURCE_STATION_CATALOG_CSV  = "scripts_basin_test/output/sed_reference_release/source_station_catalog.csv"
RELEASE_SOURCE_DATASET_CATALOG_CSV  = "scripts_basin_test/output/sed_reference_release/source_dataset_catalog.csv"
RELEASE_CLUSTER_POINTS_GPKG         = "scripts_basin_test/output/sed_reference_release/sed_reference_cluster_points.gpkg"
RELEASE_SOURCE_STATIONS_GPKG        = "scripts_basin_test/output/sed_reference_release/sed_reference_source_stations.gpkg"
RELEASE_CLUSTER_BASINS_GPKG         = "scripts_basin_test/output/sed_reference_release/sed_reference_cluster_basins.gpkg"
RELEASE_README_MD                   = "scripts_basin_test/output/sed_reference_release/README.md"
RELEASE_VALIDATION_CSV              = "scripts_basin_test/output/sed_reference_release/release_validation_report.csv"
RELEASE_INVENTORY_CSV               = "scripts_basin_test/output/sed_reference_release/release_inventory.csv"

# ── 向后兼容：保留旧的空间聚类主线常量，避免旧脚本 import 失败 ───────────────
S4_CLUSTERED_CSV        = "scripts_basin_test/output/s4_clustered_stations.csv"
S4_REPORT_CSV           = "scripts_basin_test/output/s4_merge_qc_nc_report.csv"
S4_S5_THRESHOLD_DEG     = 0.05
S4_S5_SOURCE_THRESHOLD_OVERRIDE_DEG = {}
S6_OVERLAP_CSV          = "scripts_basin_test/output/s6_overlap_for_manual_choice.csv"
S6_REPORT_CSV           = "scripts_basin_test/output/s6_merge_qc_nc_report.csv"
S7_RESOLVED_CSV         = "scripts_basin_test/output/s7_overlap_resolved.csv"
S8_MERGED_NC            = "scripts_basin_test/output/s8_merged_all.nc"

# ── 向后兼容别名（避免修改引用了旧名称的脚本） ────────────────────────────────
DEFAULT_BASIN_CSV       = S4_UPSTREAM_CSV
S4_BASIN_CLUSTERED_CSV  = S5_BASIN_CLUSTERED_CSV
S4_BASIN_REPORT_CSV     = S5_BASIN_REPORT_CSV


def get_output_r_root(script_dir: Path) -> Path:
    """
    解析 Output_r 根目录。
    优先使用 OUTPUT_R_ROOT 环境变量；否则取脚本所在目录的上一级。
    （scripts_basin_test/ 的上一级即 Output_r/）
    """
    env_root = os.environ.get("OUTPUT_R_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return script_dir.parent.resolve()
