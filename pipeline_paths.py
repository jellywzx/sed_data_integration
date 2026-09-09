#!/usr/bin/env python3
"""Central path configuration for the basin pipeline outputs."""

import os
from pathlib import Path

PIPELINE_OUTPUT_DIR     = "scripts_basin_test/output"
OUTPUT_LOG_DIR          = "scripts_basin_test/output/logs"

# Note:
RESOLUTION_DIRS = (
    "daily",
    "monthly",
    "annual",
    "climatology",
    "other",
)

S1_VERIFY_CSV           = "scripts_basin_test/output/s1_verify_time_resolution_results.csv"
S1_REVIEW_QUEUE_CSV     = "scripts_basin_test/output/s1_resolution_review_queue.csv"
S1_REVIEW_OVERRIDES_CSV = "scripts_basin_test/output/s1_resolution_review_overrides.csv"

S2_ORGANIZED_DIR        = "../output_resolution_organized"
S2_CLASSIFICATION_DETAILS_CSV = "scripts_basin_test/output/s2_resolution_classification_details.csv"
S2_OTHER_SUMMARY_CSV    = "scripts_basin_test/output/s2_other_resolution_summary.csv"
S2_OTHER_DETAILS_CSV    = "scripts_basin_test/output/s2_other_resolution_details.csv"

S3_COLLECTED_CSV        = "scripts_basin_test/output/s3_collected_stations.csv"

S4_UPSTREAM_CSV         = "scripts_basin_test/output/s4_upstream_basins.csv"
S4_UPSTREAM_GPKG        = "scripts_basin_test/output/s4_upstream_basins.gpkg"
S4_LOCAL_GPKG           = "scripts_basin_test/output/s4_local_catchments.gpkg"  
S4_REPORTED_AREA_CHECK_CSV = "scripts_basin_test/output/s4_reported_area_check.csv"

S5_BASIN_CLUSTERED_CSV  = "scripts_basin_test/output/s5_basin_clustered_stations.csv"
S5_BASIN_REPORT_CSV     = "scripts_basin_test/output/s5_basin_cluster_report.csv"
S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV = "scripts_basin_test/output/s5b_satellite_main_cluster_links_v2.csv"
S5B_SATELLITE_MAIN_CLUSTER_CANDIDATES_CSV = "scripts_basin_test/output/s5b_satellite_main_cluster_candidates_v2.csv"
S5B_SATELLITE_MAIN_CLUSTER_REPORT_CSV = "scripts_basin_test/output/s5b_satellite_main_cluster_report_v2.csv"

S6_MERGED_NC            = "scripts_basin_test/output/s6_basin_merged_all.nc"
S6_QUALITY_ORDER_CSV    = "scripts_basin_test/output/s6_cluster_quality_order.csv"
S6_MATRIX_DIR           = "scripts_basin_test/output/s6_matrix_by_resolution"
S6_MATRIX_SUMMARY_DIR   = "scripts_basin_test/output/s6_matrix_by_resolution/summary"
S6_CLIMATOLOGY_NC       = "scripts_basin_test/output/s6_climatology_only.nc"
S6_CLIMATOLOGY_SHP      = "scripts_basin_test/output/s6_climatology_stations.shp" 
S6_SATELLITE_VALIDATION_NC = "scripts_basin_test/output/s6_satellite_validation_only.nc"
S6_SATELLITE_VALIDATION_CATALOG_CSV = "scripts_basin_test/output/s6_satellite_validation_catalog.csv"

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

# Reference data
RELEASE_DATASET_DIR                 = "scripts_basin_test/output/sed_reference_release"
RELEASE_MASTER_NC                   = "scripts_basin_test/output/sed_reference_release/sed_reference_master.nc"
RELEASE_MATRIX_DAILY_NC             = "scripts_basin_test/output/sed_reference_release/sed_reference_timeseries_daily.nc"
RELEASE_MATRIX_MONTHLY_NC           = "scripts_basin_test/output/sed_reference_release/sed_reference_timeseries_monthly.nc"
RELEASE_MATRIX_ANNUAL_NC            = "scripts_basin_test/output/sed_reference_release/sed_reference_timeseries_annual.nc"
RELEASE_CLIMATOLOGY_NC              = "scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc"
RELEASE_STATION_CATALOG_CSV         = "scripts_basin_test/output/sed_reference_release/station_catalog.csv"
RELEASE_SOURCE_STATION_CATALOG_CSV  = "scripts_basin_test/output/sed_reference_release/source_station_catalog.csv"
RELEASE_SOURCE_DATASET_CATALOG_CSV  = "scripts_basin_test/output/sed_reference_release/source_dataset_catalog.csv"
RELEASE_OVERLAP_CANDIDATES_CSV      = "scripts_basin_test/output/sed_reference_release/sed_reference_overlap_candidates.csv.gz"
RELEASE_SATELLITE_NC                = "scripts_basin_test/output/sed_reference_release/sed_reference_satellite.nc"
RELEASE_SATELLITE_CATALOG_CSV       = "scripts_basin_test/output/sed_reference_release/satellite_catalog.csv"
# Deprecated compatibility aliases. Use RELEASE_SATELLITE_NC and
# RELEASE_SATELLITE_CATALOG_CSV for new release code.
RELEASE_SATELLITE_VALIDATION_NC     = RELEASE_SATELLITE_NC
RELEASE_SATELLITE_VALIDATION_CATALOG_CSV = RELEASE_SATELLITE_CATALOG_CSV
RELEASE_CLUSTER_POINTS_GPKG         = "scripts_basin_test/output/sed_reference_release/sed_reference_cluster_points.gpkg"
RELEASE_SOURCE_STATIONS_GPKG        = "scripts_basin_test/output/sed_reference_release/sed_reference_source_stations.gpkg"
RELEASE_CLUSTER_BASINS_GPKG         = "scripts_basin_test/output/sed_reference_release/sed_reference_cluster_basins.gpkg"
RELEASE_README_MD                   = "scripts_basin_test/output/sed_reference_release/README.md"
RELEASE_VALIDATION_CSV              = "scripts_basin_test/output/sed_reference_release/release_validation_report.csv"
RELEASE_INVENTORY_CSV               = "scripts_basin_test/output/sed_reference_release/release_inventory.csv"

S4_CLUSTERED_CSV        = "scripts_basin_test/output/s4_clustered_stations.csv"
S4_REPORT_CSV           = "scripts_basin_test/output/s4_merge_qc_nc_report.csv"
S4_S5_THRESHOLD_DEG     = 0.05
S4_S5_SOURCE_THRESHOLD_OVERRIDE_DEG = {}
S6_OVERLAP_CSV          = "scripts_basin_test/output/s6_overlap_for_manual_choice.csv"
S6_REPORT_CSV           = "scripts_basin_test/output/s6_merge_qc_nc_report.csv"
S7_RESOLVED_CSV         = "scripts_basin_test/output/s7_overlap_resolved.csv"
S8_MERGED_NC            = "scripts_basin_test/output/s8_merged_all.nc"

DEFAULT_BASIN_CSV       = S4_UPSTREAM_CSV
S4_BASIN_CLUSTERED_CSV  = S5_BASIN_CLUSTERED_CSV
S4_BASIN_REPORT_CSV     = S5_BASIN_REPORT_CSV


def get_output_r_root(script_dir: Path) -> Path:
    """Resolve the Output_r root directory from OUTPUT_R_ROOT or the script location."""
    env_root = os.environ.get("OUTPUT_R_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return script_dir.parent.resolve()


def get_log_path(script_dir: Path, log_name: str) -> Path:
    """Return an output/logs path for a script log, creating the directory."""
    log_dir = get_output_r_root(script_dir) / OUTPUT_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / log_name
