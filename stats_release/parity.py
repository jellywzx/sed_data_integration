#!/usr/bin/env python3
"""Legacy stats output parity manifest for release-only stats."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


def _targets_for(module: str, legacy_script: str, outputs: Iterable[str]) -> List[Dict[str, object]]:
    rows = []
    for rel in outputs:
        rows.append(
            {
                "module": module,
                "legacy_script": legacy_script,
                "legacy_output": rel,
                "new_output": "{}/{}".format(module, rel),
                "release_only_capable": 1,
                "unsupported_reason": "",
            }
        )
    return rows


LEGACY_TARGETS: List[Dict[str, object]] = []

LEGACY_TARGETS += _targets_for(
    "inventory",
    "stats/stats_release_inventory.py",
    [
        "release_inventory_stats_files.csv",
        "release_inventory_stats_summary.csv",
        "release_inventory_stats_summary_wide.csv",
        "release_inventory_stats_netcdf_schema.csv",
        "release_inventory_stats_gpkg_layers.csv",
        "release_inventory_stats_article_metrics.csv",
        "release_inventory_stats_summary.md",
    ],
)

LEGACY_TARGETS += _targets_for(
    "source_dataset_layers",
    "stats/count_main_side_source_datasets.py",
    [
        "source_dataset_layer_membership.csv",
        "source_dataset_layer_summary.csv",
        "source_dataset_layer_report.md",
    ],
)

LEGACY_TARGETS += _targets_for(
    "spatial",
    "stats/spatial_coverage_stats.py",
    [
        "article_spatial_coverage_summary.md",
        "figures/fig_climatology_spatial_coverage.png",
        "figures/fig_climatology_spatial_coverage.pdf",
        "figures/fig_climatology_vs_timeseries_coverage.png",
        "figures/fig_climatology_vs_timeseries_coverage.pdf",
        "figures/fig_composite_spatial_coverage.png",
        "figures/fig_composite_spatial_coverage.pdf",
        "figures/fig_global_bubble_map.png",
        "figures/fig_global_cluster_distribution.png",
        "figures/fig_global_cluster_distribution.pdf",
        "figures/fig_global_cluster_status_and_basins.png",
        "figures/fig_global_cluster_status_and_basins.pdf",
        "figures/fig_main_vs_satellite_spatial_coverage.png",
        "figures/fig_main_vs_satellite_spatial_coverage.pdf",
        "figures/fig_satellite_upstream_area_distribution.png",
        "figures/fig_satellite_upstream_area_distribution.pdf",
        "figures/fig_satellite_validation_spatial_distribution.png",
        "figures/fig_satellite_validation_spatial_distribution.pdf",
        "figures/fig_source_spatial_contribution.png",
        "figures/fig_source_spatial_contribution.pdf",
        "figures/fig_spatial_coverage_by_region.png",
        "figures/fig_spatial_coverage_by_region.pdf",
        "figures/fig_spatial_coverage_by_region_country.png",
        "figures/fig_spatial_coverage_by_region_country.pdf",
        "figures/fig_spatial_coverage_by_region_resolution.png",
        "figures/fig_spatial_coverage_by_region_resolution.pdf",
        "figures/fig_spatial_coverage_by_region_source_clusters.png",
        "figures/fig_spatial_coverage_by_region_source_clusters.pdf",
        "figures/fig_spatial_coverage_by_region_source_records.png",
        "figures/fig_spatial_coverage_by_region_source_records.pdf",
        "figures/fig_spatial_coverage_by_resolution.png",
        "figures/fig_spatial_coverage_by_resolution.pdf",
        "figures/fig_timeseries_spatial_coverage.png",
        "figures/fig_timeseries_spatial_coverage.pdf",
        "figures/fig_upstream_area_distribution.png",
        "figures/fig_upstream_area_distribution.pdf",
        "figures/fig_upstream_area_histogram.png",
        "figures/global_cluster_distribution_points.geojson",
        "tables/article_spatial_coverage_metrics.csv",
        "tables/table_basin_polygon_layers.csv",
        "tables/table_cluster_spatial_attributes.csv",
        "tables/table_satellite_upstream_area_distribution_s4.csv",
        "tables/table_satellite_validation_spatial_coverage.csv",
        "tables/table_spatial_coverage_by_country.csv",
        "tables/table_spatial_coverage_by_region.csv",
        "tables/table_spatial_coverage_by_region_resolution.csv",
        "tables/table_spatial_coverage_by_region_source.csv",
        "tables/table_spatial_coverage_by_resolution.csv",
        "tables/table_spatial_coverage_by_source.csv",
        "tables/table_spatial_coverage_by_source_type.csv",
        "tables/table_spatial_coverage_summary.csv",
        "tables/table_unknown_country_region_clusters.csv",
        "tables/table_upstream_area_distribution.csv",
    ],
)

LEGACY_TARGETS += _targets_for(
    "basin_diagnostics",
    "stats/explain_s8_spatial_matching_error.py",
    [
        "figures/basin_flag_counts.png",
        "figures/basin_status_by_reported_area_presence.png",
        "figures/distance_hist_logx.png",
        "figures/reported_area_presence_counts.png",
        "figures/spatial_error_class_counts.png",
        "figures/threshold_sensitivity.png",
        "figures/unknown_points_map.png",
        "manual_review_area_mismatch.csv",
        "manual_review_geometry_inconsistent.csv",
        "manual_review_high_risk.csv",
        "manual_review_top_large_offsets.csv",
        "remote_sensing_exclusion_summary.csv",
        "remote_sensing_exclusion_summary.txt",
        "reported_area_area_error_bin_quality_counts.csv",
        "reported_area_match_flag_counts.csv",
        "reported_area_match_quality_counts.csv",
        "reported_area_match_status_counts.csv",
        "reported_area_match_status_quality_counts.csv",
        "reported_area_spatial_error_class_counts.csv",
        "reported_area_spatial_match_rows.csv",
        "spatial_match_area_error_bins.csv",
        "spatial_match_distance_bins.csv",
        "spatial_match_error_class_counts.csv",
        "spatial_match_error_detailed_report.md",
        "spatial_match_error_summary.txt",
        "spatial_match_error_summary_essd.md",
        "spatial_match_error_table.csv",
        "spatial_match_flag_by_resolution.csv",
        "spatial_match_flag_by_source.csv",
        "spatial_match_flag_counts.csv",
        "spatial_match_quality_counts.csv",
        "spatial_match_status_by_reported_area_presence.csv",
        "spatial_match_status_by_resolution.csv",
        "spatial_match_status_by_source.csv",
        "spatial_match_status_counts.csv",
        "spatial_match_threshold_sensitivity.csv",
        "unknown_stations.csv",
    ],
)

LEGACY_TARGETS += _targets_for(
    "temporal",
    "stats/temporal_coverage_stats.py",
    [
        "article_temporal_coverage_report.md",
        "article_temporal_coverage_summary.md",
        "figures/fig_active_clusters_by_year.png",
        "figures/fig_active_clusters_by_year.pdf",
        "figures/fig_active_units_by_year.png",
        "figures/fig_active_units_by_year.pdf",
        "figures/fig_climatology_record_length_distribution.png",
        "figures/fig_climatology_record_length_distribution.pdf",
        "figures/fig_climatology_source_contribution.png",
        "figures/fig_climatology_source_contribution.pdf",
        "figures/fig_climatology_variable_coverage.png",
        "figures/fig_climatology_variable_coverage.pdf",
        "figures/fig_long_record_counts.png",
        "figures/fig_long_record_counts.pdf",
        "figures/fig_record_length_distribution.png",
        "figures/fig_record_length_distribution.pdf",
        "figures/fig_record_length_histogram.png",
        "figures/fig_record_length_histogram.pdf",
        "figures/fig_records_by_year_variable.png",
        "figures/fig_records_by_year_variable.pdf",
        "figures/fig_satellite_active_units_by_year.png",
        "figures/fig_satellite_active_units_by_year.pdf",
        "figures/fig_satellite_record_length_distribution.png",
        "figures/fig_satellite_record_length_distribution.pdf",
        "figures/fig_satellite_records_by_year_variable.png",
        "figures/fig_satellite_records_by_year_variable.pdf",
        "figures/fig_satellite_source_contribution.png",
        "figures/fig_satellite_source_contribution.pdf",
        "figures/fig_satellite_temporal_heatmap.png",
        "figures/fig_satellite_temporal_heatmap.pdf",
        "figures/fig_source_temporal_span.png",
        "figures/fig_source_temporal_span.pdf",
        "figures/fig_temporal_coverage_heatmap.png",
        "figures/fig_temporal_coverage_heatmap.pdf",
        "tables/table_active_clusters_by_year.csv",
        "tables/table_active_units_by_year.csv",
        "tables/table_climatology_by_source.csv",
        "tables/table_climatology_record_lengths_by_station.csv",
        "tables/table_climatology_temporal_summary.csv",
        "tables/table_long_records_by_resolution.csv",
        "tables/table_record_length_distribution.csv",
        "tables/table_satellite_by_linked_cluster.csv",
        "tables/table_satellite_by_source.csv",
        "tables/table_satellite_by_year.csv",
        "tables/table_satellite_record_lengths_by_station.csv",
        "tables/table_satellite_temporal_summary.csv",
        "tables/table_temporal_coverage_by_region_resolution.csv",
        "tables/table_temporal_coverage_by_resolution.csv",
        "tables/table_temporal_coverage_by_source.csv",
        "tables/table_temporal_coverage_by_variable.csv",
        "tables/table_temporal_coverage_record_lengths_by_unit.csv",
    ],
)

LEGACY_TARGETS += _targets_for(
    "variable_summary",
    "stats/variable_coverage_and_summary_stats.py",
    [
        "figures/fig_Q_distribution.png",
        "figures/fig_SSC_distribution.png",
        "figures/fig_SSL_distribution.png",
        "tables/table_colocated_variable_coverage.csv",
        "tables/table_colocated_variable_coverage_analysis_grade.csv",
        "tables/table_extreme_value_review_points.csv",
        "tables/table_variable_coverage_by_resolution.csv",
        "tables/table_variable_coverage_by_resolution_analysis_grade.csv",
        "tables/table_variable_summary_statistics.csv",
        "tables/table_variable_summary_statistics_analysis_grade.csv",
        "variable_coverage_results_report_ESSD.md",
    ],
)

LEGACY_TARGETS += _targets_for(
    "source_contribution",
    "stats/stats_source_contribution.py",
    [
        "figures/fig_climatology_contribution_clusters.png",
        "figures/fig_climatology_contribution_records.png",
        "figures/fig_climatology_contribution_stations.png",
        "figures/fig_climatology_resolution_stacked.png",
        "figures/fig_climatology_temporal_coverage.png",
        "figures/fig_climatology_variable_stacked.png",
        "figures/fig_satellite_contribution_clusters.png",
        "figures/fig_satellite_contribution_records.png",
        "figures/fig_satellite_contribution_stations.png",
        "figures/fig_satellite_resolution_stacked.png",
        "figures/fig_satellite_temporal_coverage.png",
        "figures/fig_satellite_variable_stacked.png",
        "figures/fig_source_contribution_clusters.png",
        "figures/fig_source_contribution_records.png",
        "figures/fig_source_contribution_stations.png",
        "figures/fig_source_cumulative_contribution.png",
        "figures/fig_source_group_records.png",
        "figures/fig_source_resolution_stacked.png",
        "figures/fig_source_temporal_coverage.png",
        "figures/fig_source_type_records.png",
        "figures/fig_source_variable_stacked.png",
        "reports/source_contribution_report.md",
        "tables/source_classification_template.csv",
        "tables/table_report_key_metrics.csv",
        "tables/table_source_contribution_cumulative.csv",
        "tables/table_source_dataset_contribution.csv",
        "tables/table_source_resolution_contribution.csv",
        "tables/table_source_temporal_coverage.csv",
        "tables/table_source_type_contribution.csv",
        "tables/table_source_variable_contribution.csv",
        "tables/table_top_source_contributors.csv",
    ],
)

qc_files = [
    "article_qc_flag_report.md",
    "figures/fig_qc_flag_by_source_type.png",
    "figures/fig_qc_flag_distribution.png",
    "figures/fig_qc_health_by_resolution.png",
    "figures/fig_qc_missing_trends.png",
    "figures/fig_qc_stage_summary.png",
    "figures/fig_qc_top_problem_clusters.png",
    "figures/fig_qc_top_problem_sources.png",
    "figures/fig_qc_yearly_problem_trends.png",
    "tables/table_qc_flag_by_cluster.csv",
    "tables/table_qc_flag_by_resolution.csv",
    "tables/table_qc_flag_by_source.csv",
    "tables/table_qc_flag_by_variable.csv",
    "tables/table_qc_flag_by_year.csv",
    "tables/table_qc_flag_problem_clusters.csv",
    "tables/table_qc_flag_summary.csv",
    "tables/table_qc_health_kpis.csv",
    "tables/table_qc_issue_hotspots.csv",
    "tables/table_qc_stage_effectiveness.csv",
    "tables/table_qc_yearly_trends.csv",
]
LEGACY_TARGETS += _targets_for("qc_flags", "stats/qc_flag_statistics.py", qc_files)
for product in ("climatology", "satellite"):
    LEGACY_TARGETS += _targets_for(
        "qc_flags",
        "stats/qc_flag_statistics.py",
        [path.replace("figures/", f"figures/{product}/").replace("tables/", f"tables/{product}/").replace("article_qc_flag_report.md", f"reports/{product}/article_qc_flag_report.md") for path in qc_files],
    )

for rel in [
    "s4_spatial_match_error_include_satellite/s4_include_satellite_overview.csv",
    "s4_spatial_match_error_include_satellite/s4_upstream_basins/s4_upstream_basins_main_rows_include_satellite.csv",
    "s4_spatial_match_error/s4_upstream_basins/s4_upstream_basins_all_rows.csv",
    "count_main_side_source/mainline_s3_collected_stations",
    "count_main_side_source/mainline_s5_clustered_stations",
    "count_main_side_source/mainline_s6_quality_order_candidates",
    "count_main_side_source/mainline_s7_source_station_catalog",
]:
    LEGACY_TARGETS.append(
        {
            "module": "unsupported",
            "legacy_script": "stats pipeline-intermediate diagnostics",
            "legacy_output": rel,
            "new_output": "",
            "release_only_capable": 0,
            "unsupported_reason": "requires S3/S4/S5/S6/S7 pipeline intermediate files outside the release package",
        }
    )


def build_parity_manifest(out_root: Path) -> pd.DataFrame:
    rows = []
    out_root = Path(out_root).resolve()
    for target in LEGACY_TARGETS:
        row = dict(target)
        new_output = str(row.get("new_output", ""))
        if not int(row.get("release_only_capable", 0)):
            row["status"] = "unsupported_release_only"
            row["exists"] = 0
            row["size_bytes"] = 0
        else:
            path = out_root / new_output
            exists = path.is_file()
            row["status"] = "generated" if exists else "missing_release_capable"
            row["exists"] = int(exists)
            row["size_bytes"] = int(path.stat().st_size) if exists else 0
        rows.append(row)
    return pd.DataFrame(rows)
