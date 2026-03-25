#!/usr/bin/env python3
"""
Centralized default paths for s1-s8 pipeline.

Change filenames/dirs here once, and downstream scripts follow.
All paths are relative to Output_r root.

Optional environment override:
  - OUTPUT_R_ROOT: when set, all scripts should use this as Output_r root.
"""

import os
from pathlib import Path

S1_VERIFY_CSV = "scripts/output/s1_verify_time_resolution_results.csv"
S2_ORGANIZED_DIR = "../output_resolution_organized"
S2_OTHER_SUMMARY_CSV = "scripts/output/s2_other_resolution_summary.csv"
S2_OTHER_DETAILS_CSV = "scripts/output/s2_other_resolution_details.csv"
S3_COLLECTED_CSV = "scripts/output/s3_collected_stations.csv"
S4_CLUSTERED_CSV = "scripts/output/s4_clustered_stations.csv"
S4_REPORT_CSV = "scripts/output/s4_merge_qc_nc_report.csv"
S6_REPORT_CSV = "scripts/output/s6_merge_qc_nc_report.csv"
S6_OVERLAP_CSV = "scripts/output/s6_overlap_for_manual_choice.csv"
S7_RESOLVED_CSV = "scripts/output/s7_overlap_resolved.csv"
S8_MERGED_NC = "scripts/output/s8_merged_all.nc"
PIPELINE_OUTPUT_DIR = "scripts/output"

# Shared parameters across steps
# s4 clustering threshold (degrees) and s5 verification threshold should stay the same.
S4_S5_THRESHOLD_DEG = 0.01

# Per-source stricter threshold override (degrees), e.g., GFQA stations are very dense.
# Matching rule: source name startswith(key), case-insensitive.
S4_S5_SOURCE_THRESHOLD_OVERRIDE_DEG = {
    "GFQA": 0.005,
    "GFQA_v2": 0.005,
}


def get_output_r_root(script_dir: Path) -> Path:
    """
    Resolve Output_r root with env override.

    Priority:
      1) OUTPUT_R_ROOT (if provided)
      2) script_dir.parent (scripts/ 上一级)
    """
    env_root = os.environ.get("OUTPUT_R_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()
    return script_dir.parent.resolve()
