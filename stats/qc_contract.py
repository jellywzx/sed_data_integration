#!/usr/bin/env python3
"""Local fallback contract constants for stats scripts.

``stats/temporal_coverage_stats.py`` normally imports these constants from the
pipeline-level ``qc_contract.py`` module.  Some deployments copy or run only the
``stats/`` directory under ``scripts_basin_test/``; in that layout the
pipeline-level module is not importable and the stats script fails before doing
any work.  Keeping the small variable-name contract here makes the stats script
self-contained for those deployments while preserving the original import path
when the full pipeline module is available.
"""

TIME_VAR_NAMES = ["time", "Time", "t", "datetime", "date", "sample"]
Q_VAR_NAMES = ["Q", "discharge", "Discharge_m3_s", "Discharge"]
SSC_VAR_NAMES = ["SSC", "ssc", "TSS_mg_L", "TSS"]
SSL_VAR_NAMES = ["SSL", "sediment_load", "Sediment_load"]
