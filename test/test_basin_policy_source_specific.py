#!/usr/bin/env python3
"""Smoke tests for no-basin-match source policy."""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from basin_policy import BASIN_FLAG_ORDER, classify_basin_result, should_skip_basin_matching


def _assert_case(label, expected, **kwargs):
    actual = classify_basin_result(**kwargs)
    if actual != expected:
        raise AssertionError(
            "{}: expected {}, got {} for {}".format(label, expected, actual, kwargs)
        )


def main():
    expected_flags = {
        "ok",
        "reach_product_offset_ok",
        "large_offset",
        "area_mismatch",
        "geometry_inconsistent",
        "no_match",
    }
    if set(BASIN_FLAG_ORDER) != expected_flags:
        raise AssertionError("Unexpected basin flags: {}".format(BASIN_FLAG_ORDER))

    for source_name in ("Dethier", "Deither", "dethier"):
        if not should_skip_basin_matching(source_name):
            raise AssertionError("{} should skip basin matching".format(source_name))

    for source_name in ("RiverSed", "GSED", "USGS", "HYBAM"):
        if should_skip_basin_matching(source_name):
            raise AssertionError("{} should not skip basin matching".format(source_name))

    _assert_case(
        "generic_small_offset",
        ("resolved", "ok"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=250,
        source_name="USGS",
        point_in_local=False,
        point_in_basin=False,
    )
    _assert_case(
        "gsed_reach_scale_offset",
        ("resolved", "reach_product_offset_ok"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=2000,
        source_name="GSED",
        point_in_local=True,
        point_in_basin=True,
    )
    _assert_case(
        "riversed_small_offset",
        ("resolved", "ok"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=250,
        source_name="RiverSed",
        point_in_local=True,
        point_in_basin=True,
    )
    _assert_case(
        "gsed_reach_scale_no_local",
        ("unresolved", "large_offset"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=2000,
        source_name="GSED",
        point_in_local=False,
        point_in_basin=True,
    )
    _assert_case(
        "generic_large_offset",
        ("unresolved", "large_offset"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=2000,
        source_name="USGS",
        point_in_local=True,
        point_in_basin=True,
    )
    _assert_case(
        "missing_basin_id",
        ("unresolved", "no_match"),
        basin_id=None,
        match_quality="distance_only",
        distance_m=2000,
        source_name="GSED",
        point_in_local=True,
        point_in_basin=True,
    )
    _assert_case(
        "failed_match",
        ("unresolved", "no_match"),
        basin_id=12345,
        match_quality="failed",
        distance_m=2000,
        source_name="GSED",
        point_in_local=True,
        point_in_basin=True,
    )
    print("Basin policy source-specific smoke tests passed.")


if __name__ == "__main__":
    main()
