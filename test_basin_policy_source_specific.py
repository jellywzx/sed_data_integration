#!/usr/bin/env python3
"""Smoke tests for the source-specific basin policy override."""

from basin_policy import classify_basin_result


def _assert_case(label, expected, **kwargs):
    actual = classify_basin_result(**kwargs)
    if actual != expected:
        raise AssertionError(
            "{}: expected {}, got {} for {}".format(label, expected, actual, kwargs)
        )


def main():
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
        "gsed_reach_product_override",
        ("resolved", "reach_product_offset_ok"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=2000,
        source_name="GSED",
        point_in_local=True,
        point_in_basin=True,
    )
    _assert_case(
        "riversed_basin_only_not_enough",
        ("unresolved", "large_offset"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=2000,
        source_name="RiverSed",
        point_in_local=False,
        point_in_basin=True,
    )
    _assert_case(
        "gsed_too_far",
        ("unresolved", "large_offset"),
        basin_id=12345,
        match_quality="distance_only",
        distance_m=6000,
        source_name="GSED",
        point_in_local=True,
        point_in_basin=True,
    )
    _assert_case(
        "gsed_area_mismatch_still_rejected",
        ("unresolved", "area_mismatch"),
        basin_id=12345,
        match_quality="area_mismatch",
        distance_m=2000,
        source_name="GSED",
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
    print("Source-specific basin policy smoke tests passed.")


if __name__ == "__main__":
    main()
