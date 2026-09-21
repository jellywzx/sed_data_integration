#!/usr/bin/env python3
"""Contract tests for source_family vs manuscript source_subgroup."""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from source_family import classify_source_family  # noqa: E402
from source_subgroup import (  # noqa: E402
    classify_source_subgroup,
    source_subgroup_display_name,
)


def test_source_family_and_source_subgroup_are_distinct_taxonomies():
    cases = [
        ("GloRiSe", "daily", "in_situ", "global_multi_source_archives"),
        ("GFQA_v2", "daily", "in_situ", "global_multi_source_archives"),
        ("USGS NWIS", "daily", "in_situ", "national_agency_monitoring_networks"),
        ("HYDAT", "daily", "in_situ", "national_agency_monitoring_networks"),
        ("Bayern", "daily", "in_situ", "national_agency_monitoring_networks"),
        ("HYBAM", "daily", "in_situ", "regional_basin_specific_datasets"),
        ("Mekong Delta", "daily", "in_situ", "regional_basin_specific_datasets"),
        ("Milliman", "climatology", "climatology", "climatological_long_term_synthesis_sources"),
        ("HMA", "climatology", "climatology", "climatological_long_term_synthesis_sources"),
        ("GSED", "monthly", "satellite", "satellite_derived_sources"),
        ("Dethier", "monthly", "satellite", "satellite_derived_sources"),
        ("RivSed", "daily", "satellite", "satellite_derived_sources"),
    ]

    for source, resolution, expected_family, expected_subgroup in cases:
        assert classify_source_family(source, resolution=resolution) == expected_family
        assert classify_source_subgroup(source, resolution=resolution) == expected_subgroup


def test_huanghe_subgroup_depends_on_release_role():
    assert classify_source_family("Huanghe", resolution="annual") == "in_situ"
    assert classify_source_subgroup("Huanghe", resolution="annual") == (
        "regional_basin_specific_datasets"
    )

    assert classify_source_family("Huanghe", resolution="climatology") == "climatology"
    assert classify_source_subgroup("Huanghe", resolution="climatology") == (
        "climatological_long_term_synthesis_sources"
    )


def test_source_subgroup_display_names_match_manuscript_labels():
    assert source_subgroup_display_name("USGS NWIS", "daily") == (
        "National and agency monitoring networks"
    )
    assert source_subgroup_display_name("GloRiSe", "daily") == (
        "Global and multi-source archives"
    )
    assert source_subgroup_display_name("HYBAM", "daily") == (
        "Regional and basin-specific datasets"
    )
    assert source_subgroup_display_name("Milliman", "climatology") == (
        "Climatological and long-term synthesis sources"
    )
    assert source_subgroup_display_name("GSED", "monthly") == (
        "Satellite-derived sources"
    )