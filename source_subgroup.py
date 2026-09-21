#!/usr/bin/env python3
"""Manuscript-facing source subgroup taxonomy.

This module represents the Source subgroup terminology used in manuscript
Table 2 / Supplementary Table S1. It is intentionally separate from
source_family.py.

Important distinction
---------------------
source_family is an operational pipeline class used for processing and merge
policy: in_situ, climatology, satellite, or other.

source_subgroup is a descriptive provenance class used to describe where a
dataset belongs in the manuscript source inventory:
- global_multi_source_archives
- national_agency_monitoring_networks
- regional_basin_specific_datasets
- climatological_long_term_synthesis_sources
- satellite_derived_sources
- other

Do not use source_subgroup to decide basin-mainline merge eligibility,
validation-only behavior, or release branching. Those decisions belong to
source_family / temporal-resolution logic.

Huanghe contributes to both the main station-reference and climatology
components. Its subgroup is therefore resolution-aware: climatology maps to
climatological/long-term synthesis; otherwise it maps to regional and
basin-specific.
"""

from source_family import normalize_resolution, normalize_source_name


SOURCE_SUBGROUP_GLOBAL_MULTI_SOURCE_ARCHIVES = "global_multi_source_archives"
SOURCE_SUBGROUP_NATIONAL_AGENCY_MONITORING_NETWORKS = (
    "national_agency_monitoring_networks"
)
SOURCE_SUBGROUP_REGIONAL_BASIN_SPECIFIC_DATASETS = (
    "regional_basin_specific_datasets"
)
SOURCE_SUBGROUP_CLIMATOLOGICAL_LONG_TERM_SYNTHESIS_SOURCES = (
    "climatological_long_term_synthesis_sources"
)
SOURCE_SUBGROUP_SATELLITE_DERIVED_SOURCES = "satellite_derived_sources"
SOURCE_SUBGROUP_OTHER = "other"


SOURCE_SUBGROUP_DISPLAY_NAMES = {
    SOURCE_SUBGROUP_GLOBAL_MULTI_SOURCE_ARCHIVES:
        "Global and multi-source archives",
    SOURCE_SUBGROUP_NATIONAL_AGENCY_MONITORING_NETWORKS:
        "National and agency monitoring networks",
    SOURCE_SUBGROUP_REGIONAL_BASIN_SPECIFIC_DATASETS:
        "Regional and basin-specific datasets",
    SOURCE_SUBGROUP_CLIMATOLOGICAL_LONG_TERM_SYNTHESIS_SOURCES:
        "Climatological and long-term synthesis sources",
    SOURCE_SUBGROUP_SATELLITE_DERIVED_SOURCES:
        "Satellite-derived sources",
    SOURCE_SUBGROUP_OTHER:
        "Other / unclassified",
}


_GLOBAL_MULTI_SOURCE_ARCHIVES = frozenset([
    "glorise",
    "gfqa_v2",
    "gfqa",
])

_NATIONAL_AGENCY_MONITORING_NETWORKS = frozenset([
    "usgs",
    "usgs_nwis",
    "hydat",
    "bayern",
])

_REGIONAL_BASIN_SPECIFIC_DATASETS = frozenset([
    "eurasian_river",
    "hybam",
    "rhine",
    "mekong_delta",
    "myanmar",
    "myanmar_rivers",
    "yajiang",
    "chao_phraya_river",
    "robotham",
    "nerc",
    "fukushima",
    "shashi_jianli",
    "huanghe",
    "eusedcollab",
])

_CLIMATOLOGICAL_LONG_TERM_SYNTHESIS_SOURCES = frozenset([
    "milliman",
    "hma",
    "ali_de_boer",
    "vanmaercke",
    "huanghe",
])

_SATELLITE_DERIVED_SOURCES = frozenset([
    "riversed",
    "river_sed",
    "gsed",
    "dethier",
])

_CLIMATOLOGY_RESOLUTION_KEYS = frozenset([
    "climatology",
    "annually_climatology",
])


def classify_source_subgroup(source_name, resolution=None):
    """Return the manuscript-facing source subgroup code for a dataset."""
    key = normalize_source_name(source_name)
    resolution_key = normalize_resolution(resolution)

    if key in _SATELLITE_DERIVED_SOURCES:
        return SOURCE_SUBGROUP_SATELLITE_DERIVED_SOURCES

    if key == "huanghe":
        if resolution_key in _CLIMATOLOGY_RESOLUTION_KEYS:
            return SOURCE_SUBGROUP_CLIMATOLOGICAL_LONG_TERM_SYNTHESIS_SOURCES
        return SOURCE_SUBGROUP_REGIONAL_BASIN_SPECIFIC_DATASETS

    if key in _CLIMATOLOGICAL_LONG_TERM_SYNTHESIS_SOURCES:
        return SOURCE_SUBGROUP_CLIMATOLOGICAL_LONG_TERM_SYNTHESIS_SOURCES
    if key in _GLOBAL_MULTI_SOURCE_ARCHIVES:
        return SOURCE_SUBGROUP_GLOBAL_MULTI_SOURCE_ARCHIVES
    if key in _NATIONAL_AGENCY_MONITORING_NETWORKS:
        return SOURCE_SUBGROUP_NATIONAL_AGENCY_MONITORING_NETWORKS
    if key in _REGIONAL_BASIN_SPECIFIC_DATASETS:
        return SOURCE_SUBGROUP_REGIONAL_BASIN_SPECIFIC_DATASETS

    return SOURCE_SUBGROUP_OTHER


def source_subgroup_display_name(source_name=None, resolution=None, subgroup=None):
    """Return the manuscript-facing display label for a subgroup."""
    if subgroup is None:
        subgroup = classify_source_subgroup(source_name, resolution=resolution)
    return SOURCE_SUBGROUP_DISPLAY_NAMES.get(
        subgroup,
        SOURCE_SUBGROUP_DISPLAY_NAMES[SOURCE_SUBGROUP_OTHER],
    )