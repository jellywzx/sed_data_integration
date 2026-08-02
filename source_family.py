#!/usr/bin/env python3
"""Shared source_family taxonomy for the sediment reference pipeline.

source_family is a coarse, release-facing grouping of contributing datasets:
  in_situ       gauge / monitoring-network point observations
  climatology   long-term mean / compilation products (Milliman, HMA,
                Ali & De Boer, Vanmaercke, and Huanghe climatology)
  satellite     remote-sensing derived records (RiverSed, GSED, Dethier)
  other         anything not yet classified

Classification is driven by the canonical SOURCE NAME (the ``source`` column
from s3/s5, stored in s6_cluster_quality_order.csv), NOT by the upstream
observation_type string.  observation_type is kept only as a low-priority
fallback for unknown source names.

Huanghe exists both as a monthly/annual in-situ source and as a climatology
compilation; the resolution argument disambiguates it.
"""

import re

# ---------------------------------------------------------------------------
# Family constants
# ---------------------------------------------------------------------------
SOURCE_FAMILY_IN_SITU = "in_situ"
SOURCE_FAMILY_CLIMATOLOGY = "climatology"
SOURCE_FAMILY_SATELLITE = "satellite"
SOURCE_FAMILY_OTHER = "other"

# ---------------------------------------------------------------------------
# Canonical source-name sets (lowercased, underscore-normalized)
# ---------------------------------------------------------------------------
_IN_SITU_SOURCES = frozenset([
    "glorise",
    "gfqa_v2",
    "gfqa",
    "usgs",
    "usgs_nwis",
    "hydat",
    "bayern",
    "eusedcollab",
    "eurasian_river",
    "hybam",
    "rhine",
    "mekong_delta",
    "myanmar",
    "yajiang",
    "chao_phraya_river",
    "robotham",
    "nerc",
    "fukushima",
    "shashi_jianli",
    "huanghe",   # fallback; resolution="climatology" overrides
])

_CLIMATOLOGY_SOURCES = frozenset([
    "milliman",
    "hma",
    "ali_de_boer",
    "vanmaercke",
    "huanghe",
])

_SATELLITE_SOURCES = frozenset([
    "riversed",
    "river_sed",
    "gsed",
    "dethier",
])

# Public mapping dictionary (built from the internal sets above,
# for introspection / external consumers)
SOURCE_FAMILY_MAP = {}
for _src in _IN_SITU_SOURCES:
    SOURCE_FAMILY_MAP[_src] = "in_situ"
for _src in _CLIMATOLOGY_SOURCES:
    SOURCE_FAMILY_MAP[_src] = "climatology"
for _src in _SATELLITE_SOURCES:
    SOURCE_FAMILY_MAP[_src] = "satellite"

# ---------------------------------------------------------------------------
# Display-name aliases -> canonical keys
# (variants seen in organized filenames / catalogs / manuscript tables)
# ---------------------------------------------------------------------------
_SOURCE_NAME_ALIASES = {
    # Climatology sources
    "ali_&_de_boer": "ali_de_boer",
    "ali_and_de_boer": "ali_de_boer",
    "ali_de_boer_(upper_indus)": "ali_de_boer",
    "high_mountain_asia": "hma",
    "high_mountain_asia_(hma)": "hma",
    "milliman_&_farnsworth": "milliman",
    "milliman_and_farnsworth": "milliman",
    "milliman_farnsworth": "milliman",
    "vanmaercke_et_al": "vanmaercke",
    "vanmaercke_et_al.": "vanmaercke",
    "vanmaercke_africa": "vanmaercke",
    # Satellite sources
    "riversed_(usa)": "riversed",
    "rivsed": "riversed",
    "aquasat": "riversed",
    # In-situ sources with display-name variants
    "huanghe_(yellow_river)": "huanghe",
    "yellow_river": "huanghe",
    "chao_phraya": "chao_phraya_river",
    "shashi_jianli": "shashi_jianli",
    "mekong_delta": "mekong_delta",
    "eurasian_river": "eurasian_river",
    "nerc_avon": "nerc",
    "nerc-hampshire_avon": "nerc",
    "hampshire_avon": "nerc",
}

# ---------------------------------------------------------------------------
# Resolution keys that mean "climatology" (mirrors RESOLUTION_CODES aliases)
# ---------------------------------------------------------------------------
_CLIMATOLOGY_RESOLUTION_KEYS = frozenset([
    "climatology",
    "annually_climatology",
])

# ---------------------------------------------------------------------------
# Merge / validation policy for the basin mainline
# ---------------------------------------------------------------------------
MERGE_EXCLUDED_SOURCE_FAMILIES = frozenset(["satellite", "climatology"])
VALIDATION_ONLY_SOURCE_FAMILIES = frozenset(["satellite"])


# ===================================================================
# Public helpers
# ===================================================================

def _normalize_key(text):
    """Normalize a source name or resolution string to a stable lookup key."""
    if text is None:
        return ""
    s = str(text).strip().lower()
    s = s.replace("-", "_").replace(" ", "_")
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_source_name(source_name):
    """Return the canonical source-family lookup key for *source_name*.

    Handles common display-name variants (e.g. ``"Ali & De Boer"`` ->
    ``"ali_de_boer"``) via the alias table.
    """
    key = _normalize_key(source_name)
    return _SOURCE_NAME_ALIASES.get(key, key)


def normalize_resolution(resolution):
    """Normalize a resolution value for comparison against
    ``_CLIMATOLOGY_RESOLUTION_KEYS``."""
    return _normalize_key(resolution)


# ===================================================================
# Primary classifier
# ===================================================================

def classify_source_family(source_name, resolution=None, observation_type=None):
    """Classify **source_family** from the canonical source name.

    Priority (first match wins):

    1. **satellite** source names -> ``"satellite"``
    2. **climatology** source names -> ``"climatology"``
       (Huanghe -> climatology ONLY when *resolution* is ``"climatology"``)
    3. **in_situ** source names -> ``"in_situ"``
    4. *observation_type* text fallback (legacy heuristics)
    5. anything else -> ``"other"``

    Parameters
    ----------
    source_name : str or None
        Dataset name (the ``source`` column from s3/s5/s6 CSVs).
    resolution : str or None
        Optional temporal resolution.  Only needed for Huanghe disambiguation.
    observation_type : str or None
        Optional upstream observation_type string; used as a last-resort
        fallback when *source_name* is not recognised.

    Returns
    -------
    str
        One of ``"in_situ"``, ``"climatology"``, ``"satellite"``, ``"other"``.
    """
    key = normalize_source_name(source_name)
    # Handle backward compat: if source_name itself is an observation_type
    # keyword (e.g. "Satellite"), use the fallback heuristics.
    if key in _SATELLITE_SOURCES or key in {"satellite", "remote_sensing", 
                                             "remote_sensing_observation",
                                             "satellite_observation"}:
        return SOURCE_FAMILY_SATELLITE
    if key == "huanghe":
        if normalize_resolution(resolution) in _CLIMATOLOGY_RESOLUTION_KEYS:
            return SOURCE_FAMILY_CLIMATOLOGY
        return SOURCE_FAMILY_IN_SITU
    if key in _CLIMATOLOGY_SOURCES:
        return SOURCE_FAMILY_CLIMATOLOGY
    if key in _IN_SITU_SOURCES or key in {"in_situ", "insitu",
             "in_situ_observation", "station", "station_observation",
             "gauge", "gauge_observation", "in_situ_station_data",
             "usgs", "hydat"}:
        return SOURCE_FAMILY_IN_SITU
    if key in {"secondary_compilation", "compiled", "compilation",
               "secondary", "secondary_dataset"}:
        return "secondary_compilation"
    if observation_type is not None:
        legacy = _family_from_observation_type_text(observation_type)
        if legacy:
            return legacy
    return SOURCE_FAMILY_OTHER


# ===================================================================
# Merge policy helpers
# ===================================================================

def is_merge_eligible_source(source_name, resolution=None, observation_type=None,
                             include_satellite_in_main_merge=False):
    """Return True if a source should be considered for the main basin merge."""
    family = classify_source_family(
        source_name, resolution=resolution, observation_type=observation_type)
    if include_satellite_in_main_merge and family == SOURCE_FAMILY_SATELLITE:
        return True
    return family not in MERGE_EXCLUDED_SOURCE_FAMILIES


def merge_exclusion_reason(source_name, resolution=None, observation_type=None,
                           include_satellite_in_main_merge=False):
    """Return a human-readable reason if *source_name* is excluded from merging,
    or an empty string if it is eligible."""
    family = classify_source_family(
        source_name, resolution=resolution, observation_type=observation_type)
    if family == SOURCE_FAMILY_SATELLITE and not include_satellite_in_main_merge:
        return "source_family=satellite excluded from default main merge"
    if family not in MERGE_EXCLUDED_SOURCE_FAMILIES:
        return ""
    return "source_family={} excluded from default main merge".format(family)


def merge_policy_for_source(source_name, resolution=None, observation_type=None,
                            include_satellite_in_main_merge=False):
    """Return the merge policy label for *source_name*.

    Returns one of ``"merge_candidate"``, ``"validation_only"``, or
    ``"excluded_from_main_merge"``.
    """
    family = classify_source_family(
        source_name, resolution=resolution, observation_type=observation_type)
    if family in VALIDATION_ONLY_SOURCE_FAMILIES and not include_satellite_in_main_merge:
        return "validation_only"
    if is_merge_eligible_source(
        source_name, resolution=resolution, observation_type=observation_type,
        include_satellite_in_main_merge=include_satellite_in_main_merge,
    ):
        return "merge_candidate"
    return "excluded_from_main_merge"


# ===================================================================
# Legacy compat: observation_type-based fallback
# ===================================================================

def _family_from_observation_type_text(observation_type):
    """Legacy observation_type heuristics -- kept ONLY as a last-resort
    fallback for unknown source names.  Returns ``""`` when nothing matches."""
    low = _normalize_key(observation_type)
    if low in {"satellite", "remote_sensing", "remote_sensing_observation",
               "satellite_observation"}:
        return SOURCE_FAMILY_SATELLITE
    if low in {"in_situ", "insitu", "in_situ_observation", "station",
               "station_observation", "gauge", "gauge_observation",
               "in_situ_station_data", "usgs", "hydat"}:
        return SOURCE_FAMILY_IN_SITU
    if low in {"climatology", "clim", "compilation", "compiled",
               "secondary", "secondary_compilation"}:
        return SOURCE_FAMILY_CLIMATOLOGY
    return ""


# ---------------------------------------------------------------------------
# Backward-compatible alias so stale imports do not raise ImportError.
# Old callers that pass observation_type positionally will land in
# ``source_name``, which for unknown obs-strings (e.g. "In-situ station data")
# returns "other" -- identical to the old behaviour.
# ---------------------------------------------------------------------------
classify_source_family_from_observation_type = classify_source_family
