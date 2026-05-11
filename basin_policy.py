#!/usr/bin/env python3
"""
Shared basin-matching policy helpers for the basin reference pipeline.

The release policy is intentionally conservative:
  - only resolved stations may keep a published basin assignment;
  - unresolved stations retain observations but do not publish basin polygons.

This module does not perform any geometric computation by itself.
The geometric evidence (`point_in_local`, `point_in_basin`, `distance_m`) is
computed upstream by `basin_tracer.py`, and this file only converts those
signals into release-facing labels such as:
  - basin_status: resolved / unresolved
  - basin_flag: the primary reason for that status
"""

import math


# Ordered list of basin tracer match-quality labels.
#
# These names come from the reach-matching stage and are preserved here so that
# downstream NetCDF / CSV outputs can use stable codes and flag meanings.
MATCH_QUALITY_ORDER = (
    "distance_only",
    "area_matched",
    "area_approximate",
    "area_mismatch",
    "failed",
)
MATCH_QUALITY_CODES = {name: idx for idx, name in enumerate(MATCH_QUALITY_ORDER)}
MATCH_QUALITY_CODE_TO_NAME = {idx: name for name, idx in MATCH_QUALITY_CODES.items()}
MATCH_QUALITY_MEANINGS = " ".join(MATCH_QUALITY_ORDER + ("unknown",))

# Release-facing status labels.
#
# The public dataset deliberately keeps this binary:
#   - resolved: basin assignment is considered publishable
#   - unresolved: keep the observation, but do not publish a basin polygon
BASIN_STATUS_ORDER = ("resolved", "unresolved")
BASIN_STATUS_CODES = {name: idx for idx, name in enumerate(BASIN_STATUS_ORDER)}
BASIN_STATUS_CODE_TO_NAME = {idx: name for name, idx in BASIN_STATUS_CODES.items()}
BASIN_STATUS_MEANINGS = " ".join(BASIN_STATUS_ORDER + ("unknown",))

# Reason flags explaining why a station ended up resolved or unresolved.
#
# These are intentionally compact, because they are written into machine-facing
# outputs and then explained in README / metadata documents.
BASIN_FLAG_ORDER = (
    "ok",
    "large_offset",
    "area_mismatch",
    "geometry_inconsistent",
    "no_match",
)
BASIN_FLAG_CODES = {name: idx for idx, name in enumerate(BASIN_FLAG_ORDER)}
BASIN_FLAG_CODE_TO_NAME = {idx: name for name, idx in BASIN_FLAG_CODES.items()}
BASIN_FLAG_MEANINGS = " ".join(BASIN_FLAG_ORDER + ("unknown",))

# These source products are remote-sensing / reach-scale records rather than
# bank-gauge observations. The basin mainline keeps their observations but does
# not publish a MERIT basin assignment for them.
NO_BASIN_MATCH_SOURCES = ("RiverSed", "GSED", "Dethier")
NO_BASIN_MATCH_SOURCE_ALIASES = {
    "deither": "dethier",
}
NO_BASIN_MATCH_SOURCE_SET = frozenset(
    tuple(name.lower() for name in NO_BASIN_MATCH_SOURCES)
    + tuple(NO_BASIN_MATCH_SOURCE_ALIASES.keys())
)


def _clean_text(value):
    """Normalize arbitrary text-like input to a safe, comparable string."""
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _coerce_float(value):
    """Convert loosely typed numeric input to a finite float or math.nan.

    The policy layer receives values from CSV rows, pandas scalars, and Python
    objects. Normalizing them here keeps the decision logic below compact and
    predictable.
    """
    try:
        if value is None:
            return math.nan
        number = float(value)
        return number if math.isfinite(number) else math.nan
    except Exception:
        return math.nan


def _coerce_bool(value):
    """Convert mixed boolean encodings to True / False.

    This accepts actual booleans as well as common serialized truthy strings
    such as "1", "true", and "yes". Empty strings and unknown text default to
    False so that the release policy stays conservative.
    """
    if isinstance(value, bool):
        return value
    text = _clean_text(value).lower()
    if not text:
        return False
    return text in {"1", "true", "yes", "y", "t"}


def normalize_match_quality(value):
    """Map raw match-quality text to the canonical controlled vocabulary.

    Unknown labels are normalized to "unknown" instead of raising an error, so
    downstream pipelines can keep running while still surfacing that something
    unexpected entered the policy layer.
    """
    text = _clean_text(value).lower()
    return text if text in MATCH_QUALITY_CODES else "failed" if text == "failed" else "unknown"


def normalize_source_name(value):
    """Map source names to a lowercase comparable token."""
    return _clean_text(value).lower()


def canonical_source_name(value):
    """Normalize source names and known spelling aliases for policy checks."""
    source = normalize_source_name(value)
    return NO_BASIN_MATCH_SOURCE_ALIASES.get(source, source)


def should_skip_basin_matching(source_name):
    """Return True when a source should not enter MERIT basin matching."""
    return canonical_source_name(source_name) in NO_BASIN_MATCH_SOURCE_SET


def classify_basin_result(
    basin_id,
    match_quality,
    distance_m,
    source_name="",
    point_in_local=False,
    point_in_basin=False,
):
    """Classify one station's basin assignment for release.

    Parameters
    ----------
    basin_id
        Matched COMID or equivalent basin identifier. Missing basin_id means the
        tracer failed to produce a usable assignment.
    match_quality
        Reach-matching quality from the tracer. This captures whether the match
        was based only on geometric proximity or also supported by drainage
        area.
    distance_m
        Distance from the original station point to the matched reach.
    source_name
        Source dataset identifier. A small, explicit list of remote-sensing /
        reach-scale products is never released with a MERIT basin assignment.
    point_in_local
        Whether the original point is covered by the matched local catchment.
        This is the strongest geometry-consistency check in the current policy.
    point_in_basin
        Whether the original point is covered by the traced full upstream basin.
        This is kept mainly as a diagnostic signal; by itself it is too broad to
        justify automatic release.

    Returns
    -------
    tuple[str, str]
        `(basin_status, basin_flag)`

    Decision order
    --------------
    The branch order is deliberate:
      1. reject sources that do not enter basin matching;
      2. reject missing / failed matches;
      3. reject area-mismatch cases;
      4. accept only a small set of high-confidence rules;
      5. otherwise keep the record but mark the basin unresolved.
    """
    basin_missing = math.isnan(_coerce_float(basin_id))
    quality = normalize_match_quality(match_quality)
    distance = _coerce_float(distance_m)
    local_ok = _coerce_bool(point_in_local)
    basin_ok = _coerce_bool(point_in_basin)

    if should_skip_basin_matching(source_name):
        return "unresolved", "no_match"

    # No basin id or an outright failed reach match means there is no basin
    # assignment we can safely publish. We still keep the observation itself.
    if basin_missing or quality == "failed":
        return "unresolved", "no_match"

    # If reported area and matched reach area disagree strongly, keep that
    # warning instead of accepting the match by distance alone.
    if quality == "area_mismatch":
        return "unresolved", "area_mismatch"

    resolved = False
    # Rule 1: very small offset is accepted even if the point sits slightly off
    # the river centerline. This is the common "bank vs channel" case.
    if math.isfinite(distance) and distance <= 300.0:
        resolved = True
    # Rule 2: allow modest offsets when drainage-area evidence supports the
    # matched reach.
    elif math.isfinite(distance) and distance <= 1000.0 and quality in {"area_matched", "area_approximate"}:
        resolved = True
    # Rule 3: allow modest offsets when the original point still falls inside
    # the matched local catchment. This preserves a simple local-consistency
    # check without introducing heavier heuristic logic.
    elif math.isfinite(distance) and distance <= 1000.0 and local_ok:
        resolved = True

    if resolved:
        return "resolved", "ok"

    # Beyond 1 km, the current policy assumes the offset is too large for
    # automatic release unless a future manual-review workflow says otherwise.
    if math.isfinite(distance) and distance > 1000.0:
        return "unresolved", "large_offset"

    # If neither the local catchment nor the full traced basin covers the point,
    # keep the observation but flag the basin assignment as geometrically
    # inconsistent. The final fallback below intentionally returns the same flag:
    # anything not accepted by the simple rules remains unresolved.
    if not local_ok and not basin_ok:
        return "unresolved", "geometry_inconsistent"

    return "unresolved", "geometry_inconsistent"
