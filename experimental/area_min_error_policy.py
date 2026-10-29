#!/usr/bin/env python3
"""Experimental area-only acceptance; do not use as a publication rule."""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional

from experimental.area_min_error_selector import positive_finite, relative_area_error

REACH_HINT_SOURCES = frozenset({"gsed", "riversed"})


@dataclass(frozen=True)
class Decision:
    status: str
    flag: str
    area_rel_error: float
    review_required: bool
    review_reasons: str


def area_policy(
    *,
    reach: Optional[dict],
    reported_area: object,
    source: str,
    threshold: float = 0.10,
    evidence_verified: bool = False,
    geometry_rechecked: bool = False,
    geometry_method: str = "",
    point_in_local: Optional[bool] = None,
    point_in_basin: Optional[bool] = None,
    ambiguous: bool = False,
) -> Decision:
    if not (math.isfinite(threshold) and 0 < threshold < 1):
        raise ValueError("area threshold must be in (0, 1)")
    if str(source).strip().lower() in REACH_HINT_SOURCES:
        raise ValueError("Reach-hint sources must remain on the production S4 rule")
    if positive_finite(reported_area) is None:
        return Decision("unresolved", "no_match", math.nan, True, "invalid_independent_area")
    if not reach or reach.get("COMID") is None:
        return Decision("unresolved", "no_match", math.nan, True, "no_valid_merit_candidate")
    error = relative_area_error(reach.get("uparea"), reported_area)
    if not math.isfinite(error) or error > threshold:
        return Decision("unresolved", "area_mismatch", error, True, "area_error_exceeds_threshold")

    distance = reach.get("distance")
    try:
        distance = float(distance)
    except (ValueError, TypeError):
        distance = math.nan
    if not math.isfinite(distance) or distance < 0:
        return Decision("unresolved", "no_match", error, True, "invalid_reach_distance")

    review = []
    if not evidence_verified:
        review.append("independent_area_source_unverified")
    if distance > 1000.0:
        review.append("distance_over_1km")
    if distance > 10000.0:
        review.append("distance_over_10km_high_risk")
    if ambiguous:
        review.append("competing_area_matched_reaches")
    if not geometry_rechecked:
        review.append("geometry_not_retraced")
    elif geometry_method != "upstream_traced":
        review.append("upstream_polygon_not_traced")
    elif point_in_local is False and point_in_basin is False:
        review.append("station_outside_local_and_upstream_basin")
    elif point_in_local is False:
        review.append("station_outside_local_catchment")
    # Keep existing basin_flag controlled vocabulary; experiment details go in
    # additional experimental_* columns. 'ok' is only a PROVISIONAL S4 result.
    return Decision("resolved", "ok", error, bool(review), ";".join(review))


def geometry_priority_policy(
    *,
    reach: Optional[dict],
    reported_area: object,
    source: str,
    threshold: float = 0.10,
    evidence_verified: bool = False,
    ambiguous: bool = False,
    selection_complete: bool = True,
) -> Decision:
    """Acceptance policy for E; priority-3 geometry is never resolved."""
    if str(source).strip().lower() in REACH_HINT_SOURCES:
        raise ValueError("Reach-hint sources must remain on the production S4 rule")
    if positive_finite(reported_area) is None:
        return Decision("unresolved", "no_match", math.nan, True, "invalid_independent_area")
    if not reach or reach.get("COMID") is None:
        return Decision("unresolved", "no_match", math.nan, True, "no_area_qualified_candidate")
    error = relative_area_error(reach.get("uparea"), reported_area)
    if not math.isfinite(error) or error > threshold:
        return Decision("unresolved", "area_mismatch", error, True, "area_error_exceeds_threshold")

    reasons = []
    if not evidence_verified:
        reasons.append("independent_area_source_unverified")
    distance = positive_finite(reach.get("distance"))
    if distance is None:
        reasons.append("invalid_reach_distance")
    elif distance > 10000:
        reasons.append("distance_over_10km_high_risk")
    elif distance > 1000:
        reasons.append("distance_over_1km")
    if ambiguous:
        reasons.append("competing_geometry_area_candidates")
    if not selection_complete:
        reasons.append("bounded_geometry_check_incomplete")

    priority = reach.get("geometry_priority")
    if priority == 1:
        reasons.append("local_catchment_covered")
        return Decision("resolved", "ok", error, bool(reasons), ";".join(reasons))
    if priority == 2:
        reasons.append("upstream_basin_covered")
        return Decision("resolved", "ok", error, bool(reasons), ";".join(reasons))
    reasons.append("no_verified_geometry_coverage")
    return Decision("unresolved", "geometry_inconsistent", error, True, ";".join(reasons))
