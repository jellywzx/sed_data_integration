#!/usr/bin/env python3
"""Pure upstream-area COMID ranking for the experimental S4 branch.

This module deliberately does not change basin_tracer.py. Candidate retrieval
is delegated to UpstreamBasinTracer.get_nearby_candidate_reaches(), so the
production search box and 120 km candidate-distance restriction are retained.

Distance never contributes to ranking except for a genuinely equal area error.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional

import numpy as np
import pandas as pd
from shapely.geometry import Point


@dataclass
class Selection:
    reach_info: Optional[dict]
    ranked: pd.DataFrame
    ambiguous: bool
    nearby_competitors: int


@dataclass
class GeometrySelection:
    reach_info: Optional[dict]
    candidates: pd.DataFrame
    ambiguous: bool
    competitor_comids: tuple[int, ...]
    local_checked_count: int
    local_unchecked_count: int
    basin_checked_count: int
    basin_unchecked_count: int
    selection_complete: bool


def positive_finite(value: object) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0.0 else None


def relative_area_error(merit_area: object, reported_area: object) -> float:
    """Absolute relative difference, not the legacy signed log area_error."""
    merit = positive_finite(merit_area)
    reported = positive_finite(reported_area)
    if merit is None or reported is None:
        return math.nan
    return abs(merit - reported) / reported


def legacy_quality(merit_area: float, reported_area: float) -> str:
    """Original tracer quality labels for the unchanged-policy comparison."""
    log_error = abs(math.log10(merit_area / reported_area))
    if log_error < 0.1:
        return "area_matched"
    if log_error < 0.3:
        return "area_approximate"
    return "area_mismatch"


def select_area_min_error(
    candidates: Optional[pd.DataFrame],
    reported_area: object,
    *,
    ambiguity_gap: float = 0.01,
    ambiguity_distance_m: float = 5000.0,
) -> Selection:
    """Rank ALL retrieved candidates by absolute upstream-area relative error.

    The primary sort key is exact numeric area error. Distance is a strict
    tie-breaker only, followed by COMID. Close but non-equal area scores never
    let distance override area. Ambiguity is a separate review diagnostic:
    another COMID within 1 percentage point of the best error and with a
    point-to-reach distance within 5 km of the best distance by default.
    """
    if not (math.isfinite(ambiguity_gap) and ambiguity_gap >= 0):
        raise ValueError("ambiguity_gap must be finite and nonnegative")
    if not (math.isfinite(ambiguity_distance_m) and ambiguity_distance_m >= 0):
        raise ValueError("ambiguity_distance_m must be finite and nonnegative")
    area = positive_finite(reported_area)
    if area is None:
        return Selection(None, pd.DataFrame(), False, 0)
    if candidates is None or len(candidates) == 0:
        return Selection(None, pd.DataFrame(), False, 0)
    required = {"COMID", "uparea", "dist_m", "pfaf_code"}
    if required - set(candidates.columns):
        raise ValueError("Candidate table missing: {}".format(sorted(required - set(candidates.columns))))

    work = candidates.loc[:, ["COMID", "uparea", "dist_m", "pfaf_code"]].copy()
    for key in ("COMID", "uparea", "dist_m"):
        work[key] = pd.to_numeric(work[key], errors="coerce")
    work["pfaf_code"] = work["pfaf_code"].astype("string")
    keep = (
        np.isfinite(work["COMID"])
        & (work["COMID"] > 0)
        & (work["COMID"] % 1 == 0)
        & np.isfinite(work["uparea"])
        & (work["uparea"] > 0)
        & np.isfinite(work["dist_m"])
        & (work["dist_m"] >= 0)
        & work["pfaf_code"].notna()
        & work["pfaf_code"].ne("")
    )
    work = work.loc[keep].copy()
    if work.empty:
        return Selection(None, work, False, 0)

    work["COMID"] = work["COMID"].astype("int64")
    work["area_rel_error"] = (work["uparea"] / area - 1.0).abs()
    ratio = work["uparea"] / area
    work["legacy_log_error"] = np.log10(ratio)
    work["legacy_combined_score"] = np.abs(np.log10(ratio.clip(0.001, 1000.0))) + work["dist_m"] / 120000.0
    work = work.sort_values(["area_rel_error", "dist_m", "COMID"], kind="mergesort")
    work = work.drop_duplicates(subset="COMID", keep="first").reset_index(drop=True)
    work.insert(0, "rank_area_only", np.arange(1, len(work) + 1))
    best = work.iloc[0]
    near = work[
        work["area_rel_error"].le(best["area_rel_error"] + ambiguity_gap + 1e-14)
        & (work["dist_m"] - best["dist_m"]).abs().le(ambiguity_distance_m)
    ]
    comid = int(best["COMID"])
    reach = {
        "COMID": comid,
        "uparea": float(best["uparea"]),
        "distance": float(best["dist_m"]),
        "pfaf_code": str(best["pfaf_code"]),
        "match_quality": legacy_quality(float(best["uparea"]), area),
        # Keep the legacy SIGNED LOG10 quantity: downstream code already uses it.
        "area_error": float(best["legacy_log_error"]),
        "area_rel_error": float(best["area_rel_error"]),
    }
    return Selection(
        reach_info=reach,
        ranked=work,
        ambiguous=near["COMID"].nunique() > 1,
        nearby_competitors=max(0, int(near["COMID"].nunique()) - 1),
    )


def choose_from_tracer(tracer, lon: float, lat: float, area: float, **kwargs) -> Selection:
    return select_area_min_error(tracer.get_nearby_candidate_reaches(lon, lat), area, **kwargs)


def select_area_geometry_priority(
    tracer,
    lon: float,
    lat: float,
    reported_area: object,
    *,
    threshold: float = 0.10,
    basin_check_limit: int = 10,
    ambiguity_gap: float = 0.01,
) -> GeometrySelection:
    """Select an area-qualified reach using verified geometry as priority.

    Every area-qualified candidate is first checked against its single local
    MERIT catchment.  Only when none covers the station are a bounded number
    of the best area candidates fully traced.  Unchecked/failed checks remain
    unknown rather than being converted to ``False``.
    """
    if not (math.isfinite(threshold) and 0 < threshold < 1):
        raise ValueError("area threshold must be in (0, 1)")
    if basin_check_limit < 1:
        raise ValueError("basin_check_limit must be at least 1")
    if not (math.isfinite(ambiguity_gap) and ambiguity_gap >= 0):
        raise ValueError("ambiguity_gap must be finite and nonnegative")

    ranked = select_area_min_error(
        tracer.get_nearby_candidate_reaches(lon, lat),
        reported_area,
        ambiguity_gap=ambiguity_gap,
    ).ranked
    qualified = ranked.loc[ranked["area_rel_error"].le(threshold + 1e-14)].copy()
    qualified["point_in_local"] = pd.Series([None] * len(qualified), index=qualified.index, dtype="object")
    qualified["local_check_status"] = "unchecked"
    qualified["point_in_basin"] = pd.Series([None] * len(qualified), index=qualified.index, dtype="object")
    qualified["basin_check_status"] = "unchecked"
    qualified["geometry_method"] = "not_checked"
    qualified["geometry_priority"] = 3
    if qualified.empty:
        return GeometrySelection(None, qualified, False, (), 0, 0, 0, 0, True)

    point = Point(float(lon), float(lat))
    station_local_comids: set[int] = set()
    # Production tracer stores catchments by the first two COMID digits.  Load
    # each file once and evaluate its candidate subset vectorially; fake/test
    # tracers retain the public single-polygon fallback below.
    if hasattr(tracer, "_load_level2_catchments"):
        regions = qualified["COMID"].astype("int64").astype(str).str[:2]
        for region in regions.unique():
            indexes = qualified.index[regions.eq(region)]
            try:
                catchments = tracer._load_level2_catchments(region)
            except Exception:
                qualified.loc[indexes, "local_check_status"] = "error"
                continue
            if catchments is None:
                qualified.loc[indexes, "local_check_status"] = "unavailable"
                continue
            try:
                possible = list(catchments.sindex.intersection(point.bounds))
                point_rows = catchments.iloc[possible]
                point_rows = point_rows[point_rows.geometry.covers(point)]
                station_local_comids.update(int(value) for value in point_rows["COMID"])
            except Exception:
                # Candidate-local checks below are still valid; full-upstream
                # checks will use the production polygon path as a fallback.
                pass
            comids = qualified.loc[indexes, "COMID"].astype("int64")
            present = comids.isin(catchments.index)
            qualified.loc[indexes[~present], "local_check_status"] = "unavailable"
            if not present.any():
                continue
            found_indexes = indexes[present]
            polygons = catchments.loc[comids[present].to_numpy(), "geometry"]
            covered = polygons.covers(point).to_numpy(dtype=bool)
            qualified.loc[found_indexes, "point_in_local"] = covered
            qualified.loc[found_indexes, "local_check_status"] = "checked"
            local_indexes = found_indexes[covered]
            qualified.loc[local_indexes, "geometry_priority"] = 1
            qualified.loc[local_indexes, "geometry_method"] = "local_catchment"
    else:
        for index, candidate in qualified.iterrows():
            try:
                polygon = tracer.get_upstream_basin_polygon({int(candidate["COMID"])})
            except Exception:
                qualified.at[index, "local_check_status"] = "error"
                continue
            if polygon is None or polygon.is_empty:
                qualified.at[index, "local_check_status"] = "unavailable"
                continue
            covered = bool(polygon.covers(point))
            qualified.at[index, "point_in_local"] = covered
            qualified.at[index, "local_check_status"] = "checked"
            if covered:
                qualified.at[index, "geometry_priority"] = 1
                qualified.at[index, "geometry_method"] = "local_catchment"

    has_local_match = qualified["point_in_local"].map(lambda value: value is True).any()
    if not has_local_match:
        for index, candidate in qualified.head(basin_check_limit).iterrows():
            reach = {
                "COMID": int(candidate["COMID"]),
                "uparea": float(candidate["uparea"]),
                "distance": float(candidate["dist_m"]),
                "pfaf_code": str(candidate["pfaf_code"]),
                "match_quality": legacy_quality(float(candidate["uparea"]), float(reported_area)),
                "area_error": float(candidate["legacy_log_error"]),
                "area_rel_error": float(candidate["area_rel_error"]),
            }
            if station_local_comids and hasattr(tracer, "trace_upstream_reaches"):
                try:
                    upstream = tracer.trace_upstream_reaches(int(candidate["COMID"]), str(candidate["pfaf_code"]))
                except Exception:
                    qualified.at[index, "basin_check_status"] = "error"
                    continue
                method = "upstream_topology_catchment"
                covered = bool(station_local_comids.intersection(upstream))
            else:
                try:
                    geometry = tracer.get_upstream_basin_from_reach(lon, lat, reach)
                except Exception:
                    qualified.at[index, "basin_check_status"] = "error"
                    continue
                method = str(geometry.get("method") or "unavailable")
                if method != "upstream_traced":
                    qualified.at[index, "geometry_method"] = method
                    qualified.at[index, "basin_check_status"] = "fallback_not_geometry"
                    continue
                covered = bool(geometry.get("point_in_basin") is True)
            qualified.at[index, "geometry_method"] = method
            qualified.at[index, "point_in_basin"] = covered
            qualified.at[index, "basin_check_status"] = "checked"
            if covered:
                qualified.at[index, "geometry_priority"] = 2

    qualified = qualified.sort_values(
        ["geometry_priority", "area_rel_error", "dist_m", "COMID"], kind="mergesort"
    ).reset_index(drop=True)
    qualified.insert(0, "rank_geometry_priority", np.arange(1, len(qualified) + 1))
    best = qualified.iloc[0]
    same_priority = qualified[qualified["geometry_priority"].eq(best["geometry_priority"])]
    close = same_priority[
        same_priority["area_rel_error"].le(float(best["area_rel_error"]) + ambiguity_gap + 1e-14)
    ]
    unchecked_basin = qualified["basin_check_status"].eq("unchecked")
    unchecked_close = unchecked_basin & qualified["area_rel_error"].le(
        float(best["area_rel_error"]) + ambiguity_gap + 1e-14
    )
    potential_same_priority = unchecked_close if int(best["geometry_priority"]) > 1 else pd.Series(False, index=qualified.index)
    selection_complete = bool(
        (int(best["geometry_priority"]) == 1)
        or (int(best["geometry_priority"]) == 2 and not potential_same_priority.any())
        or (int(best["geometry_priority"]) == 3 and not unchecked_basin.any())
    )
    competitors = set(int(value) for value in close["COMID"])
    competitors.update(int(value) for value in qualified.loc[potential_same_priority, "COMID"])
    ambiguous = len(competitors) > 1 or not selection_complete
    reach = {
        "COMID": int(best["COMID"]),
        "uparea": float(best["uparea"]),
        "distance": float(best["dist_m"]),
        "pfaf_code": str(best["pfaf_code"]),
        "match_quality": legacy_quality(float(best["uparea"]), float(reported_area)),
        "area_error": float(best["legacy_log_error"]),
        "area_rel_error": float(best["area_rel_error"]),
        "point_in_local": best["point_in_local"],
        "point_in_basin": best["point_in_basin"],
        "geometry_method": str(best["geometry_method"]),
        "geometry_priority": int(best["geometry_priority"]),
    }
    return GeometrySelection(
        reach_info=reach,
        candidates=qualified,
        ambiguous=ambiguous,
        competitor_comids=tuple(sorted(competitors)),
        local_checked_count=int(qualified["local_check_status"].eq("checked").sum()),
        local_unchecked_count=int(qualified["local_check_status"].ne("checked").sum()),
        basin_checked_count=int(qualified["basin_check_status"].eq("checked").sum()),
        basin_unchecked_count=int(unchecked_basin.sum()),
        selection_complete=selection_complete,
    )
