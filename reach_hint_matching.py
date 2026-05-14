#!/usr/bin/env python3
"""Reach-scale midpoint/endpoint helpers for s4 basin matching.

GSED and RiverSed are reach products. Their file-level lat/lon should remain a
representative midpoint, while s4 can use endpoint candidates first to infer the
best downstream MERIT reach and then trace the basin from that selected reach.
"""

import json
import math

import numpy as np
import pandas as pd

REACH_HINT_SOURCES = {"gsed", "riversed"}


def clean_source(value):
    if value is None:
        return ""
    text = str(value).strip().lower()
    return "" if text == "nan" else text


def should_use_reach_hint_matching(source_name):
    return clean_source(source_name) in REACH_HINT_SOURCES


def _finite_float(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else np.nan
    except Exception:
        return np.nan


def _valid_pair(lat, lon):
    return math.isfinite(_finite_float(lat)) and math.isfinite(_finite_float(lon))


def _candidate_key(lat, lon):
    return (round(float(lat), 12), round(float(lon), 12))


def _parse_json_candidates(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return []
    try:
        payload = json.loads(text)
    except Exception:
        return []
    if isinstance(payload, dict):
        payload = payload.get("endpoints") or payload.get("candidates") or []
    candidates = []
    if not isinstance(payload, list):
        return candidates
    for item in payload:
        if isinstance(item, dict):
            lat = item.get("latitude", item.get("lat"))
            lon = item.get("longitude", item.get("lon"))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            lat, lon = item[0], item[1]
        else:
            continue
        if _valid_pair(lat, lon):
            candidates.append({"latitude": float(lat), "longitude": float(lon)})
    return candidates


def parse_reach_hint_candidates(station):
    """Return endpoint candidates and midpoint from one s3/s4 station row."""
    endpoints = []
    seen = set()

    def add_candidate(lat, lon):
        if not _valid_pair(lat, lon):
            return
        key = _candidate_key(lat, lon)
        if key in seen:
            return
        seen.add(key)
        endpoints.append({"latitude": float(lat), "longitude": float(lon)})

    for candidate in _parse_json_candidates(station.get("reach_endpoint_candidates_json")):
        add_candidate(candidate["latitude"], candidate["longitude"])

    add_candidate(station.get("reach_endpoint_1_lat"), station.get("reach_endpoint_1_lon"))
    add_candidate(station.get("reach_endpoint_2_lat"), station.get("reach_endpoint_2_lon"))

    midpoint_lat = _finite_float(station.get("reach_midpoint_lat"))
    midpoint_lon = _finite_float(station.get("reach_midpoint_lon"))
    if not _valid_pair(midpoint_lat, midpoint_lon):
        midpoint_lat = _finite_float(station.get("lat"))
        midpoint_lon = _finite_float(station.get("lon"))

    return {
        "endpoints": endpoints,
        "midpoint_lat": midpoint_lat,
        "midpoint_lon": midpoint_lon,
    }


def _empty_reach_info():
    return {
        "COMID": None,
        "uparea": np.nan,
        "distance": np.nan,
        "pfaf_code": None,
        "match_quality": "failed",
        "area_error": np.nan,
    }


def _valid_reach_info(reach_info):
    if not isinstance(reach_info, dict):
        return False
    comid = reach_info.get("COMID")
    return comid is not None and not pd.isna(comid)


def resolve_reach_anchor(tracer, station):
    """Resolve a GSED/RiverSed anchor by endpoint-first MERIT reach matching."""
    hints = parse_reach_hint_candidates(station)
    endpoint_matches = []

    for idx, endpoint in enumerate(hints["endpoints"]):
        lat = endpoint["latitude"]
        lon = endpoint["longitude"]
        reach_info = tracer.find_best_reach(float(lon), float(lat), reported_area=None)
        if not _valid_reach_info(reach_info):
            continue
        endpoint_matches.append(
            {
                "candidate_index": idx,
                "latitude": float(lat),
                "longitude": float(lon),
                "reach_info": reach_info,
            }
        )

    if endpoint_matches:
        endpoint_matches.sort(
            key=lambda item: (
                -_finite_float(item["reach_info"].get("uparea")),
                _finite_float(item["reach_info"].get("distance")),
                item["candidate_index"],
            )
        )
        best = endpoint_matches[0]
        return {
            "reach_hint_used": True,
            "reach_anchor_source": "endpoint",
            "reach_anchor_lat": best["latitude"],
            "reach_anchor_lon": best["longitude"],
            "reach_endpoint_match_count": len(endpoint_matches),
            "reach_hint_method": "endpoint_largest_uparea_then_distance",
            "reach_info": best["reach_info"],
        }

    midpoint_lat = hints["midpoint_lat"]
    midpoint_lon = hints["midpoint_lon"]
    reach_info = _empty_reach_info()
    if _valid_pair(midpoint_lat, midpoint_lon):
        reach_info = tracer.find_best_reach(float(midpoint_lon), float(midpoint_lat), reported_area=None)

    return {
        "reach_hint_used": True,
        "reach_anchor_source": "midpoint_fallback" if _valid_reach_info(reach_info) else "no_valid_hint",
        "reach_anchor_lat": midpoint_lat,
        "reach_anchor_lon": midpoint_lon,
        "reach_endpoint_match_count": 0,
        "reach_hint_method": "midpoint_fallback",
        "reach_info": reach_info,
    }
