#!/usr/bin/env python3
"""
Shared helpers for turning one public GSED reach into a MERIT reach hint.

This module is intentionally lightweight so both `s4_basin_trace_watch.py`
and `build_gsed_merit_area_lookup.py` can reuse the same GSED-specific logic
without pulling in the full basin-tracing CLI stack.
"""

import struct
from pathlib import Path

import numpy as np
import pandas as pd


def normalize_gsed_rid(value):
    """Normalize a raw GSED reach id to a stable integer-like string."""
    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def _read_gsed_dbf_records(dbf_path):
    """Read the minimal GSED DBF attributes without requiring GIS libraries."""
    records = []
    with open(dbf_path, "rb") as handle:
        header = handle.read(32)
        if len(header) < 32:
            raise ValueError(f"Invalid DBF header: {dbf_path}")

        record_count = struct.unpack("<I", header[4:8])[0]
        record_length = struct.unpack("<H", header[10:12])[0]

        fields = []
        while True:
            first = handle.read(1)
            if not first or first == b"\r":
                break
            descriptor = first + handle.read(31)
            name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore")
            fields.append(
                (
                    name,
                    descriptor[11:12].decode("ascii", "ignore"),
                    descriptor[16],
                    descriptor[17],
                )
            )

        for _ in range(record_count):
            record = handle.read(record_length)
            if not record:
                break

            deleted = record[:1] == b"*"
            row = {"_deleted": deleted}
            offset = 1

            for name, field_type, length, decimals in fields:
                raw = record[offset:offset + length]
                offset += length
                text = raw.decode("latin1", "ignore").strip()

                if name not in {"R_ID", "R_level", "Length"}:
                    continue

                if not text:
                    row[name] = None
                    continue

                if field_type in {"N", "F"}:
                    try:
                        value = float(text)
                        if decimals == 0:
                            value = int(value)
                        row[name] = value
                    except ValueError:
                        row[name] = text
                else:
                    row[name] = text

            records.append(row)

    return records


def _extract_polyline_parts(record_content):
    """Parse a PolyLine record into ordered point parts."""
    if len(record_content) < 44:
        return []

    shape_type = struct.unpack("<i", record_content[:4])[0]
    if shape_type == 0:
        return []
    if shape_type != 3:
        raise ValueError(f"Unsupported shapefile geometry type: {shape_type}")

    num_parts = struct.unpack("<i", record_content[36:40])[0]
    num_points = struct.unpack("<i", record_content[40:44])[0]
    parts_offset = 44
    points_offset = parts_offset + 4 * num_parts
    points_end = points_offset + num_points * 16

    if points_end > len(record_content):
        raise ValueError("Corrupted polyline record in shapefile.")
    if num_points == 0:
        return []

    part_starts = [
        struct.unpack(
            "<i",
            record_content[parts_offset + i * 4: parts_offset + (i + 1) * 4],
        )[0]
        for i in range(num_parts)
    ]
    if not part_starts:
        part_starts = [0]
    part_starts.append(num_points)

    points = []
    for i in range(num_points):
        x, y = struct.unpack(
            "<2d",
            record_content[points_offset + i * 16: points_offset + (i + 1) * 16],
        )
        points.append((float(x), float(y)))

    parts = []
    for part_start, part_end in zip(part_starts[:-1], part_starts[1:]):
        if part_end <= part_start:
            continue
        parts.append(points[part_start:part_end])

    return parts


def _extract_polyline_representatives(record_content):
    """Extract the polyline midpoint and unique part endpoints."""
    parts = _extract_polyline_parts(record_content)
    if not parts:
        return None, None, []

    endpoint_candidates = []
    seen = set()
    fallback_point = None
    total_length = 0.0
    for part_points in parts:
        if not part_points:
            continue
        if fallback_point is None:
            fallback_point = part_points[0]
        for lon, lat in (part_points[0], part_points[-1]):
            key = (round(lat, 12), round(lon, 12))
            if key in seen:
                continue
            seen.add(key)
            endpoint_candidates.append(
                {
                    "latitude": lat,
                    "longitude": lon,
                }
            )

        for (lon0, lat0), (lon1, lat1) in zip(part_points[:-1], part_points[1:]):
            total_length += float(np.hypot(lon1 - lon0, lat1 - lat0))

    if fallback_point is None:
        return None, None, endpoint_candidates

    if total_length <= 0.0:
        fallback_lon, fallback_lat = fallback_point
        return fallback_lat, fallback_lon, endpoint_candidates

    midpoint_distance = total_length / 2.0
    traversed = 0.0
    last_point = fallback_point

    for part_points in parts:
        if not part_points:
            continue
        last_point = part_points[-1]
        for (lon0, lat0), (lon1, lat1) in zip(part_points[:-1], part_points[1:]):
            segment_length = float(np.hypot(lon1 - lon0, lat1 - lat0))
            if segment_length <= 0.0:
                continue

            next_traversed = traversed + segment_length
            if next_traversed >= midpoint_distance:
                segment_ratio = (midpoint_distance - traversed) / segment_length
                midpoint_lon = lon0 + segment_ratio * (lon1 - lon0)
                midpoint_lat = lat0 + segment_ratio * (lat1 - lat0)
                return float(midpoint_lat), float(midpoint_lon), endpoint_candidates
            traversed = next_traversed

    last_lon, last_lat = last_point
    return float(last_lat), float(last_lon), endpoint_candidates


def load_gsed_reach_metadata(shapefile_path, target_rids=None):
    """Load midpoint and endpoint candidates for the requested GSED reaches."""
    shapefile_path = Path(shapefile_path)
    dbf_records = _read_gsed_dbf_records(shapefile_path.with_suffix(".dbf"))
    target_rids = (
        {normalize_gsed_rid(r_id) for r_id in target_rids}
        if target_rids is not None
        else None
    )
    metadata = {}

    with open(shapefile_path, "rb") as handle:
        handle.read(100)

        for dbf_row in dbf_records:
            record_header = handle.read(8)
            if not record_header:
                break

            content_length_words = struct.unpack(">i", record_header[4:8])[0]
            content = handle.read(content_length_words * 2)

            if dbf_row.get("_deleted"):
                continue

            r_id_str = normalize_gsed_rid(dbf_row.get("R_ID"))
            if r_id_str is None:
                continue
            if target_rids and r_id_str not in target_rids:
                continue

            midpoint_lat, midpoint_lon, endpoint_candidates = (
                _extract_polyline_representatives(content)
            )
            metadata[r_id_str] = {
                "r_id_str": r_id_str,
                "r_level": (
                    int(dbf_row["R_level"])
                    if dbf_row.get("R_level") is not None
                    else None
                ),
                "reach_length_m": (
                    float(dbf_row["Length"])
                    if dbf_row.get("Length") is not None
                    else None
                ),
                "latitude": midpoint_lat,
                "longitude": midpoint_lon,
                "midpoint_latitude": midpoint_lat,
                "midpoint_longitude": midpoint_lon,
                "endpoint_candidates": endpoint_candidates,
            }

    return metadata


def _empty_reach_info():
    return {
        "COMID": None,
        "uparea": np.nan,
        "distance": np.nan,
        "pfaf_code": None,
        "match_quality": "failed",
        "area_error": np.nan,
    }


def _has_valid_coordinate_pair(lat, lon):
    return pd.notna(lat) and pd.notna(lon)


def _is_valid_reach_info(reach_info):
    if not isinstance(reach_info, dict):
        return False
    comid = reach_info.get("COMID")
    return comid is not None and not pd.isna(comid)


def _safe_float(value, default=np.nan):
    try:
        number = float(value)
        return number if np.isfinite(number) else default
    except Exception:
        return default


def resolve_gsed_anchor(
    tracer,
    meta,
    allow_midpoint_fallback=True,
    allow_centroid_fallback=None,
):
    """Choose the downstream endpoint proxy and matched MERIT reach."""
    if allow_centroid_fallback is not None:
        allow_midpoint_fallback = allow_centroid_fallback

    endpoint_matches = []
    endpoint_candidates = meta.get("endpoint_candidates") or []

    for candidate_index, endpoint in enumerate(endpoint_candidates):
        lat = endpoint.get("latitude")
        lon = endpoint.get("longitude")
        if not _has_valid_coordinate_pair(lat, lon):
            continue

        reach_info = tracer.find_best_reach(float(lon), float(lat), reported_area=None)
        if not _is_valid_reach_info(reach_info):
            continue

        endpoint_matches.append(
            {
                "candidate_index": candidate_index,
                "latitude": float(lat),
                "longitude": float(lon),
                "reach_info": reach_info,
            }
        )

    if endpoint_matches:
        endpoint_matches.sort(
            key=lambda item: (
                -_safe_float(item["reach_info"].get("uparea"), default=-np.inf),
                _safe_float(item["reach_info"].get("distance"), default=np.inf),
                item["candidate_index"],
            )
        )
        best_endpoint = endpoint_matches[0]
        return {
            "anchor_source": "downstream_endpoint",
            "endpoint_match_count": len(endpoint_matches),
            "latitude": best_endpoint["latitude"],
            "longitude": best_endpoint["longitude"],
            "reach_info": best_endpoint["reach_info"],
        }

    midpoint_lat = meta.get("midpoint_latitude", meta.get("latitude"))
    midpoint_lon = meta.get("midpoint_longitude", meta.get("longitude"))
    if not allow_midpoint_fallback:
        return {
            "anchor_source": "no_endpoint_match",
            "endpoint_match_count": 0,
            "latitude": midpoint_lat,
            "longitude": midpoint_lon,
            "reach_info": _empty_reach_info(),
        }

    reach_info = _empty_reach_info()
    if _has_valid_coordinate_pair(midpoint_lat, midpoint_lon):
        reach_info = tracer.find_best_reach(
            float(midpoint_lon),
            float(midpoint_lat),
            reported_area=None,
        )

    return {
        "anchor_source": "midpoint_fallback",
        "endpoint_match_count": 0,
        "latitude": midpoint_lat,
        "longitude": midpoint_lon,
        "reach_info": reach_info,
    }
