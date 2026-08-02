#!/usr/bin/env python3
"""Link satellite rows to s5 main in-situ clusters with reach topology checks.

This v2 script is intentionally independent from the production s5b linkage.
It reads the s5 basin-clustered station table, builds one deterministic main
cluster representative per cluster/resolution, and links satellite rows only
when the MERIT reach evidence satisfies the configured rules.
"""

from __future__ import print_function

import argparse
import hashlib
import math
import os
import sys
import time
from collections import Counter, deque
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point
from shapely.ops import transform

from basin_tracer import UpstreamBasinTracer
from pipeline_paths import S5_BASIN_CLUSTERED_CSV, get_output_r_root

try:
    from source_family import classify_source_family
except ImportError:  # pragma: no cover - local tests normally import this.
    classify_source_family = None

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - CLI will raise a readable error.
    gpd = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_INPUT = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUTPUT_LINKS = PROJECT_ROOT / "scripts_basin_test/output/s5b_satellite_main_cluster_links_v2.csv"
DEFAULT_OUTPUT_CANDIDATES = PROJECT_ROOT / "scripts_basin_test/output/s5b_satellite_main_cluster_candidates_v2.csv"
DEFAULT_OUTPUT_REPORT = PROJECT_ROOT / "scripts_basin_test/output/s5b_satellite_main_cluster_report_v2.csv"
DEFAULT_MERIT_DIR = PROJECT_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"

SUPPORTED_RESOLUTIONS = ("daily", "monthly", "annual")
SATELLITE_SOURCE_FALLBACKS = frozenset(("riversed", "river_sed", "gsed", "dethier", "aquasat"))

DEFAULT_MAX_UPSTREAM_REL_ERROR = 0.10
DEFAULT_MAX_CROSS_REACH_DISTANCE_M = 1000.0
DEFAULT_MAX_TOPOLOGY_HOPS = 50
DEFAULT_AMBIGUOUS_DISTANCE_DELTA_M = 100.0
DEFAULT_AMBIGUOUS_AREA_DELTA = 0.01
DEFAULT_WORKERS = max(1, os.cpu_count() or 12)
DEFAULT_CHUNK_SIZE = 0

_TRANSFORMER_CACHE = {}

LINK_COLUMNS = [
    "satellite_station_id",
    "satellite_key",
    "satellite_source",
    "satellite_resolution",
    "satellite_lat",
    "satellite_lon",
    "satellite_comid",
    "satellite_uparea_merit",
    "satellite_pfaf_code",
    "link_status",
    "link_method",
    "link_confidence",
    "linked_cluster_id",
    "linked_cluster_uid",
    "linked_station_id",
    "linked_source",
    "linked_comid",
    "linked_uparea_merit",
    "linked_lat",
    "linked_lon",
    "same_reach",
    "topology_relation",
    "topology_hops",
    "area_rel_error",
    "satellite_point_to_insitu_reach_m",
    "insitu_point_to_satellite_reach_m",
    "representative_point_distance_m",
    "n_valid_candidates",
    "n_same_reach_candidates",
    "n_connected_candidates",
    "rejection_reason",
]

CANDIDATE_COLUMNS = [
    "satellite_key",
    "candidate_cluster_id",
    "candidate_cluster_uid",
    "candidate_comid",
    "same_resolution",
    "same_reach",
    "same_pfaf_region",
    "topology_relation",
    "topology_hops",
    "area_rel_error",
    "satellite_point_to_insitu_reach_m",
    "insitu_point_to_satellite_reach_m",
    "passes_resolution",
    "passes_reach_or_topology",
    "passes_area",
    "passes_distance",
    "accepted",
    "candidate_rank",
    "rejection_reason",
]


def clean_text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "" if text.lower() in ("nan", "none", "nat") else text


def normalize_resolution(value):
    text = clean_text(value).lower()
    return {
        "quarterly": "monthly",
        "single_point": "daily",
        "annually_climatology": "climatology",
    }.get(text, text)


def finite_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def finite_int(value):
    number = finite_float(value)
    if not math.isfinite(number):
        return None
    return int(number)


def symmetric_rel_error(left, right):
    left = finite_float(left)
    right = finite_float(right)
    if not (math.isfinite(left) and math.isfinite(right) and left > 0 and right > 0):
        return math.nan
    return abs(left - right) / max(abs(left), abs(right))


def haversine_distance_m(lat1, lon1, lat2, lon2):
    values = [finite_float(v) for v in (lat1, lon1, lat2, lon2)]
    if not all(math.isfinite(v) for v in values):
        return math.nan
    lat1, lon1, lat2, lon2 = values
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return 6371008.8 * 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def normalize_pfaf(value):
    text = clean_text(value)
    if not text:
        return ""
    number = finite_float(text)
    if math.isfinite(number) and float(number).is_integer():
        return str(int(number))
    return text.strip()


def same_pfaf_region(left, right):
    left = normalize_pfaf(left)
    right = normalize_pfaf(right)
    if not left or not right:
        return False
    if left == right or left.startswith(right) or right.startswith(left):
        return True
    width = min(len(left), len(right), 2)
    return width > 0 and left[:width] == right[:width]


def source_key(value):
    text = clean_text(value).lower()
    return "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")


def is_satellite_row(row):
    """Check if *row* represents a satellite dataset.

    Uses the shared source_family classifier.  Falls back to source-name
    heuristics (SATELLITE_SOURCE_FALLBACKS) when the classifier is unavailable.
    """
    source = clean_text(row.get("source", ""))
    if source and classify_source_family is not None:
        family = classify_source_family(source)
        return clean_text(family).lower() == "satellite"
    key = source_key(source)
    return any(token in key for token in SATELLITE_SOURCE_FALLBACKS)


def cluster_uid_from_id(cluster_id):
    value = finite_int(cluster_id)
    return "SED{:06d}".format(value) if value is not None and value >= 0 else ""


def satellite_key(row):
    station_id = clean_text(row.get("station_id", ""))
    source = clean_text(row.get("source", "")).lower()
    native = clean_text(row.get("source_station_id", ""))
    resolution = normalize_resolution(row.get("resolution", ""))
    payload = "\x1f".join([station_id, source, native, resolution])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return "SATV2{}".format(digest)


def point_to_geometry_distance_m(lat, lon, geometry):
    lat = finite_float(lat)
    lon = finite_float(lon)
    if not (math.isfinite(lat) and math.isfinite(lon)):
        return math.nan
    if geometry is None or getattr(geometry, "is_empty", True):
        return math.nan
    local_crs = UpstreamBasinTracer._get_local_metric_crs(None, lon, lat)
    cache_key = str(local_crs)
    transformer = _TRANSFORMER_CACHE.get(cache_key)
    if transformer is None:
        transformer = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)
        _TRANSFORMER_CACHE[cache_key] = transformer
    point_x, point_y = transformer.transform(lon, lat)
    point_proj = Point(point_x, point_y)
    geom_proj = transform(transformer.transform, geometry)
    return float(point_proj.distance(geom_proj))


def reach_file_code_from_pfaf_or_comid(pfaf_code, comid):
    pfaf = normalize_pfaf(pfaf_code)
    if pfaf:
        return pfaf[:2]
    comid_int = finite_int(comid)
    return str(abs(comid_int))[:2] if comid_int is not None else ""


class MeritReachNetwork:
    """Cached MERIT reach reader keyed by Pfaf region and COMID."""

    def __init__(self, merit_dir=None, reach_rows=None):
        self.merit_dir = Path(merit_dir).expanduser().resolve() if merit_dir else None
        self._pfaf_cache = {}
        self._reach_to_pfaf = {}
        self._downstream_cache = {}
        self._manual_reaches = {}
        if reach_rows:
            for reach_id, row in reach_rows.items():
                normalized = self._normalize_reach_row(reach_id, row, row.get("pfaf_code", ""))
                if normalized is not None:
                    self._manual_reaches[int(normalized["comid"])] = normalized
                    if normalized["pfaf_code"]:
                        self._reach_to_pfaf[int(normalized["comid"])] = normalized["pfaf_code"]
            self._build_downstream_index("_manual", self._manual_reaches)

    @staticmethod
    def _normalize_reach_row(reach_id, row, pfaf_code):
        comid = finite_int(row.get("COMID", row.get("comid", reach_id)))
        if comid is None:
            return None
        upstream = []
        for name in ("up1", "up2", "up3", "up4"):
            value = finite_int(row.get(name, 0))
            if value is not None and value > 0:
                upstream.append(int(value))
        geometry = row.get("geometry")
        return {
            "comid": int(comid),
            "pfaf_code": normalize_pfaf(row.get("pfaf_code", pfaf_code)),
            "uparea": finite_float(row.get("uparea", row.get("uparea_merit", math.nan))),
            "upstream_ids": tuple(upstream),
            "geometry": geometry,
        }

    def _load_pfaf(self, pfaf_code):
        pfaf_code = normalize_pfaf(pfaf_code)
        if not pfaf_code:
            return {}
        if pfaf_code in self._pfaf_cache:
            return self._pfaf_cache[pfaf_code]
        if gpd is None:
            raise RuntimeError("geopandas is required to read MERIT-Basins river shapefiles")
        if self.merit_dir is None:
            raise RuntimeError("MERIT-Basins directory is not configured")
        path = (
            self.merit_dir
            / "pfaf_level_01"
            / "riv_pfaf_{}_MERIT_Hydro_v07_Basins_v01_bugfix1.shp".format(pfaf_code)
        )
        if not path.is_file():
            self._pfaf_cache[pfaf_code] = {}
            return self._pfaf_cache[pfaf_code]
        frame = gpd.read_file(path)
        if frame.crs is None:
            frame = frame.set_crs("EPSG:4326")
        required = {"COMID", "uparea", "geometry"}
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError("{} missing MERIT columns: {}".format(path, ", ".join(missing)))
        reaches = {}
        for _, row in frame.iterrows():
            normalized = self._normalize_reach_row(None, row, pfaf_code)
            if normalized is not None:
                reaches[int(normalized["comid"])] = normalized
                self._reach_to_pfaf[int(normalized["comid"])] = pfaf_code
        self._build_downstream_index(pfaf_code, reaches)
        self._pfaf_cache[pfaf_code] = reaches
        return reaches

    def _build_downstream_index(self, pfaf_code, reaches):
        downstream = {}
        for comid, row in reaches.items():
            downstream.setdefault(int(comid), set())
            for upstream_id in row.get("upstream_ids", ()):
                downstream.setdefault(int(upstream_id), set()).add(int(comid))
        self._downstream_cache[normalize_pfaf(pfaf_code) or pfaf_code] = {
            reach_id: tuple(sorted(values)) for reach_id, values in downstream.items()
        }

    def _pfaf_for_comid(self, comid, pfaf_code=""):
        comid_int = finite_int(comid)
        if comid_int is None:
            return ""
        pfaf = normalize_pfaf(pfaf_code) or self._reach_to_pfaf.get(comid_int, "")
        if not pfaf:
            pfaf = reach_file_code_from_pfaf_or_comid("", comid_int)
        return pfaf

    def _reaches_for_comid(self, comid, pfaf_code=""):
        comid_int = finite_int(comid)
        if comid_int is None:
            return "", {}
        if comid_int in self._manual_reaches:
            return "_manual", self._manual_reaches
        pfaf = self._pfaf_for_comid(comid_int, pfaf_code)
        return pfaf, self._load_pfaf(pfaf)

    def get(self, comid, pfaf_code=""):
        comid_int = finite_int(comid)
        if comid_int is None:
            return None
        if comid_int in self._manual_reaches:
            return self._manual_reaches[comid_int]
        _, reaches = self._reaches_for_comid(comid_int, pfaf_code)
        return reaches.get(comid_int)

    def connected_reach_maps(self, satellite_comid, satellite_pfaf, max_hops):
        """Return reachable COMIDs keyed to topology relation and hop count."""
        start = finite_int(satellite_comid)
        if start is None:
            return {}, False
        pfaf, reaches = self._reaches_for_comid(start, satellite_pfaf)
        if start not in reaches:
            return {}, False
        downstream = self._downstream_cache.get(normalize_pfaf(pfaf) or pfaf, {})
        connected = {}

        def walk(seed_ids, relation):
            frontier = deque((int(item), 1) for item in sorted(seed_ids))
            seen = {start}
            while frontier:
                current, hops = frontier.popleft()
                if current in seen:
                    continue
                seen.add(current)
                previous = connected.get(current)
                if previous is None or hops < previous[1]:
                    connected[current] = (relation, int(hops))
                if hops >= int(max_hops):
                    continue
                reach = reaches.get(current)
                if reach is None:
                    continue
                if relation == "satellite_downstream":
                    next_ids = reach.get("upstream_ids", ())
                else:
                    next_ids = downstream.get(current, ())
                for next_id in sorted(next_ids):
                    if int(next_id) not in seen:
                        frontier.append((int(next_id), hops + 1))

        start_reach = reaches.get(start)
        walk(start_reach.get("upstream_ids", ()), "satellite_downstream")
        walk(downstream.get(start, ()), "satellite_upstream")
        return connected, True

    def topology_relation(self, satellite_comid, insitu_comid, satellite_pfaf, insitu_pfaf, max_hops):
        satellite_comid = finite_int(satellite_comid)
        insitu_comid = finite_int(insitu_comid)
        if satellite_comid is None or insitu_comid is None:
            return "unknown", pd.NA
        if satellite_comid == insitu_comid:
            return "same_reach", 0
        sat = self.get(satellite_comid, satellite_pfaf)
        insitu = self.get(insitu_comid, insitu_pfaf)
        if sat is None or insitu is None:
            return "unknown", pd.NA
        up_hops = self._upstream_hops(start_downstream=insitu_comid, target_upstream=satellite_comid, pfaf_code=insitu_pfaf, max_hops=max_hops)
        if up_hops == "max_topology_hops_reached":
            return "unknown", pd.NA
        if up_hops is not None:
            return "satellite_upstream", int(up_hops)
        down_hops = self._upstream_hops(start_downstream=satellite_comid, target_upstream=insitu_comid, pfaf_code=satellite_pfaf, max_hops=max_hops)
        if down_hops == "max_topology_hops_reached":
            return "unknown", pd.NA
        if down_hops is not None:
            return "satellite_downstream", int(down_hops)
        return "not_connected", pd.NA

    def _upstream_hops(self, start_downstream, target_upstream, pfaf_code, max_hops):
        start = finite_int(start_downstream)
        target = finite_int(target_upstream)
        if start is None or target is None:
            return None
        frontier = deque([(start, 0)])
        seen = {start}
        reached_limit = False
        while frontier:
            current, hops = frontier.popleft()
            if hops >= int(max_hops):
                reached_limit = True
                continue
            reach = self.get(current, pfaf_code)
            if reach is None:
                continue
            for upstream_id in sorted(reach["upstream_ids"]):
                if upstream_id == target:
                    return hops + 1
                if upstream_id not in seen:
                    seen.add(upstream_id)
                    frontier.append((upstream_id, hops + 1))
        if reached_limit:
            return "max_topology_hops_reached"
        return None


def validate_station_columns(stations):
    required = {
        "station_id",
        "source",
        "lat",
        "lon",
        "resolution",
        "observation_type",
        "cluster_id",
        "basin_id",
        "basin_status",
        "uparea_merit",
        "pfaf_code",
    }
    missing = sorted(required - set(stations.columns))
    if missing:
        raise ValueError("s5 input missing required columns: {}".format(", ".join(missing)))


def representative_row(group):
    work = group.copy()
    work["_station_sort"] = pd.to_numeric(work["station_id"], errors="coerce")
    work["_cluster_sort"] = pd.to_numeric(work["cluster_id"], errors="coerce")
    exact = work[work["_station_sort"].eq(work["_cluster_sort"])]
    if not exact.empty:
        work = exact
    return work.sort_values(["_station_sort", "source", "source_station_id"], kind="mergesort").iloc[0]


def build_main_clusters(stations):
    rows = []
    work = stations.copy()
    work["_is_satellite"] = work.apply(is_satellite_row, axis=1)
    work["_resolution_norm"] = work["resolution"].map(normalize_resolution)
    work["_cluster_id_num"] = pd.to_numeric(work["cluster_id"], errors="coerce")
    grouped = work.loc[~work["_is_satellite"]].groupby(
        ["_resolution_norm", "_cluster_id_num"], sort=True, dropna=True
    )
    for (resolution, cluster_id), group in grouped:
        if resolution not in SUPPORTED_RESOLUTIONS:
            continue
        resolved = group["basin_status"].fillna("").astype(str).str.strip().str.lower().eq("resolved")
        valid = group.loc[resolved].copy()
        valid["basin_id_num"] = pd.to_numeric(valid["basin_id"], errors="coerce")
        valid = valid.dropna(subset=["basin_id_num"])
        if valid.empty:
            continue
        rep = representative_row(valid)
        comid = finite_int(rep.get("basin_id"))
        if comid is None:
            continue
        rows.append(
            {
                "cluster_id": int(cluster_id),
                "cluster_uid": cluster_uid_from_id(cluster_id),
                "station_id": int(finite_int(rep.get("station_id")) or int(cluster_id)),
                "source": clean_text(rep.get("source", "")),
                "resolution": resolution,
                "lat": finite_float(rep.get("lat")),
                "lon": finite_float(rep.get("lon")),
                "comid": int(comid),
                "uparea_merit": finite_float(rep.get("uparea_merit")),
                "pfaf_code": normalize_pfaf(rep.get("pfaf_code")),
            }
        )
    return sorted(rows, key=lambda item: (item["resolution"], item["pfaf_code"], item["comid"], item["cluster_id"]))


def base_link_row(sat):
    return {
        "satellite_station_id": sat["station_id"],
        "satellite_key": sat["satellite_key"],
        "satellite_source": sat["source"],
        "satellite_resolution": sat["resolution"],
        "satellite_lat": sat["lat"],
        "satellite_lon": sat["lon"],
        "satellite_comid": sat["comid"] if sat["comid"] is not None else pd.NA,
        "satellite_uparea_merit": sat["uparea_merit"],
        "satellite_pfaf_code": sat["pfaf_code"],
        "link_status": "unlinked",
        "link_method": "",
        "link_confidence": "low",
        "linked_cluster_id": pd.NA,
        "linked_cluster_uid": "",
        "linked_station_id": pd.NA,
        "linked_source": "",
        "linked_comid": pd.NA,
        "linked_uparea_merit": math.nan,
        "linked_lat": math.nan,
        "linked_lon": math.nan,
        "same_reach": pd.NA,
        "topology_relation": "unknown",
        "topology_hops": pd.NA,
        "area_rel_error": math.nan,
        "satellite_point_to_insitu_reach_m": math.nan,
        "insitu_point_to_satellite_reach_m": math.nan,
        "representative_point_distance_m": math.nan,
        "n_valid_candidates": 0,
        "n_same_reach_candidates": 0,
        "n_connected_candidates": 0,
        "rejection_reason": "",
    }


def candidate_row_template(sat, cluster):
    return {
        "satellite_key": sat["satellite_key"],
        "candidate_cluster_id": cluster["cluster_id"],
        "candidate_cluster_uid": cluster["cluster_uid"],
        "candidate_comid": cluster["comid"],
        "same_resolution": sat["resolution"] == cluster["resolution"],
        "same_reach": sat["comid"] == cluster["comid"],
        "same_pfaf_region": same_pfaf_region(sat["pfaf_code"], cluster["pfaf_code"]),
        "topology_relation": "unknown",
        "topology_hops": pd.NA,
        "area_rel_error": symmetric_rel_error(sat["uparea_merit"], cluster["uparea_merit"]),
        "satellite_point_to_insitu_reach_m": math.nan,
        "insitu_point_to_satellite_reach_m": math.nan,
        "passes_resolution": sat["resolution"] == cluster["resolution"],
        "passes_reach_or_topology": False,
        "passes_area": False,
        "passes_distance": False,
        "accepted": False,
        "candidate_rank": pd.NA,
        "rejection_reason": "",
    }


def fill_link_from_candidate(base, sat, candidate, row, status, method, confidence, reason=""):
    out = dict(base)
    out.update(
        {
            "link_status": status,
            "link_method": method,
            "link_confidence": confidence,
            "linked_cluster_id": candidate["cluster_id"],
            "linked_cluster_uid": candidate["cluster_uid"],
            "linked_station_id": candidate["station_id"],
            "linked_source": candidate["source"],
            "linked_comid": candidate["comid"],
            "linked_uparea_merit": candidate["uparea_merit"],
            "linked_lat": candidate["lat"],
            "linked_lon": candidate["lon"],
            "same_reach": bool(row["same_reach"]),
            "topology_relation": row["topology_relation"],
            "topology_hops": row["topology_hops"],
            "area_rel_error": row["area_rel_error"],
            "satellite_point_to_insitu_reach_m": row["satellite_point_to_insitu_reach_m"],
            "insitu_point_to_satellite_reach_m": row["insitu_point_to_satellite_reach_m"],
            "representative_point_distance_m": haversine_distance_m(
                sat["lat"], sat["lon"], candidate["lat"], candidate["lon"]
            ),
            "rejection_reason": reason,
        }
    )
    return out


def candidate_sort_key(row):
    return (
        int(row["topology_hops"]) if not pd.isna(row["topology_hops"]) else 10**9,
        max(
            finite_float(row["satellite_point_to_insitu_reach_m"]),
            finite_float(row["insitu_point_to_satellite_reach_m"]),
        ),
        finite_float(row["area_rel_error"]) if math.isfinite(finite_float(row["area_rel_error"])) else math.inf,
        int(row["candidate_cluster_id"]),
    )


def candidates_ambiguous(left, right, max_distance_delta_m, max_area_delta):
    if int(left["topology_hops"]) != int(right["topology_hops"]):
        return False
    left_dist = max(
        finite_float(left["satellite_point_to_insitu_reach_m"]),
        finite_float(left["insitu_point_to_satellite_reach_m"]),
    )
    right_dist = max(
        finite_float(right["satellite_point_to_insitu_reach_m"]),
        finite_float(right["insitu_point_to_satellite_reach_m"]),
    )
    left_area = finite_float(left["area_rel_error"])
    right_area = finite_float(right["area_rel_error"])
    return abs(left_dist - right_dist) <= float(max_distance_delta_m) and abs(left_area - right_area) <= float(max_area_delta)


def rejection_reason_for_rule2(row):
    if not row["passes_resolution"]:
        return "resolution_mismatch"
    if not row["same_pfaf_region"]:
        return "different_pfaf_region"
    if row["topology_relation"] == "unknown":
        return "topology_unknown"
    if row["topology_relation"] == "not_connected":
        return "not_topology_connected"
    area = finite_float(row["area_rel_error"])
    if not math.isfinite(area):
        return "missing_upstream_area"
    if not row["passes_area"]:
        return "area_mismatch"
    d1 = finite_float(row["satellite_point_to_insitu_reach_m"])
    d2 = finite_float(row["insitu_point_to_satellite_reach_m"])
    if not (math.isfinite(d1) and math.isfinite(d2)):
        return "missing_geometry"
    if not row["passes_distance"]:
        return "distance_mismatch"
    return ""


def evaluate_candidate(sat, cluster, network, args, topology=None, compute_distance=True):
    row = candidate_row_template(sat, cluster)
    if topology is None:
        relation, hops = network.topology_relation(
            sat["comid"],
            cluster["comid"],
            sat["pfaf_code"],
            cluster["pfaf_code"],
            args["max_topology_hops"],
        )
    else:
        relation, hops = topology
    row["topology_relation"] = relation
    row["topology_hops"] = hops
    if compute_distance:
        sat_reach = network.get(sat["comid"], sat["pfaf_code"])
        cluster_reach = network.get(cluster["comid"], cluster["pfaf_code"])
        if cluster_reach is not None:
            row["satellite_point_to_insitu_reach_m"] = point_to_geometry_distance_m(
                sat["lat"], sat["lon"], cluster_reach.get("geometry")
            )
        if sat_reach is not None:
            row["insitu_point_to_satellite_reach_m"] = point_to_geometry_distance_m(
                cluster["lat"], cluster["lon"], sat_reach.get("geometry")
            )
    row["representative_point_distance_m"] = haversine_distance_m(
        sat["lat"], sat["lon"], cluster["lat"], cluster["lon"]
    )
    row["passes_reach_or_topology"] = row["same_reach"] or relation in {"satellite_upstream", "satellite_downstream"}
    area = finite_float(row["area_rel_error"])
    row["passes_area"] = math.isfinite(area) and area <= float(args["max_upstream_rel_error"])
    d1 = finite_float(row["satellite_point_to_insitu_reach_m"])
    d2 = finite_float(row["insitu_point_to_satellite_reach_m"])
    row["passes_distance"] = (
        math.isfinite(d1)
        and math.isfinite(d2)
        and d1 <= float(args["max_cross_reach_distance_m"])
        and d2 <= float(args["max_cross_reach_distance_m"])
    )
    if row["same_reach"]:
        row["passes_reach_or_topology"] = True
        row["accepted"] = row["passes_resolution"]
        row["rejection_reason"] = "" if row["accepted"] else "resolution_mismatch"
    else:
        row["accepted"] = (
            row["passes_resolution"]
            and row["same_pfaf_region"]
            and row["passes_reach_or_topology"]
            and row["passes_area"]
            and row["passes_distance"]
        )
        row["rejection_reason"] = rejection_reason_for_rule2(row)
    return row



def _sorted_clusters_by_reach(clusters_by_reach, resolution, reachable):
    clusters = []
    for comid in sorted(reachable):
        clusters.extend(clusters_by_reach.get((resolution, int(comid)), []))
    return sorted(clusters, key=lambda item: (int(item["cluster_id"]), int(item["comid"])))


def _first_diagnostic_candidate(sat, candidate_clusters, max_upstream_rel_error, topology=("not_connected", pd.NA)):
    if not candidate_clusters:
        return None
    cluster = sorted(candidate_clusters, key=lambda item: (int(item["cluster_id"]), int(item["comid"])))[0]
    row = candidate_row_template(sat, cluster)
    row["topology_relation"], row["topology_hops"] = topology
    row["passes_reach_or_topology"] = row["same_reach"] or row["topology_relation"] in {
        "satellite_upstream",
        "satellite_downstream",
    }
    area = finite_float(row["area_rel_error"])
    row["passes_area"] = math.isfinite(area) and area <= float(max_upstream_rel_error)
    row["rejection_reason"] = rejection_reason_for_rule2(row)
    return row


def _process_satellite_rows(
    sat_batch,
    network,
    clusters_by_resolution,
    same_reach_index,
    pfaf_index,
    clusters_by_reach,
    args_dict,
    ambiguous_distance_delta_m,
    ambiguous_area_delta,
    full_candidate_audit=False,
):
    links = []
    candidate_rows = []
    stats = Counter()
    for sat in sat_batch:
        base = base_link_row(sat)
        if not (math.isfinite(sat["lat"]) and math.isfinite(sat["lon"])):
            base["rejection_reason"] = "missing_coordinates"
            links.append(base)
            continue
        if sat["basin_status"] != "resolved" or sat["comid"] is None:
            base["rejection_reason"] = "unresolved_reach"
            links.append(base)
            continue

        same_clusters = same_reach_index.get((sat["resolution"], int(sat["comid"])), [])
        evaluated_same = [evaluate_candidate(sat, cluster, network, args_dict) for cluster in same_clusters]
        stats["candidate_pairs_considered"] += len(evaluated_same)
        stats["distance_evaluations"] += 2 * len(evaluated_same)
        for row in evaluated_same:
            row["accepted"] = bool(row["accepted"])
        if evaluated_same:
            evaluated_same.sort(
                key=lambda row: (
                    finite_float(row["representative_point_distance_m"]),
                    finite_float(row["area_rel_error"]) if math.isfinite(finite_float(row["area_rel_error"])) else math.inf,
                    int(row["candidate_cluster_id"]),
                )
            )
            for rank, row in enumerate(evaluated_same, start=1):
                row["candidate_rank"] = rank
                row["accepted"] = rank == 1
                candidate_rows.append(row)
            best = evaluated_same[0]
            best_cluster = next(cluster for cluster in same_clusters if cluster["cluster_id"] == best["candidate_cluster_id"])
            out = fill_link_from_candidate(base, sat, best_cluster, best, "linked", "same_reach", "high")
            out["n_valid_candidates"] = len(evaluated_same)
            out["n_same_reach_candidates"] = len(evaluated_same)
            out["n_connected_candidates"] = 0
            links.append(out)
            continue

        pfaf_key = reach_file_code_from_pfaf_or_comid(sat["pfaf_code"], sat["comid"])
        candidate_clusters = [
            cluster for cluster in pfaf_index.get((sat["resolution"], pfaf_key), [])
            if cluster["comid"] != sat["comid"]
        ]
        candidate_lookup = {int(cluster["cluster_id"]): cluster for cluster in candidate_clusters}
        if not candidate_clusters and not clusters_by_resolution.get(sat["resolution"], []):
            base["rejection_reason"] = "no_same_resolution_main_coverage"
            links.append(base)
            continue

        if full_candidate_audit:
            evaluated = [evaluate_candidate(sat, cluster, network, args_dict) for cluster in candidate_clusters]
            stats["candidate_pairs_considered"] += len(evaluated)
            stats["topology_prefilter_candidates"] += len(evaluated)
            stats["distance_evaluations"] += 2 * len(evaluated)
        else:
            connected, sat_reach_known = network.connected_reach_maps(
                sat["comid"], sat["pfaf_code"], args_dict["max_topology_hops"]
            )
            reachable_comids = set(connected)
            pre_candidates = [
                cluster for cluster in _sorted_clusters_by_reach(clusters_by_reach, sat["resolution"], reachable_comids)
                if cluster["comid"] != sat["comid"] and same_pfaf_region(sat["pfaf_code"], cluster["pfaf_code"])
            ]
            for cluster in pre_candidates:
                candidate_lookup[int(cluster["cluster_id"])] = cluster
            stats["topology_prefilter_candidates"] += len(pre_candidates)
            evaluated = []
            diagnostic = None
            for cluster in pre_candidates:
                relation, hops = connected.get(cluster["comid"], ("not_connected", pd.NA))
                row = evaluate_candidate(
                    sat,
                    cluster,
                    network,
                    args_dict,
                    topology=(relation, hops),
                    compute_distance=False,
                )
                stats["candidate_pairs_considered"] += 1
                area = finite_float(row["area_rel_error"])
                row["passes_area"] = math.isfinite(area) and area <= float(args_dict["max_upstream_rel_error"])
                if not row["passes_area"]:
                    row["rejection_reason"] = rejection_reason_for_rule2(row)
                    if diagnostic is None:
                        diagnostic = row
                    continue
                row = evaluate_candidate(
                    sat,
                    cluster,
                    network,
                    args_dict,
                    topology=(relation, hops),
                    compute_distance=True,
                )
                stats["distance_evaluations"] += 2
                evaluated.append(row)
                if not row["accepted"] and diagnostic is None:
                    diagnostic = row
            if not evaluated and diagnostic is None and candidate_clusters:
                if not sat_reach_known:
                    diagnostic = _first_diagnostic_candidate(
                        sat,
                        candidate_clusters,
                        args_dict["max_upstream_rel_error"],
                        topology=("unknown", pd.NA),
                    )
                else:
                    diagnostic = _first_diagnostic_candidate(
                        sat,
                        candidate_clusters,
                        args_dict["max_upstream_rel_error"],
                    )
            if diagnostic is not None:
                evaluated.append(diagnostic)

        valid = [row for row in evaluated if row["accepted"]]
        valid.sort(key=candidate_sort_key)
        valid_ids = {row["candidate_cluster_id"]: rank for rank, row in enumerate(valid, start=1)}
        wrote_failure_diagnostic = False
        for row in sorted(evaluated, key=lambda item: (item["candidate_cluster_id"], item["candidate_comid"])):
            if row["candidate_cluster_id"] in valid_ids:
                row["candidate_rank"] = valid_ids[row["candidate_cluster_id"]]
            if full_candidate_audit or row["accepted"]:
                candidate_rows.append(row)
            elif not valid and not wrote_failure_diagnostic:
                candidate_rows.append(row)
                wrote_failure_diagnostic = True

        base["n_valid_candidates"] = len(valid)
        base["n_same_reach_candidates"] = 0
        base["n_connected_candidates"] = len(valid)
        if not valid:
            reasons = [clean_text(row["rejection_reason"]) for row in evaluated if clean_text(row["rejection_reason"])]
            base["rejection_reason"] = Counter(reasons).most_common(1)[0][0] if reasons else "no_same_resolution_main_coverage"
            links.append(base)
            continue

        if len(valid) >= 2 and candidates_ambiguous(
            valid[0],
            valid[1],
            max_distance_delta_m=ambiguous_distance_delta_m,
            max_area_delta=ambiguous_area_delta,
        ):
            best = valid[0]
            best_cluster = candidate_lookup[int(best["candidate_cluster_id"])]
            out = fill_link_from_candidate(
                base,
                sat,
                best_cluster,
                best,
                "ambiguous",
                "multiple_valid_candidates",
                "low",
                reason="multiple_candidates",
            )
            out["n_valid_candidates"] = len(valid)
            out["n_connected_candidates"] = len(valid)
            links.append(out)
            continue

        best = valid[0]
        best_cluster = candidate_lookup[int(best["candidate_cluster_id"])]
        out = fill_link_from_candidate(
            base,
            sat,
            best_cluster,
            best,
            "linked",
            "connected_nearby_reach",
            "medium",
        )
        out["n_valid_candidates"] = len(valid)
        out["n_connected_candidates"] = len(valid)
        links.append(out)

    stats["satellite_rows_processed"] += len(sat_batch)
    stats["candidate_audit_rows"] += len(candidate_rows)
    return links, candidate_rows, stats


def _process_satellite_data_batch(
    merit_dir_str,
    sat_batch,
    clusters_by_resolution,
    same_reach_index,
    pfaf_index,
    clusters_by_reach,
    args_dict,
    ambiguous_distance_delta_m,
    ambiguous_area_delta,
    full_candidate_audit,
):
    """Worker function: process a batch of satellite row dicts within one process.

    Each worker creates its own MeritReachNetwork (loads shapefiles
    independently) so there are no thread-safety concerns with the
    lazy-fill caches.  Returns (links_list, candidate_rows_list).
    """
    network = MeritReachNetwork(merit_dir=merit_dir_str)
    return _process_satellite_rows(
        sat_batch,
        network,
        clusters_by_resolution,
        same_reach_index,
        pfaf_index,
        clusters_by_reach,
        args_dict,
        ambiguous_distance_delta_m,
        ambiguous_area_delta,
        full_candidate_audit=full_candidate_audit,
    )


def _link_satellite_parallel(
    sat_rows,
    clusters_by_resolution,
    same_reach_index,
    pfaf_index,
    clusters_by_reach,
    args_dict,
    allowed_resolutions,
    ambiguous_distance_delta_m,
    ambiguous_area_delta,
    merit_dir,
    workers,
    chunk_size=DEFAULT_CHUNK_SIZE,
    full_candidate_audit=False,
):
    """Dispatch satellite-row processing across a process pool.

    Each worker gets its own MeritReachNetwork so lazy-fill caches are
    process-local and safe.  Returns (links_df, candidates_df, report_df).
    """
    # Convert DataFrame rows to list of plain dicts for safe pickling
    sat_dicts = []
    for _, raw in sat_rows.iterrows():
        sat = {
            "station_id": int(finite_int(raw.get("station_id")) or -1),
            "satellite_key": satellite_key(raw),
            "source": clean_text(raw.get("source", "")),
            "resolution": normalize_resolution(raw.get("resolution", "")),
            "lat": finite_float(raw.get("lat")),
            "lon": finite_float(raw.get("lon")),
            "comid": finite_int(raw.get("basin_id")),
            "uparea_merit": finite_float(raw.get("uparea_merit")),
            "pfaf_code": normalize_pfaf(raw.get("pfaf_code")),
            "basin_status": clean_text(raw.get("basin_status", "")).lower(),
        }
        sat_dicts.append(sat)

    n_tasks = len(sat_dicts)
    if n_tasks == 0:
        empty_links = pd.DataFrame([], columns=LINK_COLUMNS)
        empty_candidates = pd.DataFrame([], columns=CANDIDATE_COLUMNS)
        return empty_links, empty_candidates, build_report(empty_links, empty_candidates)

    actual_workers = max(1, min(workers, n_tasks))
    if chunk_size and chunk_size > 0:
        actual_chunk_size = int(chunk_size)
    else:
        actual_chunk_size = max(16, min(512, int(math.ceil(n_tasks / max(1, actual_workers * 4)))))
    chunks = [sat_dicts[i:i + actual_chunk_size] for i in range(0, n_tasks, actual_chunk_size)]
    merit_dir_str = str(merit_dir)
    links = []
    candidate_rows = []
    stats = Counter()

    with ProcessPoolExecutor(max_workers=actual_workers) as executor:
        futures = {}
        for chunk in chunks:
            fut = executor.submit(
                _process_satellite_data_batch,
                merit_dir_str,
                chunk,
                clusters_by_resolution,
                same_reach_index,
                pfaf_index,
                clusters_by_reach,
                args_dict,
                ambiguous_distance_delta_m,
                ambiguous_area_delta,
                full_candidate_audit,
            )
            futures[fut] = len(chunk)

        with tqdm(total=n_tasks, desc="Linking satellite rows", unit="row") as pbar:
            for future in as_completed(futures):
                chunk_links, chunk_candidates, chunk_stats = future.result()
                links.extend(chunk_links)
                candidate_rows.extend(chunk_candidates)
                stats.update(chunk_stats)
                pbar.update(len(chunk_links))
                pbar.set_postfix(
                    linked=sum(1 for row in links if row["link_status"] == "linked"),
                    cand=int(stats["candidate_pairs_considered"]),
                    dist=int(stats["distance_evaluations"]),
                )

    links_df = pd.DataFrame(links, columns=LINK_COLUMNS)
    candidates_df = pd.DataFrame(candidate_rows, columns=CANDIDATE_COLUMNS)
    report_df = build_report(links_df, candidates_df, stats=stats)
    return links_df, candidates_df, report_df


def link_satellite_to_main_clusters_v2(
    stations,
    reach_network,
    resolutions=SUPPORTED_RESOLUTIONS,
    max_upstream_rel_error=DEFAULT_MAX_UPSTREAM_REL_ERROR,
    max_cross_reach_distance_m=DEFAULT_MAX_CROSS_REACH_DISTANCE_M,
    max_topology_hops=DEFAULT_MAX_TOPOLOGY_HOPS,
    ambiguous_distance_delta_m=DEFAULT_AMBIGUOUS_DISTANCE_DELTA_M,
    ambiguous_area_delta=DEFAULT_AMBIGUOUS_AREA_DELTA,
    workers=24,
    merit_dir=None,
    chunk_size=DEFAULT_CHUNK_SIZE,
    full_candidate_audit=False,
):
    validate_station_columns(stations)
    allowed_resolutions = tuple(normalize_resolution(item) for item in resolutions)
    args = {
        "max_upstream_rel_error": float(max_upstream_rel_error),
        "max_cross_reach_distance_m": float(max_cross_reach_distance_m),
        "max_topology_hops": int(max_topology_hops),
    }
    clusters = build_main_clusters(stations)
    clusters_by_resolution = {}
    same_reach_index = {}
    pfaf_index = {}
    clusters_by_reach = {}
    for cluster in clusters:
        clusters_by_resolution.setdefault(cluster["resolution"], []).append(cluster)
        same_reach_index.setdefault((cluster["resolution"], cluster["comid"]), []).append(cluster)
        clusters_by_reach.setdefault((cluster["resolution"], cluster["comid"]), []).append(cluster)
        pfaf_key = reach_file_code_from_pfaf_or_comid(cluster["pfaf_code"], cluster["comid"])
        pfaf_index.setdefault((cluster["resolution"], pfaf_key), []).append(cluster)

    work = stations.copy()
    work["_is_satellite"] = work.apply(is_satellite_row, axis=1)
    work["_resolution_norm"] = work["resolution"].map(normalize_resolution)
    sat_rows = work.loc[work["_is_satellite"] & work["_resolution_norm"].isin(allowed_resolutions)].copy()
    sat_rows["_station_sort"] = pd.to_numeric(sat_rows["station_id"], errors="coerce")
    sat_rows = sat_rows.sort_values(["_resolution_norm", "_station_sort", "source"], kind="mergesort")

    if workers > 1 and merit_dir is not None:
        return _link_satellite_parallel(
            sat_rows,
            clusters_by_resolution,
            same_reach_index,
            pfaf_index,
            clusters_by_reach,
            args,
            allowed_resolutions,
            ambiguous_distance_delta_m,
            ambiguous_area_delta,
            merit_dir,
            workers,
            chunk_size=chunk_size,
            full_candidate_audit=full_candidate_audit,
        )
    sat_dicts = []
    for _, raw in sat_rows.iterrows():
        sat = {
            "station_id": int(finite_int(raw.get("station_id")) or -1),
            "satellite_key": satellite_key(raw),
            "source": clean_text(raw.get("source", "")),
            "resolution": normalize_resolution(raw.get("resolution", "")),
            "lat": finite_float(raw.get("lat")),
            "lon": finite_float(raw.get("lon")),
            "comid": finite_int(raw.get("basin_id")),
            "uparea_merit": finite_float(raw.get("uparea_merit")),
            "pfaf_code": normalize_pfaf(raw.get("pfaf_code")),
            "basin_status": clean_text(raw.get("basin_status", "")).lower(),
        }
        sat_dicts.append(sat)

    links, candidate_rows, stats = _process_satellite_rows(
        sat_dicts,
        reach_network,
        clusters_by_resolution,
        same_reach_index,
        pfaf_index,
        clusters_by_reach,
        args,
        ambiguous_distance_delta_m,
        ambiguous_area_delta,
        full_candidate_audit=full_candidate_audit,
    )

    links_df = pd.DataFrame(links, columns=LINK_COLUMNS)
    candidates_df = pd.DataFrame(candidate_rows, columns=CANDIDATE_COLUMNS)
    report_df = build_report(links_df, candidates_df, stats=stats)
    return links_df, candidates_df, report_df


def build_report(links, candidates, stats=None):
    rows = []
    stats = Counter(stats or {})

    def add(group, metric, count):
        rows.append({"group": group, "metric": metric, "count": int(count)})

    add("overall", "total_satellite_objects", len(links))
    add("overall", "resolved_satellite_reaches", int(links["satellite_comid"].notna().sum()) if not links.empty else 0)
    add("overall", "same_reach_linked", int(links["link_method"].eq("same_reach").sum()) if not links.empty else 0)
    add("overall", "connected_nearby_reach_linked", int(links["link_method"].eq("connected_nearby_reach").sum()) if not links.empty else 0)
    add("overall", "ambiguous", int(links["link_status"].eq("ambiguous").sum()) if not links.empty else 0)
    add("overall", "unlinked", int(links["link_status"].eq("unlinked").sum()) if not links.empty else 0)
    if not links.empty:
        for reason, count in links["rejection_reason"].fillna("").replace("", "none").value_counts().sort_index().items():
            add("overall", "unlinked_by_reason:{}".format(reason), count)
        for source, group in links.groupby("satellite_source", sort=True):
            source_name = clean_text(source) or "unknown"
            add("source:{}".format(source_name), "total_satellite_objects", len(group))
            add("source:{}".format(source_name), "linked", int(group["link_status"].eq("linked").sum()))
            add("source:{}".format(source_name), "ambiguous", int(group["link_status"].eq("ambiguous").sum()))
            add("source:{}".format(source_name), "unlinked", int(group["link_status"].eq("unlinked").sum()))
    if not candidates.empty:
        for reason, count in candidates["rejection_reason"].fillna("").replace("", "accepted_or_same_reach").value_counts().sort_index().items():
            add("candidates", "candidate_reason:{}".format(reason), count)
    for metric in (
        "satellite_rows_processed",
        "candidate_pairs_considered",
        "topology_prefilter_candidates",
        "distance_evaluations",
        "candidate_audit_rows",
    ):
        if metric in stats:
            add("performance", metric, stats[metric])
    return pd.DataFrame(rows, columns=["group", "metric", "count"])


def resolve_merit_dir(cli_value):
    candidates = []
    if cli_value:
        candidates.append(Path(cli_value).expanduser())
    env_value = os.environ.get("MERIT_DIR") or os.environ.get("MERIT_BASINS_DIR")
    if env_value:
        candidates.append(Path(env_value).expanduser())
    candidates.append(DEFAULT_MERIT_DIR)
    for path in candidates:
        resolved = path.resolve()
        if (resolved / "pfaf_level_01").is_dir():
            return resolved
    tried = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "MERIT-Basins directory could not be resolved; expected pfaf_level_01 under one of: {}".format(tried)
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-links", default=str(DEFAULT_OUTPUT_LINKS))
    parser.add_argument("--output-candidates", default=str(DEFAULT_OUTPUT_CANDIDATES))
    parser.add_argument("--output-report", default=str(DEFAULT_OUTPUT_REPORT))
    parser.add_argument("--merit-basins-dir", default="")
    parser.add_argument("--max-upstream-rel-error", type=float, default=DEFAULT_MAX_UPSTREAM_REL_ERROR)
    parser.add_argument("--max-cross-reach-distance-m", type=float, default=DEFAULT_MAX_CROSS_REACH_DISTANCE_M)
    parser.add_argument("--max-topology-hops", type=int, default=DEFAULT_MAX_TOPOLOGY_HOPS)
    parser.add_argument("--ambiguous-distance-delta-m", type=float, default=DEFAULT_AMBIGUOUS_DISTANCE_DELTA_M)
    parser.add_argument("--ambiguous-area-delta", type=float, default=DEFAULT_AMBIGUOUS_AREA_DELTA)
    parser.add_argument("--resolution", nargs="+", default=list(SUPPORTED_RESOLUTIONS))
    # --overwrite is always True by default (safe for test/development)
    parser.add_argument("--workers", "-w", type=int, default=0,
                help="并行进程数（0=自动取CPU核数）。默认: 0")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                help="每个并行任务处理的 satellite 行数（0=自动）。默认: 0")
    parser.add_argument("--full-candidate-audit", action="store_true",
                help="写出所有候选审计行；默认只写必要候选以提升速度。")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def ensure_output_paths(paths, overwrite):
    existing = [path for path in paths if path.exists()]
    if existing and not overwrite:
        raise FileExistsError(
            "output already exists; pass --overwrite to replace: {}".format(
                ", ".join(str(path) for path in existing)
            )
        )
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)


def main(argv=None):
    args = parse_args(argv)
    input_path = Path(args.input).expanduser().resolve()
    output_links = Path(args.output_links).expanduser().resolve()
    output_candidates = Path(args.output_candidates).expanduser().resolve()
    output_report = Path(args.output_report).expanduser().resolve()
    if not input_path.is_file():
        print("Error: input not found: {}".format(input_path), file=sys.stderr)
        return 1
    try:
        merit_dir = resolve_merit_dir(args.merit_basins_dir)
        ensure_output_paths([output_links, output_candidates, output_report], overwrite=True)
        print("Input: {}".format(input_path))
        print("Output links: {}".format(output_links))
        print("Output candidates: {}".format(output_candidates))
        print("Output report: {}".format(output_report))
        print("MERIT dir: {}".format(merit_dir))
        print(
            "Thresholds: max_upstream_rel_error={}, max_cross_reach_distance_m={}, "
            "max_topology_hops={}, ambiguous_distance_delta_m={}, ambiguous_area_delta={}".format(
                args.max_upstream_rel_error,
                args.max_cross_reach_distance_m,
                args.max_topology_hops,
                args.ambiguous_distance_delta_m,
                args.ambiguous_area_delta,
            )
        )
        stations = pd.read_csv(input_path, low_memory=False)
        print("Loaded s5 rows: {}".format(len(stations)))
        network = MeritReachNetwork(merit_dir=merit_dir)
        n_workers = args.workers if args.workers > 0 else DEFAULT_WORKERS
        links, candidates, report = link_satellite_to_main_clusters_v2(
            stations,
            reach_network=network,
            resolutions=args.resolution,
            max_upstream_rel_error=args.max_upstream_rel_error,
            max_cross_reach_distance_m=args.max_cross_reach_distance_m,
            max_topology_hops=args.max_topology_hops,
            ambiguous_distance_delta_m=args.ambiguous_distance_delta_m,
            ambiguous_area_delta=args.ambiguous_area_delta,
            workers=n_workers,
            merit_dir=merit_dir,
            chunk_size=args.chunk_size,
            full_candidate_audit=args.full_candidate_audit,
        )
        print("Satellite objects: {}".format(len(links)))
        print("Candidate audit rows: {}".format(len(candidates)))
        print(
            "Final: linked={}, ambiguous={}, unlinked={}".format(
                int(links["link_status"].eq("linked").sum()),
                int(links["link_status"].eq("ambiguous").sum()),
                int(links["link_status"].eq("unlinked").sum()),
            )
        )
        reason_counts = links["rejection_reason"].fillna("").replace("", "none").value_counts().sort_index()
        print("Rejection reasons:")
        for reason, count in reason_counts.items():
            print("  {}: {}".format(reason, int(count)))
        links.to_csv(output_links, index=False)
        candidates.to_csv(output_candidates, index=False)
        report.to_csv(output_report, index=False)
        print("Wrote: {}".format(output_links))
        print("Wrote: {}".format(output_candidates))
        print("Wrote: {}".format(output_report))
        return 0
    except (FileExistsError, FileNotFoundError, RuntimeError, ValueError) as exc:
        print("Error: {}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
