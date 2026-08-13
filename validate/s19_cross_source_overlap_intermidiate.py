#!/usr/bin/env python3
"""Build a source-station-level cross-source overlap inventory.

This diagnostic is intentionally independent of the current s5 cluster
membership.  It uses the source-station rows in
``s5_basin_clustered_stations.csv`` only as a convenient joined table of:

- source-station identity and source NetCDF path from s3;
- selected MERIT reach and upstream-area attributes from s4; and
- the current cluster_id as an audit field only.

Candidate station pairs are discovered through three high-recall routes:

1. the same selected MERIT reach (same COMID);
2. directly connected MERIT reaches within a configurable hop count; and
3. nearby source coordinates within a configurable radius, including
   unresolved stations.

For each cross-source pair the script reports:

- source-station identifiers, names, river names, countries, coordinates;
- current-cluster membership, but never uses it to select candidates;
- same/connected reach relation, topology hops, point distance;
- MERIT and source-reported upstream-area differences;
- common dates, months, and years;
- variables jointly available at the native temporal support; and
- exact and near-exact value fractions for Q, SSC, and SSL.

Exact-value fractions are calculated only for pairs with the same temporal
resolution, using date for daily, calendar month for monthly, and calendar year
for annual data.  Cross-resolution pairs remain in the overlap inventory but
are not treated as value-comparison pairs.

The script is read-only with respect to pipeline inputs and release products.
By default, outputs are written under:

    validate/output/cross_source_overlap/

Suggested repository path:

    validate/s13_cross_source_overlap_inventory.py

Examples
--------
Full high-recall inventory::

    python validate/s13_cross_source_overlap_inventory.py

Use analysis-ready flags only and disable nearby-coordinate candidates::

    python validate/s13_cross_source_overlap_inventory.py \
        --allowed-flags 0,1 --skip-nearby

Fast metadata-only spatial inventory::

    python validate/s13_cross_source_overlap_inventory.py --skip-temporal

Restrict to the reviewer-priority sources::

    python validate/s13_cross_source_overlap_inventory.py \
        --include-sources GFQA_v2,USGS_NWIS,HYDAT,Bayern,GloRiSe,EUSEDcollab,Rhine,NERC,HYBAM
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from collections import OrderedDict, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import (  # noqa: E402
    S2_ORGANIZED_DIR,
    S5_BASIN_CLUSTERED_CSV,
    get_output_r_root,
)
from source_family import (  # noqa: E402
    classify_source_family,
    normalize_source_name,
)

try:  # Reuse the pipeline's MERIT topology implementation.
    from s5b_link_satellite_to_main_clusters_v2 import (  # noqa: E402
        DEFAULT_MERIT_DIR,
        MeritReachNetwork,
    )
    MERIT_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - depends on optional GIS stack.
    DEFAULT_MERIT_DIR = Path("")
    MeritReachNetwork = None
    MERIT_IMPORT_ERROR = "{}: {}".format(type(exc).__name__, exc)

try:  # Reuse the exact source-NC interpretation used by s6.
    from s6_basin_merge_to_nc import HAS_NC, load_nc_series  # noqa: E402
    SERIES_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - depends on runtime environment.
    HAS_NC = False
    load_nc_series = None
    SERIES_IMPORT_ERROR = "{}: {}".format(type(exc).__name__, exc)


VARIABLES = ("Q", "SSC", "SSL")
SUPPORTED_RESOLUTIONS = ("daily", "monthly", "annual")
DEFAULT_MAX_TOPOLOGY_HOPS = 1
DEFAULT_NEARBY_RADIUS_M = 5000.0
DEFAULT_CACHE_SIZE = 128
DEFAULT_DUPLICATE_MIN_PAIRED = 20
DEFAULT_DUPLICATE_EXACT_FRACTION = 0.95
DEFAULT_NEAR_RTOL = 1.0e-6
DEFAULT_NEAR_ATOL = 1.0e-12
EARTH_RADIUS_M = 6371008.8

PRIORITY_GROUP_RANK = {
    "GFQA_v2_vs_agency": 1,
    "GloRiSe_vs_other": 2,
    "EUSEDcollab_vs_European_source": 3,
    "HYBAM_vs_other": 4,
    "other_cross_source": 9,
}

PAIR_ID_COLUMNS = (
    "station_key_a",
    "station_key_b",
)


class Config:
    def __init__(
        self,
        s5_csv: Path,
        source_root: Path,
        merit_dir: Path,
        out_dir: Path,
        source_families: Set[str],
        include_sources: Set[str],
        exclude_sources: Set[str],
        same_resolution_only: bool,
        max_topology_hops: int,
        nearby_radius_m: float,
        skip_connected: bool,
        skip_nearby: bool,
        skip_temporal: bool,
        allowed_flags: Set[int],
        cache_size: int,
        near_rtol: float,
        near_atol: float,
        duplicate_min_paired: int,
        duplicate_exact_fraction: float,
        max_pairs: int,
    ) -> None:
        self.s5_csv = Path(s5_csv)
        self.source_root = Path(source_root)
        self.merit_dir = Path(merit_dir)
        self.out_dir = Path(out_dir)
        self.source_families = set(source_families)
        self.include_sources = set(include_sources)
        self.exclude_sources = set(exclude_sources)
        self.same_resolution_only = bool(same_resolution_only)
        self.max_topology_hops = int(max_topology_hops)
        self.nearby_radius_m = float(nearby_radius_m)
        self.skip_connected = bool(skip_connected)
        self.skip_nearby = bool(skip_nearby)
        self.skip_temporal = bool(skip_temporal)
        self.allowed_flags = set(int(v) for v in allowed_flags)
        self.cache_size = max(1, int(cache_size))
        self.near_rtol = float(near_rtol)
        self.near_atol = float(near_atol)
        self.duplicate_min_paired = max(1, int(duplicate_min_paired))
        self.duplicate_exact_fraction = float(duplicate_exact_fraction)
        self.max_pairs = max(0, int(max_pairs))


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "nat"} else text


def normalize_resolution(value) -> str:
    text = clean_text(value).lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "single_point": "daily",
        "day": "daily",
        "daily_observation": "daily",
        "quarterly": "monthly",
        "month": "monthly",
        "monthly_observation": "monthly",
        "year": "annual",
        "yearly": "annual",
        "annually": "annual",
        "annually_climatology": "climatology",
    }
    return aliases.get(text, text)


def normalized_name(value) -> str:
    text = clean_text(value).casefold()
    return "".join(ch for ch in text if ch.isalnum())


def name_similarity(left, right) -> float:
    left_norm = normalized_name(left)
    right_norm = normalized_name(right)
    if not left_norm or not right_norm:
        return math.nan
    if left_norm == right_norm:
        return 1.0
    # Lightweight Sørensen-Dice similarity on character bigrams.
    def bigrams(text: str) -> Set[str]:
        if len(text) < 2:
            return {text}
        return {text[i : i + 2] for i in range(len(text) - 1)}

    left_bi = bigrams(left_norm)
    right_bi = bigrams(right_norm)
    denom = len(left_bi) + len(right_bi)
    return (2.0 * len(left_bi.intersection(right_bi)) / denom) if denom else math.nan


def finite_float(value) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def finite_int(value) -> Optional[int]:
    number = finite_float(value)
    if not math.isfinite(number):
        return None
    rounded = int(number)
    return rounded if abs(number - rounded) < 1.0e-9 else None


def valid_lat_lon(lat, lon) -> bool:
    lat_f = finite_float(lat)
    lon_f = finite_float(lon)
    return bool(
        math.isfinite(lat_f)
        and math.isfinite(lon_f)
        and -90.0 <= lat_f <= 90.0
        and -180.0 <= lon_f <= 180.0
    )


def haversine_distance_m(lat1, lon1, lat2, lon2) -> float:
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
    return EARTH_RADIUS_M * 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def symmetric_rel_error(left, right) -> float:
    left_f = finite_float(left)
    right_f = finite_float(right)
    if not (
        math.isfinite(left_f)
        and math.isfinite(right_f)
        and left_f > 0.0
        and right_f > 0.0
    ):
        return math.nan
    return abs(left_f - right_f) / max(abs(left_f), abs(right_f))


def parse_csv_set(text: str, normalizer=None) -> Set:
    values = []
    for item in clean_text(text).split(","):
        item = clean_text(item)
        if item:
            values.append(normalizer(item) if normalizer else item)
    return set(values)


def parse_flags(text: str) -> Set[int]:
    values = parse_csv_set(text)
    try:
        flags = {int(value) for value in values}
    except ValueError as exc:
        raise argparse.ArgumentTypeError("allowed flags must be comma-separated integers") from exc
    invalid = sorted(flags.difference({0, 1, 2, 3, 9}))
    if invalid:
        raise argparse.ArgumentTypeError("unsupported flag values: {}".format(invalid))
    return flags


def require_columns(frame: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError("{} missing required columns: {}".format(label, ", ".join(missing)))


def source_pair_priority(source_a: str, source_b: str) -> str:
    pair = {normalize_source_name(source_a), normalize_source_name(source_b)}
    agencies = {"usgs", "usgs_nwis", "hydat", "bayern"}
    european = {"bayern", "rhine", "nerc", "nerc_avon"}
    if "gfqa_v2" in pair and pair.intersection(agencies):
        return "GFQA_v2_vs_agency"
    if "glorise" in pair and len(pair) > 1:
        return "GloRiSe_vs_other"
    if "eusedcollab" in pair and pair.intersection(european):
        return "EUSEDcollab_vs_European_source"
    if "hybam" in pair and len(pair) > 1:
        return "HYBAM_vs_other"
    return "other_cross_source"


def support_for_resolution(resolution: str) -> str:
    return {
        "daily": "date",
        "monthly": "month",
        "annual": "year",
    }.get(normalize_resolution(resolution), "")


def relation_invert(relation: str) -> str:
    return {
        "a_upstream_of_b": "a_downstream_of_b",
        "a_downstream_of_b": "a_upstream_of_b",
    }.get(relation, relation)


def relation_from_network_label(label: str) -> str:
    # In s5b v2 the first argument is named "satellite".  Here it is simply A.
    return {
        "satellite_upstream": "a_upstream_of_b",
        "satellite_downstream": "a_downstream_of_b",
        "same_reach": "same_reach",
    }.get(clean_text(label), "unknown")


def markdown_table(frame: pd.DataFrame, max_rows: Optional[int] = None) -> str:
    if frame is None or frame.empty:
        return "_No rows._"
    work = frame.head(max_rows).copy() if max_rows else frame.copy()
    columns = list(work.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in work.iterrows():
        values = []
        for column in columns:
            value = row.get(column)
            if isinstance(value, float) and math.isnan(value):
                text = ""
            else:
                text = clean_text(value)
            values.append(text.replace("|", "\\|"))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Station table preparation
# ---------------------------------------------------------------------------


def load_station_table(cfg: Config) -> pd.DataFrame:
    if not cfg.s5_csv.is_file():
        raise FileNotFoundError("s5 source-station table not found: {}".format(cfg.s5_csv))
    frame = pd.read_csv(cfg.s5_csv, low_memory=False)
    require_columns(
        frame,
        ["station_id", "path", "source", "lat", "lon", "resolution"],
        "s5 source-station table",
    )
    work = frame.copy()
    if "station_key" not in work.columns:
        work["station_key"] = work["station_id"].map(lambda value: "station_id:{}".format(clean_text(value)))
    if "cluster_id" not in work.columns:
        work["cluster_id"] = np.nan
    for column in (
        "source_station_id",
        "station_name",
        "river_name",
        "country",
        "continent_region",
        "observation_type",
        "basin_status",
        "pfaf_code",
    ):
        if column not in work.columns:
            work[column] = ""
    for column in ("basin_id", "uparea_merit", "reported_area"):
        if column not in work.columns:
            work[column] = np.nan

    work["station_key"] = work["station_key"].map(clean_text)
    missing_key = work["station_key"].eq("")
    work.loc[missing_key, "station_key"] = work.loc[missing_key, "station_id"].map(
        lambda value: "station_id:{}".format(clean_text(value))
    )
    if work["station_key"].duplicated().any():
        duplicated = work.loc[work["station_key"].duplicated(keep=False), ["station_key", "station_id", "path"]]
        raise ValueError(
            "s5 station_key is not unique. First duplicates:\n{}".format(
                duplicated.head(20).to_string(index=False)
            )
        )

    work["source"] = work["source"].map(clean_text)
    work["source_canonical"] = work["source"].map(normalize_source_name)
    work["resolution"] = work["resolution"].map(normalize_resolution)
    work["source_family"] = work.apply(
        lambda row: classify_source_family(
            row.get("source", ""),
            resolution=row.get("resolution", ""),
            observation_type=row.get("observation_type", ""),
        ),
        axis=1,
    )
    family_mask = work["source_family"].isin(cfg.source_families)
    work = work.loc[family_mask].copy()
    if cfg.include_sources:
        work = work.loc[work["source_canonical"].isin(cfg.include_sources)].copy()
    if cfg.exclude_sources:
        work = work.loc[~work["source_canonical"].isin(cfg.exclude_sources)].copy()

    work["lat"] = pd.to_numeric(work["lat"], errors="coerce")
    work["lon"] = pd.to_numeric(work["lon"], errors="coerce")
    work["uparea_merit"] = pd.to_numeric(work["uparea_merit"], errors="coerce")
    work["reported_area"] = pd.to_numeric(work["reported_area"], errors="coerce")
    work["cluster_id"] = pd.to_numeric(work["cluster_id"], errors="coerce")
    basin_status = work["basin_status"].map(lambda value: clean_text(value).lower())
    basin_id = pd.to_numeric(work["basin_id"], errors="coerce")
    resolved = basin_status.eq("resolved") & basin_id.notna() & (basin_id > 0)
    work["comid"] = basin_id.where(resolved, np.nan)
    work["pfaf_code"] = work["pfaf_code"].map(clean_text)
    work["path"] = work["path"].map(clean_text)
    work["_sort_key"] = list(
        zip(
            work["source_canonical"].map(clean_text),
            work["station_key"].map(clean_text),
        )
    )
    work = work.sort_values(["source_canonical", "station_key"], kind="mergesort").reset_index(drop=True)
    work["row_index"] = np.arange(len(work), dtype=np.int64)
    return work


# ---------------------------------------------------------------------------
# Candidate pair construction
# ---------------------------------------------------------------------------


def pair_order(stations: pd.DataFrame, left_idx: int, right_idx: int) -> Tuple[int, int, bool]:
    left = stations.iloc[int(left_idx)]
    right = stations.iloc[int(right_idx)]
    left_key = (clean_text(left["source_canonical"]), clean_text(left["station_key"]))
    right_key = (clean_text(right["source_canonical"]), clean_text(right["station_key"]))
    if left_key <= right_key:
        return int(left_idx), int(right_idx), False
    return int(right_idx), int(left_idx), True


def pair_allowed(stations: pd.DataFrame, left_idx: int, right_idx: int, cfg: Config) -> bool:
    left = stations.iloc[int(left_idx)]
    right = stations.iloc[int(right_idx)]
    if clean_text(left["source_canonical"]) == clean_text(right["source_canonical"]):
        return False
    if cfg.same_resolution_only and clean_text(left["resolution"]) != clean_text(right["resolution"]):
        return False
    return True


def add_candidate(
    candidates: MutableMapping[Tuple[int, int], Dict],
    stations: pd.DataFrame,
    left_idx: int,
    right_idx: int,
    cfg: Config,
    mode: str,
    relation_left_to_right: str = "",
    topology_hops: Optional[int] = None,
) -> None:
    if int(left_idx) == int(right_idx) or not pair_allowed(stations, left_idx, right_idx, cfg):
        return
    ordered_left, ordered_right, swapped = pair_order(stations, left_idx, right_idx)
    relation = clean_text(relation_left_to_right)
    if swapped and relation:
        relation = relation_invert(relation)
    key = (ordered_left, ordered_right)
    entry = candidates.setdefault(
        key,
        {
            "row_index_a": ordered_left,
            "row_index_b": ordered_right,
            "candidate_modes": set(),
            "topology_relation": "",
            "topology_hops": None,
        },
    )
    entry["candidate_modes"].add(mode)
    if mode == "same_reach":
        entry["topology_relation"] = "same_reach"
        entry["topology_hops"] = 0
    elif mode == "connected_reach" and relation:
        previous_hops = entry.get("topology_hops")
        if previous_hops is None or topology_hops is None or int(topology_hops) < int(previous_hops):
            entry["topology_relation"] = relation
            entry["topology_hops"] = int(topology_hops) if topology_hops is not None else None


def generate_same_reach_candidates(
    stations: pd.DataFrame,
    candidates: MutableMapping[Tuple[int, int], Dict],
    cfg: Config,
) -> Dict[str, int]:
    valid = stations.loc[pd.to_numeric(stations["comid"], errors="coerce").notna()].copy()
    groups_checked = 0
    pair_attempts = 0
    before = len(candidates)
    for _, group in valid.groupby("comid", sort=True):
        indices = group.index.to_list()
        if len(indices) < 2:
            continue
        groups_checked += 1
        for pos, left_idx in enumerate(indices[:-1]):
            for right_idx in indices[pos + 1 :]:
                pair_attempts += 1
                add_candidate(candidates, stations, left_idx, right_idx, cfg, mode="same_reach")
    return {
        "same_reach_groups_checked": groups_checked,
        "same_reach_pair_attempts": pair_attempts,
        "same_reach_pairs_added": len(candidates) - before,
    }


def generate_connected_candidates(
    stations: pd.DataFrame,
    candidates: MutableMapping[Tuple[int, int], Dict],
    cfg: Config,
) -> Tuple[Dict[str, int], List[Dict]]:
    stats = {
        "connected_reaches_checked": 0,
        "connected_reaches_missing_from_merit": 0,
        "connected_reach_pairs_seen": 0,
        "connected_station_pairs_added": 0,
    }
    errors: List[Dict] = []
    if cfg.skip_connected or cfg.max_topology_hops <= 0:
        return stats, errors
    if MeritReachNetwork is None:
        errors.append(
            {
                "stage": "connected_candidate_generation",
                "item": "MeritReachNetwork import",
                "error": MERIT_IMPORT_ERROR or "MeritReachNetwork unavailable",
            }
        )
        return stats, errors
    if not cfg.merit_dir.is_dir():
        errors.append(
            {
                "stage": "connected_candidate_generation",
                "item": str(cfg.merit_dir),
                "error": "MERIT directory not found; same-reach and nearby candidates remain available",
            }
        )
        return stats, errors

    valid = stations.loc[pd.to_numeric(stations["comid"], errors="coerce").notna()].copy()
    reach_to_indices: Dict[int, List[int]] = defaultdict(list)
    reach_to_pfaf: Dict[int, str] = {}
    for idx, row in valid.iterrows():
        comid = finite_int(row.get("comid"))
        if comid is None:
            continue
        reach_to_indices[comid].append(int(idx))
        if comid not in reach_to_pfaf or not reach_to_pfaf[comid]:
            reach_to_pfaf[comid] = clean_text(row.get("pfaf_code", ""))

    network = MeritReachNetwork(merit_dir=cfg.merit_dir)
    observed_reaches = set(reach_to_indices)
    processed_reach_pairs: Set[Tuple[int, int]] = set()
    before = len(candidates)

    for count, comid in enumerate(sorted(observed_reaches), start=1):
        stats["connected_reaches_checked"] += 1
        try:
            connected, found = network.connected_reach_maps(
                comid,
                reach_to_pfaf.get(comid, ""),
                cfg.max_topology_hops,
            )
        except Exception as exc:
            errors.append(
                {
                    "stage": "connected_candidate_generation",
                    "item": "COMID {}".format(comid),
                    "error": "{}: {}".format(type(exc).__name__, exc),
                }
            )
            continue
        if not found:
            stats["connected_reaches_missing_from_merit"] += 1
            continue
        for target, raw_relation_hops in connected.items():
            target_int = finite_int(target)
            if target_int is None or target_int not in observed_reaches or target_int == comid:
                continue
            reach_pair = tuple(sorted((int(comid), int(target_int))))
            if reach_pair in processed_reach_pairs:
                continue
            processed_reach_pairs.add(reach_pair)
            stats["connected_reach_pairs_seen"] += 1
            raw_relation, hops = raw_relation_hops
            relation = relation_from_network_label(raw_relation)
            for left_idx in reach_to_indices[comid]:
                for right_idx in reach_to_indices[target_int]:
                    add_candidate(
                        candidates,
                        stations,
                        left_idx,
                        right_idx,
                        cfg,
                        mode="connected_reach",
                        relation_left_to_right=relation,
                        topology_hops=int(hops),
                    )
        if count % 250 == 0:
            print(
                "  connected reach scan: {:,}/{:,} observed reaches; {:,} station pairs so far".format(
                    count, len(observed_reaches), len(candidates)
                )
            )

    stats["connected_station_pairs_added"] = len(candidates) - before
    return stats, errors


def generate_nearby_candidates(
    stations: pd.DataFrame,
    candidates: MutableMapping[Tuple[int, int], Dict],
    cfg: Config,
) -> Dict[str, int]:
    stats = {
        "nearby_coordinate_rows_checked": 0,
        "nearby_pair_attempts": 0,
        "nearby_station_pairs_added": 0,
    }
    if cfg.skip_nearby or cfg.nearby_radius_m <= 0.0:
        return stats

    valid_indices = [
        int(idx)
        for idx, row in stations.iterrows()
        if valid_lat_lon(row.get("lat"), row.get("lon"))
    ]
    valid_indices.sort(key=lambda idx: finite_float(stations.iloc[idx]["lat"]))
    lat_values = np.array([finite_float(stations.iloc[idx]["lat"]) for idx in valid_indices], dtype=float)
    lat_tolerance = cfg.nearby_radius_m / 110574.0
    before = len(candidates)

    for pos, left_idx in enumerate(valid_indices):
        stats["nearby_coordinate_rows_checked"] += 1
        left = stations.iloc[left_idx]
        lat_left = finite_float(left["lat"])
        lon_left = finite_float(left["lon"])
        end = int(np.searchsorted(lat_values, lat_left + lat_tolerance, side="right"))
        for other_pos in range(pos + 1, end):
            right_idx = valid_indices[other_pos]
            if not pair_allowed(stations, left_idx, right_idx, cfg):
                continue
            right = stations.iloc[right_idx]
            lat_right = finite_float(right["lat"])
            lon_right = finite_float(right["lon"])
            mean_lat = math.radians((lat_left + lat_right) / 2.0)
            lon_scale = max(abs(math.cos(mean_lat)), 1.0e-4)
            lon_tolerance = cfg.nearby_radius_m / (111320.0 * lon_scale)
            lon_delta = abs(((lon_right - lon_left + 180.0) % 360.0) - 180.0)
            if lon_delta > lon_tolerance:
                continue
            stats["nearby_pair_attempts"] += 1
            distance = haversine_distance_m(lat_left, lon_left, lat_right, lon_right)
            if math.isfinite(distance) and distance <= cfg.nearby_radius_m:
                add_candidate(candidates, stations, left_idx, right_idx, cfg, mode="nearby_coordinates")

    stats["nearby_station_pairs_added"] = len(candidates) - before
    return stats


# ---------------------------------------------------------------------------
# Source time-series loading and comparison
# ---------------------------------------------------------------------------


def aggregate_support_frame(frame: pd.DataFrame, key: pd.Series) -> pd.DataFrame:
    work = frame.loc[:, list(VARIABLES)].copy()
    work.insert(0, "_support_key", key.values)
    work = work.dropna(subset=["_support_key"])
    if work.empty:
        return pd.DataFrame(columns=list(VARIABLES))
    return work.groupby("_support_key", sort=True)[list(VARIABLES)].mean()


class SeriesCache:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.cache: "OrderedDict[str, Dict]" = OrderedDict()
        self.audit: Dict[str, Dict] = {}

    def resolve_path(self, path_value) -> Path:
        path = Path(clean_text(path_value)).expanduser()
        if path.is_absolute():
            return path.resolve()
        return (self.cfg.source_root / path).resolve()

    def get(self, row: Mapping) -> Dict:
        station_key = clean_text(row.get("station_key", ""))
        path = self.resolve_path(row.get("path", ""))
        cache_key = str(path)
        if cache_key in self.cache:
            item = self.cache.pop(cache_key)
            self.cache[cache_key] = item
            self._record_audit(station_key, row, item)
            return item
        item = self._load(path)
        self.cache[cache_key] = item
        while len(self.cache) > self.cfg.cache_size:
            self.cache.popitem(last=False)
        self._record_audit(station_key, row, item)
        return item

    def _record_audit(self, station_key: str, row: Mapping, item: Mapping) -> None:
        if station_key in self.audit:
            return
        self.audit[station_key] = {
            "station_key": station_key,
            "station_id": row.get("station_id"),
            "source": row.get("source"),
            "source_station_id": row.get("source_station_id"),
            "resolution": row.get("resolution"),
            "path": row.get("path"),
            "absolute_path": item.get("absolute_path", ""),
            "read_status": item.get("status", ""),
            "read_error": item.get("error", ""),
            "n_time_rows": item.get("n_time_rows", 0),
            "n_valid_dates": item.get("n_valid_dates", 0),
            "n_duplicate_date_rows": item.get("n_duplicate_date_rows", 0),
            "time_start": item.get("time_start", ""),
            "time_end": item.get("time_end", ""),
            "available_variables": "|".join(item.get("available_variables", [])),
            "unit_issue_count": item.get("unit_issue_count", 0),
        }

    def _empty(self, path: Path, status: str, error: str = "") -> Dict:
        empty = pd.DataFrame(columns=list(VARIABLES))
        return {
            "status": status,
            "error": error,
            "absolute_path": str(path),
            "n_time_rows": 0,
            "n_valid_dates": 0,
            "n_duplicate_date_rows": 0,
            "time_start": "",
            "time_end": "",
            "available_variables": [],
            "unit_issue_count": 0,
            "date_set": set(),
            "month_set": set(),
            "year_set": set(),
            "support_frames": {"date": empty, "month": empty, "year": empty},
        }

    def _load(self, path: Path) -> Dict:
        if not path.is_file():
            return self._empty(path, "missing_file", "source NetCDF does not exist")
        if load_nc_series is None or not HAS_NC:
            return self._empty(
                path,
                "reader_unavailable",
                SERIES_IMPORT_ERROR or "netCDF4/load_nc_series unavailable",
            )
        try:
            frame, unit_issues = load_nc_series(path)
        except Exception as exc:  # pragma: no cover - load_nc_series normally catches.
            return self._empty(path, "read_error", "{}: {}".format(type(exc).__name__, exc))
        if frame is None or frame.empty:
            return self._empty(path, "no_readable_series", "load_nc_series returned no rows")

        work = frame.copy()
        work["_date"] = pd.to_datetime(work.get("date"), errors="coerce").dt.normalize()
        work = work.loc[work["_date"].notna()].copy()
        if work.empty:
            return self._empty(path, "no_valid_dates", "all source time values were invalid")

        for variable in VARIABLES:
            if variable not in work.columns:
                work[variable] = np.nan
            values = pd.to_numeric(work[variable], errors="coerce")
            flag_column = "{}_flag".format(variable)
            if flag_column in work.columns:
                flags = pd.to_numeric(work[flag_column], errors="coerce")
                values = values.where(flags.isin(self.cfg.allowed_flags), np.nan)
            work[variable] = values

        date_key = work["_date"]
        month_key = date_key.dt.to_period("M")
        year_key = date_key.dt.year.astype("Int64")
        date_set = set(date_key.tolist())
        month_set = set(month_key.tolist())
        year_set = set(int(value) for value in year_key.dropna().tolist())
        available = [variable for variable in VARIABLES if work[variable].notna().any()]
        duplicate_rows = int(work["_date"].duplicated(keep=False).sum())
        return {
            "status": "ok",
            "error": "",
            "absolute_path": str(path),
            "n_time_rows": int(len(frame)),
            "n_valid_dates": int(len(work)),
            "n_duplicate_date_rows": duplicate_rows,
            "time_start": date_key.min().strftime("%Y-%m-%d"),
            "time_end": date_key.max().strftime("%Y-%m-%d"),
            "available_variables": available,
            "unit_issue_count": int(len(unit_issues or [])),
            "date_set": date_set,
            "month_set": month_set,
            "year_set": year_set,
            "support_frames": {
                "date": aggregate_support_frame(work, date_key),
                "month": aggregate_support_frame(work, month_key),
                "year": aggregate_support_frame(work, year_key),
            },
        }


def format_support_value(value, support: str) -> str:
    if value is None:
        return ""
    if support == "date":
        try:
            return pd.Timestamp(value).strftime("%Y-%m-%d")
        except Exception:
            return clean_text(value)
    if support == "month":
        return clean_text(value)
    if support == "year":
        try:
            return str(int(value))
        except Exception:
            return clean_text(value)
    return clean_text(value)


def intersection_summary(left: Set, right: Set, support: str) -> Tuple[int, str, str]:
    common = sorted(left.intersection(right))
    if not common:
        return 0, "", ""
    return len(common), format_support_value(common[0], support), format_support_value(common[-1], support)


def compare_native_values(
    left: Mapping,
    right: Mapping,
    support: str,
    cfg: Config,
) -> Dict:
    result: Dict[str, object] = {
        "comparison_support": support,
        "n_common_native_periods": 0,
        "common_variables_native": "",
        "n_paired_values_total": 0,
        "n_exact_equal_values_total": 0,
        "exact_equal_fraction_all": math.nan,
        "near_equal_fraction_all": math.nan,
    }
    for variable in VARIABLES:
        result["n_paired_{}".format(variable)] = 0
        result["n_exact_equal_{}".format(variable)] = 0
        result["exact_equal_fraction_{}".format(variable)] = math.nan
        result["near_equal_fraction_{}".format(variable)] = math.nan

    if not support:
        result["comparison_support"] = "cross_resolution"
        return result
    left_frame = left["support_frames"].get(support)
    right_frame = right["support_frames"].get(support)
    if left_frame is None or right_frame is None or left_frame.empty or right_frame.empty:
        return result
    common_index = left_frame.index.intersection(right_frame.index)
    result["n_common_native_periods"] = int(len(common_index))
    if len(common_index) == 0:
        return result

    left_common = left_frame.loc[common_index]
    right_common = right_frame.loc[common_index]
    common_variables = []
    total_paired = 0
    total_exact = 0
    total_near = 0
    for variable in VARIABLES:
        left_values = pd.to_numeric(left_common[variable], errors="coerce").to_numpy(dtype=float)
        right_values = pd.to_numeric(right_common[variable], errors="coerce").to_numpy(dtype=float)
        paired = np.isfinite(left_values) & np.isfinite(right_values)
        n_paired = int(paired.sum())
        result["n_paired_{}".format(variable)] = n_paired
        if n_paired == 0:
            continue
        common_variables.append(variable)
        left_paired = left_values[paired]
        right_paired = right_values[paired]
        exact = left_paired == right_paired
        near = np.isclose(
            left_paired,
            right_paired,
            rtol=cfg.near_rtol,
            atol=cfg.near_atol,
            equal_nan=False,
        )
        n_exact = int(exact.sum())
        n_near = int(near.sum())
        result["n_exact_equal_{}".format(variable)] = n_exact
        result["exact_equal_fraction_{}".format(variable)] = n_exact / n_paired
        result["near_equal_fraction_{}".format(variable)] = n_near / n_paired
        total_paired += n_paired
        total_exact += n_exact
        total_near += n_near

    result["common_variables_native"] = "|".join(common_variables)
    result["n_paired_values_total"] = total_paired
    result["n_exact_equal_values_total"] = total_exact
    if total_paired > 0:
        result["exact_equal_fraction_all"] = total_exact / total_paired
        result["near_equal_fraction_all"] = total_near / total_paired
    return result


# ---------------------------------------------------------------------------
# Pair-level diagnostics
# ---------------------------------------------------------------------------


def station_metadata(prefix: str, row: Mapping) -> Dict:
    return {
        "station_key_{}".format(prefix): row.get("station_key"),
        "station_id_{}".format(prefix): row.get("station_id"),
        "source_{}".format(prefix): row.get("source"),
        "source_canonical_{}".format(prefix): row.get("source_canonical"),
        "source_station_id_{}".format(prefix): row.get("source_station_id"),
        "station_name_{}".format(prefix): row.get("station_name"),
        "river_name_{}".format(prefix): row.get("river_name"),
        "country_{}".format(prefix): row.get("country"),
        "continent_region_{}".format(prefix): row.get("continent_region"),
        "resolution_{}".format(prefix): row.get("resolution"),
        "path_{}".format(prefix): row.get("path"),
        "lat_{}".format(prefix): row.get("lat"),
        "lon_{}".format(prefix): row.get("lon"),
        "current_cluster_id_{}".format(prefix): row.get("cluster_id"),
        "basin_status_{}".format(prefix): row.get("basin_status"),
        "comid_{}".format(prefix): row.get("comid"),
        "pfaf_code_{}".format(prefix): row.get("pfaf_code"),
        "uparea_merit_{}".format(prefix): row.get("uparea_merit"),
        "reported_area_{}".format(prefix): row.get("reported_area"),
    }


def pair_spatial_row(candidate: Mapping, stations: pd.DataFrame) -> Dict:
    row_a = stations.iloc[int(candidate["row_index_a"])]
    row_b = stations.iloc[int(candidate["row_index_b"])]
    modes = sorted(candidate.get("candidate_modes", set()))
    comid_a = finite_int(row_a.get("comid"))
    comid_b = finite_int(row_b.get("comid"))
    cluster_a = finite_int(row_a.get("cluster_id"))
    cluster_b = finite_int(row_b.get("cluster_id"))
    source_a = clean_text(row_a.get("source", ""))
    source_b = clean_text(row_b.get("source", ""))
    same_reach = comid_a is not None and comid_b is not None and comid_a == comid_b
    if same_reach:
        relation = "same_reach"
        hops = 0
        primary_relation = "same_reach"
    elif "connected_reach" in modes:
        relation = clean_text(candidate.get("topology_relation", "")) or "connected_unknown_direction"
        hops = candidate.get("topology_hops")
        primary_relation = "connected_reach"
    elif "nearby_coordinates" in modes:
        relation = "not_evaluated_as_connected"
        hops = pd.NA
        primary_relation = "nearby_coordinates"
    else:
        relation = "unknown"
        hops = pd.NA
        primary_relation = modes[0] if modes else "unknown"

    output = {
        "pair_id": "{}__{}".format(row_a.get("station_key"), row_b.get("station_key")),
        "priority_group": source_pair_priority(source_a, source_b),
        "priority_rank": PRIORITY_GROUP_RANK.get(source_pair_priority(source_a, source_b), 9),
        "candidate_modes": "|".join(modes),
        "primary_spatial_relation": primary_relation,
        "same_reach": bool(same_reach),
        "topology_relation": relation,
        "topology_hops": hops,
        "same_resolution": clean_text(row_a.get("resolution")) == clean_text(row_b.get("resolution")),
        "same_current_cluster": cluster_a is not None and cluster_b is not None and cluster_a == cluster_b,
        "coordinate_distance_m": haversine_distance_m(
            row_a.get("lat"), row_a.get("lon"), row_b.get("lat"), row_b.get("lon")
        ),
        "merit_uparea_rel_error": symmetric_rel_error(
            row_a.get("uparea_merit"), row_b.get("uparea_merit")
        ),
        "reported_area_rel_error": symmetric_rel_error(
            row_a.get("reported_area"), row_b.get("reported_area")
        ),
        "source_station_id_match": bool(
            normalized_name(row_a.get("source_station_id"))
            and normalized_name(row_a.get("source_station_id"))
            == normalized_name(row_b.get("source_station_id"))
        ),
        "station_name_match": bool(
            normalized_name(row_a.get("station_name"))
            and normalized_name(row_a.get("station_name"))
            == normalized_name(row_b.get("station_name"))
        ),
        "river_name_match": bool(
            normalized_name(row_a.get("river_name"))
            and normalized_name(row_a.get("river_name"))
            == normalized_name(row_b.get("river_name"))
        ),
        "country_match": bool(
            normalized_name(row_a.get("country"))
            and normalized_name(row_a.get("country"))
            == normalized_name(row_b.get("country"))
        ),
        "station_name_similarity": name_similarity(
            row_a.get("station_name"), row_b.get("station_name")
        ),
        "river_name_similarity": name_similarity(
            row_a.get("river_name"), row_b.get("river_name")
        ),
    }
    output.update(station_metadata("a", row_a))
    output.update(station_metadata("b", row_b))
    return output


def evaluate_temporal_pair(
    spatial: Dict,
    row_a: Mapping,
    row_b: Mapping,
    cache: SeriesCache,
    cfg: Config,
) -> Dict:
    output = dict(spatial)
    if cfg.skip_temporal:
        output.update(
            {
                "series_status_a": "not_read_skip_temporal",
                "series_status_b": "not_read_skip_temporal",
                "n_common_dates": 0,
                "n_common_months": 0,
                "n_common_years": 0,
                "temporal_overlap_level": "not_evaluated",
                "common_variables_any": "",
                "comparison_support": "not_evaluated",
                "n_common_native_periods": 0,
                "n_paired_values_total": 0,
                "n_exact_equal_values_total": 0,
                "common_variables_native": "",
                "exact_equal_fraction_all": math.nan,
                "near_equal_fraction_all": math.nan,
                "independence_review_priority": "not_evaluated",
                "review_reasons": "",
            }
        )
        for variable in VARIABLES:
            output["n_paired_{}".format(variable)] = 0
            output["n_exact_equal_{}".format(variable)] = 0
            output["exact_equal_fraction_{}".format(variable)] = math.nan
            output["near_equal_fraction_{}".format(variable)] = math.nan
        return output

    series_a = cache.get(row_a)
    series_b = cache.get(row_b)
    output["series_status_a"] = series_a["status"]
    output["series_status_b"] = series_b["status"]
    output["series_error_a"] = series_a["error"]
    output["series_error_b"] = series_b["error"]
    output["series_time_start_a"] = series_a["time_start"]
    output["series_time_end_a"] = series_a["time_end"]
    output["series_time_start_b"] = series_b["time_start"]
    output["series_time_end_b"] = series_b["time_end"]
    output["series_duplicate_date_rows_a"] = series_a["n_duplicate_date_rows"]
    output["series_duplicate_date_rows_b"] = series_b["n_duplicate_date_rows"]

    n_dates, date_start, date_end = intersection_summary(
        series_a["date_set"], series_b["date_set"], "date"
    )
    n_months, month_start, month_end = intersection_summary(
        series_a["month_set"], series_b["month_set"], "month"
    )
    n_years, year_start, year_end = intersection_summary(
        series_a["year_set"], series_b["year_set"], "year"
    )
    output.update(
        {
            "n_common_dates": n_dates,
            "common_date_start": date_start,
            "common_date_end": date_end,
            "n_common_months": n_months,
            "common_month_start": month_start,
            "common_month_end": month_end,
            "n_common_years": n_years,
            "common_year_start": year_start,
            "common_year_end": year_end,
        }
    )
    if n_dates > 0:
        output["temporal_overlap_level"] = "date"
    elif n_months > 0:
        output["temporal_overlap_level"] = "month"
    elif n_years > 0:
        output["temporal_overlap_level"] = "year"
    else:
        output["temporal_overlap_level"] = "none"

    available_a = set(series_a["available_variables"])
    available_b = set(series_b["available_variables"])
    output["available_variables_a"] = "|".join(sorted(available_a))
    output["available_variables_b"] = "|".join(sorted(available_b))
    output["common_variables_any"] = "|".join(sorted(available_a.intersection(available_b)))

    same_resolution = bool(output["same_resolution"])
    support = support_for_resolution(row_a.get("resolution")) if same_resolution else ""
    output.update(compare_native_values(series_a, series_b, support, cfg))

    reasons = []
    paired_total = int(output.get("n_paired_values_total", 0) or 0)
    exact_fraction = finite_float(output.get("exact_equal_fraction_all"))
    high_exact = (
        paired_total >= cfg.duplicate_min_paired
        and math.isfinite(exact_fraction)
        and exact_fraction >= cfg.duplicate_exact_fraction
    )
    variable_high_exact = False
    for variable in VARIABLES:
        n_paired = int(output.get("n_paired_{}".format(variable), 0) or 0)
        fraction = finite_float(output.get("exact_equal_fraction_{}".format(variable)))
        if (
            n_paired >= cfg.duplicate_min_paired
            and math.isfinite(fraction)
            and fraction >= cfg.duplicate_exact_fraction
        ):
            variable_high_exact = True
            reasons.append("high_exact_{}_fraction".format(variable))
    if high_exact:
        reasons.append("high_exact_fraction_all_variables")
    if output.get("source_station_id_match"):
        reasons.append("same_source_station_id_text")
    if output.get("station_name_match") and output.get("river_name_match"):
        reasons.append("same_station_and_river_name")
    if output.get("same_current_cluster"):
        reasons.append("already_same_current_cluster")
    if n_years > 0 and paired_total == 0 and same_resolution:
        reasons.append("calendar_overlap_but_no_paired_values")
    if series_a["status"] != "ok" or series_b["status"] != "ok":
        reasons.append("source_series_read_problem")

    if high_exact or variable_high_exact or output.get("source_station_id_match"):
        review_priority = "high"
    elif (
        n_years > 0
        and (
            output.get("station_name_match")
            or output.get("river_name_match")
            or output.get("same_reach")
        )
    ):
        review_priority = "medium"
    elif n_years > 0:
        review_priority = "normal"
    else:
        review_priority = "low"
    output["independence_review_priority"] = review_priority
    output["review_reasons"] = "|".join(sorted(set(reasons)))
    return output


def evaluate_candidates(
    candidates: Mapping[Tuple[int, int], Dict],
    stations: pd.DataFrame,
    cfg: Config,
) -> Tuple[pd.DataFrame, SeriesCache]:
    items = list(candidates.values())
    spatial_rows = [pair_spatial_row(item, stations) for item in items]
    spatial_rows.sort(
        key=lambda row: (
            int(row.get("priority_rank", 9)),
            0 if row.get("same_reach") else 1,
            finite_float(row.get("coordinate_distance_m"))
            if math.isfinite(finite_float(row.get("coordinate_distance_m")))
            else math.inf,
            clean_text(row.get("source_canonical_a")),
            clean_text(row.get("source_canonical_b")),
            clean_text(row.get("station_key_a")),
            clean_text(row.get("station_key_b")),
        )
    )
    if cfg.max_pairs > 0:
        spatial_rows = spatial_rows[: cfg.max_pairs]

    station_lookup = stations.set_index("station_key", drop=False)
    cache = SeriesCache(cfg)
    rows = []
    for count, spatial in enumerate(spatial_rows, start=1):
        row_a = station_lookup.loc[spatial["station_key_a"]]
        row_b = station_lookup.loc[spatial["station_key_b"]]
        rows.append(evaluate_temporal_pair(spatial, row_a, row_b, cache, cfg))
        if count % 100 == 0 or count == len(spatial_rows):
            print("  temporal diagnostics: {:,}/{:,} candidate pairs".format(count, len(spatial_rows)))
    return pd.DataFrame(rows), cache


# ---------------------------------------------------------------------------
# Summaries and reporting
# ---------------------------------------------------------------------------


def safe_fraction(numerator, denominator) -> float:
    numerator_f = finite_float(numerator)
    denominator_f = finite_float(denominator)
    if not (math.isfinite(numerator_f) and math.isfinite(denominator_f) and denominator_f > 0):
        return math.nan
    return numerator_f / denominator_f


def build_source_pair_summary(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory.empty:
        return pd.DataFrame()
    rows = []
    group_columns = ["source_a", "source_b", "priority_group"]
    for keys, group in inventory.groupby(group_columns, sort=True, dropna=False):
        source_a, source_b, priority_group = keys
        row = {
            "source_a": source_a,
            "source_b": source_b,
            "priority_group": priority_group,
            "priority_rank": PRIORITY_GROUP_RANK.get(clean_text(priority_group), 9),
            "n_station_pairs": int(len(group)),
            "n_unique_station_a": int(group["station_key_a"].nunique()),
            "n_unique_station_b": int(group["station_key_b"].nunique()),
            "n_same_reach_pairs": int(group["same_reach"].fillna(False).astype(bool).sum()),
            "n_connected_reach_pairs": int(group["candidate_modes"].fillna("").str.contains("connected_reach").sum()),
            "n_nearby_coordinate_pairs": int(group["candidate_modes"].fillna("").str.contains("nearby_coordinates").sum()),
            "n_same_resolution_pairs": int(group["same_resolution"].fillna(False).astype(bool).sum()),
            "n_pairs_with_common_date": int((pd.to_numeric(group.get("n_common_dates", 0), errors="coerce") > 0).sum()),
            "n_pairs_with_common_month": int((pd.to_numeric(group.get("n_common_months", 0), errors="coerce") > 0).sum()),
            "n_pairs_with_common_year": int((pd.to_numeric(group.get("n_common_years", 0), errors="coerce") > 0).sum()),
            "n_pairs_with_native_values": int((pd.to_numeric(group.get("n_paired_values_total", 0), errors="coerce") > 0).sum()),
            "n_high_review_priority": int(group.get("independence_review_priority", pd.Series(index=group.index, dtype=str)).eq("high").sum()),
        }
        for variable in VARIABLES:
            paired = pd.to_numeric(group.get("n_paired_{}".format(variable), 0), errors="coerce").fillna(0)
            exact = pd.to_numeric(group.get("n_exact_equal_{}".format(variable), 0), errors="coerce").fillna(0)
            row["n_pairs_with_{}".format(variable)] = int((paired > 0).sum())
            row["n_paired_{}".format(variable)] = int(paired.sum())
            row["weighted_exact_fraction_{}".format(variable)] = safe_fraction(exact.sum(), paired.sum())
        total_paired = pd.to_numeric(group.get("n_paired_values_total", 0), errors="coerce").fillna(0)
        total_exact = pd.to_numeric(group.get("n_exact_equal_values_total", 0), errors="coerce").fillna(0)
        row["n_paired_values_total"] = int(total_paired.sum())
        row["weighted_exact_fraction_all"] = safe_fraction(total_exact.sum(), total_paired.sum())
        rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["priority_rank", "n_pairs_with_common_year", "n_station_pairs", "source_a", "source_b"],
        ascending=[True, False, False, True, True],
        kind="mergesort",
    )


def build_review_queue(inventory: pd.DataFrame) -> pd.DataFrame:
    if inventory.empty:
        return inventory.copy()
    common_years = pd.to_numeric(inventory.get("n_common_years", 0), errors="coerce").fillna(0)
    common_months = pd.to_numeric(inventory.get("n_common_months", 0), errors="coerce").fillna(0)
    common_dates = pd.to_numeric(inventory.get("n_common_dates", 0), errors="coerce").fillna(0)
    queue = inventory.loc[(common_years > 0) | (common_months > 0) | (common_dates > 0)].copy()
    if queue.empty:
        return queue
    review_rank = {"high": 1, "medium": 2, "normal": 3, "low": 4, "not_evaluated": 9}
    queue["_review_rank"] = queue["independence_review_priority"].map(review_rank).fillna(9)
    queue["_same_reach_rank"] = np.where(queue["same_reach"].fillna(False), 0, 1)
    queue = queue.sort_values(
        [
            "priority_rank",
            "_review_rank",
            "_same_reach_rank",
            "n_common_dates",
            "n_paired_values_total",
            "coordinate_distance_m",
        ],
        ascending=[True, True, True, False, False, True],
        kind="mergesort",
    )
    return queue.drop(columns=["_review_rank", "_same_reach_rank"])


def build_run_summary(
    stations: pd.DataFrame,
    inventory: pd.DataFrame,
    review_queue: pd.DataFrame,
    candidate_stats: Mapping[str, int],
    cache: SeriesCache,
    cfg: Config,
) -> pd.DataFrame:
    rows = []

    def add(metric: str, value, note: str = "") -> None:
        rows.append({"metric": metric, "value": value, "note": note})

    add("source_station_rows_in_scope", len(stations))
    add("source_datasets_in_scope", stations["source_canonical"].nunique())
    add("spatial_candidate_pairs", len(inventory))
    add("temporal_review_queue_pairs", len(review_queue))
    add("same_reach_pairs", int(inventory.get("same_reach", pd.Series(dtype=bool)).fillna(False).sum()))
    add(
        "connected_reach_pairs",
        int(inventory.get("candidate_modes", pd.Series(dtype=str)).fillna("").str.contains("connected_reach").sum()),
    )
    add(
        "nearby_coordinate_pairs",
        int(inventory.get("candidate_modes", pd.Series(dtype=str)).fillna("").str.contains("nearby_coordinates").sum()),
    )
    if not cfg.skip_temporal and not inventory.empty:
        add("pairs_with_common_date", int((pd.to_numeric(inventory["n_common_dates"], errors="coerce") > 0).sum()))
        add("pairs_with_common_month", int((pd.to_numeric(inventory["n_common_months"], errors="coerce") > 0).sum()))
        add("pairs_with_common_year", int((pd.to_numeric(inventory["n_common_years"], errors="coerce") > 0).sum()))
        add("pairs_with_native_paired_values", int((pd.to_numeric(inventory["n_paired_values_total"], errors="coerce") > 0).sum()))
        add("high_independence_review_pairs", int(inventory["independence_review_priority"].eq("high").sum()))
        add("source_station_files_read", len(cache.audit))
        add("source_station_read_problems", sum(1 for item in cache.audit.values() if item["read_status"] != "ok"))
    for key, value in sorted(candidate_stats.items()):
        add(key, value)
    add("same_resolution_only", int(cfg.same_resolution_only))
    add("max_topology_hops", cfg.max_topology_hops)
    add("nearby_radius_m", cfg.nearby_radius_m)
    add("allowed_flags", "|".join(str(v) for v in sorted(cfg.allowed_flags)))
    add("duplicate_min_paired", cfg.duplicate_min_paired)
    add("duplicate_exact_fraction", cfg.duplicate_exact_fraction)
    return pd.DataFrame(rows)


def write_report(
    path: Path,
    stations: pd.DataFrame,
    inventory: pd.DataFrame,
    source_summary: pd.DataFrame,
    review_queue: pd.DataFrame,
    run_summary: pd.DataFrame,
    errors: pd.DataFrame,
    cfg: Config,
) -> None:
    read_problem_count = 0
    if not inventory.empty and "series_status_a" in inventory.columns:
        read_problem_count = int(
            (
                ~inventory["series_status_a"].isin(["ok", "not_read_skip_temporal"])
                | ~inventory["series_status_b"].isin(["ok", "not_read_skip_temporal"])
            ).sum()
        )
    lines = [
        "# Cross-Source Overlap Inventory",
        "",
        "This is a source-station-level screening diagnostic. It does not use current s5 cluster membership to select candidate pairs, and it does not by itself establish that two records are independent or duplicated.",
        "",
        "## Inputs and settings",
        "",
        "- s5 source-station table: `{}`".format(cfg.s5_csv),
        "- source NetCDF root: `{}`".format(cfg.source_root),
        "- MERIT directory: `{}`".format(cfg.merit_dir),
        "- source families: `{}`".format("|".join(sorted(cfg.source_families))),
        "- same-resolution-only candidate filter: `{}`".format(cfg.same_resolution_only),
        "- maximum topology hops: `{}`".format(cfg.max_topology_hops),
        "- nearby-coordinate radius: `{:.1f} m`".format(cfg.nearby_radius_m),
        "- allowed quality flags for value comparison: `{}`".format("|".join(str(v) for v in sorted(cfg.allowed_flags))),
        "- temporal diagnostics skipped: `{}`".format(cfg.skip_temporal),
        "",
        "## Inventory funnel",
        "",
        "- source-station rows in scope: {:,}".format(len(stations)),
        "- source datasets in scope: {:,}".format(stations["source_canonical"].nunique()),
        "- spatial candidate pairs: {:,}".format(len(inventory)),
        "- pairs with calendar overlap in the review queue: {:,}".format(len(review_queue)),
        "- candidate pairs with a source-series read problem: {:,}".format(read_problem_count),
        "",
        "## Interpretation",
        "",
        "- `same_reach` means the two source stations have the same selected MERIT reach COMID.",
        "- `connected_reach` means their selected reaches are upstream/downstream connected within the configured hop limit.",
        "- `nearby_coordinates` is a high-recall screen and may include different rivers, parallel channels, or inaccurate coordinates.",
        "- common dates/months/years describe calendar support. Value identity is assessed only when temporal resolutions are the same.",
        "- a high exact-value fraction is a duplication-review signal, not proof of shared provenance; original-provider metadata still require manual review.",
        "- low agreement is not proof of independence because sampling methods, TSS/SSC definitions, cross-section support, and timing can differ.",
        "",
        "## Source-pair summary",
        "",
        markdown_table(source_summary, max_rows=40),
        "",
        "## Run summary",
        "",
        markdown_table(run_summary),
    ]
    if not errors.empty:
        lines.extend(
            [
                "",
                "## Topology and runtime warnings",
                "",
                markdown_table(errors, max_rows=50),
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------


def run(cfg: Config) -> Dict[str, object]:
    print("Loading source-station table: {}".format(cfg.s5_csv))
    stations = load_station_table(cfg)
    print(
        "Stations in scope: {:,} from {:,} source datasets".format(
            len(stations), stations["source_canonical"].nunique()
        )
    )

    candidates: Dict[Tuple[int, int], Dict] = {}
    candidate_stats: Dict[str, int] = {}
    runtime_errors: List[Dict] = []

    print("Generating same-reach candidates...")
    candidate_stats.update(generate_same_reach_candidates(stations, candidates, cfg))
    print("  candidate pairs after same-reach scan: {:,}".format(len(candidates)))

    print("Generating connected-reach candidates...")
    connected_stats, connected_errors = generate_connected_candidates(stations, candidates, cfg)
    candidate_stats.update(connected_stats)
    runtime_errors.extend(connected_errors)
    print("  candidate pairs after connected-reach scan: {:,}".format(len(candidates)))

    print("Generating nearby-coordinate candidates...")
    candidate_stats.update(generate_nearby_candidates(stations, candidates, cfg))
    print("  total unique spatial candidate pairs: {:,}".format(len(candidates)))

    print("Evaluating temporal and value overlap...")
    inventory, cache = evaluate_candidates(candidates, stations, cfg)
    if not inventory.empty:
        inventory = inventory.sort_values(
            [
                "priority_rank",
                "source_canonical_a",
                "source_canonical_b",
                "same_reach",
                "n_common_years" if "n_common_years" in inventory.columns else "priority_rank",
                "station_key_a",
                "station_key_b",
            ],
            ascending=[True, True, True, False, False, True, True],
            kind="mergesort",
        )

    source_summary = build_source_pair_summary(inventory)
    review_queue = build_review_queue(inventory)
    station_read_summary = pd.DataFrame(list(cache.audit.values()))
    if not station_read_summary.empty:
        station_read_summary = station_read_summary.sort_values(
            ["read_status", "source", "station_key"], kind="mergesort"
        )
    errors = pd.DataFrame(runtime_errors, columns=["stage", "item", "error"])
    run_summary = build_run_summary(
        stations,
        inventory,
        review_queue,
        candidate_stats,
        cache,
        cfg,
    )

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "inventory": cfg.out_dir / "cross_source_overlap_inventory.csv.gz",
        "review_queue": cfg.out_dir / "cross_source_overlap_review_queue.csv",
        "source_pair_summary": cfg.out_dir / "cross_source_overlap_source_pair_summary.csv",
        "station_read_summary": cfg.out_dir / "cross_source_overlap_station_read_summary.csv",
        "run_summary": cfg.out_dir / "cross_source_overlap_run_summary.csv",
        "errors": cfg.out_dir / "cross_source_overlap_errors.csv",
        "report": cfg.out_dir / "cross_source_overlap_report.md",
    }
    inventory.to_csv(paths["inventory"], index=False, compression="gzip")
    review_queue.to_csv(paths["review_queue"], index=False)
    source_summary.to_csv(paths["source_pair_summary"], index=False)
    station_read_summary.to_csv(paths["station_read_summary"], index=False)
    run_summary.to_csv(paths["run_summary"], index=False)
    errors.to_csv(paths["errors"], index=False)
    write_report(
        paths["report"],
        stations,
        inventory,
        source_summary,
        review_queue,
        run_summary,
        errors,
        cfg,
    )

    return {
        "paths": paths,
        "stations": stations,
        "inventory": inventory,
        "review_queue": review_queue,
        "source_pair_summary": source_summary,
        "station_read_summary": station_read_summary,
        "run_summary": run_summary,
        "errors": errors,
    }


def default_config() -> Config:
    output_r_root = get_output_r_root(REPO_ROOT)
    source_root = (output_r_root / S2_ORGANIZED_DIR).resolve()
    default_merit = Path(DEFAULT_MERIT_DIR).expanduser().resolve() if clean_text(DEFAULT_MERIT_DIR) else Path("")
    return Config(
        s5_csv=(output_r_root / S5_BASIN_CLUSTERED_CSV).resolve(),
        source_root=source_root,
        merit_dir=default_merit,
        out_dir=(REPO_ROOT / "validate" / "output" / "cross_source_overlap").resolve(),
        source_families={"in_situ"},
        include_sources=set(),
        exclude_sources=set(),
        same_resolution_only=False,
        max_topology_hops=DEFAULT_MAX_TOPOLOGY_HOPS,
        nearby_radius_m=DEFAULT_NEARBY_RADIUS_M,
        skip_connected=False,
        skip_nearby=False,
        skip_temporal=False,
        allowed_flags={0, 1, 2},
        cache_size=DEFAULT_CACHE_SIZE,
        near_rtol=DEFAULT_NEAR_RTOL,
        near_atol=DEFAULT_NEAR_ATOL,
        duplicate_min_paired=DEFAULT_DUPLICATE_MIN_PAIRED,
        duplicate_exact_fraction=DEFAULT_DUPLICATE_EXACT_FRACTION,
        max_pairs=0,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    defaults = default_config()
    parser = argparse.ArgumentParser(
        description=(
            "Generate a source-station-level cross-source spatial/temporal overlap inventory "
            "without using existing cluster membership as a candidate criterion."
        )
    )
    parser.add_argument("--s5-csv", default=str(defaults.s5_csv))
    parser.add_argument("--source-root", default=str(defaults.source_root))
    parser.add_argument("--merit-dir", default=str(defaults.merit_dir))
    parser.add_argument("--out-dir", default=str(defaults.out_dir))
    parser.add_argument(
        "--source-families",
        default="in_situ",
        help="Comma-separated source families to include. Default: in_situ",
    )
    parser.add_argument(
        "--include-sources",
        default="",
        help="Optional comma-separated canonical/display source names to retain.",
    )
    parser.add_argument(
        "--exclude-sources",
        default="",
        help="Optional comma-separated canonical/display source names to exclude.",
    )
    parser.add_argument(
        "--same-resolution-only",
        action="store_true",
        help="Generate candidates only when both source stations have the same temporal resolution.",
    )
    parser.add_argument(
        "--max-topology-hops",
        type=int,
        default=defaults.max_topology_hops,
        help="Maximum upstream/downstream MERIT reach hops. Default: 1",
    )
    parser.add_argument(
        "--nearby-radius-m",
        type=float,
        default=defaults.nearby_radius_m,
        help="Coordinate-only high-recall candidate radius. Default: 5000 m",
    )
    parser.add_argument("--skip-connected", action="store_true")
    parser.add_argument("--skip-nearby", action="store_true")
    parser.add_argument("--skip-temporal", action="store_true")
    parser.add_argument(
        "--allowed-flags",
        default="0,1,2",
        help="Quality flags retained for exact-value comparison. Default: 0,1,2",
    )
    parser.add_argument("--cache-size", type=int, default=defaults.cache_size)
    parser.add_argument("--near-rtol", type=float, default=defaults.near_rtol)
    parser.add_argument("--near-atol", type=float, default=defaults.near_atol)
    parser.add_argument(
        "--duplicate-min-paired",
        type=int,
        default=defaults.duplicate_min_paired,
        help="Minimum paired values before a high exact-match review flag. Default: 20",
    )
    parser.add_argument(
        "--duplicate-exact-fraction",
        type=float,
        default=defaults.duplicate_exact_fraction,
        help="Exact-value fraction triggering high-priority provenance review. Default: 0.95",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=0,
        help="Optional cap after spatial ranking, mainly for test runs. 0 means unlimited.",
    )
    args = parser.parse_args(argv)

    allowed_flags = parse_flags(args.allowed_flags)
    source_families = parse_csv_set(args.source_families, lambda value: clean_text(value).lower())
    include_sources = parse_csv_set(args.include_sources, normalize_source_name)
    exclude_sources = parse_csv_set(args.exclude_sources, normalize_source_name)
    if not source_families:
        parser.error("--source-families cannot be empty")
    if args.max_topology_hops < 0:
        parser.error("--max-topology-hops must be >= 0")
    if args.nearby_radius_m < 0:
        parser.error("--nearby-radius-m must be >= 0")
    if not (0.0 <= args.duplicate_exact_fraction <= 1.0):
        parser.error("--duplicate-exact-fraction must be between 0 and 1")

    return Config(
        s5_csv=Path(args.s5_csv).expanduser().resolve(),
        source_root=Path(args.source_root).expanduser().resolve(),
        merit_dir=Path(args.merit_dir).expanduser().resolve(),
        out_dir=Path(args.out_dir).expanduser().resolve(),
        source_families=source_families,
        include_sources=include_sources,
        exclude_sources=exclude_sources,
        same_resolution_only=args.same_resolution_only,
        max_topology_hops=args.max_topology_hops,
        nearby_radius_m=args.nearby_radius_m,
        skip_connected=args.skip_connected,
        skip_nearby=args.skip_nearby,
        skip_temporal=args.skip_temporal,
        allowed_flags=allowed_flags,
        cache_size=args.cache_size,
        near_rtol=args.near_rtol,
        near_atol=args.near_atol,
        duplicate_min_paired=args.duplicate_min_paired,
        duplicate_exact_fraction=args.duplicate_exact_fraction,
        max_pairs=args.max_pairs,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    result = run(cfg)
    inventory = result["inventory"]
    review_queue = result["review_queue"]
    print("\nCross-source overlap inventory complete")
    print("Output directory: {}".format(cfg.out_dir))
    print("Spatial candidate pairs: {:,}".format(len(inventory)))
    print("Pairs with calendar overlap: {:,}".format(len(review_queue)))
    if not cfg.skip_temporal and not inventory.empty:
        native = int((pd.to_numeric(inventory["n_paired_values_total"], errors="coerce") > 0).sum())
        high = int(inventory["independence_review_priority"].eq("high").sum())
        print("Pairs with native-support paired values: {:,}".format(native))
        print("High-priority provenance-review pairs: {:,}".format(high))
    for label, path in result["paths"].items():
        print("  {}: {}".format(label, path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

