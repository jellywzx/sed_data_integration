#!/usr/bin/env python3
"""Sensitivity analysis for MERIT-Basins station matching thresholds.

This script is designed for the ``sed_data_integration`` repository.  It is
read-only with respect to the s3/s4/s5 production outputs and writes an
independent validation package under ``validate/output``.

Default analysis
----------------
The default run implements the compact reviewer-facing experiment described in
Sect. 3.2 of the manuscript:

* maximum conditionally accepted station-to-reach distance: 300, 500, 1000 m;
* drainage-area classification cutoffs scaled by 0.5, 1.0, and 1.5, i.e.
  (0.05, 0.15), (0.10, 0.30), and (0.15, 0.45) in absolute log10 area error;
* a fixed-kilometre candidate-search window, avoiding latitude-dependent loss
  of candidates caused by a fixed 1.0 degree longitude box;
* a single legacy 1.0 degree baseline scenario for reproduction checks.

The script reports both source-station and re-clustered release-unit resolved
fractions.  The latter reuses ``basin_station_merge.py`` so that the reported
cluster denominator follows the repository's complete-linkage merge logic.

Extended analysis
-----------------
Command-line options also expose the other hard-coded choices: candidate search
radius, relative weights of the area and distance score terms, and the station
merge distance/area thresholds.  This permits a larger grid without changing
production scripts.

Typical use
-----------
Run from the repository root or place this file in ``validate/``::

    python validate/s13_basin_matching_threshold_sensitivity.py \
        --merit-dir /path/to/MERIT_Hydro_v07_Basins_v01_bugfix1 \
        --workers 24

Minimal 3 x 3 table only (fixed-km search plus one legacy baseline)::

    python validate/s13_basin_matching_threshold_sensitivity.py \
        --distance-limits-m 300 500 1000 \
        --da-scales 0.5 1.0 1.5

Example extended grid::

    python validate/s13_basin_matching_threshold_sensitivity.py \
        --candidate-radii-km 60 120 180 \
        --score-weight-pairs 1:1 2:1 1:2 \
        --merge-distances-m 500 1000 1500

Outputs
-------
* basin_matching_sensitivity_summary.csv
* basin_matching_sensitivity_summary.md
* basin_matching_sensitivity_station_details.csv.gz
* basin_matching_sensitivity_metadata.json
* baseline_reproduction_differences.csv (only when an s4 baseline is present)
"""

from __future__ import annotations

import argparse
import itertools
import json
import logging
import math
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from shapely.geometry import Point


LOGGER = logging.getLogger("basin_matching_sensitivity")
EARTH_METRES_PER_DEGREE = 111_320.0
DEFAULT_SATELLITE_SOURCES = {"gsed", "riversed", "rivsed", "dethier"}
SATELLITE_OBSERVATION_TYPES = {
    "satellite",
    "remote_sensing",
    "remote_sensing_observation",
    "satellite_observation",
}
MAIN_RESOLUTIONS = {"daily", "monthly", "annual"}


# ---------------------------------------------------------------------------
# Repository discovery and imports
# ---------------------------------------------------------------------------

def _find_repo_root(start: Path) -> Path:
    """Find the repository root containing the production matching modules."""
    candidates = [start] + list(start.parents)
    for candidate in candidates:
        if (
            (candidate / "pipeline_paths.py").is_file()
            and (candidate / "basin_tracer.py").is_file()
            and (candidate / "basin_station_merge.py").is_file()
        ):
            return candidate
    raise RuntimeError(
        "Could not find the sed_data_integration repository root. Place this "
        "script under the repository (recommended: validate/) or run it from "
        "a directory below the repository root."
    )


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = _find_repo_root(SCRIPT_PATH.parent)
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from basin_station_merge import load_station_to_basin_cluster_map  # noqa: E402
from basin_tracer import UpstreamBasinTracer  # noqa: E402
from pipeline_paths import (  # noqa: E402
    S3_COLLECTED_CSV,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    S6_QUALITY_ORDER_CSV,
    get_output_r_root,
)

OUTPUT_R_ROOT = get_output_r_root(REPO_ROOT)
DEFAULT_S3_CSV = OUTPUT_R_ROOT / S3_COLLECTED_CSV
DEFAULT_S4_CSV = OUTPUT_R_ROOT / S4_UPSTREAM_CSV
DEFAULT_S5_CSV = OUTPUT_R_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_QUALITY_ORDER_CSV = OUTPUT_R_ROOT / S6_QUALITY_ORDER_CSV
DEFAULT_MERIT_DIR = Path(
    os.environ.get(
        "MERIT_DIR",
        str(OUTPUT_R_ROOT.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"),
    )
)
DEFAULT_OUT_DIR = REPO_ROOT / "validate" / "output" / "s15_basin_matching_threshold_sensitivity"


# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SelectionConfig:
    search_mode: str
    candidate_radius_km: float
    area_weight: float
    distance_weight: float

    @property
    def key(self) -> str:
        return (
            f"search-{self.search_mode}__r-{self.candidate_radius_km:g}km"
            f"__w-{self.area_weight:g}-{self.distance_weight:g}"
        )


@dataclass(frozen=True)
class MatchConfig:
    selection: SelectionConfig
    distance_limit_m: float
    da_scale: float
    close_distance_m: float
    base_area_match_cutoff: float
    base_area_approx_cutoff: float

    @property
    def area_match_cutoff(self) -> float:
        return self.base_area_match_cutoff * self.da_scale

    @property
    def area_approx_cutoff(self) -> float:
        return self.base_area_approx_cutoff * self.da_scale

    @property
    def key(self) -> str:
        return (
            f"{self.selection.key}__d-{self.distance_limit_m:g}m"
            f"__da-{self.da_scale:g}"
        )


@dataclass(frozen=True)
class MergeConfig:
    max_station_distance_m: float
    max_upstream_rel_error: float

    @property
    def key(self) -> str:
        return (
            f"merge-d-{self.max_station_distance_m:g}m"
            f"__merge-a-{self.max_upstream_rel_error:g}"
        )


# ---------------------------------------------------------------------------
# Normalization and pure policy helpers
# ---------------------------------------------------------------------------

def _normalize_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _normalize_token(value) -> str:
    return _normalize_text(value).lower().replace("-", "_").replace(" ", "_")


def _finite_float(value, default=np.nan) -> float:
    try:
        number = float(value)
    except Exception:
        return float(default)
    return number if math.isfinite(number) else float(default)


def _valid_positive(value) -> bool:
    number = _finite_float(value)
    return bool(math.isfinite(number) and number > 0.0)


def _is_satellite_row(row: pd.Series) -> bool:
    observation_type = _normalize_token(row.get("observation_type"))
    source = _normalize_token(row.get("source"))
    return (
        observation_type in SATELLITE_OBSERVATION_TYPES
        or source in DEFAULT_SATELLITE_SOURCES
    )


def _classify_area_quality(
    log10_area_error: float,
    has_reported_area: bool,
    area_match_cutoff: float,
    area_approx_cutoff: float,
) -> str:
    """Apply configurable versions of the current 0.10/0.30 cutoffs."""
    if not has_reported_area or not math.isfinite(log10_area_error):
        return "distance_only"
    absolute_error = abs(log10_area_error)
    if absolute_error < area_match_cutoff:
        return "area_matched"
    if absolute_error < area_approx_cutoff:
        return "area_approximate"
    return "area_mismatch"


def classify_basin_result_sensitivity(
    *,
    basin_id,
    distance_m,
    point_in_local: bool,
    match_quality: str,
    close_distance_m: float,
    distance_limit_m: float,
) -> Tuple[str, str]:
    """Configurable equivalent of ``basin_policy.classify_basin_result``.

    The baseline ``close_distance_m=300`` and ``distance_limit_m=1000``
    reproduces the ordinary gauge-station policy.  When the maximum limit is
    300 m the conditional band disappears.  Area mismatch is rejected before
    distance acceptance, matching the production decision order.
    """
    basin_number = _finite_float(basin_id)
    distance = _finite_float(distance_m)
    if not math.isfinite(basin_number) or basin_number <= 0:
        return "unresolved", "no_match"
    if match_quality == "area_mismatch":
        return "unresolved", "area_mismatch"
    if not math.isfinite(distance):
        return "unresolved", "no_match"

    automatic_limit = min(float(close_distance_m), float(distance_limit_m))
    if distance <= automatic_limit:
        return "resolved", "ok"
    if distance <= float(distance_limit_m) and (
        match_quality in {"area_matched", "area_approximate"}
        or bool(point_in_local)
    ):
        return "resolved", "ok"
    if distance > float(distance_limit_m):
        return "unresolved", "large_offset"
    return "unresolved", "geometry_inconsistent"


def _parse_weight_pairs(values: Sequence[str]) -> List[Tuple[float, float]]:
    pairs: List[Tuple[float, float]] = []
    for raw in values:
        text = str(raw).strip()
        if ":" not in text:
            raise argparse.ArgumentTypeError(
                f"Invalid score-weight pair {raw!r}; expected AREA:DISTANCE, e.g. 1:1"
            )
        left, right = text.split(":", 1)
        area_weight = float(left)
        distance_weight = float(right)
        if area_weight < 0 or distance_weight < 0:
            raise argparse.ArgumentTypeError("Score weights must be non-negative")
        if area_weight == 0 and distance_weight == 0:
            raise argparse.ArgumentTypeError("At least one score weight must be positive")
        pairs.append((area_weight, distance_weight))
    return pairs


# ---------------------------------------------------------------------------
# Candidate search: legacy degree box and fixed-kilometre box
# ---------------------------------------------------------------------------

class SensitivityTracer(UpstreamBasinTracer):
    """Production tracer with configurable, audit-friendly candidate search."""

    def __init__(self, merit_basins_dir: str, max_candidate_radius_m: float):
        super().__init__(merit_basins_dir)
        self.max_candidate_radius_m = float(max_candidate_radius_m)
        self._local_geometry_cache: Dict[int, object] = {}

    @staticmethod
    def _boxes_intersect(left: Tuple[float, float, float, float], right) -> bool:
        lminx, lminy, lmaxx, lmaxy = left
        rminx, rminy, rmaxx, rmaxy = right
        return not (
            lmaxx < rminx or rmaxx < lminx or lmaxy < rminy or rmaxy < lminy
        )

    @staticmethod
    def _legacy_degree_boxes(lon: float, lat: float) -> List[Tuple[float, float, float, float]]:
        return [(lon - 1.0, lat - 1.0, lon + 1.0, lat + 1.0)]

    @staticmethod
    def _fixed_km_boxes(
        lon: float,
        lat: float,
        radius_m: float,
    ) -> List[Tuple[float, float, float, float]]:
        """Return one or two WGS84 boxes guaranteed to cover a metric radius.

        Longitude width increases toward the poles.  Dateline-crossing boxes are
        split so they can be passed to GeoPandas spatial indexes.
        """
        lat_delta = radius_m / EARTH_METRES_PER_DEGREE
        # Use the poleward edge of the latitude span for a conservative
        # longitude width. The final exact metric-distance filter removes the
        # intentional over-selection introduced by this bounding box.
        poleward_abs_lat = min(89.999999, abs(lat) + lat_delta)
        cos_lat = abs(math.cos(math.radians(poleward_abs_lat)))
        if cos_lat < 1.0e-6:
            lon_delta = 180.0
        else:
            lon_delta = min(180.0, radius_m / (EARTH_METRES_PER_DEGREE * cos_lat))

        min_lat = max(-90.0, lat - lat_delta)
        max_lat = min(90.0, lat + lat_delta)
        min_lon = lon - lon_delta
        max_lon = lon + lon_delta

        if min_lon >= -180.0 and max_lon <= 180.0:
            return [(min_lon, min_lat, max_lon, max_lat)]
        if min_lon < -180.0:
            return [
                (min_lon + 360.0, min_lat, 180.0, max_lat),
                (-180.0, min_lat, max_lon, max_lat),
            ]
        return [
            (min_lon, min_lat, 180.0, max_lat),
            (-180.0, min_lat, max_lon - 360.0, max_lat),
        ]

    def _candidate_region_codes(
        self,
        lon: float,
        lat: float,
        boxes: Sequence[Tuple[float, float, float, float]],
        mode: str,
    ) -> List[str]:
        if mode == "legacy_degree":
            # Exact current behavior: only regions whose total bounds contain
            # the station point are loaded.
            return self._get_pfaf_level1_codes(lon, lat)

        # Fixed-km mode also includes neighbouring Pfaf regions intersecting the
        # search window, avoiding a boundary omission present in point-only
        # region selection.
        codes = []
        for pfaf_code, bounds in self._region_bounds.items():
            if any(self._boxes_intersect(box, bounds) for box in boxes):
                codes.append(pfaf_code)
        return codes

    def gather_candidates(
        self,
        lon: float,
        lat: float,
        mode: str,
    ) -> Optional[pd.DataFrame]:
        if not (math.isfinite(lon) and math.isfinite(lat)):
            return None
        if mode not in {"legacy_degree", "fixed_km"}:
            raise ValueError(f"Unsupported search mode: {mode}")

        boxes = (
            self._legacy_degree_boxes(lon, lat)
            if mode == "legacy_degree"
            else self._fixed_km_boxes(lon, lat, self.max_candidate_radius_m)
        )
        pfaf_codes = self._candidate_region_codes(lon, lat, boxes, mode)
        if not pfaf_codes:
            return None

        all_candidates: List[pd.DataFrame] = []
        for pfaf_code in pfaf_codes:
            rivers = self._load_level1_rivers(pfaf_code)
            if rivers is None or len(rivers) == 0:
                continue

            possible_positions = set()
            for box in boxes:
                possible_positions.update(rivers.sindex.intersection(box))
            if not possible_positions:
                continue

            candidates = rivers.iloc[sorted(possible_positions)].copy()
            candidates["dist_m"] = self._distance_point_to_geoms_m(
                candidates.geometry, lon, lat
            )
            candidates = candidates[
                pd.to_numeric(candidates["dist_m"], errors="coerce")
                < self.max_candidate_radius_m
            ].copy()
            if len(candidates) == 0:
                continue
            candidates["pfaf_code"] = str(pfaf_code)
            all_candidates.append(
                candidates[["COMID", "uparea", "dist_m", "pfaf_code"]]
            )

        if not all_candidates:
            return None
        merged = pd.concat(all_candidates, ignore_index=True)
        merged["COMID"] = pd.to_numeric(merged["COMID"], errors="coerce")
        merged["uparea"] = pd.to_numeric(merged["uparea"], errors="coerce")
        merged["dist_m"] = pd.to_numeric(merged["dist_m"], errors="coerce")
        merged = merged.dropna(subset=["COMID", "dist_m"])
        merged["COMID"] = merged["COMID"].astype("int64")
        merged = merged.sort_values(["dist_m", "COMID"]).drop_duplicates(
            subset=["COMID"], keep="first"
        )
        return merged.reset_index(drop=True) if len(merged) else None

    def point_in_local_catchment(self, comid: int, lon: float, lat: float) -> bool:
        comid = int(comid)
        if comid not in self._local_geometry_cache:
            self._local_geometry_cache[comid] = self.get_upstream_basin_polygon({comid})
        geometry = self._local_geometry_cache[comid]
        if geometry is None or geometry.is_empty:
            return False
        return bool(geometry.covers(Point(float(lon), float(lat))))

    def clear_cache(self):
        """Release loaded river/catchment data and per-COMID geometry cache.

        The production tracer's ``clear_cache`` drops the level-01 river frames,
        topology and level-02 catchments, but not this subclass's per-COMID
        polygon cache.  Override it so a region change fully frees memory.
        """
        super().clear_cache()
        self._local_geometry_cache.clear()


def select_best_reach(
    candidates: Optional[pd.DataFrame],
    reported_area,
    config: SelectionConfig,
) -> Dict[str, object]:
    """Select the best candidate using the production score with free weights."""
    failed = {
        "basin_id": np.nan,
        "uparea_merit": np.nan,
        "distance_m": np.nan,
        "pfaf_code": "",
        "log10_area_error": np.nan,
        "has_reported_area": False,
        "candidate_count": 0,
        "score": np.nan,
    }
    if candidates is None or len(candidates) == 0:
        return failed

    radius_m = float(config.candidate_radius_km) * 1000.0
    subset = candidates[candidates["dist_m"] < radius_m].copy()
    if len(subset) == 0:
        return failed

    has_area = _valid_positive(reported_area)
    if has_area:
        reported = float(reported_area)
        subset = subset[np.isfinite(subset["uparea"]) & (subset["uparea"] > 0)].copy()
        if len(subset) == 0:
            return failed
        subset["area_ratio"] = subset["uparea"] / reported
        subset["area_score"] = np.abs(
            np.log10(subset["area_ratio"].clip(0.001, 1000.0))
        )
        subset["distance_score"] = subset["dist_m"] / radius_m
        subset["score"] = (
            float(config.area_weight) * subset["area_score"]
            + float(config.distance_weight) * subset["distance_score"]
        )
        best = subset.sort_values(["score", "dist_m", "COMID"]).iloc[0]
        log10_area_error = float(np.log10(best["uparea"] / reported))
        score = float(best["score"])
    else:
        best = subset.sort_values(["dist_m", "COMID"]).iloc[0]
        log10_area_error = np.nan
        score = float(best["dist_m"] / radius_m)

    return {
        "basin_id": int(best["COMID"]),
        "uparea_merit": float(best["uparea"]),
        "distance_m": float(best["dist_m"]),
        "pfaf_code": str(best["pfaf_code"]),
        "log10_area_error": log10_area_error,
        "has_reported_area": bool(has_area),
        "candidate_count": int(len(subset)),
        "score": score,
    }


# ---------------------------------------------------------------------------
# Multiprocessing station evaluation
# ---------------------------------------------------------------------------

_WORKER_TRACER: Optional[SensitivityTracer] = None
_WORKER_SELECTIONS: List[SelectionConfig] = []
_WORKER_MATCHES: List[MatchConfig] = []
_WORKER_MODES: List[str] = []
_WORKER_LAST_REGION: Optional[str] = None


def _init_worker(
    merit_dir: str,
    max_candidate_radius_m: float,
    selection_dicts: List[dict],
    match_dicts: List[dict],
) -> None:
    global _WORKER_TRACER, _WORKER_SELECTIONS, _WORKER_MATCHES, _WORKER_MODES
    global _WORKER_LAST_REGION
    _WORKER_TRACER = SensitivityTracer(merit_dir, max_candidate_radius_m)
    _WORKER_LAST_REGION = None
    _WORKER_SELECTIONS = [SelectionConfig(**item) for item in selection_dicts]
    selection_lookup = {item.key: item for item in _WORKER_SELECTIONS}
    _WORKER_MATCHES = []
    for item in match_dicts:
        selection_key = item.pop("selection_key")
        _WORKER_MATCHES.append(
            MatchConfig(selection=selection_lookup[selection_key], **item)
        )
    _WORKER_MODES = sorted({item.search_mode for item in _WORKER_SELECTIONS})


def _process_station_chunk(chunk: Tuple[str, List[dict]]) -> List[dict]:
    if _WORKER_TRACER is None:
        raise RuntimeError("Worker tracer was not initialized")

    region_key, station_rows = chunk
    global _WORKER_LAST_REGION
    if _WORKER_LAST_REGION != region_key:
        _WORKER_TRACER.clear_cache()
        _WORKER_LAST_REGION = region_key

    output: List[dict] = []
    matches_by_selection: Dict[str, List[MatchConfig]] = {}
    for match in _WORKER_MATCHES:
        matches_by_selection.setdefault(match.selection.key, []).append(match)

    for station in station_rows:
        station_key = _normalize_text(station.get("station_key"))
        station_id = int(station["station_id"])
        lon = float(station["lon"])
        lat = float(station["lat"])
        reported_area = station.get("reported_area")
        source = _normalize_text(station.get("source"))
        resolution = _normalize_text(station.get("resolution"))
        observation_type = _normalize_text(station.get("observation_type"))

        candidates_by_mode = {
            mode: _WORKER_TRACER.gather_candidates(lon, lat, mode)
            for mode in _WORKER_MODES
        }

        for selection in _WORKER_SELECTIONS:
            selected = select_best_reach(
                candidates_by_mode.get(selection.search_mode),
                reported_area,
                selection,
            )
            basin_id = selected["basin_id"]
            point_in_local = False
            if math.isfinite(_finite_float(basin_id)):
                point_in_local = _WORKER_TRACER.point_in_local_catchment(
                    int(basin_id), lon, lat
                )

            for match in matches_by_selection[selection.key]:
                match_quality = _classify_area_quality(
                    float(selected["log10_area_error"]),
                    bool(selected["has_reported_area"]),
                    match.area_match_cutoff,
                    match.area_approx_cutoff,
                )
                basin_status, basin_flag = classify_basin_result_sensitivity(
                    basin_id=basin_id,
                    distance_m=selected["distance_m"],
                    point_in_local=point_in_local,
                    match_quality=match_quality,
                    close_distance_m=match.close_distance_m,
                    distance_limit_m=match.distance_limit_m,
                )
                output.append(
                    {
                        "match_scenario_id": match.key,
                        "selection_scenario_id": selection.key,
                        "station_key": station_key,
                        "station_id": station_id,
                        "source": source,
                        "resolution": resolution,
                        "observation_type": observation_type,
                        "lon": lon,
                        "lat": lat,
                        "reported_area": _finite_float(reported_area),
                        "search_mode": selection.search_mode,
                        "candidate_radius_km": selection.candidate_radius_km,
                        "area_weight": selection.area_weight,
                        "distance_weight": selection.distance_weight,
                        "distance_limit_m": match.distance_limit_m,
                        "close_distance_m": match.close_distance_m,
                        "da_scale": match.da_scale,
                        "area_match_cutoff": match.area_match_cutoff,
                        "area_approx_cutoff": match.area_approx_cutoff,
                        "basin_id": basin_id,
                        "uparea_merit": selected["uparea_merit"],
                        "distance_m": selected["distance_m"],
                        "pfaf_code": selected["pfaf_code"],
                        "area_error": selected["log10_area_error"],
                        "match_quality": match_quality,
                        "point_in_local": bool(point_in_local),
                        "candidate_count": selected["candidate_count"],
                        "selection_score": selected["score"],
                        "basin_status": basin_status,
                        "basin_flag": basin_flag,
                    }
                )
    return output


def _chunked(records: Sequence[dict], size: int) -> Iterable[List[dict]]:
    for start in range(0, len(records), size):
        yield list(records[start : start + size])


def _assign_primary_regions(records: List[dict], merit_dir: str) -> List[dict]:
    """Tag each station with its primary Pfaf level-01 region.

    Uses only the cheap total-bounds index (no river shapefile is loaded), so
    stations can later be grouped so each worker touches one region at a time.
    """
    region_tracer = UpstreamBasinTracer(merit_dir)
    for record in records:
        codes = region_tracer._get_pfaf_level1_codes(
            float(record["lon"]), float(record["lat"])
        )
        record["primary_region"] = sorted(codes)[0] if codes else "unassigned"
    return records


# ---------------------------------------------------------------------------
# Input preparation and scenario grid
# ---------------------------------------------------------------------------

def _require_columns(df: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def load_station_scope(
    s3_csv: Path,
    s5_csv: Optional[Path],
    scope: str,
    max_stations: Optional[int],
) -> pd.DataFrame:
    if not s3_csv.is_file():
        raise FileNotFoundError(f"s3 CSV not found: {s3_csv}")
    stations = pd.read_csv(s3_csv, low_memory=False)
    _require_columns(
        stations,
        ["station_key", "station_id", "lat", "lon", "source", "resolution"],
        "s3 CSV",
    )
    if "reported_area" not in stations.columns:
        stations["reported_area"] = np.nan
    if "observation_type" not in stations.columns:
        stations["observation_type"] = ""

    stations["station_key"] = stations["station_key"].fillna("").astype(str).str.strip()
    stations["station_id"] = pd.to_numeric(stations["station_id"], errors="coerce")
    stations["lat"] = pd.to_numeric(stations["lat"], errors="coerce")
    stations["lon"] = pd.to_numeric(stations["lon"], errors="coerce")
    stations["reported_area"] = pd.to_numeric(stations["reported_area"], errors="coerce")

    invalid = (
        stations["station_key"].eq("")
        | stations["station_id"].isna()
        | stations["lat"].isna()
        | stations["lon"].isna()
        | ~stations["lat"].between(-90.0, 90.0)
        | ~stations["lon"].between(-180.0, 180.0)
    )
    if invalid.any():
        raise ValueError(
            f"s3 CSV has {int(invalid.sum())} rows with invalid key/id/coordinates; "
            "the production s4 script also requires all of these fields."
        )
    if stations["station_key"].duplicated().any():
        raise ValueError("s3 CSV contains duplicate station_key values")
    if stations["station_id"].duplicated().any():
        raise ValueError("s3 CSV contains duplicate station_id values")
    stations["station_id"] = stations["station_id"].astype("int64")

    if scope in {"main_all", "release_main"}:
        resolution_mask = stations["resolution"].map(_normalize_token).isin(MAIN_RESOLUTIONS)
        satellite_mask = stations.apply(_is_satellite_row, axis=1)
        stations = stations.loc[resolution_mask & ~satellite_mask].copy()

    if scope == "release_main":
        if s5_csv is None or not s5_csv.is_file():
            LOGGER.warning(
                "s5 CSV is unavailable; falling back from release_main to all main-resolution s3 stations"
            )
        else:
            s5 = pd.read_csv(
                s5_csv, usecols=lambda name: name in {"station_key", "cluster_id"}
            )
            _require_columns(s5, ["station_key", "cluster_id"], "s5 CSV")
            s5["station_key"] = s5["station_key"].fillna("").astype(str).str.strip()
            released_keys = set(s5["station_key"])
            stations = stations[stations["station_key"].isin(released_keys)].copy()
            stations = stations.merge(
                s5[["station_key", "cluster_id"]].drop_duplicates("station_key"),
                on="station_key",
                how="left",
                validate="one_to_one",
            ).rename(columns={"cluster_id": "release_cluster_id"})

    stations = stations.sort_values(["lon", "lat", "station_id"]).reset_index(drop=True)
    if max_stations is not None:
        stations = stations.head(int(max_stations)).copy()
    if len(stations) == 0:
        raise ValueError("The selected station scope is empty")
    return stations


def load_published_cluster_ids(quality_order_csv: Optional[Path]) -> Optional[set]:
    """Return the set of production cluster_id values with n_publish_rows > 0.

    These are exactly the clusters published in ``sed_reference_release`` (i.e.
    clusters with at least one SSC/SSL record).  Returns None when the quality
    order CSV is unavailable so the caller can fall back to all clusters.
    """
    if quality_order_csv is None or not quality_order_csv.is_file():
        return None
    quality = pd.read_csv(
        quality_order_csv, usecols=["cluster_id", "n_publish_rows"], low_memory=False
    )
    publishable = quality["n_publish_rows"] > 0
    return set(
        pd.to_numeric(
            quality.loc[publishable, "cluster_id"], errors="coerce"
        ).dropna().astype("int64").tolist()
    )


def build_scenarios(args) -> Tuple[List[SelectionConfig], List[MatchConfig], List[MergeConfig]]:
    weight_pairs = _parse_weight_pairs(args.score_weight_pairs)
    selection_set = set()
    for mode, radius, (area_weight, distance_weight) in itertools.product(
        args.search_modes,
        args.candidate_radii_km,
        weight_pairs,
    ):
        selection_set.add(
            SelectionConfig(
                search_mode=mode,
                candidate_radius_km=float(radius),
                area_weight=float(area_weight),
                distance_weight=float(distance_weight),
            )
        )

    # Always include the exact current-search baseline unless explicitly disabled.
    if not args.no_legacy_baseline:
        selection_set.add(
            SelectionConfig(
                search_mode="legacy_degree",
                candidate_radius_km=120.0,
                area_weight=1.0,
                distance_weight=1.0,
            )
        )

    selections = sorted(
        selection_set,
        key=lambda item: (
            item.search_mode,
            item.candidate_radius_km,
            item.area_weight,
            item.distance_weight,
        ),
    )

    matches = [
        MatchConfig(
            selection=selection,
            distance_limit_m=float(distance_limit),
            da_scale=float(da_scale),
            close_distance_m=float(args.close_distance_m),
            base_area_match_cutoff=float(args.area_match_cutoff),
            base_area_approx_cutoff=float(args.area_approx_cutoff),
        )
        for selection, distance_limit, da_scale in itertools.product(
            selections,
            args.distance_limits_m,
            args.da_scales,
        )
    ]

    # For the automatically added legacy selection, retain only the current
    # baseline unless the user explicitly requested legacy_degree in the grid.
    if (
        "legacy_degree" not in args.search_modes
        and not args.no_legacy_baseline
    ):
        matches = [
            item
            for item in matches
            if item.selection.search_mode != "legacy_degree"
            or (
                math.isclose(item.selection.candidate_radius_km, 120.0)
                and math.isclose(item.selection.area_weight, 1.0)
                and math.isclose(item.selection.distance_weight, 1.0)
                and math.isclose(item.distance_limit_m, 1000.0)
                and math.isclose(item.da_scale, 1.0)
            )
        ]

    merge_configs = [
        MergeConfig(float(distance), float(area_error))
        for distance, area_error in itertools.product(
            args.merge_distances_m,
            args.merge_area_errors,
        )
    ]
    return selections, matches, merge_configs


def _serialize_match_configs(matches: Sequence[MatchConfig]) -> List[dict]:
    rows = []
    for item in matches:
        rows.append(
            {
                "selection_key": item.selection.key,
                "distance_limit_m": item.distance_limit_m,
                "da_scale": item.da_scale,
                "close_distance_m": item.close_distance_m,
                "base_area_match_cutoff": item.base_area_match_cutoff,
                "base_area_approx_cutoff": item.base_area_approx_cutoff,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Summary, re-clustering, and baseline comparison
# ---------------------------------------------------------------------------

def _reference_match_key(matches: Sequence[MatchConfig]) -> Optional[str]:
    for item in matches:
        if (
            item.selection.search_mode == "legacy_degree"
            and math.isclose(item.selection.candidate_radius_km, 120.0)
            and math.isclose(item.selection.area_weight, 1.0)
            and math.isclose(item.selection.distance_weight, 1.0)
            and math.isclose(item.distance_limit_m, 1000.0)
            and math.isclose(item.da_scale, 1.0)
        ):
            return item.key
    return None


def summarize_with_reclustering(
    details: pd.DataFrame,
    stations: pd.DataFrame,
    matches: Sequence[MatchConfig],
    merge_configs: Sequence[MergeConfig],
    out_dir: Path,
    published_cluster_ids: Optional[set] = None,
) -> pd.DataFrame:
    match_lookup = {item.key: item for item in matches}
    reference_match_key = _reference_match_key(matches)
    reference_comids = None
    if reference_match_key is not None:
        reference_comids = (
            details[details["match_scenario_id"] == reference_match_key]
            .set_index("station_key")["basin_id"]
        )

    release_cluster_map = None
    if "release_cluster_id" in stations.columns:
        release_cluster_map = stations.set_index("station_key")["release_cluster_id"]

    summary_rows = []
    temporary_parent = out_dir / ".tmp_cluster_inputs"
    temporary_parent.mkdir(parents=True, exist_ok=True)
    try:
        for match_id, scenario_df in details.groupby("match_scenario_id", sort=False):
            match = match_lookup[match_id]
            scenario_df = scenario_df.copy()
            n_station = int(len(scenario_df))
            n_resolved_station = int(
                scenario_df["basin_status"].astype(str).eq("resolved").sum()
            )
            station_pct = 100.0 * n_resolved_station / n_station if n_station else np.nan

            comid_change_pct = np.nan
            if reference_comids is not None:
                current = scenario_df.set_index("station_key")["basin_id"]
                joined = pd.concat(
                    [reference_comids.rename("reference"), current.rename("current")],
                    axis=1,
                    join="inner",
                )
                ref_num = pd.to_numeric(joined["reference"], errors="coerce")
                cur_num = pd.to_numeric(joined["current"], errors="coerce")
                same = (ref_num.eq(cur_num)) | (ref_num.isna() & cur_num.isna())
                comid_change_pct = 100.0 * float((~same).mean()) if len(same) else np.nan

            for merge in merge_configs:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    suffix=".csv",
                    prefix="scenario_",
                    dir=temporary_parent,
                    delete=False,
                    encoding="utf-8",
                ) as handle:
                    temp_path = Path(handle.name)
                    cluster_input = scenario_df[
                        [
                            "station_key",
                            "station_id",
                            "lat",
                            "lon",
                            "observation_type",
                            "basin_id",
                            "uparea_merit",
                            "basin_status",
                        ]
                    ].copy()
                    cluster_input.to_csv(handle, index=False)
                try:
                    mapping, cluster_stats = load_station_to_basin_cluster_map(
                        temp_path,
                        station_df=stations,
                        max_station_distance_m=merge.max_station_distance_m,
                        max_upstream_rel_error=merge.max_upstream_rel_error,
                        upstream_area_col="uparea_merit",
                    )
                finally:
                    temp_path.unlink(missing_ok=True)

                scenario_df["sensitivity_cluster_id"] = scenario_df["station_id"].map(mapping)
                if release_cluster_map is not None:
                    scenario_df["release_cluster_id"] = scenario_df["station_key"].map(
                        release_cluster_map
                    )
                    if published_cluster_ids is not None:
                        scenario_df["_publishable"] = scenario_df[
                            "release_cluster_id"
                        ].isin(published_cluster_ids)
                    else:
                        scenario_df["_publishable"] = True
                else:
                    scenario_df["_publishable"] = True
                missing_mapping = scenario_df["sensitivity_cluster_id"].isna()
                if missing_mapping.any():
                    scenario_df.loc[missing_mapping, "sensitivity_cluster_id"] = scenario_df.loc[
                        missing_mapping, "station_id"
                    ]
                cluster_status = scenario_df.groupby("sensitivity_cluster_id")[
                    "basin_status"
                ].apply(lambda values: bool(values.astype(str).eq("resolved").all()))
                if not bool(scenario_df["_publishable"].all()):
                    publishable_clusters = set(
                        scenario_df.loc[
                            scenario_df["_publishable"], "sensitivity_cluster_id"
                        ]
                    )
                    cluster_status = cluster_status[
                        cluster_status.index.isin(publishable_clusters)
                    ]
                n_cluster = int(len(cluster_status))
                n_resolved_cluster = int(cluster_status.sum())
                cluster_pct = (
                    100.0 * n_resolved_cluster / n_cluster if n_cluster else np.nan
                )

                summary_rows.append(
                    {
                        "scenario_id": f"{match_id}__{merge.key}",
                        "match_scenario_id": match_id,
                        "search_mode": match.selection.search_mode,
                        "candidate_radius_km": match.selection.candidate_radius_km,
                        "area_weight": match.selection.area_weight,
                        "distance_weight": match.selection.distance_weight,
                        "close_distance_m": match.close_distance_m,
                        "distance_limit_m": match.distance_limit_m,
                        "da_scale": match.da_scale,
                        "area_match_cutoff": match.area_match_cutoff,
                        "area_approx_cutoff": match.area_approx_cutoff,
                        "merge_distance_m": merge.max_station_distance_m,
                        "merge_area_relative_error": merge.max_upstream_rel_error,
                        "n_source_stations": n_station,
                        "n_resolved_source_stations": n_resolved_station,
                        "resolved_source_station_pct": station_pct,
                        "n_clusters": n_cluster,
                        "n_resolved_clusters": n_resolved_cluster,
                        "resolved_cluster_pct": cluster_pct,
                        "selected_comid_change_pct_vs_legacy_baseline": comid_change_pct,
                        "n_station_ids_changed_by_clustering": int(
                            cluster_stats.get("n_changed", 0)
                        ),
                    }
                )
    finally:
        try:
            temporary_parent.rmdir()
        except OSError:
            pass

    summary = pd.DataFrame(summary_rows)
    if len(summary) == 0:
        return summary

    reference_mask = (
        summary["search_mode"].eq("legacy_degree")
        & np.isclose(summary["candidate_radius_km"], 120.0)
        & np.isclose(summary["area_weight"], 1.0)
        & np.isclose(summary["distance_weight"], 1.0)
        & np.isclose(summary["distance_limit_m"], 1000.0)
        & np.isclose(summary["da_scale"], 1.0)
        & np.isclose(summary["merge_distance_m"], 1000.0)
        & np.isclose(summary["merge_area_relative_error"], 0.10)
    )
    if reference_mask.any():
        reference_row = summary.loc[reference_mask].iloc[0]
        summary["delta_resolved_source_station_pct_points_vs_baseline"] = (
            summary["resolved_source_station_pct"]
            - float(reference_row["resolved_source_station_pct"])
        )
        summary["delta_resolved_cluster_pct_points_vs_baseline"] = (
            summary["resolved_cluster_pct"]
            - float(reference_row["resolved_cluster_pct"])
        )
        summary["is_legacy_baseline"] = reference_mask
    else:
        summary["delta_resolved_source_station_pct_points_vs_baseline"] = np.nan
        summary["delta_resolved_cluster_pct_points_vs_baseline"] = np.nan
        summary["is_legacy_baseline"] = False

    return summary.sort_values(
        [
            "search_mode",
            "candidate_radius_km",
            "area_weight",
            "distance_weight",
            "distance_limit_m",
            "da_scale",
            "merge_distance_m",
            "merge_area_relative_error",
        ]
    ).reset_index(drop=True)


def compare_with_baseline_s4(
    details: pd.DataFrame,
    matches: Sequence[MatchConfig],
    s4_csv: Path,
    out_dir: Path,
) -> Dict[str, object]:
    reference_key = _reference_match_key(matches)
    if reference_key is None or not s4_csv.is_file():
        return {"available": False}

    baseline = pd.read_csv(s4_csv, low_memory=False)
    required = ["station_key", "basin_id", "basin_status"]
    missing = [column for column in required if column not in baseline.columns]
    if missing:
        LOGGER.warning("Skipping s4 comparison; missing columns: %s", missing)
        return {"available": False, "missing_columns": missing}

    current = details[details["match_scenario_id"] == reference_key][
        ["station_key", "basin_id", "basin_status", "distance_m", "match_quality"]
    ].copy()
    baseline = baseline[
        [
            column
            for column in [
                "station_key",
                "basin_id",
                "basin_status",
                "distance_m",
                "match_quality",
            ]
            if column in baseline.columns
        ]
    ].copy()
    joined = current.merge(
        baseline,
        on="station_key",
        how="inner",
        suffixes=("_sensitivity", "_s4"),
        validate="one_to_one",
    )
    if len(joined) == 0:
        return {"available": False, "reason": "no shared station_key"}

    left_id = pd.to_numeric(joined["basin_id_sensitivity"], errors="coerce")
    right_id = pd.to_numeric(joined["basin_id_s4"], errors="coerce")
    joined["basin_id_same"] = left_id.eq(right_id) | (left_id.isna() & right_id.isna())
    joined["basin_status_same"] = (
        joined["basin_status_sensitivity"].fillna("").astype(str)
        == joined["basin_status_s4"].fillna("").astype(str)
    )
    difference_mask = ~(joined["basin_id_same"] & joined["basin_status_same"])
    differences = joined.loc[difference_mask].copy()
    if len(differences):
        differences.to_csv(out_dir / "baseline_reproduction_differences.csv", index=False)

    return {
        "available": True,
        "n_shared": int(len(joined)),
        "basin_id_agreement_pct": 100.0 * float(joined["basin_id_same"].mean()),
        "basin_status_agreement_pct": 100.0 * float(joined["basin_status_same"].mean()),
        "n_differences": int(difference_mask.sum()),
    }


def write_markdown_summary(summary: pd.DataFrame, path: Path) -> None:
    columns = [
        "search_mode",
        "candidate_radius_km",
        "area_weight",
        "distance_weight",
        "distance_limit_m",
        "area_match_cutoff",
        "area_approx_cutoff",
        "merge_distance_m",
        "n_resolved_source_stations",
        "n_source_stations",
        "resolved_source_station_pct",
        "n_resolved_clusters",
        "n_clusters",
        "resolved_cluster_pct",
        "delta_resolved_cluster_pct_points_vs_baseline",
    ]
    table = summary[columns].copy()
    for column in [
        "resolved_source_station_pct",
        "resolved_cluster_pct",
        "delta_resolved_cluster_pct_points_vs_baseline",
    ]:
        table[column] = table[column].map(
            lambda value: "" if pd.isna(value) else f"{float(value):.2f}"
        )
    for column in [
        "candidate_radius_km",
        "area_weight",
        "distance_weight",
        "distance_limit_m",
        "area_match_cutoff",
        "area_approx_cutoff",
        "merge_distance_m",
    ]:
        table[column] = table[column].map(lambda value: f"{float(value):g}")

    headers = list(table.columns)
    lines = [
        "# Basin-matching threshold sensitivity",
        "",
        "Resolved percentages use the same scoped station denominator for every scenario. ",
        "The legacy baseline uses the current 1.0 degree candidate box, 120 km score normalization, ",
        "1:1 score weights, 300/1000 m release policy, D_A cutoffs 0.10/0.30, and 1 km clustering.",
        "",
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in table.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(map(str, row)) + " |")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI and main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Re-run MERIT reach matching under configurable distance and drainage-area "
            "thresholds and report resolved station/cluster proportions."
        )
    )
    parser.add_argument("--s3-csv", type=Path, default=DEFAULT_S3_CSV)
    parser.add_argument("--s4-baseline-csv", type=Path, default=DEFAULT_S4_CSV)
    parser.add_argument("--s5-csv", type=Path, default=DEFAULT_S5_CSV)
    parser.add_argument("--quality-order-csv", type=Path, default=DEFAULT_QUALITY_ORDER_CSV)
    parser.add_argument(
        "--no-publish-filter",
        action="store_true",
        help="Disable release-aligned cluster filtering (report all re-clustered clusters).",
    )
    parser.add_argument("--merit-dir", type=Path, default=DEFAULT_MERIT_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument(
        "--scope",
        choices=["release_main", "main_all", "all"],
        default="release_main",
        help=(
            "release_main: daily/monthly/annual non-satellite rows present in s5; "
            "main_all: all such s3 rows; all: no temporal/source filtering."
        ),
    )
    parser.add_argument(
        "--search-modes",
        nargs="+",
        choices=["fixed_km", "legacy_degree"],
        default=["fixed_km"],
        help="Default tests fixed-km search; one legacy baseline is added automatically.",
    )
    parser.add_argument(
        "--candidate-radii-km",
        nargs="+",
        type=float,
        default=[120.0],
        help="Candidate radius and distance-score normalization d_max in km.",
    )
    parser.add_argument(
        "--score-weight-pairs",
        nargs="+",
        default=["1:1"],
        metavar="AREA:DISTANCE",
        help="Relative weights of Eq. (2) area and distance terms.",
    )
    parser.add_argument(
        "--distance-limits-m",
        nargs="+",
        type=float,
        default=[300.0, 500.0, 1000.0],
        help=(
            "Maximum accepted station-to-reach distance. Distances above the 300 m "
            "close threshold require area or local-catchment support."
        ),
    )
    parser.add_argument("--close-distance-m", type=float, default=300.0)
    parser.add_argument("--da-scales", nargs="+", type=float, default=[0.5, 1.0, 1.5])
    parser.add_argument("--area-match-cutoff", type=float, default=0.10)
    parser.add_argument("--area-approx-cutoff", type=float, default=0.30)
    parser.add_argument(
        "--merge-distances-m",
        nargs="+",
        type=float,
        default=[1000.0],
        help="Complete-linkage station-clustering distance thresholds.",
    )
    parser.add_argument(
        "--merge-area-errors",
        nargs="+",
        type=float,
        default=[0.10],
        help="Complete-linkage upstream-area symmetric relative-error thresholds.",
    )
    parser.add_argument(
        "--no-legacy-baseline",
        action="store_true",
        help="Do not add the current 1.0 degree baseline scenario.",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--chunk-size", type=int, default=50)
    parser.add_argument(
        "--start-method",
        choices=["fork", "spawn", "forkserver"],
        default="fork" if os.name != "nt" else "spawn",
    )
    parser.add_argument("--max-stations", type=int, default=None, help="Debug subset only.")
    parser.add_argument("--no-details", action="store_true")
    parser.add_argument("--overwrite", action="store_true", default=True)
    parser.add_argument("--log-level", default="INFO")
    return parser


def validate_arguments(args) -> None:
    if not args.merit_dir.is_dir():
        raise FileNotFoundError(f"MERIT directory not found: {args.merit_dir}")
    if args.workers < 1 or args.chunk_size < 1:
        raise ValueError("workers and chunk-size must be positive")
    numeric_positive_lists = {
        "candidate-radii-km": args.candidate_radii_km,
        "distance-limits-m": args.distance_limits_m,
        "da-scales": args.da_scales,
        "merge-distances-m": args.merge_distances_m,
        "merge-area-errors": args.merge_area_errors,
    }
    for label, values in numeric_positive_lists.items():
        if any(float(value) <= 0 for value in values):
            raise ValueError(f"All {label} values must be positive")
    if args.close_distance_m <= 0:
        raise ValueError("close-distance-m must be positive")
    if not (0 < args.area_match_cutoff < args.area_approx_cutoff):
        raise ValueError("Require 0 < area-match-cutoff < area-approx-cutoff")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    validate_arguments(args)

    out_dir = args.out_dir.expanduser().resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        if not args.overwrite:
            raise FileExistsError(
                f"Output directory is not empty: {out_dir}. Use --overwrite or choose another directory."
            )
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    stations = load_station_scope(
        args.s3_csv.expanduser().resolve(),
        args.s5_csv.expanduser().resolve() if args.s5_csv else None,
        args.scope,
        args.max_stations,
    )
    selections, matches, merge_configs = build_scenarios(args)
    max_candidate_radius_m = max(item.candidate_radius_km for item in selections) * 1000.0

    LOGGER.info(
        "Scope contains %d stations; %d selection configs, %d matching configs, %d merge configs",
        len(stations),
        len(selections),
        len(matches),
        len(merge_configs),
    )

    station_records = stations.to_dict("records")
    station_records = _assign_primary_regions(
        station_records,
        str(args.merit_dir.expanduser().resolve()),
    )
    station_records.sort(
        key=lambda item: (
            str(item["primary_region"]),
            item["lon"],
            item["lat"],
            item["station_id"],
        )
    )
    # Group by primary region first so a worker's river-file cache stays
    # within one region; each chunk carries its region key for clear_cache().
    chunks: List[Tuple[str, List[dict]]] = []
    for region_key, group in itertools.groupby(
        station_records, key=lambda item: item["primary_region"]
    ):
        for chunk in _chunked(list(group), int(args.chunk_size)):
            chunks.append((region_key, chunk))
    selection_dicts = [asdict(item) for item in selections]
    match_dicts = _serialize_match_configs(matches)

    all_rows: List[dict] = []
    context = mp.get_context(args.start_method)
    if args.workers == 1:
        _init_worker(
            str(args.merit_dir.expanduser().resolve()),
            max_candidate_radius_m,
            selection_dicts,
            match_dicts,
        )
        for index, chunk in enumerate(chunks, start=1):
            all_rows.extend(_process_station_chunk(chunk))
            if index % 10 == 0 or index == len(chunks):
                LOGGER.info("Processed %d/%d chunks", index, len(chunks))
    else:
        with context.Pool(
            processes=int(args.workers),
            initializer=_init_worker,
            initargs=(
                str(args.merit_dir.expanduser().resolve()),
                max_candidate_radius_m,
                selection_dicts,
                match_dicts,
            ),
        ) as pool:
            for index, rows in enumerate(
                pool.imap_unordered(_process_station_chunk, chunks, chunksize=1),
                start=1,
            ):
                all_rows.extend(rows)
                if index % 10 == 0 or index == len(chunks):
                    LOGGER.info("Processed %d/%d chunks", index, len(chunks))

    details = pd.DataFrame(all_rows)
    expected_rows = len(stations) * len(matches)
    if len(details) != expected_rows:
        raise RuntimeError(
            f"Unexpected detail row count: got {len(details)}, expected {expected_rows}"
        )

    details = details.sort_values(
        ["match_scenario_id", "station_id"]
    ).reset_index(drop=True)
    if not args.no_details:
        details.to_csv(
            out_dir / "basin_matching_sensitivity_station_details.csv.gz",
            index=False,
            compression="gzip",
        )

    published_cluster_ids = None
    if not args.no_publish_filter:
        published_cluster_ids = load_published_cluster_ids(
            args.quality_order_csv.expanduser().resolve()
        )
        if published_cluster_ids is None:
            LOGGER.warning(
                "quality-order CSV not found; falling back to reporting all clusters"
            )

    summary = summarize_with_reclustering(
        details,
        stations,
        matches,
        merge_configs,
        out_dir,
        published_cluster_ids=published_cluster_ids,
    )
    summary.to_csv(out_dir / "basin_matching_sensitivity_summary.csv", index=False)
    write_markdown_summary(
        summary,
        out_dir / "basin_matching_sensitivity_summary.md",
    )

    reproduction = compare_with_baseline_s4(
        details,
        matches,
        args.s4_baseline_csv.expanduser().resolve(),
        out_dir,
    )
    metadata = {
        "script": SCRIPT_PATH.name,
        "repository_root": str(REPO_ROOT),
        "output_r_root": str(OUTPUT_R_ROOT),
        "inputs": {
            "s3_csv": str(args.s3_csv.expanduser().resolve()),
            "s4_baseline_csv": str(args.s4_baseline_csv.expanduser().resolve()),
            "s5_csv": str(args.s5_csv.expanduser().resolve()),
            "merit_dir": str(args.merit_dir.expanduser().resolve()),
        },
        "scope": args.scope,
        "n_scoped_stations": int(len(stations)),
        "selection_configs": [asdict(item) for item in selections],
        "match_configs": [
            {
                "key": item.key,
                "selection_key": item.selection.key,
                "distance_limit_m": item.distance_limit_m,
                "close_distance_m": item.close_distance_m,
                "da_scale": item.da_scale,
                "area_match_cutoff": item.area_match_cutoff,
                "area_approx_cutoff": item.area_approx_cutoff,
            }
            for item in matches
        ],
        "merge_configs": [asdict(item) for item in merge_configs],
        "baseline_reproduction": reproduction,
        "fixed_km_search_note": (
            "The fixed-km mode converts the requested metric radius to a latitude-dependent "
            "longitude span, splits dateline-crossing windows, intersects neighbouring Pfaf "
            "region bounds, and finally enforces the exact point-to-line metric radius."
        ),
    }
    (out_dir / "basin_matching_sensitivity_metadata.json").write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    if len(summary):
        display_columns = [
            "search_mode",
            "candidate_radius_km",
            "distance_limit_m",
            "area_match_cutoff",
            "area_approx_cutoff",
            "resolved_source_station_pct",
            "resolved_cluster_pct",
            "delta_resolved_cluster_pct_points_vs_baseline",
        ]
        print(summary[display_columns].to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    LOGGER.info("Wrote sensitivity outputs to %s", out_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
