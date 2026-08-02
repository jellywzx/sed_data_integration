#!/usr/bin/env python3
"""
Read-only diagnostic for the overlap between the 1-degree candidate-search box
and the 120 km candidate-retention rule used by basin_tracer.py.

The script mirrors the current generic reach-candidate stage:

1. Determine the MERIT-Basins pfaf_level_01 region(s) whose bounds contain the
   station point.
2. Query river reaches whose geometry bounds intersect a +/-1 degree box.
3. Calculate station-to-reach point-to-line distance in the local metric CRS.
4. Count how many station-candidate pairs are removed because distance is
   greater than or equal to 120 km.

It also checks whether the 120 km filter changes:
- the nearest-reach result for distance-only matching;
- the minimum area-distance score for stations with a valid reported area.

No pipeline products are modified.

Recommended repository path:
    validate/diagnose_basin_candidate_retention.py
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


SCRIPT_PATH = Path(__file__).resolve()
SCRIPT_DIR = SCRIPT_PATH.parent
REPO_ROOT = SCRIPT_DIR.parent if SCRIPT_DIR.name == "validate" else SCRIPT_DIR
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from basin_tracer import (  # noqa: E402
    SEARCH_RADIUS_DEG,
    SEARCH_RADIUS_M,
    UpstreamBasinTracer,
)

try:
    from pipeline_paths import S3_COLLECTED_CSV, get_output_r_root  # noqa: E402
except ImportError:
    S3_COLLECTED_CSV = Path("scripts_basin_test/output/s3_collected_stations.csv")

    def get_output_r_root(script_dir: Path) -> Path:
        env = os.environ.get("OUTPUT_R_ROOT")
        return Path(env).expanduser() if env else script_dir


LOGGER = logging.getLogger("candidate_retention")
SATELLITE_SOURCE_TOKENS = frozenset({"gsed", "riversed", "dethier"})
SATELLITE_OBSERVATION_TOKENS = frozenset(
    {
        "satellite",
        "remote_sensing",
        "remote_sensing_observation",
        "satellite_observation",
    }
)
_WORKER_TRACER: Optional[UpstreamBasinTracer] = None


def parse_args() -> argparse.Namespace:
    output_r_root = get_output_r_root(REPO_ROOT)
    default_input = output_r_root / S3_COLLECTED_CSV
    default_merit = Path(
        os.environ.get(
            "MERIT_DIR",
            str(output_r_root.parent.parent / "MERIT_Hydro_v07_Basins_v01_bugfix1"),
        )
    )
    default_output = REPO_ROOT / "validate" / "output" / "basin_candidate_retention"

    parser = argparse.ArgumentParser(
        description=(
            "Quantify how many reaches returned by the 1-degree bbox are "
            "discarded by the 120 km candidate-retention rule."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=default_input,
        help=f"s3 station CSV (default: {default_input})",
    )
    parser.add_argument(
        "--merit-dir",
        type=Path,
        default=default_merit,
        help=f"MERIT-Basins root directory (default: {default_merit})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output,
        help=f"Directory for diagnostic outputs (default: {default_output})",
    )
    parser.add_argument(
        "--search-deg",
        type=float,
        default=float(SEARCH_RADIUS_DEG),
        help=f"Half-width of bbox in degrees (default from basin_tracer: {SEARCH_RADIUS_DEG})",
    )
    parser.add_argument(
        "--retention-km",
        type=float,
        default=float(SEARCH_RADIUS_M) / 1000.0,
        help=(
            "Distance retention threshold in km. The current implementation "
            f"keeps distance < threshold (default: {SEARCH_RADIUS_M / 1000.0:g})."
        ),
    )
    parser.add_argument(
        "--scope",
        choices=("gauge", "all"),
        default="gauge",
        help=(
            "gauge: exclude satellite/reach-scale rows; "
            "all: analyze every s3 row using its stored station coordinates."
        ),
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Analyze only this source. Repeat the option for multiple sources.",
    )
    parser.add_argument(
        "--exclude-source",
        action="append",
        default=[],
        help="Exclude this source. Repeat the option for multiple sources.",
    )
    parser.add_argument(
        "--max-stations",
        type=int,
        default=None,
        help="Optional maximum number of stations after filtering.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=1.0,
        help="Optional reproducible station sample fraction in (0, 1].",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used with --sample-fraction.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress every N stations.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, (os.cpu_count() or 2) - 1),
        help=(
            "Number of worker processes for station-level analysis "
            "(default: CPU count minus one). Use 1 for serial execution."
        ),
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1,
        help="Station task chunksize used by the process pool (default: 1).",
    )
    parser.add_argument(
        "--example-limit",
        type=int,
        default=500,
        help="Maximum removed-candidate examples written to CSV.",
    )
    parser.add_argument(
        "--minor-fraction-pct",
        type=float,
        default=1.0,
        help="Heuristic percentage used when wording the generated conclusion.",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
    )
    return parser.parse_args()


def normalize_token(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return "" if text == "nan" else text


def normalize_observation_type(value: Any) -> str:
    return normalize_token(value).replace("-", "_").replace(" ", "_")


def finite_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number if math.isfinite(number) else math.nan


def load_stations(
    path: Path,
    scope: str,
    include_sources: Sequence[str],
    exclude_sources: Sequence[str],
    sample_fraction: float,
    seed: int,
    max_stations: Optional[int],
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if not path.is_file():
        raise FileNotFoundError(f"Input station CSV not found: {path}")

    stations = pd.read_csv(path)
    required = {"lat", "lon"}
    missing = required.difference(stations.columns)
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {sorted(missing)}")

    if "station_key" not in stations.columns:
        stations["station_key"] = [f"row_{idx}" for idx in stations.index]
    if "station_id" not in stations.columns:
        stations["station_id"] = stations.index
    if "source" not in stations.columns:
        stations["source"] = "unknown"
    if "reported_area" not in stations.columns:
        stations["reported_area"] = np.nan

    original_rows = len(stations)
    stations["lat"] = pd.to_numeric(stations["lat"], errors="coerce")
    stations["lon"] = pd.to_numeric(stations["lon"], errors="coerce")
    valid_coord = (
        stations["lat"].between(-90.0, 90.0, inclusive="both")
        & stations["lon"].between(-180.0, 180.0, inclusive="both")
    )
    invalid_coord_rows = int((~valid_coord).sum())
    stations = stations.loc[valid_coord].copy()

    stations["_source_token"] = stations["source"].map(normalize_token)
    scope_excluded_rows = 0
    if scope == "gauge":
        if "observation_type" in stations.columns:
            obs = stations["observation_type"].map(normalize_observation_type)
            satellite_mask = obs.isin(SATELLITE_OBSERVATION_TOKENS)
            # Empty or inconsistent observation_type fields are backed up by source labels.
            satellite_mask |= stations["_source_token"].isin(SATELLITE_SOURCE_TOKENS)
        else:
            satellite_mask = stations["_source_token"].isin(SATELLITE_SOURCE_TOKENS)
        scope_excluded_rows = int(satellite_mask.sum())
        stations = stations.loc[~satellite_mask].copy()

    include_tokens = {normalize_token(value) for value in include_sources if normalize_token(value)}
    if include_tokens:
        stations = stations.loc[stations["_source_token"].isin(include_tokens)].copy()

    exclude_tokens = {normalize_token(value) for value in exclude_sources if normalize_token(value)}
    if exclude_tokens:
        stations = stations.loc[~stations["_source_token"].isin(exclude_tokens)].copy()

    if not (0.0 < sample_fraction <= 1.0):
        raise ValueError("--sample-fraction must be in (0, 1].")
    if sample_fraction < 1.0 and len(stations) > 0:
        stations = stations.sample(frac=sample_fraction, random_state=seed)

    # Sorting by longitude improves cache reuse because nearby stations often
    # reside in the same MERIT-Basins pfaf region.
    stations = stations.sort_values(["lon", "lat", "station_key"], kind="stable")
    if max_stations is not None:
        if max_stations <= 0:
            raise ValueError("--max-stations must be positive.")
        stations = stations.head(max_stations)

    stations = stations.reset_index(drop=True)
    audit = {
        "input_rows": int(original_rows),
        "invalid_coordinate_rows": int(invalid_coord_rows),
        "scope_excluded_rows": int(scope_excluded_rows),
        "analyzed_rows": int(len(stations)),
    }
    return stations, audit


def collect_bbox_candidates(
    tracer: UpstreamBasinTracer,
    lon: float,
    lat: float,
    search_deg: float,
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Reproduce the pre-retention part of basin_tracer._gather_nearby_candidate_reaches.

    Returns
    -------
    candidates, status
        candidates contains all rows from the +/- search_deg bbox before the
        distance-retention filter. status explains no-candidate cases.
    """
    pfaf_codes = tracer._get_pfaf_level1_codes(lon, lat)
    if not pfaf_codes:
        return None, "no_pfaf_region"

    parts = []
    for pfaf_code in pfaf_codes:
        rivers = tracer._load_level1_rivers(pfaf_code)
        if rivers is None or len(rivers) == 0:
            continue

        search_box = (
            lon - search_deg,
            lat - search_deg,
            lon + search_deg,
            lat + search_deg,
        )
        possible_idx = list(rivers.sindex.intersection(search_box))
        if not possible_idx:
            continue

        candidates = rivers.iloc[possible_idx].copy()
        candidates["pfaf_code"] = str(pfaf_code)
        candidates["dist_m"] = tracer._distance_point_to_geoms_m(
            candidates.geometry,
            lon,
            lat,
        )
        parts.append(candidates)

    if not parts:
        return None, "no_bbox_candidate"

    merged = pd.concat(parts, ignore_index=True)
    if len(merged) == 0:
        return None, "no_bbox_candidate"
    return merged, "ok"


def candidate_key_frame(candidates: pd.DataFrame) -> pd.DataFrame:
    """
    Produce one row per COMID for reach-count statistics.

    The production code keeps candidate rows as returned by each pfaf region.
    The unique frame is used only to answer the user-facing question in terms of
    distinct reaches and to detect any duplicate COMIDs.
    """
    frame = candidates.copy()
    if "COMID" not in frame.columns:
        frame["COMID"] = np.arange(len(frame), dtype=np.int64)
    frame["_comid_text"] = frame["COMID"].astype(str)
    frame = frame.sort_values("dist_m", na_position="last", kind="stable")
    return frame.drop_duplicates("_comid_text", keep="first")


def best_distance_only(candidates: pd.DataFrame) -> Tuple[Optional[str], float]:
    finite = candidates.loc[np.isfinite(candidates["dist_m"])].copy()
    if finite.empty:
        return None, math.nan
    row = finite.loc[finite["dist_m"].idxmin()]
    return str(row.get("COMID")), float(row["dist_m"])


def best_area_distance(
    candidates: pd.DataFrame,
    reported_area: float,
    retention_m: float,
) -> Tuple[Optional[str], float, float]:
    if not math.isfinite(reported_area) or reported_area <= 0:
        return None, math.nan, math.nan
    if "uparea" not in candidates.columns:
        return None, math.nan, math.nan

    work = candidates.copy()
    work["uparea"] = pd.to_numeric(work["uparea"], errors="coerce")
    valid = (
        np.isfinite(work["dist_m"])
        & np.isfinite(work["uparea"])
        & (work["uparea"] > 0)
    )
    work = work.loc[valid].copy()
    if work.empty:
        return None, math.nan, math.nan

    ratio = (work["uparea"] / reported_area).clip(0.001, 1000.0)
    work["_area_error"] = np.abs(np.log10(ratio))
    work["_score"] = work["_area_error"] + work["dist_m"] / retention_m
    row = work.loc[work["_score"].idxmin()]
    return str(row.get("COMID")), float(row["dist_m"]), float(row["_score"])


def safe_fraction(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else math.nan


def summarize_one_station(
    tracer: UpstreamBasinTracer,
    station: Mapping[str, Any],
    search_deg: float,
    retention_m: float,
    example_limit_remaining: int,
) -> Tuple[Dict[str, Any], pd.DataFrame]:
    lon = float(station["lon"])
    lat = float(station["lat"])
    reported_area = finite_float(station.get("reported_area"))

    candidates, status = collect_bbox_candidates(tracer, lon, lat, search_deg)
    base: Dict[str, Any] = {
        "station_key": str(station.get("station_key", "")),
        "station_id": station.get("station_id"),
        "source": station.get("source", ""),
        "resolution": station.get("resolution", ""),
        "lon": lon,
        "lat": lat,
        "abs_lat": abs(lat),
        "reported_area": reported_area,
        "candidate_status": status,
        "n_pfaf_regions": len(tracer._get_pfaf_level1_codes(lon, lat)),
        "n_bbox_candidate_rows": 0,
        "n_bbox_unique_reaches": 0,
        "n_duplicate_candidate_rows": 0,
        "n_finite_distance_rows": 0,
        "n_retained_rows": 0,
        "n_retained_unique_reaches": 0,
        "n_removed_ge_retention_rows": 0,
        "n_removed_ge_retention_unique_reaches": 0,
        "n_removed_nonfinite_rows": 0,
        "removed_fraction_rows": math.nan,
        "removed_fraction_unique_reaches": math.nan,
        "any_removed_ge_retention": False,
        "all_bbox_candidates_removed": False,
        "min_bbox_distance_m": math.nan,
        "median_bbox_distance_m": math.nan,
        "max_bbox_distance_m": math.nan,
        "min_retained_distance_m": math.nan,
        "max_retained_distance_m": math.nan,
        "distance_only_winner_before": None,
        "distance_only_winner_after": None,
        "distance_only_winner_before_distance_m": math.nan,
        "distance_only_winner_after_distance_m": math.nan,
        "distance_only_winner_changed": False,
        "area_score_comparable": False,
        "area_score_winner_before": None,
        "area_score_winner_after": None,
        "area_score_winner_before_distance_m": math.nan,
        "area_score_winner_after_distance_m": math.nan,
        "area_score_winner_before_score": math.nan,
        "area_score_winner_after_score": math.nan,
        "area_score_winner_changed": False,
        "n_distance_le_1km": 0,
        "n_distance_1_10km": 0,
        "n_distance_10_50km": 0,
        "n_distance_50_100km": 0,
        "n_distance_100_retention_km": 0,
        "n_distance_ge_retention_km": 0,
    }

    if candidates is None or candidates.empty:
        return base, pd.DataFrame()

    candidates = candidates.copy()
    candidates["dist_m"] = pd.to_numeric(candidates["dist_m"], errors="coerce")
    unique = candidate_key_frame(candidates)

    finite_mask = np.isfinite(candidates["dist_m"])
    retained_mask = finite_mask & (candidates["dist_m"] < retention_m)
    removed_distance_mask = finite_mask & (candidates["dist_m"] >= retention_m)
    removed_nonfinite_mask = ~finite_mask

    unique_finite = np.isfinite(unique["dist_m"])
    unique_retained = unique_finite & (unique["dist_m"] < retention_m)
    unique_removed_distance = unique_finite & (unique["dist_m"] >= retention_m)

    finite_dist = candidates.loc[finite_mask, "dist_m"]
    retained_dist = candidates.loc[retained_mask, "dist_m"]

    base.update(
        {
            "n_bbox_candidate_rows": int(len(candidates)),
            "n_bbox_unique_reaches": int(len(unique)),
            "n_duplicate_candidate_rows": int(len(candidates) - len(unique)),
            "n_finite_distance_rows": int(finite_mask.sum()),
            "n_retained_rows": int(retained_mask.sum()),
            "n_retained_unique_reaches": int(unique_retained.sum()),
            "n_removed_ge_retention_rows": int(removed_distance_mask.sum()),
            "n_removed_ge_retention_unique_reaches": int(unique_removed_distance.sum()),
            "n_removed_nonfinite_rows": int(removed_nonfinite_mask.sum()),
            "removed_fraction_rows": safe_fraction(
                int(removed_distance_mask.sum()),
                int(finite_mask.sum()),
            ),
            "removed_fraction_unique_reaches": safe_fraction(
                int(unique_removed_distance.sum()),
                int(unique_finite.sum()),
            ),
            "any_removed_ge_retention": bool(removed_distance_mask.any()),
            "all_bbox_candidates_removed": bool(
                finite_mask.any() and not retained_mask.any()
            ),
            "min_bbox_distance_m": (
                float(finite_dist.min()) if not finite_dist.empty else math.nan
            ),
            "median_bbox_distance_m": (
                float(finite_dist.median()) if not finite_dist.empty else math.nan
            ),
            "max_bbox_distance_m": (
                float(finite_dist.max()) if not finite_dist.empty else math.nan
            ),
            "min_retained_distance_m": (
                float(retained_dist.min()) if not retained_dist.empty else math.nan
            ),
            "max_retained_distance_m": (
                float(retained_dist.max()) if not retained_dist.empty else math.nan
            ),
            "n_distance_le_1km": int((finite_dist <= 1_000.0).sum()),
            "n_distance_1_10km": int(
                ((finite_dist > 1_000.0) & (finite_dist <= 10_000.0)).sum()
            ),
            "n_distance_10_50km": int(
                ((finite_dist > 10_000.0) & (finite_dist <= 50_000.0)).sum()
            ),
            "n_distance_50_100km": int(
                ((finite_dist > 50_000.0) & (finite_dist <= 100_000.0)).sum()
            ),
            "n_distance_100_retention_km": int(
                ((finite_dist > 100_000.0) & (finite_dist < retention_m)).sum()
            ),
            "n_distance_ge_retention_km": int(
                (finite_dist >= retention_m).sum()
            ),
        }
    )

    before_id, before_dist = best_distance_only(candidates)
    retained = candidates.loc[retained_mask].copy()
    after_id, after_dist = best_distance_only(retained)
    base.update(
        {
            "distance_only_winner_before": before_id,
            "distance_only_winner_after": after_id,
            "distance_only_winner_before_distance_m": before_dist,
            "distance_only_winner_after_distance_m": after_dist,
            "distance_only_winner_changed": before_id != after_id,
        }
    )

    if math.isfinite(reported_area) and reported_area > 0:
        area_before_id, area_before_dist, area_before_score = best_area_distance(
            candidates,
            reported_area,
            retention_m,
        )
        area_after_id, area_after_dist, area_after_score = best_area_distance(
            retained,
            reported_area,
            retention_m,
        )
        comparable = area_before_id is not None
        base.update(
            {
                "area_score_comparable": comparable,
                "area_score_winner_before": area_before_id,
                "area_score_winner_after": area_after_id,
                "area_score_winner_before_distance_m": area_before_dist,
                "area_score_winner_after_distance_m": area_after_dist,
                "area_score_winner_before_score": area_before_score,
                "area_score_winner_after_score": area_after_score,
                "area_score_winner_changed": comparable
                and area_before_id != area_after_id,
            }
        )

    examples = pd.DataFrame()
    if example_limit_remaining > 0 and removed_distance_mask.any():
        example_columns = [
            col
            for col in ("COMID", "uparea", "pfaf_code", "dist_m")
            if col in candidates.columns
        ]
        examples = (
            candidates.loc[removed_distance_mask, example_columns]
            .sort_values("dist_m", ascending=False)
            .head(example_limit_remaining)
            .copy()
        )
        examples.insert(0, "station_key", base["station_key"])
        examples.insert(1, "station_id", base["station_id"])
        examples.insert(2, "source", base["source"])
        examples.insert(3, "station_lon", lon)
        examples.insert(4, "station_lat", lat)
        examples["retention_m"] = retention_m

    return base, examples


def init_worker(merit_dir: str) -> None:
    global _WORKER_TRACER
    _WORKER_TRACER = UpstreamBasinTracer(merit_dir)


def summarize_one_station_worker(
    task: Tuple[int, Dict[str, Any], float, float, int],
) -> Tuple[int, Dict[str, Any], pd.DataFrame]:
    index, station, search_deg, retention_m, example_limit = task
    if _WORKER_TRACER is None:
        raise RuntimeError("Worker tracer was not initialized.")
    row, examples = summarize_one_station(
        tracer=_WORKER_TRACER,
        station=station,
        search_deg=search_deg,
        retention_m=retention_m,
        example_limit_remaining=example_limit,
    )
    return index, row, examples


def grouped_summary(
    station_df: pd.DataFrame,
    group_columns: Iterable[str],
) -> pd.DataFrame:
    group_columns = list(group_columns)
    if station_df.empty:
        return pd.DataFrame()

    grouped = station_df.groupby(group_columns, dropna=False, observed=False)
    result = grouped.agg(
        station_count=("station_key", "size"),
        stations_with_bbox_candidates=(
            "n_bbox_candidate_rows",
            lambda x: int((x > 0).sum()),
        ),
        bbox_candidate_pairs=("n_bbox_candidate_rows", "sum"),
        bbox_unique_reach_pairs=("n_bbox_unique_reaches", "sum"),
        retained_candidate_pairs=("n_retained_rows", "sum"),
        retained_unique_reach_pairs=("n_retained_unique_reaches", "sum"),
        removed_ge_retention_pairs=("n_removed_ge_retention_rows", "sum"),
        removed_ge_retention_unique_reach_pairs=(
            "n_removed_ge_retention_unique_reaches",
            "sum",
        ),
        stations_any_removed=("any_removed_ge_retention", "sum"),
        stations_all_removed=("all_bbox_candidates_removed", "sum"),
        distance_only_winner_changed=("distance_only_winner_changed", "sum"),
        area_score_comparable=("area_score_comparable", "sum"),
        area_score_winner_changed=("area_score_winner_changed", "sum"),
    ).reset_index()

    result["removed_fraction_pct"] = np.where(
        result["bbox_candidate_pairs"] > 0,
        100.0
        * result["removed_ge_retention_pairs"]
        / result["bbox_candidate_pairs"],
        np.nan,
    )
    result["removed_unique_fraction_pct"] = np.where(
        result["bbox_unique_reach_pairs"] > 0,
        100.0
        * result["removed_ge_retention_unique_reach_pairs"]
        / result["bbox_unique_reach_pairs"],
        np.nan,
    )
    return result


def build_overall_summary(
    station_df: pd.DataFrame,
    input_audit: Dict[str, int],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    bbox_pairs = int(station_df["n_bbox_candidate_rows"].sum())
    unique_pairs = int(station_df["n_bbox_unique_reaches"].sum())
    removed_pairs = int(station_df["n_removed_ge_retention_rows"].sum())
    removed_unique_pairs = int(
        station_df["n_removed_ge_retention_unique_reaches"].sum()
    )

    return {
        **input_audit,
        "scope": args.scope,
        "search_half_width_deg": float(args.search_deg),
        "retention_threshold_km": float(args.retention_km),
        "retention_operator": "distance_m < threshold",
        "stations_with_bbox_candidates": int(
            (station_df["n_bbox_candidate_rows"] > 0).sum()
        ),
        "stations_without_bbox_candidates": int(
            (station_df["n_bbox_candidate_rows"] == 0).sum()
        ),
        "bbox_candidate_station_pairs": bbox_pairs,
        "bbox_unique_reach_station_pairs": unique_pairs,
        "retained_candidate_station_pairs": int(
            station_df["n_retained_rows"].sum()
        ),
        "retained_unique_reach_station_pairs": int(
            station_df["n_retained_unique_reaches"].sum()
        ),
        "removed_ge_retention_station_pairs": removed_pairs,
        "removed_ge_retention_unique_reach_station_pairs": removed_unique_pairs,
        "removed_fraction_pct": (
            100.0 * removed_pairs / bbox_pairs if bbox_pairs else None
        ),
        "removed_unique_fraction_pct": (
            100.0 * removed_unique_pairs / unique_pairs if unique_pairs else None
        ),
        "stations_with_any_removed_candidate": int(
            station_df["any_removed_ge_retention"].sum()
        ),
        "stations_with_all_bbox_candidates_removed": int(
            station_df["all_bbox_candidates_removed"].sum()
        ),
        "distance_only_winner_changed_stations": int(
            station_df["distance_only_winner_changed"].sum()
        ),
        "area_score_comparable_stations": int(
            station_df["area_score_comparable"].sum()
        ),
        "area_score_winner_changed_stations": int(
            station_df["area_score_winner_changed"].sum()
        ),
        "nonfinite_distance_candidate_rows": int(
            station_df["n_removed_nonfinite_rows"].sum()
        ),
        "duplicate_candidate_rows_across_pfaf_results": int(
            station_df["n_duplicate_candidate_rows"].sum()
        ),
    }


def classify_effect(summary: Dict[str, Any], minor_fraction_pct: float) -> str:
    fraction = summary.get("removed_unique_fraction_pct")
    all_removed = int(summary.get("stations_with_all_bbox_candidates_removed", 0))
    distance_changed = int(summary.get("distance_only_winner_changed_stations", 0))
    area_changed = int(summary.get("area_score_winner_changed_stations", 0))

    if fraction is None:
        return "No bbox candidates were found, so the rule could not be evaluated."
    if fraction == 0 and all_removed == 0 and distance_changed == 0 and area_changed == 0:
        return (
            "The 120 km rule had no observable effect for the analyzed stations "
            "and is operationally redundant for this sample."
        )
    if (
        fraction < minor_fraction_pct
        and all_removed == 0
        and distance_changed == 0
        and area_changed == 0
    ):
        return (
            "The 120 km rule removed only a minor fraction of candidates and did "
            "not change either matching winner; it functions mainly as a defensive "
            "implementation-level cap for this sample."
        )
    if all_removed == 0 and distance_changed == 0 and area_changed == 0:
        return (
            "The rule pruned candidates but did not change a selected reach for "
            "the analyzed stations. It is not count-wise redundant, but its current "
            "decision impact is negligible."
        )
    return (
        "The rule changed candidate availability or a selected reach for at least "
        "one station, so it should not be described as redundant without reviewing "
        "the affected cases."
    )


def write_markdown_report(
    path: Path,
    summary: Dict[str, Any],
    effect_text: str,
    args: argparse.Namespace,
) -> None:
    removed_pct = summary.get("removed_unique_fraction_pct")
    removed_pct_text = "NA" if removed_pct is None else f"{removed_pct:.6f}%"
    lines = [
        "# Basin Candidate-Retention Diagnostic",
        "",
        "## Purpose",
        "",
        (
            f"Evaluate the overlap between the +/-{args.search_deg:g} degree "
            f"candidate-search box and the {args.retention_km:g} km "
            "distance-retention rule."
        ),
        "",
        "## Primary counts",
        "",
        f"- Analyzed stations: {summary['analyzed_rows']:,}",
        (
            "- Stations with at least one bbox candidate: "
            f"{summary['stations_with_bbox_candidates']:,}"
        ),
        (
            "- Distinct reach-station candidate pairs returned by the bbox: "
            f"{summary['bbox_unique_reach_station_pairs']:,}"
        ),
        (
            f"- Distinct reach-station pairs removed at distance >= "
            f"{args.retention_km:g} km: "
            f"{summary['removed_ge_retention_unique_reach_station_pairs']:,}"
        ),
        f"- Removed fraction: {removed_pct_text}",
        (
            "- Stations for which all bbox candidates were removed: "
            f"{summary['stations_with_all_bbox_candidates_removed']:,}"
        ),
        "",
        "## Decision impact",
        "",
        (
            "- Distance-only winner changed or disappeared: "
            f"{summary['distance_only_winner_changed_stations']:,} stations"
        ),
        (
            "- Stations with a comparable reported-area score: "
            f"{summary['area_score_comparable_stations']:,}"
        ),
        (
            "- Area-distance winner changed or disappeared: "
            f"{summary['area_score_winner_changed_stations']:,} stations"
        ),
        "",
        "## Interpretation",
        "",
        effect_text,
        "",
        "## Notes",
        "",
        (
            "- Candidate removal follows the current implementation exactly: "
            "`distance_m < threshold` is retained, so a candidate at exactly the "
            "threshold is removed."
        ),
        (
            "- Counts are reach-station pairs, not globally unique reaches, because "
            "the same MERIT reach can be a candidate for more than one station."
        ),
        (
            "- The script uses private helper methods from `UpstreamBasinTracer` so "
            "that its bbox query and local metric distance calculation remain aligned "
            "with the production implementation."
        ),
        (
            "- With `--scope gauge` (default), satellite/reach-scale source rows are "
            "excluded because the manuscript basin-matching method concerns gauge-based "
            "main-matrix observations."
        ),
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    retention_m = float(args.retention_km) * 1000.0
    if args.search_deg <= 0:
        raise ValueError("--search-deg must be positive.")
    if retention_m <= 0:
        raise ValueError("--retention-km must be positive.")
    if args.workers <= 0:
        raise ValueError("--workers must be positive.")
    if args.chunksize <= 0:
        raise ValueError("--chunksize must be positive.")
    if args.progress_every <= 0:
        raise ValueError("--progress-every must be positive.")
    if args.example_limit < 0:
        raise ValueError("--example-limit must be non-negative.")

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    stations, input_audit = load_stations(
        path=args.input.expanduser().resolve(),
        scope=args.scope,
        include_sources=args.source,
        exclude_sources=args.exclude_source,
        sample_fraction=args.sample_fraction,
        seed=args.seed,
        max_stations=args.max_stations,
    )
    LOGGER.info("Stations selected for analysis: %d", len(stations))
    LOGGER.info("MERIT directory: %s", args.merit_dir)

    merit_dir = str(args.merit_dir.expanduser().resolve())
    station_rows = []
    example_frames = []
    example_count = 0

    if args.workers == 1 or len(stations) <= 1:
        LOGGER.info("Running station analysis serially")
        tracer = UpstreamBasinTracer(merit_dir)
        for index, station in stations.iterrows():
            row, examples = summarize_one_station(
                tracer=tracer,
                station=station,
                search_deg=float(args.search_deg),
                retention_m=retention_m,
                example_limit_remaining=max(0, args.example_limit - example_count),
            )
            station_rows.append(row)
            if not examples.empty and example_count < args.example_limit:
                keep = examples.head(args.example_limit - example_count)
                example_frames.append(keep)
                example_count += len(keep)

            completed = index + 1
            if completed % args.progress_every == 0 or completed == len(stations):
                LOGGER.info(
                    "Processed %d/%d stations; bbox pairs=%d; removed >= threshold=%d",
                    completed,
                    len(stations),
                    sum(item["n_bbox_unique_reaches"] for item in station_rows),
                    sum(
                        item["n_removed_ge_retention_unique_reaches"]
                        for item in station_rows
                    ),
                )
    else:
        worker_count = min(int(args.workers), len(stations))
        LOGGER.info(
            "Running station analysis with %d worker processes; chunksize=%d",
            worker_count,
            args.chunksize,
        )
        tasks = (
            (
                index,
                station,
                float(args.search_deg),
                retention_m,
                args.example_limit,
            )
            for index, station in enumerate(stations.to_dict("records"))
        )
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=worker_count,
            initializer=init_worker,
            initargs=(merit_dir,),
        ) as executor:
            for completed, (_index, row, examples) in enumerate(
                executor.map(
                    summarize_one_station_worker,
                    tasks,
                    chunksize=args.chunksize,
                ),
                start=1,
            ):
                station_rows.append(row)
                if not examples.empty and example_count < args.example_limit:
                    keep = examples.head(args.example_limit - example_count)
                    example_frames.append(keep)
                    example_count += len(keep)

                if completed % args.progress_every == 0 or completed == len(stations):
                    LOGGER.info(
                        (
                            "Processed %d/%d stations; bbox pairs=%d; "
                            "removed >= threshold=%d"
                        ),
                        completed,
                        len(stations),
                        sum(item["n_bbox_unique_reaches"] for item in station_rows),
                        sum(
                            item["n_removed_ge_retention_unique_reaches"]
                            for item in station_rows
                        ),
                    )

    station_df = pd.DataFrame(station_rows)
    station_path = output_dir / "candidate_retention_station_summary.csv"
    station_df.to_csv(station_path, index=False)

    source_df = grouped_summary(station_df, ["source"])
    source_path = output_dir / "candidate_retention_source_summary.csv"
    source_df.to_csv(source_path, index=False)

    lat_bins = [-0.001, 15, 30, 45, 60, 75, 90]
    lat_labels = ["0-15", "15-30", "30-45", "45-60", "60-75", "75-90"]
    station_df_for_lat = station_df.copy()
    station_df_for_lat["abs_lat_band_deg"] = pd.cut(
        station_df_for_lat["abs_lat"],
        bins=lat_bins,
        labels=lat_labels,
        include_lowest=True,
        right=True,
    )
    lat_df = grouped_summary(station_df_for_lat, ["abs_lat_band_deg"])
    lat_path = output_dir / "candidate_retention_latitude_summary.csv"
    lat_df.to_csv(lat_path, index=False)

    examples_path = output_dir / "removed_candidate_examples.csv"
    if example_frames:
        examples_df = pd.concat(example_frames, ignore_index=True).head(
            args.example_limit
        )
        examples_df.to_csv(examples_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "station_key",
                "station_id",
                "source",
                "station_lon",
                "station_lat",
                "COMID",
                "uparea",
                "pfaf_code",
                "dist_m",
                "retention_m",
            ]
        ).to_csv(examples_path, index=False)

    summary = build_overall_summary(station_df, input_audit, args)
    effect_text = classify_effect(summary, args.minor_fraction_pct)
    summary["interpretation"] = effect_text

    json_path = output_dir / "candidate_retention_overall_summary.json"
    json_path.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False),
        encoding="utf-8",
    )

    report_path = output_dir / "candidate_retention_report.md"
    write_markdown_report(report_path, summary, effect_text, args)

    LOGGER.info("Station summary: %s", station_path)
    LOGGER.info("Source summary: %s", source_path)
    LOGGER.info("Latitude summary: %s", lat_path)
    LOGGER.info("Removed examples: %s", examples_path)
    LOGGER.info("Overall JSON: %s", json_path)
    LOGGER.info("Markdown report: %s", report_path)
    LOGGER.info("Conclusion: %s", effect_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
