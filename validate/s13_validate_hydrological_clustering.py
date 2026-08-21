#!/usr/bin/env python3
"""Rule-based structural validation for s5 hydrological clustering outputs.

The script is read-only with respect to s3/s4/s5 inputs.  It writes diagnostic
tables and a Markdown report under validate/output/hydrological_clustering by
default.
"""

import argparse
import itertools
import math
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import (  # noqa: E402
    S3_COLLECTED_CSV,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    S5_BASIN_REPORT_CSV,
    get_output_r_root,
)


DEFAULT_MAX_DISTANCE_M = 1000.0
DEFAULT_MAX_AREA_ERROR = 0.10
NEAR_DISTANCE_M = 900.0
NEAR_AREA_ERROR = 0.08
LARGE_CLUSTER_SIZE = 10
STATION_CLUSTER_OFFSET_REVIEW_M = 5000.0
DISTANCE_COORDINATE_BASIS = "s4_lat_lon_with_s5_station_fallback"
SATELLITE_TYPES = {
    "satellite",
    "remote_sensing",
    "remote_sensing_observation",
    "satellite_observation",
}
RELEASE_BASIN_FIELDS = [
    "basin_id",
    "basin_area",
    "area_error",
    "uparea_merit",
    "pfaf_code",
    "method",
    "n_upstream_reaches",
]
PAIRWISE_COLUMNS = [
    "cluster_id",
    "station_id_1",
    "station_id_2",
    "source_1",
    "source_2",
    "basin_id_1",
    "basin_id_2",
    "distance_coordinate_basis",
    "coord_source_1",
    "coord_source_2",
    "cluster_lat_1",
    "cluster_lon_1",
    "cluster_lat_2",
    "cluster_lon_2",
    "station_lat_1",
    "station_lon_1",
    "station_lat_2",
    "station_lon_2",
    "distance_m",
    "uparea_1",
    "uparea_2",
    "area_relative_error",
    "distance_pass",
    "area_pass",
]
VIOLATION_TYPES = [
    "duplicate_station_id",
    "lost_station",
    "inconsistent_basin_id",
    "unresolved_in_multistation_cluster",
    "satellite_in_multistation_cluster",
    "missing_pairwise_input",
    "distance_threshold_violation",
    "area_threshold_violation",
    "invalid_cluster_representative",
    "missed_merge",
    "inconsistent_s5_report",
]


class Config:
    def __init__(
        self,
        s3_csv,
        s4_csv,
        s5_csv,
        s5_report_csv,
        out_dir,
        max_distance_m=DEFAULT_MAX_DISTANCE_M,
        max_area_error=DEFAULT_MAX_AREA_ERROR,
    ):
        self.s3_csv = Path(s3_csv)
        self.s4_csv = Path(s4_csv) if s4_csv is not None else None
        self.s5_csv = Path(s5_csv)
        self.s5_report_csv = Path(s5_report_csv) if s5_report_csv is not None else None
        self.out_dir = Path(out_dir)
        self.max_distance_m = float(max_distance_m)
        self.max_area_error = float(max_area_error)


def normalize_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def normalize_observation_type(value) -> str:
    return normalize_text(value).lower().replace("-", "_").replace(" ", "_")


def is_satellite(value) -> bool:
    return normalize_observation_type(value) in SATELLITE_TYPES


def numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def is_blank(value) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, str):
        return value.strip() == ""
    return False


def valid_basin_id(value) -> bool:
    try:
        number = float(value)
    except Exception:
        return False
    return bool(np.isfinite(number) and number > 0)


def valid_lat_lon(lat, lon) -> bool:
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return False
    return bool(
        np.isfinite(lat_f)
        and np.isfinite(lon_f)
        and -90.0 <= lat_f <= 90.0
        and -180.0 <= lon_f <= 180.0
    )


def valid_area(value) -> bool:
    try:
        number = float(value)
    except Exception:
        return False
    return bool(np.isfinite(number))


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return 2.0 * radius * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def symmetric_rel_error(a: float, b: float) -> float:
    denom = max(abs(a), abs(b))
    if denom == 0.0:
        return 0.0 if a == b else float("inf")
    return abs(a - b) / denom


def require_columns(df: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError("{} missing required columns: {}".format(label, ", ".join(missing)))


def read_csv_required(path: Path, label: str) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError("{} not found: {}".format(label, path))
    return pd.read_csv(path, low_memory=False)


def read_csv_optional(path: Optional[Path], label: str) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    if not path.is_file():
        print("Warning: optional {} not found: {}".format(label, path))
        return None
    return pd.read_csv(path, low_memory=False)


def ensure_station_id(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "station_id" not in work.columns:
        work.insert(0, "station_id", np.arange(len(work), dtype=int))
    work["station_id"] = pd.to_numeric(work["station_id"], errors="coerce")
    return work


def prepare_coordinate_basis(s5: pd.DataFrame, s4: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Attach the coordinate basis that s5 clustering actually used."""
    work = s5.copy()
    work["station_lat"] = pd.to_numeric(work["lat"], errors="coerce")
    work["station_lon"] = pd.to_numeric(work["lon"], errors="coerce")

    if s4 is not None:
        require_columns(s4, ["station_id", "lat", "lon"], "s4 basin CSV")
        s4_coord = s4[["station_id", "lat", "lon"]].copy()
        s4_coord["station_id"] = pd.to_numeric(s4_coord["station_id"], errors="coerce")
        s4_coord["s4_lat"] = pd.to_numeric(s4_coord["lat"], errors="coerce")
        s4_coord["s4_lon"] = pd.to_numeric(s4_coord["lon"], errors="coerce")
        s4_coord = s4_coord[["station_id", "s4_lat", "s4_lon"]]
        s4_coord = s4_coord.dropna(subset=["station_id"])
        s4_coord = s4_coord.drop_duplicates(subset=["station_id"], keep="first")
        work = work.merge(s4_coord, on="station_id", how="left")
    else:
        work["s4_lat"] = np.nan
        work["s4_lon"] = np.nan

    s4_valid = work.apply(lambda r: valid_lat_lon(r.get("s4_lat"), r.get("s4_lon")), axis=1)
    station_valid = work.apply(lambda r: valid_lat_lon(r.get("station_lat"), r.get("station_lon")), axis=1)
    work["cluster_lat"] = work["s4_lat"].where(s4_valid, work["station_lat"])
    work["cluster_lon"] = work["s4_lon"].where(s4_valid, work["station_lon"])
    work["coord_source"] = np.where(
        s4_valid,
        "s4_lat_lon",
        np.where(station_valid, "s5_station_lat_lon_fallback", "missing"),
    )

    offsets = []
    for _, row in work.iterrows():
        if valid_lat_lon(row.get("station_lat"), row.get("station_lon")) and valid_lat_lon(row.get("cluster_lat"), row.get("cluster_lon")):
            offsets.append(
                haversine_distance_m(
                    float(row["station_lat"]),
                    float(row["station_lon"]),
                    float(row["cluster_lat"]),
                    float(row["cluster_lon"]),
                )
            )
        else:
            offsets.append(np.nan)
    work["station_to_cluster_coord_offset_m"] = offsets
    return work


def add_coordinate_basis_summary(s5: pd.DataFrame, summary: List[Dict]) -> None:
    fallback = int(s5["coord_source"].eq("s5_station_lat_lon_fallback").sum()) if "coord_source" in s5.columns else 0
    missing = int(s5["coord_source"].eq("missing").sum()) if "coord_source" in s5.columns else len(s5)
    offset_review = int(
        (
            pd.to_numeric(
                s5.get("station_to_cluster_coord_offset_m", pd.Series(np.nan, index=s5.index)),
                errors="coerce",
            )
            > STATION_CLUSTER_OFFSET_REVIEW_M
        ).sum()
    )
    add_summary(summary, "distance_coordinate_basis_s4_missing_fallback", len(s5), len(s5) - fallback, fallback, severity="warning")
    add_summary(summary, "distance_coordinate_basis_missing", len(s5), len(s5) - missing, missing, severity="hard")
    add_summary(summary, "station_cluster_coordinate_offset_over_5km", len(s5), len(s5) - offset_review, offset_review, severity="warning")


def add_violation(rows: List[Dict], violation_type: str, severity: str, **kwargs) -> None:
    row = {"violation_type": violation_type, "severity": severity}
    row.update(kwargs)
    rows.append(row)


def check_membership_integrity(
    s3: pd.DataFrame,
    s5: pd.DataFrame,
    violations: List[Dict],
    summary: List[Dict],
) -> None:
    s3_ids = set(s3["station_id"].dropna().astype(int).tolist())
    s5_station = s5["station_id"]
    dup_mask = s5_station.duplicated(keep=False)
    n_dup_rows = int(dup_mask.sum())
    if n_dup_rows:
        for _, row in s5.loc[dup_mask].iterrows():
            add_violation(
                violations,
                "duplicate_station_id",
                "hard",
                station_id=row.get("station_id"),
                cluster_id=row.get("cluster_id"),
                detail="station_id appears more than once in s5",
            )
    s5_ids = set(s5_station.dropna().astype(int).tolist())
    lost = sorted(s3_ids.difference(s5_ids))
    for sid in lost:
        add_violation(
            violations,
            "lost_station",
            "hard",
            station_id=sid,
            detail="station_id present in s3 but absent from s5",
        )
    extra = sorted(s5_ids.difference(s3_ids))
    for sid in extra:
        add_violation(
            violations,
            "lost_station",
            "hard",
            station_id=sid,
            detail="station_id present in s5 but outside s3 station_id range",
        )

    add_summary(summary, "s3_s5_row_count", len(s3), int(len(s3) == len(s5)), int(len(s3) != len(s5)))
    add_summary(summary, "station_id_unique_in_s5", len(s5), len(s5) - n_dup_rows, n_dup_rows)
    add_summary(summary, "station_membership_closed", len(s3_ids), len(s3_ids) - len(lost) - len(extra), len(lost) + len(extra))


def add_summary(rows: List[Dict], check_item: str, checked: int, passed: int, failed: int, severity: str = "hard") -> None:
    status = "PASS" if failed == 0 else "FAIL"
    rows.append(
        {
            "check_item": check_item,
            "severity": severity,
            "checked": int(checked),
            "passed": int(max(0, passed)),
            "failed": int(max(0, failed)),
            "status": status,
        }
    )


def source_set(values: Iterable) -> str:
    cleaned = sorted({normalize_text(v) for v in values if normalize_text(v)})
    return "|".join(cleaned)


def recompute_report(s5: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for cid, grp in s5.groupby("cluster_id", dropna=False):
        rows.append(
            {
                "cluster_id": cid,
                "station_count": len(grp),
                "sources": source_set(grp["source"]) if "source" in grp.columns else "",
                "resolutions": source_set(grp["resolution"]) if "resolution" in grp.columns else "",
                "lat_mean": round(float(pd.to_numeric(grp["lat"], errors="coerce").mean()), 6),
                "lon_mean": round(float(pd.to_numeric(grp["lon"], errors="coerce").mean()), 6),
                "lat_min": round(float(pd.to_numeric(grp["lat"], errors="coerce").min()), 6),
                "lat_max": round(float(pd.to_numeric(grp["lat"], errors="coerce").max()), 6),
                "lon_min": round(float(pd.to_numeric(grp["lon"], errors="coerce").min()), 6),
                "lon_max": round(float(pd.to_numeric(grp["lon"], errors="coerce").max()), 6),
            }
        )
    return pd.DataFrame(rows)


def compare_s5_report(
    s5: pd.DataFrame,
    report: Optional[pd.DataFrame],
    violations: List[Dict],
    summary: List[Dict],
) -> None:
    if report is None:
        add_summary(summary, "s5_report_present", 1, 0, 1)
        add_violation(
            violations,
            "inconsistent_s5_report",
            "hard",
            detail="s5 report file is missing",
        )
        return
    required = [
        "cluster_id",
        "station_count",
        "sources",
        "resolutions",
        "lat_mean",
        "lon_mean",
        "lat_min",
        "lat_max",
        "lon_min",
        "lon_max",
    ]
    require_columns(report, required, "s5 report")
    expected = recompute_report(s5)
    merged = expected.merge(report, on="cluster_id", how="outer", suffixes=("_expected", "_report"), indicator=True)
    mismatches = 0
    numeric_cols = ["station_count", "lat_mean", "lon_mean", "lat_min", "lat_max", "lon_min", "lon_max"]
    text_cols = ["sources", "resolutions"]
    for _, row in merged.iterrows():
        reasons = []
        if row["_merge"] != "both":
            reasons.append("cluster_id presence mismatch")
        else:
            for col in numeric_cols:
                left = row.get(col + "_expected")
                right = row.get(col + "_report")
                if pd.isna(left) and pd.isna(right):
                    continue
                if col == "station_count":
                    ok = int(left) == int(right)
                else:
                    ok = np.isclose(float(left), float(right), atol=1e-6, rtol=0)
                if not ok:
                    reasons.append("{} expected={} report={}".format(col, left, right))
            for col in text_cols:
                left = normalize_text(row.get(col + "_expected"))
                right = normalize_text(row.get(col + "_report"))
                if left != right:
                    reasons.append("{} expected={} report={}".format(col, left, right))
        if reasons:
            mismatches += 1
            add_violation(
                violations,
                "inconsistent_s5_report",
                "hard",
                cluster_id=row.get("cluster_id"),
                detail="; ".join(reasons),
            )
    add_summary(summary, "s5_report_cluster_count", len(expected), len(expected) - mismatches, mismatches)


def pairwise_rows_for_group(grp: pd.DataFrame, max_distance_m: float, max_area_error: float) -> List[Dict]:
    rows = []
    records = list(grp.to_dict("records"))
    for left, right in itertools.combinations(records, 2):
        cid = left.get("cluster_id")
        sid1 = left.get("station_id")
        sid2 = right.get("station_id")
        lat1, lon1 = left.get("cluster_lat"), left.get("cluster_lon")
        lat2, lon2 = right.get("cluster_lat"), right.get("cluster_lon")
        area1, area2 = left.get("uparea_merit"), right.get("uparea_merit")
        if not (valid_lat_lon(lat1, lon1) and valid_lat_lon(lat2, lon2) and valid_area(area1) and valid_area(area2)):
            dist = np.nan
            err = np.nan
            distance_pass = False
            area_pass = False
        else:
            dist = haversine_distance_m(float(lat1), float(lon1), float(lat2), float(lon2))
            err = symmetric_rel_error(float(area1), float(area2))
            distance_pass = bool(dist <= max_distance_m)
            area_pass = bool(err <= max_area_error)
        rows.append(
            {
                "cluster_id": cid,
                "station_id_1": sid1,
                "station_id_2": sid2,
                "source_1": left.get("source", ""),
                "source_2": right.get("source", ""),
                "basin_id_1": left.get("basin_id"),
                "basin_id_2": right.get("basin_id"),
                "distance_coordinate_basis": DISTANCE_COORDINATE_BASIS,
                "coord_source_1": left.get("coord_source", ""),
                "coord_source_2": right.get("coord_source", ""),
                "cluster_lat_1": left.get("cluster_lat"),
                "cluster_lon_1": left.get("cluster_lon"),
                "cluster_lat_2": right.get("cluster_lat"),
                "cluster_lon_2": right.get("cluster_lon"),
                "station_lat_1": left.get("station_lat"),
                "station_lon_1": left.get("station_lon"),
                "station_lat_2": right.get("station_lat"),
                "station_lon_2": right.get("station_lon"),
                "distance_m": dist,
                "uparea_1": area1,
                "uparea_2": area2,
                "area_relative_error": err,
                "distance_pass": distance_pass,
                "area_pass": area_pass,
            }
        )
    return rows


def validate_multistation_clusters(
    s5: pd.DataFrame,
    violations: List[Dict],
    summary: List[Dict],
    max_distance_m: float,
    max_area_error: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pair_rows: List[Dict] = []
    cluster_rows: List[Dict] = []
    n_multi = 0
    hard_fail_clusters = set()
    for cid, grp in s5.groupby("cluster_id", dropna=False):
        station_count = len(grp)
        sources = source_set(grp["source"]) if "source" in grp.columns else ""
        resolutions = source_set(grp["resolution"]) if "resolution" in grp.columns else ""
        basin_values = source_set(grp["basin_id"]) if "basin_id" in grp.columns else ""
        manual_reasons = []
        pair_df = pd.DataFrame(columns=PAIRWISE_COLUMNS)
        if station_count > 1:
            n_multi += 1
            statuses = grp["basin_status"].fillna("").astype(str).str.strip().str.lower()
            unresolved = grp.loc[~statuses.eq("resolved")]
            if len(unresolved):
                hard_fail_clusters.add(cid)
                for _, row in unresolved.iterrows():
                    add_violation(
                        violations,
                        "unresolved_in_multistation_cluster",
                        "hard",
                        cluster_id=cid,
                        station_id=row.get("station_id"),
                        basin_status=row.get("basin_status"),
                    )
            basin_ids = pd.to_numeric(grp["basin_id"], errors="coerce")
            valid_basin = basin_ids.map(lambda x: valid_basin_id(x))
            if (not bool(valid_basin.all())) or int(basin_ids[valid_basin].nunique()) != 1:
                hard_fail_clusters.add(cid)
                add_violation(
                    violations,
                    "inconsistent_basin_id",
                    "hard",
                    cluster_id=cid,
                    detail="multi-station cluster must have one non-missing basin_id",
                    basin_ids=basin_values,
                )
            if "observation_type" in grp.columns:
                sat = grp.loc[grp["observation_type"].map(is_satellite)]
                if len(sat):
                    hard_fail_clusters.add(cid)
                    for _, row in sat.iterrows():
                        add_violation(
                            violations,
                            "satellite_in_multistation_cluster",
                            "hard",
                            cluster_id=cid,
                            station_id=row.get("station_id"),
                            observation_type=row.get("observation_type"),
                        )
            invalid_input = grp.loc[
                ~grp.apply(
                    lambda r: valid_lat_lon(r.get("cluster_lat"), r.get("cluster_lon")) and valid_area(r.get("uparea_merit")),
                    axis=1,
                )
            ]
            if len(invalid_input):
                hard_fail_clusters.add(cid)
                for _, row in invalid_input.iterrows():
                    add_violation(
                        violations,
                        "missing_pairwise_input",
                        "hard",
                        cluster_id=cid,
                        station_id=row.get("station_id"),
                        cluster_lat=row.get("cluster_lat"),
                        cluster_lon=row.get("cluster_lon"),
                        coord_source=row.get("coord_source"),
                        uparea_merit=row.get("uparea_merit"),
                    )
            station_ids = pd.to_numeric(grp["station_id"], errors="coerce")
            try:
                cid_num = float(cid)
                rep_ok = int(cid_num) == int(station_ids.min()) and bool((station_ids == cid_num).any())
            except Exception:
                rep_ok = False
            if not rep_ok:
                hard_fail_clusters.add(cid)
                add_violation(
                    violations,
                    "invalid_cluster_representative",
                    "hard",
                    cluster_id=cid,
                    expected_cluster_id=station_ids.min(),
                    detail="cluster_id must equal the minimum station_id in the cluster",
                )
            pair_rows.extend(pairwise_rows_for_group(grp, max_distance_m, max_area_error))
            pair_df = pd.DataFrame(pair_rows[-(station_count * (station_count - 1) // 2):], columns=PAIRWISE_COLUMNS)
            if len(pair_df):
                bad_dist = pair_df.loc[~pair_df["distance_pass"].fillna(False)]
                bad_area = pair_df.loc[~pair_df["area_pass"].fillna(False)]
                for _, row in bad_dist.iterrows():
                    hard_fail_clusters.add(cid)
                    add_violation(
                        violations,
                        "distance_threshold_violation",
                        "hard",
                        cluster_id=cid,
                        station_id_1=row.get("station_id_1"),
                        station_id_2=row.get("station_id_2"),
                        distance_m=row.get("distance_m"),
                        threshold_m=max_distance_m,
                    )
                for _, row in bad_area.iterrows():
                    hard_fail_clusters.add(cid)
                    add_violation(
                        violations,
                        "area_threshold_violation",
                        "hard",
                        cluster_id=cid,
                        station_id_1=row.get("station_id_1"),
                        station_id_2=row.get("station_id_2"),
                        area_relative_error=row.get("area_relative_error"),
                        threshold=max_area_error,
                    )
        max_dist = np.nan
        max_area = np.nan
        if len(pair_df):
            max_dist = float(pd.to_numeric(pair_df["distance_m"], errors="coerce").max())
            max_area = float(pd.to_numeric(pair_df["area_relative_error"], errors="coerce").max())
            if max_dist >= NEAR_DISTANCE_M:
                manual_reasons.append("near_distance_threshold")
            if max_area >= NEAR_AREA_ERROR:
                manual_reasons.append("near_area_threshold")
        if source_set(grp.get("source", [])) and len(source_set(grp.get("source", [])).split("|")) > 1:
            manual_reasons.append("multi_source")
        if source_set(grp.get("resolution", [])) and len(source_set(grp.get("resolution", [])).split("|")) > 1:
            manual_reasons.append("multi_resolution")
        if station_count > LARGE_CLUSTER_SIZE:
            manual_reasons.append("large_cluster")
        max_offset = pd.to_numeric(
            grp.get("station_to_cluster_coord_offset_m", pd.Series(np.nan, index=grp.index)),
            errors="coerce",
        ).max()
        if pd.notna(max_offset) and float(max_offset) > STATION_CLUSTER_OFFSET_REVIEW_M:
            manual_reasons.append("station_cluster_coordinate_offset")
        if inconsistent_names(grp):
            manual_reasons.append("name_or_country_inconsistent")
        cluster_rows.append(
            {
                "cluster_id": cid,
                "station_count": station_count,
                "sources": sources,
                "resolutions": resolutions,
                "basin_id": basin_values,
                "max_pairwise_distance_m": max_dist,
                "max_pairwise_area_error": max_area,
                "distance_coordinate_basis": DISTANCE_COORDINATE_BASIS,
                "max_station_to_cluster_coord_offset_m": max_offset,
                "multi_source": "|" in sources,
                "multi_resolution": "|" in resolutions,
                "near_threshold": ("near_distance_threshold" in manual_reasons) or ("near_area_threshold" in manual_reasons),
                "manual_review_reason": "|".join(sorted(set(manual_reasons))),
            }
        )
    pairwise_df = pd.DataFrame(pair_rows, columns=PAIRWISE_COLUMNS)
    cluster_df = pd.DataFrame(cluster_rows)
    add_summary(summary, "multistation_cluster_hard_rules", n_multi, n_multi - len(hard_fail_clusters), len(hard_fail_clusters))
    return pairwise_df, cluster_df


def inconsistent_names(grp: pd.DataFrame) -> bool:
    for col in ["station_name", "river_name", "country"]:
        if col in grp.columns:
            values = {normalize_text(v).lower() for v in grp[col] if normalize_text(v)}
            if len(values) > 1:
                return True
    return False


def validate_singleton_rules(s5: pd.DataFrame, violations: List[Dict], summary: List[Dict]) -> None:
    checks = []
    status = s5["basin_status"].fillna("").astype(str).str.strip().str.lower() if "basin_status" in s5.columns else pd.Series("", index=s5.index)
    basin_valid = s5["basin_id"].map(valid_basin_id) if "basin_id" in s5.columns else pd.Series(False, index=s5.index)
    obs_sat = s5["observation_type"].map(is_satellite) if "observation_type" in s5.columns else pd.Series(False, index=s5.index)
    coord_valid = s5.apply(lambda r: valid_lat_lon(r.get("cluster_lat"), r.get("cluster_lon")), axis=1)
    area_valid = s5["uparea_merit"].map(valid_area) if "uparea_merit" in s5.columns else pd.Series(False, index=s5.index)
    masks = {
        "non_resolved_singleton": ~status.eq("resolved"),
        "invalid_basin_id_singleton": ~basin_valid,
        "satellite_singleton": obs_sat,
        "invalid_lat_lon_singleton": ~coord_valid,
        "invalid_uparea_singleton": ~area_valid,
    }
    for name, mask in masks.items():
        subset = s5.loc[mask].copy()
        failed = 0
        for _, row in subset.iterrows():
            try:
                ok = int(float(row.get("cluster_id"))) == int(float(row.get("station_id")))
            except Exception:
                ok = False
            if not ok:
                failed += 1
                add_violation(
                    violations,
                    "invalid_cluster_representative",
                    "hard",
                    check_item=name,
                    station_id=row.get("station_id"),
                    cluster_id=row.get("cluster_id"),
                    detail="non-candidate station must remain singleton with cluster_id == station_id",
                )
        checks.append((name, len(subset), len(subset) - failed, failed))
    for name, checked, passed, failed in checks:
        add_summary(summary, name, checked, passed, failed)

    unresolved = s5.loc[~status.eq("resolved")]
    failed_fields = 0
    for _, row in unresolved.iterrows():
        dirty = [c for c in RELEASE_BASIN_FIELDS if c in s5.columns and not is_blank(row.get(c))]
        if dirty:
            failed_fields += 1
            add_violation(
                violations,
                "unresolved_basin_fields_not_cleared",
                "hard",
                station_id=row.get("station_id"),
                cluster_id=row.get("cluster_id"),
                dirty_fields="|".join(dirty),
                detail="unresolved stations should not expose release-facing basin fields",
            )
    add_summary(summary, "unresolved_release_basin_fields_cleared", len(unresolved), len(unresolved) - failed_fields, failed_fields)


def candidate_mask(s5: pd.DataFrame) -> pd.Series:
    status = s5["basin_status"].fillna("").astype(str).str.strip().str.lower()
    basin_valid = s5["basin_id"].map(valid_basin_id)
    obs_sat = s5["observation_type"].map(is_satellite) if "observation_type" in s5.columns else pd.Series(False, index=s5.index)
    coord_valid = s5.apply(lambda r: valid_lat_lon(r.get("cluster_lat"), r.get("cluster_lon")), axis=1)
    area_valid = s5["uparea_merit"].map(valid_area)
    return status.eq("resolved") & basin_valid & ~obs_sat & coord_valid & area_valid


def cross_pair_metrics(left: pd.DataFrame, right: pd.DataFrame, max_distance_m: float, max_area_error: float) -> Tuple[List[Dict], bool, str]:
    rows = []
    any_distance_fail = False
    any_area_fail = False
    for _, lrow in left.iterrows():
        for _, rrow in right.iterrows():
            dist = haversine_distance_m(
                float(lrow["cluster_lat"]),
                float(lrow["cluster_lon"]),
                float(rrow["cluster_lat"]),
                float(rrow["cluster_lon"]),
            )
            err = symmetric_rel_error(float(lrow["uparea_merit"]), float(rrow["uparea_merit"]))
            distance_pass = bool(dist <= max_distance_m)
            area_pass = bool(err <= max_area_error)
            any_distance_fail = any_distance_fail or not distance_pass
            any_area_fail = any_area_fail or not area_pass
            rows.append(
                {
                    "station_id_1": lrow.get("station_id"),
                    "station_id_2": rrow.get("station_id"),
                    "source_1": lrow.get("source", ""),
                    "source_2": rrow.get("source", ""),
                    "basin_id_1": lrow.get("basin_id"),
                    "basin_id_2": rrow.get("basin_id"),
                    "distance_coordinate_basis": DISTANCE_COORDINATE_BASIS,
                    "coord_source_1": lrow.get("coord_source", ""),
                    "coord_source_2": rrow.get("coord_source", ""),
                    "cluster_lat_1": lrow.get("cluster_lat"),
                    "cluster_lon_1": lrow.get("cluster_lon"),
                    "cluster_lat_2": rrow.get("cluster_lat"),
                    "cluster_lon_2": rrow.get("cluster_lon"),
                    "station_lat_1": lrow.get("station_lat"),
                    "station_lon_1": lrow.get("station_lon"),
                    "station_lat_2": rrow.get("station_lat"),
                    "station_lon_2": rrow.get("station_lon"),
                    "distance_m": dist,
                    "uparea_1": lrow.get("uparea_merit"),
                    "uparea_2": rrow.get("uparea_merit"),
                    "area_relative_error": err,
                    "distance_pass": distance_pass,
                    "area_pass": area_pass,
                }
            )
    if not any_distance_fail and not any_area_fail:
        reason = "missed_merge"
        can_merge = True
    elif any_distance_fail and any_area_fail:
        reason = "distance_and_area_blocked"
        can_merge = False
    elif any_distance_fail:
        reason = "distance_blocked"
        can_merge = False
    else:
        reason = "area_blocked"
        can_merge = False
    return rows, can_merge, reason


def detect_missed_merges(
    s5: pd.DataFrame,
    violations: List[Dict],
    summary: List[Dict],
    max_distance_m: float,
    max_area_error: float,
) -> pd.DataFrame:
    candidates = s5.loc[candidate_mask(s5)].copy()
    reason_rows = []
    checked = 0
    missed = 0
    for basin_id, basin_grp in candidates.groupby("basin_id"):
        cluster_ids = sorted(basin_grp["cluster_id"].dropna().unique().tolist(), key=lambda x: float(x))
        if len(cluster_ids) < 2:
            continue
        groups = {cid: basin_grp.loc[basin_grp["cluster_id"] == cid] for cid in cluster_ids}
        for cid1, cid2 in itertools.combinations(cluster_ids, 2):
            checked += 1
            cross_rows, can_merge, reason = cross_pair_metrics(groups[cid1], groups[cid2], max_distance_m, max_area_error)
            reason_rows.append(
                {
                    "basin_id": basin_id,
                    "cluster_id_1": cid1,
                    "cluster_id_2": cid2,
                    "reason": reason,
                    "n_cross_pairs": len(cross_rows),
                }
            )
            if can_merge:
                missed += 1
                violation_id = "missed_merge:{}:{}:{}".format(basin_id, cid1, cid2)
                for pair in cross_rows:
                    add_violation(
                        violations,
                        "missed_merge",
                        "hard",
                        violation_id=violation_id,
                        basin_id=basin_id,
                        cluster_id_1=cid1,
                        cluster_id_2=cid2,
                        detail="complete-linkage cross-cluster pair passes both thresholds",
                        **pair,
                    )
    add_summary(summary, "missed_merge_complete_linkage_recheck", checked, checked - missed, missed)
    return pd.DataFrame(reason_rows)


def descriptive_stats(s5: pd.DataFrame, cluster_df: pd.DataFrame, same_basin_reasons: pd.DataFrame) -> Dict:
    total_stations = len(s5)
    total_clusters = int(s5["cluster_id"].nunique(dropna=False))
    sizes = s5.groupby("cluster_id", dropna=False).size()
    singleton = int((sizes == 1).sum())
    multi = int((sizes > 1).sum())
    size_bins = {
        "1": int((sizes == 1).sum()),
        "2": int((sizes == 2).sum()),
        "3-5": int(((sizes >= 3) & (sizes <= 5)).sum()),
        "6-10": int(((sizes >= 6) & (sizes <= 10)).sum()),
        ">10": int((sizes > 10).sum()),
    }
    multi_source = int(cluster_df["multi_source"].sum()) if "multi_source" in cluster_df.columns else 0
    multi_resolution = int(cluster_df["multi_resolution"].sum()) if "multi_resolution" in cluster_df.columns else 0
    source_combos = cluster_df["sources"].value_counts(dropna=False).to_dict() if "sources" in cluster_df.columns else {}
    max_size = int(sizes.max()) if len(sizes) else 0
    max_cluster_id = sizes.idxmax() if len(sizes) else ""
    max_members = s5.loc[s5["cluster_id"] == max_cluster_id, "station_id"].astype(str).tolist() if len(sizes) else []
    dist_stats = quantile_stats(cluster_df["max_pairwise_distance_m"])
    area_stats = quantile_stats(cluster_df["max_pairwise_area_error"])
    near_distance = int((pd.to_numeric(cluster_df["max_pairwise_distance_m"], errors="coerce") >= NEAR_DISTANCE_M).sum())
    near_area = int((pd.to_numeric(cluster_df["max_pairwise_area_error"], errors="coerce") >= NEAR_AREA_ERROR).sum())
    name_inconsistent = int(cluster_df["manual_review_reason"].fillna("").str.contains("name_or_country_inconsistent").sum())
    offset_clusters = int(cluster_df["manual_review_reason"].fillna("").str.contains("station_cluster_coordinate_offset").sum())
    reason_counts = same_basin_reasons["reason"].value_counts().to_dict() if len(same_basin_reasons) else {}
    return {
        "source_station_total": total_stations,
        "cluster_total": total_clusters,
        "singleton_clusters": singleton,
        "multi_station_clusters": multi,
        "singleton_ratio": singleton / total_clusters if total_clusters else 0.0,
        "multi_station_ratio": multi / total_clusters if total_clusters else 0.0,
        "cluster_size_distribution": size_bins,
        "multi_source_clusters": multi_source,
        "multi_resolution_clusters": multi_resolution,
        "source_combination_counts": source_combos,
        "max_cluster_id": max_cluster_id,
        "max_cluster_size": max_size,
        "max_cluster_members": max_members,
        "distance_stats": dist_stats,
        "area_error_stats": area_stats,
        "near_distance_clusters": near_distance,
        "near_area_clusters": near_area,
        "name_country_inconsistent_clusters": name_inconsistent,
        "station_cluster_coordinate_offset_clusters": offset_clusters,
        "same_basin_unmerged_reason_counts": reason_counts,
    }


def quantile_stats(series: pd.Series) -> Dict[str, float]:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) == 0:
        return {k: np.nan for k in ["min", "median", "p90", "p95", "p99", "max"]}
    return {
        "min": float(clean.min()),
        "median": float(clean.quantile(0.50)),
        "p90": float(clean.quantile(0.90)),
        "p95": float(clean.quantile(0.95)),
        "p99": float(clean.quantile(0.99)),
        "max": float(clean.max()),
    }


def build_manual_review(s5: pd.DataFrame, cluster_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, crow in cluster_df.iterrows():
        reasons = [r for r in normalize_text(crow.get("manual_review_reason")).split("|") if r]
        if not reasons:
            continue
        cid = crow["cluster_id"]
        members = s5.loc[s5["cluster_id"] == cid, "station_id"].astype(str).tolist()
        for reason in reasons:
            rows.append(
                {
                    "cluster_id": cid,
                    "review_reason": reason,
                    "station_count": crow.get("station_count"),
                    "sources": crow.get("sources"),
                    "resolutions": crow.get("resolutions"),
                    "basin_id": crow.get("basin_id"),
                    "max_pairwise_distance_m": crow.get("max_pairwise_distance_m"),
                    "max_pairwise_area_error": crow.get("max_pairwise_area_error"),
                    "distance_coordinate_basis": crow.get("distance_coordinate_basis"),
                    "max_station_to_cluster_coord_offset_m": crow.get("max_station_to_cluster_coord_offset_m"),
                    "station_ids": "|".join(members),
                }
            )
    return pd.DataFrame(rows)


def method_semantics() -> List[Tuple[str, str]]:
    return [
        (
            "basin_id_semantics",
            "s4 writes basin_id from basin_tracer reach_info['COMID']; s4 then traces the full upstream basin from that selected MERIT reach. Thus s5 groups by selected/matched MERIT reach COMID, not by a separately minted upstream-basin polygon id.",
        ),
        (
            "same_merit_basins_river_reach",
            "The code implements grouping by identical selected MERIT reach COMID plus complete-linkage distance and uparea checks. This is compatible with a strict 'same MERIT-Basins river reach' statement only if basin_id is described as the selected MERIT reach COMID; it is not merely 'same upstream basin polygon'.",
        ),
        (
            "area_variable",
            "Clustering uses uparea_merit by default. s4 writes basin_area equal to the selected reach uparea, while reported_area is used only during reach selection/audit and is not the clustering variable.",
        ),
        (
            "resolution_behavior",
            "s5 does not group by resolution, so clustering can cross daily/monthly/annual/climatology if all basin/distance/uparea rules pass. Multi-resolution clusters are therefore code-permitted.",
        ),
        (
            "satellite_behavior",
            "basin_station_merge excludes observation_type in {satellite, remote_sensing, remote_sensing_observation, satellite_observation} from merge candidates, so those rows should remain singleton and cannot enter the same s5 cluster as main in-situ stations through s5.",
        ),
        (
            "threshold_note",
            "basin_station_merge accepts max_station_distance_m as a parameter. The current validation default and s5_basin_merge.py built-in default both use 1000 m.",
        ),
        (
            "distance_coordinate_basis",
            "Pairwise distances are recomputed with the same coordinate basis used by s5 clustering: s4_upstream_basins.csv lat/lon, with fallback to s5 station coordinates only when s4 coordinates are missing. The s5 output lat/lon columns remain the original station coordinates and are not treated as the clustering-distance basis.",
        ),
    ]


def markdown_table(df: pd.DataFrame, max_rows: Optional[int] = None) -> str:
    if df is None or len(df) == 0:
        return "_No rows._"
    work = df.head(max_rows).copy() if max_rows else df.copy()
    cols = list(work.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for _, row in work.iterrows():
        values = [normalize_text(row.get(col)).replace("|", "\\|") for col in cols]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_report(
    path: Path,
    stats: Dict,
    summary_df: pd.DataFrame,
    violations_df: pd.DataFrame,
    manual_df: pd.DataFrame,
    same_basin_reasons: pd.DataFrame,
    cfg: Config,
) -> None:
    hard_df = violations_df.loc[violations_df.get("severity", pd.Series(dtype=str)).eq("hard")] if len(violations_df) else violations_df
    warning_count = len(manual_df)
    lines = [
        "# Hydrological Clustering Validation Report",
        "",
        "This analysis is a rule-based structural validation of s5 hydrological clustering outputs. It is not an independent external validation of MERIT-Basins matching accuracy.",
        "",
        "## Inputs",
        "",
        "- s3: `{}`".format(cfg.s3_csv),
        "- s4: `{}`".format(cfg.s4_csv if cfg.s4_csv else ""),
        "- s5: `{}`".format(cfg.s5_csv),
        "- s5 report: `{}`".format(cfg.s5_report_csv if cfg.s5_report_csv else ""),
        "- distance threshold: {:.1f} m".format(cfg.max_distance_m),
        "- upstream-area symmetric relative error threshold: {:.3f}".format(cfg.max_area_error),
        "",
        "## Hard Violations",
        "",
        "- hard violation rows: {:,}".format(len(hard_df)),
        "- missed_merge rows: {:,}".format(int((violations_df.get("violation_type", pd.Series(dtype=str)) == "missed_merge").sum()) if len(violations_df) else 0),
    ]
    if len(hard_df):
        lines += ["", "Top hard violations:"]
        for _, row in hard_df.head(50).iterrows():
            lines.append(
                "- {violation_type}: cluster={cluster} station={station} detail={detail}".format(
                    violation_type=row.get("violation_type", ""),
                    cluster=row.get("cluster_id", row.get("cluster_id_1", "")),
                    station=row.get("station_id", row.get("station_id_1", "")),
                    detail=row.get("detail", ""),
                )
            )
    lines += [
        "",
        "## Warnings And Manual Review",
        "",
        "- manual review rows: {:,}".format(warning_count),
        "- near-distance clusters: {:,}".format(stats["near_distance_clusters"]),
        "- near-area clusters: {:,}".format(stats["near_area_clusters"]),
        "- station-vs-clustering coordinate offset clusters: {:,}".format(stats["station_cluster_coordinate_offset_clusters"]),
        "- name/river/country inconsistent clusters: {:,}".format(stats["name_country_inconsistent_clusters"]),
        "",
        "## Descriptive Diagnostics",
        "",
        "- source stations: {:,}".format(stats["source_station_total"]),
        "- clusters: {:,}".format(stats["cluster_total"]),
        "- singleton clusters: {:,} ({:.2%})".format(stats["singleton_clusters"], stats["singleton_ratio"]),
        "- multi-station clusters: {:,} ({:.2%})".format(stats["multi_station_clusters"], stats["multi_station_ratio"]),
        "- multi-source clusters: {:,}".format(stats["multi_source_clusters"]),
        "- multi-resolution clusters: {:,}".format(stats["multi_resolution_clusters"]),
        "- cluster size distribution: {}".format(stats["cluster_size_distribution"]),
        "- max cluster: {} with {:,} members".format(stats["max_cluster_id"], stats["max_cluster_size"]),
        "- max cluster members: {}".format("|".join(stats["max_cluster_members"])),
        "- cluster-level max pairwise distance stats: {}".format(stats["distance_stats"]),
        "- cluster-level max pairwise area error stats: {}".format(stats["area_error_stats"]),
        "- distance coordinate basis: {}".format(DISTANCE_COORDINATE_BASIS),
        "- same-basin unmerged reason counts: {}".format(stats["same_basin_unmerged_reason_counts"]),
        "",
        "## Method Semantics",
        "",
    ]
    for key, text in method_semantics():
        lines.append("- **{}**: {}".format(key, text))
    lines += [
        "",
        "## Manuscript Results Candidate Text",
        "",
        (
            "A rule-based structural validation of the hydrological clustering output examined "
            "{:,} source stations grouped into {:,} clusters, including {:,} multi-station clusters. "
            "Using the implemented complete-linkage criteria (all pairwise clustering-coordinate distances <= {:.0f} m "
            "and all pairwise upstream-area symmetric relative errors <= {:.2f}), the validation found "
            "{:,} hard-rule violation rows and {:,} missed-merge violation rows. "
            "{:,} clusters were flagged for manual review based on near-threshold distances or area errors, "
            "large cluster size, mixed sources/resolutions, or inconsistent station/river/country names."
        ).format(
            stats["source_station_total"],
            stats["cluster_total"],
            stats["multi_station_clusters"],
            cfg.max_distance_m,
            cfg.max_area_error,
            len(hard_df),
            int((violations_df.get("violation_type", pd.Series(dtype=str)) == "missed_merge").sum()) if len(violations_df) else 0,
            len(manual_df),
        ),
        "",
        "## Summary Table",
        "",
        markdown_table(summary_df),
    ]
    if len(same_basin_reasons):
        reason_df = same_basin_reasons["reason"].value_counts().rename_axis("reason").reset_index(name="count")
        lines += ["", "## Same-Basin Unmerged Cluster Pair Reasons", "", markdown_table(reason_df)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_validation(cfg: Config) -> Dict[str, object]:
    s3 = ensure_station_id(read_csv_required(cfg.s3_csv, "s3 CSV"))
    s5 = read_csv_required(cfg.s5_csv, "s5 clustered station CSV")
    s4 = read_csv_optional(cfg.s4_csv, "s4 basin CSV")
    report = read_csv_optional(cfg.s5_report_csv, "s5 report CSV")

    require_columns(s3, ["station_id"], "s3 CSV")
    require_columns(
        s5,
        ["station_id", "cluster_id", "source", "resolution", "lat", "lon", "basin_id", "basin_status", "uparea_merit"],
        "s5 clustered station CSV",
    )
    s5 = s5.copy()
    s5["station_id"] = pd.to_numeric(s5["station_id"], errors="coerce")
    s5["cluster_id"] = pd.to_numeric(s5["cluster_id"], errors="coerce")
    if "observation_type" not in s5.columns:
        s5["observation_type"] = ""
    s5 = prepare_coordinate_basis(s5, s4)

    violations: List[Dict] = []
    summary: List[Dict] = []
    add_coordinate_basis_summary(s5, summary)
    check_membership_integrity(s3, s5, violations, summary)
    compare_s5_report(s5, report, violations, summary)
    pairwise_df, cluster_df = validate_multistation_clusters(
        s5, violations, summary, cfg.max_distance_m, cfg.max_area_error
    )
    validate_singleton_rules(s5, violations, summary)
    same_basin_reasons = detect_missed_merges(s5, violations, summary, cfg.max_distance_m, cfg.max_area_error)

    # Explicit cluster representative existence check for all clusters.
    invalid_rep = 0
    for cid, grp in s5.groupby("cluster_id", dropna=False):
        if not bool((grp["station_id"] == cid).any()):
            invalid_rep += 1
            add_violation(
                violations,
                "invalid_cluster_representative",
                "hard",
                cluster_id=cid,
                detail="cluster_id does not exist among member station_id values",
            )
    add_summary(summary, "cluster_id_exists_as_member_station_id", s5["cluster_id"].nunique(dropna=False), s5["cluster_id"].nunique(dropna=False) - invalid_rep, invalid_rep)

    if s4 is not None:
        require_columns(s4, ["station_id", "basin_id"], "s4 basin CSV")
        s4_ids = set(pd.to_numeric(s4["station_id"], errors="coerce").dropna().astype(int).tolist())
        s5_ids = set(s5["station_id"].dropna().astype(int).tolist())
        missing_s4 = len(s5_ids.difference(s4_ids))
        add_summary(summary, "s4_s5_station_id_coverage", len(s5_ids), len(s5_ids) - missing_s4, missing_s4, severity="warning")

    cluster_df = cluster_df.sort_values(["station_count", "cluster_id"], ascending=[False, True])
    manual_df = build_manual_review(s5, cluster_df)
    violations_df = pd.DataFrame(violations)
    if len(violations_df) == 0:
        violations_df = pd.DataFrame(columns=["violation_type", "severity"])
    summary_df = pd.DataFrame(summary)
    stats = descriptive_stats(s5, cluster_df, same_basin_reasons)

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary": cfg.out_dir / "hydrological_clustering_validation_summary.csv",
        "cluster_summary": cfg.out_dir / "hydrological_clustering_cluster_summary.csv",
        "pairwise": cfg.out_dir / "hydrological_clustering_pairwise_diagnostics.csv.gz",
        "violations": cfg.out_dir / "hydrological_clustering_rule_violations.csv",
        "manual_review": cfg.out_dir / "hydrological_clustering_manual_review.csv",
        "report": cfg.out_dir / "hydrological_clustering_validation_report.md",
        "same_basin_reasons": cfg.out_dir / "hydrological_clustering_same_basin_unmerged_reasons.csv",
    }
    summary_df.to_csv(paths["summary"], index=False)
    cluster_df.to_csv(paths["cluster_summary"], index=False)
    pairwise_df.to_csv(paths["pairwise"], index=False, compression="gzip")
    violations_df.to_csv(paths["violations"], index=False)
    manual_df.to_csv(paths["manual_review"], index=False)
    same_basin_reasons.to_csv(paths["same_basin_reasons"], index=False)
    write_report(paths["report"], stats, summary_df, violations_df, manual_df, same_basin_reasons, cfg)

    return {
        "paths": paths,
        "stats": stats,
        "summary": summary_df,
        "violations": violations_df,
        "manual_review": manual_df,
        "cluster_summary": cluster_df,
        "pairwise": pairwise_df,
    }


def default_config() -> Config:
    output_r_root = get_output_r_root(REPO_ROOT)
    return Config(
        s3_csv=output_r_root / S3_COLLECTED_CSV,
        s4_csv=output_r_root / S4_UPSTREAM_CSV,
        s5_csv=output_r_root / S5_BASIN_CLUSTERED_CSV,
        s5_report_csv=output_r_root / S5_BASIN_REPORT_CSV,
        out_dir=REPO_ROOT / "validate" / "output" / "s13_validate_hydrological_clustering",
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    defaults = default_config()
    ap = argparse.ArgumentParser(description="Validate s5 hydrological clustering outputs without modifying inputs.")
    ap.add_argument("--s3-csv", default=str(defaults.s3_csv))
    ap.add_argument("--s4-csv", default=str(defaults.s4_csv))
    ap.add_argument("--s5-csv", default=str(defaults.s5_csv))
    ap.add_argument("--s5-report-csv", default=str(defaults.s5_report_csv))
    ap.add_argument("--out-dir", default=str(defaults.out_dir))
    ap.add_argument("--max-distance-m", type=float, default=DEFAULT_MAX_DISTANCE_M)
    ap.add_argument("--max-area-error", type=float, default=DEFAULT_MAX_AREA_ERROR)
    args = ap.parse_args(argv)
    return Config(
        s3_csv=Path(args.s3_csv).expanduser().resolve(),
        s4_csv=Path(args.s4_csv).expanduser().resolve() if normalize_text(args.s4_csv) else None,
        s5_csv=Path(args.s5_csv).expanduser().resolve(),
        s5_report_csv=Path(args.s5_report_csv).expanduser().resolve() if normalize_text(args.s5_report_csv) else None,
        out_dir=Path(args.out_dir).expanduser().resolve(),
        max_distance_m=float(args.max_distance_m),
        max_area_error=float(args.max_area_error),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    result = run_validation(cfg)
    violations = result["violations"]
    manual = result["manual_review"]
    stats = result["stats"]
    hard_count = int((violations["severity"] == "hard").sum()) if len(violations) else 0
    missed_count = int((violations["violation_type"] == "missed_merge").sum()) if len(violations) else 0
    print("Hydrological clustering validation complete")
    print("Output directory: {}".format(cfg.out_dir))
    print("Hard violation rows: {:,}".format(hard_count))
    print("Missed-merge violation rows: {:,}".format(missed_count))
    print("Warnings/manual-review rows: {:,}".format(len(manual)))
    print("Clusters: {:,}".format(stats["cluster_total"]))
    print("Multi-station clusters: {:,}".format(stats["multi_station_clusters"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
