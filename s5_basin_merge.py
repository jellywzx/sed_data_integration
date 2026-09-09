#!/usr/bin/env python3
"""Assign basin-based cluster IDs to s3 stations using basin tracer output."""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from basin_station_merge import load_station_to_basin_cluster_map
from pipeline_paths import (
    S3_COLLECTED_CSV,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    S5_BASIN_REPORT_CSV,
    get_output_r_root,
)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

_DEFAULT_S3_CSV    = PROJECT_ROOT / S3_COLLECTED_CSV
_DEFAULT_OUT       = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
_DEFAULT_REPORT    = PROJECT_ROOT / S5_BASIN_REPORT_CSV
_DEFAULT_BASIN_CSV = PROJECT_ROOT / S4_UPSTREAM_CSV

DEFAULT_MAX_STATION_DISTANCE_M = 1000.0
DEFAULT_MAX_UPSTREAM_REL_ERROR = 0.10
DEFAULT_UPSTREAM_AREA_COL = "uparea_merit"


def _build_cluster_report(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize station information by cluster_id for the report DataFrame."""
    rows = []
    for cid, grp in df.groupby("cluster_id"):
        rows.append(
            {
                "cluster_id":    cid,
                "station_count": len(grp),
                "sources":       "|".join(sorted(grp["source"].dropna().unique())),
                "resolutions":   "|".join(sorted(grp["resolution"].dropna().unique())),
                "lat_mean":      round(float(grp["lat"].mean()), 6),
                "lon_mean":      round(float(grp["lon"].mean()), 6),
                "lat_min":       round(float(grp["lat"].min()), 6),
                "lat_max":       round(float(grp["lat"].max()), 6),
                "lon_min":       round(float(grp["lon"].min()), 6),
                "lon_max":       round(float(grp["lon"].max()), 6),
            }
        )
    return pd.DataFrame(rows)


def _mask_unresolved_basin_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Hide release-facing basin attributes for unresolved stations."""
    if "basin_status" not in df.columns:
        return df

    work = df.copy()
    unresolved = (
        work["basin_status"].fillna("").astype(str).str.strip().str.lower() != "resolved"
    )
    if not unresolved.any():
        return work

    for col in ["basin_id", "basin_area", "area_error", "uparea_merit", "pfaf_code"]:
        if col in work.columns:
            work.loc[unresolved, col] = np.nan
    if "n_upstream_reaches" in work.columns:
        work.loc[unresolved, "n_upstream_reaches"] = pd.NA
    if "method" in work.columns:
        work.loc[unresolved, "method"] = ""
    return work


def _normalize_key_series(series):
    return series.fillna("").astype(str).map(lambda x: x.strip())


def _sample_rows(df, columns, limit=20):
    cols = [c for c in columns if c in df.columns]
    if not cols:
        return "(no sample columns available)"
    return df.loc[:, cols].head(limit).to_string(index=False)


def _require_station_key_unique(df, label):
    if "station_key" not in df.columns:
        raise ValueError(
            "{} missing station_key. Rerun s3, then rerun s4 with S4_RESUME=0 before running s5.".format(
                label
            )
        )
    df["station_key"] = _normalize_key_series(df["station_key"])
    missing = df["station_key"].eq("")
    if missing.any():
        raise ValueError(
            "{} has {} missing station_key values. First rows:\n{}".format(
                label,
                int(missing.sum()),
                _sample_rows(df.loc[missing], ["station_key", "station_id", "path"]),
            )
        )
    dup = df["station_key"].duplicated(keep=False)
    if dup.any():
        raise ValueError(
            "{} has {} duplicate station_key values. First rows:\n{}".format(
                label,
                int(dup.sum()),
                _sample_rows(df.loc[dup], ["station_key", "station_id", "path"]),
            )
        )


def _coerce_station_id(df, label):
    if "station_id" not in df.columns:
        raise ValueError("{} missing station_id".format(label))
    numeric = pd.to_numeric(df["station_id"], errors="coerce")
    invalid = numeric.isna() | (numeric % 1 != 0)
    if invalid.any():
        raise ValueError(
            "{} has {} invalid station_id values. First rows:\n{}".format(
                label,
                int(invalid.sum()),
                _sample_rows(df.loc[invalid], ["station_key", "station_id", "path"]),
            )
        )
    df["station_id"] = numeric.astype("int64")


def validate_station_key_join(s3_df, basin_df):
    """Validate that s3 and s4 basin metadata form a strict one-to-one station_key join."""
    _require_station_key_unique(s3_df, "s3 CSV")
    _require_station_key_unique(basin_df, "s4 basin CSV")
    _coerce_station_id(s3_df, "s3 CSV")
    _coerce_station_id(basin_df, "s4 basin CSV")
    s3_keys = set(s3_df["station_key"].tolist())
    basin_keys = set(basin_df["station_key"].tolist())
    missing = sorted(s3_keys.difference(basin_keys))
    extra = sorted(basin_keys.difference(s3_keys))
    if missing:
        sample = s3_df[s3_df["station_key"].isin(missing[:20])]
        raise ValueError(
            "s4 basin CSV is missing {} s3 station_key values. First rows:\n{}".format(
                len(missing),
                _sample_rows(sample, ["station_key", "station_id", "path"]),
            )
        )
    if extra:
        sample = basin_df[basin_df["station_key"].isin(extra[:20])]
        raise ValueError(
            "s4 basin CSV has {} extra station_key values. First rows:\n{}".format(
                len(extra),
                _sample_rows(sample, ["station_key", "station_id", "lat", "lon"]),
            )
        )
    merged = s3_df[["station_key", "station_id"]].merge(
        basin_df[["station_key", "station_id"]],
        on="station_key",
        suffixes=("_s3", "_s4"),
        validate="one_to_one",
    )
    mismatch = merged["station_id_s3"].astype("int64") != merged["station_id_s4"].astype("int64")
    if mismatch.any():
        raise ValueError(
            "s3/s4 station_id mismatch for {} station_key values. First rows:\n{}".format(
                int(mismatch.sum()),
                merged.loc[mismatch].head(20).to_string(index=False),
            )
        )


def main():
    raw_argv = sys.argv[1:]
    has_distance_override = any(
        a == "--max-station-distance-m" or a.startswith("--max-station-distance-m=")
        for a in raw_argv
    )
    has_rel_error_override = any(
        a == "--max-upstream-rel-error" or a.startswith("--max-upstream-rel-error=")
        for a in raw_argv
    )
    has_area_col_override = any(
        a == "--upstream-area-col" or a.startswith("--upstream-area-col=")
        for a in raw_argv
    )

    ap = argparse.ArgumentParser(
        description="Step s4 basin mode: assign cluster_id to s3 stations using basin tracer results"
    )
    ap.add_argument(
        "--s3-csv",
        default=str(_DEFAULT_S3_CSV),
        help="s3 output CSV (columns: station_key, station_id, path, source, lat, lon, resolution). Default: {}".format(_DEFAULT_S3_CSV),
    )
    ap.add_argument(
        "--basin-csv",
        default=str(_DEFAULT_BASIN_CSV),
        help=(
            "basin tracer output CSV (columns: station_key, station_id, basin_id).\n"
            "station_key must map one-to-one with the s3 CSV; station_id must match the s3 station_id for the same key.\n"
            "Default: {}".format(_DEFAULT_BASIN_CSV)
        ),
    )
    ap.add_argument(
        "--out",
        default=str(_DEFAULT_OUT),
        help="Output station CSV with cluster_id. Default: {}".format(_DEFAULT_OUT),
    )
    ap.add_argument(
        "--report",
        default=str(_DEFAULT_REPORT),
        help="Output cluster summary report CSV. Default: {}".format(_DEFAULT_REPORT),
    )
    ap.add_argument(
        "--max-station-distance-m",
        type=float,
        default=DEFAULT_MAX_STATION_DISTANCE_M,
        help="Maximum distance between any two stations in the same cluster, in meters. Default: {}".format(
            DEFAULT_MAX_STATION_DISTANCE_M
        ),
    )
    ap.add_argument(
        "--max-upstream-rel-error",
        type=float,
        default=DEFAULT_MAX_UPSTREAM_REL_ERROR,
        help="Maximum upstream-area relative error between any two stations in the same cluster. Default: {}".format(
            DEFAULT_MAX_UPSTREAM_REL_ERROR
        ),
    )
    ap.add_argument(
        "--upstream-area-col",
        default=DEFAULT_UPSTREAM_AREA_COL,
        help="Column used for upstream-area relative-error calculation. Default: {}".format(
            DEFAULT_UPSTREAM_AREA_COL
        ),
    )
    args = ap.parse_args()

    s3_path    = Path(args.s3_csv)
    basin_path = Path(args.basin_csv)
    out_path   = Path(args.out)
    report_path = Path(args.report)

    if not s3_path.is_file():
        print("Error: s3 CSV not found: {}".format(s3_path))
        return 1

    df = pd.read_csv(s3_path)
    basin_df = pd.read_csv(basin_path) if basin_path.is_file() else None
    if basin_df is not None:
        try:
            validate_station_key_join(df, basin_df)
        except ValueError as exc:
            print("Error: {}".format(exc))
            return 1
    else:
        try:
            _require_station_key_unique(df, "s3 CSV")
            _coerce_station_id(df, "s3 CSV")
        except ValueError as exc:
            print("Error: {}".format(exc))
            return 1
    print("Loaded s3 stations: {} rows".format(len(df)))

    if not basin_path.is_file():
        print("Error: basin CSV not found: {}".format(basin_path))
        print(
            "  Run the updated s3 first to generate station_key/station_id,\n"
            "  then rerun basin tracer with S4_RESUME=0 to generate the updated s4 output before this script."
        )
        return 1

    station_to_cluster, stats = load_station_to_basin_cluster_map(
        basin_path,
        station_df=df,
        max_station_distance_m=args.max_station_distance_m,
        max_upstream_rel_error=args.max_upstream_rel_error,
        upstream_area_col=args.upstream_area_col,
    )
    print(
        "Merge params: max_station_distance_m={}, max_upstream_rel_error={}, upstream_area_col={}".format(
            args.max_station_distance_m, args.max_upstream_rel_error, args.upstream_area_col
        )
    )
    if not (has_distance_override or has_rel_error_override or has_area_col_override):
        print(
            "Merge params source: built-in defaults "
            "(use --max-station-distance-m/--max-upstream-rel-error/--upstream-area-col to override)"
        )
    else:
        print("Merge params source: CLI override")
    print(
        "Basin map: n_station={}, n_success={}, n_satellite_excluded_from_merge={}, "
        "n_basins={}, n_clusters_from_basins={}, n_remapped={}".format(
            stats["n_station"],
            stats["n_success"],
            stats.get("n_satellite_excluded_from_merge", 0),
            stats["n_basins"],
            stats["n_clusters_from_basins"],
            stats["n_changed"],
        )
    )

    df["cluster_id"] = df["station_id"].map(lambda sid: station_to_cluster.get(sid, sid))

    BASIN_META_COLS = [
        "station_key", "basin_id", "basin_area", "match_quality",
        "area_error", "uparea_merit", "pfaf_code", "method", "n_upstream_reaches",
        "distance_m", "point_in_local", "point_in_basin", "basin_status", "basin_flag",
    ]
    available = [c for c in BASIN_META_COLS if c in basin_df.columns]
    basin_meta = basin_df[available].drop_duplicates(subset=["station_key"])
    df = df.merge(basin_meta, on="station_key", how="left", validate="one_to_one")
    df = _mask_unresolved_basin_fields(df)

    n_clusters = df["cluster_id"].nunique()
    n_multi    = int((df.groupby("cluster_id")["station_id"].count() > 1).sum())
    print(
        "Clusters: total={}, multi-station={}, single-station={}".format(
            n_clusters, n_multi, n_clusters - n_multi
        )
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print("Wrote: {}".format(out_path))

    report_df = _build_cluster_report(df)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(report_path, index=False)
    print("Wrote: {}".format(report_path))

    print("\nDone. Total clusters: {}".format(len(report_df)))
    return 0


if __name__ == "__main__":
    exit(main())
