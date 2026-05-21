#!/usr/bin/env python3
"""Early overlap validation from s4/s5 outputs.

This script is intended to run after s5_basin_merge.py / s5 basin clustering
and before s6/s7/s8.

Built-in behavior, no command-line arguments needed:
  - reads scripts_basin_test/output/s4_upstream_basins.csv
  - reads scripts_basin_test/output/s5_basin_clustered_stations.csv
  - checks daily, monthly, and annual
  - skips climatology by default
  - reads source NetCDF files referenced by the s5 "path" column
  - builds candidate-level overlap rows for each cluster_id + resolution + date
  - reuses s10_validation_results pairwise metric helpers
  - writes early validation CSV/Markdown outputs

Edit the DEFAULT_* constants below if you want to change behavior.
"""

from __future__ import annotations

import math
import time as time_module
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover
    nc4 = None

from pipeline_paths import (
    S2_ORGANIZED_DIR,
    S4_UPSTREAM_CSV,
    S5_BASIN_CLUSTERED_CSV,
    get_output_r_root,
)

from qc_contract import (
    FINAL_Q_FLAG_NAMES,
    FINAL_SSC_FLAG_NAMES,
    FINAL_SSL_FLAG_NAMES,
    Q_VAR_NAMES,
    SSC_VAR_NAMES,
    SSL_VAR_NAMES,
    TIME_VAR_NAMES,
)

from s10_validation_results import (
    VARIABLES,
    aggregate_pair_metrics,
    build_overlap_pair_records,
    classify_source_family,
    compute_pair_metrics,
)

try:
    from s10_validation_results import aggregate_pair_metrics_pooled
except ImportError:  # fallback for older s10 versions
    aggregate_pair_metrics_pooled = None


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
ORGANIZED_ROOT = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()

DEFAULT_S4_CSV = PROJECT_ROOT / S4_UPSTREAM_CSV
DEFAULT_S5_CSV = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_OUT_DIR = PROJECT_ROOT / "scripts_basin_test" / "output" / "early_validation_results"

# ---------------------------------------------------------------------------
# Built-in runtime config
# ---------------------------------------------------------------------------
# Jelly requested no command-line parameters. Change these constants directly
# if you want a different default run.
DEFAULT_RESOLUTIONS = ["daily", "monthly", "annual"]
DEFAULT_MODE = "overlap-only"          # "overlap-only" or "all-candidates"
DEFAULT_MIN_SOURCES = 2
DEFAULT_MAX_GROUPS = None              # set to an int for quick debugging
DEFAULT_INCLUDE_CLIMATOLOGY = False

EARLY_METHOD_NOTES = (
    "early validation from s5_basin_clustered_stations.csv and source NetCDF files; "
    "does not require s6/s7/s8 release products"
)
EARLY_ASSUMPTIONS = (
    "s5 cluster_id + resolution define candidate groups; selected_flag is reconstructed "
    "as the first readable candidate by file-level quality_score for each date"
)

FILL_VALUES = {-9999.0, -9999, 1.0e20}


class Args:
    """Small config object used instead of argparse."""

    pass


def get_args() -> Args:
    """Return built-in runtime config.

    No command-line arguments are read. Edit DEFAULT_* constants near the top of
    this file if defaults need to change.
    """
    args = Args()
    args.s4_csv = str(DEFAULT_S4_CSV)
    args.s5_csv = str(DEFAULT_S5_CSV)
    args.out_dir = str(DEFAULT_OUT_DIR)

    # Built-in validation scope: check all three time resolutions.
    args.resolutions = list(DEFAULT_RESOLUTIONS)

    # Built-in behavior.
    args.mode = DEFAULT_MODE
    args.min_sources = DEFAULT_MIN_SOURCES
    args.max_groups = DEFAULT_MAX_GROUPS
    args.include_climatology = DEFAULT_INCLUDE_CLIMATOLOGY

    return args


def log_progress(message: str) -> None:
    stamp = time_module.strftime("%Y-%m-%d %H:%M:%S")
    print("[{}] {}".format(stamp, message), flush=True)


def _clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, float) and math.isnan(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def _first_existing(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    column_set = {str(col) for col in columns}
    for name in candidates:
        if name in column_set:
            return name
    lower = {str(col).lower(): str(col) for col in columns}
    for name in candidates:
        if name.lower() in lower:
            return lower[name.lower()]
    return None


def _safe_int(value, default: int = -1) -> int:
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _cluster_uid(cluster_id) -> str:
    cid = _safe_int(cluster_id, -1)
    return "SED{:06d}".format(cid) if cid >= 0 else ""


def _resolve_station_path(path_text: str) -> Path:
    """Resolve s5 path values against output_resolution_organized/.

    s3/s5 usually store paths relative to output_resolution_organized.
    Old absolute paths from another machine are handled by extracting the path
    after the output_resolution_organized marker when possible.
    """
    text = _clean_text(path_text)
    path = Path(text)
    if not text:
        return path

    if not path.is_absolute():
        return (ORGANIZED_ROOT / path).resolve()

    if path.is_file():
        return path

    try:
        marker = "output_resolution_organized"
        parts = path.parts
        for i, part in enumerate(parts):
            if part == marker and i + 1 < len(parts):
                rel = Path(*parts[i + 1 :])
                candidate = (ORGANIZED_ROOT / rel).resolve()
                if candidate.is_file():
                    return candidate
    except Exception:
        pass

    return path


def _first_nc_variable(ds, names):
    for name in names:
        if name in ds.variables:
            return name, ds.variables[name]
    return None, None


def _pad_array(values, size: int, fill_value):
    arr = np.asarray(values).reshape(-1)
    if len(arr) >= size:
        return arr[:size]
    return np.concatenate([arr, np.full(size - len(arr), fill_value)])


def _read_candidate_numeric_var(ds, names, size: int) -> Tuple[np.ndarray, str]:
    name, var = _first_nc_variable(ds, names)
    if var is None:
        return np.full(size, np.nan, dtype=np.float64), ""

    arr = np.ma.asarray(var[:]).astype(np.float64).reshape(-1)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype=np.float64)

    arr = _pad_array(arr, size, np.nan).astype(np.float64)
    for fill_value in FILL_VALUES:
        arr[arr == fill_value] = np.nan

    units = _clean_text(getattr(var, "units", ""))
    return arr, units


def _read_candidate_flag_var(ds, names, size: int) -> np.ndarray:
    name, var = _first_nc_variable(ds, names)
    if var is None:
        return np.full(size, 9, dtype=np.int16)

    raw = np.ma.asarray(var[:]).reshape(-1)
    if np.ma.isMaskedArray(raw):
        raw = raw.filled(9)
    raw = _pad_array(raw, size, 9)
    numeric = pd.to_numeric(pd.Series(raw), errors="coerce").fillna(9)
    return numeric.astype(np.int16).to_numpy()


def _decode_candidate_dates(time_var):
    raw = np.asarray(time_var[:]).reshape(-1)
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")

    try:
        decoded = nc4.num2date(
            raw,
            units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
        )
    except TypeError:
        try:
            decoded = nc4.num2date(raw, units, calendar=calendar)
        except Exception:
            decoded = pd.to_datetime(raw, unit="D", origin="1970-01-01")
    except Exception:
        decoded = pd.to_datetime(raw, unit="D", origin="1970-01-01")

    dates = []
    for item in list(decoded):
        try:
            ts = pd.Timestamp(item)
        except Exception:
            if hasattr(item, "isoformat"):
                try:
                    ts = pd.Timestamp(item.isoformat())
                except Exception:
                    ts = pd.Timestamp(item.year, item.month, item.day)
            elif hasattr(item, "year"):
                ts = pd.Timestamp(item.year, item.month, item.day)
            else:
                ts = pd.Timestamp(str(item))
        dates.append(ts.normalize())
    return dates


def read_candidate_series(path: Path):
    """Read one source-station NetCDF into date/Q/SSC/SSL/flags."""
    if nc4 is None:
        return None, {}, "netCDF4 is unavailable"

    try:
        with nc4.Dataset(str(path), "r") as ds:
            time_name, time_var = _first_nc_variable(ds, TIME_VAR_NAMES)
            if time_var is None:
                return None, {}, "time variable not found"

            dates = _decode_candidate_dates(time_var)
            n = len(dates)
            if n == 0:
                return None, {}, "empty time variable"

            q, q_units = _read_candidate_numeric_var(ds, Q_VAR_NAMES, n)
            ssc, ssc_units = _read_candidate_numeric_var(ds, SSC_VAR_NAMES, n)
            ssl, ssl_units = _read_candidate_numeric_var(ds, SSL_VAR_NAMES, n)

            df = pd.DataFrame(
                {
                    "date": [ts.strftime("%Y-%m-%d") for ts in dates],
                    "time": [
                        float((ts - pd.Timestamp("1970-01-01")).days)
                        for ts in dates
                    ],
                    "Q": q,
                    "SSC": ssc,
                    "SSL": ssl,
                    "Q_flag": _read_candidate_flag_var(ds, FINAL_Q_FLAG_NAMES, n),
                    "SSC_flag": _read_candidate_flag_var(ds, FINAL_SSC_FLAG_NAMES, n),
                    "SSL_flag": _read_candidate_flag_var(ds, FINAL_SSL_FLAG_NAMES, n),
                }
            )

            df = df[df[["Q", "SSC", "SSL"]].notna().any(axis=1)].copy()
            units = {"Q": q_units, "SSC": ssc_units, "SSL": ssl_units}

            if df.empty:
                return df, units, "no non-empty Q/SSC/SSL rows"

            df = df.drop_duplicates("date", keep="first").reset_index(drop=True)
            return df, units, ""
    except Exception as exc:
        return None, {}, "cannot read candidate NetCDF: {}".format(exc)


def compute_quality_metrics(df: pd.DataFrame) -> Dict[str, float]:
    total = 0
    good = 0

    for col in ("Q_flag", "SSC_flag", "SSL_flag"):
        if col not in df.columns:
            continue
        flags = pd.to_numeric(df[col], errors="coerce").fillna(9).astype(np.int16)
        valid_mask = (flags >= 0) & (flags <= 3)
        total += int(valid_mask.sum())
        good += int(((flags == 0) & valid_mask).sum())

    data_cols = [col for col in VARIABLES if col in df.columns]
    nonempty_rows = int(df[data_cols].notna().any(axis=1).sum()) if data_cols else 0

    return {
        "quality_score": (good / total) if total > 0 else 0.0,
        "good_flag_count": int(good),
        "valid_flag_count": int(total),
        "n_time_rows": int(len(df)),
        "n_nonempty_rows": int(nonempty_rows),
    }


def _source_station_uid(row: pd.Series) -> str:
    uid_col = _first_existing(
        row.index,
        (
            "source_station_uid",
            "station_uid",
            "source_station_id",
            "native_id",
            "source_station_native_id",
        ),
    )
    if uid_col:
        value = _clean_text(row.get(uid_col, ""))
        if value:
            return value

    source = _clean_text(row.get("source", "source")).replace(" ", "_")
    station = _clean_text(row.get("station_id", ""))
    if not station:
        station = Path(_clean_text(row.get("path", ""))).stem
    return "{}:{}".format(source, station)


def _source_station_index(row: pd.Series, fallback: int) -> int:
    idx_col = _first_existing(row.index, ("source_station_index", "station_id"))
    if idx_col:
        return _safe_int(row.get(idx_col), fallback)
    return fallback


def _candidate_source_family(row: pd.Series) -> str:
    family_col = _first_existing(
        row.index,
        ("source_family", "source_type", "source_category"),
    )
    if family_col:
        value = _clean_text(row.get(family_col, ""))
        if value:
            return value
    return classify_source_family(_clean_text(row.get("source", "")))


def build_s4_basin_summary(s4_csv: Path) -> pd.DataFrame:
    if not s4_csv.is_file():
        return pd.DataFrame(
            [
                {
                    "check_type": "s4_basin_file",
                    "group": "",
                    "n_rows": 0,
                    "notes": "s4 CSV not found: {}".format(s4_csv),
                }
            ]
        )

    df = pd.read_csv(s4_csv, keep_default_na=False)
    rows = [
        {
            "check_type": "s4_basin_file",
            "group": "all",
            "n_rows": int(len(df)),
            "notes": "loaded {}".format(s4_csv.name),
        }
    ]

    for col in ("basin_status", "basin_flag", "match_quality"):
        if col not in df.columns:
            rows.append(
                {
                    "check_type": "s4_missing_column",
                    "group": col,
                    "n_rows": 0,
                    "notes": "column not present in s4 CSV",
                }
            )
            continue

        counts = df[col].fillna("").astype(str).str.strip().value_counts(dropna=False)
        for value, count in counts.items():
            rows.append(
                {
                    "check_type": col,
                    "group": value or "(blank)",
                    "n_rows": int(count),
                    "notes": "",
                }
            )

    return pd.DataFrame(rows)


def _filter_resolutions(stations: pd.DataFrame, resolutions: Sequence[str]) -> pd.DataFrame:
    work = stations.copy()
    work["resolution"] = work["resolution"].fillna("").astype(str).str.strip()

    wanted = [r.strip().lower() for r in resolutions if r.strip()]
    if not wanted or "all" in wanted:
        return work

    return work[work["resolution"].str.lower().isin(wanted)].copy()


def summarize_s5_candidates(stations: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "cluster_id",
        "cluster_uid",
        "resolution",
        "n_candidate_rows",
        "n_sources",
        "n_station_ids",
        "n_existing_paths",
    ]

    if stations.empty:
        return pd.DataFrame(columns=columns)

    work = stations.copy()
    work["cluster_uid"] = work["cluster_id"].map(_cluster_uid)
    work["_path_exists"] = work["path"].map(lambda p: int(_resolve_station_path(p).is_file()))

    agg_spec = {
        "n_candidate_rows": ("path", "count"),
        "n_sources": ("source", "nunique"),
        "n_existing_paths": ("_path_exists", "sum"),
    }
    if "station_id" in work.columns:
        agg_spec["n_station_ids"] = ("station_id", "nunique")
    else:
        agg_spec["n_station_ids"] = ("path", "count")

    grouped = (
        work.groupby(["cluster_id", "cluster_uid", "resolution"], dropna=False)
        .agg(**agg_spec)
        .reset_index()
    )
    return grouped.reindex(columns=columns).sort_values(
        ["n_sources", "n_candidate_rows", "cluster_id", "resolution"],
        ascending=[False, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def build_early_overlap_candidates(
    stations: pd.DataFrame,
    mode: str = "overlap-only",
    min_sources: int = 2,
    max_groups: Optional[int] = None,
) -> Tuple[pd.DataFrame, List[str]]:
    if mode not in {"overlap-only", "all-candidates"}:
        raise ValueError("mode must be overlap-only or all-candidates")

    required = {"path", "source", "cluster_id", "resolution"}
    missing = sorted(required - set(stations.columns))
    if missing:
        raise ValueError("s5 CSV missing required columns: {}".format(", ".join(missing)))

    rows: List[Dict[str, object]] = []
    warnings: List[str] = []

    group_cols = ["cluster_id", "resolution"]
    groups = list(stations.groupby(group_cols, sort=True, dropna=False))
    submitted = 0

    for (cluster_id, resolution), group in groups:
        if max_groups is not None and submitted >= max_groups:
            break

        if mode == "overlap-only" and len(group) < 2:
            continue

        if "source" in group.columns and group["source"].astype(str).nunique() < min_sources:
            continue

        submitted += 1
        cluster_uid = _cluster_uid(cluster_id)

        candidates = []
        for fallback_idx, (_, qrow) in enumerate(group.iterrows()):
            raw_path = _clean_text(qrow.get("path", ""))
            resolved_path = _resolve_station_path(raw_path)

            if not resolved_path.is_file():
                warnings.append("missing candidate file: {}".format(raw_path))
                continue

            series, units, read_note = read_candidate_series(resolved_path)
            if series is None:
                warnings.append("{}: {}".format(raw_path, read_note))
                continue

            if series.empty:
                continue

            metrics = compute_quality_metrics(series)
            candidates.append(
                {
                    "row": qrow,
                    "series": series.set_index("date"),
                    "units": units,
                    "resolved_path": resolved_path,
                    "raw_path": raw_path,
                    "source_station_uid": _source_station_uid(qrow),
                    "source_station_index": _source_station_index(qrow, fallback_idx),
                    "quality_score": float(metrics["quality_score"]),
                    "good_flag_count": int(metrics["good_flag_count"]),
                    "valid_flag_count": int(metrics["valid_flag_count"]),
                    "n_time_rows": int(metrics["n_time_rows"]),
                    "n_nonempty_rows": int(metrics["n_nonempty_rows"]),
                }
            )

        if mode == "overlap-only" and len(candidates) < 2:
            continue

        candidates.sort(
            key=lambda item: (
                -item["quality_score"],
                _clean_text(item["row"].get("source", "")),
                int(item["source_station_index"]),
                _clean_text(item["raw_path"]),
            )
        )

        for rank, item in enumerate(candidates, start=1):
            item["candidate_rank"] = rank

        all_dates = sorted(set().union(*(set(item["series"].index) for item in candidates)))
        for date_text in all_dates:
            present = [item for item in candidates if date_text in item["series"].index]
            if not present:
                continue

            is_overlap = int(len(present) > 1)
            if mode == "overlap-only" and not is_overlap:
                continue

            selected_item = present[0]
            candidate_group_key = "{}|{}|{}".format(cluster_uid, resolution, date_text)

            for item in present:
                qrow = item["row"]
                data_row = item["series"].loc[date_text]
                if isinstance(data_row, pd.DataFrame):
                    data_row = data_row.iloc[0]

                source = _clean_text(qrow.get("source", ""))
                source_family = _candidate_source_family(qrow)

                rows.append(
                    {
                        "cluster_uid": cluster_uid,
                        "cluster_id": int(_safe_int(cluster_id)),
                        "resolution": str(resolution),
                        "time": float(data_row.get("time", np.nan)),
                        "date": str(date_text),
                        "source": source,
                        "source_family": source_family,
                        "source_station_uid": item["source_station_uid"],
                        "source_station_index": int(item["source_station_index"]),
                        "source_station_native_id": _clean_text(
                            qrow.get("source_station_id", qrow.get("native_id", ""))
                        ),
                        "source_station_name": _clean_text(qrow.get("station_name", "")),
                        "source_station_river_name": _clean_text(qrow.get("river_name", "")),
                        "source_station_paths": _clean_text(qrow.get("path", "")),
                        "source_station_lat": _safe_float(qrow.get("lat", np.nan)),
                        "source_station_lon": _safe_float(qrow.get("lon", np.nan)),
                        "candidate_path": _clean_text(item["raw_path"]),
                        "resolved_candidate_path": str(item["resolved_path"]),
                        "candidate_rank": int(item["candidate_rank"]),
                        "candidate_quality_score": float(item["quality_score"]),
                        "good_flag_count": int(item["good_flag_count"]),
                        "valid_flag_count": int(item["valid_flag_count"]),
                        "n_time_rows": int(item["n_time_rows"]),
                        "n_nonempty_rows": int(item["n_nonempty_rows"]),
                        "selected_flag": int(item is selected_item),
                        "is_overlap": is_overlap,
                        "n_candidates_at_time": int(len(present)),
                        "n_ranked_candidates_for_cluster_resolution": int(len(candidates)),
                        "candidate_group_key": candidate_group_key,
                        "selection_reason": (
                            "first readable candidate by quality_score for this date"
                        ),
                        "Q": data_row.get("Q", np.nan),
                        "SSC": data_row.get("SSC", np.nan),
                        "SSL": data_row.get("SSL", np.nan),
                        "Q_flag": int(data_row.get("Q_flag", 9)),
                        "SSC_flag": int(data_row.get("SSC_flag", 9)),
                        "SSL_flag": int(data_row.get("SSL_flag", 9)),
                        "Q_units": item["units"].get("Q", ""),
                        "SSC_units": item["units"].get("SSC", ""),
                        "SSL_units": item["units"].get("SSL", ""),
                        "has_Q": int(pd.notna(data_row.get("Q", np.nan))),
                        "has_SSC": int(pd.notna(data_row.get("SSC", np.nan))),
                        "has_SSL": int(pd.notna(data_row.get("SSL", np.nan))),
                        "method_notes": EARLY_METHOD_NOTES,
                        "assumptions": EARLY_ASSUMPTIONS,
                    }
                )

    columns = [
        "cluster_uid",
        "cluster_id",
        "resolution",
        "time",
        "date",
        "source",
        "source_family",
        "source_station_uid",
        "source_station_index",
        "source_station_native_id",
        "source_station_name",
        "source_station_river_name",
        "source_station_paths",
        "source_station_lat",
        "source_station_lon",
        "candidate_path",
        "resolved_candidate_path",
        "candidate_rank",
        "candidate_quality_score",
        "good_flag_count",
        "valid_flag_count",
        "n_time_rows",
        "n_nonempty_rows",
        "selected_flag",
        "is_overlap",
        "n_candidates_at_time",
        "n_ranked_candidates_for_cluster_resolution",
        "candidate_group_key",
        "selection_reason",
        "Q",
        "SSC",
        "SSL",
        "Q_flag",
        "SSC_flag",
        "SSL_flag",
        "Q_units",
        "SSC_units",
        "SSL_units",
        "has_Q",
        "has_SSC",
        "has_SSL",
        "method_notes",
        "assumptions",
    ]

    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=columns)
    else:
        frame = frame.reindex(columns=columns)
        frame = frame.sort_values(
            [
                "resolution",
                "cluster_uid",
                "time",
                "date",
                "candidate_rank",
                "source_station_uid",
            ],
            kind="mergesort",
        ).reset_index(drop=True)

    return frame, warnings


def summarize_overlap_candidates(candidates: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "resolution",
        "source",
        "source_family",
        "n_candidate_rows",
        "n_selected_rows",
        "n_overlap_rows",
        "n_overlap_groups",
        "n_clusters",
        "method_notes",
        "assumptions",
    ]
    if candidates.empty:
        return pd.DataFrame(columns=columns)

    work = candidates.copy()
    work["_cluster_key"] = work["cluster_uid"].astype(str)
    work.loc[work["_cluster_key"].str.strip().eq(""), "_cluster_key"] = work["cluster_id"].astype(str)

    rows = []
    for keys, group in work.groupby(["resolution", "source", "source_family"], dropna=False):
        overlap_groups = group.loc[
            pd.to_numeric(group["is_overlap"], errors="coerce").fillna(0).astype(int) == 1,
            "candidate_group_key",
        ].nunique()
        rows.append(
            {
                "resolution": keys[0],
                "source": keys[1],
                "source_family": keys[2],
                "n_candidate_rows": int(len(group)),
                "n_selected_rows": int(pd.to_numeric(group["selected_flag"], errors="coerce").fillna(0).sum()),
                "n_overlap_rows": int(pd.to_numeric(group["is_overlap"], errors="coerce").fillna(0).sum()),
                "n_overlap_groups": int(overlap_groups),
                "n_clusters": int(group["_cluster_key"].nunique()),
                "method_notes": EARLY_METHOD_NOTES,
                "assumptions": EARLY_ASSUMPTIONS,
            }
        )

    return pd.DataFrame(rows).reindex(columns=columns)


def pooled_metrics_fallback(pair_records: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "source_pair",
        "source_family_pair",
        "resolution",
        "variable",
        "bias",
        "RMSE",
        "MAE",
        "MAPE",
        "n_valid_mape",
        "Pearson correlation",
        "Spearman rank correlation",
        "n_pairs",
        "n_clusters",
        "time_start",
        "time_end",
        "method_notes",
        "assumptions",
    ]
    if pair_records.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for keys, group in pair_records.groupby(
        ["source_pair", "source_family_pair", "variable"],
        dropna=False,
    ):
        metrics = compute_pair_metrics(group["value_a"], group["value_b"])
        cluster_ids = group["cluster_uid"].astype(str)
        fallback = group["cluster_id"].astype(str)
        cluster_key = cluster_ids.where(cluster_ids.str.strip().ne(""), fallback)
        time_values = group["time"].astype(str)
        rows.append(
            {
                "source_pair": keys[0],
                "source_family_pair": keys[1],
                "resolution": "all",
                "variable": keys[2],
                "bias": metrics["bias"],
                "RMSE": metrics["RMSE"],
                "MAE": metrics["MAE"],
                "MAPE": metrics["MAPE"],
                "n_valid_mape": metrics["n_valid_mape"],
                "Pearson correlation": metrics["Pearson correlation"],
                "Spearman rank correlation": metrics["Spearman rank correlation"],
                "n_pairs": metrics["n_pairs"],
                "n_clusters": int(cluster_key.nunique()),
                "time_start": time_values.min() if len(time_values) else "",
                "time_end": time_values.max() if len(time_values) else "",
                "method_notes": EARLY_METHOD_NOTES,
                "assumptions": EARLY_ASSUMPTIONS,
            }
        )
    return pd.DataFrame(rows).reindex(columns=columns)


def override_notes(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "method_notes" in out.columns:
        out["method_notes"] = EARLY_METHOD_NOTES
    if "assumptions" in out.columns:
        out["assumptions"] = EARLY_ASSUMPTIONS
    return out


def write_summary_md(
    path: Path,
    args,
    s4_summary: pd.DataFrame,
    s5_summary: pd.DataFrame,
    candidates: pd.DataFrame,
    pair_records: pd.DataFrame,
    by_resolution_metrics: pd.DataFrame,
    pooled_metrics: pd.DataFrame,
    warnings: Sequence[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    n_groups = (
        candidates["candidate_group_key"].nunique()
        if "candidate_group_key" in candidates.columns and not candidates.empty
        else 0
    )
    n_clusters = (
        candidates["cluster_uid"].replace("", np.nan).nunique()
        if "cluster_uid" in candidates.columns and not candidates.empty
        else 0
    )

    lines = [
        "# Early validation results from s4/s5",
        "",
        "## Inputs",
        "",
        "- s4 CSV: `{}`".format(args.s4_csv),
        "- s5 CSV: `{}`".format(args.s5_csv),
        "- resolutions: `{}`".format(" ".join(args.resolutions)),
        "- mode: `{}`".format(args.mode),
        "- min_sources: `{}`".format(args.min_sources),
        "",
        "## Method",
        "",
        EARLY_METHOD_NOTES,
        "",
        EARLY_ASSUMPTIONS,
        "",
        "## Output summary",
        "",
        "- s4 summary rows: {:,}".format(len(s4_summary)),
        "- s5 cluster-resolution groups: {:,}".format(len(s5_summary)),
        "- candidate rows: {:,}".format(len(candidates)),
        "- candidate overlap groups: {:,}".format(n_groups),
        "- clusters with candidate rows: {:,}".format(n_clusters),
        "- pair records: {:,}".format(len(pair_records)),
        "- by-resolution metric rows: {:,}".format(len(by_resolution_metrics)),
        "- pooled metric rows: {:,}".format(len(pooled_metrics)),
        "",
    ]

    if warnings:
        lines.extend(
            [
                "## Warnings",
                "",
                "Only the first 50 warnings are shown here; full details are in `early_read_warnings.csv`.",
                "",
            ]
        )
        for warning in warnings[:50]:
            lines.append("- {}".format(warning))
        lines.append("")

    if candidates.empty:
        lines.extend(
            [
                "## Interpretation",
                "",
                "No candidate-level overlap rows were produced. Common causes:",
                "",
                "1. no cluster/resolution group has at least the requested number of sources;",
                "2. source NetCDF paths in s5 are missing or not portable to this machine;",
                "3. candidate files have no non-empty Q/SSC/SSL rows;",
                "4. selected resolutions exclude the overlap groups.",
                "",
            ]
        )
    elif pair_records.empty:
        lines.extend(
            [
                "## Interpretation",
                "",
                "Candidate rows were produced, but no pairwise metric rows were available. "
                "This usually means overlap rows do not contain valid numeric values for both sides of a source pair.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "## Interpretation",
                "",
                "Pairwise source overlap metrics were computed from upstream source NetCDF values before s6/s7/s8.",
                "",
            ]
        )

    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = get_args()

    s4_csv = Path(args.s4_csv)
    s5_csv = Path(args.s5_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if nc4 is None:
        print("Error: netCDF4 is required. Install it first, for example: pip install netCDF4")
        return 1

    log_progress("Writing early validation outputs to {}".format(out_dir))
    log_progress("Built-in resolutions: {}".format(", ".join(args.resolutions)))

    log_progress("Reading s4 basin summary")
    s4_summary = build_s4_basin_summary(s4_csv)
    s4_summary.to_csv(out_dir / "early_s4_basin_status_summary.csv", index=False)

    if not s5_csv.is_file():
        print("Error: s5 CSV not found: {}".format(s5_csv))
        return 1

    log_progress("Reading s5 stations: {}".format(s5_csv))
    stations = pd.read_csv(s5_csv, keep_default_na=False)

    for col in ("path", "source", "cluster_id", "resolution"):
        if col not in stations.columns:
            print("Error: s5 CSV missing required column '{}'".format(col))
            return 1

    stations = _filter_resolutions(stations, args.resolutions)

    if not args.include_climatology:
        res_lower = stations["resolution"].fillna("").astype(str).str.strip().str.lower()
        n_clim = int((res_lower == "climatology").sum())
        if n_clim:
            stations = stations[res_lower != "climatology"].copy()
            log_progress("Filtered {} climatology rows".format(n_clim))

    if stations.empty:
        print("Error: no s5 rows remain after resolution/climatology filtering")
        return 1

    log_progress("Summarizing s5 candidate groups")
    s5_summary = summarize_s5_candidates(stations)
    s5_summary.to_csv(out_dir / "early_s5_cluster_candidate_summary.csv", index=False)

    log_progress("Building early overlap candidate rows from s5/source NetCDF files")
    candidates, warnings = build_early_overlap_candidates(
        stations,
        mode=args.mode,
        min_sources=args.min_sources,
        max_groups=args.max_groups,
    )
    candidates.to_csv(
        out_dir / "early_overlap_candidates.csv.gz",
        index=False,
        compression="gzip",
    )

    warnings_df = pd.DataFrame({"warning": list(warnings)})
    warnings_df.to_csv(out_dir / "early_read_warnings.csv", index=False)

    log_progress("Summarizing candidate rows")
    candidate_summary = summarize_overlap_candidates(candidates)
    candidate_summary.to_csv(out_dir / "early_overlap_candidate_summary.csv", index=False)

    log_progress("Building source-pair records and metrics")
    pair_records = build_overlap_pair_records(candidates)
    pair_records = override_notes(pair_records)
    pair_records.to_csv(out_dir / "early_overlap_pair_records.csv", index=False)

    by_resolution_metrics = aggregate_pair_metrics(pair_records)
    by_resolution_metrics = override_notes(by_resolution_metrics)
    by_resolution_metrics.to_csv(
        out_dir / "early_overlap_source_pairs_by_variable.csv",
        index=False,
    )

    if aggregate_pair_metrics_pooled is not None:
        pooled_metrics = aggregate_pair_metrics_pooled(pair_records)
    else:
        pooled_metrics = pooled_metrics_fallback(pair_records)
    pooled_metrics = override_notes(pooled_metrics)
    pooled_metrics.to_csv(out_dir / "early_overlap_source_pairs.csv", index=False)

    write_summary_md(
        out_dir / "early_validation_summary.md",
        args,
        s4_summary=s4_summary,
        s5_summary=s5_summary,
        candidates=candidates,
        pair_records=pair_records,
        by_resolution_metrics=by_resolution_metrics,
        pooled_metrics=pooled_metrics,
        warnings=warnings,
    )

    log_progress("Done")
    print("")
    print("Wrote:")
    for name in [
        "early_s4_basin_status_summary.csv",
        "early_s5_cluster_candidate_summary.csv",
        "early_overlap_candidates.csv.gz",
        "early_overlap_candidate_summary.csv",
        "early_overlap_pair_records.csv",
        "early_overlap_source_pairs_by_variable.csv",
        "early_overlap_source_pairs.csv",
        "early_read_warnings.csv",
        "early_validation_summary.md",
    ]:
        print("  - {}".format(out_dir / name))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

