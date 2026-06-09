#!/usr/bin/env python3
"""
Diagnose extreme values in the sediment reference release dataset.

Purpose
-------
This script is a follow-up to findings from variable_coverage_and_summary_stats.py.
It investigates:

  1. SSL negative values (SSL < 0) — physically impossible.
  2. SSC extreme high values — whether they are real or erroneous.
  3. SSL formula cross-check — SSL vs Q * SSC * 0.0864 at the release level.

Outputs
-------
  output_other/variable_anomaly_diagnostics/
    tables/
      table_ssl_negative_records_detail.csv
      table_ssl_negative_by_source.csv
      table_ssl_negative_by_cluster.csv
      table_ssc_extreme_level1_detail.csv
      table_ssc_extreme_level2_detail.csv
      table_ssc_extreme_by_source.csv
      table_ssl_formula_anomaly_detail.csv
      table_ssl_formula_anomaly_by_source.csv
    reports/
      variable_anomaly_diagnostic_report.md

Run
---
  cd scripts_basin_test
  python tools/diagnose_extreme_values.py
"""

import argparse
import sys
import math
from pathlib import Path
from datetime import datetime
from collections import OrderedDict

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import (
    RELEASE_MASTER_NC,
    RELEASE_CLIMATOLOGY_NC,
    get_output_r_root,
)

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

PROJECT_ROOT = get_output_r_root(REPO_ROOT)

DEFAULT_MASTER_NC = PROJECT_ROOT / RELEASE_MASTER_NC
DEFAULT_CLIMATOLOGY_NC = PROJECT_ROOT / RELEASE_CLIMATOLOGY_NC

VARIABLES = ("Q", "SSC", "SSL")
FLAG_COLUMNS = {"Q": "Q_flag", "SSC": "SSC_flag", "SSL": "SSL_flag"}
RESOLUTION_ORDER = ("daily", "monthly", "annual", "climatology", "other", "all")
FILL_SENTINELS = (-9999.0, -9999, -127)
SSL_FACTOR = 0.0864


# ── Reusable helpers (adapted from variable_coverage_and_summary_stats.py) ──

def _clean_text(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in ("", "nan", "none") else text


def _percent(numer, denom):
    denom = int(denom)
    if denom <= 0:
        return np.nan
    return 100.0 * float(numer) / float(denom)


def _pad_or_trim(arr, size, fill_value=np.nan):
    arr = np.asarray(arr).reshape(-1)
    if arr.size >= size:
        return arr[:size]
    if arr.size == 0:
        return np.full(size, fill_value)
    return np.concatenate([arr, np.full(size - arr.size, fill_value)])


def _attr_values(var, attr_name):
    raw = getattr(var, attr_name, None)
    if raw is None:
        return []
    arr = np.asarray(raw).reshape(-1)
    return arr.tolist()


def _replace_fill_values(arr, var=None):
    out = np.asarray(arr, dtype=np.float64).reshape(-1).copy()
    out[~np.isfinite(out)] = np.nan
    fill_values = list(FILL_SENTINELS)
    if var is not None:
        for attr_name in ("_FillValue", "missing_value"):
            fill_values.extend(_attr_values(var, attr_name))
    for fill in fill_values:
        try:
            fill_float = float(fill)
        except Exception:
            continue
        if np.isfinite(fill_float):
            out[np.isclose(out, fill_float, rtol=0.0, atol=0.0)] = np.nan
    return out


def _read_float_var(ds, name, size):
    if name not in ds.variables:
        return np.full(size, np.nan, dtype=np.float64), ""
    var = ds.variables[name]
    raw = np.ma.asarray(var[:]).reshape(-1)
    values = raw.astype(np.float64).filled(np.nan)
    values = _pad_or_trim(values, size, fill_value=np.nan)
    values = _replace_fill_values(values, var=var)
    units = _clean_text(getattr(var, "units", ""))
    return values, units


def _read_int_var(ds, name, size, default=-1):
    if name not in ds.variables:
        return np.full(size, default, dtype=np.int64)
    var = ds.variables[name]
    raw = np.ma.asarray(var[:]).reshape(-1)
    values = raw.astype(np.float64).filled(default)
    values = _pad_or_trim(values, size, fill_value=default)
    values = _replace_fill_values(values, var=var)
    values = np.where(np.isfinite(values), values, default)
    return values.astype(np.int64)


def _read_string_var(ds, name, size):
    if name not in ds.variables:
        return [""] * size
    raw = np.asarray(ds.variables[name][:], dtype=object).reshape(-1)
    values = [_clean_text(item) for item in raw]
    if len(values) >= size:
        return values[:size]
    return values + ([""] * (size - len(values)))


def _infer_n_records(ds):
    if "n_records" in ds.dimensions:
        return len(ds.dimensions["n_records"])
    for name in ("resolution", "station_index", "Q", "SSC", "SSL", "time"):
        if name in ds.variables:
            return int(np.asarray(ds.variables[name][:]).reshape(-1).size)
    raise ValueError("Could not infer n_records from NetCDF file")


# ── NC reading ──

def load_data(path, dataset_kind, fallback_resolution=None):
    """Load records from a release NetCDF into a flat dict of numpy arrays."""
    path = Path(path)
    if not path.is_file():
        print(f"Warning: file not found, skipping: {path}", file=sys.stderr)
        return None, None

    result = {"source_file_kind": "", "source_file": "", "resolution": [],
              "station_index": np.asarray([], dtype=np.int64),
              "cluster_id": np.asarray([], dtype=np.int64),
              "cluster_uid": np.asarray([], dtype=object),
              "source": np.asarray([], dtype=object)}
    for v in VARIABLES:
        result[v] = np.asarray([], dtype=np.float64)
    for v in VARIABLES:
        result[FLAG_COLUMNS[v]] = np.asarray([], dtype=np.int64)
    for stage in ("Q_qc1", "SSC_qc1", "SSL_qc1",
                  "Q_qc2", "SSC_qc2", "SSL_qc2",
                  "SSC_qc3", "SSL_qc3"):
        result[stage] = np.asarray([], dtype=np.int64)
    units = {}

    with nc4.Dataset(path, "r") as ds:
        n_records = _infer_n_records(ds)
        result["source_file_kind"] = dataset_kind
        result["source_file"] = str(path)

        # resolution
        if "resolution" in ds.variables:
            res_codes = _read_int_var(ds, "resolution", n_records, default=4)
            # decode resolution
            mapping = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"}
            if "resolution" in ds.variables:
                fm = _clean_text(getattr(ds.variables["resolution"], "flag_meanings", ""))
                if fm:
                    for i, lbl in enumerate(fm.split()):
                        mapping[i] = str(lbl).strip().lower()
            result["resolution"] = [mapping.get(int(c), "other") for c in res_codes]
        else:
            result["resolution"] = [fallback_resolution or dataset_kind or "other"] * n_records

        result["station_index"] = _read_int_var(ds, "station_index", n_records, default=-1)

        # cluster info (master NC only)
        if dataset_kind == "main":
            # cluster_id and cluster_uid are per-station (shape [n_stations]), not per-record.
            # Use station_index to map them.
            n_sta = int(len(ds.dimensions["n_stations"])) if "n_stations" in ds.dimensions else n_records
            sta_cluster_id = _read_int_var(ds, "cluster_id", n_sta, default=-1)
            sta_cluster_uid = _read_string_var(ds, "cluster_uid", n_sta)
            si = result["station_index"]
            result["cluster_id"] = np.array([sta_cluster_id[int(i)] if 0 <= int(i) < len(sta_cluster_id) else -1 for i in si], dtype=np.int64)
            result["cluster_uid"] = [str(sta_cluster_uid[int(i)]) if 0 <= int(i) < len(sta_cluster_uid) else "" for i in si]
            result["source"] = _read_string_var(ds, "source", n_records)
        else:
            result["cluster_id"] = np.full(n_records, -1, dtype=np.int64)
            result["cluster_uid"] = [""] * n_records
            result["source"] = [dataset_kind] * n_records

        # variable values
        for var_name in VARIABLES:
            values, unit = _read_float_var(ds, var_name, n_records)
            result[var_name] = values
            units[var_name] = unit

        # final flags
        for var_name in VARIABLES:
            flag_name = FLAG_COLUMNS[var_name]
            result[flag_name] = _read_int_var(ds, flag_name, n_records, default=9)

        # stage flags (qc1, qc2, qc3) — try to read, fill with 9 if missing
        for stage in ("Q_qc1", "SSC_qc1", "SSL_qc1",
                      "Q_qc2", "SSC_qc2", "SSL_qc2",
                      "SSC_qc3", "SSL_qc3"):
            result[stage] = _read_int_var(ds, stage, n_records, default=9)

    return result, units


def resolve_cluster_ids(ds_main, station_index):
    """Map station_index -> cluster_id using the per-station cluster_id array."""
    if "cluster_id" in ds_main.variables and "n_stations" in ds_main.dimensions:
        n_sta = len(ds_main.dimensions["n_stations"])
        cid_arr = np.asarray(ds_main.variables["cluster_id"][:]).reshape(-1)
        cupid_arr = _read_string_var(ds_main, "cluster_uid", n_sta)
        return cid_arr, cupid_arr
    return None, None


# ── Diagnostic 1: SSL negative values ──

def diagnose_ssl_negative(data, units, output_dir):
    print("\n" + "=" * 72)
    print("DIAGNOSIS 1: SSL Negative Values")
    print("=" * 72)

    ssl = data["SSL"]
    mask_neg = (ssl < 0) & np.isfinite(ssl)
    n_neg = int(mask_neg.sum())
    n_total = int(np.isfinite(ssl).sum())
    print(f"Total finite SSL records: {n_total:,}")
    print(f"SSL < 0 records:          {n_neg:,}  ({_percent(n_neg, n_total):.2f}% of finite)")

    if n_neg == 0:
        print("No negative SSL values found. Skipping.")
        return

    # Build detail DataFrame
    records = _build_detail_records(data, mask_neg,
                                     extra_cols=["SSL", "Q", "SSC",
                                                  "SSL_flag", "SSL_qc1", "SSL_qc2", "SSL_qc3",
                                                  "Q_flag", "SSC_flag",
                                                  "Q", "SSC", "source", "cluster_id", "cluster_uid",
                                                  "station_index", "resolution"])
    # Add calculated SSL
    q_vals = records["Q"].to_numpy(dtype=float)
    ssc_vals = records["SSC"].to_numpy(dtype=float)
    calc = q_vals * ssc_vals * SSL_FACTOR
    records["SSL_calc_from_QSSC"] = np.where(np.isfinite(q_vals) & np.isfinite(ssc_vals) & (q_vals > 0) & (ssc_vals > 0), calc, np.nan)
    records["ratio_if_valid"] = np.where(np.isfinite(records["SSL_calc_from_QSSC"]) & (records["SSL_calc_from_QSSC"] > 0),
                                          records["SSL"] / records["SSL_calc_from_QSSC"], np.nan)

    # Flag status summary
    print("\n--- QC Flag Distribution of Negative SSL Records ---")
    for flag_name in ["SSL_flag", "SSL_qc1"]:
        counts = records[flag_name].value_counts().sort_index()
        print(f"  {flag_name}:")
        for val, cnt in counts.items():
            print(f"    flag={val}: {cnt:,} records ({_percent(cnt, n_neg):.1f}%)")

    # By source
    print("\n--- By Source Dataset ---")
    src_gb = records.groupby("source")
    src_summary = src_gb.size().to_frame(name="n_neg_ssl")
    src_summary["pct_of_neg"] = src_summary["n_neg_ssl"] / n_neg * 100
    # also get total SSL per source
    src_total = _count_by_source(data, "SSL", finite_only=True)
    src_summary = src_summary.join(src_total, how="left")
    src_summary["pct_of_source_ssl"] = src_summary["n_neg_ssl"] / src_summary["n_total_ssl"] * 100
    src_summary = src_summary.sort_values("n_neg_ssl", ascending=False)
    for idx, row in src_summary.head(20).iterrows():
        print(f"  {idx:<25s}  {int(row['n_neg_ssl']):>8,}  ({row['pct_of_neg']:.1f}%)  "
              f"of source SSL: {row['pct_of_source_ssl']:.1f}%")
    if len(src_summary) > 20:
        print(f"  ... and {len(src_summary)-20} more sources")

    # By cluster (top 20)
    print("\n--- Top 20 Clusters by Negative SSL Count ---")
    if "cluster_uid" in records.columns and records["cluster_uid"].astype(str).str.strip().str.len().sum() > 0:
        clust_gb = records.groupby("cluster_uid")
        clust_summary = clust_gb.size().to_frame(name="n_neg_ssl").sort_values("n_neg_ssl", ascending=False)
        for idx, row in clust_summary.head(20).iterrows():
            print(f"  {str(idx):<20s}  {int(row['n_neg_ssl']):>8,}")
    else:
        print("  (no cluster info available for negative SSL records)")

    # By resolution
    print("\n--- By Resolution ---")
    res_gb = records.groupby("resolution")
    for res_name in RESOLUTION_ORDER:
        if res_name in res_gb.groups:
            cnt = len(res_gb.get_group(res_name))
            print(f"  {res_name:<15s}  {cnt:>8,}")

    # Save detail CSV
    detail_path = output_dir / "tables" / "table_ssl_negative_records_detail.csv"
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    out_cols = ["source", "resolution", "cluster_uid", "cluster_id", "station_index",
                "SSL", "Q", "SSC", "SSL_calc_from_QSSC", "ratio_if_valid",
                "SSL_flag", "SSL_qc1", "SSL_qc2", "SSL_qc3", "Q_flag", "SSC_flag",
            ]
    records[out_cols].to_csv(detail_path, index=False)
    print(f"\nDetail written: {detail_path}")

    # Save by-source
    src_out_path = output_dir / "tables" / "table_ssl_negative_by_source.csv"
    src_summary.to_csv(src_out_path)
    print(f"By source: {src_out_path}")

    # Save by-cluster
    try:
        if "cluster_uid" in records.columns and clust_summary is not None:
            clust_out_path = output_dir / "tables" / "table_ssl_negative_by_cluster.csv"
            clust_summary.to_csv(clust_out_path)
            print(f"By cluster: {clust_out_path}")
    except (NameError, UnboundLocalError):
        pass

    return records


# ── Diagnostic 2: SSC extreme high values ──

def diagnose_ssc_extreme(data, units, output_dir, level1_threshold=100000, level2_threshold=39100):
    print("\n" + "=" * 72)
    print("DIAGNOSIS 2: SSC Extreme High Values")
    print("=" * 72)

    ssc = data["SSC"]
    mask_valid = np.isfinite(ssc)
    n_valid = int(mask_valid.sum())
    print(f"Total finite SSC records: {n_valid:,}")

    # Level 1: > level1_threshold
    mask_l1 = mask_valid & (ssc > level1_threshold)
    n_l1 = int(mask_l1.sum())
    print(f"\n--- Level 1: SSC > {level1_threshold:,} mg/L ---")
    print(f"Records: {n_l1:,}  ({_percent(n_l1, n_valid):.4f}% of finite)")

    if n_l1 > 0:
        l1_records = _build_detail_records(data, mask_l1,
                                            extra_cols=["SSC", "Q", "SSL",
                                                         "SSC_flag", "SSC_qc1", "SSC_qc2", "SSC_qc3",
                                                         "Q_flag", "SSL_flag",
                                                         "source", "cluster_uid", "cluster_id",
                                                         "station_index", "resolution"])
        _print_source_summary(l1_records, n_l1, "SSC")
        _print_cluster_summary(l1_records, n_l1, "SSC")

        detail_path = output_dir / "tables" / "table_ssc_extreme_level1_detail.csv"
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        out_cols = ["source", "resolution", "cluster_uid", "cluster_id", "station_index",
                    "SSC", "Q", "SSL",
                    "SSC_flag", "SSC_qc1", "SSC_qc2", "SSC_qc3", "Q_flag", "SSL_flag",
            ]
        l1_records[out_cols].to_csv(detail_path, index=False)
        print(f"Level 1 detail: {detail_path}")

    # Level 2: between level2_threshold and level1_threshold
    mask_l2 = mask_valid & (ssc > level2_threshold) & (ssc <= level1_threshold)
    n_l2 = int(mask_l2.sum())
    print(f"\n--- Level 2: SSC {level2_threshold:,} ~ {level1_threshold:,} mg/L ---")
    print(f"Records: {n_l2:,}  ({_percent(n_l2, n_valid):.2f}% of finite)")

    if n_l2 > 0:
        l2_records = _build_detail_records(data, mask_l2,
                                            extra_cols=["SSC", "Q", "SSL",
                                                         "SSC_flag", "SSC_qc1", "SSC_qc2", "SSC_qc3",
                                                         "source", "cluster_uid", "cluster_id",
                                                         "station_index", "resolution"])
        _print_source_summary(l2_records, n_l2, "SSC")
        _print_cluster_summary(l2_records, n_l2, "SSC")

        detail_path = output_dir / "tables" / "table_ssc_extreme_level2_detail.csv"
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        out_cols = ["source", "resolution", "cluster_uid", "cluster_id", "station_index",
                    "SSC", "Q", "SSL",
                    "SSC_flag", "SSC_qc1", "SSC_qc2", "SSC_qc3",
            ]
        l2_records[out_cols].to_csv(detail_path, index=False)
        print(f"Level 2 detail: {detail_path}")

    # Combined by-source summary (all extreme: > level2_threshold)
    mask_all_extreme = mask_valid & (ssc > level2_threshold)
    if mask_all_extreme.any():
        ext_records = _build_detail_records(data, mask_all_extreme,
                                             extra_cols=["SSC", "source", "resolution",
                                                          "cluster_uid", "cluster_id",
                                                          "SSC_flag", "SSC_qc1", "SSC_qc2", "SSC_qc3"])
        src_all = _make_source_summary(ext_records, "SSC")
        src_out_path = output_dir / "tables" / "table_ssc_extreme_by_source.csv"
        src_all.to_csv(src_out_path)
        print(f"\nBy-source summary (all SSC > {level2_threshold:,}): {src_out_path}")

    return {"n_level1": n_l1, "n_level2": n_l2}


# ── Diagnostic 3: SSL formula cross-check ──

def diagnose_ssl_formula(data, units, output_dir, ratio_tolerance=0.15):
    print("\n" + "=" * 72)
    print("DIAGNOSIS 3: SSL Formula Cross-Check (release level)")
    print("=" * 72)

    q = data["Q"]
    ssc = data["SSC"]
    ssl = data["SSL"]

    valid = np.isfinite(q) & np.isfinite(ssc) & np.isfinite(ssl) & (q > 0) & (ssc > 0) & (ssl > 0)
    n_valid = int(valid.sum())
    print(f"Records with Q>0, SSC>0, SSL>0: {n_valid:,}")

    if n_valid == 0:
        print("No valid formula records. Skipping.")
        return

    calc = q * ssc * SSL_FACTOR
    ratio = np.full(len(valid), np.nan, dtype=np.float64)
    ratio[valid] = ssl[valid] / calc[valid]

    # Summarize ratio distribution
    rv = ratio[valid]
    print(f"\nRatio = SSL / (Q * SSC * {SSL_FACTOR}) distribution:")
    print(f"  Median: {np.nanmedian(rv):.4f}")
    print(f"  P05:    {np.nanpercentile(rv, 5):.4f}")
    print(f"  P95:    {np.nanpercentile(rv, 95):.4f}")
    print(f"  P01:    {np.nanpercentile(rv, 1):.4f}")
    print(f"  P99:    {np.nanpercentile(rv, 99):.4f}")
    print(f"  Min:    {np.nanmin(rv):.4f}")
    print(f"  Max:    {np.nanmax(rv):.4f}")

    # Find anomalies: ratio outside [1-ratio_tolerance, 1+ratio_tolerance]
    mask_good = np.abs(rv - 1.0) <= ratio_tolerance
    n_good = int(mask_good.sum())
    n_anomaly = n_valid - n_good
    print(f"\n  Ratio within {ratio_tolerance} of 1.0: {n_good:,} ({_percent(n_good, n_valid):.1f}%)")
    print(f"  Ratio anomalous:              {n_anomaly:,} ({_percent(n_anomaly, n_valid):.1f}%)")

    # Classify anomalies by factor
    suspect_factors = [
        (10.0, "ssl_10x_high", "possible 0.864 factor instead of 0.0864"),
        (0.1, "ssl_10x_low", "possible extra divide by 10"),
        (1000.0, "ssl_1000x_high", "possible g/L or kg/m3 treated as mg/L"),
        (0.001, "ssl_1000x_low", "possible extra divide/multiply by 1000"),
    ]
    if n_anomaly > 0:
        mask_all = np.full(n_valid, True)
        detail_rows = []
        from collections import defaultdict
        factor_counts = defaultdict(int)
        factor_details = defaultdict(list)

        for factor, label, reason in suspect_factors:
            mask = np.abs(rv / factor - 1.0) <= ratio_tolerance
            count = int(mask.sum())
            factor_counts[label] = count
            if count > 0:
                indices = np.where(mask)[0]
                for idx in indices[:1000]:  # cap per type
                    orig_idx = np.where(valid)[0][idx]
                    factor_details[label].append({
                        "ratio": rv[idx], "factor": factor,
                        "Q": q[orig_idx], "SSC": ssc[orig_idx],
                        "SSL": ssl[orig_idx], "SSL_calc": calc[orig_idx],
                    })

        for label, cnt in sorted(factor_counts.items()):
            if cnt > 0:
                print(f"  {label:<25s}: {cnt:>8,} records")

        # Build detail for any anomalous record
        mask_anomaly_in_valid = ~mask_good
        anomaly_indices = np.where(mask_anomaly_in_valid)[0]
        print(f"\n  Saving up to 10,000 anomaly detail rows...")

        # Map back to original data indices
        valid_indices = np.where(valid)[0]
        detail_df_rows = []
        for i in anomaly_indices[:10000]:
            orig_i = valid_indices[i]
            r = ratio[orig_i]
            # classify
            anomaly_type = "other"
            anomaly_factor = 0
            for factor, label, _ in suspect_factors:
                if np.abs(r / factor - 1.0) <= ratio_tolerance:
                    anomaly_type = label
                    anomaly_factor = factor
                    break
            detail_df_rows.append({
                "ratio": r,
                "anomaly_type": anomaly_type,
                "anomaly_factor": anomaly_factor,
                "Q": q[orig_i],
                "SSC": ssc[orig_i],
                "SSL": ssl[orig_i],
                "SSL_calc": calc[orig_i],
                "source": str(data["source"][orig_i]) if orig_i < len(data["source"]) else "",
                "resolution": data["resolution"][orig_i] if len(data["resolution"]) > orig_i else "",
                "cluster_uid": str(data["cluster_uid"][orig_i]) if orig_i < len(data["cluster_uid"]) else "",
            })

        if detail_df_rows:
            detail_df = pd.DataFrame(detail_df_rows)
            detail_path = output_dir / "tables" / "table_ssl_formula_anomaly_detail.csv"
            detail_path.parent.mkdir(parents=True, exist_ok=True)
            detail_df.to_csv(detail_path, index=False)
            print(f"Anomaly detail: {detail_path}")

            # By-source summary
            src_gb = detail_df.groupby("source").agg(
                n_anomaly_records=("ratio", "count"),
                mean_ratio=("ratio", "mean"),
            ).sort_values("n_anomaly_records", ascending=False)
            src_out_path = output_dir / "tables" / "table_ssl_formula_anomaly_by_source.csv"
            src_gb.to_csv(src_out_path)
            print(f"Anomaly by-source: {src_out_path}")

    return {"n_valid": n_valid, "n_anomaly": n_anomaly}


# ── Helper functions ──

def _build_detail_records(data, mask, extra_cols):
    """Build a DataFrame from data dict subsetted by mask."""
    n = int(mask.sum())
    rows = {}
    for col in extra_cols:
        arr = data.get(col)
        if arr is None or isinstance(arr, str):
            continue
        if isinstance(arr, list):
            vals = [arr[i] for i in range(len(arr)) if mask[i]]
            rows[col] = vals
        elif isinstance(arr, np.ndarray) and arr.dtype.kind in ("U", "O"):
            vals = arr[mask]
            rows[col] = vals
        elif isinstance(arr, np.ndarray):
            vals = arr[mask]
            rows[col] = vals
    return pd.DataFrame(rows, index=range(n))


def _count_by_source(data, var_name, finite_only=True):
    """Count total finite records per source for a variable."""
    vals = data[var_name]
    sources_np = np.array(data["source"], dtype=str)
    mask = np.isfinite(vals) if finite_only else np.ones(len(vals), dtype=bool)
    result = {}
    unique = sorted(set(str(s) for s in sources_np if str(s).strip()))
    for src in unique:
        src_mask = (sources_np == src) & mask
        result[src] = int(src_mask.sum())
    s = pd.Series(result, name="n_total_ssl")
    return s


def _print_source_summary(records_df, n_total, var_name):
    """Print top sources by count."""
    print(f"\n  --- Top Sources by Count ---")
    src_gb = records_df.groupby("source").size().sort_values(ascending=False)
    for idx, cnt in src_gb.head(15).items():
        print(f"    {str(idx):<25s}  {cnt:>8,}  ({_percent(cnt, n_total):.1f}%)")


def _print_cluster_summary(records_df, n_total, var_name):
    """Print top clusters by count."""
    if "cluster_uid" in records_df.columns and records_df["cluster_uid"].astype(str).str.strip().str.len().sum() > 0:
        print(f"\n  --- Top 15 Clusters by Count ---")
        clust_gb = records_df.groupby("cluster_uid").size().sort_values(ascending=False)
        for idx, cnt in clust_gb.head(15).items():
            print(f"    {str(idx):<20s}  {cnt:>8,}  ({_percent(cnt, n_total):.1f}%)")


def _make_source_summary(records_df, var_name):
    """Build by-source summary DataFrame."""
    src_gb = records_df.groupby("source")
    summary = src_gb.agg(
        n_records=(var_name, "count"),
    ).sort_values("n_records", ascending=False)
    return summary


# ── Report generation ──

def generate_report(ssl_result, ssc_result, formula_result, output_dir):
    report_path = output_dir / "reports" / "variable_anomaly_diagnostic_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Variable Anomaly Diagnostic Report",
        "",
        f"Generated by: `diagnose_extreme_values.py`",
        f"Date: {datetime.now().strftime('%Y-%m-%d')}",
        "",
        "## Overview",
        "",
        "This report follows up on findings from `variable_coverage_and_summary_stats.py`, "
        "specifically the **negative SSL mean** (-1,090,000 ton day-1) and the **extreme SSC mean** "
        "(13,000,000 mg L-1).",
        "",
        "---",
        "",
        "## 1. SSL Negative Values",
        "",
    ]

    if ssl_result is not None:
        n_neg = len(ssl_result)
        n_total = int(np.isfinite(ssl_result["SSL"]).sum()) if "SSL" in ssl_result.columns else 0
        pct = _percent(n_neg, n_total)
        lines += [
            f"- **Total negative SSL records**: {n_neg:,} ({pct:.2f}% of finite SSL records)",
            "",
            "### QC Flag Distribution",
            "",
        ]
        for flag_name in ["SSL_flag", "SSL_qc1"]:
            counts = ssl_result[flag_name].value_counts().sort_index()
            flag_lines = [f"| {flag_name} | Count | % |"]
            flag_lines.append(f"|{'---|'*3}")
            for val, cnt in counts.items():
                flag_lines.append(f"| {int(val)} | {cnt:,} | {_percent(cnt, n_neg):.1f}% |")
            lines += flag_lines + [""]

        lines.append("### Top Sources")
        src_gb = ssl_result.groupby("source").size().sort_values(ascending=False)
        lines.append(f"| Source | Negative SSL | % of Total |")
        lines.append(f"|{'---|'*3}")
        for src, cnt in src_gb.head(10).items():
            lines.append(f"| {src} | {cnt:,} | {_percent(cnt, n_neg):.1f}% |")
        lines.append("")

        if "cluster_uid" in ssl_result.columns:
            clust_gb = ssl_result.groupby("cluster_uid").size().sort_values(ascending=False).head(10)
            lines.append("### Top Clusters")
            lines.append(f"| Cluster | Negative SSL |")
            lines.append(f"|{'---|'*2}")
            for cid, cnt in clust_gb.items():
                lines.append(f"| {cid} | {cnt:,} |")
            lines.append("")
    else:
        lines.append("No negative SSL values found.\n")

    lines += [
        "---",
        "",
        "## 2. SSC Extreme High Values",
        "",
    ]

    if ssc_result:
        lines += [
            f"- **Level 1 (SSC > 100,000 mg/L)**: {ssc_result.get('n_level1', 0):,} records",
            f"- **Level 2 (SSC > 39,100 mg/L)**: {ssc_result.get('n_level2', 0):,} records",
            "",
            "See CSV tables for detailed breakdown by source and cluster.",
            "",
        ]

    lines += [
        "---",
        "",
        "## 3. SSL Formula Cross-Check",
        "",
    ]

    if formula_result:
        lines += [
            f"- **Valid records with Q>0, SSC>0, SSL>0**: {formula_result.get('n_valid', 0):,}",
            f"- **Anomalous ratio records**: {formula_result.get('n_anomaly', 0):,}",
            "",
            "See CSV tables for detailed anomaly records.",
            "",
        ]

    lines += [
        "---",
        "",
        "## Recommendations",
        "",
        "- Review negative SSL records to determine root cause (fill value issue vs data error).",
        "- For SSC extreme values, validate against known high-sediment rivers (e.g., Huanghe).",
        "- Consider whether to exclude non-physical records from summary statistics.",
        "",
    ]

    report_text = "\n".join(lines)
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\nReport written: {report_path}")
    return report_path


# ── Main ──

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Diagnose extreme values (SSL negative, SSC extreme, SSL formula) in release NC."
    )
    parser.add_argument("--master-nc", default=str(DEFAULT_MASTER_NC))
    parser.add_argument("--climatology-nc", default=str(DEFAULT_CLIMATOLOGY_NC))
    parser.add_argument("--no-climatology", action="store_true")
    parser.add_argument("--output-dir", default=str(
        PROJECT_ROOT / "scripts_basin_test/output_other/variable_anomaly_diagnostics"
    ))
    parser.add_argument("--ssl-negative", action="store_true", default=True, help="Run SSL negative value diagnosis")
    parser.add_argument("--ssc-extreme", action="store_true", default=True, help="Run SSC extreme value diagnosis")
    parser.add_argument("--ssl-formula", action="store_true", default=True, help="Run SSL formula cross-check")
    parser.add_argument("--ssc-level1-threshold", type=float, default=100000.0)
    parser.add_argument("--ssc-level2-threshold", type=float, default=39100.0)
    parser.add_argument("--ratio-tolerance", type=float, default=0.15)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    if nc4 is None:
        print("Error: netCDF4 is required. Install with: pip install netCDF4", file=sys.stderr)
        return 1

    master_nc = Path(args.master_nc)
    climatology_nc = Path(args.climatology_nc)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not master_nc.is_file():
        print(f"Error: master NetCDF not found: {master_nc}", file=sys.stderr)
        return 1

    # Load master
    print(f"Loading master: {master_nc}")
    master_data, master_units = load_data(master_nc, dataset_kind="main")
    if master_data is None:
        print("Error: failed to load master NC", file=sys.stderr)
        return 1

    # Optionally load climatology
    if not args.no_climatology and climatology_nc.is_file():
        print(f"Loading climatology: {climatology_nc}")
        clim_data, clim_units = load_data(climatology_nc, dataset_kind="climatology",
                                          fallback_resolution="climatology")
        if clim_data is not None:
            # Merge: concat arrays (all object/string arrays are lists; numeric are ndarray)
            merged = {}
            for key in master_data:
                if isinstance(master_data[key], str):
                    merged[key] = master_data[key]
                elif isinstance(master_data[key], list):
                    merged[key] = master_data[key] + clim_data[key]
                elif isinstance(master_data[key], np.ndarray):
                    merged[key] = np.concatenate([master_data[key], clim_data[key]])
                else:
                    merged[key] = master_data[key]
            data = merged
            # Merge units
            units = dict(master_units or {})
            for k, v in (clim_units or {}).items():
                if v and v not in units.get(k, ""):
                    units[k] = " | ".join(filter(None, [units.get(k, ""), v]))
        else:
            data = master_data
            units = master_units
    else:
        data = master_data
        units = master_units

    n_rec = len(data.get("Q", []))
    print(f"Total records loaded: {n_rec:,}")

    # Run diagnostics
    ssl_result = None
    ssc_result = None
    formula_result = None

    if args.ssl_negative:
        ssl_result = diagnose_ssl_negative(data, units, output_dir)

    if args.ssc_extreme:
        ssc_result = diagnose_ssc_extreme(data, units, output_dir,
                                           level1_threshold=args.ssc_level1_threshold,
                                           level2_threshold=args.ssc_level2_threshold)

    if args.ssl_formula:
        formula_result = diagnose_ssl_formula(data, units, output_dir,
                                               ratio_tolerance=args.ratio_tolerance)

    # Report
    report_path = generate_report(ssl_result, ssc_result, formula_result, output_dir)

    print("\n" + "=" * 72)
    print("ALL DIAGNOSTICS COMPLETE")
    print(f"Output directory: {output_dir}")
    print(f"Report: {report_path}")
    print("=" * 72)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
