#!/usr/bin/env python3
"""
Standalone SSC validation: satellite vs in-situ.

Reads sed_reference_satellite.nc and sed_reference_master.nc directly,
bypassing the s8→s11 pipeline filtering, to extract SSC pairs and
produce aggregate validation metrics and plots.

Usage:
  cd /share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test
  LD_PRELOAD=/share/home/dq134/.conda/envs/wzx/lib/libstdc++.so.6 \\
    /share/home/dq134/.conda/envs/wzx/bin/python3 validate/validate_ssc_standalone.py
"""

import argparse
import os
import sys
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# matplotlib with Agg backend (headless)
# ---------------------------------------------------------------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parent  # scripts_basin_test
DEFAULT_RELEASE_DIR = BASE_DIR / "output" / "sed_reference_release"
DEFAULT_OUT_DIR = BASE_DIR / "output" / "validation_ssc_results"

WINDOW_DAYS = {"exact": 0, "pm1d": 1, "pm2d": 2}

# ---------------------------------------------------------------------------
# Helper: extract float array from NC variable (handling masked arrays)
# ---------------------------------------------------------------------------
def nc_float(var):
    """Read NC variable as float64 ndarray (masked -> NaN)."""
    data = var[:]
    if isinstance(data, np.ma.MaskedArray):
        return np.ma.filled(data, np.nan).astype(np.float64)
    return data.astype(np.float64)


def nc_int(var):
    """Read NC variable as int32 ndarray (masked -> 0, but masks rare for ints)."""
    data = var[:]
    if isinstance(data, np.ma.MaskedArray):
        data = np.ma.filled(data, -1)
    return np.asarray(data).astype(np.int32)


def nc_str(var):
    """Read NC string/char variable as list of str."""
    data = var[:]
    if isinstance(data, np.ma.MaskedArray):
        data = data.filled(b"")
    out = []
    for v in data:
        if isinstance(v, bytes):
            out.append(v.decode("utf-8", errors="replace"))
        else:
            out.append(str(v))
    return np.array(out, dtype=object)


# ===================================================================
# 1. Load satellite SSC records (linked stations only)
# ===================================================================
def load_satellite_ssc(satellite_nc_path):
    """
    Load satellite SSC records for linked stations.

    Returns DataFrame with columns:
      sat_station_index, time, SSC_sat, linked_cluster_uid, source, lat, lon,
      station_resolution, sat_station_uid
    """
    print(f"  Opening: {satellite_nc_path}")
    ds = nc.Dataset(satellite_nc_path, "r")

    # Station-level data
    link_status = nc_str(ds["link_status"])
    linked_mask = link_status == "linked"
    linked_indices = np.where(linked_mask)[0]
    print(f"  Linked stations: {linked_mask.sum()} / {len(link_status)}")

    if linked_mask.sum() == 0:
        ds.close()
        return pd.DataFrame()

    # Station info for linked stations
    linked_cluster_uid = nc_str(ds["linked_cluster_uid"])[linked_mask]
    source = nc_str(ds["source"])[linked_mask]
    lat = nc_float(ds["lat"])[linked_mask]
    lon = nc_float(ds["lon"])[linked_mask]
    station_resolution = nc_str(ds["station_resolution"])[linked_mask]
    sat_station_uid = nc_str(ds["satellite_station_uid"])[linked_mask]

    print(f"  Unique linked clusters: {len(np.unique(linked_cluster_uid))}")
    src_counts = dict(zip(*np.unique(source, return_counts=True)))
    print(f"  Sources: {src_counts}")

    # Record-level data
    sat_station_index = nc_int(ds["satellite_station_index"])
    time = nc_float(ds["time"])
    ssc = nc_float(ds["SSC"])

    # Map original station index -> position in linked array
    orig_to_linked = {int(idx): pos for pos, idx in enumerate(linked_indices)}

    # Find records at linked stations
    linked_set = set(int(x) for x in linked_indices)
    record_is_linked = np.array([int(x) in linked_set for x in sat_station_index])

    # Filter
    linked_si = sat_station_index[record_is_linked]
    linked_time = time[record_is_linked]
    linked_ssc = ssc[record_is_linked]

    # Filter to SSC non-null
    ssc_ok = ~np.isnan(linked_ssc)
    linked_si = linked_si[ssc_ok]
    linked_time = linked_time[ssc_ok]
    linked_ssc = linked_ssc[ssc_ok]

    print(f"  Records at linked stations: {record_is_linked.sum()}")
    print(f"  SSC non-null at linked: {len(linked_ssc)} ({100*len(linked_ssc)/max(1,record_is_linked.sum()):.2f}%)")

    if len(linked_ssc) == 0:
        ds.close()
        return pd.DataFrame()

    # Build DataFrame
    rows = []
    for i in range(len(linked_ssc)):
        si = int(linked_si[i])
        li = orig_to_linked[si]
        cu = linked_cluster_uid[li]
        if cu == "" or cu == "0":
            continue  # Skip records linked to empty cluster
        rows.append({
            "sat_station_index": si,
            "time": linked_time[i],
            "SSC_sat": linked_ssc[i],
            "linked_cluster_uid": cu,
            "source": source[li],
            "lat": lat[li],
            "lon": lon[li],
            "station_resolution": station_resolution[li],
            "sat_station_uid": sat_station_uid[li],
        })

    df_sat = pd.DataFrame(rows)
    print(f"  Satellite SSC DataFrame: {len(df_sat)} rows")
    print(f"  Unique clusters: {df_sat['linked_cluster_uid'].nunique()}")
    print(f"  Sources in SSC records: {dict(df_sat['source'].value_counts())}")
    ds.close()
    return df_sat


# ===================================================================
# 2. Load master matrix SSC
# ===================================================================
def load_master_ssc(master_nc_path):
    """
    Load master matrix SSC records.

    Returns:
      cluster_to_indices: dict {cluster_uid: [station_indices]}
      station_sorted: DataFrame with columns [cluster_uid, time, SSC] sorted by time
    """
    print(f"  Opening: {master_nc_path}")
    ds = nc.Dataset(master_nc_path, "r")

    # Station-level: cluster_uid for each station
    cluster_uid = nc_str(ds["cluster_uid"])
    n_stations = len(cluster_uid)
    print(f"  Master stations: {n_stations}")

    # Record-level
    station_index = nc_int(ds["station_index"])
    time = nc_float(ds["time"])
    ssc = nc_float(ds["SSC"])
    n_records = len(station_index)
    print(f"  Master records: {n_records}")

    # Build cluster_uid -> station_index mapping
    cluster_to_indices = {}
    for i in range(n_stations):
        cu = cluster_uid[i]
        if cu not in cluster_to_indices:
            cluster_to_indices[cu] = []
        cluster_to_indices[cu].append(i)

    print(f"  Unique master clusters: {len(cluster_to_indices)}")

    # Build bulk DataFrame for all records
    df_records = pd.DataFrame({
        "station_index": station_index,
        "time": time,
        "SSC": ssc,
    })

    # Attach cluster_uid per record via station_index
    si_to_cu = pd.Series(cluster_uid, index=range(n_stations), dtype=object)
    df_records["cluster_uid"] = df_records["station_index"].map(si_to_cu)

    # Drop NaN SSC
    before = len(df_records)
    df_records = df_records.dropna(subset=["SSC"]).reset_index(drop=True)
    print(f"  Records with finite SSC: {len(df_records)} / {before}")

    # Sort by time for efficient matching
    df_records = df_records.sort_values("time").reset_index(drop=True)

    ds.close()
    return cluster_to_indices, df_records


# ===================================================================
# 3. Pairing
# ===================================================================
def pair_ssc(df_sat, df_master, windows=("exact", "pm1d", "pm2d")):
    """
    Pair satellite SSC with master SSC by linked cluster + time window.

    Uses a merge-based approach: for each unique linked cluster_uid,
    merge satellite records with master records on cluster_uid,
    then filter by time window.
    """
    max_window = max(WINDOW_DAYS[w] for w in windows if w in WINDOW_DAYS)

    # Build list of cluster_uids in master
    master_clusters = set(df_master["cluster_uid"].unique())

    # Filter satellite to only clusters in master
    df_sat_f = df_sat[df_sat["linked_cluster_uid"].isin(master_clusters)].copy()
    print(f"  Sat clusters in master: {df_sat_f['linked_cluster_uid'].nunique()} / {df_sat['linked_cluster_uid'].nunique()}")

    if df_sat_f.empty:
        return pd.DataFrame()

    # Cross-join: each sat record with all master records in same cluster
    # Use merge on cluster_uid
    merged = df_sat_f.merge(
        df_master[["cluster_uid", "time", "SSC"]],
        left_on="linked_cluster_uid",
        right_on="cluster_uid",
        suffixes=("_sat", "_main"),
        how="inner",
    )

    # Time difference
    merged["dt"] = np.abs(merged["time_main"] - merged["time_sat"])

    # Apply windows
    window_masks = []
    for w_name, w_days in WINDOW_DAYS.items():
        if w_name not in windows:
            continue
        if w_days == 0:
            window_masks.append(merged["dt"] < 0.5)
        else:
            window_masks.append(
                (merged["dt"] >= w_days - 0.5) & (merged["dt"] <= w_days + 0.5)
            )

    if not window_masks:
        return pd.DataFrame()

    combined_mask = window_masks[0]
    for m in window_masks[1:]:
        combined_mask |= m

    matched = merged[combined_mask].copy()
    print(f"  Pre-dedup matched: {len(matched)}")

    if matched.empty:
        return pd.DataFrame()

    # Assign window label
    matched["window"] = "pm2d"  # default
    for w_name, w_days in sorted(WINDOW_DAYS.items(), key=lambda x: -x[1]):
        if w_name not in windows:
            continue
        if w_days == 0:
            matched.loc[matched["dt"] < 0.5, "window"] = w_name
        else:
            matched.loc[
                (matched["dt"] >= w_days - 0.5) & (matched["dt"] <= w_days + 0.5),
                "window"
            ] = w_name

    # Dedup: keep the smallest time difference per (sat_rec, window)
    matched = matched.sort_values("dt").drop_duplicates(
        subset=["sat_station_index", "time_sat", "source", "window"],
        keep="first"
    ).reset_index(drop=True)

    # Build output
    pairs = pd.DataFrame({
        "sat_station_uid": matched["sat_station_uid"],
        "source": matched["source"],
        "linked_cluster_uid": matched["linked_cluster_uid"],
        "window": matched["window"],
        "time_sat": matched["time_sat"],
        "time_main": matched["time_main"],
        "time_diff_days": matched["time_main"] - matched["time_sat"],
        "SSC_sat": matched["SSC_sat"],
        "SSC_insitu": matched["SSC"],
        "lat": matched["lat"],
        "lon": matched["lon"],
    })

    print(f"  Total paired records: {len(pairs)}")
    for w in windows:
        cnt = (pairs["window"] == w).sum()
        print(f"    {w}: {cnt}")
    print(f"  Unique clusters: {pairs['linked_cluster_uid'].nunique()}")
    print(f"  By source: {dict(pairs['source'].value_counts())}")

    return pairs


# ===================================================================
# 4. Metrics computation
# ===================================================================
def compute_metrics(obs, pred):
    """
    Compute validation metrics.
    obs, pred: 1D numpy arrays, both finite.
    """
    n = len(obs)
    if n == 0:
        return {"n": 0, "R2": np.nan, "RMSE": np.nan, "Bias": np.nan,
                "NSE": np.nan, "KGE": np.nan}

    ss_res = np.sum((obs - pred) ** 2)
    ss_tot = np.sum((obs - np.mean(obs)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan
    rmse = np.sqrt(np.mean((obs - pred) ** 2))
    bias = np.mean(pred - obs)
    nse = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

    r = np.corrcoef(obs, pred)[0, 1] if n > 1 else np.nan
    alpha = np.std(pred) / np.std(obs) if np.std(obs) > 0 else np.nan
    beta = np.mean(pred) / np.mean(obs) if np.mean(obs) > 0 else np.nan
    kge = (1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)
           if np.all(np.isfinite([r, alpha, beta])) else np.nan)

    return {"n": n, "R2": r2, "RMSE": rmse, "Bias": bias, "NSE": nse, "KGE": kge}


def aggregate_metrics(df_pairs, group_col=None):
    """Compute metrics, optionally grouped."""
    if df_pairs.empty:
        return pd.DataFrame()

    if group_col is None:
        obs = df_pairs["SSC_insitu"].values
        pred = df_pairs["SSC_sat"].values
        return pd.DataFrame([compute_metrics(obs, pred)])

    rows = []
    for name, grp in df_pairs.groupby(group_col):
        obs = grp["SSC_insitu"].values
        pred = grp["SSC_sat"].values
        m = compute_metrics(obs, pred)
        m[group_col] = name
        rows.append(m)
    return pd.DataFrame(rows)


# ===================================================================
# 5. Plotting
# ===================================================================
def plot_scatter(df_pairs, out_path, title="Satellite vs In-Situ SSC", annotate=True):
    """Aggregate scatter plot."""
    if df_pairs.empty:
        print("  No data for scatter plot.")
        return

    fig, ax = plt.subplots(figsize=(8, 7))

    sources = df_pairs["source"].unique()
    colors = plt.cm.Set1(np.linspace(0, 1, max(1, len(sources))))
    for src, color in zip(sources, colors):
        sub = df_pairs[df_pairs["source"] == src]
        ax.scatter(sub["SSC_insitu"], sub["SSC_sat"], label=src, alpha=0.7,
                   s=40, c=[color], edgecolors="k", linewidths=0.5)

    # 1:1 line
    all_vals = pd.concat([df_pairs["SSC_insitu"], df_pairs["SSC_sat"]])
    vmin, vmax = all_vals.min(), all_vals.max()
    margin = max((vmax - vmin) * 0.05, 1)
    line_pts = np.linspace(vmin - margin, vmax + margin, 100)
    ax.plot(line_pts, line_pts, "k--", linewidth=1, alpha=0.5, label="1:1")

    obs = df_pairs["SSC_insitu"].values
    pred = df_pairs["SSC_sat"].values
    m = compute_metrics(obs, pred)

    if len(obs) > 1:
        coeffs = np.polyfit(obs, pred, 1)
        reg = np.poly1d(coeffs)
        ax.plot(line_pts, reg(line_pts), "r-", linewidth=1.5, alpha=0.7,
                label=f"OLS (slope={coeffs[0]:.3f})")

    if annotate:
        txt = (f"n = {m['n']}\n"
               f"R² = {m['R2']:.4f}\n"
               f"RMSE = {m['RMSE']:.1f} mg/L\n"
               f"Bias = {m['Bias']:.2f} mg/L\n"
               f"NSE = {m['NSE']:.4f}\n"
               f"KGE = {m['KGE']:.4f}")
        ax.text(0.05, 0.95, txt, transform=ax.transAxes, fontsize=10,
                verticalalignment="top",
                bbox=dict(boxstyle="round,pad=0.5", facecolor="wheat", alpha=0.8))

    ax.set_xlabel("In-Situ SSC (mg/L)", fontsize=12)
    ax.set_ylabel("Satellite SSC (mg/L)", fontsize=12)
    ax.set_title(title, fontsize=13)
    ax.legend(fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_per_cluster(df_pairs, out_dir, min_pairs=5):
    """Per-cluster scatter plots."""
    if df_pairs.empty:
        return

    cluster_counts = df_pairs["linked_cluster_uid"].value_counts()
    clusters = cluster_counts[cluster_counts >= min_pairs].index

    cluster_dir = out_dir / "scatter_by_cluster"
    cluster_dir.mkdir(parents=True, exist_ok=True)

    for cu in clusters:
        sub = df_pairs[df_pairs["linked_cluster_uid"] == cu]
        title = f"Cluster {cu} ({sub['source'].iloc[0]}, n={len(sub)})"
        out_path = cluster_dir / f"scatter_{cu}.png"
        plot_scatter(sub, out_path, title=title, annotate=True)


# ===================================================================
# Main
# ===================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Standalone SSC validation: satellite vs in-situ."
    )
    parser.add_argument(
        "--satellite-nc",
        default=str(DEFAULT_RELEASE_DIR / "sed_reference_satellite.nc"),
        help="Satellite validation NC file",
    )
    parser.add_argument(
        "--master-nc",
        default=str(DEFAULT_RELEASE_DIR / "sed_reference_master.nc"),
        help="Master matrix NC file",
    )
    parser.add_argument(
        "--linkage-csv",
        default=str(BASE_DIR / "output" / "s5b_satellite_main_cluster_linkage.csv"),
        help="Linkage CSV (optional)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Output directory",
    )
    parser.add_argument(
        "--windows",
        nargs="+",
        default=["exact", "pm1d", "pm2d"],
        choices=sorted(WINDOW_DAYS.keys()),
        help="Time windows for pairing",
    )
    parser.add_argument(
        "--min-cluster-pairs",
        type=int,
        default=5,
        help="Minimum pairs for per-cluster plots",
    )
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("Standalone SSC Validation")
    print("=" * 60)
    print(f"  Satellite NC: {args.satellite_nc}")
    print(f"  Master NC:    {args.master_nc}")
    print(f"  Windows:      {args.windows}")
    print(f"  Output:       {out_dir}")

    # 1. Load satellite
    print("\n[1/4] Loading satellite SSC records ...")
    df_sat = load_satellite_ssc(args.satellite_nc)
    if df_sat.empty:
        print("ERROR: No satellite SSC records found.")
        return 1

    # 2. Load master
    print("\n[2/4] Loading master SSC records ...")
    cluster_to_indices, df_master = load_master_ssc(args.master_nc)
    if df_master.empty:
        print("ERROR: No master SSC records found.")
        return 1

    # 3. Pair
    print("\n[3/4] Pairing satellite and in-situ SSC ...")
    df_pairs = pair_ssc(df_sat, df_master, windows=tuple(args.windows))
    if df_pairs.empty:
        print("ERROR: No paired records found.")
        return 1

    # Save pairs
    pairs_path = out_dir / "pairs.csv"
    df_pairs.to_csv(pairs_path, index=False)
    print(f"\n  Pairs saved: {pairs_path}")

    # 4. Metrics & Plots
    print("\n[4/4] Computing metrics and plotting ...")

    # Overall
    metrics_all = aggregate_metrics(df_pairs)
    metrics_all.insert(0, "group", "overall")
    print(f"\n  === Aggregate Metrics ===")
    for k, v in metrics_all.iloc[0].items():
        print(f"    {k}: {v}")

    # By source
    metrics_by_source = aggregate_metrics(df_pairs, group_col="source")

    # By cluster
    metrics_by_cluster = aggregate_metrics(df_pairs, group_col="linked_cluster_uid")

    # Combine
    metrics_all = pd.concat(
        [metrics_all, metrics_by_source, metrics_by_cluster], ignore_index=True
    )
    metrics_path = out_dir / "metrics.csv"
    metrics_all.to_csv(metrics_path, index=False)
    print(f"\n  Metrics saved: {metrics_path}")

    # By-source print
    print(f"\n  === Metrics by Source ===")
    for _, row in metrics_by_source.iterrows():
        print(f"    {str(row['source']):12s} n={int(row['n']):4d}  "
              f"R²={row['R2']:.4f}  RMSE={row['RMSE']:.1f}  "
              f"Bias={row['Bias']:.2f}  NSE={row['NSE']:.4f}")

    # Aggregate scatter
    plot_scatter(df_pairs, out_dir / "scatter_aggregate.png",
                 "Satellite vs In-Situ SSC (All Windows)")

    # By window
    for w in args.windows:
        sub = df_pairs[df_pairs["window"] == w]
        if not sub.empty:
            plot_scatter(sub, out_dir / f"scatter_{w}.png",
                         f"Satellite vs In-Situ SSC ({w})")

    # Per cluster
    plot_per_cluster(df_pairs, out_dir, min_pairs=args.min_cluster_pairs)

    print(f"\n{'=' * 60}")
    print("DONE")
    print(f"  Pairs:   {pairs_path}")
    print(f"  Metrics: {metrics_path}")
    print(f"  Plots:   {out_dir / 'scatter_aggregate.png'}")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
