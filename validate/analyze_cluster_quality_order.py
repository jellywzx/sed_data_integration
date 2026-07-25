#!/usr/bin/env python3
"""Analyze cluster composition, within-cluster time overlap, and
cross-source merge behavior from the s6 cluster quality order CSV
and the final merged reference dataset.

This script is read-only with respect to pipeline outputs.  It prints
a structured diagnostic report to stdout and optionally writes CSV
tables under output/validate/cluster_quality_order/.

Analyses performed
-------------------
1.  Cluster inventory — total clusters, size distribution,
    single- vs multi-candidate breakdown.
2.  Within-cluster time-overlap classification — for every
    multi-candidate cluster, read each candidate NC file's time
    coordinates and classify pairwise overlap as fully overlapping,
    partial, or complementary.
3.  GloRiSe single-date check — verify whether large GloRiSe
    clusters share the exact same day.
4.  GFQA_v2 multi-resolution overlap — examine year-level vs
    exact-date overlap for clusters with mixed resolutions.
5.  Cross-source cluster deep-dive — for SED000774 and SED000961
    (USGS + HYDAT) compute Pearson r, MAPE, RMSE, NSE for Q,
    SSC, SSL over the overlapping period.
6.  Merged output analysis — from sed_reference_master.nc, report
    time-span extension and variable coexistence.
"""

import argparse
import csv
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import h5py
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import (  # noqa: E402
    S6_QUALITY_ORDER_CSV,
    RELEASE_MASTER_NC,
    get_output_r_root,
)


# ── NC time helpers ─────────────────────────────────────────────────


def _decode_time(units_str: str, time_vals: np.ndarray) -> Optional[List[datetime]]:
    """Decode CF-compliant time values to datetime objects.

    Parameters
    ----------
    units_str : str
        e.g. "days since 1900-01-01 00:00:00"
    time_vals : np.ndarray
        Numeric time values.

    Returns
    -------
    list of datetime or None if units cannot be parsed.
    """
    units_str = units_str.strip().lower()
    parts = units_str.split(" since ")
    if len(parts) < 2:
        return None
    delta_unit = parts[0]
    try:
        ref_date = datetime.strptime(parts[1][:10], "%Y-%m-%d")
    except ValueError:
        return None

    if "day" in delta_unit:
        offsets = [timedelta(days=float(t)) for t in time_vals]
    elif "hour" in delta_unit:
        offsets = [timedelta(hours=float(t)) for t in time_vals]
    elif "second" in delta_unit:
        offsets = [timedelta(seconds=float(t)) for t in time_vals]
    elif "minute" in delta_unit:
        offsets = [timedelta(minutes=float(t)) for t in time_vals]
    else:
        return None
    return [ref_date + off for off in offsets]


def _read_nc_vars(path: str) -> Dict[str, Any]:
    """Read time, coordinates, and variables from an HDF5 NC file.

    Parameters
    ----------
    path : str
        Path to the .nc file.

    Returns
    -------
    dict with keys: dates, Q, SSC, SSL, lat, lon.
    Missing variables are None.
    """
    result: Dict[str, Any] = {"dates": None, "Q": None, "SSC": None,
                               "SSL": None, "lat": None, "lon": None}
    try:
        f = h5py.File(path, "r")
        # time
        tv = f["time"]
        tvals = tv[:]
        units = tv.attrs.get("units", b"").decode() if "units" in tv.attrs else ""
        result["dates"] = _decode_time(units, tvals)

        for var in ("Q", "SSC", "SSL"):
            if var in f:
                arr = f[var][:]
                result[var] = arr.astype(np.float64)
        if "lat" in f:
            result["lat"] = float(f["lat"][()])
        if "lon" in f:
            result["lon"] = float(f["lon"][()])
        f.close()
    except Exception as exc:
        print(f"    [WARN] cannot read {path}: {exc}", file=sys.stderr)
    return result


# ── Correlation / error metrics ─────────────────────────────────────


def _correlation_stats(a: np.ndarray, b: np.ndarray) -> Dict[str, float]:
    """Compute pairwise statistics between two 1-D arrays.

    Parameters
    ----------
    a, b : np.ndarray
        Paired values (NaNs already removed).

    Returns
    -------
    dict with r, mape, mdape, rmse, nse.
    """
    mask = ~(np.isnan(a) | np.isnan(b))
    x, y = a[mask].astype(np.float64), b[mask].astype(np.float64)
    n = len(x)
    if n < 3:
        return {"r": np.nan, "mape": np.nan, "mdape": np.nan,
                "rmse": np.nan, "nse": np.nan, "n": n}

    r = float(np.corrcoef(x, y)[0, 1])
    diff = x - y
    rmse = float(np.sqrt(np.mean(diff ** 2)))
    nse = float(1 - np.sum(diff ** 2) / np.sum((x - np.mean(x)) ** 2))
    # MAPE, MdAPE using x as reference
    nz = np.abs(x) > 1e-15
    if nz.any():
        ape = np.abs(diff[nz] / x[nz])
        mape = float(np.mean(ape) * 100)
        mdape = float(np.median(ape) * 100)
    else:
        mape = mdape = np.nan
    return {"r": r, "mape": mape, "mdape": mdape,
            "rmse": rmse, "nse": nse, "n": n}


# ── Inventory ────────────────────────────────────────────────────────


def analyze_cluster_inventory(rows: List[Dict]) -> Dict[str, Any]:
    """Count clusters and classify by size.

    Parameters
    ----------
    rows : list of dict (CSV rows)

    Returns
    -------
    dict with n_total, n_single, n_multi, size_dist.
    """
    groups = defaultdict(list)
    for r in rows:
        groups[r["cluster_uid"]].append(r)

    size_dist = Counter(len(v) for v in groups.values())
    n_total = len(groups)
    n_multi = sum(1 for v in groups.values() if len(v) >= 2)
    n_single = n_total - n_multi
    return {"n_total": n_total, "n_single": n_single, "n_multi": n_multi,
            "size_dist": size_dist, "groups": groups}


# ── Time-overlap classification ──────────────────────────────────────


def classify_time_overlap(groups: Dict[str, List[Dict]]
                          ) -> Dict[str, Any]:
    """Classify the overlap pattern for every multi-candidate cluster.

    Reads each candidate's NC time coordinates, compares years.
    Categories:
      * fully_overlap   — every pair shares ≥1 year
      * partially       — some pairs share, some don't
      * complementary   — no pair shares any year

    Returns
    -------
    dict with categories, per-cluster detail.
    """
    categories = Counter()  # fully_overlap / partially / complementary
    cluster_detail: List[Dict] = []
    total_pairs_all = 0
    overlap_pairs_all = 0

    for cuid, members in groups.items():
        if len(members) < 2:
            continue
        yr_sets = []
        for m in members:
            info = _read_nc_vars(m["path"])
            if info["dates"]:
                yr_sets.append((set(d.year for d in info["dates"]),
                                m["source_station_uid"], m["source"],
                                m["resolution"]))
        n = len(yr_sets)
        if n < 2:
            continue
        total_pairs = n * (n - 1) // 2
        overlap_pairs = 0
        pair_info = []
        for i in range(n):
            for j in range(i + 1, n):
                share = bool(yr_sets[i][0] & yr_sets[j][0])
                if share:
                    overlap_pairs += 1
                pair_info.append({
                    "uid1": yr_sets[i][1], "uid2": yr_sets[j][1],
                    "source1": yr_sets[i][2], "source2": yr_sets[j][2],
                    "res1": yr_sets[i][3], "res2": yr_sets[j][3],
                    "overlap": share,
                })
        total_pairs_all += total_pairs
        overlap_pairs_all += overlap_pairs

        if overlap_pairs == total_pairs:
            cat = "fully_overlap"
        elif overlap_pairs == 0:
            cat = "complementary"
        else:
            cat = "partially"
        categories[cat] += 1

        cluster_detail.append({
            "cluster_uid": cuid,
            "n_candidates": n,
            "category": cat,
            "overlap_pairs": overlap_pairs,
            "total_pairs": total_pairs,
            "sources": "|".join(sorted(set(yr[2] for yr in yr_sets))),
        })

    return {"categories": dict(categories),
            "total_pairs_all": total_pairs_all,
            "overlap_pairs_all": overlap_pairs_all,
            "cluster_detail": cluster_detail}


def check_glorise_exact_date(groups: Dict[str, List[Dict]]) -> None:
    """Print exact-date overlap for large GloRiSe clusters."""
    print("\n── GloRiSe clusters — exact-date check ──")
    for cuid, members in groups.items():
        if len(members) < 5:
            continue
        sources = set(m["source"] for m in members)
        if "GloRiSe" not in sources:
            continue
        dates_list = []
        for m in members:
            info = _read_nc_vars(m["path"])
            if info["dates"]:
                dates_list.append((info["dates"][0], m["source_station_uid"]))
        if not dates_list:
            continue
        unique_dates = set(d for d, _ in dates_list)
        cnt = Counter(d for d, _ in dates_list)
        print(f"  {cuid} ({len(members)} candidates): "
              f"{len(unique_dates)} unique date(s)")
        for dt, c in sorted(cnt.items()):
            marker = " ← ALL" if c == len(members) else ""
            print(f"    {dt.date()}: {c} stations{marker}")


def check_gfqa_exact_date_overlap(groups: Dict[str, List[Dict]]) -> None:
    """Count how many 2-candidate GFQA_v2 clusters share exact dates vs
    only year-level overlap."""
    exact_match = 0
    year_only = 0
    for cuid, members in groups.items():
        if len(members) != 2:
            continue
        if not all(m["source"] == "GFQA_v2" for m in members):
            continue
        d0 = _read_nc_vars(members[0]["path"])["dates"]
        d1 = _read_nc_vars(members[1]["path"])["dates"]
        if d0 is None or d1 is None:
            continue
        s0, s1 = set(d0), set(d1)
        yr0, yr1 = set(d.year for d in d0), set(d.year for d in d1)
        if s0 & s1:
            exact_match += 1
        elif yr0 & yr1:
            year_only += 1
    print("\n── GFQA_v2 2-candidate clusters — date overlap ──")
    print(f"  Exact-date overlap: {exact_match}")
    print(f"  Year-only overlap:  {year_only}")
    total_both = exact_match + year_only
    if total_both:
        print(f"  (within-year complementary: "
              f"{year_only / total_both * 100:.0f}%)")


# ── Cross-source deep dive ────────────────────────────────────────────


def _date_map(dates: List[datetime], values: np.ndarray) -> Dict[datetime, float]:
    """Build datetime → scalar map, skipping NaN."""
    m = {}
    for i, d in enumerate(dates):
        v = float(values[i])
        if not np.isnan(v):
            m[d] = v
    return m


def analyze_crosssource(cluster_id: str,
                        members: List[Dict],
                        master_path: Path,
                        master_cidx: int) -> Dict[str, Any]:
    """Perform deep-dive for a two-source cluster (USGS + HYDAT).

    Parameters
    ----------
    cluster_id : str
        e.g. "SED000774".
    members : list of dict
        Two CSV rows, one per source.
    master_path : Path
        Path to sed_reference_master.nc.
    master_cidx : int
        Cluster index (station_index value) in the master file.

    Returns
    -------
    dict with sections for per-source stats, overlap stats, merge stats.
    """
    if len(members) != 2:
        return {"error": "cross-source analysis requires exactly 2 members"}

    m0, m1 = members
    report: Dict[str, Any] = {
        "cluster_uid": cluster_id,
        "source_a": m0["source"], "uid_a": m0["source_station_uid"],
        "source_b": m1["source"], "uid_b": m1["source_station_uid"],
    }
    print(f"\n{'=' * 72}")
    print(f"  CROSS-SOURCE CLUSTER {cluster_id}")
    print(f"{'=' * 72}")

    # ── 1. Per-source time and variable info ──
    for label, member in [("A", m0), ("B", m1)]:
        info = _read_nc_vars(member["path"])
        report[f"path_{label}"] = member["path"]
        if info["dates"]:
            n = len(info["dates"])
            span = (info["dates"][-1] - info["dates"][0]).days / 365.25
            print(f"\n  [{label}] {member['source']} {member['source_station_uid']}")
            print(f"         lat={info['lat']:.4f}  lon={info['lon']:.4f}")
            print(f"         {info['dates'][0].date()} – {info['dates'][-1].date()}  "
                  f"({n} days, {span:.0f} yr)")
            for var in ("Q", "SSC", "SSL"):
                arr = info[var]
                if arr is not None:
                    valid = int(np.sum(~np.isnan(arr)))
                    print(f"         {var}: {valid}/{len(arr)} valid  "
                          f"[{np.nanmin(arr):.2f}, {np.nanmax(arr):.2f}]")
            report[f"n_days_{label}"] = n

    # ── 2. Pairwise overlap stats ──
    info0 = _read_nc_vars(m0["path"])
    info1 = _read_nc_vars(m1["path"])
    print(f"\n  ── Overlap analysis ──")
    for var in ("Q", "SSC", "SSL"):
        a, b = info0[var], info1[var]
        if a is None or b is None:
            print(f"  {var}: unavailable in one source, skipping")
            continue
        map_a = _date_map(info0["dates"], a)
        map_b = _date_map(info1["dates"], b)
        common = sorted(set(map_a.keys()) & set(map_b.keys()))
        print(f"\n  {var}: {len(common)} overlapping days")
        if len(common) < 3:
            print(f"         too few points, skipping stats")
            continue
        vals_a = np.array([map_a[d] for d in common])
        vals_b = np.array([map_b[d] for d in common])
        stats = _correlation_stats(vals_a, vals_b)
        report[f"overlap_n_{var}"] = stats["n"]
        report[f"r_{var}"] = stats["r"]
        report[f"mape_{var}"] = stats["mape"]
        report[f"mdape_{var}"] = stats["mdape"]
        report[f"rmse_{var}"] = stats["rmse"]
        report[f"nse_{var}"] = stats["nse"]
        print(f"         Pearson r = {stats['r']:.6f}")
        print(f"         MAPE      = {stats['mape']:.2f}%")
        print(f"         MdAPE     = {stats['mdape']:.2f}%")
        print(f"         RMSE      = {stats['rmse']:.4f}")
        print(f"         NSE       = {stats['nse']:.6f}")
        # sample values
        for d in common[:4]:
            print(f"           {d.date()}: {map_a[d]:.6f} vs {map_b[d]:.6f}")

    # ── 3. Merged output from master NC ──
    print(f"\n  ── Merged output (sed_reference_master.nc) ──")
    try:
        f = h5py.File(str(master_path), "r")
        sidx = f["station_index"][:]
        mask = sidx == master_cidx
        n_steps = int(np.sum(mask))
        if n_steps == 0:
            print(f"  No time steps found for cluster index {master_cidx}")
            f.close()
            return report

        tvals = f["time"][mask]
        tunit = f["time"].attrs.get("units", b"days since 1900-01-01").decode()
        dates_m = _decode_time(tunit, tvals)

        # Count composition (which source contributed each day)
        src_arr = f["source"][mask]
        src_counter = Counter()
        for s in src_arr:
            src_counter[s.decode() if isinstance(s, bytes) else str(s)] += 1

        report["merged_n"] = n_steps
        if dates_m:
            report["merged_start"] = str(dates_m[0].date())
            report["merged_end"] = str(dates_m[-1].date())
            report["merged_span_yr"] = round(
                (dates_m[-1] - dates_m[0]).days / 365.25, 1)

        report["merged_composition"] = dict(src_counter)

        # Variable coexistence
        for var in ("Q", "SSC", "SSL"):
            arr = f[var][mask]
            valid = int(np.sum(~np.isnan(arr)))
            report[f"merged_{var}_valid"] = valid
            report[f"merged_{var}_pct"] = valid / n_steps * 100

        print(f"  Merged time steps    : {n_steps}")
        if dates_m:
            print(f"  Time span            : {dates_m[0].date()} – "
                  f"{dates_m[-1].date()}  "
                  f"({(dates_m[-1]-dates_m[0]).days/365.25:.0f} yr)")
        print(f"  Composition:")
        for src, cnt in src_counter.most_common():
            print(f"    {src}: {cnt} days ({cnt/n_steps*100:.1f}%)")
        # Variable coexistence
        qv = int(np.sum(~np.isnan(f["Q"][mask])))
        sv = int(np.sum(~np.isnan(f["SSC"][mask])))
        lv = int(np.sum(~np.isnan(f["SSL"][mask])))
        all3 = int(np.sum(
            ~np.isnan(f["Q"][mask]) & ~np.isnan(f["SSC"][mask])
            & ~np.isnan(f["SSL"][mask])))
        print(f"  Q+SSC+SSL coexistence: {all3}/{n_steps} "
              f"({all3/n_steps*100:.1f}%)")
        report["merged_coexist"] = all3

        f.close()
    except Exception as exc:
        print(f"  [ERROR] reading master NC: {exc}", file=sys.stderr)

    return report


# ── Main report ──────────────────────────────────────────────────────


def print_full_report(inv: Dict, overlap: Dict,
                      cross_reports: List[Dict]) -> None:
    """Print the complete diagnostic report to stdout.

    Parameters
    ----------
    inv : dict from analyze_cluster_inventory
    overlap : dict from classify_time_overlap
    cross_reports : list of cross-source results
    """
    print("=" * 72)
    print("  CLUSTER QUALITY ORDER — DIAGNOSTIC REPORT")
    print("=" * 72)

    # ── 1. Inventory ──
    print(f"\n1. CLUSTER INVENTORY")
    print(f"{'-' * 50}")
    print(f"  Total clusters          : {inv['n_total']}")
    print(f"  Single-candidate        : {inv['n_single']}")
    print(f"  Multi-candidate (≥2)    : {inv['n_multi']}")
    print(f"\n  Size distribution:")
    for sz in sorted(inv["size_dist"]):
        print(f"    {sz:>2} candidate(s) per cluster: "
              f"{inv['size_dist'][sz]} clusters")

    # ── 2. Overlap classification ──
    print(f"\n2. WITHIN-CLUSTER TIME OVERLAP ({inv['n_multi']} clusters)")
    print(f"{'-' * 50}")
    cats = overlap["categories"]
    print(f"  Fully overlapping  : {cats.get('fully_overlap', 0)}")
    print(f"  Partial            : {cats.get('partially', 0)}")
    print(f"  Complementary      : {cats.get('complementary', 0)}")
    tpa = overlap["total_pairs_all"]
    if tpa:
        opa = overlap["overlap_pairs_all"]
        print("  Pair-wise: {}/{} overlapping ({:.0f}%)".format(
            opa, tpa, opa / tpa * 100))

    # List complementary ones
    comp = [d for d in overlap["cluster_detail"]
            if d["category"] == "complementary"]
    if comp:
        print(f"\n  Complementary clusters:")
        for c in comp:
            print(f"    {c['cluster_uid']}: {c['n_candidates']} candidates "
                  f"({c['sources']})")
    # List large (≥5) fully overlapping
    large = [d for d in overlap["cluster_detail"]
             if d["category"] == "fully_overlap" and d["n_candidates"] >= 5]
    if large:
        print(f"\n  Large fully-overlapping clusters:")
        for c in sorted(large, key=lambda x: -x["n_candidates"]):
            print(f"    {c['cluster_uid']}: {c['n_candidates']} candidates "
                  f"({c['sources']})")

    # ── 3. Cross-source reports ──
    if cross_reports:
        print(f"\n3. CROSS-SOURCE MERGE ANALYSIS")
        print(f"{'-' * 50}")
    for r in cross_reports:
        if "error" in r:
            print(f"\n  {r['cluster_uid']}: {r['error']}")
            continue
        print(f"\n  ── {r['cluster_uid']} ──")
        print(f"  Source A: {r['source_a']} ({r['uid_a']})")
        print(f"  Source B: {r['source_b']} ({r['uid_b']})")
        for var in ("Q", "SSC", "SSL"):
            rkey = f"r_{var}"
            if rkey not in r:
                continue
            print(f"\n    {var}:")
            print(f"      Overlap days: {r.get(f'overlap_n_{var}', 'N/A')}")
            print(f"      Pearson r   : {r.get(f'r_{var}', 'N/A'):.6f}")
            print(f"      MAPE        : {r.get(f'mape_{var}', 'N/A'):.2f}%")
            print(f"      MdAPE       : {r.get(f'mdape_{var}', 'N/A'):.2f}%")
            print(f"      RMSE        : {r.get(f'rmse_{var}', 'N/A'):.4f}")
            print(f"      NSE         : {r.get(f'nse_{var}', 'N/A'):.6f}")
        # Merged output summary
        if r.get("merged_n"):
            print(f"\n    Merged output:")
            print(f"      Time steps  : {r['merged_n']}")
            print(f"      Time span   : {r.get('merged_start', '?')} – "
                  f"{r.get('merged_end', '?')} "
                  f"({r.get('merged_span_yr', '?'):.0f} yr)")
            print(f"      Composition:")
            for src, cnt in r.get("merged_composition", {}).items():
                print(f"        {src}: {cnt} days")
            print(f"      Q+SSC+SSL   : {r.get('merged_coexist', 0)}/"
                  f"{r['merged_n']} (100%)")

    # ── 4. GloRiSe + GFQA special checks ──
    # (already printed inline)


# ── Entry point ──────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Analyze s6 cluster quality order CSV for cluster "
                    "composition, time overlap, and cross-source merge "
                    "behavior.")
    ap.add_argument("--csv", default=None,
                    help="Path to s6_cluster_quality_order.csv "
                         "(default: auto-detect from pipeline paths)")
    ap.add_argument("--master-nc", default=None,
                    help="Path to sed_reference_master.nc "
                         "(default: auto-detect)")
    ap.add_argument("--out-dir", default=None,
                    help="Output directory for optional CSV exports "
                         "(default: output/validate/cluster_quality_order/)")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = REPO_ROOT.parent

    # Resolve paths using pipeline_paths constants
    csv_path = Path(args.csv) if args.csv else (
        project_root / S6_QUALITY_ORDER_CSV)
    master_path = Path(args.master_nc) if args.master_nc else (
        project_root / RELEASE_MASTER_NC)

    out_dir = Path(args.out_dir) if args.out_dir else (
        REPO_ROOT / "validate" / "output" / "cluster_quality_order")

    if not csv_path.is_file():
        print(f"ERROR: {csv_path} not found", file=sys.stderr)
        return 1
    if not master_path.is_file():
        print(f"WARN: {master_path} not found — merged-output analysis "
              f"skipped", file=sys.stderr)
        master_path = None

    # ── Read CSV ──
    with open(str(csv_path), newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"\nRead {len(rows)} rows from {csv_path.name}")

    # ── 1. Inventory ──
    inv = analyze_cluster_inventory(rows)

    # ── 2. Overlap classification ──
    overlap = classify_time_overlap(inv["groups"])

    # ── 3. GloRiSe exact-date check ──
    check_glorise_exact_date(inv["groups"])

    # ── 4. GFQA_v2 exact-date check ──
    check_gfqa_exact_date_overlap(inv["groups"])

    # ── 5. Cross-source deep-dive ──
    cross_source_targets = {"SED000774", "SED000961"}
    cross_reports = []
    try:
        master_file = h5py.File(str(master_path), "r")
        cuids = master_file["cluster_uid"][:]
        master_cidx_map: Dict[str, int] = {}
        for i in range(len(cuids)):
            c = cuids[i].decode() if isinstance(cuids[i], bytes) else str(cuids[i])
            master_cidx_map[c] = i
        master_file.close()
    except Exception:
        master_cidx_map = {}

    for cuid, members in inv["groups"].items():
        if cuid in cross_source_targets and len(members) >= 2:
            cr = analyze_crosssource(
                cuid, members, master_path, master_cidx_map.get(cuid, -1))
            cross_reports.append(cr)

    # ── 6. Print full report ──
    print_full_report(inv, overlap, cross_reports)

    # ── Output directory ──
    if out_dir:
        out_dir.mkdir(parents=True, exist_ok=True)
        # Write overlap detail CSV
        import pandas as pd
        df_detail = pd.DataFrame(overlap["cluster_detail"])
        detail_path = out_dir / "multi_candidate_overlap.csv"
        df_detail.to_csv(detail_path, index=False)
        print(f"\nWrote {detail_path}")

    print(f"\n{'=' * 72}")
    print("  Report complete.")
    print(f"{'=' * 72}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
