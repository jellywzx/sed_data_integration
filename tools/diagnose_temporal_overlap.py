#!/usr/bin/env python3
"""Diagnose temporal overlap between satellite and in-situ observations in s11b.

Directly reads the master NC and satellite source files (bypassing s11b internals)
to diagnose why ``pairs=0`` in the s11b validation run.
"""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

try:
    import xarray as xr
except ImportError:
    raise SystemExit("xarray is required")

try:
    from pipeline_paths import S5_BASIN_CLUSTERED_CSV, S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from pipeline_paths import S5_BASIN_CLUSTERED_CSV, S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUTPUT_R_ROOT = Path(
    os.environ.get("OUTPUT_R_ROOT", str(PROJECT_DIR.parent))
).expanduser().resolve()

DEFAULT_LINKAGE_CSV = OUTPUT_R_ROOT / S5B_SATELLITE_MAIN_CLUSTER_LINKS_CSV
DEFAULT_S5_CSV = OUTPUT_R_ROOT / S5_BASIN_CLUSTERED_CSV
DEFAULT_SOURCE_ROOT = (OUTPUT_R_ROOT / "../output_resolution_organized").resolve()
DEFAULT_RELEASE_DIR = OUTPUT_R_ROOT / "scripts_basin_test/output/sed_reference_release"

RESOLUTION_CODE = {0: "daily", 1: "monthly", 2: "annual", 3: "climatology"}


def banner(text: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {text}")
    print("=" * 72)


def section(text: str) -> None:
    print(f"\n── {text} ──")


def log(msg: str) -> None:
    print(f"  {msg}")


# ── Helpers (replicated from s11b to avoid import issues) ──────────────

def _clean_text(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip()
    return str(value).strip()


def _normalize_resolution(text) -> str:
    text = str(text).strip().lower()
    if text in RESOLUTION_CODE.values():
        return text
    if text in ("clim", "climatological"):
        return "climatology"
    if text in ("yearly", "yr", "year"):
        return "annual"
    if text in ("mon", "mn", "month"):
        return "monthly"
    if text in ("day", "dly", "1d", "d"):
        return "daily"
    return text


def _satellite_key_from_row(row) -> str:
    """Replicate s11b._satellite_key_from_row."""
    station_id = _clean_text(row.get("station_id", ""))
    source = _clean_text(row.get("source", "")).lower()
    native = _clean_text(row.get("source_station_id", ""))
    resolution = _normalize_resolution(row.get("resolution", ""))
    payload = "\x1f".join([station_id, source, native, resolution])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16].upper()
    return "SATV2{}".format(digest)


def _is_satellite_s5_row(row) -> bool:
    """Replicate s11b._is_satellite_s5_row."""
    obs_type = _clean_text(row.get("observation_type", "")).lower()
    if obs_type:
        return "satellite" in obs_type
    source = _clean_text(row.get("source", "")).lower()
    return any(token in source for token in ("riversed", "river_sed", "gsed", "dethier", "aquasat"))


def _cluster_uid_from_id(value) -> str:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return ""
        number = int(float(value))
    except Exception:
        return ""
    return "SED{:06d}".format(number) if number >= 0 else ""


# ── Step 1: Load linkage targets ──────────────────────────────────────

def load_linkage_targets(linkage_csv: Path, s5_csv: Path,
                         release_dir: Path) -> Tuple[Set[Tuple[str, str]], pd.DataFrame]:
    banner("STEP 1: Loading linkage targets")

    linkage = pd.read_csv(linkage_csv, low_memory=False)
    s5 = pd.read_csv(s5_csv, low_memory=False)
    log(f"Linkage CSV: {len(linkage)} rows, columns: {list(linkage.columns)}")
    log(f"S5 CSV: {len(s5)} rows")

    # Filter s5 to satellite rows
    s5_sat = s5[s5.apply(_is_satellite_s5_row, axis=1)].copy()
    log(f"S5 satellite rows: {len(s5_sat)}")

    # Compute satellite_key for s5 rows
    s5_sat["satellite_key"] = s5_sat.apply(_satellite_key_from_row, axis=1)
    s5_sat["satellite_location_uid"] = s5_sat["satellite_key"]
    s5_sat["cluster_uid"] = s5_sat["cluster_id"].map(_cluster_uid_from_id)
    s5_sat["resolution"] = s5_sat["resolution"].map(_normalize_resolution)

    # Check for duplicate keys
    dups = s5_sat.duplicated("satellite_key", keep=False)
    if dups.any():
        log(f"  WARNING: {int(dups.sum())} duplicate satellite_key values in s5")

    # Clean keys in linkage
    linkage["satellite_key"] = linkage["satellite_key"].map(_clean_text)

    # Merge linkage with s5 on satellite_key
    needed = ["satellite_key", "path", "source", "source_station_id",
              "cluster_id", "resolution", "station_id", "cluster_uid"]
    s5_lookup = s5_sat[needed].drop_duplicates("satellite_key")
    merged = linkage.merge(s5_lookup, on="satellite_key", how="left", suffixes=("", "_s5"))

    missing_path = merged["path"].isna()
    if missing_path.any():
        log(f"  WARNING: {int(missing_path.sum())} rows have no path after merge")

    linked = merged[merged["link_status"].eq("linked")].copy()
    log(f"Linked rows: {len(linked)}")
    log(f"Linked rows with path: {linked['path'].notna().sum()}")

    # Populate linked_cluster_uid from linkage CSV (it's already there)
    # The linkage CSV has linked_cluster_uid as a column
    linked["linked_cluster_uid"] = linked["linked_cluster_uid"].map(_clean_text)
    linked["linked_resolution"] = linked["satellite_resolution"].map(_normalize_resolution)

    # Load station catalog
    catalog_path = release_dir / "station_catalog.csv"
    catalog = pd.read_csv(catalog_path)
    catalog_pairs = set()
    for _, row in catalog.iterrows():
        uid = _clean_text(row.get("cluster_uid", ""))
        res = _normalize_resolution(row.get("resolution", ""))
        if uid and res:
            catalog_pairs.add((uid, res))
    log(f"Station catalog pairs: {len(catalog_pairs)}")

    # Build target pairs from linked rows that are in catalog
    target_pairs: Set[Tuple[str, str]] = set()
    for _, row in linked.iterrows():
        uid = _clean_text(row.get("linked_cluster_uid", ""))
        res = _normalize_resolution(row.get("linked_resolution", ""))
        if uid and res and (uid, res) in catalog_pairs:
            target_pairs.add((uid, res))

    log(f"Valid target pairs (in catalog): {len(target_pairs)}")
    return target_pairs, linked


# ── Step 2: Load in-situ from master NC ───────────────────────────────

def load_insitu_direct(release_dir: Path,
                       target_pairs: Set[Tuple[str, str]]) -> pd.DataFrame:
    banner("STEP 2: Loading in-situ from master NC")

    path = release_dir / "sed_reference_master.nc"
    log(f"Opening: {path}")

    ds = xr.open_dataset(path)
    n_records = int(ds.sizes.get("n_records", 0))
    n_stations = int(ds.sizes.get("n_stations", 0))
    log(f"Dimensions: n_records={n_records}, n_stations={n_stations}")

    # Station-level: cluster_uid
    station_cluster_uid = np.asarray(ds["cluster_uid"].values).reshape(-1)
    station_cluster_uid = np.array([_clean_text(v) for v in station_cluster_uid])

    # Record-level
    station_index = np.asarray(ds["station_index"].values).reshape(-1).astype(int)
    resolution_codes = np.asarray(ds["resolution"].values).reshape(-1).astype(int)

    # Time (already datetime64)
    time_vals = np.asarray(ds["time"].values).reshape(-1)
    times = pd.DatetimeIndex(time_vals).floor("D")

    # Map station_index -> cluster_uid
    record_cluster_uid = np.array([station_cluster_uid[i] for i in station_index])
    record_resolution = np.array([RESOLUTION_CODE.get(c, "unknown") for c in resolution_codes])

    # Filter to target pairs
    mask = np.zeros(n_records, dtype=bool)
    for uid, res in target_pairs:
        mask |= (record_cluster_uid == uid) & (record_resolution == res)
    n_matched = int(mask.sum())
    log(f"Records matching target pairs: {n_matched}/{n_records}")

    if n_matched == 0:
        ds.close()
        log("WARNING: No records match target pairs!")
        return pd.DataFrame()

    idx = np.where(mask)[0]

    records = pd.DataFrame({
        "cluster_uid": record_cluster_uid[idx],
        "resolution": record_resolution[idx],
        "time": times[idx],
    })

    for var in ["Q", "SSC", "SSL"]:
        if var in ds.variables:
            arr = np.asarray(ds[var].values).reshape(-1)
            records[var] = pd.to_numeric(arr[idx], errors="coerce")

    for var in ["source_family", "source"]:
        if var in ds.variables:
            arr = np.asarray(ds[var].values).reshape(-1)
            if arr.dtype.kind in ("S", "U"):
                records[var] = [_clean_text(arr[i]) for i in idx]
            else:
                records[var] = arr[idx]

    ds.close()

    # Filter to in_situ
    if "source_family" in records.columns:
        insitu = records[
            records["source_family"].str.lower().str.contains("in_situ", na=False)
        ].copy()
    else:
        insitu = records.copy()

    log(f"In-situ records: {len(insitu)}")
    insitu["_dt"] = pd.to_datetime(insitu["time"], errors="coerce").dt.floor("D")
    log(f"Valid dates: {insitu['_dt'].notna().sum()}/{len(insitu)}")

    return insitu


# ── Step 3: Load satellite data ──────────────────────────────────────

def _read_one_satellite(path: Path, linked_uid: str, linked_res: str,
                        source_name: str) -> Optional[pd.DataFrame]:
    """Read time + Q/SSC/SSL from a single satellite NetCDF file."""
    try:
        ds = xr.open_dataset(path)
    except Exception:
        return None

    try:
        # Find time variable
        time_name = None
        for c in ("time", "date", "datetime", "timestamp", "obs_time"):
            if c in ds.variables:
                time_name = c
                break
        if time_name is None:
            return None

        da = ds[time_name]
        raw = np.asarray(da.values).reshape(-1)

        if np.issubdtype(raw.dtype, np.datetime64):
            times = pd.DatetimeIndex(raw).floor("D")
        elif raw.dtype.kind in {"S", "U", "O"}:
            texts = [
                v.decode("utf-8", errors="ignore") if isinstance(v, bytes) else str(v)
                for v in raw
            ]
            times = pd.DatetimeIndex(
                pd.to_datetime(texts, errors="coerce")
            ).floor("D")
        else:
            units = str(da.attrs.get("units", da.encoding.get(
                "units", "days since 1970-01-01")))
            calendar = str(da.attrs.get("calendar", da.encoding.get(
                "calendar", "standard")))
            try:
                import netCDF4
                decoded = netCDF4.num2date(
                    raw.astype(float), units=units, calendar=calendar,
                    only_use_cftime_datetimes=False,
                    only_use_python_datetimes=False,
                )
                times = pd.DatetimeIndex([
                    pd.Timestamp("{:04d}-{:02d}-{:02d}".format(
                        int(d.year), int(d.month), int(d.day)))
                    for d in np.asarray(decoded).reshape(-1)
                ])
            except Exception:
                times = pd.DatetimeIndex(
                    pd.to_datetime(raw, unit="D", origin="1970-01-01",
                                   errors="coerce")
                ).floor("D")

        n = len(times)

        result = {"date": [], "Q": [], "SSC": [], "SSL": []}
        for var, candidates in [
            ("Q", ("Q", "q", "discharge")),
            ("SSC", ("SSC", "ssc")),
            ("SSL", ("SSL", "ssl")),
        ]:
            vals = np.full(n, np.nan)
            for c_name in candidates:
                if c_name in ds.variables:
                    da_var = ds[c_name]
                    if da_var.dims[0] == time_name or len(da_var.dims) == 1:
                        arr = np.asarray(da_var.values)
                        if arr.ndim == 1:
                            vals = arr
                        elif arr.ndim == 2:
                            vals = arr[:, 0]
                    break
            result[var] = vals

        result["date"] = [
            t.strftime("%Y-%m-%d") if pd.notna(t) else ""
            for t in times
        ]

        df = pd.DataFrame(result)
        df["linked_cluster_uid"] = linked_uid
        df["linked_resolution"] = linked_res
        df["source"] = source_name

        # Keep only rows with at least one finite value
        has_val = (
            pd.to_numeric(df["Q"], errors="coerce").notna() |
            pd.to_numeric(df["SSC"], errors="coerce").notna() |
            pd.to_numeric(df["SSL"], errors="coerce").notna()
        )
        return df[has_val].copy() if has_val.any() else None
    finally:
        ds.close()


def load_satellite_direct(source_root: Path, linked: pd.DataFrame,
                          target_pairs: Set[Tuple[str, str]],
                          workers: int) -> pd.DataFrame:
    banner("STEP 3: Loading satellite data")

    source_root = source_root.resolve()
    files_to_read: List[Tuple[Path, str, str, str]] = []

    seen_paths = set()
    for _, row in linked.iterrows():
        uid = _clean_text(row.get("linked_cluster_uid", ""))
        res = _normalize_resolution(row.get("linked_resolution", ""))
        if (uid, res) not in target_pairs:
            continue
        path_str = _clean_text(row.get("path", ""))
        if not path_str:
            continue
        p = Path(path_str)
        if not p.is_absolute():
            p = source_root / path_str
        p_str = str(p.resolve())
        if p_str in seen_paths:
            continue
        seen_paths.add(p_str)
        if p.is_file():
            files_to_read.append((p, uid, res,
                                  _clean_text(row.get("source", ""))))

    log(f"Unique satellite files for target pairs: {len(files_to_read)}")

    if not files_to_read:
        # Debug: show some paths
        log("DEBUG: No files resolved. Sample linked rows:")
        for i, (_, row) in enumerate(linked.head(10).iterrows()):
            uid = _clean_text(row.get("linked_cluster_uid", ""))
            res = _normalize_resolution(row.get("linked_resolution", ""))
            path_str = _clean_text(row.get("path", ""))
            p = Path(path_str) if path_str else Path("")
            if not p.is_absolute() and path_str:
                p = source_root / path_str
            log(f"  [{i}] uid={uid}, res={res}, path={path_str}, "
                f"resolved={p}, exists={p.is_file() if path_str else False}")
        return pd.DataFrame()

    log(f"Reading {len(files_to_read)} files with {workers} workers...")

    all_rows = []
    success = 0

    if workers <= 1:
        for p, uid, res, src in files_to_read:
            df = _read_one_satellite(p, uid, res, src)
            if df is not None and len(df) > 0:
                all_rows.append(df)
                success += 1
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_read_one_satellite, p, uid, res, src): p
                for p, uid, res, src in files_to_read
            }
            for future in as_completed(futures):
                try:
                    df = future.result()
                    if df is not None and len(df) > 0:
                        all_rows.append(df)
                        success += 1
                except Exception:
                    pass

    log(f"Files successfully read: {success}/{len(files_to_read)}")

    if not all_rows:
        return pd.DataFrame()

    satellite = pd.concat(all_rows, ignore_index=True)
    log(f"Total satellite rows: {len(satellite)}")

    satellite["_dt"] = pd.to_datetime(satellite["date"], errors="coerce")
    log(f"Valid dates: {satellite['_dt'].notna().sum()}/{len(satellite)}")

    return satellite


# ── Step 4: Diagnostic analysis ──────────────────────────────────────

def diagnose(insitu: pd.DataFrame, satellite: pd.DataFrame,
             target_pairs: Set[Tuple[str, str]]) -> None:
    banner("STEP 4: Temporal overlap diagnostics")

    i_dates = insitu["_dt"].dropna()
    s_dates = satellite["_dt"].dropna()

    if i_dates.empty or s_dates.empty:
        log("ERROR: One or both datasets have no valid dates!")
        return

    # ── 4a: Overall time ranges ──────────────────────────────────
    section("4a: Overall time ranges")
    log(f"In-situ:  {i_dates.min().date()} -> {i_dates.max().date()}  "
        f"({i_dates.nunique()} unique dates, {len(i_dates)} rows)")
    log(f"Satellite: {s_dates.min().date()} -> {s_dates.max().date()}  "
        f"({s_dates.nunique()} unique dates, {len(s_dates)} rows)")

    overlap_start = max(i_dates.min(), s_dates.min())
    overlap_end = min(i_dates.max(), s_dates.max())
    if overlap_start <= overlap_end:
        log(f"Global overlap: {overlap_start.date()} -> {overlap_end.date()}  OK")
    else:
        log(f"NO global overlap! In-situ ends {i_dates.max().date()}, "
            f"satellite starts {s_dates.min().date()}")

    # ── 4b: Per-pair analysis ────────────────────────────────────
    section("4b: Per (cluster_uid, resolution) pair analysis")

    i_groups: Dict[Tuple[str, str], pd.Series] = {}
    for (uid, res), grp in insitu.groupby(["cluster_uid", "resolution"], sort=False):
        dates = grp["_dt"].dropna()
        if len(dates) > 0:
            i_groups[(uid, res)] = dates

    s_groups: Dict[Tuple[str, str], pd.Series] = {}
    for (uid, res), grp in satellite.groupby(
        ["linked_cluster_uid", "linked_resolution"], sort=False
    ):
        dates = grp["_dt"].dropna()
        if len(dates) > 0:
            s_groups[(uid, res)] = dates

    with_i = set(i_groups.keys())
    with_s = set(s_groups.keys())
    both = with_i & with_s
    i_only = with_i - with_s
    s_only = with_s - with_i

    log(f"Target pairs:                    {len(target_pairs)}")
    log(f"With in-situ data:               {len(with_i)}")
    log(f"With satellite data:             {len(with_s)}")
    log(f"Both in-situ & satellite:        {len(both)}")
    log(f"In-situ only (no satellite):     {len(i_only)}")
    log(f"Satellite only (no in-situ):     {len(s_only)}")

    if i_only:
        log(f"\n  First 10 in-situ-only pairs:")
        for key in sorted(i_only)[:10]:
            d = i_groups[key]
            log(f"    {key}: {len(d)} obs, {d.min().date()} -> {d.max().date()}")

    if s_only:
        log(f"\n  First 10 satellite-only pairs:")
        for key in sorted(s_only)[:10]:
            d = s_groups[key]
            log(f"    {key}: {len(d)} obs, {d.min().date()} -> {d.max().date()}")

    # ── 4c: Overlap within shared pairs ──────────────────────────
    section("4c: Temporal overlap within shared pairs")

    no_exact = no_pm1d = no_pm2d = 0
    gap_details = []

    for key in sorted(both):
        i_set = set(i_groups[key])
        s_set = set(s_groups[key])
        i_min, i_max = i_groups[key].min(), i_groups[key].max()
        s_min, s_max = s_groups[key].min(), s_groups[key].max()

        exact = s_set & i_set
        i_pm1d = set()
        for d in i_set:
            i_pm1d |= {d, d + pd.Timedelta(days=1), d - pd.Timedelta(days=1)}
        pm1d = s_set & i_pm1d
        i_pm2d = set()
        for d in i_set:
            for delta in range(-2, 3):
                i_pm2d.add(d + pd.Timedelta(days=delta))
        pm2d = s_set & i_pm2d

        gap_details.append({
            "key": key, "i_n": len(i_set), "s_n": len(s_set),
            "i_min": i_min, "i_max": i_max,
            "s_min": s_min, "s_max": s_max,
            "exact": len(exact), "pm1d": len(pm1d), "pm2d": len(pm2d),
        })
        if not exact: no_exact += 1
        if not pm1d:  no_pm1d += 1
        if not pm2d:  no_pm2d += 1

    log(f"Shared pairs: {len(both)}")
    log(f"  Exact (+/-0d) overlap:  {len(both) - no_exact}")
    log(f"  +/-1d overlap:          {len(both) - no_pm1d}")
    log(f"  +/-2d overlap:          {len(both) - no_pm2d}")
    log(f"  NO overlap at +/-2d:    {no_pm2d}")

    # ── 4d: Gap details ──────────────────────────────────────────
    section("4d: Gap analysis for non-overlapping pairs")

    zero_ol = [g for g in gap_details if g["pm2d"] == 0]
    if zero_ol:
        log(f"Pairs with ZERO matches at +/-2d: {len(zero_ol)}")
        for g in zero_ol[:15]:
            if g["s_min"] > g["i_max"]:
                gap = (g["s_min"] - g["i_max"]).days
                note = f"sat starts {gap}d after insitu ends"
            elif g["i_min"] > g["s_max"]:
                gap = (g["i_min"] - g["s_max"]).days
                note = f"insitu starts {gap}d after sat ends"
            else:
                note = "interleaved but no +/-2d match"
            log(f"  {g['key']}: i[{g['i_min'].date()} -> {g['i_max'].date()}] "
                f"s[{g['s_min'].date()} -> {g['s_max'].date()}] {note}")

        some_ol = [g for g in gap_details if 0 < g["pm2d"] <= 5]
        if some_ol:
            log(f"\n  Pairs with 1-5 matches at +/-2d: {len(some_ol)}")
            for g in some_ol[:10]:
                log(f"    {g['key']}: exact={g['exact']}, pm1d={g['pm1d']}, pm2d={g['pm2d']}")
    else:
        log("All shared pairs have some +/-2d overlap.")

    # ── 4e: Variable availability ────────────────────────────────
    section("4e: Satellite variable availability")
    for var in ["Q", "SSC", "SSL"]:
        if var not in satellite.columns:
            log(f"  {var}: column missing")
            continue
        vals = pd.to_numeric(satellite[var], errors="coerce")
        n = int(vals.notna().sum())
        log(f"  {var}: {n}/{len(satellite)} finite")
        if n > 0:
            log(f"       range: {vals.min():.6g} -> {vals.max():.6g}")

    # ── 4f: Resolution & source breakdown ────────────────────────
    section("4f: Data breakdown")
    log("In-situ by resolution:")
    for r, c in insitu["resolution"].value_counts().items():
        log(f"  {r}: {c}")
    log("Satellite by linked_resolution:")
    for r, c in satellite["linked_resolution"].value_counts().items():
        log(f"  {r}: {c}")
    log("Satellite by source:")
    for s, c in satellite["source"].value_counts().items():
        log(f"  {s}: {c}")

    # ── Summary ──────────────────────────────────────────────────
    banner("DIAGNOSTIC SUMMARY")

    if len(both) == 0:
        print("  [FAIL] Zero shared (cluster_uid, resolution) pairs between")
        print("         satellite and in-situ datasets.")
        print()
        print("  >>> ROOT CAUSE: The linkage's linked_cluster_uid+resolution")
        print("      has no overlap with the in-situ cluster_uid+resolution in")
        print("      sed_reference_master.nc.")
        print()
        if s_only:
            print(f"      {len(s_only)} target pairs have satellite but NO in-situ data.")
        if i_only:
            print(f"      {len(i_only)} target pairs have in-situ but NO satellite data.")
    elif no_pm2d == len(both):
        print("  [FAIL] All shared pairs have ZERO temporal overlap at +/-2d.")
        print("  >>> ROOT CAUSE: Satellite and in-situ cover different time periods")
        print("      for the same clusters. See gap report in 4d.")
    elif no_pm2d > 0:
        print(f"  [WARN] {no_pm2d}/{len(both)} shared pairs have no +/-2d overlap.")
        print(f"         {len(both) - no_pm2d} pairs DO have overlap.")
    else:
        print(f"  [PASS] All {len(both)} shared pairs have +/-2d overlap.")
        print(f"         If s11b pairs=0, check downstream filtering.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Temporal overlap diagnostic")
    parser.add_argument("--release-dir", type=Path, default=DEFAULT_RELEASE_DIR)
    parser.add_argument("--linkage-csv", type=Path, default=DEFAULT_LINKAGE_CSV)
    parser.add_argument("--s5-csv", type=Path, default=DEFAULT_S5_CSV)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--workers", type=int,
                        default=max(1, min(32, (os.cpu_count() or 1))))
    args = parser.parse_args()

    release_dir = args.release_dir.expanduser().resolve()
    linkage_csv = args.linkage_csv.expanduser().resolve()
    s5_csv = args.s5_csv.expanduser().resolve()
    source_root = args.source_root.expanduser().resolve()

    print("=" * 72)
    print("  s11b Temporal Overlap Diagnostic")
    print("=" * 72)
    print(f"  Release dir:  {release_dir}")
    print(f"  Linkage CSV:  {linkage_csv}")
    print(f"  S5 CSV:       {s5_csv}")
    print(f"  Source root:  {source_root}")
    print(f"  Workers:      {args.workers}")

    target_pairs, linked = load_linkage_targets(linkage_csv, s5_csv, release_dir)
    insitu = load_insitu_direct(release_dir, target_pairs)
    satellite = load_satellite_direct(source_root, linked, target_pairs, args.workers)

    if insitu.empty:
        print("\nERROR: No in-situ data. Cannot continue.")
        return
    if satellite.empty:
        print("\nERROR: No satellite data. Cannot continue.")
        return

    diagnose(insitu, satellite, target_pairs)


if __name__ == "__main__":
    main()
