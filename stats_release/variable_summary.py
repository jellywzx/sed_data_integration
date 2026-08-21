#!/usr/bin/env python3
"""Q/SSC/SSL coverage statistics from release NetCDF products."""
# ---- Library path setup: MUST happen before any extension-module imports ----
import os as _os
import ctypes as _ctypes
from pathlib import Path as _Path
_conda_lib = "/share/home/dq134/.conda/envs/wzx/lib"
if _os.path.isdir(_conda_lib):
    _os.environ["LD_LIBRARY_PATH"] = _conda_lib + _os.pathsep + _os.environ.get("LD_LIBRARY_PATH", "")
    try:
        _ctypes.CDLL(str(_Path(_conda_lib) / "libstdc++.so.6"), mode=_ctypes.RTLD_GLOBAL)
    except Exception:
        pass
del _os, _ctypes, _Path, _conda_lib
# ---------------------------------------------------------------------------





import argparse
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import (
    add_common_args,
    context_from_args,
    copy_report_to_docs,
    netcdf_record_count,
    read_numeric_var,
    read_text_var,
    setup_matplotlib,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import numeric_stats, pct, resolution_values
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


VARIABLES = ("Q", "SSC", "SSL")


def _count_product(ctx, file_name: str, label: str, chunk_size: int) -> pd.DataFrame:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(
            [{"product": label, "variable": var, "n_records": 0, "n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0} for var in VARIABLES]
        )
    rows = []
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        counts = {var: {"n_records": n_records, "n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0} for var in VARIABLES}
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            for var in VARIABLES:
                values = read_numeric_var(ds, var, key=slc)
                if values.size == 0:
                    continue
                present = np.isfinite(values)
                counts[var]["n_present"] += int(np.count_nonzero(present))
                flag_name = "{}_flag".format(var)
                if flag_name in ds.variables:
                    flags = np.ma.asarray(ds.variables[flag_name][slc]).filled(9).reshape(-1)
                    counts[var]["n_good"] += int(np.count_nonzero(present & (flags == 0)))
                    counts[var]["n_usable"] += int(np.count_nonzero(present & np.isin(flags, [0, 1])))
                    counts[var]["n_estimated"] += int(np.count_nonzero(present & (flags == 1)))
        for var in VARIABLES:
            row = {"product": label, "variable": var}
            row.update(counts[var])
            n = row["n_records"]
            row["present_percent"] = round(100.0 * row["n_present"] / n, 6) if n else 0.0
            row["good_percent"] = round(100.0 * row["n_good"] / n, 6) if n else 0.0
            row["estimated_percent"] = round(100.0 * row["n_estimated"] / n, 6) if n else 0.0
            row["usable_percent"] = round(100.0 * row["n_usable"] / n, 6) if n else 0.0
            rows.append(row)
    return pd.DataFrame(rows)


def _read_values_for_variable(ctx, file_name: str, var_name: str, chunk_size: int) -> np.ndarray:
    """Read all valid values for a variable from a NetCDF product."""
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return np.asarray([])
    pieces = []
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            values = read_numeric_var(ds, var_name, key=slice(start, stop))
            if values.size == 0:
                continue
            valid = np.isfinite(values)
            pieces.append(values[valid])
    return np.concatenate(pieces) if pieces else np.asarray([])


def build_variable_summary(ctx, chunk_size: int) -> pd.DataFrame:
    frames = [
        _count_product(ctx, PRODUCT_FILES["master_nc"], "master", chunk_size),
        _count_product(ctx, PRODUCT_FILES["climatology_nc"], "climatology", chunk_size),
        _count_product(ctx, PRODUCT_FILES["satellite_nc"], "satellite", chunk_size),
    ]
    return pd.concat(frames, ignore_index=True)


def _scan_master_variable_tables(ctx, chunk_size: int) -> dict:
    path = ctx.require_input(ctx.release_file(PRODUCT_FILES["master_nc"]), required=False)
    if path is None:
        empty = pd.DataFrame()
        return {
            "variable_coverage_by_resolution": empty,
            "variable_summary_statistics": empty,
            "colocated_variable_coverage": empty,
            "extreme_value_review_points": empty,
            "flag01_summary_statistics": empty,
            "zero_value_flag_distribution": empty,
            "flag01_colocated_variable_coverage": empty,
        }
    totals = {}
    values_by_key = {}
    flag01_values_by_key = {}
    zero_flag_counts = {}
    colocated = {}
    flag01_colocated = {}
    extremes = []
    zero_source_audit = {}  # key: (resolution, source)
    with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
        n_records = netcdf_record_count(ds)
        source_values = np.asarray(read_text_var(ds, "source"), dtype=object) if "source" in ds.variables else np.asarray([], dtype=object)
        units = {var: getattr(ds.variables[var], "units", "") if var in ds.variables else "" for var in VARIABLES}
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            res = np.asarray(resolution_values(ds, slc), dtype=object)
            if "station_index" in ds.variables:
                station_idx = np.ma.asarray(ds.variables["station_index"][slc]).filled(-1).astype(int).reshape(-1)
            else:
                station_idx = np.arange(start, stop)
            masks = {}
            vals_by_var = {}
            for var in VARIABLES:
                vals = read_numeric_var(ds, var, key=slc)
                vals = np.asarray(vals).reshape(-1)
                vals_by_var[var] = vals
                masks[var] = np.isfinite(vals)
            flags_by_var = {}
            for var in VARIABLES:
                flag_name = "{}_flag".format(var)
                if flag_name in ds.variables:
                    flags_by_var[var] = np.ma.asarray(ds.variables[flag_name][slc]).filled(9).reshape(-1)
            any_present = masks["Q"] | masks["SSC"] | masks["SSL"]
            # ---- Zero-source audit (SSC only) ----
            ssc_vals = vals_by_var.get("SSC")
            if ssc_vals is not None and ssc_vals.size:
                ssc_zero_mask = masks.get("SSC", np.zeros(0, dtype=bool)) & (ssc_vals == 0)
                if ssc_zero_mask.any() and source_values.size:
                    chunk_source = source_values[start:stop]
                    q_vals = vals_by_var.get("Q")
                    q_mask = masks.get("Q", np.zeros(0, dtype=bool))
                    ssc_flags_arr = flags_by_var.get("SSC")
                    for resolution_str in sorted(set(res)):
                        audit_rmask = res == resolution_str
                        combined = audit_rmask & ssc_zero_mask
                        if not combined.any():
                            continue
                        for src in sorted(set(chunk_source[combined])):
                            src = str(src).strip()
                            if not src:
                                continue
                            src_mask = combined & (chunk_source == src)
                            if not src_mask.any():
                                continue
                            key = (str(resolution_str), src)
                            item = zero_source_audit.setdefault(key, {
                                "ssc_zero_total": 0,
                                "q_zero_ssc_zero": 0,
                                "q_pos_ssc_zero": 0,
                                "direct_ssc_zero": 0,
                                "derived_ssc_zero": 0,
                            })
                            item["ssc_zero_total"] += int(np.count_nonzero(src_mask))
                            item["q_zero_ssc_zero"] += int(np.count_nonzero(src_mask & q_mask & (q_vals == 0)))
                            item["q_pos_ssc_zero"] += int(np.count_nonzero(src_mask & q_mask & (q_vals > 0)))
                            if ssc_flags_arr is not None:
                                item["direct_ssc_zero"] += int(np.count_nonzero(src_mask & (ssc_flags_arr == 0)))
                                item["derived_ssc_zero"] += int(np.count_nonzero(src_mask & (ssc_flags_arr == 1)))
            # ---- End zero-source audit ----
            for resolution in sorted(set(res)):
                resolution = str(resolution)
                rmask = res == resolution
                item = totals.setdefault(
                    resolution,
                    {
                        "n_records_total": 0,
                        "clusters_total": set(),
                        "var_records": {var: 0 for var in VARIABLES},
                        "var_clusters": {var: set() for var in VARIABLES},
                    },
                )
                item["n_records_total"] += int(np.count_nonzero(rmask & any_present))
                item["clusters_total"].update(int(v) for v in station_idx[rmask & any_present] if int(v) >= 0)
                for var in VARIABLES:
                    mask = rmask & masks[var]
                    item["var_records"][var] += int(np.count_nonzero(mask))
                    item["var_clusters"][var].update(int(v) for v in station_idx[mask] if int(v) >= 0)
                    vals = vals_by_var[var][mask]
                    if vals.size:
                        values_by_key.setdefault((resolution, var), []).append(vals.astype("float64"))
                        top_n = min(20, vals.size)
                        idx = np.argpartition(vals, -top_n)[-top_n:]
                        for local in idx:
                            pos = np.flatnonzero(mask)[local]
                            extremes.append(
                                {
                                    "resolution": resolution,
                                    "variable": var,
                                    "value": float(vals_by_var[var][pos]),
                                    "station_index": int(station_idx[pos]),
                                    "record_index": int(start + pos),
                                    "review_reason": "top_high_value",
                                    "unit": units.get(var, ""),
                                }
                            )
                    if var in flags_by_var:
                        flag01_mask = rmask & masks[var] & np.isin(flags_by_var[var], [0, 1])
                        flag01_vals = vals_by_var[var][flag01_mask]
                        if flag01_vals.size:
                            flag01_values_by_key.setdefault((resolution, var), []).append(flag01_vals.astype("float64"))
                        zero_mask = rmask & masks[var] & (vals_by_var[var] == 0)
                        zero_flags = flags_by_var[var][zero_mask]
                        if zero_flags.size:
                            zitem = zero_flag_counts.setdefault((resolution, var), {})
                            for fv in np.unique(zero_flags):
                                zitem[int(fv)] = zitem.get(int(fv), 0) + int(np.count_nonzero(zero_flags == fv))
                combos = {
                    "Q only": masks["Q"] & ~masks["SSC"] & ~masks["SSL"],
                    "SSC only": masks["SSC"] & ~masks["Q"] & ~masks["SSL"],
                    "SSL only": masks["SSL"] & ~masks["Q"] & ~masks["SSC"],
                    "Q+SSC": masks["Q"] & masks["SSC"] & ~masks["SSL"],
                    "Q+SSL": masks["Q"] & masks["SSL"] & ~masks["SSC"],
                    "SSC+SSL": masks["SSC"] & masks["SSL"] & ~masks["Q"],
                    "Q+SSC+SSL": masks["Q"] & masks["SSC"] & masks["SSL"],
                    "Any": any_present,
                }
                for name, cmask0 in combos.items():
                    cmask = rmask & cmask0
                    citem = colocated.setdefault((resolution, name), {"n_records": 0, "clusters": set()})
                    citem["n_records"] += int(np.count_nonzero(cmask))
                    citem["clusters"].update(int(v) for v in station_idx[cmask] if int(v) >= 0)

                # Flag 0–1 co-located variable coverage
                flag01_masks = {}
                for var in VARIABLES:
                    if var in flags_by_var:
                        flag01_masks[var] = masks[var] & np.isin(flags_by_var[var], [0, 1])
                    else:
                        flag01_masks[var] = np.zeros_like(masks[var], dtype=bool)
                flag01_combos = {
                    "Q+SSC+SSL all flag 0\u20131": flag01_masks["Q"] & flag01_masks["SSC"] & flag01_masks["SSL"],
                }
                for name, fcmask0 in flag01_combos.items():
                    fcmask = rmask & fcmask0
                    fcitem = flag01_colocated.setdefault((resolution, name), {"n_records": 0, "clusters": set()})
                    fcitem["n_records"] += int(np.count_nonzero(fcmask))
                    fcitem["clusters"].update(int(v) for v in station_idx[fcmask] if int(v) >= 0)

    coverage_rows = []
    for resolution, item in sorted(totals.items()):
        total_records = int(item["n_records_total"])
        total_clusters = len(item["clusters_total"])
        row = {"resolution": resolution, "n_records_total": total_records, "n_clusters_total": total_clusters}
        for var in VARIABLES:
            records = int(item["var_records"][var])
            clusters = len(item["var_clusters"][var])
            row["{}_records".format(var)] = records
            row["{}_clusters".format(var)] = clusters
            row["{}_record_coverage_pct".format(var)] = pct(records, total_records)
            row["{}_cluster_coverage_pct".format(var)] = pct(clusters, total_clusters)
        coverage_rows.append(row)
    summary_rows = []
    for (resolution, var), pieces in sorted(values_by_key.items()):
        vals = np.concatenate(pieces) if pieces else np.asarray([])
        stats = numeric_stats(vals)
        summary_rows.append(
            {
                "resolution": resolution,
                "variable": var,
                "n_nonmissing_records": int(vals.size),
                "n_nonmissing_clusters": len(totals.get(resolution, {}).get("var_clusters", {}).get(var, set())),
                **stats,
                "unit": units.get(var, ""),
            }
        )
    flag01_summary_rows = []
    for (resolution, var), pieces in sorted(flag01_values_by_key.items()):
        vals = np.concatenate(pieces) if pieces else np.asarray([])
        stats = numeric_stats(vals)
        flag01_summary_rows.append(
            {
                "resolution": resolution,
                "variable": var,
                "n_flag01_records": int(vals.size),
                "mean": stats["mean"],
                "median": stats["median"],
                "p05": stats["p05"],
                "p95": stats["p95"],
                "p99": stats["p99"],
                "unit": units.get(var, ""),
            }
        )
    from stats_release.common_stats import FLAG_MEANINGS
    zero_flag_dist_rows = []
    for (resolution, var), zcounts in sorted(zero_flag_counts.items()):
        for fv in sorted(zcounts.keys()):
            zero_flag_dist_rows.append({
                "resolution": resolution,
                "variable": var,
                "flag_value": fv,
                "flag_meaning": FLAG_MEANINGS.get(fv, "unknown"),
                "n_zero": zcounts[fv],
                "unit": units.get(var, ""),
            })
    colocated_rows = []
    for (resolution, combo), item in sorted(colocated.items()):
        total_records = totals.get(resolution, {}).get("n_records_total", 0)
        total_clusters = len(totals.get(resolution, {}).get("clusters_total", set()))
        any_records = colocated.get((resolution, "Any"), {}).get("n_records", 0)
        colocated_rows.append(
            {
                "resolution": resolution,
                "combination": combo,
                "combination_type": "any" if combo == "Any" else "exact",
                "definition": combo,
                "n_records": int(item["n_records"]),
                "n_clusters": len(item["clusters"]),
                "pct_of_all_records": pct(item["n_records"], total_records),
                "pct_of_nonempty_records": pct(item["n_records"], any_records),
                "pct_of_clusters": pct(len(item["clusters"]), total_clusters),
            }
        )

    # Flag 0–1 co-located variable coverage rows
    flag01_colocated_rows = []
    for (resolution, combo), item in sorted(flag01_colocated.items()):
        total_records = totals.get(resolution, {}).get("n_records_total", 0)
        total_clusters = len(totals.get(resolution, {}).get("clusters_total", set()))
        flag01_colocated_rows.append(
            {
                "resolution": resolution,
                "combination": combo,
                "n_records": int(item["n_records"]),
                "pct_records": pct(item["n_records"], total_records),
                "n_clusters": len(item["clusters"]),
            }
        )
    extreme_df = pd.DataFrame(extremes)
    if not extreme_df.empty:
        extreme_df = (
            extreme_df.sort_values(["variable", "value"], ascending=[True, False])
            .groupby(["resolution", "variable"], as_index=False, group_keys=False)
            .head(20)
        )
    zero_source_audit_rows = []
    for (resolution, source), counts in sorted(zero_source_audit.items()):
        zero_source_audit_rows.append({
            "resolution": resolution,
            "source": source,
            "SSC=0 total": counts["ssc_zero_total"],
            "Q=0 & SSC=0": counts["q_zero_ssc_zero"],
            "Q>0 & SSC=0": counts["q_pos_ssc_zero"],
            "direct SSC=0": counts["direct_ssc_zero"],
            "derived SSC=0": counts["derived_ssc_zero"],
        })
    return {
        "variable_coverage_by_resolution": pd.DataFrame(coverage_rows),
        "variable_summary_statistics": pd.DataFrame(summary_rows),
        "colocated_variable_coverage": pd.DataFrame(colocated_rows),
        "flag01_colocated_variable_coverage": pd.DataFrame(flag01_colocated_rows),
        "extreme_value_review_points": extreme_df,
        "flag01_summary_statistics": pd.DataFrame(flag01_summary_rows),
        "zero_value_flag_distribution": pd.DataFrame(zero_flag_dist_rows),
        "zero_source_audit": pd.DataFrame(zero_source_audit_rows),
    }


def _scan_monthly_flag01_source_variable(ctx, chunk_size: int) -> pd.DataFrame:
    """Monthly-resolution source x variable statistics restricted to Flag 0-1.

    Each (source, variable) group is further split by SSC_flag: 0 (direct
    measurement), 1 (derived/estimated), and the combined Flag 0-1 subset.
    """
    path = ctx.require_input(ctx.release_file(PRODUCT_FILES["master_nc"]), required=False)
    if path is None:
        return pd.DataFrame()
    values_by_key = {}
    units = {var: "" for var in VARIABLES}
    with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
        n_records = netcdf_record_count(ds)
        source_values = np.asarray(read_text_var(ds, "source"), dtype=object) if "source" in ds.variables else np.asarray([], dtype=object)
        units = {var: getattr(ds.variables[var], "units", "") if var in ds.variables else "" for var in VARIABLES}
        has_ssc_flag = "SSC_flag" in ds.variables
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            res = np.asarray(resolution_values(ds, slc), dtype=object)
            monthly_mask = res == "monthly"
            if not monthly_mask.any():
                continue
            chunk_source = source_values[start:stop]
            if has_ssc_flag:
                ssc_flag = np.ma.asarray(ds.variables["SSC_flag"][slc]).filled(9).reshape(-1)
            else:
                ssc_flag = np.full(stop - start, 9, dtype=int)
            for var in VARIABLES:
                vals = np.asarray(read_numeric_var(ds, var, key=slc)).reshape(-1)
                present = np.isfinite(vals)
                flag_name = "{}_flag".format(var)
                flags = np.ma.asarray(ds.variables[flag_name][slc]).filled(9).reshape(-1) if flag_name in ds.variables else np.full(stop - start, 9)
                base_mask = monthly_mask & present & np.isin(flags, [0, 1])
                if not base_mask.any():
                    continue
                for src in sorted(set(chunk_source[base_mask])):
                    src = str(src).strip()
                    if not src:
                        continue
                    src_mask = base_mask & (chunk_source == src)
                    values_by_key.setdefault((src, var, "0-1"), []).append(vals[src_mask].astype("float64"))
                    for ssc_val in (0, 1):
                        sub_mask = src_mask & (ssc_flag == ssc_val)
                        if sub_mask.any():
                            values_by_key.setdefault((src, var, str(ssc_val)), []).append(vals[sub_mask].astype("float64"))
    rows = []
    for (source, var, ssc_flag), pieces in sorted(values_by_key.items()):
        vals = np.concatenate(pieces) if pieces else np.asarray([])
        stats = numeric_stats(vals)
        rows.append(
            {
                "source": source,
                "variable": var,
                "ssc_flag": ssc_flag,
                "n": int(vals.size),
                "median": stats["median"],
                "p05": stats["p05"],
                "p95": stats["p95"],
                "p99": stats["p99"],
                "min": stats["min"],
                "max": stats["max"],
                "unit": units.get(var, ""),
            }
        )
    return pd.DataFrame(rows)


def _count_satellite_by_source(ctx, chunk_size: int) -> pd.DataFrame:
    file_name = PRODUCT_FILES["satellite_nc"]
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame()
    counts = {}
    with ctx.open_dataset(file_name, required=True) as ds:
        if "source" not in ds.variables or "satellite_station_index" not in ds.variables:
            return pd.DataFrame()
        station_sources = np.asarray(read_text_var(ds, "source"), dtype=object)
        n_records = netcdf_record_count(ds)
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            station_idx = np.ma.asarray(ds.variables["satellite_station_index"][slc]).filled(-1).astype(int).reshape(-1)
            source_values = np.asarray([""] * len(station_idx), dtype=object)
            valid_idx = (station_idx >= 0) & (station_idx < len(station_sources))
            source_values[valid_idx] = station_sources[station_idx[valid_idx]]
            for var in VARIABLES:
                values = read_numeric_var(ds, var, key=slc)
                if values.size == 0:
                    continue
                present = np.isfinite(values)
                flag_name = "{}_flag".format(var)
                flags = np.ma.asarray(ds.variables[flag_name][slc]).filled(9).reshape(-1) if flag_name in ds.variables else np.full(values.shape, 9)
                for source in sorted(set(source_values)):
                    if not source:
                        continue
                    mask = source_values == source
                    key = (source, var)
                    item = counts.setdefault(key, {"n_records": 0, "n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0})
                    item["n_records"] += int(np.count_nonzero(mask))
                    item["n_present"] += int(np.count_nonzero(mask & present))
                    item["n_good"] += int(np.count_nonzero(mask & present & (flags == 0)))
                    item["n_usable"] += int(np.count_nonzero(mask & present & np.isin(flags, [0, 1])))
                    item["n_estimated"] += int(np.count_nonzero(mask & present & (flags == 1)))
    rows = []
    for (source, var), item in sorted(counts.items()):
        row = {"product": "satellite", "source_name": source, "variable": var}
        row.update(item)
        n = row["n_records"]
        row["present_percent"] = round(100.0 * row["n_present"] / n, 6) if n else 0.0
        row["good_percent"] = round(100.0 * row["n_good"] / n, 6) if n else 0.0
        row["estimated_percent"] = round(100.0 * row["n_estimated"] / n, 6) if n else 0.0
        row["usable_percent"] = round(100.0 * row["n_usable"] / n, 6) if n else 0.0
        rows.append(row)
    return pd.DataFrame(rows)


def _count_master_source_variables(ctx, chunk_size: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Master-product per-source variable coverage and value distribution.

    Returns (coverage_df, values_df, flag01_values_df):
      - coverage_df: one row per (source_name, variable); n_records/n_present/
        n_good/n_estimated/n_usable + percentages (mirrors _count_satellite_by_source).
      - values_df: one row per (source, variable); value-distribution stats over
        ALL finite values (all resolutions, flags 0-8).
      - flag01_values_df: like values_df but restricted to flag 0-1 values only.
    """
    file_name = PRODUCT_FILES["master_nc"]
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    coverage = {}
    values_by_key = {}
    flag01_values_by_key = {}
    units = {var: "" for var in VARIABLES}
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = netcdf_record_count(ds)
        source_values = (
            np.asarray(read_text_var(ds, "source"), dtype=object)
            if "source" in ds.variables
            else np.asarray([], dtype=object)
        )
        units = {var: getattr(ds.variables[var], "units", "") if var in ds.variables else "" for var in VARIABLES}
        for start in range(0, n_records, chunk_size):
            stop = min(start + chunk_size, n_records)
            slc = slice(start, stop)
            chunk_source = source_values[start:stop]
            for var in VARIABLES:
                values = np.asarray(read_numeric_var(ds, var, key=slc)).reshape(-1)
                if values.size == 0:
                    continue
                present = np.isfinite(values)
                flag_name = "{}_flag".format(var)
                flags = (
                    np.ma.asarray(ds.variables[flag_name][slc]).filled(9).reshape(-1)
                    if flag_name in ds.variables
                    else np.full(values.shape, 9)
                )
                for raw in sorted(set(chunk_source)):
                    src = str(raw).strip()
                    if not src:
                        continue
                    mask = chunk_source == raw  # compare raw element; label with stripped src
                    item = coverage.setdefault(
                        (src, var),
                        {"n_records": 0, "n_present": 0, "n_good": 0, "n_estimated": 0, "n_usable": 0},
                    )
                    item["n_records"] += int(np.count_nonzero(mask))
                    item["n_present"] += int(np.count_nonzero(mask & present))
                    item["n_good"] += int(np.count_nonzero(mask & present & (flags == 0)))
                    item["n_usable"] += int(np.count_nonzero(mask & present & np.isin(flags, [0, 1])))
                    item["n_estimated"] += int(np.count_nonzero(mask & present & (flags == 1)))
                    vals = values[mask & present]
                    if vals.size:
                        values_by_key.setdefault((src, var), []).append(vals.astype("float64"))
                    flag01_vals = values[mask & present & np.isin(flags, [0, 1])]
                    if flag01_vals.size:
                        flag01_values_by_key.setdefault((src, var), []).append(flag01_vals.astype("float64"))

    coverage_rows = []
    for (source, var), item in sorted(coverage.items()):
        row = {"product": "master", "source_name": source, "variable": var}
        row.update(item)
        n = row["n_records"]
        row["present_percent"] = round(100.0 * row["n_present"] / n, 6) if n else 0.0
        row["good_percent"] = round(100.0 * row["n_good"] / n, 6) if n else 0.0
        row["estimated_percent"] = round(100.0 * row["n_estimated"] / n, 6) if n else 0.0
        row["usable_percent"] = round(100.0 * row["n_usable"] / n, 6) if n else 0.0
        coverage_rows.append(row)

    value_rows = []
    for (source, var), pieces in sorted(values_by_key.items()):
        vals = np.concatenate(pieces) if pieces else np.asarray([])
        stats = numeric_stats(vals)
        value_rows.append({
            "source": source,
            "variable": var,
            "n": int(vals.size),
            "mean": stats["mean"],
            "median": stats["median"],
            "min": stats["min"],
            "max": stats["max"],
            "p05": stats["p05"],
            "p95": stats["p95"],
            "p99": stats["p99"],
            "unit": units.get(var, ""),
        })

    flag01_value_rows = []
    for (source, var), pieces in sorted(flag01_values_by_key.items()):
        vals = np.concatenate(pieces) if pieces else np.asarray([])
        stats = numeric_stats(vals)
        flag01_value_rows.append({
            "source": source,
            "variable": var,
            "n": int(vals.size),
            "mean": stats["mean"],
            "median": stats["median"],
            "min": stats["min"],
            "max": stats["max"],
            "p05": stats["p05"],
            "p95": stats["p95"],
            "p99": stats["p99"],
            "unit": units.get(var, ""),
        })

    return pd.DataFrame(coverage_rows), pd.DataFrame(value_rows), pd.DataFrame(flag01_value_rows)


def build_variable_stats(ctx, chunk_size: int) -> dict:
    legacy = _scan_master_variable_tables(ctx, chunk_size)
    master_coverage, master_values, master_values_flag01 = _count_master_source_variables(ctx, chunk_size)
    result = {
        "variable_coverage": build_variable_summary(ctx, chunk_size),
        "satellite_variable_by_source": _count_satellite_by_source(ctx, chunk_size),
        "monthly_flag01_source_variable": _scan_monthly_flag01_source_variable(ctx, chunk_size),
        "master_variable_by_source": master_coverage,
        "master_source_variable_values": master_values,
        "master_source_variable_values_flag01": master_values_flag01,
        **legacy,
    }
    return result


def write_figures(ctx, figures_dir: Path, dpi: int, chunk_size: int) -> None:
    """Write variable distribution figures."""
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)

    colors = {"master": "#4c78a8", "climatology": "#e45756", "satellite": "#f58518"}
    product_keys = [("master", "master_nc"), ("climatology", "climatology_nc"), ("satellite", "satellite_nc")]

    for var_name in VARIABLES:
        use_log = var_name in ("SSC", "SSL")
        fig, ax = plt.subplots(figsize=(7.2, 4.0))
        any_data = False
        for label, pkey in product_keys:
            values = _read_values_for_variable(ctx, PRODUCT_FILES[pkey], var_name, chunk_size)
            if values.size == 0:
                continue
            any_data = True
            if use_log:
                values = values[values > 0]
                if values.size == 0:
                    continue
                values = np.log10(values)
            ax.hist(values, bins=80, density=True, histtype="step", linewidth=1.5,
                    color=colors.get(label, "#333333"), label="{} (n={:,})".format(label, len(values)))
        if not any_data:
            ax.text(0.5, 0.5, "No valid {} values".format(var_name), ha="center", va="center", transform=ax.transAxes)
        else:
            xlabel = "log10({})".format(var_name) if use_log else var_name
            ax.set_xlabel(xlabel)
            ax.set_ylabel("Density")
            ax.set_title("Distribution of {}".format(var_name))
            ax.legend(frameon=False, fontsize=8)
            ax.grid(True, alpha=0.3)
        fig.tight_layout()
        fig.savefig(figures_dir / "fig_{}_distribution.png".format(var_name), dpi=dpi)
        plt.close(fig)


def build_satellite_coverage_warning(
    satellite_df: pd.DataFrame,
    coverage_df: pd.DataFrame,
) -> list[str]:
    """Build a dedicated warning section for the satellite product's sparse variable coverage.

    The satellite product concatenates observations from multiple sources that each
    cover different subsets of Q/SSC/SSL.  A user reading the Q or SSL variable
    directly from sed_reference_satellite.nc will encounter >99 % NaN.
    """
    lines = [
        "",
        "## Satellite Product Coverage Warning",
        "",
        "**The satellite product (``sed_reference_satellite.nc``) is validation-only and "
        "should not be interpreted as a uniformly complete Q\u2013SSC\u2013SSL time-series product because variable availability differs among source datasets.**",
        "",
        "It concatenates records from multiple independent satellite-derived sources "
        "(Dethier, GSED, RiverSed) that each cover different variables.  Reading a "
        "variable column (e.g. ``Q`` or ``SSL``) directly from the file will return "
        "mostly NaN because the source that produced those rows does not carry that variable.",
        "",
    ]

    # Per-source variable summary
    if not satellite_df.empty and "source_name" in satellite_df.columns:
        _srcs = satellite_df["source_name"].unique()
        lines.append("### Per-source variable availability")
        lines.append("")
        for src in sorted(_srcs):
            sub = satellite_df[satellite_df["source_name"] == src]
            lines.append("- **{}**: ".format(src))
            for _, r in sub.iterrows():
                pct = str(r.get("present_percent", ""))
                lines.append("  - {}: {} present ({})".format(r["variable"], fmt_int(r.get("n_present", 0)), pct))
        lines.append("")

    # Overall product-level warning numbers
    if not coverage_df.empty:
        sat = coverage_df[coverage_df["product"] == "satellite"]
        if not sat.empty:
            lines.append("### Product-level summary")
            lines.append("")
            lines.append("| variable | total records | n present | present % |")
            lines.append("|---|---|---|---|")
            for _, r in sat.iterrows():
                lines.append(
                    "| {} | {} | {} | {:.3f}% |".format(
                        r["variable"],
                        fmt_int(r.get("n_records", 0)),
                        fmt_int(r.get("n_present", 0)),
                        float(r.get("present_percent", 0)),
                    )
                )
            lines.append("")

    lines.extend(
        [
            "### Recommended usage",
            "",
            "1. **Always filter by source before reading variable values.** "
            "Join the satellite file with ``satellite_catalog.csv`` on "
            "``satellite_station_uid`` to resolve the ``source`` name for each row.",
            "2. **Filter rows where the target variable is present for that source:**",
            "",
            "   ```python",
            "   # Python / xarray example \u2014 keep only non-missing SSC",
            "   ds = xr.open_dataset('sed_reference_satellite.nc')",
            "   ssc_valid = ds['SSC'].where(ds['SSC'].notnull())",
            "",
            "   # Or filter by source \u00d7 variable combination in pandas",
            "   df = ds.to_dataframe()",
            "   # Keep only Dethier rows for Q, GSED+RiverSed rows for SSC, etc.",
            "   dethier_q = df[df['source'] == 'Dethier'][['Q']].dropna()",
            "   gsed_ssc  = df[df['source'] == 'GSED'][['SSC']].dropna()",
            "   ```",
            "",
            "3. **Use ``usable_percent`` as a guidance threshold.**  For any "
            "source \u00d7 variable combination with ``present_percent < 1 %``, "
            "treat the column as effectively empty for that source.",
            "4. **Do not use ``sed_reference_satellite.nc`` as input to model training "
            "or as a continuous forcing dataset.**  It is designed for cross-validation "
            "between satellite retrievals and in-situ reference records.",
            "",
        ]
    )

    return lines


def build_detailed_variable_report(ctx, stats: dict, tables_dir: Path, figures_dir: Path, report_dir: Path) -> list[str]:
    coverage = stats.get("variable_coverage", pd.DataFrame())
    by_resolution = stats.get("variable_coverage_by_resolution", pd.DataFrame())
    summary = stats.get("variable_summary_statistics", pd.DataFrame())
    colocated = stats.get("colocated_variable_coverage", pd.DataFrame())
    satellite = stats.get("satellite_variable_by_source", pd.DataFrame())
    extremes = stats.get("extreme_value_review_points", pd.DataFrame())
    flag01_summary = stats.get("flag01_summary_statistics", pd.DataFrame())
    zero_flag_dist = stats.get("zero_value_flag_distribution", pd.DataFrame())
    zero_source_audit = stats.get("zero_source_audit", pd.DataFrame())
    flag01_colocated = stats.get("flag01_colocated_variable_coverage", pd.DataFrame())
    monthly_flag01_source_var = stats.get("monthly_flag01_source_variable", pd.DataFrame())
    master_by_source = stats.get("master_variable_by_source", pd.DataFrame())
    master_source_values = stats.get("master_source_variable_values", pd.DataFrame())
    master_source_values_flag01 = stats.get("master_source_variable_values_flag01", pd.DataFrame())

    total_products = coverage["product"].nunique() if not coverage.empty and "product" in coverage.columns else 0
    total_records = pd.to_numeric(coverage.get("n_records", 0), errors="coerce").fillna(0).sum() if not coverage.empty else 0
    low_satellite = pd.DataFrame()
    if not satellite.empty and "present_percent" in satellite.columns:
        low_satellite = satellite[pd.to_numeric(satellite["present_percent"], errors="coerce").fillna(0).lt(1)].copy()

    lines = [
        "# Variable Coverage Results Report",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Variables covered: Q, SSC, SSL.",
        "",
        "## Headline",
        "",
        "- Product groups summarized: {}".format(fmt_int(total_products)),
        "- Product-variable denominator rows: {}".format(fmt_int(total_records)),
        "- Satellite source-variable rows with less than 1% present values: {}".format(fmt_int(len(low_satellite))),
        "- Extreme review points emitted: {}".format(fmt_int(len(extremes))),
        "",
        "## Product by Variable Coverage",
        "",
        sorted_markdown_table(
            coverage,
            columns=["product", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "good_percent", "estimated_percent", "usable_percent"],
            max_rows=18,
        ),
    ]
    append_table_section(
        lines,
        "Matrix Coverage by Resolution",
        by_resolution,
        columns=[
            "resolution",
            "n_records_total",
            "n_clusters_total",
            "Q_records",
            "Q_record_coverage_pct",
            "SSC_records",
            "SSC_record_coverage_pct",
            "SSL_records",
            "SSL_record_coverage_pct",
        ],
        sort_by="n_records_total",
        max_rows=8,
        note="Includes all finite (non-NaN) values regardless of quality flag (flags 0–8). Does not filter to flags 0–3.",
    )
    append_table_section(
        lines,
        "Variable Summary Statistics",
        summary,
        columns=["resolution", "variable", "n_nonmissing_records", "n_nonmissing_clusters", "mean", "median", "min", "max", "p05", "p95", "p99", "unit"],
        sort_by="n_nonmissing_records",
        max_rows=18,
        note="Statistics computed on all finite (non-NaN) values regardless of quality flag (flags 0–8).",
    )
    append_table_section(
        lines,
        "Flag 0–1 Summary Statistics (master product)",
        flag01_summary,
        columns=["resolution", "variable", "n_flag01_records", "mean", "median", "p05", "p95", "p99", "unit"],
        sort_by="n_flag01_records",
        max_rows=18,
        note="Statistics computed only on values where the variable flag is 0 (good) or 1 (estimated/derived).",
    )
    append_table_section(
        lines,
        "Zero-Value Flag Distribution (master product)",
        zero_flag_dist,
        columns=["resolution", "variable", "flag_value", "flag_meaning", "n_zero", "unit"],
        sort_by="n_zero",
        max_rows=24,
        note="For exact-zero variable values, the distribution of their associated flags.",
    )
    append_table_section(
        lines,
        "Zero-Source Audit (master product)",
        zero_source_audit,
        columns=["resolution", "source", "SSC=0 total", "Q=0 & SSC=0", "Q>0 & SSC=0", "direct SSC=0", "derived SSC=0"],
        sort_by="SSC=0 total",
        max_rows=60,
        note="SSC=0 records broken down by resolution x source, paired Q status, and direct (flag=0) vs derived (flag=1).",
    )
    append_table_section(
        lines,
        "Monthly Flag 0–1 Source × Variable Statistics (master product)",
        monthly_flag01_source_var,
        columns=["source", "variable", "ssc_flag", "n", "median", "p05", "p95", "p99", "min", "max", "unit"],
        sort_by="n",
        max_rows=60,
        note="Monthly-resolution records only. Values restricted to the variable's own flag 0 or 1. `ssc_flag` splits each source × variable into 0 (direct measurement), 1 (derived/estimated), and 0-1 (combined Flag 0-1 subset).",
    )
    append_table_section(
        lines,
        "Master Source by Variable",
        master_by_source,
        columns=["source_name", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "good_percent", "estimated_percent", "usable_percent"],
        sort_by="n_records",
        max_rows=60,
        note="Master product (`sed_reference_master.nc`). Presence counts include all finite values regardless of flag; `n_good`/`n_estimated` use the variable's own flag 0/1. A 0-present row means that source does not provide that variable.",
    )
    append_table_section(
        lines,
        "Master Source \u00d7 Variable Value Distribution",
        master_source_values,
        columns=["source", "variable", "n", "mean", "median", "min", "max", "p05", "p95", "p99", "unit"],
        sort_by="n",
        max_rows=60,
        note="Master product. Value statistics over all finite values (all resolutions, flags 0\u20138), complementing the monthly Flag 0\u20131 table above.",
    )
    append_table_section(
        lines,
        "Master Source \u00d7 Variable Value Distribution (Flag 0\u20131)",
        master_source_values_flag01,
        columns=["source", "variable", "n", "mean", "median", "min", "max", "p05", "p95", "p99", "unit"],
        sort_by="n",
        max_rows=60,
        note="Master product. Value statistics over finite values restricted to the variable's own flag 0 (good) or 1 (derived/estimated), all resolutions.",
    )
    append_table_section(
        lines,
        "Co-Located Variable Coverage",
        colocated,
        columns=["resolution", "combination", "combination_type", "n_records", "n_clusters", "pct_of_all_records", "pct_of_nonempty_records", "pct_of_clusters"],
        sort_by="n_records",
        max_rows=18,
        note="Co-location counts include all finite (non-NaN) values regardless of quality flag (flags 0–8).",
    )
    append_table_section(
        lines,
        "Flag 0–1 Co-Located Variable Coverage (master product)",
        flag01_colocated,
        columns=["resolution", "combination", "n_records", "pct_records", "n_clusters"],
        sort_by="n_records",
        max_rows=8,
        note="Only records where Q, SSC, and SSL all have flag 0 (good) or 1 (estimated/derived) simultaneously.",
    )
    append_table_section(
        lines,
        "Satellite Source by Variable",
        satellite,
        columns=["source_name", "variable", "n_records", "n_present", "n_good", "n_estimated", "n_usable", "present_percent", "good_percent", "estimated_percent", "usable_percent"],
        sort_by="n_records",
        max_rows=18,
        note="Validation-only satellite products may contain many rows with no Q or SSL values; keep this table near any satellite analysis.",
    )
    append_table_section(
        lines,
        "Satellite Low-Coverage Rows",
        low_satellite,
        columns=["source_name", "variable", "n_records", "n_present", "present_percent", "usable_percent"],
        sort_by="n_records",
        max_rows=18,
    )

    # ---- Satellite coverage warning section ----
    _sat_summary = build_satellite_coverage_warning(satellite, coverage)
    lines.extend(_sat_summary)

    append_table_section(
        lines,
        "Extreme Value Review Points",
        extremes,
        columns=["resolution", "variable", "value", "station_index", "record_index", "review_reason", "unit"],
        sort_by="value",
        max_rows=20,
        note="Extreme values selected from all finite (non-NaN) records regardless of quality flag.",
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- `good_percent` can be misleading when a release intentionally marks derived SSL as estimated; always check `estimated_percent` to distinguish estimated data (acceptable) from truly missing/problematic data. The gap `usable_percent - good_percent` is explained by `estimated_percent`.",
            "- Satellite rows MUST be filtered by source and variable before use because validation-sidecar variable density is source-dependent and highly variable (see Satellite Product Coverage Warning above).",
            "- Cluster percentages across variable combinations are non-exclusive because the same cluster may contain different variable combinations at different time steps.",
            "- Extreme review points are candidates for manual inspection, not automatic removal rules.",
        "",
        "### Quality Flag Reference",
        "",
        "| Flag | Meaning |",
        "|------|---------|",
        "| 0 | good (direct measurement) |",
        "| 1 | derived / estimated |",
        "| 2 | suspect |",
        "| 3 | bad |",
        "| 8 | not checked |",
        "| 9 | missing (NaN in data variables) |",
        "",
        "Tables without \"Flag\" in the title (Matrix Coverage, Summary Statistics, Co-Located Coverage, Extreme Values) include **all finite values regardless of flag** (flags 0\u20138). Use the Flag 0\u20131 and Zero-Value Flag Distribution tables to assess data quality.",
        "",
        ]
    )
    append_figure_index(lines, figures_dir, report_dir)
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only variable coverage statistics.")
    add_common_args(parser, "variable_summary")
    parser.add_argument("--chunk-size", type=int, default=500000)
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    chunk_size = max(1, int(args.chunk_size))
    stats = build_variable_stats(ctx, chunk_size)
    for name, frame in stats.items():
        write_csv(frame, tables_dir / "table_{}.csv".format(name))
    out_csv = tables_dir / "table_variable_coverage.csv"
    if not args.skip_figures:
        try:
            write_figures(ctx, ctx.figures_dir(), max(72, int(args.dpi)), chunk_size)
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    md_path = ctx.output_path("reports", "variable_coverage_summary.md")
    report_lines = build_detailed_variable_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("variable_coverage_results_report_ESSD.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote variable summary to {}".format(out_csv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
