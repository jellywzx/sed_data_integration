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
        }
    totals = {}
    values_by_key = {}
    colocated = {}
    extremes = []
    with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
        n_records = netcdf_record_count(ds)
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
            any_present = masks["Q"] | masks["SSC"] | masks["SSL"]
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
    extreme_df = pd.DataFrame(extremes)
    if not extreme_df.empty:
        extreme_df = (
            extreme_df.sort_values(["variable", "value"], ascending=[True, False])
            .groupby(["resolution", "variable"], as_index=False, group_keys=False)
            .head(20)
        )
    return {
        "variable_coverage_by_resolution": pd.DataFrame(coverage_rows),
        "variable_summary_statistics": pd.DataFrame(summary_rows),
        "colocated_variable_coverage": pd.DataFrame(colocated_rows),
        "extreme_value_review_points": extreme_df,
    }


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


def build_variable_stats(ctx, chunk_size: int) -> dict:
    legacy = _scan_master_variable_tables(ctx, chunk_size)
    legacy["variable_coverage_by_resolution_analysis_grade"] = legacy["variable_coverage_by_resolution"].assign(analysis_grade="release_nonmissing") if not legacy["variable_coverage_by_resolution"].empty else pd.DataFrame()
    legacy["variable_summary_statistics_analysis_grade"] = legacy["variable_summary_statistics"].assign(analysis_grade="release_nonmissing") if not legacy["variable_summary_statistics"].empty else pd.DataFrame()
    legacy["colocated_variable_coverage_analysis_grade"] = legacy["colocated_variable_coverage"].assign(analysis_grade="release_nonmissing") if not legacy["colocated_variable_coverage"].empty else pd.DataFrame()
    return {
        "variable_coverage": build_variable_summary(ctx, chunk_size),
        "satellite_variable_by_source": _count_satellite_by_source(ctx, chunk_size),
        **legacy,
    }


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
        "MUST NOT be treated as a complete Q/SSC/SSL time series for any station.**",
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
    by_resolution_analysis = stats.get("variable_coverage_by_resolution_analysis_grade", pd.DataFrame())
    summary = stats.get("variable_summary_statistics", pd.DataFrame())
    summary_analysis = stats.get("variable_summary_statistics_analysis_grade", pd.DataFrame())
    colocated = stats.get("colocated_variable_coverage", pd.DataFrame())
    colocated_analysis = stats.get("colocated_variable_coverage_analysis_grade", pd.DataFrame())
    satellite = stats.get("satellite_variable_by_source", pd.DataFrame())
    extremes = stats.get("extreme_value_review_points", pd.DataFrame())

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
    )
    append_table_section(
        lines,
        "Analysis-Grade Coverage by Resolution",
        by_resolution_analysis,
        columns=[
            "resolution",
            "analysis_grade",
            "n_records_total",
            "Q_record_coverage_pct",
            "SSC_record_coverage_pct",
            "SSL_record_coverage_pct",
        ],
        sort_by="n_records_total",
        max_rows=8,
        note="Analysis-grade rows use the release filter emitted by this module; no non-release QC intermediates are read.",
    )
    append_table_section(
        lines,
        "Variable Summary Statistics",
        summary,
        columns=["resolution", "variable", "n_nonmissing_records", "n_nonmissing_clusters", "mean", "median", "min", "max", "p05", "p95", "p99", "unit"],
        sort_by="n_nonmissing_records",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Analysis-Grade Summary Statistics",
        summary_analysis,
        columns=["resolution", "variable", "analysis_grade", "n_nonmissing_records", "mean", "median", "p05", "p95", "p99", "unit"],
        sort_by="n_nonmissing_records",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Co-Located Variable Coverage",
        colocated,
        columns=["resolution", "combination", "combination_type", "n_records", "n_clusters", "pct_of_all_records", "pct_of_nonempty_records", "pct_of_clusters"],
        sort_by="n_records",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Analysis-Grade Co-Located Coverage",
        colocated_analysis,
        columns=["resolution", "analysis_grade", "combination", "n_records", "n_clusters", "pct_of_nonempty_records", "pct_of_clusters"],
        sort_by="n_records",
        max_rows=18,
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
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- `good_percent` can be misleading when a release intentionally marks derived SSL as estimated; always check `estimated_percent` to distinguish estimated data (acceptable) from truly missing/problematic data. The gap `usable_percent - good_percent` is explained by `estimated_percent`.",
            "- Satellite rows MUST be filtered by source and variable before use because validation-sidecar variable density is source-dependent and highly variable (see Satellite Product Coverage Warning above).",
            "- Extreme review points are candidates for manual inspection, not automatic removal rules.",
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
    for legacy_name in (
        "variable_coverage_by_resolution",
        "variable_coverage_by_resolution_analysis_grade",
        "variable_summary_statistics",
        "variable_summary_statistics_analysis_grade",
        "colocated_variable_coverage",
        "colocated_variable_coverage_analysis_grade",
        "extreme_value_review_points",
    ):
        write_csv(stats[legacy_name], tables_dir / "table_{}.csv".format(legacy_name))
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
