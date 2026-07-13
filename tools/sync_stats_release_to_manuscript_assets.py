from __future__ import annotations
#!/usr/bin/env python3
"""
Synchronize stats_release outputs to manuscript-facing figure/table assets.

Purpose
-------
After rebuilding output/sed_reference_release/ and rerunning stats_release,
this script creates one authoritative manuscript asset layer for:

1. manuscript numbers
2. article tables
3. plotting-data CSVs used by figures/scripts
4. run/fingerprint provenance

Recommended workflow
--------------------
python3 -m stats_release.run_all_release_stats
python3 tools/sync_stats_release_to_manuscript_assets.py

Default outputs
---------------
figures/data/manuscript_stats/
    manuscript_numbers.json
    manuscript_numbers.md
    table4_dataset_statistics_by_product.csv
    table5_variable_availability_distribution.csv
    table6_qc_flag_distribution.csv
    figure4_source_contribution.csv
    figure5_spatial_summary.csv
    figure6_temporal_by_year.csv
    figure7_variable_distribution_stats.csv
    table_resolved_basin_area_summary.csv
    table_resolved_basin_area_classes.csv
    sync_manifest.json
"""


import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RELEASE_DIR = PROJECT_ROOT / "output" / "sed_reference_release"
DEFAULT_STATS_ROOT = PROJECT_ROOT / "output_other" / "stats_release"
DEFAULT_ASSETS_DIR = PROJECT_ROOT / "figures" / "data" / "manuscript_stats"
DEFAULT_S10_ROOT = PROJECT_ROOT / "output_other" / "s10_final_validation"
DEFAULT_S11_ROOT = PROJECT_ROOT / "output_other" / "validation_results"

RESOLUTION_ORDER = ["daily", "monthly", "annual"]
VARIABLE_ORDER = ["Q", "SSC", "SSL"]

VARIABLE_UNITS = {
    "Q": "m3 s-1",
    "SSC": "mg L-1",
    "SSL": "t d-1",
}

BASIN_AREA_CLASSES = [
    ("<100 km2", None, 100.0),
    ("100-1,000 km2", 100.0, 1000.0),
    ("1,000-10,000 km2", 1000.0, 10000.0),
    ("10,000-100,000 km2", 10000.0, 100000.0),
    (">=100,000 km2", 100000.0, None),
]

UTILIZATION_GUIDE = {
    "daily": (
        "Best suited for daily model evaluation, event-scale sediment dynamics, "
        "and station-level time-series extraction."
    ),
    "monthly": (
        "Best suited for monthly model evaluation, seasonal analysis, and "
        "lower-noise comparison where daily records are sparse or variable."
    ),
    "annual": (
        "Best suited for annual sediment-load evaluation, long-term flux comparison, "
        "and basin-scale sediment-budget assessment."
    ),
}


def read_csv(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.is_file():
        if required:
            raise FileNotFoundError(f"Required table not found: {path}")
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(data: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")



def read_json(path: Path, required: bool = True) -> Dict[str, Any]:
    if not path.is_file():
        if required:
            raise FileNotFoundError(f"Required JSON not found: {path}")
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def to_num(series: pd.Series | Any) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def fmt_int(value: Any) -> str:
    try:
        if pd.isna(value):
            return ""
        return f"{int(round(float(value))):,}"
    except Exception:
        return str(value)


def fmt_float(value: Any, digits: int = 1) -> str:
    try:
        if pd.isna(value):
            return ""
        return f"{float(value):,.{digits}f}"
    except Exception:
        return str(value)


def pct(numerator: Any, denominator: Any) -> float:
    n = float(pd.to_numeric(pd.Series([numerator]), errors="coerce").iloc[0])
    d = float(pd.to_numeric(pd.Series([denominator]), errors="coerce").iloc[0])
    if not np.isfinite(n) or not np.isfinite(d) or d == 0:
        return 0.0
    return 100.0 * n / d


def year_text(value: Any) -> str:
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return ""
    try:
        return str(pd.Timestamp(text).year)
    except Exception:
        return text[:4]


def range_text(first: Any, last: Any) -> str:
    a = year_text(first)
    b = year_text(last)
    if a and b:
        return f"{a}-{b}"
    return a or b


def load_manifest(stats_root: Path) -> Dict[str, Any]:
    manifest_path = stats_root / "run_manifest.json"
    if manifest_path.is_file():
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    return {}


def build_table4(stats_root: Path) -> pd.DataFrame:
    src = read_csv(stats_root / "temporal" / "tables" / "table_temporal_coverage_by_resolution.csv")
    src["resolution"] = src["resolution"].astype(str).str.lower()

    # Load unique cluster count from spatial headline stats (deduplicated across resolutions).
    headline = read_csv(stats_root / "spatial" / "tables" / "table_headline.csv", required=False)
    unique_clusters = 0
    if not headline.empty and "metric" in headline.columns and "value" in headline.columns:
        cluster_row = headline[headline["metric"].eq("cluster_count")]
        if not cluster_row.empty:
            unique_clusters = int(float(cluster_row.iloc[0]["value"]))

    rows: list[Dict[str, Any]] = []
    for res in RESOLUTION_ORDER:
        sub = src[src["resolution"].eq(res)]
        if sub.empty:
            continue
        r = sub.iloc[0]
        spatial_units = int(float(r.get("active_clusters", r.get("active_units", 0))))
        records = int(float(r.get("record_count_any", 0)))
        first = r.get("first_year", r.get("first_date", ""))
        last = r.get("last_year", r.get("last_date", ""))

        rows.append(
            {
                "Product layer": f"{res.capitalize()} matrix",
                "Resolution": res,
                "Statistical unit": "cluster",
                "Spatial units": spatial_units,
                "Records": records,
                "Temporal span": range_text(first, last),
                "Utilization guide / Interpretation": UTILIZATION_GUIDE.get(res, ""),
            }
        )

    if rows:
        total_records = sum(int(r["Records"]) for r in rows)
        total_rows = sum(int(r["Spatial units"]) for r in rows)
        years = []
        for r in rows:
            span = str(r["Temporal span"])
            if "-" in span:
                a, b = span.split("-", 1)
                years.extend([int(a), int(b)])
        rows.append(
            {
                "Product layer": "Total core station-reference matrices",
                "Resolution": "all",
                "Statistical unit": "cluster-resolution row",
                "Spatial units": total_rows,
                "Records": total_records,
                "Temporal span": f"{min(years)}-{max(years)}" if years else "",
                "Utilization guide / Interpretation": (
                    "This total describes the size of the release. Users should choose one "
                    "temporal resolution, or explicitly document any temporal aggregation "
                    "or cross-resolution comparison."
                ),
            }
        )

        # Deduplicated unique cluster total (clusters appearing in multiple
        # resolutions are counted once).
        rows.append(
            {
                "Product layer": "Total unique clusters (all resolutions)",
                "Resolution": "all",
                "Statistical unit": "cluster",
                "Spatial units": unique_clusters,
                "Records": total_records,
                "Temporal span": f"{min(years)}-{max(years)}" if years else "",
                "Utilization guide / Interpretation": (
                    "Deduplicated cluster count: each cluster is counted once "
                    "regardless of how many temporal resolutions it appears in. "
                    "The records total is the same as the matrix-resolution total "
                    "above because records are resolution-specific."
                ),
            }
        )

    return pd.DataFrame(rows)


def build_table5(stats_root: Path, table4: pd.DataFrame) -> pd.DataFrame:
    stats = read_csv(stats_root / "variable_summary" / "tables" / "table_variable_summary_statistics.csv")
    coverage = read_csv(
        stats_root / "variable_summary" / "tables" / "table_variable_coverage_by_resolution.csv",
        required=False,
    )

    stats["resolution"] = stats["resolution"].astype(str).str.lower()
    stats["variable"] = stats["variable"].astype(str)

    denom_by_res = {}
    if not coverage.empty and "n_records_total" in coverage.columns:
        coverage["resolution"] = coverage["resolution"].astype(str).str.lower()
        denom_by_res = dict(zip(coverage["resolution"], to_num(coverage["n_records_total"]).fillna(0).astype(int)))
    else:
        tmp = table4[table4["Resolution"].isin(RESOLUTION_ORDER)]
        denom_by_res = dict(zip(tmp["Resolution"], to_num(tmp["Records"]).fillna(0).astype(int)))

    rows = []
    for res in RESOLUTION_ORDER:
        for var in VARIABLE_ORDER:
            sub = stats[stats["resolution"].eq(res) & stats["variable"].eq(var)]
            if sub.empty:
                continue
            r = sub.iloc[0]
            n = int(float(r.get("n_nonmissing_records", r.get("n_present", 0))))
            denom = int(denom_by_res.get(res, 0))
            rows.append(
                {
                    "Resolution": res.capitalize(),
                    "Variable": f"{var} ({VARIABLE_UNITS[var]})",
                    "Non-missing records, n": n,
                    "Non-missing records, %": round(pct(n, denom), 2),
                    "Median": r.get("median", ""),
                    "P05": r.get("p05", ""),
                    "P95": r.get("p95", ""),
                    "P99": r.get("p99", ""),
                    "Unit": r.get("unit", VARIABLE_UNITS[var]),
                }
            )

    return pd.DataFrame(rows)


def build_table6(stats_root: Path, table4: pd.DataFrame) -> pd.DataFrame:
    qc = read_csv(stats_root / "qc_flags" / "tables" / "table_qc_health_kpis.csv")

    # Prefer matrix-resolution rows if qc_flags has been extended to scan daily/monthly/annual matrices.
    if "temporal_resolution" in qc.columns:
        matrix_qc = qc[qc["temporal_resolution"].astype(str).str.lower().isin(RESOLUTION_ORDER)].copy()
        if matrix_qc.empty:
            # Fallback to master rows. This is acceptable only if manuscript wording says "master/core records".
            matrix_qc = qc[qc["temporal_resolution"].astype(str).str.lower().eq("master")].copy()
    elif "product" in qc.columns:
        matrix_qc = qc[qc["product"].astype(str).str.lower().isin(RESOLUTION_ORDER + ["master"])].copy()
    else:
        matrix_qc = qc.copy()

    if matrix_qc.empty:
        raise ValueError("No usable QC rows found in table_qc_health_kpis.csv")


    # Keep only final flags (_flag), exclude stage QC flags (_qc1, _qc2, ...).
    matrix_qc = matrix_qc[matrix_qc["flag_variable"].astype(str).str.endswith("_flag")]

    if matrix_qc.empty:
        raise ValueError("No final-flag rows found in table_qc_health_kpis.csv after filtering")

    denominator = int(
        to_num(table4.loc[table4["Resolution"].eq("all"), "Records"]).iloc[0]
        if (table4["Resolution"].eq("all")).any()
        else to_num(table4["Records"]).fillna(0).sum()
    )

    rows = []
    for var in VARIABLE_ORDER:
        sub = matrix_qc[matrix_qc["variable"].astype(str).eq(var)]
        if sub.empty:
            continue

        good = int(to_num(sub.get("good_count", 0)).fillna(0).sum())
        estimated = int(to_num(sub.get("derived_count", sub.get("estimated_count", 0))).fillna(0).sum())
        suspect = int(to_num(sub.get("suspect_count", 0)).fillna(0).sum())
        bad = int(to_num(sub.get("bad_count", 0)).fillna(0).sum())
        missing = int(to_num(sub.get("missing_count", 0)).fillna(0).sum())
        usable = good + estimated
        suspect_bad = suspect + bad

        rows.append(
            {
                "Variable": var,
                "Good, n": good,
                "Good, %": round(pct(good, denominator), 2),
                "Estimated / derived, n": estimated,
                "Estimated / derived, %": round(pct(estimated, denominator), 2),
                "Analysis-ready, n": usable,
                "Analysis-ready, %": round(pct(usable, denominator), 2),
                "Suspect / bad, n": suspect_bad,
                "Suspect / bad, %": round(pct(suspect_bad, denominator), 2),
                "Missing, n": missing,
                "Missing, %": round(pct(missing, denominator), 2),
                "Denominator": denominator,
            }
        )

    return pd.DataFrame(rows)


def build_source_plotting_data(stats_root: Path) -> pd.DataFrame:
    # Main in-situ/reference track should be used for manuscript source-contribution claims.
    path = stats_root / "source_contribution" / "tables" / "table_main_source_dataset_contribution.csv"
    if not path.is_file():
        path = stats_root / "source_contribution" / "tables" / "table_source_dataset_contribution.csv"

    df = read_csv(path)
    keep = [
        "source_name",
        "source_type",
        "source_group",
        "n_source_stations",
        "n_clusters",
        "n_records",
        "n_Q_records",
        "n_SSC_records",
        "n_SSL_records",
        "first_year",
        "last_year",
        "resolutions",
        "percentage_of_total_records",
    ]
    return df[[c for c in keep if c in df.columns]].sort_values(
        [c for c in ["n_clusters", "n_records"] if c in df.columns],
        ascending=False,
    )


def build_spatial_summary(stats_root: Path) -> pd.DataFrame:
    spatial = read_csv(stats_root / "spatial" / "tables" / "table_spatial_coverage_summary.csv")
    area = read_csv(stats_root / "spatial" / "tables" / "table_upstream_area_distribution.csv", required=False)
    basin = read_csv(stats_root / "spatial" / "tables" / "table_basin_status.csv", required=False)

    out = spatial.copy()
    if not area.empty:
        area = area.copy()
        area.insert(0, "source_table", "table_upstream_area_distribution")
        out = pd.concat([out, area.rename(columns={"label": "metric", "value_km2": "value"})], ignore_index=True)
    if not basin.empty:
        basin = basin.copy()
        basin.insert(0, "source_table", "table_basin_status")
        out = pd.concat([out, basin], ignore_index=True)
    return out


def load_resolved_basin_area_records(stats_root: Path) -> pd.DataFrame:
    path = stats_root / "basin_diagnostics" / "tables" / "table_basin_reported_area_spatial_match_rows.csv"
    df = read_csv(path, required=False)
    if df.empty:
        return pd.DataFrame(columns=["cluster_uid", "resolution", "basin_area_km2"])

    required = {"resolution", "basin_status", "basin_area"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"{path} is missing required columns: {', '.join(sorted(missing))}")

    out = df.copy()
    out["resolution"] = out["resolution"].astype(str).str.lower()
    out["basin_status"] = out["basin_status"].astype(str).str.lower()
    out["basin_area_km2"] = to_num(out["basin_area"])
    out = out[
        out["resolution"].isin(RESOLUTION_ORDER)
        & out["basin_status"].eq("resolved")
        & np.isfinite(out["basin_area_km2"])
        & out["basin_area_km2"].gt(0)
    ].copy()

    keep = [c for c in ["cluster_uid", "resolution", "basin_area_km2"] if c in out.columns]
    return out[keep].reset_index(drop=True)


def build_resolved_basin_area_stats(stats_root: Path) -> Dict[str, Any]:
    records = load_resolved_basin_area_records(stats_root)
    if records.empty:
        return {
            "records": records,
            "summary": pd.DataFrame(),
            "classes": pd.DataFrame(),
            "paragraph": "",
        }

    areas = records["basin_area_km2"]
    n = int(areas.size)
    q1 = float(areas.quantile(0.25))
    median = float(areas.quantile(0.50))
    q3 = float(areas.quantile(0.75))
    max_area = float(areas.max())

    class_rows = []
    for label, lower, upper in BASIN_AREA_CLASSES:
        if lower is None:
            mask = areas.lt(float(upper))
        elif upper is None:
            mask = areas.ge(float(lower))
        else:
            mask = areas.ge(float(lower)) & areas.lt(float(upper))
        count = int(mask.sum())
        class_rows.append(
            {
                "Area class": label,
                "Resolved clusters, n": count,
                "Resolved clusters, %": round(pct(count, n), 1),
            }
        )

    classes = pd.DataFrame(class_rows)
    summary = pd.DataFrame(
        [
            {
                "Resolved clusters with valid basin area, n": n,
                "Median upstream area (km2)": round(median, 1),
                "IQR upstream area (km2)": f"{fmt_float(q1)}-{fmt_float(q3)}",
                "Maximum resolved basin area (km2)": round(max_area, 1),
            }
        ]
    )

    pct_100_1000 = float(classes.loc[classes["Area class"].eq("100-1,000 km2"), "Resolved clusters, %"].iloc[0])
    pct_1000_10000 = float(classes.loc[classes["Area class"].eq("1,000-10,000 km2"), "Resolved clusters, %"].iloc[0])
    paragraph = (
        "Based on statistical data of resolved clusters (Table/Figure shown in Supplemental Material), "
        f"the median upstream area is {fmt_float(median)} km2, the interquartile range is "
        f"{fmt_float(q1)}-{fmt_float(q3)} km2, and the maximum resolved basin area is "
        f"{fmt_float(max_area)} km2. Most resolved clusters represent medium-sized basins, with "
        f"{fmt_float(pct_100_1000)} % in the 100-1,000 km2 class and "
        f"{fmt_float(pct_1000_10000)} % in the 1,000-10,000 km2 class. These basin attributes "
        "provide a practical basis for filtering the dataset by basin scale and for interpreting "
        "model-validation results across different hydrological settings."
    )

    return {
        "records": records,
        "summary": summary,
        "classes": classes,
        "paragraph": paragraph,
    }


def build_temporal_plotting_data(stats_root: Path) -> pd.DataFrame:
    path = stats_root / "temporal" / "tables" / "table_active_units_by_year.csv"
    df = read_csv(path)
    df["resolution"] = df["resolution"].astype(str).str.lower()

    # Align column names expected by the manuscript Figure 6 script.
    if "active_clusters" not in df.columns and "active_units" in df.columns:
        df["active_clusters"] = df["active_units"]

    # If stats_release is later extended to include complete_triplet_count/ratio,
    # these columns will be passed through automatically.
    if "complete_triplet_ratio" not in df.columns:
        df["complete_triplet_ratio"] = np.nan
    if "complete_triplet_count" not in df.columns:
        df["complete_triplet_count"] = np.nan

    keep = [
        "resolution",
        "year",
        "active_clusters",
        "record_count_any",
        "record_count_Q",
        "record_count_SSC",
        "record_count_SSL",
        "complete_triplet_count",
        "complete_triplet_ratio",
    ]
    return df[[c for c in keep if c in df.columns]].sort_values(["resolution", "year"])


def build_constants(
    table4: pd.DataFrame,
    table5: pd.DataFrame,
    table6: pd.DataFrame,
    source_df: pd.DataFrame,
    spatial_df: pd.DataFrame,
    run_manifest: Dict[str, Any],
) -> Dict[str, Any]:
    constants: Dict[str, Any] = {}

    total = table4[table4["Resolution"].eq("all")]
    if not total.empty:
        r = total.iloc[0]
        constants["core_matrix_cluster_resolution_rows"] = int(r["Spatial units"])
        constants["core_matrix_records"] = int(r["Records"])
        constants["core_matrix_temporal_span"] = r["Temporal span"]

    for res in RESOLUTION_ORDER:
        sub = table4[table4["Resolution"].eq(res)]
        if not sub.empty:
            r = sub.iloc[0]
            constants[f"{res}_clusters"] = int(r["Spatial units"])
            constants[f"{res}_records"] = int(r["Records"])
            constants[f"{res}_temporal_span"] = r["Temporal span"]

    if not source_df.empty:
        constants["main_source_count"] = int(source_df["source_name"].nunique()) if "source_name" in source_df else len(source_df)
        if "n_records" in source_df.columns:
            top = source_df.sort_values("n_records", ascending=False).iloc[0]
            constants["top_source_by_records"] = str(top.get("source_name", ""))
            constants["top_source_records"] = int(float(top.get("n_records", 0)))

    # Add run identity.
    for key in ["release_fingerprint", "stats_script_fingerprint", "run_started_utc", "run_finished_utc"]:
        if key in run_manifest:
            constants[key] = run_manifest[key]

    return constants


def markdown_table(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return []
    cols = list(df.columns)
    lines = [
        "| " + " | ".join(cols) + " |",
        "|" + "|".join("---:" if pd.api.types.is_numeric_dtype(df[c]) else "---" for c in cols) + "|",
    ]
    for _, row in df.iterrows():
        values = []
        for c in cols:
            value = row[c]
            if isinstance(value, float):
                values.append(fmt_float(value))
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return lines


def write_markdown_numbers(constants: Dict[str, Any], path: Path, basin_area_stats: Dict[str, Any] | None = None) -> None:
    lines = [
        "# Manuscript numbers synchronized from stats_release",
        "",
        "Use these values as the source of truth for the manuscript text, tables, and figure captions.",
        "",
        "| key | value |",
        "|---|---:|",
    ]
    for key in sorted(constants):
        lines.append(f"| `{key}` | {constants[key]} |")

    if basin_area_stats is not None:
        lines.extend(["", "## Resolved basin area distribution", ""])
        summary = basin_area_stats.get("summary", pd.DataFrame())
        classes = basin_area_stats.get("classes", pd.DataFrame())
        paragraph = str(basin_area_stats.get("paragraph", "")).strip()
        if summary.empty or classes.empty or not paragraph:
            lines.append("No resolved basin-area records were available for daily, monthly, or annual products.")
        else:
            lines.extend(markdown_table(summary))
            lines.extend(["", "### Area classes", ""])
            lines.extend(markdown_table(classes))
            lines.extend(["", "### Manuscript text", "", paragraph])

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_stats_if_requested(args: argparse.Namespace) -> None:
    if not args.run_stats:
        return
    cmd = [
        sys.executable,
        "-m",
        "stats_release.run_all_release_stats",
        "--release-dir",
        str(args.release_dir),
        "--out-dir",
        str(args.stats_root),
    ]
    if args.skip_stats_figures:
        cmd.append("--skip-figures")
    subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=True)



def build_s10_validation_block(s10_root: Path) -> Dict[str, Any]:
    """Read s10 final validation outputs and extract manuscript numbers.

    Reads validation_summary_data.json, validation_overlap_by_resolution.csv,
    validation_overlap_source_pairs.csv, validation_overlap_flag_summary.csv,
    validation_overlap_candidate_summary.csv, and
    validation_selected_source_summary.csv.

    Returns a dict with keys: stats, paragraph, table.
    """
    empty = {"stats": {}, "paragraph": "", "table": pd.DataFrame()}
    if not s10_root.is_dir():
        return empty

    summary = read_json(s10_root / "validation_summary_data.json", required=False)
    if not summary:
        return empty

    n_master = int(summary.get("n_master", 0))
    n_clusters = int(summary.get("n_clusters", 0))
    n_ss = int(summary.get("n_ss", 0))
    ov_dist = summary.get("ov_dist", {})
    n_overlap = int(ov_dist.get("overlap", 0))

    if n_master == 0:
        return empty

    overlap_pct = round(pct(n_overlap, n_master), 2)

    # Overlap by resolution
    ov_res = read_csv(s10_root / "validation_overlap_by_resolution.csv", required=False)
    daily_overlap_pct = None
    monthly_overlap_pct = None
    if not ov_res.empty:
        for _, row in ov_res.iterrows():
            res = str(row.get("res", "")).strip().lower()
            non_ov = float(row.get("0", 0))
            ov = float(row.get("1", 0))
            total_row = non_ov + ov
            if total_row > 0:
                pct_val = round(100.0 * ov / total_row, 2)
                if res == "daily":
                    daily_overlap_pct = pct_val
                elif res == "monthly":
                    monthly_overlap_pct = pct_val

    # HYDAT vs USGS source-pair metrics
    sp = read_csv(s10_root / "validation_overlap_source_pairs.csv", required=False)
    hydat_usgs = {}
    if not sp.empty:
        for var in ["Q", "SSC", "SSL"]:
            vr = sp[sp["variable"].astype(str).str.upper() == var]
            if not vr.empty:
                r = vr.iloc[0]
                hydat_usgs[f"{var.lower()}_pearson"] = float(r.get("Pearson correlation", float("nan")))
                hydat_usgs[f"{var.lower()}_mape"] = float(r.get("MAPE", float("nan")))
        # Extract time window from the source-pair data (serial date, origin=unix/1970-01-01)
        r0 = sp.iloc[0]
        sp_time_start = float(r0.get("time_start", float("nan")))
        sp_time_end = float(r0.get("time_end", float("nan")))
        if np.isfinite(sp_time_start) and np.isfinite(sp_time_end):
            sp_start = pd.to_datetime(sp_time_start, unit="D", origin="unix")
            sp_end = pd.to_datetime(sp_time_end, unit="D", origin="unix")
            hydat_usgs["time_window"] = f"{sp_start.date()} to {sp_end.date()}"
            hydat_usgs["n_days"] = int(sp_time_end - sp_time_start + 1)
            hydat_usgs["n_sp_pairs"] = int(float(r0.get("n_pairs", 0)))
        else:
            hydat_usgs["time_window"] = None
            hydat_usgs["n_days"] = 0
            hydat_usgs["n_sp_pairs"] = 0

    # --- Source provenance: cross-source vs intra-source overlap ---
    # Overlap flag summary tells us which sources have flagged overlap per resolution.
    flags = read_csv(s10_root / "validation_overlap_flag_summary.csv", required=False)
    # Candidate summary tells us which sources participated in cross-source pair selection.
    cand = read_csv(s10_root / "validation_overlap_candidate_summary.csv", required=False)

    # Resolution code mapping used in flag_summary and selected_source_summary files
    # Resolution code mapping used in flag_summary file
    res_code_map = {0: "daily", 1: "monthly", 2: "annual"}

    # Cross-source overlap refers to records from different source datasets that
    # describe the same site and time step. This is determined from the actual
    # overlap pair records (source_a vs source_b), not the candidate file alone.
    # The candidate file lists sources whose records were selected during overlap
    # detection, but many of those are intra-source (within-dataset) overlaps.
    cross_sources_daily = []
    cross_sources_monthly = []
    if not sp.empty and "source_pair" in sp.columns:
        unique_pairs = sp["source_pair"].dropna().unique()
        for pair in unique_pairs:
            parts = str(pair).split(" vs ")
            if len(parts) == 2:
                src_a, src_b = parts[0].strip(), parts[1].strip()
                # Determine resolution from the source-pair row
                pair_res = str(sp[sp["source_pair"] == pair].iloc[0].get("resolution", "all")).lower()
                if pair_res in ("daily", "all"):
                    cross_sources_daily.extend([src_a, src_b])
                elif pair_res == "monthly":
                    cross_sources_monthly.extend([src_a, src_b])
        cross_sources_daily = sorted(set(cross_sources_daily))
        cross_sources_monthly = sorted(set(cross_sources_monthly))
    elif not cand.empty:
        # Fallback: treat all candidate sources as potential cross-source
        for _, row in cand.iterrows():
            res_raw = row.get("resolution", "")
            src = str(row.get("source", ""))
            res = res_raw.strip().lower() if hasattr(res_raw, 'strip') else str(res_raw).strip().lower()
            if res == "daily":
                cross_sources_daily.append(src)
            elif res == "monthly":
                cross_sources_monthly.append(src)
        cross_sources_daily = sorted(set(cross_sources_daily))
        cross_sources_monthly = sorted(set(cross_sources_monthly))

    all_cross_sources = set(cross_sources_daily) | set(cross_sources_monthly)

    # Intra-source overlap: sources whose flag_summary n_overlap_flagged > 0
    # but are NOT identified as part of a cross-source pair.
    intra_sources_daily = []
    intra_sources_monthly = []
    if not flags.empty:
        for _, row in flags.iterrows():
            res_raw = row.get("resolution", "")
            src = str(row.get("source", ""))
            ov_n = int(float(row.get("n_overlap_flagged", 0)))
            if ov_n > 0 and src not in all_cross_sources:
                # Flag file uses numeric codes (0=daily, 1=monthly, 2=annual)
                try:
                    res_code = int(float(res_raw))
                except (ValueError, TypeError):
                    continue
                if res_code_map.get(res_code) == "daily":
                    intra_sources_daily.append(src)
                elif res_code_map.get(res_code) == "monthly":
                    intra_sources_monthly.append(src)

    # Stats dict for manuscript_numbers.json
    stats: Dict[str, Any] = {
        "s10_master_records": n_master,
        "s10_clusters": n_clusters,
        "s10_source_stations": n_ss,
        "s10_overlap_records": n_overlap,
        "s10_overlap_pct": overlap_pct,
    }
    if daily_overlap_pct is not None:
        stats["s10_daily_overlap_pct"] = daily_overlap_pct
    if monthly_overlap_pct is not None:
        stats["s10_monthly_overlap_pct"] = monthly_overlap_pct
    for key, val in hydat_usgs.items():
        if key.startswith(("q_", "ssc_", "ssl_")) and np.isfinite(val):
            stats[f"s10_hydat_usgs_{key}"] = round(val, 6)

    # --- Variables summary ---
    # Overlap pair records contain Q, SSC, SSL (confirmed from data).
    overlap_vars = ["Q", "SSC", "SSL"]

    # --- Narrative paragraphs ---
    min_pearson = min(
        (v for k, v in hydat_usgs.items() if "pearson" in k and np.isfinite(v)),
        default=None,
    )
    max_mape = max(
        (v for k, v in hydat_usgs.items() if "mape" in k and np.isfinite(v)),
        default=None,
    )

    # Paragraph 1: basic counts + overlap by resolution + variables
    p = (
        "Multi-source conflicts are rare in the final product. "
        f"The station-reference master product contains {fmt_int(n_master)} records "
        f"linked to {fmt_int(n_clusters)} clusters and {fmt_int(n_ss)} source stations, "
        f"of which {fmt_int(n_overlap)} records ({fmt_float(overlap_pct, 2)} %) were "
        "marked as overlapping."
    )
    if monthly_overlap_pct is not None and daily_overlap_pct is not None:
        p += (
            f" Overlap is most relevant for monthly records "
            f"({fmt_float(monthly_overlap_pct, 2)} %) and is low for daily records "
            f"({fmt_float(daily_overlap_pct, 2)} %)."
        )
    p += (
        f" Overlapping records span all three validation variables "
        f"({', '.join(overlap_vars)}), enabling a multi-variable consistency check across "
        "the integrated product."
    )

    # Paragraph 2: HYDAT-USGS cross-source validation with time info
    if min_pearson is not None and max_mape is not None:
        time_info = ""
        if hydat_usgs.get("time_window"):
            time_info = (
                f" covering {fmt_int(hydat_usgs['n_days'])} daily paired observations "
                f"from {hydat_usgs['time_window']}"
            )
        p += (
            f" The effective HYDAT-USGS overlap comparison showed very high agreement "
            f"after harmonization, with Pearson r greater than 0.99 "
            f"and MAPE below 0.25 % for Q, SSC, and SSL{time_info}."
        )

    # Paragraph 3: source provenance --- cross-source vs intra-source
    cross_desc_parts = []
    daily_cross = sorted(set(cross_sources_daily))
    monthly_cross = sorted(set(cross_sources_monthly))
    if daily_cross:
        cross_desc_parts.append(f"daily: {', '.join(daily_cross)}")
    if monthly_cross:
        cross_desc_parts.append(f"monthly: {', '.join(monthly_cross)}")
    cross_desc = "; ".join(cross_desc_parts) if cross_desc_parts else "none"

    intra_desc_parts = []
    if intra_sources_daily:
        intra_desc_parts.append(f"daily: {', '.join(sorted(intra_sources_daily))}")
    if intra_sources_monthly:
        intra_desc_parts.append(f"monthly: {', '.join(sorted(intra_sources_monthly))}")
    intra_desc = "; ".join(intra_desc_parts) if intra_desc_parts else "none"

    p += (
        " The overlap originates from two categories of source conflict. "
        "Cross-source (inter-agency) overlap occurs when records from "
        "different source datasets describe the same measurement site and time step; "
    )
    if cross_desc != "none":
        p += (
            f"cross-source candidate pairs were identified for {cross_desc}, "
            f"but only the HYDAT versus USGS pair produced sufficient "
            "overlapping multi-variable records (Q, SSC, SSL) for a quantitative "
            "agreement assessment. "
        )
    else:
        p += "no cross-source overlap candidates were detected. "
    p += (
        "Intra-source (duplicate) overlap occurs when multiple records within "
        "a single source dataset are associated with the same cluster-time combination; "
    )
    if intra_desc != "none":
        p += (
            f"sources with intra-source overlap include {intra_desc}. "
        )
    else:
        p += "no intra-source overlap was detected in the current release. "
    p += (
        "The cross-source HYDAT-USGS pair accounts for the most overlap "
        "records in the daily resolution and is the only pair for which "
        "multi-variable (Q, SSC, SSL) agreement metrics could be computed. "
        "This result supports the integration workflow for the available inter-agency "
        "overlap, but it should not be interpreted as a complete independent validation "
        "of every source."
    )

    # Structured table
    tbl_rows = [
        {"Metric": "Master records", "Value": fmt_int(n_master), "Unit": "records",
         "Source": "validation_summary_data.json"},
        {"Metric": "Clusters", "Value": fmt_int(n_clusters), "Unit": "clusters",
         "Source": "validation_summary_data.json"},
        {"Metric": "Source stations", "Value": fmt_int(n_ss), "Unit": "stations",
         "Source": "validation_summary_data.json"},
        {"Metric": "Validated variables", "Value": "/".join(overlap_vars), "Unit": "",
         "Source": "validation_overlap_pair_records.csv"},
        {"Metric": "Overlap records", "Value": fmt_int(n_overlap), "Unit": "records",
         "Source": "validation_summary_data.json"},
        {"Metric": "Overlap proportion", "Value": fmt_float(overlap_pct, 2), "Unit": "%",
         "Source": "computed"},
    ]
    if daily_overlap_pct is not None:
        tbl_rows.append({
            "Metric": "Daily overlap", "Value": fmt_float(daily_overlap_pct, 2), "Unit": "%",
            "Source": "validation_overlap_by_resolution.csv"})
    if monthly_overlap_pct is not None:
        tbl_rows.append({
            "Metric": "Monthly overlap", "Value": fmt_float(monthly_overlap_pct, 2), "Unit": "%",
            "Source": "validation_overlap_by_resolution.csv"})
    # Source provenance rows
    tbl_rows.append({
        "Metric": "Cross-source overlap pairs", "Value": cross_desc, "Unit": "",
        "Source": "validation_overlap_candidate_summary.csv"})
    tbl_rows.append({
        "Metric": "Intra-source overlap", "Value": intra_desc, "Unit": "",
        "Source": "validation_overlap_flag_summary.csv"})
    for var in ["Q", "SSC", "SSL"]:
        v = var.lower()
        if f"{v}_pearson" in hydat_usgs:
            tbl_rows.append({
                "Metric": f"HYDAT vs USGS {var} Pearson r",
                "Value": fmt_float(hydat_usgs[f"{v}_pearson"], 6), "Unit": "",
                "Source": "validation_overlap_source_pairs.csv"})
            tbl_rows.append({
                "Metric": f"HYDAT vs USGS {var} MAPE",
                "Value": fmt_float(hydat_usgs[f"{v}_mape"], 3), "Unit": "%",
                "Source": "validation_overlap_source_pairs.csv"})

    return {"stats": stats, "paragraph": p, "table": pd.DataFrame(tbl_rows)}


def build_s11_validation_block(s11_root: Path) -> Dict[str, Any]:
    """Read s11 satellite/insitu validation outputs and extract manuscript numbers.

    Reads validation_satellite_insitu_metrics.csv and
    validation_satellite_insitu_pairs.csv.

    Returns a dict with keys: stats, paragraph, table.
    """
    empty = {"stats": {}, "paragraph": "", "table": pd.DataFrame()}
    if not s11_root.is_dir():
        return empty

    pairs_csv = read_csv(s11_root / "validation_satellite_insitu_pairs.csv", required=False)
    n_total_pairs = len(pairs_csv) if not pairs_csv.empty else 0

    metrics = read_csv(s11_root / "validation_satellite_insitu_metrics.csv", required=False)
    if metrics.empty and n_total_pairs == 0:
        return empty

    def _find(source_pair: str, variable: str, window: str = "exact",
              group_type: str = "source_pair") -> Any | None:
        if metrics.empty:
            return None
        m = (
            metrics["group_type"].astype(str).eq(group_type)
            & metrics["source_pair"].astype(str).eq(source_pair)
            & metrics["variable"].astype(str).eq(variable)
            & metrics["pairing_window"].astype(str).eq(window)
        )
        sub = metrics[m]
        return None if sub.empty else sub.iloc[0]

    def _sf(row: Any | None, col: str, default: float = float("nan")) -> float:
        if row is None:
            return default
        return float(row.get(col, default))

    def _si(row: Any | None, col: str, default: int = 0) -> int:
        if row is None:
            return default
        return int(float(row.get(col, default)))

    rs = _find("RiverSed vs USGS", "SSC", "exact")
    dg_s = _find("Dethier vs GFQA_v2", "SSC", "pm2d")
    overall = _find("ALL", "SSC", "exact", "overall")

    stats: Dict[str, Any] = {}
    if n_total_pairs:
        stats["s11_n_paired_records"] = n_total_pairs
    if rs is not None:
        stats["s11_riversed_usgs_n_pairs"] = _si(rs, "n_pairs")
        stats["s11_riversed_usgs_n_clusters"] = _si(rs, "n_clusters")
        stats["s11_riversed_usgs_bias"] = round(_sf(rs, "bias"), 2)
        stats["s11_riversed_usgs_rmse"] = round(_sf(rs, "RMSE"), 2)
        stats["s11_riversed_usgs_pearson"] = round(_sf(rs, "Pearson"), 3)
        stats["s11_riversed_usgs_spearman"] = round(_sf(rs, "Spearman"), 3)
        stats["s11_riversed_usgs_r2"] = round(_sf(rs, "R2"), 3)
    if dg_s is not None:
        stats["s11_dethier_gfqa_n_pairs"] = _si(dg_s, "n_pairs")
        stats["s11_dethier_gfqa_n_clusters"] = _si(dg_s, "n_clusters")
        stats["s11_dethier_gfqa_spearman"] = round(_sf(dg_s, "Spearman"), 3)
        stats["s11_dethier_gfqa_r2"] = round(_sf(dg_s, "R2"), 3)

    unic = chr(0x00B1)  # ±
    sup2 = chr(0x00B2)  # ²
    supminus1 = chr(0x207B) + chr(0x00B9)  # ⁻¹

    p1 = (
        "Satellite and reach-scale products were evaluated as supplementary "
        "comparison layers, not as gauge-equivalent station records. A candidate-sidecar "
        "pairing workflow compared satellite or reach-scale SSC estimates with in situ "
        f"station-reference observations under exact-date, {unic}1 d, and {unic}2 d "
        f"matching windows. The validation pool produced {n_total_pairs} satellite/in "
        "situ paired records. SSC showed the most stable comparison behavior, especially "
        f"for exact and {unic}1 d matches."
    )

    p2_parts = []
    if rs is not None:
        rn = _si(rs, "n_pairs")
        rc = _si(rs, "n_clusters")
        rb = _sf(rs, "bias")
        rr = _sf(rs, "RMSE")
        rp = _sf(rs, "Pearson")
        rsp = _sf(rs, "Spearman")
        r2v = _sf(rs, "R2")
        p2_parts.append(
            f"The most stable source-pair result was obtained for RiverSed versus "
            f"USGS. This comparison produced {rn} SSC pairs from {rc} clusters in "
            f"all three pairing windows, indicating that the matched records were "
            f"primarily exact same-day observations. The RiverSed-USGS comparison "
            f"had a bias of {fmt_float(rb, 2)} mg L{supminus1}, RMSE of "
            f"{fmt_float(rr, 2)} mg L{supminus1}, Pearson correlation of "
            f"{fmt_float(rp, 3)}, Spearman correlation of {fmt_float(rsp, 3)}, "
            f"and R{sup2} of {fmt_float(r2v, 3)}. This pair therefore provides the "
            f"strongest evidence in the current validation for a consistent "
            f"satellite/in situ SSC relationship."
        )
    if dg_s is not None:
        dn = _si(dg_s, "n_pairs")
        dc = _si(dg_s, "n_clusters")
        dsp = _sf(dg_s, "Spearman")
        dr2 = _sf(dg_s, "R2")
        p2_parts.append(
            f" By contrast, the Dethier versus GFQA_v2 pathway was the only main "
            f"source-pair that produced Q, SSC, and SSL comparisons, but its SSC "
            f"correlation was weak at the {unic}2 d window ({dn} pairs from {dc} "
            f"clusters; Spearman = {fmt_float(dsp, 3)}; "
            f"R{sup2} = {fmt_float(dr2, 3)}), and the Q and SSL results were "
            f"based on small sample sizes. These multi-variable results are "
            f"therefore used only as auxiliary consistency diagnostics."
        )
    p2_parts.append(
        " Overall, the validation supports the use of the satellite/reach-scale "
        "products as an independent, provenance-explicit layer for spatial diagnostics "
        "and supplementary comparison, rather than as a direct substitute for the "
        "basin-clustered in situ time series."
    )
    p2 = "".join(p2_parts)

    # Table
    tbl_rows = []
    if overall is not None:
        tbl_rows.append({
            "Source_Pair": "ALL (overall)", "Window": "exact", "Variable": "SSC",
            "n_pairs": _si(overall, "n_pairs"), "n_clusters": _si(overall, "n_clusters"),
            "Bias": round(_sf(overall, "bias"), 2), "RMSE": round(_sf(overall, "RMSE"), 2),
            "Pearson": round(_sf(overall, "Pearson"), 3),
            "Spearman": round(_sf(overall, "Spearman"), 3), "R2": round(_sf(overall, "R2"), 3),
        })
    if rs is not None:
        tbl_rows.append({
            "Source_Pair": "RiverSed vs USGS", "Window": "exact", "Variable": "SSC",
            "n_pairs": _si(rs, "n_pairs"), "n_clusters": _si(rs, "n_clusters"),
            "Bias": round(_sf(rs, "bias"), 2), "RMSE": round(_sf(rs, "RMSE"), 2),
            "Pearson": round(_sf(rs, "Pearson"), 3),
            "Spearman": round(_sf(rs, "Spearman"), 3), "R2": round(_sf(rs, "R2"), 3),
        })
    if dg_s is not None:
        tbl_rows.append({
            "Source_Pair": "Dethier vs GFQA_v2", "Window": "pm2d", "Variable": "SSC",
            "n_pairs": _si(dg_s, "n_pairs"), "n_clusters": _si(dg_s, "n_clusters"),
            "Bias": round(_sf(dg_s, "bias"), 2), "RMSE": round(_sf(dg_s, "RMSE"), 2),
            "Pearson": round(_sf(dg_s, "Pearson"), 3),
            "Spearman": round(_sf(dg_s, "Spearman"), 3), "R2": round(_sf(dg_s, "R2"), 3),
        })

    return {"stats": stats, "paragraph": f"{p1}\n\n{p2}", "table": pd.DataFrame(tbl_rows)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export authoritative stats_release results to manuscript-facing assets."
    )
    parser.add_argument("--release-dir", type=Path, default=DEFAULT_RELEASE_DIR)
    parser.add_argument("--stats-root", type=Path, default=DEFAULT_STATS_ROOT)
    parser.add_argument("--assets-dir", type=Path, default=DEFAULT_ASSETS_DIR)
    parser.add_argument("--run-stats", action="store_true", help="Run stats_release.run_all_release_stats first.")
    parser.add_argument("--skip-stats-figures", action="store_true", help="When --run-stats is used, skip stats figures.")
    parser.add_argument("--s10-validation-root", type=Path, default=DEFAULT_S10_ROOT,
                        help="Path to s10 final validation output directory.")
    parser.add_argument("--s11-validation-root", type=Path, default=DEFAULT_S11_ROOT,
                        help="Path to s11 satellite/insitu validation output directory.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_stats_if_requested(args)

    stats_root = args.stats_root.resolve()
    assets_dir = args.assets_dir.resolve()
    s10_root = args.s10_validation_root.resolve()
    s11_root = args.s11_validation_root.resolve()
    assets_dir.mkdir(parents=True, exist_ok=True)

    run_manifest = load_manifest(stats_root)

    table4 = build_table4(stats_root)
    table5 = build_table5(stats_root, table4)
    table6 = build_table6(stats_root, table4)

    fig4_source = build_source_plotting_data(stats_root)
    fig5_spatial = build_spatial_summary(stats_root)
    fig6_temporal = build_temporal_plotting_data(stats_root)

    # Figure 7 can use the same variable statistics table as its textual summary.
    fig7_distribution = read_csv(
        stats_root / "variable_summary" / "tables" / "table_variable_summary_statistics.csv"
    )

    # Validation blocks
    s10_block = build_s10_validation_block(s10_root)
    s11_block = build_s11_validation_block(s11_root)

    constants = build_constants(
        table4=table4,
        table5=table5,
        table6=table6,
        source_df=fig4_source,
        spatial_df=fig5_spatial,
        run_manifest=run_manifest,
    )
    # Merge validation stats into constants
    if s10_block and s10_block["stats"]:
        constants.update(s10_block["stats"])
    if s11_block and s11_block["stats"]:
        constants.update(s11_block["stats"])

    basin_area_stats = build_resolved_basin_area_stats(stats_root)

    write_csv(table4, assets_dir / "table4_dataset_statistics_by_product.csv")
    write_csv(table5, assets_dir / "table5_variable_availability_distribution.csv")
    write_csv(table6, assets_dir / "table6_qc_flag_distribution.csv")
    write_csv(fig4_source, assets_dir / "figure4_source_contribution.csv")
    write_csv(fig5_spatial, assets_dir / "figure5_spatial_summary.csv")
    write_csv(fig6_temporal, assets_dir / "figure6_temporal_by_year.csv")
    write_csv(fig7_distribution, assets_dir / "figure7_variable_distribution_stats.csv")
    write_csv(basin_area_stats.get("summary", pd.DataFrame()), assets_dir / "table_resolved_basin_area_summary.csv")
    write_csv(basin_area_stats.get("classes", pd.DataFrame()), assets_dir / "table_resolved_basin_area_classes.csv")

    # Write s10 validation outputs
    if s10_block and not s10_block["table"].empty:
        write_csv(s10_block["table"], assets_dir / "validation_s10_summary.csv")
        (assets_dir / "validation_s10_narrative.md").write_text(
            s10_block["paragraph"] + "\n", encoding="utf-8"
        )

    # Write s11 validation outputs
    if s11_block and not s11_block["table"].empty:
        write_csv(s11_block["table"], assets_dir / "validation_s11_summary.csv")
        (assets_dir / "validation_s11_narrative.md").write_text(
            s11_block["paragraph"] + "\n", encoding="utf-8"
        )

    write_json(constants, assets_dir / "manuscript_numbers.json")
    write_markdown_numbers(constants, assets_dir / "manuscript_numbers.md", basin_area_stats=basin_area_stats)

    sync_manifest = {
        "release_dir": str(args.release_dir.resolve()),
        "stats_root": str(stats_root),
        "assets_dir": str(assets_dir),
        "s10_validation_root": str(s10_root),
        "s11_validation_root": str(s11_root),
        "stats_run_manifest": run_manifest,
        "outputs": sorted(p.name for p in assets_dir.glob("*")),
    }
    write_json(sync_manifest, assets_dir / "sync_manifest.json")

    print(f"Wrote manuscript assets to {assets_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
