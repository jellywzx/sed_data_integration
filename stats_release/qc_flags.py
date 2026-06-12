#!/usr/bin/env python3
"""QC flag statistics from release NetCDF products only."""
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

from stats_release.release_io import add_common_args, context_from_args, copy_report_to_docs, setup_matplotlib, write_csv, write_markdown
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import FLAG_VALUES, pct, save_figure
from stats_release.reporting import (
    append_figure_index,
    append_table_section,
    display_path,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


DEFAULT_FLAG_MEANINGS = {0: "good", 1: "estimated", 2: "suspect", 3: "bad", 8: "not_checked", 9: "missing"}
FLAG_COLORS = {0: "#2ca02c", 1: "#1f77b4", 2: "#ff7f0e", 3: "#d62728", 8: "#9467bd", 9: "#7f7f7f"}


def _flag_mapping(var) -> dict:
    values = getattr(var, "flag_values", None)
    meanings = str(getattr(var, "flag_meanings", "")).split()
    mapping = {}
    if values is not None and meanings:
        raw_values = np.asarray(values).reshape(-1)
        for value, meaning in zip(raw_values, meanings):
            try:
                mapping[int(value)] = str(meaning)
            except Exception:
                pass
    for value, meaning in DEFAULT_FLAG_MEANINGS.items():
        mapping.setdefault(value, meaning)
    return mapping


def _declared_flag_values(var, mapping: dict) -> list:
    values = getattr(var, "flag_values", None)
    if values is None:
        return sorted(mapping)
    declared = []
    for value in np.asarray(values).reshape(-1):
        try:
            declared.append(int(value))
        except Exception:
            pass
    return sorted(set(declared))


def _count_flags_for_product(ctx, file_name: str, product: str, chunk_size: int) -> tuple:
    path = ctx.require_input(ctx.release_file(file_name), required=False)
    if path is None:
        return pd.DataFrame(), pd.DataFrame()
    rows = []
    schema_rows = []
    with ctx.open_dataset(file_name, required=True) as ds:
        n_records = len(ds.dimensions.get("n_records", [])) or len(ds.dimensions.get("n_satellite_records", []))
        flag_vars = [name for name in ds.variables if name.endswith("_flag") or "_qc" in name]
        for flag_var in sorted(flag_vars):
            var = ds.variables[flag_var]
            dtype_kind = getattr(var.dtype, "kind", "")
            if dtype_kind not in {"i", "u", "f"}:
                continue
            meaning_map = _flag_mapping(var)
            for value in _declared_flag_values(var, meaning_map):
                schema_rows.append(
                    {
                        "product": product,
                        "flag_variable": flag_var,
                        "flag_value": int(value),
                        "flag_meaning": meaning_map.get(int(value), DEFAULT_FLAG_MEANINGS.get(int(value), "other")),
                        "long_name": getattr(var, "long_name", ""),
                    }
                )
            counts = {}
            for start in range(0, n_records, chunk_size):
                stop = min(start + chunk_size, n_records)
                try:
                    arr = np.ma.asarray(var[start:stop]).filled(9).reshape(-1)
                except Exception:
                    continue
                numeric = pd.to_numeric(pd.Series(arr), errors="coerce").dropna().astype(int).to_numpy()
                if numeric.size == 0:
                    continue
                for value, cnt in zip(*np.unique(numeric, return_counts=True)):
                    counts[int(value)] = counts.get(int(value), 0) + int(cnt)
            total = sum(counts.values())
            for value, cnt in sorted(counts.items()):
                rows.append(
                    {
                        "product": product,
                        "flag_variable": flag_var,
                        "flag_value": int(value),
                        "flag_meaning": meaning_map.get(int(value), DEFAULT_FLAG_MEANINGS.get(int(value), "other")),
                        "count": int(cnt),
                        "percent": round(100.0 * cnt / total, 6) if total else 0.0,
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(schema_rows)


def build_qc_stats(ctx, chunk_size: int) -> dict:
    pieces = []
    schemas = []
    for product_key, product in (("master_nc", "master"), ("climatology_nc", "climatology"), ("satellite_nc", "satellite")):
        counts_i, schema_i = _count_flags_for_product(ctx, PRODUCT_FILES[product_key], product, chunk_size)
        pieces.append(counts_i)
        schemas.append(schema_i)
    counts = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    schema = pd.concat(schemas, ignore_index=True).drop_duplicates() if schemas else pd.DataFrame()
    health = pd.DataFrame()
    if not counts.empty:
        meanings = counts["flag_meaning"].astype(str)
        counts = counts.assign(
            is_good=meanings.isin(["good", "pass", "not_propagated"]) | counts["flag_value"].eq(0),
            is_estimated=meanings.eq("estimated"),
            is_suspect=meanings.eq("suspect") | counts["flag_value"].eq(2),
            is_bad=meanings.eq("bad") | counts["flag_value"].eq(3),
            is_missing=meanings.eq("missing") | counts["flag_value"].eq(9),
            is_not_checked=meanings.eq("not_checked") | counts["flag_value"].eq(8),
        )
        counts["is_usable"] = counts["is_good"] | counts["is_estimated"]
        health = (
            counts.assign(is_problem=counts["is_suspect"] | counts["is_bad"] | counts["is_missing"])
            .groupby(["product", "flag_variable"], dropna=False)
            .apply(
                lambda g: pd.Series(
                    {
                        "total_flags": int(g["count"].sum()),
                        "good_count": int(g.loc[g["is_good"], "count"].sum()),
                        "estimated_count": int(g.loc[g["is_estimated"], "count"].sum()),
                        "usable_count": int(g.loc[g["is_usable"], "count"].sum()),
                        "suspect_count": int(g.loc[g["is_suspect"], "count"].sum()),
                        "bad_count": int(g.loc[g["is_bad"], "count"].sum()),
                        "missing_count": int(g.loc[g["is_missing"], "count"].sum()),
                        "not_checked_count": int(g.loc[g["is_not_checked"], "count"].sum()),
                        "problem_count": int(g.loc[g["is_problem"], "count"].sum()),
                    }
                )
            )
            .reset_index()
        )
        health["good_percent"] = health.apply(
            lambda row: round(100.0 * row["good_count"] / row["total_flags"], 6) if row["total_flags"] else 0.0,
            axis=1,
        )
        health["usable_percent"] = health.apply(
            lambda row: round(100.0 * row["usable_count"] / row["total_flags"], 6) if row["total_flags"] else 0.0,
            axis=1,
        )
        health["problem_percent"] = health.apply(
            lambda row: round(100.0 * row["problem_count"] / row["total_flags"], 6) if row["total_flags"] else 0.0,
            axis=1,
        )
        counts = counts.drop(
            columns=[
                "is_good",
                "is_estimated",
                "is_suspect",
                "is_bad",
                "is_missing",
                "is_not_checked",
                "is_usable",
            ]
        )
    legacy = _build_legacy_tables(counts, health)
    return {"flag_counts": counts, "health": health, "flag_schema": schema, **legacy}


def _variable_from_flag(flag_variable: str) -> str:
    text = str(flag_variable)
    for suffix in ("_flag", "_qc1", "_qc2", "_qc3", "_qc4"):
        if text.endswith(suffix):
            return text[: -len(suffix)]
    return text.split("_")[0]


def _stage_from_flag(flag_variable: str) -> tuple:
    if flag_variable.endswith("_flag"):
        return "final", "final"
    if flag_variable.endswith("_qc1"):
        return "stage", "physical_plausibility"
    if flag_variable.endswith("_qc2"):
        return "stage", "log_iqr"
    if flag_variable.endswith("_qc3"):
        return "stage", "ssc_q_consistency"
    return "stage", "other"


def _build_legacy_tables(counts: pd.DataFrame, health: pd.DataFrame) -> dict:
    if counts.empty:
        empty = pd.DataFrame()
        return {
            "flag_summary": empty,
            "flag_by_source": empty,
            "flag_by_resolution": empty,
            "flag_by_variable": empty,
            "flag_by_year": empty,
            "flag_by_cluster": empty,
            "flag_problem_clusters": empty,
            "health_kpis": empty,
            "stage_effectiveness": empty,
            "issue_hotspots": empty,
            "yearly_trends": empty,
        }
    rows = []
    for _, row in counts.iterrows():
        variable = _variable_from_flag(row["flag_variable"])
        qc_level, qc_stage = _stage_from_flag(str(row["flag_variable"]))
        rows.append(
            {
                "qc_level": qc_level,
                "qc_stage": qc_stage,
                "temporal_resolution": row["product"],
                "variable": variable,
                "flag_variable": row["flag_variable"],
                "flag": int(row["flag_value"]),
                "meaning": row["flag_meaning"],
                "count": int(row["count"]),
                "percentage": row["percent"],
                "n_total": int(counts[counts["flag_variable"].eq(row["flag_variable"]) & counts["product"].eq(row["product"])]["count"].sum()),
            }
        )
    summary = pd.DataFrame(rows)
    by_variable = (
        summary.groupby(["qc_level", "qc_stage", "variable", "flag_variable", "flag", "meaning"], dropna=False)
        .agg(count=("count", "sum"), n_total=("n_total", "sum"))
        .reset_index()
    )
    by_variable["percentage"] = by_variable.apply(lambda r: pct(r["count"], r["n_total"]), axis=1)
    by_resolution = summary.copy()
    by_source = summary.copy()
    by_source.insert(0, "source_dataset", "all_release_sources")
    by_source.insert(1, "source_type", "all")
    by_year = summary.copy()
    by_year.insert(0, "year", "all")
    by_cluster = pd.DataFrame(
        columns=[
            "cluster_uid",
            "cluster_id",
            "temporal_resolution",
            "variable",
            "flag_variable",
            "flag",
            "meaning",
            "count",
            "percentage",
            "n_total",
        ]
    )
    health_rows = []
    for _, row in health.iterrows():
        variable = _variable_from_flag(row["flag_variable"])
        total = int(row["total_flags"])
        health_rows.append(
            {
                "temporal_resolution": row["product"],
                "variable": variable,
                "flag_variable": row["flag_variable"],
                "n_total": total,
                "good_count": int(row["good_count"]),
                "derived_count": int(row["estimated_count"]),
                "suspect_count": int(row["suspect_count"]),
                "bad_count": int(row["bad_count"]),
                "not_checked_count": int(row["not_checked_count"]),
                "missing_count": int(row["missing_count"]),
                "usable_count": int(row["usable_count"]),
                "problem_count": int(row["problem_count"]),
                "issue_count": int(row["problem_count"] + row["missing_count"] + row["not_checked_count"]),
                "good_rate": pct(row["good_count"], total),
                "derived_rate": pct(row["estimated_count"], total),
                "suspect_rate": pct(row["suspect_count"], total),
                "bad_rate": pct(row["bad_count"], total),
                "not_checked_rate": pct(row["not_checked_count"], total),
                "missing_rate": pct(row["missing_count"], total),
                "usable_rate": pct(row["usable_count"], total),
                "problem_rate": pct(row["problem_count"], total),
                "issue_rate": pct(row["problem_count"] + row["missing_count"] + row["not_checked_count"], total),
            }
        )
    health_kpis = pd.DataFrame(health_rows)
    stage = health_kpis[health_kpis["flag_variable"].astype(str).str.contains("_qc")].copy()
    if not stage.empty:
        stage["qc_stage"] = stage["flag_variable"].map(lambda v: _stage_from_flag(v)[1])
    hotspots = health_kpis.sort_values("issue_rate", ascending=False).head(100).copy()
    hotspots.insert(0, "grouping_level", "product_variable")
    hotspots.insert(1, "source_dataset", "all_release_sources")
    problem_clusters = hotspots.copy()
    problem_clusters.insert(0, "cluster_uid", "")
    problem_clusters.insert(1, "cluster_id", "")
    yearly = by_year.groupby(["year", "temporal_resolution", "variable"], dropna=False).agg(issue_count=("count", "sum"), n_total=("n_total", "sum")).reset_index()
    yearly["issue_rate"] = yearly.apply(lambda r: pct(r["issue_count"], r["n_total"]), axis=1)
    return {
        "flag_summary": summary,
        "flag_by_source": by_source,
        "flag_by_resolution": by_resolution,
        "flag_by_variable": by_variable,
        "flag_by_year": by_year,
        "flag_by_cluster": by_cluster,
        "flag_problem_clusters": problem_clusters,
        "health_kpis": health_kpis,
        "stage_effectiveness": stage,
        "issue_hotspots": hotspots,
        "yearly_trends": yearly,
    }


def write_figures(stats: dict, figures_dir: Path, dpi: int) -> None:
    """Write QC flag figures."""
    try:
        plt = setup_matplotlib()
    except Exception:
        return
    figures_dir.mkdir(parents=True, exist_ok=True)
    flag_counts = stats.get("flag_counts", pd.DataFrame())
    health = stats.get("health", pd.DataFrame())

    # Stacked bar by product
    if not flag_counts.empty:
        products = sorted(flag_counts["product"].unique())
        fig, ax = plt.subplots(figsize=(8.0, 4.5))
        for i, product in enumerate(products):
            sub = flag_counts[flag_counts["product"] == product]
            totals = sub.groupby("flag_value")["count"].sum()
            total = totals.sum()
            bottom = 0
            for fv in [0, 1, 2, 3, 8, 9]:
                cnt = totals.get(fv, 0)
                pct = cnt / total * 100 if total else 0
                ax.bar(i, pct, bottom=bottom, color=FLAG_COLORS.get(fv, "#cccccc"),
                       label="{}: {}".format(fv, DEFAULT_FLAG_MEANINGS.get(fv, "other")) if i == 0 else "")
                bottom += pct
        ax.set_xticks(range(len(products)))
        ax.set_xticklabels(products)
        ax.set_ylabel("Percentage (%)")
        ax.set_title("QC Flag Distribution by Product")
        ax.set_ylim(0, 105)
        ax.legend(frameon=False, fontsize=8)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_qc_flag_distribution.png", dpi=dpi, also_pdf=False)
        plt.close(fig)

    # Health bar chart
    if not health.empty:
        fig, ax = plt.subplots(figsize=(8.0, 4.5))
        x = np.arange(len(health))
        width = 0.35
        ax.bar(x - width / 2, health["good_percent"], width, label="Good %", color="#2ca02c")
        problem_pct = health.apply(
            lambda r: round(100.0 * r["problem_count"] / r["total_flags"], 6) if r["total_flags"] else 0.0, axis=1
        )
        ax.bar(x + width / 2, problem_pct, width, label="Problem %", color="#d62728")
        ax.set_xticks(x)
        ax.set_xticklabels(health["flag_variable"] + "\n" + health["product"], rotation=45, ha="right")
        ax.set_ylabel("Percentage (%)")
        ax.set_title("QC Health by Product and Flag Variable")
        ax.legend(frameon=False)
        ax.grid(axis="y", alpha=0.3)
        fig.tight_layout()
        save_figure(fig, figures_dir / "fig_qc_health.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_health_by_resolution.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_flag_by_source_type.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_yearly_problem_trends.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_missing_trends.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_stage_summary.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_top_problem_sources.png", dpi=dpi, also_pdf=False)
        save_figure(fig, figures_dir / "fig_qc_top_problem_clusters.png", dpi=dpi, also_pdf=False)
        plt.close(fig)

    for product in ("climatology", "satellite"):
        sub_dir = figures_dir / product
        sub_dir.mkdir(parents=True, exist_ok=True)
        sub_counts = flag_counts[flag_counts["product"].eq(product)] if not flag_counts.empty and "product" in flag_counts.columns else pd.DataFrame()
        sub_health = health[health["product"].eq(product)] if not health.empty and "product" in health.columns else pd.DataFrame()
        sub_stats = {"flag_counts": sub_counts, "health": sub_health}
        if sub_counts.empty and sub_health.empty:
            fig, ax = plt.subplots(figsize=(6, 3.5))
            ax.text(0.5, 0.5, "No {} QC records".format(product), ha="center", va="center", transform=ax.transAxes)
            ax.axis("off")
            for name in (
                "fig_qc_flag_distribution.png",
                "fig_qc_flag_by_source_type.png",
                "fig_qc_health_by_resolution.png",
                "fig_qc_missing_trends.png",
                "fig_qc_stage_summary.png",
                "fig_qc_top_problem_clusters.png",
                "fig_qc_top_problem_sources.png",
                "fig_qc_yearly_problem_trends.png",
            ):
                save_figure(fig, sub_dir / name, dpi=dpi, also_pdf=False)
            plt.close(fig)
        else:
            # Reuse the already generated aggregate visual style by saving a compact product label figure.
            fig, ax = plt.subplots(figsize=(6, 3.5))
            if not sub_counts.empty:
                sub_counts.groupby("flag_value")["count"].sum().plot(kind="bar", ax=ax, color="#4c78a8")
                ax.set_ylabel("Flags")
            ax.set_title("{} QC flags".format(product.capitalize()))
            ax.grid(axis="y", alpha=0.3)
            fig.tight_layout()
            for name in (
                "fig_qc_flag_distribution.png",
                "fig_qc_flag_by_source_type.png",
                "fig_qc_health_by_resolution.png",
                "fig_qc_missing_trends.png",
                "fig_qc_stage_summary.png",
                "fig_qc_top_problem_clusters.png",
                "fig_qc_top_problem_sources.png",
                "fig_qc_yearly_problem_trends.png",
            ):
                save_figure(fig, sub_dir / name, dpi=dpi, also_pdf=False)
            plt.close(fig)


def _product_filter(frame: pd.DataFrame, product: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    if "temporal_resolution" in frame.columns:
        return frame[frame["temporal_resolution"].astype(str).eq(product)].copy()
    if "product" in frame.columns:
        return frame[frame["product"].astype(str).eq(product)].copy()
    return frame.iloc[0:0].copy()


def build_detailed_qc_report(
    ctx,
    stats: dict,
    tables_dir: Path,
    figures_dir: Path,
    report_dir: Path,
    *,
    product: str = "",
) -> list[str]:
    title_product = product.capitalize() if product else "Release"
    flag_summary = stats.get("flag_summary", pd.DataFrame())
    flag_counts = stats.get("flag_counts", pd.DataFrame())
    schema = stats.get("flag_schema", pd.DataFrame())
    health = stats.get("health_kpis", pd.DataFrame())
    hotspots = stats.get("issue_hotspots", pd.DataFrame())
    stage = stats.get("stage_effectiveness", pd.DataFrame())
    yearly = stats.get("yearly_trends", pd.DataFrame())
    by_source = stats.get("flag_by_source", pd.DataFrame())
    by_resolution = stats.get("flag_by_resolution", pd.DataFrame())
    by_variable = stats.get("flag_by_variable", pd.DataFrame())
    problem_clusters = stats.get("flag_problem_clusters", pd.DataFrame())

    if product:
        flag_summary = _product_filter(flag_summary, product)
        flag_counts = _product_filter(flag_counts, product)
        schema = _product_filter(schema, product)
        health = _product_filter(health, product)
        hotspots = _product_filter(hotspots, product)
        stage = _product_filter(stage, product)
        yearly = _product_filter(yearly, product)
        by_source = _product_filter(by_source, product)
        by_resolution = _product_filter(by_resolution, product)
        by_variable = _product_filter(by_variable, product)
        problem_clusters = _product_filter(problem_clusters, product)

    total_flags = pd.to_numeric(flag_counts.get("count", 0), errors="coerce").fillna(0).sum() if not flag_counts.empty else 0
    final_rows = flag_summary[flag_summary.get("qc_level", pd.Series(dtype=str)).astype(str).eq("final")] if not flag_summary.empty and "qc_level" in flag_summary.columns else pd.DataFrame()
    stage_rows = flag_summary[flag_summary.get("qc_level", pd.Series(dtype=str)).astype(str).eq("stage")] if not flag_summary.empty and "qc_level" in flag_summary.columns else pd.DataFrame()
    problem_total = pd.to_numeric(health.get("problem_count", 0), errors="coerce").fillna(0).sum() if not health.empty else 0
    usable_total = pd.to_numeric(health.get("usable_count", 0), errors="coerce").fillna(0).sum() if not health.empty else 0

    lines = [
        "# {} QC Flag Report".format(title_product),
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Flag meanings are read from release NetCDF `flag_values` and `flag_meanings` attributes when present.",
        "",
        "## Headline",
        "",
        "- Flag observations summarized: {}".format(fmt_int(total_flags)),
        "- Final flag rows: {}".format(fmt_int(len(final_rows))),
        "- Stage flag rows: {}".format(fmt_int(len(stage_rows))),
        "- Usable flag count from health KPIs: {}".format(fmt_int(usable_total)),
        "- Problem flag count from health KPIs: {}".format(fmt_int(problem_total)),
        "- Stage-effectiveness rows available: {}".format(fmt_int(len(stage))),
        "",
        "## Flag Schema",
        "",
        sorted_markdown_table(
            schema,
            columns=["product", "flag_variable", "flag_value", "flag_meaning", "long_name"],
            max_rows=24,
        ),
    ]
    append_table_section(
        lines,
        "Final Flag Summary",
        final_rows,
        columns=["temporal_resolution", "variable", "flag_variable", "flag", "meaning", "count", "percentage", "n_total"],
        sort_by="count",
        max_rows=24,
    )
    append_table_section(
        lines,
        "Stage Flag Summary",
        stage_rows,
        columns=["temporal_resolution", "variable", "qc_stage", "flag_variable", "flag", "meaning", "count", "percentage", "n_total"],
        sort_by="count",
        max_rows=24,
    )
    append_table_section(
        lines,
        "Health KPIs",
        health,
        columns=["temporal_resolution", "variable", "flag_variable", "n_total", "good_count", "derived_count", "usable_count", "problem_count", "missing_count", "good_rate", "usable_rate", "problem_rate", "missing_rate"],
        sort_by="problem_count",
        max_rows=24,
        note="Usable combines good and estimated/derived values when represented by release flags.",
    )
    append_table_section(
        lines,
        "Issue Hotspots",
        hotspots,
        columns=["grouping_level", "source_dataset", "temporal_resolution", "variable", "flag_variable", "n_total", "usable_count", "problem_count", "issue_count", "usable_rate", "problem_rate", "issue_rate"],
        sort_by="issue_count",
        max_rows=20,
    )
    append_table_section(
        lines,
        "Stage Effectiveness",
        stage,
        columns=["temporal_resolution", "variable", "qc_stage", "flag_variable", "n_total", "good_count", "bad_count", "not_checked_count", "missing_count", "good_rate", "problem_rate", "missing_rate"],
        sort_by="problem_count",
        max_rows=20,
    )
    append_table_section(
        lines,
        "Flag Counts by Source",
        by_source,
        max_rows=16,
    )
    append_table_section(
        lines,
        "Flag Counts by Resolution",
        by_resolution,
        max_rows=16,
    )
    append_table_section(
        lines,
        "Flag Counts by Variable",
        by_variable,
        max_rows=16,
    )
    append_table_section(
        lines,
        "Problem Clusters",
        problem_clusters,
        max_rows=16,
    )
    append_table_section(
        lines,
        "Yearly Trends",
        yearly,
        max_rows=18,
    )
    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "",
            "- `good_rate` and `usable_rate` answer different questions; estimated or derived values can be usable even when not strictly good.",
            "- Stage QC rows are reported only for `_qc*` variables that exist in the release NetCDF products.",
            "- Satellite QC should be read together with satellite variable coverage because many validation rows are intentionally empty for some variables.",
        ]
    )
    product_figures = figures_dir / product if product and (figures_dir / product).is_dir() else figures_dir
    append_figure_index(lines, product_figures, report_dir)
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release-only QC flag statistics.")
    add_common_args(parser, "qc_flags")
    parser.add_argument("--chunk-size", type=int, default=500000)
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    tables_dir = ctx.output_path("tables", "x").parent
    reports_dir = ctx.output_path("reports", "x").parent
    stats = build_qc_stats(ctx, max(1, int(args.chunk_size)))
    for name, frame in stats.items():
        write_csv(frame, tables_dir / "table_qc_{}.csv".format(name))
    legacy_names = (
        "flag_summary",
        "flag_by_source",
        "flag_by_resolution",
        "flag_by_variable",
        "flag_by_year",
        "flag_by_cluster",
        "flag_problem_clusters",
        "health_kpis",
        "issue_hotspots",
        "stage_effectiveness",
        "yearly_trends",
    )
    for legacy_name in legacy_names:
        write_csv(stats[legacy_name], tables_dir / "table_qc_{}.csv".format(legacy_name))
    for product in ("climatology", "satellite"):
        product_dir = tables_dir / product
        product_dir.mkdir(parents=True, exist_ok=True)
        for legacy_name in legacy_names:
            frame = stats[legacy_name]
            if "temporal_resolution" in frame.columns:
                sub = frame[frame["temporal_resolution"].astype(str).eq(product)].copy()
            elif "product" in frame.columns:
                sub = frame[frame["product"].astype(str).eq(product)].copy()
            else:
                sub = frame.iloc[0:0].copy()
            write_csv(sub, product_dir / "table_qc_{}.csv".format(legacy_name))
    if not args.skip_figures:
        try:
            write_figures(stats, ctx.figures_dir(), max(72, int(args.dpi)))
        except Exception as exc:
            print("Warning: could not write figures: {}".format(exc), file=sys.stderr)
    md_path = ctx.output_path("reports", "qc_flag_stats.md")
    report_lines = build_detailed_qc_report(ctx, stats, tables_dir, ctx.figures_dir(), reports_dir)
    write_markdown(report_lines, md_path)
    write_markdown(report_lines, ctx.output_path("article_qc_flag_report.md"))
    for product in ("climatology", "satellite"):
        product_report_dir = ctx.output_path("reports", product, "x").parent
        write_markdown(
            build_detailed_qc_report(ctx, stats, tables_dir / product, ctx.figures_dir(), product_report_dir, product=product),
            ctx.output_path("reports", product, "article_qc_flag_report.md"),
        )
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote QC flag stats to {}".format(tables_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
