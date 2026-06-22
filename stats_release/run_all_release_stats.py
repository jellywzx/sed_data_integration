#!/usr/bin/env python3
"""Run all release-only statistics modules with a reproducible manifest."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import (
    add_common_args,
    context_from_args,
    file_manifest,
    metadata_fingerprint,
    script_fingerprint,
    write_csv,
    write_json,
    write_markdown,
)
from stats_release.parity import build_parity_manifest
from stats_release.reporting import (
    append_table_section,
    display_path,
    fmt_int,
    read_csv_if_exists,
    safe_lines,
    sorted_markdown_table,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_STATS_RELEASE_DIR = PROJECT_ROOT / "docs" / "reports" / "stats_release"

MODULES = (
    "inventory",
    "spatial",
    "temporal",
    "source_dataset_layers",
    "source_contribution",
    "basin_diagnostics",
    "variable_summary",
    "qc_flags",
)


RUN_FILES = (
    "run_summary.csv",
    "run_summary.md",
    "run_manifest.csv",
    "run_manifest.json",
    "parity_manifest.csv",
    "release_detailed_report.md",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _clean_managed_outputs(out_dir: Path, modules: list) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for module in modules:
        target = out_dir / module
        if target.exists():
            shutil.rmtree(str(target))
    for name in RUN_FILES:
        target = out_dir / name
        if target.exists():
            target.unlink()


def _collect_output_files(out_dir: Path, modules: list) -> pd.DataFrame:
    rows = []
    for module in modules:
        module_dir = out_dir / module
        for item in file_manifest(module_dir):
            rel = "{}/{}".format(module, item["relative_path"])
            rows.append(
                {
                    "module": module,
                    "relative_path": rel,
                    "size_bytes": item["size_bytes"],
                    "mtime_ns": item["mtime_ns"],
                }
            )
    return pd.DataFrame(rows, columns=["module", "relative_path", "size_bytes", "mtime_ns"])


def _copy_markdown_reports_to_docs(out_root: Path, enabled: bool) -> list[Path]:
    if not enabled:
        return []
    out_root = Path(out_root).resolve()
    copied = []
    for source in sorted(out_root.rglob("*.md")):
        target = DOCS_STATS_RELEASE_DIR / source.relative_to(out_root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source), str(target))
        copied.append(target)
    return copied


def _module_report_rows(out_root: Path) -> pd.DataFrame:
    report_map = [
        ("inventory", "inventory/reports/release_inventory_stats.md", "Release inventory and health report"),
        ("spatial", "spatial/reports/spatial_coverage_stats.md", "Spatial coverage report"),
        ("spatial", "spatial/article_spatial_coverage_summary.md", "Article spatial coverage summary"),
        ("temporal", "temporal/reports/temporal_coverage_stats.md", "Temporal coverage report"),
        ("temporal", "temporal/article_temporal_coverage_report.md", "Article temporal coverage report"),
        ("source_dataset_layers", "source_dataset_layers/reports/source_dataset_layers.md", "Source dataset layer report"),
        ("source_contribution", "source_contribution/reports/source_contribution_report.md", "Source contribution report"),
        ("basin_diagnostics", "basin_diagnostics/spatial_match_error_detailed_report.md", "Basin matching detailed report"),
        ("variable_summary", "variable_summary/variable_coverage_results_report_ESSD.md", "Variable coverage report"),
        ("qc_flags", "qc_flags/article_qc_flag_report.md", "QC flag report"),
    ]
    rows = []
    for module, rel_path, description in report_map:
        path = out_root / rel_path
        rows.append(
            {
                "module": module,
                "report": rel_path,
                "exists": int(path.is_file()),
                "size_bytes": int(path.stat().st_size) if path.is_file() else 0,
                "description": description,
            }
        )
    return pd.DataFrame(rows)


def build_release_detailed_report(
    ctx,
    out_root: Path,
    rows: list,
    parity: pd.DataFrame,
    release_fp: str,
    script_fp: str,
    run_started: str,
    run_finished: str,
    clean_output: bool,
) -> list[str]:
    run_df = pd.DataFrame(rows)
    parity_counts = (
        parity.groupby("status", dropna=False).size().reset_index(name="count").sort_values("count", ascending=False)
        if not parity.empty and "status" in parity.columns
        else pd.DataFrame()
    )
    missing_release_capable = 0
    if not parity.empty and "status" in parity.columns:
        missing_release_capable = int(parity["status"].astype(str).eq("missing_release_capable").sum())

    inventory_dir = out_root / "inventory" / "tables"
    path_leaks = read_csv_if_exists(inventory_dir / "path_leaks.csv")
    path_leak_rows = int(pd.to_numeric(path_leaks.get("local_path_count", 0), errors="coerce").fillna(0).gt(0).sum()) if not path_leaks.empty else 0
    inactive = read_csv_if_exists(inventory_dir / "active_metadata_consistency.csv")
    inactive_rows = int(pd.to_numeric(inactive.get("inactive_nc_entries", 0), errors="coerce").fillna(0).gt(0).sum()) if not inactive.empty else 0
    validation = read_csv_if_exists(inventory_dir / "validation_contradictions.csv")

    basin_dir = out_root / "basin_diagnostics" / "tables"
    basin_status = read_csv_if_exists(basin_dir / "table_basin_spatial_match_status_counts.csv")
    unresolved_rows = 0
    unresolved_records = 0
    if not basin_status.empty and "basin_status" in basin_status.columns:
        mask = basin_status["basin_status"].astype(str).eq("unresolved")
        unresolved_rows = pd.to_numeric(basin_status.loc[mask, "rows"], errors="coerce").fillna(0).sum() if "rows" in basin_status.columns else 0
        unresolved_records = pd.to_numeric(basin_status.loc[mask, "records"], errors="coerce").fillna(0).sum() if "records" in basin_status.columns else 0
    anomalies = read_csv_if_exists(basin_dir / "table_basin_resolved_point_anomalies.csv")

    variable_dir = out_root / "variable_summary" / "tables"
    satellite_variable = read_csv_if_exists(variable_dir / "table_satellite_variable_by_source.csv")
    sparse_satellite = (
        satellite_variable[pd.to_numeric(satellite_variable.get("present_percent", 0), errors="coerce").fillna(0).lt(1)]
        if not satellite_variable.empty
        else pd.DataFrame()
    )

    temporal_axis = read_csv_if_exists(out_root / "temporal" / "tables" / "table_temporal_time_axis_diagnostics.csv")
    sparse_axes = []
    if not temporal_axis.empty and "axis_interpretation" in temporal_axis.columns:
        sparse_axes = sorted(set(temporal_axis[temporal_axis["axis_interpretation"].astype(str).str.contains("sparse", case=False, na=False)]["resolution"].astype(str)))

    report_rows = _module_report_rows(out_root)
    module_failures = run_df[pd.to_numeric(run_df.get("return_code", 1), errors="coerce").fillna(1).ne(0)] if not run_df.empty else pd.DataFrame()

    lines = [
        "# Sediment Reference Release Detailed Statistics Report",
        "",
        "## Run Identity",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Stats output: `{}`".format(display_path(out_root)),
        "- Run started UTC: `{}`".format(run_started),
        "- Run finished UTC: `{}`".format(run_finished),
        "- Clean output before run: `{}`".format(bool(clean_output)),
        "- Release fingerprint: `{}`".format(release_fp),
        "- Stats script fingerprint: `{}`".format(script_fp),
        "",
        "## Run Status",
        "",
        "- Modules requested: {}".format(fmt_int(len(rows))),
        "- Module failures: {}".format(fmt_int(len(module_failures))),
        "- Missing release-capable parity outputs: {}".format(fmt_int(missing_release_capable)),
        "- Unsupported release-only parity outputs: {}".format(
            fmt_int(parity["status"].astype(str).eq("unsupported_release_only").sum())
            if not parity.empty and "status" in parity.columns
            else 0
        ),
        "",
        sorted_markdown_table(run_df, columns=["module", "return_code", "started_utc", "finished_utc"], max_rows=20),
    ]
    append_table_section(
        lines,
        "Parity Manifest Summary",
        parity_counts,
        columns=["status", "count"],
        sort_by="count",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Detailed Module Reports",
        report_rows,
        columns=["module", "report", "exists", "size_bytes", "description"],
        max_rows=20,
    )
    lines.extend(
        [
            "",
            "## Release Risks and QA Signals",
            "",
            "- Inventory path-leak fields with host-local paths: {}".format(fmt_int(path_leak_rows)),
            "- NetCDF metadata dimensions with inactive entries: {}".format(fmt_int(inactive_rows)),
            "- Validation/file-existence contradictions: {}".format(fmt_int(len(validation))),
            "- Unresolved basin rows: {}".format(fmt_int(unresolved_rows)),
            "- Records affected by unresolved basin rows: {}".format(fmt_int(unresolved_records)),
            "- Resolved basin point-flag anomalies: {}".format(fmt_int(len(anomalies))),
            "- Satellite source-variable rows with less than 1% present values: {}".format(fmt_int(len(sparse_satellite))),
            "- Sparse time axes: {}".format(", ".join(sparse_axes) if sparse_axes else "none"),
        ]
    )
    append_table_section(
        lines,
        "Inventory Path-Leak Fields",
        path_leaks.drop(columns=["sample"], errors="ignore") if not path_leaks.empty else path_leaks,
        columns=["product", "layer", "field", "n_values", "absolute_path_count", "local_path_count"],
        sort_by="local_path_count",
        max_rows=12,
        note="Raw examples stay in `inventory/tables/path_leaks.csv`; this report does not echo local machine paths.",
    )
    append_table_section(
        lines,
        "Inactive Metadata Consistency",
        inactive,
        columns=["entity", "nc_dimension", "nc_unique", "catalog_rows", "catalog_unique", "used_unique", "inactive_nc_entries"],
        sort_by="inactive_nc_entries",
        max_rows=8,
    )
    append_table_section(
        lines,
        "Top Unresolved Basin Sources",
        read_csv_if_exists(basin_dir / "table_basin_unresolved_by_source.csv"),
        columns=["source_name", "rows", "unresolved_rows", "records", "unresolved_records", "unresolved_row_percent", "unresolved_record_percent"],
        sort_by="unresolved_records",
        max_rows=10,
    )
    append_table_section(
        lines,
        "Satellite Variable Coverage Watchlist",
        sparse_satellite,
        columns=["source_name", "variable", "n_records", "n_present", "present_percent", "usable_percent"],
        sort_by="n_records",
        max_rows=12,
    )
    lines.extend(
        [
            "",
            "## How to Read These Outputs",
            "",
            "- Per-module reports are the authoritative narrative summaries; CSV tables remain the reproducible data source.",
            "- `unsupported_release_only` means the legacy output requires non-release pipeline intermediates and is intentionally not recreated.",
            "- Release-only reports do not change any dataset values or basin statuses; they expose QA priorities for the next release build.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run the complete release-only statistics suite.")
    add_common_args(parser, "run_all")
    parser.add_argument(
        "--modules",
        nargs="+",
        default=list(MODULES),
        choices=list(MODULES),
        help="Subset of modules to run.",
    )
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument(
        "--no-clean-output",
        action="store_false",
        dest="clean_output",
        default=True,
        help="Do not remove managed module outputs before running. Cleaning is enabled by default.",
    )
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    out_root = Path(args.out_dir).resolve()
    modules = list(args.modules)

    if args.clean_output:
        _clean_managed_outputs(out_root, modules)
    else:
        out_root.mkdir(parents=True, exist_ok=True)

    run_started = _utc_now()
    release_fp = metadata_fingerprint(ctx.release_dir)
    script_fp = script_fingerprint()
    rows = []
    for module in modules:
        module_out = out_root / module
        cmd = [
            sys.executable,
            "-m",
            "stats_release.{}".format(module),
            "--release-dir",
            str(ctx.release_dir),
            "--out-dir",
            str(module_out),
            "--dpi",
            str(int(args.dpi)),
        ]
        if args.skip_figures:
            cmd.append("--skip-figures")
        if not ctx.strict_release_only:
            cmd.append("--allow-non-release-inputs")
        print("Running {}".format(module))
        module_started = _utc_now()
        result = subprocess.run(cmd, cwd=str(Path(__file__).resolve().parents[1]))
        module_finished = _utc_now()
        rows.append(
            {
                "module": module,
                "return_code": int(result.returncode),
                "out_dir": str(module_out),
                "started_utc": module_started,
                "finished_utc": module_finished,
            }
        )
        if result.returncode != 0 and not args.continue_on_error:
            break

    run_finished = _utc_now()
    summary = ctx.output_path("run_summary.csv")
    summary_df = pd.DataFrame(rows)
    write_csv(summary_df, summary)

    outputs = _collect_output_files(out_root, [row["module"] for row in rows])
    outputs["release_fingerprint"] = release_fp
    outputs["stats_script_fingerprint"] = script_fp
    outputs["run_started_utc"] = run_started
    manifest_csv = ctx.output_path("run_manifest.csv")
    write_csv(outputs, manifest_csv)
    manifest_json = ctx.output_path("run_manifest.json")
    write_json(
        {
            "run_started_utc": run_started,
            "run_finished_utc": run_finished,
            "release_dir": str(ctx.release_dir),
            "release_fingerprint": release_fp,
            "stats_script_fingerprint": script_fp,
            "clean_output": bool(args.clean_output),
            "modules": rows,
            "outputs": outputs.to_dict(orient="records"),
        },
        manifest_json,
    )

    parity = build_parity_manifest(out_root)
    parity_csv = ctx.output_path("parity_manifest.csv")
    write_csv(parity, parity_csv)

    report_lines = [
        "# Release Stats Run Summary",
        "",
        "- Release directory: `{}`".format(display_path(ctx.release_dir)),
        "- Run started UTC: `{}`".format(run_started),
        "- Run finished UTC: `{}`".format(run_finished),
        "- Release fingerprint: `{}`".format(release_fp),
        "- Stats script fingerprint: `{}`".format(script_fp),
        "- Clean output before run: `{}`".format(bool(args.clean_output)),
        "- Summary CSV: `{}`".format(display_path(summary)),
        "- Manifest CSV: `{}`".format(display_path(manifest_csv)),
        "- Manifest JSON: `{}`".format(display_path(manifest_json)),
        "- Legacy parity manifest: `{}`".format(display_path(parity_csv)),
    ]
    for row in rows:
        status = "OK" if row["return_code"] == 0 else "FAILED (code {})".format(row["return_code"])
        report_lines.append("- {}: {}".format(row["module"], status))
    md_path = ctx.output_path("run_summary.md")
    write_markdown(safe_lines(report_lines), md_path)
    detailed_path = ctx.output_path("release_detailed_report.md")
    write_markdown(
        build_release_detailed_report(
            ctx,
            out_root,
            rows,
            parity,
            release_fp,
            script_fp,
            run_started,
            run_finished,
            bool(args.clean_output),
        ),
        detailed_path,
    )
    copied_reports = _copy_markdown_reports_to_docs(out_root, bool(args.copy_reports))
    if copied_reports:
        print("Copied {} Markdown reports to {}".format(len(copied_reports), DOCS_STATS_RELEASE_DIR))
    failed = [row for row in rows if row["return_code"] != 0]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
