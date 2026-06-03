#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Count source datasets used in mainline and sideline products.

Root convention
---------------
This version treats `scripts_basin_test/` as the root directory.

Expected layout:

    scripts_basin_test/
      output/
        s3_collected_stations.csv
        s5_basin_clustered_stations.csv
        s6_cluster_quality_order.csv
        s7_source_station_resolution_catalog.csv
        sed_reference_release/
          source_dataset_catalog.csv
          source_station_catalog.csv
          sed_reference_climatology.nc
          satellite_validation_catalog.csv
          sed_reference_overlap_candidates.csv.gz

Run examples:

    # from scripts_basin_test/
    python stats/count_main_side_source_datasets.py

    # from scripts_basin_test/stats/
    python count_main_side_source_datasets.py

    # explicit root
    python count_main_side_source_datasets.py --root ..

Outputs:

    output/sed_reference_release/tables/source_dataset_layer_summary.csv
    output/sed_reference_release/tables/source_dataset_layer_membership.csv
    output/sed_reference_release/tables/source_dataset_layer_report.md
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

MAINLINE_RESOLUTIONS = {
    "daily",
    "monthly",
    "annual",
}

SIDELINE_CLIMATOLOGY_RESOLUTIONS = {
    "climatology",
    "annual_climatology",
    "annually_climatology",
    "monthly_climatology",
}

ALL_KNOWN_RESOLUTION_DIRS = (
    MAINLINE_RESOLUTIONS
    | SIDELINE_CLIMATOLOGY_RESOLUTIONS
    | {
        "annually",
        "quarterly",
        "single_point",
        "other",
    }
)

SOURCE_COLUMNS = [
    "source_dataset",
    "source_name",
    "source",
    "dataset",
    "dataset_name",
]

PATH_COLUMNS = [
    "path",
    "source_station_paths",
    "source_station_path",
    "candidate_path",
    "file_path",
    "nc_path",
]


# ---------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------

def find_scripts_basin_test_root(start: Path) -> Path:
    """
    Auto-detect scripts_basin_test root from current working directory.

    This allows running from:
        scripts_basin_test/
        scripts_basin_test/stats/
        any child directory under scripts_basin_test/
    """
    start = start.resolve()

    for c in [start, *start.parents]:
        if c.name == "scripts_basin_test" and (c / "output").is_dir():
            return c

        if (c / "pipeline_paths.py").is_file() and (c / "output").is_dir():
            return c

        if (c / "output" / "sed_reference_release").is_dir():
            return c

    return start


def get_candidate_organized_roots(scripts_root: Path) -> List[Path]:
    """
    Candidate locations for organized resolution input directories.

    The pipeline often uses:
        Output_r/../output_resolution_organized

    With scripts_basin_test as root:
        scripts_root = Output_r/scripts_basin_test
        output_r_root = scripts_root.parent
        organized_root = output_r_root / "../output_resolution_organized"

    This function also checks a few fallback layouts, including cases where
    daily/monthly/annually_climatology are directly under Output_r.
    """
    scripts_root = scripts_root.resolve()
    output_r_root = scripts_root.parent.resolve()

    candidates = [
        # Standard pipeline location from pipeline_paths.S2_ORGANIZED_DIR
        (output_r_root / "../output_resolution_organized").resolve(),

        # Possible local variants
        (output_r_root / "output_resolution_organized").resolve(),
        (scripts_root / "output_resolution_organized").resolve(),

        # Fallback if resolution dirs are directly under Output_r
        output_r_root,

        # Fallback if resolution dirs are directly under scripts_basin_test
        scripts_root,
    ]

    deduped: List[Path] = []
    seen = set()
    for p in candidates:
        key = str(p)
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    return deduped


# ---------------------------------------------------------------------
# Text/source parsing helpers
# ---------------------------------------------------------------------

def clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def normalize_source_set(values: Iterable) -> Set[str]:
    out: Set[str] = set()
    for value in values:
        text = clean_text(value)
        if text:
            out.add(text)
    return out


def first_existing_column(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = set(df.columns)
    for col in candidates:
        if col in cols:
            return col
    return None


def derive_source_from_path(path_text: str) -> str:
    """
    Best-effort source parser for paths such as:

        daily/HYDAT/qc/file.nc
        daily/HYDAT_daily_station.nc
        monthly/GFQA_v2/qc/file.nc
        climatology/GFQA_v2/qc/file.nc
        annually_climatology/SomeDataset_xxx.nc
        /.../output_resolution_organized/daily/HYDAT/qc/file.nc
    """
    text = clean_text(path_text)
    if not text:
        return ""

    p = Path(text)
    parts = list(p.parts)
    if not parts:
        return ""

    # If path contains output_resolution_organized, keep only the relative part after it.
    marker = "output_resolution_organized"
    if marker in parts:
        idx = parts.index(marker)
        parts = parts[idx + 1 :]

    if not parts:
        return ""

    first = parts[0].strip()
    first_low = first.lower()

    # Case: resolution/source/qc/file.nc
    if first_low in ALL_KNOWN_RESOLUTION_DIRS:
        resolution = first_low

        if "qc" in parts:
            qc_idx = parts.index("qc")
            before_qc = parts[1:qc_idx]
            if before_qc:
                return "_".join(before_qc)

        # Case: resolution/source/...
        if len(parts) >= 2:
            second = parts[1]
            if Path(second).suffix == "":
                return second

        # Case: resolution/source_resolution_xxx.nc
        stem = Path(parts[-1]).stem
        stem_parts = stem.split("_")

        for i, seg in enumerate(stem_parts):
            if seg.lower() == resolution:
                return "_".join(stem_parts[:i]) if i > 0 else ""

        # Handle annual vs annually naming differences.
        resolution_aliases = {
            "annual": {"annual", "annually"},
            "annually": {"annual", "annually"},
            "annually_climatology": {"annually_climatology", "annual_climatology"},
            "annual_climatology": {"annually_climatology", "annual_climatology"},
        }
        aliases = resolution_aliases.get(resolution, {resolution})
        for i, seg in enumerate(stem_parts):
            if seg.lower() in aliases:
                return "_".join(stem_parts[:i]) if i > 0 else ""

        return stem_parts[0] if stem_parts else ""

    # Case without explicit resolution prefix.
    if "qc" in parts:
        qc_idx = parts.index("qc")
        before_qc = parts[:qc_idx]
        if before_qc:
            return "_".join(before_qc)

    return parts[0]


# ---------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------

def empty_info(path: Path | str, note: str = "") -> Dict:
    return {
        "path": str(path),
        "exists": Path(path).is_file() if isinstance(path, Path) else False,
        "rows": 0,
        "source_column": "",
        "note": note,
    }


def read_sources_from_csv(path: Path) -> Tuple[Set[str], Dict]:
    """
    Read source names from a CSV or CSV.GZ.

    If no direct source column exists, try deriving source names from path-like columns.
    """
    info = {
        "path": str(path),
        "exists": path.is_file(),
        "rows": 0,
        "source_column": "",
        "note": "",
    }

    if not path.is_file():
        info["note"] = "missing"
        return set(), info

    try:
        df = pd.read_csv(path, keep_default_na=False)
    except Exception as exc:
        info["note"] = f"cannot_read_csv: {exc}"
        return set(), info

    info["rows"] = int(len(df))

    col = first_existing_column(df, SOURCE_COLUMNS)
    if col:
        info["source_column"] = col
        return normalize_source_set(df[col]), info

    path_col = first_existing_column(df, PATH_COLUMNS)
    if path_col:
        info["source_column"] = f"derived_from:{path_col}"
        return normalize_source_set(df[path_col].map(derive_source_from_path)), info

    info["note"] = "no_source_column"
    return set(), info


def read_sources_from_netcdf(path: Path) -> Tuple[Set[str], Dict]:
    """
    Optional NetCDF source reader.

    If netCDF4 is unavailable or the NetCDF does not expose a recognizable source
    variable, this returns an empty set with a note.
    """
    info = {
        "path": str(path),
        "exists": path.is_file(),
        "rows": 0,
        "source_column": "",
        "note": "",
    }

    if not path.is_file():
        info["note"] = "missing"
        return set(), info

    try:
        import netCDF4 as nc4
        import numpy as np
    except Exception:
        info["note"] = "netCDF4_unavailable"
        return set(), info

    def read_text_var(ds, name: str) -> List[str]:
        if name not in ds.variables:
            return []

        values = ds.variables[name][:]
        try:
            if (
                getattr(values, "dtype", None) is not None
                and values.dtype.kind in {"S", "U"}
                and values.ndim > 1
            ):
                arr = nc4.chartostring(values)
            else:
                arr = np.asarray(values, dtype=object)
        except Exception:
            arr = np.asarray(values, dtype=object)

        return [clean_text(x) for x in arr.reshape(-1)]

    try:
        with nc4.Dataset(path, "r") as ds:
            for col in SOURCE_COLUMNS:
                values = read_text_var(ds, col)
                sources = normalize_source_set(values)
                if sources:
                    info["source_column"] = col
                    info["rows"] = len(values)
                    return sources, info

            for col in PATH_COLUMNS:
                values = read_text_var(ds, col)
                sources = normalize_source_set(derive_source_from_path(x) for x in values)
                if sources:
                    info["source_column"] = f"derived_from:{col}"
                    info["rows"] = len(values)
                    return sources, info

    except Exception as exc:
        info["note"] = f"cannot_read_netcdf: {exc}"
        return set(), info

    info["note"] = "no_source_variable"
    return set(), info


def scan_sources_from_resolution_dirs(
    scripts_root: Path,
    wanted_resolutions: Set[str],
    layer_note: str,
) -> Tuple[Set[str], Dict]:
    """
    Scan organized input folders for NetCDF files and derive source names from paths.

    This is useful for input-level accounting, especially when some side products
    are not represented in release source_dataset_catalog.csv.
    """
    sources: Set[str] = set()
    n_files = 0
    scanned_roots: List[str] = []

    wanted_resolutions_lower = {x.lower() for x in wanted_resolutions}

    for scan_root in get_candidate_organized_roots(scripts_root):
        if not scan_root.is_dir():
            continue

        has_any_resolution_dir = any((scan_root / r).is_dir() for r in wanted_resolutions_lower)
        if not has_any_resolution_dir:
            continue

        scanned_roots.append(str(scan_root))

        for res in sorted(wanted_resolutions_lower):
            res_dir = scan_root / res
            if not res_dir.is_dir():
                continue

            for nc_path in res_dir.rglob("*.nc"):
                n_files += 1
                try:
                    rel = nc_path.relative_to(scan_root)
                except Exception:
                    rel = nc_path

                source = derive_source_from_path(str(rel))
                if source:
                    sources.add(source)

    info = {
        "path": " | ".join(scanned_roots),
        "exists": bool(scanned_roots),
        "rows": n_files,
        "source_column": "derived_from_nc_paths",
        "note": layer_note if scanned_roots else "no_resolution_dirs_found",
    }

    return sources, info


# ---------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------

def add_layer(
    layers: Dict[str, Set[str]],
    infos: Dict[str, Dict],
    layer_name: str,
    sources: Set[str],
    info: Dict,
) -> None:
    layers[layer_name] = set(sorted(sources))
    infos[layer_name] = dict(info)


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    """
    Minimal markdown table writer without requiring tabulate.
    """
    if df.empty:
        return "_No rows._"

    work = df.copy()
    for col in work.columns:
        work[col] = work[col].map(lambda x: "" if pd.isna(x) else str(x))

    cols = list(work.columns)

    def esc(text: str) -> str:
        return text.replace("|", "\\|").replace("\n", " ")

    lines = []
    lines.append("| " + " | ".join(esc(c) for c in cols) + " |")
    lines.append("| " + " | ".join("---" for _ in cols) + " |")

    for _, row in work.iterrows():
        lines.append("| " + " | ".join(esc(row[c]) for c in cols) + " |")

    return "\n".join(lines)


def write_report(
    out_path: Path,
    layer_summary: pd.DataFrame,
    membership: pd.DataFrame,
    layers: Dict[str, Set[str]],
) -> None:
    main_selected = layers.get("mainline_release_source_dataset_catalog", set())

    # All mainline layers EXCEPT the final release catalog — i.e. sources that
    # entered the mainline processing pipeline but were not necessarily selected.
    main_input_all = (
        layers.get("mainline_organized_inputs_daily_monthly_annual", set())
        | layers.get("mainline_s3_collected_stations", set())
        | layers.get("mainline_s5_clustered_stations", set())
        | layers.get("mainline_s6_quality_order_candidates", set())
        | layers.get("mainline_s7_source_station_catalog", set())
        | layers.get("mainline_release_source_station_catalog", set())
    )

    main_all = main_input_all | main_selected

    # Sources that entered the mainline pipeline (any stage) but did NOT make
    # it into the final core release.
    entered_but_not_selected = sorted(main_input_all - main_selected)

    side_all = (
        layers.get("sideline_climatology_organized_inputs", set())
        | layers.get("sideline_climatology_release_nc", set())
        | layers.get("sideline_satellite_validation_catalog", set())
        | layers.get("sideline_overlap_candidates_sidecar", set())
    )

    only_main = sorted(main_all - side_all)
    only_side = sorted(side_all - main_all)
    both = sorted(main_all & side_all)

    lines: List[str] = []
    lines.append("# Source dataset layer report")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "`mainline_release_source_dataset_catalog` is the strict final core-release count. "
        "It counts source datasets that contributed selected records to the main station-reference product."
    )
    lines.append("")
    lines.append(
        "Sideline layers include climatology products, satellite/validation-only products, "
        "and overlap provenance sidecars. These are intentionally separated from the core mainline product."
    )
    lines.append("")
    lines.append("## Layer summary")
    lines.append("")
    lines.append(
        dataframe_to_markdown(
            layer_summary[
                [
                    "layer",
                    "n_source_datasets",
                    "n_rows_or_files",
                    "exists",
                    "source_column",
                    "note",
                ]
            ]
        )
    )
    lines.append("")
    lines.append("## Key counts")
    lines.append("")
    lines.append(f"- Mainline final selected source datasets: {len(main_selected)}")
    lines.append(f"- Any mainline source datasets: {len(main_all)}")
    lines.append(f"- Any sideline source datasets: {len(side_all)}")
    lines.append(f"- Sources in both mainline and sideline: {len(both)}")
    lines.append(f"- Mainline-only sources: {len(only_main)}")
    lines.append(f"- Sideline-only sources: {len(only_side)}")
    lines.append("")
    lines.append("## Mainline input sources not selected for final core release")
    lines.append("")
    lines.append(
        "These source datasets entered the mainline processing pipeline "
        "(organized inputs, collected stations, clustered stations, "
        "quality-order candidates, or release station catalog) "
        "but were **not** selected into the final core 16 source datasets."
    )
    lines.append("")
    if entered_but_not_selected:
        for s in entered_but_not_selected:
            lines.append(f"- {s}")
    else:
        lines.append("_All mainline input sources were selected._")
    lines.append("")
    lines.append(f"Count: {len(entered_but_not_selected)}")
    lines.append("")

    lines.append("## Mainline final selected sources")
    lines.append("")
    if main_selected:
        for s in sorted(main_selected):
            lines.append(f"- {s}")
    else:
        lines.append("_None found._")

    lines.append("")
    lines.append("## Sideline-only sources")
    lines.append("")
    if only_side:
        for s in only_side:
            lines.append(f"- {s}")
    else:
        lines.append("_None found._")

    lines.append("")
    lines.append("## Sources in both mainline and sideline")
    lines.append("")
    if both:
        for s in both:
            lines.append(f"- {s}")
    else:
        lines.append("_None found._")

    lines.append("")
    lines.append("## Full membership table")
    lines.append("")
    lines.append(dataframe_to_markdown(membership))
    lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def build_empty_membership(layers: Dict[str, Set[str]]) -> pd.DataFrame:
    columns = ["source_dataset"]
    columns.extend(sorted(layers.keys()))
    columns.extend(
        [
            "in_any_mainline",
            "in_any_sideline",
            "mainline_final_selected",
        ]
    )
    return pd.DataFrame(columns=columns)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Count source datasets used in mainline and sideline products. "
            "The root is scripts_basin_test/."
        )
    )
    ap.add_argument(
        "--root",
        default=".",
        help=(
            "scripts_basin_test root. Default: auto-detect from current directory. "
            "Example from stats/: --root .."
        ),
    )
    ap.add_argument(
        "--out-dir",
        default="output/sed_reference_release/tables",
        help="Output directory relative to scripts_basin_test root.",
    )
    args = ap.parse_args()

    if args.root == ".":
        scripts_root = find_scripts_basin_test_root(Path.cwd())
    else:
        scripts_root = Path(args.root).resolve()

    output_dir = scripts_root / "output"
    release_dir = output_dir / "sed_reference_release"
    out_dir = scripts_root / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Using scripts_basin_test root: {scripts_root}")
    print(f"Using output dir: {output_dir}")
    print(f"Using release dir: {release_dir}")
    print(f"Using table output dir: {out_dir}")

    layers: Dict[str, Set[str]] = {}
    infos: Dict[str, Dict] = {}

    # -----------------------------------------------------------------
    # Mainline input-level accounting
    # -----------------------------------------------------------------

    sources, info = scan_sources_from_resolution_dirs(
        scripts_root=scripts_root,
        wanted_resolutions=MAINLINE_RESOLUTIONS,
        layer_note="organized daily/monthly/annual input NetCDF paths",
    )
    add_layer(
        layers,
        infos,
        "mainline_organized_inputs_daily_monthly_annual",
        sources,
        info,
    )

    sources, info = read_sources_from_csv(output_dir / "s3_collected_stations.csv")
    add_layer(
        layers,
        infos,
        "mainline_s3_collected_stations",
        sources,
        info,
    )

    sources, info = read_sources_from_csv(output_dir / "s5_basin_clustered_stations.csv")
    add_layer(
        layers,
        infos,
        "mainline_s5_clustered_stations",
        sources,
        info,
    )

    # -----------------------------------------------------------------
    # Mainline product-level accounting
    # -----------------------------------------------------------------

    sources, info = read_sources_from_csv(output_dir / "s6_cluster_quality_order.csv")
    add_layer(
        layers,
        infos,
        "mainline_s6_quality_order_candidates",
        sources,
        info,
    )

    sources, info = read_sources_from_csv(output_dir / "s7_source_station_resolution_catalog.csv")
    add_layer(
        layers,
        infos,
        "mainline_s7_source_station_catalog",
        sources,
        info,
    )

    sources, info = read_sources_from_csv(release_dir / "source_station_catalog.csv")
    add_layer(
        layers,
        infos,
        "mainline_release_source_station_catalog",
        sources,
        info,
    )

    sources, info = read_sources_from_csv(release_dir / "source_dataset_catalog.csv")
    add_layer(
        layers,
        infos,
        "mainline_release_source_dataset_catalog",
        sources,
        info,
    )

    # -----------------------------------------------------------------
    # Sideline: climatology
    # -----------------------------------------------------------------

    sources, info = scan_sources_from_resolution_dirs(
        scripts_root=scripts_root,
        wanted_resolutions=SIDELINE_CLIMATOLOGY_RESOLUTIONS,
        layer_note="organized climatology / annually_climatology input NetCDF paths",
    )
    add_layer(
        layers,
        infos,
        "sideline_climatology_organized_inputs",
        sources,
        info,
    )

    sources, info = read_sources_from_netcdf(release_dir / "sed_reference_climatology.nc")
    add_layer(
        layers,
        infos,
        "sideline_climatology_release_nc",
        sources,
        info,
    )

    # -----------------------------------------------------------------
    # Sideline: satellite validation
    # -----------------------------------------------------------------

    sat_sources_total: Set[str] = set()
    sat_infos: List[Dict] = []

    for path in [
        release_dir / "satellite_validation_catalog.csv",
        output_dir / "s6_satellite_validation_catalog.csv",
    ]:
        sources, info = read_sources_from_csv(path)
        sat_sources_total |= sources
        sat_infos.append(info)

    sources, info = read_sources_from_netcdf(release_dir / "sed_reference_satellite_validation.nc")
    sat_sources_total |= sources
    sat_infos.append(info)

    add_layer(
        layers,
        infos,
        "sideline_satellite_validation_catalog",
        sat_sources_total,
        {
            "path": " | ".join(x.get("path", "") for x in sat_infos),
            "exists": any(bool(x.get("exists", False)) for x in sat_infos),
            "rows": sum(int(x.get("rows", 0) or 0) for x in sat_infos),
            "source_column": "csv_or_netcdf_source_fields",
            "note": "combined satellite validation catalog/nc sources",
        },
    )

    # -----------------------------------------------------------------
    # Sideline: overlap provenance sidecar
    # -----------------------------------------------------------------

    sources, info = read_sources_from_csv(release_dir / "sed_reference_overlap_candidates.csv.gz")
    add_layer(
        layers,
        infos,
        "sideline_overlap_candidates_sidecar",
        sources,
        info,
    )

    # -----------------------------------------------------------------
    # Build layer summary
    # -----------------------------------------------------------------

    summary_rows: List[Dict] = []
    for layer_name in sorted(layers.keys()):
        sources = layers[layer_name]
        info = infos[layer_name]
        summary_rows.append(
            {
                "layer": layer_name,
                "n_source_datasets": len(sources),
                "n_rows_or_files": int(info.get("rows", 0) or 0),
                "exists": bool(info.get("exists", False)),
                "source_column": info.get("source_column", ""),
                "path": info.get("path", ""),
                "note": info.get("note", ""),
                "source_list": "|".join(sorted(sources)),
            }
        )

    layer_summary = pd.DataFrame(summary_rows)

    # -----------------------------------------------------------------
    # Build source membership table
    # -----------------------------------------------------------------

    all_sources: Set[str] = set()
    for source_set in layers.values():
        all_sources |= source_set

    membership_rows: List[Dict] = []

    for source in sorted(all_sources):
        row: Dict[str, object] = {"source_dataset": source}

        for layer_name in sorted(layers.keys()):
            row[layer_name] = int(source in layers[layer_name])

        row["in_any_mainline"] = int(
            row.get("mainline_organized_inputs_daily_monthly_annual", 0)
            or row.get("mainline_s3_collected_stations", 0)
            or row.get("mainline_s5_clustered_stations", 0)
            or row.get("mainline_s6_quality_order_candidates", 0)
            or row.get("mainline_s7_source_station_catalog", 0)
            or row.get("mainline_release_source_station_catalog", 0)
            or row.get("mainline_release_source_dataset_catalog", 0)
        )

        row["in_any_sideline"] = int(
            row.get("sideline_climatology_organized_inputs", 0)
            or row.get("sideline_climatology_release_nc", 0)
            or row.get("sideline_satellite_validation_catalog", 0)
            or row.get("sideline_overlap_candidates_sidecar", 0)
        )

        row["mainline_final_selected"] = int(
            row.get("mainline_release_source_dataset_catalog", 0)
        )

        membership_rows.append(row)

    if membership_rows:
        membership = (
            pd.DataFrame(membership_rows)
            .sort_values("source_dataset")
            .reset_index(drop=True)
        )
    else:
        membership = build_empty_membership(layers)

    # -----------------------------------------------------------------
    # Write outputs
    # -----------------------------------------------------------------

    summary_csv = out_dir / "source_dataset_layer_summary.csv"
    membership_csv = out_dir / "source_dataset_layer_membership.csv"
    report_md = out_dir / "source_dataset_layer_report.md"

    layer_summary.to_csv(summary_csv, index=False)
    membership.to_csv(membership_csv, index=False)
    write_report(report_md, layer_summary, membership, layers)

    # -----------------------------------------------------------------
    # Console report
    # -----------------------------------------------------------------

    print("\n=== Source dataset layer summary ===")
    cols = ["layer", "n_source_datasets", "n_rows_or_files", "exists", "note"]
    if not layer_summary.empty:
        print(layer_summary[cols].to_string(index=False))
    else:
        print("(empty)")

    main_final = layers.get("mainline_release_source_dataset_catalog", set())

    main_input_all = (
        layers.get("mainline_organized_inputs_daily_monthly_annual", set())
        | layers.get("mainline_s3_collected_stations", set())
        | layers.get("mainline_s5_clustered_stations", set())
        | layers.get("mainline_s6_quality_order_candidates", set())
        | layers.get("mainline_s7_source_station_catalog", set())
        | layers.get("mainline_release_source_station_catalog", set())
    )

    main_all = main_input_all | main_final

    entered_not_selected = sorted(main_input_all - main_final)

    side_all = (
        layers.get("sideline_climatology_organized_inputs", set())
        | layers.get("sideline_climatology_release_nc", set())
        | layers.get("sideline_satellite_validation_catalog", set())
        | layers.get("sideline_overlap_candidates_sidecar", set())
    )

    print("\n=== Key counts ===")
    print(f"Mainline final selected datasets:       {len(main_final)}")
    print(f"Any mainline datasets:                  {len(main_all)}")
    print(f"Mainline input but NOT final selected:   {len(entered_not_selected)}")
    print(f"Any sideline datasets:                  {len(side_all)}")
    print(f"Mainline ∩ sideline:                    {len(main_all & side_all)}")
    print(f"Mainline only:                          {len(main_all - side_all)}")
    print(f"Sideline only:                          {len(side_all - main_all)}")

    print("\n=== Mainline final selected source datasets ===")
    if main_final:
        for s in sorted(main_final):
            print(f"- {s}")
    else:
        print("(none found)")

    print("\n=== Mainline input sources NOT selected for final core ===")
    if entered_not_selected:
        for s in entered_not_selected:
            print(f"- {s}")
    else:
        print("(none — all mainline input sources made it to the final core)")

    print("\n=== Sideline-only source datasets ===")
    side_only = sorted(side_all - main_all)
    if side_only:
        for s in side_only:
            print(f"- {s}")
    else:
        print("(none found)")

    print("\nWrote:")
    print(f"- {summary_csv}")
    print(f"- {membership_csv}")
    print(f"- {report_md}")

    # -----------------------------------------------------------------
    # Copy markdown report to docs/reports
    # -----------------------------------------------------------------
    docs_reports_dir = scripts_root / "docs" / "reports"
    try:
        shutil.copy2(report_md, docs_reports_dir)
        print("Copied {} -> {}".format(report_md, docs_reports_dir))
    except Exception as exc:
        print("Warning: could not copy {} to {}: {}".format(report_md, docs_reports_dir, exc), file=sys.stderr)


    return 0


if __name__ == "__main__":
    raise SystemExit(main())