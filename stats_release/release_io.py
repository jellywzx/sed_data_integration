#!/usr/bin/env python3
"""Small I/O guard used by all release-only statistics scripts."""

from __future__ import annotations

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

import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:
    nc4 = None

from stats_release.release_paths import DEFAULT_RELEASE_DIR, DEFAULT_STATS_ROOT, default_out_dir


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


@dataclass
class ReleaseContext:
    release_dir: Path = DEFAULT_RELEASE_DIR
    out_dir: Path = default_out_dir("misc")
    strict_release_only: bool = True

    def __post_init__(self) -> None:
        self.release_dir = Path(self.release_dir).resolve()
        self.out_dir = Path(self.out_dir).resolve()

    def release_file(self, name: str) -> Path:
        return self.release_dir / name

    def require_input(self, path: Path, required: bool = True) -> Optional[Path]:
        path = Path(path).expanduser().resolve()
        if self.strict_release_only and not is_relative_to(path, self.release_dir):
            raise ValueError("Release-only stats may read only from the release package: {}".format(path))
        if not path.is_file():
            if required:
                raise FileNotFoundError("Required release input not found: {}".format(path))
            return None
        return path

    def figures_dir(self) -> Path:
        """Return the figures subdirectory under out_dir."""
        return ensure_parent(self.out_dir / "figures")

    def output_path(self, *parts: str) -> Path:
        path = (self.out_dir / Path(*parts)).resolve()
        if self.strict_release_only and not is_relative_to(path, self.out_dir):
            raise ValueError("Output path escapes stats output directory: {}".format(path))
        return ensure_parent(path)

    def read_csv(self, name_or_path, required=True, **kwargs):
        path = Path(name_or_path)
        if not path.is_absolute():
            path = self.release_file(str(name_or_path))
        checked = self.require_input(path, required=required)
        if checked is None:
            return pd.DataFrame()
        kwargs.setdefault("keep_default_na", False)
        return normalize_station_aliases(pd.read_csv(checked, **kwargs), checked.name)

    def open_dataset(self, name_or_path, required=True):
        if nc4 is None:
            raise RuntimeError("netCDF4 is required to read release NetCDF products")
        path = Path(name_or_path)
        if not path.is_absolute():
            path = self.release_file(str(name_or_path))
        checked = self.require_input(path, required=required)
        if checked is None:
            return None
        return nc4.Dataset(str(checked), "r")

    def sqlite_connect(self, name_or_path, required=True):
        path = Path(name_or_path)
        if not path.is_absolute():
            path = self.release_file(str(name_or_path))
        checked = self.require_input(path, required=required)
        if checked is None:
            return None
        return sqlite3.connect(str(checked))


def add_common_args(parser, module_name: str) -> None:
    default_output = DEFAULT_STATS_ROOT if module_name == "run_all" else default_out_dir(module_name)
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR), help="Path to the S8 release package directory.")
    parser.add_argument("--out-dir", default=str(default_output), help="Output directory for this release-only statistics module.")
    parser.add_argument("--strict-release-only", action="store_true", default=True, help="Reject input reads outside --release-dir. Enabled by default.")
    parser.add_argument("--allow-non-release-inputs", action="store_false", dest="strict_release_only", help="Disable the input path guard for debugging only.")
    parser.add_argument("--skip-figures", action="store_true", help="Skip PNG figure creation.")
    parser.add_argument(
        "--copy-reports",
        action="store_true",
        default=True,
        help="Also copy Markdown reports to the docs report area. Enabled by default.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Figure DPI.")


def context_from_args(args) -> ReleaseContext:
    return ReleaseContext(release_dir=Path(args.release_dir), out_dir=Path(args.out_dir), strict_release_only=bool(args.strict_release_only))


PUBLIC_OUTPUT_RENAMES = {
    "cluster_uid": "station_uid",
    "cluster_id": "station_reference_id",
    "linked_cluster_uid": "linked_station_uid",
    "linked_cluster_id": "linked_station_reference_id",
    "source_station_cluster_index": "source_station_reference_index",
    "unique_cluster_uid": "unique_station_uid",
    "n_clusters": "n_reference_stations",
    "n_clusters_total": "n_reference_stations_total",
    "n_nonmissing_clusters": "n_nonmissing_reference_stations",
    "cluster_count": "reference_station_count",
    "clusters": "reference_stations",
    "active_clusters": "active_reference_stations",
    "linked_cluster_count": "linked_reference_station_count",
    "satellite_cluster_count": "satellite_reference_station_count",
    "satellite_linked_cluster_count": "satellite_linked_reference_station_count",
    "main_cluster_count": "main_reference_station_count",
    "record_attributed_cluster_count": "record_attributed_reference_station_count",
    "cluster_attributed_record_count": "reference_station_attributed_record_count",
    "final_cluster_count": "final_reference_station_count",
    "daily_cluster_count": "daily_reference_station_count",
    "monthly_cluster_count": "monthly_reference_station_count",
    "annual_cluster_count": "annual_reference_station_count",
    "resolved_cluster_count": "resolved_reference_station_count",
    "unresolved_cluster_count": "unresolved_reference_station_count",
    "unknown_status_cluster_count": "unknown_status_reference_station_count",
    "basin_polygon_cluster_count": "basin_polygon_reference_station_count",
    "upstream_area_valid_cluster_count": "upstream_area_valid_reference_station_count",
    "upstream_area_missing_or_invalid_cluster_count": "upstream_area_missing_or_invalid_reference_station_count",
    "unknown_country_cluster_count": "unknown_country_reference_station_count",
    "valid_latlon_clusters": "valid_latlon_reference_stations",
    "percent_clusters": "percent_reference_stations",
    "fraction_of_valid_area_clusters": "fraction_of_valid_area_reference_stations",
    "pct_of_clusters": "pct_of_reference_stations",
    "cluster_percent_of_source_rows": "reference_station_percent_of_source_rows",
    "total_clusters_sum": "total_reference_stations_sum",
    "total_clusters_source_sum": "total_reference_stations_source_sum",
}
for _var in ("Q", "SSC", "SSL"):
    PUBLIC_OUTPUT_RENAMES["{}_clusters".format(_var)] = "{}_reference_stations".format(_var)
    PUBLIC_OUTPUT_RENAMES["{}_cluster_coverage_pct".format(_var)] = "{}_reference_station_coverage_pct".format(_var)
del _var

PUBLIC_OUTPUT_TEXT_REPLACEMENTS = (
    ("source_station_cluster_index", "source_station_reference_index"),
    ("linked_cluster_uid", "linked_station_uid"),
    ("linked_cluster_id", "linked_station_reference_id"),
    ("cluster_uid", "station_uid"),
    ("cluster_id", "station_reference_id"),
    ("unique_cluster_uid", "unique_station_uid"),
    ("active_clusters", "active_reference_stations"),
    ("n_nonmissing_clusters", "n_nonmissing_reference_stations"),
    ("n_clusters_total", "n_reference_stations_total"),
    ("n_clusters", "n_reference_stations"),
    ("cluster_count", "reference_station_count"),
    ("linked_cluster_count", "linked_reference_station_count"),
    ("satellite_linked_cluster_count", "satellite_linked_reference_station_count"),
    ("satellite_cluster_count", "satellite_reference_station_count"),
    ("record_attributed_cluster_count", "record_attributed_reference_station_count"),
    ("cluster_attributed_record_count", "reference_station_attributed_record_count"),
    ("total_clusters_source_sum", "total_reference_stations_source_sum"),
    ("valid_latlon_clusters", "valid_latlon_reference_stations"),
    ("unknown_country_clusters", "unknown_country_reference_stations"),
    ("clusters_with_valid_lat_lon", "reference_stations_with_valid_lat_lon"),
    ("satellite_by_linked_cluster", "satellite_by_linked_station"),
    ("table_active_clusters_by_year", "table_active_reference_stations_by_year"),
    ("table_satellite_by_linked_cluster", "table_satellite_by_linked_station"),
    ("table_cluster_spatial_attributes", "table_reference_station_spatial_attributes"),
    ("table_unknown_country_region_clusters", "table_unknown_country_region_reference_stations"),
    ("fig_active_clusters_by_year", "fig_active_reference_stations_by_year"),
    ("fig_global_cluster_distribution", "fig_global_station_distribution"),
    ("fig_global_cluster_status_and_basins", "fig_global_station_status_and_basins"),
    ("fig_spatial_coverage_by_region_source_clusters", "fig_spatial_coverage_by_region_source_reference_stations"),
    ("fig_source_contribution_clusters", "fig_source_contribution_reference_stations"),
    ("fig_satellite_contribution_clusters", "fig_satellite_contribution_reference_stations"),
    ("fig_climatology_contribution_clusters", "fig_climatology_contribution_reference_stations"),
    ("fig_qc_top_problem_clusters", "fig_qc_top_problem_reference_stations"),
    ("table_qc_flag_by_cluster", "table_qc_flag_by_reference_station"),
    ("table_qc_flag_problem_clusters", "table_qc_flag_problem_reference_stations"),
    ("clusters_by_resolution", "reference_stations_by_resolution"),
    ("contribution_clusters", "contribution_reference_stations"),
    ("problem_clusters", "problem_reference_stations"),
    ("source_clusters", "source_reference_stations"),
    ("cluster_spatial_attributes", "reference_station_spatial_attributes"),
    ("unknown_country_region_clusters", "unknown_country_region_reference_stations"),
    ("by_linked_cluster", "by_linked_station"),
    ("active_clusters", "active_reference_stations"),
    ("reference clusters", "reference stations"),
    ("Reference clusters", "Reference stations"),
    ("clusters", "reference_stations"),
    ("Clusters", "Reference stations"),
    ("cluster", "station"),
    ("Cluster", "Station"),
)


def public_station_output_name(name: object) -> str:
    text = str(name)
    for old, new in PUBLIC_OUTPUT_TEXT_REPLACEMENTS:
        text = text.replace(old, new)
    return text


def public_station_output_frame(frame: pd.DataFrame, *, rename_values: bool = True) -> pd.DataFrame:
    if frame is None:
        return frame
    out = frame.copy()
    for old, new in PUBLIC_OUTPUT_RENAMES.items():
        if old in out.columns and new in out.columns:
            old_values = out[old]
            new_blank = out[new].astype(str).str.strip().eq("")
            out.loc[new_blank, new] = old_values.loc[new_blank]
            out = out.drop(columns=[old])
        elif old in out.columns:
            out = out.rename(columns={old: new})
    if rename_values:
        for col in ("metric", "unit", "unit_type", "entity", "rank_metric", "label", "summary_level", "category", "uid_var", "catalog_col"):
            if col in out.columns:
                out[col] = out[col].map(lambda value: public_station_output_name(value) if isinstance(value, str) else value)
    return out


def public_station_output_path(path: Path) -> Path:
    path = Path(path)
    new_name = public_station_output_name(path.name)
    return path.with_name(new_name)


def write_csv(df: pd.DataFrame, path: Path) -> Path:
    path = public_station_output_path(Path(path))
    ensure_parent(path)
    rename_values = "parity" not in path.name
    public_station_output_frame(df, rename_values=rename_values).to_csv(path, index=False)
    return path


def write_markdown(lines, path: Path) -> Path:
    ensure_parent(path)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return path


def write_json(data, path: Path) -> Path:
    ensure_parent(path)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def copy_report_to_docs(report_path: Path, enabled: bool) -> Optional[Path]:
    """Optionally copy a Markdown report to the project docs/reports directory."""
    if not enabled:
        return None
    reports_dir = Path(__file__).resolve().parents[1] / "docs" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    target = reports_dir / Path(report_path).name
    shutil.copy2(str(report_path), str(target))
    return target


def file_manifest(root: Path) -> list:
    """Return a stable metadata manifest for files under root."""
    root = Path(root).resolve()
    rows = []
    if not root.exists():
        return rows
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        stat = path.stat()
        rows.append(
            {
                "relative_path": path.relative_to(root).as_posix(),
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )
    return rows


def metadata_fingerprint(root: Path) -> str:
    """Hash file names, sizes, and mtimes for a fast release/run fingerprint."""
    payload = json.dumps(file_manifest(root), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def script_fingerprint(stats_dir: Optional[Path] = None) -> str:
    """Hash stats_release Python source content for reproducibility manifests."""
    root = Path(stats_dir or Path(__file__).resolve().parent).resolve()
    digest = hashlib.sha256()
    for path in sorted(root.glob("*.py")):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def numeric_series(frame: pd.DataFrame, col: str) -> pd.Series:
    if col not in frame.columns:
        return pd.Series([np.nan] * len(frame), index=frame.index, dtype="float64")
    return pd.to_numeric(frame[col], errors="coerce")


def text_series(frame: pd.DataFrame, col: str) -> pd.Series:
    if col not in frame.columns:
        return pd.Series([""] * len(frame), index=frame.index, dtype=object)
    return frame[col].map(clean_text)


def normalize_station_aliases(frame: pd.DataFrame, file_name: str = "") -> pd.DataFrame:
    """Add public/legacy station aliases when reading either naming style."""
    if frame.empty:
        return frame
    out = frame.copy()
    if "station_uid" not in out.columns and "cluster_uid" in out.columns:
        out["station_uid"] = out["cluster_uid"]
    if "station_reference_id" not in out.columns and "cluster_id" in out.columns:
        out["station_reference_id"] = out["cluster_id"]
    if "linked_station_uid" not in out.columns and "linked_cluster_uid" in out.columns:
        out["linked_station_uid"] = out["linked_cluster_uid"]
    if "linked_station_reference_id" not in out.columns and "linked_cluster_id" in out.columns:
        out["linked_station_reference_id"] = out["linked_cluster_id"]
    if "cluster_uid" not in out.columns:
        for col in ("station_uid", "linked_station_uid"):
            if col in out.columns:
                out["cluster_uid"] = out[col]
                break
    if "cluster_id" not in out.columns and "station_reference_id" in out.columns:
        out["cluster_id"] = out["station_reference_id"]
    if "linked_cluster_uid" not in out.columns and "linked_station_uid" in out.columns:
        out["linked_cluster_uid"] = out["linked_station_uid"]
    if "linked_cluster_id" not in out.columns and "linked_station_reference_id" in out.columns:
        out["linked_cluster_id"] = out["linked_station_reference_id"]
    return out


def split_pipe(value: object) -> list:
    out = []
    for part in clean_text(value).replace(",", "|").split("|"):
        part = clean_text(part)
        if part and part not in out:
            out.append(part)
    return out


def read_text_var(ds, name: str, size=None) -> list:
    if name not in ds.variables:
        for alias in {
            "cluster_uid": ("station_uid", "linked_station_uid"),
            "cluster_id": ("station_reference_id", "linked_station_reference_id"),
            "linked_cluster_uid": ("linked_station_uid",),
            "linked_cluster_id": ("linked_station_reference_id",),
            "station_uid": ("cluster_uid",),
            "station_reference_id": ("cluster_id",),
            "linked_station_uid": ("linked_cluster_uid",),
            "linked_station_reference_id": ("linked_cluster_id",),
        }.get(name, ()):
            if alias in ds.variables:
                name = alias
                break
    if name not in ds.variables:
        return [""] * int(size or 0)
    arr = np.asarray(ds.variables[name][:], dtype=object).reshape(-1)
    return [clean_text(item) for item in arr]


def read_numeric_var(ds, name: str, key=slice(None), fill_values=(-9999.0, 1.0e20)):
    if name not in ds.variables:
        return np.asarray([], dtype=np.float64)
    arr = np.ma.asarray(ds.variables[name][key]).astype(np.float64)
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    else:
        arr = np.asarray(arr, dtype=np.float64)
    for fill in fill_values:
        arr[arr == fill] = np.nan
    return arr


def netcdf_record_count(ds) -> int:
    """Return the direct record dimension size used by record-oriented products."""
    for dim_name in ("n_records", "n_satellite_records", "record"):
        if dim_name in ds.dimensions:
            return int(len(ds.dimensions[dim_name]))
    return 0


def count_matrix_selected_cells(ds, row_chunk_size: int = 256) -> Optional[int]:
    """Count selected station-time cells in matrix NetCDF products.

    Matrix products store observations on an n_stations x time grid, so they do
    not have an n_records dimension.  The selected_source_index mask is the
    release contract for cells that correspond to actual source records.
    """
    if "selected_source_index" not in ds.variables:
        return None
    n_stations = int(len(ds.dimensions.get("n_stations", [])))
    selected_total = 0
    row_chunk_size = max(1, int(row_chunk_size))
    for start in range(0, n_stations, row_chunk_size):
        stop = min(start + row_chunk_size, n_stations)
        selected = np.ma.asarray(ds.variables["selected_source_index"][start:stop, :]).filled(-1)
        selected_total += int(np.count_nonzero(selected >= 0))
    return int(selected_total)


def product_exists(ctx: ReleaseContext, file_name: str) -> bool:
    return ctx.release_file(file_name).is_file()


def setup_matplotlib():
    """Import matplotlib with robust environment setup. Returns plt module or raises."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt
