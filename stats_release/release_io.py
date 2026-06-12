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
        return pd.read_csv(checked, **kwargs)

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
    parser.add_argument("--copy-reports", action="store_true", help="Also copy Markdown reports to docs/reports/. Disabled by default to keep stats output self-contained.")
    parser.add_argument("--dpi", type=int, default=300, help="Figure DPI.")


def context_from_args(args) -> ReleaseContext:
    return ReleaseContext(release_dir=Path(args.release_dir), out_dir=Path(args.out_dir), strict_release_only=bool(args.strict_release_only))


def write_csv(df: pd.DataFrame, path: Path) -> Path:
    ensure_parent(path)
    df.to_csv(path, index=False)
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


def split_pipe(value: object) -> list:
    out = []
    for part in clean_text(value).replace(",", "|").split("|"):
        part = clean_text(part)
        if part and part not in out:
            out.append(part)
    return out


def read_text_var(ds, name: str, size=None) -> list:
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
