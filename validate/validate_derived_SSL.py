#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Audit recalculated SSL against source-reported SSL across Sed_data datasets.

This is a read-only diagnostic for the ``jellywzx/Sed_data`` first-stage
processing repository.  It addresses the following question:

    For every dataset whose processing code recalculates
    SSL = Q * SSC * 0.0864, does the raw source also contain an independently
    reported SSL field?  If yes, how far is the recalculated SSL from the
    source-reported value?

The script deliberately does *not* use final ``SSL_flag`` as the sole source of
provenance because some dataset scripts declare SSL as non-independent at the
whole-variable level.  Instead it:

1. scans dataset processing scripts for Q-SSC-to-SSL assignments;
2. scans the corresponding raw source directory for SSL-like fields;
3. reads standardized QC NetCDF output to obtain Q and SSC in common units;
4. matches source SSL to standardized records by station and time;
5. compares source-reported SSL with ``Q * SSC * 0.0864``;
6. writes inventory, paired-record, summary, and Markdown report files.

Default directory convention
----------------------------
The repository is expected to be laid out beside ``Source`` and ``Output_r``::

    project/
      Sed_data/       # this repository
      Source/         # original source datasets
      Output_r/       # standardized/QC outputs

The environment variables used by ``code/runtime.py`` are also supported:
``SEDIMENT_SOURCE_ROOT`` and ``SEDIMENT_OUTPUT_ROOT``.

Typical usage
-------------
Place this file at ``Sed_data/validate/validate_recalculated_ssl_against_source.py``
and run from the repository root::

    python validate/validate_recalculated_ssl_against_source.py

Explicit paths::

    python validate/validate_recalculated_ssl_against_source.py \
        --repo-root /path/to/Sed_data \
        --source-root /path/to/Source \
        --output-root /path/to/Output_r

Restrict datasets::

    python validate/validate_recalculated_ssl_against_source.py \
        --datasets EUSEDcollab Bayern HYBAM

Supply a unit when a raw SSL field has no parseable unit::

    python validate/validate_recalculated_ssl_against_source.py \
        --assume-source-unit "DatasetName=t day-1"

Outputs
-------
By default, files are written under::

    Output_r/validation/recalculated_ssl_against_source/

including:

- ``tables/recalculation_script_inventory.csv``
- ``tables/source_ssl_field_inventory.csv``
- ``tables/dataset_audit_funnel.csv``
- ``tables/ssl_source_vs_recalculated_pairs.csv.gz``
- ``tables/ssl_error_summary_by_dataset.csv``
- ``tables/ssl_error_summary_by_dataset_resolution.csv``
- ``tables/ssl_error_summary_by_source_field.csv``
- ``figures/ssl_source_vs_recalculated.png`` (when matplotlib is available)
- ``derived_ssl_source_comparison_report.md``

Interpretation
--------------
The signed error is ``recalculated - source_reported``.  Relative errors are
computed only where source-reported SSL is non-zero.  A match demonstrates
agreement or disagreement with the source-reported value; it does not prove
that either value is an independent field measurement because some providers
may themselves derive reported SSL from discharge and concentration.
"""

from __future__ import annotations

import argparse
import ast
import concurrent.futures
import csv
import json
import math
import multiprocessing
import os
import re
import socket
import sys
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - checked at runtime
    nc4 = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - figures are optional
    plt = None


SSL_FACTOR = 0.0864
FILL_VALUES = (-9999.0, -9999, -99999.0, -99999, 9.96921e36)
SUPPORTED_SOURCE_SUFFIXES = {".csv", ".tsv", ".txt", ".xlsx", ".xls", ".nc", ".nc4", ".cdf"}
# --- Parallel processing helpers (module-level for pickle compatibility) ---

def _discover_inspect_subtree(args):
    """Discover and inspect source files within a subdirectory tree.
    
    Each worker independently walks its assigned subtree, finding files with
    supported suffixes and inspecting them for SSL-like fields.  This avoids
    the single-threaded rglob() bottleneck for huge datasets.
    
    Module-level function required for multiprocessing pickling.
    
    Parameters
    ----------
    args : tuple
        (dataset: str, root_dir: Path, tier: str, max_files: int,
         skip_intermediate: bool, dataset_dir: Path)
    
    Returns
    -------
    Tuple[List[SourceField], int]
        (discovered fields, number of files scanned)
    """
    dataset, root_dir, tier, max_files, skip_intermediate, dataset_dir = args
    results = []
    scanned = 0
    
    # Collect files with supported suffixes by walking the subtree
    file_paths = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for fn in filenames:
            if Path(fn).suffix.lower() in SUPPORTED_SOURCE_SUFFIXES:
                file_paths.append(Path(dirpath) / fn)
    
    # Apply intermediate-path filter if requested
    if skip_intermediate:
        file_paths = [p for p in file_paths
                      if not is_intermediate_source_path(p, dataset_dir)]
    
    for path in file_paths:
        if max_files and scanned >= max_files:
            break
        scanned += 1
        try:
            suffix = path.suffix.lower()
            if suffix in {".csv", ".tsv", ".txt"}:
                results.extend(inspect_delimited_file(dataset, path, tier))
            elif suffix in {".xlsx", ".xls"}:
                results.extend(inspect_excel_file(dataset, path, tier))
            elif suffix in {".nc", ".nc4", ".cdf"}:
                results.extend(inspect_netcdf_file(dataset, path, tier))
        except Exception:
            pass
    return results, scanned


def _read_source_single(args):
    """Read source rows for a single field (module-level for multiprocessing).
    
    Parameters
    ----------
    args : tuple
        (SourceField, assumptions: Dict[str, str])
    
    Returns
    -------
    pd.DataFrame
    """
    field, assumptions = args
    try:
        return read_source_rows(field, assumptions, AuditLog())
    except Exception:
        return pd.DataFrame()


def _read_output_single(args):
    """Read a single output NetCDF file (module-level for multiprocessing).
    
    Parameters
    ----------
    args : tuple
        (dataset: str, path: Path)
    
    Returns
    -------
    pd.DataFrame
    """
    dataset, path = args
    try:
        return read_output_nc(dataset, path, AuditLog())
    except Exception:
        return pd.DataFrame()


def _get_default_workers() -> int:
    """Return a sensible default worker count."""
    cpu_count = multiprocessing.cpu_count()
    return max(1, min(cpu_count - 1, 16))


EXCLUDED_REPO_TOP_LEVEL = {
    ".git",
    ".github",
    "code",
    "docs",
    "test",
    "tests",
    "tools",
    "validate",
    "validation",
    "__pycache__",
}

# These folders commonly contain products produced by the processing scripts,
# not original provider files.  They are skipped in the first pass.  When a
# dataset has no raw-like SSL field, a second pass may inspect them and clearly
# labels the result as an intermediate fallback.
INTERMEDIATE_SOURCE_SEGMENTS = {
    "qc",
    "output",
    "outputs",
    "processed",
    "derived",
    "standardized",
    "netcdf_output",
    "netcdf_outputs",
    "netcdf_output_ss",
    "netcdf_output_bs",
}

Q_VAR_ALIASES = (
    "Q",
    "q",
    "discharge",
    "Discharge",
    "Discharge_m3_s",
    "flow",
    "streamflow",
)
SSC_VAR_ALIASES = (
    "SSC",
    "ssc",
    "TSS",
    "tss",
    "TSS_mg_L",
    "suspended_sediment_concentration",
)
SSL_VAR_ALIASES = (
    "SSL",
    "ssl",
    "sediment_load",
    "Sediment_load",
    "sediment_flux",
    "suspended_sediment_load",
    "suspended_sediment_discharge",
)
TIME_VAR_ALIASES = (
    "time",
    "Time",
    "date",
    "Date",
    "datetime",
    "timestamp",
    "sample_date",
)
STATION_ATTR_ALIASES = (
    "station_id",
    "Source_ID",
    "source_id",
    "Station_ID",
    "site_no",
    "site_id",
    "location_id",
    "Catchment ID",
)
STATION_NAME_ATTR_ALIASES = (
    "station_name",
    "Station_Name",
    "site_name",
    "name",
)
RESOLUTION_ATTR_ALIASES = (
    "temporal_resolution",
    "Temporal_Resolution",
    "resolution",
    "time_resolution",
)

DATE_COLUMN_ALIASES = {
    "date",
    "datetime",
    "time",
    "timestamp",
    "sampledate",
    "measurementdate",
    "startdate",
    "samplingdate",
}
STATION_COLUMN_ALIASES = {
    "stationid",
    "stationcode",
    "stationnumber",
    "sourceid",
    "siteid",
    "siteno",
    "sitecode",
    "gaugeid",
    "gaugeno",
    "locationid",
    "catchmentid",
    "riverstation",
}
STATION_NAME_COLUMN_ALIASES = {
    "stationname",
    "sitename",
    "locationname",
    "catchmentname",
}

DERIVATION_FACTOR_MARKERS = {
    "0.0864",
    "ssc_discharge_to_ssl_factor",
    "ssl_factor",
}
DERIVATION_FUNCTION_MARKERS = {
    "calculate_ssl",
    "derive_ssl",
    "compute_ssl",
    "recalculate_ssl",
    "calculate_sediment_load",
    "compute_sediment_load",
}


@dataclass
class ScriptEvidence:
    dataset: str
    script_path: str
    evidence_type: str
    line_number: int
    evidence: str


@dataclass
class SourceField:
    dataset: str
    file_path: str
    source_tier: str
    container: str
    ssl_field: str
    raw_unit: str
    normalized_unit: str
    unit_status: str
    station_field: str = ""
    station_name_field: str = ""
    time_field: str = ""
    score: int = 0
    notes: str = ""


@dataclass
class AuditLog:
    rows: List[Dict[str, object]] = field(default_factory=list)

    def add(self, level: str, dataset: str, stage: str, message: str, path: str = "") -> None:
        self.rows.append(
            {
                "time": datetime.now().isoformat(timespec="seconds"),
                "level": level,
                "dataset": dataset,
                "stage": stage,
                "path": path,
                "message": message,
            }
        )


# -----------------------------------------------------------------------------
# General helpers
# -----------------------------------------------------------------------------

def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def normalize_token(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def normalize_station(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    # Preserve alphanumeric identity while removing formatting differences.
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def safe_numeric(values: object) -> np.ndarray:
    if np.ma.isMaskedArray(values):
        values = np.ma.filled(values, np.nan)
    arr = pd.to_numeric(pd.Series(np.asarray(values).reshape(-1)), errors="coerce").to_numpy(dtype=float)
    for fill in FILL_VALUES:
        arr[np.isclose(arr, float(fill), rtol=1e-7, atol=1e-7)] = np.nan
    return arr


def first_existing_name(names: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    name_list = list(names)
    exact = set(name_list)
    for candidate in candidates:
        if candidate in exact:
            return candidate
    lower_map = {str(name).lower(): str(name) for name in name_list}
    for candidate in candidates:
        found = lower_map.get(candidate.lower())
        if found is not None:
            return found
    return None


def get_first_attr(obj: object, names: Sequence[str]) -> str:
    attrs: List[str]
    try:
        attrs = list(obj.ncattrs())
    except Exception:
        attrs = []
    attr_map = {str(name).lower(): str(name) for name in attrs}
    for name in names:
        actual = attr_map.get(name.lower(), name)
        try:
            value = getattr(obj, actual)
        except Exception:
            continue
        text = clean_text(value)
        if text:
            return text
    return ""


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def locate_repo_root(start: Optional[str] = None) -> Path:
    start_path = Path(start).expanduser().resolve() if start else Path(__file__).resolve()
    if start_path.is_file():
        start_path = start_path.parent
    for candidate in [start_path, *start_path.parents]:
        if (candidate / "code" / "runtime.py").is_file():
            return candidate
    raise FileNotFoundError(
        "Could not locate Sed_data repository root. Pass --repo-root explicitly."
    )


def resolve_roots(args: argparse.Namespace) -> Tuple[Path, Path, Path, Path]:
    repo_root = locate_repo_root(args.repo_root or None)
    source_root = (
        Path(args.source_root).expanduser().resolve()
        if args.source_root
        else Path(os.environ.get("SEDIMENT_SOURCE_ROOT", repo_root.parent / "Source")).expanduser().resolve()
    )
    output_root = (
        Path(args.output_root).expanduser().resolve()
        if args.output_root
        else Path(os.environ.get("SEDIMENT_OUTPUT_ROOT", repo_root.parent / "Output_r")).expanduser().resolve()
    )
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else output_root / "validation" / "recalculated_ssl_against_source"
    )
    return repo_root, source_root, output_root, out_dir


def find_dataset_dir(root: Path, dataset: str) -> Optional[Path]:
    direct = root / dataset
    if direct.is_dir():
        return direct
    target = normalize_token(dataset)
    matches = [p for p in root.iterdir() if p.is_dir() and normalize_token(p.name) == target] if root.is_dir() else []
    return matches[0] if len(matches) == 1 else None


def is_intermediate_source_path(path: Path, dataset_dir: Path) -> bool:
    try:
        relative_parts = path.relative_to(dataset_dir).parts[:-1]
    except Exception:
        relative_parts = path.parts[:-1]
    normalized = {normalize_token(part) for part in relative_parts}
    return bool(normalized.intersection({normalize_token(x) for x in INTERMEDIATE_SOURCE_SEGMENTS}))


def infer_resolution(path: Path, explicit: str = "") -> str:
    text = " ".join([explicit, *path.parts]).lower()
    if "climat" in text:
        return "climatology"
    if "month" in text or "quarter" in text:
        return "monthly"
    if "annual" in text or "yearly" in text:
        return "annual"
    if "daily" in text or "day" in text:
        return "daily"
    return "other"


def infer_station_from_filename(path: Path, dataset: str) -> str:
    stem = path.stem
    patterns = (
        r"(?:station|site|gauge|id)[_-]*([A-Za-z0-9.]+)",
        r"ID[_-]*([A-Za-z0-9.]+)",
        r"([0-9]{4,})",
    )
    for pattern in patterns:
        match = re.search(pattern, stem, flags=re.IGNORECASE)
        if match:
            return clean_text(match.group(1))

    cleaned = re.sub(re.escape(dataset), "", stem, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"(?i)(qc|processed|standardized|output|daily|monthly|annual|data|sediment|river)",
        " ",
        cleaned,
    )
    tokens = [token for token in re.split(r"[^A-Za-z0-9.]+", cleaned) if token]
    return tokens[-1] if len(tokens) == 1 else ""


# -----------------------------------------------------------------------------
# Processing-script discovery
# -----------------------------------------------------------------------------

def _target_tokens(node: ast.AST) -> List[str]:
    tokens: List[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Name):
            tokens.append(child.id.lower())
        elif isinstance(child, ast.Attribute):
            tokens.append(child.attr.lower())
        elif isinstance(child, ast.Constant) and isinstance(child.value, str):
            tokens.append(child.value.lower())
    return tokens


def _expression_identifiers(node: ast.AST) -> List[str]:
    ids: List[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Name):
            ids.append(child.id.lower())
        elif isinstance(child, ast.Attribute):
            ids.append(child.attr.lower())
        elif isinstance(child, ast.Constant):
            ids.append(clean_text(child.value).lower())
    return ids


def _contains_any_token(tokens: Sequence[str], markers: Sequence[str]) -> bool:
    for token in tokens:
        compact = normalize_token(token)
        for marker in markers:
            marker_compact = normalize_token(marker)
            if marker_compact and marker_compact in compact:
                return True
    return False


def script_derivation_evidence(dataset: str, script_path: Path, repo_root: Path) -> List[ScriptEvidence]:
    try:
        text = script_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []

    evidence: List[ScriptEvidence] = []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        tree = None

    if tree is not None:
        for node in ast.walk(tree):
            target_nodes: List[ast.AST] = []
            value_node: Optional[ast.AST] = None
            if isinstance(node, ast.Assign):
                target_nodes = list(node.targets)
                value_node = node.value
            elif isinstance(node, ast.AnnAssign):
                target_nodes = [node.target]
                value_node = node.value
            elif isinstance(node, ast.AugAssign):
                target_nodes = [node.target]
                value_node = node.value
            if not target_nodes or value_node is None:
                continue

            targets = [token for target in target_nodes for token in _target_tokens(target)]
            expr_ids = _expression_identifiers(value_node)
            try:
                segment = ast.get_source_segment(text, node) or ast.unparse(node)
            except Exception:
                segment = ""
            segment_lower = segment.lower()

            target_is_ssl = _contains_any_token(
                targets,
                ["ssl", "sediment_load", "sedimentload", "sediment_flux", "sedimentflux"],
            )
            has_q = _contains_any_token(expr_ids, ["q", "discharge", "streamflow", "flow"])
            has_ssc = _contains_any_token(expr_ids, ["ssc", "tss", "concentration"])
            has_factor = any(marker in segment_lower for marker in DERIVATION_FACTOR_MARKERS)
            has_factor = has_factor or _contains_any_token(expr_ids, list(DERIVATION_FACTOR_MARKERS))
            has_function = _contains_any_token(expr_ids, list(DERIVATION_FUNCTION_MARKERS))

            if target_is_ssl and has_q and has_ssc and (has_factor or has_function):
                evidence.append(
                    ScriptEvidence(
                        dataset=dataset,
                        script_path=str(script_path.relative_to(repo_root)),
                        evidence_type="ast_assignment",
                        line_number=int(getattr(node, "lineno", 0) or 0),
                        evidence=" ".join(segment.strip().split())[:1000],
                    )
                )

    # Fallback line-level evidence is used only when the file cannot be parsed as
    # Python.  This avoids treating a docstring that merely states the formula
    # as proof that the script actually recalculates SSL.
    if tree is None:
        lines = text.splitlines()
        for index, line in enumerate(lines, 1):
            lower = line.lower()
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if not any(marker in lower for marker in DERIVATION_FACTOR_MARKERS):
                continue
            if not re.search(r"\b(ssl|sediment[_ ]?(load|flux))\b", lower):
                continue
            if not re.search(r"\b(q|discharge|flow)\b", lower):
                continue
            if not re.search(r"\b(ssc|tss|concentration)\b", lower):
                continue
            if "=" not in line or not any(op in line for op in ("*", "np.multiply", "calculate_ssl", "derive_ssl", "compute_ssl")):
                continue
            normalized_line = " ".join(line.strip().split())
            evidence.append(
                ScriptEvidence(
                    dataset=dataset,
                    script_path=str(script_path.relative_to(repo_root)),
                    evidence_type="text_line_unparsed_file",
                    line_number=index,
                    evidence=normalized_line[:1000],
                )
            )
    return evidence


def discover_recalculation_scripts(repo_root: Path, selected_datasets: Sequence[str]) -> pd.DataFrame:
    selected_norm = {normalize_token(x) for x in selected_datasets}
    rows: List[Dict[str, object]] = []
    for top_dir in sorted(p for p in repo_root.iterdir() if p.is_dir()):
        if top_dir.name in EXCLUDED_REPO_TOP_LEVEL or top_dir.name.startswith("."):
            continue
        dataset = top_dir.name
        if selected_norm and normalize_token(dataset) not in selected_norm:
            continue
        for script in sorted(top_dir.rglob("*.py")):
            if any(part in {"__pycache__", "test", "tests"} for part in script.parts):
                continue
            for item in script_derivation_evidence(dataset, script, repo_root):
                rows.append(item.__dict__)
    columns = ["dataset", "script_path", "evidence_type", "line_number", "evidence"]
    return pd.DataFrame(rows, columns=columns)


# -----------------------------------------------------------------------------
# Raw source field discovery
# -----------------------------------------------------------------------------

def ssl_field_score(name: str) -> int:
    raw = clean_text(name)
    compact = normalize_token(raw)
    if not compact:
        return -100
    reject = ("flag", "qc", "quality", "uncert", "error", "ratio", "derived", "estimateflag")
    if any(token in compact for token in reject):
        return -100
    # Sediment yield needs drainage area and is not directly comparable to load.
    if "yield" in compact and "load" not in compact:
        return -80

    exact_high = {
        "ssl",
        "sedimentload",
        "suspendedsedimentload",
        "sedimentflux",
        "suspendedsedimentflux",
        "suspendedsedimentdischarge",
        "sedimentdischarge",
    }
    if compact in exact_high:
        return 100
    if compact.startswith("ssl") or compact.endswith("ssl"):
        return 95
    if "suspendedsediment" in compact and any(x in compact for x in ("load", "flux", "discharge", "transport")):
        return 90
    if "sediment" in compact and any(x in compact for x in ("load", "flux", "discharge", "transport")):
        return 80
    if compact in {"qs", "qss", "sslvalue"}:
        return 70
    return -50


def station_field_score(name: str) -> int:
    compact = normalize_token(name)
    if compact in STATION_COLUMN_ALIASES:
        return 100
    if "station" in compact and any(x in compact for x in ("id", "code", "number", "no")):
        return 90
    if "site" in compact and any(x in compact for x in ("id", "code", "number", "no")):
        return 85
    if "gauge" in compact and any(x in compact for x in ("id", "code", "number", "no")):
        return 80
    return -50


def station_name_field_score(name: str) -> int:
    compact = normalize_token(name)
    if compact in STATION_NAME_COLUMN_ALIASES:
        return 100
    if ("station" in compact or "site" in compact) and "name" in compact:
        return 80
    return -50


def time_field_score(name: str) -> int:
    compact = normalize_token(name)
    if compact in DATE_COLUMN_ALIASES:
        return 100
    if "date" in compact or "datetime" in compact or "timestamp" in compact:
        return 90
    if compact == "time":
        return 85
    return -50


def best_scored_name(names: Sequence[str], scorer) -> Tuple[str, int]:
    scored = sorted(((scorer(name), str(name)) for name in names), reverse=True)
    if not scored or scored[0][0] < 0:
        return "", -1
    return scored[0][1], int(scored[0][0])


def normalize_unit_text(value: str) -> str:
    text = clean_text(value).lower()
    replacements = {
        "³": "3",
        "²": "2",
        "⁻": "-",
        "−": "-",
        "–": "-",
        "—": "-",
        "·": " ",
        "_": " ",
        "per": "/",
        "tonnes": "t",
        "tonne": "t",
        "tons": "t",
        "ton": "t",
        "metrictons": "t",
        "metric tons": "t",
        "megatonnes": "mt",
        "megatonne": "mt",
        "litres": "l",
        "liters": "l",
        "litre": "l",
        "liter": "l",
        "years": "yr",
        "year": "yr",
        "months": "month",
        "days": "day",
        "day": "d",
        "seconds": "s",
        "second": "s",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = text.replace("^", "")
    text = re.sub(r"[\[\](){}]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def unit_from_name(name: str) -> str:
    raw = clean_text(name)
    bracketed = re.findall(r"[\[(]([^\])]+)[\])]", raw)
    candidates = bracketed + [raw]
    for candidate in candidates:
        normalized = normalize_unit_text(candidate)
        if any(token in normalized for token in ("kg", "mg", " g", "t", "mt")) and any(
            token in normalized for token in ("/", "-1", " day", " d", " yr", " month", " s")
        ):
            return candidate
    return ""


def classify_ssl_unit(raw_unit: str) -> Tuple[str, str]:
    """Return (normalized unit label, status)."""
    u = normalize_unit_text(raw_unit)
    compact = re.sub(r"\s+", "", u)
    if not u:
        return "", "missing"

    def rate_has(period: str, aliases: Sequence[str]) -> bool:
        return any(alias in compact for alias in aliases)

    per_second = rate_has("second", ("/s", "s-1"))
    per_day = rate_has("day", ("/d", "d-1", "/day", "day-1"))
    per_month = rate_has("month", ("/month", "month-1", "/mo", "mo-1", "m-1"))
    per_year = rate_has("year", ("/yr", "yr-1", "/y", "y-1", "/a", "a-1"))
    per_event = "event" in compact

    mass = ""
    if "mg" in compact:
        mass = "mg"
    elif "kg" in compact:
        mass = "kg"
    elif "mt" in compact or "megaton" in compact:
        mass = "Mt"
    elif re.search(r"(^|[^a-z])t([^a-z]|$)", u) or compact.startswith("t/") or compact.startswith("td-"):
        mass = "t"
    elif compact.startswith("g/") or re.search(r"(^|[^a-z])g([^a-z]|$)", u):
        mass = "g"

    period = ""
    if per_second:
        period = "s"
    elif per_day:
        period = "d"
    elif per_month:
        period = "month"
    elif per_year:
        period = "yr"
    elif per_event:
        period = "event"

    if mass and period:
        if period == "event":
            return f"{mass} event-1", "unsupported_event_rate"
        return f"{mass} {period}-1", "ok"
    return u, "unrecognized"


def convert_ssl_to_t_day(values: np.ndarray, normalized_unit: str, dates: pd.Series) -> np.ndarray:
    values = np.asarray(values, dtype=float)
    out = np.full(values.shape, np.nan, dtype=float)
    valid = np.isfinite(values)
    if not valid.any():
        return out

    mass, period = normalized_unit.split(" ", 1)
    period = period.replace("-1", "")
    mass_to_ton = {
        "mg": 1e-9,
        "g": 1e-6,
        "kg": 1e-3,
        "t": 1.0,
        "Mt": 1e6,
    }[mass]

    mass_ton = values * mass_to_ton
    if period == "s":
        out[valid] = mass_ton[valid] * 86400.0
    elif period == "d":
        out[valid] = mass_ton[valid]
    elif period == "yr":
        out[valid] = mass_ton[valid] / 365.25
    elif period == "month":
        date_series = pd.to_datetime(dates, errors="coerce")
        days = date_series.dt.days_in_month.to_numpy(dtype=float)
        good = valid & np.isfinite(days) & (days > 0)
        out[good] = mass_ton[good] / days[good]
    return out


def detect_delimiter(path: Path, encoding: str) -> str:
    default = "\t" if path.suffix.lower() == ".tsv" else ","
    try:
        sample = path.read_text(encoding=encoding, errors="ignore")[:65536]
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        return dialect.delimiter
    except Exception:
        return default


def read_delimited_header(path: Path) -> Tuple[pd.DataFrame, str, str]:
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        delimiter = detect_delimiter(path, encoding)
        try:
            frame = pd.read_csv(path, sep=delimiter, nrows=50, encoding=encoding, low_memory=False)
            return frame, encoding, delimiter
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Could not read delimited file header: {last_error}")


def inspect_delimited_file(dataset: str, path: Path, tier: str) -> List[SourceField]:
    frame, _, _ = read_delimited_header(path)
    columns = [str(c) for c in frame.columns]
    candidates = sorted(
        ((ssl_field_score(name), name) for name in columns),
        reverse=True,
    )
    candidates = [(score, name) for score, name in candidates if score >= 70]
    if not candidates:
        return []
    station_field, _ = best_scored_name(columns, station_field_score)
    station_name_field, _ = best_scored_name(columns, station_name_field_score)
    time_field, _ = best_scored_name(columns, time_field_score)
    fields: List[SourceField] = []
    for score, ssl_field in candidates:
        raw_unit = unit_from_name(ssl_field)
        normalized_unit, status = classify_ssl_unit(raw_unit)
        fields.append(
            SourceField(
                dataset=dataset,
                file_path=str(path),
                source_tier=tier,
                container="delimited",
                ssl_field=ssl_field,
                raw_unit=raw_unit,
                normalized_unit=normalized_unit,
                unit_status=status,
                station_field=station_field,
                station_name_field=station_name_field,
                time_field=time_field,
                score=score,
            )
        )
    return fields


def inspect_excel_file(dataset: str, path: Path, tier: str) -> List[SourceField]:
    fields: List[SourceField] = []
    book = pd.ExcelFile(path)
    for sheet in book.sheet_names:
        try:
            frame = pd.read_excel(path, sheet_name=sheet, nrows=50)
        except Exception:
            continue
        columns = [str(c) for c in frame.columns]
        candidates = sorted(
            ((ssl_field_score(name), name) for name in columns),
            reverse=True,
        )
        candidates = [(score, name) for score, name in candidates if score >= 70]
        if not candidates:
            continue
        station_field, _ = best_scored_name(columns, station_field_score)
        station_name_field, _ = best_scored_name(columns, station_name_field_score)
        time_field, _ = best_scored_name(columns, time_field_score)
        for score, ssl_field in candidates:
            raw_unit = unit_from_name(ssl_field)
            normalized_unit, status = classify_ssl_unit(raw_unit)
            fields.append(
                SourceField(
                    dataset=dataset,
                    file_path=str(path),
                    source_tier=tier,
                    container=f"excel:{sheet}",
                    ssl_field=ssl_field,
                    raw_unit=raw_unit,
                    normalized_unit=normalized_unit,
                    unit_status=status,
                    station_field=station_field,
                    station_name_field=station_name_field,
                    time_field=time_field,
                    score=score,
                )
            )
    return fields


def inspect_netcdf_file(dataset: str, path: Path, tier: str) -> List[SourceField]:
    if nc4 is None:
        return []
    fields: List[SourceField] = []
    with nc4.Dataset(path, "r") as ds:
        names = list(ds.variables)
        candidates = sorted(
            ((ssl_field_score(name), name) for name in names), reverse=True
        )
        for score, ssl_field in candidates:
            if score < 0:
                continue
            var = ds.variables[ssl_field]
            raw_unit = clean_text(getattr(var, "units", "")) or unit_from_name(ssl_field)
            normalized_unit, status = classify_ssl_unit(raw_unit)
            time_field = first_existing_name(names, TIME_VAR_ALIASES) or ""
            fields.append(
                SourceField(
                    dataset=dataset,
                    file_path=str(path),
                    source_tier=tier,
                    container="netcdf",
                    ssl_field=ssl_field,
                    raw_unit=raw_unit,
                    normalized_unit=normalized_unit,
                    unit_status=status,
                    station_field=get_first_attr(ds, STATION_ATTR_ALIASES),
                    station_name_field=get_first_attr(ds, STATION_NAME_ATTR_ALIASES),
                    time_field=time_field,
                    score=int(score),
                    notes="station_field and station_name_field contain global-attribute values for NetCDF sources",
                )
            )
    return fields


def inspect_source_file(dataset: str, path: Path, tier: str, log: AuditLog) -> List[SourceField]:
    try:
        suffix = path.suffix.lower()
        if suffix in {".csv", ".tsv", ".txt"}:
            return inspect_delimited_file(dataset, path, tier)
        if suffix in {".xlsx", ".xls"}:
            return inspect_excel_file(dataset, path, tier)
        if suffix in {".nc", ".nc4", ".cdf"}:
            return inspect_netcdf_file(dataset, path, tier)
    except Exception as exc:
        log.add("WARN", dataset, "source_field_inspection", str(exc), str(path))
    return []


def discover_source_fields(
    dataset: str,
    dataset_dir: Path,
    include_intermediate: bool,
    max_files: int,
    log: AuditLog,
    workers: int = 0,
) -> Tuple[List[SourceField], int]:
    """Discover SSL-like fields in source files, optionally in parallel.

    When *workers* > 1, each worker is assigned a subdirectory of the dataset
    and independently walks it (os.walk), filters intermediates, and inspects
    files — so both file discovery and inspection are parallelised.  This avoids
    the single-threaded ``rglob()`` bottleneck for huge datasets like USGS.

    Parameters
    ----------
    workers : int
        0 or 1 = sequential; > 1 = parallel with that many workers.
    """
    _workers = max(1, workers) if workers else 1

    # --- Sequential path (original behaviour) ---
    if _workers <= 1:
        all_files = [
            path
            for path in dataset_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in SUPPORTED_SOURCE_SUFFIXES
        ]
        raw_like = [p for p in all_files if not is_intermediate_source_path(p, dataset_dir)]
        intermediate = [p for p in all_files if p not in raw_like]

        fields: List[SourceField] = []
        scanned = 0
        for p in raw_like:
            if max_files and scanned >= max_files:
                break
            scanned += 1
            fields.extend(inspect_source_file(dataset, p, "raw", log))

        remaining = max_files - scanned if max_files else 0
        if include_intermediate or not fields:
            for p in intermediate:
                if remaining is not None and remaining <= 0:
                    break
                scanned += 1
                fields.extend(inspect_source_file(dataset, p, "intermediate_fallback", log))
                if max_files:
                    remaining -= 1
        return fields, scanned

    # --- Parallel path ---
    # Collect top-level subdirectories so each worker gets its own subtree
    subdirs = sorted(
        [dataset_dir / d for d in os.listdir(dataset_dir)
         if (dataset_dir / d).is_dir()],
        key=lambda d: d.name,
    )
    if not subdirs:
        subdirs = [dataset_dir]

    # Distribute subdirectories across workers (round-robin so workloads
    # are roughly balanced when subdirectory sizes vary)
    per_worker: List[List[Path]] = [[] for _ in range(min(_workers, len(subdirs)))]
    for i, sd in enumerate(subdirs):
        per_worker[i % len(per_worker)].append(sd)

    # Build work items: each worker now gets one TaskArgs with multiple dirs
    # but we submit per top-level subdir for finer granularity
    per_dir_max = max_files // len(subdirs) + 1 if max_files else 0

    raw_args = [
        (dataset, sd, "raw", per_dir_max, True, dataset_dir)
        for sd in subdirs
    ]
    int_args = [
        (dataset, sd, "intermediate_fallback", per_dir_max, False, dataset_dir)
        for sd in subdirs
    ]

    fields: List[SourceField] = []
    scanned = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=_workers) as executor:
        # First pass: raw-like files
        futures = {
            executor.submit(_discover_inspect_subtree, arg): arg[1]
            for arg in raw_args
        }
        for future in concurrent.futures.as_completed(futures):
            subdir = futures[future]
            try:
                batch_fields, batch_scanned = future.result()
                fields.extend(batch_fields)
                scanned += batch_scanned
            except Exception as exc:
                log.add("WARN", dataset, "parallel_discover", str(exc), str(subdir))

        # Second pass: intermediate fallback (only when needed)
        if include_intermediate or not fields:
            remaining = max_files - scanned if max_files else 0
            int_futures = {
                executor.submit(_discover_inspect_subtree, arg): arg[1]
                for arg in int_args
            }
            for future in concurrent.futures.as_completed(int_futures):
                subdir = int_futures[future]
                if max_files and scanned >= max_files:
                    future.cancel()
                    continue
                try:
                    batch_fields, batch_scanned = future.result()
                    if max_files:
                        allowed = max_files - scanned
                        batch_fields = batch_fields[:allowed]
                        batch_scanned = min(batch_scanned, allowed)
                    fields.extend(batch_fields)
                    scanned += batch_scanned
                except Exception as exc:
                    log.add("WARN", dataset, "parallel_discover_int", str(exc), str(subdir))

    return fields, scanned


# -----------------------------------------------------------------------------
# Source value readers
# -----------------------------------------------------------------------------

def parse_frame_dates(frame: pd.DataFrame, explicit_field: str) -> Tuple[pd.Series, str]:
    if explicit_field and explicit_field in frame.columns:
        return pd.to_datetime(frame[explicit_field], errors="coerce", dayfirst=False), explicit_field

    columns = [str(c) for c in frame.columns]
    date_field, _ = best_scored_name(columns, time_field_score)
    if date_field:
        values = pd.to_datetime(frame[date_field], errors="coerce", dayfirst=False)
        # Retry day-first when it resolves more values.
        values_dayfirst = pd.to_datetime(frame[date_field], errors="coerce", dayfirst=True)
        if values_dayfirst.notna().sum() > values.notna().sum():
            values = values_dayfirst
        return values, date_field

    normalized = {normalize_token(c): c for c in columns}
    year_col = next((normalized[key] for key in normalized if key in {"year", "yr"}), None)
    month_col = next((normalized[key] for key in normalized if key in {"month", "mon"}), None)
    day_col = next((normalized[key] for key in normalized if key in {"day", "dy"}), None)
    if year_col:
        payload = {
            "year": pd.to_numeric(frame[year_col], errors="coerce"),
            "month": pd.to_numeric(frame[month_col], errors="coerce") if month_col else 1,
            "day": pd.to_numeric(frame[day_col], errors="coerce") if day_col else 1,
        }
        return pd.to_datetime(payload, errors="coerce"), "+".join(x for x in (year_col, month_col, day_col) if x)
    return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]"), ""


def parse_frame_station(
    frame: pd.DataFrame,
    explicit_field: str,
    explicit_name_field: str,
    path: Path,
    dataset: str,
) -> Tuple[pd.Series, pd.Series, str, str]:
    columns = [str(c) for c in frame.columns]
    station_field = explicit_field if explicit_field in columns else best_scored_name(columns, station_field_score)[0]
    station_name_field = (
        explicit_name_field if explicit_name_field in columns else best_scored_name(columns, station_name_field_score)[0]
    )

    if station_field:
        station = frame[station_field].map(clean_text)
    else:
        inferred = infer_station_from_filename(path, dataset)
        station = pd.Series(inferred, index=frame.index, dtype=object)

    if station_name_field:
        station_name = frame[station_name_field].map(clean_text)
    else:
        station_name = pd.Series("", index=frame.index, dtype=object)
    return station, station_name, station_field, station_name_field


def apply_assumed_unit(field: SourceField, assumptions: Mapping[str, str]) -> SourceField:
    if field.unit_status == "ok":
        return field
    assumed = assumptions.get(normalize_token(field.dataset), "")
    if not assumed:
        return field
    normalized, status = classify_ssl_unit(assumed)
    if status != "ok":
        return field
    copied = SourceField(**field.__dict__)
    copied.raw_unit = assumed
    copied.normalized_unit = normalized
    copied.unit_status = "assumed"
    copied.notes = (copied.notes + " | " if copied.notes else "") + "unit supplied with --assume-source-unit"
    return copied


def read_delimited_source_rows(field: SourceField, assumptions: Mapping[str, str], log: AuditLog) -> pd.DataFrame:
    path = Path(field.file_path)
    header, encoding, delimiter = read_delimited_header(path)
    usecols = {field.ssl_field}
    for name in (field.station_field, field.station_name_field, field.time_field):
        if name:
            usecols.add(name)
    # Include possible split date columns and station identifiers not caught in the header pass.
    for col in header.columns:
        compact = normalize_token(col)
        if compact in {"year", "yr", "month", "mon", "day", "dy"}:
            usecols.add(str(col))
        if station_field_score(str(col)) >= 0 or station_name_field_score(str(col)) >= 0:
            usecols.add(str(col))

    try:
        frame = pd.read_csv(
            path,
            sep=delimiter,
            encoding=encoding,
            usecols=lambda c: str(c) in usecols,
            low_memory=False,
        )
    except Exception:
        frame = pd.read_csv(path, sep=delimiter, encoding=encoding, low_memory=False)

    return source_frame_to_rows(frame, field, assumptions)


def read_excel_source_rows(field: SourceField, assumptions: Mapping[str, str], log: AuditLog) -> pd.DataFrame:
    path = Path(field.file_path)
    sheet = field.container.split(":", 1)[1]
    frame = pd.read_excel(path, sheet_name=sheet)
    return source_frame_to_rows(frame, field, assumptions)


def source_frame_to_rows(frame: pd.DataFrame, field: SourceField, assumptions: Mapping[str, str]) -> pd.DataFrame:
    field = apply_assumed_unit(field, assumptions)
    if field.ssl_field not in frame.columns:
        return pd.DataFrame()
    dates, actual_time_field = parse_frame_dates(frame, field.time_field)
    station, station_name, actual_station_field, actual_name_field = parse_frame_station(
        frame,
        field.station_field,
        field.station_name_field,
        Path(field.file_path),
        field.dataset,
    )
    raw_values = pd.to_numeric(frame[field.ssl_field], errors="coerce").to_numpy(dtype=float)
    for fill in FILL_VALUES:
        raw_values[np.isclose(raw_values, float(fill), rtol=1e-7, atol=1e-7)] = np.nan

    converted = np.full(len(frame), np.nan, dtype=float)
    if field.unit_status in {"ok", "assumed"}:
        converted = convert_ssl_to_t_day(raw_values, field.normalized_unit, dates)

    result = pd.DataFrame(
        {
            "dataset": field.dataset,
            "source_file": field.file_path,
            "source_tier": field.source_tier,
            "source_container": field.container,
            "source_ssl_field": field.ssl_field,
            "source_ssl_raw_unit": field.raw_unit,
            "source_ssl_normalized_unit": field.normalized_unit,
            "source_unit_status": field.unit_status,
            "source_station_id": station,
            "source_station_name": station_name,
            "source_time": dates,
            "source_ssl_raw": raw_values,
            "source_ssl_t_day": converted,
            "source_time_field": actual_time_field,
            "source_station_field": actual_station_field,
            "source_station_name_field": actual_name_field,
        }
    )
    result["station_key"] = result["source_station_id"].map(normalize_station)
    result["station_name_key"] = result["source_station_name"].map(normalize_station)
    return result


def decode_netcdf_time(ds, name: str, size: int) -> pd.Series:
    if not name or name not in ds.variables:
        return pd.Series(pd.NaT, index=range(size), dtype="datetime64[ns]")
    var = ds.variables[name]
    raw = np.ma.asarray(var[:]).reshape(-1)
    raw = raw[:size]
    units = clean_text(getattr(var, "units", ""))
    calendar = clean_text(getattr(var, "calendar", "standard")) or "standard"
    if " since " in units:
        try:
            decoded = nc4.num2date(raw, units=units, calendar=calendar, only_use_cftime_datetimes=False)
        except TypeError:
            decoded = nc4.num2date(raw, units=units, calendar=calendar)
        except Exception:
            decoded = []
        if len(decoded):
            return pd.to_datetime([clean_text(x) for x in decoded], errors="coerce")
    return pd.to_datetime(raw, errors="coerce")


def read_netcdf_source_rows(field: SourceField, assumptions: Mapping[str, str], log: AuditLog) -> pd.DataFrame:
    if nc4 is None:
        return pd.DataFrame()
    path = Path(field.file_path)
    field = apply_assumed_unit(field, assumptions)
    with nc4.Dataset(path, "r") as ds:
        if field.ssl_field not in ds.variables:
            return pd.DataFrame()
        var = ds.variables[field.ssl_field]
        raw = safe_numeric(var[:])
        n = len(raw)
        dates = decode_netcdf_time(ds, field.time_field, n)
        station_id = field.station_field or get_first_attr(ds, STATION_ATTR_ALIASES)
        station_name = field.station_name_field or get_first_attr(ds, STATION_NAME_ATTR_ALIASES)
        if not station_id:
            station_id = infer_station_from_filename(path, field.dataset)
        converted = np.full(n, np.nan, dtype=float)
        if field.unit_status in {"ok", "assumed"}:
            converted = convert_ssl_to_t_day(raw, field.normalized_unit, dates)
        result = pd.DataFrame(
            {
                "dataset": field.dataset,
                "source_file": field.file_path,
                "source_tier": field.source_tier,
                "source_container": field.container,
                "source_ssl_field": field.ssl_field,
                "source_ssl_raw_unit": field.raw_unit,
                "source_ssl_normalized_unit": field.normalized_unit,
                "source_unit_status": field.unit_status,
                "source_station_id": station_id,
                "source_station_name": station_name,
                "source_time": dates,
                "source_ssl_raw": raw,
                "source_ssl_t_day": converted,
                "source_time_field": field.time_field,
                "source_station_field": "global_attribute_or_filename",
                "source_station_name_field": "global_attribute",
            }
        )
        result["station_key"] = result["source_station_id"].map(normalize_station)
        result["station_name_key"] = result["source_station_name"].map(normalize_station)
        return result


def read_source_rows(field: SourceField, assumptions: Mapping[str, str], log: AuditLog) -> pd.DataFrame:
    try:
        if field.container == "delimited":
            return read_delimited_source_rows(field, assumptions, log)
        if field.container.startswith("excel:"):
            return read_excel_source_rows(field, assumptions, log)
        if field.container == "netcdf":
            return read_netcdf_source_rows(field, assumptions, log)
    except Exception as exc:
        log.add("WARN", field.dataset, "source_value_read", f"{exc}", field.file_path)
    return pd.DataFrame()


# -----------------------------------------------------------------------------
# Standardized output readers
# -----------------------------------------------------------------------------

def output_dataset_match(path: Path, output_root: Path, dataset: str) -> bool:
    target = normalize_token(dataset)
    try:
        relative = path.relative_to(output_root)
    except Exception:
        relative = path
    return any(normalize_token(part) == target for part in relative.parts)


def discover_output_nc_files(output_root: Path, dataset: str, include_non_qc: bool) -> List[Path]:
    if not output_root.is_dir():
        return []
    files = [
        path
        for path in output_root.rglob("*.nc")
        if output_dataset_match(path, output_root, dataset)
    ]
    qc_files = [path for path in files if any(normalize_token(part) == "qc" for part in path.parts)]
    return sorted(files if include_non_qc or not qc_files else qc_files)


def _read_aligned_netcdf_var(ds, name: Optional[str], n: int) -> np.ndarray:
    if not name or name not in ds.variables:
        return np.full(n, np.nan, dtype=float)
    raw = safe_numeric(ds.variables[name][:])
    if len(raw) >= n:
        return raw[:n]
    out = np.full(n, np.nan, dtype=float)
    out[: len(raw)] = raw
    return out


def read_output_nc(dataset: str, path: Path, log: AuditLog) -> pd.DataFrame:
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to read standardized outputs")
    try:
        with nc4.Dataset(path, "r") as ds:
            names = list(ds.variables)
            q_name = first_existing_name(names, Q_VAR_ALIASES)
            ssc_name = first_existing_name(names, SSC_VAR_ALIASES)
            ssl_name = first_existing_name(names, SSL_VAR_ALIASES)
            time_name = first_existing_name(names, TIME_VAR_ALIASES)
            if not q_name or not ssc_name or not time_name:
                return pd.DataFrame()

            n = min(
                np.asarray(ds.variables[q_name][:]).size,
                np.asarray(ds.variables[ssc_name][:]).size,
                np.asarray(ds.variables[time_name][:]).size,
            )
            q = _read_aligned_netcdf_var(ds, q_name, n)
            ssc = _read_aligned_netcdf_var(ds, ssc_name, n)
            output_ssl = _read_aligned_netcdf_var(ds, ssl_name, n)
            dates = decode_netcdf_time(ds, time_name, n)
            station_id = get_first_attr(ds, STATION_ATTR_ALIASES) or infer_station_from_filename(path, dataset)
            station_name = get_first_attr(ds, STATION_NAME_ATTR_ALIASES)
            resolution = infer_resolution(path, get_first_attr(ds, RESOLUTION_ATTR_ALIASES))
            recalculated = q * ssc * SSL_FACTOR
            invalid = ~np.isfinite(q) | ~np.isfinite(ssc) | (q < 0) | (ssc < 0)
            recalculated[invalid] = np.nan

            frame = pd.DataFrame(
                {
                    "dataset": dataset,
                    "output_file": str(path),
                    "output_station_id": station_id,
                    "output_station_name": station_name,
                    "station_key": normalize_station(station_id),
                    "station_name_key": normalize_station(station_name),
                    "temporal_resolution": resolution,
                    "output_time": dates,
                    "Q_m3_s": q,
                    "SSC_mg_L": ssc,
                    "output_SSL_t_day": output_ssl,
                    "recalculated_SSL_t_day": recalculated,
                }
            )
            frame["output_minus_formula_t_day"] = frame["output_SSL_t_day"] - frame["recalculated_SSL_t_day"]
            return frame
    except Exception as exc:
        log.add("WARN", dataset, "output_read", f"{exc}", str(path))
        return pd.DataFrame()


# -----------------------------------------------------------------------------
# Matching and statistics
# -----------------------------------------------------------------------------

def time_key(values: pd.Series, resolution: str) -> pd.Series:
    dates = pd.to_datetime(values, errors="coerce")
    if resolution == "annual":
        return dates.dt.strftime("%Y")
    if resolution == "monthly":
        return dates.dt.strftime("%Y-%m")
    return dates.dt.strftime("%Y-%m-%d")


def aggregate_source_for_matching(source: pd.DataFrame, resolution: str) -> pd.DataFrame:
    work = source.copy()
    work["match_time_key"] = time_key(work["source_time"], resolution)
    work = work.loc[
        work["source_ssl_t_day"].notna()
        & work["match_time_key"].notna()
        & (work["match_time_key"] != "")
    ].copy()
    if work.empty:
        return work

    group_cols = [
        "dataset",
        "station_key",
        "station_name_key",
        "match_time_key",
        "source_file",
        "source_container",
        "source_ssl_field",
        "source_ssl_raw_unit",
        "source_ssl_normalized_unit",
        "source_unit_status",
        "source_tier",
    ]
    aggregated = (
        work.groupby(group_cols, dropna=False, as_index=False)
        .agg(
            source_time=("source_time", "min"),
            source_station_id=("source_station_id", "first"),
            source_station_name=("source_station_name", "first"),
            source_ssl_t_day=("source_ssl_t_day", "median"),
            source_ssl_raw=("source_ssl_raw", "median"),
            source_duplicate_count=("source_ssl_t_day", "size"),
        )
    )
    return aggregated


def _match_exact_station(output: pd.DataFrame, source: pd.DataFrame, resolution: str) -> pd.DataFrame:
    out = output.copy()
    out["match_time_key"] = time_key(out["output_time"], resolution)
    src = aggregate_source_for_matching(source, resolution)
    if out.empty or src.empty:
        return pd.DataFrame()

    # Match by station ID first.  Station-name matching is handled separately
    # only for rows still unmatched and only when names are non-empty.
    id_src = src.loc[src["station_key"] != ""].copy()
    id_out = out.loc[out["station_key"] != ""].copy()
    matched_id = id_out.merge(
        id_src,
        on=["dataset", "station_key", "match_time_key"],
        how="inner",
        suffixes=("_output", "_source"),
    )
    if not matched_id.empty:
        matched_id["station_match_method"] = "station_id_exact"

    matched_output_keys = set(
        zip(
            matched_id.get("output_file", pd.Series(dtype=str)),
            matched_id.get("output_time", pd.Series(dtype="datetime64[ns]")),
        )
    ) if not matched_id.empty else set()

    remaining_out_mask = np.asarray([
        (row.output_file, row.output_time) not in matched_output_keys
        for row in out.itertuples()
    ], dtype=bool)
    name_out = out.loc[remaining_out_mask & (out["station_name_key"] != "")].copy() if len(out) else out.iloc[0:0]
    name_src = src.loc[src["station_name_key"] != ""].copy()
    matched_name = name_out.merge(
        name_src,
        on=["dataset", "station_name_key", "match_time_key"],
        how="inner",
        suffixes=("_output", "_source"),
    )
    if not matched_name.empty:
        matched_name["station_match_method"] = "station_name_exact"

    frames = [frame for frame in (matched_id, matched_name) if not frame.empty]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def match_source_to_output(dataset: str, source: pd.DataFrame, output: pd.DataFrame, log: AuditLog) -> pd.DataFrame:
    if source.empty or output.empty:
        return pd.DataFrame()

    # If the source has no usable station ID and there is only one output station,
    # assign it conservatively.  Do not perform date-only matching for multi-station
    # datasets because that can create convincing but incorrect pairs.
    source = source.copy()
    output = output.copy()
    output_station_keys = [x for x in output["station_key"].dropna().unique() if x]
    if not source["station_key"].astype(bool).any() and len(output_station_keys) == 1:
        source["station_key"] = output_station_keys[0]
        source["source_station_id"] = source["source_station_id"].replace("", output["output_station_id"].iloc[0])
        log.add("INFO", dataset, "matching", "Assigned sole output station ID to source rows lacking station identifiers")

    matched_frames: List[pd.DataFrame] = []
    for resolution, out_group in output.groupby("temporal_resolution", dropna=False):
        res = resolution if resolution in {"daily", "monthly", "annual"} else "daily"
        matched = _match_exact_station(out_group, source, res)
        if not matched.empty:
            matched["matching_resolution"] = res
            matched_frames.append(matched)

    if not matched_frames:
        return pd.DataFrame()
    pairs = pd.concat(matched_frames, ignore_index=True, sort=False)

    # Normalize columns after merges with different suffix behavior.
    for desired, candidates in {
        "station_key": ["station_key", "station_key_output", "station_key_source"],
        "station_name_key": ["station_name_key", "station_name_key_output", "station_name_key_source"],
    }.items():
        if desired not in pairs.columns:
            source_name = next((c for c in candidates if c in pairs.columns), None)
            if source_name:
                pairs[desired] = pairs[source_name]

    pairs["error_t_day"] = pairs["recalculated_SSL_t_day"] - pairs["source_ssl_t_day"]
    pairs["absolute_error_t_day"] = pairs["error_t_day"].abs()
    nonzero = pairs["source_ssl_t_day"].notna() & (pairs["source_ssl_t_day"] != 0)
    pairs["relative_error_pct"] = np.nan
    pairs.loc[nonzero, "relative_error_pct"] = (
        100.0 * pairs.loc[nonzero, "error_t_day"] / pairs.loc[nonzero, "source_ssl_t_day"]
    )
    pairs["absolute_relative_error_pct"] = pairs["relative_error_pct"].abs()
    positive = (pairs["source_ssl_t_day"] > 0) & (pairs["recalculated_SSL_t_day"] > 0)
    pairs["recalculated_to_source_ratio"] = np.nan
    pairs.loc[positive, "recalculated_to_source_ratio"] = (
        pairs.loc[positive, "recalculated_SSL_t_day"] / pairs.loc[positive, "source_ssl_t_day"]
    )
    pairs["absolute_log10_ratio"] = np.nan
    pairs.loc[positive, "absolute_log10_ratio"] = np.abs(
        np.log10(pairs.loc[positive, "recalculated_to_source_ratio"])
    )
    return pairs


def safe_quantile(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.quantile(q)) if len(clean) else np.nan


def error_summary(group: pd.DataFrame) -> Dict[str, object]:
    source = pd.to_numeric(group["source_ssl_t_day"], errors="coerce")
    calc = pd.to_numeric(group["recalculated_SSL_t_day"], errors="coerce")
    error = pd.to_numeric(group["error_t_day"], errors="coerce")
    abs_error = error.abs()
    rel = pd.to_numeric(group["relative_error_pct"], errors="coerce")
    abs_rel = rel.abs()
    ratio = pd.to_numeric(group["recalculated_to_source_ratio"], errors="coerce")
    abs_log = pd.to_numeric(group["absolute_log10_ratio"], errors="coerce")

    valid = source.notna() & calc.notna()
    source_valid = source[valid]
    calc_valid = calc[valid]
    error_valid = error[valid]
    abs_error_valid = abs_error[valid]
    total_source = float(source_valid.sum()) if len(source_valid) else np.nan
    total_calc = float(calc_valid.sum()) if len(calc_valid) else np.nan
    weighted_relative_bias = (
        100.0 * (total_calc - total_source) / total_source
        if np.isfinite(total_source) and total_source != 0
        else np.nan
    )

    pearson = source_valid.corr(calc_valid, method="pearson") if len(source_valid) >= 2 else np.nan
    spearman = source_valid.corr(calc_valid, method="spearman") if len(source_valid) >= 2 else np.nan
    median_abs_log = float(abs_log.dropna().median()) if abs_log.notna().any() else np.nan

    return {
        "n_pairs": int(valid.sum()),
        "n_stations": int(group["station_key"].replace("", np.nan).nunique(dropna=True)),
        "n_source_files": int(group["source_file"].nunique(dropna=True)),
        "source_ssl_median_t_day": float(source_valid.median()) if len(source_valid) else np.nan,
        "recalculated_ssl_median_t_day": float(calc_valid.median()) if len(calc_valid) else np.nan,
        "mean_bias_t_day": float(error_valid.mean()) if len(error_valid) else np.nan,
        "median_bias_t_day": float(error_valid.median()) if len(error_valid) else np.nan,
        "mae_t_day": float(abs_error_valid.mean()) if len(abs_error_valid) else np.nan,
        "median_absolute_error_t_day": float(abs_error_valid.median()) if len(abs_error_valid) else np.nan,
        "rmse_t_day": float(np.sqrt(np.nanmean(np.square(error_valid)))) if len(error_valid) else np.nan,
        "weighted_relative_bias_pct": weighted_relative_bias,
        "median_relative_error_pct": float(rel.dropna().median()) if rel.notna().any() else np.nan,
        "median_absolute_relative_error_pct": float(abs_rel.dropna().median()) if abs_rel.notna().any() else np.nan,
        "p75_absolute_relative_error_pct": safe_quantile(abs_rel, 0.75),
        "p90_absolute_relative_error_pct": safe_quantile(abs_rel, 0.90),
        "p95_absolute_relative_error_pct": safe_quantile(abs_rel, 0.95),
        "median_ratio_recalculated_over_source": float(ratio.dropna().median()) if ratio.notna().any() else np.nan,
        "p05_ratio_recalculated_over_source": safe_quantile(ratio, 0.05),
        "p95_ratio_recalculated_over_source": safe_quantile(ratio, 0.95),
        "typical_factor_error": float(10.0 ** median_abs_log) if np.isfinite(median_abs_log) else np.nan,
        "within_10pct_rate": float((abs_rel <= 10).mean()) if abs_rel.notna().any() else np.nan,
        "within_25pct_rate": float((abs_rel <= 25).mean()) if abs_rel.notna().any() else np.nan,
        "within_50pct_rate": float((abs_rel <= 50).mean()) if abs_rel.notna().any() else np.nan,
        "within_factor_2_rate": float(((ratio >= 0.5) & (ratio <= 2.0)).mean()) if ratio.notna().any() else np.nan,
        "pearson_r": float(pearson) if pd.notna(pearson) else np.nan,
        "spearman_rho": float(spearman) if pd.notna(spearman) else np.nan,
        "source_zero_count_relative_error_excluded": int((source == 0).sum()),
    }


def summarize_pairs(pairs: pd.DataFrame, group_cols: Sequence[str]) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []
    if pairs.empty:
        return pd.DataFrame()
    for keys, group in pairs.groupby(list(group_cols), dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {column: value for column, value in zip(group_cols, keys)}
        row.update(error_summary(group))
        rows.append(row)
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Reporting and figures
# -----------------------------------------------------------------------------

def markdown_table(df: pd.DataFrame, columns: Sequence[str], max_rows: int = 100) -> str:
    if df is None or df.empty:
        return "_No rows._"
    work = df.loc[:, [c for c in columns if c in df.columns]].head(max_rows).copy()
    lines = [
        "| " + " | ".join(work.columns) + " |",
        "| " + " | ".join(["---"] * len(work.columns)) + " |",
    ]
    for _, row in work.iterrows():
        values: List[str] = []
        for value in row:
            if isinstance(value, float):
                text = "" if not np.isfinite(value) else f"{value:.4g}"
            else:
                text = clean_text(value)
            values.append(text.replace("|", "\\|"))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_report(
    path: Path,
    script_inventory: pd.DataFrame,
    field_inventory: pd.DataFrame,
    funnel: pd.DataFrame,
    summary: pd.DataFrame,
    assumptions: Mapping[str, str],
) -> None:
    lines = [
        "# Source-Reported vs Recalculated SSL Audit",
        "",
        "This read-only audit identifies datasets whose processing scripts recalculate suspended sediment load as `Q × SSC × 0.0864`, searches their source directories for reported SSL fields, and compares matched source values with recalculated SSL from standardized Q and SSC.",
        "",
        "## Scope",
        "",
        f"- candidate datasets: {script_inventory['dataset'].nunique() if not script_inventory.empty else 0:,}",
        f"- recalculation evidence rows: {len(script_inventory):,}",
        f"- source SSL fields detected: {len(field_inventory):,}",
        f"- datasets with matched comparison pairs: {summary['dataset'].nunique() if not summary.empty else 0:,}",
        "- signed error: recalculated SSL minus source-reported SSL",
        "- relative error: signed error divided by source-reported SSL; source zeros are excluded",
        "",
        "## Dataset Funnel",
        "",
        markdown_table(
            funnel,
            [
                "dataset",
                "n_recalculation_scripts",
                "source_dir_found",
                "n_source_files_scanned",
                "n_source_ssl_fields",
                "n_usable_source_ssl_rows",
                "n_output_nc_files",
                "n_output_formula_records",
                "n_matched_pairs",
                "status",
            ],
        ),
        "",
        "## Error Magnitudes",
        "",
        markdown_table(
            summary,
            [
                "dataset",
                "n_pairs",
                "n_stations",
                "mean_bias_t_day",
                "median_bias_t_day",
                "mae_t_day",
                "rmse_t_day",
                "weighted_relative_bias_pct",
                "median_relative_error_pct",
                "median_absolute_relative_error_pct",
                "p90_absolute_relative_error_pct",
                "median_ratio_recalculated_over_source",
                "typical_factor_error",
                "within_25pct_rate",
                "within_factor_2_rate",
                "spearman_rho",
            ],
        ),
        "",
        "## Interpretation Notes",
        "",
        "1. A source field is treated as independently reported only because it exists in the raw provider file before this repository recalculates SSL. The provider may still have calculated it internally.",
        "2. Matching is conservative: exact normalized station ID is preferred, followed by exact station name; date-only matching is not used for multi-station datasets.",
        "3. Monthly and annual source loads are converted to `t d-1` using days in month or 365.25 days per year, respectively.",
        "4. Fields with missing or unrecognized units are inventoried but excluded unless a unit was supplied with `--assume-source-unit`.",
        "5. Source duplicates at the same station and time are summarized by their median and the duplicate count is retained in the paired table.",
    ]
    if assumptions:
        lines += ["", "## User-Supplied Unit Assumptions", ""]
        for key, value in sorted(assumptions.items()):
            lines.append(f"- `{key}`: `{value}`")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_scatter(pairs: pd.DataFrame, path: Path) -> None:
    if plt is None or pairs.empty:
        return
    positive = pairs.loc[
        (pairs["source_ssl_t_day"] > 0)
        & (pairs["recalculated_SSL_t_day"] > 0)
        & np.isfinite(pairs["source_ssl_t_day"])
        & np.isfinite(pairs["recalculated_SSL_t_day"])
    ].copy()
    if positive.empty:
        return
    fig, ax = plt.subplots(figsize=(7.2, 6.4))
    ax.scatter(positive["source_ssl_t_day"], positive["recalculated_SSL_t_day"], s=9, alpha=0.35)
    low = min(positive["source_ssl_t_day"].min(), positive["recalculated_SSL_t_day"].min())
    high = max(positive["source_ssl_t_day"].max(), positive["recalculated_SSL_t_day"].max())
    ax.plot([low, high], [low, high], linestyle="--", linewidth=1)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Source-reported SSL (t d$^{-1}$)")
    ax.set_ylabel("Recalculated Q × SSC × 0.0864 (t d$^{-1}$)")
    ax.set_title("Source-reported versus recalculated SSL")
    fig.tight_layout()
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

def parse_unit_assumptions(values: Sequence[str]) -> Dict[str, str]:
    assumptions: Dict[str, str] = {}
    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid --assume-source-unit value {item!r}; expected DATASET=UNIT")
        dataset, unit = item.split("=", 1)
        normalized, status = classify_ssl_unit(unit)
        if status != "ok":
            raise ValueError(f"Unrecognized assumed SSL unit for {dataset!r}: {unit!r}")
        assumptions[normalize_token(dataset)] = unit.strip()
    return assumptions


def build_funnel_row(
    dataset: str,
    script_inventory: pd.DataFrame,
    source_dir: Optional[Path],
    scanned: int,
    fields: List[SourceField],
    source_rows: pd.DataFrame,
    output_files: List[Path],
    output_rows: pd.DataFrame,
    pairs: pd.DataFrame,
) -> Dict[str, object]:
    n_usable_source = int(source_rows["source_ssl_t_day"].notna().sum()) if not source_rows.empty else 0
    n_output_formula = int(output_rows["recalculated_SSL_t_day"].notna().sum()) if not output_rows.empty else 0
    if source_dir is None:
        status = "source_directory_not_found"
    elif not fields:
        status = "no_source_ssl_field_detected"
    elif n_usable_source == 0:
        status = "source_ssl_found_but_not_usable"
    elif not output_files:
        status = "standardized_output_not_found"
    elif n_output_formula == 0:
        status = "no_output_q_ssc_formula_records"
    elif pairs.empty:
        status = "source_and_output_found_but_unmatched"
    else:
        status = "compared"
    return {
        "dataset": dataset,
        "n_recalculation_scripts": int(script_inventory.loc[script_inventory["dataset"] == dataset, "script_path"].nunique()),
        "n_recalculation_evidence_rows": int((script_inventory["dataset"] == dataset).sum()),
        "source_dir_found": source_dir is not None,
        "source_dir": str(source_dir) if source_dir else "",
        "n_source_files_scanned": scanned,
        "n_source_ssl_fields": len(fields),
        "n_source_ssl_rows_read": len(source_rows),
        "n_usable_source_ssl_rows": n_usable_source,
        "n_output_nc_files": len(output_files),
        "n_output_records": len(output_rows),
        "n_output_formula_records": n_output_formula,
        "n_matched_pairs": len(pairs),
        "status": status,
    }


def run(args: argparse.Namespace) -> int:
    if nc4 is None:
        raise RuntimeError("netCDF4 is required. Install it with: pip install netCDF4")

    repo_root, source_root, output_root, out_dir = resolve_roots(args)
    tables_dir = ensure_dir(out_dir / "tables")
    figures_dir = ensure_dir(out_dir / "figures")
    log = AuditLog()
    assumptions = parse_unit_assumptions(args.assume_source_unit)

    print(f"Repository root: {repo_root}")
    print(f"Source root:     {source_root}")
    print(f"Output root:     {output_root}")
    print(f"Audit output:    {out_dir}")

    script_inventory = discover_recalculation_scripts(repo_root, args.datasets)
    script_inventory.to_csv(tables_dir / "recalculation_script_inventory.csv", index=False)
    if script_inventory.empty:
        print("No Q-SSC-to-SSL recalculation scripts were detected.")
        return 2

    datasets = sorted(script_inventory["dataset"].unique().tolist())
    print(f"Detected {len(datasets)} candidate datasets: {', '.join(datasets)}")

    all_field_rows: List[Dict[str, object]] = []
    all_pairs: List[pd.DataFrame] = []
    funnel_rows: List[Dict[str, object]] = []

    for index, dataset in enumerate(datasets, 1):
        print(f"[{index}/{len(datasets)}] {dataset}")
        source_dir = find_dataset_dir(source_root, dataset)
        fields: List[SourceField] = []
        scanned = 0
        source_frames: List[pd.DataFrame] = []

        if source_dir is not None:
            fields, scanned = discover_source_fields(
                dataset,
                source_dir,
                include_intermediate=args.include_intermediate_source_files,
                max_files=args.max_source_files,
                log=log,
                workers=args.workers,
            )
            # Apply assumptions before writing inventory so status is transparent.
            fields = [apply_assumed_unit(field, assumptions) for field in fields]
            for field in fields:
                all_field_rows.append(field.__dict__)
        else:
            log.add("WARN", dataset, "source_discovery", "Source dataset directory not found", str(source_root))

        # --- Read output rows first (typically manageable in size) ---
        output_files = discover_output_nc_files(output_root, dataset, args.include_non_qc_output)
        output_frames: List[pd.DataFrame] = []
        if output_files:
            if args.workers > 1 and len(output_files) > 1:
                batch_args = [(dataset, p) for p in output_files]
                n_w = min(args.workers, len(output_files))
                with concurrent.futures.ProcessPoolExecutor(max_workers=n_w) as executor:
                    futures = [executor.submit(_read_output_single, ba) for ba in batch_args]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            frame = future.result()
                            if not frame.empty:
                                output_frames.append(frame)
                        except Exception as exc:
                            log.add("WARN", dataset, "parallel_output_read", str(exc))
            else:
                for path in output_files:
                    frame = read_output_nc(dataset, path, log)
                    if not frame.empty:
                        output_frames.append(frame)
        output_frames = [f for f in output_frames if not f.empty]
        output_rows = pd.concat(output_frames, ignore_index=True, sort=False) if output_frames else pd.DataFrame()

        # --- Incremental matching: read source fields one by one, match,
        #     discard source data immediately to bound memory ---
        dataset_pairs: List[pd.DataFrame] = []
        total_source_ssl_rows = 0
        total_usable_source_ssl_rows = 0

        if fields and not output_rows.empty:
            # Serial per-field matching to keep memory bounded.
            # Parallel source reading is less critical here because matching
            # is the memory-heavy step; doing it serially avoids peak memory.
            max_fields = args.max_source_fields if args.max_source_fields else len(fields)
            processed_fields = 0
            for field in fields:
                if max_fields and processed_fields >= max_fields:
                    log.add("INFO", dataset, "incremental_matching",
                            f"Reached --max-source-fields limit ({max_fields}); "
                            f"{len(fields) - processed_fields} fields skipped")
                    break
                processed_fields += 1
                frame = read_source_rows(field, assumptions, log)
                if frame.empty:
                    continue
                total_source_ssl_rows += len(frame)
                usable = int(frame["source_ssl_t_day"].notna().sum())
                total_usable_source_ssl_rows += usable
                if usable == 0:
                    continue
                field_pairs = match_source_to_output(dataset, frame, output_rows, log)
                if not field_pairs.empty:
                    dataset_pairs.append(field_pairs)
                # frame is freed here (loop variable overwritten next iteration)

        # Build dummy source_rows container for funnel statistics
        if total_source_ssl_rows > 0:
            source_rows = pd.DataFrame({"source_ssl_t_day": [0.0] * total_source_ssl_rows})
        else:
            source_rows = pd.DataFrame()

        pairs = pd.concat(dataset_pairs, ignore_index=True, sort=False) if dataset_pairs else pd.DataFrame()
        if not pairs.empty:
            all_pairs.append(pairs)

        funnel_row = build_funnel_row(
                dataset,
                script_inventory,
                source_dir,
                scanned,
                fields,
                source_rows,
                output_files,
                output_rows,
                pairs,
            )
        # Override with actual incremental counts
        funnel_row["n_source_ssl_rows_read"] = total_source_ssl_rows
        funnel_row["n_usable_source_ssl_rows"] = total_usable_source_ssl_rows
        # Recalculate status with correct counts
        if source_dir is None:
            funnel_row["status"] = "source_directory_not_found"
        elif not fields:
            funnel_row["status"] = "no_source_ssl_field_detected"
        elif total_usable_source_ssl_rows == 0:
            funnel_row["status"] = "source_ssl_found_but_not_usable"
        elif not output_files:
            funnel_row["status"] = "standardized_output_not_found"
        elif funnel_row["n_output_formula_records"] == 0:
            funnel_row["status"] = "no_output_q_ssc_formula_records"
        elif pairs.empty:
            funnel_row["status"] = "source_and_output_found_but_unmatched"
        else:
            funnel_row["status"] = "compared"
        funnel_rows.append(funnel_row)

    field_inventory = pd.DataFrame(all_field_rows)
    field_inventory.to_csv(tables_dir / "source_ssl_field_inventory.csv", index=False)

    funnel = pd.DataFrame(funnel_rows)
    funnel.to_csv(tables_dir / "dataset_audit_funnel.csv", index=False)

    pairs_all = pd.concat(all_pairs, ignore_index=True, sort=False) if all_pairs else pd.DataFrame()
    pair_path = tables_dir / "ssl_source_vs_recalculated_pairs.csv.gz"
    pairs_all.to_csv(pair_path, index=False, compression="gzip")

    summary_dataset = summarize_pairs(pairs_all, ["dataset"])
    summary_resolution = summarize_pairs(pairs_all, ["dataset", "temporal_resolution"])
    summary_source_field = summarize_pairs(
        pairs_all,
        ["dataset", "source_file", "source_container", "source_ssl_field"],
    )
    summary_dataset.to_csv(tables_dir / "ssl_error_summary_by_dataset.csv", index=False)
    summary_resolution.to_csv(tables_dir / "ssl_error_summary_by_dataset_resolution.csv", index=False)
    summary_source_field.to_csv(tables_dir / "ssl_error_summary_by_source_field.csv", index=False)

    log_df = pd.DataFrame(log.rows)
    log_df.to_csv(tables_dir / "audit_log.csv", index=False)

    make_scatter(pairs_all, figures_dir / "ssl_source_vs_recalculated.png")
    write_report(
        out_dir / "derived_ssl_source_comparison_report.md",
        script_inventory,
        field_inventory,
        funnel,
        summary_dataset,
        assumptions,
    )

    print("\nAudit complete")
    print(f"  Candidate datasets: {len(datasets)}")
    print(f"  Source SSL fields:  {len(field_inventory)}")
    print(f"  Matched pairs:      {len(pairs_all)}")
    print(f"  Compared datasets:  {summary_dataset['dataset'].nunique() if not summary_dataset.empty else 0}")
    print(f"  Report:             {out_dir / 'derived_ssl_source_comparison_report.md'}")

    if args.fail_if_no_pairs and pairs_all.empty:
        return 3
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare source-reported SSL with SSL recalculated from standardized Q and SSC"
    )
    parser.add_argument("--repo-root", default="", help="Sed_data repository root")
    parser.add_argument("--source-root", default="", help="Original Source root")
    parser.add_argument("--output-root", default="", help="Standardized Output_r root")
    parser.add_argument("--out-dir", default="", help="Audit output directory")
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=[],
        help="Optional dataset names; otherwise discover all recalculation scripts",
    )
    parser.add_argument(
        "--assume-source-unit",
        action="append",
        default=[],
        metavar="DATASET=UNIT",
        help="Unit assumption for source SSL fields with no parseable unit; repeatable",
    )
    parser.add_argument(
        "--include-intermediate-source-files",
        action="store_true",
        help="Inspect processed/output-like files inside Source even when raw SSL fields exist",
    )
    parser.add_argument(
        "--include-non-qc-output",
        action="store_true",
        help="Read all matching Output_r NetCDF files, not only qc folders",
    )
    parser.add_argument(
        "--max-source-files",
        type=int,
        default=0,
        help="Maximum source files inspected per dataset; 0 means unlimited",
    )
    parser.add_argument(
        "--max-source-fields",
        type=int,
        default=500,
        help="Maximum source SSL fields to read and match per dataset; 0 means unlimited "
             "(default: 500 — caps memory and runtime for huge datasets like USGS)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=_get_default_workers(),
        help=f"Number of parallel workers for file inspection and reading "
             f"(default: min(cpu_count-1, 16) = {_get_default_workers()})",
    )
    parser.add_argument(
        "--fail-if-no-pairs",
        action="store_true",
        help="Return a non-zero exit code when no source/output pairs are produced",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        return run(parse_args(argv))
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

