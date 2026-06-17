#!/usr/bin/env python3
"""Independent post-release audit for the S8 sediment reference package."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import netCDF4 as nc
import numpy as np
import pandas as pd


EXPECTED_RELEASE_FILES = [
    "sed_reference_master.nc",
    "sed_reference_timeseries_daily.nc",
    "sed_reference_timeseries_monthly.nc",
    "sed_reference_timeseries_annual.nc",
    "sed_reference_climatology.nc",
    "sed_reference_satellite.nc",
    "station_catalog.csv",
    "source_station_catalog.csv",
    "source_dataset_catalog.csv",
    "satellite_catalog.csv",
    "sed_reference_overlap_candidates.csv.gz",
    "release_validation_report.csv",
    "release_inventory.csv",
]

NETCDF_FILES = [
    "sed_reference_master.nc",
    "sed_reference_timeseries_daily.nc",
    "sed_reference_timeseries_monthly.nc",
    "sed_reference_timeseries_annual.nc",
    "sed_reference_climatology.nc",
    "sed_reference_satellite.nc",
]

MATRIX_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}

FINAL_FLAG_ALLOWED = {0, 1, 2, 3, 9}
STAGE_FLAG_ALLOWED = {
    "Q_qc1": {0, 3, 9},
    "SSC_qc1": {0, 3, 9},
    "SSL_qc1": {0, 3, 9},
    "Q_qc2": {0, 2, 8, 9},
    "SSC_qc2": {0, 2, 8, 9},
    "SSL_qc2": {0, 2, 8, 9},
    "SSC_qc3": {0, 2, 8, 9},
    "SSL_qc3": {0, 1, 8, 9},
}

UPSTREAM_QC_NAMES = {
    "Q_qc1": "Q_flag_qc1_physical",
    "SSC_qc1": "SSC_flag_qc1_physical",
    "SSL_qc1": "SSL_flag_qc1_physical",
    "Q_qc2": "Q_flag_qc2_log_iqr",
    "SSC_qc2": "SSC_flag_qc2_log_iqr",
    "SSL_qc2": "SSL_flag_qc2_log_iqr",
    "SSC_qc3": "SSC_flag_qc3_ssc_q",
    "SSL_qc3": "SSL_flag_qc3_from_ssc_q",
}

PROMOTED_ALIASES = {
    "country": ["country", "Country"],
    "continent_region": ["continent_region", "continent", "region", "Continent_Region"],
    "geographic_coverage": ["geographic_coverage", "Geographic_Coverage"],
    "iso_a3": ["iso_a3", "ISO_A3", "adm0_a3", "ADM0_A3"],
    "station_id": ["station_id", "Source_ID", "Station_ID", "source_id", "stationID", "ID", "location_id"],
    "dataset_name": ["dataset_name", "Dataset_Name"],
    "data_source_name": ["data_source_name", "Data_Source_Name"],
    "observation_type": ["observation_type", "Observation_Type"],
    "temporal_resolution": ["temporal_resolution", "Temporal_Resolution", "time_resolution", "resolution"],
    "time_coverage_start": ["time_coverage_start", "data_period_start", "start_date"],
    "time_coverage_end": ["time_coverage_end", "data_period_end", "end_date"],
    "creator_name": ["creator_name", "Creator_Name"],
    "creator_email": ["creator_email", "Creator_Email"],
    "creator_institution": ["creator_institution", "contributor_institution", "institution", "insitiution"],
    "source_data_link": ["source_data_link", "source_url", "sediment_data_source", "discharge_data_source"],
    "processing_level": ["processing_level", "Processing_Level"],
    "featureType": ["featureType", "feature_type"],
    "date_created": ["date_created", "Date_Created"],
    "date_modified": ["date_modified", "Date_Modified"],
}

CAT_SOURCE_STATION_FIELDS = [
    "source_station_uid",
    "cluster_uid",
    "cluster_id",
    "resolution",
    "source_name",
    "source_station_native_id",
    "source_station_name",
    "source_station_river_name",
    "source_station_lat",
    "source_station_lon",
    "country",
    "continent_region",
    "geographic_coverage",
    "iso_a3",
    "source_station_paths",
]

CAT_STATION_FIELDS = [
    "cluster_uid",
    "cluster_id",
    "lat",
    "lon",
    "basin_area",
    "pfaf_code",
    "basin_status",
    "basin_flag",
    "country",
    "continent_region",
    "geographic_coverage",
    "iso_a3",
    "available_resolutions",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", default="scripts_basin_test/output/sed_reference_release")
    parser.add_argument("--sample-per-source-resolution", type=int, default=5)
    parser.add_argument("--sample-matrix-cells", type=int, default=100)
    parser.add_argument("--sample-master-records", type=int, default=300)
    parser.add_argument("--sample-overlap-records", type=int, default=100)
    parser.add_argument("--sample-climatology-stations", type=int, default=50)
    parser.add_argument("--sample-satellite-stations", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", default="docs/reports/s8_release_audit")
    return parser.parse_args()


def resolve_existing_path(path: str | Path, base: Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    if p.exists():
        return p.resolve()
    if p.parts and p.parts[0] == base.name:
        stripped = Path(*p.parts[1:])
        if stripped.exists():
            return stripped.resolve()
    return (base / p).resolve()


def is_missing_scalar(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, np.ma.core.MaskedConstant):
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "nat", "none", "null", "--", "<na>"}


def to_python(value: Any) -> Any:
    if isinstance(value, np.ma.MaskedArray):
        if value.shape == ():
            return None if bool(np.ma.is_masked(value)) else to_python(value.item())
        return [to_python(v) for v in value.compressed().tolist()]
    if isinstance(value, np.ndarray):
        if value.shape == ():
            return to_python(value.item())
        return [to_python(v) for v in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return [to_python(v) for v in value]
    return value


def to_text(value: Any) -> str:
    value = to_python(value)
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return "".join(to_text(v) for v in value).strip()
    return str(value).strip()


def norm_text(value: Any) -> str:
    if is_missing_scalar(value):
        return ""
    text = to_text(value)
    if text.endswith(".0"):
        try:
            return str(int(float(text)))
        except ValueError:
            pass
    return text


def norm_compare(value: Any) -> str:
    if isinstance(value, (list, tuple, np.ndarray)):
        vals = [norm_compare(v) for v in to_python(value)]
        vals = [v for v in vals if v != ""]
        return "|".join(vals)
    text = norm_text(value)
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})[T ]00:00:00(?:\.0+)?(?:Z)?", text)
    if match:
        return match.group(1)
    if text:
        try:
            f = float(text)
        except ValueError:
            return text
        if math.isfinite(f):
            return f"{f:.10g}"
    return text


def canonicalize_json(value: Any) -> Any:
    value = to_python(value)
    if isinstance(value, dict):
        return {str(k): canonicalize_json(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list):
        normalized = [canonicalize_json(v) for v in value]
        return sorted(normalized, key=lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False))
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(canonicalize_json(value), sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def read_json(text: Any) -> dict[str, Any]:
    raw = to_text(text)
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {"__json_parse_error__": raw}
    return loaded if isinstance(loaded, dict) else {"__non_object_json__": loaded}


def read_var(ds: nc.Dataset, name: str, index: Any = slice(None)) -> Any:
    return ds.variables[name][index]


def read_text_var(ds: nc.Dataset, name: str, index: Any = slice(None)) -> Any:
    arr = read_var(ds, name, index)
    if np.ndim(arr) == 0:
        return to_text(arr)
    return [to_text(v) for v in np.asarray(arr, dtype=object).ravel()]


def get_fill_value(var: nc.Variable) -> Any:
    return getattr(var, "_FillValue", getattr(var, "missing_value", None))


def numeric_missing(values: np.ndarray | np.ma.MaskedArray, fill: Any = None) -> np.ndarray:
    arr = np.ma.asarray(values)
    mask = np.ma.getmaskarray(arr).copy()
    data = np.asarray(arr.filled(np.nan if np.issubdtype(arr.dtype, np.floating) else 0))
    if np.issubdtype(data.dtype, np.floating):
        mask |= ~np.isfinite(data)
    if fill is not None:
        try:
            mask |= data == fill
        except TypeError:
            pass
    return mask


def value_at(ds: nc.Dataset, name: str, idx: Any) -> Any:
    if name not in ds.variables:
        return None
    return to_python(ds.variables[name][idx])


def scalar_value(ds: nc.Dataset, name: str, idx: Any) -> Any:
    value = value_at(ds, name, idx)
    if isinstance(value, list) and len(value) == 1:
        return value[0]
    return value


def collect_ncattrs(path: Path) -> dict[str, Any]:
    with nc.Dataset(path) as ds:
        return {name: to_python(getattr(ds, name)) for name in ds.ncattrs()}


def merge_upstream_attrs(paths: Iterable[Path]) -> dict[str, Any]:
    merged: dict[str, list[Any]] = defaultdict(list)
    valid_paths = [p for p in paths if p.exists()]
    for path in valid_paths:
        for key, value in collect_ncattrs(path).items():
            if not any(canonical_json(value) == canonical_json(old) for old in merged[key]):
                merged[key].append(value)
    if not valid_paths:
        return {}
    if len(valid_paths) == 1:
        return collect_ncattrs(valid_paths[0])
    return {key: values[0] if len(values) == 1 else values for key, values in sorted(merged.items())}


def split_paths(raw: Any) -> list[Path]:
    text = to_text(raw)
    if not text:
        return []
    return [Path(piece.strip()) for piece in text.split("|") if piece.strip()]


def sample_indices(count: int, sample_size: int, rng: random.Random) -> list[int]:
    if count <= 0 or sample_size <= 0:
        return []
    n = min(count, sample_size)
    return sorted(rng.sample(range(count), n))


def sample_df(df: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    if df.empty or n <= 0:
        return df.iloc[0:0].copy()
    if len(df) <= n:
        return df.copy()
    return df.iloc[sorted(rng.sample(range(len(df)), n))].copy()


def add_row(rows: list[dict[str, Any]], **kwargs: Any) -> None:
    rows.append({k: ("" if v is None else v) for k, v in kwargs.items()})


class Audit:
    def __init__(self, args: argparse.Namespace) -> None:
        self.repo_dir = Path.cwd()
        self.release_dir = resolve_existing_path(args.release_dir, self.repo_dir)
        self.output_dir = resolve_existing_path(args.output_dir, self.repo_dir)
        self.args = args
        self.rng = random.Random(args.seed)
        self.summary: list[dict[str, Any]] = []
        self.global_samples: list[dict[str, Any]] = []
        self.global_mismatches: list[dict[str, Any]] = []
        self.promoted_mismatches: list[dict[str, Any]] = []
        self.catalog_mismatches: list[dict[str, Any]] = []
        self.qc_samples: list[dict[str, Any]] = []
        self.qc_mismatches: list[dict[str, Any]] = []
        self.stage_qc_mismatches: list[dict[str, Any]] = []
        self.selection_mismatches: list[dict[str, Any]] = []
        self.semantic_warnings: list[dict[str, Any]] = []
        self.schema_lines: list[str] = []
        self.source_station_catalog = self.read_catalog("source_station_catalog.csv")
        self.station_catalog = self.read_catalog("station_catalog.csv")
        self.satellite_catalog = self.read_catalog("satellite_catalog.csv")
        self._upstream_time_cache: dict[Path, dict[str, Any]] = {}

    def read_catalog(self, name: str) -> pd.DataFrame:
        path = self.release_dir / name
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path, keep_default_na=False)

    def record_summary(self, check: str, status: str, details: str, count: int = 0) -> None:
        self.summary.append({"check": check, "status": status, "count": count, "details": details})

    def audit_schema(self) -> None:
        missing = [name for name in EXPECTED_RELEASE_FILES if not (self.release_dir / name).exists()]
        self.record_summary(
            "release_required_files",
            "pass" if not missing else "fail",
            "all expected files present" if not missing else "missing: " + ", ".join(missing),
            len(missing),
        )
        self.schema_lines.append(f"- Release directory: `{self.release_dir}`")
        for name in NETCDF_FILES:
            path = self.release_dir / name
            if not path.exists():
                self.schema_lines.append(f"- `{name}`: missing")
                continue
            with nc.Dataset(path) as ds:
                dims = {k: len(v) for k, v in ds.dimensions.items()}
                variables = list(ds.variables)
                attrs = list(ds.ncattrs())
                self.schema_lines.append(
                    f"- `{name}`: dims={dims}; variables={len(variables)}; global_attrs={len(attrs)}"
                )
                self.schema_lines.append(f"  - variables: {', '.join(variables)}")
                self.schema_lines.append(f"  - global_attrs: {', '.join(attrs)}")

    def master_source_samples(self) -> pd.DataFrame:
        df = self.source_station_catalog
        if df.empty:
            return df
        parts = []
        group_cols = [c for c in ["source_name", "resolution"] if c in df.columns]
        if group_cols:
            for _, group in df.groupby(group_cols, dropna=False):
                parts.append(sample_df(group, self.args.sample_per_source_resolution, self.rng))
            return pd.concat(parts, ignore_index=True) if parts else df.iloc[0:0].copy()
        return sample_df(df, self.args.sample_per_source_resolution, self.rng)

    def compare_attr_payload(
        self,
        product: str,
        entity: str,
        entity_uid: str,
        paths: list[Path],
        release_json: dict[str, Any],
        release_names: Any,
        release_count: Any,
    ) -> None:
        existing_paths = [p for p in paths if p.exists()]
        expected = merge_upstream_attrs(paths)
        expected_keys = set(expected)
        release_keys = set(release_json)
        release_name_set = {x for x in to_text(release_names).split("|") if x}
        try:
            count_value = int(release_count)
        except (TypeError, ValueError):
            count_value = -1
        match_json = canonical_json(expected) == canonical_json(release_json)
        match_names = expected_keys == release_name_set
        match_count = len(expected_keys) == count_value
        add_row(
            self.global_samples,
            product=product,
            entity=entity,
            entity_uid=entity_uid,
            n_paths=len(paths),
            n_existing_paths=len(existing_paths),
            expected_key_count=len(expected_keys),
            release_key_count=len(release_keys),
            release_name_count=len(release_name_set),
            json_match=match_json,
            names_match=match_names,
            count_match=match_count,
        )
        if not paths:
            add_row(
                self.global_mismatches,
                product=product,
                entity=entity,
                entity_uid=entity_uid,
                field="source_path",
                expected="at least one upstream path",
                actual="",
                detail="no source path available",
            )
            return
        missing_paths = [str(p) for p in paths if not p.exists()]
        if missing_paths:
            add_row(
                self.global_mismatches,
                product=product,
                entity=entity,
                entity_uid=entity_uid,
                field="source_path",
                expected="all paths exist",
                actual="; ".join(missing_paths[:5]),
                detail=f"{len(missing_paths)} missing upstream path(s)",
            )
        if not match_json:
            only_expected = sorted(expected_keys - release_keys)
            only_release = sorted(release_keys - expected_keys)
            different = sorted(k for k in expected_keys & release_keys if canonical_json(expected[k]) != canonical_json(release_json[k]))
            add_row(
                self.global_mismatches,
                product=product,
                entity=entity,
                entity_uid=entity_uid,
                field="global_attrs_json",
                expected=f"keys={len(expected_keys)}",
                actual=f"keys={len(release_keys)}",
                detail=f"missing={only_expected[:10]}; extra={only_release[:10]}; different={different[:10]}",
            )
        if not match_names:
            add_row(
                self.global_mismatches,
                product=product,
                entity=entity,
                entity_uid=entity_uid,
                field="global_attr_names",
                expected="|".join(sorted(expected_keys)),
                actual="|".join(sorted(release_name_set)),
                detail="name set differs from expected upstream attr keys",
            )
        if not match_count:
            add_row(
                self.global_mismatches,
                product=product,
                entity=entity,
                entity_uid=entity_uid,
                field="global_attr_count",
                expected=len(expected_keys),
                actual=count_value,
                detail="count differs from expected upstream attr key count",
            )

    def audit_global_attrs(self) -> None:
        master_path = self.release_dir / "sed_reference_master.nc"
        if master_path.exists() and not self.source_station_catalog.empty:
            samples = self.master_source_samples()
            with nc.Dataset(master_path) as ds:
                uid_to_idx = {
                    uid: i for i, uid in enumerate(read_text_var(ds, "source_station_uid"))
                }
                for _, row in samples.iterrows():
                    uid = norm_text(row.get("source_station_uid"))
                    idx = uid_to_idx.get(uid)
                    if idx is None:
                        continue
                    self.compare_attr_payload(
                        "sed_reference_master.nc",
                        "source_station",
                        uid,
                        split_paths(row.get("source_station_paths", "")),
                        read_json(value_at(ds, "source_station_global_attrs_json", idx)),
                        value_at(ds, "source_station_global_attr_names", idx),
                        value_at(ds, "source_station_global_attr_count", idx),
                    )

        self.audit_matrix_global_attrs()
        self.audit_simple_station_global_attrs(
            "sed_reference_climatology.nc",
            "station_uid",
            "source_station_path",
            "station_global_attrs_json",
            "station_global_attr_names",
            "station_global_attr_count",
            self.args.sample_climatology_stations,
        )
        self.audit_simple_station_global_attrs(
            "sed_reference_satellite.nc",
            "satellite_station_uid",
            "resolved_candidate_path",
            "satellite_station_global_attrs_json",
            "satellite_station_global_attr_names",
            "satellite_station_global_attr_count",
            self.args.sample_satellite_stations,
            fallback_path_var="candidate_path",
        )
        self.record_summary(
            "global_attr_payload_audit",
            "pass" if not self.global_mismatches else "fail",
            f"samples={len(self.global_samples)} mismatches={len(self.global_mismatches)}",
            len(self.global_mismatches),
        )

    def audit_matrix_global_attrs(self) -> None:
        master_path = self.release_dir / "sed_reference_master.nc"
        if not master_path.exists():
            return
        with nc.Dataset(master_path) as master:
            master_uid_to_idx = {uid: i for i, uid in enumerate(read_text_var(master, "cluster_uid"))}
            master_json = {
                uid: read_json(value_at(master, "station_global_attrs_json", idx))
                for uid, idx in master_uid_to_idx.items()
            }
            master_names = {
                uid: set(to_text(value_at(master, "station_global_attr_names", idx)).split("|")) - {""}
                for uid, idx in master_uid_to_idx.items()
            }
            master_counts = {
                uid: int(value_at(master, "station_global_attr_count", idx))
                for uid, idx in master_uid_to_idx.items()
            }
        for resolution, name in MATRIX_FILES.items():
            path = self.release_dir / name
            if not path.exists():
                continue
            with nc.Dataset(path) as ds:
                needed = {"cluster_uid", "station_global_attrs_json", "station_global_attr_names", "station_global_attr_count"}
                if not needed.issubset(ds.variables):
                    continue
                n = len(ds.dimensions.get("n_stations", []))
                for idx in sample_indices(n, self.args.sample_per_source_resolution * 5, self.rng):
                    uid = to_text(value_at(ds, "cluster_uid", idx))
                    if uid not in master_json:
                        add_row(
                            self.global_mismatches,
                            product=name,
                            entity="matrix_station",
                            entity_uid=uid,
                            field="cluster_uid",
                            expected="present in master",
                            actual="missing",
                            detail="matrix cluster_uid not found in master",
                        )
                        continue
                    release_json = read_json(value_at(ds, "station_global_attrs_json", idx))
                    release_names = set(to_text(value_at(ds, "station_global_attr_names", idx)).split("|")) - {""}
                    release_count = int(value_at(ds, "station_global_attr_count", idx))
                    matches = (
                        canonical_json(master_json[uid]) == canonical_json(release_json)
                        and master_names[uid] == release_names
                        and master_counts[uid] == release_count
                    )
                    add_row(
                        self.global_samples,
                        product=name,
                        entity="matrix_station",
                        entity_uid=uid,
                        n_paths="",
                        n_existing_paths="",
                        expected_key_count=master_counts[uid],
                        release_key_count=len(release_json),
                        release_name_count=len(release_names),
                        json_match=canonical_json(master_json[uid]) == canonical_json(release_json),
                        names_match=master_names[uid] == release_names,
                        count_match=master_counts[uid] == release_count,
                    )
                    if not matches:
                        add_row(
                            self.global_mismatches,
                            product=name,
                            entity="matrix_station",
                            entity_uid=uid,
                            field="station_global_attrs",
                            expected="match master station payload",
                            actual="differs",
                            detail=f"resolution={resolution}",
                        )

    def audit_simple_station_global_attrs(
        self,
        product: str,
        uid_var: str,
        path_var: str,
        json_var: str,
        names_var: str,
        count_var: str,
        sample_size: int,
        fallback_path_var: str | None = None,
    ) -> None:
        path = self.release_dir / product
        if not path.exists():
            return
        with nc.Dataset(path) as ds:
            if not {uid_var, path_var, json_var, names_var, count_var}.issubset(ds.variables):
                return
            dim = ds.variables[uid_var].shape[0]
            for idx in sample_indices(dim, sample_size, self.rng):
                raw_path = value_at(ds, path_var, idx)
                paths = split_paths(raw_path)
                if (not paths or not paths[0].exists()) and fallback_path_var and fallback_path_var in ds.variables:
                    paths = split_paths(value_at(ds, fallback_path_var, idx))
                self.compare_attr_payload(
                    product,
                    uid_var.replace("_uid", ""),
                    to_text(value_at(ds, uid_var, idx)),
                    paths,
                    read_json(value_at(ds, json_var, idx)),
                    value_at(ds, names_var, idx),
                    value_at(ds, count_var, idx),
                )

    def audit_promoted_attrs(self) -> None:
        checks = [
            ("sed_reference_master.nc", "source_station_uid", "source_station_global_attrs_json", "source_station_", "n_source_stations", self.args.sample_per_source_resolution * 20),
            ("sed_reference_master.nc", "cluster_uid", "station_global_attrs_json", "", "n_stations", self.args.sample_per_source_resolution * 20),
            ("sed_reference_climatology.nc", "station_uid", "station_global_attrs_json", "", "n_stations", self.args.sample_climatology_stations),
            ("sed_reference_satellite.nc", "satellite_station_uid", "satellite_station_global_attrs_json", "", "n_satellite_stations", self.args.sample_satellite_stations),
        ]
        for product, uid_var, json_var, prefix, dim_name, n_sample in checks:
            path = self.release_dir / product
            if not path.exists():
                continue
            with nc.Dataset(path) as ds:
                if uid_var not in ds.variables or json_var not in ds.variables:
                    continue
                n = len(ds.dimensions[dim_name]) if dim_name in ds.dimensions else ds.variables[uid_var].shape[0]
                for idx in sample_indices(n, n_sample, self.rng):
                    uid = to_text(value_at(ds, uid_var, idx))
                    payload = read_json(value_at(ds, json_var, idx))
                    for field, aliases in PROMOTED_ALIASES.items():
                        var_name = prefix + field
                        if var_name not in ds.variables:
                            continue
                        actual = norm_compare(value_at(ds, var_name, idx))
                        expected = ""
                        expected_key = ""
                        for alias in aliases:
                            if alias in payload and not is_missing_scalar(payload[alias]):
                                expected = norm_compare(payload[alias])
                                expected_key = alias
                                break
                        if not expected:
                            if field in {"country", "continent_region", "geographic_coverage", "iso_a3"} and actual:
                                prov_var = prefix + "geo_attribute_source"
                                provenance = norm_text(value_at(ds, prov_var, idx)) if prov_var in ds.variables else ""
                                if provenance:
                                    add_row(
                                        self.semantic_warnings,
                                        product=product,
                                        entity_uid=uid,
                                        field=var_name,
                                        warning_type="promoted_geo_fill",
                                        detail=f"value not present in JSON aliases; provenance={provenance}; value={actual}",
                                    )
                                else:
                                    add_row(
                                        self.promoted_mismatches,
                                        product=product,
                                        entity_uid=uid,
                                        field=var_name,
                                        expected="JSON alias or provenance-backed fill",
                                        actual=actual,
                                        detail="geographic promoted field has no JSON alias and no geo provenance",
                                    )
                            elif actual:
                                add_row(
                                    self.promoted_mismatches,
                                    product=product,
                                    entity_uid=uid,
                                    field=var_name,
                                    expected="",
                                    actual=actual,
                                    detail="promoted field populated but no alias exists in JSON payload",
                                )
                            continue
                        if actual and expected and actual != expected:
                            add_row(
                                self.promoted_mismatches,
                                product=product,
                                entity_uid=uid,
                                field=var_name,
                                expected=expected,
                                actual=actual,
                                detail=f"expected from JSON key {expected_key}",
                            )
        self.record_summary(
            "promoted_attr_audit",
            "pass" if not self.promoted_mismatches else "fail",
            f"mismatches={len(self.promoted_mismatches)} geo_warnings={sum(1 for r in self.semantic_warnings if r.get('warning_type') == 'promoted_geo_fill')}",
            len(self.promoted_mismatches),
        )

    def compare_catalog_var(
        self,
        product: str,
        entity_uid: str,
        field: str,
        csv_value: Any,
        nc_value: Any,
        tolerance: float = 1e-5,
    ) -> None:
        csv_norm = norm_compare(csv_value)
        nc_norm = norm_compare(nc_value)
        if csv_norm == nc_norm:
            return
        if "|" in csv_norm or "|" in nc_norm:
            csv_set = {v for v in csv_norm.split("|") if v}
            nc_set = {v for v in nc_norm.split("|") if v}
            if csv_set == nc_set:
                return
        try:
            if csv_norm and nc_norm and abs(float(csv_norm) - float(nc_norm)) <= tolerance:
                return
        except ValueError:
            pass
        add_row(
            self.catalog_mismatches,
            product=product,
            entity_uid=entity_uid,
            field=field,
            expected=csv_norm,
            actual=nc_norm,
            detail="catalog value differs from NetCDF value",
        )

    def audit_catalog_parity(self) -> None:
        master_path = self.release_dir / "sed_reference_master.nc"
        if master_path.exists():
            with nc.Dataset(master_path) as ds:
                if not self.source_station_catalog.empty and "source_station_uid" in ds.variables:
                    uid_to_idx = {uid: i for i, uid in enumerate(read_text_var(ds, "source_station_uid"))}
                    for _, row in self.source_station_catalog.iterrows():
                        uid = norm_text(row.get("source_station_uid"))
                        idx = uid_to_idx.get(uid)
                        if idx is None:
                            add_row(self.catalog_mismatches, product="sed_reference_master.nc", entity_uid=uid, field="source_station_uid", expected="present", actual="missing", detail="catalog source station missing from master")
                            continue
                        for field in CAT_SOURCE_STATION_FIELDS:
                            if field not in row:
                                continue
                            nc_value = self.master_source_station_value(ds, idx, field)
                            if nc_value is not None:
                                self.compare_catalog_var("sed_reference_master.nc", uid, field, row[field], nc_value)
                if not self.station_catalog.empty and "cluster_uid" in ds.variables:
                    uid_to_idx = {uid: i for i, uid in enumerate(read_text_var(ds, "cluster_uid"))}
                    for _, row in self.aggregate_station_catalog_for_master().iterrows():
                        uid = norm_text(row.get("cluster_uid"))
                        idx = uid_to_idx.get(uid)
                        if idx is None:
                            add_row(self.catalog_mismatches, product="sed_reference_master.nc", entity_uid=uid, field="cluster_uid", expected="present", actual="missing", detail="station catalog cluster missing from master")
                            continue
                        for field in CAT_STATION_FIELDS:
                            if field not in row:
                                continue
                            if field == "available_resolutions":
                                continue
                            var_name = "basin_match_quality" if field == "basin_match_quality_code" else field
                            if var_name in ds.variables:
                                self.compare_catalog_var("sed_reference_master.nc", uid, field, row[field], value_at(ds, var_name, idx))

        for resolution, name in MATRIX_FILES.items():
            path = self.release_dir / name
            if not path.exists() or self.station_catalog.empty:
                continue
            with nc.Dataset(path) as ds:
                if "cluster_uid" not in ds.variables:
                    continue
                uid_to_idx = {uid: i for i, uid in enumerate(read_text_var(ds, "cluster_uid"))}
                subset = self.station_catalog
                if "resolution" in subset.columns:
                    subset = subset[subset["resolution"].astype(str).str.contains(resolution, na=False)]
                for _, row in subset.iterrows():
                    uid = norm_text(row.get("cluster_uid"))
                    idx = uid_to_idx.get(uid)
                    if idx is None:
                        continue
                    for field in CAT_STATION_FIELDS:
                        if field == "available_resolutions" or field not in row or field not in ds.variables:
                            continue
                        self.compare_catalog_var(name, uid, field, row[field], value_at(ds, field, idx))
                    if "record_count" in row and "n_valid_time_steps" in ds.variables:
                        self.compare_catalog_var(name, uid, "record_count", row["record_count"], value_at(ds, "n_valid_time_steps", idx), tolerance=0)
        self.record_summary(
            "catalog_nc_parity_audit",
            "pass" if not self.catalog_mismatches else "fail",
            f"mismatches={len(self.catalog_mismatches)}",
            len(self.catalog_mismatches),
        )

    def aggregate_station_catalog_for_master(self) -> pd.DataFrame:
        if self.station_catalog.empty or "cluster_uid" not in self.station_catalog.columns:
            return self.station_catalog
        rows = []
        for uid, group in self.station_catalog.groupby("cluster_uid", dropna=False):
            out: dict[str, Any] = {"cluster_uid": uid}
            for field in CAT_STATION_FIELDS:
                if field not in group.columns or field == "cluster_uid":
                    continue
                values = [norm_compare(v) for v in group[field].tolist()]
                values = [v for v in values if v]
                if not values:
                    out[field] = ""
                elif field in {"lat", "lon", "basin_area", "pfaf_code", "cluster_id"}:
                    out[field] = values[0]
                elif field == "available_resolutions":
                    pieces: list[str] = []
                    for value in values:
                        pieces.extend([p for p in value.split("|") if p])
                    out[field] = "|".join(sorted(set(pieces)))
                else:
                    out[field] = "|".join(sorted(set(values)))
            rows.append(out)
        return pd.DataFrame(rows)

    def master_source_station_value(self, ds: nc.Dataset, source_idx: int, field: str) -> Any:
        if field == "cluster_uid" and {"source_station_cluster_index", "cluster_uid"}.issubset(ds.variables):
            station_idx = int(value_at(ds, "source_station_cluster_index", source_idx))
            return value_at(ds, "cluster_uid", station_idx)
        if field == "cluster_id" and {"source_station_cluster_index", "cluster_id"}.issubset(ds.variables):
            station_idx = int(value_at(ds, "source_station_cluster_index", source_idx))
            return value_at(ds, "cluster_id", station_idx)
        if field == "source_name" and {"source_station_source_index", "source_name"}.issubset(ds.variables):
            source_idx2 = int(value_at(ds, "source_station_source_index", source_idx))
            return value_at(ds, "source_name", source_idx2)
        if field == "resolution" and "source_station_resolutions" in ds.variables:
            return value_at(ds, "source_station_resolutions", source_idx)
        var_name = "source_station_" + field if field in {"country", "continent_region", "geographic_coverage", "iso_a3"} else field
        if var_name not in ds.variables:
            return None
        var = ds.variables[var_name]
        if not var.dimensions or var.shape[0] <= source_idx:
            return None
        first_dim = var.dimensions[0]
        if first_dim != "n_source_stations":
            return None
        return value_at(ds, var_name, source_idx)

    def audit_qc_flag_schema(self) -> None:
        for name in ["sed_reference_master.nc", *MATRIX_FILES.values(), "sed_reference_climatology.nc", "sed_reference_satellite.nc"]:
            path = self.release_dir / name
            if not path.exists():
                continue
            with nc.Dataset(path) as ds:
                for flag in ["Q_flag", "SSC_flag", "SSL_flag"]:
                    if flag in ds.variables:
                        self.check_flag_domain(ds, name, flag, FINAL_FLAG_ALLOWED, self.qc_mismatches)
                for flag, allowed in STAGE_FLAG_ALLOWED.items():
                    if flag in ds.variables:
                        self.check_flag_domain(ds, name, flag, allowed, self.stage_qc_mismatches)
        self.record_summary(
            "qc_flag_schema_audit",
            "pass" if not self.qc_mismatches else "fail",
            f"final_flag_mismatches={len(self.qc_mismatches)}",
            len(self.qc_mismatches),
        )
        self.record_summary(
            "stage_qc_schema_audit",
            "pass" if not self.stage_qc_mismatches else "fail",
            f"stage_flag_mismatches={len(self.stage_qc_mismatches)}",
            len(self.stage_qc_mismatches),
        )

    def check_flag_domain(
        self,
        ds: nc.Dataset,
        product: str,
        var_name: str,
        allowed: set[int],
        out_rows: list[dict[str, Any]],
        chunk: int = 128,
    ) -> None:
        var = ds.variables[var_name]
        shape = var.shape
        fill = get_fill_value(var)
        bad_counts: Counter[int] = Counter()
        sample_values: list[int] = []
        total = 0
        if len(shape) == 2:
            for start in range(0, shape[0], chunk):
                data = np.ma.asarray(var[start : start + chunk, :])
                total += data.size
                mask = np.ma.getmaskarray(data)
                vals = np.asarray(data.filled(fill if fill is not None else 0)).astype("int64", copy=False)
                valid = vals[~mask]
                for val in np.unique(valid):
                    ival = int(val)
                    if ival not in allowed and (fill is None or ival != int(fill)):
                        bad_counts[ival] += int(np.sum(valid == ival))
                        if len(sample_values) < 10:
                            sample_values.append(ival)
        else:
            data = np.ma.asarray(var[:])
            total = data.size
            mask = np.ma.getmaskarray(data)
            vals = np.asarray(data.filled(fill if fill is not None else 0)).astype("int64", copy=False)
            valid = vals[~mask]
            for val in np.unique(valid):
                ival = int(val)
                if ival not in allowed and (fill is None or ival != int(fill)):
                    bad_counts[ival] += int(np.sum(valid == ival))
                    if len(sample_values) < 10:
                        sample_values.append(ival)
        add_row(
            self.qc_samples,
            product=product,
            variable=var_name,
            allowed="|".join(str(x) for x in sorted(allowed)),
            n_values=total,
            bad_values="|".join(str(x) for x in sorted(bad_counts)),
            bad_count=sum(bad_counts.values()),
        )
        if bad_counts:
            add_row(
                out_rows,
                product=product,
                variable=var_name,
                allowed="|".join(str(x) for x in sorted(allowed)),
                actual="; ".join(f"{k}:{v}" for k, v in sorted(bad_counts.items())),
                detail=f"unexpected flag values; examples={sample_values}",
            )

    def upstream_info(self, path: Path) -> dict[str, Any] | None:
        path = Path(path)
        if path in self._upstream_time_cache:
            return self._upstream_time_cache[path]
        if not path.exists():
            return None
        try:
            ds = nc.Dataset(path)
        except OSError:
            return None
        if "time" not in ds.variables:
            ds.close()
            return None
        time_var = ds.variables["time"]
        times = time_var[:]
        units = getattr(time_var, "units", "days since 1970-01-01")
        calendar = getattr(time_var, "calendar", "gregorian")
        dates = nc.num2date(times, units=units, calendar=calendar, only_use_cftime_datetimes=False, only_use_python_datetimes=False)
        by_date = {str(d)[:10]: i for i, d in enumerate(dates)}
        info = {"ds": ds, "by_date": by_date}
        self._upstream_time_cache[path] = info
        if len(self._upstream_time_cache) > 256:
            old_path, old_info = next(iter(self._upstream_time_cache.items()))
            old_info["ds"].close()
            del self._upstream_time_cache[old_path]
        return info

    def close_upstream_cache(self) -> None:
        for info in self._upstream_time_cache.values():
            info["ds"].close()
        self._upstream_time_cache.clear()

    def date_from_numeric(self, value: Any, var: nc.Variable) -> str:
        date = nc.num2date(
            float(value),
            units=getattr(var, "units", "days since 1970-01-01"),
            calendar=getattr(var, "calendar", "gregorian"),
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=False,
        )
        return str(date)[:10]

    def compare_source_record(
        self,
        product: str,
        entity_uid: str,
        path: Path,
        release_date: str,
        release_values: dict[str, Any],
    ) -> None:
        info = self.upstream_info(path)
        if info is None:
            add_row(
                self.qc_mismatches,
                product=product,
                entity_uid=entity_uid,
                time=release_date,
                variable="source_file",
                expected="openable upstream NetCDF",
                actual=str(path),
                detail="upstream file missing/unreadable, or selected_source_station_uid could not be joined to source_station_catalog.csv",
            )
            return
        ds = info["ds"]
        idx = info["by_date"].get(release_date)
        if idx is None:
            add_row(
                self.qc_mismatches,
                product=product,
                entity_uid=entity_uid,
                time=release_date,
                variable="time",
                expected="date present in upstream file",
                actual="missing",
                detail=str(path),
            )
            return
        for var_name in ["Q", "SSC", "SSL"]:
            if var_name not in ds.variables:
                continue
            expected = to_python(ds.variables[var_name][idx])
            actual = release_values.get(var_name)
            if not self.float_equal(expected, actual):
                add_row(
                    self.qc_mismatches,
                    product=product,
                    entity_uid=entity_uid,
                    time=release_date,
                    variable=var_name,
                    expected=norm_compare(expected),
                    actual=norm_compare(actual),
                    detail=f"value differs from upstream {path}",
                )
        for var_name in ["Q_flag", "SSC_flag", "SSL_flag"]:
            if var_name in ds.variables and var_name in release_values:
                expected = to_python(ds.variables[var_name][idx])
                actual = release_values[var_name]
                if not self.flag_equal(expected, actual):
                    add_row(
                        self.qc_mismatches,
                        product=product,
                        entity_uid=entity_uid,
                        time=release_date,
                        variable=var_name,
                        expected=norm_compare(expected),
                        actual=norm_compare(actual),
                        detail=f"flag differs from upstream {path}",
                    )
        for release_name, upstream_name in UPSTREAM_QC_NAMES.items():
            if upstream_name in ds.variables and release_name in release_values:
                expected = to_python(ds.variables[upstream_name][idx])
                actual = release_values[release_name]
                if not self.flag_equal(expected, actual):
                    add_row(
                        self.stage_qc_mismatches,
                        product=product,
                        entity_uid=entity_uid,
                        time=release_date,
                        variable=release_name,
                        expected=norm_compare(expected),
                        actual=norm_compare(actual),
                        detail=f"stage flag differs from upstream {upstream_name} in {path}",
                    )

    def float_equal(self, expected: Any, actual: Any, tol: float = 1e-5) -> bool:
        if is_missing_scalar(expected) and is_missing_scalar(actual):
            return True
        try:
            e = float(to_python(expected))
            a = float(to_python(actual))
        except (TypeError, ValueError):
            return norm_compare(expected) == norm_compare(actual)
        if not math.isfinite(e) and not math.isfinite(a):
            return True
        return abs(e - a) <= tol

    def flag_equal(self, expected: Any, actual: Any) -> bool:
        if is_missing_scalar(expected) and is_missing_scalar(actual):
            return True
        if is_missing_scalar(expected) or is_missing_scalar(actual):
            return False
        try:
            return int(expected) == int(actual)
        except (TypeError, ValueError):
            return norm_compare(expected) == norm_compare(actual)

    def audit_source_fidelity(self) -> None:
        qc_before = len(self.qc_mismatches)
        stage_before = len(self.stage_qc_mismatches)
        self.audit_master_source_fidelity()
        self.audit_matrix_source_fidelity()
        self.close_upstream_cache()
        qc_new = len(self.qc_mismatches) - qc_before
        stage_new = len(self.stage_qc_mismatches) - stage_before
        self.record_summary(
            "source_to_s8_qc_fidelity_audit",
            "pass" if qc_new == 0 and stage_new == 0 else "fail",
            f"qc_mismatches={qc_new} stage_mismatches={stage_new}",
            qc_new + stage_new,
        )

    def audit_master_source_fidelity(self) -> None:
        path = self.release_dir / "sed_reference_master.nc"
        if not path.exists():
            return
        cat = self.source_station_catalog
        with nc.Dataset(path) as ds:
            if "n_records" not in ds.dimensions:
                return
            uid_values = read_text_var(ds, "source_station_uid")
            path_values = read_text_var(ds, "source_station_paths")
            for rec_idx in sample_indices(len(ds.dimensions["n_records"]), self.args.sample_master_records, self.rng):
                src_idx = int(value_at(ds, "source_station_index", rec_idx))
                if src_idx < 0 or src_idx >= len(uid_values):
                    continue
                uid = uid_values[src_idx]
                release_date = self.date_from_numeric(value_at(ds, "time", rec_idx), ds.variables["time"])
                release_values = {v: value_at(ds, v, rec_idx) for v in ["Q", "SSC", "SSL", "Q_flag", "SSC_flag", "SSL_flag", *STAGE_FLAG_ALLOWED] if v in ds.variables}
                add_row(
                    self.qc_samples,
                    product="sed_reference_master.nc",
                    variable="source_fidelity_record",
                    allowed="",
                    n_values=1,
                    bad_values="",
                    bad_count=0,
                    entity_uid=uid,
                    time=release_date,
                )
                paths = split_paths(path_values[src_idx])
                chosen_path = paths[0] if paths else Path("")
                if len(paths) > 1 and not cat.empty and "source_station_uid" in cat.columns:
                    row = cat[cat["source_station_uid"] == uid]
                    if not row.empty:
                        row_paths = split_paths(row.iloc[0].get("source_station_paths", ""))
                        if row_paths:
                            chosen_path = row_paths[0]
                self.compare_source_record("sed_reference_master.nc", uid, chosen_path, release_date, release_values)

    def audit_matrix_source_fidelity(self) -> None:
        if self.source_station_catalog.empty or "source_station_uid" not in self.source_station_catalog.columns:
            return
        uid_to_path = {
            norm_text(row["source_station_uid"]): split_paths(row.get("source_station_paths", ""))
            for _, row in self.source_station_catalog.iterrows()
        }
        for resolution, name in MATRIX_FILES.items():
            path = self.release_dir / name
            if not path.exists():
                continue
            with nc.Dataset(path) as ds:
                if "selected_source_station_uid" not in ds.variables or "time" not in ds.variables:
                    continue
                n_stations, n_times = ds.variables["selected_source_station_uid"].shape
                samples = set()
                attempts = 0
                while len(samples) < self.args.sample_matrix_cells and attempts < self.args.sample_matrix_cells * 100:
                    attempts += 1
                    si = self.rng.randrange(n_stations)
                    ti = self.rng.randrange(n_times)
                    uid = to_text(ds.variables["selected_source_station_uid"][si, ti])
                    if uid:
                        samples.add((si, ti, uid))
                for si, ti, uid in sorted(samples):
                    release_date = self.date_from_numeric(value_at(ds, "time", ti), ds.variables["time"])
                    release_values = {v: value_at(ds, v, (si, ti)) for v in ["Q", "SSC", "SSL", "Q_flag", "SSC_flag", "SSL_flag", *STAGE_FLAG_ALLOWED] if v in ds.variables}
                    add_row(
                        self.qc_samples,
                        product=name,
                        variable="source_fidelity_cell",
                        allowed="",
                        n_values=1,
                        bad_values="",
                        bad_count=0,
                        entity_uid=uid,
                        time=release_date,
                    )
                    paths = uid_to_path.get(uid, [])
                    self.compare_source_record(name, uid, paths[0] if paths else Path(""), release_date, release_values)

    def audit_semantics(self) -> None:
        for name in ["sed_reference_master.nc", *MATRIX_FILES.values(), "sed_reference_climatology.nc", "sed_reference_satellite.nc"]:
            path = self.release_dir / name
            if path.exists():
                with nc.Dataset(path) as ds:
                    self.check_semantic_product(ds, name)
        hard = sum(1 for r in self.semantic_warnings if r.get("severity") == "hard")
        self.record_summary(
            "value_flag_semantic_audit",
            "pass" if hard == 0 else "fail",
            f"semantic_rows={len(self.semantic_warnings)} hard={hard}",
            hard,
        )

    def check_semantic_product(self, ds: nc.Dataset, product: str, chunk: int = 64) -> None:
        variables = set(ds.variables)
        value_vars = [v for v in ["Q", "SSC", "SSL"] if v in variables]
        if not value_vars:
            return
        shape = ds.variables[value_vars[0]].shape
        if len(shape) == 2:
            for start in range(0, shape[0], chunk):
                stop = min(start + chunk, shape[0])
                self.check_semantic_slice(ds, product, (slice(start, stop), slice(None)), start)
            self.check_selected_uid_semantics(ds, product, shape)
        else:
            self.check_semantic_slice(ds, product, slice(None), 0)

    def check_semantic_slice(self, ds: nc.Dataset, product: str, idx: Any, offset: int) -> None:
        for var_name in ["Q", "SSC", "SSL"]:
            flag_name = var_name + "_flag"
            if var_name not in ds.variables or flag_name not in ds.variables:
                continue
            values = np.ma.asarray(ds.variables[var_name][idx])
            flags = np.ma.asarray(ds.variables[flag_name][idx])
            missing = numeric_missing(values, get_fill_value(ds.variables[var_name]))
            flag_arr = np.asarray(flags.filled(get_fill_value(ds.variables[flag_name]) or 9)).astype("int64", copy=False)
            bad_flag_has_value = np.argwhere(((flag_arr == 3) | (flag_arr == 9)) & ~missing)
            missing_good = np.argwhere(missing & (flag_arr == 0))
            if bad_flag_has_value.size:
                add_row(
                    self.semantic_warnings,
                    product=product,
                    severity="hard",
                    warning_type="bad_or_missing_flag_has_value",
                    field=var_name,
                    count=len(bad_flag_has_value),
                    detail=f"first_index={self.format_index(bad_flag_has_value[0], offset)}",
                )
            if missing_good.size:
                add_row(
                    self.semantic_warnings,
                    product=product,
                    severity="warning",
                    warning_type="missing_value_good_flag",
                    field=var_name,
                    count=len(missing_good),
                    detail=f"first_index={self.format_index(missing_good[0], offset)}",
                )
        present = None
        for var_name in ["Q", "SSC", "SSL"]:
            if var_name not in ds.variables:
                continue
            values = np.ma.asarray(ds.variables[var_name][idx])
            missing = numeric_missing(values, get_fill_value(ds.variables[var_name]))
            present = ~missing if present is None else (present | ~missing)
        sed_present = None
        for var_name in ["SSC", "SSL"]:
            if var_name not in ds.variables:
                continue
            values = np.ma.asarray(ds.variables[var_name][idx])
            missing = numeric_missing(values, get_fill_value(ds.variables[var_name]))
            sed_present = ~missing if sed_present is None else (sed_present | ~missing)
        if present is not None and sed_present is not None:
            no_sed = np.argwhere(present & ~sed_present)
            if no_sed.size:
                add_row(
                    self.semantic_warnings,
                    product=product,
                    severity="warning",
                    warning_type="published_record_without_ssc_or_ssl",
                    field="SSC|SSL",
                    count=len(no_sed),
                    detail=f"first_index={self.format_index(no_sed[0], offset)}",
                )
    def check_selected_uid_semantics(self, ds: nc.Dataset, product: str, shape: tuple[int, ...]) -> None:
        if "selected_source_station_uid" not in ds.variables or len(shape) != 2:
            return
        n_samples = min(max(self.args.sample_matrix_cells, 1), shape[0] * shape[1])
        misses = 0
        first = ""
        attempts = 0
        checked = 0
        while checked < n_samples and attempts < n_samples * 200:
            attempts += 1
            si = self.rng.randrange(shape[0])
            ti = self.rng.randrange(shape[1])
            has_value = False
            for var_name in ["Q", "SSC", "SSL"]:
                if var_name not in ds.variables:
                    continue
                value = ds.variables[var_name][si, ti]
                if not numeric_missing(np.ma.asarray([value]), get_fill_value(ds.variables[var_name]))[0]:
                    has_value = True
                    break
            if not has_value:
                continue
            checked += 1
            uid = to_text(ds.variables["selected_source_station_uid"][si, ti])
            if not uid:
                misses += 1
                if not first:
                    first = f"{si},{ti}"
        if misses:
            add_row(
                self.semantic_warnings,
                product=product,
                severity="hard",
                warning_type="value_without_selected_source_station_uid",
                field="selected_source_station_uid",
                count=misses,
                detail=f"sampled_cells={checked}; first_index={first}",
            )

    def format_index(self, idx: np.ndarray, offset: int) -> str:
        values = [int(v) for v in np.asarray(idx).ravel()]
        if len(values) == 2:
            values[0] += offset
        elif len(values) == 1:
            values[0] += offset
        return ",".join(str(v) for v in values)

    def audit_overlap_selection(self) -> None:
        path = self.release_dir / "sed_reference_overlap_candidates.csv.gz"
        if not path.exists():
            self.record_summary("overlap_selection_audit", "fail", "overlap sidecar missing", 1)
            return
        df = pd.read_csv(path, keep_default_na=False, dtype=str, low_memory=False)
        if "is_overlap" in df.columns:
            df = df[df["is_overlap"].astype(str).isin(["1", "True", "true"])]
        key_cols = [c for c in ["cluster_uid", "resolution", "date", "time"] if c in df.columns]
        selected_col = "selected_flag" if "selected_flag" in df.columns else None
        uid_col = "source_station_uid" if "source_station_uid" in df.columns else "selected_source_station_uid" if "selected_source_station_uid" in df.columns else None
        if not key_cols or not selected_col:
            add_row(self.selection_mismatches, product="sed_reference_overlap_candidates.csv.gz", field="schema", expected="key columns and selected_flag", actual="missing", detail=f"columns={list(df.columns)}")
        else:
            groups = list(df.groupby(key_cols, dropna=False))
            chosen = sample_df(pd.DataFrame({"i": range(len(groups))}), self.args.sample_overlap_records, self.rng)
            for i in chosen["i"].tolist():
                key, group = groups[int(i)]
                selected = group[group[selected_col].astype(str).isin(["1", "True", "true"])]
                if len(selected) != 1:
                    add_row(
                        self.selection_mismatches,
                        product="sed_reference_overlap_candidates.csv.gz",
                        field="selected_flag",
                        expected=1,
                        actual=len(selected),
                        detail=f"key={key}",
                    )
        self.audit_overlap_against_products(df, uid_col)
        self.record_summary(
            "overlap_selection_audit",
            "pass" if not self.selection_mismatches else "fail",
            f"selection_mismatches={len(self.selection_mismatches)}",
            len(self.selection_mismatches),
        )

    def audit_overlap_against_products(self, df: pd.DataFrame, uid_col: str | None) -> None:
        if df.empty or uid_col is None or "cluster_uid" not in df.columns:
            return
        selected_col = "selected_flag" if "selected_flag" in df.columns else None
        if not selected_col:
            return
        df = df[df[selected_col].astype(str).isin(["1", "True", "true"])]
        if df.empty:
            return
        samples = sample_df(df, self.args.sample_overlap_records, self.rng)
        master_path = self.release_dir / "sed_reference_master.nc"
        master_lookup: dict[tuple[str, str], str] = {}
        if master_path.exists():
            with nc.Dataset(master_path) as ds:
                if {"cluster_uid", "station_index", "source_station_index", "source_station_uid", "time"}.issubset(ds.variables):
                    cluster_uids = read_text_var(ds, "cluster_uid")
                    source_uids = read_text_var(ds, "source_station_uid")
                    for rec_idx in sample_indices(len(ds.dimensions["n_records"]), min(50000, len(ds.dimensions["n_records"])), self.rng):
                        station_idx = int(value_at(ds, "station_index", rec_idx))
                        src_idx = int(value_at(ds, "source_station_index", rec_idx))
                        if 0 <= station_idx < len(cluster_uids) and 0 <= src_idx < len(source_uids):
                            date = self.date_from_numeric(value_at(ds, "time", rec_idx), ds.variables["time"])
                            master_lookup[(cluster_uids[station_idx], date)] = source_uids[src_idx]
        for _, row in samples.iterrows():
            cluster = norm_text(row.get("cluster_uid"))
            expected_uid = norm_text(row.get(uid_col))
            date = norm_text(row.get("date", row.get("time", "")))[:10]
            actual = master_lookup.get((cluster, date))
            if actual and expected_uid and actual != expected_uid:
                add_row(
                    self.selection_mismatches,
                    product="sed_reference_master.nc",
                    field="selected_source_station_uid",
                    expected=expected_uid,
                    actual=actual,
                    detail=f"cluster_uid={cluster}; date={date}",
                )

    def write_outputs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        outputs = {
            "s8_audit_summary.csv": self.summary,
            "s8_global_attr_audit_sample.csv": self.global_samples,
            "s8_global_attr_mismatches.csv": self.global_mismatches,
            "s8_promoted_attr_mismatches.csv": self.promoted_mismatches,
            "s8_catalog_nc_parity_mismatches.csv": self.catalog_mismatches,
            "s8_qc_flag_audit_sample.csv": self.qc_samples,
            "s8_qc_flag_mismatches.csv": self.qc_mismatches,
            "s8_stage_qc_mismatches.csv": self.stage_qc_mismatches,
            "s8_selection_mismatches.csv": self.selection_mismatches,
            "s8_semantic_warnings.csv": self.semantic_warnings,
        }
        for name, rows in outputs.items():
            pd.DataFrame(rows).to_csv(self.output_dir / name, index=False)
        self.write_markdown_summary()

    def write_markdown_summary(self) -> None:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines = [
            "# S8 Release Product Audit",
            "",
            f"Generated: {now}",
            f"Release directory: `{self.release_dir}`",
            f"Seed: `{self.args.seed}`",
            "",
            "## Summary",
            "",
            "| Check | Status | Count | Details |",
            "|---|---:|---:|---|",
        ]
        for row in self.summary:
            lines.append(f"| {row['check']} | {row['status']} | {row['count']} | {str(row['details']).replace('|', '/')} |")
        lines.extend(["", "## NetCDF Schema Inventory", ""])
        lines.extend(self.schema_lines)
        lines.extend(
            [
                "",
                "## Report Files",
                "",
                "- `s8_audit_summary.csv`",
                "- `s8_global_attr_audit_sample.csv`",
                "- `s8_global_attr_mismatches.csv`",
                "- `s8_promoted_attr_mismatches.csv`",
                "- `s8_catalog_nc_parity_mismatches.csv`",
                "- `s8_qc_flag_audit_sample.csv`",
                "- `s8_qc_flag_mismatches.csv`",
                "- `s8_stage_qc_mismatches.csv`",
                "- `s8_selection_mismatches.csv`",
                "- `s8_semantic_warnings.csv`",
            ]
        )
        (self.output_dir / "s8_audit_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def run(self) -> None:
        self.audit_schema()
        self.audit_global_attrs()
        self.audit_promoted_attrs()
        self.audit_catalog_parity()
        self.audit_qc_flag_schema()
        self.audit_source_fidelity()
        self.audit_semantics()
        self.audit_overlap_selection()
        self.write_outputs()


def main() -> int:
    args = parse_args()
    audit = Audit(args)
    try:
        audit.run()
    finally:
        audit.close_upstream_cache()
    failures = [row for row in audit.summary if row["status"] == "fail"]
    print(f"Wrote S8 audit reports to {audit.output_dir}")
    print(f"Summary checks: {len(audit.summary)}; failing checks: {len(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
