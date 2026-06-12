#!/usr/bin/env python3
"""Boundary-based geographic metadata enrichment for S6 release variables.

This module intentionally treats boundary matching as metadata enrichment only.
It never participates in station selection, QC, basin matching, or value
merging. Upstream NetCDF global attributes remain the first source of truth; an
admin0 polygon layer is used only to fill missing promoted geographic fields.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

import numpy as np

try:
    import geopandas as gpd
    from shapely.geometry import Point

    HAS_GPD = True
except Exception:  # pragma: no cover - depends on runtime geo stack
    gpd = None
    Point = None
    HAS_GPD = False


GEO_VALUE_FIELDS = ("country", "continent_region", "geographic_coverage", "iso_a3")
GEO_PROVENANCE_FIELDS = (
    "geo_attribute_source",
    "geo_attribute_confidence",
    "geo_attribute_method",
    "geo_boundary_dataset",
    "geo_boundary_version",
)

COUNTRY_CANDIDATES = (
    "country",
    "country_name",
    "admin",
    "ADMIN",
    "name",
    "NAME",
    "NAME_EN",
    "NAME_LONG",
    "SOVEREIGNT",
)
ISO_CANDIDATES = ("iso_a3", "ISO_A3", "adm0_a3", "ADM0_A3", "ADM0_A3_US", "SOV_A3", "GU_A3")
CONTINENT_CANDIDATES = ("continent", "CONTINENT", "continent_region")
REGION_CANDIDATES = ("region", "REGION", "subregion", "SUBREGION", "REGION_UN", "REGION_WB")


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
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def add_geo_boundary_args(parser) -> None:
    parser.add_argument(
        "--geo-boundary-file",
        default=os.environ.get("SED_GEO_BOUNDARY_FILE", ""),
        help="Optional admin0 polygon file used to fill missing promoted geographic variables.",
    )
    parser.add_argument(
        "--geo-boundary-name-col",
        default=os.environ.get("SED_GEO_BOUNDARY_NAME_COL", ""),
        help="Boundary layer country/name column. Default: auto-detect.",
    )
    parser.add_argument(
        "--geo-boundary-iso-col",
        default=os.environ.get("SED_GEO_BOUNDARY_ISO_COL", ""),
        help="Boundary layer ISO alpha-3 column. Default: auto-detect.",
    )
    parser.add_argument(
        "--geo-boundary-continent-col",
        default=os.environ.get("SED_GEO_BOUNDARY_CONTINENT_COL", ""),
        help="Boundary layer continent column. Default: auto-detect.",
    )
    parser.add_argument(
        "--geo-boundary-region-col",
        default=os.environ.get("SED_GEO_BOUNDARY_REGION_COL", ""),
        help="Boundary layer region/subregion column. Default: auto-detect.",
    )
    parser.add_argument(
        "--geo-boundary-dataset",
        default=os.environ.get("SED_GEO_BOUNDARY_DATASET", ""),
        help="Boundary dataset name stored in promoted provenance variables.",
    )
    parser.add_argument(
        "--geo-boundary-version",
        default=os.environ.get("SED_GEO_BOUNDARY_VERSION", ""),
        help="Boundary dataset version stored in promoted provenance variables.",
    )
    parser.add_argument(
        "--skip-boundary-geo-enrichment",
        action="store_true",
        default=os.environ.get("SED_SKIP_BOUNDARY_GEO_ENRICHMENT", "").strip().lower() in {"1", "true", "yes", "y"},
        help="Disable admin0 boundary enrichment even if --geo-boundary-file is configured.",
    )


def boundary_options_from_args(args) -> Dict[str, object]:
    return {
        "boundary_file": clean_text(getattr(args, "geo_boundary_file", "")),
        "name_col": clean_text(getattr(args, "geo_boundary_name_col", "")),
        "iso_col": clean_text(getattr(args, "geo_boundary_iso_col", "")),
        "continent_col": clean_text(getattr(args, "geo_boundary_continent_col", "")),
        "region_col": clean_text(getattr(args, "geo_boundary_region_col", "")),
        "boundary_dataset": clean_text(getattr(args, "geo_boundary_dataset", "")),
        "boundary_version": clean_text(getattr(args, "geo_boundary_version", "")),
        "skip": bool(getattr(args, "skip_boundary_geo_enrichment", False)),
    }


def boundary_options_from_argv(argv: Sequence[str]) -> Dict[str, object]:
    options = {
        "boundary_file": os.environ.get("SED_GEO_BOUNDARY_FILE", ""),
        "name_col": os.environ.get("SED_GEO_BOUNDARY_NAME_COL", ""),
        "iso_col": os.environ.get("SED_GEO_BOUNDARY_ISO_COL", ""),
        "continent_col": os.environ.get("SED_GEO_BOUNDARY_CONTINENT_COL", ""),
        "region_col": os.environ.get("SED_GEO_BOUNDARY_REGION_COL", ""),
        "boundary_dataset": os.environ.get("SED_GEO_BOUNDARY_DATASET", ""),
        "boundary_version": os.environ.get("SED_GEO_BOUNDARY_VERSION", ""),
        "skip": os.environ.get("SED_SKIP_BOUNDARY_GEO_ENRICHMENT", "").strip().lower() in {"1", "true", "yes", "y"},
    }
    value_options = {
        "--geo-boundary-file": "boundary_file",
        "--geo-boundary-name-col": "name_col",
        "--geo-boundary-iso-col": "iso_col",
        "--geo-boundary-continent-col": "continent_col",
        "--geo-boundary-region-col": "region_col",
        "--geo-boundary-dataset": "boundary_dataset",
        "--geo-boundary-version": "boundary_version",
    }
    idx = 0
    argv = list(argv)
    while idx < len(argv):
        arg = argv[idx]
        if arg == "--skip-boundary-geo-enrichment":
            options["skip"] = True
        elif "=" in arg:
            name, value = arg.split("=", 1)
            key = value_options.get(name)
            if key:
                options[key] = value
        else:
            key = value_options.get(arg)
            if key and idx + 1 < len(argv):
                options[key] = argv[idx + 1]
                idx += 1
        idx += 1
    return {key: clean_text(value) if key != "skip" else bool(value) for key, value in options.items()}


def _first_col(columns: Iterable[str], preferred: str, candidates: Sequence[str]) -> Optional[str]:
    columns = list(columns)
    if preferred and preferred in columns:
        return preferred
    lower = {str(col).lower(): col for col in columns}
    if preferred and preferred.lower() in lower:
        return lower[preferred.lower()]
    for candidate in candidates:
        if candidate in columns:
            return candidate
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def _continent_region(continent: object, region: object) -> str:
    continent_text = clean_text(continent)
    region_text = clean_text(region)
    if continent_text and region_text and continent_text.lower() != region_text.lower():
        return "{}, {}".format(continent_text, region_text)
    return continent_text or region_text


def _valid_lat_lon(lat: object, lon: object) -> bool:
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return False
    return np.isfinite(lat_f) and np.isfinite(lon_f) and -90 <= lat_f <= 90 and -180 <= lon_f <= 180


def _normalise_boundary(
    boundary_file: str,
    name_col: str = "",
    iso_col: str = "",
    continent_col: str = "",
    region_col: str = "",
):
    if not HAS_GPD:
        raise RuntimeError("geopandas is required for --geo-boundary-file enrichment")
    path = Path(clean_text(boundary_file)).expanduser()
    if not path.is_file():
        raise FileNotFoundError("Geo boundary file not found: {}".format(path))

    boundary = gpd.read_file(path)
    if boundary.empty:
        raise ValueError("Geo boundary file has no features: {}".format(path))
    geom_type = boundary.geometry.geom_type.fillna("").astype(str)
    boundary = boundary[geom_type.str.contains("Polygon", case=False, regex=False)].copy()
    if boundary.empty:
        raise ValueError("Geo boundary file must contain polygon geometries, not only lines/points: {}".format(path))
    boundary = boundary.set_crs("EPSG:4326") if boundary.crs is None else boundary.to_crs("EPSG:4326")

    country_col = _first_col(boundary.columns, name_col, COUNTRY_CANDIDATES)
    iso_col = _first_col(boundary.columns, iso_col, ISO_CANDIDATES)
    continent_col = _first_col(boundary.columns, continent_col, CONTINENT_CANDIDATES)
    region_col = _first_col(boundary.columns, region_col, REGION_CANDIDATES)

    if not country_col and not iso_col:
        raise ValueError("Geo boundary file must provide at least a country/name or ISO column")

    out = boundary[["geometry"]].copy()
    out["_boundary_country"] = boundary[country_col].map(clean_text) if country_col else ""
    out["_boundary_iso_a3"] = boundary[iso_col].map(clean_text) if iso_col else ""
    continents = boundary[continent_col].map(clean_text).tolist() if continent_col else [""] * len(boundary)
    regions = boundary[region_col].map(clean_text).tolist() if region_col else [""] * len(boundary)
    out["_boundary_continent_region"] = [
        _continent_region(continent, region) for continent, region in zip(continents, regions)
    ]
    return out


def _boundary_values_for_points(boundary, lats: Sequence[object], lons: Sequence[object]) -> Dict[int, Dict[str, str]]:
    valid_indices = [idx for idx, (lat, lon) in enumerate(zip(lats, lons)) if _valid_lat_lon(lat, lon)]
    if not valid_indices:
        return {}
    points = gpd.GeoDataFrame(
        {"_payload_index": valid_indices},
        geometry=[Point(float(lons[idx]), float(lats[idx])) for idx in valid_indices],
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(points, boundary, how="left", predicate="within")
    matched_payload_indices = set()
    for _, row in joined.iterrows():
        if any(
            clean_text(row.get(col, ""))
            for col in ("_boundary_country", "_boundary_iso_a3", "_boundary_continent_region")
        ):
            matched_payload_indices.add(int(row["_payload_index"]))
    missing_indices = [idx for idx in valid_indices if idx not in matched_payload_indices]
    if missing_indices:
        retry_points = points[points["_payload_index"].isin(missing_indices)]
        retry = gpd.sjoin(retry_points, boundary, how="left", predicate="intersects")
        joined = gpd.GeoDataFrame(
            list(joined.to_dict("records")) + list(retry.to_dict("records")),
            geometry="geometry",
            crs="EPSG:4326",
        )

    out: Dict[int, Dict[str, str]] = {}
    for _, row in joined.iterrows():
        idx = int(row["_payload_index"])
        if idx in out:
            continue
        country = clean_text(row.get("_boundary_country", ""))
        iso = clean_text(row.get("_boundary_iso_a3", ""))
        continent_region = clean_text(row.get("_boundary_continent_region", ""))
        if not any((country, iso, continent_region)):
            continue
        out[idx] = {
            "country": country,
            "iso_a3": iso,
            "continent_region": continent_region,
            "geographic_coverage": country,
        }
    return out


def _geo_confidence(promoted: Mapping[str, object]) -> str:
    filled = sum(1 for field in GEO_VALUE_FIELDS if clean_text(promoted.get(field, "")))
    if filled == len(GEO_VALUE_FIELDS):
        return "high"
    if filled:
        return "medium"
    return "missing"


def _ensure_upstream_geo_provenance(promoted: MutableMapping[str, object]) -> None:
    if not any(clean_text(promoted.get(field, "")) for field in GEO_VALUE_FIELDS):
        promoted.setdefault("geo_attribute_source", "")
        promoted.setdefault("geo_attribute_confidence", "")
        promoted.setdefault("geo_attribute_method", "")
        promoted.setdefault("geo_boundary_dataset", "")
        promoted.setdefault("geo_boundary_version", "")
        return
    if not clean_text(promoted.get("geo_attribute_source", "")):
        promoted["geo_attribute_source"] = (
            "source_nc_global_attrs"
            if all(clean_text(promoted.get(field, "")) for field in GEO_VALUE_FIELDS)
            else "source_nc_global_attrs_partial"
        )
    if not clean_text(promoted.get("geo_attribute_confidence", "")):
        promoted["geo_attribute_confidence"] = _geo_confidence(promoted)
    if not clean_text(promoted.get("geo_attribute_method", "")):
        promoted["geo_attribute_method"] = "upstream_global_attrs"
    promoted.setdefault("geo_boundary_dataset", "")
    promoted.setdefault("geo_boundary_version", "")


def enrich_global_attr_payloads(
    payloads: List[MutableMapping[str, object]],
    lats: Sequence[object],
    lons: Sequence[object],
    boundary_file: str = "",
    name_col: str = "",
    iso_col: str = "",
    continent_col: str = "",
    region_col: str = "",
    boundary_dataset: str = "",
    boundary_version: str = "",
    skip: bool = False,
    subject: str = "station",
    logger=print,
) -> List[MutableMapping[str, object]]:
    """Fill missing promoted geographic fields from an admin0 boundary layer.

    The input payload objects are mutated in place and returned for convenience.
    The raw JSON payload is not changed; only the promoted direct-query fields
    are enriched.
    """
    for payload in payloads:
        promoted = payload.setdefault("promoted", {})
        _ensure_upstream_geo_provenance(promoted)

    boundary_file = clean_text(boundary_file)
    if skip or not boundary_file:
        return payloads

    try:
        boundary = _normalise_boundary(
            boundary_file,
            name_col=name_col,
            iso_col=iso_col,
            continent_col=continent_col,
            region_col=region_col,
        )
        point_values = _boundary_values_for_points(boundary, lats, lons)
    except Exception as exc:
        if logger:
            logger("Warning: boundary geo enrichment skipped for {}: {}".format(subject, exc))
        return payloads

    dataset_name = clean_text(boundary_dataset) or Path(boundary_file).stem
    version_text = clean_text(boundary_version)
    filled_payloads = 0
    filled_values = 0

    for idx, payload in enumerate(payloads):
        values = point_values.get(idx)
        if not values:
            continue
        promoted = payload.setdefault("promoted", {})
        had_geo = any(clean_text(promoted.get(field, "")) for field in GEO_VALUE_FIELDS)
        changed = False
        for field in GEO_VALUE_FIELDS:
            if not clean_text(promoted.get(field, "")) and clean_text(values.get(field, "")):
                promoted[field] = clean_text(values[field])
                changed = True
                filled_values += 1
        if changed:
            filled_payloads += 1
            promoted["geo_attribute_source"] = (
                "source_nc_global_attrs_plus_boundary_admin0" if had_geo else "boundary_admin0"
            )
            promoted["geo_attribute_confidence"] = _geo_confidence(promoted)
            promoted["geo_attribute_method"] = "point_in_polygon"
            promoted["geo_boundary_dataset"] = dataset_name
            promoted["geo_boundary_version"] = version_text

    if logger:
        logger(
            "Boundary geo enrichment for {}: filled {} values across {} payloads using {}".format(
                subject,
                filled_values,
                filled_payloads,
                dataset_name,
            )
        )
    return payloads


def geo_values_from_payload(payload: Mapping[str, object]) -> Dict[str, str]:
    promoted = payload.get("promoted", {}) if isinstance(payload, Mapping) else {}
    return {field: clean_text(promoted.get(field, "")) for field in GEO_VALUE_FIELDS + GEO_PROVENANCE_FIELDS}
