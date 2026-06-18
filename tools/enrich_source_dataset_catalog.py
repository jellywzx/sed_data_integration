#!/usr/bin/env python3
"""Enrich source_dataset_catalog.csv with ESSD-ready provenance fields.

The S8 release builder creates a compact source_dataset_catalog.csv from the
source-station catalog. That compact catalog is useful for joins, but ESSD data
papers also need dataset-level provenance fields such as source role, source
version, DOI/URL, licence or terms, access date, variable treatment, and a
preferred citation.

This utility is intentionally conservative: it never invents an access date or
licence. It preserves existing non-empty values, adds release-derived counts and
time coverage from source_station_catalog.csv, and fills known source descriptors
from a small curated registry. Unknown or not-yet-verified fields are left empty
or marked as TBC so they can be completed manually before submission.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_RELEASE_DIR = SCRIPT_DIR / "scripts_basin_test/output/sed_reference_release"
DEFAULT_SOURCE_DATASET_CATALOG = DEFAULT_RELEASE_DIR / "source_dataset_catalog.csv"
DEFAULT_SOURCE_STATION_CATALOG = DEFAULT_RELEASE_DIR / "source_station_catalog.csv"

MINIMAL_MATRIX_RESOLUTIONS = {"daily", "monthly", "annual"}

OUTPUT_COLUMNS = [
    "source_name",
    "source_long_name",
    "institution",
    "source_category",
    "release_role",
    "included_in_minimal_release",
    "source_version",
    "reference",
    "data_doi",
    "article_doi",
    "source_url",
    "license_or_terms",
    "access_date",
    "country",
    "continent_region",
    "geographic_coverage",
    "available_resolutions",
    "temporal_resolution_used",
    "temporal_span_used",
    "time_start",
    "time_end",
    "n_source_stations",
    "n_clusters",
    "n_source_station_resolution_rows",
    "n_cluster_resolution_rows",
    "n_records",
    "variables_used",
    "variable_treatment",
    "preferred_citation",
    "source_station_paths_sample",
    "data_limitations",
    "provenance_notes",
    "metadata_status",
]

# Values here are source-level descriptors, not release statistics. Release
# statistics are derived from source_station_catalog.csv when available.
SOURCE_REGISTRY: List[Dict[str, Any]] = [
    {
        "aliases": ["GloRiSe v1.1", "GloRiSe", "glorise_v1_1"],
        "source_long_name": "Global River Sediments database version 1.1",
        "source_category": "global",
        "release_role": "final_matrix_candidate",
        "source_version": "1.1",
        "data_doi": "10.5281/zenodo.4485795",
        "article_doi": "10.5194/essd-13-3565-2021",
        "source_url": "https://github.com/GerritMuller/GloRiSe",
        "preferred_citation": "Muller et al. (2021)",
        "variable_treatment": "Use only records carrying Q, SSC, or SSL; do not treat sediment-composition observations as Q-SSC-SSL time series.",
        "provenance_notes": "GloRiSe is primarily a sediment-composition database; verify whether it contributes to the final Q-SSC-SSL matrix before manuscript submission.",
    },
    {
        "aliases": ["GFQA_v2", "GFQA", "GEMS", "GEMS_Water", "GEMStat"],
        "source_long_name": "Global Freshwater Quality Assessment v2 / GEMS-water-derived source",
        "source_category": "global",
        "release_role": "final_matrix",
        "source_version": "v2",
        "preferred_citation": "Heinle et al. (2024)",
        "variable_treatment": "TSS is treated as SSC for harmonization; SSL is derived where paired Q and SSC/TSS are available.",
        "provenance_notes": "Manuscript station counts should be checked against this release catalog before submission.",
    },
    {
        "aliases": ["USGS NWIS", "USGS_NWIS", "NWIS", "USGS"],
        "source_long_name": "U.S. Geological Survey National Water Information System",
        "institution": "U.S. Geological Survey",
        "source_category": "national",
        "release_role": "final_matrix",
        "source_url": "https://waterdata.usgs.gov/nwis",
        "preferred_citation": "U.S. Geological Survey (2016)",
        "variable_treatment": "Direct Q, SSC, and/or SSL are retained where available; missing SSL can be derived from paired Q and SSC.",
    },
    {
        "aliases": ["HYDAT", "Water Survey of Canada"],
        "source_long_name": "HYDAT / Water Survey of Canada hydrometric database",
        "institution": "Water Survey of Canada",
        "source_category": "national",
        "release_role": "final_matrix",
        "source_url": "https://wateroffice.ec.gc.ca/",
        "preferred_citation": "Water Survey of Canada",
        "variable_treatment": "Daily hydrometric and sediment variables are unit-harmonized to release units.",
    },
    {
        "aliases": ["Bayern", "GKD Bayern", "Bayern_GKD"],
        "source_long_name": "Bavarian Hydrological Service / Gewaesserkundlicher Dienst Bayern",
        "institution": "Bavarian Hydrological Service",
        "source_category": "national",
        "release_role": "final_matrix",
        "source_url": "https://www.gkd.bayern.de/",
        "preferred_citation": "GKD Bayern",
        "variable_treatment": "Daily records are unit-harmonized; SSL is derived where needed from Q and SSC.",
    },
    {
        "aliases": ["HYBAM"],
        "source_long_name": "Observation Service HYBAM",
        "institution": "HYBAM Observatory",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "source_url": "https://hybam.obs-mip.fr/",
        "preferred_citation": "HYBAM Observatory",
        "variable_treatment": "Daily Q and sediment observations are harmonized; SSL is derived where paired Q and SSC are available.",
    },
    {
        "aliases": ["Eurasian Dataset", "Eurasian_River", "Eurasian_Arctic", "Eurasian"],
        "source_long_name": "Eurasian Arctic river sediment/discharge dataset",
        "source_category": "regional",
        "release_role": "final_matrix",
        "preferred_citation": "Holmes and Peterson (2016)",
        "variable_treatment": "Monthly Q, SSC, and SSL records are unit-harmonized.",
    },
    {
        "aliases": ["EUSEDcollab", "EUSEDcollab.v1", "EUSED"],
        "source_long_name": "European Sediments Collaboration database",
        "source_category": "regional",
        "release_role": "final_matrix",
        "source_version": "v1",
        "article_doi": "10.1038/s41597-023-02393-8",
        "preferred_citation": "Matthews et al. (2023)",
        "variable_treatment": "Daily, monthly, and event-derived Q/SSC/SSL records are harmonized to release temporal classes where appropriate.",
    },
    {
        "aliases": ["Rhine", "Rhine Basin"],
        "source_long_name": "Rhine suspended sediment / SPM-MPM dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Slabon et al. (2025)",
        "variable_treatment": "SPM/MPM measurements are treated as SSC-equivalent after source-specific harmonization.",
    },
    {
        "aliases": ["Mekong Delta", "Mekong_Delta"],
        "source_long_name": "Vietnamese Mekong Delta ADCP/sediment dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Darby et al. (2020)",
        "variable_treatment": "ADCP/source-specific Q and sediment records are harmonized; SSL is derived where needed.",
    },
    {
        "aliases": ["Myanmar Rivers", "Myanmar_Rivers", "Irrawaddy Salween"],
        "source_long_name": "Irrawaddy and Salween river sediment dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Baronas et al. (2020)",
        "variable_treatment": "Monthly or discrete observations are harmonized; SSL is derived where paired Q and SSC are available.",
    },
    {
        "aliases": ["Yajiang / Yarlung Tsangpo", "Yajiang", "Yajiang_Yarlung_Tsangpo", "Yarlung_Tsangpo"],
        "source_long_name": "Yajiang / Yarlung Tsangpo river basin dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Shi Xiaonan (2025)",
        "variable_treatment": "Daily Q and SSC records are harmonized; SSL is derived where needed.",
        "provenance_notes": "If unpublished, provide permission, source version, and data-use terms before ESSD submission.",
    },
    {
        "aliases": ["Chao Phraya River", "Chao_Phraya", "Chao Phraya"],
        "source_long_name": "Chao Phraya River annual sediment flux dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Wei Bingbing (2025)",
        "variable_treatment": "Annual Q, SSC, and/or SSL values are harmonized; SSC or SSL may be derived from paired annual variables.",
        "provenance_notes": "If unpublished, provide permission, source version, and data-use terms before ESSD submission.",
    },
    {
        "aliases": ["Robotham", "Littlestock Brook"],
        "source_long_name": "Littlestock Brook dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Robotham et al. (2022)",
        "variable_treatment": "Q values reported in L s-1 are converted to m3 s-1; SSL is derived where needed.",
    },
    {
        "aliases": ["NERC-Hampshire Avon", "NERC_Hampshire_Avon", "Hampshire Avon"],
        "source_long_name": "NERC Hampshire Avon / River Avon dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Heppell and Binley (2016)",
        "variable_treatment": "Daily Q and SSC records are harmonized; SSL is derived where needed.",
    },
    {
        "aliases": ["Fukushima", "Fukushima_Niida", "Niida River"],
        "source_long_name": "Fukushima Niida River dataset",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Bin et al. (2022)",
        "variable_treatment": "Daily Q and SSC records are harmonized; SSL is derived where needed.",
    },
    {
        "aliases": ["Shashi_Jianli", "Shashi-Jianli", "Shashi Jianli"],
        "source_long_name": "Shashi and Jianli Yangtze River stations",
        "source_category": "basin_specific",
        "release_role": "final_matrix",
        "preferred_citation": "Nones and Guo (2025)",
        "variable_treatment": "Daily Q and SSC records are harmonized; SSL is derived where needed.",
    },
    {
        "aliases": ["Huanghe", "Huanghe (Yellow River)", "Yellow River", "Huanghe_Yellow_River"],
        "source_long_name": "Yellow River / Huanghe dataset",
        "source_category": "basin_specific",
        "release_role": "climatology_or_matrix_candidate",
        "preferred_citation": "Zhang Yaonan et al. (2021)",
        "variable_treatment": "SSC-only or climatological records should not be interpreted as complete co-located Q-SSC-SSL time series unless Q/SSL are present in the release.",
    },
    {
        "aliases": ["Milliman & Farnsworth", "Milliman_Farnsworth", "Milliman and Farnsworth"],
        "source_long_name": "Global river discharge and sediment flux compilation",
        "source_category": "global_climatology",
        "release_role": "climatology_candidate",
        "preferred_citation": "Milliman and Farnsworth (2013)",
        "variable_treatment": "Long-term Q and SSL values; SSC may be derived from Q and SSL. Keep separate from time-resolved station matrix products unless explicitly exported as annual or climatology.",
    },
    {
        "aliases": ["High Mountain Asia", "HMA"],
        "source_long_name": "High Mountain Asia sediment flux compilation",
        "source_category": "regional_climatology",
        "release_role": "climatology_candidate",
        "preferred_citation": "Li et al. (2021)",
        "variable_treatment": "Long-term Q and SSL values; SSC may be derived. Best treated as climatological support.",
    },
    {
        "aliases": ["Ali & De Boer", "Ali_De_Boer", "Upper Indus"],
        "source_long_name": "Upper Indus sediment yield compilation",
        "source_category": "regional_climatology",
        "release_role": "climatology_candidate",
        "preferred_citation": "Ali and De Boer (2007)",
        "variable_treatment": "Sediment yield is converted to SSL using basin area where needed.",
    },
    {
        "aliases": ["Vanmaercke", "Vanmaercke et al.", "Vanmaercke_Africa"],
        "source_long_name": "African sediment yield synthesis",
        "source_category": "regional_climatology",
        "release_role": "climatology_candidate",
        "preferred_citation": "Vanmaercke et al. (2014)",
        "variable_treatment": "Sediment yield is converted to SSL using basin area where needed.",
    },
    {
        "aliases": ["GSED"],
        "source_long_name": "Global Suspended Sediment Dynamics",
        "source_category": "satellite_derived",
        "release_role": "satellite_candidate_or_validation_only",
        "preferred_citation": "Sun et al. (2025)",
        "variable_treatment": "Satellite-derived SSC; not treated as an ordinary gauge-equivalent source station in the minimal matrix package.",
    },
    {
        "aliases": ["Dethier", "Dethier et al."],
        "source_long_name": "Satellite-derived virtual station sediment dataset",
        "source_category": "satellite_derived",
        "release_role": "satellite_candidate_or_validation_only",
        "preferred_citation": "Dethier et al. (2022, 2023)",
        "variable_treatment": "Satellite-derived or calibrated estimates; keep provenance distinct from in-situ gauge observations.",
    },
    {
        "aliases": ["RiverSed", "RiverSed (USA)", "RiverSed_USA"],
        "source_long_name": "RiverSed USA satellite-derived suspended sediment dataset",
        "source_category": "satellite_derived",
        "release_role": "satellite_candidate_or_validation_only",
        "preferred_citation": "Gardner et al. (2021/2023)",
        "variable_treatment": "Satellite-derived SSC; not treated as an ordinary gauge-equivalent source station in the minimal matrix package.",
    },
]


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat"}:
        return ""
    return text


def normalize_key(value: Any) -> str:
    text = clean_text(value).lower()
    chars = []
    previous_sep = False
    for char in text:
        if char.isalnum():
            chars.append(char)
            previous_sep = False
        elif not previous_sep:
            chars.append("_")
            previous_sep = True
    return "".join(chars).strip("_")


def unique_join(values: Iterable[Any], sep: str = "|") -> str:
    out: List[str] = []
    seen = set()
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        for part in str(text).split("|"):
            item = clean_text(part)
            key = item.lower()
            if item and key not in seen:
                out.append(item)
                seen.add(key)
    return sep.join(out)


def first_nonempty(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def build_registry_lookup() -> Dict[str, Dict[str, Any]]:
    lookup: Dict[str, Dict[str, Any]] = {}
    for entry in SOURCE_REGISTRY:
        for alias in entry.get("aliases", []):
            key = normalize_key(alias)
            if key:
                lookup[key] = entry
    return lookup


REGISTRY_LOOKUP = build_registry_lookup()


def registry_entry(source_name: Any) -> Mapping[str, Any]:
    key = normalize_key(source_name)
    if key in REGISTRY_LOOKUP:
        return REGISTRY_LOOKUP[key]
    compact_key = key.replace("_", "")
    for alias_key, entry in REGISTRY_LOOKUP.items():
        if compact_key and compact_key == alias_key.replace("_", ""):
            return entry
    return {}


def read_catalog(path: Path, required: bool = True) -> pd.DataFrame:
    if not path.is_file():
        if required:
            raise FileNotFoundError(path)
        return pd.DataFrame()
    return pd.read_csv(path, keep_default_na=False)


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = ""
    return out


def min_date_text(values: Iterable[Any]) -> str:
    cleaned = [clean_text(v) for v in values if clean_text(v)]
    if not cleaned:
        return ""
    parsed = pd.to_datetime(cleaned, errors="coerce")
    valid = parsed[~pd.isna(parsed)]
    if len(valid) == 0:
        return sorted(cleaned)[0]
    return pd.Timestamp(valid.min()).strftime("%Y-%m-%d")


def max_date_text(values: Iterable[Any]) -> str:
    cleaned = [clean_text(v) for v in values if clean_text(v)]
    if not cleaned:
        return ""
    parsed = pd.to_datetime(cleaned, errors="coerce")
    valid = parsed[~pd.isna(parsed)]
    if len(valid) == 0:
        return sorted(cleaned)[-1]
    return pd.Timestamp(valid.max()).strftime("%Y-%m-%d")


def count_unique_nonempty(values: Iterable[Any]) -> int:
    return len({clean_text(v) for v in values if clean_text(v)})


def sample_paths(values: Iterable[Any], max_items: int = 5) -> str:
    paths: List[str] = []
    seen = set()
    for value in values:
        for part in clean_text(value).split("|"):
            item = clean_text(part)
            key = item.lower()
            if item and key not in seen:
                paths.append(item)
                seen.add(key)
            if len(paths) >= max_items:
                return "|".join(paths)
    return "|".join(paths)


def aggregate_source_station_catalog(source_station: pd.DataFrame) -> pd.DataFrame:
    if source_station.empty or "source_name" not in source_station.columns:
        return pd.DataFrame(columns=["source_name"])

    work = ensure_columns(
        source_station,
        [
            "source_name",
            "resolution",
            "time_start",
            "time_end",
            "n_records",
            "source_station_uid",
            "cluster_uid",
            "source_station_variables_provided",
            "source_station_data_limitations",
            "source_station_declared_temporal_resolution",
            "source_station_paths",
        ],
    )
    work["n_records"] = pd.to_numeric(work["n_records"], errors="coerce").fillna(0).astype(int)

    rows: List[Dict[str, Any]] = []
    for source_name, group in work.groupby("source_name", dropna=False, sort=False):
        time_start = min_date_text(group["time_start"])
        time_end = max_date_text(group["time_end"])
        resolutions = unique_join(sorted(set(clean_text(v).lower() for v in group["resolution"] if clean_text(v))))
        declared = unique_join(group["source_station_declared_temporal_resolution"])
        rows.append(
            {
                "source_name": source_name,
                "available_resolutions": resolutions,
                "temporal_resolution_used": first_nonempty(resolutions, declared),
                "time_start": time_start,
                "time_end": time_end,
                "temporal_span_used": "{}-{}".format(time_start, time_end) if time_start or time_end else "",
                "n_source_stations": count_unique_nonempty(group["source_station_uid"]),
                "n_clusters": count_unique_nonempty(group["cluster_uid"]),
                "n_source_station_resolution_rows": len(group),
                "n_cluster_resolution_rows": len(group.drop_duplicates(subset=["cluster_uid", "resolution"])),
                "n_records": int(group["n_records"].sum()),
                "variables_used": unique_join(group["source_station_variables_provided"]),
                "data_limitations": unique_join(group["source_station_data_limitations"]),
                "source_station_paths_sample": sample_paths(group["source_station_paths"]),
            }
        )
    return pd.DataFrame(rows)


def infer_release_role(row: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    available = {item.strip().lower() for item in clean_text(row.get("available_resolutions", "")).split("|") if item.strip()}
    registry_role = clean_text(entry.get("release_role", ""))
    if available & MINIMAL_MATRIX_RESOLUTIONS:
        if registry_role.endswith("_candidate") or registry_role in {"climatology_or_matrix_candidate"}:
            return "final_matrix"
        if registry_role.startswith("satellite"):
            return registry_role
        return registry_role or "final_matrix"
    return registry_role or "source_dataset"


def included_in_minimal_release(row: Mapping[str, Any]) -> str:
    role = clean_text(row.get("release_role", "")).lower()
    available = {item.strip().lower() for item in clean_text(row.get("available_resolutions", "")).split("|") if item.strip()}
    return "true" if role == "final_matrix" and bool(available & MINIMAL_MATRIX_RESOLUTIONS) else "false"


def fill_registry_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    registry_fields = [
        "source_long_name",
        "institution",
        "source_category",
        "release_role",
        "source_version",
        "data_doi",
        "article_doi",
        "source_url",
        "license_or_terms",
        "preferred_citation",
        "variable_treatment",
        "provenance_notes",
    ]
    out = ensure_columns(out, registry_fields)
    for idx, row in out.iterrows():
        entry = registry_entry(row.get("source_name", ""))
        for field in registry_fields:
            out.at[idx, field] = first_nonempty(row.get(field, ""), entry.get(field, ""))
        out.at[idx, "release_role"] = infer_release_role(out.loc[idx], entry)
        out.at[idx, "included_in_minimal_release"] = included_in_minimal_release(out.loc[idx])
        if not clean_text(out.at[idx, "preferred_citation"]):
            out.at[idx, "preferred_citation"] = first_nonempty(row.get("reference", ""), row.get("source_name", ""))
    return out


def merge_preserving_existing(base: pd.DataFrame, derived: pd.DataFrame) -> pd.DataFrame:
    if derived.empty or "source_name" not in derived.columns:
        return base
    if base.empty:
        return derived.copy()
    merged = base.merge(derived, on="source_name", how="outer", suffixes=("", "__derived"))
    for column in list(derived.columns):
        if column == "source_name":
            continue
        derived_column = "{}__derived".format(column)
        if derived_column not in merged.columns:
            continue
        if column not in merged.columns:
            merged[column] = merged[derived_column]
        else:
            merged[column] = merged[column].where(
                merged[column].astype(str).str.strip().ne(""), merged[derived_column]
            )
        merged = merged.drop(columns=[derived_column])
    return merged


def add_metadata_status(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    required_for_submission = ["data_doi", "license_or_terms", "access_date", "preferred_citation"]
    statuses = []
    for _, row in out.iterrows():
        missing = [field for field in required_for_submission if not clean_text(row.get(field, ""))]
        if missing:
            statuses.append("needs_review:missing_{}".format("|".join(missing)))
        else:
            statuses.append("complete_for_submission")
    out["metadata_status"] = statuses
    return out


def enrich_source_dataset_catalog(source_dataset: pd.DataFrame, source_station: pd.DataFrame) -> pd.DataFrame:
    base = source_dataset.copy()
    if base.empty and not source_station.empty and "source_name" in source_station.columns:
        base = pd.DataFrame({"source_name": sorted(source_station["source_name"].astype(str).unique())})

    base = ensure_columns(base, ["source_name", "source_long_name", "institution", "reference", "source_url"])
    derived = aggregate_source_station_catalog(source_station)
    out = merge_preserving_existing(base, derived)
    out = ensure_columns(out, OUTPUT_COLUMNS)
    out = fill_registry_fields(out)
    out = add_metadata_status(out)
    out = out.loc[:, OUTPUT_COLUMNS]
    out = out.sort_values("source_name", kind="mergesort").reset_index(drop=True)
    return out


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR), help="S8 release directory")
    parser.add_argument("--source-dataset-catalog", default=None, help="Input source_dataset_catalog.csv")
    parser.add_argument("--source-station-catalog", default=None, help="Input source_station_catalog.csv")
    parser.add_argument("--out", default=None, help="Output CSV path; defaults to source_dataset_catalog_enriched.csv")
    parser.add_argument("--in-place", action="store_true", help="Overwrite source_dataset_catalog.csv")
    parser.add_argument("--backup", action="store_true", help="Create .bak before in-place overwrite")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    release_dir = Path(args.release_dir).expanduser().resolve()
    source_dataset_path = Path(args.source_dataset_catalog).expanduser().resolve() if args.source_dataset_catalog else release_dir / "source_dataset_catalog.csv"
    source_station_path = Path(args.source_station_catalog).expanduser().resolve() if args.source_station_catalog else release_dir / "source_station_catalog.csv"

    source_dataset = read_catalog(source_dataset_path, required=source_station_path.is_file())
    source_station = read_catalog(source_station_path, required=False)
    enriched = enrich_source_dataset_catalog(source_dataset, source_station)

    if args.in_place:
        out_path = source_dataset_path
        if args.backup and source_dataset_path.is_file():
            backup_path = source_dataset_path.with_suffix(source_dataset_path.suffix + ".bak")
            shutil.copy2(source_dataset_path, backup_path)
            print("[backup] {}".format(backup_path))
    else:
        out_path = Path(args.out).expanduser().resolve() if args.out else source_dataset_path.with_name("source_dataset_catalog_enriched.csv")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out_path, index=False)
    print("[write] {}".format(out_path))
    print("[summary] sources={} complete_for_submission={}".format(
        len(enriched),
        int(enriched["metadata_status"].eq("complete_for_submission").sum()),
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
