#!/usr/bin/env python3
"""Count major source datasets by continent using the top-source map inputs.

The script reuses the release inputs used by plot_top_sources_map_full.py:
source_station_catalog.csv, source_dataset_catalog.csv, and
sed_reference_climatology.nc. Source-station CSV rows get continent metadata
from the existing cluster spatial attributes table; climatology NC rows use
their own promoted continent_region variable.
"""

import argparse
from pathlib import Path
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent.parent
DEFAULT_RELEASE_DIR = PROJECT_DIR / "output" / "sed_reference_release_minimal"
DEFAULT_CLUSTER_SPATIAL_TABLE = (
    PROJECT_DIR / "output_other" / "spatial_coverage_stats" / "tables" / "table_cluster_spatial_attributes.csv"
)
DEFAULT_OUT_DIR = PROJECT_DIR / "figures" / "data" / "source_dataset_continent_stats"

SOURCE_STATION_CSV = "source_station_catalog.csv"
SOURCE_DATASET_CSV = "source_dataset_catalog.csv"
CLIMATOLOGY_NC = "sed_reference_climatology.nc"

SATELLITE_DATASETS = {"Dethier", "GSED", "RiverSed (USA)"}
UNKNOWN_CONTINENT = "Unknown"
MISSING_GEO_VALUES = {"", "unknown", "unresolved", "not available", "n/a", "na"}

SOURCE_CONTINENT_DEFAULTS = {
    "Vanmaercke et al.": "Africa",
}

RESOLUTION_FLAG_MEANINGS = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}


def parse_resolution(value: object) -> str:
    """Normalize a resolution flag (0-4) or text label to a canonical string."""
    text = clean_text(value)
    if not text:
        return "other"
    # Handle integer flags that may be stored as numeric or text
    try:
        flag = int(text)
        return RESOLUTION_FLAG_MEANINGS.get(flag, "other")
    except (ValueError, TypeError):
        pass
    # Handle already-canonical text labels
    lower = text.casefold()
    for label in ("daily", "monthly", "annual", "climatology"):
        if label == lower:
            return label
    return "other"


COUNTRY_CONTINENT_DEFAULTS = {
    "Brazil": "South America",
    "Canada": "North America",
    "China": "Asia",
    "France": "Europe",
    "Greenland": "North America",
    "India": "Asia",
    "Portugal": "Europe",
    "USA": "North America",
    "United States": "North America",
    "United States of America": "North America",
    "Vietnam": "Asia",
}

SOURCE_NAME_ALIASES = {
    "ALi_De_Boer": "Ali & De Boer (Upper Indus)",
    "Ali and De Boer": "Ali & De Boer (Upper Indus)",
    "HMA": "High Mountain Asia (HMA)",
    "Milliman": "Milliman & Farnsworth",
    "Vanmaercke": "Vanmaercke et al.",
    "RiverSed": "RiverSed (USA)",
    "USGS": "USGS NWIS",
    "Eurasian_River": "Eurasian Dataset",
    "Eurasian River": "Eurasian Dataset",
    "GloRiSe": "GloRiSe v1.1",
    "Huanghe": "Huanghe (Yellow River)",
    "Myanmar": "Myanmar Rivers",
    "NERC": "NERC-Hampshire Avon",
    "NERC Avon": "NERC-Hampshire Avon",
    "Yajiang": "Yajiang / Yarlung Tsangpo",
    "Chao_Phraya_River": "Chao Phraya River",
    "Chao Phraya": "Chao Phraya River",
    "Mekong_Delta": "Mekong Delta",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count the main in-situ source datasets represented in each continent."
    )
    parser.add_argument("--release-dir", type=Path, default=DEFAULT_RELEASE_DIR)
    parser.add_argument("--cluster-spatial-table", type=Path, default=DEFAULT_CLUSTER_SPATIAL_TABLE)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--top-n", type=int, default=10, help="Number of ranked source datasets to report per continent.")
    parser.add_argument(
        "--continent-level",
        choices=("continent", "continent_region"),
        default="continent",
        help="Group by top-level continent or by the detailed continent_region label.",
    )
    parser.add_argument(
        "--include-satellite",
        action="store_true",
        help="Keep satellite source names if they are present in the map input tables.",
    )
    return parser.parse_args()


def clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if np.ma.is_masked(value):
            return ""
    except Exception:
        pass
    if isinstance(value, np.ma.MaskedArray):
        if value.size == 0 or bool(np.ma.getmaskarray(value).all()):
            return ""
        value = value.filled("")
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            value = value.item()
        elif value.dtype.kind in {"S", "U"}:
            value = b"".join(value.astype("S").tolist()).decode("utf-8", errors="ignore")
        else:
            value = value.tolist()
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip().strip("\x00")
    if text.startswith("b'") and text.endswith("'"):
        text = text[2:-1]
    if text.startswith('b"') and text.endswith('"'):
        text = text[2:-1]
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def clean_geo_text(value: object) -> str:
    text = clean_text(value)
    return "" if text.casefold() in MISSING_GEO_VALUES else text


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise FileNotFoundError("Required input not found: {}".format(path))
    return path


def canonical_source_name(name: object, catalog_names: Iterable[str]) -> str:
    text = clean_text(name)
    if not text:
        return ""

    catalog_list = [clean_text(value) for value in catalog_names]
    catalog_set = set(catalog_list)
    if text in catalog_set:
        return text

    alias = SOURCE_NAME_ALIASES.get(text)
    if alias:
        return alias

    compact = text.replace("_", " ").strip()
    if compact in catalog_set:
        return compact

    casefold_lookup = {value.casefold(): value for value in catalog_list}
    if text.casefold() in casefold_lookup:
        return casefold_lookup[text.casefold()]
    if compact.casefold() in casefold_lookup:
        return casefold_lookup[compact.casefold()]
    return text


def top_level_continent(continent_region: object) -> str:
    text = clean_geo_text(continent_region)
    if not text:
        return ""
    return clean_geo_text(text.split(",", 1)[0])


def join_values(values: pd.Series) -> str:
    items = sorted({clean_text(value) for value in values if clean_text(value)})
    return "|".join(items)


def sum_available(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().any():
        return float(numeric.sum())
    return np.nan


def country_from_station_id(value: object) -> str:
    text = clean_text(value)
    if "-" not in text:
        return ""
    prefix = text.split("-", 1)[0].strip().upper()
    return "USA" if prefix == "USA" else ""


def read_dataset_catalog(release_dir: Path) -> pd.DataFrame:
    path = require_file(release_dir / SOURCE_DATASET_CSV)
    catalog = pd.read_csv(path)
    required = {"source_name", "n_source_stations", "n_records"}
    missing = sorted(required.difference(catalog.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(path, ", ".join(missing)))
    catalog = catalog.copy()
    catalog["source_name"] = catalog["source_name"].map(clean_text)
    catalog["n_source_stations"] = pd.to_numeric(catalog["n_source_stations"], errors="coerce").fillna(0).astype(int)
    catalog["n_records"] = pd.to_numeric(catalog["n_records"], errors="coerce").fillna(0).astype(int)
    return catalog[catalog["source_name"].ne("")].copy()


def read_cluster_spatial_table(path: Path) -> pd.DataFrame:
    path = require_file(path)
    frame = pd.read_csv(path)
    if "cluster_uid" not in frame.columns and "cluster_key" not in frame.columns:
        raise ValueError("{} must include cluster_uid or cluster_key".format(path))
    out = frame.copy()
    if "cluster_uid" in out.columns:
        out["_cluster_key"] = out["cluster_uid"].map(clean_text)
    else:
        out["_cluster_key"] = out["cluster_key"].map(clean_text)
    if "cluster_key" in out.columns:
        out["_cluster_key"] = out["_cluster_key"].where(out["_cluster_key"].ne(""), out["cluster_key"].map(clean_text))

    for col in ("continent", "region", "country", "geographic_coverage"):
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].map(clean_geo_text)

    out["country"] = out["country"].where(out["country"].ne(""), out["geographic_coverage"])
    out["continent_region_raw"] = out["continent"].where(out["continent"].ne(""), out["region"])
    mapped_continent = out["country"].map(COUNTRY_CONTINENT_DEFAULTS).fillna("")
    out["continent_region_raw"] = out["continent_region_raw"].where(out["continent_region_raw"].ne(""), mapped_continent)
    return out[out["_cluster_key"].ne("")][["_cluster_key", "country", "continent_region_raw"]].drop_duplicates("_cluster_key")


def read_source_station_points(
    release_dir: Path,
    catalog_names: Iterable[str],
    cluster_geo: pd.DataFrame,
) -> pd.DataFrame:
    path = require_file(release_dir / SOURCE_STATION_CSV)
    frame = pd.read_csv(path)
    required = {"source_name", "source_station_lat", "source_station_lon", "cluster_uid", "n_records", "resolution"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(path, ", ".join(missing)))

    out = pd.DataFrame(
        {
            "source_name_raw": frame["source_name"].map(clean_text),
            "source_name": frame["source_name"].map(lambda value: canonical_source_name(value, catalog_names)),
            "source_station_uid": frame.get("source_station_uid", pd.Series(index=frame.index, dtype=object)).map(clean_text),
            "source_station_native_id": frame.get(
                "source_station_native_id", pd.Series(index=frame.index, dtype=object)
            ).map(clean_text),
            "cluster_uid": frame["cluster_uid"].map(clean_text),
            "lat": pd.to_numeric(frame["source_station_lat"], errors="coerce"),
            "lon": pd.to_numeric(frame["source_station_lon"], errors="coerce"),
            "record_count_available": pd.to_numeric(frame["n_records"], errors="coerce"),
            "resolution": frame["resolution"].map(parse_resolution),
            "input_file": SOURCE_STATION_CSV,
        }
    )
    out["source_station_uid"] = out["source_station_uid"].where(
        out["source_station_uid"].ne(""), "source_station_catalog_row_" + out.index.astype(str)
    )
    out["_cluster_key"] = out["cluster_uid"]
    out = out.merge(cluster_geo, on="_cluster_key", how="left")
    out["country"] = out.get("country", "").map(clean_geo_text)
    station_id_country = out["source_station_native_id"].map(country_from_station_id)
    out["country"] = out["country"].where(out["country"].ne(""), station_id_country)
    out["continent_region_raw"] = out.get("continent_region_raw", "").map(clean_geo_text)
    station_id_continent = out["country"].map(COUNTRY_CONTINENT_DEFAULTS).fillna("")
    out["continent_region_raw"] = out["continent_region_raw"].where(out["continent_region_raw"].ne(""), station_id_continent)
    return out.drop(columns=["_cluster_key"])


def read_nc_text_variable(ds, name: str) -> List[str]:
    if name not in ds.variables:
        raise ValueError("{} is missing variable: {}".format(CLIMATOLOGY_NC, name))
    return [clean_text(value) for value in ds.variables[name][:]]


def read_climatology_points(release_dir: Path, catalog_names: Iterable[str]) -> pd.DataFrame:
    try:
        import netCDF4 as nc4
    except ImportError as exc:
        raise ImportError("netCDF4 is required to read {}".format(CLIMATOLOGY_NC)) from exc

    path = require_file(release_dir / CLIMATOLOGY_NC)
    with nc4.Dataset(str(path), "r") as ds:
        for var_name in ("lat", "lon", "source_index", "source_name", "resolution", "continent_region"):
            if var_name not in ds.variables:
                raise ValueError("{} is missing variable: {}".format(path, var_name))

        lat = np.asarray(ds.variables["lat"][:], dtype="float64")
        lon = np.asarray(ds.variables["lon"][:], dtype="float64")
        source_index = np.asarray(ds.variables["source_index"][:], dtype="float64")
        source_names = [canonical_source_name(value, catalog_names) for value in ds.variables["source_name"][:]]
        continent_region = read_nc_text_variable(ds, "continent_region")
        resolution_flags = list(ds.variables["resolution"][:])
        country = read_nc_text_variable(ds, "country") if "country" in ds.variables else [""] * len(lat)
        station_uid = read_nc_text_variable(ds, "station_uid") if "station_uid" in ds.variables else [""] * len(lat)
        source_station_id = (
            read_nc_text_variable(ds, "source_station_id") if "source_station_id" in ds.variables else [""] * len(lat)
        )

    if not (len(lat) == len(lon) == len(source_index) == len(continent_region) == len(resolution_flags)):
        raise ValueError("{} has inconsistent station variable lengths".format(path))

    valid_source = np.isfinite(source_index) & (source_index >= 0) & (source_index < len(source_names))
    source_labels = np.array([""] * len(source_index), dtype=object)
    source_labels[valid_source] = [source_names[int(idx)] for idx in source_index[valid_source]]

    out = pd.DataFrame(
        {
            "source_name_raw": source_labels,
            "source_name": source_labels,
            "source_station_uid": station_uid,
            "source_station_native_id": source_station_id,
            "cluster_uid": "",
            "lat": lat,
            "lon": lon,
            "resolution": [parse_resolution(v) for v in resolution_flags],
            "country": country,
            "continent_region_raw": continent_region,
            "record_count_available": np.nan,
            "input_file": CLIMATOLOGY_NC,
        }
    )
    out["source_station_uid"] = out["source_station_uid"].map(clean_text).where(
        out["source_station_uid"].map(clean_text).ne(""), "climatology_row_" + out.index.astype(str)
    )
    return out


def valid_latlon(frame: pd.DataFrame) -> pd.Series:
    lat = pd.to_numeric(frame["lat"], errors="coerce")
    lon = pd.to_numeric(frame["lon"], errors="coerce")
    return lat.between(-90, 90) & lon.between(-180, 180)


def fill_missing_continents(points: pd.DataFrame) -> pd.DataFrame:
    out = points.copy()
    out["continent_region_raw"] = out["continent_region_raw"].map(clean_geo_text)
    out["continent_fill_method"] = np.where(out["continent_region_raw"].ne(""), "direct", "")
    out["_continent_candidate"] = out["continent_region_raw"].map(top_level_continent)

    source_to_continent = {}
    for source_name, group in out.groupby("source_name", dropna=False):
        continents = sorted({clean_geo_text(value) for value in group["_continent_candidate"] if clean_geo_text(value)})
        if len(continents) == 1:
            source_to_continent[clean_text(source_name)] = continents[0]

    missing = out["continent_region_raw"].eq("")
    inferred = missing & out["source_name"].map(source_to_continent).fillna("").ne("")
    out.loc[inferred, "continent_region_raw"] = out.loc[inferred, "source_name"].map(source_to_continent)
    out.loc[inferred, "continent_fill_method"] = "source_unique_continent"

    missing = out["continent_region_raw"].eq("")
    fallback = missing & out["source_name"].isin(SOURCE_CONTINENT_DEFAULTS)
    out.loc[fallback, "continent_region_raw"] = out.loc[fallback, "source_name"].map(SOURCE_CONTINENT_DEFAULTS)
    out.loc[fallback, "continent_fill_method"] = "source_default"

    out = out.drop(columns=["_continent_candidate"])
    return out


def load_points(
    release_dir: Path,
    catalog: pd.DataFrame,
    cluster_spatial_table: Path,
    include_satellite: bool,
) -> pd.DataFrame:
    catalog_names = catalog["source_name"].tolist()
    cluster_geo = read_cluster_spatial_table(cluster_spatial_table)
    frames = [
        read_source_station_points(release_dir, catalog_names, cluster_geo),
        read_climatology_points(release_dir, catalog_names),
    ]
    points = pd.concat(frames, ignore_index=True, sort=False)
    points = points[points["source_name"].map(clean_text).ne("") & valid_latlon(points)].copy()
    if not include_satellite:
        points = points[~points["source_name"].isin(SATELLITE_DATASETS)].copy()

    points = fill_missing_continents(points)
    points["continent"] = points["continent_region_raw"].map(top_level_continent)
    points["continent_resolved"] = points["continent"].ne("")
    points["continent"] = points["continent"].where(points["continent_resolved"], UNKNOWN_CONTINENT)
    points["source_station_key"] = points["input_file"].map(clean_text) + ":" + points["source_station_uid"].map(clean_text)
    return points.reset_index(drop=True)


def build_summary(points: pd.DataFrame, continent_level: str) -> pd.DataFrame:
    group_col = "continent" if continent_level == "continent" else "continent_region_raw"
    work = points.copy()
    work["continent_group"] = work[group_col].map(clean_text).replace("", UNKNOWN_CONTINENT)

    rows = []
    for (continent_group, source_name), group in work.groupby(["continent_group", "source_name"], dropna=False):
        rows.append(
            {
                "continent_level": continent_level,
                "continent_group": clean_text(continent_group) or UNKNOWN_CONTINENT,
                "source_name": clean_text(source_name) or "Unknown",
                "source_station_count": int(group["source_station_key"].nunique()),
                "point_rows": int(len(group)),
                "record_count_available": sum_available(group["record_count_available"]),
                "input_files": join_values(group["input_file"]),
                "continent_region_values": join_values(group["continent_region_raw"]),
            }
        )

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    totals = summary.groupby("continent_group")["source_station_count"].sum().rename("_continent_total")
    summary = summary.merge(totals, on="continent_group", how="left")
    summary["percent_of_continent_stations"] = (
        summary["source_station_count"] / summary["_continent_total"].where(summary["_continent_total"].ne(0), np.nan) * 100
    )
    summary["_record_sort"] = pd.to_numeric(summary["record_count_available"], errors="coerce").fillna(0)
    summary = summary.sort_values(
        ["continent_group", "source_station_count", "_record_sort", "source_name"],
        ascending=[True, False, False, True],
    )
    summary["rank_in_continent"] = summary.groupby("continent_group").cumcount() + 1
    return summary.drop(columns=["_continent_total", "_record_sort"]).reset_index(drop=True)


def build_resolution_summary(points: pd.DataFrame, continent_level: str) -> pd.DataFrame:
    """Count resolution types per continent."""
    group_col = "continent" if continent_level == "continent" else "continent_region_raw"
    work = points.copy()
    work["continent_group"] = work[group_col].map(clean_text).replace("", UNKNOWN_CONTINENT)

    rows = []
    for (continent_group, resolution), group in work.groupby(["continent_group", "resolution"], dropna=False):
        rows.append(
            {
                "continent_level": continent_level,
                "continent_group": clean_text(continent_group) or UNKNOWN_CONTINENT,
                "resolution": clean_text(resolution) or "other",
                "source_station_count": int(group["source_station_key"].nunique()),
                "point_rows": int(len(group)),
            }
        )

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    totals = summary.groupby("continent_group")["source_station_count"].sum().rename("_continent_total")
    summary = summary.merge(totals, on="continent_group", how="left")
    summary["percent_of_continent_stations"] = (
        summary["source_station_count"] / summary["_continent_total"].where(summary["_continent_total"].ne(0), np.nan) * 100
    )
    resolution_order = ["daily", "monthly", "annual", "climatology", "other"]
    summary["_resolution_rank"] = summary["resolution"].map(
        {label: i for i, label in enumerate(resolution_order)}
    ).fillna(99)
    summary = summary.sort_values(
        ["continent_group", "_resolution_rank"],
        ascending=[True, True],
    )
    return summary.drop(columns=["_continent_total", "_resolution_rank"]).reset_index(drop=True)


def validate_source_counts(points: pd.DataFrame, catalog: pd.DataFrame, include_satellite: bool) -> None:
    expected = catalog.copy()
    if not include_satellite:
        expected = expected[~expected["source_name"].isin(SATELLITE_DATASETS)].copy()
    else:
        expected = expected[expected["source_name"].isin(points["source_name"].unique())].copy()

    observed = points.groupby("source_name")["source_station_key"].nunique().rename("observed_source_stations")
    check = expected[["source_name", "n_source_stations"]].merge(observed, on="source_name", how="left")
    check["observed_source_stations"] = check["observed_source_stations"].fillna(0).astype(int)
    mismatches = check[check["observed_source_stations"].ne(check["n_source_stations"])]
    if not mismatches.empty:
        raise ValueError(
            "Source station count validation failed:\n{}".format(
                mismatches.to_string(index=False)
            )
        )
    print(
        "Source station count validation: OK ({:,} stations)".format(
            int(check["observed_source_stations"].sum())
        )
    )


def write_report(
    path: Path,
    points: pd.DataFrame,
    summary: pd.DataFrame,
    top: pd.DataFrame,
    unresolved: pd.DataFrame,
    args: argparse.Namespace,
    resolution_summary: pd.DataFrame = None,
) -> None:
    lines = [
        "# Source Datasets by Continent",
        "",
        "Inputs:",
        "- release_dir: `{}`".format(args.release_dir.resolve()),
        "- cluster_spatial_table: `{}`".format(args.cluster_spatial_table.resolve()),
        "- continent_level: `{}`".format(args.continent_level),
        "- include_satellite: `{}`".format(bool(args.include_satellite)),
        "",
        "Summary:",
        "- valid point rows: {:,}".format(len(points)),
        "- source stations counted: {:,}".format(points["source_station_key"].nunique()),
        "- source datasets counted: {:,}".format(points["source_name"].nunique()),
        "- unresolved continent rows: {:,}".format(len(unresolved)),
        "",
    ]
    if not unresolved.empty:
        lines.append("Warning: unresolved continent rows are listed in `source_dataset_by_continent_unresolved.csv`.")
        lines.append("")

    lines.extend(["Top datasets by continent:", ""])
    for continent, group in top.groupby("continent_group", sort=True):
        lines.append("## {}".format(continent))
        lines.append("")
        lines.append("| rank | source_name | source_station_count | percent | record_count_available |")
        lines.append("| ---: | --- | ---: | ---: | ---: |")
        for _, row in group.iterrows():
            record_value = row["record_count_available"]
            record_text = "" if pd.isna(record_value) else "{:,.0f}".format(float(record_value))
            lines.append(
                "| {rank} | {source} | {stations:,} | {percent:.2f} | {records} |".format(
                    rank=int(row["rank_in_continent"]),
                    source=row["source_name"],
                    stations=int(row["source_station_count"]),
                    percent=float(row["percent_of_continent_stations"]),
                    records=record_text,
                )
            )
        lines.append("")

    # --- Resolution by continent section ---
    if resolution_summary is not None and not resolution_summary.empty:
        lines.extend(["", "## Resolution by Continent", ""])
        lines.append("| continent | resolution | source_station_count | percent_of_continent |")
        lines.append("| --- | --- | ---: | ---: |")
        for _, row in resolution_summary.iterrows():
            lines.append(
                "| {continent} | {resolution} | {stations:,} | {percent:.2f} |".format(
                    continent=row["continent_group"],
                    resolution=row["resolution"],
                    stations=int(row["source_station_count"]),
                    percent=float(row["percent_of_continent_stations"]),
                )
            )
        lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def write_outputs(points: pd.DataFrame, summary: pd.DataFrame, args: argparse.Namespace, resolution_summary: pd.DataFrame = None) -> Dict[str, Path]:
    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    top = summary[summary["rank_in_continent"].le(args.top_n)].copy()
    unresolved = points[~points["continent_resolved"]].copy()

    paths = {
        "points": out_dir / "source_dataset_by_continent_points.csv",
        "summary": out_dir / "source_dataset_by_continent_summary.csv",
        "top": out_dir / "source_dataset_by_continent_topN.csv",
        "unresolved": out_dir / "source_dataset_by_continent_unresolved.csv",
        "report": out_dir / "source_dataset_by_continent_report.md",
        "resolution_summary": out_dir / "resolution_by_continent_summary.csv",
        "resolution_pivot": out_dir / "resolution_by_continent_pivot.csv",
    }

    point_cols = [
        "source_station_key",
        "source_station_uid",
        "source_station_native_id",
        "source_name",
        "source_name_raw",
        "input_file",
        "cluster_uid",
        "lat",
        "lon",
        "country",
        "continent",
        "continent_region_raw",
        "continent_resolved",
        "continent_fill_method",
        "record_count_available",
    ]
    points[[col for col in point_cols if col in points.columns]].to_csv(paths["points"], index=False)
    summary.to_csv(paths["summary"], index=False)
    top.to_csv(paths["top"], index=False)
    unresolved[[col for col in point_cols if col in unresolved.columns]].to_csv(paths["unresolved"], index=False)

    if resolution_summary is not None and not resolution_summary.empty:
        resolution_summary.to_csv(paths["resolution_summary"], index=False)
        # Build pivot: rows=continents, columns=resolutions, values=station_count
        pivot = resolution_summary.pivot_table(
            index="continent_group", columns="resolution", values="source_station_count", aggfunc="sum", fill_value=0
        )
        pivot.to_csv(paths["resolution_pivot"])
    else:
        # Write empty placeholders
        pd.DataFrame().to_csv(paths["resolution_summary"], index=False)
        pd.DataFrame().to_csv(paths["resolution_pivot"], index=False)

    write_report(paths["report"], points, summary, top, unresolved, args, resolution_summary=resolution_summary)
    return paths


def main() -> int:
    args = parse_args()
    if args.top_n <= 0:
        raise ValueError("--top-n must be positive")

    catalog = read_dataset_catalog(args.release_dir)
    points = load_points(args.release_dir, catalog, args.cluster_spatial_table, args.include_satellite)
    validate_source_counts(points, catalog, args.include_satellite)

    summary = build_summary(points, args.continent_level)
    resolution_summary = build_resolution_summary(points, args.continent_level)
    paths = write_outputs(points, summary, args, resolution_summary=resolution_summary)

    unresolved_count = int((~points["continent_resolved"]).sum())
    if unresolved_count:
        print("Warning: {:,} rows have unresolved continent metadata.".format(unresolved_count))

    print("Wrote continent source-dataset statistics:")
    for path in paths.values():
        print("  {}".format(path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
