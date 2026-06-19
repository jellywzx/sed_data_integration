#!/usr/bin/env python3
"""Organize the manuscript source table into minimal and side-release sheets."""

import re
import importlib.util
from pathlib import Path

import pandas as pd
import xarray as xr

# Import fill logic to use in-memory (no intermediate CSV files)
from fill_manuscript_source_table import (
    fill_manuscript_dataframe,
    summarize_minimal_source_stats,
    read_minimal_source_dataset_catalog,
    resolve_existing_path,
    read_csv_text,
    clean_text,
    normalize_key,
)


BASE_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output")
MINIMAL_DIR = BASE_DIR / "sed_reference_release_minimal"
CLIMATOLOGY_DIR = BASE_DIR / "sed_reference_release_climatology"
SATELLITE_DIR = BASE_DIR / "sed_reference_release_satellite"
DOCS_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/docs")

RAW_MANUSCRIPT_CSV = DOCS_DIR / "manuscript_source_table_cleaned.csv"
SOURCE_STATION_CSV = MINIMAL_DIR / "source_station_catalog.csv"
SOURCE_DATASET_CSV = MINIMAL_DIR / "source_dataset_catalog.csv"
SATELLITE_CATALOG_CSV = SATELLITE_DIR / "satellite_catalog.csv"

OUT_XLSX = DOCS_DIR / "manuscript_source_table_cleaned_minimal_filled_organized.xlsx"


NC_BY_RESOLUTION = {
    "daily": MINIMAL_DIR / "sed_reference_timeseries_daily.nc",
    "monthly": MINIMAL_DIR / "sed_reference_timeseries_monthly.nc",
    "annual": MINIMAL_DIR / "sed_reference_timeseries_annual.nc",
}

SIDE_ALIASES = {
    "GloRiSe v1.1": ["GloRiSe", "glorise_v1_1"],
    "GFQA_v2": ["GFQA_v2", "GFQA"],
    "Milliman & Farnsworth": ["Milliman"],
    "High Mountain Asia (HMA)": ["HMA"],
    "Ali & De Boer (Upper Indus)": ["ALi_De_Boer", "Ali_De_Boer"],
    "Vanmaercke et al.": ["Vanmaercke"],
    "Huanghe (Yellow River)": ["Huanghe"],
    "GSED": ["GSED"],
    "Dethier": ["Dethier"],
    "RiverSed (USA)": ["RiverSed"],
}

MINIMAL_ALIASES = {
    "GloRiSe v1.1": ["GloRiSe", "glorise_v1_1"],
    "GFQA_v2": ["GFQA_v2", "GFQA"],
    "Milliman & Farnsworth": ["Milliman"],
    "USGS NWIS": ["USGS"],
    "HYDAT": ["HYDAT"],
    "Bayern": ["Bayern"],
    "Eurasian Dataset": ["Eurasian_River"],
    "EUSEDcollab": ["EUSEDcollab"],
    "High Mountain Asia (HMA)": ["HMA"],
    "Ali & De Boer (Upper Indus)": ["ALi_De_Boer", "Ali_De_Boer"],
    "Vanmaercke et al.": ["Vanmaercke"],
    "HYBAM": ["HYBAM"],
    "Rhine": ["Rhine"],
    "Mekong Delta": ["Mekong_Delta"],
    "Myanmar Rivers": ["Myanmar"],
    "Yajiang / Yarlung Tsangpo": ["Yajiang"],
    "Chao Phraya River": ["Chao_Phraya_River", "Chao_Phraya"],
    "Robotham": ["Robotham"],
    "NERC-Hampshire Avon": ["NERC"],
    "Fukushima": ["Fukushima"],
    "Shashi_Jianli": ["Shashi_Jianli"],
    "Huanghe (Yellow River)": ["Huanghe"],
    "GSED": ["GSED"],
    "Dethier": ["Dethier"],
    "RiverSed (USA)": ["RiverSed"],
}

OUTPUT_COLUMNS = [
    "Data Source Name",
    "Type",
    "Observation type",
    "Temporal resolution",
    "Temporal_span",
    "Variables Provided",
    "Geographic coverage",
    "Citation",
    "reference",
    "source_url",
    "access_date",
    "n_source_stations",
    "n_clusters",
    "n_records",
]


SOURCE_FOLDER = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Source")

SOURCE_FOLDER_MAP = {
    "GFQA_v2": "GFQA_v2",
    "USGS NWIS": "USGS",
    "HYDAT": "Hydat",
    "Bayern": "bayern",
    "Eurasian Dataset": "Eurasian_River",
    "EUSEDcollab": "EUSEDcollab",
    "HYBAM": "HYBAM",
    "Rhine": "Rhine",
    "Mekong Delta": "Mekong_Delta",
    "Myanmar Rivers": "Myanmar",
    "Yajiang / Yarlung Tsangpo": "Yajiang",
    "Chao Phraya River": "Chao_Phraya_River",
    "Robotham": "Robotham",
    "NERC-Hampshire Avon": "NERC",
    "Fukushima": "Fukushima",
    "Shashi_Jianli": "Shashi_Jianli",
    "Huanghe (Yellow River)": "HuangHe",
    "GloRiSe v1.1": "GloRiSe",
    "Milliman & Farnsworth": "Milliman",
    "High Mountain Asia (HMA)": "HMA",
    "Ali & De Boer (Upper Indus)": "ALi_De_Boer",
    "Vanmaercke et al.": "Vanmaercke",
    "GSED": "GSED",
    "Dethier": "Dethier",
    "RiverSed (USA)": "RiverSed",
}


def load_access_dates() -> dict:
    """Scan Source folders for access/download dates.

    Priority:
    1. readme.html / __README.html  "Accessed from ... on YYYY-MM-DD"
    2. citation.rtf  "Accessed DD Mon YYYY"
    3. Filenames containing YYYY-MM-DD or DD.MM.YYYY
    4. Fallback: earliest file mtime in the folder
    """
    from datetime import datetime
    access_dates = {}

    for ms_name, folder_name in SOURCE_FOLDER_MAP.items():
        folder = SOURCE_FOLDER / folder_name
        if not folder.is_dir():
            continue

        date_str = ""

        # Priority 1: readme.html / __README.html
        for html_name in ("readme.html", "__README.html"):
            html_path = folder / html_name
            if html_path.exists():
                content = html_path.read_text(encoding="utf-8", errors="ignore")
                m = re.search(r"Accessed from.*?on\s+(\d{4}-\d{2}-\d{2})", content, re.DOTALL)
                if m:
                    date_str = m.group(1)
                    break
            if date_str:
                break

        # Priority 2: citation.rtf
        if not date_str:
            rtf_path = folder / "citation.rtf"
            if rtf_path.exists():
                content = rtf_path.read_text(encoding="utf-8", errors="ignore")
                m = re.search(r"Accessed\s+(\d{1,2}\s+\w+\s+\d{4})", content)
                if m:
                    try:
                        dt = datetime.strptime(m.group(1), "%d %b %Y")
                        date_str = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

        # Priority 3: filenames with dates (search recursively)
        if not date_str:
            for f in sorted(folder.rglob("*"), key=lambda p: len(str(p))):
                if f.is_file() and f.name != ".DS_Store" and ".claude" not in str(f):
                    m = re.search(r"(\d{4}-\d{2}-\d{2})", f.name)
                    if m:
                        date_str = m.group(1)
                        break
                    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", f.name)
                    if m and int(m.group(1)) <= 31 and int(m.group(2)) <= 12:
                        date_str = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                        break

        # Priority 4: earliest file mtime as fallback
        if not date_str:
            earliest = None
            for f in folder.rglob("*"):
                if f.is_file() and f.name != ".DS_Store" and ".claude" not in str(f):
                    mtime = f.stat().st_mtime
                    if earliest is None or mtime < earliest:
                        earliest = mtime
            if earliest is not None:
                date_str = datetime.fromtimestamp(earliest).strftime("%Y-%m-%d")

        if date_str:
            access_dates[ms_name] = date_str

    return access_dates




def clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat", "na", "n/a", "null"}:
        return ""
    return text


def normalize_key(value) -> str:
    text = clean_text(value).lower().replace("&", "and")
    text = re.sub(r"[^0-9a-z]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def decode_value(value) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore").strip()
    return clean_text(value)


def sorted_join(values) -> str:
    cleaned = sorted({clean_text(v) for v in values if clean_text(v)})
    return "; ".join(cleaned)


def open_nc(path: Path):
    for engine in ("h5netcdf", "netcdf4", None):
        try:
            if engine is None:
                return xr.open_dataset(path, decode_times=False)
            return xr.open_dataset(path, engine=engine, decode_times=False)
        except ValueError:
            continue
    return xr.open_dataset(path, decode_times=False)


def short_coverage(values, fallback: str = "") -> str:
    cleaned = sorted({clean_text(v) for v in values if clean_text(v)})
    if not cleaned:
        return fallback
    if len(cleaned) <= 12:
        return "; ".join(cleaned)
    return "; ".join(cleaned[:12]) + f"; (+{len(cleaned) - 12} more)"


def date_span(start_values, end_values, fallback: str = "") -> str:
    starts = pd.to_datetime([clean_text(v) for v in start_values if clean_text(v)], errors="coerce")
    ends = pd.to_datetime([clean_text(v) for v in end_values if clean_text(v)], errors="coerce")
    starts = starts[~pd.isna(starts)]
    ends = ends[~pd.isna(ends)]
    if len(starts) and len(ends):
        return f"{pd.Timestamp(starts.min()).strftime('%Y')}-{pd.Timestamp(ends.max()).strftime('%Y')}"
    return fallback


def alias_lookup(names, aliases):
    lookup = {}
    for name in names:
        key = normalize_key(name)
        if key:
            lookup[key] = name
    for manuscript_name, alias_names in aliases.items():
        candidates = [manuscript_name] + alias_names
        target = ""
        for candidate in candidates:
            key = normalize_key(candidate)
            if key in lookup:
                target = lookup[key]
                break
        if not target:
            continue
        for candidate in candidates:
            key = normalize_key(candidate)
            if key:
                lookup[key] = target
    return lookup


def load_nc_cluster_uids(path: Path) -> set:
    with open_nc(path) as ds:
        values = ds["cluster_uid"].values
    return {decode_value(v) for v in values if decode_value(v)}


def load_climatology_summary():
    path = CLIMATOLOGY_DIR / "sed_reference_climatology.nc"
    with open_nc(path) as ds:
        source_names = {decode_value(v) for v in ds["source_name"].values if decode_value(v)}
        rows = pd.DataFrame(
            {
                "source": [decode_value(v) for v in ds["source"].values],
                "dataset_name": [decode_value(v) for v in ds["dataset_name"].values],
                "country": [decode_value(v) for v in ds["country"].values],
                "geographic_coverage": [decode_value(v) for v in ds["geographic_coverage"].values],
                "time_start": [decode_value(v) for v in ds["source_station_time_coverage_start"].values],
                "time_end": [decode_value(v) for v in ds["source_station_time_coverage_end"].values],
            }
        )
    return rows, source_names


def load_satellite_summary():
    df = pd.read_csv(SATELLITE_CATALOG_CSV, keep_default_na=False, encoding="utf-8-sig")
    return df, set(df["source"].dropna().astype(str).str.strip())


def build_minimal_validation(source_station: pd.DataFrame):
    nc_clusters = {resolution: load_nc_cluster_uids(path) for resolution, path in NC_BY_RESOLUTION.items()}
    validation = {}
    report_rows = []

    for source_name, source_group in source_station.groupby("source_name", sort=True):
        source_details = []
        source_status = "pass"
        actual_resolutions = []
        for resolution, group in source_group.groupby("resolution", sort=True):
            resolution = clean_text(resolution).lower()
            if resolution not in NC_BY_RESOLUTION:
                continue
            actual_resolutions.append(resolution)
            expected_clusters = {clean_text(v) for v in group["cluster_uid"] if clean_text(v)}
            missing = expected_clusters - nc_clusters[resolution]
            nc_name = NC_BY_RESOLUTION[resolution].name
            if missing:
                source_status = "fail"
                detail = f"{resolution}:{nc_name}:missing_clusters={len(missing)}"
            else:
                detail = f"{resolution}:{nc_name}:clusters={len(expected_clusters)}:pass"
            source_details.append(detail)
            report_rows.append(
                {
                    "Data Source Name": source_name,
                    "Release package": "minimal",
                    "Expected temporal resolution": resolution,
                    "NC file": nc_name,
                    "Validation status": "fail" if missing else "pass",
                    "Details": detail,
                }
            )
        validation[source_name] = {
            "status": source_status if actual_resolutions else "not_found",
            "resolutions": sorted(actual_resolutions),
            "details": "; ".join(source_details),
        }
    return validation, report_rows


def supplement_from_minimal(row, minimal_name, source_group, dataset_row, minimal_validation):
    actual_resolutions = sorted(clean_text(v).lower() for v in source_group["resolution"].unique() if clean_text(v))
    validation = minimal_validation.get(minimal_name, {})
    release_temporal_span = date_span(
        source_group["time_start"],
        source_group["time_end"],
        clean_text(row.get("temporal_span_used", "")) or clean_text(row.get("Temporal Span", "")),
    )
    out = dict(row)
    out["Temporal resolution"] = "; ".join(actual_resolutions) or clean_text(row.get("Temporal Resolution", ""))
    out["Temporal_span"] = release_temporal_span
    out["Geographic coverage"] = clean_text(row.get("Geographic Coverage", ""))
    out["Observation type"] = clean_text(row.get("Acquisition", ""))
    out["Release package"] = "minimal"
    out["NC validation status"] = validation.get("status", "not_found")
    out["NC validation details"] = validation.get("details", "")
    if not out["Geographic coverage"] and dataset_row is not None:
        out["Geographic coverage"] = clean_text(dataset_row.get("geographic_coverage", ""))
    return out


def supplement_from_climatology(row, clim_name, clim_rows):
    group = clim_rows[clim_rows["source"] == clim_name]
    out = dict(row)
    out["Temporal resolution"] = "climatological"
    out["Temporal_span"] = date_span(group["time_start"], group["time_end"], clean_text(row.get("Temporal Span", "")))
    out["Geographic coverage"] = short_coverage(group["country"], clean_text(row.get("Geographic Coverage", "")))
    out["Observation type"] = clean_text(row.get("Acquisition", ""))
    out["n_source_stations"] = int(len(group))
    out["n_clusters"] = ""
    out["n_records"] = int(len(group))
    out["Release package"] = "climatology"
    out["NC validation status"] = "pass"
    out["NC validation details"] = f"sed_reference_climatology.nc:source={clim_name}:records={len(group)}"
    return out


def supplement_from_satellite(row, sat_name, sat_rows):
    group = sat_rows[sat_rows["source"] == sat_name].copy()
    out = dict(row)
    out["Temporal resolution"] = sorted_join(group["resolution"])
    out["Temporal_span"] = date_span(group["time_start"], group["time_end"], clean_text(row.get("Temporal Span", "")))
    out["Geographic coverage"] = short_coverage(group["country"], clean_text(row.get("Geographic Coverage", "")))
    out["Observation type"] = "Satellite"
    out["n_source_stations"] = int(len(group))
    out["n_clusters"] = int(group["cluster_uid"].astype(str).str.strip().nunique())
    out["n_records"] = int(pd.to_numeric(group["n_records"], errors="coerce").fillna(0).sum())
    out["Release package"] = "satellite"
    out["NC validation status"] = "pass"
    out["NC validation details"] = f"sed_reference_satellite.nc:source={sat_name}:stations={len(group)}"
    return out


def finalize_columns(rows):
    df = pd.DataFrame(rows)
    rename = {
        "Temporal Resolution": "Temporal resolution",
        "Temporal Span": "Temporal_span",
        "Geographic Coverage": "Geographic coverage",
        "Acquisition": "Observation type",
    }
    df = df.rename(columns=rename)
    deduped = pd.DataFrame(index=df.index)
    for col in dict.fromkeys(df.columns):
        same_name = df.loc[:, df.columns == col]
        if same_name.shape[1] == 1:
            deduped[col] = same_name.iloc[:, 0]
            continue
        series = same_name.iloc[:, -1].copy()
        for i in range(same_name.shape[1] - 2, -1, -1):
            empty = series.astype(str).str.strip().eq("")
            series = series.mask(empty, same_name.iloc[:, i])
        deduped[col] = series
    df = deduped
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[OUTPUT_COLUMNS].copy()


def excel_writer_engine() -> str:
    if importlib.util.find_spec("xlsxwriter"):
        return "xlsxwriter"
    if importlib.util.find_spec("openpyxl"):
        return "openpyxl"
    raise RuntimeError("Cannot write .xlsx: neither xlsxwriter nor openpyxl is installed.")


def main() -> int:
    # Step 1: Load raw manuscript and release catalogs
    manuscript = pd.read_csv(RAW_MANUSCRIPT_CSV, keep_default_na=False, encoding="utf-8-sig")
    source_station = pd.read_csv(SOURCE_STATION_CSV, keep_default_na=False, encoding="utf-8-sig")
    source_dataset = pd.read_csv(SOURCE_DATASET_CSV, keep_default_na=False, encoding="utf-8-sig")

    # Step 2: Fill manuscript with release-derived statistics (in-memory, no intermediate CSV)
    stats = summarize_minimal_source_stats(BASE_DIR / "sed_reference_release_minimal")
    manuscript = fill_manuscript_dataframe(manuscript, stats, source_dataset)

    # Step 3: Organize into minimal / climatology / satellite sheets
    clim_rows, clim_sources = load_climatology_summary()
    sat_rows, sat_sources = load_satellite_summary()

    minimal_lookup = alias_lookup(source_station["source_name"].unique(), MINIMAL_ALIASES)
    dataset_lookup = {
        clean_text(row["source_name"]): row
        for _, row in source_dataset.iterrows()
    }
    side_lookup = alias_lookup(sorted(clim_sources | sat_sources), SIDE_ALIASES)
    minimal_validation, validation_rows = build_minimal_validation(source_station)

    minimal_rows = []
    side_rows = []
    all_validation_rows = list(validation_rows)

    access_dates = load_access_dates()

    for _, row in manuscript.iterrows():
        manuscript_name = clean_text(row["Data Source Name"])
        minimal_name = minimal_lookup.get(normalize_key(manuscript_name), "")
        if minimal_name:
            group = source_station[source_station["source_name"] == minimal_name]
            dataset_row = dataset_lookup.get(minimal_name)
            out = supplement_from_minimal(row, minimal_name, group, dataset_row, minimal_validation)
            side_membership = []
            side_name = side_lookup.get(normalize_key(manuscript_name), "")
            if side_name in clim_sources:
                side_membership.append("climatology")
            if side_name in sat_sources:
                side_membership.append("satellite")
            if side_membership:
                out["Release package"] = "minimal; " + "; ".join(side_membership)
                out["NC validation details"] = out["NC validation details"] + "; also_in=" + "|".join(side_membership)
            minimal_rows.append(out)
            continue

        side_name = side_lookup.get(normalize_key(manuscript_name), "")
        if side_name in clim_sources:
            out = supplement_from_climatology(row, side_name, clim_rows)
        elif side_name in sat_sources:
            out = supplement_from_satellite(row, side_name, sat_rows)
        else:
            out = dict(row)
            out["Temporal resolution"] = clean_text(row.get("Temporal Resolution", ""))
            out["Temporal_span"] = clean_text(row.get("temporal_span_used", "")) or clean_text(row.get("Temporal Span", ""))
            out["Geographic coverage"] = clean_text(row.get("Geographic Coverage", ""))
            out["Observation type"] = clean_text(row.get("Acquisition", ""))
            out["Release package"] = "not_found_in_minimal_climatology_satellite"
            out["NC validation status"] = "not_found"
            out["NC validation details"] = "No matching source found in minimal, climatology, or satellite release outputs"
        side_rows.append(out)

    for row in side_rows:
        package = row.get("Release package", "")
        nc_file = ""
        if package == "climatology":
            nc_file = "sed_reference_climatology.nc"
        elif package == "satellite":
            nc_file = "sed_reference_satellite.nc"
        all_validation_rows.append(
            {
                "Data Source Name": row["Data Source Name"],
                "Release package": package,
                "Expected temporal resolution": row["Temporal resolution"],
                "NC file": nc_file,
                "Validation status": row.get("NC validation status", ""),
                "Details": row.get("NC validation details", ""),
            }
        )

    for row in minimal_rows:
        name = clean_text(row.get("Data Source Name", ""))
        if name in access_dates:
            row["access_date"] = access_dates[name]
    for row in side_rows:
        name = clean_text(row.get("Data Source Name", ""))
        if name in access_dates:
            row["access_date"] = access_dates[name]

    minimal_df = finalize_columns(minimal_rows)
    side_df = finalize_columns(side_rows)
    validation_df = pd.DataFrame(all_validation_rows)

    with pd.ExcelWriter(OUT_XLSX, engine=excel_writer_engine()) as writer:
        minimal_df.to_excel(writer, sheet_name="minimal_17", index=False)
        side_df.to_excel(writer, sheet_name="clim_sat_8", index=False)
        validation_df.to_excel(writer, sheet_name="nc_validation", index=False)

    print(f"[write] {OUT_XLSX}")
    print(
        f"[summary] minimal_sheet_rows={len(minimal_df)} side_sheet_rows={len(side_df)} "
        f"validation_rows={len(validation_df)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
