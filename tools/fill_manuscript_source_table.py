#!/usr/bin/env python3
"""
Fill the current manuscript_source_table_cleaned.csv / manuscript_source__table_cleaned.csv
with source-level statistics from the final minimal release package.

No command-line parameters are required. Edit the CONFIG block below only if your file
locations or overwrite policy are different.

Default inputs, relative to the sed_data_integration repository root:
  scripts_basin_test/output/sed_reference_release_minimal/source_station_catalog.csv
  manuscript_source__table_cleaned.csv  or  manuscript_source_table_cleaned.csv

Default outputs:
  manuscript_source_table_cleaned_minimal_filled.csv
  minimal_source_dataset_stats_for_manuscript.csv
  minimal_source_dataset_stats_match_report.csv
  minimal_source_dataset_manuscript_qa_report.csv
  manuscript_source_table_cleaned_minimal_diff_summary.csv
"""

import re
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

# =============================================================================
# CONFIG: all settings are built in here. Normally you only run:
#   python tools/fill_manuscript_source_table_from_minimal_builtin.py
# =============================================================================

MINIMAL_DIR = "scripts_basin_test/output/sed_reference_release_minimal"
MINIMAL_SOURCE_STATION_CATALOG = "source_station_catalog.csv"
MINIMAL_SOURCE_DATASET_CATALOG = "source_dataset_catalog.csv"

MANUSCRIPT_TABLE_CANDIDATES = [
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/docs/manuscript_source_table_cleaned.csv",
]

# If None, the script writes <input_stem>_minimal_filled.csv beside the input table.
OUTPUT_MANUSCRIPT_TABLE: Optional[str] = None

STATS_OUTPUT = "minimal_source_dataset_stats_for_manuscript.csv"
MATCH_REPORT_OUTPUT = "minimal_source_dataset_stats_match_report.csv"
QA_REPORT_OUTPUT = "minimal_source_dataset_manuscript_qa_report.csv"
DIFF_SUMMARY_OUTPUT = "manuscript_source_table_cleaned_minimal_diff_summary.csv"

# Auto-detects by default. Set to "Data Source Name" if you want to force it.
SOURCE_NAME_COLUMN: Optional[str] = None

# overwrite: replace the manuscript statistics with final minimal-release statistics.
# fill-empty-only: only fill blank cells.
FILL_MODE = "overwrite"  # "overwrite" or "fill-empty-only"

# Also update the manuscript-level "Temporal Span" column using the release-derived
# year span, e.g. 1994-2024. This is useful when Table 1 should reflect the final
# minimal release rather than the raw source's full nominal span.
UPDATE_TEMPORAL_SPAN_COLUMN = True

# Keep the original manuscript CSV unchanged by default.
# If True, a .bak backup is created and the input CSV is overwritten.
IN_PLACE = False

# Add diagnostic match columns to the output manuscript table. Usually False for a
# clean manuscript table; matching details are always written to MATCH_REPORT_OUTPUT.
ADD_MATCH_COLUMNS_TO_MANUSCRIPT = False

MINIMAL_RESOLUTIONS = {"daily", "monthly", "annual"}
MISSING_TEXT = {"", "nan", "none", "nat", "na", "n/a", "null", "_", "--"}

STAT_COLUMNS = [
    "temporal_span_used",
    "time_start",
    "time_end",
    "n_source_stations",
    "n_clusters",
    "n_records",
]

METADATA_COLUMN_MAP = {
    "reference": "reference",
    "source_url": "source_url",
    "Geographic Coverage": "geographic_coverage",
    "Citation": "preferred_citation",
    "Temporal Resolution": "temporal_resolution_used",
}

# Built-in aliases for forgiving matching between manuscript names and
# source_station_catalog.csv source_name values. Keys and values are normalized
# before matching, so punctuation/case differences are tolerated.
SOURCE_ALIASES: Dict[str, List[str]] = {
    "GloRiSe v1.1": ["GloRiSe", "glorise_v1_1"],
    "GFQA_v2": ["GFQA", "GEMS", "GEMStat", "GEMS_Water", "GFQA v2"],
    "Milliman & Farnsworth": ["Milliman and Farnsworth", "Milliman_Farnsworth"],
    "USGS NWIS": ["NWIS", "USGS", "USGS_NWIS"],
    "HYDAT": ["Water Survey of Canada", "WSC"],
    "Bayern": ["GKD Bayern", "Bayern_GKD", "gkd_bayern"],
    "Eurasian Dataset": ["Eurasian", "Eurasian_River", "Eurasian_Arctic"],
    "EUSEDcollab": ["EUSED", "EUSEDcollab.v1", "EUSEDcollab_v1"],
    "High Mountain Asia (HMA)": ["High Mountain Asia", "HMA"],
    "Ali & De Boer (Upper Indus)": ["Ali & De Boer", "Ali and De Boer", "Ali_De_Boer", "Upper Indus"],
    "Vanmaercke et al.": ["Vanmaercke", "Vanmaercke_Africa"],
    "HYBAM": ["HYBAM Observatory"],
    "Rhine": ["Rhine Basin", "Rhine_Basin"],
    "Mekong Delta": ["Mekong_Delta"],
    "Myanmar Rivers": ["Myanmar", "Myanmar_Rivers", "Irrawaddy Salween"],
    "Yajiang / Yarlung Tsangpo": ["Yajiang", "Yarlung Tsangpo", "Yajiang_Yarlung_Tsangpo"],
    "Chao Phraya River": ["Chao Phraya", "Chao_Phraya"],
    "Robotham": ["Littlestock Brook", "Littlestock_Brook"],
    "NERC-Hampshire Avon": ["NERC", "NERC_Hampshire_Avon", "Hampshire Avon", "River Avon"],
    "Fukushima": ["Fukushima_Niida", "Niida River"],
    "Shashi_Jianli": ["Shashi-Jianli", "Shashi Jianli", "Yangtze Shashi Jianli"],
    "Huanghe (Yellow River)": ["Huanghe", "Yellow River", "Huanghe_Yellow_River"],
    "GSED": ["Global Suspended Sediment Dynamics"],
    "Dethier": ["Dethier et al.", "Dethier_virtual_stations"],
    "RiverSed (USA)": ["RiverSed", "RiverSed_USA", "RiverSed USA"],
}


# =============================================================================
# Utility functions
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent


def candidate_roots() -> List[Path]:
    roots: List[Path] = []
    for path in [Path.cwd(), SCRIPT_DIR, SCRIPT_DIR.parent, SCRIPT_DIR.parent.parent]:
        try:
            resolved = path.resolve()
        except Exception:
            continue
        if resolved not in roots:
            roots.append(resolved)
    return roots


def resolve_existing_path(path_text: str) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path.resolve()
    for root in candidate_roots():
        candidate = (root / path).resolve()
        if candidate.exists():
            return candidate
    return (Path.cwd() / path).resolve()


def resolve_output_path(path_text: str, base_dir: Optional[Path] = None) -> Path:
    path = Path(path_text).expanduser()
    if path.is_absolute():
        return path.resolve()
    if base_dir is not None:
        return (base_dir / path).resolve()
    return (Path.cwd() / path).resolve()


def clean_text(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in MISSING_TEXT:
        return ""
    return text


def normalize_key(value) -> str:
    text = clean_text(value).lower()
    text = text.replace("&", "and")
    text = re.sub(r"[^0-9a-z]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = ""
    return out


def read_csv_text(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=False, encoding="utf-8-sig")


def write_csv_text(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def min_date_text(values: Iterable) -> str:
    cleaned = [clean_text(v) for v in values if clean_text(v)]
    if not cleaned:
        return ""
    parsed = pd.to_datetime(cleaned, errors="coerce")
    valid = parsed[~pd.isna(parsed)]
    if len(valid) == 0:
        return sorted(cleaned)[0]
    return pd.Timestamp(valid.min()).strftime("%Y-%m-%d")


def max_date_text(values: Iterable) -> str:
    cleaned = [clean_text(v) for v in values if clean_text(v)]
    if not cleaned:
        return ""
    parsed = pd.to_datetime(cleaned, errors="coerce")
    valid = parsed[~pd.isna(parsed)]
    if len(valid) == 0:
        return sorted(cleaned)[-1]
    return pd.Timestamp(valid.max()).strftime("%Y-%m-%d")


def temporal_span_text(time_start: str, time_end: str) -> str:
    if time_start and time_end:
        return f"{time_start}-{time_end}"
    return time_start or time_end or ""


def year_span_text(time_start: str, time_end: str) -> str:
    y0 = time_start[:4] if time_start else ""
    y1 = time_end[:4] if time_end else ""
    if y0 and y1:
        return y0 if y0 == y1 else f"{y0}-{y1}"
    return y0 or y1


def count_unique_nonempty(values: Iterable) -> int:
    return len({clean_text(v) for v in values if clean_text(v)})


# =============================================================================
# Main aggregation and filling logic
# =============================================================================


def find_manuscript_table() -> Path:
    for name in MANUSCRIPT_TABLE_CANDIDATES:
        path = resolve_existing_path(name)
        if path.is_file():
            return path
    tried = ", ".join(MANUSCRIPT_TABLE_CANDIDATES)
    raise FileNotFoundError(f"Cannot find manuscript source table. Tried: {tried}")


def detect_source_column(df: pd.DataFrame) -> str:
    if SOURCE_NAME_COLUMN:
        if SOURCE_NAME_COLUMN not in df.columns:
            raise ValueError(f"Configured SOURCE_NAME_COLUMN not found: {SOURCE_NAME_COLUMN}")
        return SOURCE_NAME_COLUMN

    candidates = [
        "Data Source Name",
        "source_name",
        "Data Source",
        "Source Dataset Name",
        "Dataset",
        "dataset",
        "Source",
        "source",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    if len(df.columns) == 0:
        raise ValueError("The manuscript CSV has no columns.")
    return df.columns[0]


def summarize_minimal_source_stats(minimal_dir: Path) -> pd.DataFrame:
    source_station_path = minimal_dir / MINIMAL_SOURCE_STATION_CATALOG
    if not source_station_path.is_file():
        raise FileNotFoundError(f"Cannot find minimal source-station catalog: {source_station_path}")

    df = read_csv_text(source_station_path)
    required = [
        "source_name",
        "resolution",
        "source_station_uid",
        "cluster_uid",
        "n_records",
        "time_start",
        "time_end",
    ]
    df = ensure_columns(df, required)

    resolution = df["resolution"].astype(str).str.strip().str.lower()
    work = df[resolution.isin(MINIMAL_RESOLUTIONS)].copy()
    work["n_records"] = pd.to_numeric(work["n_records"], errors="coerce").fillna(0).astype("int64")

    rows = []
    for source_name, group in work.groupby("source_name", dropna=False, sort=True):
        source_name = clean_text(source_name)
        if not source_name:
            continue
        time_start = min_date_text(group["time_start"])
        time_end = max_date_text(group["time_end"])
        rows.append(
            {
                "source_name": source_name,
                "source_key": normalize_key(source_name),
                "temporal_span_used": temporal_span_text(time_start, time_end),
                "temporal_span_years": year_span_text(time_start, time_end),
                "time_start": time_start,
                "time_end": time_end,
                "n_source_stations": count_unique_nonempty(group["source_station_uid"]),
                "n_clusters": count_unique_nonempty(group["cluster_uid"]),
                "n_records": int(group["n_records"].sum()),
            }
        )

    return pd.DataFrame(rows).sort_values("source_name", kind="mergesort").reset_index(drop=True)


def read_minimal_source_dataset_catalog(minimal_dir: Path) -> pd.DataFrame:
    source_dataset_path = minimal_dir / MINIMAL_SOURCE_DATASET_CATALOG
    if not source_dataset_path.is_file():
        raise FileNotFoundError(f"Cannot find minimal source-dataset catalog: {source_dataset_path}")

    df = read_csv_text(source_dataset_path)
    needed = [
        "source_name",
        "reference",
        "source_url",
        "geographic_coverage",
        "preferred_citation",
        "temporal_resolution_used",
        "access_date",
        "metadata_status",
    ]
    df = ensure_columns(df, needed)
    df["source_key"] = df["source_name"].map(normalize_key)
    return df


def build_stats_lookup(stats: pd.DataFrame) -> Dict[str, Dict[str, object]]:
    lookup: Dict[str, Dict[str, object]] = {}
    for _, row in stats.iterrows():
        row_dict = row.to_dict()
        key = normalize_key(row_dict.get("source_name", ""))
        if key:
            lookup[key] = row_dict
    return lookup


def build_dataset_lookup(source_dataset: pd.DataFrame) -> Dict[str, Dict[str, object]]:
    lookup: Dict[str, Dict[str, object]] = {}
    for _, row in source_dataset.iterrows():
        row_dict = row.to_dict()
        key = normalize_key(row_dict.get("source_name", ""))
        if key:
            lookup[key] = row_dict
    return lookup


def build_alias_lookup(stats_lookup: Dict[str, Dict[str, object]]) -> Dict[str, str]:
    alias_to_source_key: Dict[str, str] = {}
    for source_key in stats_lookup:
        alias_to_source_key[source_key] = source_key

    for manuscript_name, aliases in SOURCE_ALIASES.items():
        candidate_keys = [normalize_key(manuscript_name)] + [normalize_key(a) for a in aliases]
        target_key = ""
        for key in candidate_keys:
            if key in stats_lookup:
                target_key = key
                break
        if not target_key:
            continue
        for key in candidate_keys:
            if key:
                alias_to_source_key[key] = target_key
    return alias_to_source_key


def values_differ(left, right) -> bool:
    return normalize_key(left) != normalize_key(right)


def build_diff_summary(original: pd.DataFrame, filled: pd.DataFrame, source_col: str) -> pd.DataFrame:
    rows = []
    common_columns = [col for col in original.columns if col in filled.columns]
    for idx, (before, after) in enumerate(zip(original.to_dict("records"), filled.to_dict("records")), start=1):
        for col in common_columns:
            before_value = clean_text(before.get(col, ""))
            after_value = clean_text(after.get(col, ""))
            if before_value == after_value:
                continue
            rows.append(
                {
                    "row_number": idx,
                    "manuscript_source_name": clean_text(before.get(source_col, "")),
                    "column": col,
                    "before": before_value,
                    "after": after_value,
                }
            )
    return pd.DataFrame(rows)



def fill_manuscript_dataframe(
    manuscript: pd.DataFrame,
    stats: pd.DataFrame,
    source_dataset: pd.DataFrame,
    source_col: Optional[str] = None,
) -> pd.DataFrame:
    """Fill a manuscript DataFrame with release-derived statistics, in-memory.
    
    This is the core fill logic, exposed for callers that already have a DataFrame
    and don't need the CSV-read/write wrappers in fill_manuscript_table_impl.
    
    Returns the filled manuscript DataFrame (modified copy).
    """
    if FILL_MODE not in {"overwrite", "fill-empty-only"}:
        raise ValueError("FILL_MODE must be 'overwrite' or 'fill-empty-only'.")

    manuscript = ensure_columns(manuscript, STAT_COLUMNS)
    if source_col is None:
        source_col = detect_source_column(manuscript)

    stats_lookup = build_stats_lookup(stats)
    dataset_lookup = build_dataset_lookup(source_dataset)
    alias_lookup = build_alias_lookup(stats_lookup)

    out = manuscript.copy()

    for idx, row in out.iterrows():
        manuscript_name = clean_text(row.get(source_col, ""))
        manuscript_key = normalize_key(manuscript_name)
        source_key = alias_lookup.get(manuscript_key, "")
        stat = stats_lookup.get(source_key, {}) if source_key else {}
        dataset = dataset_lookup.get(source_key, {}) if source_key else {}

        if stat:
            for col in STAT_COLUMNS:
                value = stat.get(col, "")
                if not clean_text(value):
                    continue
                if FILL_MODE == "fill-empty-only" and clean_text(out.at[idx, col]):
                    continue
                out.at[idx, col] = value

            if UPDATE_TEMPORAL_SPAN_COLUMN and "Temporal Span" in out.columns:
                year_span = clean_text(stat.get("temporal_span_years", ""))
                if year_span:
                    if not (FILL_MODE == "fill-empty-only" and clean_text(out.at[idx, "Temporal Span"])):
                        out.at[idx, "Temporal Span"] = year_span

            for manuscript_col, dataset_col in METADATA_COLUMN_MAP.items():
                if manuscript_col not in out.columns:
                    continue
                dataset_value = clean_text(dataset.get(dataset_col, ""))
                manuscript_value = clean_text(out.at[idx, manuscript_col])
                if not dataset_value:
                    continue
                if not manuscript_value:
                    out.at[idx, manuscript_col] = dataset_value
                elif values_differ(manuscript_value, dataset_value):
                    pass  # metadata diff noted in stats only

    return out


def fill_manuscript_table_impl(
    manuscript_csv: Path,
    stats: pd.DataFrame,
    source_dataset: pd.DataFrame,
    output_csv: Optional[Path] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if FILL_MODE not in {"overwrite", "fill-empty-only"}:
        raise ValueError("FILL_MODE must be 'overwrite' or 'fill-empty-only'.")

    manuscript = read_csv_text(manuscript_csv)
    source_col = detect_source_column(manuscript)
    manuscript = ensure_columns(manuscript, STAT_COLUMNS)

    stats_lookup = build_stats_lookup(stats)
    dataset_lookup = build_dataset_lookup(source_dataset)
    alias_lookup = build_alias_lookup(stats_lookup)

    out = manuscript.copy()
    match_rows = []
    qa_rows = []

    match_rows = []
    qa_rows = []

    if ADD_MATCH_COLUMNS_TO_MANUSCRIPT:
        out["matched_minimal_source_name"] = ""
        out["matched_minimal_source_key"] = ""
        out["minimal_stats_match_status"] = ""

    for idx, row in out.iterrows():
        row_number = idx + 1
        manuscript_name = clean_text(row.get(source_col, ""))
        manuscript_key = normalize_key(manuscript_name)
        source_key = alias_lookup.get(manuscript_key, "")
        stat = stats_lookup.get(source_key, {}) if source_key else {}
        dataset = dataset_lookup.get(source_key, {}) if source_key else {}
        metadata_differences = []
        metadata_filled = []

        if stat:
            status = "matched_minimal_release"
            for col in STAT_COLUMNS:
                value = stat.get(col, "")
                if not clean_text(value):
                    continue
                if FILL_MODE == "fill-empty-only" and clean_text(out.at[idx, col]):
                    continue
                out.at[idx, col] = value

            if UPDATE_TEMPORAL_SPAN_COLUMN and "Temporal Span" in out.columns:
                year_span = clean_text(stat.get("temporal_span_years", ""))
                if year_span:
                    if not (FILL_MODE == "fill-empty-only" and clean_text(out.at[idx, "Temporal Span"])):
                        out.at[idx, "Temporal Span"] = year_span

            for manuscript_col, dataset_col in METADATA_COLUMN_MAP.items():
                if manuscript_col not in out.columns:
                    continue
                dataset_value = clean_text(dataset.get(dataset_col, ""))
                manuscript_value = clean_text(out.at[idx, manuscript_col])
                if not dataset_value:
                    continue
                if not manuscript_value:
                    out.at[idx, manuscript_col] = dataset_value
                    metadata_filled.append(manuscript_col)
                elif values_differ(manuscript_value, dataset_value):
                    metadata_differences.append(manuscript_col)
        else:
            status = "not_in_minimal_release"

        metadata_review_reasons = []
        if stat:
            metadata_status = clean_text(dataset.get("metadata_status", ""))
            if metadata_status:
                metadata_review_reasons.append(metadata_status)
            if not clean_text(dataset.get("access_date", "")):
                metadata_review_reasons.append("missing_access_date")
            if metadata_differences:
                metadata_review_reasons.append("metadata_diff:" + "|".join(metadata_differences))

        row_status = [status]
        if metadata_review_reasons:
            row_status.append("metadata_needs_review")

        if ADD_MATCH_COLUMNS_TO_MANUSCRIPT:
            out.at[idx, "matched_minimal_source_name"] = clean_text(stat.get("source_name", "")) if stat else ""
            out.at[idx, "matched_minimal_source_key"] = source_key
            out.at[idx, "minimal_stats_match_status"] = status

        match_rows.append(
            {
                "row_number": row_number,
                "manuscript_source_name": manuscript_name,
                "manuscript_source_key": manuscript_key,
                "matched_source_name": clean_text(stat.get("source_name", "")) if stat else "",
                "matched_source_key": source_key,
                "status": status,
            }
        )
        qa_rows.append(
            {
                "row_number": row_number,
                "manuscript_source_name": manuscript_name,
                "matched_source_name": clean_text(stat.get("source_name", "")) if stat else "",
                "row_status": "|".join(row_status),
                "minimal_release_status": status,
                "metadata_review_status": "metadata_needs_review" if metadata_review_reasons else "metadata_ok",
                "metadata_review_reasons": ";".join(metadata_review_reasons),
                "metadata_fields_filled": "|".join(metadata_filled),
                "metadata_fields_differ": "|".join(metadata_differences),
                "access_date": clean_text(dataset.get("access_date", "")),
                "source_dataset_metadata_status": clean_text(dataset.get("metadata_status", "")),
            }
        )

    if output_csv is not None:
        write_csv_text(out, output_csv)
    diff_summary = build_diff_summary(manuscript, out, source_col)
    return out, pd.DataFrame(match_rows), pd.DataFrame(qa_rows), diff_summary


def main() -> int:
    minimal_dir = resolve_existing_path(MINIMAL_DIR)
    manuscript_csv = find_manuscript_table()

    if IN_PLACE:
        output_csv = manuscript_csv
        backup = manuscript_csv.with_suffix(manuscript_csv.suffix + ".bak")
        shutil.copy2(manuscript_csv, backup)
        print(f"[backup] {backup}")
    elif OUTPUT_MANUSCRIPT_TABLE:
        output_csv = resolve_output_path(OUTPUT_MANUSCRIPT_TABLE, manuscript_csv.parent)
    else:
        output_csv = manuscript_csv.with_name(f"{manuscript_csv.stem}_minimal_filled.csv")

    stats_out = resolve_output_path(STATS_OUTPUT, manuscript_csv.parent)
    match_report_out = resolve_output_path(MATCH_REPORT_OUTPUT, manuscript_csv.parent)
    qa_report_out = resolve_output_path(QA_REPORT_OUTPUT, manuscript_csv.parent)
    diff_summary_out = resolve_output_path(DIFF_SUMMARY_OUTPUT, manuscript_csv.parent)

    print(f"[input] minimal_dir: {minimal_dir}")
    print(f"[input] manuscript_table: {manuscript_csv}")

    stats = summarize_minimal_source_stats(minimal_dir)
    source_dataset = read_minimal_source_dataset_catalog(minimal_dir)
    write_csv_text(stats, stats_out)

    filled, match_report, qa_report, diff_summary = fill_manuscript_table_impl(
        manuscript_csv,
        stats,
        source_dataset,
        output_csv,
    )
    write_csv_text(match_report, match_report_out)
    write_csv_text(qa_report, qa_report_out)
    write_csv_text(diff_summary, diff_summary_out)

    n_matched = int(match_report["status"].eq("matched_minimal_release").sum()) if not match_report.empty else 0
    n_unmatched = int(match_report["status"].eq("not_in_minimal_release").sum()) if not match_report.empty else 0
    n_metadata_review = (
        int(qa_report["metadata_review_status"].eq("metadata_needs_review").sum()) if not qa_report.empty else 0
    )

    print(f"[write] stats: {stats_out}")
    print(f"[write] manuscript: {output_csv}")
    print(f"[write] match_report: {match_report_out}")
    print(f"[write] qa_report: {qa_report_out}")
    print(f"[write] diff_summary: {diff_summary_out}")
    print(
        "[summary] "
        f"minimal_sources={len(stats)} manuscript_rows={len(filled)} "
        f"matched={n_matched} unmatched={n_unmatched} metadata_needs_review={n_metadata_review}"
    )

    if n_unmatched:
        print("[unmatched]")
        for name in match_report.loc[
            match_report["status"].eq("not_in_minimal_release"),
            "manuscript_source_name",
        ].tolist():
            print(f"  - {name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
