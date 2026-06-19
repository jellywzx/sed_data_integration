#!/usr/bin/env python3
"""
Fill empty reference/source_url in source_dataset_catalog.csv
from the reference Excel manuscript table.
"""

import pandas as pd
import re, sys
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
CSV_PATH = SCRIPT_DIR / "output/sed_reference_release_minimal/source_dataset_catalog.csv"
XLSX_PATH = SCRIPT_DIR / "docs/manuscript_source_table_cleaned_minimal_filled_organized.xlsx"


def normalize(name):
    if pd.isna(name):
        return ""
    return re.sub(r"\s+", " ", str(name).strip().lower())


def main():
    # 1. Load Excel reference
    if not XLSX_PATH.is_file():
        print(f"[error] Excel not found: {XLSX_PATH}")
        sys.exit(1)
    xlsx = pd.read_excel(XLSX_PATH, engine="openpyxl")
    print(f"[ref] Excel loaded: {len(xlsx)} rows")

    # 2. Load CSV
    if not CSV_PATH.is_file():
        print(f"[error] CSV not found: {CSV_PATH}")
        sys.exit(1)
    csv = pd.read_csv(CSV_PATH)
    print(f"[csv] CSV loaded: {len(csv)} rows")

    # 3. Build lookup: normalized name → Excel row
    lookup = {}
    for _, row in xlsx.iterrows():
        key = normalize(row.get("Data Source Name", ""))
        if key:
            lookup[key] = row

    matched_names = set()
    fill_count = 0
    for idx, row in csv.iterrows():
        key = normalize(row.get("source_name", ""))
        if not key or key not in lookup:
            continue
        matched_names.add(key)
        xrow = lookup[key]
        for col in ["reference", "source_url"]:
            csv_val = row.get(col, "")
            if pd.isna(csv_val) or str(csv_val).strip() == "":
                xlsx_val = xrow.get(col, "")
                if pd.notna(xlsx_val) and str(xlsx_val).strip():
                    csv.at[idx, col] = str(xlsx_val).strip()
                    fill_count += 1
                    prefix = str(xlsx_val)[:80]
                    print(f"  [fill] {row['source_name']}.{col} <- {prefix}...")

    # 4. Write back
    csv.to_csv(CSV_PATH, index=False)
    print(f"\n[done] {fill_count} cell(s) filled, {len(matched_names)} source(s) matched")
    print(f"       saved to {CSV_PATH}")

    # 5. Verify
    csv2 = pd.read_csv(CSV_PATH)
    empty = csv2[
        csv2["reference"].isna()
        | (csv2["reference"] == "")
        | csv2["source_url"].isna()
        | (csv2["source_url"] == "")
    ]
    if empty.empty:
        print("[verify] All cells filled ✓")
    else:
        print(f"[verify] {len(empty)} row(s) still with empty fields (no Excel match):")
        for _, row in empty.iterrows():
            ref = (
                "EMPTY"
                if pd.isna(row["reference"]) or row["reference"] == ""
                else "OK"
            )
            url = (
                "EMPTY"
                if pd.isna(row["source_url"]) or row["source_url"] == ""
                else "OK"
            )
            print(f"  {row['source_name']:35s} ref={ref:6s}  url={url}")


if __name__ == "__main__":
    main()
