#!/usr/bin/env python3
"""Patch missing reference and source_url in source_dataset_catalog.csv.

Fills empty reference/source_url cells from a curated lookup table derived
from the manuscript source table. Only fills cells that are currently empty
— existing values are never overwritten.

Usage:
    python tools/patch_source_dataset_catalog_references.py
    python tools/patch_source_dataset_catalog_references.py --csv <path>
    python tools/patch_source_dataset_catalog_references.py --backup
"""

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CSV = (
    SCRIPT_DIR.parent
    / "output/sed_reference_release_minimal/source_dataset_catalog.csv"
)

# Curated lookup: source_name (exact match) -> {reference, source_url}
# Derived from the manuscript source table provided by the user.
SOURCE_LOOKUP = {
    "Ali & De Boer (Upper Indus)": {
        "reference": (
            "Ali, K. F. and De Boer, D. H.: Spatial patterns and variation of "
            "suspended sediment yield in the upper Indus River basin, northern "
            "Pakistan, J. Hydrol., 334, 368-387, "
            "https://doi.org/10.1016/j.jhydrol.2006.10.013, 2007."
        ),
        "source_url": "https://doi.org/10.1016/j.jhydrol.2006.10.013",
    },
    "Dethier": {
        "reference": (
            "Dethier, E. N., Renshaw, C. E., and Magilligan, F. J.: Rapid "
            "changes to global river suspended sediment flux by humans, "
            "Science, 376, 1447-1452, "
            "https://doi.org/10.1126/science.abn7980, 2022."
        ),
        "source_url": "https://doi.org/10.1126/science.abn7980",
    },
    "GSED": {
        "reference": (
            "Sun, X., Tian, L., Fang, H., Walling, D. E., Huang, L., Park, E., "
            "Li, D., Zheng, C., and Feng, L.: Changes in global fluvial sediment "
            "concentrations and fluxes between 1985 and 2020, Nat. Sustain., 8, "
            "142-151, https://doi.org/10.1038/s41893-024-01476-7, 2025."
        ),
        "source_url": "https://figshare.com/s/dde3bffd8e12227e2b26",
    },
    "High Mountain Asia (HMA)": {
        "reference": (
            "Li, D., Lu, X., Overeem, I., Walling, D. E., Syvitski, J., "
            "Kettner, A. J., Bookhagen, B., Zhou, Y., and Zhang, T.: "
            "Exceptional increases in fluvial sediment fluxes in a warmer and "
            "wetter High Mountain Asia, Science, 374, 599-603, "
            "https://doi.org/10.1126/science.abi9649, 2021."
        ),
        "source_url": "https://doi.org/10.1126/science.abi9649",
    },
    "Milliman & Farnsworth": {
        "reference": (
            "Milliman, J. D. and Farnsworth, K. L.: River Discharge to the "
            "Coastal Ocean: A Global Synthesis, Cambridge University Press, "
            "Cambridge, https://doi.org/10.1017/CBO9780511781247, 2011."
        ),
        "source_url": "https://doi.org/10.1017/CBO9780511781247",
    },
    "RiverSed (USA)": {
        "reference": (
            "Gardner, J., Pavelsky, T., Topp, S., Yang, X., Ross, M. R., and "
            "Cohen, S.: Human activities change suspended sediment concentration "
            "along rivers, Environ. Res. Lett., 18, 064032, "
            "https://doi.org/10.5281/zenodo.7938267, 2023."
        ),
        "source_url": "https://doi.org/10.5281/zenodo.7938267",
    },
    "Vanmaercke et al.": {
        "reference": (
            "Vanmaercke, M., Poesen, J., Broeckx, J., and Nyssen, J.: Sediment "
            "yield in Africa, Earth-Sci. Rev., 136, 350-368, "
            "https://doi.org/10.1016/j.earscirev.2014.06.004, 2014."
        ),
        "source_url": "https://doi.org/10.1016/j.earscirev.2014.06.004",
    },
}


def clean(value):
    """Return stripped string or empty string for NaN/None."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "nat", ""}:
        return ""
    return text


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV),
        help="Path to source_dataset_catalog.csv (default: minimal release dir)",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create .bak backup before overwriting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be filled without writing",
    )
    return parser.parse_args(argv)


def main():
    args = parse_args()
    csv_path = Path(args.csv).resolve()

    if not csv_path.is_file():
        print(f"[error] CSV not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path, keep_default_na=False)
    print(f"[read] {csv_path}  ({len(df)} rows)")

    fill_count = 0
    for idx, row in df.iterrows():
        name = clean(row.get("source_name", ""))
        if name not in SOURCE_LOOKUP:
            continue

        entry = SOURCE_LOOKUP[name]
        for col in ["reference", "source_url"]:
            current = clean(row.get(col, ""))
            new_value = entry.get(col, "")
            if not current and new_value:
                df.at[idx, col] = new_value
                fill_count += 1
                preview = new_value[:80] + ("..." if len(new_value) > 80 else "")
                print(f"  [fill] {name:35s} {col}: {preview}")

    if fill_count == 0:
        print("[info] No empty cells to fill — all references and source_urls present.")
        verify(df)
        return 0

    if args.dry_run:
        print(f"\n[dry-run] Would fill {fill_count} cell(s) — no changes written.")
        return 0

    if args.backup and csv_path.is_file():
        backup_path = csv_path.with_suffix(csv_path.suffix + ".bak")
        shutil.copy2(csv_path, backup_path)
        print(f"[backup] {backup_path}")

    df.to_csv(csv_path, index=False)
    print(f"\n[write] {csv_path}  ({fill_count} cell(s) filled)")

    verify(df)


def verify(df):
    """Check for remaining empty reference/source_url."""
    empty_ref = df[
        df["reference"].apply(lambda x: clean(x) == "")
    ]
    empty_url = df[
        df["source_url"].apply(lambda x: clean(x) == "")
    ]
    total_empty = set(empty_ref["source_name"].tolist()) | set(
        empty_url["source_name"].tolist()
    )
    if total_empty:
        print(f"\n[verify] {len(total_empty)} source(s) still have empty fields:")
        for name in sorted(total_empty):
            ref = "EMPTY" if clean(empty_ref[empty_ref["source_name"] == name].iloc[0]["reference"] if not empty_ref[empty_ref["source_name"] == name].empty else "") == "" else "OK"
            print(f"  {name:35s} ref={'EMPTY' if name in set(empty_ref['source_name']) else 'OK':6s}  url={'EMPTY' if name in set(empty_url['source_name']) else 'OK'}")
    else:
        print("[verify] All reference and source_url cells are filled.")


if __name__ == "__main__":
    sys.exit(main())
