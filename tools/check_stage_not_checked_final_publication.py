#!/usr/bin/env python3
"""Check whether stage-level not_checked=8 records are published as usable."""

import csv
from datetime import datetime
from pathlib import Path

import numpy as np
import xarray as xr


BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output_other" / "qc_not_checked_publication_check"
SUMMARY_CSV = OUTPUT_DIR / "qc_not_checked_publication_summary.csv"
REPORT_MD = OUTPUT_DIR / "qc_not_checked_publication_report.md"

INPUT_FILES = [
    "output/sed_reference_release/sed_reference_master.nc",
    "output/sed_reference_release/sed_reference_timeseries_daily.nc",
    "output/sed_reference_release/sed_reference_timeseries_monthly.nc",
    "output/sed_reference_release/sed_reference_timeseries_annual.nc",
    "output/sed_reference_release_minimal/sed_reference_timeseries_daily.nc",
    "output/sed_reference_release_minimal/sed_reference_timeseries_monthly.nc",
    "output/sed_reference_release_minimal/sed_reference_timeseries_annual.nc",
]

VARIABLE_SPECS = {
    "Q": {
        "value": "Q",
        "flag": "Q_flag",
        "stage": ("Q_qc2",),
    },
    "SSC": {
        "value": "SSC",
        "flag": "SSC_flag",
        "stage": ("SSC_qc2", "SSC_qc3"),
    },
    "SSL": {
        "value": "SSL",
        "flag": "SSL_flag",
        "stage": ("SSL_qc2", "SSL_qc3"),
    },
}

FINAL_FLAGS_TO_COUNT = (0, 1, 2, 3, 9)
PUBLISHED_USABLE_FLAGS = (0, 1, 2)
EXTRA_MISSING_VALUES = (-9999, -127)


def _flatten_var(ds, name):
    if name not in ds.variables:
        return None
    return np.asarray(ds[name].values).reshape(-1)


def _numeric_attr_values(attrs, key):
    if key not in attrs:
        return []
    values = np.asarray(attrs[key]).reshape(-1)
    numeric = []
    for value in values:
        try:
            numeric.append(float(value))
        except (TypeError, ValueError):
            continue
    return numeric


def _missing_values_for(ds, name):
    attrs = ds[name].attrs
    values = list(EXTRA_MISSING_VALUES)
    values.extend(_numeric_attr_values(attrs, "_FillValue"))
    values.extend(_numeric_attr_values(attrs, "missing_value"))
    unique = []
    for value in values:
        if not any(np.isclose(value, existing, rtol=0.0, atol=0.0) for existing in unique):
            unique.append(value)
    return unique


def _non_missing_value_mask(ds, name):
    values = _flatten_var(ds, name)
    if values is None:
        return None

    if np.ma.isMaskedArray(values):
        mask = ~np.ma.getmaskarray(values).reshape(-1)
        values = np.asarray(values.filled(np.nan)).reshape(-1)
    else:
        mask = np.ones(values.shape, dtype=bool)

    if np.issubdtype(values.dtype, np.floating):
        mask &= np.isfinite(values)
        compare_values = values.astype(np.float64, copy=False)
        for missing in _missing_values_for(ds, name):
            if np.isfinite(missing):
                mask &= ~np.isclose(compare_values, missing, rtol=1e-5, atol=1e-5)
    else:
        compare_values = values.astype(np.float64, copy=False)
        for missing in _missing_values_for(ds, name):
            if np.isfinite(missing):
                mask &= compare_values != missing
    return mask


def _stage_not_checked_mask(ds, names, size):
    mask = np.zeros(size, dtype=bool)
    present = []
    for name in names:
        values = _flatten_var(ds, name)
        if values is None:
            continue
        if values.size != size:
            raise ValueError(f"{name} has {values.size} values, expected {size}")
        present.append(name)
        mask |= values.astype(np.int16, copy=False) == 8
    return mask, present


def _flag_values(ds, name, size):
    values = _flatten_var(ds, name)
    if values is None:
        return None
    if values.size != size:
        raise ValueError(f"{name} has {values.size} values, expected {size}")
    return values.astype(np.int16, copy=False)


def _analyze_variable(ds, file_label, variable):
    spec = VARIABLE_SPECS[variable]
    value_mask = _non_missing_value_mask(ds, spec["value"])
    value_present = value_mask is not None
    size = int(value_mask.size) if value_mask is not None else 0

    row = {
        "file": file_label,
        "exists": "yes",
        "variable": variable,
        "value_var": spec["value"],
        "value_var_present": "yes" if value_present else "no",
        "final_flag_var": spec["flag"],
        "final_flag_present": "no",
        "stage_flag_vars": "|".join(spec["stage"]),
        "stage_flag_vars_present": "",
        "n_cells_checked": size,
        "final_flag_has_8": "no",
        "stage_not_checked_nonmissing_count": 0,
        "published_usable_stage_not_checked_count": 0,
    }
    for flag in FINAL_FLAGS_TO_COUNT:
        row[f"final_flag_{flag}_count"] = 0

    if value_mask is None:
        return row

    final_flag = _flag_values(ds, spec["flag"], size)
    if final_flag is None:
        return row
    row["final_flag_present"] = "yes"
    row["final_flag_has_8"] = "yes" if bool(np.any(final_flag == 8)) else "no"

    stage_mask, present_stage = _stage_not_checked_mask(ds, spec["stage"], size)
    row["stage_flag_vars_present"] = "|".join(present_stage)
    stage_value_mask = stage_mask & value_mask
    row["stage_not_checked_nonmissing_count"] = int(np.count_nonzero(stage_value_mask))

    usable_mask = stage_value_mask & np.isin(final_flag, PUBLISHED_USABLE_FLAGS)
    row["published_usable_stage_not_checked_count"] = int(np.count_nonzero(usable_mask))
    for flag in FINAL_FLAGS_TO_COUNT:
        row[f"final_flag_{flag}_count"] = int(np.count_nonzero(stage_value_mask & (final_flag == flag)))
    return row


def _missing_file_rows(file_label):
    rows = []
    for variable, spec in VARIABLE_SPECS.items():
        row = {
            "file": file_label,
            "exists": "no",
            "variable": variable,
            "value_var": spec["value"],
            "value_var_present": "no",
            "final_flag_var": spec["flag"],
            "final_flag_present": "no",
            "stage_flag_vars": "|".join(spec["stage"]),
            "stage_flag_vars_present": "",
            "n_cells_checked": 0,
            "final_flag_has_8": "no",
            "stage_not_checked_nonmissing_count": 0,
            "published_usable_stage_not_checked_count": 0,
        }
        for flag in FINAL_FLAGS_TO_COUNT:
            row[f"final_flag_{flag}_count"] = 0
        rows.append(row)
    return rows


def _write_summary(rows):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "file",
        "exists",
        "variable",
        "value_var",
        "value_var_present",
        "final_flag_var",
        "final_flag_present",
        "stage_flag_vars",
        "stage_flag_vars_present",
        "n_cells_checked",
        "final_flag_has_8",
        "stage_not_checked_nonmissing_count",
        "published_usable_stage_not_checked_count",
    ]
    fieldnames.extend(f"final_flag_{flag}_count" for flag in FINAL_FLAGS_TO_COUNT)
    with SUMMARY_CSV.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _markdown_table(rows):
    lines = [
        "| File | Variable | Final flag has 8 | Stage 8 + nonmissing | Stage 8 + nonmissing + final 0/1/2 | Final flag 0 | 1 | 2 | 3 | 9 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        if row["exists"] != "yes":
            continue
        lines.append(
            "| {file} | {variable} | {final_flag_has_8} | {stage_not_checked_nonmissing_count} | "
            "{published_usable_stage_not_checked_count} | {final_flag_0_count} | "
            "{final_flag_1_count} | {final_flag_2_count} | {final_flag_3_count} | "
            "{final_flag_9_count} |".format(**row)
        )
    return lines


def _write_report(rows):
    checked_rows = [row for row in rows if row["exists"] == "yes"]
    missing_files = sorted({row["file"] for row in rows if row["exists"] != "yes"})
    total_stage_nonmissing = sum(int(row["stage_not_checked_nonmissing_count"]) for row in checked_rows)
    total_published_usable = sum(int(row["published_usable_stage_not_checked_count"]) for row in checked_rows)
    any_final_flag_8 = any(row["final_flag_has_8"] == "yes" for row in checked_rows)
    final_flag_8_rows = [
        f"{row['file']}:{row['final_flag_var']}"
        for row in checked_rows
        if row["final_flag_has_8"] == "yes"
    ]

    if total_published_usable > 0:
        prevents_answer = "No. Checked release products contain stage-level not_checked=8 records that remain non-missing with final flag 0/1/2."
    elif total_stage_nonmissing > 0:
        prevents_answer = "No published usable stage-level not_checked=8 records were found in these files; this observed result alone does not prove an automatic stage-8 publication block."
    else:
        prevents_answer = "No stage-level not_checked=8 non-missing records were found in the checked files, so this cannot be inferred from the products alone."

    lines = [
        "# Stage not_checked=8 Final Publication Check",
        "",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "## Direct Answers",
        "",
        f"- Does stage-level not_checked=8 automatically prevent publication? {prevents_answer}",
        "- Are there published non-missing values with stage-level not_checked=8 and final flag 0/1/2? "
        + ("Yes" if total_published_usable > 0 else "No")
        + f" (count = {total_published_usable}).",
        "- Does any final Q_flag/SSC_flag/SSL_flag contain 8? "
        + ("Yes: " + ", ".join(final_flag_8_rows) if any_final_flag_8 else "No."),
        "",
        "## Totals",
        "",
        f"- Stage-level not_checked=8 and value non-missing records: {total_stage_nonmissing}",
        f"- Stage-level not_checked=8, value non-missing, and final flag 0/1/2 records: {total_published_usable}",
        "",
        "## Per-file Results",
        "",
    ]
    lines.extend(_markdown_table(rows))
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Matrix products are flattened from [n_stations, time] to one-dimensional arrays before counting.",
            "- Master products are counted on their one-dimensional [n_records] variables.",
            "- Value missingness excludes NaN, _FillValue, missing_value, -9999, and -127.",
            "- Final flag counts are reported over records with stage-level not_checked=8 and a non-missing value.",
        ]
    )
    if missing_files:
        lines.extend(["", "## Missing Inputs", ""])
        lines.extend(f"- {path}" for path in missing_files)
    REPORT_MD.write_text("\n".join(lines) + "\n")
    return {
        "total_stage_nonmissing": total_stage_nonmissing,
        "total_published_usable": total_published_usable,
        "any_final_flag_8": any_final_flag_8,
        "final_flag_8_rows": final_flag_8_rows,
        "missing_files": missing_files,
    }


def main() -> int:
    rows = []
    for rel_path in INPUT_FILES:
        path = BASE_DIR / rel_path
        print(f"Checking {rel_path}")
        if not path.is_file():
            print(f"  missing: {rel_path}")
            rows.extend(_missing_file_rows(rel_path))
            continue
        with xr.open_dataset(path, engine="h5netcdf", mask_and_scale=False) as ds:
            for variable in ("Q", "SSC", "SSL"):
                row = _analyze_variable(ds, rel_path, variable)
                rows.append(row)
                print(
                    "  {variable}: final_has_8={final_flag_has_8}, "
                    "stage8_nonmissing={stage_not_checked_nonmissing_count}, "
                    "published_usable={published_usable_stage_not_checked_count}".format(**row)
                )

    _write_summary(rows)
    totals = _write_report(rows)
    print("")
    print(f"Wrote summary CSV: {SUMMARY_CSV}")
    print(f"Wrote report: {REPORT_MD}")
    print("")
    print("Direct answers:")
    if totals["total_published_usable"] > 0:
        print("Does stage-level not_checked=8 automatically prevent publication? No.")
    else:
        print("Does stage-level not_checked=8 automatically prevent publication? Not demonstrated by published usable records in this check.")
    print(
        "Are there published non-missing values with stage-level not_checked=8 and final flag 0/1/2? "
        + ("Yes" if totals["total_published_usable"] > 0 else "No")
        + f" (count = {totals['total_published_usable']})."
    )
    print(
        "Does any final Q_flag/SSC_flag/SSL_flag contain 8? "
        + ("Yes: " + ", ".join(totals["final_flag_8_rows"]) if totals["any_final_flag_8"] else "No.")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
