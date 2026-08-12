#!/usr/bin/env python3
"""
Final publication-gate audit for the S9 public sediment reference release.

Designed for the final ESSD / Zenodo v1.0.0 S9 public release package.

Checks
------
1. Required release files exist.
2. S8/S9 validation reports contain no FAIL.
3. NetCDF release_version matches the expected release version.
4. No DOI / manuscript placeholders remain in publication metadata.
5. Dataset DOI is present in references/citation/metadata_link.
6. time / lat / lon coverage attributes agree with actual coordinates.
7. time contains no NaN / Inf / stored FillValue.
8. lat/lon are finite and in valid geographic ranges.
9. Q / SSC / SSL units are correct.
10. Q_flag / SSC_flag / SSL_flag definitions are correct.
11. Science variables use a real _FillValue and do not mix raw NaN with
    the canonical FillValue representation.
12. Missing science values agree with flag == 9.
13. Matrix n_valid_time_steps agrees with actual SSC-or-SSL populated cells.
14. All populated sediment cells have selected_source_station_uid.
15. All selected_source_station_uid values join to source_station_catalog.csv
    at the corresponding resolution.
16. All source_station_catalog.source_name values join to
    source_dataset_catalog.csv.
17. Matrix station/record counts agree with station_catalog.csv.
18. Public products contain no old cluster_* schema names.
19. Optional manuscript-expected statistics agree with the final files.

Outputs
-------
<output_dir>/release_publication_audit.csv
<output_dir>/release_publication_audit.md

Exit status
-----------
0 : no FAIL
1 : one or more FAIL checks
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import netCDF4 as nc4
import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from release_public_station_names import contains_old_public_schema_token

DEFAULT_RELEASE_DIR = SCRIPT_DIR / "output" / "sed_reference_release_minimal"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output" / "s9_zenodo_publication_audit"


# ---------------------------------------------------------------------
# Release structure
# ---------------------------------------------------------------------

MATRIX_FILES = {
    "daily": "sed_reference_timeseries_daily.nc",
    "monthly": "sed_reference_timeseries_monthly.nc",
    "annual": "sed_reference_timeseries_annual.nc",
}

INTEGRATED_PUBLIC_EXTENSION_FILES = [
    "sed_reference_climatology.nc",
    "climatology_catalog.csv",
    "sed_reference_satellite.nc",
    "satellite_catalog.csv",
]

COMMON_REQUIRED_FILES = [
    *MATRIX_FILES.values(),
    "station_catalog.csv",
    "source_station_catalog.csv",
    "source_dataset_catalog.csv",
    "release_validation_report.csv",
    "release_inventory.csv",
    "public_station_names_report.csv",
    "README.md",
    "example_reference_workflow.py",
    *INTEGRATED_PUBLIC_EXTENSION_FILES,
]

SCIENCE_UNITS = {
    "Q": "m3 s-1",
    "SSC": "mg L-1",
    "SSL": "t d-1",
}

FLAG_VALUES_EXPECTED = np.array([0, 1, 2, 3, 9], dtype=np.int64)
FLAG_MEANINGS_EXPECTED = "good derived suspect bad missing"

PLACEHOLDER_RE = re.compile(
    r"("
    r"<[^>]*(DOI|MANUSCRIPT|ZENODO)[^>]*>"
    r"|XXXXXXXX"
    r"|XXXX"
    r"|TODO"
    r"|TBD"
    r"|PLACEHOLDER"
    r"|zenodo\.x+"
    r")",
    flags=re.IGNORECASE,
)

DOI_RE = re.compile(
    r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b",
    flags=re.IGNORECASE,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--release-dir",
        default=str(DEFAULT_RELEASE_DIR),
        help="Final S9 public minimal release directory.",
    )

    parser.add_argument(
        "--expected-version",
        default="1.0.0",
        help="Expected release_version. Default: 1.0.0",
    )

    parser.add_argument(
        "--expected-stats",
        default="",
        help=(
            "Optional JSON file containing manuscript-ready expected "
            "station/record/time statistics."
        ),
    )

    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Audit report output directory.",
    )

    parser.add_argument(
        "--station-chunk-size",
        type=int,
        default=16,
        help="Number of matrix station rows scanned per chunk.",
    )

    parser.add_argument(
        "--allow-missing-doi",
        action="store_true",
        help=(
            "Do not fail if no DOI is found in references/citation/"
            "metadata_link. Useful before reserving the Zenodo DOI."
        ),
    )

    return parser.parse_args()


def clean_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip()

    if isinstance(value, np.generic):
        value = value.item()

    return str(value).strip()


def add_row(
    rows: List[Dict[str, Any]],
    check: str,
    status: str,
    target: str,
    details: str,
    expected: Any = "",
    actual: Any = "",
) -> None:
    rows.append(
        {
            "check": check,
            "status": status,
            "target": target,
            "expected": expected,
            "actual": actual,
            "details": details,
        }
    )


def get_attr(obj: Any, name: str, default: Any = "") -> Any:
    try:
        if hasattr(obj, "ncattrs") and name not in obj.ncattrs():
            return default
        return getattr(obj, name)
    except Exception:
        return default


def get_fill_value(var: nc4.Variable) -> Any:
    if "_FillValue" in var.ncattrs():
        return var.getncattr("_FillValue")
    if "missing_value" in var.ncattrs():
        return var.getncattr("missing_value")
    return None


def read_raw(var: nc4.Variable, key: Any = slice(None)) -> np.ndarray:
    """
    Read the stored values without netCDF4 auto masking/scaling.

    This is important for distinguishing actual stored NaN values from
    _FillValue values.
    """
    try:
        var.set_auto_maskandscale(False)
    except Exception:
        try:
            var.set_auto_mask(False)
        except Exception:
            pass

    return np.asarray(var[key])


def numeric_masks(
    raw: np.ndarray,
    fill_value: Any,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    arr = np.asarray(raw)

    try:
        values = arr.astype(np.float64, copy=False)
    except (TypeError, ValueError):
        values = np.asarray(arr, dtype=np.float64)

    nan_mask = np.isnan(values)
    inf_mask = np.isinf(values)

    fill_mask = np.zeros(values.shape, dtype=bool)

    if fill_value is not None:
        try:
            fill = float(fill_value)
            if np.isfinite(fill):
                fill_mask = values == fill
            elif np.isnan(fill):
                fill_mask = nan_mask.copy()
        except (TypeError, ValueError):
            pass

    return nan_mask, inf_mask, fill_mask


def scientific_valid_mask(
    raw: np.ndarray,
    fill_value: Any,
) -> np.ndarray:
    nan_mask, inf_mask, fill_mask = numeric_masks(raw, fill_value)
    return ~(nan_mask | inf_mask | fill_mask)


def normalize_string_array(
    raw: np.ndarray,
    expected_ndim: Optional[int] = None,
) -> np.ndarray:
    arr = np.asarray(raw)

    # Fixed-width NetCDF character array.
    if expected_ndim is not None and arr.ndim == expected_ndim + 1:
        if arr.dtype.kind in {"S", "U"}:
            try:
                arr = nc4.chartostring(arr)
            except Exception:
                pass

    if arr.dtype.kind == "S":
        arr = np.char.decode(arr, "utf-8", errors="ignore")

    if arr.dtype.kind not in {"U"}:
        arr = arr.astype(str)

    arr = np.char.strip(arr.astype("U"))

    bad = np.isin(
        np.char.lower(arr),
        ["nan", "none", "null", "--", "<na>"],
    )
    arr[bad] = ""

    return arr


def canonical_time_text(value: Any) -> str:
    text = clean_text(value)

    if not text:
        return ""

    try:
        ts = pd.Timestamp(text)
        if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
            return ts.strftime("%Y-%m-%d")
        return ts.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return text


def decode_time_value(
    value: float,
    units: str,
    calendar: str,
) -> str:
    decoded = nc4.num2date(
        value,
        units=units,
        calendar=calendar,
        only_use_cftime_datetimes=False,
        only_use_python_datetimes=False,
    )

    hour = int(getattr(decoded, "hour", 0))
    minute = int(getattr(decoded, "minute", 0))
    second = int(getattr(decoded, "second", 0))

    if hour == 0 and minute == 0 and second == 0:
        return decoded.strftime("%Y-%m-%d")

    return decoded.strftime("%Y-%m-%dT%H:%M:%S")


def first_existing_column(
    frame: pd.DataFrame,
    candidates: Tuple[str, ...],
) -> Optional[str]:
    for name in candidates:
        if name in frame.columns:
            return name
    return None


def station_uid_column(frame: pd.DataFrame) -> Optional[str]:
    return first_existing_column(frame, ("station_uid",))


def nc_station_uid_variable(ds: nc4.Dataset) -> Optional[str]:
    return "station_uid" if "station_uid" in ds.variables else None


def load_expected_stats(path: str) -> Dict[str, Any]:
    if not path:
        return {}

    p = Path(path)

    if not p.is_file():
        raise FileNotFoundError(
            f"Expected-statistics JSON does not exist: {p}"
        )

    with p.open("r", encoding="utf-8") as stream:
        result = json.load(stream)

    if not isinstance(result, dict):
        raise ValueError(
            "Expected statistics JSON must contain a JSON object."
        )

    return result


# ---------------------------------------------------------------------
# File / validation audit
# ---------------------------------------------------------------------

def audit_required_files(
    release_dir: Path,
    rows: List[Dict[str, Any]],
) -> None:
    for name in COMMON_REQUIRED_FILES:
        path = release_dir / name

        add_row(
            rows,
            check="required_file",
            status="pass" if path.is_file() else "fail",
            target=name,
            expected="exists",
            actual="exists" if path.is_file() else "missing",
            details=str(path),
        )


def audit_release_validation_report(
    release_dir: Path,
    rows: List[Dict[str, Any]],
) -> None:
    path = release_dir / "release_validation_report.csv"

    if not path.is_file():
        return

    frame = pd.read_csv(path, keep_default_na=False)

    if "status" not in frame.columns:
        add_row(
            rows,
            "s9_release_validation",
            "fail",
            path.name,
            "release_validation_report.csv has no status column",
        )
        return

    statuses = frame["status"].astype(str).str.strip().str.lower()
    failed = frame.loc[statuses.eq("fail")]

    add_row(
        rows,
        check="s9_release_validation",
        status="pass" if failed.empty else "fail",
        target=path.name,
        expected="0 fail rows",
        actual=f"{len(failed)} fail rows",
        details=(
            "All S8/S9 inherited release validation checks passed."
            if failed.empty
            else " | ".join(
                failed.head(10)
                .astype(str)
                .agg(": ".join, axis=1)
                .tolist()
            )
        ),
    )


def audit_public_station_names_report(
    release_dir: Path,
    rows: List[Dict[str, Any]],
) -> None:
    path = release_dir / "public_station_names_report.csv"

    if not path.is_file():
        add_row(
            rows,
            check="s9_public_station_names_report",
            status="fail",
            target=path.name,
            expected="exists",
            actual="missing",
            details="S9 public station-name conversion report is required.",
        )
        return

    frame = pd.read_csv(path, keep_default_na=False)

    if "status" not in frame.columns:
        add_row(
            rows,
            check="s9_public_station_names_report",
            status="fail",
            target=path.name,
            expected="status column",
            actual="missing",
            details="public_station_names_report.csv has no status column.",
        )
        return

    statuses = frame["status"].astype(str).str.strip().str.lower()
    failed = frame.loc[statuses.eq("fail")]

    add_row(
        rows,
        check="s9_public_station_names_report",
        status="pass" if failed.empty else "fail",
        target=path.name,
        expected="0 fail rows",
        actual="{} fail rows".format(len(failed)),
        details=(
            "S9 public station-name conversion report has no fail rows."
            if failed.empty
            else " | ".join(
                failed.head(10)
                .astype(str)
                .agg(": ".join, axis=1)
                .tolist()
            )
        ),
    )


# ---------------------------------------------------------------------
# Global publication metadata
# ---------------------------------------------------------------------

def audit_global_metadata(
    path: Path,
    expected_version: str,
    allow_missing_doi: bool,
    rows: List[Dict[str, Any]],
) -> None:
    with nc4.Dataset(path, "r") as ds:
        required_attrs = (
            "title",
            "summary",
            "Conventions",
            "release_version",
            "creator_name",
            "creator_institution",
            "institution",
            "references",
            "license",
        )

        for attr in required_attrs:
            value = clean_text(get_attr(ds, attr))

            add_row(
                rows,
                check=f"global_attr_{attr}",
                status="pass" if value else "fail",
                target=path.name,
                expected="non-empty",
                actual=value,
                details=f"Global attribute {attr}",
            )

        version = clean_text(get_attr(ds, "release_version"))

        add_row(
            rows,
            check="release_version",
            status="pass" if version == expected_version else "fail",
            target=path.name,
            expected=expected_version,
            actual=version,
            details="Final archived NetCDF must carry the frozen release version.",
        )

        conventions = clean_text(get_attr(ds, "Conventions"))
        conventions_ok = (
            "CF-1.8" in conventions
            and "ACDD-1.3" in conventions
        )

        add_row(
            rows,
            check="conventions",
            status="pass" if conventions_ok else "fail",
            target=path.name,
            expected="CF-1.8, ACDD-1.3",
            actual=conventions,
            details="Release NetCDF convention declaration.",
        )

        publication_fields = (
            "references",
            "citation",
            "metadata_link",
            "license",
            "publisher_url",
        )

        combined_publication_text = []

        for attr in publication_fields:
            value = clean_text(get_attr(ds, attr))

            if value:
                combined_publication_text.append(value)

            placeholder = bool(PLACEHOLDER_RE.search(value))

            add_row(
                rows,
                check=f"no_placeholder_{attr}",
                status="fail" if placeholder else "pass",
                target=path.name,
                expected="no placeholder",
                actual=value,
                details=f"Publication metadata field: {attr}",
            )

        doi_text = " ".join(combined_publication_text)
        doi_matches = DOI_RE.findall(doi_text)

        if allow_missing_doi:
            status = "pass"
            details = (
                f"DOI found: {doi_matches[0]}"
                if doi_matches
                else "No DOI found, but --allow-missing-doi was supplied."
            )
        else:
            status = "pass" if doi_matches else "fail"
            details = (
                f"DOI found: {doi_matches[0]}"
                if doi_matches
                else "No DOI found in references/citation/metadata_link."
            )

        add_row(
            rows,
            check="dataset_doi_present",
            status=status,
            target=path.name,
            expected="final dataset DOI",
            actual=doi_matches[0] if doi_matches else "",
            details=details,
        )

        naming_authority = clean_text(
            get_attr(ds, "naming_authority")
        )

        if naming_authority == "org.earth-system-science-data":
            add_row(
                rows,
                check="naming_authority",
                status="warn",
                target=path.name,
                expected="authority controlled by dataset publisher/creator",
                actual=naming_authority,
                details=(
                    "Current naming_authority points to ESSD. "
                    "Consider using an authority controlled by the "
                    "dataset-producing institution instead."
                ),
            )


# ---------------------------------------------------------------------
# Coordinate and coverage audit
# ---------------------------------------------------------------------

def audit_coordinates_and_coverage(
    path: Path,
    rows: List[Dict[str, Any]],
) -> None:
    with nc4.Dataset(path, "r") as ds:

        # -------------------------------------------------------------
        # Latitude / longitude
        # -------------------------------------------------------------
        for var_name, low, high in (
            ("lat", -90.0, 90.0),
            ("lon", -180.0, 180.0),
        ):
            if var_name not in ds.variables:
                add_row(
                    rows,
                    check=f"{var_name}_exists",
                    status="fail",
                    target=path.name,
                    expected="variable exists",
                    actual="missing",
                    details="Coordinate variable missing.",
                )
                continue

            var = ds.variables[var_name]
            raw = read_raw(var)
            fill = get_fill_value(var)

            nan_mask, inf_mask, fill_mask = numeric_masks(raw, fill)
            valid = scientific_valid_mask(raw, fill)

            values = np.asarray(raw, dtype=np.float64)

            invalid_range = (
                valid
                & ((values < low) | (values > high))
            )

            missing_count = int(
                np.count_nonzero(nan_mask | inf_mask | fill_mask)
            )

            add_row(
                rows,
                check=f"{var_name}_valid",
                status=(
                    "pass"
                    if missing_count == 0
                    and not np.any(invalid_range)
                    else "fail"
                ),
                target=path.name,
                expected=f"finite values in [{low}, {high}]",
                actual=(
                    f"missing={missing_count}; "
                    f"out_of_range={int(np.count_nonzero(invalid_range))}"
                ),
                details=f"Raw {var_name} coordinate audit.",
            )

            finite_values = values[valid]

            if finite_values.size == 0:
                continue

            actual_min = float(np.min(finite_values))
            actual_max = float(np.max(finite_values))

            attr_min_name = f"geospatial_{var_name}_min"
            attr_max_name = f"geospatial_{var_name}_max"

            attr_min = clean_text(get_attr(ds, attr_min_name))
            attr_max = clean_text(get_attr(ds, attr_max_name))

            try:
                attr_min_float = float(attr_min)
                attr_max_float = float(attr_max)
                coverage_ok = (
                    np.isclose(attr_min_float, actual_min, atol=1e-6)
                    and np.isclose(attr_max_float, actual_max, atol=1e-6)
                )
            except Exception:
                coverage_ok = False

            add_row(
                rows,
                check=f"{var_name}_coverage_metadata",
                status="pass" if coverage_ok else "fail",
                target=path.name,
                expected=f"{actual_min} .. {actual_max}",
                actual=f"{attr_min} .. {attr_max}",
                details=(
                    f"{attr_min_name}/{attr_max_name} "
                    f"versus actual {var_name} coordinate."
                ),
            )

        # -------------------------------------------------------------
        # Time
        # -------------------------------------------------------------
        if "time" not in ds.variables:
            add_row(
                rows,
                check="time_exists",
                status="fail",
                target=path.name,
                expected="time variable",
                actual="missing",
                details="NetCDF time coordinate.",
            )
            return

        time_var = ds.variables["time"]
        raw_time = read_raw(time_var)

        fill = get_fill_value(time_var)
        nan_mask, inf_mask, fill_mask = numeric_masks(raw_time, fill)

        n_nan = int(np.count_nonzero(nan_mask))
        n_inf = int(np.count_nonzero(inf_mask))
        n_fill = int(np.count_nonzero(fill_mask))

        add_row(
            rows,
            check="time_storage_valid",
            status=(
                "pass"
                if n_nan == 0 and n_inf == 0 and n_fill == 0
                else "fail"
            ),
            target=path.name,
            expected="no NaN / Inf / stored FillValue in time",
            actual=f"nan={n_nan}; inf={n_inf}; fill={n_fill}",
            details=(
                f"_FillValue={fill!r}. "
                "This check catches NC_FILL_DOUBLE-style time corruption."
            ),
        )

        units = clean_text(get_attr(time_var, "units"))
        calendar = clean_text(get_attr(time_var, "calendar"))

        add_row(
            rows,
            check="time_units",
            status="pass" if units else "fail",
            target=path.name,
            expected="non-empty CF time units",
            actual=units,
            details="Time units.",
        )

        add_row(
            rows,
            check="time_calendar",
            status="pass" if calendar else "fail",
            target=path.name,
            expected="non-empty calendar",
            actual=calendar,
            details="Time calendar.",
        )

        valid_time = np.asarray(
            raw_time[
                ~(nan_mask | inf_mask | fill_mask)
            ],
            dtype=np.float64,
        )

        if valid_time.size == 0 or not units:
            return

        calendar = calendar or "gregorian"

        actual_start = decode_time_value(
            float(np.min(valid_time)),
            units,
            calendar,
        )
        actual_end = decode_time_value(
            float(np.max(valid_time)),
            units,
            calendar,
        )

        attr_start = canonical_time_text(
            get_attr(ds, "time_coverage_start")
        )
        attr_end = canonical_time_text(
            get_attr(ds, "time_coverage_end")
        )

        start_ok = canonical_time_text(actual_start) == attr_start
        end_ok = canonical_time_text(actual_end) == attr_end

        add_row(
            rows,
            check="time_coverage_metadata",
            status="pass" if start_ok and end_ok else "fail",
            target=path.name,
            expected=f"{actual_start} .. {actual_end}",
            actual=f"{attr_start} .. {attr_end}",
            details=(
                "Global time_coverage_start/time_coverage_end "
                "versus actual time coordinate."
            ),
        )


# ---------------------------------------------------------------------
# Science variables / FillValue / QC flags
# ---------------------------------------------------------------------

def audit_science_variables(
    path: Path,
    rows: List[Dict[str, Any]],
    chunk_size: int,
) -> None:
    with nc4.Dataset(path, "r") as ds:

        for science_name, expected_unit in SCIENCE_UNITS.items():

            flag_name = f"{science_name}_flag"

            if science_name not in ds.variables:
                continue

            science_var = ds.variables[science_name]

            unit = clean_text(get_attr(science_var, "units"))

            add_row(
                rows,
                check=f"{science_name}_units",
                status="pass" if unit == expected_unit else "fail",
                target=path.name,
                expected=expected_unit,
                actual=unit,
                details=f"Units for {science_name}.",
            )

            science_fill = get_fill_value(science_var)

            fill_ok = False
            if science_fill is not None:
                try:
                    fill_ok = np.isfinite(float(science_fill))
                except Exception:
                    fill_ok = False

            add_row(
                rows,
                check=f"{science_name}_fillvalue_definition",
                status="pass" if fill_ok else "fail",
                target=path.name,
                expected="finite explicit _FillValue",
                actual=repr(science_fill),
                details=(
                    "Science variables should use one explicit numeric "
                    "missing-value representation."
                ),
            )

            if flag_name not in ds.variables:
                add_row(
                    rows,
                    check=f"{flag_name}_exists",
                    status="fail",
                    target=path.name,
                    expected="quality flag variable",
                    actual="missing",
                    details=f"Missing {flag_name}.",
                )
                continue

            flag_var = ds.variables[flag_name]

            flag_values = np.asarray(
                get_attr(flag_var, "flag_values", []),
                dtype=np.int64,
            ).reshape(-1)

            flag_meanings = clean_text(
                get_attr(flag_var, "flag_meanings")
            )

            add_row(
                rows,
                check=f"{flag_name}_flag_values",
                status=(
                    "pass"
                    if np.array_equal(
                        flag_values,
                        FLAG_VALUES_EXPECTED,
                    )
                    else "fail"
                ),
                target=path.name,
                expected=FLAG_VALUES_EXPECTED.tolist(),
                actual=flag_values.tolist(),
                details=f"flag_values for {flag_name}.",
            )

            add_row(
                rows,
                check=f"{flag_name}_flag_meanings",
                status=(
                    "pass"
                    if flag_meanings == FLAG_MEANINGS_EXPECTED
                    else "fail"
                ),
                target=path.name,
                expected=FLAG_MEANINGS_EXPECTED,
                actual=flag_meanings,
                details=f"flag_meanings for {flag_name}.",
            )

            flag_fill = get_fill_value(flag_var)

            shape = science_var.shape

            if shape != flag_var.shape:
                add_row(
                    rows,
                    check=f"{science_name}_flag_shape",
                    status="fail",
                    target=path.name,
                    expected=str(shape),
                    actual=str(flag_var.shape),
                    details=(
                        f"{science_name} and {flag_name} dimensions differ."
                    ),
                )
                continue

            total_nan = 0
            total_inf = 0
            total_fill = 0
            total_flag_nan = 0
            invalid_flag_values = 0
            missing_value_bad_flag = 0
            valid_value_missing_flag = 0

            if len(shape) == 0:
                chunks = [(slice(None),)]
            else:
                n0 = shape[0]
                chunks = [
                    (
                        slice(start, min(start + chunk_size, n0)),
                        *([slice(None)] * (len(shape) - 1)),
                    )
                    for start in range(0, n0, chunk_size)
                ]

            for key in chunks:
                science_raw = read_raw(science_var, key)
                flag_raw = read_raw(flag_var, key)

                science_values = np.asarray(
                    science_raw,
                    dtype=np.float64,
                )
                flag_values_raw = np.asarray(
                    flag_raw,
                    dtype=np.float64,
                )

                nan_mask, inf_mask, fill_mask = numeric_masks(
                    science_values,
                    science_fill,
                )

                flag_nan_mask, flag_inf_mask, flag_fill_mask = (
                    numeric_masks(
                        flag_values_raw,
                        flag_fill,
                    )
                )

                total_nan += int(np.count_nonzero(nan_mask))
                total_inf += int(np.count_nonzero(inf_mask))
                total_fill += int(np.count_nonzero(fill_mask))

                total_flag_nan += int(
                    np.count_nonzero(
                        flag_nan_mask | flag_inf_mask
                    )
                )

                flag_finite = ~(
                    flag_nan_mask | flag_inf_mask
                )

                if np.any(flag_finite):
                    flag_int = flag_values_raw[flag_finite].astype(
                        np.int64
                    )

                    invalid_flag_values += int(
                        np.count_nonzero(
                            ~np.isin(
                                flag_int,
                                FLAG_VALUES_EXPECTED,
                            )
                        )
                    )

                science_missing = (
                    nan_mask | inf_mask | fill_mask
                )

                # QC contract: flag=9 means missing.
                flag_is_missing = flag_values_raw == 9

                missing_value_bad_flag += int(
                    np.count_nonzero(
                        science_missing & ~flag_is_missing
                    )
                )

                valid_value_missing_flag += int(
                    np.count_nonzero(
                        ~science_missing & flag_is_missing
                    )
                )

            # Raw NaN should not coexist with a canonical finite FillValue.
            raw_storage_ok = (
                total_nan == 0
                and total_inf == 0
            )

            add_row(
                rows,
                check=f"{science_name}_raw_missing_storage",
                status="pass" if raw_storage_ok else "fail",
                target=path.name,
                expected=(
                    "missing values represented by _FillValue, "
                    "not raw NaN/Inf"
                ),
                actual=(
                    f"raw_nan={total_nan}; "
                    f"raw_inf={total_inf}; "
                    f"stored_fill={total_fill}"
                ),
                details=(
                    "Detects mixed NaN/_FillValue storage."
                ),
            )

            add_row(
                rows,
                check=f"{flag_name}_allowed_values",
                status=(
                    "pass"
                    if total_flag_nan == 0
                    and invalid_flag_values == 0
                    else "fail"
                ),
                target=path.name,
                expected="[0, 1, 2, 3, 9]",
                actual=(
                    f"invalid={invalid_flag_values}; "
                    f"nan_or_inf={total_flag_nan}"
                ),
                details="Stored QC flag value-domain audit.",
            )

            add_row(
                rows,
                check=f"{science_name}_flag_missing_consistency",
                status=(
                    "pass"
                    if missing_value_bad_flag == 0
                    and valid_value_missing_flag == 0
                    else "fail"
                ),
                target=path.name,
                expected=(
                    "missing science value <=> flag 9"
                ),
                actual=(
                    f"missing_value_non9_flag="
                    f"{missing_value_bad_flag}; "
                    f"valid_value_flag9="
                    f"{valid_value_missing_flag}"
                ),
                details=(
                    f"{science_name} versus {flag_name}."
                ),
            )


# ---------------------------------------------------------------------
# Matrix provenance and record counts
# ---------------------------------------------------------------------

def audit_matrix(
    release_dir: Path,
    resolution: str,
    source_station_catalog: pd.DataFrame,
    station_catalog: pd.DataFrame,
    rows: List[Dict[str, Any]],
    station_chunk_size: int,
) -> Dict[str, Any]:

    path = release_dir / MATRIX_FILES[resolution]

    if not path.is_file():
        return {}

    source_uid_col = first_existing_column(
        source_station_catalog,
        ("source_station_uid",),
    )

    matrix_stats: Dict[str, Any] = {
        "resolution": resolution,
        "stations": 0,
        "records": 0,
        "time_start": "",
        "time_end": "",
        "station_uids": set(),
    }

    with nc4.Dataset(path, "r") as ds:

        if "n_stations" not in ds.dimensions:
            add_row(
                rows,
                "matrix_n_stations_dimension",
                "fail",
                path.name,
                "n_stations dimension missing.",
            )
            return matrix_stats

        n_stations = len(ds.dimensions["n_stations"])
        matrix_stats["stations"] = n_stations

        uid_var_name = nc_station_uid_variable(ds)

        if uid_var_name:
            uid_raw = read_raw(ds.variables[uid_var_name])
            uids = normalize_string_array(uid_raw).reshape(-1)
            uid_set = {x for x in uids.tolist() if x}
            matrix_stats["station_uids"] = uid_set

            add_row(
                rows,
                check=f"{resolution}_station_uid_unique",
                status=(
                    "pass"
                    if len(uid_set) == n_stations
                    else "fail"
                ),
                target=path.name,
                expected=n_stations,
                actual=len(uid_set),
                details=(
                    f"Unique {uid_var_name} values versus n_stations."
                ),
            )
        else:
            add_row(
                rows,
                check=f"{resolution}_station_uid_variable",
                status="fail",
                target=path.name,
                expected="station_uid variable",
                actual="missing",
                details="S9 public matrix products must expose station_uid, not cluster_uid.",
            )

        if "SSC" not in ds.variables or "SSL" not in ds.variables:
            add_row(
                rows,
                check=f"{resolution}_sediment_variables",
                status="fail",
                target=path.name,
                expected="SSC and SSL",
                actual="missing",
                details="Cannot determine actual valid record count.",
            )
            return matrix_stats

        ssc_var = ds.variables["SSC"]
        ssl_var = ds.variables["SSL"]

        ssc_fill = get_fill_value(ssc_var)
        ssl_fill = get_fill_value(ssl_var)

        uid_prov_var = ds.variables.get(
            "selected_source_station_uid"
        )

        source_uids_seen: Set[str] = set()

        missing_provenance_cells = 0
        provenance_without_sediment = 0
        actual_record_count = 0

        for start in range(
            0,
            n_stations,
            station_chunk_size,
        ):
            stop = min(
                start + station_chunk_size,
                n_stations,
            )

            key = (slice(start, stop), slice(None))

            ssc_raw = read_raw(ssc_var, key)
            ssl_raw = read_raw(ssl_var, key)

            ssc_valid = scientific_valid_mask(
                ssc_raw,
                ssc_fill,
            )
            ssl_valid = scientific_valid_mask(
                ssl_raw,
                ssl_fill,
            )

            sediment_present = ssc_valid | ssl_valid

            actual_record_count += int(
                np.count_nonzero(sediment_present)
            )

            if uid_prov_var is not None:
                uid_raw = read_raw(uid_prov_var, key)

                uid_array = normalize_string_array(
                    uid_raw,
                    expected_ndim=2,
                )

                if uid_array.shape != sediment_present.shape:
                    add_row(
                        rows,
                        check=f"{resolution}_selected_uid_shape",
                        status="fail",
                        target=path.name,
                        expected=str(sediment_present.shape),
                        actual=str(uid_array.shape),
                        details=(
                            "selected_source_station_uid shape "
                            "does not match matrix cells."
                        ),
                    )
                    continue

                uid_nonempty = uid_array != ""

                missing_provenance_cells += int(
                    np.count_nonzero(
                        sediment_present & ~uid_nonempty
                    )
                )

                provenance_without_sediment += int(
                    np.count_nonzero(
                        ~sediment_present & uid_nonempty
                    )
                )

                if np.any(uid_nonempty):
                    source_uids_seen.update(
                        np.unique(
                            uid_array[uid_nonempty]
                        ).tolist()
                    )

        matrix_stats["records"] = actual_record_count

        add_row(
            rows,
            check=f"{resolution}_cell_provenance_present",
            status=(
                "pass"
                if missing_provenance_cells == 0
                else "fail"
            ),
            target=path.name,
            expected="0 sediment cells without selected_source_station_uid",
            actual=missing_provenance_cells,
            details=(
                "Every SSC/SSL output cell must retain "
                "source-station provenance."
            ),
        )

        add_row(
            rows,
            check=f"{resolution}_provenance_without_sediment",
            status=(
                "pass"
                if provenance_without_sediment == 0
                else "warn"
            ),
            target=path.name,
            expected=0,
            actual=provenance_without_sediment,
            details=(
                "Cells with provenance UID but neither SSC nor SSL."
            ),
        )

        if "n_valid_time_steps" in ds.variables:
            counts_raw = read_raw(
                ds.variables["n_valid_time_steps"]
            )
            reported_count = int(
                np.asarray(
                    counts_raw,
                    dtype=np.int64,
                ).sum()
            )

            add_row(
                rows,
                check=f"{resolution}_n_valid_time_steps",
                status=(
                    "pass"
                    if reported_count == actual_record_count
                    else "fail"
                ),
                target=path.name,
                expected=actual_record_count,
                actual=reported_count,
                details=(
                    "Sum(n_valid_time_steps) versus cells "
                    "with at least one non-missing SSC or SSL."
                ),
            )

        # -------------------------------------------------------------
        # selected_source_station_uid -> source_station_catalog
        # -------------------------------------------------------------
        if source_uid_col and source_uids_seen:

            catalog = source_station_catalog.copy()

            if "resolution" in catalog.columns:
                catalog = catalog[
                    catalog["resolution"]
                    .astype(str)
                    .str.strip()
                    .eq(resolution)
                ]

            catalog_uids = {
                clean_text(v)
                for v in catalog[source_uid_col].tolist()
                if clean_text(v)
            }

            missing_uids = sorted(
                source_uids_seen - catalog_uids
            )

            add_row(
                rows,
                check=f"{resolution}_selected_uid_catalog_join",
                status=(
                    "pass"
                    if not missing_uids
                    else "fail"
                ),
                target=path.name,
                expected="all selected source UIDs join",
                actual=(
                    f"selected_unique={len(source_uids_seen)}; "
                    f"missing={len(missing_uids)}"
                ),
                details=(
                    "sample_missing="
                    + ", ".join(missing_uids[:10])
                    if missing_uids
                    else "All selected_source_station_uid values resolved."
                ),
            )

        # -------------------------------------------------------------
        # Matrix vs station_catalog
        # -------------------------------------------------------------
        catalog = station_catalog.copy()

        if "resolution" in catalog.columns:
            catalog = catalog[
                catalog["resolution"]
                .astype(str)
                .str.strip()
                .eq(resolution)
            ]

        add_row(
            rows,
            check=f"{resolution}_station_catalog_count",
            status=(
                "pass"
                if len(catalog) == n_stations
                else "fail"
            ),
            target="station_catalog.csv",
            expected=n_stations,
            actual=len(catalog),
            details=(
                f"Catalog rows for {resolution} versus "
                "matrix n_stations."
            ),
        )

        if "record_count" in catalog.columns:
            catalog_records = int(
                pd.to_numeric(
                    catalog["record_count"],
                    errors="coerce",
                )
                .fillna(0)
                .sum()
            )

            add_row(
                rows,
                check=f"{resolution}_station_catalog_records",
                status=(
                    "pass"
                    if catalog_records == actual_record_count
                    else "fail"
                ),
                target="station_catalog.csv",
                expected=actual_record_count,
                actual=catalog_records,
                details=(
                    f"Sum(record_count) for {resolution} "
                    "versus actual SSC/SSL matrix cells."
                ),
            )

        # -------------------------------------------------------------
        # Time stats
        # -------------------------------------------------------------
        if "time" in ds.variables:
            time_var = ds.variables["time"]
            raw_time = read_raw(time_var)

            fill = get_fill_value(time_var)
            nan_mask, inf_mask, fill_mask = numeric_masks(
                raw_time,
                fill,
            )

            valid = np.asarray(
                raw_time[
                    ~(nan_mask | inf_mask | fill_mask)
                ],
                dtype=np.float64,
            )

            if valid.size:
                units = clean_text(
                    get_attr(time_var, "units")
                )
                calendar = clean_text(
                    get_attr(
                        time_var,
                        "calendar",
                        "gregorian",
                    )
                )

                matrix_stats["time_start"] = decode_time_value(
                    float(np.min(valid)),
                    units,
                    calendar,
                )

                matrix_stats["time_end"] = decode_time_value(
                    float(np.max(valid)),
                    units,
                    calendar,
                )

    return matrix_stats


# ---------------------------------------------------------------------
# Catalog joins
# ---------------------------------------------------------------------

def audit_catalogs(
    source_station_catalog: pd.DataFrame,
    source_dataset_catalog: pd.DataFrame,
    rows: List[Dict[str, Any]],
) -> None:

    if "source_name" not in source_station_catalog.columns:
        add_row(
            rows,
            "source_station_source_name",
            "fail",
            "source_station_catalog.csv",
            "source_name column missing.",
        )
        return

    if "source_name" not in source_dataset_catalog.columns:
        add_row(
            rows,
            "source_dataset_source_name",
            "fail",
            "source_dataset_catalog.csv",
            "source_name column missing.",
        )
        return

    source_dataset_names = {
        clean_text(v)
        for v in source_dataset_catalog["source_name"].tolist()
        if clean_text(v)
    }

    source_station_names = {
        clean_text(v)
        for v in source_station_catalog["source_name"].tolist()
        if clean_text(v)
    }

    missing_sources = sorted(
        source_station_names - source_dataset_names
    )

    add_row(
        rows,
        check="source_name_catalog_join",
        status=(
            "pass"
            if not missing_sources
            else "fail"
        ),
        target="source_station_catalog.csv -> source_dataset_catalog.csv",
        expected="all source_name values resolve",
        actual=(
            f"station_sources={len(source_station_names)}; "
            f"dataset_sources={len(source_dataset_names)}; "
            f"missing={len(missing_sources)}"
        ),
        details=(
            "missing="
            + ", ".join(missing_sources)
            if missing_sources
            else "All source_name values resolved."
        ),
    )

    # Practical source-station key should be unique.
    if {
        "source_station_uid",
        "resolution",
    }.issubset(source_station_catalog.columns):

        duplicated = source_station_catalog.duplicated(
            ["source_station_uid", "resolution"],
            keep=False,
        )

        n_dup = int(duplicated.sum())

        add_row(
            rows,
            check="source_station_key_unique",
            status="pass" if n_dup == 0 else "fail",
            target="source_station_catalog.csv",
            expected="unique source_station_uid + resolution",
            actual=f"duplicate_rows={n_dup}",
            details=(
                "Practical provenance key uniqueness."
            ),
        )

    duplicated_sources = source_dataset_catalog.duplicated(
        ["source_name"],
        keep=False,
    )

    n_dup_sources = int(duplicated_sources.sum())

    add_row(
        rows,
        check="source_dataset_key_unique",
        status=(
            "pass"
            if n_dup_sources == 0
            else "fail"
        ),
        target="source_dataset_catalog.csv",
        expected="unique source_name",
        actual=f"duplicate_rows={n_dup_sources}",
        details="Source-dataset primary key uniqueness.",
    )


# ---------------------------------------------------------------------
# Public S9 schema audit
# ---------------------------------------------------------------------

def audit_public_schema_csv(
    path: Path,
    rows: List[Dict[str, Any]],
) -> None:
    if path.name == "public_station_names_report.csv":
        return

    try:
        frame = pd.read_csv(path, keep_default_na=False, dtype=str)
    except Exception as exc:
        add_row(
            rows,
            check="public_schema_csv_readable",
            status="fail",
            target=path.name,
            expected="readable CSV",
            actual=repr(exc),
            details="Cannot scan CSV for old public cluster schema names.",
        )
        return

    failures = []

    for column in frame.columns:
        if contains_old_public_schema_token(column):
            failures.append("column:{}".format(column))

    for column in frame.columns:
        if frame.empty:
            continue
        mask = frame[column].map(contains_old_public_schema_token)
        if mask.any():
            failures.append(
                "values:{}={}".format(column, int(mask.sum()))
            )

    add_row(
        rows,
        check="public_schema_no_old_cluster_tokens_csv",
        status="pass" if not failures else "fail",
        target=path.name,
        expected="no old public cluster schema tokens",
        actual="; ".join(failures[:20]),
        details="CSV public schema/token scan.",
    )


def audit_public_schema_text(
    path: Path,
    rows: List[Dict[str, Any]],
) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return

    has_old = contains_old_public_schema_token(text)

    add_row(
        rows,
        check="public_schema_no_old_cluster_tokens_text",
        status="pass" if not has_old else "fail",
        target=path.name,
        expected="no old public cluster schema tokens",
        actual="old tokens present" if has_old else "",
        details="Public text metadata scan.",
    )


def audit_public_schema_netcdf(
    path: Path,
    rows: List[Dict[str, Any]],
) -> None:
    failures = []

    with nc4.Dataset(path, "r") as ds:
        for name in list(ds.dimensions):
            if contains_old_public_schema_token(name):
                failures.append("dimension:{}".format(name))
        for name in list(ds.variables):
            if contains_old_public_schema_token(name):
                failures.append("variable:{}".format(name))
        for attr_name in ds.ncattrs():
            if contains_old_public_schema_token(attr_name):
                failures.append("global_attr_name:{}".format(attr_name))
            value = get_attr(ds, attr_name)
            if isinstance(value, str) and contains_old_public_schema_token(value):
                failures.append("global_attr_value:{}".format(attr_name))
        for var_name, var in ds.variables.items():
            for attr_name in var.ncattrs():
                if contains_old_public_schema_token(attr_name):
                    failures.append(
                        "var_attr_name:{}:{}".format(var_name, attr_name)
                    )
                value = get_attr(var, attr_name)
                if isinstance(value, str) and contains_old_public_schema_token(value):
                    failures.append(
                        "var_attr_value:{}:{}".format(var_name, attr_name)
                    )

    add_row(
        rows,
        check="public_schema_no_old_cluster_tokens_netcdf",
        status="pass" if not failures else "fail",
        target=path.name,
        expected="no old public cluster schema tokens",
        actual="; ".join(failures[:20]),
        details="NetCDF public schema/token scan.",
    )


def audit_public_schema_residuals(
    release_dir: Path,
    rows: List[Dict[str, Any]],
) -> None:
    for path in sorted(release_dir.iterdir()):
        if not path.is_file():
            continue
        suffix = path.suffix.lower()
        if path.name.endswith(".csv") or path.name.endswith(".csv.gz"):
            audit_public_schema_csv(path, rows)
        elif suffix == ".nc":
            audit_public_schema_netcdf(path, rows)
        elif suffix in {".md", ".txt", ".py", ".json", ".yml", ".yaml"}:
            audit_public_schema_text(path, rows)


# ---------------------------------------------------------------------
# Manuscript / expected statistics
# ---------------------------------------------------------------------

def compare_expected(
    rows: List[Dict[str, Any]],
    check: str,
    target: str,
    expected: Any,
    actual: Any,
) -> None:
    if expected is None or expected == "":
        return

    if isinstance(expected, str) and "time_" in check:
        ok = (
            canonical_time_text(expected)
            == canonical_time_text(actual)
        )
    else:
        ok = expected == actual

    add_row(
        rows,
        check=check,
        status="pass" if ok else "fail",
        target=target,
        expected=expected,
        actual=actual,
        details="Final release versus manuscript expected statistics.",
    )


def audit_expected_statistics(
    expected: Dict[str, Any],
    matrix_stats: Dict[str, Dict[str, Any]],
    station_catalog: pd.DataFrame,
    source_dataset_catalog: pd.DataFrame,
    rows: List[Dict[str, Any]],
) -> None:

    if not expected:
        add_row(
            rows,
            check="manuscript_expected_statistics",
            status="skip",
            target="expected-stats JSON",
            expected="provided",
            actual="not provided",
            details=(
                "Use --expected-stats to make manuscript/release "
                "numerical consistency a hard release gate."
            ),
        )
        return

    for resolution in ("daily", "monthly", "annual"):
        if resolution not in matrix_stats:
            continue

        actual = matrix_stats[resolution]
        exp = expected.get(resolution, {})

        compare_expected(
            rows,
            f"manuscript_{resolution}_stations",
            resolution,
            exp.get("stations"),
            actual.get("stations"),
        )

        compare_expected(
            rows,
            f"manuscript_{resolution}_records",
            resolution,
            exp.get("records"),
            actual.get("records"),
        )

        compare_expected(
            rows,
            f"manuscript_{resolution}_time_start",
            resolution,
            exp.get("time_start"),
            actual.get("time_start"),
        )

        compare_expected(
            rows,
            f"manuscript_{resolution}_time_end",
            resolution,
            exp.get("time_end"),
            actual.get("time_end"),
        )

    all_exp = expected.get("all", {})

    all_station_uids: Set[str] = set()
    total_records = 0

    for stats in matrix_stats.values():
        all_station_uids.update(
            stats.get("station_uids", set())
        )
        total_records += int(
            stats.get("records", 0) or 0
        )

    # Fallback if matrices do not expose station_uid.
    if not all_station_uids:
        uid_col = station_uid_column(station_catalog)
        if uid_col:
            all_station_uids = {
                clean_text(v)
                for v in station_catalog[uid_col].tolist()
                if clean_text(v)
            }

    compare_expected(
        rows,
        "manuscript_all_unique_stations",
        "all matrices",
        all_exp.get("unique_stations"),
        len(all_station_uids),
    )

    compare_expected(
        rows,
        "manuscript_all_records",
        "all matrices",
        all_exp.get("records"),
        total_records,
    )

    compare_expected(
        rows,
        "manuscript_source_dataset_count",
        "source_dataset_catalog.csv",
        all_exp.get("source_datasets"),
        len(source_dataset_catalog),
    )


# ---------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------

def write_reports(
    rows: List[Dict[str, Any]],
    output_dir: Path,
) -> None:

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    frame = pd.DataFrame(rows)

    csv_path = output_dir / "release_publication_audit.csv"
    md_path = output_dir / "release_publication_audit.md"

    frame.to_csv(
        csv_path,
        index=False,
    )

    fail_count = int(
        frame["status"].astype(str).str.lower().eq("fail").sum()
    )

    warn_count = int(
        frame["status"].astype(str).str.lower().eq("warn").sum()
    )

    pass_count = int(
        frame["status"].astype(str).str.lower().eq("pass").sum()
    )

    skip_count = int(
        frame["status"].astype(str).str.lower().eq("skip").sum()
    )

    lines = [
        "# Final ESSD / Zenodo S9 Public Release Audit",
        "",
        "## Summary",
        "",
        f"- PASS: {pass_count}",
        f"- WARN: {warn_count}",
        f"- SKIP: {skip_count}",
        f"- FAIL: {fail_count}",
        "",
    ]

    if fail_count == 0:
        lines.extend(
            [
                "**Publication gate: PASS**",
                "",
                "No hard audit failures were detected.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "**Publication gate: FAIL**",
                "",
                "The release should not be frozen/uploaded until "
                "the FAIL rows are resolved.",
                "",
                "## Failures",
                "",
            ]
        )

        for _, row in frame[
            frame["status"]
            .astype(str)
            .str.lower()
            .eq("fail")
        ].iterrows():
            lines.append(
                f"- `{row['check']}` — "
                f"**{row['target']}**: "
                f"{row['details']} "
                f"(expected={row['expected']!r}; "
                f"actual={row['actual']!r})"
            )

        lines.append("")

    lines.extend(
        [
            "## Output",
            "",
            f"- `{csv_path.name}`",
            f"- `{md_path.name}`",
            "",
        ]
    )

    md_path.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print(f"Wrote: {csv_path}")
    print(f"Wrote: {md_path}")
    print(
        "Audit summary: "
        f"PASS={pass_count}, "
        f"WARN={warn_count}, "
        f"SKIP={skip_count}, "
        f"FAIL={fail_count}"
    )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    release_dir = Path(
        args.release_dir
    ).expanduser().resolve()

    output_dir = Path(
        args.output_dir
    ).expanduser().resolve()

    if not release_dir.is_dir():
        print(
            f"ERROR: release directory not found: "
            f"{release_dir}",
            file=sys.stderr,
        )
        return 1

    try:
        expected_stats = load_expected_stats(
            args.expected_stats
        )
    except Exception as exc:
        print(
            f"ERROR loading expected statistics: {exc}",
            file=sys.stderr,
        )
        return 1

    rows: List[Dict[str, Any]] = []

    # -------------------------------------------------------------
    # Required files and S9 validation reports
    # -------------------------------------------------------------
    audit_required_files(
        release_dir,
        rows,
    )

    audit_release_validation_report(
        release_dir,
        rows,
    )

    audit_public_station_names_report(
        release_dir,
        rows,
    )

    # -------------------------------------------------------------
    # Catalogs
    # -------------------------------------------------------------
    station_catalog_path = (
        release_dir / "station_catalog.csv"
    )
    source_station_catalog_path = (
        release_dir / "source_station_catalog.csv"
    )
    source_dataset_catalog_path = (
        release_dir / "source_dataset_catalog.csv"
    )

    station_catalog = (
        pd.read_csv(
            station_catalog_path,
            keep_default_na=False,
            low_memory=False,
        )
        if station_catalog_path.is_file()
        else pd.DataFrame()
    )

    source_station_catalog = (
        pd.read_csv(
            source_station_catalog_path,
            keep_default_na=False,
            low_memory=False,
        )
        if source_station_catalog_path.is_file()
        else pd.DataFrame()
    )

    source_dataset_catalog = (
        pd.read_csv(
            source_dataset_catalog_path,
            keep_default_na=False,
            low_memory=False,
        )
        if source_dataset_catalog_path.is_file()
        else pd.DataFrame()
    )

    if (
        not source_station_catalog.empty
        and not source_dataset_catalog.empty
    ):
        audit_catalogs(
            source_station_catalog,
            source_dataset_catalog,
            rows,
        )

    # -------------------------------------------------------------
    # NetCDF metadata / coordinates / science variables
    # -------------------------------------------------------------
    netcdf_files: List[Path] = []

    for name in MATRIX_FILES.values():
        path = release_dir / name
        if path.is_file():
            netcdf_files.append(path)

    for name in INTEGRATED_PUBLIC_EXTENSION_FILES:
        if not name.endswith(".nc"):
            continue
        path = release_dir / name
        if path.is_file():
            netcdf_files.append(path)

    for path in netcdf_files:
        print(f"Audit NetCDF: {path.name}")

        audit_global_metadata(
            path,
            expected_version=args.expected_version,
            allow_missing_doi=args.allow_missing_doi,
            rows=rows,
        )

        audit_coordinates_and_coverage(
            path,
            rows,
        )

        audit_science_variables(
            path,
            rows,
            chunk_size=args.station_chunk_size,
        )

    # -------------------------------------------------------------
    # Matrices: counts + provenance
    # -------------------------------------------------------------
    matrix_stats: Dict[str, Dict[str, Any]] = {}

    for resolution in (
        "daily",
        "monthly",
        "annual",
    ):
        print(
            f"Audit matrix provenance/counts: "
            f"{resolution}"
        )

        stats = audit_matrix(
            release_dir=release_dir,
            resolution=resolution,
            source_station_catalog=source_station_catalog,
            station_catalog=station_catalog,
            rows=rows,
            station_chunk_size=args.station_chunk_size,
        )

        if stats:
            matrix_stats[resolution] = stats

    # -------------------------------------------------------------
    # Manuscript / expected statistics
    # -------------------------------------------------------------
    audit_expected_statistics(
        expected=expected_stats,
        matrix_stats=matrix_stats,
        station_catalog=station_catalog,
        source_dataset_catalog=source_dataset_catalog,
        rows=rows,
    )

    # -------------------------------------------------------------
    # S9 public schema residuals
    # -------------------------------------------------------------
    audit_public_schema_residuals(
        release_dir,
        rows,
    )

    # -------------------------------------------------------------
    # Reports
    # -------------------------------------------------------------
    write_reports(
        rows,
        output_dir,
    )

    fail_count = sum(
        1
        for row in rows
        if str(row.get("status", "")).lower()
        == "fail"
    )

    return 1 if fail_count else 0


if __name__ == "__main__":
    sys.exit(main())
