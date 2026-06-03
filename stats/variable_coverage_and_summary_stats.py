#!/usr/bin/env python3
"""
Generate variable coverage tables and distribution figures for manuscript
Section 4.3 / Table 2 / Figure 6.

Default inputs:
  - scripts_basin_test/output/s6_basin_merged_all.nc
  - scripts_basin_test/output/s6_climatology_only.nc

Default outputs, relative to this stats/ directory:
  - tables/table_variable_summary_statistics.csv
  - tables/table_variable_coverage_by_resolution.csv
  - tables/table_colocated_variable_coverage.csv
  - figures/fig_Q_distribution.png
  - figures/fig_SSC_distribution.png
  - figures/fig_SSL_distribution.png

Notes:
  - "non-missing" means finite values not equal to NetCDF fill values.
  - By default, statistics are not restricted to flag == 0. Use --good-only to
    require the corresponding Q_flag / SSC_flag / SSL_flag to be 0.
  - For log10 statistics and log-scale figures, only positive values are used.
  - The basin mainline file is cluster based. The standalone climatology file is
    unclustered, so its spatial unit count represents climatology source stations.
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import shutil

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline_paths import (  # noqa: E402
    S6_MERGED_NC,
    S6_CLIMATOLOGY_NC,
    RELEASE_MASTER_NC,
    RELEASE_CLIMATOLOGY_NC,
    get_output_r_root,
)

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - checked at runtime
    nc4 = None


PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)

DEFAULT_MASTER_NC = PROJECT_ROOT / S6_MERGED_NC
DEFAULT_CLIMATOLOGY_NC = PROJECT_ROOT / S6_CLIMATOLOGY_NC
DEFAULT_RELEASE_MASTER_NC = PROJECT_ROOT / RELEASE_MASTER_NC
DEFAULT_RELEASE_CLIMATOLOGY_NC = PROJECT_ROOT / RELEASE_CLIMATOLOGY_NC
DEFAULT_TABLES_DIR = PROJECT_ROOT / "scripts_basin_test/output_other/variable_coverage_summary/tables"
DEFAULT_FIGURES_DIR = PROJECT_ROOT / "scripts_basin_test/output_other/variable_coverage_summary/figures"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "scripts_basin_test/output_other/variable_coverage_summary"

VARIABLES = ("Q", "SSC", "SSL")
FLAG_COLUMNS = {"Q": "Q_flag", "SSC": "SSC_flag", "SSL": "SSL_flag"}
RESOLUTION_ORDER = ("daily", "monthly", "annual", "climatology", "other", "all")
RESOLUTION_CODE_TO_NAME_DEFAULT = {
    0: "daily",
    1: "monthly",
    2: "annual",
    3: "climatology",
    4: "other",
}
COMBINATION_ORDER = (
    "Q only",
    "SSC only",
    "SSL only",
    "Q + SSC",
    "Q + SSL",
    "SSC + SSL",
    "Q + SSC + SSL",
)
FILL_SENTINELS = (-9999.0, -9999, -127)


OUTPUT_FILES = {
    "summary_statistics": "table_variable_summary_statistics.csv",
    "coverage_by_resolution": "table_variable_coverage_by_resolution.csv",
    "colocated_coverage": "table_colocated_variable_coverage.csv",
    "Q": "fig_Q_distribution.png",
    "SSC": "fig_SSC_distribution.png",
    "SSL": "fig_SSL_distribution.png",
}


def _clean_text(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() in ("", "nan", "none") else text


def _percent(numer, denom):
    denom = int(denom)
    if denom <= 0:
        return np.nan
    return 100.0 * float(numer) / float(denom)


def _pad_or_trim(arr, size, fill_value=np.nan):
    arr = np.asarray(arr).reshape(-1)
    if arr.size >= size:
        return arr[:size]
    if arr.size == 0:
        return np.full(size, fill_value)
    return np.concatenate([arr, np.full(size - arr.size, fill_value)])


def _attr_values(var, attr_name):
    raw = getattr(var, attr_name, None)
    if raw is None:
        return []
    arr = np.asarray(raw).reshape(-1)
    return arr.tolist()


def _replace_fill_values(arr, var=None):
    out = np.asarray(arr, dtype=np.float64).reshape(-1).copy()
    out[~np.isfinite(out)] = np.nan

    fill_values = list(FILL_SENTINELS)
    if var is not None:
        for attr_name in ("_FillValue", "missing_value"):
            fill_values.extend(_attr_values(var, attr_name))

    for fill in fill_values:
        try:
            fill_float = float(fill)
        except Exception:
            continue
        if np.isfinite(fill_float):
            out[np.isclose(out, fill_float, rtol=0.0, atol=0.0)] = np.nan
    return out


def _read_float_var(ds, name, size):
    if name not in ds.variables:
        return np.full(size, np.nan, dtype=np.float64), ""
    var = ds.variables[name]
    raw = np.ma.asarray(var[:]).reshape(-1)
    values = raw.astype(np.float64).filled(np.nan)
    values = _pad_or_trim(values, size, fill_value=np.nan)
    values = _replace_fill_values(values, var=var)
    units = _clean_text(getattr(var, "units", ""))
    return values, units


def _read_int_var(ds, name, size, default=-1):
    if name not in ds.variables:
        return np.full(size, default, dtype=np.int64)
    var = ds.variables[name]
    raw = np.ma.asarray(var[:]).reshape(-1)
    values = raw.astype(np.float64).filled(default)
    values = _pad_or_trim(values, size, fill_value=default)
    values = _replace_fill_values(values, var=var)
    values = np.where(np.isfinite(values), values, default)
    return values.astype(np.int64)


def _read_string_var(ds, name, size):
    if name not in ds.variables:
        return [""] * size
    raw = np.asarray(ds.variables[name][:], dtype=object).reshape(-1)
    values = [_clean_text(item) for item in raw]
    if len(values) >= size:
        return values[:size]
    return values + ([""] * (size - len(values)))


def _infer_n_records(ds):
    if "n_records" in ds.dimensions:
        return len(ds.dimensions["n_records"])
    for name in ("resolution", "station_index", "Q", "SSC", "SSL", "time"):
        if name in ds.variables:
            return int(np.asarray(ds.variables[name][:]).reshape(-1).size)
    raise ValueError("Could not infer n_records from NetCDF file")


def _decode_resolution_map(ds):
    mapping = dict(RESOLUTION_CODE_TO_NAME_DEFAULT)
    if "resolution" not in ds.variables:
        return mapping
    flag_meanings = _clean_text(getattr(ds.variables["resolution"], "flag_meanings", ""))
    if flag_meanings:
        labels = flag_meanings.split()
        for code, label in enumerate(labels):
            mapping[int(code)] = str(label).strip().lower()
    return mapping


def _spatial_units_from_dataset(ds, dataset_kind, station_index, n_records):
    station_index = np.asarray(station_index, dtype=np.int64).reshape(-1)
    if dataset_kind == "main" and "cluster_id" in ds.variables:
        cluster_ids = np.asarray(ds.variables["cluster_id"][:]).reshape(-1)
        out = []
        for idx in station_index:
            if 0 <= idx < cluster_ids.size:
                out.append("main:{}".format(_clean_text(cluster_ids[int(idx)])))
            else:
                out.append("main:missing")
        return out

    if dataset_kind == "climatology" and "station_uid" in ds.variables:
        n_stations = len(ds.dimensions["n_stations"]) if "n_stations" in ds.dimensions else 0
        station_uids = _read_string_var(ds, "station_uid", n_stations)
        out = []
        for idx in station_index:
            if 0 <= idx < len(station_uids) and station_uids[int(idx)]:
                out.append("climatology:{}".format(station_uids[int(idx)]))
            else:
                out.append("climatology:{}".format(int(idx)))
        return out

    prefix = dataset_kind or "dataset"
    return ["{}:{}".format(prefix, int(idx)) for idx in station_index[:n_records]]


def read_record_table(path, dataset_kind, fallback_resolution=None):
    """Read one merged/reference NetCDF file into a record-level DataFrame."""
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(str(path))

    with nc4.Dataset(path, "r") as ds:
        n_records = _infer_n_records(ds)
        station_index = _read_int_var(ds, "station_index", n_records, default=-1)

        if "resolution" in ds.variables:
            resolution_codes = _read_int_var(ds, "resolution", n_records, default=4)
            resolution_map = _decode_resolution_map(ds)
            resolution = [resolution_map.get(int(code), "other") for code in resolution_codes]
        else:
            resolution = [fallback_resolution or dataset_kind or "other"] * n_records

        spatial_units = _spatial_units_from_dataset(ds, dataset_kind, station_index, n_records)

        data = {
            "source_file_kind": [dataset_kind] * n_records,
            "source_file": [str(path)] * n_records,
            "resolution": resolution,
            "station_index": station_index,
            "spatial_unit_id": spatial_units,
        }
        units = {}
        for var_name in VARIABLES:
            values, unit = _read_float_var(ds, var_name, n_records)
            data[var_name] = values
            units[var_name] = unit

        for var_name in VARIABLES:
            flag_name = FLAG_COLUMNS[var_name]
            data[flag_name] = _read_int_var(ds, flag_name, n_records, default=9)

    df = pd.DataFrame(data)
    df["resolution"] = df["resolution"].fillna("other").astype(str).str.strip().str.lower()
    df.loc[~df["resolution"].isin(RESOLUTION_ORDER[:-1]), "resolution"] = "other"
    return df, units


def _merge_units(unit_dicts):
    merged = {}
    for var_name in VARIABLES:
        seen = []
        for unit_dict in unit_dicts:
            unit = _clean_text(unit_dict.get(var_name, ""))
            if unit and unit not in seen:
                seen.append(unit)
        merged[var_name] = " | ".join(seen)
    return merged


def add_presence_columns(df, good_only=False):
    out = df.copy()
    for var_name in VARIABLES:
        values = out[var_name].to_numpy(dtype=np.float64)
        present = np.isfinite(values)
        if good_only:
            flag_name = FLAG_COLUMNS[var_name]
            if flag_name in out.columns:
                present = present & (out[flag_name].to_numpy(dtype=np.int64) == 0)
            else:
                present = np.zeros(len(out), dtype=bool)
        out["has_{}".format(var_name)] = present
    return out


def _iter_resolution_subsets(df):
    for resolution in RESOLUTION_ORDER:
        if resolution == "all":
            sub = df
        else:
            sub = df[df["resolution"] == resolution]
        yield resolution, sub


def compute_variable_summary(df, units):
    rows = []
    for resolution, sub in _iter_resolution_subsets(df):
        for var_name in VARIABLES:
            present_col = "has_{}".format(var_name)
            present = sub[present_col].to_numpy(dtype=bool) if len(sub) else np.asarray([], dtype=bool)
            values = sub.loc[present, var_name].astype(float).to_numpy()
            values = values[np.isfinite(values)]
            n = int(values.size)
            positive = values[values > 0]
            log_values = np.log10(positive) if positive.size else np.asarray([], dtype=float)

            if n:
                p05, p25, p75, p95, p99 = np.percentile(values, [5, 25, 75, 95, 99])
                row = {
                    "resolution": resolution,
                    "variable": var_name,
                    "n_nonmissing_records": n,
                    "n_nonmissing_clusters": int(sub.loc[present, "spatial_unit_id"].nunique()),
                    "mean": float(np.mean(values)),
                    "median": float(np.median(values)),
                    "standard_deviation": float(np.std(values, ddof=1)) if n > 1 else np.nan,
                    "min": float(np.min(values)),
                    "max": float(np.max(values)),
                    "p05": float(p05),
                    "p25": float(p25),
                    "p75": float(p75),
                    "p95": float(p95),
                    "p99": float(p99),
                    "log10_mean": float(np.mean(log_values)) if log_values.size else np.nan,
                    "log10_median": float(np.median(log_values)) if log_values.size else np.nan,
                    "n_positive_for_log": int(positive.size),
                    "unit": units.get(var_name, ""),
                }
            else:
                row = {
                    "resolution": resolution,
                    "variable": var_name,
                    "n_nonmissing_records": 0,
                    "n_nonmissing_clusters": 0,
                    "mean": np.nan,
                    "median": np.nan,
                    "standard_deviation": np.nan,
                    "min": np.nan,
                    "max": np.nan,
                    "p05": np.nan,
                    "p25": np.nan,
                    "p75": np.nan,
                    "p95": np.nan,
                    "p99": np.nan,
                    "log10_mean": np.nan,
                    "log10_median": np.nan,
                    "n_positive_for_log": 0,
                    "unit": units.get(var_name, ""),
                }
            rows.append(row)
    return pd.DataFrame(rows)


def compute_coverage_by_resolution(df):
    rows = []
    for resolution, sub in _iter_resolution_subsets(df):
        n_records = int(len(sub))
        n_clusters = int(sub["spatial_unit_id"].nunique()) if n_records else 0
        row = {
            "resolution": resolution,
            "n_records_total": n_records,
            "n_clusters_total": n_clusters,
        }
        for var_name in VARIABLES:
            present_col = "has_{}".format(var_name)
            present = sub[present_col].to_numpy(dtype=bool) if n_records else np.asarray([], dtype=bool)
            n_var_records = int(present.sum())
            n_var_clusters = int(sub.loc[present, "spatial_unit_id"].nunique()) if n_records else 0
            row["{}_records".format(var_name)] = n_var_records
            row["{}_clusters".format(var_name)] = n_var_clusters
            row["{}_record_coverage_pct".format(var_name)] = _percent(n_var_records, n_records)
            row["{}_cluster_coverage_pct".format(var_name)] = _percent(n_var_clusters, n_clusters)
        rows.append(row)
    return pd.DataFrame(rows)


def compute_colocated_coverage(df):
    rows = []
    definitions = {
        "Q only": "Q present; SSC and SSL missing",
        "SSC only": "SSC present; Q and SSL missing",
        "SSL only": "SSL present; Q and SSC missing",
        "Q + SSC": "Q and SSC present; SSL may be present or missing",
        "Q + SSL": "Q and SSL present; SSC may be present or missing",
        "SSC + SSL": "SSC and SSL present; Q may be present or missing",
        "Q + SSC + SSL": "Q, SSC, and SSL all present",
    }
    combination_type = {
        "Q only": "exact",
        "SSC only": "exact",
        "SSL only": "exact",
        "Q + SSC": "inclusive_pair",
        "Q + SSL": "inclusive_pair",
        "SSC + SSL": "inclusive_pair",
        "Q + SSC + SSL": "exact_triple",
    }

    for resolution, sub in _iter_resolution_subsets(df):
        n_records = int(len(sub))
        n_clusters = int(sub["spatial_unit_id"].nunique()) if n_records else 0
        if n_records:
            q = sub["has_Q"].to_numpy(dtype=bool)
            ssc = sub["has_SSC"].to_numpy(dtype=bool)
            ssl = sub["has_SSL"].to_numpy(dtype=bool)
        else:
            q = ssc = ssl = np.asarray([], dtype=bool)
        nonempty = q | ssc | ssl
        n_nonempty_records = int(nonempty.sum())

        masks = {
            "Q only": q & ~ssc & ~ssl,
            "SSC only": ssc & ~q & ~ssl,
            "SSL only": ssl & ~q & ~ssc,
            "Q + SSC": q & ssc,
            "Q + SSL": q & ssl,
            "SSC + SSL": ssc & ssl,
            "Q + SSC + SSL": q & ssc & ssl,
        }
        for combo in COMBINATION_ORDER:
            mask = masks[combo]
            n_combo_records = int(mask.sum())
            n_combo_clusters = int(sub.loc[mask, "spatial_unit_id"].nunique()) if n_records else 0
            rows.append(
                {
                    "resolution": resolution,
                    "combination": combo,
                    "combination_type": combination_type[combo],
                    "definition": definitions[combo],
                    "n_records": n_combo_records,
                    "n_clusters": n_combo_clusters,
                    "pct_of_all_records": _percent(n_combo_records, n_records),
                    "pct_of_nonempty_records": _percent(n_combo_records, n_nonempty_records),
                    "pct_of_clusters": _percent(n_combo_clusters, n_clusters),
                }
            )
    return pd.DataFrame(rows)


def write_csv(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def _plot_values_for_variable(sub, var_name, use_log10):
    present_col = "has_{}".format(var_name)
    if present_col not in sub.columns or len(sub) == 0:
        return np.asarray([], dtype=float)
    values = sub.loc[sub[present_col], var_name].astype(float).to_numpy()
    values = values[np.isfinite(values)]
    if use_log10:
        values = values[values > 0]
        if values.size:
            values = np.log10(values)
    return values


def plot_distribution(df, var_name, units, out_path, bins=60, dpi=300, q_log_scale=False):
    use_log10 = var_name in ("SSC", "SSL") or (var_name == "Q" and q_log_scale)
    fig, ax = plt.subplots(figsize=(7.2, 4.8))

    any_data = False
    for resolution in RESOLUTION_ORDER[:-1]:
        sub = df[df["resolution"] == resolution]
        values = _plot_values_for_variable(sub, var_name, use_log10)
        if values.size == 0:
            continue
        any_data = True
        label = "{} (n={:,})".format(resolution, int(values.size))
        ax.hist(values, bins=bins, density=True, histtype="step", linewidth=1.5, label=label)

    unit = units.get(var_name, "")
    unit_part = " [{}]".format(unit) if unit else ""
    if use_log10:
        ax.set_xlabel("log10({}{})".format(var_name, unit_part))
    else:
        ax.set_xlabel("{}{}".format(var_name, unit_part))
    ax.set_ylabel("Density")
    ax.set_title("Distribution of {} by temporal resolution".format(var_name))
    ax.grid(True, linewidth=0.4, alpha=0.35)

    if any_data:
        ax.legend(frameon=False, fontsize=8)
    else:
        ax.text(
            0.5,
            0.5,
            "No valid {} values".format(var_name),
            ha="center",
            va="center",
            transform=ax.transAxes,
        )

    fig.tight_layout()
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=dpi)
    plt.close(fig)
    return out_path


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate Q/SSC/SSL variable coverage tables and distribution figures."
    )
    parser.add_argument("--master-nc", default=str(DEFAULT_MASTER_NC), help="Main basin-merged NetCDF file")
    parser.add_argument("--climatology-nc", default=str(DEFAULT_CLIMATOLOGY_NC), help="Standalone climatology NetCDF file")
    parser.add_argument("--release-inputs", action="store_true", help="Use sed_reference_release master/climatology files as inputs")
    parser.add_argument("--no-climatology", action="store_true", help="Do not include standalone climatology NetCDF")
    parser.add_argument(
        "--allow-duplicate-climatology",
        action="store_true",
        help="Include standalone climatology even if the master NetCDF already contains climatology records",
    )
    parser.add_argument("--tables-dir", default=str(DEFAULT_TABLES_DIR), help="Output directory for CSV tables")
    parser.add_argument("--figures-dir", default=str(DEFAULT_FIGURES_DIR), help="Output directory for distribution figures")
    parser.add_argument("--good-only", action="store_true", help="Count only records with the corresponding variable flag == 0")
    parser.add_argument("--q-log-scale", action="store_true", help="Plot Q distribution on log10 scale, matching SSC/SSL treatment")
    parser.add_argument("--bins", type=int, default=60, help="Histogram bin count")
    parser.add_argument("--dpi", type=int, default=300, help="Figure DPI")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    if nc4 is None:
        print("Error: netCDF4 is required. Install with: pip install netCDF4", file=sys.stderr)
        return 1

    master_nc = Path(args.master_nc)
    climatology_nc = Path(args.climatology_nc)
    if args.release_inputs:
        master_nc = DEFAULT_RELEASE_MASTER_NC
        climatology_nc = DEFAULT_RELEASE_CLIMATOLOGY_NC

    if not master_nc.is_file():
        print("Error: master NetCDF not found: {}".format(master_nc), file=sys.stderr)
        return 1

    tables_dir = Path(args.tables_dir)
    figures_dir = Path(args.figures_dir)

    record_tables = []
    unit_dicts = []

    print("Reading master NetCDF: {}".format(master_nc))
    master_df, master_units = read_record_table(master_nc, dataset_kind="main")
    record_tables.append(master_df)
    unit_dicts.append(master_units)

    master_has_climatology = bool((master_df["resolution"] == "climatology").any())
    include_climatology = not args.no_climatology

    if include_climatology:
        if not climatology_nc.is_file():
            print("Warning: climatology NetCDF not found, skipping: {}".format(climatology_nc), file=sys.stderr)
        elif master_has_climatology and not args.allow_duplicate_climatology:
            print(
                "Warning: master NetCDF already contains climatology records; "
                "skipping standalone climatology to avoid double counting. "
                "Use --allow-duplicate-climatology to include it anyway.",
                file=sys.stderr,
            )
        else:
            print("Reading climatology NetCDF: {}".format(climatology_nc))
            clim_df, clim_units = read_record_table(
                climatology_nc,
                dataset_kind="climatology",
                fallback_resolution="climatology",
            )
            record_tables.append(clim_df)
            unit_dicts.append(clim_units)

    records = pd.concat(record_tables, ignore_index=True)
    records = add_presence_columns(records, good_only=args.good_only)
    units = _merge_units(unit_dicts)

    summary_df = compute_variable_summary(records, units)
    coverage_df = compute_coverage_by_resolution(records)
    colocated_df = compute_colocated_coverage(records)

    summary_path = write_csv(summary_df, tables_dir / OUTPUT_FILES["summary_statistics"])
    coverage_path = write_csv(coverage_df, tables_dir / OUTPUT_FILES["coverage_by_resolution"])
    colocated_path = write_csv(colocated_df, tables_dir / OUTPUT_FILES["colocated_coverage"])

    figure_paths = []
    for var_name in VARIABLES:
        figure_paths.append(
            plot_distribution(
                records,
                var_name,
                units,
                figures_dir / OUTPUT_FILES[var_name],
                bins=max(1, int(args.bins)),
                dpi=max(72, int(args.dpi)),
                q_log_scale=bool(args.q_log_scale),
            )
        )

    all_coverage = coverage_df[coverage_df["resolution"] == "all"].iloc[0]
    all_colocated = colocated_df[colocated_df["resolution"] == "all"]
    q_ssc_records = int(all_colocated.loc[all_colocated["combination"] == "Q + SSC", "n_records"].iloc[0])
    complete_records = int(all_colocated.loc[all_colocated["combination"] == "Q + SSC + SSL", "n_records"].iloc[0])

    print("\nWrote tables:")
    print("  {}".format(summary_path))
    print("  {}".format(coverage_path))
    print("  {}".format(colocated_path))
    print("Wrote figures:")
    for path in figure_paths:
        print("  {}".format(path))

    print("\nAbstract-ready coverage summary (all resolutions):")
    for var_name in VARIABLES:
        print(
            "  {}: {:,} records across {:,} spatial units".format(
                var_name,
                int(all_coverage["{}_records".format(var_name)]),
                int(all_coverage["{}_clusters".format(var_name)]),
            )
        )
    print("  Q + SSC co-located records: {:,}".format(q_ssc_records))
    print("  Q + SSC + SSL complete records: {:,}".format(complete_records))

    if args.good_only:
        print("\nNote: --good-only was used; counts require the corresponding variable flag == 0.")
    else:

        print("\nNote: counts use non-missing values regardless of QC flag. Use --good-only for flag==0 counts.")

    # Copy the ESSD report markdown to the output directory
    report_src = SCRIPT_DIR / "variable_coverage_results_report_ESSD.md"
    if report_src.is_file():
        report_dst = DEFAULT_REPORT_DIR / report_src.name
        report_dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(report_src), str(report_dst))
        print("Copied report: {}".format(report_dst))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
