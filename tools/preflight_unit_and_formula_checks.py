#!/usr/bin/env python3
"""Preflight checks for SSC units and SSL conversion.

Checks:
- SSC units should be mg L-1.
- Detect mg/L, g/L, kg/m3 mixing.
- Remind that g/L -> mg/L and kg/m3 -> mg/L both require x1000.
- Recompute SSL_calc = Q * SSC * 0.0864 for records with Q, SSC, SSL.
- Export records that look 10x, 0.1x, 1000x, or 0.001x off.

Outputs:
- scripts_basin_test/output/tables/table_unit_formula_check_summary.csv
- scripts_basin_test/output/tables/table_suspect_ssl_conversion_records.csv
- scripts_basin_test/output/logs/unit_formula_check.log
"""

from __future__ import annotations

import argparse
import logging
import math
import re
import sys
from collections import Counter, OrderedDict
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import netCDF4 as nc4
    from netCDF4 import num2date
except Exception:
    nc4 = None
    num2date = None

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

from pipeline_paths import (  # noqa: E402
    get_output_r_root,
    PIPELINE_OUTPUT_DIR,
    OUTPUT_LOG_DIR,
    S2_ORGANIZED_DIR,
    S3_COLLECTED_CSV,
    S5_BASIN_CLUSTERED_CSV,
    S6_MERGED_NC,
)
from qc_contract import Q_VAR_NAMES, SSC_VAR_NAMES, SSL_VAR_NAMES, TIME_VAR_NAMES  # noqa: E402

PROJECT_ROOT = get_output_r_root(SCRIPT_DIR)
TABLE_DIR = PROJECT_ROOT / PIPELINE_OUTPUT_DIR / "tables"
LOG_PATH = PROJECT_ROOT / OUTPUT_LOG_DIR / "unit_formula_check.log"
ORGANIZED_ROOT = (PROJECT_ROOT / S2_ORGANIZED_DIR).resolve()
S5_CSV = PROJECT_ROOT / S5_BASIN_CLUSTERED_CSV
S3_CSV = PROJECT_ROOT / S3_COLLECTED_CSV
S6_NC = PROJECT_ROOT / S6_MERGED_NC

SSL_FACTOR = 0.0864
FILL_VALUES = (-9999.0, -9999, -99999.0, -99999)
SUSPECTS = (
    (10.0, "ssl_10x_high", "possible 0.864 factor instead of 0.0864"),
    (0.1, "ssl_10x_low", "possible extra divide by 10"),
    (1000.0, "ssl_1000x_high", "possible g/L or kg/m3 treated as mg/L"),
    (0.001, "ssl_1000x_low", "possible extra divide/multiply by 1000"),
)
PATH_COLUMNS = ("path", "nc_path", "file_path", "source_path", "source_station_path", "source_station_paths")
META_COLUMNS = (
    "source", "source_family", "resolution", "cluster_id", "cluster_uid",
    "source_station_id", "source_station_uid", "station_name", "river_name",
)


def clean(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def setup_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("unit_formula_check")
    logger.handlers[:] = []
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    fh = logging.FileHandler(str(LOG_PATH), mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(sh)
    return logger


def unit_compact(value) -> str:
    text = clean(value).lower()
    for old, new in {
        "㎥": "m3", "³": "3", "⁻": "-", "¹": "1",
        "−": "-", "–": "-", "—": "-", "_": " ",
        "per": "/", "litre": "l", "liter": "l", "litres": "l", "liters": "l",
        "seconds": "s", "second": "s", "sec": "s", "days": "d", "day": "d",
        "tonnes": "t", "tonne": "t", "tons": "t",
    }.items():
        text = text.replace(old, new)
    text = text.replace("^", "")
    return re.sub(r"[\s\*\(\)\[\]]+", "", text)


def classify_q(units: str) -> str:
    u = unit_compact(units)
    if not u:
        return "missing"
    if "m3" in u and ("/s" in u or "s-1" in u):
        return "m3 s-1"
    return "other"


def classify_ssc(units: str) -> str:
    u = unit_compact(units)
    if not u:
        return "missing"
    if "mg" in u and ("/l" in u or "l-1" in u):
        return "mg L-1"
    if u.startswith("g") and "mg" not in u and ("/l" in u or "l-1" in u):
        return "g L-1"
    if "kg" in u and ("/m3" in u or "m-3" in u):
        return "kg m-3"
    return "other"


def classify_ssl(units: str) -> str:
    u = unit_compact(units)
    if not u:
        return "missing"
    if (u.startswith("t") or u.startswith("ton")) and ("/d" in u or "d-1" in u):
        return "t day-1"
    return "other"


def first_var(ds, names):
    for name in names:
        if name in ds.variables:
            return name
    lookup = {name.lower(): name for name in ds.variables}
    for name in names:
        if name.lower() in lookup:
            return lookup[name.lower()]
    return None


def units(ds, name):
    return clean(getattr(ds.variables[name], "units", "")) if name else ""


def values_1d(var, start, stop) -> np.ndarray:
    arr = np.ma.asarray(var[start:stop])
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    out = np.asarray(arr, dtype="float64").reshape(-1)
    for fill in FILL_VALUES:
        out[out == float(fill)] = np.nan
    return out


def format_times(time_var, start, stop, positions):
    if time_var is None or len(positions) == 0:
        return [""] * len(positions)
    try:
        if len(time_var.shape) != 1 or time_var.shape[0] < stop:
            return [""] * len(positions)
        raw = np.asarray(time_var[start:stop]).reshape(-1)[positions]
    except Exception:
        return [""] * len(positions)
    unit_text = clean(getattr(time_var, "units", ""))
    calendar = clean(getattr(time_var, "calendar", "standard")) or "standard"
    if num2date is not None and " since " in unit_text:
        try:
            decoded = num2date(raw, unit_text, calendar=calendar, only_use_cftime_datetimes=False)
        except TypeError:
            try:
                decoded = num2date(raw, unit_text, calendar=calendar)
            except Exception:
                decoded = None
        except Exception:
            decoded = None
        if decoded is not None:
            return [str(pd.Timestamp(x).date()) if not isinstance(x, str) else x for x in decoded]
    return [clean(x) for x in raw]


def split_paths(value: str):
    return [p.strip() for p in re.split(r"\s*[|;]\s*|\n+", clean(value)) if p.strip()]


def resolve_path(raw: str) -> Path:
    p = Path(clean(raw)).expanduser()
    if p.is_absolute():
        return p
    for candidate in ((ORGANIZED_ROOT / p), (PROJECT_ROOT / p), (SCRIPT_DIR / p)):
        if candidate.exists():
            return candidate.resolve()
    return (ORGANIZED_ROOT / p).resolve()


def default_input_csv() -> Path | None:
    if S5_CSV.exists():
        return S5_CSV
    if S3_CSV.exists():
        return S3_CSV
    return None


def collect_targets(input_csv: Path | None, scan_root: Path | None, include_s6: bool, logger):
    targets = OrderedDict()
    if input_csv and input_csv.exists():
        df = pd.read_csv(input_csv, keep_default_na=False)
        path_col = next((c for c in PATH_COLUMNS if c in df.columns), None)
        if path_col:
            for _, row in df.iterrows():
                for raw in split_paths(row.get(path_col, "")):
                    path = resolve_path(raw)
                    meta = {c: clean(row.get(c, "")) for c in META_COLUMNS if c in df.columns}
                    meta.update({"path": str(path), "raw_path": raw, "target_kind": "source_nc"})
                    targets.setdefault(str(path), meta)
            logger.info("Collected %d unique targets from %s", len(targets), input_csv)
    if not targets:
        root = scan_root or ORGANIZED_ROOT
        if root.exists():
            for path in sorted(root.rglob("*.nc")):
                targets.setdefault(str(path.resolve()), {"path": str(path.resolve()), "target_kind": "source_nc_scan"})
            logger.info("Collected %d targets by scanning %s", len(targets), root)
    if include_s6 and S6_NC.exists():
        targets.setdefault(str(S6_NC.resolve()), {"path": str(S6_NC.resolve()), "target_kind": "s6_merged_nc"})
        logger.info("Included s6 merged NC: %s", S6_NC)
    return list(targets.values())


def scan_file(meta: dict, args, logger):
    path = Path(meta["path"])
    file_row = {
        "path": str(path), "target_kind": meta.get("target_kind", ""),
        "source": meta.get("source", ""), "source_family": meta.get("source_family", ""),
        "resolution": meta.get("resolution", ""), "cluster_id": meta.get("cluster_id", ""),
        "cluster_uid": meta.get("cluster_uid", ""), "can_open": False,
        "q_var": "", "ssc_var": "", "ssl_var": "", "q_units": "", "ssc_units": "", "ssl_units": "",
        "q_unit_class": "missing", "ssc_unit_class": "missing", "ssl_unit_class": "missing",
        "n_records": 0, "n_formula_records": 0, "n_ratio_near_1": 0,
        "n_suspect_records": 0, "n_ssl_10x_high": 0, "n_ssl_10x_low": 0,
        "n_ssl_1000x_high": 0, "n_ssl_1000x_low": 0, "scan_note": "",
    }
    suspects = []
    ratios_sample = []
    if nc4 is None:
        file_row["scan_note"] = "netCDF4 not available"
        return file_row, suspects, ratios_sample
    if not path.exists():
        file_row["scan_note"] = "file not found"
        return file_row, suspects, ratios_sample
    try:
        with nc4.Dataset(str(path), "r") as ds:
            file_row["can_open"] = True
            qn, sn, ln = first_var(ds, Q_VAR_NAMES), first_var(ds, SSC_VAR_NAMES), first_var(ds, SSL_VAR_NAMES)
            tn = first_var(ds, TIME_VAR_NAMES)
            file_row.update({"q_var": qn or "", "ssc_var": sn or "", "ssl_var": ln or ""})
            file_row.update({"q_units": units(ds, qn), "ssc_units": units(ds, sn), "ssl_units": units(ds, ln)})
            file_row.update({
                "q_unit_class": classify_q(file_row["q_units"]),
                "ssc_unit_class": classify_ssc(file_row["ssc_units"]),
                "ssl_unit_class": classify_ssl(file_row["ssl_units"]),
            })
            if not (qn and sn and ln):
                file_row["scan_note"] = "missing Q, SSC, or SSL variable"
                return file_row, suspects, ratios_sample
            qv, sv, lv = ds.variables[qn], ds.variables[sn], ds.variables[ln]
            if not (len(qv.shape) == len(sv.shape) == len(lv.shape) == 1):
                file_row["scan_note"] = "formula check skipped because variables are not all 1-D"
                return file_row, suspects, ratios_sample
            n = int(min(qv.shape[0], sv.shape[0], lv.shape[0]))
            file_row["n_records"] = n
            tv = ds.variables[tn] if tn else None
            for start in range(0, n, args.chunk_size):
                stop = min(start + args.chunk_size, n)
                q = values_1d(qv, start, stop)
                ssc = values_1d(sv, start, stop)
                ssl = values_1d(lv, start, stop)
                size = min(len(q), len(ssc), len(ssl))
                q, ssc, ssl = q[:size], ssc[:size], ssl[:size]
                calc = q * ssc * SSL_FACTOR
                valid = np.isfinite(q) & np.isfinite(ssc) & np.isfinite(ssl) & np.isfinite(calc) & (q > 0) & (ssc > 0) & (ssl > 0) & (calc > 0)
                if not valid.any():
                    continue
                ratio = np.full(size, np.nan)
                ratio[valid] = ssl[valid] / calc[valid]
                rv = ratio[valid]
                file_row["n_formula_records"] += int(valid.sum())
                file_row["n_ratio_near_1"] += int(np.count_nonzero(np.abs(rv - 1.0) <= args.ratio_tolerance))
                if len(ratios_sample) < 5000:
                    ratios_sample.extend(rv[: 5000 - len(ratios_sample)].tolist())
                labels = {}
                suspect_mask = np.zeros(size, dtype=bool)
                for factor, label, reason in SUSPECTS:
                    mask = valid & np.isfinite(ratio) & (ratio > 0) & (np.abs(ratio / factor - 1.0) <= args.ratio_tolerance)
                    count = int(mask.sum())
                    file_row["n_{}".format(label)] += count
                    for pos in np.where(mask)[0].tolist():
                        labels.setdefault(pos, (label, factor, reason))
                    suspect_mask |= mask
                file_row["n_suspect_records"] += int(suspect_mask.sum())
                remaining = args.max_suspect_records - len(args._suspect_rows)
                if remaining <= 0:
                    continue
                positions = np.where(suspect_mask)[0][:remaining]
                times = format_times(tv, start, stop, positions)
                for pos, t in zip(positions, times):
                    label, factor, reason = labels[int(pos)]
                    suspects.append({
                        "target_kind": meta.get("target_kind", ""), "source": meta.get("source", ""),
                        "source_family": meta.get("source_family", ""), "resolution": meta.get("resolution", ""),
                        "cluster_id": meta.get("cluster_id", ""), "cluster_uid": meta.get("cluster_uid", ""),
                        "source_station_id": meta.get("source_station_id", ""), "source_station_uid": meta.get("source_station_uid", ""),
                        "station_name": meta.get("station_name", ""), "river_name": meta.get("river_name", ""),
                        "path": str(path), "record_index": start + int(pos), "time": t,
                        "Q": float(q[pos]), "SSC": float(ssc[pos]), "SSL": float(ssl[pos]),
                        "SSL_calc_0p0864": float(calc[pos]), "ratio_ssl_over_calc": float(ratio[pos]),
                        "suspect_type": label, "target_factor": factor, "reason": reason,
                        "q_units": file_row["q_units"], "ssc_units": file_row["ssc_units"], "ssl_units": file_row["ssl_units"],
                        "q_unit_class": file_row["q_unit_class"], "ssc_unit_class": file_row["ssc_unit_class"], "ssl_unit_class": file_row["ssl_unit_class"],
                    })
    except Exception as exc:
        file_row["scan_note"] = "cannot scan NetCDF: {}".format(exc)
        logger.exception("Failed scanning %s", path)
    return file_row, suspects, ratios_sample


def ratio_stats(samples):
    arr = np.asarray(samples, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return {"median": np.nan, "p05": np.nan, "p95": np.nan}
    return {"median": np.nanmedian(arr), "p05": np.nanpercentile(arr, 5), "p95": np.nanpercentile(arr, 95)}


def build_summary(file_rows, suspect_rows, ratio_samples):
    unit_counts = Counter(r.get("ssc_unit_class", "missing") for r in file_rows if r.get("ssc_var"))
    classes = {r.get("ssc_unit_class", "missing") for r in file_rows if r.get("ssc_var")}
    non_missing = {c for c in classes if c != "missing"}
    total_sus = sum(int(r.get("n_suspect_records", 0)) for r in file_rows)
    stats = ratio_stats(ratio_samples)
    rows = []

    def add(name, status, value, details):
        rows.append({"check_name": name, "status": status, "value": value, "details": details, "created_at": datetime.now().isoformat(timespec="seconds")})

    add("files_total", "INFO", len(file_rows), "NetCDF targets discovered")
    add("files_opened", "PASS" if all(r.get("can_open") for r in file_rows) else "WARN", sum(bool(r.get("can_open")) for r in file_rows), "files that could be opened")
    add("formula_records_checked", "INFO", sum(int(r.get("n_formula_records", 0)) for r in file_rows), "finite positive records with Q, SSC, SSL")
    add("ratio_median", "INFO", stats["median"], "sampled observed SSL / (Q * SSC * 0.0864)")
    add("ratio_p05", "INFO", stats["p05"], "sampled ratio p05")
    add("ratio_p95", "INFO", stats["p95"], "sampled ratio p95")
    for cls, count in sorted(unit_counts.items()):
        add("ssc_unit_class_{}".format(cls.replace(" ", "_").replace("-", "minus")), "PASS" if cls == "mg L-1" else "WARN", count, "SSC unit class counted per file")
    add("ssc_units_all_mg_L_minus1", "PASS" if classes == {"mg L-1"} else "WARN", ", ".join(sorted(classes)) or "none", "SSC should be uniformly mg L-1")
    add("ssc_mixed_units", "WARN" if len(non_missing) > 1 else "PASS", ", ".join(sorted(non_missing)) or "none", "detects kg/m3, g/L, mg/L, or other mixing")
    add("g_L_to_mg_L_requires_x1000", "WARN" if unit_counts.get("g L-1", 0) else "PASS", unit_counts.get("g L-1", 0), "1 g/L = 1000 mg/L")
    add("kg_m3_to_mg_L_requires_x1000", "WARN" if unit_counts.get("kg m-3", 0) else "PASS", unit_counts.get("kg m-3", 0), "1 kg/m3 = 1000 mg/L")
    add("ssl_formula_factor_0p0864", "WARN" if total_sus else "PASS", total_sus, "records near 10x, 0.1x, 1000x, or 0.001x off canonical SSL")
    for _, label, _ in SUSPECTS:
        count = sum(int(r.get("n_{}".format(label), 0)) for r in file_rows)
        add(label, "WARN" if count else "PASS", count, "scanned suspect records for this class")
    add("suspect_rows_written", "INFO", len(suspect_rows), "rows written to suspect CSV; may be capped by --max-suspect-records")
    return pd.DataFrame(rows)


def parse_args():
    p = argparse.ArgumentParser(description="Preflight SSC unit and SSL formula checks")
    p.add_argument("--input-csv", default=str(default_input_csv() or ""))
    p.add_argument("--scan-root", default="")
    p.add_argument("--no-include-s6", action="store_true")
    p.add_argument("--chunk-size", type=int, default=200000)
    p.add_argument("--ratio-tolerance", type=float, default=0.15)
    p.add_argument("--max-suspect-records", type=int, default=250000)
    p.add_argument("--fail-on-warn", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    args._suspect_rows = []
    logger = setup_logger()
    logger.info("Starting unit/formula preflight")
    logger.info("Canonical formula: SSL_calc = Q * SSC * %.4f", SSL_FACTOR)
    input_csv = Path(args.input_csv) if args.input_csv else None
    scan_root = Path(args.scan_root) if args.scan_root else None
    targets = collect_targets(input_csv, scan_root, not args.no_include_s6, logger)
    file_rows, ratio_samples = [], []
    for i, meta in enumerate(targets, 1):
        logger.info("[%d/%d] scanning %s", i, len(targets), meta.get("path", ""))
        row, suspects, samples = scan_file(meta, args, logger)
        file_rows.append(row)
        args._suspect_rows.extend(suspects)
        if len(ratio_samples) < 10000:
            ratio_samples.extend(samples[: 10000 - len(ratio_samples)])
        logger.info("    SSC units=%s, formula_records=%s, suspects=%s, note=%s", row.get("ssc_unit_class"), row.get("n_formula_records"), row.get("n_suspect_records"), row.get("scan_note", ""))
    summary = build_summary(file_rows, args._suspect_rows, ratio_samples)
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = TABLE_DIR / "table_unit_formula_check_summary.csv"
    suspect_path = TABLE_DIR / "table_suspect_ssl_conversion_records.csv"
    summary.to_csv(summary_path, index=False)
    pd.DataFrame(args._suspect_rows).to_csv(suspect_path, index=False)
    logger.info("Wrote %s", summary_path)
    logger.info("Wrote %s", suspect_path)
    logger.info("Wrote %s", LOG_PATH)
    if args.fail_on_warn and (summary["status"] == "WARN").any():
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
