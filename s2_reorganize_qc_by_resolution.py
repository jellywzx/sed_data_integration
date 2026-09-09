#!/usr/bin/env python3
"""Copy QC NetCDF files into resolution-specific folders using s1 temporal-resolution results."""

import re
import shutil
import sys
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
from pipeline_paths import (
    S1_REVIEW_OVERRIDES_CSV,
    S1_REVIEW_QUEUE_CSV,
    S1_VERIFY_CSV,
    S2_CLASSIFICATION_DETAILS_CSV,
    S2_ORGANIZED_DIR,
    S2_OTHER_SUMMARY_CSV,
    S2_OTHER_DETAILS_CSV,
    RESOLUTION_DIRS,
    get_log_path,
)
from qc_contract import ensure_stage1_alias_parity
from time_resolution import (
    should_treat_annual_as_daily,
    should_treat_irregular_as_daily,
    should_treat_monthly_as_daily,
    sync_temporal_resolution_attrs,
)

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
VERIFY_CSV = S1_VERIFY_CSV
REVIEW_QUEUE_CSV = S1_REVIEW_QUEUE_CSV
REVIEW_OVERRIDES_CSV = S1_REVIEW_OVERRIDES_CSV
OUT_DIR = S2_ORGANIZED_DIR
CLASSIFICATION_DETAILS_CSV = S2_CLASSIFICATION_DETAILS_CSV
DEFAULT_WORKERS = 16
LEGACY_RESOLUTION_DIRS = ("annually_climatology", "quarterly", "single_point")

def get_source_from_path(path: str, root_dir: Path) -> str:
    """Parse a source name from a path such as daily/GloRiSe/SS/qc/file.nc."""
    try:
        p = Path(path).resolve()
        root = root_dir.resolve()
        rel = p.relative_to(root)
        parts = rel.parts
        if "qc" in parts:
            idx = parts.index("qc")
            before = parts[:idx]
            if len(before) >= 2:
                source = "_".join(before[1:])
            else:
                source = before[0] if before else "unknown"
        else:
            source = parts[0] if parts else "unknown"
        return re.sub(r"[^\w\-]", "_", source).strip("_") or "unknown"
    except Exception:
        return "unknown"


def safe_fname_part(s: str) -> str:
    """Return a filename-safe component containing only letters, digits, underscores, and hyphens."""
    return re.sub(r"[^\w\-]", "_", str(s)).strip("_") or "unknown"


def normalize_dataset_selector(value: str) -> str:
    """Normalize --dataset values across case, comma, and simple whitespace differences."""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("\\", "/")
    text = re.sub(r"\s*/\s*", "/", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip(" _/").lower()


def split_dataset_selectors(values) -> set:
    """Parse --dataset values separated by spaces or commas."""
    keep = set()
    for raw in values or []:
        for piece in str(raw).split(","):
            norm = normalize_dataset_selector(piece)
            if norm:
                keep.add(norm)
    return keep


def get_dataset_parts_from_path(path: str, root_dir: Path):
    """Extract the dataset path components relative to ROOT_DIR after the leading resolution folder."""
    try:
        p = Path(path).resolve()
        root = root_dir.resolve()
        rel = p.relative_to(root)
        parts = rel.parts
        if "qc" in parts:
            before = parts[: parts.index("qc")]
        else:
            before = parts[:-1]
        if len(before) >= 2:
            return tuple(str(x) for x in before[1:])
        if len(before) == 1:
            return (str(before[0]),)
    except Exception:
        pass
    return tuple()


def get_dataset_filter_aliases(path: str, root_dir: Path) -> set:
    """Build aliases used for --dataset matching."""
    aliases = set()
    source = get_source_from_path(path, root_dir)
    if source:
        aliases.add(normalize_dataset_selector(source))

    dataset_parts = get_dataset_parts_from_path(path, root_dir)
    if dataset_parts:
        aliases.add(normalize_dataset_selector(dataset_parts[0]))
        aliases.add(normalize_dataset_selector("/".join(dataset_parts)))
        aliases.add(normalize_dataset_selector("_".join(dataset_parts)))

    return {x for x in aliases if x}


def resolution_from_semantics(temporal_semantics: str) -> str:
    """Map s1 temporal_semantics values to s2 directory names."""
    if not temporal_semantics or not isinstance(temporal_semantics, str):
        return "other"
    d = temporal_semantics.strip().lower()
    if d == "hourly":
        return "daily"
    if d == "single_point":
        return "daily"
    if d == "quarterly":
        return "monthly"
    if d in RESOLUTION_DIRS:
        return d
    return "other"


def _read_review_queue(root_dir: Path):
    review_path = root_dir / REVIEW_QUEUE_CSV
    if not review_path.is_file():
        return pd.DataFrame(), review_path
    try:
        df = pd.read_csv(review_path, keep_default_na=False)
    except Exception as exc:
        raise SystemExit("Error: failed to read manual review queue {}: {}".format(review_path, exc))
    if len(df) == 0:
        return df, review_path
    if "review_required" in df.columns:
        mask = df["review_required"].astype(str).str.strip().str.lower().isin(("1", "true", "yes"))
        df = df[mask].copy()
    return df, review_path


def _copy_one(item):
    """Copy one file for the thread pool and return destination metadata plus any error."""
    src_path, dest_path, res_dir_name = item[:3]
    try:
        shutil.copy2(src_path, dest_path)
        return (res_dir_name, str(dest_path), None)
    except Exception as e:
        return (res_dir_name, None, (str(src_path), str(e)))


def _normalize_one(item):
    dest_path_str, sync_target_resolution, sync_reason = item[:3]
    try:
        if sync_target_resolution:
            sync_temporal_resolution_attrs(
                dest_path_str,
                target_resolution=sync_target_resolution,
                stage="s2",
                reason=sync_reason,
            )
        return (dest_path_str, None)
    except Exception as exc:
        return (dest_path_str, str(exc))

def _check_irregular(item):
    """Return whether an irregular file should be reclassified as daily."""
    idx, path_str = item
    p = Path(path_str)
    if p.is_file() and should_treat_irregular_as_daily(p):
        return (idx, True)
    return (idx, False)


def _check_monthly(item):
    """Return whether a monthly file should be downgraded to daily."""
    idx, path_str = item
    p = Path(path_str)
    if p.is_file() and should_treat_monthly_as_daily(p):
        return (idx, True)
    return (idx, False)


def _check_annual(item):
    """Return whether an annual file should be downgraded to daily."""
    idx, path_str = item
    p = Path(path_str)
    if p.is_file() and should_treat_annual_as_daily(p):
        return (idx, True)
    return (idx, False)


def _get_s2_copy_resolution(row):
    """Return the temporal resolution that should be written back to an s2 copy."""
    resolution_dir = str(row.get("resolution_dir", "") or "").strip().lower()
    if resolution_dir in ("daily", "monthly", "annual", "climatology"):
        return resolution_dir
    return ""


def _get_s2_copy_reason(row):
    raw_freq = str(row.get("raw_detected_frequency", row.get("detected_frequency", "")) or "").strip().lower()
    detected_freq = str(row.get("detected_frequency", "") or "").strip().lower()
    resolution_dir = str(row.get("resolution_dir", "") or "").strip().lower()

    if raw_freq == "irregular" and resolution_dir == "daily":
        return "s2 irregular secondary check"
    if raw_freq == "monthly" and resolution_dir == "daily":
        return "s2 monthly intra-month multi-record check"
    if raw_freq == "annual" and resolution_dir == "daily":
        return "s2 annual intra-year multi-record check"
    if raw_freq == "hourly" and resolution_dir == "daily":
        return "s2 mapped hourly to daily"
    if raw_freq == "quarterly" and resolution_dir == "monthly":
        return "s2 mapped quarterly to monthly"
    if raw_freq == "single_point" and resolution_dir == "daily":
        return "s2 mapped single_point to daily"
    if detected_freq == "annual" and resolution_dir == "climatology":
        return "s2 aligned annual metadata to climatology"
    if resolution_dir:
        return "s2 aligned organized copy to final resolution"
    return ""


def export_resolution_classification_details(classified_df: pd.DataFrame, root_dir: Path, classification_out: str):
    """Export full s2 classification details for source-file and category lookups."""
    classification_path = root_dir / classification_out
    classification_path.parent.mkdir(parents=True, exist_ok=True)

    work = classified_df.copy()
    work["source_path"] = work["path"] if "path" in work.columns else ""
    work["final_resolution_dir"] = work["resolution_dir"] if "resolution_dir" in work.columns else ""

    if "final_semantics" in work.columns:
        work["final_temporal_semantics"] = work["final_semantics"]
    elif "temporal_semantics" in work.columns:
        work["final_temporal_semantics"] = work["temporal_semantics"]
    elif "detected_frequency" in work.columns:
        work["final_temporal_semantics"] = work["detected_frequency"]
    else:
        work["final_temporal_semantics"] = ""

    desired_cols = [
        "source",
        "source_path",
        "final_resolution_dir",
        "final_temporal_semantics",
        "detected_frequency",
        "raw_detected_frequency",
        "temporal_semantics",
        "final_semantics",
        "single_point_interpretation",
        "path_resolution",
        "consistent",
        "rel_path",
        "s2_copy_status",
        "s2_attr_status",
        "s2_filename",
        "s2_dest_rel_path",
        "s2_dest_path",
        "s2_copy_error",
        "s2_attr_error",
    ]
    cols = [c for c in desired_cols if c in work.columns]
    if not cols:
        cols = list(work.columns)

    sort_cols = [c for c in ("final_resolution_dir", "source", "source_path") if c in work.columns]
    if sort_cols:
        work = work.sort_values(sort_cols)

    work[cols].fillna("").reset_index(drop=True).to_csv(classification_path, index=False)
    print("Exported full s2 classification details:")
    print(f"  details: {classification_path}")


def export_other_resolution_reports(other_df: pd.DataFrame, root_dir: Path, summary_out: str, details_out: str):
    """Export summary and detail CSV reports for the other category."""
    summary_path = root_dir / summary_out
    details_path = root_dir / details_out
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    details_path.parent.mkdir(parents=True, exist_ok=True)

    if len(other_df) == 0:
        pd.DataFrame(columns=["metric", "value"]).to_csv(summary_path, index=False)
        pd.DataFrame(columns=["path", "source", "detected_frequency", "temporal_semantics", "single_point_interpretation"]).to_csv(
            details_path, index=False
        )
        print("No data in the other directory; wrote empty reports:")
        print(f"  summary: {summary_path}")
        print(f"  details: {details_path}")
        return

    work = other_df.copy()
    if "single_point_interpretation" not in work.columns:
        work["single_point_interpretation"] = ""
    work["single_point_interpretation"] = work["single_point_interpretation"].fillna("").astype(str)

    details_cols = [
        "path",
        "source",
        "detected_frequency",
        "temporal_semantics",
        "single_point_interpretation",
        "rel_path",
        "path_resolution",
        "consistent",
    ]
    details_cols = [c for c in details_cols if c in work.columns]
    details_df = work[details_cols].sort_values(["detected_frequency", "source"]).reset_index(drop=True)

    summary_rows = [
        {"metric": "other_total_files", "value": int(len(work))},
        {"metric": "other_total_sources", "value": int(work["source"].nunique())},
    ]

    freq_counts = work["detected_frequency"].value_counts()
    for freq, cnt in freq_counts.items():
        summary_rows.append({"metric": f"detected_frequency::{freq}", "value": int(cnt)})

    single_df = work[work["detected_frequency"] == "single_point"]
    if len(single_df) > 0:
        interp_counts = single_df["single_point_interpretation"].replace("", "(empty)").value_counts()
        for interp, cnt in interp_counts.items():
            summary_rows.append({"metric": f"single_point_interpretation::{interp}", "value": int(cnt)})

    source_counts = work["source"].value_counts()
    for src, cnt in source_counts.items():
        summary_rows.append({"metric": f"source::{src}", "value": int(cnt)})

    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)
    details_df.to_csv(details_path, index=False)
    print("Exported other-category reports:")
    print(f"  summary: {summary_path}")
    print(f"  details: {details_path}")


def clear_output_resolution_dirs(out_base: Path):
    """Clear existing contents under the s2 output directory before a rerun."""
    cleared = []
    for sub in RESOLUTION_DIRS + LEGACY_RESOLUTION_DIRS:
        d = out_base / sub
        if not d.exists():
            continue
        for child in d.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except FileNotFoundError:
                continue
        cleared.append(d)
    return cleared


_LOG_TEE_ENABLED = False


def _enable_script_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    import atexit
    import sys
    from datetime import datetime

    log_path = get_log_path(SCRIPT_DIR, "{}_log.txt".format(Path(__file__).stem))
    if log_path.exists():
        try:
            log_path.unlink()
        except Exception:
            pass
    log_fp = open(log_path, "w", encoding="utf-8")
    log_fp.write("\n===== Run started {} =====\n".format(datetime.now().isoformat(timespec="seconds")))
    log_fp.flush()
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr

    class _TeeStream:
        def __init__(self, stream, log_file):
            self._stream = stream
            self._log_file = log_file

        def write(self, data):
            self._stream.write(data)
            try:
                self._log_file.write(data)
                self._log_file.flush()
            except (ValueError, OSError):
                pass

        def flush(self):
            self._stream.flush()
            try:
                self._log_file.flush()
            except (ValueError, OSError):
                pass

    def _close_log_file():
        if sys.stdout is not orig_stdout:
            sys.stdout = orig_stdout
        if sys.stderr is not orig_stderr:
            sys.stderr = orig_stderr
        try:
            log_fp.close()
        except (ValueError, OSError):
            pass

    sys.stdout = _TeeStream(sys.stdout, log_fp)
    sys.stderr = _TeeStream(sys.stderr, log_fp)
    atexit.register(_close_log_file)
    _LOG_TEE_ENABLED = True


def main():
    _enable_script_logging()
    ap = argparse.ArgumentParser(description="Copy QC NetCDF files into a new directory using temporal-resolution verification results (source_resolution_originalname)")
    ap.add_argument("--out-dir", "-o", default=OUT_DIR, help=f"New directory name relative to Output_r; default {OUT_DIR}")
    ap.add_argument("--verify-csv", default=VERIFY_CSV, help=f"Verification-results CSV path; default {VERIFY_CSV}")
    ap.add_argument(
        "--dataset",
        nargs="+",
        help="Process only selected datasets; accepts multiple values, source names, top-level folders, GloRiSe/SS-style names, and comma-separated lists",
    )
    ap.set_defaults(clear_mode="auto")
    ap.add_argument(
        "--clear-all",
        "--clear",
        dest="clear_mode",
        action="store_const",
        const="all",
        help="Clear all temporal-semantics directories under the output directory before copying. Dangerous; also applies with --dataset.",
    )
    ap.add_argument(
        "--no-clear",
        dest="clear_mode",
        action="store_const",
        const="none",
        help="Skip pre-cleaning and keep existing files in the output directory.",
    )
    ap.add_argument(
        "--workers",
        "-j",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help=f"Parallel worker count; stage 1 uses copy threads and stage 2 uses attribute-normalization processes; default {DEFAULT_WORKERS}",
    )
    ap.add_argument("--other-summary-out", default=S2_OTHER_SUMMARY_CSV, help="Output CSV for the other-category summary")
    ap.add_argument("--other-details-out", default=S2_OTHER_DETAILS_CSV, help="Output CSV for other-category details")
    ap.add_argument(
        "--classification-out",
        default=CLASSIFICATION_DETAILS_CSV,
        help="Output CSV for full s2 final classification details",
    )
    ap.add_argument(
        "--csv-only",
        "--no-copy",
        action="store_true",
        help="Only export the final classification CSV and other reports; do not clear outputs, copy NetCDF files, or write attributes. Copying is the default when omitted.",
    )
    args = ap.parse_args()

    root_dir = Path(ROOT_DIR).resolve()
    if not root_dir.is_dir():
        print(f"Error: root directory does not exist: {root_dir}", file=sys.stderr)
        sys.exit(1)

    ensure_stage1_alias_parity()

    review_queue, review_path = _read_review_queue(root_dir)
    if len(review_queue) > 0:
        overrides_path = root_dir / REVIEW_OVERRIDES_CSV
        print("Error: unresolved temporal-semantics conflicts remain; s2 is blocked.", file=sys.stderr)
        print("Resolve the manual review queue first: {}".format(review_path), file=sys.stderr)
        print("Write resolved decisions to the override file after review: {}".format(overrides_path), file=sys.stderr)
        sys.exit(1)

    verify_path = root_dir / args.verify_csv
    if not verify_path.is_file():
        print(f"Error: verification results not found: {verify_path}; run s1_verify_time_resolution.py first", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(verify_path)
    for col in ("path", "detected_frequency"):
        if col not in df.columns:
            print(f"Error: CSV is missing column {col}", file=sys.stderr)
            sys.exit(1)

    def is_qc_path(path_str):
        if pd.isna(path_str):
            return False
        parts = Path(path_str).parts
        return "qc" in parts

    df = df[df["path"].apply(is_qc_path)].copy()
    df["source"] = df["path"].apply(lambda p: get_source_from_path(p, root_dir))

    if args.dataset:
        keep = split_dataset_selectors(args.dataset)
        if not keep:
            print("Error: --dataset did not resolve to any valid dataset name.", file=sys.stderr)
            sys.exit(1)

        dataset_aliases = df["path"].apply(lambda p: get_dataset_filter_aliases(p, root_dir))
        mask = dataset_aliases.apply(lambda aliases: bool(aliases & keep))
        df = df[mask].copy()

        print("Filtering by dataset: {}".format(", ".join(sorted(keep))))
        print(f"Matched QC file count: {len(df)}")

        if len(df) == 0:
            available_aliases = sorted(alias for alias in dataset_aliases.explode().dropna().astype(str).unique())
            preview = ", ".join(available_aliases[:20]) if available_aliases else "(no available datasets)"
            print(
                "Error: --dataset did not match any QC files. Try a top-level folder, source name, or a GloRiSe/SS-style value.",
                file=sys.stderr,
            )
            print(f"Example available filter names: {preview}", file=sys.stderr)
            sys.exit(1)

        matched_sources = sorted(df["source"].dropna().astype(str).unique())
        preview = ", ".join(matched_sources[:20])
        print(f"Matched sources: {preview}")
        if len(matched_sources) > 20:
            print(f"  ... {len(matched_sources)} sources total")

    if "final_semantics" in df.columns:
        df["resolution_dir"] = df["final_semantics"].apply(resolution_from_semantics)
    elif "temporal_semantics" in df.columns:
        df["resolution_dir"] = df["temporal_semantics"].apply(resolution_from_semantics)
    else:
        df["resolution_dir"] = df["detected_frequency"].apply(resolution_from_semantics)

    # irregular_mask = df["resolution_dir"].astype(str).str.strip().str.lower() == "irregular"

    if "final_semantics" in df.columns:
        irregular_mask = df["final_semantics"].astype(str).str.strip().str.lower() == "irregular"
    elif "temporal_semantics" in df.columns:
        irregular_mask = df["temporal_semantics"].astype(str).str.strip().str.lower() == "irregular"
    else:
        irregular_mask = df["detected_frequency"].astype(str).str.strip().str.lower() == "irregular"

    workers = max(1, int(args.workers))

    irregular_idx = df[irregular_mask].index.tolist()
    n_irregular_to_daily = 0
    if irregular_idx:
        irregular_items = [(idx, df.at[idx, "path"]) for idx in irregular_idx]
        if workers == 1:
            for item in tqdm(irregular_items, desc="Classifying irregular -> daily", unit="file"):
                idx, should_be_daily = _check_irregular(item)
                if should_be_daily:
                    df.at[idx, "resolution_dir"] = "daily"
                    n_irregular_to_daily += 1
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_check_irregular, item): item for item in irregular_items}
                for fut in tqdm(as_completed(futures), total=len(futures), desc="Classifying irregular -> daily", unit="file"):
                    idx, should_be_daily = fut.result()
                    if should_be_daily:
                        df.at[idx, "resolution_dir"] = "daily"
                        n_irregular_to_daily += 1

    if n_irregular_to_daily > 0:
        print(f"Second-pass irregular-to-daily reclassification: {n_irregular_to_daily} files")

    monthly_mask = df["resolution_dir"].astype(str).str.strip().str.lower() == "monthly"
    monthly_idx = df[monthly_mask].index.tolist()
    n_monthly_to_daily = 0
    if monthly_idx:
        monthly_items = [(idx, df.at[idx, "path"]) for idx in monthly_idx]
        if workers == 1:
            for item in tqdm(monthly_items, desc="Classifying monthly -> daily", unit="file"):
                idx, should_be_daily = _check_monthly(item)
                if should_be_daily:
                    df.at[idx, "resolution_dir"] = "daily"
                    n_monthly_to_daily += 1
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_check_monthly, item): item for item in monthly_items}
                for fut in tqdm(as_completed(futures), total=len(futures), desc="Classifying monthly -> daily", unit="file"):
                    idx, should_be_daily = fut.result()
                    if should_be_daily:
                        df.at[idx, "resolution_dir"] = "daily"
                        n_monthly_to_daily += 1

    if n_monthly_to_daily > 0:
        print(
            "Monthly files downgraded to daily because of multiple within-month observations: {} files "
            "(2+ non-missing SSC/SSL observation dates within the same month)".format(n_monthly_to_daily)
        )
    # -----------------------------------------------------------------

    annual_mask = df["resolution_dir"].astype(str).str.strip().str.lower() == "annual"
    annual_idx = df[annual_mask].index.tolist()
    n_annual_to_daily = 0
    if annual_idx:
        annual_items = [(idx, df.at[idx, "path"]) for idx in annual_idx]
        if workers == 1:
            for item in tqdm(annual_items, desc="Classifying annual -> daily", unit="file"):
                idx, should_be_daily = _check_annual(item)
                if should_be_daily:
                    df.at[idx, "resolution_dir"] = "daily"
                    n_annual_to_daily += 1
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_check_annual, item): item for item in annual_items}
                for fut in tqdm(as_completed(futures), total=len(futures), desc="Classifying annual -> daily", unit="file"):
                    idx, should_be_daily = fut.result()
                    if should_be_daily:
                        df.at[idx, "resolution_dir"] = "daily"
                        n_annual_to_daily += 1

    if n_annual_to_daily > 0:
        print(
            "Annual files downgraded to daily because of multiple within-year observations: {} files "
            "(2+ non-missing SSC/SSL observation dates within the same year)".format(n_annual_to_daily)
        )
    # -----------------------------------------------------------------

    out_base = root_dir / args.out_dir

    df["stem"] = df["path"].apply(lambda p: Path(p).stem)
    df["safe_source"] = df["source"].apply(safe_fname_part)
    df["safe_stem"] = df["stem"].apply(safe_fname_part)
    df["s2_copy_status"] = "pending"
    df["s2_attr_status"] = ""
    df["s2_filename"] = ""
    df["s2_dest_rel_path"] = ""
    df["s2_dest_path"] = ""
    df["s2_copy_error"] = ""
    df["s2_attr_error"] = ""

    used = {}
    for r in RESOLUTION_DIRS:
        used[r] = set()

    copied = {r: 0 for r in RESOLUTION_DIRS}
    skipped = 0
    tasks = []  # (src_path, dest_path, res_dir_name, sync_target_resolution, sync_reason, row_index)

    for row_index, row in df.iterrows():
        src_path = Path(row["path"])
        if not src_path.is_file():
            skipped += 1
            df.at[row_index, "s2_copy_status"] = "missing_source"
            continue
        res_dir_name = row["resolution_dir"]
        res_dir = out_base / res_dir_name
        base = f"{row['safe_source']}_{res_dir_name}_{row['safe_stem']}"
        base_candidate = base
        idx = 2
        while base_candidate in used[res_dir_name]:
            base_candidate = f"{base}_{idx}"
            idx += 1
        used[res_dir_name].add(base_candidate)
        dest_path = res_dir / (base_candidate + ".nc")
        dest_path_abs = dest_path.resolve()
        out_base_abs = out_base.resolve()
        try:
            dest_rel_path = str(dest_path_abs.relative_to(out_base_abs))
        except ValueError:
            dest_rel_path = str(dest_path)
        df.at[row_index, "s2_copy_status"] = "scheduled"
        df.at[row_index, "s2_filename"] = dest_path.name
        df.at[row_index, "s2_dest_rel_path"] = dest_rel_path
        df.at[row_index, "s2_dest_path"] = str(dest_path_abs)
        tasks.append(
            (
                src_path,
                dest_path,
                res_dir_name,
                _get_s2_copy_resolution(row),
                _get_s2_copy_reason(row),
                row_index,
            )
        )

    if args.csv_only:
        df.loc[df["s2_copy_status"] == "scheduled", "s2_copy_status"] = "csv_only"
        print("\nCSV-only mode: final classification is complete; exporting CSV only without clearing outputs, copying NetCDF files, or writing attributes.")
        print(f"Planned output directory: {out_base}")
        print(f"Classified QC NetCDF count: {len(df)} (missing source files: {skipped})")
        export_resolution_classification_details(df, root_dir, args.classification_out)
        other_df = df[df["resolution_dir"] == "other"]
        export_other_resolution_reports(other_df, root_dir, args.other_summary_out, args.other_details_out)
        sys.exit(0)

    for sub in RESOLUTION_DIRS:
        (out_base / sub).mkdir(parents=True, exist_ok=True)
    for legacy_sub in LEGACY_RESOLUTION_DIRS:
        legacy_dir = out_base / legacy_sub
        if legacy_dir.exists():
            legacy_dir.mkdir(parents=True, exist_ok=True)

    dataset_mode = bool(args.dataset)
    if args.clear_mode == "all":
        should_clear = True
    elif args.clear_mode == "none":
        should_clear = False
    else:
        should_clear = not dataset_mode

    if dataset_mode and args.clear_mode == "auto":
        print("Detected --dataset; pre-cleaning is skipped by default. Pass --clear-all explicitly to clear everything.")

    if should_clear:
        cleared_dirs = clear_output_resolution_dirs(out_base)
        if cleared_dirs:
            print("Cleared output directory before run:")
            for d in cleared_dirs:
                print(f"  {d}")
        else:
            print("No pre-run output-directory contents found to clear.")
    else:
        if args.clear_mode == "none":
            print("Skipped output pre-cleaning (--no-clear).")
        else:
            print("Skipped output pre-cleaning (auto mode).")

    copy_errors = []
    attr_errors = []
    normalize_tasks = []

    print("\nStage 1: parallel copy")

    if workers == 1:
        for item in tqdm(tasks, desc="Copying files", unit="file"):
            res_dir_name, dest_path_str, err = _copy_one(item)
            row_index = item[5]
            if err:
                copy_errors.append(err)
                df.at[row_index, "s2_copy_status"] = "copy_failed"
                df.at[row_index, "s2_copy_error"] = err[1]
            else:
                copied[res_dir_name] += 1
                df.at[row_index, "s2_copy_status"] = "copied"
                df.at[row_index, "s2_attr_status"] = "queued"
                normalize_tasks.append(
                    (
                        dest_path_str,
                        item[3],
                        item[4],
                        row_index,
                    )
                )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_item = {}
            for item in tasks:
                fut = executor.submit(_copy_one, item)
                future_to_item[fut] = item
            for fut in tqdm(as_completed(future_to_item), total=len(future_to_item), desc="Parallel copy", unit="file"):
                res_dir_name, dest_path_str, err = fut.result()
                item = future_to_item[fut]
                row_index = item[5]
                if err:
                    copy_errors.append(err)
                    df.at[row_index, "s2_copy_status"] = "copy_failed"
                    df.at[row_index, "s2_copy_error"] = err[1]
                else:
                    copied[res_dir_name] += 1
                    df.at[row_index, "s2_copy_status"] = "copied"
                    df.at[row_index, "s2_attr_status"] = "queued"
                    normalize_tasks.append(
                        (
                            dest_path_str,
                            item[3],
                            item[4],
                            row_index,
                        )
                    )

    print(f"New directory: {out_base}")
    print(f"Processed QC NetCDF count: {len(df)} (missing skipped: {skipped})")
    for r in RESOLUTION_DIRS:
        print(f"  {r}: {copied[r]} files")
    if copy_errors:
        print(f"Copy failed for {len(copy_errors)} files:")
        for p, e in copy_errors[:10]:
            print(f"  {p} -> {e}")
        if len(copy_errors) > 10:
            print(f"  ... {len(copy_errors)} total")
    else:
        print("All files copied.")

    if normalize_tasks:
        print("\nStage 2: write temporal-resolution attributes to copies and normalize global attributes in parallel")
        if workers == 1:
            for item in tqdm(normalize_tasks, desc="Normalizing attributes", unit="file"):
                _, err = _normalize_one(item)
                row_index = item[3]
                if err:
                    attr_errors.append((item[0], err))
                    df.at[row_index, "s2_attr_status"] = "failed"
                    df.at[row_index, "s2_attr_error"] = err
                else:
                    df.at[row_index, "s2_attr_status"] = "ok"
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                future_to_item = {executor.submit(_normalize_one, item): item for item in normalize_tasks}
                for fut in tqdm(as_completed(future_to_item), total=len(future_to_item), desc="Parallel attribute normalization", unit="file"):
                    item = future_to_item[fut]
                    row_index = item[3]
                    dest_path_str, err = fut.result()
                    if err:
                        attr_errors.append((dest_path_str, err))
                        df.at[row_index, "s2_attr_status"] = "failed"
                        df.at[row_index, "s2_attr_error"] = err
                    else:
                        df.at[row_index, "s2_attr_status"] = "ok"

        if attr_errors:
            print(f"[s2] WARNING: attribute normalization failed for {len(attr_errors)} files:")
            for p, e in attr_errors[:10]:
                print(f"  {p} -> {e}")
            if len(attr_errors) > 10:
                print(f"  ... {len(attr_errors)} total")
        else:
            print("All file attributes normalized.")
    else:
        print("\nStage 2: no copied files; skipping attribute normalization.")

    export_resolution_classification_details(df, root_dir, args.classification_out)

    other_df = df[df["resolution_dir"] == "other"]
    if len(other_df) > 0:
        print("\n--- Contents of the other directory (files not assigned to standard temporal-semantics directories)---")
        print("Counts by detected_frequency:")
        for freq, cnt in other_df["detected_frequency"].value_counts().items():
            print(f"  {freq}: {cnt} files")
        single_in_other = other_df[other_df["detected_frequency"] == "single_point"]
        if len(single_in_other) > 0 and "single_point_interpretation" in other_df.columns:
            print("\nsingle_point_interpretation counts remaining in other (top 15):")
            interp = single_in_other["single_point_interpretation"].fillna("").astype(str)
            for val, c in interp.value_counts().head(15).items():
                print(f"  {val or '(empty)'}: {c} files")
        print("---")
    export_other_resolution_reports(other_df, root_dir, args.other_summary_out, args.other_details_out)

    sys.exit(1 if copy_errors else 0)


if __name__ == "__main__":
    main()
