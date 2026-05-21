#!/usr/bin/env python3
"""Fast satellite / in-situ validation using CSV sidecars.

This entrypoint keeps the original ``s11_satellite_insitu_validation.py`` intact
and avoids rescanning ``sed_reference_satellite_validation.nc`` when the release
contains ``sed_reference_satellite_candidates.csv.gz``.  It reads:

1. ``sed_reference_overlap_candidates.csv.gz`` for in-situ / non-satellite
   candidate-level observations, ideally generated in ``all-candidates`` mode.
2. ``sed_reference_satellite_candidates.csv.gz`` for satellite validation-only
   observations, generated once by ``s6_export_satellite_validation_candidates_csv.py``.

If the satellite CSV is missing, this script can fall back to the original NC
chunk scanner unless ``--no-nc-fallback`` is set.
"""

import argparse
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pandas as pd

import s11_satellite_insitu_validation as base


SATELLITE_CANDIDATE_SIDECAR_FILES = (
    "sed_reference_satellite_candidates.parquet",
    "sed_reference_satellite_candidates.csv.gz",
)


def _find_sidecar(release_dir: Path, names) -> Optional[Path]:
    for name in names:
        path = release_dir / name
        if path.exists():
            return path
    return None


def load_satellite_candidate_sidecar(
    release_dir: Path,
    explicit_path: Optional[Path] = None,
    progress=base.log_progress,
) -> Tuple[pd.DataFrame, Optional[Path], str]:
    if explicit_path is not None:
        paths = [explicit_path]
    else:
        paths = [release_dir / name for name in SATELLITE_CANDIDATE_SIDECAR_FILES]
    for path in paths:
        if path.exists():
            if progress:
                progress("Reading satellite candidate sidecar: {}".format(path))
            return base._read_table(path), path, "satellite_candidate_sidecar"
    return pd.DataFrame(), None, "satellite candidate sidecar not found"


def _filter_satellite_rows_to_candidate_windows(
    satellite_rows: pd.DataFrame,
    candidate_rows: pd.DataFrame,
    windows: Sequence[str],
    progress=base.log_progress,
) -> pd.DataFrame:
    """Keep only satellite rows near candidate sidecar keys and time windows."""
    if satellite_rows.empty or candidate_rows.empty:
        return satellite_rows.iloc[0:0].copy()

    candidate_windows, candidate_key_count = base._build_satellite_candidate_windows(
        candidate_rows,
        windows=windows,
        satellite_time_units="days since 1970-01-01",
    )
    if not candidate_windows:
        if progress:
            progress("Satellite candidate CSV filtering skipped: no candidate windows")
        return satellite_rows.iloc[0:0].copy()

    work = satellite_rows.copy()
    work["_cluster_id_numeric"] = pd.to_numeric(work.get("cluster_id", pd.Series(index=work.index)), errors="coerce")
    work["_resolution_norm"] = work.get("resolution", "").map(base._normalize_resolution)
    work["_time_day"] = base._coerce_datetime_from_columns(work)

    keep = []
    for _, row in work.iterrows():
        cluster_id = row.get("_cluster_id_numeric")
        if pd.isna(cluster_id):
            keep.append(False)
            continue
        key = (int(cluster_id), base._normalize_resolution(row.get("_resolution_norm", "")))
        window = candidate_windows.get(key)
        if not window:
            keep.append(False)
            continue
        time_value = row.get("_time_day")
        if pd.isna(time_value):
            keep.append(False)
            continue
        numeric_day = base._datetime_to_cf_days(
            pd.Series([time_value]),
            "days since 1970-01-01",
        ).iloc[0]
        keep.append(
            pd.notna(numeric_day)
            and float(window["start_day"]) <= float(numeric_day) <= float(window["end_day"])
        )

    filtered = work[pd.Series(keep, index=work.index)].copy()
    filtered = filtered.drop(columns=["_cluster_id_numeric", "_resolution_norm", "_time_day"], errors="ignore")
    if progress:
        progress(
            "Satellite candidate CSV filtering: candidate_keys={}, input_rows={}, kept_rows={}".format(
                candidate_key_count,
                len(satellite_rows),
                len(filtered),
            )
        )
    return filtered.reset_index(drop=True)


def run_validation_fast(
    release_dir: Path,
    out_dir: Path,
    candidate_sidecar: Optional[Path] = None,
    satellite_candidate_sidecar: Optional[Path] = None,
    source_taxonomy_csv: Optional[Path] = None,
    external_attributes_csv: Optional[Path] = None,
    allow_master_fallback: bool = True,
    allow_nc_fallback: bool = True,
    windows: Sequence[str] = ("exact", "pm1d", "pm2d"),
    high_turbidity_ssc: float = base.DEFAULT_HIGH_TURBIDITY_SSC,
    ssc_bin_edges: Sequence[float] = base.DEFAULT_SSC_BIN_EDGES,
    figure_variables: Sequence[str] = ("SSC",),
    write_plots: bool = True,
    workers: int = base.DEFAULT_WORKERS,
    satellite_chunk_size: int = base.DEFAULT_SATELLITE_CHUNK_SIZE,
    progress=base.log_progress,
) -> None:
    release_dir = release_dir.resolve()
    out_dir = out_dir.resolve()
    if not release_dir.exists() or not release_dir.is_dir():
        raise SystemExit("release-dir does not exist or is not a directory: {}".format(release_dir))
    unknown_windows = [window for window in windows if window not in base.WINDOW_DAYS]
    if unknown_windows:
        raise SystemExit("unknown pairing windows: {}".format(", ".join(unknown_windows)))

    if progress:
        progress("Starting fast s11 satellite / in-situ validation")
        progress("Release dir: {}".format(release_dir))
        progress("Output dir: {}".format(out_dir))
    out_dir.mkdir(parents=True, exist_ok=True)

    taxonomy = base.load_source_taxonomy(source_taxonomy_csv)
    external_attrs = base._load_external_attributes(external_attributes_csv)

    input_path = base._find_candidate_sidecar(release_dir, candidate_sidecar)
    raw = pd.DataFrame()
    load_note = ""
    input_mode = ""
    if input_path is not None:
        raw, input_path, input_mode = base.load_observations_from_candidate_sidecar(
            release_dir,
            input_path,
            progress=progress,
        )
        load_note = "candidate sidecar loaded"
    if raw.empty:
        if candidate_sidecar is not None and not allow_master_fallback:
            raise SystemExit("candidate sidecar not found or empty: {}".format(candidate_sidecar))
        if not allow_master_fallback:
            raise SystemExit("candidate sidecar not found and master fallback is disabled")
        raw, load_note = base.load_observations_from_master_nc(release_dir, progress=progress)
        input_path = release_dir / base.MASTER_FILE
        input_mode = "selected_master"

    if input_mode == "candidate_sidecar" and not raw.empty:
        satellite_rows, sat_path, sat_note = load_satellite_candidate_sidecar(
            release_dir,
            satellite_candidate_sidecar,
            progress=progress,
        )
        if not satellite_rows.empty:
            satellite_rows = _filter_satellite_rows_to_candidate_windows(
                satellite_rows,
                raw,
                windows=windows,
                progress=progress,
            )
            if not satellite_rows.empty:
                raw = pd.concat([raw, satellite_rows], ignore_index=True, sort=False)
                load_note = "{}; appended {} satellite CSV rows from {}".format(
                    load_note,
                    len(satellite_rows),
                    sat_path.name if sat_path is not None else "satellite sidecar",
                )
            else:
                load_note = "{}; satellite CSV found but no rows matched candidate windows".format(load_note)
        elif allow_nc_fallback:
            satellite_rows, satellite_stats = base.load_relevant_satellite_validation_records(
                release_dir,
                raw,
                windows=windows,
                workers=workers,
                chunk_size=satellite_chunk_size,
                progress=progress,
            )
            if progress:
                progress(
                    "Satellite validation NC fallback: rows={satellite_rows}, station_hits={station_hits}, "
                    "time_hits={time_hits}, value_hits={value_hits}".format(**satellite_stats)
                )
            if not satellite_rows.empty:
                raw = pd.concat([raw, satellite_rows], ignore_index=True, sort=False)
                load_note = "{}; appended {} satellite NC fallback rows".format(load_note, len(satellite_rows))
        else:
            if progress:
                progress("Satellite candidate sidecar missing and NC fallback disabled")

    raw = base.add_observation_type_from_source_attrs(raw, release_dir, workers=workers, progress=progress)
    observations = base.normalize_observation_table(raw, taxonomy, input_mode=input_mode)
    if progress:
        progress("Normalized observations: {}".format(len(observations)))

    pair_records = base.pair_satellite_insitu_records(
        observations,
        windows=windows,
        input_mode=input_mode,
        workers=workers,
        progress=progress,
    )
    pair_records = base.assign_strata(
        pair_records,
        external_attributes=external_attrs,
        high_turbidity_ssc=high_turbidity_ssc,
        ssc_bin_edges=ssc_bin_edges,
    )
    if progress:
        progress("Built pair records: {}".format(len(pair_records)))
    metrics = base.compute_satellite_insitu_metrics(pair_records)
    if progress:
        progress("Aggregated metric rows: {}".format(len(metrics)))

    pair_path = out_dir / "validation_satellite_insitu_pairs.csv"
    metric_path = out_dir / "validation_satellite_insitu_metrics.csv"
    summary_path = out_dir / "validation_satellite_insitu_summary.md"
    pair_records.to_csv(pair_path, index=False)
    metrics.to_csv(metric_path, index=False)
    generated_outputs = [
        (pair_path.name, "generated"),
        (metric_path.name, "generated"),
    ]
    if write_plots:
        generated_outputs.extend(base.write_figures(pair_records, metrics, out_dir, figure_variables=figure_variables))
    else:
        generated_outputs.extend(
            [
                ("figures/satellite_insitu_scatter_by_window_SSC.png", "skipped: --no-figures"),
                ("figures/satellite_insitu_residual_by_ssc_bin.png", "skipped: --no-figures"),
                ("figures/satellite_insitu_metric_heatmap.png", "skipped: --no-figures"),
            ]
        )
    generated_outputs.append((summary_path.name, "generated"))
    base.write_summary(
        summary_path,
        input_path,
        input_mode,
        load_note,
        observations,
        pair_records,
        metrics,
        generated_outputs,
    )
    if progress:
        progress("fast s11 validation complete")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast satellite/reach-scale validation against in-situ records.")
    parser.add_argument("--release-dir", default=str(base.DEFAULT_RELEASE_DIR), help="Path to sed_reference_release.")
    parser.add_argument("--out-dir", default=str(base.DEFAULT_OUT_DIR), help="Output directory for validation tables and figures.")
    parser.add_argument("--candidate-sidecar", help="Optional in-situ candidate sidecar path.")
    parser.add_argument("--satellite-candidate-sidecar", help="Optional satellite candidate sidecar path.")
    parser.add_argument("--source-taxonomy-csv", help="Optional source taxonomy override CSV.")
    parser.add_argument("--external-attributes-csv", help="Optional cluster external attributes CSV for width/climate strata.")
    parser.add_argument("--no-master-fallback", action="store_true", help="Fail if no candidate sidecar is available.")
    parser.add_argument("--no-nc-fallback", action="store_true", help="Do not scan sed_reference_satellite_validation.nc if satellite CSV is missing.")
    parser.add_argument("--windows", nargs="+", default=["exact", "pm1d", "pm2d"], choices=sorted(base.WINDOW_DAYS))
    parser.add_argument("--high-turbidity-ssc", type=float, default=base.DEFAULT_HIGH_TURBIDITY_SSC)
    parser.add_argument("--ssc-bin-edges", default=",".join(base._format_edge(v) for v in base.DEFAULT_SSC_BIN_EDGES))
    parser.add_argument("--figure-variables", nargs="+", default=["SSC"], choices=list(base.VARIABLES))
    parser.add_argument("--no-figures", action="store_true")
    parser.add_argument("--satellite-chunk-size", type=int, default=base.DEFAULT_SATELLITE_CHUNK_SIZE)
    parser.add_argument("--workers", type=int, default=base.DEFAULT_WORKERS)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    run_validation_fast(
        release_dir=Path(args.release_dir),
        out_dir=Path(args.out_dir),
        candidate_sidecar=Path(args.candidate_sidecar).resolve() if args.candidate_sidecar else None,
        satellite_candidate_sidecar=(
            Path(args.satellite_candidate_sidecar).resolve() if args.satellite_candidate_sidecar else None
        ),
        source_taxonomy_csv=Path(args.source_taxonomy_csv).resolve() if args.source_taxonomy_csv else None,
        external_attributes_csv=Path(args.external_attributes_csv).resolve() if args.external_attributes_csv else None,
        allow_master_fallback=not args.no_master_fallback,
        allow_nc_fallback=not args.no_nc_fallback,
        windows=args.windows,
        high_turbidity_ssc=float(args.high_turbidity_ssc),
        ssc_bin_edges=base.parse_ssc_bin_edges(args.ssc_bin_edges),
        figure_variables=args.figure_variables,
        write_plots=not args.no_figures,
        workers=args.workers,
        satellite_chunk_size=args.satellite_chunk_size,
    )


if __name__ == "__main__":
    main()
