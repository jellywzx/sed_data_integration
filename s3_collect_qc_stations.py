#!/usr/bin/env python3
"""Collect station metadata from s2-organized NetCDF files for the basin pipeline."""

import argparse
import hashlib
import re
from pathlib import PurePosixPath
from pathlib import Path
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
from pipeline_paths import S2_ORGANIZED_DIR, S3_COLLECTED_CSV, RESOLUTION_DIRS, get_output_r_root, get_log_path
from qc_contract import LAT_VAR_NAMES, LON_VAR_NAMES, read_scalar_variable, read_station_metadata

try:
    import netCDF4 as nc4
    HAS_NC = True
except ImportError:
    HAS_NC = False

FILL = -9999.0
_AREA_VAR_NAMES = [
    "upstream_area",
    "drainage_area",
    "basin_area",
    "catchment_area",
]
_AREA_ATTR_NAMES = [
    "Drainage area (km2)",
    "drainage_area_km2",
    "drainage_area",
    "upstream_area",
    "Area",
    "area",
    "basin_area",
    "catchment_area",
]
_REACH_HINT_NUMERIC_NAMES = [
    "reach_midpoint_lat",
    "reach_midpoint_lon",
    "reach_endpoint_1_lat",
    "reach_endpoint_1_lon",
    "reach_endpoint_2_lat",
    "reach_endpoint_2_lon",
]
_REACH_HINT_TEXT_NAMES = [
    "reach_endpoint_candidates_json",
    "reach_coordinate_method",
    "reach_geometry_source",
]
_REACH_HINT_SOURCES = {"gsed", "riversed"}
STATION_KEY_PREFIX = "S3_"
STATION_KEY_DIGEST_CHARS = 24


def _get_scalar(var):
    if var is None:
        return None
    if np.ma.isMaskedArray(var):
        v = var.flatten()
        if v.size == 0:
            return None
        v = v.flat[0]
        if np.ma.is_masked(v):
            return np.nan
        v = float(v)
    else:
        arr = np.asarray(var).flatten()
        if arr.size == 0:
            return None
        v = float(arr.flat[0])
    if np.isnan(v) or v == FILL or v == -9999:
        return np.nan
    return v


def get_reported_area_from_nc(path):
    """Extract upstream drainage area from a NetCDF file, returning None on failure."""
    if not HAS_NC:
        return None
    try:
        with nc4.Dataset(path, "r") as nc:
            for var_name in _AREA_VAR_NAMES:
                if var_name in nc.variables:
                    val = _get_scalar(nc.variables[var_name][:])
                    if val is not None and not np.isnan(val) and val > 0:
                        return float(val)

            for attr_name in _AREA_ATTR_NAMES:
                raw = getattr(nc, attr_name, None)
                if raw is not None:
                    try:
                        v = float(raw)
                        if v > 0 and not np.isnan(v):
                            return v
                    except (ValueError, TypeError):
                        pass

            for attr_name in nc.ncattrs():
                if "area" in attr_name.lower() and "ratio" not in attr_name.lower():
                    raw = getattr(nc, attr_name, None)
                    if raw is not None:
                        try:
                            v = float(raw)
                            if v > 0 and not np.isnan(v):
                                return v
                        except (ValueError, TypeError):
                            pass
    except Exception:
        pass
    return None


def get_lat_lon_from_nc(path):
    """Read scalar latitude and longitude from a NetCDF file."""
    if not HAS_NC:
        return None, None
    try:
        with nc4.Dataset(path, "r") as nc:
            lat = read_scalar_variable(nc, LAT_VAR_NAMES)
            lon = read_scalar_variable(nc, LON_VAR_NAMES)
            if lat is None or lon is None or (np.isnan(lat) or np.isnan(lon)):
                return None, None
            return float(lat), float(lon)
    except Exception:
        return None, None


def get_station_meta_from_nc(path):
    """Read station-level metadata from NetCDF global attributes."""
    if not HAS_NC:
        return {
            "station_name": "",
            "river_name": "",
            "source_station_id": "",
            "continent_region": "",
            "country": "",
        }
    try:
        with nc4.Dataset(path, "r") as nc:
            return read_station_metadata(nc)
    except Exception:
        return {
            "station_name": "",
            "river_name": "",
            "source_station_id": "",
            "continent_region": "",
            "country": "",
        }


def _clean_text(value):
    if value is None or np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return ""
        return _clean_text(value.flat[0])
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none"} else text


def _normalize_station_key_part(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()


def _normalize_station_path(value):
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip().replace("\\", "/")
    while text.startswith("./"):
        text = text[2:]
    return PurePosixPath(text).as_posix()


def build_station_key(source, resolution, path):
    """Build the stable s3-s5 station identity key from normalized source/resolution/path."""
    payload = "\n".join(
        [
            _normalize_station_key_part(source),
            _normalize_station_key_part(resolution),
            _normalize_station_path(path),
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return "{}{}".format(STATION_KEY_PREFIX, digest[:STATION_KEY_DIGEST_CHARS])


def _format_duplicate_rows(df, columns, limit=20):
    dup_mask = df.duplicated(subset=columns, keep=False)
    shown = df.loc[dup_mask, list(columns)].head(limit)
    return shown.to_string(index=False)


def _fail_on_duplicates(df, columns, label):
    dup_mask = df.duplicated(subset=columns, keep=False)
    if dup_mask.any():
        raise ValueError(
            "Duplicate {} detected in s3 station collection (count={}). First rows:\n{}".format(
                label,
                int(dup_mask.sum()),
                _format_duplicate_rows(df, columns),
            )
        )


def prepare_s3_station_table(stations):
    """Normalize path, generate station_key/station_id, and enforce stable ordering."""
    if stations is None or len(stations) == 0:
        return pd.DataFrame()
    required = {"path", "source", "resolution"}
    missing = required.difference(stations.columns)
    if missing:
        raise ValueError("s3 station collection missing required columns: {}".format(sorted(missing)))

    work = stations.copy()
    work = work.drop(columns=["station_key", "station_id"], errors="ignore")
    work["path"] = work["path"].map(_normalize_station_path)
    work["_sort_resolution"] = work["resolution"].map(_normalize_station_key_part)
    work["_sort_source"] = work["source"].map(_normalize_station_key_part)
    work["_sort_path"] = work["path"].map(_normalize_station_path)
    work["station_key"] = [
        build_station_key(source, resolution, path)
        for source, resolution, path in zip(work["source"], work["resolution"], work["path"])
    ]
    work = work.sort_values(
        ["_sort_resolution", "_sort_source", "_sort_path"],
        kind="mergesort",
    ).reset_index(drop=True)
    work.insert(1, "station_id", np.arange(len(work), dtype=np.int64))
    work = work.drop(columns=["_sort_resolution", "_sort_source", "_sort_path"])

    _fail_on_duplicates(work, ["station_key"], "station_key")
    _fail_on_duplicates(work, ["station_id"], "station_id")
    _fail_on_duplicates(work, ["path"], "path")

    preferred = ["station_key", "station_id", "path", "source", "lat", "lon", "resolution"]
    columns = [c for c in preferred if c in work.columns] + [
        c for c in work.columns if c not in preferred
    ]
    return work[columns]


def _get_nc_text(nc, name):
    if name in nc.variables:
        try:
            text = _clean_text(nc.variables[name][()])
            if text:
                return text
        except Exception:
            pass
    return _clean_text(getattr(nc, name, ""))


def get_observation_type_from_nc(path):
    """Read observation_type from NetCDF global attributes, returning an empty string if unavailable."""
    if not HAS_NC:
        return ""
    try:
        with nc4.Dataset(path, "r") as nc:
            return _clean_text(getattr(nc, "observation_type", ""))
    except Exception:
        return ""


def get_reach_hints_from_nc(path):
    hints = {name: np.nan for name in _REACH_HINT_NUMERIC_NAMES}
    hints.update({name: "" for name in _REACH_HINT_TEXT_NAMES})
    if not HAS_NC:
        return hints
    try:
        with nc4.Dataset(path, "r") as nc:
            for name in _REACH_HINT_NUMERIC_NAMES:
                if name in nc.variables:
                    val = _get_scalar(nc.variables[name][:])
                    if val is not None and not np.isnan(val):
                        hints[name] = float(val)
                        continue
                raw = getattr(nc, name, None)
                if raw is not None:
                    try:
                        val = float(raw)
                        if not np.isnan(val) and val != FILL:
                            hints[name] = val
                    except (TypeError, ValueError):
                        pass
            for name in _REACH_HINT_TEXT_NAMES:
                hints[name] = _get_nc_text(nc, name)
    except Exception:
        pass
    return hints


def get_resolution_from_path(path, root_dir):
    """Parse temporal resolution from the first path component."""
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if parts:
            res = parts[0].strip().lower()
            if res in RESOLUTION_DIRS:
                return res
            return res
    except Exception:
        pass
    return "unknown"


def get_source_from_path(path, root_dir):
    """Parse a source name from a relative path such as daily/GloRiSe/SS/qc/file.nc."""
    try:
        rel = Path(path).relative_to(Path(root_dir))
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
        return re.sub(r"[^\w\-]", "_", source)
    except Exception:
        return "unknown"


def get_source_from_organized_path(path, root_dir):
    """Parse a source name from an s2-organized filename."""
    try:
        rel = Path(path).relative_to(Path(root_dir))
        parts = rel.parts
        if not parts:
            return "unknown"
        resolution = get_resolution_from_path(path, root_dir)
        stem = Path(parts[-1]).stem
        stem_parts = stem.split("_")
        for i, seg in enumerate(stem_parts):
            if seg == resolution:
                return "_".join(stem_parts[:i]) if i > 0 else (stem_parts[0] if stem_parts else "unknown")
        return stem_parts[0] if stem_parts else "unknown"
    except Exception:
        return "unknown"


def normalize_source_selector(value):
    """Normalize source filters so GloRiSe/SS and GloRiSe_SS match the s3 source."""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("\\", "/")
    text = re.sub(r"\s*/\s*", "_", text)
    text = re.sub(r"[^\w\-]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_").lower()


def split_source_selectors(values):
    """Parse --exclude-source values; supports repeated values and comma-separated lists."""
    selectors = set()
    for raw in values or []:
        for piece in str(raw).split(","):
            norm = normalize_source_selector(piece)
            if norm:
                selectors.add(norm)
    return selectors


def _collect_one_nc(path, root_dir):
    """Read one NetCDF file and return station path, source, coordinates, resolution, and metadata."""
    try:
        lat, lon = get_lat_lon_from_nc(path)
        if lat is None or lon is None:
            return None
        station_meta = get_station_meta_from_nc(path)
        source = get_source_from_organized_path(path, root_dir)
        resolution = get_resolution_from_path(path, root_dir)
        observation_type = get_observation_type_from_nc(path)
        reach_hints = get_reach_hints_from_nc(path)
        reported_area = get_reported_area_from_nc(path)
        # Reach-scale satellite products match MERIT by geometry hints in s4,
        # not by source/NHDPlus upstream-area values.
        if source.strip().lower() in _REACH_HINT_SOURCES:
            reported_area = None
        rel_path = Path(path).relative_to(root_dir).as_posix()
        return {
            "path": rel_path,
            "source": source,
            "lat": lat,
            "lon": lon,
            "resolution": resolution,
            "observation_type": observation_type,
            "station_name": station_meta["station_name"],
            "river_name": station_meta["river_name"],
            "source_station_id": station_meta["source_station_id"],
            "reported_area": reported_area if reported_area is not None else float("nan"),
            "continent_region": station_meta.get("continent_region", ""),
            "country": station_meta.get("country", ""),
            **reach_hints,
        }
    except (ValueError, OSError):
        return None


ORGANIZED_DIR = S2_ORGANIZED_DIR


def collect_qc_nc_stations(root_dir, workers=1, excluded_resolutions=None, excluded_sources=None):
    """Collect all NetCDF files under root/S2_ORGANIZED_DIR."""
    root = Path(root_dir).resolve()
    scan_root = root / ORGANIZED_DIR
    excluded = {str(x).strip().lower() for x in (excluded_resolutions or []) if str(x).strip()}
    excluded_source_keys = split_source_selectors(excluded_sources)
    paths = []
    for p in scan_root.rglob("*.nc"):
        try:
            rel = p.relative_to(scan_root)
        except ValueError:
            continue
        if not rel.parts or rel.parts[0] not in RESOLUTION_DIRS:
            continue
        if rel.parts[0].strip().lower() in excluded:
            continue
        if excluded_source_keys:
            source = get_source_from_organized_path(p, scan_root)
            if normalize_source_selector(source) in excluded_source_keys:
                continue
        paths.append(str(p))
    paths = sorted(paths)
    if not paths:
        return pd.DataFrame()
    root_str = str(scan_root)
    if workers <= 1:
        rows = [_collect_one_nc(p, root_str) for p in paths]
    else:
        with Pool(min(workers, len(paths), cpu_count() or 1)) as pool:
            rows = pool.starmap(_collect_one_nc, [(p, root_str) for p in paths])
    rows = [r for r in rows if r is not None]
    return prepare_s3_station_table(pd.DataFrame(rows))


_LOG_TEE_ENABLED = False


def _enable_script_logging():
    global _LOG_TEE_ENABLED
    if _LOG_TEE_ENABLED:
        return
    import atexit
    import sys
    from datetime import datetime

    log_path = get_log_path(Path(__file__).resolve().parent, "{}_log.txt".format(Path(__file__).stem))
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
    _default_root = str(get_output_r_root(Path(__file__).resolve().parent))
    ap = argparse.ArgumentParser(description="Step s3: collect NetCDF stations (path, source, lat, lon, resolution, observation_type) into s3_collected_stations.csv")
    ap.add_argument("--root", default=_default_root, help="Root directory; default is the parent of this script directory (Output_r)")
    ap.add_argument("--out", default=S3_COLLECTED_CSV, help="Step s3 output CSV path")
    ap.add_argument("--workers", "-j", type=int, default=0,
                    help="Parallel workers; 0=auto (cpu_count-1, max 32)")
    ap.add_argument(
        "--exclude-resolutions",
        default="climatology",
        help="Comma-separated resolution directory names to exclude; default excludes climatology from the basin pipeline",
    )
    ap.add_argument(
        "--exclude-source",
        "--exclude-sources",
        nargs="+",
        default=[],
        help="Exclude selected sources; accepts multiple values and comma-separated lists, for example Huanghe or GloRiSe/SS",
    )
    args = ap.parse_args()

    if not HAS_NC:
        print("Error: netCDF4 is required. Install with: pip install netCDF4")
        return

    root_dir = Path(args.root).resolve()
    workers = args.workers if args.workers > 0 else min(32, max(1, (cpu_count() or 2) - 1))
    excluded_resolutions = [
        x.strip().lower()
        for x in str(args.exclude_resolutions).split(",")
        if x.strip()
    ]
    excluded_sources = split_source_selectors(args.exclude_source)

    print(
        "Collecting .nc stations from {} (workers={}, excluded_resolutions={}, excluded_sources={}) ...".format(
            ORGANIZED_DIR,
            workers,
            ",".join(excluded_resolutions) if excluded_resolutions else "(none)",
            ",".join(sorted(excluded_sources)) if excluded_sources else "(none)",
        )
    )
    stations = collect_qc_nc_stations(
        root_dir,
        workers=workers,
        excluded_resolutions=excluded_resolutions,
        excluded_sources=excluded_sources,
    )
    if len(stations) == 0:
        print("No organized .nc files found with valid lat/lon.")
        return
    print("Found {} organized .nc files.".format(len(stations)))

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = root_dir / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    stations.to_csv(out_path, index=False)
    print("Saved to {}.".format(out_path))
    if "climatology" in excluded_resolutions:
        print("Note: climatology files were excluded from the basin mainline collection.")
        print("Next: run s4_basin_trace_watch.py with input {}".format(out_path))
        print("Climatology can be exported separately with s6_export_climatology_to_nc.py")
    else:
        print("Next: run s4_basin_trace_watch.py with input {}".format(out_path))


if __name__ == "__main__":
    main()
