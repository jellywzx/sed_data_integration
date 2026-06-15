#!/usr/bin/env python3
"""Shared helpers for release-only statistics parity outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import numpy as np
import pandas as pd

from stats_release.release_io import clean_text, ensure_parent, read_text_var


VARIABLES = ("Q", "SSC", "SSL")
FLAG_VALUES = (0, 1, 2, 3, 8, 9)
FLAG_MEANINGS = {
    0: "good",
    1: "derived/estimated",
    2: "suspect",
    3: "bad",
    8: "not checked",
    9: "missing",
}


def pct(numerator: float, denominator: float, digits: int = 6) -> float:
    denominator = float(denominator or 0)
    if denominator == 0:
        return 0.0
    return round(100.0 * float(numerator) / denominator, digits)


def first_text(values: Iterable[object], default: str = "") -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return default


def unique_pipe(values: Iterable[object]) -> str:
    seen: List[str] = []
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        for part in text.replace(",", "|").split("|"):
            part = clean_text(part)
            if part and part not in seen:
                seen.append(part)
    return "|".join(sorted(seen))


def year_from_text(value: object) -> float:
    text = clean_text(value)
    if not text:
        return np.nan
    try:
        return float(pd.Timestamp(text).year)
    except Exception:
        try:
            return float(str(text)[:4])
        except Exception:
            return np.nan


def decode_time_values(ds, values: Sequence[float], time_var_name: str = "time") -> pd.DatetimeIndex:
    if time_var_name not in ds.variables:
        return pd.DatetimeIndex([])
    vals = np.asarray(values, dtype="float64")
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return pd.DatetimeIndex([])
    time_var = ds.variables[time_var_name]
    units = getattr(time_var, "units", "days since 1970-01-01")
    calendar = getattr(time_var, "calendar", "gregorian")
    import netCDF4 as nc4

    try:
        dates = nc4.num2date(vals, units=units, calendar=calendar, only_use_cftime_datetimes=False)
    except TypeError:
        dates = nc4.num2date(vals, units=units, calendar=calendar)
    return pd.DatetimeIndex(pd.to_datetime([str(d) for d in dates], errors="coerce"))


def decode_time_axis(ds, time_var_name: str = "time") -> pd.DatetimeIndex:
    if time_var_name not in ds.variables:
        return pd.DatetimeIndex([])
    arr = np.ma.asarray(ds.variables[time_var_name][:]).astype("float64")
    if np.ma.isMaskedArray(arr):
        arr = arr.filled(np.nan)
    return decode_time_values(ds, np.asarray(arr).reshape(-1), time_var_name=time_var_name)


def enum_mapping(var, fallback: Dict[int, str] | None = None) -> Dict[int, str]:
    mapping = dict(fallback or {})
    values = getattr(var, "flag_values", None)
    meanings = clean_text(getattr(var, "flag_meanings", "")).split()
    if values is None or not meanings:
        return mapping
    for raw_value, meaning in zip(np.asarray(values).reshape(-1), meanings):
        try:
            mapping[int(raw_value)] = clean_text(meaning)
        except Exception:
            pass
    return mapping


def resolution_values(ds, key=slice(None)) -> np.ndarray:
    if "resolution" not in ds.variables:
        return np.asarray([""] * int(len(ds.dimensions.get("n_records", []))), dtype=object)
    var = ds.variables["resolution"]
    arr = np.asarray(var[key])
    if getattr(var.dtype, "kind", "") in {"i", "u", "f"}:
        mapping = enum_mapping(var, {0: "daily", 1: "monthly", 2: "annual", 3: "climatology", 4: "other"})
        return np.asarray([mapping.get(int(v), str(v)) for v in np.asarray(arr).reshape(-1)], dtype=object)
    return np.asarray(read_text_var(ds, "resolution"), dtype=object)[key]


def classify_source(source_name: object, source_family: object = "") -> tuple[str, str]:
    source = clean_text(source_name)
    family = clean_text(source_family).lower()
    lower = source.lower()
    satellite = {"riversed", "gsed", "dethier", "shashi_jianli"}
    climatology = {"milliman", "vanmaercke", "hma", "ali_de_boer", "ali de boer"}
    agencies = {"usgs", "hydat", "bayern"}
    regional = {"hybam"}
    if "satellite" in family or lower in satellite:
        return "satellite", "satellite products"
    if lower in climatology or "climatology" in family:
        return "climatology", "global compilations"
    if lower in agencies:
        return "in-situ", "national agencies"
    if lower in regional:
        return "in-situ", "regional datasets"
    if source:
        return "literature", "global compilations"
    return "unknown", "unknown"


def attach_source_classification(frame: pd.DataFrame, source_col: str = "source_name") -> pd.DataFrame:
    out = frame.copy()
    if source_col not in out.columns:
        out[source_col] = ""
    families = out["source_family"] if "source_family" in out.columns else pd.Series([""] * len(out), index=out.index)
    classified = [classify_source(src, fam) for src, fam in zip(out[source_col], families)]
    out["source_type"] = [item[0] for item in classified]
    out["source_group"] = [item[1] for item in classified]
    return out


def numeric_stats(values: np.ndarray) -> dict:
    vals = np.asarray(values, dtype="float64")
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return {
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
        }
    pos = vals[vals > 0]
    return {
        "mean": float(np.mean(vals)),
        "median": float(np.median(vals)),
        "standard_deviation": float(np.std(vals)),
        "min": float(np.min(vals)),
        "max": float(np.max(vals)),
        "p05": float(np.percentile(vals, 5)),
        "p25": float(np.percentile(vals, 25)),
        "p75": float(np.percentile(vals, 75)),
        "p95": float(np.percentile(vals, 95)),
        "p99": float(np.percentile(vals, 99)),
        "log10_mean": float(np.mean(np.log10(pos))) if pos.size else np.nan,
        "log10_median": float(np.median(np.log10(pos))) if pos.size else np.nan,
        "n_positive_for_log": int(pos.size),
    }


def save_figure(fig, png_path: Path, dpi: int = 300, also_pdf: bool = True) -> None:
    png_path = ensure_parent(Path(png_path))
    fig.savefig(png_path, dpi=dpi)
    if also_pdf:
        fig.savefig(png_path.with_suffix(".pdf"))


def write_geojson_points(frame: pd.DataFrame, path: Path, lat_col: str = "lat", lon_col: str = "lon") -> None:
    rows = []
    lat = pd.to_numeric(frame.get(lat_col, pd.Series([], dtype=float)), errors="coerce")
    lon = pd.to_numeric(frame.get(lon_col, pd.Series([], dtype=float)), errors="coerce")
    for idx, row in frame.loc[lat.between(-90, 90) & lon.between(-180, 180)].iterrows():
        props = {}
        for col in ("cluster_uid", "country", "continent_region", "resolution", "record_count"):
            if col in frame.columns:
                props[col] = clean_text(row.get(col, ""))
        rows.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon.loc[idx]), float(lat.loc[idx])]},
                "properties": props,
            }
        )
    import json

    ensure_parent(path).write_text(json.dumps({"type": "FeatureCollection", "features": rows}, indent=2) + "\n", encoding="utf-8")
