#!/usr/bin/env python3
"""Query station metadata and time series by cluster_uid from the s6 NetCDF products."""

# ══════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════

from pathlib import Path

CLUSTER_UID  = "SED000183"

OUTPUT_ROOT  = None

RESOLUTION   = "daily"

VARIABLE     = "all"

PREVIEW_ROWS = 20

OUT_CSV = None
OUT_TXT = None


ENABLE_COLOR = True

# ══════════════════════════════════════════════════════════════════════

import sys

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
sys.path.insert(0, str(SCRIPT_DIR))

try:
    import netCDF4 as nc4
    HAS_NC = True
except ImportError:
    nc4 = None
    HAS_NC = False

from pipeline_paths import (
    S6_MERGED_NC,
    S6_MATRIX_DIR,
    get_output_r_root,
)

FILL = -9999.0
RESOLUTION_CODES = {"daily": 0, "monthly": 1, "annual": 2, "climatology": 3, "other": 4}
RESOLUTION_NAMES = {v: k for k, v in RESOLUTION_CODES.items()}
VAR_UNITS        = {"Q": "m³/s", "SSC": "mg/L", "SSL": "ton/day"}
FLAG_LABELS      = {0: "good", 1: "est", 2: "suspect", 3: "bad", 9: "missing"}
MATCH_LABELS     = {0: "distance_only", 1: "area_matched", 2: "failed", -1: "unknown"}
MATRIX_FILES     = {
    "daily":   "s6_basin_matrix_daily.nc",
    "monthly": "s6_basin_matrix_monthly.nc",
    "annual":  "s6_basin_matrix_annual.nc",
}


class _C:
    BCYAN   = "\033[1;36m"
    CYAN    = "\033[36m"
    BYELLOW = "\033[1;33m"
    YELLOW  = "\033[33m"
    GREEN   = "\033[32m"
    RED     = "\033[31m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RESET   = "\033[0m"


def _c(text, code=""):
    if not ENABLE_COLOR or not code:
        return str(text)
    return "{}{}{}".format(code, text, _C.RESET)


def _section(title):
    bar = "═" * 64
    print()
    print(_c(bar, _C.BCYAN))
    print(_c("  " + title, _C.BCYAN))
    print(_c(bar, _C.BCYAN))


def _row(label, value, lw=22):
    lbl = _c("{:<{}}".format(label, lw), _C.YELLOW)
    print("  {} : {}".format(lbl, value))


def _dash(text=""):
    return _c(text if text else "—", _C.DIM)


def _txt(value):
    if value is None:
        return ""
    if np.ma.is_masked(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def _txt_arr(arr):
    return [_txt(v) for v in np.asarray(arr, dtype=object).reshape(-1)]


def _pct_str(n, total):
    if total == 0:
        return _dash()
    pct = 100.0 * n / total
    s = "{:.1f}%".format(pct)
    if pct >= 80:   return _c(s, _C.GREEN)
    if pct >= 20:   return _c(s, _C.YELLOW)
    return _c(s, _C.RED)


def _normalize_uid(query):
    q = str(query).strip().upper()
    if q.startswith("SED"):
        num = q[3:]
    else:
        num = q
    try:
        return "SED{:06d}".format(int(num))
    except ValueError:
        return q


def _paths():
    root = Path(OUTPUT_ROOT).expanduser().resolve() if OUTPUT_ROOT else get_output_r_root(SCRIPT_DIR.parent)
    matrix_dir = root / S6_MATRIX_DIR
    return {
        "master": root / S6_MERGED_NC,
        **{"matrix_" + res: matrix_dir / fname for res, fname in MATRIX_FILES.items()},
    }


def _read_station_meta(nc_path, uid):
    with nc4.Dataset(str(nc_path), "r") as ds:
        uids = _txt_arr(ds.variables["cluster_uid"][:])
        if uid not in uids:
            return None, None, uids

        idx = uids.index(uid)

        def _f(name):
            return float(np.ma.asarray(ds.variables[name][idx]).filled(np.nan))

        def _i(name, fill=-9999):
            return int(np.ma.asarray(ds.variables[name][idx]).filled(fill))

        meta = {
            "cluster_uid":         uid,
            "cluster_id":          _i("cluster_id"),
            "lat":                 _f("lat"),
            "lon":                 _f("lon"),
            "station_name":        _txt(ds.variables["station_name"][idx]),
            "river_name":          _txt(ds.variables["river_name"][idx]),
            "basin_area":          _f("basin_area"),
            "pfaf_code":           _f("pfaf_code"),
            "n_upstream_reaches":  _i("n_upstream_reaches"),
            "basin_match_quality": MATCH_LABELS.get(
                int(np.ma.asarray(ds.variables["basin_match_quality"][idx]).filled(-1)), "unknown"),
            "sources_used":        _txt(ds.variables["sources_used"][idx]),
            "n_source_stations":   _i("n_source_stations_in_cluster"),
            "station_idx":         idx,
        }

        src_names = _txt_arr(ds.variables["source_name"][:])
        meta["source_lookup"] = {
            n: {
                "long_name":   _txt(ds.variables["source_long_name"][i]),
                "institution": _txt(ds.variables["institution"][i]),
                "reference":   _txt(ds.variables["reference"][i]),
                "url":         _txt(ds.variables["source_url"][i]),
            }
            for i, n in enumerate(src_names)
        }

    return meta, idx, uids


def _read_source_stations(nc_path, station_idx, source_lookup):
    with nc4.Dataset(str(nc_path), "r") as ds:
        cluster_idx_arr = np.asarray(ds.variables["source_station_cluster_index"][:], dtype=np.int32)
        hit = np.flatnonzero(cluster_idx_arr == station_idx)
        if len(hit) == 0:
            return []

        src_names_all = _txt_arr(ds.variables["source_name"][:])
        rows = []
        for i in hit:
            src_i = int(np.ma.asarray(ds.variables["source_station_source_index"][i]).filled(-1))
            rows.append({
                "uid":         _txt(ds.variables["source_station_uid"][i]),
                "native_id":   _txt(ds.variables["source_station_native_id"][i]),
                "name":        _txt(ds.variables["source_station_name"][i]),
                "river":       _txt(ds.variables["source_station_river_name"][i]),
                "lat":         float(np.ma.asarray(ds.variables["source_station_lat"][i]).filled(np.nan)),
                "lon":         float(np.ma.asarray(ds.variables["source_station_lon"][i]).filled(np.nan)),
                "resolutions": _txt(ds.variables["source_station_resolutions"][i]),
                "source":      src_names_all[src_i] if 0 <= src_i < len(src_names_all) else "",
            })
    return rows


def _read_matrix(nc_path, uid, variables):
    with nc4.Dataset(str(nc_path), "r") as ds:
        uids = _txt_arr(ds.variables["cluster_uid"][:])
        if uid not in uids:
            return None
        row = uids.index(uid)

        time_var = ds.variables["time"]
        units = getattr(time_var, "units", "days since 1970-01-01")
        cal   = getattr(time_var, "calendar", "gregorian")
        try:
            times = nc4.num2date(time_var[:], units, calendar=cal,
                                 only_use_cftime_datetimes=False)
        except TypeError:
            times = nc4.num2date(time_var[:], units, calendar=cal)

        data = {"time": pd.to_datetime(list(times))}

        for var in variables:
            if var in ds.variables:
                data[var] = np.ma.asarray(ds.variables[var][row, :]).filled(np.nan)
            fk = "{}_flag".format(var)
            if fk in ds.variables:
                data[fk] = np.ma.asarray(ds.variables[fk][row, :]).filled(9).astype(np.int16)

        if "is_overlap" in ds.variables:
            data["is_overlap"] = np.ma.asarray(ds.variables["is_overlap"][row, :]).filled(0)

        if "selected_source_index" in ds.variables:
            src_idx_arr = np.ma.asarray(ds.variables["selected_source_index"][row, :]).filled(-1)
            src_names   = _txt_arr(ds.variables["source_name"][:]) if "source_name" in ds.variables else []
            data["source"] = [
                src_names[int(i)] if 0 <= int(i) < len(src_names) else "" for i in src_idx_arr
            ]

    return pd.DataFrame(data)


def _read_master_records(nc_path, station_idx, res_codes, variables, chunk=500_000):
    frames = []
    with nc4.Dataset(str(nc_path), "r") as ds:
        n_rec    = len(ds.dimensions["n_records"])
        time_var = ds.variables["time"]
        units    = getattr(time_var, "units", "days since 1970-01-01")
        cal      = getattr(time_var, "calendar", "gregorian")

        for start in range(0, n_rec, chunk):
            stop    = min(start + chunk, n_rec)
            s_arr   = np.asarray(ds.variables["station_index"][start:stop], dtype=np.int32)
            r_arr   = np.asarray(ds.variables["resolution"][start:stop],    dtype=np.int16)
            local   = np.flatnonzero((s_arr == station_idx) & np.isin(r_arr, res_codes))
            if len(local) == 0:
                continue
            g = start + local

            t_raw = np.asarray(ds.variables["time"][g], dtype=np.float64)
            try:
                times = nc4.num2date(t_raw, units, calendar=cal, only_use_cftime_datetimes=False)
            except TypeError:
                times = nc4.num2date(t_raw, units, calendar=cal)

            row = {
                "time":       pd.to_datetime(list(times)),
                "resolution": np.asarray(ds.variables["resolution"][g], dtype=np.int16),
                "source":     _txt_arr(ds.variables["source"][g]),
                "is_overlap": np.ma.asarray(ds.variables["is_overlap"][g]).filled(0),
            }
            for var in variables:
                if var in ds.variables:
                    row[var] = np.ma.asarray(ds.variables[var][g]).filled(np.nan)
                fk = "{}_flag".format(var)
                if fk in ds.variables:
                    row[fk] = np.ma.asarray(ds.variables[fk][g]).filled(9).astype(np.int16)

            frames.append(pd.DataFrame(row))

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("time").reset_index(drop=True)


def _availability(df, variables):
    if df is None or len(df) == 0:
        return None
    cols = [v for v in variables if v in df.columns]
    has  = df[cols].notna().any(axis=1) if cols else pd.Series(False, index=df.index)
    sub  = df[has]
    out  = {
        "n_total": len(df),
        "n_valid": len(sub),
        "t_min":   sub["time"].min() if len(sub) else None,
        "t_max":   sub["time"].max() if len(sub) else None,
    }
    for var in variables:
        out["{}_valid".format(var)] = int(df[var].notna().sum()) if var in df.columns else 0
    return out


def _fmt_time(t, res):
    if t is None:
        return "—"
    ts = pd.Timestamp(t)
    if res == "monthly":     return ts.strftime("%Y-%m")
    if res == "climatology": return "month {:02d}".format(ts.month)
    return ts.strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════

def _print_station(meta):
    _section("Station Summary  ·  {}".format(_c(meta["cluster_uid"], _C.BCYAN)))

    _row("cluster_uid",       _c(meta["cluster_uid"], _C.BCYAN))
    _row("cluster_id",        str(meta["cluster_id"]))

    lat, lon = meta["lat"], meta["lon"]
    if not np.isnan(lat) and not np.isnan(lon):
        coord = "{:.6f}°{}  /  {:.6f}°{}".format(
            abs(lat), "N" if lat >= 0 else "S",
            abs(lon), "E" if lon >= 0 else "W")
    else:
        coord = _dash()
    _row("Coordinates (lat / lon)",  coord)

    _row("Station name",          meta["station_name"]  or _dash("(unknown)"))
    _row("River name",          meta["river_name"]    or _dash("(unknown)"))

    ba = meta["basin_area"]
    _row("Basin area",          "{:,.1f} km²".format(ba) if not np.isnan(ba) else _dash())

    pc = meta["pfaf_code"]
    _row("Pfafstetter code",  str(int(pc)) if not np.isnan(pc) else _dash())

    nr = meta["n_upstream_reaches"]
    _row("Upstream reach count",        str(nr) if nr >= 0 else _dash())

    _row("Basin match quality",      meta["basin_match_quality"])

    srcs = meta["sources_used"]
    _row("Data sources",          " | ".join(srcs.split("|")) if srcs else _dash())
    _row("Source station count",        str(meta["n_source_stations"]))


def _print_source_stations(stations):
    _section("Source Station List  ({} stations)".format(len(stations)))
    if not stations:
        print("  " + _dash("(no source station information)"))
        return
    for i, s in enumerate(stations, 1):
        src_lbl = _c(s["source"], _C.YELLOW) if s["source"] else _dash("unknown source")
        print("  [{}] {}  |  source: {}".format(i, _c(s["uid"], _C.CYAN), src_lbl))
        print("      Native ID    : {}".format(s["native_id"] or _dash()))
        print("      Station name  : {}".format(s["name"]      or _dash()))
        print("      River name  : {}".format(s["river"]     or _dash()))
        if not (np.isnan(s["lat"]) or np.isnan(s["lon"])):
            print("      Coordinates: {:.6f} deg N  /  {:.6f} deg E".format(s["lat"], s["lon"]))
        print("      Available resolutions: {}".format(s["resolutions"] or _dash()))
        print()


def _print_availability(avail_map, variables):
    _section("Data Availability Summary")

    RW, NW, TW = 12, 8, 28
    VW = 10
    hdr = "{:<{}}{}{}".format(
        "Resolution", RW,
        "{:<{}}".format("Records", NW),
        "{:<{}}".format("Time range", TW),
    ) + "".join("{:<{}}".format("{} valid rate".format(v), VW) for v in variables)
    print("  " + _c(hdr, _C.BOLD))
    print("  " + _c("─" * (RW + NW + TW + VW * len(variables)), _C.DIM))

    found = False
    for res in ["daily", "monthly", "annual", "climatology", "other"]:
        a = avail_map.get(res)
        if not a:
            continue
        found = True
        rng = "{} ~ {}".format(_fmt_time(a["t_min"], res), _fmt_time(a["t_max"], res)) \
              if a["t_min"] else _dash()
        line = "{:<{}}".format(res, RW)
        line += "{:<{},}".format(a["n_valid"], NW)
        line += "{:<{}}".format(rng, TW)
        for var in variables:
            line += "{:<{}}".format(_pct_str(a.get("{}_valid".format(var), 0), a["n_valid"]), VW)
        print("  " + line)

    if not found:
        print("  " + _dash("(no data for any resolution)"))

    print()
    print("  " + _c("Flag meanings: 0=good 1=estimated 2=suspect 3=bad 9=missing", _C.DIM))


def _print_timeseries(df, variables, resolution, max_rows):
    if df is None or len(df) == 0:
        return

    cols = [v for v in variables if v in df.columns]
    has  = df[cols].notna().any(axis=1) if cols else pd.Series(False, index=df.index)
    sub  = df[has].copy()
    if len(sub) == 0:
        return

    n_total = len(sub)
    show_all = (max_rows == 0)
    if not show_all:
        sub = sub.head(max_rows)

    _section("Time Series Data  ·  {}  ({:,} rows total, showing {:,})".format(
        resolution, n_total, len(sub)))

    TW, VW, FW, SW, OW = 13, 12, 10, 14, 5

    hdr = "{:<{}}".format("Time", TW)
    for var in variables:
        hdr += "{:<{}}".format("{}({})".format(var, VAR_UNITS.get(var, "")), VW)
    for var in variables:
        hdr += "{:<{}}".format("{}_flag".format(var), FW)
    hdr += "{:<{}}".format("Source", SW)
    hdr += "{:<{}}".format("overlap", OW)
    print("  " + _c(hdr, _C.BOLD))
    print("  " + _c("─" * (TW + VW * len(variables) + FW * len(variables) + SW + OW), _C.DIM))

    for _, row in sub.iterrows():
        line = "{:<{}}".format(_fmt_time(row["time"], resolution), TW)

        for var in variables:
            val = row.get(var, np.nan)
            if pd.isna(val):
                line += _c("{:<{}}".format("—", VW), _C.DIM)
            else:
                line += "{:<{}.4g}".format(float(val), VW)

        for var in variables:
            fv = int(row.get("{}_flag".format(var), 9))
            fs = FLAG_LABELS.get(fv, str(fv))
            if fv == 0:
                line += _c("{:<{}}".format(fs, FW), _C.GREEN)
            elif fv in (1, 2):
                line += _c("{:<{}}".format(fs, FW), _C.YELLOW)
            else:
                line += _c("{:<{}}".format(fs, FW), _C.RED)

        src = str(row.get("source", ""))[:SW - 1]
        line += "{:<{}}".format(src, SW)

        ovlp = int(row.get("is_overlap", 0))
        line += _c("yes", _C.YELLOW) if ovlp else _c("no", _C.DIM)
        print("  " + line)

    if not show_all and n_total > max_rows:
        print("  " + _c("  … {:,} more rows (set PREVIEW_ROWS=0 to show all)".format(
            n_total - max_rows), _C.DIM))


def _print_source_info(sources_used, source_lookup):
    _section("Source Dataset Information")
    if not sources_used:
        print("  " + _dash("(no source information)"))
        return
    for src in sources_used.split("|"):
        src = src.strip()
        if not src:
            continue
        info = source_lookup.get(src, {})
        print("  " + _c(src, _C.BYELLOW) + "  |  " +
              (info.get("long_name") or _dash("(no full name)")))
        if info.get("institution"):
            _row("Institution", info["institution"])
        if info.get("reference"):
            _row("Reference", info["reference"][:100])
        if info.get("url"):
            _row("URL",  info["url"][:100])
        print()


# ══════════════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════

import re

def _strip_ansi(text):
    """Strip ANSI color codes before saving plain text."""
    return re.sub(r"\033\[[0-9;]*m", "", text)

class _Tee:
    """Write output to both terminal and file."""
    def __init__(self, file):
        self._file = file
        self._stdout = sys.stdout
    def write(self, text):
        self._stdout.write(text)
        self._file.write(_strip_ansi(text))
    def flush(self):
        self._stdout.flush()
        self._file.flush()

def main():
    global OUT_TXT, OUT_CSV

    uid = _normalize_uid(CLUSTER_UID)
    _ensure_output_names(uid)

    _txt_file = None
    if OUT_TXT:
        out_path = Path(OUT_TXT).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        _txt_file = open(out_path, "w", encoding="utf-8")
        sys.stdout = _Tee(_txt_file)

    try:
        result = _main()
    finally:
        if _txt_file:
            sys.stdout = sys.stdout._stdout
            _txt_file.close()
            print("Saved output to: {}".format(OUT_TXT))
    return result

def _build_source_summary(stations):
    if not stations:
        return {
            "source_station_uids": "",
            "source_station_names": "",
            "source_station_sources": "",
            "source_station_native_ids": "",
        }

    return {
        "source_station_uids": " | ".join(s.get("uid", "") for s in stations),
        "source_station_names": " | ".join(s.get("name", "") for s in stations),
        "source_station_sources": " | ".join(s.get("source", "") for s in stations),
        "source_station_native_ids": " | ".join(s.get("native_id", "") for s in stations),
    }


def _ensure_output_names(uid):
    global OUT_CSV, OUT_TXT
    if OUT_CSV is None:
        OUT_CSV = str(SCRIPT_DIR / "{}.csv".format(uid))
    if OUT_TXT is None:
        OUT_TXT = str(SCRIPT_DIR / "{}.txt".format(uid))


def _main():
    if not HAS_NC:

        print("Error: netCDF4 is required. Run: pip install netCDF4")
        return 1

    uid   = _normalize_uid(CLUSTER_UID)
    _ensure_output_names(uid)
    paths = _paths()

    print(_c("\nQuerying: {}  (from {})".format(
        uid, paths["master"].parent.parent.name), _C.BOLD))

    if not paths["master"].is_file():
        print(_c("Error: master NetCDF file not found", _C.RED))
        print("  Expected path: {}".format(paths["master"]))
        print("  Check OUTPUT_ROOT configuration (current: {})".format(OUTPUT_ROOT or "auto-derived"))
        return 1

    meta, station_idx, all_uids = _read_station_meta(paths["master"], uid)
    if meta is None:
        print(_c("Error: cluster_uid not found: {}".format(uid), _C.RED))
        print("  Dataset contains {:,} clusters.".format(len(all_uids)))
        try:
            num = int(uid[3:])
            cands = sorted(all_uids, key=lambda u: abs(int(u[3:]) - num))[:5]
            print("  Nearest 5 UIDs: {}".format(", ".join(cands)))
        except (ValueError, IndexError):
            print("  First 5 UIDs: {}".format(", ".join(all_uids[:5])))
        return 1

    res_targets = (
        ["daily", "monthly", "annual"]
        if RESOLUTION == "all" else [RESOLUTION]
    )
    var_targets = list(VAR_UNITS.keys()) if VARIABLE == "all" else [VARIABLE]

    _print_station(meta)

    source_stations = _read_source_stations(paths["master"], station_idx, meta["source_lookup"])
    _print_source_stations(source_stations)
    source_summary = _build_source_summary(source_stations)

    all_dfs   = {}
    avail_map = {}
    fallback_used = []

    for res in res_targets:
        mkey = "matrix_{}".format(res)
        mp   = paths.get(mkey)

        if mp and mp.is_file():
            df = _read_matrix(mp, uid, var_targets)
        else:
            fallback_used.append(res)
            df = _read_master_records(
                paths["master"], station_idx,
                [RESOLUTION_CODES[res]], var_targets,
            )
            if df is not None and len(df) > 0:
                df["resolution"] = res

        if df is not None and len(df) > 0:
            all_dfs[res]   = df
            avail_map[res] = _availability(df, var_targets)

    if fallback_used:
        print(_c("  Note: matrix NetCDF for {} does not exist; read from master NetCDF instead (slower)".format(
            ", ".join(fallback_used)), _C.DIM))

    _print_availability(avail_map, var_targets)

    for res, df in all_dfs.items():
        _print_timeseries(df, var_targets, res, PREVIEW_ROWS)

    _print_source_info(meta["sources_used"], meta["source_lookup"])

    if OUT_CSV:
        frames = []
        for res, df in all_dfs.items():
            tmp = df.copy()
            tmp.insert(0, "resolution", res)
            for key, value in source_summary.items():
                tmp[key] = value
            frames.append(tmp)
        if frames:
            combined  = pd.concat(frames, ignore_index=True)
            out_path  = Path(OUT_CSV).expanduser().resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            combined.to_csv(out_path, index=False)
            print(_c("\nExported CSV ({:,} rows): {}".format(len(combined), out_path), _C.GREEN))

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
