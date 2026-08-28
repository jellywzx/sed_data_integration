#!/usr/bin/env python3
"""
Prepare data for a basin-merge example figure.

Reads existing S5 / S7 / release output (does NOT re-run basin tracer or
re-assign cluster_id) and writes a self-contained output bundle under
output/figure_basin_merge_example/ containing:

  data/figure_basin_merge_stations.csv   -- station table with figure_role
  data/figure_basin_merge_pairs.csv      -- all station-pair constraints
  data/figure_basin_merge_basins.gpkg    -- subset polygon GPKG  (sqlite3)
  data/figure_basin_merge_readme.md      -- summary of selections
  final/figure_basin_merge_preview.png   -- optional preview  (matplotlib)
  scripts/prepare_figure_basin_merge_example.py  -- self-copy
"""

import argparse
import csv
import math
import os
import shutil
import sqlite3
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = SCRIPT_ROOT / "output"
OUTPUT_OTHER = SCRIPT_ROOT / "output_other"

S5_STATION_CSV = OUTPUT_DIR / "s5_basin_clustered_stations.csv"
RELEASE_BASINS_GPKG = (
    OUTPUT_DIR / "sed_reference_release" / "sed_reference_cluster_basins.gpkg"
)
S7_BASINS_GPKG = OUTPUT_DIR / "s7_cluster_basins.gpkg"
S4_BASINS_GPKG = OUTPUT_DIR / "s4_upstream_basins.gpkg"

DEFAULT_OUT_DIR = OUTPUT_OTHER / "figure_basin_merge_example"

DEFAULT_MAX_DISTANCE_M = 5000.0
DEFAULT_MAX_REL_ERROR = 0.10

# ---------------------------------------------------------------------------
# User configuration -- edit these to change default behavior without CLI args
# ---------------------------------------------------------------------------

CONFIG = {
    # Target basin ID. Known examples:
    #   77045760  -- Mexico (original example, has chain failure)
    #   77005478  -- Sacramento River, CA
    #   72055451  -- Milwaukee River, WI
    #   72055822  -- Menomonee River, WI
    "main_basin_id": 77005478,
    # Output subdirectory name (under output_other/)
    "out_dir_name": "figure_basin_merge_example_sacramento",
    # Number of background basins to include
    "background_count": 3,
    # Max station distance for merging (meters)
    "max_distance_m": 5000.0,
    # Max relative error for upstream area
    "max_rel_error": 0.10,
    # Exclude satellite/remote sensing stations?
    "skip_satellite": True,
    # Generate preview PNG?
    "preview": True,
    # Preview image DPI
    "dpi": 200,
}

# Column order for station output CSV
STATION_CSV_COLUMNS = [
    "station_id", "cluster_id", "basin_id", "basin_status",
    "observation_type", "lat", "lon", "uparea_merit", "figure_role",
]

PAIR_CSV_COLUMNS = [
    "station_id_a", "station_id_b",
    "distance_m", "uparea_rel_error",
    "distance_within_limit", "area_within_limit", "dual_pass",
    "pair_role", "recommended_line_style",
]

SATELLITE_OBSERVATION_TYPES = frozenset(
    ["satellite", "remote_sensing", "remote_sensing_observation",
     "satellite_observation"]
)

WGS84_SRS_DEF = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,'
    'AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],'
    'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],'
    'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],'
    'AUTHORITY["EPSG","4326"]]'
)

GPKG_APPLICATION_ID = 1196444487
GPKG_USER_VERSION = 10200

# ---------------------------------------------------------------------------
# Helpers (copied from basin_station_merge.py)
# ---------------------------------------------------------------------------


def _normalize_observation_type(value):
    if value is None:
        return ""
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def _is_satellite_observation_type(value):
    return _normalize_observation_type(value) in SATELLITE_OBSERVATION_TYPES


def haversine_distance_m(lat1, lon1, lat2, lon2):
    """Great-circle distance in metres."""
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2.0) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2.0) ** 2)
    return 2.0 * R * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def symmetric_rel_error(a, b):
    """abs(a - b) / max(abs(a), abs(b))."""
    denom = max(abs(a), abs(b))
    if denom == 0.0:
        return 0.0 if a == b else float("inf")
    return abs(a - b) / denom


def _validate_uparea(value):
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(v) and v > 0


# ---------------------------------------------------------------------------
# GPKG utilities  (pure sqlite3, no geopandas)
# ---------------------------------------------------------------------------


def _init_gpkg_metadata(conn):
    """Create mandatory GeoPackage metadata tables and insert SRS 4326."""
    # Python 3.6 sqlite3 does not support parameterized PRAGMA
    conn.execute("PRAGMA application_id = %d" % GPKG_APPLICATION_ID)
    conn.execute("PRAGMA user_version = %d" % GPKG_USER_VERSION)

    conn.execute("""CREATE TABLE IF NOT EXISTS gpkg_spatial_ref_sys (
        srs_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL PRIMARY KEY,
        organization TEXT NOT NULL,
        organization_coordsys_id INTEGER NOT NULL,
        definition TEXT NOT NULL,
        description TEXT)""")
    conn.execute(
        "INSERT OR IGNORE INTO gpkg_spatial_ref_sys "
        "(srs_name, srs_id, organization, organization_coordsys_id, "
        " definition, description) VALUES (?, ?, ?, ?, ?, ?)",
        ("WGS 84 geodetic", 4326, "EPSG", 4326, WGS84_SRS_DEF, "WGS84"))

    conn.execute("""CREATE TABLE IF NOT EXISTS gpkg_contents (
        table_name TEXT NOT NULL PRIMARY KEY,
        data_type TEXT NOT NULL,
        identifier TEXT,
        description TEXT DEFAULT '',
        last_change TEXT NOT NULL
            DEFAULT (strftime('%%Y-%%m-%%dT%%H:%%M:%%fZ','now')),
        min_x REAL, max_x REAL, min_y REAL, max_y REAL,
        srs_id INTEGER,
        CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id)
            REFERENCES gpkg_spatial_ref_sys(srs_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS gpkg_geometry_columns (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        geometry_type_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL,
        z TINYINT NOT NULL DEFAULT 0,
        m TINYINT NOT NULL DEFAULT 0,
        CONSTRAINT pk_gc PRIMARY KEY (table_name, column_name),
        CONSTRAINT fk_gc_tn FOREIGN KEY (table_name)
            REFERENCES gpkg_contents(table_name),
        CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id)
            REFERENCES gpkg_spatial_ref_sys(srs_id))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS gpkg_extensions (
        table_name TEXT, column_name TEXT,
        extension_name TEXT NOT NULL,
        definition TEXT NOT NULL,
        scope TEXT NOT NULL,
        CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name))""")

    conn.execute("""CREATE TABLE IF NOT EXISTS gpkg_ogr_contents (
        table_name TEXT NOT NULL PRIMARY KEY,
        feature_count INTEGER DEFAULT NULL)""")

    conn.commit()


def _extract_gpkg_column_info(conn, table):
    """Return column metadata dicts for *table*."""
    cur = conn.execute('PRAGMA table_info("%s")' % table)
    cols = []
    for cid, name, typ, notnull, dflt, pk in cur.fetchall():
        cols.append({"cid": cid, "name": name, "type": typ,
                      "notnull": bool(notnull), "dflt": dflt, "pk": bool(pk)})
    return cols


def _make_point_wkb_hex(lon, lat):
    """Build WKB hex for a 2D POINT in EPSG:4326 (little-endian)."""
    wkb = bytearray()
    wkb.append(0x01)  # little-endian
    wkb.extend(struct.pack("<I", 1))   # Point type
    wkb.extend(struct.pack("<d", lon))
    wkb.extend(struct.pack("<d", lat))
    return wkb.hex().upper()


def _create_feature_table(conn, table, col_info, geom_col="geom",
                           geom_type="MULTIPOLYGON"):
    """Create a feature table and register it in gpkg metadata."""
    col_defs = []
    for c in col_info:
        notnull = " NOT NULL" if c["notnull"] else ""
        pk = " PRIMARY KEY" if c["pk"] else ""
        col_defs.append('"%s" %s%s%s' % (c["name"],
                        c["type"] if c["type"] else "TEXT",
                        notnull, pk))
    sql = "CREATE TABLE IF NOT EXISTS \"%s\" (%s)" % (table,
          ",\n  ".join(col_defs))
    conn.execute(sql)

    last_change = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    conn.execute(
        "INSERT OR REPLACE INTO gpkg_contents "
        "(table_name, data_type, identifier, last_change, srs_id) "
        "VALUES (?, 'features', ?, ?, 4326)",
        (table, table, last_change))
    conn.execute(
        "INSERT OR REPLACE INTO gpkg_geometry_columns "
        "(table_name, column_name, geometry_type_name, srs_id, z, m) "
        "VALUES (?, ?, ?, 4326, 0, 0)",
        (table, geom_col, geom_type))
    conn.execute(
        "INSERT OR IGNORE INTO gpkg_ogr_contents "
        "(table_name, feature_count) VALUES (?, 0)",
        (table,))
    conn.commit()


def _read_rows_for_basins(src_gpkg, src_table, basin_ids):
    """Read feature rows matching *basin_ids* from *src_gpkg*.

    Returns (col_info, rows_list) where each row is a tuple matching
    the column order of *src_table*.
    """
    conn = sqlite3.connect(str(src_gpkg))
    try:
        col_info = _extract_gpkg_column_info(conn, src_table)
        col_names = ['"%s"' % c["name"] for c in col_info]

        if len(basin_ids) == 1:
            where = "ABS(basin_id - ?) < 0.5"
            params = (float(basin_ids[0]),)
        else:
            clauses = " OR ".join(
                "ABS(basin_id - %d) < 0.5" % int(b) for b in basin_ids)
            where = clauses
            params = ()

        sql = "SELECT %s FROM \"%s\" WHERE %s" % (
              ", ".join(col_names), src_table, where)
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    return col_info, rows


def _write_rows_to_gpkg(dst_path, dst_table, col_info, rows, geom_type,
                           extent=None):
    """Write rows into a new GPKG at *dst_path*.

    *col_info* from _extract_gpkg_column_info; *rows* from
    _read_rows_for_basins.
    *extent*: optional (min_x, max_x, min_y, max_y) for gpkg_contents.
    """
    exists = dst_path.is_file()
    conn = sqlite3.connect(str(dst_path))
    try:
        if not exists:
            _init_gpkg_metadata(conn)
        else:
            conn.execute('DROP TABLE IF EXISTS "%s"' % dst_table)
        _create_feature_table(conn, dst_table, col_info, "geom", geom_type)

        col_names = [c["name"] for c in col_info]
        placeholders = ", ".join("?" for _ in col_names)
        insert_sql = 'INSERT INTO "%s" (%s) VALUES (%s)' % (
            dst_table,
            ", ".join('"%s"' % c for c in col_names),
            placeholders)
        conn.executemany(insert_sql, rows)
        n = len(rows)

        if extent is not None:
            _update_gpkg_contents_extent(conn, dst_table, extent)

        conn.execute(
            "UPDATE gpkg_ogr_contents SET feature_count = ? "
            "WHERE table_name = ?", (n, dst_table))
        conn.commit()
    finally:
        conn.close()

    return len(rows)


def _compute_basin_extents(src_gpkg, src_table, basin_ids, rtree_table=None):
    """Read R-tree from *src_gpkg* for *basin_ids* and return a dict:
      basin_id -> (min_lon, max_lon, min_lat, max_lat)
    Also returns the overall (min_lon, max_lon, min_lat, max_lat).

    Falls back to None if R-tree is unavailable.
    """
    if rtree_table is None:
        rtree_table = "rtree_%s_geom" % src_table

    conn = sqlite3.connect(str(src_gpkg))
    try:
        # Check if rtree table exists
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (rtree_table,))
        if not cur.fetchone():
            return None, None

        clauses = " OR ".join(
            "ABS(f.basin_id - %d) < 0.5" % int(b) for b in basin_ids)
        sql = (
            "SELECT ROUND(f.basin_id) as bid,"
            "       MIN(r.minx), MAX(r.maxx),"
            "       MIN(r.miny), MAX(r.maxy) "
            'FROM "%s" f '
            'JOIN "%s" r ON r.id = f.fid '
            "WHERE (%s) "
            "GROUP BY ROUND(f.basin_id) "
            "ORDER BY ROUND(f.basin_id)" %
            (src_table, rtree_table, clauses))
        cur = conn.execute(sql)
        extents = {}
        overall_minx = overall_miny = float("inf")
        overall_maxx = overall_maxy = float("-inf")
        for bid, minx, maxx, miny, maxy in cur.fetchall():
            bid = int(bid)
            extents[bid] = (minx, maxx, miny, maxy)
            overall_minx = min(overall_minx, minx)
            overall_maxx = max(overall_maxx, maxx)
            overall_miny = min(overall_miny, miny)
            overall_maxy = max(overall_maxy, maxy)
        overall = (overall_minx, overall_maxx, overall_miny, overall_maxy)
    finally:
        conn.close()

    return extents, overall


def _update_gpkg_contents_extent(conn, table, extent):
    """Update min_x/max_x/min_y/max_y in gpkg_contents for *table*.

    *extent*: (min_x, max_x, min_y, max_y).
    """
    minx, maxx, miny, maxy = extent
    conn.execute(
        "UPDATE gpkg_contents SET min_x=?, max_x=?, min_y=?, max_y=? "
        "WHERE table_name=?",
        (minx, maxx, miny, maxy, table))
    conn.commit()


# ---------------------------------------------------------------------------
# Polygon source discovery
# ---------------------------------------------------------------------------


def find_polygon_source(basin_id, gpkg_chain):
    """Return (gpkg, table) for the first GPKG that has basin_id polygons."""
    for label, gpkg, table in gpkg_chain:
        if not gpkg.is_file():
            continue
        try:
            conn = sqlite3.connect(str(gpkg))
            try:
                cur = conn.execute(
                    'SELECT COUNT(*) FROM "%s" WHERE ABS(basin_id - ?) < 0.5'
                    % table, (float(basin_id),))
                if cur.fetchone()[0] > 0:
                    return gpkg, table
            finally:
                conn.close()
        except Exception:
            continue
    return None, None


# ---------------------------------------------------------------------------
# Chain-failure detection (station-pair level)
# ---------------------------------------------------------------------------


def _detect_chain_failure(station_rows, max_dist, max_rel_err):
    """Find chain failure: A-X passes, X-B passes, but A-B fails.

    Unlike cluster-level analysis, this works at the individual station
    level to catch cases where a bridge station is in the *same* cluster
    as one of the endpoints (e.g., cluster 45412 contains stations 45412
    and 45413; 45413 bridges to 45414 but 45412 cannot).
    """
    n = len(station_rows)
    chain_fail_pairs = []
    chain_pass_pairs = []

    by_id = {r["station_id"]: r for r in station_rows}

    # Precompute all pair results
    pair_results = {}
    for i in range(n):
        for j in range(i + 1, n):
            a, b = station_rows[i], station_rows[j]
            sid_a, sid_b = a["station_id"], b["station_id"]
            d = haversine_distance_m(
                a["lat"], a["lon"], b["lat"], b["lon"])
            rel = symmetric_rel_error(
                float(a.get("uparea_merit") or 0),
                float(b.get("uparea_merit") or 0))
            d_pass = d <= max_dist
            r_pass = rel <= max_rel_err
            same_cl = a["cluster_id"] == b["cluster_id"]
            pair_results[(sid_a, sid_b)] = pair_results[(sid_b, sid_a)] = {
                "dist": d, "rel": rel, "pass": d_pass and r_pass,
                "d_pass": d_pass, "r_pass": r_pass, "same_cluster": same_cl,
            }

    seen_fail = set()
    seen_pass = set()

    for (sid_a, sid_b), pr in pair_results.items():
        if (sid_a, sid_b) in seen_fail or (sid_a, sid_b) in seen_pass:
            continue
        if pr["pass"] or pr["same_cluster"]:
            continue

        # Failing cross-cluster pair. Look for a bridge X.
        found = False
        for sid_x in by_id:
            if sid_x == sid_a or sid_x == sid_b:
                continue
            pr_ax = pair_results.get((sid_a, sid_x))
            pr_xb = pair_results.get((sid_x, sid_b))
            if pr_ax and pr_xb and pr_ax["pass"] and pr_xb["pass"]:
                key = tuple(sorted([sid_a, sid_b]))
                if key not in seen_fail:
                    chain_fail_pairs.append(
                        (sid_a, sid_b, pr["dist"], pr["rel"]))
                    seen_fail.add(key)
                for bridge_key in [
                        tuple(sorted([sid_a, sid_x])),
                        tuple(sorted([sid_x, sid_b]))]:
                    if bridge_key not in seen_pass:
                        bp = pair_results[bridge_key]
                        chain_pass_pairs.append(
                            (bridge_key[0], bridge_key[1], bp["dist"]))
                        seen_pass.add(bridge_key)
                found = True
                break

    return {
        "chain_fail_pairs": chain_fail_pairs,
        "chain_pass_pairs": chain_pass_pairs,
        "chain_fail_found": len(chain_fail_pairs) > 0,
        "fallback_pair": None,
    }


# ---------------------------------------------------------------------------
# Background basin selection
# ---------------------------------------------------------------------------


def select_background_basins(df, main_basin_id, n_bg, gpkg_chain):
    """Select N nearest resolved basins with polygon data."""
    main_mask = (df["basin_id"].fillna(0).astype(float).round()
                 == float(main_basin_id))
    main_centroid = (df.loc[main_mask, "lat"].mean(),
                     df.loc[main_mask, "lon"].mean())

    # Filter to candidate resolved basins
    mask = (df["basin_status"].fillna("").astype(str).str.strip().str.lower()
            == "resolved")
    mask &= df["basin_id"].notna()
    if "observation_type" in df.columns:
        mask &= ~df["observation_type"].map(_is_satellite_observation_type)
    mask &= (abs(df["basin_id"].fillna(0).astype(float).round()
                 - float(main_basin_id)) > 0.5)
    candidates = df[mask].copy()
    if candidates.empty:
        return []

    candidates["_bid"] = candidates["basin_id"].astype(float).round().astype(int)
    centroids = candidates.groupby("_bid")[["lat", "lon"]].mean()

    scored = []
    for bid, (clat, clon) in centroids.iterrows():
        dist = haversine_distance_m(
            main_centroid[0], main_centroid[1], clat, clon)
        scored.append((dist, int(bid)))

    scored.sort(key=lambda x: x[0])

    selected = []
    for dist, bid in scored:
        if len(selected) >= n_bg:
            break
        for label, gpkg, table in gpkg_chain:
            if not gpkg.is_file():
                continue
            try:
                conn = sqlite3.connect(str(gpkg))
                try:
                    cur = conn.execute(
                        'SELECT COUNT(*) FROM "%s" WHERE ABS(basin_id - ?) < 0.5'
                        % table, (float(bid),))
                    if cur.fetchone()[0] > 0:
                        selected.append(bid)
                        break
                finally:
                    conn.close()
            except Exception:
                continue

    return selected


# ---------------------------------------------------------------------------
# Pipeline-style filter
# ---------------------------------------------------------------------------


def _filter_pipeline(df):
    """Resolved, valid basin_id, non-satellite, finite lat/lon/area."""
    keep = pd.Series(True, index=df.index)

    if "basin_status" in df.columns:
        keep &= (df["basin_status"].fillna("").astype(str)
                 .str.strip().str.lower().eq("resolved"))
    if "basin_id" in df.columns:
        keep &= pd.to_numeric(df["basin_id"], errors="coerce").notna()
    if "observation_type" in df.columns:
        keep &= ~df["observation_type"].map(_is_satellite_observation_type)
    for col in ("lat", "lon"):
        if col in df.columns:
            v = pd.to_numeric(df[col], errors="coerce")
            keep &= v.notna() & np.isfinite(v)
    return df[keep].copy()


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def write_station_csv(path, station_rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STATION_CSV_COLUMNS)
        writer.writeheader()
        for row in station_rows:
            writer.writerow({k: row.get(k, "") for k in STATION_CSV_COLUMNS})
    return len(station_rows)


def write_pair_csv(path, pair_rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PAIR_CSV_COLUMNS)
        writer.writeheader()
        for row in pair_rows:
            writer.writerow({k: row.get(k, "") for k in PAIR_CSV_COLUMNS})
    return len(pair_rows)


def write_readme(path, info, chain, station_rows, pair_rows,
                 bg_basin_ids, poly_src, poly_count, poly_table,
                 extent_info=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    lines.append("# Basin Merge Example — Figure Data Summary\n")
    lines.append("Generated: %s\n" % datetime.now(timezone.utc)
                 .strftime("%Y-%m-%d %H:%M UTC"))
    lines.append("## Input Files\n")
    lines.append("- Station CSV: `%s`" % info.get("station_csv", ""))
    for g in info.get("gpkg_chain_labels", []):
        lines.append("- GPKG: `%s`" % g)
    lines.append("")

    main_id = info.get("main_basin_id")
    lines.append("## Main Basin: `basin_id = %s`\n" % main_id)
    n_total = len(station_rows)
    n_insitu = sum(1 for r in station_rows
                   if not _is_satellite_observation_type(
                       r.get("observation_type", "")))
    n_main = sum(1 for r in station_rows if r.get("figure_role", "").startswith("main_"))
    n_main_insitu = sum(1 for r in station_rows if r.get("figure_role", "").startswith("main_") and not _is_satellite_observation_type(r.get("observation_type", "")))
    lines.append("- Total stations: %d" % n_main)
    lines.append("- In-situ: %d" % n_main_insitu)
    lines.append("- Satellite: %d" % (n_main - n_main_insitu))

    lines.append("\n### Cluster Assignments (from S5 pipeline)\n")
    # Only list main-basin stations in cluster section
    main_for_clusters = [r for r in station_rows if r.get("figure_role", "").startswith("main_")]
    cluster_map = {}
    for r in main_for_clusters:
        cid = int(r.get("cluster_id", 0))
        cluster_map.setdefault(cid, []).append(int(r.get("station_id", 0)))
    for cid in sorted(cluster_map):
        members = cluster_map[cid]
        if len(members) > 1:
            lines.append("- **cluster_id = %d**: stations %s (merged)"
                         % (cid, ", ".join(str(m) for m in members)))
        else:
            lines.append("- **cluster_id = %d**: station %s (singleton)"
                         % (cid, members[0]))

    lines.append("\n### Chain Analysis\n")
    if chain["chain_fail_found"]:
        lines.append("Chain failure detected (complete-linkage clique break):\n")
        for a, b, d, rel in chain["chain_fail_pairs"]:
            lines.append("  - %d ↔ %d: distance=%.0fm, rel_error=%.4f → **FAIL**"
                         % (a, b, d, rel))
        lines.append("\nBridging pass pairs:")
        for a, b, d in chain["chain_pass_pairs"]:
            lines.append("  - %d ↔ %d: distance=%.0fm → pass" % (a, b, d))
    else:
        lines.append("No chain failure found in this basin.")
        fb = chain.get("fallback_pair")
        if fb:
            lines.append("Fallback pair: %d ↔ %d" % (fb[0], fb[1]))
    lines.append("")

    lines.append("### All Station Pairs\n")
    lines.append("| A | B | Distance (m) | Area Err | D Pass | A Pass | Both | Role |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for pr in pair_rows:
        lines.append("| %d | %d | %.0f | %.4f | %s | %s | %s | %s |" % (
            pr.get("station_id_a", ""), pr.get("station_id_b", ""),
            pr.get("distance_m", 0), pr.get("uparea_rel_error", 0),
            pr.get("distance_within_limit", ""),
            pr.get("area_within_limit", ""),
            pr.get("dual_pass", ""),
            pr.get("pair_role", "")))

    lines.append("\n## Background Basins\n")
    if bg_basin_ids:
        lines.append("Selected: %s\n" % ", ".join(str(b) for b in bg_basin_ids))
    else:
        lines.append("None selected.\n")

    lines.append("## Polygon Output\n")
    if poly_src:
        lines.append("Source GPKG: `%s` → table `%s` → %d features\n"
                     % (poly_src, poly_table or "?", poly_count))
    else:
        lines.append("No polygon data found.\n")

    lines.append("## Station Roles\n")
    roles = {}
    for r in station_rows:
        role = r.get("figure_role", "unknown")
        roles[role] = roles.get(role, 0) + 1
    for role in sorted(roles):
        lines.append("- %s: %d" % (role, roles[role]))
    lines.append("")

    # Extent info
    if extent_info is not None:
        main_ext = extent_info.get("main")
        overall = extent_info.get("overall")
        bg_extents = extent_info.get("background", {})
        lines.append("## Basin Extent (Bounding Box)\n")
        if main_ext:
            minx, maxx, miny, maxy = main_ext
            lines.append("### Main Basin (`basin_id = %s`)" % info.get("main_basin_id", ""))
            lines.append("")
            lines.append("- Longitude: %.4f\u00b0 ~ %.4f\u00b0 (width = %.4f\u00b0)" % (minx, maxx, maxx-minx))
            lines.append("- Latitude:  %.4f\u00b0 ~ %.4f\u00b0 (height = %.4f\u00b0)" % (miny, maxy, maxy-miny))
            lines.append("")
        if bg_extents:
            lines.append("### Background Basins")
            lines.append("")
            for bid in sorted(bg_extents):
                minx, maxx, miny, maxy = bg_extents[bid]
                lines.append("- `basin_id = %d`:  lon [%.4f, %.4f]  lat [%.4f, %.4f]" % (bid, minx, maxx, miny, maxy))
            lines.append("")
        if overall:
            minx, maxx, miny, maxy = overall
            lines.append("### Overall (all basins)")
            lines.append("")
            lines.append("- Longitude: %.4f\u00b0 ~ %.4f\u00b0 (width = %.4f\u00b0)" % (minx, maxx, maxx-minx))
            lines.append("- Latitude:  %.4f\u00b0 ~ %.4f\u00b0 (height = %.4f\u00b0)" % (miny, maxy, maxy-miny))
            lines.append("")

    text = "\n".join(lines)
    with open(str(path), "w") as f:
        f.write(text)
    return text


# ---------------------------------------------------------------------------
# Preview (matplotlib, optional)
# ---------------------------------------------------------------------------


def generate_preview(station_rows, pair_rows, gpkg_path, gpkg_table,
                     bg_basin_ids, out_path, dpi=200):
    """Generate a PNG preview map."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("  [preview] matplotlib not available, skipping")
        return None

    out_path.parent.mkdir(parents=True, exist_ok=True)

    color_map = {
        "main_merged": "#2ca02c",
        "main_singleton": "#1f77b4",
        "main_satellite": "#9467bd",
        "background": "#d3d3d3",
    }
    marker_map = {
        "main_merged": "o", "main_singleton": "s",
        "main_satellite": "^", "background": "o",
    }
    size_map = {
        "main_merged": 80, "main_singleton": 60,
        "main_satellite": 50, "background": 30,
    }

    fig, ax = plt.subplots(1, 1, figsize=(10, 8))

    main_rows = [r for r in station_rows
                 if r.get("figure_role", "").startswith("main_")]
    if not main_rows:
        print("  [preview] no main stations, skipping")
        plt.close(fig)
        return None

    lats = [float(r["lat"]) for r in main_rows]
    lons = [float(r["lon"]) for r in main_rows]
    margin = 0.3
    xlim = (min(lons) - margin, max(lons) + margin)
    ylim = (min(lats) - margin, max(lats) + margin)

    # Draw polygons
    try:
        if gpkg_path.is_file():
            conn = sqlite3.connect(str(gpkg_path))
            try:
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (gpkg_table,))
                if cur.fetchone() is not None:
                    cur2 = conn.execute(
                        'SELECT "geom", basin_id FROM "%s"' % gpkg_table)
                    for geom_blob, bid in cur2:
                        bid_rounded = round(bid) if bid else 0
                        is_main = abs(float(bid_rounded)
                                      - float(main_rows[0].get("basin_id", 0))) < 0.5
                        color = "#ff7f0e" if is_main else "#d3d3d3"
                        alpha = 0.15 if is_main else 0.08
                        _plot_wkb_polygon(
                            ax, geom_blob, color=color, alpha=alpha)
            finally:
                conn.close()
    except Exception as e:
        print("  [preview] polygon draw error: %s" % e)

    # Pair lines
    for pr in pair_rows:
        role = pr.get("pair_role", "")
        if role == "regular_fail":
            continue
        a_id = int(pr["station_id_a"])
        b_id = int(pr["station_id_b"])
        a_row = next((r for r in station_rows if int(r["station_id"]) == a_id), None)
        b_row = next((r for r in station_rows if int(r["station_id"]) == b_id), None)
        if not a_row or not b_row:
            continue
        ax.plot(
            [float(a_row["lon"]), float(b_row["lon"])],
            [float(a_row["lat"]), float(b_row["lat"])],
            color="#e6550d" if role == "chain_fail" else "#2ca02c",
            linewidth=2 if role == "chain_fail" else 1.5,
            linestyle="--" if role == "chain_fail" else "-",
            alpha=0.7, zorder=2)

    # Station markers
    for r in station_rows:
        role = r.get("figure_role", "background")
        color = color_map.get(role, "#333333")
        marker = marker_map.get(role, "o")
        s = size_map.get(role, 40)
        ax.scatter(float(r["lon"]), float(r["lat"]),
                   c=color, marker=marker, s=s,
                   edgecolors="black", linewidths=0.5, zorder=5,
                   label="_nolegend_")
        if role.startswith("main_"):
            ax.annotate(str(r["station_id"]),
                        (float(r["lon"]), float(r["lat"])),
                        xytext=(6, 6), textcoords="offset points",
                        fontsize=7, zorder=6)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Basin Merge Example (basin_id = %s)" % main_rows[0].get("basin_id", "?"))
    ax.grid(True, alpha=0.3)
    ax.set_aspect(1.0 / math.cos(math.radians(sum(lats) / len(lats))))

    from matplotlib.lines import Line2D
    has_sat = any(r.get('figure_role') == 'main_satellite' for r in station_rows)
    legend = [
        Line2D([0], [0], marker="o", color="w",
               markerfacecolor="#2ca02c", markersize=10, label="Merged"),
        Line2D([0], [0], marker="s", color="w",
               markerfacecolor="#1f77b4", markersize=10, label="Singleton"),
    ]
    if has_sat:
        legend.append(
            Line2D([0], [0], marker="^", color="w",
                   markerfacecolor="#9467bd", markersize=10, label="Satellite"))
    legend.extend([
        Line2D([0], [0], color="#2ca02c", linewidth=1.5, label="Pass"),
        Line2D([0], [0], color="#e6550d", linewidth=2,
               linestyle="--", label="Chain fail"),
    ])
    ax.legend(handles=legend, loc="lower right", fontsize=8)

    fig.tight_layout()
    fig.savefig(str(out_path), dpi=dpi)
    plt.close(fig)
    return out_path


def _strip_gpkg_header(data):
    """Strip GeoPackage binary geometry header to return raw WKB."""
    if len(data) < 8 or data[:2] != b"GP":
        return data
    flags = data[2]
    env_type = flags & 0x03
    # env_type: 0=none, 1=XY(4 doubles), 2=XYZ(6 doubles), 3=XYZM(8 doubles)
    env_sizes = [0, 32, 48, 64]
    skip = 8 + env_sizes[env_type]
    if skip >= len(data):
        return data
    # Some writers embed the envelope despite env_type=0; scan for WKB start
    if skip < len(data) and data[skip] not in (0, 1):
        for i in range(8, min(len(data) - 9, 100)):
            if data[i] in (0, 1):
                bo = data[i]
                gt = struct.unpack("<I" if bo == 1 else ">I", data[i+1:i+5])[0]
                if (gt & 0x7FFFFFFF) in (3, 6, 11):
                    skip = i
                    break
    if skip >= len(data):
        return data
    return data[skip:]


def _plot_wkb_polygon(ax, geom_blob, color="#ff7f0e", alpha=0.15):
    """Parse and plot a WKB blob (MultiPolygon or Polygon)."""
    try:
        wkb = _strip_gpkg_header(bytes(geom_blob))
        from shapely.wkb import loads as wkb_loads
        geom = wkb_loads(wkb)
        if geom is None or geom.is_empty:
            return
        import geopandas as gpd
        gdf = gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326")
        gdf.plot(ax=ax, facecolor=color, edgecolor="none", alpha=alpha)
    except Exception:
        try:
            _plot_wkb_simple(ax, geom_blob, color, alpha)
        except Exception:
            pass


def _plot_wkb_simple(ax, geom_blob, color, alpha):
    """Minimal WKB polygon parser (no shapely)."""
    try:
        data = _strip_gpkg_header(bytes(geom_blob))
        if len(data) < 9:
            return
        endian = data[0]
        geom_type = (struct.unpack("<I", data[1:5])[0] if endian == 1
                     else struct.unpack(">I", data[1:5])[0])
        pos = 5

        if geom_type == 6:  # MultiPolygon
            n_polys = (struct.unpack("<I", data[pos:pos+4])[0] if endian == 1
                       else struct.unpack(">I", data[pos:pos+4])[0])
            pos += 4
            for _ in range(n_polys):
                if pos + 9 > len(data):
                    break
                p_endian = data[pos]
                p_type = (struct.unpack("<I", data[pos+1:pos+5])[0] if p_endian == 1
                          else struct.unpack(">I", data[pos+1:pos+5])[0])
                pos += 5
                if p_type != 3:
                    # skip unknown sub-geometry
                    continue
                pos = _parse_one_polygon_wkb_body(ax, data, pos, p_endian, color, alpha)
        elif geom_type == 3:  # single Polygon
            _parse_one_polygon_wkb_body(ax, data, pos, endian, color, alpha)
    except Exception:
        pass


def _parse_one_polygon_wkb(ax, data, pos, color, alpha):
    """Parse one Polygon from within a MultiPolygon at *pos*."""
    if pos + 5 > len(data):
        return
    p_endian = data[pos]
    p_type = (struct.unpack("<I", data[pos+1:pos+5])[0] if p_endian == 1
              else struct.unpack(">I", data[pos+1:pos+5])[0])
    if p_type != 3:
        return
    pos += 5
    _parse_one_polygon_wkb_body(ax, data, pos, p_endian, color, alpha)


def _parse_one_polygon_wkb_body(ax, data, pos, endian, color, alpha):
    """Parse ring coordinates starting at *pos*, plot filled polygon. Returns new pos."""
    if pos + 4 > len(data):
        return pos
    n_rings = (struct.unpack("<I", data[pos:pos+4])[0] if endian == 1
               else struct.unpack(">I", data[pos:pos+4])[0])
    pos += 4
    ring_idx = 0
    while ring_idx < n_rings and pos + 4 <= len(data):
        n_pts = (struct.unpack("<I", data[pos:pos+4])[0] if endian == 1
                 else struct.unpack(">I", data[pos:pos+4])[0])
        pos += 4
        coords = []
        for _ in range(n_pts):
            if pos + 16 > len(data):
                break
            x = (struct.unpack("<d", data[pos:pos+8])[0] if endian == 1
                 else struct.unpack(">d", data[pos:pos+8])[0])
            y = (struct.unpack("<d", data[pos+8:pos+16])[0] if endian == 1
                 else struct.unpack(">d", data[pos+8:pos+16])[0])
            coords.append((x, y))
            pos += 16
        if coords:
            xs, ys = zip(*coords)
            if ring_idx == 0:
                ax.fill(xs, ys, color=color, alpha=alpha, edgecolor="none")
                ax.plot(xs, ys, color=color,
                        alpha=min(alpha * 2, 0.5), linewidth=0.5)
        ring_idx += 1
    return pos


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Prepare basin merge example figure data (S5/S7/release).")
    parser.add_argument("--station-csv", type=Path, default=S5_STATION_CSV,
                        help="S5 clustered stations CSV")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="Output directory (default: output_other/<out_dir_name>)")
    parser.add_argument("--main-basin-id", type=int, default=None,
                        help='Target basin_id (default: CONFIG["main_basin_id"])')
    parser.add_argument("--background-count", type=int, default=None,
                        help="Number of background basins")
    parser.add_argument("--max-distance-m", type=float, default=None,
                        help="Max station distance (m)")
    parser.add_argument("--max-rel-error", type=float, default=None,
                        help="Max upstream area rel error")
    parser.add_argument("--skip-satellite", action="store_true",
                        help="Exclude satellite/remote sensing stations")
    parser.add_argument("--include-satellite", action="store_true",
                        help="Include satellite stations (overrides CONFIG skip_satellite)")
    parser.add_argument("--no-preview", action="store_true",
                        help="Skip preview figure")
    parser.add_argument("--dpi", type=int, default=None,
                        help="Preview DPI")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv=None):
    args = parse_args(argv)

    # Resolve config: CONFIG as base, CLI args as overrides
    cfg = dict(CONFIG)
    if args.main_basin_id is not None:
        cfg["main_basin_id"] = args.main_basin_id
    if args.out_dir is not None:
        cfg["out_dir"] = str(args.out_dir.resolve())
    else:
        cfg["out_dir"] = str(OUTPUT_OTHER / cfg["out_dir_name"])
    if args.background_count is not None:
        cfg["background_count"] = args.background_count
    if args.max_distance_m is not None:
        cfg["max_distance_m"] = args.max_distance_m
    if args.max_rel_error is not None:
        cfg["max_rel_error"] = args.max_rel_error
    if args.include_satellite:
        cfg["skip_satellite"] = False
    elif args.skip_satellite:
        cfg["skip_satellite"] = True
    if args.no_preview:
        cfg["preview"] = False
    if args.dpi is not None:
        cfg["dpi"] = args.dpi

    main_basin_id = cfg["main_basin_id"]
    file_prefix = "figure_basin_merge_%s" % main_basin_id
    out_dir = Path(cfg["out_dir"]).resolve()
    station_csv = Path(args.station_csv).resolve()

    data_dir = out_dir / "data"
    final_dir = out_dir / "final"
    scripts_dir = out_dir / "scripts"

    data_dir.mkdir(parents=True, exist_ok=True)
    final_dir.mkdir(parents=True, exist_ok=True)
    scripts_dir.mkdir(parents=True, exist_ok=True)

    print("== Basin Merge Example Data Preparation ==")
    print("  Main basin ID: %s" % main_basin_id)
    print("  Output: %s" % out_dir)
    print("")

    # --- Load S5 ---
    if not station_csv.is_file():
        print("ERROR: %s not found" % station_csv)
        return 1

    print("  Loading S5: %s ..." % station_csv)
    df = pd.read_csv(station_csv, low_memory=False)
    print("  Read %d rows, %d columns" % (len(df), len(df.columns)))

    basin_mask = (df["basin_id"].fillna(0).astype(float).round()
                  == float(main_basin_id))
    basin_df = df[basin_mask].copy()
    if basin_df.empty:
        print("ERROR: basin_id=%s not found" % main_basin_id)
        return 1
    print("  Found %d stations in basin %s" % (len(basin_df), main_basin_id))

    if cfg["skip_satellite"]:
        sat_mask = basin_df['observation_type'].astype(str).apply(
            lambda x: str(x).strip().lower().replace('-','_').replace(' ','_')
            in ('satellite','remote_sensing','remote_sensing_observation','satellite_observation'))
        basin_df = basin_df[~sat_mask].copy()
        print("  After removing satellites: %d stations" % len(basin_df))

    # --- figure_role ---
    station_rows = []
    for _, row in basin_df.iterrows():
        sid = int(row["station_id"])
        cid = int(row.get("cluster_id", sid) or sid)
        is_sat = _is_satellite_observation_type(
            row.get("observation_type", ""))
        if is_sat:
            role = "main_satellite"
        else:
            cluster_in_basin = basin_df[
                basin_df["cluster_id"].fillna(0).astype(int).eq(cid)]
            if len(cluster_in_basin) >= 2:
                role = "main_merged"
            else:
                role = "main_singleton"
        station_rows.append({
            "station_id": sid,
            "cluster_id": cid,
            "basin_id": main_basin_id,
            "basin_status": row.get("basin_status", ""),
            "observation_type": row.get("observation_type", ""),
            "lat": float(row.get("lat", 0)),
            "lon": float(row.get("lon", 0)),
            "uparea_merit": float(row.get("uparea_merit", 0) or 0),
            "figure_role": role,
        })

    # --- Pairs ---
    print("\n  Computing station pairs ...")
    insitu = [r for r in station_rows
              if r["figure_role"] != "main_satellite"]
    pair_rows = []
    for i in range(len(insitu)):
        for j in range(i + 1, len(insitu)):
            a, b = insitu[i], insitu[j]
            dist = haversine_distance_m(a["lat"], a["lon"], b["lat"], b["lon"])
            rel_err = symmetric_rel_error(a["uparea_merit"], b["uparea_merit"])
            d_pass = dist <= cfg["max_distance_m"]
            a_pass = rel_err <= cfg["max_rel_error"]
            dual = d_pass and a_pass
            same_cl = a["cluster_id"] == b["cluster_id"]
            role = "same_cluster" if same_cl else ("chain_pass" if dual else "regular_fail")
            style = "solid" if (same_cl or dual) else "dashed"
            pair_rows.append({
                "station_id_a": a["station_id"],
                "station_id_b": b["station_id"],
                "distance_m": round(dist, 2),
                "uparea_rel_error": round(rel_err, 6),
                "distance_within_limit": str(d_pass),
                "area_within_limit": str(a_pass),
                "dual_pass": str(dual),
                "pair_role": role,
                "recommended_line_style": style,
            })

    # --- Chain failure ---
    print("  Detecting chain failures ...")
    chain = _detect_chain_failure(insitu, cfg["max_distance_m"],
                                   cfg["max_rel_error"])
    if chain["chain_fail_found"]:
        print("  Chain failure: %d pair(s)" % len(chain["chain_fail_pairs"]))
        fail_ids = set()
        for a, b, _d, _rel in chain["chain_fail_pairs"]:
            fail_ids.add((a, b))
            fail_ids.add((b, a))
        pass_ids = set()
        for a, b, _d in chain["chain_pass_pairs"]:
            pass_ids.add((a, b))
            pass_ids.add((b, a))
        for pr in pair_rows:
            key = (pr["station_id_a"], pr["station_id_b"])
            if key in fail_ids:
                pr["pair_role"] = "chain_fail"
                pr["recommended_line_style"] = "highlight"
            elif key in pass_ids:
                pr["pair_role"] = "chain_pass"
                pr["recommended_line_style"] = "solid"
    else:
        print("  No chain failure found")
        mergeable = [pr for pr in pair_rows
                     if pr["dual_pass"] == "True"
                     and pr["pair_role"] != "same_cluster"]
        if mergeable:
            fb = mergeable[0]
            chain["fallback_pair"] = (fb["station_id_a"], fb["station_id_b"])
            print("  Fallback: %d - %d" % (fb["station_id_a"], fb["station_id_b"]))

    # --- GPKG chain ---
    gpkg_chain = []
    _check_gpkg(RELEASE_BASINS_GPKG, gpkg_chain, "release",
                "basin_annual", "basin_monthly", "basin_daily")
    _check_gpkg(S7_BASINS_GPKG, gpkg_chain, "s7", "basin_annual")
    _check_gpkg(S4_BASINS_GPKG, gpkg_chain, "s4", "s4_upstream_basins")

    print("\n  Searching for polygons ...")
    src_gpkg, src_table = find_polygon_source(main_basin_id, gpkg_chain)
    poly_source_label = "none"
    poly_count = 0

    # --- Background basins ---
    print("  Selecting background basins ...")
    bg_ids = select_background_basins(df, main_basin_id,
                                       cfg["background_count"], gpkg_chain)
    print("  Background: %s" % bg_ids)

    # --- Copy polygons ---
    gpkg_out = data_dir / ("%s_basins.gpkg" % file_prefix)
    extent_info = None
    if src_gpkg is not None:
        poly_source_label = src_gpkg.name
        all_ids = [main_basin_id] + bg_ids

        # Compute extents from R-tree (if available)
        rtree_table = "rtree_%s_geom" % src_table
        raw_extents, overall = _compute_basin_extents(
            src_gpkg, src_table, all_ids, rtree_table)
        extent_info = {"main": None, "background": {}, "overall": overall}
        if raw_extents:
            for bid, ext in raw_extents.items():
                if bid == main_basin_id:
                    extent_info["main"] = ext
                else:
                    extent_info["background"][bid] = ext

        col_info, rows = _read_rows_for_basins(src_gpkg, src_table, all_ids)
        if rows:
            poly_count = _write_rows_to_gpkg(
                gpkg_out, src_table, col_info, rows, "MULTIPOLYGON",
                extent=overall)
            print("  Copied %d polygons (%s + %d bg) from %s"
                  % (poly_count, main_basin_id, len(bg_ids), src_gpkg.name))
            if overall:
                print("  Extent: lon [%.4f, %.4f] lat [%.4f, %.4f]" % overall)
        else:
            print("  WARNING: no polygon rows found")
    else:
        print("  WARNING: no polygon GPKG found for basin %s" % main_basin_id)

    # --- Background station rows ---
    bg_station_rows = []
    for bid in bg_ids:
        mask = (df["basin_id"].fillna(0).astype(float).round().eq(float(bid))
                & df["basin_status"].fillna("").astype(str).str.strip()
                      .str.lower().eq("resolved"))
        subset = df[mask]
        if subset.empty:
            continue
        rep = subset.iloc[0]
        bg_station_rows.append({
            "station_id": int(rep["station_id"]),
            "cluster_id": int(rep.get("cluster_id", rep["station_id"])
                              or rep["station_id"]),
            "basin_id": bid,
            "basin_status": rep.get("basin_status", ""),
            "observation_type": rep.get("observation_type", ""),
            "lat": float(rep.get("lat", 0)),
            "lon": float(rep.get("lon", 0)),
            "uparea_merit": float(rep.get("uparea_merit", 0) or 0),
            "figure_role": "background",
        })

    all_station_rows = station_rows + bg_station_rows

    # --- Write outputs ---
    station_csv_out = data_dir / ("%s_stations.csv" % file_prefix)
    pair_csv_out = data_dir / ("%s_pairs.csv" % file_prefix)
    readme_out = data_dir / ("%s_readme.md" % file_prefix)
    preview_out = final_dir / ("%s_preview.png" % file_prefix)

    write_station_csv(station_csv_out, all_station_rows)
    write_pair_csv(pair_csv_out, pair_rows)

    info = {
        "station_csv": str(station_csv),
        "gpkg_chain_labels": [str(p) for _, p, _ in gpkg_chain],
        "main_basin_id": main_basin_id,
    }
    write_readme(readme_out, info, chain, all_station_rows, pair_rows,
                 bg_ids, poly_source_label, poly_count, src_table,
                 extent_info=extent_info)

    # Self-copy
    script_dst = scripts_dir / "prepare_figure_basin_merge_example.py"
    try:
        shutil.copy2(Path(__file__).resolve(), script_dst)
        print("\n  Self-copied to %s" % script_dst)
    except Exception:
        pass

    # Preview
    if not cfg["preview"] and gpkg_out.is_file():
        generate_preview(all_station_rows, pair_rows, gpkg_out,
                          src_table or "s4_upstream_basins", bg_ids,
                          preview_out, dpi=cfg["dpi"])

    # --- Summary ---
    print("\n== Output Summary ==")
    print("  Stations: %d" % len(all_station_rows))
    for role in sorted(set(r["figure_role"] for r in all_station_rows)):
        cnt = sum(1 for r in all_station_rows if r["figure_role"] == role)
        print("    - %s: %d" % (role, cnt))
    print("  Pairs: %d" % len(pair_rows))
    print("  Polygons: %d" % poly_count)
    print("  Background basins: %s" % bg_ids)
    print("  Chain failure: %s" % ("YES" if chain["chain_fail_found"] else "no"))
    print("")
    for p in (station_csv_out, pair_csv_out, gpkg_out, readme_out):
        print("  %s" % p)
    if preview_out.is_file():
        print("  %s (%.0f KB)" % (preview_out, preview_out.stat().st_size / 1024))
    print("  %s" % script_dst)
    print("")
    return 0


def _check_gpkg(path, chain, label, *tables):
    """If *path* has one of *tables*, add to chain."""
    if not path.is_file():
        return
    try:
        conn = sqlite3.connect(str(path))
        try:
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing = {r[0] for r in cur.fetchall()}
            for t in tables:
                if t in existing:
                    chain.append((label, path, t))
                    return
        finally:
            conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
