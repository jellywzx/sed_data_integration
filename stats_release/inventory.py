#!/usr/bin/env python3
"""Inventory and release-health statistics for the published release package."""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from stats_release.release_io import (
    add_common_args,
    clean_text,
    context_from_args,
    copy_report_to_docs,
    read_text_var,
    write_csv,
    write_markdown,
)
from stats_release.release_paths import PRODUCT_FILES
from stats_release.common_stats import pct
from stats_release.reporting import (
    append_table_section,
    display_path,
    fmt_float,
    fmt_int,
    safe_lines,
    sorted_markdown_table,
)


LOCAL_PATH_MARKERS = ("/share/home/", "/home/", "/Users/")


def _csv_rows(path: Path) -> int:
    try:
        return max(0, sum(1 for _ in path.open("r", encoding="utf-8", errors="ignore")) - 1)
    except Exception:
        return -1


def _nc_detail(ctx, file_name: str) -> str:
    try:
        with ctx.open_dataset(file_name, required=True) as ds:
            dims = ["{}={}".format(name, len(dim)) for name, dim in ds.dimensions.items()]
            return "; ".join(dims)
    except Exception as exc:
        return "cannot inspect NetCDF: {}".format(exc)


def _gpkg_detail(ctx, file_name: str) -> str:
    try:
        conn = ctx.sqlite_connect(file_name, required=True)
        if conn is None:
            return ""
        with conn:
            layers = pd.read_sql_query(
                "SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name",
                conn,
            )
        return "layers={}".format("|".join(layers["table_name"].astype(str).tolist()))
    except Exception as exc:
        return "cannot inspect GeoPackage: {}".format(exc)


def _registered_inventory_rows(ctx) -> list:
    rows = []
    for product, file_name in sorted(PRODUCT_FILES.items()):
        path = ctx.release_file(file_name)
        exists = path.is_file()
        suffixes = "".join(path.suffixes).lower()
        detail = ""
        row_count = ""
        if exists and suffixes.endswith(".csv"):
            row_count = _csv_rows(path)
        elif exists and path.suffix.lower() == ".nc":
            detail = _nc_detail(ctx, file_name)
        elif exists and path.suffix.lower() == ".gpkg":
            detail = _gpkg_detail(ctx, file_name)
        elif exists and suffixes.endswith(".csv.gz"):
            detail = "compressed CSV sidecar"
        elif exists and path.suffix.lower() == ".parquet":
            detail = "Parquet sidecar"
        rows.append(
            {
                "product": product,
                "registration_status": "registered",
                "file_name": file_name,
                "exists": int(exists),
                "size_mb": round(path.stat().st_size / 1024.0 / 1024.0, 6) if exists else 0.0,
                "row_count": row_count,
                "detail": detail,
            }
        )
    return rows


def _unregistered_inventory_rows(ctx) -> list:
    registered = set(PRODUCT_FILES.values())
    rows = []
    for path in sorted(p for p in ctx.release_dir.iterdir() if p.is_file()):
        if path.name in registered:
            continue
        rows.append(
            {
                "product": "",
                "registration_status": "unregistered",
                "file_name": path.name,
                "exists": 1,
                "size_mb": round(path.stat().st_size / 1024.0 / 1024.0, 6),
                "row_count": "",
                "detail": "file exists in release_dir but is not in PRODUCT_FILES",
            }
        )
    return rows


def build_inventory(ctx) -> pd.DataFrame:
    """Return registered products plus any unregistered top-level release files."""
    return pd.DataFrame(_registered_inventory_rows(ctx) + _unregistered_inventory_rows(ctx))


def build_release_files(ctx) -> pd.DataFrame:
    registered = {name: product for product, name in PRODUCT_FILES.items()}
    inventory = ctx.read_csv(PRODUCT_FILES["inventory_csv"], required=False)
    descriptions = {}
    kinds = {}
    if not inventory.empty and "file_name" in inventory.columns:
        for _, row in inventory.iterrows():
            name = clean_text(row.get("file_name", ""))
            if not name:
                continue
            descriptions[name] = clean_text(row.get("description", ""))
            kinds[name] = clean_text(row.get("kind", ""))
    rows = []
    for path in sorted(p for p in ctx.release_dir.iterdir() if p.is_file()):
        suffixes = "".join(path.suffixes).lower()
        if suffixes.endswith(".csv.gz"):
            file_type = "csv.gz"
        else:
            file_type = path.suffix.lower().lstrip(".") or "unknown"
        rows.append(
            {
                "file_name": path.name,
                "relative_path": path.name,
                "product": registered.get(path.name, ""),
                "registered_in_product_files": int(path.name in registered),
                "listed_in_release_inventory": int(path.name in descriptions),
                "kind": kinds.get(path.name, ""),
                "file_type": file_type,
                "size_bytes": int(path.stat().st_size),
                "size_mb": round(path.stat().st_size / 1024.0 / 1024.0, 6),
                "description": descriptions.get(path.name, ""),
            }
        )
    return pd.DataFrame(rows)


def build_netcdf_schema(ctx) -> pd.DataFrame:
    rows = []
    for product, file_name in PRODUCT_FILES.items():
        if not file_name.endswith(".nc") or not ctx.release_file(file_name).is_file():
            continue
        try:
            with ctx.open_dataset(file_name, required=True) as ds:
                rows.append(
                    {
                        "product": product,
                        "file_name": file_name,
                        "schema_section": "global",
                        "name": "global_attributes",
                        "dtype": "",
                        "dimensions": "",
                        "size": int(len(ds.ncattrs())),
                        "units": "",
                        "long_name": "",
                        "flag_values": "",
                        "flag_meanings": "",
                    }
                )
                for name, dim in ds.dimensions.items():
                    rows.append(
                        {
                            "product": product,
                            "file_name": file_name,
                            "schema_section": "dimension",
                            "name": name,
                            "dtype": "",
                            "dimensions": "",
                            "size": int(len(dim)),
                            "units": "",
                            "long_name": "",
                            "flag_values": "",
                            "flag_meanings": "",
                        }
                    )
                for name, var in ds.variables.items():
                    flag_values = getattr(var, "flag_values", "")
                    try:
                        flag_values = "|".join(str(int(v)) for v in np.asarray(flag_values).reshape(-1))
                    except Exception:
                        flag_values = clean_text(flag_values)
                    rows.append(
                        {
                            "product": product,
                            "file_name": file_name,
                            "schema_section": "variable",
                            "name": name,
                            "dtype": str(var.dtype),
                            "dimensions": "|".join(var.dimensions),
                            "size": int(np.prod(var.shape)) if getattr(var, "shape", ()) else 1,
                            "units": clean_text(getattr(var, "units", "")),
                            "long_name": clean_text(getattr(var, "long_name", "")),
                            "flag_values": flag_values,
                            "flag_meanings": clean_text(getattr(var, "flag_meanings", "")),
                        }
                    )
        except Exception as exc:
            rows.append(
                {
                    "product": product,
                    "file_name": file_name,
                    "schema_section": "error",
                    "name": "cannot_read",
                    "dtype": "",
                    "dimensions": "",
                    "size": 0,
                    "units": "",
                    "long_name": str(exc),
                    "flag_values": "",
                    "flag_meanings": "",
                }
            )
    return pd.DataFrame(rows)


def build_gpkg_layers(ctx) -> pd.DataFrame:
    rows = []
    for product, file_name in PRODUCT_FILES.items():
        if not file_name.endswith(".gpkg") or not ctx.release_file(file_name).is_file():
            continue
        path = ctx.release_file(file_name)
        try:
            with sqlite3.connect(str(path)) as conn:
                layers = pd.read_sql_query(
                    "SELECT table_name, data_type, identifier FROM gpkg_contents ORDER BY table_name",
                    conn,
                )
                for _, layer in layers.iterrows():
                    table = clean_text(layer.get("table_name", ""))
                    quoted = '"' + table.replace('"', '""') + '"'
                    try:
                        feature_count = int(pd.read_sql_query("SELECT COUNT(*) AS n FROM {}".format(quoted), conn)["n"].iloc[0])
                    except Exception:
                        feature_count = -1
                    try:
                        cols = pd.read_sql_query("PRAGMA table_info({})".format(quoted), conn)
                        column_count = int(len(cols))
                    except Exception:
                        column_count = 0
                    rows.append(
                        {
                            "product": product,
                            "file_name": file_name,
                            "layer_name": table,
                            "data_type": clean_text(layer.get("data_type", "")),
                            "identifier": clean_text(layer.get("identifier", "")),
                            "feature_count": feature_count,
                            "column_count": column_count,
                        }
                    )
        except Exception as exc:
            rows.append(
                {
                    "product": product,
                    "file_name": file_name,
                    "layer_name": "cannot_read",
                    "data_type": "",
                    "identifier": str(exc),
                    "feature_count": -1,
                    "column_count": 0,
                }
            )
    return pd.DataFrame(rows)


def build_inventory_summary(ctx, files_df: pd.DataFrame, schema_df: pd.DataFrame, gpkg_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    def add(group: str, product: str, metric: str, value, unit: str = "", file_name: str = "", notes: str = "") -> None:
        rows.append(
            {
                "group": group,
                "product": product,
                "metric": metric,
                "value": value,
                "unit": unit,
                "file_name": file_name,
                "notes": notes,
            }
        )

    add("release", "all", "file_count", int(len(files_df)), "files")
    add("release", "all", "total_size_mb", round(float(files_df["size_mb"].sum()) if not files_df.empty else 0.0, 6), "MB")
    add("release", "all", "registered_file_count", int(files_df["registered_in_product_files"].sum()) if "registered_in_product_files" in files_df else 0, "files")
    add("release", "all", "inventory_listed_file_count", int(files_df["listed_in_release_inventory"].sum()) if "listed_in_release_inventory" in files_df else 0, "files")
    for product, file_name in PRODUCT_FILES.items():
        path = ctx.release_file(file_name)
        add("file", product, "exists", int(path.is_file()), "boolean", file_name)
        if path.is_file():
            add("file", product, "size_mb", round(path.stat().st_size / 1024.0 / 1024.0, 6), "MB", file_name)
            if file_name.endswith(".csv"):
                add("csv", product, "row_count", _csv_rows(path), "rows", file_name)
    for (product, file_name), group in schema_df.groupby(["product", "file_name"], dropna=False):
        add("netcdf_schema", product, "dimension_count", int(group["schema_section"].eq("dimension").sum()), "dimensions", file_name)
        add("netcdf_schema", product, "variable_count", int(group["schema_section"].eq("variable").sum()), "variables", file_name)
        global_rows = group[group["schema_section"].eq("global")]
        add("netcdf_schema", product, "global_attribute_count", int(global_rows["size"].iloc[0]) if len(global_rows) else 0, "attributes", file_name)
    for (product, file_name), group in gpkg_df.groupby(["product", "file_name"], dropna=False):
        add("geopackage", product, "layer_count", int(len(group)), "layers", file_name)
        add("geopackage", product, "feature_count", int(pd.to_numeric(group["feature_count"], errors="coerce").clip(lower=0).sum()), "features", file_name)
    return pd.DataFrame(rows)


def build_article_metrics(summary_df: pd.DataFrame, files_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    def value(group: str, product: str, metric: str):
        hit = summary_df[(summary_df["group"].eq(group)) & (summary_df["product"].eq(product)) & (summary_df["metric"].eq(metric))]
        return hit["value"].iloc[0] if len(hit) else ""

    rows.append({"section": "release_inventory", "metric": "file_count", "value": value("release", "all", "file_count"), "unit": "files", "source_file": "release_dir", "notes": ""})
    rows.append({"section": "release_inventory", "metric": "total_size_mb", "value": value("release", "all", "total_size_mb"), "unit": "MB", "source_file": "release_dir", "notes": ""})
    for product in ("master_nc", "daily_nc", "monthly_nc", "annual_nc", "climatology_nc", "satellite_nc"):
        rows.append(
            {
                "section": "netcdf_products",
                "metric": "{}_variable_count".format(product),
                "value": value("netcdf_schema", product, "variable_count"),
                "unit": "variables",
                "source_file": PRODUCT_FILES[product],
                "notes": "",
            }
        )
    if not files_df.empty:
        unlisted = int((files_df["listed_in_release_inventory"] == 0).sum())
        rows.append({"section": "release_health", "metric": "files_not_listed_in_release_inventory", "value": unlisted, "unit": "files", "source_file": "release_inventory.csv", "notes": ""})
    return pd.DataFrame(rows)


def build_release_inventory_mismatches(ctx) -> pd.DataFrame:
    """Compare release_inventory.csv contents against files actually on disk."""
    actual = {p.name for p in ctx.release_dir.iterdir() if p.is_file()}
    inventory = ctx.read_csv(PRODUCT_FILES["inventory_csv"], required=False)
    if inventory.empty or "file_name" not in inventory.columns:
        return pd.DataFrame(
            [{"file_name": PRODUCT_FILES["inventory_csv"], "issue": "release_inventory_missing_or_has_no_file_name_column"}]
        )
    listed = {clean_text(v) for v in inventory["file_name"] if clean_text(v)}
    rows = []
    for name in sorted(actual - listed):
        rows.append({"file_name": name, "issue": "on_disk_not_in_release_inventory"})
    for name in sorted(listed - actual):
        rows.append({"file_name": name, "issue": "release_inventory_entry_missing_on_disk"})
    return pd.DataFrame(rows, columns=["file_name", "issue"])


def _path_leak_row(product: str, layer: str, field: str, values) -> dict:
    texts = [clean_text(v) for v in values if clean_text(v)]
    abs_count = sum(text.startswith("/") for text in texts)
    local_count = sum(any(marker in text for marker in LOCAL_PATH_MARKERS) for text in texts)
    sample = next((text for text in texts if any(marker in text for marker in LOCAL_PATH_MARKERS)), "")
    return {
        "product": product,
        "layer": layer,
        "field": field,
        "n_values": len(texts),
        "absolute_path_count": int(abs_count),
        "local_path_count": int(local_count),
        "sample": sample,
    }


def build_path_leak_stats(ctx) -> pd.DataFrame:
    rows = []
    csv_fields = {
        "source_station_catalog": ("source_station_paths",),
        "satellite_catalog": ("candidate_path", "resolved_candidate_path"),
        "satellite_validation_catalog": ("candidate_path", "resolved_candidate_path"),
    }
    for product, fields in csv_fields.items():
        frame = ctx.read_csv(PRODUCT_FILES[product], required=False)
        if frame.empty:
            continue
        for field in fields:
            if field in frame.columns:
                rows.append(_path_leak_row(product, "csv", field, frame[field]))

    nc_products = ("master_nc", "satellite_nc")
    for product in nc_products:
        file_name = PRODUCT_FILES[product]
        path = ctx.require_input(ctx.release_file(file_name), required=False)
        if path is None:
            continue
        try:
            with ctx.open_dataset(file_name, required=True) as ds:
                for field in sorted(name for name in ds.variables if "path" in name.lower()):
                    rows.append(_path_leak_row(product, "netcdf", field, read_text_var(ds, field)))
        except Exception as exc:
            rows.append(
                {
                    "product": product,
                    "layer": "netcdf",
                    "field": "",
                    "n_values": 0,
                    "absolute_path_count": 0,
                    "local_path_count": 0,
                    "sample": "cannot inspect NetCDF paths: {}".format(exc),
                }
            )
    return pd.DataFrame(rows)


def _used_uids(ds, index_var_name: str, uid_values: list) -> set:
    if index_var_name not in ds.variables:
        return set()
    raw = np.ma.asarray(ds.variables[index_var_name][:]).filled(-1).reshape(-1)
    used = {int(v) for v in raw if int(v) >= 0 and int(v) < len(uid_values)}
    return {uid_values[i] for i in used}


def build_active_metadata_consistency(ctx) -> pd.DataFrame:
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"], required=False)
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    rows = []
    path = ctx.require_input(ctx.release_file(PRODUCT_FILES["master_nc"]), required=False)
    if path is None:
        return pd.DataFrame(rows)
    try:
        with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
            checks = [
                {
                    "entity": "cluster_uid",
                    "uid_var": "cluster_uid",
                    "index_var": "station_index",
                    "catalog": station,
                    "catalog_col": "cluster_uid",
                    "dimension": "n_stations",
                },
                {
                    "entity": "source_station_uid",
                    "uid_var": "source_station_uid",
                    "index_var": "source_station_index",
                    "catalog": source_station,
                    "catalog_col": "source_station_uid",
                    "dimension": "n_source_stations",
                },
            ]
            for check in checks:
                uid_values = read_text_var(ds, check["uid_var"])
                nc_set = {v for v in uid_values if v}
                used = _used_uids(ds, check["index_var"], uid_values)
                catalog = check["catalog"]
                catalog_set = (
                    {clean_text(v) for v in catalog[check["catalog_col"]] if clean_text(v)}
                    if not catalog.empty and check["catalog_col"] in catalog.columns
                    else set()
                )
                inactive = sorted(nc_set - used)
                rows.append(
                    {
                        "entity": check["entity"],
                        "nc_dimension": int(len(ds.dimensions.get(check["dimension"], []))),
                        "nc_unique": len(nc_set),
                        "catalog_rows": int(len(catalog)),
                        "catalog_unique": len(catalog_set),
                        "nc_missing_from_catalog": len(nc_set - catalog_set),
                        "catalog_missing_from_nc": len(catalog_set - nc_set),
                        "used_unique": len(used),
                        "used_missing_from_catalog": len(used - catalog_set),
                        "inactive_nc_entries": len(inactive),
                        "sample_inactive_nc_entries": "|".join(inactive[:20]),
                    }
                )
    except Exception as exc:
        rows.append(
            {
                "entity": "master_nc",
                "nc_dimension": 0,
                "nc_unique": 0,
                "catalog_rows": 0,
                "catalog_unique": 0,
                "nc_missing_from_catalog": 0,
                "catalog_missing_from_nc": 0,
                "used_unique": 0,
                "used_missing_from_catalog": 0,
                "inactive_nc_entries": 0,
                "sample_inactive_nc_entries": "cannot inspect active metadata: {}".format(exc),
            }
        )
    return pd.DataFrame(rows)


def build_inactive_metadata_entries(ctx) -> pd.DataFrame:
    station = ctx.read_csv(PRODUCT_FILES["station_catalog"], required=False)
    source_station = ctx.read_csv(PRODUCT_FILES["source_station_catalog"], required=False)
    rows = []
    path = ctx.require_input(ctx.release_file(PRODUCT_FILES["master_nc"]), required=False)
    if path is None:
        return pd.DataFrame(columns=["entity", "uid", "in_catalog", "used_by_records"])
    try:
        with ctx.open_dataset(PRODUCT_FILES["master_nc"], required=True) as ds:
            checks = [
                {
                    "entity": "cluster_uid",
                    "uid_var": "cluster_uid",
                    "index_var": "station_index",
                    "catalog": station,
                    "catalog_col": "cluster_uid",
                },
                {
                    "entity": "source_station_uid",
                    "uid_var": "source_station_uid",
                    "index_var": "source_station_index",
                    "catalog": source_station,
                    "catalog_col": "source_station_uid",
                },
            ]
            for check in checks:
                uid_values = read_text_var(ds, check["uid_var"])
                nc_set = {v for v in uid_values if v}
                used = _used_uids(ds, check["index_var"], uid_values)
                catalog = check["catalog"]
                catalog_set = (
                    {clean_text(v) for v in catalog[check["catalog_col"]] if clean_text(v)}
                    if not catalog.empty and check["catalog_col"] in catalog.columns
                    else set()
                )
                for uid in sorted(nc_set - used):
                    rows.append(
                        {
                            "entity": check["entity"],
                            "uid": uid,
                            "in_catalog": int(uid in catalog_set),
                            "used_by_records": 0,
                        }
                    )
    except Exception as exc:
        rows.append({"entity": "master_nc", "uid": "cannot inspect inactive metadata: {}".format(exc), "in_catalog": 0, "used_by_records": 0})
    return pd.DataFrame(rows, columns=["entity", "uid", "in_catalog", "used_by_records"])


def build_validation_contradictions(ctx) -> pd.DataFrame:
    validation = ctx.read_csv(PRODUCT_FILES["validation_csv"], required=False)
    if validation.empty or "details" not in validation.columns:
        return pd.DataFrame(columns=["check", "status", "file_name", "issue", "details"])
    actual = {p.name for p in ctx.release_dir.iterdir() if p.is_file()}
    rows = []
    pattern = re.compile(r"[A-Za-z0-9_./-]+\.(?:csv\.gz|parquet|csv|nc|gpkg|md|py)")
    for _, row in validation.iterrows():
        details = clean_text(row.get("details", ""))
        lowered = details.lower()
        if "not generated" not in lowered and "not found" not in lowered:
            continue
        for match in pattern.findall(details):
            file_name = Path(match).name
            if file_name in actual:
                rows.append(
                    {
                        "check": row.get("check", ""),
                        "status": row.get("status", ""),
                        "file_name": file_name,
                        "issue": "validation_reports_missing_but_file_exists",
                        "details": details,
                    }
                )
    return pd.DataFrame(rows, columns=["check", "status", "file_name", "issue", "details"])


def build_inventory_tables(ctx) -> dict:
    files = build_release_files(ctx)
    schema = build_netcdf_schema(ctx)
    gpkg = build_gpkg_layers(ctx)
    summary = build_inventory_summary(ctx, files, schema, gpkg)
    article = build_article_metrics(summary, files)
    wide = {}
    for _, row in summary.iterrows():
        key = "{}__{}__{}".format(row.get("group", ""), row.get("product", ""), row.get("metric", ""))
        wide[key] = row.get("value", "")
    return {
        "release_inventory_stats": build_inventory(ctx),
        "release_inventory_stats_files": files,
        "release_inventory_stats_summary": summary,
        "release_inventory_stats_summary_wide": pd.DataFrame([wide]),
        "release_inventory_stats_netcdf_schema": schema,
        "release_inventory_stats_gpkg_layers": gpkg,
        "release_inventory_stats_article_metrics": article,
        "release_inventory_mismatches": build_release_inventory_mismatches(ctx),
        "path_leaks": build_path_leak_stats(ctx),
        "active_metadata_consistency": build_active_metadata_consistency(ctx),
        "inactive_metadata_entries": build_inactive_metadata_entries(ctx),
        "validation_contradictions": build_validation_contradictions(ctx),
    }


def build_detailed_inventory_report(ctx, tables: dict, tables_dir: Path) -> list[str]:
    inventory = tables.get("release_inventory_stats", pd.DataFrame())
    files = tables.get("release_inventory_stats_files", pd.DataFrame())
    summary = tables.get("release_inventory_stats_summary", pd.DataFrame())
    article = tables.get("release_inventory_stats_article_metrics", pd.DataFrame())
    schema = tables.get("release_inventory_stats_netcdf_schema", pd.DataFrame())
    gpkg = tables.get("release_inventory_stats_gpkg_layers", pd.DataFrame())
    mismatches = tables.get("release_inventory_mismatches", pd.DataFrame())
    path_leaks = tables.get("path_leaks", pd.DataFrame())
    active = tables.get("active_metadata_consistency", pd.DataFrame())
    inactive = tables.get("inactive_metadata_entries", pd.DataFrame())
    validation = tables.get("validation_contradictions", pd.DataFrame())

    registered_missing = inventory[
        inventory.get("registration_status", pd.Series(dtype=str)).astype(str).eq("registered")
        & pd.to_numeric(inventory.get("exists", 0), errors="coerce").fillna(0).eq(0)
    ] if not inventory.empty else pd.DataFrame()
    unregistered = inventory[
        inventory.get("registration_status", pd.Series(dtype=str)).astype(str).eq("unregistered")
    ] if not inventory.empty else pd.DataFrame()
    leak_rows = path_leaks[
        pd.to_numeric(path_leaks.get("local_path_count", 0), errors="coerce").fillna(0).gt(0)
    ] if not path_leaks.empty else pd.DataFrame()
    inactive_rows = active[
        pd.to_numeric(active.get("inactive_nc_entries", 0), errors="coerce").fillna(0).gt(0)
    ] if not active.empty else pd.DataFrame()

    schema_summary = pd.DataFrame()
    if not schema.empty and {"product", "schema_section"}.issubset(schema.columns):
        schema_summary = (
            schema.groupby(["product", "schema_section"], dropna=False)
            .size()
            .unstack(fill_value=0)
            .reset_index()
            .rename_axis(None, axis=1)
        )

    file_count = len(files) if not files.empty else len(inventory)
    total_size_mb = files["size_mb"].sum() if not files.empty and "size_mb" in files.columns else np.nan
    lines = [
        "# Release Inventory Statistics",
        "",
        "## Scope",
        "",
        "- Release package: `{}`".format(display_path(ctx.release_dir)),
        "- Output tables: `{}`".format(display_path(tables_dir)),
        "- Inputs are limited to the published release package; pipeline intermediates are not read.",
        "",
        "## Headline",
        "",
        "- Files discovered in release package: {}".format(fmt_int(file_count)),
        "- Total release size: {} MB".format(fmt_float(total_size_mb, 3) if np.isfinite(total_size_mb) else ""),
        "- Registered products checked: {}".format(fmt_int(len(PRODUCT_FILES))),
        "- Missing registered products: {}".format(fmt_int(len(registered_missing))),
        "- Unregistered top-level files: {}".format(fmt_int(len(unregistered))),
        "- Inventory/listing mismatches: {}".format(fmt_int(len(mismatches))),
        "- Fields with local absolute-path values: {}".format(fmt_int(len(leak_rows))),
        "- NetCDF metadata dimensions with inactive entries: {}".format(fmt_int(len(inactive_rows))),
        "- Validation/file-existence contradictions: {}".format(fmt_int(len(validation))),
        "",
        "## Article-Ready Metrics",
        "",
        sorted_markdown_table(
            article,
            columns=["section", "metric", "value", "unit", "source_file", "notes"],
            max_rows=24,
        ),
    ]
    append_table_section(
        lines,
        "Release File Inventory",
        files,
        columns=["file_name", "product", "registered_in_product_files", "listed_in_release_inventory", "kind", "file_type", "size_mb", "description"],
        sort_by="size_mb",
        max_rows=18,
        note="This table compares the physical release contents with both the code-side product registry and `release_inventory.csv`.",
    )
    append_table_section(
        lines,
        "NetCDF Schema Summary",
        schema_summary,
        max_rows=18,
        note="Counts are derived from release NetCDF dimensions, variables, and global attributes.",
    )
    append_table_section(
        lines,
        "GeoPackage Layers",
        gpkg,
        columns=["product", "file_name", "layer_name", "feature_count", "column_count"],
        sort_by="feature_count",
        max_rows=18,
    )
    append_table_section(
        lines,
        "Release Inventory Mismatches",
        mismatches,
        columns=["file_name", "issue"],
        max_rows=20,
        note="Rows here require release packaging cleanup or an explicit registry decision.",
    )
    append_table_section(
        lines,
        "Absolute Path Leak Diagnostics",
        leak_rows.drop(columns=["sample"], errors="ignore"),
        columns=["product", "layer", "field", "n_values", "absolute_path_count", "local_path_count"],
        sort_by="local_path_count",
        max_rows=20,
        note="Raw samples are intentionally kept only in `tables/path_leaks.csv`; Markdown reports avoid echoing host-local paths.",
    )
    append_table_section(
        lines,
        "Active Metadata Consistency",
        active,
        columns=["entity", "nc_dimension", "nc_unique", "catalog_rows", "catalog_unique", "used_unique", "inactive_nc_entries", "used_missing_from_catalog"],
        sort_by="inactive_nc_entries",
        max_rows=10,
        note="Inactive entries are NetCDF metadata identifiers that are not used by active release records or catalogs.",
    )
    append_table_section(
        lines,
        "Validation Contradictions",
        validation,
        columns=["check", "status", "file_name", "issue", "details"],
        max_rows=12,
    )
    lines.extend(
        [
            "",
            "## Recommended Follow-Up",
            "",
            "- Rebuild or update `release_inventory.csv` when mismatch rows are present.",
            "- Replace host-local paths in release CSV/NetCDF provenance fields with release-relative paths, public URLs, or stable provenance tokens.",
            "- Either trim inactive NetCDF metadata dimensions or publish an explicit inactive metadata catalog with `is_active` semantics.",
            "- Re-run validation after release sidecar registration changes so skip messages match actual file existence.",
        ]
    )
    return safe_lines(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Build release package inventory and health statistics.")
    add_common_args(parser, "inventory")
    args = parser.parse_args(argv)
    ctx = context_from_args(args)
    ctx.out_dir.mkdir(parents=True, exist_ok=True)

    tables = build_inventory_tables(ctx)
    tables_dir = ctx.output_path("tables", "x").parent
    for name, frame in tables.items():
        write_csv(frame, tables_dir / "{}.csv".format(name))
    for legacy_name in (
        "release_inventory_stats_files",
        "release_inventory_stats_summary",
        "release_inventory_stats_summary_wide",
        "release_inventory_stats_netcdf_schema",
        "release_inventory_stats_gpkg_layers",
        "release_inventory_stats_article_metrics",
    ):
        write_csv(tables[legacy_name], ctx.output_path("{}.csv".format(legacy_name)))

    inventory = tables["release_inventory_stats"]
    missing = inventory[
        inventory["registration_status"].eq("registered") & inventory["exists"].eq(0)
    ]["file_name"].astype(str).tolist()
    report = build_detailed_inventory_report(ctx, tables, tables_dir)
    md_path = ctx.output_path("reports", "release_inventory_stats.md")
    write_markdown(report, md_path)
    write_markdown(report, ctx.output_path("release_inventory_stats_summary.md"))
    try:
        copy_report_to_docs(md_path, bool(args.copy_reports))
    except Exception:
        pass
    print("Wrote inventory stats to {}".format(tables_dir))
    return 0 if not missing else 2


if __name__ == "__main__":
    raise SystemExit(main())
