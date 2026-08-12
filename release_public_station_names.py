#!/usr/bin/env python3
"""Convert public minimal release products from cluster naming to station naming."""

import re
import shutil
from pathlib import Path

import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - environment dependent
    nc4 = None


EXACT_IDENTIFIER_RENAMES = {
    "linked_cluster_uid": "linked_station_uid",
    "linked_cluster_id": "linked_station_reference_id",
    "cluster_uid": "station_uid",
    "cluster_id": "station_reference_id",
    "n_cluster_resolution_rows": "n_reference_station_resolution_rows",
    "n_source_stations_in_cluster": "n_source_stations_in_reference_station",
    "n_clusters": "n_reference_stations",
    "linked_cluster_semantics": "linked_station_semantics",
}

FILE_LABEL_RENAMES = {
    "cluster_points": "station_points",
    "cluster_basins": "station_basins",
    "cluster_point": "station_point",
    "cluster_basin": "station_basin",
}

OLD_PUBLIC_SCHEMA_RE = re.compile(
    r"(^|[^A-Za-z0-9])("
    r"linked_cluster_uid|linked_cluster_id|cluster_uid|cluster_id|n_clusters|"
    r"n_cluster_[A-Za-z0-9_]*|[A-Za-z0-9_]*_cluster_[A-Za-z0-9_]*|"
    r"cluster_points?|cluster_basins?"
    r")(?=$|[^A-Za-z0-9])"
)


class ReportRow:
    def __init__(self, file, product_type, action, status, old_name="", new_name="", details=""):
        self.file = file
        self.product_type = product_type
        self.action = action
        self.status = status
        self.old_name = old_name
        self.new_name = new_name
        self.details = details

    def as_dict(self):
        return {
            "file": self.file,
            "product_type": self.product_type,
            "action": self.action,
            "status": self.status,
            "old_name": self.old_name,
            "new_name": self.new_name,
            "details": self.details,
        }


def _case_word(text: str, lower: str, upper_first: str) -> str:
    text = re.sub(r"\bclusters\b", "reference stations", text)
    text = re.sub(r"\bClusters\b", "Reference stations", text)
    text = re.sub(r"\bcluster\b", lower, text)
    text = re.sub(r"\bCluster\b", upper_first, text)
    return text


def rename_identifier(name):
    """Rename a schema identifier while preserving non-cluster identifiers."""
    if name in EXACT_IDENTIFIER_RENAMES:
        return EXACT_IDENTIFIER_RENAMES[name]

    updated = str(name)
    replacements = {}
    replacements.update(FILE_LABEL_RENAMES)
    replacements.update(EXACT_IDENTIFIER_RENAMES)
    for old in sorted(replacements, key=len, reverse=True):
        updated = updated.replace(old, replacements[old])

    updated = re.sub(r"(?<![A-Za-z0-9])cluster(?![A-Za-z0-9])", "reference_station", updated)
    updated = re.sub(r"(?<![A-Za-z0-9])Cluster(?![A-Za-z0-9])", "Reference_Station", updated)
    return updated


def rename_text_metadata(text):
    """Rename public metadata text, including prose in NetCDF attrs and README files."""
    updated = str(text)
    replacements = {}
    replacements.update(FILE_LABEL_RENAMES)
    replacements.update(EXACT_IDENTIFIER_RENAMES)
    for old in sorted(replacements, key=len, reverse=True):
        updated = updated.replace(old, replacements[old])
    updated = _case_word(updated, "reference station", "Reference station")
    return updated


def contains_old_public_schema_token(text):
    return bool(OLD_PUBLIC_SCHEMA_RE.search(str(text)))


def rename_csv_value(value):
    if not isinstance(value, str):
        return value
    if not contains_old_public_schema_token(value):
        return value
    return rename_text_metadata(value)


def _relative(path: Path, base_dir: Path) -> str:
    try:
        return str(path.relative_to(base_dir))
    except ValueError:
        return str(path)


def _append(rows, base_dir, path, product_type, action, status, old_name="", new_name="", details=""):
    rows.append(
        ReportRow(
            file=_relative(path, base_dir),
            product_type=product_type,
            action=action,
            status=status,
            old_name=str(old_name),
            new_name=str(new_name),
            details=str(details),
        )
    )


def process_csv(path, base_dir, rows, dry_run=False):
    df = pd.read_csv(path, keep_default_na=False, dtype=str)
    original_columns = list(df.columns)
    new_columns = [rename_identifier(col) for col in original_columns]

    for old, new in zip(original_columns, new_columns):
        if old != new:
            _append(rows, base_dir, path, "csv", "rename_column", "dry-run" if dry_run else "changed", old, new)

    changed_values = 0
    if not df.empty:
        for column in original_columns:
            updated = df[column].map(rename_csv_value)
            changed_values += int((updated != df[column]).sum())
            df[column] = updated

    if changed_values:
        _append(
            rows,
            base_dir,
            path,
            "csv",
            "rename_cell_metadata",
            "dry-run" if dry_run else "changed",
            details="changed_values={}".format(changed_values),
        )

    if not dry_run and (new_columns != original_columns or changed_values):
        df.columns = new_columns
        df.to_csv(path, index=False)
    elif dry_run:
        return
    else:
        _append(rows, base_dir, path, "csv", "scan", "unchanged")


def _rename_nc_attr(container, attr_name, base_dir, path, rows, dry_run):
    new_name = rename_identifier(attr_name)
    value = getattr(container, attr_name)
    is_text_value = isinstance(value, str)
    new_value = rename_text_metadata(value) if is_text_value else value
    changed_name = new_name != attr_name
    changed_value = is_text_value and new_value != value

    if changed_name:
        _append(rows, base_dir, path, "netcdf", "rename_attribute", "dry-run" if dry_run else "changed", attr_name, new_name)
    if changed_value:
        _append(rows, base_dir, path, "netcdf", "rewrite_attribute_value", "dry-run" if dry_run else "changed", attr_name, new_name)

    if dry_run or not (changed_name or changed_value):
        return

    if changed_name:
        container.renameAttribute(attr_name, new_name)
    setattr(container, new_name, new_value)


def process_netcdf(path, base_dir, rows, dry_run=False):
    if nc4 is None:
        _append(rows, base_dir, path, "netcdf", "scan", "skip", details="netCDF4 is not available")
        return

    mode = "r" if dry_run else "r+"
    with nc4.Dataset(path, mode) as ds:
        for dim_name in list(ds.dimensions):
            new_name = rename_identifier(dim_name)
            if new_name != dim_name:
                status = "dry-run" if dry_run else "changed"
                _append(rows, base_dir, path, "netcdf", "rename_dimension", status, dim_name, new_name)
                if not dry_run:
                    if new_name in ds.dimensions:
                        _append(rows, base_dir, path, "netcdf", "rename_dimension", "fail", dim_name, new_name, "target exists")
                    else:
                        ds.renameDimension(dim_name, new_name)

        for attr_name in list(ds.ncattrs()):
            _rename_nc_attr(ds, attr_name, base_dir, path, rows, dry_run)

        for var_name in list(ds.variables):
            new_name = rename_identifier(var_name)
            if new_name != var_name:
                status = "dry-run" if dry_run else "changed"
                _append(rows, base_dir, path, "netcdf", "rename_variable", status, var_name, new_name)
                if not dry_run:
                    if new_name in ds.variables:
                        _append(rows, base_dir, path, "netcdf", "rename_variable", "fail", var_name, new_name, "target exists")
                    else:
                        ds.renameVariable(var_name, new_name)
                        var_name = new_name
            var = ds.variables[var_name]
            for attr_name in list(var.ncattrs()):
                _rename_nc_attr(var, attr_name, base_dir, path, rows, dry_run)


def process_text(path, base_dir, rows, dry_run=False):
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        _append(rows, base_dir, path, "text", "scan", "skip", details="not utf-8 text")
        return
    updated = rename_text_metadata(text)
    if updated == text:
        _append(rows, base_dir, path, "text", "scan", "unchanged")
        return
    _append(rows, base_dir, path, "text", "rewrite_text", "dry-run" if dry_run else "changed")
    if not dry_run:
        path.write_text(updated, encoding="utf-8")


def process_gpkg(path, base_dir, rows, dry_run=False):
    try:
        import fiona
        import geopandas as gpd
    except ImportError:
        _append(
            rows,
            base_dir,
            path,
            "gpkg",
            "scan",
            "skip",
            details="geopandas and fiona are required for GPKG conversion",
        )
        return

    layers = list(fiona.listlayers(path))
    temp_path = path.with_suffix(path.suffix + ".tmp")
    wrote = False
    for layer in layers:
        new_layer = rename_identifier(layer)
        frame = gpd.read_file(path, layer=layer)
        new_columns = {col: rename_identifier(col) for col in frame.columns if rename_identifier(col) != col}
        if new_layer != layer:
            _append(rows, base_dir, path, "gpkg", "rename_layer", "dry-run" if dry_run else "changed", layer, new_layer)
        for old, new in new_columns.items():
            _append(rows, base_dir, path, "gpkg", "rename_column", "dry-run" if dry_run else "changed", old, new, "layer={}".format(layer))
        if dry_run:
            continue
        if new_columns:
            frame = frame.rename(columns=new_columns)
        frame.to_file(temp_path, layer=new_layer, driver="GPKG")
        wrote = True

    if wrote and not dry_run:
        temp_path.replace(path)
    elif temp_path.exists() and not dry_run:
        temp_path.unlink()
    if not layers:
        _append(rows, base_dir, path, "gpkg", "scan", "unchanged", details="no layers")


def copy_example_script(example_script, release_dir, rows, dry_run=False):
    if not example_script.is_file():
        _append(rows, release_dir, release_dir / "example_reference_workflow.py", "text", "copy_example", "skip", details="source missing: {}".format(example_script))
        return
    dst = release_dir / "example_reference_workflow.py"
    _append(rows, release_dir, dst, "text", "copy_example", "dry-run" if dry_run else "changed", details=str(example_script))
    if dry_run:
        return
    shutil.copy2(example_script, dst)
    process_text(dst, release_dir, rows, dry_run=False)


def _text_files(release_dir: Path):
    suffixes = {".md", ".txt", ".py", ".json", ".yml", ".yaml"}
    return sorted(path for path in release_dir.iterdir() if path.is_file() and path.suffix.lower() in suffixes)


def _csv_files(release_dir: Path):
    return sorted(path for path in release_dir.iterdir() if path.is_file() and path.suffix.lower() in {".csv", ".gz"} and path.name.endswith((".csv", ".csv.gz")))


def _nc_files(release_dir: Path):
    return sorted(path for path in release_dir.iterdir() if path.is_file() and path.suffix.lower() == ".nc")


def _gpkg_files(release_dir: Path):
    return sorted(path for path in release_dir.iterdir() if path.is_file() and path.suffix.lower() == ".gpkg")


def audit_release_dir(release_dir, rows):
    for path in _csv_files(release_dir):
        try:
            df = pd.read_csv(path, keep_default_na=False, dtype=str)
        except Exception as exc:
            _append(rows, release_dir, path, "audit", "scan_csv", "fail", details=exc)
            continue
        for column in df.columns:
            if contains_old_public_schema_token(column):
                _append(rows, release_dir, path, "audit", "audit_residual", "fail", column, details="csv column")
        for column in df.columns:
            mask = df[column].map(contains_old_public_schema_token) if not df.empty else []
            if hasattr(mask, "any") and mask.any():
                count = int(mask.sum())
                _append(rows, release_dir, path, "audit", "audit_residual", "fail", column, details="csv values={}".format(count))

    for path in _text_files(release_dir):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        matches = sorted({match.group(2) for match in OLD_PUBLIC_SCHEMA_RE.finditer(text)})
        if matches:
            _append(rows, release_dir, path, "audit", "audit_residual", "fail", details="text tokens={}".format("|".join(matches)))

    for path in _nc_files(release_dir):
        if nc4 is None:
            continue
        with nc4.Dataset(path, "r") as ds:
            for name in list(ds.dimensions) + list(ds.variables) + list(ds.ncattrs()):
                if contains_old_public_schema_token(name):
                    _append(rows, release_dir, path, "audit", "audit_residual", "fail", name, details="netcdf schema name")
            for attr_name in ds.ncattrs():
                value = getattr(ds, attr_name)
                if isinstance(value, str) and contains_old_public_schema_token(value):
                    _append(rows, release_dir, path, "audit", "audit_residual", "fail", attr_name, details="global attr value")
            for var_name, var in ds.variables.items():
                for attr_name in var.ncattrs():
                    if contains_old_public_schema_token(attr_name):
                        _append(rows, release_dir, path, "audit", "audit_residual", "fail", "{}:{}".format(var_name, attr_name), details="variable attr name")
                    value = getattr(var, attr_name)
                    if isinstance(value, str) and contains_old_public_schema_token(value):
                        _append(rows, release_dir, path, "audit", "audit_residual", "fail", "{}:{}".format(var_name, attr_name), details="variable attr value")


def convert_release_dir(
    release_dir,
    example_script=None,
    dry_run=False,
    audit=True,
):
    release_dir = Path(release_dir).resolve()
    if not release_dir.is_dir():
        raise FileNotFoundError("release directory not found: {}".format(release_dir))

    rows = []
    if example_script is not None:
        copy_example_script(Path(example_script).resolve(), release_dir, rows, dry_run=dry_run)

    for path in _csv_files(release_dir):
        process_csv(path, release_dir, rows, dry_run=dry_run)
    for path in _nc_files(release_dir):
        process_netcdf(path, release_dir, rows, dry_run=dry_run)
    for path in _text_files(release_dir):
        process_text(path, release_dir, rows, dry_run=dry_run)
    for path in _gpkg_files(release_dir):
        process_gpkg(path, release_dir, rows, dry_run=dry_run)

    if audit and not dry_run:
        audit_release_dir(release_dir, rows)
    return rows


def write_report(rows, report_path, dry_run=False):
    df = pd.DataFrame([row.as_dict() for row in rows])
    if df.empty:
        df = pd.DataFrame(columns=["file", "product_type", "action", "status", "old_name", "new_name", "details"])
    if not dry_run:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(report_path, index=False)


def has_failures(rows):
    return any(row.status == "fail" for row in rows)
