"""
Utilities for publishing station-facing names in S8 release artifacts.

Upstream S5/S6/S7 products still use cluster-oriented internal names.  S8 keeps
those internal names while building the package, then applies this small
post-processing layer so user-facing outputs use station/reference-station
terminology consistently.
"""

from pathlib import Path

import pandas as pd

try:
    import netCDF4 as nc4
except ImportError:  # pragma: no cover - callers already gate NetCDF work.
    nc4 = None


PUBLIC_NAME_RENAMES = {
    "cluster_uid": "station_uid",
    "cluster_id": "station_reference_id",
    "linked_cluster_uid": "linked_station_uid",
    "linked_cluster_id": "linked_station_reference_id",
    "source_station_cluster_index": "source_station_reference_index",
    "n_source_stations_in_cluster": "n_source_stations_in_reference_station",
    "n_clusters": "n_reference_stations",
    "n_cluster_resolution_rows": "n_station_resolution_rows",
    "n_ranked_candidates_for_cluster_resolution": "n_ranked_candidates_for_station_resolution",
    "linked_cluster_semantics": "linked_station_semantics",
}

SATELLITE_NC_NAME_RENAMES = dict(PUBLIC_NAME_RENAMES)
SATELLITE_NC_NAME_RENAMES.update(
    {
        "cluster_id_station": "station_reference_id",
        "cluster_id": "record_station_reference_id",
    }
)

TEXT_REPLACEMENTS = (
    ("n_ranked_candidates_for_cluster_resolution", "n_ranked_candidates_for_station_resolution"),
    ("n_source_stations_in_cluster", "n_source_stations_in_reference_station"),
    ("source_station_cluster_index", "source_station_reference_index"),
    ("linked_cluster_semantics", "linked_station_semantics"),
    ("linked_cluster_uid / linked_cluster_id", "linked_station_uid / linked_station_reference_id"),
    ("linked_cluster_uid", "linked_station_uid"),
    ("linked_cluster_id", "linked_station_reference_id"),
    ("cluster_uid / cluster_id", "station_uid / station_reference_id"),
    ("cluster_uid + resolution", "station_uid + resolution"),
    ("cluster_uid", "station_uid"),
    ("cluster_id_station", "station_reference_id"),
    ("cluster_id", "station_reference_id"),
    ("n_clusters", "n_reference_stations"),
    ("n_cluster_resolution_rows", "n_station_resolution_rows"),
    ("cluster-resolution", "station-resolution"),
    ("cluster/resolution", "station/resolution"),
    ("Cluster point", "Station point"),
    ("cluster point", "station point"),
    ("Cluster basin", "Station basin"),
    ("cluster basin", "station basin"),
    ("station-reference clusters", "station-reference stations"),
    ("reference clusters", "reference stations"),
    ("main reference cluster", "main reference station"),
    ("cluster lookup", "station lookup"),
    ("clusters", "reference stations"),
    ("Clusters", "Reference stations"),
)

GPKG_LAYER_RENAMES = {
    "cluster_summary": "station_summary",
    "cluster_daily": "station_daily",
    "cluster_monthly": "station_monthly",
    "cluster_annual": "station_annual",
}

FORBIDDEN_PUBLIC_NAMES = (
    "cluster_uid",
    "cluster_id",
    "cluster_id_station",
    "linked_cluster_uid",
    "linked_cluster_id",
    "source_station_cluster_index",
    "n_source_stations_in_cluster",
    "n_clusters",
    "n_cluster_resolution_rows",
    "n_ranked_candidates_for_cluster_resolution",
)


def public_name_for(name, satellite_nc=False):
    mapping = SATELLITE_NC_NAME_RENAMES if satellite_nc else PUBLIC_NAME_RENAMES
    return mapping.get(name, name)


def apply_public_text(value):
    if not isinstance(value, str):
        return value
    text = value
    for old, new in TEXT_REPLACEMENTS:
        text = text.replace(old, new)
    return text


def _rename_mapping_for_columns(columns):
    rename = {}
    existing = set(columns)
    for old, new in PUBLIC_NAME_RENAMES.items():
        if old in existing and new not in existing:
            rename[old] = new
    return rename


def apply_public_station_names_to_dataframe(df):
    out = df.copy()
    for old, new in PUBLIC_NAME_RENAMES.items():
        if old in out.columns and new in out.columns:
            new_blank = out[new].astype(str).str.strip().eq("")
            out.loc[new_blank, new] = out.loc[new_blank, old]
            out = out.drop(columns=[old])
    out = out.rename(columns=_rename_mapping_for_columns(out.columns))
    if out.empty:
        return out

    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].map(apply_public_text)
    return out


def apply_public_station_names_to_csv(path):
    path = Path(path)
    if not path.is_file() or path.suffix.lower() not in {".csv", ".gz"}:
        return False
    df = pd.read_csv(path, keep_default_na=False)
    out = apply_public_station_names_to_dataframe(df)
    if list(out.columns) == list(df.columns) and out.equals(df):
        return False
    if path.name.endswith(".gz"):
        tmp_path = path.with_name(path.name[:-3] + ".tmp.gz")
    else:
        tmp_path = path.with_name(path.name + ".tmp")
    out.to_csv(tmp_path, index=False)
    tmp_path.replace(path)
    return True


def _rename_nc_attrs(obj, satellite_nc=False):
    changed = False
    for attr_name in list(obj.ncattrs()):
        attr_value = obj.getncattr(attr_name)
        new_attr_name = public_name_for(attr_name, satellite_nc=satellite_nc)
        if new_attr_name != attr_name:
            obj.setncattr(new_attr_name, apply_public_text(attr_value))
            obj.delncattr(attr_name)
            changed = True
        elif isinstance(attr_value, str):
            new_value = apply_public_text(attr_value)
            if new_value != attr_value:
                obj.setncattr(attr_name, new_value)
                changed = True
    return changed


def apply_public_station_names_to_netcdf(path):
    if nc4 is None:
        raise RuntimeError("netCDF4 is required to rename public NetCDF variables")

    path = Path(path)
    if not path.is_file():
        return False
    changed = False
    satellite_nc = path.name == "sed_reference_satellite.nc"
    with nc4.Dataset(path, "a") as ds:
        changed = _rename_nc_attrs(ds, satellite_nc=satellite_nc) or changed
        for var_name in list(ds.variables):
            new_name = public_name_for(var_name, satellite_nc=satellite_nc)
            if new_name != var_name and new_name not in ds.variables:
                ds.renameVariable(var_name, new_name)
                changed = True
        for var_name in list(ds.variables):
            changed = _rename_nc_attrs(ds.variables[var_name], satellite_nc=satellite_nc) or changed
    return changed


def apply_public_station_names_to_text_file(path):
    path = Path(path)
    if not path.is_file():
        return False
    text = path.read_text(encoding="utf-8")
    new_text = apply_public_text(text)
    if new_text == text:
        return False
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(new_text, encoding="utf-8")
    tmp_path.replace(path)
    return True


def apply_public_station_names_to_gpkg(path):
    path = Path(path)
    if not path.is_file() or path.suffix.lower() != ".gpkg":
        return False
    try:
        import fiona
        import geopandas as gpd
    except ImportError:
        return False

    layers = fiona.listlayers(str(path))
    if not layers:
        return False
    tmp_path = path.with_name(path.stem + ".station_names.tmp.gpkg")
    if tmp_path.exists():
        tmp_path.unlink()
    changed = False
    for layer in layers:
        gdf = gpd.read_file(str(path), layer=layer)
        new_layer = GPKG_LAYER_RENAMES.get(layer, apply_public_text(layer))
        new_gdf = apply_public_station_names_to_dataframe(gdf)
        if new_layer != layer or list(new_gdf.columns) != list(gdf.columns):
            changed = True
        new_gdf.to_file(str(tmp_path), layer=new_layer, driver="GPKG")
    if changed:
        tmp_path.replace(path)
        return True
    tmp_path.unlink()
    return False


def apply_public_station_names_to_release_dir(release_dir):
    release_dir = Path(release_dir)
    changed = []
    for path in sorted(release_dir.iterdir()):
        suffix = path.suffix.lower()
        if suffix == ".nc":
            if apply_public_station_names_to_netcdf(path):
                changed.append(path.name)
        elif suffix == ".csv" or path.name.endswith(".csv.gz"):
            if apply_public_station_names_to_csv(path):
                changed.append(path.name)
        elif suffix in {".md", ".py"}:
            if apply_public_station_names_to_text_file(path):
                changed.append(path.name)
        elif suffix == ".gpkg":
            if apply_public_station_names_to_gpkg(path):
                changed.append(path.name)
    return changed


def audit_public_station_names_in_release_dir(release_dir):
    release_dir = Path(release_dir)
    problems = []
    for path in sorted(release_dir.iterdir()):
        suffix = path.suffix.lower()
        if suffix == ".nc" and nc4 is not None:
            with nc4.Dataset(path, "r") as ds:
                for name in FORBIDDEN_PUBLIC_NAMES:
                    if name in ds.variables or name in ds.ncattrs():
                        problems.append("{}:{}".format(path.name, name))
                for var_name, var in ds.variables.items():
                    for name in FORBIDDEN_PUBLIC_NAMES:
                        if name in var.ncattrs():
                            problems.append("{}:{}.{}".format(path.name, var_name, name))
        elif suffix == ".csv" or path.name.endswith(".csv.gz"):
            columns = pd.read_csv(path, nrows=0, keep_default_na=False).columns
            for name in FORBIDDEN_PUBLIC_NAMES:
                if name in columns:
                    problems.append("{}:{}".format(path.name, name))
        elif suffix == ".gpkg":
            try:
                import fiona
            except ImportError:
                continue
            for layer in fiona.listlayers(str(path)):
                if "cluster" in str(layer).lower():
                    problems.append("{}:layer:{}".format(path.name, layer))
                with fiona.open(str(path), layer=layer) as src:
                    for name in FORBIDDEN_PUBLIC_NAMES:
                        if name in src.schema.get("properties", {}):
                            problems.append("{}:{}:{}".format(path.name, layer, name))
    return {
        "check": "public_station_names",
        "status": "pass" if not problems else "fail",
        "details": "no legacy public cluster field names in NetCDF variables/attributes or CSV columns"
        if not problems
        else "; ".join(problems[:20]),
    }
