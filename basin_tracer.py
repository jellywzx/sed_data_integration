"""Trace upstream basins from station locations using MERIT-Basins river-network topology."""

import logging
import os
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set

import geopandas as gpd
import numpy as np
import pandas as pd
import fiona

def _pyogrio_read_info(path):
    """Drop-in replacement for _pyogrio_read_info()."""
    with fiona.open(path) as src:
        return {"total_bounds": src.bounds}
from shapely.geometry import Point
from shapely.ops import unary_union, transform
from pyproj import CRS, Transformer

try:
    from shapely import coverage_union_all as _coverage_union_all
    _HAS_COVERAGE_UNION = True
except ImportError:
    _HAS_COVERAGE_UNION = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
AREA_MATCH_TOLERANCE = 0.5
SEARCH_RADIUS_M = 120000.0
SEARCH_RADIUS_DEG = 1.0


class UpstreamBasinTracer:
    """Trace complete upstream catchments from stations using MERIT-Basins topology."""

    def __init__(self, merit_basins_dir: str):
        """Initialize the tracer with MERIT-Basins river-network and catchment directories."""
        self.merit_basins_dir = Path(merit_basins_dir)
        self.pfaf_level1_dir = self.merit_basins_dir / "pfaf_level_01"
        self.pfaf_level2_dir = self.merit_basins_dir / "pfaf_level_02"

        self._level1_rivers: Dict[str, gpd.GeoDataFrame] = {}
        self._level2_catchments: Dict[str, gpd.GeoDataFrame] = {}
        self._level1_topology: Dict[str, Dict[int, List[int]]] = {}
        self._region_bounds: Dict[str, tuple] = {}

        self._build_region_index()


    def _build_region_index(self):
        """Scan pfaf_level_01 river-network files and record their bounding boxes."""
        if not self.pfaf_level1_dir.exists():
            logger.warning(f"pfaf_level_01 not found: {self.pfaf_level1_dir}")
            return

        riv_files = list(self.pfaf_level1_dir.glob(
            "riv_pfaf_*_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"
        ))
        logger.info(f"Found {len(riv_files)} pfaf_level_01 river files")

        for riv_file in riv_files:
            pfaf_code = riv_file.stem.split("_")[2]
            try:
                info = _pyogrio_read_info(str(riv_file))
                self._region_bounds[pfaf_code] = info["total_bounds"]
            except Exception as e:
                logger.warning(f"Error reading bounds for {pfaf_code}: {e}")

        logger.info(f"Indexed {len(self._region_bounds)} pfaf_level_01 regions")

    def _distance_point_to_geoms_m(self, geoms, lon: float, lat: float):
        """Project a point and geometry to a local metric CRS and return planar distance in meters."""
        local_crs = self._get_local_metric_crs(lon, lat)
        transformer = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)

        x0, y0 = transformer.transform(lon, lat)
        point_proj = Point(x0, y0)

        def _dist(geom):
            if geom is None or geom.is_empty:
                return np.nan
            geom_proj = transform(transformer.transform, geom)
            return geom_proj.distance(point_proj)

        return geoms.apply(_dist)


    def _get_local_metric_crs(self, lon: float, lat: float):
        """Select a local metric projection for the station location, preferring UTM."""
        if lat >= 84:
            return CRS.from_epsg(3413)  # Arctic Polar Stereographic
        if lat <= -80:
            return CRS.from_epsg(3031)  # Antarctic Polar Stereographic

        zone = int((lon + 180) // 6) + 1
        if lat >= 0:
            epsg = 32600 + zone
        else:
            epsg = 32700 + zone
        return CRS.from_epsg(epsg)

    def _get_pfaf_level1_codes(self, lon: float, lat: float) -> List[str]:
        """Return pfaf_level_01 region codes whose bounding boxes contain the given point."""
        candidates = []
        for pfaf_code, bounds in self._region_bounds.items():
            minx, miny, maxx, maxy = bounds
            if minx <= lon <= maxx and miny <= lat <= maxy:
                candidates.append(pfaf_code)
        return candidates

    def _load_level1_rivers(self, pfaf_code: str) -> Optional[gpd.GeoDataFrame]:
        """Load a pfaf_level_01 river-network GeoDataFrame and build COMID and spatial indexes."""
        if pfaf_code in self._level1_rivers:
            return self._level1_rivers[pfaf_code]

        riv_path = (
            self.pfaf_level1_dir
            / f"riv_pfaf_{pfaf_code}_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"
        )
        if not riv_path.exists():
            return None

        try:
            logger.info(f"Loading pfaf_level_01 rivers for region {pfaf_code}...")
            gdf = gpd.read_file(riv_path)
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326")

            gdf = gdf.set_index("COMID", drop=False)
            gdf.sindex  # build spatial index

            self._level1_rivers[pfaf_code] = gdf
            up_cols = [c for c in ["up1", "up2", "up3", "up4"] if c in gdf.columns]
            if up_cols:
                up_data = gdf[up_cols].to_numpy(dtype=float, na_value=0.0)
                topo: Dict[int, List[int]] = {}
                for i, comid in enumerate(gdf.index):
                    ups = [int(v) for v in up_data[i] if v > 0]
                    if ups:
                        topo[comid] = ups
                self._level1_topology[pfaf_code] = topo

            logger.info(f"Loaded {len(gdf)} river reaches for region {pfaf_code}")
            return gdf
        except Exception as e:
            logger.error(f"Error loading {riv_path}: {e}")
            return None

    def _load_level2_catchments(self, pfaf2_code: str) -> Optional[gpd.GeoDataFrame]:
        """Load pfaf_level_02 catchment polygons for a region code."""
        if pfaf2_code in self._level2_catchments:
            return self._level2_catchments[pfaf2_code]

        cat_path = (
            self.pfaf_level2_dir
            / f"cat_pfaf_{pfaf2_code}_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"
        )
        if not cat_path.exists():
            logger.warning(f"Catchment file not found: {cat_path}")
            return None

        try:
            logger.debug(f"Loading pfaf_level_02 catchments for region {pfaf2_code}...")
            gdf = gpd.read_file(cat_path)
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326")

            gdf = gdf.set_index("COMID", drop=False)
            self._level2_catchments[pfaf2_code] = gdf
            logger.debug(f"Loaded {len(gdf)} catchments for region {pfaf2_code}")
            return gdf
        except Exception as e:
            logger.error(f"Error loading {cat_path}: {e}")
            return None

    def _gather_nearby_candidate_reaches(
        self, lon: float, lat: float
    ) -> Optional[gpd.GeoDataFrame]:
        """Collect nearby candidate reaches after spatial-index and distance filtering."""
        if pd.isna(lon) or pd.isna(lat):
            return None

        pfaf_codes = self._get_pfaf_level1_codes(lon, lat)
        if not pfaf_codes:
            return None

        all_candidates: List[gpd.GeoDataFrame] = []

        for pfaf_code in pfaf_codes:
            riv_gdf = self._load_level1_rivers(pfaf_code)
            if riv_gdf is None:
                continue

            search_box = (
                lon - SEARCH_RADIUS_DEG,
                lat - SEARCH_RADIUS_DEG,
                lon + SEARCH_RADIUS_DEG,
                lat + SEARCH_RADIUS_DEG,
            )

            possible_idx = list(riv_gdf.sindex.intersection(search_box))
            if not possible_idx:
                continue

            candidates = riv_gdf.iloc[possible_idx].copy()

            candidates["dist_m"] = self._distance_point_to_geoms_m(
                candidates.geometry, lon, lat
            )
            candidates = candidates[candidates["dist_m"] < SEARCH_RADIUS_M]


            if len(candidates) > 0:
                candidates["pfaf_code"] = pfaf_code
                all_candidates.append(candidates)

        if not all_candidates:
            return None

        merged = pd.concat(all_candidates, ignore_index=True)
        if len(merged) == 0:
            return None

        crs = all_candidates[0].crs if all_candidates[0].crs is not None else "EPSG:4326"
        return gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)

    def get_nearby_candidate_reaches(
        self, lon: float, lat: float
    ) -> Optional[gpd.GeoDataFrame]:
        """Return nearby candidate reaches for debugging and visualization."""
        return self._gather_nearby_candidate_reaches(lon, lat)

    def find_best_reach(
        self,
        lon: float,
        lat: float,
        reported_area: float = None,
    ) -> Dict:
        """Select the best matching reach for a station from nearby river-network candidates."""
        result = {
            "COMID": None,
            "uparea": np.nan,
            "distance": np.nan,
            "pfaf_code": None,
            "match_quality": "failed",
            "area_error": np.nan,
        }

        if pd.isna(lon) or pd.isna(lat):
            return result

        candidates = self._gather_nearby_candidate_reaches(lon, lat)
        if candidates is None or len(candidates) == 0:
            return result

        if reported_area is not None and reported_area > 0:
            candidates["area_ratio"] = candidates["uparea"] / reported_area
            candidates["area_error"] = np.abs(
                np.log10(candidates["area_ratio"].clip(0.001, 1000))
            )
            candidates["dist_score"] = candidates["dist_m"] / SEARCH_RADIUS_M
            candidates["score"] = candidates["area_error"] + candidates["dist_score"]

            best_idx = candidates["score"].idxmin()
            best = candidates.loc[best_idx]

            area_ratio = best["area_ratio"]
            log_err = abs(np.log10(area_ratio))

            if log_err < 0.1:
                match_quality = "area_matched"
            elif log_err < 0.3:
                match_quality = "area_approximate"
            else:
                match_quality = "area_mismatch"
        else:
            best_idx = candidates["dist_m"].idxmin()
            best = candidates.loc[best_idx]
            match_quality = "distance_only"

        result["COMID"] = int(best["COMID"])
        result["uparea"] = float(best["uparea"])
        result["distance"] = float(best["dist_m"])
        result["pfaf_code"] = best["pfaf_code"]
        result["match_quality"] = match_quality

        if reported_area is not None and reported_area > 0:
            result["area_error"] = np.log10(best["uparea"] / reported_area)

        return result

    def trace_upstream_reaches(
        self,
        start_comid: int,
        pfaf_code: str,
    ) -> Set[int]:
        """Traverse upstream COMIDs from a starting reach using breadth-first search."""
        riv_gdf = self._load_level1_rivers(pfaf_code)
        if riv_gdf is None:
            return {start_comid}

        topo = self._level1_topology.get(pfaf_code, {})

        upstream_comids: Set[int] = set()
        to_process = deque([start_comid])

        while to_process:
            current = to_process.popleft()

            if current in upstream_comids:
                continue

            upstream_comids.add(current)

            for up_comid in topo.get(current, []):
                if up_comid not in upstream_comids:
                    to_process.append(up_comid)

            if len(upstream_comids) % 10000 == 0:
                logger.info(
                    f"  Tracing upstream: {len(upstream_comids)} reaches found..."
                )

        return upstream_comids


    def get_upstream_basin_polygon(self, upstream_comids: Set[int]):
        """Merge catchment polygons for upstream COMIDs into a single geometry."""
        if not upstream_comids:
            return None

        comids_by_region: Dict[str, list] = {}
        for comid in upstream_comids:
            region = str(comid)[:2]
            comids_by_region.setdefault(region, []).append(comid)

        all_geoms = []
        for region, comids in comids_by_region.items():
            cat_gdf = self._load_level2_catchments(region)
            if cat_gdf is None:
                continue

            valid_mask = cat_gdf.index.isin(comids)
            if valid_mask.any():
                all_geoms.extend(cat_gdf.loc[valid_mask, "geometry"].tolist())

        if not all_geoms:
            return None

        try:
            if _HAS_COVERAGE_UNION and len(all_geoms) > 1:
                try:
                    return _coverage_union_all(np.array(all_geoms, dtype=object))
                except Exception:
                    pass
            return unary_union(all_geoms)
        except Exception as e:
            logger.error(f"Error merging polygons: {e}")
            return None


    def get_upstream_basin(
        self,
        lon: float,
        lat: float,
        reported_area: float = None,
    ) -> Dict:
        """Run the full single-station workflow: reach matching, upstream tracing, and catchment merge."""
        reach_info = self.find_best_reach(lon, lat, reported_area)
        return self.get_upstream_basin_from_reach(lon, lat, reach_info)

    def get_upstream_basin_from_reach(
        self,
        lon: float,
        lat: float,
        reach_info: Dict,
    ) -> Dict:
        """Trace an upstream basin directly from known MERIT reach information."""
        result = {
            "geometry": None,
            "geometry_local": None,
            "basin_area": np.nan,
            "basin_id": None,
            "match_quality": "failed",
            "area_error": np.nan,
            "uparea_merit": np.nan,
            "pfaf_code": None,
            "distance": np.nan,
            "method": None,
            "n_upstream_reaches": 0,
            "point_in_local": False,
            "point_in_basin": False,
        }

        if not isinstance(reach_info, dict):
            return result

        basin_id = reach_info.get("COMID")
        pfaf_code = reach_info.get("pfaf_code")
        if basin_id is None or not pfaf_code:
            return result

        try:
            basin_id = int(basin_id)
        except (TypeError, ValueError):
            return result

        def _coerce_float_or_nan(value):
            try:
                number = float(value)
                return number if np.isfinite(number) else np.nan
            except Exception:
                return np.nan

        uparea = _coerce_float_or_nan(reach_info.get("uparea"))
        distance = _coerce_float_or_nan(reach_info.get("distance"))
        area_error = _coerce_float_or_nan(reach_info.get("area_error"))

        result["basin_id"] = basin_id
        result["match_quality"] = str(reach_info.get("match_quality", "failed"))
        result["pfaf_code"] = str(pfaf_code)
        result["uparea_merit"] = uparea
        result["area_error"] = area_error
        result["basin_area"] = uparea
        result["distance"] = distance

        result["geometry_local"] = self.get_upstream_basin_polygon({basin_id})

        upstream_comids = self.trace_upstream_reaches(
            basin_id,
            str(pfaf_code),
        )
        result["n_upstream_reaches"] = len(upstream_comids)

        merged_polygon = self.get_upstream_basin_polygon(upstream_comids)

        if merged_polygon is not None:
            result["geometry"] = merged_polygon
            result["method"] = "upstream_traced"
        else:
            result["geometry"] = self._create_area_buffer(
                lon, lat, uparea
            )
            result["method"] = "area_buffer_fallback"

        point = None
        if pd.notna(lon) and pd.notna(lat):
            point = Point(lon, lat)

        if (
            point is not None
            and result["geometry_local"] is not None
            and not result["geometry_local"].is_empty
        ):
            result["point_in_local"] = result["geometry_local"].covers(point)

        if (
            point is not None
            and result["geometry"] is not None
            and not result["geometry"].is_empty
            and result["method"] == "upstream_traced"
        ):
            result["point_in_basin"] = result["geometry"].covers(point)

        return result

    def _create_area_buffer(self, lon: float, lat: float, area_km2: float):
        """Create an area-equivalent circular buffer in a local metric CRS and return it in WGS84."""
        if area_km2 is None or not np.isfinite(area_km2) or area_km2 <= 0:
            return Point(lon, lat)

        radius_m = np.sqrt(area_km2 * 1_000_000.0 / np.pi)

        local_crs = self._get_local_metric_crs(lon, lat)
        forward = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)
        backward = Transformer.from_crs(local_crs, "EPSG:4326", always_xy=True)

        x, y = forward.transform(lon, lat)
        point_proj = Point(x, y)
        buffer_proj = point_proj.buffer(radius_m)

        return transform(backward.transform, buffer_proj)

    def clear_cache(self):
        """Clear cached river-network and catchment GeoDataFrames."""
        self._level1_rivers.clear()
        self._level1_topology.clear() 
        self._level2_catchments.clear()

    def get_upstream_basins_from_csv(
        self,
        csv_path: str,
        lon_col: str = "lon",
        lat_col: str = "lat",
        area_col: Optional[str] = None,
        station_id_col: Optional[str] = "cluster_id",
        dedup_by_station: bool = True,
    ) -> gpd.GeoDataFrame:
        """Read station rows from CSV, trace upstream basins, and return a WGS84 GeoDataFrame."""
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV not found: {csv_file}")

        stations = pd.read_csv(csv_file)
        required_cols = [lon_col, lat_col]
        missing = [c for c in required_cols if c not in stations.columns]
        if missing:
            raise ValueError(f"Missing required columns in CSV: {missing}")

        stations = stations.dropna(subset=[lon_col, lat_col]).copy()
        if len(stations) == 0:
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")

        if dedup_by_station:
            if station_id_col and station_id_col in stations.columns:
                stations = stations.drop_duplicates(subset=[station_id_col], keep="first")
            else:
                stations = stations.drop_duplicates(subset=[lon_col, lat_col], keep="first")

        result_rows = []
        n_total = len(stations)
        logger.info(f"Tracing upstream basins for {n_total} stations...")

        for i, row in stations.iterrows():
            lon = float(row[lon_col])
            lat = float(row[lat_col])
            reported_area = None
            if area_col and area_col in stations.columns:
                area_value = row[area_col]
                if pd.notna(area_value):
                    reported_area = float(area_value)

            basin_result = self.get_upstream_basin(lon, lat, reported_area=reported_area)
            out = {
                "lon": lon,
                "lat": lat,
                "reported_area": reported_area if reported_area is not None else np.nan,
                "geometry": basin_result["geometry"],
                "geometry_local": basin_result["geometry_local"],
                "basin_area": basin_result["basin_area"],
                "basin_id": basin_result["basin_id"],
                "match_quality": basin_result["match_quality"],
                "area_error": basin_result["area_error"],
                "uparea_merit": basin_result["uparea_merit"],
                "pfaf_code": basin_result["pfaf_code"],
                "method": basin_result["method"],
                "n_upstream_reaches": basin_result["n_upstream_reaches"],
            }
            if station_id_col and station_id_col in stations.columns:
                out["station_id"] = row[station_id_col]
            result_rows.append(out)

            done = len(result_rows)
            if done % 100 == 0 or done == n_total:
                logger.info(f"  Progress: {done}/{n_total}")

        result_gdf = gpd.GeoDataFrame(result_rows, geometry="geometry", crs="EPSG:4326")
        return result_gdf


SCRIPT_DIR = Path(__file__).resolve().parent

# Other settings
BUILTIN_CONFIG = {
    "merit_dir": os.environ.get("MERIT_DIR", "/path/to/MERIT_Hydro_v07_Basins_v01_bugfix1"),
    "stations_csv": str(SCRIPT_DIR / "output" / "s3_collected_stations.csv"),
    "lon_col": "lon",
    "lat_col": "lat",
    "area_col": None,
    "station_id_col": "cluster_id",
    "dedup_by_station": True,
    "out_gpkg": str(SCRIPT_DIR / "output" / "s4_upstream_basins.gpkg"),
    "out_csv": str(SCRIPT_DIR / "output" / "s4_upstream_basins.csv"),
    "log_level": "INFO",
}


def main():
    """Run the built-in CSV-to-GPKG/CSV tracing workflow."""
    cfg = BUILTIN_CONFIG
    logging.basicConfig(
        level=getattr(logging, str(cfg["log_level"]).upper()),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    tracer = UpstreamBasinTracer(str(cfg["merit_dir"]))
    result_gdf = tracer.get_upstream_basins_from_csv(
        csv_path=str(cfg["stations_csv"]),
        lon_col=str(cfg["lon_col"]),
        lat_col=str(cfg["lat_col"]),
        area_col=cfg["area_col"],
        station_id_col=cfg["station_id_col"],
        dedup_by_station=bool(cfg["dedup_by_station"]),
    )

    logger.info(f"Done. Generated {len(result_gdf)} basin results.")
    if cfg["out_gpkg"]:
        out_gpkg = Path(str(cfg["out_gpkg"]))
        out_gpkg.parent.mkdir(parents=True, exist_ok=True)
        result_gdf.to_file(out_gpkg, driver="GPKG")
        logger.info(f"Saved polygons to: {out_gpkg}")

    if cfg["out_csv"]:
        out_csv = Path(str(cfg["out_csv"]))
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        result_gdf.drop(columns=["geometry", "geometry_local"], errors="ignore").to_csv(out_csv, index=False)
        logger.info(f"Saved tabular summary to: {out_csv}")


if __name__ == "__main__":
    main()
