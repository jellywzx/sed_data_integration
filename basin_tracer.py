"""上游流域追溯（Upstream Basin Tracer）— 基于河网拓扑的完整上游汇水区圈定。

本模块在 MERIT-Basins 数据集上工作，主要流程为：
1. 根据测站位置（及可选的上游面积）在河段矢量中匹配对应的河网弧段（reach）；
2. 从该弧段出发，沿拓扑字段（up1–up4）广度优先遍历，收集所有上游 COMID；
3. 按 COMID 加载二级分区（pfaf_level_02）下的单元汇水面，合并为单个几何。

术语简述：
- COMID：河段/汇水单元的唯一标识，在 shapefile 属性与索引中一致使用。
- Pfafstetter 编码（pfaf）：MERIT-Basins 按层级划分的区域码；level_01 为更粗的河网区，
  level_02 的 catchment 文件名与 COMID 前两位数字对应的分区一致。

数据来源说明：由 /Volumes/Data01/HII/HII/scripts/upstream_basin_tracer.py 移植。
"""

import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Set

os.environ.setdefault("PROJ_LIB", "/root/miniconda3/envs/wzx/share/proj")

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
from shapely.geometry import Point
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 全局阈值：影响“测站–河段”匹配行为，可按数据尺度调整
# ---------------------------------------------------------------------------
# 若提供 reported_area： Merit 栅格汇水面积 uparea 与报告面积之比在此相对误差内视为“面积匹配良好”
AREA_MATCH_TOLERANCE = 0.5  # 例如 0.5 表示允许约 ±50% 的相对偏差仍标为 area_matched
# 以测站为中心、在 WGS84 下展平的方形搜索窗口半边长（度），用于空间索引初筛邻近河段
SEARCH_RADIUS_DEG = 1.0


class UpstreamBasinTracer:
    """基于 MERIT-Basins 河网拓扑，从测站追溯完整上游集水区。

    实例化时会扫描 pfaf_level_01 下所有河网 shp，读取各区的 total_bounds 建立空间索引，
    后续仅对可能包含测站的区域加载完整河段与汇水面，以控制内存与 I/O。
    """

    def __init__(self, merit_basins_dir: str):
        """初始化追溯器。

        Args:
            merit_basins_dir: MERIT-Basins 根目录路径
                （例如 .../MERIT_Hydro_v07_Basins_v01_bugfix1/），其下应有
                pfaf_level_01（河网 riv_*.shp）与 pfaf_level_02（汇水 cat_*.shp）。
        """
        self.merit_basins_dir = Path(merit_basins_dir)
        # 一级 Pfaf 区：整套河段拓扑（含 up1–up4 等字段）
        self.pfaf_level1_dir = self.merit_basins_dir / "pfaf_level_01"
        # 二级 Pfaf 区：单元汇水多边形，与 COMID 一一对应；按 COMID 前两位分组加载文件
        self.pfaf_level2_dir = self.merit_basins_dir / "pfaf_level_02"

        # 内存缓存：同一区域只读一次 shp，避免批量站点时重复解析
        self._level1_rivers: Dict[str, gpd.GeoDataFrame] = {}
        self._level2_catchments: Dict[str, gpd.GeoDataFrame] = {}
        # pfaf_level_01 区号 -> (minx, miny, maxx, maxy)，用于快速判断点落在哪些一级区内
        self._region_bounds: Dict[str, tuple] = {}

        self._build_region_index()

    def _build_region_index(self):
        """扫描 pfaf_level_01 下标准命名的河网文件，记录每个一级区的包络矩形。

        不做复杂空间索引；仅用 total_bounds 与点坐标比较，足够将候选区从全局缩到少量文件。
        """
        if not self.pfaf_level1_dir.exists():
            logger.warning(f"pfaf_level_01 not found: {self.pfaf_level1_dir}")
            return

        riv_files = list(self.pfaf_level1_dir.glob(
            "riv_pfaf_*_MERIT_Hydro_v07_Basins_v01_bugfix1.shp"
        ))
        logger.info(f"Found {len(riv_files)} pfaf_level_01 river files")

        for riv_file in riv_files:
            # 文件名形如 riv_pfaf_XX_...，取第三段为该区 Pfaf 码
            pfaf_code = riv_file.stem.split("_")[2]
            try:
                # 用 pyogrio.read_info 仅读取元数据中的 total_bounds，
                # 避免将整个 shapefile（数百 MB）加载到内存，大幅缩短初始化时间
                info = pyogrio.read_info(str(riv_file))
                self._region_bounds[pfaf_code] = info["total_bounds"]
            except Exception as e:
                logger.warning(f"Error reading bounds for {pfaf_code}: {e}")

        logger.info(f"Indexed {len(self._region_bounds)} pfaf_level_01 regions")

    def _get_pfaf_level1_codes(self, lon: float, lat: float) -> List[str]:
        """根据经纬度返回所有包络框包含该点的一级 Pfaf 区编码列表。

        边界上的点也计入对应区；若数据跨区或边界情况，可能出现多个候选区。
        """
        candidates = []
        for pfaf_code, bounds in self._region_bounds.items():
            minx, miny, maxx, maxy = bounds
            if minx <= lon <= maxx and miny <= lat <= maxy:
                candidates.append(pfaf_code)
        return candidates

    def _load_level1_rivers(self, pfaf_code: str) -> Optional[gpd.GeoDataFrame]:
        """按需加载指定一级区的河网 GeoDataFrame，并建立 COMID 索引与空间索引（sindex）。

        属性中应含 COMID、uparea（上游累积面积）、以及 up1–up4（直接上游河段 ID，无则为 0）。
        """
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

            # 以 COMID 为行索引，便于 O(1) 按 ID 取河段；drop=False 保留 COMID 列
            gdf = gdf.set_index("COMID", drop=False)
            # 访问 sindex 会构建 R 树，加速后续 intersection 查询
            gdf.sindex  # build spatial index

            self._level1_rivers[pfaf_code] = gdf
            logger.info(f"Loaded {len(gdf)} river reaches for region {pfaf_code}")
            return gdf
        except Exception as e:
            logger.error(f"Error loading {riv_path}: {e}")
            return None

    def _load_level2_catchments(self, pfaf2_code: str) -> Optional[gpd.GeoDataFrame]:
        """加载二级 Pfaf 区对应的单元汇水多边形（cat_pfaf_{code}_...shp）。"""
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
        """在测站周围收集空间索引+距离筛选后的候选河段（与 find_best_reach 第一步一致）。"""
        if pd.isna(lon) or pd.isna(lat):
            return None

        pfaf_codes = self._get_pfaf_level1_codes(lon, lat)
        if not pfaf_codes:
            return None

        point = Point(lon, lat)
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
            candidates["dist"] = candidates.geometry.distance(point)
            candidates = candidates[candidates["dist"] < SEARCH_RADIUS_DEG]

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
        """返回测站附近候选河段 GeoDataFrame，供调试与可视化（列含 dist、pfaf_code 等）。"""
        return self._gather_nearby_candidate_reaches(lon, lat)

    def find_best_reach(
        self,
        lon: float,
        lat: float,
        reported_area: float = None,
    ) -> Dict:
        """在候选河网中选取与测站最匹配的河段。

        步骤概要：
        1. 用一级区 bounds 筛出可能含点的区域，加载河网；
        2. 在测站周围 SEARCH_RADIUS_DEG 的包围盒内用空间索引取候选弧段；
        3. 计算各弧段几何到测站点的欧式距离（度）；
        4. 若给定 reported_area：综合“面积比的对数偏差”与“归一化距离”得 score，取最小者；
           否则仅按距离最近选取。

        Args:
            lon: 测站经度（WGS84）
            lat: 测站纬度
            reported_area: 文献/观测给出的上游汇水面积（km²），用于消歧；不传则纯几何最近

        Returns:
            字典键：COMID, uparea, distance, pfaf_code, match_quality, area_error
            match_quality 取值：failed | area_matched | area_approximate | area_mismatch | distance_only
        """
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
            # 面积项：用 log10|ratio| 衡量倍数差，避免纯比值在跨数量级时过于极端；clip 防止 log(0)
            candidates["area_ratio"] = candidates["uparea"] / reported_area
            candidates["area_error"] = np.abs(
                np.log10(candidates["area_ratio"].clip(0.001, 1000))
            )
            # 距离项：归一化到 [0,1] 量级附近，与面积项相加形成可加性评分
            candidates["dist_score"] = candidates["dist"] / SEARCH_RADIUS_DEG
            candidates["score"] = candidates["area_error"] + candidates["dist_score"]

            best_idx = candidates["score"].idxmin()
            best = candidates.loc[best_idx]

            area_ratio = best["area_ratio"]
            # 相对报告面积在 (1±AREA_MATCH_TOLERANCE) 内视为高质量匹配
            if 1 / (1 + AREA_MATCH_TOLERANCE) < area_ratio < (1 + AREA_MATCH_TOLERANCE):
                match_quality = "area_matched"
            elif 0.1 < area_ratio < 10:
                match_quality = "area_approximate"
            else:
                match_quality = "area_mismatch"
        else:
            best_idx = candidates["dist"].idxmin()
            best = candidates.loc[best_idx]
            match_quality = "distance_only"

        result["COMID"] = int(best["COMID"])
        result["uparea"] = float(best["uparea"])
        result["distance"] = float(best["dist"])
        result["pfaf_code"] = best["pfaf_code"]
        result["match_quality"] = match_quality

        if reported_area is not None and reported_area > 0:
            # 输出用的相对误差：(Merit − 报告) / 报告，与内部评分用的对数量纲不同
            result["area_error"] = (best["uparea"] - reported_area) / reported_area

        return result

    def trace_upstream_reaches(
        self,
        start_comid: int,
        pfaf_code: str,
    ) -> Set[int]:
        """从起始河段 COMID 出发，沿 up1–up4 广度优先遍历所有上游河段。

        MERIT 河网中一条河段最多四个直接上游连接；0 或缺失表示无上游。
        队列式 BFS：先发现的 COMID 先入队，避免重复展开（已访问集合 upstream_comids）。

        Args:
            start_comid: 测站匹配到的河段 COMID
            pfaf_code: 该河段所属 pfaf_level_01 区号（与 find_best_reach 一致）

        Returns:
            含起点在内的所有上游 COMID 集合；若河网加载失败则仅返回 {start_comid}。
        """
        riv_gdf = self._load_level1_rivers(pfaf_code)
        if riv_gdf is None:
            return {start_comid}

        upstream_comids: Set[int] = set()
        to_process = [start_comid]

        while to_process:
            current = to_process.pop(0)

            if current in upstream_comids:
                continue
            if current == 0 or pd.isna(current):
                continue

            upstream_comids.add(current)

            # 从当前行读取拓扑字段，将有效上游 ID 入队
            if current in riv_gdf.index:
                reach = riv_gdf.loc[current]
                for up_field in ["up1", "up2", "up3", "up4"]:
                    up_comid = reach.get(up_field, 0)
                    if up_comid and up_comid > 0 and up_comid not in upstream_comids:
                        to_process.append(int(up_comid))

            if len(upstream_comids) % 10000 == 0:
                logger.info(
                    f"  Tracing upstream: {len(upstream_comids)} reaches found..."
                )

        return upstream_comids

    def get_upstream_basin_polygon(self, upstream_comids: Set[int]):
        """将所有上游 COMID 对应的单元汇水多边形合并为单一几何（可能为多部件）。

        COMID 在 MERIT 中与二级区文件对应：取 str(comid)[:2] 作为 cat 文件分区键，
        仅在已追溯到的集合内加载所需分区并提取几何，最后 unary_union 融合边界。

        Args:
            upstream_comids: trace_upstream_reaches 得到的 COMID 集合

        Returns:
            合并后的 shapely 几何，若无有效多边形则 None。
        """
        if not upstream_comids:
            return None

        # 按 COMID 前两位划分到 pfaf_level_02 文件，减少重复扫描
        comids_by_region: Dict[str, list] = {}
        for comid in upstream_comids:
            region = str(comid)[:2]
            comids_by_region.setdefault(region, []).append(comid)

        polygons = []
        for region, comids in comids_by_region.items():
            cat_gdf = self._load_level2_catchments(region)
            if cat_gdf is None:
                continue

            for comid in comids:
                if comid in cat_gdf.index:
                    polygons.append(cat_gdf.loc[comid].geometry)

        if not polygons:
            return None

        try:
            return unary_union(polygons)
        except Exception as e:
            logger.error(f"Error merging polygons: {e}")
            return None

    def get_upstream_basin(
        self,
        lon: float,
        lat: float,
        reported_area: float = None,
    ) -> Dict:
        """单站完整流程：匹配河段 → 上游追溯 → 合并汇水面（失败则用面积圆缓冲兜底）。

        Returns:
            geometry: 流域多边形或缓冲圆
            basin_area: 匹配河段的 Merit uparea（km²）
            basin_id: 匹配河段 COMID
            match_quality / area_error / uparea_merit / pfaf_code: 与 find_best_reach 一致
            method: upstream_traced（正常）或 area_buffer_fallback（无 cat 几何时）
            n_upstream_reaches: 上游弧段数量
        """
        result = {
            "geometry": None,
            "geometry_local": None,
            "basin_area": np.nan,
            "basin_id": None,
            "match_quality": "failed",
            "area_error": np.nan,
            "uparea_merit": np.nan,
            "pfaf_code": None,
            "method": None,
            "n_upstream_reaches": 0,
        }

        reach_info = self.find_best_reach(lon, lat, reported_area)

        if reach_info["COMID"] is None:
            return result

        result["basin_id"] = reach_info["COMID"]
        result["match_quality"] = reach_info["match_quality"]
        result["pfaf_code"] = reach_info["pfaf_code"]
        result["uparea_merit"] = reach_info["uparea"]
        result["area_error"] = reach_info["area_error"]
        result["basin_area"] = reach_info["uparea"]

        # ① 最小单元集水区：只取匹配 COMID 对应的 cat 多边形（来自 cat_pfaf_* 面文件）
        result["geometry_local"] = self.get_upstream_basin_polygon({reach_info["COMID"]})

        # ② 完整上游流域：BFS 遍历所有上游 COMID（原有逻辑，完全保留）
        upstream_comids = self.trace_upstream_reaches(
            reach_info["COMID"],
            reach_info["pfaf_code"],
        )
        result["n_upstream_reaches"] = len(upstream_comids)

        merged_polygon = self.get_upstream_basin_polygon(upstream_comids)

        if merged_polygon is not None:
            result["geometry"] = merged_polygon
            result["method"] = "upstream_traced"
        else:
            result["geometry"] = self._create_area_buffer(
                lon, lat, reach_info["uparea"]
            )
            result["method"] = "area_buffer_fallback"


        return result

    def _create_area_buffer(self, lon: float, lat: float, area_km2: float):
        """用近似等面积的圆形缓冲代替真实流域（兜底几何）。

        将 km² 换为半径后，按纬度折合成经度、纬度方向的度步长（约 111 km/°），
        取平均得 buffer_deg，在测站点对 Point 做 buffer。非等面积投影下的近似。
        """
        radius_km = np.sqrt(area_km2 / np.pi)
        lat_rad = np.radians(lat)
        deg_per_km_lat = 1 / 111
        deg_per_km_lon = 1 / (111 * np.cos(lat_rad))
        avg_deg_per_km = (deg_per_km_lat + deg_per_km_lon) / 2
        buffer_deg = radius_km * avg_deg_per_km
        return Point(lon, lat).buffer(buffer_deg)

    def clear_cache(self):
        """释放已缓存的一级河网与二级汇水 GeoDataFrame，批量任务分段跑时可用。"""
        self._level1_rivers.clear()
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
        """批量读取 CSV，对每行（或去重后的每站）调用 get_upstream_basin，返回 GeoDataFrame。

        Args:
            csv_path: 含测站经纬度的 CSV 路径
            lon_col / lat_col: 经纬度列名
            area_col: 可选，上游面积列名（km²），会传给 find_best_reach
            station_id_col: 可选，去重与回写时用；若 dedup 且无此列则按 (lon,lat) 去重
            dedup_by_station: True 时每个站只保留一行再追溯，避免重复计算

        Returns:
            EPSG:4326 的 GeoDataFrame，geometry 为流域或兜底圆，并含 basin_id、method 等列
        """
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV not found: {csv_file}")

        stations = pd.read_csv(csv_file)
        required_cols = [lon_col, lat_col]
        missing = [c for c in required_cols if c not in stations.columns]
        if missing:
            raise ValueError(f"Missing required columns in CSV: {missing}")

        # 无坐标无法匹配河网，直接丢弃
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


# 命令行直接运行脚本时使用的内置路径与列名（可按本地环境改写或通过其他入口传入）
BUILTIN_CONFIG = {
    "merit_dir": "/share/home/dq134/wzx/sed_data/MERIT_Hydro_v07_Basins_v01_bugfix1",
    "stations_csv": "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts/output/s6_overlap_for_manual_choice.csv",
    "lon_col": "lon",
    "lat_col": "lat",
    "area_col": None,
    "station_id_col": "cluster_id",
    "dedup_by_station": True,
    "out_gpkg": "/share/home/dq134/wzx/sed_data/basin_results/s6_upstream_basins.gpkg",
    "out_csv": "/share/home/dq134/wzx/sed_data/basin_results/s6_upstream_basins.csv",
    "log_level": "INFO",
}


def main():
    """脚本入口：读 BUILTIN_CONFIG，批量追溯并写出 GPKG（几何）与 CSV（属性表，无 geometry 列）。"""
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
        result_gdf.drop(columns=["geometry"]).to_csv(out_csv, index=False)
        logger.info(f"Saved tabular summary to: {out_csv}")


if __name__ == "__main__":
    main()
