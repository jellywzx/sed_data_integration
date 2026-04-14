# 实现计划：统一 QC NC 文件全局属性（attr_normalizer + s2 集成）

---

## 一、背景与目标

- 53,470 个 QC NC 文件，来自 25 种不同属性结构的数据集
- 目标：经 s2 复制后，每个文件都拥有同一套完整的标准属性层
- 规则：
  1. **统一命名** — 同一信息的不同变体归入一个标准属性名
  2. **缺失补空** — 文件中无对应信息时写入空字符串 `""`
  3. **不破坏原始** — 原始属性保留不动，标准属性是额外叠加/覆盖的层

---

## 二、最终标准属性 Schema

### A. CF/ACDD 惯例属性

| 标准属性名 | 来源变体 | 处理方式 |
|-----------|---------|---------|
| `Conventions` | `Conventions`, `conventions` | 强制覆写为 `CF-1.8, ACDD-1.3` |
| `title` | `title` | 缺失补 `""` |
| `history` | `history` | 缺失补 `""` |
| `summary` | `summary` | 缺失补 `""` |
| `comment` | `comment` | 缺失补 `""` |
| `processing_level` | `processing_level` | 缺失补 `""` |
| `date_created` | `date_created` | 缺失补 `""` |
| `date_modified` | `date_modified` | 缺失补 `""` |
| `featureType` | `featureType` | 缺失补 `""` |

### B. 站点身份属性

| 标准属性名 | 来源变体（按优先级） | 处理方式 |
|-----------|------------------|---------|
| `station_id` | `station_id` > `Source_ID` > `source_id` > `location_id` | 取第一个非空值；缺失补 `""` |
| `station_name` | `station_name` | 缺失补 `""` |
| `river_name` | `river_name` | 缺失补 `""` |
| `station_name_chinese` | `station_name_chinese` | 仅在原文件中存在时保留，不向其他文件补空 |
| `river_name_chinese` | `river_name_chinese` | 同上 |
| `station_location` | `station_location` | 缺失补 `""` |

### C. 地理空间属性

| 标准属性名 | 来源变体（按优先级） | 处理方式 |
|-----------|------------------|---------|
| `geographic_coverage` | `geographic_coverage` > `Geographic_Coverage` | 取第一个非空值；缺失补 `""` |
| `country` | `country` | 缺失补 `""` |
| `continent_region` | `continent_region` | 缺失补 `""` |
| `geospatial_lat_min` | `geospatial_lat_min` | 缺失补 `""` |
| `geospatial_lat_max` | `geospatial_lat_max` | 缺失补 `""` |
| `geospatial_lon_min` | `geospatial_lon_min` | 缺失补 `""` |
| `geospatial_lon_max` | `geospatial_lon_max` | 缺失补 `""` |
| `geospatial_vertical_min` | `geospatial_vertical_min` | 缺失补 `""` |
| `geospatial_vertical_max` | `geospatial_vertical_max` | 缺失补 `""` |
| `upstream_area` | `upstream_area` | 缺失补 `""` |

### D. 时间属性

| 标准属性名 | 来源变体（按优先级） | 处理方式 |
|-----------|------------------|---------|
| `temporal_resolution` | `temporal_resolution` > `Temporal_Resolution` | 取第一个非空值；缺失补 `""` |
| `temporal_span` | `temporal_span` > `Temporal_Span` > `measurement_period` | 取第一个非空值；缺失补 `""` |
| `time_coverage_start` | `time_coverage_start` > `data_period_start` | 取第一个非空值；缺失补 `""` |
| `time_coverage_end` | `time_coverage_end` > `data_period_end` | 取第一个非空值；缺失补 `""` |

> **说明**：`temporal_span` 对 climatology 数据至关重要，存储 `"1990-2020"` 形式的覆盖时段，
> 不可用 `time_coverage_start/end` 替代。`measurement_period` 为同义异名，映射进来。

### E. 数据描述属性

| 标准属性名 | 来源变体（按优先级） | 处理方式 |
|-----------|------------------|---------|
| `observation_type` | `observation_type` > `type` > `Type` | 取第一个非空值；缺失补 `""` |
| `variables_provided` | `variables_provided` > `Variables_Provided` | 取第一个非空值；缺失补 `""` |
| `data_limitations` | `data_limitations` | 缺失补 `""` |
| `source` | `source` | 缺失补 `""` |

> **说明**：`type` 重命名为 `observation_type`，避免与 NetCDF 数据类型语义冲突。
> `number_of_data` 移除（纯派生量，可从 `len(time)` 读取，易与实际数据不同步）。

### F. 数据来源与引用

| 标准属性名 | 来源变体（按优先级） | 处理方式 |
|-----------|------------------|---------|
| `data_source_name` | `data_source_name` > `Data_Source_Name` > `dataset_name` | 取第一个非空值；缺失补 `""` |
| `source_data_link` | `source_data_link` > `source_url` > `sediment_data_source` > `discharge_data_source` | 取第一个非空值；缺失补 `""` |
| `references` | 合并：`references`, `reference`, `Reference`, `Reference1`, `reference1`, `reference2` | 所有非空值用 ` \| ` 拼接后写入 |
| `creator_institution` | `creator_institution` > `contributor_institution` > `institution` > `insitiution` | 取第一个非空值；缺失补 `""` |
| `creator_name` | `creator_name` > `contributor_name` | 取第一个非空值；缺失补 `""` |
| `creator_email` | `creator_email` > `contributor_email` | 取第一个非空值；缺失补 `""` |

### G. 卫星/特殊数据集属性（仅在原文件中存在时保留）

| 标准属性名 | 来源变体 | 处理方式 |
|-----------|---------|---------|
| `reach_id` | `reach_id` | 原文件有则保留，无则不补空 |
| `reach_length_m` | `reach_length_m` | 原文件有则保留，无则不补空 |

---

## 三、属性完整性规则总结

| 情形 | 处理方式 |
|------|---------|
| 目标属性已存在且非空 | 不修改，保留原值 |
| 目标属性不存在/为空，有候选来源 | 从候选列表取第一个非空值写入 |
| 目标属性不存在/为空，无候选来源 | 写入空字符串 `""` |
| `references` | 特殊合并逻辑：所有参考文献键去重后拼接 |
| `Conventions` | 强制覆写，无论是否已存在 |
| G 组属性（reach_id 等） | 跳过，不向缺失文件补空 |
| 语言特定属性（`*_chinese`） | 同上，跳过，不补空 |

---

## 四、实现文件

### 4.1 新建 `attr_normalizer.py`

位置：`/home/user/sed_data_integration/attr_normalizer.py`

内容结构：

```python
# ── 优先级映射表 ─────────────────────────────────────────────────────────────
# 格式：标准属性名 -> [候选属性名列表，按优先级排列]
ATTR_PRIORITY_MAP = {
    "Conventions":          [],          # 特殊：强制覆写
    "title":                ["title"],
    "history":              ["history"],
    "summary":              ["summary"],
    "comment":              ["comment"],
    "processing_level":     ["processing_level"],
    "date_created":         ["date_created"],
    "date_modified":        ["date_modified"],
    "featureType":          ["featureType"],

    "station_id":           ["station_id", "Source_ID", "source_id", "location_id"],
    "station_name":         ["station_name"],
    "river_name":           ["river_name"],
    "station_location":     ["station_location"],

    "geographic_coverage":  ["geographic_coverage", "Geographic_Coverage"],
    "country":              ["country"],
    "continent_region":     ["continent_region"],
    "geospatial_lat_min":   ["geospatial_lat_min"],
    "geospatial_lat_max":   ["geospatial_lat_max"],
    "geospatial_lon_min":   ["geospatial_lon_min"],
    "geospatial_lon_max":   ["geospatial_lon_max"],
    "geospatial_vertical_min": ["geospatial_vertical_min"],
    "geospatial_vertical_max": ["geospatial_vertical_max"],
    "upstream_area":        ["upstream_area"],

    "temporal_resolution":  ["temporal_resolution", "Temporal_Resolution"],
    "temporal_span":        ["temporal_span", "Temporal_Span", "measurement_period"],
    "time_coverage_start":  ["time_coverage_start", "data_period_start"],
    "time_coverage_end":    ["time_coverage_end", "data_period_end"],

    "observation_type":     ["observation_type", "type", "Type"],
    "variables_provided":   ["variables_provided", "Variables_Provided"],
    "data_limitations":     ["data_limitations"],
    "source":               ["source"],

    "data_source_name":     ["data_source_name", "Data_Source_Name", "dataset_name"],
    "source_data_link":     ["source_data_link", "source_url",
                             "sediment_data_source", "discharge_data_source"],
    "creator_institution":  ["creator_institution", "contributor_institution",
                             "institution", "insitiution"],
    "creator_name":         ["creator_name", "contributor_name"],
    "creator_email":        ["creator_email", "contributor_email"],
}

# ── 引用属性合并键 ────────────────────────────────────────────────────────────
REFERENCE_KEYS = [
    "references", "reference", "Reference",
    "Reference1", "reference1", "reference2",
]

# ── 仅在原文件存在时保留、不向缺失文件补空的属性 ─────────────────────────────
OPTIONAL_PASSTHROUGH = {
    "station_name_chinese",
    "river_name_chinese",
    "reach_id",
    "reach_length_m",
}

CONVENTIONS_VALUE = "CF-1.8, ACDD-1.3"


def normalize_nc_attrs(path: str) -> None:
    """
    原地修改 NC 文件，写入标准属性层。
    - 已存在且非空的属性不修改
    - 缺失/空属性从候选来源取值；无来源则补空字符串
    - references 合并所有参考文献键
    - Conventions 强制覆写
    """
    import netCDF4 as nc4

    with nc4.Dataset(path, "a") as ds:
        existing = {k: str(getattr(ds, k, "")).strip() for k in ds.ncattrs()}

        # Conventions 强制覆写
        ds.setncattr("Conventions", CONVENTIONS_VALUE)

        # 优先级映射属性
        for canon, candidates in ATTR_PRIORITY_MAP.items():
            if canon == "Conventions":
                continue
            # 已存在且非空则跳过
            if existing.get(canon, ""):
                continue
            # 从候选中取第一个非空值
            value = ""
            for src in candidates:
                v = existing.get(src, "").strip()
                if v:
                    value = v
                    break
            ds.setncattr(canon, value)

        # references 合并
        ref_parts = []
        for key in REFERENCE_KEYS:
            v = existing.get(key, "").strip()
            if v and v not in ref_parts:
                ref_parts.append(v)
        if ref_parts:
            ds.setncattr("references", " | ".join(ref_parts))
        elif not existing.get("references", ""):
            ds.setncattr("references", "")

4.2 修改 s2_reorganize_qc_by_resolution.py
在 _copy_one 函数中 shutil.copy2(src, dest) 之后插入：

# 标准化全局属性
try:
    from attr_normalizer import normalize_nc_attrs
    normalize_nc_attrs(str(dest))
except Exception as exc:
    # 属性标准化失败不阻断主流程，仅记录警告
    print(f"[s2] WARNING: attr_normalizer failed on {dest}: {exc}")

五、验证步骤
# 1. 单文件测试（用任意一个 qc/*.nc 文件）
python3 - <<'EOF'
import shutil, netCDF4 as nc4
from attr_normalizer import normalize_nc_attrs

shutil.copy2("path/to/sample.nc", "/tmp/test_norm.nc")
normalize_nc_attrs("/tmp/test_norm.nc")

with nc4.Dataset("/tmp/test_norm.nc") as ds:
    for k in sorted(ds.ncattrs()):
        print(f"{k:35s}: {str(getattr(ds, k))[:80]}")
EOF

# 2. 检查 Conventions 是否被强制覆写
python3 -c "
import netCDF4 as nc4
with nc4.Dataset('/tmp/test_norm.nc') as ds:
    print(ds.Conventions)
"

# 3. 完整 s2 运行后，用 s0 重新审计验证属性统一性
python s0_audit_qc_attributes.py
# 预期：s0_audit_qc_summary.txt 中「属性结构种数」从 25 降为 1

六、注意事项
attr_normalizer.py 与 s2_reorganize_qc_by_resolution.py 放在同一目录下，
确保 import 可正常找到。
normalize_nc_attrs 以 "a"（append）模式打开文件，不重建变量，仅修改全局属性，
对数据本身零影响。
若后续新增数据集带有新属性名变体，只需在 ATTR_PRIORITY_MAP 对应列表末尾追加，
无需改动 s2。
number_of_data / Number_of_data 已从 schema 移除，原文件中若存在则自然保留，
不再向缺失文件传播。


