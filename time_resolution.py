"""
时间分辨率分类与属性同步工具。

用途：
1. 根据时间间隔推断 frequency；
2. 为 s1 / s2 提供统一的时间语义映射；
3. 为镜像库与 s2 副本提供 temporal_resolution 属性回写工具；
4. 默认优先使用 Output_r_attr_fixed 作为 s1 / s2 输入根目录。
"""

import os
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr


TIME_VAR_NAMES = ["time", "Time", "t", "datetime", "date"]
LEGACY_TEMPORAL_RESOLUTION_KEYS = ("Temporal_Resolution", "time_resolution", "resolution")


def get_preferred_output_root(script_dir):
    """返回 s1/s2 默认应读取的根目录。

    优先级：
    1. 环境变量 OUTPUT_R_ROOT；
    2. 与 Output_r 同级的 Output_r_attr_fixed（若存在）；
    3. 脚本所在目录上一级 Output_r。
    """
    env_root = os.environ.get("OUTPUT_R_ROOT", "").strip()
    if env_root:
        return Path(env_root).expanduser().resolve()

    script_dir = Path(script_dir).resolve()
    output_r_root = script_dir.parent
    project_root = output_r_root.parent
    attr_fixed_root = project_root / "Output_r_attr_fixed"
    if attr_fixed_root.is_dir():
        return attr_fixed_root
    return output_r_root


def classify_frequency(time_values):
    """根据时间间隔推断频率。"""
    if len(time_values) < 2:
        return "single_point"

    diffs = np.diff(time_values) / np.timedelta64(1, "h")
    median_diff = np.median(diffs)

    if median_diff < 2:
        return "hourly"
    elif median_diff < 36:
        return "daily"
    elif median_diff < 24 * 45:
        return "monthly"
    elif median_diff < 24 * 120:
        return "quarterly"
    elif median_diff < 24 * 500:
        return "annual"
    else:
        return "irregular"


def infer_temporal_semantics(detected_frequency, single_point_interpretation=""):
    """将检测到的时间频率进一步解释为更稳定的时间语义。"""
    freq = str(detected_frequency or "").strip().lower()
    interp = str(single_point_interpretation or "").strip().lower()

    if freq in ("hourly", "daily"):
        return "daily"
    if freq == "monthly":
        return "monthly"
    if freq == "quarterly":
        return "monthly"
    if freq == "annual":
        if (
            "climatology" in interp
            or "long_term_average" in interp
            or "long-term average" in interp
            or "historical average" in interp
        ):
            return "climatology"
        return "annual"
    if freq == "single_point":
        if (
            "climatology" in interp
            or "long_term_average" in interp
            or "average" in interp
            or "mean" in interp
        ):
            return "climatology"
        return "daily"
    if freq == "irregular":
        return "irregular"
    if freq == "no_time_var":
        return "no_time_var"
    if freq.startswith("error:"):
        return "error"
    return "other"


def should_treat_irregular_as_daily(nc_path):
    """对 irregular 文件做二次判定：若时间轴呈离散日值记录，则按 daily 处理。"""
    try:
        with xr.open_dataset(str(nc_path)) as ds:
            time_var = next((name for name in TIME_VAR_NAMES if name in ds.variables), None)
            if time_var is None:
                return False

            times = pd.to_datetime(ds[time_var].values, errors="coerce")
            if getattr(times, "size", 0) < 1:
                return False

            ts = pd.Series(times).dropna()
            if len(ts) < 1:
                return False

            if not ((ts.dt.hour == 0) & (ts.dt.minute == 0) & (ts.dt.second == 0)).all():
                return False

            return True
    except Exception:
        return False


def sync_temporal_resolution_attrs(nc_path, target_resolution, stage, reason="", update_legacy_existing_only=True):
    """将 temporal_resolution 同步到 nc 全局属性中。

    规则：
    - 总是维护 canonical 键 temporal_resolution；
    - legacy 键仅在文件原本已存在时同步更新；
    - 仅当值确实变化时才追加 history。
    """
    target = str(target_resolution or "").strip()
    if not target:
        return {
            "changed": False,
            "changed_keys": [],
            "old_resolution": "",
            "new_resolution": "",
        }

    with _open_netcdf_for_attrs(str(nc_path), "r") as ds:
        existing_attrs = _read_dataset_attrs(ds)

    old_resolution = _first_nonempty_resolution(existing_attrs)
    changed_keys = []

    if str(existing_attrs.get("temporal_resolution", "")).strip() != target:
        changed_keys.append("temporal_resolution")

    for key in LEGACY_TEMPORAL_RESOLUTION_KEYS:
        if update_legacy_existing_only and key not in existing_attrs:
            continue
        if str(existing_attrs.get(key, "")).strip() != target:
            changed_keys.append(key)

    if not changed_keys:
        return {
            "changed": False,
            "changed_keys": [],
            "old_resolution": old_resolution,
            "new_resolution": target,
        }

    history_note = _build_history_note(stage, old_resolution, target, reason)
    new_history = _append_history(existing_attrs.get("history", ""), history_note)

    with _open_netcdf_for_attrs(str(nc_path), "a") as ds:
        _set_dataset_attr(ds, "temporal_resolution", target)
        for key in LEGACY_TEMPORAL_RESOLUTION_KEYS:
            if update_legacy_existing_only and key not in existing_attrs:
                continue
            _set_dataset_attr(ds, key, target)
        if new_history != str(existing_attrs.get("history", "")).strip():
            _set_dataset_attr(ds, "history", new_history)
            if "history" not in changed_keys:
                changed_keys.append("history")

    return {
        "changed": True,
        "changed_keys": changed_keys,
        "old_resolution": old_resolution,
        "new_resolution": target,
    }


def _first_nonempty_resolution(attrs):
    for key in ("temporal_resolution",) + LEGACY_TEMPORAL_RESOLUTION_KEYS:
        value = str(attrs.get(key, "")).strip()
        if value and value.lower() not in ("none", "nan"):
            return value
    return ""


def _build_history_note(stage, old_resolution, new_resolution, reason):
    old_text = str(old_resolution or "").strip() or "(empty)"
    new_text = str(new_resolution or "").strip() or "(empty)"
    reason_text = str(reason or "").strip()
    note = "[{0}] temporal_resolution: {1} -> {2}".format(stage, old_text, new_text)
    if reason_text:
        note += " ({0})".format(reason_text)
    return note


def _append_history(existing_history, note):
    existing_text = str(existing_history or "").strip()
    note = str(note or "").strip()
    if not note:
        return existing_text
    if note in existing_text:
        return existing_text
    if not existing_text:
        return note
    return "{0}\n{1}".format(existing_text, note)


@contextmanager
def _open_netcdf_for_attrs(nc_path, mode):
    try:
        import netCDF4 as nc4

        ds = nc4.Dataset(str(nc_path), mode)
        try:
            yield ds
        finally:
            ds.close()
        return
    except Exception:
        pass

    import h5netcdf

    ds = h5netcdf.File(str(nc_path), mode)
    try:
        yield ds
    finally:
        ds.close()


def _read_dataset_attrs(ds):
    try:
        if hasattr(ds, "ncattrs"):
            return dict((key, str(getattr(ds, key, "")).strip()) for key in ds.ncattrs())
    except Exception:
        pass
    try:
        return dict((str(key), str(value).strip()) for key, value in ds.attrs.items())
    except Exception:
        return {}


def _set_dataset_attr(ds, key, value):
    if hasattr(ds, "setncattr"):
        ds.setncattr(key, value)
    else:
        try:
            if key in ds.attrs and hasattr(ds.attrs, "_h5attrs") and hasattr(ds.attrs._h5attrs, "modify"):
                ds.attrs._h5attrs.modify(key, value)
                return
        except Exception:
            pass
        try:
            ds.attrs[key] = value
        except Exception:
            try:
                if key in ds.attrs:
                    del ds.attrs[key]
            except Exception:
                pass
            ds.attrs[key] = value
