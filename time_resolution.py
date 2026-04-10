"""
时间分辨率分类：根据时间间隔推断 frequency。
供 verify_time_resolution 等脚本调用。
"""
import numpy as np
import pandas as pd


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
    """将检测到的时间频率进一步解释为更稳定的时间语义。

    返回值用于 s2 的重组目录和后续最终 NC 的 time_type：
      daily / monthly / annual / climatology / irregular /
      no_time_var / error / other
    """
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
