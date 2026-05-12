#!/usr/bin/env python3
"""Explain the basin fallback rule with synthetic policy cases and a PNG.

This helper is intentionally lightweight:
  - no shapely/geopandas dependency;
  - synthetic geometry only;
  - reuses basin_policy.classify_basin_result() for the release labels.

Running the script will:
  1. print five synthetic cases that mirror the README explanation;
  2. assert that the current basin policy still behaves as expected;
  3. write a two-panel explainer image to output/.
"""

from collections import namedtuple
import math
from pathlib import Path
from typing import Iterable, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Polygon, Rectangle

from basin_policy import classify_basin_result


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
DEFAULT_OUT = OUTPUT_DIR / "basin_fallback_rule_explainer.png"

Point = Tuple[float, float]


SyntheticCase = namedtuple(
    "SyntheticCase",
    [
        "label",
        "method",
        "source_name",
        "distance_m",
        "point_in_local",
        "point_in_basin",
        "expected",
        "note",
    ],
)


def _build_cases() -> Iterable[SyntheticCase]:
    return (
        SyntheticCase(
            label="traced_basin_can_set_point_in_basin",
            method="upstream_traced",
            source_name="USGS",
            distance_m=250.0,
            point_in_local=False,
            point_in_basin=True,
            expected=("resolved", "ok"),
            note="A real traced upstream polygon exists, so point_in_basin may be True.",
        ),
        SyntheticCase(
            label="fallback_buffer_stays_diagnostic_only",
            method="area_buffer_fallback",
            source_name="USGS",
            distance_m=800.0,
            point_in_local=False,
            point_in_basin=False,
            expected=("unresolved", "geometry_inconsistent"),
            note=(
                "The fallback buffer is centered on the station, so the raw circle would "
                "contain the point. The published point_in_basin field must still stay False."
            ),
        ),
        SyntheticCase(
            label="local_catchment_within_1km_is_enough",
            method="upstream_traced",
            source_name="USGS",
            distance_m=800.0,
            point_in_local=True,
            point_in_basin=False,
            expected=("resolved", "ok"),
            note="Within 1 km, point_in_local=True can resolve the station even without basin=True.",
        ),
        SyntheticCase(
            label="gsed_is_skipped_no_basin_match",
            method="upstream_traced",
            source_name="GSED",
            distance_m=2000.0,
            point_in_local=True,
            point_in_basin=False,
            expected=("unresolved", "no_match"),
            note="GSED keeps the observation but skips MERIT basin assignment.",
        ),
        SyntheticCase(
            label="generic_large_offset_stays_unresolved",
            method="upstream_traced",
            source_name="USGS",
            distance_m=2000.0,
            point_in_local=True,
            point_in_basin=True,
            expected=("unresolved", "large_offset"),
            note="For ordinary sources, offsets above 1 km stay unresolved even with geometry diagnostics.",
        ),
    )


def _circle_covers_point(center: Point, radius: float, point: Point) -> bool:
    """Return True when a synthetic circle covers a point."""
    return math.hypot(point[0] - center[0], point[1] - center[1]) <= radius + 1e-12


def _assert_case(case: SyntheticCase) -> Tuple[str, str]:
    actual = classify_basin_result(
        basin_id=12345,
        match_quality="distance_only",
        distance_m=case.distance_m,
        source_name=case.source_name,
        point_in_local=case.point_in_local,
        point_in_basin=case.point_in_basin,
    )
    if actual != case.expected:
        raise AssertionError(
            "{}: expected {}, got {}".format(case.label, case.expected, actual)
        )
    return actual


def _validate_teaching_assumptions(cases: Iterable[SyntheticCase]) -> None:
    case_map = {case.label: case for case in cases}

    for case in case_map.values():
        _assert_case(case)

    station = (2.2, 2.0)
    fallback_center = station
    fallback_radius = 1.4
    raw_buffer_contains_station = _circle_covers_point(fallback_center, fallback_radius, station)
    if not raw_buffer_contains_station:
        raise AssertionError("Synthetic fallback buffer should cover the station.")

    fallback_case = case_map["fallback_buffer_stays_diagnostic_only"]
    if fallback_case.method != "area_buffer_fallback" or fallback_case.point_in_basin:
        raise AssertionError("Fallback teaching case must keep point_in_basin=False.")

    gsed_case = case_map["gsed_is_skipped_no_basin_match"]
    gsed_same_with_basin_true = classify_basin_result(
        basin_id=12345,
        match_quality="distance_only",
        distance_m=gsed_case.distance_m,
        source_name=gsed_case.source_name,
        point_in_local=True,
        point_in_basin=True,
    )
    if gsed_same_with_basin_true != gsed_case.expected:
        raise AssertionError("GSED should stay no_match even when geometry diagnostics are true.")

    large_offset_case = case_map["generic_large_offset_stays_unresolved"]
    large_offset_actual = classify_basin_result(
        basin_id=12345,
        match_quality="distance_only",
        distance_m=large_offset_case.distance_m,
        source_name=large_offset_case.source_name,
        point_in_local=True,
        point_in_basin=True,
    )
    if large_offset_actual != large_offset_case.expected:
        raise AssertionError("Ordinary large-offset case should stay unresolved.")


def _print_cases(cases: Iterable[SyntheticCase]) -> None:
    line = "-" * 126
    print("Synthetic basin fallback explainer")
    print(line)
    print(
        "{:<38} {:<20} {:<8} {:>10} {:>16} {:>16} {:<12} {:<24}".format(
            "case",
            "method",
            "source",
            "distance_m",
            "point_in_local",
            "point_in_basin",
            "status",
            "flag",
        )
    )
    print(line)
    for case in cases:
        status, flag = _assert_case(case)
        print(
            "{:<38} {:<20} {:<8} {:>10.0f} {:>16} {:>16} {:<12} {:<24}".format(
                case.label,
                case.method,
                case.source_name,
                case.distance_m,
                str(case.point_in_local),
                str(case.point_in_basin),
                status,
                flag,
            )
        )
        print("  note: {}".format(case.note))
    print(line)

    station = (2.2, 2.0)
    raw_buffer_contains_station = _circle_covers_point(station, 1.4, station)
    print("Fallback geometry sanity check")
    print("  synthetic_buffer_centered_on_station = True")
    print("  raw_circle_covers_station           = {}".format(raw_buffer_contains_station))
    print("  published_point_in_basin            = False")
    print(
        "  reason: a fallback circle is a rough area proxy, not traced upstream basin evidence."
    )


def _style_axis(ax, title: str) -> None:
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xlim(0.0, 5.0)
    ax.set_ylim(0.0, 4.5)
    ax.set_aspect("equal", adjustable="box")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)


def _draw_upstream_panel(ax) -> None:
    station = (2.1, 2.1)
    upstream_polygon = Polygon(
        [(0.7, 0.8), (4.0, 0.9), (4.5, 3.2), (3.2, 4.0), (1.1, 3.7), (0.5, 2.2)],
        closed=True,
        facecolor="#b8d8ba",
        edgecolor="#2b6a3f",
        linewidth=2.0,
        alpha=0.75,
    )
    local_catchment = Rectangle(
        (1.6, 1.5),
        0.9,
        0.8,
        facecolor="#f6d88f",
        edgecolor="#9b6a00",
        linewidth=1.6,
        alpha=0.95,
    )
    ax.add_patch(upstream_polygon)
    ax.add_patch(local_catchment)
    ax.scatter(
        [station[0]],
        [station[1]],
        marker="*",
        s=260,
        color="#d14b2a",
        edgecolors="black",
        linewidths=0.8,
        zorder=5,
    )
    ax.text(2.85, 1.9, "station", fontsize=10, color="#7a2312")
    ax.text(
        0.35,
        4.2,
        "method = upstream_traced\n"
        "real upstream polygon covers station = True\n"
        "published point_in_basin = True",
        va="top",
        ha="left",
        fontsize=9.5,
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "#f7fbf7", "edgecolor": "#90b998"},
    )
    ax.text(2.05, 2.55, "local catchment", fontsize=9, color="#765000", ha="center")
    _style_axis(ax, "Real Traced Basin")


def _draw_fallback_panel(ax) -> None:
    station = (2.2, 2.0)
    fallback_buffer = Circle(
        station,
        radius=1.4,
        facecolor="#cfe4ff",
        edgecolor="#2c5f99",
        linewidth=2.0,
        alpha=0.78,
    )
    local_catchment = Rectangle(
        (3.55, 1.2),
        0.8,
        0.75,
        facecolor="#f6d88f",
        edgecolor="#9b6a00",
        linewidth=1.5,
        alpha=0.95,
    )
    ax.add_patch(fallback_buffer)
    ax.add_patch(local_catchment)
    ax.scatter(
        [station[0]],
        [station[1]],
        marker="*",
        s=260,
        color="#d14b2a",
        edgecolors="black",
        linewidths=0.8,
        zorder=5,
    )
    ax.text(2.75, 1.9, "station", fontsize=10, color="#7a2312")
    ax.text(
        0.35,
        4.2,
        "method = area_buffer_fallback\n"
        "raw fallback circle covers station = True\n"
        "published point_in_basin = False",
        va="top",
        ha="left",
        fontsize=9.5,
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "#f6f9ff", "edgecolor": "#8aa7cf"},
    )
    ax.text(
        2.2,
        0.45,
        "The circle is centered on the station.\n"
        "If we exposed that as point_in_basin=True,\n"
        "the field would stop meaning real traced basin evidence.",
        va="bottom",
        ha="center",
        fontsize=9.0,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#c7d3e4"},
    )
    _style_axis(ax, "Fallback Buffer Is Not Basin Evidence")


def write_explainer_plot(out_png: Path = DEFAULT_OUT) -> Path:
    fig, axes = plt.subplots(1, 2, figsize=(13.5, 6.6), constrained_layout=True)
    _draw_upstream_panel(axes[0])
    _draw_fallback_panel(axes[1])
    fig.suptitle(
        "Why area_buffer_fallback cannot set point_in_basin=True",
        fontsize=15,
        fontweight="bold",
    )
    fig.text(
        0.5,
        0.02,
        "Left: point_in_basin is trustworthy only when a real upstream polygon was traced.   "
        "Right: the fallback circle is centered on the station, so its raw coverage is not basin evidence.",
        ha="center",
        va="bottom",
        fontsize=9.5,
    )
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return out_png


def main() -> None:
    cases = tuple(_build_cases())
    _validate_teaching_assumptions(cases)
    _print_cases(cases)
    out_png = write_explainer_plot(DEFAULT_OUT)
    print("Wrote explainer plot: {}".format(out_png))


if __name__ == "__main__":
    main()
