#!/usr/bin/env python3
"""Unit tests for GSED/RiverSed reach-hint anchor selection."""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    import tqdm as _tqdm  # noqa: F401
except ImportError:
    import types

    class _DummyTqdm:
        def __init__(self, *args, **kwargs):
            self.n = 0

        def refresh(self):
            return None

        def close(self):
            return None

    fake_tqdm = types.ModuleType("tqdm")
    fake_tqdm.tqdm = _DummyTqdm
    sys.modules["tqdm"] = fake_tqdm

from s4_basin_trace_watch import _build_reach_hint_meta, _resolve_reach_hint_anchor  # noqa: E402


class FakeTracer:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def find_best_reach(self, lon, lat, reported_area=None):
        if reported_area is not None:
            raise AssertionError("reach hints must not pass reported_area")
        key = (round(float(lon), 6), round(float(lat), 6))
        self.calls.append(key)
        return self.responses.get(
            key,
            {
                "COMID": None,
                "uparea": float("nan"),
                "distance": float("nan"),
                "pfaf_code": None,
                "match_quality": "failed",
                "area_error": float("nan"),
            },
        )


def _reach(comid, uparea, distance):
    return {
        "COMID": comid,
        "uparea": uparea,
        "distance": distance,
        "pfaf_code": "01",
        "match_quality": "distance_only",
        "area_error": float("nan"),
    }


def test_endpoint_selects_max_uparea_then_min_distance():
    meta = {
        "endpoint_candidates": [
            {"latitude": 1.0, "longitude": 10.0},
            {"latitude": 2.0, "longitude": 20.0},
            {"latitude": 3.0, "longitude": 30.0},
        ],
        "midpoint_latitude": 9.0,
        "midpoint_longitude": 90.0,
    }
    tracer = FakeTracer(
        {
            (10.0, 1.0): _reach(101, 100.0, 5.0),
            (20.0, 2.0): _reach(202, 200.0, 80.0),
            (30.0, 3.0): _reach(303, 200.0, 20.0),
        }
    )
    result = _resolve_reach_hint_anchor(tracer, meta)
    assert result["anchor_source"] == "endpoint"
    assert result["endpoint_match_count"] == 3
    assert result["reach_info"]["COMID"] == 303
    assert result["hint_method"] == "endpoint_uparea_max_distance_min"


def test_midpoint_fallback_after_endpoint_failure():
    meta = {
        "endpoint_candidates": [{"latitude": 1.0, "longitude": 10.0}],
        "midpoint_latitude": 9.0,
        "midpoint_longitude": 90.0,
    }
    tracer = FakeTracer({(90.0, 9.0): _reach(909, 50.0, 7.0)})
    result = _resolve_reach_hint_anchor(tracer, meta)
    assert result["anchor_source"] == "midpoint_fallback"
    assert result["endpoint_match_count"] == 0
    assert result["reach_info"]["COMID"] == 909
    assert tracer.calls == [(10.0, 1.0), (90.0, 9.0)]


def test_build_reach_hint_meta_combines_json_and_columns():
    station = {
        "lat": 9.0,
        "lon": 90.0,
        "reach_endpoint_candidates_json": '[{"latitude":1,"longitude":10}]',
        "reach_endpoint_1_lat": 2.0,
        "reach_endpoint_1_lon": 20.0,
    }
    meta = _build_reach_hint_meta(station)
    assert meta["midpoint_latitude"] == 9.0
    assert meta["midpoint_longitude"] == 90.0
    assert meta["endpoint_candidates"] == [
        {"latitude": 1.0, "longitude": 10.0},
        {"latitude": 2.0, "longitude": 20.0},
    ]


def main():
    test_endpoint_selects_max_uparea_then_min_distance()
    test_midpoint_fallback_after_endpoint_failure()
    test_build_reach_hint_meta_combines_json_and_columns()
    print("reach-hint matching tests passed.")


if __name__ == "__main__":
    main()
