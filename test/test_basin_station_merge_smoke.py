import tempfile
from pathlib import Path

import pandas as pd

from basin_station_merge import load_station_to_basin_cluster_map


def _build_inputs():
    basin_df = pd.DataFrame(
        [
            {"station_id": 0, "basin_id": 1, "basin_status": "resolved", "uparea_merit": 100.0},
            {"station_id": 1, "basin_id": 1, "basin_status": "resolved", "uparea_merit": 105.0},
            {"station_id": 2, "basin_id": 2, "basin_status": "resolved", "uparea_merit": 200.0},
            {"station_id": 3, "basin_id": 2, "basin_status": "resolved", "uparea_merit": 202.0},
            {"station_id": 4, "basin_id": 3, "basin_status": "resolved", "uparea_merit": 100.0},
            {"station_id": 5, "basin_id": 3, "basin_status": "resolved", "uparea_merit": 130.0},
            {"station_id": 6, "basin_id": 4, "basin_status": "resolved", "uparea_merit": 150.0},
            {"station_id": 7, "basin_id": 5, "basin_status": "resolved", "uparea_merit": 152.0},
            {"station_id": 8, "basin_id": 1, "basin_status": "unresolved", "uparea_merit": 101.0},
            {"station_id": 9, "basin_id": 6, "basin_status": "resolved", "uparea_merit": 100.0},
            {"station_id": 10, "basin_id": 6, "basin_status": "resolved", "uparea_merit": 109.0},
            {"station_id": 11, "basin_id": 6, "basin_status": "resolved", "uparea_merit": 119.0},
        ]
    )
    station_df = pd.DataFrame(
        [
            {"station_id": 0, "lat": 0.0, "lon": 0.0},
            {"station_id": 1, "lat": 0.0, "lon": 0.02},
            {"station_id": 2, "lat": 0.0, "lon": 1.0},
            {"station_id": 3, "lat": 0.0, "lon": 1.10},
            {"station_id": 4, "lat": 1.0, "lon": 0.0},
            {"station_id": 5, "lat": 1.0, "lon": 0.02},
            {"station_id": 6, "lat": 2.0, "lon": 0.0},
            {"station_id": 7, "lat": 2.0, "lon": 0.01},
            {"station_id": 8, "lat": 0.0, "lon": 0.01},
            {"station_id": 9, "lat": 3.0, "lon": 0.00},
            {"station_id": 10, "lat": 3.0, "lon": 0.02},
            {"station_id": 11, "lat": 3.0, "lon": 0.04},
        ]
    )
    return basin_df, station_df


def main():
    basin_df, station_df = _build_inputs()
    with tempfile.TemporaryDirectory() as tmp:
        basin_csv = Path(tmp) / "basin.csv"
        basin_df.to_csv(basin_csv, index=False)
        mapping, stats = load_station_to_basin_cluster_map(
            basin_csv,
            station_df=station_df,
            max_station_distance_m=5000.0,
            max_upstream_rel_error=0.10,
            upstream_area_col="uparea_merit",
        )

    # a) same basin + distance < 5 km + area error < 10% -> merge
    assert mapping[0] == 0 and mapping[1] == 0
    # b) same basin + distance > 5 km -> no merge
    assert mapping[2] == 2 and mapping[3] == 3
    # c) same basin + area error > 10% -> no merge
    assert mapping[4] == 4 and mapping[5] == 5
    # d) different basin -> no merge
    assert mapping[6] == 6 and mapping[7] == 7
    # e) unresolved basin -> no merge
    assert mapping[8] == 8
    # f) A-B compatible, B-C compatible, A-C incompatible -> not all three merged
    assert mapping[9] == 9 and mapping[10] == 9 and mapping[11] == 11

    assert stats["max_station_distance_m"] == 5000.0
    assert stats["max_upstream_rel_error"] == 0.10
    assert stats["upstream_area_col"] == "uparea_merit"
    print("smoke test passed")


if __name__ == "__main__":
    main()
