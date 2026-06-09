import numpy as np
import pandas as pd

import s6_basin_merge_to_nc as s6
from qc_contract import read_standardized_qc_stage_arrays


class _FakeVar:
    def __init__(self, values):
        self._values = np.asarray(values, dtype=np.int8)

    def __getitem__(self, key):
        return self._values[key]


class _FakeDataset:
    def __init__(self, variables):
        self.variables = dict(variables)


def _series(rows):
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    for col in ["Q", "SSC", "SSL"]:
        if col not in frame:
            frame[col] = np.nan
    for col in ["Q_flag", "SSC_flag", "SSL_flag"]:
        if col not in frame:
            frame[col] = 9
    for field_name in s6.STANDARD_QC_STAGE_NAMES:
        if field_name not in frame:
            frame[field_name] = s6.STANDARD_QC_STAGE_NAME_TO_SPEC[field_name]["fill_value"]
    return frame


def test_bayern_ssc_qc3_consistency_alias_maps_to_standard_qc3():
    ds = _FakeDataset(
        {
            "SSC_flag_qc3_ssc_q_consistency": _FakeVar([0, 2, 8, 9]),
        }
    )

    arrays = read_standardized_qc_stage_arrays(ds, size=4)

    assert "SSC_qc3" in arrays
    assert arrays["SSC_qc3"].tolist() == [0, 2, 8, 9]


def _with_series(frames, func):
    original = s6.load_nc_series

    def fake_load_nc_series(path):
        return frames[str(path)].copy(), []

    s6.load_nc_series = fake_load_nc_series
    try:
        return func()
    finally:
        s6.load_nc_series = original


def test_q_only_time_steps_are_omitted():
    frames = {
        "source_a.nc": _series(
            [
                {
                    "date": "2020-01-01",
                    "Q": 10.0,
                    "SSC": np.nan,
                    "SSL": np.nan,
                    "Q_flag": 0,
                    "SSC_flag": 9,
                    "SSL_flag": 9,
                },
                {
                    "date": "2020-01-02",
                    "Q": 11.0,
                    "SSC": 20.0,
                    "SSL": np.nan,
                    "Q_flag": 0,
                    "SSC_flag": 0,
                    "SSL_flag": 9,
                },
            ]
        )
    }

    def run():
        return s6.build_cluster_series(
            1,
            "daily",
            [("SourceA", "in_situ", "source_a.nc", 0)],
        )

    result = _with_series(frames, run)
    dates_arr, q_arr, ssc_arr = result[0], result[1], result[2]
    assert [ts.strftime("%Y-%m-%d") for ts in pd.to_datetime(dates_arr)] == ["2020-01-02"]
    assert q_arr.tolist() == [11.0]
    assert ssc_arr.tolist() == [20.0]


def test_lower_ranked_sediment_record_replaces_higher_ranked_q_only_date():
    frames = {
        "high_q_only.nc": _series(
            [
                {
                    "date": "2020-01-01",
                    "Q": 10.0,
                    "SSC": np.nan,
                    "SSL": np.nan,
                    "Q_flag": 0,
                    "SSC_flag": 9,
                    "SSL_flag": 9,
                }
            ]
        ),
        "sediment.nc": _series(
            [
                {
                    "date": "2020-01-01",
                    "Q": 8.0,
                    "SSC": np.nan,
                    "SSL": 100.0,
                    "Q_flag": 1,
                    "SSC_flag": 9,
                    "SSL_flag": 1,
                }
            ]
        ),
    }

    def run():
        return s6.build_cluster_series(
            2,
            "daily",
            [
                ("HighQOnly", "in_situ", "high_q_only.nc", 0),
                ("SedimentSource", "in_situ", "sediment.nc", 1),
            ],
        )

    result = _with_series(frames, run)
    assert [ts.strftime("%Y-%m-%d") for ts in pd.to_datetime(result[0])] == ["2020-01-01"]
    assert result[3].tolist() == [100.0]
    assert result[9].tolist() == ["SedimentSource"]
    assert result[10].tolist() == [1]


def test_all_q_only_candidates_skip_cluster_resolution():
    frames = {
        "source_a.nc": _series(
            [
                {
                    "date": "2020-01-01",
                    "Q": 10.0,
                    "SSC": np.nan,
                    "SSL": np.nan,
                    "Q_flag": 0,
                    "SSC_flag": 9,
                    "SSL_flag": 9,
                }
            ]
        ),
        "source_b.nc": _series(
            [
                {
                    "date": "2020-01-02",
                    "Q": 12.0,
                    "SSC": np.nan,
                    "SSL": np.nan,
                    "Q_flag": 0,
                    "SSC_flag": 9,
                    "SSL_flag": 9,
                }
            ]
        ),
    }

    def run():
        return s6.build_cluster_series(
            3,
            "monthly",
            [
                ("SourceA", "in_situ", "source_a.nc", 0),
                ("SourceB", "in_situ", "source_b.nc", 1),
            ],
        )

    result = _with_series(frames, run)
    assert result[0] is None
    assert "no publishable sediment records" in result[2]


def main():
    test_q_only_time_steps_are_omitted()
    test_lower_ranked_sediment_record_replaces_higher_ranked_q_only_date()
    test_all_q_only_candidates_skip_cluster_resolution()
    print("q-only filtering tests passed")


if __name__ == "__main__":
    main()
