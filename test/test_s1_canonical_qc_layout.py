#!/usr/bin/env python3
"""Regression tests for canonical source-level QC path discovery."""

from s1_verify_time_resolution import _match_dataset_filter


def test_canonical_qc_layout_is_accepted():
    assert _match_dataset_filter(
        ("daily", "GloRiSe", "qc", "GloRiSe_example.nc"),
        [],
    )


def test_nested_qc_layout_is_excluded():
    assert not _match_dataset_filter(
        ("daily", "GloRiSe", "BS", "qc", "GloRiSe_example.nc"),
        [],
    )
