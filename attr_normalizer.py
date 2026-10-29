#!/usr/bin/env python3
"""Thin adapter around the shared canonical global-attribute normalizer."""

import sys
from pathlib import Path


SCRIPT_ROOT = next(
    (parent / "Script" for parent in Path(__file__).resolve().parents
     if (parent / "Script").is_dir()),
    None,
)
if SCRIPT_ROOT is None:
    raise RuntimeError("Stage-1 Script directory not found above {}".format(__file__))
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from code.global_attrs import normalize_nc_attrs as _shared_normalize_nc_attrs


def normalize_nc_attrs(path):
    """Normalize global attrs using the shared Script/code implementation."""
    return _shared_normalize_nc_attrs(path)
