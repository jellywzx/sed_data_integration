#!/usr/bin/env python3
"""Thin adapter around the shared canonical global-attribute normalizer."""

import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[2] / "Script"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from code.global_attrs import normalize_nc_attrs as _shared_normalize_nc_attrs


def normalize_nc_attrs(path):
    """Normalize global attrs using the shared Script/code implementation."""
    return _shared_normalize_nc_attrs(path)
