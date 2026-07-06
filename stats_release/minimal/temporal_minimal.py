#!/usr/bin/env python3
"""Temporal coverage statistics for sed_reference_release_minimal."""

from __future__ import annotations

import sys
from pathlib import Path


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from stats_release.temporal import main as temporal_main


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MINIMAL_RELEASE_DIR = PROJECT_ROOT / "output" / "sed_reference_release_minimal"
DEFAULT_OUT_DIR = PROJECT_ROOT / "output_other" / "stats_release_minimal" / "temporal"


def _has_option(argv: list[str], option: str) -> bool:
    return any(arg == option or arg.startswith(option + "=") for arg in argv)


def main(argv=None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not _has_option(args, "--release-dir"):
        args[:0] = ["--release-dir", str(DEFAULT_MINIMAL_RELEASE_DIR)]
    if not _has_option(args, "--out-dir"):
        args[:0] = ["--out-dir", str(DEFAULT_OUT_DIR)]
    return temporal_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
