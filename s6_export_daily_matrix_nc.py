#!/usr/bin/env python3
"""Export only the daily basin matrix NetCDF."""

import sys

from s6_export_resolution_matrix_ncs import main as export_main


def _build_argv(argv):
    if "--resolutions" in argv:
        raise SystemExit(
            "s6_export_daily_matrix_nc.py fixes --resolutions daily; "
            "please omit --resolutions."
        )
    return ["--resolutions", "daily"] + list(argv)


if __name__ == "__main__":
    raise SystemExit(export_main(_build_argv(sys.argv[1:])))
