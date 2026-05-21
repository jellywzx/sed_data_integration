#!/usr/bin/env python3
"""Export only the monthly basin matrix NetCDF."""

import sys

from s6_export_resolution_matrix_ncs import main as export_main


def _build_argv(argv):
    if "--resolutions" in argv:
        raise SystemExit(
            "s6_export_monthly_matrix_nc.py fixes --resolutions monthly; "
            "please omit --resolutions."
        )
    return ["--resolutions", "monthly"] + list(argv)


if __name__ == "__main__":
    raise SystemExit(export_main(_build_argv(sys.argv[1:])))
