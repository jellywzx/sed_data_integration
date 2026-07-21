#!/usr/bin/env python3
"""Fast satellite-to-main-cluster linkage using the released station catalog.

This is an alternative producer for ``s5b_satellite_main_cluster_linkage.csv``.
It preserves the linkage rules implemented by
``s5b_link_satellite_to_main_clusters.py`` but replaces the three main-matrix
NetCDF reads with one lightweight ``station_catalog.csv`` read.

Inputs
------