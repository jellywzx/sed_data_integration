#!/usr/bin/env python3
"""Shared metadata schema and constants for release NetCDF products.

This module defines the canonical attribute values, product descriptions,
and variable schemas used by both the conventions normalizer
(``release_netcdf_conventions.py``) and the S6 export scripts.

All three product families (matrix, climatology, satellite) share the same
science-variable metadata, flag definitions, coordinate attributes, and
ACDD discovery defaults defined here.
"""

from __future__ import annotations

import numpy as np

# ---------------------------------------------------------------------------
# Convention identifiers
# ---------------------------------------------------------------------------

CF_CONVENTION = "CF-1.8"
ACDD_CONVENTION = "ACDD-1.3"

# ---------------------------------------------------------------------------
# Flag schema (shared across Q_flag / SSC_flag / SSL_flag)
# ---------------------------------------------------------------------------

FLAG_VALUES = np.array([0, 1, 2, 3, 9], dtype=np.int8)
FLAG_MEANINGS = "good derived suspect bad missing"

# ---------------------------------------------------------------------------
# Science variable units and long names
# ---------------------------------------------------------------------------

SCIENCE_UNITS = {
    "Q": "m3 s-1",
    "SSC": "mg L-1",
    "SSL": "t d-1",
}

SCIENCE_LONG_NAMES = {
    "Q": "river discharge",
    "SSC": "suspended sediment concentration",
    "SSL": "suspended sediment load",
}

# ---------------------------------------------------------------------------
# Coordinate variable attributes
# ---------------------------------------------------------------------------

COORD_ATTRS = {
    "lat": {"standard_name": "latitude", "units": "degrees_north", "axis": "Y"},
    "lon": {"standard_name": "longitude", "units": "degrees_east", "axis": "X"},
    "time": {"standard_name": "time", "axis": "T"},
}

# ---------------------------------------------------------------------------
# Attribute parity check lists
# ---------------------------------------------------------------------------

CF_PARITY_ATTRS = (
    "standard_name",
    "axis",
    "cf_role",
    "coordinates",
    "ancillary_variables",
    "instance_dimension",
    "units",
    "flag_values",
    "flag_meanings",
)

VARIABLE_PARITY_ATTRS = CF_PARITY_ATTRS + ("coverage_content_type",)

ACDD_PARITY_GLOBAL_ATTRS = (
    "Conventions",
    "featureType",
    "title",
    "summary",
    "keywords",
    "naming_authority",
    "creator_name",
    "creator_email",
    "creator_institution",
    "institution",
    "license",
    "references",
    "geospatial_lat_min",
    "geospatial_lat_max",
    "geospatial_lon_min",
    "geospatial_lon_max",
    "time_coverage_start",
    "time_coverage_end",
    "time_coverage_resolution",
    "standard_name_vocabulary",
)

ACDD_PUBLICATION_DEPENDENT_ATTRS = (
    "references",
    "license",
    "publisher_name",
    "publisher_email",
    "publisher_url",
    "metadata_link",
    "date_issued",
)

# ---------------------------------------------------------------------------
# Shared ACDD global-attribute defaults
# ---------------------------------------------------------------------------

RELEASE_ACDD_CONFIG = {
    "creator_name": "Zixin Wei",
    "creator_email": "weizx6@mail2.sysu.edu.cn",
    "creator_institution": "Sun Yat-sen University",
    "institution": "Sun Yat-sen University",
    "project": (
        "A Harmonized Global Station-Reference Dataset of River Discharge, "
        "Suspended Sediment Concentration, and Suspended Sediment Load"
    ),
    "naming_authority": "org.earth-system-science-data",
    "keywords": (
        "river discharge, suspended sediment concentration, "
        "suspended sediment load, river sediment, global rivers, "
        "station observations, time series, data harmonization, "
        "quality control, source traceability, model evaluation"
    ),
    "processing_level": "Harmonized, quality-controlled, source-traceable release product",
    "standard_name_vocabulary": "CF Standard Name Table v94",
    "acknowledgement": "",
    "license": (
        "Creative Commons Attribution 4.0 International "
        "(CC BY 4.0); https://creativecommons.org/licenses/by/4.0/"
    ),
    "references": (
        "Dataset manuscript: <MANUSCRIPT_CITATION_OR_DOI>; "
        "source-dataset references are provided in source_dataset_catalog.csv."
    ),
    "publisher_name": "Zenodo",
    "citation": (
        "Dataset manuscript: <MANUSCRIPT_CITATION_OR_DOI>; "
        "source-dataset references are provided in source_dataset_catalog.csv."
    ),
}

# ---------------------------------------------------------------------------
# Per-product descriptions (title, summary, resolution, CDM type, comment)
# ---------------------------------------------------------------------------

PRODUCT_DESCRIPTIONS = {
    "master": {
        "title": "A Harmonized Global Station-Reference Dataset of River Discharge, Suspended Sediment Concentration, and Suspended Sediment Load: Master Record-Level Dataset",
        "summary": (
            "Authoritative record-level release product for harmonized river discharge, "
            "suspended sediment concentration, and suspended sediment load observations, "
            "including source-station provenance."
        ),
        "time_coverage_resolution": "",
        "cdm_data_type": "Other",
        "comment": "Master records may mix temporal resolutions; no single ACDD time_coverage_resolution is assigned.",
    },
    "daily_matrix": {
        "title": "A Harmonized Global Station-Reference Dataset of River Discharge, Suspended Sediment Concentration, and Suspended Sediment Load: Daily Station-Reference Matrix",
        "summary": (
            "This product provides harmonized daily observations of river "
            "discharge (Q), suspended sediment concentration (SSC), and "
            "suspended sediment load (SSL), organized as station-by-time "
            "matrices. Observations were standardized, quality flagged, "
            "georeferenced to the MERIT-Basins river network, and integrated "
            "across hydrologically comparable source stations. Record-level "
            "source links are retained through station and source catalogues."
        ),
        "time_coverage_resolution": "P1D",
        "cdm_data_type": "TimeSeries",
        "comment": "Daily matrix product; each populated cell keeps selected source-station provenance.",
    },
    "monthly_matrix": {
        "title": "A Harmonized Global Station-Reference Dataset of River Discharge, Suspended Sediment Concentration, and Suspended Sediment Load: Monthly Station-Reference Matrix",
        "summary": (
            "This product provides harmonized monthly observations of river "
            "discharge (Q), suspended sediment concentration (SSC), and "
            "suspended sediment load (SSL), organized as station-by-time "
            "matrices. Observations were standardized, quality flagged, "
            "georeferenced to the MERIT-Basins river network, and integrated "
            "across hydrologically comparable source stations. Record-level "
            "source links are retained through station and source catalogues."
        ),
        "time_coverage_resolution": "P1M",
        "cdm_data_type": "TimeSeries",
        "comment": "Monthly matrix product; each populated cell keeps selected source-station provenance.",
    },
    "annual_matrix": {
        "title": "A Harmonized Global Station-Reference Dataset of River Discharge, Suspended Sediment Concentration, and Suspended Sediment Load: Annual Station-Reference Matrix",
        "summary": (
            "This product provides harmonized annual observations of river "
            "discharge (Q), suspended sediment concentration (SSC), and "
            "suspended sediment load (SSL), organized as station-by-time "
            "matrices. Observations were standardized, quality flagged, "
            "georeferenced to the MERIT-Basins river network, and integrated "
            "across hydrologically comparable source stations. Record-level "
            "source links are retained through station and source catalogues."
        ),
        "time_coverage_resolution": "P1Y",
        "cdm_data_type": "TimeSeries",
        "comment": "Annual matrix product; each populated cell keeps selected source-station provenance.",
    },
    "climatology": {
        "title": "A Harmonized Global Station-Reference Dataset of River Discharge, Suspended Sediment Concentration, and Suspended Sediment Load: Climatology Auxiliary Product",
        "summary": (
            "This auxiliary product provides harmonized climatological "
            "observations of river discharge (Q), suspended sediment "
            "concentration (SSC), and suspended sediment load (SSL). "
            "Climatological observations are retained separately from the main "
            "daily, monthly, and annual station-reference matrices and provide "
            "long-term regional context, particularly where time-resolved gauge "
            "coverage is sparse."
        ),
        "time_coverage_resolution": "",
        "cdm_data_type": "TimeSeries",
        "comment": "Climatology records can represent source-specific climatological periods; no single ACDD time_coverage_resolution is assigned.",
    },
    "satellite": {
        "title": "A Harmonized Global Station-Reference Dataset of River Discharge, Suspended Sediment Concentration, and Suspended Sediment Load: Satellite-Derived Auxiliary Product",
        "summary": (
            "This auxiliary product provides harmonized satellite-derived river "
            "sediment observations from RiverSed, GSED, and Dethier datasets. "
            "Where spatial and temporal matching criteria were satisfied, "
            "satellite-derived stations were linked to the main station-reference "
            "stations. The product supports assessment of broad spatial sediment "
            "patterns, identification of gauge-coverage gaps, and complementary "
            "comparison with station-reference observations."
        ),
        "time_coverage_resolution": "",
        "cdm_data_type": "TimeSeries",
        "comment": "Satellite validation records can be irregular or mixed resolution; no single ACDD time_coverage_resolution is assigned.",
    },
}

# ---------------------------------------------------------------------------
# Product kind registry
# ---------------------------------------------------------------------------

PRODUCT_ALIASES = {
    "daily": "daily_matrix",
    "monthly": "monthly_matrix",
    "annual": "annual_matrix",
    "matrix_daily": "daily_matrix",
    "matrix_monthly": "monthly_matrix",
    "matrix_annual": "annual_matrix",
    "satellite_validation": "satellite",
}

SUPPORTED_PRODUCT_KINDS = {
    "master",
    "daily_matrix",
    "monthly_matrix",
    "annual_matrix",
    "climatology",
    "satellite",
}
