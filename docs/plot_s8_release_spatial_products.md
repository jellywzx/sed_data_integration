# S8 release spatial product plots

This note documents `plot/plot_s8_release_spatial_products.py`, a plotting helper for the product-level files published by `s8_publish_reference_dataset.py`.

## Purpose

The script draws three spatial distribution figures from the published `sed_reference_release/` package:

1. `fig_s8_climatology_spatial_distribution.png` from `sed_reference_climatology.nc`.
2. `fig_s8_satellite_validation_spatial_distribution.png` from `sed_reference_satellite_validation.nc`.
3. `fig_s8_release_cluster_status_and_basins.png` from the main published product sidecars, showing cluster points by `basin_status` and, when available, the optional basin polygon sidecar.

The basin polygon sidecar is optional. The release README states that basin polygons are only published for rows with `basin_status=resolved` and a valid exported basin polygon. The plotting script therefore treats polygons as an overlay, not as a complete set of all clusters or all resolved clusters.

## Default inputs

The defaults are the s8 release paths configured in `pipeline_paths.py`:

- `scripts_basin_test/output/sed_reference_release/sed_reference_climatology.nc`
- `scripts_basin_test/output/sed_reference_release/sed_reference_satellite_validation.nc`
- `scripts_basin_test/output/sed_reference_release/station_catalog.csv`
- `scripts_basin_test/output/sed_reference_release/sed_reference_cluster_points.gpkg`
- `scripts_basin_test/output/sed_reference_release/sed_reference_cluster_basins.gpkg`

Figures are written by default to:

- `scripts_basin_test/output/sed_reference_release/figures/`

## Usage

From the repository root:

```bash
python plot/plot_s8_release_spatial_products.py
```

With an explicit Output_r root:

```bash
OUTPUT_R_ROOT=/path/to/Output_r python plot/plot_s8_release_spatial_products.py
```

With optional world boundaries for map context:

```bash
python plot/plot_s8_release_spatial_products.py \
  --world-boundaries /path/to/world_boundaries.gpkg
```

Skip the basin polygon overlay if you only want cluster status points:

```bash
python plot/plot_s8_release_spatial_products.py --skip-basins
```

Skip NetCDF maps and only draw the main product cluster map:

```bash
python plot/plot_s8_release_spatial_products.py --skip-nc
```

If a NetCDF uses non-standard coordinate variable names, pass them explicitly:

```bash
python plot/plot_s8_release_spatial_products.py \
  --climatology-lat-var lat --climatology-lon-var lon \
  --satellite-lat-var source_station_lat --satellite-lon-var source_station_lon
```

## Runtime dependencies

Required for all figures:

- `numpy`
- `pandas`
- `matplotlib`

Required to read NetCDF products:

- either `xarray` or `netCDF4`

Required for GPKG inputs and basin polygon overlay:

- `geopandas`

If `geopandas` is unavailable, the script falls back to `station_catalog.csv` for cluster point locations and skips polygon overlays.

## Outputs

The script writes a small manifest next to the figures:

- `s8_release_spatial_plot_manifest.csv`

The manifest records each figure name, source input, plotted point count, and notes about how the figure was generated.
