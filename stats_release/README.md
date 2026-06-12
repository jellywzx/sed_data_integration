# S8 Release-Only Stats

This directory contains the release-facing statistics suite for the sediment
reference dataset.

All modules are designed to read only files inside:

```text
output/sed_reference_release/
```

Outputs are written under:

```text
output_other/stats_release/
```

## Run

```bash
python3 -m stats_release.run_all_release_stats
```

The all-module runner cleans its managed output subdirectories by default and
writes `run_manifest.csv` plus `run_manifest.json` with the release and script
fingerprints for reproducibility.

Run one module:

```bash
python3 -m stats_release.spatial --release-dir output/sed_reference_release
```

## Guardrail

`--strict-release-only` is enabled by default. The shared I/O layer rejects
input paths outside the release package. Use `--allow-non-release-inputs` only
for local debugging. Markdown reports stay under the stats output directory
unless `--copy-reports` is passed.

## Modules

- `inventory`: package file inventory and lightweight product details.
- `spatial`: catalog/GIS sidecar spatial coverage summaries.
- `temporal`: release time-span and record-count summaries.
- `variable_summary`: Q/SSC/SSL coverage from release NetCDF products.
- `source_contribution`: source contribution tables from release catalogs.
- `qc_flags`: QC flag counts from release NetCDF products.
- `basin_diagnostics`: basin status and distance diagnostics from the station catalog.
- `source_dataset_layers`: source membership across release catalogs.
