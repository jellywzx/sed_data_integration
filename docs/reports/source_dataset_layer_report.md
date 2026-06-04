# Source dataset layer report

## Interpretation

`mainline_release_source_dataset_catalog` is the strict final core-release count. It counts source datasets that contributed selected records to the main station-reference product.

Sideline layers include climatology products, satellite/validation-only products, and overlap provenance sidecars. These are intentionally separated from the core mainline product.

## Layer summary

| layer | n_source_datasets | n_rows_or_files | exists | source_column | note |
| --- | --- | --- | --- | --- | --- |
| mainline_organized_inputs_daily_monthly_annual | 23 | 157738 | True | derived_from_nc_paths | organized daily/monthly/annual input NetCDF paths |
| mainline_release_source_dataset_catalog | 17 | 17 | True | source_name |  |
| mainline_release_source_station_catalog | 17 | 3775 | True | source_name |  |
| mainline_s3_collected_stations | 20 | 51887 | True | source |  |
| mainline_s5_clustered_stations | 20 | 51887 | True | source |  |
| mainline_s6_quality_order_candidates | 17 | 4101 | True | source |  |
| mainline_s7_source_station_catalog | 17 | 3775 | True | source_name |  |
| sideline_climatology_organized_inputs | 7 | 4533 | True | derived_from_nc_paths | organized climatology / annually_climatology input NetCDF paths |
| sideline_climatology_release_nc | 6 | 6 | True | source_name |  |
| sideline_overlap_candidates_sidecar | 16 | 14439196 | True | source |  |
| sideline_satellite_validation_catalog | 3 | 95572 | True | csv_or_netcdf_source_fields | combined satellite validation catalog/nc sources |

## Key counts

- Mainline final selected source datasets: 17
- Any mainline source datasets: 23
- Any sideline source datasets: 23
- Sources in both mainline and sideline: 19
- Mainline-only sources: 4
- Sideline-only sources: 4

## Mainline input sources not selected for final core release

These source datasets entered the mainline processing pipeline (organized inputs, collected stations, clustered stations, quality-order candidates, or release station catalog) but were **not** selected into the final core 16 source datasets.

- Dethier
- GSED
- GloRiSe
- GloRiSe_BS
- GloRiSe_SS
- RiverSed

Count: 6

## Mainline final selected sources

- Bayern
- Chao_Phraya_River
- EUSEDcollab
- Eurasian_River
- Fukushima
- GFQA_v2
- HYBAM
- HYDAT
- Huanghe
- Mekong_Delta
- Myanmar
- NERC
- Rhine
- Robotham
- Shashi_Jianli
- USGS
- Yajiang

## Sideline-only sources

- ALi_De_Boer
- HMA
- Milliman
- Vanmaercke

## Sources in both mainline and sideline

- Bayern
- Chao_Phraya_River
- Dethier
- EUSEDcollab
- Eurasian_River
- Fukushima
- GFQA_v2
- GSED
- HYBAM
- HYDAT
- Huanghe
- Mekong_Delta
- Myanmar
- NERC
- Rhine
- RiverSed
- Robotham
- Shashi_Jianli
- USGS

## Full membership table

| source_dataset | mainline_organized_inputs_daily_monthly_annual | mainline_release_source_dataset_catalog | mainline_release_source_station_catalog | mainline_s3_collected_stations | mainline_s5_clustered_stations | mainline_s6_quality_order_candidates | mainline_s7_source_station_catalog | sideline_climatology_organized_inputs | sideline_climatology_release_nc | sideline_overlap_candidates_sidecar | sideline_satellite_validation_catalog | in_any_mainline | in_any_sideline | mainline_final_selected |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ALi_De_Boer | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| Bayern | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Chao_Phraya_River | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 1 | 0 | 1 | 1 | 1 |
| Dethier | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 1 | 0 |
| EUSEDcollab | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Eurasian_River | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Fukushima | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| GFQA_v2 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 1 |
| GSED | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 1 | 0 |
| GloRiSe | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 |
| GloRiSe_BS | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 |
| GloRiSe_SS | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 |
| HMA | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| HYBAM | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| HYDAT | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Huanghe | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 1 | 1 | 1 |
| Mekong_Delta | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Milliman | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| Myanmar | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| NERC | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Rhine | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| RiverSed | 1 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 1 | 0 |
| Robotham | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Shashi_Jianli | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| USGS | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 1 | 0 | 1 | 1 | 1 |
| Vanmaercke | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| Yajiang | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
