# Station-consolidation reproducibility note

This note documents how the station-consolidation implementation relates to the
method description used for the ESSD submission.

## Effective manuscript rule

The manuscript describes source-station consolidation using the same resolved
MERIT-Basins river reach together with a complete-linkage pairwise station
distance threshold of 1000 m.

## Production implementation

The release-producing s5 implementation keeps those criteria and additionally
applies a conservative upstream-area consistency safeguard:

- metric: symmetric relative error in MERIT-Basins upstream area;
- threshold: 0.10;
- implementation: complete-linkage, so every cross-station pair in a proposed
  cluster must satisfy the threshold.

The safeguard is retained in the production code to preserve the exact
release-producing implementation. It should not be silently removed only to
make the source code text shorter or closer to the abbreviated manuscript
description.

## Reproducibility check

Run:

```bash
python validate/s14_validate_upstream_area_merge_equivalence.py
```

The script reruns s5 twice from the same s3/s4 inputs:

1. production baseline: 1000 m distance plus upstream-area error <= 0.10;
2. manuscript-effective comparison: 1000 m distance with the upstream-area
   threshold disabled.

It compares full station membership, rather than only the number of clusters.
When the current s5 output is available, it also requires the production
baseline to reproduce the current station_id -> cluster_id mapping.

The default report path is:

```text
docs/reports/upstream_area_merge_equivalence_v1.0.0.md
```

and any changed station assignments are written to:

```text
validate/output/s14_upstream_area_merge_equivalence/changed_station_assignments.csv
```

A PASS report with zero changed assignments, pairwise Jaccard = 1 and adjusted
Rand index = 1 is the reproducible evidence that the additional 10% safeguard
is non-binding for the supplied release inputs.

## Why this is preferable to deleting the safeguard

Keeping the original safeguard preserves provenance: the checked-in production
implementation remains the implementation used to generate the release.
The explicit equivalence test separately demonstrates whether that extra code
condition affected the published station partition. This makes the relationship
between manuscript wording, release data, and executable code auditable without
silently changing the historical workflow.
