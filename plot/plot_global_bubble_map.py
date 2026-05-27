#!/usr/bin/env python3
"""Generate a global bubble map of sediment cluster coverage.

Uses table_cluster_spatial_attributes.csv output from spatial_coverage_stats.py.

Map features:
  - x/y: longitude / latitude
  - point size: sqrt(upstream basin area) — larger = bigger contributing area
  - point color: basin resolution status (resolved / unresolved / unknown)
  - world coastline background from Natural Earth 110m
"""

import json
import sys
import urllib.request
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

COASTLINE_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/"
    "geojson/ne_110m_coastline.geojson"
)

# Colours for basin_status categories
STATUS_COLORS = {
    "resolved":   "#2196F3",   # blue
    "unresolved": "#FF9800",   # orange
    "unknown":    "#9E9E9E",   # grey
}

# ── helpers ──────────────────────────────────────────────────────────


def _load_coastlines(cache_dir: Path):
    """Load Natural Earth coastline GeoJSON (cached locally)."""
    cache_path = cache_dir / "ne_110m_coastline.geojson"
    if not cache_path.is_file():
        print("Downloading world coastline data (110 m) …")
        urllib.request.urlretrieve(COASTLINE_URL, cache_path)
    with open(cache_path, encoding="utf-8") as fh:
        return json.load(fh)


def _draw_coastlines(ax, coastlines):
    """Draw coastline lines from Natural-Earth GeoJSON onto *ax*."""
    for feat in coastlines["features"]:
        geom = feat["geometry"]
        if geom["type"] == "MultiLineString":
            segs = geom["coordinates"]
        else:  # LineString
            segs = [geom["coordinates"]]

        for seg in segs:
            xs, ys = zip(*seg)
            ax.plot(xs, ys, color="#555555", linewidth=0.5, zorder=1)


def _stats_text(n_total, n_resolved, n_unresolved):
    """Return a formatted multi-line stats string."""
    n_unknown = n_total - n_resolved - n_unresolved
    lines = [
        f"Total clusters:  {n_total}",
        f"Resolved:        {n_resolved}",
        f"Unresolved:     {n_unresolved}",
    ]
    if n_unknown > 0:
        lines.append(f"Unknown:         {n_unknown}")
    return "\n".join(lines)


# ── main ─────────────────────────────────────────────────────────────


def main():
    root = Path(
        "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/"
        "scripts_basin_test/output_other/spatial_coverage_stats"
    )
    csv_path = root / "tables" / "table_cluster_spatial_attributes.csv"
    out_dir  = root / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── read data ────────────────────────────────────────────────
    df = pd.read_csv(csv_path)
    df = df[df["lat"].notna() & df["lon"].notna()].copy()

    has_area = df["area_km2"].notna() & (df["area_km2"] > 0)

    # ── coastline background ─────────────────────────────────────
    cache_dir = Path.home() / ".cache" / "plot_global_bubble_map"
    cache_dir.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(14, 7))

    try:
        coastlines = _load_coastlines(cache_dir)
        _draw_coastlines(ax, coastlines)
    except Exception as exc:
        print(f"Coastline background unavailable — using grid only: {exc}",
              file=sys.stderr)

    # ── point size mapping ───────────────────────────────────────
    # Marker area (s) ∝ sqrt(area) so the visual circle area is
    # proportional to contributing catchment size.
    sizes = np.where(
        has_area,
        np.sqrt(np.clip(df["area_km2"], 10, None)) * 0.18,
        4.0,  # fixed tiny dot for unresolved / unknown clusters
    )

    # ── render per status group (needed for legend) ──────────────
    for status in ["resolved", "unresolved", "unknown"]:
        mask = df["basin_status"] == status
        if not mask.any():
            continue
        ax.scatter(
            df.loc[mask, "lon"],
            df.loc[mask, "lat"],
            s=sizes[mask.values],
            c=STATUS_COLORS[status],
            alpha=0.5,
            label=status,
            edgecolors="none",
            zorder=2,
        )

    # ── map decoration ───────────────────────────────────────────
    ax.set_xlim(-180, 180)
    ax.set_ylim(-60, 85)
    ax.set_xlabel("Longitude", fontsize=11)
    ax.set_ylabel("Latitude",  fontsize=11)
    ax.set_title(
        "Global distribution of sediment clusters\n"
        "Point size ∝ √(upstream basin area)",
        fontsize=13, fontweight="bold",
    )
    ax.grid(True, linewidth=0.3, alpha=0.35, zorder=0)

    n_total = len(df)
    n_resolved = int((df["basin_status"] == "resolved").sum())
    n_unresolved = int((df["basin_status"] == "unresolved").sum())

    ax.legend(
        loc="lower left", markerscale=1.8, framealpha=0.85,
        title="Basin status",
    )
    ax.text(
        0.98, 0.02,
        _stats_text(n_total, n_resolved, n_unresolved),
        transform=ax.transAxes, fontsize=10,
        verticalalignment="bottom", horizontalalignment="right",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.85),
    )

    fig.tight_layout()
    out_path = out_dir / "fig_global_bubble_map.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved: {out_path}")
    print(f"  Points plotted: {n_total}")
    print(f"  Resolved:   {n_resolved}")
    print(f"  Unresolved: {n_unresolved}")
    print(f"  Unknown:    {n_total - n_resolved - n_unresolved}")


if __name__ == "__main__":
    sys.exit(main())
