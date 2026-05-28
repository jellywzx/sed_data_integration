#!/usr/bin/env python3
"""Plot regional spatial coverage bars for the ESSD dataset paper.

This script reads table_spatial_coverage_by_region.csv produced by
stats/spatial_coverage_stats.py and writes a stacked horizontal bar chart showing
how final clusters are distributed across continent-region groups.

Default input:
    output_other/spatial_coverage_stats/tables/table_spatial_coverage_by_region.csv

Default outputs:
    output_other/spatial_coverage_stats/figures/fig_spatial_coverage_by_region.png
    output_other/spatial_coverage_stats/figures/fig_spatial_coverage_by_region.pdf
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("matplotlib is required to run this plotting script") from exc


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_SCRIPT_DIR = SCRIPT_DIR.parent
if str(PROJECT_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_SCRIPT_DIR))

try:
    from pipeline_paths import get_output_r_root
except ImportError:
    get_output_r_root = None


def default_stats_root() -> Path:
    """Return the default spatial coverage stats output directory."""
    if get_output_r_root is None:
        return PROJECT_SCRIPT_DIR / "output_other" / "spatial_coverage_stats"
    root = get_output_r_root(PROJECT_SCRIPT_DIR)
    return root / "scripts_basin_test/output_other/spatial_coverage_stats"


DEFAULT_STATS_ROOT = default_stats_root()
DEFAULT_INPUT_TABLE = DEFAULT_STATS_ROOT / "tables" / "table_spatial_coverage_by_region.csv"
DEFAULT_OUTPUT_DIR = DEFAULT_STATS_ROOT / "figures"


COUNT_COLUMNS = {
    "cluster_count": ["cluster_count", "final_cluster_count", "n_clusters"],
    "resolved_cluster_count": [
        "resolved_cluster_count",
        "resolved_basin_assignment_cluster_count",
        "basin_status_resolved_cluster_count",
    ],
    "unresolved_cluster_count": [
        "unresolved_cluster_count",
        "basin_status_unresolved_cluster_count",
    ],
    "other_cluster_count": [
        "other_cluster_count",
        "unknown_or_other_cluster_count",
        "basin_status_unknown_or_other_cluster_count",
    ],
    "basin_polygon_cluster_count": [
        "basin_polygon_cluster_count",
        "clusters_with_basin_polygon",
    ],
}


PLOT_COLORS = {
    "resolved_cluster_count": "#4c78a8",
    "unresolved_cluster_count": "#f58518",
    "other_cluster_count": "#b8b8b8",
}


def first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def numeric_series(df: pd.DataFrame, col: str | None, default: float = 0.0) -> pd.Series:
    if col is None:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").fillna(default)


def clean_text(value, fallback: str = "Unknown") -> str:
    if pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return fallback
    return text


def build_region_label(row: pd.Series) -> str:
    continent = clean_text(row.get("continent", "Unknown"))
    region = clean_text(row.get("region", "Unknown"))
    if region == "Unknown" or region == continent:
        return continent
    return f"{continent}: {region}"


def load_and_prepare_region_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Input table not found: {path}\n"
            "Run stats/spatial_coverage_stats.py first, or pass --input-table."
        )

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Input table is empty: {path}")

    canonical = pd.DataFrame(index=df.index)
    canonical["continent"] = df["continent"] if "continent" in df.columns else "Unknown"
    canonical["region"] = df["region"] if "region" in df.columns else "Unknown"
    canonical["region_label"] = df.apply(build_region_label, axis=1)

    for output_col, candidates in COUNT_COLUMNS.items():
        source_col = first_existing_column(df, candidates)
        canonical[output_col] = numeric_series(df, source_col)

    if first_existing_column(df, COUNT_COLUMNS["cluster_count"]) is None:
        raise ValueError(
            "Could not find a cluster-count column. Expected one of: "
            + ", ".join(COUNT_COLUMNS["cluster_count"])
        )

    inferred_other = (
        canonical["cluster_count"]
        - canonical["resolved_cluster_count"]
        - canonical["unresolved_cluster_count"]
    )
    inferred_other = inferred_other.clip(lower=0)
    if canonical["other_cluster_count"].sum() == 0:
        canonical["other_cluster_count"] = inferred_other

    count_cols = [
        "cluster_count",
        "resolved_cluster_count",
        "unresolved_cluster_count",
        "other_cluster_count",
        "basin_polygon_cluster_count",
    ]
    for col in count_cols:
        canonical[col] = canonical[col].round().astype(int)

    canonical["resolved_fraction"] = np.where(
        canonical["cluster_count"] > 0,
        canonical["resolved_cluster_count"] / canonical["cluster_count"],
        np.nan,
    )
    canonical["polygon_fraction"] = np.where(
        canonical["cluster_count"] > 0,
        canonical["basin_polygon_cluster_count"] / canonical["cluster_count"],
        np.nan,
    )
    return canonical


def select_regions(
    df: pd.DataFrame,
    top_n: int,
    min_clusters: int,
    combine_other: bool,
) -> pd.DataFrame:
    out = df.loc[df["cluster_count"] >= min_clusters].copy()
    out = out.sort_values(["cluster_count", "region_label"], ascending=[False, True])

    if top_n is not None and top_n > 0 and len(out) > top_n:
        top = out.iloc[:top_n].copy()
        rest = out.iloc[top_n:].copy()
        if combine_other and not rest.empty:
            numeric_cols = [
                "cluster_count",
                "resolved_cluster_count",
                "unresolved_cluster_count",
                "other_cluster_count",
                "basin_polygon_cluster_count",
            ]
            other = {col: int(rest[col].sum()) for col in numeric_cols}
            other.update(
                {
                    "continent": "Other",
                    "region": f"{len(rest)} smaller regions",
                    "region_label": f"Other regions (n={len(rest)})",
                }
            )
            other["resolved_fraction"] = (
                other["resolved_cluster_count"] / other["cluster_count"]
                if other["cluster_count"]
                else np.nan
            )
            other["polygon_fraction"] = (
                other["basin_polygon_cluster_count"] / other["cluster_count"]
                if other["cluster_count"]
                else np.nan
            )
            out = pd.concat([top, pd.DataFrame([other])], ignore_index=True)
        else:
            out = top

    if out.empty:
        raise ValueError("No regions remain after applying --min-clusters and --top-n")
    return out


def plot_region_bars(
    df: pd.DataFrame,
    output_paths: list[Path],
    title: str,
    width: float,
    bar_height: float,
    dpi: int,
    annotate_counts: bool,
) -> None:
    plot_df = df.reset_index(drop=True)
    height = max(4.5, 1.5 + bar_height * len(plot_df))
    fig, ax = plt.subplots(figsize=(width, height))

    y = np.arange(len(plot_df))
    left = np.zeros(len(plot_df), dtype="float64")
    stacks = [
        ("resolved_cluster_count", "Resolved basin assignment"),
        ("unresolved_cluster_count", "Unresolved"),
        ("other_cluster_count", "Unknown or other status"),
    ]

    for col, label in stacks:
        values = plot_df[col].to_numpy(dtype="float64")
        ax.barh(
            y,
            values,
            left=left,
            label=label,
            color=PLOT_COLORS[col],
            edgecolor="white",
            linewidth=0.4,
        )
        left += values

    ax.set_yticks(y)
    ax.set_yticklabels(plot_df["region_label"])
    ax.invert_yaxis()
    ax.set_xlabel("Number of final clusters")
    ax.set_title(title)
    ax.grid(axis="x", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.set_axisbelow(True)

    max_count = max(float(plot_df["cluster_count"].max()), 1.0)
    ax.set_xlim(0, max_count * 1.18)

    if annotate_counts:
        pad = max_count * 0.015
        for yi, total, frac in zip(
            y,
            plot_df["cluster_count"],
            plot_df["resolved_fraction"],
        ):
            if np.isfinite(frac):
                label = f"n={int(total)} ({frac * 100:.0f}% resolved)"
            else:
                label = f"n={int(total)}"
            ax.text(total + pad, yi, label, va="center", ha="left", fontsize=8)

    ax.legend(loc="lower right", frameon=False)
    fig.tight_layout()

    for path in output_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot a regional spatial coverage stacked bar chart."
    )
    parser.add_argument(
        "--input-table",
        type=Path,
        default=DEFAULT_INPUT_TABLE,
        help="Path to table_spatial_coverage_by_region.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for output figure files.",
    )
    parser.add_argument(
        "--output-name",
        default="fig_spatial_coverage_by_region",
        help="Output figure basename without extension.",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        default=["png", "pdf"],
        choices=["png", "pdf", "svg"],
        help="Figure formats to write.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Number of largest regions to show. Use 0 or a negative value to show all.",
    )
    parser.add_argument(
        "--min-clusters",
        type=int,
        default=1,
        help="Drop regions with fewer than this many clusters before plotting.",
    )
    parser.add_argument(
        "--no-other-bin",
        action="store_true",
        help="Do not combine regions beyond --top-n into an 'Other regions' bar.",
    )
    parser.add_argument(
        "--no-annotations",
        action="store_true",
        help="Do not annotate bars with total count and resolved percentage.",
    )
    parser.add_argument("--dpi", type=int, default=300, help="Raster output DPI.")
    parser.add_argument("--width", type=float, default=9.5, help="Figure width in inches.")
    parser.add_argument(
        "--bar-height",
        type=float,
        default=0.34,
        help="Height in inches allocated per region bar.",
    )
    parser.add_argument(
        "--title",
        default="Spatial coverage by continent and region",
        help="Figure title.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    top_n = args.top_n if args.top_n > 0 else None

    region_table = load_and_prepare_region_table(args.input_table)
    plot_table = select_regions(
        region_table,
        top_n=top_n,
        min_clusters=args.min_clusters,
        combine_other=not args.no_other_bin,
    )

    output_paths = [args.output_dir / f"{args.output_name}.{fmt}" for fmt in args.formats]
    plot_region_bars(
        plot_table,
        output_paths=output_paths,
        title=args.title,
        width=args.width,
        bar_height=args.bar_height,
        dpi=args.dpi,
        annotate_counts=not args.no_annotations,
    )

    print("Wrote regional spatial coverage figure(s):")
    for path in output_paths:
        print(f"  {path}")
    print(f"Regions shown: {len(plot_table)}")
    print(f"Clusters represented: {int(plot_table['cluster_count'].sum())}")


if __name__ == "__main__":
    main()
