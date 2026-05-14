#!/usr/bin/env python3
"""
Plot presentation-ready figures for s10 validation results.

This version uses absolute paths only.

Required input directory:
    ABS_VALIDATION_DIR/
        validation_overlap_source_pairs.csv
        validation_overlap_pair_records.csv

Optional input file:
        validation_overlap_source_pairs_by_variable.csv

Output directory:
    ABS_OUT_DIR/
        01_n_pairs_by_variable.png
        02_mape_by_variable.png
        03_spearman_by_variable.png
        04_bias_by_variable.png
        05_rmse_by_variable.png
        10_scatter_Q.png
        10_scatter_SSC.png
        10_scatter_SSL.png
        20_bias_distribution_Q.png
        20_bias_distribution_SSC.png
        20_bias_distribution_SSL.png
        30_relative_error_distribution_Q.png
        30_relative_error_distribution_SSC.png
        30_relative_error_distribution_SSL.png
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# 只需要修改这里：必须写绝对路径
# ============================================================

ABS_VALIDATION_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/validation_results"
)

ABS_OUT_DIR = Path(
    "/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/output/validation_results/figures_presentation"
)

# 如果你的结果在 scripts_basin_test 里面，可以改成类似：
#
# ABS_VALIDATION_DIR = Path(
#     "/Users/你的用户名/你的项目路径/scripts_basin_test/output/validation_results"
# )
#
# ABS_OUT_DIR = Path(
#     "/Users/你的用户名/你的项目路径/scripts_basin_test/output/validation_results/figures_presentation"
# )


VARIABLE_ORDER = {
    "Q": 0,
    "SSC": 1,
    "SSL": 2,
}


def clean_column_name(name: str) -> str:
    """
    Convert column names to lower snake_case.

    Example:
        "Source Pair" -> "source_pair"
        "source-b value" -> "source_b_value"
    """
    name = str(name).strip()
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def read_csv_if_exists(path: Path) -> pd.DataFrame | None:
    """Read CSV file if it exists."""
    if not path.exists():
        print(f"[WARN] Missing file: {path}")
        return None

    df = pd.read_csv(path)
    df.columns = [clean_column_name(c) for c in df.columns]

    print(f"[OK] Loaded {path.name}: {len(df):,} rows")
    print(f"     Columns: {list(df.columns)}")

    return df


def require_columns(df: pd.DataFrame, columns: list[str], table_name: str) -> None:
    """Raise a clear error if required columns are missing."""
    missing = [c for c in columns if c not in df.columns]

    if missing:
        raise ValueError(
            f"{table_name} is missing required columns: {missing}\n"
            f"Available columns are:\n{list(df.columns)}"
        )


def get_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column name that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def prepare_summary_table(summary: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare source-pair summary table for plotting.

    Prefer rows where resolution == "all" if such rows exist.
    """
    require_columns(summary, ["variable"], "validation_overlap_source_pairs.csv")

    df = summary.copy()

    if "resolution" in df.columns:
        mask_all = df["resolution"].astype(str).str.lower().eq("all")
        if mask_all.any():
            df = df.loc[mask_all].copy()

    if "source_pair" in df.columns:
        df["plot_label"] = (
            df["source_pair"].astype(str) + " / " + df["variable"].astype(str)
        )
    elif "family_pair" in df.columns:
        df["plot_label"] = (
            df["family_pair"].astype(str) + " / " + df["variable"].astype(str)
        )
    else:
        df["plot_label"] = df["variable"].astype(str)

    df["_var_order"] = df["variable"].astype(str).map(VARIABLE_ORDER).fillna(999)
    df = df.sort_values(["_var_order", "plot_label"]).reset_index(drop=True)

    return df


def save_bar_plot(
    df: pd.DataFrame,
    metric: str,
    title: str,
    ylabel: str,
    out_path: Path,
    value_format: str | None = None,
) -> None:
    """Save a simple bar chart for one summary metric."""
    if metric not in df.columns:
        print(f"[WARN] Skip {metric}: column not found")
        return

    plot_df = df.dropna(subset=[metric]).copy()

    if plot_df.empty:
        print(f"[WARN] Skip {metric}: no non-null values")
        return

    x = np.arange(len(plot_df))
    y = pd.to_numeric(plot_df[metric], errors="coerce")

    fig, ax = plt.subplots(figsize=(9, 5))

    ax.bar(x, y)
    ax.set_xticks(x)
    ax.set_xticklabels(plot_df["plot_label"], rotation=30, ha="right")
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.3)

    for i, val in enumerate(y):
        if pd.notna(val):
            if value_format:
                label = value_format.format(val)
            else:
                label = f"{val:.3g}"

            ax.text(
                i,
                val,
                label,
                ha="center",
                va="bottom",
                fontsize=9,
            )

    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)

    print(f"[OK] Saved {out_path}")


def save_summary_plots(summary: pd.DataFrame, out_dir: Path) -> None:
    """Save high-level summary figures from validation_overlap_source_pairs.csv."""
    df = prepare_summary_table(summary)

    save_bar_plot(
        df=df,
        metric="n_pairs",
        title="Number of paired HYDAT-USGS observations by variable",
        ylabel="Number of paired observations",
        out_path=out_dir / "01_n_pairs_by_variable.png",
        value_format="{:.0f}",
    )

    save_bar_plot(
        df=df,
        metric="mape",
        title="Mean absolute percentage error by variable",
        ylabel="MAPE",
        out_path=out_dir / "02_mape_by_variable.png",
        value_format="{:.3f}",
    )

    save_bar_plot(
        df=df,
        metric="spearman",
        title="Spearman rank correlation by variable",
        ylabel="Spearman correlation",
        out_path=out_dir / "03_spearman_by_variable.png",
        value_format="{:.6f}",
    )

    save_bar_plot(
        df=df,
        metric="bias",
        title="Mean bias by variable",
        ylabel="Bias: source_b - source_a",
        out_path=out_dir / "04_bias_by_variable.png",
        value_format="{:.3g}",
    )

    save_bar_plot(
        df=df,
        metric="rmse",
        title="RMSE by variable",
        ylabel="RMSE",
        out_path=out_dir / "05_rmse_by_variable.png",
        value_format="{:.3g}",
    )


def find_value_columns(records: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    Try to identify source A and source B value columns in pair-record table.

    Different versions of s10 may use slightly different names, so this function
    checks several possible column names.
    """
    value_a_candidates = [
        "value_a",
        "source_a_value",
        "a_value",
        "x_value",
        "source_1_value",
        "value_1",
        "left_value",
        "candidate_value_a",
        "value_source_a",
        "source_a_q",
        "source_a_ssc",
        "source_a_ssl",
    ]

    value_b_candidates = [
        "value_b",
        "source_b_value",
        "b_value",
        "y_value",
        "source_2_value",
        "value_2",
        "right_value",
        "candidate_value_b",
        "value_source_b",
        "source_b_q",
        "source_b_ssc",
        "source_b_ssl",
    ]

    value_a = get_first_existing_column(records, value_a_candidates)
    value_b = get_first_existing_column(records, value_b_candidates)

    return value_a, value_b


def find_existing_error_columns(records: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    Try to find existing bias / relative error columns if value_a/value_b are absent.
    """
    bias_candidates = [
        "bias",
        "diff",
        "difference",
        "value_diff",
        "source_b_minus_source_a",
        "delta",
    ]

    relative_error_candidates = [
        "relative_error",
        "rel_error",
        "absolute_percentage_error",
        "ape",
        "mape_component",
    ]

    bias_col = get_first_existing_column(records, bias_candidates)
    relative_error_col = get_first_existing_column(records, relative_error_candidates)

    return bias_col, relative_error_col


def add_derived_error_columns(
    records: pd.DataFrame,
    value_a: str,
    value_b: str,
) -> pd.DataFrame:
    """Add bias, absolute error, and relative error columns."""
    df = records.copy()

    df[value_a] = pd.to_numeric(df[value_a], errors="coerce")
    df[value_b] = pd.to_numeric(df[value_b], errors="coerce")

    df["bias_value"] = df[value_b] - df[value_a]
    df["absolute_error"] = df["bias_value"].abs()

    denom = df[value_a].abs()
    df["relative_error"] = np.where(
        denom > 0,
        df["absolute_error"] / denom,
        np.nan,
    )

    return df


def should_use_log_scale(x: pd.Series, y: pd.Series) -> bool:
    """
    Decide whether log scale is helpful.

    Log scale is useful when values are positive and strongly right-skewed,
    which often happens for Q and SSL.
    """
    values = pd.concat([x, y]).dropna()
    values = values[values > 0]

    if len(values) < 10:
        return False

    q05 = values.quantile(0.05)
    q95 = values.quantile(0.95)

    if q05 <= 0:
        return False

    return (q95 / q05) > 100


def add_metric_text_box(
    ax: plt.Axes,
    variable_name: str,
    summary_df: pd.DataFrame,
) -> None:
    """Add n, MAPE, and Spearman metrics to scatter figure."""
    metric_row = summary_df[
        summary_df["variable"].astype(str).eq(variable_name)
    ]

    if metric_row.empty:
        return

    row = metric_row.iloc[0]
    text_lines = []

    if "n_pairs" in row and pd.notna(row["n_pairs"]):
        text_lines.append(f"n = {row['n_pairs']:.0f}")

    if "mape" in row and pd.notna(row["mape"]):
        text_lines.append(f"MAPE = {row['mape']:.3f}")

    if "spearman" in row and pd.notna(row["spearman"]):
        text_lines.append(f"Spearman = {row['spearman']:.6f}")

    if not text_lines:
        return

    ax.text(
        0.05,
        0.95,
        "\n".join(text_lines),
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=10,
        bbox={
            "boxstyle": "round",
            "alpha": 0.15,
        },
    )


def save_scatter_plots(
    records: pd.DataFrame,
    summary: pd.DataFrame,
    out_dir: Path,
) -> None:
    """
    Save scatter, bias distribution, and relative error figures from pair records.
    """
    require_columns(records, ["variable"], "validation_overlap_pair_records.csv")

    value_a, value_b = find_value_columns(records)
    summary_df = prepare_summary_table(summary)

    if value_a is None or value_b is None:
        print("[WARN] Could not identify value_a/value_b columns.")
        print("[WARN] Scatter plots will be skipped.")
        print("[WARN] Available columns:")
        print(list(records.columns))

        save_error_distribution_plots_without_values(records, out_dir)
        return

    records = add_derived_error_columns(records, value_a, value_b)

    for variable, group in records.groupby("variable"):
        variable_name = str(variable)

        plot_df = group[
            [value_a, value_b, "bias_value", "relative_error"]
        ].dropna(subset=[value_a, value_b])

        if plot_df.empty:
            print(f"[WARN] Skip {variable_name}: no valid paired values")
            continue

        # Downsample for readability and speed if the table is very large.
        if len(plot_df) > 50_000:
            plot_df = plot_df.sample(50_000, random_state=42)

        x = plot_df[value_a]
        y = plot_df[value_b]

        min_val = np.nanmin([x.min(), y.min()])
        max_val = np.nanmax([x.max(), y.max()])

        # ------------------------------------------------------------
        # Scatter plot
        # ------------------------------------------------------------
        fig, ax = plt.subplots(figsize=(6, 6))

        ax.scatter(
            x,
            y,
            s=10,
            alpha=0.35,
        )

        ax.plot(
            [min_val, max_val],
            [min_val, max_val],
            linestyle="--",
            linewidth=1,
        )

        ax.set_xlabel("Source A value")
        ax.set_ylabel("Source B value")
        ax.set_title(f"{variable_name}: paired source comparison")
        ax.grid(alpha=0.3)

        if should_use_log_scale(x, y):
            positive_mask = (x > 0) & (y > 0)

            if positive_mask.any():
                ax.set_xscale("log")
                ax.set_yscale("log")

        add_metric_text_box(ax, variable_name, summary_df)

        fig.tight_layout()

        out_path = out_dir / f"10_scatter_{variable_name}.png"
        fig.savefig(out_path, dpi=300)
        plt.close(fig)

        print(f"[OK] Saved {out_path}")

        # ------------------------------------------------------------
        # Bias distribution boxplot
        # ------------------------------------------------------------
        bias_values = plot_df["bias_value"].replace(
            [np.inf, -np.inf],
            np.nan,
        ).dropna()

        if not bias_values.empty:
            fig, ax = plt.subplots(figsize=(7, 5))

            ax.boxplot(
                bias_values,
                vert=True,
                showfliers=False,
            )

            ax.axhline(
                0,
                linestyle="--",
                linewidth=1,
            )

            ax.set_xticks([1])
            ax.set_xticklabels([variable_name])
            ax.set_ylabel("Bias: source_b - source_a")
            ax.set_title(f"{variable_name}: bias distribution")
            ax.grid(axis="y", alpha=0.3)

            fig.tight_layout()

            out_path = out_dir / f"20_bias_distribution_{variable_name}.png"
            fig.savefig(out_path, dpi=300)
            plt.close(fig)

            print(f"[OK] Saved {out_path}")

        # ------------------------------------------------------------
        # Relative error histogram
        # ------------------------------------------------------------
        rel = plot_df["relative_error"].replace(
            [np.inf, -np.inf],
            np.nan,
        ).dropna()

        if not rel.empty:
            # Clip only for visualization so extreme outliers do not hide
            # the main distribution.
            upper = rel.quantile(0.99)
            rel_plot = rel[rel <= upper]

            fig, ax = plt.subplots(figsize=(7, 5))

            ax.hist(
                rel_plot,
                bins=40,
            )

            ax.set_xlabel("Relative error")
            ax.set_ylabel("Count")
            ax.set_title(f"{variable_name}: relative error distribution")
            ax.grid(axis="y", alpha=0.3)

            fig.tight_layout()

            out_path = out_dir / f"30_relative_error_distribution_{variable_name}.png"
            fig.savefig(out_path, dpi=300)
            plt.close(fig)

            print(f"[OK] Saved {out_path}")


def save_error_distribution_plots_without_values(
    records: pd.DataFrame,
    out_dir: Path,
) -> None:
    """
    Fallback plotting when value_a/value_b columns cannot be found.

    This uses existing bias or relative error columns if they exist.
    """
    bias_col, relative_error_col = find_existing_error_columns(records)

    if bias_col is None and relative_error_col is None:
        print("[WARN] No value columns and no error columns found.")
        print("[WARN] Pair-record distribution plots were skipped.")
        return

    for variable, group in records.groupby("variable"):
        variable_name = str(variable)

        if bias_col is not None:
            bias_values = pd.to_numeric(
                group[bias_col],
                errors="coerce",
            ).replace([np.inf, -np.inf], np.nan).dropna()

            if not bias_values.empty:
                fig, ax = plt.subplots(figsize=(7, 5))

                ax.boxplot(
                    bias_values,
                    vert=True,
                    showfliers=False,
                )

                ax.axhline(
                    0,
                    linestyle="--",
                    linewidth=1,
                )

                ax.set_xticks([1])
                ax.set_xticklabels([variable_name])
                ax.set_ylabel("Bias")
                ax.set_title(f"{variable_name}: bias distribution")
                ax.grid(axis="y", alpha=0.3)

                fig.tight_layout()

                out_path = out_dir / f"20_bias_distribution_{variable_name}.png"
                fig.savefig(out_path, dpi=300)
                plt.close(fig)

                print(f"[OK] Saved {out_path}")

        if relative_error_col is not None:
            rel = pd.to_numeric(
                group[relative_error_col],
                errors="coerce",
            ).replace([np.inf, -np.inf], np.nan).dropna()

            if not rel.empty:
                upper = rel.quantile(0.99)
                rel_plot = rel[rel <= upper]

                fig, ax = plt.subplots(figsize=(7, 5))

                ax.hist(
                    rel_plot,
                    bins=40,
                )

                ax.set_xlabel("Relative error")
                ax.set_ylabel("Count")
                ax.set_title(f"{variable_name}: relative error distribution")
                ax.grid(axis="y", alpha=0.3)

                fig.tight_layout()

                out_path = out_dir / f"30_relative_error_distribution_{variable_name}.png"
                fig.savefig(out_path, dpi=300)
                plt.close(fig)

                print(f"[OK] Saved {out_path}")


def validate_absolute_paths() -> None:
    """Validate that both configured paths are absolute."""
    if not ABS_VALIDATION_DIR.is_absolute():
        raise ValueError(
            f"ABS_VALIDATION_DIR must be an absolute path:\n{ABS_VALIDATION_DIR}"
        )

    if not ABS_OUT_DIR.is_absolute():
        raise ValueError(
            f"ABS_OUT_DIR must be an absolute path:\n{ABS_OUT_DIR}"
        )

    if not ABS_VALIDATION_DIR.exists():
        raise FileNotFoundError(
            f"Validation directory does not exist:\n{ABS_VALIDATION_DIR}"
        )


def main() -> None:
    """
    Absolute-path version.

    No command-line relative paths are used.
    """
    validate_absolute_paths()

    ABS_OUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary_path = ABS_VALIDATION_DIR / "validation_overlap_source_pairs.csv"
    records_path = ABS_VALIDATION_DIR / "validation_overlap_pair_records.csv"
    by_variable_path = ABS_VALIDATION_DIR / "validation_overlap_source_pairs_by_variable.csv"

    print("=" * 80)
    print("s10 validation plotting")
    print("=" * 80)
    print(f"Validation directory: {ABS_VALIDATION_DIR}")
    print(f"Output figure directory: {ABS_OUT_DIR}")
    print("=" * 80)

    summary = read_csv_if_exists(summary_path)
    records = read_csv_if_exists(records_path)

    # This file is optional. The current script does not require it,
    # but loading it here helps verify that it exists.
    _by_variable = read_csv_if_exists(by_variable_path)

    if summary is None:
        raise FileNotFoundError(
            "Cannot find validation_overlap_source_pairs.csv at:\n"
            f"{summary_path}"
        )

    save_summary_plots(
        summary=summary,
        out_dir=ABS_OUT_DIR,
    )

    if records is not None and len(records) > 0:
        save_scatter_plots(
            records=records,
            summary=summary,
            out_dir=ABS_OUT_DIR,
        )
    else:
        print("[WARN] validation_overlap_pair_records.csv is missing or empty.")
        print("[WARN] Scatter and distribution plots were skipped.")

    print("\nDone.")
    print(f"Figures saved to:\n{ABS_OUT_DIR}")


if __name__ == "__main__":
    main()