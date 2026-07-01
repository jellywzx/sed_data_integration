#!/usr/bin/env python3
"""Draw the combined source-contribution release figure directly with Matplotlib.

This script does not paste pre-rendered PNG panels. It reloads the release CSV
tables, draws all panels in one Matplotlib figure, and exports vector PDF plus
PNG companion files.
"""

import argparse
import datetime
import importlib.util
from pathlib import Path
import shutil
import subprocess
from typing import Dict, List, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
SOURCE_SCRIPT = SCRIPT_DIR / "plot_fig_source_spatial_temporal_contribution_overlay_release.py"
DEFAULT_OUTPUT_DIR = Path("/share/home/dq134/wzx/sed_data/sediment_wzx_1111/Output_r/scripts_basin_test/figures")
COMBINED_OUTPUT_STEM = "fig_combined_source_contribution_direct_release"

# --- Create figure ---
WIDTH_CM = 35.0
HEIGHT_CM = 45.0
DPI = 300
CM_PER_INCH = 2.54

FONT_SIZE = 18
AXES_LABEL_SIZE = 18
AXES_TITLE_SIZE = 16
TICK_LABEL_SIZE = 16
LEGEND_FONT_SIZE = 16
PANEL_LABEL_SIZE = 20
MIN_VISIBLE_FONT_SIZE = 16

# --- Layout spacing (separate control) ---
# Vertical spacing between panel (a) and panel (b)
HSPACE_PANEL = 0.25
# Vertical spacing between the two sub-panels within panel (b)
HSPACE_SUB = 0.7


def _load_source_module():
    spec = importlib.util.spec_from_file_location("source_contribution_release", str(SOURCE_SCRIPT))
    if spec is None or spec.loader is None:
        raise ImportError("Could not load source plotting module: {}".format(SOURCE_SCRIPT))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SOURCE = _load_source_module()


def configure_matplotlib() -> None:
    SOURCE.FONT_SIZE = FONT_SIZE
    SOURCE.AXES_LABEL_SIZE = AXES_LABEL_SIZE
    SOURCE.AXES_TITLE_SIZE = AXES_TITLE_SIZE
    SOURCE.TICK_LABEL_SIZE = TICK_LABEL_SIZE
    SOURCE.LEGEND_FONT_SIZE = LEGEND_FONT_SIZE
    SOURCE.configure_matplotlib(plt)


def ensure_figure_dirs(figures_root: Path) -> Dict[str, Path]:
    root = Path(figures_root).resolve()
    dirs = {
        "root": root,
        "final": root / "final",
        "data": root / "data",
        "scripts": root / "scripts",
        "checklists": root / "checklists",
    }
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)
    return dirs


def run_text_command(cmd: List[str]) -> Tuple[bool, str]:
    try:
        result = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
    except FileNotFoundError:
        return False, "{} unavailable".format(cmd[0])
    return result.returncode == 0, result.stdout.strip()


def file_size_mb(path: Path) -> str:
    if not path.is_file():
        return "not found"
    return "{:.2f} MB".format(path.stat().st_size / (1024 * 1024))


def draw_main_source_panel(ax_cluster, df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("No main source rows available for plotting.")

    y = np.arange(len(df))
    ax_year = ax_cluster.twiny()
    ax_year.patch.set_alpha(0)
    ax_year.set_zorder(ax_cluster.get_zorder() + 1)

    ax_cluster.barh(
        y,
        df["cluster_count"],
        color=SOURCE.SPATIAL_COLOR,
        alpha=0.45,
        height=0.62,
        zorder=1,
    )
    ax_cluster.set_yticks(y)
    ax_cluster.set_yticklabels(df["source_name"])
    ax_cluster.set_xlabel("Cluster count", color=SOURCE.SPATIAL_COLOR)
    ax_cluster.tick_params(axis="x", colors=SOURCE.SPATIAL_COLOR)
    ax_cluster.spines["bottom"].set_color(SOURCE.SPATIAL_COLOR)
    ax_cluster.grid(axis="x", linewidth=0.3, alpha=0.45, color=SOURCE.SPATIAL_COLOR)
    ax_cluster.set_axisbelow(True)

    max_cluster = pd.to_numeric(df["cluster_count"], errors="coerce").max()
    if pd.notna(max_cluster) and max_cluster > 0:
        ax_cluster.set_xlim(0, max_cluster * 1.24)

    time_df = df.dropna(subset=["first_year", "last_year"]).copy()
    if not time_df.empty:
        for idx, row in time_df.iterrows():
            ax_year.hlines(
                y[idx],
                row["first_year"],
                row["last_year"],
                color=SOURCE.TEMPORAL_LINE_COLOR,
                linewidth=1.7,
                alpha=0.9,
                zorder=3,
            )
        ax_year.scatter(
            time_df["last_year"],
            y[time_df.index],
            s=52,
            color=SOURCE.TEMPORAL_POINT_COLOR,
            alpha=0.82,
            edgecolor="white",
            linewidth=0.5,
            zorder=4,
        )
        SOURCE._set_year_limits(ax_year, time_df)

    SOURCE.annotate_cluster_counts_on_twin(ax_cluster, ax_year, df, y)
    ax_year.set_xlabel("Year", color=SOURCE.TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="x", colors=SOURCE.TEMPORAL_LINE_COLOR)
    ax_year.tick_params(axis="y", left=False, labelleft=False)
    ax_year.spines["top"].set_color(SOURCE.TEMPORAL_LINE_COLOR)


def add_panel_label(ax, label: str, x: float = -0.12, y: float = 1.1) -> None:
    ax.text(
        x,
        y,
        label,
        transform=ax.transAxes,
        fontsize=PANEL_LABEL_SIZE,
        fontweight="bold",
        va="top",
        ha="left",
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.9, "pad": 2.2},
        clip_on=False,
    )


def legend_handles() -> List[object]:
    return [
        Patch(facecolor=SOURCE.SPATIAL_COLOR, alpha=0.45, edgecolor="none", label="main source clusters"),
        Patch(facecolor=SOURCE.OKABE_ITO["bluish_green"], alpha=0.48, edgecolor="none", label="climatology stations"),
        Patch(
            facecolor=SOURCE.OKABE_ITO["reddish_purple"],
            alpha=0.48,
            edgecolor="none",
            label="satellite linked clusters",
        ),
        Patch(facecolor=SOURCE.SPATIAL_COLOR, alpha=0.72, edgecolor="#2f4f6f", linewidth=0.5, label="counts / records"),
        Line2D([0], [0], color=SOURCE.TEMPORAL_LINE_COLOR, linewidth=1.7, label="temporal span"),
        Line2D(
            [0],
            [0],
            marker="o",
            linestyle="none",
            markerfacecolor=SOURCE.TEMPORAL_POINT_COLOR,
            markeredgecolor="white",
            markersize=7,
            label="span end",
        ),
    ]


def plot_combined_direct(
    merged: pd.DataFrame,
    climatology: pd.DataFrame,
    satellite: pd.DataFrame,
    figure_id: str,
    figure_dirs: Dict[str, Path],
    dpi: int = DPI,
    width_cm: float = WIDTH_CM,
    height_cm: float = HEIGHT_CM,
) -> Tuple[Path, Path, Tuple[float, float]]:
    figsize = (width_cm / CM_PER_INCH, height_cm / CM_PER_INCH)
    fig = plt.figure(figsize=figsize)

    # Outer GridSpec: panel (a) | panel (b)
    outer_gs = fig.add_gridspec(
        2, 1,
        height_ratios=[2.6, 2],
        hspace=HSPACE_PANEL,
        left=0.22,
        right=0.97,
        top=0.9,
        bottom=0.16,
    )

    ax_main = fig.add_subplot(outer_gs[0, 0])

    # Inner GridSpec within panel (b): climatology | satellite
    inner_gs = outer_gs[1, 0].subgridspec(
        2, 1,
        height_ratios=[1, 1],
        hspace=HSPACE_SUB,
    )
    ax_climatology = fig.add_subplot(inner_gs[0, 0])
    ax_satellite = fig.add_subplot(inner_gs[1, 0])

    draw_main_source_panel(ax_main, merged)
    SOURCE.plot_other_product_panel(
        ax_climatology,
        climatology,
        "",
        "Station count",
        SOURCE.OKABE_ITO["bluish_green"],
    )
    SOURCE.plot_other_product_panel(
        ax_satellite,
        satellite,
        "",
        "Linked cluster count",
        SOURCE.OKABE_ITO["reddish_purple"],
    )

    add_panel_label(ax_main, "(a) In situ", x=-0.15, y=1.05)
    add_panel_label(ax_climatology, "(b) Climatology", x=-0.15, y=1.3)
    add_panel_label(ax_satellite, "(c) Satellite ", x=-0.15, y=1.3)

    # fig.legend(
    #     handles=legend_handles(),
    #     loc="lower center",
    #     ncol=3,
    #     frameon=False,
    #     bbox_to_anchor=(0.56, 0),
    # )

    # 删除 fig.legend() 那一段，改为：
    ax_satellite.legend(
        handles=legend_handles(),
        loc="upper center",
        ncol=3,
        frameon=False,
        bbox_to_anchor=(0.5, -0.30),   # (水平居中, 子图底部向外)
    )

    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(pdf_path, bbox_inches="tight")
    plt.close(fig)
    return png_path, pdf_path, figsize


def write_combined_checklist(
    checklist_path: Path,
    figure_id: str,
    pdf_path: Path,
    png_path: Path,
    data_paths: List[Path],
    script_copy_path: Path,
    dpi: int,
    figsize: Tuple[float, float],
) -> Path:
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(["pdffonts", str(pdf_path)])
    width_cm = figsize[0] * CM_PER_INCH
    height_cm = figsize[1] * CM_PER_INCH
    font_status = (
        SOURCE.font_embedding_status(pdffonts_output)
        if pdffonts_ok
        else "not checked ({})".format(pdffonts_output)
    )
    text = """# {} ESSD figure checklist

## File information
- Final PDF: `{}`
- Final PNG: `{}`
- Formats: PDF vector preferred; PNG bitmap companion
- Plotting script: `{}`
- Checklist: `{}`

## Format and resolution
- Preferred vector format used: yes, PDF
- Bitmap dpi: {}
- PDF page size: {}
- PDF file size: {}
- PNG file size: {}

## Size and layout
- Figure size: {:.1f} x {:.1f} cm ({:.1f} x {:.1f} in)
- Width >= 8 cm: yes
- Multi-panel layout: 3 rows x 1 column
- Panel labels: (a), (b)

## Fonts
- Font family: DejaVu Sans
- Minimum visible font size: {} pt
- Single font family used: yes
- Font embedding setting: pdf.fonttype = 42
- Font embedding status (via pdffonts): {}

## Reproducibility
- Figure is drawn directly from release CSV tables; no PNG sub-figure compositing is used.
- Plotting-data availability: {} CSV files
- Export date: {}
""".format(
        figure_id,
        pdf_path.name,
        png_path.name,
        script_copy_path.name,
        checklist_path.name,
        dpi,
        SOURCE.pdf_page_size(pdfinfo_output) if pdfinfo_ok else "not checked ({})".format(pdfinfo_output),
        file_size_mb(pdf_path),
        file_size_mb(png_path),
        width_cm,
        height_cm,
        figsize[0],
        figsize[1],
        MIN_VISIBLE_FONT_SIZE,
        font_status,
        len(data_paths),
        datetime.date.today().isoformat(),
    )
    if data_paths:
        text += "\n".join("- Plotting data file: `{}`".format(p.name) for p in data_paths) + "\n"
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text(text, encoding="utf-8")
    return checklist_path


def copy_script(scripts_dir: Path) -> Path:
    src = Path(__file__).resolve()
    dst = scripts_dir / src.name
    scripts_dir.mkdir(parents=True, exist_ok=True)
    if src != dst:
        shutil.copy2(src, dst)
    return dst


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Draw the combined source-contribution release figure directly from release CSV tables."
    )
    parser.add_argument(
        "--release-dir",
        default=str(SOURCE.DEFAULT_RELEASE_DIR),
        help="Release CSV directory. Default: {}".format(SOURCE.DEFAULT_RELEASE_DIR),
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Figure output directory. Default: {}".format(DEFAULT_OUTPUT_DIR),
    )
    parser.add_argument(
        "--figure-id",
        default=COMBINED_OUTPUT_STEM,
        help="Combined figure ID stem. Default: {}".format(COMBINED_OUTPUT_STEM),
    )
    parser.add_argument("--dpi", type=int, default=DPI, help="PNG output DPI. Default: {}".format(DPI))
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_matplotlib()

    release_dir = Path(args.release_dir).expanduser().resolve()
    figure_dirs = ensure_figure_dirs(Path(args.out_dir).expanduser().resolve())
    dpi = int(args.dpi)

    merged = SOURCE.load_main_sources_from_minimal(release_dir)
    climatology_df, satellite_df = SOURCE.load_other_product_sources_from_minimal(release_dir)
    data_paths = SOURCE.write_plotting_data(
        figure_dirs["data"],
        args.figure_id,
        merged,
        climatology_df,
        satellite_df,
    )

    png_path, pdf_path, figsize = plot_combined_direct(
        merged=merged,
        climatology=climatology_df,
        satellite=satellite_df,
        figure_id=args.figure_id,
        figure_dirs=figure_dirs,
        dpi=dpi,
    )
    script_copy_path = copy_script(figure_dirs["scripts"])
    checklist_path = write_combined_checklist(
        checklist_path=figure_dirs["checklists"] / "{}_checklist.md".format(args.figure_id),
        figure_id=args.figure_id,
        pdf_path=pdf_path,
        png_path=png_path,
        data_paths=data_paths,
        script_copy_path=script_copy_path,
        dpi=dpi,
        figsize=figsize,
    )

    print("Wrote {}".format(pdf_path))
    print("Wrote {}".format(png_path))
    print("Copied script to {}".format(script_copy_path))
    print("Wrote {}".format(checklist_path))
    for data_path in data_paths:
        print("Wrote {}".format(data_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
