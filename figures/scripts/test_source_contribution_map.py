#!/usr/bin/env python3
"""Plot in-situ/climatology and satellite source-dataset station locations.

Command-line usage examples:

    python plot_source_map_insitu_clim_sat.py
    python plot_source_map_insitu_clim_sat.py --legend-user-order
    python plot_source_map_insitu_clim_sat.py --release-dir output/sed_reference_release_minimal --figures-root figures
    python plot_source_map_insitu_clim_sat.py --top-n 10 --dpi 300 --legend-user-order
    python plot_source_map_insitu_clim_sat.py --help

Use --legend-user-order to draw legend text in the user-provided source order;
omitting it preserves the default legend ordering.
"""

from __future__ import annotations

import argparse
import ctypes
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

CONDA_LIB = "/share/home/dq134/.conda/envs/wzx/lib"
if os.path.isdir(CONDA_LIB):
    os.environ["LD_LIBRARY_PATH"] = CONDA_LIB + os.pathsep + os.environ.get("LD_LIBRARY_PATH", "")
    try:
        ctypes.CDLL(str(Path(CONDA_LIB) / "libstdc++.so.6"), mode=ctypes.RTLD_GLOBAL)
    except Exception:
        pass

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import netCDF4 as nc4
import numpy as np
import pandas as pd

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    HAS_CARTOPY = True
except Exception:
    ccrs = None
    cfeature = None
    HAS_CARTOPY = False

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent.parent
DEFAULT_RELEASE_DIR = PROJECT_DIR / "output" / "sed_reference_release_minimal"
DEFAULT_FIGURES_ROOT = PROJECT_DIR / "figures"
OUTPUT_STEM = "source_map_insitu_clim_sat"
FIGSIZE = (12.5, 12.8)

FONT_SIZE_TITLE = 15
FONT_SIZE_AXIS_LABEL = 13
FONT_SIZE_LEGEND = 11
FONT_SIZE_PANEL_LABEL = 18

SOURCE_STATION_CSV = "source_station_catalog.csv"
SOURCE_DATASET_CSV = "source_dataset_catalog.csv"
SATELLITE_CSV = "satellite_catalog.csv"
CLIMATOLOGY_NC = "sed_reference_climatology.nc"

UNKNOWN_DATASET_LABEL = "unknown dataset"
SATELLITE_DATASETS = {"Dethier", "GSED", "RiverSed (USA)"}
SATELLITE_SOURCE_ORDER = ["RiverSed", "GSED", "Dethier"]
MIN_LAT = -60  # southern extent bound, excluding Antarctica

SOURCE_NAME_ALIASES = {
    "ALi_De_Boer": "Ali and De Boer",
    "HMA": "HMA",
    "Milliman": "Milliman",
    "Vanmaercke": "Vanmaercke",
    "RiverSed": "RivSed",
    "USGS": "USGS NWIS",
    "Eurasian_River": "Eurasian River",
    "GloRiSe": "GloRiSe",
    "Huanghe": "Huanghe",
    "Myanmar": "Myanmar Rivers",
    "NERC": "NERC Avon",
    "Yajiang": "Yajiang",
    "Chao_Phraya_River": "Chao Phraya",
    "Mekong_Delta": "Mekong Delta",
}

CLIMATOLOGY_SOURCE_NAMES = {
    "Milliman",
    "Milliman & Farnsworth",
    "HMA",
    "High Mountain Asia (HMA)",
    "Ali and De Boer",
    "Ali & De Boer (Upper Indus)",
    "Vanmaercke",
    "Vanmaercke et al.",
}

OKABE_ITO = {
    "orange": "#E69F00",
    "sky_blue": "#56B4E9",
    "bluish_green": "#009E73",
    "yellow": "#F0E442",
    "blue": "#0072B2",
    "vermillion": "#D55E00",
    "reddish_purple": "#CC79A7",
    "black": "#000000",
}

OKABE_ITO_EXTENDED = [
    OKABE_ITO["orange"],
    OKABE_ITO["sky_blue"],
    OKABE_ITO["bluish_green"],
    OKABE_ITO["yellow"],
    OKABE_ITO["blue"],
    OKABE_ITO["vermillion"],
    OKABE_ITO["reddish_purple"],
    "#882E72",  # dark purple (replaces black)
    "#A6761D",  # amber/brown (Wong 2011)
    "#66A61E",  # lime green  (Wong 2011)
]

TOP_MARKERS = ["o", "^", "s", "D", "P", "X", "*", "p", "h", "<"]

SATELLITE_COLORS = {
    "RiverSed": OKABE_ITO["blue"],
    "GSED": OKABE_ITO["vermillion"],
    "Dethier": OKABE_ITO["bluish_green"],
}

SATELLITE_MARKERS = {
    "RiverSed": "o",
    "GSED": "^",
    "Dethier": "s",
}

PREFERRED_LEGEND_LABEL_ORDER = [
    "GloRiSe v1.1",
    "GFQA_v2",
    "USGS NWIS",
    "HYDAT",
    "Bayern",
    "Eurasian River",
    "EUSEDcollab",
    "HYBAM",
    "Rhine",
    "Mekong Delta",
    "Myanmar Rivers",
    "Yajiang / Yarlung Tsangpo",
    "Chao Phraya River",
    "Robotham",
    "NERC-Hampshire Avon",
    "Fukushima",
    "Shashi_Jianli",
    "Huanghe (Yellow River)",
    "Milliman & Farnsworth",
    "High Mountain Asia (HMA)",
    "Ali & De Boer (Upper Indus)",
    "Vanmaercke et al.",
]


# ---------------------------------------------------------------------------
# ESSD helper functions
# ---------------------------------------------------------------------------

def configure_matplotlib(plt) -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
            "svg.fonttype": "none",
        }
    )


def ensure_figure_dirs(figures_root: Path) -> dict[str, Path]:
    root = Path(figures_root).resolve()
    dirs = {
        "root": root,
        "final": root / "final",
        "data": root / "data",
        "scripts": root / "scripts",
        "checklists": root / "checklists",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def run_text_command(cmd: list[str]) -> tuple[bool, str]:
    try:
        result = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    except FileNotFoundError:
        return False, "{} unavailable".format(cmd[0])
    return result.returncode == 0, result.stdout.strip()


def pdf_page_size(pdfinfo_output: str) -> str:
    for line in pdfinfo_output.splitlines():
        if line.startswith("Page size:"):
            return line.split(":", 1)[1].strip()
    return "not found in pdfinfo output"


def font_embedding_status(pdffonts_output: str) -> str:
    lines = pdffonts_output.splitlines()
    if len(lines) < 3:
        return "no fonts reported by pdffonts"
    header = lines[0]
    if "emb" not in header or "sub" not in header:
        return "checked with pdffonts; review raw output"
    emb_start = header.index("emb")
    sub_start = header.index("sub")
    values = [line[emb_start:sub_start].strip().lower() for line in lines[2:] if line.strip()]
    if values and all(value == "yes" for value in values):
        return "all reported fonts embedded"
    if values:
        return "some reported fonts may not be embedded; review pdffonts output"
    return "no fonts reported by pdffonts"


def write_checklist(
    checklist_path: Path,
    figure_id: str,
    pdf_path: Path,
    png_path: Path,
    data_paths: list[Path],
    script_copy_path: Path,
    dpi: int,
    figsize: tuple[float, float],
) -> Path:
    pdfinfo_ok, pdfinfo_output = run_text_command(["pdfinfo", str(pdf_path)])
    pdffonts_ok, pdffonts_output = run_text_command(["pdffonts", str(pdf_path)])
    width_cm = figsize[0] * 2.54
    height_cm = figsize[1] * 2.54
    lines = [
        "# {} ESSD figure checklist".format(figure_id),
        "",
        "- Final PDF: `{}`".format(pdf_path.name),
        "- Final PNG: `{}`".format(png_path.name),
        "- Formats: PDF vector preferred; PNG bitmap companion",
        "- PNG dpi: {}".format(dpi),
        "- Intended size: {:.1f} x {:.1f} cm ({:.1f} x {:.1f} in)".format(width_cm, height_cm, figsize[0], figsize[1]),
        "- PDF page size: {}".format(pdf_page_size(pdfinfo_output) if pdfinfo_ok else "not checked ({})".format(pdfinfo_output)),
        "- Width >= 8 cm: yes",
        "- Font family: DejaVu Sans",
        "- Font consistency: one sans-serif family set in Matplotlib rcParams",
        "- Font embedding status: {}".format(font_embedding_status(pdffonts_output) if pdffonts_ok else "not checked ({})".format(pdffonts_output)),
        "- PDF font check command: `pdffonts {}`".format(pdf_path),
        "- PDF size check command: `pdfinfo {}`".format(pdf_path),
        "- Colorblind-safe status: Okabe-Ito extended + marker shapes + dark edges for dual encoding",
        "- Coblis/equivalent review: requires manual Coblis/equivalent review after export",
        "- Legend completeness: colors, marker shapes, dark edges, transparency, and station counts explained",
        "- Panel labels: `(a)` in-situ/climatology source datasets; `(b)` satellite source datasets",
        "- Units and ranges: station counts use comma-separated integers; lat/lon in degrees",
        "- Map projection: Robinson (global map)",
        "- Data filtering: panel (a) excludes satellite sources; panel (b) includes RiverSed, GSED, and Dethier satellite sources",
        "- Plotting script: `{}`".format(script_copy_path.name),
        "- Plotting-data availability: {} CSV files".format(len(data_paths)),
    ]
    lines.extend("- Plotting data file: `{}`".format(path.name) for path in data_paths)
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return checklist_path


def _write_csv(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def write_plotting_data(
    data_dir: Path,
    figure_id: str,
    points: pd.DataFrame,
    catalog: pd.DataFrame,
    top_sources: List[str],
    category_counts: Dict[str, int],
    satellite_points: pd.DataFrame,
    satellite_counts: Dict[str, int],
) -> List[Path]:
    outputs = [
        _write_csv(points[["lat", "lon", "source_name", "category", "input_file"]],
                    data_dir / "{}_panel_a_source_points.csv".format(figure_id)),
        _write_csv(catalog[catalog["source_name"].isin(top_sources)],
                    data_dir / "{}_panel_a_top_sources.csv".format(figure_id)),
        _write_csv(pd.DataFrame(list(category_counts.items()), columns=["category", "n_stations"]),
                    data_dir / "{}_panel_a_category_counts.csv".format(figure_id)),
        _write_csv(satellite_points[["lat", "lon", "source", "input_file"]],
                    data_dir / "{}_panel_b_satellite_points.csv".format(figure_id)),
        _write_csv(pd.DataFrame(list(satellite_counts.items()), columns=["source", "n_stations"]),
                    data_dir / "{}_panel_b_satellite_source_counts.csv".format(figure_id)),
    ]
    return outputs


# ---------------------------------------------------------------------------
# Data loading and processing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot global source-station distributions for in-situ/climatology and satellite source datasets."
    )
    parser.add_argument("--release-dir", type=Path, default=DEFAULT_RELEASE_DIR)
    parser.add_argument("--figures-root", type=Path, default=DEFAULT_FIGURES_ROOT,
                        help="ESSD figure root with final/data/scripts/checklists subdirs.")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="Legacy output directory (optional); ESSD outputs go to --figures-root.")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument(
        "--legend-user-order",
        action="store_true",
        help="Order legend entries by the user-provided source label order.",
    )
    parser.add_argument(
        "--datasets", type=str, default=None,
        help="Comma-separated dataset source names for single-panel plot "
             "(e.g. 'HYBAM,Rhine'). If omitted, all datasets are shown.",
    )
    args = parser.parse_args()
    if args.datasets is not None:
        args.datasets = [name.strip() for name in args.datasets.split(",") if name.strip()]
    return args


def clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, np.ma.MaskedArray):
        if value.size == 0 or bool(np.ma.getmaskarray(value).all()):
            return ""
        value = value.filled("")
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            value = value.item()
        elif value.dtype.kind in {"S", "U"}:
            value = b"".join(value.astype("S").tolist()).decode("utf-8", errors="ignore")
        else:
            value = value.tolist()
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", errors="ignore")
    text = str(value).strip()
    if text.startswith("b'") and text.endswith("'"):
        text = text[2:-1]
    if text.startswith('b"') and text.endswith('"'):
        text = text[2:-1]
    return "" if text.lower() in {"nan", "none", "null", "<na>"} else text


def require_file(path: Path) -> Path:
    if not path.is_file():
        raise FileNotFoundError("Required input not found: {}".format(path))
    return path


def valid_latlon(frame: pd.DataFrame) -> pd.Series:
    lat = pd.to_numeric(frame["lat"], errors="coerce")
    lon = pd.to_numeric(frame["lon"], errors="coerce")
    return lat.between(-90, 90) & lon.between(-180, 180)


def canonical_source_name(name: object, catalog_names: Iterable[str]) -> str:
    text = clean_text(name)
    if not text:
        return ""
    if text in SOURCE_NAME_ALIASES:
        return SOURCE_NAME_ALIASES[text]

    catalog_set = set(catalog_names)
    if text in catalog_set:
        return text

    compact = text.replace("_", " ").strip()
    if compact in catalog_set:
        return compact
    return text


def read_dataset_catalog(release_dir: Path) -> pd.DataFrame:
    path = require_file(release_dir / SOURCE_DATASET_CSV)
    catalog = pd.read_csv(path)
    required = {"source_name", "n_source_stations"}
    missing = sorted(required.difference(catalog.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(path, ", ".join(missing)))
    catalog["source_name"] = catalog["source_name"].map(clean_text)
    catalog["n_source_stations"] = pd.to_numeric(catalog["n_source_stations"], errors="coerce").fillna(0).astype(int)
    return catalog[catalog["source_name"].ne("")].copy()


def read_source_station_catalog(release_dir: Path, catalog_names: Iterable[str]) -> pd.DataFrame:
    path = require_file(release_dir / SOURCE_STATION_CSV)
    frame = pd.read_csv(path)
    required = {"source_name", "source_station_lat", "source_station_lon"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(path, ", ".join(missing)))

    out = pd.DataFrame(
        {
            "source_name": frame["source_name"].map(lambda x: canonical_source_name(x, catalog_names)),
            "lat": pd.to_numeric(frame["source_station_lat"], errors="coerce"),
            "lon": pd.to_numeric(frame["source_station_lon"], errors="coerce"),
            "input_file": SOURCE_STATION_CSV,
        }
    )
    return out[out["source_name"].ne("")].copy()


def read_satellite_catalog(release_dir: Path) -> pd.DataFrame:
    path = require_file(release_dir / SATELLITE_CSV)
    frame = pd.read_csv(path, usecols=lambda col: col in {"source", "lat", "lon"})
    required = {"source", "lat", "lon"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError("{} is missing columns: {}".format(path, ", ".join(missing)))

    out = pd.DataFrame(
        {
            "source": frame["source"].map(clean_text),
            "lat": pd.to_numeric(frame["lat"], errors="coerce"),
            "lon": pd.to_numeric(frame["lon"], errors="coerce"),
            "input_file": SATELLITE_CSV,
        }
    )
    out = out[out["source"].isin(SATELLITE_SOURCE_ORDER)].copy()
    out = out[valid_latlon(out)].copy()
    return out.reset_index(drop=True)


def read_nc_source_points(release_dir: Path, file_name: str, catalog_names: Iterable[str]) -> pd.DataFrame:
    path = require_file(release_dir / file_name)
    with nc4.Dataset(str(path), "r") as ds:
        for var_name in ["lat", "lon"]:
            if var_name not in ds.variables:
                raise ValueError("{} is missing variable: {}".format(path, var_name))

        lat = np.asarray(ds.variables["lat"][:], dtype="float64")
        lon = np.asarray(ds.variables["lon"][:], dtype="float64")

        # Support two NC layouts:
        # (A) source_index (int indices) + source_name (string array) — legacy format
        # (B) source (string per record, same length as lat/lon) — climatology NC format
        if "source_index" in ds.variables and "source_name" in ds.variables:
            source_index = np.asarray(ds.variables["source_index"][:], dtype="float64")
            source_names_raw = [canonical_source_name(value, catalog_names) for value in ds.variables["source_name"][:]]
            if not (len(lat) == len(lon) == len(source_index)):
                raise ValueError("{} has inconsistent lat/lon/source_index lengths".format(path))
            valid_source = np.isfinite(source_index) & (source_index >= 0) & (source_index < len(source_names_raw))
            source_labels = np.array([""] * len(source_index), dtype=object)
            source_labels[valid_source] = [source_names_raw[int(idx)] for idx in source_index[valid_source]]
        elif "source" in ds.variables:
            src_var = ds.variables["source"]
            if src_var.shape == lat.shape:
                source_labels = np.array([canonical_source_name(value, catalog_names) for value in src_var[:]], dtype=object)
            else:
                raise ValueError("{} source variable shape {} does not match lat shape {}".format(
                    path, src_var.shape, lat.shape))
        else:
            raise ValueError("{} has neither (source_index+source_name) nor source variable".format(path))

    out = pd.DataFrame(
        {
            "source_name": source_labels,
            "lat": lat,
            "lon": lon,
            "input_file": file_name,
        }
    )
    return out[out["source_name"].ne("")].copy()


def load_all_points(release_dir: Path, catalog: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    catalog_names = catalog["source_name"].tolist()
    frames = [
        read_source_station_catalog(release_dir, catalog_names),
        read_nc_source_points(release_dir, CLIMATOLOGY_NC, catalog_names),
        # Satellite NC intentionally excluded — only in-situ data
    ]
    input_counts = pd.concat(frames, ignore_index=True).groupby("input_file").size()
    points = pd.concat(frames, ignore_index=True)
    points = points[valid_latlon(points)].copy()
    points["lat"] = pd.to_numeric(points["lat"], errors="coerce")
    points["lon"] = pd.to_numeric(points["lon"], errors="coerce")
    return points.reset_index(drop=True), input_counts


def select_top_sources(catalog: pd.DataFrame, top_n: int, exclude_names: set[str] = None) -> List[str]:
    if top_n <= 0:
        raise ValueError("--top-n must be positive")
    filtered = catalog.copy()
    if exclude_names:
        filtered = filtered[~filtered["source_name"].isin(exclude_names)]
    return (
        filtered.sort_values(["n_source_stations", "source_name"], ascending=[False, True])
        .head(top_n)["source_name"]
        .tolist()
    )


def add_categories(points: pd.DataFrame, top_sources: List[str]) -> pd.DataFrame:
    out = points.copy()
    top_set = set(top_sources)
    out["category"] = out["source_name"].map(lambda value: clean_text(value) or UNKNOWN_DATASET_LABEL)
    out.loc[out["category"].isin(top_set), "category"] = out.loc[out["category"].isin(top_set), "source_name"]
    return out


def validate_counts(points: pd.DataFrame, catalog: pd.DataFrame) -> pd.DataFrame:
    extracted = points.groupby("source_name").size().rename("extracted_n").reset_index()
    check = catalog[["source_name", "n_source_stations"]].merge(extracted, on="source_name", how="outer")
    check["n_source_stations"] = check["n_source_stations"].fillna(0).astype(int)
    check["extracted_n"] = check["extracted_n"].fillna(0).astype(int)
    check["diff"] = check["extracted_n"] - check["n_source_stations"]
    return check.sort_values(["diff", "source_name"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def setup_cartopy_axis(ax) -> None:
    ax.set_extent([-180, 180, MIN_LAT, 90], crs=ccrs.PlateCarree())
    ax.set_facecolor("#f7fbff")
    try:
        ax.add_feature(cfeature.LAND, facecolor="#f2f2f2", edgecolor="none", zorder=0)
        ax.add_feature(cfeature.OCEAN, facecolor="#f7fbff", edgecolor="none", zorder=0)
        ax.add_feature(cfeature.COASTLINE, linewidth=0.35, edgecolor="#777777", zorder=1)
        ax.add_feature(cfeature.BORDERS, linewidth=0.18, edgecolor="#aaaaaa", zorder=1)
    except Exception as exc:
        print("Warning: failed to draw cartopy features: {}".format(exc))
    gridliner = ax.gridlines(
        crs=ccrs.PlateCarree(),
        draw_labels=False,
        linewidth=0.25,
        color="#bbbbbb",
        alpha=0.45,
        linestyle="-",
    )
    gridliner.xlocator = plt.FixedLocator(np.arange(-180, 181, 60))
    gridliner.ylocator = plt.FixedLocator(np.arange(-60, 61, 30))


def setup_plain_axis(ax) -> None:
    ax.set_xlim(-180, 180)
    ax.set_ylim(MIN_LAT, 90)
    ax.set_facecolor("#f7fbff")
    ax.grid(True, color="#bbbbbb", linewidth=0.25, alpha=0.45)
    ax.set_xlabel("Longitude", fontsize=FONT_SIZE_AXIS_LABEL)
    ax.set_ylabel("Latitude", fontsize=FONT_SIZE_AXIS_LABEL)


def scatter_points(
    ax,
    frame: pd.DataFrame,
    color: str,
    label: str,
    size: float,
    alpha: float,
    zorder: int,
    marker: str = "o",
    edgecolor: str = "#333333",
    linewidth: float = 0.3,
) -> None:
    kwargs = {
        "s": size,
        "c": color,
        "label": label,
        "alpha": alpha,
        "linewidths": linewidth,
        "edgecolors": edgecolor,
        "marker": marker,
        "rasterized": True,
        "zorder": zorder,
    }
    if HAS_CARTOPY and hasattr(ax, "projection"):
        ax.scatter(frame["lon"], frame["lat"], transform=ccrs.PlateCarree(), **kwargs)
    else:
        ax.scatter(frame["lon"], frame["lat"], **kwargs)


def category_style(index: int) -> tuple[str, str]:
    color_count = len(OKABE_ITO_EXTENDED)
    marker_count = len(TOP_MARKERS)
    color = OKABE_ITO_EXTENDED[index % color_count]
    marker = TOP_MARKERS[(index + index // color_count) % marker_count]
    return color, marker



def order_legend_entries(
    handles: List[Line2D], labels: List[str], use_user_order: bool
) -> Tuple[List[Line2D], List[str]]:
    if not use_user_order:
        return handles, labels

    preferred_order = {label: index for index, label in enumerate(PREFERRED_LEGEND_LABEL_ORDER)}
    entries = []
    seen = set()
    for original_index, (handle, label) in enumerate(zip(handles, labels)):
        clean_label = label.strip()
        if clean_label in seen:
            continue
        seen.add(clean_label)
        entries.append((preferred_order.get(clean_label, len(preferred_order)), original_index, handle, label))

    entries.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in entries], [item[3] for item in entries]


def add_panel_label(ax, label: str) -> None:
    ax.text(
        0.015,
        0.965,
        label,
        transform=ax.transAxes,
        fontsize=FONT_SIZE_PANEL_LABEL,
        fontweight="bold",
        va="top",
        ha="left",
        bbox=dict(boxstyle="round,pad=0.18", facecolor="white", edgecolor="none", alpha=0.85),
        zorder=10,
    )


def draw_top_sources_panel(
    ax,
    points: pd.DataFrame,
    top_sources: List[str],
    counts: Dict[str, int],
    legend_user_order: bool = False,
) -> None:
    if HAS_CARTOPY and hasattr(ax, "projection"):
        setup_cartopy_axis(ax)
    else:
        setup_plain_axis(ax)

    top_set = set(top_sources)
    other_sources = sorted(category for category in counts if category not in top_set)
    category_order = top_sources + other_sources

    def legend_label(label: str) -> str:
        return "{} ({:,})".format(label, counts.get(label, 0))

    legend_order = sorted(
        category_order,
        key=lambda label: (len(legend_label(label)), legend_label(label).casefold()),
    )
    style_map = {category: category_style(i) for i, category in enumerate(category_order)}

    for idx, category in enumerate(category_order):
        subset = points[points["category"].eq(category)]
        if subset.empty:
            continue
        color, marker = style_map[category]
        scatter_points(
            ax,
            subset,
            color,
            category,
            size=8,
            alpha=0.72,
            zorder=3 + idx,
            marker=marker,
        )

    handles = []
    handle_labels = []
    for label in legend_order:
        if counts.get(label, 0) <= 0:
            continue
        color, marker = style_map[label]
        handles.append(
            Line2D(
                [0],
                [0],
                marker=marker,
                color="none",
                markerfacecolor=color,
                markeredgecolor="#333333",
                markeredgewidth=0.3,
                markersize=6,
                label="{} ({:,})".format(label, counts[label]),
            )
        )
        handle_labels.append(label)
    handles, _ = order_legend_entries(handles, handle_labels, legend_user_order)

    # Unified legend
    leg = ax.legend(
        handles=handles,
        loc="lower left",
        bbox_to_anchor=(0.06, -0.01),
        ncol=1,
        frameon=False,
        fontsize=FONT_SIZE_LEGEND,
        columnspacing=1.2,
        handletextpad=0.35,
    )
    leg._legend_box.align = "left"
    

def draw_satellite_panel(ax, satellite_points: pd.DataFrame, counts: Dict[str, int]) -> None:
    if HAS_CARTOPY and hasattr(ax, "projection"):
        setup_cartopy_axis(ax)
    else:
        setup_plain_axis(ax)

    for idx, source in enumerate(SATELLITE_SOURCE_ORDER):
        subset = satellite_points[satellite_points["source"].eq(source)]
        if subset.empty:
            continue
        size = 4 if source == "RiverSed" else 8
        alpha = 0.25 if source == "RiverSed" else 0.55
        scatter_points(
            ax,
            subset,
            SATELLITE_COLORS[source],
            source,
            size=size,
            alpha=alpha,
            zorder=3 + idx,
            marker=SATELLITE_MARKERS[source],
        )

    handles = []
    for source in SATELLITE_SOURCE_ORDER:
        if counts.get(source, 0) <= 0:
            continue
        handles.append(
            Line2D(
                [0],
                [0],
                marker=SATELLITE_MARKERS[source],
                color="none",
                markerfacecolor=SATELLITE_COLORS[source],
                markeredgecolor="#333333",
                markeredgewidth=0.3,
                markersize=6,
                label="{} ({:,})".format(SOURCE_NAME_ALIASES.get(source, source), counts[source]),
            )
        )
    leg_sat = ax.legend(
        handles=handles,
        loc="lower left",
        bbox_to_anchor=(0.06, 0.02),
        ncol=1,
        frameon=False,
        fontsize=FONT_SIZE_LEGEND,
        columnspacing=1.2,
        handletextpad=0.35,
        title="Satellite",
        title_fontsize=FONT_SIZE_LEGEND,
    )
    leg_sat.get_title().set_fontweight("bold")
    leg_sat._legend_box.align = "left"


def plot_map(
    points: pd.DataFrame,
    top_sources: List[str],
    counts: Dict[str, int],
    satellite_points: pd.DataFrame,
    satellite_counts: Dict[str, int],
    figure_id: str,
    figure_dirs: dict,
    dpi: int,
    legend_user_order: bool = False,
) -> List[Path]:
    figure_dirs["final"].mkdir(parents=True, exist_ok=True)
    if HAS_CARTOPY:
        fig, ax = plt.subplots(
            1,
            1,
            figsize=FIGSIZE,
            subplot_kw={"projection": ccrs.Robinson()},
        )
    else:
        fig, ax = plt.subplots(1, 1, figsize=FIGSIZE)

    draw_top_sources_panel(ax, points, top_sources, counts, legend_user_order)

    fig.subplots_adjust(left=0.02, right=0.98, top=0.97, bottom=0.04)

    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    fig.savefig(pdf_path, dpi=dpi, bbox_inches="tight")
    fig.savefig(png_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return [pdf_path, png_path]


def print_summary(
    catalog: pd.DataFrame,
    points: pd.DataFrame,
    input_counts: pd.Series,
    top_sources: List[str],
    satellite_counts: Dict[str, int],
) -> None:
    category_counts = points.groupby("category").size().to_dict()
    catalog_counts = catalog.set_index("source_name")["n_source_stations"].to_dict()
    top_set = set(top_sources)

    print("Input station rows:")
    for name, count in input_counts.sort_index().items():
        print("  {}: {:,}".format(name, int(count)))

    print("\nTop source datasets by n_source_stations (in-situ only):")
    for source in top_sources:
        print("  {}: {:,}".format(source, int(catalog_counts.get(source, 0))))
    other_counts = {name: count for name, count in category_counts.items() if name not in top_set}
    if other_counts:
        print("\nOther in-situ source datasets (shown separately):")
        for source, count in sorted(other_counts.items()):
            print("  {}: {:,}".format(source, int(count)))
    print("\nFinal valid lat/lon points: {:,}".format(len(points)))
    print("\nPanel (a) excludes satellite sources.")
    print("\nPanel (b) satellite source datasets:")
    for source in SATELLITE_SOURCE_ORDER:
        print("  {}: {:,}".format(source, int(satellite_counts.get(source, 0))))


# ---------------------------------------------------------------------------
# ESSD figure orchestration
# ---------------------------------------------------------------------------

def create_figure(release_dir: Path, figures_root: Path, top_n: int, dpi: int,
                  legend_user_order: bool = False, datasets: list = None) -> dict[str, object]:
    configure_matplotlib(plt)
    figure_dirs = ensure_figure_dirs(figures_root)

    catalog = read_dataset_catalog(release_dir)
    points, input_counts = load_all_points(release_dir, catalog)
    satellite_points = read_satellite_catalog(release_dir)
    top_sources = select_top_sources(catalog, top_n, exclude_names=SATELLITE_DATASETS)
    points = add_categories(points, top_sources)

    # Filter by --datasets if specified
    if datasets:
        points = points[points["source_name"].isin(datasets)].copy()
        points["category"] = points["source_name"]
        top_sources = [d for d in datasets if d in points["source_name"].unique()]
        if not top_sources:
            print("Warning: none of the requested datasets found in points: {}".format(datasets))
            print("Available source names: {}".format(
                sorted(points["source_name"].unique()) if len(points) > 0 else "none"))

    # Determine output filename
    if datasets:
        safe_names = "_".join(d.replace(" ", "_").replace("/", "_").replace("&", "and") for d in top_sources)
        figure_id = "source_map_{}".format(safe_names)
    else:
        figure_id = OUTPUT_STEM

    pdf_path = figure_dirs["final"] / "{}.pdf".format(figure_id)
    png_path = figure_dirs["final"] / "{}.png".format(figure_id)
    script_copy_path = figure_dirs["scripts"] / "plot_{}.py".format(figure_id)
    checklist_path = figure_dirs["checklists"] / "{}_checklist.md".format(figure_id)

    count_check = validate_counts(points, catalog)
    mismatches = count_check[count_check["diff"].ne(0)]
    if mismatches.empty:
        print("Source station count check: OK")
    else:
        print("Warning: source station count mismatches:")
        print(mismatches.to_string(index=False))

    category_counts = points.groupby("category").size().to_dict()
    satellite_counts = {
        source: int((satellite_points["source"] == source).sum())
        for source in SATELLITE_SOURCE_ORDER
    }
    print_summary(catalog, points, input_counts, top_sources, satellite_counts)

    outputs = plot_map(
        points,
        top_sources,
        category_counts,
        satellite_points,
        satellite_counts,
        figure_id,
        figure_dirs,
        dpi,
        legend_user_order,
    )

    # In --datasets mode, skip writing CSV data files, script copy, and checklist
    if not datasets:
        data_paths = write_plotting_data(
            figure_dirs["data"],
            figure_id,
            points,
            catalog,
            top_sources,
            category_counts,
            satellite_points,
            satellite_counts,
        )
        script_src = Path(__file__).resolve()
        if script_src != script_copy_path:
            shutil.copy2(script_src, script_copy_path)
        write_checklist(checklist_path, figure_id, pdf_path, png_path, data_paths, script_copy_path, dpi, FIGSIZE)
    else:
        data_paths = []

    result = {
        "figure_id": figure_id,
        "pdf_path": pdf_path,
        "png_path": png_path,
        "data_paths": data_paths,
    }
    if not datasets:
        result["script_copy_path"] = script_copy_path
        result["checklist_path"] = checklist_path
    return result


def main() -> int:
    args = parse_args()
    release_dir = args.release_dir.resolve()
    figures_root = args.figures_root.resolve()

    outputs = create_figure(release_dir, figures_root, args.top_n, args.dpi,
                            args.legend_user_order, datasets=args.datasets)

    print("\nWrote ESSD-compliant figure outputs:")
    print("  {}".format(outputs["pdf_path"]))
    print("  {}".format(outputs["png_path"]))
    if outputs.get("data_paths"):
        for path in outputs["data_paths"]:
            print("  {}".format(path))
    if outputs.get("script_copy_path"):
        print("  {}".format(outputs["script_copy_path"]))
    if outputs.get("checklist_path"):
        print("  {}".format(outputs["checklist_path"]))

    # Optional legacy output
    if args.out_dir is not None:
        legacy_dir = args.out_dir.resolve()
        legacy_dir.mkdir(parents=True, exist_ok=True)
        stem = OUTPUT_STEM
        for ext in (".pdf", ".png"):
            src = figures_root / "final" / "{}{}".format(stem, ext)
            dst = legacy_dir / "{}{}".format(stem, ext)
            if src.is_file():
                shutil.copy2(str(src), str(dst))
                print("  (legacy copy: {})".format(dst))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
