#!/usr/bin/env python3
"""Markdown reporting helpers for release-only statistics modules."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_PATH_MARKERS = ("/share/home/dq134", "/share/home/", "/home/", "/Users/")


def sanitize_text(value: object) -> str:
    """Return text safe for Markdown reports without exposing host-local roots."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    text = str(value)
    project = PROJECT_ROOT.as_posix()
    if project in text:
        text = text.replace(project, ".")
    text = text.replace("/share/home/dq134", "$WORKSPACE")
    for marker in LOCAL_PATH_MARKERS[1:]:
        text = text.replace(marker, "$LOCAL_HOME/")
    return text.strip()


def display_path(path: object, *, relative_to: Optional[Path] = None) -> str:
    """Format a path for reports, preferring project-relative paths."""
    if path is None:
        return ""
    text = sanitize_text(path)
    if not text:
        return ""
    try:
        p = Path(str(path)).expanduser()
    except Exception:
        return text
    if not p.is_absolute():
        return sanitize_text(p.as_posix())
    bases = []
    if relative_to is not None:
        bases.append(Path(relative_to))
    bases.append(PROJECT_ROOT)
    for base in bases:
        try:
            return p.resolve().relative_to(base.resolve()).as_posix()
        except Exception:
            continue
    return sanitize_text(p.as_posix())


def safe_lines(lines: Iterable[object]) -> list[str]:
    return [sanitize_text(line) for line in lines]


def md_escape(value: object) -> str:
    return sanitize_text(value).replace("|", r"\|").replace("\n", " ")


def as_number(value: object) -> float:
    try:
        out = float(value)
    except Exception:
        return float("nan")
    return out if math.isfinite(out) else float("nan")


def fmt_int(value: object) -> str:
    num = as_number(value)
    if not math.isfinite(num):
        return ""
    return "{:,}".format(int(round(num)))


def fmt_float(value: object, digits: int = 2) -> str:
    num = as_number(value)
    if not math.isfinite(num):
        return ""
    if abs(num - round(num)) < 1e-9:
        return "{:,}".format(int(round(num)))
    return ("{:,.%df}" % digits).format(num)


def fmt_pct(value: object, digits: int = 2) -> str:
    text = fmt_float(value, digits=digits)
    return "{}%".format(text) if text else ""


def fmt_cell(value: object, column: str = "") -> str:
    if isinstance(value, (np.integer, int)):
        return fmt_int(value)
    if isinstance(value, (np.floating, float)):
        if not math.isfinite(float(value)):
            return ""
        if any(key in column.lower() for key in ("pct", "percent", "rate", "share")):
            return fmt_pct(value)
        if abs(float(value)) >= 1000 or abs(float(value) - round(float(value))) < 1e-9:
            return fmt_int(value)
        return fmt_float(value)
    return md_escape(value)


def markdown_table(
    frame: pd.DataFrame,
    columns: Optional[Sequence[str]] = None,
    headers: Optional[Mapping[str, str] | Sequence[str]] = None,
    *,
    max_rows: int = 12,
) -> str:
    """Render a compact Markdown table from a DataFrame."""
    if frame is None or frame.empty:
        return "_No rows._"
    cols = list(columns or frame.columns)
    cols = [col for col in cols if col in frame.columns]
    if not cols:
        return "_No matching columns._"
    work = frame.loc[:, cols].head(max_rows).copy()
    if isinstance(headers, Mapping):
        labels = [headers.get(col, col) for col in cols]
    elif headers is not None:
        labels = list(headers)[: len(cols)]
    else:
        labels = [col.replace("_", " ") for col in cols]
    lines = [
        "| {} |".format(" | ".join(md_escape(label) for label in labels)),
        "|{}|".format("|".join("---" for _ in labels)),
    ]
    for _, row in work.iterrows():
        lines.append("| {} |".format(" | ".join(fmt_cell(row.get(col, ""), col) for col in cols)))
    if len(frame) > len(work):
        lines.append("")
        lines.append("_Showing first {:,} of {:,} rows._".format(len(work), len(frame)))
    return "\n".join(lines)


def sorted_markdown_table(
    frame: pd.DataFrame,
    columns: Optional[Sequence[str]] = None,
    headers: Optional[Mapping[str, str] | Sequence[str]] = None,
    *,
    sort_by: Optional[str] = None,
    ascending: bool = False,
    max_rows: int = 12,
) -> str:
    if frame is None or frame.empty:
        return "_No rows._"
    work = frame.copy()
    if sort_by and sort_by in work.columns:
        work[sort_by] = pd.to_numeric(work[sort_by], errors="ignore")
        work = work.sort_values(sort_by, ascending=ascending)
    return markdown_table(work, columns=columns, headers=headers, max_rows=max_rows)


def metric_value(frame: pd.DataFrame, metric: str, default: object = "") -> object:
    if frame is None or frame.empty or "metric" not in frame.columns or "value" not in frame.columns:
        return default
    hit = frame[frame["metric"].astype(str).eq(str(metric))]
    if hit.empty:
        return default
    return hit.iloc[0]["value"]


def append_table_section(
    lines: list[str],
    title: str,
    frame: pd.DataFrame,
    columns: Optional[Sequence[str]] = None,
    headers: Optional[Mapping[str, str] | Sequence[str]] = None,
    *,
    sort_by: Optional[str] = None,
    ascending: bool = False,
    max_rows: int = 12,
    note: str = "",
) -> None:
    lines.extend(["", "## {}".format(title), ""])
    if note:
        lines.extend([sanitize_text(note), ""])
    lines.append(
        sorted_markdown_table(
            frame,
            columns=columns,
            headers=headers,
            sort_by=sort_by,
            ascending=ascending,
            max_rows=max_rows,
        )
    )


def append_figure_index(lines: list[str], figures_dir: Path, report_dir: Path, *, max_items: int = 16) -> None:
    figures = sorted(Path(figures_dir).glob("*.png")) if Path(figures_dir).is_dir() else []
    lines.extend(["", "## Figures", ""])
    if not figures:
        lines.append("_No figures were generated for this module._")
        return
    for path in figures[:max_items]:
        rel = display_path(path, relative_to=report_dir)
        lines.append("- `{}`: `{}`".format(path.name, rel))
    if len(figures) > max_items:
        lines.append("- Additional figures: {:,}".format(len(figures) - max_items))


def issue_label(count: object) -> str:
    num = as_number(count)
    if not math.isfinite(num) or num <= 0:
        return "OK"
    return "Review"


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    try:
        if Path(path).is_file():
            return pd.read_csv(path, keep_default_na=False)
    except Exception:
        pass
    return pd.DataFrame()
