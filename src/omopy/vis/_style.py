"""Style configuration and text formatting for vis output.

Provides :func:`customise_text` for styling text strings, and
style dataclasses for configuring table/plot appearance.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

__all__ = [
    "PlotStyle",
    "TableStyle",
    "customise_text",
    "default_plot_style",
    "default_table_style",
]


# ── Text customisation ────────────────────────────────────────────────────


def customise_text(
    x: str | list[str],
    *,
    fun: Callable[[str], str] | None = None,
    custom: dict[str, str] | None = None,
    keep: list[str] | None = None,
) -> str | list[str]:
    """Style text strings for display.

    Default transformation: replace underscores with spaces and apply
    sentence case.

    Args:
        x: String or list of strings to style.
        fun: Custom transformation function. If ``None``, uses the
            default snake_case-to-sentence-case transform.
        custom: Dict of exact replacements (``{old: new}``).
        keep: Values to keep unchanged.

    Returns:
        Styled string(s), same type as input.

    Examples:
        >>> customise_text("cohort_name")
        'Cohort name'
        >>> customise_text(["age_group", "sex"])
        ['Age group', 'Sex']
        >>> customise_text("cdm_name", custom={"cdm_name": "Database"})
        'Database'
    """
    if fun is None:
        fun = _default_text_transform

    keep_set = set(keep) if keep else set()
    custom_map = custom or {}

    if isinstance(x, str):
        return _apply_text_transform(x, fun, custom_map, keep_set)
    else:
        return [_apply_text_transform(s, fun, custom_map, keep_set) for s in x]


def _default_text_transform(s: str) -> str:
    """Default text transform: snake_case -> Sentence case."""
    # Replace underscores with spaces
    result = s.replace("_", " ")
    # Sentence case (capitalize first letter, lowercase rest)
    if result:
        result = result[0].upper() + result[1:]
    return result


def _apply_text_transform(
    s: str,
    fun: Callable[[str], str],
    custom: dict[str, str],
    keep: set[str],
) -> str:
    """Apply text transformation with custom/keep overrides."""
    if s in keep:
        return s
    if s in custom:
        return custom[s]
    return fun(s)


# ── Table style ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TableStyle:
    """Configuration for table appearance.

    Attributes:
        title_align: Alignment for the table title (``"left"``, ``"center"``, ``"right"``).
        title_color: Colour for the title text (CSS colour string).
        header_background: Background colour for column headers.
        header_color: Text colour for column headers.
        header_align: Alignment for column headers.
        body_align: Default alignment for body cells.
        group_background: Background colour for group label rows.
        group_color: Text colour for group label rows.
        stripe: Whether to apply row striping.
        stripe_color: Background colour for striped rows.
        na_display: String to display for ``None``/missing values.
        font_family: Font family for the table.
        font_size: Base font size in pixels.
    """

    title_align: str = "left"
    title_color: str = "#333333"
    header_background: str = "#4361ee"
    header_color: str = "#ffffff"
    header_align: str = "center"
    body_align: str = "right"
    group_background: str = "#e8eaf6"
    group_color: str = "#333333"
    stripe: bool = True
    stripe_color: str = "#f5f5f5"
    na_display: str = "\u2013"  # en-dash
    font_family: str = "system-ui, -apple-system, sans-serif"
    font_size: int = 14


@dataclass(frozen=True)
class PlotStyle:
    """Configuration for plot appearance.

    Attributes:
        color_palette: List of hex colour strings for data series.
        background_color: Plot background colour.
        text_color: Default text colour.
        grid_color: Gridline colour.
        font_family: Font family.
        font_size: Base font size in points.
        title_size: Title font size in points.
        show_legend: Whether to show the legend by default.
    """

    color_palette: list[str] = field(
        default_factory=lambda: [
            "#4361ee",
            "#3a86ff",
            "#8338ec",
            "#ff006e",
            "#fb5607",
            "#ffbe0b",
            "#06d6a0",
            "#118ab2",
            "#073b4c",
            "#ef476f",
        ]
    )
    background_color: str = "#ffffff"
    text_color: str = "#333333"
    grid_color: str = "#e0e0e0"
    font_family: str = "system-ui, -apple-system, sans-serif"
    font_size: int = 12
    title_size: int = 16
    show_legend: bool = True


def default_table_style() -> TableStyle:
    """Return the default table style."""
    return TableStyle()


def default_plot_style() -> PlotStyle:
    """Return the default plot style."""
    return PlotStyle()
