"""Time-window specification and naming utilities.

Time windows define date ranges relative to an index date, specified as
``(lower, upper)`` tuples of integer days. Negative = before index date,
positive = after. Use ``float('inf')`` / ``float('-inf')`` for unbounded.

Window naming follows the R PatientProfiles convention:
    (0, inf)     → "0_to_inf"
    (-inf, -1)   → "minf_to_m1"
    (-365, -1)   → "m365_to_m1"
    (0, 0)       → "0_to_0"
"""

from __future__ import annotations

import math
import re

__all__ = [
    "Window",
    "format_name_style",
    "validate_windows",
    "window_name",
]

# A window is a 2-tuple of (lower, upper) in days relative to index date.
# Use float('inf') / float('-inf') for unbounded.
Window = tuple[float, float]


def validate_windows(
    windows: Window | list[Window],
) -> list[Window]:
    """Validate and normalise window specifications.

    Accepts a single window tuple or a list of windows. Returns a list
    of validated ``(lower, upper)`` tuples.

    Parameters
    ----------
    windows
        A single ``(lower, upper)`` tuple or a list of them.

    Returns
    -------
    list[Window]
        Validated window list.

    Raises
    ------
    ValueError
        If any window has ``lower > upper``.
    """
    if (
        isinstance(windows, tuple)
        and len(windows) == 2
        and not isinstance(windows[0], tuple)
    ):
        # Single window
        windows = [windows]  # type: ignore[assignment]

    result: list[Window] = []
    for w in windows:  # type: ignore[union-attr]
        if len(w) != 2:
            msg = f"Each window must be a (lower, upper) pair, got {w}"
            raise ValueError(msg)
        lo, hi = float(w[0]), float(w[1])
        if lo > hi:
            msg = f"Window lower bound ({lo}) must be <= upper bound ({hi})"
            raise ValueError(msg)
        result.append((lo, hi))
    return result


def window_name(window: Window) -> str:
    """Generate a standardised name for a time window.

    Follows the R convention: negative values prefixed with ``m``,
    infinity as ``inf``.

    Parameters
    ----------
    window
        A ``(lower, upper)`` pair.

    Returns
    -------
    str
        E.g. ``"0_to_inf"``, ``"m365_to_m1"``.

    Examples
    --------
    >>> window_name((0, float('inf')))
    '0_to_inf'
    >>> window_name((-365, -1))
    'm365_to_m1'
    >>> window_name((float('-inf'), float('inf')))
    'minf_to_inf'
    """

    def _fmt(v: float) -> str:
        if math.isinf(v):
            return "minf" if v < 0 else "inf"
        iv = int(v)
        return f"m{abs(iv)}" if iv < 0 else str(iv)

    return f"{_fmt(window[0])}_to_{_fmt(window[1])}"


def format_name_style(
    template: str,
    **replacements: str,
) -> str:
    """Format a name-style template with the given replacements.

    Templates use ``{placeholder}`` syntax. The result is converted to
    snake_case and lowered.

    Parameters
    ----------
    template
        A string like ``"{cohort_name}_{window_name}"``.
    **replacements
        Keyword arguments for each placeholder.

    Returns
    -------
    str
        Formatted, snake_case column name.

    Examples
    --------
    >>> format_name_style("{cohort_name}_{window_name}",
    ...                   cohort_name="My Cohort", window_name="0_to_inf")
    'my_cohort_0_to_inf'
    """
    result = template
    for key, value in replacements.items():
        result = result.replace(f"{{{key}}}", value)
    return _to_snake_case(result)


def _to_snake_case(s: str) -> str:
    """Convert a string to snake_case.

    Handles camelCase, PascalCase, spaces, hyphens, and consecutive
    non-alphanumeric characters.
    """
    # Replace non-alphanumeric with underscore
    s = re.sub(r"[^a-zA-Z0-9]", "_", s)
    # Insert underscore between lower and upper or between upper sequences
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s)
    # Strip leading/trailing underscores
    s = s.strip("_")
    return s.lower()
