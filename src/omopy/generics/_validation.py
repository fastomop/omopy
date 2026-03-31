"""Assertion / validation helpers.

Mirrors R's omopgenerics ``assertCharacter()``, ``assertClass()``, etc.
Each function raises ``TypeError`` or ``ValueError`` on failure.
"""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any

__all__ = [
    "assert_character",
    "assert_choice",
    "assert_class",
    "assert_date",
    "assert_list",
    "assert_logical",
    "assert_numeric",
    "assert_table_columns",
    "assert_true",
]


def assert_character(
    value: Any,
    *,
    name: str = "value",
    min_length: int | None = None,
    max_length: int | None = None,
    na_allowed: bool = True,
    null_allowed: bool = False,
) -> None:
    """Assert *value* is a string or sequence of strings."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)

    items: Sequence[str | None]
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, Sequence):
        items = value
    else:
        msg = (
            f"`{name}` must be a string or sequence of"
            f" strings, got {type(value).__name__}"
        )
        raise TypeError(msg)

    for i, item in enumerate(items):
        if item is None:
            if not na_allowed:
                msg = f"`{name}[{i}]` must not be None"
                raise ValueError(msg)
            continue
        if not isinstance(item, str):
            msg = f"`{name}[{i}]` must be a string, got {type(item).__name__}"
            raise TypeError(msg)

    if min_length is not None and len(items) < min_length:
        msg = f"`{name}` must have at least {min_length} element(s), got {len(items)}"
        raise ValueError(msg)
    if max_length is not None and len(items) > max_length:
        msg = f"`{name}` must have at most {max_length} element(s), got {len(items)}"
        raise ValueError(msg)


def assert_choice(
    value: Any,
    choices: Sequence[Any],
    *,
    name: str = "value",
    null_allowed: bool = False,
) -> None:
    """Assert *value* is one of the given *choices*."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)

    if value not in choices:
        msg = f"`{name}` must be one of {list(choices)}, got {value!r}"
        raise ValueError(msg)


def assert_class(
    value: Any,
    cls: type | tuple[type, ...],
    *,
    name: str = "value",
    null_allowed: bool = False,
) -> None:
    """Assert *value* is an instance of *cls*."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)

    if not isinstance(value, cls):
        expected = (
            cls.__name__
            if isinstance(cls, type)
            else " | ".join(c.__name__ for c in cls)
        )
        msg = f"`{name}` must be an instance of {expected}, got {type(value).__name__}"
        raise TypeError(msg)


def assert_date(
    value: Any,
    *,
    name: str = "value",
    null_allowed: bool = False,
) -> None:
    """Assert *value* is a ``datetime.date`` (or datetime)."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)
    if not isinstance(value, datetime.date):
        msg = f"`{name}` must be a date, got {type(value).__name__}"
        raise TypeError(msg)


def assert_list(
    value: Any,
    *,
    name: str = "value",
    element_class: type | None = None,
    min_length: int | None = None,
    null_allowed: bool = False,
) -> None:
    """Assert *value* is a list (or sequence)."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)
    if not isinstance(value, (list, tuple)):
        msg = f"`{name}` must be a list or tuple, got {type(value).__name__}"
        raise TypeError(msg)
    if min_length is not None and len(value) < min_length:
        msg = f"`{name}` must have at least {min_length} element(s), got {len(value)}"
        raise ValueError(msg)
    if element_class is not None:
        for i, item in enumerate(value):
            if not isinstance(item, element_class):
                msg = (
                    f"`{name}[{i}]` must be an instance of "
                    f"{element_class.__name__}, got {type(item).__name__}"
                )
                raise TypeError(msg)


def assert_logical(
    value: Any,
    *,
    name: str = "value",
    null_allowed: bool = False,
) -> None:
    """Assert *value* is a boolean."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)
    if not isinstance(value, bool):
        msg = f"`{name}` must be a bool, got {type(value).__name__}"
        raise TypeError(msg)


def assert_numeric(
    value: Any,
    *,
    name: str = "value",
    min_val: int | float | None = None,
    max_val: int | float | None = None,
    null_allowed: bool = False,
) -> None:
    """Assert *value* is numeric (int or float)."""
    if value is None:
        if null_allowed:
            return
        msg = f"`{name}` must not be None"
        raise TypeError(msg)
    if not isinstance(value, (int, float)):
        msg = f"`{name}` must be numeric, got {type(value).__name__}"
        raise TypeError(msg)
    if min_val is not None and value < min_val:
        msg = f"`{name}` must be >= {min_val}, got {value}"
        raise ValueError(msg)
    if max_val is not None and value > max_val:
        msg = f"`{name}` must be <= {max_val}, got {value}"
        raise ValueError(msg)


def assert_true(
    condition: bool,
    *,
    msg: str = "Assertion failed",
) -> None:
    """Assert a boolean condition is True."""
    if not condition:
        raise ValueError(msg)


def assert_table_columns(
    columns: Sequence[str],
    required: Sequence[str],
    *,
    table_name: str = "table",
) -> None:
    """Assert all *required* columns are present in *columns*."""
    col_set = set(columns)
    missing = [c for c in required if c not in col_set]
    if missing:
        msg = f"Table '{table_name}' is missing required columns: {missing}"
        raise ValueError(msg)
