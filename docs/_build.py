#!/usr/bin/env python3
"""Wrapper that applies the CPython 3.14 beta compatibility shim before
invoking mkdocs.  This is needed because ``mkdocstrings-python`` uses Pydantic
dataclasses internally, and Pydantic >=2.12 calls
``typing._eval_type(..., prefer_fwd_module=True)`` which doesn't exist in
CPython 3.14.0b4 (the parameter is called ``parent_fwdref`` there).

Usage::

    uv run python docs/_build.py build --strict
    uv run python docs/_build.py serve
"""

import inspect
import sys
import typing

# ---------------------------------------------------------------------------
# Apply the same shim from src/omopy/__init__.py
# ---------------------------------------------------------------------------
if sys.version_info[:3] < (3, 14, 0) or (
    sys.version_info[:3] == (3, 14, 0)
    and "b" in (sys.version.split()[0])
    and not hasattr(typing._eval_type, "__wrapped__")
):
    _orig_sig = inspect.signature(typing._eval_type)
    if (
        "parent_fwdref" in _orig_sig.parameters
        and "prefer_fwd_module" not in _orig_sig.parameters
    ):
        _orig_eval_type = typing._eval_type

        def _patched_eval_type(*args, **kwargs):  # type: ignore[no-untyped-def]
            if "prefer_fwd_module" in kwargs:
                kwargs["parent_fwdref"] = kwargs.pop("prefer_fwd_module")
            return _orig_eval_type(*args, **kwargs)

        _patched_eval_type.__wrapped__ = True  # type: ignore[attr-defined]
        typing._eval_type = _patched_eval_type  # type: ignore[attr-defined]
# ---------------------------------------------------------------------------

from mkdocs.__main__ import cli

if __name__ == "__main__":
    cli()
