"""OMOPy — Pythonic, type-safe interface for OMOP CDM databases.

Submodules:
    - ``omopy.generics`` — Core type system (Phase 0)
    - ``omopy.connector`` — Database CDM access (Phase 1+2)
    - ``omopy.profiles`` — Patient profiles (Phase 3A)
    - ``omopy.codelist`` — Codelist generation (Phase 3B)
    - ``omopy.vis`` — Visualisation (Phase 3C, future)
"""

# ---------------------------------------------------------------------------
# CPython 3.14 compatibility shim for Pydantic
# ---------------------------------------------------------------------------
# Pydantic >=2.12 calls ``typing._eval_type(..., prefer_fwd_module=True)``
# which was introduced in CPython 3.14.0rc1.  Earlier 3.14 betas (up to b4)
# use the name ``parent_fwdref`` instead.  We monkey-patch the stdlib
# function so that the ``prefer_fwd_module`` kwarg is silently translated.
import sys as _sys
import typing as _typing

if _sys.version_info[:3] < (3, 14, 0) or (
    _sys.version_info[:3] == (3, 14, 0)
    and "b" in (_sys.version.split()[0])  # beta build
    and not hasattr(_typing._eval_type, "__wrapped__")  # not already patched
):
    import inspect as _inspect

    _orig_sig = _inspect.signature(_typing._eval_type)
    if (
        "parent_fwdref" in _orig_sig.parameters
        and "prefer_fwd_module" not in _orig_sig.parameters
    ):
        _orig_eval_type = _typing._eval_type

        def _patched_eval_type(*args, **kwargs):  # type: ignore[no-untyped-def]
            if "prefer_fwd_module" in kwargs:
                kwargs["parent_fwdref"] = kwargs.pop("prefer_fwd_module")
            return _orig_eval_type(*args, **kwargs)

        _patched_eval_type.__wrapped__ = True  # type: ignore[attr-defined]
        _typing._eval_type = _patched_eval_type  # type: ignore[attr-defined]

    del _inspect, _orig_sig
# ---------------------------------------------------------------------------

__version__ = "0.1.0"
