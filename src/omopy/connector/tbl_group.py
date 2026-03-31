"""Table grouping helper.

Provides ``tbl_group()`` to retrieve table names by logical group
(vocab, clinical, all, default, derived).  Equivalent to R's
``tblGroup()``.
"""

from __future__ import annotations

from omopy.generics._schema import CdmSchema
from omopy.generics._types import CdmVersion, TableGroup

__all__ = ["tbl_group"]


def tbl_group(
    group: str | TableGroup | list[str | TableGroup],
    *,
    cdm_version: str | CdmVersion = CdmVersion.V5_4,
) -> list[str]:
    """Return CDM table names belonging to one or more logical groups.

    Parameters
    ----------
    group
        A group name (or ``TableGroup`` enum), or a list of them.
        Valid values: ``"vocab"``, ``"clinical"``, ``"all"``,
        ``"default"``, ``"derived"``.
    cdm_version
        CDM version to use for the lookup.  Defaults to 5.4.
        The set of table groups is the same across 5.3 and 5.4.

    Returns
    -------
    list[str]
        Unique table names in the requested group(s), in schema order.

    Examples
    --------
    >>> tbl_group("vocab")
    ['concept', 'vocabulary', ...]
    >>> tbl_group(["clinical", "derived"])
    ['person', 'observation_period', ..., 'drug_era', ...]
    """
    groups = [group] if isinstance(group, (str, TableGroup)) else list(group)

    schema = CdmSchema(CdmVersion(str(cdm_version)))

    seen: set[str] = set()
    result: list[str] = []
    for g in groups:
        tg = TableGroup(str(g))
        for name in schema.table_names_in_group(tg):
            if name not in seen:
                seen.add(name)
                result.append(name)

    return result
