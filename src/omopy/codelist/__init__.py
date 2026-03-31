"""``omopy.codelist`` — Vocabulary-based code list generation and analysis.

This subpackage provides functions to search the OMOP vocabulary,
traverse concept hierarchies, and build/manipulate code lists for
phenotyping studies. It is the Python equivalent of the R
CodelistGenerator package.

Primary functions::

    from omopy.codelist import (
        get_candidate_codes,
        get_descendants,
        get_ancestors,
        get_drug_ingredient_codes,
        get_atc_codes,
        get_mappings,
        union_codelists,
        intersect_codelists,
        compare_codelists,
        subset_to_codes_in_use,
        stratify_by_domain,
        stratify_by_vocabulary,
        summarise_code_use,
        summarise_orphan_codes,
    )
"""

from omopy.codelist._diagnostics import (
    summarise_code_use,
    summarise_orphan_codes,
)
from omopy.codelist._drug import (
    get_atc_codes,
    get_drug_ingredient_codes,
)
from omopy.codelist._hierarchy import (
    get_ancestors,
    get_descendants,
)
from omopy.codelist._operations import (
    compare_codelists,
    intersect_codelists,
    union_codelists,
)
from omopy.codelist._search import (
    get_candidate_codes,
    get_mappings,
)
from omopy.codelist._stratify import (
    stratify_by_concept_class,
    stratify_by_domain,
    stratify_by_vocabulary,
)
from omopy.codelist._subset import (
    subset_by_domain,
    subset_by_vocabulary,
    subset_to_codes_in_use,
)

__all__ = [
    "compare_codelists",
    "get_ancestors",
    "get_atc_codes",
    # search
    "get_candidate_codes",
    # hierarchy
    "get_descendants",
    # drug
    "get_drug_ingredient_codes",
    "get_mappings",
    "intersect_codelists",
    "stratify_by_concept_class",
    # stratify
    "stratify_by_domain",
    "stratify_by_vocabulary",
    "subset_by_domain",
    "subset_by_vocabulary",
    # subset
    "subset_to_codes_in_use",
    # diagnostics
    "summarise_code_use",
    "summarise_orphan_codes",
    # operations
    "union_codelists",
]
