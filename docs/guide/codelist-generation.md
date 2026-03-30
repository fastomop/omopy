# Codelist Generation

The `omopy.codelist` module provides tools for searching OMOP vocabularies,
building codelists, and analysing concept usage. It is the Python equivalent
of the R `CodelistGenerator` package.

## Searching for Concepts

```python
from omopy.codelist import get_candidate_codes

# Search by keyword
codelist = get_candidate_codes(
    cdm,
    keywords=["sinusitis"],
    domains=["Condition"],
    standard_concept="S",
)
print(codelist)
# Codelist({"sinusitis": [40481087, 257012, 4283893, ...]})
```

### Search Options

```python
codelist = get_candidate_codes(
    cdm,
    keywords=["diabetes", "mellitus"],
    domains=["Condition"],              # restrict to specific domains
    standard_concept="S",               # standard concepts only
    vocabulary_id=["SNOMED"],           # restrict to specific vocabularies
    concept_class_id=["Disorder"],      # restrict to concept classes
    exclude=["insipidus"],              # exclude concepts matching these
)
```

## Concept Mappings

```python
from omopy.codelist import get_mappings

# Find standard concepts mapped from source codes
mapped = get_mappings(
    cdm,
    codelist,
    relationship_id="Maps to",
)
```

## Hierarchy Traversal

```python
from omopy.codelist import get_descendants, get_ancestors

# Expand codelist to include descendant concepts
expanded = get_descendants(cdm, codelist)

# Find ancestor concepts
ancestors = get_ancestors(cdm, codelist)
```

## Drug-Specific Functions

```python
from omopy.codelist import get_drug_ingredient_codes, get_atc_codes

# Find drug ingredients by keyword
ingredients = get_drug_ingredient_codes(cdm, ingredient="ibuprofen")

# Find ATC codes
atc = get_atc_codes(cdm, atc_name="anti-inflammatory", level="3")
```

## Codelist Operations

```python
from omopy.codelist import union_codelists, intersect_codelists, compare_codelists

# Combine codelists
combined = union_codelists(codelist_a, codelist_b)

# Find common concepts
common = intersect_codelists(codelist_a, codelist_b)

# Compare two codelists
comparison = compare_codelists(codelist_a, codelist_b)
```

## Subsetting

```python
from omopy.codelist import subset_by_domain, subset_by_vocabulary, subset_to_codes_in_use

# Keep only Condition concepts
conditions_only = subset_by_domain(codelist, cdm, domain_id="Condition")

# Keep only SNOMED concepts
snomed_only = subset_by_vocabulary(codelist, cdm, vocabulary_id="SNOMED")

# Keep only concepts that appear in the data
in_use = subset_to_codes_in_use(codelist, cdm)
```

## Stratification

```python
from omopy.codelist import stratify_by_domain, stratify_by_vocabulary, stratify_by_concept_class

# Split by domain
by_domain = stratify_by_domain(codelist, cdm)
# {"Condition": Codelist(...), "Drug": Codelist(...), ...}

# Split by vocabulary
by_vocab = stratify_by_vocabulary(codelist, cdm)

# Split by concept class
by_class = stratify_by_concept_class(codelist, cdm)
```

## Diagnostics

```python
from omopy.codelist import summarise_code_use, summarise_orphan_codes

# How often do codelist concepts appear in the data?
usage = summarise_code_use(codelist, cdm)
print(usage)  # Polars DataFrame with counts per concept per domain

# Find orphan codes (in hierarchy but not in codelist, yet present in data)
orphans = summarise_orphan_codes(codelist, cdm)
print(orphans)
```
