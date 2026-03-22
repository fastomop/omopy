# omopy.codelist

Vocabulary-based code list generation and analysis — search the OMOP vocabulary,
traverse concept hierarchies, and build/manipulate code lists for phenotyping.

This module is the Python equivalent of the R `CodelistGenerator` package.

## Vocabulary Search

::: omopy.codelist.get_candidate_codes

::: omopy.codelist.get_mappings

## Hierarchy Traversal

::: omopy.codelist.get_descendants

::: omopy.codelist.get_ancestors

## Drug & ATC

::: omopy.codelist.get_drug_ingredient_codes

::: omopy.codelist.get_atc_codes

## Set Operations

::: omopy.codelist.union_codelists

::: omopy.codelist.intersect_codelists

::: omopy.codelist.compare_codelists

## Subsetting

::: omopy.codelist.subset_to_codes_in_use

::: omopy.codelist.subset_by_domain

::: omopy.codelist.subset_by_vocabulary

## Stratification

::: omopy.codelist.stratify_by_domain

::: omopy.codelist.stratify_by_vocabulary

::: omopy.codelist.stratify_by_concept_class

## Diagnostics

::: omopy.codelist.summarise_code_use

::: omopy.codelist.summarise_orphan_codes
