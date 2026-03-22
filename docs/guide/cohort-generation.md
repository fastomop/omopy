# Cohort Generation

OMOPy supports two approaches to cohort generation: concept-based cohorts
(simple, code-driven) and CIRCE-based cohorts (complex, from ATLAS JSON
definitions).

## Concept-Based Cohorts

The simplest approach — define cohorts by lists of OMOP concept IDs:

```python
from omopy.generics import Codelist
from omopy.connector import generate_concept_cohort_set

# Define concept sets
codelist = Codelist({
    "hypertension": [320128],
    "viral_sinusitis": [40481087],
    "diabetes": [201826, 442793],
})

# Generate cohorts
cdm = generate_concept_cohort_set(
    cdm,
    codelist,
    name="conditions",
)

# Access the result
cohort = cdm["conditions"]
df = cohort.collect()
print(df)
```

### How It Works

For each concept set, `generate_concept_cohort_set`:

1. Looks up which clinical domain each concept belongs to (Condition, Drug, etc.)
2. Queries the corresponding domain table (e.g., `condition_occurrence`)
3. Filters to rows matching the concept IDs
4. Constrains to records within an observation period
5. Collapses overlapping date ranges into continuous cohort eras
6. Assigns `cohort_definition_id` based on the codelist order

### Options

```python
cdm = generate_concept_cohort_set(
    cdm,
    codelist,
    name="conditions",
    include_descendants=True,   # expand to descendant concepts
    domains=["Condition"],       # restrict to specific domains
    observation_filter=True,     # require observation period overlap
    gap_days=0,                  # days allowed between eras for collapsing
)
```

## CIRCE-Based Cohorts (ATLAS JSON)

For complex cohort definitions created in [ATLAS](https://atlas-demo.ohdsi.org/):

```python
from omopy.connector import generate_cohort_set

# From a directory of JSON files
cdm = generate_cohort_set(
    cdm,
    "path/to/cohort_definitions/",
    name="atlas_cohorts",
)

# From a single JSON file
cdm = generate_cohort_set(
    cdm,
    "path/to/cohort.json",
    name="my_cohort",
)
```

### CIRCE Engine Features

The CIRCE engine is a clean-room Python implementation supporting:

- **Primary criteria** — initial qualifying events from any clinical domain
- **Inclusion rules** — additional criteria with temporal windows
- **Correlated criteria** — count-based requirements (at least N, at most N, exactly N)
- **Demographic criteria** — age, gender, race, ethnicity filters
- **End strategies** — date offset, custom drug era
- **Era collapsing** — merge overlapping cohort periods with configurable gap days
- **Censor windows** — hard date boundaries

## CohortTable Structure

Generated cohorts are stored as `CohortTable` objects with companion metadata:

```python
cohort = cdm["conditions"]

# Required columns
# cohort_definition_id | subject_id | cohort_start_date | cohort_end_date

# Companion data
print(cohort.settings)        # Polars DF: cohort_definition_id, cohort_name, ...
print(cohort.attrition)       # Polars DF: step-by-step record counts
print(cohort.cohort_codelist)  # Polars DF: concept IDs per cohort
```

## Working with Codelist Objects

```python
from omopy.generics import Codelist

# Create from a dict
cl = Codelist({
    "aspirin": [1112807],
    "ibuprofen": [1177480],
})

# Access
print(cl.names)            # ('aspirin', 'ibuprofen')
print(cl.all_concept_ids)  # {1112807, 1177480}
print(cl["aspirin"])       # [1112807]
```

### ConceptSetExpression (Rich Codelist)

For codelists with include/exclude/descendants flags:

```python
from omopy.generics import ConceptSetExpression, ConceptEntry

cse = ConceptSetExpression({
    "diabetes": [
        ConceptEntry(concept_id=201826, include_descendants=True),
        ConceptEntry(concept_id=442793, is_excluded=True),
    ]
})

# Convert to a flat Codelist (requires CDM for descendant resolution)
codelist = cse.to_codelist()
```
