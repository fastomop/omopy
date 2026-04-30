# Installation

## Requirements

OMOPy requires **Python 3.14 or later**. It does not support older Python versions.

## Install from PyPI

```bash
pip install omopy
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add omopy
```

## Database Backends

OMOPy uses [Ibis](https://ibis-project.org/) for database access. DuckDB support
is included by default. For other databases, install the appropriate extras:

```bash
# PostgreSQL
pip install omopy[postgres]

# SQL Server
pip install omopy[mssql]

# Snowflake
pip install omopy[snowflake]

# BigQuery
pip install omopy[bigquery]

# All backends
pip install omopy[all]
```

## Development Install

Clone the repository and install with dev dependencies:

```bash
git clone https://github.com/fastomop/omopy.git
cd omopy
uv sync          # installs all deps + dev tools
uv run pytest    # run the test suite
```

## Verify Installation

```python
import omopy
print(omopy.__version__)
# 0.1.0
```

```python
from omopy.connector import cdm_from_con
cdm = cdm_from_con("path/to/your/omop.duckdb", cdm_schema="cdm")
print(cdm)
```
