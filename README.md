# EHRglot

EHR conversion system with PII tagging and data masking for Snowflake/Databricks.

## Features

- **Multi-Backend Support**: Arrow, Polars, DuckDB for high-performance data processing
- **FHIR R4 Canonical Model**: Industry-standard intermediate representation
- **PII Detection & Tagging**: Automatic detection of HIPAA Safe Harbor identifiers
- **Masking Policy Generation**: Snowflake DDM and Databricks Unity Catalog policies
- **YAML Schema Definitions**: Flexible, version-controlled schema mappings

## Installation

```bash
# Using uv (recommended)
uv sync

# With dev dependencies
uv sync --extra dev
```

## Quick Start

```python
from ehrglot import ConversionEngine
from ehrglot.backends import PolarsBackend

engine = ConversionEngine(backend=PolarsBackend())

# Convert Epic Clarity data to Snowflake format with masking policies
result = engine.convert(
    source="epic_clarity_export.parquet",
    source_system="epic_clarity",
    target_system="snowflake",
)

# Get generated masking policies
print(result.masking_policies)
```

## Development

```bash
# Install with dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Run linting
uv run ruff check src/ tests/

# Run type checking
uv run mypy src/

# Install pre-commit hooks
uv run pre-commit install
```

## Architecture

```
Source EHR (Epic/Cerner) → FHIR R4 (Canonical) → Target (Snowflake/Databricks)
                              ↓
                     PII Detection & Tagging
                              ↓
                     Masking Policy Generation
```

## License

MIT
