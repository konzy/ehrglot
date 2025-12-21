"""Backend implementations for EHRglot."""

from ehrglot.backends.arrow import ArrowBackend
from ehrglot.backends.base import Backend, DataFrame
from ehrglot.backends.duckdb_backend import DuckDBBackend
from ehrglot.backends.polars_backend import PolarsBackend

__all__ = [
    "ArrowBackend",
    "Backend",
    "DataFrame",
    "DuckDBBackend",
    "PolarsBackend",
]
