"""Abstract backend interface for data operations."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pathlib import Path

    import pyarrow as pa

    from ehrglot.core.types import SchemaDefinition


@runtime_checkable
class DataFrame(Protocol):
    """Protocol for DataFrame-like objects (Arrow Table, Polars DataFrame, etc.)."""

    @property
    def columns(self) -> list[str]:
        """Return column names."""
        ...

    @property
    def shape(self) -> tuple[int, int]:
        """Return (rows, columns) shape."""
        ...

    def to_arrow(self) -> pa.Table:
        """Convert to PyArrow Table."""
        ...


@runtime_checkable
class Backend(Protocol):
    """Abstract interface for data processing backends.

    Implementations should support reading/writing data and schema operations.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the backend name (e.g., 'arrow', 'polars', 'duckdb')."""
        ...

    @abstractmethod
    def read_parquet(self, path: str | Path) -> Any:
        """Read a Parquet file into the backend's native DataFrame type.

        Args:
            path: Path to the Parquet file.

        Returns:
            Backend-specific DataFrame.
        """
        ...

    @abstractmethod
    def read_csv(self, path: str | Path, **options: Any) -> Any:
        """Read a CSV file into the backend's native DataFrame type.

        Args:
            path: Path to the CSV file.
            **options: Backend-specific options (delimiter, encoding, etc.).

        Returns:
            Backend-specific DataFrame.
        """
        ...

    @abstractmethod
    def write_parquet(self, df: Any, path: str | Path) -> None:
        """Write a DataFrame to a Parquet file.

        Args:
            df: Backend-specific DataFrame.
            path: Output path for the Parquet file.
        """
        ...

    @abstractmethod
    def execute_sql(self, query: str, **params: Any) -> Any:
        """Execute a SQL query and return results.

        Args:
            query: SQL query string.
            **params: Query parameters.

        Returns:
            Backend-specific DataFrame with query results.
        """
        ...

    @abstractmethod
    def get_schema(self, df: Any) -> SchemaDefinition:
        """Extract schema definition from a DataFrame.

        Args:
            df: Backend-specific DataFrame.

        Returns:
            SchemaDefinition with column metadata.
        """
        ...

    @abstractmethod
    def to_arrow(self, df: Any) -> pa.Table:
        """Convert backend DataFrame to PyArrow Table.

        Args:
            df: Backend-specific DataFrame.

        Returns:
            PyArrow Table.
        """
        ...

    @abstractmethod
    def from_arrow(self, table: pa.Table) -> Any:
        """Convert PyArrow Table to backend DataFrame.

        Args:
            table: PyArrow Table.

        Returns:
            Backend-specific DataFrame.
        """
        ...

    @abstractmethod
    def select_columns(self, df: Any, columns: list[str]) -> Any:
        """Select specific columns from a DataFrame.

        Args:
            df: Backend-specific DataFrame.
            columns: List of column names to select.

        Returns:
            Backend-specific DataFrame with only selected columns.
        """
        ...

    @abstractmethod
    def rename_columns(self, df: Any, mapping: dict[str, str]) -> Any:
        """Rename columns in a DataFrame.

        Args:
            df: Backend-specific DataFrame.
            mapping: Dictionary of old_name -> new_name.

        Returns:
            Backend-specific DataFrame with renamed columns.
        """
        ...

    @abstractmethod
    def sample(self, df: Any, n: int) -> Any:
        """Sample n rows from a DataFrame.

        Args:
            df: Backend-specific DataFrame.
            n: Number of rows to sample.

        Returns:
            Backend-specific DataFrame with sampled rows.
        """
        ...
