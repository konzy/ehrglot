"""PyArrow backend implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ehrglot.core.types import ColumnMetadata, DataType, SchemaDefinition


def _arrow_type_to_data_type(arrow_type: pa.DataType) -> DataType:
    """Convert PyArrow type to EHRglot DataType."""
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return DataType.STRING
    elif pa.types.is_integer(arrow_type):
        return DataType.INTEGER
    elif pa.types.is_floating(arrow_type):
        return DataType.FLOAT
    elif pa.types.is_boolean(arrow_type):
        return DataType.BOOLEAN
    elif pa.types.is_date(arrow_type):
        return DataType.DATE
    elif pa.types.is_timestamp(arrow_type):
        return DataType.TIMESTAMP
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return DataType.BINARY
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return DataType.ARRAY
    elif pa.types.is_struct(arrow_type) or pa.types.is_map(arrow_type):
        return DataType.OBJECT
    else:
        return DataType.STRING  # Fallback


class ArrowBackend:
    """PyArrow-based backend for data operations.

    Uses PyArrow Tables for in-memory processing and Parquet for storage.
    Best for: Large-scale batch processing, Parquet/Arrow IPC files.
    """

    @property
    def name(self) -> str:
        """Return the backend name."""
        return "arrow"

    def read_parquet(self, path: str | Path) -> pa.Table:
        """Read a Parquet file into a PyArrow Table.

        Args:
            path: Path to the Parquet file.

        Returns:
            PyArrow Table.
        """
        return pq.read_table(str(path))

    def read_csv(self, path: str | Path, **options: Any) -> pa.Table:
        """Read a CSV file into a PyArrow Table.

        Args:
            path: Path to the CSV file.
            **options: Options passed to pyarrow.csv.read_csv.

        Returns:
            PyArrow Table.
        """
        import pyarrow.csv as csv

        return csv.read_csv(str(path), **options)

    def write_parquet(self, df: pa.Table, path: str | Path) -> None:
        """Write a PyArrow Table to a Parquet file.

        Args:
            df: PyArrow Table.
            path: Output path for the Parquet file.
        """
        pq.write_table(df, str(path))

    def execute_sql(self, query: str, **params: Any) -> pa.Table:
        """Execute SQL is not natively supported in Arrow.

        For SQL support, use DuckDB backend instead.

        Raises:
            NotImplementedError: Arrow doesn't support SQL natively.
        """
        raise NotImplementedError(
            "Arrow backend doesn't support SQL. Use DuckDB backend for SQL queries."
        )

    def get_schema(self, df: pa.Table) -> SchemaDefinition:
        """Extract schema definition from a PyArrow Table.

        Args:
            df: PyArrow Table.

        Returns:
            SchemaDefinition with column metadata.
        """
        columns = []
        for field in df.schema:
            col = ColumnMetadata(
                name=field.name,
                data_type=_arrow_type_to_data_type(field.type),
                nullable=field.nullable,
            )
            columns.append(col)

        return SchemaDefinition(
            name="arrow_table",
            version="1.0",
            columns=columns,
        )

    def to_arrow(self, df: pa.Table) -> pa.Table:
        """Return the PyArrow Table as-is.

        Args:
            df: PyArrow Table.

        Returns:
            Same PyArrow Table.
        """
        return df

    def from_arrow(self, table: pa.Table) -> pa.Table:
        """Return the PyArrow Table as-is.

        Args:
            table: PyArrow Table.

        Returns:
            Same PyArrow Table.
        """
        return table

    def select_columns(self, df: pa.Table, columns: list[str]) -> pa.Table:
        """Select specific columns from a PyArrow Table.

        Args:
            df: PyArrow Table.
            columns: List of column names to select.

        Returns:
            PyArrow Table with only selected columns.
        """
        return df.select(columns)

    def rename_columns(self, df: pa.Table, mapping: dict[str, str]) -> pa.Table:
        """Rename columns in a PyArrow Table.

        Args:
            df: PyArrow Table.
            mapping: Dictionary of old_name -> new_name.

        Returns:
            PyArrow Table with renamed columns.
        """
        new_names = [mapping.get(name, name) for name in df.column_names]
        return df.rename_columns(new_names)

    def sample(self, df: pa.Table, n: int) -> pa.Table:
        """Sample n rows from a PyArrow Table.

        Args:
            df: PyArrow Table.
            n: Number of rows to sample.

        Returns:
            PyArrow Table with sampled rows.
        """
        if df.num_rows <= n:
            return df
        return df.slice(0, n)
