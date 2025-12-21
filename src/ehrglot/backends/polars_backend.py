"""Polars backend implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa

from ehrglot.core.types import ColumnMetadata, DataType, SchemaDefinition


def _polars_type_to_data_type(polars_type: pl.DataType) -> DataType:
    """Convert Polars type to EHRglot DataType."""
    if polars_type == pl.Utf8 or polars_type == pl.String:
        return DataType.STRING
    elif polars_type in (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    ):
        return DataType.INTEGER
    elif polars_type in (pl.Float32, pl.Float64):
        return DataType.FLOAT
    elif polars_type == pl.Boolean:
        return DataType.BOOLEAN
    elif polars_type == pl.Date:
        return DataType.DATE
    elif polars_type == pl.Datetime or isinstance(polars_type, pl.Datetime):
        return DataType.DATETIME
    elif polars_type == pl.Binary:
        return DataType.BINARY
    elif isinstance(polars_type, pl.List):
        return DataType.ARRAY
    elif isinstance(polars_type, pl.Struct):
        return DataType.OBJECT
    else:
        return DataType.STRING  # Fallback


class PolarsBackend:
    """Polars-based backend for data operations.

    Uses Polars DataFrames for high-performance in-memory processing.
    Best for: Fast transformations, lazy evaluation, memory efficiency.
    """

    @property
    def name(self) -> str:
        """Return the backend name."""
        return "polars"

    def read_parquet(self, path: str | Path) -> pl.DataFrame:
        """Read a Parquet file into a Polars DataFrame.

        Args:
            path: Path to the Parquet file.

        Returns:
            Polars DataFrame.
        """
        return pl.read_parquet(str(path))

    def read_csv(self, path: str | Path, **options: Any) -> pl.DataFrame:
        """Read a CSV file into a Polars DataFrame.

        Args:
            path: Path to the CSV file.
            **options: Options passed to polars.read_csv.

        Returns:
            Polars DataFrame.
        """
        return pl.read_csv(str(path), **options)

    def write_parquet(self, df: pl.DataFrame, path: str | Path) -> None:
        """Write a Polars DataFrame to a Parquet file.

        Args:
            df: Polars DataFrame.
            path: Output path for the Parquet file.
        """
        df.write_parquet(str(path))

    def execute_sql(self, query: str, **params: Any) -> pl.DataFrame:
        """Execute SQL using Polars SQL context.

        Args:
            query: SQL query string.
            **params: Named DataFrames to register (name -> DataFrame).

        Returns:
            Polars DataFrame with query results.
        """
        ctx = pl.SQLContext()
        for name, df in params.items():
            ctx.register(name, df)
        return ctx.execute(query).collect()

    def get_schema(self, df: pl.DataFrame) -> SchemaDefinition:
        """Extract schema definition from a Polars DataFrame.

        Args:
            df: Polars DataFrame.

        Returns:
            SchemaDefinition with column metadata.
        """
        columns = []
        for name, dtype in df.schema.items():
            # Check if column has nulls to determine nullability
            col = ColumnMetadata(
                name=name,
                data_type=_polars_type_to_data_type(dtype),
                nullable=True,  # Polars doesn't track nullability in schema
            )
            columns.append(col)

        return SchemaDefinition(
            name="polars_dataframe",
            version="1.0",
            columns=columns,
        )

    def to_arrow(self, df: pl.DataFrame) -> pa.Table:
        """Convert Polars DataFrame to PyArrow Table.

        Args:
            df: Polars DataFrame.

        Returns:
            PyArrow Table.
        """
        return df.to_arrow()

    def from_arrow(self, table: pa.Table) -> pl.DataFrame:
        """Convert PyArrow Table to Polars DataFrame.

        Args:
            table: PyArrow Table.

        Returns:
            Polars DataFrame.
        """
        result = pl.from_arrow(table)
        if isinstance(result, pl.DataFrame):
            return result
        # If it's a Series, convert to DataFrame
        return result.to_frame()

    def select_columns(self, df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
        """Select specific columns from a Polars DataFrame.

        Args:
            df: Polars DataFrame.
            columns: List of column names to select.

        Returns:
            Polars DataFrame with only selected columns.
        """
        return df.select(columns)

    def rename_columns(self, df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
        """Rename columns in a Polars DataFrame.

        Args:
            df: Polars DataFrame.
            mapping: Dictionary of old_name -> new_name.

        Returns:
            Polars DataFrame with renamed columns.
        """
        return df.rename(mapping)

    def sample(self, df: pl.DataFrame, n: int) -> pl.DataFrame:
        """Sample n rows from a Polars DataFrame.

        Args:
            df: Polars DataFrame.
            n: Number of rows to sample.

        Returns:
            Polars DataFrame with sampled rows.
        """
        if df.height <= n:
            return df
        return df.sample(n=n)
