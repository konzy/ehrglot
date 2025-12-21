"""DuckDB backend implementation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from ehrglot.core.types import ColumnMetadata, DataType, SchemaDefinition


def _duckdb_type_to_data_type(type_str: str) -> DataType:
    """Convert DuckDB type string to EHRglot DataType."""
    type_upper = type_str.upper()
    if "VARCHAR" in type_upper or "TEXT" in type_upper or "STRING" in type_upper:
        return DataType.STRING
    elif "INT" in type_upper or "BIGINT" in type_upper or "SMALLINT" in type_upper:
        return DataType.INTEGER
    elif (
        "FLOAT" in type_upper
        or "DOUBLE" in type_upper
        or "REAL" in type_upper
        or "DECIMAL" in type_upper
    ):
        return DataType.FLOAT
    elif "BOOL" in type_upper:
        return DataType.BOOLEAN
    elif type_upper == "DATE":
        return DataType.DATE
    elif "TIMESTAMP" in type_upper:
        return DataType.TIMESTAMP
    elif "BLOB" in type_upper or "BINARY" in type_upper:
        return DataType.BINARY
    elif "LIST" in type_upper or "ARRAY" in type_upper:
        return DataType.ARRAY
    elif "STRUCT" in type_upper or "MAP" in type_upper:
        return DataType.OBJECT
    else:
        return DataType.STRING  # Fallback


class DuckDBBackend:
    """DuckDB-based backend for data operations.

    Uses DuckDB for SQL-based transformations and analytical queries.
    Best for: SQL transformations, complex analytics, joining multiple sources.
    """

    def __init__(self, database: str = ":memory:") -> None:
        """Initialize DuckDB backend.

        Args:
            database: Path to DuckDB database file, or ":memory:" for in-memory.
        """
        self._conn = duckdb.connect(database)
        self._registered_tables: dict[str, bool] = {}

    @property
    def name(self) -> str:
        """Return the backend name."""
        return "duckdb"

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return the DuckDB connection."""
        return self._conn

    def read_parquet(self, path: str | Path) -> duckdb.DuckDBPyRelation:
        """Read a Parquet file into a DuckDB relation.

        Args:
            path: Path to the Parquet file.

        Returns:
            DuckDB relation.
        """
        return self._conn.read_parquet(str(path))

    def read_csv(self, path: str | Path, **options: Any) -> duckdb.DuckDBPyRelation:
        """Read a CSV file into a DuckDB relation.

        Args:
            path: Path to the CSV file.
            **options: Options passed to DuckDB read_csv.

        Returns:
            DuckDB relation.
        """
        return self._conn.read_csv(str(path), **options)

    def write_parquet(self, df: duckdb.DuckDBPyRelation, path: str | Path) -> None:
        """Write a DuckDB relation to a Parquet file.

        Args:
            df: DuckDB relation.
            path: Output path for the Parquet file.
        """
        df.write_parquet(str(path))

    def execute_sql(self, query: str, **params: Any) -> duckdb.DuckDBPyRelation:
        """Execute SQL query.

        Args:
            query: SQL query string.
            **params: Named DataFrames to register before query (name -> DataFrame).

        Returns:
            DuckDB relation with query results.
        """
        # Register any provided DataFrames as tables
        for name, df in params.items():
            self._conn.register(name, df)
        return self._conn.sql(query)

    def register_table(self, name: str, data: Any) -> None:
        """Register a table for SQL queries.

        Args:
            name: Table name to register.
            data: Data to register (DataFrame, relation, path, etc.).
        """
        self._conn.register(name, data)
        self._registered_tables[name] = True

    def get_schema(self, df: duckdb.DuckDBPyRelation) -> SchemaDefinition:
        """Extract schema definition from a DuckDB relation.

        Args:
            df: DuckDB relation.

        Returns:
            SchemaDefinition with column metadata.
        """
        columns = []
        for col_name, col_type in zip(df.columns, df.types, strict=True):
            col = ColumnMetadata(
                name=col_name,
                data_type=_duckdb_type_to_data_type(str(col_type)),
                nullable=True,  # DuckDB doesn't expose nullability easily
            )
            columns.append(col)

        return SchemaDefinition(
            name="duckdb_relation",
            version="1.0",
            columns=columns,
        )

    def to_arrow(self, df: duckdb.DuckDBPyRelation) -> pa.Table:
        """Convert DuckDB relation to PyArrow Table.

        Args:
            df: DuckDB relation.

        Returns:
            PyArrow Table.
        """
        return df.arrow()

    def from_arrow(self, table: pa.Table) -> duckdb.DuckDBPyRelation:
        """Convert PyArrow Table to DuckDB relation.

        Args:
            table: PyArrow Table.

        Returns:
            DuckDB relation.
        """
        return self._conn.from_arrow(table)

    def select_columns(
        self, df: duckdb.DuckDBPyRelation, columns: list[str]
    ) -> duckdb.DuckDBPyRelation:
        """Select specific columns from a DuckDB relation.

        Args:
            df: DuckDB relation.
            columns: List of column names to select.

        Returns:
            DuckDB relation with only selected columns.
        """
        cols = ", ".join(f'"{c}"' for c in columns)
        return df.select(cols)

    def rename_columns(
        self, df: duckdb.DuckDBPyRelation, mapping: dict[str, str]
    ) -> duckdb.DuckDBPyRelation:
        """Rename columns in a DuckDB relation.

        Args:
            df: DuckDB relation.
            mapping: Dictionary of old_name -> new_name.

        Returns:
            DuckDB relation with renamed columns.
        """
        select_exprs = []
        for col in df.columns:
            if col in mapping:
                select_exprs.append(f'"{col}" AS "{mapping[col]}"')
            else:
                select_exprs.append(f'"{col}"')
        return df.select(", ".join(select_exprs))

    def sample(self, df: duckdb.DuckDBPyRelation, n: int) -> duckdb.DuckDBPyRelation:
        """Sample n rows from a DuckDB relation.

        Args:
            df: DuckDB relation.
            n: Number of rows to sample.

        Returns:
            DuckDB relation with sampled rows.
        """
        return df.limit(n)

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()

    def __enter__(self) -> DuckDBBackend:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()
