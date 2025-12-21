"""Databricks Unity Catalog masking policy generator."""

from __future__ import annotations

from typing import Any

from ehrglot.core.types import DataType, MaskingStrategy
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator, MaskingRule


class DatabricksMaskingGenerator(MaskingPolicyGenerator):
    """Generates Databricks Unity Catalog column masking policies.

    Databricks uses SQL functions with row filters and column masks
    applied through Unity Catalog for data governance.

    Reference: https://docs.databricks.com/en/data-governance/unity-catalog/column-masking.html
    """

    # Databricks-specific default roles (groups)
    DEFAULT_FULL_ACCESS_ROLES = ["phi_admin", "clinical_admin"]
    DEFAULT_PARTIAL_ACCESS_ROLES = ["analyst", "data_scientist"]

    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return "databricks"

    def _get_sql_type(self, data_type: DataType) -> str:
        """Map DataType to Databricks SQL type."""
        type_map = {
            DataType.STRING: "STRING",
            DataType.INTEGER: "BIGINT",
            DataType.FLOAT: "DOUBLE",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.DATETIME: "TIMESTAMP",
            DataType.TIMESTAMP: "TIMESTAMP",
            DataType.BINARY: "BINARY",
        }
        return type_map.get(data_type, "STRING")

    def _function_name(self, table_name: str, column_name: str) -> str:
        """Generate a masking function name for a column."""
        return f"mask_{table_name}_{column_name}".lower()

    def generate_mask_expression(
        self,
        column_name: str,
        data_type: str,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Generate Databricks masking expression.

        Args:
            column_name: Name of the column parameter.
            data_type: Databricks data type.
            strategy: Masking strategy.
            params: Strategy parameters.

        Returns:
            SQL expression for masking.
        """
        if strategy == MaskingStrategy.REDACT:
            replacement = params.get("replacement", "***REDACTED***")
            if data_type in ("BIGINT", "INT", "DOUBLE", "FLOAT"):
                return "NULL"
            return f"'{replacement}'"

        elif strategy == MaskingStrategy.HASH:
            params.get("algorithm", "sha256")
            truncate = params.get("truncate", 0)
            if truncate:
                return f"LEFT(SHA2({column_name}, 256), {truncate})"
            return f"SHA2({column_name}, 256)"

        elif strategy == MaskingStrategy.PARTIAL:
            show_last = params.get("show_last", 4)
            mask_char = params.get("mask_char", "X")
            return f"CONCAT(REPEAT('{mask_char}', GREATEST(LENGTH({column_name}) - {show_last}, 0)), RIGHT({column_name}, {show_last}))"

        elif strategy == MaskingStrategy.GENERALIZE:
            if data_type == "DATE" or data_type == "TIMESTAMP":
                precision = params.get("precision", "year")
                if precision == "year":
                    return f"DATE_TRUNC('YEAR', {column_name})"
                elif precision == "month":
                    return f"DATE_TRUNC('MONTH', {column_name})"
                elif precision == "day":
                    return f"DATE_TRUNC('DAY', {column_name})"
            if params.get("mask_octets"):
                octets = params.get("mask_octets", 2)
                if octets == 2:
                    return f"REGEXP_REPLACE({column_name}, '\\\\.[0-9]+\\\\.[0-9]+$', '.XXX.XXX')"
            return column_name

        elif strategy == MaskingStrategy.TOKENIZE:
            return f"CONCAT('TKN_', LEFT(SHA2({column_name}, 256), 16))"

        elif strategy == MaskingStrategy.SUPPRESS:
            return "NULL"

        return column_name

    def _generate_function_sql(
        self,
        function_name: str,
        column_name: str,
        data_type: str,
        rule: MaskingRule,
        catalog_name: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        """Generate CREATE FUNCTION statement for masking."""
        full_mask = self.generate_mask_expression(
            column_name, data_type, rule.strategy, rule.params
        )
        partial_mask = self.generate_mask_expression(
            column_name, data_type, MaskingStrategy.PARTIAL, {"show_last": 4}
        )

        # Build function qualified name
        func_parts = []
        if catalog_name:
            func_parts.append(catalog_name)
        if schema_name:
            func_parts.append(schema_name)
        func_parts.append(function_name)
        fq_function = ".".join(func_parts)

        # Build is_member checks
        full_checks = " OR ".join(f"is_member('{r}')" for r in rule.full_access_roles)
        partial_checks = " OR ".join(f"is_member('{r}')" for r in rule.partial_access_roles)

        return f"""CREATE OR REPLACE FUNCTION {fq_function}({column_name} {data_type})
RETURNS {data_type}
RETURN CASE
  WHEN {full_checks} THEN {column_name}
  WHEN {partial_checks} THEN {partial_mask}
  ELSE {full_mask}
END;"""

    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate Databricks Unity Catalog masking SQL.

        Args:
            policy: Masking policy to generate.

        Returns:
            GeneratedSQL with CREATE FUNCTION and ALTER TABLE statements.
        """
        create_statements: list[str] = []
        apply_statements: list[str] = []
        drop_statements: list[str] = []
        comments: list[str] = []

        # Build fully qualified table name
        table_parts = []
        catalog_name = policy.database_name  # In Databricks, this is often the catalog
        if catalog_name:
            table_parts.append(catalog_name)
        if policy.schema_name:
            table_parts.append(policy.schema_name)
        table_parts.append(policy.table_name)
        fq_table = ".".join(table_parts)

        comments.append(f"Table: {fq_table}")
        comments.append(f"Columns with masking: {len(policy.rules)}")
        comments.append("Platform: Databricks Unity Catalog")

        for rule in policy.rules:
            function_name = self._function_name(policy.table_name, rule.column_name)
            data_type = "STRING"  # Default, could be enhanced

            # Generate masking function
            func_sql = self._generate_function_sql(
                function_name,
                rule.column_name,
                data_type,
                rule,
                catalog_name,
                policy.schema_name,
            )
            create_statements.append(func_sql)
            create_statements.append("")

            # Build function qualified name for ALTER
            func_parts = []
            if catalog_name:
                func_parts.append(catalog_name)
            if policy.schema_name:
                func_parts.append(policy.schema_name)
            func_parts.append(function_name)
            fq_function = ".".join(func_parts)

            # Generate column mask application
            apply_sql = (
                f"ALTER TABLE {fq_table} ALTER COLUMN {rule.column_name} SET MASK {fq_function};"
            )
            apply_statements.append(apply_sql)

            # Generate drop statement
            drop_sql = f"DROP FUNCTION IF EXISTS {fq_function};"
            drop_statements.append(drop_sql)

        return GeneratedSQL(
            create_statements=create_statements,
            apply_statements=apply_statements,
            drop_statements=drop_statements,
            comments=comments,
        )

    def generate_row_filter(
        self,
        table_name: str,
        filter_column: str,
        allowed_values: list[str],
        schema_name: str | None = None,
        catalog_name: str | None = None,
    ) -> str:
        """Generate a row-level security filter for the table.

        Args:
            table_name: Table to filter.
            filter_column: Column to use for filtering.
            allowed_values: Values that should be visible.
            schema_name: Optional schema name.
            catalog_name: Optional catalog name.

        Returns:
            SQL to create and apply row filter.
        """
        # Build qualified names
        parts = [p for p in [catalog_name, schema_name, table_name] if p]
        fq_table = ".".join(parts)

        filter_name = f"filter_{table_name}_{filter_column}".lower()
        filter_parts = [p for p in [catalog_name, schema_name, filter_name] if p]
        fq_filter = ".".join(filter_parts)

        values_str = ", ".join(f"'{v}'" for v in allowed_values)

        return f"""-- Row-level security filter
CREATE OR REPLACE FUNCTION {fq_filter}({filter_column} STRING)
RETURNS BOOLEAN
RETURN
  is_member('phi_admin')
  OR {filter_column} IN ({values_str});

ALTER TABLE {fq_table} SET ROW FILTER {fq_filter} ON ({filter_column});
"""

    def generate_audit_log_table(
        self,
        schema_name: str,
        catalog_name: str | None = None,
    ) -> str:
        """Generate audit logging table for PHI access.

        Args:
            schema_name: Schema for the audit table.
            catalog_name: Optional catalog name.

        Returns:
            SQL to create audit logging infrastructure.
        """
        prefix = f"{catalog_name}." if catalog_name else ""

        return f"""-- PHI Access Audit Log
CREATE TABLE IF NOT EXISTS {prefix}{schema_name}.phi_access_audit (
    audit_id STRING DEFAULT UUID(),
    table_name STRING,
    column_name STRING,
    access_user STRING DEFAULT CURRENT_USER(),
    access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    access_type STRING,
    row_count BIGINT,
    query_id STRING
);

-- Create view for recent PHI access
CREATE OR REPLACE VIEW {prefix}{schema_name}.phi_access_summary AS
SELECT
    table_name,
    column_name,
    access_user,
    access_type,
    COUNT(*) as access_count,
    MAX(access_time) as last_access
FROM {prefix}{schema_name}.phi_access_audit
WHERE access_time > CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY table_name, column_name, access_user, access_type
ORDER BY last_access DESC;
"""
