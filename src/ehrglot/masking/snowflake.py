"""Snowflake Dynamic Data Masking policy generator."""

from __future__ import annotations

from typing import Any

from ehrglot.core.types import DataType, MaskingStrategy
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator, MaskingRule


class SnowflakeMaskingGenerator(MaskingPolicyGenerator):
    """Generates Snowflake Dynamic Data Masking (DDM) policies.

    Snowflake DDM applies masking at query time based on user roles.
    Policies are created as schema-level objects and bound to columns.

    Reference: https://docs.snowflake.com/en/user-guide/security-column-ddm
    """

    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return "snowflake"

    def _get_sql_type(self, data_type: DataType) -> str:
        """Map DataType to Snowflake SQL type."""
        type_map = {
            DataType.STRING: "VARCHAR",
            DataType.INTEGER: "NUMBER",
            DataType.FLOAT: "FLOAT",
            DataType.BOOLEAN: "BOOLEAN",
            DataType.DATE: "DATE",
            DataType.DATETIME: "TIMESTAMP_NTZ",
            DataType.TIMESTAMP: "TIMESTAMP_NTZ",
            DataType.BINARY: "BINARY",
        }
        return type_map.get(data_type, "VARCHAR")

    def _policy_name(self, table_name: str, column_name: str) -> str:
        """Generate a policy name for a column."""
        return f"mask_{table_name}_{column_name}".upper()

    def generate_mask_expression(
        self,
        column_name: str,
        data_type: str,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Generate Snowflake masking expression.

        Args:
            column_name: Name of the column (used as 'val' parameter).
            data_type: Snowflake data type.
            strategy: Masking strategy.
            params: Strategy parameters.

        Returns:
            SQL CASE expression for masking.
        """
        if strategy == MaskingStrategy.REDACT:
            replacement = params.get("replacement", "***REDACTED***")
            if data_type in ("NUMBER", "FLOAT"):
                return "NULL"
            return f"'{replacement}'"

        elif strategy == MaskingStrategy.HASH:
            params.get("algorithm", "sha256").upper()
            truncate = params.get("truncate", 0)
            if truncate:
                return f"LEFT(SHA2(val, 256), {truncate})"
            return "SHA2(val, 256)"

        elif strategy == MaskingStrategy.PARTIAL:
            show_last = params.get("show_last", 4)
            mask_char = params.get("mask_char", "X")
            return f"CONCAT(REPEAT('{mask_char}', GREATEST(LENGTH(val) - {show_last}, 0)), RIGHT(val, {show_last}))"

        elif strategy == MaskingStrategy.GENERALIZE:
            if data_type == "DATE" or data_type.startswith("TIMESTAMP"):
                precision = params.get("precision", "year")
                if precision == "year":
                    return "DATE_TRUNC('YEAR', val)"
                elif precision == "month":
                    return "DATE_TRUNC('MONTH', val)"
                elif precision == "day":
                    return "DATE_TRUNC('DAY', val)"
            # For other types, return truncated/generalized value
            if params.get("keep_state"):
                return "REGEXP_REPLACE(val, '.*,\\s*', '')"  # Keep state from address
            if params.get("mask_octets"):
                octets = params.get("mask_octets", 2)
                if octets == 2:
                    return "REGEXP_REPLACE(val, '\\.[0-9]+\\.[0-9]+$', '.XXX.XXX')"
            return "val"

        elif strategy == MaskingStrategy.TOKENIZE:
            # Use a deterministic hash for tokenization (reversible via lookup)
            return "CONCAT('TKN_', LEFT(SHA2(val, 256), 16))"

        elif strategy == MaskingStrategy.SUPPRESS:
            return "NULL"

        return "val"  # Default: no masking

    def _generate_policy_sql(
        self,
        policy_name: str,
        data_type: str,
        rule: MaskingRule,
    ) -> str:
        """Generate CREATE MASKING POLICY statement."""
        full_mask = self.generate_mask_expression(
            rule.column_name, data_type, rule.strategy, rule.params
        )
        partial_mask = self.generate_mask_expression(
            rule.column_name, data_type, MaskingStrategy.PARTIAL, {"show_last": 4}
        )

        # Build role check conditions
        full_roles = ", ".join(f"'{r}'" for r in rule.full_access_roles)
        partial_roles = ", ".join(f"'{r}'" for r in rule.partial_access_roles)

        return f"""CREATE OR REPLACE MASKING POLICY {policy_name}
AS (val {data_type})
RETURNS {data_type} ->
  CASE
    WHEN CURRENT_ROLE() IN ({full_roles}) THEN val
    WHEN CURRENT_ROLE() IN ({partial_roles}) THEN {partial_mask}
    ELSE {full_mask}
  END;"""

    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate Snowflake DDM SQL statements.

        Args:
            policy: Masking policy to generate.

        Returns:
            GeneratedSQL with CREATE, APPLY, and DROP statements.
        """
        create_statements: list[str] = []
        apply_statements: list[str] = []
        drop_statements: list[str] = []
        comments: list[str] = []

        # Build fully qualified table name
        table_parts = []
        if policy.database_name:
            table_parts.append(policy.database_name)
        if policy.schema_name:
            table_parts.append(policy.schema_name)
        table_parts.append(policy.table_name)
        fq_table = ".".join(table_parts)

        comments.append(f"Table: {fq_table}")
        comments.append(f"Columns with masking: {len(policy.rules)}")

        for rule in policy.rules:
            policy_name = self._policy_name(policy.table_name, rule.column_name)

            # Determine data type (default to VARCHAR)
            data_type = "VARCHAR"  # Could be enhanced to look up from schema

            # Generate policy creation
            create_sql = self._generate_policy_sql(policy_name, data_type, rule)
            create_statements.append(create_sql)
            create_statements.append("")

            # Generate policy application
            apply_sql = f"ALTER TABLE {fq_table} MODIFY COLUMN {rule.column_name} SET MASKING POLICY {policy_name};"
            apply_statements.append(apply_sql)

            # Generate drop statement
            drop_sql = f"DROP MASKING POLICY IF EXISTS {policy_name};"
            drop_statements.append(drop_sql)

        return GeneratedSQL(
            create_statements=create_statements,
            apply_statements=apply_statements,
            drop_statements=drop_statements,
            comments=comments,
        )

    def generate_audit_policy(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> str:
        """Generate Snowflake audit logging for PHI access.

        Args:
            table_name: Table to audit.
            schema_name: Optional schema name.

        Returns:
            SQL to enable access history tracking.
        """

        return f"""-- Enable access history for PHI audit
ALTER ACCOUNT SET ENABLE_ACCOUNT_USAGE_FOR_PRIVACY_INFORMATION = TRUE;

-- Create view for PHI access audit
CREATE OR REPLACE VIEW {schema_name or 'PUBLIC'}.phi_access_log AS
SELECT
    query_id,
    user_name,
    role_name,
    query_text,
    start_time,
    end_time
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE CONTAINS(query_text, '{table_name}')
  AND start_time > DATEADD(day, -90, CURRENT_TIMESTAMP())
ORDER BY start_time DESC;
"""
