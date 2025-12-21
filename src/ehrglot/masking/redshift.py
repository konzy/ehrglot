"""Amazon Redshift Dynamic Data Masking policy generator."""

from __future__ import annotations

from typing import Any

from ehrglot.core.types import MaskingStrategy
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator, MaskingRule


class RedshiftMaskingGenerator(MaskingPolicyGenerator):
    """Generates Amazon Redshift Dynamic Data Masking policies.

    Redshift supports DDM through masking policies attached to columns,
    with role-based access control.

    Reference: https://docs.aws.amazon.com/redshift/latest/dg/t_ddm.html
    """

    # Redshift-specific default IAM roles
    DEFAULT_FULL_ACCESS_ROLES = ["phi_admin", "data_admin"]
    DEFAULT_PARTIAL_ACCESS_ROLES = ["analyst", "researcher"]

    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return "redshift"

    def generate_mask_expression(
        self,
        column_name: str,
        data_type: str,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Generate Redshift masking expression.

        Args:
            column_name: Name of the column.
            data_type: Redshift data type.
            strategy: Masking strategy.
            params: Strategy parameters.

        Returns:
            SQL expression for masking.
        """
        if strategy == MaskingStrategy.REDACT:
            replacement = params.get("replacement", "***REDACTED***")
            if data_type.upper() in ("INTEGER", "BIGINT", "SMALLINT", "DECIMAL", "FLOAT", "REAL"):
                return "NULL"
            return f"'{replacement}'"

        elif strategy == MaskingStrategy.HASH:
            truncate = params.get("truncate", 0)
            if truncate:
                return f"LEFT(MD5({column_name}::VARCHAR), {truncate})"
            return f"MD5({column_name}::VARCHAR)"

        elif strategy == MaskingStrategy.PARTIAL:
            show_last = params.get("show_last", 4)
            mask_char = params.get("mask_char", "X")
            return f"REPEAT('{mask_char}', GREATEST(LEN({column_name}) - {show_last}, 0)) || RIGHT({column_name}, {show_last})"

        elif strategy == MaskingStrategy.GENERALIZE:
            if data_type.upper() == "DATE" or "TIMESTAMP" in data_type.upper():
                precision = params.get("precision", "year")
                if precision == "year":
                    return f"DATE_TRUNC('year', {column_name})"
                elif precision == "month":
                    return f"DATE_TRUNC('month', {column_name})"
                elif precision == "day":
                    return f"DATE_TRUNC('day', {column_name})"
            return column_name

        elif strategy == MaskingStrategy.TOKENIZE:
            return f"'TKN_' || LEFT(MD5({column_name}::VARCHAR), 16)"

        elif strategy == MaskingStrategy.SUPPRESS:
            return "NULL"

        return column_name

    def _generate_masking_policy(
        self,
        policy_name: str,
        data_type: str,
        rule: MaskingRule,
    ) -> str:
        """Generate CREATE MASKING POLICY statement."""
        full_mask = self.generate_mask_expression("val", data_type, rule.strategy, rule.params)
        partial_mask = self.generate_mask_expression(
            "val", data_type, MaskingStrategy.PARTIAL, {"show_last": 4}
        )

        # Build role checks
        full_roles = ", ".join(f"'{r}'" for r in rule.full_access_roles)
        partial_roles = ", ".join(f"'{r}'" for r in rule.partial_access_roles)

        return f"""CREATE MASKING POLICY {policy_name}
WITH (val {data_type})
USING (
  CASE
    WHEN current_user_is_member({full_roles}) THEN val
    WHEN current_user_is_member({partial_roles}) THEN {partial_mask}
    ELSE {full_mask}
  END
);"""

    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate Redshift DDM SQL statements.

        Args:
            policy: Masking policy to generate.

        Returns:
            GeneratedSQL with CREATE, ATTACH, and DROP statements.
        """
        create_statements: list[str] = []
        apply_statements: list[str] = []
        drop_statements: list[str] = []
        grant_statements: list[str] = []
        comments: list[str] = []

        schema = policy.schema_name or "public"
        table = policy.table_name

        comments.append(f"Schema: {schema}")
        comments.append(f"Table: {table}")
        comments.append(f"Columns with masking: {len(policy.rules)}")
        comments.append("Platform: Amazon Redshift")

        for rule in policy.rules:
            policy_name = f"mask_{table}_{rule.column_name}".lower()
            data_type = "VARCHAR"  # Default

            # Create masking policy
            create_sql = self._generate_masking_policy(policy_name, data_type, rule)
            create_statements.append(create_sql)
            create_statements.append("")

            # Attach policy to column
            apply_statements.append(
                f"ATTACH MASKING POLICY {policy_name} ON {schema}.{table}({rule.column_name}) TO PUBLIC;"
            )

            # Drop statement
            drop_statements.append(
                f"DETACH MASKING POLICY {policy_name} ON {schema}.{table}({rule.column_name}) FROM PUBLIC;"
            )
            drop_statements.append(f"DROP MASKING POLICY IF EXISTS {policy_name};")

        # Grant role memberships
        grant_statements.append("-- Create roles for PHI access")
        grant_statements.append("CREATE ROLE phi_admin;")
        grant_statements.append("CREATE ROLE analyst;")
        grant_statements.append("")
        grant_statements.append("-- Grant schema access")
        grant_statements.append(f"GRANT USAGE ON SCHEMA {schema} TO phi_admin, analyst;")
        grant_statements.append(f"GRANT SELECT ON {schema}.{table} TO phi_admin, analyst;")

        return GeneratedSQL(
            create_statements=create_statements,
            apply_statements=apply_statements,
            drop_statements=drop_statements,
            grant_statements=grant_statements,
            comments=comments,
        )

    def generate_row_level_security(
        self,
        table_name: str,
        policy_name: str,
        filter_column: str,
        schema_name: str = "public",
    ) -> str:
        """Generate Redshift row-level security policy.

        Args:
            table_name: Table to apply RLS.
            policy_name: Name for the RLS policy.
            filter_column: Column to filter on.
            schema_name: Schema name.

        Returns:
            SQL to create RLS policy.
        """
        return f"""-- Create RLS policy
CREATE RLS POLICY {policy_name}
WITH ({filter_column} VARCHAR(256))
USING (
  current_user_is_member('phi_admin')
  OR {filter_column} = current_user_name()
);

-- Attach RLS policy
ALTER TABLE {schema_name}.{table_name} ROW LEVEL SECURITY ON;
ATTACH RLS POLICY {policy_name} ON {schema_name}.{table_name} TO PUBLIC;
"""
