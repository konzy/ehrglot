"""Azure Synapse Analytics Dynamic Data Masking policy generator."""

from __future__ import annotations

from typing import Any

from ehrglot.core.types import MaskingStrategy
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator


class SynapseMaskingGenerator(MaskingPolicyGenerator):
    """Generates Azure Synapse Analytics Dynamic Data Masking policies.

    Synapse uses SQL Server-style DDM with built-in masking functions
    and role-based exemptions.

    Reference: https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-dynamic-data-masking
    """

    # Azure-specific default roles
    DEFAULT_FULL_ACCESS_ROLES = ["db_owner", "phi_admin"]
    DEFAULT_PARTIAL_ACCESS_ROLES = ["analyst", "researcher"]

    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return "synapse"

    def _get_masking_function(
        self,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Get Synapse DDM masking function.

        Synapse supports these built-in masking functions:
        - default(): Full masking based on data type
        - email(): Shows first letter and domain
        - random(start, end): Random number in range
        - partial(prefix, padding, suffix): Custom partial mask
        """
        if strategy == MaskingStrategy.REDACT:
            return "default()"

        elif strategy == MaskingStrategy.PARTIAL:
            show_first = params.get("show_first", 0)
            show_last = params.get("show_last", 4)
            mask_char = params.get("mask_char", "X")
            padding_count = params.get("padding", 4)
            padding = mask_char * padding_count
            return f"partial({show_first}, '{padding}', {show_last})"

        elif strategy == MaskingStrategy.HASH:
            # Synapse DDM doesn't have native hash, use partial with long mask
            return "partial(0, 'XXXXXXXXXXXXXXXX', 0)"

        elif strategy == MaskingStrategy.GENERALIZE:
            # For emails, use email() function
            return "email()"

        elif strategy == MaskingStrategy.SUPPRESS:
            return "default()"

        elif strategy == MaskingStrategy.TOKENIZE:
            return "partial(0, 'TKN_XXXXXXXXXXXX', 0)"

        return "default()"

    def generate_mask_expression(
        self,
        column_name: str,
        data_type: str,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Generate Synapse masking function call.

        Args:
            column_name: Name of the column.
            data_type: SQL data type.
            strategy: Masking strategy.
            params: Strategy parameters.

        Returns:
            Masking function string.
        """
        return self._get_masking_function(strategy, params)

    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate Azure Synapse DDM SQL statements.

        Args:
            policy: Masking policy to generate.

        Returns:
            GeneratedSQL with ALTER TABLE and GRANT statements.
        """
        create_statements: list[str] = []
        apply_statements: list[str] = []
        drop_statements: list[str] = []
        grant_statements: list[str] = []
        comments: list[str] = []

        schema = policy.schema_name or "dbo"
        table = policy.table_name

        comments.append(f"Schema: {schema}")
        comments.append(f"Table: {table}")
        comments.append(f"Columns with masking: {len(policy.rules)}")
        comments.append("Platform: Azure Synapse Analytics")

        for rule in policy.rules:
            mask_function = self._get_masking_function(rule.strategy, rule.params)

            # Add masking to column
            apply_statements.append(
                f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{rule.column_name}] "
                f"ADD MASKED WITH (FUNCTION = '{mask_function}');"
            )

            # Drop masking
            drop_statements.append(
                f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{rule.column_name}] DROP MASKED;"
            )

        # Grant unmask permission to privileged roles
        for role in self.full_access_roles:
            grant_statements.append(f"GRANT UNMASK TO [{role}];")

        # Create roles if they don't exist
        create_statements.append("-- Create roles for PHI access")
        create_statements.append("""IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'phi_admin')
    CREATE ROLE [phi_admin];""")
        create_statements.append("""IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'analyst')
    CREATE ROLE [analyst];""")
        create_statements.append("")

        # Grant table access
        grant_statements.append("")
        grant_statements.append("-- Grant table access")
        grant_statements.append(f"GRANT SELECT ON [{schema}].[{table}] TO [phi_admin];")
        grant_statements.append(f"GRANT SELECT ON [{schema}].[{table}] TO [analyst];")

        return GeneratedSQL(
            create_statements=create_statements,
            apply_statements=apply_statements,
            drop_statements=drop_statements,
            grant_statements=grant_statements,
            comments=comments,
        )

    def generate_security_policy(
        self,
        table_name: str,
        filter_column: str,
        schema_name: str = "dbo",
    ) -> str:
        """Generate Synapse row-level security policy.

        Args:
            table_name: Table to apply RLS.
            filter_column: Column to filter on.
            schema_name: Schema name.

        Returns:
            SQL to create RLS policy.
        """
        return f"""-- Create security predicate function
CREATE FUNCTION [{schema_name}].[fn_securitypredicate_{table_name}](@{filter_column} AS NVARCHAR(256))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS result
WHERE
    IS_MEMBER('phi_admin') = 1
    OR @{filter_column} = USER_NAME();
GO

-- Create security policy
CREATE SECURITY POLICY [{schema_name}].[SecurityPolicy_{table_name}]
ADD FILTER PREDICATE [{schema_name}].[fn_securitypredicate_{table_name}]([{filter_column}])
ON [{schema_name}].[{table_name}]
WITH (STATE = ON);
GO
"""
