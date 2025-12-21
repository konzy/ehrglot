"""Microsoft Fabric data masking policy generator."""

from __future__ import annotations

from typing import Any

from ehrglot.core.types import MaskingStrategy
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator


class FabricMaskingGenerator(MaskingPolicyGenerator):
    """Generates Microsoft Fabric data protection policies.

    Microsoft Fabric uses OneLake security with sensitivity labels
    and column-level security through T-SQL compatible masking.

    Reference: https://learn.microsoft.com/en-us/fabric/security/
    """

    # Fabric-specific default roles (Entra ID groups)
    DEFAULT_FULL_ACCESS_ROLES = ["PHI_Admins", "Clinical_Staff"]
    DEFAULT_PARTIAL_ACCESS_ROLES = ["Analysts", "Researchers"]

    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return "fabric"

    def _get_masking_function(
        self,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Get Fabric DDM masking function.

        Fabric supports T-SQL style masking functions:
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
            return "partial(0, 'HASH_XXXXXXXXXXXXXX', 0)"

        elif strategy == MaskingStrategy.GENERALIZE:
            return "email()"  # For email-like data

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
        """Generate Fabric masking function call."""
        return self._get_masking_function(strategy, params)

    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate Microsoft Fabric data masking SQL.

        Args:
            policy: Masking policy to generate.

        Returns:
            GeneratedSQL with masking statements and sensitivity labels.
        """
        create_statements: list[str] = []
        apply_statements: list[str] = []
        drop_statements: list[str] = []
        grant_statements: list[str] = []
        comments: list[str] = []

        lakehouse = policy.database_name or "healthcare_lakehouse"
        schema = policy.schema_name or "dbo"
        table = policy.table_name

        comments.append(f"Lakehouse: {lakehouse}")
        comments.append(f"Schema: {schema}")
        comments.append(f"Table: {table}")
        comments.append(f"Columns with masking: {len(policy.rules)}")
        comments.append("Platform: Microsoft Fabric")

        # Create sensitivity label policy
        create_statements.append("-- Sensitivity Labels Configuration")
        create_statements.append("-- Configure in Microsoft Purview compliance portal:")
        create_statements.append("-- 1. Create sensitivity labels: PHI, Confidential, Internal")
        create_statements.append("-- 2. Configure auto-labeling policies for healthcare data")
        create_statements.append("-- 3. Enable sensitivity labels in Fabric workspace")
        create_statements.append("")

        for rule in policy.rules:
            mask_function = self._get_masking_function(rule.strategy, rule.params)

            # Add masking to column (T-SQL compatible)
            apply_statements.append(
                f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{rule.column_name}] "
                f"ADD MASKED WITH (FUNCTION = '{mask_function}');"
            )

            # Drop masking
            drop_statements.append(
                f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{rule.column_name}] DROP MASKED;"
            )

        # Fabric-specific security configuration
        create_statements.append("-- Workspace Security Configuration")
        create_statements.append("-- Run in Fabric Workspace Settings > Security:")
        create_statements.append("")

        # Grant access through Fabric workspace roles
        grant_statements.append("-- Fabric Workspace Permissions")
        grant_statements.append("-- Configure in Workspace Settings > Manage Access:")
        grant_statements.append(f"-- 1. Add {self.full_access_roles} as Admins/Members")
        grant_statements.append(f"-- 2. Add {self.partial_access_roles} as Contributors/Viewers")
        grant_statements.append("")

        # T-SQL grants for SQL endpoint
        grant_statements.append("-- SQL Endpoint Permissions")
        for role in self.full_access_roles:
            grant_statements.append(f"-- GRANT UNMASK TO [{role}];")
        grant_statements.append("")
        grant_statements.append(f"GRANT SELECT ON [{schema}].[{table}] TO [Analysts];")

        return GeneratedSQL(
            create_statements=create_statements,
            apply_statements=apply_statements,
            drop_statements=drop_statements,
            grant_statements=grant_statements,
            comments=comments,
        )

    def generate_onelake_security(
        self,
        workspace_name: str,
        lakehouse_name: str,
        table_name: str,
    ) -> str:
        """Generate OneLake security configuration.

        Args:
            workspace_name: Fabric workspace name.
            lakehouse_name: Lakehouse name.
            table_name: Table to secure.

        Returns:
            Configuration instructions for OneLake security.
        """
        return f"""# OneLake Security Configuration for {table_name}

## Workspace: {workspace_name}
## Lakehouse: {lakehouse_name}

### Step 1: Configure Workspace Roles
1. Navigate to Workspace Settings > Manage Access
2. Add Entra ID groups:
   - PHI_Admins: Admin role
   - Clinical_Staff: Member role
   - Analysts: Contributor role
   - Researchers: Viewer role

### Step 2: Enable Sensitivity Labels
1. Open Fabric Admin Portal
2. Navigate to Tenant Settings > Information Protection
3. Enable "Allow sensitivity labels for content"
4. Configure auto-labeling for PHI patterns

### Step 3: Configure OneLake Data Access
1. In Lakehouse settings, enable "OneLake data access roles"
2. Create custom roles:

```json
{{
  "roleName": "PHI_Full_Access",
  "permissions": ["read", "write"],
  "tablePermissions": {{
    "{table_name}": ["*"]
  }}
}}
```

### Step 4: Apply Row-Level Security (Optional)
```sql
CREATE FUNCTION dbo.fn_rls_{table_name}(@UserPrincipal NVARCHAR(256))
RETURNS TABLE
AS
RETURN
  SELECT 1 AS access
  WHERE IS_MEMBER('PHI_Admins') = 1
     OR @UserPrincipal = SUSER_SNAME();
```

### Step 5: Monitor Access
- Enable Fabric audit logs
- Configure alerts for PHI access patterns
- Review access reports in Purview
"""

    def generate_purview_classification(
        self,
        table_name: str,
        columns: list[str],
    ) -> str:
        """Generate Microsoft Purview classification rules.

        Args:
            table_name: Table to classify.
            columns: List of columns with PHI.

        Returns:
            Purview classification configuration.
        """
        column_list = "\n".join(f"    - {col}" for col in columns)

        return f"""# Microsoft Purview Classification Rules

## Asset: {table_name}

### Sensitive Information Types to Detect:
- U.S. Social Security Number (SSN)
- U.S. Individual Taxpayer Identification Number (ITIN)
- All Medical Terms (custom)
- Protected Health Information (PHI)

### Columns Requiring Classification:
{column_list}

### Auto-labeling Policy:
```yaml
name: PHI_Auto_Label_{table_name}
scope:
  - Microsoft Fabric
conditions:
  - contentContains:
      - sensitiveInfoTypes:
          - "U.S. Social Security Number"
          - "All Medical Terms"
actions:
  - applyLabel: "Highly Confidential/PHI"
  - notify:
      - "compliance@organization.com"
```
"""
