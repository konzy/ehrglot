"""Base masking policy definitions and interfaces."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from ehrglot.core.types import MaskingStrategy, SchemaDefinition


@dataclass
class MaskingRule:
    """A masking rule for a specific column."""

    column_name: str
    strategy: MaskingStrategy
    params: dict[str, Any] = field(default_factory=dict)

    # Role-based access control
    full_access_roles: list[str] = field(default_factory=list)
    partial_access_roles: list[str] = field(default_factory=list)
    no_access_roles: list[str] = field(default_factory=list)


@dataclass
class MaskingPolicy:
    """A complete masking policy for a table."""

    table_name: str
    schema_name: str | None = None
    database_name: str | None = None
    rules: list[MaskingRule] = field(default_factory=list)
    description: str = ""


@dataclass
class GeneratedSQL:
    """Generated SQL statements for applying masking policies."""

    create_statements: list[str]
    apply_statements: list[str]
    drop_statements: list[str]
    grant_statements: list[str] = field(default_factory=list)
    comments: list[str] = field(default_factory=list)

    def to_script(self, include_drops: bool = False) -> str:
        """Generate a complete SQL script.

        Args:
            include_drops: Include DROP statements before CREATE.

        Returns:
            Complete SQL script as a string.
        """
        parts = []

        # Header comment
        parts.append("-- EHRglot Generated Masking Policies")
        parts.append("-- " + "=" * 50)
        parts.append("")

        if self.comments:
            for comment in self.comments:
                parts.append(f"-- {comment}")
            parts.append("")

        if include_drops and self.drop_statements:
            parts.append("-- Drop existing policies")
            parts.extend(self.drop_statements)
            parts.append("")

        if self.create_statements:
            parts.append("-- Create masking policies")
            parts.extend(self.create_statements)
            parts.append("")

        if self.apply_statements:
            parts.append("-- Apply policies to columns")
            parts.extend(self.apply_statements)
            parts.append("")

        if self.grant_statements:
            parts.append("-- Grant permissions")
            parts.extend(self.grant_statements)
            parts.append("")

        return "\n".join(parts)


class MaskingPolicyGenerator(ABC):
    """Abstract base class for masking policy generators."""

    # Default roles for healthcare environments
    DEFAULT_FULL_ACCESS_ROLES: list[str] = ["PHI_ADMIN", "CLINICAL_ADMIN"]
    DEFAULT_PARTIAL_ACCESS_ROLES: list[str] = ["ANALYST", "RESEARCHER"]
    DEFAULT_NO_ACCESS_ROLES: list[str] = ["PUBLIC", "VIEWER"]

    def __init__(
        self,
        full_access_roles: list[str] | None = None,
        partial_access_roles: list[str] | None = None,
    ) -> None:
        """Initialize generator with role configurations.

        Args:
            full_access_roles: Roles that can see unmasked data.
            partial_access_roles: Roles that see partially masked data.
        """
        self.full_access_roles = full_access_roles or self.DEFAULT_FULL_ACCESS_ROLES.copy()
        self.partial_access_roles = partial_access_roles or self.DEFAULT_PARTIAL_ACCESS_ROLES.copy()

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Return the platform name (e.g., 'snowflake', 'databricks')."""
        ...

    def create_policy_from_schema(
        self,
        schema: SchemaDefinition,
        table_name: str,
        schema_name: str | None = None,
        database_name: str | None = None,
    ) -> MaskingPolicy:
        """Create a masking policy from a tagged schema.

        Args:
            schema: Schema with PII tagging.
            table_name: Target table name.
            schema_name: Optional schema/namespace.
            database_name: Optional database name.

        Returns:
            MaskingPolicy with rules for all PII columns.
        """
        rules = []

        for column in schema.columns:
            if column.masking_strategy != MaskingStrategy.NONE:
                rule = MaskingRule(
                    column_name=column.name,
                    strategy=column.masking_strategy,
                    params=column.masking_params,
                    full_access_roles=self.full_access_roles.copy(),
                    partial_access_roles=self.partial_access_roles.copy(),
                )
                rules.append(rule)

        return MaskingPolicy(
            table_name=table_name,
            schema_name=schema_name,
            database_name=database_name,
            rules=rules,
            description=f"Masking policy for {schema.name}",
        )

    @abstractmethod
    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate SQL statements for the masking policy.

        Args:
            policy: Masking policy to generate SQL for.

        Returns:
            GeneratedSQL with all necessary statements.
        """
        ...

    @abstractmethod
    def generate_mask_expression(
        self,
        column_name: str,
        data_type: str,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Generate the masking expression for a specific strategy.

        Args:
            column_name: Name of the column.
            data_type: SQL data type of the column.
            strategy: Masking strategy to apply.
            params: Strategy-specific parameters.

        Returns:
            SQL expression for masking.
        """
        ...
