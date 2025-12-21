"""Google BigQuery column-level security policy generator."""

from __future__ import annotations

from typing import Any

from ehrglot.core.types import MaskingStrategy
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator


class BigQueryMaskingGenerator(MaskingPolicyGenerator):
    """Generates BigQuery column-level security policies.

    BigQuery uses Data Masking with policy tags and the Data Catalog
    for fine-grained access control.

    Reference: https://cloud.google.com/bigquery/docs/column-data-masking
    """

    # BigQuery-specific default IAM roles
    DEFAULT_FULL_ACCESS_ROLES = ["roles/bigquery.dataViewer", "roles/bigquery.admin"]
    DEFAULT_PARTIAL_ACCESS_ROLES = ["roles/bigquery.maskedReader"]

    @property
    def platform_name(self) -> str:
        """Return the platform name."""
        return "bigquery"

    def _policy_tag_name(self, project: str, location: str, taxonomy: str, tag: str) -> str:
        """Generate fully qualified policy tag name."""
        return f"projects/{project}/locations/{location}/taxonomies/{taxonomy}/policyTags/{tag}"

    def generate_mask_expression(
        self,
        column_name: str,
        data_type: str,
        strategy: MaskingStrategy,
        params: dict[str, Any],
    ) -> str:
        """Generate BigQuery masking expression.

        Args:
            column_name: Name of the column.
            data_type: BigQuery data type.
            strategy: Masking strategy.
            params: Strategy parameters.

        Returns:
            SQL expression for masking.
        """
        if strategy == MaskingStrategy.REDACT:
            replacement = params.get("replacement", "***REDACTED***")
            if data_type in ("INT64", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
                return "NULL"
            return f"'{replacement}'"

        elif strategy == MaskingStrategy.HASH:
            truncate = params.get("truncate", 0)
            if truncate:
                return f"SUBSTR(TO_HEX(SHA256(CAST({column_name} AS STRING))), 1, {truncate})"
            return f"TO_HEX(SHA256(CAST({column_name} AS STRING)))"

        elif strategy == MaskingStrategy.PARTIAL:
            show_last = params.get("show_last", 4)
            mask_char = params.get("mask_char", "X")
            return f"CONCAT(REPEAT('{mask_char}', GREATEST(LENGTH({column_name}) - {show_last}, 0)), RIGHT({column_name}, {show_last}))"

        elif strategy == MaskingStrategy.GENERALIZE:
            if data_type == "DATE" or data_type.startswith("TIMESTAMP"):
                precision = params.get("precision", "year")
                if precision == "year":
                    return f"DATE_TRUNC({column_name}, YEAR)"
                elif precision == "month":
                    return f"DATE_TRUNC({column_name}, MONTH)"
                elif precision == "day":
                    return f"DATE_TRUNC({column_name}, DAY)"
            return column_name

        elif strategy == MaskingStrategy.TOKENIZE:
            return f"CONCAT('TKN_', SUBSTR(TO_HEX(SHA256(CAST({column_name} AS STRING))), 1, 16))"

        elif strategy == MaskingStrategy.SUPPRESS:
            return "NULL"

        return column_name

    def generate_sql(self, policy: MaskingPolicy) -> GeneratedSQL:
        """Generate BigQuery data masking SQL.

        Args:
            policy: Masking policy to generate.

        Returns:
            GeneratedSQL with taxonomy, policy tag, and masking rule statements.
        """
        create_statements: list[str] = []
        apply_statements: list[str] = []
        drop_statements: list[str] = []
        grant_statements: list[str] = []
        comments: list[str] = []

        project = policy.database_name or "your-project-id"
        dataset = policy.schema_name or "healthcare"
        location = "us"  # Default location

        comments.append(f"Project: {project}")
        comments.append(f"Dataset: {dataset}")
        comments.append(f"Table: {policy.table_name}")
        comments.append("Platform: Google BigQuery")

        # Create taxonomy
        taxonomy_name = f"phi_taxonomy_{policy.table_name}"
        create_statements.append(f"""-- Create policy taxonomy for PHI data
-- Run with: bq mk --taxonomy --location={location} \\
--   --description="PHI masking taxonomy for {policy.table_name}" \\
--   {taxonomy_name}
""")

        for rule in policy.rules:
            tag_name = f"mask_{rule.column_name}".lower()
            data_type = "STRING"

            # Create policy tag
            create_statements.append(f"""-- Create policy tag for {rule.column_name}
-- Run with: bq mk --policy_tag --taxonomy={taxonomy_name} \\
--   --description="Masking policy for {rule.column_name}" \\
--   {tag_name}
""")

            # Create data masking rule
            mask_expr = self.generate_mask_expression(
                rule.column_name, data_type, rule.strategy, rule.params
            )
            create_statements.append(f"""-- Create masking rule for {rule.column_name}
CREATE OR REPLACE MASKING RULE `{project}.{dataset}.mask_{rule.column_name}`
AS ({rule.column_name} STRING) RETURNS STRING AS (
  {mask_expr}
);
""")

            # Apply policy tag to column
            apply_statements.append(f"""-- Apply policy tag to {rule.column_name}
ALTER TABLE `{project}.{dataset}.{policy.table_name}`
ALTER COLUMN {rule.column_name}
SET OPTIONS (policy_tags = ['{self._policy_tag_name(project, location, taxonomy_name, tag_name)}']);
""")

            # Grant statements
            for role in rule.full_access_roles:
                grant_statements.append(
                    f"-- GRANT Fine grained reader on policy tag {tag_name} to {role}"
                )

        return GeneratedSQL(
            create_statements=create_statements,
            apply_statements=apply_statements,
            drop_statements=drop_statements,
            grant_statements=grant_statements,
            comments=comments,
        )

    def generate_authorized_views(
        self,
        source_table: str,
        view_name: str,
        project: str,
        dataset: str,
        columns_to_mask: dict[str, str],
    ) -> str:
        """Generate an authorized view with masking applied.

        Args:
            source_table: Source table name.
            view_name: Name for the masked view.
            project: GCP project ID.
            dataset: BigQuery dataset.
            columns_to_mask: Dict of column_name -> mask_expression.

        Returns:
            SQL to create authorized view.
        """
        select_parts = []
        for col, mask_expr in columns_to_mask.items():
            select_parts.append(f"  {mask_expr} AS {col}")

        select_clause = ",\n".join(select_parts)

        return f"""-- Create authorized view with masking
CREATE OR REPLACE VIEW `{project}.{dataset}.{view_name}` AS
SELECT
{select_clause},
  * EXCEPT({', '.join(columns_to_mask.keys())})
FROM `{project}.{dataset}.{source_table}`;

-- Grant access to the view
GRANT `roles/bigquery.dataViewer`
ON TABLE `{project}.{dataset}.{view_name}`
TO "group:analysts@example.com";
"""
