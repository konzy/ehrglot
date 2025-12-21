"""Tests for masking policy generators."""

import pytest

from ehrglot.core.types import (
    ColumnMetadata,
    DataType,
    MaskingStrategy,
    PIILevel,
    SchemaDefinition,
)
from ehrglot.masking.base import MaskingRule
from ehrglot.masking.databricks import DatabricksMaskingGenerator
from ehrglot.masking.snowflake import SnowflakeMaskingGenerator


class TestMaskingRule:
    """Tests for MaskingRule dataclass."""

    def test_create_rule(self) -> None:
        """Test creating a masking rule."""
        rule = MaskingRule(
            column_name="ssn",
            strategy=MaskingStrategy.PARTIAL,
            params={"show_last": 4},
            full_access_roles=["PHI_ADMIN"],
        )
        assert rule.column_name == "ssn"
        assert rule.strategy == MaskingStrategy.PARTIAL
        assert rule.params["show_last"] == 4


class TestSnowflakeMaskingGenerator:
    """Tests for Snowflake masking policy generator."""

    @pytest.fixture
    def generator(self) -> SnowflakeMaskingGenerator:
        """Create generator instance."""
        return SnowflakeMaskingGenerator()

    @pytest.fixture
    def sample_schema(self) -> SchemaDefinition:
        """Create sample schema with PII columns."""
        return SchemaDefinition(
            name="Patient",
            version="1.0",
            columns=[
                ColumnMetadata(
                    name="id",
                    data_type=DataType.STRING,
                    pii_level=PIILevel.NONE,
                ),
                ColumnMetadata(
                    name="ssn",
                    data_type=DataType.STRING,
                    pii_level=PIILevel.CRITICAL,
                    masking_strategy=MaskingStrategy.PARTIAL,
                    masking_params={"show_last": 4},
                ),
                ColumnMetadata(
                    name="email",
                    data_type=DataType.STRING,
                    pii_level=PIILevel.CRITICAL,
                    masking_strategy=MaskingStrategy.HASH,
                ),
            ],
        )

    def test_platform_name(self, generator: SnowflakeMaskingGenerator) -> None:
        """Test platform name."""
        assert generator.platform_name == "snowflake"

    def test_generate_redact_mask(self, generator: SnowflakeMaskingGenerator) -> None:
        """Test generating redact mask expression."""
        expr = generator.generate_mask_expression(
            "name", "VARCHAR", MaskingStrategy.REDACT, {"replacement": "***"}
        )
        assert "***" in expr

    def test_generate_partial_mask(self, generator: SnowflakeMaskingGenerator) -> None:
        """Test generating partial mask expression."""
        expr = generator.generate_mask_expression(
            "ssn", "VARCHAR", MaskingStrategy.PARTIAL, {"show_last": 4}
        )
        assert "RIGHT" in expr
        assert "4" in expr

    def test_generate_hash_mask(self, generator: SnowflakeMaskingGenerator) -> None:
        """Test generating hash mask expression."""
        expr = generator.generate_mask_expression("email", "VARCHAR", MaskingStrategy.HASH, {})
        assert "SHA2" in expr

    def test_create_policy_from_schema(
        self, generator: SnowflakeMaskingGenerator, sample_schema: SchemaDefinition
    ) -> None:
        """Test creating policy from schema."""
        policy = generator.create_policy_from_schema(
            sample_schema, table_name="patients", schema_name="healthcare"
        )
        assert policy.table_name == "patients"
        assert policy.schema_name == "healthcare"
        assert len(policy.rules) == 2  # ssn and email

    def test_generate_sql(
        self, generator: SnowflakeMaskingGenerator, sample_schema: SchemaDefinition
    ) -> None:
        """Test generating SQL statements."""
        policy = generator.create_policy_from_schema(sample_schema, table_name="patients")
        sql = generator.generate_sql(policy)

        assert len(sql.create_statements) > 0
        assert len(sql.apply_statements) == 2
        assert len(sql.drop_statements) == 2

        # Check SQL content
        script = sql.to_script()
        assert "CREATE OR REPLACE MASKING POLICY" in script
        assert "SET MASKING POLICY" in script
        assert "CURRENT_ROLE()" in script


class TestDatabricksMaskingGenerator:
    """Tests for Databricks masking policy generator."""

    @pytest.fixture
    def generator(self) -> DatabricksMaskingGenerator:
        """Create generator instance."""
        return DatabricksMaskingGenerator()

    @pytest.fixture
    def sample_schema(self) -> SchemaDefinition:
        """Create sample schema with PII columns."""
        return SchemaDefinition(
            name="Patient",
            version="1.0",
            columns=[
                ColumnMetadata(
                    name="id",
                    data_type=DataType.STRING,
                    pii_level=PIILevel.NONE,
                ),
                ColumnMetadata(
                    name="ssn",
                    data_type=DataType.STRING,
                    pii_level=PIILevel.CRITICAL,
                    masking_strategy=MaskingStrategy.PARTIAL,
                    masking_params={"show_last": 4},
                ),
            ],
        )

    def test_platform_name(self, generator: DatabricksMaskingGenerator) -> None:
        """Test platform name."""
        assert generator.platform_name == "databricks"

    def test_generate_mask_expression(self, generator: DatabricksMaskingGenerator) -> None:
        """Test generating mask expression."""
        expr = generator.generate_mask_expression(
            "ssn", "STRING", MaskingStrategy.PARTIAL, {"show_last": 4}
        )
        assert "RIGHT" in expr
        assert "ssn" in expr

    def test_generate_sql(
        self, generator: DatabricksMaskingGenerator, sample_schema: SchemaDefinition
    ) -> None:
        """Test generating SQL statements."""
        policy = generator.create_policy_from_schema(sample_schema, table_name="patients")
        sql = generator.generate_sql(policy)

        assert len(sql.create_statements) > 0
        assert len(sql.apply_statements) == 1  # Only ssn has masking

        script = sql.to_script()
        assert "CREATE OR REPLACE FUNCTION" in script
        assert "SET MASK" in script
        assert "is_member" in script
