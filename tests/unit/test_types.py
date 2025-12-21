"""Tests for core types."""

from ehrglot.core.types import (
    ColumnMetadata,
    DataType,
    HIPAAIdentifier,
    MaskingStrategy,
    PIICategory,
    PIILevel,
    SchemaDefinition,
)


class TestPIILevel:
    """Tests for PIILevel enum."""

    def test_pii_levels_exist(self) -> None:
        """Verify all PII levels are defined."""
        assert PIILevel.NONE.value == "none"
        assert PIILevel.LOW.value == "low"
        assert PIILevel.MEDIUM.value == "medium"
        assert PIILevel.HIGH.value == "high"
        assert PIILevel.CRITICAL.value == "critical"

    def test_pii_level_comparison(self) -> None:
        """Verify PII levels can be compared."""
        levels = [PIILevel.NONE, PIILevel.LOW, PIILevel.MEDIUM, PIILevel.HIGH, PIILevel.CRITICAL]
        # Higher index = higher sensitivity
        for _i, level in enumerate(levels):
            assert level in levels


class TestHIPAAIdentifier:
    """Tests for HIPAA Safe Harbor identifiers."""

    def test_all_18_identifiers_exist(self) -> None:
        """Verify all 18 HIPAA Safe Harbor identifiers are defined."""
        identifiers = list(HIPAAIdentifier)
        assert len(identifiers) == 18

    def test_specific_identifiers(self) -> None:
        """Verify specific important identifiers."""
        assert HIPAAIdentifier.NAMES.value == "names"
        assert HIPAAIdentifier.SSN.value == "ssn"
        assert HIPAAIdentifier.MRN.value == "mrn"
        assert HIPAAIdentifier.EMAIL_ADDRESSES.value == "email_addresses"
        assert HIPAAIdentifier.IP_ADDRESSES.value == "ip_addresses"


class TestMaskingStrategy:
    """Tests for masking strategy enum."""

    def test_masking_strategies_exist(self) -> None:
        """Verify all masking strategies are defined."""
        assert MaskingStrategy.NONE.value == "none"
        assert MaskingStrategy.REDACT.value == "redact"
        assert MaskingStrategy.TOKENIZE.value == "tokenize"
        assert MaskingStrategy.HASH.value == "hash"
        assert MaskingStrategy.GENERALIZE.value == "generalize"
        assert MaskingStrategy.SUPPRESS.value == "suppress"
        assert MaskingStrategy.PARTIAL.value == "partial"


class TestColumnMetadata:
    """Tests for ColumnMetadata dataclass."""

    def test_create_basic_column(self) -> None:
        """Test creating a basic column."""
        col = ColumnMetadata(name="id", data_type=DataType.STRING)
        assert col.name == "id"
        assert col.data_type == DataType.STRING
        assert col.nullable is True
        assert col.pii_level == PIILevel.NONE

    def test_create_pii_column(self) -> None:
        """Test creating a column with PII metadata."""
        col = ColumnMetadata(
            name="ssn",
            data_type=DataType.STRING,
            nullable=False,
            pii_level=PIILevel.CRITICAL,
            pii_category=PIICategory.DIRECT_IDENTIFIER,
            hipaa_identifier=HIPAAIdentifier.SSN,
            masking_strategy=MaskingStrategy.PARTIAL,
            masking_params={"show_last": 4},
        )
        assert col.name == "ssn"
        assert col.pii_level == PIILevel.CRITICAL
        assert col.hipaa_identifier == HIPAAIdentifier.SSN
        assert col.masking_strategy == MaskingStrategy.PARTIAL
        assert col.masking_params == {"show_last": 4}


class TestSchemaDefinition:
    """Tests for SchemaDefinition dataclass."""

    def test_create_schema(self) -> None:
        """Test creating a schema definition."""
        columns = [
            ColumnMetadata(name="id", data_type=DataType.STRING),
            ColumnMetadata(name="name", data_type=DataType.STRING),
        ]
        schema = SchemaDefinition(
            name="Patient",
            version="R4",
            columns=columns,
            description="FHIR Patient resource",
        )
        assert schema.name == "Patient"
        assert schema.version == "R4"
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "id"
