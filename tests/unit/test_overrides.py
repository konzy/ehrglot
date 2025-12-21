"""Tests for schema override system."""

import tempfile
from pathlib import Path

import pytest

from ehrglot.core.types import MaskingStrategy, PIILevel
from ehrglot.schemas.overrides import (
    OVERRIDABLE_PROPERTIES,
    FieldOverride,
    SchemaOverride,
    SchemaOverrideLoader,
)


class TestFieldOverride:
    """Tests for FieldOverride dataclass."""

    def test_create_empty_override(self) -> None:
        """Test creating an override with no values."""
        override = FieldOverride(field_name="test_field")
        assert override.field_name == "test_field"
        assert override.pii_level is None
        assert not override.has_overrides()

    def test_create_override_with_values(self) -> None:
        """Test creating an override with values."""
        override = FieldOverride(
            field_name="ssn",
            pii_level=PIILevel.CRITICAL,
            masking_strategy=MaskingStrategy.HASH,
        )
        assert override.has_overrides()
        assert override.pii_level == PIILevel.CRITICAL


class TestSchemaOverride:
    """Tests for SchemaOverride dataclass."""

    def test_get_field_override(self) -> None:
        """Test getting a field override."""
        field_override = FieldOverride(field_name="birthDate", pii_level=PIILevel.CRITICAL)
        schema_override = SchemaOverride(
            resource_name="Patient",
            field_overrides={"birthDate": field_override},
        )
        assert schema_override.get_field_override("birthDate") is field_override
        assert schema_override.get_field_override("missing") is None


class TestOverridableProperties:
    """Tests for overridable properties set."""

    def test_allowed_properties(self) -> None:
        """Test that expected properties are allowed."""
        assert "pii_level" in OVERRIDABLE_PROPERTIES
        assert "pii_category" in OVERRIDABLE_PROPERTIES
        assert "hipaa_identifier" in OVERRIDABLE_PROPERTIES
        assert "masking_strategy" in OVERRIDABLE_PROPERTIES
        assert "masking_params" in OVERRIDABLE_PROPERTIES

    def test_disallowed_properties(self) -> None:
        """Test that structural properties are NOT allowed."""
        assert "name" not in OVERRIDABLE_PROPERTIES
        assert "type" not in OVERRIDABLE_PROPERTIES
        assert "required" not in OVERRIDABLE_PROPERTIES
        assert "description" not in OVERRIDABLE_PROPERTIES


class TestSchemaOverrideLoader:
    """Tests for SchemaOverrideLoader."""

    def test_load_nonexistent_override(self) -> None:
        """Test loading override that doesn't exist returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = SchemaOverrideLoader(tmpdir)
            assert loader.load_fhir_override("nonexistent") is None

    def test_load_valid_override(self) -> None:
        """Test loading a valid override file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create override directory structure
            fhir_dir = Path(tmpdir) / "fhir_r4"
            fhir_dir.mkdir()

            # Write override file
            override_file = fhir_dir / "patient.yaml"
            override_file.write_text(
                """
resource: Patient
description: Test overrides
field_overrides:
  birthDate:
    pii_level: critical
    masking_strategy: redact
  address:
    pii_level: high
    masking_params:
      keep_state: true
"""
            )

            loader = SchemaOverrideLoader(tmpdir)
            override = loader.load_fhir_override("patient")

            assert override is not None
            assert override.resource_name == "patient"
            assert override.description == "Test overrides"

            birth_date = override.get_field_override("birthDate")
            assert birth_date is not None
            assert birth_date.pii_level == PIILevel.CRITICAL
            assert birth_date.masking_strategy == MaskingStrategy.REDACT

            address = override.get_field_override("address")
            assert address is not None
            assert address.pii_level == PIILevel.HIGH
            assert address.masking_params == {"keep_state": True}

    def test_invalid_property_raises_error(self) -> None:
        """Test that invalid override properties raise ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fhir_dir = Path(tmpdir) / "fhir_r4"
            fhir_dir.mkdir()

            override_file = fhir_dir / "patient.yaml"
            override_file.write_text(
                """
field_overrides:
  birthDate:
    pii_level: critical
    name: invalid_name_change  # NOT ALLOWED
"""
            )

            loader = SchemaOverrideLoader(tmpdir)
            with pytest.raises(ValueError, match="Invalid override properties"):
                loader.load_fhir_override("patient")

    def test_list_overrides(self) -> None:
        """Test listing available override files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create some override files
            fhir_dir = Path(tmpdir) / "fhir_r4"
            fhir_dir.mkdir()
            (fhir_dir / "patient.yaml").write_text("field_overrides: {}")
            (fhir_dir / "observation.yaml").write_text("field_overrides: {}")

            loader = SchemaOverrideLoader(tmpdir)
            overrides = loader.list_overrides()

            assert len(overrides) == 2
            assert "fhir_r4/patient.yaml" in overrides
            assert "fhir_r4/observation.yaml" in overrides
