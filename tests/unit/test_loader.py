"""Tests for schema loader with nested fields."""

import tempfile
from pathlib import Path

from ehrglot.core.types import MaskingStrategy, PIICategory, PIILevel
from ehrglot.schemas import SchemaLoader


class TestNestedFieldLoading:
    """Tests for loading schemas with nested BackboneElement fields."""

    def test_load_location_with_nested_fields(self) -> None:
        """Test loading location schema with nested position and hoursOfOperation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create schema directory structure
            fhir_dir = Path(tmpdir) / "fhir_r4"
            fhir_dir.mkdir()

            # Write a location schema with nested fields
            (fhir_dir / "location.yaml").write_text("""
resource: Location
version: R4
fields:
  - name: position
    type: BackboneElement
    description: GPS coordinates
    pii_level: medium
    pii_category: quasi_identifier
    masking_strategy: suppress
    fields:
      - name: longitude
        type: decimal
        required: true
      - name: latitude
        type: decimal
        required: true
      - name: altitude
        type: decimal
  - name: hoursOfOperation
    type: array<BackboneElement>
    description: Hours of operation
    fields:
      - name: daysOfWeek
        type: array<code>
      - name: allDay
        type: boolean
""")

            loader = SchemaLoader(tmpdir)
            schema = loader.load_fhir_resource("location")

            # Check position field has nested fields
            position = next(f for f in schema.fields if f.name == "position")
            assert position.fields is not None
            assert len(position.fields) == 3

            # Check that child fields inherit parent PII settings
            longitude = position.fields[0]
            assert longitude.name == "longitude"
            assert longitude.pii_level == PIILevel.MEDIUM
            assert longitude.pii_category == PIICategory.QUASI_IDENTIFIER
            assert longitude.masking_strategy == MaskingStrategy.SUPPRESS

            latitude = position.fields[1]
            assert latitude.pii_level == PIILevel.MEDIUM

            # Check hoursOfOperation has no PII (no inheritance)
            hours = next(f for f in schema.fields if f.name == "hoursOfOperation")
            assert hours.fields is not None
            assert len(hours.fields) == 2
            days_of_week = hours.fields[0]
            assert days_of_week.pii_level == PIILevel.NONE

    def test_child_can_override_parent_pii(self) -> None:
        """Test that child fields can override parent PII settings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            fhir_dir = Path(tmpdir) / "fhir_r4"
            fhir_dir.mkdir()

            (fhir_dir / "test.yaml").write_text("""
resource: Test
version: R4
fields:
  - name: parent
    type: BackboneElement
    pii_level: high
    masking_strategy: hash
    fields:
      - name: sensitive_child
        type: string
        description: Inherits parent settings
      - name: less_sensitive_child
        type: string
        pii_level: low
        masking_strategy: partial
        description: Overrides with different PII level
""")

            loader = SchemaLoader(tmpdir)
            schema = loader.load_fhir_resource("test")

            parent = schema.fields[0]
            assert parent.fields is not None
            assert len(parent.fields) == 2

            # First child inherits
            sensitive = parent.fields[0]
            assert sensitive.pii_level == PIILevel.HIGH
            assert sensitive.masking_strategy == MaskingStrategy.HASH

            # Second child overrides
            less_sensitive = parent.fields[1]
            assert less_sensitive.pii_level == PIILevel.LOW
            assert less_sensitive.masking_strategy == MaskingStrategy.PARTIAL
