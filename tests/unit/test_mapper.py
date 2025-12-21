"""Tests for schema mapper."""

import pytest

from ehrglot.conversion.mapper import (
    SchemaMapper,
    date_to_fhir_date,
    epic_sex_to_fhir_gender,
    normalize_phone,
    to_string,
)
from ehrglot.core.types import FieldMapping, SchemaMapping


class TestTransformFunctions:
    """Tests for built-in transform functions."""

    def test_to_string(self) -> None:
        """Test to_string transform."""
        assert to_string(123) == "123"
        assert to_string(None) is None
        assert to_string("abc") == "abc"

    def test_date_to_fhir_date(self) -> None:
        """Test date conversion to FHIR format."""
        assert date_to_fhir_date("2023-01-15") == "2023-01-15"
        assert date_to_fhir_date("01/15/2023") == "2023-01-15"
        assert date_to_fhir_date(None) is None

    def test_normalize_phone(self) -> None:
        """Test phone number normalization."""
        assert normalize_phone("5551234567") == "(555) 123-4567"
        assert normalize_phone("(555) 123-4567") == "(555) 123-4567"
        assert normalize_phone("15551234567") == "+1 (555) 123-4567"
        assert normalize_phone(None) is None

    def test_epic_sex_to_fhir_gender(self) -> None:
        """Test Epic sex code to FHIR gender conversion."""
        assert epic_sex_to_fhir_gender(1) == "male"
        assert epic_sex_to_fhir_gender(2) == "female"
        assert epic_sex_to_fhir_gender(3) == "other"
        assert epic_sex_to_fhir_gender("M") == "male"
        assert epic_sex_to_fhir_gender(None) == "unknown"


class TestSchemaMapper:
    """Tests for SchemaMapper class."""

    @pytest.fixture
    def simple_mapping(self) -> SchemaMapping:
        """Create a simple mapping for testing."""
        return SchemaMapping(
            source_system="test_ehr",
            source_table="PATIENT",
            target_resource="Patient",
            field_mappings=[
                FieldMapping(source="PAT_ID", target="id", transform="to_string"),
                FieldMapping(source="FIRST_NAME", target="name[0].given[0]"),
                FieldMapping(source="LAST_NAME", target="name[0].family"),
                FieldMapping(source="SEX_C", target="gender", transform="epic_sex_to_fhir_gender"),
            ],
        )

    def test_map_simple_row(self, simple_mapping: SchemaMapping) -> None:
        """Test mapping a simple row."""
        mapper = SchemaMapper(simple_mapping)

        source_row = {
            "PAT_ID": 12345,
            "FIRST_NAME": "John",
            "LAST_NAME": "Doe",
            "SEX_C": 1,
        }

        result, errors = mapper.map_row(source_row)

        assert result["id"] == "12345"
        assert result["name"][0]["given"][0] == "John"
        assert result["name"][0]["family"] == "Doe"
        assert result["gender"] == "male"
        assert len(errors) == 0

    def test_map_with_null_values(self, simple_mapping: SchemaMapping) -> None:
        """Test mapping with null values."""
        mapper = SchemaMapper(simple_mapping)

        source_row = {
            "PAT_ID": 12345,
            "FIRST_NAME": None,
            "LAST_NAME": "Doe",
            "SEX_C": None,
        }

        result, _errors = mapper.map_row(source_row)

        assert result["id"] == "12345"
        assert "name" in result
        assert result["name"][0]["family"] == "Doe"
        # SEX_C=None gets transformed to "unknown", which is set
        assert result.get("gender") == "unknown"

    def test_map_dataset(self, simple_mapping: SchemaMapping) -> None:
        """Test mapping multiple rows."""
        mapper = SchemaMapper(simple_mapping)

        source_rows = [
            {"PAT_ID": 1, "FIRST_NAME": "John", "LAST_NAME": "Doe", "SEX_C": 1},
            {"PAT_ID": 2, "FIRST_NAME": "Jane", "LAST_NAME": "Smith", "SEX_C": 2},
        ]

        results, _errors_by_row = mapper.map_dataset(source_rows)

        assert len(results) == 2
        assert results[0]["id"] == "1"
        assert results[1]["id"] == "2"
        assert results[0]["gender"] == "male"
        assert results[1]["gender"] == "female"

    def test_register_custom_transform(self, simple_mapping: SchemaMapping) -> None:
        """Test registering a custom transform function."""
        mapper = SchemaMapper(simple_mapping)

        def uppercase(value: str) -> str:
            return value.upper() if value else value

        mapper.register_transform("uppercase", uppercase)

        # Update mapping to use custom transform
        simple_mapping.field_mappings[1].transform = "uppercase"

        source_row = {
            "PAT_ID": 1,
            "FIRST_NAME": "john",
            "LAST_NAME": "doe",
            "SEX_C": 1,
        }

        result, _ = mapper.map_row(source_row)
        assert result["name"][0]["given"][0] == "JOHN"

    def test_nested_path_handling(self) -> None:
        """Test setting values at nested paths."""
        mapping = SchemaMapping(
            source_system="test",
            source_table="TEST",
            target_resource="Test",
            field_mappings=[
                FieldMapping(source="value1", target="nested.deep.value"),
                FieldMapping(source="value2", target="array[0].item"),
                FieldMapping(source="value3", target="array[1].item"),
            ],
        )

        mapper = SchemaMapper(mapping)
        source_row = {"value1": "a", "value2": "b", "value3": "c"}

        result, _ = mapper.map_row(source_row)

        assert result["nested"]["deep"]["value"] == "a"
        assert result["array"][0]["item"] == "b"
        assert result["array"][1]["item"] == "c"
