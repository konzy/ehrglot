"""Tests for custom schema support and bidirectional mappings."""

import pytest

from ehrglot.conversion.mapper import BidirectionalMapper, generate_reverse_mappings
from ehrglot.core.types import (
    BidirectionalMapping,
    CustomSchema,
    CustomSchemaField,
    DataType,
    FieldMapping,
    PIILevel,
)
from ehrglot.schemas.loader import SchemaLoader


class TestCustomSchemaTypes:
    """Test custom schema type definitions."""

    def test_custom_schema_field_creation(self) -> None:
        """Test creating a custom schema field."""
        field = CustomSchemaField(
            name="patient_id",
            type=DataType.STRING,
            required=True,
            pii_level=PIILevel.HIGH,
            description="Patient identifier",
        )
        assert field.name == "patient_id"
        assert field.type == DataType.STRING
        assert field.required is True
        assert field.pii_level == PIILevel.HIGH

    def test_custom_schema_creation(self) -> None:
        """Test creating a custom schema."""
        schema = CustomSchema(
            name="my_warehouse",
            version="1.0",
            fields=[
                CustomSchemaField(name="id", type=DataType.STRING, required=True),
                CustomSchemaField(name="name", type=DataType.STRING),
            ],
            description="Test warehouse schema",
            namespace="custom",
        )
        assert schema.name == "my_warehouse"
        assert schema.version == "1.0"
        assert len(schema.fields) == 2
        assert schema.namespace == "custom"

    def test_bidirectional_mapping_creation(self) -> None:
        """Test creating a bidirectional mapping."""
        bimap = BidirectionalMapping(
            source_schema="fhir_r4/patient",
            target_schema="custom/warehouse",
            field_mappings=[
                FieldMapping(source="id", target="patient_id"),
                FieldMapping(source="birthDate", target="dob"),
            ],
            auto_reverse=True,
        )
        assert bimap.source_schema == "fhir_r4/patient"
        assert bimap.target_schema == "custom/warehouse"
        assert len(bimap.field_mappings) == 2
        assert bimap.auto_reverse is True


class TestBidirectionalMapper:
    """Test bidirectional mapping functionality."""

    def test_generate_reverse_mappings(self) -> None:
        """Test automatic reverse mapping generation."""
        forward = [
            FieldMapping(source="id", target="patient_id"),
            FieldMapping(source="name", target="full_name"),
            FieldMapping(source="dob", target="date_of_birth"),
        ]
        reverse = generate_reverse_mappings(forward)
        assert len(reverse) == 3
        assert reverse[0].source == "patient_id"
        assert reverse[0].target == "id"

    def test_generate_reverse_skips_complex(self) -> None:
        """Test that complex expressions are skipped in reverse generation."""
        forward = [
            FieldMapping(source="id", target="patient_id"),
            FieldMapping(source="name[0].given + name[0].family", target="full_name"),
        ]
        reverse = generate_reverse_mappings(forward)
        # Should only have 1 (skips the complex expression)
        assert len(reverse) == 1
        assert reverse[0].source == "patient_id"

    def test_bidirectional_mapper_forward(self) -> None:
        """Test forward mapping."""
        bimap_def = BidirectionalMapping(
            source_schema="fhir_r4/patient",
            target_schema="custom/warehouse",
            field_mappings=[
                FieldMapping(source="id", target="patient_id"),
                FieldMapping(source="gender", target="gender_code"),
            ],
            auto_reverse=True,
        )
        mapper = BidirectionalMapper(bimap_def)

        source_row = {"id": "12345", "gender": "male"}
        result, errors = mapper.forward(source_row)

        assert result["patient_id"] == "12345"
        assert result["gender_code"] == "male"
        assert len(errors) == 0

    def test_bidirectional_mapper_reverse(self) -> None:
        """Test reverse mapping."""
        bimap_def = BidirectionalMapping(
            source_schema="fhir_r4/patient",
            target_schema="custom/warehouse",
            field_mappings=[
                FieldMapping(source="id", target="patient_id"),
                FieldMapping(source="gender", target="gender_code"),
            ],
            auto_reverse=True,
        )
        mapper = BidirectionalMapper(bimap_def)

        target_row = {"patient_id": "12345", "gender_code": "female"}
        result, errors = mapper.reverse(target_row)

        assert result["id"] == "12345"
        assert result["gender"] == "female"
        assert len(errors) == 0

    def test_bidirectional_mapper_explicit_reverse(self) -> None:
        """Test with explicit reverse mappings."""
        bimap_def = BidirectionalMapping(
            source_schema="fhir_r4/patient",
            target_schema="custom/warehouse",
            field_mappings=[
                FieldMapping(source="id", target="patient_id"),
            ],
            reverse_field_mappings=[
                FieldMapping(source="patient_id", target="id"),
                FieldMapping(source="warehouse_field", target="extra_field"),
            ],
            auto_reverse=False,
        )
        mapper = BidirectionalMapper(bimap_def)

        target_row = {"patient_id": "ABC", "warehouse_field": "value"}
        result, errors = mapper.reverse(target_row)

        assert result["id"] == "ABC"
        assert result["extra_field"] == "value"


class TestSchemaLoaderCustomSchema:
    """Test SchemaLoader custom schema methods."""

    def test_register_custom_schema(self, tmp_path: pytest.TempPathFactory) -> None:
        """Test registering a custom schema."""
        # Create minimal schema dir
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        (schema_dir / "fhir_r4").mkdir()

        loader = SchemaLoader(schema_dir)

        schema = CustomSchema(
            name="test_warehouse",
            version="1.0",
            fields=[CustomSchemaField(name="id", type=DataType.STRING)],
            namespace="custom",
        )
        loader.register_custom_schema(schema)

        retrieved = loader.get_custom_schema("custom/test_warehouse")
        assert retrieved is not None
        assert retrieved.name == "test_warehouse"

    def test_load_custom_schema_from_yaml(self, tmp_path: pytest.TempPathFactory) -> None:
        """Test loading custom schema from YAML file."""
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        (schema_dir / "fhir_r4").mkdir()
        custom_dir = schema_dir / "custom"
        custom_dir.mkdir()

        # Create test schema file
        schema_yaml = custom_dir / "test_schema.yaml"
        schema_yaml.write_text("""
name: test_schema
version: "2.0"
namespace: custom
description: Test schema from YAML

fields:
  - name: patient_key
    type: string
    required: true
    pii_level: high
  - name: full_name
    type: string
    pii_level: medium
""")

        loader = SchemaLoader(schema_dir)
        schema = loader.load_custom_schema("custom/test_schema")

        assert schema.name == "test_schema"
        assert schema.version == "2.0"
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "patient_key"
        assert schema.fields[0].required is True
        assert schema.fields[0].pii_level == PIILevel.HIGH

    def test_list_custom_schemas(self, tmp_path: pytest.TempPathFactory) -> None:
        """Test listing custom schemas."""
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        (schema_dir / "fhir_r4").mkdir()
        custom_dir = schema_dir / "custom"
        custom_dir.mkdir()

        # Create test schema files
        (custom_dir / "schema_a.yaml").write_text("name: schema_a\nversion: '1.0'\nfields: []")
        (custom_dir / "schema_b.yaml").write_text("name: schema_b\nversion: '1.0'\nfields: []")
        # This should be excluded (it's a mapping, not a schema)
        (custom_dir / "something_mapping.yaml").write_text("source_schema: a\ntarget_schema: b")

        loader = SchemaLoader(schema_dir)
        schemas = loader.list_custom_schemas()

        assert "custom/schema_a" in schemas
        assert "custom/schema_b" in schemas
        assert "custom/something_mapping" not in schemas
