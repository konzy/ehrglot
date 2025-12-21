"""YAML schema loader with validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ehrglot.core.types import (
    ColumnMetadata,
    DataType,
    FieldMapping,
    HIPAAIdentifier,
    MaskingStrategy,
    PIICategory,
    PIILevel,
    SchemaDefinition,
    SchemaMapping,
)


@dataclass
class FHIRField:
    """Represents a field in a FHIR resource schema."""

    name: str
    type: str
    required: bool = False
    pii_level: PIILevel = PIILevel.NONE
    pii_category: PIICategory = PIICategory.NONE
    hipaa_identifier: HIPAAIdentifier | None = None
    masking_strategy: MaskingStrategy = MaskingStrategy.NONE
    masking_params: dict[str, Any] = field(default_factory=dict)
    description: str = ""
    enum: list[str] | None = None
    default: Any = None


@dataclass
class FHIRResourceSchema:
    """Represents a complete FHIR resource schema."""

    resource: str
    version: str
    fields: list[FHIRField]
    fhir_url: str = ""
    description: str = ""


def _parse_pii_level(value: str | None) -> PIILevel:
    """Parse PII level from YAML value."""
    if not value:
        return PIILevel.NONE
    return PIILevel(value.lower())


def _parse_pii_category(value: str | None) -> PIICategory:
    """Parse PII category from YAML value."""
    if not value:
        return PIICategory.NONE
    return PIICategory(value.lower())


def _parse_hipaa_identifier(value: str | None) -> HIPAAIdentifier | None:
    """Parse HIPAA identifier from YAML value."""
    if not value:
        return None
    return HIPAAIdentifier(value.lower())


def _parse_masking_strategy(value: str | None) -> MaskingStrategy:
    """Parse masking strategy from YAML value."""
    if not value:
        return MaskingStrategy.NONE
    return MaskingStrategy(value.lower())


def _parse_data_type(type_str: str) -> DataType:
    """Parse FHIR/YAML type string to DataType."""
    type_lower = type_str.lower()
    if type_lower in ("string", "code", "id", "uri", "url", "canonical", "markdown"):
        return DataType.STRING
    elif type_lower in ("integer", "positiveint", "unsignedint"):
        return DataType.INTEGER
    elif type_lower in ("decimal", "float"):
        return DataType.FLOAT
    elif type_lower == "boolean":
        return DataType.BOOLEAN
    elif type_lower == "date":
        return DataType.DATE
    elif type_lower in ("datetime", "instant"):
        return DataType.DATETIME
    elif type_lower == "timestamp":
        return DataType.TIMESTAMP
    elif type_lower in ("base64binary", "binary"):
        return DataType.BINARY
    elif type_lower.startswith("array<") or type_lower.startswith("list<"):
        return DataType.ARRAY
    else:
        # Complex types (Reference, CodeableConcept, etc.) are treated as objects
        return DataType.OBJECT


class SchemaLoader:
    """Loads and validates YAML schema definitions."""

    def __init__(self, schema_dir: str | Path) -> None:
        """Initialize schema loader.

        Args:
            schema_dir: Root directory containing schema files.
        """
        self.schema_dir = Path(schema_dir)
        self._cache: dict[str, Any] = {}

    def load_fhir_resource(self, resource_name: str) -> FHIRResourceSchema:
        """Load a FHIR R4 resource schema.

        Args:
            resource_name: Name of the resource (e.g., 'patient', 'observation').

        Returns:
            FHIRResourceSchema with all field definitions.

        Raises:
            FileNotFoundError: If schema file doesn't exist.
            ValueError: If schema is invalid.
        """
        cache_key = f"fhir_r4:{resource_name}"
        if cache_key in self._cache:
            cached: FHIRResourceSchema = self._cache[cache_key]
            return cached

        schema_path = self.schema_dir / "fhir_r4" / f"{resource_name.lower()}.yaml"
        if not schema_path.exists():
            raise FileNotFoundError(f"FHIR schema not found: {schema_path}")

        with open(schema_path) as f:
            data = yaml.safe_load(f)

        fields = []
        for field_data in data.get("fields", []):
            fhir_field = FHIRField(
                name=field_data["name"],
                type=field_data["type"],
                required=field_data.get("required", False),
                pii_level=_parse_pii_level(field_data.get("pii_level")),
                pii_category=_parse_pii_category(field_data.get("pii_category")),
                hipaa_identifier=_parse_hipaa_identifier(field_data.get("hipaa_identifier")),
                masking_strategy=_parse_masking_strategy(field_data.get("masking_strategy")),
                masking_params=field_data.get("masking_params", {}),
                description=field_data.get("description", ""),
                enum=field_data.get("enum"),
                default=field_data.get("default"),
            )
            fields.append(fhir_field)

        schema = FHIRResourceSchema(
            resource=data["resource"],
            version=data["version"],
            fields=fields,
            fhir_url=data.get("fhir_url", ""),
            description=data.get("description", ""),
        )

        self._cache[cache_key] = schema
        return schema

    def load_mapping(self, source_system: str, resource_name: str) -> SchemaMapping:
        """Load a source-to-FHIR mapping definition.

        Args:
            source_system: Source EHR system (e.g., 'epic_clarity', 'cerner_millennium').
            resource_name: Target FHIR resource name.

        Returns:
            SchemaMapping with field mappings.

        Raises:
            FileNotFoundError: If mapping file doesn't exist.
        """
        cache_key = f"mapping:{source_system}:{resource_name}"
        if cache_key in self._cache:
            cached_mapping: SchemaMapping = self._cache[cache_key]
            return cached_mapping

        mapping_path = self.schema_dir / source_system / f"{resource_name.lower()}_mapping.yaml"
        if not mapping_path.exists():
            raise FileNotFoundError(f"Mapping not found: {mapping_path}")

        with open(mapping_path) as f:
            data = yaml.safe_load(f)

        field_mappings = []
        for fm_data in data.get("field_mappings", []):
            mapping = FieldMapping(
                source=fm_data.get("source", ""),
                target=fm_data["target"],
                transform=fm_data.get("transform"),
                default_value=fm_data.get("default"),
            )
            field_mappings.append(mapping)

        schema_mapping = SchemaMapping(
            source_system=data["source_system"],
            source_table=data["source_table"],
            target_resource=data["target_resource"],
            field_mappings=field_mappings,
            description=data.get("description", ""),
        )

        self._cache[cache_key] = schema_mapping
        return schema_mapping

    def fhir_to_schema_definition(self, resource_name: str) -> SchemaDefinition:
        """Convert FHIR resource schema to SchemaDefinition.

        Args:
            resource_name: Name of the FHIR resource.

        Returns:
            SchemaDefinition for the FHIR resource.
        """
        fhir_schema = self.load_fhir_resource(resource_name)

        columns = []
        for fhir_field in fhir_schema.fields:
            col = ColumnMetadata(
                name=fhir_field.name,
                data_type=_parse_data_type(fhir_field.type),
                nullable=not fhir_field.required,
                description=fhir_field.description,
                pii_level=fhir_field.pii_level,
                pii_category=fhir_field.pii_category,
                hipaa_identifier=fhir_field.hipaa_identifier,
                masking_strategy=fhir_field.masking_strategy,
                masking_params=fhir_field.masking_params,
            )
            columns.append(col)

        return SchemaDefinition(
            name=fhir_schema.resource,
            version=fhir_schema.version,
            columns=columns,
            description=fhir_schema.description,
        )

    def list_fhir_resources(self) -> list[str]:
        """List all available FHIR R4 resource schemas.

        Returns:
            List of resource names.
        """
        fhir_dir = self.schema_dir / "fhir_r4"
        if not fhir_dir.exists():
            return []
        return [p.stem for p in fhir_dir.glob("*.yaml")]

    def list_source_systems(self) -> list[str]:
        """List all available source system mappings.

        Returns:
            List of source system names.
        """
        systems = []
        for p in self.schema_dir.iterdir():
            if p.is_dir() and p.name != "fhir_r4":
                systems.append(p.name)
        return systems

    def clear_cache(self) -> None:
        """Clear the schema cache."""
        self._cache.clear()
