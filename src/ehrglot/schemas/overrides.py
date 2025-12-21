"""Schema override system for customizing PII/masking properties."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from ehrglot.core.types import HIPAAIdentifier, MaskingStrategy, PIICategory, PIILevel

# Properties that users are allowed to override
OVERRIDABLE_PROPERTIES = frozenset(
    {
        "pii_level",
        "pii_category",
        "hipaa_identifier",
        "masking_strategy",
        "masking_params",
    }
)


@dataclass
class FieldOverride:
    """Override specification for a single field."""

    field_name: str
    pii_level: PIILevel | None = None
    pii_category: PIICategory | None = None
    hipaa_identifier: HIPAAIdentifier | None = None
    masking_strategy: MaskingStrategy | None = None
    masking_params: dict[str, Any] = field(default_factory=dict)

    def has_overrides(self) -> bool:
        """Check if this override has any non-None values."""
        return any(
            [
                self.pii_level is not None,
                self.pii_category is not None,
                self.hipaa_identifier is not None,
                self.masking_strategy is not None,
                self.masking_params,
            ]
        )


@dataclass
class SchemaOverride:
    """Override specification for a schema (resource or mapping)."""

    resource_name: str
    field_overrides: dict[str, FieldOverride] = field(default_factory=dict)
    description: str = ""

    def get_field_override(self, field_name: str) -> FieldOverride | None:
        """Get override for a specific field."""
        return self.field_overrides.get(field_name)


class SchemaOverrideLoader:
    """Loads and manages schema overrides from YAML files.

    Override files should be placed in the `schema_overrides` directory
    with the same structure as the base schemas:
      - schema_overrides/fhir_r4/patient.yaml
      - schema_overrides/epic_clarity/patient_mapping.yaml

    Example override file:
    ```yaml
    resource: Patient
    description: Custom overrides for our organization
    field_overrides:
      birthDate:
        pii_level: critical
        masking_strategy: redact
      address:
        pii_level: high
        masking_strategy: partial
        masking_params:
          show_last: 5
    ```
    """

    def __init__(self, override_dir: str | Path) -> None:
        """Initialize override loader.

        Args:
            override_dir: Directory containing override YAML files.
        """
        self.override_dir = Path(override_dir)
        self._cache: dict[str, SchemaOverride] = {}

    def load_fhir_override(self, resource_name: str) -> SchemaOverride | None:
        """Load overrides for a FHIR resource.

        Args:
            resource_name: Name of the resource (e.g., 'patient').

        Returns:
            SchemaOverride if override file exists, None otherwise.
        """
        cache_key = f"fhir_r4:{resource_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        override_path = self.override_dir / "fhir_r4" / f"{resource_name.lower()}.yaml"
        if not override_path.exists():
            return None

        override = self._load_override_file(override_path, resource_name)
        self._cache[cache_key] = override
        return override

    def load_mapping_override(
        self, source_system: str, resource_name: str
    ) -> SchemaOverride | None:
        """Load overrides for a source-to-FHIR mapping.

        Args:
            source_system: Source EHR system.
            resource_name: Target FHIR resource name.

        Returns:
            SchemaOverride if override file exists, None otherwise.
        """
        cache_key = f"mapping:{source_system}:{resource_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        override_path = self.override_dir / source_system / f"{resource_name.lower()}_mapping.yaml"
        if not override_path.exists():
            return None

        override = self._load_override_file(override_path, resource_name)
        self._cache[cache_key] = override
        return override

    def _load_override_file(self, path: Path, resource_name: str) -> SchemaOverride:
        """Load and parse an override YAML file.

        Args:
            path: Path to override file.
            resource_name: Name of the resource being overridden.

        Returns:
            Parsed SchemaOverride.

        Raises:
            ValueError: If override contains non-overridable properties.
        """
        with open(path) as f:
            data = yaml.safe_load(f)

        field_overrides: dict[str, FieldOverride] = {}

        for field_name, field_data in data.get("field_overrides", {}).items():
            # Validate that only allowed properties are being overridden
            invalid_props = set(field_data.keys()) - OVERRIDABLE_PROPERTIES
            if invalid_props:
                raise ValueError(
                    f"Invalid override properties for field '{field_name}': {invalid_props}. "
                    f"Only these properties can be overridden: {OVERRIDABLE_PROPERTIES}"
                )

            field_override = FieldOverride(
                field_name=field_name,
                pii_level=self._parse_pii_level(field_data.get("pii_level")),
                pii_category=self._parse_pii_category(field_data.get("pii_category")),
                hipaa_identifier=self._parse_hipaa_identifier(field_data.get("hipaa_identifier")),
                masking_strategy=self._parse_masking_strategy(field_data.get("masking_strategy")),
                masking_params=field_data.get("masking_params", {}),
            )
            field_overrides[field_name] = field_override

        return SchemaOverride(
            resource_name=resource_name,
            field_overrides=field_overrides,
            description=data.get("description", ""),
        )

    def _parse_pii_level(self, value: str | None) -> PIILevel | None:
        """Parse PII level, returning None if not specified."""
        if not value:
            return None
        return PIILevel(value.lower())

    def _parse_pii_category(self, value: str | None) -> PIICategory | None:
        """Parse PII category, returning None if not specified."""
        if not value:
            return None
        return PIICategory(value.lower())

    def _parse_hipaa_identifier(self, value: str | None) -> HIPAAIdentifier | None:
        """Parse HIPAA identifier, returning None if not specified."""
        if not value:
            return None
        return HIPAAIdentifier(value.lower())

    def _parse_masking_strategy(self, value: str | None) -> MaskingStrategy | None:
        """Parse masking strategy, returning None if not specified."""
        if not value:
            return None
        return MaskingStrategy(value.lower())

    def clear_cache(self) -> None:
        """Clear the override cache."""
        self._cache.clear()

    def list_overrides(self) -> list[str]:
        """List all available override files.

        Returns:
            List of override file paths relative to override_dir.
        """
        if not self.override_dir.exists():
            return []

        overrides = []
        for yaml_file in self.override_dir.rglob("*.yaml"):
            overrides.append(str(yaml_file.relative_to(self.override_dir)))
        return sorted(overrides)
