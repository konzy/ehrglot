"""Data validation against FHIR R4 specifications."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from ehrglot.schemas.loader import FHIRResourceSchema, SchemaLoader


@dataclass
class ValidationError:
    """A single validation error."""

    field: str
    error_type: str
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validating a resource."""

    resource_type: str
    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        """Return number of errors."""
        return len(self.errors)

    def add_error(
        self,
        field_name: str,
        error_type: str,
        message: str,
        value: Any = None,
    ) -> None:
        """Add a validation error."""
        self.errors.append(
            ValidationError(field=field_name, error_type=error_type, message=message, value=value)
        )
        self.is_valid = False

    def add_warning(
        self,
        field_name: str,
        error_type: str,
        message: str,
        value: Any = None,
    ) -> None:
        """Add a validation warning."""
        self.warnings.append(
            ValidationError(field=field_name, error_type=error_type, message=message, value=value)
        )


class FHIRValidator:
    """Validates data against FHIR R4 resource schemas."""

    # Regex patterns for FHIR primitive types
    PATTERNS = {
        "id": re.compile(r"^[A-Za-z0-9\-\.]{1,64}$"),
        "uri": re.compile(r"^\S+$"),
        "url": re.compile(r"^https?://\S+$"),
        "code": re.compile(r"^[^\s]+(\s[^\s]+)*$"),
        "date": re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$"),
        "datetime": re.compile(
            r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
        ),
        "instant": re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"),
    }

    def __init__(self, schema_loader: SchemaLoader) -> None:
        """Initialize validator.

        Args:
            schema_loader: Loader for FHIR schemas.
        """
        self.schema_loader = schema_loader
        self._schema_cache: dict[str, FHIRResourceSchema] = {}

    def _get_schema(self, resource_type: str) -> FHIRResourceSchema | None:
        """Get schema for a resource type."""
        if resource_type not in self._schema_cache:
            try:
                self._schema_cache[resource_type] = self.schema_loader.load_fhir_resource(
                    resource_type.lower()
                )
            except FileNotFoundError:
                return None
        return self._schema_cache[resource_type]

    def _validate_type(
        self,
        value: Any,
        expected_type: str,
        field_name: str,
        result: ValidationResult,
    ) -> bool:
        """Validate a value against its expected type."""
        type_lower = expected_type.lower()

        # Handle null values
        if value is None:
            return True

        # Handle array types
        if type_lower.startswith("array<") or type_lower.startswith("list<"):
            if not isinstance(value, list):
                result.add_error(
                    field_name, "type_error", f"Expected array, got {type(value).__name__}", value
                )
                return False
            return True

        # Handle primitive types
        if type_lower == "string":
            if not isinstance(value, str):
                result.add_error(
                    field_name, "type_error", f"Expected string, got {type(value).__name__}", value
                )
                return False

        elif type_lower == "integer":
            if not isinstance(value, int) or isinstance(value, bool):
                result.add_error(
                    field_name,
                    "type_error",
                    f"Expected integer, got {type(value).__name__}",
                    value,
                )
                return False

        elif type_lower in ("decimal", "float"):
            if not isinstance(value, int | float) or isinstance(value, bool):
                result.add_error(
                    field_name, "type_error", f"Expected number, got {type(value).__name__}", value
                )
                return False

        elif type_lower == "boolean":
            if not isinstance(value, bool):
                result.add_error(
                    field_name,
                    "type_error",
                    f"Expected boolean, got {type(value).__name__}",
                    value,
                )
                return False

        elif type_lower == "date":
            if isinstance(value, str) and not self.PATTERNS["date"].match(value):
                result.add_error(
                    field_name, "format_error", "Invalid date format (expected YYYY-MM-DD)", value
                )
                return False

        elif type_lower in ("datetime", "instant"):
            if isinstance(value, str) and not self.PATTERNS["datetime"].match(value):
                result.add_error(field_name, "format_error", "Invalid datetime format", value)
                return False

        elif type_lower == "id" and isinstance(value, str):
            if not self.PATTERNS["id"].match(value):
                result.add_error(field_name, "format_error", "Invalid FHIR id format", value)
                return False

        return True

    def _validate_enum(
        self,
        value: Any,
        allowed_values: list[str],
        field_name: str,
        result: ValidationResult,
    ) -> bool:
        """Validate value against allowed enum values."""
        if value is None:
            return True
        if value not in allowed_values:
            result.add_error(
                field_name,
                "enum_error",
                f"Value must be one of: {allowed_values}",
                value,
            )
            return False
        return True

    def validate_resource(
        self,
        resource: dict[str, Any],
        resource_type: str | None = None,
    ) -> ValidationResult:
        """Validate a FHIR resource.

        Args:
            resource: Resource data to validate.
            resource_type: Optional resource type override.

        Returns:
            ValidationResult with errors and warnings.
        """
        # Get resource type
        if resource_type is None:
            resource_type = resource.get("resourceType", "")

        result = ValidationResult(resource_type=resource_type, is_valid=True)

        if not resource_type:
            result.add_error("resourceType", "required", "resourceType is required")
            return result

        # Get schema
        schema = self._get_schema(resource_type)
        if schema is None:
            result.add_warning("resourceType", "unknown", f"No schema found for {resource_type}")
            return result

        # Validate each field
        for fhir_field in schema.fields:
            value = resource.get(fhir_field.name)

            # Check required fields
            if fhir_field.required and value is None:
                result.add_error(
                    fhir_field.name, "required", f"Required field '{fhir_field.name}' is missing"
                )
                continue

            if value is None:
                continue

            # Validate type
            self._validate_type(value, fhir_field.type, fhir_field.name, result)

            # Validate enum if applicable
            if fhir_field.enum:
                self._validate_enum(value, fhir_field.enum, fhir_field.name, result)

        return result

    def validate_batch(
        self,
        resources: list[dict[str, Any]],
        resource_type: str | None = None,
    ) -> list[ValidationResult]:
        """Validate a batch of resources.

        Args:
            resources: List of resources to validate.
            resource_type: Optional resource type (if all same type).

        Returns:
            List of ValidationResults.
        """
        return [self.validate_resource(r, resource_type) for r in resources]

    def get_validation_summary(
        self,
        results: list[ValidationResult],
    ) -> dict[str, Any]:
        """Get summary statistics for validation results.

        Args:
            results: List of validation results.

        Returns:
            Summary dictionary.
        """
        total = len(results)
        valid = sum(1 for r in results if r.is_valid)
        invalid = total - valid
        total_errors = sum(r.error_count for r in results)

        error_types: dict[str, int] = {}
        for result in results:
            for error in result.errors:
                error_types[error.error_type] = error_types.get(error.error_type, 0) + 1

        return {
            "total_resources": total,
            "valid_resources": valid,
            "invalid_resources": invalid,
            "validation_rate": valid / total if total > 0 else 0,
            "total_errors": total_errors,
            "error_types": error_types,
        }
