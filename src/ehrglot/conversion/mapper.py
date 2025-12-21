"""Schema mapping engine for transforming data between EHR systems."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from ehrglot.core.types import BidirectionalMapping, FieldMapping, SchemaMapping


@dataclass
class TransformContext:
    """Context for field transformations."""

    source_row: dict[str, Any]
    target_row: dict[str, Any]
    lookup_tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


# Built-in transform functions
def to_string(value: Any) -> str | None:
    """Convert value to string."""
    if value is None:
        return None
    return str(value)


def date_to_fhir_date(value: Any) -> str | None:
    """Convert date to FHIR date format (YYYY-MM-DD)."""
    if value is None:
        return None
    if isinstance(value, str):
        # Try to parse common date formats
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]:
            try:
                value = datetime.strptime(value, fmt).date()
                break
            except ValueError:
                continue
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def datetime_to_fhir_datetime(value: Any) -> str | None:
    """Convert datetime to FHIR instant format (ISO 8601)."""
    if value is None:
        return None
    if isinstance(value, str):
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M:%S"]:
            try:
                value = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def normalize_phone(value: Any) -> str | None:
    """Normalize phone number to standard format."""
    if value is None:
        return None
    # Remove all non-digit characters
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == "1":
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return str(value)


def epic_sex_to_fhir_gender(value: Any) -> str:
    """Convert Epic SEX_C to FHIR gender."""
    mapping = {
        1: "male",
        2: "female",
        3: "other",
        "1": "male",
        "2": "female",
        "3": "other",
        "M": "male",
        "F": "female",
    }
    return mapping.get(value, "unknown")


def cerner_sex_to_fhir_gender(value: Any) -> str:
    """Convert Cerner SEX_CD to FHIR gender."""
    mapping = {
        362: "male",
        363: "female",
        364: "unknown",
    }
    return mapping.get(value, "unknown")


# HL7 v2.x Transform Functions


def hl7_datetime_to_fhir_date(value: Any) -> str | None:
    """Convert HL7 TS (YYYYMMDD...) to FHIR date (YYYY-MM-DD)."""
    if value is None:
        return None
    s = str(value).strip()
    if len(s) >= 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) >= 4:
        return s[:4]
    return None


def hl7_datetime_to_fhir_datetime(value: Any) -> str | None:
    """Convert HL7 TS to FHIR dateTime (ISO 8601)."""
    if value is None:
        return None
    s = str(value).strip()
    if len(s) >= 14:
        # YYYYMMDDHHMMSS -> YYYY-MM-DDTHH:MM:SS
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}T{s[8:10]}:{s[10:12]}:{s[12:14]}"
    if len(s) >= 12:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}T{s[8:10]}:{s[10:12]}:00"
    if len(s) >= 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None


def hl7_datetime_to_fhir_instant(value: Any) -> str | None:
    """Convert HL7 TS to FHIR instant (with timezone)."""
    result = hl7_datetime_to_fhir_datetime(value)
    if result and "T" in result:
        return f"{result}Z"  # Assume UTC if no timezone
    return result


def hl7_sex_to_fhir_gender(value: Any) -> str:
    """Convert HL7 v2.x sex code to FHIR gender."""
    mapping = {
        "M": "male",
        "F": "female",
        "O": "other",
        "U": "unknown",
        "A": "other",
        "N": "unknown",
    }
    return mapping.get(str(value).upper() if value else "", "unknown")


def hl7_abnormal_flag_to_fhir(value: Any) -> str | None:
    """Convert HL7 abnormal flag to FHIR interpretation code."""
    if value is None:
        return None
    mapping = {
        "L": "L",
        "H": "H",
        "LL": "LL",
        "HH": "HH",
        "N": "N",
        "A": "A",
        "AA": "AA",
        "<": "L",
        ">": "H",
    }
    return mapping.get(str(value).upper(), str(value))


def hl7_result_status_to_fhir(value: Any) -> str:
    """Convert HL7 result status to FHIR observation status."""
    mapping = {
        "C": "corrected",
        "D": "cancelled",
        "F": "final",
        "I": "registered",
        "P": "preliminary",
        "R": "registered",
        "S": "preliminary",
        "U": "registered",
        "W": "entered-in-error",
        "X": "cancelled",
    }
    return mapping.get(str(value).upper() if value else "", "unknown")


def hl7_yn_to_boolean(value: Any) -> bool | None:
    """Convert HL7 Y/N to boolean."""
    if value is None:
        return None
    s = str(value).upper()
    if s in ("Y", "YES", "1", "TRUE"):
        return True
    if s in ("N", "NO", "0", "FALSE"):
        return False
    return None


def hl7_coding_system_to_uri(value: Any) -> str:
    """Convert HL7 coding system identifier to FHIR URI."""
    if value is None:
        return ""
    mapping = {
        "LN": "http://loinc.org",
        "LOINC": "http://loinc.org",
        "SCT": "http://snomed.info/sct",
        "SNOMED": "http://snomed.info/sct",
        "I9C": "http://hl7.org/fhir/sid/icd-9-cm",
        "I10": "http://hl7.org/fhir/sid/icd-10-cm",
        "ICD10": "http://hl7.org/fhir/sid/icd-10-cm",
        "CPT": "http://www.ama-assn.org/go/cpt",
        "CPT4": "http://www.ama-assn.org/go/cpt",
        "RXNORM": "http://www.nlm.nih.gov/research/umls/rxnorm",
        "NDC": "http://hl7.org/fhir/sid/ndc",
        "CVX": "http://hl7.org/fhir/sid/cvx",
    }
    return mapping.get(str(value).upper(), f"urn:oid:{value}")


# Registry of transform functions
TRANSFORM_REGISTRY: dict[str, Callable[..., Any]] = {
    "to_string": to_string,
    "date_to_fhir_date": date_to_fhir_date,
    "datetime_to_fhir_datetime": datetime_to_fhir_datetime,
    "normalize_phone": normalize_phone,
    "epic_sex_to_fhir_gender": epic_sex_to_fhir_gender,
    "cerner_sex_to_fhir_gender": cerner_sex_to_fhir_gender,
    # HL7 v2.x transforms
    "hl7_datetime_to_fhir_date": hl7_datetime_to_fhir_date,
    "hl7_datetime_to_fhir_datetime": hl7_datetime_to_fhir_datetime,
    "hl7_datetime_to_fhir_instant": hl7_datetime_to_fhir_instant,
    "hl7_sex_to_fhir_gender": hl7_sex_to_fhir_gender,
    "hl7_abnormal_flag_to_fhir": hl7_abnormal_flag_to_fhir,
    "hl7_result_status_to_fhir": hl7_result_status_to_fhir,
    "hl7_yn_to_boolean": hl7_yn_to_boolean,
    "hl7_coding_system_to_uri": hl7_coding_system_to_uri,
}


class SchemaMapper:
    """Maps data from source schema to target schema using mapping definitions."""

    def __init__(
        self,
        mapping: SchemaMapping,
        transforms: dict[str, Callable[..., Any]] | None = None,
    ) -> None:
        """Initialize schema mapper.

        Args:
            mapping: Schema mapping definition.
            transforms: Additional transform functions to register.
        """
        self.mapping = mapping
        self.transforms = {**TRANSFORM_REGISTRY}
        if transforms:
            self.transforms.update(transforms)

    def _get_nested_value(self, obj: dict[str, Any], path: str) -> Any:
        """Get a value from a nested path like 'name[0].given[0]'."""
        parts = re.split(r"\.|\[|\]", path)
        parts = [p for p in parts if p]  # Remove empty strings

        current: Any = obj
        for part in parts:
            if current is None:
                return None
            if part.isdigit():
                idx = int(part)
                if isinstance(current, list) and idx < len(current):
                    current = current[idx]
                else:
                    return None
            elif isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current

    def _set_nested_value(self, obj: dict[str, Any], path: str, value: Any) -> None:
        """Set a value at a nested path, creating intermediate structures."""
        parts = re.split(r"\.|\[(\d+)\]", path)
        parts = [p for p in parts if p]  # Remove empty strings

        current: Any = obj
        for i, part in enumerate(parts[:-1]):
            next_part = parts[i + 1] if i + 1 < len(parts) else None

            if part.isdigit():
                idx = int(part)
                if isinstance(current, list):
                    while len(current) <= idx:
                        current.append({} if not next_part or not next_part.isdigit() else [])
                    current = current[idx]
            else:
                if isinstance(current, dict):
                    if part not in current:
                        if next_part and next_part.isdigit():
                            current[part] = []
                        else:
                            current[part] = {}
                    current = current[part]

        # Set the final value
        final_part = parts[-1]
        if final_part.isdigit():
            idx = int(final_part)
            if isinstance(current, list):
                while len(current) <= idx:
                    current.append(None)
                current[idx] = value
        elif isinstance(current, dict):
            current[final_part] = value

    def _apply_transform(
        self,
        value: Any,
        transform_name: str | None,
        context: TransformContext,
    ) -> Any:
        """Apply a transform function to a value."""
        if transform_name is None:
            return value

        if transform_name not in self.transforms:
            context.errors.append(f"Unknown transform: {transform_name}")
            return value

        try:
            return self.transforms[transform_name](value)
        except Exception as e:
            context.errors.append(f"Transform error ({transform_name}): {e}")
            return value

    def map_row(
        self,
        source_row: dict[str, Any],
        lookup_tables: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """Map a single row from source to target schema.

        Args:
            source_row: Source data row.
            lookup_tables: Optional lookup tables for reference data.

        Returns:
            Tuple of (mapped row, list of errors).
        """
        target_row: dict[str, Any] = {}
        context = TransformContext(
            source_row=source_row,
            target_row=target_row,
            lookup_tables=lookup_tables or {},
        )

        for field_mapping in self.mapping.field_mappings:
            # Get source value
            if field_mapping.source:
                source_value = self._get_nested_value(source_row, field_mapping.source)
            else:
                source_value = field_mapping.default_value

            # Skip if null and no default
            if source_value is None and field_mapping.default_value is not None:
                source_value = field_mapping.default_value

            # Apply transform
            transformed_value = self._apply_transform(
                source_value, field_mapping.transform, context
            )

            # Set target value
            if transformed_value is not None:
                self._set_nested_value(target_row, field_mapping.target, transformed_value)

        return target_row, context.errors

    def map_dataset(
        self,
        source_rows: list[dict[str, Any]],
        lookup_tables: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], list[tuple[int, list[str]]]]:
        """Map a dataset from source to target schema.

        Args:
            source_rows: List of source data rows.
            lookup_tables: Optional lookup tables.

        Returns:
            Tuple of (mapped rows, list of (row_index, errors) for rows with errors).
        """
        mapped_rows = []
        errors_by_row = []

        for i, source_row in enumerate(source_rows):
            mapped_row, errors = self.map_row(source_row, lookup_tables)
            mapped_rows.append(mapped_row)
            if errors:
                errors_by_row.append((i, errors))

        return mapped_rows, errors_by_row

    def register_transform(self, name: str, func: Callable[..., Any]) -> None:
        """Register a custom transform function.

        Args:
            name: Name to register the transform under.
            func: Transform function.
        """
        self.transforms[name] = func


def generate_reverse_mappings(forward_mappings: list[FieldMapping]) -> list[FieldMapping]:
    """Generate reverse mappings by inverting source/target.

    Args:
        forward_mappings: List of forward field mappings.

    Returns:
        List of inverted field mappings.
    """
    reverse = []
    for fm in forward_mappings:
        # Skip complex expressions that can't be auto-reversed
        if fm.source and fm.target and "+" not in fm.source and "[" not in fm.source:
            reverse.append(
                FieldMapping(
                    source=fm.target,
                    target=fm.source,
                    transform=None,
                    default_value=None,
                )
            )
    return reverse


class BidirectionalMapper:
    """Maps data bidirectionally between two schemas.

    Supports conversion in both directions:
    - forward(): source_schema → target_schema
    - reverse(): target_schema → source_schema

    Example:
        >>> bimap = BidirectionalMapper(bidirectional_mapping)
        >>> fhir_patient = bimap.forward(warehouse_row)
        >>> warehouse_row = bimap.reverse(fhir_patient)
    """

    def __init__(
        self,
        mapping: BidirectionalMapping,
        transforms: dict[str, Callable[..., Any]] | None = None,
    ) -> None:
        """Initialize bidirectional mapper.

        Args:
            mapping: BidirectionalMapping definition.
            transforms: Additional transform functions.
        """
        self.mapping = mapping
        self.transforms = {**TRANSFORM_REGISTRY}
        if transforms:
            self.transforms.update(transforms)

        # Create forward mapper
        self._forward_mapping = SchemaMapping(
            source_system=mapping.source_schema,
            source_table="",
            target_resource=mapping.target_schema,
            field_mappings=mapping.field_mappings,
            description=mapping.description,
        )
        self._forward_mapper = SchemaMapper(self._forward_mapping, self.transforms)

        # Create reverse mapper
        reverse_mappings = mapping.reverse_field_mappings
        if not reverse_mappings and mapping.auto_reverse:
            reverse_mappings = generate_reverse_mappings(mapping.field_mappings)

        self._reverse_mapping = SchemaMapping(
            source_system=mapping.target_schema,
            source_table="",
            target_resource=mapping.source_schema,
            field_mappings=reverse_mappings,
            description=f"Reverse of: {mapping.description}",
        )
        self._reverse_mapper = SchemaMapper(self._reverse_mapping, self.transforms)

    def forward(
        self,
        source_row: dict[str, Any],
        lookup_tables: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """Map data from source to target schema.

        Args:
            source_row: Data in source schema format.
            lookup_tables: Optional lookup tables.

        Returns:
            Tuple of (target row, errors).
        """
        return self._forward_mapper.map_row(source_row, lookup_tables)

    def reverse(
        self,
        target_row: dict[str, Any],
        lookup_tables: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], list[str]]:
        """Map data from target back to source schema.

        Args:
            target_row: Data in target schema format.
            lookup_tables: Optional lookup tables.

        Returns:
            Tuple of (source row, errors).
        """
        return self._reverse_mapper.map_row(target_row, lookup_tables)

    def forward_dataset(
        self,
        source_rows: list[dict[str, Any]],
        lookup_tables: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], list[tuple[int, list[str]]]]:
        """Map dataset from source to target schema."""
        return self._forward_mapper.map_dataset(source_rows, lookup_tables)

    def reverse_dataset(
        self,
        target_rows: list[dict[str, Any]],
        lookup_tables: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[list[dict[str, Any]], list[tuple[int, list[str]]]]:
        """Map dataset from target back to source schema."""
        return self._reverse_mapper.map_dataset(target_rows, lookup_tables)

    def register_transform(self, name: str, func: Callable[..., Any]) -> None:
        """Register a custom transform function for both directions."""
        self.transforms[name] = func
        self._forward_mapper.register_transform(name, func)
        self._reverse_mapper.register_transform(name, func)
