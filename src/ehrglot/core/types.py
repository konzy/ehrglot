"""Core type definitions for EHRglot."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PIILevel(Enum):
    """PII sensitivity level for a column."""

    NONE = "none"
    LOW = "low"  # Quasi-identifiers with low re-identification risk
    MEDIUM = "medium"  # Quasi-identifiers with moderate risk
    HIGH = "high"  # Direct identifiers that need masking
    CRITICAL = "critical"  # Highly sensitive (SSN, MRN, etc.)


class PIICategory(Enum):
    """Category of PII based on HIPAA Safe Harbor."""

    NONE = "none"
    DIRECT_IDENTIFIER = "direct_identifier"  # Names, SSN, MRN
    QUASI_IDENTIFIER = "quasi_identifier"  # Age, ZIP, dates
    SENSITIVE_DATA = "sensitive_data"  # Health conditions, etc.


class HIPAAIdentifier(Enum):
    """HIPAA Safe Harbor 18 identifiers."""

    NAMES = "names"
    GEOGRAPHIC = "geographic"  # Smaller than state
    DATES = "dates"  # Except year
    PHONE_NUMBERS = "phone_numbers"
    FAX_NUMBERS = "fax_numbers"
    EMAIL_ADDRESSES = "email_addresses"
    SSN = "ssn"  # Social Security numbers
    MRN = "mrn"  # Medical record numbers
    HEALTH_PLAN_ID = "health_plan_id"
    ACCOUNT_NUMBERS = "account_numbers"
    LICENSE_NUMBERS = "license_numbers"
    VEHICLE_IDENTIFIERS = "vehicle_identifiers"
    DEVICE_IDENTIFIERS = "device_identifiers"
    WEB_URLS = "web_urls"
    IP_ADDRESSES = "ip_addresses"
    BIOMETRIC = "biometric"
    PHOTOS = "photos"  # Full-face photographs
    OTHER_UNIQUE = "other_unique"  # Any other unique identifier


class MaskingStrategy(Enum):
    """Data masking strategies."""

    NONE = "none"
    REDACT = "redact"  # Replace with NULL or fixed value
    TOKENIZE = "tokenize"  # Replace with reversible token
    HASH = "hash"  # One-way hash
    GENERALIZE = "generalize"  # Reduce precision
    SUPPRESS = "suppress"  # Remove entirely
    PARTIAL = "partial"  # Show partial value (e.g., last 4 of SSN)


class DataType(Enum):
    """Supported data types for schema definitions."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    ARRAY = "array"
    OBJECT = "object"


@dataclass
class ColumnMetadata:
    """Metadata for a single column including PII tagging."""

    name: str
    data_type: DataType
    nullable: bool = True
    description: str = ""

    # PII tagging
    pii_level: PIILevel = PIILevel.NONE
    pii_category: PIICategory = PIICategory.NONE
    hipaa_identifier: HIPAAIdentifier | None = None

    # Masking configuration
    masking_strategy: MaskingStrategy = MaskingStrategy.NONE
    masking_params: dict[str, Any] = field(default_factory=dict)

    # Source mapping
    source_column: str | None = None
    transform: str | None = None  # Transformation expression


@dataclass
class SchemaDefinition:
    """Definition of a table/resource schema."""

    name: str
    version: str
    columns: list[ColumnMetadata]
    description: str = ""
    source_system: str | None = None
    target_resource: str | None = None  # FHIR resource type


@dataclass
class FieldMapping:
    """Mapping between source and target fields."""

    source: str
    target: str
    transform: str | None = None
    default_value: Any = None


@dataclass
class SchemaMapping:
    """Complete mapping between source and target schemas."""

    source_system: str
    source_table: str
    target_resource: str
    field_mappings: list[FieldMapping]
    description: str = ""


@dataclass
class CustomSchemaField:
    """Field definition for custom schemas.

    Allows third parties to define their own schema fields with
    full PII and type support.
    """

    name: str
    type: DataType
    required: bool = False
    pii_level: PIILevel = PIILevel.NONE
    pii_category: PIICategory = PIICategory.NONE
    hipaa_identifier: HIPAAIdentifier | None = None
    masking_strategy: MaskingStrategy = MaskingStrategy.NONE
    masking_params: dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass
class CustomSchema:
    """Custom schema definition for third-party integrations.

    Enables organizations to define their own data schemas
    (e.g., data warehouse, internal database) for conversion
    to/from FHIR R4.

    Example:
        >>> schema = CustomSchema(
        ...     name="my_warehouse",
        ...     version="1.0",
        ...     fields=[
        ...         CustomSchemaField(name="patient_id", type=DataType.STRING, required=True),
        ...         CustomSchemaField(name="full_name", type=DataType.STRING),
        ...     ]
        ... )
    """

    name: str
    version: str
    fields: list[CustomSchemaField]
    description: str = ""
    namespace: str = "custom"  # Namespace for schema identification


@dataclass
class BidirectionalMapping:
    """Bidirectional mapping between two schemas.

    Supports conversion in both directions:
    - Forward: source_schema → target_schema
    - Reverse: target_schema → source_schema

    If reverse_field_mappings is not provided, it will be
    auto-generated by inverting forward mappings where possible.

    Example use cases:
    - FHIR R4 ↔ Custom Data Warehouse
    - Epic Clarity → FHIR R4 → Partner System
    """

    source_schema: str  # e.g., "fhir_r4/patient" or "custom/my_warehouse"
    target_schema: str  # e.g., "custom/partner_format"
    field_mappings: list[FieldMapping]  # Forward mappings
    reverse_field_mappings: list[FieldMapping] = field(default_factory=list)
    description: str = ""
    auto_reverse: bool = True  # Auto-generate reverse mappings if not provided
