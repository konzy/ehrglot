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
