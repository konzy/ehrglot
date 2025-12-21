"""HIPAA Safe Harbor 18 identifiers and detection patterns."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import ClassVar

from ehrglot.core.types import HIPAAIdentifier, PIICategory, PIILevel


@dataclass
class HIPAAIdentifierSpec:
    """Specification for a HIPAA Safe Harbor identifier."""

    identifier: HIPAAIdentifier
    name: str
    description: str
    pii_level: PIILevel
    pii_category: PIICategory

    # Column name patterns (case-insensitive)
    column_patterns: list[str]

    # Value regex patterns for detection
    value_patterns: list[str]

    # Example values for testing
    examples: list[str]


# Complete list of HIPAA Safe Harbor 18 identifiers
HIPAA_IDENTIFIERS: list[HIPAAIdentifierSpec] = [
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.NAMES,
        name="Names",
        description="Names of individuals including first, last, and maiden names",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*name.*",
            r".*first.*name.*",
            r".*last.*name.*",
            r".*maiden.*",
            r".*surname.*",
            r".*given.*name.*",
            r".*family.*name.*",
            r"pat_first.*",
            r"pat_last.*",
            r"patient.*name.*",
        ],
        value_patterns=[],  # Names are too varied to detect by pattern
        examples=["John Smith", "Jane Doe", "O'Connor"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.GEOGRAPHIC,
        name="Geographic Data",
        description="All geographic subdivisions smaller than a state (street, city, ZIP)",
        pii_level=PIILevel.HIGH,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*address.*",
            r".*street.*",
            r".*city.*",
            r".*zip.*",
            r".*postal.*",
            r".*county.*",
            r".*geo.*",
            r"add_line.*",
        ],
        value_patterns=[
            r"^\d{5}(-\d{4})?$",  # US ZIP code
            r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$",  # Canadian postal code
        ],
        examples=["123 Main St", "New York", "10001", "90210-1234"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.DATES,
        name="Dates",
        description="All elements of dates except year (birth, admission, discharge, death)",
        pii_level=PIILevel.HIGH,
        pii_category=PIICategory.QUASI_IDENTIFIER,
        column_patterns=[
            r".*birth.*date.*",
            r".*dob.*",
            r".*death.*date.*",
            r".*admit.*date.*",
            r".*discharge.*date.*",
            r".*service.*date.*",
            r".*effective.*date.*",
            r".*dt_tm$",
            r".*_date$",
        ],
        value_patterns=[
            r"^\d{4}-\d{2}-\d{2}$",  # ISO date
            r"^\d{2}/\d{2}/\d{4}$",  # US date
            r"^\d{2}-\d{2}-\d{4}$",  # US date with dashes
        ],
        examples=["1990-01-15", "03/15/1985", "12-25-2000"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.PHONE_NUMBERS,
        name="Phone Numbers",
        description="Telephone numbers including area codes",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*phone.*",
            r".*tel.*",
            r".*mobile.*",
            r".*cell.*",
            r".*contact.*num.*",
        ],
        value_patterns=[
            r"^\+?1?\s*\(?[0-9]{3}\)?[\s.-]?[0-9]{3}[\s.-]?[0-9]{4}$",  # US phone
            r"^\+[0-9]{1,3}[\s.-]?[0-9]{6,14}$",  # International
        ],
        examples=["(555) 123-4567", "+1-555-123-4567", "5551234567"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.FAX_NUMBERS,
        name="Fax Numbers",
        description="Fax numbers",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*fax.*",
        ],
        value_patterns=[
            r"^\+?1?\s*\(?[0-9]{3}\)?[\s.-]?[0-9]{3}[\s.-]?[0-9]{4}$",
        ],
        examples=["(555) 123-4567", "555-123-4567"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.EMAIL_ADDRESSES,
        name="Email Addresses",
        description="Electronic mail addresses",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*email.*",
            r".*e_mail.*",
            r".*mail.*addr.*",
        ],
        value_patterns=[
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        ],
        examples=["patient@example.com", "john.doe@hospital.org"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.SSN,
        name="Social Security Numbers",
        description="Social Security numbers",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*ssn.*",
            r".*social.*sec.*",
            r".*ss_num.*",
            r".*soc.*sec.*num.*",
        ],
        value_patterns=[
            r"^\d{3}-?\d{2}-?\d{4}$",
            r"^\d{9}$",
        ],
        examples=["123-45-6789", "123456789"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.MRN,
        name="Medical Record Numbers",
        description="Medical record numbers and health plan identifiers",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*mrn.*",
            r".*medical.*rec.*",
            r".*pat_id.*",
            r".*patient.*id.*",
            r".*chart.*num.*",
        ],
        value_patterns=[],  # MRN formats vary by institution
        examples=["MRN12345678", "00-123456"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.HEALTH_PLAN_ID,
        name="Health Plan Beneficiary Numbers",
        description="Health plan beneficiary numbers and insurance IDs",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*health.*plan.*",
            r".*insurance.*id.*",
            r".*member.*id.*",
            r".*beneficiary.*",
            r".*subscriber.*id.*",
            r".*policy.*num.*",
        ],
        value_patterns=[],
        examples=["HPB123456789", "INS-98765"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.ACCOUNT_NUMBERS,
        name="Account Numbers",
        description="Account numbers including bank and billing accounts",
        pii_level=PIILevel.HIGH,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*account.*",
            r".*acct.*",
            r".*billing.*num.*",
            r".*fin.*num.*",
        ],
        value_patterns=[],
        examples=["1234567890", "ACCT-12345"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.LICENSE_NUMBERS,
        name="Certificate/License Numbers",
        description="Certificate and license numbers (driver's license, professional)",
        pii_level=PIILevel.HIGH,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*license.*",
            r".*licence.*",
            r".*driver.*lic.*",
            r".*dl_num.*",
            r".*certificate.*",
        ],
        value_patterns=[],
        examples=["DL12345678", "LIC-2023-001"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.VEHICLE_IDENTIFIERS,
        name="Vehicle Identifiers",
        description="Vehicle identifiers and serial numbers including license plate numbers",
        pii_level=PIILevel.MEDIUM,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*vehicle.*",
            r".*vin.*",
            r".*plate.*",
            r".*car.*id.*",
        ],
        value_patterns=[
            r"^[A-HJ-NPR-Z0-9]{17}$",  # VIN
        ],
        examples=["1HGCM82633A123456", "ABC-1234"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.DEVICE_IDENTIFIERS,
        name="Device Identifiers",
        description="Device identifiers and serial numbers",
        pii_level=PIILevel.MEDIUM,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*device.*id.*",
            r".*serial.*",
            r".*imei.*",
            r".*mac.*addr.*",
            r".*equipment.*id.*",
        ],
        value_patterns=[
            r"^[0-9A-Fa-f]{2}(:[0-9A-Fa-f]{2}){5}$",  # MAC address
            r"^\d{15}$",  # IMEI
        ],
        examples=["00:1A:2B:3C:4D:5E", "123456789012345"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.WEB_URLS,
        name="Web URLs",
        description="Web Universal Resource Locators (URLs)",
        pii_level=PIILevel.MEDIUM,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*url.*",
            r".*website.*",
            r".*web.*addr.*",
            r".*link.*",
        ],
        value_patterns=[
            r"^https?://[^\s]+$",
        ],
        examples=["https://patient-portal.example.com/user123"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.IP_ADDRESSES,
        name="IP Addresses",
        description="Internet Protocol (IP) address numbers",
        pii_level=PIILevel.MEDIUM,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*ip.*addr.*",
            r".*ip_address.*",
            r".*client.*ip.*",
            r".*source.*ip.*",
        ],
        value_patterns=[
            r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",  # IPv4
            r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",  # IPv6
        ],
        examples=["192.168.1.1", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"],
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.BIOMETRIC,
        name="Biometric Identifiers",
        description="Biometric identifiers including finger/voice prints, retinal scans",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*biometric.*",
            r".*fingerprint.*",
            r".*retina.*",
            r".*iris.*",
            r".*voice.*print.*",
            r".*face.*id.*",
        ],
        value_patterns=[],
        examples=[],  # Binary data, not detectable by pattern
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.PHOTOS,
        name="Full-face Photographs",
        description="Full-face photographic images and comparable images",
        pii_level=PIILevel.CRITICAL,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*photo.*",
            r".*image.*",
            r".*picture.*",
            r".*portrait.*",
            r".*headshot.*",
            r".*avatar.*",
        ],
        value_patterns=[],
        examples=[],  # Binary data
    ),
    HIPAAIdentifierSpec(
        identifier=HIPAAIdentifier.OTHER_UNIQUE,
        name="Other Unique Identifiers",
        description="Any other unique identifying number, characteristic, or code",
        pii_level=PIILevel.HIGH,
        pii_category=PIICategory.DIRECT_IDENTIFIER,
        column_patterns=[
            r".*unique.*id.*",
            r".*guid.*",
            r".*uuid.*",
            r".*external.*id.*",
        ],
        value_patterns=[
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",  # UUID
        ],
        examples=["550e8400-e29b-41d4-a716-446655440000"],
    ),
]


# Build lookup dictionaries for fast access
IDENTIFIER_BY_TYPE: dict[HIPAAIdentifier, HIPAAIdentifierSpec] = {
    spec.identifier: spec for spec in HIPAA_IDENTIFIERS
}


class HIPAAPatternMatcher:
    """Matches column names and values against HIPAA identifier patterns."""

    # Pre-compiled regex patterns for column names
    _column_patterns: ClassVar[dict[HIPAAIdentifier, list[re.Pattern[str]]]] = {}

    # Pre-compiled regex patterns for values
    _value_patterns: ClassVar[dict[HIPAAIdentifier, list[re.Pattern[str]]]] = {}

    @classmethod
    def _ensure_compiled(cls) -> None:
        """Compile regex patterns if not already done."""
        if cls._column_patterns:
            return

        for spec in HIPAA_IDENTIFIERS:
            cls._column_patterns[spec.identifier] = [
                re.compile(pattern, re.IGNORECASE) for pattern in spec.column_patterns
            ]
            cls._value_patterns[spec.identifier] = [
                re.compile(pattern) for pattern in spec.value_patterns
            ]

    @classmethod
    def match_column_name(cls, column_name: str) -> list[HIPAAIdentifierSpec]:
        """Match a column name against HIPAA identifier patterns.

        Args:
            column_name: Name of the column to check.

        Returns:
            List of matching HIPAA identifier specs, ordered by likelihood.
        """
        cls._ensure_compiled()
        matches = []

        for spec in HIPAA_IDENTIFIERS:
            for pattern in cls._column_patterns[spec.identifier]:
                if pattern.match(column_name):
                    matches.append(spec)
                    break

        return matches

    @classmethod
    def match_value(cls, value: str) -> list[HIPAAIdentifierSpec]:
        """Match a value against HIPAA identifier patterns.

        Args:
            value: Value to check.

        Returns:
            List of matching HIPAA identifier specs.
        """
        cls._ensure_compiled()
        matches = []

        for spec in HIPAA_IDENTIFIERS:
            for pattern in cls._value_patterns[spec.identifier]:
                if pattern.match(value):
                    matches.append(spec)
                    break

        return matches

    @classmethod
    def get_spec(cls, identifier: HIPAAIdentifier) -> HIPAAIdentifierSpec:
        """Get the specification for a HIPAA identifier.

        Args:
            identifier: The HIPAA identifier type.

        Returns:
            The identifier specification.
        """
        return IDENTIFIER_BY_TYPE[identifier]
