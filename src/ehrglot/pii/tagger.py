"""PII tagging interface for annotating schemas."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ehrglot.core.types import (
    ColumnMetadata,
    HIPAAIdentifier,
    MaskingStrategy,
    PIICategory,
    PIILevel,
    SchemaDefinition,
)
from ehrglot.pii.detector import DatasetPIIReport, PIIDetectionResult, PIIDetector


@dataclass
class PIITag:
    """A PII tag to apply to a column."""

    pii_level: PIILevel
    pii_category: PIICategory
    hipaa_identifier: HIPAAIdentifier | None = None
    masking_strategy: MaskingStrategy = MaskingStrategy.NONE
    masking_params: dict[str, Any] | None = None
    notes: str = ""


# Default masking strategies for each HIPAA identifier
DEFAULT_MASKING_STRATEGIES: dict[HIPAAIdentifier, tuple[MaskingStrategy, dict[str, Any]]] = {
    HIPAAIdentifier.NAMES: (MaskingStrategy.REDACT, {"replacement": "[REDACTED]"}),
    HIPAAIdentifier.GEOGRAPHIC: (MaskingStrategy.GENERALIZE, {"keep_state": True}),
    HIPAAIdentifier.DATES: (MaskingStrategy.GENERALIZE, {"precision": "year"}),
    HIPAAIdentifier.PHONE_NUMBERS: (MaskingStrategy.REDACT, {"replacement": "XXX-XXX-XXXX"}),
    HIPAAIdentifier.FAX_NUMBERS: (MaskingStrategy.REDACT, {"replacement": "XXX-XXX-XXXX"}),
    HIPAAIdentifier.EMAIL_ADDRESSES: (MaskingStrategy.HASH, {"algorithm": "sha256", "truncate": 8}),
    HIPAAIdentifier.SSN: (MaskingStrategy.PARTIAL, {"show_last": 4, "mask_char": "X"}),
    HIPAAIdentifier.MRN: (MaskingStrategy.TOKENIZE, {}),
    HIPAAIdentifier.HEALTH_PLAN_ID: (MaskingStrategy.TOKENIZE, {}),
    HIPAAIdentifier.ACCOUNT_NUMBERS: (MaskingStrategy.PARTIAL, {"show_last": 4}),
    HIPAAIdentifier.LICENSE_NUMBERS: (MaskingStrategy.REDACT, {}),
    HIPAAIdentifier.VEHICLE_IDENTIFIERS: (MaskingStrategy.REDACT, {}),
    HIPAAIdentifier.DEVICE_IDENTIFIERS: (MaskingStrategy.HASH, {}),
    HIPAAIdentifier.WEB_URLS: (MaskingStrategy.REDACT, {}),
    HIPAAIdentifier.IP_ADDRESSES: (MaskingStrategy.GENERALIZE, {"mask_octets": 2}),
    HIPAAIdentifier.BIOMETRIC: (MaskingStrategy.SUPPRESS, {}),
    HIPAAIdentifier.PHOTOS: (MaskingStrategy.SUPPRESS, {}),
    HIPAAIdentifier.OTHER_UNIQUE: (MaskingStrategy.TOKENIZE, {}),
}


class PIITagger:
    """Tags schemas and columns with PII metadata."""

    def __init__(
        self,
        detector: PIIDetector | None = None,
        auto_assign_masking: bool = True,
    ) -> None:
        """Initialize PII tagger.

        Args:
            detector: PIIDetector instance for automatic detection.
            auto_assign_masking: Automatically assign masking strategies based on HIPAA identifiers.
        """
        self.detector = detector or PIIDetector()
        self.auto_assign_masking = auto_assign_masking

    def create_tag_from_detection(
        self,
        detection: PIIDetectionResult,
    ) -> PIITag:
        """Create a PII tag from detection results.

        Args:
            detection: Detection result for a column.

        Returns:
            PIITag with appropriate settings.
        """
        if detection.detected_pii_level == PIILevel.NONE:
            return PIITag(
                pii_level=PIILevel.NONE,
                pii_category=PIICategory.NONE,
            )

        # Use first detected HIPAA identifier
        hipaa_id = (
            detection.detected_hipaa_identifiers[0]
            if detection.detected_hipaa_identifiers
            else None
        )

        # Get default masking strategy
        masking_strategy = MaskingStrategy.NONE
        masking_params: dict[str, Any] = {}

        if self.auto_assign_masking and hipaa_id:
            strategy_info = DEFAULT_MASKING_STRATEGIES.get(hipaa_id)
            if strategy_info:
                masking_strategy, masking_params = strategy_info

        return PIITag(
            pii_level=detection.detected_pii_level,
            pii_category=detection.detected_pii_category,
            hipaa_identifier=hipaa_id,
            masking_strategy=masking_strategy,
            masking_params=masking_params,
            notes=f"Auto-detected via {detection.detection_method} (confidence: {detection.confidence:.0%})",
        )

    def tag_column(
        self,
        column: ColumnMetadata,
        tag: PIITag,
    ) -> ColumnMetadata:
        """Apply a PII tag to a column.

        Args:
            column: Column metadata to tag.
            tag: PII tag to apply.

        Returns:
            New ColumnMetadata with PII tagging applied.
        """
        return ColumnMetadata(
            name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            description=column.description,
            pii_level=tag.pii_level,
            pii_category=tag.pii_category,
            hipaa_identifier=tag.hipaa_identifier,
            masking_strategy=tag.masking_strategy,
            masking_params=tag.masking_params or {},
            source_column=column.source_column,
            transform=column.transform,
        )

    def tag_schema(
        self,
        schema: SchemaDefinition,
        report: DatasetPIIReport,
    ) -> SchemaDefinition:
        """Apply PII tags to all columns in a schema based on detection report.

        Args:
            schema: Schema definition to tag.
            report: PII detection report for the schema.

        Returns:
            New SchemaDefinition with PII tagging applied.
        """
        tagged_columns = []

        for column in schema.columns:
            if column.name in report.column_results:
                detection = report.column_results[column.name]
                tag = self.create_tag_from_detection(detection)
                tagged_column = self.tag_column(column, tag)
            else:
                tagged_column = column

            tagged_columns.append(tagged_column)

        return SchemaDefinition(
            name=schema.name,
            version=schema.version,
            columns=tagged_columns,
            description=schema.description,
            source_system=schema.source_system,
            target_resource=schema.target_resource,
        )

    def tag_schema_auto(
        self,
        schema: SchemaDefinition,
    ) -> tuple[SchemaDefinition, DatasetPIIReport]:
        """Automatically detect and tag PII in a schema.

        Args:
            schema: Schema definition to analyze and tag.

        Returns:
            Tuple of (tagged schema, detection report).
        """
        column_names = [col.name for col in schema.columns]
        report = self.detector.detect_from_schema(column_names)
        tagged_schema = self.tag_schema(schema, report)
        return tagged_schema, report

    def manual_tag(
        self,
        column_name: str,
        pii_level: PIILevel,
        pii_category: PIICategory,
        hipaa_identifier: HIPAAIdentifier | None = None,
        masking_strategy: MaskingStrategy | None = None,
    ) -> PIITag:
        """Create a manual PII tag.

        Args:
            column_name: Name of column (for documentation).
            pii_level: PII sensitivity level.
            pii_category: PII category.
            hipaa_identifier: Optional HIPAA identifier type.
            masking_strategy: Optional masking strategy override.

        Returns:
            PIITag with specified settings.
        """
        # Get default masking if not specified
        strategy = masking_strategy or MaskingStrategy.NONE
        params: dict[str, Any] = {}

        if strategy == MaskingStrategy.NONE and hipaa_identifier:
            strategy_info = DEFAULT_MASKING_STRATEGIES.get(hipaa_identifier)
            if strategy_info:
                strategy, params = strategy_info

        return PIITag(
            pii_level=pii_level,
            pii_category=pii_category,
            hipaa_identifier=hipaa_identifier,
            masking_strategy=strategy,
            masking_params=params,
            notes=f"Manually tagged: {column_name}",
        )

    def get_pii_summary(self, schema: SchemaDefinition) -> dict[str, Any]:
        """Get a summary of PII tagging for a schema.

        Args:
            schema: Tagged schema to summarize.

        Returns:
            Dictionary with PII summary statistics.
        """
        summary: dict[str, Any] = {
            "total_columns": len(schema.columns),
            "by_pii_level": {},
            "by_hipaa_identifier": {},
            "masking_required": [],
            "no_pii": [],
        }

        for col in schema.columns:
            # Count by PII level
            level_name = col.pii_level.value
            summary["by_pii_level"][level_name] = summary["by_pii_level"].get(level_name, 0) + 1

            # Count by HIPAA identifier
            if col.hipaa_identifier:
                id_name = col.hipaa_identifier.value
                summary["by_hipaa_identifier"][id_name] = (
                    summary["by_hipaa_identifier"].get(id_name, 0) + 1
                )

            # Track columns needing masking
            if col.masking_strategy != MaskingStrategy.NONE:
                summary["masking_required"].append(col.name)

            # Track clean columns
            if col.pii_level == PIILevel.NONE:
                summary["no_pii"].append(col.name)

        return summary
