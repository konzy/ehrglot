"""Automatic PII detection using pattern matching and data sampling."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ehrglot.core.types import HIPAAIdentifier, PIICategory, PIILevel
from ehrglot.pii.hipaa_identifiers import HIPAAIdentifierSpec, HIPAAPatternMatcher

if TYPE_CHECKING:
    from ehrglot.backends.base import Backend


@dataclass
class PIIDetectionResult:
    """Result of PII detection for a single column."""

    column_name: str
    detected_pii_level: PIILevel
    detected_pii_category: PIICategory
    detected_hipaa_identifiers: list[HIPAAIdentifier]
    confidence: float  # 0.0 to 1.0
    detection_method: str  # 'column_name', 'value_pattern', 'both'
    sample_matches: list[str] = field(default_factory=list)
    notes: str = ""


@dataclass
class DatasetPIIReport:
    """Complete PII detection report for a dataset."""

    total_columns: int
    columns_with_pii: int
    high_risk_columns: list[str]
    critical_columns: list[str]
    column_results: dict[str, PIIDetectionResult]
    summary: str = ""


class PIIDetector:
    """Detects PII in datasets using pattern matching and sampling."""

    def __init__(
        self,
        sample_size: int = 100,
        confidence_threshold: float = 0.5,
    ) -> None:
        """Initialize PII detector.

        Args:
            sample_size: Number of rows to sample for value pattern matching.
            confidence_threshold: Minimum confidence to flag as PII.
        """
        self.sample_size = sample_size
        self.confidence_threshold = confidence_threshold

    def detect_column(
        self,
        column_name: str,
        sample_values: list[Any] | None = None,
    ) -> PIIDetectionResult:
        """Detect PII for a single column.

        Args:
            column_name: Name of the column.
            sample_values: Optional sample of values from the column.

        Returns:
            PIIDetectionResult with detection details.
        """
        # Match column name against patterns
        name_matches = HIPAAPatternMatcher.match_column_name(column_name)

        # Match values against patterns if provided
        value_matches: list[HIPAAIdentifierSpec] = []
        sample_match_examples: list[str] = []

        if sample_values:
            value_match_counts: dict[HIPAAIdentifier, int] = {}
            for value in sample_values:
                if value is None:
                    continue
                str_value = str(value)
                matches = HIPAAPatternMatcher.match_value(str_value)
                for match in matches:
                    value_match_counts[match.identifier] = (
                        value_match_counts.get(match.identifier, 0) + 1
                    )
                    if len(sample_match_examples) < 3:
                        sample_match_examples.append(str_value[:50])

            # Only consider value matches that hit a significant portion of samples
            non_null_count = sum(1 for v in sample_values if v is not None)
            for identifier, count in value_match_counts.items():
                if non_null_count > 0 and count / non_null_count >= 0.3:
                    spec = HIPAAPatternMatcher.get_spec(identifier)
                    value_matches.append(spec)

        # Combine results
        all_matches = list({m.identifier: m for m in name_matches + value_matches}.values())

        if not all_matches:
            return PIIDetectionResult(
                column_name=column_name,
                detected_pii_level=PIILevel.NONE,
                detected_pii_category=PIICategory.NONE,
                detected_hipaa_identifiers=[],
                confidence=0.0,
                detection_method="none",
            )

        # Determine detection method
        if name_matches and value_matches:
            method = "both"
            confidence = 0.95
        elif name_matches:
            method = "column_name"
            confidence = 0.7
        else:
            method = "value_pattern"
            confidence = 0.8

        # Use highest PII level from matches (order: NONE < LOW < MEDIUM < HIGH < CRITICAL)
        level_order = {
            PIILevel.NONE: 0,
            PIILevel.LOW: 1,
            PIILevel.MEDIUM: 2,
            PIILevel.HIGH: 3,
            PIILevel.CRITICAL: 4,
        }
        highest_level = max((m.pii_level for m in all_matches), key=lambda x: level_order.get(x, 0))
        category = all_matches[0].pii_category  # Use first match's category

        return PIIDetectionResult(
            column_name=column_name,
            detected_pii_level=highest_level,
            detected_pii_category=category,
            detected_hipaa_identifiers=[m.identifier for m in all_matches],
            confidence=confidence,
            detection_method=method,
            sample_matches=sample_match_examples,
        )

    def detect_dataframe(
        self,
        df: Any,
        backend: Backend,
    ) -> DatasetPIIReport:
        """Detect PII in a DataFrame using the specified backend.

        Args:
            df: Backend-specific DataFrame.
            backend: Backend instance for data operations.

        Returns:
            DatasetPIIReport with complete detection results.
        """
        schema = backend.get_schema(df)
        column_names = [col.name for col in schema.columns]

        # Sample data for value pattern matching
        sampled_df = backend.sample(df, self.sample_size)
        arrow_table = backend.to_arrow(sampled_df)

        # Detect PII for each column
        column_results: dict[str, PIIDetectionResult] = {}
        high_risk: list[str] = []
        critical: list[str] = []

        for col_name in column_names:
            # Get column values as list
            try:
                col_data = arrow_table.column(col_name).to_pylist()
            except (KeyError, AttributeError):
                col_data = None

            result = self.detect_column(col_name, col_data)
            column_results[col_name] = result

            if result.detected_pii_level == PIILevel.CRITICAL:
                critical.append(col_name)
            elif result.detected_pii_level == PIILevel.HIGH:
                high_risk.append(col_name)

        columns_with_pii = sum(
            1 for r in column_results.values() if r.detected_pii_level != PIILevel.NONE
        )

        summary_parts = [
            f"Analyzed {len(column_names)} columns.",
            f"Found {columns_with_pii} columns with potential PII.",
        ]
        if critical:
            summary_parts.append(f"CRITICAL: {', '.join(critical)}")
        if high_risk:
            summary_parts.append(f"HIGH RISK: {', '.join(high_risk)}")

        return DatasetPIIReport(
            total_columns=len(column_names),
            columns_with_pii=columns_with_pii,
            high_risk_columns=high_risk,
            critical_columns=critical,
            column_results=column_results,
            summary=" ".join(summary_parts),
        )

    def detect_from_schema(
        self,
        column_names: list[str],
    ) -> DatasetPIIReport:
        """Detect PII based only on column names (no data sampling).

        Args:
            column_names: List of column names to analyze.

        Returns:
            DatasetPIIReport with detection results.
        """
        column_results: dict[str, PIIDetectionResult] = {}
        high_risk: list[str] = []
        critical: list[str] = []

        for col_name in column_names:
            result = self.detect_column(col_name, None)
            column_results[col_name] = result

            if result.detected_pii_level == PIILevel.CRITICAL:
                critical.append(col_name)
            elif result.detected_pii_level == PIILevel.HIGH:
                high_risk.append(col_name)

        columns_with_pii = sum(
            1 for r in column_results.values() if r.detected_pii_level != PIILevel.NONE
        )

        return DatasetPIIReport(
            total_columns=len(column_names),
            columns_with_pii=columns_with_pii,
            high_risk_columns=high_risk,
            critical_columns=critical,
            column_results=column_results,
            summary=f"Schema-only analysis: {columns_with_pii}/{len(column_names)} columns flagged.",
        )
