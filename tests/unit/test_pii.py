"""Tests for PII detection and tagging."""

from ehrglot.core.types import HIPAAIdentifier, MaskingStrategy, PIILevel
from ehrglot.pii.detector import PIIDetector
from ehrglot.pii.hipaa_identifiers import HIPAA_IDENTIFIERS, HIPAAPatternMatcher
from ehrglot.pii.tagger import DEFAULT_MASKING_STRATEGIES


class TestHIPAAIdentifiers:
    """Tests for HIPAA identifier specifications."""

    def test_all_18_identifiers_defined(self) -> None:
        """Verify all 18 HIPAA identifiers have specifications."""
        assert len(HIPAA_IDENTIFIERS) == 18

    def test_identifier_specs_complete(self) -> None:
        """Verify each identifier spec has required fields."""
        for spec in HIPAA_IDENTIFIERS:
            assert spec.identifier is not None
            assert spec.name
            assert spec.description
            assert spec.pii_level is not None
            assert spec.pii_category is not None
            assert isinstance(spec.column_patterns, list)


class TestHIPAAPatternMatcher:
    """Tests for pattern matching against HIPAA identifiers."""

    def test_match_ssn_column(self) -> None:
        """Test matching SSN column names."""
        matches = HIPAAPatternMatcher.match_column_name("ssn")
        assert len(matches) > 0
        assert any(m.identifier == HIPAAIdentifier.SSN for m in matches)

    def test_match_name_columns(self) -> None:
        """Test matching name-related columns."""
        for col_name in ["first_name", "last_name", "patient_name", "PAT_FIRST_NAME"]:
            matches = HIPAAPatternMatcher.match_column_name(col_name)
            assert len(matches) > 0, f"No match for {col_name}"
            assert any(m.identifier == HIPAAIdentifier.NAMES for m in matches)

    def test_match_email_column(self) -> None:
        """Test matching email columns."""
        matches = HIPAAPatternMatcher.match_column_name("email_address")
        assert len(matches) > 0
        assert any(m.identifier == HIPAAIdentifier.EMAIL_ADDRESSES for m in matches)

    def test_match_phone_column(self) -> None:
        """Test matching phone columns."""
        for col_name in ["phone", "home_phone", "mobile_number", "cell_phone"]:
            matches = HIPAAPatternMatcher.match_column_name(col_name)
            assert len(matches) > 0, f"No match for {col_name}"
            assert any(m.identifier == HIPAAIdentifier.PHONE_NUMBERS for m in matches)

    def test_match_mrn_column(self) -> None:
        """Test matching MRN columns."""
        for col_name in ["mrn", "medical_record_number", "pat_id", "patient_id"]:
            matches = HIPAAPatternMatcher.match_column_name(col_name)
            assert len(matches) > 0, f"No match for {col_name}"
            assert any(m.identifier == HIPAAIdentifier.MRN for m in matches)

    def test_match_ssn_value(self) -> None:
        """Test matching SSN values."""
        matches = HIPAAPatternMatcher.match_value("123-45-6789")
        assert len(matches) > 0
        assert any(m.identifier == HIPAAIdentifier.SSN for m in matches)

    def test_match_email_value(self) -> None:
        """Test matching email values."""
        matches = HIPAAPatternMatcher.match_value("patient@hospital.com")
        assert len(matches) > 0
        assert any(m.identifier == HIPAAIdentifier.EMAIL_ADDRESSES for m in matches)

    def test_match_ip_address_value(self) -> None:
        """Test matching IP address values."""
        matches = HIPAAPatternMatcher.match_value("192.168.1.1")
        assert len(matches) > 0
        assert any(m.identifier == HIPAAIdentifier.IP_ADDRESSES for m in matches)

    def test_no_match_generic_column(self) -> None:
        """Test that generic columns don't match."""
        matches = HIPAAPatternMatcher.match_column_name("status")
        assert len(matches) == 0


class TestPIIDetector:
    """Tests for PII detection."""

    def test_detect_ssn_column(self) -> None:
        """Test detecting SSN column."""
        detector = PIIDetector()
        result = detector.detect_column("ssn", ["123-45-6789", "987-65-4321"])
        assert result.detected_pii_level == PIILevel.CRITICAL
        assert HIPAAIdentifier.SSN in result.detected_hipaa_identifiers
        assert result.confidence > 0.5

    def test_detect_email_column(self) -> None:
        """Test detecting email column."""
        detector = PIIDetector()
        result = detector.detect_column("email", ["patient@example.com", "user@hospital.org"])
        assert result.detected_pii_level == PIILevel.CRITICAL
        assert HIPAAIdentifier.EMAIL_ADDRESSES in result.detected_hipaa_identifiers

    def test_detect_non_pii_column(self) -> None:
        """Test that non-PII columns are correctly identified."""
        detector = PIIDetector()
        result = detector.detect_column("status", ["active", "inactive", "pending"])
        assert result.detected_pii_level == PIILevel.NONE
        assert len(result.detected_hipaa_identifiers) == 0

    def test_detect_from_schema(self) -> None:
        """Test schema-only PII detection."""
        detector = PIIDetector()
        columns = ["id", "ssn", "first_name", "email", "status", "created_at"]
        report = detector.detect_from_schema(columns)

        assert report.total_columns == 6
        assert report.columns_with_pii > 0
        assert "ssn" in report.critical_columns or "ssn" in report.high_risk_columns


class TestPIITagger:
    """Tests for PII tagging."""

    def test_default_masking_strategies(self) -> None:
        """Test default masking strategies are defined for all HIPAA identifiers."""
        for identifier in HIPAAIdentifier:
            assert identifier in DEFAULT_MASKING_STRATEGIES

    def test_ssn_masking_strategy(self) -> None:
        """Test SSN uses partial masking."""
        strategy, params = DEFAULT_MASKING_STRATEGIES[HIPAAIdentifier.SSN]
        assert strategy == MaskingStrategy.PARTIAL
        assert params.get("show_last") == 4

    def test_email_masking_strategy(self) -> None:
        """Test email uses hash masking."""
        strategy, _ = DEFAULT_MASKING_STRATEGIES[HIPAAIdentifier.EMAIL_ADDRESSES]
        assert strategy == MaskingStrategy.HASH

    def test_photo_masking_strategy(self) -> None:
        """Test photo uses suppress masking."""
        strategy, _ = DEFAULT_MASKING_STRATEGIES[HIPAAIdentifier.PHOTOS]
        assert strategy == MaskingStrategy.SUPPRESS
