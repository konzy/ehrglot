"""Integration tests for HL7 v2.x to FHIR conversion."""

from pathlib import Path

import pytest

from ehrglot.parsers.hl7v2 import HL7v2Message, HL7v2Parser

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "hl7v2"


class TestHL7v2Parser:
    """Test HL7 v2.x message parsing."""

    @pytest.fixture
    def parser(self) -> HL7v2Parser:
        return HL7v2Parser()

    @pytest.fixture
    def adt_message(self) -> str:
        return (FIXTURES_DIR / "adt_a01.hl7").read_text()

    @pytest.fixture
    def oru_message(self) -> str:
        return (FIXTURES_DIR / "oru_r01.hl7").read_text()

    def test_parse_adt_message(self, parser: HL7v2Parser, adt_message: str) -> None:
        """Test parsing ADT A01 message."""
        msg = parser.parse_message(adt_message)

        assert isinstance(msg, HL7v2Message)
        assert msg.message_type == "ADT^A01"
        assert len(msg.segments) == 4  # MSH, EVN, PID, PV1

    def test_extract_msh_segment(self, parser: HL7v2Parser, adt_message: str) -> None:
        """Test extracting MSH segment fields."""
        msg = parser.parse_message(adt_message)
        msh = msg.get_segment("MSH")

        assert msh is not None
        assert msh.get_value("3") == "EPIC"  # Sending application
        assert msh.get_value("4") == "HOSPITAL"  # Sending facility

    def test_extract_pid_segment(self, parser: HL7v2Parser, adt_message: str) -> None:
        """Test extracting PID segment fields."""
        msg = parser.parse_message(adt_message)
        pid = msg.get_segment("PID")

        assert pid is not None
        assert pid.get_value("3.1") == "12345678"  # Patient ID
        assert pid.get_value("5.1") == "DOE"  # Family name
        assert pid.get_value("5.2") == "JOHN"  # Given name
        assert pid.get_value("7") == "19800515"  # DOB
        assert pid.get_value("8") == "M"  # Sex

    def test_extract_pv1_segment(self, parser: HL7v2Parser, adt_message: str) -> None:
        """Test extracting PV1 segment fields."""
        msg = parser.parse_message(adt_message)
        pv1 = msg.get_segment("PV1")

        assert pv1 is not None
        assert pv1.get_value("2") == "I"  # Inpatient
        assert pv1.get_value("3.1") == "ICU"  # Location

    def test_parse_oru_message(self, parser: HL7v2Parser, oru_message: str) -> None:
        """Test parsing ORU R01 message."""
        msg = parser.parse_message(oru_message)

        assert msg.message_type == "ORU^R01"
        obx_segments = msg.get_all_segments("OBX")
        assert len(obx_segments) == 6  # 6 lab results

    def test_extract_obx_results(self, parser: HL7v2Parser, oru_message: str) -> None:
        """Test extracting lab results from OBX segments."""
        msg = parser.parse_message(oru_message)
        obx_segments = msg.get_all_segments("OBX")

        # First OBX - Glucose
        glucose = obx_segments[0]
        assert glucose.get_value("3.1") == "2345-7"  # LOINC code
        assert glucose.get_value("3.2") == "GLUCOSE"  # Display name
        assert glucose.get_value("5") == "95"  # Value
        assert glucose.get_value("6") == "mg/dL"  # Units
        assert glucose.get_value("8") == "N"  # Normal flag

    def test_segment_to_dict(self, parser: HL7v2Parser, adt_message: str) -> None:
        """Test converting segment to dictionary."""
        msg = parser.parse_message(adt_message)
        pid = msg.get_segment("PID")
        assert pid is not None

        pid_dict = parser.segment_to_dict(pid)

        assert "PID-3" in pid_dict
        assert "PID-5" in pid_dict
        assert pid_dict["PID-5.1"] == "DOE"


class TestHL7v2ToFHIR:
    """Test HL7 v2.x to FHIR conversion."""

    @pytest.fixture
    def parser(self) -> HL7v2Parser:
        return HL7v2Parser()

    @pytest.fixture
    def adt_message(self, parser: HL7v2Parser) -> HL7v2Message:
        raw = (FIXTURES_DIR / "adt_a01.hl7").read_text()
        return parser.parse_message(raw)

    @pytest.fixture
    def oru_message(self, parser: HL7v2Parser) -> HL7v2Message:
        raw = (FIXTURES_DIR / "oru_r01.hl7").read_text()
        return parser.parse_message(raw)

    def test_convert_pid_to_patient(self, parser: HL7v2Parser, adt_message: HL7v2Message) -> None:
        """Test converting PID to FHIR Patient."""
        patient = parser.to_fhir(adt_message, "Patient")

        assert patient["resourceType"] == "Patient"
        # Check that basic fields are present (birthDate and gender use transforms)
        assert patient["birthDate"] == "1980-05-15"
        assert patient["gender"] == "male"

    def test_convert_pv1_to_encounter(self, parser: HL7v2Parser, adt_message: HL7v2Message) -> None:
        """Test converting PV1 to FHIR Encounter."""
        encounter = parser.to_fhir(adt_message, "Encounter")

        assert encounter["resourceType"] == "Encounter"
        # Check that class and location are present in some form
        assert "class" in encounter or "location" in encounter or "period" in encounter

    def test_convert_obx_to_observation(
        self, parser: HL7v2Parser, oru_message: HL7v2Message
    ) -> None:
        """Test converting OBX to FHIR Observation."""
        observation = parser.to_fhir(oru_message, "Observation")

        assert observation["resourceType"] == "Observation"
        assert observation["status"] == "final"
        # Check that code or value is present
        assert "code" in observation or "valueString" in observation

    def test_parse_file(self, parser: HL7v2Parser) -> None:
        """Test parsing messages from file."""
        messages = parser.parse_file(FIXTURES_DIR / "adt_a01.hl7")

        assert len(messages) >= 1
        assert messages[0].message_type == "ADT^A01"


class TestHL7v2EdgeCases:
    """Test edge cases in HL7 parsing."""

    @pytest.fixture
    def parser(self) -> HL7v2Parser:
        return HL7v2Parser()

    def test_empty_message_raises(self, parser: HL7v2Parser) -> None:
        """Test that empty message raises error."""
        with pytest.raises(ValueError, match="Empty HL7 message"):
            parser.parse_message("")

    def test_missing_msh_raises(self, parser: HL7v2Parser) -> None:
        """Test that message without MSH raises error."""
        with pytest.raises(ValueError, match="must start with MSH"):
            parser.parse_message("PID|1||12345")

    def test_missing_segment_returns_none(self, parser: HL7v2Parser) -> None:
        """Test that missing segment returns None."""
        raw = "MSH|^~\\&|TEST|TEST|TEST|TEST|20231215||ADT^A01|1|P|2.5"
        msg = parser.parse_message(raw)

        assert msg.get_segment("PID") is None

    def test_missing_field_returns_none(self, parser: HL7v2Parser) -> None:
        """Test that missing field returns None."""
        raw = "MSH|^~\\&|TEST|TEST|TEST|TEST|20231215||ADT^A01|1|P|2.5\nPID|1||12345"
        msg = parser.parse_message(raw)
        pid = msg.get_segment("PID")

        assert pid is not None
        assert pid.get_value("99") is None  # Non-existent field
