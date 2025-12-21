"""Integration tests for CLI tool."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from ehrglot.cli import cli

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "hl7v2"


class TestCLI:
    """Test CLI commands."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def schema_dir(self) -> Path:
        return Path(__file__).parent.parent.parent / "schemas"

    def test_cli_version(self, runner: CliRunner) -> None:
        """Test --version flag."""
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_cli_help(self, runner: CliRunner) -> None:
        """Test --help flag."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "EHRglot" in result.output
        assert "convert" in result.output

    def test_list_sources(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test list-sources command."""
        result = runner.invoke(cli, ["--schema-dir", str(schema_dir), "list-sources"])
        assert result.exit_code == 0
        assert "epic_clarity" in result.output
        assert "cerner_millennium" in result.output
        assert "hl7v2" in result.output

    def test_list_resources(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test list-resources command."""
        result = runner.invoke(cli, ["--schema-dir", str(schema_dir), "list-resources"])
        assert result.exit_code == 0
        assert "patient" in result.output
        assert "observation" in result.output

    def test_show_mappings(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test show-mappings command."""
        result = runner.invoke(
            cli, ["--schema-dir", str(schema_dir), "show-mappings", "epic_clarity"]
        )
        assert result.exit_code == 0
        assert "patient" in result.output

    def test_convert_hl7v2_patient(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test converting HL7 v2.x to FHIR Patient."""
        hl7_file = FIXTURES_DIR / "adt_a01.hl7"
        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "hl7v2",
                "-r",
                "patient",
                str(hl7_file),
            ],
        )
        assert result.exit_code == 0
        assert '"resourceType": "Patient"' in result.output
        assert '"birthDate": "1980-05-15"' in result.output
        assert '"gender": "male"' in result.output

    def test_convert_hl7v2_encounter(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test converting HL7 v2.x to FHIR Encounter."""
        hl7_file = FIXTURES_DIR / "adt_a01.hl7"
        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "hl7v2",
                "-r",
                "encounter",
                str(hl7_file),
            ],
        )
        assert result.exit_code == 0
        assert '"resourceType": "Encounter"' in result.output

    def test_convert_hl7v2_observation(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test converting HL7 v2.x to FHIR Observation."""
        hl7_file = FIXTURES_DIR / "oru_r01.hl7"
        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "hl7v2",
                "-r",
                "observation",
                str(hl7_file),
            ],
        )
        assert result.exit_code == 0
        assert '"resourceType": "Observation"' in result.output

    def test_convert_output_to_file(
        self, runner: CliRunner, schema_dir: Path, tmp_path: Path
    ) -> None:
        """Test converting with output to file."""
        hl7_file = FIXTURES_DIR / "adt_a01.hl7"
        output_file = tmp_path / "output.json"

        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "hl7v2",
                "-r",
                "patient",
                "-o",
                str(output_file),
                str(hl7_file),
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()
        assert "Patient" in output_file.read_text()

    def test_convert_compact_output(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test converting with compact JSON output."""
        hl7_file = FIXTURES_DIR / "adt_a01.hl7"
        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "hl7v2",
                "-r",
                "patient",
                "--compact",
                str(hl7_file),
            ],
        )
        assert result.exit_code == 0
        # Compact output should be a single line
        lines = [line for line in result.output.strip().split("\n") if line.strip()]
        assert len(lines) == 1

    def test_convert_missing_file(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test error handling for missing file."""
        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "hl7v2",
                "-r",
                "patient",
                "nonexistent.hl7",
            ],
        )
        assert result.exit_code != 0

    def test_validate_fhir_schema(
        self, runner: CliRunner, schema_dir: Path, tmp_path: Path
    ) -> None:
        """Test validate command with valid data."""
        # Create a valid patient JSON
        patient_file = tmp_path / "patient.json"
        patient_file.write_text('{"resourceType": "Patient", "id": "123", "gender": "male"}')

        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "validate",
                "-s",
                "fhir_r4/patient",
                str(patient_file),
            ],
        )
        assert result.exit_code == 0
        assert "PASSED" in result.output


class TestCLIEdgeCases:
    """Test CLI edge cases and error handling."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        return CliRunner()

    @pytest.fixture
    def schema_dir(self) -> Path:
        return Path(__file__).parent.parent.parent / "schemas"

    def test_invalid_source_format(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test error for invalid source format."""
        hl7_file = FIXTURES_DIR / "adt_a01.hl7"
        result = runner.invoke(
            cli,
            [
                "--schema-dir",
                str(schema_dir),
                "convert",
                "-s",
                "invalid_source",
                "-r",
                "patient",
                str(hl7_file),
            ],
        )
        # Should fail since invalid_source doesn't have a patient_mapping.yaml
        assert result.exit_code != 0 or "Error" in result.output or "No mapping" in result.output

    def test_show_mappings_nonexistent(self, runner: CliRunner, schema_dir: Path) -> None:
        """Test show-mappings for non-existent source."""
        result = runner.invoke(
            cli, ["--schema-dir", str(schema_dir), "show-mappings", "nonexistent"]
        )
        assert result.exit_code != 0 or "not found" in result.output.lower()
