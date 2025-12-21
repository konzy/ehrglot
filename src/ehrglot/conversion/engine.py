"""Main conversion engine orchestrating the EHR conversion pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ehrglot.backends.base import Backend
from ehrglot.conversion.mapper import SchemaMapper
from ehrglot.conversion.validator import FHIRValidator, ValidationResult
from ehrglot.masking.base import GeneratedSQL, MaskingPolicy, MaskingPolicyGenerator
from ehrglot.masking.databricks import DatabricksMaskingGenerator
from ehrglot.masking.snowflake import SnowflakeMaskingGenerator
from ehrglot.pii.detector import DatasetPIIReport
from ehrglot.pii.tagger import PIITagger
from ehrglot.schemas.loader import SchemaLoader


@dataclass
class ConversionOptions:
    """Options for the conversion process."""

    # Validation
    validate_fhir: bool = True
    fail_on_validation_error: bool = False

    # PII detection
    auto_detect_pii: bool = True
    pii_sample_size: int = 100

    # Masking
    generate_masking_policies: bool = True
    masking_full_access_roles: list[str] = field(
        default_factory=lambda: ["PHI_ADMIN", "CLINICAL_ADMIN"]
    )
    masking_partial_access_roles: list[str] = field(
        default_factory=lambda: ["ANALYST", "RESEARCHER"]
    )

    # Output
    output_format: str = "parquet"  # parquet, csv, json


@dataclass
class ConversionResult:
    """Result of a conversion operation."""

    success: bool
    source_system: str
    target_system: str
    rows_processed: int
    rows_converted: int
    conversion_errors: list[tuple[int, list[str]]]  # (row_index, errors)

    # Validation
    validation_results: list[ValidationResult] = field(default_factory=list)

    # PII detection
    pii_report: DatasetPIIReport | None = None

    # Masking policies
    masking_policy: MaskingPolicy | None = None
    masking_sql: GeneratedSQL | None = None

    # Timing
    start_time: datetime = field(default_factory=datetime.now)
    end_time: datetime | None = None
    duration_seconds: float = 0.0

    # Audit
    audit_log: list[str] = field(default_factory=list)

    def log(self, message: str) -> None:
        """Add an audit log entry."""
        timestamp = datetime.now().isoformat()
        self.audit_log.append(f"[{timestamp}] {message}")

    def finalize(self) -> None:
        """Finalize the result with timing."""
        self.end_time = datetime.now()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()


class ConversionEngine:
    """Main orchestrator for EHR data conversion.

    Handles the complete pipeline:
    1. Load source data
    2. Map to FHIR R4 canonical format
    3. Apply PII detection and tagging
    4. Validate against FHIR specs
    5. Generate masking policies for target platform
    6. Output converted data
    """

    def __init__(
        self,
        backend: Backend,
        schema_dir: str | Path,
    ) -> None:
        """Initialize conversion engine.

        Args:
            backend: Data processing backend (Arrow, Polars, DuckDB).
            schema_dir: Directory containing schema definitions.
        """
        self.backend = backend
        self.schema_loader = SchemaLoader(schema_dir)
        self.pii_tagger = PIITagger()
        self.validator = FHIRValidator(self.schema_loader)

    def _get_masking_generator(self, target_system: str) -> MaskingPolicyGenerator | None:
        """Get the appropriate masking generator for target system."""
        if target_system.lower() == "snowflake":
            return SnowflakeMaskingGenerator()
        elif target_system.lower() == "databricks":
            return DatabricksMaskingGenerator()
        return None

    def convert(
        self,
        source_path: str | Path,
        source_system: str,
        target_system: str,
        resource_type: str = "patient",
        options: ConversionOptions | None = None,
        output_path: str | Path | None = None,
    ) -> ConversionResult:
        """Convert data from source EHR to target platform.

        Args:
            source_path: Path to source data file.
            source_system: Source EHR system (e.g., 'epic_clarity', 'cerner_millennium').
            target_system: Target platform (e.g., 'snowflake', 'databricks', 'fhir_r4').
            resource_type: FHIR resource type being converted.
            options: Conversion options.
            output_path: Optional output path for converted data.

        Returns:
            ConversionResult with all details.
        """
        options = options or ConversionOptions()
        result = ConversionResult(
            success=False,
            source_system=source_system,
            target_system=target_system,
            rows_processed=0,
            rows_converted=0,
            conversion_errors=[],
        )

        try:
            result.log(f"Starting conversion: {source_system} -> {target_system}")

            # 1. Load source data
            result.log(f"Loading source data from {source_path}")
            source_df = self.backend.read_parquet(source_path)
            self.backend.get_schema(source_df)
            arrow_table = self.backend.to_arrow(source_df)
            source_rows = [
                {col: arrow_table.column(col)[i].as_py() for col in arrow_table.column_names}
                for i in range(arrow_table.num_rows)
            ]
            result.rows_processed = len(source_rows)
            result.log(f"Loaded {result.rows_processed} rows")

            # 2. Load mapping and create mapper
            result.log(f"Loading mapping for {source_system} -> {resource_type}")
            mapping = self.schema_loader.load_mapping(source_system, resource_type)
            mapper = SchemaMapper(mapping)

            # 3. Map to FHIR R4
            result.log("Mapping data to FHIR R4 canonical format")
            mapped_rows, mapping_errors = mapper.map_dataset(source_rows)
            result.conversion_errors = mapping_errors
            result.rows_converted = len(mapped_rows)
            result.log(f"Mapped {result.rows_converted} rows, {len(mapping_errors)} with errors")

            # 4. Apply PII detection
            if options.auto_detect_pii:
                result.log("Running PII detection")
                fhir_schema = self.schema_loader.fhir_to_schema_definition(resource_type)
                tagged_schema, pii_report = self.pii_tagger.tag_schema_auto(fhir_schema)
                result.pii_report = pii_report
                result.log(
                    f"PII detection complete: {pii_report.columns_with_pii}/{pii_report.total_columns} columns flagged"
                )

            # 5. Validate against FHIR
            if options.validate_fhir:
                result.log("Validating against FHIR R4 specification")
                result.validation_results = self.validator.validate_batch(
                    mapped_rows, resource_type
                )
                valid_count = sum(1 for r in result.validation_results if r.is_valid)
                result.log(f"Validation complete: {valid_count}/{len(mapped_rows)} valid")

                if options.fail_on_validation_error and valid_count < len(mapped_rows):
                    result.log("Failing due to validation errors")
                    result.finalize()
                    return result

            # 6. Generate masking policies
            if options.generate_masking_policies:
                result.log(f"Generating masking policies for {target_system}")
                generator = self._get_masking_generator(target_system)

                if generator and result.pii_report:
                    tagged_schema, _ = self.pii_tagger.tag_schema_auto(
                        self.schema_loader.fhir_to_schema_definition(resource_type)
                    )
                    policy = generator.create_policy_from_schema(
                        tagged_schema,
                        table_name=resource_type.lower(),
                        schema_name="healthcare",
                    )
                    result.masking_policy = policy
                    result.masking_sql = generator.generate_sql(policy)
                    result.log(f"Generated {len(policy.rules)} masking rules")

            # 7. Write output
            if output_path:
                result.log(f"Writing output to {output_path}")
                # Convert mapped rows back to backend format
                import pyarrow as pa

                # Simple conversion - in production would need proper schema handling
                if mapped_rows:
                    # Flatten nested FHIR structure for table output
                    flat_rows = []
                    for row in mapped_rows:
                        flat_row = self._flatten_fhir_resource(row)
                        flat_rows.append(flat_row)

                    if flat_rows:
                        output_table = pa.Table.from_pylist(flat_rows)
                        output_df = self.backend.from_arrow(output_table)
                        self.backend.write_parquet(output_df, output_path)
                        result.log(f"Wrote {len(flat_rows)} rows to {output_path}")

            result.success = True
            result.log("Conversion completed successfully")

        except Exception as e:
            result.log(f"Conversion failed: {e}")
            result.success = False

        result.finalize()
        return result

    def _flatten_fhir_resource(
        self,
        resource: dict[str, Any],
        prefix: str = "",
    ) -> dict[str, Any]:
        """Flatten nested FHIR resource for tabular output.

        Args:
            resource: Nested FHIR resource.
            prefix: Key prefix for nested fields.

        Returns:
            Flattened dictionary.
        """
        flat: dict[str, Any] = {}

        for key, value in resource.items():
            full_key = f"{prefix}{key}" if prefix else key

            if isinstance(value, dict):
                flat.update(self._flatten_fhir_resource(value, f"{full_key}_"))
            elif isinstance(value, list):
                if len(value) > 0:
                    # For arrays, flatten first element with index
                    if isinstance(value[0], dict):
                        flat.update(self._flatten_fhir_resource(value[0], f"{full_key}_0_"))
                    else:
                        flat[f"{full_key}_0"] = value[0]
                    # Store count
                    flat[f"{full_key}_count"] = len(value)
            else:
                flat[full_key] = value

        return flat

    def get_supported_source_systems(self) -> list[str]:
        """Get list of supported source EHR systems."""
        return self.schema_loader.list_source_systems()

    def get_supported_resources(self) -> list[str]:
        """Get list of supported FHIR resources."""
        return self.schema_loader.list_fhir_resources()

    def preview_mapping(
        self,
        source_system: str,
        resource_type: str,
        sample_row: dict[str, Any],
    ) -> dict[str, Any]:
        """Preview mapping for a sample row.

        Args:
            source_system: Source EHR system.
            resource_type: Target FHIR resource type.
            sample_row: Sample source data row.

        Returns:
            Mapped row preview.
        """
        mapping = self.schema_loader.load_mapping(source_system, resource_type)
        mapper = SchemaMapper(mapping)
        mapped_row, _ = mapper.map_row(sample_row)
        return mapped_row
