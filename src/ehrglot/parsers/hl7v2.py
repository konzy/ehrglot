"""HL7 v2.x message parser and FHIR converter."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ehrglot.conversion.mapper import SchemaMapper
from ehrglot.core.types import FieldMapping, SchemaMapping


@dataclass
class HL7v2Field:
    """Represents a single HL7 v2.x field with components."""

    value: str
    components: list[str] = field(default_factory=list)
    subcomponents: list[list[str]] = field(default_factory=list)

    def get_component(self, index: int) -> str | None:
        """Get component by 1-based index (HL7 convention)."""
        if 1 <= index <= len(self.components):
            return self.components[index - 1] or None
        return None

    def __str__(self) -> str:
        return self.value


@dataclass
class HL7v2Segment:
    """Represents an HL7 v2.x segment (e.g., PID, PV1)."""

    segment_id: str
    fields: list[HL7v2Field]
    raw: str = ""

    def get_field(self, index: int) -> HL7v2Field | None:
        """Get field by 1-based index (e.g., PID-3 is index 3)."""
        # Field 0 is the segment ID itself
        if index == 0:
            return HL7v2Field(value=self.segment_id, components=[self.segment_id])
        if 1 <= index <= len(self.fields):
            return self.fields[index - 1]
        return None

    def get_value(self, field_path: str) -> str | None:
        """Get value by field path (e.g., 'PID-3.1' or '3.1.2')."""
        # Remove segment prefix if present
        path = field_path
        if "-" in path:
            path = path.split("-", 1)[1]

        parts = path.split(".")
        field_idx = int(parts[0])
        field_obj = self.get_field(field_idx)

        if field_obj is None:
            return None

        if len(parts) == 1:
            return field_obj.value or None

        comp_idx = int(parts[1])
        value = field_obj.get_component(comp_idx)

        if len(parts) >= 3 and value:
            subcomp_idx = int(parts[2])
            if comp_idx - 1 < len(field_obj.subcomponents) and subcomp_idx - 1 < len(
                field_obj.subcomponents[comp_idx - 1]
            ):
                return field_obj.subcomponents[comp_idx - 1][subcomp_idx - 1] or None

        return value

    def to_dict(self) -> dict[str, Any]:
        """Convert segment to dictionary for mapping."""
        result: dict[str, Any] = {}
        for i, f in enumerate(self.fields, 1):
            key = f"{self.segment_id}-{i}"
            result[key] = f.value
            for j, comp in enumerate(f.components, 1):
                result[f"{key}.{j}"] = comp
        return result


@dataclass
class HL7v2Message:
    """Represents a complete HL7 v2.x message."""

    segments: list[HL7v2Segment]
    raw: str = ""
    encoding_chars: str = "^~\\&"
    field_separator: str = "|"

    def get_segment(self, segment_id: str, index: int = 0) -> HL7v2Segment | None:
        """Get segment by ID (e.g., 'PID'). Index for repeating segments."""
        matches = [s for s in self.segments if s.segment_id == segment_id]
        if index < len(matches):
            return matches[index]
        return None

    def get_all_segments(self, segment_id: str) -> list[HL7v2Segment]:
        """Get all segments with given ID."""
        return [s for s in self.segments if s.segment_id == segment_id]

    @property
    def message_type(self) -> str | None:
        """Get message type from MSH-9 (e.g., 'ADT^A01')."""
        msh = self.get_segment("MSH")
        if msh:
            return msh.get_value("9")
        return None


class HL7v2Parser:
    """Parses HL7 v2.x messages and converts to FHIR."""

    def __init__(self, schema_dir: str | Path | None = None) -> None:
        """Initialize parser with optional schema directory."""
        self.schema_dir = Path(schema_dir) if schema_dir else None

    def parse_message(self, raw: str) -> HL7v2Message:
        """Parse raw HL7 v2.x message string.

        Args:
            raw: Raw HL7 message (pipe-delimited, \\r or \\n separated).

        Returns:
            Parsed HL7v2Message object.
        """
        # Normalize line endings
        raw = raw.replace("\r\n", "\r").replace("\n", "\r")
        lines = [line.strip() for line in raw.split("\r") if line.strip()]

        if not lines:
            raise ValueError("Empty HL7 message")

        # Parse MSH to get delimiters
        if not lines[0].startswith("MSH"):
            raise ValueError("HL7 message must start with MSH segment")

        field_sep = lines[0][3] if len(lines[0]) > 3 else "|"
        encoding_chars = lines[0][4:8] if len(lines[0]) > 7 else "^~\\&"

        component_sep = encoding_chars[0] if encoding_chars else "^"
        subcomp_sep = encoding_chars[3] if len(encoding_chars) > 3 else "&"

        segments = []
        for line in lines:
            segment = self._parse_segment(line, field_sep, component_sep, subcomp_sep)
            segments.append(segment)

        return HL7v2Message(
            segments=segments,
            raw=raw,
            encoding_chars=encoding_chars,
            field_separator=field_sep,
        )

    def _parse_segment(
        self, line: str, field_sep: str, comp_sep: str, subcomp_sep: str
    ) -> HL7v2Segment:
        """Parse a single segment line."""
        parts = line.split(field_sep)
        segment_id = parts[0]

        # MSH is special - field 1 is the separator itself
        if segment_id == "MSH":
            # Insert field separator as MSH-1
            parts = [segment_id, field_sep] + parts[1:]

        fields = []
        for part in parts[1:]:
            components = part.split(comp_sep)
            subcomponents = [c.split(subcomp_sep) for c in components]
            fields.append(
                HL7v2Field(value=part, components=components, subcomponents=subcomponents)
            )

        return HL7v2Segment(segment_id=segment_id, fields=fields, raw=line)

    def segment_to_dict(self, segment: HL7v2Segment) -> dict[str, Any]:
        """Convert segment to flat dictionary for mapping.

        Returns dict with keys like 'PID-3', 'PID-3.1', 'PID-5.1', etc.
        """
        return segment.to_dict()

    def to_fhir(
        self,
        message: HL7v2Message,
        resource_type: str,
        segment_id: str | None = None,
    ) -> dict[str, Any]:
        """Convert HL7 message to FHIR resource using mappings.

        Args:
            message: Parsed HL7 message.
            resource_type: Target FHIR resource (e.g., 'patient', 'encounter').
            segment_id: Primary segment to use (auto-detected if None).

        Returns:
            FHIR resource as dictionary.
        """
        # Auto-detect segment based on resource type
        if segment_id is None:
            segment_map = {
                "patient": "PID",
                "encounter": "PV1",
                "observation": "OBX",
                "diagnosticreport": "OBR",
            }
            segment_id = segment_map.get(resource_type.lower(), "PID")

        segment = message.get_segment(segment_id)
        if segment is None:
            raise ValueError(f"Segment {segment_id} not found in message")

        # Build source data from segment
        source_data = self.segment_to_dict(segment)

        # Create mapping for this conversion
        mapping = self._create_hl7_mapping(segment_id, resource_type)

        # Use SchemaMapper to apply transformation
        mapper = SchemaMapper(mapping)
        result, errors = mapper.map_row(source_data)

        # Add resourceType
        result["resourceType"] = resource_type.capitalize()

        return result

    def _create_hl7_mapping(self, segment_id: str, resource_type: str) -> SchemaMapping:
        """Create a basic HL7 to FHIR mapping."""
        # Define basic mappings for common segments
        pid_mappings = [
            FieldMapping(source="PID-3.1", target="identifier[0].value"),
            FieldMapping(source="PID-5.1", target="name[0].family"),
            FieldMapping(source="PID-5.2", target="name[0].given[0]"),
            FieldMapping(source="PID-7", target="birthDate", transform="hl7_datetime_to_fhir_date"),
            FieldMapping(source="PID-8", target="gender", transform="hl7_sex_to_fhir_gender"),
            FieldMapping(source="PID-11.1", target="address[0].line[0]"),
            FieldMapping(source="PID-11.3", target="address[0].city"),
            FieldMapping(source="PID-11.4", target="address[0].state"),
            FieldMapping(source="PID-11.5", target="address[0].postalCode"),
            FieldMapping(source="PID-13.1", target="telecom[0].value", transform="normalize_phone"),
        ]

        pv1_mappings = [
            FieldMapping(source="PV1-19.1", target="identifier[0].value"),
            FieldMapping(source="PV1-2", target="class.code"),
            FieldMapping(source="PV1-3.1", target="location[0].location.display"),
            FieldMapping(
                source="PV1-44",
                target="period.start",
                transform="hl7_datetime_to_fhir_datetime",
            ),
            FieldMapping(
                source="PV1-45",
                target="period.end",
                transform="hl7_datetime_to_fhir_datetime",
            ),
        ]

        obx_mappings = [
            FieldMapping(source="OBX-3.1", target="code.coding[0].code"),
            FieldMapping(source="OBX-3.2", target="code.coding[0].display"),
            FieldMapping(source="OBX-5", target="valueString"),
            FieldMapping(source="OBX-11", target="status", transform="hl7_result_status_to_fhir"),
            FieldMapping(
                source="OBX-14",
                target="effectiveDateTime",
                transform="hl7_datetime_to_fhir_datetime",
            ),
        ]

        mapping_table = {
            "PID": pid_mappings,
            "PV1": pv1_mappings,
            "OBX": obx_mappings,
        }

        field_mappings = mapping_table.get(segment_id, pid_mappings)

        return SchemaMapping(
            source_system="hl7v2",
            source_table=segment_id,
            target_resource=resource_type,
            field_mappings=field_mappings,
        )

    def parse_file(self, filepath: str | Path) -> list[HL7v2Message]:
        """Parse HL7 messages from a file.

        Handles files with multiple messages separated by blank lines.
        """
        filepath = Path(filepath)
        content = filepath.read_text()

        # Split on double newlines (message separators)
        raw_messages = content.split("\n\n")
        messages = []

        for raw in raw_messages:
            if raw.strip():
                try:
                    messages.append(self.parse_message(raw))
                except ValueError:
                    continue

        return messages
