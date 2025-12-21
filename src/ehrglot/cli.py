"""Command-line interface for EHRglot."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import click

from ehrglot.conversion.mapper import SchemaMapper
from ehrglot.parsers.hl7v2 import HL7v2Parser
from ehrglot.schemas.loader import SchemaLoader


def get_default_schema_dir() -> Path:
    """Get default schema directory."""
    # Look for schemas relative to package
    package_dir = Path(__file__).parent.parent.parent.parent / "schemas"
    if package_dir.exists():
        return package_dir
    # Fallback to current directory
    return Path.cwd() / "schemas"


@click.group()
@click.version_option(version="0.1.0")
@click.option(
    "--schema-dir",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Schema directory path",
)
@click.pass_context
def cli(ctx: click.Context, schema_dir: Path | None) -> None:
    """EHRglot - Healthcare data transformation toolkit."""
    ctx.ensure_object(dict)
    ctx.obj["schema_dir"] = schema_dir or get_default_schema_dir()


@cli.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--source",
    "-s",
    required=True,
    help="Source format (hl7v2, epic_clarity, cerner_millennium, etc.)",
)
@click.option(
    "--target",
    "-t",
    default="fhir",
    help="Target format (default: fhir)",
)
@click.option(
    "--resource",
    "-r",
    default="patient",
    help="FHIR resource type (default: patient)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output file (default: stdout)",
)
@click.option(
    "--pretty/--compact",
    default=True,
    help="Pretty print JSON output",
)
@click.pass_context
def convert(
    ctx: click.Context,
    input_file: Path,
    source: str,
    target: str,
    resource: str,
    output: Path | None,
    pretty: bool,
) -> None:
    """Convert healthcare data between formats.

    Examples:

        ehrglot convert -s hl7v2 -r patient message.hl7

        ehrglot convert -s epic_clarity -r encounter data.csv -o output.json
    """
    schema_dir = ctx.obj["schema_dir"]

    try:
        if source.lower() == "hl7v2":
            result = convert_hl7v2(input_file, resource)
        else:
            result = convert_csv(input_file, source, resource, schema_dir)

        output_json = json.dumps(result, indent=2 if pretty else None, default=str)

        if output:
            output.write_text(output_json)
            click.echo(f"Output written to {output}")
        else:
            click.echo(output_json)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


def convert_hl7v2(input_file: Path, resource: str) -> dict[str, Any] | list[dict[str, Any]]:
    """Convert HL7 v2.x file to FHIR."""
    parser = HL7v2Parser()
    messages = parser.parse_file(input_file)

    if not messages:
        raise click.ClickException("No valid HL7 messages found in file")

    results = []
    for msg in messages:
        try:
            fhir_resource = parser.to_fhir(msg, resource)
            results.append(fhir_resource)
        except ValueError as e:
            click.echo(f"Warning: Skipping message - {e}", err=True)

    return results[0] if len(results) == 1 else results


def convert_csv(
    input_file: Path, source: str, resource: str, schema_dir: Path
) -> list[dict[str, Any]]:
    """Convert CSV file using schema mappings."""
    import csv

    loader = SchemaLoader(schema_dir)

    try:
        mapping = loader.load_mapping(source, resource)
    except FileNotFoundError as err:
        raise click.ClickException(
            f"No mapping found for {source}/{resource}. "
            f"Use 'ehrglot list-sources' to see available sources."
        ) from err

    mapper = SchemaMapper(mapping)

    with open(input_file, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    results, errors = mapper.map_dataset(rows)

    if errors:
        click.echo(f"Warning: {len(errors)} rows had errors", err=True)

    # Add resourceType to each result
    resource_type = resource.capitalize()
    for r in results:
        r["resourceType"] = resource_type

    return results


@cli.command()
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--schema",
    "-s",
    required=True,
    help="Schema to validate against (e.g., fhir_r4/patient)",
)
@click.pass_context
def validate(ctx: click.Context, input_file: Path, schema: str) -> None:
    """Validate data against a schema.

    Example:

        ehrglot validate -s fhir_r4/patient patient.json
    """
    schema_dir = ctx.obj["schema_dir"]

    try:
        loader = SchemaLoader(schema_dir)
        parts = schema.split("/")

        if len(parts) != 2:
            raise click.ClickException("Schema must be in format 'namespace/resource'")

        namespace, resource_name = parts

        # Load data
        data = json.loads(input_file.read_text())

        # Basic validation - check required fields
        if namespace == "fhir_r4":
            schema_obj = loader.load_fhir_resource(resource_name)
            required_fields = [f.name for f in schema_obj.fields if f.required]
            missing = [f for f in required_fields if f not in data]

            if missing:
                click.echo(f"Validation FAILED: Missing required fields: {missing}", err=True)
                sys.exit(1)
            else:
                click.echo("Validation PASSED: All required fields present")
        else:
            # For custom schemas, just verify it loads
            loader.load_custom_schema(schema)
            click.echo("Validation PASSED: Schema loaded successfully")

    except FileNotFoundError as e:
        raise click.ClickException(str(e)) from e


@cli.command("list-sources")
@click.pass_context
def list_sources(ctx: click.Context) -> None:
    """List available source systems."""
    schema_dir = ctx.obj["schema_dir"]
    loader = SchemaLoader(schema_dir)

    sources = loader.list_source_systems()
    custom = loader.list_custom_schemas()

    click.echo("Available source systems:")
    click.echo()

    click.echo("  Direct Database:")
    for source in sorted(sources):
        if source not in [
            "fhir_r4",
            "schema_overrides",
            "hl7v2",
            "ccda",
            "custom",
            "hl7v2_overrides",
        ]:
            click.echo(f"    - {source}")

    click.echo()
    click.echo("  Legacy Formats:")
    click.echo("    - hl7v2")
    click.echo("    - ccda")

    if custom:
        click.echo()
        click.echo("  Custom Schemas:")
        for c in sorted(custom):
            click.echo(f"    - {c}")


@cli.command("list-resources")
@click.pass_context
def list_resources(ctx: click.Context) -> None:
    """List available FHIR resources."""
    schema_dir = ctx.obj["schema_dir"]
    loader = SchemaLoader(schema_dir)

    resources = loader.list_fhir_resources()

    click.echo("Available FHIR R4 resources:")
    click.echo()
    for resource in sorted(resources):
        click.echo(f"  - {resource}")


@cli.command()
@click.argument("source")
@click.pass_context
def show_mappings(ctx: click.Context, source: str) -> None:
    """Show available mappings for a source system.

    Example:

        ehrglot show-mappings epic_clarity
    """
    schema_dir = ctx.obj["schema_dir"]

    source_dir = schema_dir / source
    if not source_dir.exists():
        raise click.ClickException(f"Source system not found: {source}")

    mappings = list(source_dir.glob("*_mapping.yaml"))

    if not mappings:
        click.echo(f"No mappings found for {source}")
        return

    click.echo(f"Mappings for {source}:")
    click.echo()
    for m in sorted(mappings):
        resource = m.stem.replace("_mapping", "")
        click.echo(f"  - {resource}")


def main() -> None:
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
