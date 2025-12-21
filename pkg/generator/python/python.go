// Package python generates Python code from schemas.
package python

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/konzy/ehrglot/pkg/schema"
)

// Generator generates Python code from schemas.
type Generator struct{}

// NewGenerator creates a new Python code generator.
func NewGenerator() *Generator {
	return &Generator{}
}

// Generate generates Python dataclasses from schemas.
func (g *Generator) Generate(schemas []schema.Schema, outputDir string) error {
	// Group schemas by namespace
	byNamespace := make(map[string][]schema.Schema)
	for _, s := range schemas {
		byNamespace[s.Namespace] = append(byNamespace[s.Namespace], s)
	}

	for namespace, nsSchemas := range byNamespace {
		nsDir := filepath.Join(outputDir, namespace)
		if err := os.MkdirAll(nsDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Generate __init__.py
		initPath := filepath.Join(nsDir, "__init__.py")
		if err := g.generateInit(nsSchemas, initPath); err != nil {
			return err
		}

		// Generate each schema file
		for _, s := range nsSchemas {
			filename := strings.ToLower(s.GetName()) + ".py"
			path := filepath.Join(nsDir, filename)
			if err := g.generateSchema(s, path); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *Generator) generateInit(schemas []schema.Schema, path string) error {
	tmpl := `"""Generated from YAML schemas."""

{{range .}}from .{{. | schemaName | lower}} import {{. | schemaName}}
{{end}}
__all__ = [
{{range .}}    "{{. | schemaName}}",
{{end}}]
`
	return g.executeTemplate(tmpl, schemas, path)
}

func (g *Generator) generateSchema(s schema.Schema, path string) error {
	tmpl := `"""{{.Description}}"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


@dataclass
class {{. | schemaName}}:
    """{{.Description}}"""
{{range .Fields}}
    {{.Name | snake}}: {{.Type | pythonType}}{{if not .Required}} | None = None{{end}}{{if .Description}}  # {{.Description}}{{end}}
{{end}}
`
	return g.executeTemplate(tmpl, s, path)
}

func (g *Generator) executeTemplate(tmplStr string, data any, path string) error {
	funcMap := template.FuncMap{
		"lower":      strings.ToLower,
		"snake":      toSnakeCase,
		"pythonType": toPythonType,
		"schemaName": func(s schema.Schema) string { return s.GetName() },
	}

	tmpl, err := template.New("").Funcs(funcMap).Parse(tmplStr)
	if err != nil {
		return fmt.Errorf("failed to parse template: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	return tmpl.Execute(f, data)
}

// GenerateMappings generates Python mapper functions.
func (g *Generator) GenerateMappings(mappings []schema.SchemaMapping, outputDir string) error {
	// TODO: Implement mapping generation
	return nil
}

func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

func toPythonType(yamlType string) string {
	switch yamlType {
	case "string", "code", "id", "uri", "url":
		return "str"
	case "integer", "positiveInt", "unsignedInt":
		return "int"
	case "decimal":
		return "float"
	case "boolean":
		return "bool"
	case "date":
		return "date"
	case "datetime", "instant":
		return "datetime"
	case "base64Binary":
		return "bytes"
	default:
		if strings.HasPrefix(yamlType, "[]") {
			innerType := strings.TrimPrefix(yamlType, "[]")
			return fmt.Sprintf("list[%s]", toPythonType(innerType))
		}
		return "Any"
	}
}
