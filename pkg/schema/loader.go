// Package schema provides schema loading and code generation interfaces.
package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Field represents a schema field definition.
type Field struct {
	Name        string  `yaml:"name"`
	Type        string  `yaml:"type"`
	Required    bool    `yaml:"required"`
	Description string  `yaml:"description"`
	PIILevel    string  `yaml:"pii_level,omitempty"`
	Children    []Field `yaml:"children,omitempty"`
}

// Schema represents a YAML schema definition.
type Schema struct {
	Name        string  `yaml:"name"`
	Resource    string  `yaml:"resource"` // FHIR uses 'resource' instead of 'name'
	Description string  `yaml:"description,omitempty"`
	Fields      []Field `yaml:"fields"`
	SourceFile  string  `yaml:"-"`
	Namespace   string  `yaml:"-"`
}

// GetName returns the schema name (handles both 'name' and 'resource' fields).
func (s Schema) GetName() string {
	if s.Name != "" {
		return s.Name
	}
	return s.Resource
}

// Mapping represents a field mapping from source to target.
type FieldMapping struct {
	Source    string `yaml:"source"`
	Target    string `yaml:"target"`
	Transform string `yaml:"transform,omitempty"`
}

// SchemaMapping represents a complete source-to-target mapping.
type SchemaMapping struct {
	SourceSystem   string         `yaml:"source_system"`
	SourceTable    string         `yaml:"source_table"`
	TargetResource string         `yaml:"target_resource"`
	FieldMappings  []FieldMapping `yaml:"field_mappings"`
	SourceFile     string         `yaml:"-"`
}

// Loader loads schemas from YAML files.
type Loader struct {
	baseDir string
}

// NewLoader creates a new schema loader.
func NewLoader(baseDir string) *Loader {
	return &Loader{baseDir: baseDir}
}

// LoadAll loads all schemas from the base directory.
func (l *Loader) LoadAll() ([]Schema, error) {
	var schemas []Schema

	// Load FHIR R4 schemas
	fhirDir := filepath.Join(l.baseDir, "fhir_r4")
	if _, err := os.Stat(fhirDir); err == nil {
		fhirSchemas, err := l.loadSchemaDir(fhirDir, "fhir_r4")
		if err != nil {
			return nil, fmt.Errorf("failed to load fhir_r4: %w", err)
		}
		schemas = append(schemas, fhirSchemas...)
	}

	// Load other schema directories
	entries, err := os.ReadDir(l.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema dir: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == "fhir_r4" || name == "schema_overrides" {
			continue
		}

		dir := filepath.Join(l.baseDir, name)
		dirSchemas, err := l.loadSchemaDir(dir, name)
		if err != nil {
			// Skip directories that don't have schemas
			continue
		}
		schemas = append(schemas, dirSchemas...)
	}

	return schemas, nil
}

func (l *Loader) loadSchemaDir(dir, namespace string) ([]Schema, error) {
	var schemas []Schema

	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		// Skip mapping files
		if strings.HasSuffix(file, "_mapping.yaml") {
			continue
		}

		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var schema Schema
		if err := yaml.Unmarshal(data, &schema); err != nil {
			continue
		}

		if schema.GetName() == "" {
			continue
		}

		schema.SourceFile = file
		schema.Namespace = namespace
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// LoadMappings loads all schema mappings.
func (l *Loader) LoadMappings() ([]SchemaMapping, error) {
	var mappings []SchemaMapping

	err := filepath.WalkDir(l.baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(path, "_mapping.yaml") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		var mapping SchemaMapping
		if err := yaml.Unmarshal(data, &mapping); err != nil {
			return nil
		}

		mapping.SourceFile = path
		mappings = append(mappings, mapping)
		return nil
	})

	return mappings, err
}

// ListSchemas returns a list of available schema names.
func (l *Loader) ListSchemas() ([]string, error) {
	schemas, err := l.LoadAll()
	if err != nil {
		return nil, err
	}

	var names []string
	for _, s := range schemas {
		names = append(names, fmt.Sprintf("%s/%s", s.Namespace, s.GetName()))
	}
	return names, nil
}

// Generator is the interface for language-specific code generators.
type Generator interface {
	Generate(schemas []Schema, outputDir string) error
	GenerateMappings(mappings []SchemaMapping, outputDir string) error
}
