package main

import (
	"fmt"
	"os"

	"github.com/konzy/ehrglot/pkg/generator/csharp"
	"github.com/konzy/ehrglot/pkg/generator/golang"
	"github.com/konzy/ehrglot/pkg/generator/java"
	"github.com/konzy/ehrglot/pkg/generator/kotlin"
	"github.com/konzy/ehrglot/pkg/generator/python"
	"github.com/konzy/ehrglot/pkg/generator/rust"
	"github.com/konzy/ehrglot/pkg/generator/scala"
	"github.com/konzy/ehrglot/pkg/generator/sql"
	"github.com/konzy/ehrglot/pkg/generator/typescript"
	"github.com/konzy/ehrglot/pkg/schema"
	"github.com/spf13/cobra"
)

var (
	version   = "0.1.0"
	schemaDir = "schemas"
	outputDir = "./generated"
	language  = "python"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "ehrglot",
		Short: "Healthcare schema code generator",
		Long: `EHRglot generates type-safe code from YAML schema definitions.

Supports generating:
  - Python dataclasses
  - Go structs
  - TypeScript interfaces
  - Java classes
  - Rust structs
  - C# classes
  - Scala case classes
  - Kotlin data classes
  - SQL DDL + dbt models

Example:
  ehrglot generate --lang python --output ./generated`,
	}

	rootCmd.AddCommand(generateCmd())
	rootCmd.AddCommand(listCmd())
	rootCmd.AddCommand(versionCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func generateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate code from schemas",
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := schema.NewLoader(schemaDir)

			schemas, err := loader.LoadAll()
			if err != nil {
				return fmt.Errorf("failed to load schemas: %w", err)
			}

			var generator schema.Generator
			switch language {
			case "python":
				generator = python.NewGenerator()
			case "go", "golang":
				generator = golang.NewGenerator()
			case "typescript", "ts":
				generator = typescript.NewGenerator()
			case "java":
				generator = java.NewGenerator()
			case "rust", "rs":
				generator = rust.NewGenerator()
			case "csharp", "cs":
				generator = csharp.NewGenerator()
			case "scala":
				generator = scala.NewGenerator()
			case "kotlin", "kt":
				generator = kotlin.NewGenerator()
			case "sql", "dbt":
				generator = sql.NewGenerator()
			default:
				return fmt.Errorf("unsupported language: %s", language)
			}

			if err := generator.Generate(schemas, outputDir); err != nil {
				return fmt.Errorf("failed to generate code: %w", err)
			}

			fmt.Printf("Generated %s code in %s\n", language, outputDir)
			return nil
		},
	}

	cmd.Flags().StringVarP(&schemaDir, "schemas", "s", "schemas", "Schema directory path")
	cmd.Flags().StringVarP(&outputDir, "output", "o", "./generated", "Output directory")
	cmd.Flags().StringVarP(&language, "lang", "l", "python", "Target language (python, go, ts, java, rust, csharp, scala, kotlin, sql)")

	return cmd
}

func listCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available schemas",
		RunE: func(cmd *cobra.Command, args []string) error {
			loader := schema.NewLoader(schemaDir)

			schemas, err := loader.ListSchemas()
			if err != nil {
				return fmt.Errorf("failed to list schemas: %w", err)
			}

			fmt.Println("Available schemas:")
			for _, s := range schemas {
				fmt.Printf("  - %s\n", s)
			}
			return nil
		},
	}

	cmd.Flags().StringVarP(&schemaDir, "schemas", "s", "schemas", "Schema directory path")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("ehrglot version %s\n", version)
		},
	}
}
