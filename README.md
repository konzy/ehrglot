# EHRglot

Healthcare schema definitions and multi-language code generator.

## Features

- **YAML Schemas** - FHIR R4, HL7 v2.x, C-CDA, and EHR vendor mappings
- **Code Generation** - Generate type-safe code from schemas
- **Multi-language** - Python, Go, TypeScript support

## Installation

```bash
go install github.com/konzy/ehrglot/cmd/ehrglot@latest
```

Or build from source:
```bash
go build -o bin/ehrglot ./cmd/ehrglot
```

## Usage

### List Available Schemas
```bash
ehrglot list
```

### Generate Code
```bash
# Generate Python dataclasses
ehrglot generate --lang python --output ./generated

# Generate Go structs
ehrglot generate --lang go --output ./generated

# Generate TypeScript interfaces
ehrglot generate --lang typescript --output ./generated
```

## Schema Directory Structure

```
schemas/
├── fhir_r4/           # FHIR R4 resource definitions
├── hl7v2/             # HL7 v2.x segment mappings
├── ccda/              # C-CDA template mappings
├── epic_clarity/      # Epic Clarity → FHIR mappings
├── cerner_millennium/ # Cerner → FHIR mappings
└── ...
```

## Generated Output

### Python
```python
@dataclass
class Patient:
    id: str
    birth_date: date | None = None
    gender: str | None = None
```

### Go
```go
type Patient struct {
    ID        string    `json:"id"`
    BirthDate *time.Time `json:"birthDate,omitempty"`
    Gender    string    `json:"gender,omitempty"`
}
```

### TypeScript
```typescript
interface Patient {
    id: string;
    birthDate?: string;
    gender?: string;
}
```

## Related Projects

- [ehrglot-python](https://github.com/konzy/ehrglot-python) - Python runtime library with PII detection, masking, and HL7 parsing

## License

MIT
