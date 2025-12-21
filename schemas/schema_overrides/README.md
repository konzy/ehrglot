# Schema Overrides Directory

This directory contains custom overrides for the base EHRglot schemas.
Use these to customize PII levels, masking strategies, and other
protection properties for your organization's needs.

## How It Works

1. Create a YAML file with the same path as the base schema you want to override
2. Only specify the fields and properties you want to change
3. The override will be merged with the base schema at load time

## Allowed Override Properties

You can ONLY override these properties:

- `pii_level`: none, low, medium, high, critical
- `pii_category`: none, direct_identifier, quasi_identifier, temporal, geographic, contact, clinical, financial, biometric
- `hipaa_identifier`: names, geographic_data, dates, phone_numbers, fax_numbers, email, ssn, mrn, health_plan_id, account_numbers, license_numbers, vehicle_ids, device_identifiers, urls, ip_addresses, biometrics, photos, other_unique
- `masking_strategy`: none, redact, hash, partial, generalize, tokenize, suppress
- `masking_params`: dictionary of strategy-specific parameters

## Example Override

Create `schema_overrides/fhir_r4/patient.yaml`:

```yaml
# Custom PII settings for our organization
description: Stricter masking for research compliance

field_overrides:
  birthDate:
    pii_level: critical
    masking_strategy: generalize
    masking_params:
      precision: year

  address:
    pii_level: critical
    masking_strategy: suppress

  telecom:
    masking_strategy: partial
    masking_params:
      show_last: 2
```

## Directory Structure

```
schema_overrides/
├── README.md (this file)
├── fhir_r4/
│   ├── patient.yaml
│   ├── observation.yaml
│   └── ...
├── epic_clarity/
│   └── patient_mapping.yaml
└── cerner_millennium/
    └── patient_mapping.yaml
```
