# CSVW-EO Framework

CSVW-EO consists of vocabulary definitions, validation rules, and Python tooling.

## Components

| File | Purpose |
|---|---|
| `csvw-eo-vocab.ttl` | RDF vocabulary |
| `csvw-eo-context.jsonld` | JSON-LD context |
| `csvw-eo-constraints.ttl` | SHACL constraints |
| `csvw-eo-library` | Python tooling |

## Validation Layers

CSVW-EO metadata may be validated using:

### Pydantic Validation

Implemented in:

- `validate_metadata.py`

Checks:

- required fields
- datatypes
- structure

### SHACL Validation

Implemented in:

- `validate_metadata_shacl.py`

Checks:

- RDF consistency
- graph constraints
- semantic rules

## Python Library

The `csvw-eo-library` provides:

- metadata generation
- dummy data generation
- validation
- OpenDP integration
- SmartNoise SQL conversion

![Workflow](../images/csvwsafe_workflow_1.png)
![Integrations](../images/csvwsafe_workflow_2.png)

## Differential Privacy Integrations

CSVW-EO can be converted to:

- OpenDP contexts
- SmartNoise SQL metadata

This enables automated DP pipelines (in [Lomas](https://github.com/dscc-admin-ch/lomas) for instance).