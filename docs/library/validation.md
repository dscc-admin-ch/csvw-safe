# Metadata Validation

CSVW-SAFE provides two validation layers:

1. Internal schema validation
2. SHACL validation

---

## Internal Validation

The `validate_metadata.py` utility validates metadata against the internal Pydantic schema.

It checks:

- Required fields
- Datatypes
- Structural consistency
- Metadata object validity


### Usage

```bash
python validate_metadata.py metadata.json
```

## SHACL Validation
The `validate_metadata_shacl.py` utility validates metadata against RDF SHACL constraints.

This is the strictest validation layer.

It checks:

- RDF consistency
- SHACL constraints
- Cross-field consistency
- Structural restrictions


### Usage

```bash
python validate_metadata_shacl.py \
  metadata.json \
  csvw-safe-constraints.ttl
```

## Validation Recommendations

| Validator                    | Purpose                |
| ---------------------------- | ---------------------- |
| `validate_metadata.py`       | Fast schema validation |
| `validate_metadata_shacl.py` | Formal RDF validation  |

Both validators should be used before publishing metadata.

## Structure Validation

The `assert_same_structure.py` utility validates that a generated dummy dataset matches the structure of the original dataset.

It checks:

- Column names
- Column ordering
- Datatypes
- Nullable constraints
- Optional categorical domains

### Usage

```bash
python assert_same_structure.py \
  original.csv \
  dummy.csv
```

## Important Notes

Validation ensures structural correctness only.

Validation does not guarantee:

- Metadata safety
- Privacy preservation
- Statistical validity