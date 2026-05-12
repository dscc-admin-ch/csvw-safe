# Metadata Generation

The `make_metadata_from_data.py` utility generates baseline CSVW-SAFE metadata from an existing dataset.

---

## Purpose

The generator infers:

- Datatypes
- Nullable proportions
- Numeric bounds
- Public keys
- Contribution assumptions
- Optional dependencies
- Optional partitions
- Optional column groups

!!! danger "Important"

    Automatically generated metadata may leak sensitive information.
    Manual review is always required before publication.

---

## Basic Usage

```bash
python make_metadata_from_data.py \
  data.csv \
  --privacy_unit user_id
```

## Contribution Levels

Four contribution levels are supported:

| Level             | Description                           |
| ----------------- | ------------------------------------- |
| `table`           | Table-level DP metadata only          |
| `table_with_keys` | Table-level metadata with public keys |
| `column`          | Per-column DP contribution metadata   |
| `partition`       | Fine-grained partition-level metadata |

Higher level incurr more risk of disclosure. **Always use the lowest metadata granularity sufficient for the use case.**

### Example: Table-Level Metadata
```bash
python make_metadata_from_data.py \
  data.csv \
  --privacy_unit user_id \
  --default_contributions_level table
```

### Example: Continuous Partitions
```bash
python make_metadata_from_data.py \
  data.csv \
  --privacy_unit user_id \
  --continuous_partitions '{"age":[0,18,30,50,100]}'
```

### Example: Column Groups
```bash
python make_metadata_from_data.py \
  data.csv \
  --privacy_unit user_id \
  --column_groups '[["age","income"]]'
```

### Example: Dependency Detection
```bash
python make_metadata_from_data.py \
  data.csv \
  --privacy_unit user_id \
  --with_dependencies True
```

## Notes
- Datetime columns are inferred automatically
- Numeric bounds are inferred for numeric columns
- Dependency detection may increase runtime
- Fine-grained metadata increases disclosure risk
