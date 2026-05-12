# Typical Workflows

CSVW-SAFE workflows usually follow four main steps:

1. Generate metadata with the minimal details level
2. Review and validate metadata (remove non public information)
3. Generate dummy datasets
4. Use metadata in DP systems

---

# CLI Workflow

## 1. Generate Metadata

```bash
python make_metadata_from_data.py data.csv \
  --privacy_unit user_id
```

## 2. Review Metadata

Review the generated metadata manually.

Important checks:

- Remove sensitive statistics
- Remove unnecessary keys
- Minimize disclosure
- Verify DP assumptions

## 3. Validate Metadata

### Internal schema validation

```bash
python validate_metadata.py metadata.json
```

### SHACL validation
```bash
python validate_metadata_shacl.py \
  metadata.json \
  csvw-safe-constraints.ttl
```

## 4. Generate Dummy Dataset

```bash
python make_dummy_from_metadata.py \
  metadata.json \
  --rows 1000 \
  --output dummy.csv
```

## 4. Validate Dummy Structure

```bash
python assert_same_structure.py \
  data.csv \
  dummy.csv
```

## Python API Workflow

```bash
import pandas as pd

from csvw_safe.make_metadata_from_data import make_metadata_from_data
from csvw_safe.validate_metadata import validate_metadata
from csvw_safe.make_dummy_from_metadata import make_dummy_from_metadata

df = pd.read_csv("data.csv")

metadata = make_metadata_from_data(
    df,
    individual_col="user_id",
)

validate_metadata(metadata)

dummy_df = make_dummy_from_metadata(
    metadata,
    nb_rows=500,
)
```

## Workflow Recommendations

| Step                | Recommendation                             |
| ------------------- | ------------------------------------------ |
| Metadata generation | Use lowest contribution detail possible    |
| Validation          | Always run both validators                 |
| Publication         | Remove unnecessary information             |
| Dummy generation    | Use fixed random seeds for reproducibility |


## Important Warning

**Automatically generated metadata is not safe.**

**Human review is mandatory before publication or sharing.**

**When in doubt, remove information.**