# CSVW-SAFE Utility Library

This library provides Python utilities for generating, validating, and testing CSVW-SAFE metadata and associated dummy datasets for differential privacy (DP) development and safe data modeling workflows.

It includes four main scripts:

1. make_metadata_from_data.py
2. make_dummy_from_metadata.py
3. validate_metadata.py
4. assert_same_structure.py

![Overview](../images/utils_scripts.png)

## Installation

Install Python 3.9+ and required dependencies:
```
pip install pandas numpy pyshacl
```
Note: pyshacl is optional. SHACL validation will be skipped if not installed.

Install the library via pip: TODO!!
```
pip install csvw-safe-library
```
or 
```
git clone https://github.com/your-org/csvw-safe-library.git
cd csvw-safe-library
pip install .
```

## Scripts Overview

### 1. **`make_metadata_from_data.py`**
**Purpose**: Automatically generate baseline CSVW-SAFE metadata from an existing dataset.
- Drafts metadata describing column types, partitions, nullability, and DP bounds.
- Does not replace manual governance review — all generated metadata must be reviewed before publication.

Modes:
| Mode               | Description                                                                                                                                                                                  |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unsafe` (default) | Produces metadata reflecting many structural characteristics of the dataset, similar to a descriptive data release.                                                                          |
| `safe`             | Minimalist metadata generation (restricted structural information). (TODO)                                                                                                                   |
| `fine`             | More controlled generation: <br>• Explicit grouping columns (categorical, non-private)<br>• Coarse value domains<br>• Restricted partition enumeration<br>• Conservative contribution bounds |


Example usage:
```
# Basic metadata generation
python make_metadata_from_data.py data.csv --id user_id


# Strict mode
python make_metadata_from_data.py data.csv --id user_id --mode strict


# Enable automatic column groups
python make_metadata_from_data.py data.csv --id user_id --auto-column-groups


# Disable automatic partition key detection
python make_metadata_from_data.py data.csv --id user_id --no-auto-partition-keys


# Save to a custom file
python make_metadata_from_data.py data.csv --id user_id --output my_metadata.json
```

### 2. **`make_dummy_from_metadata.py`**

**Purpose:** Generate a synthetic dummy dataset using only the declared metadata.

The generated dataset:
- Respects the declared schema (datatypes, constraints)
- Respects declared partition structure
- Respects declared bounds

**Use cases:**
- Functional testing of DP pipelines (Development and debugging without access to real data)
- Schema validation

Example usage:
```
python make_dummy_from_metadata.py my_metadata.json --rows 500 --output dummy.csv
```

### 3. **`validate_metadata.py`**

**Purpose:** Verify that metadata complies with all declared constraints.
- Validates structural consistency (columns, groups, partitions).
- Checks logical DP consistency (max contributions, influenced partitions, worst-case bounds).
- Validates metadata against SHACL rules (optional).

| Concern               | Tool             |
| --------------------- | ---------------- |
| Recursive logic       | Python           |
| DP math               | Python           |
| Interval overlap      | Python           |
| Type checking         | Python and SHACL |
| RDF graph validation  | SHACL            |
| Standards compliance  | SHACL            |
| Tool interoperability | SHACL            |


Example usage:
```
# Python-only validation
python validate_metadata.py my_metadata.json


# With SHACL validation
python validate_metadata.py my_metadata.json --shacl csvw-safe-constraints.ttl
```

### 4. **`assert_same_structure.py`**

**Purpose:** Verify that two datasets share the same declared structure.
Ensures dummy datasets match the real dataset in:
- Column names and order
- Datatypes
- Required/nullable properties
- Categorical partitions (optional)

Example usage:
```
# Check dummy CSV against original
python assert_same_structure.py original.csv dummy.csv


# Skip categorical value subset check
python assert_same_structure.py original.csv dummy.csv --no-categories
```

## Typical Workflow

### Via CLI
1. Generate baseline metadata from the original dataset:
```
python make_metadata_from_data.py data.csv --id user_id --mode fine
```

2. Review and approve metadata for safety and governance compliance.
3. Generate a dummy dataset from the approved metadata:
```
python make_dummy_from_metadata.py metadata.json --rows 1000 --output dummy.csv
```

4. Validate that the dummy matches the original structure:
```
python assert_same_structure.py data.csv dummy.csv
```

5. Optionally, run metadata validation:
```
python validate_metadata.py metadata.json --shacl csvw-safe-constraints.ttl
```

### Via python code
```
import pandas as pd
from csvw_safe_library.metadata import generate_csvw_dp_metadata
from csvw_safe_library.dummy import make_dummy_dataset_csvw_dp
from csvw_safe_library.validate import validate_metadata
from csvw_safe_library.assert_structure import assert_same_structure

df = pd.read_csv("data.csv")

# Generate metadata
metadata = generate_csvw_dp_metadata(df, csv_url="data.csv", individual_col="user_id")

# Generate dummy dataset
dummy_df = make_dummy_dataset_csvw_dp(metadata, nb_rows=500)

# Validate metadata
errors = validate_metadata(metadata)

# Assert structure
assert_same_structure(df, dummy_df)
```

## Tests
```
pip install -r requirements.txt
pytest tests/
```

## Notes

These scripts assist safe data modeling workflows; they do not replace governance decisions.

Automatically generated metadata may contain sensitive information — manual review is required before publishing.

The dummy dataset is intended for development, testing, and pipeline verification, not analysis of real individuals.

# Structure

- Library functions in `csvw_safe_library/` for Python usage  
- Thin CLI wrappers in `scripts/` for command-line convenience  
```
csvw_safe_library/
├─ csvw_safe_library/          # Python package
│  ├─ __init__.py
│  ├─ metadata.py              # make_metadata_from_data.py logic
│  ├─ dummy.py                 # make_dummy_from_metadata.py logic
│  ├─ validate.py              # validate_metadata.py logic
│  ├─ assert_structure.py      # assert_same_structure.py logic
│  └─ utils.py                 # shared helpers (dtype inference, margins)
├─ scripts/                    # CLI wrappers
│  ├─ make_metadata.py
│  ├─ make_dummy.py
│  ├─ validate_metadata.py
│  └─ assert_structure.py
├─ tests/                      # Optional: sample data for testing
├─ README.md
├─ setup.py
├─ pyproject.toml
└─ requirements.txt
```